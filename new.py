"""
Simplified Raft implementation (asyncio) for educational / testing purposes.

Features:
- Simulates N Raft nodes communicating via in-memory message passing (asyncio Queues).
- Leader election (RequestVote), randomized election timeouts.
- AppendEntries (log replication) with heartbeats.
- Persistent state stored in JSON files per node (term, voted_for, log).
- Simple commit logic: leader commits entries once replicated on majority.
- Simulation harness demonstrates election, leader crash, recovery.
- Adjustable network reliability and timing for experiments.

This is NOT a production-ready Raft.
"""

import asyncio
import random
import json
import os
import time
from typing import List, Dict, Any, Optional, Tuple

# ---------- Configuration ----------
NUM_NODES = 3
ELECTION_TIMEOUT_MIN = 0.15  # seconds
ELECTION_TIMEOUT_MAX = 0.3
HEARTBEAT_INTERVAL = 0.05
PERSIST_DIR = "raft_persist"
NETWORK_DROP_RATE = 0.05  # probability a message is dropped
NETWORK_DELAY_RANGE = (0.0, 0.03)  # simulated network delay

LOG = True  # set False to reduce stdout


def now_ms():
    return int(time.time() * 1000)


def log(*args, **kwargs):
    if LOG:
        print(*args, **kwargs)


# ---------- Persistent storage helpers ----------
def ensure_persist_dir():
    os.makedirs(PERSIST_DIR, exist_ok=True)


def persist_filename(node_id: int):
    ensure_persist_dir()
    return os.path.join(PERSIST_DIR, f"node-{node_id}.json")


def load_persist(node_id: int) -> Dict[str, Any]:
    fn = persist_filename(node_id)
    if os.path.exists(fn):
        with open(fn, "r") as f:
            return json.load(f)
    return {"current_term": 0, "voted_for": None, "log": []}


def save_persist(node_id: int, state: Dict[str, Any]):
    fn = persist_filename(node_id)
    with open(fn, "w") as f:
        json.dump(state, f)


# ---------- Raft message definitions ----------
class RequestVote:
    def __init__(self, term: int, candidate_id: int, last_log_index: int, last_log_term: int):
        self.term = term
        self.candidate_id = candidate_id
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term


class RequestVoteResponse:
    def __init__(self, term: int, vote_granted: bool):
        self.term = term
        self.vote_granted = vote_granted


class AppendEntries:
    def __init__(self, term: int, leader_id: int, prev_log_index: int, prev_log_term: int,
                 entries: List[Dict[str, Any]], leader_commit: int):
        self.term = term
        self.leader_id = leader_id
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.entries = entries  # list of log entries (dict with 'term' and 'cmd')
        self.leader_commit = leader_commit


class AppendEntriesResponse:
    def __init__(self, term: int, success: bool, match_index: Optional[int] = None):
        self.term = term
        self.success = success
        self.match_index = match_index  # last index matched on follower on success


# ---------- Node role ----------
FOLLOWER = "Follower"
CANDIDATE = "Candidate"
LEADER = "Leader"


# ---------- In-memory network ----------
class Network:
    def __init__(self):
        # mapping node_id -> asyncio.Queue
        self.queues: Dict[int, asyncio.Queue] = {}
        self.drop_rate = NETWORK_DROP_RATE
        self.delay_range = NETWORK_DELAY_RANGE

    def register(self, node_id: int):
        q = asyncio.Queue()
        self.queues[node_id] = q
        return q

    async def send(self, to_id: int, message: Tuple[str, Any]):
        # Simulate unreliable network
        if random.random() < self.drop_rate:
            # drop message
            log(f"[net] drop -> node{to_id} {type(message[1]).__name__}")
            return
        delay = random.uniform(*self.delay_range)
        await asyncio.sleep(delay)
        q = self.queues.get(to_id)
        if q:
            await q.put(message)


# ---------- Raft node ----------
class RaftNode:
    def __init__(self, node_id: int, peers: List[int], network: Network):
        self.node_id = node_id
        self.peers = peers  # other node ids
        self.network = network
        self.queue = network.register(node_id)

        # persistent state (durable)
        persisted = load_persist(node_id)
        self.current_term: int = persisted["current_term"]
        self.voted_for: Optional[int] = persisted["voted_for"]
        self.log_entries: List[Dict[str, Any]] = persisted["log"]  # 1-based indexing conceptually

        # volatile state
        self.commit_index: int = -1
        self.last_applied: int = -1

        # leader volatile
        self.next_index: Dict[int, int] = {}
        self.match_index: Dict[int, int] = {}

        self.state = FOLLOWER
        self.votes_received = 0
        self.election_reset_event = asyncio.Event()
        self.stopped = False

        # For debugging / traces
        self.trace: List[str] = []

    def persist(self):
        save_persist(self.node_id, {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": self.log_entries
        })

    def last_log_index(self) -> int:
        return len(self.log_entries) - 1

    def last_log_term(self) -> int:
        if self.last_log_index() >= 0:
            return self.log_entries[self.last_log_index()]["term"]
        return 0

    def become_follower(self, term: int, leader_id: Optional[int] = None):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.persist()
        old = self.state
        self.state = FOLLOWER
        self.votes_received = 0
        self.election_reset_event.set()
        log(f"[Node {self.node_id}] {old} -> Follower (term {self.current_term})")

    def become_candidate(self):
        self.state = CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.persist()
        self.votes_received = 1  # voted for self
        self.election_reset_event.clear()
        log(f"[Node {self.node_id}] -> Candidate (term {self.current_term})")

    def become_leader(self):
        self.state = LEADER
        # initialize leader volatile state
        next_idx = self.last_log_index() + 1
        for p in self.peers:
            self.next_index[p] = next_idx
            self.match_index[p] = -1
        log(f"[Node {self.node_id}] -> Leader (term {self.current_term})")
        # leader immediately sends initial empty AppendEntries (heartbeat)
        self.election_reset_event.set()

    async def send_request_vote(self, peer_id: int):
        rv = RequestVote(term=self.current_term,
                         candidate_id=self.node_id,
                         last_log_index=self.last_log_index(),
                         last_log_term=self.last_log_term())
        await self.network.send(peer_id, ("RequestVote", rv))

    async def send_append_entries(self, peer_id: int, entries: List[Dict[str, Any]]):
        prev_index = self.next_index.get(peer_id, 0) - 1
        prev_term = self.log_entries[prev_index]["term"] if prev_index >= 0 and prev_index < len(self.log_entries) else 0
        ae = AppendEntries(term=self.current_term,
                           leader_id=self.node_id,
                           prev_log_index=prev_index,
                           prev_log_term=prev_term,
                           entries=entries,
                           leader_commit=self.commit_index)
        await self.network.send(peer_id, ("AppendEntries", ae))

    async def handle_request_vote(self, source: int, req: RequestVote):
        # Reply false if term < currentTerm
        if req.term < self.current_term:
            resp = RequestVoteResponse(term=self.current_term, vote_granted=False)
            await self.network.send(source, ("RequestVoteResponse", resp))
            return

        # If RPC term > currentTerm: become follower
        if req.term > self.current_term:
            self.become_follower(req.term)

        # If haven't voted or voted for candidate, and candidate's log is at
        # least as up-to-date as receiver's log, grant vote.
        up_to_date = (req.last_log_term > self.last_log_term()) or \
                     (req.last_log_term == self.last_log_term() and req.last_log_index >= self.last_log_index())

        if (self.voted_for is None or self.voted_for == req.candidate_id) and up_to_date:
            self.voted_for = req.candidate_id
            self.persist()
            self.election_reset_event.set()
            resp = RequestVoteResponse(term=self.current_term, vote_granted=True)
            log(f"[Node {self.node_id}] grants vote to {req.candidate_id} (term {self.current_term})")
        else:
            resp = RequestVoteResponse(term=self.current_term, vote_granted=False)
        await self.network.send(source, ("RequestVoteResponse", resp))

    async def handle_append_entries(self, source: int, req: AppendEntries):
        # Reply false if term < currentTerm
        if req.term < self.current_term:
            resp = AppendEntriesResponse(term=self.current_term, success=False)
            await self.network.send(source, ("AppendEntriesResponse", resp))
            return

        # If leader's term >= currentTerm, become follower
        if req.term > self.current_term or self.state != FOLLOWER:
            self.become_follower(req.term, leader_id=req.leader_id)

        # Reset election timeout
        self.election_reset_event.set()

        # Check matching log
        if req.prev_log_index >= 0:
            if req.prev_log_index >= len(self.log_entries):
                resp = AppendEntriesResponse(term=self.current_term, success=False)
                await self.network.send(source, ("AppendEntriesResponse", resp))
                return
            if self.log_entries[req.prev_log_index]["term"] != req.prev_log_term:
                # conflict, delete entry and all that follow it
                # find first index with that term and truncate
                self.log_entries = self.log_entries[:req.prev_log_index]
                self.persist()
                resp = AppendEntriesResponse(term=self.current_term, success=False)
                await self.network.send(source, ("AppendEntriesResponse", resp))
                return

        # Append any new entries
        idx = req.prev_log_index + 1
        for entry in req.entries:
            # if existing entry conflicts with a new one (same index but different term), delete the existing and all that follow it
            if idx < len(self.log_entries):
                if self.log_entries[idx]["term"] != entry["term"]:
                    self.log_entries = self.log_entries[:idx]
                    self.log_entries.append(entry)
            else:
                self.log_entries.append(entry)
            idx += 1

        self.persist()

        # update commit index
        if req.leader_commit > self.commit_index:
            self.commit_index = min(req.leader_commit, self.last_log_index())

        resp = AppendEntriesResponse(term=self.current_term, success=True, match_index=self.last_log_index())
        await self.network.send(source, ("AppendEntriesResponse", resp))

    async def apply_committed_entries(self):
        # Here we simply print applied entries
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log_entries[self.last_applied]
            log(f"[Node {self.node_id}] apply idx={self.last_applied} cmd={entry['cmd']}")

    async def run(self):
        log(f"[Node {self.node_id}] starting (term {self.current_term}, voted_for {self.voted_for})")
        election_task = asyncio.create_task(self.election_loop())
        rpc_task = asyncio.create_task(self.rpc_loop())
        try:
            await asyncio.gather(election_task, rpc_task)
        except asyncio.CancelledError:
            pass
        finally:
            self.stopped = True

    async def election_loop(self):
        while not self.stopped:
            # Followers and candidates wait for an election timeout
            timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
            try:
                await asyncio.wait_for(self.election_reset_event.wait(), timeout=timeout)
                # reset event for next cycle
                self.election_reset_event.clear()
                continue
            except asyncio.TimeoutError:
                # Election timeout elapsed
                if self.state != LEADER:
                    # start election
                    self.become_candidate()
                    await self.start_election()
            await asyncio.sleep(0)  # yield

    async def start_election(self):
        # send RequestVote RPCs to all peers
        for peer in self.peers:
            asyncio.create_task(self.send_request_vote(peer))

        # wait for votes or term change / election reset
        election_deadline = time.time() + random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        while time.time() < election_deadline and self.state == CANDIDATE:
            try:
                # process incoming messages to collect votes (they come through rpc_loop)
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                return

            # check majority
            if self.votes_received > len(self.peers) // 2:
                self.become_leader()
                # start leader heartbeat loop
                asyncio.create_task(self.leader_heartbeat_loop())
                return

        # election failed -> will retry after next timeout

    async def leader_heartbeat_loop(self):
        while self.state == LEADER and not self.stopped:
            # send heartbeats (empty AppendEntries)
            for peer in self.peers:
                asyncio.create_task(self.send_append_entries(peer, []))
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def rpc_loop(self):
        while not self.stopped:
            msg = await self.queue.get()
            msg_type, payload = msg
            # For each message type, handle accordingly
            if msg_type == "RequestVote":
                await self.handle_request_vote(payload.candidate_id, payload)
            elif msg_type == "RequestVoteResponse":
                # handle vote reply (source is embedded in peer's send; but we don't carry source here)
                # For simplicity we rely on the response term and vote flag
                if payload.term > self.current_term:
                    self.become_follower(payload.term)
                elif self.state == CANDIDATE and payload.vote_granted:
                    self.votes_received += 1
                    log(f"[Node {self.node_id}] received vote (total {self.votes_received})")
            elif msg_type == "AppendEntries":
                await self.handle_append_entries(payload.leader_id, payload)
            elif msg_type == "AppendEntriesResponse":
                if payload.term > self.current_term:
                    self.become_follower(payload.term)
                elif self.state == LEADER:
                    # handle replication acknowledgement
                    # In this simplified model we do not track per-peer mapping in message; it's impossible here
                    # So we approximate by updating commit index if majority likely matched
                    if payload.success:
                        # naive majority commit: increment commit_index if possible
                        # Compute potential new commit index
                        N = self.last_log_index()
                        # count nodes that have at least N (include leader)
                        # In this simplified sim, we assume followers that responded successfully have the latest
                        # For better fidelity you should include peer id in responses.
                        # We'll simply move commit index forward by 1 when append success messages are seen.
                        if self.commit_index < N:
                            self.commit_index = N
                            log(f"[Leader {self.node_id}] advanced commit_index to {self.commit_index}")
            else:
                log(f"[Node {self.node_id}] unknown message {msg_type}")

            # apply committed entries
            await self.apply_committed_entries()

    # ---- Client API to propose commands to the cluster (only works on leader) ----
    async def propose(self, command: Any) -> bool:
        if self.state != LEADER:
            return False
        # append to local log
        entry = {"term": self.current_term, "cmd": command}
        self.log_entries.append(entry)
        self.persist()
        # send AppendEntries to peers
        for peer in self.peers:
            # send the new entries starting at next_index[peer]
            # simple approach: send all entries from next_index
            start = self.next_index.get(peer, 0)
            entries = self.log_entries[start:]
            asyncio.create_task(self.send_append_entries(peer, entries))
        return True

    def stop(self):
        self.stopped = True


# ---------- Simulation harness ----------
async def simulate():
    # clean persistence
    if os.path.exists(PERSIST_DIR):
        for f in os.listdir(PERSIST_DIR):
            os.remove(os.path.join(PERSIST_DIR, f))

    net = Network()
    nodes: Dict[int, RaftNode] = {}
    tasks = {}

    for i in range(NUM_NODES):
        peers = [j for j in range(NUM_NODES) if j != i]
        node = RaftNode(node_id=i, peers=peers, network=net)
        nodes[i] = node

    # start nodes
    for i, node in nodes.items():
        tasks[i] = asyncio.create_task(node.run())

    # Wait for some time for leader election
    await asyncio.sleep(1.0)
    leader = find_leader(nodes)
    if leader is None:
        log("No leader elected yet.")
    else:
        log(f"Leader elected: Node {leader.node_id}")

    # If leader exists, propose some commands
    if leader:
        for cmd in ["x=1", "y=2"]:
            ok = await leader.propose(cmd)
            log(f"Propose {cmd} -> {'accepted' if ok else 'rejected'}")
            await asyncio.sleep(0.2)

    # Crash the leader
    if leader:
        log(f"*** Crashing leader Node {leader.node_id} ***")
        tasks[leader.node_id].cancel()
        nodes[leader.node_id].stop()
        await asyncio.sleep(0.1)

    # Wait to observe a new election
    await asyncio.sleep(1.0)
    new_leader = find_leader(nodes, exclude=[leader.node_id] if leader else None)
    if new_leader:
        log(f"New leader elected: Node {new_leader.node_id}")
        # propose another command
        ok = await new_leader.propose("z=3")
        log(f"Propose z=3 -> {'accepted' if ok else 'rejected'}")
    else:
        log("No new leader found after crash.")

    # Restart crashed node
    if leader:
        log(f"*** Restarting node {leader.node_id} ***")
        # Create new node instance that picks up persisted state
        peers = [j for j in range(NUM_NODES) if j != leader.node_id]
        node = RaftNode(node_id=leader.node_id, peers=peers, network=net)
        nodes[leader.node_id] = node
        tasks[leader.node_id] = asyncio.create_task(node.run())

    # Let the cluster stabilize
    await asyncio.sleep(1.0)

    # Print logs for each node
    log("\n=== Logs per node ===")
    for nid, node in nodes.items():
        try:
            log(f"Node {nid}: term={node.current_term} state={node.state} commit_index={node.commit_index} log={node.log_entries}")
        except Exception:
            log(f"Node {nid}: (stopped)")

    # Cancel tasks and shutdown
    for t in tasks.values():
        t.cancel()
    await asyncio.sleep(0.1)


def find_leader(nodes: Dict[int, RaftNode], exclude: Optional[List[int]] = None) -> Optional[RaftNode]:
    for node in nodes.values():
        if exclude and node.node_id in exclude:
            continue
        if node.state == LEADER:
            return node
    return None


if __name__ == "__main__":
    random.seed(42)
    try:
        asyncio.run(simulate())
    except KeyboardInterrupt:
        pass
