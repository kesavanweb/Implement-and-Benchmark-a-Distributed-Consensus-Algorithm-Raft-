# Implement-and-Benchmark-a-Distributed-Consensus-Algorithm-Raft-
[Node 0] starting (term 0, voted_for None)
[Node 1] starting (term 0, voted_for None)
[Node 2] starting (term 0, voted_for None)
[Node 1] -> Candidate (term 1)
[Node 2] Follower -> Follower (term 1)
[Node 2] grants vote to 1 (term 1)
[Node 1] received vote (total 2)
[Node 1] -> Leader (term 1)
[Node 0] Follower -> Follower (term 1)
[Node 0] grants vote to 1 (term 1)
[net] drop -> node0 AppendEntries
[net] drop -> node1 AppendEntriesResponse
Leader elected: Node 1
Propose x=1 -> accepted
[Leader 1] advanced commit_index to 0
[Node 1] apply idx=0 cmd=x=1
[Node 2] apply idx=0 cmd=x=1
[Node 0] apply idx=0 cmd=x=1
Propose y=2 -> accepted
[Leader 1] advanced commit_index to 1
[Node 1] apply idx=1 cmd=y=2
[Node 2] apply idx=1 cmd=y=2
[Node 0] apply idx=1 cmd=y=2
*** Crashing leader Node 1 ***
[Node 0] -> Candidate (term 2)
[Node 2] Follower -> Follower (term 2)
[Node 2] grants vote to 0 (term 2)
[Node 0] received vote (total 2)
[Node 0] -> Leader (term 2)
[net] drop -> node1 AppendEntries
[net] drop -> node1 AppendEntries
New leader elected: Node 0
Propose z=3 -> accepted
*** Restarting node 1 ***
[Node 1] starting (term 1, voted_for 1)
[Node 1] Follower -> Follower (term 2)
[net] drop -> node0 AppendEntriesResponse
[Node 1] apply idx=0 cmd=x=1
[Node 1] apply idx=1 cmd=y=2
[Leader 0] advanced commit_index to 2
[Node 0] apply idx=2 cmd=z=3
[Node 2] apply idx=2 cmd=z=3
[Node 1] apply idx=2 cmd=z=3
[net] drop -> node0 AppendEntriesResponse
[net] drop -> node0 AppendEntriesResponse

=== Logs per node ===
Node 0: term=2 state=Leader commit_index=2 log=[{'term': 1, 'cmd': 'x=1'}, {'term': 1, 'cmd': 'y=2'}, {'term': 2, 'cmd': 'z=3'}]
Node 1: term=2 state=Follower commit_index=2 log=[{'term': 1, 'cmd': 'x=1'}, {'term': 1, 'cmd': 'y=2'}, {'term': 2, 'cmd': 'z=3'}]
Node 2: term=2 state=Follower commit_index=2 log=[{'term': 1, 'cmd': 'x=1'}, {'term': 1, 'cmd': 'y=2'}, {'term': 2, 'cmd': 'z=3'}]
