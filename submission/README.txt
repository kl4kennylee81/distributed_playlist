CS 5414

Lab 1

Hong Jeon
hjj24

Kenneth Lee
kl488

Joe Antonakakis
jma353

We set up servers connected via TCP sockets that were each maintained by separate threads.
The servers had a global state contained in a Server object that we locked when accessing on a
per-thread basis.  We also maintained a connection to the master client (master.py) the entire time.
Threads maintaining connections between processes were ClientConnectionHandlers.
Threads maintaining connection to the master client were MasterClientHandlers.

We wrote classes for internal messaging, building in serializing and deserializing functionality
for sending over sockets.  These we deserialize on receiving a message, and our global server state +
the message contents dictate what action we perform.  This reactive design allowed us to write
handlers on a per-message basis, where we perform all our logic.

We took the same approach for handling messages / requests from the master client.

We wrote a storage driver to handle logging some of our global state (transaction history, DT Logging,
actual songs / urls on disk, crash messages from the master client, debugging) to files that are named
after the pid of the process + what type of log it is.  These enable us to load in our state on recovering
based on the contents of the process' log files.

We use enum34 in our project for Python enum support for enum-compatible values, such as YES / NO votes,
system states (COMMITTED, COMMITTABLE, UNCERTAIN, ABORT), etc.

We kept a series of system constants in a constants.py file.


Description of our tests Tests Custom

### Two Nodes
- crashPartialPreCommit1: Testing case where new leader is elected but he’s the only one so he has to move on by examining his current state
- crashPartialPrecommit2: 2 should become new leader and then commit, but making sure that the new coordinator who has already doesn’t commit to the playlist again, meanwhile everyone else does
- crashVoteREQ1: Participant is uncertain so should lead to abort
- voteNO: Regular case testing vote no
- crashAfterAck: Crash after ack test, should be able to unilaterally commit


### Three Nodes
- oneManDown: participant crashed after vote, but rest move on
- oneManDown2: participant crashes after ack, but rest of the guys move on
- reelection: coordinator crashes after votereq but reelection happens
- cascadingCrash: crash -> reelect -> crash -> reelct -> complete transaction
- roundRobin: correct reelect order round robin
- crashPartialCommit2: testing termination gather where new leader commited and other is undecided
- crashPartialCommit3: testing termination gather where new leader undecided and other is commited




