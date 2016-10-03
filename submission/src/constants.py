from enum import Enum

BUFFER_SIZE = 1024
START_PORT = 20000
ADDRESS = "localhost"
TIMEOUT_SECS = 50.0

# Possible server states 
State = Enum('State', 'aborted uncertain committable committed')

# Possible coordinator states 
CoordinatorState = Enum('CoordinatorState', 'standby votereq precommit completed termination')

# Voting choices 
Choice = Enum('Choice', 'yes no')

# Possible decisions 
Decide = Enum('Decide', 'commit abort')

# Forced Action Commands from master client
ForcedType = Enum('ForcedType', 'crash voteNo crashAfterVote crashAfterAck crashVoteReq crashPartialPrecommit crashPartialCommit')

# Commands from master client
RequestType = Enum('RequestType', 'add get delete')




