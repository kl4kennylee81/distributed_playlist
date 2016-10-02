from enum import Enum

# Possible server states 
State = Enum('State', 'aborted uncertain committable committed')

# Possible coordinator states 
CoordinatorState = Enum('CoordinatorState', 'standby votereq precommit completed')

# Voting choices 
Choice = Enum('Choice', 'yes no')

# Possible decisions 
Decide = Enum('Decide', 'commit abort')

# Forced Action Commands from master client
ForcedType = Enum('ForcedType', 'crash voteNo crashAfterVote crashAfterAck crashVoteReq crashPartialPrecommit crashPartialCommit')

# Commands from master client
RequestType = Enum('RequestType', 'add get delete')




