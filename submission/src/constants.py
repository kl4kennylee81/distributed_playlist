from enum import Enum

# Possible server states 
State = Enum('State', 'aborted uncertain committable committed')

# Possible coordinator states 
CoordinatorState = Enum('CoordinatorState', 'standby votereq precommit completed')

# Commands from master client
Request = Enum('Request', 'add get delete')

# Voting choices 
Choice = Enum('Choice', 'yes no')

# Possible decisions 
Dec = Enum('Decision', 'commit abort')








