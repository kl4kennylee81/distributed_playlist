from enum import Enum

# Possible server states 
State = Enum('State', 'aborted uncertain committable committed')

# Voting choices 
Choice = Enum('Choice', 'yes no')

# Possible decisions 
Dec = Enum('Decision', 'commit abort')








