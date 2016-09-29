import json 
from enum import Enum 
from abc import ABCMeta, abstractmethod

# Messaging enum 
MSG = Enum('MSG', 'vote decision precommit recover')


# Different types of messages 
MSG_TYPES = {
  'vote': 1,
  'decision': 2,
  'precommit': 3,
  'recover': 4
}



# Message class 
class Message: 
  __metaclass__ = ABCMeta

  def __init__(self, port, msg_type): 
    self.port = port 
    self.type = msg_type

  @classmethod
  def from_json(cls, my_json): 
    return cls(my_json["port"], my_json["type"])

  # Undumped JSON for use in subclasses 
  @abstractmethod
  def serialize(self): 
    return { "port": self.port, "type": self.type }



# Vote
class Vote(Message): 

  def __init__(self, port, choice): 
    super(Vote, self).__init__(port, MSG_TYPES[MSG.vote.name]) 
    self.Choice = Enum('Choice', 'yes no')
    self.choice = self.Choice[choice.lower()]

  @classmethod
  def from_json(cls, my_json): 
    return cls(my_json['port'], my_json['choice'])

  def serialize(self):
    myJSON = super(Vote, self).serialize() 
    myJSON['choice'] = self.choice.name
    return json.dumps(myJSON) 



# Decision
class Decision(Message):

  def __init__(self, port, decision):
    super(Decision, self).__init__(port, MSG_TYPES[MSG.decision.name])
    self.Decision = Enum('Decision', 'commit abort')
    self.decision = self.Decision[decision.lower()]

  @classmethod
  def from_json(cls, my_json): 
    return cls(my_json['port'], my_json['decision'])

  def serialize(self): 
    myJSON = super(Decision, self).serialize() 
    myJSON['decision'] = self.decision.name
    return json.dumps(myJSON) 



# Pre-Commit
class PreCommit(Message): 

  def __init__(self, port): 
    super(PreCommit, self).__init__(port, MSG_TYPES[MSG.precommit.name])

  @classmethod 
  def from_json(cls, my_json):
    return cls(my_json['port'])



MSG_CONSTRUCTORS = { 
  1: Vote, 
  2: Decision,
  3: PreCommit
}

# Deserialize 
def deserialize_message(msg_string):
  myJSON = json.loads(msg_string)
  return MSG_CONSTRUCTORS[myJSON['type']].from_json(myJSON)















