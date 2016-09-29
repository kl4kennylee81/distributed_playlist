import json 
from enum import Enum 
from abc import ABCMeta, abstractmethod


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
  # Different types of votes 
  Choice = Enum('Choice', 'yes no')
  msg_type = 1

  def __init__(self, port, choice): 
    super(Vote, self).__init__(port, Vote.msg_type) 
    self.choice = Vote.Choice[choice.lower()]

  @classmethod
  def from_json(cls, my_json): 
    return cls(my_json['port'], my_json['choice'])

  def serialize(self):
    myJSON = super(Vote, self).serialize() 
    myJSON['choice'] = self.choice.name
    return json.dumps(myJSON) 



# Decision
class Decision(Message):
  # Different types of decisions 
  Dec = Enum('Decision', 'commit abort')
  msg_type = 2

  def __init__(self, port, decision):
    super(Decision, self).__init__(port, Decision.msg_type)
    self.decision = Decision.Dec[decision.lower()]

  @classmethod
  def from_json(cls, my_json): 
    return cls(my_json['port'], my_json['decision'])

  def serialize(self): 
    myJSON = super(Decision, self).serialize() 
    myJSON['decision'] = self.decision.name
    return json.dumps(myJSON) 



# Pre-Commit
class PreCommit(Message): 
  msg_type = 3

  def __init__(self, port): 
    super(PreCommit, self).__init__(port, PreCommit.msg_type)

  @classmethod 
  def from_json(cls, my_json):
    return cls(my_json['port'])

  def serialize(self): 
    return super(PreCommit, self).serialize() 



# Recover 
class Recover(Message): 
  msg_type = 4 

  def __init__(self, port): 
    super(Recover, self).__init__(port, Recover.msg_type)

  @classmethod 
  def from_json(cls, my_json):
    return cls(my_json['port'])

  def serialize(self): 
    return super(PreCommit, self).serialize() 



# Reelect 
class Reelect(Message): 
  msg_type = 5 

  def __init__(self, port, new_coord_port):
    super(Reelect, self).__init__(port, Reelect.msg_type)
    self.new_coord_port = new_coord_port

  @classmethod
  def from_json(cls, my_json): 
    return cls(my_json['port'], my_json['new_coord_port'])

  def serialize(self): 
    myJSON = super(Reelect, self).serialize() 
    myJSON['new_coord_port'] = self.new_coord_port
    return myJSON



# VoteReq
class VoteReq(Message):
  msg_type = 6 

  def __init__(self, port, participants): 
    super(VoteReq, self).__init__(port, VoteReq.msg_type)
    self.participants = participants

  @classmethod
  def from_json(cls, my_json):
    return cls(my_json['port'], my_json['participants'])

  def serialize(self): 
    myJSON = super(VoteReq, self).serialize() 
    myJSON['participants'] = self.participants
    return myJSON



# 










MSG_CONSTRUCTORS = { 
  Vote.msg_type: Vote, 
  Decision.msg_type: Decision,
  PreCommit.msg_type: PreCommit,
  Recover.msg_type: Recover, 
  Reelect.msg_type: Reelect,
  VoteReq.msg_type: VoteReq
}


# Deserialize 
def deserialize_message(msg_string):
  myJSON = json.loads(msg_string)
  return MSG_CONSTRUCTORS[myJSON['type']].from_json(myJSON)















