import json
from abc import ABCMeta, abstractmethod
from constants import *

# Message class 
class Message: 
  __metaclass__ = ABCMeta

  def __init__(self, pid, msg_type): 
    self.pid = int(pid)
    self.type = msg_type

  """Un-dumped JSON for use in subclasses"""
  @abstractmethod
  def serialize(self): 
    return { "pid": self.pid, "type": self.type }


# Internal message 

# Vote
class Vote(Message): 
  msg_type = 1

  def __init__(self, pid, choice): 
    super(Vote, self).__init__(pid, Vote.msg_type) 
    self.choice = choice

  @classmethod
  def from_json(cls, my_json): 
    return cls(my_json['pid'], my_json['choice'])

  def serialize(self):
    myJSON = super(Vote, self).serialize() 
    myJSON['choice'] = self.choice.name
    return json.dumps(myJSON) 



# Decision
class Decision(Message):
  msg_type = 2

  def __init__(self, pid, decision):
    super(Decision, self).__init__(pid, Decision.msg_type)
    self.decision = decision

  @classmethod
  def from_json(cls, my_json): 
    return cls(my_json['pid'], my_json['decision'])

  def serialize(self): 
    myJSON = super(Decision, self).serialize() 
    myJSON['decision'] = self.decision.name
    return json.dumps(myJSON) 



# Pre-Commit
class PreCommit(Message): 
  msg_type = 3

  def __init__(self, pid): 
    super(PreCommit, self).__init__(pid, PreCommit.msg_type)

  @classmethod 
  def from_json(cls, my_json):
    return cls(my_json['pid'])

  def serialize(self): 
    undumped = super(PreCommit, self).serialize() 
    return json.dumps(undumped)



# Recover 
class Recover(Message): 
  msg_type = 4 

  def __init__(self, pid): 
    super(Recover, self).__init__(pid, Recover.msg_type)

  @classmethod 
  def from_json(cls, my_json):
    return cls(my_json['pid'])

  def serialize(self): 
    undumped = super(PreCommit, self).serialize() 
    return json.dumps(undumped)



# Reelect 
class Reelect(Message): 
  msg_type = 5 

  def __init__(self, pid, new_coord_pid):
    super(Reelect, self).__init__(pid, Reelect.msg_type)
    self.new_coord_pid = new_coord_pid

  @classmethod
  def from_json(cls, my_json): 
    return cls(my_json['pid'], my_json['new_coord_pid'])

  def serialize(self): 
    myJSON = super(Reelect, self).serialize() 
    myJSON['new_coord_pid'] = self.new_coord_pid
    return json.dumps(myJSON)


# VoteReq
class VoteReq(Message):
  msg_type = 6 

  def __init__(self, pid, request): 
    super(VoteReq, self).__init__(pid, VoteReq.msg_type)
    self.request = request

  @classmethod
  def from_json(cls, my_json):
    return cls(my_json['pid'], my_json['request'])

  def serialize(self): 
    myJSON = super(VoteReq, self).serialize() 
    myJSON['request'] = self.request
    return json.dumps(myJSON)


# StateReq 
class StateReq(Message): 
  msg_type = 7 

  def __init__(self, pid): 
    super(StateReq, self).__init__(pid, StateReq.msg_type)

  @classmethod 
  def from_json(cls, my_json):
    return cls(my_json['pid'])

  def serialize(self): 
    undumped = super(StateReq, self).serialize() 
    json.dumps(undumped)



# StateRepid
class StateRepid(Message): 
  msg_type = 8 

  def __init__(self, pid, state): 
    super(StateRepid, self).__init__(pid, StateRepid.msg_type)
    self.state = State[state.lower()]

  @classmethod
  def from_json(cls, my_json):
    return cls(my_json['pid'], my_json['state'])

  def serialize(self): 
    myJSON = super(StateRepid, self).serialize() 
    myJSON['state'] = self.state.name
    return json.dumps(myJSON)

# Ack
class Ack(Message): 
  msg_type = 9

  def __init__(self, pid): 
    super(Ack, self).__init__(pid, Ack.msg_type)

  @classmethod
  def from_json(cls, my_json):
    return cls(my_json['pid'])

  def serialize(self): 
    myJSON = super(Ack, self).serialize() 
    return json.dumps(myJSON)

class Identifier(Message):

  msg_type = 10

  def __init__(self,pid):
    super(Identifier, self).__init__(pid, Identifier.msg_type)

  @classmethod
  def from_json(cls, my_json):
    return cls(my_json['pid'])

  def serialize(self):
    myJSON = super(Identifier, self).serialize() 
    return json.dumps(myJSON)


# Constructors to be called in deserialize on a per-
# msg_type basis 
MSG_CONSTRUCTORS = { 
  VoteReq.msg_type: VoteReq, 
  Vote.msg_type: Vote, 
  PreCommit.msg_type: PreCommit, 
  Ack.msg_type: Ack,
  Decision.msg_type: Decision, 
  Recover.msg_type: Recover, 
  Reelect.msg_type: Reelect,
  StateReq.msg_type: StateReq,
  StateRepid.msg_type: StateRepid,
  Identifier.msg_type: Identifier,
}


# Deserialize (called for internal message passing)
def deserialize_message(msg_string):
  myJSON = json.loads(msg_string)
  return MSG_CONSTRUCTORS[myJSON['type']].from_json(myJSON)



