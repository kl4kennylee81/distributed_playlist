import json 
from enum import Enum 
from abc import ABCMeta, abstractmethod

# Different possible server states 
State = Enum('State', 'aborted uncertain committable committed')


# Message class 
class Message: 
  __metaclass__ = ABCMeta

  def __init__(self, port, msg_type): 
    self.port = port 
    self.type = msg_type

  # Undumped JSON for use in subclasses 
  @abstractmethod
  def serialize(self): 
    return { "port": self.port, "type": self.type }


# Internal message 

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
    return super(Recover, self).serialize() 



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



# StateReq 
class StateReq(Message): 
  msg_type = 7 

  def __init__(self, port): 
    super(StateReq, self).__init__(port, StateReq.msg_type)

  @classmethod 
  def from_json(cls, my_json):
    return cls(my_json['port'])

  def serialize(self): 
    return super(StateReq, self).serialize() 



# StateReport
class StateReport(Message): 
  msg_type = 8 

  def __init__(self, port, state): 
    super(StateReport, self).__init__(port, StateReport.msg_type)
    self.state = State[state.lower()]

  @classmethod
  def from_json(cls, my_json):
    return cls(my_json['port'], my_json['state'])

  def serialize(self): 
    myJSON = super(StateReport, self).serialize() 
    myJSON['state'] = self.state.name
    return myJSON


# Constructors to be called in deserialize on a per-
# msg_type basis 
MSG_CONSTRUCTORS = { 
  Vote.msg_type: Vote, 
  Decision.msg_type: Decision, 
  PreCommit.msg_type: PreCommit, 
  Recover.msg_type: Recover, 
  Reelect.msg_type: Reelect,
  VoteReq.msg_type: VoteReq, 
  StateReq.msg_type: StateReq,
  StateReport.msg_type: StateReport
}


# Deserialize (called for internal message passing)
def deserialize_message(msg_string):
  myJSON = json.loads(msg_string)
  return MSG_CONSTRUCTORS[myJSON['type']].from_json(myJSON)



# Master -> Server messages 

# Add 
class Add(Message): 
  msg_type = 9

  def __init__(self, song_name, url): 
    super(Add, self).__init__(-1, Add.msg_type)
    self.song_name = song_name
    self.url = url

# Delete 
class Delete: 
  msg_type = 10 

  def __init__(self, song_name): 
    super(Delete, self).__init__(-1, Delete.msg_type)
    self.song_name = song_name

# Get 
class Get: 
  msg_type = 11

  def __init__(self, song_name): 
    super(Get, self).__init__(-1, Get.msg_type)
    self.song_name = song_name


# Deserialize the Client Request 
def deserialize_client_req(msg_string): 
  # Trim white space, split, and clean of extra spacing 
  msg_string = msg_string.trim() 
  msg_list = msg_string.split(" ")
  msg_list = filter(lambda a: a != '', msg_list)

  if msg_list[0].lower() == "add": 
    return Add(msg_list[1], msg_list[2])
  elif msg_list[0].lower() == "delete":
    return Delete(msg_list[1])
  else: 
    return Get(msg_list[1])


















