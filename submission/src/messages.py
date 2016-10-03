import json
from abc import ABCMeta, abstractmethod
from constants import *
from request_messages import client_req_from_log

# Message class 
class Message: 
  __metaclass__ = ABCMeta

  def __init__(self, pid, tid, msg_type):
    self.pid = pid
    self.tid = tid
    self.type = msg_type

  """Un-dumped JSON for use in subclasses"""
  @abstractmethod
  def serialize(self): 
    return { "pid": self.pid,
             "tid": self.tid,
             "type": self.type }

# Internal message 

# Vote
class Vote(Message): 
  msg_type = 1

  def __init__(self, pid, tid, choice):
    super(Vote, self).__init__(pid, tid, Vote.msg_type)
    self.choice = Choice[choice]

  @classmethod
  def from_json(cls, my_json):
    return cls(my_json['pid'], my_json['tid'], my_json['choice'])

  def serialize(self):
    myJSON = super(Vote, self).serialize() 
    myJSON['choice'] = self.choice.name
    return json.dumps(myJSON) 


# Decision
class Decision(Message):
  msg_type = 2

  def __init__(self, pid, tid, decision):
    super(Decision, self).__init__(pid, tid, Decision.msg_type)
    self.decision = Decide[decision]

  @classmethod
  def from_json(cls, my_json): 
    return cls(my_json['pid'], my_json['tid'], my_json['decision'])

  def serialize(self): 
    myJSON = super(Decision, self).serialize()
    myJSON['decision'] = self.decision.name
    return json.dumps(myJSON) 


# Pre-Commit
class PreCommit(Message): 
  msg_type = 3

  def __init__(self, pid, tid):
    super(PreCommit, self).__init__(pid, tid, PreCommit.msg_type)

  @classmethod 
  def from_json(cls, my_json):
    return cls(my_json['pid'], my_json['tid'])

  def serialize(self): 
    undumped = super(PreCommit, self).serialize() 
    return json.dumps(undumped)


# Reelect 
class Reelect(Message): 
  msg_type = 4

  def __init__(self, pid, tid, new_atomic_leader):
    super(Reelect, self).__init__(pid, tid, Reelect.msg_type)
    self.new_atomic_leader = int(new_atomic_leader)

  @classmethod
  def from_json(cls, my_json): 
    return cls(my_json['pid'], my_json['tid'], my_json['new_atomic_leader'])

  def serialize(self): 
    myJSON = super(Reelect, self).serialize() 
    myJSON['new_atomic_leader'] = self.new_atomic_leader
    return json.dumps(myJSON)


# VoteReq
class VoteReq(Message):
  msg_type = 5

  def __init__(self, pid, tid, request, transactions_diff=None):
    super(VoteReq, self).__init__(pid, tid, VoteReq.msg_type)
    self.request = request
    # To contain instances of Add or Delete
    self.transactions_diff = transactions_diff

  @classmethod
  def from_json(cls, my_json):
    transactions_diff = None if my_json['transactions_diff'] == '' else \
      [client_req_from_log(t) for t in my_json['transactions_diff'].split(';')]
    result = cls(my_json['pid'], my_json['tid'], my_json['request'], transactions_diff)
    return result

  def serialize(self): 
    myJSON = super(VoteReq, self).serialize() 
    myJSON['request'] = self.request
    myJSON['transactions_diff'] = None if self.transactions_diff is None else \
      ';'.join([t.serialize() for t in self.transactions_diff])
    return json.dumps(myJSON)


# StateReq 
class StateReq(Message): 
  msg_type = 6

  def __init__(self, pid, tid, atomic_leader):
    super(StateReq, self).__init__(pid, tid, StateReq.msg_type)
    self.atomic_leader = atomic_leader
    # as the new coordinator the stateReq must piggy back the new
    # atomic leader number to make sure the receiver will ignore it
    # or accept him as the new leader

  @classmethod 
  def from_json(cls, my_json):
    return cls(my_json['pid'], my_json['tid'], my_json['atomic_leader'])

  def serialize(self): 
    myJSON = super(StateReq, self).serialize() 
    myJSON['atomic_leader'] = self.atomic_leader
    return json.dumps(myJSON)


# StateReqResponse
class StateReqResponse(Message):
  msg_type = 7


  def __init__(self, pid, tid, state):
    super(StateReqResponse, self).__init__(pid, tid, StateReqResponse.msg_type)
    self.state = State[state.lower()]

  @classmethod
  def from_json(cls, my_json):
    return cls(my_json['pid'], my_json['tid'], my_json['state'])

  def serialize(self): 
    myJSON = super(StateReqResponse, self).serialize()
    myJSON['state'] = self.state.name
    return json.dumps(myJSON)


# Ack
class Ack(Message): 
  msg_type = 8

  def __init__(self, pid, tid):
    super(Ack, self).__init__(pid, tid, Ack.msg_type)

  @classmethod
  def from_json(cls, my_json):
    return cls(my_json['pid'], my_json['tid'])

  def serialize(self): 
    myJSON = super(Ack, self).serialize() 
    return json.dumps(myJSON)


# Identifying at the beginning of connecting to hosts
class Identifier(Message):
  """
  Initial message sent to a procedure on creation of a socket.
  Fields:

  - pid: Process ID (PID) of the sending process
  - tid: Transaction ID that the sending process is on
  - is_leader: Boolean indicating if the sending process is the leader
  - state: State of the sending process
  - last_alive_set: The list of PID's that were last alive (used for recovery)
  - is_recovering: Boolean indicating if the sending process is recovering
  """
  msg_type = 9

  def __init__(self, pid, tid, atomic_leader, state, last_alive_set, is_recovering):
    super(Identifier, self).__init__(pid, tid, Identifier.msg_type)
    self.atomic_leader = atomic_leader
    self.state = state
    self.last_alive_set = last_alive_set
    self.is_recovering = is_recovering

  @classmethod
  def from_json(cls, my_json):
    return cls(my_json['pid'],
               my_json['tid'],
               my_json['atomic_leader'],
               State[my_json['state']],
               [] if my_json['last_alive_set'] == "" else \
                 [int(s) for s in my_json['last_alive_set'].split(';')],
               my_json['is_recovering'])

  def serialize(self):
    myJSON = super(Identifier, self).serialize()
    myJSON['atomic_leader'] = self.atomic_leader
    myJSON['state'] = self.state.name
    myJSON['last_alive_set'] = ';'.join([str(s) for s in self.last_alive_set])
    myJSON['is_recovering'] = self.is_recovering
    return json.dumps(myJSON)


# Constructors to be called in deserialize on a per-
# msg_type basis 
MSG_CONSTRUCTORS = { 
  VoteReq.msg_type: VoteReq, 
  Vote.msg_type: Vote, 
  PreCommit.msg_type: PreCommit, 
  Ack.msg_type: Ack,
  Decision.msg_type: Decision,
  Reelect.msg_type: Reelect,
  StateReq.msg_type: StateReq,
  StateReqResponse.msg_type: StateReqResponse,
  Identifier.msg_type: Identifier,
}

# Deserialize (called for internal message passing)
def deserialize_message(msg_string):
  myJSON = json.loads(msg_string)
  return MSG_CONSTRUCTORS[myJSON['type']].from_json(myJSON)



