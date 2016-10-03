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


# Recover 
class Recover(Message): 
  msg_type = 4 

  def __init__(self, pid, tid):
    super(Recover, self).__init__(pid, tid, Recover.msg_type)

  @classmethod 
  def from_json(cls, my_json):
    return cls(my_json['pid'], my_json['tid'])

  def serialize(self): 
    undumped = super(Recover, self).serialize()
    return json.dumps(undumped)


# Reelect 
class Reelect(Message): 
  msg_type = 5 

  def __init__(self, pid, tid, new_atomic_leader):
    super(Reelect, self).__init__(pid, tid, Reelect.msg_type)
    self.new_atomic_leader = new_atomic_leader

  @classmethod
  def from_json(cls, my_json): 
    return cls(my_json['pid'], my_json['tid'], my_json['new_atomic_leader'])

  def serialize(self): 
    myJSON = super(Reelect, self).serialize() 
    myJSON['new_atomic_leader'] = self.new_atomic_leader
    return json.dumps(myJSON)


# VoteReq
class VoteReq(Message):
  msg_type = 6 

  def __init__(self, pid, tid, request, transaction_diff=None):
    super(VoteReq, self).__init__(pid, tid, VoteReq.msg_type)
    self.request = request
    # self.transaction_diff = transaction_diff # Instance of TransactionDiff

  def hasTransactionDiff(self):
    return self.transaction_diff is not None

  def setTransactionDiff(self, transaction_diff):
    self.transaction_diff = transaction_diff

  @classmethod
  def from_json(cls, my_json):
    result = cls(my_json['pid'], my_json['tid'], my_json['request'])
    # txn_diff = None if my_json['transaction_diff'] is None \
    #   else VoteReq.TransactionDiff.from_json(my_json['transaction_diff'])
    # result.setTransactionDiff(txn_diff)
    return result

  def serialize(self): 
    myJSON = super(VoteReq, self).serialize() 
    myJSON['request'] = self.request
    # myJSON['transaction_diff'] = None if self.transaction_diff is None else self.transaction_diff.serialize()
    return json.dumps(myJSON)

  # Inner, transaction-diff message component
  class TransactionDiff:

    def __init__(self, diff_start, transactions):
      self.diff_start = diff_start
      # To contain instances of Add or Delete (all with TID > diff_start)
      self.transactions = [t for t in transactions if t.tid > diff_start]

    @classmethod
    def from_json(cls, my_json):
      # Grab the diff_start we care about
      diff_start = int(my_json['diff_start'])
      # Grab a list of Adds and Deletes
      transactions_string = my_json['transactions']
      transactions = transactions_string.split(';')
      transactions = [client_req_from_log(txn_log) for txn_log in transactions]
      return cls(diff_start, transactions)

    def serialize(self):
      myJSON = dict()
      result_arr = [msg.serialize() for msg in self.transactions]
      myJSON['transactions'] = ';'.join(result_arr)
      myJSON['diff_start'] = self.diff_start
      return json.dumps(myJSON)


# StateReq 
class StateReq(Message): 
  msg_type = 7 

  def __init__(self, pid, tid):
    super(StateReq, self).__init__(pid, tid, StateReq.msg_type)

  @classmethod 
  def from_json(cls, my_json):
    return cls(my_json['pid'], my_json['tid'])

  def serialize(self): 
    undumped = super(StateReq, self).serialize() 
    return json.dumps(undumped)

# StateRepid
class StateReqResponse(Message):
  msg_type = 8 

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
  msg_type = 9

  def __init__(self, pid, tid):
    super(Ack, self).__init__(pid, tid, Ack.msg_type)

  @classmethod
  def from_json(cls, my_json):
    return cls(my_json['pid'], my_json['tid'])

  def serialize(self): 
    myJSON = super(Ack, self).serialize() 
    return json.dumps(myJSON)

# Identify myself
class Identifier(Message):
  msg_type = 10

  def __init__(self, pid, tid, atomic_leader):
    super(Identifier, self).__init__(pid, tid, Identifier.msg_type)
    self.atomic_leader = atomic_leader

  @classmethod
  def from_json(cls, my_json):
    return cls(my_json['pid'], my_json['tid'], my_json['atomic_leader'])


  def serialize(self):
    myJSON = super(Identifier, self).serialize()
    myJSON['atomic_leader'] = self.atomic_leader
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
  StateReqResponse.msg_type: StateReqResponse,
  Identifier.msg_type: Identifier,
}


# Deserialize (called for internal message passing)
def deserialize_message(msg_string):
  myJSON = json.loads(msg_string)
  return MSG_CONSTRUCTORS[myJSON['type']].from_json(myJSON)



