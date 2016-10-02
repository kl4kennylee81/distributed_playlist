import json
from abc import ABCMeta, abstractmethod
from constants import *


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
    self.choice = choice

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
    self.decision = decision

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

  def __init__(self, pid, tid, new_coord_pid):
    super(Reelect, self).__init__(pid, tid, Reelect.msg_type)
    self.new_coord_pid = new_coord_pid

  @classmethod
  def from_json(cls, my_json): 
    return cls(my_json['pid'], my_json['tid'], my_json['new_coord_pid'])

  def serialize(self): 
    myJSON = super(Reelect, self).serialize() 
    myJSON['new_coord_pid'] = self.new_coord_pid
    return json.dumps(myJSON)


# VoteReq
class VoteReq(Message):
  msg_type = 6 

  def __init__(self, pid, tid, request, transaction_diff=None):
    super(VoteReq, self).__init__(pid, tid, VoteReq.msg_type)
    self.request = request
    self.transaction_diff = transaction_diff # Instance of TransactionDiff

  def hasTransactionDiff(self):
    return self.transaction_diff is not None

  @classmethod
  def from_json(cls, my_json):
    txn_diff = TransactionDiff.from_json(my_json['transaction_diff'])
    return cls(my_json['pid'], my_json['tid'], my_json['request'], txn_diff)

  def serialize(self): 
    myJSON = super(VoteReq, self).serialize() 
    myJSON['request'] = self.request
    myJSON['transaction_diff'] = None if self.transaction_diff is None else self.transaction_diff.serialize()
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
    json.dumps(undumped)



# StateRepid
class StateRepid(Message): 
  msg_type = 8 

  def __init__(self, pid, tid, state):
    super(StateRepid, self).__init__(pid, tid, StateRepid.msg_type)
    self.state = State[state.lower()]

  @classmethod
  def from_json(cls, my_json):
    return cls(my_json['pid'], my_json['tid'], my_json['state'])

  def serialize(self): 
    myJSON = super(StateRepid, self).serialize() 
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

  def __init__(self, pid, tid):
    super(Identifier, self).__init__(pid, tid, Identifier.msg_type)

  @classmethod
  def from_json(cls, my_json):
    return cls(my_json['pid'], my_json['tid'])

  def serialize(self):
    myJSON = super(Identifier, self).serialize() 
    return json.dumps(myJSON)


class TransactionDiff(Message):
  msg_type = 11

  def __init__(self, pid, tid, transactions, diff_start):
    super(TransactionDiff, self).__init__(pid, tid, TransactionDiff.msg_type)
    self.transactions = transactions # To contain instances of Add or Delete
    self.diff_start = diff_start

  @classmethod
  def from_json(cls, my_json):
    transactions_string = my_json['transactions']
    transactions = transactions_string.split(',')
    transactions = [client_req_from_log(txn, my_json['pid']) for txn in transactions]
    return cls(my_json['pid'], my_json['tid'], transactions, my_json['diff_start'])

  def serialize(self):
    myJSON = super(TransactionDiff, self).serialize()
    result_arr = [msg.serialize()
                  for msg in self.transactions
                  if msg.tid > self.diff_start]
    myJSON['transactions'] = ';'.join(result_arr)
    myJSON['diff_start'] = self.diff_start
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
  TransactionDiff.msg_type: TransactionDiff
}


# Deserialize (called for internal message passing)
def deserialize_message(msg_string):
  myJSON = json.loads(msg_string)
  return MSG_CONSTRUCTORS[myJSON['type']].from_json(myJSON)



# Master -> Server messages 

# Add 
class Add(Message): 
  msg_type = 11

  def __init__(self, pid, tid, song_name, url):
    super(Add, self).__init__(pid, tid, Add.msg_type)
    self.song_name = song_name
    self.url = url

  def serialize(self):
    return str(self.tid) + ",add " + self.song_name + " " + self.url

# Delete 
class Delete(Message): 
  msg_type = 12

  def __init__(self, pid, tid, song_name):
    super(Delete, self).__init__(pid, tid, Delete.msg_type)
    self.song_name = song_name

  def serialize(self):
    return str(self.tid) + ",delete " + self.song_name

# Get 
class Get(Message): 
  msg_type = 13

  def __init__(self, pid, tid, song_name):
    super(Get, self).__init__(pid, tid, Get.msg_type)
    self.song_name = song_name

  def serialize(self):
    return str(self.tid) + ",get " + self.song_name


# Deserialize the Client Request 
def deserialize_client_req(msg_string, pid, tid):
  # Trim white space, split, and clean of extra spacing 
  msg_string = msg_string.strip() 
  msg_list = msg_string.split(" ")
  msg_list = filter(lambda a: a != '', msg_list)

  if msg_list[0].lower() == "add": 
    return Add(pid, tid, msg_list[1], msg_list[2])
  elif msg_list[0].lower() == "delete":
    return Delete(pid, tid, msg_list[1])
  else: 
    return Get(pid, tid, msg_list[1])

# Function to handle pulling historical client requests
# from the Transaction Log of a Process
def client_req_from_log(log_string, pid):
  comma = log_string.find(',')
  tid = int(log_string[:comma])
  msg_string = log_string[comma+1:]
  return deserialize_client_req(msg_string, pid, tid)



