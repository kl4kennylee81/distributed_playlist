from constants import *
from abc import ABCMeta, abstractmethod
import json
from request_messages import Request,deserialize_client_command_request

class CrashRequest(Request):
  msg_type = ForcedType.crash

  def __init__(self,pid):
    super(CrashRequest, self).__init__(pid, CrashRequest.msg_type)

# format vote NO
class VoteNoRequest(Request):
  msg_type = ForcedType.voteNo

  def __init__(self, pid): 
    super(VoteNoRequest, self).__init__(pid, VoteNoRequest.msg_type)


# format crashAfterVote
class CrashAfterVoteRequest(Request):
  msg_type = ForcedType.crashAfterVote

  def __init__(self, pid): 
    super(CrashAfterVoteRequest, self).__init__(pid, CrashAfterVoteRequest.msg_type)

# format crashAfterAck
class CrashAfterAckRequest(Request):
  msg_type = ForcedType.crashAfterAck

  def __init__(self, pid): 
    super(CrashAfterAckRequest, self).__init__(pid, CrashAfterAckRequest.msg_type)

# format is crashVoteREQ 2 3
class CrashVoteRequest(Request):
  msg_type = ForcedType.crashVoteReq

  def __init__(self, pid, sendTopid): 
    super(CrashVoteRequest, self).__init__(pid, CrashVoteRequest.msg_type)

    self.sendTopid = map(int,sendTopid)

# format is crashPartialPreCommit 2 3
class CrashPartialPrecommit(Request):
  msg_type = ForcedType.crashPartialPrecommit

  def __init__(self, pid, sendTopid): 
    super(CrashPartialPrecommit, self).__init__(pid, CrashPartialPrecommit.msg_type)

    self.sendTopid = map(int,sendTopid)

# format is crashPartialCommit 2 3
class CrashPartialCommit(Request):
  msg_type = ForcedType.crashPartialCommit

  def __init__(self, pid, sendTopid): 
    super(CrashPartialCommit, self).__init__(pid, CrashPartialCommit.msg_type)

    self.sendTopid = map(int,sendTopid)

# Deserialize the Client Request 
def deserialize_client_request(msg_string, pid):
  commandRequest = deserialize_client_command_request(msg_string,pid)
  if commandRequest != None:
    return commandRequest
  # Trim white space, split, and clean of extra spacing 
  msg_string = msg_string.strip() 
  msg_list = msg_string.split(" ")
  msg_list = filter(lambda a: a != '', msg_list)

  if msg_list[0].lower() == "crash": 
    return CrashRequest(pid)
  elif msg_list[0].lower() == "vote":
    return VoteNoRequest(pid)
  elif msg_list[0].lower() == "crashaftervote":
    return CrashAfterVoteRequest(pid)
  elif msg_list[0].lower() == "crashafterack": 
    return CrashAfterAckRequest(pid)
  elif msg_list[0].lower() == "crashvotereq": 
    return CrashVoteRequest(pid,msg_list[1:])
  elif msg_list[0].lower() == "crashpartialprecommit":
    return CrashPartialPrecommit(pid,msg_list[1:])
  elif msg_list[0].lower() == "crashpartialcommit":
    return CrashPartialCommit(pid,msg_list[1:])
  else:
    # Malformed message then
    return None



