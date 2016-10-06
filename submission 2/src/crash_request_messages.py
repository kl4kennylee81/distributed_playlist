from constants import *
from abc import ABCMeta, abstractmethod
import json
from request_messages import Request, deserialize_client_command_request

class CrashRequest(Request):
  msg_type = ForcedType.crash

  def __init__(self, tid):
    super(CrashRequest, self).__init__(tid, CrashRequest.msg_type)

# format vote NO
class VoteNoRequest(Request):
  msg_type = ForcedType.voteNo

  def __init__(self, tid): 
    super(VoteNoRequest, self).__init__(tid, VoteNoRequest.msg_type)


# format crashAfterVote
class CrashAfterVoteRequest(Request):
  msg_type = ForcedType.crashAfterVote

  def __init__(self, tid): 
    super(CrashAfterVoteRequest, self).__init__(tid, CrashAfterVoteRequest.msg_type)

# format crashAfterAck
class CrashAfterAckRequest(Request):
  msg_type = ForcedType.crashAfterAck

  def __init__(self, tid): 
    super(CrashAfterAckRequest, self).__init__(tid, CrashAfterAckRequest.msg_type)

# format is crashVoteREQ 2 3
class CrashVoteRequest(Request):
  msg_type = ForcedType.crashVoteReq

  def __init__(self, tid, sendTopid): 
    super(CrashVoteRequest, self).__init__(tid, CrashVoteRequest.msg_type)

    self.sendTopid = map(int,sendTopid)

# format is crashPartialPreCommit 2 3
class CrashPartialPrecommit(Request):
  msg_type = ForcedType.crashPartialPrecommit

  def __init__(self, tid, sendTopid): 
    super(CrashPartialPrecommit, self).__init__(tid, CrashPartialPrecommit.msg_type)

    self.sendTopid = map(int,sendTopid)

# format is crashPartialCommit 2 3
class CrashPartialCommit(Request):
  msg_type = ForcedType.crashPartialCommit

  def __init__(self, tid, sendTopid): 
    super(CrashPartialCommit, self).__init__(tid, CrashPartialCommit.msg_type)

    self.sendTopid = map(int,sendTopid)

# Deserialize the Client Request
def deserialize_client_request(msg_string, tid):
  msg_string = msg_string.strip()

  commandRequest = deserialize_client_command_request(msg_string, tid)
  if commandRequest is not None:
    return commandRequest
  # Trim white space, split, and clean of extra spacing

  # this is because the first request from the master client will have format "add song url", while
  # all other voteReqs attach "0,add song url"
  msg_array = msg_string.split(",")
  if len(msg_array) < 2:
    msg_list = msg_string.split(" ")
  else:
    msg_list = msg_array[1]
    msg_list = msg_list.split(" ")

  msg_list = filter(lambda a: a != '', msg_list)

  if msg_list[0].lower() == "crash":
    return CrashRequest(tid)
  elif msg_list[0].lower() == "vote":
    return VoteNoRequest(tid)
  elif msg_list[0].lower() == "crashaftervote":
    return CrashAfterVoteRequest(tid)
  elif msg_list[0].lower() == "crashafterack": 
    return CrashAfterAckRequest(tid)
  elif msg_list[0].lower() == "crashvotereq":
    return CrashVoteRequest(tid,msg_list[1:])
  elif msg_list[0].lower() == "crashpartialprecommit":
    return CrashPartialPrecommit(tid,msg_list[1:])
  elif msg_list[0].lower() == "crashpartialcommit":
    return CrashPartialCommit(tid,msg_list[1:])
  else:
    # Malformed message then
    return None



