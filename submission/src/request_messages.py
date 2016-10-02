from constants import *

# Master -> Server messages 
class Request(object):

  def __init__(self, tid, msg_type):
    self.tid = int(tid)
    self.type = msg_type

# Add 
class Add(Request): 
  msg_type = RequestType.add

  def __init__(self, tid, song_name, url):
    super(Add, self).__init__(tid, Add.msg_type)
    self.song_name = song_name
    self.url = url
    
  def serialize(self):
    return str(self.tid) + ",add " + self.song_name + " " + self.url

# Delete 
class Delete(Request): 
  msg_type = RequestType.delete

  def __init__(self, tid, song_name):
    super(Delete, self).__init__(tid, Delete.msg_type)
    self.song_name = song_name

  def serialize(self):
    return str(self.tid) + ",delete " + self.song_name

# Get 
class Get(Request): 
  msg_type = RequestType.get

  def __init__(self, tid, song_name):
    super(Get, self).__init__(tid, Get.msg_type)
    self.song_name = song_name

  def serialize(self):
    return str(self.tid) + ",get " + self.song_name


# Deserialize the Client Request 
def deserialize_client_command_request(msg_string, tid):
  # Trim white space, split, and clean of extra spacing
  msg_string = msg_string.strip()

  # this is because the first request from the master client will have format "add song url", while
  # all other voteReqs attach "0,add song url"
  msg_array = msg_string.split(",")
  if len(msg_array) < 2:
    msg_list = msg_string.split(" ")
  else:
    msg_list = msg_array[1]
    msg_list = msg_list.split(" ")

  msg_list = filter(lambda a: a != '', msg_list)

  print "msg list man", msg_list

  if msg_list[0].lower() == "add": 
    return Add(tid, msg_list[1], msg_list[2])
  elif msg_list[0].lower() == "delete":
    return Delete(tid, msg_list[1])
  elif msg_list[0].lower() == "get":
    return Get(tid, msg_list[1])
  else:
    return None

# Function to handle pulling historical client requests
# from the Transaction Log of a Process
def client_req_from_log(log_string):
  comma = log_string.find(',')
  tid = int(log_string[:comma])
  msg_string = log_string[comma + 1:]
  return deserialize_client_command_request(msg_string, tid)
