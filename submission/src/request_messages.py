from constants import *

# Master -> Server messages 
class Request(object):

  def __init__(self, pid, msg_type): 
    self.pid = int(pid)
    self.type = msg_type

# Add 
class Add(Request): 
  msg_type = RequestType.add

  def __init__(self, pid, song_name, url): 
    super(Add, self).__init__(pid, Add.msg_type)
    self.song_name = song_name
    self.url = url
    
  def serialize(self):
    return "add " + self.song_name + " " + self.url

# Delete 
class Delete(Request): 
  msg_type = RequestType.delete

  def __init__(self, pid, song_name): 
    super(Delete, self).__init__(pid, Delete.msg_type)
    self.song_name = song_name

  def serialize(self):
    return "delete " + self.song_name

# Get 
class Get(Request): 
  msg_type = RequestType.get

  def __init__(self, pid, song_name): 
    super(Get, self).__init__(pid, Get.msg_type)
    self.song_name = song_name

  def serialize(self):
    return "get " + self.song_name


# Deserialize the Client Request 
def deserialize_client_command_request(msg_string, pid): 
  # Trim white space, split, and clean of extra spacing 
  msg_string = msg_string.strip() 
  msg_list = msg_string.split(" ")
  msg_list = filter(lambda a: a != '', msg_list)

  if msg_list[0].lower() == "add": 
    return Add(pid, msg_list[1], msg_list[2])
  elif msg_list[0].lower() == "delete":
    return Delete(pid, msg_list[1])
  elif msg_list[0].lower() == "get":
    return Get(pid, msg_list[1])
  else:
    return None