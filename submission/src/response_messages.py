from constants import *

class ResponseCoordinator(object):

  def __init__(self, pid): 
    self.pid = pid 

  def serialize(self):
    s = "coordinator {}\n".format(self.pid)
    return s

class ResponseGet(object):

  def __init__(self):
    self.url = None

  def __init__(self, url): 
    self.url = url

  def serialize(self):
    if self.url == None:
      resp = "NONE"
    else:
      resp = self.url

    s = "resp {}\n".format(resp)
    return s

class ResponseAck(object):

  def __init__(self,decision):
    self.decision = decision


  def serialize(self):
    if self.decision == Decide.commit:
      decision_str = "commit"
    elif self.decision == Decide.abort:
      decision_str = "abort"
    s = "ack {}\n".format(decision_str)
    return s

