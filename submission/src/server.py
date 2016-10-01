import sys, os
import subprocess
import time
from threading import Thread, Lock, RLock
import socket
from socket import SOCK_STREAM, AF_INET
from messages import *
from response_messages import *
from request_messages import *
from constants import *
from crash_request_messages import *
from collections import deque


BUFFER_SIZE = 1024
START_PORT = 20000
ADDRESS = "localhost"


class MasterClientHandler(Thread):
  """
  A special client thread that services requests from the master client 
  ex. the application requests
  """

  def __init__(self, master_conn, server):
    Thread.__init__(self)
    self.master_conn = master_conn
    self.server = server
    self.handlers = {
      Add.msg_type: self._add_handler,
      Get.msg_type: self._get_handler,
      Delete.msg_type: self._delete_handler,
      CrashRequest.msg_type: self._crash_handler,
      VoteNoRequest.msg_type: self._voteNo_handler,
      CrashAfterVoteRequest.msg_type: self._crashAfterVote_handler,
      CrashAfterAckRequest.msg_type: self._crashAfterAck_handler,
      CrashVoteRequest.msg_type: self._crashVoteRequest_handler,
      CrashPartialPrecommit.msg_type: self._crashPartialPrecommit_handler,
      CrashPartialCommit.msg_type: self._crashPartialCommit_handler,
    }

  def run(self):
    while self.isValid():
      # TODO: logic for handling application requests
      data = self.master_conn.recv(BUFFER_SIZE)

      self._parse_data(data)

  def isValid(self):
    with self.server.global_lock:
      return self.server.isValid()

  def _parse_data(self, data):
    deserialized = deserialize_client_request(data, self.server.pid)
    self.handlers[deserialized.type](deserialized, self.server)

  def _add_handler(self, deserialized, server):
    with server.global_lock:
      server.add_request(deserialized)
      server.setCoordinatorState(CoordinatorState.votereq)
      voteReq = VoteReq(server.pid, deserialized.serialize())

      server.broadCastMessage(voteReq)

  def _get_handler(self, deserialized, server):
    with server.global_lock:
      # deliver back to the master client
      url = server.getUrl(deserialized.song_name)
      url_resp = ResponseGet(url)

      self.send(url_resp.serialize())

  def _delete_handler(self, deserialized, server):
    with server.global_lock:
      server.add_request(deserialized)
      server.setCoordinatorState(CoordinatorState.votereq)

      voteReq = VoteReq(server.pid, deserialized.serialize())

      server.broadCastMessage(voteReq)

  def send(self, s):
    with self.server.global_lock:
      self.master_conn.send(str(s))

  def _crash_handler(self, deserialized, server):
    print("we made it crash")
    self.server.add_crash_request(deserialized)

  def _voteNo_handler(self,deserialized,server):
    print("we made it vote no")
    self.server.add_voteNo_request(deserialized)

  def _crashAfterVote_handler(self,deserialized,server):
    print("we made it crash after vote")
    self.server.add_crashAfterVote_request(deserialized)

  def _crashAfterAck_handler(self,deserialized,server):
    print("we made it crash after ack")
    self.server.add_crashAfterAck_request(deserialized)

  def _crashVoteRequest_handler(self,deserialized,server):
    print("we made it crash vote req")
    self.server.add_crashVoteReq_request(deserialized)

  def _crashPartialPrecommit_handler(self,deserialized,server):
    print("we made it crash partial precommit")
    self.server.add_crashPartialPrecommit(deserialized)

  def _crashPartialCommit_handler(self,deserialized,server):
    print("we made it crash partial commit")
    self.server.add_crashPartialCommit(deserialized)


class ClientConnectionHandler(Thread):
  """
  Internal server process has an associated client handler thread to handle the 
  request from other internal servers 
  ex. the votes/decisions/precommits etc.
  """

  def __init__(self):
    Thread.__init__(self)
    self.participant_handlers = {
      # These are coordinator sent messages ###
      VoteReq.msg_type: self._voteReqHandler,
      PreCommit.msg_type: self._preCommitHandler, 
      Decision.msg_type: self._decisionHandler,
      Identifier.msg_type: self._idHandler,

      #### These are for the special features ####
      # Recover.msg_type: Recover, 
      # Reelect.msg_type: Reelect,
      # StateReq.msg_type: StateReq,
      # StateRepid.msg_type: StateRepid
    }

    self.coordinator_handlers = {
      Vote.msg_type: self._voteHandler,
      Ack.msg_type: self._ackHandler,
      Identifier.msg_type: self._idHandler,

      #### These are for the special features ####
      # Recover.msg_type: Recover, 
      # Reelect.msg_type: Reelect,
      # StateReq.msg_type: StateReq,
      # StateRepid.msg_type: StateRepid
    }

    self.parti_failureHandler = {
    }

    self.coord_failureHandler = {
    }

  @classmethod
  def fromConnection(cls, conn, server):
    result = cls()
    result.conn = conn
    result.valid = True
    result.server = server
    return result

  @classmethod
  def fromAddress(cls, address, port, server):
    result = cls()
    result.conn = socket.socket(AF_INET, SOCK_STREAM) 
    result.conn.connect((address, port))
    result.valid = True
    result.server = server
    return result

  def isValid(self):
    with self.server.global_lock:
      return self.valid and self.server.isValid()

  def run(self):
    id_msg = Identifier(self.server.pid)
    self.send(id_msg.serialize())
    # I commented out the try except for debugging
    # try:
    while self.isValid():
      data = self.conn.recv(BUFFER_SIZE)

      msg = deserialize_message(str(data))

      if self.server.getLeader():
        self.coordinatorRecv(msg)
      else:
        self.participantRecv(msg)
      # #
      # # ADD THE exception logic for timeouts here
      # # This will be where it will run termination protocol
      # # and the reelection
      # # if it timeout from the coordinator
      # # need a local copy of who he thinks is
      # # the coordinator
      # except:
      #   # create a state handler here to handle failures
      #   # for each state, one for coordinator handler failure
      #   # and one for partcipants handler for his state
      #   # for example coordinator in wait_acks on a failure
      #   # would delete the alive guy and increment the count
      #   # of acks responded

      #   if (self.server.getLeader()):
      #     self.coordinatorStateFailureHandler(self.server)
      #   else:
      #     self.participantStateFailureHandler(self.server)

      #   self.valid = False
      #   return
      #   # remove self from alive list
      #   # possibly when other procs is pid -> channel
      #   # you make the pid key -> None, until some
      #   # recovered new channel thread takes its place

      #   # handle network failure, timeouts etc
      #   # close the channel
      #   # return the thread

  def coordinatorStateFailureHandler(self,server):
    if server.getCoordinatorState() in self.coord_failureHandler:
      coord_failureHandler[server.getCoordinatorState()](server)

  def participantStateFailureHandler(self,server):
    if server.getState() in self.parti_failureHandler:
      parti_failureHandler[server.getState()](server)

  def _wait_votes_failure(server):

    # mark to state abort and wakeup the main process
    # to send out abort to everyone
    # the master if he is woken up and for example
    # his normal state is changed he will
    # do something dependent on his coordinator state AND his Normal State
    # the participants act only based on their normal state
    decision = Decision(server.pid,Decide.abort)

    server.setCoordinatorState(CoordinatorState.standby)

    server.broadCastMessage(decision)

  def _wait_ack_failure(server):
    pass

    # don't mark state as aborted and update the ack_phase_responded+=1
    # take this thread/socket/pid out of the alive set and set the other_procs
    # for this pid to map to None

# '''
#   Coordinator recieve only vote and acks from internal
#   partcipants
#   TODO consider when a coordinator dies and comes back
#   and new one is elected how will it recognize its no
#   longer the coordinator
# '''
  def coordinatorRecv(self,msg):
    # do error handling if key error that means it got 
    # a message it was not suppose to
    self.coordinator_handlers[msg.type](msg,self.server)

  def participantRecv(self,msg):
    # do error handling if key error that means it got 
    # a message it was not suppose to
    self.participant_handlers[msg.type](msg,self.server)


  def _idHandler(self,msg,server):
    with server.global_lock:
      server.other_procs[msg.pid] = self

  # Participant recieved messages votereq,precommit,decision #
  def _voteReqHandler(self,msg,server):
    with server.global_lock:
      if (server.getState() == State.aborted or server.getState() == State.committed):
        server.setState(State.uncertain)

        # in the recovery mode you would check the response id (rid)
        # of the voteReq and also update your state to become consistent

        request = deserialize_client_request(msg.request,server.pid)
        server.add_request(request)

        voteRes = Vote(server.pid,Choice.yes)
        self.send(voteRes.serialize())

  def _preCommitHandler(self,msg,server):
    with server.global_lock:
      if (server.getState() == State.uncertain):
        server.setState(State.committable)

        ackRes = Ack(server.pid)
        self.send(ackRes.serialize())


  def _decisionHandler(self,msg,server):
    with server.global_lock:
      if (server.getState() == State.committable):
        server.commit_cur_request()
      else:
        # maybe raise an error not sure? log something maybe
        pass

  # coordinator received messages vote,acks
  def _voteHandler(self,msg,server):
    with server.global_lock:
      if (server.getCoordinatorState() == CoordinatorState.votereq):
        server.decrementResponsesNeeded()
        if server.hasAllResponsesNeeded():
          server.setCoordinatorState(CoordinatorState.precommit)
          
          precommit = PreCommit(server.pid)
          server.broadCastMessage(precommit)

        
  def _ackHandler(self,msg,server):
    with server.global_lock:
      if (server.getCoordinatorState() == CoordinatorState.precommit):
        server.decrementResponsesNeeded()

        if server.hasAllResponsesNeeded():
          server.coordinator_commit_cur_request()
          
          d = Decide.commit
          decision = Decision(server.pid,d)
          server.broadCastMessage(decision)

          respAck = ResponseAck(d)

          server.master_thread.send(respAck.serialize())


  def send(self, s):
    with self.server.global_lock:
        if self.isValid():
          self.conn.send(str(s))

  def close(self):
      try:
          self.valid = False
          self.conn.close()
      except:
          pass


class ServerConnectionHandler(Thread):
  """
  Handles Incoming connection on the internal facing socket
  That will come from other participant processes. This will create
  a ClientConnectionHandler Thread to then service the requests from the
  participant process.

  This is the server socket that newly started partcipant processes will
  attempt to connect to.
  """

  def __init__(self, free_port_no, server):
    Thread.__init__(self)
    self.server = server
    self.valid = True

    # this is what all the other participants connect to
    self.internal_server = socket.socket(AF_INET, SOCK_STREAM) 
    self.internal_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.internal_server.bind((ADDRESS, free_port_no))
    print "Initialized server socket at port {}".format(free_port_no)

  def isValid(self):
    with self.server.global_lock:
      return self.server.isValid()

  def run(self):
    while self.isValid():
      self.internal_server.listen(0)
      (conn, (ip,port)) = self.internal_server.accept()
      print("Pid {}: Connection Accepted".format(self.server.pid))

      with self.server.global_lock:
        new_client_thread = ClientConnectionHandler.fromConnection(conn, self.server)
        new_client_thread.start()


class Server:
  """
  use port 20000+i for each process' server socket, where i is the process id. 
  Each process will search ports between 20000 and 20000+n-1 to see which
  process is alive. To then connect to.

  The server is passed onto all the handler threads because it essentially acts
  as a container for the "global state" of the processes server. Each of the 
  handler needs to reference the server "Global" to get the current state of
  this participant and to interact and work collectively
  """

  """
  Base Class for Server to extend to Coordinators and Participants

  Fields:
  other_procs: A dict of pid -> socket channel

  other_procs_lock: mutex for other_procs

  pid: The unique process id for this server

  server: The server socket that is listening on connection requests
  to create new sockets for incoming connections
  """
  
  def __init__(self, pid, n, port, leader):
    self.commandRequestExecutors = {
        Add.msg_type: self._add_commit_handler,
        Delete.msg_type: self._delete_commit_handler,
    }

    # the pid is immutable you should never change this value after
    # the process starts. can access without a lock
    self.pid = pid

    # Global State : global lock
    self.global_lock = RLock()

    # Global State : coordinator
    # all the instance variables in here are "global states" for the server
    self.leader = leader

    # Global State : is the whole server still active or crashing
    # when False join all the threads and then return
    # OR do we just fail ungracefully and just do a sys exit
    self.valid = True

    # Global State: current phase of the coordinator's 3 phase commit
    self.coordinator_state = CoordinatorState.standby

    # Global State: current state for participants
    self.state = State.aborted

    # Global state: this is the threads currently active on the request id
    self.cur_request_set = set()

    # Global State:
    # Vote Phase: all vote yes
    # decrement on each incoming vote yes
    # Ack Phase: respone ack + timeouts
    # decrement on each ack or timeouts
    self.responsesNeeded = sys.maxint
    
    # Global State:
    # playlist is the global dictionary mapping
    # songname -> url
    self.playlist = dict()

    # Global State:
    # update other procs to be a mapping from pid -> thread
    self.other_procs = dict()

    # GLOBAL FOR REQUEST COMMANDS
    # Global State: the current request id this server is working on
    self.rid = 0

    # Global State: a queue of requests to process
    self.request_queue = deque()

    # CRASHING COMMAND GLOBAL QUEUES
    self.crash_queue = deque()

    # PARTCIPANT CRASH QUEUE
    self.voteNo_queue = deque()

    self.crashAfterVote_queue = deque()

    self.crashAfterAck_queue = deque()

    # COORDINATOR CRASH QUEUE
    self.crashVoteReq_queue = deque()

    self.crashPartialPrecommit_queue = deque()

    self.crashPartialCommit_queue = deque()


    self.master_server = socket.socket(AF_INET, SOCK_STREAM) 
    self.master_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.master_server.bind((ADDRESS, port))

    # wait synchronously for the master to connect
    self.master_server.listen(1)
    (master_conn, (ip, master_client_port)) = self.master_server.accept()
    self.master_thread = MasterClientHandler(master_conn, self)
    self.master_thread.start()

    # this is the socket that all the other internal participants connect to
    free_port_no = START_PORT + self.pid
    self.internal_server = ServerConnectionHandler(free_port_no, self)
    self.internal_server.start()

    for i in range(self.pid):
      try:
        port_to_connect = START_PORT + i
        cur_handler = ClientConnectionHandler.fromAddress(ADDRESS, 
                                                          port_to_connect, 
                                                          self)
        cur_handler.start()
      except:
        print sys.exc_info()
        print("Error connecting to port: {}".format(port_to_connect))

  def isValid(self):
    with self.global_lock:
      return self.valid

  def setResponsesNeeded(self):
    with self.global_lock:
      if self.isValid():
        self.responsesNeeded = len(self.cur_request_set)

  def decrementResponsesNeeded(self):
    with self.global_lock:
      if self.isValid():
        if self.getResponsesNeeded() > 0:
          self.responsesNeeded-=1

  def getResponsesNeeded(self):
    with self.global_lock:
      return self.responsesNeeded

  def hasAllResponsesNeeded(self):
    with self.global_lock:
      if self.isValid():
        return self.responsesNeeded == 0
      return False

  def incrementRid(self):
    with self.global_lock:
      self.rid +=1

  def getRid(self):
    with self.global_lock:
      return self.rid

  def setLeader(self,isLeader):
    with self.global_lock:
      self.leader = isLeader

  def getLeader(self):
    with self.global_lock:
      return self.leader

  def getCoordinatorState(self):
    with self.global_lock:
      return self.coordinator_state

  def getState(self):
    with self.global_lock:
      return self.state

  def setCoordinatorState(self,newCoordState):
    with self.global_lock:
      if self.isValid():
        self.coordinator_state = newCoordState
        if newCoordState == CoordinatorState.standby:
          self.setState(State.aborted)
          self.resetState()
          
        elif newCoordState == CoordinatorState.votereq:
          self.incrementRid()
          self.setState(State.uncertain)

          self.setCurRequestProcesses()

          self.setResponsesNeeded()
        elif newCoordState == CoordinatorState.precommit:
          self.setState(State.committable)
          self.setResponsesNeeded()
        elif newCoordState == CoordinatorState.completed:
          self.setState(State.committed)

  def setCurRequestProcesses(self):
    with self.global_lock:
      if self.isValid():
        newAliveSet = set()
        for pid,proc in self.other_procs.iteritems():
          newAliveSet.add(proc)
        self.cur_request_set = newAliveSet

  def setState(self,newState):
    with self.global_lock:
      if self.isValid():
        self.state = newState

  def newParticipant(pid,new_thread):
    with self.global_lock:
      if self.isValid():
        self.server.other_procs[pid] = new_client_thread


  def add_request(self,request):
    with self.global_lock:
      if self.isValid():
        self.request_queue.append(request)


  def add_crash_request(self,request):
    with self.global_lock:
      # not sure if we need to queue
      # up anything since you just fail
      # automatically maybe a kill signal
      # is all we need
      self.crash_queue.append(request)

  def add_voteNo_request(self,request):
    with self.global_lock:
      self.voteNo_queue.append(request)

  def add_crashAfterVote_request(self,request):
    with self.global_lock:
      self.crashAfterVote_queue.append(request)

  def add_crashAfterAck_request(self,request):
    with self.global_lock:
      self.crashAfterAck_queue.append(request)

  def add_crashVoteReq_request(self,request):
    with self.global_lock:
      self.crashVoteReq_queue.append(request)

  def add_crashPartialPrecommit(self,request):
    with self.global_lock:
      self.crashPartialPrecommit_queue.append(request)

  def add_crashPartialCommit(self,request):
    with self.global_lock:
      self.crashPartialCommit_queue.append(request)


  def getUrl(self,songname):
    with self.global_lock:
      if self.isValid():
        if songname in self.playlist:
          return self.playlist[songname]
        return None

  def broadCastMessage(self,msg):
    with self.global_lock:
      if self.isValid():
        for proc in self.cur_request_set:
          serialized = msg.serialize()
          proc.send(serialized)

  def commit_cur_request(self):
    with self.global_lock:
      if self.isValid():
        current_request = self.request_queue.popleft()
        if current_request != None:
          self.commandRequestExecutors[current_request.msg_type](current_request)
          self.state = State.committed

  def coordinator_commit_cur_request(self):
    with self.global_lock:
      if self.isValid():
          self.coordinator_state = CoordinatorState.completed
          self.commit_cur_request()

  def _add_commit_handler(self,request):
    with self.global_lock:
      if self.isValid():
          songname,url = request.song_name,request.url
          self.playlist[songname] = url

  def _delete_commit_handler(self,request):
    with self.global_lock:
      if self.isValid():
          songname = request.song_name
          del self.playlist[songname]


  def resetState(self):
    with self.global_lock:
      self.responsesNeeded = sys.maxint
      self.state = State.aborted
      self.coordinator_state = CoordinatorState.standby

  def exit(self):
    pass
    # We eithe issue a signal kill
    # or we join all the threads adn fail gracefully
    
    



