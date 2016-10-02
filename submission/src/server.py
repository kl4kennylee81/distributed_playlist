import sys, os
import subprocess
import time
from threading import Thread, Lock, RLock
import socket
from socket import SOCK_STREAM, AF_INET

from constants import *
from collections import deque

# In-house messaging support
from messages import *
# Response messages to the client
from responseMessages import *
# Stable storage writing support
import storage


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
    }

  def run(self):
    """
    Captures messages on the socket every iteration.
    Calls corresponding handler.
    """
    while True:
      data = self.master_conn.recv(BUFFER_SIZE)
      deserialized = deserialize_client_req(data, self.server.pid, self.server.getTid())
      self.handlers[deserialized.type](deserialized, self.server)

  def _get_handler(self, deserialized, server):
    """
    Processes GET request of data and responds to the master client.
    :param deserialized: deserialized Message
    :param server: an instance of any process
    """
    with server.global_lock:
      # deliver back to the master client
      url = server.getUrl(deserialized.song_name)
      url_resp = ResponseGet(url)
      self.send(url_resp.serialize())

  def _add_handler(self, deserialized, server):
    """
    Begins 3-Phase-Commit for the addition of a song
    :param deserialized: deserialized Message
    :param server: an instance of the COORDINATOR
    """
    with server.global_lock:
      server.add_request(deserialized)
      server.setCoordinatorState(CoordinatorState.votereq)

      # Compose VOTE-REQ, log, and send to all participants
      voteReq = VoteReq(server.pid, server.getTid(), deserialized.serialize())
      server.storage.write_dt_log(voteReq.serialize())
      server.broadCastMessage(voteReq)


  def _delete_handler(self, deserialized, server):
    """
    Begins 3-Phase-Commit for the deletion of a song
    :param deserialized: deserialized Message
    :param server: an instance of the COORDINATOR
    """
    with server.global_lock:
      server.add_request(deserialized)
      server.setCoordinatorState(CoordinatorState.votereq)
      voteReq = VoteReq(server.pid, server.getTid(), deserialized.serialize())
      server.broadCastMessage(voteReq)

  def send(self, s):
    """
    Sends response to master client
    :param s: message string
    """
    with self.server.global_lock:
      self.master_conn.send(str(s))


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

    # Handlers for timeouts
    # TODO: Hong and Joe
    self.parti_failureHandler = {}
    self.coord_failureHandler = {}

    # Silencing warnings
    self.server = None
    self.conn = None
    self.valid = None

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

  def run(self):
    # TODO: Add timeout to sockets
    # TODO: Discuss need to send PID @ beginning of this connection's life
    id_msg = Identifier(self.server.pid, self.server.getTid())
    self.send(id_msg.serialize())
    # I commented out the try except for debugging
    # try:
    while self.valid:

      # Receive data on socket, deserialize it, and log it
      data = self.conn.recv(BUFFER_SIZE)
      msg = deserialize_message(str(data))
      self.server.storage.write_debug(data)

      # Handle based on whether the process is a coordinator or a
      # participant
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

  def coordinatorStateFailureHandler(self, server):
    try:
      self.coord_failureHandler[server.getCoordinatorState()](server)
    except KeyError, e:
      self.server.storage.write_debug(str(e) + "\n[^] Invalid coordinator state")

  def participantStateFailureHandler(self, server):
    try:
      self.parti_failureHandler[server.getState()](server)
    except KeyError, e:
      self.server.storage.write_debug(str(e) + "\n[^] Invalid participant state")


  def _wait_votes_failure(self):

    # mark to state abort and wakeup the main process
    # to send out abort to everyone
    # the master if he is woken up and for example
    # his normal state is changed he will
    # do something dependent on his coordinator state AND his Normal State
    # the participants act only based on their normal state

    abort = Decision(self.server.pid, self.server.getTid(), Decide.abort)
    self.server.setCoordinatorState(CoordinatorState.standby)
    self.server.broadCastMessage(abort)

  def _wait_ack_failure(self):
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
  # TODO: Think about this really hard
  def coordinatorRecv(self, msg):
    # do error handling if key error that means it got 
    # a message it was not suppose to
    self.coordinator_handlers[msg.type](msg)

  def participantRecv(self, msg):
    # do error handling if key error that means it got 
    # a message it was not suppose to
    self.participant_handlers[msg.type](msg)

  # TODO: Check possible error?
  def _idHandler(self, msg):
    with self.server.global_lock:
      self.server.other_procs[msg.pid] = self

  # Participant recieved messages votereq, precommit, decision #
  def _voteReqHandler(self, msg):
    with self.server.global_lock:
      if self.server.getState() == State.aborted or self.server.getState() == State.committed:
        self.server.setState(State.uncertain)

        # in the recovery mode you would check the response id (rid)
        # of the voteReq and also update your state to become consistent


        # Deserialize request and add it to the request queue
        request = deserialize_client_req(msg.request, self.server.pid, self.server.getTid())
        self.server.add_request(request)

        # Log that we voted yes + then vote yes
        yesVote = Vote(self.server.pid, self.server.getTid(), Choice.yes)
        yesVoteSerialized = yesVote.serialize()
        self.server.storage.write_dt_log(yesVoteSerialized)
        self.send(yesVoteSerialized)


  def _preCommitHandler(self, msg):
    with self.server.global_lock:
      if self.server.getState() == State.uncertain:
        self.server.setState(State.committable)

        ackRes = Ack(self.server.pid, self.server.getTid())
        self.send(ackRes.serialize())


  def _decisionHandler(self, msg):
    with self.server.global_lock:
      if self.server.getState() == State.committable:
        self.server.commit_cur_request()
      else:
        # maybe raise an error not sure? log something maybe
        pass

  # coordinator received messages vote, acks
  def _voteHandler(self, msg):
    with self.server.global_lock:
      if self.server.getCoordinatorState() == CoordinatorState.votereq:
        self.server.decrementResponsesNeeded()
        if self.server.hasAllResponsesNeeded():
          self.server.setCoordinatorState(CoordinatorState.precommit)
          
          precommit = PreCommit(self.server.pid, self.server.getTid())
          self.server.broadCastMessage(precommit)

        
  def _ackHandler(self, msg):
    with self.server.global_lock:
      if self.server.getCoordinatorState() == CoordinatorState.precommit:
        self.server.decrementResponsesNeeded()

        if self.server.hasAllResponsesNeeded():
          self.server.coordinator_commit_cur_request()
          
          d = Decide.commit
          decision = Decision(self.server.pid, self.server.getTid(), d)

          self.server.broadCastMessage(decision)
          respAck = ResponseAck(d)

          self.server.master_thread.send(respAck.serialize())


  def send(self, s):
    with self.server.global_lock:
      if self.valid:
        self.conn.send(str(s))


  def close(self):
    try:
      self.valid = False
      self.conn.close()
    except socket.error, e:
      self.server.storage.write_debug(str(e) + "\n[^] Socket error")


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

    # this is what all the other participants connect to
    self.internal_server = socket.socket(AF_INET, SOCK_STREAM) 
    self.internal_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.internal_server.bind((ADDRESS, free_port_no))
    print "Initialized server socket at port {}".format(free_port_no)

  def run(self):
    while True:
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

    # all the instance variables in here are "global states" for the server
    self.leader = leader

    # the pid is immutable you should never change this value after
    # the process starts. can access without a lock
    self.pid = pid

    # Global State : global lock
    self.global_lock = RLock()

    # Global State: the current request id this server is working on
    self.tid = 0

    # Global State: a queue of requests to process
    self.request_queue = deque()

    # Global State: current phase of the coordinator's 3 phase commit
    self.coordinator_state = CoordinatorState.standby

    # Global State: current state for participants
    self.state = State.aborted

    # Global state: this is the threads currently active on the request id
    self.cur_request_set = set()

    # Stable storage for this server
    self.storage = storage.Storage(self.pid)

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

    for i in range(n):
      try:
        port_to_connect = START_PORT + i
        cur_handler = ClientConnectionHandler.fromAddress(ADDRESS, 
                                                          port_to_connect, 
                                                          self)
        cur_handler.start()
      except:
        # only connect to the sockets that are active
        continue

  def setResponsesNeeded(self):
    with self.global_lock:
      self.responsesNeeded = len(self.cur_request_set)

  def decrementResponsesNeeded(self):
    with self.global_lock:
      if self.getResponsesNeeded() > 0:
        self.responsesNeeded-=1

  def getResponsesNeeded(self):
    with self.global_lock:
      return self.responsesNeeded

  def hasAllResponsesNeeded(self):
    with self.global_lock:
      return self.responsesNeeded == 0

  def incrementTid(self):
    with self.global_lock:
      self.tid +=1

  def getTid(self):
    with self.global_lock:
      return self.tid

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
      self.coordinator_state = newCoordState
      if newCoordState == CoordinatorState.standby:
        self.setState(State.aborted)
        self.resetState()
        
      elif newCoordState == CoordinatorState.votereq:
        self.incrementTid()
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
      newAliveSet = set()
      for pid,proc in self.other_procs.iteritems():
        newAliveSet.add(proc)
      self.cur_request_set = newAliveSet

  def setState(self,newState):
    with self.global_lock:
      self.state = newState

  def newParticipant(self, pid, new_thread):
    with self.global_lock:
      self.other_procs[pid] = new_thread

  def add_request(self,request):
    with self.global_lock:
      self.request_queue.append(request)

  def getUrl(self,songname):
    with self.global_lock:
      if songname in self.playlist:
        return self.playlist[songname]
      return None

  def broadCastMessage(self,msg):
    with self.global_lock:
      for proc in self.cur_request_set:
        serialized = msg.serialize()
        proc.send(serialized)

  def commit_cur_request(self):
    with self.global_lock:
      current_request = self.request_queue.popleft()
      if current_request != None:
        self.commandRequestExecutors[current_request.msg_type](current_request)
        self.state = State.committed

  def coordinator_commit_cur_request(self):
    with self.global_lock:
      self.coordinator_state = CoordinatorState.completed
      self.commit_cur_request()

  def _add_commit_handler(self,request):
    with self.global_lock:
      song_name, url = request.song_name, request.url
      self.storage.add_song(song_name, url)
      self.playlist[song_name] = url

  def _delete_commit_handler(self,request):
    print("{} is deleting the entry".format(self.pid))
    with self.global_lock:
      song_name = request.song_name
      self.storage.delete_song(song_name)
      del self.playlist[song_name]


  def resetState(self):
    with self.global_lock:
      self.responsesNeeded = sys.maxint
      self.state = State.aborted
      self.coordinator_state = CoordinatorState.standby


