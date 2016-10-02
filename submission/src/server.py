import sys, os
import subprocess
import time
from threading import Thread, Lock, RLock
import socket
from socket import SOCK_STREAM, AF_INET
from constants import *
from crash_request_messages import *
from collections import deque

# In-house messaging support
from messages import *
# Request messages to the client
from request_messages import *
# Response messages to the client
from response_messages import *
# Stable storage writing support
import storage


BUFFER_SIZE = 1024
START_PORT = 20000
ADDRESS = "localhost"
TIMEOUT_SECS = 5.0


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
    """
    Captures messages on the socket every iteration.
    Calls corresponding handler.
    """
    while self.isValid() :
      data = self.master_conn.recv(BUFFER_SIZE)
      deserialized = deserialize_client_req(data, self.server.pid, self.server.getTid())
      self.handlers[deserialized.type](deserialized, self.server)


  def isValid(self):
    with self.server.global_lock:
      return self.server.isValid()


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

  def _transaction_handler(self,deserialized,server):
    with server.global_lock:
      server.add_request(deserialized)
      server.setCoordinatorState(CoordinatorState.votereq)
      voteReq = VoteReq(server.pid, deserialized.serialize())

      crashAfterVoteReq = server.pop_crashVoteReq_request()
      if crashAfterVoteReq != None:
        server.broadCastMessage(voteReq,crashAfterVoteReq.sendTopid)
        server.exit()
      else:
        server.broadCastMessage(voteReq)


  def send(self, s):
    """
    Sends response to master client
    :param s: message string
    """
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
    self.client_pid = -1
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
    self.parti_failureHandler = {
      State.aborted: self._voteReq_timeout,
      State.uncertain: self._preCommit_timeout,
      State.committable: self._commit_timeout,
    }

    self.coord_failureHandler = {
      CoordinatorState.votereq: self._vote_timeout,
      CoordinatorState.precommit: self._ack_timeout,
    }

    # Silencing warnings
    self.server = None
    self.conn = None
    self.valid = None

    # pid of the server on the other side of the socket.
    # Determined after first messages are sent.
    self.connection_pid = None


  @classmethod
  def fromConnection(cls, conn, server):
    result = cls()
    result.conn = conn
    result.conn.settimeout(TIMEOUT_SECS)
    result.valid = True
    result.server = server
    return result


  @classmethod
  def fromAddress(cls, address, port, server):
    result = cls()
    result.conn = socket.socket(AF_INET, SOCK_STREAM) 
    result.conn.connect((address, port))
    result.conn.settimeout(TIMEOUT_SECS)
    result.valid = True
    result.server = server
    return result


  def setClientPid(self,client_pid):
    with self.server.global_lock:
      self.client_pid = client_pid

  def getClientPid(self):
    with self.server.global_lock:
      return self.client_pid

  def isValid(self):
    with self.server.global_lock:
      return self.valid and self.server.isValid()

  def run(self):
    # TODO: Discuss need to send PID @ beginning of this connection's life
    id_msg = Identifier(self.server.pid, self.server.getTid(), self.server.isLeader())
    self.send(id_msg.serialize())

    try:
      while self.valid:
        data = self.conn.recv(BUFFER_SIZE)
        msg = deserialize_message(str(data))
        self.server.storage.write_debug(data)

        if self.server.getLeader():
          self._coordinatorRecv(msg)
        else:
          self._participantRecv(msg)

    except socket.timeout, e:
      self.server.storage.write_debug(str(e) + "\n[^] Timeout error")

      self.valid = False # TODO: why do we need self.valid?
      self.server.other_procs[self.connection_pid] = None
      self.connection_pid = None
      self.conn.close()

      if self.server.getLeader():
        self._coordinator_timeout_handler()
      else:
        self._participant_timeout_handler()


      # if it timeout from the coordinator
      # need a local copy of who he thinks is
      # the coordinator

      # create a state handler here to handle failures
      # for each state, one for coordinator handler failure
      # and one for partcipants handler for his state
      # for example coordinator in wait_acks on a failure
      # would delete the alive guy and increment the count
      # of acks responded

      # remove self from alive list
      # possibly when other procs is pid -> channel
      # you make the pid key -> None, until some
      # recovered new channel thread takes its place


  def _coordinator_timeout_handler(self):
    try:
      self.coord_failureHandler[self.server.getCoordinatorState()]()
    except KeyError, e:
      self.server.storage  .write_debug(str(e) + "\n[^] Invalid coordinator state")


  def _participant_timeout_handler(self):
    try:
      self.parti_failureHandler[self.server.getState()]()
    except KeyError, e:
      self.server.storage.write_debug(str(e) + "\n[^] Invalid participant state")


  """ Participant timeout handlers """
  def _voteReq_timeout(self):
    """
    If a process times out waiting for voteReq, unilaterally abort.
    """
    with self.server.global_lock:
      abort = Decision(self.server.pid, self.server.getTid(), Decision.abort)
      abortSerialized = abort.serialize()
      self.server.storage.write_dt_log(abortSerialized)


  def _preCommit_timeout(self):
    pass


  def _commit_timeout(self):
    pass


  """ Coordinator timeout handlers """
  def _vote_timeout(self):
    pass


  def _ack_timeout(self):
    pass


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
  def _coordinatorRecv(self, msg):
    # do error handling if key error that means it got 
    # a message it was not suppose to
    self.coordinator_handlers[msg.type](msg)

  def _participantRecv(self, msg):
    # do error handling if key error that means it got 
    # a message it was not suppose to
    self.participant_handlers[msg.type](msg)

  def _idHandler(self, msg):
    with self.server.global_lock:
      self.server.other_procs[msg.pid] = self
      self.connection_pid = msg.pid

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
        self.send(yesVoteSerialized)  # if participant crash
        afterVoteCrash = self.server.pop_crashAfterVote_request()

        # TODO change this if we keep track of the leader to
        # check if current leader == his own pid in a helper function
        # is leader
        if afterVoteCrash != None and not self.server.isLeader():
          self.server.exit()

  def _preCommitHandler(self, msg):
    with self.server.global_lock:
      if self.server.getState() == State.uncertain:
        self.server.setState(State.committable)

        ackRes = Ack(self.server.pid, self.server.getTid())
        self.send(ackRes.serialize())

        crashAfterAck = self.server.pop_crashAfterAck_request()
        if crashAfterAck != None and not self.server.isLeader():
          self.server.exit()

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

          crashPartialPreCommit = self.server.pop_crashPartialPrecommit()
          if crashPartialPreCommit is not None:
            self.server.broadCastMessage(precommit, self.server.crashPartialPrecommit.sendTopid)
            self.server.exit()
          else:
            self.server.broadCastMessage(precommit)


        
  def _ackHandler(self, msg):
    with self.server.global_lock:
      if self.server.getCoordinatorState() == CoordinatorState.precommit:
        self.server.decrementResponsesNeeded()

        if self.server.hasAllResponsesNeeded():
          self.server.coordinator_commit_cur_request()
          
          d = Decide.commit
          decision = Decision(self.server.pid, self.server.getTid(), d)

          crashPartialCommit = self.server.pop_crashPartialCommit()

          if crashPartialCommit is not None:
            self.server.broadCastMessage(decision,crashPartialCommit.sendTopid)
            self.server.exit()
          else:
            self.server.broadCastMessage(decision)
            respAck = ResponseAck(d)

            self.server.master_thread.send(respAck.serialize())

          # TODO check if crash partial commit
          # do we send the ack to the master before or after
          # sending the partial broadcast. that is if there is
          # a partial broadcast do we notify the master only if
          # commit have been sent to everyone

  def send(self, s):
    with self.server.global_lock:
      if self.isValid():
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

  def __init__(self, pid, n, port, isLeader):
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
    
    # leader flag will be set by the initial identifier message sent
    # than it will be set by the recepient participant
    # this allows this field to be updated to the current leader

    # leader: the process id of the current leader
    self.leader = -1
    if isLeader:
      self.leader = self.pid

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

    # GLOBAL FOR REQUEST COMMANDS
    # Global State: the current transaction id this server is working on
    self.tid = 0

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

    # Transaction history
    self.transaction_history = self.storage.get_transcations()

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

  def isLeader(self):
    with self.global_lock:
      return self.getLeader() == self.pid

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
      if self.isValid():
        newAliveSet = set()
        for pid,proc in self.other_procs.iteritems():
          if proc is not None:
            newAliveSet.add(proc)
        self.cur_request_set = newAliveSet

  def setState(self,newState):
    with self.global_lock:
      if self.isValid():
        self.state = newState

  def newParticipant(self, pid, new_thread):
    with self.global_lock:
      if self.isValid():
        self.server.other_procs[pid] = new_thread

  def add_request(self, request):
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

  def pop_voteNo_request(self):
    with self.global_lock:
      if self.voteNo_queue: 
        self.voteNo_queue.popleft()
      else:
        return None

  def pop_crashAfterVote_request(self):
    with self.global_lock:
      if self.crashAfterVote_queue: 
        self.crashAfterVote_queue.popleft()
      else:
        return None

  def pop_crashAfterAck_request(self):
    with self.global_lock:
      if self.crashAfterAck_queue: 
        self.crashAfterAck_queue.popleft()
      else:
        return None

  def pop_crashVoteReq_request(self):
    with self.global_lock:
      if self.crashVoteReq_queue: 
        self.crashVoteReq_queue.popleft()
      else:
        return None

  def pop_crashPartialPrecommit(self):
    with self.global_lock:
      if self.crashPartialPrecommit_queue: 
        self.crashPartialPrecommit_queue.popleft()
      else:
        return None

  def pop_crashPartialCommit(self):
    with self.global_lock:
      if self.crashPartialCommit_queue: 
        self.crashPartialCommit_queue.popleft()
      else:
        return None

  def getUrl(self, songname):
    with self.global_lock:
      if self.isValid():
        if songname in self.playlist:
          return self.playlist[songname]
        return None

  def broadCastMessage(self, msg):
    with self.global_lock:
      if self.isValid():
        for proc in self.cur_request_set:
          serialized = msg.serialize()
          proc.send(serialized)

  def broadCastMessage(self,msg,sendTopid):
    with self.global_lock:
      if self.isValid():
        for proc in self.cur_request_set:
          if proc.getClientPid() in sendTopid:
            serialized = msg.serialize()
            proc.send(serialized)

  def broadcastVoteReq(self, voteReq):
    pass

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
        song_name, url = request.song_name, request.url
        self.storage.add_song(song_name, url)
        self.playlist[song_name] = url

  def _delete_commit_handler(self,request):
    with self.global_lock:
      if self.isValid():
        song_name = request.song_name
        self.storage.delete_song(song_name)
        del self.playlist[song_name]

  # Send transactions to the process identified by PID
  # that start from diff_start
  def send_transaction_diff(self, pid, diff_start):
    with self.global_lock:
      txn_diff = TransactionDiff(self.pid,
                                 self.tid,
                                 self.transaction_history,
                                 diff_start)
      # Send this to the desired process
      self.other_procs[pid].send(txn_diff.serialize())

  def resetState(self):
    with self.global_lock:
      self.responsesNeeded = sys.maxint
      self.state = State.aborted
      self.coordinator_state = CoordinatorState.standby

  def exit(self):
    pass
    # We eithe issue a signal kill
    # or we join all the threads and fail gracefully
    
    



