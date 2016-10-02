# only star import from libraries, not our own files!
from constants import *
import socket
from socket import SOCK_STREAM, AF_INET
from collections import deque
from threading import RLock
import sys

# whitelist import from our files
from messages import Add, Delete
import storage

from server_handler import ServerConnectionHandler
from client_handler import ClientConnectionHandler
from master_client_handler import MasterClientHandler

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

  def __init__(self, pid, n, port):
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
    self.leader = -1  # leader not initializd yet

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


    no_socket = 0
    for i in range(n):
      try:
        if i != pid:
          # only try to connect to not yourself
          port_to_connect = START_PORT + i
          cur_handler = ClientConnectionHandler.fromAddress(ADDRESS,
                                                            port_to_connect,
                                                            self)
          cur_handler.start()
      except:
        # only connect to the sockets that are active
        no_socket += 1
        continue

    # if you are the first socket to come up, you are the leader
    if no_socket == (n-1):
      self.leader = self.pid


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

  def setLeader(self, isLeader):
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

  def broadCastMessage(self, msg, sendTopid=None):
    with self.global_lock:
      if self.isValid():
        for proc in self.cur_request_set:
          if sendTopid:
            if proc.getClientPid() in sendTopid:
              serialized = msg.serialize()
              proc.send(serialized)
          else:
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
