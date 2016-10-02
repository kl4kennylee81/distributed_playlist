# only star import from libraries, not our own files!
from constants import *
import socket
from socket import SOCK_STREAM, AF_INET
from collections import deque
from threading import RLock
import sys

# whitelist import from our files
from request_messages import Add, Delete
import storage

from server_handler import ServerConnectionHandler
from client_handler import ClientConnectionHandler
from master_client_handler import MasterClientHandler

class Server:
  """
  Overall Server class, containing the global state of the server

  Fields:

  - pid: ID of this process (immutable)
  - global_lock: global lock
  - leader: PID of the leader
  - valid: is the whole server still active or crashing when False join all the threads
         and then return OR do we just fail ungracefully and just do a sys exit
  - coordinator_state: current phase of the coordinator's 3 phase commit
  - state: current state for participants
  - storage: stable storage for this server
  - responsesNeeded:
         Vote Phase - all vote yes; decrement on each incoming vote yes
         Ack Phase - respone ack + timeouts decrement on each ack or timeouts
  - playlist: playlist is the global dictionary mapping songname -> url
  - other_procs: update other procs to be a mapping from pid -> thread
  - tid: the current transaction id this server is working on
  - cur_request_set: threads currently active on the transaction that is occurring
  - request_queue: a queue of requests to process
  - crash_queue: a queue of `crash` commands for this process
  - voteNo_queue: a queue of `vote NO` commands for this process
  - crashAfterVote_queue: a queue of `crashAfterVote` commands for this process
  - crashAfterAck_queue: a queue of `crashAfterAck` commands for this process
  - crashVoteReq_queue: a queue of `crashVoteReq` commands for this process (involves
         failing after sending to a certain # of processes)
  - crashPartialPrecommit_queue: a queue of `crashPartialPrecommit` for this process
         (involves failing after sending to a certain # of processes)
  - crashPartialCommit_queue: a queue of `crashPartialCommit` for this process (involves
         failing after sending to a certain # of processes)

  *** In Memory Logging -- Backed Up In Stable Storage ***
  - transaction_history: list of previous, successful transactions (Adds + Deletes)
  - last_alive_set: set of PIDs, indicating the set of processes that were last alive

  - master_server: reference to master server
  - master_thread: reference to the thread dealing with the open socket with the master
         server
  - internal_server: server for other processes to connect to

  """

  def __init__(self, pid, n, port):

    self.commandRequestExecutors = {
      Add.msg_type: self._add_commit_handler,
      Delete.msg_type: self._delete_commit_handler,
    }

    # Fields
    self.pid = pid
    self.global_lock = RLock()
    self.leader = -1  # leader not initializd yet
    self.valid = True
    self.coordinator_state = CoordinatorState.standby
    self.state = State.aborted

    self.storage = storage.Storage(self.pid)
    self.responsesNeeded = sys.maxint
    self.playlist = dict()
    self.other_procs = dict()
    self.tid = 0
    self.cur_request_set = set()

    # Various queues for tracking requests + crash messages
    self.request_queue = deque()
    self.crash_queue = deque()
    self.voteNo_queue = deque()
    self.crashAfterVote_queue = deque()
    self.crashAfterAck_queue = deque()
    self.crashVoteReq_queue = deque()
    self.crashPartialPrecommit_queue = deque()
    self.crashPartialCommit_queue = deque()

    # Stable storage, in-memory
    self.transaction_history = self.storage.get_transcations()
    self.last_alive_set = set(self.storage.get_alive_set())

    # Setup master server / thread fields
    self.master_server, self.master_thread = \
      self._setup_master_server_and_thread(port)

    # The socket that all other internal processes connect to
    # for this process
    free_port_no = START_PORT + self.pid
    self.internal_server = ServerConnectionHandler(free_port_no, self)
    self.internal_server.start()

    # Initial socket connections or RECOVERY entrypoint
    self._initial_socket_or_recovery_handler(n)


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
        self.other_procs[pid] = new_thread

  def get_transaction_history(self):
    return self.transaction_history

  # Atomically log a transaction to both stable storage and
  # in-memory datastore
  def log_transaction(self, request):
    self.storage.write_transaction(request)
    self.transaction_history.append(request)

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

  def commit_cur_request(self):
    with self.global_lock:
      if self.isValid():
        current_request = self.request_queue.popleft()
        if current_request is not None:
          self.commandRequestExecutors[current_request.msg_type](current_request)
          self.state = State.committed

  def coordinator_commit_cur_request(self):
    with self.global_lock:
      if self.isValid():
          self.coordinator_state = CoordinatorState.completed
          self.commit_cur_request()

  def serialize_transaction_history(self):
    return [t.serialize() for t in self.transaction_history]

  def resetState(self):
    with self.global_lock:
      self.responsesNeeded = sys.maxint
      self.state = State.aborted
      self.coordinator_state = CoordinatorState.standby

  def exit(self):
    pass
    # We eithe issue a signal kill
    # or we join all the threads and fail gracefully

  def _setup_master_server_and_thread(self, port):
    master_server = socket.socket(AF_INET, SOCK_STREAM)
    master_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    master_server.bind((ADDRESS, port))

    # wait synchronously for the master to connect
    master_server.listen(1)
    (master_conn, (_,_)) = master_server.accept()
    master_thread = MasterClientHandler(master_conn, self)
    master_thread.start()

    return master_server, master_thread

  def _add_commit_handler(self, request):
    with self.global_lock:
      if self.isValid():
        song_name, url = request.song_name, request.url
        # Stable storage
        self.storage.add_song(song_name, url)
        self.log_transaction(request)
        # Local change
        self.playlist[song_name] = url


  def _delete_commit_handler(self, request):
    with self.global_lock:
      if self.isValid():
        song_name = request.song_name
        # Stable storage
        self.storage.delete_song(song_name)
        self.log_transaction(request)
        # Local change
        del self.playlist[song_name]


  def _initial_socket_or_recovery_handler(self, n):
    """
    use port 20000+i for each process' server socket, where i is the process id.
    Each process will search ports between 20000 and 20000+n-1 to see which
    process is alive. To then connect to.

    The server is passed onto all the handler threads because it essentially acts
    as a container for the "global state" of the processes server. Each of the
    handler needs to reference the server "Global" to get the current state of
    this participant and to interact and work collectively
    """

    # Initial Socket Connection Coordination
    if not self.storage.has_dt_log():
      no_socket = 0
      for i in range(n):
        try:
          if i != self.pid:
            # only try to connect to not yourself
            port_to_connect = START_PORT + i
            cur_handler = ClientConnectionHandler.fromAddress(ADDRESS, port_to_connect, self)
            cur_handler.start()
        except:
          # only connect to the sockets that are active
          no_socket += 1
          continue
      # if you are the first socket to come up, you are the leader
      if no_socket == (n - 1):
        self.leader = self.pid

    # If you failed and come back up
    else:
      dt_log = self.storage.get_dt_log()
      dt_log_arr = dt_log[len(dt_log)-1].split(',')
      tid = int(dt_log_arr[0])



