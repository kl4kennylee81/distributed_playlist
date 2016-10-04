# only star import from libraries, not our own files!
from constants import *
import socket
from socket import SOCK_STREAM, AF_INET
from collections import deque
from threading import RLock
import sys
import os

# whitelist import from our files
from request_messages import Add, Delete
from response_messages import ResponseAck,ResponseCoordinator,ResponseGet
from messages import StateReq, Decision, PreCommit, Reelect
import storage

from server_handler import ServerConnectionHandler
from client_handler import ClientConnectionHandler
from master_client_handler import MasterClientHandler


class Server:
  """
  Overall Server class, containing the global state of the server

  Fields:

  - n: The number of processes involved in this run of the system (immutable)
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
  - is_recovering: boolean indicating if this process is recovering


  *** Command Queues ***
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

  *** In Memory Logging -- Backed Up In Stable Storage***
  - transaction_history: list of previous, successful transactions (Adds + Deletes)
  - last_alive_set: set of PIDs, indicating the set of processes that were last alive

  - intersection: FOR USE IN RECOVERY.. the current, logged intersection of PIDs
         of last alive processes
  - recovered_set: List of PIDs indicating the processes who have recovered

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

    # the pid is immutable you should never change this value after
    # the process starts. can access without a lock

    # Fields
    self.n = n
    self.pid = pid
    self.global_lock = RLock()
    self.leader = -1  # leader not initializd yet
    self.valid = True
    self.coordinator_state = CoordinatorState.standby
    self.state = State.aborted

    self.storage = storage.Storage(self.pid)
    self.responsesNeeded = sys.maxint
    self.playlist = self.storage.get_disk()
    self.other_procs = dict()
    self.tid = 0
    self.cur_request_set = set()
    self.is_recovering = False

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
    # Logic to add yourself to last alive set if this is a fresh set of logs
    last_alive = set(self.storage.get_alive_set())
    self.last_alive_set = { self.pid } if len(last_alive) == 0 else last_alive

    # For recovery
    self.intersection = self.last_alive_set.copy()
    self.recovered_set = { self.pid }

    # Setup master server / thread fields
    self.master_server, self.master_thread = \
      self._setup_master_server_and_thread(port)

    # The socket that all other internal processes connect to
    # for this process
    free_port_no = START_PORT + self.pid
    self.internal_server = ServerConnectionHandler(free_port_no, self)
    self.internal_server.start()

    # Initial socket connections or RECOVERY entrypoint
    self._initial_socket_or_recovery_handler()


  def isValid(self):
    with self.global_lock:
      return self.valid

  # ONLY DIRECTLY SET IN THE CASE OF TERMINATION PROTOCOL
  # WHEN SENDING TO A SET OF UNDECIDED

  def setResponsesNeeded(self, request_set=None):
    with self.global_lock:
      if self.isValid():
        if request_set:
          self.responsesNeeded = request_set
        else:
          self.responsesNeeded = set([proc.getClientPid() for proc in self.cur_request_set])


  def deleteResponsesNeeded(self, pid):
    with self.global_lock:
      if self.isValid():
        if len(self.responsesNeeded) > 0:
          if pid in self.responsesNeeded:
            self.responsesNeeded.remove(pid)


  def getResponsesNeeded(self):
    with self.global_lock:
      return self.responsesNeeded

  def hasAllResponsesNeeded(self):
    with self.global_lock:
      if self.isValid():
        return len(self.responsesNeeded) <= 0
      return False

  def getPid(self):
    with self.global_lock:
      return self.pid

  def incrementTid(self):
    with self.global_lock:
      self.tid +=1

  def getTid(self):
    with self.global_lock:
      return self.tid

  def setTid(self, tid):
    with self.global_lock:
      self.tid = tid

  def setAtomicLeader(self, new_leader):
    """ Test and set for making sure the new leader is actually bigger than
        the current leader. This is used to ignore new leaders that are lower """
    with self.global_lock:
      if new_leader >= self.getAtomicLeader():
        old = self.getAtomicLeader()
        oldindex = self.getLeader()
        self.leader = new_leader
        storage.debug_print("PID: {} set Atomic:{}->{} Actual: {}->{}".format(self.pid, old, new_leader,oldindex, self.getLeader()))

        # ping master client of new leader
        new_coord = ResponseCoordinator(self.getLeader())
        self.master_thread.send(new_coord.serialize())

        return True
      return False

  def getAtomicLeader(self):
    with self.global_lock:
      return self.leader

  def getLeader(self):
    """ Returns the ACTUAL pids (from 0 ... n) of the current leader"""
    with self.global_lock:
      return self.getAtomicLeader() % self.n

  def isLeader(self):
    with self.global_lock:
      return self.getLeader() == self.pid

  def getCoordinatorState(self):
    with self.global_lock:
      return self.coordinator_state

  def getState(self):
    with self.global_lock:
      return self.state

  def get_last_alive_set(self):
    return self.last_alive_set

  def add_to_alive_set(self, pid):
    self.last_alive_set.add(pid)
    self.storage.update_alive_set(self.last_alive_set)

  def remove_from_alive_set(self, pid):
    if pid in self.last_alive_set:
      self.last_alive_set.remove(pid)
      self.storage.update_alive_set(self.last_alive_set)

  def set_is_recovering(self, b):
    self.is_recovering = b

  def is_in_transaction_history(self, tid):
    tid_set = set([t.tid for t in self.transaction_history])
    return tid in tid_set


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

        elif newCoordState == CoordinatorState.termination:
          self.setCurRequestProcesses()
          self.setResponsesNeeded()

  def setCurRequestProcesses(self):
    with self.global_lock:
      if self.isValid():
        newAliveSet = set()
        for pid,proc in self.other_procs.iteritems():
          if proc is not None:
            newAliveSet.add(proc)
        self.cur_request_set = newAliveSet
        self.last_alive_set = set([conn.getClientPid() for conn in newAliveSet] + [self.getPid()])
        self.storage.update_alive_set(self.last_alive_set)


  def is_in_cur_transaction(self,client_pid):
    if client_pid == self.pid:
      return True

    active_pids = [client.getClientPid() for client in self.cur_request_set]

    print "{}. active_pids:{}, client_pid:{}".format(self.pid, active_pids, client_pid)

    return client_pid in active_pids


  def remove_from_cur_transaction(self, client_pid):
    """ removes from the alive set and sets to None. Fails softly for key that doesn't exist """
    to_remove = None

    for client_proc in self.cur_request_set:
      if client_pid == client_proc.getClientPid():
        to_remove = client_proc
        break

    self.other_procs[client_pid] = None

    if to_remove in self.cur_request_set:
      self.cur_request_set.remove(to_remove)


  def setState(self,newState):
    with self.global_lock:
      if self.isValid():
        if newState == State.aborted:
          if self.request_queue:
            # dequeue the request that we are aborting to avoid repeats
            aborted_request = self.request_queue.popleft()
            print "{}. state is now aborted, throwing away {}".format(self.pid, aborted_request)
        self.state = newState

  def newParticipant(self, pid, new_thread):
    with self.global_lock:
      if self.isValid():
        self.other_procs[pid] = new_thread

  def get_transaction_history(self):
    return self.transaction_history

  def log_transaction(self, request):
    """
    Atomically log a transaction to both stable storage and
    in-memory datastore
    :param request:
    """
    self.storage.write_transaction(request)
    self.transaction_history.append(request)


  def update_transactions(self, transactions_diff):
    # Applying the changes as necessary to DB to bring it back up to date
    # (e.g. if we need to apply several adds and deletes that are packaged
    # into the VOTE-REQ, we do it here)

    for old_txn in transactions_diff:
      # Still sends the last transaction even if you're up to date (for those
      # that died in the previous round) so guard with this
      if not self.is_in_transaction_history(old_txn.tid):
        self.commandRequestExecutors[old_txn.type](old_txn)


  def add_request(self, request):
    """
    Add a request [type Request] to the request queue.
    We must keep track of these for later in case the process
    represented by this server commits later.
    :param request: Request object (Add, Delete, Get)
    """
    with self.global_lock:
      if self.isValid():
        self.request_queue.append(request)

  def add_crash_request(self,request):
    with self.global_lock:
      # not sure if we need to queue
      # up anything since you just fail
      # automatically maybe a kill signal
      # is all we need
      # self.crash_queue.append(request)
      # TODO: find out about this? doesn't seem like a big deal though
      self.exit()

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
        return self.voteNo_queue.popleft()
      else:
        return None

  def pop_crashAfterVote_request(self):
    with self.global_lock:
      if self.crashAfterVote_queue:
        return self.crashAfterVote_queue.popleft()
      else:
        return None

  def pop_crashAfterAck_request(self):
    with self.global_lock:
      if self.crashAfterAck_queue:
        return self.crashAfterAck_queue.popleft()
      else:
        return None

  def pop_crashVoteReq_request(self):
    with self.global_lock:
      if self.crashVoteReq_queue:
        return self.crashVoteReq_queue.popleft()
      else:
        return None

  def pop_crashPartialPrecommit(self):
    with self.global_lock:
      if self.crashPartialPrecommit_queue:
        return self.crashPartialPrecommit_queue.popleft()
      else:
        return None

  def pop_crashPartialCommit(self):
    with self.global_lock:
      if self.crashPartialCommit_queue:
        return self.crashPartialCommit_queue.popleft()
      else:
        return None

  def getUrl(self, songname):
    with self.global_lock:
      if self.isValid():
        if songname in self.playlist:
          return self.playlist[songname]
        return None

  def broadCastMessage(self, msg, sendTopid=None):
    """
    sentTopid is the list of pids to sent to. Otherwise broadcasts to all.
    """
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

  def broadCastStateReq(self):
    with self.global_lock:
      self.setCoordinatorState(CoordinatorState.termination)
      stateReqMsg = StateReq(self.pid, self.getTid(), self.getAtomicLeader())
      self.broadCastMessage(stateReqMsg)
      # also need to set the number of stateResponses needed
      # in the alive set of threads we'll keep track of their states


  def broadCastAbort(self):
    with self.global_lock:
      abortMsg = Decision(self.pid, self.getTid(), Decide.abort.name)
      self.setCoordinatorState(CoordinatorState.standby)

      print "broadcasting abort man"

      # Log the abort
      self.storage.write_dt_log(abortMsg)
      self.broadCastMessage(abortMsg)

      # TODO send an ack to the master client with abort
      responseAck = ResponseAck(Decide.abort)
      self.master_thread.send(responseAck.serialize())


  def broadCastCommit(self):
    with self.global_lock:
      self.coordinator_commit_cur_request()
      self.setCoordinatorState(CoordinatorState.completed)
      commitMsg = Decision(self.pid, self.tid, Decide.commit.name)

      crashPartialCommit = self.pop_crashPartialCommit()
      if crashPartialCommit is not None:
        # broadcast commit will add it to the playlist
        self.broadCastMessage(commitMsg,crashPartialCommit.sendTopid)
        self.exit()
      else:
        # broadcast commit will add it to the playlist
        self.broadCastMessage(commitMsg)

  def broadCastPrecommit(self,sendTopid=None):
    with self.global_lock:
      self.setCoordinatorState(CoordinatorState.precommit)
      precommit = PreCommit(self.pid, self.getTid())
      crashPartialPreCommit = self.pop_crashPartialPrecommit()
      if crashPartialPreCommit is not None:
        if sendTopid != None:
          setToSend = set.intersection(set(crashPartialPreCommit.sendTopid), sendTopid)

          # sendTopid is passed in IN THE SPECIAL CASE when i am only sending it to the
          # uncertain peopl in the termination protocol I need to set my responsesNeeded to
          # the length of that list
          self.setResponsesNeeded(sendTopid)
        else:
          setToSend = crashPartialPreCommit.sendTopid

        self.broadCastMessage(precommit,setToSend)
        self.exit()
      else:
        self.broadCastMessage(precommit)

  def commit_cur_request(self):
    with self.global_lock:
      if self.isValid():
        current_request = self.request_queue.popleft()
        print "{}. this is the current request that i am committing {}".format(self.pid, current_request)
        if current_request is not None:
          self.commandRequestExecutors[current_request.msg_type](current_request)
          self.setState(State.committed)

  def coordinator_commit_cur_request(self):
    with self.global_lock:
      if self.isValid():
        # TODO get it from the log that i have already committed because for example I get
        # TODO get elected leader and then am calling broadcastCommit again don't want to reexecute
        if self.getState() != State.committed:
          self.commit_cur_request()
        # TODO send an ack to the master client with commit
        # TODO do we send the response before we crash aka before we send
        # TODO aka when we commited the request
        responseAck = ResponseAck(Decide.commit)
        self.master_thread.send(responseAck.serialize())
        self.setCoordinatorState(CoordinatorState.completed)


  def full_recovery_check(self, last_alive_set):
    with self.global_lock:
      print "{}. starting full_recovery_check".format(self.pid)
      # Reset this server's intersection to be an intersection
      # of its current intersection and the other process' last_alive_set
      self.intersection = \
        set.intersection(self.intersection, last_alive_set)

      # R is a superset of the intersection of all the last_alive_sets of the
      # recovered processes
      if self.intersection.issubset(self.recovered_set):
        self.set_is_recovering(False)  # We're no longer recovering
        self.send_election()

  def send_election(self):
    with self.global_lock:
      # round robin selection of next leader
      self.setCurRequestProcesses()
      print "{}. is it getting set or not {}".format(self.pid,
                                            self.cur_request_set)
      self._set_next_leader()

      if self.isLeader():
        if len(self.cur_request_set) == 0:
          # if there's literally nobody alive and I just run termination protocol
          # with my own state aka if i was uncertain i abort if im precommit i commit
          # because basically condition 4 is send precommit to everyone else
          # then commit however committing to myself is just commit
          self.terminationGather()
        else:
          # send out the stateReq to everybody that's in the cur_request_set
          self.broadCastStateReq()

      else:
        electMsg = Reelect(self.pid, self.getTid(), self.getAtomicLeader())
        newLeaderIndex = self.getLeader()
        self.other_procs[newLeaderIndex].send(electMsg.serialize())


  def _set_next_leader(self):
    """
    Initiate round robin to select the next leader starting from the pid
    of the previous leader.
    """
    with self.global_lock:
      active_pids = [proc.getClientPid() for proc in self.cur_request_set]

      for i in xrange(self.n):
        cur_leader = self.getAtomicLeader()+i
        cur_leader_index = cur_leader % self.n

        # skipping processes that aren't in the current transaction anymore
        if not self.is_in_cur_transaction(cur_leader_index):
          continue

        elif cur_leader_index == self.pid:
          # need to set the coordinator State since i'm coordinator now to termination
          self.setCoordinatorState(CoordinatorState.termination)
          self.setAtomicLeader(cur_leader)
          return

        elif cur_leader_index in active_pids:
          self.setAtomicLeader(cur_leader)
          return

      raise Exception("Server:{}... ".format(self.getPid()))


  def terminationGather(self):
    with self.global_lock:

      allStates = [self.getState()]

      print("{}. calling termination gather {}".format(self.getPid(), self.cur_request_set))
      # first case is check for aborts
      for client_proc in self.cur_request_set:
        allStates.append(client_proc.getClientState())

      for tpState in allStates:
        if tpState == State.aborted:
          self.broadCastAbort()
          return

      # second case is check for commit
      for tpState in allStates:
        if tpState == State.committed:
          print("{}. did we broadcast commit to all".format(self.getPid()))
          self.broadCastCommit()
          return

      # 3rd case is check for all undecided
      num_uncertain = len(allStates)
      for tpState in allStates:
        if tpState == State.uncertain:
          num_uncertain-=1

      if num_uncertain == 0:
        self.broadCastAbort()
        return

      # 4th case commitable
      isCommitable = False
      for tpState in allStates:
        if tpState == State.committable:
          isCommitable = True
          break

      # I myself am always commitable at this phase of checking
      # of the termination protocol
      if isCommitable:
        sendTopid = set()
        for client_proc in self.cur_request_set:
          if client_proc.getClientState() == State.uncertain:
            sendTopid.add(client_proc)

        # then I am the only one left so i can unilaterally commit
        if len(self.cur_request_set) == 0:
          self.coordinator_commit_cur_request()

        # There is > 0 other processes in the current transacaction
        # however they are all committable and now you are as well
        elif len(sendTopid) == 0:
          self.broadCastCommit()

        # we broadcast a precommit to UNCERTAIN PEOPLE setting num responses
        # needed to the number of uncertain recepients
        else:
          # will set you from if YOU were uncertain -> commitable
          self.broadCastPrecommit(sendTopid)
      else:
        print("not supposed to be here")


  def serialize_transaction_history(self):
    return [t.serialize() for t in self.transaction_history]

  def resetState(self):
    with self.global_lock:
      self.responsesNeeded = sys.maxint
      self.state = State.aborted
      self.coordinator_state = CoordinatorState.standby


  def _setup_master_server_and_thread(self, port):
    # Register master server reference
    master_server = socket.socket(AF_INET, SOCK_STREAM)
    master_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    master_server.bind((ADDRESS, port))

    # wait synchronously for the master to connect
    master_server.listen(1)
    (master_conn, (_,_)) = master_server.accept()
    master_thread = MasterClientHandler(master_conn, self)
    master_thread.start()

    # Return tuple
    return master_server, master_thread


  def _add_commit_handler(self, add_req):
    """
    Handles adding a song.  Logs the transaction in stable storage,
    adds the song to the DB, and adds the song to the in-memory
    dictionary.
    :param add_req: Add request
    """
    with self.global_lock:
      if self.isValid():
        song_name, url = add_req.song_name, add_req.url
        print "{}. commiting {} {}".format(self.pid, song_name, url)
        # LOG: Stable storage
        self.storage.write_dt_log(add_req)
        self.storage.add_song(song_name, url)
        self.log_transaction(add_req)
        # Local change
        self.playlist[song_name] = url


  def _delete_commit_handler(self, delete_req):
    """
    Handles the deletion of a song.  Logs the transaction in stable
    storage, deletes the song from the DB, and removes the song from
    the in-memory dictionary.
    :param delete_req: Delete request
    """

    with self.global_lock:
      if self.isValid():
        song_name = delete_req.song_name
        # LOG: Stable storage
        self.storage.write_dt_log(delete_req)
        self.storage.delete_song(song_name)
        self.log_transaction(delete_req)
        # Local change
        del self.playlist[song_name]


  def _connect_with_peers(self, n):
    """
    Uses port 20000 as a base, and iterates from 0 to n-1 (inclusive) to
    to attempt to connect to peer processes.

    The server is passed into the handler threads because it essentially
    acts as the "Global" state.  It is a container and the thread
    corresponding to the socket connection registers itself with the
    server.

    :param n: Number of processes involved in the run of the system.
    :return: The number of sockets that don't exist
    """
    no_socket = 0
    for i in range(n):
      try:
        if i != self.pid:
          # only try to connect to not yourself
          port_to_connect = START_PORT + i
          cur_handler = ClientConnectionHandler.fromAddress(ADDRESS, port_to_connect, self)
          cur_handler.setClientPid(i)
          cur_handler.start()
      except:
        # only connect to the sockets that are active
        no_socket += 1
        continue

    return no_socket


  def _initial_socket_or_recovery_handler(self):
    """
    If this process is starting up for the first time, connects with peers
    for the first time.  Else, attempts to initiate recovery procedure.
    :param n: Number of processes involved in this run of the system.
    """
    with self.global_lock:
      # Initial Socket Connection Coordination
      if not self.storage.has_dt_log():
        # Grab the sockets that don't exist
        no_socket = self._connect_with_peers(self.n)
        # if you are the first socket to come up, you are the leader
        if no_socket == (self.n - 1):
          self.setAtomicLeader(self.pid)

      # If you failed and come back up
      else:
        dt_log_arr = self.storage.get_last_dt_entry().split(',')
        self.setTid(int(dt_log_arr[0]))

        # IF WE HIT THIS, WE ARE ABSOLUTELY 100% RECOVERING

        # Based on the DT Log, set our state
        self.set_is_recovering(True)
        if dt_log_arr[1] == "yes" or dt_log_arr[1] == "votereq":
          self.setState(State.uncertain)
        elif dt_log_arr[1] == "commit":
          self.setState(State.committed)
        elif dt_log_arr[1] == "abort":
          self.setState(State.aborted)

        # Connect with our peers
        self._connect_with_peers(self.n)

        # Check to see if we're the only node and we're safe to recover
        self.full_recovery_check(self.last_alive_set)


  def exit(self):
    with self.global_lock:

      for other, conn in self.other_procs.iteritems():
        if conn:
          conn.close()

      self.master_thread.close()
      self.internal_server.close()
      self.master_server.close()

      self.isValid = False

    os.system('kill %d' % os.getpid())