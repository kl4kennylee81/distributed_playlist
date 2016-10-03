from constants import *
import socket
from socket import SOCK_STREAM, AF_INET
from threading import Thread

from messages import VoteReq, PreCommit, Decision, Identifier, Vote, Ack, Reelect, StateReqResponse
from messages import deserialize_message

from crash_request_messages import deserialize_client_request

from response_messages import ResponseAck

class ClientConnectionHandler(Thread):
  """
  Internal server process has an associated client handler thread to handle the
  request from other internal servers
  ex. the votes/decisions/precommits etc.
  """

  def __init__(self):
    Thread.__init__(self)
    # pid of the server on the other side of the socket.
    # Determined after first messages are sent.
    self.client_pid = -1
    self.client_state = State.aborted

    self.participant_handlers = {
      VoteReq.msg_type: self._voteReqHandler,
      PreCommit.msg_type: self._preCommitHandler,
      Decision.msg_type: self._decisionHandler,
      #### These are for the special features ####
      # Recover.msg_type: Recover,
      # Reelect.msg_type: Reelect,
      # StateReq.msg_type: StateReq,
      # StateRepid.msg_type: StateRepid
    }

    self.coordinator_handlers = {
      Vote.msg_type: self._voteHandler,
      Ack.msg_type: self._ackHandler,
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

  def setClientPid(self, client_pid):
    with self.server.global_lock:
      self.client_pid = client_pid

  def getClientPid(self):
    with self.server.global_lock:
      return self.client_pid

  def getClientState(self):
    with self.server.global_lock:
      return self.client_state

  def isValid(self):
    with self.server.global_lock:
      return self.valid and self.server.isValid()

  def run(self):
    # on bootup, first send Identifier message to the processes you connected to
    id_msg = Identifier(self.server.pid, self.server.getTid(), self.server.getAtomicLeader())
    self.send(id_msg.serialize())

    try:
      while self.valid:
        data = self.conn.recv(BUFFER_SIZE)
        msg = deserialize_message(str(data))
        self.server.storage.write_debug("Received: {}".format(self.server.pid, data))

        if Identifier.msg_type == msg.type:
          self._idHandler(msg)
        elif self.server.isLeader():
          self._coordinatorRecv(msg)
        else:
          self._participantRecv(msg)

    # catching all exceptions more than just timeout
    # TODO: figure out what the actual exceptions are
    except Exception, e:
      self.server.storage.write_debug(str(e) + "\n[^] Timeout error")

      self.valid = False  # TODO: why do we need self.valid?
      self.server.other_procs[self.getClientPid()] = None
      self.server.remove_from_cur_transaction(self.getClientPid())
      self.setClientPid(-1)
      self.conn.close()

      if self.server.isLeader():
        self._coordinator_timeout_handler()
      else:
        self._participant_timeout_handler()


  def _coordinator_timeout_handler(self):
    with self.server.global_lock:
      try:
        self.coord_failureHandler[self.server.getCoordinatorState()]()
      except KeyError, e:
        self.server.storage.write_debug(str(e) + "\n[^] Invalid coordinator state")


  def _participant_timeout_handler(self):
    with self.server.global_lock:
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
      abort = Decision(self.server.pid, self.server.getTid(), Decide.abort)
      abortSerialized = abort.serialize()
      self.server.storage.write_dt_log(abortSerialized)


  def _preCommit_timeout(self):
    self._send_election()


  def _commit_timeout(self):
    self._send_election()


  def _send_election(self):
    with self.server.global_lock:
      # round robin selection of next leader
      self._set_next_leader()

      if self.server.isLeader():
        # send out the stateReq to everybody that's in the cur_request_set
        self.server.broadCastStateReq()

      else:
        electMsg = Reelect(self.server.pid,self.server.getAtomicLeader())
        newLeaderIndex = self.server.getLeader()
        self.server.other_procs[newLeaderIndex].send(electMsg.serialize())

  def _set_next_leader(self):
    """
    Initiate round robin to select the next leader starting from the pid
    of the previous leader.
    """
    with self.server.global_lock:
      n  = len(self.server.other_procs)

      for i in xrange(n):
        cur_leader = self.getAtomicLeader()+i
        cur_leader_index = cur_leader % n

        # skipping processes that aren't in the current transaction anymore
        if not self.server.is_in_cur_transaction(cur_leader_index):
          continue

        elif cur_leader_index == self.server.pid:
          self.server.setAtomicLeader(cur_leader)
          return

        elif cur_leader_index in self.server.other_procs and self.server.other_procs[cur_leader_index]:
          self.server.setAtomicLeader(cur_leader)
          return

      raise Exception("Election result is not found")


  """ Coordinator timeout handlers """


  def _vote_timeout(self):
    """ The coordinator timed out waiting for a vote, so automatically aborts the transaction """
    with self.server.global_lock:
      self.server.broadCastAbort()


  def _ack_timeout(self):
    # the client_pid and thread has already been removed from the set of processes
    # active in this transaction by the exception handler
    with self.server.global_lock:
      self._ackHandler(None, self.server)


  def _coordinatorRecv(self, msg):
    self.coordinator_handlers[msg.type](msg)


  def _participantRecv(self, msg):
    self.participant_handlers[msg.type](msg)


  def _idHandler(self, msg):
    with self.server.global_lock:
      self.server.other_procs[msg.pid] = self
      self.setClientPid(msg.pid)

      # this checks if atomic_leader is greater than the self.leader for this process
      # if so, updates the leader
      self.server.setAtomicLeader(msg.atomic_leader)


  # Participant recieved messages votereq, precommit, decision #
  def _voteReqHandler(self, msg):
    with self.server.global_lock:
      if self.server.getState() == State.aborted or self.server.getState() == State.committed:

        # in the recovery mode you would check the response id (rid)
        # of the voteReq and also update your state to become consistent

        # Deserialize request and add it to the request queue
        self.server.storage.write_debug("this is what is getting passed in man {}".format(msg.request))
        request = deserialize_client_request(msg.request, self.server.getTid())
        self.server.add_request(request)

        forcedNo = self.server.pop_voteNo_request()

        if forcedNo != None:
          choice = Choice.no
        else:
          choice = Choice.yes

        # Log that we voted yes + then vote yes
        choiceMsg = Vote(self.server.pid, self.server.getTid(), choice)
        choiceSerialized = choiceMsg.serialize()
        self.server.storage.write_dt_log(choiceSerialized)
        self.send(choiceSerialized)  # if participant crash

        if forcedNo != None:
          self.server.setState(State.aborted)
        else:
          self.server.setState(State.uncertain)

        afterVoteCrash = self.server.pop_crashAfterVote_request()

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
        self.server.storage.write_debug(str(msg.decision))
        self.server.storage.write_debug(str(msg.decision == Decide.commit))
        self.server.storage.write_debug(str(Decide.commit))
        if msg.decision == Decide.commit.name:
          self.server.storage.write_debug("am commiting")
          self.server.commit_cur_request()
        else:
          if msg.decision == Decide.abort.name:
            self.server.storage.write_debug("am aborting")
            self.storage.write_dt_log(msg.serialize())
      else:
        # if out of the commitable stage there can be a abort message
        if msg.decision == Decide.abort.name:
            self.storage.write_dt_log(msg.serialize())


  # coordinator received messages vote, acks
  def _voteHandler(self, msg):
    with self.server.global_lock:
      if self.server.getCoordinatorState() == CoordinatorState.votereq:
        if msg.choice == Choice.no:
          self.client_state = State.aborted

        else:
          self.client_state = State.uncertain
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

        self.client_state = State.committable

        if self.server.hasAllResponsesNeeded():
          self.server.coordinator_commit_cur_request()

          d = Decide.commit
          decision = Decision(self.server.pid, self.server.getTid(), d)

          crashPartialCommit = self.server.pop_crashPartialCommit()

          if crashPartialCommit is not None:
            self.server.broadCastMessage(decision, crashPartialCommit.sendTopid)
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


  def _stateReqHandler(self, msg):
    with self.server.global_lock:
      # respond with my current state
      stateResponseMsg = StateReqResponse(self.server.pid, self.server.getTid(), self.server.getState())
      self.send(stateResponseMsg.serialize())


  def _reelectHandler(self, msg):
    """ Ignores any relect message coming in that is lower than the current atomic leader"""
    with self.server.global_lock:
      if self.server.setAtomicLeader(msg.new_atomic_leader):
        self.server.broadCastStateReq()


  def _stateReqResponseHandler(self, msg):
    with self.server.global_lock:
      if self.getCoordinatorState() == CoordinatorState.termination:
        self.client_state = msg.state
        self.server.decrementResponsesNeeded()

        if self.server.hasAllResponsesNeeded():
          # run the state termination gathering which will return a msg
          # either a precommit, abort, commit
          self._terminationGather()


  def _terminationGather(self):
    with self.server.global_lock:

      # first case is check for aborts
      for client_proc in self.server.cur_request_set:
        if client_proc.getClientState() == State.aborted:
          self.server.broadCastAbort()
          return

      # second case is check for commit
      for client_proc in self.server.cur_request_set:
        if client_proc.getClientState() == State.committed:
          commitMsg = Decision(self.server.pid,self.server.tid,Decide.commit)
          self.server.broadCastMessage(commitMsg)
          self.server.setCoordinatorState(CoordinatorState.completed)

          self.server.commit_cur_request()
          return

      # 3rd case is check for all undecided
      num_uncertain = len(self.server.cur_request_set)
      for client_proc in self.server.cur_request_set:
        if client_proc.getClientState() == State.uncertain:
          num_uncertain-=1

      if num_uncertain == 0:
        self.server.broadCastAbort()
        return

      isCommitable = False
      sendToPids = []
      # 4th case commitable
      for client_proc in self.server.cur_request_set:
        if client_proc.getClientState() == State.committable:
          isCommitable = True
          continue
        if client_proc.getClientState() == State.uncertain:
          sendToPids.append(client_proc.getClientPid())

      if isCommitable:
        precommit = Decision(self.server.pid, self.server.tid, Decide.precommit)
        self.server.setCoordinatorState(CoordinatorState.precommit)
        self.server.broadCastMessage(precommit, sendToPids)


  def send(self, s):
    with self.server.global_lock:
      if self.isValid():
        self.conn.send(str(s))


  def close(self):
    try:
      self.valid = False

      # close the connection socket
        self.conn.shutdown(socket.SHUT_RDWR)
      self.conn.close()
    except socket.error, e:
      self.server.storage.write_debug(str(e) + "\n[^] Socket error")