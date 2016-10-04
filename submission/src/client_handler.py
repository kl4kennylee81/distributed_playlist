from constants import *
import socket
from socket import SOCK_STREAM, AF_INET
from threading import Thread

from messages import VoteReq, PreCommit, Decision, Identifier, Vote, Ack, Reelect, StateReq, StateReqResponse, Recovery
from messages import deserialize_message

from crash_request_messages import deserialize_client_request
from response_messages import ResponseAck

from storage import debug_print


class ClientConnectionHandler(Thread):
  """
  Internal server process has an associated client handler thread to handle the
  request from other internal servers
  ex. the votes/decisions/precommits etc.
  """

  def __init__(self):
    Thread.__init__(self)

    # Connection metadata
    self.client_pid = -1
    self.client_state = State.aborted
    self.client_tid = 0

    self.participant_handlers = {
      VoteReq.msg_type: self._voteReqHandler,
      Recovery.msg_type: self._recoveryHandler,
      PreCommit.msg_type: self._preCommitHandler,
      Decision.msg_type: self._decisionHandler,
      Reelect.msg_type: self._reelectHandler,
      StateReq.msg_type: self._stateReqHandler,
    }

    self.coordinator_handlers = {
      Recovery.msg_type: self._recoveryHandler,
      Vote.msg_type: self._voteHandler,
      Ack.msg_type: self._ackHandler,
      Reelect.msg_type: self._nopHandler,
      StateReqResponse.msg_type: self._stateReqResponseHandler,
      StateReq.msg_type: self._stateReqHandler,
    }

    # Handlers for timeouts
    self.parti_failureHandler = {
      State.aborted: self._voteReq_timeout,
      State.uncertain: self._preCommit_timeout,
      State.committable: self._commit_timeout,
      State.committed: self._committed_timeout,
    }

    self.coord_failureHandler = {
      CoordinatorState.votereq: self._vote_timeout,
      CoordinatorState.precommit: self._ack_timeout,
      CoordinatorState.termination: self._termination_timeout,
      CoordinatorState.standby:self._nop_timeout,
      CoordinatorState.completed:self._nop_timeout,
    }

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

  def setClientPid(self, client_pid):
    with self.server.global_lock:
      self.client_pid = client_pid

  def setClientTid(self, client_tid):
    with self.server.global_lock:
      self.client_tid = client_tid

  def getClientPid(self):
    with self.server.global_lock:
      return self.client_pid

  def getClientTid(self):
    with self.server.global_lock:
      return self.client_tid

  def getClientState(self):
    with self.server.global_lock:
      return self.client_state

  def isValid(self):
    with self.server.global_lock:
      return self.valid and self.server.isValid()

  def run(self):
    # on bootup, first send Identifier message to the processes you connected to
    id_msg = Identifier(self.server.pid,
                        self.server.getTid(),
                        self.server.getAtomicLeader(),
                        self.server.getState(),
                        self.server.get_last_alive_set(),
                        self.server.is_recovering)
    self.send(id_msg.serialize())

    try:
      while self.isValid():
        data = self.conn.recv(BUFFER_SIZE)

        print("{}->{}. RECV MSG {}".format(self.getClientPid(), self.server.pid,data))
        with self.server.global_lock:
          if len(data) <= 0:
            self.server.storage.write_debug("Got EOF from socket {}".format(self.getClientPid()))
            self._begin_timeout(True)
            return
          self.server.storage.write_debug("Received:{}, length:{}".format(data, len(data)))

          msg = deserialize_message(str(data))

          if Identifier.msg_type == msg.type:
            self._idHandler(msg)
          elif self.server.isLeader():
            self._coordinatorRecv(msg)
          else:
            self._participantRecv(msg)

    # catching all exceptions more than just timeout
    except (socket.timeout, socket.error) as e:
      with self.server.global_lock:
        self.server.storage.write_debug(str(e) + "[^] Timeout error or Socket Closed")
        self._begin_timeout(False)
        return


  def _begin_timeout(self, already_closed):
    with self.server.global_lock:

      self.close(already_closed)

      if self.server.isLeader():
        self._coordinator_timeout_handler()
      else:
        print "{}. this is clientPID:{}, this is who i think the leader is {}".format(self.server.pid, self.getClientPid(), self.server.getLeader())
        if self.getClientPid() == self.server.getLeader():
          self._participant_timeout_handler()


  def _coordinator_timeout_handler(self):
    with self.server.global_lock:
      "{}.Timeout error handler for client_pid:{}".format(self.server.pid,self.getClientPid())
      self.coord_failureHandler[self.server.getCoordinatorState()]()


  def _participant_timeout_handler(self):
    with self.server.global_lock:
      print "{}.In participant_timeout_handler with state {}".format(self.server.pid, self.server.getState().name)
      self.parti_failureHandler[self.server.getState()]()

  """ Participant timeout handlers """


  def _voteReq_timeout(self):
    """
    If a process times out waiting for voteReq, unilaterally abort.
    """
    with self.server.global_lock:
      abort = Decision(self.server.pid, self.server.getTid(), Decide.abort.name)
      self.server.storage.write_dt_log(abort)


  def _preCommit_timeout(self):
    debug_print("precommit_timeout")
    self.server.send_election()


  def _commit_timeout(self):
    debug_print("commit_timeout")
    self.server.send_election()

  def _committed_timeout(self):
    debug_print("committed_timeout")
    self.server.send_election()

  # This is because EOF can be gotten at anytime thus you don't do anything
  # and then the timeout handler will close the thread and socket
  def _nop_timeout(self):
    pass




  """ Coordinator timeout handlers """


  def _vote_timeout(self):
    """ The coordinator timed out waiting for a vote, so automatically aborts the transaction """
    with self.server.global_lock:
      self.server.broadCastAbort()


  def _ack_timeout(self):
    # the client_pid and thread has already been removed from the set of processes
    # active in this transaction by the exception handler
    with self.server.global_lock:
      self._ackHandler(None)


  def _termination_timeout(self):
    # we remove this client_pid from the responsesNeeded Set and then
    # continue on with the stateReqResponse Handler
    # pass in a none message we will decrement the client_pid from the set
    # of responses needed
    self._stateReqResponseHandler(None)

  def _coordinatorRecv(self, msg):
    with self.server.global_lock:
      self.coordinator_handlers[msg.type](msg)


  def _participantRecv(self, msg):
    with self.server.global_lock:
      self.participant_handlers[msg.type](msg)


  def _nopHandler(self, msg):
    pass


  def _idHandler(self, msg):
    """
    Handles reception of an Identifier message on a process level.

    This handler deals with processes in two different states.

    The process could be regrouping after it has just recovered
    from failing, in which case it needs to see if other processes
    exist that are undergoing a transaction.  If there are, it no
    longer is recovering and "waits" until the next round of VOTE-
    REQ.  It may attempt to arrange a full-system recovery (re-election)
    if it finds the last process that failed on a full system failure.

    Otherwise, the process is coming alive and establishing a socket +
    a record of the process that is alive and working.

    :param msg: Identifier message
    """

    with self.server.global_lock:
      # Necessary for all ID-ing message calls
      self.server.other_procs[msg.pid] = self

      self.server.setAtomicLeader(msg.atomic_leader)
      self.setClientTid(msg.tid)
      self.setClientPid(msg.pid)

      # if we're not the leader, and the process reaching out to us is
      # not recovering and we're not recovering, then we're a process
      # trying to get our bearings as to who is alive around us
      # We MUST add it to alive set so that we can keep track of up processes
      # from the beginning
      if not self.server.isLeader() \
        and not msg.is_recovering \
        and not self.server.is_recovering:
        self.server.add_to_alive_set(msg.pid)


      # send the new guy who connected to you the transaction diffs so he can recover and join the party ayy turn up
      if self.server.getTid() > msg.tid:
        diff_start = msg.tid
        transactions_diff = [t for t in self.server.get_transaction_history() if t.tid > diff_start]
        recovery_msg = Recovery(self.server.pid, self.server.getTid(), transactions_diff)
        self.send(recovery_msg.serialize())

      # If we're both uncertain and we're both recovering and we're on the same transaction
      # we want to perform intersection / set logic to see if we can recover.
      # We'll never pre-maturely recover b/c if there are other servers that are up,
      # we'll never include them in our recovered_set and we'll never try and re-elect...
      if self.server.is_recovering and msg.is_recovering:
        self.server.recovered_set.add(msg.pid)

        # he has to be recovering after having been committed
        # he is the leader and is in standby to restart the protocol
        # while he is blocked on the current transaction until the intersection is met
        if self.server.isLeader() and self.CoordinatorState == standby:
          if self.server.can_issue_full_recovery(self.server.get_last_alive_set()):
            self.server.master_waiting_on_recovery().notify()

        # if they were in total failure while in the same transaction then
        # then we must continue on.
        elif self.getClientTid() == self.server.getTid():
          self.server.full_recovery_check(msg.last_alive_set)


  def _recoveryHandler(self, msg):
    with self.server.global_lock:
      if self.server.getTid() < msg.tid or self.server.is_recovering:
        print "{} got the recovery message!".format(self.server.pid)
        self.server.update_transactions(msg.transactions_diff)


  # Participant recieved messages votereq
  def _voteReqHandler(self, msg):
    with self.server.global_lock:
      if self.server.getState() == State.aborted \
      or self.server.getState() == State.committed:
        self.server.setState(State.uncertain)

        # Brings us up to date
        self.server.setTid(msg.tid)

        # If we're getting a VOTE-REQ, we're back in the flow of things
        self.server.set_is_recovering(False)

        # Update our last alive set and the participant's cur_request_set
        self.server.setCurRequestProcesses()

        # in the recovery mode you would check the response id (rid)
        # of the voteReq and also update your state to become consistent

        # Deserialize request and add it to the request queue
        request = deserialize_client_request(msg.request, self.server.getTid())
        self.server.add_request(request)

        # this gets the transaction diff from someone else and updates your logs
        self.server.update_transactions(msg.transactions_diff)

        forcedNo = self.server.pop_voteNo_request()

        if forcedNo is not None:
          choice = Choice.no
        else:
          choice = Choice.yes

        # LOG: Log the vote if we vote yes, then vote
        choiceMsg = Vote(self.server.pid, self.server.getTid(), choice.name)
        if choiceMsg.choice == Choice.yes: self.server.storage.write_dt_log(choiceMsg)
        self.send(choiceMsg.serialize())  # if participant crash

        if forcedNo is not None:
          self.server.setState(State.aborted)
        else:
          self.server.setState(State.uncertain)

        afterVoteCrash = self.server.pop_crashAfterVote_request()

        if afterVoteCrash is not None and not self.server.isLeader():
          self.server.exit()


  def _preCommitHandler(self, msg):
    with self.server.global_lock:
      if self.server.getState() == State.uncertain:
        self.server.setState(State.committable)

        ackRes = Ack(self.server.pid, self.server.getTid())
        self.send(ackRes.serialize())

        crashAfterAck = self.server.pop_crashAfterAck_request()
        if crashAfterAck is not None and not self.server.isLeader():
          self.server.exit()


  def _decisionHandler(self, msg):
    with self.server.global_lock:
      if self.server.getState() == State.committable or self.server.getState() == State.uncertain:
        if msg.decision == Decide.commit:
          self.server.storage.write_debug("am commiting")
          self.server.commit_cur_request()
        elif msg.decision == Decide.abort:
          self.server.storage.write_debug("am aborting")
          self.server.storage.write_dt_log(msg)
          self.server.setState(State.aborted)
      else:
        # if out of the commitable stage there can be a abort message
        if msg.decision == Decide.abort:
            self.server.storage.write_dt_log(msg)
            self.server.setState(State.aborted)


  # coordinator received messages vote, acks
  def _voteHandler(self, msg):
    with self.server.global_lock:
      if self.server.getCoordinatorState() == CoordinatorState.votereq:
        if msg.choice == Choice.no:
          self.server.broadCastAbort()
        else:
          self.client_state = State.uncertain
          self.server.deleteResponsesNeeded(self.client_pid)
          if self.server.hasAllResponsesNeeded():
            self.server.broadCastPrecommit()


  def _ackHandler(self, msg):
    """

    :param msg: Ack
    :return:
    """

    with self.server.global_lock:
      if self.server.getCoordinatorState() == CoordinatorState.precommit:
        self.server.deleteResponsesNeeded(self.client_pid)

        self.client_state = State.committable

        if self.server.hasAllResponsesNeeded():
          self.server.broadCastCommit()

  def _stateReqHandler(self, msg):
    # TODO think over this case again
    # also called by coordinator because he can be replaced by a stateReq
    # of someone that is higher value
    with self.server.global_lock:
      if self.server.setAtomicLeader(msg.atomic_leader):
        # respond with my current state
        stateResponseMsg = StateReqResponse(self.server.pid, self.server.getTid(), self.server.getState().name)
        debug_print("{}. sending stateReqResponse -> {}".format(self.server.pid,stateResponseMsg.serialize()))
        self.send(stateResponseMsg.serialize())

  def _reelectHandler(self, msg):
    """ Ignores any relect message coming in that is lower than the current atomic leader"""
    with self.server.global_lock:
      if self.server.setAtomicLeader(msg.new_atomic_leader):
        self.server.broadCastStateReq()


  def _stateReqResponseHandler(self, msg):
    with self.server.global_lock:
      if self.server.getCoordinatorState() == CoordinatorState.termination:
        # this is because we call it from the termination timeout handler
        # and in that case we didn't receive a message but we still need to
        # no longer consider him
        if msg is not None:
          self.client_state = msg.state

        self.server.deleteResponsesNeeded(self.client_pid)

        if self.server.hasAllResponsesNeeded():
          # run the state termination gathering which will return a msg
          # either a precommit, abort, commit
          self.server.terminationGather()


  def send(self, s):
    with self.server.global_lock:
      if self.isValid():
        self.conn.send(str(s))


  def close(self,isClosed=False):
    with self.server.global_lock:
      if self.isValid():
        try:
          # update global data structures
          # LOG: Also, log last alive set
          self.server.remove_from_cur_transaction(self.getClientPid())

          # if the other guy closed the socket on you, update your alive set
          if isClosed:
            self.server.remove_from_alive_set(self.getClientPid())

          # close the connection socket and send EOF
          self.valid = False
          if not isClosed:
            self.conn.shutdown(socket.SHUT_RDWR)
            self.conn.close()
        except socket.error, e:
          self.server.storage.write_debug(str(e) + "\n[^] Client Socket error while closing")