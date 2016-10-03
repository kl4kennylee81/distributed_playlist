from constants import *
import socket
from socket import SOCK_STREAM, AF_INET
from threading import Thread

from messages import VoteReq, PreCommit, Decision, Identifier, Vote, Ack, Reelect, StateReq, StateReqResponse
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
      PreCommit.msg_type: self._preCommitHandler,
      Decision.msg_type: self._decisionHandler,
      Reelect.msg_type: self._reelectHandler,
      StateReq.msg_type: self._stateReqHandler,
    }

    self.coordinator_handlers = {
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
    self._send_election()


  def _commit_timeout(self):
    debug_print("commit_timeout")
    self._send_election()

  def _committed_timeout(self):
    debug_print("committed_timeout")
    self._send_election()

  # This is because EOF can be gotten at anytime thus you don't do anything
  # and then the timeout handler will close the thread and socket
  def _nop_timeout(self):
    pass


  """ Coordinator timeout handlers """

  def _send_election(self):
    with self.server.global_lock:
      # round robin selection of next leader
      self.server.setCurRequestProcesses()
      self._set_next_leader()

      if self.server.isLeader():
        if len(self.server.cur_request_set) == 0:
          # if there's literally nobody alive and I just run termination protocol
          # with my own state aka if i was uncertain i abort if im precommit i commit
          # because basically condition 4 is send precommit to everyone else
          # then commit however committing to myself is just commit
          self._terminationGather()
        else:
          # send out the stateReq to everybody that's in the cur_request_set
          self.server.broadCastStateReq()

      else:
        electMsg = Reelect(self.server.pid, self.server.getTid(), self.server.getAtomicLeader())
        newLeaderIndex = self.server.getLeader()
        self.server.other_procs[newLeaderIndex].send(electMsg.serialize())


  def _set_next_leader(self):
    """
    Initiate round robin to select the next leader starting from the pid
    of the previous leader.
    """
    with self.server.global_lock:
      active_pids = [proc.getClientPid() for proc in self.server.cur_request_set]

      for i in xrange(self.server.n):
        cur_leader = self.server.getAtomicLeader()+i
        cur_leader_index = cur_leader % self.server.n

        # skipping processes that aren't in the current transaction anymore
        if not self.server.is_in_cur_transaction(cur_leader_index):
          continue

        elif cur_leader_index == self.server.pid:
          # need to set the coordinator State since i'm coordinator now to termination
          self.server.setCoordinatorState(CoordinatorState.termination)
          self.server.setAtomicLeader(cur_leader)
          return

        elif cur_leader_index in active_pids:
          self.server.setAtomicLeader(cur_leader)
          return

      raise Exception("Server:{}... Election result is not found on Client:{}".format(self.server.pid,
                                                                                      self.getClientPid()))


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
      self.setClientPid(msg.pid)
      self.setClientTid(msg.tid)


      # If we're a recovering server + the responding server is on
      # the same transaction as us
      if self.server.is_recovering and \
      self.server.getState() == State.uncertain and \
      self.getClientTid() == self.server.getTid():

        # This is how we track processes we know about
        self.server.recovered_set.add(msg.pid)

        # Reset this server's intersection to be an intersection
        # of its current intersection and the other process' last_alive_set
        self.server.intersection = \
          set.intersection(self.server.intersection, msg.last_alive_set)

        # R is a superset of the intersection of all the last_alive_sets of the
        # recovered processes
        if self.server.intersection.issubset(self.server.recovered_set):
          self.server.set_is_recovering(False) # We're no longer recovering
          self._send_election()


  # Participant recieved messages votereq
  def _voteReqHandler(self, msg):
    with self.server.global_lock:
      if self.server.getState() == State.aborted \
      or self.server.getState() == State.committed:
        self.server.setState(State.uncertain)

        # If we're getting a VOTE-REQ, we're back in the flow of things
        self.server.set_is_recovering(False)

        # in the recovery mode you would check the response id (rid)
        # of the voteReq and also update your state to become consistent

        # Deserialize request and add it to the request queue
        request = deserialize_client_request(msg.request, self.server.getTid())
        self.server.add_request(request)

        forcedNo = self.server.pop_voteNo_request()

        if forcedNo is not None:
          choice = Choice.no
        else:
          choice = Choice.yes

        # Log that we voted yes + then vote yes
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
      else:
        # if out of the commitable stage there can be a abort message
        if msg.decision == Decide.abort:
            self.server.storage.write_dt_log(msg)


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
        if msg != None:
          self.client_state = msg.state

        self.server.deleteResponsesNeeded(self.client_pid)

        if self.server.hasAllResponsesNeeded():
          # run the state termination gathering which will return a msg
          # either a precommit, abort, commit
          self._terminationGather()


  def _terminationGather(self):
    with self.server.global_lock:

      allStates = [self.server.getState()]

      debug_print("{}. calling termination gather {}".format(self.server.pid,self.server.cur_request_set))
      # first case is check for aborts
      for client_proc in self.server.cur_request_set:
        allStates.append(client_proc.getClientState())

      for tpState in allStates:
        if tpState == State.aborted:
          self.server.broadCastAbort()
          return

      # second case is check for commit
      for tpState in allStates:
        if tpState == State.committed:
          debug_print("{}. did we broadcast commit to all".format(self.server.pid))
          self.server.broadCastCommit()
          return

      # 3rd case is check for all undecided
      num_uncertain = len(allStates)
      for tpState in allStates:
        if tpState == State.uncertain:
          num_uncertain-=1

      if num_uncertain == 0:
        self.server.broadCastAbort()
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
        for client_proc in self.server.cur_request_set:
          if client_proc.getClientState() == State.uncertain:
            sendTopid.add(client_proc)

        # then I am the only one left so i can unilaterally commit
        if len(self.server.cur_request_set) == 0:
          self.server.coordinator_commit_cur_request()

        # There is > 0 other processes in the current transacaction
        # however they are all committable and now you are as well
        elif len(sendTopid) == 0:
          self.server.broadCastCommit()

        # we broadcast a precommit to UNCERTAIN PEOPLE setting num responses
        # needed to the number of uncertain recepients
        else:
          # will set you from if YOU were uncertain -> commitable
          self.server.broadCastPrecommit(sendTopid)
      else:
        debug_print("not supposed to be here")


  def send(self, s):
    with self.server.global_lock:
      if self.isValid():
        self.conn.send(str(s))


  def close(self,isClosed=False):
    with self.server.global_lock:
      if self.isValid():
        try:
          # update global data structures
          self.server.remove_from_cur_transaction(self.getClientPid())

          # close the connection socket and send EOF
          self.valid = False
          if not isClosed:
            self.conn.shutdown(socket.SHUT_RDWR)
            self.conn.close()
        except socket.error, e:
          self.server.storage.write_debug(str(e) + "\n[^] Client Socket error while closing")