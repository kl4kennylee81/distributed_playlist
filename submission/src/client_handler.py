from constants import *
import socket
from socket import SOCK_STREAM, AF_INET
from threading import Thread

from messages import VoteReq, PreCommit, Decision, Identifier, Vote, Ack, Reelect, StateReq, StateReqResponse
from messages import deserialize_message

from crash_request_messages import deserialize_client_request

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
      Reelect.msg_type: self._reelectHandler,
      StateReq.msg_type: self._stateReqHandler,
    }

    self.coordinator_handlers = {
      Vote.msg_type: self._voteHandler,
      Ack.msg_type: self._ackHandler,
      Reelect.msg_type: self._nopHandler,
      StateReqResponse.msg_type: self._stateReqResponseHandler,
    }

    # Handlers for timeouts
    self.parti_failureHandler = {
      State.aborted: self._voteReq_timeout,
      State.uncertain: self._preCommit_timeout,
      State.committable: self._commit_timeout,
      State.committed: self._nop_timeout,
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
      while self.isValid():
        data = self.conn.recv(BUFFER_SIZE)

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
    # TODO: figure out what the actual exceptions are
    except (socket.timeout, socket.error) as e:
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
      print "{}.Timeout error handler for client_pid:{}".format(self.server.pid,self.getClientPid())
      self.coord_failureHandler[self.server.getCoordinatorState()]()


  def _participant_timeout_handler(self):
    with self.server.global_lock:
      self.parti_failureHandler[self.server.getState()]()

  """ Participant timeout handlers """


  def _voteReq_timeout(self):
    """
    If a process times out waiting for voteReq, unilaterally abort.
    """
    with self.server.global_lock:
      abort = Decision(self.server.pid, self.server.getTid(), Decide.abort.name)
      abortSerialized = abort.serialize()
      self.server.storage.write_dt_log(abortSerialized)


  def _preCommit_timeout(self):
    print "precommit_timeout"
    self._send_election()


  def _commit_timeout(self):
    print "commit_timeout"
    self._send_election()

  # This is because EOF can be gotten at anytime thus you don't do anything
  # and then the timeout handler will close the thread and socket
  def _nop_timeout(self):
    pass


  def _send_election(self):
    with self.server.global_lock:
      # round robin selection of next leader
      self.server.setCurRequestProcesses()
      print "{}. is it getting set or not {}".format(self.server.pid,self.server.cur_request_set)
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
      print "{}.set_next_leader".format(self.server.pid)
      active_pids = [proc.getClientPid() for proc in self.server.cur_request_set]

      for i in xrange(self.server.n):
        print i
        cur_leader = self.server.getAtomicLeader()+i
        cur_leader_index = cur_leader % self.server.n

        print "{}.cur_leader_index {}".format(self.server.pid,cur_leader_index)

        # skipping processes that aren't in the current transaction anymore
        if not self.server.is_in_cur_transaction(cur_leader_index):
          print "{}. did we escape jail at index {}".format(self.server.pid, cur_leader_index)
          continue

        elif cur_leader_index == self.server.pid:
          # need to set the coordinator State since i'm coordinator now to termination
          self.server.setCoordinatorState(CoordinatorState.termination)
          self.server.setAtomicLeader(cur_leader)
          return

        elif cur_leader_index in active_pids:
          self.server.setAtomicLeader(cur_leader)
          return

      raise Exception("Server:{}... Election result is not found on Client:{}".format(self.server.pid, self.getClientPid()))


  """ Coordinator timeout handlers """


  def _vote_timeout(self):
    """ The coordinator timed out waiting for a vote, so automatically aborts the transaction """
    with self.server.global_lock:
      self.server.broadCastAbort()


  def _ack_timeout(self):
    # the client_pid and thread has already been removed from the set of processes
    # active in this transaction by the exception handler
    with self.server.global_lock:
      print "timed out on acks, entering ackhandler"
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
    with self.server.global_lock:
      self.server.other_procs[msg.pid] = self
      self.setClientPid(msg.pid)

      # this checks if atomic_leader is greater than the self.leader for this process
      # if so, updates the leader
      self.server.setAtomicLeader(msg.atomic_leader)

      if self.server.getLeader() == self.getClientPid():

        # only set timeout with connections to the leader
        # otherwise just have the EOF be caught with internal connection with other participants
        self.conn.settimeout(TIMEOUT_SECS)


  # Participant recieved messages votereq
  def _voteReqHandler(self, msg):
    with self.server.global_lock:
      if self.server.getState() == State.aborted or self.server.getState() == State.committed:
        # in the recovery mode you would check the response id (rid)
        # of the voteReq and also update your state to become consistent

        # Deserialize request and add it to the request queue
        request = deserialize_client_request(msg.request, self.server.getTid())
        self.server.add_request(request)

        forcedNo = self.server.pop_voteNo_request()

        if forcedNo != None:
          choice = Choice.no
        else:
          choice = Choice.yes

        # Log that we voted yes + then vote yes
        choiceMsg = Vote(self.server.pid, self.server.getTid(), choice.name)
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
      print "{}. hit decision handler".format(self.server.pid)
      if self.server.getState() == State.committable or self.server.getState() == State.uncertain:
        if msg.decision == Decide.commit:
          self.server.storage.write_debug("am commiting")
          self.server.commit_cur_request()
        elif msg.decision == Decide.abort:
          self.server.storage.write_debug("am aborting")
          self.server.storage.write_dt_log(msg.serialize())
      else:
        # if out of the commitable stage there can be a abort message
        if msg.decision == Decide.abort:
            self.server.storage.write_dt_log(msg.serialize())


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
    with self.server.global_lock:
      if self.server.getCoordinatorState() == CoordinatorState.precommit:
        self.server.deleteResponsesNeeded(self.client_pid)

        self.client_state = State.committable

        if self.server.hasAllResponsesNeeded():
          self.server.broadCastCommit()

  def _stateReqHandler(self, msg):
    with self.server.global_lock:
      print "hi"
      # respond with my current state
      stateResponseMsg = StateReqResponse(self.server.pid, self.server.getTid(), self.server.getState().name)
      print "{}. sending stateReqResponse -> {}".format(self.server.pid,stateResponseMsg.serialize())
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

      print "{}. calling termination gather {}".format(self.server.pid,self.server.cur_request_set)
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
          print "{}. did we broadcast commit to all".format(self.server.pid)
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
        print("not supposed to be here")



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