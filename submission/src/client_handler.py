from constants import *
import socket
from socket import SOCK_STREAM, AF_INET
from threading import Thread

from messages import VoteReq, PreCommit, Decision, Identifier, Vote, Ack
from messages import deserialize_message, deserialize_client_req

from response_messages import ResponseAck

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

  def setClientPid(self, client_pid):
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
        print "this is data", data
        msg = deserialize_message(str(data))
        self.server.storage.write_debug(data)

        if self.server.getLeader():
          self._coordinatorRecv(msg)
        else:
          self._participantRecv(msg)

    except socket.timeout, e:
      self.server.storage.write_debug(str(e) + "\n[^] Timeout error")

      self.valid = False  # TODO: why do we need self.valid?
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
      self.server.storage.write_debug(str(e) + "\n[^] Invalid coordinator state")

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