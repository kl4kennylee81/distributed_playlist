from constants import *
from threading import Thread
from messages import VoteReq
from request_messages import Add, Delete, Get
from crash_request_messages import CrashRequest, VoteNoRequest, CrashAfterVoteRequest, CrashAfterAckRequest, \
  CrashVoteRequest, CrashPartialCommit, CrashPartialPrecommit, deserialize_client_request
from response_messages import ResponseGet


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
    while self.isValid():
      print Add.msg_type
      data = self.master_conn.recv(BUFFER_SIZE)
      deserialized = deserialize_client_request(data, self.server.getTid())
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

  def _transaction_handler(self, deserialized, server):
    with server.global_lock:
      server.add_request(deserialized)
      server.setCoordinatorState(CoordinatorState.votereq)
      voteReq = VoteReq(server.pid, deserialized.serialize())

      crashAfterVoteReq = server.pop_crashVoteReq_request()
      if crashAfterVoteReq != None:
        server.broadCastMessage(voteReq, crashAfterVoteReq.sendTopid)
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

  def _voteNo_handler(self, deserialized, server):
    print("we made it vote no")
    self.server.add_voteNo_request(deserialized)

  def _crashAfterVote_handler(self, deserialized, server):
    print("we made it crash after vote")
    self.server.add_crashAfterVote_request(deserialized)

  def _crashAfterAck_handler(self, deserialized, server):
    print("we made it crash after ack")
    self.server.add_crashAfterAck_request(deserialized)

  def _crashVoteRequest_handler(self, deserialized, server):
    print("we made it crash vote req")
    self.server.add_crashVoteReq_request(deserialized)

  def _crashPartialPrecommit_handler(self, deserialized, server):
    print("we made it crash partial precommit")
    self.server.add_crashPartialPrecommit(deserialized)

  def _crashPartialCommit_handler(self, deserialized, server):
    print("we made it crash partial commit")
    self.server.add_crashPartialCommit(deserialized)