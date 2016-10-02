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

  def _vote_req_sender(self, pid, request):
    connecting_thread = self.server.other_procs[pid]
    if connecting_thread is not None:
      diff_start = connecting_thread.connection_tid
      transactions_diff = [t for t in self.server.get_transaction_history() if t.tid > diff_start]
      vote_req = VoteReq(self.server.pid, self.server.getTid(), request, transactions_diff)
      connecting_thread.send(vote_req.serialize())

  def _get_handler(self, deserialized):
    """
    Processes GET request of data and responds to the master client.
    :param deserialized: deserialized Message
    :param server: an instance of any process
    """
    with self.server.global_lock:
      # deliver back to the master client
      url = self.server.getUrl(deserialized.song_name)
      url_resp = ResponseGet(url)
      self.send(url_resp.serialize())

  def _transaction_handler(self, deserialized):
    with self.server.global_lock:
      self.server.add_request(deserialized)
      self.server.setCoordinatorState(CoordinatorState.votereq)

      # Grab the serialized request "add songName URL"
      request = deserialized.serialize()

      # Compose VOTE-REQ, log, and send to all participants
      voteReq = VoteReq(self.server.pid, self.server.getTid(), request)
      self.server.storage.write_dt_log(voteReq.serialize())

      # Check crash condition
      crashAfterVoteReq = self.server.pop_crashVoteReq_request()
      # If we should crash, send to a subset and then crash
      if crashAfterVoteReq is not None:
        for p in crashAfterVoteReq.sendTopid:
          self._vote_req_sender(p, request)
        self.server.exit()
      # If we shouldn't crash, send to cur_request_set of the server
      else:
        for p in self.server.cur_request_set:
          self._vote_req_sender(p, request)


  def _add_handler(self, deserialized):
    """
    Begins 3-Phase-Commit for the addition of a song
    :param deserialized: deserialized Message
    :param server: an instance of the COORDINATOR
    """
    self._transaction_handler(deserialized)

  def _delete_handler(self, deserialized):
    """
    Begins 3-Phase-Commit for the deletion of a song
    :param deserialized: deserialized Message
    :param server: an instance of the COORDINATOR
    """
    self._transaction_handler(deserialized)

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