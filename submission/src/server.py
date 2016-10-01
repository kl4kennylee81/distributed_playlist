import sys, os
import subprocess
import time
from threading import Thread, Lock
import socket
from socket import SOCK_STREAM, AF_INET
from messages import *
from constants import *


BUFFER_SIZE = 1024
START_PORT = 20000
ADDRESS = "localhost"


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
    }

  def run(self):
    while True:
      # TODO: logic for handling application requests
      data = self.master_conn.recv(BUFFER_SIZE)
      print "got message from master client: {}".format(data)
      self._parse_data(data)


  def _parse_data(self, data):
    deserialized = deserialize_client_req(data, self.server.pid)
    self.handlers[deserialized.type](deserialized, data, self.server)

  def _add_handler(self, deserialized, data, server):
    with server.global_lock:
      server.current_request = deserialized
      server.coordinator_state = CoordinatorState.voteReq
      server.state = State.uncertain
      voteReq = VoteReq(self.pid, data)

      for procs in server.other_procs:
        serialized = voteReq.serialize()
        procs.send(serialized)

  def _get_handler(self):
    pass

  def _delete_handler(self):
    pass


class ClientConnectionHandler(Thread):
  """
  Internal server process has an associated client handler thread to handle the 
  request from other internal servers 
  ex. the votes/decisions/precommits etc.
  """

  def __init__(self):
    Thread.__init__(self)
    self.participant_handlers = {
      # These are coordinator sent messages ###
      VoteReq.msg_type: _voteReqHandler,
      PreCommit.msg_type: _preCommitHandler, 
      Decision.msg_type: _DecisionHandler, 

      #### These are for the special features ####
      # Recover.msg_type: Recover, 
      # Reelect.msg_type: Reelect,
      # StateReq.msg_type: StateReq,
      # StateRepid.msg_type: StateRepid
    }

    self.coordinator_handlers = {
      Vote.msg_type: Vote, _voteHandler,
      Ack.msg_type: Ack, _ackHandler,

      #### These are for the special features ####
      # Recover.msg_type: Recover, 
      # Reelect.msg_type: Reelect,
      # StateReq.msg_type: StateReq,
      # StateRepid.msg_type: StateRepid
    }

  @classmethod
  def fromConnection(cls, conn, server):
    result = cls()
    result.conn = conn
    result.valid = True
    result.result = server
    return result

  @classmethod
  def fromAddress(cls, address, port, server):
    result = cls()
    result.conn = socket.socket(AF_INET, SOCK_STREAM) 
    result.conn.connect((address, port))
    result.valid = True
    result.server = server
    return result

  def run(self):
    print "[+] New ClientConnectionHandler"
    while self.isValid:
      try:
        data = self.conn.recv(BUFFER_SIZE)
        
        msg = deserialize_message(str(data))

        if self.server.leader:
          coordinatorRecv(msg)
        else:
          participantRecv(msg)
        break 
      except:
        # create a state handler here to handle failures
        # for each state, one for coordinator handler failure
        # and one for partcipants handler for his state
        # for example coordinator in wait_acks on a failure
        # would delete the alive guy and increment the count
        # of acks responded

        if (self.server.leader):
          coordinatorStateFailureHandler(self,server)
        else:
          participantStateFailureHandler(self,server)

        self.isValid = False
        return
        # remove self from alive list
        # possibly when other procs is pid -> channel
        # you make the pid key -> None, until some
        # recovered new channel thread takes its place

        # handle network failure, timeouts etc
        # close the channel
        # return the thread

  def coordinatorStateFailureHandler(self,server):
    coord_failureHandler[server.coordinator_state](server)

  def participantStateFailureHandler(self,server):
    parti_failureHandler[server.state](server)

  def _wait_votes_failure(server):

    # mark to state abort and wakeup the main process
    # to send out abort to everyone
    # the master if he is woken up and for example
    # his normal state is changed he will
    # do something dependent on his coordinator state AND his Normal State
    # the participants act only based on their normal state
    decision = Decision(self.pid,Decision.abort)

    for procs in self.other_procs:
      serialized = decision.serialize()
      procs.send(serialized)
      self.coordinator_state = CoordinatorState.standby
      self.state = State.aborted

  def _wait_ack_failure(server):

    # don't mark state as aborted and update the ack_phase_responded+=1
    # take this thread/socket/pid out of the alive set and set the other_procs
    # for this pid to map to None


  '''
  Coordinator recieve only vote and acks from internal
  partcipants
  TODO consider when a coordinator dies and comes back
  and new one is elected how will it recognize its no
  longer the coordinator
  '''
  def coordinatorRecv(self,msg):
    # do error handling if key error that means it got 
    # a message it was not suppose to
    coordinator_handlers[msg.type](msg,self.server)

  '''
  participants recieve votereq,precommit,decisions
  '''
  def participantRecv(self,msg):
    # do error handling if key error that means it got 
    # a message it was not suppose to
    participant_handlers[msg.type](msg,self.server)



  # Participant recieved messages votereq,precommit,decision #
  def _voteReqHandler(msg,server):
    with server.global_lock:
      if (server.state == State.aborted):
        self.state = State.uncertain

        voteRes = Vote(server.pid,Choice.yes)
        self.send(voteRes.serialize())

  def _preCommitHandler(msg,server):
    with server.global_lock:
      if (server.state == State.uncertain):
        self.state = State.committable

        ackRes = Ack(server.pid)
        self.send(ack.serialize())


  def _decisionHandler(msg,server):
    with server.global_lock:
      if (server.state == State.commitable):
        songname,url = self.server.current_request.song_name,self.server.current_request.url
        self.playlist[songname] = url
        self.state = State.committed
      else:
        # maybe raise an error not sure? log something maybe


  # coordinator received messages vote,acks
  def _voteHandler(msg,server):
    with server.global_lock:
      if (self.coordinator_state == votereq):
        self.number_yes+=1
        if self.number_yes == len(self.other_procs):
          self.state = State.committable
          self.coordinator_state = CoordinatorState.precommit
          for procs in self.alive_set:
            serialized = precommit.serialize()
            procs.send(serialized)

        
  def _ackHandler(msg,server):
    with server.global_lock:
      if (self.coordinator_state == precommit)
        server.increment_ack_phase_responded()
        if self.ack_phase_responded == len(self.other_procs):

          server.coordinator_state = CoordinatorState.completed
          server.state = State.committed

          decision = Decision(self.pid,Decision.commit)

          # the server commits when he receives all the acks
          server.commitCurRequest()

          for procs in self.other_procs:
            serialized = decision.serialize()
            procs.send(serialized)


  def send(self, s):
      if self.valid:
          self.conn.send(str(s))

  def close(self):
      try:
          self.valid = False
          self.conn.close()
      except:
          pass


class ServerConnectionHandler(Thread):
  """
  Handles Incoming connection on the internal facing socket
  That will come from other participant processes. This will create
  a ClientConnectionHandler Thread to then service the requests from the
  participant process.

  This is the server socket that newly started partcipant processes will
  attempt to connect to.
  """

  def __init__(self, free_port_no, server):
    Thread.__init__(self)
    self.server = server

    # this is what all the other participants connect to
    self.internal_server = socket.socket(AF_INET, SOCK_STREAM) 
    self.internal_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.internal_server.bind((ADDRESS, free_port_no))
    print "Initialized server socket at port {}".format(free_port_no)

  def run(self):
    while True:
      self.internal_server.listen(0)
      (conn, (ip,port)) = self.internal_server.accept()
      print("Pid {}: Connection Accepted".format(self.server.pid))

      with self.server.global_lock:
        new_client_thread = ClientConnectionHandler.fromConnection(conn, self)
        self.server.other_procs.append(new_client_thread)
        new_client_thread.start()


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
  
  def __init__(self, pid, n, port, leader):
    self.commandRequestExecutors = {
        Add.msg_type: self._add_commit_handler,
        Delete.msg_type: self._delete_commit_handler,
    }

    # all the instance variables in here are "global states" for the server
    self.leader = leader
    self.pid = pid

    # keeping track of current global state
    self.global_lock = Lock()
    self.current_request = None
    self.coordinator_state = CoordinatorState.standby
    self.state = State.aborted
    self.number_yes = 0
    # used when accumulating acks
    # This is a bit different than number yes
    # because if something times out we also
    # increment this counter because we move on
    # after everyone has responded with either an
    # ack or a timeout
    self.ack_phase_responded = 0

    # we then will sent to the people in the alive
    # set removing them if they timed out in the ack
    # phase/ or any phase
    self.alive_set = set()
    
    # playlist is the global dictionary mapping
    # songname -> url
    self.playlist = dict()

    # used for the main process thread to wait until it
    # can make progress on the protocol
    self.main_process_wait = Condition(self.global_lock)

    # TODO maybe update other procs to be a mapping from pid -> thread
    self.other_procs = [None for i in range(self.pid)]

    self.master_server = socket.socket(AF_INET, SOCK_STREAM) 
    self.master_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.master_server.bind((ADDRESS, port))

    # wait synchronously for the master to connect
    self.master_server.listen(0)
    (master_conn, (ip, master_client_port)) = self.master_server.accept()
    self.master_thread = MasterClientHandler(master_conn, self)
    self.master_thread.start()

    # this is the socket that all the other internal participants connect to
    free_port_no = START_PORT + self.pid
    self.internal_server = ServerConnectionHandler(free_port_no, self)
    self.internal_server.start()
    print "Starting server connection handler"

    for i in range(self.pid):
      try:
        port_to_connect = START_PORT + i
        cur_handler = ClientConnectionHandler.fromAddress(ADDRESS, 
                                                          port_to_connect, 
                                                          self)
        with self.global_lock:
          self.other_procs[i] = cur_handler
          self.alive_set.add(cur_handler)
          self.other_procs[i].start()
      except:
        print sys.exc_info()
        print("Error connecting to port: {}".format(port_to_connect))

  def increment_ack_phase_responded(self):
    self.ack_phase_responded+=1

  def commit_cur_request(self):
    if self.current_request != None:
      self.commandRequestExecutors[self.current_request.msg_type](self.current_request)

  def _add_commit_handler(self,request):
    songname,url = self.current_request.song_name,self.current_request.url
    self.playlist[songname] = url
    self.current_request = None

  def _delete_commit_handler(self,request):
    songname = self.current_request.song_name
    del self.playlist[songname]
    self.current_request = None


  def resetState():
    with self.global_lock:
      ack_phase_responded = 0
      number_yes = 0
      self.state = State.aborted
      self.coordinator_state = CoordinatorState.standby


