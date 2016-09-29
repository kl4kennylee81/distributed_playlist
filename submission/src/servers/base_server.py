import sys, os
import subprocess
import time
from threading import Thread, Lock
import socket
from socket import SOCK_STREAM, AF_INET


BUFFER_SIZE = 1024
START_PORT = 20000
ADDRESS = "localhost"


class MasterClientHandler(Thread):
  """
  A special client thread that services requests from the master client 
  ex. the application requests
  """

  def __init__(self, master_conn):
    Thread.__init__(self)
    self.master_conn = master_conn

  def run(self):
    while(True):
      # TODO: logic for handling application requests
      data = self.master_conn.recv(BUFFER_SIZE)
      print "got message from master client: {}".format(data)


class ClientConnectionHandler(Thread):
  """
  Internal server process has an associated client handler thread to handle the 
  request from other internal servers 
  ex. the votes/decisions/precommits etc.
  """

  def __init__(self):
    Thread.__init__(self)

  @classmethod
  def fromConnection(cls, conn, pid):
    result = cls()
    result.conn = conn
    result.valid = True
    result.pid = pid
    return result

  @classmethod
  def fromAddress(cls, address, port, pid):
    result = cls()
    result.conn = socket.socket(AF_INET, SOCK_STREAM) 
    result.conn.connect((address, port))
    result.valid = True
    result.pid = pid
    return result

  def run(self):
    print "[+] New ClientConnectionHandler"
    while True:
      # TODO: 3 phase commit logic
      break 

  def send(self, s):
      if self.valid:
          self.conn.send(str(s) + '\n')

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

  def __init__(self, free_port_no, pid, other_procs, other_procs_lock):
    Thread.__init__(self)
    self.pid = pid
    self.other_procs = other_procs
    self.other_procs_lock = other_procs_lock

    # this is what all the other participants connect to
    self.internal_server = socket.socket(AF_INET, SOCK_STREAM) 
    self.internal_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.internal_server.bind((ADDRESS, free_port_no))
    print "Initialized server socket at port {}".format(free_port_no)

  def run(self):
    while True:
      self.internal_server.listen(0)
      (conn, (ip,port)) = self.internal_server.accept()
      print("Pid {}: Connection Accepted".format(self.pid))

      with self.other_procs_lock:
        new_client_thread = ClientConnectionHandler.fromConnection(conn)
        self.other_procs.append(new_client_thread)
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
  
  def __init__(self, pid, n, port):
    self.pid = pid
    self.other_procs = [None for i in range(self.pid)]
    self.other_procs_lock = Lock()

    self.master_server = socket.socket(AF_INET, SOCK_STREAM) 
    self.master_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.master_server.bind((ADDRESS, port))

    # wait synchronously for the master to connect
    self.master_server.listen(0)
    (master_conn, (ip, master_client_port)) = self.master_server.accept()
    self.master_thread = MasterClientHandler(master_conn)
    self.master_thread.start()

    # this is the socket that 
    free_port_no = START_PORT + self.pid
    self.internal_server = ServerConnectionHandler(free_port_no, 
                                                    self.pid, 
                                                    self.other_procs, 
                                                    self.other_procs_lock)
    self.internal_server.start()
    print "Starting server connection handler"

    for i in range(self.pid):
      try:
        port_to_connect = START_PORT + i
        cur_handler = ClientConnectionHandler.fromAddress(ADDRESS, 
                                                          port_to_connect)
        with self.other_procs_lock:
          self.other_procs[i] = cur_handler
          self.other_procs[i].start()
      except:
        print sys.exc_info()
        print("Error connecting to port: {}".format(port_to_connect))


  def run(self):
    pass






