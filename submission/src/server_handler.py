from constants import *
import socket
from socket import SOCK_STREAM, AF_INET
from threading import Thread

from client_handler import ClientConnectionHandler
from storage import debug_print

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
    self.valid = True

    # this is what all the other participants connect to
    self.internal_server = socket.socket(AF_INET, SOCK_STREAM)
    self.internal_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.internal_server.bind((ADDRESS, free_port_no))
    debug_print("Initialized server socket at port {}".format(free_port_no))

  def isValid(self):
    with self.server.global_lock:
      return self.server.isValid()

  def run(self):
    while self.isValid():
      self.internal_server.listen(0)
      (conn, (ip,port)) = self.internal_server.accept()
      debug_print("Pid {}: Connection Accepted".format(self.server.pid))

      with self.server.global_lock:
        new_client_thread = ClientConnectionHandler.fromConnection(conn, self.server)
        new_client_thread.start()

  def close(self):
    if self.isValid():
      with self.server.global_lock:
        try:
          # close the listener server socket
          self.internal_server.close()
          self.valid = False
        except socket.error, e:
          self.server.storage.write_debug(str(e) + "\n[^] Server Socket error while closing")
