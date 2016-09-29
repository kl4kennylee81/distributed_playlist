import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET
from servers import base_server

def main():
  """ 
  Master client has requested another process to start.
  The arguments are in the order [name] [pid] [n] [port] 
  """
  pid, n, port = map(int, sys.argv[1:])
  server = base_server.Server(pid, n, port)
  print "Finished starting up process {} at port {}".format(pid, port)

if __name__ == "__main__":
  main()