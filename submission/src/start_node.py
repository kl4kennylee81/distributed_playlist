import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET
import server

def main():
  """ 
  Master client has requested another process to start.
  The arguments are in the order [name] [pid] [n] [port] 
  """
  pid, n, port = map(int, sys.argv[1:])
  leader = (pid == 0) # TODO: fix this later
  node = server.Server(pid, n, port, leader)
  node.run()
  print "Finished starting up process {} at port {}".format(pid, port)

if __name__ == "__main__":
  main()