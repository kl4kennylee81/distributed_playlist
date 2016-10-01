# Python TCP Client A
import socket

if __name__ == "__main__":
  host = 'localhost'
  port = 2004
  BUFFER_SIZE = 2000
  MESSAGE = raw_input("tcpClientA: Enter message/ Enter exit:")

  tcpClientA = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  tcpClientA.connect((host, port))

  while True:
    tcpClientA.send(MESSAGE)

    # I want to quit connection with server
    if MESSAGE == 'exit':
      break

    data = tcpClientA.recv(BUFFER_SIZE)
    print " Client2 received data:", data

    # server want's to quit connection
    if data == 'exit':
      break

    MESSAGE = raw_input("tcpClientA: Enter message to continue/ Enter exit:")

  tcpClientA.close()
