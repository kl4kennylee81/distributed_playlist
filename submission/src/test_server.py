import socket 
from threading import Thread 
from SocketServer import ThreadingMixIn 

# Multithreaded Python server : TCP Server Socket Thread Pool
class ClientThread(Thread): 
 
    def __init__(self, ip, port, conn): 
        Thread.__init__(self)
        self.ip = ip
        self.port = port
        self.conn = conn
        print "[+] New server socket thread started for " + ip + ":" + str(port)
 
    def run(self): 
        while True: 
            data = self.conn.recv(2048) 

            # client wants to end connection
            if data == 'exit':
                break

            print "Server received data:", data
            MESSAGE = raw_input("Multithreaded Python server : Enter Response from Server/Enter exit: ")
            self.conn.send(MESSAGE) # echo 

            # I want to end connection with client
            if MESSAGE == 'exit':
                break

        print "ending connection with {}:{}".format(ip,port)

if __name__ == "__main__":
    # Multithreaded Python server : TCP Server Socket Program Stub
    TCP_IP = 'localhost' 
    TCP_PORT = 2004 
     
    tcpServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    tcpServer.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # prevents addr already in use problem
    tcpServer.bind((TCP_IP, TCP_PORT))
    threads = []
     
    while True:
        print "Server is listening on {}:{}...".format(TCP_IP, TCP_PORT)
        print "There are currently {} connections active".format(len(threads))
        tcpServer.listen(4) 
        print "Multithreaded Python server : Waiting for connections from TCP clients..." 
        (conn, (ip,port)) = tcpServer.accept()
        newthread = ClientThread(ip,port, conn)
        newthread.start() 
        threads.append(newthread) 
     
    for t in threads: 
        t.join() 