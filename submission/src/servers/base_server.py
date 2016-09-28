import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET

"""
use port 20000+i for each process' server socket, where i is the process id. 
Each process will search ports between 20000 and 20000+n−1 to see which 
process is alive. To then connect to.

The server is passed onto all the handler threads because it essentially acts
as a container for the "global state" of the processes server. Each of the handler
needs to reference the server "Global" to get the current state of this participant
and to interact and work collectively
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
class Server:

    ADDRESS = "localhost"

    START_PORT = 20000

    # this lets the os handle the selection of your ip
    TCP_IP = '0.0.0.0' 

	"""
	pid:the unique_id for the server 
	(that will be used to lookup the transaction log during recovery.)

	n: the number of server processes currently started

	client_port: the port the master client wants us to listen on in which
	the master client then connects to after we spawn

	"""
	def __init__(self, pid, n, client_port):
		self.pid = pid

     
    	master_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    	master_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
    	master_server.bind((TCP_IP, client_port))

		self.other_procs = [None for i in xrange(n-1)]
		self.other_procs_lock = Lock()

		# wait synchronously for the master to connect
		self.master_server.listen()
		(master_conn, (ip,port)) = tcpServer.accept()
		self.master_thread = MasterClientHandler(master_conn)
		self.master_thread.start()


		free_port_no = START_PORT + n
		self.internal_server = ServerConnectionHandler(free_port_no)

		for i in range(n-1):
			port_to_connect = START_PORT + i
			try:
				cur_handler = ClientHandler(i,ADDRESS,port_to_connect,timeout)
				
				self.other_procs_lock.acquire()
				self.other_procs[i] = cur_handler
				self.other_procs[i].start()
				self.other_procs_lock.release()
			except timeout:
				print("port:{} was not there".format(port_to_connect))


	"""
	Handles Incoming connection on the internal facing socket
	That will come from other participant processes. This will create
	a ClientHandler Thread to then service the requests from the
	participant process.

	This is the server socket that newly started Partcipant processes will
	attempt to connect to.
	"""
	class ServerConnectionHandler(Thread):

		def __init__(free_port_no,pid,server):
			Thread.__init__(self)
			self.server = server
			self.pid = pid
			self.internal_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	    	self.internal_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
	    	
	    	self.internal_server.bind((TCP_IP, free_port_no))

	    def run(self):
	    	while(True):
		    	self.internal_server.listen()
		        (conn, (ip,port)) = tcpServer.accept() 
		        print("Pid {}: Connection Accepted".format(pid))
		        
		        # is there a synchronized decorator?
		        server.other_procs_lock.acquire()

		        newClientThread = ClientHandler(conn)
		        server.other_procs.append(newClientThread)
		        newClientthread.start() 

			    server.other_procs_lock.release() 


	"""
	The master client is a special client thread that services
	requests from the master client
	ex. the application requests
	"""
    class MasterClientHandler(Thread):

    	def __init__(self,master_sock):
    		Thread.__init__(self)
    		self.master_conn = master_sock


    	def run(self):
    		while(True):
    			# handle requests from the master node

   	"""
   	Internal server process has an associated client handler thread
   	to handle the request from other internal servers
   	ex. the votes/decisions/precommits etc.
   	"""
	class ClientHandler(Thread):
	    def __init__(self, index, address, port,server,timeout):
	        Thread.__init__(self)
	        self.index = index
	        self.server = server
	        self.sock = socket(AF_INET, SOCK_STREAM)
	        self.sock.create_connect((address, port),timeout=timeout)
	        self.valid = True

	    def __init__(self,index,conn,server):
	    	Thread.__init__(self)
	    	self.index = index
	    	self.server = server
	    	self.sock = conn
	    	self.valid = True

	    def run(self):
	        while self.valid:
	            try:
	            	# TODO This is where the 3 phase commit will live
	            	# this is where the 3 phase commit will be implemented
	            except:
	                # print sys.exc_info()
	                self.valid = False
	                # ERROR HANDLING for WHEN Client Connection CRASHES
	                # del threads[self.index]
	                # self.sock.close()
	                break

	    def send(self, s):
	        if self.valid:
	            self.sock.send(str(s) + '\n')

	    def close(self):
	        try:
	            self.valid = False
	            self.sock.close()
	        except:
	            pass






