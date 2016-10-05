CS 5414

Lab 1

Hong Jeon
hjj24

Kenneth Lee
kl488

Joe Antonakakis
jma353

We set up servers connected via TCP sockets that were each maintained by separate threads.
The servers had a global state contained in a Server object that we locked when accessing on a
per-thread basis.  We also maintained a connection to the master client the entire time.
Threads maintaining connections between processes were ClientConnectionHandlers.
Threads maintaining connection to the master client were MasterClientHandlers.

We wrote classes for internal messaging, building in serializing and deserializing functionality
for sending over sockets.  These we deserialize on receiving a message, and our global server state +
the message contents dictate what action we perform.  This reactive design allowed us to write
handlers on a per-message basis, where we perform all our logic.

We took the same approach for handling messages / requests from the master client.

We wrote a storage driver to handle logging some of our global state (transaction history, DT Logging,
actual songs / urls on disk, crash messages from the master client, debugging) to files that are named
after the pid of the process + what type of log it is.  These enable us to load in our state on recovering
based on the contents of the process' log files.

We use enum34 in our project for Python enum support for enum-compatible values, such as YES / NO votes,
system states (COMMITTED, COMMITTABLE, UNCERTAIN, ABORT), etc.

We kept a series of system constants in a constants.py file.




