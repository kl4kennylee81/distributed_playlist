# For use in transaction acquisiton
from messages import client_req_from_log, Decision, Vote, VoteReq
from constants import Choice

class Storage:
  """
  Class to handle writing to various forms of stable storage
  on a per-process basis

  Fields:
  - pid: PID of the process to which this storage belongs to
  - dt_log: DT Log path (for state checking on recovering)
  - disk: Disk path (for songs)
  - debug: Debug path (for debug logging)
  - valid: is the whole server still active or crashing when False join all the threads
         and then return OR do we just fail ungracefully and just do a sys exit
  - transactions: Transactions path (for successful adds / deletes + corresponding TID)
  - alive_set: Alive set path (last alive processes of this process)
  """

  # Constructor 
  def __init__(self, pid):
    self.pid = pid
    self.dt_log = 'src/db/' + str(self.pid) + '_dt.txt'
    self.disk = 'src/db/' + str(self.pid) + '_disk.txt'
    self.debug = 'src/db/' + str(self.pid) + '_debug.txt'
    self.transactions = 'src/db/' + str(self.pid) + '_transactions.txt'
    self.alive_set = 'src/db/' + str(self.pid) + '_alive_set.txt'

    # Building all these files so we can interact with them for the
    # purposes of storage
    dt = open(self.dt_log, 'a'); dt.close()
    db = open(self.disk, 'a'); db.close()
    log = open(self.debug, 'a'); log.close()
    txn = open(self.transactions, 'a'); txn.close()
    alive = open(self.alive_set, 'a'); alive.close()

  # Get data from a file + return the appropriate 
  # dictionary holding mappings (<songName: URL>) 
  def get_disk(self): 
    data = {} 
    with open(self.disk, 'r')  as f:
      for line in [line.rstrip('\n') for line in f]: 
        vals = line.split(',')
        data[vals[0]] = vals[1]
    f.close() 
    return data

  # Get the listing of transactions
  # (Add or Delete Message objects)
  def get_transcations(self):
    with open(self.transactions, 'r') as f:
      result = [client_req_from_log(line) \
                for line in [line.rstrip('\n') for line in f]]
    return result 

  # Get an array of log messages from log file
  def get_dt_log(self): 
    with open(self.dt_log, 'r') as f: 
      result = [line for line in [line.rstrip('\n') for line in f]]
    f.close() 
    return result

  # Get an the last known processes pids (e.g. [1, 2, 3], [] if nothing in the file)
  def get_alive_set(self):
    with open(self.alive_set, 'r') as f:
      result = [] if len(f.readlines()) == 0 else str(f.readlines()[0].split(';'))
    return result

  # Check to see if a
  def has_dt_log(self):
    return len(self.get_dt_log()) == 0

  # Add a song as necessary to disk space 
  def add_song(self, song_name, song_url):
    # Read in the lines of the file
    f = open(self.disk,"r")
    lines = f.readlines()
    f.close()
    song_in_file = False
    # Write back appropriate values
    with open(self.disk, "w") as f:
      for line in lines:
        if line.split(",")[0] == song_name:
          f.write(song_name + ',' + song_url)
          song_in_file = True
        else:
          f.write(line)
      if not song_in_file:
        f.write(song_name + ',' + song_url)

  # Remove a song as necessary from disk space
  def delete_song(self, song_name):
    # Read in lines of the file
    f = open(self.disk,"r")
    lines = f.readlines()
    f.close()
    with open(self.disk, "w") as f:
      for line in lines:
        if line.split(",")[0] != song_name:
          f.write(line)

  # Update the alive set (stored in file)
  def update_alive_set(self, alive_set):
    serialized_alive_set = ';'.join([str(s) for s in alive_set])
    with open(self.alive_set, 'w') as f:
      f.write(serialized_alive_set)

  # Used in all methods below (b/c just tacking on lines
  # to logs as such
  @staticmethod
  def _append_to_file(file_name, a_log):
    with open(file_name, 'a') as f:
      f.write(a_log + "\n")

  # Write a DT log 
  def write_dt_log(self, msg):
    if isinstance(msg, Decision):
      Storage._append_to_file(self.dt_log, str(msg.tid) + ','
        + msg.decision.name)
    elif isinstance(msg, Vote):
      if msg.choice == Choice.yes:
        Storage._append_to_file(self.dt_log, str(msg.tid) + ','
        + msg.choice.name)
      else: raise Exception("You passed in a NO vote")
    elif isinstance(msg, VoteReq):
      Storage._append_to_file(self.dt_log, str(msg.tid) + ','
        + "votereq")
    else:
      raise Exception("You passed in an invalid message. " +
      "\nThis can't be logged.")

  # Write debug message to file 
  def write_debug(self, debug_log):
    Storage._append_to_file(self.debug, debug_log)

  # Write a transaction to the log
  def write_transaction(self, txn):
    Storage._append_to_file(self.transactions, txn)
