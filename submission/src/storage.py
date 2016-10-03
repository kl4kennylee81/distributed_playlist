# For use in transaction acquisiton
from messages import client_req_from_log, Decision, Vote, VoteReq
from constants import Choice

DEBUG=True

def debug_print(s):
  if (DEBUG):
    print s

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


  def get_disk(self):
    """
    Get data from a file + return the appropriate dictionary
    holding mappings (<songName: URL>)
    :return: A dictionary with <songName: URL> key:value pairs
    """
    data = {} 
    with open(self.disk, 'r')  as f:
      for line in [line.rstrip('\n') for line in f]: 
        vals = line.split(',')
        data[vals[0]] = vals[1]
    f.close() 
    return data


  def get_transcations(self):
    """
    Get the listing of transactions
    :return: List of Add or Delete objects
    """
    with open(self.transactions, 'r') as f:
      result = [client_req_from_log(line) \
                for line in [line.rstrip('\n') for line in f]]
    return result 


  def get_dt_log(self):
    """
    Get an array of log messages from log file
    :return: List of String log message (each of the format `tid,log_value`)
    """
    with open(self.dt_log, 'r') as f: 
      result = [line for line in [line.rstrip('\n') for line in f]]
    return result


  def get_last_dt_entry(self):
    """
    Gets the last (most important) entry of the DT Log
    :return: A String (`tid,log_value`)
    """
    result = self.get_dt_log()
    return result[len(result)-1]


  def get_alive_set(self):
    """
    Get the last known alive processes' PIDs
    :return: List of PIDs (e.g. [1, 2, 3], [] if nothing in the file)
    """
    with open(self.alive_set, 'r') as f:
      result = [] if len(f.readlines()) == 0 else [int(p) for p in f.readlines()[0].split(';')]
    return result


  def has_dt_log(self):
    """
    Indicates whether there exists a non-blank DT Log
    :return: Boolean.. False if DT Log is an empty file, True otherwise
    """
    return len(self.get_dt_log()) != 0


  def add_song(self, song_name, song_url):
    """
    Add a song as necessary to disk space
    :param song_name: A song name (String)
    :param song_url: A song URL (String)
    """
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


  def delete_song(self, song_name):
    """
    Remove a song as necessary from disk space
    :param song_name: A song name (String)
    """
    # Read in lines of the file
    f = open(self.disk,"r")
    lines = f.readlines()
    f.close()
    with open(self.disk, "w") as f:
      for line in lines:
        if line.split(",")[0] != song_name:
          f.write(line)


  def update_alive_set(self, alive_set):
    """
    Update the alive set (stored in file)
    :param alive_set: List of PIDs
    """
    serialized_alive_set = ';'.join([str(s) for s in alive_set])
    with open(self.alive_set, 'w') as f:
      f.write(serialized_alive_set)


  @staticmethod
  def _append_to_file(file_name, a_log):
    with open(file_name, 'a') as f:
      f.write(a_log + "\n")


  def write_dt_log(self, msg):
    """
    Log appropriate value `tid,val` to the DT Log
    :param msg: A Decision, Vote, or VoteReq
    :return:
    """
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


  def write_debug(self, debug_message):
    """
    Write debug message to file
    :param debug_log: A String
    """
    debug_message = "PID: {}, LOG: {}".format(self.pid, debug_message)
    debug_print(debug_message)
    Storage._append_to_file(self.debug, debug_message)

  def write_transaction(self, txn):
    """
    Write a transaction to the log
    :param txn: A Request object
    """
    Storage._append_to_file(self.transactions, txn.serialize())
