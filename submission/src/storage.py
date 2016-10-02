# For use in transaction acquisiton
from messages import client_req_from_log

class Storage:
  """
  Class to handle writing to various forms of stable storage
  on a per-process basis
  """

  # Constructor 
  def __init__(self, pid):
    # PID of the process to which this storage belongs to
    self.pid = pid
    # DT Log (for state checking on recovering)
    self.dt_log = 'src/db/' + str(self.pid) + '_dt.txt'
    # Disk (for songs)
    self.disk = 'src/db/' + str(self.pid) + '_disk.txt'
    # Debug (for debug loggin)
    self.debug = 'src/db/' + str(self.pid) + '_debug.txt'
    # For successful transactions (adds / deletes + corresponding TID)
    self.transactions = 'src/db/' + str(self.pid) + '_transactions.txt'
    # Last logged alive set of this process
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
      result = [client_req_from_log(line, self.pid)
                for line in [line.rstrip('\n') for line in f]]
    return result 

  # Get an array of log messages from log file
  def get_dt_log(self): 
    with open(self.dt_log, 'r') as f: 
      result = [line for line in [line.rstrip('\n') for line in f]]
    f.close() 
    return result

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

  @staticmethod
  def _append_to_file(file_name, a_log):
    with open(file_name, 'a') as f:
      f.write(a_log + "\n")

  # Write a DT log 
  def write_dt_log(self, a_log):
    Storage._append_to_file(self.dt_log, a_log)

  # Write debug message to file 
  def write_debug(self, debug_log):
    Storage._append_to_file(self.debug, debug_log)

  # Write a transaction to the log
  def write_transaction(self, txn):
    Storage._append_to_file(self.transactions, txn)

  # Update the alive set (stored in file)
  def update_alive_set(self, alive_set):
    serialized_alive_set = ';'.join([str(s) for s in alive_set])
    with open(self.alive_set, 'w') as f:
      f.write(serialized_alive_set)

