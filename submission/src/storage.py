import os 


class Storage:
  """
  Class to handle writing to various forms of stable storage
  on a per-process basis
  """

  # Constructor 
  def __init__(self, pid): 
    self.pid = pid
    self.dt_log = './db/' + str(self.pid) + '_dt'
    self.disk = './db/' + str(self.pid) + '_disk'
    self.debug = './db/' + str(self.pid) + '_debug'

  # Get data from a file + return the appropriate 
  # dictionary holding mappings (<songName: URL>) 
  def get_disk(self): 
    data = {} 
    with open(self.disk, 'r') as f:
      for line in [line.rstrip('\n') for line in f]: 
        vals = line.split(',')
        data[vals[0]] = vals[1]
    f.close() 
    return data 

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
  def _append_to_file(self, file_name, a_log):
    with open(file_name, 'a') as f:
      f.write(a_log + "\n")

  # Write a DT log 
  def write_dt_log(self, a_log):
    Storage._append_to_file(self.dt_log, a_log)

  # Write debug message to file 
  def write_debug(self, debug_log):
    Storage._append_to_file(self.debug, debug_log)


