import os 

# Class to handle writing to various forms of stable storage 
# on a per-process basis 
class Storage:

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

  # Generic write function 
  def _write_to_file(self, file_name, a_log):
    with open(file_name, 'a') as f:
      f.write(a_log + "\n")
    f.close()

  # Add a song as necessary to disk space 
  def write_song(self, song_name, song_url): 
    self._write_to_file(self.disk, song_name + ',' + song_url)

  # Write a DT log 
  def write_dt_log(self, a_log):
    self._write_to_file(self.dt_log, a_log)

  # Write debug message to file 
  def write_debug(self, debug_log):
    self._write_to_file(self.debug, debug_log)


