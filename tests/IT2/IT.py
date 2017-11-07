#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os.path
import subprocess
import time
import re
import shlex
import filecmp
from adaptative_tbl import *
import syslog
import string
import random
from optparse import OptionParser

fileSize=int(4)
loop=int(32)
process=int(8)
EXPORT_SID_NB=int(8)
STORCLI_SID_NB=int(8)
nbGruyere=int(256)
stopOnFailure=True
fuseTrace=False
DEFAULT_RETRIES=int(20)
tst_file="tst_file"
device_number=""
mapper_modulo=""
mapper_redundancy=""

ifnumber=int(0)
instance=None
site=None
eid=None
vid=None
mnt=None
exepath=None
inverse=None
forward=None
safe=None
nb_failures=None
sids=[]
hosts=[]
verbose=False

#___________________________________________________
# Messages and logs
#___________________________________________________
resetLine="\r"
#___________________________________________________
# output a message in syslog
def log(string): 
#___________________________________________________
  syslog.syslog(string)
#___________________________________________________
# output a message on the console
def console(string): 
#___________________________________________________
  # Reset current line
  clearline()  
  # Add carriage return to the message
  string="\r"+string+"\n"
  # write the message in the buffer
  sys.stdout.write(string)
  # force ouput
  sys.stdout.flush()
#___________________________________________________
# output a message on the console as well as in syslog
def report(string): 
#___________________________________________________
  console(string)
  log(string)  
#___________________________________________________
# Add temporary text to the current line 
def addline(string):
#___________________________________________________
  global resetLine
  # Set bold and yellow effects
  sys.stdout.write(bold+yellow)
  # Add the string in the buffer
  sys.stdout.write(string)
  # End of effects
  sys.stdout.write(endeffect)
  # force ouput
  sys.stdout.flush() 
  # Prepare the reset line that should overwrite 
  # current line with ' '
  resetLine = resetLine + (' ' * len(string)) 
#___________________________________________________
# Clear the temporary text of the current line 
def clearline():
#___________________________________________________
  global resetLine
  # End of effects
  sys.stdout.write(endeffect)
  # Write the reset line that should overwrite 
  # current line text with ' '
  sys.stdout.write(resetLine)
  # force ouput  
  sys.stdout.flush()
  # Restart reset line  
  resetLine="\r"
#___________________________________________________
# Clear current line and restart a new temporary line
def backline(string):
#___________________________________________________
  clearline()
  addline("\r%s"%(string))   
    
    
#___________________________________________________
def clean_cache(val=3): 
#___________________________________________________
  os.system("echo %s > /proc/sys/vm/drop_caches"%val)

#___________________________________________________
def clean_rebuild_dir():
#___________________________________________________
  #os.system("mkdir -p /tmp/rebuild/; mv -f /var/run/storage_rebuild/* /tmp/rebuild/");
  os.system("mkdir -p /tmp/rebuild/");
 
#___________________________________________________
def my_duration (val):
#___________________________________________________

  hour=val/3600  
  min=val%3600  
  sec=min%60
  min=min/60
  return "%2d:%2.2d:%2.2d"%(hour,min,sec)

#___________________________________________________
def reset_counters():
# Use debug interface to reset profilers and some counters
#___________________________________________________
  return

#___________________________________________________
def get_device_numbers(hid,cid):
# Use debug interface to get the number of sid from exportd
#___________________________________________________
  device_number=1
  mapper_modulo=1
  mapper_redundancy=1 

  storio_name="storio:0"
  
  string="./build/src/rozodiag/rozodiag -i localhost%s -T storaged -c storio"%(hid)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for line in cmd.stdout:
    if "mode" in line:
      if "multiple" in line:
        storio_name="storio:%s"%(cid)
      break; 
     
  string="./build/src/rozodiag/rozodiag -i localhost%s -T %s -c device 1> /dev/null"%(hid,storio_name)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  for line in cmd.stdout:
    if "device_number" in line:
      device_number=line.split()[2]
    if "mapper_modulo" in line:
      mapper_modulo=line.split()[2]
    if "mapper_redundancy" in line:
      mapper_redundancy=line.split()[2]
      
  return device_number,mapper_modulo,mapper_redundancy   
#___________________________________________________
def get_if_nb():

  string="./setup.py display"     
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  for line in cmd.stdout:
    if "Listen" in line:
      words=line.split()
      for idx in range(0,len(words)):
        if words[idx] == "Listen": return words[idx+2]
  return 0	

#___________________________________________________
def get_sid_nb():
# Use debug interface to get the number of sid from exportd
#___________________________________________________

  string="./build/src/rozodiag/rozodiag -T mount:%s:1 -c storaged_status"%(instance)       
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  storcli_sid=int(0)
  for line in cmd.stdout:
    if "UP" in line or "DOWN" in line:
      storcli_sid=storcli_sid+1
          
  string="./build/src/rozodiag/rozodiag -T export -c vfstat_stor"
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  export_sid=int(0)
  for line in cmd.stdout:
    if len(line.split()) == 0: continue
    if line.split()[0] != vid: continue;
    if "UP" in line or "DOWN" in line:export_sid=export_sid+1

  return export_sid,storcli_sid    
#___________________________________________________
def reset_storcli_counter():
# Use debug interface to get the number of sid from exportd
#___________________________________________________

  string="./build/src/rozodiag/rozodiag -T mount:%s:1 -T mount:%s:2 -T mount:%s:3 -T mount:%s:4 -c counter reset"%(instance,instance,instance,instance)       
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#___________________________________________________
def check_storcli_crc(expect):
# Use debug interface to get the number of sid from exportd
#___________________________________________________

  string="./build/src/rozodiag/rozodiag -T mount:%s:1 -T mount:%s:2 -T mount:%s:3 -T mount:%s:4 -c profiler"%(instance,instance,instance,instance)       
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  os.system("echo \"\" > /tmp/trace_crc")
  result = False    
  for line in cmd.stdout:
    os.system("echo \"%s\" >> /tmp/trace_crc"%(line))
    if "read_blk_crc" in line:
      result = True
      break; 
  if expect != result:
    os.system("cat /tmp/trace_crc")
  return result  
   
#___________________________________________________
def export_count_sid_up ():
# Use debug interface to count the number of sid up 
# seen from the export. 
#___________________________________________________
  global vid
  
  string="./build/src/rozodiag/rozodiag -T export -c vfstat_stor"
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  match=int(0)
  for line in cmd.stdout:
    if len(line.split()) == 0:
      continue
    if line.split()[0] != vid:
      continue
    if "UP" in line:
      match=match+1

  return match
 
#___________________________________________________
def storcli_count_sid_available ():
# Use debug interface to count the number of sid 
# available seen from the storcli. 
#___________________________________________________

  string="./build/src/rozodiag/rozodiag -T mount:%s:1 -c storaged_status"%(instance)       
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  # Looking for state=UP and selectable=YES
  match=int(0)
  for line in cmd.stdout:
    words=line.split('|')
    if len(words) >= 11:
      if 'YES' in words[6] and 'UP' in words[4]:
        match=match+1
    
  return match  

#___________________________________________________
def loop_wait_until (success,retries,function):
# Loop until <function> returns <success> for a maximum 
# of <retries> attempt (one attempt per second)
#___________________________________________________
  up=int(0)

  while int(up) != int(success):

    retries=retries-1
    if retries == 0:
      print "Maximum retries reached. %s is %s\n"%(function,up)      
      return False
      
    sys.stdout.write(".")
    sys.stdout.flush()     
     
    up=getattr(sys.modules[__name__],function)()
    time.sleep(1)
   
  time.sleep(1)    
  return True  
#___________________________________________________
def loop_wait_until_less (success,retries,function):
# Loop until <function> returns <success> for a maximum 
# of <retries> attempt (one attempt per second)
#___________________________________________________
  up=int(0)

  while int(up) >= int(success):

    retries=retries-1
    if retries == 0:
      print "Maximum retries reached. %s is %s\n"%(function,up)      
      return False
      
    sys.stdout.write(".")
    sys.stdout.flush()     
     
    up=getattr(sys.modules[__name__],function)()
    time.sleep(1)
   
  time.sleep(1)    
  return True    
#___________________________________________________
def start_all_sid () :
# Wait for all sid up seen by storcli as well as export
#___________________________________________________
  for sid in range(STORCLI_SID_NB):
    hid=sid+(site*STORCLI_SID_NB)
    os.system("./setup.py storage %s start"%(hid+1))    
    

      
#___________________________________________________
def wait_until_all_sid_up (retries=DEFAULT_RETRIES) :
# Wait for all sid up seen by storcli as well as export
#___________________________________________________
  if loop_wait_until(STORCLI_SID_NB,retries,'storcli_count_sid_available') == False: 
    print "storcli_count_sid_available %s failed"%(STORCLI_SID_NB)
    return False
  if loop_wait_until(EXPORT_SID_NB,retries,'export_count_sid_up') == False:
    print "export_count_sid_up %s failed"%(EXPORT_SID_NB)
    return False
  return True  
  
    
#___________________________________________________
def wait_until_one_sid_down (retries=DEFAULT_RETRIES) :
# Wait until one sid down seen by storcli 
#___________________________________________________

  if loop_wait_until_less(STORCLI_SID_NB,retries,'storcli_count_sid_available') == False:
    return False
  return True   
#___________________________________________________
def wait_until_x_sid_down (x,retries=DEFAULT_RETRIES) :
# Wait until one sid down seen by storcli 
#___________________________________________________

  if loop_wait_until_less(int(STORCLI_SID_NB)-int(x),retries,'storcli_count_sid_available') == False:
    return False
  return True   
#___________________________________________________
def storageStart (hid,count=int(1)) :

  sys.stdout.write("\r                                   ")
  sys.stdout.write("\rStorage start ")

  for idx in range(int(count)): 
    sys.stdout.write("%s "%(int(hid)+idx)) 
    sys.stdout.flush()
    os.system("./setup.py storage %s start"%(int(hid)+idx))
        
#___________________________________________________
def storageStartAndWait (hid,count=int(1)) :

  storageStart(hid,count)
  time.sleep(1)
  if wait_until_all_sid_up() == True:
    return 0
        
  return 1 
#___________________________________________________
def storageStop (hid,count=int(1)) :

  sys.stdout.write("\r                                   ")
  sys.stdout.write("\rStorage stop ")

  for idx in range(int(count)): 
    sys.stdout.write("%s "%(int(hid)+idx)) 
    sys.stdout.flush()
    os.system("./setup.py storage %s stop"%(int(hid)+idx))
  
#___________________________________________________
def storageStopAndWait (hid,count=int(1)) :

  storageStop(hid,count)
  time.sleep(1)
  wait_until_x_sid_down(count)   
    
#___________________________________________________
def storageFailed (test) :
# Run test names <test> implemented in function <test>()
# under the circumstance that a storage is stopped
#___________________________________________________        
  global hosts     
       
  # Wait all sid up before starting the test     
  if wait_until_all_sid_up() == False:
    return 1
    
  # Loop on hosts
  for hid in hosts:  
   
    # Process hosts in a bunch of allowed failures	   
    if int(nb_failures) != int(1):
      if int(hid)%int(nb_failures) != int(1):
        continue
      	    
    # Reset a bunch of storages	    
    storageStopAndWait(hid,nb_failures)
    reset_counters()
    
    # Run the test
    try:
      # Resolve and call <test> function
      ret = getattr(sys.modules[__name__],test)()         
    except:
      print "Error on %s"%(test)
      ret = 1
      
    # Restart every storages  
    storageStartAndWait(hid,nb_failures)  

    if ret != 0:
      return 1      

      
  return 0

#___________________________________________________
def snipper_storcli ():
# sub process that periodicaly resets the storcli(s)
#___________________________________________________
  
  while True:

      sys.stdout.write("\r                                 ")
      sys.stdout.flush()  
      sys.stdout.write("\rStorcli reset")
      sys.stdout.flush()

      p = subprocess.Popen(["ps","-ef"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      for proc in p.stdout:
        if not "storcli -i" in proc:
          continue
        if "rozolauncher" in proc:
          continue
        if not "%s"%(mnt) in proc:
          continue  
	
        pid=proc.split()[1]
        os.system("kill -9 %s"%(pid))
	    

      for i in range(9):
        sys.stdout.write(".")
        sys.stdout.flush()
        time.sleep(1)

#___________________________________________________
def storcliReset (test):
# Run test names <test> implemented in function <test>()
# under the circumstance where storcli is periodicaly reset
#___________________________________________________

  global loop

  time.sleep(3)
 
  # Start process that reset the storages
  string="./IT2/IT.py --snipper storcli --mount %s"%(mnt)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stderr=subprocess.PIPE)

  saveloop=loop
  loop=loop*2
  
  try:
    # Resolve and call <test> function
    ret = getattr(sys.modules[__name__],test)()         
  except:
    ret = 1
    
  loop=saveloop

  # kill the storcli snipper process
  cmd.kill()

  if ret != 0:
      return 1
  return 0

#___________________________________________________
def snipper_if ():
# sub process that periodicaly resets the storio(s)
#___________________________________________________
  global ifnumber
  
  while True:
      
    for itf in range(0,int(ifnumber)):

      for hid in hosts:     
	  
	sys.stdout.write("\r                                 ")
	sys.stdout.flush()
	sys.stdout.write("\rhost %s if %s down "%(hid,itf))
        sys.stdout.flush()
	
	os.system("./setup.py storage %s ifdown %s"%(hid,itf))
	time.sleep(1)
	
          
	sys.stdout.write("\r                                 ")
	sys.stdout.flush()
	sys.stdout.write("\rhost %s if %s up   "%(hid,itf))
        sys.stdout.flush()
	
	os.system("./setup.py storage %s ifup %s"%(hid,itf))
	time.sleep(0.2)  
#___________________________________________________
def ifUpDown (test):
# Run test names <test> implemented in function <test>()
# under the circumstance that the interfaces goes up and down
#___________________________________________________
  global loop
  
 
  # Start process that reset the storages
  string="./IT2/IT.py --snipper if --mount %s"%(instance)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stderr=subprocess.PIPE)

  saveloop=loop
  loop=loop*8
  
  try:
    # Resolve and call <test> function
    ret = getattr(sys.modules[__name__],test)() 
  except:
    ret = 1

  loop=saveloop

  # kill the storio snipper process
  cmd.kill()

  if ret != 0:
      return 1
  return 0
  
  
#___________________________________________________
def snipper_storage ():
# sub process that periodicaly resets the storio(s)
#___________________________________________________
    
  while True:
    for hid in hosts:      

      # Process hosts in a bunch of allowed failures	   
      if int(nb_failures)!= int(1):
	if int(hid)%int(nb_failures) != int(1): continue

      # Wait all sid up before starting the test     
      if wait_until_all_sid_up() == False: return 1
      
      time.sleep(0.25)
          
      sys.stdout.write("\r                                 ")
      sys.stdout.flush()
      sys.stdout.write("\rStorage reset ")
      cmd=""
      for idx in range(int(nb_failures)):
        val=int(hid)+int(idx)
        sys.stdout.write("%s "%(val))
	cmd+="./setup.py storage %s reset;"%(val)
	
      sys.stdout.flush()
      os.system(cmd)



  
#___________________________________________________
def storageReset (test):
# Run test names <test> implemented in function <test>()
# under the circumstance that storio(s) are periodicaly reset
#___________________________________________________
  global loop
  
 
  # Start process that reset the storages
  string="./IT2/IT.py --snipper storage --mount %s"%(instance)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stderr=subprocess.PIPE)

  saveloop=loop
  loop=loop*8
  
  try:
    # Resolve and call <test> function
    ret = getattr(sys.modules[__name__],test)() 
  except:
    ret = 1
    
  loop=saveloop

  # kill the storio snipper process
  cmd.kill()

  if ret != 0:
      return 1
  return 0

#___________________________________________________
def snipper (target):
# A snipper command has been received for a given target. 
# Resolve function snipper_<target> and call it.
#___________________________________________________

  func='snipper_%s'%(target)
  try:
    ret = getattr(sys.modules[__name__],func)()         
  except:
    print "Failed snipper %s"%(func)
    ret = 1
  return ret  

#___________________________________________________
def wr_rd_total ():
#___________________________________________________
  ret=os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -total -mount %s"%(process,loop,fileSize,tst_file,exepath))
  return ret  

#___________________________________________________
def wr_rd_partial ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -partial -mount %s"%(process,loop,fileSize,tst_file,exepath))

#___________________________________________________
def wr_rd_random ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -random -mount %s"%(process,loop,fileSize,tst_file,exepath))

#___________________________________________________
def wr_rd_total_close ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -total -file %s -closeAfter -mount %s"%(process,loop,fileSize,tst_file,exepath))

#___________________________________________________
def wr_rd_partial_close ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -partial -file %s -closeAfter -mount %s"%(process,loop,fileSize,tst_file,exepath))

#___________________________________________________
def wr_rd_random_close ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -random -file %s -closeAfter -mount %s"%(process,loop,fileSize,tst_file,exepath))

#___________________________________________________
def wr_close_rd_total ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -total -closeBetween -mount %s"%(process,loop,fileSize,tst_file,exepath))

#___________________________________________________
def wr_close_rd_partial ():
#___________________________________________________
  ret=os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -partial -closeBetween -mount %s"%(process,loop,fileSize,tst_file,exepath))
  return ret 

#___________________________________________________
def wr_close_rd_random ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -random -closeBetween -mount %s"%(process,loop,fileSize,tst_file,exepath))

#___________________________________________________
def wr_close_rd_total_close ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -total -closeBetween -closeAfter -mount %s"%(process,loop,fileSize,tst_file,exepath))

#___________________________________________________
def wr_close_rd_partial_close ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -partial -closeBetween -closeAfter -mount %s"%(process,loop,fileSize,tst_file,exepath))

#___________________________________________________
def wr_close_rd_random_close ():
#___________________________________________________
  return os.system("./IT2/rw.exe -process %d -loop %d -fileSize %d -file %s -random -closeBetween -closeAfter -mount %s"%(process,loop,fileSize,tst_file,exepath))

#___________________________________________________
def rw2 ():
#___________________________________________________
  return os.system("./IT2/rw2.exe -loop %s -file %s/%s"%(loop,exepath,tst_file))
#___________________________________________________
def write_parallel ():
#___________________________________________________
  return os.system("./IT2/write_parallel.exe -file %s/%s"%(exepath,tst_file))

#___________________________________________________
def prepare_file_to_read(filename,mega):
#___________________________________________________

  if not os.path.exists(filename):
    os.system("dd if=/dev/zero of=%s bs=1M count=%s 1> /dev/null"%(filename,mega))    

#___________________________________________________
def read_parallel ():
#___________________________________________________

  zefile='%s/%s'%(exepath,tst_file)
  prepare_file_to_read(zefile,fileSize) 
  ret=os.system("./IT2/read_parallel.exe -process %s -loop %s -file %s"%(process,loop,zefile)) 
  return ret 

 
#___________________________________________________
def crc32():
#___________________________________________________

  backline("Create file %s/crc32"%(exepath))

  crcfile="%s/crc32"%(exepath)
  # Create a file
  if os.path.exists(crcfile): os.remove(crcfile)      
  os.system("cp ./ref %s"%(crcfile))  
  
  # Clear error counter
  reset_storcli_counter()
  # Check CRC errors 
  if check_storcli_crc(False) == True:
    report("CRC errors after counter reset")
    return 1 
    
  # Get its localization  
  os.system("./setup.py cou %s > /tmp/crc32loc"%(crcfile))
      
  # Find the 1rst bins file
  bins = None
  with open("/tmp/crc32loc","r") as f: 
    for line in f.readlines():
      if "/bins_0/" in line:
        bins = line.split()[3]
        break;
  if bins == None:
    report("Fail to find bins file name in /tmp/crc32loc")
    return -1    

  # Corrupt the bins file
  f = open(bins, 'r+b')     
  f.seek(1211) 
  f.write("DDT")
  f.seek(3111) 
  f.write("DDT")
  f.seek(4444) 
  f.write("DDT")    
  f.seek(1024*112) 
  f.write("DDT")    
  f.seek(1024*114) 
  f.write("DDT")    
  f.seek(1024*115) 
  f.write("DDT")    
  f.seek(1024*118) 
  f.write("DDT")    
  f.seek(1024*120) 
  f.write("DDT")    
  size = os.path.getsize(bins)
  f.seek(size-11);
  f.write("DDT")       
  f.close()
  backline("Corrupt bins file ")

  # Reset storages
  os.system("./setup.py storage all reset; echo 3 > /proc/sys/vm/drop_caches")
  wait_until_all_sid_up()
 
  # Clear error counter
  reset_storcli_counter()
  
  # Reread the file
  os.system("dd of=/dev/null if=%s bs=1M > /dev/null 2>&1"%(crcfile))  

  time.sleep(1)
 
  # Checl for CRC32 errors
  if check_storcli_crc(True) == True:
    backline("Repair procedure has been run ")
  else:     
    report("No CRC errors after file reread")
    return 1 

  # Reset storages
  os.system("./setup.py storage all reset")
  wait_until_all_sid_up()
 
  # Clear error counter
  reset_storcli_counter()
  
  # Reread the file
  os.system("dd of=/dev/null if=%s bs=1M > /dev/null 2>&1"%(crcfile))  
 
  # Checl for CRC32 errors
  if check_storcli_crc(False) == True:
    report("Bins file still has errors")
    return 1     
  backline("Bins file is repaired")
  
  if filecmp.cmp(crcfile,"./ref") == False: 
    report("%s and %s differ"%(crcfile,"./ref"))
    return 1
    
  return 0 
 
#___________________________________________________
def xattr():
#___________________________________________________
  return os.system("./IT2/test_xattr.exe -process %d -loop %d -mount %s"%(process,loop,exepath))

#___________________________________________________
def link():
#___________________________________________________
  return os.system("./IT2/test_link.exe -process %d -loop %d -mount %s"%(process,loop,exepath))

#___________________________________________________
def symlink():
#___________________________________________________
  return os.system("./IT2/test_symlink.exe -process %d -loop %d -mount %s"%(process,loop,exepath))

#___________________________________________________
def readdir():
#___________________________________________________ 
  return os.system("./IT2/test_readdir.exe -process %d -loop %d -mount %s"%(process,loop,exepath))

#___________________________________________________
def rename():
#___________________________________________________
  ret=os.system("./IT2/test_rename.exe -process %d -loop %d -mount %s"%(process,loop,exepath))
  return ret 

#___________________________________________________
def chmod():
#___________________________________________________
  return os.system("./IT2/test_chmod.exe -process %d -loop %d -mount %s"%(process,loop,exepath))

#___________________________________________________
def truncate():
#___________________________________________________
  return os.system("./IT2/test_trunc.exe -process %d -loop %d -mount %s"%(process,loop,exepath))

#___________________________________________________
def makeBigFName(c):
#___________________________________________________
  FNAME="%s/bigFName/"%(exepath)
  for i in range(510): FNAME=FNAME+c
  return FNAME

#___________________________________________________
def bigFName():
#___________________________________________________
  charList="abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_-#:$@:=.;"
  
  if os.path.exists("%s/bigFName"%(exepath)):
    os.system("rm -rf %s/bigFName"%(exepath))
  
  if not os.path.exists("%s/bigFName"%(exepath)):
    os.system("mkdir -p %s/bigFName"%(exepath))
    
  for c in charList:
    FNAME=makeBigFName(c)
    f = open(FNAME, 'w')
    f.write(FNAME) 
    f.close()
    
  for c in charList:
    FNAME=makeBigFName(c)
    f = open(FNAME, 'r')
    data = f.read(1000) 
    f.close()   
    if data != FNAME:
      syslog.syslog("%s\nbad content %s\n"%(FNAME,data))
      return -1
  return 0
	  
#___________________________________________________
def lock_race():
#___________________________________________________ 
  zefile='%s/%s'%(exepath,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  
  return os.system("./IT2/test_lock_race.exe -process %d -loop %d -file %s "%(process,loop,zefile))  
#___________________________________________________
def lock_posix_passing():
#___________________________________________________ 
  zefile='%s/%s'%(exepath,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  
  return os.system("./IT2/test_file_lock.exe -process %d -loop %d -file %s -nonBlocking"%(process,loop,zefile))  

#___________________________________________________
def lock_posix_blocking():
#___________________________________________________
  zefile='%s/%s'%(exepath,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  

  ret=os.system("./IT2/test_file_lock.exe -process %d -loop %d -file %s"%(process,loop,zefile))
  return ret 

#___________________________________________________
def lock_bsd_passing():
#___________________________________________________  
  zefile='%s/%s'%(exepath,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  
  return os.system("./IT2/test_file_lock.exe -process %d -loop %d -file %s -nonBlocking -bsd"%(process,loop,zefile))


#___________________________________________________
def quiet(val=10):
#___________________________________________________

  while True:
    time.sleep(val)


#___________________________________________________
def lock_bsd_blocking():
#___________________________________________________
  zefile='%s/%s'%(exepath,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  
  return os.system("./IT2/test_file_lock.exe -process %d -loop %d -file %s -nonBlocking"%(process,loop,zefile))  
#___________________________________________________
def check_one_criteria(attr,f1,f2):
#___________________________________________________
  one=getattr(os.stat(f1),attr)
  two=getattr(os.stat(f2),attr)
  try: 
    one=int(one)
    two=int(two)
  except: pass  
  #print "%s %s %s"%(attr,one,two)
  if one != two:
    print "%s %s for %s"%(one,attr,f1)
    print "%s %s for %s"%(two,attr,f2)
    return False 
  return True

#___________________________________________________
def check_rsync(src,dst):
#___________________________________________________
  criterias=['st_nlink','st_size','st_mode','st_uid','st_gid','st_mtime']
  
  for dirpath, dirnames, filenames in os.walk(src):

    d1 = dirpath
    d2 = "%s/rsync_dest/%s"%(exepath,dirpath[len(src):]) 

    if os.path.exists(d1) == False:
      print "source directory %s does not exist"%(d1)
      return False

    if os.path.exists(d2) == False:
      print "destination directory %s does not exist"%(d2)
      return False

    for criteria in criterias:
      if check_one_criteria(criteria,d1,d2) == False:
	return False
    
    for fileName in filenames:
    	  
      f1 = os.path.join(dirpath, fileName)
      f2 = "%s/rsync_dest/%s/%s"%(exepath,dirpath[len(src):], fileName) 

      if os.path.exists(f1) == False:
        print "source file %s does not exist"%(f1)
	return False
	
      if os.path.exists(f2) == False:
        print "destination file %s does not exist"%(f2)
	return False
 
      for criteria in criterias:
        if check_one_criteria(criteria,f1,f2) == False:
	  return False

      if filecmp.cmp(f1,f2) == False:
	print "%s and %s differ"
	return False
  return True
#___________________________________________________
def internal_rsync(src,count,delete=False):
#___________________________________________________
    
  if delete == True: os.system("/bin/rm -rf %s/rsync_dest; mkdir -p %s/rsync_dest"%(exepath,exepath))

  bytes=0
  string="rsync -aHr --stats %s/ %s/rsync_dest"%(src,exepath)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for line in cmd.stdout:
    if "Total transferred file size:" in line:
      bytes=line.split(':')[1].split()[0]  

  if int(bytes) != int(count):
    print "%s bytes transfered while expecting %d!!!"%(bytes,count)
    return False
  return check_rsync(src,"%s/rsync_dest"%(exepath))
#___________________________________________________
def create_rsync_file(f,rights,owner=None):
#___________________________________________________
 size = random.randint(1,50)
 string=''.join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(size))
 os.system("echo %s > %s"%(string,f))
 os.system("chmod %s %s"%(rights,f))
 if owner != None:
  os.system("chown -h %s:%s %s"%(owner,owner,f)) 
 return int(os.stat(f).st_size)
#___________________________________________________
def create_rsync_hlink(f,h):
#___________________________________________________
  os.system("ln %s %s"%(f,h))
#___________________________________________________
def create_rsync_slink(f,s,owner=None):
#___________________________________________________
  os.system("ln -s %s %s"%(f,s))
  if owner != None:
    os.system("chown -h %s:%s %s"%(owner,owner,s))   

#___________________________________________________
def create_rsync_dir(src):
#___________________________________________________
  os.system("/bin/rm -rf %s/"%(src))
  os.system("mkdir -p %s"%(src))

  size = int(0)

  size =        create_rsync_file("%s/A"%(src),"777")
  size = size + create_rsync_file("%s/B"%(src),"700")
  size = size + create_rsync_file("%s/C"%(src),"754")
  
  size = size + create_rsync_file("%s/a"%(src),"774","rozo")
  size = size + create_rsync_file("%s/b"%(src),"744","rozo")
  size = size + create_rsync_file("%s/c"%(src),"750","rozo")
  
  create_rsync_hlink("%s/A"%(src),"%s/HA"%(src))
  create_rsync_hlink("%s/B"%(src),"%s/HB"%(src))
  create_rsync_hlink("%s/C"%(src),"%s/HC"%(src))

  create_rsync_hlink("%s/a"%(src),"%s/ha1"%(src))
  create_rsync_hlink("%s/a"%(src),"%s/ha2"%(src))
  create_rsync_hlink("%s/a"%(src),"%s/ha3"%(src))

  create_rsync_slink("A","%s/SA"%(src))
  create_rsync_slink("B","%s/SB"%(src))
  create_rsync_slink("C","%s/SC"%(src))
  create_rsync_slink("c","%s/Sc"%(src))
  
  create_rsync_slink("a","%s/sa"%(src),"rozo")
  create_rsync_slink("b","%s/sb"%(src),"rozo")
  create_rsync_slink("c","%s/sc"%(src),"rozo")
  create_rsync_slink("C","%s/sC"%(src),"rozo")
  return size

  


#___________________________________________________
def touch_one_file(f):
  os.system("touch %s"%(f))
  return int(os.stat(f).st_size)
#___________________________________________________
def rsync():

  src="%s/rsync_source"%(exepath) 
  size = create_rsync_dir(src)
  size = size + create_rsync_dir(src+'/subdir1')
  size = size + create_rsync_dir(src+'/subdir1/subdir2')
  size = size + create_rsync_dir(src+'/subdir1/subdir2/subdir3')
  size = size + create_rsync_dir(src+'/subdir1/subdir2/subdir4')
     
  if internal_rsync(src,size,True) == False: return 1 
   
  time.sleep(2)
  if internal_rsync(src,int(0)) == False: return 1

  size = touch_one_file("%s/a"%(src))
  time.sleep(2)
  if internal_rsync(src,size) == False: return 1
  
  size = touch_one_file("%s/HB"%(src))
  size = size + touch_one_file("%s/subdir1/a"%(src))
  time.sleep(2)
  if internal_rsync(src,size) == False: return 1 
   
  size = touch_one_file("%s/c"%(src))
  size = size + touch_one_file("%s/subdir1/subdir2/subdir3/ha1"%(src))
  size = size + touch_one_file("%s/subdir1/subdir2/B"%(src))
  time.sleep(2)
  if internal_rsync(src,size) == False: return 1  
  
  time.sleep(2)
  if internal_rsync(src,int(0)) == False: return 1  
   
  return 0
#___________________________________________________
def is_elf(name):
  string="file %s/git/build/%s"%(exepath,name)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for line in cmd.stdout:
    if "ELF" in line: return True
  print "%/git/build/%s not generated"%(exepath,name)
  return False
     
def compil():
  os.system("cd %s; rm -rf git; mkdir git; git clone https://github.com/rozofs/rozofs.git git 1> /dev/null; cd git; mkdir build; cd build; cmake -G \"Unix Makefiles\" ../ 1> /dev/null; make -j16 1> /dev/null"%(exepath))
  if is_elf("src/rozodiag/rozodiag") == False: return 1
  if is_elf("src/exportd/exportd") == False: return 1
  if is_elf("src/rozofsmount/rozofsmount") == False: return 1
  if is_elf("src/storcli/storcli") == False: return 1
  if is_elf("src/storaged/storaged") == False: return 1
  if is_elf("src/storaged/storio") == False: return 1
  return 0     
 
#___________________________________________________
def gruyere_one_reread():
# reread files create by test_rebuild utility to check
# their content
#___________________________________________________ 
  clean_cache()
  syslog.syslog("re-read %d files"%(int(nbGruyere)))
  res=cmd_returncode("./IT2/test_rebuild.exe -action check -nbfiles %d -mount %s"%(int(nbGruyere),exepath))
  syslog.syslog("re-read result %s"%(res))
  return res
  
#___________________________________________________
def gruyere_reread():
# reread files create by test_rebuild with every storage
# possible fault to check every projection combination
#___________________________________________________

  ret = gruyere_one_reread()
  if ret != 0:
    return ret
    
  ret = storageFailed('gruyere_one_reread')
  time.sleep(3)
  return ret

#___________________________________________________
def gruyere_write():
# Use test_rebuild utility to create a bunch of files
#___________________________________________________ 
  return os.system("rm -f %s/rebuild/*; ./IT2/test_rebuild.exe -action create -nbfiles %d -mount %s"%(exepath,int(nbGruyere),exepath))  
#___________________________________________________
def gruyere():
# call gruyere_write that create a bunch of files while
# block per block while storages are reset. This makes
# files with block dispersed on every storage. 
#___________________________________________________

  ret = storageReset('gruyere_write')
  if ret != 0:  
    return ret
  if rebuildCheck == True: 
    return gruyere_reread()
  return 0
#___________________________________________________
def rebuild_1dev() :
# test rebuilding device per device
#___________________________________________________
  global sids

  ret=1 
  for s in sids:
    
    hid=s.split('-')[0]
    cid=s.split('-')[1]
    sid=s.split('-')[2]
        
    device_number,mapper_modulo,mapper_redundancy = get_device_numbers(hid,cid)
    
    dev=int(hid)%int(mapper_modulo)
    clean_rebuild_dir()    
    string="./setup.py sid %s %s rebuild -fg -d %s -o one_cid%s_sid%s_dev%s"%(cid,sid,dev,cid,sid,dev)
    os.system("./setup.py sid %s %s device-clear %s"%(cid,sid,dev))
    ret = cmd_returncode(string)
    if ret != 0:
      return ret
      
    if int(mapper_modulo) > 1:
      dev=(dev+1)%int(mapper_modulo)
      os.system("./setup.py sid %s %s device-clear %s"%(cid,sid,dev))
      ret = cmd_returncode("./setup.py sid %s %s rebuild -fg -d %s -o one_cid%s_sid%s_dev%s "%(cid,sid,dev,cid,sid,dev))
      if ret != 0:
	return ret
	
    if rebuildCheck == True:      
      ret = gruyere_one_reread()  
      if ret != 0:
        return ret 

  if rebuildCheck == True:      
    ret = gruyere_reread()          
    return ret
  return 0
#___________________________________________________
def relocate_1dev() :
# test rebuilding device per device
#___________________________________________________


  ret=1 
  modulo=1
  selfHealing="No"
  for s in sids:
    
    if modulo == 3:
      modulo=1
    else:
      modulo = modulo + 1
      continue
        
    hid=s.split('-')[0]
    cid=s.split('-')[1]
    sid=s.split('-')[2]


    # Check wether automount is configured
    string="./build/src/rozodiag/rozodiag -i localhost%s -T storio:%s -c cc | grep 'device_automount ' "%(hid,cid)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    automount = False
    for line in cmd.stdout:
      # No automount 
      if "False" in line:
  	automount = False
	break
      if "True" in line:
        automount = True
        break
        
    # Check wether self healing is configured
    string="./build/src/rozodiag/rozodiag -i localhost%s -T storio:%s -c cc | grep device_selfhealing_mode"%(hid,cid)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    delay       = 1
    selfHealing ="spareOnly"
    for line in cmd.stdout:
      if "spareOnly" in line:
  	selfHealing="spareOnly"
	continue
      if "relocate" in line:
  	selfHealing="relocate"  
	continue          
        
    clean_rebuild_dir()
    
    
    # Create a spare device in automount mode
    if automount == True:
      syslog.syslog("automount %s and selHealing %s : Wait rebuild on spare"%(automount,selfHealing))	          
      os.system("./setup.py spare")
      waitRebuild = True
      	      
    else :

      # No automount and spare only          
      if selfHealing == "spareOnly":
        syslog.syslog("automount %s and selHealing %s : call relocate"%(automount,selfHealing))	          
        ret = cmd_returncode("./setup.py sid %s %s rebuild -fg -d 0 -R -o reloc_cid%s_sid%s_dev0 "%(cid,sid,cid,sid))
        if ret != 0: return ret
        waitRebuild = False
        status="IS"
        
      # No automount but relocate enabled	      
      if selfHealing == "relocate":
        syslog.syslog("automount %s and selHealing %s : Wait relocate"%(automount,selfHealing))	          
        waitRebuild = True
	
    # Wait for selhealing	
    if waitRebuild == True:
    
      ret = os.system("./setup.py sid %s %s device-delete 0"%(cid,sid))
      if ret != 0:
	return ret
	      	
      count=int(50)	
      status="INIT"
      
      while count != int(0):

	if "OOS" == status:
	  syslog.syslog("count %d device is %s"%(count,status))
	  break    
	      
	if "IS" == status:
	  syslog.syslog("count %d device is %s"%(count,status))
	  break        

        count=count-1
        time.sleep(10)
	
        # Check The status of the device
        string="./build/src/rozodiag/rozodiag -i localhost%s -T storio:%s -c device "%(hid,cid)
	parsed = shlex.split(string)
	cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	curcid=0
	cursid=0
	for line in cmd.stdout:
	
          # Read cid sid
	  if "cid =" in line and "sid =" in line:
	    words=line.split()
	    curcid=int(words[2])
	    cursid=int(words[5])
	    continue
	      
          if int(curcid) != int(cid) or int(cursid) != int(sid): continue 
          words=line.split('|')
	  try:
	    if int(words[0]) != int(0):
	      continue
	    status=words[1].split()[0]
	    break
	  except:
	    pass    
	    
      if count == int(0):
        print "Relocate failed host %s cluster %s sid %s device 0 status %s"%(hid,cid,sid,status)
	return 1
	
    if rebuildCheck == True:		      
      ret = gruyere_one_reread()  
      if ret != 0:
	return ret 
      
    if status == "OOS":
      # Re create the device
      ret = os.system("./setup.py sid %s %s device-create 0"%(cid,sid))
      # re initialize it
      ret = os.system("./setup.py sid %s %s rebuild -fg -d 0 -K"%(cid,sid))
      time.sleep(11)
      ret = os.system("./setup.py sid %s %s rebuild -fg -d 0"%(cid,sid))
            
  if rebuildCheck == True:      
    ret = gruyere_reread()          
    return ret         
  return 0

#___________________________________________________
def rebuild_all_dev() :
# test re-building all devices of a sid
#___________________________________________________

  ret=1 
  for s in sids:
    
    hid=s.split('-')[0]
    cid=s.split('-')[1]
    sid=s.split('-')[2]

    clean_rebuild_dir()

    os.system("./setup.py sid %s %s device-clear all 1> /dev/null"%(cid,sid))
    ret = cmd_returncode("./setup.py sid %s %s rebuild -fg -o all_cid%s_sid%s "%(cid,sid,cid,sid))
    if ret != 0:
      return ret

    if rebuildCheck == True:	
      ret = gruyere_one_reread()  
      if ret != 0:
	return ret    

  if rebuildCheck == True:      
    ret = gruyere_reread()          
    return ret
  return 0  

#___________________________________________________
def rebuild_1node() :
# test re-building a whole storage
#___________________________________________________
  global hosts
  global sids
    
  ret=1 
  # Loop on every host
  for hid in hosts:
 
    # Delete every device of every CID/SID on this host
    for s in sids:

      zehid=s.split('-')[0]
      if int(zehid) != int(hid): continue
      
      cid=s.split('-')[1]
      sid=s.split('-')[2]
      
      os.system("./setup.py sid %s %s device-clear all 1> /dev/null"%(cid,sid))

    clean_rebuild_dir()
    
    string="./setup.py storage %s rebuild -fg -o node_%s"%(hid,hid)
    ret = cmd_returncode(string)
    if ret != 0:
      return ret

    if rebuildCheck == True:	
      ret = gruyere_one_reread()  
      if ret != 0:
	return ret    

  if rebuildCheck == True:      
    ret = gruyere_reread()          
    return ret
  return 0 
#___________________________________________________
def rebuild_1node_parts() :
# test re-building a whole storage
#___________________________________________________
  global hosts
  global sids
    
  ret=1 
  # Loop on every host
  for hid in hosts:
 
    # Delete every device of every CID/SID on this host
    for s in sids:

      zehid=s.split('-')[0]
      if int(zehid) != int(hid): continue
      
      cid=s.split('-')[1]
      sid=s.split('-')[2]
      
      os.system("./setup.py sid %s %s device-clear all 1> /dev/null"%(cid,sid))

    clean_rebuild_dir()
    
    string="./setup.py storage %s rebuild -fg -o node_nominal_%s --nominal"%(hid,hid)
    ret = cmd_returncode(string)
    if ret != 0:
      return ret
    
    string="./setup.py storage %s rebuild -fg -o node_spare_%s --spare"%(hid,hid)
    ret = cmd_returncode(string)
    if ret != 0:
      return ret

    if rebuildCheck == True:	
      ret = gruyere_one_reread()  
      if ret != 0:
	return ret    

  if rebuildCheck == True:      
    ret = gruyere_reread()          
    return ret
  return 0    
#___________________________________________________
def delete_rebuild() :
# test re-building a whole storage
#___________________________________________________
  os.system("rm -rf %s/rebuild  1> /dev/null"%(exepath))
  return 0
#___________________________________________________
def rebuild_fid() :
# test rebuilding per FID
#___________________________________________________
  skip=0

  for f in range(int(nbGruyere)/10):
  
    skip=skip+1
    if skip == 4:
      skip=0  

    # Get the split of file on storages      
#    string="./setup.py cou %s/rebuild/%d"%(mnt,f+1)
    string="attr -g rozofs %s/rebuild/%d"%(mnt,f+1)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # loop on the bins file constituting this file, and ask
    # the storages for a rebuild of the file
    bins_list = []
    fid=""
    cid=0
    storages=""
    for line in cmd.stdout:
	  
      if "FID" in line:
        words=line.split();
	if len(words) >= 2:
          fid=words[2]
	  continue
	  
      if "CLUSTER" in line:
        words=line.split();
	if len(words) >= 2:
          cid=words[2]
	  continue
	  
      if "STORAGE" in line:
        words=line.split();
	if len(words) >= 2:
          storages=words[2]
	  continue	  	  	    
      
    # loop on the bins file constituting this file, and ask
    # the storages for a rebuild of the file
    line_nb=0

    for sid in storages.split('-'):
    
      sid=int(sid)
      line_nb=line_nb+1
      if skip >= line_nb:
	  continue;  

      clean_rebuild_dir()
	    	
      string="./setup.py sid %s %s rebuild -fg -f %s -o fid%s_cid%s_sid%s"%(cid,sid,fid,fid,cid,sid)
      ret = cmd_returncode(string)

      if ret != 0:
        print "%s failed"%(string)
	return 1 	       

  if rebuildCheck == True:      
    ret = gruyere_reread()          
    return ret
  return 0   

#___________________________________________________
def append_circumstance_test_list(list,input_list,circumstance):
# Add to <list> the list <input_list> prefixed with 
# <circumstance> that should be a valid circumstance test list.
# function <circumstance>() should exist to implement this
# particuler test circumstance.
#___________________________________________________

   for tst in input_list:
     list.append("%s/%s"%(circumstance,tst)) 

#___________________________________________________
def do_compile_program(program): 
# compile program if program.c is younger
#___________________________________________________

  if not os.path.exists("%s.exe"%(program)) or os.stat("%s.exe"%(program)).st_mtime < os.stat("%s.c"%(program)).st_mtime:
    os.system("gcc -g %s.c -lpthread -o %s.exe"%(program,program))

#___________________________________________________
def do_compile_programs(): 
# compile all program if program.c is younger
#___________________________________________________
  dirs=os.listdir("%s/IT2"%(os.getcwd()))
  for file in dirs:
    if ".c" not in file:
      continue
    words=file.split('.')
    prg=words[0]   
    do_compile_program("IT2/%s"%(prg))

#___________________________________________________
def do_run_list(list):
# run a list of test
#___________________________________________________
  global tst_file
  
  tst_num=int(0)
  failed=int(0)
  success=int(0)
  
  dis = adaptative_tbl(4,"TEST RESULTS")
  dis.new_center_line()
  dis.set_column(1,'#')
  dis.set_column(2,'Name')
  dis.set_column(3,'Result')
  dis.set_column(4,'Duration')
  dis.end_separator()  

  time_start=time.time()
  
  total_tst=len(list)    
  for tst in list:

    tst_num=tst_num+1
    
    syslog.syslog("%10s ........ %s"%("START TEST",tst))
    
    sys.stdout.write( "\r___%4d/%d : %-40s \n"%(tst_num,total_tst,tst))

    dis.new_line()  
    dis.set_column(1,'%s'%(tst_num))
    dis.set_column(2,tst)

    
    # Split optional circumstance and test name
    split=tst.split('/') 
    
    time_before=time.time()
    reset_counters()   
    tst_file="tst_file" 
    
    if len(split) > 1:
    
      tst_file="%s.%s"%(split[1],split[0])
    
      # There is a test circumstance. resolve and call the circumstance  
      # function giving it the test name
      try:
        ret = getattr(sys.modules[__name__],split[0])(split[1])          
      except:
        ret = 2

    else:

      tst_file=split[0]


      
      # No test circumstance. Resolve and call the test function
      try:
        ret = getattr(sys.modules[__name__],split[0])()
      except:
        ret = 2
	
    delay=time.time()-time_before;	
    dis.set_column(4,'%s'%(my_duration(delay)))
    
    if ret == 0:
      syslog.syslog("%10s %8s %s"%("SUCCESS",my_duration(delay),tst))    
      dis.set_column(3,'OK')
      success=success+1
    elif ret == 2:
      syslog.syslog("%10s %8s %s"%("NOT FOUND",my_duration(delay),tst))        
      dis.set_column(3,'NOT FOUND')
      failed=failed+1    
    else:
      syslog.syslog("%10s %8s %s"%("FAILURE",my_duration(delay),tst))        
      dis.set_column(3,'FAILED')
      failed=failed+1
      
    if failed != 0 and stopOnFailure == True:
        break
         
    
  dis.end_separator()   
  dis.new_line()  
  dis.set_column(1,'%s'%(success+failed))
  dis.set_column(2,exepath)
  if failed == 0:
    dis.set_column(3,'OK')
  else:
    dis.set_column(3,'%d FAILED'%(failed))
    
  delay=time.time()-time_start    
  dis.set_column(4,'%s'%(my_duration(delay)))
  
  print ""
  dis.display()        
  
     
#___________________________________________________
def do_list():
# Display the list of all tests
#___________________________________________________

  num=int(0)
  dis = adaptative_tbl(4,"TEST LIST")
  dis.new_center_line()  
  dis.set_column(1,'Number')
  dis.set_column(2,'Test name')
  dis.set_column(3,'Test group')
  
  dis.end_separator()  
  for tst in TST_BASIC:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,tst)
    dis.set_column(3,'basic')  
    
  dis.end_separator()         
  for tst in TST_REBUILD:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,tst)
    dis.set_column(3,'rebuild') 
    
  dis.end_separator()         
  for tst in TST_RW:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,tst)
    dis.set_column(3,'rw') 
     
  dis.end_separator()         
  for tst in TST_RW:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,"%s/%s"%('storageFailed',tst))
    dis.set_column(3,'storageFailed') 

  dis.end_separator()         
  for tst in TST_RW:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,"%s/%s"%('storageReset',tst))
    dis.set_column(3,'storageReset') 

  dis.end_separator()         
  for tst in TST_RW:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,"%s/%s"%('ifUpDown',tst))
    dis.set_column(3,'ifUpDown') 
    
  dis.end_separator()         
  for tst in TST_RW:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,"%s/%s"%('storcliReset',tst))
    dis.set_column(3,'storcliReset')  

  dis.display()    
#____________________________________
def resolve_sid(cid,sid):

  string="%s/setup.py sid %s %s info"%(os.getcwd(),cid,sid)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for line in cmd.stdout:
    words = line.split()
    if len(words)<3: continue
    if words[0] == "site0": site0=words[2]
    if words[0] == "path0" : path0=words[2]
          
  try:int(site0)
  except:
    print "No such cid/sid %s/%s"%(cid,sid)
    return -1,"" 
  return site0,path0
       
#____________________________________
def resolve_mnt(inst):
  global site
  global eid
  global vid
  global mnt
  global exp
  global inverse
  global forward
  global safe
  global instance
  global nb_failures
  global sids
  global hosts
    
  vid="A"
  pid=None  
  instance = inst
        
  string="%s/setup.py mount %s info"%(os.getcwd(),instance)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for line in cmd.stdout:
    words = line.split()
    if len(words)<3: continue
    if words[0] == "site": site=words[2]
    if words[0] == "eid" : eid=words[2]
    if words[0] == "vid" : vid=words[2]
    if words[0] == "failures": nb_failures=int(words[2])
    if words[0] == "hosts": hosts=line.split("=")[1].split()
    if words[0] == "sids": sids=line.split("=")[1].split()
    if words[0] == "path": mnt=words[2]
    if words[0] == "pid": pid=words[2]
    if words[0] == "layout": 
      inverse=words[2]
      forward=words[3]
      safe=words[4]
          
  try:int(vid)
  except:
    print "No such RozoFS mount instance %s"%(instance)
    exit(1)    
    
  if pid == None:
    print "RozoFS instance %s is not running"%(instance)
    exit(1)      
#___________________________________________  
def cmd_returncode (string):
  global verbose
  if verbose: print string
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  cmd.wait()
  return cmd.returncode
#___________________________________________  
def cmd_system (string):
  global verbose
  if verbose: print string
  os.system(string)
        
#___________________________________________________
def usage():
#___________________________________________________

  print "\n./IT2/IT.py -l"
  print "  Display the whole list of tests."
  print "\n./IT2/IT.py [options] [extra] <test name/group> [<test name/group>...]"      
  print "  Runs a test list."
  print "    options:"
  print "      [--mount <mount1,mount2,..>]  A comma separated list of mount point instances. (default 1)"   
  print "      [--speed]          The run 4 times faster tests."
  print "      [--fast]           The run 2 times faster tests."
  print "      [--long]           The run 2 times longer tests."
  print "      [--repeat <nb>]    The number of times the test list must be repeated."   
  print "      [--cont]           To continue tests on failure." 
  print "      [--fusetrace]      To enable fuse trace on test. When set, --stop is automaticaly set."
  print "    extra:"
  print "      [--process <nb>]   The number of processes that will run the test in paralell. (default %d)"%(process)
  print "      [--count <nb>]     The number of loop that each process will do. (default %s)"%(loop) 
  print "      [--fileSize <nb>]  The size in MB of the file for the test. (default %d)"%(fileSize)   
  print "      [--rebuildCheck]   To check strictly after each rebuild that the files are secured."
  print "    Test group and names can be displayed thanks to ./IT2/IT.py -l"
  print "       - all              designate all the tests."
  print "       - rw               designate the read/write test list."
  print "       - storageFailed    designate the read/write test list run when a storage is failed."
  print "       - storageReset     designate the read/write test list run while a storage is reset."
  print "       - storcliReset     designate the read/write test list run while the storcli is reset."
  print "       - basic            designate the non read/write test list."
  print "       - rebuild          designate the rebuild test list."
  exit(0)



#___________________________________________________
# MAIN
#___________________________________________________                  
parser = OptionParser()
parser.add_option("-v","--verbose", action="store_true",dest="verbose", default=False, help="To set the verbose mode")
parser.add_option("-p","--process", action="store",type="string", dest="process", help="The number of processes that will run the test in paralell")
parser.add_option("-c","--count", action="store", type="string", dest="count", help="The number of loop that each process will do.")
parser.add_option("-f","--fileSize", action="store", type="string", dest="fileSize", help="The size in MB of the file for the test.")
parser.add_option("-l","--list",action="store_true",dest="list", default=False, help="To display the list of test")
parser.add_option("-k","--snipper",action="store",type="string",dest="snipper", help="To start a storage/storcli snipper.")
parser.add_option("-s","--cont", action="store_true",dest="cont", default=False, help="To continue on failure.")
parser.add_option("-t","--fusetrace", action="store_true",dest="fusetrace", default=False, help="To enable fuse trace on test.")
parser.add_option("-F","--fast", action="store_true",dest="fast", default=False, help="To run 2 times faster tests.")
parser.add_option("-S","--speed", action="store_true",dest="speed", default=False, help="To run 4 times faster tests.")
parser.add_option("-L","--long", action="store_true",dest="long", default=False, help="To run 2 times longer tests.")
parser.add_option("-r","--repeat", action="store", type="string", dest="repeat", help="A repetition count.")
parser.add_option("-m","--mount", action="store", type="string", dest="mount", help="A comma separated list of mount points to test on.")
parser.add_option("-R","--rebuildCheck", action="store_true", dest="rebuildCheck", default=False, help="To request for stron rebuild check after each rebuild.")
parser.add_option("-e","--exepath", action="store", type="string", dest="exepath", help="re-exported path to run the test on.")
parser.add_option("-n","--nfs", action="store_true",dest="nfs", default=False, help="Running through NFS.")

# Read/write test list
TST_RW=['read_parallel','write_parallel','rw2','wr_rd_total','wr_rd_partial','wr_rd_random','wr_rd_total_close','wr_rd_partial_close','wr_rd_random_close','wr_close_rd_total','wr_close_rd_partial','wr_close_rd_random','wr_close_rd_total_close','wr_close_rd_partial_close','wr_close_rd_random_close']
# Basic test list
TST_BASIC=['readdir','xattr','link','symlink', 'rename','chmod','truncate','bigFName','lock_posix_passing','lock_posix_blocking','lock_race','crc32','rsync','compil']
TST_BASIC_NFS=['readdir','link', 'rename','chmod','truncate','bigFName','lock_posix_passing','lock_posix_blocking','lock_race','crc32','rsync','compil']
# Rebuild test list
TST_REBUILD=['gruyere','rebuild_fid','rebuild_1dev','relocate_1dev','rebuild_all_dev','rebuild_1node','gruyere_reread','gruyere','rebuild_1node_parts','gruyere_reread']

ifnumber=get_if_nb()

list_cid=[]
list_sid=[]
list_host=[]

syslog.openlog("RozoTests",syslog.LOG_INFO)

(options, args) = parser.parse_args()
 
if options.nfs == True:
  TST_BASIC=TST_BASIC_NFS
 
if options.rebuildCheck == True:  
  rebuildCheck=True 
else:
  rebuildCheck=False
     
if options.process != None:
  process=int(options.process)
  
if options.count != None:
  loop=int(options.count)
  
if options.fileSize != None:
  fileSize=int(options.fileSize)

if options.verbose == True:
  verbose=True

if options.list == True:
  do_list()
  exit(0)
    
if options.cont == True:  
  stopOnFailure=False 

if options.fusetrace == True:  
  stopOnFailure=True 
  fuseTrace=True

if options.speed == True:  
  loop=loop/4
  nbGruyere=nbGruyere/4
     
elif options.fast == True:  
  loop=loop/2
  nbGruyere=nbGruyere/2
   
elif options.long == True:  
  loop=loop*2 
  nbGruyere=nbGruyere*2

if options.mount == None: mnt="0"
else:                     mnt=options.mount

resolve_mnt(int(mnt))
EXPORT_SID_NB,STORCLI_SID_NB=get_sid_nb()
  
if options.exepath == None: exepath = mnt
else:                       exepath = options.exepath

#print "mnt %s exepath %s"%(mnt,exepath)

# Snipper   
if options.snipper != None:
  snipper(options.snipper)
  exit(0)  
  
#TST_REBUILD=TST_REBUILD+['rebuild_delete']

# Build list of test 
list=[] 
for arg in args:  
  if arg == "all":
    list.extend(TST_BASIC)
    list.extend(TST_REBUILD)
    list.extend(TST_RW)
    append_circumstance_test_list(list,TST_RW,'storageFailed')
    append_circumstance_test_list(list,TST_RW,'storageReset') 
    if int(ifnumber) > int(1):
      append_circumstance_test_list(list,TST_RW,'ifUpDown')
       
#re    append_circumstance_test_list(list,TST_RW,'storcliReset')   
  elif arg == "rw":
    list.extend(TST_RW)
  elif arg == "storageFailed":
    append_circumstance_test_list(list,TST_RW,arg)
  elif arg == "storageReset":
    append_circumstance_test_list(list,TST_RW,arg)
  elif arg == "storcliReset":
    append_circumstance_test_list(list,TST_RW,arg)
  elif arg == "ifUpDown":
    append_circumstance_test_list(list,TST_RW,arg)   
  elif arg == "basic":
    list.extend(TST_BASIC)
  elif arg == "rebuild":
    list.extend(TST_REBUILD)  
  else:
    list.append(arg)              
# No list of test. Print usage
if len(list) == 0:
  usage()
  
new_list=[]    
if options.repeat != None:
  repeat = int(options.repeat)
  while repeat != int(0):
    new_list.extend(list)
    repeat=repeat-1
else:
  new_list.extend(list)  

do_compile_programs() 



do_run_list(new_list)
