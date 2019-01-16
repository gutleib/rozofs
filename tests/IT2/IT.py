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
stopOnFailure=False
fuseTrace=False
DEFAULT_RETRIES=int(40)
tst_file="tst_file"
device_number=""
mapper_modulo=""
mapper_redundancy=""

ifnumber=int(0)
instance=None
site=None
eid=None
vid=None
vid_fast=None
mnt=None
exepath=None
inverse=None
forward=None
safe=None
nb_failures=None
sids=[]
hosts=[]
verbose=False
corrupt_offsets = [ 1211, 3111, 4444, 1024*112, 1024*114, 1024*115, 1024*118, 1024*120 ];

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

  # Add carriage return to the message
  string=string+"\n"
  log(string)
   
  # Reset current line
  clearline()  
  string="\r"+string
    
  # write the message in the buffer
  sys.stdout.write(string)
  # force ouput
  sys.stdout.flush()
#___________________________________________________
# output a message on the console as well as in syslog
def report(string): 
#___________________________________________________
  console(string)
#___________________________________________________
# Add temporary text to the current line 
def addline(string):
#___________________________________________________
  global resetLine
  global verbose
  
  if options.debug == True:    
    # write the message in the buffer
    sys.stdout.write(string)
    # force ouput
    sys.stdout.flush()
    return
    
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
  log(string)
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
  
  string="rozodiag -i localhost%s -T storaged -c storio"%(hid)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for line in cmd.stdout:
    if "mode" in line:
      if "multiple" in line:
        storio_name="storio:%s"%(cid)
      break; 
     
  string="rozodiag -i localhost%s -T %s -c device 1> /dev/null"%(hid,storio_name)
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
  global vid
  global vid_fast
  
  string="rozodiag -T mount:%s:1 -c storaged_status"%(instance)       
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  storcli_sid=int(0)
  for line in cmd.stdout:
    if "UP" in line or "DOWN" in line:
      storcli_sid=storcli_sid+1
          
  string="rozodiag -T export -c vfstat_stor"
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  export_sid=int(0)
  for line in cmd.stdout:
    if len(line.split()) == 0: continue
    if line.split()[0] != vid: 
      if vid_fast == None: continue
      if line.split()[0] != vid_fast: continue;
    if "UP" in line or "DOWN" in line:export_sid=export_sid+1

  return export_sid,storcli_sid    
#___________________________________________________
def reset_storcli_counter():
# Use debug interface to get the number of sid from exportd
#___________________________________________________

  string="rozodiag -T mount:%s:1 -T mount:%s:2 -T mount:%s:3 -T mount:%s:4 -c counter reset"%(instance,instance,instance,instance)       
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  
  time.sleep(1)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  

#___________________________________________________
def check_storcli_crc(expect):
# Use debug interface to get the number of sid from exportd
#___________________________________________________

  string="rozodiag -T mount:%s:1-4 -c profiler"%(instance)       
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  result = False    
  for line in cmd.stdout:
    if "read_blk_crc" in line:
      result = True
      break; 
  if expect != result:
    os.system("%s"%(string))
  return result  
   
#___________________________________________________
def export_count_sid_up ():
# Use debug interface to count the number of sid up 
# seen from the export. 
#___________________________________________________
  global vid
  global vid_fast
  
  string="rozodiag -T export:1 -t 12 -c vfstat_stor"
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  match=int(0)
  for line in cmd.stdout:
    if len(line.split()) == 0:
      continue
    if line.split()[0] != vid:
      if vid_fast == None: continue
      if line.split()[0] != vid_fast:  continue
    if "UP" in line:
      match=match+1

  return match
#___________________________________________________
def export_all_sid_available (total):
# Use debug interface to check all SID are seen UP
#___________________________________________________
  global vid
  global vid_fast
  
  string="rozodiag -T export:1 -t 12 -c vfstat_stor"
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  match=int(0)
  for line in cmd.stdout:
    if len(line.split()) == 0:
      continue
    if line.split()[0] != vid:
      if vid_fast == None: continue
      if line.split()[0] != vid_fast:  continue
    if "UP" in line:
      match=match+1
      
  log("export:1 %s sid up"%(match))    
  if match != total: return False
  return True
#___________________________________________________
def wait_until_export_all_sid_available (total,retries):
#___________________________________________________

  addline("E")
  count = int(retries)
  
  while True:

    addline(".")
     
    if export_all_sid_available(total) == True: return True    

    count = count-1      
    if count == 0: break;
    time.sleep(1)    
    
  report("wait_until_export_all_sid_available : Maximum retries reached %s"%(retries))
  return False  
#___________________________________________________
def storcli_all_sid_available (total):
# Use debug interface to check all SID are seen UP
#___________________________________________________
  
  string="rozodiag -T mount:%s -c stc"%(instance)       
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  
  nbstorcli = 0
  for line in cmd.stdout:
    words=line.split(':')
    if words[0] == "number of configured storcli":
      nbstorcli = int(words[1])
      break;
  
  nbstorcli = nbstorcli + 1
  for storcli in range(1,nbstorcli):
  
    string="rozodiag -T mount:%s:%d -c storaged_status"%(instance,storcli)       
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Looking for state=UP and selectable=YES
    match=int(0)
    for line in cmd.stdout:
      words=line.split('|')
      if len(words) >= 11:
        if 'YES' in words[6] and 'UP' in words[4]: match=match+1         
    log("mount:%s:%s : %s sid up"%(instance, storcli, match))       
    if match != total: return False
  time.sleep(1)  
  log("mount:%s all sid up"%(instance))        
  return True
#___________________________________________________
def wait_until_storcli_all_sid_available (total,retries):
#___________________________________________________
  
  addline("S")
  count = int(retries)

  while True:

    addline(".")
     
    if storcli_all_sid_available(total) == True: return True    

    count = count-1      
    if count == 0: break;
    time.sleep(1)    
    
  report("wait_until_storcli_all_sid_available : Maximum retries reached %s"%(retries))
  return False

#___________________________________________________
def storcli_count_sid_available ():
# Use debug interface to count the number of sid 
# available seen from the storcli. 
#___________________________________________________

  string="rozodiag -T mount:%s:1 -c storaged_status"%(instance)       
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
      report("Maximum retries reached. %s is %s\n"%(function,up))     
      return False
      
    addline(".")
     
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
      report( "Maximum retries reached. %s is %s\n"%(function,up))      
      return False
      
    addline(".")
     
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
  time.sleep(3)
  wait_until_storcli_all_sid_available(STORCLI_SID_NB,retries)
  wait_until_export_all_sid_available(EXPORT_SID_NB,retries)
  time.sleep(2)  
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

  backline("Storage start ")

  for idx in range(int(count)): 
    addline("%s "%(int(hid)+idx)) 
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

  backline("Storage stop ")

  for idx in range(int(count)): 
    addline("%s "%(int(hid)+idx)) 
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
      report("Error on %s"%(test))
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

      backline("Storcli reset")

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
        addline(".")
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
	  
	backline("host %s if %s down "%(hid,itf))
	
	os.system("./setup.py storage %s ifdown %s"%(hid,itf))
	time.sleep(1)
	          
	backline("host %s if %s up   "%(hid,itf))
	
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
      
      time.sleep(1)
          
      backline("Storage reset ")
      cmd=""
      for idx in range(int(nb_failures)):
        val=int(hid)+int(idx)
        addline("%s "%(val))
	cmd+="./setup.py storage %s reset;"%(val)
	
      os.system(cmd)
      time.sleep(1)


  
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
    report("Failed snipper %s"%(func))
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
def reread():
#___________________________________________________
  SIZE="1111111"
  NBFILE="128"

  backline("START ALL UP: Write files")  
  ret = os.system("./IT2/test_file.exe -fullpath %s/reread -nbfiles %s -size %s -action create"%(exepath,NBFILE,SIZE))
  if ret != 0:
    report("START ALL UP: Error on 1rst file creation")    
    return 1
    
  backline("START ALL UP: Reread files")
  
  ret = os.system("./IT2/test_file.exe -fullpath %s/reread -nbfiles %s -size %s -action check"%(exepath,NBFILE,SIZE))
  if ret != 0:
    report("START ALL UP: 1rst reread error")
    return 1

  # Loop on hosts
  for hid in hosts:  
         	    
    # Reset a bunch of storages	    
    storageStopAndWait(hid,1)
    
    ret = os.system("./IT2/test_file.exe -fullpath %s/reread -nbfiles %s -size %s -action check"%(exepath,NBFILE,SIZE))
    if ret != 0:
      report("START ALL UP: reread error with storage %s failed"%(hid))
      return 1

    backline("START ALL UP: Unmount with storage %s failed"%(hid))
    os.system("./setup.py mount %s stop"%(instance))
    time.sleep(2)
    
    backline("START ALL UP: Mount with storage %s failed"%(hid))
    os.system("./setup.py mount %s start"%(instance))
    time.sleep(3)
    
    ret = os.system("./IT2/test_file.exe -fullpath %s/reread -nbfiles %s -size %s -action check"%(exepath,NBFILE,SIZE))
    if ret != 0:
      report("START ALL UP: reread error with storage %s failed after remount"%hid)
      return 1    
          
    # Restart every storages  
    storageStartAndWait(hid,1)  


  # Loop on hosts
  for hid in hosts:  
         	    
    # Reset a bunch of storages	    
    storageStopAndWait(hid,1)
  
    backline("STORAGE %s FAILED: Re-write files"%(hid))
    ret = os.system("./IT2/test_file.exe -fullpath %s/reread -nbfiles %s -size %s -action create"%(exepath,NBFILE,SIZE))
    if ret != 0:
      report("STORAGE %s FAILED: write error"%(hid))    
      return 1
    
    backline("STORAGE %s FAILED: Reread files"%(hid))
  
    ret = os.system("./IT2/test_file.exe -fullpath %s/reread -nbfiles %s -size %s -action check"%(exepath,NBFILE,SIZE))
    if ret != 0:
      report("STORAGE %s FAILED: reread files"%(hid))
      return 1
 
    backline("STORAGE %s FAILED: Unmount"%(hid))
    os.system("./setup.py mount %s stop"%(instance))
    time.sleep(2)
    
    backline("STORAGE %s FAILED: Mount"%(hid))
    os.system("./setup.py mount %s start"%(instance))
    time.sleep(3)
    
    ret = os.system("./IT2/test_file.exe -fullpath %s/reread -nbfiles %s -size %s -action check"%(exepath,NBFILE,SIZE))
    if ret != 0:
      report("STORAGE %s FAILED: reread error after remount"%(hid))
      return 1      
          
    # Restart every storages  
    storageStartAndWait(hid,1)  

    ret = os.system("./IT2/test_file.exe -fullpath %s/reread -nbfiles %s -size %s -action check"%(exepath,NBFILE,SIZE))
    if ret != 0:
      report("STORAGE %s FAILED: reread error after remount and restart"%(hid))
      return 1  
      
  os.system("./IT2/test_file.exe -fullpath %s/reread -nbfiles %s -size %s -action delete"%(exepath,NBFILE,SIZE))        
  return 0
  
#___________________________________________________
def get_1rst_header_and_bins(fname):
#___________________________________________________

  # Get fie loalization
  os.system("./setup.py cou %s > /tmp/get_header_and_bins"%(fname))
     
  # Find the 1rst mapper file
  mapper = None
  bins   = None
  cid    = int(0)
  sid    = ""
  
  with open("/tmp/get_header_and_bins","r") as f: 
    for line in f.readlines():
                 
      if "/hdr_0/" in line:
        mapper = line.split()[3]
        if bins != None: break    
        continue
        
      if "/bins_0/" in line:
        bins = line.split()[3]
        if mapper != None: break    
        continue  

  if mapper != None:       
    cid = mapper.split('/')[4].split('_')[1]
    sid = mapper.split('/')[4].split('_')[2]
                  
  os.system("rm -f /tmp/get_header_and_bins")
  return cid,sid,mapper,bins

#___________________________________________________
def get_2nd_subfile_header_and_bins(fname):
#___________________________________________________

  # Get fie loalization
  os.system("./setup.py cou %s > /tmp/get_header_and_bins"%(fname))
     
  # Find the 1rst mapper file
  mapper = None
  bins   = None
  cid    = int(0)
  sid    = ""
  
  markfound = False
  mark2find = ""
  with open("/tmp/get_header_and_bins","r") as f: 
    for line in f.readlines():

      if "HYBRID" in line and "Yes" in line: mark2find = "#1"        
      if "HYBRID" in line and "No" in line:  mark2find = "#2"
      if mark2find == "": continue
                  
      if "S_INODE" in line and mark2find in line: markfound = True
      if markfound == False: continue
      
      if "/hdr_0/" in line:
        mapper = line.split()[3]
        if bins != None: break    
        continue
        
      if "/bins_0/" in line:
        bins = line.split()[3]
        if mapper != None: break    
        continue  

  if mapper != None:       
    cid = mapper.split('/')[4].split('_')[1]
    sid = mapper.split('/')[4].split('_')[2]
                  
  os.system("rm -f /tmp/get_header_and_bins")
  return cid,sid,mapper,bins
#___________________________________________________
def run_mapper_corruption(cid,sid,mapper,crcfile):
#___________________________________________________

  backline("Truncate mapper/header file %s "%(mapper))

  # Truncate mapper file  
  with open(mapper,"w") as f: f.truncate(0)
  # Check file has been truncated
  statinfo = os.stat(mapper)
  if statinfo.st_size != 0:
    report("%s has not been truncated"%(mapper))
    return -1

  # Reset storages
  os.system("./setup.py storage all reset; echo 3 > /proc/sys/vm/drop_caches")
  wait_until_all_sid_up()

  # Reread the file
  os.system("dd of=/dev/null if=%s bs=1M > /dev/null 2>&1"%(crcfile))  

  # Check mapper file has been repaired
  statinfo = os.stat(mapper)
  if statinfo.st_size == 0:
    report("%s has not been repaired"%(mapper))
    return -1             
  backline("mapper/header has been repaired ")

  # Corrupt mapper file 
  f = open(mapper, "w+")     
  f.truncate(0)        
  size = statinfo.st_size     
  while size != 0:
    f.write('a')
    size=size-1
  f.close()

  backline("Corrupt mapper/header file %s "%(mapper))

  # Reset storage
  os.system("./setup.py storage all reset; echo 3 > /proc/sys/vm/drop_caches")
  wait_until_all_sid_up()

  # Reread the file
  os.system("dd of=/dev/null if=%s bs=1M > /dev/null 2>&1"%(crcfile))  
  #if filecmp.cmp(crcfile,"./ref") == False: report("%s and %s differ"%(crcfile,"./ref"))

  # Check file has been re written
  f = open(mapper, "rb")      
  char = f.read(1)     
  if char == 'a':
    report("%s has not been rewritten"%(mapper))
    return -1      
  backline("mapper/header has been repaired ")
  return 0


#___________________________________________________
def check_corrupt(fname):
  global corrupt_offsets
  repaired = int(0)
  corrupted = int(0)
  result=""

  f = open(fname, 'r')       
  for offset in corrupt_offsets:
    f.seek(offset) 
    data = f.read(3) 
    if data == "DDT":
      corrupted = int(corrupted) + int(1)
      result = result + " %s"%(offset)
    else:
      repaired = int(repaired) + int(1)
  f.close()      
  log("REPAIRED=%s / CORRUPTED=%s%s"%(repaired,corrupted,result))  

  return int(corrupted)
      
#___________________________________________________
def do_corrupt(fname):
  global corrupt_offsets

  f = open(fname, 'r+b')       
  for offset in corrupt_offsets:
    f.seek(offset) 
    f.write("DDT")
  f.close()      
 
  
#___________________________________________________
def run_bins_corruption(cid,sid,bins,crcfile):
#___________________________________________________
  backline("Corrupt bins file %s "%(bins))

  # Corrupt the bins file, 
  do_corrupt(bins)
  if check_corrupt(bins) == int(0) :
    report("Can not corrupt %s"%(bins))
    return 1     

  # Clear error counter
  reset_storcli_counter()

  # Reset storages
  os.system("./setup.py storage all reset; echo 3 > /proc/sys/vm/drop_caches")
  wait_until_all_sid_up() 

  # Reread the file
  os.system("dd of=/dev/null if=%s bs=1M > /dev/null 2>&1"%(crcfile))  
    
  # Reread the file
  if filecmp.cmp(crcfile,"./ref",shallow=False) == False: 
    report("1 !!! %s and %s differ"%(crcfile,"./ref"))
    return 1 
  if filecmp.cmp(crcfile,"./ref",shallow=False) == False: 
    report("2 !!! %s and %s differ"%(crcfile,"./ref"))
    return 1
  time.sleep(1)     
  if filecmp.cmp(crcfile,"./ref",shallow=False) == False: 
    report("3 !!! %s and %s differ"%(crcfile,"./ref"))
    return 1   
  if filecmp.cmp(crcfile,"./ref",shallow=False) == False: 
    report("4 !!! %s and %s differ"%(crcfile,"./ref"))
    return 1 
  
  if check_storcli_crc(True) == True:
    backline("Repair procedure has been run ")
  else:     
    report("No CRC errors after file reread")
    return 1     
    
  if check_corrupt(bins) != int(0) :
    report("Bins file %s is still corrupted"%(bins))
    return 1     
      
  backline("Bins file %s is repaired"%(bins))
  
  return 0   
  
#___________________________________________________
def crc32():
#___________________________________________________

  wait_until_all_sid_up()

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
  cid,sid,mapper,bins = get_1rst_header_and_bins(crcfile)
             
  if mapper == None or bins == None or int(cid) == int(0) or sid == "" :
    report("Fail to find mapper/bins file name in /tmp/crc32loc")
    return -1
    
  hid = get_hid(cid,sid)
  device_number,mapper_modulo,mapper_redundancy = get_device_numbers(hid,cid)   
  
  if int(mapper_redundancy) > 1: 
    ret = run_mapper_corruption(cid,sid,mapper,crcfile)
    if ret != 0: return 1

  ret = run_bins_corruption (cid,sid,bins,crcfile)   
  if ret != 0: return 1

    
  # Get its localization  
  cid,sid,mapper,bins = get_2nd_subfile_header_and_bins(crcfile)
  if mapper == None or bins == None or int(cid) == int(0) or sid == "" :
    return 0
    
  hid = get_hid(cid,sid)
  device_number,mapper_modulo,mapper_redundancy = get_device_numbers(hid,cid)   
  
  if int(mapper_redundancy) > 1: 
    ret = run_mapper_corruption(cid,sid,mapper,crcfile)
    if ret != 0: return 1

  ret = run_bins_corruption (cid,sid,bins,crcfile)   
  if ret != 0: return 1

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
      report("%s\nbad content %s\n"%(FNAME,data))
      return -1
  return 0
	  
#___________________________________________________
def lock_race():
#___________________________________________________ 
  dir="%s/lock"%(exepath)
  os.system("mkdir -p %s"%(dir))
  zefile='%s/%s'%(dir,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  
  return os.system("./IT2/test_lock_race.exe -process %d -loop %d -file %s "%(process,loop,zefile))  
#___________________________________________________
def flockp_race():
#___________________________________________________ 
  dir="%s/flockp"%(exepath)
  os.system("mkdir -p %s"%(dir))
  zefile='%s/%s'%(dir,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  
  return os.system("./IT2/test_lock_race.exe -flockp -process %d -loop %d -file %s "%(process,loop,zefile))  
#___________________________________________________
def lock_posix_passing():
#___________________________________________________ 
  dir="%s/lock"%(exepath)
  os.system("mkdir -p %s"%(dir))
  zefile='%s/%s'%(dir,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  
  return os.system("./IT2/test_file_lock.exe -process %d -loop %d -file %s -nonBlocking"%(process,loop,zefile))  
#___________________________________________________
def flockp_posix_passing():
#___________________________________________________ 
  dir="%s/flockp"%(exepath)
  os.system("mkdir -p %s"%(dir))
  zefile='%s/%s'%(dir,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  
  return os.system("./IT2/test_file_lock.exe -flockp -process %d -loop %d -file %s -nonBlocking"%(process,loop,zefile))  

#___________________________________________________
def lock_posix_blocking():
#___________________________________________________
  dir="%s/lock"%(exepath)
  os.system("mkdir -p %s"%(dir))
  zefile='%s/%s'%(dir,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  

  ret=os.system("./IT2/test_file_lock.exe -process %d -loop %d -file %s"%(process,loop,zefile))
  return ret 
#___________________________________________________
def flockp_posix_blocking():
#___________________________________________________
  dir="%s/flockp"%(exepath)
  os.system("mkdir -p %s"%(dir))
  zefile='%s/%s'%(dir,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  

  ret=os.system("./IT2/test_file_lock.exe -flockp -process %d -loop %d -file %s"%(process,loop,zefile))
  return ret 

#___________________________________________________
def flockp_bsd_passing():
#___________________________________________________  
  dir="%s/flockp"%(exepath)
  os.system("mkdir -p %s"%(dir))
  zefile='%s/%s'%(dir,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  
  return os.system("./IT2/test_file_lock.exe -flockp -process %d -loop %d -file %s -nonBlocking -bsd"%(process,loop,zefile))

#___________________________________________________
def lock_bsd_passing():
#___________________________________________________  
  dir="%s/lock"%(exepath)
  os.system("mkdir -p %s"%(dir))
  zefile='%s/%s'%(dir,tst_file)
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
  dir="%s/lock"%(exepath)
  os.system("mkdir -p %s"%(dir))
  zefile='%s/%s'%(dir,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  
  return os.system("./IT2/test_file_lock.exe -process %d -loop %d -file %s -bsd "%(process,loop,zefile))  
#___________________________________________________
def flockp_bsd_blocking():
#___________________________________________________
  dir="%s/flockp"%(exepath)
  os.system("mkdir -p %s"%(dir))
  zefile='%s/%s'%(dir,tst_file)
  try:
    os.remove(zefile)
  except:
    pass  
  return os.system("./IT2/test_file_lock.exe -flockp -process %d -loop %d -file %s -bsd "%(process,loop,zefile))  
#___________________________________________________
def check_one_criteria(attr,f1,f2):
#___________________________________________________
  one=getattr(os.stat(f1),attr)
  two=getattr(os.stat(f2),attr)
  try: 
    one=int(one)
    two=int(two)
  except: pass  
  if one != two:
    report("%s %s for %s"%(one,attr,f1))
    report("%s %s for %s"%(two,attr,f2))
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
      report( "source directory %s does not exist"%(d1))
      return False

    if os.path.exists(d2) == False:
      report("destination directory %s does not exist"%(d2))
      return False

    for criteria in criterias:
      if check_one_criteria(criteria,d1,d2) == False:
	return False
    
    for fileName in filenames:
    	  
      f1 = os.path.join(dirpath, fileName)
      f2 = "%s/rsync_dest/%s/%s"%(exepath,dirpath[len(src):], fileName) 

      if os.path.exists(f1) == False:
        report("source file %s does not exist"%(f1))
	return False
	
      if os.path.exists(f2) == False:
        report("destination file %s does not exist"%(f2))
	return False
 
      for criteria in criterias:
        if check_one_criteria(criteria,f1,f2) == False:
	  return False

      if filecmp.cmp(f1,f2) == False:
	report("%s and %s differ"%(f1,f2))
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
    report("%s bytes transfered while expecting %d!!!"%(bytes,count))
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
     
  backline("1rst rsync")
  if internal_rsync(src,size,True) == False: return 1 
   
  time.sleep(2)
  backline("2nd rsync")  
  if internal_rsync(src,int(0)) == False: return 1

  size = touch_one_file("%s/a"%(src))
  time.sleep(2)
  backline("rsync after 1 touch")  
  if internal_rsync(src,size) == False: return 1
  
  size = touch_one_file("%s/HB"%(src))
  size = size + touch_one_file("%s/subdir1/a"%(src))
  time.sleep(2)
  backline("rsync after 2 touch")  
  if internal_rsync(src,size) == False: return 1 
   
  size = touch_one_file("%s/c"%(src))
  size = size + touch_one_file("%s/subdir1/subdir2/subdir3/ha1"%(src))
  size = size + touch_one_file("%s/subdir1/subdir2/B"%(src))
  time.sleep(2)
  backline("rsync after 3 touch")  
  if internal_rsync(src,size) == False: return 1  
  
  time.sleep(2)
  backline("rsync again")  
  if internal_rsync(src,int(0)) == False: return 1  
   
  return 0
#___________________________________________________
def is_elf(name):
  string="file %s/git/build/%s"%(exepath,name)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for line in cmd.stdout:
    if "ELF" in line: return True
  report("%s not generated as ELF"%(string))
  return False
#___________________________________________________     

def compil_openmpi(): 
#___________________________________________________
  os.system("rm -rf %s/tst_openmpi; cp -f ./IT2/tst_openmpi.tgz %s; cd %s; tar zxf tst_openmpi.tgz  > %s/compil_openmpi 2>&1; rm -f tst_openmpi.tgz; cd tst_openmpi; ./compil_openmpi.sh  >> %s/compil_openmpi 2>&1;"%(exepath,exepath,exepath,exepath,exepath))
  
  string="cat %s/tst_openmpi/hello.res"%(exepath)
  parsed = shlex.split(string)  
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
     
  found = [ False,  False,  False,  False,  False,  False ]
  
  for line in cmd.stdout:
    if line.split()[0] != "hello": continue
    if line.split()[1] != "6": continue
    found[int(line.split()[2])] = True     
  
  for val in found: 
   if val == False:
     report("Mising lines in hello.res"%(i))
     os.system("cat %s/tst_openmpi/hello.res"%(exepath))
     return 1
  return 0   
  
   
#___________________________________________________
# Get rozofs from github, compile it and test rozodiag
#___________________________________________________     
def compil_rozofs():  
  os.system("cd %s; rm -rf git; git clone https://github.com/rozofs/rozofs.git git  > %s/compil_rozofs 2>&1; cd %s/git; mkdir build; cd build; cmake -G \"Unix Makefiles\" ../ 1>> %s/compil_rozofs; make -j16  >> %s/compil_rozofs 2>&1"%(exepath,exepath,exepath,exepath,exepath))
  if is_elf("src/rozodiag/rozodiag") == False: return 1
  if is_elf("src/exportd/exportd") == False: return 1
  if is_elf("src/rozofsmount/rozofsmount") == False: return 1
  if is_elf("src/storcli/storcli") == False: return 1
  if is_elf("src/storaged/storaged") == False: return 1
  if is_elf("src/storaged/storio") == False: return 1
  
  # Check wether automount is configured
  string="%s/git/build/src/rozodiag/rozodiag -T mount:%s -c up "%(exepath,instance)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for line in cmd.stdout:
    # No automount 
    if "uptime" in line: return 0
  report("Bad response to %s"%(string))
  return 1
  
#___________________________________________________  
def read_size(filename):
#___________________________________________________  
  string="attr -g rozofs %s"%(filename)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  # loop on the bins file constituting this file, and ask
  for line in cmd.stdout:  
    words=line.split();
    if len(words) >= 2:
      if words[0] == "SIZE":
          #print line
          try:  
	    return int(words[2])
	  except:
	    return int(-1);  
	  
  return int(-1) 
#___________________________________________________  
def mmap(): 

  os.system("mkdir -p %s/mmap"%(exepath));

  #
  # Write files
  #
  backline("Write files") 
  size = int(0)
  for i in range(64):
    size += int(111)
    filename="%s/mmap/file.%s"%(exepath,i)
    string="%s/IT2/test_mmap.exe %s write %s"%(os.getcwd(),filename,size)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    cmd.wait()
    if cmd.returncode != 0:
      report("Error writing %s %s"%(filename,cmd.returncode))
      return 1

  #
  # Read whole files
  #
  backline("Read whole files") 
  size = int(0)      
  for i in range(64):
    size += int(111)
    filename="%s/mmap/file.%s"%(exepath,i)
    string="%s/IT2/test_mmap.exe %s read %s"%(os.getcwd(),filename,size)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    cmd.wait()
    if cmd.returncode != 0:
      report("Error reading %s %s"%(filename,cmd.returncode))
      return 1

  #
  # Read less
  #
  backline("Read part of files")   
  size = int(0)      
  for i in range(64):
    size += int(32)
    filename="%s/mmap/file.%s"%(exepath,i)
    string="%s/IT2/test_mmap.exe %s read %s"%(os.getcwd(),filename,size)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    cmd.wait()
    if cmd.returncode != 0:
      report("Error reading %s %s"%(filename,cmd.returncode))
      return 1

  #
  # Read too much
  #            
  backline("Read too much")   
  size = int(0)      
  for i in range(64):
    size += int(236)
    filename="%s/mmap/file.%s"%(exepath,i)    
    string="%s/IT2/test_mmap.exe %s read %s"%(os.getcwd(),filename,size)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    cmd.wait()
    if cmd.returncode == 0:
      report("No error re-reading %s %s"%(filename,cmd.returncode))    
      return 1
      
  return 0
          
#__________________________________________________
  
#___________________________________________________  
def resize(): 
#___________________________________________________
  report(red + bold + "   !!! resize service is not yet supported !!!" + endeffect )
  return 0
  realSizeMB = 15
  
  # Create a 1M file
  os.system("dd if=/dev/zero of=%s/resize bs=1M count=%s > /dev/null 2>&1"%(exepath,realSizeMB))  
  size = read_size("%s/resize"%(exepath))
  if size != int(1024*1024*realSizeMB):
    report("%s/resize size is %s instead of %d after dd "%(exepath,size,int(1024*1024*realSizeMB)))
    return 1
    
    
  for loop in range(0,100):

    sz = loop*10
    
    # Patch size to 10bytes    
    os.system("attr -s rozofs -V \" size = %d\" %s/resize 1> /dev/null"%(sz,exepath))
    size = read_size("%s/resize"%(exepath))
    if size != sz:
      report("%s/resize size is %s instead of %s after attr -s "%(exepath,size,sz))
      return 1

    # Request for resizing  
    os.system("%s/IT2/test_resize.exe %s/resize"%(os.getcwd(),exepath))
    
    size = read_size("%s/resize"%(exepath))
    if size != int(1024*1024*realSizeMB):
      report( "%s/resize size is %s instead of %d after resize "%(exepath,size,int(1024*1024*realSizeMB)))
      return 1

  return 0  
#___________________________________________________
# Kill a process
#___________________________________________________   
def crash_process(process,main):

  string="rozodiag %s -c ps"%(process)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  pid="?"
  for line in cmd.stdout:
    if main in line:
      pid=line.split()[1]
      break
  try:
    int(pid)
  except:
    report("Can not find PID of \"%s\""%(process))
    return False  

  os.system("kill -6 %s"%(pid))
  return True
#___________________________________________________
# Check a core file exist for a process
#___________________________________________________   
def check_core_process(process,cores):

  time.sleep(8)
  
  string="rozodiag %s -c core"%(process)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  nb=0
  for line in cmd.stdout:
    if "/var/run/rozofs/core" in line: nb=nb+1
  if nb != cores:
    report("%s core file generated for %s"%(nb,process))
    return False
    
  return True 
   
#___________________________________________________
# Test that core file are generated on signals
#___________________________________________________     
def cores():  
  os.system("./setup.py core remove all")  

  # Storaged
  backline("Crash storaged of localhost1 and check core file")
  process="-i localhost1 -T storaged"
  if crash_process(process,"Main") != True: return 1
  if check_core_process(process,1) != True: return 1
  os.system("./setup.py core remove all")  

  # Storio
  string="%s/setup.py mount %s info"%(os.getcwd(),instance)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for line in cmd.stdout:
    words = line.split()
    if len(words)<3: continue
    if words[0] == "sids": 
      node=words[2].split('-')[0]
      cid=words[2].split('-')[1]
      sid=words[2].split('-')[2]
      backline("Crash storio:%s of localhost%s and check core file"%(cid,node))
      process="-i localhost%s -T storio:%s"%(node,cid)
      if crash_process(process,"Main") != True: return 1
      if check_core_process(process,1) != True: return 1
  os.system("./setup.py core remove all")  

  # Stspare
  backline("Crash stspare of localhost2 and check core file")  
  process="-i localhost2 -T stspare"
  if crash_process(process,"Main") != True: return 1
  if check_core_process(process,1) != True: return 1
  os.system("./setup.py core remove all")  
  
  # export slave
  backline("Crash exportd slave 1 and check core file")  
  process="-T export:1"
  if crash_process(process,"Blocking") != True: return 1
  if check_core_process(process,1) != True: return 1
  os.system("./setup.py core remove all")  

  # export master
  backline("Crash exportd and check core file")  
  process="-T exportd"
  if crash_process(process,"Blocking") != True: return 1
  os.system("./setup.py exportd start")
  time.sleep(4)
  if check_core_process(process,1) != True: return 1
  os.system("./setup.py core remove all")  

  return 0


#___________________________________________________
def del_check_file(dir1,dir2):
#  print(" del_check_file(%s,%s)"%(dir1,dir2))
  nb = int(0)
  for f in os.listdir(dir1):
    if '@' in f:
      f2=f.split('@')[3]
    else:
      f2=f
    f1="%s/%s"%(dir1,f)
    f2="%s/%s"%(dir2,f2)
#    print("%s %s"%(f1,f2))

    if filecmp.cmp(f1,f2) == False: 
      report("%s and %s differ"%(f1,f2))
      return 0
    nb = nb + int(1)  
  return nb

#___________________________________________________
def trash_allocate(trash_dir, nb):
# Create every thing in trash test directory
#___________________________________________________ 
  backline("Trash create test directory %s"%(trash_dir))    
  os.system("mkdir -p %s"%(trash_dir))
  os.system("rozo_trash recursive %s > /dev/null"%(trash_dir))
  os.system("rozo_trash root enable %s > /dev/null"%(trash_dir))
  os.system("mkdir -p %s/ref"%(trash_dir))
  
  for idx in range(int(nb)):
    os.system("cp ref %s/ref/f%s"%(trash_dir,idx))
  for idx in range(int(nb)):
    os.system("truncate -s %d %s/ref/empty%s"%(int(4096)*int(idx)+4096,trash_dir,idx))

  os.system("cp -rf %s/ref %s/del"%(trash_dir,trash_dir))

  res = del_check_file("%s/ref"%(trash_dir), "%s/del"%(trash_dir))
  if int(res) != int(nb)*2:
    report("create check file failed %s"%(res))
    return 1
  return 0  

#___________________________________________________
def trash_release(trash_dir):
# Definitively delete every thing in trash test directory
#___________________________________________________ 
  backline("Trash remove test directory %s"%(trash_dir))    
  os.system("rm -rf %s"%(trash_dir))
  os.system("rm -rf %s/@rozofs-trash@/*"%(mnt))
#___________________________________________________
def trash_get_delete_dir(trash_dir):
# Get the name of the del dir in trash
#___________________________________________________ 

  # Get trashed named
  trash_del_dir = None
  for f in os.listdir("%s/@rozofs-trash@"%(trash_dir)):
    if "del" in f: 
      return f

  report("Can not find del dir in %s/@rozofs-trash@"%(trash_dir))
  return 1
#___________________________________________________
def trash_delete(trash_dir,nb):
# Delete the del directory and its content. i.e put to trash
#___________________________________________________ 
  backline("Trash delete %s/del"%(trash_dir))    

  # Delete files
  os.system("rm -rf %s/del"%(trash_dir))

  # Get trashed directory named
  trash_del_dir = trash_get_delete_dir(trash_dir)
  if trash_del_dir == None: 
    return 1

  # Check restored files againt reference files
  res = del_check_file("%s/@rozofs-trash@/%s"%(trash_dir,trash_del_dir), "%s/ref"%(trash_dir))
  if int(res) != int(nb)*2:
    report("Delete check file failed %s"%(res))
    return 1
  return 0     
#___________________________________________________
def trash_restore(trash_dir,nb):
# Restore the del directory and its content. 
#___________________________________________________ 

  backline("Trash restore %s/del"%(trash_dir))    

  # Get trashed directory named
  trash_del_dir = trash_get_delete_dir(trash_dir)
  if trash_del_dir == None: 
    return 1

  # Restore files
  os.system("cd %s/@rozofs-trash@/; mv %s .."%(trash_dir,trash_del_dir))
  for f in os.listdir("%s/@rozofs-trash@/@rozofs-trash@del"%(trash_dir)):
    length=len(f.split('@'))
    name = f.split('@')[length-1]
    os.system("mv %s/@rozofs-trash@/@rozofs-trash@del/%s %s/del/%s"%(trash_dir,f,trash_dir,name))

  # Check restored files againt reference files
  res = del_check_file("%s/ref"%(trash_dir), "%s/del"%(trash_dir))
  if int(res) != int(nb)*2:
    report("restored del_check_file %s"%(res))
    return 1

  return 0  
#___________________________________________________
def trashNrestore():
# Create files and the delete them and then restore 
# them 2 times
#___________________________________________________ 

  nb=int(nbGruyere)
  trash_dir="%s/trash.%s"%(exepath,nb)
  
  # Create trash test directory when it does not exist
  if not os.path.exists(trash_dir): trash_allocate(trash_dir,nb)
  
  # Loop on delete then restore   
  for loop in range(3):
    if trash_delete(trash_dir,nb) != 0:  return 1
    if trash_restore(trash_dir,nb) != 0: return 1
    
  trash_release(trash_dir)           
  return 0  
#___________________________________________________
def trashNrebuild():
# reread files create by test_rebuild utility to check
# their content
#___________________________________________________ 
  nb=int(nbGruyere)
  trash_dir="%s/trash.%s"%(exepath,nb)
  
  # Create trash test directory when it does not exist
  if not os.path.exists(trash_dir): trash_allocate(trash_dir,nb)

  # Put del dir to trash
  if trash_delete(trash_dir,nb) != 0:  return 1
  # Rebuild every node
  if rebuild_1node() != 0:  return 1
  # Restore files
  if trash_restore(trash_dir,nb) != 0: return 1

  trash_release(trash_dir)               
  return 0
 
#___________________________________________________
def gruyere_one_reread():
# reread files create by test_rebuild utility to check
# their content
#___________________________________________________ 
  clean_cache()
  res=cmd_returncode("./IT2/test_rebuild.exe -action check -nbfiles %d -mount %s"%(int(nbGruyere),exepath))
  if res != 0: report("re-read result %s"%(res))
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
  return 0
#___________________________________________________
def rebuild_1dev() :
# test rebuilding device per device
#___________________________________________________
  global sids

  if rebuildCheck == True: 
    gruyere()        

  ret=1 
  for s in sids:
    
    hid=s.split('-')[0]
    cid=s.split('-')[1]
    sid=s.split('-')[2]
        
    device_number,mapper_modulo,mapper_redundancy = get_device_numbers(hid,cid)
    
    dev=int(hid)%int(mapper_modulo)
    clean_rebuild_dir()    

    backline("rebuild cid %s sid %s device %s"%(cid,sid,dev))
    os.system("./setup.py sid %s %s device-clear %s"%(cid,sid,dev))    
    string="./setup.py sid %s %s rebuild -fg -d %s -o one_cid%s_sid%s_dev%s"%(cid,sid,dev,cid,sid,dev)
    ret = cmd_returncode(string)
    if ret != 0:
      return ret
      
    if int(mapper_modulo) > 1:
      dev=(dev+1)%int(mapper_modulo)
      backline("rebuild cid %s sid %s device %s"%(cid,sid,dev))
      os.system("./setup.py sid %s %s device-clear %s"%(cid,sid,dev))
      string="./setup.py sid %s %s rebuild -fg -d %s -o one_cid%s_sid%s_dev%s "%(cid,sid,dev,cid,sid,dev)
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
def get_device_state(hid, cid, sid, dev) :

  # Check The status of the device
  string="rozodiag -i localhost%s -T storio:%s -c device "%(hid,cid)
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
      if int(words[0]) != int(dev): continue
      status=words[1].split()[0]
      return status
    except:
      pass    
  return "???"
#___________________________________________________
def loop_on_waiting_device_status(hid, cid, sid, dev, expected_status) :

  #
  # Loop waiting for device to become 
  #
  init=int(150)
  count = init	
  status    = "??"
  newstatus = get_device_state(hid,cid,sid,dev)

  log("%s/%s/%s wait %s loops from %s to %s"%(cid, sid, dev, count, newstatus, expected_status))  

  while count > int(0):

    newstatus = get_device_state(hid,cid,sid,dev)
    
    if expected_status == newstatus: 
      log("%s/%s/%s count %3d/%d -> %s SUCCESS"%(cid, sid, dev, count,init,newstatus))
      return 0       

    if newstatus != status: log("%s/%s/%s count %3d/%d -> %s"%(cid, sid, dev, count,init,newstatus))
    status = newstatus 

    # Sleep for 10 seconds
    count=count-1
    time.sleep(3)

  report("%s/%s/%s count %3d/%d -> %s FAILED !!!"%(cid, sid, dev, count,init,expected_status))
  return 1
    
#___________________________________________________
def selfhealing_spare(hid, cid, sid, dev) :

  # Check wether automount is configured
  string="rozodiag -i localhost%s -T storio:%s -c cc set device_selfhealing_mode spareOnly"%(hid,cid)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  log("Wait rebuild on spare %s:%s:%s"%(cid,sid,dev))	          

  #
  # Loop waiting for device to become 
  #
  ret = loop_on_waiting_device_status(hid,cid,sid,dev,"IS")
  if ret != 0: return ret
  return 0
#___________________________________________________
def selfhealing_resecure(hid, cid, sid, dev) :
  log("Wait resecure host %s cid %s sid %s device %s"%(hid,cid,sid,dev))	          
  
  # Check wether automount is configured
  string="rozodiag -i localhost%s -T storio:%s -c cc set device_selfhealing_mode resecure"%(hid,cid)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  # Delete a device from a SID
  ret = os.system("./setup.py sid %s %s device-delete %s"%(cid,sid,dev))

  #
  # Loop waiting for device to become 
  #
  ret = loop_on_waiting_device_status(hid,cid,sid,dev,"OOS")
  if ret != 0: return ret
  return 0
  
#___________________________________________________
def get_hid(cid,sid) :
  for s in sids:
    hid=s.split('-')[0]
    
    zcid=s.split('-')[1]
    if int(zcid) != int(cid): continue
    
    zsid=s.split('-')[2] 
    if int(zsid) != int(sid): continue
   
    return hid
  report("get_hid( cid=%s, sid=%s) No such cid/sid"%(cid,hid))
  sys.exit(1) 
     
#___________________________________________________
def selfhealing() :
# test rebuilding device per device
#___________________________________________________

  clean_rebuild_dir()

  # Create reference file
  dir="%s/selfHealing"%(mnt)
  os.system("rm -rf %s; mkdir -p %s"%(dir,dir))
  for i in range(60):
    zefile="%s/ref%s"%(dir,i)
    os.system("cp ./ref %s"%(zefile))  

  # Get file distribution
  reffile="%s/ref1"%(dir)
  string = "./setup.py cou %s"%(reffile)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for line in cmd.stdout:

    if "CLUSTER" in line:
        cid=int(line.split()[2])
	continue

    if "STORAGE" in line:
        storages=line.split()[2].split('-')   
        sid0 = int(storages[0])             
        sid1 = int(storages[1])             
        sid2 = int(storages[2])             
        sid3 = int(storages[3])             
	continue	
          	  	
    if "bins_0" not in line: continue

    if "/srv/rozofs/storages/storage_%s_%s/"%(cid,sid0) in line:
      dev0 = line.split()[3].split('/')[5]
      log("cid %s sid %s dev %s %s"%(cid,sid0,dev0,line))
      continue
          	  	  
    if "/srv/rozofs/storages/storage_%s_%s/"%(cid,sid1) in line:
      dev1 = line.split()[3].split('/')[5]
      log("id %s sid %s dev %s %s"%(cid,sid1,dev1,line))
      continue
          	  	  
    if "/srv/rozofs/storages/storage_%s_%s/"%(cid,sid2) in line:
      dev2 = line.split()[3].split('/')[5]
      log("id %s sid %s dev %s %s"%(cid,sid2,dev2,line))
      continue
      
    if "/srv/rozofs/storages/storage_%s_%s/"%(cid,sid3) in line:
      dev3 = line.split()[3].split('/')[5]
      log("id %s sid %s dev %s %s"%(cid,sid3,dev3,line))
      break

  hid0 = get_hid(cid,sid0)
  ret = selfhealing_resecure(hid0,cid,sid0,dev0)
  if ret != 0: return 1
     
  hid1 = get_hid(cid,sid1)
  ret = selfhealing_resecure(hid1,cid,sid1,dev1)
  if ret != 0: return 1
     
  hid2 = get_hid(cid,sid2)
  ret = selfhealing_resecure(hid2,cid,sid2,dev2)
  if ret != 0: return 1
     
  hid3 = get_hid(cid,sid3)
  ret = selfhealing_resecure(hid3,cid,sid3,dev3)
  if ret != 0: return 1
     
  if filecmp.cmp(reffile,"./ref") == False: 
    report("%s and %s differ"%(reffile,"./ref"))
    return 1 

  # Create 2 spare device
  log("Create spare devices")
  os.system("./setup.py spare; ./setup.py spare; ./setup.py spare; ./setup.py spare")

  ret = selfhealing_spare(hid0,cid,sid0,dev0)
  if ret != 0: return 1
  
  ret = selfhealing_spare(hid1,cid,sid1,dev1)
  if ret != 0: return 1

  ret = selfhealing_spare(hid2,cid,sid2,dev2)
  if ret != 0: return 1

  ret = selfhealing_spare(hid3,cid,sid3,dev3)
  if ret != 0: return 1

  for i in range(60):
    zefile="%s/ref%s"%(dir,i)
    if filecmp.cmp(reffile,"./ref") == False: 
      report("%s and %s differ"%(reffile,"./ref"))
      return 1            
  return 0
#___________________________________________________
def rebuild_all_dev() :
# test re-building all devices of a sid
#___________________________________________________
  global sids

  if rebuildCheck == True: 
    gruyere()        

  ret=1 
  for s in sids:
    
    hid=s.split('-')[0]
    cid=s.split('-')[1]
    sid=s.split('-')[2]

    clean_rebuild_dir()

    os.system("./setup.py sid %s %s device-clear all 1> /dev/null"%(cid,sid))
    backline("rebuild cid %s sid %s"%(cid,sid))
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

  if rebuildCheck == True: 
    gruyere()        
    
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
    backline("rebuild node %s"%(hid))
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

  if rebuildCheck == True: 
    gruyere()        
    
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
    
    backline("rebuild node %s nominal"%(hid))    
    string="./setup.py storage %s rebuild -fg -o node_nominal_%s --nominal"%(hid,hid)
    ret = cmd_returncode(string)
    if ret != 0:
      return ret
    
    backline("rebuild node %s spare"%(hid))    
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
def rebuild_fid() :
# test rebuilding per FID
#___________________________________________________

  if rebuildCheck == True: 
    gruyere()        
    
  list2rebuild = [ '1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','empty1','empty2','empty3','empty4']
  for f in list2rebuild:
  
    # Get the split of file on storages      
    string="attr -q -g rozofs %s/rebuild/%s"%(mnt,f)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # loop on the bins file constituting this file, and ask
    # the storages for a rebuild of the file
    bins_list = []
    fid=""
    cid=int(0)
    storages=""
    slice=""
    for line in cmd.stdout:
	  
      if "FID_SP" in line:
        if int(cid) == int(0): continue
        words=line.split();
	if len(words) < 3: continue
        fid=words[2]
        # A subfile can be rebuilt
        # loop on the bins file constituting this file, and ask
        # the storages for a rebuild of the file
        line_nb=0
        for sid in storages.split('-'):
          sid=int(sid)
          line_nb=line_nb+1

          clean_rebuild_dir()
          
          string="./setup.py sid %s %s path"%(cid,sid)
          parsed = shlex.split(string)
          cmd1 = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
          for line in cmd1.stdout:
            path =  line.split()[0]
            break
          os.system("rm -f %s/*/bins_*/%s/%s-*"%(path,slice,fid))
                    
          backline("rebuild/%s cid %s sid %s FID %s"%(f,cid,sid,fid))

          string="./setup.py sid %s %s rebuild --nolog -fg -f %s -o fid%s_cid%s_sid%s"%(cid,sid,fid,fid,cid,sid)
          ret = cmd_returncode(string)

          if ret != 0:
            report("%s failed"%(string))
	    return 1 	                 
	  continue
	continue
          
      if "CLUSTER" in line:
        words=line.split();
	if len(words) >= 2:
          cid=int(words[2])
	  continue
          
      if "ST.SLICE" in line:
        words=line.split();
	if len(words) >= 2:
          slice=int(words[1])
	  continue
	  
      if "STORAGE" in line:
        words=line.split();
	if len(words) >= 2:
          storages=words[2]
	  continue	  	  	           

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
  global stopOnFailure
  
  tst_num=int(0)
  failed=int(0)
  success=int(0)
  
  dis = adaptative_tbl(4,"TEST RESULTS",blue)
  dis.new_center_line()
  dis.set_column(1,'#',blue)
  dis.set_column(2,'Name',blue)
  dis.set_column(3,'Result',blue)
  dis.set_column(4,'Duration',blue)
  dis.end_separator()  

  time_start=time.time()
  
  total_tst=len(list)    
  for tst in list:

    tst_num=tst_num+1
    
    log("%10s ........ %s"%("START TEST",tst))
    
    console( "___%4d/%d : %-40s "%(tst_num,total_tst,tst))

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
      log("%10s %8s %s"%("SUCCESS",my_duration(delay),tst))    
      dis.set_column(3,'OK',green)
      success=success+1
    elif ret == 2:
      log("%10s %8s %s"%("NOT FOUND",my_duration(delay),tst))        
      dis.set_column(3,'NOT FOUND',red)
      failed=failed+1    
    else:
      log("%10s %8s %s"%("FAILURE",my_duration(delay),tst))        
      dis.set_column(3,'FAILED',red)
      failed=failed+1
      
    if failed != 0 and stopOnFailure == True:
        break
         
    
  dis.end_separator()   
  dis.new_line()  
  dis.set_column(1,'%s'%(success+failed))
  dis.set_column(2,"All tests")
  if failed == 0:
    dis.set_column(3,'OK',green)
  else:
    dis.set_column(3,'%d FAILED'%(failed),red)
    
  delay=time.time()-time_start    
  dis.set_column(4,'%s'%(my_duration(delay)))
  
  console("")
  dis.display()        
  
     
#___________________________________________________
def do_list():
# Display the list of all tests
#___________________________________________________

  num=int(0)
  dis = adaptative_tbl(4,"TEST LIST",blue)
  dis.new_center_line()  
  dis.set_column(1,'Number',blue)
  dis.set_column(2,'Test name',blue)
  dis.set_column(3,'Test group',blue)
  
  dis.end_separator()  
  for tst in TST_BASIC:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,tst)
    dis.set_column(3,'basic') 
     
  dis.end_separator()  
  for tst in TST_TRASH:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,tst)
    dis.set_column(3,'trash') 
     
  dis.end_separator()  
  for tst in TST_FLOCK:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,tst)
    dis.set_column(3,'flock')  
    
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

  dis.end_separator()         
  for tst in TST_COMPIL:
    num=num+1
    dis.new_line()  
    dis.set_column(1,"%s"%num)
    dis.set_column(2,"%s"%(tst))
    dis.set_column(3,'compil')  


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
    report( "No such cid/sid %s/%s"%(cid,sid))
    return -1,"" 
  return site0,path0
       
#____________________________________
def resolve_mnt(inst):
  global site
  global eid
  global vid
  global vid_fast
  global mnt
  global exp
  global inverse
  global forward
  global safe
  global instance
  global nb_failures
  global sids
  global hosts
    
  vid      = "A"
  vid_fast = None
  pid      = None  
  instance = inst
        
  string="%s/setup.py mount %s info"%(os.getcwd(),instance)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  for line in cmd.stdout:
    words = line.split()
    if len(words)<3: continue
    if words[0] == "site": site=words[2]
    if words[0] == "eid" : eid=words[2]
    if words[0] == "vid"      : vid      = words[2]
    if words[0] == "vid_fast" : vid_fast = words[2]
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
    report( "No such RozoFS mount instance %s"%(instance))
    exit(1)    
    
  if pid == None:
    report( "RozoFS instance %s is not running"%(instance))
    exit(1)      
#___________________________________________  
def cmd_returncode (string):
  global verbose
  if verbose: console(string)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#  for line in cmd.stdout:
#    print line
  cmd.wait()
  
  return cmd.returncode
#___________________________________________  
def cmd_system (string):
  global verbose
  if verbose: console(string)
  os.system(string)
        
#___________________________________________________
def usage():
#___________________________________________________

  console("\n./IT2/IT.py -l")
  console("  Display the whole list of tests.")
  console("\n./IT2/IT.py [options] [extra] <test name/group> [<test name/group>...]" )     
  console("  Runs a test list.")
  console("    options:")
  console("      [--mount <mount1,mount2,..>]  A comma separated list of mount point instances. (default 1)"  ) 
  console("      [--speed]          The run 4 times faster tests.")
  console("      [--fast]           The run 2 times faster tests.")
  console("      [--long]           The run 2 times longer tests.")
  console("      [--repeat <nb>]    The number of times the test list must be repeated." )  
  console("      [--stop]           To stop tests on failure." )
  console("      [--fusetrace]      To enable fuse trace on test. When set, --stop is automaticaly set.")
  console("      [--debug]          ACtivate debug trace.")
  console("    extra:")
  console("      [--process <nb>]   The number of processes that will run the test in paralell. (default %d)"%(process))
  console("      [--count <nb>]     The number of loop that each process will do. (default %s)"%(loop) )
  console("      [--fileSize <nb>]  The size in MB of the file for the test. (default %d)"%(fileSize)  ) 
  console("      [--rebuildCheck]   To check strictly after each rebuild that the files are secured.")
  console("    Test group and names can be displayed thanks to ./IT2/IT.py -l")
  console("       - all              designate all the tests.")
  console("       - rw               designate the read/write test list.")
  console("       - storageFailed    designate the read/write test list run when a storage is failed.")
  console("       - storageReset     designate the read/write test list run while a storage is reset.")
  console("       - storcliReset     designate the read/write test list run while the storcli is reset.")
  console("       - basic            designate the basic test list.")
  console("       - trash            designate the trash feature test list.")
  console("       - rebuild          designate the rebuild test list.")
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
parser.add_option("-s","--stop", action="store_true",dest="stop", default=False, help="To stop on failure.")
parser.add_option("-t","--fusetrace", action="store_true",dest="fusetrace", default=False, help="To enable fuse trace on test.")
parser.add_option("-F","--fast", action="store_true",dest="fast", default=False, help="To run 2 times faster tests.")
parser.add_option("-S","--speed", action="store_true",dest="speed", default=False, help="To run 4 times faster tests.")
parser.add_option("-L","--long", action="store_true",dest="long", default=False, help="To run 2 times longer tests.")
parser.add_option("-r","--repeat", action="store", type="string", dest="repeat", help="A repetition count.")
parser.add_option("-m","--mount", action="store", type="string", dest="mount", help="A comma separated list of mount points to test on.")
parser.add_option("-R","--rebuildCheck", action="store_true", dest="rebuildCheck", default=False, help="To request for strong rebuild checks on each rebuild.")
parser.add_option("-e","--exepath", action="store", type="string", dest="exepath", help="re-exported path to run the test on.")
parser.add_option("-n","--nfs", action="store_true",dest="nfs", default=False, help="Running through NFS.")
parser.add_option("-d","--debug", action="store_true",dest="debug", default=False, help="Debug trace.")

# Read/write test list
TST_RW=['read_parallel','write_parallel','rw2','wr_rd_total','wr_rd_partial','wr_rd_random','wr_rd_total_close','wr_rd_partial_close','wr_rd_random_close','wr_close_rd_total','wr_close_rd_partial','wr_close_rd_random','wr_close_rd_total_close','wr_close_rd_partial_close','wr_close_rd_random_close']
# Basic test list
TST_BASIC=['cores','readdir','xattr','link','symlink', 'rename','chmod','truncate','bigFName','crc32','rsync','resize','reread','mmap']
TST_BASIC_NFS=['cores','readdir','link', 'rename','chmod','truncate','bigFName','crc32','rsync','resize','reread','mmap']
# Rebuild test list
# In case of strong rebuild checks, gruyere is processed before eand gruyere_reread aftter each test
TST_REBUILD=['gruyere','rebuild_fid','rebuild_1dev','rebuild_all_dev','rebuild_1node','rebuild_1node_parts','selfhealing','gruyere_reread']
TST_REBUILDCHECK=['rebuild_fid','rebuild_1dev','rebuild_all_dev','rebuild_1node','rebuild_1node_parts','selfhealing']

# File locking
TST_FLOCK=['lock_posix_passing','lock_posix_blocking','lock_bsd_passing','lock_bsd_blocking','lock_race','flockp_posix_passing','flockp_posix_blocking','flockp_bsd_passing','flockp_bsd_blocking','flockp_race']
TST_COMPIL=['compil_rozofs','compil_openmpi']
TST_TRASH=['trashNrestore','trashNrebuild']

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
    
if options.stop == True:  
  stopOnFailure=True 

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
    list.extend(TST_TRASH)
    list.extend(TST_FLOCK)
    if rebuildCheck == True:
      list.extend(TST_REBUILDCHECK)
    else:  
      list.extend(TST_REBUILD)
    list.extend(TST_COMPIL)    
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
  elif arg == "trash":
    list.extend(TST_TRASH)
  elif arg == "rebuild":
    if rebuildCheck == True:
      list.extend(TST_REBUILDCHECK)
    else:  
      list.extend(TST_REBUILD)
  elif arg == "flock":
    list.extend(TST_FLOCK)  
  elif arg == "compil":
    list.extend(TST_COMPIL)  
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
