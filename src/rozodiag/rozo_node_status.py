#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
# This file is part of Rozofs.
#
# Rozofs is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published
# by the Free Software Foundation, version 2.
#
# Rozofs is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see
# <http://www.gnu.org/licenses/>.
import sys
import os.path
import subprocess
import time
import re
import mmap
import shlex
import datetime
from optparse import OptionParser
import traceback

exportds=[]
ROZODIAG=None
dis=None
error_table=None
version=None
symbol=int(0)
diagnostic=False
fulldiagnostic=False

alarm_level = int(-1)

time_limit_minutes=0

#!/usr/bin/python
# -*- coding: utf-8 -*-

base=""

import sys

#_______________________________________________
class constants:

  def joined_column(self): return "#Zis_IZ_a_JoInED_ColUMn"
  
#_______________________________________________
class column_desc:

  def __init__(self,shift):  
    self.column_nb    = 0
    self.column_sizes = []
    self.shift        = shift
     
  def update_column(self, num, size):
  
    # Column number extension
    if int(num) > self.column_nb:
      for i in range(self.column_nb,num):
        self.column_sizes.append('0')
      self.column_nb = num 	
	
    # Column size extension
    if int(self.column_sizes[int(num)-1]) < int(size):
      self.column_sizes[int(num)-1] = int(size)

    
#_______________________________________________
class big_title:

  def __init__(self,text):  
    self.text = text

  def display(self,column_desc):
    l=0
    for col in range(column_desc.column_nb):
      l += (column_desc.column_sizes[col]+3)
    l -= (len(self.text) +3)
    
    line = ''    
    for i in range(int(column_desc.shift)): line+=' '		
    line+="| "
    start = int(l)/2
    end   = int(l)-start
    for i in range(start): line+=" "	    	
    line+=self.text
    for i in range(end): line+=" " 
    line+=" |"   
    print line  
#_______________________________________________
class separator_line:

  def __init__(self,extreme,separator,previous_line=None):  
    self.extreme    = extreme  
    self.separator  = separator
    self.separators = []
    if previous_line == None: return
      
    const = constants()    
    self.separators.append(extreme)
    skip=True
    for col in previous_line.column:
      if skip==True: 
        skip=False
	continue
      if col == const.joined_column(): self.separators.append('_')
      else:                            self.separators.append(separator)
    self.separators.append(extreme) 
    
  def display(self,column_desc):

    const = constants()
    line = ''    
    for i in range(int(column_desc.shift)): line+=' '
    
    if len(self.separators) != 0:
      for c in range(column_desc.column_nb):
	line += self.separators[c]
	line+='_'	
	for ci in range(int(column_desc.column_sizes[c])): line+='_'	
	line+='_' 
      line+=self.extreme 
      print line
      return       
    
    first=True
    for c in range(column_desc.column_nb):
      if first == True:
        # 1rst line begins with extreme separator
        first = False
	line += self.extreme
      else:	
        # Not a fisrt line
	line += self.separator
      line+='_'	
      for ci in range(int(column_desc.column_sizes[c])): line+='_'	
      line+='_' 
    line+=self.extreme   
    print line    
#_______________________________________________
class display_line:

  def __init__(self,centered=False):  
    self.column     = []   
    self.centered   = centered
      
  def set_column(self,column,value):
    # Extend column number
    if int(column) > len(self.column):
      for i in range(len(self.column),int(column)):
        self.column.append('')
    self.column[int(column)-1] = value

  def check_column(self,column,value):
    # Extend column number
    if int(column) > len(self.column): return False
    if self.column[int(column)-1] == value: return True
    return False
    
  # Join a colum with its preceding column  
  def join_preceding_column(self,column):
    const = constants()
    # Extend column number
    if int(column) > len(self.column):
      for i in range(len(self.column),int(column)):
        self.column.append('')
    self.column[int(column)-1] = const.joined_column()

  def display(self,column_desc):
    const = constants()
    line=''	
    for i in range(int(column_desc.shift)): line+=' '		
    line+="| "
    for col in range(column_desc.column_nb):
    
      try:     val=self.column[col]
      except:  val=''	
      
      if val == const.joined_column(): continue

      l = column_desc.column_sizes[col]-len(val)
      joined = 0
      for jc in range(col+1,column_desc.column_nb):
        try:    next = self.column[jc]
	except: next = ''
        if next != const.joined_column(): break
	l += column_desc.column_sizes[jc]+3
	joined += 1	
      if self.centered == True:
	start = int(l)/2
	end   = int(l)-start
      else:
	try:
	  float(val)	  
          start=l
	  end=0
	except:
	  start = 0
	  end = l

      for i in range(start): line+=" "	    	
      line+=val
      for i in range(end): line+=" " 
      line+=" | "  
      col+=joined 
    print line
        
#_______________________________________________
class adaptative_tbl:

  def __init__(self, shift, title=None):  
    self.row_nb      = int(0)
    self.row         = [] 
    self.current_row = None 
    self.column_desc = column_desc(shift)   
    if title == None: 
      self.separator(' ',' ')      
    else:
      self.separator(' ','_')
      self.row.append(big_title(title)) 
      self.row_nb += 1
      self.separator('|','_')
    
  def add_line(self,centered):
    line = display_line(centered)
    self.row.append(line) 
    self.row_nb += 1
    self.current_row = line
    
  def new_line(self):    self.add_line(False)
  def new_center_line(self): self.add_line(True)
  
  def separator(self,extreme,separator):
    self.row.append(separator_line(extreme,separator,self.current_row)) 
    self.row_nb = int(self.row_nb)+1
    self.current_row = None
            
  def end_separator(self): self.separator('|','|')	 
         
  def set_column(self,column,value):
    self.current_row.set_column(column,value)
    self.column_desc.update_column(column,len(value))      

  def join_preceding_column(self,column):
    self.current_row.join_preceding_column(column)
                	
  def display(self):
    # Must we add and end separator ?
    sys.stdout.write("\r")
    if self.current_row != None: self.end_separator()  
    for row in range(int(self.row_nb)):              
      self.row[row].display(self.column_desc)
      previous_line=self.row[row]

#_______________________________________________  
def increment_counter(parsed):
  global symbol
  global fulldiagnostic
  global debug
    
  if debug == True: 
    print parsed   
  else:
    if fulldiagnostic == True:
      symbol+=int(1)
      sys.stdout.write("\rTest nb %s"%(symbol))
      sys.stdout.flush()
  return

#_______________________________________________  
# Translate a criticity to its integer value
def criticity2int(criticity):
  if criticity == 'WARNING':    return int(1)
  if criticity == 'ERROR':      return int(2)
  if criticity == 'CRITICAL':   return int(3)
  return int(0)

#_______________________________________________  
# Retrieve the error table
def get_error_table():
  global error_table

  # Create error table on the first call
  if error_table == None: 
    error_table = adaptative_tbl(0,"ERROR/WARNING LIST")
    error_table.new_center_line()      
    error_table.set_column(1,'host') 
    error_table.set_column(2,'port') 
    error_table.set_column(3,'module') 
    error_table.set_column(4,'criticity') 
    error_table.set_column(5,'problem description') 
    error_table.set_column(6,'diagnostic command') 
    error_table.end_separator()
    
  return error_table   

#_______________________________________________  
# Check whether an error is present in the error table
def check_error_table(host, port, module, criticity, string):
  global error_table

  if error_table == None: return False

  for row in error_table.row:
    try:
      if row.check_column(1, host) == False: continue
      if row.check_column(2,"%s"%(port)) == False: continue
      if row.check_column(3,module) == False: continue 
      if row.check_column(4,criticity) == False: continue 
      if row.check_column(5,string) == False: continue
      return True 
    except:
      continue
  return False
#_______________________________________________  
# Add an error to the error table
def add_error_table(host, port, module, addr, criticity, string, tip=None):
  global   base
  global   alarm_level

  # Check whether this error has already been registered
  if check_error_table(host, port, module, criticity, string) == True: return 

  # Update global criticity level
  if criticity2int(criticity) > int(alarm_level):
    alarm_level = criticity2int(criticity)

  tbl = get_error_table()
  tbl.new_line()
  tbl.set_column(1,host) 
  tbl.set_column(2,"%s"%(port)) 
  tbl.set_column(3,module) 
  tbl.set_column(4,criticity) 
  tbl.set_column(5,string) 
  if tip == None: return
  try:
    port = int(self.port)
    tbl.set_column(6,"%sdiag -i %s -p %s -c %s"%(base,addr,port,tip))
  except:
    tbl.set_column(6,"%sdiag -i %s -T %s -c %s"%(base,addr,port,tip))

#_______________________________________________  
# Display the error table
def display_error_table():
  global error_table
  global quiet
  if quiet == True: return
  print "      "
  if error_table != None: 
    error_table.display() 
  else:                   
    print "No error detected"  
  
#_______________________________________________
class rozofs_module:

  def __init__(self,port,module):
    global modules
    host = "127.0.0.1"
    if options.address != None: host = options.address
    self.host   = host
    self.addr   = self.host.split('/')
    self.port   = port
    self.module = module
    self.activeAddr = host
    
  def id(self): return "[%s-%s]"%(self.module, self.port) 
                        
  def CRITICAL(self,string,tip=None): 
    add_error_table(self.host,self.port,self.module,self.activeAddr,'CRITICAL',string, tip)
  def ERROR(self,string,tip=None): 
    add_error_table(self.host,self.port,self.module,self.activeAddr,'ERROR',string,tip) 
  def WARNING(self,string,tip=None): 
    add_error_table(self.host,self.port,self.module,self.activeAddr,'WARNING',string,tip)

  def rozodiag_one_addr(self,cmd,addr):
    global ROZODIAG        
    if addr == '': return None

    
    # who is either a port number or a target name 
    try:
      port = int(self.port)
      parsed = [ROZODIAG, "-t", "1", "-i",addr,"-p","%s"%(self.port),"-c",cmd] 
    except:
      parsed = [ROZODIAG, "-t", "1", "-i",addr,"-T",self.port,"-c",cmd]  

    increment_counter(parsed)

    try:    command = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except: return None

    try:    output, error = command.communicate()
    except: return None

    if options.debug == True: 
      print output    
      if error != '': print error

    if "error on connect" in output: 
      self.CRITICAL("Not responding")
      return None

    if error != '': return None
    
    self.activeAddr = addr
    lines=output.split('\n')
    return lines
            
  def rozodiag(self,cmd):
    """Run a rozodiag command toward this rozofs module
    """        
    return self.rozodiag_one_addr(cmd,self.activeAddr)


  def check_accessibility(self):
    return self.rozodiag("?")  

  def check_version(self):
    """Get the version of a process
    """    
    global version
    if version == None: return True
    
    res = self.rozodiag("version")
    if res == None: return None

    for line in res:
      words = line.split()
      if len(words) < 3: continue
      if words[0] == "version":
	if version != words[2]:
          self.WARNING("has version %s (exportd has version %s)"%(words[2],version),"version")
	  return False
	return True  

  def get_git_tag(self):
    """Get the git tag
    """    
    global version
    res = self.rozodiag("git")
    if res == None: return None

    for line in res:
      words = line.split()
      if len(words) < 3: continue
      if words[0] == "git":
	tag = line.split(':')[1]
	return tag
    return None  
  
  def check_buffer(self):
    """Check whether some buffers are exhausted
    """    
    res = self.rozodiag("buffer")
    if res == None: return

    for line in res:
      words=line.split()
      if len(words) < 13: continue
      available=words[11]

      if available == "0/": self.WARNING("No buffer %s left"%(words[0]),"buffer")

  def check_rpc_resources(self):
     """Check whether some RPC errors have been detected
     """    
     TOCHECK=['SEND_ERR','ENCODING_ERROR','ENCODING_ERROR','NO_CTX_ERROR','NO_BUFFER_ERROR']
     failed = False

     res = self.rozodiag("rpc_resources")
     if res == None: return False

     for line in res:
       for what in TOCHECK:
	 if what in line:
           val= line.split(':')[1].split()[0]
           if int(val) > int(0):
             self.WARNING("%s RPC %s detected"%(val,what),"rpc_resources")	     
	     return False
     return True

  def check_tcp_info(self):
     """Check whether some TCP miss functionning have occured
     """    
     res = self.rozodiag("tcp_info")
     if res == None: return False

     for line in res:
       words=line.split()
       if len(words) < int(24): continue
       try:
         int(words[6]) 
         if int(words[6]) != 0 or int(words[18]) != 0 or int(words[20]) !=0:
	  self.WARNING("TCP retransmit or lost","tcp_info");	     
	  return True
       except:
         continue	 
     return True
     
  def check_uptime(self):
     """Check the system is up for more than 1 day"
     """    
     global time_limit_minutes
     res = self.rozodiag("uptime")
     if res == None: return False

     for line in res:
       words=line.split()
       try:
	 minutes  = int(words[2])*24*60 
	 minutes += int(words[4].split(':')[0])*60
	 minutes += int(words[4].split(':')[1])	 
	 if int(minutes) <= int(time_limit_minutes): self.WARNING("UP for only %s minutes"%(minutes),"uptime")
       except:
	 continue
       return True  
      
  def check_core_files(self):
     res = self.rozodiag("core")
     if res == None: return False
     if "None" in res: return False
     self.WARNING("Some core file exist","core");	    
     return True
    
  def check_logs(self):
    path = self.get_core_path()
    for file in os.listdir(path):
      self.WARNING("core file exist %s/%s"%(path,file))
    return	     
     
  def check_trx(self):
    """Check whether some tansaction error have occured
       on who at address addr
    """    
    TOCHECK=['SEND_ERR','TX_TIMEOUT',]

    res = self.rozodiag("trx")
    if res == None: return False

    status = True
    for line in res:

      if "number of transaction" in line:
	count=line.split(':')[1]
	initial=count.split('/')[0]
	allocated=count.split('/')[1]   
	if int(initial) ==  int(allocated):
          self.WARNING("All TRX context allocated","trx")
	  status = False
	else:
	  percent = int(allocated)*100/int(initial)
	  if int(percent) >= int(90):
            self.WARNING("%s/%s TRX context allocated"%(allocated,initial),"trx")	     
	continue

      words=line.split()
      if len(words) < 3: continue

      if words[1] in TOCHECK:
	if words[3] != 0: 
	  self.WARNING("%s %s"%(words[3],words[1]),"trx");	     
	  status = False
      
#_______________________________________________
class storio(rozofs_module):

  def __init__(self,port):
    rozofs_module.__init__(self,port,'storio')

  def check_storio_log(self):
    """
    """    
    res = self.rozodiag("log")
    if res == None: return False

    for line in res:
      if "nb log" not in line: continue
      val=line.split(':')[1].split('/')[0]
      if int(val) != int(0):
	self.WARNING("%s disk errors occured"%(val),"log")
	return False
    return True  

  def check_fid(self):
    """
    """    
    res = self.rozodiag("fid")
    if res == None: return False

    for line in res:
      if "out of ctx" not in line: continue
      val=line.split(':')[1].split()[0]
      if int(val) > int(0):
	self.WARNING("has %s out of FID ctx"%(val),"fid")
	return False
      return True  
    return False	

  def check_data_integrity(self):
    """Check whether CRC32 errors have been detected
    """    
    res = self.rozodiag("data_integrity")
    if res == None: return

    for line in res:
      words=line.split()  
      if "generation" in line:
	if words[3] != "ENABLED":
          self.WARNING("CRC32 not generated","data_integrity")
	continue
      if "control" in line:
	if words[3] != "ENABLED":
          self.WARNING("CRC32 not controled","data_integrity")
	  break
	continue
      if "computing" in line:
	if words[4] != "HARDWARE":
          self.WARNING("CRC32 computation is %s"%(words[4]),"data_integrity")
	continue
      if "error counter" in line:
	if words[4] != "0": 
          self.WARNING("%s CRC32 errors detected"%(words[4]),"data_integrity")
	continue	   


  def check(self):
    global fulldiagnostic
    if self.check_storio_log() == False: return False
    if fulldiagnostic == True:
      self.rozodiag("uptime")    
      self.check_version() 
      self.check_core_files()
      self.check_uptime()    
      self.check_buffer()
      self.check_data_integrity()
      self.check_rpc_resources()
      #self.check_tcp_info()
      self.check_fid()
    return True 
      

#_______________________________________________
class storage(rozofs_module):

  def __init__(self,site,cid,sid):
    global storaged
    rozofs_module.__init__(self,'storaged','storaged')    
    self.site    = site
    self.cid     = cid
    self.sid     = sid
    self.failed  = False

  def display(self):
    if self.failed == True: print "        sid %s \t%s\tFailed !!!"%(self.sid,self.host)
    else:                   print "        sid %s \t%s"%(self.sid,self.host)
                
  def check_devices(self):
    res = self.rozodiag("device")
    if res == None: return -1
        
    error_list=[]	
    for line in res:
      words=line.split('|') 
      try: int(words[1])
      except: continue
      status = words[4].split()[0]
      cid=words[1].split()[0]
      sid=words[2].split()[0]
      dev=words[3].split()[0]		
      free= int(words[7]) 
      try:diag= words[15].split()[0]
      except:diag=""
      if status == "REBUILD":
	self.WARNING("Device %s of %s is in REBUILD"%(dev, string),"device")
      elif status != "IS":
        error_list.append("cid%s/sid%s"%(cid,sid))
	self.ERROR("Device %s of cid%s/sid%s is %s"%(dev, cid, sid, status),"device") 
        self.failed = True 
      if free <= int(20):
	self.WARNING("Device %s of cid%s/sid%s has only %s%s free space"%(dev, cid, sid, free,'%'),"device")  

    # More than 1 error per CID/SID is critical
    for error in error_list:
      if error_list.count(error) > 1: self.CRITICAL("Several devices failed in %s"%(error),"device")         
    return 0	
             
  def check_storaged(self):    
    global fulldiagnostic 
    if self.check_accessibility() == None: return -1
    
    res = self.check_devices() 
    if res == -1: return -1
         
    if fulldiagnostic == True:
      self.check_version()     
      self.check_uptime()
      self.check_buffer()
      self.check_rpc_resources()
      #self.check_tcp_info() 
      self.check_core_files()   
    return 0
                   
  def check_storios(self):
      res = self.rozodiag("storio_nb")
      if res == None:
        self.failed = True 
        return -1
      for line in res:
	words=line.split()
	if len(words) < 1:    continue
	if words[0] == "mode":
	  if words[2] == "single":
	    stio = storio("storio:0")
	    if stio.check() == False: self.failed = True	    
	    return 0
	if words[0] == "cids":
	  for idx in range(2,len(words)):   
	    stio = storio("storio:%s"%(words[idx]))
	    if stio.check() == False: self.failed = True	    
	  return 0
      return -1
  
#_______________________________________________
class storcli(rozofs_module):

  def __init__(self,port):
    rozofs_module.__init__(self,port,'storcli')          

  def check(self): 
    global fulldiagnostic
    status = True
    self.check_trx()
    if fulldiagnostic == True:
      self.check_uptime()
      self.check_version()
      self.check_buffer() 
      #self.check_tcp_info()
      self.check_core_files()
      res = self.rozodiag("start_config")
      if res == None:
	status = False
      else:  
	for line in res:
	  words=line.split()
	  if len(words) < 3: continue
	  if words[0] == "shaper":
	    if words[2] != "0":
	      self.failed = True 
	      self.ERROR("shaper is counfigured to %s"%(words[2]),"start_config")
	      status = False	          
    res = self.rozodiag("storaged_status")
    if res == None:
      status = False
    else: 
      error_count={}
      for line in res:
	words=line.split()
	if len(words) < 20:   continue
	if words[0] == "cid": continue
	if words[8] != "UP":
	  self.ERROR("cid%d/sid%d unreachable on host %s"%(int(words[0]),int(words[2]),words[4]),"storaged_status")
	  if not error_count.has_key(int(words[0])):
            error_count[int(words[0])]=0
	  error_count[int(words[0])]=error_count[int(words[0])] + 1
	  status = False
          continue
	if words[10] != "UP":
	  status = False
	  self.ERROR("cid%d/sid%d unreachable on host %s"%(int(words[0]),int(words[2]),words[4]),"storaged_status")
	  if not error_count.has_key(int(words[0])):
            error_count[int(words[0])]=0
          error_count[int(words[0])]=error_count[int(words[0])] + 1
	if words[12]!= "YES":
	  status = False
	  self.ERROR("cid%d/sid%d unreachable on host %s"%(int(words[0]),int(words[2]),words[4]),"storaged_status")
      	  if not error_count.has_key(int(words[0])):
            error_count[int(words[0])]=0
          error_count[int(words[0])]=error_count[int(words[0])] + 1
      for error_sid_nb in error_count.values():
        if error_sid_nb >= int(2):
	  self.CRITICAL("Too much cid/sid unreachable","storaged_status")
	  break

    return status

#_______________________________________________
class client(rozofs_module):

  def __init__(self,port,age):
    rozofs_module.__init__(self,port,'mount')          
    self.nbstorcli = "?" 
    self.status    = "OK"
    self.eid       = "?"
    self.vid       = "?"
    self.bsize     = "?"
    self.layout    = "?"
    self.age       = age
    self.site      = "?"
    self.mount     = "?"
    
    res = self.rozodiag("?") 
    if res == None: 
      self.status = "Failed"
      return
    res = self.rozodiag("start_config") 
    for line in res:
      if "running_site" in line:
	self.site=line.split('=')[1].split()[0]
	break        
           
    self.check_vid()
	     
    res = self.rozodiag("stclbg")
    if res == None: 
      self.status = "Failed"
      return
    else:
      for line in res:
        if "number of configured storcli" in line:
	  self.nbstorcli=line.split(':')[1].split()[0] 
	  break   
    res = self.rozodiag("exp_eid")
    if res != None:
      for line in res:
        words=line.split()
	if len(words) < 2: continue
	if words[0] == "mount":
	  self.eid="%d"%(int(words[1]))
	  break
    res = self.rozodiag("layout")
    if res != None:
      for line in res:
        if "LAYOUT_" in line:
	  words=line.split()
          self.layout = words[0].split('LAYOUT_')[1]
	  self.bsize  = words[4]
	  break
	  
  def check_vid(self):
    res = self.rozodiag("mount")
    if res == None: return

    for line in res:
      words=line.split()
      if len(words) < 3: continue
      if words[0] == "mount":
        self.mount = words[2]
	break
           
    try:
      parsed = ["attr", "-g","rozofs",self.mount]        
      command = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      output, error = command.communicate()
      for line in output.split('\n'):
        words=line.split()
        if len(words) < 3: continue
        if words[0] == "VID":
          self.vid = words[2]
        if words[0] == "EID":
          self.eid = words[2]	        
    except: 
      pass
      
  def check(self): 
    global diagnostic
    if self.status == "Failed": return
    self.check_trx()
    if diagnostic == True:
      self.check_version()  
      self.check_uptime()
      self.check_buffer()
      self.check_core_files()
    #self.check_tcp_info()
    
    try:
      int(self.nbstorcli)
    except:
      return  
    for idx in range (1,int(self.nbstorcli)+1):
      try:
        storcli_port=int(self.port)+int(idx)
        stc = storcli(storcli_port)
      except:
        stc = storcli(self.port+':'+"%d"%(idx))        	
      if stc.check() == False: self.status = "STORCLI failed" 
       
  def display(self):
    global dis
    if dis == None:     
      dis = adaptative_tbl(2,"Clients")
      dis.new_center_line()
      dis.set_column(1,'IP') 
      dis.set_column(2,'diag.')      
#      dis.join_preceding_column(2)
#      dis.join_preceding_column(3)
      dis.set_column(3,'site')        
      dis.set_column(4,'vol')          
      dis.set_column(5,'exp')        
      dis.set_column(6,'block')
      dis.set_column(7,'layout')       
      dis.set_column(8,'nb')
      dis.set_column(9,'status')        
      dis.set_column(10,'age') 
      dis.new_center_line()
      dis.set_column(1,'address') 
      dis.set_column(2,'port')
      dis.set_column(4,'id')       
      dis.set_column(5,'id') 
      dis.set_column(6,'size')
      dis.set_column(8,'stc')
      dis.set_column(10,'(sec)')          
      dis.end_separator()
    dis.new_line()
    dis.set_column(1,self.host) 
    dis.set_column(2,self.port)
    dis.set_column(3,self.site) 
    dis.set_column(4,self.vid) 
    dis.set_column(5,self.eid) 
    dis.set_column(6,self.bsize)
    dis.set_column(7,self.layout)
    dis.set_column(8,self.nbstorcli)
    dis.set_column(9,self.status)  
    dis.set_column(10,self.age)      

  @staticmethod   
  def output():
    global dis
    if dis != None: dis.display()
    dis = None
    

#_______________________________________________
def display_unit(val,precision=1):

  try:
    num=float(val)
  except:
    return val

  for x in ['','K','M','G','T','P']:
    if int(num) < 1000:
       format="%3."+("%d"%(precision))+"f%s"
       return format % (num, x)
    num /= 1000
    for cl in self.clients: cl.check()

#_______________________________________________
def test_client(instance):
  c = client("mount:%s"%(instance),"0")
  c.check()
  c.display()  

#_______________________________________________
def do_execute():
    
  # Check local storage configured in storage.conf
  s = storage(0,0,0) 
  
  if s.check_storaged() == 0: s.check_storios()

  # check local clients from /etc/fstab  
  #test_client(0)
  #test_client(1)
  #test_client(2)  
  with open("/etc/fstab") as f:
    for line in f:
      if line[0] == "#": continue
      if len(line.split()) < 3: continue
      if line.split()[2] == "rozofs":
	instance="0"
	if "instance" in line:
	  i=0
	  string=line.split("instance=")[1]
	  while string[i] != ',' and string[i] != ' ': 
	    instance=instance+string[i]
	    i=i+1
        c = client("mount:%d"%(int(instance)),"0")
        c.check()
        c.display()
  client.output()
    

  
#_______________________________________________
def do_exit():
  global fulldiagnostic
  global alarm_level
  
  if fulldiagnostic: 
    display_error_table() 
    try: client.output()
    except: pass	 	
  elif int(alarm_level) == int(-1):
    alarm_level = 0
  else: 
    display_error_table()
    
  sys.exit(int(alarm_level))	   

#_______________________________________________
def fatal(string):
  print("\nExecution stack:")
  traceback.print_exc()  
  print("\nFATAL: %s !!!"%(string))  
  sys.exit(-1)


###############################################
#
#                  M A I N 
#
###############################################
base=os.path.basename(sys.argv[0])
base=base.split("_node_status")[0]

parser = OptionParser()
parser.add_option("-d","--diagnostic", action="store_true",default=False, dest="diagnostic", help="Display the error list.")
parser.add_option("-f","--full", action="store_true",default=False, dest="full", help="Process to a full diagnostic of the system.")
parser.add_option("-g","--debug", action="store_true",default=False, dest="debug", help="Debug trace.")
parser.add_option("-a","--address", action="store",type="string", dest="address", help="optional address of the storaged.")

(options, args) = parser.parse_args()


time_limit_minutes = 2

# Is it a quiet or a verbose mode
if options.diagnostic == True: quiet=False
else:                          quiet=True

if options.full == True: 
  fulldiagnostic=True   
  quiet=False
else:
  fulldiagnostic=False  

debug=False
if options.debug == True: debug=True


# Find out rozodiag path
ROZODIAG=None
for path in ["./rozodiag","/usr/bin/rozodiag","/usr/sbin/rozodiag","/usr/local/bin/rozodiag","/usr/local/sbin/rozodiag","./build/src/rozodiag/rozodiag"]:
  if os.path.isfile(path):
    ROZODIAG=path
    break
if ROZODIAG == None: fatal("Can not find rozodiag")
 

try:     do_execute()
except:  fatal("execution failure")
do_exit()
  
