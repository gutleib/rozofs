#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os.path
import subprocess
import time
import re
import shlex
import datetime
import shutil
from adaptative_tbl import *
import syslog


#___________________________________________________
# Messages and logs
#___________________________________________________
def log(string): syslog.syslog(string)
def console(string): print string
def report(string): 
  console(string)
  log(string)
  
# Read configuratino file
#import * from setup.config

cids = []
hosts = []
volumes= []
mount_points = []

vid_nb=0
cid_nb=0
eid_nb=0

#____________________________________
def get_device_from_mount(mnt):
      
  for line in subprocess.check_output(['mount', '-l']).split('\n'):
    parts = line.split(' ')
    if len(parts) > 2:
      if parts[2] == mnt: return parts[0]
  return None    

#____________________________________
# Class host
#____________________________________
class host_class:
  def __init__(self, number, site):
    global hosts
    self.number = number
    self.addr=""
    self.addr += "192.168.%s.%s"%(10,self.number)
    for i in range(1,rozofs.nb_listen): self.addr += "/192.168.%s.%s"%(int(i+10),self.number)
    self.sid = []
    self.site = site
    self.admin = True
    hosts.append(self)

  @staticmethod    
  def get_host(val):
    global hosts
    for h in hosts:
      if h.number == val: return h
    return None
  def get_sid(self,val):
    for s in self.sid:
      if s.sid == val: return s
    return None  
    
  def add_sid(self,sid):
    self.sid.append(sid)

  def nb_sid(self): return len(self.sid)

  def set_admin_off(self):
    self.admin = False

  def set_admin_on(self):
    self.admin = True    

  def display(self):
    d = adaptative_tbl(2,"Hosts") 
    d.new_center_line()
    d.set_column(1,"#")
    d.set_column(2,"@")
    d.set_column(3,"Site")      
    d.set_column(4,"Vid")
    d.set_column(5,"Cid/Sid")
    d.end_separator()   
    for h in hosts:
      d.new_line()
      d.set_column(1,"%s"%(h.number))   
      d.set_column(2,"%s"%(h.addr))     
      d.set_column(3,"%s"%(h.site))  
      my_vols = []
      string=""
      for s in h.sid:
        string+="%s/%s "%(s.cid.cid,s.sid)
	if s.cid.volume not in my_vols: my_vols.append(s.cid.volume)
      d.set_column(5,"%s"%(string))
      string=""
      for v in my_vols: 
        string+="%s "%(v.vid)
      d.set_column(4,"%s"%(string))
    d.display()       

  def get_config_name(self): return "%s/storage_%s.conf"%(rozofs.get_config_path(),self.number)

  def create_config (self):
    save_stdout = sys.stdout
    sys.stdout = open(self.get_config_name(),"w")
    self.display_config()
    sys.stdout.close()
    sys.stdout = save_stdout

  def delete_config (self):
    try: os.remove(self.get_config_name())
    except: pass 
          
  def start(self):
    if self.admin == False: return
    self.add_if() 
    os.system("rozolauncher deamon /var/run/launcher_storaged_%s.pid storaged -c %s -H %s &"%(self.number,self.get_config_name(),self.addr))

  def stop(self):
    os.system("rozolauncher stop /var/run/launcher_storaged_%s.pid storaged"%(self.number))

  def del_if(self,nb=None):
    # Delete one interface
    if nb != None:
      cmd_silent("ip addr del 192.168.%s.%s/32 dev %s"%(int(nb)+10,self.number,rozofs.interface))  
      return
    # Delete all interfaces
    for i in range(rozofs.nb_listen): self.del_if(i)  

  def add_if(self,nb=None):
    # Add one interface
    if nb != None:
      cmd_silent("ip addr add 192.168.%s.%s/32 dev %s"%(int(nb)+10,self.number,rozofs.interface))
      return
    # Add all interfaces
    for i in range(rozofs.nb_listen): self.add_if(i)  
     
  def reset(self): 
    self.stop()
    self.start()

  def display_config(self):
    global rozofs    
    print "listen = ( "
    nextl=" "
    for i in range(rozofs.nb_listen):
      print "\t%s{addr = \"192.168.%s.%s\"; port = 41000;}"%(nextl,int(i+10),self.number)
      nextl=","
    print ");"
    nexts=" "
    print "storages = ("
    for s in self.sid:
      if rozofs.device_automount == True:
        if s.cid.volume.vid == 1 :
          print "\t%s{cid = %s; sid = %s; device-total = %s; device-mapper = %s; device-redundancy = %s; }"%(nexts,s.cid.cid,s.sid,s.cid.dev_total,s.cid.dev_mapper,s.cid.dev_red)      
        else:
          print "\t%s{cid = %s; sid = %s; device-total = %s; device-mapper = %s; device-redundancy = %s; spare-mark = \"%s\";}"%(nexts,s.cid.cid,s.sid,s.cid.dev_total,s.cid.dev_mapper,s.cid.dev_red,s.cid.volume.vid)                
      else:
        print "\t%s{cid = %s; sid = %s; root =\"%s\"; device-total = %s; device-mapper = %s; device-redundancy = %s; }"%(nexts,s.cid.cid,s.sid,s.get_root_path(self.number),s.cid.dev_total,s.cid.dev_mapper,s.cid.dev_red)
      nexts=","
    print "); "
       
  def process(self,opt):
    string="ps -fC rozolauncher"
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in cmd.stdout:
      if not "storaged" in line: continue
      if not "storage_%s.conf"%(self.number) in line: continue
      pid=line.split()[1]
      console("\n_______________STORAGE localhost%s"%(self.number))    
      os.system("pstree %s %s"%(opt,pid))
    return
    
  def rebuild(self,argv):  
    param=""
    rebef=False
    for i in range(4,len(argv)): 
      if argv[i] == "-id": rebef = True
      param += " %s"%(argv[i])
    if rebef == True:
      res=cmd_returncode("storage_rebuild --simu %s %s"%(exportd.get_config_name(),param))      
    else:  
      res=cmd_returncode("storage_rebuild --simu %s -c %s -H localhost%s %s"%(exportd.get_config_name(),self.get_config_name(),self.number,param))  
    sys.exit(res)
#____________________________________
# Class sid
#____________________________________
class sid_class:

  def __init__(self, cid, sid, site, host):  
    self.cid        = cid
    self.sid        = sid 
    self.host       = []   
    self.add_host(site,host)
 
  def add_host(self,site,name):
    if name == None: return
    h = host_class.get_host(name)
    if h == None: 
      h = host_class(name,site)
    else:
      if site != h.site:
        report("host localhost%s is used on site %s as well as site %s"%(h.number,h.site,site))
	sys.exit(1)
    self.host.append(h)
    h.add_sid(self)    
       
  def get_root_path(self,host_number):
    if rozofs.device_automount == True:
      return "/srv/rozofs/storages/storage_%s_%s"%(self.cid.cid,self.sid)
    else:   
      return "%s/storage_%s_%s_%s"%(rozofs.get_config_path(),host_number,self.cid.cid,self.sid)  

  def get_site_root_path(self,site):
    if len(self.host) < (int(site)+1): return None
    return self.get_root_path(self.host[site].number)  
    
  def create_path(self):

    if rozofs.device_automount == True: 
      for h in self.host:
        self.create_device("all",h)
        return          

    for h in self.host:    
      root_path=self.get_root_path(h.number)   
      try:os.mkdir(root_path)
      except: pass
      self.create_device("all",h)    

  def delete_path(self):
                
    for h in self.host:    

      try:self.delete_device("all",h)
      except: pass     

      if rozofs.device_automount == True:
        continue

      root_path=self.get_root_path(h.number)   
      try: shutil.rmtree(root_path)
      except: pass 
      

  def delete_device(self,device,h):
    if device == "all":
      for dev in range(self.cid.dev_total): self.delete_device(dev,h)
    else:
      self.delete_device_file(device,h)

      path=self.get_root_path(h.number)+"/%s" % (device)

      # Avoid to remove mark files if we use automount feature
      if rozofs.device_automount == True:
          subdirs = [ path + '/bins_0' , path + '/bins_1', path + "/hdr_0", path + "/hdr_1"]
          for dir in subdirs:
              try:
                shutil.rmtree(dir)
                log("%s deleted" % (dir))
              except:
                log("%s delete failed" % (dir))      
                pass
          return

      try: 
        shutil.rmtree(path)
        log("%s deleted"%(path))      
      except: 
        log("%s delete failed"%(path))      
        pass 
    
  def get_device_file_path(self,site): 
#    if len(self.host) < (int(site)+1): return None  
    return "%s/devices/site%d/cid%s/sid%s/"%(rozofs.get_config_path(),site,self.cid.cid,self.sid)
 

  def mount_device_file(self,dev,h):
    
    if rozofs.disk_size_mb == None: return 
    if rozofs.device_automount == True: return
    
    if dev == "all":
      for dev in range(self.cid.dev_total): self.mount_device_file(dev,h)
      return
      
    path=self.get_device_file_path(h.site)   
    os.system("touch %s/%s/X"%(self.get_root_path(h.number),dev))
       
    string="losetup -j %s%s "%(path,dev)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = cmd.communicate()
    if output != "":
      loop=output.split(':')[0]  
      log("%s%s \t-> %s \t-> %s/%s"%(path,dev,loop,self.get_root_path(h.number),dev))
      if rozofs.fstype == "ext4":
        os.system("mount -t ext4 %s %s/%s"%(loop,self.get_root_path(h.number),dev))
      else:
        if rozofs.allocsize == None:
          os.system("mount -t xfs %s %s/%s"%(loop,self.get_root_path(h.number),dev))      		
	else:
          os.system("mount -t xfs -o allocsize=%s %s %s/%s"%(rozofs.allocsize,loop,self.get_root_path(h.number),dev))      	
    else:
      report("No /dev/loop for %s%s"%(path,dev))  
    return
     	  
  def umount_device_file(self,dev, h):
    cmd_silent("umount %s/%s"%(self.get_root_path(h.number),dev))
      
  def create_device_file(self,device,h):
  
    if rozofs.disk_size_mb == None: return
        
    if device == "all":
      for dev in range(self.cid.dev_total): self.create_device_file(dev,h)
      return          
	  
    path=self.get_device_file_path(h.site) 
    try: os.makedirs(path)
    except: pass 
    
    path="%s/%s"%(path,device)
    mark="storage_c%s_s%s_%s"%(self.cid.cid,self.sid,device)
    
    if os.path.exists(path): return
    rozofs.create_loopback_device(path,mark)

  def delete_device_file(self,device,h):

    if rozofs.disk_size_mb == None: return

    if device == "all":
      for dev in range(self.cid.dev_total): self.delete_device_file(dev,h)
      return
      
    mnt="%s/%s"%(self.get_root_path(h.number),device)
    dev = get_device_from_mount(mnt)
    
    self.umount_device_file(device,h)      
    if dev != None: rozofs.delete_loopback_device(dev)
          
  def create_device(self,device,h):
    if device == "all":
      for dev in range(self.cid.dev_total): self.create_device(dev,h)
      return
             
    self.create_device_file(device,h)
    if rozofs.device_automount == True: return

    path=self.get_root_path(h.number)+"/%s"%(device)   
    try: os.makedirs(path)
    except: pass 
    self.mount_device_file(device,h)
     
  def clear_device(self,device,h):
    if device == "all":
      for dev in range(self.cid.dev_total): self.clear_device(dev,h)
      return
      
    mnt="%s/%s"%(self.get_root_path(h.number),device)
    os.system("rm -rf %s/bins*"%mnt)            
    os.system("rm -rf %s/hdr*"%mnt)            
      
        
  def rebuild(self,argv):
    param=""
    rebef=False
    for i in range(5,len(argv)): 
      if argv[i] == "-id": rebef = True
      param += " %s"%(argv[i])
    if rebef == True:
      res=cmd_returncode("storage_rebuild --simu %s %s"%(exportd.get_config_name(),param))      
    else: 
      h = self.host[0]   
      res=cmd_returncode("storage_rebuild --simu %s -c %s -H localhost%s -s %d/%d %s"%(exportd.get_config_name(),h.get_config_name(),h.number,self.cid.cid,self.sid,param))  
    sys.exit(res)
                   
  def info(self):
    print "cid = %s"%(self.cid.cid)
    print "sid = %s"%(self.sid)
    print "site0 = %s"%(self.host[0].number)
    print "siteNum = %s"%(self.host[0].site)
    print "@site0 = %s"%(self.host[0].addr)
    print "path0 = %s"%(self.get_root_path(self.host[0].number))
    if len(self.host) > 1:
      print "site1 = %s"%(self.host[1].number)
      print "siteNum = %s"%(self.host[1].site)    
      print "@site1 = %s"%(self.host[1].addr)      
      print "path1 = %s"%(self.get_root_path(self.host[1].number))
#____________________________________
# Class cid
#____________________________________
def get_cid(cid):
  global cids 
  for c in cids:
    if c.cid == cid: return c
  return None  

class cid_class:

  def __init__(self, volume, dev_total, dev_mapper, dev_red, dev_size):
    global cids 
    global cid_nb
    cid_nb+=1
    self.cid        = cid_nb
    self.sid        = []
    self.dev_total  = dev_total
    self.dev_mapper = dev_mapper
    self.dev_red    = dev_red 
    self.volume     = volume
    self.georep     = None 
    self.dev_size   = 0;
    cids.append(self) 
    
  @staticmethod
  def get_cid(val):
    global cids
    for c in cids:
      if c.cid == val: return c
    return None  
   
  def get_sid(self,val):
    for s in self.sid:
      if s.sid == val: return s
    return None  
    
  def set_georep(self,georep):
    if self.georep == None:  self.georep = georep
    else:
      if self.georep != georep:
        report( "gereplication inconsistency on cid %s"%(self.cid))
	sys.exit(1)
	    
  def add_sid_on_host(self, host0, site0=0, host1=None, site1=1):
    sid=len(self.sid)
    sid+=1  
    s = sid_class(self, sid, site0, host0)
    # For geo repliction
    if host1 != None:
      s.add_host(site1,host1) 
    self.sid.append(s)
    if host1 == None: self.set_georep(False)
    else            : self.set_georep(True)    	
    return s   
  
  def create_path(self):
    for s in self.sid: s.create_path()     

  def delete_path(self):
    for s in self.sid: s.delete_path()     

  def nb_sid(self): return len(self.sid)
  



#____________________________________
# Class mount
#____________________________________
class mount_point_class:

  def __init__(self, eid, layout, site=0):
    global mount_points
    instance = len(mount_points)
    self.numanode=instance+1
    # When more than 2 storcli, use one rozofsmount instance upon 2
    if rozofs.nb_storcli > 2 : instance = 2 * instance       
    self.instance = instance
    self.eid = eid
    self.site= site    
    self.layout = layout
    self.nfs_path="/mnt/nfs-%s"%(self.instance) 
    # Timers
    if rozofs.client_fast_reconnect != 0:
      self.set_spare_tmr_ms(rozofs.client_fast_reconnect*1000/2)
      self.rozofsexporttimeout  = rozofs.client_fast_reconnect
      self.rozofsstoragetimeout = rozofs.client_fast_reconnect
      self.rozofsstorclitimeout = rozofs.client_fast_reconnect
    else:
      self.set_spare_tmr_ms(6000)
      self.rozofsexporttimeout  = None
      self.rozofsstoragetimeout = None
      self.rozofsstorclitimeout = None
      self.spare_tmr_ms         = 6000
    mount_points.append(self)

  def set_fast_reconnect(self):  

        
    rozofs.set_client_fast_reconnect()
    
  def set_spare_tmr_ms(self,tmr): self.spare_tmr_ms=tmr

  def info(self):
    print "instance = %s"%(self.instance)
    print "eid = %s"%(self.eid.eid)
    print "vid = %s"%(self.eid.volume.vid)
    print "site = %s"%(self.site)
    print "path = %s"%(self.get_mount_path())
    l=self.layout.split('_')
    print "layout = %s %s %s"%(l[1],l[2],l[3])
    print "failures = %s"%(self.eid.failures)
    list=[]
    string=""
    for h in hosts:
      for s in h.sid:
        if s.cid.volume.vid == self.eid.volume.vid:
	  if self.eid.volume.georep() == True:
	    if h.site == self.site:
	      if not h in list:
		list.append(h)
		string += " %s"%(h.number)
	  else:
              if not h in list:
		list.append(h)
		string += " %s"%(h.number)	    	
    print "hosts = %s"%(string)	        
    list=[]
    string=""
    for h in hosts:
      for s in h.sid:
        if s.cid.volume.vid == self.eid.volume.vid:
	  if self.eid.volume.georep() == True:	
	    if h.site == self.site:
	      string += " %s-%s-%s"%(h.number,s.cid.cid,s.sid)
	  else:
	      string += " %s-%s-%s"%(h.number,s.cid.cid,s.sid)   
    print "sids = %s"%(string)	    

    string="ps -o pid=,cmd= -C rozofsmount"
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in cmd.stdout:
      if not "instance" in line:
        if int(self.instance) == int(0):
	  pid=line.split()[0]
	  break	
	else:
	  continue
      else:	    
        if "instance=%s"%(self.instance) in line: 
	  pid=line.split()[0]
	  break	
    try:
      print "pid = %s"%(pid)
    except:
      pass          
    return   
    
  def get_mount_path(self):
    return "%s/mnt%s_eid%s_site%s"%(rozofs.get_config_path(),self.instance,self.eid.eid,self.site)
    
  def create_path(self):
    global rozofs
    try:
      os.mkdir(self.get_mount_path())
    except: pass
    
  def delete_path(self):
    try: shutil.rmtree(self.get_mount_path())
    except: pass 
               
  def start(self):
    global rozofs
    options=""
    options += " -o rozofsnbstorcli=%s"%(rozofs.nb_storcli)
    options += " -o rozofssparestoragems=%s"%(self.spare_tmr_ms)
    if self.rozofsexporttimeout != None: 
      options += " -o rozofsexporttimeout=%s"%(self.rozofsexporttimeout)
    if self.rozofsstoragetimeout != None: 
      options += " -o rozofsstoragetimeout=%s"%(self.rozofsstoragetimeout)
    if self.rozofsstorclitimeout != None: 
      options += " -o rozofsstorclitimeout=%s"%(self.rozofsstorclitimeout)      
    options += " -o auto_unmount"
    options += " -o suid"
    options += " -o numanode=%s"%(self.numanode)
    options += " -o site=%s"%(self.site)
    if self.instance != 0: options += " -o instance=%s"%(self.instance)
    if rozofs.read_mojette_threads == True: options += " -o mojThreadRead=1"
    if rozofs.write_mojette_threads == False: options += " -o mojThreadWrite=0"
    if rozofs.mojette_threads_threshold != None: options += " -o mojThreadThreshold=%s"%(rozofs.mojette_threads_threshold)

    os.system("rozofsmount -H %s -E %s %s %s"%(exportd.export_host,self.eid.get_name(),self.get_mount_path(),options))
    os.system("chmod 0777 %s"%(self.get_mount_path()))
          
  def stop(self):
    try: self.nfs(False)
    except: pass
    if os.path.exists(self.get_mount_path()):
      if os.path.ismount(self.get_mount_path()):
        os.system("umount %s"%(self.get_mount_path()))
        if os.path.ismount(self.get_mount_path()): 
          os.system("umount -l %s"%(self.get_mount_path()))
     
  def reset(self): 
    self.stop()
    self.start()

  def nfs_add_mount_path(self):
    with open('/etc/exports', 'r') as exp_file:
      for line in exp_file.readlines():
        path = line.split()[0]
	if path == self.get_mount_path(): return
    with open('/etc/exports', 'a') as exp_file :	
      exp_file.write("%s *(rw,no_root_squash,fsid=1,no_subtree_check)\n"%(self.get_mount_path()))
                 
  def nfs_server(self,status):   
    if status == "check":   
      # NFS server must be running
      string="/etc/init.d/nfs-kernel-server status"
      parsed = shlex.split(string)
      cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      for line in cmd.stdout:
	if "nfsd running" in line: 
          return "on"
      return "off"
      
    if status == "on":
      self.nfs_server("off")
      self.nfs_add_mount_path()
      os.system("/etc/init.d/nfs-kernel-server start")
      return
      
    if status == "off":
      os.system("/etc/init.d/nfs-kernel-server stop")  
      return
          
  def nfs(self,status):      
    if status == True: 
      self.nfs_server("on")
      if not os.path.exists(self.nfs_path):  
	try:os.mkdir(self.nfs_path)
	except: pass 
      if os.path.ismount(self.nfs_path): os.system("umount %s"%(self.nfs_path))
      os.system("mount 127.0.0.1:%s %s"%(self.get_mount_path(),self.nfs_path))
    else:
      if os.path.ismount(self.nfs_path): os.system("umount %s"%(self.nfs_path))
           
  def display(self):   
    d = adaptative_tbl(2,"Mount points") 
    d.new_center_line()
    d.set_column(1,"Instance")
    d.set_column(2,"Volume")      
    d.set_column(3,"Export")
    d.set_column(4,"layout")
    d.set_column(5,"Block")
    d.set_column(6,"Site") 
    d.set_column(7,"Mount path") 
    d.new_center_line()
    d.set_column(2,"id")      
    d.set_column(3,"id")
    d.set_column(5,"size")
    d.set_column(6,"number") 
    d.end_separator()    
    for m in mount_points:
      d.new_line()
      d.set_column(1,"%s"%(m.instance))
      d.set_column(2,"%s"%(m.eid.volume.vid))      
      d.set_column(3,"%s"%(m.eid.eid))
      d.set_column(4,"%s"%(m.layout))
      d.set_column(5,"%s"%(rozofs.bsize(m.eid.bsize))) 
      d.set_column(6,"%s"%(m.site))   
      d.set_column(7,"%s"%(m.get_mount_path())) 
    d.display()
    
  def process(self,opt):
    string="ps -fC rozofsmount"
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in cmd.stdout:
      if not "instance=%s"%(self.instance) in line: continue
      pid=line.split()[1]
      console("\n_______________FS %s eid %s vid %s %s"%(self.instance,self.eid.eid,self.eid.volume.vid,self.get_mount_path()))     
      os.system("pstree %s %s"%(opt,pid))
    return    
    
  def process(self,opt):
    string="ps -fC rozofsmount"
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in cmd.stdout:
      if not "instance=%s"%(self.instance) in line: continue
      pid=line.split()[1]
      print "\n_______________FS %s eid %s vid %s %s"%(self.instance,self.eid.eid,self.eid.volume.vid,self.get_mount_path())     
      os.system("pstree %s %s"%(opt,pid))
    return        
#____________________________________
# Class export
#____________________________________
class export_class:

  def __init__(self, bsize, volume,layout=None):
    global eid_nb
    eid_nb += 1
    self.eid   = eid_nb
    self.bsize = bsize
    self.volume= volume
    self.thin = False
    if layout == None:
      self.layout = volume.layout
    else:
      self.layout = rozofs.layout(layout)
    self.hquota= ""
    self.squota= "" 
    self.mount =[]
    if int(rozofs.failures(layout)) <=  int(volume.get_failures()): 
      self.failures = rozofs.failures(layout)   
    else:
      self.failures = volume.get_failures()
    
  def set_hquota(self,quota):
    self.hquota= quota
    
  def set_thin(self):
    self.thin = True
    
  def set_squota(self,quota):
    self.squota= quota            

  def get_root_path(self):
    return "%s/export/export_%s"%(rozofs.get_config_path(),self.eid)  

  def get_name(self):
    return "eid%s"%(self.eid)  
     
  def add_mount(self,site=0):
    m = mount_point_class(self,self.layout,site)
    self.mount.append(m)
  
  def create_path(self):  
    try:os.mkdir(self.get_root_path())
    except: pass 
    for m in self.mount: m.create_path()   

  def delete_path(self):
    for m in self.mount: m.delete_path()   
    try: shutil.rmtree(self.get_root_path())
    except: pass 

  def nb_mount_point(self): return len(self.mount)
  	          
  def display(self):
    for m in self.mount: m.display()
#____________________________________
# Class volume
#____________________________________
class volume_class:

  def __init__(self,layout):
    global vid_nb
    global rozofs
    vid_nb+=1
    self.vid        = vid_nb
    self.cid        = [] 
    self.eid        = []  
    self.layout     = layout
    volumes.append(self)
    self.set_failures(rozofs.failures(layout))

  def set_failures(self, failures): self.failures = failures
  def get_failures(self): return self.failures
    
  def add_cid(self, dev_total, dev_mapper, dev_red, dev_size=0):
    c = cid_class(self,dev_total, dev_mapper, dev_red, dev_size)
    self.cid.append(c)
    return c
    
  def georep(self,):
    georep = None
    for c in self.cid:
      if georep == None: georep = c.georep
      elif c.georep != georep:
	  report("inconsistent georeplication on volume %s"%(v.vid))
	  exit(1)
    return georep
     
  def add_export(self, bsize,layout=None):
    e = export_class(bsize,self,layout)
    self.eid.append(e)
    return e

  def create_path(self):  
    for c in self.cid: c.create_path()
    for e in self.eid: e.create_path()    

  def delete_path(self):
    for c in self.cid: c.delete_path()
    for e in self.eid: e.delete_path()    

  def nb_cid(self): return len(self.cid)
  def nb_eid(self): return len(self.eid)

  def get_rebalance_config_name(self): return "%s/rebalance_vol%d.conf"%(rozofs.get_config_path(),self.vid)
                              
  def display_rebalance_config(self):        
    display_config_int("free_avg_tolerance",5)
    display_config_int("free_low_threshold",70)
    display_config_int("frequency",25)
    display_config_int("movecnt",12)
    display_config_string("movesz","100M")
    display_config_long("older",1)
    display_config_int("throughput",2)
    

  def create_rebalance_config(self):
    save_stdout = sys.stdout
    sys.stdout = open(self.get_rebalance_config_name(),"w")
    self.display_rebalance_config()
    sys.stdout.close()
    sys.stdout = save_stdout      

  def display(self):
    d = adaptative_tbl(2,"Volumes") 
    d.new_center_line()
    d.set_column(1,"Vid")
    d.set_column(2,"Cid")      
    d.set_column(3,"Export")
    d.set_column(4,"layout")
    d.end_separator()    
    d.new_line()
    for v in volumes:
      d.set_column(1,"%s"%(v.vid))
      string=""
      for c in v.cid: string += "%s "%(c.cid)
      d.set_column(2,"%s"%(string))
      string=""
      for e in v.eid: string += "%s "%(e.eid)
      d.set_column(3,"%s"%(string))
      d.set_column(4,"%s"%(rozofs.layout(v.layout)))
    d.display()

#____________________________________
# Class rozo_fs
#____________________________________
class exportd_class:

  def __init__(self,hosts="localhost"):
    self.export_host=hosts  

  def get_config_name(self): return "%s/export.conf"%(rozofs.get_config_path())

  def delete_config(self):
    try: os.remove(self.get_config_name())
    except: pass     
        
  def create_config (self):
    save_stdout = sys.stdout
    sys.stdout = open(self.get_config_name(),"w")
    self.display_config()
    sys.stdout.close()
    sys.stdout = save_stdout
#    for v in volumes: v.create_rebalance_config()
       
  def pid(self):
    string="ps -fC exportd"
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    pid=0
    for line in cmd.stdout:
      if not "exportd" in line: continue
      if not "-i" in line: pid=line.split()[1]
    return pid
           
  def start(self):
    pid=self.pid()
    if pid != 0: 
      report("exportd is already started as process %s"%(pid))
      return
    os.system("exportd -c %s"%(self.get_config_name()))    
    
  def stop(self):
    pid=self.pid()
    if pid == 0: return
    os.system("kill %s"%(pid))
    pid=self.pid()
    if pid == 0: return
    os.system("kill -9 %s"%(pid))    

  def reset(self):
    pid=self.pid()
    if pid == 0: self.start_exportd()
    os.system("kill -HUP %s"%(pid))

  def reload(self):
    pid=self.pid()
    os.system("kill -1 %s"%(pid))
    
  def process(self,opt):
    pid = self.pid()
    if pid != 0: 
      console("\n_______________EXPORTD" )  
      os.system("pstree %s %s"%(opt,pid))
     
  def display_config (self):  
    global volumes
    print "layout = 1;"
    print "volumes ="
    print "("
    nextv=" "
    for v in volumes:
      print "  %s{"%(nextv)
      nextv=","
      print "    vid = %s;"%(v.vid)
      print "    layout = %s;"%(v.layout)
      print "    georep = %s;"%(v.georep())
#      print "    rebalance = \"%s\";"%(v.get_rebalance_config_name())
      print "    cids = "
      print "    ("
      nextc=" "
      for c in v.cid:
	print "     %s{"%(nextc)
	nextc=","      
	print "        cid = %s;"%(c.cid)
	print "        sids = "
	print "        ("
	nexts=" "
	for s in c.sid:
	  if len(s.host) == 1 :
	    if rozofs.site_number == 1:
  	      print "          %s{sid=%s; host=\"%s\";}"%(nexts,s.sid,s.host[0].addr)	    
	    else:
  	      print "          %s{sid=%s; host=\"%s\"; site=%s;}"%(nexts,s.sid,s.host[0].addr,s.host[0].site)
	  else:
	    print "          %s{sid=%s; site0=\"%s\"; site1=\"%s\";}"%(nexts,s.sid,s.host[0].addr,s.host[1].addr)
	  nexts=","
	print "        );"
	print "      }"    
      print "    );"
      print "  }"
    print ");"
    print "filters ="
    print "("
    nexte=" "
    for v in volumes:
      for e in v.eid: 
        print " %s{"%(nexte)
	nexte=","                   
        print "    filter = \"flt_%d\","%(e.eid)
        print "    rule   = \"forbid\","
        print "    subnets ="
        print "    ("
        print "      { ip4subnet=\"127.0.0.0/24\",     rule=\"allow\"},"
        print "      { ip4subnet=\"127.0.0.0/28\",     rule=\"forbid\"},"
        print "      { ip4subnet=\"127.0.0.16/28\",    rule=\"forbid\"},"
        print "      { ip4subnet=\"127.0.0.1/32\",     rule=\"allow\"},"
        print "      { ip4subnet=\"127.0.0.17/32\",    rule=\"allow\"},"
        print "      { ip4subnet=\"192.168.10.0/24\",  rule=\"allow\"}"
        print "    );"
        print "  }"
    print ");"
    
    print "exports ="
    print "("
    nexte=" "
    for v in volumes:
      for e in v.eid:
        root_path=e.get_root_path()
        if e.squota == "": squota=""
        else             : squota="squota=\"%s\";"%(e.squota)
        if e.hquota == "": hquota=""
        else             : hquota="hquota=\"%s\";"%(e.hquota)
        if e.thin == True: thin = ", thin-provisionning = True"
        else             : thin=""
	print "  %s{eid=%s; bsize=\"%s\"; root=\"%s\"; name=\"%s\", filter=\"flt_%d\"; %s%s vid=%s; layout=%s %s}"%(nexte,e.eid,rozofs.bsize(e.bsize),root_path,e.get_name(),e.eid,hquota,squota,v.vid,rozofs.layout2int(e.layout),thin)
	nexte=","	
    print ");"

  def display(self): 
    console("EXPORTD:")
    console("  . %-12s : %s"%("Hosts",self.export_host))    

#____________________________________
# Class geomgr_class
#____________________________________
class geomgr_class:

  def display(self):
    console("- %s"%(self.number))
    for s in self.sid: console("    cid %s sid %s"%(s.cid.cid,s.sid))

  def get_config_name(self): return "%s/geomgr.conf"%(rozofs.get_config_path())
  def get_saved_config_name(self): return "%s/geomgr.conf"%(os.getcwd())

  def create_config (self):
    save_stdout = sys.stdout
    sys.stdout = open(self.get_config_name(),"w")
    self.initial_config()
    sys.stdout.close()
    sys.stdout = save_stdout

  def delete_config (self):
    try: os.remove(self.get_config_name())
    except: pass    

  def modify(self):
    os.system("nedit %s"%(self.get_config_name()))
      
  def reinit(self): 
    self.delete_config()
    self.create_config()
    
  def  display_config(self):
    os.system("cat %s"%(self.get_config_name()))
    return
  
  def  initial_config(self):
    print "active = True ;"
    print "export-daemons = (" 
    print "   {" 
    print "	active = True;" 
    print "	host   = \"%s\";"%(exportd.export_host)
    print "	exports="   
    print "	("  
    nexte=" " 
    for v in volumes:
      if v.georep() != True: continue
      for e in v.eid:
	print "         %s{"%(nexte)
	nexte="," 
	print "               active = True;"  
	print "               path   = \"%s\";"%(e.get_root_path()) 
	print "               site   = 1;" 
	print "               nb     = 1;" 
	print "          }," 
	print "          {" 
	print "               active = True;"  
	print "               path   = \"%s\";"%(e.get_root_path())  
	print "               site   = 0;" 
	print "               nb     = 1;" 
	print "               calendar =" 
	print "		   (" 
	print "		     { start=\"8:00\"; stop=\"12:15\";  },"
	print "		     { start=\"14:15\"; stop=\"17:30\"; }"
	print "		   );"
	print "          }" 
    print "	);"   
    print "   }" 
    print ');' 

          
  def start(self):
    os.system("rozolauncher start /var/run/launcher_geomgr.pid geomgr -c %s -t 5 &"%(self.get_config_name()))

  def stop(self):
    os.system("rozolauncher stop /var/run/launcher_geomgr.pid geomgr")
     
  def reset(self): 
    self.stop()
    self.start()
       
  def process(self,opt):
    string="ps -fC rozolauncher"
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in cmd.stdout:
      if not "geomgr" in line: continue
      pid=line.split()[1]
      console("\n_______________GEOMGR")
      os.system("pstree %s %s"%(opt,pid))
    return
  
def display_config_string(name,val):
  if val != None: print "%-27s = \"%s\";"%(name,val)
    
def display_config_int(name,val):        
  if val != None: print "%-27s = %d;"%(name,int(val))

def display_config_long(name,val):        
  if val != None: print "%-27s = %ld;"%(name,long(val))

def display_config_true(name): print "%-27s = True;"%(name)
  
def display_config_false(name): print "%-27s = False;"%(name)  
    
def display_config_bool(name,val):
  if val == True:  display_config_true(name)      
  else:            display_config_false(name) 
     
 
#____________________________________
# Class rozo_fs
#____________________________________
class rozofs_class:

  def __init__(self):
    self.threads = 0
    self.nb_core_file = 8
    self.crc32 = True
    self.device_selfhealing_mode  = "relocate"
    self.device_selfhealing_delay = 1
    self.nb_listen=1;
    self.storio_mode="multiple";
    self.interface = "lo"
    self.read_mojette_threads = False
    self.write_mojette_threads = True
    self.mojette_threads_threshold = None
    self.nb_storcli = 1
    self.disk_size_mb = None
    self.trace = False
    self.storio_slice = 8
    self.spin_down_allowed = False
    self.file_distribution = 1
    self.fid_recycle = False
    self.trash_threshold = 10
    self.alloc_mb = None
    self.storaged_start_script = None
    self.device_automount = False
    self.site_number = 1
    self.client_fast_reconnect = 0
    self.deletion_delay = None
    self.trashed_file_per_run = 100
    self.spare_restore_loop_delay = 15
    self.metadata_size = None;
    self.min_metadata_inodes = None
    self.min_metadata_MB = None
    self.mkfscmd = None
    
  def set_min_metadata_inodes(self,val): self.min_metadata_inodes = val
  def set_min_metadata_MB(self,val): self.min_metadata_MB = val
  def set_metadata_size(self,size): self.metadata_size = size;
  def set_set_spare_restore_loop_delay(self,number): self.spare_restore_loop_delay = number  
  def set_site_number(self,number): self.site_number    
  def set_device_automount(self): 
    self.device_automount = True
    self.device_selfhealing_mode = "spareOnly"
  def set_storaged_start_script(self,storaged_start_script):
    self.storaged_start_script = storaged_start_script
  def set_alloc_mb(self,alloc_mb): self.alloc_mb = alloc_mb    
  def set_fid_recycle(self,threshold=10): 
    self.fid_recycle = True 
    self.trash_threshold = threshold     
  def allow_disk_spin_down(self): self.spin_down_allowed = True    
  def set_trace(self): self.trace = True
  def storio_mode_single(self):self.storio_mode = "single"  
  def set_storio_slice(self,sl):self.storio_slice = sl 
  def set_nb_listen(self,nb_listen):self.nb_listen = nb_listen  
  def set_nb_core_file(self,nb_core_file):self.nb_core_file = nb_core_file     
  def set_threads(self,threads):self.threads = threads  
  def set_self_healing(self,delay,mode="spareOnly"):
    self.device_selfhealing_delay = delay
    self.device_selfhealing_mode  = mode      
  def set_crc32(self,crc32):self.crc32 = crc32  
  def enable_read_mojette_threads(self): self.read_mojette_threads = True
  def disable_write_mojette_threads(self): self.read_mojette_threads = False
  def set_mojette_threads_threshold(self,threshold): self.mojette_threads_threshold = threshold
  def set_nb_storcli(self,nb=1): 
    self.nb_storcli = nb
    # Must have enough share memry size
    new = int(13107200) * int(nb)
    with open("/proc/sys/kernel/shmmax") as f: val=f.readlines()
    if int(val[0]) < int(new): os.system("echo %s > /proc/sys/kernel/shmmax"%(new))  
  def set_file_distribution(self,val): self.file_distribution = val
  def set_client_fast_reconnect(self,val=2): 
    self.client_fast_reconnect = val

  def set_mkfscmd(self,cmd):
    self.mkfscmd = cmd    
    
  def set_xfs(self,mb,allocsize=None):
    self.fstype       = "xfs"
    self.disk_size_mb = mb
    self.allocsize    = allocsize
    self.set_mkfscmd("mkfs.xfs -f -q ")      

  def set_deletion_delay(self,deletion_delay): self.deletion_delay = deletion_delay; 
    
  def set_ext4(self,mb):
    self.fstype = "ext4"
    self.disk_size_mb = mb
    self.set_device_automount()
    self.set_mkfscmd("mkfs.ext4 -b 4096 -m 0 -q ")      

  def get_config_path(self):
    path = "%s/SIMU"%(os.getcwd())
    if not os.path.exists(path): 
      os.makedirs(path)
    if not os.path.exists("%s/export"%(path)):  
      os.makedirs("%s/export"%(path))
    if not os.path.exists("%s/devices"%(path)):    
      os.makedirs("%s/devices"%(path))      
    return path
    
  def core_dir(self)        :
    if not os.path.exists("/var/run/rozofs"): 
      os.mkdir("/var/run/rozofs") 
    if not os.path.exists("/var/run/rozofs/core"):  
      os.mkdir("/var/run/rozofs/core") 
    return "/var/run/rozofs/core"  
  def layout_2_3_4(self)    : return 0
  def layout_4_6_8(self)    : return 1
  def layout_8_12_16(self)  : return 2
  def layout_4_6_9(self)    : return 3 
  def layout(self,val):
    if val == 0: return "layout_2_3_4"
    if val == 1: return "layout_4_6_8"
    if val == 2: return "layout_8_12_16"
    if val == 3: return "layout_4_6_9"
  def layout2int(self,val):
    if val == "layout_2_3_4": return 0
    if val == "layout_4_6_8": return 1
    if val == "layout_8_12_16": return 2
   
  def min_sid(self,val):
    if val == 0: return 4
    if val == 1: return 8
    if val == 2: return 16
    if val == 3: return 9

  def failures(self,val): 
    if val == 1: return 2
    if val == 2: return 4
    return 1
       
  def bsize4K(self)    : return 0
  def bsize8K(self)    : return 1
  def bsize16K(self)   : return 2
  def bsize32K(self)   : return 3 
  def bsize(self,val):
    if val == 0: return "4K"
    if val == 1: return "8K"
    if val == 2: return "16K"
    if val == 3: return "32K"

  def create_loopback_device(self,path,mark,content=None):  
    if rozofs.disk_size_mb == None: return
    
    # Need a working directory
    tmpdir="/tmp/setup"
    os.system("umount -f %s  > /dev/null 2>&1"%(tmpdir))
    os.system("mkdir -p %s "%(tmpdir))

    # Find out a free loop back device to map on it
    string="losetup -f "
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = cmd.communicate()
    if len(output) < 1:
      log( "Can not find /dev/loop for %s %s"%(path,mark))
      return
    loop=output[0].split('\n')[0]
    if content == None:
      report("%sMB %-12s %s %s"%(rozofs.disk_size_mb,loop,path,mark))
    else:
      report("%sMB %-12s %s %s(%s)"%(rozofs.disk_size_mb,loop,path,mark,content))
      
    # Create the file with the given path
    os.system("truncate -s %s %s 2>&1"%(rozofs.disk_size_mb*1024*1024, path))
    
    # Bind the loop back device to the file    
    string="losetup %s %s "%(loop,path)
    os.system(string)
    
    # Format it and mount it on the working directory
    os.system("%s %s"%(self.mkfscmd,loop))
    os.system("mount %s %s"%(loop,tmpdir))

    # Create the mark file  
    if content == None:     		
      os.system("touch %s/%s"%(tmpdir,mark))
    else:
      os.system("echo %s > %s/%s"%(content,tmpdir,mark))      
    time.sleep(2)
    # Umount the temporary directory
    os.system("umount %s"%(tmpdir))	  
    os.system("umount -f %s  > /dev/null 2>&1"%(tmpdir))
    syslog.syslog("Created %s -> %s -> %s"%(path,loop,mark))	  
    return  	 

  def create_export_loopback_device(self,path,mount,sizeMB):  
    if sizeMB == None: return
    
    # Need a working directory
    os.system("umount -f %s  > /dev/null 2>&1"%(mount))
    os.system("mkdir -p %s "%(mount))

    # Find out a free loop back device to map on it
    string="losetup -f "
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = cmd.communicate()
    if len(output) < 1:
      log( "Can not find /dev/loop for %s"%(path))
      return
    loop=output[0].split('\n')[0]
    report("%sMB %-12s %s -> %s"%(sizeMB,loop,path,mount))
      
    # Create the file with the given path
    os.system("dd if=/dev/zero of=%s bs=1MB count=%s > /dev/null 2>&1"%(path,sizeMB))
    
    # Bind the loop back device to the file    
    string="losetup %s %s "%(loop,path)
    os.system(string)
    
    # Format it and mount it on the working directory
    os.system("mkfs.ext4 -m 0 -q %s"%(loop))
    os.system("mount -t ext4 %s %s"%(loop,mount))
    syslog.syslog("Created %s -> %s -> %s"%(path,loop,mount))
    time.sleep(2)	  
    return  	     

    
  def delete_loopback_device(self,path):  
    devFile = ""
    string="losetup %s"%(path)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = cmd.communicate()
    if output != "":
      parts=output.split()
      if len(parts) > 2:
        devFile=parts[2]
    if devFile== "": return
    try:
      string="losetup -d %s"%(path)
      parsed = shlex.split(string)
      for i in range (10):
        cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = cmd.communicate()
	if error == "": break
	time.sleep(1)
      devFile = devFile.replace('(','')
      devFile = devFile.replace(')','')   
      os.remove(devFile)
      log("%s Deleted -> %s"%(path,devFile))	
    except: pass 
  
  def newspare(self,mark=None):
    # Find a free spare number
    for idx in range(0,4096):
      path="%s/devices/spare%s"%(self.get_config_path(),idx)
      if not os.path.exists(path):
        rozofs.create_loopback_device(path,"rozofs_spare",mark)            
        return
    report("No free spare number for spare device file")
               
  def display_common_config(self):        
    display_config_int("nb_disk_thread",rozofs.threads)
    display_config_int("nb_core_file",rozofs.nb_core_file)
    display_config_bool("crc32c_check",rozofs.crc32)
    display_config_bool("crc32c_generate",rozofs.crc32)
    display_config_true("crc32c_hw_forced")
    display_config_int("storio_slice_number",rozofs.storio_slice)
    display_config_bool("numa_aware",True)
    if self.storio_mode == "multiple": display_config_true("storio_multiple_mode")
    else:                              display_config_false("storio_multiple_mode")
    display_config_bool("allow_disk_spin_down", rozofs.spin_down_allowed)
    display_config_int("file_distribution_rule",self.file_distribution)
    if self.fid_recycle == True: 
      display_config_true("fid_recycle")
      display_config_int("trash_high_threshold",self.trash_threshold)
    display_config_int("alloc_estimated_mb",self.alloc_mb)
    display_config_string("storaged_start_script",self.storaged_start_script)
    display_config_bool("device_automount",self.device_automount)
    #display_config_int("device_self_healing_process",2)
    display_config_int("device_selfhealing_delay",rozofs.device_selfhealing_delay)
    display_config_string("device_selfhealing_mode",rozofs.device_selfhealing_mode)
    display_config_string("export_hosts",exportd.export_host)
    display_config_bool("client_xattr_cache",True)
    display_config_bool("async_setattr",True)
    if self.deletion_delay != None :
      display_config_int("deletion_delay",self.deletion_delay)
    if self.client_fast_reconnect != 0: display_config_bool("client_fast_reconnect",True)
    display_config_int("storio_buf_cnt",32)
    display_config_int("export_buf_cnt",32)
    display_config_int("spare_restore_spare_ctx",1)
    display_config_int("spare_restore_loop_delay",rozofs.spare_restore_loop_delay);
    display_config_int("storio_fidctx_ctx",1)   
    display_config_int("trashed_file_per_run",rozofs.trashed_file_per_run)
    if rozofs.min_metadata_inodes != None:
      display_config_int("min_metadata_inodes",rozofs.min_metadata_inodes)
    if rozofs.min_metadata_MB != None:
      display_config_int("min_metadata_MB",rozofs.min_metadata_MB)
    display_config_int("nb_trash_thread",8)
    
  def create_common_config(self):
    if not os.path.exists("/usr/local/etc/rozofs"): os.system("mkdir -p /usr/local/etc/rozofs")  
    if not os.path.exists("/etc/rozofs/"):          os.system("mkdir -p /etc/rozofs/")  
    try: os.remove('/usr/local/etc/rozofs/rozofs.conf');
    except:pass
    try: os.remove('/etc/rozofs/rozofs.conf');
    except:pass    
    save_stdout = sys.stdout
    sys.stdout = open("/usr/local/etc/rozofs/rozofs.conf","wr")
    self.display_common_config()
    sys.stdout.close()
    sys.stdout = save_stdout    
    shutil.copy2('/usr/local/etc/rozofs/rozofs.conf', '/etc/rozofs/rozofs.conf')
    
  def create_config(self):
    global hosts
    self.create_common_config()
    exportd.create_config()
    for h in hosts: h.create_config()
    geomgr.create_config()
    
  def delete_config(self):
    global hosts
    exportd.delete_config()
    mount="%s/export"%(rozofs.get_config_path())    
    os.system("umount -f %s > /dev/null 2>&1"%(mount))  
    for h in hosts: h.delete_config()
    geomgr.delete_config()
      

  def display(self):
    exportd.display()
    console("STORCLI:") 
    console("  . %-12s : %s "%("Nb",self.nb_storcli))
    console("  * Mojette threads")
    console("    . %-10s : %s"%("Read",self.read_mojette_threads))
    console("    . %-10s : %s"%("Write",self.write_mojette_threads))
    if self.mojette_threads_threshold == None:
      console("    . %-10s : %s"%("Threshold","default"))    
    else:  
      console("    . %-10s : %s bytes"%("Threshold",self.mojette_threads_threshold))
    console("STORAGE:")
    console("  . %-12s : %s"%("Mode",self.storio_mode))
    console("  . %-12s : %s"%("CRC32",self.crc32))
    console("  . %-12s : %s"%("Self healing mode",self.device_selfhealing_mode))
    console("  . %-12s : %s minutes"%("Self healing delay",self.device_selfhealing_delay))
    console("  . %-12s : %s ports"%("Listen",self.nb_listen))
    console("  . %-12s : %s "%("Threads",self.threads))
    if self.device_automount == True:
      console("  . %-12s : %s "%("Automount","YES"))
    else:  
      console("  . %-12s : %s "%("Automount","no"))  
    if self.disk_size_mb == None:
      console("  . %-12s : %s "%("Device size","no limit"))
    else:
      console("  . %-12s : %s MB (%s)"%("Device size",self.disk_size_mb,self.fstype))
        
    if len(volumes) != int(0):
      volumes[0].display()
    if len(hosts) != int(0):
      hosts[0].display()
    if len(mount_points) != int(0):      
      mount_points[0].display()    

  def create_path(self):
    for v in volumes: v.create_path()
    
  def delete_path(self):
    for v in volumes: v.delete_path()

    #	Delete spare devices
    for i in range(127):
      rozofs.delete_loopback_device("/dev/loop%d"%(i))
    os.system("rm -rf %s/devices"%(rozofs.get_config_path()))  
        
  def resume(self):
#    self.create_config()  
    check_build()  
    os.system("rm -rf /root/tmp/export; mkdir -p /root/tmp/export; rm -rf /root/tmp/storage; mkdir -p /root/tmp/storage;")
    for h in hosts: h.start()
    exportd.start()
    for m in mount_points: m.start() 
    geomgr.start()

  def configure(self):
    # Case of a loop device for metadata
    if rozofs.metadata_size != None:
      path="%s/devices/exportd"%(rozofs.get_config_path())
      mount="%s/export"%(rozofs.get_config_path())
      rozofs.create_export_loopback_device(path,mount,rozofs.metadata_size)  
    self.create_path()
    self.create_config()   
       
  def start(self):  
    self.stop()
    self.configure()    
    self.resume()
    
  def pause(self):
    geomgr.stop()
    for m in mount_points: m.stop()
    for h in hosts: h.stop()
    exportd.stop() 
    
#    time.sleep(1)
#    os.system("killall rozolauncher 2>/dev/null")
#    self.delete_config()

  def stop(self):
    self.pause()
    self.delete_config()
    for h in hosts: h.del_if()
    self.delete_path()
        
  def process(self,opt): 
    geomgr.process(opt)
    exportd.process(opt)
    for h in hosts: h.process(opt)
    for m in mount_points: m.process(opt)

  def cou(self,f):
    if not os.path.exists(f):
      console( "%s does not exist"%(f))
      exit(1) 
    string="attr -R -g rozofs %s"%(f)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    console(" ___________ %s ___________"%(f)) 
    for line in cmd.stdout:  
      line=line.split('\n')[0]
      console(line)
      words=line.split()
      if len(words) < int(3): continue      
      if words[0] =="MODE": 
        mode = words[2]
        if mode == "DIRECTORY" or mode == "SYMBOLIC": return 	
	continue
      if words[0] =="FID": 
        fid = words[2]
	continue  
      if words[0] =="STORAGE": 
        dist = words[2]
	continue  
      if words[0] =="CLUSTER": 
        cid = words[2]
	continue  
      if words[0] =="EID": 
        eid = words[2]
	continue  
      if words[0] =="VID": 
        vid = words[2]
	continue      	 
      if words[0] =="ST.NAME": 
        st_name = words[2]
	continue      	 

    SID_LIST=dist.split('-')
    
    c = get_cid(int(cid))
    
    for site in range(0,2):
    
      console("__________________Site %s"%(site)) 
      for sid in SID_LIST:
        console("")
        s = c.sid[int(sid)-1]
	path = s.get_site_root_path(site)
        string="find %s -name \"%s*\""%(path,st_name)
	parsed = shlex.split(string)
	cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for line in cmd.stdout: 
	  fname=line.split('\n')[0]
	  sz=os.path.getsize(fname) 
          tm=datetime.datetime.fromtimestamp(os.path.getmtime(fname))
          console("%10s  %s  %s"%(sz,tm,fname))	

  def exe_from_core_dir(self,dir):
    if dir == "storio": return "%s/build/src/%s/%s"%(os.getcwd(),"storaged",dir)
    if dir == "stspare": return "%s/build/src/%s/%s"%(os.getcwd(),"storaged",dir)
    if dir == "export_slave": return "%s/build/src/%s/%s"%(os.getcwd(),"exportd","exportd")
    if dir == "geomgr" : return "%s/build/src/%s/%s"%(os.getcwd(),"geocli",dir)    
    if dir == "rozo_rebalance" : return "%s/build/src/%s/%s"%(os.getcwd(),"exportd",dir)    
    return "%s/build/src/%s/%s"%(os.getcwd(),dir,dir)

  def do_monitor_cfg (self): 
    global hosts
    global exportd
    global mount_points
    for v in volumes: print "VOLUME %s %s"%(exportd.export_host,v.vid)
    for h in hosts: print "STORAGE %s"%(h.addr) 
    for m in mount_points: print "FSMOUNT localhost %s"%(m.instance)

  def monitor(self): 
    save_stdout = sys.stdout
    sys.stdout = open("monitor.cfg","w")
    self.do_monitor_cfg()
    sys.stdout.close()
    sys.stdout = save_stdout  
    os.system("./monitor.py 5 -c monitor.cfg")

  def core(self,argv):
    if argv == None or len(argv) == 2:
      nocore=True
      for d in os.listdir(self.core_dir()):
        if os.path.isdir(os.path.join(self.core_dir(), d)):
          exe=self.exe_from_core_dir(d)
	  for f in os.listdir(os.path.join(self.core_dir(), d)):
	    name=os.path.join(self.core_dir(), d, f)
            if nocore == True:
              nocore = False
              console("Some core files exist:")
            if os.path.getmtime(name) < os.path.getmtime(exe):
	      console("  (OLD) %s/%s"%(d,f))
	    else:
	      console("  (NEW) %s/%s"%(d,f)) 
      return  
    if argv[2] == "remove":
      if len(argv) == 3: return
      if argv[3] == "all":
        for d in os.listdir(self.core_dir()):
          if os.path.isdir(os.path.join(self.core_dir(), d)):
	    for f in os.listdir(os.path.join(self.core_dir(), d)):
	      try: os.remove(os.path.join(self.core_dir(), d, f))
	      except: pass
      else:
        try: os.remove(os.path.join(self.core_dir(), argv[3]))  
        except: pass
      return
    
    dir=argv[2].split('/')[0]
    exe=self.exe_from_core_dir(dir)
    if not os.path.exists(exe):
      syntax("No such executable","debug")
      return
    os.system("ddd %s -core %s"%(exe,os.path.join(self.core_dir(), argv[2]))) 
	      
#___________________________________________  
def cmd_returncode (string):
  global rozofs
  if rozofs.trace: console(string)
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  cmd.wait()
  return cmd.returncode
 
#___________________________________________  
def cmd_silent (string):
  # print string
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  output, error = cmd.communicate()	
        
#___________________________________________  
def check_build ():
  sucess=True
  if not os.path.exists("./build/src/exportd/exportd"):
    report("export is not built")
    sucess=False
  if not os.path.exists("./build/src/storaged/storaged"):
    report("storaged is not built")
    sucess=False
  if not os.path.exists("./build/src/storaged/storio"):
    report("storio is not built")
    sucess=False
  if not os.path.exists("./build/src/storaged/storage_rebuild"):
    report("storage_rebuild is not built")
  if not os.path.exists("./build/src/storaged/storage_list_rebuilder"):
    report("storage_list_rebuilder is not built")
  if not os.path.exists("./build/src/rozofsmount/rozofsmount"):
    report("rozofsmount is not built")
    sucess=False
  if not os.path.exists("./build/src/storcli/storcli"):
    report("storcli is not built")
    sucess=False
  if not os.path.exists("./build/src/rozodiag/rozodiag"):
    report("rozodiag is not built")
    sucess=False
  if not os.path.exists("./build/src/geocli/geocli"):
    report("geocli is not built")
    sucess=False
  if not os.path.exists("./build/src/geocli/geomgr"):
    report("geomgr is not built")
    sucess=False
  if not os.path.exists("./build/src/launcher/rozolauncher"):
    report("geomgr is not built")
    sucess=False
  if sucess==False: sys.exit(-1)
#_____________________________________________  
def syntax_export() :
  console("./setup.py \texportd  \t{start|stop|reset|pid|reload}") 

#_____________________________________________  
def syntax_geomgr() :
  console("./setup.py \tgeomgr \t{start|stop|reset|pid|modify|reinit}")
         
#_____________________________________________  
def syntax_mount() :
  console("./setup.py \tmount   \t{all|<instance>} {start|stop|reset|pid|info}")
  console("./setup.py \tmount   \t{all|<instance>} nfs {on|off}")
#_____________________________________________  
def syntax_storage() :
  console("./setup.py \tstorage \t{all|<host idx>} {start|stop|reset|rebuild|pid}")
  console("./setup.py \tstorage \t{all|<host idx>} {ifup|ifdown <#if>}")
  
#_____________________________________________  
def syntax_cou() :
  console("./setup.py \tcou     \t<fileName>")
#_____________________________________________  
def syntax_config() :
  console("./setup.py \tconfig  \t<confFileName>")  
#_____________________________________________  
def syntax_sid() :
  console("./setup.py \tsid     \t<cid> <sid>\tdevice-delete {all|<#device>} [<site>]")
  console("./setup.py \tsid     \t<cid> <sid>\tdevice-create {all|<#device>} [<site>]")
  console("./setup.py \tsid     \t<cid> <sid>\tdevice-clear  {all|<#device>} [<site>]")
  console("./setup.py \tsid     \t<cid> <sid>\trebuild...")
  console("./setup.py \tsid     \t<cid> <sid>\tinfo")
#_____________________________________________  
def syntax_if() :
  console("./setup.py \tifup|ifdown  \t<#if>")
#_____________________________________________  
def syntax_monitor() :
  console("./setup.py \tmonitor") 
#_____________________________________________  
def syntax_debug() :
  console("./setup.py \tcore    \tremove {all|<coredir>/<corefile>}")
  console("./setup.py \tcore    \t[<coredir>/<corefile>]")
  
#_____________________________________________  
def syntax_all() :
  console("Usage:")
  #console("./setup.py \tsite    \t<0|1>"
  console("./setup.py \tdisplay\t\t[conf. file]")
  console("./setup.py \t{start|stop}")
  console("./setup.py \t{configure|pause|resume}")
  console("./setup.py \tcmd <command to be executed in the setup context>")

  syntax_monitor()
    
  syntax_export()
  syntax_geomgr()
  syntax_mount()
  syntax_storage() 
  syntax_sid() 
  syntax_cou() 
  syntax_config()  
  syntax_if()
  syntax_debug()
  console("./setup.py \tprocess \t[pid]")
  console("./setup.py \tvnr ...")
  console("./setup.py \t{build|rebuild|clean}")
  sys.exit(-1)   
          
#_____________________________________________  
def syntax(string=None,topic=None) :

  if string != None: console("!!! %s !!!\n"%(string))

  if topic == None: syntax_all()
  
  func='syntax_%s'%(topic)
  try:
    getattr(sys.modules[__name__],func)() 
  except:
    pass
  sys.exit(-1)

#_____________________________________________  
def clean_build() :
  if os.path.exists("build") :    shutil.rmtree("build")
  if os.path.exists("../build") : shutil.rmtree("../build")  


#_____________________________________________  
def clean() :
  rozofs.stop() 
  clean_build()   

#_____________________________________________  
def build() :
  clean_build()   
  os.mkdir("build") 
  os.system("./setup.sh build")

#_____________________________________________  
def rebuild() :  
  if os.path.exists("build") : 
    os.system("cd build; make")
  else:
    build()

#_____________________________________________  	 
def test_parse(command, argv):	
  global rozofs
  global exportd
  global geomgr
   

  # Add path for rozofs executables
  try:
    for dir in os.listdir("%s/build/src"%(os.getcwd())):
      dir="%s/build/src/%s"%(os.getcwd(),dir)
      if os.path.isdir(dir):
        os.environ["PATH"] += (os.pathsep+dir)
  except: pass
  # To retrieve python tools
  try:
    for dir in os.listdir("%s/../src"%(os.getcwd())):
      dir="%s/../src/%s"%(os.getcwd(),dir)
      if os.path.isdir(dir):
        os.environ["PATH"] += (os.pathsep+dir)
  except: pass
      

  if   command == "display"            : rozofs.display()  
  elif command == "cmd"                : 
    cmd=""
    for arg in argv[2:]: cmd=cmd+" "+arg
    os.system("%s"%(cmd))
  elif command == "start"              : rozofs.start()  
  elif command == "stop"               : rozofs.stop() 
  elif command == "configure"          : rozofs.configure() 
  elif command == "pause"              : rozofs.pause()  
  elif command == "resume"             : rozofs.resume()  
  elif command == "build"              : build()
  elif command == "rebuild"            : rebuild()
  elif command == "clean"              : clean()
  elif command == "monitor"            : rozofs.monitor()
  elif command == "rozofs.conf"        : rozofs.create_common_config()

  elif command == "spare"              : 
    try: 
      rozofs.newspare(argv[2]) 
    except:
      rozofs.newspare() 
      
  elif command == "ifup":
    itf=None 
    if len(argv) < 3 : syntax("Missing interface number","if")
    try:    itf=int(argv[2])
    except: syntax("Bad interface number","if")
    for h in hosts: h.add_if(itf)
    
  elif command == "ifdown": 
    itf=None
    if len(argv) < 3 : syntax("Missing interface number","if")
    try:    itf=int(argv[2])
    except: syntax("Bad interface number","if")
    for h in hosts: h.del_if(itf)

  elif command == "it" or command == "vnr" : 
    param=""
    mount = mount_points[0]
    i=int(2)
    while int(i) < len(argv):
      if argv[i] == "-m" or argv[i] == "--mount":
        i+=1
	try:mount = mount_points[int(argv[i])]
	except:
	  console("-mount without valid mount point instance !!!")
	  sys.exit(1)
      elif argv[i] == "-nfs":
        param += " --nfs -e %s"%(mount.nfs_path)
      else:
        param +=" %s"%(argv[i])
      i+=1
    os.system("%s/IT2/IT.py -m %s %s"%(os.getcwd(),mount.instance,param))    

  elif command == "process"            : 
    if len(argv) == 2: rozofs.process('-a') 
    else:              rozofs.process('-ap') 
  elif command == "core"               : rozofs.core(argv)  

  elif command == "exportd"             :
       if len(argv) <= 2: syntax("export requires an action","export")  
       if argv[2] == "stop"        : exportd.stop()
       if argv[2] == "start"       : exportd.start()     
       if argv[2] == "reset"       : exportd.reset() 
       if argv[2] == "pid"         : exportd.process('-ap') 
       if argv[2] == "reload"      : exportd.reload() 

  elif command == "geomgr"             :
       if len(argv) <= 2: syntax("geomgr requires an action","geomgr")  
       if argv[2] == "stop"        : geomgr.stop()
       if argv[2] == "start"       : geomgr.start()     
       if argv[2] == "reset"       : geomgr.reset() 
       if argv[2] == "pid"         : geomgr.process('-ap') 

       if argv[2] == "modify"      : geomgr.modify()
       if argv[2] == "reinit"      : geomgr.reinit()     

  elif command == "mount"             :
       if len(argv) <= 3: syntax("mount requires instance + action","mount")
       if argv[2] == "all":
	 first=0
	 last=len(mount_points)
       else:
	 try: instance = int(argv[2])  
	 except: syntax("mount requires an integer instance","mount")
	 if (len(mount_points)) <= int(instance):syntax("No such mount instance %s"%(argv[2]),"mount")
	 first=instance
	 last=instance+1
       for idx in range(first,last):
	 obj = mount_points[idx]       
	 if argv[3] == "stop"        : obj.stop()
	 elif argv[3] == "start"       : obj.start()     
	 elif argv[3] == "reset"       : obj.reset()          
	 elif argv[3] == "pid"         : obj.process('-ap') 
	 elif argv[3] == "info"        : obj.info() 
	 elif argv[3] == "nfs"         : 
	   if argv[4] == None: syntax("Missing nfs action","mount")
	   if argv[4] == "on": obj.nfs(True)
	   else:               obj.nfs(False)
	 else: syntax("No such action %s"%(argv[3]),"mount")

  elif command == "storage"             :
       if len(argv) <= 3: syntax("storage requires instance + action","storage")
       if argv[2] == "all":
	 first=0
	 last=len(hosts)
       else:
	 try: instance = int(argv[2])  
	 except: syntax("storage requires an integer instance","storage")
	 if instance == 0: syntax("No such storage instance","storage")
	 if (len(hosts)) < int(instance):syntax("No such storage instance","storage")
	 first=instance-1
	 last=instance
       for idx in range(first,last):
	 obj = hosts[idx]     
	 if argv[3] == "stop"        : obj.stop()
	 if argv[3] == "start"       : obj.start()     
	 if argv[3] == "reset"       : obj.reset() 
	 if argv[3] == "rebuild"     : obj.rebuild(argv) 
	 if argv[3] == "ifdown"      :
	   if len(argv) <= 4: syntax("Missing interface#","storage")
	   
	   obj.del_if(argv[4])
	 if argv[3] == "ifup"      :
	   if len(argv) <= 4: syntax("Missing interface#","storage")
	   obj.add_if(argv[4])  
	 if argv[3] == "pid"         : obj.process('-ap')           

  elif command == "cou"                  : 
       if len(argv) <= 2: syntax("cou requires a file name","cou")
       rozofs.cou(argv[2]) 

  elif command == "get_nb_vol"         : 
       console("%d"%(len(volumes)))

  elif command == "sid" : 
       if len(argv) <= 4: syntax("sid requires cid+sid numbers","sid")

       try:     cid = int(argv[2])
       except:  syntax("get_cid_sid requires an integer for cluster id","sid") 
       if cid == 0: syntax("No such cluster id","sid")  
       if (len(cids)) < int(cid): syntax("No such cluster id","sid")
       c = get_cid(int(cid))

       try:     sid = int(argv[3])
       except:  syntax("get_cid_sid requires an integer for storage id","sid") 
       if sid == 0: syntax("No such storage id")  
       if sid > c.nb_sid(): syntax("No such storage id in this cluster","sid")
       sid-= 1         
       s = c.sid[sid]
              
       if argv[4] == "device-delete" : 
	 if len(argv) <= 5: syntax("sid device-delete requires a device number","sid")
	 if len(argv) <= 6: s.delete_device(argv[5],s.host[0])
	 else:
	   try:
	     hnum=int(argv[6])
	     h = s.host[hnum]
	     s.delete_device(argv[5],h)
	   except:
	     console("unexpected site number %s"%(argv[6]))
	     sys.exit(-1) 	

       if argv[4] == "device-create" : 
	 if len(argv) <= 5: syntax("sid device-create requires a device number","sid")
	 if len(argv) <= 6: s.create_device(argv[5],s.host[0])
	 else:
	   try:
	     hnum=int(argv[6])
	     h = s.host[hnum]
	     s.create_device(argv[5],h)
	   except:
	     console("unexpected site number %s"%(argv[6]))
	     sys.exit(-1) 

       if argv[4] == "device-clear" : 
	 if len(argv) <= 5: syntax("sid device-clear requires a device number","sid")
	 if len(argv) <= 6: 
	   s.clear_device(argv[5],s.host[0])
	 else:
	   try:
	     hnum=int(argv[6])
	     h = s.host[hnum]
	     s.clear_device(argv[5],h)
	   except:
	     console("unexpected site number %s"%(argv[6]))
	     sys.exit(-1) 
	     	 
       if argv[4] == "rebuild":
         s.rebuild(argv)         
       if argv[4] == "info"          : s.info()

              
  elif command == "get_vol_clusters"   : 
       if len(argv) <= 2: syntax("get_vol_clusters requires a volume number")
       try:    idx = int(argv[2])
       except: syntax("get_vol_clusters requires an integer for volume number") 
       if idx == 0: syntax("No such volume number")
       if (len(volumes)) < int(idx):syntax("No such volume number")       
       idx-=1 
       v = volumes[idx]
       string=""
       for c in v.cid: string += " %s"%(c.cid)
       console(string)

  elif command == "get_cluster_sid_nb" : 
       if len(argv) <= 2: syntax("get_cluster_sid requires a cluster number")
       try:     idx = int(argv[2])
       except:  syntax("get_cluster_sid requires an integer for cluster number") 
       if idx == 0: syntax("No such cluster number")  
       if (len(cids)) < int(idx): syntax("No such cluster number")
       c = get_cid(idx)
       console("%s"%(c.nb_sid()))

  else                                 : syntax("Unexpected command \"%s\n"%(command))

#____Initialize a few objects
def test_init():
  global rozofs
  global exportd
  global geomgr
  
  rozofs  = rozofs_class()
  exportd = exportd_class("localhost1/localhost2")
  geomgr  = geomgr_class()
  

# Add path for rozofs executables
try:
  for dir in os.listdir("%s/build/src"%(os.getcwd())):
    dir="%s/build/src/%s"%(os.getcwd(),dir)
    if os.path.isdir(dir):
      os.environ["PATH"] += (os.pathsep+dir)
except: pass
# To retrieve python tools
try:
  for dir in os.listdir("%s/../src"%(os.getcwd())):
    dir="%s/../src/%s"%(os.getcwd(),dir)
    if os.path.isdir(dir):
      os.environ["PATH"] += (os.pathsep+dir)
except: pass

FILE="%s/build/src/exportd/rozo_rbsList"%(os.getcwd())
os.system("if [ -f %s ]; then cp -f %s /usr/bin;fi"%(FILE,FILE))

if len(sys.argv) < int(2): syntax()
command = sys.argv[1]

# Some initializations
test_init()

# Read configuration file
if command == "display": 
  if len(sys.argv) == 2: 
    if os.path.exists("cnf.py"): execfile("cnf.py")
  else:
    if os.path.exists(sys.argv[2]): execfile(sys.argv[2])
else:  
  if os.path.exists("cnf.py"): execfile("cnf.py")


# Parse the command and execute it 
cmd=""
for arg in sys.argv: cmd=cmd+" "+arg
log(cmd)
 
test_parse(command,sys.argv)
