#!/usr/bin/python
# -*- coding: utf-8 -*-
cnf_clusters=[]
global layout
global layout_int
global clients_nb

#_____________________________________ 
def setLayout(l=0):
  global layout
  global layout_int
  
  layout_int=int(l)
  if   l == 0: layout = rozofs.layout_2_3_4()
  elif l == 1: layout = rozofs.layout_4_6_8()
  elif l == 2: layout = rozofs.layout_8_12_16()
  else: 
    print "No such layout %s"%(l)
    sys.exit(-1)  
  
#_____________________________________ 
# Create a volume with the given number of host
#
# The number of SID per host may be given as input
# when one want more SID per host than the minimum 
# by the layout. If no SID/host number is given or
# when the given number is too small, the minimum 
# required number of SID per host is used.
#
def setVolumeHosts(nbHosts, nbclusters, nbSidPerHost=0,vid=None):
  global layout_int
  global clients_nb
  global failures
   
  # Compute the required number of SID per host 
  # considering the number of host and the chossen layout
  minimumSidPerHost = int(1)
  safe              = rozofs.min_sid(layout_int)
  nb                = int(nbHosts)
  
  while int(safe) > int(nb):
    minimumSidPerHost = int(minimumSidPerHost) + 1
    nb                = int(nb) + int(nbHosts)
  
  # If the proposed number of SID per host is 
  # too small to have a correct SID distribution
  # create enough SID per host
  if int(nbSidPerHost) < minimumSidPerHost: 
    nbSidPerHost = minimumSidPerHost
    
  # Create a volume
  v1 = volume_class(layout, vid)

  # Compute the number of host failure allowed
  if minimumSidPerHost != 1:
    failures = int(v1.get_failures())
    failures = failures / minimumSidPerHost
    v1.set_failures(failures)
  
  # Create clusters on this volume
  for i in range(nbclusters):

    devSIze = int(rozofs.disk_size_mb)
    if int(xtraDevice) != int(0): devSIze += (i * xtraDevice)
    c = v1.add_cid(devices,mapper,redundancy,dev_size=devSIze)  
    cnf_clusters.append(c)
    nbSid = xtraSID * i

    # Create the required number of sid on each cluster
    # The 2 clusters use the same host for a given sid number
    for s in range(nbHosts):
      for f in range(nbSidPerHost):
        c.add_sid_on_host(s+1,(s % rozofs.site_number)+1)
      
    while nbSid != 0:
      for s in range(nbHosts):
        c.add_sid_on_host(s+1,(s % rozofs.site_number)+1)
        nbSid -= 1
        if nbSid == 0: break
                 
  return v1 
   
#_____________________________________ 
def addPrivate(vol,layout=None,eid=63):

  # Create on export for 4K, and one mount point
  e = vol.add_export(rozofs.bsize4K(),layout,eid)
  m = e.add_mount(0,name="private")
  return e    

#_____________________________________ 
def addExport(vol,layout=None,eid=None):

  # Create on export for 4K, and one mount point
  e = vol.add_export(rozofs.bsize4K(),layout,eid)

  for i in range(1,clients_nb+1): 
    if rozofs.site_number == 1:
      m1 = e.add_mount(0)
    else:	
      for site in range(0,rozofs.site_number+1): 
        m1 = e.add_mount(site)
  return e        
#_____________________________________ 

#rozofs.spare_restore_disable()

# Set metadata device characteristics
rozofs.set_metadata_size(500)
rozofs.set_min_metadata_inodes(1000)
rozofs.set_min_metadata_MB(5)


# Number of sites : default is to have 1 site
#rozofs.set_site_number(4)

#rozofs.set_trace()

# Trash rate
rozofs.set_trashed_file_per_run(1000)
rozofs.set_alloc_mb(0);

# Change number of core files
# rozofs.set_nb_core_file(1);

# Minimum delay in sec between remove and effective deletion
rozofs.set_deletion_delay(12)

# Enable FID recycling
#rozofs.set_fid_recycle(10)
#--------------STORIO GENERAL

# Set original RozoFS file distribution
rozofs.set_file_distribution(5)

# Disable CRC32
# rozofs.set_crc32(False)

# Enaable self healing
#rozofs.set_self_healing(1,"resecure")
rozofs.set_self_healing(1,"spareOnly")

# Disable spare file restoration
#rozofs.spare_restore_disable()

# Modify number of listen port/ per storio
# rozofs.set_nb_listen(4)

# Modify number of storio threads
#rozofs.set_threads(16)

# Use fixed size file mounted through losetup for devices

#rozofs.set_xfs(1000,None)
#rozofs.set_xfs(1000,"4096")
#rozofs.set_xfs(1000,"64K")
#rozofs.set_xfs(1000,"128M")

#--------------CLIENT GENERAL

# Enable mojette thread for read
# rozofs.enable_read_mojette_threads()

# Disable mojette thread for write
# rozofs.disable_write_mojette_threads()

# Modify mojette threads threshold
# rozofs.set_mojette_threads_threshold(32*1024)

# NB STORCLI
#rozofs.set_nb_storcli(4)

# Disable POSIX lock
#rozofs.no_posix_lock()

# Disable BSD lock
#rozofs.no_bsd_lock()


# Client fast reconnect
#rozofs.set_client_fast_reconnect()

# Unbalancing factor between clusters
# Add extra SID on each new cluster
xtraSID = 0
# Add extra MB on devices of each new cluster
xtraDevice = 0

#_________________________________________________
#          NB devices per sid
#_________________________________________________

device_mode = "mono"
sid_size_MB = 320

#-------------------
# Mono device mode : no header file
#
if device_mode == "mono":
  rozofs.set_ext4(sid_size_MB)  
  devices    = 1
  mapper     = 0
  redundancy = 0
#-------------------
# Legacy single device mode : 1 device with mapper/header file
#
elif  device_mode == "single":
  rozofs.set_ext4(sid_size_MB)  
  devices    = 1
  mapper     = 1
  redundancy = 1
#-------------------
# 2 devices with mapper/header file
#
elif  device_mode == "dual":
  rozofs.set_ext4(sid_size_MB/2)  
  devices    = 2
  mapper     = 2
  redundancy = 2
#-------------------
# 3 devices with mapper/header file
#
elif  device_mode == "triple":
  rozofs.set_ext4(sid_size_MB/3)  
  devices    = 3
  mapper     = 2
  redundancy = 2
else:
  print "Unexpected device mode %s"%(device_mode)



# default is to have one mount point per site and export
clients_nb = 1
# Define default layout
setLayout(1)


#_________________________________________________
#          Multi file configuration
#_________________________________________________

unit_512K = 1
unit_1M = 2
factor_2_files = 1
factor_3_files = 2
factor_4_files = 3
factor_5_files = 4

config_choice = "multiple"

#-------------------
# Single 
#
if config_choice == "single":

  vol = setVolumeHosts(nbHosts = 4, nbclusters = 5)
  e = addExport(vol,layout=1,eid=1)

#-------------------
# multiple 
#
if config_choice == "multiple":

  vol = setVolumeHosts(nbHosts = 4, nbclusters = 5)
  exp = addExport(vol,layout=1,eid=1)
  exp.set_striping(factor_3_files,unit_1M)
  
#-------------------
# Hybrid 
# 
if config_choice == "hybrid":

  vfast = setVolumeHosts(nbHosts = 4, nbclusters = 3)
  vol   = setVolumeHosts(nbHosts = 4, nbclusters = 4)

  exp = addExport(vol,layout=1,eid=1)
  exp.set_vid_fast(vfast)
  exp.set_fast_mode("hybrid")
  exp.set_striping(factor_3_files,unit_1M)  
  
#-------------------
# aging 
# 
if config_choice == "aging":

  vfast = setVolumeHosts(nbHosts = 4, nbclusters = 3)
  vol   = setVolumeHosts(nbHosts = 4, nbclusters = 3)

  exp = addExport(vol,layout=1,eid=1)
  exp.set_vid_fast(vfast)
  exp.set_fast_mode("aging")
  exp.set_striping(factor_3_files,unit_1M)
  

# Freeze cluster 1
#
#c = get_cid(int(1))
#c.freeze()


# Set thin provisionning
#e.set_thin()

#e = addExport(vol,layout=1,eid=9)
#e = addExport(vol,layout=1,eid=17)

addPrivate(vol,layout=1)


# Set host 1 faulty
#h1 = host_class.get_host(1)
#if h1 == None:
#  print "Can find host 1"
#else:
#  h1.set_admin_off()  
  
