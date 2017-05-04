#!/usr/bin/python
# -*- coding: utf-8 -*-

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
import syslog


#_______________________________________________
def log(string):
  syslog.syslog(string)
  print string

#_______________________________________________
def fatal(string):
  log(string)
  sys.exit(1)
  
#_______________________________________________
def debug(string):
  if options.debug == True: log(string)

#_______________________________________________
def ping(addr):
  """Sends a ping to destination
  """   
  parsed = ["ping", addr,"-c","1","-w","2"]
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  output, error = cmd.communicate()
  if error != '':return False

  for line in output.split('\n'):
	words = line.split()
	for i in range(len(words)):
	  if words[i] == "received,":
		nb=int(words[i-1])
		if nb == int(0): return False
  return True
#_______________________________________________
def rozodiag_check(exp,cmd):

  if ping(exp) == False: return False

  parsed = ["rozodiag", "-i",exp,"-t","2","-T","exportd","-c",cmd]  
  try:    command = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  except: return False

  try:    output, error = command.communicate()
  except: return False

  if "error on connect Connection refused!!!" in output: return False
  if error != '': return None
  return True
#_______________________________________________
def parse_export(exports):
  for exp in exports.split('/'):
    if rozodiag_check(exp,"exp_slave") == True: return exp
  return None	

#_______________________________________________
def system_cmd(cmd): 
  debug("%s"%(cmd)) 
  os.system(cmd)
    
#_______________________________________________
def ssh_export(exp,cmd): 
  global options
  
  string="ssh "
  if options.port:      string=string+"-p %s "%(options.port)
  if options.sshoption: string=string+"%s "%(options.sshoption)
  if options.user:      string=string+"%s@%s %s"%(options.user,exp,cmd)
  else:                 string=string+"root@%s %s"%(exp,cmd)
      
  system_cmd(string)
  
#_______________________________________________
def get_export(ldir,rdir,exp): 
  global options

  string="scp -r "
  # Capital -P for scp. 
  if options.port:       string=string+"-P %s "%(options.port)
  if options.sshoption:  string=string+"%s "%(options.sshoption)
  if options.user:       string=string+"%s"%(options.user)
  else:                  string=string+"root"
  string=string+"@%s:%s/* %s "%(exp,rdir,ldir)

  debug("%s"%(string)) 

#  parsed = ["scp", "-r", "root@%s:"%(exp),cmd]  
  parsed = shlex.split(string)
  try:    command = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  except: return None

  try:    output, error = command.communicate()
  except: return None

  return output  
###############################################
#
#                  M A I N 
#
###############################################
debut=time.time()
base=os.path.basename(sys.argv[0])

MYPATH=os.path.dirname(os.path.realpath(__file__))

string=""
for arg in sys.argv: string=string+" %s"%(arg)
log("%s"%(string)) 

parser = OptionParser()
parser.add_option("-e","--export", action="store",type="string", dest="exports", help="A \'/\' separated list of IP addresses or host names of the export nodes.")
parser.add_option("-c","--cidsid", action="store",type="string", dest="cidsid", help="A \',\' separated list of cid:sid to rebuild.")
parser.add_option("-p","--parallel", action="store",type="string", dest="parallel", help="rebuild parallelism.")
parser.add_option("-r","--rebuildRef", action="store",type="string", dest="rebuildRef", help="rebuild reference.")
parser.add_option("-E","--expDirectory", action="store",type="string", dest="expDirectory", help="Export directory to use.")
parser.add_option("-S","--storDirectory", action="store",type="string", dest="storDirectory", help="Storage directory to use.")
parser.add_option("-d","--debug", action="store_true",default=False, dest="debug", help="Debug trace.")
parser.add_option("-s","--simu", action="store",type="string", dest="simu", help="exportd configuration file")
parser.add_option("-u","--user", action="store",type="string", dest="user", help="User name to use for scp or ssh when different from root.")
parser.add_option("-P","--port", action="store",type="string", dest="port", help="Port to use for scp or ssh.")
parser.add_option("-o","--sshoption", action="store",type="string", dest="sshoption", help="Other ssh or scp option (such as -i <key path>)")
parser.add_option("--spare", action="store_true",dest="spare", help="set to only get spare file list.")
parser.add_option("--nominal", action="store_true", dest="nominal", help="set to only get nominal file list.")

(options, args) = parser.parse_args()

options.debug = True


# Check export parameter
if options.exports == None: fatal("Missing --export option")
# Parse and check export parameter
export = parse_export(options.exports)
if export == None: fatal("Bad --export parameter")


# Check parallel option
if options.parallel == None: fatal("Missing --parallel option")

try:    parallel = int(options.parallel)
except: fatal("Bad --parallel option %s"%(options.parallel))
  
# Check rebuild reference
if options.rebuildRef == None: fatal("Missing --rebuildRef option")
try:    rebuildRef = int(options.rebuildRef)
except:fatal("Bad --rebuildRef option %s"%(options.rebuildRef))
    
# Check cid/sid     
if options.cidsid == None: fatal("Missing --cidsid option")   

# Check cid/sid list
for cs in options.cidsid.split(','):
  try:
    cid=int(cs.split(':')[0])
    sid=int(cs.split(':')[1])
  except: fatal("Bad --cidsid option %s"%(cs))

# By default use /tmp on export
if options.expDirectory == None: options.expDirectory = "/tmp"

cmd="sudo rozo_rbsList -p %d -r %d -E %s -i %s"%( parallel, rebuildRef, options.expDirectory, options.cidsid)	
if options.simu != None: cmd=cmd+" -c %s"%(options.simu)
if options.nominal == True: cmd=cmd+" --nominal"
else:
  if options.spare == True: cmd=cmd+" --spare"
ssh_export(export,cmd)


ldir="%s/rbs.%d"%(options.storDirectory,rebuildRef)
rdir="%s/rebuild.%d"%(options.expDirectory,rebuildRef)

get_export(ldir,rdir,export)
ssh_export(export,"rm -rf %s"%(rdir))
log("%.2f sec"%(time.time()-debut))
