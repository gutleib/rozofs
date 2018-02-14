#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os.path
import subprocess
import time
import re
import shlex
import datetime
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders
from email import Encoders
import socket

from optparse import OptionParser
    
rozofs_status="OK"
    
    
class display_array:
  def __init__(self,columns):
  
    self.column_nb = columns
    self.column_size = []
    self.left = []

    for i in range(self.column_nb):
      self.column_size.append(0)
      self.left.append(True)
      
    self.row_nb = int(0)
    self.value = []    
    
    
  def display_separator_line(self,sep):
    line=''
    for c in range(self.column_nb):
      line=line+sep+'__'
      for ci in range(int(self.column_size[c])):
        line=line+'_'
    line=line+sep
    return line

  def new_line(self):
    row = []
    for c in range(self.column_nb):    
      row.append('')    
    self.value.append(row) 
    self.row_nb = int(self.row_nb)+1
    
  def separator(self):
    self.value.append(None) 
    self.row_nb = int(self.row_nb)+1

  def update_column_size(self,row,size):
    if int(self.column_size[int(row)-1]) < int(size):
      self.column_size[int(row)-1] = int(size)
            
  def set_column(self,column,value):
    if int(column) > int(self.column_nb):
      print "set_column column number to big %d"%(column)
      exit(-1)
    self.value[int(self.row_nb)-1][int(column)-1] = value
    self.update_column_size(column,len(value))

  def set_column_on_right(self,column):
    if int(column) > int(self.column_nb):
      print "set_column_on_left column number to big %d"%(column)
      exit(-1)
    self.left[int(column)-1] = False
      
                	
  def display(self):
  
    topline=self.display_separator_line(' ')
    separator=self.display_separator_line('|')
    
    print topline
    for row in range(int(self.row_nb)):
    
      if self.value[row] == None:
        print separator 
	continue
	
      line=''
      line=line+"| "
      for col in range(self.column_nb):
	l = self.column_size[col]-len(self.value[row][col])
	if self.left[col] == False:
      	  for i in range(l):
	    line=line+" "
	  line=line+self.value[row][col]+" | "
	else:
	  line=line+self.value[row][col]
      	  for i in range(l):
	    line=line+" "
	  line=line+" | "   	     
      print line  
          
    print separator    
#_____________________________________________  
def parse_period( period_string ):

  if period_string == None:
    return parse_period(DEFAULT_PERIOD)
    
  last=period_string[-1:]
  if last == 'h': 
    units=int(3600)  
    first=period_string[:-1]    
  if last == 'm':
    units=int(60)
    first=period_string[:-1]        
  elif last == 's':
    units=int(1) 
    first=period_string[:-1]        
  else:
    units=int(60)
    first=period_string    
           
  try:
    value=int(first)
    return int(value * units)
  except:
    pass

  return parse_period(DEFAULT_PERIOD)               
    
  
 
#_____________________________________________  
def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return " "
   
#_____________________________________________   
def rozofs_new_status(status,dis):
  global rozofs_status
  
  if rozofs_status == status:
    return
    
  rozofs_status = status
  
  if options.email == None:
    return

  zedate=time.strftime('%Y/%m/%d %H:%M')
    
  subject="%s: RozoFS %s status is %s"%(zedate,name,status)
  fro = "%s<%s@mail.com>"%(name,name)

 
  msg = MIMEMultipart()
  msg['From'] = fro
  msg['To'] = options.email
  msg['Date'] = formatdate(localtime=True) 
  msg['Subject'] = subject

  msg.attach( MIMEText(subject))

  file='/tmp/.monitor'
  save = sys.stdout
  sys.stdout = open(file, 'w')      
  dis.display()
  sys.stdout.close()      
  sys.stdout = save

  part = MIMEBase('application', "octet-stream")
  part.set_payload( open(file,"rb").read() )
  Encoders.encode_base64(part)
  part.add_header('Content-Disposition', 'attachment; filename="%s"'% os.path.basename(file))
  msg.attach(part)  
  
  try:
    smtp = smtplib.SMTP(options.smtp)
  except:
    print "Fail to connect to SMTP server \"%s\""%(options.smtp) 
    return
    
  try:   
    smtp.sendmail(fro, options.email, msg.as_string() )
  except:
    print "Fail to send mail. Check email adresses. %s"%(options.smtp) 
  smtp.close()    
    
#_____________________________________________   
def do_one_test( ):

  errors=int(0)
  warnings=int(0)

  dis = display_array(4)
  dis.new_line()
  dis.set_column(1,'Type')
  dis.set_column(2,'Identifier')
  dis.set_column(3,'Status')
  dis.set_column(4,'Diagnostic')
  dis.separator() 
       
  for line in config:

    words=line.split()
    dis.new_line()
    dis.set_column(1,words[0])
       
    if words[0] == "VOLUME":    
      string="%s -H %s -volume %d -w 25%c -c 10%c"%(vol,words[1],int(words[2]),'%','%')
      dis.set_column(2,"%s %s"%(words[1],words[2])) 
    if words[0] == "STORAGE":
      string="%s -H %s"%(stor,words[1])
      dis.set_column(2,"%s"%(words[1]))       
    if words[0] == "FSMOUNT":
      string="%s -H %s -instance %d"%(mount,words[1],int(words[2]))
      dis.set_column(2,"%s %s"%(words[1],words[2])) 
   
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = cmd.communicate()
    if error != '' :
      dis.set_column(3,'NOK') 
      dis.set_column(4,"execution error") 
      errors=errors+1
    else:
      result=output.split()
      dis.set_column(3,result[0])       
      if result[0] == "WARNING": warnings=warnings+1
      elif result[0] == "ERROR": errors=errors+1
      if result[0] == "OK":
        msg=find_between(output,'>','\n')
      else:
        msg=find_between(output,'<','>')	
      dis.set_column(4,msg) 	 

  if options.nodisplay == False:   
    print "\n%s : %s"%(name,time.strftime('%Y/%m/%d %H:%M:%S'))      	    	 
    dis.display()  
  
  if errors != int(0):
    rozofs_new_status("ERROR",dis)
  elif warnings != int(0):
    rozofs_new_status("WARNING",dis) 
  else:
    rozofs_new_status("OK",dis) 
          

#_____________________________________________  
def syntax(string) :

  if string != None:
    print "!!! %s !!!\n"%(string)
 
  print " -n <system name> "
  print "    This is a unic name identifying the RozoFS system in event mails."
  print "    Default value is %s"%(DEFAULT_NAME)
  print " -c <configuration file name> "
  print "    It gives the name of the configuration file telling what the RozoFS" 
  print "    system is made of. The configuration file format is similar to the"
  print "    input file of generate_rozofs_dynamic.sh. These are lines like:"
  print "      VOLUME <host> <volume id>"
  print "      STORAGE <host>"
  print "      FSMOUNT <host> <instance id>"
  print "    Default configuration file name is %s"%(DEFAULT_CFG_NAME)
  print " -t <period> "
  print "    This optionnal parameter gives the periodicity in minutes of the RozoFS polling."
  print "    Default value is %s minute"%(DEFAULT_PERIOD)
  print "    A value of 0 means a one shot call."
  print " -p <nagios plugins path> "
  print "    This optionnal parameter gives the path where to find the nagios pluggins."
  print "    Default value is \"%s\"."%(DEFAULT_PLUGIN_PATH)
  print " -s <smtp server> "
  print "    This optionnal parameter gives the name of the SMTP server when needed."
  print "    Default value is \"%s\"."%(DEFAULT_SMTP_SERVER)
  print " -e <email1@>,<email2@>,<email3@>,...,<emailN@> "
  print "    This optionnal parameter is a comma separated value without blanks giving the email"
  print "    addresses to send status change events to."
  print " -x "
  print "    This optionnal parameter prevents periodic display."  
  exit(-1) 
         
    
DEFAULT_CFG_NAME="monitor.cfg"          
DEFAULT_PLUGIN_PATH="../contrib/nagios/plugins"   
DEFAULT_SMTP_SERVER="localhost"   
DEFAULT_NAME=socket.gethostname() 
DEFAULT_PERIOD="10s"

rozofs_status="OK"

    
parser = OptionParser()
parser.add_option("-n","--name", action="store",type="string", dest="name", help="A RozoFS system unic name")
parser.add_option("-t","--period", action="store",type="string", dest="period", help="The period for polling in minutes")
parser.add_option("-c","--config", action="store",type="string", dest="config", help="The configuration file")
parser.add_option("-p","--plugins", action="store",type="string", dest="plugins", help="The path to the plugins")
parser.add_option("-s","--smtp", action="store",type="string", dest="smtp", help="The SMTP server name.")
parser.add_option("-e","--email", action="store",type="string", dest="email", help="A comma separated list of email addresses.")
parser.add_option("-x","--nodisplay", action="store_true",default=False, dest="nodisplay", help="Disables display.")
  
(options, args) = parser.parse_args()


period=parse_period(options.period)

  
if options.name == None:
  name=DEFAULT_NAME
else:
  name=options.name  
  
if options.config == None:
  config=DEFAULT_CFG_NAME
else:
  config=options.config  
# Read configuration file
if not os.path.exists(config):
  syntax("Configuration file %s does not exist"%(config))
with  open(config,"r") as fo:
  config=fo.readlines()

  
if options.plugins == None:
  plugins=DEFAULT_PLUGIN_PATH
else:
  plugins= options.plugins

vol="%s/check_rozofs_volume.sh"%(plugins)
if not os.path.exists(vol):
  syntax("%s not found"%vol)
  
stor="%s/check_rozofs_storaged.sh"%(plugins)
if not os.path.exists(stor):
  syntax("%s not found"%stor)
  
mount="%s/check_rozofs_rozofsmount.sh"%(plugins)
if not os.path.exists(mount):
  syntax("%s not found"%mount)
   
  
if options.smtp == None:
  smtp=DEFAULT_SMTP_SERVER
else:
  smtp= options.plugins


while True:
  do_one_test()  
  if period == 0:
    exit(0)	
  time.sleep(period)	
      
  

   
