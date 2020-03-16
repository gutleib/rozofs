#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os.path
import subprocess
import time
import re
import shlex
import datetime
from optparse import OptionParser

import sys
  
objects = []
modules = []
  
#_______________________________________________
class conf_obj:

  def __init__(self,name,module,genre,default,comment,minmax=None,values=None):

    self.name     = name
    self.module   = module
    self.genre    = genre
    self.default  = default
    self.mini=None
    self.maxi=None
    self.comment = comment    
    if values != None:
      self.values = values.replace('"','')
    else:
      self.values = None
      
    if minmax != None:
      try:
        self.mini=minmax.split(':')[0]
	self.maxi=minmax.split(':')[1]
      except:
        print "%s %s bad min:max \"%s\""%(genre,name,minmax)
        raise ValueError()    


    if module not in modules: modules.append(module)
   
    objects.append(self)

#    print "%s is %s in %s"%(name,genre,module)

  def write_in_struct(self,struct_name):

    for comment in self.comment:  print "  %s"%(comment) 
     
    if self.genre == "STRING":
      print "  char *      %s;"%(self.name) 
    elif self.genre == "ENUM":    
      print "  %s_%s_e  %s;"%(struct_name,self.name,self.name)
    elif self.genre == "LONG":
      print "  int64_t     %s;"%(self.name)
    else:
      print "  int32_t     %s;"%(self.name)

  def write_in_show(self,struct_name, alignment = "  "):  
    print "%s%s_IS_DEFAULT_%s(%s,%s);"%(alignment,struct_name.upper(),self.genre,self.name,self.default) 
    print "%sif (isDefaultValue==0) pChar += rozofs_string_set_bold(pChar);"%(alignment)        
    for comment in self.comment: print "%spChar += rozofs_string_append(pChar,\"%s\\n\");"%(alignment,comment)
    if self.genre == "ENUM":
      print "%s%s_SHOW_ENUM(%s,\"%s\",\"%s\");"%(alignment,struct_name.upper(),self.name,self.default,self.values)      
    elif self.mini != None:
      print "%s%s_SHOW_%s_OPT(%s,%s,\"%s:%s\");"%(alignment,struct_name.upper(),self.genre,self.name,self.default,self.mini,self.maxi) 
    else:
      print "%s%s_SHOW_%s(%s,%s);"%(alignment,struct_name.upper(),self.genre,self.name,self.default) 
    print "%sif (isDefaultValue==0) pChar += rozofs_string_set_default(pChar);"%(alignment)         

  def write_in_save(self,struct_name):  
    print ""
    print "  %s_IS_DEFAULT_%s(%s,%s);"%(struct_name.upper(),self.genre,self.name,self.default) 
    print "  if (isDefaultValue==0) {"
    for comment in self.comment: print "    pChar += rozofs_string_append(pChar,\"%s\\n\");"%(comment)
    if self.genre == "ENUM":
      print "    %s_SHOW_%s(%s,%s,\"%s\");"%(struct_name.upper(),self.genre,self.name,self.default,self.values)      
    elif self.mini != None:
      print "    %s_SHOW_%s_OPT(%s,%s,\"%s:%s\");"%(struct_name.upper(),self.genre,self.name,self.default,self.mini,self.maxi) 
    else:
      print "    %s_SHOW_%s(%s,%s);"%(struct_name.upper(),self.genre,self.name,self.default) 
    print "  }"
              
  def read(self,struct_name):
    for comment in self.comment: print "  %s "%(comment)
    if self.genre == "ENUM" :
      print "  %s_%s_READ_ENUM(&cfg);"%(struct_name.upper(),self.name.upper())             
    elif self.mini == None:
      print "  %s_READ_%s(%s,%s);"%(struct_name.upper(),self.genre,self.name,self.default)       
    else:
      print "  %s_READ_%s_MINMAX(%s,%s,%s,%s);"%(struct_name.upper(),self.genre,self.name,self.default,self.mini,self.maxi)                

  def set(self,struct_name):
    print "  if (strcmp(parameter,\"%s\")==0) {"%(self.name)
    if self.genre == "ENUM":
      print "    %s_%s_SET_ENUM(value);"%(struct_name.upper(),self.name.upper())             
    elif self.mini == None:
      print "    %s_SET_%s(%s,value);"%(struct_name.upper(),self.genre,self.name)       
    else:
      print "    %s_SET_%s_MINMAX(%s,value,%s,%s);"%(struct_name.upper(),self.genre,self.name,self.mini,self.maxi)                    
    print "  }"  

  def search(self,struct_name):
    print "  if (strcasestr(\"%s\",parameter) != NULL) {"%(self.name)  
    print "    match++;"
    self.write_in_show(struct_name,alignment="    ");
    print "  }"  
#_______________________________________________
class conf_int(conf_obj):

  def __init__(self,name,module,default,comment,minmax=None):
    try: int(default)
    except:
      print "INT %s has not an integer default value \"%s\""%(name,default)
      raise ValueError()
    conf_obj.__init__(self,name,module,"INT",default,comment,minmax)

#_______________________________________________
class conf_long(conf_obj):

  def __init__(self,name,module,default,comment,minmax=None):
    try: int(default)
    except:
      print "LONG %s has not an integer default value \"%s\""%(name,default)
      raise ValueError()
    conf_obj.__init__(self,name,module,"LONG",default,comment,minmax)

#_______________________________________________
class conf_string(conf_obj):

  def __init__(self,name,module,default,comment):
    conf_obj.__init__(self,name,module,"STRING",default,comment)

#_______________________________________________
class conf_bool(conf_obj):

  def __init__(self,name,module,default,comment):
    try: bool(default)
    except:
      print "BOOL %s has not a boolean default value \"%s\""%(name,default)
      raise ValueError()
    conf_obj.__init__(self,name,module,"BOOL",default,comment)    
#_______________________________________________
class conf_enum(conf_obj):

  def __init__(self,name,module,default,valueList,comment):
    if default == None:
      print "ENUM %s has no default value."%(name)
      raise ValueError()
    if valueList == None:
      print "ENUM %s has no value list."%(name)
      raise ValueError()
     
    conf_obj.__init__(self,name,module,"ENUM",default,comment,values=valueList)    

#_______________________________________________
def start_header_file(name,struct_name):
  global save_stdout
  
  save_stdout = sys.stdout
  sys.stdout = open("%s.h"%(name),"w")


  print "/*"
  print ""  
  print " File generated by ../../tools/%s.py from %s.input"%(struct_name,struct_name)
  print ""
  print " Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>"
  print " This file is part of Rozofs."
  print ""
  print " Rozofs is free software; you can redistribute it and/or modify"
  print " it under the terms of the GNU General Public License as published"
  print " by the Free Software Foundation, version 2."
  print ""
  print " Rozofs is distributed in the hope that it will be useful, but"
  print " WITHOUT ANY WARRANTY; without even the implied warranty of"
  print " MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU"
  print " General Public License for more details."
  print ""
  print " You should have received a copy of the GNU General Public License"
  print " along with this program.  If not, see"
  print " <http://www.gnu.org/licenses/>."
  print " */"
  print "#ifndef _%s_H"%(name.upper())
  print "#define _%s_H"%(name.upper())
  print ""
  print "#include <stdlib.h>"
  print "#include <stdint.h>"
  print "#include <string.h>"
  print "#include <errno.h>"
  print "#include <libconfig.h>"
  print "#include <unistd.h>"
  print "#include <inttypes.h>"
  print "#include <sys/types.h>"
  print "#include <dirent.h>"
#_______________________________________________
def end_file(struct_name):
  global save_stdout
  
  print "#endif"
  sys.stdout.close()
  sys.stdout = save_stdout    


#_______________________________________________
def go_read_input_file(struct_name):
  comment =  []
  
  if not os.path.exists(struct_name):
    print "Missing file %s"%(struct_name)
    sys.exit(1)
    
  with open(struct_name,"r") as finput:
  
    for line in finput:
 
      if len(line.split()) == 0: continue
      if line[0] == '#': continue         

      try:
        if line[0] == '/' and line[1] == '/':
	  genre = "//"
	else:
	  genre=line.split()[0]  
      except: 
        genre=line.split()[0]
      	
      if genre == "//": 
        comment.append(line[:-1])
	continue
	
      module=line.split()[1]
      name=line.split()[2]
      if genre != "STRING": 
        default=line.split()[3]
      else:
        default='"'+line.split('"')[1]+'"'
	
      if genre == "INT":
        if len(line.split()) == 5:
	  obj = conf_int(name,module,default,comment,line.split()[4])
	else:
	  obj = conf_int(name,module,default,comment)  
          
      elif genre == "LONG":
        if len(line.split()) == 5:
	  obj = conf_long(name,module,default,comment,line.split()[4])
	else:
	  obj = conf_long(name,module,default,comment)  
                  
      elif genre == "BOOL":
        obj = conf_bool(name,module,default,comment)
        
      elif genre == "STRING": 
        obj = conf_string(name,module,default,comment)
        
      elif genre == "ENUM":
        valueList = None
        if len(line.split()) == 5:
           valueList = line.split()[4]
        obj = conf_enum(name,module,default,valueList,comment)
        
      else:
        print "Unknown type %s for %s"%(genre,name)	
      comment =  []	
#_______________________________________________
def go_build_macros(struct_name):

  print "#define %s_SHOW_NAME(val,def) {\\"%(struct_name.upper())
  print "  if (isDefaultValue) {\\"  
  print "    pChar += rozofs_string_append(pChar,\"// \");\\"
  print "    pChar += rozofs_string_padded_append(pChar, 50, rozofs_left_alignment, #val);\\"
  print "    pChar += rozofs_string_append(pChar, \" = \");\\"
  print "  } else {\\"
  print "    pChar += rozofs_string_append(pChar,\"// default is \");\\"
  print "    pChar += rozofs_string_append(pChar, #def);\\"
  print "    pChar += rozofs_eol(pChar);\\"
  print "    pChar += rozofs_string_append(pChar,\"   \");\\"  
  print "    pChar += rozofs_string_padded_append(pChar, 50, rozofs_left_alignment, #val);\\"
  print "    pChar += rozofs_string_append(pChar, \" = \");\\"
  print "  }\\"
  print "}"
  print ""

  print "#define  %s_SHOW_NEXT \\"%(struct_name.upper())
  print "  pChar += rozofs_eol(pChar);\\"
  print "  pChar += rozofs_eol(pChar);"
  print ""
  
  print "#define  %s_SHOW_END \\"%(struct_name.upper())
  print "  *pChar++ = ';';\\"
  print "  %s_SHOW_NEXT"%(struct_name.upper())
  print ""
  
  print "#define  %s_SHOW_END_OPT(opt) \\"%(struct_name.upper())
  print "  pChar += rozofs_string_append(pChar,\"; \\t// \");\\"
  print "  pChar += rozofs_string_append(pChar,opt);\\"
  print "  %s_SHOW_NEXT"%(struct_name.upper())
  print ""
  
  print "#define %s_IS_DEFAULT_BOOL(val,def) \\"%(struct_name.upper())
  print "  isDefaultValue = 0;\\"
  print "  if (((%s.val)&&(strcmp(#def,\"True\")==0)) \\"%(struct_name)
  print "  ||  ((!%s.val)&&(strcmp(#def,\"False\")==0))) \\"%(struct_name)
  print "    isDefaultValue = 1;"
  print ""  
  
  print "#define %s_SHOW_BOOL(val,def)  {\\"%(struct_name.upper())
  print "  %s_SHOW_NAME(val,def)\\"%(struct_name.upper())
  print "  if (%s.val) pChar += rozofs_string_append(pChar, \"True\");\\"%(struct_name)
  print "  else        pChar += rozofs_string_append(pChar, \"False\");\\"
  print "  %s_SHOW_END\\"%(struct_name.upper())
  print "}"
  print "" 
   
  print "#define %s_IS_DEFAULT_STRING(val,def) \\"%(struct_name.upper())
  print "  isDefaultValue = 0; \\"
  print "  if (strcmp(%s.val,def)==0) isDefaultValue = 1;"%(struct_name)
  print ""  
       
  print "#define %s_SHOW_STRING(val,def)  {\\"%(struct_name.upper())
  print "  %s_SHOW_NAME(val,def)\\"%(struct_name.upper())
  print "  *pChar++ = '\\\"';\\"
  print "  if (%s.val!=NULL) pChar += rozofs_string_append(pChar, %s.val);\\"%(struct_name,struct_name)
  print "  *pChar++ = '\\\"';\\"
  print "  %s_SHOW_END\\"%(struct_name.upper())
  print "}"
  print ""  
  
  print "#define %s_IS_DEFAULT_ENUM(val,def) \\"%(struct_name.upper())
  print "  isDefaultValue = 0; \\"
  print "  if (%s.val == string2%s_ ## val (def)) isDefaultValue = 1;"%(struct_name,struct_name)
  print ""  
    
  print "#define %s_SHOW_ENUM(val,def,opt)  {\\"%(struct_name.upper())
  print "  %s_SHOW_NAME(val,def)\\"%(struct_name.upper())
  print "  *pChar++ = '\\\"';\\"
  print "  pChar += rozofs_string_append(pChar, %s_ ## val ## 2String(%s.val));\\"%(struct_name,struct_name)
  print "  *pChar++ = '\\\"';\\"
  print "  %s_SHOW_END_OPT(opt)\\"%(struct_name.upper())
  print "}"
  print ""  

  print "#define %s_IS_DEFAULT_INT(val,def) \\"%(struct_name.upper())
  print "  isDefaultValue = 0; \\"
  print "  if (%s.val == def) isDefaultValue = 1;"%(struct_name)
  print "" 
    
  print "#define %s_SHOW_INT(val,def)  {\\"%(struct_name.upper())
  print "  %s_SHOW_NAME(val,def)\\"%(struct_name.upper())
  print "  pChar += rozofs_i32_append(pChar, %s.val);\\"%(struct_name)
  print "  %s_SHOW_END\\"%(struct_name.upper())
  print "}"  
  print "" 

  print "#define %s_IS_DEFAULT_INT_OPT(val,def)  %s_IS_DEFAULT_INT(val,def)"%(struct_name.upper(),struct_name.upper())

  print "#define %s_SHOW_INT_OPT(val,def,opt)  {\\"%(struct_name.upper())
  print "  %s_SHOW_NAME(val,def)\\"%(struct_name.upper())
  print "  pChar += rozofs_i32_append(pChar, %s.val);\\"%(struct_name)
  print "  %s_SHOW_END_OPT(opt)\\"%(struct_name.upper())
  print "}"  
  print ""  

  print "#define %s_IS_DEFAULT_LONG(val,def)  %s_IS_DEFAULT_INT(val,def)"%(struct_name.upper(),struct_name.upper())
    
  print "#define %s_SHOW_LONG(val,def)  {\\"%(struct_name.upper())
  print "  %s_SHOW_NAME(val,def)\\"%(struct_name.upper())
  print "  pChar += rozofs_i64_append(pChar, %s.val);\\"%(struct_name)
  print "  %s_SHOW_END\\"%(struct_name.upper())
  print "}"  
  print "" 
  
  print "#define %s_IS_DEFAULT_LONG_OPT(val,def)  %s_IS_DEFAULT_INT(val,def)"%(struct_name.upper(),struct_name.upper())

  print "#define %s_SHOW_LONG_OPT(val,def,opt)  {\\"%(struct_name.upper())
  print "  %s_SHOW_NAME(val,def)\\"%(struct_name.upper())
  print "  pChar += rozofs_i64_append(pChar, %s.val);\\"%(struct_name)
  print "  %s_SHOW_END_OPT(opt)\\"%(struct_name.upper())
  print "}"  
  print ""  

  print "#define %s_READ_BOOL(val,def)  {\\"%(struct_name.upper())
  print "  int  boolval;\\"  
  print "  if (strcmp(#def,\"True\")==0) {\\"
  print "    %s.val = 1;\\"%(struct_name)
  print "  } else {\\"
  print "    %s.val = 0;\\"%(struct_name)
  print "  }\\"
  print "  if (config_lookup_bool(&cfg, #val, &boolval) == CONFIG_TRUE) { \\"
  print "    %s.val = boolval;\\"%(struct_name)
  print "  }\\"
  print "}"
  print "#define %s_SET_BOOL(val,def)  {\\"%(struct_name.upper())
  print "  if (strcmp(def,\"True\")==0) {\\"
  print "    %s.val = 1;\\"%(struct_name)
  print "    pChar += rozofs_string_append(pChar,#val);\\"
  print "    pChar += rozofs_string_append(pChar,\" set to value \");\\"
  print "    pChar += rozofs_string_append(pChar,def);\\"
  print "    pChar += rozofs_eol(pChar);\\"
  print "    return 0;\\"
  print "  }\\"
  print "  if (strcmp(def,\"False\")==0) {\\"
  print "    %s.val = 0;\\"%(struct_name)
  print "    pChar += rozofs_string_append(pChar,#val);\\"
  print "    pChar += rozofs_string_append(pChar,\" set to value \");\\"
  print "    pChar += rozofs_string_append(pChar,def);\\"
  print "    pChar += rozofs_eol(pChar);\\"
  print "    return 0;\\"
  print "  }\\"
  print "  pChar += rozofs_string_append_error(pChar,\"True or False value expected.\\n\" );\\"
  print "  return -1;\\"
  print "}"
  print ""  
  print "#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR >= 4)) \\"
  print "             || (LIBCONFIG_VER_MAJOR > 1))"
  print "static int               intval;"
  print "#else"
  print "static long int          intval;"
  print "#endif"
  print ""  
  print "#define %s_READ_INT_MINMAX(val,def,mini,maxi)  {\\"%(struct_name.upper())
  print "  %s.val = def;\\"%(struct_name)
  print "  if (config_lookup_int(&cfg, #val, &intval) == CONFIG_TRUE) { \\"
  print "    if (intval<mini) {\\"
  print "      %s.val = mini;\\"%(struct_name)
  print "    }\\"
  print "    else if (intval>maxi) { \\"
  print "      %s.val = maxi;\\"%(struct_name)
  print "    }\\"
  print "    else {\\"
  print "      %s.val = intval;\\"%(struct_name)
  print "    }\\"
  print "  }\\"
  print "}"
  print ""  
  print "#define %s_SET_INT_MINMAX(val,def,mini,maxi)  {\\"%(struct_name.upper())
  print "  int valint;\\"
  print "  if (sscanf(def,\"%d\",&valint) != 1) {\\"
  print "    pChar += rozofs_string_append_error(pChar,\"integer value expected.\\n\");\\"  
  print "    return -1;\\"
  print "  }\\"  
  print "  if (valint<mini) {\\"
  print "    pChar += rozofs_string_append_error(pChar,\"value lower than minimum.\\n\");\\"  
  print "    return -1;\\"
  print "  }\\"
  print "  if (valint>maxi) { \\"
  print "    pChar += rozofs_string_append_error(pChar,\"value bigger than maximum.\\n\");\\"  
  print "    return -1;\\"
  print "  }\\"
  print "  pChar += rozofs_string_append(pChar,#val);\\"
  print "  pChar += rozofs_string_append(pChar,\" set to value \");\\"
  print "  pChar += rozofs_string_append(pChar,def);\\"
  print "  pChar += rozofs_eol(pChar);\\"
  print "  %s.val = valint;\\"%(struct_name)
  print "  return 0;\\"
  print "}"
  print ""   
      
  print "#define %s_READ_INT(val,def) {\\"%(struct_name.upper())
  print "  %s.val = def;\\"%(struct_name)
  print "  if (config_lookup_int(&cfg, #val, &intval) == CONFIG_TRUE) { \\"
  print "    %s.val = intval;\\"%(struct_name)
  print "  }\\"
  print "}" 
  print ""  
  print "#define %s_SET_INT(val,def)  {\\"%(struct_name.upper())
  print "  int valint;\\"
  print "  if (sscanf(def,\"%d\",&valint) != 1) {\\"
  print "    pChar += rozofs_string_append_error(pChar,\"integer value expected.\\n\");\\"  
  print "    return -1;\\"
  print "  }\\"  
  print "  pChar += rozofs_string_append(pChar, #val);\\"
  print "  pChar += rozofs_string_append(pChar,\" set to value \");\\"
  print "  pChar += rozofs_string_append(pChar,def);\\"
  print "  pChar += rozofs_eol(pChar);\\"
  print "  %s.val = valint;\\"%(struct_name)
  print "  return 0;\\"
  print "}"
  print ""   
     
  print "#define %s_READ_LONG(val,def) {\\"%(struct_name.upper())
  print "  long long         longval;\\"
  print "  %s.val = def;\\"%(struct_name)
  print "  if (config_lookup_int64(&cfg, #val, &longval) == CONFIG_TRUE) { \\"
  print "    %s.val = longval;\\"%(struct_name)
  print "  }\\"
  print "}" 
  print ""  
  print "#define %s_SET_LONG(val,def) {\\"%(struct_name.upper())
  print "  long long         longval;\\"
  print "  if (sscanf(def,\"%lld\",&longval) != 1) {\\"
  print "    pChar += rozofs_string_append_error(pChar,\"long long integer value expected.\\n\");\\"  
  print "    return -1;\\"
  print "  }\\"  
  print "  pChar += rozofs_string_append(pChar,#val);\\"
  print "  pChar += rozofs_string_append(pChar,\" set to value \");\\"
  print "  pChar += rozofs_string_append(pChar,def);\\"
  print "  pChar += rozofs_eol(pChar);\\"
  print "  %s.val = longval;\\"%(struct_name)
  print "  return 0;\\" 
  print "}" 
  print ""  

  print ""  
  print "#define %s_READ_LONG_MINMAX(val,def,mini,maxi)  {\\"%(struct_name.upper())
  print "  long long         longval;\\"
  print "  %s.val = def;\\"%(struct_name)
  print "  if (config_lookup_int64(&cfg, #val, &longval) == CONFIG_TRUE) { \\"
  print "    if (longval<mini) {\\"
  print "      %s.val = mini;\\"%(struct_name)
  print "    }\\"
  print "    else if (longval>maxi) { \\"
  print "      %s.val = maxi;\\"%(struct_name)
  print "    }\\"
  print "    else {\\"
  print "      %s.val = longval;\\"%(struct_name)
  print "    }\\"
  print "  }\\"
  print "}"
  print ""  
  print ""  
  print "#define %s_SET_LONG_MINMAX(val,def,mini,maxi)  {\\"%(struct_name.upper())
  print "  long long         longval;\\"
  print "  if (sscanf(def,\"%lld\",&longval) != 1) {\\"
  print "    pChar += rozofs_string_append_error(pChar,\"long long integer value expected.\\n\"));\\"  
  print "    return -1;\\"
  print "  }\\"  
  print "  if (longval<mini) {\\"
  print "    pChar += rozofs_string_append_error(pChar,\"value lower than minimum.\\n\"));\\"  
  print "    return -1;\\"
  print "  }\\"
  print "  if (longval>maxi) { \\"
  print "    pChar += rozofs_string_append_error(pChar,\"value bigger than maximum.\\n\"));\\"  
  print "    return -1;\\"
  print "  }\\"
  print "  pChar += rozofs_string_append(pChar,#val);\\"
  print "  pChar += rozofs_string_append(pChar,\" set to value \");\\"
  print "  pChar += rozofs_string_append(pChar,def);\\"
  print "  pChar += rozofs_eol(pChar);\\"
  print "  %s.val = longval;\\"%(struct_name)
  print "  return 0;\\"
  print "}"

  print "#define %s_READ_STRING(val,def)  {\\"%(struct_name.upper())
  print "  const char * charval;\\"
  print "  if (%s.val) free(%s.val);\\"%(struct_name,struct_name)
  print "  if (config_lookup_string(&cfg, #val, &charval) == CONFIG_TRUE) {\\"
  print "    %s.val = strdup(charval);\\"%(struct_name)
  print "  } else {\\"
  print "    %s.val = strdup(def);\\"%(struct_name)
  print "  }\\"
  print "}" 
  print ""

  print "#define %s_SET_STRING(val,def)  {\\"%(struct_name.upper())
  print "  if (%s.val) free(%s.val);\\"%(struct_name,struct_name)
  print "  %s.val = strdup(def);\\"%(struct_name)
  print "  pChar += rozofs_string_append(pChar,#val);\\"
  print "  pChar += rozofs_string_append(pChar,\" set to value \");\\"
  print "  pChar += rozofs_string_append(pChar,def);\\"
  print "  pChar += rozofs_eol(pChar);\\"
  print "  return 0;\\"
  print "}" 
  print "" 
  
#_______________________________________________
def go_build_cfile(struct_name):
  global save_stdout
  
  save_stdout = sys.stdout
  sys.stdout = open("%s.c"%(struct_name),"w")

  print "/*"
  print " Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>"
  print " This file is part of Rozofs."
  print ""
  print " Rozofs is free software; you can redistribute it and/or modify"
  print " it under the terms of the GNU General Public License as published"
  print " by the Free Software Foundation, version 2."
  print ""
  print " Rozofs is distributed in the hope that it will be useful, but"
  print " WITHOUT ANY WARRANTY; without even the implied warranty of"
  print " MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU"
  print " General Public License for more details."
  print ""
  print " You should have received a copy of the GNU General Public License"
  print " along with this program.  If not, see"
  print " <http://www.gnu.org/licenses/>."
  print " */"
  print ""
  print "#include \"config.h\""
  print "#include \"%s.h\""%(struct_name)
  print "#include <rozofs/core/uma_dbg_api.h>"
  print "#include <rozofs/rozofs.h>"
  print "#include <rozofs/common/log.h>"
  print ""
  print ""
  print "static char %s_file_name[256] = {0};"%(struct_name)
  print "static int  %s_file_is_read=0;"%(struct_name)
  print "%s_t %s;"%(struct_name,struct_name)
  print ""
  print "void show_%s(char * argv[], uint32_t tcpRef, void *bufRef);"%(struct_name)
  print "void %s_read(char * fname) ;"%(struct_name)
  print ""
  print "char   myBigBuffer[1024*1024];"
  print "static int isDefaultValue;" 
  
  go_build_macros(struct_name)
    
  print ""
  print "#include \"%s_read_show.h\""%(struct_name)
  print "void %s_extra_checks(void);"%(struct_name)
  print ""
  print "void show_%s(char * argv[], uint32_t tcpRef, void *bufRef) {"%(struct_name)
  print "  %s_generated_show(argv,tcpRef,bufRef);"%(struct_name)
  print "}"
  print ""
  print "void %s_read(char * fname) {"%(struct_name)
  print "  %s_generated_read(fname);"%(struct_name)
  
  # Look for extra checks added when reading the conf
  if os.path.exists("%s_extra_checks.c"%(struct_name)):
    print ""
    print "  /*"
    print "  ** Add some consistency checks"
    print "  */"
    print "  %s_extra_checks();"%(struct_name)
  
  print "}"
    
  sys.stdout.close()
  sys.stdout = save_stdout 
  
#_______________________________________________
def go_build_struct(struct_name):
  print "void %s_read(char * fname);"%(struct_name)
  print ""

  print ""  
  print "/*_______________________________"
  print "** ENUM definion"
  print "*/"
  for obj in objects:
    if obj.genre =="ENUM":
      print "\n// %s "%(obj.name)
      print "typedef enum _%s_%s_e {"%(struct_name,obj.name)
      for value in obj.values.split(","):
         print "  %s_%s_%s,"%(struct_name,obj.name,value)
      print "} %s_%s_e;"%(struct_name,obj.name)
      print "// enum to string"    
      print "static inline char * %s_%s2String(%s_%s_e x) {"%(struct_name,obj.name,struct_name,obj.name)
      print "  switch(x) {"
      for value in obj.values.split(","):
         print "    case %s_%s_%s: return \"%s\";"%(struct_name,obj.name,value,value)
      print "    default: return \"?\";"
      print "  }"
      print "  return \"?\";"
      print "}"
      print "// string to enum"    
      print "static inline %s_%s_e string2%s_%s(const char * x) {"%(struct_name,obj.name,struct_name,obj.name)
      for value in obj.values.split(","):
        print "  if (strcmp(x,\"%s\")==0) return %s_%s_%s;"%(value,struct_name,obj.name,value)
      print "  return -1;" 
      print "}"     

  print ""  
  print "/*_______________________________"
  print "** %s structure"%(struct_name)
  print "*/"
                     
  print "typedef struct _%s_t {"%(struct_name)
  for module in modules:
    print ""  
    print "  /*"
    print "  ** %s scope configuration parameters"%(module)
    print "  */"
    print ""
    for obj in objects:
      if obj.module == module:     
        obj.write_in_struct(struct_name)    
  print "} %s_t;\n"%(struct_name)
  print "extern %s_t %s;"%(struct_name,struct_name)
  print ""  
  print "/*_______________________________"
  print "** ENUM macro"
  print "*/"
  for obj in objects:
    if obj.genre =="ENUM":
      print "// Read enum from configuration file"    
      print "static inline void %s_%s_READ_ENUM(config_t * cfg) {"%(struct_name.upper(),obj.name.upper())
      print "  const char * charval;"
      print "  %s.%s = -1;"%(struct_name,obj.name)
      print "  if (config_lookup_string(cfg, \"%s\", &charval) == CONFIG_TRUE) {"%(obj.name)
      print "    %s.%s = string2%s_%s(charval);"%(struct_name,obj.name,struct_name,obj.name)  
      print "  }"
      print "  if (%s.%s == -1) {"%(struct_name,obj.name)
      print "    %s.%s =  string2%s_%s(%s);"%(struct_name,obj.name,struct_name,obj.name,obj.default)
      print "  }"
      print "}"  
      print "// Set enum value thanks to rozodiag"    
      print "#define %s_%s_SET_ENUM(VAL)  {\\"%(struct_name.upper(),obj.name.upper())
      print "  int myval = string2%s_%s(VAL);\\"%(struct_name,obj.name)  
      print "  if (myval == -1) {\\"
      print "    pChar += rozofs_string_append_error(pChar,\" Unexpected enum value for %s : \");\\"%(obj.name)
      print "    pChar += rozofs_string_append_error(pChar,VAL);\\"
      print "  }\\"
      print "  else {\\"
      print "    %s.%s = myval;\\"%(struct_name,obj.name)
      print "    pChar += rozofs_string_append(pChar,\"%s\");\\"%(obj.name)
      print "    pChar += rozofs_string_append(pChar,\" set to value \");\\"
      print "    pChar += rozofs_string_append(pChar,VAL);\\"
      print "  }\\"         
      print "  pChar += rozofs_eol(pChar);\\"
      print "  return 0;\\"
      print "}" 
      print ""       

  #print "extern int             config_file_is_read;"
  #print "extern char            config_file_name[];"  

#_______________________________________________
def go_build_man(struct_name,command): 
  print "/*____________________________________________________________________________________________"
  print "**"
  print "** %s man function"%(struct_name)
  print "**"
  print "*/"
  print "void man_%s(char * pChar) {"%(struct_name)
  print "  pChar += rozofs_string_append_underscore(pChar,\"\\nUsage:\\n\");"
  print "  pChar += rozofs_string_append_bold(pChar,\"\\t%s [long]\");"%(command)
  print "  pChar += rozofs_string_append     (pChar,\"\\t\\tdisplays the whole %s configuration.\\n\");"%(struct_name)
  print "  pChar += rozofs_string_append_bold(pChar,\"\\t%s [long] <scope>\");"%(command)
  print "  pChar += rozofs_string_append     (pChar,\"\\tdisplays only the <scope> configuration part.\\n\");"
  print "  pChar += rozofs_string_append_bold(pChar,\"\\t%s search <parameter>\");"%(command)
  print "  pChar += rozofs_string_append     (pChar,\"\\tdisplays parameters approximatively like <parameter>.\\n\");"
  print "  pChar += rozofs_string_append_bold(pChar,\"\\t%s reload\");"%(command)
  print "  pChar += rozofs_string_append     (pChar,\"\\treloads and then displays the configuration.\\n\");"
  print "  pChar += rozofs_string_append_bold(pChar,\"\\t%s set <param> <value>\");"%(command)
  print "  pChar += rozofs_string_append     (pChar,\"\\tmodifies a configuration parameter in memory.\\n\");"
  print "  pChar += rozofs_string_append_bold(pChar,\"\\t%s save\");"%(command)
  print "  pChar += rozofs_string_append     (pChar,\"\\tsaves configuration from memory to disk.\\n\");"
  print "  pChar += rozofs_string_append_bold(pChar,\"\\t%s files\");"%(command)
  print "  pChar += rozofs_string_append     (pChar,\"\\tReturns the name of the configuration file and the saved ones.\\n\");"
  print "}"

  
#_______________________________________________
def build_show_module(module,struct_name): 

  print "/*____________________________________________________________________________________________"
  print "**"
  print "** %s scope configuration parameters"%(module)
  print "**"
  print "*/"
  print "char * show_%s_module_%s(char * pChar) {"%(struct_name,module)  
  print ""
  print "  pChar += rozofs_string_append_effect(pChar,\"#                                                            \\n#     \", ROZOFS_COLOR_BLUE ROZOFS_COLOR_BOLD ROZOFS_COLOR_REVERSE);"    
  print "  pChar += rozofs_string_append_effect(pChar,\"%-50s\", ROZOFS_COLOR_YELLOW ROZOFS_COLOR_BOLD ROZOFS_COLOR_REVERSE);"%("    %s SCOPE CONFIGURATION PARAMETERS"%(module.upper()))
  print "  pChar += rozofs_string_append_effect(pChar,\"     \\n#                                                            \\n\\n\", ROZOFS_COLOR_BLUE ROZOFS_COLOR_BOLD ROZOFS_COLOR_REVERSE);"      
  for obj in objects:
    if obj.module == module:     
      print ""
      obj.write_in_show(struct_name) 
  print "  return pChar;"
  print "}"

  print "/*____________________________________________________________________________________________"
  print "**"
  print "** %s scope configuration parameters"%(module)
  print "**"
  print "*/"
  print "char * show_%s_module_%s_short(char * pChar) {"%(struct_name,module)  
  print ""
  print "  pChar += rozofs_string_append_effect(pChar,\"#                                                            \\n#     \", ROZOFS_COLOR_BLUE ROZOFS_COLOR_BOLD ROZOFS_COLOR_REVERSE);"    
  print "  pChar += rozofs_string_append_effect(pChar,\"%-50s\", ROZOFS_COLOR_YELLOW ROZOFS_COLOR_BOLD ROZOFS_COLOR_REVERSE);"%("    %s SCOPE CONFIGURATION PARAMETERS"%(module.upper()))
  print "  pChar += rozofs_string_append_effect(pChar,\"     \\n#                                                            \\n\\n\", ROZOFS_COLOR_BLUE ROZOFS_COLOR_BOLD ROZOFS_COLOR_REVERSE);"      
  for obj in objects:
    if obj.module == module:     
      print ""
      obj.write_in_save(struct_name) 
  print "  return pChar;"
  print "}"

#_______________________________________________
def go_build_show_modules(struct_name): 

  for module in modules:
    build_show_module(module,struct_name)

#_______________________________________________
def build_save_module(module,struct_name): 
  print "/*____________________________________________________________________________________________"
  print "**"
  print "** %s scope configuration parameters"%(module)
  print "**"
  print "*/"
  print "char * save_%s_module_%s(char * pChar) {"%(struct_name,module)
  print ""
  print "  pChar += rozofs_string_append(pChar,\"#____________________________________________________________\\n\");"    
  print "  pChar += rozofs_string_append(pChar,\"# \");"
  print "  pChar += rozofs_string_append(pChar,\"%s\");"%(module)
  print "  pChar += rozofs_string_append(pChar,\" scope configuration parameters\\n\");"   
  print "  pChar += rozofs_string_append(pChar,\"#____________________________________________________________\\n\\n\");"    
  for obj in objects:
    if obj.module == module:     
      obj.write_in_save(struct_name) 
  print "  return pChar;"
  print "}"
#_______________________________________________
def go_build_save_modules(struct_name): 

  for module in modules:
    build_save_module(module,struct_name)
#_______________________________________________
def go_build_show_files(struct_name): 
  print "/*____________________________________________________________________________________________"
  print "**"
  print "** %s diagnostic function"%(struct_name)
  print "**"
  print "*/"
  print "char * %s_generated_show_all_files(char * pChar) {"%(struct_name)
  print "  char            cmd[256];"
  print ""
  print "  if (%s_file_is_read==0) {"%(struct_name)
  print "    pChar += rozofs_string_append_error(pChar,\"Can not read configuration file \");"
  print "    return pChar;"
  print "  }"
  print "  sprintf(cmd,\"ls -lisa %%s*\",%s_file_name);"%(struct_name)
  print "  uma_dbg_run_system_cmd(cmd, pChar, uma_dbg_get_buffer_len()); " 
  print "  return pChar;"
  print "}"    
#_______________________________________________
def go_build_show(struct_name): 

  print "/*____________________________________________________________________________________________"
  print "**"
  print "** %s diagnostic function"%(struct_name)
  print "**"
  print "*/"
  print "void %s_generated_show(char * argv[], uint32_t tcpRef, void *bufRef) {"%(struct_name)
  print "char *pChar = uma_dbg_get_buffer();"
  print "char *pHead;"
  print "int     longformat = 0;"
  print "char  * moduleName = NULL;"
  print ""
  print "  if (argv[1] != NULL) {"
  print ""
  print "    if (strcmp(argv[1],\"reload\")==0) {"  
  print "      %s_read(NULL);"%(struct_name)
  print "      pChar += rozofs_string_append(pChar, \"File reloaded\\n\");"
  print "      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());"
  print "      return;"   
  print "    }"
  print ""
  print "    if (strcmp(argv[1],\"set\")==0) {"
  print "      if ((argv[2] == NULL)||(argv[3] == NULL)) {"
  print "        pChar += rozofs_string_append_error(pChar, \"Missing <parameter> and/or <value>\\n\");"
  print "        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());"
  print "        return;"   
  print "      }"  
  print "      %s_generated_set(pChar, argv[2],argv[3]);"%(struct_name)
  print "      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());"
  print "      return;"           
  print "    }" 
  print ""
  print "    if (strcmp(argv[1],\"search\")==0) {"
  print "      if (argv[2] == NULL) {"
  print "        pChar += rozofs_string_append_error(pChar, \"Missing <parameter>\\n\");"
  print "        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());"
  print "        return;"   
  print "      }"  
  print "      %s_generated_search(pChar, argv[2]);"%(struct_name)
  print "      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());"
  print "      return;"           
  print "    }" 
  print ""
  print "    if (strcmp(argv[1],\"save\")==0) {"
  print "      %s_generated_save(pChar);"%(struct_name)
  print "      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());"
  print "      return;"           
  print "    }" 
  print ""  
  print "    if (strcmp(argv[1],\"files\")==0) {"
  print "      %s_generated_show_all_files(pChar);"%(struct_name)
  print "      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());"
  print "      return;"           
  print "    }" 
  print ""
  print "    if (strcmp(argv[1],\"long\")==0) {"
  print "      longformat = 1;"
  print "      moduleName = argv[2];"
  print "    }" 
  print "    else {"
  print "      moduleName = argv[1];"
  print "      if (argv[2] != NULL) {"
  print "        if (strcmp(argv[2],\"long\")==0) {"
  print "          longformat = 1;"
  print "        }" 
  print "      }"
  print "    }"
  print ""
  print "    if (moduleName != NULL) {"
  first = True
  for module in modules:
    if first == True:
      print "      if (strcasecmp(\"%s\",moduleName)==0) {"%(module)
      first=False
    else:
      print "      else if (strcasecmp(\"%s\",moduleName)==0) {"%(module)
    print "        if ((pHead = (char *)ruc_buf_getPayload(bufRef)) == NULL) {"
    print "          severe( \"ruc_buf_getPayload(%p)\", bufRef );"
    print "          return;"
    print "        }"             
    print "        /*"
    print "        ** Set the command recall string"
    print "        */"
    print "        pChar = uma_dbg_cmd_recall((UMA_MSGHEADER_S *)pHead);"   
    print "        if (longformat) {"
    print "          pChar = show_%s_module_%s(pChar);"%(struct_name,module)  
    print "        } else {"
    print "          pChar = show_%s_module_%s_short(pChar);"%(struct_name,module)  
    print "        } "
    print "        uma_dbg_send_buffer(tcpRef, bufRef, pChar-pHead, TRUE);"
    print "        return;"           
    print "      }"
  print "      else {"
  print "        pChar += rozofs_string_append_error(pChar, \"Unexpected configuration scope\\n\");"
  print "        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());"
  print "        return;"           
  print "      }"
  print "    }"
  print "  }"  
  print ""
  print "  if ((pHead = (char *)ruc_buf_getPayload(bufRef)) == NULL) {"
  print "    severe( \"ruc_buf_getPayload(%p)\", bufRef );"
  print "    return;"
  print "  }"             
  print "  /*"
  print "  ** Set the command recall string"
  print "  */"
  print "  pChar = uma_dbg_cmd_recall((UMA_MSGHEADER_S *)pHead);"     
  print "  if (%s_file_is_read==0) {"%(struct_name)
  print "    pChar += rozofs_string_append_error(pChar,\"Can not read configuration file \");"    
  print "  }"
  print "  "    
  for module in modules:
    print "  "    

    print "  if (longformat) {"
    print "    pChar = show_%s_module_%s(pChar);"%(struct_name,module)  
    print "  } else {"
    print "    pChar = show_%s_module_%s_short(pChar);"%(struct_name,module)  
    print "  } "
    print "  uma_dbg_send_buffer(tcpRef, bufRef, pChar-pHead, FALSE);"
    print "  "
    print "  bufRef = uma_dbg_get_new_buffer(tcpRef);"
    print "  if (bufRef == NULL) {"
    print "    warning( \"uma_dbg_get_new_buffer() Buffer depletion\");"
    print "    return;"    
    print "  }"
    print "  if ((pHead = (char *)ruc_buf_getPayload(bufRef)) == NULL) {"
    print "    severe( \"ruc_buf_getPayload(%p)\", bufRef );"
    print "    return;"
    print "  }"             
    print "  pChar = pHead+sizeof(UMA_MSGHEADER_S);"
    print "  *pChar = 0;"                                
  print "  pChar += rozofs_string_append(pChar,\"#____________________________________________________________\\n\");"    
  print "  pChar += rozofs_string_append(pChar,\"# \");"
  print "  pChar += rozofs_string_append(pChar,\" %s file is \");"%(struct_name)
  print "  pChar += rozofs_string_append(pChar,%s_file_name);"%(struct_name)
  print "  pChar += rozofs_string_append(pChar,\"\\n#____________________________________________________________\\n\\n\");"    
  print "  uma_dbg_send_buffer(tcpRef, bufRef, pChar-pHead, TRUE);"
  print "  return;"     
  print "}"
#_______________________________________________
def go_build_set(file_name,struct_name):   
  print "/*____________________________________________________________________________________________"
  print "**"
  print "** Set a value to a common configuration parameter"
  print "** "
  print "** @param param   Parameter name"
  print "** @param value   New value to set"
  print "** "
  print "** @retval 1 on success, 0 else"    
  print "*/"
  print "static inline int %s_generated_set(char * pChar, char *parameter, char *value) {"%(struct_name)

  for obj in objects:
    obj.set(struct_name) 
  print "  pChar += rozofs_string_append_error(pChar,\"No such parameter \");"
  print "  pChar += rozofs_string_append_error(pChar,parameter);"
  print "  pChar += rozofs_eol(pChar);\\"
  print "  return -1;"  
  print "}"  
#_______________________________________________
def go_build_search(file_name,struct_name):   
  print "/*____________________________________________________________________________________________"
  print "**"
  print "** Search for parameters approximatively like a given string "
  print "** "
  print "** @param parameter   Approximative parameter name we are searching for"
  print "** "
  print "** @retval The number of parameters matching the input string"    
  print "*/"
  print "static inline int %s_generated_search(char * pChar, char *parameter) {"%(struct_name)
  print "  int match = 0;"
  
  for obj in objects:
    print ""
    obj.search(struct_name) 
  print "  if (match == 0) {"  
  print "    pChar += rozofs_string_append_error(pChar,\"No such parameter like \");"
  print "    pChar += rozofs_string_append_error(pChar,parameter);"
  print "    pChar += rozofs_eol(pChar);\\"
  print "  }"  
  print "  return match;"  
  print "}"    
#_______________________________________________
def go_build_save(file_name,struct_name):   
  print "/*____________________________________________________________________________________________"
  print "**"
  print "** Save configuration parameter on disk"
  print "** "
  print "** @param pChar   Parameter name"
  print "** @param value   New value to set"
  print "** "
  print "** @retval 1 on success, 0 else"    
  print "*/"
  print "static inline int %s_generated_save(char * pChar) {"%(struct_name)
  print "  char *pBuff;"
  print "  int   fd;"
  print "  char  saved_file[256];"
  print ""
  print "  /*"
  print "  ** Save previous file"
  print "  */"  
  print "  time_t t = time(NULL);"
  print "  struct tm tm = *localtime(&t);"
  print "  sprintf(saved_file,\"%%s_%%2.2d-%%2.2d-%%2.2d_%%2.2d:%%2.2d:%%2.2d\", %s_file_name,tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);"%(struct_name)
  print "  rename(%s_file_name,saved_file);"%(struct_name)
  print ""
  print "  /*"
  print "  ** Creat a new file"
  print "  */"
  print "  fd = open(%s_file_name,O_CREAT|O_TRUNC|O_APPEND|O_WRONLY,0777);"%(struct_name)
  print "  if (fd < 0) {"
  print "    pChar += rozofs_string_append_error(pChar,\"Can not open \");"
  print "    pChar += rozofs_string_append_error(pChar,%s_file_name);"%(struct_name)
  print "    return -1;"  
  print "  }"
  for module in modules:
    print "  pBuff = save_%s_module_%s(myBigBuffer);"%(struct_name,module)
    print "  if (write(fd,myBigBuffer,pBuff-myBigBuffer)<0) {"
    print "    pChar += rozofs_string_append_error(pChar,\"Can not write \");"
    print "    pChar += rozofs_string_append_error(pChar,%s_file_name);"%(struct_name)
    print "    close(fd);"
    print "    return -1;"
    print "  }" 
  print "  pChar += rozofs_string_append(pChar,\"Saved in \");"
  print "  pChar += rozofs_string_append(pChar,%s_file_name);"%(struct_name)
  print "  pChar += rozofs_eol(pChar);"
  print "  close(fd);"
  print "  return 0;"    
  print "}" 
#_______________________________________________
def go_build_read(file_name,struct_name,command):   
  print "/*____________________________________________________________________________________________"
  print "**"
  print "** Read the configuration file"
  print "*/"
  print "static inline void %s_generated_read(char * fname) {"%(struct_name)
  print "  config_t          cfg; "
  print ""
  print "  if (%s_file_is_read == 0) {"%(struct_name)
  print "    uma_dbg_addTopicAndMan(\"%s\",show_%s, man_%s, 0);"%(command,struct_name,struct_name)
  print "    if (fname == NULL) {"
  print "      strcpy(%s_file_name,ROZOFS_CONFIG_DIR\"/%s\");"%(struct_name,file_name)
  print "    }"
  print "    else {"
  print "      strcpy(%s_file_name,fname); "%(struct_name)
  print "    } "
  print "  }"
  print ""
  print "  if (access(%s_file_name,R_OK)!=0) {"%(struct_name)
  print "    printf(\"cant access %%s: %%s.\", %s_file_name, strerror(errno));"%(struct_name)  
  print "    fatal(\"cant access %%s: %%s.\", %s_file_name, strerror(errno));"%(struct_name)
  print "  }"
  print ""     
  print "  config_init(&cfg);"
  print "  %s_file_is_read = 1;"%(struct_name)
  print "  if (config_read_file(&cfg, %s_file_name) == CONFIG_FALSE) {"%(struct_name)
  print "    if (errno == ENOENT) {"
  print "      info(\"Missing file %%s.\", %s_file_name);"%(struct_name)
  print "    }"
  print "    else {"
  print "      severe(\"cant read %%s: %%s (line %%d).\", %s_file_name, config_error_text(&cfg),config_error_line(&cfg));"%(struct_name)
  print "    }"	    
  print "    %s_file_is_read = 0;"%(struct_name)	    
  print "  }"
  print ""


  for module in modules:
    print "  /*"
    print "  ** %s scope configuration parameters"%(module)
    print "  */"
    for obj in objects:
      if obj.module == module:     
        obj.read(struct_name) 

  print " "
  print "  config_destroy(&cfg);"
  print "}" 
  

#_______________________________________________________________________________
#
# config_generate : Generate code for one configuration file
#
# @param file_name           The config file name used on site by RozoFS software 
#                            to save the configuration.
#
# @param input_file_name     relative path from tool directory of input file 
#                            (with .input suffix) describing the configuration
#                            parameters (type, usage and possible values).
#
# @param cli_name            The CLI name to register in rozodiag interface
#                            for managing the configuration.
#_______________________________________________________________________________
def config_generate(file_name, input_file_name, cli_name):
  global objects
  global modules

  objects = []
  modules = []

  #
  # Resolve path of the tools directory
  #
  TOOLS_DIR = os.path.dirname( os.path.realpath( __file__ ) )
  
  print "Re-generate code for %-20s from %s"%(file_name,os.path.abspath(TOOLS_DIR+'/'+input_file_name))
  
  #
  # Split input file name in relative_path and file name
  #
  relative_path  = os.path.dirname(input_file_name)
  struct_name    = os.path.basename(input_file_name)
  struct_name    = struct_name.split('.')[0]
  
  string="%s/%s"%(TOOLS_DIR,relative_path)
  os.chdir(string)
    
 
  go_read_input_file("%s.input"%(struct_name))

  # Generate structure header file
  start_header_file(struct_name,struct_name)
  go_build_struct(struct_name)
  end_file(struct_name) 

  # Generate read and show function
  start_header_file("%s_read_show"%(struct_name),struct_name)
  go_build_set(file_name,struct_name)
  go_build_search(file_name,struct_name)
  go_build_man(struct_name,cli_name)
  go_build_show_modules(struct_name)
  go_build_save_modules(struct_name)
  go_build_save(file_name,struct_name)
  go_build_show_files(struct_name)
  go_build_show(struct_name)
  go_build_read(file_name,struct_name,cli_name)
  end_file(struct_name)
  
  # Generate C file
  go_build_cfile(struct_name)
  go_build_cfile

