#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

red = '\033[91m'
green = '\033[92m'
yellow = '\033[93m'
blue = '\033[94m'
purple = '\033[95m'
cyan = '\033[96m'
white = '\033[97m'
endeffect = '\033[0m'
bold = '\033[1m'
underline = '\033[4m'
blink = '\033[5m'
reverse = '\033[7m'


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

  def __init__(self,text,effect=None):  
    self.text   = text
    self.effect = effect

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
    
    if self.effect != None:
      line+=self.effect+bold+reverse
    for i in range(start): line+=" "
    line+=self.text
    for i in range(end): line+=" " 
    line+=endeffect
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
class display_element:

  def __init__(self,value,effect=None):  
    self.value  = value 
    self.effect = effect
     

#_______________________________________________
class display_line:

  def __init__(self,centered=False):  
    self.column     = []   
    self.centered   = centered
      
  def set_column(self,column,value,effect=None):
    # Extend column number
    if int(column) > len(self.column):
      for i in range(len(self.column),int(column)):
        self.column.append(display_element(''))
    self.column[int(column)-1] = display_element(value,effect)

  def check_column(self,column,value):
    # Extend column number
    if int(column) > len(self.column): return False
    if self.column[int(column)-1].value == value: return True
    return False
    
  # Join a colum with its preceding column  
  def join_preceding_column(self,column):
    const = constants()
    # Extend column number
    if int(column) > len(self.column):
      for i in range(len(self.column),int(column)):
        self.column.append(display_element(''))
    self.column[int(column)-1] = display_element(const.joined_column())

  def display(self,column_desc):
    const = constants()
    line=''	
    for i in range(int(column_desc.shift)): line+=' '		
    line+="| "
    for col in range(column_desc.column_nb):
    
      try:     elt=self.column[col]
      except:  elt=display_element('')	
      
      val = elt.value
      eff = elt.effect
      
      if val == const.joined_column(): continue

      l = column_desc.column_sizes[col]-len(val)
      joined = 0
      for jc in range(col+1,column_desc.column_nb):
        try:    next = self.column[jc].value
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
          
      if eff != None: line+=eff+bold+reverse
      for i in range(start): line+=" "
      line+=val
      for i in range(end): line+=" " 
      line+=endeffect
      line+=" | "  
      col+=joined 
    print line
        
#_______________________________________________
class adaptative_tbl:

  def __init__(self, shift, title=None,effect=None):  
    self.row_nb      = int(0)
    self.row         = [] 
    self.current_row = None 
    self.column_desc = column_desc(shift)   
    if title == None: 
      self.separator(' ',' ')      
    else:
      self.separator(' ','_')
      self.row.append(big_title(title,effect)) 
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
         
  def set_column(self,column,value,effect=None):
    self.current_row.set_column(column,value,effect)
    self.column_desc.update_column(column,len(value))      

  def join_preceding_column(self,column):
    self.current_row.join_preceding_column(column)
                	
  def display(self):
    # Must we add and end separator ?
    if self.current_row != None: self.end_separator()  
    for row in range(int(self.row_nb)):              
      self.row[row].display(self.column_desc)
      previous_line=self.row[row]

