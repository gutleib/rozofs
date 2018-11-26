/*
  Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
  This file is part of Rozofs.

  Rozofs is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published
  by the Free Software Foundation, version 2.

  Rozofs is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see
  <http://www.gnu.org/licenses/>.
 */

/*
**   I N C L U D E  F I L E S
*/

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "rozofs_throughput.h"

static uint32_t COLS=1;
static uint32_t LINES=60;
static int      AVERAGE=0;
static int      PERSTEP=1;

typedef uint64_t     colaverage_t[15];

#define              ROZOFS_MAX_DISPLAY_COUNTER 16
colaverage_t         elementaverage[ROZOFS_MAX_DISPLAY_COUNTER];

 
/*_______________________________________________________________________
* Request for througput per second unit
* i.e when display is done per minute step, the display throughput is a
*     throughput per second
*/
void rozofs_thr_display_throughput_per_sec() {
  PERSTEP = 0;
}
/*_______________________________________________________________________
* Request for througput per display step unit
* i.e when display is done per minute step, the display throughput is a
*     throughput per minute
*/
void rozofs_thr_display_throughput_per_step() {
  PERSTEP = 1;
}
/*
*_______________________________________________________________________
* Request for average display at the end of the colums
*
*/
void rozofs_thr_set_average() {
  AVERAGE = 1;
}
/*_______________________________________________________________________
* Change the number of columns of the display
*
* @param columns The number of columns per minutes
*/
void rozofs_thr_set_column(int columns) {
  switch(columns) {
    case 1:
    case 2:
    case 3:
    case 4:
    case 5:
    case 6:
    case 10:
    case 12:
      COLS = columns;
      LINES=60/COLS;      
      break;
    case 7:
      COLS  = 7;
      LINES = 9;      
      break;
    case 8:
      COLS  = 8;
      LINES = 8;      
      break;
    case 9:
      COLS  = 9;
      LINES = 7;      
      break;
    case 11:
      COLS  = 11;
      LINES = 5;      
      break;
    case 13:
      COLS  = 13;
      LINES = 4;      
      break;
    case 14:
      COLS  = 14;
      LINES = 4;      
      break;
    case 15:
      COLS  = 15;
      LINES = 4;      
      break;       
    default: 
      COLS  = 6;
      LINES = 10;      
    break;
  }    
}
/*_______________________________________________________________________
* Make a string from a unit
*
* @param unit     Unit value
*
* @retval unit string
*/
char * rozofs_thr_unit2string(rozofs_thr_unit_e unit) {
  switch(unit) {
    case rozofs_thr_unit_second: return "second";
    case rozofs_thr_unit_minute: return "minute";
    case rozofs_thr_unit_hour:   return "hour";
    default: return "?";
  }
  return "??";
}    
/*_______________________________________________________________________
* Display throughput counters
*
* @param pChar    Where to format the ouput
* @param pChar    The counters
* @param nb       The number of counters
* @param unit     The unit to display
*/
char * rozofs_thr_display_unit(char * pChar, rozofs_thr_cnts_t * counters[], int nb, rozofs_thr_unit_e unit) {
  uint32_t t;
  struct timeval tv;
  int    rank;
  int    idx,line,col;
  rozofs_thr_1_cnt_t *p;

  int      value;
  uint64_t count;
  char    * unitDisplay;
  char    * unitString;

  if (AVERAGE) {
    memset(elementaverage,0, ROZOFS_MAX_DISPLAY_COUNTER*sizeof(colaverage_t));
  }

  if (counters==NULL) {
    pChar += rozofs_string_append(pChar,"Counters not initialized\n");
    return pChar;
  }   
  for (value=0; value< nb; value++) {
    if (counters[value] == NULL) {
      pChar += rozofs_string_append(pChar,"Counters not initialized\n");
      return pChar;
    }
  }
     
  if (unit == rozofs_thr_unit_all) {
    pChar = rozofs_thr_display_unit(pChar, counters, nb, rozofs_thr_unit_hour);
    pChar = rozofs_thr_display_unit(pChar, counters, nb, rozofs_thr_unit_minute);
    pChar = rozofs_thr_display_unit(pChar, counters, nb, rozofs_thr_unit_second);
    return pChar;
  }   
     
//  LINES=60/COLS;
    
  gettimeofday(&tv,(struct timezone *)0);
  switch(unit) {  
    case rozofs_thr_unit_second: 
      unitString = "SECOND";
      unitDisplay = "| SEC |";
      t = tv.tv_sec-1;
      break; 

    case rozofs_thr_unit_minute: 
      unitString = "MINUTE";    
      unitDisplay = "| MIN |";
      t = (tv.tv_sec/60)-1;
      break; 

    case rozofs_thr_unit_hour: 
      unitString = "HOUR";    
      unitDisplay = "| HR  |";
      t = (tv.tv_sec/3600)-1;
      break;
    
    default:
      pChar += rozofs_string_append(pChar,"No such unit ");
      pChar += rozofs_u32_append(pChar,unit);
      pChar += rozofs_string_append(pChar," !!!\n");
      return pChar;         
  } 
  pChar += rozofs_string_append(pChar,"___ ");
  pChar += rozofs_time2string(pChar,t);
  pChar += rozofs_string_append(pChar," ___ THROUGHPUT PER ");
  /*
  ** Pers step or per second unit display
  */
  if (PERSTEP) {
    pChar += rozofs_string_append_bold(pChar,unitString);
  }
  else {
     pChar += rozofs_string_append_bold(pChar,"SECOND");
  }
  pChar += rozofs_string_append(pChar," HISTORY WITH A 1 ");  
  pChar += rozofs_string_append_bold(pChar,unitString);
  pChar += rozofs_string_append(pChar," STEP ___\n");  
   
  rank = t % ROZOFS_THR_CNTS_NB;  

  for (col=0; col<COLS; col++) {
    pChar += rozofs_string_append(pChar," _____ ");
    for (value=0; value< nb; value++) {
      pChar += rozofs_string_append(pChar,"_________ ");
    }  
  }  
  pChar += rozofs_eol(pChar);
 
  for (col=0; col<COLS; col++) {  
    pChar += rozofs_string_append(pChar,unitDisplay);
    for (value=0; value< nb; value++) {  
      pChar += rozofs_string_append(pChar," ");    
      pChar += rozofs_string_padded_append(pChar, 8, rozofs_left_alignment, counters[value]->name);
      pChar += rozofs_string_append(pChar,"|");    
    }  
  }  
  pChar += rozofs_eol(pChar);

  for (col=0; col<COLS; col++) {  
    pChar += rozofs_string_append(pChar,"|_____|");
    for (value=0; value< nb; value++) {  
      pChar += rozofs_string_append(pChar,"_________|");  
    }  
  }  
  pChar += rozofs_eol(pChar);
     
  
  for (line=0; line< LINES; line++) {
    
    for (col=0; col<COLS; col++) {
    
      idx = (ROZOFS_THR_CNTS_NB+rank-line-(col*LINES))%ROZOFS_THR_CNTS_NB;

      pChar += rozofs_string_append(pChar,"|");
      pChar += rozofs_i32_padded_append(pChar, 4, rozofs_right_alignment, -1-line-(col*LINES));
      pChar += rozofs_string_append(pChar," |");

      for (value=0; value< nb; value++) {  
        switch(unit) {
          case rozofs_thr_unit_second: 
            p = &(counters[value]->second[idx]);
	    if (p->ts != (t-line-(col*LINES))) p->count = 0;
            count = p->count;	
            break; 
          case rozofs_thr_unit_minute: 
            p = &(counters[value]->minute[idx]);
            if (p->ts != (t-line-(col*LINES))) p->count = 0;	
            count = p->count;	
            if (PERSTEP==0) count /= 60;
            break; 

          case rozofs_thr_unit_hour: 
            p = &(counters[value]->hour[idx]);
            if (p->ts != (t-line-(col*LINES))) p->count = 0;
            count = p->count;	
            if (PERSTEP==0) count /= 3600;
            break;    
             
          default:
            pChar += rozofs_string_append(pChar,"No such unit ");
            pChar += rozofs_u32_append(pChar,unit);
            pChar += rozofs_string_append(pChar," !!!\n");
            return pChar;               
        }  
	pChar += rozofs_string_append(pChar," ");	
	pChar += rozofs_bytes_padded_append(pChar,7, count);
	if (AVERAGE) elementaverage[value][col] += count;
	pChar += rozofs_string_append(pChar," |");
      }    
    }
    pChar += rozofs_eol(pChar);  
  }
  
  for (col=0; col<COLS; col++) {  
    pChar += rozofs_string_append(pChar,"|_____|");
    for (value=0; value< nb; value++) {  
      pChar += rozofs_string_append(pChar,"_________|");
    }  
  }  
  pChar += rozofs_eol(pChar);
  
  if (AVERAGE) {
    for (col=0; col<COLS; col++) {
      pChar += rozofs_string_append(pChar,"| Avg |");
      for (value=0; value< nb; value++) {  	
	pChar += rozofs_string_append(pChar," ");	
	pChar += rozofs_bytes_padded_append(pChar,7, elementaverage[value][col]/LINES);
	pChar += rozofs_string_append(pChar," |");
      }
    }
    pChar += rozofs_eol(pChar);
    for (col=0; col<COLS; col++) {  
      pChar += rozofs_string_append(pChar,"|_____|");
      for (value=0; value< nb; value++) {  
	pChar += rozofs_string_append(pChar,"_________|");
      }  
    }           
  }
  pChar += rozofs_eol(pChar);  
  
  PERSTEP = 1;
  AVERAGE = 0;
  return pChar;    
}
/*_______________________________________________________________________
* Display throughput counters
*
* @param pChar    Where to format the ouput
* @param pChar    The counters
* @param nb       The number of counters
* @param unit     The unit to display
*/
char * rozofs_thr_display_bitmask(char * pChar, rozofs_thr_cnts_t * counters[], uint32_t bitmask, rozofs_thr_unit_e unit) {
  uint32_t t;
  struct timeval tv;
  int    rank;
  int    idx,line,col;
  rozofs_thr_1_cnt_t *p;

  int      value;
  uint64_t count;
  char    * unitDisplay;
  char    * unitString;

  if (AVERAGE) {
    memset(elementaverage,0, ROZOFS_MAX_DISPLAY_COUNTER*sizeof(colaverage_t));
  }

  if (counters==NULL) {
    pChar += rozofs_string_append(pChar,"Counters not initialized\n");
    return pChar;
  }   
     
  if (unit == rozofs_thr_unit_all) {
    unit = rozofs_thr_unit_second;
  }   
     
//  LINES=60/COLS;
    
  gettimeofday(&tv,(struct timezone *)0);
  switch(unit) {  
    case rozofs_thr_unit_second: 
      unitString = "SECOND";
      unitDisplay = "| SEC |";
      t = tv.tv_sec-1;
      break; 

    case rozofs_thr_unit_minute: 
      unitString = "MINUTE";    
      unitDisplay = "| MIN |";
      t = (tv.tv_sec/60)-1;
      break; 

    case rozofs_thr_unit_hour: 
      unitString = "HOUR";    
      unitDisplay = "| HR  |";
      t = (tv.tv_sec/3600)-1;
      break;
    
    default:
      pChar += rozofs_string_append(pChar,"No such unit ");
      pChar += rozofs_u32_append(pChar,unit);
      pChar += rozofs_string_append(pChar," !!!\n");
      return pChar;         
  } 
  pChar += rozofs_string_append(pChar,"___ ");
  pChar += rozofs_time2string(pChar,t);
  pChar += rozofs_string_append(pChar," ___ COUNTER PER ");
  /*
  ** Pers step or per second unit display
  */
  if (PERSTEP) {
    pChar += rozofs_string_append_bold(pChar,unitString);
  }
  else {
     pChar += rozofs_string_append_bold(pChar,"SECOND");
  }
  pChar += rozofs_string_append(pChar," HISTORY WITH A 1 ");  
  pChar += rozofs_string_append_bold(pChar,unitString);
  pChar += rozofs_string_append(pChar," STEP ___\n");  
   
  rank = t % ROZOFS_THR_CNTS_NB;  

  for (col=0; col<COLS; col++) {
    pChar += rozofs_string_append(pChar," _____ ");
    for (value=0; value< ROZOFS_MAX_DISPLAY_COUNTER; value++) { 
      if ((bitmask & (1<<value)) == 0) continue; 
      pChar += rozofs_string_append(pChar,"________ ");
    }  
  }  
  pChar += rozofs_eol(pChar);
 
  for (col=0; col<COLS; col++) {  
    pChar += rozofs_string_append(pChar,unitDisplay);
    for (value=0; value< ROZOFS_MAX_DISPLAY_COUNTER; value++) { 
      if ((bitmask & (1<<value)) == 0) continue; 
      pChar += rozofs_string_append(pChar," ");    
      pChar += rozofs_string_padded_append(pChar, 7, rozofs_left_alignment, counters[value]->name);
      pChar += rozofs_string_append(pChar,"|");    
    }  
  }  
  pChar += rozofs_eol(pChar);

  for (col=0; col<COLS; col++) {  
    pChar += rozofs_string_append(pChar,"|_____|");
    for (value=0; value< ROZOFS_MAX_DISPLAY_COUNTER; value++) { 
      if ((bitmask & (1<<value)) == 0) continue; 
      pChar += rozofs_string_append(pChar,"________|");  
    }  
  }  
  pChar += rozofs_eol(pChar);
     
  
  for (line=0; line< LINES; line++) {
    
    for (col=0; col<COLS; col++) {
    
      idx = (ROZOFS_THR_CNTS_NB+rank-line-(col*LINES))%ROZOFS_THR_CNTS_NB;

      pChar += rozofs_string_append(pChar,"|");
      pChar += rozofs_i32_padded_append(pChar, 4, rozofs_right_alignment, -1-line-(col*LINES));
      pChar += rozofs_string_append(pChar," |");

      for (value=0; value< ROZOFS_MAX_DISPLAY_COUNTER; value++) { 
        if ((bitmask & (1<<value)) == 0) continue; 
        switch(unit) {
          case rozofs_thr_unit_second: 
            p = &(counters[value]->second[idx]);
	    if (p->ts != (t-line-(col*LINES))) p->count = 0;
            count = p->count;	
            break; 
          case rozofs_thr_unit_minute: 
            p = &(counters[value]->minute[idx]);
            if (p->ts != (t-line-(col*LINES))) p->count = 0;	
            count = p->count;	
            if (PERSTEP==0) count /= 60;
            break; 

          case rozofs_thr_unit_hour: 
            p = &(counters[value]->hour[idx]);
            if (p->ts != (t-line-(col*LINES))) p->count = 0;
            count = p->count;	
            if (PERSTEP==0) count /= 3600;
            break;    
             
          default:
            pChar += rozofs_string_append(pChar,"No such unit ");
            pChar += rozofs_u32_append(pChar,unit);
            pChar += rozofs_string_append(pChar," !!!\n");
            return pChar;               
        }  
	pChar += rozofs_string_append(pChar," ");	
	pChar += rozofs_count_padded_append(pChar,6, count);
	if (AVERAGE) elementaverage[value][col] += count;
	pChar += rozofs_string_append(pChar," |");
      }    
    }
    pChar += rozofs_eol(pChar);  
  }
  
  for (col=0; col<COLS; col++) {  
    pChar += rozofs_string_append(pChar,"|_____|");
    for (value=0; value< ROZOFS_MAX_DISPLAY_COUNTER; value++) {  
      if ((bitmask & (1<<value)) == 0) continue; 
      pChar += rozofs_string_append(pChar,"________|");
    }  
  }  
  pChar += rozofs_eol(pChar);
  
  if (AVERAGE) {
    for (col=0; col<COLS; col++) {
      pChar += rozofs_string_append(pChar,"| Avg |");
      for (value=0; value<ROZOFS_MAX_DISPLAY_COUNTER; value++) {    
        if ((bitmask & (1<<value)) == 0) continue; 	
	pChar += rozofs_string_append(pChar," ");	
	pChar += rozofs_count_padded_append(pChar,6, elementaverage[value][col]/LINES);
	pChar += rozofs_string_append(pChar," |");
      }
    }
    pChar += rozofs_eol(pChar);
    for (col=0; col<COLS; col++) {  
      pChar += rozofs_string_append(pChar,"|_____|");
      for (value=0; value< ROZOFS_MAX_DISPLAY_COUNTER; value++) {  
        if ((bitmask & (1<<value)) == 0) continue; 
	pChar += rozofs_string_append(pChar,"________|");
      }  
    }           
  }
  pChar += rozofs_eol(pChar);  
  
  PERSTEP = 1;
  AVERAGE = 0;
  return pChar;    
}
