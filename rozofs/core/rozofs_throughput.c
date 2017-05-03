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
static int      AVERAGE=0;

typedef uint64_t     colaverage_t[6];
colaverage_t         elementaverage[30];

 

/*
*_______________________________________________________________________
* Request for average display at the end of the colums
*
* @param average wether average is requested or not
*/
void rozofs_thr_set_average(int average) {
  AVERAGE = average;
}
/*_______________________________________________________________________
* Change the number of columns of the display
*
* @param columns The number of columns per minutes
*/
void rozofs_thr_set_column(int columns) {
  if ((columns>0)&&(columns<=6)) COLS = columns;
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
  uint32_t LINES;
  int      value;
  uint64_t count;
  char    * unitDisplay;

  if (AVERAGE) {
    memset(elementaverage,0, nb*sizeof(colaverage_t));
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
     
  LINES=60/COLS;

    
  gettimeofday(&tv,(struct timezone *)0);

  pChar += rozofs_string_append(pChar,"___ AVERAGE THROUGHPUT PER SECOND HISTORY WITH A 1 ");  
  switch(unit) {
  
    case rozofs_thr_unit_second: 
      pChar += rozofs_string_append_bold(pChar,"SECOND ");
      unitDisplay = "| SEC |";
      t = tv.tv_sec-1;
      break; 

    case rozofs_thr_unit_minute: 
      pChar += rozofs_string_append_bold(pChar,"MINUTE ");
      unitDisplay = "| MIN |";
      t = (tv.tv_sec/60)-1;
      break; 

    case rozofs_thr_unit_hour: 
      pChar += rozofs_string_append_bold(pChar,"HOUR ");
      unitDisplay = "| HR  |";
      t = (tv.tv_sec/3600)-1;
      break;
    
    default:
      pChar += rozofs_string_append(pChar,"No such unit ");
      pChar += rozofs_u32_append(pChar,unit);
      pChar += rozofs_string_append(pChar," !!!\n");
      return pChar;         
  } 
  pChar += rozofs_string_append(pChar,"STEP ___\n");  
   
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
            count = p->count/60;	
            break; 

          case rozofs_thr_unit_hour: 
            p = &(counters[value]->hour[idx]);
            if (p->ts != (t-line-(col*LINES))) p->count = 0;
            count = p->count/3600;	
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
  return pChar;    
}
