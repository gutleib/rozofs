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
#ifndef rozofs_thr_H
#define rozofs_thr_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "rozofs_string.h"
#include <rozofs/core/ruc_sockCtl_api.h> 
#include <malloc.h>

#define ROZOFS_THR_CNTS_NB    64

typedef struct _rozofs_thr_1_cnt_t {
  uint64_t     ts;
  uint64_t     count;
} rozofs_thr_1_cnt_t;

   
typedef struct _rozofs_thr_cnts_t {
  char                   * name;
  rozofs_thr_1_cnt_t       second[ROZOFS_THR_CNTS_NB];
  rozofs_thr_1_cnt_t       minute[ROZOFS_THR_CNTS_NB];
  rozofs_thr_1_cnt_t       hour[ROZOFS_THR_CNTS_NB];  
} rozofs_thr_cnts_t;


typedef enum _rozofs_thr_unit_e {
  rozofs_thr_unit_all,
  rozofs_thr_unit_second,
  rozofs_thr_unit_minute,
  rozofs_thr_unit_hour
} rozofs_thr_unit_e; 
 

/*_______________________________________________________________________
* Update throughput counter
*
* @param counters the counter structure
* @param count    a count to increment counters with
* @param t        the time in seconds
*/
static inline void rozofs_thr_cnt_update_with_time(rozofs_thr_cnts_t * counters, uint64_t count, uint32_t t) {
  int    rank;
  if (counters == NULL) return;
  
  /*
  ** Update counters per second
  */
  rank = t % ROZOFS_THR_CNTS_NB;
  
  if (counters->second[rank].ts == t) {
    counters->second[rank].count += count;
  }
  else {
    counters->second[rank].ts    = t;
    counters->second[rank].count = count;
  }
  
  /*
  ** Update counter per minute
  */
  t = t/60;
  rank = t % ROZOFS_THR_CNTS_NB;
  
  if (counters->minute[rank].ts == t) {
    counters->minute[rank].count += count;
  }
  else {
    counters->minute[rank].ts    = t;
    counters->minute[rank].count = count;
  }

  /*
  ** Update counter per hour
  */
  t = t/60;
  rank = t % ROZOFS_THR_CNTS_NB;
  
  if (counters->hour[rank].ts == t) {
    counters->hour[rank].count += count;
  }
  else {
    counters->hour[rank].ts    = t;
    counters->hour[rank].count = count;
  }
    
}  
/*_______________________________________________________________________
* Update throughput counter
*
* @param counters the counter structure
* @param count    a count to increment counters with
*/
static inline void rozofs_thr_cnt_update(rozofs_thr_cnts_t * counters, uint64_t count) {
  if (counters == NULL) return;
  rozofs_thr_cnt_update_with_time(counters,count,rozofs_get_ticker_s());
}

/*_______________________________________________________________________
* Display throughput counters
*
* @param pChar    Where to format the ouput
* @param pChar    The counters
* @param nb       The number of counters
* @param unit     The unit to display
*/
char * rozofs_thr_display_unit(char * pChar, rozofs_thr_cnts_t * counters[], int nb, rozofs_thr_unit_e unit);

/*_______________________________________________________________________
* Display throughput counters
*
* @param pChar    Where to format the ouput
* @param pChar    The counters
* @param nb       The number of counters
* @param unit     The unit to display
*/
char * rozofs_thr_display_bitmask(char * pChar, rozofs_thr_cnts_t * counters[], uint32_t bitmask, rozofs_thr_unit_e unit) ;

/*_______________________________________________________________________
* Reset counters
*
* @param counters  The structure countaining counters
*/
static inline void rozofs_thr_cnts_reset(rozofs_thr_cnts_t * counters) {
  memset(counters->second,0,sizeof(rozofs_thr_1_cnt_t)*ROZOFS_THR_CNTS_NB);
  memset(counters->minute,0,sizeof(rozofs_thr_1_cnt_t)*ROZOFS_THR_CNTS_NB);
}
/*_______________________________________________________________________
* Initialize a thoughput measurement structure
*
* @param counters  The structure to initialize of NULL if it is to be allocated
*
* @retval the initialized structure address
*/
static inline rozofs_thr_cnts_t * rozofs_thr_cnts_allocate(rozofs_thr_cnts_t * counters, char * name) {

  /*
  ** Allocate counters when needed
  */
  if (counters == NULL) {
    counters = memalign(32,sizeof(rozofs_thr_cnts_t));
    counters->name = strdup(name);
  }
    
  /*
  ** Reset counters
  */
  rozofs_thr_cnts_reset(counters);

  return counters;
}
/*_______________________________________________________________________
* Free a thoughput measurement structure
*
* @param counters  The structure to free
*
*/
static inline void rozofs_thr_cnts_free(rozofs_thr_cnts_t ** counters) {

  if (*counters == NULL) return;
  
  if ((*counters)->name) {
    free((*counters)->name);
    (*counters)->name = NULL;
  }

  free(*counters);
  *counters = NULL;
}
/*_______________________________________________________________________
* Change the number of columns of the display
*
* @param columns The number of columns per minutes
*/
void rozofs_thr_set_column(int columns);
/*
*_______________________________________________________________________
* Request for average display at the end of the colums
*
*/
void rozofs_thr_set_average() ;
/*_______________________________________________________________________
* Request for througput per second unit
* i.e when display is done per minute step, the display throughput is a
*     throughput per second
*/
void rozofs_thr_display_throughput_per_sec() ;
/*_______________________________________________________________________
* Request for througput per display step unit
* i.e when display is done per minute step, the display throughput is a
*     throughput per minute
*/
void rozofs_thr_display_throughput_per_step() ;
#endif


