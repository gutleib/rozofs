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

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "rozofs_timer_conf.h"

rozofs_configure_param_t rozofs_timer_conf[TMR_MAX_ENTRY];


#define DEF_TMR(name,min,max,default,ms_or_sec) { \
 rozofs_timer_conf[TMR_##name].display_name = #name; \
 rozofs_timer_conf[TMR_##name].default_val = default; \
 rozofs_timer_conf[TMR_##name].min_val = min; \
 rozofs_timer_conf[TMR_##name].max_val = max; \
 rozofs_timer_conf[TMR_##name].cur_val = default;\
 rozofs_timer_conf[TMR_##name].unit = ms_or_sec;\
}
/*__________________________________________________________________________
*/
/**
*  Initialization of the parameter associated with the different
*  Guard timers used by rozofs
 @param none
 @retval none
*/
void rozofs_tmr_init_configuration()
{
  DEF_TMR(EXPORT_PROGRAM,1,120,60,TMR_SEC); /**< exportd transaction timeout :default 25 s */
  DEF_TMR(STORAGE_PROGRAM,2,120,30,TMR_SEC);  /**< storaged transaction timeout : default 4 s  */
  DEF_TMR(STORCLI_PROGRAM,2,120,60,TMR_SEC);            /**< storagd client transaction timeout :      default 15 s */
  DEF_TMR(EXPORTD_PROFILE_PROGRAM,5,120,25,TMR_SEC);     /**< exportd profiler program                  default 25 s */
  DEF_TMR(ROZOFSMOUNT_PROFILE_PROGRAM,5,120,25,TMR_SEC); /**< rozofsmount profiler program              default 25 s */
  DEF_TMR(MONITOR_PROGRAM,2,30,4,TMR_SEC);             /**< storaged monitor program                  default 4 s  */
  DEF_TMR(STORAGED_PROFILE_PROGRAM,2,120,25,TMR_SEC);    /**< storaged profiler program                 default 25 s */
  DEF_TMR(STORCLI_PROFILE_PROGRAM,2,120,25,TMR_SEC);     /**< storaged client profiler program          default 25 s */
  /*
  ** timers related to dirent cache
  */
  DEF_TMR(FUSE_ATTR_CACHE,0,300,10,TMR_SEC);            /**< attribute cache timeout for fuse           default 10 s */
  DEF_TMR(FUSE_ATTR_CACHE_MS,0,300000,0,TMR_MS);            /**< attribute cache timeout for fuse           default 10 s */
  DEF_TMR(FUSE_ENTRY_CACHE,0,300,10,TMR_SEC);           /**< entry cache timeout for fuse               default 10 s */
  DEF_TMR(FUSE_ENTRY_CACHE_MS,0,300000,0,TMR_MS);           /**< entry cache timeout for fuse               default 10 s */
  /*
  ** timer related to dirent cache (directory case)
  */
  DEF_TMR(FUSE_ATTR_DIR_CACHE_MS,0,300000,10000,TMR_MS);            /**< directory attribute cache timeout for fuse    */
  DEF_TMR(FUSE_ENTRY_DIR_CACHE_MS,0,300000,10000,TMR_MS);           /**< directory entry cache timeout for fuse        */
  /*
  ** timer related to archive cache (file case only)
  */
  DEF_TMR(FUSE_ATTR_ARCH_CACHE,0,300,10,TMR_SEC);                   /**< file attribute cache timeout for fuse archive case    */
  DEF_TMR(FUSE_ENTRY_ARCH_CACHE,0,300,10,TMR_SEC);           /**< file entry cache timeout for fuse archive case        */
  /*
  ** timer related to TCP connection and load balancing group
  */
  DEF_TMR(TCP_FIRST_RECONNECT,1,10,2,TMR_SEC);        /**< TCP timer for the first TCP re-connect attempt  default   2 s */
  DEF_TMR(TCP_RECONNECT,1,30,4,TMR_SEC);              /**< TCP timer for subsequent TCP re-connect attempts  default 4 s */
  DEF_TMR(RPC_NULL_PROC_TCP,1,120,3,TMR_SEC);          /**< timer associated to a null rpc procedure polling initiated from TCP cnx default 3 s */
  DEF_TMR(RPC_NULL_PROC_LBG,1,120,4,TMR_SEC);          /**< timer associated to a null rpc procedure polling initiated from TCP cnx default 4 s */
  /*
  ** timer related to projection read/write
  */
  DEF_TMR(PRJ_READ_SPARE,20,60000,15000,TMR_MS);            /**< guard timer started upon receiving the first projection (read) default 15000 ms */
  DEF_TMR(WR_BLOCK,20,30000,1000,TMR_MS);            /**< guard timer associated to the write block (default 1000 ms ) */
  DEF_TMR(LINK_CACHE,0,30000,1000,TMR_MS);          /**< target of symbolic link cache timeout in rozofsmount default 1000 ms )*/
  /*
  ** ENOENT cache
  */
  DEF_TMR(FUSE_ENOENT_CACHE_MS,0,30000,2000,TMR_MS);          /**< target of symbolic link cache timeout in rozofsmount default 1000 ms )*/

}
/*__________________________________________________________________________
*/
/**
*  Configure one timer of rozofs

 @param timer_id: index of the timer (name: see rozofs_timer_e)
 @param val : value of the timer (see the definition of the timer for the unit)
 
 @retval 0 on success
 @retval -1 on error (see errno for details)
*/
int rozofs_tmr_configure(int timer_id,int val)
{
rozofs_configure_param_t *p; 

  if (timer_id >= TMR_MAX_ENTRY)
  {
    errno = EINVAL; 
    return -1; 
  }
  p = &rozofs_timer_conf[timer_id];
  if ((val < p->min_val) ||  (val > p->max_val))
  {
    errno = ERANGE;
    return -1;   
  }
  p->cur_val = val;
  return 0;
}

/*__________________________________________________________________________
*/
/**
*  Configure one timer of rozofs

 @param  timer_id: index of the timer (name: see rozofs_timer_e)
 
 @retval 0 on success
 @retval -1 on error (see errno for details)
*/
int rozofs_tmr_set_to_default(int timer_id)
{
  rozofs_configure_param_t *p; 

  if (timer_id >= TMR_MAX_ENTRY)
  {
    errno = EINVAL; 
    return -1; 
  }
  p = &rozofs_timer_conf[timer_id];
  p->cur_val = p->default_val;
  return 0;
}
/*__________________________________________________________________________
*/
/**
*  Display the configuration of every timer

 @param  buf    where to format the outpu
 
 @retval pointer to the end of output
*/
#define DISPLAY_TMR() \
{ \
  buf+=sprintf(buf," %-27s | %3d | %7d | %7d | %7d | %7d | %4s |\n",\
       p->display_name,i,p->default_val,p->min_val,p->max_val,p->cur_val,(p->unit==TMR_SEC)?"sec":"ms");\
}

char *rozofs_tmr_display(char *buf)
{
  rozofs_configure_param_t *p=&rozofs_timer_conf[0];
  int i= 0;
  
  buf+=sprintf(buf,"    timer name               | idx | default |  min    | max     | current | unit |\n");
  buf+=sprintf(buf,"-----------------------------+-----+---------+---------+---------+---------+------+\n");
  for (i=0; i< TMR_MAX_ENTRY; i++,p++) {
    DISPLAY_TMR(); 
  }  
  return buf;

}
/*__________________________________________________________________________
*/
/**
*  Gives the timer index from its name

 @param  name    name of the timer
 
 @retval index of the timer or -1 when no such timer
*/
int rozofs_tmr_get_idx_from_name(char * name)
{
  rozofs_configure_param_t *p=&rozofs_timer_conf[0];
  int i= 0;
  
  for (i=0; i< TMR_MAX_ENTRY; i++,p++) {
    if (strcasecmp(name,p->display_name) == 0) return i;
  }  
  return -1;

}
