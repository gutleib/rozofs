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
#ifndef ROZOFS_TIMER_CONF_H
#define ROZOFS_TIMER_CONF_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <rozofs/common/common_config.h>
#include <rozofs/core/ruc_sockCtl_api.h>

typedef struct _rozofs_configure_param_t
{
   char    * display_name; /* Display name of the timer */
   uint32_t  unit;         /**< time unit         */
   uint32_t  default_val;  /**< default value     */
   uint32_t  min_val;      /**< min value         */
   uint32_t  max_val;      /**< min value         */
   uint32_t  cur_val;      /**< current value     */
} rozofs_configure_param_t;

#define TMR_MS 0    /**< time in milliseconds units */
#define TMR_SEC 1   /**< time in seconds units */

typedef enum
{
  TMR_EXPORT_PROGRAM = 0,          /**< exportd transaction timeout:              default 25 s */
  TMR_STORAGE_PROGRAM ,            /**< storaged transaction timeout:             default 3 s  */
  TMR_STORCLI_PROGRAM ,            /**< stor_cli transaction timeout:             default 10 s */
  TMR_EXPORTD_PROFILE_PROGRAM,     /**< exportd profiler program                  default 25 s */
  TMR_ROZOFSMOUNT_PROFILE_PROGRAM, /**< rozofsmount profiler program              default 25 s */
  TMR_MONITOR_PROGRAM,             /**< storaged monitor program                  default 4 s  */
  TMR_STORAGED_PROFILE_PROGRAM,    /**< storaged profiler program                 default 25 s */
  TMR_STORCLI_PROFILE_PROGRAM,     /**< storaged client profiler program          default 25 s */
  /*
  ** timer related to dirent cache
  */
  TMR_FUSE_ATTR_CACHE,            /**< attribute cache timeout for fuse           default 10 s */
  TMR_FUSE_ATTR_CACHE_MS,         /**< attribute cache timeout for fuse           default 10 s */
  TMR_FUSE_ENTRY_CACHE,           /**< entry cache timeout for fuse               default 10 s */
  TMR_FUSE_ENTRY_CACHE_MS,        /**< entry cache timeout for fuse               default 10 s */
  /*
  ** timer related to dirent cache (directory case)
  */
  TMR_FUSE_ATTR_DIR_CACHE_MS,            /**< directory attribute cache timeout for fuse    */
  TMR_FUSE_ENTRY_DIR_CACHE_MS,           /**< directory entry cache timeout for fuse        */

  /*
  ** timer related to archiving cache (file case)
  */
  TMR_FUSE_ATTR_ARCH_CACHE,            /**< file attribute cache timeout for fuse    */
  TMR_FUSE_ENTRY_ARCH_CACHE,           /**< file entry cache timeout for fuse        */
  
  /*
  ** timer related to TCP connection and load balancing group
  */
  TMR_TCP_FIRST_RECONNECT,        /**< TCP timer for the first TCP re-connect attempt  default   2 s */
  TMR_TCP_RECONNECT,              /**< TCP timer for subsequent TCP re-connect attempts  default 4 s */
  TMR_RPC_NULL_PROC_TCP,          /**< timer associated to a null rpc procedure polling initiated from TCP cnx default 3 s */
  TMR_RPC_NULL_PROC_LBG,          /**< timer associated to a null rpc procedure polling initiated from TCP cnx default 4 s */
  /*
  ** timer related to projection read/write
  */
  TMR_PRJ_READ_SPARE,            /**< guard timer started upon receiving the first projection (read) default 15000 ms */
  TMR_WR_BLOCK,                  /**< guard timer associated to the write block (default 1000 ms )*/
  TMR_LINK_CACHE,                /**< target of symbolic link cache timeout in rozofsmount default 1000 ms )*/

  /*
  ** ENOENT cache 
  */
  TMR_FUSE_ENOENT_CACHE_MS,      /**< Caching ENOENT */

  TMR_MAX_ENTRY

} rozofs_timer_e;


extern rozofs_configure_param_t rozofs_timer_conf[TMR_MAX_ENTRY];


typedef enum
{
  FRQ_BAL_VOL_THREAD = 0,          /**< period of the volume balance thread :      default 25 s */
  FRQ_RM_BINS_THREAD ,             /**< remove bins thread period     :            default 3 s  */
  FRQ_MONITOR_THREAD ,             /**< monitor thread period:                     default 10 s */
  FRQ_LOAD_TRASH_THREAD,           /**< trash thread period                        default 25 s */
  FRQ_CONNECT_STORAGE,             /**< connect storage thread period              default 25 s */
  FRQ_MAX_ENTRY

} rozofs_periodic_e;



/**
*  Initialization of the parameter associated with the different
*  Guard timers used by rozofs
 @param none
 @retval none
*/
void rozofs_tmr_init_configuration();

/*__________________________________________________________________________
*/
/**
*  Configure one timer of rozofs

 @param timer_id: index of the timer (name: see rozofs_timer_e)
 @param val : value of the timer (see the definition of the timer for the unit)
 
 @retval 0 on success
 @retval -1 on error (see errno for details)
*/
int rozofs_tmr_configure(int timer_id,int val);

/*__________________________________________________________________________
*/
/**
*  Configure one timer of rozofs

 @param  timer_id: index of the timer (name: see rozofs_timer_e)
 
 @retval 0 on success
 @retval -1 on error (see errno for details)
*/
int rozofs_tmr_set_to_default(int timer_id);
/*__________________________________________________________________________
*/
/**
*  Get the current value of a timer

  @param timer_id: reference of the timer
*/
#define ROZOFS_TMR_GET rozofs_tmr_get
static inline uint32_t rozofs_tmr_get(int timer_id)
{
  return rozofs_timer_conf[timer_id].cur_val;
}
/*__________________________________________________________________________
*/
/**
*  Get the attribute cache timer for FUSE

  @param dir: assert to 1 when the inode is a directory
*/
static inline double rozofs_tmr_get_attr(int dir)
{
  if (dir)
  {
    return ((double) rozofs_timer_conf[TMR_FUSE_ATTR_DIR_CACHE_MS].cur_val)/1000;
  }
     
  if (rozofs_timer_conf[TMR_FUSE_ATTR_CACHE].cur_val != 0) {
    return (double) rozofs_timer_conf[TMR_FUSE_ATTR_CACHE].cur_val;
  }
  if (rozofs_timer_conf[TMR_FUSE_ATTR_CACHE_MS].cur_val != 0) {
    return ((double) rozofs_timer_conf[TMR_FUSE_ATTR_CACHE_MS].cur_val)/1000;
  }
  return 0;
}

/*__________________________________________________________________________
*/
/**
*  Get the attribute cache timer for FUSE

*/
static inline int rozofs_tmr_get_attr_ms()
{
  if (rozofs_timer_conf[TMR_FUSE_ATTR_CACHE].cur_val != 0) {
    return  rozofs_timer_conf[TMR_FUSE_ATTR_CACHE].cur_val*1000;
  }
  if (rozofs_timer_conf[TMR_FUSE_ATTR_CACHE_MS].cur_val != 0) {
    return  rozofs_timer_conf[TMR_FUSE_ATTR_CACHE_MS].cur_val;
  }
  return 0;
}
/*__________________________________________________________________________
*/
/**
*  Get the attribute cache timer in micro sec to compare to the IE timestamp

  @param dir: assert to 1 when the inode is a directory

*/
static inline uint64_t rozofs_tmr_get_attr_us(int dir)
{
  return (uint64_t) (rozofs_tmr_get_attr(dir)*1000000);
}
/*__________________________________________________________________________
*/
/**
*  Get the entry cache timer for FUSE

  @param dir: assert to 1 when the inode is a directory

*/
static inline double rozofs_tmr_get_entry(int dir)
{
  if (dir)
  {
    return ((double) rozofs_timer_conf[TMR_FUSE_ENTRY_DIR_CACHE_MS].cur_val)/1000;
  }
  if (rozofs_timer_conf[TMR_FUSE_ENTRY_CACHE].cur_val != 0) {
    return (double) rozofs_timer_conf[TMR_FUSE_ENTRY_CACHE].cur_val;
  }
  if (rozofs_timer_conf[TMR_FUSE_ENTRY_CACHE_MS].cur_val != 0) {
    return ((double) rozofs_timer_conf[TMR_FUSE_ENTRY_CACHE_MS].cur_val)/1000;
  }
  return 0;
}
/*__________________________________________________________________________
*/
/**
*  Get the entry cache timer for FUSE in case of non existent entry

*/
static inline double rozofs_tmr_get_enoent(void)
{
  if (rozofs_timer_conf[TMR_FUSE_ENOENT_CACHE_MS].cur_val != 0) {
    return ((double) rozofs_timer_conf[TMR_FUSE_ENOENT_CACHE_MS].cur_val)/1000;
  }
  return 0;
}


/*__________________________________________________________________________
*/
/**
*  Get the entry cache timer for FUSE

*/
static inline int rozofs_tmr_get_entry_ms()
{
  if (rozofs_timer_conf[TMR_FUSE_ENTRY_CACHE].cur_val != 0) {
    return rozofs_timer_conf[TMR_FUSE_ENTRY_CACHE].cur_val*1000;
  }
  if (rozofs_timer_conf[TMR_FUSE_ENTRY_CACHE_MS].cur_val != 0) {
    return  rozofs_timer_conf[TMR_FUSE_ENTRY_CACHE_MS].cur_val;
  }
  return 0;
}

int rozofs_tmr_get_idx_from_name(char * name);


char *rozofs_tmr_display(char *buf);

#endif
