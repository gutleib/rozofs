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

#ifndef ROZOFS_QUOTA_API_H
#define ROZOFS_QUOTA_API_H

#include <stdint.h>
#include "rozofs_quota.h"

#define ROZOFS_QT_INC 1
#define ROZOFS_QT_DEC 0
/*
**__________________________________________________________________
*/
/**
*   update inode quota
    
    @param eid: export identifier
    
    @param usr_id : user quota
    @param grp_id : group quota
    @param nb_inode
    @param action: 1: increment, 0 decrement 
    @param share : share identifier (0 if none)
    
    @retval : 0 on success
    @retval < 0 on error
 */
int rozofs_qt_inode_update(int eid,int user_id,int grp_id,int nb_inode,int action,uint16_t share);
/*
**__________________________________________________________________
*/
/**
*   update size (blocks) quota
    
    @param eid: export identifier
    
    @param usr_id : user quota
    @param grp_id : group quota
    @param nb_inode
    @param action: 1: increment, 0 decrement 
    @param share : share identifier (0 if none)
    
    @retval : 0 on success
    @retval < 0 on error
 */
int rozofs_qt_block_update(int eid,int user_id,int grp_id,uint64_t size,int action,uint16_t share);
/*
**__________________________________________________________________
*/
/**
*  Export quota table create

   That service is called at export creation time. Its purpose is to allocate
   data structure for export attributes management.
   
   @param eid : export identifier
   @param root_path : root path of the export
   @param create_flag : assert to 1 if quota files MUST be created
   
   @retval <> NULL: pointer to the attributes tracking table
   @retval == NULL : error (see errno for details)
*/
void *rozofs_qt_alloc_context(uint16_t eid, char *root_path, int create);

/*
**__________________________________________________________________
*/
/**
*   Init of the quota module of RozoFS

    
    @retval 0 on success
    @retval -1 on error
*/
int rozofs_qt_init();
/*
**__________________________________________________________________
*/
/**
*   Init of the quota module of RozoFS

    @param instance: instance id of the exportd slave
    
    @retval 0 on success
    @retval -1 on error
*/
int rozofs_qt_thread_intf_create(int instance);

/*
 *_______________________________________________________________________
 */
/**
*   Quota cache statistics

  @param argv : standard argv[] params of debug callback
  @param tcpRef : reference of the TCP debug connection
  @param bufRef : reference of an output buffer 
  
  @retval none
*/
void show_quota_cache(char * argv[], uint32_t tcpRef, void *bufRef);
/**
*  writeback thread statistics
*/
void show_wbcache_quota_thread(char * argv[], uint32_t tcpRef, void *bufRef);

/*
 *_______________________________________________________________________
 */
/**
*   Read quota user or group

  @param argv : standard argv[] params of debug callback
  @param tcpRef : reference of the TCP debug connection
  @param bufRef : reference of an output buffer 
  
  @retval none
*/
void rw_quota_entry(char * argv[], uint32_t tcpRef, void *bufRef);

char *quota_wbcache_display_stats(char *pChar);
#endif
