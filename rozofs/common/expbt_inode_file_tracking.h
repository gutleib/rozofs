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

#ifndef EXPBT_INODE_FILE_TRACKING_H
#define EXPBT_INODE_FILE_TRACKING_H
 #include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <errno.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>        /* For mode constants */
#include <fcntl.h>           /* For O_* constants */
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <rozofs/rozofs.h>

#define ROZO_SLICE_COUNT 256
#define ROZO_NB_ENTRIES_CHG_COUNT 1024  /**< number of tracking file per slice  */
#define ROZO_NB_TRK_FILE_ENTRIES 8
#define ROZO_REG_ATTR_NB_INODE_ENTRIES_PER_COUNTER 1
#define ROZO_DIR_ATTR_NB_INODE_ENTRIES_PER_COUNTER 32



typedef struct _rozo_trk_file_bitmap_t
{
   uint64_t change_bitmap[(ROZO_NB_ENTRIES_CHG_COUNT*ROZO_NB_TRK_FILE_ENTRIES)/sizeof(uint64_t)];
} rozo_trk_file_bitmap_t;



typedef struct _rozo_trk_file_t
{
   uint32_t change_count[ROZO_NB_ENTRIES_CHG_COUNT*ROZO_NB_TRK_FILE_ENTRIES*ROZO_SLICE_COUNT];
} rozo_trk_file_t;


typedef struct rozo_trk_file_eid_shm_t
{
   int eid;        /**< export identifier */
   int type;       /**< either reg_attr or dir_attr */
   int fd;         /**< file descripotor of the shared memory */
   void *addr;     /**< address of the shared memory */
   uint64_t length; /**< length of the sharead memory */
} rozo_trk_file_eid_shm_t;
   
/*
** Global data
*/


/*
**______________________________________________________________________________
*/
int expbt_shared_mem_init();
/*
**______________________________________________________________________________
*/
/**
  Open a shared memory for tracking the file tracking changes
  
  @param eid: export identifier
  @param type:ROZOFS_REG or ROZOFS_DIR
  @param create: assert to 1 if shared memory must be created
  
  @retval 0 on success
  @retval -1 on error (see errno for details)
*/

int expb_open_shared_memory(int eid,int type,int create);

/*
**______________________________________________________________________________
*/
/**
  Track the change that happen for an inode within its tracking file
  
  @param inode: pointer to the inode
  
  @retval none
*/
void expbt_track_inode_update(rozofs_inode_t *inode);
/*
**______________________________________________________________________________
*/
/**
  Track the change that happen for an inode within its tracking file
  
  @param inode: pointer to the inode
  @param all: when asserted does not care about the range that is applied on inode index (mainly for directory)
  
  @retval counter_value: current value of the counter (0 if it does not exits
*/

uint32_t expbt_track_inode_get_counter(rozofs_inode_t *inode,int all);

/*
**______________________________________________________________________________
*/
void show_expbt_track_inode_display_for_slice(char * argv[], uint32_t tcpRef, void *bufRef);
/*
**______________________________________________________________________________
*/
/**
  check the existence of a shared memory used from file tracking changes
  
  @param eid: export identifier
  @param type:ROZOFS_REG or ROZOFS_DIR

  
  @retval 1 on success
  @retval 0 on error (see errno for details)
*/

int expb_is_shared_memory_entry_exist(int eid,int type);
#endif
