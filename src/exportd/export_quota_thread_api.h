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
 
 #ifndef EXPORT_QUOTA_THREAD_API_H
 #define EXPORT_QUOTA_THREAD_API_H
 #include <string.h>
#include <stdio.h>
#include <stdint.h>

/*
**__________________________________________________________________
*/
/**
*   Init of the quota periodic thread of RozoFS

    @param none
    
    @retval 0 on success
    @retval -1 on error
*/
int export_fstat_init();

/*
**__________________________________________________________________
*/
/**
*  Export quota table create

   That service is called at export creation time. Its purpose is to allocate
   data structure for export attributes management.
   
   @param eid : export identifier
   @param root_path : root path of the export
   @param create_flag : assert to 1 if  file MUST be created
   @param hquota : hardware quota in blocks
   @param squota : software quota in blocks
   @param hquota_fast : hardware quota in blocks for fast volume
   
   @retval <> NULL: pointer to the attributes tracking table
   @retval == NULL : error (see errno for details)
*/
void *export_fstat_alloc_context(uint16_t eid, char *root_path,uint64_t hquota,uint64_t squota,uint64_t hquota_fast,int create);

/*
**__________________________________________________________________
*/
/** update the number of files in file system
 *
 * @param eid: the export to update
 * @param n: number of files
   @param vid_fast: reference of a fast volume (0: not significant)
 *
 * @return always 0
 */
int export_fstat_create_files(uint16_t eid, uint32_t n,uint8_t vid_fast);
int export_fstat_delete_files(uint16_t eid, uint32_t n,uint8_t vid_fast);
/*
**__________________________________________________________________
*/
/** update the number of blocks in file system
 *
  @param eid: the export to update
  @param newblocks: new number of blocks
  @param oldblocks: old number of blocks
  @param thin_provisioning: assert to 1 if export is configured with thin provisioning
   @param vid_fast: reference of a fast volume (0 is not significant)
   @param hybrid_size_block: size of the hybrid section given in blocks

 * @return 0 on success -1 otherwise
 */
int export_fstat_update_blocks(uint16_t eid, uint64_t newblocks, uint64_t oldblocks,int thin_provisioning,uint8_t vid_fast,uint32_t hybrid_size_block);

/*
**_______________________________________________________________
*/
void show_export_fstat_thread(char * argv[], uint32_t tcpRef, void *bufRef);

/*
**__________________________________________________________________
*/
/** update the number of blocks in file system
 *
 * @param eid: the export to update
 * @param n: number of blocks
 *
 * @return 0 on success -1 otherwise
 */
int expthin_fstat_update_blocks(uint16_t eid,uint32_t nb_blocks, int dir);
/*
**__________________________________________________________________
** Check whether slow and fast quota have exceeded or not
**
** @param e: the export context
** @param quota_slow: return 1 if allocation is possible, 0 else
** @param quota_fast: return 1 if allocation is possible, 0 else
**
**__________________________________________________________________
*/
void export_fstat_check_quotas(export_t *e, int * quota_slow, int * quota_fast);
 #endif
