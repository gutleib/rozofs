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
 #ifndef ROZOFS_EXP_MOVER_H
 #define ROZOFS_EXP_MOVER_H
 
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include <rozofs/common/common_config.h>
#include "config.h"
#include "exp_cache.h"
#include "export.h"

typedef struct _rozofs_mv_idx_dist_t
{
   uint8_t mov_idx;
   cid_t   cid;
   sid_t   sids[ROZOFS_SAFE_MAX_STORCLI];
} rozofs_mv_idx_dist_t; 
 
/*
**__________________________________________________________________
*/
/**
*  allocate an index for moving a file towards
   a new set of cid/sids
   
   @param lv2: level 2 cache entry associated with the file to move
   @param trash_mv_p : pointer to the trash context associated with the former file mover
   @param cid: cluster id of the mover
   @param sids_p : sid distribution
   
   @retval 0 on success
   @retval -1 on error
   
*/
int rozofs_mover_file_create (lv2_entry_t *lv2,rozofs_mv_idx_dist_t *trash_mv_p,cid_t cid, sid_t *sids_p);

/*
**__________________________________________________________________
*/
/**
*  allocate an index for moving a file towards
   a new set of cid/sids
   
   @param lv2: level 2 cache entry associated with the file to move
   @param trash_mv_p : pointer to the trash context associated with the former file mover
   @param guard_time: guard delay
   @param mover: assert to 1 when the source is the "mover", 0 otherwise
   
   @retval 0 on success
   @retval -1 on error (see errno for details)
   
*/
int rozofs_mover_file_validate (lv2_entry_t *lv2,rozofs_mv_idx_dist_t *trash_mv_p,uint64_t guard_time,int mover);

/*
**__________________________________________________________________
*/
/**
*  Invalidate the mover

   @param lv2: level 2 cache entry associated with the file

  @retval none
   
*/
static inline void rozofs_mover_invalidate (lv2_entry_t *lv2) 
{
  lv2->access_cpt = 1;
}

/*
**__________________________________________________________________
*/
/**
*   scanning of the mover file allocation

    @param value: pointer to the beginning of the buffer that contains the extended attributes
    @param p: pointer to the buffer that contains the extended attributes
    @param length: length of the extended attributes
    @param new_cid: new cluster identifier
    @param lv2: pointer to the cache entry that contains the i-node data.
    @param e: pointer to the exportd context.
    
    @retval 0 on success
    @retval -1 on error (see errno for details)
*/
int rozofs_mover_allocate_scan(char *value,char *p,int length,export_t *e,lv2_entry_t *lv2,int new_cid);
/*
**__________________________________________________________________
*/
/**
*   scanning of the mover file validation

    @param lv2: pointer to the cache entry that contains the i-node data.
    @param e: pointer to the exportd context.
    @param guard_time: guard timer in seconds
    
    @retval 0 on success
    @retval -1 on error (see errno for details)
*/
int rozofs_mover_valid_scan(export_t *e,lv2_entry_t *lv2,uint64_t guard_time);
/*
**__________________________________________________________________
**
**   scanning of the mover file invalidation

    @param lv2: pointer to the cache entry that contains the i-node data.
    @param e: pointer to the exportd context.
    @param unused     Unused up to now
    
    @retval 0 on success
    @retval -1 on error (see errno for details)
*/
int rozofs_mover_invalid_scan(export_t *e,lv2_entry_t *lv2,uint64_t unused);

/*
**__________________________________________________________________
*/
/**
*  Put the fid and distribution of the file that has been moved by the "mover" process

   @param e: pointer to the exportd
   @param lv2: pointer to the cache entry that contains the i-node information
   @param trash_mv_p: pointer to the distribution that must moved to the trash
   
   @retval 0 on success
   @retval < 0 on error (see errno for details)
*/
int rozofs_mover_put_trash(export_t *e,lv2_entry_t *lv2,rozofs_mv_idx_dist_t *trash_mv_p);
/*
**__________________________________________________________________
*/
/**
*  check if the i-node has some pending distribution to move

   When it is the case, the "mover distribution must be deleted and the i-node re-writtren
   
   @param lv2: level 2 cache entry associated with the file to move
   @param trash_mv_p : pointer to the trash context associated with the former file mover
   @param guard_time: guard delay
   
   @retval 0 on success
   @retval -1 on error (see errno for details)
   
*/
void rozofs_mover_check_for_validation (export_t *e,lv2_entry_t *lv2,fid_t fid);
/*
**__________________________________________________________________
*/
/**
*  Put the fid and distribution of the file that has been moved by the "mover" process in the trash
   That function is intended to be called when there is an unlink of the regular file

   @param e: pointer to the exportd
   @param lv2: pointer to the cache entry that contains the i-node information
   
   @retval 0 on success
   @retval < 0 on error (see errno for details)
*/
int rozofs_mover_unlink_mover_distribution(export_t *e,lv2_entry_t *lv2);
 #endif
