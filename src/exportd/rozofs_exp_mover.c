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
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include <rozofs/common/common_config.h>
#include "rozofs_exp_mover.h"
#include <rozofs/rozofs_srv.h>


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
int rozofs_mover_file_create (lv2_entry_t *lv2,rozofs_mv_idx_dist_t *trash_mv_p,cid_t cid, sid_t *sids_p)
{
   ext_mattr_t *attr_p;
   rozofs_mover_sids_t *dist_mv_p;   
   rozofs_mover_children_t mover_idx;
   
   trash_mv_p->cid = 0;
   
   attr_p = &lv2->attributes;
   dist_mv_p = (rozofs_mover_sids_t*)&attr_p->s.attrs.sids;
   
   mover_idx.u32 = attr_p->s.attrs.children;
   
   if (mover_idx.fid_st_idx.mover_idx != mover_idx.fid_st_idx.primary_idx)
   {
      /*
      ** need to put on the trash the old mover file
      */
      if (dist_mv_p->dist_t.mover_cid != 0)
      {
        trash_mv_p->mov_idx = mover_idx.fid_st_idx.mover_idx;
	trash_mv_p->cid = dist_mv_p->dist_t.mover_cid;
	memcpy(trash_mv_p->sids,dist_mv_p->dist_t.mover_sids,ROZOFS_SAFE_MAX_STORCLI);  
      }
   }
   /*
   ** compute the next file mover index for storage mover FID computation
   */
   mover_idx.fid_st_idx.mover_idx = (mover_idx.fid_st_idx.mover_idx+1)%(256);
   if (mover_idx.fid_st_idx.mover_idx == mover_idx.fid_st_idx.primary_idx)
   {
     mover_idx.fid_st_idx.mover_idx = (mover_idx.fid_st_idx.primary_idx+1)%(256);   
   }
   /*
   ** copy the distribution and the reference of the cluster
   */
   dist_mv_p->dist_t.mover_cid =cid;
   memcpy(dist_mv_p->dist_t.mover_sids,sids_p,ROZOFS_SAFE_MAX_STORCLI); 
   /*
   ** put the timestamp in the lv2_entry
   */
   lv2->time_move = time(NULL);
   lv2->mover_state = ROZOFS_MOVER_IN_PRG;
   lv2->access_cpt = 0; 
   attr_p->s.attrs.children = mover_idx.u32;
   /*
   ** lock the entry in the level 2 cache
   */
   lv2_cache_lock_entry_in_cache(lv2);
   return 0;
}
/*
**__________________________________________________________________
*/
/**
*  Invalidate a file move. 
   The mover destination must be put to tbe trash
   
   @param lv2: level 2 cache entry associated with the file to move
   @param trash_mv_p : pointer to the trash context associated with the former file mover
   
   @retval 0 on success
   @retval -1 on error (see errno for details)
   
*/
int rozofs_mover_file_invalidate (lv2_entry_t *lv2,rozofs_mv_idx_dist_t *trash_mv_p)
{
   ext_mattr_t *attr_p;
   rozofs_mover_sids_t *dist_mv_p;   
   rozofs_mover_children_t mover_idx;  
   
   attr_p = &lv2->attributes;
   mover_idx.u32 = attr_p->s.attrs.children;
   
   trash_mv_p->cid = 0;
   
   if (mover_idx.fid_st_idx.mover_idx == mover_idx.fid_st_idx.primary_idx)
   {
      /*
      ** nothing to do
      */
      errno = EINVAL;
      return -1;
   }
   dist_mv_p = (rozofs_mover_sids_t*)&attr_p->s.attrs.sids;
   if (dist_mv_p->dist_t.mover_cid == 0)
   {
     /*
     ** no move pending
     */
     errno = EINVAL;
     return -1;
   }

   /*
   ** Recopy the mover distribution to the trash entry
   */
   trash_mv_p->mov_idx = mover_idx.fid_st_idx.mover_idx;
   trash_mv_p->cid = dist_mv_p->dist_t.mover_cid;
   memcpy(trash_mv_p->sids,dist_mv_p->dist_t.mover_sids,ROZOFS_SAFE_MAX_STORCLI);   

   /*
   ** Clear the mover distribution in the attribute
   */
   dist_mv_p->dist_t.mover_cid = 0;
   memset(dist_mv_p->dist_t.mover_sids,0,ROZOFS_SAFE_MAX_STORCLI);
   
   /*
   ** Unlock the lv2 entry
   */
   lv2_cache_unlock_entry_in_cache(lv2);
   
   /*
   ** Reset moving status
   */
   lv2->mover_state = ROZOFS_MOVER_IDLE;
   
   /*
   ** remove from the move_list
   */
   list_remove(&lv2->move_list);  
   return 0;  	
}
/*
**__________________________________________________________________
*/
/**
*  allocate an index for moving a file towards
   a new set of cid/sids
   
   @param lv2: level 2 cache entry associated with the file to move
   @param trash_mv_p : pointer to the trash context associated with the former file mover
   @param guard_time: guard delay (relevant when source is mover )
   @param mover: assert to 1 when the source is the "mover", 0 otherwise
   
   @retval 0 on success
   @retval -1 on error (see errno for details)
   
*/
int rozofs_mover_file_validate (lv2_entry_t *lv2,rozofs_mv_idx_dist_t *trash_mv_p,uint64_t guard_time,int mover)
{
   ext_mattr_t *attr_p;
   rozofs_mover_sids_t *dist_mv_p;   
   rozofs_mover_children_t mover_idx;  
   int status;
   
   attr_p = &lv2->attributes;
   mover_idx.u32 = attr_p->s.attrs.children;
   
   trash_mv_p->cid = 0;
   
   if (mover_idx.fid_st_idx.mover_idx == mover_idx.fid_st_idx.primary_idx)
   {
      /*
      ** nothing to do
      */
      errno = EINVAL;
      return -1;
   }
   dist_mv_p = (rozofs_mover_sids_t*)&attr_p->s.attrs.sids;
   if (dist_mv_p->dist_t.mover_cid == 0)
   {
     /*
     ** no move pending
     */
     errno = EINVAL;
     return -1;
   }
   if (mover)
   {
     switch (lv2->mover_state)
     {
	case ROZOFS_MOVER_IDLE:
	default:
          /*
	  ** need to flush the distribution of the mover
	  */
          rozofs_mover_file_invalidate (lv2,trash_mv_p);
	  errno = EACCES;
	  return -1;   	

	case ROZOFS_MOVER_IN_PRG:
	case ROZOFS_MOVER_DONE:
	  if (lv2->access_cpt != 0)
	  {
	    /*
	    ** there was some access during the move of the file-> reject
	    */
	    rozofs_mover_file_invalidate (lv2,trash_mv_p);
	    errno = EACCES;
	    return -1;
	  } 
	  lv2->mover_state = ROZOFS_MOVER_DONE;
	  /*
	  ** check the timestamp
	  */
	  if ((time(NULL) - lv2->time_move) < guard_time)
	  {
	     /*
	     ** EAGAIN-> insert the entry in the pending list of the file whose distribution 
	     **          has been changed by the mover and are waiting for guard timer expiration
	     */
#warning insert the lv2 entry in the pending list waiting for mover guard timer expiration
	     lv2->mover_guard_timer = guard_time;
	     errno = EAGAIN;
	     return -1;
	  }
	  /*
	  ** swap the primary and the mover information
	  */
	  trash_mv_p->mov_idx = mover_idx.fid_st_idx.primary_idx;
	  trash_mv_p->cid = attr_p->s.attrs.cid; 
	  memcpy(trash_mv_p->sids,dist_mv_p->dist_t.primary_sids,ROZOFS_SAFE_MAX_STORCLI); 

	  attr_p->s.attrs.cid = dist_mv_p->dist_t.mover_cid;  
	  memcpy(attr_p->s.attrs.sids,dist_mv_p->dist_t.mover_sids,ROZOFS_SAFE_MAX_STORCLI); 
	  mover_idx.fid_st_idx.primary_idx = mover_idx.fid_st_idx.mover_idx;
	  attr_p->s.attrs.children = mover_idx.u32 ;
	  /*
	  ** clear the "mover" distribution
	  */

	  memset(dist_mv_p->dist_t.mover_sids,0,ROZOFS_SAFE_MAX_STORCLI);
	  dist_mv_p->dist_t.mover_cid = 0;
	  /*
	  ** clear the time_move otherwise the entry will be locked in the export cache
	  */
	  lv2->mover_state = ROZOFS_MOVER_IDLE;
	  /*
	  ** remove from the move_list
	  */
	  list_remove(&lv2->move_list);  
	  lv2_cache_unlock_entry_in_cache(lv2);
	  lv2->time_move = 0;
	  return 0;
     }
   }
   /*
   ** case of a call for validation from lookup or getattr
   */
   switch (lv2->mover_state)
   {
      case ROZOFS_MOVER_IDLE:
      default:
        /*
	** need to flush the distribution of the mover
	*/
	rozofs_mover_file_invalidate (lv2,trash_mv_p);
	errno = EACCES;
	status = -1;   
	break;	
      
      case ROZOFS_MOVER_IN_PRG:
        lv2->access_cpt = 1;
	errno = EINVAL;
	status= -1;
	break;
	
      case ROZOFS_MOVER_DONE:
	if ((lv2->access_cpt != 0) || ((time(NULL) - lv2->time_move) < lv2->mover_guard_timer))
	{
	  /*
	  ** there was some access during the move of the file-> reject
	  */
          rozofs_mover_file_invalidate (lv2,trash_mv_p);
	  errno = EACCES;
	  status = -1;
	  break;
	} 
	/*
	** swap the primary and the mover information
	*/
	trash_mv_p->mov_idx = mover_idx.fid_st_idx.primary_idx;
	trash_mv_p->cid = attr_p->s.attrs.cid; 
	memcpy(trash_mv_p->sids,dist_mv_p->dist_t.primary_sids,ROZOFS_SAFE_MAX_STORCLI); 

	attr_p->s.attrs.cid = dist_mv_p->dist_t.mover_cid;  
	memcpy(attr_p->s.attrs.sids,dist_mv_p->dist_t.mover_sids,ROZOFS_SAFE_MAX_STORCLI); 
	mover_idx.fid_st_idx.primary_idx = mover_idx.fid_st_idx.mover_idx;
	attr_p->s.attrs.children = mover_idx.u32 ;
	/*
	** clear the "mover" distribution
	*/

	memset(dist_mv_p->dist_t.mover_sids,0,ROZOFS_SAFE_MAX_STORCLI);
	dist_mv_p->dist_t.mover_cid = 0;
	/*
	** clear the time_move otherwise the entry will be locked in the export cache
	*/
	lv2->mover_state = ROZOFS_MOVER_IDLE;
	lv2_cache_unlock_entry_in_cache(lv2);
	lv2->time_move = 0;
	errno = 0;
	/*
	** remove from the move_list
	*/
	list_remove(&lv2->move_list);  
	status = 0;
	break;
   }
   return status;
}
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
void rozofs_mover_check_for_validation (export_t *e,lv2_entry_t *lv2,fid_t fid)
{
   int ret;
   rozofs_mv_idx_dist_t trash_mv;
   rozofs_inode_t *inode_p;
   /*
   ** Check the mode of the object
   */
   if (!S_ISREG(lv2->attributes.s.attrs.mode)) return;
   
   inode_p = (rozofs_inode_t*)fid;
   /*
   ** nothing to do for the case of the file mover
   */
   if ((inode_p->s.key == ROZOFS_REG_S_MOVER) || (inode_p->s.key == ROZOFS_REG_D_MOVER)) return;
   /*
   ** case of a regular file access
   */
   ret = rozofs_mover_file_validate (lv2,&trash_mv,0,0);
   if (ret < 0) 
   {
     if (errno != EACCES) return;
   }
   /*
   ** push to the trash the old distribution and storage fid
   */
   rozofs_mover_put_trash(e,lv2,&trash_mv);    
   export_lv2_write_attributes(e->trk_tb_p,lv2, 0/* no sync */);
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
int rozofs_mover_allocate_scan(char *value,char *unused,int length,export_t *e,lv2_entry_t *lv2,int new_cid)
{
  int          idx,jdx;
  char         *p;
  int          new_sids[ROZOFS_SAFE_MAX]; 
  uint8_t      rozofs_safe;
  sid_t        sids[ROZOFS_SAFE_MAX];
  cid_t        cid;
  int          ret;
  char         *str;
  char         *saveptr;
  char         *token;
  rozofs_mv_idx_dist_t trash_mv;

  /*
  ** Skip the part that has already been scanned
  */
  
  for (str = value,idx= 0; idx < 3; str = NULL,idx++) 
  {
      token = strtok_r(str," ", &saveptr);
      if (token == NULL)
      {
         errno = EINVAL;
         return -1;
       }
  }
  p = saveptr;

  /*
  ** Scan value
  */
  rozofs_safe = rozofs_get_rozofs_safe(e->layout);
  memset (new_sids,0,sizeof(new_sids));
  memset (sids,0,sizeof(sids));

  errno = 0;

  for (idx=0; idx < rozofs_safe; idx++) {

    if ((p-value)>=length) {
      severe("p %p value %p length %d idx %d",p,value,length,idx);
      errno = EINVAL;
      break;
    }
    new_sids[idx] = strtol(p,&p,10);
    if (errno != 0) return -1;
    if (new_sids[idx]<0) new_sids[idx] *= -1;
  }

  /* Not enough sid in the list */
  if (idx != rozofs_safe) {
    errno = EINVAL;
    return -1;
  }
  /*
  ** Check the same sid is not set 2 times
  */
  for (idx=0; idx < rozofs_safe; idx++) {
    for (jdx=idx+1; jdx < rozofs_safe; jdx++) {
      if (new_sids[idx] == new_sids[jdx]) {
        errno = EINVAL;
	return -1;
      }
    }
  }  
  /*
  ** Check cluster and sid exist
  */
  if (volume_distribution_check(e->volume, rozofs_safe, new_cid, new_sids) != 0) {

    if (e->volume_fast == NULL) return -1;
    
    if (volume_distribution_check(e->volume_fast, rozofs_safe, new_cid, new_sids) != 0) {
      return -1;
    }  
  }
  /*
  ** OK for the new distribution
  */
  cid = new_cid;
  for (idx=0; idx < rozofs_safe; idx++) {
    sids[idx] = new_sids[idx];
  }
  ret = rozofs_mover_file_create (lv2,&trash_mv,cid,sids);  
  /*
  ** move to trash the old distribution if any
  */
  rozofs_mover_put_trash(e,lv2,&trash_mv);    

  if (ret < 0) return ret;
  /*
  ** flush the i-node on disk
  */
  ret = export_lv2_write_attributes(e->trk_tb_p,lv2, 1/* no sync */);

  return ret;
}

/*
**__________________________________________________________________
*/
/**
*   scanning of the mover file validation

    @param lv2: pointer to the cache entry that contains the i-node data.
    @param e: pointer to the exportd context.
    @param mtime
    
    @retval 0 on success
    @retval -1 on error (see errno for details)
*/
int rozofs_mover_valid_scan(export_t *e,lv2_entry_t *lv2,uint64_t guard_time)
{
  int ret;
  int xerrno;
  rozofs_mv_idx_dist_t trash_mv;

  errno = 0;
  ret = rozofs_mover_file_validate (lv2,&trash_mv,guard_time,1);
  if (ret < 0) 
  {
    if (errno != EACCES) return ret;
  }
  xerrno = errno;
  /*
  ** push to the trash the old distribution and storage fid
  */
  rozofs_mover_put_trash(e,lv2,&trash_mv);    

  ret = export_lv2_write_attributes(e->trk_tb_p,lv2, 0/* no sync */);

  errno = xerrno;
  return ret;
}
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
int rozofs_mover_invalid_scan(export_t *e,lv2_entry_t *lv2,uint64_t unused)
{
  int ret;
  rozofs_mv_idx_dist_t trash_mv;

  errno = 0;
  ret = rozofs_mover_file_invalidate (lv2,&trash_mv);
  if (ret < 0) 
  {
    return ret;
  }
  /*
  ** push to the trash the old distribution and storage fid
  */
  rozofs_mover_put_trash(e,lv2,&trash_mv);    

  ret = export_lv2_write_attributes(e->trk_tb_p,lv2, 0/* no sync */);

  return ret;
}

/*
**__________________________________________________________________
*/
/**
*  Put the fid and distribution of the file that has been moved by the "mover" process in the trash

   @param e: pointer to the exportd
   @param lv2: pointer to the cache entry that contains the i-node information
   @param trash_mv_p: pointer to the distribution that must moved to the trash
   
   @retval 0 on success
   @retval < 0 on error (see errno for details)
*/
int rozofs_mover_put_trash(export_t *e,lv2_entry_t *lv2,rozofs_mv_idx_dist_t *trash_mv_p)
{

  rmfentry_disk_t trash_entry;
  int ret;
  rozofs_inode_t  *fake_inode_p;

  if (trash_mv_p->cid == 0)
  {
     /*
     ** nothing to trash
     */
     return 0;
  }
  uint32_t hash = rozofs_storage_fid_slice(lv2->attributes.s.attrs.fid);  
  /*
  ** prepare the trash entry
  */
  trash_entry.size = lv2->attributes.s.attrs.size;
  memcpy(trash_entry.fid, lv2->attributes.s.attrs.fid, sizeof (fid_t));
  /*
  ** transform the fid_export in the corresponding storage_fid
  */
  rozofs_build_storage_fid(trash_entry.fid,trash_mv_p->mov_idx);
  trash_entry.cid = trash_mv_p->cid;
  memcpy(trash_entry.initial_dist_set, trash_mv_p->sids,
          sizeof (sid_t) * ROZOFS_SAFE_MAX_STORCLI);
  memcpy(trash_entry.current_dist_set, trash_mv_p->sids,
          sizeof (sid_t) * ROZOFS_SAFE_MAX_STORCLI);
  fake_inode_p =  (rozofs_inode_t *)lv2->attributes.s.pfid;   
  ret = exp_trash_entry_create(e->trk_tb_p,fake_inode_p->s.usr_id,&trash_entry); 
  if (ret < 0)
  {
     /*
     ** error while inserting entry in trash file
     */
     severe("error on trash insertion from mover: %s",strerror(errno)); 
  }
  /*
  ** Preparation of the rmfentry
  */
  rmfentry_t *rmfe = export_alloc_rmentry(&trash_entry);
  /* 
  ** Acquire lock on bucket trash list
  */
  if ((errno = pthread_rwlock_wrlock
          (&e->trash_buckets[hash].rm_lock)) != 0) {
      severe("pthread_rwlock_wrlock failed: %s", strerror(errno));
      // Best effort
  }
  /*
  ** Check size of file 
  */
  if (lv2->attributes.s.attrs.size >= RM_FILE_SIZE_TRESHOLD) {
      // Add to front of list
      list_push_front(&e->trash_buckets[hash].rmfiles, &rmfe->list);
  } else {
      // Add to back of list
      list_push_back(&e->trash_buckets[hash].rmfiles, &rmfe->list);
  }

  if ((errno = pthread_rwlock_unlock
          (&e->trash_buckets[hash].rm_lock)) != 0) {
      severe("pthread_rwlock_unlock failed: %s", strerror(errno));
      // Best effort
  }
  return 0;
}
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
int rozofs_mover_unlink_mover_distribution(export_t *e,lv2_entry_t *lv2)
{

   ext_mattr_t *attr_p;
   rozofs_mover_sids_t *dist_mv_p;   
   rozofs_mover_children_t mover_idx;  
   rozofs_mv_idx_dist_t trash_mv;
   
   attr_p = &lv2->attributes;
   mover_idx.u32 = attr_p->s.attrs.children;
   
   
   if (mover_idx.fid_st_idx.mover_idx == mover_idx.fid_st_idx.primary_idx)
   {
      /*
      ** nothing to do
      */
      return 0;
   }
   dist_mv_p = (rozofs_mover_sids_t*)&attr_p->s.attrs.sids;
   if (dist_mv_p->dist_t.mover_cid == 0)
   {
     /*
     ** no move pending
     */
     return 0;
   }
   trash_mv.mov_idx = mover_idx.fid_st_idx.mover_idx;
   trash_mv.cid = dist_mv_p->dist_t.mover_cid;
   memcpy(trash_mv.sids,dist_mv_p->dist_t.mover_sids,ROZOFS_SAFE_MAX_STORCLI); 

   return rozofs_mover_put_trash(e,lv2,&trash_mv);  
}
