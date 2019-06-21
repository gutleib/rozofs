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
#include <sys/param.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <uuid/uuid.h>
#include <errno.h>
#include <string.h>
 
//#include <rozofs/common/xmalloc.h>
//#include <rozofs/common/profile.h>
//#include <rozofs/rpc/epproto.h>
#include "cache.h"
#include "export.h"
#include "rozofs_exp_mover.h"
#include "export_thin_prov_api.h"

#include <rozofs/common/export_track.h>

#define EXP_MAX_FAKE_LVL2_ENTRIES 16
//#warning LV2_MAX_ENTRIES  2048
//#define LV2_MAX_ENTRIES (2048)
#define LV2_MAX_ENTRIES (512*1024)
#define LV2_BUKETS (1024*64)

lv2_entry_t  exp_fake_lv2_entry[EXP_MAX_FAKE_LVL2_ENTRIES];
unsigned int exp_fake_lv2_entry_idx = 0;
int rozofs_export_host_id = 0;   /**< reference between 0..7: default 0  */

/**
*  pointers table of the context associated with the eid: MAX is EXPGW_EID_MAX_IDX (see rozofs.h for details)
*/
export_tracking_table_t * export_tracking_table[EXPGW_EID_MAX_IDX+1] = { 0 };

time_t exp_lv2_last_locked_entry_in_lru = 0;

/**
 * hashing function used to find lv2 entry in the cache
 */
uint32_t lv2_hash(void *key) {
    uint32_t       hash = 0;
    uint8_t       *c;
    int            i;
    rozofs_inode_t fake_inode;
    
    /*
    ** Clear recycle counter in key (which is a FID)
    */
    memcpy(&fake_inode,key,sizeof(rozofs_inode_t));
    rozofs_reset_recycle_on_fid(&fake_inode);
    /*
    ** Clear the mover_idx since a lookup can by done from the storage node with a storage_fid encoding
    */
    fake_inode.s.mover_idx = 0;
    /*
    ** clear the delete pending bit
    */
    fake_inode.s.del = 0;

    c = (uint8_t *) &fake_inode;
    for (i = 0; i < sizeof(rozofs_inode_t); c++,i++)
        hash = *c + (hash << 6) + (hash << 16) - hash;
    return hash;
}

int lv2_cmp(void *k1, void *k2) {
    rozofs_inode_t fake_inode1;
    rozofs_inode_t fake_inode2;  
      
    /*
    ** Clear recycle counter in keys (which are FIDs)
    */
    memcpy(&fake_inode1,k1,sizeof(rozofs_inode_t));
    rozofs_reset_recycle_on_fid(&fake_inode1);
    fake_inode1.s.del = 0;
    memcpy(&fake_inode2,k2,sizeof(rozofs_inode_t));
    rozofs_reset_recycle_on_fid(&fake_inode2);
    fake_inode2.s.del = 0;    
    return uuid_compare((uint8_t*)&fake_inode1, (uint8_t*)&fake_inode2);
}
/*
**__________________________________________________________________
*/
/**
*   Remove an entry from the attribute cache

    @param: pointer to the cache context
    @param: pointer to entry to remove
    
    @retval none
*/
static inline void lv2_cache_unlink(lv2_cache_t *cache,lv2_entry_t *entry) {

  file_lock_remove_fid_locks(&entry->file_lock);
#ifndef LIBROZO_FLAG
  /*
  ** check if the entry to remove is a directory, in such a case we need to check
  ** if the entry is dirty and needs to be re-write on disk
  */
  if (S_ISDIR(entry->attributes.s.attrs.mode))
  {
     export_dir_check_sync_write_on_lru(entry);  
  }
#endif  
  mattr_release(&entry->attributes.s.attrs);
  /*
  ** check the presence of the extended attribute block and free it
  */
  if (entry->extended_attr_p != NULL) free(entry->extended_attr_p);
  /*
  ** check the presence of the root_idx bitmap : for directory only
  */
  if (entry->dirent_root_idx_p != NULL) free(entry->dirent_root_idx_p); 
  /*
  ** Remove symbolic link name if any 
  */
  if (entry->symlink_target != NULL) free(entry->symlink_target);
  /*
  ** release slave inode if any 
  */
  if (entry->slave_inode_p != NULL) xfree(entry->slave_inode_p);
  
  list_remove(&entry->list);
  /*
  ** remove from the move_list
  */
  list_remove(&entry->move_list); 
  /*
  ** check if there was a thin-provisioning context. If it the case we should release the
  ** entry
  */
#ifndef LIBROZO_FLAG
  expthin_remove_entry(entry);
#endif   
  free(entry);
  cache->size--;  
}
/*
**__________________________________________________________________
*/
/**
*   init of an exportd attribute cache

    @param: pointer to the cache context
    
    @retval none
*/
void lv2_cache_initialize(lv2_cache_t *cache) {
    int i;
    cache->max = LV2_MAX_ENTRIES;
    cache->size = 0;
    cache->hit  = 0;
    cache->miss = 0;
    cache->lru_del = 0;
    list_init(&cache->lru);
    list_init(&cache->flock_list);
    htable_initialize(&cache->htable, LV2_BUKETS, lv2_hash, lv2_cmp);
    for (i = 0; i < EXPORT_LV2_MAX_LOCK; i++)
    {
      list_init(&cache->lru_th[i]);
      list_init(&cache->flock_list_th[i]);    
    }
    memset(cache->hash_stats,0,sizeof(uint64_t)*EXPORT_LV2_MAX_LOCK);
    
    /* 
    ** Lock service initalize 
    */
    file_lock_service_init();
}
/*
**__________________________________________________________________
*/
/**
*   delete of an exportd attribute cache

    @param: pointer to the cache context
    
    @retval none
*/
void lv2_cache_release(lv2_cache_t *cache) {
    list_t *p, *q;

    /*
    ** Check that the cache has been initialized
    ** before trying to release it
    */
    if (cache==NULL) return;
    if (cache->size==0) return;
    
    list_for_each_forward_safe(p, q, &cache->lru) {
        lv2_entry_t *entry = list_entry(p, lv2_entry_t, list);
        htable_del(&cache->htable, entry->attributes.s.attrs.fid);
	lv2_cache_unlink(cache,entry);
    }
    list_for_each_forward_safe(p, q, &cache->flock_list) {
        lv2_entry_t *entry = list_entry(p, lv2_entry_t, list);
        htable_del(&cache->htable, entry->attributes.s.attrs.fid);
	lv2_cache_unlink(cache,entry);
    }
}
/*
**__________________________________________________________________
*/
/**
*   Get an enry from the attributes cache

    @param: pointer to the cache context
    @param: fid : key of the element to find
    
    @retval <>NULL : pointer to the cache entry that contains the attributes
    @retval NULL: not found
*/
lv2_entry_t *lv2_cache_get(lv2_cache_t *cache, fid_t fid) {
    lv2_entry_t *entry = 0;

//    START_PROFILING(lv2_cache_get);

    if ((entry = htable_get(&cache->htable, fid)) != 0) {
        // Update the lru
        lv2_cache_update_lru(cache,entry); 
	cache->hit++;
    }
    else {
      cache->miss++;
    }

//    STOP_PROFILING(lv2_cache_get);
    return entry;
}
/*
**__________________________________________________________________
*/
/**
   read the attributes from disk
  
   attributes can be the one of a regular file, symbolic link or a directory.
   The type of the object is indicated within the lower part of the fid (field key)
   
   @param trk_tb_p: export attributes tracking table
   @param fid: unique file identifier
   @param entry : pointer to the array where attributes will be returned
   
   @retval 0 on success
   @retval -1 on error (see errno for details
*/
int exp_meta_get_object_attributes(export_tracking_table_t *trk_tb_p,fid_t fid,lv2_entry_t *entry_p)
{
   int ret;
   rozofs_inode_t *fake_inode;
   exp_trck_top_header_t *p = NULL;
   
   fake_inode = (rozofs_inode_t*)fid;
   
   if (fake_inode->s.key >= ROZOFS_MAXATTR)
   {
     errno = EINVAL;
     return -1;
   }
   p = trk_tb_p->tracking_table[fake_inode->s.key];
   if (p == NULL)
   {
     errno = ENOTSUP;
     return -1;    
   }   
   /*
   ** read the attributes from disk
   */
   ret = exp_metadata_read_attributes(p,fake_inode,entry_p,sizeof(ext_mattr_t));
   if (ret < 0)
   { 
     return -1;
   } 
  /*
   ** Check that the FID in the read attribute matches the input FID
   */
   {
     rozofs_inode_t * read_inode_p = (rozofs_inode_t *) entry_p->attributes.s.attrs.fid;
     if ((read_inode_p->s.eid     != fake_inode->s.eid) 
     ||  (read_inode_p->s.usr_id  != fake_inode->s.usr_id) 
     ||  (read_inode_p->s.file_id != fake_inode->s.file_id) 
     ||  (read_inode_p->s.idx     != fake_inode->s.idx)) {
       /*
       ** The read inode does not match the input inode
       */
       errno = ENOENT;
       return -1;
     } 
   }     
   /*
   ** Check the case of a regular file with slave inodes (multiple file mode)
   */
   if ((fake_inode->s.key == ROZOFS_REG) && (entry_p->attributes.s.multi_desc.common.master != 0))
   {
     int attr_sz;
      /*
      ** Get the striping factor of the master inode in order to find out how many slave inodes need
      ** to be read
      */
      attr_sz = sizeof(ext_mattr_t)*(entry_p->attributes.s.multi_desc.master.striping_factor+1);
      /*
      ** allocate the memory array for the slave inodes
      */
      entry_p->slave_inode_p = xmalloc(attr_sz);
      if (entry_p->slave_inode_p== NULL)
      {
        severe("Out of memory while loading slave inodes");
	errno = ENOMEM;
	return -1;
      }
      /*
      ** move the pointer to the first index of the slave inodes
      */
      if (exp_metadata_read_slave_inode_burst(p,fake_inode,entry_p->slave_inode_p,attr_sz) < 0)
      {
         /*
	 ** release the allocated memory
	 */
	 int xerror = errno;
	 
	 xfree(entry_p->slave_inode_p);
	 entry_p->slave_inode_p = NULL;
	 errno = xerror;
	 return -1;
      }	    
   }
   return 0; 
}

/*
**__________________________________________________________________
*/
/**
   read the extended attributes block from disk
  
   attributes can be the one of a regular file, symbolic link or a directory.
   The type of the object is indicated within the lower part of the fid (field key)
   
   @param trk_tb_p: export attributes tracking table
   @param entry : pointer to the array where attributes will be returned
   
   @retval 0 on success
   @retval -1 on error (see errno for details
*/
int exp_meta_get_xattr_block(export_tracking_table_t *trk_tb_p,lv2_entry_t *entry_p)
{
   int ret;
   rozofs_inode_t fake_inode;
   exp_trck_top_header_t *p = NULL;
   
   fake_inode.fid[1] = entry_p->attributes.s.i_file_acl;
   
   if (fake_inode.s.key >= ROZOFS_MAXATTR)
   {
     errno = EINVAL;
     return -1;
   }
   p = trk_tb_p->tracking_table[fake_inode.s.key];
   if (p == NULL)
   {
     errno = ENOTSUP;
     return -1;    
   }   
   /*
   ** read the attributes from disk
   */
   ret = exp_metadata_read_attributes(p,&fake_inode,entry_p->extended_attr_p,ROZOFS_XATTR_BLOCK_SZ);
   if (ret < 0)
   { 
     return -1;
   }  
   return 0; 
}
/*
**__________________________________________________________________
*/
/**
*   The purpose of that service is to read object attributes and store them in the attributes cache

  @param trk_tb_p: export attributes tracking table
  @param cache : pointer to the export attributes cache
  @param fid : unique identifier of the object
  
  @retval <> NULL: attributes of the object
  @retval == NULL : no attribute returned for the object (see errno for details)
*/

lv2_entry_t *lv2_cache_put(export_tracking_table_t *trk_tb_p,lv2_cache_t *cache, fid_t fid) {
    lv2_entry_t *entry;
    int count=0;
    rozofs_inode_t *fake_inode,*fake_inode_attr;
   
    fake_inode = (rozofs_inode_t*)fid;
//    START_PROFILING(lv2_cache_put);

    // maybe already cached.
    if ((entry = htable_get(&cache->htable, fid)) != 0) {
        goto out;
    }
    entry = malloc(sizeof(lv2_entry_t));
    if (entry == NULL)
    {
       severe("lv2_cache_put: %s\b",strerror(errno));
       return NULL;
    }
    memset(entry,0,sizeof(lv2_entry_t));

    /*
    ** get the attributes of the object
    */
    if (exp_meta_get_object_attributes(trk_tb_p,fid,entry) < 0)
    {
      /*
      ** cannot get the attributes: need to log the returned errno
      */
      goto error;
    }
    /*
    ** case of the fid recycle
    */

    if (fake_inode->s.key == ROZOFS_REG)
    {
      fake_inode_attr = (rozofs_inode_t*)entry->attributes.s.attrs.fid;
      if( rozofs_get_recycle_from_fid(fake_inode) !=  fake_inode_attr->s.recycle_cpt)
      {
         /*
	 ** it correspond to the case where the fid has been recycled
	 */
         goto error;
      }
    }    
    /*
    ** Initialize file locking 
    */
    list_init(&entry->file_lock);
    entry->nb_locks = 0;
    /*
    ** Need eventually to re install the persistent file locks saved in extended attributes
    */
    rozofs_reload_flockp(entry,trk_tb_p);    
    
    list_init(&entry->list);
    /*
    ** init of the move list
    */
    list_init(&entry->move_list);
    /*
    ** Try to remove older entries
    */
    count = 0;
    while ((cache->size >= cache->max) && (!list_empty(&cache->lru))){ 
      lv2_entry_t *lru;
		
	  lru = list_entry(cache->lru.prev, lv2_entry_t, list);  
          
          /*
          ** An entry with a lock is in the LRU list
          ** This should not happen !
          */
 	  if (lru->nb_locks != 0) {
            {
              char     msgString[128];
              char *   pChar = msgString;
              pChar += sprintf(pChar,"lv2 with lock in LRU ");
              pChar += rozofs_fid_append(pChar,lru->attributes.s.attrs.fid);
              severe("%s",msgString);
            }  
            /*
            ** Replace it the right list
            */
            lv2_cache_update_lru(cache,lru);
 	  }
          
          /*
	  ** This is suprising that a locked in cache entry (moved by rebalancer)
          ** is at the end of the lru. This is worth a warning. 
	  */
	  else if (lru->locked_in_cache) {
            time_t now = time(NULL);

            /*
            ** avoid too much syslog
            */
            if (now-exp_lv2_last_locked_entry_in_lru > 30) {
              {
                char     msgString[128];
                char *   pChar = msgString;            
                pChar += sprintf(pChar,"locked_in_cache entry ");
                pChar += rozofs_fid_append(pChar,lru->attributes.s.attrs.fid);
                warning("%s",msgString);
              }
              exp_lv2_last_locked_entry_in_lru = now;
            } 
            /*
            ** Push the entry up
            */
            lv2_cache_update_lru(cache,lru);
          }
          
          /*
          ** Remove this old entry from the cache
          */
          else {
  	    htable_del(&cache->htable, lru->attributes.s.attrs.fid);
	    lv2_cache_unlink(cache,lru);
	    cache->lru_del++;
          }
          
	  count++;
	  if (count >= 3) break;
    }
    /*
    ** Insert the new entry
    */
    lv2_cache_update_lru(cache,entry);
    htable_put(&cache->htable, entry->attributes.s.attrs.fid, entry);
    cache->size++;    

    goto out;
error:
    if (entry) {
        free(entry);
        entry = 0;
    }
out:
//    STOP_PROFILING(lv2_cache_put);
    return entry;
}



/*
**__________________________________________________________________
*/
/**
*   The purpose of that service is to store object attributes in the attributes cache

  @param attr_p: pointer to the attributes of the object
  @param cache : pointer to the export attributes cache
  @param fid : unique identifier of the object
  @param slave_inode_p: pointer to the slave_inode: might be NULL
  
  @retval <> NULL: attributes of the object
  @retval == NULL : no attribute returned for the object (see errno for details)
*/

static inline lv2_entry_t *lv2_cache_put_forced_internal(lv2_cache_t *cache, fid_t fid,ext_mattr_t *attr_p,ext_mattr_t *slave_inode_p) {
    lv2_entry_t *entry;
    int count=0;

    // maybe already cached.
    if ((entry = htable_get(&cache->htable, fid)) != 0) {
        goto out;
    }
    entry = malloc(sizeof(lv2_entry_t));
    if (entry == NULL)
    {
       severe("lv2_cache_put: %s\b",strerror(errno));
       return NULL;
    }
    memset(entry,0,sizeof(lv2_entry_t));

    /*
    ** copy the attributes
    */
    memcpy(entry,attr_p,sizeof( ext_mattr_t));
    entry->slave_inode_p = slave_inode_p;
    /*
    ** Initialize file locking 
    */
    list_init(&entry->file_lock);
    entry->nb_locks = 0;
    list_init(&entry->list);
    /*
    ** init of the move list
    */
    list_init(&entry->move_list);
    /*
    ** Try to remove older entries
    */
    count = 0;
    while ((cache->size >= cache->max) && (!list_empty(&cache->lru))){ 
      lv2_entry_t *lru;
		
	  lru = list_entry(cache->lru.prev, lv2_entry_t, list);  
          
          /*
          ** An entry with a lock is in the LRU list
          ** This should not happen !
          */
 	  if (lru->nb_locks != 0) {
            {
              char     msgString[128];
              char *   pChar = msgString;
              pChar += sprintf(pChar,"lv2 with lock in LRU ");
              pChar += rozofs_fid_append(pChar,lru->attributes.s.attrs.fid);
              severe("%s",msgString);
            }  
            /*
            ** Replace it the right list
            */
            lv2_cache_update_lru(cache,lru);
 	  }
          
          /*
	  ** This is suprising that a locked in cache entry (moved by rebalancer)
          ** is at the end of the lru. This is worth a warning. 
	  */
	  else if (lru->locked_in_cache) {
            time_t now = time(NULL);

            /*
            ** avoid too much syslog
            */
            if (now-exp_lv2_last_locked_entry_in_lru > 30) {
              {
                char     msgString[128];
                char *   pChar = msgString;            
                pChar += sprintf(pChar,"locked_in_cache entry ");
                pChar += rozofs_fid_append(pChar,lru->attributes.s.attrs.fid);
                warning("%s",msgString);
              }
              exp_lv2_last_locked_entry_in_lru = now;
            } 
            /*
            ** Push the entry up
            */
            lv2_cache_update_lru(cache,lru);
          }
          
          /*
          ** Remove this old entry from the cache
          */
          else {
	    htable_del(&cache->htable, lru->attributes.s.attrs.fid);
	    lv2_cache_unlink(cache,lru);
	    cache->lru_del++;
          }
	  count++;
	  if (count >= 3) break;
    }
    /*
    ** Insert the new entry
    */
    lv2_cache_update_lru(cache,entry);
    htable_put(&cache->htable, entry->attributes.s.attrs.fid, entry);
    cache->size++;    
out:
    return entry;
}
/*
**__________________________________________________________________
*/
/**
*   The purpose of that service is to store object attributes in the attributes cache

  @param attr_p: pointer to the attributes of the object
  @param cache : pointer to the export attributes cache
  @param fid : unique identifier of the object
  
  @retval <> NULL: attributes of the object
  @retval == NULL : no attribute returned for the object (see errno for details)
*/

lv2_entry_t *lv2_cache_put_forced(lv2_cache_t *cache, fid_t fid,ext_mattr_t *attr_p) {
   return lv2_cache_put_forced_internal(cache,fid,attr_p,NULL);
}


/*
**__________________________________________________________________
*/
/**
*   The purpose of that service is to store object master and slaves attributes in the attributes cache

  @param attr_p: pointer to the attributes of the object
  @param cache : pointer to the export attributes cache
  @param fid : unique identifier of the object
  
  @retval <> NULL: attributes of the object
  @retval == NULL : no attribute returned for the object (see errno for details)
*/

lv2_entry_t *lv2_cache_put_forced_multiple(lv2_cache_t *cache, fid_t fid,ext_mattr_t *attr_p,ext_mattr_t *slave_inode_p) {
   return lv2_cache_put_forced_internal(cache,fid,attr_p,slave_inode_p);
}

/*
**__________________________________________________________________
*/
/**
*   delete an entry from the attribute cache

    @param cache: pointer to the level 2 cache
    @param fid : key of the entry to remove
*/
void lv2_cache_del(lv2_cache_t *cache, fid_t fid) 
{
    lv2_entry_t *entry = 0;
//    START_PROFILING(lv2_cache_del);

    if ((entry = htable_del(&cache->htable, fid)) != 0) {
	lv2_cache_unlink(cache,entry);
    }
//    STOP_PROFILING(lv2_cache_del);
}
/*
**__________________________________________________________________
*/
/**
*   Remove an entry from the attribute cache without deleting it

    @param cache: pointer to the level 2 cache
    @param fid : key of the entry to remove
*/
void lv2_cache_remove_hash(lv2_cache_t *cache, fid_t fid) 
{
    lv2_entry_t *entry = 0;

    if ((entry = htable_del(&cache->htable, fid)) != 0) {
        /*
        ** De assert locked_in_cache, otherwise the entry an be stuck in
        ** the lru for ever
        */
	lv2_cache_unlock_entry_in_cache(entry);
    }
}

/*
**__________________________________________________________________
*/
/** search a fid in the attribute cache
 
 if fid is not cached, try to find it on the underlying file system
 and cache it.
 
  @param trk_tb_p: export attributes tracking table
  @param cache: pointer to the cache associated with the export
  @param fid: the searched fid_orig
  @param mover_invalidate: assert to 1 where mover can be invalidated
 
  @return a pointer to lv2 entry or null on error (errno is set)
*/
lv2_entry_t *export_lookup_fid_internal(export_tracking_table_t *trk_tb_p,lv2_cache_t *cache, fid_t fid_orig,int mover_invalidate) {
    lv2_entry_t *lv2 = 0;
    rozofs_inode_t fake_inode;
    rozofs_inode_t *fid = &fake_inode;
    rozofs_inode_t *fake_inode_src_p = (rozofs_inode_t*)fid_orig;
    uint32_t slice;
//    START_PROFILING(export_lookup_fid);
    /*
    ** use a temporary FID for doing the lookup
    */
    fid->fid[0] = fake_inode_src_p->fid[0];
    fid->fid[1] = fake_inode_src_p->fid[1];
    
    if ((fid->fid[0]==0) && (fid->fid[1]==0))
    {
      return NULL;
    }   
    if ((fid->s.key == ROZOFS_REG_S_MOVER) || (fid->s.key == ROZOFS_REG_D_MOVER))
    {
       /*
       ** switch back to the ROZOFS_REG type for the lookup
       */
       fid->s.key = ROZOFS_REG;
    }
    /*
    ** check the case of @rozofs-trash@ directory: need to set the type to directory for doing the
    ** lookup.
    */
    if (fid->s.key == ROZOFS_TRASH)
    {
       /*
       ** switch back to the ROZOFS_REG type for the lookup
       */
       fid->s.key = ROZOFS_DIR;
    }
    /*
    ** clear the delete pending bit from the fid
    */
    fid->s.del = 0;
    /*
    ** get the slice of the fid :extracted from the upper part 
    */
    exp_trck_get_slice((unsigned char *)fid,&slice);
    /*
    ** check if the slice is local
    */
    if (exp_trck_is_local_slice(slice))
    {
      if (!(lv2 = lv2_cache_get(cache, (unsigned char *)fid))) {
          // not cached, find it an cache it
          if (!(lv2 = lv2_cache_put(trk_tb_p,cache,(unsigned char *) fid))) {
              goto out;
          }
      }
    }
    else
    {
      int idx  = (++exp_fake_lv2_entry_idx)%(EXP_MAX_FAKE_LVL2_ENTRIES);
      lv2 = &exp_fake_lv2_entry[idx];
      if (exp_meta_get_object_attributes(trk_tb_p,(unsigned char *)fid,lv2) < 0)
      {
	/*
	** cannot get the attributes: need to log the returned errno
	*/
	lv2 = NULL;
      } 
      else
      {
         memset(lv2,0,sizeof(lv2_entry_t));
      }         
    }
out:
    if (lv2!= NULL)
    {
       if ((fake_inode_src_p->s.key != ROZOFS_REG_S_MOVER) && (fake_inode_src_p->s.key != ROZOFS_REG_D_MOVER))
       {
          rozofs_mover_invalidate(lv2);
       }
    }
//    STOP_PROFILING(export_lookup_fid);
    return lv2;
}
/*
**__________________________________________________________________
*/
/** search a fid in the attribute cache
 
 if fid is not cached, try to find it on the underlying file system
 and cache it.
 
  @param trk_tb_p: export attributes tracking table
  @param cache: pointer to the cache associated with the export
  @param fid_orig: the searched fid
 
  @return a pointer to lv2 entry or null on error (errno is set)
*/
lv2_entry_t *export_lookup_fid(export_tracking_table_t *trk_tb_p,lv2_cache_t *cache, fid_t fid_orig) {
    return export_lookup_fid_internal(trk_tb_p,cache,fid_orig,0);
}

/*
**__________________________________________________________________
*/
/** search a fid in the attribute cache
 
 if fid is not cached, try to find it on the underlying file system
 and cache it.
 
  @param trk_tb_p: export attributes tracking table
  @param cache: pointer to the cache associated with the export
  @param fid: the searched fid
 
  @return a pointer to lv2 entry or null on error (errno is set)
*/
lv2_entry_t *export_lookup_fid_no_invalidate_mover(export_tracking_table_t *trk_tb_p,lv2_cache_t *cache, fid_t fid_orig) {
    return export_lookup_fid_internal(trk_tb_p,cache,fid_orig,0);
}
/*
**__________________________________________________________________
*/
/** store the attributes part of an attribute cache entry  to the export's file system
 *
   @param trk_tb_p: export attributes tracking table
   @param entry: the entry used
   @param sync: whether to force sync on disk of attributes
 
   @return: 0 on success otherwise -1
 */
int export_lv2_write_attributes(export_tracking_table_t *trk_tb_p,lv2_entry_t *entry,int sync) {

   int ret;
   rozofs_inode_t *fake_inode;
   exp_trck_top_header_t *p = NULL;
   
   fake_inode = (rozofs_inode_t*)entry->attributes.s.attrs.fid;
   if (fake_inode->s.key >= ROZOFS_MAXATTR)
   {
     errno = EINVAL;
     return -1;
   }
   p = trk_tb_p->tracking_table[fake_inode->s.key];
   if (p == NULL)
   {
     errno = ENOTSUP;
     return -1;    
   }
   /*
   ** read the attributes from disk
   */
   ret = exp_metadata_write_attributes(p,fake_inode,entry,sizeof(ext_mattr_t), sync);
   if (ret < 0)
   { 
     return -1;
   }  
   return 0; 
}
/*
**__________________________________________________________________
*/
/**
*    delete an inode associated with an object

   @param trk_tb_p: export attributes tracking table
   @param fid: fid of the object (key)
   
   @retval 0 on success
   @retval -1 on error
*/
int exp_attr_delete(export_tracking_table_t *trk_tb_p,fid_t fid)
{
   rozofs_inode_t *fake_inode;
   exp_trck_top_header_t *p = NULL;
   int ret;

   fake_inode = (rozofs_inode_t*)fid;
   if (fake_inode->s.key >= ROZOFS_MAXATTR)
   {
     errno = EINVAL;
     return -1;
   }
   p = trk_tb_p->tracking_table[fake_inode->s.key];
   if (p == NULL)
   {
     errno = ENOTSUP;
     return -1;    
   }   
   /*
   ** release the inode
   */
   ret = exp_metadata_release_inode(p,fake_inode);
   if (ret < 0)
   { 
      return -1;
   }
   return 0;
}

/*
**__________________________________________________________________
*/
/**
*    delete a master inode with its associated slave inodes (case of the multiple file mode)

   @param trk_tb_p: export attributes tracking table
   @param fid: fid of the object (key)
   @param nb_slaves: number of slave inodes
   
   @retval 0 on success
   @retval -1 on error
*/
int exp_attr_delete_multiple(export_tracking_table_t *trk_tb_p,fid_t fid,int nb_slaves)
{
   rozofs_inode_t *fake_inode;
   exp_trck_top_header_t *p = NULL;
   int ret;

   fake_inode = (rozofs_inode_t*)fid;
   if (fake_inode->s.key >= ROZOFS_MAXATTR)
   {
     errno = EINVAL;
     return -1;
   }
   p = trk_tb_p->tracking_table[fake_inode->s.key];
   if (p == NULL)
   {
     errno = ENOTSUP;
     return -1;    
   }   
   /*
   ** release the inode
   */
   ret = exp_metadata_release_inode_multiple(p,fake_inode,nb_slaves);
   if (ret < 0)
   { 
      return -1;
   }
   return 0;
}


/*
**__________________________________________________________________
*/
/**
*  Create the attributes of a regular file in burst mode without writing attributes on disk

  create an oject according to its type. The service performs the allocation of the fid. 
  It is assumed that all the other fields of the object attributes are already been filled in.
  
  @param trk_tb_p: export attributes tracking table
  @param slice: slice of the parent directory
  @param global_attr_p : pointer to the attributes of the object (master inode attributes)
  @param inode_count: number of consecutive inode to allocated

  
  @retval 0 on success: (the attributes contains the lower part of the fid that is allocated by the service)
  @retval -1 on error (see errno for details)
*/
int exp_attr_create_write_cond_burst(export_tracking_table_t *trk_tb_p,uint32_t slice,ext_mattr_t *global_attr_p,int inode_count)
{
   fid_t fid;
   rozofs_inode_t *fake_inode;
   int ret;
   exp_trck_top_header_t *p = NULL;
   int inode_allocated = 0;

   
   rozofs_uuid_generate(fid,rozofs_get_export_host_id());


   fake_inode = (rozofs_inode_t*)fid;
   fake_inode->fid[1] = 0;
   fake_inode->s.key = ROZOFS_REG;
   fake_inode->s.usr_id = slice; /** always the parent slice for storage */
   fake_inode->s.eid = trk_tb_p->eid; 
      
   if (fake_inode->s.key >= ROZOFS_MAXATTR)
   {
     errno = EINVAL;
     return -1;
   }
   p = trk_tb_p->tracking_table[fake_inode->s.key];
   if (p == NULL)
   {
     errno = ENOTSUP;
     goto error;
   }   
   /*
   ** allocate the inode
   */
   ret = exp_metadata_allocate_inode_burst(p,fake_inode,ROZOFS_REG,slice,inode_count);
   if (ret < 0)
   { 
      goto error;
   }
   inode_allocated = 1;
   /*
   ** copy the definitive fid of the object
   */
   memcpy(&global_attr_p->s.attrs.fid,fake_inode,sizeof(fid_t));

   return 0;

error:
   if (inode_allocated)
   {
     exp_attr_delete(trk_tb_p,global_attr_p->s.attrs.fid);           
   }
   return -1;
}


/*
**__________________________________________________________________
*/
/**
*  Create the attributes of a directory/regular file or symbolic link without write attributes on disk

  create an oject according to its type. The service performs the allocation of the fid. 
  It is assumed that all the other fields of the object attributes are already been filled in.
  
  @param trk_tb_p: export attributes tracking table
  @param slice: slice of the parent directory
  @param global_attr_p : pointer to the attributes of the object
  @param type: type of the object (ROZOFS_REG: regular file, ROZOFS_SLNK: symbolic link, ROZOFS_DIR : directory
  @param link: pointer to the symbolic link (significant for ROZOFS_SLNK only)
  
  @retval 0 on success: (the attributes contains the lower part of the fid that is allocated by the service)
  @retval -1 on error (see errno for details)
*/
int exp_attr_create_write_cond(export_tracking_table_t *trk_tb_p,uint32_t slice,ext_mattr_t *global_attr_p,int type,char *link,uint8_t write)
{
   fid_t fid;
   rozofs_inode_t *fake_inode;
   rozofs_inode_t fake_inode_link;
   int ret;
   exp_trck_top_header_t *p = NULL;
   exp_trck_top_header_t *p_link = NULL;
   uint32_t link_slice;
   int inode_allocated = 0;
   int link_allocated = 0;
   int sync = 1;
   
   rozofs_uuid_generate(fid,rozofs_get_export_host_id());


   fake_inode = (rozofs_inode_t*)fid;
   fake_inode->fid[1] = 0;
   fake_inode->s.key = type;
   fake_inode->s.usr_id = slice; /** always the parent slice for storage */
   fake_inode->s.eid = trk_tb_p->eid; 
      
   if (fake_inode->s.key >= ROZOFS_MAXATTR)
   {
     errno = EINVAL;
     return -1;
   }
   p = trk_tb_p->tracking_table[fake_inode->s.key];
   if (p == NULL)
   {
     errno = ENOTSUP;
     goto error;
   }   
   /*
   ** allocate the inode
   */
   ret = exp_metadata_allocate_inode(p,fake_inode,type,slice);
   if (ret < 0)
   { 
      goto error;
   }
   inode_allocated = 1;
   /*
   ** copy the definitive fid of the object
   */
   memcpy(&global_attr_p->s.attrs.fid,fake_inode,sizeof(fid_t));
   if (link == NULL)
   {
     if (write)
     {
       /*
       ** write the metadata on disk for the directory and the regular file
       */
       ret = exp_metadata_write_attributes(p,fake_inode,global_attr_p,sizeof(ext_mattr_t),sync);
       if (ret < 0)
       {
	 goto error;
       }
    }
    return 0;
  }
  /*
  ** case of a symbolic link: here we need to allocate a block for storing the link
  ** the slice associated with the link is the one associated with the fid of the file
  ** and not the fid of the parent directory
  */
  exp_trck_get_slice(global_attr_p->s.attrs.fid,&link_slice);

  fake_inode = (rozofs_inode_t*)fid;
  fake_inode_link.fid[1] = 0;
  fake_inode_link.s.key = ROZOFS_SLNK;
  fake_inode_link.s.usr_id = link_slice; 
  fake_inode_link.s.eid = trk_tb_p->eid;   

  p_link = trk_tb_p->tracking_table[ROZOFS_SLNK];
  if (p_link == NULL)
  {
    errno = ENOTSUP;
    goto error;
  }  
   /*
   ** allocate the inode
   */
   ret = exp_metadata_allocate_inode(p_link,&fake_inode_link,ROZOFS_SLNK,link_slice);
   if (ret < 0)
   { 
    goto error;
   }
   link_allocated = 1;
   /*
   ** write the link value
   */
   int len = strlen(link);
   ret = exp_metadata_write_attributes(p_link,&fake_inode_link,link,len+1,sync);
   if (ret < 0)
   {
    goto error;
   }
   /*
   ** update the inode with the reference of the block allocated for storing the link value
   */
   global_attr_p->s.i_link_name = fake_inode_link.fid[1];
   if (write)
   {
     ret = exp_metadata_write_attributes(p,fake_inode,global_attr_p,sizeof(ext_mattr_t),sync);
     if (ret < 0)
     {  
       goto error;
     }   
   }
   return 0;

error:
   if (inode_allocated)
   {
     exp_attr_delete(trk_tb_p,global_attr_p->s.attrs.fid);           
   }
   if (link_allocated)
   {
     memcpy(fid,&fake_inode_link,sizeof(fid_t));
     exp_attr_delete(trk_tb_p,fid);           
   }
   return -1;
}





/*
**__________________________________________________________________
*/
/**
*  Create the extended attributes of a directory/regular file or symbolic link

  create an oject according to its type. The service performs the allocation of the fid. 
  It is assumed that all the other fields of the object attributes are already been filled in.
  
  @param trk_tb_p: export attributes tracking table
  @param entry : pointer to the inode and extended attributes of the inode (header)
  @param block_ref_p : pointer to the reference of the allocated block (Not Significant if retval is -1)
  
  @retval 0 on success: (the attributes contains the lower part of the fid that is allocated by the service)
  @retval -1 on error (see errno for details)
*/
int exp_xattr_block_create(export_tracking_table_t *trk_tb_p,lv2_entry_t *entry,uint64_t *block_ref_p)
{
   rozofs_inode_t fake_inode;
   int ret;
   exp_trck_top_header_t *p = NULL;
   uint32_t slice;
   uint64_t *xattr_p;   
   /*
   ** get slice of the fid of the inode
   */
   exp_trck_get_slice(entry->attributes.s.attrs.fid,&slice); 
   /*
   ** prepare the fid for the extended block
   */
   fake_inode.fid[1] = 0;
   fake_inode.s.key = ROZOFS_EXTATTR;
   fake_inode.s.usr_id = slice; /** always the inode slice for storage */
   fake_inode.s.eid = trk_tb_p->eid;   
   
   p = trk_tb_p->tracking_table[fake_inode.s.key];
   if (p == NULL)
   {
     errno = ENOTSUP;
     return -1;    
   }   
   /*
   ** allocate the extended block
   */
   ret = exp_metadata_allocate_inode(p,&fake_inode,ROZOFS_EXTATTR,slice);
   if (ret < 0)
   { 
      return -1;
   }
   /*
   ** Skip the very 1rst block that has a ref of 0 
   */	
   if (fake_inode.fid[1] == 0) {
     // Skip NULL reference buffer that could be interpreted as no buffer
     // info("skip null");
     exp_metadata_release_inode(p,&fake_inode);
     ret = exp_metadata_allocate_inode(p,&fake_inode,ROZOFS_EXTATTR,slice);
     if (ret < 0)
     { 
	return -1;
     }
   }
   /*
   ** get the reference of the block since we will need it for the inode
   */
   *block_ref_p = fake_inode.fid[1];   
   /*
   ** write back the inode reference in the header of the extended attribute block
   */
   xattr_p = entry->extended_attr_p;
   xattr_p[(ROZOFS_XATTR_BLOCK_SZ/sizeof(uint64_t))-1] = fake_inode.fid[1];
   /*
   ** write the extended attribute block  on disk
   */
   ret = exp_metadata_write_attributes(p,&fake_inode,xattr_p,ROZOFS_XATTR_BLOCK_SZ, 0 /* not sync */);
   if (ret < 0)
   {
     return -1;
   }
   return 0;
}

/*
**__________________________________________________________________
*/
/** store the extended attributes part of an attribute cache entry to the export's file system
 *
   @param trk_tb_p: export attributes tracking table
   @param entry: the entry used
 
   @return: 0 on success otherwise -1
 */
int export_lv2_write_xattr(export_tracking_table_t *trk_tb_p,lv2_entry_t *entry) {

   int ret;
   rozofs_inode_t fake_inode;
   exp_trck_top_header_t *p = NULL;
   
   fake_inode.fid[1] = entry->attributes.s.i_file_acl;

   p = trk_tb_p->tracking_table[ROZOFS_EXTATTR];
   if (p == NULL)
   {
     errno = ENOTSUP;
     return -1;    
   }
   if (entry->extended_attr_p == NULL)
   {
     errno = EINVAL;
     return -1;
   }
   /*
   ** read the attributes from disk
   */
   ret = exp_metadata_write_attributes(p,&fake_inode,entry->extended_attr_p,ROZOFS_XATTR_BLOCK_SZ, 0 /* not sync */);
   if (ret < 0)
   { 
     return -1;
   }  
   return 0; 
}

/*
**__________________________________________________________________
*/
/**
*  Create an entry in the trash for a file to delete

 
  
  @param trk_tb_p: export attributes tracking table
  @param slice: slice of the parent directory
  @param global_attr_p : pointer to the attributes relative to the object to delete
  @param link: pointer to the symbolic link (significant for ROZOFS_SLNK only)
  
  @retval 0 on success: (the attributes contains the lower part of the fid that is allocated by the service)
  @retval -1 on error (see errno for details)
*/
int exp_trash_entry_create(export_tracking_table_t *trk_tb_p,uint32_t slice,void *ptr)
{
   rozofs_inode_t fake_inode;
   int ret;
   exp_trck_top_header_t *p = NULL;
   rmfentry_disk_t *global_attr_p = (rmfentry_disk_t *)ptr;
   
   fake_inode.fid[0] = 0;
   fake_inode.fid[1] = 0;   
   fake_inode.s.key = ROZOFS_TRASH;
   fake_inode.s.usr_id = slice; 
   fake_inode.s.eid = trk_tb_p->eid;   
      
   p = trk_tb_p->tracking_table[fake_inode.s.key];
   if (p == NULL)
   {
     errno = ENOTSUP;
     return -1;    
   }   
   /*
   ** allocate the inode
   */
   ret = exp_metadata_allocate_inode(p,&fake_inode,ROZOFS_TRASH,slice);
   if (ret < 0)
   { 
      return -1;
   }

   /*
   ** copy the reference of the trash inode
   */
   memcpy(&global_attr_p->trash_inode,&fake_inode,sizeof(rozofs_inode_t));
   /*
   ** write the metadata on disk
   */
   ret = exp_metadata_write_attributes(p,&fake_inode,global_attr_p,sizeof(rmfentry_disk_t),1 /* sync */);
  if (ret < 0)
  {
    return -1;
  }
  return 0;
}
/*
**__________________________________________________________________
*/
/**
*  Create an entry in the fid recycle tracking file 

 
  
  @param trk_tb_p: export attributes tracking table
  @param slice: slice of the parent directory
  @param global_attr_p : pointer to the attributes relative to the object to delete
  @param link: pointer to the symbolic link (significant for ROZOFS_SLNK only)
  
  @retval 0 on success: (the attributes contains the lower part of the fid that is allocated by the service)
  @retval -1 on error (see errno for details)
*/
int exp_recycle_entry_create(export_tracking_table_t *trk_tb_p,uint32_t slice,void *ptr)
{
   rozofs_inode_t fake_inode;
   int ret;
   exp_trck_top_header_t *p = NULL;
   recycle_disk_t *global_attr_p = (recycle_disk_t *)ptr;
   
   fake_inode.s.key = ROZOFS_RECYCLE;
   fake_inode.s.usr_id = slice; 
   fake_inode.s.eid = trk_tb_p->eid;   
      
   p = trk_tb_p->tracking_table[fake_inode.s.key];
   if (p == NULL)
   {
     errno = ENOTSUP;
     return -1;    
   }   
   /*
   ** allocate the inode
   */
   ret = exp_metadata_allocate_inode(p,&fake_inode,ROZOFS_RECYCLE,slice);
   if (ret < 0)
   { 
      return -1;
   }

   /*
   ** copy the reference of the trash inode
   */
   memcpy(&global_attr_p->recycle_inode,&fake_inode,sizeof(rozofs_inode_t));
   /*
   ** write the metadata on disk
   */
   ret = exp_metadata_write_attributes(p,&fake_inode,global_attr_p,sizeof(recycle_disk_t), 1 /* sync */);
  if (ret < 0)
  {
    return -1;
  }
  return 0;
}

/*
**__________________________________________________________________
*/
/**
*  Creation of one metadata tracking context (private API)

 
   @param tab_p: pointer to the global tracking context of an export
   @param index: index of the metadata tracking context  
   @param name: name of the metadata tracking context
   @param size: size of the attribute context for the metadata tracking type
   @param root_path : root path of the export
   @param create_flag : assert to 1 if tracking files MUST be created
   
   @retval 0 on success
   @retval <0 on error(see errno for details)
*/

int exp_create_one_attributes_tracking_context(export_tracking_table_t *tab_p,int index,char *name,int size,char *root_path,int create)
{
   char pathname[1024];

   sprintf(pathname,"%s/%s",root_path,name);
   if (access(pathname, F_OK) == -1) 
   {
    if ((errno == ENOENT) && (create)) 
    {
      /*
      ** create the directory
      */
      if (mkdir(pathname, S_IRUSR | S_IWUSR | S_IXUSR) != 0)
      goto error; 
     }
     else
     {
       severe("cannot access to %s: %s\n",pathname,strerror(errno));
       goto error;
     }
   }
   tab_p->tracking_table[index] = exp_trck_top_allocate(name,pathname,size,create);
  if (tab_p->tracking_table[index] == NULL)
  {
    goto error;
  }
  return 0;  
error:
  return -1;
}
/*
**__________________________________________________________________
*/
/**
*  Export tracking table create

   That service is called at export creation time. Its purpose is to allocate
   data structure for export attributes management.
   
   @param eid : export identifier
   @param root_path : root path of the export
   @param create_flag : assert to 1 if tracking files MUST be created
   
   @retval <> NULL: pointer to the attributes tracking table
   @retval == NULL : error (see errno for details)
*/
export_tracking_table_t *exp_create_attributes_tracking_context(uint16_t eid, char *root_path, int create)
{
   export_tracking_table_t *tab_p = NULL; 
   int ret; 
   int loop;
   
   /*
   ** check if the entry already exists: this is done to address the case of the exportd reload
   */
   if (eid > EXPGW_EID_MAX_IDX) 
   {
      /*
      ** eid value is out of range
      */
      severe("failed to create ressource: eid %d is out of range max is %d",eid,EXPGW_EID_MAX_IDX);
      return NULL;
   }
   if (export_tracking_table[eid]!= NULL)
   {
      /*
      ** check if there is a change in the root path--> in that case the memory should be released
      ** the test is done by read the root path found in the reg_attr
      */      
       export_tracking_table_t *p = export_tracking_table[eid];
       exp_trck_top_header_t *top_hdr_p = p->tracking_table[ROZOFS_REG];

       if (top_hdr_p == NULL)
       {
	  fatal("no memory structure for reg_attr of eid %d path %s",eid,root_path);
       }
       if (strcmp(root_path,top_hdr_p->root_path) == 0)
       {
	  /*
	  ** the context is already allocated: nothing more to done:
	  **  note: it is not foreseen the change the root path of an exportd !!
	  */
	  return export_tracking_table[eid];	 
       }
       else
       {
	  /*
	  ** release the context
	  */
	  exp_release_attributes_tracking_context(export_tracking_table[eid]);
	  export_tracking_table[eid] = NULL;	 
       }
   }
   
   tab_p = malloc(sizeof(export_tracking_table_t));
   if (tab_p == NULL)
   {
     /*
     ** out of memory
     */
     return NULL;
   }
   memset(tab_p,0,sizeof(export_tracking_table_t));
   tab_p->eid = eid;
   /*
   ** regular file
   */
   ret = exp_create_one_attributes_tracking_context(tab_p,ROZOFS_REG,"reg_attr",ROZOFS_TRACKING_ATTR_SIZE,root_path,create);
   if (ret < 0) goto error;
   /*
   ** create all entries
   */
   for(loop= 0; loop < EXP_TRCK_MAX_USER_ID; loop++)
   {
     ret = exp_trck_top_add_user_id(tab_p->tracking_table[ROZOFS_REG],loop);
     if (ret < 0) 
     {
       severe("reg_attr:creation failure for user_id %d \n",loop);
       goto error;   
     }  
   }
   /*
   ** directory
   */
   ret = exp_create_one_attributes_tracking_context(tab_p,ROZOFS_DIR,"dir_attr",ROZOFS_TRACKING_ATTR_SIZE,root_path,create);
   if (ret < 0) goto error;
   /*
   ** create all entries
   */
   for(loop= 0; loop < EXP_TRCK_MAX_USER_ID; loop++)
   {
     ret = exp_trck_top_add_user_id(tab_p->tracking_table[ROZOFS_DIR],loop);
     if (ret < 0) 
     {
       severe("dir_attr:creation failure for user_id %d \n",loop);
       goto error;   
     }  
   }
   /*
   ** symlink
   */
   ret = exp_create_one_attributes_tracking_context(tab_p,ROZOFS_SLNK,"dir_slnk",ROZOFS_PATH_MAX,root_path,create);
   if (ret < 0) goto error;
   for (loop= 0; loop < EXP_TRCK_MAX_USER_ID; loop++)
   {
     ret = exp_trck_top_add_user_id(tab_p->tracking_table[ROZOFS_SLNK],loop);
     if (ret < 0) 
     {
       severe("dir_slnk:creation failure for user_id %d \n",loop);
       goto error;   
     }  
   }   
   /*
   ** extended attributes
   */
   ret = exp_create_one_attributes_tracking_context(tab_p,ROZOFS_EXTATTR,"dir_xattr",ROZOFS_XATTR_BLOCK_SZ,root_path,create);
   if (ret < 0) goto error;
   for (loop= 0; loop < EXP_TRCK_MAX_USER_ID; loop++)
   {
     ret = exp_trck_top_add_user_id(tab_p->tracking_table[ROZOFS_EXTATTR],loop);
     if (ret < 0) 
     {
       severe("dir_xattr:creation failure for user_id %d \n",loop);
       goto error;   
     }  
   }   

   /*
   ** trash directory
   */
   ret = exp_create_one_attributes_tracking_context(tab_p,ROZOFS_TRASH,"dir_trash",sizeof(rmfentry_disk_t),root_path,create);
   if (ret < 0) goto error;
   for (loop= 0; loop < EXP_TRCK_MAX_USER_ID; loop++)
   {
     ret = exp_trck_top_add_user_id(tab_p->tracking_table[ROZOFS_TRASH],loop);
     if (ret < 0) 
     {
       severe("dir_xattr:creation failure for user_id %d \n",loop);
       goto error;   
     }  
   }   

   if (common_config.fid_recycle)
   {
     /*
     ** recycle directory
     */
     ret = exp_create_one_attributes_tracking_context(tab_p,ROZOFS_RECYCLE,"dir_recycle",sizeof(recycle_disk_t),root_path,create);
     if (ret < 0) goto error;
     for (loop= 0; loop < EXP_TRCK_MAX_USER_ID; loop++)
     {
       ret = exp_trck_top_add_user_id(tab_p->tracking_table[ROZOFS_RECYCLE],loop);
       if (ret < 0) 
       {
	 severe("dir_recycle:creation failure for user_id %d \n",loop);
	 goto error;   
       }  
     }
   }   
   /*
   ** everything is fine, so store the reference of the context in the table at the index of the eid
   */
   export_tracking_table[eid]= tab_p;
   
   return tab_p;

error:
   exp_release_attributes_tracking_context(tab_p);
   return NULL;


}

/*
**__________________________________________________________________
*/
/**
*  Export tracking table deletion

   That service is called at export creation time. Its purpose is to allocate
   data structure for export attributes management.
   
   @param tab_p  : pointer to the attributes tracking table 
   
   @retval none
*/
void exp_release_attributes_tracking_context(export_tracking_table_t *tab_p)
{
   int i;
   
   if (tab_p == NULL) return;
   for (i = 0 ; i < ROZOFS_MAXATTR; i++)
   {
      if (tab_p->tracking_table[i] == NULL) continue;
      exp_trck_top_release(tab_p->tracking_table[i]);
   }
   free(tab_p);
}


#define EXPORT_MULTI 1
#ifdef EXPORT_MULTI
/*
**________________________________________________________________________
     M U L T I   T H R E A D   C A C H I N G  
**________________________________________________________________________
*/     
/*
 *___________________________________________________________________
 * Put the entry in front of the lru list when no lock is set
 *
 * @param cache: the cache context
 * @param entry: the cache entry
 *___________________________________________________________________
 */
static inline void lv2_cache_update_lru_th(lv2_cache_t *cache, lv2_entry_t *entry,uint32_t hash) {
    list_remove(&entry->list);
    if (entry->nb_locks == 0) {
        pthread_rwlock_wrlock(&cache->htable.lock[hash%cache->htable.lock_size]);
        list_push_front(&cache->lru_th[hash%cache->htable.lock_size], &entry->list);
        pthread_rwlock_unlock(&cache->htable.lock[hash%cache->htable.lock_size]);
    }
    else {
        pthread_rwlock_wrlock(&cache->htable.lock[hash%cache->htable.lock_size]);
        list_push_front(&cache->flock_list_th[hash%cache->htable.lock_size], &entry->list);    
        pthread_rwlock_unlock(&cache->htable.lock[hash%cache->htable.lock_size]);
    }
}
/*
**__________________________________________________________________
*/
/**
*   Remove an entry from the attribute cache

    @param cache: pointer to the cache context
    @param entry: pointer to entry to remove
    @param hash: hash value
    
    @retval none
*/
static inline void lv2_cache_unlink_th(lv2_cache_t *cache,lv2_entry_t *entry,uint32_t hash) {

  file_lock_remove_fid_locks(&entry->file_lock);
  mattr_release(&entry->attributes.s.attrs);
  /*
  ** check the presence of the extended attribute block and free it
  */
  if (entry->extended_attr_p != NULL) free(entry->extended_attr_p);
  /*
  ** check the presence of the root_idx bitmap : for directory only
  */
  if (entry->dirent_root_idx_p != NULL) free(entry->dirent_root_idx_p); 
  /*
  ** Remove symbolic link name if any 
  */
  if (entry->symlink_target != NULL) free(entry->symlink_target);
  
  pthread_rwlock_wrlock(&cache->htable.lock[hash%cache->htable.lock_size]);  
  list_remove(&entry->list);
  pthread_rwlock_unlock(&cache->htable.lock[hash%cache->htable.lock_size]);  

  free(entry);
  cache->size--;  
}
/*
**__________________________________________________________________
*/
/**
*   Get an enry from the attributes cache

    @param: pointer to the cache context
    @param: fid : key of the element to find
    @param: hash : hash value of the fid
    
    @retval <>NULL : pointer to the cache entry that contains the attributes
    @retval NULL: not found
*/
lv2_entry_t *lv2_cache_get_th(lv2_cache_t *cache, fid_t fid,uint32_t hash) 
{
    lv2_entry_t *entry = 0;

    if ((entry = htable_get_th(&cache->htable, fid,hash)) != 0) {
        // Update the lru
        lv2_cache_update_lru_th(cache,entry,hash); 
	cache->hit++;
    }
    else {
      cache->miss++;
    }
    return entry;
}

/*
**__________________________________________________________________
*/
/**
*   The purpose of that service is to read object attributes and store them in the attributes cache

  @param trk_tb_p: export attributes tracking table
  @param cache : pointer to the export attributes cache
  @param fid : unique identifier of the object
  @param hash : hash value of the fid
  
  @retval <> NULL: attributes of the object
  @retval == NULL : no attribute returned for the object (see errno for details)
*/

lv2_entry_t *lv2_cache_put_th(export_tracking_table_t *trk_tb_p,lv2_cache_t *cache, fid_t fid,uint32_t hash) 
{
    lv2_entry_t *entry;
    int count=0;
    rozofs_inode_t *fake_inode,*fake_inode_attr;
   
    fake_inode = (rozofs_inode_t*)fid;

    // maybe already cached.
    if ((entry = htable_get_th(&cache->htable, fid,hash)) != 0) {
        goto out;
    }
    entry = malloc(sizeof(lv2_entry_t));
    if (entry == NULL)
    {
       severe("lv2_cache_put: %s\b",strerror(errno));
       return NULL;
    }
    memset(entry,0,sizeof(lv2_entry_t));

    /*
    ** get the attributes of the object
    */
    if (exp_meta_get_object_attributes(trk_tb_p,fid,entry) < 0)
    {
      /*
      ** cannot get the attributes: need to log the returned errno
      */
      goto error;
    }
    /*
    ** case of the fid recycle
    */

    if (fake_inode->s.key == ROZOFS_REG)
    {
      fake_inode_attr = (rozofs_inode_t*)entry->attributes.s.attrs.fid;
      if( fake_inode->s.recycle_cpt !=  fake_inode_attr->s.recycle_cpt)
      {
         /*
	 ** it correspond to the case where the fid has been recycled
	 */
         goto error;
      }
    }    
    /*
    ** Initialize file locking 
    */
    list_init(&entry->file_lock);
    entry->nb_locks = 0;
    list_init(&entry->list);

    /*
    ** Try to remove older entries
    */
    count = 0;
    if (cache->size >= cache->max)
    {
        pthread_rwlock_wrlock(&cache->htable.lock[hash%cache->htable.lock_size]);
	while(!list_empty(&cache->lru_th[hash%cache->htable.lock_size]))
	{
          lv2_entry_t *lru;

	  lru = list_entry(cache->lru_th[hash%cache->htable.lock_size].prev, lv2_entry_t, list);  
 	  if (lru->nb_locks != 0) {
	    severe("lv2 with %d locks in lru",lru->nb_locks);
 	  }	
	  htable_del(&cache->htable, lru->attributes.s.attrs.fid);
	  lv2_cache_unlink(cache,lru);
	  cache->lru_del++;
	  count++;
	  if (count >= 3) break;	
	
	}
        pthread_rwlock_unlock(&cache->htable.lock[hash%cache->htable.lock_size]);
    }
    /*
    ** Insert the new entry
    */
    lv2_cache_update_lru_th(cache,entry,hash);
    htable_put_th(&cache->htable, entry->attributes.s.attrs.fid, entry,hash);
    cache->size++;    

    goto out;
error:
    if (entry) {
        free(entry);
        entry = 0;
    }
out:
    return entry;
}


/*
**__________________________________________________________________
*/
/**
*   The purpose of that service is to store object attributes in the attributes cache

  @param attr_p: pointer to the attributes of the object
  @param cache : pointer to the export attributes cache
  @param fid : unique identifier of the object
  
  @retval <> NULL: attributes of the object
  @retval == NULL : no attribute returned for the object (see errno for details)
*/

lv2_entry_t *lv2_cache_put_forced_th(lv2_cache_t *cache, fid_t fid,ext_mattr_t *attr_p) {
    lv2_entry_t *entry;
    int count=0;
    uint32_t hash = lv2_hash(fid);

    // maybe already cached.
    if ((entry = htable_get_th(&cache->htable, fid,hash)) != 0) {
        goto out;
    }
    entry = malloc(sizeof(lv2_entry_t));
    if (entry == NULL)
    {
       severe("lv2_cache_put: %s\b",strerror(errno));
       return NULL;
    }
    memset(entry,0,sizeof(lv2_entry_t));

    /*
    ** copy the attributes
    */
    memcpy(entry,attr_p,sizeof( ext_mattr_t));
    /*
    ** Initialize file locking 
    */
    list_init(&entry->file_lock);
    entry->nb_locks = 0;
    list_init(&entry->list);

    /*
    ** Try to remove older entries
    */
    count = 0;
    if (cache->size >= cache->max)
    {
        pthread_rwlock_wrlock(&cache->htable.lock[hash%cache->htable.lock_size]);
	while(!list_empty(&cache->lru_th[hash%cache->htable.lock_size]))
	{
          lv2_entry_t *lru;

	  lru = list_entry(cache->lru_th[hash%cache->htable.lock_size].prev, lv2_entry_t, list);  
 	  if (lru->nb_locks != 0) {
	    severe("lv2 with %d locks in lru",lru->nb_locks);
 	  }	
	  htable_del(&cache->htable, lru->attributes.s.attrs.fid);
	  lv2_cache_unlink(cache,lru);
	  cache->lru_del++;
	  count++;
	  if (count >= 3) break;	
	
	}
        pthread_rwlock_unlock(&cache->htable.lock[hash%cache->htable.lock_size]);
    }
    /*
    ** Insert the new entry
    */
    lv2_cache_update_lru_th(cache,entry,hash);
    htable_put_th(&cache->htable, entry->attributes.s.attrs.fid, entry,hash);
    cache->size++;    
out:
    return entry;
}

/*
**__________________________________________________________________
*/
/** search a fid in the attribute cache
 
 if fid is not cached, try to find it on the underlying file system
 and cache it.
 
  @param trk_tb_p: export attributes tracking table
  @param cache: pointer to the cache associated with the export
  @param fid: the searched fid
 
  @return a pointer to lv2 entry or null on error (errno is set)
*/
lv2_entry_t *export_lookup_fid_th(export_tracking_table_t *trk_tb_p,lv2_cache_t *cache, fid_t fid) {
    lv2_entry_t *lv2 = 0;
    uint32_t slice;
    uint32_t hash = lv2_hash(fid);
    
    cache->hash_stats[hash%EXPORT_LV2_MAX_LOCK]++;

    /*
    ** get the slice of the fid :extracted from the upper part 
    */
    exp_trck_get_slice(fid,&slice);
    /*
    ** check if the slice is local
    */
    if (exp_trck_is_local_slice(slice))
    {
      if (!(lv2 = lv2_cache_get_th(cache, fid,hash))) {
          // not cached, find it an cache it
          if (!(lv2 = lv2_cache_put_th(trk_tb_p,cache, fid,hash))) {
              goto out;
          }
      }
    }
    else
    {
      int idx  = (++exp_fake_lv2_entry_idx)%(EXP_MAX_FAKE_LVL2_ENTRIES);
      lv2 = &exp_fake_lv2_entry[idx];
      if (exp_meta_get_object_attributes(trk_tb_p,fid,lv2) < 0)
      {
	/*
	** cannot get the attributes: need to log the returned errno
	*/
	lv2 = NULL;
      } 
      else
      {
         memset(lv2,0,sizeof(lv2_entry_t));
      }         
    }
out:
//    STOP_PROFILING(export_lookup_fid);
    return lv2;
}

/*
**__________________________________________________________________
*/
/**
*   delete an entry from the attribute cache

    @param cache: pointer to the level 2 cache
    @param fid : key of the entry to remove
*/
void lv2_cache_del_th(lv2_cache_t *cache, fid_t fid) 
{
    lv2_entry_t *entry = 0;
    uint32_t hash = lv2_hash(fid);
    if ((entry = htable_del_th(&cache->htable, fid,hash)) != 0) {
	lv2_cache_unlink_th(cache,entry,hash);
    }
}
/*
**________________________________________________________________________
     M U L T I   T H R E A D   C A C H I N G 
**________________________________________________________________________
*/     


#endif
