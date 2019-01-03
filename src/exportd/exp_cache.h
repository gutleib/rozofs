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

#ifndef _EXP_CACHE_H
#define _EXP_CACHE_H


#include <rozofs/rozofs.h>
#include <rozofs/common/list.h>
#include <rozofs/common/htable.h>
#include <rozofs/common/mattr.h>
#include <rozofs/common/export_track.h>
#include <rozofs/rpc/eproto.h>
#include "mreg.h"
#include "mdir.h"
#include "mslnk.h"



/** API lv2 cache management functions.
 *
 * lv2 cache is common to several exports to take care of max fd opened.
 */
/**
*  mover state:
*/
typedef enum _rozofs_mover_state_e
{
   ROZOFS_MOVER_IDLE = 0,
   ROZOFS_MOVER_IN_PRG,    /**< file move is in progress   */
   ROZOFS_MOVER_DONE       /**< file move is done, to validate the new distribution just for for the guard timer expiration  */
} rozofs_mover_state_e;

/** lv2 entry cached */
typedef struct lv2_entry {
    ext_mattr_t attributes; ///< attributes of this entry
    void        *extended_attr_p; /**< pointer to xattr array */
    void        *dirent_root_idx_p; /**< pointer to bitmap of the dirent root file presence : directory only */
    char        *symlink_target; ///< symbolic link target name (only for symlink) */

    list_t list;        ///< list used by cache    
    int          locked_in_cache:1;  /**< assert to 1 to lock the entry in the lv2 cache                    */
    int          dirty_bit:1;          /**< that bit is intended to be used by attributes related to directory to deal with per directory byte count   */
    int          filler:30;          /**< for future usage                                                  */
    /*
    ** File mover
    */
    int           access_cpt:1;         /**< assert to 1 each time the attributes are read when time_move is not 0. */
    int           mover_state:3;        /**< mover state.                                                           */
    int           mover_guard_timer:28; /**< mover guard timer in seconds.                                          */
    time_t        time_move;            /**< date of the file move start                                            */       
    /* 
    ** File locking
    */
    int            nb_locks;    ///< Number of locks on the FID
    list_t         file_lock;   ///< List of the lock on the FID
    list_t         move_list;   ///< pending fist of the file waiting for move validation
    void           *thin_provisioning_ctx_p;   /**< pointer to the thin provisioning context (for exportd with thin provisioning option) */
    ext_mattr_t    *slave_inode_p;   /**< pointer to the contexts of the slave inode associated with a master inode */
} lv2_entry_t;

/** lv2 cache
 *
 * used to keep track of open file descriptors and corresponding attributes
 */
 #define EXPORT_LV2_MAX_LOCK ROZOFS_HTABLE_MAX_LOCK
typedef struct lv2_cache {
    int max;            ///< max entries in the cache
    int size;           ///< current number of entries
    uint64_t   hit;
    uint64_t   miss;
    uint64_t   lru_del;
    list_t     lru;     ///< LRU 
    list_t     flock_list;
    /*
    ** case of multi-threads
    */
    list_t     lru_th[EXPORT_LV2_MAX_LOCK];     ///< LRU 
    list_t     flock_list_th[EXPORT_LV2_MAX_LOCK];
    uint64_t   hash_stats[EXPORT_LV2_MAX_LOCK];
    htable_t htable;    ///< entries hashing
} lv2_cache_t;


/**
*  Per export tracking table 
*/
typedef struct _export_tracking_table_t
{
  uint16_t               eid; ///< Export identifier
  exp_trck_top_header_t *tracking_table[ROZOFS_MAXATTR];
} export_tracking_table_t;

#define ROZOFS_TRACKING_ATTR_SIZE 512

extern lv2_cache_t cache;

/*
**__________________________________________________________________
*/
/**
  lock the entry in the level2 cache of the exportd. By doing so
  the entry is not eligible to LRU removal
  
  @param lv2_p: entry to lock
  
  @retval none
*/
static inline void lv2_cache_lock_entry_in_cache(lv2_entry_t *lv2_p)
{
  lv2_p->locked_in_cache = 1;

}
/*
**__________________________________________________________________
*/
/**
  unlock the entry in the level2 cache of the exportd. By doing so
  the entry is  eligible to LRU removal
  
  @param lv2_p: entry to lock
  
  @retval none
*/
static inline void lv2_cache_unlock_entry_in_cache(lv2_entry_t *lv2_p)
{
  lv2_p->locked_in_cache = 0;

}
/*
**__________________________________________________________________
*/
/**
  Check if the entry has been locked in the level 2 cache
  
  @param lv2_p: entry 
  
  @retval 0: not locked
  @retval 1: locked
*/
static inline int lv2_cache_is_locked_entry_in_cache(lv2_entry_t *lv2_p)
{
  return (lv2_p->locked_in_cache == 0?0:1);

}
/*
**__________________________________________________________________
*/
/**
*   init of an exportd attribute cache

    @param: pointer to the cache context
    
    @retval none
*/
void lv2_cache_initialize(lv2_cache_t *cache);
/*
**__________________________________________________________________
*/
/**
*   delete of an exportd attribute cache

    @param: pointer to the cache context
    
    @retval none
*/
void lv2_cache_release(lv2_cache_t *cache);

/*
**__________________________________________________________________
*/
/**
*   delete an entry from the attribute cache

    @param cache: pointer to the level 2 cache
    @param fid : key of the entry to remove
*/
void lv2_cache_del(lv2_cache_t *cache, fid_t fid) ;
/*
**__________________________________________________________________
*/
/**
*   Remove an entry from the attribute cache without deleting it

    @param cache: pointer to the level 2 cache
    @param fid : key of the entry to remove
*/
void lv2_cache_remove_hash(lv2_cache_t *cache, fid_t fid);
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

lv2_entry_t *lv2_cache_put(export_tracking_table_t *trk_tb_p,lv2_cache_t *cache, fid_t fid);
lv2_entry_t *lv2_cache_put_th(export_tracking_table_t *trk_tb_p,lv2_cache_t *cache, fid_t fid,uint32_t hash);

/*
**__________________________________________________________________
*/
/**
*   The purpose of that service is to store object attributes in the attributes cache

  @param attr_p: pointer to the attribut of the object
  @param cache : pointer to the export attributes cache
  @param fid : unique identifier of the object
  
  @retval <> NULL: attributes of the object
  @retval == NULL : no attribute returned for the object (see errno for details)
*/

lv2_entry_t *lv2_cache_put_forced(lv2_cache_t *cache, fid_t fid,ext_mattr_t *attr_p);
lv2_entry_t *lv2_cache_put_forced_th(lv2_cache_t *cache, fid_t fid,ext_mattr_t *attr_p);

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

lv2_entry_t *lv2_cache_put_forced_multiple(lv2_cache_t *cache, fid_t fid,ext_mattr_t *attr_p,ext_mattr_t *slave_inode_p);

/** Format statistics information about the lv2 cache
 *
 *
 * @param cache: the cache context
 * @param pChar: where to format the output
 *
 * @retval the end of the output string
 */
char * lv2_cache_display(lv2_cache_t *cache, char * pChar) ;
/*
 *___________________________________________________________________
 * Put the entry in front of the lru list when no lock is set
 *
 * @param cache: the cache context
 * @param entry: the cache entry
 *___________________________________________________________________
 */
static inline void lv2_cache_update_lru(lv2_cache_t *cache, lv2_entry_t *entry) {
    list_remove(&entry->list);
    if (entry->nb_locks == 0) {
        list_push_front(&cache->lru, &entry->list);
    }
    else {
        list_push_front(&cache->flock_list, &entry->list);    
    }
}
/*
**__________________________________________________________________
*/
/** search a fid in the attribute cache
 
 if fid is not cached, try to find it on the underlying file system
 and cache it.
 
  @param trk_tb_p: export tracking table
  @param cache: pointer to the cache associated with the export
  @param fid: the searched fid
 
  @return a pointer to lv2 entry or null on error (errno is set)
*/
#define EXPORT_LOOKUP_FID export_lookup_fid

lv2_entry_t *export_lookup_fid(export_tracking_table_t *trk_tb_p,lv2_cache_t *cache, fid_t fid);
lv2_entry_t *export_lookup_fid_th(export_tracking_table_t *trk_tb_p,lv2_cache_t *cache, fid_t fid);
lv2_entry_t *export_lookup_fid_no_invalidate_mover(export_tracking_table_t *trk_tb_p,lv2_cache_t *cache, fid_t fid);
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
int export_lv2_write_attributes(export_tracking_table_t *trk_tb_p,lv2_entry_t *entry,int sync);
/*
**__________________________________________________________________
*/
/**
*  Create the attributes of a directory/regular file or symbolic link

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
int exp_attr_create(export_tracking_table_t *trk_tb_p,uint32_t slice,ext_mattr_t *global_attr_p,int type,char *link);


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
  @param write: assert to 1 if write is requested/ 0: no write
  
  @retval 0 on success: (the attributes contains the lower part of the fid that is allocated by the service)
  @retval -1 on error (see errno for details)
*/
int exp_attr_create_write_cond(export_tracking_table_t *trk_tb_p,uint32_t slice,ext_mattr_t *global_attr_p,int type,char *link,uint8_t write);
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
int exp_xattr_block_create(export_tracking_table_t *trk_tb_p,lv2_entry_t *entry,uint64_t *block_ref_p);

/**
*  Create an entry in the trash for a file to delete

 
  
  @param trk_tb_p: export attributes tracking table
  @param slice: slice of the parent directory
  @param global_attr_p : pointer to the attributes relative to the object to delete
  @param link: pointer to the symbolic link (significant for ROZOFS_SLNK only)
  
  @retval 0 on success: (the attributes contains the lower part of the fid that is allocated by the service)
  @retval -1 on error (see errno for details)
*/
int exp_trash_entry_create(export_tracking_table_t *trk_tb_p,uint32_t slice,void *global_attr_p);

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
int exp_recycle_entry_create(export_tracking_table_t *trk_tb_p,uint32_t slice,void *ptr);
/*
**__________________________________________________________________
*/
/** store the extended attributes part of an attribute cache entry to the export's file system
 *
   @param trk_tb_p: export attributes tracking table
   @param entry: the entry used
 
   @return: 0 on success otherwise -1
 */
int export_lv2_write_xattr(export_tracking_table_t *trk_tb_p,lv2_entry_t *entry);

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
int exp_meta_get_xattr_block(export_tracking_table_t *trk_tb_p,lv2_entry_t *entry_p);
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
int exp_attr_delete(export_tracking_table_t *trk_tb_p,fid_t fid);
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
export_tracking_table_t *exp_create_attributes_tracking_context(uint16_t eid, char *root_path, int create);
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
void exp_release_attributes_tracking_context(export_tracking_table_t *tab_p);

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
int exp_meta_get_object_attributes(export_tracking_table_t *trk_tb_p,fid_t fid,lv2_entry_t *entry_p);

/*
**__________________________________________________________________
*/
/*
**
   Get the pointer to the slave inodes (multi-file)
   
   @param lv2_entry_t *entry_p
   
   @retval pointer to the slave inodes
*/
static inline ext_mattr_t *export_lv2_get_slave_inode_ptr (lv2_entry_t *entry_p)
{
  return entry_p->slave_inode_p;
}

/*
**__________________________________________________________________
*/
/*
**
  Set the pointer to the slave inodes (multi-file)
   
   @param lv2_entry_t *entry_p
   @param slave_inode_p: pointer to the slave inodes 
   
   @retval none
*/
static inline void export_lv2_set_slave_inode_ptr (lv2_entry_t *entry_p,ext_mattr_t *slave_inode_p)
{
  entry_p->slave_inode_p = slave_inode_p;
}

/*
**__________________________________________________________________
*/
/**
*  Create the attributes of a regular file in burst mode without write attributes on disk

  create an oject according to its type. The service performs the allocation of the fid. 
  It is assumed that all the other fields of the object attributes are already been filled in.
  
  @param trk_tb_p: export attributes tracking table
  @param slice: slice of the parent directory
  @param global_attr_p : pointer to the attributes of the object (master inode attributes)
  @param inode_count: number of consecutive inode to allocated

  
  @retval 0 on success: (the attributes contains the lower part of the fid that is allocated by the service)
  @retval -1 on error (see errno for details)
*/
int exp_attr_create_write_cond_burst(export_tracking_table_t *trk_tb_p,uint32_t slice,ext_mattr_t *global_attr_p,int inode_count);

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
int exp_attr_delete_multiple(export_tracking_table_t *trk_tb_p,fid_t fid,int nb_slaves);

/*
**__________________________________________________________________________________
**
** Set write error on lv2 entry
**
** @param lv2   entry to raise error on
**__________________________________________________________________________________
*/
static inline void rozofs_set_werror(lv2_entry_t *lv2) {    
  lv2->attributes.s.bitfield1 |=  ROZOFS_BITFIELD1_WRITE_ERROR;
}
/*
**__________________________________________________________________________________
**
** Clear write error on lv2 entry
**
** @param lv2   entry to clear error from
**__________________________________________________________________________________
*/
static inline void rozofs_clear_werror(lv2_entry_t *lv2) {    
  lv2->attributes.s.bitfield1 &= ~ROZOFS_BITFIELD1_WRITE_ERROR;
}
/*
**__________________________________________________________________________________
**
** Get write error of lv2 entry
**
** @param lv2   entry to get write error status from
**
** @retval    1 when an error is set. 0 else.
**__________________________________________________________________________________
*/
static inline int rozofs_is_wrerror(lv2_entry_t *lv2) {    
  if (lv2->attributes.s.bitfield1 & ROZOFS_BITFIELD1_WRITE_ERROR) return 1;
  return 0;
}
#endif
