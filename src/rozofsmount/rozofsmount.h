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

#ifndef ROZOFSMOUNT_H
#define ROZOFSMOUNT_H

#define FUSE_USE_VERSION 26
#include <fuse/fuse_lowlevel.h>
#include <sys/ioctl.h>
#include <rozofs/common/htable.h>
#include <rozofs/rpc/rozofs_rpc_util.h>
#include <rozofs/core/rozofs_tx_common.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/rozofs_throughput.h>

#include "file.h"
#include "rozofs_ext4.h"


#define ROZOFSMOUNT_MAX_EXPORT_TX 128
#define ROZOFSMOUNT_MAX_DEFAULT_STORCLI_TX_STANDALONE  128
#define ROZOFSMOUNT_MAX_DEFAULT_STORCLI_TX_PER_PROCESS  48
#define ROZOFSMOUNT_MAX_4_STORCLI_TX_PER_PROCESS  48

#define hash_xor8(n)    (((n) ^ ((n)>>8) ^ ((n)>>16) ^ ((n)>>24)) & 0xff)
#define ROOT_INODE 1

typedef struct _rozofs_fuse_profile_t
{
   unsigned long ra_pages; 
   unsigned max_background;  
   unsigned congestion_threshold; 
} rozofs_fuse_profile_t;

extern rozofs_fuse_profile_t fuse_kern_profile[];

extern exportclt_t exportclt;

extern list_t inode_entries;
extern int list_wr_block_count;   /**< statistics counter of wr_block that has been queued because of exportd loss of connectivity */
extern list_t list_wr_block_head;

extern htable_t htable_inode;
extern htable_t htable_fid;
extern uint64_t rozofs_ientries_count;

extern double direntry_cache_timeo ;
extern double entry_cache_timeo ;
extern double attr_cache_timeo ;
extern int rozofs_cache_mode;
extern int rozofs_mode;
extern int rozofs_rotation_read_modulo;
extern int rozofs_bugwatch;
extern uint16_t rozofsmount_diag_port;
extern int rozofs_max_storcli_tx ;  /**< depends on the number of storcli processes */
extern char *rozofs_mountpoint;
extern struct fuse_lowlevel_ops rozofs_ll_operations;
extern int ROZOFS_MAX_WRITE_THREADS;  /**< number of active write threads           */
extern int rozofsmount_main_ready;
typedef struct rozofsmnt_conf {
    char *host;
    char *export;
    char *passwd;
    unsigned buf_size;
    unsigned min_read_size;
    unsigned nbstorcli;    
    unsigned max_retry;
    unsigned dbg_port;  /**< lnkdebug base port: rozofsmount=dbg_port, storcli(1)=dbg_port+1, ....  */
    unsigned instance;  /**< rozofsmount instance: needed when more than 1 rozofsmount run the same server and exports the same filesystem */
    unsigned export_timeout;
    unsigned storcli_timeout;
    unsigned storage_timeout;
    unsigned spare_timeout;
    unsigned fs_mode; /**< rozofs mode: 0-> file system/ 1-> block mode */
    unsigned cache_mode;  /**< 0: no option, 1: direct_read, 2: keep_cache */
    unsigned attr_timeout;
    unsigned attr_timeout_ms;
    unsigned entry_timeout;
    unsigned entry_timeout_ms;
    unsigned entry_dir_timeout_ms;
    unsigned attr_dir_timeout_ms;

    unsigned enoent_timeout_ms;  
    
    unsigned symlink_timeout;
    unsigned shaper;
    unsigned rotate;
    unsigned posix_file_lock;    
    unsigned bsd_file_lock;  
    unsigned no_file_lock;  
    unsigned max_write_pending ; /**< Maximum number pending write */
    unsigned quota; /* ignored */    
    unsigned noXattr;
    int site;
    int conf_site_file;
    unsigned running_site;
    unsigned mojThreadWrite;
    unsigned mojThreadRead;    
    unsigned mojThreadThreshold;  
    unsigned no0trunc;     
    /*
    ** when set this options tells that this client is the only writter of
    ** the file it writes, and so can avoid some internal re-read when writes
    ** are not aligned on block bondary
    */
    unsigned onlyWriter;  
    /*
    ** Whether to favor local storage on read to save network bandwith
    ** in case of poor network connection
    */
    unsigned localPreference;    
    unsigned noReadFaultTolerant;   
    unsigned xattrcache;   /**< assert to 1 for extended attributes caching                        */      
    unsigned asyncsetattr; /**< assert to 1 to operate in asynchronous mode for setattr operations */
    unsigned numanode;  
    unsigned wbcache;  /**< writeback cache fuse */    
    unsigned nb_writeThreads;  /**< number of write threads (default:0                             */ 
    unsigned long rozofs_bypass_size;     
    unsigned idx_fuse_profile;
    unsigned pagecache; /**< when assert storcli performs a direct write in Linux page cache for I/O greater than 256 KB */
    unsigned kernel_max_read; /**< image of the block size at kernel level */
    unsigned kernel_max_write; /**< image of the block size at kernel level */
    unsigned rozo_module;  /**< assert to one if rozo fuse module is in use */
} rozofsmnt_conf_t;
rozofsmnt_conf_t conf;


/** entry kept locally to map fuse_inode_t with rozofs fid_t */
typedef struct ientry {
    hash_entry_t he;
    fuse_ino_t inode; ///< value of the inode allocated by rozofs
    fid_t fid; ///< unique file identifier associated with the file or directory
    fid_t pfid; ///< unique file identifier associated with the parent
     /**< assert to 1 when there is a setattr on mtime  , clear on write */
    int64_t file_extend_size; /**< The pending size extenstion */
    /* some bit fields... */
    uint64_t  mtime_locked:1;  
    uint64_t  file_extend_pending:1; /**< assert to one when file is extended by not yet confirm on exportd */
    uint64_t  file_extend_running:1; /**< assert to one when file is extended by not yet confirm on exportd */
    uint64_t  ientry_long:1; /**< assert to one when the ientry contains the extended attributes of the inode*/
    uint64_t  write_error:1; /**< Asserted when a write error ocuures. Cleared when write block is sent to exportd */
    dirbuf_t db; ///< buffer used for directory listing
    unsigned long nlookup; ///< number of lookup done on this entry (used for forget)
    list_t list;
    /** This is the address of the latest file_t structure on which there is some data
     ** pending in the buffer that have not been flushed to disk. Only one file_t at a time
     ** can be in this case for all the open that have occured on this file. Writing into
     ** a file_t buffer automaticaly triggers the flush to disk of the previous pending write.
     */ 
    file_t    * write_pending;
    list_t list_wr_block;   /**< to chain pending write block in case of no response from exportd  */    
    /**
     ** This counter is used for a reader to know whether the data in its buffer can be
     ** used safely or if they must be thrown away and a re-read from the disk is required
     ** because some write has occured since the last read.
     */
    uint64_t    read_consistency;
    uint64_t    timestamp;
    uint64_t    xattr_timestamp;
    uint64_t    timestamp_wr_block;
    char      * symlink_target;
    uint64_t    symlink_ts;
    int         pending_getattr_cnt;   /**< pending get attr count  */
    int         pending_setattr_with_size_update; /**< number of pending setattr triggered by a truncate callback */
//    uint32_t    io_write_error_counter;   /**< incremented each time there is an I/O write error for that i-node  */
    rozofs_slave_inode_t   *slave_inode_p;      /**< case of the multiple file */
    struct inode_internal_t attrs;   /**< attributes caching for fs_mode = block mode   */
    /* !!!WARNING !!! DO NOT ADD ANY FIELD BELOW attrs since that array can be extended for storing extended attributes */
} ientry_t;
/*
** ientry size when the client caches the extended attributes of the inode
*/
#define ROZOFS_IENTRY_LARGE_SZ   (sizeof(ientry_t) - sizeof(struct inode_internal_t) + sizeof(lv2_entry_t)+sizeof(uint64_t))

/*
** About exportd id quota
*/
extern uint64_t eid_free_quota;
extern int rozofs_xattr_disable; /**< assert to one to disable xattr for the exported file system */
/*
** write alignment statistics
*/
extern uint64_t    rozofs_aligned_write_start[2];
extern uint64_t    rozofs_aligned_write_end[2];
extern int ROZOFS_MAX_WRITE_PENDING;
extern int rozofs_site_number;

/**______________________________________________________________________________
*/
/**
*  get the current site number of the rozofsmount client

*/
static inline int rozofs_get_site_number()
{
  return rozofs_site_number;
}

/**______________________________________________________________________________
*/
/**
*  Set export id free block count when a quota is set
*  @param free_quota   Count of free blocks before reaching the hard quota
*
*/
static inline void eid_set_free_quota(uint64_t free_quota) {
  eid_free_quota = free_quota;
}
/**______________________________________________________________________________
*/
/**
*  Check export id hard quota
*
*  @param oldSize   Old size of the file
*  @param newSize   New size of the file
*
* @retval 0 not enough space left
* @retval 1 there is the requested space
*
*/
static inline int eid_check_free_quota(uint32_t bsize, uint64_t oldSize, uint64_t newSize) {
  uint64_t oldBlocks;
  uint64_t newBlocks;
  uint32_t bbytes = ROZOFS_BSIZE_BYTES(bsize);

  if (eid_free_quota == -1) return 1; // No quota so go on
  
  // File truncate is always good
  if (newSize < oldSize) return 1;

  // Compute current number of blocks of the file
  oldBlocks = oldSize / bbytes;
  if (oldSize % bbytes) oldBlocks++;

  // Compute futur number of blocks of the file
  newBlocks = newSize / bbytes;
  if (newSize % bbytes) newBlocks++;  
  
  if ((newBlocks-oldBlocks) > eid_free_quota) {
    errno = ENOSPC;
    return 0;
  }  
  return 1;
}


static inline uint32_t fuse_ino_hash_fnv_with_len( void *key1) {

    unsigned char *d = (unsigned char *) key1;
    int i = 0;
    int h;

     h = 2166136261U;
    /*
     ** hash on name
     */
    d = key1;
    for (i = 0; i <sizeof (fuse_ino_t) ; d++, i++) {
        h = (h * 16777619)^ *d;
    }
    return (uint32_t) h;
}

static inline uint32_t fuse_ino_hash(void *n) {
    return fuse_ino_hash_fnv_with_len(n);
}


extern uint64_t hash_inode_collisions_count;
extern uint64_t hash_inode_max_collisions;
extern uint64_t hash_inode_cur_collisions;

static inline int fuse_ino_cmp(void *v1, void *v2) {
      int ret;
      ret =  memcmp(v1, v2, sizeof (fuse_ino_t));
      if (ret != 0) {
          hash_inode_collisions_count++;
	  hash_inode_cur_collisions++;
	  return ret;
      }
      if (hash_inode_max_collisions < hash_inode_cur_collisions) hash_inode_max_collisions = hash_inode_cur_collisions;
      return ret;
//    return (*(fuse_ino_t *) v1 - *(fuse_ino_t *) v2);

}

static inline int fid_cmp(void *key1, void *key2) {
    return memcmp(key1, key2, sizeof (fid_t));
}

static inline unsigned int fid_hash(void *key) {
    uint32_t hash = 0;
    uint8_t *c;
    for (c = key; c != key + 16; c++)
        hash = *c + (hash << 6) + (hash << 16) - hash;
    return hash;
}

static inline void ientries_release() {
    return;
}

static inline void put_ientry(ientry_t * ie) {
    DEBUG("put inode: %llx\n",(unsigned long long int)ie->inode);
    rozofs_ientries_count++;
    ie->he.key   = &ie->inode;
    ie->he.value = ie;
    htable_put_entry(&htable_inode, &ie->he);
//    htable_put(&htable_fid, ie->fid, ie);
    list_push_front(&inode_entries, &ie->list);
}

static inline void del_ientry(ientry_t * ie) {
    DEBUG("del inode: %llx\n",(unsigned long long int) ie->inode);
    /*
    ** take care of the write_block retry: in that case the entry is not deleted
    ** it might e delauyed untile the exportd acknowlegdes the write_block
    */
    if (!list_empty(&ie->list_wr_block)) return;
    if (ie->nlookup != 0) return;
    
    rozofs_ientries_count--;
    htable_del_entry(&htable_inode, &ie->he);
//    htable_del(&htable_fid, ie->fid);
    list_remove(&ie->list);
    /*
    ** future: might need to check if the ientry is queued because one wr_block is missing 
    **         because of a exportd switch over
    **         In that case the ientry deletion must be delayed until the client successfully updates
    **         the i-node on the exportd with that lastest file size
    */
    list_remove(&ie->list_wr_block);
    if (ie->db.p != NULL) {
      free(ie->db.p);
      ie->db.p = NULL;
    }
    if (ie->symlink_target) {
      free(ie->symlink_target);
      ie->symlink_target = NULL;
    }
    /*
    ** check if is a long ientry. In such case we might need to release the memory used
    ** for storing the extended attributes.
    */
    if (ie->ientry_long)
    {
       lv2_entry_t *fake_lv2_p;
       fake_lv2_p = (lv2_entry_t*)&ie->attrs;       
       if (fake_lv2_p->extended_attr_p != NULL) xfree(fake_lv2_p->extended_attr_p);    
       fake_lv2_p->extended_attr_p = NULL;
    }
    /*
    ** Check if there are some slave inode associated with the ientry (case of the rozofs multiple file
    */
    if (ie->slave_inode_p != NULL)
    {
       xfree(ie->slave_inode_p);
       ie->slave_inode_p = NULL;    
    }
    xfree(ie);    
}

/**
*   That function returns 1 when the inode is associated with a directory

    @param ino: inode returned to fuse
    
    @retval 1: inode is a directory
    @retval 0: other inode type
*/
static inline int rozofs_is_directory_inode(fuse_ino_t ino)
{
    rozofs_inode_t fake_id;

    if (ino == 1) return 1;
    fake_id.fid[1]=ino;
    if ((ROZOFS_DIR_FID == fake_id.s.key) || (ROZOFS_DIR == fake_id.s.key)||(ROZOFS_TRASH == fake_id.s.key) )
    {
      return 1;
    }
    return 0;
}

static inline ientry_t *get_ientry_by_inode(fuse_ino_t ino) {
    rozofs_inode_t fake_id;

    if (ino == 0x2800000000000001) {
      fuse_ino_t ino_root = 1;
      return htable_get(&htable_inode, &ino_root);
    }
    fake_id.fid[1]=ino;
    /*
    ** check if the fid designate a directory referenced by its FID (relative path) or if is the trash associated with the
    ** directory: the delete bit in the fid can be set for the case of the trash only.
    */
    if (ROZOFS_TRASH != fake_id.s.key)
    {
      fake_id.s.del = 0;
    
    }
    if ( ROZOFS_DIR_FID == fake_id.s.key )
    {
      fake_id.s.key = ROZOFS_DIR;
    }
    hash_inode_cur_collisions = 0;
    return htable_get(&htable_inode, &fake_id.fid[1]);
}

static inline ientry_t *get_ientry_by_fid(fid_t fid) {
    rozofs_inode_t *fake_id = (rozofs_inode_t *) fid;
    /*
    ** check if empty inode to address the case of invalid attributes returned by exportd
    */
    if (fake_id->fid[1] == 0) return NULL; 
    if (memcmp(fid, exportclt.rfid, sizeof (fid_t)) == 0){
      return get_ientry_by_inode(ROOT_INODE);
    }
    return get_ientry_by_inode(fake_id->fid[1]);
}

static inline ientry_t *alloc_ientry(fid_t fid) {
	ientry_t *ie;
	rozofs_inode_t *inode_p ;
	int extended_attributes = 0;
	
	inode_p = (rozofs_inode_t*) fid;
	/*
	** clear the delete bit if the fid does not designate the trash associated with a directory
	*/
	if (inode_p->s.key != ROZOFS_TRASH) inode_p->s.del = 0;
	/*
	** Check the alloc mode for ientry
	*/
	if ((common_config.client_xattr_cache) || (conf.xattrcache))
	{
	  extended_attributes = 1;
	}

        if (extended_attributes)
	{
	  ie = xmalloc(ROZOFS_IENTRY_LARGE_SZ);
	  ie->ientry_long = 1;
	}
	else
	{
	  ie = xmalloc(sizeof(ientry_t));
	  ie->ientry_long = 0;
	}
        ie->write_error = 0;

	memcpy(ie->fid, fid, sizeof(fid_t));
        memset(ie->pfid, 0, sizeof(fid_t));
	ie->inode = inode_p->fid[1]; //fid_hash(fid);
	ie->slave_inode_p = NULL;
	list_init(&ie->list);
	list_init(&ie->list_wr_block);
	ie->db.size = 0;
	ie->db.eof = 0;
	ie->db.cookie = 0;
	ie->db.p = NULL;
	ie->nlookup = 0;
        ie->write_pending = NULL; 
        ie->read_consistency = 1;
	ie->file_extend_size = 0;
	ie->file_extend_pending = 0;
	ie->file_extend_running = 0;
	ie->mtime_locked        = 0;
	ie->timestamp_wr_block = 0;
	ie->xattr_timestamp = 0;
	ie->symlink_target = NULL;
        ie->symlink_ts     = 0;
	ie->pending_getattr_cnt= 0;
	ie->pending_setattr_with_size_update = 0;
//	ie->io_write_error_counter = 0;
	put_ientry(ie);
	/*
	** when the ientry stores the extended attributes we should initialize the ext_mattr section
	*/
	if (ie->ientry_long)
	{
	   lv2_entry_t *fake_lv2_p;
	   fake_lv2_p = (lv2_entry_t*)&ie->attrs;
	   	
	   memset(fake_lv2_p,0,sizeof(lv2_entry_t));   
	   fake_lv2_p->attributes.s.i_extra_isize = ROZOFS_I_EXTRA_ISIZE;	
	}

	return ie;
}



static inline ientry_t *recycle_ientry(ientry_t * ie, fid_t fid) {

	memcpy(ie->fid, fid, sizeof(fid_t));
	ie->db.size = 0;
	ie->db.eof = 0;
	ie->db.cookie = 0;
	if (ie->db.p != NULL) {
	  free(ie->db.p);
	  ie->db.p = NULL;
	}	
	// ie->nlookup = 0;
        ie->write_pending = NULL; 
        ie->read_consistency = 1;
	ie->file_extend_size = 0;	
	ie->file_extend_pending = 0;
	ie->file_extend_running = 0;
	ie->timestamp_wr_block = 0;
	if (ie->symlink_target) {
	  free(ie->symlink_target);
	  ie->symlink_target = NULL;
	}
	list_remove(&ie->list_wr_block);	
        ie->symlink_ts     = 0;
	return ie;
}

static inline void ientry_update_parent(ientry_t * ie, fid_t pfid) {

	memcpy(ie->pfid, pfid, sizeof(fid_t));
}

#define CLEAR_WRITE(p) \
{ \
  p->write_pos = 0;\
  p->write_from = 0;\
  p->buf_write_wait = 0;\
}


#define CLEAR_READ(p) \
{ \
  p->read_pos = 0;\
  p->read_from = 0;\
  p->buf_read_wait = 0;\
}

/*
**__________________________________________________________________
*/
/**
*  Some request may trigger an internal flush before beeing executed.

   That's the case of a read request while the file buffer contains
   some data that have not yet been saved on disk, but do not contain 
   the data that the read wants. 

   No fuse reply is expected

 @param fi   file info structure where information related to the file can be found (file_t structure)
 
 @retval 0 in case of failure 1 on success
*/

int rozofs_asynchronous_flush(struct fuse_file_info *fi) ;
/**
*  Flush all write pending on a given ientry 

 @param ie : pointer to the ientry in the cache
 
 @retval 1  on success
 @retval 0  in case of any flushing error
 */

static inline int flush_write_ientry(ientry_t * ie) {
    file_t              * f;
    struct fuse_file_info fi;
    int                   ret;
    
    /*
    ** Check whether any write is pending in some buffer open on this file by any application
    */
    if ((f = ie->write_pending) != NULL) {

       ie->write_pending = NULL;
       
       /*
       ** Double check this file descriptor points to this ie
       */
       if (f->ie != ie) {
         char fid_string[64];
	 uuid_unparse(ie->fid, fid_string);
         severe("Bad write pending ino %llu FID %s", (long long unsigned int)ie->inode, fid_string);
	 return 1;
       }
     
       fi.fh = (unsigned long) f;
       ret = rozofs_asynchronous_flush(&fi);
       if (ret == 0) return 0;

       f->buf_write_wait = 0;
       f->write_from     = 0;
       f->write_pos      = 0;
    }
    return 1;
}

static inline struct stat *mattr_to_stat(struct inode_internal_t * attr, struct stat *st, uint32_t bsize) {
    memset(st, 0, sizeof (struct stat));
    st->st_mode = attr->attrs.mode;
    st->st_nlink = attr->attrs.nlink;
    st->st_size = attr->attrs.size;
    st->st_ctime = attr->attrs.ctime;
    st->st_atime = attr->attrs.atime;
    st->st_mtime = attr->attrs.mtime;    
    st->st_blksize = ROZOFS_BSIZE_BYTES(bsize);
    /*
    ** check the case of the thin provisioning
    */
    if ((rozofs_get_thin_provisioning()) && (S_ISREG(st->st_mode)))
    {
      uint64_t local_blocks = attr->hpc_reserved.reg.nb_blocks_thin;
      local_blocks *=4096;
      st->st_blocks = ((local_blocks + 512 - 1) / 512);
    }
    else
      st->st_blocks = ((attr->attrs.size + 512 - 1) / 512);
    st->st_dev = 0;
    st->st_uid = attr->attrs.uid;
    st->st_gid = attr->attrs.gid;
    return st;
}

static inline mattr_t *stat_to_mattr(struct stat *st, mattr_t * attr,
		int to_set) {
	if (to_set & FUSE_SET_ATTR_MODE)
		attr->mode = st->st_mode;
	if (to_set & FUSE_SET_ATTR_SIZE)
		attr->size = st->st_size;
	if (to_set & FUSE_SET_ATTR_ATIME)
		attr->atime = st->st_atime;
	if (to_set & FUSE_SET_ATTR_MTIME)
		attr->mtime = st->st_mtime;
	if (to_set & FUSE_SET_ATTR_UID)
		attr->uid = st->st_uid;
	if (to_set & FUSE_SET_ATTR_GID)
		attr->gid = st->st_gid;
	return attr;
}

/*Export commands prototypes*/
void rozofs_ll_getattr_nb(fuse_req_t req, fuse_ino_t ino,
		struct fuse_file_info *fi);

void rozofs_ll_setattr_nb(fuse_req_t req, fuse_ino_t ino, struct stat *stbuf,
		int to_set, struct fuse_file_info *fi);

void rozofs_ll_lookup_nb(fuse_req_t req, fuse_ino_t parent, const char *name);

void rozofs_ll_mkdir_nb(fuse_req_t req, fuse_ino_t parent, const char *name,
		mode_t mode);

void rozofs_ll_mknod_nb(fuse_req_t req, fuse_ino_t parent, const char *name,
		mode_t mode, dev_t rdev);

void rozofs_ll_open_nb(fuse_req_t req, fuse_ino_t ino,
		struct fuse_file_info *fi);

void rozofs_ll_symlink_nb(fuse_req_t req, const char *link, fuse_ino_t parent,
		const char *name);

void rozofs_ll_readlink_nb(fuse_req_t req, fuse_ino_t ino);

void rozofs_ll_link_nb(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
		const char *newname);

void rozofs_ll_unlink_nb(fuse_req_t req, fuse_ino_t parent, const char *name);

void rozofs_ll_rmdir_nb(fuse_req_t req, fuse_ino_t parent, const char *name);

void rozofs_ll_rename_nb(fuse_req_t req, fuse_ino_t parent, const char *name,
		fuse_ino_t newparent, const char *newname);

void rozofs_ll_statfs_nb(fuse_req_t req, fuse_ino_t ino);

void rozofs_ll_create_nb(fuse_req_t req, fuse_ino_t parent, const char *name,
		mode_t mode, struct fuse_file_info *fi);

void rozofs_ll_setxattr_nb(fuse_req_t req, fuse_ino_t ino, const char *name,
		const char *value, size_t size, int flags);

void rozofs_ll_getxattr_nb(fuse_req_t req, fuse_ino_t ino, const char *name,
		size_t size);

void rozofs_ll_removexattr_nb(fuse_req_t req, fuse_ino_t ino, const char *name);

void rozofs_ll_listxattr_nb(fuse_req_t req, fuse_ino_t ino, size_t size);

void rozofs_ll_readdir_nb(fuse_req_t req, fuse_ino_t ino, size_t size,
		off_t off, struct fuse_file_info *fi);

void rozofs_ll_read_nb(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
		struct fuse_file_info *fi);

void rozofs_ll_write_nb(fuse_req_t req, fuse_ino_t ino, const char *buf,
		size_t size, off_t off, struct fuse_file_info *fi);

void rozofs_ll_flush_nb(fuse_req_t req, fuse_ino_t ino,
		struct fuse_file_info *fi);

void rozofs_ll_release_nb(fuse_req_t req, fuse_ino_t ino,
		struct fuse_file_info *fi);

void rozofs_ll_getlk_nb(fuse_req_t req, fuse_ino_t ino,
		struct fuse_file_info *fi, struct flock *lock);

void rozofs_ll_setlk_nb(fuse_req_t req, fuse_ino_t ino,
		struct fuse_file_info *fi, struct flock *lock, int sleep);

void rozofs_ll_flock_nb(fuse_req_t req, fuse_ino_t ino,
		struct fuse_file_info *fi, int op);

void init_write_flush_stat(int max_write_pending);

void rozofs_ll_opendir_nb(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void rozofs_ll_releasedir_nb(fuse_req_t req, fuse_ino_t ino,struct fuse_file_info *fi);
void init_write_thread_active(int nb_writeThreads);
/*
**__________________________________________________________________
*/
/**
 *  flush the content of the buffer to the disk

 @param fuse_ctx_p: pointer to the fuse transaction context
 @param p : pointer to the file structure that contains buffer information

 @retval len_write >= total data length push to the disk
 @retval < 0 --> error while attempting to initiate a write request towards storcli
 */
int buf_flush(void *fuse_ctx_p,file_t *p);

int export_write_block_asynchrone(void *fuse_ctx_p, file_t *file_p,
		sys_recv_pf_t recv_cbk);

/*
**__________________________________________________________________
*/
/**
 API to clear the buffer after a flush
 If some data is pending in the buffer the clear is not done

 @param *p : pointer to the file structure where read buffer information can be retrieved

 @retval -1 some data to write is pending
 @retval 0 if the read buffer is not empty
 */
int clear_read_data(file_t *p);

/*
**__________________________________________________________________
*/
/**
  the goal of that API is to update the metadata attributes in
  the ientry.
  
  @param ientry_t *ie
  @param struct inode_internal_t  attr

*/
static inline void rozofs_ientry_update(ientry_t *ie,struct inode_internal_t  *attr_p)
{

    /**
    *  update the timestamp in the ientry context
    */
    ie->timestamp = rozofs_get_ticker_us();
    /*
    ** check if there is a pending extension of the size
    */
    if ((ie->file_extend_pending == 0)&&(ie->file_extend_running == 0))
    {
       /*
       ** nothing pending so full copy
       */
       memcpy(&ie->attrs,attr_p, sizeof (struct inode_internal_t));   
       return;
   }
   /*
   ** preserve the size of the ientry
   */
   uint64_t file_size = ie->attrs.attrs.size;
   memcpy(&ie->attrs,attr_p, sizeof (struct inode_internal_t));   
   ie->attrs.attrs.size = file_size;
}
/*
**__________________________________________________________________
*/
/**
  rozofsmount applicative supervision callback to check the connection 
  toward the exportd thanks to an EP_NULL question/answer
  
  @param sock_p socket context

*/
void rozofs_export_lbg_cnx_polling(af_unix_ctx_generic_t  *sock_p);
/**
*  reset all the locks of a given client
*  This is an internal request that do not trigger any response toward fuse


 @param eid           :eid this client is mounted on
 @param client_hash   :reference of the client
 
 @retval none
*/
void rozofs_ll_clear_client_file_lock(int eid, uint64_t client_hash);
/**
* Read/write bandwidth counters
*/
typedef enum _rozofs_mount_counter_e {
   ROZOFSMOUNT_COUNTER_READ_THR,
   ROZOFSMOUNT_COUNTER_READ_IO,
   ROZOFSMOUNT_COUNTER_READ_LATENCY,
   ROZOFSMOUNT_COUNTER_WRITE_THR,
   ROZOFSMOUNT_COUNTER_WRITE_IO,
   ROZOFSMOUNT_COUNTER_WRITE_LATENCY,
   ROZOFSMOUNT_COUNTER_WRITE_LATENCY_COUNT,
   ROZOFSMOUNT_COUNTER_FCR8,
   ROZOFSMOUNT_COUNTER_FDEL,
   ROZOFSMOUNT_COUNTER_DCR8,
   ROZOFSMOUNT_COUNTER_DDEL,
   ROZOFSMOUNT_COUNTER_LOOKUP,
   ROZOFSMOUNT_COUNTER_GETATTR,
   ROZOFSMOUNT_COUNTER_XATTR,
   ROZOFSMOUNT_COUNTER_OTHER,
   ROZOFSMOUNT_COUNTER_MAX
} rozofs_mount_counter_e;

extern rozofs_thr_cnts_t *rozofs_thr_counter[];

/*
**__________________________________________________________________
*/
/**
*   Fill up the information needed by storio in order to read/write a file

    @param ie: pointer to the inode entry that contains file information
    @param cid: pointer to the array where the cluster_id is returned
    @param sids_p: pointer to the array where storage id are returned
    @param fid_storage: pointer to the array where the fid of the file on storage is returned
    
    @retval 0 on success
    @retval < 0 error (see errno for details)
*/
static inline int rozofs_fill_storage_info(ientry_t *ie,cid_t *cid,uint8_t *sids_p,fid_t fid_storage)
{
  mattr_t *attrs_p;
  rozofs_inode_t *inode_p;
  int key = ROZOFS_PRIMARY_FID;
  int ret;
  rozofs_mover_sids_t *dist_mv_p;
  
  
  attrs_p = &ie->attrs.attrs;
  inode_p = (rozofs_inode_t*)attrs_p->fid; 
  
  if (inode_p->s.key == ROZOFS_REG_D_MOVER) key = ROZOFS_MOVER_FID; 
  ret = rozofs_build_storage_fid_from_attr(attrs_p,fid_storage,key);
  if (ret < 0) return ret;
  /*
  ** get the cluster and the list of the sid
  */
  if (key == ROZOFS_MOVER_FID)
  {
    dist_mv_p = (rozofs_mover_sids_t*)attrs_p->sids;
    *cid = dist_mv_p->dist_t.mover_cid;
    memcpy(sids_p,dist_mv_p->dist_t.mover_sids,ROZOFS_SAFE_MAX_STORCLI);
  }
  else
  {
    *cid = attrs_p->cid;
    memcpy(sids_p,attrs_p->sids,ROZOFS_SAFE_MAX_STORCLI);  
  }
  return 0;
}

/*
**__________________________________________________________________
*/
/**
  DeQueue an ientry from pending write block list
  
  @param ie : ientry context
  
  @retval none

 */
static inline void rozofs_export_dequeue_ie_from_wr_block_list(ientry_t *ie)
{
 if (list_empty(&ie->list_wr_block)) return;
  /*
  ** remove from the pending list
  */
  list_wr_block_count--;
  list_remove(&ie->list_wr_block);
}

/*
**__________________________________________________________________
*/
/**
  Queue a pending write block in the pending list
 
   The entry parameter should contain the inode value provide by VFS
 
  @param ie : ientry context
  
  @retval none

 */
static inline void rozofs_export_queue_ie_in_wr_block_list(ientry_t *ie)
{
  /*
  ** remove from the pending list
  */
  if (list_empty(&ie->list_wr_block))
  {
    list_wr_block_count++;
    list_push_back(&list_wr_block_head, &ie->list_wr_block);
  }  
}

void rozofs_export_write_block_list_process(void);


/*
**__________________________________________________________________

    M U L T I P L E   F I L E  S E R V I C E S
**__________________________________________________________________
*/    


/*
**__________________________________________________________________
*/
/** That function returns the striping size of the file
    That information is found in one attributes of the main inode
    
    @param ie: pointer to the inode information
    
    @retval > 0: value of the striping size in byte
    @retval < 0 : error (see errno for details)
 */
static inline int rozofs_get_striping_size_from_ie(ientry_t *ie)
{
  rozofs_multiple_desc_t *p;  
  struct inode_internal_t *inode_p = &ie->attrs;  
  
  p = &inode_p->multi_desc;

  return rozofs_get_striping_size(p);
}


/*
**__________________________________________________________________
*/
/** That function returns the hybrid size portion of a file
    That information is found in one attributes of the main inode
    
    @param ie: pointer to the inode information
    
    @retval  0: the file in not hybrid
    @retval > 0: value of the hybrid size in byte
    @retval < 0 : error (see errno for details)
 */
static inline int rozofs_get_hybrid_size_from_ie(ientry_t *ie)
{
  rozofs_multiple_desc_t *p;  
  rozofs_hybrid_desc_t *q;  
  struct inode_internal_t *inode_p = &ie->attrs;  
  
  p = &inode_p->multi_desc;
  q = &inode_p->hybrid_desc;

  return rozofs_get_hybrid_size(p,q);
}

/*
**__________________________________________________________________
*/
/**
  Get the striping factor: that value indicates the number of secondary inode that
  are associated with the primary inode
  
  The FID of the secondary inodes are in the same file index as the primary inode*
  the first secondary inode is found has the next index in sequence in the file index
  that contains the primary inode

    @param ie: pointer to the inode information
    
    @retval > 0: value of the striping size in byte
    @retval < 0 : error (see errno for details)
*/

static inline int rozofs_get_striping_factor_from_ie(ientry_t *ie)
{
  rozofs_multiple_desc_t *p;
  struct inode_internal_t *inode_p = &ie->attrs;  
  p = &inode_p->multi_desc;
  return rozofs_get_striping_factor(p);

}


/*
**__________________________________________________________________
*/
/**
  Write slave inode context
  
  That service allocates memory to save the information related to the slave i-node.
  The size of the arry depends on the striping factor of the master inode.
  

    @param ie: pointer to the inode information
    @param len : data length in buffer_p
    @param buffer_p: pointer to the slave inodes descriptors
    
    @retval  0 on success
    @retval < 0 : error (see errno for details)
*/

static inline int rozofs_ientry_slave_inode_write(ientry_t *ie,uint32_t len,uint8_t *buffer_p)
{
  rozofs_multiple_desc_t *p;
  rozofs_slave_inode_t *slave_inode_p;
  int striping_factor;
  struct inode_internal_t *inode_p = &ie->attrs;  
  
  p = &inode_p->multi_desc;
  slave_inode_p = (rozofs_slave_inode_t*)buffer_p;
  /*
  ** Get the number of files
  */
  striping_factor = rozofs_get_striping_factor(p);
  if (striping_factor < 0) return -1;
  

  if (striping_factor == 1) 
  {
    /*
    ** Nothing to do since it the default case: single file
    */
    return 0;
  }
  if (len != sizeof(rozofs_slave_inode_t)*striping_factor)
  {
    severe("slave inode buffer size insconsistent: %u (expected :%u)",(unsigned int) len,(unsigned int)sizeof(rozofs_slave_inode_t)*striping_factor);
    errno = EINVAL;
    return -1;
  }
  if (ie->slave_inode_p == NULL) 
  {
    ie->slave_inode_p = xmalloc(sizeof(rozofs_slave_inode_t)*striping_factor);
    if (ie->slave_inode_p == NULL)
    {
      errno = ENOMEM;
      return -1;
    }
  }
  memcpy(ie->slave_inode_p,slave_inode_p,sizeof(rozofs_slave_inode_t)*striping_factor);
  return 0;
}


/*
**__________________________________________________________________
*/
/**
  Write slave inode context
  
  That service allocates memory to save the information related to the slave i-node.
  The size of the arry depends on the striping factor of the master inode.
  

    @param ie: pointer to the inode information
    @param slave_inode_p: pointer to the slave inodes descriptors
    
    @retval  0 on success
    @retval < 0 : error (see errno for details)
*/

static inline int rozofs_write_slave_inode(ientry_t *ie,rozofs_slave_inode_t *slave_inode_p)
{
  rozofs_multiple_desc_t *p;
  int striping_factor;
  struct inode_internal_t *inode_p = &ie->attrs;  
  p = &inode_p->multi_desc;
  /*
  ** Get the number of files
  */
  striping_factor = rozofs_get_striping_factor(p);
  if (striping_factor < 0) return -1;
  
  if (ie->slave_inode_p != NULL) 
  {
     xfree(ie->slave_inode_p);
     ie->slave_inode_p = NULL;
  }
  if (striping_factor == 1) 
  {
    /*
    ** Nothing to do since it the default case: single file
    */
    return 0;
  }
  ie->slave_inode_p = xmalloc(sizeof(rozofs_slave_inode_t)*striping_factor);
  if (ie->slave_inode_p == NULL)
  {
    errno = ENOMEM;
    return -1;
  }
  memcpy(ie->slave_inode_p,slave_inode_p,sizeof(rozofs_slave_inode_t)*striping_factor);
  return 0;
}




/*
**__________________________________________________________________
*/
/**
  get slave inode contexts
  
  That service allocates memory to save the information related to the slave i-node.
  The size of the arry depends on the striping factor of the master inode.
  

    @param ie: pointer to the inode information
    
    @retval <>NULL :: pointer to the inode slave contexts
    @retval NULL : not slave inode contexts
*/

static inline rozofs_slave_inode_t *rozofs_get_slave_inode_from_ie(ientry_t *ie)
{

  return ie->slave_inode_p;
}




/*
**__________________________________________________________________
*/
/**
*   Fill up the information needed by storio in order to read/write a file

    @param ie: pointer to the inode entry that contains file information
    @param cid: pointer to the array where the cluster_id is returned
    @param sids_p: pointer to the array where storage id are returned
    @param fid_storage: pointer to the array where the fid of the file on storage is returned
    @param file_index
    
    @retval 0 on success
    @retval < 0 error (see errno for details)
*/
static inline int rozofs_fill_storage_info_multiple(ientry_t *ie,cid_t *cid,uint8_t *sids_p,fid_t fid_storage,uint16_t file_idx)
{
  mattr_t *attrs_p;
  rozofs_inode_t *inode_p;
  rozofs_mover_children_t mover_idx;   
  rozofs_slave_inode_t *slave_inode_p = NULL; 
   
  if (file_idx == 0) return rozofs_fill_storage_info(ie,cid,sids_p,fid_storage);
  
  /*
  ** case of a slave file
  */
  file_idx -=1;
  /*
  ** Get the pointer to the slave inodes
  */
  slave_inode_p = rozofs_get_slave_inode_from_ie(ie);
  if (slave_inode_p == NULL)
  {  
     errno = EINVAL;
     return -1;
  }
  slave_inode_p +=file_idx;
  mover_idx.u32 = slave_inode_p->children;
  
  attrs_p = &ie->attrs.attrs;  
  /*
  ** get the cluster and the list of the sid
  */
  memcpy(fid_storage,attrs_p->fid,sizeof(fid_t));
  /*
  ** append the file index to the fid to get the real fid of the slave inode on the storage side
  */
  inode_p = (rozofs_inode_t*)fid_storage;
  inode_p->s.idx += (file_idx+1);

  rozofs_build_storage_fid(fid_storage,mover_idx.fid_st_idx.primary_idx);   
  
  *cid = slave_inode_p->cid;
  memcpy(sids_p,slave_inode_p->sids,ROZOFS_SAFE_MAX_STORCLI);  
  return 0;
}


#define ROZOFS_FDL_MAX_SID 12  /* for test purpose */
#define ROZOFS_FDL_FILE_SID 8 /* for test purpose */
static inline int rozofs_build_fake_slave_inode(ientry_t *ie,int striping_factor,int striping_unit)
{

  int nb_files;
  rozofs_slave_inode_t *slave_inode_p, *q;
  rozofs_mover_children_t mover_idx; 
  int ret;
  rozofs_multiple_desc_t * p;
  int i;

#if 0
  uint8_t dist1[]={1,3,4,5,6,7,8,2};
  uint8_t dist2[]={6,7,8,9,10,11,12,2};
  uint8_t dist3[]={10,11,12,1,2,3,4,5};
  uint8_t dist4[]={3,4,5,6,7,8,10,11};     
#endif

  uint8_t dist1[]={1,4,7,10,2,5,8,11};
  uint8_t dist2[]={3,5,8,11,1,6,9,12};
  uint8_t dist3[]={1,6,9,12,3,4,7,10};
  uint8_t dist4[]={3,5,8,11,10,7,6,2};    

  
  struct inode_internal_t *inode_p = &ie->attrs;  
  p = &inode_p->multi_desc;
  p->byte = 0;
  
  if (striping_factor > ROZOFS_MAX_STRIPING_FACTOR)
  {
     errno = EINVAL;
     return -1;
  }
  if (striping_unit > ROZOFS_MAX_STRIPING_UNIT_POWEROF2) 
  {
     errno = EINVAL;
     return -1;
  }  
  p->common.master = 1;
  p->master.striping_unit = striping_unit;
  p->master.striping_factor = striping_factor;
  
  nb_files = rozofs_get_striping_factor(p);
  
  slave_inode_p = malloc(sizeof(rozofs_slave_inode_t)*nb_files);
  q = slave_inode_p;
  mover_idx.u32 = 0;
  for (i=0; i < nb_files;i++,q++)
  {
      q->size = 0;
      q->cid = inode_p->attrs.cid;
      memset(q->sids,0,ROZOFS_SAFE_MAX);
      switch (i)
      {
         case 0:
	   memcpy(q->sids,dist1,ROZOFS_FDL_FILE_SID);
	   break;
         case 1:
	   memcpy(q->sids,dist2,ROZOFS_FDL_FILE_SID);
	   break;
         case 2:
	   memcpy(q->sids,dist3,ROZOFS_FDL_FILE_SID);
	   break;
         case 3:
	   memcpy(q->sids,dist4,ROZOFS_FDL_FILE_SID);
	   break;
      
      }
#if 0
      {
         int idx;
//	 pbuf += sprintf(pbuf,"File %d: ",i+1);
         for (idx = 0; idx < ROZOFS_FDL_FILE_SID;idx++)
	 {
//	   q->sids[idx]=(idx+1+4*(i))%ROZOFS_FDL_MAX_SID;
//           q->sids[idx]=(dist[idx]+4*(i))%ROZOFS_FDL_MAX_SID;
	   if (q->sids[idx] == 0) q->sids[idx]=ROZOFS_FDL_MAX_SID;
//	   pbuf +=sprintf(pbuf,"%d ",q->sids[idx]);
         }
      }
//      info ("FDL distrib: %s",bufall);
#endif
      mover_idx.fid_st_idx.primary_idx = i+1;
      q->children = mover_idx.u32;      
  
  }
  /*
  ** move it in the ientry
  */
  ret = rozofs_write_slave_inode(ie,slave_inode_p);
  free(slave_inode_p);
  return ret;
  
}

/*
**__________________________________________________________________
*/
/*
**  Update the size of a slave inode
    
    That function is called during file write
   
    @param ie: pointer to the master ientry
    @param file_idx : index of the slave inode
    @param size: new size
    
    @retval 0 on success
    @retval < 0 on error see errno for details
*/
static inline int rozofs_slave_inode_update_size_write(ientry_t *ie,int file_idx,size_t size)
{
  rozofs_slave_inode_t *slave_inode_p = NULL; 
   
  /*
  ** does not care about file index 0 since it designates the master inode
  */
  if (file_idx == 0) return 0;
  
  /*
  ** case of a slave file
  */
  file_idx -=1;
  /*
  ** Get the pointer to the slave inodes
  */
  slave_inode_p = rozofs_get_slave_inode_from_ie(ie);
  if (slave_inode_p == NULL)
  {  
     errno = EINVAL;
     return -1;
  }
  slave_inode_p +=file_idx;
  if (slave_inode_p->size < size) slave_inode_p->size = size;

  return 0;
}


/*
**__________________________________________________________________
*/
/*
**  Update the size of a slave inode
    
    That function is called during file truncate 
   
    @param ie: pointer to the master ientry
    @param file_idx : index of the slave inode
    @param size: new size
    
    @retval 0 on success
    @retval < 0 on error see errno for details
*/
static inline int rozofs_slave_inode_update_size_truncate(ientry_t *ie,int file_idx,size_t size)
{
  rozofs_slave_inode_t *slave_inode_p = NULL; 
   
  /*
  ** does not care about file index 0 since it designates the master inode
  */
  if (file_idx == 0) return 0;
  
  /*
  ** case of a slave file
  */
  file_idx -=1;
  /*
  ** Get the pointer to the slave inodes
  */
  slave_inode_p = rozofs_get_slave_inode_from_ie(ie);
  if (slave_inode_p == NULL)
  {  
     errno = EINVAL;
     return -1;
  }
  slave_inode_p +=file_idx;
  slave_inode_p->size = size;

  return 0;
}   

/*
**__________________________________________________________________
*/
/**
* Truncate  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_truncate_cbk(void *this,void *param);
/*
**__________________________________________________________________
*/
/** Truncate of file has been created as a multiple file
 
    The opaque fields of the transaction context are used as follows:
    
     - opaque[1]: index of the file
 *
 * @param fuse_ctx_p: pointer to the rozofs fuse context
 * @param size: new size of the file
 * @param ie: pointer to the i-node context of the client
 
*/ 
int truncate_buf_multitple_nb(void *fuse_ctx_p,ientry_t *ie, uint64_t size);
/*
**__________________________________________________________________
*/
/** Send a request to the export server to know the file size
 *  adjust the write buffer to write only whole data blocks,
 *  reads blocks if necessary (incomplete blocks)
 *  and uses the function write_blocks to write data
 
    The opaque fields of the transaction context are used as follows:
    
     - opaque[0]: command index (index within the shared buffer=
     - opaque[1]: index of the file
     - opaque[2]: requested length
 *
 * @param *f: pointer to the file structure
 * @param off: offset to write from
 * @param *buf: pointer where the data are be stored
 * @param len: length to write
*/
 
int read_buf_multitple_nb(void *fuse_ctx_p,file_t * f, uint64_t off, const char *buf, uint32_t len);
/*
**__________________________________________________________________
*/
/** Send a request to the export server to know the file size
   adjust the write buffer to write only whole data blocks,
   reads blocks if necessary (incomplete blocks)
   and uses the function write_blocks to write data
   
   When 'thread' is asserted, we just copy the data from kernel to the allocated shared bufffer 
   and then sends back a response in order to finish the write transaction.
 
    The opaque fields of the transaction context are used as follows:
    
     - opaque[0]: command index (index within the shared buffer=
     - opaque[1]: index of the file
 
 @param *f: pointer to the file structure
 @param off: offset to write from
 @param *buf: pointer where the data are be stored
 @param len: length to write
 @param thread : assert to 1 if it is called from the write thread , 0 otherwise
*/
 
int64_t write_buf_multiple_nb(void *fuse_ctx_p,file_t * f, uint64_t off, const char *buf, uint32_t len,int thread);

/*
**__________________________________________________________________________
*/
/**
   
   Process a write request after the copy of data from kernel to shared buffer by a write thread.
   It corresponds to the function which is called when the file has no slave inodes (multiple file)

    The message contains the following information:
   -opcode          : ROZOFS_FUSE_WRITE_BUF_MULTI
   -rozofs_tx_ctx_p : contains the pointer to the fuse_context
   -status          : 0 if not error -1 otherwise
   -errval          : errno value
   
   The message has been encoded by the write thread, so we just need to send the message to the selected storcli
*/   
void af_unix_fuse_write_process_response_multiple(void *msg_p);


/*
**__________________________________________________________________________
*/
/**
*   Post a write request in multiple mode towards a write thread

    In multiple mode, the write thread performs the copy of the write data from the kernel space towards 
    the allocated shared buffer, then upon receiving the response, the main thread allocates
    all the need resources to perform the write.
    The only difference between the write without threads is the copy of the data from the kernel by the thread

    The message contains the following information:
   -opcode          : ROZOFS_FUSE_WRITE_BUF_MULTI
   -rozofs_tx_ctx_p : contains the pointer to the fuse_context
   -rm_xid          : length to write
   
   all the other fields are not significant 
    
    @param fuse_ctx_p
    @param off: file offset
    @param len: length to copy
    
    @retval 0 on success
    @retval -1 on error (see errno for details)
*/
int rozofs_storcli_wr_thread_send_multiple(void *fuse_ctx_p,uint64_t off,uint32_t len);
#endif
