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

#ifndef _EXPORT_H
#define _EXPORT_H

/** export API
 *
 * export manages storage of meta data. Rozofs managed several exports but
 * each of them has is responsible for its own meta data which are
 * self-contained and self-sufficient. Any empty repository can be used as
 * an export. The repository will be initialize at exportd startup
 * (@see export_create()). Each repository is a 3 level directories tree.
 * First level (lv1 in the code) is a hash table of fid_t (which should
 * identifies uniquely a rozofs file across exports). Second level contains
 * files (regular or directory) named by fid. Rozofs regular files's meta data
 * are stored in regular files. Rozofs directories leads to directories which
 * contains Third level files theses last correspond to directory entries and
 * meta data needs to find each of them.
 */

#include <limits.h>
#include <sys/stat.h>
#include <uuid/uuid.h>
#include <sys/param.h>
#include <sys/vfs.h> 

#include <rozofs/rozofs.h>
#include <rozofs/common/dist.h>
#include <rozofs/common/export_track.h>
#include "volume.h"
#include "cache.h"
#include "export_expgw_conf.h"
#include "geo_replication.h"

#define TRASH_DNAME "trash"
#define FSTAT_FNAME "fstat"
#define CONST_FNAME "const"

/** 'to_set' flags in setattr */
#define EXPORT_SET_ATTR_MODE  (1 << 0)
#define EXPORT_SET_ATTR_UID   (1 << 1)
#define EXPORT_SET_ATTR_GID   (1 << 2)
#define EXPORT_SET_ATTR_SIZE  (1 << 3)
#define EXPORT_SET_ATTR_ATIME (1 << 4)
#define EXPORT_SET_ATTR_MTIME (1 << 5)


/** Nb. max of entries to delete during one call of export_rm_bins function */
#define RM_FILES_MAX 500
/* Frequency calls of export_rm_bins function */
#define RM_BINS_PTHREAD_FREQUENCY_SEC 2
/** File size treshold: if a removed file is bigger than or equel to this size
 *  it will be push in front of list */
#define RM_FILE_SIZE_TRESHOLD 0x40000000LL
#define TRACKING_PTHREAD_FREQUENCY_SEC 60  /**< default polling period of tracking thread */

/*
** Maximum numer of rmbins thread
*/
#define ROZO_NB_RMBINS_THREAD    8

/*
** rmbins thread context
*/
typedef struct _rmbins_thread_t {
  int           idx;        /**< Thread index */
  pthread_t     thId;       /**< Thread id    */
  uint64_t      nb_run;     /**< Nb of run processes */
  uint64_t      last_count; /**< Last run deletion count */
  uint64_t      last_usec;  /**< Last run duration */
  uint64_t      total_count;/**< Total deletion count */
  uint64_t      total_usec; /**< Total run duration */

} rmbins_thread_t;
extern rmbins_thread_t rmbins_thread[];

/** stat of an export
 * these values are independent of volume
 */
#define ROZOFS_MAX_BLOCK_BITS  (ROZOFS_STORAGE_FILE_MAX_SIZE_POWER2-12+2)
typedef struct export_fstat {
    uint64_t blocks;
    uint64_t files;
    uint64_t file_per_size[ROZOFS_MAX_BLOCK_BITS];
    uint64_t blocks_thin;  /**< number of blocks for thin provisioning */
} export_fstat_t;


/** structure for store the list of files to remove
 * for one trash bucket.
 */
typedef struct trash_bucket {
    list_t rmfiles; ///< List of files to delete
    pthread_rwlock_t rm_lock; ///< Lock for the list of files to delete
} trash_bucket_t;

/** structure for store the list of files to recycle
 * for one recycle bucket.
 */
typedef struct recycle_bucket {
    list_t rmfiles; ///< List of files to recycle
} recycle_bucket_t;
/**
* structuire for tracking file for which there is a deletion in progress
  it corresponds to the structure saved on diosk
*/
typedef struct _rmfentry_disk_t {
    fid_t trash_inode;      /**< reference of the trash inode */
    uint64_t size;          /**< size of the file */
    fid_t fid;              /**<  unique file id .*/
    cid_t cid;              /**< unique cluster id where the file is stored.*/
    sid_t initial_dist_set[ROZOFS_SAFE_MAX];
    ///< initial sids of storage nodes target for this file.
    sid_t current_dist_set[ROZOFS_SAFE_MAX];
} rmfentry_disk_t;


/**
 *  Structure used for store information about a file to remove
 */
typedef struct rmfentry {
    fid_t trash_inode;      /**< reference of the trash inode */
    fid_t fid; ///<  unique file id.
    cid_t cid; /// unique cluster id where the file is stored.
    sid_t initial_dist_set[ROZOFS_SAFE_MAX];
    ///< initial sids of storage nodes target for this file.
    sid_t current_dist_set[ROZOFS_SAFE_MAX];
    ///< current sids of storage nodes target for this file.
    time_t   time; //< Time when the effective deletion can occur
    list_t list; ///<  pointer for extern list.
} rmfentry_t;

/**
* structuire for tracking file for which there is a fid recycle
  it corresponds to the structure saved on disk
*/
typedef struct _recycle_disk_t {
    fid_t fid;              /**<  unique file id .*/
    fid_t recycle_inode;      /**< reference of the recycle inode */
    uint32_t filler[16];
} recycle_disk_t;

/**
* structuire for tracking file for which there is a fid recycle
  it corresponds to the structure saved on disk
*/
typedef struct _recycle_mem_t {
    fid_t fid;              /**<  unique file id .*/
    fid_t recycle_inode;      /**< reference of the recycle inode */
    list_t list; ///<  pointer for extern list.
} recycle_mem_t;

/**
* exportd gateway entry structure
*/

typedef struct expgw_entry {
    expgw_t expgw;
    list_t list;
} expgw_entry_t;

/** must be a power of 2
*/
#define EXPORT_GEO_MAX_BIT 1
#define EXPORT_GEO_MAX_CTX (1<<EXPORT_GEO_MAX_BIT)

/*
** Metadata device resources
*/
typedef struct _metadat_resource_t {
  uint64_t  total;         //<* Maximum capacity of the device
  uint64_t  free ;         //<* Current free capacity     
} metadat_resource_t;

typedef struct _meta_resources_t {
  uint8_t   full;          //<* Flash full indicator    
  uint64_t  next_microsec; //<* When to retry a statfs on flash
  uint64_t  full_counter;  //<* Nb times full is returned on mknod/mkdir    
  uint64_t  statfs_error;  //<* Nb times the statfs returned an error   
  metadat_resource_t   inodes;
  metadat_resource_t   sizeMB;
} meta_resources_t;
/** export stucture
 *
 */
typedef struct export {
    eid_t eid; ///< export identifier
    volume_t *volume; ///< the volume export relies on
    uint32_t bsize; ///< the block size from enum ROZOFS_BSIZE_E
    char root[PATH_MAX]; ///< absolute path of the storage root
    char name[PATH_MAX]; ///< Export name
    uint8_t layout; ///< layout
    
    /*  Some options */
    uint8_t thin:1;  //< Thin provisionning */
    uint8_t backup:1;  //< backupflag needed for asynchronous replication */

    /*
    ** To check metadat device resources
    */
    meta_resources_t   space_left;
    
    uint64_t squota; ///< soft quota in blocks
    uint64_t hquota; ///< hard quota in blocks
    void    *quota_p;  ///< pointer to the quota context
//    export_fstat_t fstat; ///< fstat value
    int fdstat; ///< open file descriptor on stat file
    fid_t rfid; ///< root fid
    lv2_cache_t *lv2_cache; ///< cache of lv2 entries
    export_tracking_table_t *trk_tb_p; 
    geo_rep_srv_ctx_t  *geo_replication_tb[EXPORT_GEO_MAX_CTX];        
    trash_bucket_t * trash_buckets; ///< Address of the array of trash buckets of this export
    recycle_bucket_t * recycle_buckets; ///< Address of the array of recycle buckets of this export
    // of files to delete for each bucket trash
    pthread_t load_trash_thread; ///< pthread for load the list of trash and recycle files
    // to delete when we start or reload this export
    rozofs_ip4_subnet_t * filter_tree;
    char md5[ROZOFS_MD5_SIZE]; ///< passwd
} export_t;

extern uint32_t export_configuration_file_hash;  /**< hash value of the configuration file */
extern int export_local_site_number; 
extern int rozofs_no_site_file; 
extern uint64_t export_rm_bins_trashed_count; /**< trash thread statistics  */
extern uint64_t export_rm_bins_done_count ;  /**< trash thread statistics  */
extern int export_limit_rm_files;


/**
*  trash statistics display

   @param buf : pointer to the buffer that will contains the statistics
*/
char *export_rm_bins_stats(char *pChar);
char *export_rm_bins_stats_json(char *pChar);
/**
*  Reload in memory the files that have not yet been deleted
*  return the current sitde number of the exportd

   @param e : pointer to the export structure
   
*/
int export_load_rmfentry(export_t * e) ;
/**
*  Reload in memory the files that could have their fid recycled

   @param e : pointer to the export structure
   
*/
int export_load_recycle_entry(export_t * e);
/**
* statistics for tracking thread
*/
void show_tracking_thread(char * argv[], uint32_t tcpRef, void *bufRef);
/**
*  return the current sitde number of the exportd

  @param none
  
  @retval local_site_unumber (default 0)
*/
static inline int export_get_local_site_number()
{
  return export_local_site_number;
}
/** Remove bins files from trash 
 *
 * @param export: pointer to the export
 * @param first_bucket_idx: pointer for the first index bucket to remove
 * @param thCtx: pointer for the rmbins thread context
 *
 * @return: Number of deleted files
 */
uint64_t export_rm_bins(export_t * e, uint16_t * first_bucket_idx, rmbins_thread_t * thCtx);


/** check if export directory is valid
 *
 * @param root: root directory to check
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_is_valid(const char *root);


/** create an export
 *
 * create rozofs system files
 * create a export_stat file an a trash directory
 * generate root uuid create lv2 directory
 *
 * @param root: root directory to create file in
 * @param export: pointer to the export
 * @param lv2_cache: pointer to the cache to use
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_create(const char *root,export_t * e,lv2_cache_t *lv2_cache);

/** initialize an export
 *
 * @param export: pointer to the export
 * @param volume: pointer to the volume the export relies on
 * @param layout: Layout of this export. May differ from the volume layout
 * @param bsize: Block size as defined in enum ROZOFS_BSIZE_E
 * @param lv2_cache: pointer to the cache to use
 * @param eid: id of this export
 * @param root: path to root directory
 * @param name:export name
 * @param md5: password
 * @param squota: soft quotas
 * @param: hard quotas
 * @param: IPv4 filter name
 * @param: thin whether thin provisionning is used
 * @return 0 on success -1 otherwise (errno is set)
 */
int export_initialize(export_t * e, volume_t *volume, uint8_t layout, ROZOFS_BSIZE_E bsize,
        lv2_cache_t *lv2_cache, eid_t eid, const char *root, const char *name, const char *md5,
        uint64_t squota, uint64_t hquota, char * filter_name, uint8_t thin);

/** initialize an export.
 *
 * close file descriptors.
 *
 * @param export: pointer to the export
 */
void export_release(export_t * e);

/** stat an export.
 *
 *
 * @param export: pointer to the export
 * @param st: stat to fill in (see estat_t)
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_stat(export_t * e, ep_statfs_t * st);

/** lookup file in an export.
 *
 *
 * @param export: pointer to the export
 * @param pfid: fid of the parent of the searched file
 * @param name: pointer to the name of the searched file
 * @param[out] attrs: mattr_t to fill (child attributes used by upper level functions)
 * @param[out] pattrs: mattr_t to fill (parent attributes) 
 
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_lookup(export_t *e, fid_t pfid, char *name, mattr_t * attrs, mattr_t * pattrs);

/** get attributes of a managed file
 *
 * @param e: the export managing the file
 * @param fid: the id of the file
 * @param attrs: attributes to fill.
 * @param pattrs: parent attributes to fill.
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_getattr(export_t *e, fid_t fid, mattr_t * attrs, mattr_t * pattrs);

/** set attributes of a managed file
 *
 * @param e: the export managing the file
 * @param fid: the id of the file
 * @param attrs: attributes to set.
 * @param to_set: fields to set in attributes
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_setattr(export_t *e, fid_t fid, mattr_t * attrs, int to_set);

/** create a hard link
 *
 * @param e: the export managing the file
 * @param inode: the id of the file we want to be link on
 * @param newparent: parent od the new file (the link)
 * @param newname: the name of the new file
 * @param[out] attrs: mattr_t to fill (child attributes used by upper level functions)
 * @param[out] pattrs: mattr_t to fill (parent attributes)
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_link(export_t *e, fid_t inode, fid_t newparent, char *newname,
        mattr_t *attrs,mattr_t *pattrs);

/** create a new file
 *
 * @param e: the export managing the file
 * @param site_number: needed for geo-replication
 * @param pfid: the id of the parent
 * @param name: the name of this file.
 * @param uid: the user id
 * @param gid: the group id
 * @param mode: mode of this file
 * @param[out] attrs: mattr_t to fill (child attributes used by upper level functions)
 * @param[out] pattrs: mattr_t to fill (parent attributes)
  
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_mknod(export_t *e,uint32_t site_number,
                 fid_t pfid, char *name, uint32_t uid,
                 uint32_t gid, mode_t mode, mattr_t *attrs,mattr_t *pattrs);

/** create a new directory
 *
 * @param e: the export managing the file
 * @param pfid: the id of the parent
 * @param name: the name of this file.
 * @param uid: the user id
 * @param gid: the group id
 * @param mode: mode of this file
 * @param[out] attrs: mattr_t to fill (child attributes used by upper level functions)
 * @param[out] pattrs: mattr_t to fill (parent attributes)
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_mkdir(export_t *e, fid_t pfid, char *name, uint32_t uid,
        uint32_t gid, mode_t mode, mattr_t *attrs,mattr_t *pattrs);

/** remove a file
 *
 * @param e: the export managing the file
 * @param pfid: the id of the parent
 * @param name: the name of this file.
 * @param[out] fid: the fid of the removed file
 * @param[out] pattrs: mattr_t to fill (parent attributes)
 * 
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_unlink(export_t * e, fid_t pfid, char *name, fid_t fid, mattr_t * pattrs);

/*
int export_rm_bins(export_t * e);
 */

/** remove a new directory
 *
 * @param e: the export managing the file
 * @param pfid: the id of the parent
 * @param name: the name of directory to remove.
 * @param[out] fid: the fid of the removed file
 * @param[out] pattrs: mattr_t to fill (parent attributes)
 * 
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_rmdir(export_t *e, fid_t pfid, char *name, fid_t fid, mattr_t * pattrs);

/** create a symlink
 *
 * @param e: the export managing the file
 * @param link: target name
 * @param pfid: the id of the parent
 * @param name: the name of the file to link.
 * @param[out] attrs: mattr_t to fill (child attributes used by upper level functions)
 * @param[out] pattrs: mattr_t to fill (parent attributes)
 * @param uid: user identifier
 * @param gid: group identifier 
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_symlink(export_t * e, char *link, fid_t pfid, char *name,
        mattr_t * attrs,mattr_t *pattrs, 
	uint32_t uid, uint32_t gid) ;

/** read a symbolic link
 *
 * @param e: the export managing the file
 * @param fid: file id
 * @param link: link to fill
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_readlink(export_t *e, fid_t fid, char link[PATH_MAX]);

/** rename (move) a file
 *
 * @param e: the export managing the file
 * @param pfid: parent file id
 * @param name: file name
 * @param npfid: target parent file id
 * @param newname: target file name
 * @param fid: file id
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_rename(export_t * e, fid_t pfid, char *name, fid_t npfid,
        char *newname, fid_t fid,
	mattr_t * attrs);

/** Read to a regular file
 *
 * no effective read is done, just check if offset and len are consistent
 * with the file size, detect EOF, calculate index of the first block to read,
 * the nb. of bocks to read and update atime for this file
 *
 * @param e: the export managing the file
 * @param fid: id of the file to read
 * @param offset: offset to read from
 * @param len: length wanted
 * @param first_blk: id of the first block to read
 * @param nb_blks: Nb. of blocks to read
 *
 * @return: the readable length on success or -1 otherwise (errno is set)
 */
int64_t export_read(export_t * e, fid_t fid, uint64_t offset, uint32_t len, uint64_t * first_blk, uint32_t * nb_blks);

/** Get distributions for n blocks
 *
 * @param *e: the export managing the file
 * @param fid: id of the file to read
 * @param bid: first block address (from the start of the file)
 * @param n: number of blocks wanted
 * @param *d: pointer to distributions
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_read_block(export_t *e, fid_t fid, bid_t bid, uint32_t n, dist_t * d);

/** update the file size, mtime and ctime
 *
 * dist is the same for all blocks
 *
 * @param e: the export managing the file
 * @param fid: id of the file to read
 * @param bid: first block address (from the start of the file)
 * @param n: number of blocks
 * @param d: distribution to set
 * @param off: offset to write from
 * @param len: length written
 * @param site_number: siet number for geo-replication
 * @param geo_wr_start: write start offset
 * @param geo_wr_end: write end offset
 * @param[out] attrs: updated attributes of the file
 *
 * @return: the written length on success or -1 otherwise (errno is set)
 */
int64_t export_write_block(export_t *e, fid_t fid, uint64_t bid, uint32_t n,
                           dist_t d, uint64_t off, uint32_t len,
			   uint32_t site_number,uint64_t geo_wr_start,uint64_t geo_wr_end,
			   mattr_t *attrs);

/** read a directory
 *
 * @param e: the export managing the file
 * @param fid: the id of the directory
 * @param children: pointer to pointer where the first children we will stored
 * @param cookie: index mdirentries where we must begin to list the mdirentries
 * @param eof: pointer that indicates if we list all the entries or not
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_readdir(export_t * e, fid_t fid, uint64_t * cookie, child_t **children, uint8_t *eof);

int export_readdir2(export_t * e, fid_t fid, uint64_t * cookie,
        char *buf_readdir, uint8_t * eof);
	
/** retrieve an extended attribute value.
 *
 * @param e: the export managing the file or directory.
 * @param fid: the id of the file or directory.
 * @param name: the extended attribute name.
 * @param value: the value of this extended attribute.
 * @param size: the size of a buffer to hold the value associated
 *  with this extended attribute.
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */
ssize_t export_getxattr(export_t *e, fid_t fid, const char *name, void *value, size_t size);


/** retrieve an extended attribute value in raw mode.
 *
 * @param e: the export managing the file or directory.
 * @param fid: the id of the file or directory.
 * @param name: the extended attribute name.
 * @param value: the value of this extended attribute.
 * @param size: the size of a buffer to hold the value associated
 *  with this extended attribute.
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */
ssize_t export_getxattr_raw(export_t *e, fid_t fid, const char *name, void *value, size_t size,epgw_getxattr_raw_ret_t *ret);
/** set an extended attribute value for a file or directory.
 *
 * @param e: the export managing the file or directory.
 * @param fid: the id of the file or directory.
 * @param name: the extended attribute name.
 * @param value: the value of this extended attribute.
 * @param size: the size of a buffer to hold the value associated
 *  with this extended attribute.
 * @param flags: parameter can be used to refine the semantics of the operation.
 * 
 * @return: On success, zero is returned.  On failure, -1 is returned.
 */
int export_setxattr(export_t *e, fid_t fid, char *name, const void *value, size_t size, int flags, epgw_setxattr_symlink_t *symlink);

/** remove an extended attribute from a file or directory.
 *
 * @param e: the export managing the file or directory.
 * @param fid: the id of the file or directory.
 * @param name: the extended attribute name.
 * 
 * @return: On success, zero is returned.  On failure, -1 is returned.
 */
int export_removexattr(export_t *e, fid_t fid, char *name);

/** list extended attribute names from the lv2 regular file.
 *
 * @param e: the export managing the file or directory.
 * @param fid: the id of the file or directory.
 * @param list: list of extended attribute names associated with this file/dir.
 * @param size: the size of a buffer to hold the list of extended attributes.
 * 
 * @return: On success, the size of the extended attribute name list.
 * On failure, -1 is returned and errno is set appropriately.
 */
ssize_t export_listxattr(export_t *e, fid_t fid, void *list, size_t size);

/*
 *_______________________________________________________________________
 */

/**
*  Non blocing thread section
*/
/*
 *_______________________________________________________________________
 */
#define EXPORTNB_CNF_NO_BUF_CNT          1
#define EXPORTNB_CNF_NO_BUF_SZ           1
/**
*  North Interface
*/
#define EXPORTNB_CTX_CNT   32  /**< context for processing either a read or write request from rozofsmount and internal read req */
#define EXPORTNB_CTX_MIN_CNT 3 /**< minimum count to process a request from rozofsmount */
/**
* Buffer s associated with the reception of the load balancing group on north interface
*/
#define EXPORTNB_NORTH_LBG_BUF_RECV_CNT   EXPORTNB_CTX_CNT  /**< number of reception buffer to receive from rozofsmount */
#define EXPORTNB_NORTH_LBG_BUF_RECV_SZ    (1024*38)  /**< max user data payload is 38K       */
/**
* Storcli buffer mangement configuration
*  INTERNAL_READ are small buffer used when a write request requires too trigger read of first and/or last block
*/
#define EXPORTNB_NORTH_MOD_INTERNAL_READ_BUF_CNT   EXPORTNB_CTX_CNT  /**< expgw_north_small_buf_count  */
#define EXPORTNB_NORTH_MOD_INTERNAL_READ_BUF_SZ   2048  /**< expgw_north_small_buf_sz  */

#define EXPORTNB_NORTH_MOD_XMIT_BUF_CNT   EXPORTNB_CTX_CNT  /**< expgw_north_large_buf_count  */
#define EXPORTNB_NORTH_MOD_XMIT_BUF_SZ    EXPORTNB_NORTH_LBG_BUF_RECV_SZ  /**< expgw_north_large_buf_sz  */

#define EXPORTNB_SOUTH_TX_XMIT_BUF_CNT   (EXPORTNB_CTX_CNT)  /**< expgw_south_large_buf_count  */
#define EXPORTNB_SOUTH_TX_XMIT_BUF_SZ    (1024*8)                           /**< expgw_south_large_buf_sz  */

/**
* configuartion of the resource of the transaction module
*
*  concerning the buffers, only reception buffer are required 
*/

#define EXPORTNB_SOUTH_TX_CNT   (EXPORTNB_CTX_CNT)  /**< number of transactions towards storaged  */
#define EXPORTNB_SOUTH_TX_RECV_BUF_CNT   EXPORTNB_SOUTH_TX_CNT  /**< number of recption buffers  */
#define EXPORTNB_SOUTH_TX_RECV_BUF_SZ    EXPORTNB_SOUTH_TX_XMIT_BUF_SZ  /**< buffer size  */

#define EXPORTNB_NORTH_TX_BUF_CNT   0  /**< not needed for the case of storcli  */
#define EXPORTNB_NORTH_TX_BUF_SZ    0  /**< buffer size  */


#define EXPORTNB_MAX_RETRY   3  /**< max attempts for read or write on mstorage */

extern volatile int expgwc_non_blocking_thread_started; /**< clear on start, and asserted by non blocking thread when ready */

/*
** Set to 0 before starting the non blocking thread
** and then set to 1 when blocking thread has finished reading the configuration
*/
extern int volatile export_non_blocking_thread_can_process_messages;

typedef struct _exportd_start_conf_param_t
{
   uint16_t debug_port;   /**< port value to be used by rmonitor  */
   uint16_t slave;        /**< flag to indicate that the export is a slave */
   uint16_t instance;     /**< rozofsmount instance: needed when more than 1 rozofsmount run the same server and exports the same filesystem */
   char     *exportd_hostname;  /**< hostname of the exportd: needed by nobody!!! */
} exportd_start_conf_param_t;

extern exportd_start_conf_param_t  expgwc_non_blocking_conf;  /**< configuration of the non blocking side */

/*
 *_______________________________________________________________________
 */

/**
 *  This function is the entry point for setting rozofs in non-blocking mode

   @param args->ch: reference of the fuse channnel
   @param args->se: reference of the fuse session
   @param args->max_transactions: max number of transactions that can be handled in parallel
   
   @retval -1 on error
   @retval : no retval -> only on fatal error

 */
int expgwc_start_nb_blocking_th(void *args);

/**
*  Init of the channel module (interface between the main process and the non-blocking thread
*/
uint32_t expgwc_int_chan_moduleInit();

/*
 *_______________________________________________________________________
 */
/**
*  Init of the array that is used for building an exportd configuration message
  That array is allocated during the initialization of the exportd and might be
  released upon the termination of the exportd process
  
  @param none
  
  @retval 0 on success
  @retval -1 on error
 */
int exportd_init_storage_configuration_message();


/*
 *_______________________________________________________________________
 */
 /**
 *  That API is intended to be called by ep_conf_storage_1_svc() 
    prior to build the configuration message
    
    The goal is to clear the number of storages and to clear the
    number of sid per storage entry
    
    @param none
    retval none
*/
void exportd_reinit_storage_configuration_message();


/*
 *_______________________________________________________________________
 */
/**
*  Init of the array that is used for building an exportd configuration message
  That array is allocated during the initialization of the exportd and might be
  released upon the termination of the exportd process
  
  @param none
  
  @retval 0 on success
  @retval -1 on error
 */
int exportd_init_storage_configuration_message();

/*
**______________________________________________________________________________
*/
/**
*  Init of the data structure used for sending out export gateway configuration
   to rozofsmount.
   That API must be called during the inif of exportd
  
  @param exportd_hostname: VIP address of the exportd (extracted from the exportd configuration file)
  @retval none
*/
void ep_expgw_init_configuration_message(char *exportd_hostname);

/*
**______________________________________________________________________________
*/
/** Set a lock on a file
 *
 * @param e: the export managing the file or directory.
 * @param fid: the id of the file or directory.
 * @param lock: the lock to set/remove
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */
int export_set_file_lock(export_t *e, fid_t fid, ep_lock_t * lock_requested, ep_lock_t * blocking_lock, ep_client_info_t * info) ;
/*
**______________________________________________________________________________
*/
/** reset all locks from a client
 *
 * @param e: the export managing the file or directory.
 * @param lock: the identifier of the client whose locks are to remove
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */
int export_clear_client_file_lock(export_t *e, ep_lock_t * lock_requested, ep_client_info_t * info);
/*
**______________________________________________________________________________
*/
/** reset all locks from an owner
 *
 * @param e: the export managing the file or directory.
 * @param lock: the identifier of the client whose locks are to remove
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */
int export_clear_owner_file_lock(export_t *e, fid_t fid, ep_lock_t * lock_requested);
/*
**______________________________________________________________________________
*/
/** Get a lock on a file
 *
 * @param e: the export managing the file or directory.
 * @param fid: the id of the file or directory.
 * @param lock: the lock to set/remove
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */
int export_get_file_lock(export_t *e, fid_t fid, ep_lock_t * lock_requested, ep_lock_t * blocking_lock) ;
/*
**______________________________________________________________________________
*/
/** Get a poll event from a client
 *
 * @param e: the export managing the file or directory.
 * @param lock: the lock to set/remove
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */
int export_poll_file_lock(export_t *e, ep_lock_t * lock_requested, ep_client_info_t * info) ;
/*
**______________________________________________________________________________
*/
extern int export_instance_id;    /**< instance id of the export  : 0 is the master   */
extern int export_master;         /**< assert to 1 for export Master                  */
/*
**______________________________________________________________________________
*/
/**
*  exportd set instance value and role

   @param instance_id
   
   @retval none
*/
static inline void exportd_set_export_instance_and_role(int instance_id,int role)
{
   export_instance_id = instance_id;
   export_master = role;
}
/*
**______________________________________________________________________________
*/
/**
*  get the instance id of the export

  retval: instance of the export
*/
static inline int exportd_get_export_instance()
{
  return export_instance_id;
}
/*
**______________________________________________________________________________
*/
/**
*  get role of the export
*/
static inline int exportd_is_master()
{
  return (export_master==0?0:1);
}
/*
**______________________________________________________________________________
*/
/**
*  check if the eid matches the instance

   @param eid: export identifier
   @retval 1 : if it matches
   @retval 0 otherwise
*/
static inline int exportd_is_eid_match_with_instance(int eid)
{

    int idx = (eid-1)%EXPORT_SLICE_PROCESS_NB +1;
    if (idx == export_instance_id) return 1;
    return 0;


}   

/*
**______________________________________________________________________________
*/
/**
*  check if the eid matches the instance

   @param eid: export identifier
   @retval instance_id of the exportd
*/
static inline int exportd_get_instance_id_from_eid(int eid)
{

    int idx = (eid-1)%EXPORT_SLICE_PROCESS_NB +1;
    return idx;
}   
/*
**_______________________________________________________________________________
*/
/**
* The purpose of that service is to check the content of the tracking file and
  to truncate or delete the tracking file when is possible for releasing blocks
  to the filesystem

   e: pointer to the export
   type : type of the tracking files
   
*/   
int exp_trck_inode_release_poll(export_t * e,int type);

#define TRK_TH_INODE_DEL_STATS   0 
#define TRK_TH_INODE_TRUNC_STATS 1
#define TRK_TH_INODE_MAX_STATS (TRK_TH_INODE_TRUNC_STATS+1) 
typedef struct _exp_trk_th_stats_t
{
  uint64_t counter[TRK_TH_INODE_MAX_STATS];
} exp_trk_th_stats_t;

extern exp_trk_th_stats_t  *exp_trk_th_stats_p;
/*
**_______________________________________________________________________________
*/
/**
*  open the parent directory

   @param e : pointer to the export structure
   @param parent : fid of the parent directory
   
   @retval > 0 : fd of the directory
   @retval < 0 error
*/
int export_open_parent_directory(export_t *e,fid_t parent);
/*
**__________________________________________________________________
*/
/** Get pointer to the export statistics in memory
 *
 * @param eid: the export to get statistics from
 *
 * @return the pointer to the statitics of NULL 
 */
export_fstat_t * export_fstat_get_stat(uint16_t eid) ;
/*
** Get the export reference associated with the host
   
   @param none
   
   @retval export host value
*/
extern int rozofs_export_host_id;  /**< reference between 0..7  */

static inline uint8_t rozofs_get_export_host_id()
{
  return rozofs_export_host_id;
}

/*
**__________________________________________________________________
*/
/**
*  Init of the attribute writeback thread

   @param none
   
   @retval 0 on success
   @retval -1 on error (see errno for details
*/
int export_wr_attr_th_init();
/*
 *_______________________________________________________________________
 */
/**
*   Get the configuration file of the exportd

    @param none
    
    @retval: pointer to the exportd configuration file (full path)
*/
char *export_get_config_file_path();
/*
**__________________________________________________________________
*/
/** Allocate a rmfentry_t structure to chain the deletion job
 *
 * @param trash_entry     The disk context of removed file
 * 
 * @return: the address of the allocated structure
 */
rmfentry_t * export_alloc_rmentry(rmfentry_disk_t * trash_entry);
/*
**__________________________________________________________________

     Control meta-data SSD free size     
     
**__________________________________________________________________
*/
#define          ONE_SECOND_TICK_CREDIT 1000000LL

static inline int export_metadata_check_device(export_t *e) {
  struct statfs      buf;
  meta_resources_t * pRes = &e->space_left;
  
  /*
  ** Re avaluate the SSD left size
  */
  if (statfs(e->root,&buf) < 0) {
    pRes->statfs_error++; // Count the statfs errors
    return pRes->full;
  }
  
  /*
  ** A priori the device is not full
  */
  pRes->full   = 0;  

  pRes->inodes.free  = buf.f_ffree;
  pRes->inodes.total = buf.f_files;
  
  pRes->sizeMB.free  = (buf.f_bavail*buf.f_bsize)/(1024*1024);
  pRes->sizeMB.total = (buf.f_blocks*buf.f_bsize)/(1024*1024);  
    
  /*
  ** Which resource is the smaller now
  */
  if (pRes->inodes.free < common_config.min_metadata_inodes) {
    pRes->full = 1; 
    return 1;
  }  
  if (pRes->sizeMB.free < common_config.min_metadata_MB) {
    pRes->full = 1; 
    return 1;
  }  
 
  return 0;
}   
static inline int export_metadata_device_full(export_t *e, uint64_t microsec) {
  meta_resources_t * pRes = &e->space_left;

  /*
  ** Is it time to re-evaluate the SSD free size
  */
  if (microsec < pRes->next_microsec) {
    return pRes->full;
  }
  
  if (export_metadata_check_device(e)) {
    pRes->full_counter++;
    return 1;
  }  
    
  /*
  ** 8 times the limit. We are good for 32 secondes
  */
  if ((pRes->inodes.free  > (8*common_config.min_metadata_inodes))
  &&   (pRes->sizeMB.free > (8*common_config.min_metadata_MB))) {
    pRes->next_microsec = microsec + (32*ONE_SECOND_TICK_CREDIT);
    return 0;
  }
  
  /*
  ** 2 times the limit. We are good 8 seconds 
  */  
  if ((pRes->inodes.free  > (2*common_config.min_metadata_inodes))
  &&   (pRes->sizeMB.free > (2*common_config.min_metadata_MB))) {
    pRes->next_microsec = microsec + (ONE_SECOND_TICK_CREDIT*8);
    return 0;
  } 
  
  /*
  ** We are good for 1/2 sec
  */   
  pRes->next_microsec = microsec + (ONE_SECOND_TICK_CREDIT/2);   
  return 0;
}   

/**
 * hashing function used to find lv2 entry in the cache
 */
uint32_t lv2_hash(void *key);
int lv2_cmp(void *k1, void *k2);


/*
**__________________________________________________________________
*/
/**

 check for Re-write the directory attributes if it is time to do so when the entry is removed from lv2 cache
 
 @param dir: pointer to the directory i-node (cache structure)


*/
void export_dir_check_sync_write_on_lru(lv2_entry_t *dir);

#endif
