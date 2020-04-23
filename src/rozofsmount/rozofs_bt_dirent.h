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
#ifndef _ROZOFS_BT_DIRENT_H
#define _ROZOFS_BT_DIRENT_H
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>          
#include <errno.h>  
#include <rozofs/rozofs.h>
#include <rozofs/common/htable.h>
#include <rozofs/common/list.h>
#include <rozofs/core/ruc_buffer_debug.h>
#include "rozofs_bt_inode.h"

#define ROZOFS_BT_DIRENT_GUARD_DELAY 60 /**< guard delay before reloading a dirent file with the update time of the directory does not match the mtime of the btmap entry of dirent */

typedef struct _rozofs_bt_dirent_stats {
   uint64_t lookup_inode_hit[2];  /**< number of attempts to get the inode   */
   uint64_t lookup_inode_miss[2];  /**< number of attempts to get the inode   */
   uint64_t lookup_bitmap_miss;  /**< number of attempts to get the inode   */
   uint64_t lookup_bitmap_hit;  /**< number of attempts to get the inode   */
   uint64_t lookup_dir_bad_mtime;  /**< number of attempts to get the inode   */

   uint64_t get_inode_attempts[2];  /**< number of attempts to get the inode   */
   uint64_t get_inode_duplicated[2]; /**< number of time there was a pending request for a given inode */
   uint64_t get_tracking_file_attempts[2];
   uint64_t get_tracking_file_success[2];
   uint64_t get_tracking_file_failure[2];
   uint64_t check_dirent_root_valid;
   uint64_t check_dirent_root_valid_ok;
   uint64_t check_dirent_root_valid_nok;
   uint64_t check_dirent_too_early;
   uint64_t get_dirent_file_attempts;
   uint64_t get_dirent_file_success;
   uint64_t get_dirent_file_failure;
} rozofs_bt_dirent_stats;


#define ROZOFS_BT_DIRENT_STATS_INO(probe,ino) \
{ \
   rozofs_inode_t rozofs_inode; \
   rozofs_inode.fid[1] = ino; \
   dirent_stats_p->probe[(rozofs_inode.s.key == ROZOFS_DIR)?0:1]++; \
} 

#define ROZOFS_BT_DIRENT_STATS_DIR(probe) \
   dirent_stats_p->probe++; 


#define ROOT_BTMAP_HSIZE (8192*4)
/**
* internal structure used for bitmap root_idx: only for directory
*/
#define DIRENT_MAX_IDX_FOR_EXPORT 4096
#define DIRENT_MASK_FOR_EXPORT (4096-1)
#define DIRENT_FILE_BYTE_BITMAP_SZ (DIRENT_MAX_IDX_FOR_EXPORT/8)
typedef struct _dirent_dir_root_idx_bitmap_t
{
   int dirty; /**< assert to one if the bitmap must be re-written on disk */
   char bitmap[DIRENT_FILE_BYTE_BITMAP_SZ];
} dirent_dir_root_idx_bitmap_t;



typedef struct _dentry_btmap_t {
    hash_entry_t he;
    uint64_t inode;  /**< inode of the directory   */
    uint64_t mtime;  /**< mtime of the directory   */
    uint64_t     deadline_timestamp; /**< delay for garbage collector */
    uint64_t     lru_timestamp;     /**< last time the entry has been move to the front of the lru */
    list_t   list;  /**< to create the linked list of the entries */
    list_t   lru;  /**< to create the lru linked list of the entries */
    dirent_dir_root_idx_bitmap_t btmap;  /**< dirent bitmap */
} dentry_btmap_t ;  


extern int rozofs_bt_dirent_eid;
extern rozofs_bt_dirent_stats *dirent_stats_p;
extern int rozofs_bt_dirent_load_debug;
/*
**________________________________________________________________
*/
/**
*   Init of the hash table used for caching the dirent root content associated with the directory

    @param eid: export identifier
    @param export_root_path: export root path on the client side
    
    @retval 0 on success
    @retval -1 on error
*/
int rozofs_bt_dirent_hash_init(int eid, char *export_root_path);

/*
**________________________________________________________________
*/
/**
  Load in the cache the content of the bitmap file of the directory
  
  @param inode: directory in node in RozoFS format
  @param mtime of the directory
  
  @retval 0 on success
  @retval -1 on error (see errno for details
*/
int rozofs_bt_load_dirent_root_bitmap_file(uint64_t inode,uint64_t mtime);


/**
**_______________________________________________________________________________________
*/
/**
  Flush any entry that has expired within the root btmap garbage collector
  
  @param none
  
  @retval number of deleted context
*/

uint64_t rozofs_bt_root_btmap_thread_garbage_collector_deadline_delay_sec;
uint64_t rozofs_bt_root_btmap_garbage_collector_count;
uint64_t rozofs_root_bmap_max_ientries;
int rozofs_bt_root_btmap_flush_garbage_collector();

/*
**________________________________________________________________
*/
/*
** attempt to get the inode value from the local dirent
  @param fid_parent: fid of the parent directory
  @param name: name to search
  @param fid: returned fid
  
*/
int rozofs_bt_get_mdirentry(fid_t fid_parent, char * name, fid_t fid);
/**
**_______________________________________________________________________________________
*/
/**
*  attempt to get the inode attribute

   @param inode: inode value
   @param tracking_ret_p: pointer to the tracking entry (might be NULL)
   @param dirent_valid: pointer to an array where the service indicates if dirent are valid (0: invalid, 1: valid) (for directory only)
      
   @retval <> NULL: the inode attributes are valid, tracking cache entry available if provided
   @retval NULL: inode attributes not available (see errno for detail
   
   EAGAIN: the loading of the tracking file (and dirent file) are in progress
   ENOTSUP: not supported
   ENOENT: the entry does not exist
   other: 
*/
ext_mattr_t *rozofs_bt_load_dirent_from_main_thread(uint64_t inode,rozofs_bt_tracking_cache_t **tracking_ret_p,int *dirent_valid);

/**
**_______________________________________________________________________________________
*/
/**
   Internal inode lookup: use by batch read
   
   @param inode: lower part of the inode
   
   @retval <> NULL: pointer to the inode context
   @retval NULL: not found
*/
ext_mattr_t *rozofs_bt_lookup_inode_internal(uint64_t inode);

/*
**________________________________________________________________
*/
/**
   Get the context of a root bitmap entry from rozofs fid (128 bits)
   
   @param fid: fid of the directory
   
   @retval <> NULL : pointer to the root bitmap entry
   @retval NULL: not found
*/
dentry_btmap_t *get_btmap_entry_by_fid_external(fid_t fid);

/*
**________________________________________________________________
*/
static inline uint64_t rozofs_bt_inode_normalize(uint64_t ino)
{
    rozofs_inode_t fake_id;
    
    if ((ino == 0x2800000000000001) || (ino == 1)){
      return  0x1800000000000000 ;
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
    return fake_id.fid[1];
}

/*
**_______________________________________________________________________________
   DATA STRUCTURE FOR READDIR & GET_DENTRY
**_______________________________________________________________________________
*/   

typedef struct _bt_readdir_arg_t
{
   uint32_t eid; /**<export identifier       */
   fid_t    fid; /**< fid of the directory   */
   uint64_t cookie; /**< dirent cookie ; opaque for the client */
   char     *db_buffer; /**< pointer to the buffer that will contains the result of the readdir */
} bt_readdir_arg_t;


typedef struct _bt_dirlist2_t {
	uint8_t eof;  /**< assert to 1 if eof has been encountered */
	uint64_t cookie; /**< return ccokie for the next call */
        uint32_t value_len;  /**< length of the payload that contains name & inode & type */
} bt_dirlist2_t;

typedef struct _bt_readdir2_ret
{
   int status;  /**< status of the operation 0 if OK, -1 if error */
   union
   {
     int errcode;  /**< errcode when status is -1 */
     bt_dirlist2_t reply;
   } s;
} bt_readdir2_ret;  


typedef struct _bt_lookup_arg_t
{
    uint32_t eid;         /**< export identifier            */
    fid_t    parent_fid;  /**< fid of the parent directory  */
    char     *name;       /**< name to search for           */
} bt_lookup_arg_t;


typedef struct _bt_lookup_ret_t
{
   fid_t parent_fid;   /**< fid of the parent */
   int status;  /**< status of the operation 0 if OK, -1 if error */
   union
   {
     int errcode;  /**< errcode when status is -1 */
     fid_t child_fid;  /**< inode value   */
   } s;
} bt_lookup_ret_t;

/*
**____________________________________________________________________
*/
/*
  uint32_t xid;         message identifier   
  uint32_t opcode;      opcode of the message 
  void     *user_ctx;  opaque data: pointer to the fuse context   
  int       queue_prio; not used                    
  void     *rozofs_queue; : not used 
} expbt_msgint_hdr_t;
*/


typedef struct _bt_dirent_msg_t
{
  expbt_msgint_hdr_t  hdr;  /*<< header of the message  see above */
  union 
  {  
    bt_readdir_arg_t readdir_rq;
    bt_readdir2_ret  readdir_rsp;
    bt_lookup_arg_t  lookup_rq;
    bt_lookup_ret_t  lookup_rsp;
  } s;
} bt_dirent_msg_t;

typedef enum {
  ROZOFS_BT_DIRENT_READDIR = 0,
  ROZOFS_BT_DIRENT_GET_DENTRY,      
  ROZOFS_BT_DIRENT_MAX
} rozofs_bt_dirent_opcode_e;

/*
**____________________________________________________________________
*/
/**
*  attempt to perform a local readdir if the dirent files of the directory are available

   @param fid: fid of the directory
   @param cookie: readdir cookie (opaque to client)
   @param fuse_ctx_p : pointer to the fuse context to handle the readdir request of the application
      
   @retval 0 on success
   @retval -1 on error (see errno for details)
*/
int rozofs_bt_readdir_req_from_main_thread(fid_t fid,uint64_t cookie,void *fuse_ctx_p);

/*
**____________________________________________________________________
*/
/**
*  callback for readdir on client

   the payload of the ruc_buffer contains the response to the readdir.
   see bt_readdir2_ret strcuture  for details.
   

   @param ruc_buffer: pointer to the ruc buffer that contains the response

*/
void rozofs_bt_readdir2_cbk(void *ruc_buffer);

/*__________________________________________________________________________
** Initialize the dirent thread interface (used for READDIR & LOOKUP)
 
  @param hostname    hostname (for tests)
  @param instance_id : expbt instance
  @param nb_threads  Number of threads that can process the read or write requests
*
*  @retval 0 on success -1 in case of error
*/
int rozofs_bt_dirent_thread_intf_create(char * hostname, int instance_id, int nb_threads);


void rozofs_bt_show_dirent_cache(char * argv[], uint32_t tcpRef, void *bufRef);

/*
 **______________________________________________________________________________
 */

/**
 * Attempt to release the cache entry associated with the root dirent file
 *
 * @param dirfd: file descriptor of the parent directory
 * @param *name: pointer to the name of the mdirentry to put
 * @param fid_parent: unique identifier of the parent directory
 *  @param fid: unique identifier of the mdirentry to put
 * @param type: type of the mdirentry to put
 *
 * @retval  0 -> success
 * @retval -1 -> the entry does not exist
 */

int bt_dirent_remove_root_entry_from_cache(fid_t fid, int root_idx);

/*
**____________________________________________________________________
*/
/**
*  attempt to perform a local readdir if the dirent files of the directory are available

   @param  eid: export identifier
   @param  parent_fid: fid of the parent directory
   @param  fuse_ctx_p: fuse context
         
   @retval 0 on success
   @retval -1 on error (see errno for details)
*/
int rozofs_bt_lookup_req_from_main_thread(uint32_t eid,fid_t parent_fid,void *fuse_ctx_p);

#if 0
/*
**__________________________________________________________________________________________________
*/
/*
**
   Get the slave inodes if any: only fro regular files

   @param tracking_ret_p: pointer to the tracking file cache entry
   @param rozofs_inode_p: pointer to the master rozofs inode
   @param ie: inode ientry pointer
   
   @retval 0 on success
   @retval < 0 on error (see errno for details)

*/
int rozofs_bt_get_slave_inode(rozofs_bt_tracking_cache_t *tracking_ret_p,rozofs_inode_t *rozofs_inode_p,ientry_t *ie);
#endif


extern uint64_t rozofs_bt_lookup_local_attempt;
extern uint64_t rozofs_bt_lookup_local_reject_from_main;
extern uint64_t rozofs_bt_lookup_local_reject_from_dirent_thread;
#endif
