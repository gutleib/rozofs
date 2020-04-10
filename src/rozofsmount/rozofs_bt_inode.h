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
#ifndef _ROZOFS_BT_INODE_H
#define _ROZOFS_BT_INODE_H
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <fcntl.h> 
#include <pthread.h> 
#include <sys/un.h>             
#include <errno.h>  
#include <rozofs/rozofs.h>
#include <rozofs/common/mattr.h>
#include <rozofs/core/rozofs_queue.h>
#include <rozofs/core/rozofs_queue_pri.h>
#include <rozofs/common/htable.h>
#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/rpc/expbt_protocol.h>
#define ROZOFS_BT_INODE_MAX_THREADS 4

extern struct  sockaddr_un rozofs_bt_south_socket_name;
extern pthread_rwlock_t rozofs_bt_buf_pool_lock;


extern int rozofs_bt_debug;

#define FDL_INFO(fmt, ...) if (rozofs_bt_debug) info( fmt, ##__VA_ARGS__)
#define ROZOFS_BT_DEADLINE_SEC  120 /**< duration of a file tracking cache entry validity */

#define PRIO_CMDQ 3
#define PRIO_CMD 2
#define PRIO_RSP 1
#define MAX_PRIO 4

typedef union
{
   uint64_t u64;
   struct
   {
     uint64_t  eid:10;        /**< export identifier */     
     uint64_t  usr_id:8;     /**< usr defined value-> for exportd;it is the slice   */
     uint64_t  file_id:40;    /**< bitmap file index within the slice                */
     uint64_t  key:6;        /**< inode type           */
   } s;
} rozofs_bt_track_key_t;

typedef struct _rozofs_bt_tracking_cache_t
{
    hash_entry_t he;
    rozofs_bt_track_key_t key;  /**< use as a key    */
    list_t   list;
    list_t   pending_list;    /**< queue of the clients that are waiting for the tracking file */
    int      pending;         /**< assert to 1 when the system is waiting for the content of the tracking file */
    uint32_t length;  
    int      errcode;         /**< errno value: 0 when all is fine    */
    uint32_t lock_count;    /**< number of user on the tracking file  */
    uint32_t change_count;  /**< number of time that the tracking file has been updated */
    uint64_t mtime;          /**< last mtime of the file               */
    uint64_t cache_time;    /**< for future usage                     */
    uint64_t timestamp;
    uint64_t deadline_sec;  /**< dead line validity of the cache entry in seconds : updated on each lookup access  */
    uint64_t access_count;  /**< number of time that entry has been accessed    */
    uint64_t reload_count;  /**< number of time that entry has been reloaded    */
    
      
} rozofs_bt_tracking_cache_t;

#define INODE_BT_HSIZE (1024)

typedef struct _expbt_msgint_hdr_t 
{
  uint32_t xid;    /**< message identifier                         */
  uint32_t opcode; /**< opcode of the message                      */
  void     *user_ctx;      /**< opaque data                        */
  int       queue_prio;  /**< -1 if no priority                    */
  void     *rozofs_queue;  /**< reference of the rozofs queue used for the response */
} expbt_msgint_hdr_t;



typedef struct _expbt_msgint_req_t 
{
   union 
   {
      struct 
      {
        expbt_trk_read_req_t read_trk;
	rozofs_bt_tracking_cache_t *image_p;     /**< pointer to the array where data must be copied */
	uint64_t  inode;                         /**< used as a context for dentry management        */
      
      } read_trk_rq;

      struct 
      {
        expbt_dirent_load_req_t req;      
      } dirent_rq;
      struct 
      {
        expbt_trk_check_req_t cmd;
	uint32_t              user_data[EXP_BT_TRK_CHECK_MAX_CMD];  /**< opaque data: for the file tracking RCU, it contains the cache index */
	expbt_trk_check_req_entry_t entry[EXP_BT_TRK_CHECK_MAX_CMD];
      
      } check_rq;
    };
} expbt_msgint_req_t;


typedef struct _expbt_msgint_rsp_t 
{
   union 
   {
      struct
      {
        expbt_trk_read_rsp_t  rsp;
      } read_trk_rsp;
      struct
      {
        expbt_dirent_load_rsp_t  rsp;
      } dirent_rsp;
      struct
      {
        expbt_trk_main_rsp_t  global_rsp;
        expbt_trk_check_rsp_t rsp;
	expbt_trk_check_rsp_entry_t entry[EXP_BT_TRK_CHECK_MAX_CMD];
      } check_rsp;
   };
} expbt_msgint_rsp_t;


typedef struct _expbt_msgint_full_t
{
   expbt_msgint_hdr_t hdr;
   expbt_msgint_req_t req;
   expbt_msgint_rsp_t rsp;
} expbt_msgint_full_t;


/*
** Global data
*/
extern htable_t htable_bt_inode;
extern uint64_t rozofs_tracking_file_count;

extern uint64_t hash_tracking_collisions_count;
extern uint64_t hash_tracking_max_collisions;
extern uint64_t hash_tracking_cur_collisions;
extern char *rozofs_bt_export_path_p;


extern rozofs_queue_prio_t rozofs_bt_ino_queue_req;
extern rozofs_queue_t rozofs_bt_ino_queue_rsp;

/**
**_______________________________________________________________________________________
*/    
rozofs_bt_track_key_t rozofs_bt_inode_get_filekey_from_inode(uint64_t inode);


/*
**_________________________________________________________________
*/
/**
*    Local file tracking read: only when rozofsmount and export resides on the same node

     @param export_path: export pathname
     @param eid: export identifier
     @param usr_id: slice identifier
     @param file_id: index of the file
     
     @retval 0 on success
     @retval < 0 on error
*/
int rozofs_bt_load_inode_tracking_file_local(char *export_path,int eid,uint16_t usr_id,uint64_t file_id,void *working_p);
/*
**__________________________________________________________________
*/
/**
*  Insert a tracking file in the tracking file cache

   @param trk_p: pointer to the tracking file context
   
   @retval none
*/
void rozofs_bt_put_tracking(rozofs_bt_tracking_cache_t * trk_p);

/*
**__________________________________________________________________
*/
/**
*  delete a tracking file cache entry  in the tracking file cache

   @param trk_p: pointer to the tracking file context
   
   @retval none
*/

 void rozofs_bt_del_tracking(rozofs_bt_tracking_cache_t * trk_p);
 /*
**__________________________________________________________________
*/
/**
     Get the pointer to a tracking file
     
     @param key: key of the tracking file (eid,usr_id & file_id)
     
     @retval NULL: not found
     @retval != NULL pointer to the tracking file cache entry
*/
rozofs_bt_tracking_cache_t *rozofs_bt_get_tracking(rozofs_bt_track_key_t key);
/*
**__________________________________________________________________
*/
/**
*   Init of the tracking file cache used by the rozofs batch

   @param queue_depth: command queue depth
   @param eid: export id of associated with the mountpoint

    @retval 0 on success
    
    @retval -1 on error
*/
int rozofs_bt_inode_init(int queue_depth,int eid);

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

/**
**_______________________________________________________________________________________
*/
/**
   Internal inode lookup: use by batch read
   
   @param inode: lower part of the inode
   @param tracking_ret_p: pointer to the tracking entry (might be NULL)
   
   @retval <> NULL: pointer to the inode context, tracking cache entry available if provided
   @retval NULL: not found
*/
ext_mattr_t *rozofs_bt_lookup_inode_internal_with_tracking_entry(uint64_t inode,rozofs_bt_tracking_cache_t **tracking_ret_p);


extern struct  sockaddr_un rozofs_bt_cli_reader_socket_name[];

void show_trkrd_profiler(char * argv[], uint32_t tcpRef, void *bufRef);

/*
**_________________________________________________________________________________________________
*/
/**
 Create the RCU inode thread that is used to update tracking file in cache and to delet tracking files that are not used anymore
 
 @retval 0 on success
 @retval -1 on error

*/ 
int rozofs_bt_trk_cli_reader_thread_create(int nb_threads);

/*
**______________________________________________________________________________
*/
/**
*  Add an eid entry 

  @param exportd_id : reference of the exportd that manages the eid
  @param eid: eid within the exportd
  @param hostname: hostname of the exportd
  @param port:   port of the exportd
  @param nb_gateways : number of gateways
  @param gateway_rank : rank of the current export gateway
  
  @retval  <>0 success: reference of the load balancing group
  @retval -1 on error (see errno for details)
  
*/

int rozofs_bt_create_client_lbg(void *module_p);


/**
**_______________________________________________________________________________________
*/
/**
*   the sock_id is an AF_UNIX socket used in non connected node (thread_ctx_p->sendSocket)
*/
int rozofs_bt_trigger_read_trk_file_for_io_from_main_thread(uint64_t inode,void *bt_ioctx_p);


/*
 *_______________________________________________________________________
 */
/**
*  Init of the dirent file loader for a client

   @param  nb_buffer: number of buffer allocated for command submit
   @param ipaddr: IP@ of the client
   @param local_export_path: local root path on the client when dirent file will be stored
   @param eid: exportd identifier of the client

   @retval 0 on success
   @retval -1 on error
*/   
int rozofs_bt_dirent_init(int nb_buffer,char *host /*,char *local_export_path*/,int eid);
/*
 *_______________________________________________________________________
 */
/*
** tracking file lock and unlock
*/

static inline void tracking_entry_lock(rozofs_bt_tracking_cache_t *tracking_p,char *file,int line)
{
   if (tracking_p != NULL) 
   {
     info("FDL %s:%d : tracking_entry_lock :%u",file,line,tracking_p->lock_count);
     __atomic_fetch_add(&tracking_p->lock_count,1,__ATOMIC_SEQ_CST);
   }
}


static inline void tracking_entry_unlock(rozofs_bt_tracking_cache_t *tracking_p,char *file,int line)
{
   if (tracking_p != NULL) {
       info("FDL %s:%d : tracking_entry_unlock :%u",file,line,tracking_p->lock_count);
     __atomic_fetch_sub(&tracking_p->lock_count,1,__ATOMIC_SEQ_CST);
   }
}

#define ROZOFS_BT_LOCK_TRACKING_ENTRY(p) tracking_entry_lock(p,__FILE__,__LINE__);
#define ROZOFS_BT_UNLOCK_TRACKING_ENTRY(p) tracking_entry_unlock(p,__FILE__,__LINE__);

void show_trkrd_cache(char * argv[], uint32_t tcpRef, void *bufRef);


/**
**_______________________________________________________________________________________
*/
/**
*   Get the pointer to an inode from the tracking cache file entry

    @param tracking_p: pointer to the tracking cache entry
    @param inode: inode to search
    
    @retval <> NULL if found
    @retval NULL if not found
*/
ext_mattr_t *rozofs_bt_get_inode_ptr_from_image(rozofs_bt_tracking_cache_t *tracking_p,uint64_t inode);
#endif
