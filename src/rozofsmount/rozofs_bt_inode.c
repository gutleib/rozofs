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
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <fcntl.h> 
#include <sys/un.h>             
#include <errno.h> 
#include "rozofs_bt_inode.h"
#include <sys/stat.h>
#include <unistd.h>
#include <rozofs/rozofs.h>
#include "rozofs_bt_api.h"
#include "rozofs_bt_proto.h"
#include <rozofs/common/log.h>
#include <rozofs/core/ruc_buffer_api.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/ruc_buffer_debug.h>
#include <rozofs/common/export_track.h>
#include <rozofs/common/mattr.h>
#include <rozofs/core/rozofs_queue.h>
#include <rozofs/core/rozofs_queue_pri.h>
#include <rozofs/core/ruc_sockCtl_api_th.h>
#include <rozofs/core/ruc_timer_api_th.h>
#include <rozofs/core/af_inet_stream_api.h>
#include <rozofs/core/rozofs_tx_common.h>
#include <rozofs/core/rozofs_tx_api.h>
#include "rozofs_bt_dirent.h"

#define xmalloc malloc
#define xfree free 


typedef struct _rozofs_bt_lookup_stats_t {
    uint64_t lookup_attempts;
    uint64_t lookup_hit;    
    uint64_t lookup_miss;    
    uint64_t lookup_time;
} rozofs_bt_lookup_stats_t;

/*
** Global data
*/
htable_t htable_bt_inode;
uint64_t rozofs_tracking_file_count;
uint64_t hash_tracking_collisions_count;
uint64_t hash_tracking_max_collisions;
uint64_t hash_tracking_cur_collisions;
list_t rozofs_bt_tracking_list;
int rozofs_bt_eid;  /**< reference of the export */
char *rozofs_bt_export_path_p = NULL;
static pthread_rwlock_t rozofs_bt_inode_lock;
/**
*  Thread table
*/
rozofs_bt_thread_ctx_t rozofs_bt_inode_thread_ctx_tb[ROZOFS_BT_INODE_MAX_THREADS];
rozofs_bt_thread_ctx_t rozofs_bt_rcu_thread_ctx[ROZOFS_BT_INODE_MAX_THREADS];
rozofs_queue_prio_t rozofs_bt_ino_queue_req;
rozofs_queue_t rozofs_bt_ino_queue_rsp;
#define ROZOFS_BT_MAX_TRK_CACHE 1024
rozofs_bt_tracking_cache_t  **rozofs_bt_inode_rcu_tb_p = NULL;
uint64_t rozofs_bt_rcu_number_of_tracking_files = 0;

uint64_t rozobs_bt_inode_track_cache_entry_delay_sec = ROZOFS_BT_DEADLINE_SEC; 



rozofs_bt_tracking_cache_t  **rozofs_bt_inode_debug_tb_p = NULL;
rozofs_bt_lookup_stats_t rozofs_bt_lookup_stats;




/**
**_______________________________________________________________________________________
*/

typedef struct _rozofs_bt_internal_attr_mem_t
{
  uint32_t         key; /**< 0 not significant */
  rozofs_memreg_t *mem_p;     

} rozofs_bt_internal_attr_mem_t;

typedef struct _rozofs_bt_internal_attr_file_p
{
   uint64_t key;
   rozofs_bt_tracking_cache_t *tracking_p;

} rozofs_bt_internal_attr_file_p;



/**
*  Working context structure
*/
struct _rozofs_bt_attr_ctx_t;
typedef void (*rozofs_bt_pfc_callback_t)(struct _rozofs_bt_attr_ctx_t * attr_ctx_p,int errcode);
typedef struct _rozofs_bt_attr_ctx_t 
{
   expbt_msgint_full_t msg;   /**< message array when the module needs to trigger the reading of the tracking file */
   rozofs_bt_tracking_cache_t *image_p;  /**< pointer to the allocated cache entry*/
   list_t pending_list;       /**< queue the working context if there is already a pending reading for the tracking file */
   void *recv_buf;    /**< ruc buffer that contains the command */
   int   socket_id;   /**< reference of the socket to use for sending back the response */
   int nb_commands;
   rozofs_bt_pfc_callback_t rsp_trk_read_cbk;  /**< callback function upron receiving the response on a tracking file read */
   rozofs_bt_internal_attr_mem_t mem[ROZOFS_MAX_BATCH_CMD];
   rozofs_bt_internal_attr_file_p file[ROZOFS_MAX_BATCH_CMD];
   rozofs_bt_rsp_buf response;
} rozofs_bt_attr_ctx_t;



/**
**_______________________________________________________________________________________
*/


void show_trkrd_cache(char * argv[], uint32_t tcpRef, void *bufRef) 
{

  char *pChar = uma_dbg_get_buffer();
  list_t             * p;
  list_t             * n;
  int i = 0;
  rozofs_bt_tracking_cache_t *image_p;
  char date[64];
  char date2[64];
  struct tm tm;
  uint64_t nb_tracking_files = 0;
  uint64_t cur_time_us;
//  uint64_t timestamp_sec;
  
  pChar +=sprintf(pChar,"lookup attempts/hit/miss: %llu/%llu/%llu\n",(unsigned long long int)rozofs_bt_lookup_stats.lookup_attempts,
                                                                     (unsigned long long int)rozofs_bt_lookup_stats.lookup_hit,
								     (unsigned long long int)rozofs_bt_lookup_stats.lookup_miss);
  pChar +=sprintf(pChar,"lookup average time (us): %llu\n\n",(rozofs_bt_lookup_stats.lookup_time== 0)?0:(unsigned long long int)(rozofs_bt_lookup_stats.lookup_attempts/rozofs_bt_lookup_stats.lookup_time));

  if (rozofs_bt_inode_debug_tb_p == NULL) {
      rozofs_bt_inode_debug_tb_p = malloc(sizeof(rozofs_bt_tracking_cache_t *)*ROZOFS_BT_MAX_TRK_CACHE);
    if (rozofs_bt_inode_debug_tb_p == NULL)
    {
      pChar +=sprintf(pChar,"Tracking cache file show not supported\n");
     return uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());  
   }
  }
  while(1)
  {
    pthread_rwlock_rdlock(&rozofs_bt_inode_lock);
    if (list_empty(&rozofs_bt_tracking_list)) {
      pthread_rwlock_unlock(&rozofs_bt_inode_lock);
      break;
    }

    /*
    ** loop on the tracking file
    */
    i = 0;
    list_for_each_forward_safe(p,n,&rozofs_bt_tracking_list) 
    {  
      image_p = (rozofs_bt_tracking_cache_t*)list_entry(p,rozofs_bt_tracking_cache_t,list);
      rozofs_bt_inode_debug_tb_p[i] = image_p;
      image_p->lock_count = 1;
      i++;
      if (i == ROZOFS_BT_MAX_TRK_CACHE) break;
    }

    pthread_rwlock_unlock(&rozofs_bt_inode_lock);
    break;
  }

  
  if (i == 0)
  {
      pChar +=sprintf(pChar,"Tracking cache file is empty\n");
     return uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
  }
  nb_tracking_files = i;

  cur_time_us = rozofs_get_ticker_us();
  cur_time_us = cur_time_us/1000000;
  tm = *localtime((time_t*)&cur_time_us);  
  pChar += sprintf(pChar,"current date : %d-%02d-%02d %02d:%02d:%02d\n\n", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);  
  pChar +=sprintf(pChar,"+-------+---------------+------+----------------------+------------+------------+--------------+--------------+---------+------------------------------------------+\n");
  pChar +=sprintf(pChar,"| slice |   file_index  | type |        mtime         | expiration | cache(ms)  | access_count | reload_count | pending |    errcode                               |\n");
  pChar +=sprintf(pChar,"+-------+---------------+------+----------------------+------------+------------+--------------+--------------+---------+------------------------------------------+\n");
  
 
   for (i = 0; i < nb_tracking_files; i++)
  {

    cur_time_us = rozofs_get_ticker_us();
    image_p = rozofs_bt_inode_debug_tb_p[i];
    
//    timestamp_sec = image_p->timestamp/1000000;
//    tm = *localtime((time_t*)&timestamp_sec);  
//    pChar += sprintf(pChar,"current date : %d-%02d-%02d %02d:%02d:%02d\n\n", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);     

    if (cur_time_us > (image_p->timestamp + image_p->cache_time))
    {
       cur_time_us  = 0;
    }
    else
    {
      cur_time_us = (image_p->timestamp + image_p->cache_time) - cur_time_us;
    }    
    tm = *localtime((time_t*)&image_p->deadline_sec);
    sprintf(date,"  %02d:%02d:%02d ", tm.tm_hour, tm.tm_min, tm.tm_sec);
    tm = *localtime((time_t*)&image_p->mtime);
    sprintf(date2," %d-%02d-%02d %02d:%02d:%02d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);

    pChar += sprintf(pChar,"| %4d  | %12llu  | %s | %s |%s | %10llu | %12llu | %12llu | %s | %40s |\n", image_p->key.s.usr_id,(unsigned long long int)image_p->key.s.file_id,
                    ( image_p->key.s.key == ROZOFS_REG)?"REG ":"DIR ",date2,date,
		    (unsigned long long int)cur_time_us/1000,
		    (unsigned long long int)image_p->access_count,
		    (unsigned long long int)image_p->reload_count,
		    (unsigned long long int)image_p->pending?"  YES ":"  NO   ",strerror(image_p->errcode));
    /*
    ** unlock the entry
    */
    image_p->lock_count = 0;
  }
  pChar +=sprintf(pChar,"+-------+---------------+------+----------------------+------------+--------------+--------------+---------+------------------------------------------+\n");

  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());  
}  


/*
**_________________________________________________________________
*/

int rozofs_bt_check_tracking_file_update_local(rozofs_bt_tracking_cache_t *image_p)
{
   int fd;
   char filename[1024];
   struct stat statbuf;
   ssize_t ret;
   char *data_p;

   sprintf(filename,"%s/host0/reg_attr/%d/trk_%llu",rozofs_bt_export_path_p,image_p->key.s.usr_id,(long long unsigned int)image_p->key.s.file_id);
   fd = open(filename,O_RDONLY);
   if (fd < 0) return -1;
   ret = fstat(fd,&statbuf);
   if (ret < 0)
   {
     close(fd);
     return -1;
   } 
   ret = fstat(fd,&statbuf);
   if (ret < 0)
   {
     close(fd);
     return -1;
   }
   /*
   ** check the mtime of the file
   */
   if ( statbuf.st_mtime == image_p->mtime)
   {
     close(fd);
     return 0;
   }
   data_p = (char *) (image_p+1);
   ret = pread( fd, data_p, statbuf.st_size, 0);
   if (ret != statbuf.st_size)
   {
     close(fd);
     return -1;
   }
   /*
   ** OK niow save the new mtime
   */
   image_p->mtime = statbuf.st_mtime;
   return 0;

}

/*
**_________________________________________________________________
*/
/**
*  Send a request towards the tracking file client reader

   @param working_p: working context
   @param eid: export identifier
   @param usr_id : tracking file slice
   @param file_id: file to read within the slice
   @param image_p: pointer to the array where data must be returned
   
   
   @retval 0 on success
   @retval < 0 on error
*/

int rozofs_bt_tracking_file_read_request(rozofs_bt_attr_ctx_t *working_p,int eid,uint16_t usr_id,uint64_t file_id,uint32_t type,rozofs_bt_tracking_cache_t *image_p)
{
   expbt_msgint_full_t *msg_p;
   int ret;
   uint64_t message;
   
   msg_p = &working_p->msg;
   msg_p->hdr.xid = 0;         /**< not yet used */
   msg_p->hdr.user_ctx = NULL; /**< not yet used */ 
   msg_p->hdr.opcode = EXP_BT_TRK_READ; 
   msg_p->hdr.queue_prio = PRIO_RSP;
   msg_p->hdr.rozofs_queue = &rozofs_bt_ino_queue_req;
   
   msg_p->req.read_trk_rq.read_trk.eid = eid;
   msg_p->req.read_trk_rq.read_trk.usr_id = usr_id;
   msg_p->req.read_trk_rq.read_trk.file_id = file_id;
   msg_p->req.read_trk_rq.read_trk.type = type;
   msg_p->req.read_trk_rq.image_p = image_p;
   message = (uint64_t) working_p;
   
  ret = sendto(rozofs_bt_inode_thread_ctx_tb[0].sendSocket,&message, sizeof(message),0,(struct sockaddr*)&rozofs_bt_cli_reader_socket_name[0],sizeof(rozofs_bt_cli_reader_socket_name[0]));
  if (ret <= 0) {
     fatal("error while submtting a request to the tracking file client reader sendto socket %d (%s) %s",rozofs_bt_inode_thread_ctx_tb[0].sendSocket, rozofs_bt_cli_reader_socket_name[0].sun_path, strerror(errno));
     exit(0);  
  }
  return 0;
}
/*
**__________________________________________________________________
*/
/**
*  Insert a tracking file in the tracking file cache

   @param trk_p: pointer to the tracking file context
   
   @retval none
*/
void rozofs_bt_put_tracking(rozofs_bt_tracking_cache_t * trk_p) 
{
    pthread_rwlock_wrlock(&rozofs_bt_inode_lock);

    rozofs_tracking_file_count++;
    trk_p->he.key   = &trk_p->key.u64;
    trk_p->he.value = trk_p;
    htable_put_entry(&htable_bt_inode, &trk_p->he);
    list_push_front(&rozofs_bt_tracking_list, &trk_p->list);

    pthread_rwlock_unlock(&rozofs_bt_inode_lock);

}

/*
**__________________________________________________________________
*/
/**
*  delete a tracking file cache entry  in the tracking file cache

   @param trk_p: pointer to the tracking file context
   
   @retval none
*/

 void rozofs_bt_del_tracking(rozofs_bt_tracking_cache_t * trk_p) 
{

    pthread_rwlock_wrlock(&rozofs_bt_inode_lock);
    /*
    ** check the case of the pending and the lock
    */
    if ((trk_p->lock_count) || (trk_p->pending)) goto out;

    rozofs_tracking_file_count--;
    htable_del_entry(&htable_bt_inode, &trk_p->he);
    list_remove(&trk_p->list);
    xfree(trk_p);    

out:
    pthread_rwlock_unlock(&rozofs_bt_inode_lock);

}

/*
**__________________________________________________________________
*/
/**
     Get the pointer to a tracking file
     
     @param key: key of the tracking file (eid,usr_id & file_id)
     
     @retval NULL: not found
     @retval != NULL pointer to the tracking file cache entry
*/
rozofs_bt_tracking_cache_t *rozofs_bt_get_tracking(rozofs_bt_track_key_t key) 
{
  rozofs_bt_tracking_cache_t *p;

  pthread_rwlock_rdlock(&rozofs_bt_inode_lock);
  
  hash_tracking_cur_collisions = 0;
  p= htable_get(&htable_bt_inode, &key.u64);
  /*
  ** lock the context for a given duration (based on the directory timeout right now
  */
  if (p != NULL) {
    p->deadline_sec = time(NULL) + rozobs_bt_inode_track_cache_entry_delay_sec;   
    p->access_count++;
  }
  
  pthread_rwlock_unlock(&rozofs_bt_inode_lock);

  return p;

}

/*
**__________________________________________________________________
*/

static inline uint32_t tracking_hash(void *key1) {

    unsigned char *d = (unsigned char *) key1;
    int i = 0;
    int h;

     h = 2166136261U;
    /*
     ** hash on name
     */
    d = key1;
    for (i = 0; i <sizeof (rozofs_bt_track_key_t) ; d++, i++) {
        h = (h * 16777619)^ *d;
    }
    return (uint32_t) h;
}
/*
**__________________________________________________________________
*/

static inline int tracking_cmp(void *v1, void *v2) {
      int ret;
      ret =  memcmp(v1, v2, sizeof (rozofs_bt_track_key_t));
      if (ret != 0) {
          hash_tracking_collisions_count++;
	  hash_tracking_cur_collisions++;
	  return ret;
      }
      if (hash_tracking_max_collisions < hash_tracking_cur_collisions) hash_tracking_max_collisions = hash_tracking_cur_collisions;
      return ret;
}


/**
**_______________________________________________________________________________________
*/    
rozofs_bt_track_key_t rozofs_bt_inode_get_filekey_from_inode(uint64_t inode)
{
  rozofs_bt_track_key_t key;
  rozofs_inode_t rozofs_ino;
   
   rozofs_ino.fid[0] = 0;
   rozofs_ino.fid[1] = inode;
   
  
  key.u64 = 0;
  if ((rozofs_ino.s.key != ROZOFS_REG) && (rozofs_ino.s.key != ROZOFS_DIR))
  {
     /*
     ** not a regular file
     */
     return key;
   }
   key.s.eid     =rozofs_bt_eid; 
   key.s.usr_id  =rozofs_ino.s.usr_id;
   key.s.file_id =rozofs_ino.s.file_id;
   key.s.key     = rozofs_ino.s.key;
   return key;

}


/**
**_______________________________________________________________________________________
*/
/**
*  Ask to the tracking file client READER to get the content of a tracking file

   That function is called when the tracking file is not found in the hash table
   It alllocates en entry to handle the content of the tracking file and inserts it in the hash table
   The entry remains pending until the tracking file client reader fills the entry the file content
   The working context is queued on the pending list of the tracking file cache context.
   
   The context is removed from the pending list upon receiving the response of the tracking file client reader
   

   @param :file_key: key of the file to get
   @param working_èp: working context
   
   @retval < 0 with errno = EAGAIN
   @reval < 0 with errno <> EAGAIN --> error
*/
int rozofs_bt_load_tracking_file(rozofs_bt_track_key_t file_key,rozofs_bt_attr_ctx_t *working_p,rozofs_bt_pfc_callback_t rsp_trk_read_cbk)
{
   ssize_t read_size;
   rozofs_bt_tracking_cache_t *image_p;
   /*
   ** allocate the memory to store a full tracking file
   */
   read_size = sizeof(exp_trck_file_header_t)+EXP_TRCK_MAX_INODE_PER_FILE*sizeof(ext_mattr_t);   

   image_p = xmalloc(read_size + sizeof(rozofs_bt_tracking_cache_t));
   if (image_p == NULL)
   {
     errno = ENOMEM;
     return -1;
   }
   /*
   ** fill up the key
   */
   image_p->key.u64 = 0;
   image_p->key.s.eid = file_key.s.eid;
   image_p->key.s.usr_id  = file_key.s.usr_id;
   image_p->key.s.file_id = file_key.s.file_id;
   image_p->key.s.key = file_key.s.key;
   image_p->lock_count = 0;
   image_p->cache_time = 0;
   image_p->reload_count = 0;
   image_p->access_count = 0;

   image_p->length = read_size;
   image_p->mtime = 0;
   image_p->errcode = 0;
   list_init(&image_p->list);
   list_init(&image_p->pending_list);
   /*
   ** insert in the hash table
   */
   image_p->timestamp = 0;
   image_p->cache_time = 0;
   image_p->deadline_sec = time(NULL) + rozobs_bt_inode_track_cache_entry_delay_sec;  
   /*
   ** assert the pending flag and insert the entry in the hash table
   */
   image_p->pending = 1;;
   rozofs_bt_put_tracking(image_p);
   /*
   ** send a read request towards the tracking file client reader
   */
   rozofs_bt_tracking_file_read_request(working_p,file_key.s.eid,file_key.s.usr_id,file_key.s.file_id,image_p->key.s.key,(void *)image_p);
   /*
   ** insert the working context in the pending list and the response callback
   */
   working_p->rsp_trk_read_cbk = rsp_trk_read_cbk;
   list_init(&working_p->pending_list);
   working_p->image_p =image_p;
   list_push_front(&image_p->pending_list, &working_p->pending_list);
   errno = EAGAIN;
   return -1;
}
/**
**_______________________________________________________________________________________
*/
/**
   There is already a read request for that tracking file.
   We queue the working context on the pending list of the tracking file cache entry
   
   @param image_p: pointer to the cache entry 
   @param working_p: pointer to the working context
   
   @retval < 0 with errno = EAGAIN
   @reval < 0 with errno <> EAGAIN --> error
*/      
int rozofs_bt_queue_working_ctx_on_tracking_file_entry(rozofs_bt_tracking_cache_t *image_p,rozofs_bt_attr_ctx_t *working_p)
{
   list_init(&working_p->pending_list);
   list_push_back(&image_p->pending_list, &working_p->pending_list);
   errno = EAGAIN;
   return -1;
}

/**
**_______________________________________________________________________________________
*/

char *rozofs_bt_get_fname(char *bufout,rozofs_inode_fname_t *fname)
{
   if (fname->name_type == ROZOFS_FNAME_TYPE_DIRECT)
   {
      memcpy(bufout,fname->name,fname->len);
      bufout[fname->len] = 0;
      return bufout;
   }
   return bufout;
}
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
ext_mattr_t *rozofs_bt_get_inode_ptr_from_image(rozofs_bt_tracking_cache_t *tracking_p,uint64_t inode)
{
   exp_trck_file_header_t *trk_hdr_p;
   rozofs_inode_t rozofs_ino;
   off_t off; 
   uint16_t *p16;
   uint8_t *p8;
   uint16_t idx;  
   ext_mattr_t  *inode_p;
   rozofs_inode_t *inode_val_p;

   
   rozofs_ino.fid[0] = 0;
   rozofs_ino.fid[1] = inode;   
   
   trk_hdr_p = (exp_trck_file_header_t*)(tracking_p+1);
   
   off = GET_FILE_OFFSET(rozofs_ino.s.idx);
   p8 = (uint8_t*)trk_hdr_p;
   p16 = (uint16_t *)(p8+off);
   idx = *p16;
   if (idx == 0xffff) {   
     errno = ENOENT;
     return NULL;
   }   
   inode_p = (ext_mattr_t*)(trk_hdr_p+1);
   inode_p += idx;
   /*
   ** check the value of the inode in the context
   */
   inode_val_p = (rozofs_inode_t*)inode_p->s.attrs.fid;
   if(inode_val_p->fid[1] != inode) 
   {
     errno = ENOENT;
     return NULL;   
   }
   return inode_p;
}

/**
**_______________________________________________________________________________________
*/
/**
*  Allocation of a working context to handle the getattr()

   @param none
   @retval <> NULL : pointer to the working context
   @retval NULL : out of memory
*/
rozofs_bt_attr_ctx_t *rozofs_bt_inode_alloc_working_ctx()
{
  rozofs_bt_attr_ctx_t *working_p;

  working_p = (rozofs_bt_attr_ctx_t*)malloc(sizeof(rozofs_bt_attr_ctx_t));
  memset(working_p,0,sizeof(rozofs_bt_attr_ctx_t));
  list_init(&working_p->pending_list);
  return working_p;
}
/**
**_______________________________________________________________________________________
*/
/**
*  release of a working context to

   @param p: pointer to the working context
   @retval none 
*/

void rozofs_bt_inode_free_working_ctx(void *p)
{
 /*
 ** we might need to check that the context is not queued on the pending_list
 */
 free(p);
}

/**
**_______________________________________________________________________________________
*/
/**
   Internal inode lookup
   
   @param inode: lower part of the inode
   @param tracking_ret_p: pointer to the tracking entry (might be NULL)
   
   @retval <> NULL: pointer to the inode context, tracking cache entry available if provided
   @retval NULL: not found (errno can be:
                          - EAGAIN: need to load tracking file
			  - ENOENT; the inode does not exists
			  - other : errcode returned while attempting the read the tracking file)
*/
ext_mattr_t *rozofs_bt_lookup_inode_internal_with_tracking_entry(uint64_t inode,rozofs_bt_tracking_cache_t **tracking_ret_p)
{
   ext_mattr_t *inode_p = NULL;
   rozofs_bt_track_key_t file_key;
   rozofs_bt_tracking_cache_t *tracking_p = NULL;
   int errcode = 0;
    struct timeval tv;
    uint64_t tic,toc;

   if (tracking_ret_p != NULL) *tracking_ret_p = NULL;
   /*
   ** normalize the inode value
   */
   inode = rozofs_bt_inode_normalize(inode);
   
   
    __atomic_fetch_add(&rozofs_bt_lookup_stats.lookup_attempts,1,__ATOMIC_SEQ_CST);

    gettimeofday(&tv,(struct timezone *)0);
    tic = MICROLONG(tv);
   /*
   ** check if the inode file exists
   */
   file_key = rozofs_bt_inode_get_filekey_from_inode(inode);
   if (file_key.u64 == 0)
   {
     errcode = EINVAL;
     goto out;
   }
   tracking_p = rozofs_bt_get_tracking(file_key);
   if (tracking_p == NULL)
   {
     errcode = EAGAIN;
     goto out;
   }
   /*
   ** check if the entry is pending: if it is the case we should send EAGAIN
   */
   if (tracking_p->pending) 
   {
      errcode = EAGAIN;
      goto out;
   }
   if (tracking_p->errcode != 0)
   {   
      
      errno = tracking_p->errcode;
      return NULL;
   }
   /*
   ** if there is no entry for the inode, errno is set to ENOENT
   */
   inode_p = rozofs_bt_get_inode_ptr_from_image(tracking_p,inode);
out:
   if (inode_p!=NULL) __atomic_fetch_add(&rozofs_bt_lookup_stats.lookup_hit,1,__ATOMIC_SEQ_CST);
   else       __atomic_fetch_add(&rozofs_bt_lookup_stats.lookup_miss,1,__ATOMIC_SEQ_CST);

   gettimeofday(&tv,(struct timezone *)0);
   toc = MICROLONG(tv);
   tic = toc-tic;
    __atomic_fetch_add(&rozofs_bt_lookup_stats.lookup_time,tic,__ATOMIC_SEQ_CST);
   errno = errcode;
   /*
   ** return the pointer to the tracking cache entry
   */
   if (tracking_ret_p != NULL) *tracking_ret_p = tracking_p;

   return inode_p;
}


/**
**_______________________________________________________________________________________
*/
/**
   Internal inode lookup: use by batch read
   
   @param inode: lower part of the inode
   
   @retval <> NULL: pointer to the inode context
   @retval NULL: not found
*/
ext_mattr_t *rozofs_bt_lookup_inode_internal(uint64_t inode)
{
   return rozofs_bt_lookup_inode_internal_with_tracking_entry(inode,NULL);
}
/**
**_______________________________________________________________________________________
*/
int rozofs_bt_process_get_inode_attr(rozo_attr_cmd_t *cmd_p,rozofs_bt_attr_ctx_t *working_p,rozofs_bt_pfc_callback_t rsp_trk_read_cbk)
{
   rozofs_memreg_t *mem_p = NULL;
   rozofs_bt_track_key_t file_key;
   rozofs_bt_tracking_cache_t *tracking_p = NULL;
   int ret;
   int k;
   ext_mattr_t  *inode_p = NULL;
      
   for (k= 0; k< working_p->nb_commands;k++)
   {
     if (working_p->mem[k].key == 0) break;
     if (working_p->mem[k].key == cmd_p->rozofs_shm_ref)
     {
        mem_p = working_p->mem[k].mem_p;
	break;
     }   
   }  
   if (mem_p == NULL)
   { 
     working_p->mem[k].key = cmd_p->rozofs_shm_ref;
     mem_p = rozofs_shm_lookup(cmd_p->rozofs_shm_ref);
     if (mem_p == NULL)
     {
       return -1;
     }
     working_p->mem[k].mem_p = mem_p;
   }
   /*
   ** check if the inode file exists
   */
   file_key = rozofs_bt_inode_get_filekey_from_inode(cmd_p->inode);
   if (file_key.u64 == 0)
   {
     errno = EINVAL;
     return -1;
   }
   for (k= 0; k< working_p->nb_commands;k++)
   {
     if (working_p->file[k].key == 0) break;
     if (working_p->file[k].key == file_key.u64)
     {
        tracking_p = working_p->file[k].tracking_p;
	break;
     }   
   }
   if (tracking_p == NULL)
   { 
     /*
     ** Attempt to get the pointer to the inode image file
     */
     tracking_p = rozofs_bt_get_tracking(file_key);
     if (tracking_p == NULL)
     {
       /*
       ** need to get the tracking file
       */
       ret = rozofs_bt_load_tracking_file(file_key,working_p,rsp_trk_read_cbk);
       if (ret < 0)
       {
	 return -1;
       }
     } 
     else
     {
       /*
       **  The entry has been found, but its content might no be yet available
       ** Check if we have a pending read of that tracking file.
       ** If it is the case we queue the working context on the pending list of the tracking file entry
       */
       if (tracking_p->pending)
       {
          working_p->rsp_trk_read_cbk = rsp_trk_read_cbk;
          return rozofs_bt_queue_working_ctx_on_tracking_file_entry(tracking_p,working_p);
       }
       /*
       ** The entry has been found, if there is an error during the reading the service is rejected
       ** we do not register it in the working context of the command
       */
       if (tracking_p->errcode != 0)
       {
	  errno = tracking_p->errcode;
	  return -1;
       }
       /*
       ** The entry and its content are available, so save it in the working context
       */
       working_p->file[k].key = file_key.u64;       
       working_p->file[k].tracking_p = tracking_p;
     }
   }
   else
   {
   }
   /*
   ** The entry has been found, if there is an error during the reading the service is rejected
   */
   if (tracking_p->errcode != 0)
   {
      errno = tracking_p->errcode;
      return -1;
   }
   inode_p = rozofs_bt_get_inode_ptr_from_image(tracking_p,cmd_p->inode);
   if (inode_p)
   {
      uint8_t *remote_p;
      uint8_t *local_p;
      uint8_t *remote_buf;
      int64_t size;
      
      remote_p = mem_p->remote_addr;
      local_p = mem_p->addr;
      remote_buf = cmd_p->buf;
      size = remote_buf -remote_p;
      local_p +=size;
      memcpy(local_p,&inode_p->s.attrs.size,sizeof(uint64_t));
      
      errno = 0;
      return 0;
   }
   return -1;

}


/**
**_______________________________________________________________________________________
*/

void rozofs_bt_process_getattr_continue(rozofs_bt_attr_ctx_t *attr_working_ctx_p,int errcode)  
{

   rozo_batch_hdr_t *hdr_p;
   rozo_attr_cmd_t *cmd_p;
   int i;
   int ret;
   rozofs_bt_rsp_buf *response_p;

   response_p =  &attr_working_ctx_p->response; 


   hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(attr_working_ctx_p->recv_buf); 
   cmd_p = (rozo_attr_cmd_t*)(hdr_p+1);
   FDL_INFO("rozofs_bt_process_getattr_continue : %d \n",attr_working_ctx_p->nb_commands);

   for (i = attr_working_ctx_p->nb_commands; i <hdr_p->nb_commands; i++,cmd_p++)
   {
      ret = rozofs_bt_process_get_inode_attr(cmd_p,attr_working_ctx_p,rozofs_bt_process_getattr_continue);
      if (ret < 0)
      {
        if (errno == EAGAIN) return;
        response_p->res[i].data = cmd_p->data;
	response_p->res[i].status = -1;
	response_p->res[i].size = errno;
      }
      else
      {
        response_p->res[i].data = cmd_p->data;
	response_p->res[i].status = 0;
	response_p->res[i].size = ret;
      }
      attr_working_ctx_p->nb_commands++;   
   }
   response_p->hdr.msg_sz = sizeof(rozo_batch_hdr_t)-sizeof(uint32_t)+hdr_p->nb_commands*sizeof(rozo_io_res_t);
   errno = 0;
   ret = send(attr_working_ctx_p->socket_id,response_p,response_p->hdr.msg_sz+sizeof(uint32_t),0);
 
   rozofs_bt_free_receive_buf(attr_working_ctx_p->recv_buf);
   rozofs_bt_inode_free_working_ctx(attr_working_ctx_p);

}

/**
**_______________________________________________________________________________________
*/

/**
  Process a getattr in batch mode
  
  @param recv_buf: pointer to the buffer that contains the command
  @param socket_id: file descriptor used for sending back the response
  
  @retval none
*/
void rozofs_bt_process_getattr(void *recv_buf,int socket_id)
{
   rozo_batch_hdr_t *hdr_p;
   rozo_attr_cmd_t *cmd_p;
   int i;
   int ret;
   rozofs_bt_rsp_buf *response_p;
   rozofs_bt_attr_ctx_t *attr_working_ctx_p;
   
   attr_working_ctx_p = rozofs_bt_inode_alloc_working_ctx();

   attr_working_ctx_p->recv_buf = recv_buf;
   attr_working_ctx_p->socket_id = socket_id; 
   response_p =  &attr_working_ctx_p->response; 

   hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(recv_buf); 
   cmd_p = (rozo_attr_cmd_t*)(hdr_p+1);
   memcpy(&response_p->hdr,hdr_p,sizeof(rozo_batch_hdr_t));
   attr_working_ctx_p->nb_commands = 0;
    
   
   for (i = 0; i <hdr_p->nb_commands; i++,cmd_p++)
   {
      ret = rozofs_bt_process_get_inode_attr(cmd_p,attr_working_ctx_p,rozofs_bt_process_getattr_continue);
      if (ret < 0)
      {
        if (errno == EAGAIN) return;
        response_p->res[i].data = cmd_p->data;
	response_p->res[i].status = -1;
	response_p->res[i].size = errno;
      }
      else
      {
        response_p->res[i].data = cmd_p->data;
	response_p->res[i].status = 0;
	response_p->res[i].size = ret;
      }
      attr_working_ctx_p->nb_commands++;   
   }
   response_p->hdr.msg_sz = sizeof(rozo_batch_hdr_t)-sizeof(uint32_t)+hdr_p->nb_commands*sizeof(rozo_io_res_t);
   errno = 0;
   ret = send(socket_id,response_p,response_p->hdr.msg_sz+sizeof(uint32_t),0);

   rozofs_bt_free_receive_buf(recv_buf);
   rozofs_bt_inode_free_working_ctx(attr_working_ctx_p);
} 



/**
**_______________________________________________________________________________________
*/
void rozofs_bt_wakeup_read_trk_waiters(rozofs_bt_attr_ctx_t *attr_working_ctx_p)
{

  expbt_msgint_full_t *msg_p;
  rozofs_bt_tracking_cache_t *image_p;
  errno = 0;
  list_t *p,*n;
  list_t *pending_head;
  rozofs_bt_attr_ctx_t *work_p=NULL;

  msg_p = &attr_working_ctx_p->msg;
  image_p = attr_working_ctx_p->image_p;
  pending_head = &image_p->pending_list;
  
  if (msg_p->rsp.read_trk_rsp.rsp.status < 0)
  {
     errno = msg_p->rsp.read_trk_rsp.rsp.errcode;
     
  }

  image_p->pending = 0;  
    
  list_for_each_forward_safe(p,n,pending_head) {
      work_p = (rozofs_bt_attr_ctx_t *) list_entry(p, rozofs_bt_attr_ctx_t, pending_list);
      list_remove(&work_p->pending_list);
//      rozofs_bt_process_getattr_continue(work_p);
      (*work_p->rsp_trk_read_cbk)(work_p,errno);

  }

}
/**
**_______________________________________________________________________________________
*/

void rozofs_bt_process_trk_file_read_response(rozofs_bt_attr_ctx_t *attr_working_ctx_p) 
{

  expbt_msgint_full_t *msg_p;
  
  msg_p = &attr_working_ctx_p->msg;
   switch (msg_p->hdr.opcode)
   {
     case EXP_BT_TRK_READ:
       return rozofs_bt_wakeup_read_trk_waiters(attr_working_ctx_p);
       break;

      default:
        fatal("unexpected opcode : %u",msg_p->hdr.opcode);
	break;
   }
} 

/**
**_______________________________________________________________________________________
*/
void rozofs_bt_process_getattr_from_main_thread(void *recv_buf,int socket_id)
{
   rozo_batch_hdr_t *hdr_p;
   
   hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(recv_buf); 
   hdr_p->private = (uint64_t)socket_id;
   
   rozofs_queue_put_prio(&rozofs_bt_ino_queue_req,recv_buf,PRIO_CMD);
}

/**
**_______________________________________________________________________________________
*/

/**
  Process a batch init command : registration of the eid and export path
  
  @param recv_buf: pointer to the buffer that contains the command
  @param socket_id: file descriptor used for sending back the response
  
  @retval none
*/
void rozofs_bt_process_init(void *recv_buf,int socket_id)
{
   rozo_batch_hdr_t *hdr_p;
   rozo_init_cmd_t *cmd_p;
   rozofs_bt_rsp_buf response;
   

   hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(recv_buf); 
   cmd_p = (rozo_init_cmd_t*)(hdr_p+1);
   memcpy(&response.hdr,hdr_p,sizeof(rozo_batch_hdr_t));
   
   rozofs_bt_export_path_p = strdup(cmd_p->export_path);
   rozofs_bt_eid = cmd_p->eid;

   response.res[0].data = 0;
   response.res[0].status = 0;
   response.res[0].size = 0;

   response.hdr.msg_sz = sizeof(rozo_batch_hdr_t)-sizeof(uint32_t)+hdr_p->nb_commands*sizeof(rozo_io_res_t);
   errno = 0;
   send(socket_id,&response,response.hdr.msg_sz+sizeof(uint32_t),0);
   rozofs_bt_free_receive_buf(recv_buf);   
}   


/*
**_____________________________________________________________________________________________

     T R A C K I N G    F I L E    R E A D    F O R   IO_BT_READ
**_____________________________________________________________________________________________
*/     
/**
**_______________________________________________________________________________________
*/
/**
*  Ask to the tracking file client READER to get the content of a tracking file

   That function is called when the tracking file is not found in the hash table
   It alllocates en entry to handle the content of the tracking file and inserts it in the hash table
   The entry remains pending until the tracking file client reader fills the entry the file content
   The working context is queued on the pending list of the tracking file cache context.
   
   The context is removed from the pending list upon receiving the response of the tracking file client reader
   

   @param :inode: inode for which the tracking file does not exists
   @param working_p: working context
   @param rsp_trk_read_cbk: call back to call upon receiving the response of tracking file read.
   
   @retval 0 : the command has been sent, just wait for the answer
   @retval < 0: error see errno for details

*/
int rozofs_bt_load_tracking_file_for_io_read(uint64_t inode,rozofs_bt_attr_ctx_t *working_p,rozofs_bt_pfc_callback_t rsp_trk_read_cbk)
{
   ssize_t read_size;
   rozofs_bt_tracking_cache_t *image_p = NULL;
   rozofs_bt_track_key_t file_key;
   
   file_key = rozofs_bt_inode_get_filekey_from_inode(inode);
   if (file_key.u64 == 0)
   {
     errno = EINVAL;
     return -1;
   }
   image_p = rozofs_bt_get_tracking(file_key);
   if (image_p != NULL)
   {
      /*
      ** if it is pending we should queue the working context
      */
      FDL_INFO("FDL file_for_io_read QUEUE inode %llu",(unsigned long long int) inode);
      if (image_p->pending)
      {
         working_p->rsp_trk_read_cbk = rsp_trk_read_cbk;
         rozofs_bt_queue_working_ctx_on_tracking_file_entry(image_p,working_p);
	 return 0;
      }
      /*
      ** The entry is valid now: it was not the case at the time we ask for file tracking loading
      ** so we can reply right away now since it is not pending
      */
      (*rsp_trk_read_cbk)(working_p,image_p->errcode);
      return 0;
      
   }
   /*
   ** it does not exists: allocate the memory to store a full tracking file
   */
   read_size = sizeof(exp_trck_file_header_t)+EXP_TRCK_MAX_INODE_PER_FILE*sizeof(ext_mattr_t);   

   image_p = xmalloc(read_size + sizeof(rozofs_bt_tracking_cache_t));
   if (image_p == NULL)
   {
     errno = ENOMEM;
     return -1;
   }
   /*
   ** fill up the key
   */
   image_p->key.u64 = 0;
   image_p->key.s.eid = file_key.s.eid;
   image_p->key.s.usr_id  = file_key.s.usr_id;
   image_p->key.s.file_id = file_key.s.file_id;
   image_p->key.s.key = file_key.s.key;
   image_p->lock_count = 0;
   image_p->cache_time = 0;
   image_p->reload_count = 0;
   image_p->access_count = 0;
   
   image_p->length = read_size;
   image_p->mtime = 0;
   image_p->errcode = 0;
   list_init(&image_p->list);
   list_init(&image_p->pending_list);
   /*
   ** insert in the hash table
   */
   image_p->timestamp = 0;
   image_p->cache_time = 0;
   image_p->deadline_sec = time(NULL) + rozobs_bt_inode_track_cache_entry_delay_sec;  
   /*
   ** assert the pending flag and insert the entry in the hash table
   */
   image_p->pending = 1;
   rozofs_bt_put_tracking(image_p);
   /*
   ** send a read request towards the tracking file client reader
   */
   FDL_INFO("FDL file_for_io_read SEND inode %llu",(unsigned long long int) inode);
   rozofs_bt_tracking_file_read_request(working_p,file_key.s.eid,file_key.s.usr_id,file_key.s.file_id,image_p->key.s.key,(void *)image_p);
   /*
   ** insert the working context in the pending list and the response callback
   */
   working_p->rsp_trk_read_cbk = rsp_trk_read_cbk;
   list_init(&working_p->pending_list);
   working_p->image_p =image_p;
   list_push_front(&image_p->pending_list, &working_p->pending_list);
   return 0;

}

/**
**_______________________________________________________________________________________
*/

void rozofs_bt_process_read_trk_file_for_io_cbk(rozofs_bt_attr_ctx_t *attr_working_ctx_p,int errcode)
{

   rozo_batch_hdr_t *hdr_p;
   rozo_trk_file_cmd_t *cmd_p;
   int ret;
   rozofs_bt_thread_msg_t msg_rsp;


   hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(attr_working_ctx_p->recv_buf); 
   cmd_p = (rozo_trk_file_cmd_t*)(hdr_p+1);
   /*
   ** return the errcode send by the tracking file reader thread
   */
   FDL_INFO("FDL read_trk_file_for_io_cbk errocode %u (%s)",errcode,strerror(errcode));
   if (errcode == 0)
   {
     msg_rsp.status = 0;
     msg_rsp.errcode = 0;   
   }
   else
   {
     msg_rsp.status = -1;
     msg_rsp.errcode = errcode; 
   }
   msg_rsp.opcode = hdr_p->opcode;  
   msg_rsp.work_p = cmd_p->data;
   msg_rsp.cmd    = 0;
   /*
   ** send back the response
   */
   ret = sendto(attr_working_ctx_p->socket_id,&msg_rsp, sizeof(msg_rsp),0,(struct sockaddr*)&cmd_p->sun_path,sizeof(cmd_p->sun_path));
   if (ret < 0)
   {
      fatal("error on sendto error %s",strerror(errno));
   }
   if (ret != sizeof(msg_rsp))
   {
      fatal("error on sendto bad size (%d expected %d",ret,(int)sizeof(msg_rsp));
   }
   /*
   ** release the working context and the received buffer
   */
   rozofs_bt_free_receive_buf(attr_working_ctx_p->recv_buf);
   rozofs_bt_inode_free_working_ctx(attr_working_ctx_p);

}

/**
**_______________________________________________________________________________________
*/

/**
  Process a file tracking read triggered by the main thread during batch io_read 
  
  @param recv_buf: pointer to the buffer that contains the command
  @param socket_id: file descriptor used for sending back the response
  
  @retval none
*/
void rozofs_bt_process_read_trk_file_for_io(void *recv_buf,int socket_id)
{

   rozo_batch_hdr_t *hdr_p;
   rozo_trk_file_cmd_t *cmd_p;
   int ret;

   rozofs_bt_attr_ctx_t *attr_working_ctx_p;
   rozofs_bt_thread_msg_t msg_rsp;
      
   attr_working_ctx_p = rozofs_bt_inode_alloc_working_ctx();

   attr_working_ctx_p->recv_buf = recv_buf;
   attr_working_ctx_p->socket_id = socket_id; 

   hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(recv_buf); 
   cmd_p = (rozo_trk_file_cmd_t*)(hdr_p+1);
   
   ret = rozofs_bt_load_tracking_file_for_io_read(cmd_p->inode,attr_working_ctx_p,rozofs_bt_process_read_trk_file_for_io_cbk);
   if (ret < 0)
   {
     msg_rsp.opcode = hdr_p->opcode;    
     msg_rsp.status = -1;
     msg_rsp.errcode = errno;
     msg_rsp.work_p = cmd_p->data;
     
     ret = sendto(socket_id,&msg_rsp, sizeof(msg_rsp),0,(struct sockaddr*)&cmd_p->sun_path,sizeof(cmd_p->sun_path));
     if (ret < 0)
     {
	fatal("error on sendto error %s",strerror(errno));
     }
     if (ret != sizeof(msg_rsp))
     {
	fatal("error on sendto bad size (%d expected %d",ret,(int)sizeof(msg_rsp));
     }
     /*
     ** release the working context and the received buffer
     */
     rozofs_bt_free_receive_buf(attr_working_ctx_p->recv_buf);
     rozofs_bt_inode_free_working_ctx(attr_working_ctx_p);
   }
}


/**
**_______________________________________________________________________________________
*/
/**
*   the sock_id is an AF_UNIX socket used in non connected node (thread_ctx_p->sendSocket)
*/
int rozofs_bt_trigger_read_trk_file_for_io_from_main_thread(uint64_t inode,void *bt_ioctx_p)
{
   rozo_batch_hdr_t *hdr_p;
   rozo_trk_file_cmd_t *cmd_p;
   void *xmit_buffer;
   /*
   ** allocate a buffer to trigger a file read
   */
   pthread_rwlock_wrlock(&rozofs_bt_buf_pool_lock);
   xmit_buffer = ruc_buf_getBuffer(rozofs_bt_buffer_pool_p); 
   pthread_rwlock_unlock(&rozofs_bt_buf_pool_lock); 

   if (xmit_buffer ==NULL)
   {
      errno = ENOMEM;
      return -1;
   }
   hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(xmit_buffer); 
   hdr_p->opcode = ROZO_BATCH_TRK_FILE_READ;
   hdr_p->nb_commands = 1;
   hdr_p->caller = 0;
   hdr_p->private = 0; 
   
   cmd_p = (rozo_trk_file_cmd_t*)(hdr_p+1);
   cmd_p->inode = inode;
   cmd_p->data = bt_ioctx_p;
   /*
   ** reply is performed on an AF_UNIX socket 
   */
   cmd_p->queue = NULL;
   cmd_p->prio = 0;
   memcpy(&cmd_p->sun_path,&rozofs_bt_south_socket_name,sizeof(rozofs_bt_south_socket_name));
   
   rozofs_queue_put_prio(&rozofs_bt_ino_queue_req,xmit_buffer,PRIO_CMD);
   return 0;
}  

   
/*
**_____________________________________________________________________________________________

     T R A C K I N G    F I L E    R E A D    F O R   IO_BT_READ  (END)
**_____________________________________________________________________________________________
*/ 


/**
**_______________________________________________________________________________________
*/
/**
*  Call back associated with a tracking file reading fro directory

   @param attr_working_ctx_p: allocated context within the attributes thread
   @param errcode : error code related to the tracking file read

*/
void rozofs_bt_process_read_trk_file_for_directory_cbk(rozofs_bt_attr_ctx_t *attr_working_ctx_p,int errcode)
{
   expbt_msgint_full_t *msg_th_p;
   expbt_msgint_hdr_t *hdr_p;
   expbt_trk_read_rsp_t *rsp_p;

   msg_th_p = (expbt_msgint_full_t*) attr_working_ctx_p->recv_buf; 

   hdr_p = &msg_th_p->hdr;
   rsp_p = &msg_th_p->rsp.read_trk_rsp.rsp;
   /*
   ** return the errcode send by the tracking file reader thread
   */
   FDL_INFO("FDL read_trk_file_for_io_cbk errocode %u (%s)",errcode,strerror(errcode));
   if (errcode == 0)
   {
     rsp_p->status = 0;
     rsp_p->errcode = 0;   
   }
   else
   {
     rsp_p->status = -1;
     rsp_p->errcode = errcode; 
   }
   /*
   ** reply has to be done on a rozofs_queue that works with priority
   */
   rozofs_queue_put_prio((rozofs_queue_prio_t*)hdr_p->rozofs_queue,msg_th_p,hdr_p->queue_prio);
   /*
   ** release the working context
   */
   rozofs_bt_inode_free_working_ctx(attr_working_ctx_p);

}

/**
**_______________________________________________________________________________________
*/

/**
  Process a file tracking read triggered by the dirent thread : this happen when the tracking file is not found in the cache 
  
  @param msg_th_p: pointer to the buffer that contains the command
  
  @retval none
*/
void rozofs_bt_process_read_trk_file_for_directory(expbt_msgint_full_t *msg_th_p)
{

   int ret;
   rozofs_inode_t rozofs_ino;
   
   expbt_msgint_hdr_t *hdr_p;
   expbt_trk_read_req_t *cmd_p;
   expbt_trk_read_rsp_t *rsp_p;
   

   rozofs_ino.fid[0] = 0;
   rozofs_ino.fid[1] = 0;   
   

   rozofs_bt_attr_ctx_t *attr_working_ctx_p;
      
   attr_working_ctx_p = rozofs_bt_inode_alloc_working_ctx();

   attr_working_ctx_p->recv_buf = msg_th_p;
   attr_working_ctx_p->socket_id = 0; 

   /*
   ** fill up the lower part of the rozofs inode (without eid since it is in the upper part
   */
//   rozofs_ino.fid.s.usr_id = recv_buf->req.read_trk_rq.read_trk.usr_id;
//  rozofs_ino.s.file_id =    recv_buf->req.read_trk_rq.read_trk.file_id;
//   rozofs_ino.fid.s.key =    recv_buf->req.read_trk_rq.read_trk.type;
   
   hdr_p = &msg_th_p->hdr;
   cmd_p = &msg_th_p->req.read_trk_rq.read_trk;
   rsp_p = &msg_th_p->rsp.read_trk_rsp.rsp;

   rozofs_ino.s.usr_id  = cmd_p->usr_id;
   rozofs_ino.s.file_id = cmd_p->file_id;
   rozofs_ino.s.key     = cmd_p->type;   
   
   
   ret = rozofs_bt_load_tracking_file_for_io_read(rozofs_ino.fid[1],attr_working_ctx_p,rozofs_bt_process_read_trk_file_for_directory_cbk);
   if (ret < 0)
   { 
     rsp_p->status = -1;
     rsp_p->errcode = errno;     
     /*
     ** reply has to be done on a rozofs_queue that works with priority
     */
     rozofs_queue_put_prio((rozofs_queue_prio_t*)hdr_p->rozofs_queue,msg_th_p,hdr_p->queue_prio);
     /*
     ** release the working context
     */
     rozofs_bt_inode_free_working_ctx(attr_working_ctx_p);
   }
} 



/*
**_____________________________________________________________________________________________

   R C U    T H R E A D   T R A C K I N G    F I L E   
**_____________________________________________________________________________________________
*/ 

typedef struct _rozofs_bt_rcu_private_t {
   uint64_t period_sec;  /**< polling period en seconds */
   uint64_t messages_count;
   uint64_t check_count;
   uint64_t reload_count;
   uint64_t polling_count;
   uint64_t deletion_count;
   uint64_t polling_time;
   int      deletion_enable;
} rozofs_bt_rcu_private_t;

/*
**_______________________________________________________________
*/
static char * show_trkrd_rcu_thread_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"trkrd_rcu reset                : reset statistics\n");
  pChar += sprintf(pChar,"trkrd_rcu disable              : disable tracking cache deletion (for debug)\n");
  pChar += sprintf(pChar,"trkrd_rcu enable               : enable tracking cache deletion (for debug)\n");
  pChar += sprintf(pChar,"trkrd_rcu period [ <period> ]  : change thread period(unit is second)\n");  
  return pChar; 
}
/*
**_______________________________________________________________
*/
char  *show_trkrd_rcu_thread_stats_display(char *pChar,rozofs_bt_thread_ctx_t *thread_ctx_p,int reset)
{
   rozofs_bt_rcu_private_t *private_p = NULL;
   private_p = thread_ctx_p->thread_private;
   
   pChar +=sprintf(pChar,"Polling period : %llu sec\n",(unsigned long long int)private_p->period_sec);
   pChar +=sprintf(pChar,"deletion       : %s\n",(private_p->deletion_enable)?"ENABLED":"DISABLED");
   pChar +=sprintf(pChar,"polling count  : %llu \n",(unsigned long long int)private_p->polling_count);
   pChar +=sprintf(pChar,"polling time   : %llu us \n",(unsigned long long int)private_p->polling_time);
   pChar +=sprintf(pChar,"polling rate  :  %llu msg/s\n",(unsigned long long int)((private_p->polling_time==0)?0:(private_p->polling_count*1000000)/private_p->polling_time));
   pChar +=sprintf(pChar,"messages count : %llu \n",(unsigned long long int)private_p->messages_count);
   pChar +=sprintf(pChar,"check count    : %llu \n",(unsigned long long int)private_p->check_count);
   pChar +=sprintf(pChar,"check rate     : %llu check/sec\n",(unsigned long long int)((private_p->polling_time==0)?0:(private_p->check_count*1000000)/private_p->polling_time));
   pChar +=sprintf(pChar,"reload count   : %llu \n",(unsigned long long int)private_p->reload_count);
   pChar +=sprintf(pChar,"deletion count : %llu \n",(unsigned long long int)private_p->deletion_count);
   
   if (reset)
   {
     private_p->polling_count = 0;
     private_p->messages_count = 0;
     private_p->check_count = 0;
     private_p->reload_count = 0;
     private_p->deletion_count = 0;
     private_p->polling_time = 0;   
   }
   return pChar;

}
/*
**_______________________________________________________________
*/
void show_trkrd_rcu_thread(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int ret;
    int period;
   rozofs_bt_thread_ctx_t *thread_ctx_p;
   rozofs_bt_rcu_private_t *private_p = NULL;
      
   thread_ctx_p = &rozofs_bt_rcu_thread_ctx[0];

    private_p = thread_ctx_p->thread_private;
    
    
    if (argv[1] == NULL) {
      show_trkrd_rcu_thread_stats_display(pChar,thread_ctx_p,0);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer()); 
      return;  	  
    }

    if (strcmp(argv[1],"reset")==0) {
      pChar = show_trkrd_rcu_thread_stats_display(pChar,thread_ctx_p,1);
      pChar +=sprintf(pChar,"\nStatistics have been cleared\n");
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());  
      return;   
    }
    if (strcmp(argv[1],"period")==0) {   
	if (argv[2] == NULL) {
	show_trkrd_rcu_thread_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;  	  
      }
      ret = sscanf(argv[2], "%d", &period);
      if (ret != 1) {
	show_trkrd_rcu_thread_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;   
      }
      /*
      ** change the period of the thread
      */
      if (period == 0)
      {
        uma_dbg_send(tcpRef, bufRef, TRUE, "value not supported\n");
        return;
      }
      
      private_p->period_sec = period;
      uma_dbg_send(tcpRef, bufRef, TRUE, "Done\n");   	  
      return;
    }
    if (strcmp(argv[1],"disable")==0) {   
	private_p->deletion_enable = 0;
	uma_dbg_send(tcpRef, bufRef, TRUE,"tracking cache entry deletion is disabled");   
	return;   
    }
    if (strcmp(argv[1],"enable")==0) {   
	private_p->deletion_enable = 1;
	uma_dbg_send(tcpRef, bufRef, TRUE,"tracking cache entry deletion  is enabled");   
	return;   
    }
    show_trkrd_rcu_thread_help(pChar);	
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());    
    return;
}



/**
**_________________________________________________________________________________________
*/
/**
* re-read of a tracking file because of the mtime change

   @param thread_ctx_p: pointer to the thread context
   @param eid: export identifier
   @param usr_id : slice number
   @param file_id: tracking file index within the slice
   @param type: type of the tracking file( DIR_ATT, REG_ATTR...)
   @param image_p: pointer to the cache entry
   
   @retval none
*/  
int rozofs_bt_inode_rcu_re_read_tracking_file(rozofs_bt_thread_ctx_t * thread_ctx_p,uint32_t eid,uint32_t usr_id,uint64_t file_id,uint32_t type,rozofs_bt_tracking_cache_t *image_p)
{

   expbt_msgint_full_t *msg_p;
   int ret;
   uint64_t message;
   rozofs_bt_rcu_private_t *private_p = NULL;

   private_p = thread_ctx_p->thread_private;

  FDL_INFO("FDL tracking file re-read for eid %u usr_id %u file_id %llu (mtime %llu)",eid,usr_id,(long long unsigned int)file_id,(long long unsigned int)image_p->mtime);
   /*
   ** allocate memory for sending the message
   */
   msg_p = malloc(sizeof(expbt_msgint_full_t));
   if (msg_p == NULL) 
   {
      errno = EAGAIN;
      return -1;
   }
   msg_p->hdr.xid = 0;         /**< not yet used */
   msg_p->hdr.user_ctx = NULL; /**< not yet used */ 
   msg_p->hdr.opcode = EXP_BT_TRK_READ; 
   msg_p->hdr.queue_prio = PRIO_RSP;
   msg_p->hdr.rozofs_queue = thread_ctx_p->queue_req;
   
   msg_p->req.read_trk_rq.read_trk.eid = eid;
   msg_p->req.read_trk_rq.read_trk.usr_id = usr_id;
   msg_p->req.read_trk_rq.read_trk.file_id = file_id;
   msg_p->req.read_trk_rq.read_trk.type = type;
   msg_p->req.read_trk_rq.image_p = image_p;
   message = (uint64_t)msg_p;
   
   private_p->messages_count++;
   private_p->reload_count++;
  ret = sendto(thread_ctx_p->sendSocket,&message, sizeof(message),0,(struct sockaddr*)&rozofs_bt_cli_reader_socket_name[0],sizeof(rozofs_bt_cli_reader_socket_name[0]));
  if (ret <= 0) {
     fatal("error while submtting a request to the tracking file client reader sendto socket %d (%s) %s",rozofs_bt_inode_thread_ctx_tb[0].sendSocket, rozofs_bt_cli_reader_socket_name[0].sun_path, strerror(errno));
     exit(0);  
  }
  /*
  **  Wait for the response
  */

  {
    void   *msg_rsp_p;
    int prio;  
     /*
     ** Read some data from the queue
     */
     msg_rsp_p = rozofs_queue_get_prio(thread_ctx_p->queue_req,&prio);  
     switch (prio)
     {
       case PRIO_CMD:
	 info("PRIO_CMD Unexpected!!");
	 break;

       case PRIO_RSP:
	 FDL_INFO("FDL Re-Read done!!");
	 //rozofs_bt_process_trk_file_read_response((rozofs_bt_attr_ctx_t*)msg_p);
	 break;    

       default:
	 info("Unexpected!!");
	 break;    
     }
     free(msg_rsp_p);
   }
   return 0;
}

/**
**_________________________________________________________________________________________
*/
/**
*  Deletion of a cache entry because of ENOENT status

   @param thread_ctx_p: pointer to the thread context
   @param image_p: pointer to the cache entry
   
   @retval none
*/
void  rozofs_bt_inode_delete_cache_entry(  rozofs_bt_thread_ctx_t * thread_ctx_p, rozofs_bt_tracking_cache_t *image_p)
{

   info("FDL tracking file deletion for eid %u,usr_id %u file_id %llu",image_p->key.s.eid,image_p->key.s.usr_id,(long long unsigned int)image_p->key.s.file_id);
}
/**
**_________________________________________________________________________________________
*/
/*
** The function check if there is a change in the mtime of the tracking file that is in cache.
   When there is a change of mtime, that function gets the new tracking file that corresponds to that mtime
   
   @param thread_ctx_p: pointer to the thread context
   @param msg_th_p: pointer to the message that contains the response
   
   @retval none
*/   
void rozofs_bt_inode_rcu_process_tracking_file_update_th(rozofs_bt_thread_ctx_t * thread_ctx_p,expbt_msgint_full_t *msg_th_p)
{
    int i;
    rozofs_bt_tracking_cache_t *image_p;

    
   for (i = 0; i < msg_th_p->req.check_rq.cmd.nb_commands;i++)
   {
     image_p = rozofs_bt_inode_rcu_tb_p[msg_th_p->req.check_rq.user_data[i]];
     if (msg_th_p->rsp.check_rsp.entry[i].status  == -1)
     {
        if (msg_th_p->rsp.check_rsp.entry[i].errcode == ENOENT)
	{
	   rozofs_bt_inode_delete_cache_entry(thread_ctx_p,image_p);
	}
	continue;
     }
     /*
     ** There is no error, so check if there is a change in the mtime
     */
     if (msg_th_p->rsp.check_rsp.entry[i].status == 0)
     {
        /*
	** no change: udpate the timestamp of the entry and the cache time (needed since it can be change on the fly by rozodiag)
	*/
	image_p->timestamp = rozofs_get_ticker_us();
        image_p->cache_time = (msg_th_p->rsp.check_rsp.entry[i].type== ROZOFS_REG)?rozofs_tmr_get_attr_us(0):rozofs_tmr_get_attr_us(1);
	FDL_INFO("FDL no change for eid %u,usr_id %u file_id %llu type %d",
	      msg_th_p->req.check_rq.entry[i].eid,msg_th_p->req.check_rq.entry[i].usr_id,(long long unsigned int)msg_th_p->req.check_rq.entry[i].file_id,msg_th_p->req.check_rq.entry[i].type);
	continue;
     }
     /*
     ** there is a change we need to re-read the tracking file
     */
     image_p->reload_count++;
     rozofs_bt_inode_rcu_re_read_tracking_file(thread_ctx_p,
                                               msg_th_p->req.check_rq.entry[i].eid,
                                               msg_th_p->req.check_rq.entry[i].usr_id,
					       msg_th_p->req.check_rq.entry[i].file_id,
					       msg_th_p->req.check_rq.entry[i].type,
					       image_p);
					       
   }
}
/**
**_________________________________________________________________________________________
*/
/**
*  That function is intended to get the mtime from export for each tracking file that is used by the rozofsmount

  @param thread_ctx_p: pointer to the thread context
*/

void rozofs_bt_inode_rcu_check_tracking_file_update_th(rozofs_bt_thread_ctx_t * thread_ctx_p)
{

  list_t             * p;
  list_t             * n;
  int i;
  rozofs_bt_tracking_cache_t *image_p;
  expbt_msgint_full_t *msg_th_p = NULL;
  int nb_sent  = 0;
  int cur_entry= 0;
  uint64_t message;
  int ret;
//  uint64_t cur_time_us;
  time_t cur_time_s;

   void   *msg_p;
   int prio;
   rozofs_bt_rcu_private_t *private_p = NULL;

  private_p = thread_ctx_p->thread_private;
  
  pthread_rwlock_rdlock(&rozofs_bt_inode_lock);
  if (list_empty(&rozofs_bt_tracking_list)) {
    pthread_rwlock_unlock(&rozofs_bt_inode_lock);
    return;
  }

  
  /*
  ** loop on the tracking file
  */
  i = 0;
  list_for_each_forward_safe(p,n,&rozofs_bt_tracking_list) 
  {  
    image_p = (rozofs_bt_tracking_cache_t*)list_entry(p,rozofs_bt_tracking_cache_t,list);
    rozofs_bt_inode_rcu_tb_p[i] = image_p;
    i++;
    if (i == ROZOFS_BT_MAX_TRK_CACHE) break;
  }
  

  pthread_rwlock_unlock(&rozofs_bt_inode_lock);
  
  rozofs_bt_rcu_number_of_tracking_files = i;
//  cur_time_us = rozofs_get_ticker_us();
  cur_time_s = time(NULL);

  for (i = 0; i < rozofs_bt_rcu_number_of_tracking_files; i++)
  {
    image_p = rozofs_bt_inode_rcu_tb_p[i];
    /*
    ** don't care about tracking file for which there is a read in progress
    */
    
    if (image_p->pending == 1) continue;
    
    /*
    ** check if the entry has expired
    */
    if (image_p->deadline_sec < cur_time_s)
//    if (cur_time_us > (image_p->timestamp+image_p->cache_time))
    {
       if (image_p->lock_count) continue;
       /*
       ** we need to delete the entry (warning about locking)
       */       
       struct tm tm = *localtime((time_t*)&image_p->deadline_sec);

       FDL_INFO(" FDL remove tracking file expiration date: %d-%02d-%02d %02d:%02d:%02d\n", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);

       if (private_p->deletion_enable)
       {
         rozofs_bt_del_tracking(image_p);
	 private_p->deletion_count++;
       }
       continue;
    }

#if 0
    if (cur_time_us < (image_p->timestamp+image_p->cache_time))
    {
       /*
       ** no need to refresh
       */
       continue;
    }
#endif
    /*
    ** allocate a sending buffer if needed
    */

    if (msg_th_p == NULL)
    {
      msg_th_p = (expbt_msgint_full_t*)malloc(sizeof(expbt_msgint_full_t));
      if (msg_th_p == NULL)
      {
	 warning("RCU tracking file check malloc failure %s",strerror(errno));
	 if (nb_sent == 0) return;
	 /*
	 ** there are some pending requests, we need to wait fro the response to avoid losing more memory
	 */
	 goto wait_data;
      }
      msg_th_p->hdr.xid = 0;         /**< not yet used */
      msg_th_p->hdr.user_ctx = NULL; /**< not yet used */ 
      msg_th_p->hdr.opcode = EXP_BT_TRK_CHECK; 
      msg_th_p->hdr.queue_prio = PRIO_RSP;
      msg_th_p->hdr.rozofs_queue = thread_ctx_p->queue_rsp;

    }

    // rozofs_bt_check_tracking_file_update_local(image_p);
    
    msg_th_p->req.check_rq.entry[cur_entry].eid     =  image_p->key.s.eid ;
    msg_th_p->req.check_rq.entry[cur_entry].type    = image_p->key.s.key; //ROZOFS_REG;
    msg_th_p->req.check_rq.entry[cur_entry].usr_id  = image_p->key.s.usr_id;
    msg_th_p->req.check_rq.entry[cur_entry].file_id = image_p->key.s.file_id;
    msg_th_p->req.check_rq.entry[cur_entry].mtime   = image_p->mtime;
    msg_th_p->req.check_rq.entry[cur_entry].change_count   = image_p->change_count;
    msg_th_p->req.check_rq.user_data[cur_entry] = i;
    cur_entry++;
    private_p->check_count++;

    FDL_INFO("FDL -->  check for type %d usr_id %d file_id %llu mtime %llu change_count %d",
        (int) image_p->key.s.key,(int)image_p->key.s.usr_id, (unsigned long long int) image_p->key.s.file_id,(unsigned long long int) image_p->mtime,(int) image_p->change_count);
    if (cur_entry== EXP_BT_TRK_CHECK_MAX_CMD) 
    {
       msg_th_p->req.check_rq.cmd.nb_commands = cur_entry;
       message = (uint64_t) msg_th_p;
       private_p->messages_count++;

      ret = sendto(thread_ctx_p->sendSocket,&message, sizeof(message),0,(struct sockaddr*)&rozofs_bt_cli_reader_socket_name[0],sizeof(rozofs_bt_cli_reader_socket_name[0]));
      if (ret <= 0) {
	 fatal("error while submitting a request to the tracking file client reader sendto socket %d (%s) %s",thread_ctx_p->sendSocket, rozofs_bt_cli_reader_socket_name[0].sun_path, strerror(errno));
	 exit(0);  
      } 
      cur_entry = 0; 
      msg_th_p = NULL;
      nb_sent++;    
    }
  }
  /*
  ** we have looked at all the entries, check if there is a buffer under construction 
  */
  if (cur_entry != 0)
  {
    msg_th_p->req.check_rq.cmd.nb_commands = cur_entry;
    message = (uint64_t) msg_th_p;
    private_p->messages_count++;
    ret = sendto(thread_ctx_p->sendSocket,&message, sizeof(message),0,(struct sockaddr*)&rozofs_bt_cli_reader_socket_name[0],sizeof(rozofs_bt_cli_reader_socket_name[0]));
    if (ret <= 0) {
       fatal("error while submitting a request to the tracking file client reader sendto socket %d (%s) %s",thread_ctx_p->sendSocket, rozofs_bt_cli_reader_socket_name[0].sun_path, strerror(errno));
       exit(0);  
    } 
    cur_entry = 0; 
    msg_th_p = NULL;
    nb_sent++;      
  }
  /*
  ** now wait for the data
  */
wait_data:
  while(nb_sent)
  {
    msg_p = rozofs_queue_get_prio(thread_ctx_p->queue_rsp,&prio);  
    switch (prio)
    {
      case PRIO_CMD:
         info("FDL: not expected message on that priority %d",prio);
	 free(msg_p);

	 break;

      case PRIO_RSP:
        rozofs_bt_inode_rcu_process_tracking_file_update_th(thread_ctx_p,msg_p);
	FDL_INFO("FDL check done!!");
	/*
	** Release the message that has been allocated for the check of the tracking files
	*/
	 free(msg_p);
	break;    

      default:
	info("Unexpected!!");
	free(msg_p);
	break;    
    }  
  
    nb_sent--;
  
  }

}



int xxx_debug =0;

/*
 *_______________________________________________________________________
 */
/** Share memory deletion polling thread
 */
 #define ROZOFS_BT_RCU_PTHREAD_FREQUENCY_SEC 3
 
static void *rozofs_bt_rcu_inode_polling_thread(void *v) {

   rozofs_bt_thread_ctx_t * thread_ctx_p = (rozofs_bt_thread_ctx_t*)v;
   char name[64];
   struct timespec ts = {ROZOFS_BT_RCU_PTHREAD_FREQUENCY_SEC, 0};
   rozofs_bt_rcu_private_t *private_p = NULL;
   uint64_t tic,toc;
  struct timeval tc;

   
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    sprintf(name,"TRK_READER#%d",thread_ctx_p->thread_idx);
    
    private_p = thread_ctx_p->thread_private;

    uma_dbg_thread_add_self("bt_rcu");
    for (;;) {
	ts.tv_sec = private_p->period_sec;    

        nanosleep(&ts, NULL);  
	tic = rozofs_get_ticker_us();
	rozofs_bt_inode_rcu_check_tracking_file_update_th(thread_ctx_p);     
	gettimeofday(&tc,(struct timezone *)0);
        toc = rozofs_get_ticker_us();
	private_p->polling_count++;
//	info("FDL tic %llu toc %llu",(unsigned long long int)tic,(unsigned long long int)toc);
	private_p->polling_time +=(toc-tic);
    }
    return 0;
}

/*
**_________________________________________________________________________________________________
*/
/**
 Create the RCU inode thread that is used to update tracking file in cache and to delet tracking files that are not used anymore
 
 @retval 0 on success
 @retval -1 on error

*/ 
int rozofs_bt_rcu_thread_create(int queue_depth)
{
    int err;
    pthread_attr_t             attr;    
    int i;
    rozofs_bt_thread_ctx_t *p;
    rozofs_bt_rcu_private_t *private_p = NULL;    
    
    
    rozofs_bt_inode_rcu_tb_p = malloc(sizeof(rozofs_bt_tracking_cache_t *)*ROZOFS_BT_MAX_TRK_CACHE);
    if (rozofs_bt_inode_rcu_tb_p == NULL)
    {
      return -1;      
   }
   memset(rozofs_bt_inode_rcu_tb_p,0,sizeof(sizeof(rozofs_bt_tracking_cache_t *)*ROZOFS_BT_MAX_TRK_CACHE));
   rozofs_bt_rcu_number_of_tracking_files = 0;
   

   err = pthread_attr_init(&attr);
   if (err != 0) {
     fatal("rozofs_bt_thread_create pthread_attr_init() %s",strerror(errno));
     return -1;
   }  

   memset(&rozofs_bt_rcu_thread_ctx,0,sizeof(rozofs_bt_rcu_thread_ctx));
   p = rozofs_bt_rcu_thread_ctx;
   /*
   ** create the queue to get the reponse from the client tracking file thread for the check case
   */
   p->queue_rsp = malloc(sizeof(rozofs_queue_prio_t));
   if (p->queue_rsp == NULL)
   {
     fatal("Out of memory while create RCU thread of tracking files");
   }
   /*
   ** Create the AF_UNIX socket to communictaion with the client tracking file thread
   */
   rozofs_queue_init_prio(p->queue_rsp,queue_depth+16,MAX_PRIO);

   /*
   ** create the queue to get the reponse from the client tracking file thread for the re-read case
   */
   p->queue_req = malloc(sizeof(rozofs_queue_prio_t));
   if (p->queue_req == NULL)
   {
     fatal("Out of memory while create RCU thread of tracking files");
   }
   /*
   ** Create the AF_UNIX socket to communictaion with the client tracking file thread
   */
   rozofs_queue_init_prio(p->queue_req,queue_depth+16,MAX_PRIO);

   p->sendSocket = socket(AF_UNIX,SOCK_DGRAM,0);
   if (p->sendSocket < 0) {
      fatal("rozofs_bt_thread_create fail to create socket: %s", strerror(errno));
      return -1;   
   } 
   
   for (i = 0; i < 1; i++,p++)
   {
     p->thread_idx = i;
         p->thread_private = malloc(sizeof(rozofs_bt_rcu_private_t));
     if (p->thread_private == NULL) 
     {
	fatal("Out of memory");
	return -1;
     }
     memset(p->thread_private,0,sizeof(rozofs_bt_rcu_private_t));
     private_p = p->thread_private;
     private_p->period_sec = ROZOFS_BT_RCU_PTHREAD_FREQUENCY_SEC;   
     private_p->deletion_enable = 1;
     err = pthread_create(&p->thrdId,&attr,rozofs_bt_rcu_inode_polling_thread,p);
     if (err != 0) {
       fatal("rozofs_bt_rcu_thread_create pthread_create() %s", strerror(errno));
       return -1;
     } 
     uma_dbg_addTopic_option("trkrd_rcu", show_trkrd_rcu_thread,UMA_DBG_OPTION_RESET);

   }
   return 0; 
}


/*
**_____________________________________________________________________________________________

   R C U    T H R E A D   T R A C K I N G    F I L E   E N D 
**_____________________________________________________________________________________________
*/ 




/*
**________________________________________________________________________________
*/
/*
**  BATCH    T H R E A D  for attributes & file tracking reading (io_bt_read)
*/

void *rozofs_bt_inode_thread(void *arg) 
{
  rozofs_bt_thread_ctx_t * ctx_p = (rozofs_bt_thread_ctx_t*)arg;
   void   *msg_p;
   rozo_batch_hdr_t *hdr_p;
   expbt_msgint_hdr_t *hdr_thread_p;
   int socket_id;
   int prio;
   
  uma_dbg_thread_add_self("Batch_ino");

  while(1) {
#if 0
    if ((ctx_p->thread_idx != 0) && (ctx_p->thread_idx >= common_config.mojette_thread_count))
    {
       sleep(30);
       continue;
    }
#endif
    /*
    ** Read some data from the queue
    */
    msg_p = rozofs_queue_get_prio(&rozofs_bt_ino_queue_req,&prio);  
    switch (prio)
    {
      case PRIO_CMD:
	 hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(msg_p); 
	 socket_id = (int)hdr_p->private;
	 switch (hdr_p->opcode) {

	   case ROZO_BATCH_GETATTR:
             rozofs_bt_process_getattr(msg_p,socket_id);
             break;
	   case ROZO_BATCH_TRK_FILE_READ:
	     /*
	     ** we don't care about the socket id of the message, since the AF_UNIX socket to use
	     ** will be found in the command part of the request.
	     */
             rozofs_bt_process_read_trk_file_for_io(msg_p,ctx_p->sendSocket);
             break;

	   default:
             fatal(" unexpected opcode : %d\n",hdr_p->opcode);
             exit(0);       
	 }
	 break;

      case PRIO_CMDQ:
	 hdr_thread_p = (expbt_msgint_hdr_t*) (msg_p); 
	 switch (hdr_thread_p->opcode) {
	   case ROZO_BATCH_TRK_FILE_READ:
	     /*
	     ** we don't care about the socket id of the message, since the AF_UNIX socket to use
	     ** will be found in the command part of the request.
	     */
             rozofs_bt_process_read_trk_file_for_directory((expbt_msgint_full_t*)msg_p);
             break;

	   default:
             fatal(" unexpected opcode : %d\n",hdr_p->opcode);
             exit(0);       
	 }
	 break;
      case PRIO_RSP:
	FDL_INFO("FDL Read done!!");
	rozofs_bt_process_trk_file_read_response((rozofs_bt_attr_ctx_t*)msg_p);
	break;    

      default:
	info("Unexpected!!");
	break;    
    }
  }
}

/*
**_________________________________________________________________________________________________
*/
/*
** Create the threads that will handle all the batch requests

* @param hostname    storio hostname (for tests)
* @param eid    reference of the export
* @param storcli_idx    relative index of the storcli process
* @param nb_threads  number of threads to create
*  
* @retval 0 on success -1 in case of error
*/
int rozofs_bt_inode_thread_create(int nb_threads,int queue_depth) {
   int                        i;
   int                        err;
   pthread_attr_t             attr;
   rozofs_bt_thread_ctx_t * thread_ctx_p;


   rozofs_queue_init_prio(&rozofs_bt_ino_queue_req,queue_depth+16,MAX_PRIO);

   /*
   ** clear the thread table
   */
   memset(rozofs_bt_inode_thread_ctx_tb,0,sizeof(rozofs_bt_thread_ctx_t)*ROZOFS_BT_INODE_MAX_THREADS);
   /*
   ** Now create the threads
   */
   thread_ctx_p = rozofs_bt_inode_thread_ctx_tb;
   for (i = 0; i < nb_threads ; i++) {
     /*
     ** create the socket that the thread will use for sending back response 
     */
     thread_ctx_p->sendSocket = socket(AF_UNIX,SOCK_DGRAM,0);
     if (thread_ctx_p->sendSocket < 0) {
	fatal("rozofs_bt_thread_create fail to create socket: %s", strerror(errno));
	return -1;   
     } 
     err = pthread_attr_init(&attr);
     if (err != 0) {
       fatal("rozofs_bt_thread_create pthread_attr_init(%d) %s",i,strerror(errno));
       return -1;
     }  

     thread_ctx_p->thread_idx = i;
     err = pthread_create(&thread_ctx_p->thrdId,&attr,rozofs_bt_inode_thread,thread_ctx_p);
     if (err != 0) {
       fatal("rozofs_bt_thread_create pthread_create(%d) %s",i, strerror(errno));
       return -1;
     }  
     
     thread_ctx_p++;
  }
  return 0;
}

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
int rozofs_bt_inode_init(int queue_depth,int eid)
{
    /* Initialize list and htables for inode_entries */
    list_init(&rozofs_bt_tracking_list);
    pthread_rwlock_init(&rozofs_bt_inode_lock, NULL);
    
    htable_initialize(&htable_bt_inode, INODE_BT_HSIZE, tracking_hash, tracking_cmp);
    hash_tracking_collisions_count = 0;
    hash_tracking_cur_collisions = 0;
    hash_tracking_max_collisions = 0;
    rozofs_tracking_file_count = 0;
    rozofs_bt_eid = eid;  /**< reference of the export */
    rozofs_bt_export_path_p = NULL; 
    /*
    ** Only one thread is supported
    */ 
    rozofs_bt_inode_thread_create(1,queue_depth);  
    rozofs_bt_rcu_thread_create(queue_depth);
    /*
    ** start the tracking file client readers
    */
    rozofs_bt_trk_cli_reader_thread_create(2);
    
    return 0;
}
   
