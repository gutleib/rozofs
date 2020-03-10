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

#define xmalloc malloc
#define xfree free 

#define PRIO_CMD 2
#define PRIO_RSP 1
#define MAX_PRIO 3
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

typedef struct rozofs_bt_attr_ctx_t 
{
   expbt_msgint_full_t msg;   /**< message array when the module needs to trigger the reading of the tracking file */
   rozofs_bt_tracking_cache_t *image_p;  /**< pointer to the allocated cache entry*/
   list_t pending_list;       /**< queue the working context if there is already a pending reading for the tracking file */
   void *recv_buf;    /**< ruc buffer that contains the command */
   int   socket_id;   /**< reference of the socket to use for sending back the response */
   int nb_commands;
   rozofs_bt_internal_attr_mem_t mem[ROZOFS_MAX_BATCH_CMD];
   rozofs_bt_internal_attr_file_p file[ROZOFS_MAX_BATCH_CMD];
   rozofs_bt_rsp_buf response;
} rozofs_bt_attr_ctx_t;



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

int rozofs_bt_tracking_file_read_request(rozofs_bt_attr_ctx_t *working_p,int eid,uint16_t usr_id,uint64_t file_id,uint32_t type,void *image_p)
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
   msg_p->req.read_trk_rq.read_trk.type = ROZOFS_REG;
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

    rozofs_tracking_file_count--;
    htable_del_entry(&htable_bt_inode, &trk_p->he);
    list_remove(&trk_p->list);
    xfree(trk_p);    

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
static inline rozofs_bt_track_key_t rozofs_bt_inode_get_filekey_from_inode(uint64_t inode)
{
  rozofs_bt_track_key_t key;
  rozofs_inode_t rozofs_ino;
   
   rozofs_ino.fid[0] = 0;
   rozofs_ino.fid[1] = inode;
  
  key.u64 = 0;
  if (rozofs_ino.s.key != ROZOFS_REG)
  {
     /*
     ** not a regular file
     */
     return key;
   }
   key.s.eid =rozofs_bt_eid; 
   key.s.usr_id =rozofs_ino.s.usr_id;
   key.s.file_id =rozofs_ino.s.file_id;
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
int rozofs_bt_load_tracking_file(rozofs_bt_track_key_t file_key,rozofs_bt_attr_ctx_t *working_p)
{
   ssize_t read_size;
   rozofs_bt_tracking_cache_t *image_p;
   char *data_p;
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
   image_p->lock_count = 0;
   image_p->cache_time = 0;

   image_p->length = read_size;
   image_p->mtime = 0;
   list_init(&image_p->list);
   list_init(&image_p->pending_list);
   data_p = (char *) (image_p+1);
   /*
   ** insert in the hash table
   */
   image_p->timestamp = 0;
   image_p->cache_time = 0;
   /*
   ** assert the pending flag and insert the entry in the hash table
   */
   image_p->pending = 1;;
   rozofs_bt_put_tracking(image_p);
   /*
   ** send a read request towards the tracking file client reader
   */
   rozofs_bt_tracking_file_read_request(working_p,file_key.s.eid,file_key.s.usr_id,file_key.s.file_id,0 /*type*/,(void *)data_p);
   /*
   ** insert the working context in the pending list
   */
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
#if 0
   {
        char bufout[128];	
	sprintf(bufout,"Unk");	
        rozofs_bt_get_fname(bufout,&inode_p->s.fname);
	//info("%s inode %llu size %llu",bufout,(long long unsigned int)inode,(long long unsigned int)inode_p->s.attrs.size);
   }
#endif
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
   Internal inode lookup: use by batch read
   
   @param inode: lower part of the inode
   
   @retval <> NULL: pointer to the inode context
   @retval NULL: not found
*/
ext_mattr_t *rozofs_bt_lookup_inode_internal(uint64_t inode)
{
   ext_mattr_t *inode_p = NULL;
   rozofs_bt_track_key_t file_key;
   rozofs_bt_tracking_cache_t *tracking_p = NULL;

   /*
   ** check if the inode file exists
   */
   file_key = rozofs_bt_inode_get_filekey_from_inode(inode);
   if (file_key.u64 == 0)
   {
     errno = EINVAL;
     return NULL;
   }
   tracking_p = rozofs_bt_get_tracking(file_key);
   if (tracking_p == NULL)
   {
     errno = ENOENT;
     return NULL;
   }
   inode_p = rozofs_bt_get_inode_ptr_from_image(tracking_p,inode);
   return inode_p;
}

/**
**_______________________________________________________________________________________
*/
int rozofs_bt_process_get_inode_attr(rozo_attr_cmd_t *cmd_p,rozofs_bt_attr_ctx_t *working_p)
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
	// info("FDL load tracking file\n");
       ret = rozofs_bt_load_tracking_file(file_key,working_p);
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
          return rozofs_bt_queue_working_ctx_on_tracking_file_entry(tracking_p,working_p);
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
      //info("FDL found  tracking file in local cache\n");
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
   // info("Nb command %d\n",hdr_p->nb_commands);
   attr_working_ctx_p->nb_commands = 0;
    
   
   for (i = 0; i <hdr_p->nb_commands; i++,cmd_p++)
   {
      ret = rozofs_bt_process_get_inode_attr(cmd_p,attr_working_ctx_p);
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
   // info("message send socket : %d %d: %s\n",socket_id,ret,strerror(errno));
   rozofs_bt_free_receive_buf(recv_buf);
   rozofs_bt_inode_free_working_ctx(attr_working_ctx_p);
} 


/**
**_______________________________________________________________________________________
*/

void rozofs_bt_process_getattr_continue(rozofs_bt_attr_ctx_t *attr_working_ctx_p)  
{

   rozo_batch_hdr_t *hdr_p;
   rozo_attr_cmd_t *cmd_p;
   int i;
   int ret;
   rozofs_bt_rsp_buf *response_p;

   response_p =  &attr_working_ctx_p->response; 


   hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(attr_working_ctx_p->recv_buf); 
   cmd_p = (rozo_attr_cmd_t*)(hdr_p+1);
   info("rozofs_bt_process_getattr_continue : %d \n",attr_working_ctx_p->nb_commands);

   for (i = attr_working_ctx_p->nb_commands; i <hdr_p->nb_commands; i++,cmd_p++)
   {
      ret = rozofs_bt_process_get_inode_attr(cmd_p,attr_working_ctx_p);
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
   // info("message send socket : %d %d: %s\n",socket_id,ret,strerror(errno));
   rozofs_bt_free_receive_buf(attr_working_ctx_p->recv_buf);
   rozofs_bt_inode_free_working_ctx(attr_working_ctx_p);

}


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
  
  if (errno == 0)
  {
    image_p->mtime = msg_p->rsp.read_trk_rsp.rsp.mtime;
    list_for_each_forward_safe(p,n,pending_head) {
        work_p = (rozofs_bt_attr_ctx_t *) list_entry(p, rozofs_bt_attr_ctx_t, pending_list);
	list_remove(&work_p->pending_list);
	rozofs_bt_process_getattr_continue(work_p);
    }
  }
  else
  {  
#warning need to deal with read error on tracking files  
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
   // info("message send socket : %d %d: %s\n",socket_id,ret,strerror(errno));
   rozofs_bt_free_receive_buf(recv_buf);   
}   


/*
**_________________________________________________
*/
/*
**  BATCH    T H R E A D
*/

void *rozofs_bt_inode_thread(void *arg) 
{
  rozofs_bt_thread_ctx_t * ctx_p = (rozofs_bt_thread_ctx_t*)arg;
   void   *msg_p;
   rozo_batch_hdr_t *hdr_p;
   int socket_id;
   int prio;
   
  uma_dbg_thread_add_self("Batch_ino");
       info("FDL SENDSOCK TH %p %d",ctx_p,  ctx_p->sendSocket); 

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
    info("FDL command with priority %d",prio);
    switch (prio)
    {
      case PRIO_CMD:
	 hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(msg_p); 
	 socket_id = (int)hdr_p->private;
	 switch (hdr_p->opcode) {

	   case ROZO_BATCH_GETATTR:
             // info("TH ROZO_BATCH_GETATTR received socket %d",socket_id);
             rozofs_bt_process_getattr(msg_p,socket_id);
             break;

	   default:
             fatal(" unexpected opcode : %d\n",hdr_p->opcode);
             exit(0);       
	 }
	 break;

      case PRIO_RSP:
	info("FDL Read done!!");
	rozofs_bt_process_trk_file_read_response((rozofs_bt_attr_ctx_t*)msg_p);
	break;    

      default:
	info("Unpexected!!");
	break;    
    }
  }
}

/**
**_________________________________________________________________________________________
*/



void rozofs_bt_inode_rcu_check_tracking_file_update_th()
{

  list_t             * p;
  list_t             * n;
  int i;
  rozofs_bt_tracking_cache_t *image_p;
  
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
  
  for (i = 0; i < rozofs_bt_rcu_number_of_tracking_files; i++)
  {
    image_p = rozofs_bt_inode_rcu_tb_p[i];
    rozofs_bt_check_tracking_file_update_local(image_p);
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
   
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    sprintf(name,"TRK_READER#%d",thread_ctx_p->thread_idx);
    
    // Set the frequency of calls
    struct timespec ts = {ROZOFS_BT_RCU_PTHREAD_FREQUENCY_SEC, 0};

    uma_dbg_thread_add_self("bt_rcu");
    for (;;) {
        nanosleep(&ts, NULL);
         rozofs_bt_inode_rcu_check_tracking_file_update_th();     
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
int rozofs_bt_rcu_thread_create()
{
    int err;
    pthread_attr_t             attr;    
    int i;
    rozofs_bt_thread_ctx_t *p;
    
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
#warning TESTING ONLY
   for (i = 0; i < 1; i++,p++)
   {
    p->thread_idx = i;
   err = pthread_create(&p->thrdId,&attr,rozofs_bt_rcu_inode_polling_thread,p);
   if (err != 0) {
     fatal("rozofs_bt_rcu_thread_create pthread_create() %s", strerror(errno));
     return -1;
   } 
   }
   return 0; 
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
     info("FDL SENDSOCK %p %d",thread_ctx_p, thread_ctx_p->sendSocket); 
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

    @retval 0 on success
    
    @retval -1 on error
*/
int rozofs_bt_inode_init(int queue_depth)
{
    /* Initialize list and htables for inode_entries */
    list_init(&rozofs_bt_tracking_list);
    pthread_rwlock_init(&rozofs_bt_inode_lock, NULL);
    
    htable_initialize(&htable_bt_inode, INODE_BT_HSIZE, tracking_hash, tracking_cmp);
    hash_tracking_collisions_count = 0;
    hash_tracking_cur_collisions = 0;
    hash_tracking_max_collisions = 0;
    rozofs_tracking_file_count = 0;
    rozofs_bt_eid = -1;  /**< reference of the export */
    rozofs_bt_export_path_p = NULL; 
    /*
    ** Only one thread is supported
    */ 
    rozofs_bt_inode_thread_create(1,queue_depth);  
    rozofs_bt_rcu_thread_create();
    /*
    ** start the tracking file client readers
    */
    rozofs_bt_trk_cli_reader_thread_create(2);
    
    return 0;
}
   
