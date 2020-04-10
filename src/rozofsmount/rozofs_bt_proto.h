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
#ifndef _ROZOFS_BT_PROTO_H
#define _ROZOFS_BT_PROTO_H
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <fcntl.h> 
#include <sys/un.h>             
#include <errno.h>  
#include "rozofs_bt_api.h"
#include "rozofs_bt_proto.h"
#include <rozofs/core/rozofs_queue.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/mattr.h>
#include <rozofs/core/ruc_buffer.h>

extern rozofs_queue_t rozofs_bt_rq_queue;
extern pthread_rwlock_t rozofs_bt_shm_lock;


int rozofs_bt_init(uint16_t instance,int eid,int nb_contexts);
int rozofs_bt_north_interface_init(uint32_t eid,uint16_t rozofsmount_instance,int cmd_buf_count);

typedef struct _rozofs_bt_io_entry_t
{
   uint64_t next_off;  /**< next offset to read   */
   int     status;    /**< io status             */
   int     errcode;   /**< errno when status is negative */
   uint8_t pending_request;  /**< number of pending request for that context */
   uint8_t cmd_idx;          /**< index within the batch IO request          */
   void    *inode_p;        /**< pointer to the inode context                */
} rozofs_bt_io_entry_t;


typedef struct _rozofs_bt_ioctx_t
{
   void *io_batch_p;   /**< pointer to the io batch command (payload of ruc_buffer */
   void *io_ruc_buffer; /**< pointer to the ruc_buffer that contains the commands */
   void *my_ruc_buf_p;  /**< pointer to the head of the ruc buffer (needed for release */
   int  socket;         /**< reference of the client socket                       */
   uint8_t cur_cmd_idx; /**< index of the last command for which a request has been submitted */
   uint8_t global_pending_request;  /**< number of pending request  */
   rozofs_bt_io_entry_t io_entry[ROZOFS_MAX_BATCH_CMD];
   rozofs_memreg_t *mem_p[ROZOFS_MAX_BATCH_CMD];
} rozofs_bt_ioctx_t;



/**
* batch request context
*
*  Caution: need to double checked that the pointer are pointer
* either to the fuse message or a context allocated by fuse but not
* a pointer on a local variable
*/
 /**
 * Must be the same as sys_recv_pf_t
 */
 typedef void (*rozofs_bt_end_tx_recv_pf_t)(void *tx_ctx,void *usr_param);

typedef struct _rozofs_bt_io_working_ctx_t
{
   ruc_obj_desc_t link;   /**< uwe to queue to context on the file_t structure */
   void  *ruc_buf_p;      /**< pointer to ruc_buffer head  */
   void  *io_ctx_p;       /**< pointer to the IO context used for read/write    */
   void  *inode_p;
   uint8_t *remote_buf;   /**< remote address where data must be copied */
   uint64_t ino;          /**< lower part of the rozofs inode */
   off_t off;
   size_t size;
   uint16_t cmd_idx;
   uint32_t rozofs_shm_ref;   /**< reference of the shared memory */
   uint64_t time;
   rozofs_bt_end_tx_recv_pf_t saved_cbk_of_tx_multiple;   /**< re-save the initial callback when read or write multiple is used */ 
   rozofs_bt_end_tx_recv_pf_t proc_end_tx_cbk;   /**< callback that must be call at end of transaction (mainly used by write/flush and close */ 
   rozofs_bt_end_tx_recv_pf_t proc_final_cbk;   /**< callback that must be called once a read call has been processed */ 
   void     *shared_buf_ref;             /**< reference of the shared buffer (used for STORCLI READ */
   int       trc_idx;                    /**< trace index */
   uint32_t  multiple_pending;           /**< number of transaction that are pending: read or write multiple */
   int       multiple_errno;             /**< multiple errno                                                 */ 
   int       use_page_cache;             /**< assert to 1 if the storcli must use the page cache, 0 otherwise) */
   
 } rozofs_bt_io_working_ctx_t;
/*
**_________________________________________________________________________________________________
*/
/**
   Register a shared memory
   
   @param name: name of the shared memory
   @param remote_addr: remote address of the owner
   @param owner: reference of the socket owner
   
   @retval >= 0 : rozofs key for using the shared memory
   @retval < 0: error on registration
   
*/
int rozofs_shm_register(char *name,void *remote_addr,int owner,size_t length,int fd);

/*
**_________________________________________________________________________________________________
*/
/**
   Create or map a shared memory
   
   @param name: name of the shared memory
   @param length : length of the shared memory (only for create)
   @param create: assert to 1 for shared memory creation
   @param fd: pointer to the file descriptor of the shared memory

   @retval <> NULL: pointer to the mapped shared memory
   @retval NULL (error: see errno for details
*/   
void *rozofs_shm_create(rozofs_memreg_t *mem_p,int create);


typedef struct _rozofs_bt_msg_th_t
{
   uint32_t opcode; /**< opcode */
   int  status;    /**< status of the operation */
   int  errcode;   /**< error code when status is negative */
   void *work_p;  /**< pointer to a working context */
   void *cmd;     /**< pointer to the ruc buffer that contains the command */
} rozofs_bt_thread_msg_t;

extern rozofs_bt_thread_msg_t *rozofs_bt_req_pool_p;
extern void *rozofs_bt_buffer_pool_p;  /**< reference of the read/write buffer pool */
extern void *rozofs_bt_rd_buffer_pool_p;  /**< reference of the read/write buffer pool */
extern void *rozofs_bt_io_storcli_pool_p;  /**< reference of the I/O context used for storcli same fusectx in terms of usage */
extern uint32_t rozofs_bt_cmd_ring_idx;
extern uint32_t rozofs_bt_cmd_ring_size;
extern rozofs_memreg_t *rozofs_shm_tb;
int rozofs_shm_count;

/*__________________________________________________________________________
*/
/*
**  Allocate an entry to submit a job to the mojette threads

  @param none
  
  @retval: pmointer to the context to use
*/
static inline rozofs_bt_thread_msg_t *rozofs_bt_th_req_get_next_slot()
{
  rozofs_bt_thread_msg_t *p = &rozofs_bt_req_pool_p[rozofs_bt_cmd_ring_idx];
  rozofs_bt_cmd_ring_idx++;
  if (rozofs_bt_cmd_ring_idx == rozofs_bt_cmd_ring_size) rozofs_bt_cmd_ring_idx = 0;
  return p;
}

/*
** Batch thread context
*/
#define ROZOFS_BT_MAX_THREADS 4

typedef struct _rozofs_bt_thread_ctx_t
{
  pthread_t                    thrdId; /* of disk thread */
  int                          thread_idx;
  int                          sendSocket;
  void                         *queue_req;
  void                         *queue_rsp; 
  void                         *private;   /**< private data of a thread */ 
  void                         *thread_private; /**< private data of a thread */
//  rozofs_disk_thread_stat_t    stat;
} rozofs_bt_thread_ctx_t;

extern rozofs_bt_thread_ctx_t rozofs_bt_thread_ctx_tb[];

void rozofs_bt_free_receive_buf(void *buf);

/*
**__________________________________________________________________________
*/
/**
   allocate an io_cmd buffer used by the batch application
   
   @param : none
   
   @retval : <> NULL: pointer to a ruc_buffer
   @retval: NULL: out of buffer
   
*/
void *rozofs_bt_buf_io_cmd_alloc();
/*
**__________________________________________________________________________
*/
/**
   release an io_cmd buffer used by the batch application
   
   @param bt_buf_io_cmd_p: pointer to the ruc_buffer
   
   @retval : none
*/
void rozofs_bt_buf_io_cmd_release(void *bt_buf_io_cmd_p);


/**
**_______________________________________________________________________________________
*/

/**
*   Creation of a shared memory for a client (ROZO_BATCH_MEMCREATE)

    @param recv_buf: buffer that contains the command
    @paral socket_id: file descriptor used by the client on the AF_UNIX socket created for processing the requests
    
    @retval none
*/
void rozofs_bt_process_memcreate(void *recv_buf,int socket_id);
/**
**_______________________________________________________________________________________
*/

/**
*   registration of the address used on the client side (ROZO_BATCH_MEMADDR_REG)

    @param recv_buf: buffer that contains the command
    @paral socket_id: file descriptor used by the client on the AF_UNIX socket created for processing the requests
    
    @retval none
*/
void rozofs_bt_process_mem_register_addr(void *recv_buf,int socket_id);

/*__________________________________________________________________________
* Initialize the disk thread interface
*
* @param hostname    hostname (for tests)
* @param nb_threads  Number of threads that can process the read or write requests
*
*  @retval 0 on success -1 in case of error
*/
int rozofs_bt_thread_intf_create(char * hostname, int instance_id, int nb_threads);

/**
**_______________________________________________________________________________________
*/
/**
  Process a batch init command : registration of the eid and export path
  
  @param recv_buf: pointer to the buffer that contains the command
  @param socket_id: file descriptor used for sending back the response
  
  @retval none
*/
void rozofs_bt_process_init(void *recv_buf,int socket_id);

void rozofs_bt_process_getattr_from_main_thread(void *recv_buf,int socket_id);
/*
**_________________________________________________________________________________________________
*/
/**
*   Init of the array that maintain the shared memory that have been registered with rozofsmount

    @param rozofsmount_id: rozofsmount instance
    @param create: assert to 1 for creation of the shared memory, 0 for just a registration
    
    @retval 0 on success
    @retval -1 on error (see errno for details)
*/
int rozofs_shm_init(int rozofsmount_id,int create);


void rozofs_bt_process_memcreate_from_main_thread(void *recv_buf,int socket_id);
void rozofs_bt_process_memreg_from_main_thread(void *recv_buf,int socket_id);

/**
**_______________________________________________________________________________________
*/
/**
*   Registration of a memory that has been created by the client (ROZO_BATCH_MEMREG)

    @param recv_buf: buffer that contains the command
    @paral socket_id: file descriptor used by the client on the AF_UNIX socket created for processing the requests
    
    @retval none
*/
void rozofs_bt_process_memreg(void *recv_buf,int socket_id);

/*
**_________________________________________________________________________________________________
*/
/**
   Register a shared memory on the storcli side
   
   @param rozofs_key: key of the shared memory
   @param name: name of the shared memory
   @param remote_addr: remote address of the owner
   @param length: length of the shared memory
   
   @retval >= 0 : rozofs key for using the shared memory
   @retval < 0: error on registration
   
*/
int rozofs_shm_stc_register(rozofs_mem_key rozofs_key,char *name,void *remote_addr,size_t length);
/*
**_________________________________________________________________________________________________
*/
/*
**  Get the local address to use for data transfert

    @param mem_p: pointer to the shared memory descriptor
    @param client_addr: pointer to the remote address
    @param size: size to read
    
    @retval: pointer to the local address
*/
static inline uint8_t *rozofs_bt_map_to_local(rozofs_memreg_t  *mem_p,void *client_addr,uint64_t size)
{

   uint8_t *local_addr;
   uint64_t offset;

   offset = (uint64_t)((uint8_t*) client_addr - (uint8_t*)mem_p->remote_addr);
   if ((offset+size) > mem_p->length)
   {
      errno = EFAULT;
      return NULL;
   }
   local_addr = mem_p->addr;
   return (local_addr+offset);      

}

void rozofs_bt_iosubmit_tracking_file_read_cbk(rozofs_bt_thread_msg_t *msg_p);
#endif
