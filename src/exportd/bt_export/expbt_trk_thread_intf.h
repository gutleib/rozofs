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
 
#ifndef EXPBT_TRK_THREAD_INTF_H
#define EXPBT_TRK_THREAD_INTF_H

#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <errno.h>
#include <rozofs/rozofs.h>
#include "config.h"
#include <rozofs/common/log.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/rozofs_socket_family.h>
#include <rozofs/core/rozofs_queue.h>
#include <rozofs/core/rozofs_rpc_non_blocking_generic_srv.h>
#include <rozofs/rdma/rozofs_rdma.h>


#define EXPBT_MAX_TRK_THREADS 32

typedef struct _rozofs_disk_thread_stat_t {
  uint64_t            read_count;
  uint64_t            read_Byte_count;
  uint64_t            read_error;
  uint64_t            read_enoent;
  uint64_t            read_time;
  
  uint64_t            check_count;
  uint64_t            check_error;
  uint64_t            check_enoent;  
  uint64_t            check_time;  

  uint64_t            load_dentry_count;
  uint64_t            load_dentry_error;
  uint64_t            load_dentry_enoent;  
  uint64_t            load_dentry_time;  

  uint64_t            unk_count;

} expbt_trk_thread_stat_t;
/*
** Disk thread context
*/
typedef struct _rozofs_disk_thread_ctx_t
{
  pthread_t                    thrdId; /* of disk thread */
  
  int                          thread_idx;
  rozofs_queue_t               cmdring;
  char                       * hostname;  
  int                          sendSocket;

  expbt_trk_thread_stat_t    stat;
} expbt_trk_thread_ctx_t;


/*
**__________________________________________________________________________
*/
/**
*   That function is intended to be called when the storio runs out of TCP buffer
    We wait for at least 16 buffers before re-attempting to allocated a buffer for
    a TCP receive.
    That function is called by storio_north_RcvAllocBufCallBack
    
    @param none: 
    
    @retval none;
*/
void af_unix_trk_pool_socket_on_receive_buffer_depletion();


/*__________________________________________________________________________
* Initialize the disk thread interface
*
* @param hostname    storio hostname (for tests)
  @param instance_id: expbt instance identifier
* @param nb_threads  Number of threads that can process the disk requests
  @param cmdring_size: size of the ring command (depends on the number of received buffers.
  *
*  @retval 0 on success -1 in case of error
*/
int expbt_trk_thread_intf_create(char * hostname, int instance_id,int nb_threads,int cmdring_size);

/*
**__________________________________________________________________________
*/
/**
* 
   Send back the result of an operation done by the tracking file threads
   
   The response message has been formatted by the thread, the main thread has just to send it back to the source
   
   @param rpc_ctx_p: pointer to the RPC context
   
   @retval none
*/

int expbt_send2mainthread(void *rpc_ctx_p);

/*
**_________________________________________________________________
*/ 
/**
  Post a request for tracking file check towards the command ring of the tracking threads
  
  @param arg: pointer to the header of the request
  @param  rozorpc_srv_ctx_p : pointer to the RPC context that contains the received buffer and the command to execute
  
  @retval none
*/
void exp_bt_trk_check_post2thread(void *arg, rozorpc_srv_ctx_t *rozorpc_srv_ctx_p);


/*
**_________________________________________________________________
*/
/**
   Read the content of a tracking file
   That function is called from a tracking file reading thread
   It reads the tracking file found in the request received from the client and format the response ready to send back to the main thread
   
   @param rozorpc_srv_ctx_p: RPC context associated with the request
   
   @retval none
*/
void expb_trk_read_in_thread(rozorpc_srv_ctx_t *rozorpc_srv_ctx_p);


/*
**__________________________________________________________________________
*/
/**
  Processes a disk response

   Called from the socket controller when there is a response from a disk thread
   the response is either for a disk read or write
    
  @param rozorpc_srv_ctx_p: pointer RPC context that contains the command & the response
 
  @retval :none
*/
void af_unix_trk_response(rozorpc_srv_ctx_t *rozorpc_srv_ctx_p);

/*
**_________________________________________________________________
*/ 
/**
  Post a request for tracking file read towards the command ring of the tracking threads
  
  @param arg: pointer to the header of the request
  @param  rozorpc_srv_ctx_p : pointer to the RPC context that contains the received buffer and the command to execute
  
  @retval none
*/
void expbt_trk_read_post2thread(void *arg, rozorpc_srv_ctx_t *rozorpc_srv_ctx_p);
/*
**_________________________________________________________________
*/ 
/**
  Post a request for tracking file check towards the command ring of the tracking threads
  
  @param arg: pointer to the header of the request
  @param  rozorpc_srv_ctx_p : pointer to the RPC context that contains the received buffer and the command to execute
  
  @retval none
*/
void expbt_trk_check_post2thread(void *arg, rozorpc_srv_ctx_t *rozorpc_srv_ctx_p);

/*
**_________________________________________________________________
*/
/**
   check the content of a tracking file: it tracks the mtime of the tracking file
   That function is called from a tracking file reading thread
   It reads the tracking file found in the request received from the client and format the response ready to send back to the main thread
   
   @param rozorpc_srv_ctx_p: RPC context associated with the request
   
   @retval none
*/
void expb_trk_check_in_thread(rozorpc_srv_ctx_t *rozorpc_srv_ctx_p);
/*
**____________________________________________________________________________
*/

/**
*  Get the pathname of the export according to the export identifier

   @param eid: export identifier
   
   @retval <> NULL: pointer to the export pathname
   @retval NULL (no path, see errno for details )
*/

char* expbt_get_export_path(uint32_t eid);
/*
**_________________________________________________________________
*/ 
/**
  Post a request for dirent files loading to the command ring of the tracking threads
  
  @param arg: pointer to the header of the request
  @param  rozorpc_srv_ctx_p : pointer to the RPC context that contains the received buffer and the command to execute
  
  @retval none
*/
void expbt_dirent_load_post2thread(void *arg, rozorpc_srv_ctx_t *rozorpc_srv_ctx_p);

/*
**_________________________________________________________________
*/
/**
   Read the content of a tracking file
   That function is called from a tracking file reading thread
   It reads the tracking file found in the request received from the client and format the response ready to send back to the main thread
   
   @param rozorpc_srv_ctx_p: RPC context associated with the request
   
   @retval none
*/
void expbt_load_dentry_in_thread(rozorpc_srv_ctx_t *rozorpc_srv_ctx_p);
#endif
