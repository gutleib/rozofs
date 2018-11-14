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
 
#ifndef STORAGED_SUB_THREAD_INTF_H
#define STORAGED_SUB_THREAD_INTF_H

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
#include <rozofs/core/rozofs_rpc_non_blocking_generic_srv.h>
#include <rozofs/rpc/mproto.h>

#define STORAGED_MAX_SUB_THREADS  16

#include "storage.h"

typedef struct _storaged_sub_thread_stat_t {
  uint64_t            remove_count;
  uint64_t            remove_errors;
  uint64_t            remove_time;
  
  uint64_t            list_bins_count;
  uint64_t            list_bins_errors;
  uint64_t            list_bins_time;

  uint64_t            size_count;
  uint64_t            size_errors;
  uint64_t            size_time;
  
  uint64_t            locate_count;
  uint64_t            locate_errors;
  uint64_t            locate_time;
  
} storaged_sub_thread_stat_t;
/*
** Disk thread context
*/
typedef struct _storaged_sub_thread_ctx_t
{
  pthread_t                    thrdId; /* of disk thread */
  int                          thread_idx;
  char                       * hostname;   
  int                          sendSocket;
  storaged_sub_thread_stat_t   stat;
} storaged_sub_thread_ctx_t;

extern storaged_sub_thread_ctx_t storaged_sub_thread_ctx_tb[];
extern int                       storaged_sub_pending_req_count;
extern int                       storaged_sub_pending_req_max_count;
/**
* Message sent/received in the af_unix disk sockets
*/


typedef struct _storaged_sub_thread_msg_t
{
  uint32_t            msg_len;
  uint32_t            opcode;
  uint32_t            status;
  uint32_t            transaction_id;
  uint64_t            timeStart;
  uint64_t            size;
  rozorpc_srv_ctx_t * rpcCtx;
  storage_t         * st;       
} storaged_sub_thread_msg_t;

/*__________________________________________________________________________
* Initialize the disk thread interface
*
* @param hostname    storio hostname (for simulation)
* @param nb_threads  Number of threads that can process the disk requests
* @param nb_buffer   Number of buffer for sending and number of receiving buffer
*
*  @retval 0 on success -1 in case of error
*/
int storaged_sub_thread_intf_create(char * hostname,int nb_threads);

/*__________________________________________________________________________
*/
/**
*  Send a disk request to the disk threads
*
* @param opcode     the request operation code
* @param working_ctx     pointer to the generic rpc context
* @param timeStart  time stamp when the request has been decoded
*
* @retval 0 on success -1 in case of error
*  
*/
int storaged_sub_thread_intf_send_req(uint32_t                      opcode, 
                                 rozorpc_srv_ctx_t            * rpcCtx,
				 storage_t                    * st,
				 uint64_t                       timeStart) ;

/*
**__________________________________________________________________________
*/
/**
*  Thar API is intended to be used by a disk thread for sending back a 
   disk response (read/write or truncate) towards the main thread
   
   @param thread_ctx_p: pointer to the thread context (contains the thread source socket )
   @param msg: pointer to the message that contains the disk response
   @param status : status of the disk operation
   
   @retval none
*/
void storaged_sub_thread_intf_send_response (storaged_sub_thread_ctx_t *thread_ctx_p, storaged_sub_thread_msg_t * msg, int status);

#endif
