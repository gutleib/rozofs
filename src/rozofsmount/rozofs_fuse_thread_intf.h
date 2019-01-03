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
 
#ifndef ROZOFS_FUSE_THREAD_INTF_H
#define ROZOFS_FUSE_THREAD_INTF_H

#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <errno.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/rozofs_socket_family.h>
#include <rozofs/rpc/storcli_proto.h>
#include "rozofs_fuse_api.h"
#include "rozofs_sharedmem.h"

#define ROZOFS_MAX_FUSE_THREADS 8
#define ROZOFS_MAX_FUSE_WR_THREADS 8
typedef struct _rozofs_fuse_thread_stat_t {
  
  uint64_t            write_count;
  uint64_t            write_Byte_count;
  uint64_t            write_time;

} rozofs_fuse_thread_stat_t;
/*
** Fuse thread context
*/
typedef struct _rozofs_fuse_thread_ctx_t
{
  pthread_t                    thrdId; /* of fuse thread */
  int                          thread_idx;
  char                       * hostname;  
  int                          sendSocket;
  rozofs_fuse_thread_stat_t    stat;
} rozofs_fuse_thread_ctx_t;

extern rozofs_fuse_thread_ctx_t rozofs_fuse_thread_ctx_tb[];
extern rozofs_fuse_thread_ctx_t rozofs_fuse_wr_thread_ctx_tb[];

/**
* Message sent/received in the af_unix disk sockets
*/

typedef enum _rozofs_fuse_thread_request_e {
  ROZOFS_FUSE_REPLY_BUF=1,
  ROZOFS_FUSE_WRITE_BUF,
  ROZOFS_FUSE_WRITE_BUF_MULTI,
  ROZOFS_FUSE_THREAD_MAX_OPCODE
} rozofs_fuse_thread_request_e;

typedef struct _rozofs_fuse_thread_msg_t
{
  uint32_t            opcode;
  uint32_t            status;
  uint64_t            timeStart;
  uint64_t            timeResp;
  uint32_t            size;       /**< size of the buffer   */
  fuse_req_t          req ;       /**< fuse initial request   */
  void                *payload;   /**< pointer to the buffer payload   */
  void                *bufRef;   /**< shared memory buffer reference   */
} rozofs_fuse_thread_msg_t;


/*
** message used by rozofs_write
*/
typedef struct _rozofs_fuse_wr_thread_msg_t
{
  uint32_t            opcode;
  uint32_t            status;
  int                 errval;           /**< errno code  */
  uint64_t            timeStart;
  uint64_t            timeResp;
  uint32_t            size;              /**< size of the buffer   */
  int                 rpc_opcode;        /**< storcli rpc opcode */
  xdrproc_t           encode_fct;
  void                *rozofs_tx_ctx_p;  /**< pointer to the transaction context */
  int                 storcli_idx;
  uint32_t            rm_xid;     /**< xid of the rpc transaction  */
  storcli_write_arg_t args;       /**< rpc arguments          */
  void                *rozofs_fuse_cur_rcv_buf;
  
} rozofs_fuse_wr_thread_msg_t;

/*
** Use the same response thread for read and write
*/
typedef union
{
   rozofs_fuse_thread_msg_t read;
   rozofs_fuse_wr_thread_msg_t write;
} rozofs_fuse_rd_wr_thread_msg_u;

/*__________________________________________________________________________
* Initialize the fuse thread interface
*
* @param hostname    storio hostname (for simulation)
* @param nb_threads  Number of threads that can process the fuse replies
*
*  @retval 0 on success -1 in case of error
*/
int rozofs_fuse_thread_intf_create(char * hostname, int instance_id, int nb_threads) ;

/*__________________________________________________________________________
*/
/**
*  Send a reply buffer to a fuse thread
*
* @param fidCtx     FID context
* @param rpcCtx     pointer to the generic rpc context
* @param timeStart  time stamp when the request has been decoded
*
* @retval 0 on success -1 in case of error
*  
*/
int rozofs_thread_fuse_reply_buf(fuse_req_t req,
                                 char *payload,
				 uint32_t size,
				 void *bufRef,
				 uint64_t       timeStart);


/*__________________________________________________________________________
*/
/**
*  Send a write buffer to a write fuse thread
*
* @param msg_thread_p: pointer to the message to send  
*
* @retval 0 on success -1 in case of error
*  
*/
int rozofs_sendto_wr_fuse_thread(rozofs_fuse_wr_thread_msg_t *msg_thread_p);
/*
**__________________________________________________________________________
*/
/**
*  Thar API is intended to be used by a fuse thread for sending back a 
   fuse reply buffer response towards the main thread in order to release the 
   shared memory buffer
   
   @param thread_ctx_p: pointer to the thread context (contains the thread source socket )
   @param msg: pointer to the message that contains the disk response
   @param status : status of the disk operation
   
   @retval none
*/
void rozofs_fuse_th_send_response (rozofs_fuse_thread_ctx_t *thread_ctx_p, rozofs_fuse_thread_msg_t * msg, int status);
/*
**__________________________________________________________________________
*/
/**
*  Thar API is intended to be used by a fuse write thread for sending back a 
   write prepare response  towards the main thread
   
   @param thread_ctx_p: pointer to the thread context (contains the thread source socket )
   
   @retval none
*/
void rozofs_fuse_wr_th_send_response (rozofs_fuse_thread_ctx_t *thread_ctx_p, rozofs_fuse_wr_thread_msg_t * msg);

#endif
