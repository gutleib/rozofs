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
 
 
#ifndef ROZOFS_RPC_NON_BLOCKING_GENERIC_H
#define ROZOFS_RPC_NON_BLOCKING_GENERIC_H

#ifdef __cplusplus
extern "C" {
#endif /*__cplusplus*/

#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/core/ruc_sockCtl_api.h>
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/core/north_lbg_api.h>
#include <rpc/rpc.h>


#define ROZOFS_RPC_GENERIC_MAX_REQ_CTX  256  /**< max generic rpc context  */


/**
*  Macro rpc stats: define the counter to update
*/
#define DEFINE_RPC_GENERIC_PROBE(the_probe)\
   (&gprofiler.the_probe[0])

/**
*  Macro rpc stats: start non blocking case
*/
#if 0

#define START_PROFILING_RPC_GENERIC(buffer,probe)\
{ \
  unsigned long long time;\
  struct timeval     timeDay;  \
  if (buffer != NULL)\
  { \
   buffer->profiler_probe = probe;\
   if (buffer->profiler_probe != NULL)\
   { \
     probe[P_COUNT]++;\
     gettimeofday(&timeDay,(struct timezone *)0); \
     time = MICROLONG(timeDay); \
     buffer->profiler_time =(uint64_t)time ;\
   }\
  }\
}

/**
*  Macro METADATA stop non blocking case
*/

#define STOP_PROFILING_RPC_GENERIC(buffer)\
{ \
  unsigned long long timeAfter;\
  struct timeval     timeDay;  \
  if (buffer->profiler_probe != NULL)\
  { \
    gettimeofday(&timeDay,(struct timezone *)0); \
    timeAfter = MICROLONG(timeDay); \
    buffer->profiler_probe[P_ELAPSE] += (timeAfter-buffer->profiler_time); \
    buffer->profiler_probe = NULL;\
  }\
}
#else 
#define START_PROFILING_RPC_GENERIC(buffer,probe)
#define STOP_PROFILING_RPC_GENERIC(buffer)
#endif




/**
* structure with transaction module for rpc in non-blocking mode (generic API
*/
typedef struct _rozofs_rpc_ctx_t
{
  ruc_obj_desc_t link;
  uint32_t            index;         /**< Index of the MS */
  uint32_t            free;          /**< Is the context free or allocated TRUE/FALSE */
  uint32_t            integrity;     /**< the value of this field is incremented at  each MS ctx allocation  */
  
  sys_recv_pf_t    response_cbk;     /**< callback function associated with the response of the root transaction */
  xdrproc_t        xdr_result;       /**< rpc decoding procedure                                                 */
  void             *user_ref;        /**< object index                                                           */
  int              ret_len;          /**< length of the structure for decoding the returned parameters           */
  void            *ret_p;            /**< pointer to the structure used for storing the decoded response         */
  uint64_t *profiler_probe;          /**< pointer to the profiler counter */
  uint64_t profiler_time;            /**< profiler timestamp */
} rozofs_rpc_ctx_t;



/*
**______________________________________________________________________________
*/
/**
* ROZOFS Generic RPC Request transaction in non-blocking mode

 That service initiates RPC call towards the destination referenced by its associated load balancing group
 WHen the transaction is started, the application will received the response thanks the provided callback
 
 The first parameter is a user dependent reference and the second pointer is the pointer to the decoded
 area.
 In case of decoding error, transmission error, the second pointer is NULL and errno is asserted with the
 error.
 
 The array provided for decoding the response might be a static variable within  the user context or
 can be an allocated array. If that array has be allocated by the application it is up to the application
 to release it

 @param lbg_id     : reference of the load balancing group of the exportd
 @param prog       : program
 @param vers       : program version
 @param opcode     : metadata opcode
 @param probe     : statistics counter associated with opcode (might be NULL)
 @param encode_fct : encoding function
 @param msg2encode_p     : pointer to the message to encode
 @param decode_fct  : xdr function for message decoding
 @param ret: pointer to the array that is used for message decoding
 @parem ret_len : length of the array used for decoding
 @param recv_cbk   : receive callback function (for interpretation of the rpc result
 @param ctx_p      : pointer to the user context
 
 @retval 0 on success;
 @retval -1 on error,, errno contains the cause
 */
int rozofs_rpc_non_blocking_req_send (int lbg_id,uint32_t prog,uint32_t vers,
                                      int opcode,uint64_t *probe,
                                       xdrproc_t encode_fct,void *msg2encode_p,
                                       xdrproc_t decode_fct,void *ret,int ret_len,
                                       sys_recv_pf_t recv_cbk,void *ctx_p) ;


/*
**__________________________________________________________________________
*/
/**
* release a read/write context that has been use for either a read or write operation

  @param : pointer to the context
  
  @retval <>NULL pointer to the allocated context
  @retval NULL out of free context
*/
void rozofs_rpc_req_free(rozofs_rpc_ctx_t *ctx_p);

/*
**__________________________________________________________________________
*/
/**
   rozofs_rpc_module_init

  create the Transaction context pool


@retval   : RUC_OK : done
@retval          RUC_NOK : out of memory
*/
uint32_t rozofs_rpc_module_init();


#ifdef __cplusplus
}
#endif /*__cplusplus */




#endif
