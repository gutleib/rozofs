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

#ifndef ROZORPC_SRV_EXPORT_H
#define ROZORPC_SRV_EXPORT_H


#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <rozofs/core/ruc_list.h>
#include <errno.h>
#include <rozofs/rozofs.h>
#include "config.h"
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include <rpc/rpc.h>
#include <rozofs/common/profile.h>
#include <rozofs/core/rozofs_tx_common.h>



/**
*  ROZOFS RPC generic Resource configurationj
*/
#define ROZORPC_SRV_CNF_NO_BUF_CNT          1
#define ROZORPC_SRV_CNF_NO_BUF_SZ           1

/**
*  Macro stats start non blocking case
*/
#define START_PROFILING_ROZORPC_SRV(buffer,the_probe)\
{ \
  /*unsigned long long time;*/\
  /*struct timeval     timeDay;*/  \
  if (buffer != NULL)\
  { \
   gprofiler.the_probe[P_COUNT]++;\
   buffer->profiler_probe = &gprofiler.the_probe[0]; \
   /*gettimeofday(&timeDay,(struct timezone *)0); */ \
   /*time = MICROLONG(timeDay);*/ \
   /*buffer->profiler_time =(uint64_t)time ;*/\
  }\
}

/**
*  Macro stats stop non blocking case
*/
#define STOP_PROFILING_ROZORPC_SRV(buffer)\
{ \
  /*unsigned long long timeAfter;*/\
  /*struct timeval     timeDay;  */\
  if (buffer->profiler_probe != NULL)\
  { \
    /*gettimeofday(&timeDay,(struct timezone *)0); */ \
    /*timeAfter = MICROLONG(timeDay); */\
    /*buffer->profiler_probe[P_ELAPSE] += (timeAfter-buffer->profiler_time); */\
    buffer->profiler_probe = NULL;\
  }\
}

/**
*  North Interface
*/
#define ROZORPC_SRV_CTX_CNT   (32*2)  /**< context for processing either a read or write request from rozofsmount and internal read req */
#define ROZORPC_SRV_CTX_MIN_CNT 3 /**< minimum count to process a request from rozofsmount */
/**
* Buffer s associated with the reception of the load balancing group on north interface
*/
#define ROZORPC_SRV_NORTH_LBG_BUF_RECV_CNT   ROZORPC_SRV_CTX_CNT  /**< number of reception buffer to receive from rozofsmount */
#define ROZORPC_SRV_NORTH_LBG_BUF_RECV_SZ    (1024*38)  /**< max user data payload is 38K       */
/**
* Storcli buffer mangement configuration
*  INTERNAL_READ are small buffer used when a write request requires too trigger read of first and/or last block
*/
#define ROZORPC_SRV_NORTH_MOD_INTERNAL_READ_BUF_CNT   ROZORPC_SRV_CTX_CNT  /**< rozorpc_srv_north_small_buf_count  */
#define ROZORPC_SRV_NORTH_MOD_INTERNAL_READ_BUF_SZ   1024  /**< rozorpc_srv_north_small_buf_sz  */

#define ROZORPC_SRV_NORTH_MOD_XMIT_BUF_CNT   ROZORPC_SRV_CTX_CNT  /**< rozorpc_srv_north_large_buf_count  */
#define ROZORPC_SRV_NORTH_MOD_XMIT_BUF_SZ    ROZORPC_SRV_NORTH_LBG_BUF_RECV_SZ  /**< rozorpc_srv_north_large_buf_sz  */

#define ROZORPC_SRV_SOUTH_TX_XMIT_BUF_CNT   (ROZORPC_SRV_CTX_CNT)  /**< rozorpc_srv_south_large_buf_count  */
#define ROZORPC_SRV_SOUTH_TX_XMIT_BUF_SZ    (1024*8)                           /**< rozorpc_srv_south_large_buf_sz  */

/**
* configuartion of the resource of the transaction module
*
*  concerning the buffers, only reception buffer are required 
*/

#define ROZORPC_SRV_SOUTH_TX_CNT   (ROZORPC_SRV_CTX_CNT)  /**< number of transactions towards storaged  */
#define ROZORPC_SRV_SOUTH_TX_RECV_BUF_CNT   ROZORPC_SRV_SOUTH_TX_CNT  /**< number of recption buffers  */
#define ROZORPC_SRV_SOUTH_TX_RECV_BUF_SZ    ROZORPC_SRV_SOUTH_TX_XMIT_BUF_SZ  /**< buffer size  */

#define ROZORPC_SRV_NORTH_TX_BUF_CNT   0  /**< not needed for the case of storcli  */
#define ROZORPC_SRV_NORTH_TX_BUF_SZ    0  /**< buffer size  */


#define ROZORPC_SRV_MAX_RETRY   3  /**< max attempts for read or write on mstorage */




/**
* common context for export request received from rozofsmount
*/
typedef struct _rozorpc_srv_ctx_t
{
  ruc_obj_desc_t link;
  uint32_t            index;         /**< Index of the MS */
  uint32_t            free;          /**< Is the context free or allocated TRUE/FALSE */
  uint32_t            integrity;     /**< the value of this field is incremented at  each MS ctx allocation  */
  uint32_t            opcode;      /**< opcode associated with the request, when the key is not used the value is ROZORPC_SRV_NULL */
  uint32_t   src_transaction_id;  /**< transaction id of the source request                                       */
  uint32_t   rdma;                /**< assert to 1 when the context is used with RDMA protocol                    */
  uint32_t   client_lbg_id;       /**< reference of the lbg_id on client side (between storcli & storio) needed for full rdma */
  uint64_t            timestamp;
  int                 position;  /**< position of the last decoded arg in the request                 */
  void      *recv_buf;             /**< pointer to the receive buffer that carries the request        */
  uint32_t   socketRef;            /**< reference of the socket on which the answser must be sent     */
  sys_recv_pf_t    response_cbk;  /**< callback function associated with the response of the root transaction */
  xdrproc_t        xdr_result;
  xdrproc_t        xdr_result_internal;
  int        xdr_offset_in_req;   /**< offset oif the first applicative byte in a request -> in recv_buf  */  
  void      *xmitBuf;             /**< reference of the xmit buffer that will use for sending the response with an errcode       */
  void      *decoded_arg;          /**< buffer of the decoded argument */
  xdrproc_t  arg_decoder;          /**< procedure for decoding/freeing arguments */
  uint64_t *profiler_probe;       /**< pointer to the profiler counter */
  uint64_t profiler_time;        /**< profiler timestamp */
  list_t   list;                 /**< To chain the request somewhere */
} rozorpc_srv_ctx_t;

#define ROZORPC_SRV_START_NORTH_PROF(buffer)\
 { \
  unsigned long long time;\
  struct timeval     timeDay;  \
  uint64_t *gprofiler_fake = &gprofiler.null[0]; \
  gprofiler_fake += (buffer->opcode*(P_ELAPSE+1)); \
  gprofiler_fake[P_COUNT]++;\
  if (buffer != NULL)\
    {\
        /*gettimeofday(&timeDay,(struct timezone *)0); */ \
        time = MICROLONG(timeDay); \
        (buffer)->timestamp =time;\
    }\
}
/**
*  Macro METADATA stop non blocking case
*/
#define ROZORPC_SRV_STOP_NORTH_PROF(buffer)\
{ \
  unsigned long long timeAfter;\
  struct timeval     timeDay;  \
  uint64_t *gprofiler_fake = &gprofiler.null[0]; \
  gprofiler_fake += ((buffer)->opcode*(P_ELAPSE+1)); \
  if (buffer != NULL)\
  { \
    /*gettimeofday(&timeDay,(struct timezone *)0);*/  \
    timeAfter = MICROLONG(timeDay); \
    gprofiler_fake[P_ELAPSE] += (timeAfter-(buffer)->timestamp); \
  }\
}



#define ROZORPC_SRV_ERR_PROF(the_probe)\
 { \
  gprofiler.the_probe[P_COUNT]++;\
}





/**
* transaction statistics
*/
typedef enum 
{
  ROZORPC_SRV_SEND =0 ,
  ROZORPC_SRV_SEND_ERROR,
  ROZORPC_SRV_RECV_OK, 
  ROZORPC_SRV_RECV_OUT_SEQ,
  ROZORPC_SRV_ENCODING_ERROR,
  ROZORPC_SRV_DECODING_ERROR,
  ROZORPC_SRV_NO_CTX_ERROR,
  ROZORPC_SRV_NO_BUFFER_ERROR,
  ROZORPC_SRV_COUNTER_MAX
}rozorpc_srv_tx_stats_e;

extern uint64_t rozorpc_srv_stats[];

#define ROZORPC_SRV_STATS(counter) rozorpc_srv_stats[counter]++;

/**
* Buffers information
*/

extern int rozorpc_srv_north_buf_count;
extern int rozorpc_srv_north_small_buf_sz;
extern int rozorpc_srv_north_large_buf_count;
extern int rozorpc_srv_north_large_buf_sz;
extern int rozorpc_srv_south_small_buf_count;
extern int rozorpc_srv_south_small_buf_sz;
extern int rozorpc_srv_south_large_buf_count;
extern int rozorpc_srv_south_large_buf_sz;


extern uint32_t rozorpc_srv_seqnum ;


/**
* Buffer Pools
*/
typedef enum 
{
  _ROZORPC_SRV_NORTH_SMALL_POOL =0 ,
  _ROZORPC_SRV_NORTH_LARGE_POOL, 
  _ROZORPC_SRV_SOUTH_SMALL_POOL,
  _ROZORPC_SRV_SOUTH_LARGE_POOL,
  _ROZORPC_SRV_MAX_POOL
} rozorpc_srv_buffer_pool_e;

extern void *rozorpc_srv_pool[];


#define ROZORPC_SRV_NORTH_SMALL_POOL rozorpc_srv_pool[_ROZORPC_SRV_NORTH_SMALL_POOL]
#define ROZORPC_SRV_NORTH_LARGE_POOL rozorpc_srv_pool[_ROZORPC_SRV_NORTH_LARGE_POOL]
#define ROZORPC_SRV_SOUTH_SMALL_POOL rozorpc_srv_pool[_ROZORPC_SRV_SOUTH_SMALL_POOL]
#define ROZORPC_SRV_SOUTH_LARGE_POOL rozorpc_srv_pool[_ROZORPC_SRV_SOUTH_LARGE_POOL]

/*
**__________________________________________________________________________
*/
/**
  allocate a  context to handle a client read/write transaction

  @param     : none
  @retval <>NULL pointer to the allocated context
  @retval NULL out of free context
*/
rozorpc_srv_ctx_t *rozorpc_srv_alloc_context();

/*
**__________________________________________________________________________
*/
/**
* release a read/write context that has been use for either a read or write operation

  @param : pointer to the context
  
  @retval <>NULL pointer to the allocated context
  @retval NULL out of free context
*/
void rozorpc_srv_release_context(rozorpc_srv_ctx_t *ctx_p);


extern uint32_t    rozorpc_srv_ctx_count;           /**< Max number of contexts    */
extern uint32_t    rozorpc_srv_ctx_allocated;      /**< current number of allocated context        */
/*
**__________________________________________________________________________
*/
/**
*  Get the number of free transaction context
*
  @param none
  
  @retval number of free context
*/
static inline uint32_t rozorpc_srv_get_free_transaction_context()
{
  return(rozorpc_srv_ctx_count - rozorpc_srv_ctx_allocated);

}

/*
**__________________________________________________________________________
*/
/**
* send a read pr write success reply
  insert the transaction_id associated with the inittial request transaction id
  
  In case of a success it is up to the called function to release the xmit buffer
  
  @param p : pointer to the root transaction context used for the read
  
  @retval none

*/
void rozorpc_srv_common_reply_forward(rozorpc_srv_ctx_t *p);

/*
**__________________________________________________________________________
*/
/**
* send a error read reply
  That API fill up the common header with the SP_READ_RSP opcode
  insert the transaction_id associated with the inittial request transaction id
  insert a status OK
  insert the length of the data payload
  
  In case of a success it is up to the called function to release the xmit buffer
  
  @param p : pointer to the root transaction context used for the read
  @param error : error code
  
  @retval none

*/
void rozorpc_srv_reply_error(rozorpc_srv_ctx_t *p,int error);


/*
**__________________________________________________________________________
*/
/**
* send a rpc reply: the encoding function MUST be found in xdr_result 
 of the gateway context

  It is assumed that the xmitBuf MUST be found in xmitBuf field
  
  In case of a success it is up to the called function to release the xmit buffer
  
  @param p : pointer to the root transaction context used for the read
  @param arg_ret : returned argument to encode 
  
  @retval none

*/
void rozorpc_srv_forward_reply (rozorpc_srv_ctx_t *p,char * arg_ret);

/*
**__________________________________________________________________________
*/
/**
* send a rpc reply: the encoding function MUST be found in xdr_result 
 of the gateway context

  It is assumed that the xmitBuf MUST be found in xmitBuf field
  
  In case of a success it is up to the called function to release the xmit buffer
  
  @param p : pointer to the root transaction context used for the read
  @param arg_ret : returned argument to encode 
  
  @retval none

*/
void rozorpc_srv_forward_reply_with_extra_len (rozorpc_srv_ctx_t *p,char * arg_ret,int extra_len);



/*
**__________________________________________________________________________
*/
/**
   rozorpc_srv_module_init

  create the buffer pools for receiving ans sending rpc messages


@retval   : RUC_OK : done
@retval          RUC_NOK : out of memory
*/
uint32_t rozorpc_srv_module_init();

/*
**__________________________________________________________________________
*/
/**
   rozorpc_srv_module_init

  create the buffer pools for receiving ans sending rpc messages

  @param count: number of transaction contexts

@retval   : RUC_OK : done
@retval          RUC_NOK : out of memory
*/
uint32_t rozorpc_srv_module_init_ctx_only(uint32_t count);


/*
**__________________________________________________________________________
*/
/**
*  get the arguments of the incoming request: it is mostly a rpc decode

 @param recv_buf : ruc buffer that contains the request
 @param xdr_argument : decoding procedure
 @param argument     : pointer to the array where decoded arguments will be stored
 
 @retval TRUE on success
 @retval FALSE decoding error
*/
int rozorpc_srv_getargs (void *recv_buf,xdrproc_t xdr_argument, void *argument);

/*
**__________________________________________________________________________
*/
/**
*  get the arguments of the incoming request: it is mostly a rpc decode

 @param recv_buf : ruc buffer that contains the request
 @param xdr_argument : decoding procedure
 @param argument     : pointer to the array where decoded arguments will be stored
 
 @retval TRUE on success
 @retval FALSE decoding error
*/
int rozorpc_srv_getargs_with_position (void *recv_buf,xdrproc_t xdr_argument, void *argument,int *position);
#endif
