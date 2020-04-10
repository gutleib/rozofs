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
#include <sys/types.h> 
#include <sys/socket.h>
#include <fcntl.h> 
#include <sys/un.h>             
#include <errno.h>  
#include <pthread.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/profile.h>
#include <rozofs/core/ruc_buffer_api.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/ruc_buffer_debug.h>
#include <rozofs/core/ruc_sockCtl_api.h>
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/core/rozofs_socket_family.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/core/af_inet_stream_api.h>
#include "expbt_global.h"
#include "expbt_north_intf.h"
#include <rozofs/rpc/expbt_protocol.h>
#include "expbt_trk_thread_intf.h"


DECLARE_PROFILING(expbt_profiler_t);
/**
* Buffers information
*/
int expbt_cmd_buf_count= 0;   /**< number of buffer allocated for read/write on north interface */
int expbt_cmd_buf_sz= 0;      /**<read:write buffer size on north interface */

void *expbt_buffer_pool_p = NULL;  /**< reference of the read/write buffer pool */
uint64_t tcp_receive_depletion_count = 0;

/*
**____________________________________________________
*/
/**
*  
  Callback to allocate a buffer for receiving a rpc message (mainly a RPC response
 
 
 The service might reject the buffer allocation because the pool runs
 out of buffer or because there is no pool with a buffer that is large enough
 for receiving the message because of a out of range size.

 @param userRef : pointer to a user reference: not used here
 @param socket_context_ref: socket context reference
 @param len : length of the incoming message
 
 @retval <>NULL pointer to a receive buffer
 @retval == NULL no buffer 
*/
void * expbt_north_RcvAllocBufCallBack(void *userRef,uint32_t socket_context_ref,uint32_t len)
{
    uint32_t free_count; 
    void *p;

    /*
    ** Check that the is enough buffer on the storio side
    */
    free_count = ruc_buf_getFreeBufferCount(expbt_buffer_pool_p);

    if (free_count < 1) {
   /*
    ** Case of the buffer depletion: loop on disk thread response socket on order to recover some buffers
    */
    tcp_receive_depletion_count++;
    af_unix_trk_pool_socket_on_receive_buffer_depletion();
    }
    
   p = ruc_buf_getBuffer(expbt_buffer_pool_p); 
   return p;  
}

/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   receiver ready function: called from socket controller.
   The module is intended to return if the receiver is ready to receive a new message
   and FALSE otherwise
   
   The application is ready to received if the north read/write buffer pool is not empty


  @param socket_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)

  @retval : TRUE-> receiver ready
  @retval : FALSE-> receiver not ready
*/
uint32_t expbt_north_userRcvReadyCallBack(void * socket_ctx_p,int socketId)
{

    if (ruc_buf_isPoolEmpty(expbt_buffer_pool_p))
    {
      return FALSE;
    
    }
    return TRUE;
}




/*__________________________________________________________________________
*/
/**
* test function that is called upon a failure on sending

 The application might use that callback if it has some other
 destination that can be used in case of failure of the current one
 If the application has no other destination to select, it is up to the
 application to release the buffer.
 

 @param userRef : pointer to a user reference: not used here
 @param socket_context_ref: socket context reference
 @param bufRef : pointer to the packet buffer on which the error has been encountered
 @param err_no : errno has reported by the sendto().
 
 @retval none
*/
void  expbt_north_userDiscCallBack(void *userRef,uint32_t socket_context_ref,void *bufRef,int err_no)
{
   af_unix_ctx_generic_t *this;
    /*
    ** release the current buffer if significant
    */
    if (bufRef != NULL) ruc_buf_freeBuffer(bufRef);
    
    severe("remote end disconnection");;
   
    this = af_unix_getObjCtx_p(socket_context_ref);
    if (this == NULL)
    {
       fatal("The socket does not exist");
    }
    /*
    ** release the context now and clean up all the attached buffer
    */
    af_unix_delete_socket(socket_context_ref);   
}

/*
**__________________________________________________________________________
*/
/**
   Send a response that contains a global error
   
   @param hdr_p: pointer to the header of the initial message
   @param rozorpc_srv_ctx_p: allocated RPC context
   @param error: error code to return
   
   @retval none
*/
void expbt_srv_forward_reply_err(expbt_msg_hdr_t *hdr_p,rozorpc_srv_ctx_t *rozorpc_srv_ctx_p,int error)
{
  int ret;
  expbt_msg_t *rsp_msg_p;
  int returned_size;
  
  rsp_msg_p  = (expbt_msg_t*) ruc_buf_getPayload(rozorpc_srv_ctx_p->xmitBuf); 
  memcpy(&rsp_msg_p->hdr,hdr_p,sizeof(expbt_msg_hdr_t));   
  if (error != 0) rsp_msg_p->min_rsp.global_rsp.status = -1;
  else rsp_msg_p->min_rsp.global_rsp.status = 0;
  rsp_msg_p->min_rsp.global_rsp.errcode = error;   
  rsp_msg_p->hdr.dir = 1;
  rsp_msg_p->hdr.len = 0;
  returned_size = sizeof(expbt_msg_hdr_t)+sizeof(expbt_trk_main_rsp_t)+sizeof(expbt_trk_check_rsp_t) ; 
  rsp_msg_p->hdr.len = returned_size- sizeof(uint32_t);
  ruc_buf_setPayloadLen(rozorpc_srv_ctx_p->xmitBuf,rsp_msg_p->hdr.len+sizeof(uint32_t));     
  /*
  ** send the response towards the storcli process that initiates the disk operation
  */
  ret = af_unix_generic_send_stream_with_idx((int)rozorpc_srv_ctx_p->socketRef,rozorpc_srv_ctx_p->xmitBuf); 
  if (ret == 0) {
    /**
    * success so remove the reference of the xmit buffer since it is up to the called
    * function to release it
    */
    ROZORPC_SRV_STATS(ROZORPC_SRV_SEND);
    rozorpc_srv_ctx_p->xmitBuf = NULL;
  }
  else {
    ROZORPC_SRV_STATS(ROZORPC_SRV_SEND_ERROR);
  }    
}
/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   THis the callback that is activated upon the recption of a disk
   operation from a remote client: There is 2 kinds of requests that
   are supported by this function:
   READ and WRITE

    
  @param socket_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)
 
   @retval : TRUE-> xmit ready event expected
  @retval : FALSE-> xmit  ready event not expected
*/
void expbt_req_rcv_cbk(void *userRef,uint32_t  socket_ctx_idx, void *recv_buf)
{

    uint32_t  *com_hdr_p;
    expbt_msg_hdr_t   hdr;
    char * arguments;
    int error = 0;

    rozorpc_srv_ctx_t *rozorpc_srv_ctx_p = NULL;
    
    com_hdr_p  = (uint32_t*) ruc_buf_getPayload(recv_buf); 
    /*
    ** save the header of the command
    */
    memcpy(&hdr,com_hdr_p,sizeof(expbt_msg_hdr_t));
    /*
    ** allocate a RPC context for the duration of the transaction
    */
    rozorpc_srv_ctx_p = rozorpc_srv_alloc_context();
    if (rozorpc_srv_ctx_p == NULL)
    {
       fatal(" Out of rpc context");    
    }
    /*
    ** save the initial transaction id, received buffer and reference of the connection
    */
    rozorpc_srv_ctx_p->src_transaction_id = hdr.xid;
    rozorpc_srv_ctx_p->recv_buf  = recv_buf;
    rozorpc_srv_ctx_p->socketRef = socket_ctx_idx;    
    /*
    ** Allocate buffer for decoded aeguments
    */
    rozorpc_srv_ctx_p->decoded_arg = ruc_buf_getBuffer(decoded_rpc_buffer_pool);
    if (rozorpc_srv_ctx_p->decoded_arg == NULL) {
      rozorpc_srv_ctx_p->xmitBuf = rozorpc_srv_ctx_p->recv_buf;
      rozorpc_srv_ctx_p->recv_buf = NULL;
      error = ENOMEM;   
           
      expbt_srv_forward_reply_err(&hdr,rozorpc_srv_ctx_p,error);
      rozorpc_srv_release_context(rozorpc_srv_ctx_p);    
      return;
    }    
    arguments = ruc_buf_getPayload(rozorpc_srv_ctx_p->decoded_arg);

    void (*local)(void *, rozorpc_srv_ctx_t *);

    switch (hdr.opcode) {
    
    case EXP_BT_NULL:
    
      rozorpc_srv_ctx_p->xmitBuf = rozorpc_srv_ctx_p->recv_buf;
      rozorpc_srv_ctx_p->recv_buf = NULL;
      error = 0;        

      expbt_srv_forward_reply_err(&hdr,rozorpc_srv_ctx_p,error);
      rozorpc_srv_release_context(rozorpc_srv_ctx_p);  
      return;

    case EXP_BT_TRK_READ:

      local = expbt_trk_read_post2thread;
      break;

    case EXP_BT_TRK_CHECK:

      local = expbt_trk_check_post2thread;
      break;

    case EXP_BT_DIRENT_LOAD:

      local = expbt_dirent_load_post2thread;
      break;

    default:
      rozorpc_srv_ctx_p->xmitBuf = rozorpc_srv_ctx_p->recv_buf;
      rozorpc_srv_ctx_p->recv_buf = NULL;
      error = ENOTSUP;        

      expbt_srv_forward_reply_err(&hdr,rozorpc_srv_ctx_p,error);
      rozorpc_srv_release_context(rozorpc_srv_ctx_p);    
      return;
    }
    /*
    ** register the time at which we start processing the request
    */
    rozorpc_srv_ctx_p->profiler_time = rozofs_get_ticker_us();
    /*
    ** copy the incoming message in the argument array
    */
    memcpy(arguments,com_hdr_p,hdr.len+sizeof(uint32_t));
    /*
    ** call the user call-back
    */
    (*local)(arguments, rozorpc_srv_ctx_p);    
}


/*
**____________________________________________________________________________________
*/

 /**
 *  socket configuration for the family
 */
 af_unix_socket_conf_t  af_inet_rozofs_north_conf =
{
  1,  //           family: identifier of the socket family    */
  0,         /**< instance number within the family   */
  sizeof(uint32_t),  /* headerSize  -> size of the header to read                 */
  0,                 /* msgLenOffset->  offset where the message length fits      */
  sizeof(uint32_t),  /* msgLenSize  -> size of the message length field in bytes  */

  (1024*256), //        bufSize;         /* length of buffer (xmit and received)        */
  (1024*1024), //        so_sendbufsize;  /* length of buffer (xmit and received)        */
  expbt_north_RcvAllocBufCallBack,  //    userRcvAllocBufCallBack; /* user callback for buffer allocation */
  expbt_req_rcv_cbk,           //    userRcvCallBack;   /* callback provided by the connection owner block */
  expbt_north_userDiscCallBack,   //    userDiscCallBack; /* callBack for TCP disconnection detection         */
  NULL,   //userConnectCallBack;     /**< callback for client connection only         */
  NULL,  //    userXmitDoneCallBack; /**< optional call that must be set when the application when to be warned when packet has been sent */
  expbt_north_userRcvReadyCallBack,  //    userRcvReadyCallBack; /* NULL for default callback                     */
  NULL,  //    userXmitReadyCallBack; /* NULL for default callback                    */
  NULL,  //    userXmitEventCallBack; /* NULL for default callback                    */
  NULL, // rozofs_tx_get_rpc_msg_len_cbk,        /* userHdrAnalyzerCallBack ->NULL by default, function that analyse the received header that returns the payload  length  */
  ROZOFS_GENERIC_SRV,       /* recv_srv_type ---> service type for reception : ROZOFS_RPC_SRV or ROZOFS_GENERIC_SRV  */
  (1024*256),       /*   rpc_recv_max_sz ----> max rpc reception buffer size : required for ROZOFS_RPC_SRV only */

  NULL,  //    *userRef;             /* user reference that must be recalled in the callbacks */
  NULL,  //    *xmitPool; /* user pool reference or -1 */
  NULL,   //    *recvPool; /* user pool reference or -1 */
  .priority = 3,  /** Set the af_unix socket priority */
}; 

/*
**____________________________________________________
*/
/**
   expbt_north_interface_init

  create the Transaction context pool

@param     : cmd_buf_count : number of read/write buffer
@param host : IP address in dot notation or hostname
@param port:    listening port value


@retval   : RUC_OK : done
@retval          RUC_NOK : out of memory
*/

int expbt_north_interface_init(char *host,uint16_t port,int cmd_buf_count)
{
   int ret = 0;
  uint32_t ipaddr = INADDR_ANY;

  /*
  ** create the listening af unix sockets on the north interface
  */ 
  if (host != NULL)
  {
    ret = rozofs_host2ip(host,&ipaddr);
    if (ret < 0)
    {
       fatal("Bad IP address : %s",host);
       return -1;
    } 
  } 
   

    expbt_cmd_buf_count  = cmd_buf_count;
    expbt_cmd_buf_sz     = EXP_BT_MAX_READ_RSP    ;
    while(1)
    {
      expbt_buffer_pool_p = ruc_buf_poolCreate(expbt_cmd_buf_count,expbt_cmd_buf_sz);
      if (expbt_buffer_pool_p == NULL)
      {
	 ret = -1;
	 severe( "ruc_buf_poolCreate(%d,%d)", expbt_cmd_buf_count, expbt_cmd_buf_sz ); 
	 break;
      }
      /*
      ** register the pool
      */
      ruc_buffer_debug_register_pool("expbt_rcv",expbt_buffer_pool_p);
      /*
      ** set the dscp code
      */
      af_inet_rozofs_north_conf.dscp = (uint8_t) common_config.export_dscp;  
      af_inet_rozofs_north_conf.dscp = af_inet_rozofs_north_conf.dscp<<2;

      /*
      ** create the listening af unix socket on the north interface
      */
      af_inet_rozofs_north_conf.rpc_recv_max_sz = expbt_cmd_buf_sz;

      ret =  af_inet_sock_listening_create("EXP_BT",
                                            ipaddr, 
					    port,
                                            &af_inet_rozofs_north_conf   
                                            );

      if (ret < 0) {
	uint32_t ip = ipaddr;
	severe("error on listening socket creation port %u:%s",port,strerror(errno));
	fatal("Can't create AF_INET listening socket %u.%u.%u.%u:%d",
        	ip>>24, (ip>>16)&0xFF, (ip>>8)&0xFF, ip&0xFF, port);
	return -1;
      }  
      break; 
    
    }

    return ret;

}


