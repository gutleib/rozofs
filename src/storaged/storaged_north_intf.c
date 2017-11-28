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
#include <arpa/inet.h>
#include <netinet/in.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/common_config.h>
#include <rozofs/core/ruc_buffer_api.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/core/rozofs_socket_family.h>
#include <rozofs/core/af_inet_stream_api.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/ruc_buffer_debug.h>
#include <rozofs/core/rozofs_host2ip.h>
#include <rozofs/core/rozofs_rpc_non_blocking_generic_srv.h>
#include <rozofs/rpc/rozofs_rpc_util.h>

#include "sconfig.h"
#include "storaged_north_intf.h"
#include "mprotosvc.h"

/**
* Buffers information
*/
int storage_read_write_buf_count = 0;   /**< number of buffer allocated for read/write on north interface */
int storage_read_write_buf_sz = 0;      /**<read:write buffer size on north interface */

void *storaged_buffer_pool_p = NULL;  /**< reference of the read/write buffer pool */
extern char * pHostArray[];
extern void * storaged_decoded_rpc_buffer_pool;
/*
**__________________________________________________________________________
*/
/**
  Server callback  for GW_PROGRAM protocol:

*/
void mproto_sub_thread(rozorpc_srv_ctx_t *rozorpc_srv_ctx_p, rozofs_rpc_call_hdr_t  * hdr) {
    mp_status_ret_t  arg_err;
    char * arguments;
    int size = 0;

    
    /*
    ** Allocate buffer for decoded aeguments
    */
    rozorpc_srv_ctx_p->decoded_arg = ruc_buf_getBuffer(storaged_decoded_rpc_buffer_pool);
    if (rozorpc_srv_ctx_p->decoded_arg == NULL) {
      rozorpc_srv_ctx_p->xmitBuf = rozorpc_srv_ctx_p->recv_buf;
      rozorpc_srv_ctx_p->recv_buf = NULL;
      rozorpc_srv_ctx_p->xdr_result =(xdrproc_t) xdr_mp_status_ret_t;
      arg_err.status = MP_FAILURE;
      arg_err.mp_status_ret_t_u.error = ENOMEM;        
      rozorpc_srv_forward_reply(rozorpc_srv_ctx_p,(char*)&arg_err);
      rozorpc_srv_release_context(rozorpc_srv_ctx_p);    
      return;
    } 
       
    arguments = ruc_buf_getPayload(rozorpc_srv_ctx_p->decoded_arg);

    void (*local)(void *, rozorpc_srv_ctx_t *);

    switch (hdr->proc) {

    case MP_REMOVE:
      rozorpc_srv_ctx_p->arg_decoder = (xdrproc_t) xdr_mp_remove_arg_t;
      rozorpc_srv_ctx_p->xdr_result  = (xdrproc_t) xdr_mp_status_ret_t;
      local = mp_subthread_remove;
      size = sizeof(mp_remove_arg_t);
      break;
      
    case MP_LIST_BINS_FILES:
      rozorpc_srv_ctx_p->arg_decoder = (xdrproc_t) xdr_mp_list_bins_files_arg_t;
      rozorpc_srv_ctx_p->xdr_result  = (xdrproc_t) xdr_mp_list_bins_files_ret_t;
      local = mp_subthread_list_bins_files;
      size = sizeof(mp_list_bins_files_arg_t);
      break;

    case MP_REMOVE2:
      rozorpc_srv_ctx_p->arg_decoder = (xdrproc_t) xdr_mp_remove2_arg_t;
      rozorpc_srv_ctx_p->xdr_result  = (xdrproc_t) xdr_mp_status_ret_t;
      local = mp_subthread_remove2;
      size = sizeof(mp_remove2_arg_t);
      break;
      
    case MP_SIZE:
      rozorpc_srv_ctx_p->arg_decoder = (xdrproc_t) xdr_mp_size_arg_t;
      rozorpc_srv_ctx_p->xdr_result  = (xdrproc_t) xdr_mp_size_ret_t;
      local = mp_subthread_size;
      size = sizeof(mp_size_arg_t);
      break;
    

    default:
      rozorpc_srv_ctx_p->xmitBuf = rozorpc_srv_ctx_p->recv_buf;
      rozorpc_srv_ctx_p->recv_buf = NULL;
      rozorpc_srv_ctx_p->xdr_result =(xdrproc_t) xdr_mp_status_ret_t;
      arg_err.status = MP_FAILURE;
      arg_err.mp_status_ret_t_u.error = EPROTO;        
      rozorpc_srv_forward_reply(rozorpc_srv_ctx_p,(char*)&arg_err);
      rozorpc_srv_release_context(rozorpc_srv_ctx_p);    
      return;
    }
    
    if (size > ruc_buf_getMaxPayloadLen(rozorpc_srv_ctx_p->decoded_arg)) {
      fatal("size of request %d is %d although max payload len is %d",
            hdr->proc, size, ruc_buf_getMaxPayloadLen(rozorpc_srv_ctx_p->decoded_arg));
    }
    
    memset(arguments,0, size);
    ruc_buf_setPayloadLen(rozorpc_srv_ctx_p->decoded_arg,size); // for debug 
    
    /*
    ** decode the payload of the rpc message
    */
    if (!rozorpc_srv_getargs_with_position (rozorpc_srv_ctx_p->recv_buf, 
                                            (xdrproc_t) rozorpc_srv_ctx_p->arg_decoder, 
                                            (caddr_t) arguments, 
					    &rozorpc_srv_ctx_p->position)) 
    {    
      rozorpc_srv_ctx_p->xmitBuf = rozorpc_srv_ctx_p->recv_buf;
      rozorpc_srv_ctx_p->recv_buf = NULL;
      rozorpc_srv_ctx_p->xdr_result = (xdrproc_t)xdr_mp_status_ret_t;
      arg_err.status = MP_FAILURE;
      arg_err.mp_status_ret_t_u.error = errno;        
      rozorpc_srv_forward_reply(rozorpc_srv_ctx_p,(char*)&arg_err);
      /*
      ** release the context
      */
      rozorpc_srv_release_context(rozorpc_srv_ctx_p);    
      return;
    }  
    
    /*
    ** call the user call-back
    */
    (*local)(arguments, rozorpc_srv_ctx_p);    
}

/*
**__________________________________________________________________________
*/
/**
  MPROTO dispatcher
  
  @param rozorpc_srv_ctx_p    generic RPC context
  @param hdr                  received RPC header in host format
 
*/
static void mproto_svc(rozorpc_srv_ctx_t *rozorpc_srv_ctx_p, rozofs_rpc_call_hdr_t  * hdr, uint32_t cnx_id) {
    int             size;
    union {
      mp_stat_arg_t             stat;
      mp_remove_arg_t           remove;
      mp_list_bins_files_arg_t  list_bins_file;
      mp_remove2_arg_t          remove2;      
      mp_size_arg_t             size;      
    } mproto_request;

    union {
      mp_status_ret_t           status;
      mp_stat_ret_t             stat;
      mp_ports_ret_t            ports;
      mp_list_bins_files_ret_t  list_bins_file;
      mp_size_ret_t             size;
    } mproto_response;
    
    
    mproto_response.status.status = MP_FAILURE;
    
    void (*local)(void * req, rozorpc_srv_ctx_t *, void * resp, uint32_t cnx_id);

    switch (hdr->proc) {
    
    case MP_NULL:
      rozorpc_srv_ctx_p->arg_decoder = (xdrproc_t) xdr_void;
      rozorpc_srv_ctx_p->xdr_result  = (xdrproc_t) xdr_void;
      local = mp_null_1_svc_nb;
      size = 0;
      break;

    case MP_STAT:
      rozorpc_srv_ctx_p->arg_decoder = (xdrproc_t) xdr_mp_stat_arg_t;
      rozorpc_srv_ctx_p->xdr_result  = (xdrproc_t) xdr_mp_stat_ret_t;
      local = mp_stat_1_svc_nb;
      size = sizeof(mp_stat_arg_t);
      break;
   
    case MP_PORTS:
      rozorpc_srv_ctx_p->arg_decoder = (xdrproc_t) NULL;
      rozorpc_srv_ctx_p->xdr_result  = (xdrproc_t) xdr_mp_ports_ret_t;
      local = mp_ports_1_svc_nb;
      size = 0;
      break;
     
    default:
      rozorpc_srv_ctx_p->xdr_result =(xdrproc_t) xdr_mp_status_ret_t;
      mproto_response.status.mp_status_ret_t_u.error = EPROTO;        
      goto send_response;
    }
    
    if (size != 0) {
    
      memset(&mproto_request,0, size);
    
      /*
      ** decode the payload of the rpc message
      */
      if (!rozorpc_srv_getargs_with_position (rozorpc_srv_ctx_p->recv_buf, 
                                              (xdrproc_t) rozorpc_srv_ctx_p->arg_decoder, 
                                              (caddr_t) &mproto_request, 
					      &rozorpc_srv_ctx_p->position)) 
      {    
        rozorpc_srv_ctx_p->arg_decoder = NULL;
	rozorpc_srv_ctx_p->xdr_result = (xdrproc_t)xdr_mp_status_ret_t;
	mproto_response.status.mp_status_ret_t_u.error = errno;        
	goto send_response;
      }  
    }
    
    /*
    ** call the user call-back
    */
    (*local)(&mproto_request, rozorpc_srv_ctx_p, &mproto_response, cnx_id);  

    
send_response:

    /*
    ** Send the response in the received buffer
    */
    rozorpc_srv_ctx_p->xmitBuf  = rozorpc_srv_ctx_p->recv_buf;
    rozorpc_srv_ctx_p->recv_buf = NULL;
    rozorpc_srv_forward_reply(rozorpc_srv_ctx_p,(char*)&mproto_response);

    /*
    ** Free the decoded request
    */    
    if (rozorpc_srv_ctx_p->arg_decoder) {
      xdr_free((xdrproc_t)rozorpc_srv_ctx_p->arg_decoder, (char *) &mproto_request);
      rozorpc_srv_ctx_p->arg_decoder = NULL;
    }  
      
    /*
    ** Free the encoded response
    */     
    //xdr_free((xdrproc_t)rozorpc_srv_ctx_p->xdr_result, (char *) &mproto_response);
 
    rozorpc_srv_ctx_p->xdr_result = NULL;
    /*
    ** Free the RPC context
    */
    rozorpc_srv_release_context(rozorpc_srv_ctx_p);    
}
/*
**__________________________________________________________________________
*/
/**
  
  North interface message dispatcher upon program value
    
  @param socket_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)
 
   @retval : TRUE-> xmit ready event expected
  @retval : FALSE-> xmit  ready event not expected
*/
void storaged_req_rcv_cbk(void *userRef,uint32_t  socket_ctx_idx, void *recv_buf)
{
    uint32_t               * com_hdr_p;
    rozofs_rpc_call_hdr_t    hdr;
    rozorpc_srv_ctx_t      * rozorpc_srv_ctx_p = NULL;
    
    com_hdr_p  = (uint32_t*) ruc_buf_getPayload(recv_buf); 
    com_hdr_p +=1;   /* skip the size of the rpc message */

    memcpy(&hdr,com_hdr_p,sizeof(rozofs_rpc_call_hdr_t));
    scv_call_hdr_ntoh(&hdr);
    /*
    ** allocate a context for the duration of the transaction since it might be possible
    ** that the gateway needs to interrogate the exportd and thus needs to save the current
    ** request until receiving the response from the exportd
    */
    rozorpc_srv_ctx_p = rozorpc_srv_alloc_context();
    if (rozorpc_srv_ctx_p == NULL)
    {
       fatal(" Out of rpc context");    
    }
    /*
    ** save the initial transaction id, received buffer and reference of the connection
    */
    rozorpc_srv_ctx_p->src_transaction_id = hdr.hdr.xid;
    rozorpc_srv_ctx_p->recv_buf           = recv_buf;
    rozorpc_srv_ctx_p->socketRef          = socket_ctx_idx;
    
    switch (hdr.prog) {
    
      case MONITOR_PROGRAM :
        switch(hdr.proc) {
	
	  case MP_REMOVE2:
	  case MP_REMOVE:
	  case MP_LIST_BINS_FILES:
	  case MP_SIZE:
	    mproto_sub_thread(rozorpc_srv_ctx_p, &hdr);
	    break;
	    
	  default:
            mproto_svc(rozorpc_srv_ctx_p, &hdr, socket_ctx_idx);
	    break;
	}
	break;

      default:
        severe("Received RCP request 0x%x/0x%x",hdr.prog,hdr.proc);
        rozorpc_srv_release_context(rozorpc_srv_ctx_p);          	    	
    }
}


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
void * storaged_north_RcvAllocBufCallBack(void *userRef,uint32_t socket_context_ref,uint32_t len)
{
  /*
  ** We need at least a response buffer
  */
  uint32_t free_count = ruc_buf_getFreeBufferCount(storaged_buffer_pool_p);  
  if (free_count < 1)
  {
    return NULL;
  }

   /*
   ** check if a small or a large buffer must be allocated
   */
   if (len >  storage_read_write_buf_sz)
   {   
     return NULL;   
   }
   return ruc_buf_getBuffer(storaged_buffer_pool_p);      
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
uint32_t storaged_north_userRcvReadyCallBack(void * socket_ctx_p,int socketId)
{

    uint32_t free_count = ruc_buf_getFreeBufferCount(storaged_buffer_pool_p);
    
    if (free_count < 1)
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
void  storaged_north_userDiscCallBack(void *userRef,uint32_t socket_context_ref,void *bufRef,int err_no)
{

    /*
    ** release the current buffer if significant
    */
    if (bufRef != NULL) ruc_buf_freeBuffer(bufRef);
    /*
    ** release the context now and clean up all the attached buffer
    */
    af_unix_delete_socket(socket_context_ref);   
}



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
  (300*1024), //        so_sendbufsize;  /* length of buffer (xmit and received)        */
  storaged_north_RcvAllocBufCallBack,  //    userRcvAllocBufCallBack; /* user callback for buffer allocation */
  storaged_req_rcv_cbk,           //    userRcvCallBack;   /* callback provided by the connection owner block */
  storaged_north_userDiscCallBack,   //    userDiscCallBack; /* callBack for TCP disconnection detection         */
  NULL,   //userConnectCallBack;     /**< callback for client connection only         */
  NULL,  //    userXmitDoneCallBack; /**< optional call that must be set when the application when to be warned when packet has been sent */
  storaged_north_userRcvReadyCallBack,  //    userRcvReadyCallBack; /* NULL for default callback                     */
  NULL,  //    userXmitReadyCallBack; /* NULL for default callback                    */
  NULL,  //    userXmitEventCallBack; /* NULL for default callback                    */
  rozofs_tx_get_rpc_msg_len_cbk,        /* userHdrAnalyzerCallBack ->NULL by default, function that analyse the received header that returns the payload  length  */
  ROZOFS_RPC_SRV,       /* recv_srv_type ---> service type for reception : ROZOFS_RPC_SRV or ROZOFS_GENERIC_SRV  */
  0,       /*   rpc_recv_max_sz ----> max rpc reception buffer size : required for ROZOFS_RPC_SRV only */

  NULL,  //    *userRef;             /* user reference that must be recalled in the callbacks */
  NULL,  //    *xmitPool; /* user pool reference or -1 */
  NULL   //    *recvPool; /* user pool reference or -1 */
}; 


/*
**____________________________________________________
*/
/**
   

  Creation of the north interface buffers (AF_INET)
  
@param     : read_write_buf_count : number of read/write buffer
@param     : read_write_buf_sz : size of a read/write buffer

@retval   : RUC_OK : done
@retval          RUC_NOK : out of memory
*/

int storaged_north_interface_buffer_init(int read_write_buf_count,int read_write_buf_sz)
{
   
    storage_read_write_buf_count  = read_write_buf_count;
    storage_read_write_buf_sz     = read_write_buf_sz    ;
    
    af_inet_rozofs_north_conf.rpc_recv_max_sz = storage_read_write_buf_sz;
    
    /*
    ** create the pool for receiving requests from rozofsmount
    */
    storaged_buffer_pool_p = ruc_buf_poolCreate(storage_read_write_buf_count, storage_read_write_buf_sz);
    if (storaged_buffer_pool_p == NULL)
    {
       severe( "ruc_buf_poolCreate(%d,%d)", storage_read_write_buf_count, storage_read_write_buf_sz ); 
       return -1;
    }
    ruc_buffer_debug_register_pool("xmit/rcv",  storaged_buffer_pool_p);
    return 0;

}
/*
**____________________________________________________
*/

/**
  Creation of the north interface listening sockets (AF_INET)

@retval   : RUC_OK : done
@retval   RUC_NOK : out of memory
*/

int storaged_north_interface_init() {
  int ret = -1;
  uint32_t ip = INADDR_ANY; // Default IP to use
  uint16_t port=0;

  /* Try to get debug port from /etc/services */    
  port = rozofs_get_service_port_storaged_mproto();
  /*
  ** set the dscp code
  */
  af_inet_rozofs_north_conf.dscp = (uint8_t) common_config.storio_dscp; 
  af_inet_rozofs_north_conf.dscp = af_inet_rozofs_north_conf.dscp<<2;
  // No host given => listen on every IP@
  if (pHostArray[0] == NULL) {
    ret = af_inet_sock_listening_create("MPROTO",ip, port, &af_inet_rozofs_north_conf);    
    if (ret < 0) {
      fatal("Can't create AF_INET listening socket %u.%u.%u.%u:%d",
              ip>>24, (ip>>16)&0xFF, (ip>>8)&0xFF, ip&0xFF, port);
      return -1;
    }  
    return 0;
  }  

  int idx=0;
  while (pHostArray[idx] != NULL) {
  
    // Resolve host
    ret = rozofs_host2ip(pHostArray[idx],&ip);
    if (ret != 0) {
      fatal("storaged_north_interface_init can not resolve host \"%s\"", pHostArray[idx]);
    }
    /*
    ** set the dscp code
    */
    af_inet_rozofs_north_conf.dscp = (uint8_t) common_config.storio_dscp; 
    af_inet_rozofs_north_conf.dscp = af_inet_rozofs_north_conf.dscp<<2;
    // Create the listening socket
    ret = af_inet_sock_listening_create("MPROTO",ip, port, &af_inet_rozofs_north_conf);    
    if (ret < 0) {
      fatal("Can't create AF_INET listening socket %u.%u.%u.%u:%d",
              ip>>24, (ip>>16)&0xFF, (ip>>8)&0xFF, ip&0xFF, port);
      return -1;
    }
    idx++;
  }  
  return 0;
}
