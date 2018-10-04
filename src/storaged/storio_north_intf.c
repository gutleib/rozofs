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

#include "sconfig.h"
#include "sprotosvc_nb.h"
#include "storio_north_intf.h"

#include <rozofs/rdma/rozofs_rdma.h>

/**
* Buffers information
*/
int storage_read_write_buf_count= 0;   /**< number of buffer allocated for read/write on north interface */
int storage_read_write_buf_sz= 0;      /**<read:write buffer size on north interface */

void *storage_receive_buffer_pool_p = NULL;  /**< reference of the read/write buffer pool */ 

extern void storage_req_rcv_cbk(void *userRef,uint32_t  socket_ctx_idx, void *recv_buf);

extern sconfig_t storaged_config;
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
void * storio_north_RcvAllocBufCallBack(void *userRef,uint32_t socket_context_ref,uint32_t len)
{
  /*
  ** We need at least a response buffer
  */
  uint32_t free_count = ruc_buf_getFreeBufferCount(storage_receive_buffer_pool_p);  
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
   return ruc_buf_getBuffer(storage_receive_buffer_pool_p);      
}

/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   receiver ready function: called from socket controller.
   The module is intended to return if the receiver is ready to receive a new message
   and FALSE otherwise
   
   The application is ready to received if there is still some rpc decoding context
   in the decoded_rpc_buffer_pool pool.
   The system cannot rely on the number TCP buffer since there are less TCP receive
   buffers than number of TCP connection. So to avoid a potential deadlockk it is better
   to rely on the number of rpc contexts that are allocated after the full reception of
   a rpc message.


  @param socket_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)

  @retval : TRUE-> receiver ready
  @retval : FALSE-> receiver not ready
*/
uint32_t storio_north_userRcvReadyCallBack(void * socket_ctx_p,int socketId)
{
    uint32_t free_count = ruc_buf_getFreeBufferCount(decoded_rpc_buffer_pool);
    
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
void  storio_north_userDiscCallBack(void *userRef,uint32_t socket_context_ref,void *bufRef,int err_no)
{
   /*
   ** if the userReference is not NULL, that context must be released
   */
   if (userRef != NULL)
   {
      /*
      ** Check the key
      */
      rozofs_storio_share_mem_ctx_t *sharemem_p = (rozofs_storio_share_mem_ctx_t*) userRef;
      if (sharemem_p->key_stdalone == ROZOFS_STORIO_STANDALONE_KEY) free(sharemem_p);
   }
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
  storio_north_RcvAllocBufCallBack,  //    userRcvAllocBufCallBack; /* user callback for buffer allocation */
  storio_req_rcv_cbk,           //    userRcvCallBack;   /* callback provided by the connection owner block */
  storio_north_userDiscCallBack,   //    userDiscCallBack; /* callBack for TCP disconnection detection         */
  NULL,   //userConnectCallBack;     /**< callback for client connection only         */
  NULL,  //    userXmitDoneCallBack; /**< optional call that must be set when the application when to be warned when packet has been sent */
  storio_north_userRcvReadyCallBack,  //    userRcvReadyCallBack; /* NULL for default callback                     */
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

int storio_north_interface_buffer_init(int read_write_buf_count,int read_write_buf_sz)
{
   
    storage_read_write_buf_count  = read_write_buf_count;
    storage_read_write_buf_sz     = read_write_buf_sz    ;
    
    af_inet_rozofs_north_conf.rpc_recv_max_sz = storage_read_write_buf_sz;
    
    /*
    ** create the pool for receiving requests from rozofsmount
    */
    storage_receive_buffer_pool_p = ruc_buf_poolCreate(storage_read_write_buf_count, STORIO_BUF_RECV_SZ);
    if (storage_receive_buffer_pool_p == NULL)
    {
       severe( "ruc_buf_poolCreate(%d,%d)", storage_read_write_buf_count, STORIO_BUF_RECV_SZ ); 
       return -1;
    }
    ruc_buffer_debug_register_pool("Pool_rcv",  storage_receive_buffer_pool_p);

    /*
    ** registration with the RDMA module
    */
#ifdef ROZOFS_RDMA
    {
      rozofs_rdma_memory_reg_t rdma_reg_ctx;
      rdma_reg_ctx.mem = ruc_buf_get_pool_base_and_length(storage_receive_buffer_pool_p,&rdma_reg_ctx.len);
      if (rdma_reg_ctx.mem != NULL)
      {
	rozofs_rdma_user_memory_register(&rdma_reg_ctx);
      }
    }
#endif

    return 0;

}
/*
**____________________________________________________
*/
/**
   

  Creation of the north interface listening sockets (AF_INET)


@retval   : RUC_OK : done
@retval          RUC_NOK : out of memory
*/

int storio_north_interface_init(char * host, int instance_id) {
  int i;
  int ret;
  uint32_t ipAddr;
  int count;
  int warning_raised = 0;
  
  /*
  ** Wait until at least one of the IP address configured exists
  */
  count = 0;
  while (count == 0) {
  
    /*
    ** Count the number of available IP addresses
    */
    for (i=0; i< storaged_config.io_addr_nb; i++) {     
      ipAddr = storaged_config.io_addr[i].ipv4;
      if ((ipAddr == INADDR_ANY) ||
         (is_this_ipV4_configured(ipAddr))) count++;
    }  

    /* No IP address configured Raise a warning once */  
    if (count == 0) {
      if (warning_raised==0) {	
	warning_raised = 1;
	warning("No listening I/O IP addresses configured");
      }	
      sleep(1);
    }  
    else {
      if (warning_raised == 1) {
	warning("%d listening I/O IP addresses configured now",count);
      }	      
    }
  }
  
  /* At least one port to listen to. */
  for (i=0; i< storaged_config.io_addr_nb; i++) {
    
    ipAddr = storaged_config.io_addr[i].ipv4;
    if ((ipAddr != INADDR_ANY) && !(is_this_ipV4_configured(ipAddr))) {
      severe("Listening IO IP address %u.%u.%u.%u not configured", 
               (ipAddr>>24)&0xFF, (ipAddr>>16)&0xFF, (ipAddr>>8)&0xFF, (ipAddr>>0)&0xFF); 
      continue;	            
    }
    /*
    ** set the dscp
    */
    af_inet_rozofs_north_conf.dscp = (uint8_t) common_config.storio_dscp; 
    af_inet_rozofs_north_conf.dscp = af_inet_rozofs_north_conf.dscp<<2;
    /*
    ** Create the listening sockets
    */ 
    ret = af_inet_sock_listening_create("IO",
                                        storaged_config.io_addr[i].ipv4,
					storaged_config.io_addr[i].port+instance_id,
					&af_inet_rozofs_north_conf);
    if (ret < 0) {
      uint32_t ip = storaged_config.io_addr[i].ipv4;
      fatal("Can't create AF_INET listening socket %u.%u.%u.%u:%d",
              ip>>24, (ip>>16)&0xFF, (ip>>8)&0xFF, ip&0xFF, storaged_config.io_addr[i].port+instance_id);
      return -1;
    } 
    /*
    ** attempt to create a RDMA listening: best effort
    */
#ifdef ROZOFS_RDMA
    {
      rozofs_rdma_listening_create( storaged_config.io_addr[i].ipv4,storaged_config.io_addr[i].port+instance_id);
    }
#endif
  }
  
  return 0;
}

