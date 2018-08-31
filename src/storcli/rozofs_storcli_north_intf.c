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

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/core/ruc_buffer_api.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/ruc_buffer_debug.h>
#include "rozofs_storcli_rpc.h"
#include <rozofs/rpc/storcli_proto.h>
#include "rozofs_storcli.h"
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/core/rozofs_socket_family.h>
#include "storcli_main.h"
 /**
* Buffers information
*/
int rozofs_storcli_read_write_buf_count= 0;   /**< number of buffer allocated for read/write on north interface */
int rozofs_storcli_read_write_buf_sz= 0;      /**<read:write buffer size on north interface */

void *storcli_north_buffer_pool_p = NULL;  /**< reference of the read/write buffer pool */
 

extern int rozofs_storcli_south_large_buf_count;


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
void * rozofs_storcli_north_RcvAllocBufCallBack(void *userRef,uint32_t socket_context_ref,uint32_t len)
{
    uint32_t free_count = rozofs_storcli_get_free_transaction_context();

    if (free_count < STORCLI_CTX_MIN_CNT) {
        storcli_buf_depletion_count++;
        return NULL;
    }

    if (stc_rng_is_full()) {
        storcli_rng_full_count++;
        return NULL;
    }


    /*
    ** Check that the is enough buffer on the storio side
    */
    free_count = ruc_buf_getFreeBufferCount(ROZOFS_STORCLI_SOUTH_LARGE_POOL);

    if (free_count < (rozofs_storcli_south_large_buf_count>>3)) {
        storcli_buf_depletion_count++;
        return NULL;
    }
    
   /*
   ** check if a small or a large buffer must be allocated
   */
   if (len >  rozofs_storcli_read_write_buf_sz)
   {   
     return NULL;   
   }
   return ruc_buf_getBuffer(storcli_north_buffer_pool_p);      
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
uint32_t rozofs_storcli_north_userRcvReadyCallBack(void * socket_ctx_p,int socketId)
{

    uint32_t free_count = rozofs_storcli_get_free_transaction_context();
    
    if (free_count < STORCLI_CTX_MIN_CNT)
    {
      return FALSE;
    }
    return TRUE;

#if 0 // control done on noth reception buffer of the load balancing group
    if (ruc_buf_isPoolEmpty(storcli_north_buffer_pool_p))
    {
      return FALSE;
    
    }
    return TRUE;
#endif
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
void  storcli_lbg_north_userDiscCallBack(void *userRef,uint32_t socket_context_ref,void *bufRef,int err_no)
{

    /*
    ** release the current buffer if significant
    */
    if (bufRef != NULL) ruc_buf_freeBuffer(bufRef);
    
    severe("remote end disconnection");;
    /*
    ** release the context now and clean up all the attached buffer
    */
    af_unix_delete_socket(socket_context_ref);   
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
void rozofs_storcli_req_rcv_cbk(void *userRef,uint32_t  socket_ctx_idx, void *recv_buf)
{
    uint32_t  *com_hdr_p;
    rozofs_rpc_call_hdr_t   hdr;
    int errcode = EINVAL;
    
    com_hdr_p  = (uint32_t*) ruc_buf_getPayload(recv_buf); 
    com_hdr_p +=1;   /* skip the size of the rpc message */

    memcpy(&hdr,com_hdr_p,sizeof(rozofs_rpc_call_hdr_t));
    scv_call_hdr_ntoh(&hdr);
    /*
    ** get the opcode requested and dispatch the processing accordling to that opcode
    */
    switch (hdr.proc)
    {
       case STORCLI_READ:
        rozofs_storcli_read_req_init(socket_ctx_idx,recv_buf,rozofs_storcli_remote_rsp_cbk,0,STORCLI_DO_QUEUE);
        return;
       
       case STORCLI_WRITE:
        rozofs_storcli_write_req_init(socket_ctx_idx,recv_buf,rozofs_storcli_remote_rsp_cbk);
        return;

       case STORCLI_TRUNCATE:
        rozofs_storcli_truncate_req_init(socket_ctx_idx,recv_buf,rozofs_storcli_remote_rsp_cbk);
        return;       

       case STORCLI_DELETE:
        rozofs_storcli_delete_req_init(socket_ctx_idx,recv_buf,rozofs_storcli_remote_rsp_cbk);
        return; 
	       
       default:
        /*
        ** Put code here to format a reply with an error message
        */
        rozofs_storcli_reply_error_with_recv_buf(socket_ctx_idx,recv_buf,NULL,rozofs_storcli_remote_rsp_cbk,errcode);
        return;   
    }

    return;
}


 /**
 *  socket configuration for the family
 */
 af_unix_socket_conf_t  af_unix_test_family =
{
  1,  //           family: identifier of the socket family    */
  0,         /**< instance number within the family   */
  sizeof(uint32_t),  /* headerSize  -> size of the header to read                 */
  0,                 /* msgLenOffset->  offset where the message length fits      */
  sizeof(uint32_t),  /* msgLenSize  -> size of the message length field in bytes  */

  (1024*256), //        bufSize;         /* length of buffer (xmit and received)        */
  (300*1024), //        so_sendbufsize;  /* length of buffer (xmit and received)        */
  rozofs_storcli_north_RcvAllocBufCallBack,  //    userRcvAllocBufCallBack; /* user callback for buffer allocation */
  rozofs_storcli_req_rcv_cbk,           //    userRcvCallBack;   /* callback provided by the connection owner block */
  storcli_lbg_north_userDiscCallBack,   //    userDiscCallBack; /* callBack for TCP disconnection detection         */
  NULL,   //userConnectCallBack;     /**< callback for client connection only         */
  NULL,  //    userXmitDoneCallBack; /**< optional call that must be set when the application when to be warned when packet has been sent */
  rozofs_storcli_north_userRcvReadyCallBack,  //    userRcvReadyCallBack; /* NULL for default callback                     */
  NULL,  //    userXmitReadyCallBack; /* NULL for default callback                    */
  NULL,  //    userXmitEventCallBack; /* NULL for default callback                    */
  rozofs_tx_get_rpc_msg_len_cbk,        /* userHdrAnalyzerCallBack ->NULL by default, function that analyse the received header that returns the payload  length  */
  ROZOFS_RPC_SRV,       /* recv_srv_type ---> service type for reception : ROZOFS_RPC_SRV or ROZOFS_GENERIC_SRV  */
  0,       /*   rpc_recv_max_sz ----> max rpc reception buffer size : required for ROZOFS_RPC_SRV only */

  NULL,  //    *userRef;             /* user reference that must be recalled in the callbacks */
  NULL,  //    *xmitPool; /* user pool reference or -1 */
  NULL,   //    *recvPool; /* user pool reference or -1 */
  3  /**w used dscp to set the af_unix socket priority */
}; 

/*
**____________________________________________________
*/
/**
   rozofs_storcli_north_interface_init

  create the Transaction context pool

@param     : read_write_buf_count : number of read/write buffer
@param     : read_write_buf_sz : size of a read/write buffer
@param     : eid : unique identifier of the export to which the storcli process is associated
@param     : rozofsmount_instance : instance number is needed when several reozfsmount runni ng oin the same share the export


@retval   : RUC_OK : done
@retval          RUC_NOK : out of memory
*/

int rozofs_storcli_north_interface_init(uint32_t eid,uint16_t rozofsmount_instance,uint32_t instance,
                             int read_write_buf_count,int read_write_buf_sz)
{
   int ret = 0;
   char sunpath[AF_UNIX_SOCKET_NAME_SIZE];
   

    rozofs_storcli_read_write_buf_count  = read_write_buf_count;
    rozofs_storcli_read_write_buf_sz     = read_write_buf_sz    ;
    while(1)
    {
    storcli_north_buffer_pool_p = ruc_buf_poolCreate(rozofs_storcli_read_write_buf_count,rozofs_storcli_read_write_buf_sz);
    if (storcli_north_buffer_pool_p == NULL)
    {
       ret = -1;
       severe( "ruc_buf_poolCreate(%d,%d)", rozofs_storcli_read_write_buf_count, rozofs_storcli_read_write_buf_sz ); 
       break;
    }
    /*
    ** register the pool
    */
    ruc_buffer_debug_register_pool("Rozofsmount_rcv",storcli_north_buffer_pool_p);
    /*
    ** create the listening af unix socket on the north interface
    */
    af_unix_test_family.rpc_recv_max_sz = rozofs_storcli_read_write_buf_sz;
    sprintf(sunpath,"%s%s_%d.%d_lbg%d_inst_1",ROZOFS_SOCK_FAMILY_STORCLI_NORTH_SUNPATH,storcli_get_owner(),
                     eid,rozofsmount_instance,instance);
//    sprintf(sunpath,"%s%d.%d_inst_%d",ROZOFS_SOCK_FAMILY_STORCLI_NORTH_SUNPATH,eid,rozofsmount_instance,instance);
    ret =  af_unix_sock_listening_create("STORCLI_NORTH",
                                          sunpath, 
                                          &af_unix_test_family   
                                          );

    break; 
    
    }
    return ret;

}


