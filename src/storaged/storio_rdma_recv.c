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

/**
   That source is intended to process the RPC Read & Write messages
   that has been sent from the storcli in RDMA mode
   
*/

#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <errno.h>
#include <sched.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/profile.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/rozofs_rpc_non_blocking_generic_srv.h>
#include <rozofs/core/ruc_buffer_debug.h>
#include <rozofs/core/com_cache.h>
#include <rozofs/core/rozofs_throughput.h>
#include <rozofs/rpc/sproto.h>
#include "storio_disk_thread_intf.h"
#include "sproto_nb.h"
#include "config.h"
#include "storio_device_mapping.h"
#include "storio_serialization.h"
#include "storio_rdma_recv.h"
#include "sprotosvc_nb.h"


#define DISK_SO_SENDBUF  (300*1024)
#define RDMA_RPC_REQ "rdma_rpc_req"

/**
   Structure of the RPC message posted on the RDMA_RPC_REQ_Queue (AF_UNIX)
*/

typedef struct _rdma_rpc_cmd_msg_t
{
   uint32_t rdma_opcode;  /**< RDMA opcode on queue pair                 */
   int status;            /**< status of RDMA operation                  */
   int error;             /**< error code when the status is -1          */
   uint32_t qp_num;      /**< qp on which the message has been received  */
   void *ruc_buf;        /**< reference of the ruc buffer that contains the RPC request */
} rdma_rpc_cmd_msg_t;

struct  sockaddr_un storio_rdma_south_socket_name;  /**< AF_UNIX socket name of the main thread for RDMA RPC message (READ/WRITE)   */
int storio_rdma_cp_recv_fd = -1;  /**< reference of the socket used by the storio rdma callback called by completion queue threads */
int rdma_rpc_cmd_south_socket_ref = -1;  /**< reference of the socket on the main side side used for receive RPC READ/WRITE over RDMA */
/*
**  Call back function for socket controller
*/
ruc_sockCallBack_t rdma_rpc_cmd_callBack_sock=
  {
     rdma_rpc_cmd_rcvReadysock,
     rdma_rpc_cmd_rcvMsgsock,
     rdma_rpc_cmd_xmitReadysock,
     rdma_rpc_cmd_xmitEvtsock
  };

/*
**__________________________________________________________________________
*/
/**
  Application callBack:

  Called from the socket controller. 

serial
  @param unused: not used
  @param socketId: reference of the socket (not used)
 
  @retval : always FALSE
*/

uint32_t rdma_rpc_cmd_xmitReadysock(void * unused,int socketId)
{

    return FALSE;
}


/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   Called from the socket controller upon receiving a xmit ready event
   for the associated socket. That callback is activeted only if the application
   has replied TRUE in rozofs_fuse_xmitReadysock().
   
   It typically the processing of a end of congestion on the socket

    
  @param unused: not used
  @param socketId: reference of the socket (not used)
 
   @retval :always TRUE
*/
uint32_t rdma_rpc_cmd_xmitEvtsock(void * unused,int socketId)
{
   
    return TRUE;
}
/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   receiver ready function: called from socket controller.
   We check if there is enough RPC buffer for decoding the input RPC message

    
  @param unused: not used
  @param socketId: reference of the socket (not used)
 
  @retval : TRUE-> receiver ready
  @retval : FALSE-> receiver not ready
*/

uint32_t rdma_rpc_cmd_rcvReadysock(void * unused,int socketId)
{
    uint32_t free_count = ruc_buf_getFreeBufferCount(decoded_rpc_buffer_pool);
    
    if (free_count < 1)
    {
      return FALSE;
    }
    return TRUE;
}


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
void rozorpc_srv_forward_reply_rdma_with_extra_len (rozorpc_srv_ctx_t *p,char * arg_ret,int extra_len)
{
   int ret;
   uint8_t *pbuf;           /* pointer to the part that follows the header length */
   uint32_t *header_len_p;  /* pointer to the array that contains the length of the rpc message*/
   XDR xdrs;
   int len;

//   START_PROFILING(forward_reply);
   
   if (p->xmitBuf == NULL)
   {
      ROZORPC_SRV_STATS(ROZORPC_SRV_NO_BUFFER_ERROR);
      severe("no xmit buffer");
      goto error;
   } 
    /*
    ** create xdr structure on top of the buffer that will be used for sending the response
    */
    header_len_p = (uint32_t*)ruc_buf_getPayload(p->xmitBuf); 
    pbuf = (uint8_t*) (header_len_p+1);  
    
    /* Do not forget to reset the opaque authentication part */
    memset(pbuf,0,40);          
 
    len = (int)ruc_buf_getMaxPayloadLen(p->xmitBuf);
    len -= sizeof(uint32_t);
    xdrmem_create(&xdrs,(char*)pbuf,len,XDR_ENCODE); 
    if (rozofs_encode_rpc_reply(&xdrs,(xdrproc_t)p->xdr_result,(caddr_t)arg_ret,p->src_transaction_id) != TRUE)
    {
      ROZORPC_SRV_STATS(ROZORPC_SRV_ENCODING_ERROR);
      severe("rpc reply encoding error");
      goto error;     
    }      
    /*
    ** send back the referenc eof the client lbg_id
    */ 
    rozofs_rpc_set_lbg_id_in_request(p->xmitBuf,p->client_lbg_id);
    /*
    ** compute the total length of the message for the rpc header and add 4 bytes more bytes for
    ** the ruc buffer to take care of the header length of the rpc message.
    */
    int total_len = xdr_getpos(&xdrs) ;
    total_len +=extra_len;
    *header_len_p = htonl(0x80000000 | total_len);
    total_len +=sizeof(uint32_t);
    ruc_buf_setPayloadLen(p->xmitBuf,total_len);

#warning FDL_RDMA: must be send on a queue pair
    /*
    ** Get the callback for sending back the response:
    ** A callback is needed since the request for read might be local or remote
    */
    ret =  af_unix_generic_send_stream_with_idx((int)p->socketRef,p->xmitBuf);  
    if (ret == 0)
    {
      /**
      * success so remove the reference of the xmit buffer since it is up to the called
      * function to release it
      */
      ROZORPC_SRV_STATS(ROZORPC_SRV_SEND);
      p->xmitBuf = NULL;
    }
    else
    {
      ROZORPC_SRV_STATS(ROZORPC_SRV_SEND_ERROR);
    }
error:
//    STOP_PROFILING(forward_reply);
    return;
}

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
void rozorpc_srv_forward_reply_rdma (rozorpc_srv_ctx_t *p,char * arg_ret)
{
   return rozorpc_srv_forward_reply_rdma_with_extra_len(p,arg_ret,0);
}



/*
**__________________________________________________________________________
*/
/**
  Server callback  for GW_PROGRAM protocol:
    
     GW_INVALIDATE_SECTIONS
     GW_INVALIDATE_ALL
     GW_CONFIGURATION
     GW_POLL
     
  That callback is called upon receiving RPC read or write message on a RDMA connection
  The structure of the message is the same as the one received on a TCP connection.

    
  @param msg: pointer to message that contains the context of the RDMA RPC message
  @param socketId: reference of the socket (not used)
 
   @retval : TRUE-> xmit ready event expected
  @retval : FALSE-> xmit  ready event not expected
*/
void storio_rdma_rpc_req_rcv_cbk(rdma_rpc_cmd_msg_t   *msg)
{
    uint32_t  *com_hdr_p;
    rozofs_rpc_call_hdr_t   hdr;
    sp_status_ret_t  arg_err;
    char * arguments;
    void *recv_buf;
    int size = 0;
    
    /*
    ** Check the status reported by the completion Queue
    */
    if (msg->status < 0)
    {
      /*
      ** there was an error, so the ruc_buf is not significant, only the QP is
      */
      goto error_qp;
    }
    /*
    **  Get the ruc_buf that contains the received RPC message
    */
    recv_buf = msg->ruc_buf;
    
    rozorpc_srv_ctx_t *rozorpc_srv_ctx_p = NULL;
    
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
    rozorpc_srv_ctx_p->recv_buf  = recv_buf;
    
    /*
    ** rather that putting the reference of the socket, we set the reference of the Queue Pair on  which
    ** we have receive the message
    */
    rozorpc_srv_ctx_p->socketRef = msg->qp_num;
    rozorpc_srv_ctx_p->rdma = 1; 
    /*
    ** get the reference of the lbg_id of the client from the received rpc message
    */
    rozorpc_srv_ctx_p->client_lbg_id = rozofs_rpc_get_lbg_id_in_request(recv_buf);
    
    /*
    ** Allocate buffer for decoded arguments
    */
    rozorpc_srv_ctx_p->decoded_arg = ruc_buf_getBuffer(decoded_rpc_buffer_pool);
    if (rozorpc_srv_ctx_p->decoded_arg == NULL) {
      rozorpc_srv_ctx_p->xmitBuf = rozorpc_srv_ctx_p->recv_buf;
      rozorpc_srv_ctx_p->recv_buf = NULL;
      rozorpc_srv_ctx_p->xdr_result =(xdrproc_t) xdr_sp_status_ret_t;
      arg_err.status = SP_FAILURE;
      arg_err.sp_status_ret_t_u.error = ENOMEM;        
      rozorpc_srv_forward_reply_rdma(rozorpc_srv_ctx_p,(char*)&arg_err);
      rozorpc_srv_release_context(rozorpc_srv_ctx_p);    
      return;
    }    
    arguments = ruc_buf_getPayload(rozorpc_srv_ctx_p->decoded_arg);

    void (*local)(void *, rozorpc_srv_ctx_t *);

    switch (hdr.proc) {
    

    case SP_WRITE_RDMA:
      rozorpc_srv_ctx_p->arg_decoder = (xdrproc_t)xdr_sp_write_rdma_arg_t;
      rozorpc_srv_ctx_p->xdr_result  = (xdrproc_t) xdr_sp_write_ret_t;
      local = sp_write_rdma_1_svc_disk_thread;
      size = sizeof (xdr_sp_write_rdma_arg_t);
      break;
  
    case SP_READ_RDMA:
      rozorpc_srv_ctx_p->arg_decoder = (xdrproc_t) xdr_sp_read_rdma_arg_t;
      rozorpc_srv_ctx_p->xdr_result  = (xdrproc_t) xdr_sp_read_rdma_ret_no_bins_t;
      local = sp_read_rdma_1_svc_disk_thread;
      size = sizeof (sp_read_rdma_arg_t);
      break;

    default:
      rozorpc_srv_ctx_p->xmitBuf = rozorpc_srv_ctx_p->recv_buf;
      rozorpc_srv_ctx_p->recv_buf = NULL;
      rozorpc_srv_ctx_p->xdr_result =(xdrproc_t) xdr_sp_status_ret_t;
      arg_err.status = SP_FAILURE;
      arg_err.sp_status_ret_t_u.error = ENOTSUP;        
      rozorpc_srv_forward_reply_rdma(rozorpc_srv_ctx_p,(char*)&arg_err);
      rozorpc_srv_release_context(rozorpc_srv_ctx_p);    
      return;
    }
    
    if (size > ruc_buf_getMaxPayloadLen(rozorpc_srv_ctx_p->decoded_arg)) {
      fatal("size of request %d is %d although max payload len is %d",
            hdr.proc, size, ruc_buf_getMaxPayloadLen(rozorpc_srv_ctx_p->decoded_arg));
    }
    
    memset(arguments,0, size);
    ruc_buf_setPayloadLen(rozorpc_srv_ctx_p->decoded_arg,size); // for debug 
    
    /*
    ** decode the payload of the rpc message
    */
    if (!rozorpc_srv_getargs_with_position (recv_buf, (xdrproc_t) rozorpc_srv_ctx_p->arg_decoder, 
                                            (caddr_t) arguments, &rozorpc_srv_ctx_p->position)) 
    {    
      rozorpc_srv_ctx_p->xmitBuf = rozorpc_srv_ctx_p->recv_buf;
      rozorpc_srv_ctx_p->recv_buf = NULL;
      rozorpc_srv_ctx_p->xdr_result = (xdrproc_t)xdr_sp_status_ret_t;
      arg_err.status = SP_FAILURE;
      arg_err.sp_status_ret_t_u.error = errno;        
      rozorpc_srv_forward_reply_rdma(rozorpc_srv_ctx_p,(char*)&arg_err);
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
    
    return;
    /*
    ** We fall there is there is an error on the queue pair (TMO, disconnection, etc...
    */
error_qp:
    warning("FDL RDMA error on qp %d error %s",msg->qp_num,strerror(msg->error));
    /*
    ** release the ruc_buffer
    */
    rozofs_rdma_release_rpc_buffer(msg->ruc_buf);
    return;
}

   
/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   Called from the socket controller when there is a message pending on the
   socket associated with the context provide in input arguments.
   
   That service is intended to process a response sent by a disk thread

    
  @param unused: user parameter not used by the application
  @param socketId: reference of the socket 
 
   @retval : TRUE-> xmit ready event expected
  @retval : FALSE-> xmit  ready event not expected
*/
uint32_t rdma_rpc_cmd_rcvMsgsock(void * unused,int socketId)
{
  rdma_rpc_cmd_msg_t   msg;
  int                  bytesRcvd;
  int eintr_count = 0;
  int max_msg_count = 4;
  int free_count;
#warning need to add a parameter in rozofs.conf for max_msg_count per activativation

  /*
  ** disk responses have the highest priority, loop on the socket until
  ** the socket becomes empty
  */
  while(max_msg_count > 0) {  
    /*
    ** check if there enough resources for RPC decoding.
    ** If there is no enough resources we exit from the loop
    */
    free_count = ruc_buf_getFreeBufferCount(decoded_rpc_buffer_pool);
    if (free_count == 0) goto out;
    
    /*
    ** read the north disk socket
    */
    bytesRcvd = recvfrom(socketId,
			 &msg,sizeof(msg), 
			 0,(struct sockaddr *)NULL,NULL);
    if (bytesRcvd == -1) {
     switch (errno)
     {
       case EAGAIN:
        /*
        ** the socket is empty
        */
        goto out;

       case EINTR:
         /*
         ** re-attempt to read the socket
         */
         eintr_count++;
         if (eintr_count < 3) continue;
         /*
         ** here we consider it as a error
         */
         severe ("Disk Thread Response error too many eintr_count %d",eintr_count);
         goto out;

       case EBADF:
       case EFAULT:
       case EINVAL:
       default:
         /*
         ** We might need to double checl if the socket must be killed
         */
         fatal("rdma_rpc_cmd_rcv error on recvfrom %s !!\n",strerror(errno));
         exit(0);
     }

    }
    if (bytesRcvd == 0) {
      fatal("rdma_rpc_cmd_rcv socket is dead %s !!\n",strerror(errno));
      exit(0);    
    } 
    storio_rdma_rpc_req_rcv_cbk(&msg); 
  }    
  
out:
  return TRUE;
}
/*
**__________________________________________________________________________
*/
/**
*   Call back used by the completion queue thread for post a rdma received
    from the completion queue
    That function is intended to refill also the share received queue with a ruc_buffer
    In case the pool is empty this will be done at the time the processing of the request
    will end.
    
    That function is executed in the context of the thread of the storio
    
    @param opcode: RDMA opcode
    @param ruc_buf: pointer to the ruc_buffer that contains the rpc message
    @param qp_num: reference of the queue pair on which the message has been received
    @param status: 0 for success -1 for error
    @param error: rdma error code when the status is -1 , 0 otherwise
    
    
    @retval none
*/

/*
**__________________________________________________________________________
*/
/**
*  Call back used upon receiving a RPC message over RDMA
   That call-back is called under the context onf the Completion Queue thread
   attached to a SRQ
   
   @param opcode: RDMA opcode MUST be IBV_WC_RECV
   @param ruc_buf: reference of the ruc_buffer that contains the encoded RPC message
   @param qp_num: reference of the QP on which the message has been received
   @param rozofs_rmda_ibv_cxt_p: pointer to the context of the adaptor from the rozofs side
   @param status: status of the operation (0 if no error)
   @param error: error code
*/

void storio_rdma_msg_recv_form_cq_cbk(int opcode,void *ruc_buf, uint32_t qp_num,void *rozofs_rmda_ibv_cxt_p,int status,int error)
{
    rdma_rpc_cmd_msg_t msg;
    int ret;

    /*
    ** check if the file descriptor used for sending data towards the main is valid
    */
    if (storio_rdma_cp_recv_fd == -1)
    {
       /*
       ** This is a fatal error it should not happen since that file descriptor must
       ** be created before the starting of the completion received threads that process the RDMA_READ & WRITE
       */
       fatal("storio_rdma_cp_recv_fd: does not exists");
    }
    msg.rdma_opcode = opcode;
    msg.status = status;
    msg.error = error;
    msg.qp_num = qp_num;
    msg.ruc_buf = ruc_buf;
    ret = sendto(storio_rdma_cp_recv_fd,&msg, sizeof(msg),0,(struct sockaddr*)&storio_rdma_south_socket_name,sizeof(storio_rdma_south_socket_name));
    if (ret <= 0) {
     fatal("storio_send_rdma rpc msg %d sendto(%s) %s", storio_rdma_cp_recv_fd, storio_rdma_south_socket_name.sun_path, strerror(errno));
     exit(0);  
    }
}
/*
**__________________________________________________________________________
*/

/**
* creation of the AF_UNIX socket that is attached on the socket controller

  That socket is used to receive back the response from the threads that
  perform disk operation (read/write/truncate)
  
  @param socketname : name of the socket
  
  @retval >= 0 : reference of the socket
  @retval < 0 : error
*/
int rozofs_rdma_af_unix_create_socket_for_rpc_cmd(char *socketname)
{
  int len;
  int fd = -1;
  void *sockctrl_ref;

   len = strlen(socketname);
   if (len >= AF_UNIX_SOCKET_NAME_SIZE)
   {
      /*
      ** name is too big!!
      */
      severe("socket name %s is too long: %d (max is %d)",socketname,len,AF_UNIX_SOCKET_NAME_SIZE);
      return -1;
   }
   while (1)
   {
     /*
     ** create the socket
     */
     fd = af_unix_sock_create_internal(socketname,DISK_SO_SENDBUF);
     if (fd == -1)
     {
       break;
     }
     /*
     ** OK, we are almost done, just need to connect with the socket controller
     */
     sockctrl_ref = ruc_sockctl_connect(fd,  // Reference of the socket
                                                RDMA_RPC_REQ,   // name of the socket
                                                16,                  // Priority within the socket controller
                                                (void*)NULL,      // user param for socketcontroller callback
                                                &rdma_rpc_cmd_callBack_sock);  // Default callbacks
      if (sockctrl_ref == NULL)
      {
         /*
         ** Fail to connect with the socket controller
         */
         fatal("error on ruc_sockctl_connect");
         break;
      }
      /*
      ** All is fine
      */
      break;
    }    
    return fd;
}

/*__________________________________________________________________________
* Initialize the rdma READ/WRITE thread interface
*
* @param hostname    storio hostname (for tests)

*
*  @retval 0 on success -1 in case of error
*/
int storio_rdma_rpc_cmd_intf_create(char * hostname, int instance_id) {

  int fileflags;
  /*
  ** init of the AF_UNIX sockaddr associated with the south socket (socket used for disk response receive)
  */
  storio_set_socket_name_with_hostname(&storio_rdma_south_socket_name,ROZOFS_SOCK_FAMILY_DISK_RDMA_SOUTH,hostname, instance_id);
    
  /*
  ** hostname is required for the case when several storaged run on the same server
  ** as is the case of test on one server only
  */   
  rdma_rpc_cmd_south_socket_ref = rozofs_rdma_af_unix_create_socket_for_rpc_cmd(storio_rdma_south_socket_name.sun_path);
  if (rdma_rpc_cmd_south_socket_ref < 0) {
    fatal("af_unix_sock_create(%s) %s",storio_rdma_south_socket_name.sun_path, strerror(errno));
    return -1;
  }
  /*
  ** Creat the common socket that will be used by the completion send fro posting message towards the main thread
  */
  storio_rdma_cp_recv_fd = socket(PF_UNIX,SOCK_DGRAM,0);
  if(storio_rdma_cp_recv_fd<0)
  {
    warning("af_unix socket creation failure %s", strerror(errno));
    return -1;
  }
  /*
  ** Change the mode of the socket to non blocking
  */
  if((fileflags=fcntl(storio_rdma_cp_recv_fd,F_GETFL,0))==-1)
  {
    warning("af_unix_sock_create_internal fcntl(F_GETFL %d) %s", storio_rdma_cp_recv_fd, strerror(errno));
    return -1;
  }
   
  return 0;
}
