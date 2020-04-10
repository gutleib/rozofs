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

#define _XOPEN_SOURCE 500

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stddef.h>
#include <fcntl.h>

#include <config.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/list.h>
#include <rozofs/common/log.h>
#include <rozofs/common/profile.h>
#include <rozofs/rpc/eproto.h>
#include <rozofs/core/rozofs_tx_common.h>
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/core/north_lbg_api.h>
//#include <rozofs/core/north_lbg_api_th.h>
#include <rozofs/rozofs_timer_conf.h>
#include "rozofs_fuse_api.h"
#include <rozofs/core/rozofs_host_list.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/rpc/expbt_protocol.h>
#include "rozofs_bt_inode.h"
#include "rozofs_bt_trk_reader.h"

/*
**____________________________________________________
*/
/**
* API for creation a transaction towards an storaged


 For making that API generic, the caller is intended to provide the function that
 will encode the message in XDR format. The source message that is encoded is 
 supposed to be pointed by msg2encode_p.
 Since the service is non-blocking, the caller MUST provide the callback function 
 that will be used for decoding the message
 

 @param sock_p     : pointer to the connection context (AF_INET or AF_UNIX)
 @param xmit_buf : pointer to the buffer to send, in case of error that function release the buffer
 @param recv_cbk   : receive callback function
 @param applicative_tmo_sec : guard timer for the transaction

 @param user_ctx_p : pointer to the working context
 
 @retval 0 on success;
 @retval -1 on error,, errno contains the cause
 */

int rozofs_bt_export_poll_tx(af_unix_ctx_generic_t  *sock_p,
                          void *xmit_buf,
                          int applicative_tmo_sec,                          
                          sys_recv_pf_t recv_cbk,void *user_ctx_p) 
{
   
    rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;
    int               ret;
    uint32_t sock_idx_in_lbg = -1;
    
    expbt_msg_hdr_t *msg_p;
    uint32_t xid;

   /*
   ** get the entry within the load balancing group
   */
   {
      north_lbg_ctx_t *lbg_p;
      int lbg_id = sock_p->availability_param;
      int start_idx;
      /*
      ** Get the pointer to the lbg
      */
      lbg_p = north_lbg_getObjCtx_p(lbg_id);
      if (lbg_p == NULL) 
      {
	severe("rozofs_bt_export_poll_tx: no such instance %d ",lbg_id);
	return -1;
      }      
      for (start_idx = 0; start_idx < lbg_p->nb_entries_conf; start_idx++)
      {
        if (lbg_p->entry_tb[start_idx].sock_ctx_ref  == sock_p->index) break;
      }
      if (lbg_p->nb_entries_conf == start_idx)
      {
	severe("rozofs_bt_export_poll_tx: no such instance %d ",sock_p->index);      
        return -1;
      }
      /**
      ** start_idx is the index within the load balancing
      */
      sock_idx_in_lbg = start_idx;
      
    }
    
    /*
    ** allocate a transaction context
    */
    rozofs_tx_ctx_p = rozofs_tx_alloc_th();  
    if (rozofs_tx_ctx_p == NULL) 
    {
       /*
       ** out of context
       ** --> put a pending list for the future to avoid repluing ENOMEM
       */
       TX_STATS_TH(ROZOFS_TX_NO_CTX_ERROR);
       errno = ENOMEM;
       goto error;
    }    
    /*
    ** get the pointer to the payload of the buffer
    */
    msg_p  = (expbt_msg_hdr_t*) ruc_buf_getPayload(xmit_buf);
    xid = rozofs_tx_alloc_xid(rozofs_tx_ctx_p);
    msg_p->xid = htonl(xid);
    msg_p->opcode = EXP_BT_NULL;
    msg_p->dir = 0;
    msg_p->len = sizeof(expbt_msg_hdr_t)-sizeof(uint32_t);
    /*
    ** set the payload length in the xmit buffer
    */
    int total_len = msg_p->len+sizeof(uint32_t);
    ruc_buf_setPayloadLen(xmit_buf,total_len);
    /*
    ** store the receive call back and its associated parameter
    */
    rozofs_tx_ctx_p->recv_cbk   = recv_cbk;
    rozofs_tx_ctx_p->user_param = user_ctx_p;    
    rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,1,1);  /* lock */
    rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,2,sock_p->availability_param);  /* lock */
    rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,3,sock_idx_in_lbg);  /* lock */
    /*
    ** now send the message
    */
    ret = af_unix_generic_stream_send(sock_p,xmit_buf); 
    if (ret < 0)
    {
       TX_STATS_TH(ROZOFS_TX_SEND_ERROR);
       errno = EFAULT;
      goto error;  
    }
    TX_STATS_TH(ROZOFS_TX_SEND);
    /*
    ** just iunlock the context and don't care about the end of transaction
    ** the transaction might end because of a direct error sending (tcp 
    ** disconnection)
    **
    ** By not releasing the tx context the end of the transaction ends upon receiving
    ** the tx timer expiration
    */
    rozofs_tx_write_opaque_data_th(rozofs_tx_ctx_p,1,0);  /* unlock */    
    /*
    ** OK, so now finish by starting the guard timer
    */
    rozofs_tx_start_timer_th(rozofs_tx_ctx_p,applicative_tmo_sec);  
    return 0;  
    
  error:
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr_th(rozofs_tx_ctx_p);
    return -1;    
}
/*
**____________________________________________________
*/
/**
*  
  Applicative Polling of a TCP connection towards Storage
  @param sock_p : pointer to the connection context
  
  @retval none
 */
  
extern void rozofs_bt_export_poll_cbk(void *this,void *param); 

void rozofs_bt_export_lbg_cnx_polling(af_unix_ctx_generic_t  *sock_p)
{
  void *xmit_buf = NULL;
  int ret;
  int timeout = (int)ROZOFS_TMR_GET(TMR_RPC_NULL_PROC_TCP);

  af_inet_set_cnx_tmo(sock_p,timeout*10*5);
  /*
  ** attempt to poll
  */
   xmit_buf = rozofs_tx_get_small_xmit_buf_th();
   if (xmit_buf == NULL)
   {
      return ; 
   }
   FDL_INFO("FDL xmit buffer %p",xmit_buf);
   ret =  rozofs_bt_export_poll_tx(sock_p,
                                        xmit_buf,
                                        timeout,   /** TMO in secs */
                                        rozofs_bt_export_poll_cbk,
                                        (void*)NULL);
  if (ret < 0)
  {
   /*
   ** direct need to free the xmit buffer
   */
   ruc_buf_freeBuffer(xmit_buf);    
  }
}


/*
**__________________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */

void rozofs_bt_export_poll_cbk(void *this,void *param) 
{
   uint32_t lock;
   uint32_t lbg_id;
   uint32_t sock_idx_in_lbg;
   int      active_entry;
   int status;
   void     *recv_buf = NULL;   
   int      bufsize;
   af_unix_ctx_generic_t  *sock_p;
   int timeout = (int)ROZOFS_TMR_GET(TMR_RPC_NULL_PROC_TCP);
   north_lbg_ctx_t *lbg_p;
   expbt_msg_t *rsp_msg_p;
   /*
   ** Restore opaque data
   */ 
   rozofs_tx_read_opaque_data(this,1,&lock);  
   rozofs_tx_read_opaque_data(this,2,&lbg_id);  
   rozofs_tx_read_opaque_data(this,3,&sock_idx_in_lbg);
   
   /*
   ** Read active entry of this LBG
   */
   active_entry = north_lbg_get_active_entry(lbg_id);
         
   /*   
   ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
   */
   status = rozofs_tx_get_status_th(this);
   if (status < 0)
   {
      /*
      ** something wrong happened
      */
      errno = rozofs_tx_get_errno_th(this);  
      goto error; 
   }
   /*
   ** get the pointer to the receive buffer payload
   */
   recv_buf = rozofs_tx_get_recvBuf_th(this);
   if (recv_buf == NULL)
   {
      /*
      ** something wrong happened
      */
      errno = EFAULT;  
      goto error;         
   }


   rsp_msg_p  = (expbt_msg_t*) ruc_buf_getPayload(recv_buf);
   /*
   ** Get the length of the payload
   */
   bufsize = (int) ruc_buf_getPayloadLen(recv_buf);
   bufsize -= sizeof(uint32_t); /* skip length*/
   if (bufsize != rsp_msg_p->hdr.len)
   {
    TX_STATS_TH(ROZOFS_TX_DECODING_ERROR);
    errno = EPROTO;
    goto error;
   }
   /*
   ** When this antry is now the active entry
   */
   north_lbg_set_active_entry(lbg_id,sock_idx_in_lbg);   
//   warning("Export connection UP index %d becomes active (old active %d) ",sock_idx_in_lbg,active_entry);   
   /*
   ** Check if there was a previous active entry 
   */  
   if ((active_entry != -1) && ( active_entry != sock_idx_in_lbg))
   {
      warning("Export connection index %d becomes active",sock_idx_in_lbg);    
       /*
       ** Need to tear down the other connection in order to requeue any pending request towards the export
       */
      lbg_p = north_lbg_getObjCtx_p(lbg_id);
      if (lbg_p == NULL) 
      {
	severe("rozofs_bt_export_poll_tx: no such instance %d ",lbg_id);
	goto out;
      }
      sock_p = af_unix_getObjCtx_p(lbg_p->entry_tb[active_entry].sock_ctx_ref); 
      if ( sock_p == NULL)
      {
         severe("No socket pointer for lbg_id %d entry %d", (int)lbg_id, (int)  lbg_p->entry_tb[active_entry].sock_ctx_ref);
	 goto out;
      }
      /*
      ** trigger an internal disconnection
      */
      warning("Export connection index %d disconnecting (socket %d)",active_entry,sock_p->index);
      af_unix_sock_stream_disconnect_internal(sock_p);  
      errno = EPROTO;    
      (sock_p->userDiscCallBack)(sock_p->userRef,sock_p->index,NULL,errno);      
   }
   goto out;
error:
   {
      /*
      ** Get the pointer to the lbg
      */
      lbg_p = north_lbg_getObjCtx_p(lbg_id);
      if (lbg_p == NULL) 
      {
	severe("rozofs_bt_export_poll_tx: no such instance %d ",lbg_id);
	goto out;
      }
      /*
      ** the context of the socket
      */      
      sock_p = af_unix_getObjCtx_p(lbg_p->entry_tb[sock_idx_in_lbg].sock_ctx_ref); 
      if ( sock_p == NULL)
      {
         severe("No socket pointer for lbg_id %d entry %d", (int)lbg_id, (int)  lbg_p->entry_tb[sock_idx_in_lbg].sock_ctx_ref);
	 goto out;
      }
      /*
      ** restart the timer
      */
      af_inet_set_cnx_tmo(sock_p,timeout*10*5);
      
    }

   /*
   ** When this antry used to be tha active entry, invalidate it
   ** in case of error
   */
   if (active_entry == sock_idx_in_lbg) {
     north_lbg_set_active_entry(lbg_id,-1);
   }

out:
   /*
   ** release the transaction context and the fuse context
   */
   if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);    
   if ( lock == 0) rozofs_tx_free_from_ptr_th(this);
   return;
}



 /**
 *  socket configuration for the family
 */
static af_unix_socket_conf_t  af_inet_export_bt_conf =
{
  1,  //           family: identifier of the socket family    */
  0,         /**< instance number within the family   */
  sizeof(uint32_t),  /* headerSize  -> size of the header to read                 */
  0,                 /* msgLenOffset->  offset where the message length fits      */
  sizeof(uint32_t),  /* msgLenSize  -> size of the message length field in bytes  */
  
  (1024*256), /*  bufSize        -> length of buffer (xmit and received)        */
  (300*1024), /*  so_sendbufsize -> length of buffer (xmit and received)        */
  rozofs_tx_userRcvAllocBufCallBack_th, /*  userRcvAllocBufCallBack -> user callback for buffer allocation             */
  rozofs_tx_recv_rpc_cbk_th,            /*  userRcvCallBack         -> callback provided by the connection owner block */
  rozofs_tx_xmit_abort_rpc_cbk_th,      /*  userDiscCallBack        ->callBack for TCP disconnection detection         */
  NULL,                              /* userConnectCallBack     -> callback for client connection only              */
  NULL,  //    userXmitDoneCallBack; /**< optional call that must be set when the application when to be warned when packet has been sent */
  NULL,  //    userRcvReadyCallBack; /* NULL for default callback                    */
  NULL,  //    userXmitReadyCallBack; /* NULL for default callback                    */
  NULL,  //    userXmitEventCallBack; /* NULL for default callback                    */
  NULL, //rozofs_tx_get_rpc_msg_len_cbk,        /* userHdrAnalyzerCallBack ->NULL by default, function that analyse the received header that returns the payload  length  */
  ROZOFS_GENERIC_SRV,       /* recv_srv_type ---> service type for reception : ROZOFS_RPC_SRV or ROZOFS_GENERIC_SRV  */
  0,       /*   rpc_recv_max_sz ----> max rpc reception buffer size : required for ROZOFS_RPC_SRV only */
  NULL,  //    *userRef;           /* user reference that must be recalled in the callbacks */
  NULL,  //    *xmitPool; /* user pool reference or -1 */
  NULL   //    *recvPool; /* user pool reference or -1 */
};
/*
**________________________________________________________________________________________________________
*/
/**
   Init of a load balancing group
   
   For the addressing only the port is provided. The IP@ are retrieved thanks rozofs_host_list_get_host()
   
   @param module_p: pointer to the socket controller context
   @param port_num: TCP port of the service
   @param supervision_callback: supervision callback
   
   @retval <> 0: reference of the load balancing group
   @retval < 0 error, see errno for details
*/
int rozofs_bt_lbg_initialize(void *module_p,uint32_t port_num,af_stream_poll_CBK_t supervision_callback) {
    int status = -1;
    struct sockaddr_in server;
    int lbg_size;
    int export_index=0;
    char * pHost;
    north_remote_ip_list_t my_list[32];  /**< list of the connection for the exportd */    
    int export_lbg_id; 
    af_unix_socket_conf_t *conf_p;

    server.sin_family = AF_INET;
    
    conf_p = malloc(sizeof(af_unix_socket_conf_t));
    if (conf_p == NULL)
    {
       severe("Out of Memory");
       fatal("Bye");
    }
    memcpy(conf_p,&af_inet_export_bt_conf,sizeof(af_inet_export_bt_conf));
    conf_p->socketCtrl_p = module_p;
    /*
    ** set the max receive buffer size
    */
    conf_p->bufSize = ROZOFS_BT_TRK_CLI_LARGE_RECV_SIZE;

    lbg_size = 0;
    for (export_index=0; export_index < ROZOFS_HOST_LIST_MAX_HOST; export_index++) { 

        pHost = rozofs_host_list_get_host(export_index);
	if (pHost == NULL) break;

	if (rozofs_host2ip_netw((char*)pHost, &server.sin_addr.s_addr) != 0) {
            severe("rozofs_host2ip failed for host : %s, %s", pHost,
                    strerror(errno));
            continue;
	}

        server.sin_port = htons(port_num);
	/*
	** store the IP address and port in the list of the endpoint
	*/
	my_list[lbg_size].remote_port_host = ntohs(server.sin_port);
	my_list[lbg_size].remote_ipaddr_host = ntohl(server.sin_addr.s_addr);
	lbg_size++;
    }
    if (lbg_size == 0) goto out;
    
    conf_p->rpc_recv_max_sz = rozofs_large_tx_recv_size;

    /*
    ** allocate a load balancing group
    */
    export_lbg_id = north_lbg_create_no_conf();
    if (export_lbg_id < 0)
    {     
       /*
       ** cannot create the load balancing group
       */
       severe("Cannot create Load Balancing Group for Exportd");
       goto out;
    }

    if (supervision_callback != NULL)
    {	
      int ret = north_lbg_attach_application_supervision_callback(export_lbg_id,supervision_callback);
      if (ret < 0)
      {
         /*
	 ** cannot create the load balancing group
	 */
	 severe("failure while configuring EXPORTD load balancing group");
	 goto out;     
      }
      ret = north_lbg_set_application_tmo4supervision(export_lbg_id,30);
      if (ret < 0)
      {
         /*
	 ** cannot create the load balancing group
	 */
	 severe("failure while configuring EXPORTD load balancing group");
	 goto out;     
      }
      north_lbg_set_active_standby_mode(export_lbg_id);
    }
    
    /*
    ** Request to rechain request on LBG on disconnection
    */
    north_lbg_rechain_when_lbg_gets_down(export_lbg_id);
    
    /*
    ** set the dscp code
    */
    conf_p->dscp = (uint8_t) common_config.export_dscp; 
    conf_p->dscp = conf_p->dscp<<2;    
    export_lbg_id = north_lbg_configure_af_inet_th(export_lbg_id,"EXP_BT",INADDR_ANY,0,my_list,ROZOFS_SOCK_FAMILY_EXPORT_NORTH,
                                                  lbg_size,conf_p,0);
    if (export_lbg_id >= 0)
    {
      /*
      ** the timer is started only to address the case of a dynamic port
      */
      status = 0;
      return export_lbg_id;    
    }
    severe("Cannot create Load Balancing Group for Exportd");

out:
    return  status;
}   


/*
**______________________________________________________________________________
*/
/**
*  Add an eid entry 

  @param exportd_id : reference of the exportd that manages the eid
  @param eid: eid within the exportd
  @param hostname: hostname of the exportd
  @param port:   port of the exportd
  @param nb_gateways : number of gateways
  @param gateway_rank : rank of the current export gateway
  
  @retval 0 on success
  @retval -1 on error (see errno for details)
  
*/

int rozofs_bt_create_client_lbg(void *module_p)
{
  uint16_t io_port = rozofs_get_service_port_expbt(0);

  /*
  ** create the load balancing group
  */
 return rozofs_bt_lbg_initialize(module_p,io_port, (af_stream_poll_CBK_t) rozofs_bt_export_lbg_cnx_polling);
}                
