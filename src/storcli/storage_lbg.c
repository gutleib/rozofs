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



#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/common_config.h>
#include <rozofs/common/xmalloc.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <errno.h>
#include <string.h>
#include <rpc/pmap_clnt.h>
#include <unistd.h>

#include <rozofs/core/rozofs_socket_family.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/core/ruc_common.h>
#include <rozofs/core/ruc_sockCtl_api.h>
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/core/north_lbg_api.h>
#include <rozofs/rpc/eclient.h>
#include <rozofs/rpc/rpcclt.h>
#include "rozofs_storcli.h"
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/rdma/rozofs_rdma.h>
#include "rdma_client_send.h"
#include "standalone_client_send.h"

static north_remote_ip_list_t my_list[STORAGE_NODE_PORTS_MAX];  /**< list of the connection for the exportd */

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
void * storage_lbg_userRcvAllocBufCallBack(void *userRef, uint32_t socket_context_ref, uint32_t len) {

   void *buf=NULL;
    /*
     ** check if a small or a large buffer must be allocated
     */
    if (len <= rozofs_storcli_south_small_buf_sz) {
        buf = ruc_buf_getBuffer(ROZOFS_STORCLI_SOUTH_SMALL_POOL);
	if (buf != NULL) return buf;
    }

    if (len <= rozofs_storcli_south_large_buf_sz) {
        return ruc_buf_getBuffer(ROZOFS_STORCLI_SOUTH_LARGE_POOL);
    }
    fatal("Out of range received size: %d (max %d)",len,rozofs_storcli_south_large_buf_sz);
    return NULL;
}

 /**
 *  socket configuration for the family
 */
static af_unix_socket_conf_t  af_inet_storaged_conf =
{
  1,  //           family: identifier of the socket family    */
  0,         /**< instance number within the family   */
  sizeof(uint32_t),  /* headerSize  -> size of the header to read                 */
  0,                 /* msgLenOffset->  offset where the message length fits      */
  sizeof(uint32_t),  /* msgLenSize  -> size of the message length field in bytes  */
  
  (1024*256), /*  bufSize        -> length of buffer (xmit and received)        */
  (300*1024), /*  so_sendbufsize -> length of buffer (xmit and received)        */
  storage_lbg_userRcvAllocBufCallBack, /*  userRcvAllocBufCallBack -> user callback for buffer allocation             */
  rozofs_tx_recv_rpc_cbk,            /*  userRcvCallBack         -> callback provided by the connection owner block */
  rozofs_tx_xmit_abort_rpc_cbk,      /*  userDiscCallBack        ->callBack for TCP disconnection detection         */
  NULL,                              /* userConnectCallBack     -> callback for client connection only              */
  NULL,  //    userXmitDoneCallBack; /**< optional call that must be set when the application when to be warned when packet has been sent */
  NULL,  //    userRcvReadyCallBack; /* NULL for default callback                    */
  NULL,  //    userXmitReadyCallBack; /* NULL for default callback                    */
  NULL,  //    userXmitEventCallBack; /* NULL for default callback                    */
  rozofs_tx_get_rpc_msg_len_cbk,        /* userHdrAnalyzerCallBack ->NULL by default, function that analyse the received header that returns the payload  length  */
  ROZOFS_RPC_SRV,       /* recv_srv_type ---> service type for reception : ROZOFS_RPC_SRV or ROZOFS_GENERIC_SRV  */
  0,       /*   rpc_recv_max_sz ----> max rpc reception buffer size : required for ROZOFS_RPC_SRV only */

  NULL,  //    *userRef;           /* user reference that must be recalled in the callbacks */
  NULL,  //    *xmitPool; /* user pool reference or -1 */
  NULL   //    *recvPool; /* user pool reference or -1 */
};




int storcli_next_storio_global_index =0;

int storaged_lbg_initialize(mstorage_t *s, int index) {
    int lbg_size;
    int ret;
    int i;
    int local=1;
    
    DEBUG_FUNCTION;    
#if 0
    
    /*
    ** configure the callback that is intended to perform the polling of the storaged on each TCP connection
    */
   ret =  north_lbg_attach_application_supervision_callback(s->lbg_id[index],(af_stream_poll_CBK_t)storcli_lbg_cnx_polling);
   if (ret < 0)
   {
     severe("Cannot configure Soraged polling callback");   
   }


   ret =  north_lbg_set_application_tmo4supervision(s->lbg_id[index],20);
   if (ret < 0)
   {
     severe("Cannot configure application TMO");   
   }   
#endif
   /*
   ** set the dscp for storio connections
   */
   af_inet_storaged_conf.dscp=(uint8_t)common_config.storio_dscp;
   af_inet_storaged_conf.dscp = af_inet_storaged_conf.dscp <<2;
    /*
    ** store the IP address and port in the list of the endpoint
    */
    lbg_size = s->sclients_nb;
    for (i = 0; i < lbg_size; i++)
    {
      my_list[i].remote_port_host   = s->sclients[i].port;
      my_list[i].remote_ipaddr_host = s->sclients[i].ipv4;
      if (!is_this_ipV4_local(s->sclients[i].ipv4)) local = 0;
    }
     af_inet_storaged_conf.recv_srv_type = ROZOFS_RPC_SRV;
     af_inet_storaged_conf.rpc_recv_max_sz = rozofs_storcli_south_large_buf_sz;
#ifdef ROZOFS_RDMA
     /*
     ** RDMA is supported only when the lbg_size is 1
     */
     while(1)
     {
     if (lbg_size==1)
     {
       /*
       ** check if the lbg is local: for the local case we use the standalone code
       */
       if ((local == 0) || (common_config.standalone == 0))
       {
          info ("RDMA is enabled: LBG operates in RDMA mode");
          ret = north_lbg_configure_af_inet_with_rdma_support(s->lbg_id[index],
	                                                      rozofs_rdma_tcp_client_connect_CBK,
							      rozofs_rdma_tcp_client_dis_CBK,
							      rozofs_rdma_tx_out_of_seq_cbk);
	  if (ret < 0)
	  {
	    severe("Cannot create Load Balancing Group %d for storaged %s",s->lbg_id[index],s->host);
	    return -1; 
	  } 
	  break;							         
       }
       if ((local == 1) && (common_config.standalone == 1))
       {
	 /*
	 ** use the standalone code
	 */     
          info ("RDMA is enabled: LBG operates in Standalone  mode");
	 ret = north_lbg_configure_af_inet_with_rdma_support(s->lbg_id[index],
	                                                     rozofs_standalone_tcp_client_connect_CBK,
							     rozofs_standalone_tcp_client_dis_CBK,
							     rozofs_standalone_tx_out_of_seq_cbk);
	 if (ret < 0)
	 {
	   severe("Cannot create Load Balancing Group %d for storaged %s",s->lbg_id[index],s->host);
	   return -1; 
	 } 		
       }
       break;
     }
     break;
     }	
#endif              
     /*
     ** Check the case of the standalone mode: 
     ** note: rdma and standalone mode are exclusive: if standalone mode is active, RDMA cannot not be activated
     */
     
     if ((local == 1) && (common_config.standalone == 1))
     {
       /*
       ** standalone is supported only when the lbg_size is 1
       */
       if (lbg_size==1)
       {
     
          ret = north_lbg_configure_af_inet_with_rdma_support(s->lbg_id[index],
	                                                      rozofs_standalone_tcp_client_connect_CBK,
							      rozofs_standalone_tcp_client_dis_CBK,
							      rozofs_standalone_tx_out_of_seq_cbk);
	  if (ret < 0)
	  {
	    severe("Cannot create Load Balancing Group %d for storaged %s",s->lbg_id[index],s->host);
	    return -1; 
	  } 							         
       }          
     }

     ret = north_lbg_configure_af_inet(s->lbg_id[index],
                                          s->host,
                                          INADDR_ANY,0,
                                          my_list,
                                          ROZOFS_SOCK_FAMILY_STORAGE_NORTH,lbg_size,&af_inet_storaged_conf, local);
     if (ret < 0)
     {
      severe("Cannot create Load Balancing Group %d for storaged %s",s->lbg_id[index],s->host);
      return -1;    
     }
     
     /*
     ** Mark this LBG as dedicated to a storage
     */
     storcli_lbg_cnx_supervision_tab[s->lbg_id[index]].storage = 1;
     
     north_lbg_set_next_global_entry_idx_p(s->lbg_id[index],&storcli_next_storio_global_index);
     return  0;
}     

