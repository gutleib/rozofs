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
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <syslog.h>
#include <libgen.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <pthread.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/common_config.h>
#include <rozofs/common/list.h>
#include <rozofs/core/ruc_common.h>
#include <rozofs/core/ruc_sockCtl_api.h>
#include <rozofs/core/ruc_timer_api.h>
#include <rozofs/core/uma_tcp_main_api.h>
#include <rozofs/core/af_inet_stream_api.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/ruc_tcpServer_api.h>
#include <rozofs/core/ruc_tcp_client_api.h>
#include <rozofs/core/uma_well_known_ports_api.h>
#include <rozofs/core/rozofs_rcmd.h>
#include <rozofs/core/rozofs_core_files.h>
#include <rozofs/rozofs_service_ports.h>


#if 0
#define DBG(fmt, ...) info(fmt, ##__VA_ARGS__)
#else
#define DBG(fmt, ...)
#endif


#define ROZOFS_UTILITY_NAME "diag"

#define ROZOFS_DIAG_SRV_MAX_CNX     256


/*
** Context structure for a RozoFS connected module
*/
typedef struct _rozofs_diag_srv_ctx_t {
  list_t        list;       /**< To chain the used server/client context together */
  uint32_t      remoteIP;   /**< IP address of the connected client/server */
  uint16_t      remotePort; /**< port of the connected client/server */
  uint16_t      server:1;   /**< Server or client */
  uint16_t      used:1;     /**< whether this entry is in use */     
  char       *  target;     /**< registered server target */
} rozofs_diag_srv_ctx_t;

rozofs_diag_srv_ctx_t  * rozofs_diag_srv_ctx_tb = NULL;
list_t                   usedClientList;
list_t                   usedServerList;

void *rozo_diag_srv_buffer_pool_p = NULL;  /**< reference of the buffer pool */
/*
**____________________________________________________
**
** Give the context index from its address
**
** @param pCtx    The address of the context
**
** @retval the context index or -1 when this is not a context index
**____________________________________________________
*/
int get_cnx_idx_from_ctx(rozofs_diag_srv_ctx_t * pCtx) {
  uint64_t idx;
  
  idx = (uint64_t) pCtx;
  idx -= (uint64_t)rozofs_diag_srv_ctx_tb;
  idx /= sizeof(rozofs_diag_srv_ctx_t);
  if (idx >= ROZOFS_DIAG_SRV_MAX_CNX) {
    return -1;
  }
  return idx;   
}
/*
**____________________________________________________
**
** Get intenal debug connection from the registered 
** rozodiag server to himself
**
**____________________________________________________
*/
int get_cnx_idx_internal() {
  list_t                * l;
  rozofs_diag_srv_ctx_t * pCtx;
   
  list_for_each_forward(l, &usedServerList) {
    pCtx = list_entry(l, rozofs_diag_srv_ctx_t, list);	
    if (strcmp(pCtx->target,ROZOFS_UTILITY_NAME)== 0) return get_cnx_idx_from_ctx(pCtx);	
  }
  return  -1;     
}
/*
**____________________________________________________
**
** Man for the servers CLI
**____________________________________________________
*/
void rozofs_diag_srv_server_man(char * pChar) {
  pChar += rozofs_string_append (pChar,"Display registered server information\n");
}
/*
**____________________________________________________
**
** Servers cli
**____________________________________________________  
*/
void rozofs_diag_srv_server_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
  list_t                * l;
  rozofs_diag_srv_ctx_t * pCtx;
  int                     idx;
  int                     first=1;
  char                  * pChar=uma_dbg_get_buffer();
  ;
  pChar += rozofs_string_append(pChar,"{ \"servers\" : [\n");
   
  list_for_each_forward(l, &usedServerList) {
   
    pCtx = list_entry(l, rozofs_diag_srv_ctx_t, list);	
    idx = get_cnx_idx_from_ctx(pCtx);	
    
    if (first) {
      pChar += rozofs_string_append(pChar,"   { \"idx\" : ");
      first = 0;
    }
    else {
      pChar += rozofs_string_append(pChar,"  ,{ \"idx\" : ");
    }
    pChar += rozofs_u32_padded_append(pChar, 3, rozofs_right_alignment,idx);
    if (pCtx->server) pChar += rozofs_string_append(pChar,", \"role\" : \"server\", \"address\" : \"");
    else              pChar += rozofs_string_append(pChar,", \"role\" : \"client\", \"address\" : \"");    
    pChar += rozofs_ipv4_append(pChar,pCtx->remoteIP);
    pChar += rozofs_string_append(pChar,"\", \"port\" : ");    
    pChar += rozofs_u32_padded_append(pChar, 5, rozofs_right_alignment,pCtx->remotePort);
    pChar += rozofs_string_append(pChar,", \"target\" : \"");
    pChar += rozofs_string_append(pChar,pCtx->target);
    pChar += rozofs_string_append(pChar,"\" }\n");
  }
  pChar += rozofs_string_append(pChar,"]}\n ");
  
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
  return;       
}
/*
**____________________________________________________
**
** Man of the clients cli
**____________________________________________________
*/
void rozofs_diag_srv_client_man(char * pChar) {
  pChar += rozofs_string_append (pChar,"Display connected clients information\n");
}
/*
**____________________________________________________
**
** client cli 
**____________________________________________________  
*/
void rozofs_diag_srv_client_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
  list_t                * l;
  rozofs_diag_srv_ctx_t * pCtx;
  int                     idx;
  int                     first=1;
  char                  * pChar=uma_dbg_get_buffer();
  ;
  pChar += rozofs_string_append(pChar,"{ \"clients\" : [\n");
   
  list_for_each_forward(l, &usedClientList) {
   
    pCtx = list_entry(l, rozofs_diag_srv_ctx_t, list);	
    idx = get_cnx_idx_from_ctx(pCtx);	
    
    if (first) {
      pChar += rozofs_string_append(pChar,"   { \"idx\" : ");
      first = 0;
    }
    else {
      pChar += rozofs_string_append(pChar,"  ,{ \"idx\" : ");
    }
    pChar += rozofs_u32_padded_append(pChar, 3, rozofs_right_alignment,idx);
    if (pCtx->server) pChar += rozofs_string_append(pChar,", \"role\" : \"server\", \"address\" : \"");
    else              pChar += rozofs_string_append(pChar,", \"role\" : \"client\", \"address\" : \"");    
    pChar += rozofs_ipv4_append(pChar,pCtx->remoteIP);
    pChar += rozofs_string_append(pChar,"\", \"port\" : ");    
    pChar += rozofs_u32_padded_append(pChar, 5, rozofs_right_alignment,pCtx->remotePort);
    pChar += rozofs_string_append(pChar,", \"target\" : \"");
    pChar += rozofs_string_append(pChar,pCtx->target);
    pChar += rozofs_string_append(pChar,"\" }\n");
  }
  pChar += rozofs_string_append(pChar,"]}\n ");
  
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
  return;       
}
/*
**____________________________________________________
**
** Re-initialize a rozofs diagnostic context
** 
** @param pCtx   Serveur context
**____________________________________________________
*/
static inline void rozo_diag_srv_ctx_reinit(rozofs_diag_srv_ctx_t * pCtx) {
  memset(pCtx,0, sizeof(*pCtx));  
  list_init(&pCtx->list); 
}  
/*
**____________________________________________________
**
** Free a rozofs diagnostic context
** 
** @param pCtx   Serveur context
**____________________________________________________
*/
void rozo_diag_srv_ctx_disconnect(rozofs_diag_srv_ctx_t * pCtx) {
  uint64_t idx;

  list_remove(&pCtx->list);
    
  /*
  ** Free the nick name
  */
  if (pCtx->target) {
    free(pCtx->target);
    pCtx->target = NULL;
  }  
  rozo_diag_srv_ctx_reinit(pCtx);
  
  /*
  ** Disconnect this socket from the socket controler
  ** The context index is the socket controler index.
  */
  idx = get_cnx_idx_from_ctx(pCtx);
  if (idx >= 0) {
    af_unix_delete_socket(idx);  
  }  
}  
/*
**____________________________________________________
**
** Register a server with the name it gives
**____________________________________________________
*/
void rozo_diag_srv_register(rozofs_diag_srv_ctx_t * pCtx,UMA_MSGHEADER_S * pHead, void *recv_buf) {
  char * pChar;
  
  
  pCtx->server = 1;
  
  /*
  ** Memorize the target name given by the server
  */
  if (pCtx->target) {
    free(pCtx->target);
    pCtx->target = NULL;
  }  
  pChar = (char *) (pHead+1);
  /*
  ** Remove trailing spaces
  */
  {
    char * p = pChar;
    while ((*p!=0)&&(*p!= ' ')) p++;
    if (*p == ' ') *p = 0;
  }
  pCtx->target = strdup(pChar);

  /*
  ** Remove it from the client list, and set it into the server list
  */
  list_remove(&pCtx->list);
  list_push_back(&usedServerList,&pCtx->list);
   
  
  DBG("receive register %s on socket %d",pCtx->target, get_cnx_idx_from_ctx(pCtx));
  
  ruc_buf_freeBuffer(recv_buf);         
}
/*
**____________________________________________________
**
** Forward an input command toward a registered server 
** or an input response toward the client requester
**
** @param pFrom      The context issuing the request/response
** @param pHead      The header of the received message
** @param revc_buff  The received buffer
 
**____________________________________________________
*/
void rozo_diag_srv_forward(rozofs_diag_srv_ctx_t * pFrom,UMA_MSGHEADER_S * pHead, void *recv_buf) {
  rozofs_diag_srv_ctx_t * pTo; 

  /*
  ** Retrieve destination to forward to in the fwd field of the header 
  */
  pTo = rozofs_diag_srv_ctx_tb + pHead->fwd;  
  
  /*
  ** The destination may be disconnected now...
  */
  if (!pTo->used) {
    
    ruc_buf_freeBuffer(recv_buf);
  
    if (pFrom->server) {
      /*
      ** Client does not exist any more. Do not forward the response.
      */
      ruc_buf_freeBuffer(recv_buf);
      return;
    }
    /*
    ** Server does not exist. disconnect the requester client.
    */    
    rozo_diag_srv_ctx_disconnect(pFrom);
    return;
  }  
  
  /*
  ** Response from server
  */
  if (pFrom->server) {
    /*
    ** Forward to client
    */
    af_unix_generic_send_stream_with_idx(get_cnx_idx_from_ctx(pTo),recv_buf);
    return;
  }
  
  /*
  * Request from client
  */
  
  /*
  ** Save client index in the header to be able to forward the response to the client
  */
  pHead->fwd = get_cnx_idx_from_ctx(pFrom);
  
  /*
  ** Forward to server
  */
  af_unix_generic_send_stream_with_idx(get_cnx_idx_from_ctx(pTo),recv_buf);
  DBG("forwarded to srv %s on socket %d",pTo->target, get_cnx_idx_from_ctx(pTo));       
}
/*
**____________________________________________________
**
** Initialize the service
**____________________________________________________
*/
void rozofs_diag_srv_ctx_init() {
  rozofs_diag_srv_ctx_t * pCtx;
  int                     idx;

  /*
  ** ALready initialized ?
  */
  if (rozofs_diag_srv_ctx_tb != NULL) return;
  
  /*
  ** Initialize client as well as server lists
  */
  list_init(&usedClientList);
  list_init(&usedServerList);
  
  /*
  ** Allocate memory for the contexts
  */
  rozofs_diag_srv_ctx_tb = xmalloc(sizeof(rozofs_diag_srv_ctx_t)*ROZOFS_DIAG_SRV_MAX_CNX);
  if (rozofs_diag_srv_ctx_tb == NULL) {
    fatal("xmalloc");
  }
  
  /*
  ** Initialize the contexts
  */
  pCtx = rozofs_diag_srv_ctx_tb;
  for (idx=0; idx<ROZOFS_DIAG_SRV_MAX_CNX; idx++,pCtx++) {
    rozo_diag_srv_ctx_reinit(pCtx);
  }  

  /*
  ** register some diagnostic clis
  */
  uma_dbg_addTopicAndMan("servers", rozofs_diag_srv_server_debug, rozofs_diag_srv_server_man, 0); 
  uma_dbg_addTopicAndMan("clients", rozofs_diag_srv_client_debug, rozofs_diag_srv_client_man, 0); 
   
}

 /*
**____________________________________________________
**
** Callback to allocate a buffer while receiving a message
** 
** The service might reject the buffer allocation because the pool runs
** out of buffer or because there is no pool with a buffer that is large enough
** for receiving the message because of a out of range size.
**
** @param userRef : pointer to a user reference: not used here
** @param socket_context_ref: socket context reference
** @param len : length of the incoming message
** 
** @retval <>NULL pointer to a receive buffer
** @retval == NULL no buffer 
**____________________________________________________
*/
void * rozo_diag_srv_userRcvAllocBufCallBack(void *userRef,uint32_t socket_context_ref,uint32_t len) {

  /*
  ** We need at least a response buffer
  */
  uint32_t free_count = ruc_buf_getFreeBufferCount(rozo_diag_srv_buffer_pool_p);  
  if (free_count < 1)
  {
    return NULL;
  }

  return ruc_buf_getBuffer(rozo_diag_srv_buffer_pool_p);      
}
/*
**____________________________________________________
**
** Disconnect callback
** 
** @param userRef : pointer to a user reference: not used here
** @param socket_context_ref: socket context reference
** @param bufRef : pointer to the packet buffer on which the error has been encountered
** @param err_no : errno has reported by the sendto().
** 
**____________________________________________________
*/
void rozo_diag_srv_userDiscCallBack(void *userRef,uint32_t socket_context_ref,void *bufRef,int err_no) {
  DBG("rozo_diag_srv_userDiscCallBack %d",socket_context_ref);
  rozofs_diag_srv_ctx_t  * pCtx;

  /*
  ** release the current buffer if significant
  */
  if (bufRef != NULL) ruc_buf_freeBuffer(bufRef);

  if (socket_context_ref >= ROZOFS_DIAG_SRV_MAX_CNX) {
    severe("Bad socketCtrl index %d",socket_context_ref);
    af_unix_delete_socket(socket_context_ref);
    return;
  }
  /*
  ** The context index is the socket controler index
  */
  pCtx = rozofs_diag_srv_ctx_tb + socket_context_ref;
  
  if (pCtx->used) {
    rozo_diag_srv_ctx_disconnect(pCtx);
    return;
  }  
    
  /*
  ** release the socket controler index
  */
  af_unix_delete_socket(socket_context_ref);   
}
/*
**____________________________________________________
**
** Receiver ready function: called from socket controller.
** The module is intended to return if the receiver is ready to receive a new message
** and FALSE otherwise
**   
** The application is ready to received if the buffer pool is not empty
**
**  @param socket_ctx_p: pointer to the af unix socket
**  @param socketId: reference of the socket (not used)
**
**  @retval : TRUE-> receiver ready
*  @retval : FALSE-> receiver not ready
**____________________________________________________
*/
uint64_t last_out_of_buffer_sec = 0;
uint32_t rozo_diag_srv_userRcvReadyCallBack(void * socket_ctx_p,int socketId) {

  uint32_t free_count = ruc_buf_getFreeBufferCount(rozo_diag_srv_buffer_pool_p);

  /*
  ** Check that we are not out of buffer for more than 60 seconds.
  ** Else better suicide...
  */
  if (free_count < 1) {
    if (last_out_of_buffer_sec == 0) {
      last_out_of_buffer_sec = rozofs_get_ticker_s();
    }
    else {
      if ((rozofs_get_ticker_s()-last_out_of_buffer_sec) > 60) {
        fatal("All buffers are lost");
      } 
    }
    return FALSE;
  }

  last_out_of_buffer_sec = 0;
  return TRUE;
}
/*
**____________________________________________________
**
** rozodiag server message dispatcher
**    
**  @param socket_ctx_p: pointer to the af unix socket
**  @param socketId: reference of the socket (not used)
**  @param recv_buf: received message buffer
** 
**  @retval : TRUE-> xmit ready event expected
**  @retval : FALSE-> xmit  ready event not expected
*/
void uma_dbg_userRcvCallBack(void *userRef,uint32_t  tcpCnxRef, void *bufRef) ;
void rozo_diag_srv_userRcvCallBack(void *userRef,uint32_t  socket_context_ref, void *recv_buf) {
  UMA_MSGHEADER_S        * pHead;
  rozofs_diag_srv_ctx_t  * pCtx;
  int                      idx;

  if (socket_context_ref >= ROZOFS_DIAG_SRV_MAX_CNX) {
    severe("Bad socketCtrl index %d",socket_context_ref);
    af_unix_delete_socket(socket_context_ref);
    return;
  }
  /*
  ** The context index is the socket controler index
  */
  pCtx = rozofs_diag_srv_ctx_tb + socket_context_ref;
  
  if (!pCtx->used) {
    severe("ctx %d not in use by %s", socket_context_ref, (pCtx->target==NULL)?"?":pCtx->target);
    af_unix_delete_socket(socket_context_ref);
    return;
  }  
  
  pHead = (UMA_MSGHEADER_S *) ruc_buf_getPayload(recv_buf); 
  if (pHead == NULL) {
    return;
  }  

  switch (pHead->action)  {
   
    /*
    ** Direct CLI for the rozodiag server
    ** Let's forward it to himself.
    */
    case ROZOFS_DIAG_DIRECT:
      DBG("received ROZOFS_DIAG_DIRECT(%d)",pHead->action);
      idx = get_cnx_idx_internal();
      pHead->action = ROZOFS_DIAG_FWD;
      pHead->fwd = idx;
      rozo_diag_srv_forward(pCtx,pHead,recv_buf);    
      return;
      
    /*
    ** forward 
    */
    case ROZOFS_DIAG_FWD:
      DBG("received ROZOFS_DIAG_FWD(%d)",pHead->action);
      rozo_diag_srv_forward(pCtx,pHead,recv_buf);    
      return;

    /*
    ** Server registration
    */
    case ROZOFS_DIAG_SRV_REGISTER:
      rozo_diag_srv_register(pCtx,pHead,recv_buf);
      return;

    default:
      warning("received ? (%d)",pHead->action);
      ruc_buf_freeBuffer(recv_buf);    
      rozo_diag_srv_ctx_disconnect(pCtx);
      return;        
  }      
      
  ruc_buf_freeBuffer(recv_buf);
}
/*
**____________________________________________________
**
** Incomming connection
**    
** @param userRef : pointer to a user reference: not used here
** @param socket_context_ref: socket context reference
** @param retcode : ROCÃ_OK/RUC_NOK
** @param errnum : errno when RUC_NOK
*/
void rozo_diag_srv_userConnectCallBack(void *userRef,uint32_t socket_context_ref,int retcode,int errnum) {
  char       who[64];
  rozofs_diag_srv_ctx_t  * pCtx;
    
  if (socket_context_ref >= ROZOFS_DIAG_SRV_MAX_CNX) {
    severe("Bad socketCtrl index %d",socket_context_ref);
    af_unix_delete_socket(socket_context_ref);
    return;
  }
  /*
  ** The context index is the socket controler index
  */
  pCtx = rozofs_diag_srv_ctx_tb + socket_context_ref;
  
  if (pCtx->used) {
    severe("ctx %d in use by %s", socket_context_ref, (pCtx->target==NULL)?"?":pCtx->target);
    rozo_diag_srv_ctx_disconnect(pCtx);
    return;
  }  
  
  if (af_unix_get_remote_ip_and_port(socket_context_ref, &pCtx->remoteIP, &pCtx->remotePort) != 0) {
    severe("af_unix_get_remote_ip_and_port(%d)", socket_context_ref);
    rozo_diag_srv_ctx_disconnect(pCtx);
    return;
  }
  rozofs_ipv4_port_append(who,pCtx->remoteIP,pCtx->remotePort);
  pCtx->target = strdup(who);
  pCtx->used = 1;
  
  /*
  ** So far it is a client, unless it later registers as a server
  */
  list_remove(&pCtx->list);
  list_push_back(&usedClientList,&pCtx->list);
   
  DBG("rozo_diag_srv_userConnectCallBack %d from %s ",socket_context_ref, who);
}
/*
**____________________________________________________
**
** Receiver ready function: called from socket controller.
** The module is intended to return if the receiver is ready to receive a new message
** and FALSE otherwise
**   
** The application is ready to received if the buffer pool is not empty
**
**  @param socket_ctx_p: pointer to the af unix socket
**  @param socketId: reference of the socket (not used)
**
**  @retval : TRUE-> receiver ready
*  @retval : FALSE-> receiver not ready
**____________________________________________________
*/
uint32_t rozo_diag_srv_userHdrAnalyzerCallBack(char * payload) {
  uint32_t len;
  UMA_MSGHEADER_S * pHead = (UMA_MSGHEADER_S *) payload;  
  len = ntohl(pHead->len);
  if (len > UMA_DBG_MAX_SEND_SIZE) {
    DBG("!!! rozo_diag_srv_userHdrAnalyzerCallBack %d", len);    
  }  
  return len;
}
/*
**____________________________________________________
**  socket configuration
**____________________________________________________
*/
af_unix_socket_conf_t  rozo_diag_srv_conf =
{
  1,  //           family: identifier of the socket family    */
  0,         /**< instance number within the family   */
  sizeof(UMA_MSGHEADER_S),  /* headerSize  -> size of the header to read                 */
  0,                         /* msgLenOffset->  offset where the message length fits      */
  sizeof(uint32_t),          /* msgLenSize  -> size of the message length field in bytes  */
  0, //        bufSize;         /* length of buffer (xmit and received)        */
  0, //        so_sendbufsize;  /* length of buffer (xmit and received)        */
  rozo_diag_srv_userRcvAllocBufCallBack,  //    userRcvAllocBufCallBack; /* user callback for buffer allocation */
  rozo_diag_srv_userRcvCallBack,           //    userRcvCallBack;   /* callback provided by the connection owner block */
  rozo_diag_srv_userDiscCallBack,   //    userDiscCallBack; /* callBack for TCP disconnection detection         */
  rozo_diag_srv_userConnectCallBack,   //userConnectCallBack;     /**< callback for client connection only         */
  NULL,  //    userXmitDoneCallBack; /**< optional call that must be set when the application when to be warned when packet has been sent */
  rozo_diag_srv_userRcvReadyCallBack,  //    userRcvReadyCallBack; /* NULL for default callback                     */
  NULL,  //    userXmitReadyCallBack; /* NULL for default callback                    */
  NULL,  //    userXmitEventCallBack; /* NULL for default callback                    */
  rozo_diag_srv_userHdrAnalyzerCallBack,        /* userHdrAnalyzerCallBack ->NULL by default, function that analyse the received header that returns the payload  length  */
  ROZOFS_GENERIC_SRV,       /* recv_srv_type ---> service type for reception : ROZOFS_RPC_SRV or ROZOFS_GENERIC_SRV  */
  0,       /*   rpc_recv_max_sz ----> max rpc reception buffer size : required for ROZOFS_RPC_SRV only */

  NULL,  //    *userRef;             /* user reference that must be recalled in the callbacks */
  NULL,  //    *xmitPool; /* user pool reference or -1 */
  NULL,   //    *recvPool; /* user pool reference or -1 */
  .priority = 3,  /** set the af_unix socket priority */
}; 

/*
**____________________________________________________
**
** Creation of the rozodiag server buffer pool
**  
** @param count   number of buffer
** @param size    size of a buffer
**
** @retval   RUC_OK 
** @retval   RUC_NOK out of memory
**____________________________________________________
*/
int rozo_diag_srv_buffer_pool_init(int count,int size) {
   
  /*
  ** create the pool for receiving messages
  */
  rozo_diag_srv_buffer_pool_p = ruc_buf_poolCreate(count, size);
  if (rozo_diag_srv_buffer_pool_p == NULL) {
     fatal( "ruc_buf_poolCreate(%d,%d)", count, size ); 
     return -1;
  }
  ruc_buffer_debug_register_pool("xmit/rcv",  rozo_diag_srv_buffer_pool_p);
  return 0;
}
/*
**____________________________________________________
**
** Iniialization of the rozodiag server service
**
** @retval  RUC_OK : done
** @retval RUC_NOK : out of memory
**____________________________________________________
*/
int rozo_diag_srv_init(uint16_t servicePort) {
  int      ret = -1;


  /*
  ** Create the buffer pool
  */
  rozo_diag_srv_buffer_pool_init(200,UMA_DBG_MAX_SEND_SIZE);

  /*
  ** set the export dscp code
  */
  rozo_diag_srv_conf.dscp = (uint8_t) common_config.export_dscp; 
  rozo_diag_srv_conf.dscp = rozo_diag_srv_conf.dscp<<2;

  rozo_diag_srv_conf.bufSize        = UMA_DBG_MAX_SEND_SIZE;
  rozo_diag_srv_conf.so_sendbufsize = UMA_DBG_MAX_SEND_SIZE;
    
  /*
  ** Create the listening socket
  */
  ret = af_inet_sock_listening_create("SRV", INADDR_ANY, servicePort, &rozo_diag_srv_conf);    
  if (ret < 0) {
    fatal("Can't create AF_INET listening socket on port %d %s",servicePort, strerror(errno));
    return -1;
  }  
  return 0;
}
/*__________________________________________________________________________
  The famous ruc_init
  ==========================================================================*/
#define fdl_debug_loop(line) 
uint32_t ruc_init() {
  int ret = RUC_OK;
  uint32_t mx_tcp_client = 2;
  uint32_t mx_tcp_server = 8;
  uint32_t mx_tcp_server_cnx = 10;
  uint16_t dbgPort=0;


  //#warning TCP configuration ressources is hardcoded!!
  /*
   ** init of the system ticker
   */
  rozofs_init_ticker();
  /*
   ** trace buffer initialization
   */
  ruc_traceBufInit();

  /*
   ** initialize the socket controller:
   **   for: NPS, Timer, Debug, etc...
   */
  // warning set the number of contexts for socketCtrl to 1024
  ret = ruc_sockctl_init(ROZOFS_DIAG_SRV_MAX_CNX);
  if (ret != RUC_OK) {
      fdl_debug_loop(__LINE__);
      fatal( " socket controller init failed" );
  }

  /*
   **  Timer management init
   */
  ruc_timer_moduleInit(FALSE);

  while (1) {
      /*
       **--------------------------------------
       **  configure the number of TCP connection
       **  supported
       **--------------------------------------   
       **  
       */
      ret = uma_tcp_init(mx_tcp_client + mx_tcp_server + mx_tcp_server_cnx);
      if (ret != RUC_OK) {
          fdl_debug_loop(__LINE__);
          break;
      }

      /*
       **--------------------------------------
       **  configure the number of TCP server
       **  context supported
       **--------------------------------------   
       **  
       */
      ret = ruc_tcp_server_init(mx_tcp_server);
      if (ret != RUC_OK) {
          fdl_debug_loop(__LINE__);
          break;
      }


      /*
      **--------------------------------------
      **  configure the number of AF_UNIX/AF_INET
      **  context supported
      **--------------------------------------   
      **  
      */    
      ret = af_unix_module_init(ROZOFS_DIAG_SRV_MAX_CNX,
                                2,1024*1, // xmit(count,size)
                                2,1024*1 // recv(count,size)
                                );
      if (ret != RUC_OK) break;   


      /* 
      ** Try to get debug port from /etc/services 
      */    
      dbgPort = rozofs_get_service_port(ROZOFS_SERVICE_PORT_ROZODIAG_SRV);

      /*
       **--------------------------------------
       **   D E B U G   M O D U L E,
       **--------------------------------------
       */       
       
      uma_dbg_init(10, INADDR_ANY, 0, ROZOFS_UTILITY_NAME);
      uma_dbg_set_name(ROZOFS_UTILITY_NAME);

      /*
      ** SERVER port initialization
      */
      ret = rozo_diag_srv_init(dbgPort);
      if (ret != RUC_OK) break; 

      /*
      ** Initialize context table
      */
      rozofs_diag_srv_ctx_init();


      break;
  }

//  memset(rozofs_rcmd_profiler,0, sizeof(rozofs_rcmd_profiler));
//  uma_dbg_addTopic_option("profiler", show_profiler,UMA_DBG_OPTION_RESET);

  return ret;
}
/*__________________________________________________________________________
  MAIN thread listening to incoming TCP connection
  ==========================================================================*/
int main(int argc, char *argv[]) {
  
  /*
  ** Change local directory to "/"
  */
  if (chdir("/")!=0) {}

  /*
  ** read common config file
  */
  common_config_read(NULL);    

  /*
  ** Get utility name and record it for syslog
  */
  uma_dbg_record_syslog_name(ROZOFS_UTILITY_NAME);    
  uma_dbg_thread_add_self("Main");
      
  /*
  ** Set a signal handler
  */
  rozofs_signals_declare(ROZOFS_UTILITY_NAME, common_config.nb_core_file); 

  /*
  ** RUC init
  */
  if (ruc_init() != RUC_OK) {
    fatal("ruc_init");
  }

  /*
   ** main loop
   */
  while (1) {
    ruc_sockCtrl_selectWait();
  }
  fatal("Exit from ruc_sockCtrl_selectWait()");
  fdl_debug_loop(__LINE__);
}
