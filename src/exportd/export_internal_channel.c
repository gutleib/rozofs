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
/*
**   I N C L U D E  F I L E S
*/

#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <errno.h>
#include <stdarg.h>
#include <unistd.h>
#include <time.h>

#include <rozofs/common/types.h>
//#include <rozofs/core/ruc_trace_api.h>
#include <rozofs/core/ruc_common.h>
#include <rozofs/core/ruc_sockCtl_api.h>
#include "export.h"
#include "export_internal_channel.h"
#include <rozofs/rpc/gwproto.h>
#include "export_expgateway_conf.h"



gw_configuration_t  expgw_conf_local;
int expgw_configuration_available = 0;

int export_conf_lock = 0;

uint32_t  expgwc_sup_getIntSockIdxFromSocketId(expgwc_internal_channel_conf_t *p,int socketId);
/*
**   G L O B A L    D A T A
*/

/*
**  Call back function for socket controller
*/
ruc_sockCallBack_t expgwc_sup_callBack_InternalSock=
  {
     expgwc_sup_rcvReadyInternalSock,
     expgwc_internal_channel_recv_cbk,
     expgwc_sup_xmitReadyInternalSock,
     expgwc_sup_xmitEvtInternalSock
  };

expgwc_internal_channel_conf_t   expgwc_int_chan_ctx;

/**
* xmit buffer for the channel
*/
uint8_t expgwc_internal_channel_buf_send[EXPORTDNB_INTERNAL_CHAN_SEND_SZ_MAX];

/**
* receive buffer for the channel
*/
uint8_t expgwc_internal_channel_buf_recv[EXPORTDNB_INTERNAL_CHAN_RECV_SZ_MAX];


 uint32_t expgwc_internal_channel_conf_trace = TRUE;
/*
**________________________________________________________________________
*/
/**
*  Display of the state of the current configuration of the exportd

 */

static char bufall[1024];

static char *show_expgw_display_configuration_state(char *buffer,int state)
{
    char *pchar = buffer;
   switch (state)
   {
      default:
      case EPGW_CONF_UNKNOWN:
        sprintf(pchar,"UNKNOWN   ");
        break;
   
      case EPGW_CONF_NOT_SYNCED:
        sprintf(pchar,"NOT_SYNCED");
        break;   
      case EPGW_CONF_SYNCED:
        sprintf(pchar,"SYNCED    ");
        break;   
   
   }
   return buffer;
}
/*
**________________________________________________________________________
*/
/**
*
*/
void show_expgw_configuration(char * argv[], uint32_t tcpRef, void *bufRef) 
{
    char *pchar = uma_dbg_get_buffer();
   export_expgw_conf_ctx_t *p = export_expgw_conf_table ;
   gw_configuration_t *expgw_conf_p= &expgw_conf_local;
   int i;
   
   while(1)
   {   
     if (expgw_configuration_available == 0)
     {
       pchar += sprintf(pchar,"No configuration available\n");
       break;
     }
     pchar += sprintf(pchar,"exportd id %d\n",expgw_conf_p->hdr.export_id);
     pchar += sprintf(pchar,"nb gateways : %d\n",expgw_conf_p->hdr.nb_gateways);
     pchar += sprintf(pchar,"nb eids     : %d\n",expgw_conf_p->eid.eid_len); 
     pchar += sprintf(pchar,"hash config : 0x%x\n",export_configuration_file_hash); 

pchar += sprintf(pchar,"     hostname        |  lbg_id  | state  | cnf. status |  poll (attps/ok/nok) | conf send (attps/ok/nok)\n");
pchar += sprintf(pchar,"---------------------+----------+--------+-------------+----------------------+--------------------------\n");
     for (i = 0; i < expgw_conf_p->gateway_host.gateway_host_len; i++,p++) 
     {
       pchar += sprintf(pchar,"%20s |",p->hostname);
       if ( p->gateway_lbg_id == -1)
       {
         pchar += sprintf(pchar,"  ???     |");
       }
       else
       {
         pchar += sprintf(pchar,"  %3d     |",p->gateway_lbg_id);
       
       }
       pchar += sprintf(pchar,"  %s  |",north_lbg_display_lbg_state(bufall,p->gateway_lbg_id));
       pchar += sprintf(pchar," %s  |",show_expgw_display_configuration_state(bufall,p->conf_state));
       pchar += sprintf(pchar," %6.6llu/%6.6llu/%6.6llu |",
                (long long unsigned int)p->stats.poll_counter[EXPGW_STATS_ATTEMPT],
                (long long unsigned int)p->stats.poll_counter[EXPGW_STATS_SUCCESS],
               (long long unsigned int) p->stats.poll_counter[EXPGW_STATS_FAILURE]);

       pchar += sprintf(pchar," %6.6llu/%6.6llu/%6.6llu\n",
                (long long unsigned int)p->stats.conf_counter[EXPGW_STATS_ATTEMPT],
                (long long unsigned int)p->stats.conf_counter[EXPGW_STATS_SUCCESS],
               (long long unsigned int) p->stats.conf_counter[EXPGW_STATS_FAILURE]);  


     }
     break;
  } 
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}
/*
**________________________________________________________________________
*/
/*----------------------------------------------
**  expgwc_sup_getObjRef
**----------------------------------------------
**
**  that function builds the event message and
**  sends it to the internal socket.
**
**
**  IN :
**   none
**
**  OUT :ptr to the timer context
**
**-----------------------------------------------
*/

expgwc_internal_channel_conf_t *expgwc_sup_getObjRef()
{
  return (expgwc_internal_channel_conf_t *)&expgwc_int_chan_ctx;
}

/*
 *_______________________________________________________________________
 */
/**
*  send a message on the internal channel

  The message to send is copied in a pre-allocated buffer 
  The message must not exceed the size of the pre-allocated buffer
  otherwise the message is rejected
  
  If the message has been allocated by a malloc() it is up to the calling
  function to release it.

 @param opcode : opcode of the message to send
 @param length : length of the message
 @param message : pointer to the message to send
 
 @retval 0 : success
 @retval <0 : error (see errno)
*/
int expgwc_internal_channel_send(uint32_t opcode,uint32_t length, void *message )
{

  int nBytes;
  int effective_length; 
  char *payload; 
  expgwc_int_chan_msg_t *msg = (expgwc_int_chan_msg_t*) expgwc_internal_channel_buf_send;
  
  expgwc_internal_channel_conf_t *p = &expgwc_int_chan_ctx;

  if (length > expgwc_get_internal_channel_buf_send_size())
  {
    errno = EMSGSIZE;
    return -1;  
  }
  msg->opcode = opcode;
  msg->length = length;
  payload = (char*)(msg+1);
  if (length != 0)
  {
    memcpy(payload,message,length);
  }
  effective_length = sizeof(expgwc_int_chan_msg_t) + length;

  nBytes = send(p->internalSocket[RUC_SOC_SEND],
                (const char *)msg,
                effective_length,
                0);
  if (nBytes < 0)
  {
    return -1;
  
  }
  if (nBytes != effective_length)
  {
    // message not sent
    severe("sent message has been truncated %d( %d)", nBytes,effective_length);
    return -1;

  }
  return 0;
}
/*----------------------------------------------
**  expgwc_sup_getIntSockIdxFromSocketId
**----------------------------------------------
**
**   That function returns the internal
**   socket index associated to socketId.
**   If the socketId is not found it
**   return -1.
**
**
**  IN :
**     p : Relci object pointer
**     socketId : socket Identifier to search
**
**  OUT : -1:not found
**       <>-1: found (RUC_SOC_SEND or RUC_SOC_RECV)
**
**-----------------------------------------------
*/

uint32_t  expgwc_sup_getIntSockIdxFromSocketId(expgwc_internal_channel_conf_t *p,int socketId)
{
   int i;


   for (i = 0;i < 2;i++)
   {
     if (p->internalSocket[i]==socketId) return (uint32_t)i;
   }
   return -1;
}



/*----------------------------------------------
**  expgwc_sup_rcvReadyInternalSock
**----------------------------------------------
**
**   receive ready function: only for
**   receiver socket. Nothing expected
**   on sending socket
**
**
**  IN :
**     not_significant : Relci instance index
**     socketId : socket Identifier
**
**  OUT : always TRUE for RUC_SOC_RECV
**        always FALSE for RUC_SOC_SEND
**
**-----------------------------------------------
*/
uint32_t expgwc_sup_rcvReadyInternalSock(void * not_significant,int socketId)
{
  expgwc_internal_channel_conf_t *p;
  uint32_t      socketIdx;

  /*
  **  Get the pointer to the timer Object
  */
  p = &expgwc_int_chan_ctx;
  socketIdx = expgwc_sup_getIntSockIdxFromSocketId(p,socketId);
  if (socketIdx == -1)
  {
    /*
    ** something really wrong
    */
    fatal("cannot get the socket ref:expgwc_sup_rcvReadyInternalSock()");
    return FALSE;
  }
  if (socketIdx == RUC_SOC_SEND)
    return FALSE;
  else
    return TRUE;
}


/**
*  Process a configuration message that contains the export gateways conf.

  @param msg : pointer to the rpc message
  @parem len : length of the rpc message
  
  @retval 0 on success
  @retval < 0 on error 
*/
int expgwc_load_conf_proc(char *msg,int len)
{
    XDR               xdrs; 
    gw_configuration_t *expgw_conf_p= &expgw_conf_local;
    int status = -1;
    int i;
    int ret;
    
    expgw_configuration_available = 0;
    xdr_free((xdrproc_t) xdr_gw_configuration_t, (char *) expgw_conf_p);
    
    xdrmem_create(&xdrs,(char*)msg,len,XDR_DECODE);
    if (xdr_gw_configuration_t(&xdrs,expgw_conf_p) == FALSE)
    {
      severe("encoding error");   
      errno = EINVAL; 
      goto out;
    } 
    expgw_configuration_available = 1;
    /*
    ** successful decoding
    */
//    info("NB exportd id  %d",expgw_conf_p->hdr.export_id);          
//    info("NB nb_gateways %d",expgw_conf_p->hdr.nb_gateways);          
//    info("NB nb_eid      %d",expgw_conf_p->eid.eid_len); 
      
    export_nb_gateways = expgw_conf_p->gateway_host.gateway_host_len;   
    for (i = 0; i < expgw_conf_p->gateway_host.gateway_host_len; i++) 
    {
//       info("NB host %s",(char*)expgw_conf_p->gateway_host.gateway_host_val[i].host); 
       //XXX: configuration port base (60000)  of the export gateway is almost hardcoded!!
       int port = EXPGW_PORT_EXPORTD_IDX + 60000;
       ret = export_expgw_conf_ctx_create(i,(char*)expgw_conf_p->gateway_host.gateway_host_val[i].host,port);
       if (ret < 0)
       {
          severe("export_expgw_conf_ctx_create() failed for export gateway %d",i);
       }
                                       
    }    
    status = 0;

out:
    return status;

}

/*----------------------------------------------
**  expgwc_internal_channel_recv_cbk
**----------------------------------------------
**
**   receive  function: only for
**   receiver socket. Nothing expected
**   on sending socket. It indicates
**   that there is an internal message
**   pending for the Relci instance
**
**
**  IN :
**     not_significant : Relci instance index
**     socketId : socket Identifier
**
**  OUT : always TRUE for RUC_SOC_RECV
**        always FALSE for RUC_SOC_SEND
**
**-----------------------------------------------
*/
uint32_t expgwc_internal_channel_recv_cbk(void * not_significant,int socketId)
{
  expgwc_internal_channel_conf_t *p;
  uint32_t      socketIdx;
  int         bytesRcvd;
  char       *pchar;
  int         ret;
  expgwc_int_chan_msg_t *msg = (expgwc_int_chan_msg_t*)expgwc_internal_channel_buf_recv;
  //int ret;


  p = &expgwc_int_chan_ctx;
  socketIdx = expgwc_sup_getIntSockIdxFromSocketId(p,socketId);
  if (socketIdx == -1)
  {
    /*
    ** something really wrong
    */
    fatal("cannot get the socket ref:expgwc_internal_channel_recv_cbk()");
    return FALSE;
  }

  if (socketIdx == RUC_SOC_SEND)
  {
    /*
    **  should not occur
    */
    severe("bad socket type: RUC_SOC_RECV expected");

//    RUC_WARNING(socketId);
    return FALSE;
  }
  /*
  **  receive the message
  **
  */
  bytesRcvd = recv(socketId,
                   (char *)msg,
                   EXPORTDNB_INTERNAL_CHAN_RECV_SZ_MAX,
                   0);
  if (bytesRcvd < 0)
  {
     fatal("bad message received:%s",strerror(errno));
  }
  if (bytesRcvd < sizeof(expgwc_int_chan_msg_t))
  {
     severe("received message too short:%d",bytesRcvd);
    return TRUE;  
  }

  /*
  **  process the message
  */
  pchar =(char*)( msg+1);
  switch ( msg->opcode)
  {
    case EXPGWC_NULL:
//      info(" EXPGWC_NULL received !!");
      break;
    case EXPGWC_LOAD_CONF:
      expgwc_load_conf_proc(pchar,msg->length);
//      info("EXPGWC_LOAD_CONF received -> length %d",msg->length);
      break;

    case EXPORT_LOAD_CONF:
      ret = export_reload_nb();
      export_reload_conf_status.done   = 1;
      export_reload_conf_status.status = ret;
      break;
    default:
       severe("bad opcode:%d",msg->opcode);
      break;
  }

  return TRUE;
}


/*----------------------------------------------
**  expgwc_sup_xmitReadyInternalSock
**----------------------------------------------
**
**   xmit ready function: only for
**   xmit socket. Nothing expected
**   on receiving socket.
**
**
**  IN :
**     not_significant : Relci instance index
**     socketId : socket Identifier
**
**  OUT : always FALSE for RUC_SOC_RECV
**        always FALSE for RUC_SOC_SEND
**  There is not congestion on the internal socket
**
**-----------------------------------------------
*/
uint32_t expgwc_sup_xmitReadyInternalSock(void * not_significant,int socketId)
{
  expgwc_internal_channel_conf_t *p;
  uint32_t      socketIdx;

  /*
  **  Get the pointer to the timer Object
  */
  p = &expgwc_int_chan_ctx;
  socketIdx = expgwc_sup_getIntSockIdxFromSocketId(p,socketId);
  if (socketIdx == -1)
  {
    /*
    ** something really wrong
    */
    fatal("expgwc_sup_xmitReadyInternalSock");;
    return FALSE;
  }

  if (socketIdx == RUC_SOC_RECV)
    return FALSE;
  else
    return FALSE;
}


/*----------------------------------------------
**  expgwc_sup_xmitEvtInternalSock
**----------------------------------------------
**
**   xmit event  function: only for
**   xmit socket.
**   That function should never be encountered
**
**
**  IN :
**     intSockRef : either RUC_SOC_SEND or
**                         RUC_SOC_RECV
**     socketId : socket Identifier
**
**  OUT : always FALSE for RUC_SOC_RECV
**        always TRUE for RUC_SOC_SEND
**
**-----------------------------------------------
*/
uint32_t expgwc_sup_xmitEvtInternalSock(void * not_significant,int socketId)
{
  return FALSE;
}
/*
**    I N T E R N A L   S O C K E T
**    CREATION/DELETION
*/


/*----------------------------------------------
**  expgwc_sup_createInternalSocket (private)
**----------------------------------------------
**
**  That function is intented to create a
**  socket pair. That socket pair is used
**  for sending internal event back to the
**  timer instance
**
**  IN :
**     p : pointer to the timer instance
**
**  OUT :
**    RUC_OK : all is fine
**    RUC_NOK : unable to create the internal
**              socket.
**
**  note : the socket is configured as an asynchronous
**         socket.(sending socket only)
**-----------------------------------------------
*/

uint32_t expgwc_sup_createInternalSocket(expgwc_internal_channel_conf_t *p)
{
  int    ret;
  uint32_t retcode = RUC_NOK;
//  int    fileflags;


  /*
  **  1 - create the socket pair
  */

  ret = socketpair(  AF_UNIX,
                  SOCK_DGRAM,
                  0,
                  &p->internalSocket[0]);

  if (ret < 0)
  {
    /*
    ** unable to create the sockets
    */
    severe("expgwc_sup_createInternalSocket: %s",strerror(errno));;

    return RUC_NOK;
  }
  while (1)
  {
#if 0
    /*
    ** change socket mode to asynchronous
    */
    if((fileflags=fcntl(p->internalSocket[RUC_SOC_SEND],F_GETFL,0))==-1)
    {
      severe("expgwc_sup_createInternalSocket: %s",strerror(errno));;
      break;
    }
    if(fcntl(p->internalSocket[RUC_SOC_SEND],F_SETFL,fileflags|O_NDELAY)==-1)
    {
      severe("expgwc_sup_createInternalSocket: %s",strerror(errno));;
      break;
    }
#endif
    /*
    ** 2 - perform the connection with the socket controller
    */
    p->intSockconnectionId[RUC_SOC_SEND]=
                                     ruc_sockctl_connect(p->internalSocket[RUC_SOC_SEND],
                                     "CNF_SOCK_XMIT",
                                      0,
                                      p,
                                      &expgwc_sup_callBack_InternalSock);
    if (p->intSockconnectionId[RUC_SOC_SEND]== NULL)
    {
      severe("expgwc_sup_createInternalSocket: RUC_SOC_SEND");
      break;
    }
    p->intSockconnectionId[RUC_SOC_RECV]=
                 ruc_sockctl_connect(p->internalSocket[RUC_SOC_RECV],
                                     "CNF_SOCK_RECV",
                                      0,
                                      p,
                                      &expgwc_sup_callBack_InternalSock);
    if (p->intSockconnectionId[RUC_SOC_RECV]== NULL)
    {
      severe("expgwc_sup_createInternalSocket: RUC_SOC_RECV");
      break;
    }
    /*
    **  done
    */
    retcode = RUC_OK;
    break;

  }
  if (retcode != RUC_OK)
  {
    /*
    ** something wrong: close the sockets and disconnect
    **  from socket controller
    */
    close (p->internalSocket[RUC_SOC_SEND]);
    close (p->internalSocket[RUC_SOC_RECV]);
    if (p->intSockconnectionId[RUC_SOC_RECV] != NULL)
    {
      ruc_sockctl_disconnect(p->intSockconnectionId[RUC_SOC_RECV]);
      p->intSockconnectionId[RUC_SOC_RECV] = NULL;
    }
    if (p->intSockconnectionId[RUC_SOC_SEND] != NULL)
    {
      ruc_sockctl_disconnect(p->intSockconnectionId[RUC_SOC_SEND]);
      p->intSockconnectionId[RUC_SOC_SEND] = NULL;
    }
    return RUC_NOK;
  }
  return RUC_OK;
}


/*----------------------------------------------
**  expgwc_sup_deleteInternalSocket (private)
**----------------------------------------------
**
** That function is called when a Recli
** instance is deleted:
**
**   That function performs:
**    -  the closing of the socket pair
**    -  the socket controller disconnection
**    -  the purge of the signal queue list
**
**
**  IN :
**     p : pointer to the timer instance
**
**  OUT :
**    RUC_OK : all is fine
**    RUC_NOK :
**-----------------------------------------------
*/

uint32_t expgwc_sup_deleteInternalSocket(expgwc_internal_channel_conf_t *p)
{

  if (p->internalSocket[RUC_SOC_SEND] != -1)
  {
    close (p->internalSocket[RUC_SOC_SEND]);
    p->internalSocket[RUC_SOC_SEND] = -1;
  }
  if (p->internalSocket[RUC_SOC_RECV] != -1)
  {
    close (p->internalSocket[RUC_SOC_RECV]);
    p->internalSocket[RUC_SOC_SEND] = -1;
  }
  if (p->intSockconnectionId[RUC_SOC_RECV] != NULL)
  {
    ruc_sockctl_disconnect(p->intSockconnectionId[RUC_SOC_RECV]);
    p->intSockconnectionId[RUC_SOC_RECV] = NULL;
  }
  if (p->intSockconnectionId[RUC_SOC_SEND] != NULL)
  {
    ruc_sockctl_disconnect(p->intSockconnectionId[RUC_SOC_SEND]);
    p->intSockconnectionId[RUC_SOC_SEND] = NULL;
  }

  return RUC_OK;
}

/*
**____________________________________________________
*/
/*
  Periodic timer expiration
  
    - Poll all the gateways for testing if their config is consistent
    - Perform cache conistency
*/
static void expgwc_int_periodic_ticker(void * param) 
{

  int i;
  export_expgw_conf_ctx_t *p;
  
  if (export_conf_lock) return;
  /*
  ** check if the configuration is available: nothing to do if there is no configuration
  */
  if (expgw_configuration_available == 0) return;
  
  p = export_expgw_conf_table;
  for ( i = 0; i < export_nb_gateways ;i++,p++)
  {
    export_expgw_check_config(p);  
  } 


}
/*
**____________________________________________________
*/
/*
  start a periodic timer to chech wether the export LBG is down
  When the export is restarted its port may change, and so
  the previous configuration of the LBG is not valid any more
*/
static void expgwc_int_start_timer() {
  struct timer_cell * expgwc_int_periodic_timer;

  expgwc_int_periodic_timer = ruc_timer_alloc(0,0);
  if (expgwc_int_periodic_timer == NULL) {
    fatal("expgwc_int_start_timer");
    return;
  }
  ruc_periodic_timer_start (expgwc_int_periodic_timer, 
                            5000,
 	                        expgwc_int_periodic_ticker,
 			                NULL);

}




/*----------------------------------------------
**  expgwc_int_chan_moduleInit (public)
**----------------------------------------------
**
**
**  IN :
*
**
**  OUT :
**    RUC_OK/RUC_NOK
**
**-----------------------------------------------
*/
uint32_t expgwc_int_chan_moduleInit()

{
  expgwc_internal_channel_conf_t *p;
  uint32_t      ret;

  gw_configuration_t *expgw_conf_p= &expgw_conf_local;
  
  uma_dbg_addTopic("config_status", show_expgw_configuration);

  
  /*
  ** clear the configuration area
  */
  memset(expgw_conf_p,0,sizeof(gw_configuration_t));
  /*
  **  Get the pointer to the timer Object
  */
  p = expgwc_sup_getObjRef();
  /*
  ** starts the periodic timer
  */
   expgwc_int_start_timer();

   /*
   ** create the internal socket
   */

   ret = expgwc_sup_createInternalSocket(p);
   return ret;
}
