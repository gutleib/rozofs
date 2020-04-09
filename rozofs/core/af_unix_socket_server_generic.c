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
 #include <poll.h>

#include <rozofs/common/types.h>

#include "ruc_common.h"
#include "ruc_list.h"
#include "af_unix_socket_generic.h"
#include "af_unix_socket_generic_api.h"
#include "af_inet_stream_api.h"
#include "ruc_sockCtl_api_th.h"

/**
* Callbacks prototypes
*/
uint32_t af_unix_generic_srv_rcvReadysock(void * socket_ctx_p,int socketId);
uint32_t af_unix_generic_srv_acceptIn_Cbk(void * socket_ctx_p,int socketId);
uint32_t af_unix_generic_srv_xmitReadysock(void * socket_ctx_p,int socketId);
uint32_t af_unix_generic_srv_xmitEvtsock(void * socket_ctx_p,int socketId);

/**
* Callbacks prototypes :  connected
*/

uint32_t af_unix_generic_srv_connected_rcvReady_cbk(void * socket_ctx_p,int socketId);
uint32_t af_unix_generic_srv_connected_recv_cbk(void * socket_ctx_p,int socketId);
uint32_t af_unix_generic_srv_connected_xmitReady_cbk(void * socket_ctx_p,int socketId);
uint32_t af_unix_generic_srv_connected_xmitEvt_cbk(void * socket_ctx_p,int socketId);


int af_unix_sock_accept_create(int socketRef,int af_family, char *nickname,af_unix_socket_conf_t *conf_p);

extern ruc_sockCallBack_t af_unix_generic_server_connected_callBack_sock;;


 /*
**__________________________________________________________________________
**
**    N O T   Y E T   A C C E P T E D    C A L L B A C K S
**
**__________________________________________________________________________
*/

/*
**  Call back function for socket controller
*/
ruc_sockCallBack_t af_unix_generic_listening_callBack_sock=
  {
     af_unix_generic_srv_rcvReadysock,
     af_unix_generic_srv_acceptIn_Cbk,
     af_unix_generic_srv_xmitReadysock,
     af_unix_generic_srv_xmitEvtsock
  };


/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   receiver ready function: called from socket controller.
   Always true for a listening socket


  @param socket_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)

  @retval : TRUE-> receiver ready
  @retval : FALSE-> receiver not ready
*/

uint32_t af_unix_generic_srv_rcvReadysock(void * socket_ctx_p,int socketId)
{

    return TRUE;
}

/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   Called from the socket controller when there is a message pending on the
   socket associated with the context provide in input arguments.



  @param socket_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)

   @retval : TRUE-> xmit ready event expected
  @retval : FALSE-> xmit  ready event not expected
*/

uint32_t af_unix_generic_srv_acceptIn_Cbk(void * socket_ctx_p,int socketId)
{

   af_unix_ctx_generic_t  *sock_p;
   sock_p = (af_unix_ctx_generic_t*)socket_ctx_p;
   struct sockaddr   sockAddr;
   int sockAddrLen= sizeof(sockAddr);
   int fdWork;
   int sockIdx;


   if((fdWork=accept(socketId,(struct sockaddr *)&sockAddr,(socklen_t *)&sockAddrLen)) == -1)
   {
     /*
     **  accept is rejected !
     */
     RUC_WARNING(errno);

     return TRUE;
   }
   /*
   ** Create the socket and bind it with the socket controller
   */

   sockIdx = af_unix_sock_accept_create(fdWork,sock_p->af_family,sock_p->nickname,sock_p->conf_p);
   if (sock_p->conf_p->userConnectCallBack) {     
     (*sock_p->conf_p->userConnectCallBack)(sock_p->userRef,sockIdx,RUC_OK,0);
   }
   return TRUE;
}


/*
**__________________________________________________________________________
*/
/**
  Application callBack:

  Called from the socket controller. The application is intended to
  indicate if it wants to be warn when the transmitter is ready
  Typically after facing a congestion on xmit, the application will
  generally request from xmit ready event from the socket layer

   --> always FALSE for listening socket

  @param socket_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)

  @retval : always TRUE
*/

uint32_t af_unix_generic_srv_xmitReadysock(void * socket_ctx_p,int socketId)
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
   has replied TRUE in af_unix_generic_srv_xmitReadysock().

   -> does not make sense for listening socket

  @param socket_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)

   @retval :always TRUE
*/

uint32_t af_unix_generic_srv_xmitEvtsock(void * socket_ctx_p,int socketId)
{

    return TRUE;
}

 /*
**__________________________________________________________________________
**
**     A C C E P T E D    C A L L B A C K S
**
**__________________________________________________________________________
*/

/*
**  Call back function for socket controller
*/
ruc_sockCallBack_t af_unix_generic_server_accepted_callBack_sock=
  {
     af_unix_generic_srv_connected_rcvReady_cbk,
     af_unix_generic_srv_connected_recv_cbk,
     af_unix_generic_srv_connected_xmitReady_cbk,
     af_unix_generic_srv_connected_xmitEvt_cbk
  };


/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   receiver ready function: called from socket controller.
   The module is intended to return if the receiver is ready to receive a new message
   and FALSE otherwise


  @param socket_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)

  @retval : TRUE-> receiver ready
  @retval : FALSE-> receiver not ready
*/

uint32_t af_unix_generic_srv_connected_rcvReady_cbk(void * socket_ctx_p,int socketId)
{
    af_unix_ctx_generic_t  *sock_p;
    sock_p = (af_unix_ctx_generic_t*)socket_ctx_p;
    /*
    ** check if there is a user callback associated with that socket
    ** otherwise use the default function of the generic socket
    */
    if (sock_p->userRcvReadyCallBack != NULL)
    {

      return (sock_p->userRcvReadyCallBack)(sock_p->userRef,sock_p->index);
    }
    else
    {
      /*
      ** check the amount of buffer on the receiver side
      */

    }
    return TRUE;
}

/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   Called from the socket controller when there is a message pending on the
   socket associated with the context provide in input arguments.



  @param socket_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)

   @retval : TRUE-> xmit ready event expected
  @retval : FALSE-> xmit  ready event not expected
*/

uint32_t af_unix_generic_srv_connected_recv_cbk(void * socket_ctx_p,int socketId)
{

  af_unix_ctx_generic_t  *sock_p = (af_unix_ctx_generic_t*)socket_ctx_p;
  com_recv_template_t    *recv_p;
  recv_p = &sock_p->recv;
  if (recv_p->rpc.receiver_active)
  {
    af_unix_recv_rpc_stream_generic_cbk(socket_ctx_p,socketId);
  }
  else
  {
    af_unix_recv_stream_generic_cbk(socket_ctx_p,socketId);
  }
  return TRUE;
}


/*
**__________________________________________________________________________
*/
/**
  Application callBack:

  Called from the socket controller. The application is intended to
  indicate if it wants to be warn when the transmitter is ready
  Typically after facing a congestion on xmit, the application will
  generally request from xmit ready event from the socket layer


  @param socket_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)

  @retval : always TRUE
*/

uint32_t af_unix_generic_srv_connected_xmitReady_cbk(void * socket_ctx_p,int socketId)
{
    af_unix_ctx_generic_t  *sock_p;
    com_xmit_template_t *xmit_p;
    uint32_t ret = FALSE;

    sock_p = (af_unix_ctx_generic_t*)socket_ctx_p;
    xmit_p = (com_xmit_template_t*)&sock_p->xmit;

    if (xmit_p->xmit_req_flag == 1)
    {
      ret = TRUE;
    }
    if (xmit_p->congested_flag == 1)
    {
      ret = TRUE;
    }
    return ret;
}

/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   Called from the socket controller upon receiving a xmit ready event
   for the associated socket. That callback is activeted only if the application
   has replied TRUE in af_unix_generic_xmitReadysock().

   It typically the processing of a end of congestion on the socket


  @param socket_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)

   @retval :always TRUE
*/

uint32_t af_unix_generic_srv_connected_xmitEvt_cbk(void * socket_ctx_p,int socketId)
{
    af_unix_ctx_generic_t  *sock_p;
    com_xmit_template_t *xmit_p;



    sock_p = (af_unix_ctx_generic_t*)socket_ctx_p;
    xmit_p = (com_xmit_template_t*)&sock_p->xmit;
    /*
    ** check if credit reload is required
    */
    if (xmit_p->xmit_req_flag == 1)
    {
      xmit_p->xmit_req_flag = 0;
      xmit_p->xmit_credit = 0;
      af_unix_send_stream_fsm(sock_p,xmit_p);
    }
    /*
    ** active the fsm for end of congestion (xmit ready and credit reload
    */
    if (xmit_p->congested_flag == 1)
    {
      af_unix_send_stream_fsm(sock_p,xmit_p);
    }



    return TRUE;
}




/*
**__________________________________________________________________________
*/
/**
   Creation of a AF_UNIX socket stream in non blocking mode

   see /proc/sys/net/core for socket parameters:
     - wmem_default: default xmit buffer size
     - wmem_max : max allocatable
     Changing the max is still possible with root privilege:
     either edit /etc/sysctl.conf (permanent) :
     (write):
     net.core.wmem_default = xxxx
     net.core.wmem_max = xxxx
     (read):
     net.core.rmem_default = xxxx
     net.core.rmem_max = xxxx

     or temporary with:
     echo <new_value> > /proc/sys/net/core/<your_param>

   @param name0fSocket : name of the AF_UNIX socket
   @param size: size in byte of the xmit buffer (the service double that value

    retval: >0 reference of the created AF_UNIX socket
    retval < 0 : error on socket creation

*/
int af_unix_sock_stream_create_internal(char *nameOfSocket,int size)
{
  int ret;
  int fd=-1;
  struct sockaddr_un addr;
  int fdsize;
  int optionsize=sizeof(fdsize);
  int fileflags;

  /*
  ** create a datagram socket
  */
  fd=socket(PF_UNIX,SOCK_STREAM,0);
  if(fd<0)
  {
    RUC_WARNING(errno);
   return -1;
  }
  /*
  ** remove fd if it already exists
  */
  ret=unlink(nameOfSocket);
  /*
  ** named the socket reception side
  */
  addr.sun_family= AF_UNIX;
  strcpy(addr.sun_path,nameOfSocket);
  ret=bind(fd,(struct sockaddr*)&addr,sizeof(addr));
  if(ret<0)
  {
    RUC_WARNING(errno);
   return -1;
  }
  /*
  ** change the length for the send buffer, nothing to do for receive buf
  ** since it is out of the scope of the AF_SOCKET
  */
  ret= getsockopt(fd,SOL_SOCKET,SO_SNDBUF,(char*)&fdsize,( socklen_t *)&optionsize);
  if(ret<0)
  {
    RUC_WARNING(errno);
   return -1;
  }
  /*
  ** update the size, always the double of the input
  */
  fdsize=2*size;

  /*
  ** set a new size for emission and
  ** reception socket's buffer
  */
  ret=setsockopt(fd,SOL_SOCKET,SO_SNDBUF,(char*)&fdsize,sizeof(int));
  if(ret<0)
  {
    RUC_WARNING(errno);
   return -1;
  }
  /*
  ** Change the mode of the socket to non blocking
  */
  if((fileflags=fcntl(fd,F_GETFL,0))==-1)
  {
    RUC_WARNING(errno);
    return -1;
  }
//  #warning socket is operating blocking mode
#if 1

  if((fcntl(fd,F_SETFL,fileflags|O_NDELAY))==-1)
  {
    RUC_WARNING(errno);
    return -1;
  }
#endif
  return(fd);
}



/*
**__________________________________________________________________________
*/
/**
   Creation of a AF_UNIX socket Datagram  in non blocking mode

   see /proc/sys/net/core for socket parameters:
     - wmem_default: default xmit buffer size
     - wmem_max : max allocatable
     Changing the max is still possible with root privilege:
     either edit /etc/sysctl.conf (permanent) :
     (write):
     net.core.wmem_default = xxxx
     net.core.wmem_max = xxxx
     (read):
     net.core.rmem_default = xxxx
     net.core.rmem_max = xxxx

     or temporary with:
     echo <new_value> > /proc/sys/net/core/<your_param>

   @param socketRef : fd descriptor of the new socket
   @param nickname : nicknem of the listening socket
   @param af_family : extracted from the listening scoket (AF_INET or AF_UNIX)
   @param remote : remote information
   @param size: size in byte of the xmit buffer (the service double that value

    retval: !=0 reference of the created socket context
    retval < 0 : error on socket context creation

*/
int af_unix_sock_accept_create(int socketRef,int af_family, char *nickname,af_unix_socket_conf_t *conf_p)
{
   af_unix_ctx_generic_t *sock_p;
  com_xmit_template_t    *xmit_p;
  com_recv_template_t    *recv_p;
  uint32_t              ipAddr;
  uint16_t              port;
  uint32_t              priority = 16; /**< default value */
  int ret = -1;


   /*
   ** Allocate a socket context
   */
   sock_p = af_unix_alloc();
   if (sock_p == NULL)
   {
     /*
     ** Out of socket context
     */
     RUC_WARNING(-1);
      close(socketRef);
     return -1;
   }
   xmit_p = &sock_p->xmit;
   recv_p = &sock_p->recv;
   sock_p->af_family = af_family;

   while(1)
   {
   if (sock_p->af_family == AF_UNIX)
   {
     /*
     ** Copy the name of the socket
     */
     sock_p->nickname[0]='A';
     sock_p->nickname[1]=':';
     strcpy(&sock_p->nickname[2],nickname);
   }
   else
   {
     struct  sockaddr_in vSckAddr;
     int               vSckAddrLen=sizeof(struct sockaddr);
     char             *name_p = sock_p->nickname;


     if((getpeername(socketRef, (struct sockaddr *)&vSckAddr,(socklen_t*) &vSckAddrLen)) == -1)
     {
       name_p += sprintf(name_p,"A:%s/????", nickname);
     }
     else
     {
       ipAddr = (uint32_t) ntohl((uint32_t)(/*(struct sockaddr *)*/vSckAddr.sin_addr.s_addr));
       port   = ntohs((uint16_t)(vSckAddr.sin_port));
       sock_p->remote_ipaddr_host = ipAddr;
       sock_p->remote_port_host    = port;
       name_p += sprintf(name_p,"A:%s/", nickname);
       name_p += sprintf(name_p,"%u.%u.%u.%u:%u", (ipAddr>>24)&0xFF, (ipAddr>>16)&0xFF,(ipAddr>>8)&0xFF,(ipAddr)&0xFF,port);
     }


   }
   /*
   ** configure the socket to operate in non blocking mode
   */
   sock_p->socketRef = socketRef;
   if (sock_p->af_family == AF_UNIX)
   {
     af_unix_sock_set_non_blocking(sock_p->socketRef,conf_p->so_sendbufsize);
   }
   else
   {
     af_inet_tcp_tuneTcpSocket(sock_p->socketRef,conf_p->so_sendbufsize);
     af_inet_sock_set_dscp(sock_p->socketRef,conf_p->dscp);

   }

   /*
   ** copy the family id and instance id
   */
   sock_p->family      = conf_p->family;
   sock_p->instance_id = conf_p->instance_id;

   /*
   ** install the receiver part
   */
   recv_p->headerSize = conf_p->headerSize;
   recv_p->msgLenOffset = conf_p->msgLenOffset;
   recv_p->msgLenSize = conf_p->msgLenSize;
   recv_p->bufSize = conf_p->bufSize;
   /*
   ** set- the service type for the stream receiver
   */
   if (conf_p->recv_srv_type == ROZOFS_RPC_SRV) 
   {
     recv_p->rpc.receiver_active = 1;
     recv_p->rpc.max_receive_sz  = conf_p->rpc_recv_max_sz;
   }
   else
   {
     recv_p->rpc.receiver_active = 0;   
   }   
   /*
   ** install the mandatory user callbacks
   */
   sock_p->userRcvCallBack = conf_p->userRcvCallBack;
   /*
   ** Install the optional user callbacks
   */
   if (conf_p->userRcvAllocBufCallBack) sock_p->userRcvAllocBufCallBack = conf_p->userRcvAllocBufCallBack;
   if (conf_p->userDiscCallBack) sock_p->userDiscCallBack = conf_p->userDiscCallBack;
   if (conf_p->userRcvReadyCallBack) sock_p->userRcvReadyCallBack = conf_p->userRcvReadyCallBack;
   if (conf_p->userXmitReadyCallBack) sock_p->userXmitReadyCallBack = conf_p->userXmitReadyCallBack;
   if (conf_p->userXmitEventCallBack) sock_p->userXmitEventCallBack = conf_p->userXmitEventCallBack;
   if (conf_p->userHdrAnalyzerCallBack) sock_p->userHdrAnalyzerCallBack = conf_p->userHdrAnalyzerCallBack;
   /*
   ** install the user reference
   */
   sock_p->userRef = conf_p->userRef;
  /*
   ** install the socket controller context (might be NULL for default socket controller 
   */
   sock_p->socketCtrl_p = conf_p->socketCtrl_p;
   /*
   ** Check if the user has provided its own buffer pools (notice that the user might not provide
   ** any pool and use the common AF_UNIX socket pool or can use its own one by providing
   ** a userRcvAllocBufCallBack callback.
   */
   if (conf_p->xmitPool != 0) xmit_p->xmitPoolRef = conf_p->xmitPool;
   if (conf_p->xmitPool != 0) recv_p->rcvPoolRef  = conf_p->recvPool;
   if (conf_p->priority != 0) priority = conf_p->priority;

   /*
   ** OK, we are almost done, just need to connect with the socket controller
   */
   sock_p->connectionId = RUC_SOCKCTL_CONNECT(sock_p->socketCtrl_p,
                                              sock_p->socketRef,  // Reference of the socket
                                              sock_p->nickname,   // name of the socket
                                              priority,                  // Priority within the socket controller
                                              (void*)sock_p,      // user param for socketcontroller callback
                                              &af_unix_generic_server_accepted_callBack_sock);  // Default callbacks
    if (sock_p->connectionId == NULL)
    {
       /*
       ** Fail to connect with the socket controller
       */
       RUC_WARNING(-1);
       break;
    }
    /*
    ** All is fine
    */
    xmit_p->state = XMIT_READY;
    ret = sock_p->index;
    return ret;
    break;

   }
   /*
   ** Check the operation status: in case of error, release the socket and the context
   */
   if (sock_p->socketRef != -1) close(sock_p->socketRef);
   /*
   ** release the context
   */
   af_unix_free_from_ptr(sock_p);
   return -1;
}



/*
**__________________________________________________________________________
*/
/**
   Creation of a AF_UNIX stream socket in non blocking mode

   see /proc/sys/net/core for socket parameters:
     - wmem_default: default xmit buffer size
     - wmem_max : max allocatable
     Changing the max is still possible with root privilege:
     either edit /etc/sysctl.conf (permanent) :
     (write):
     net.core.wmem_default = xxxx
     net.core.wmem_max = xxxx
     (read):
     net.core.rmem_default = xxxx
     net.core.rmem_max = xxxx

     or temporary with:
     echo <new_value> > /proc/sys/net/core/<your_param>

   @param nickname : name of socket for socket controller display name
   @param remote_sun_path : name of remote  the AF_UNIX socket
   @param size: size in byte of the xmit buffer (the service double that value

    retval: !=0 reference of the created socket context
    retval < 0 : error on socket context creation

*/
int af_unix_sock_listening_create_th(char *nickname,char *remote_sun_path,af_unix_socket_conf_t *conf_p)
{
   af_unix_ctx_generic_t *sock_p;
  com_xmit_template_t    *xmit_p;
  com_recv_template_t    *recv_p;
  int ret = -1;
  int len;
  char buf_nickname[ROZOFS_SOCK_EXTNAME_SIZE];
  char *buf_nickname_p;
  buf_nickname_p = buf_nickname;


   len = strlen(remote_sun_path);
   if (len >= AF_UNIX_SOCKET_NAME_SIZE)
   {
      /*
      ** name is too big!!
      */
      RUC_WARNING(len);
      return -1;
   }


   len = strlen(remote_sun_path);
   if (len >= AF_UNIX_SOCKET_NAME_SIZE)
   {
      /*
      ** name is too big!!
      */
      RUC_WARNING(len);
      return -1;
   }
   /*
   ** Allocate a socket context
   */
   sock_p = af_unix_alloc();
   if (sock_p == NULL)
   {
     /*
     ** Out of socket context
     */
     RUC_WARNING(-1);
     return -1;
   }
   xmit_p = &sock_p->xmit;
   recv_p = &sock_p->recv;
   sock_p->af_family = AF_UNIX;

   while(1)
   {
   /*
   ** Copy the name of the socket
   */
   buf_nickname_p +=sprintf(buf_nickname_p,"L[U]:%s",nickname);
   strcpy(sock_p->nickname,nickname);
   /*
   ** Copy the sun path
   */
   strcpy(sock_p->remote_sun_path,remote_sun_path);
   /*
   ** create the socket
   */
   sock_p->socketRef = af_unix_sock_stream_create_internal(sock_p->remote_sun_path,conf_p->so_sendbufsize);
   if (sock_p->socketRef == -1)
   {
     ret = -1;
     break;
   }
   /*
   ** store the use configuration
   */
   sock_p->conf_p = conf_p;

   /*
   ** copy the family id and instance id
   */
   sock_p->family      = conf_p->family;
   sock_p->instance_id = conf_p->instance_id;

   /*
   ** install the receiver part
   */
   recv_p->headerSize = conf_p->headerSize;
   recv_p->msgLenOffset = conf_p->msgLenOffset;
   recv_p->msgLenSize = conf_p->msgLenSize;
   recv_p->bufSize = conf_p->bufSize;

   /*
   ** install the user reference
   */
   sock_p->userRef = conf_p->userRef;
   /*
   ** Call the listen
   */
   if((listen(sock_p->socketRef,5))==-1)
   {
     severe( " ruc_tcp_server_connect: listen fails for %s ,",sock_p->nickname );
     break;
   }

   /*
   ** OK, we are almost done, just need to connect with the socket controller
   */
   sock_p->connectionId = RUC_SOCKCTL_CONNECT(sock_p->socketCtrl_p,
                                              sock_p->socketRef,  // Reference of the socket
                                              buf_nickname,       // name of the socket
                                              3,                  // Priority within the socket controller
                                              (void*)sock_p,      // user param for socketcontroller callback
                                              &af_unix_generic_listening_callBack_sock);  // Default callbacks
    if (sock_p->connectionId == NULL)
    {
       /*
       ** Fail to connect with the socket controller
       */
       RUC_WARNING(-1);
       break;
    }
    /*
    ** All is fine
    */
    xmit_p->state = XMIT_READY;
    ret = sock_p->index;
    return ret;
    break;

   }
   /*
   ** Check the operation status: in case of error, release the socket and the context
   */
   if (sock_p->socketRef != -1) close(sock_p->socketRef);
//   unlink(sock_p->sockname);
   /*
   ** release the context
   */
   af_unix_free_from_ptr(sock_p);
   return -1;
}

/*
**__________________________________________________________________________
*/

/**
  API to use to connect with the default socket controller, the one associated with the main thread

*/

int af_unix_sock_listening_create(char *nickname,char *remote_sun_path,af_unix_socket_conf_t *conf_p)
{

   conf_p->socketCtrl_p = NULL;
   return af_unix_sock_listening_create_th(nickname,remote_sun_path,conf_p);
}

/**
*  Create a bunch of AF_UNIX socket associated with a Family

 @param  basename_p : Base name of the family
 @param  base_instance: index of the first instance
 @param  nb_instances: number of instance in the family
 @param  socket_tb_p : pointer to an array were the socket references will be stored
 @param  xmit_size : size for the sending buffer (SO_SNDBUF parameter)

 @retval: 0 success, all the socket have been created
 @retval < 0 error on at least one socket creation
*/
int af_unix_socket_listening_family_create (char *nicknamebase_p,char *basename_p, int base_instance,int nb_instances,
                                             int *socket_ctx_tb_p,af_unix_socket_conf_t *conf_p)
{
  int *local_socket_tb_p = socket_ctx_tb_p;
  char nickname[128];
  char sun_path[128];
  int i;
  int error = 0;


  /*
  ** Clear the socket table
  */
  memset(local_socket_tb_p,-1,sizeof(int)*nb_instances);
  /*
  ** Loop creation
  */
  for (i = 0; i < nb_instances; i++)
  {
     sprintf(sun_path,"%s_inst_%d",basename_p,base_instance+i);
     sprintf(nickname,"%s_%d",nicknamebase_p,base_instance+i);
     conf_p->instance_id = base_instance+i;
     local_socket_tb_p[i] = af_unix_sock_listening_create(nickname,sun_path,conf_p);
     if (local_socket_tb_p[i] == -1)
     {
       error = 1;
       break;
     }
  }
  if (error)
  {
    /*
    ** clean up the sockets that have been already created
    */
    for (i = 0; i < nb_instances; i++)
    {
      if (local_socket_tb_p[i] != -1) af_unix_delete_socket(local_socket_tb_p[i]);
      return -1;
    }
  }
  return 0;
}
