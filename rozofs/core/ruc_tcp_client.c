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
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <errno.h>
#include <stdarg.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <malloc.h>

#include <rozofs/common/types.h>
#include <rozofs/common/log.h>
#include "ruc_common.h"
#include "ruc_list.h"
#include "ruc_sockCtl_api.h"
#include "ruc_buffer_api.h"
#include "ruc_timer_api.h"
#include "uma_tcp_main_api.h"
#include "uma_tcp.h"
#include "ruc_tcp_client.h"
#include "ruc_trace_api.h"
//#include "uma_mon_api.h"

/*
** G L O B A L  D A T A
*/



/*
**  Call back function for socket controller
*/

#define IPPORT2STRING(name,IpAddr,port)  sprintf(name,"%u.%u.%u.%u:%u", (IpAddr>>24)&0xFF, (IpAddr>>16)&0xFF,(IpAddr>>8)&0xFF,(IpAddr)&0xFF, port);

ruc_sockCallBack_t ruc_tcp_client_callBack=
  {
     ruc_tcp_clientisRcvReady,
     ruc_tcp_clientfake,
     ruc_tcp_clientisXmitReady,
     ruc_tcp_client_connectReply_CBK
  };

/*
**  head of the free list of the Relay-c context
*/
ruc_tcp_client_t *ruc_tcp_clientfreeList= (ruc_tcp_client_t*)NULL;
/*
** head of the active TCP  context
*/
ruc_tcp_client_t  *ruc_tcp_clientactiveList= (ruc_tcp_client_t*)NULL;

/*
**   number of context
*/
uint16_t ruc_tcp_clientnbContext;
/*
**  Init flag
*/
int ruc_tcp_clientInitDone = FALSE;



/*----------------------------------------------
   ruc_tcp_clientgetObjRef
**---------------------------------------------
** that function returns the pointer to the
** TCP CNX context
**
**  IN : tcpRef : TCP CNX object reference
**
**  OUT: NULL : out of range index
**       !=NULL : pointer to the object
**----------------------------------------------
*/
ruc_tcp_client_t *ruc_tcp_clientgetObjRef(uint32_t ObjRef)
{

   uint32_t index;
   ruc_tcp_client_t *p;

#if 0
   indexType = (ObjRef >> RUC_OBJ_SHIFT_OBJ_TYPE);
   if (indexType != ruc_tcp_CTX_TYPE)
   {
     /*
     ** not a TCP CNX index type
     */
     return (ruc_tcp_client_t*)NULL;
   }
#endif
   /*
   **  Get the pointer to the context
   */
   index = ObjRef & RUC_OBJ_MASK_OBJ_IDX;
   p = (ruc_tcp_client_t*)ruc_objGetRefFromIdx((ruc_obj_desc_t*)ruc_tcp_clientfreeList,
                                       index);
   return ((ruc_tcp_client_t*)p);
}



/*
**     CALL BACK FUNCTIONS
*/

// 64BITS uint32_t  ruc_tcp_client_rcvReadyCBK(uint32 tcpClientCnxRef,int socketId)
uint32_t  ruc_tcp_client_rcvReadyCBK(void *opaque,int socketId)
{
   ruc_tcp_client_t *pObj;
   uint64_t  tcpClientCnxRef = (uint64_t)opaque;
   pObj = ruc_tcp_clientgetObjRef((uint32_t)tcpClientCnxRef);
   if (pObj == (ruc_tcp_client_t*)NULL)
   {
      severe( " ruc_tcp_client_rcvReadyCBK: bad TCP client reference :%d ",(int)tcpClientCnxRef );
      return FALSE;
   }
   if (pObj->userRcvReadyCallBack != (ruc_pf_sock_t)NULL)
   {
//64BITS
     uint64_t userRef =  (uint64_t)pObj->userRef;
// 64BITS      return(pObj->userRcvReadyCallBack)(pObj->userRef,socketId);
      return(pObj->userRcvReadyCallBack)((void*)userRef,socketId);
   }
   /*
   ** should not occur
   */
   return TRUE;
}

//64BITS uint32_t ruc_tcp_client_connectReply_CBK(uint32 tcpClientCnxRef,int socketId)
uint32_t ruc_tcp_client_connectReply_CBK(void *opaque,int socketId)
{

   uint32_t ret;
   ruc_tcp_client_t *pObj;
   int error = -1;
   uma_tcp_create_t conf;
   uma_tcp_create_t *pconf = &conf;
   int len =sizeof(int);
   char name[32];
   //64BITS
   uint64_t tcpClientCnxRef = (uint64_t)opaque;


   /*
   **   get the TCP client connection from the list
   */
   pObj = ruc_tcp_clientgetObjRef((uint32_t)tcpClientCnxRef);
   if (pObj == (ruc_tcp_client_t*)NULL)
   {
      severe( " ruc_tcp_client_connectReply_CBK: bad TCP client reference :%d ",(int)tcpClientCnxRef );
      return RUC_NOK;
   }

  ret = RUC_OK;
  while (1)
  {
    if (getsockopt (socketId,SOL_SOCKET,
                    SO_ERROR,&error,(socklen_t*)&len) == -1)
    {
      severe( " ruc_tcp_client_connectReply_CBK: getsockopt error for context %d errno:%d ",(int)tcpClientCnxRef,errno );
      if (pObj->tcpCnxClient !=  NULL)
      {
	 ruc_sockctl_disconnect(pObj->tcpCnxClient);
	 pObj->tcpCnxClient = NULL;
      }
      close(socketId);
      pObj->tcpSocketClient = -1;

      ret = RUC_NOK;
      break;
    }
    if (error == 0)
    {
#if 0
      INFO_PRINT "Client %u.%u.%u.%u:%u connected to %u.%u.%u.%u:%u",
	pObj->srcIP>>24, (pObj->srcIP>>16)&0xFF, (pObj->srcIP>>8)&0xFF,pObj->srcIP&0xFF,
	pObj->srcTcp,
	pObj->IpAddr>>24, (pObj->IpAddr>>16)&0xFF, (pObj->IpAddr>>8)&0xFF,pObj->IpAddr&0xFF,
	pObj->tcpDestPort
	RUC_EINFO
#endif
	uma_tcp_monTxtCbk("TCP","Client %u.%u.%u.%u:%u connected to %u.%u.%u.%u:%u",
			  pObj->srcIP>>24, (pObj->srcIP>>16)&0xFF, (pObj->srcIP>>8)&0xFF,pObj->srcIP&0xFF,
			  pObj->srcTcp,
			  pObj->IpAddr>>24, (pObj->IpAddr>>16)&0xFF, (pObj->IpAddr>>8)&0xFF,pObj->IpAddr&0xFF,
			  pObj->tcpDestPort);

       /*
       ** all is fine, perform the connection with the socket
       ** controller
       */
       ruc_sockctl_disconnect(pObj->tcpCnxClient);
       pObj->tcpCnxClient = NULL;

       /*
       ** do the connection with the TCP connection handler
       */

      /*
      ** check the reference of the TCP connection
      */
      if (pObj->tcpCnxIdx == -1)
      {
	 /*
	 ** create a new connection
	 */
	 pconf->headerSize   = pObj->headerSize;
	 pconf->msgLenOffset = pObj->msgLenOffset;
	 pconf->msgLenSize   = pObj->msgLenSize;  /* sizeof(uint16_t)  FDL */
	 pconf->bufSize      = pObj->bufSize;
	 if (pObj->userRcvReadyCallBack != (ruc_pf_sock_t)NULL)
	 {
	   pconf->userRcvReadyCallBack = ruc_tcp_client_rcvReadyCBK;
	 }
	 else
	 {
	   pconf->userRcvReadyCallBack = (ruc_pf_sock_t)NULL;
	 }
	 pconf->userRcvCallBack  = ruc_tcp_client_receiveCBK;
	 pconf->userDiscCallBack = ruc_tcp_client_disc_CBK;
//64BITS
{
     uint64_t val64 = (uint64_t) pObj->ref;
	 pconf->userRef   = (void *) val64;
}
	 pconf->socketRef = socketId;

	 if ((pObj->tcpCnxIdx=uma_tcp_create_rcvRdy(pconf)) == (uint32_t)-1)
	 {
            /*
	    **  error on tcp connection creation
	    */
	    severe( " ruc_tcp_client_connectReply_CBK: fail to create to create TCP connection %d",(int)tcpClientCnxRef );

            ret =  RUC_NOK;
	    break;
	 }
	 /*
	 **  now proceed with the socket tuning and the connection with the
	 **  socket controller
	 */
	 IPPORT2STRING(name,pObj->IpAddr, pObj->tcpDestPort);
	 ret =uma_tcp_createTcpConnection(pObj->tcpCnxIdx,name);
	 if (ret != RUC_OK)
	 {
	    /*
	    **  error on tcp connection creation: proceed with the deletion
	    **  of the context
	    */
	    uma_tcp_deleteReq(pObj->tcpCnxIdx);
	    pObj->tcpCnxIdx = (uint32_t) -1;
	    pObj->tcpTimeOut = 0;
            pObj->tcpstate =UMA_TCP_IDLE;
            pObj->tcpSocketClient = -1;
	    severe( " ruc_tcp_client_connectReply_CBK: fail while tuning socket of context %d",(int)tcpClientCnxRef );

	    ret = RUC_NOK;
	    break;
	 }

	 pObj->tcpTimeOut = 0;
	 pObj->tcpstate =UMA_TCP_ACTIVE;
	 ret =  RUC_OK;
	 break;
      }
      /*
      **  the tcpcnx still exist, we just need to reconnect with the socket
      **  controller with the new socket
      */
      IPPORT2STRING(name,pObj->IpAddr, pObj->tcpDestPort);
      ret =uma_tcp_updateTcpConnection(pObj->tcpCnxIdx,socketId,name);
      if (ret != RUC_OK)
      {
	 /*
	 **  error on tcp connection update
	 */
	 uma_tcp_deleteReq(pObj->tcpCnxIdx);
	 pObj->tcpCnxIdx = (uint32_t) -1;
	 pObj->tcpTimeOut = 0;
	 pObj->tcpstate =UMA_TCP_IDLE;
	 pObj->tcpSocketClient = -1;
	 severe( " ruc_tcp_client_connectReply_CBK: fail to update TCP connection of context %d",(int)tcpClientCnxRef );
	 ret =  RUC_NOK;
	 break;
      }
      ret =  RUC_OK;
      break;
    }



    /*
    ** there is an error
    */
#if 0    
    INFO_PRINT "Client %u.%u.%u.%u:%u connection fail to %u.%u.%u.%u:%u - %s",
      pObj->srcIP>>24, (pObj->srcIP>>16)&0xFF, (pObj->srcIP>>8)&0xFF,pObj->srcIP&0xFF,
      pObj->srcTcp,
      pObj->IpAddr>>24, (pObj->IpAddr>>16)&0xFF, (pObj->IpAddr>>8)&0xFF,pObj->IpAddr&0xFF,
      pObj->tcpDestPort,
      strerror(error)
      RUC_EINFO;
#endif
    uma_tcp_monTxtCbk("TCP","Client %u.%u.%u.%u:%u connection fail to %u.%u.%u.%u:%u - %s",
		      pObj->srcIP>>24, (pObj->srcIP>>16)&0xFF, (pObj->srcIP>>8)&0xFF,pObj->srcIP&0xFF,
		      pObj->srcTcp,
		      pObj->IpAddr>>24, (pObj->IpAddr>>16)&0xFF, (pObj->IpAddr>>8)&0xFF,pObj->IpAddr&0xFF,
		  pObj->tcpDestPort,
		      strerror(error));
    switch (error)
    {
     case EADDRINUSE:
     case EBADF:
     case ENOTSOCK:
     case EISCONN:

     case ECONNREFUSED:
     case ETIMEDOUT:
     /*
     ** no listening on port
     */
     default:
#if 0
       perror("tcp client error on connect");
#endif
       break;
     }

     /*
     **  disconnect from the socket controller
     **  and close the socket
     */
     ruc_sockctl_disconnect(pObj->tcpCnxClient);
     pObj->tcpCnxClient =  NULL;
     close(socketId);
     pObj->tcpSocketClient = -1;
     ret =  RUC_NOK;
     break;
   }
   /*
   ** call the user callback on connect
   */
   //64BITS
   uint64_t userRef = (uint64_t) pObj->userRef;
   uint64_t val64 = (uint64_t) ret;
   (pObj->userConnectCallBack)((void*)userRef,pObj->tcpCnxIdx,(void*)val64);
   return RUC_OK;
}


 /*
 ** SPARE
 */

#if 0
   /*
   ** retry to connect
   */
  vSckAddr.sin_family = AF_INET;
  vSckAddr.sin_port   = htons(pObj->tcpDestPort);
  IpAddr = htonl(pObj->ipAddr);
  memcpy(&vSckAddr.sin_addr.s_addr, &IpAddr, sizeof(uint32_t));

  ret = connect(socketId,(struct sockaddr *)&vSckAddr,sizeof(struct sockaddr_in));
  if (ret == -1)
  {
    perror("big error on connect");
    ruc_sockctl_disconnect(pObj->tcpCnxClient);
    close(socketId);
    pObj->tcpSocketClient = -1;
    pObj->tcpSocketClient = ruc_tcp_client_socketCreate(pObj);
  }
#endif

//64BITS uint32_t ruc_tcp_clientisRcvReady(uint32 ref,int socketId)
uint32_t ruc_tcp_clientisRcvReady(void *ref,int socketId)
{
  /*
  ** Always TRUE
  */
  return FALSE;
}

//64BITS uint32_t ruc_tcp_clientisXmitReady(uint32 ref,int socketId)
uint32_t ruc_tcp_clientisXmitReady(void *ref,int socketId)
{


  return TRUE;
}

//64BITS uint32_t ruc_tcp_clientfake(uint32 ref,int socketId)
uint32_t ruc_tcp_clientfake(void *ref,int socketId)
{
  /*
  ** Always FALSE
  */
  return FALSE;

}



/*
**--------------------------------------------------------------------------
  uint32_t uc_tcp_client_receiveCBK(uint32 userRef,uint32 tcpCnxRef,uint32 bufRef)
**--------------------------------------------------------------------------
**  #SYNOPSIS
**   callback used by the TCP connection receiver FSM
**   when a message has been fully received on the
**   TCP connection.
**
**   When the application has finsihed to process the message, it must
**   release it
**
**   IN:
**       user reference provide at TCP connection creation time
**       reference of the TCP objet on which the buffer has been allocated
**       reference of the buffer that contains the message
**
**   OUT : none
**
**
**--------------------------------------------------------------------------
*/
//64BITS void ruc_tcp_client_receiveCBK(uint32_t tcpCnxClient,uint32 tcpCnxRef,uint32 bufRef)
void ruc_tcp_client_receiveCBK(void *opaque,uint32_t tcpCnxRef,void *bufRef)
{
   ruc_tcp_client_t *pObj;
   uint64_t  tcpCnxClient = (uint64_t) opaque;
   /*
   ** use the receive callBack of the user
   */

   /*
   **   get the TCP client connection from the list
   */
   pObj = ruc_tcp_clientgetObjRef((uint32_t)tcpCnxClient);
   if (pObj == (ruc_tcp_client_t*)NULL)
   {
      RUC_WARNING(tcpCnxClient);
      return ;
   }
   /*
   **  call the user
   */
   //64BITS
   uint64_t userRef = (uint64_t) pObj->userRef;
   (pObj->userRcvCallBack)((void*)userRef,tcpCnxRef,bufRef);

}


/*
**-------------------------------------------------------
  uint32_t ruc_tcp_client_socketCreate(ruc_tcp_client_t *pObj)
**-------------------------------------------------------
**  #SYNOPSIS
**   creation of the TCP server connection for UPPS
**
**   IN:
**
**
**
**   OUT :
**        !=-1 : OK
**        == -1 : error
**
**
**-------------------------------------------------------
*/
uint32_t ruc_tcp_client_socketCreate(ruc_tcp_client_t *pObj)
{

  int           socketId;
  int 		fileflags;
  uint32_t 	IpAddr;
  struct sockaddr_in vSckAddr;
  char          name[32];

   /*
   **  create the socket
   */


  if((socketId = socket(AF_INET, SOCK_STREAM, 0)) < 0)
  {
    /*
    ** unable to create the socket
    */
    RUC_WARNING(errno);
    return(-1);
  }
  /*
  **  It the source IP address is not IP_ANYADDR then bind
  **  the socket to the IP and TCP port
  */
  if (pObj->srcIP != INADDR_ANY)
  {
    memset(&vSckAddr, 0, sizeof(struct sockaddr_in));
    vSckAddr.sin_family = AF_INET;
    vSckAddr.sin_port   = htons(pObj->srcTcp);
    vSckAddr.sin_addr.s_addr = htonl(pObj->srcIP);
    if((bind(socketId,
            (struct sockaddr *)&vSckAddr,
             sizeof(struct sockaddr_in)))< 0)
    {
      /*
      **  error on socket binding
      */
      severe( "fail to bind socket with IP %x and TCP %x, errno = %u",pObj->srcIP,pObj->srcTcp,errno );
      close(socketId);
      return ((uint32_t)-1);
    }
  }
  /*
  ** change socket mode to asynchronous
  */
  if((fileflags=fcntl(socketId,F_GETFL,0))==-1)
  {
     RUC_WARNING(errno);
  }
  if((fcntl(socketId,F_SETFL,fileflags|O_NDELAY))==-1)
  {
    RUC_WARNING(errno);
  }
   /*
   ** perform the connection with the socket handler
   */
      /*
   **  perform the connection with the socket controller
   */
   //64BITS
   uint64_t obj_ref = (uint64_t) pObj->ref;
  IPPORT2STRING(name,pObj->IpAddr, pObj->tcpDestPort);
  pObj->tcpCnxClient = ruc_sockctl_connect (socketId,
                                          name,
                                          16,
                                          //64BITS pObj->ref,
                                          (void*) obj_ref,
                                          &ruc_tcp_client_callBack);
   if (pObj->tcpCnxClient ==  NULL)
   {
     /*
     **  fatal error while connecting with the socket controller
     */
     severe( " ruc_tcp_server_connect: unable to connect with the socket controller (%d) ",pObj->ref );
     close (socketId);
     return (uint32_t)-1;
   }
   /*
   ** do the connect with the server
   */
  vSckAddr.sin_family = AF_INET;
  vSckAddr.sin_port   = htons(pObj->tcpDestPort);
  IpAddr = htonl(pObj->IpAddr);
  memcpy(&vSckAddr.sin_addr.s_addr, &IpAddr, sizeof(uint32_t));

  connect(socketId,(struct sockaddr *)&vSckAddr,sizeof(struct sockaddr_in));
  uma_tcp_monTxtCbk ("TCP","Client %u.%u.%u.%u:%u connecting to %u.%u.%u.%u:%u",
		     pObj->srcIP>>24, (pObj->srcIP>>16)&0xFF, (pObj->srcIP>>8)&0xFF,pObj->srcIP&0xFF,
		     pObj->srcTcp,
		     pObj->IpAddr>>24, (pObj->IpAddr>>16)&0xFF, (pObj->IpAddr>>8)&0xFF,pObj->IpAddr&0xFF,
		     pObj->tcpDestPort);
  return socketId;
}

/*
**-------------------------------------------------------
  void ruc_tcp_client_disc_CBK(uint32_t nsei,uint32 tcpCnxRef)
**-------------------------------------------------------
**  #SYNOPSIS
**   That function allocates all the necessary
**   resources for UPPS TCP connections management
**
**   IN:
**       refObj : reference of the NSE context
**       tcpCnxRef : reference of the tcpConnection
**
**
**   OUT :
**        none
**
**-------------------------------------------------------
*/
//64BITS void ruc_tcp_client_disc_CBK(uint32_t refObj,uint32 tcpCnxRef)
void ruc_tcp_client_disc_CBK(void *opaque,uint32_t tcpCnxRef)
{
  ruc_tcp_client_t *p;
  uint64_t refObj = (uint64_t) opaque;
   /*
   **   get the NSE context from the active list
   */
   p = ruc_tcp_clientgetObjRef((uint32_t)refObj);
   if (p == (ruc_tcp_client_t*)NULL)
   {
      return ;
   }
   /*
   **  set the timeout val, at expiration the TCP connection
   **  is released: do not erase the reference of the TCP connection
   */
   p->tcpTimeOut = UMA_TCP_TIMEOUT_VAL;
   p->tcpstate =UMA_TCP_DISCONNECTING;

   /*
   ** close the socket and retry to re-connect
   */
   if (p->tcpSocketClient != -1)
   {
     /* FDL close(p->tcpSocketClient); */
     p->tcpSocketClient = -1;
   }
#if 0  
   INFO_PRINT "Client %u.%u.%u.%u:%u disconnected from %u.%u.%u.%u:%u",
     p->srcIP>>24, (p->srcIP>>16)&0xFF, (p->srcIP>>8)&0xFF,p->srcIP&0xFF,
     p->srcTcp,
     p->IpAddr>>24, (p->IpAddr>>16)&0xFF, (p->IpAddr>>8)&0xFF,p->IpAddr&0xFF,
     p->tcpDestPort
     RUC_EINFO;
#endif     
   uma_tcp_monTxtCbk("TCP","Client %u.%u.%u.%u:%u disconnected from %u.%u.%u.%u:%u",
		     p->srcIP>>24, (p->srcIP>>16)&0xFF, (p->srcIP>>8)&0xFF,p->srcIP&0xFF,
		     p->srcTcp,
		     p->IpAddr>>24, (p->IpAddr>>16)&0xFF, (p->IpAddr>>8)&0xFF,p->IpAddr&0xFF,
		     p->tcpDestPort);
//64BITS
{
   uint64_t val64 = (uint64_t) p->userRef;
   (p->userDiscCallBack)((void*)val64,p->tcpCnxIdx);
}
   return;
}



/*
**--------------------------------------
**    P U B L I C   F U N C T I O N S
**--------------------------------------
*/

/*
**-------------------------------------------------------
  uint32_t ruc_tcp_clientinit(uint32 nbElements)
**-------------------------------------------------------
**  #SYNOPSIS
**   That function allocates all the necessary
**   client TCP connections management
**
**   IN:
**       nbElements : number ofclient TCP connections
**                    supported
**
**
**   OUT :
**      RUC_NOK : error during the initialization
**      RUC_OK : module up and ready.
**
**-------------------------------------------------------
*/

uint32_t ruc_tcp_client_init(uint32_t nbElements)
{
  uint32_t         ret = RUC_OK;
  uint32_t         curRef;
  ruc_obj_desc_t *pnext ;
  ruc_tcp_client_t    *p;


  if (ruc_tcp_clientInitDone != FALSE)
  {
    return RUC_NOK;
  }
  while (1)
  {
    /*
    ** allocate the free connection distributor
    */
    ruc_tcp_clientfreeList = (ruc_tcp_client_t*)ruc_listCreate(nbElements,sizeof(ruc_tcp_client_t));
    if (ruc_tcp_clientfreeList == (ruc_tcp_client_t*)NULL)
    {
      /*
      ** error on distributor creation
      */
      severe( "ruc_listCreate(%d,%d)", (int)nbElements,(int)sizeof(ruc_tcp_client_t) );
      ret = RUC_NOK;
      break;
    }
    /*
    ** init of the active list
    */
    ruc_tcp_clientactiveList = (ruc_tcp_client_t*)memalign(32,sizeof(ruc_tcp_client_t));
    if (ruc_tcp_clientactiveList == (ruc_tcp_client_t*)NULL)
    {
      /*
      ** out of  pObj = ruc_tcp_clientGetNseiRef(nsei);
   if (pObj == (ruc_tcp_client_t*)NULL)
   {
      return RUC_NOK;
   } memory
      */
      severe( "ruc_tcp_clientactiveList = malloc(%d)", (int)sizeof(ruc_tcp_client_t) );
      ret = RUC_NOK;
      break;
    }
    ruc_listHdrInit((ruc_obj_desc_t*)ruc_tcp_clientactiveList);

    /*
    ** initialize each entry of the distributor
    */
    curRef = 0;
    pnext = (ruc_obj_desc_t*)NULL;
    while ((p = (ruc_tcp_client_t*)ruc_objGetNext((ruc_obj_desc_t*)ruc_tcp_clientfreeList,
                                             &pnext))
               !=(ruc_tcp_client_t*)NULL)
    {
      p->ref = curRef;

      p->tcpTimeOut = 0;
      p->tcpstate = UMA_TCP_IDLE;
      p->tcpCnxClient = NULL;
      p->tcpCnxIdx = -1;
      p->tcpSocketClient = -1;
      p->srcIP = (uint32_t)-1;
      p->srcTcp = (uint16_t) -1;
      curRef++;
    }


    /*
    **  all is done
    */
    break;
  }
  /*
  ** check if there was an error during the initialization
  */
  if (ret != RUC_OK)
  {

    return RUC_NOK;
  }
  /*
  ** everything is OK: marked it
  */
  ruc_tcp_clientInitDone = TRUE;
  return ret;
}





/*-----------------------------------------------------------------
**
**
** uint32_t ruc_tcp_client_bindClient(uint32 ipAddr,uint16_t tcpDestPort)
**
**------------------------------------------------------------------
**  #SYNOPSIS
**    That service is intended to be used by application that
**    open a TCP connection in client mode
**    The input parameters are:
**       IPaddr and TCP port of the destination (host format)
**       CallBack on Connect : (user reference, TCP connection ref, status)
**            if the status is RUC_OK: the connect has failed
**            if status is RUC_OK the connect is OK and the application
**            can start sending and receiving data on TCP connection ref
**
**      CallBack for Recv : (user reference, TCP connection ref, bufref)
**      CallBack for Disc (user ref, TCP connection ref)
**      CallBack for receiver Ready (user Ref,socketId)
**        return TRUE for ready and FALSE otherwise
**
**
**   OUT :
**      != -1 index of the connection
**      ==-1 error
**
**------------------------------------------------------------------
*/

uint32_t ruc_tcp_client_bindClient(ruc_tcp_client_create_t *pconf)
{

   ruc_tcp_client_t *pObj;

   /*
   **  allocate a free context record the IP addresses
   */
  /*
  ** get the first free Relci context
  */
  pObj = (ruc_tcp_client_t*)ruc_objGetFirst((ruc_obj_desc_t*)ruc_tcp_clientfreeList);
  if (pObj == (ruc_tcp_client_t*)NULL)
  {
    /*
    ** out of free context
    */
    return -1;
  }

  pObj->tcpCnxIdx = -1;
  pObj->tcpTimeOut = 0;
  pObj->tcpCnxClient =NULL;
  pObj->tcpstate= UMA_TCP_IDLE;
  pObj->srcIP= INADDR_ANY;

  /*
  ** store the connection infos provided by the caller
  */
  pObj->IpAddr = pconf->IpAddr;
  pObj->tcpDestPort = pconf->tcpDestPort;
  pObj->headerSize = pconf->headerSize;
  pObj->msgLenOffset =pconf->msgLenOffset;
  pObj->msgLenSize = pconf->msgLenSize;
  pObj->bufSize = pconf->bufSize;
  pObj->userRcvCallBack = pconf->userRcvCallBack;
  pObj->userDiscCallBack = pconf->userDiscCallBack;
  pObj->userConnectCallBack = pconf->userConnectCallBack;
  pObj->userRcvReadyCallBack = pconf->userRcvReadyCallBack;
  pObj->userRef =pconf->userRef;



  /*
  **  create the IP UPH SIG SMS service socket
  */
  pObj->tcpSocketClient = ruc_tcp_client_socketCreate(pObj);

  if (pObj->tcpSocketClient==(uint32_t)-1)
  {

    return -1;

   }
  /*
  ** remove it from the free list and insert it in the active list
  */
  ruc_objRemove((ruc_obj_desc_t*)pObj);
  ruc_objInsertTail((ruc_obj_desc_t*)ruc_tcp_clientactiveList,(ruc_obj_desc_t*)pObj);

  /*
  ** insert the nsei in the existing NSEI table. That table is
  ** intended to be used by the aging function
  */


  return pObj->ref;
}



/*-----------------------------------------------------------------
**
**
** uint32_t ruc_tcp_client_bindClientWithSrcIp(ruc_tcp_client_create_t *pconf)
**
**------------------------------------------------------------------
**  #SYNOPSIS
**    That service is intended to be used by application that
**    open a TCP connection in client mode. With that API
**    the caller is intended to provide a SRC IP and a src TCP
**    port in the connection interface
**    The input parameters are:
**       IPaddr and TCP port of the destination (host format)
**       srcIP : source IP address, Not significant if ANY_INADDR
**       srcTcp : source TCP port
**
**  note: IP addresses and TCP ports MUST be provided in HOST FORMAT !!!!!
**
**       CallBack on Connect : (user reference, TCP connection ref, status)
**            if the status is RUC_OK: the connect has failed
**            if status is RUC_OK the connect is OK and the application
**            can start sending and receiving data on TCP connection ref
**
**      CallBack for Recv : (user reference, TCP connection ref, bufref)
**      CallBack for Disc (user ref, TCP connection ref)
**      CallBack for receiver Ready (user Ref,socketId)
**        return TRUE for ready and FALSE otherwise
**
**
**   OUT :
**      != -1 index of the connection
**      ==-1 error
**
**------------------------------------------------------------------
*/

uint32_t ruc_tcp_client_bindClientWithSrcIp(ruc_tcp_client_create_t *pconf)
{

   ruc_tcp_client_t *pObj;

   /*
   **  allocate a free context record the IP addresses
   */
  /*
  ** get the first free Relci context
  */
  pObj = (ruc_tcp_client_t*)ruc_objGetFirst((ruc_obj_desc_t*)ruc_tcp_clientfreeList);
  if (pObj == (ruc_tcp_client_t*)NULL)
  {
    /*
    ** out of free context
    */
    return -1;
  }

  pObj->tcpCnxIdx = -1;
  pObj->tcpTimeOut = 0;
  pObj->tcpCnxClient =NULL;
  pObj->tcpstate= UMA_TCP_IDLE;
  pObj->srcIP= pconf->srcIP;
  pObj->srcTcp= pconf->srcTcp;

  /*
  ** store the connection infos provided by the caller
  */
  pObj->IpAddr = pconf->IpAddr;
  pObj->tcpDestPort = pconf->tcpDestPort;
  pObj->headerSize = pconf->headerSize;
  pObj->msgLenOffset =pconf->msgLenOffset;
  pObj->msgLenSize = pconf->msgLenSize;
  pObj->bufSize = pconf->bufSize;
  pObj->userRcvCallBack = pconf->userRcvCallBack;
  pObj->userDiscCallBack = pconf->userDiscCallBack;
  pObj->userConnectCallBack = pconf->userConnectCallBack;
  pObj->userRcvReadyCallBack = pconf->userRcvReadyCallBack;
  pObj->userRef =pconf->userRef;



  /*
  **  create the IP UPH SIG SMS service socket
  */
  pObj->tcpSocketClient = ruc_tcp_client_socketCreate(pObj);

  if (pObj->tcpSocketClient==(uint32_t)-1)
  {

    return -1;

   }
  /*
  ** remove it from the free list and insert it in the active list
  */
  ruc_objRemove((ruc_obj_desc_t*)pObj);
  ruc_objInsertTail((ruc_obj_desc_t*)ruc_tcp_clientactiveList,(ruc_obj_desc_t*)pObj);

  /*
  ** insert the nsei in the existing NSEI table. That table is
  ** intended to be used by the aging function
  */


  return pObj->ref;
}

/*
**-------------------------------------------------------
  uint32_t ruc_tcp_client_delete_connection(uint32 clientIdx)
**-------------------------------------------------------
**  #SYNOPSIS
**  that function is called to delete all the information
** related to the TCP connection that is reference by
** ClientIdx (reference has been returned by
**  the ruc_tcp_client_bindClient() service)
**
**   IN:
**       clientIdx : reference of the connection context
**
**
**   OUT :
**        none
**
**-------------------------------------------------------
*/
void ruc_tcp_client_delete_connection(uint32_t clientIdx)
{
  ruc_tcp_client_t *p;
   /*
   **   get the NSE context from the active list
   */
   p = ruc_tcp_clientgetObjRef(clientIdx);
   if (p == (ruc_tcp_client_t*)NULL)
   {
      return ;
   }
   /*
   ** clean up the context
   **
   */
   if (p->tcpCnxIdx != (uint32_t)-1)
   {
#if 0
     INFO_PRINT "Client %u.%u.%u.%u:%u request to disconnect from %u.%u.%u.%u:%u",
       p->srcIP>>24, (p->srcIP>>16)&0xFF, (p->srcIP>>8)&0xFF,p->srcIP&0xFF,
       p->srcTcp,
       p->IpAddr>>24, (p->IpAddr>>16)&0xFF, (p->IpAddr>>8)&0xFF,p->IpAddr&0xFF,
       p->tcpDestPort
       RUC_EINFO;
#endif       
     uma_tcp_monTxtCbk("TCP","Client %u.%u.%u.%u:%u request to disconnect from %u.%u.%u.%u:%u",
		       p->srcIP>>24, (p->srcIP>>16)&0xFF, (p->srcIP>>8)&0xFF,p->srcIP&0xFF,
		       p->srcTcp,
		       p->IpAddr>>24, (p->IpAddr>>16)&0xFF, (p->IpAddr>>8)&0xFF,p->IpAddr&0xFF,
		       p->tcpDestPort);

      /*
      ** ask the TCP connection handler to perform its own deletion
      */
      uma_tcp_deleteReq(p->tcpCnxIdx);
      p->tcpCnxIdx = -1;
      p->tcpSocketClient = -1;

   }
   /*
   ** clean the context with the socket handler
   */
   if (p->tcpCnxClient !=  NULL)
   {
     ruc_sockctl_disconnect(p->tcpCnxClient);
     p->tcpCnxClient = NULL;

   }
   p->tcpTimeOut = 0;
   p->tcpstate =UMA_TCP_IDLE;
   p->tcpCnxIdx = -1;

   /*
   ** put into the free list
   */
  ruc_objRemove((ruc_obj_desc_t*)p);
  ruc_objInsertTail((ruc_obj_desc_t*)ruc_tcp_clientfreeList,(ruc_obj_desc_t*)p);

}



/*
**-------------------------------------------------------
  uint32_t ruc_tcp_client_restart_connect(uint32 clientIdx)
**-------------------------------------------------------
**  #SYNOPSIS
**  Restart of a TCP connection. That service must be
**  called when there was an error either on the
** connect() or when a disconnect event has been
** received.
**
**   IN:
**       clientIdx : reference of the connection context
**
**
**   OUT :
**        none
**
**-------------------------------------------------------
*/
uint32_t ruc_tcp_client_restart_connect(uint32_t clientIdx)
{
  ruc_tcp_client_t *p;
   /*
   **   get the NSE context from the active list
   */
   p = ruc_tcp_clientgetObjRef(clientIdx);
   if (p == (ruc_tcp_client_t*)NULL)
   {
      return RUC_NOK;
   }

   p->tcpSocketClient = ruc_tcp_client_socketCreate(p);
   if (p->tcpSocketClient == -1)
   {
     /*
     ** erreor
     */
     return RUC_NOK;
   }
   return RUC_OK;

}


/*
**     S E A R C H    F U N C T I O N
*/
#if 0

/*-----------------------------------------------------------------------------
**
**
** uint32_t ruc_tcp_client_findContextFromIpAddress ( uint32 ipAddr,
**                                              uint8_t type)
**
**----------------------------------------------------------------------------
**  #SYNOPSIS
**   The purpose of that service is to return the reference of the
**   NSEI object based on the logical IP address.
**
**  IN:
**       - IP addr : searched logical IP address
**       - type: type of the logical address:
**             ruc_tcp_UPPS
**             ruc_tcp_UPH_SIG_SMS
**             ruc_tcp_UPH_CTL
**
**
**
**   OUT :
**      !=-1 : object reference
**     == -1 error
**
**----------------------------------------------------------------------------
*/

uint32_t ruc_tcp_client_findContextFromIpAddress(uint32 ipAddr, uint8_t type)
{
  ruc_obj_desc_t *pnext;
  ruc_tcp_client_t    *p;
   switch (type)
   {
     case ruc_tcp_UPPS:
     case ruc_tcp_UPH_CTL:
     case ruc_tcp_UPH_SIG_SMS:

     break;

    default:
      printf("ruc_tcp_client_appendTcpConnectionToNsei: bad type %d\n",type);
      return -1;
   }

  pnext = (ruc_obj_desc_t*)NULL;
  while ((p = (ruc_tcp_client_t*)ruc_objGetNext((ruc_obj_desc_t*)ruc_tcp_clientactiveList,
                                           &pnext))
             !=(ruc_tcp_client_t*)NULL)
  {
    if (type == ruc_tcp_UPPS)
    {
      if (p->ipAddr == ipAddr)
      {
	/*
	** context found
	*/
	return (p->ref);
      }
    }
    else
    {
      if (p->ipAddrLogRef == ipAddr)
      {
	/*
	** context found
	*/
	return (p->ref);
      }
    }
  }
  /*
  ** not found
  */
  return (uint32_t) -1;

}



/*---------------------------------------------------------------------
**
**
** uint32_t ruc_tcp_client_findTcpCnxBasedOnType(uint16_t nsei, uint8_t type, uint32 *pTcpref)
**
**---------------------------------------------------------------------
**  #SYNOPSIS
**   That service returns the reference of the TCP
** connection associated to the input type. That service is
** typcally used when an application has a message to send
** on a NSEI. For that purpose, it needs to get the reference
** of the TCP connection that must be used for sending its message.
**
**  (mainly used from MS context)

**
**  it returns RUC_NOK for the following cases:
**       - the NSEI is not in the active list or
**         does not exist
**       - unknown type
**
**   IN:
**      - the value of the NSEI
**      - the type of the TCP connection:
**             ruc_tcp_UPPS
**             ruc_tcp_UPH_SIG_SMS
**             ruc_tcp_UPH_CTL
**       - pointer to the array in which the reference of the TCP connection is filled
**
**   OUT :
**      == RUC_NOK :error, pTcpRef is not significant.
**     == RUC_OK, the pTcpRef is significant, but can be -1 if the connection
**                is not active
**
**---------------------------------------------------------------------
*/

uint32_t ruc_tcp_client_findTcpCnxBasedOnType(uint16_t nsei, uint8_t type,uint32 *pTcpRef)
{
  ruc_tcp_client_t    *pObj;
   switch (type)
   {
     case ruc_tcp_UPPS:
     case ruc_tcp_UPH_CTL:
     case ruc_tcp_UPH_SIG_SMS:

     break;

    default:
     *pTcpRef = -1;
      severe( "findTcpCnxBasedOnType(%d,%d): bad type",nsei,type );
      return RUC_NOK;
   }
   /*
   ** Search if the NSEI has been already defined
   */
   pObj = ruc_tcp_clientGetNseiRef(nsei);
   if (pObj == (ruc_tcp_client_t*)NULL)
   {
      *pTcpRef = -1;
      severe( "findTcpCnxBasedOnType(%d,%d): bad NSEI",nsei,type );
      return RUC_NOK;
   }
   *pTcpRef = pObj->tcpCnxIdx[type];
   return(RUC_OK);
 }


#endif





