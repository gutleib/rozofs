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

#include "ruc_common.h"
#include "ruc_trace_api.h"
#include "ruc_sockCtl_api_th.h"
#include "uma_dbg_api.h"
#include "ruc_timer_struct.h"
#include "ruc_timer_api_th.h"

typedef struct ruc_timer_th_t
{
 uint32_t     active;   /* TRUE/FALSE */
/*
**   internal socket reference
*/
  pthread_t           thrdId; /* of timer thread */
  int       internalSocket[2];  /* -1 if N.S */
  void *    intSockconnectionId[2];  /* -1: connection id returned by the sock Ctrl */
  int       module_idx;
} ruc_timer_th_t;


/*
**   G L O B A L    D A T A
*/



uint32_t ruc_timer_rcvReadyInternalSock_th(void * timerRef,int socketId);
uint32_t ruc_timer_rcvMsgInternalSock_th(void * timerRef,int socketId);
uint32_t ruc_timer_xmitReadyInternalSock_th(void * timerRef,int socketId);
uint32_t ruc_timer_xmitEvtInternalSock_th(void * timerRef,int socketId);
void ruc_timer_th_tickReceived_th(ruc_timer_th_t *p);
void *ruc_timer_TickerThread_th(void *arg);


/*
**  Call back function for socket controller
*/
static ruc_sockCallBack_t ruc_timer_callBack_InternalSock=
  {
     ruc_timer_rcvReadyInternalSock_th,
     ruc_timer_rcvMsgInternalSock_th,
     ruc_timer_xmitReadyInternalSock_th,
     ruc_timer_xmitEvtInternalSock_th
  };

static uint32_t timer_lock_for_debug = 0;


/*----------------------------------------------
**  ruc_timer_generateTicker_th
**----------------------------------------------
**
**  that function builds the event message and
**  sends it to the internal socket.
**
**
**  IN :
**     p : Relci object pointer
**     evt : RUC_TIMER_TICK
**
**  OUT :NONE
**
**-----------------------------------------------
*/


void ruc_timer_generateTicker_th(ruc_timer_th_t *p,uint32_t evt)
{
  int nBytes;
  ruc_timer_signalMsg_t timerSignal;

  timerSignal.timerRef = p;
  timerSignal.signalType = RUC_TIMER_TICK;

  nBytes = send(p->internalSocket[RUC_SOC_SEND],
                (const char *)&timerSignal,
                sizeof(ruc_timer_signalMsg_t),
                0);
  if (nBytes != sizeof(ruc_timer_signalMsg_t))
  {
    /*
    **  message not sent
    */
#if 0
    RUC_WARNING(errno);
#endif
  }
}


/*----------------------------------------------
**  ruc_timer_getIntSockIdxFromSocketId_th
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

uint32_t  ruc_timer_getIntSockIdxFromSocketId_th(ruc_timer_th_t *p,int socketId)
{
   int i;


   for (i = 0;i < 2;i++)
   {
     if (p->internalSocket[i]==socketId) return (uint32_t)i;
   }
   return -1;
}



/*----------------------------------------------
**  ruc_timer_rcvReadyInternalSock_th
**----------------------------------------------
**
**   receive ready function: only for
**   receiver socket. Nothing expected
**   on sending socket
**
**
**  IN :
**     timerRef : Relci instance index
**     socketId : socket Identifier
**
**  OUT : always TRUE for RUC_SOC_RECV
**        always FALSE for RUC_SOC_SEND
**
**-----------------------------------------------
*/

uint32_t ruc_timer_rcvReadyInternalSock_th(void * timerRef,int socketId)
{
  ruc_timer_th_t *p;
  uint32_t      socketIdx;

  /*
  **  Get the pointer to the timer Object
  */
  p = (ruc_timer_th_t*)timerRef;
  if (p == (ruc_timer_th_t*)NULL)
  {
    /*
    ** bad reference
    */
    RUC_WARNING(timerRef);
    return FALSE;
  }
  socketIdx = ruc_timer_getIntSockIdxFromSocketId_th(p,socketId);
  if (socketIdx == -1)
  {
    /*
    ** something really wrong
    */
    RUC_WARNING(p);
    return FALSE;
  }
  if (socketIdx == RUC_SOC_SEND)
    return FALSE;
  else
    return TRUE;
}

/*----------------------------------------------
**  ruc_timer_rcvMsgInternalSock_th
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
**     timerRef : Relci instance index
**     socketId : socket Identifier
**
**  OUT : always TRUE for RUC_SOC_RECV
**        always FALSE for RUC_SOC_SEND
**
**-----------------------------------------------
*/
uint32_t ruc_timer_rcvMsgInternalSock_th(void * timerRef,int socketId)
{
  ruc_timer_th_t *p;
  uint32_t      socketIdx;
  int         bytesRcvd;
  ruc_timer_signalMsg_t timerSignal;

  /*
  **  Get the pointer to the timer Object
  */
  p = (ruc_timer_th_t*)timerRef;
  if (p == (ruc_timer_th_t*)NULL)
  {
    /*
    ** bad reference
    */
    return FALSE;
  }
  socketIdx = ruc_timer_getIntSockIdxFromSocketId_th(p,socketId);
  if (socketIdx == -1)
  {
    /*
    ** something really wrong
    */
    return FALSE;
  }

  if (socketIdx == RUC_SOC_SEND)
  {
    /*
    **  should not occur
    */
    return FALSE;
  }
  /*
  **  Ticker received
  **
  */
  bytesRcvd = recv(socketId,
                   (char *)&timerSignal,
                   sizeof(ruc_timer_signalMsg_t),
                   0);
  if (bytesRcvd != sizeof(ruc_timer_signalMsg_t))
  {
    /*
    **  something wrong : (BUG)
    */
    RUC_WARNING(errno);
    return TRUE;
  }
  /*
  **  process the signal
  */
  switch ( timerSignal.signalType)
  {
    case RUC_TIMER_TICK:
      ruc_timer_th_tickReceived_th(p);
      break;
    default:
      RUC_WARNING(timerSignal.signalType);
      break;
  }

  return TRUE;
}


/*----------------------------------------------
**  ruc_timer_xmitReadyInternalSock_th
**----------------------------------------------
**
**   xmit ready function: only for
**   xmit socket. Nothing expected
**   on receiving socket.
**
**
**  IN :
**     timerRef : Relci instance index
**     socketId : socket Identifier
**
**  OUT : always FALSE for RUC_SOC_RECV
**        always FALSE for RUC_SOC_SEND
**  There is not congestion on the internal socket
**
**-----------------------------------------------
*/
uint32_t ruc_timer_xmitReadyInternalSock_th(void * timerRef,int socketId)
{
  ruc_timer_th_t *p;
  uint32_t      socketIdx;

  /*
  **  Get the pointer to the timer Object
  */
  p = (ruc_timer_th_t*)timerRef;
  if (p == (ruc_timer_th_t*)NULL)
  {
    /*
    ** bad reference
    */
    RUC_WARNING(timerRef);
    return FALSE;
  }
  socketIdx = ruc_timer_getIntSockIdxFromSocketId_th(p,socketId);
  if (socketIdx == -1)
  {
    /*
    ** something really wrong
    */
    RUC_WARNING(p);
    return FALSE;
  }

  if (socketIdx == RUC_SOC_RECV)
    return FALSE;
  else
    return FALSE;
}


/*----------------------------------------------
**  ruc_timer_xmitEvtInternalSock_th
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
uint32_t ruc_timer_xmitEvtInternalSock_th(void * timerRef,int socketId)
{
  ruc_timer_th_t *p;

  /*
  **  Get the pointer to the timer Object
  */
  p = (ruc_timer_th_t*)timerRef;
  if (p == (ruc_timer_th_t*)NULL)
  {
    /*
    ** bad reference
    */
    RUC_WARNING(timerRef);
    return FALSE;
  }
  RUC_WARNING(p);
  return FALSE;
}
/*
**    I N T E R N A L   S O C K E T
**    CREATION/DELETION
*/


/*----------------------------------------------
**  ruc_timer_createInternalSocket_th (private)
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

static uint32_t ruc_timer_createInternalSocket_th(void *sock_ctx_p,ruc_timer_th_t *p)
{
  int    ret;
  uint32_t retcode = RUC_NOK;
  int    fileflags;


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
    RUC_WARNING(errno);
    return RUC_NOK;
  }
  while (1)
  {
    /*
    ** change socket mode to asynchronous
    */
    if((fileflags=fcntl(p->internalSocket[RUC_SOC_SEND],F_GETFL,0))==-1)
    {
      RUC_WARNING(errno);
      break;
    }
    if(fcntl(p->internalSocket[RUC_SOC_SEND],F_SETFL,fileflags|O_NDELAY)==-1)
    {
      RUC_WARNING(errno);
      break;
    }
    /*
    ** 2 - perform the connection with the socket controller
    */
    p->intSockconnectionId[RUC_SOC_SEND]=ruc_sockctl_connect_th(sock_ctx_p,p->internalSocket[RUC_SOC_SEND],
                                			     "TMR_SOCK_XMIT",
                                			      16,
                                			      p,
                                			      &ruc_timer_callBack_InternalSock);
    if (p->intSockconnectionId[RUC_SOC_SEND]== NULL)
    {
      RUC_WARNING(RUC_SOC_SEND);
      break;
    }
    p->intSockconnectionId[RUC_SOC_RECV]=ruc_sockctl_connect_th(sock_ctx_p,p->internalSocket[RUC_SOC_RECV],
                                     "TMR_SOCK_RECV",
                                      16,
                                      p,
                                      &ruc_timer_callBack_InternalSock);
    if (p->intSockconnectionId[RUC_SOC_RECV]== NULL)
    {
      RUC_WARNING(RUC_SOC_SEND);
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
      ruc_sockctl_disconnect_th(sock_ctx_p,p->intSockconnectionId[RUC_SOC_RECV]);
      p->intSockconnectionId[RUC_SOC_RECV] = NULL;
    }

    if (p->intSockconnectionId[RUC_SOC_SEND] != NULL)
    {
      ruc_sockctl_disconnect_th(sock_ctx_p,p->intSockconnectionId[RUC_SOC_SEND]);
      p->intSockconnectionId[RUC_SOC_SEND] = NULL;
    }
    return RUC_NOK;
  }
  return RUC_OK;
}


/*----------------------------------------------
**  ruc_timer_deleteInternalSocket_th (private)
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

uint32_t ruc_timer_deleteInternalSocket_th(void *sock_ctx_p,ruc_timer_th_t *p)
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
    ruc_sockctl_disconnect_th(sock_ctx_p,p->intSockconnectionId[RUC_SOC_RECV]);
    p->intSockconnectionId[RUC_SOC_RECV] = NULL;
  }
  // 64BITS   if (p->intSockconnectionId[RUC_SOC_SEND] != (uint32_t) NULL)
  if (p->intSockconnectionId[RUC_SOC_SEND] != NULL)
  {
    ruc_sockctl_disconnect_th(sock_ctx_p,p->intSockconnectionId[RUC_SOC_SEND]);
    p->intSockconnectionId[RUC_SOC_SEND] = NULL;
  }
  p->active = TRUE;

  return RUC_OK;
}



/*
**   P D P   C O N T E X T   S I G N A L    A P I
*/




/*----------------------------------------------
**  ruc_timer_th_tickReceived_th (private)
**----------------------------------------------
**
**  That function processes all the PDP context
**  that have been queued in the signal Queue
**  of the Relci object.
**
** note:
**  The function process up to 16 events before
**  leaving. The remaining of the queue will
**  be processed on the next select
**
**  IN :
**     p :timer module context pointer
**
**  OUT :
**    NONE
**
**-----------------------------------------------
*/


void ruc_timer_th_tickReceived_th(ruc_timer_th_t *p)
{

#if 0
  RUC_TIMER_TRC("timer_tickReceived",-1,-1,-1,-1);
#endif
  
  p->active = TRUE;
  ruc_timer_process_th();
  p->active = FALSE;


}



/*----------------------------------------------
**  ruc_timer_th_threadCreate_th
**----------------------------------------------
**
**
**  IN :
**
**  OUT :
**    RUC_OK/RUC_NOK
**
**-----------------------------------------------
*/

uint32_t ruc_timer_th_threadCreate_th( pthread_t  *thrdId ,
                               void         *arg )
{
  pthread_attr_t      attr;
  int                 err;

  /*
  ** The thread is initialized with
  ** attributes object default values
  */
  if((err=pthread_attr_init(&attr)) != 0)
  {
    RUC_WARNING(errno);
   return RUC_NOK;
  }
  /*
  ** Create the thread
  */
  if((err=pthread_create(thrdId,&attr,ruc_timer_TickerThread_th,arg)) != 0)
  {
    RUC_WARNING(errno);
    return RUC_NOK;

  }
  
  return RUC_OK;
}


/*----------------------------------------------
**  ruc_timer_moduleInit (public)
**----------------------------------------------
**
**
**  IN :
**     active : TRUE/FALSE
    @param sock_ctx_p: pointer to the socket controller context
**
**  OUT :
**    RUC_OK/RUC_NOK
**
**-----------------------------------------------
*/


uint32_t ruc_timer_moduleInit_th(void *sock_ctx_p,uint32_t active)

{
  ruc_timer_th_t *p;
  uint32_t      ret;
  /*
  **  Get the pointer to the timer Object
  */
  p = malloc(sizeof(ruc_timer_th_t));
  if (p == NULL) return RUC_NOK;
  
  p->module_idx = ruc_sockctl_get_module_idx_th(sock_ctx_p);
  p->active = FALSE;
  

   /*
   ** create the timer data structure
   */
    ruc_timer_init_th (TIMER_TICK_VALUE_100MS,64*2);
   /*
   ** create the internal socket
   */

   ret = ruc_timer_createInternalSocket_th(sock_ctx_p,p);
   if (ret != RUC_OK)
     return ret;

   /*
   ** create the timer thread
   */

   ret = ruc_timer_th_threadCreate_th(&p->thrdId,(void*)p);
   return ret;
}




/*
**   T I C K E R   T H R E A D
*/


 struct timespec  ruc_ticker;

void *ruc_timer_TickerThread_th(void *arg)
{

  ruc_timer_th_t *p;
  int count = 0;
  char name[128];

  p = (ruc_timer_th_t *)arg;
  sprintf(name,"Ticker_%d",p->module_idx);

  uma_dbg_thread_add_self(name);

  /*
  **  1 second ticker
  */
 while(1)
 {
   ruc_ticker.tv_sec=0;
   ruc_ticker.tv_nsec=TIMER_TICK_VALUE_20MS*1000*1000;
   nanosleep(&ruc_ticker,(struct timespec *)NULL);
   
   if (timer_lock_for_debug) continue;
   
    if (p->active == FALSE)
    {
      /*
      ** signal the tick to the main thread
      */
      ruc_timer_generateTicker_th(p,0 /* N.S */);
    }
    count++;
    if (count == 4*5)
    {
      count = 0;
    }
  }
}
