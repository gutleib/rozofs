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
#ifndef RUC_TIMER_STRUCT_H
#define RUC_TIMER_STRUCT_H

#include <stdio.h>
#include <pthread.h>

#include <rozofs/common/types.h>

#include "ruc_common.h"
#include "ruc_list.h"
#include "ruc_timer.h"
#include "ruc_trace_api.h"



/*
**   Trace
*/
#define RUC_TIMER_TRC(name,p1,p2,p3,p4) { if (ruc_timer_trace==TRUE) \
                                          ruc_trace(name,(uint32_t)p1,(uint32_t)p2,(uint32_t)p3,(uint32_t)p4); }


/*
**   timer object  structure
*/

typedef struct ruc_timer_t
{
 uint32_t     active;   /* TRUE/FALSE */
/*
**   internal socket reference
*/
  pthread_t           thrdId; /* of timer thread */
  int       internalSocket[2];  /* -1 if N.S */
  void *    intSockconnectionId[2];  /* -1: connection id returned by the sock Ctrl */
} ruc_timer_t;



/*
**---------------------------------
**  prototypes (Private functions)
**---------------------------------
*/


/*
**  internal socket prototypes
*/

// 64BITS uint32_t ruc_timer_rcvReadyInternalSock(uint32 timerRef,int socketId);
uint32_t ruc_timer_rcvReadyInternalSock(void * timerRef,int socketId);
// 64BITS uint32_t ruc_timer_rcvMsgInternalSock(uint32 timerRef,int socketId);
uint32_t ruc_timer_rcvMsgInternalSock(void * timerRef,int socketId);
// 64BITS uint32_t ruc_timer_xmitReadyInternalSock(uint32 timerRef,int socketId);
uint32_t ruc_timer_xmitReadyInternalSock(void * timerRef,int socketId);
// 64BITS uint32_t ruc_timer_xmitEvtInternalSock(uint32 intSockRef,int socketId);
uint32_t ruc_timer_xmitEvtInternalSock(void * intSockRef,int socketId);
uint32_t ruc_timer_createInternalSocket(ruc_timer_t *p);
uint32_t ruc_timer_deleteInternalSocket(ruc_timer_t *p);

/*
**  Relci signals message structure and prototypes
*/

#define RUC_TIMER_TICK 0x54

typedef struct _ruc_timer_signalMsg_t
{
  // 64BITS   uint32_t timerRef;  /* N.S */
  void * timerRef;  /* N.S */
  uint32_t signalType; /* RUC_TIMER_TICK */
} ruc_timer_signalMsg_t;




void ruc_timer_tickReceived(ruc_timer_t *p);
ruc_timer_t *ruc_timer_getObjRef();
void ruc_timer_generateTicker(ruc_timer_t *p,uint32_t evt);
uint32_t  ruc_timer_getIntSockIdxFromSocketId(ruc_timer_t *p,int socketId);
uint32_t  ruc_timer_threadCreate( pthread_t  *thrdId ,
                                      void *arg );

void *ruc_timer_TickerThread(void *);

#endif
