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
#ifndef SOCKET_CTRL_API_H
#define SOCKET_CTRL_API_H
#include <rozofs/common/types.h>
#include <sys/select.h>
#include "ruc_common.h"
#include "ruc_list.h"
#include "rozofs_fdset.h"

/*
**  max number of priority
*/
#define RUC_SOCKCTL_MAXPRIO 4

#define RUC_SOCKCTL_POLLCOUNT 16 /**< default value for former receiver ready callbacks */
#define RUC_SOCKCTL_POLLFREQ (40*1000) /**< default polling frequency in us */

// 64BITS typedef uint32_t (*ruc_pf_sock_t)(uint32,int);
typedef uint32_t (*ruc_pf_sock_t)(void * objref, int socketid);

typedef struct _ruc_sockCallBack_t
{
   ruc_pf_sock_t  isRcvReadyFunc;
   ruc_pf_sock_t  msgInFunc;
   ruc_pf_sock_t  isXmitReadyFunc;
   ruc_pf_sock_t  xmitEvtFunc;
} ruc_sockCallBack_t;

/*
** connection structure
*/

#define RUC_SOCK_MAX_NAME 48


typedef struct _ruc_sockObj_t
{
   ruc_obj_desc_t   link;
   uint32_t connectId;
   char   name[RUC_SOCK_MAX_NAME];
   int socketId;
   uint32_t speculative;    /**< asserted to one for speculative socket */
   uint32_t priority;
  // 64BITS    uint32_t objRef;
   void   *objRef;
   uint32_t rcvCount;  /* number of times the rcvbit is set */
   uint32_t xmitCount; /* number of times the xmitbit is set */
   uint32_t processed;
   uint32_t lastTime;       /* cpu time in us of last processing */
   uint32_t cumulatedTime;  /* cumulated cpu time of las processings */
   uint32_t nbTimes;       /* NB times it has been scheduled */
   uint32_t lastTimeXmit;       /* cpu time in us of last processing */
   uint32_t cumulatedTimeXmit;  /* cumulated cpu time of las processings */
   uint32_t nbTimesXmit;       /* NB times it has been scheduled */
   ruc_sockCallBack_t *callBack;
} ruc_sockObj_t;


  
extern ruc_sockObj_t *socket_predictive_ctx_table[ ];
extern int socket_predictive_ctx_table_count[];
extern int ruc_sockCtrl_speculative_sched_enable;
extern int ruc_sockCtrl_speculative_count;
extern rozo_fd_set  sockCtrl_speculative; 


/*
**   PUBLIC SERVICES API
*/

// 64BITS uint32_t ruc_sockctl_connect(int socketId,
void * ruc_sockctl_connect(int socketId,
                           char *name,
                           uint32_t priority,
			   // 64BITS                            uint32_t objRef,
                           void *objRef,
                           ruc_sockCallBack_t *callback);

// 64BITS uint32_t ruc_sockctl_disconnect(uint32 connectionId);
uint32_t ruc_sockctl_disconnect(void * connectionId);


uint32_t ruc_sockctl_init(uint32_t nbConnection);

void ruc_sockCtrl_selectWait();

/**
*  Init of the system ticker
*/
void rozofs_init_ticker();


/**
* read the system ticker
*/
extern uint64_t rozofs_ticker_microseconds;
static inline uint64_t rozofs_get_ticker_us()
{
  return rozofs_ticker_microseconds;
}
extern uint64_t rozofs_ticker_seconds;
static inline uint64_t rozofs_get_ticker_s()
{
  return rozofs_ticker_microseconds;
}

/**
* attach a traffic shaper scheduler

  @param callback
*/
typedef void (*ruc_scheduler_t)(uint64_t cur_time);  

extern ruc_scheduler_t  ruc_applicative_traffic_shaper;
extern ruc_scheduler_t  ruc_applicative_poller;

static inline void ruc_sockCtrl_attach_traffic_shaper(ruc_scheduler_t callback)
{
   ruc_applicative_traffic_shaper = callback;
}


static inline void ruc_sockCtrl_attach_applicative_poller(ruc_scheduler_t callback)
{
   ruc_applicative_poller = callback;
}

/**
* clear the associated fd bit in the fdset

  @param int fd : file descriptor to clear
*/
void ruc_sockCtrl_clear_rcv_bit(int fd);
 /*
 **_______________________________________________________________
 */
/**
*  insert the socket reference in the speculative scheduler

  @param socketRef : socket controller reference
  @retval none
*/
static inline void ruc_sockCtrl_speculative_scheduler_insert(void *connectionId)
{
   ruc_sockObj_t *p;
   
   if (ruc_sockCtrl_speculative_sched_enable == 0 ) return;
   
   
   p = (ruc_sockObj_t*)connectionId;
   if (p->speculative == 0) return;
   
   if (socket_predictive_ctx_table[p->socketId]== NULL)
   {
     socket_predictive_ctx_table[p->socketId] = p;
     FD_SET(p->socketId,&sockCtrl_speculative);
     ruc_sockCtrl_speculative_count++;
   }
   /*
   ** update the count
   */
   socket_predictive_ctx_table_count[p->socketId]+=1;
 }
 /*
 **_______________________________________________________________
 */
/**
*  insert the socket reference in the speculative scheduler

  @param socketRef : socket controller reference
  @retval none
*/
static inline void ruc_sockCtrl_speculative_scheduler_decrement(void *connectionId)
{
   ruc_sockObj_t *p;
   
   p = (ruc_sockObj_t*)connectionId;
   
   if (p->speculative == 0) return;
   if (ruc_sockCtrl_speculative_sched_enable == 0 ) return;

   /*
   ** update the count
   */
   socket_predictive_ctx_table_count[p->socketId]--;
   if (socket_predictive_ctx_table_count[p->socketId] <= 0)
   {
     ruc_sockCtrl_speculative_count--;
     if (ruc_sockCtrl_speculative_count < 0) ruc_sockCtrl_speculative_count= 0;
     socket_predictive_ctx_table_count[p->socketId]= 0;
     socket_predictive_ctx_table[p->socketId] = NULL;
     FD_CLR(p->socketId,&sockCtrl_speculative);  
   }
 }
/**
*  set/reset speculative scheduling for a socket

  @param connectId: reference of the connection
  @param speculative: 1->set/0->reset
*/
static inline void ruc_sockCtrl_set_speculative_mode(void *connectionId,int speculative)
{
   ruc_sockObj_t *p;
   p = (ruc_sockObj_t*)connectionId;
   p->speculative = speculative;

}
#endif
