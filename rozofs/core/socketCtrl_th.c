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
#include <strings.h>
#include <unistd.h>
#include <fcntl.h>     
#include <pthread.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/resource.h>
#include <rozofs/common/log.h>
#include <rozofs/core/ruc_common.h>
#include "ruc_list.h"
#include "socketCtrl_th.h"
#include "uma_dbg_api.h"
//#include "af_unix_socket_generic_api.h"
//#include "af_unix_socket_generic.h"
//#include "north_lbg.h"
#include "rozofs_string.h"


#define RUC_SOCKCTRL_DEBUG_TOPIC      "cpu"
#define RUC_SOCKCTRL_CTX_TOPIC        "ctx_size"

#define APP_POLLING 1
#define APP_POLLING_OPT 1
#define ROZO_MES 1

#define MICROLONG(time) ((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)
/*
**  G L O B A L   D A T A
*/

int ruc_sockctl_current_idx = 1;
rozofs_sockctrl_ctx_t *ruc_sockctl_ctx_tb[ROZOFS_MAX_SOCKCTL_DESCRIPTOR]={NULL};
pthread_mutex_t   ruc_sockctl_thread_mutex = PTHREAD_MUTEX_INITIALIZER;

static char    myBuf[UMA_DBG_MAX_SEND_SIZE];
/*
**  F U N C T I O N S
*/
/*
**____________________________________________________________________________
*/
/**
* api for reading the cycles counter
*/

static __inline__ unsigned long long rdtsc(void)
{
  unsigned hi,lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo)| (((unsigned long long)hi)<<32);

}

/*
**____________________________________________________________________________
*/
/**
*  User receive ready polling: it concerns socket which priority is greater than 4


*/

static void ruc_sockCtrl_socket_poll_th(rozofs_sockctrl_ctx_t *sock_p)
{
  ruc_sockObj_t *p;
  int count = 0;
  

    while ((p = (ruc_sockObj_t*)
              ruc_objGetNext((ruc_obj_desc_t*)&sock_p->ruc_sockCtl_tabPrio[RUC_SOCKCTL_MAXPRIO],
                             &sock_p->ruc_sockctl_poll_pnextCur))!=(ruc_sockObj_t*)NULL) 
   {
      (*((p->callBack)->isRcvReadyFunc))(p->objRef,p->socketId); 
      count++; 
      if (count == sock_p->ruc_sockCtrl_max_poll_ctx) break;
   }
   if (p == NULL) sock_p->ruc_sockctl_poll_pnextCur = (ruc_obj_desc_t*)NULL;
}
/*
**____________________________________________________________________________
*/
/**
*  the purpose of that function is to return the list of the socket for which 
   there is a pending event (read or write)
  
   @param sock_p: socket controller context
   @param fdset_p : pointer to the fd set that contains the asserted bits
   @param socket_tab_p : pointer to the array that will contain the sockets for which there is an event
   @param nb_sockets : number of bits asserted in the set
   
   @retval number of sockets for which an event is asserted 
*/
static inline int ruc_sockCtrl_build_sock_table_th(rozofs_sockctrl_ctx_t *sock_p,uint64_t *fdset_p,int *socket_tab_p,int nb_sockets)
{
   int i;
   uint32_t val32;
   uint64_t val64;
   uint32_t bit;
   int curr_socket_id;
   int socket_count = 0;
   int last_sock;
   
   curr_socket_id = 0;
   
   last_sock = (sock_p->ruc_max_curr_socket+1)/64;
   if ((sock_p->ruc_max_curr_socket+1)%64) last_sock=last_sock+1;
   
   for (i = 0; i < last_sock; i++)
   {
     val64 = fdset_p[i];
     curr_socket_id = i*(sizeof(uint64_t)*8);
     if (val64 == 0) 
     {
       continue;
     }     
     /*
     ** get the socket in the 0..31 range
     */
     val32 = val64 & 0xffffffff;
     while(val32 != 0)
     {
       bit = __builtin_ffs(val32);
       socket_tab_p[socket_count]=  curr_socket_id+bit-1;
       socket_count++;
       if (socket_count == nb_sockets) return nb_sockets;
       val32 &=(~(1<<(bit-1)));
     }
     curr_socket_id +=32;
     val32 = val64 >> 32;
     while(val32 != 0)
     {
       bit = __builtin_ffs(val32);
       socket_tab_p[socket_count]=  curr_socket_id+bit-1;
       socket_count++;
       if (socket_count == nb_sockets) return nb_sockets;
       val32 &=(~(1<<(bit-1)));
     }

   }
   return socket_count;
}

/*
**____________________________________________________________________________
*/
static void ruc_sockCtrl_remove_socket(int *sock_table_p,int nb_sockets,int socket_id)
{
   int i;
   for (i = 0; i < nb_sockets; i++)
   {
     if (sock_table_p[i] != socket_id) continue;
     sock_table_p[i] = -1;
   }
}


/*
**   D E B U G 
*/


void ruc_sockCtrl_debug_show_th(int ctx_index,uint32_t tcpRef, void * bufRef) {
  ruc_sockObj_t     *p;
  int                i;
  char           *pChar=myBuf;
  uint32_t          average;
  
  if (ruc_sockctl_ctx_tb == NULL)
  {
     pChar += sprintf(pChar,"no other socket context context available\n");  
     uma_dbg_send(tcpRef,bufRef,TRUE,myBuf);
     return;
  }
  if (ctx_index >= ruc_sockctl_current_idx )
  {
     pChar += sprintf(pChar,"index is out of range (max: %d\n",ruc_sockctl_current_idx);  
     uma_dbg_send(tcpRef,bufRef,TRUE,myBuf);
     return;
  } 
  
  rozofs_sockctrl_ctx_t *sock_p =ruc_sockctl_ctx_tb[ctx_index];
  if (sock_p == NULL)
  {
     pChar += sprintf(pChar,"no socket context context available\n");  
     uma_dbg_send(tcpRef,bufRef,TRUE,myBuf);  
     return;  
  }

  p = sock_p->ruc_sockCtrl_pFirstCtx;
  pChar += sprintf(pChar,"socket controller        :%d\n",ruc_sockctl_current_idx);
  pChar += sprintf(pChar,"speculative scheduler    :%s\n",(sock_p->ruc_sockCtrl_speculative_sched_enable==0)?" Disabled":" Enabled");
  pChar += rozofs_string_append(pChar,"conditional sockets      : ");
  pChar += rozofs_u32_append(pChar ,sock_p->ruc_sockCtrl_max_nr_select);
  *pChar++ = '\n';

  pChar += rozofs_string_append(pChar,"max socket events        : ");
  pChar += rozofs_u32_append(pChar ,sock_p->ruc_sockCtrl_max_nr_select);
  *pChar++ = '\n';
  pChar += rozofs_string_append(pChar,"max speculative events   : ");
  pChar += rozofs_u32_append(pChar ,sock_p->ruc_sockCtrl_max_speculative);
  *pChar++ = '(';
  pChar += rozofs_u32_append(pChar ,sock_p->ruc_sockCtrl_speculative_count);
  *pChar++ = ')';
  *pChar++ = '\n';
  sock_p->ruc_sockCtrl_max_nr_select = 0;
  sock_p->ruc_sockCtrl_max_speculative = 0;

  pChar += rozofs_string_append(pChar,"xmit/recv prepare cycles : ");
  pChar += rozofs_u64_append(pChar ,(sock_p->ruc_count_prepare==0)?0:(long long unsigned)sock_p->ruc_time_prepare/sock_p->ruc_count_prepare);
  pChar += rozofs_string_append(pChar," cycles [");
  pChar += rozofs_u64_append(pChar ,sock_p->ruc_time_prepare);
  *pChar++ = '/';
  pChar += rozofs_u64_append(pChar ,sock_p->ruc_count_prepare);
  *pChar++ = ']';  
  *pChar++ = '\n';
  sock_p->ruc_time_prepare = 0;
  sock_p->ruc_count_prepare = 0;
  pChar += rozofs_string_append(pChar,"max recv buffered evts   : ");
  pChar += rozofs_u64_append(pChar ,(long long unsigned) sock_p->af_unix_rcv_buffered);
  *pChar++ = '\n';
  sock_p->af_unix_rcv_buffered = 0;  

  pChar += rozofs_string_append(pChar,"xmit/recv receive cycles : ");
  pChar += rozofs_u64_append(pChar ,(sock_p->ruc_count_receive==0)?0:(long long unsigned)sock_p->ruc_time_receive/sock_p->ruc_count_receive);
  pChar += rozofs_string_append(pChar," cycles [");
  pChar += rozofs_u64_append(pChar ,sock_p->ruc_time_receive);
  *pChar++ = '/';
  pChar += rozofs_u64_append(pChar ,sock_p->ruc_count_receive);
  *pChar++ = ']';  
  *pChar++ = '\n';
  sock_p->ruc_time_receive = 0;
  sock_p->ruc_count_receive = 0;							       

  pChar += rozofs_string_append(pChar,"gettimeofday cycles      : ");
  pChar += rozofs_u64_append(pChar ,(sock_p->gettimeofday_count==0)?0:(long long unsigned)sock_p->gettimeofday_cycles/sock_p->gettimeofday_count);
  pChar += rozofs_string_append(pChar," cycles [");
  pChar += rozofs_u64_append(pChar ,sock_p->gettimeofday_cycles);
  *pChar++ = '/';
  pChar += rozofs_u64_append(pChar ,sock_p->gettimeofday_count);
  *pChar++ = ']';  
  *pChar++ = '\n';
  sock_p->gettimeofday_cycles = 0;
  sock_p->gettimeofday_count  = 0;	

  pChar += rozofs_string_append(pChar,"application poll cycles  : ");
  pChar += rozofs_u64_append(pChar ,(sock_p->ruc_applicative_poller_count==0)?0:(long long unsigned)sock_p->ruc_applicative_poller_cycles/sock_p->ruc_applicative_poller_count);
  pChar += rozofs_string_append(pChar," cycles [");
  pChar += rozofs_u64_append(pChar ,sock_p->ruc_applicative_poller_cycles);
  *pChar++ = '/';
  pChar += rozofs_u64_append(pChar ,sock_p->ruc_applicative_poller_count);
  *pChar++ = ']';  
  *pChar++ = '\n';
  sock_p->ruc_applicative_poller_cycles = 0;
  sock_p->ruc_applicative_poller_count  = 0;	
  
  pChar += rozofs_string_append(pChar,"sock_p->rucRdFdSet ");
  pChar += rozofs_x64_append(pChar,(uint64_t)&sock_p->rucRdFdSet);
  pChar += rozofs_string_append(pChar," (");
  pChar += rozofs_u64_append(pChar,sizeof(sock_p->rucRdFdSet));  
  pChar += rozofs_string_append(pChar,") __FD_SETSIZE :"); 
  pChar += rozofs_u32_append(pChar,ROZO_FD_SETSIZE);
  pChar += rozofs_string_append(pChar," __NFDBITS :"); 
  pChar += rozofs_u32_append(pChar,__NFDBITS);
  *pChar++ = '\n';
    
  pChar += rozofs_string_append(pChar,"select max cpu time : ");
  pChar += rozofs_u32_append(pChar,sock_p->ruc_sockCtrl_looptimeMax);
  pChar += rozofs_string_append(pChar," us\n");   
  sock_p->ruc_sockCtrl_looptimeMax = 0;   

  pChar += rozofs_string_append(pChar,"\napplication                      sock       last  cumulated activation    average\n");
  pChar += rozofs_string_append(pChar,"name                               nb        cpu        cpu      times        cpu  prio\n\n");
    
  for (i = 0; i < sock_p->ruc_sockCtrl_maxConnection; i++)
  {
    if (p->socketId !=(uint32_t)-1)
    {
      if (p->nbTimes == 0) average = 0;
      else                 average = p->cumulatedTime/p->nbTimes;
      pChar += rozofs_string_padded_append(pChar, 33, rozofs_left_alignment, &p->name[0]);
      pChar += rozofs_u32_padded_append(pChar,  4, rozofs_right_alignment, p->socketId);
      pChar += rozofs_u64_padded_append(pChar, 11, rozofs_right_alignment, p->lastTime);
      pChar += rozofs_u64_padded_append(pChar, 11, rozofs_right_alignment, p->cumulatedTime);
      pChar += rozofs_u64_padded_append(pChar, 11, rozofs_right_alignment, p->nbTimes);
      pChar += rozofs_u64_padded_append(pChar, 11, rozofs_right_alignment, average);
      pChar += rozofs_u64_padded_append(pChar,  5, rozofs_right_alignment, p->priority);
      *pChar++ = ' ';
      pChar += rozofs_u32_append(pChar, (FD_ISSET(p->socketId, &sock_p->rucRdFdSetUnconditional)==0)?0:1);
      *pChar++ = '-';      
      pChar += rozofs_u32_append(pChar, (FD_ISSET(p->socketId, &sock_p->rucWrFdSetCongested)==0)?0:1);  
      *pChar++ = '-';      
      pChar += rozofs_u32_append(pChar, (p->speculative==0)?0:1);  
      *pChar++ = '-';      
      pChar += rozofs_u32_append(pChar, (FD_ISSET(p->socketId, &sock_p->sockCtrl_speculative)==0)?0:1);  
      *pChar++ = '-';      
      pChar += rozofs_u32_append(pChar, sock_p->socket_predictive_ctx_table_count[p->socketId]);  

      *pChar ++ = '\n';
                
      p->cumulatedTime = 0;
      p->nbTimes = 0;
    }
    p++;
  }

  if (sock_p->ruc_sockCtrl_nbTimesScheduler == 0) average = 0;
  else                                    average = sock_p->ruc_sockCtrl_cumulatedCpuScheduler/sock_p->ruc_sockCtrl_nbTimesScheduler;
  
  pChar += rozofs_string_padded_append(pChar, 33, rozofs_left_alignment, "scheduler");
  pChar += rozofs_u32_padded_append(pChar,  4, rozofs_right_alignment, 0);
  pChar += rozofs_u64_padded_append(pChar, 11, rozofs_right_alignment, sock_p->ruc_sockCtrl_lastCpuScheduler);
  pChar += rozofs_u64_padded_append(pChar, 11, rozofs_right_alignment, sock_p->ruc_sockCtrl_cumulatedCpuScheduler);
  pChar += rozofs_u64_padded_append(pChar, 11, rozofs_right_alignment, sock_p->ruc_sockCtrl_nbTimesScheduler);
  pChar += rozofs_u64_padded_append(pChar, 11, rozofs_right_alignment, average);
  pChar += rozofs_u64_padded_append(pChar,  5, rozofs_right_alignment, p->priority);
  *pChar ++ = '\n';
  *pChar = 0;
  sock_p->ruc_sockCtrl_cumulatedCpuScheduler = 0;
  sock_p->ruc_sockCtrl_nbTimesScheduler = 0;

  uma_dbg_send(tcpRef,bufRef,TRUE,myBuf);

}
/*
**  END OF DEBUG
*/


/*
**______________________________________________________________________
*/

static void ruc_sockctl_updatePnextCur_th(rozofs_sockctrl_ctx_t *sock_p,ruc_obj_desc_t *pHead,
                                   ruc_sockObj_t *pobj)
{


   if (sock_p->ruc_sockctl_pnextCur == (ruc_obj_desc_t*)pobj)
   {
      /*
      ** sock_p->ruc_sockctl_pnextCur needs to be updated
      */
      ruc_objGetNext(pHead, &sock_p->ruc_sockctl_pnextCur);
     return;
   }
   
   if (sock_p->ruc_sockctl_poll_pnextCur == (ruc_obj_desc_t*)pobj)
   {
      /*
      ** sock_p->ruc_sockctl_pnextCur needs to be updated
      */
      ruc_objGetNext(pHead, &sock_p->ruc_sockctl_poll_pnextCur);
     return;
   }
     
}

/*
**____________________________________________________________
*/
int ruc_sockclt_gobal_init()
{

   
   memset(ruc_sockctl_ctx_tb,0,sizeof(rozofs_sockctrl_ctx_t*)*ROZOFS_MAX_SOCKCTL_DESCRIPTOR);
   
   ruc_sockctl_current_idx = 1;
   return 0;
}
/*
**____________________________________________________________
*/
/**
    ruc_sockctl_init_th(uint32_t maxConnection)
     
     creation of the socket controller distributor for multi-threaded case
 
 
        @param maxConnection : number of elements to create
 
    @retval: <> NULL pointer to the socket controller context :
    @retval NULL: error see errno for details 
*/

void *ruc_sockctl_init_th(uint32_t maxConnection,char *name)
{
  int i,idx;
  ruc_obj_desc_t  *pnext=(ruc_obj_desc_t*)NULL;
  ruc_sockObj_t  *p;
  rozofs_sockctrl_ctx_t *sock_p = malloc(sizeof(rozofs_sockctrl_ctx_t));
  
  if (ruc_sockctl_current_idx == ROZOFS_MAX_SOCKCTL_DESCRIPTOR) {
    errno = ENOMEM;
    return NULL;
  }
  
  if (sock_p == NULL)
  {
     return NULL;
  }
  memset(sock_p,0,sizeof(rozofs_sockctrl_ctx_t));
  sock_p->name = strdup(name);
  /*
  ** set the thread id
  */
  sock_p->thread_owner = pthread_self();
  /*
  ** Increase the file descriptor limit for the process
  */
{
   int ret;
   struct rlimit rlim;
   
   ret = getrlimit(RLIMIT_NOFILE,&rlim);
   if (ret < 0)
   {
      severe("getrlimit(RLIMIT_NOFILE) %s",strerror(errno));
      fprintf(stderr,"getrlimit(RLIMIT_NOFILE) %s\n",strerror(errno));
      exit(0);
   }
   rlim.rlim_cur = 4096;
   ret = setrlimit(RLIMIT_NOFILE,&rlim);
   if (ret < 0)
   {
      severe("setrlimit(RLIMIT_NOFILE,%d) %s",(int)rlim.rlim_cur, strerror(errno));
      fprintf(stderr,"setrlimit(RLIMIT_NOFILE) %s\n",strerror(errno));
      exit(0);
   }
}
  /*
  ** initialization of the priority table
  */
  for (i=0;i < RUC_SOCKCTL_MAXPRIO+1; i++)
  {
     ruc_listHdrInit(&sock_p->ruc_sockCtl_tabPrio[i]);
  }
  /*
  ** erase the Fd receive & xmit set
  */
  memset(&sock_p->rucRdFdSet,0,sizeof(sock_p->rucRdFdSet));
  memset(&sock_p->sockCtrl_speculative,0,sizeof(sock_p->sockCtrl_speculative));
  memset(&sock_p->rucWrFdSet,0,sizeof(sock_p->rucWrFdSet));   
  memset(&sock_p->rucRdFdSetUnconditional,0,sizeof(sock_p->rucRdFdSetUnconditional));   
  memset(&sock_p->rucWrFdSetCongested,0,sizeof(sock_p->rucWrFdSetCongested));   
  memset(sock_p->socket_recv_table,0xff,sizeof(int)*ROZO_FD_SETSIZE);
  memset(sock_p->socket_xmit_table,0xff,sizeof(int)*ROZO_FD_SETSIZE);
  memset(sock_p->socket_ctx_table,0,sizeof(ruc_sockObj_t *)*ROZO_FD_SETSIZE);
  memset(sock_p->socket_predictive_ctx_table,0,sizeof(ruc_sockObj_t *)*ROZO_FD_SETSIZE);
  memset(sock_p->socket_predictive_ctx_table_count,0,sizeof(int)*ROZO_FD_SETSIZE);
  sock_p->ruc_sockCtrl_max_poll_ctx = RUC_SOCKCTL_POLLCOUNT;
  sock_p->ruc_sockctl_poll_pnextCur = NULL;
  sock_p->ruc_sockCtrl_poll_period = RUC_SOCKCTL_POLLFREQ; /** period of 40 ms */
  memset(sock_p->ruc_sockCtrl_nr_socket_stats,0,sizeof(uint64_t)*ROZO_FD_SETSIZE);
  /*
  ** create the connection distributor
  */
  sock_p->ruc_sockCtl_freeListHead = 
              (ruc_sockObj_t *)ruc_listCreate(maxConnection,
                                              sizeof(ruc_sockObj_t));
  if (sock_p->ruc_sockCtl_freeListHead == (ruc_sockObj_t*)NULL)
  {
    /*
    ** out of memory
    */
    return NULL;
  }
  /*
  **  initialize each element of the free list
  */
  idx = 0;
  while ((p = (ruc_sockObj_t*)
              ruc_objGetNext((ruc_obj_desc_t*)sock_p->ruc_sockCtl_freeListHead,
                             &pnext))!=(ruc_sockObj_t*)NULL) 
  {
    p->connectId = idx;
    p->socketId = -1;
    p->speculative = 0;
    p->priority  = -1;
    // 64BITS     p->objRef = -1;
    p->objRef = NULL;
    p->rcvCount = 0;
    p->xmitCount = 0;
    p->name[0] = 0;
    p->lastTime = 0;
    p->cumulatedTime = 0;
    p->nbTimes = 0;
    p->lastTimeXmit = 0;
    p->cumulatedTimeXmit = 0;
    p->nbTimesXmit = 0;
    p->callBack = (ruc_sockCallBack_t*)NULL;
    idx +=1;

  }

  /*
  **  save the pointer to the first context of the list
  */
  sock_p->ruc_sockCtrl_pFirstCtx = (ruc_sockObj_t*)ruc_objGetFirst((ruc_obj_desc_t*)sock_p->ruc_sockCtl_freeListHead);

  /*
  ** do the connection with the debug
  */
//  ruc_sockCtrl_debug_init();
  
  sock_p->ruc_sockCtrl_maxConnection = maxConnection;

  /*
  ** save in the table
  */
  pthread_mutex_lock(&ruc_sockctl_thread_mutex);
  
  sock_p->module_idx = ruc_sockctl_current_idx;
  ruc_sockctl_ctx_tb[ruc_sockctl_current_idx++] =sock_p;

  pthread_mutex_unlock(&ruc_sockctl_thread_mutex);
  return sock_p;

}


/*
**___________________________________________________________
*/
/**
    creation of connection with the socket controller in multithreaded mode.
    if there is a free connection entry, it returns the pointer to the allocated context

     @param sock_p : pointer to the socket controller context
     @param socketId : socket identifier returned by the socket() service
     @param  priority : polling priority
     @param  objRef : object reference provided as a callback parameter
     @param *callBack : pointer to the call back functions.

       @retval !=NULL : connection identifier
      @retval ==NULL : out of context

*/
void * ruc_sockctl_connect_th(void *ctx_p,int socketId,
                           char *name,
                           uint32_t priority,
                           void *objRef,
                           ruc_sockCallBack_t *callback)
{

    ruc_sockObj_t *p,*pelem;
   rozofs_sockctrl_ctx_t *sock_p = (rozofs_sockctrl_ctx_t*)ctx_p;

  if (socketId >= ROZO_FD_SETSIZE) {
    fatal("ruc_sockctl_connect socketId out of range %d",socketId);
    return NULL;
  }
  /*
  ** update the max socket value if needed
  */
  if (sock_p->ruc_max_curr_socket < socketId) sock_p->ruc_max_curr_socket = socketId;

  /*
  ** get the first element from the free list
  */


  p = (ruc_sockObj_t*)sock_p->ruc_sockCtl_freeListHead;
  pelem = (ruc_sockObj_t*)ruc_objGetFirst((ruc_obj_desc_t*)p);
  if (pelem == (ruc_sockObj_t* )NULL)
  {
    // 64BITS     return (uint32_t) NULL;
    return NULL;
  }
  /*
  **  remove the context from the free list
  */
  ruc_objRemove((ruc_obj_desc_t*)pelem);
  /*
  **  store the callback pointer,socket Id and objRef
  */
  pelem->socketId = socketId;
  
  pelem->speculative = 0;
  pelem->objRef = objRef;
  pelem->rcvCount = 0;
  pelem->xmitCount = 0;
  bcopy((const char *)name, (char*)&pelem->name[0],RUC_SOCK_MAX_NAME);
  pelem->name[RUC_SOCK_MAX_NAME-1] = 0;
  pelem->callBack = callback;
  pelem->lastTime = 0;
  pelem->cumulatedTime = 0;
  pelem->nbTimes = 0; 
  pelem->lastTimeXmit = 0;
  pelem->cumulatedTimeXmit = 0;
  pelem->nbTimesXmit = 0;   
  /*
  **  insert in the associated priority list with priority is less than RUC_SOCKCTL_MAXPRIO
  **  --> only those socket are handled by the prepare xmit/receive function
  */
  if (priority < RUC_SOCKCTL_MAXPRIO) 
  {
    ruc_objInsert(&sock_p->ruc_sockCtl_tabPrio[priority],(ruc_obj_desc_t*)pelem);
  }
  else 
  {
    ruc_objInsert(&sock_p->ruc_sockCtl_tabPrio[RUC_SOCKCTL_MAXPRIO],(ruc_obj_desc_t*)pelem);
    FD_SET(pelem->socketId,&sock_p->rucRdFdSetUnconditional);        
  }
  pelem->priority  = priority;



  // 64BITS   return ((uint32_t)pelem);
  /*
  ** insert the context in the context table indexed by the socket_id
  */
  sock_p->socket_ctx_table[pelem->socketId] = pelem;
  /*
  ** set the socket ready for receiving by default
  */ 
  return (pelem);

}
/*
**_____________________________________________________________
*/
/**

    deletion of connection with the socket controller.
    
     @param sock_p : pointer to the socket controller context
     @param connectionId : reference returned by the connection service

    @retval RUC_OK on success
    @retval RUC_NOK on error
*/

uint32_t ruc_sockctl_disconnect_th(void *ctx_p, void * connectionId)

{
   ruc_sockObj_t *p;
   rozofs_sockctrl_ctx_t *sock_p = (rozofs_sockctrl_ctx_t*)ctx_p;
   
   p = (ruc_sockObj_t*)connectionId;

   /*
   ** update PnextCur before remove the object
   */
   if (p->priority >= RUC_SOCKCTL_MAXPRIO)
   {
      ruc_sockctl_updatePnextCur_th(sock_p,&sock_p->ruc_sockCtl_tabPrio[RUC_SOCKCTL_MAXPRIO],p);
   }
   else
   {
      ruc_sockctl_updatePnextCur_th(sock_p,&sock_p->ruc_sockCtl_tabPrio[p->priority],p);
   }

   /*
   **  remove from the priority list
   */
   ruc_objRemove((ruc_obj_desc_t *)p);

   /*
   **  set it free
   */
   p->objRef = (void*)-1;
   if (p->socketId != -1)
   {
     /*
     ** clear the correspond bit on xmit and rcv ready
     */
     FD_CLR(p->socketId,&sock_p->rucRdFdSet);     
     FD_CLR(p->socketId,&sock_p->sockCtrl_speculative);     
     FD_CLR(p->socketId,&sock_p->rucWrFdSet);
     FD_CLR(p->socketId,&sock_p->rucRdFdSetUnconditional);
     FD_CLR(p->socketId,&sock_p->rucWrFdSetCongested);

     ruc_sockCtrl_remove_socket(sock_p->socket_recv_table,sock_p->socket_recv_count,p->socketId);
     ruc_sockCtrl_remove_socket(sock_p->socket_xmit_table,sock_p->socket_xmit_count,p->socketId);
     sock_p->socket_ctx_table[p->socketId] = NULL;
     sock_p->socket_predictive_ctx_table[p->socketId] = NULL;
     sock_p->socket_predictive_ctx_table_count[p->socketId] = 0; 
     p->socketId = -1;
   }

   /*
   **  insert in the free list
   */
  ruc_objInsert((ruc_obj_desc_t*)sock_p->ruc_sockCtl_freeListHead,
                (ruc_obj_desc_t*)p);

  return RUC_OK;
}

/*
**____________________________________________________________________________
*/
static inline void ruc_sockCtl_checkRcvAndXmitBits_opt_th(rozofs_sockctrl_ctx_t *sock_p,int nbrSelect)
{

  int i;
  ruc_sockObj_t *p;
  ruc_sockCallBack_t *pcallBack;
  int socketId;
  int speculative_count = 0;
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
#if APP_POLLING_OPT
  uint64_t  ruc_applicative_poller_ticker = rozofs_get_ticker_us();
#endif  
  timeBefore = 0;
  timeAfter  = 0;

  uint64_t cycles_before,cycles_after;
  cycles_before = rdtsc();
#if APP_POLLING_OPT
  if (sock_p->ruc_applicative_poller != NULL)
  {
	sock_p->ruc_applicative_poller_count++;
	uint64_t cycles_start = rdtsc();  	
	(*sock_p->ruc_applicative_poller)(0);
        sock_p->ruc_applicative_poller_cycles += (rdtsc() - cycles_start);
  }
  ruc_applicative_poller_ticker +=1;
#endif
  cycles_before = rdtsc();
  /*
  ** build the table for the receive and xmit sides
  */
  sock_p->socket_recv_count = ruc_sockCtrl_build_sock_table_th(sock_p,(uint64_t *)&sock_p->rucRdFdSet,sock_p->socket_recv_table,nbrSelect);
  sock_p->socket_xmit_count = ruc_sockCtrl_build_sock_table_th(sock_p,(uint64_t *)&sock_p->rucWrFdSet,sock_p->socket_xmit_table,nbrSelect);
  /*
  ** case of the speculative scheduler
  */
  if (sock_p->ruc_sockCtrl_speculative_sched_enable)
  {
    speculative_count= sock_p->ruc_sockCtrl_speculative_count;
    if (speculative_count > 0)
    {
      speculative_count = ruc_sockCtrl_build_sock_table_th(sock_p,(uint64_t *)&sock_p->sockCtrl_speculative,sock_p->socket_speculative_table,speculative_count);
    }
    if (sock_p->ruc_sockCtrl_max_speculative < speculative_count) sock_p->ruc_sockCtrl_max_speculative = speculative_count;

  }
  cycles_after = rdtsc();
  sock_p->ruc_time_receive += (cycles_after - cycles_before);
  sock_p->ruc_count_receive++;

  for (i = 0; i <sock_p->socket_recv_count ; i++)
  {
    socketId = sock_p->socket_recv_table[i];
    if (socketId == -1) continue;
#if 0
    /*
    ** check the traffic shaper
    */
    if (sock_p->ruc_applicative_traffic_shaper != NULL)
    {
     (*sock_p->ruc_applicative_traffic_shaper)(timeAfter);
    }
#endif
    p = sock_p->socket_ctx_table[socketId];
    if (p == NULL) 
    {
      continue;
    }
    /*
    ** call the associated callback
    */
    p->rcvCount++;
    pcallBack = p->callBack;
#ifdef ROZO_MES
    gettimeofday(&timeDay,(struct timezone *)0);  
    timeBefore = MICROLONG(timeDay);
//    timeBefore = rozofs_get_ticker_us();
#endif
    (*(pcallBack->msgInFunc))(p->objRef,p->socketId);
#ifdef ROZO_MES
    gettimeofday(&timeDay,(struct timezone *)0);  
    timeAfter = MICROLONG(timeDay);
//    timeAfter = rozofs_get_ticker_us();
    p->lastTime = (uint32_t)(timeAfter - timeBefore);
    p->cumulatedTime += p->lastTime;
    p->nbTimes ++;        
#endif
#if APP_POLLING_OPT
    if (sock_p->ruc_applicative_poller != NULL)
    {
        if (ruc_applicative_poller_ticker < timeAfter)
	{
	  sock_p->ruc_applicative_poller_count++;
	  uint64_t cycles_start = rdtsc();  	
	  (*sock_p->ruc_applicative_poller)(0);
          sock_p->ruc_applicative_poller_cycles += (rdtsc() - cycles_start);  
	  ruc_applicative_poller_ticker = timeAfter+1;
	}

    }
#endif
  }
  /*
  ** speculative scheduler
  */
  for (i = 0; i <speculative_count ; i++)
  {
    socketId = sock_p->socket_speculative_table[i];
    if (socketId == -1) continue;
    p = sock_p->socket_predictive_ctx_table[socketId];
    if (p == NULL) 
    {
      continue;
    }
    /*
    ** call the associated callback
    */
    p->rcvCount++;
    pcallBack = p->callBack;

    gettimeofday(&timeDay,(struct timezone *)0);  
    timeBefore = MICROLONG(timeDay);
//    timeBefore = rozofs_get_ticker_us();

    (*(pcallBack->msgInFunc))(p->objRef,p->socketId);

    gettimeofday(&timeDay,(struct timezone *)0);  
    timeAfter = MICROLONG(timeDay);
//    timeAfter = rozofs_get_ticker_us();
    p->lastTime = (uint32_t)(timeAfter - timeBefore);
    p->cumulatedTime += p->lastTime;
    p->nbTimes ++;         
  }

  for (i = 0; i <sock_p->socket_xmit_count ; i++)
  {
    socketId = sock_p->socket_xmit_table[i];
    if ( socketId == -1) continue;
    p = sock_p->socket_ctx_table[  sock_p->socket_xmit_table[i]];
    if (p == NULL) 
    {
      FD_CLR(socketId,&sock_p->rucWrFdSet);
      continue;
    }
    FD_CLR(socketId,&sock_p->rucWrFdSet);
    p->xmitCount++;
    pcallBack = p->callBack;
#ifdef ROZO_MES
    gettimeofday(&timeDay,(struct timezone *)0);  
    timeBefore = MICROLONG(timeDay);
//    timeBefore = rozofs_get_ticker_us();
#endif
    (*(pcallBack->xmitEvtFunc))(p->objRef,p->socketId);

#ifdef ROZO_MES
    gettimeofday(&timeDay,(struct timezone *)0);  
    timeAfter = MICROLONG(timeDay);
//    timeAfter = rozofs_get_ticker_us();
    p->lastTime = (uint32_t)(timeAfter - timeBefore);
    p->cumulatedTime += p->lastTime;
    p->nbTimes ++;
#endif
  }


}
/*
**____________________________________________________________________________
*/
static inline void ruc_sockCtl_prepareRcvAndXmitBits_th(rozofs_sockctrl_ctx_t *sock_p)
{

  int i;
  ruc_sockObj_t *p;
  uint32_t ret;
  int polling_cnt = 2;
  
  uint64_t time_before,time_after;
  sock_p->ruc_sockCtrl_nb_socket_conditional = 0;

  time_before = rdtsc();
  /*
  ** copy the bitmap of the sockets for which the receive is unconditional
  */
  memcpy(&sock_p->rucRdFdSet,&sock_p->rucRdFdSetUnconditional,sizeof(sock_p->rucRdFdSet));
  memcpy(&sock_p->rucWrFdSet,&sock_p->rucWrFdSetCongested,sizeof(sock_p->rucWrFdSet));

  for (i = 0; i <RUC_SOCKCTL_MAXPRIO ; i++)
  {
    sock_p->ruc_sockctl_pnextCur = (ruc_obj_desc_t*)NULL;
    sock_p->ruc_sockctl_prioIdxCur = RUC_SOCKCTL_MAXPRIO-1-i;

    while ((p = (ruc_sockObj_t*)
              ruc_objGetNext((ruc_obj_desc_t*)&sock_p->ruc_sockCtl_tabPrio[RUC_SOCKCTL_MAXPRIO-1-i],
                             &sock_p->ruc_sockctl_pnextCur))!=(ruc_sockObj_t*)NULL) 
    {
      sock_p->ruc_sockCtrl_nb_socket_conditional++;
#if APP_POLLING
      if ((polling_cnt!=0)&& (sock_p->ruc_applicative_poller != NULL))
      {
        polling_cnt -=1;
	sock_p->ruc_applicative_poller_count++;
	uint64_t cycles_start = rdtsc();  	
	(*sock_p->ruc_applicative_poller)(0);
        sock_p->ruc_applicative_poller_cycles += (rdtsc() - cycles_start);
      }
#endif
      FD_CLR(p->socketId,&sock_p->rucWrFdSet);
      FD_CLR(p->socketId,&sock_p->rucRdFdSet);
      ret = (*((p->callBack)->isRcvReadyFunc))(p->objRef,p->socketId);
      if(ret == TRUE)
      {
        /*
        ** The receiver is ready, assert the corresponding bit
        */
#if 0
      printf("prepareRcvBits :socketId %d, name: %s\n",p->socketId,&p->name[0]);
#endif

        FD_SET(p->socketId,&sock_p->rucRdFdSet);
      }
      ret = (*((p->callBack)->isXmitReadyFunc))(p->objRef,p->socketId);
      if(ret == TRUE)
      {
        /*
        ** The receiver is ready, assert the corresponding bit
        */
        FD_SET(p->socketId,&sock_p->rucWrFdSet);
      }
    }
  }
  time_after = rdtsc();
  sock_p->ruc_time_prepare += (time_after - time_before);
  sock_p->ruc_count_prepare++;
  
}
/*
**____________________________________________________________
*/
/**
    ruc_sockCtrl_attach_applicative_poller
     
    attach an applicative poller function to the socket controller
       
    @param ctx_p : pointer to the socket controller context
 
    @retval:none
*/


void ruc_sockCtrl_attach_applicative_poller_th(void *ctx_p,ruc_scheduler_t callback)
{
   rozofs_sockctrl_ctx_t *sock_p = (rozofs_sockctrl_ctx_t*)ctx_p;
   sock_p->ruc_applicative_poller = callback;
}

/*
**____________________________________________________________________________
*/
/**
*  Main loop
*/
void ruc_sockCtrl_selectWait_th(void *ctx)
{
    int     nbrSelect;    /* nbr of events detected by select function */
    struct timeval     timeDay;
    unsigned long long timeBefore, timeAfter;
    unsigned long long looptimeEnd,looptimeStart = 0;   
     timeBefore = 0;
     timeAfter  = 0;

    rozofs_sockctrl_ctx_t *sock_p = (rozofs_sockctrl_ctx_t*)ctx;
    /*
    ** update time before call select
    */
    gettimeofday(&timeDay,(struct timezone *)0);  
    timeBefore = MICROLONG(timeDay);
//    timeBefore = rozofs_get_ticker_us();

    while (1)
    {
      /*
      **  compute sock_p->rucRdFdSet and sock_p->FdSet
      */
      ruc_sockCtl_prepareRcvAndXmitBits_th(sock_p);
      
      gettimeofday(&timeDay,(struct timezone *)0);     
      looptimeEnd = MICROLONG(timeDay);  
//      looptimeEnd = rozofs_get_ticker_us();  
      sock_p->ruc_sockCtrl_looptime= (uint32_t)(looptimeEnd - looptimeStart); 
      if (sock_p->ruc_sockCtrl_looptime > sock_p->ruc_sockCtrl_looptimeMax)
      {
	  sock_p->ruc_sockCtrl_looptimeMax = sock_p->ruc_sockCtrl_looptime;
      }	  
      /*
      ** wait for event 
      */	  
      if((nbrSelect=select(sock_p->ruc_max_curr_socket+1,(fd_set *)&sock_p->rucRdFdSet,
                                                 (fd_set *)&sock_p->rucWrFdSet,NULL, NULL)) == 0)
      {
	/*
	** udpate time after select
	*/ 
	gettimeofday(&timeDay,(struct timezone *)0);  
	timeAfter = MICROLONG(timeDay); 
	//timeAfter = rozofs_get_ticker_us(); 
	looptimeStart  = timeAfter;
      }
      else
      {
      
        if (nbrSelect < 0) {
         if (errno == EINTR) {
           //RUC_WARNING(errno);
           continue;
         }
         
         RUC_WARNING(errno);
         return;
       }       
      	gettimeofday(&timeDay,(struct timezone *)0);  
	looptimeStart = MICROLONG(timeDay); 
//	looptimeStart = rozofs_get_ticker_us(); 
	if (sock_p->ruc_sockCtrl_max_nr_select < nbrSelect) sock_p->ruc_sockCtrl_max_nr_select = nbrSelect;
        sock_p->ruc_sockCtrl_nr_socket_stats[nbrSelect]++;
	
	ruc_sockCtl_checkRcvAndXmitBits_opt_th(sock_p,nbrSelect);
	/*
	**  insert the first element of each priority list at the
	**  tail of its priority list.
	*/
        gettimeofday(&timeDay,(struct timezone *)0);  
	timeAfter = MICROLONG(timeDay); 
//	timeAfter = rozofs_get_ticker_us(); 
      }
      /*
      ** socket polling (former receive ready callback)
      */
      if (timeAfter > (sock_p->ruc_sockCtrl_lastTimeScheduler+sock_p->ruc_sockCtrl_poll_period))
      {
	timeBefore = timeAfter;
	sock_p->ruc_sockCtrl_lastTimeScheduler = timeAfter;
	ruc_sockCtrl_socket_poll_th(sock_p);
	gettimeofday(&timeDay,(struct timezone *)0);  
	timeAfter = MICROLONG(timeDay);
//	timeAfter = rozofs_get_ticker_us();
	sock_p->ruc_sockCtrl_lastCpuScheduler = (uint32_t)(timeAfter - timeBefore);
	sock_p->ruc_sockCtrl_cumulatedCpuScheduler += sock_p->ruc_sockCtrl_lastCpuScheduler;
	sock_p->ruc_sockCtrl_nbTimesScheduler ++;     
      }   
    }
}

/**
* clear the associated fd bit in the fdset

  @param int fd : file descriptor to clear
*/
void ruc_sockCtrl_clear_rcv_bit_th(void *ctx,int fd)
{
    rozofs_sockctrl_ctx_t *sock_p = (rozofs_sockctrl_ctx_t*)ctx;
  if (fd < 0) return;
  FD_CLR(fd,&sock_p->rucRdFdSet);
}


/*
**________________________________________
*/
/**
*  Begin of congestion

    @param ctx_p: socket controller context
    @param int socket : file descriptor to set
*/
void ruc_sockctl_congested_th(void *ctx_p,int socket)
{
    rozofs_sockctrl_ctx_t *sock_p = (rozofs_sockctrl_ctx_t*)ctx_p;
    FD_SET(socket,&sock_p->rucWrFdSetCongested);
}

/*
**________________________________________
*/
/**
*  End of congestion

    @param ctx_p: socket controller context
    @param int socket : file descriptor to set
*/

void ruc_sockctl_eoc_th(void *ctx_p,int socket)
{
    rozofs_sockctrl_ctx_t *sock_p = (rozofs_sockctrl_ctx_t*)ctx_p;    
    FD_CLR(socket,&sock_p->rucWrFdSetCongested);
}
/**
*   Get the pointer to the socket controller based on the threadID
*/
void *ruc_sockctl_get_ctx_th()
{
  int i;
  int threadId =pthread_self();
  for (i=0; i< ruc_sockctl_current_idx; i++)
  {
    if (ruc_sockctl_ctx_tb[i] == NULL) continue;
    if (ruc_sockctl_ctx_tb[i]->thread_owner == threadId) return (void*)ruc_sockctl_ctx_tb[i];  
  }
  return NULL;
}


/**
*   Get the module index of the socket controller
*/
int ruc_sockctl_get_module_idx_th(void *p)
{
   rozofs_sockctrl_ctx_t *sock_p = (rozofs_sockctrl_ctx_t*)p;
   return (int) sock_p->module_idx;
}


/**
*   Get the module name of the socket controller
*/
char *ruc_sockctl_get_module_name_th(void *p)
{
   rozofs_sockctrl_ctx_t *sock_p = (rozofs_sockctrl_ctx_t*)p;
   return  sock_p->name;
}


/**
*   Get the module index of the socket controller associated to a thread
*/
int ruc_sockctl_get_thread_module_idx_th()
{

  int i;
  int threadId =pthread_self();
  for (i=0; i< ruc_sockctl_current_idx; i++)
  {
    if (ruc_sockctl_ctx_tb[i] == NULL) continue;
    if (ruc_sockctl_ctx_tb[i]->thread_owner == threadId) return (int)ruc_sockctl_ctx_tb[i]->module_idx;  
  }
  return -1;
}
