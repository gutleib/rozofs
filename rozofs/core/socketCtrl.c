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

#include "ruc_list.h"
#include "socketCtrl.h"
#include "uma_dbg_api.h"
#include "af_unix_socket_generic_api.h"
#include "af_unix_socket_generic.h"
#include "north_lbg.h"
#include "rozofs_string.h"

#define MICROLONG(time) ((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)
#define RUC_SOCKCTRL_DEBUG_TOPIC      "cpu"
#define RUC_SOCKCTRL_CTX_TOPIC        "ctx_size"

#define APP_POLLING 1
#define APP_POLLING_OPT 1
#define ROZO_MES 1


/*
**  G L O B A L   D A T A
*/

/*
**  priority table
*/
ruc_obj_desc_t  ruc_sockCtl_tabPrio[RUC_SOCKCTL_MAXPRIO+1];
int ruc_sockCtrl_speculative_sched_enable = 0;
int ruc_sockCtrl_speculative_count = 0;
int ruc_max_curr_socket = 0;
/*
** head of the free connection list
*/
ruc_sockObj_t   *ruc_sockCtl_freeListHead= (ruc_sockObj_t*)NULL;
ruc_sockObj_t   *ruc_sockCtrl_pFirstCtx = (ruc_sockObj_t*)NULL;
uint32_t          ruc_sockCtrl_maxConnection = 0;
uint64_t         af_unix_rcv_buffered =0;
/*
** file descriptor for receiving and transmitting events
*/
rozo_fd_set  sockCtrl_speculative;   
rozo_fd_set  rucRdFdSet;   
rozo_fd_set  rucWrFdSet;   
rozo_fd_set  rucRdFdSetUnconditional;
rozo_fd_set  rucWrFdSetCongested;

/*
**  gloabl data used in the loops that polls the bitfields
*/

ruc_obj_desc_t *ruc_sockctl_pnextCur;

int   ruc_sockctl_prioIdxCur;
uint64_t  ruc_time_prepare  = 0;
uint64_t  ruc_count_prepare = 0;
uint64_t  ruc_time_receive  = 0;
uint64_t  ruc_count_receive = 0;
uint32_t  ruc_sockCtrl_nb_socket_conditional= 0;
uint64_t gettimeofday_count = 0;
uint64_t gettimeofday_cycles = 0;
int ruc_sockCtrl_max_nr_select = 0;
int ruc_sockCtrl_max_speculative = 0;


uint32_t ruc_sockCtrl_lastCpuScheduler = 0;
uint32_t ruc_sockCtrl_cumulatedCpuScheduler = 0;
uint32_t ruc_sockCtrl_nbTimesScheduler = 0;
uint64_t ruc_sockCtrl_lastTimeScheduler = 0;
uint32_t ruc_sockCtrl_looptime = 0;
uint32_t ruc_sockCtrl_looptimeMax = 0;

ruc_scheduler_t ruc_applicative_traffic_shaper = NULL;
ruc_scheduler_t ruc_applicative_poller = NULL;
uint64_t ruc_applicative_poller_cycles = 0;
uint64_t ruc_applicative_poller_count = 0;
/*
** table used for storing the index of the socket for which there is the associated bit asserted
*/
int socket_recv_count = 0;
int socket_recv_table[ROZO_FD_SETSIZE];
int socket_speculative_table[ROZO_FD_SETSIZE];
int socket_xmit_count = 0;
int socket_xmit_table[ROZO_FD_SETSIZE];
ruc_sockObj_t *socket_ctx_table[ROZO_FD_SETSIZE];
ruc_sockObj_t *socket_predictive_ctx_table[ROZO_FD_SETSIZE];
int socket_predictive_ctx_table_count[ROZO_FD_SETSIZE];
int ruc_sockCtrl_max_poll_ctx = 0;
ruc_obj_desc_t *ruc_sockctl_poll_pnextCur;
uint64_t ruc_sockCtrl_poll_period = 0;   /**< period in microseconds */ 
uint64_t ruc_sockCtrl_nr_socket_stats[ROZO_FD_SETSIZE];


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

void ruc_sockCtrl_socket_poll()
{
  ruc_sockObj_t *p;
  int count = 0;
  

    while ((p = (ruc_sockObj_t*)
              ruc_objGetNext((ruc_obj_desc_t*)&ruc_sockCtl_tabPrio[RUC_SOCKCTL_MAXPRIO],
                             &ruc_sockctl_poll_pnextCur))!=(ruc_sockObj_t*)NULL) 
   {
      (*((p->callBack)->isRcvReadyFunc))(p->objRef,p->socketId); 
      count++; 
      if (count == ruc_sockCtrl_max_poll_ctx) break;
   }
   if (p == NULL) ruc_sockctl_poll_pnextCur = (ruc_obj_desc_t*)NULL;
}
/*
**____________________________________________________________________________
*/
/**
*  the purpose of that function is to return the list of the socket for which 
   there is a pending event (read or write)
  
   @param fdset_p : pointer to the fd set that contains the asserted bits
   @param socket_tab_p : pointer to the array that will contain the sockets for which there is an event
   @param nb_sockets : number of bits asserted in the set
   
   @retval number of sockets for which an event is asserted 
*/
static inline int ruc_sockCtrl_build_sock_table(uint64_t *fdset_p,int *socket_tab_p,int nb_sockets)
{
   int i;
   uint32_t val32;
   uint64_t val64;
   uint32_t bit;
   int curr_socket_id;
   int socket_count = 0;
   int last_sock;
   
   curr_socket_id = 0;
   
   last_sock = (ruc_max_curr_socket+1)/64;
   if ((ruc_max_curr_socket+1)%64) last_sock=last_sock+1;
   
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
void ruc_sockCtrl_remove_socket(int *sock_table_p,int nb_sockets,int socket_id)
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

/*__________________________________________________________________________
  Display of the context size of the various object
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void ruc_sockCtrl_ctx_show(uint32_t tcpRef, void * bufRef) {
  char           *pChar=myBuf;

  pChar += sprintf(pChar,"ruc_sockObj_t         : %lu bytes \n",(long unsigned int)sizeof(ruc_sockObj_t));
  pChar += sprintf(pChar,"af_unix_ctx_generic_t : %lu bytes \n",(long unsigned int)sizeof(af_unix_ctx_generic_t));
  pChar += sprintf(pChar,"north_lbg_ctx_t       : %lu bytes \n",(long unsigned int)sizeof(north_lbg_ctx_t));
  uma_dbg_send(tcpRef,bufRef,TRUE,myBuf);

}
/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void ruc_sockCtrl_debug_man(char * pt) {
  pt += sprintf(pt,"Display the socket controller statistics.\n");
  pt += sprintf(pt,"This provides for each listening socket the following information:\n");
  pt += sprintf(pt,"- socket name.\n");
  pt += sprintf(pt,"- linux socket id.\n");
  pt += sprintf(pt,"- cpu time consumed on last call.\n");
  pt += sprintf(pt,"- total time consumed.\n");
  pt += sprintf(pt,"- total number of call.\n");
  pt += sprintf(pt,"- average cpu time consumed.\n");
  pt += sprintf(pt,"- priority\n");
  pt += sprintf(pt,"The time is given in micro seconds.\n");  
  pt += sprintf(pt,"The counters are reset on each call to cpu command.\n");
}

void ruc_sockCtrl_debug_show(uint32_t tcpRef, void * bufRef) {
  ruc_sockObj_t     *p;
  int                i;
  char           *pChar=myBuf;
  uint32_t          average;

  p = ruc_sockCtrl_pFirstCtx;
  pChar += sprintf(pChar,"speculative scheduler    :%s\n",(ruc_sockCtrl_speculative_sched_enable==0)?" Disabled":" Enabled");
  pChar += rozofs_string_append(pChar,"conditional sockets      : ");
  pChar += rozofs_u32_append(pChar ,ruc_sockCtrl_max_nr_select);
  *pChar++ = '\n';

  pChar += rozofs_string_append(pChar,"max socket events        : ");
  pChar += rozofs_u32_append(pChar ,ruc_sockCtrl_max_nr_select);
  *pChar++ = '\n';
  pChar += rozofs_string_append(pChar,"max speculative events   : ");
  pChar += rozofs_u32_append(pChar ,ruc_sockCtrl_max_speculative);
  *pChar++ = '(';
  pChar += rozofs_u32_append(pChar ,ruc_sockCtrl_speculative_count);
  *pChar++ = ')';
  *pChar++ = '\n';
  ruc_sockCtrl_max_nr_select = 0;
  ruc_sockCtrl_max_speculative = 0;

  pChar += rozofs_string_append(pChar,"xmit/recv prepare cycles : ");
  pChar += rozofs_u64_append(pChar ,(ruc_count_prepare==0)?0:(long long unsigned)ruc_time_prepare/ruc_count_prepare);
  pChar += rozofs_string_append(pChar," cycles [");
  pChar += rozofs_u64_append(pChar ,ruc_time_prepare);
  *pChar++ = '/';
  pChar += rozofs_u64_append(pChar ,ruc_count_prepare);
  *pChar++ = ']';  
  *pChar++ = '\n';
  ruc_time_prepare = 0;
  ruc_count_prepare = 0;
  pChar += rozofs_string_append(pChar,"max recv buffered evts   : ");
  pChar += rozofs_u64_append(pChar ,(long long unsigned) af_unix_rcv_buffered);
  *pChar++ = '\n';
  af_unix_rcv_buffered = 0;  

  pChar += rozofs_string_append(pChar,"xmit/recv receive cycles : ");
  pChar += rozofs_u64_append(pChar ,(ruc_count_receive==0)?0:(long long unsigned)ruc_time_receive/ruc_count_receive);
  pChar += rozofs_string_append(pChar," cycles [");
  pChar += rozofs_u64_append(pChar ,ruc_time_receive);
  *pChar++ = '/';
  pChar += rozofs_u64_append(pChar ,ruc_count_receive);
  *pChar++ = ']';  
  *pChar++ = '\n';
  ruc_time_receive = 0;
  ruc_count_receive = 0;							       

  pChar += rozofs_string_append(pChar,"gettimeofday cycles      : ");
  pChar += rozofs_u64_append(pChar ,(gettimeofday_count==0)?0:(long long unsigned)gettimeofday_cycles/gettimeofday_count);
  pChar += rozofs_string_append(pChar," cycles [");
  pChar += rozofs_u64_append(pChar ,gettimeofday_cycles);
  *pChar++ = '/';
  pChar += rozofs_u64_append(pChar ,gettimeofday_count);
  *pChar++ = ']';  
  *pChar++ = '\n';
  gettimeofday_cycles = 0;
  gettimeofday_count  = 0;	

  pChar += rozofs_string_append(pChar,"application poll cycles  : ");
  pChar += rozofs_u64_append(pChar ,(ruc_applicative_poller_count==0)?0:(long long unsigned)ruc_applicative_poller_cycles/ruc_applicative_poller_count);
  pChar += rozofs_string_append(pChar," cycles [");
  pChar += rozofs_u64_append(pChar ,ruc_applicative_poller_cycles);
  *pChar++ = '/';
  pChar += rozofs_u64_append(pChar ,ruc_applicative_poller_count);
  *pChar++ = ']';  
  *pChar++ = '\n';
  ruc_applicative_poller_cycles = 0;
  ruc_applicative_poller_count  = 0;	
  
  pChar += rozofs_string_append(pChar,"rucRdFdSet ");
  pChar += rozofs_x64_append(pChar,(uint64_t)&rucRdFdSet);
  pChar += rozofs_string_append(pChar," (");
  pChar += rozofs_u64_append(pChar,sizeof(rucRdFdSet));  
  pChar += rozofs_string_append(pChar,") __FD_SETSIZE :"); 
  pChar += rozofs_u32_append(pChar,ROZO_FD_SETSIZE);
  pChar += rozofs_string_append(pChar," __NFDBITS :"); 
  pChar += rozofs_u32_append(pChar,__NFDBITS);
  *pChar++ = '\n';
    
  pChar += rozofs_string_append(pChar,"select max cpu time : ");
  pChar += rozofs_u32_append(pChar,ruc_sockCtrl_looptimeMax);
  pChar += rozofs_string_append(pChar," us\n");   
  ruc_sockCtrl_looptimeMax = 0;   

  pChar += rozofs_string_append(pChar,"\napplication                      sock       last  cumulated activation    average\n");
  pChar += rozofs_string_append(pChar,"name                               nb        cpu        cpu      times        cpu  prio\n\n");
    
  for (i = 0; i < ruc_sockCtrl_maxConnection; i++)
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
      pChar += rozofs_u32_append(pChar, (FD_ISSET(p->socketId, &rucRdFdSetUnconditional)==0)?0:1);
      *pChar++ = '-';      
      pChar += rozofs_u32_append(pChar, (FD_ISSET(p->socketId, &rucWrFdSetCongested)==0)?0:1);  
      *pChar++ = '-';      
      pChar += rozofs_u32_append(pChar, (p->speculative==0)?0:1);  
      *pChar++ = '-';      
      pChar += rozofs_u32_append(pChar, (FD_ISSET(p->socketId, &sockCtrl_speculative)==0)?0:1);  
      *pChar++ = '-';      
      pChar += rozofs_u32_append(pChar, socket_predictive_ctx_table_count[p->socketId]);  

      *pChar ++ = '\n';
                
      p->cumulatedTime = 0;
      p->nbTimes = 0;
    }
    p++;
  }

  if (ruc_sockCtrl_nbTimesScheduler == 0) average = 0;
  else                                    average = ruc_sockCtrl_cumulatedCpuScheduler/ruc_sockCtrl_nbTimesScheduler;
  
  pChar += rozofs_string_padded_append(pChar, 33, rozofs_left_alignment, "scheduler");
  pChar += rozofs_u32_padded_append(pChar,  4, rozofs_right_alignment, 0);
  pChar += rozofs_u64_padded_append(pChar, 11, rozofs_right_alignment, ruc_sockCtrl_lastCpuScheduler);
  pChar += rozofs_u64_padded_append(pChar, 11, rozofs_right_alignment, ruc_sockCtrl_cumulatedCpuScheduler);
  pChar += rozofs_u64_padded_append(pChar, 11, rozofs_right_alignment, ruc_sockCtrl_nbTimesScheduler);
  pChar += rozofs_u64_padded_append(pChar, 11, rozofs_right_alignment, average);
  pChar += rozofs_u64_padded_append(pChar,  5, rozofs_right_alignment, p->priority);
  *pChar ++ = '\n';
  *pChar = 0;
  ruc_sockCtrl_cumulatedCpuScheduler = 0;
  ruc_sockCtrl_nbTimesScheduler = 0;

  uma_dbg_send(tcpRef,bufRef,TRUE,myBuf);

}
/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void ruc_sockCtrl_debug(char * argv[], uint32_t tcpRef, void * bufRef) {
  ruc_sockCtrl_debug_show(tcpRef,bufRef);
}
/*__________________________________________________________________________
*/

void ruc_sockCtrl_ctx(char * argv[], uint32_t tcpRef, void * bufRef) {
  ruc_sockCtrl_ctx_show(tcpRef,bufRef);
}
/*__________________________________________________________________________
*/
static char * show_ruc_sockCtrl_conf_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"sockctrl pollcount <value> : set number of polled contexts per activation\n");
  pChar += sprintf(pChar,"sockctrl pollfreq <value>  : set activation period (unit is microseconds)\n");
  pChar += sprintf(pChar,"sockctrl speculative <enable|disable>  : set/reset speculative scheduler\n");
  pChar += sprintf(pChar,"sockctrl                   : display current configuration\n");
  return pChar; 
}  

void ruc_sockCtrl_conf(char * argv[], uint32_t tcpRef, void * bufRef) {
  char           *pChar=myBuf;
  int val = 0;  
  
  if (argv[1] != NULL)
  {
    if (strcmp(argv[1],"pollcount")==0) 
    {
      errno = 0;       
      val = (int) strtol(argv[2], (char **) NULL, 10);   
      if (errno != 0) {
       pChar += sprintf(pChar,"bad value %s\n",argv[2]);    
       pChar = show_ruc_sockCtrl_conf_help(pChar);
       uma_dbg_send(tcpRef, bufRef, TRUE, myBuf);
       return;     
      }
      ruc_sockCtrl_max_poll_ctx = val;
      uma_dbg_send(tcpRef, bufRef, TRUE, "Done\n");    
      return;      
    }
    if (strcmp(argv[1],"speculative")==0) 
    {
      if (argv[2] == NULL)
      {
       pChar += sprintf(pChar,"missing parameter\n");    
       pChar = show_ruc_sockCtrl_conf_help(pChar);
       uma_dbg_send(tcpRef, bufRef, TRUE, myBuf);
       return;           
      } 
      if (strcmp(argv[2],"enable")==0)  
      {
         ruc_sockCtrl_speculative_sched_enable = 1;
         uma_dbg_send(tcpRef, bufRef, TRUE, "Speculative scheduler is now enabled\n");  
	 return;        
      }     
      if (strcmp(argv[2],"disable")==0)  
      {
         ruc_sockCtrl_speculative_sched_enable = 0;
         uma_dbg_send(tcpRef, bufRef, TRUE, "Speculative scheduler is now disabled\n");  
	 return;        
      }  

      pChar += sprintf(pChar,"bad value %s\n",argv[2]);    
      pChar = show_ruc_sockCtrl_conf_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, myBuf);
      return;        
    }
    if (strcmp(argv[1],"pollfreq")==0) 
    {
      errno = 0;       
      val = (int) strtol(argv[2], (char **) NULL, 10);   
      if (errno != 0) {
       pChar += sprintf(pChar,"bad value %s\n",argv[2]);    
       pChar = show_ruc_sockCtrl_conf_help(pChar);
       uma_dbg_send(tcpRef, bufRef, TRUE, myBuf);
       return;     
      }
      ruc_sockCtrl_poll_period = (uint64_t) val;       
      uma_dbg_send(tcpRef, bufRef, TRUE, "Done\n");    
      return;      
    }
    pChar = show_ruc_sockCtrl_conf_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, myBuf);
    return;
  }
  pChar +=sprintf(pChar,"speculative scheduler                    : %s\n",
          (ruc_sockCtrl_speculative_sched_enable==1)?" Enable":" Disable");
  pChar +=sprintf(pChar,"max number of socket controller contexts : %u\n",ruc_sockCtrl_maxConnection);
  pChar +=sprintf(pChar,"last socket index                        : %u\n",ruc_max_curr_socket);
  pChar +=sprintf(pChar,"scheduler polling period                 : %llu us\n",(long long unsigned)ruc_sockCtrl_poll_period);
  pChar +=sprintf(pChar,"scheduler polling count                  : %u\n",ruc_sockCtrl_max_poll_ctx);
  uma_dbg_send(tcpRef, bufRef, TRUE, myBuf);
    
}
/*__________________________________________________________________________
*/
void ruc_sockCtrl_show_select_stats(char * argv[], uint32_t tcpRef, void * bufRef) {
  char           *pChar=myBuf;
  int i;
  
  pChar +=sprintf(pChar,"Per select call statistics:\n");
  for (i = 0; i < ROZO_FD_SETSIZE; i++)
  {
     if (ruc_sockCtrl_nr_socket_stats[i] == 0) continue;
     pChar +=sprintf(pChar,"[%4.4d] = %llu\n",i,(long long unsigned)ruc_sockCtrl_nr_socket_stats[i]);
  }

  memset(ruc_sockCtrl_nr_socket_stats,0,sizeof(uint64_t)*ROZO_FD_SETSIZE);
  uma_dbg_send(tcpRef, bufRef, TRUE, myBuf);
}

/*__________________________________________________________________________
  Register to the debug SWBB
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void ruc_sockCtrl_debug_init() {
  uma_dbg_addTopicAndMan(RUC_SOCKCTRL_DEBUG_TOPIC, ruc_sockCtrl_debug,ruc_sockCtrl_debug_man,UMA_DBG_OPTION_RESET); 
  uma_dbg_addTopic(RUC_SOCKCTRL_CTX_TOPIC, ruc_sockCtrl_ctx); 
  uma_dbg_addTopic("sockctrl", ruc_sockCtrl_conf); 
  uma_dbg_addTopic_option("select_stats", ruc_sockCtrl_show_select_stats,UMA_DBG_OPTION_RESET); 
}




/*
**  END OF DEBUG
*/


#if 0
/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void ruc_sockCtrl_debug_show(char *myBuf) {
  ruc_sockObj_t     *p;
  int                i;
  char           *pChar=myBuf;
  uint32_t          average;

  p = ruc_sockCtrl_pFirstCtx;
  pChar += sprintf(pChar,"select max cpu time : %u us\n",ruc_sockCtrl_looptimeMax);
  ruc_sockCtrl_looptimeMax = 0;   
  pChar += sprintf(pChar,"%-18s %4s %5s %10s %10s %10s\n","application","sock", "last","cumulated", "activation", "average");
  pChar += sprintf(pChar,"%-18s %4s %5s %10s %10s %10s\n\n","name","nb", "cpu","cpu","times","cpu");
  
  for (i = 0; i < ruc_sockCtrl_maxConnection; i++)
  {
    if (p->socketId !=(uint32_t)-1)
    {
      if (p->nbTimes == 0) average = 0;
      else                 average = p->cumulatedTime/p->nbTimes;
      pChar += sprintf(pChar, "%-20s %2d %5u %10u %10u %10u\n", &p->name[0],p->socketId, p->lastTime, p->cumulatedTime,p->nbTimes, average);
#if 0
      if (p->nbTimesXmit == 0) average = 0;
      else                 average = p->cumulatedTimeXmit/p->nbTimesXmit;
      pChar += sprintf(pChar, "%-20s    %5u %10u %10u %10u\n", "  ",p->lastTimeXmit, p->cumulatedTimeXmit,p->nbTimesXmit, average);
#endif	  
      p->cumulatedTime = 0;
      p->nbTimes = 0;
    }
    p++;
  }

}

#endif



void ruc_sockctl_updatePnextCur(ruc_obj_desc_t *pHead,
                                   ruc_sockObj_t *pobj)
{


   if (ruc_sockctl_pnextCur == (ruc_obj_desc_t*)pobj)
   {
      /*
      ** ruc_sockctl_pnextCur needs to be updated
      */
      ruc_objGetNext(pHead, &ruc_sockctl_pnextCur);
     return;
   }
   
   if (ruc_sockctl_poll_pnextCur == (ruc_obj_desc_t*)pobj)
   {
      /*
      ** ruc_sockctl_pnextCur needs to be updated
      */
      ruc_objGetNext(pHead, &ruc_sockctl_poll_pnextCur);
     return;
   }
     
}


/* #STARTDOC
**
**  #TITLE
uint32_t ruc_sockctl_init(uint32_t maxConnection)

**  #SYNOPSIS
**    creation of the socket controller distributor
**
**
**   IN:
**       maxConnection : number of elements to create
**
**   OUT :
**       RUC_OK: the distributor has been created
**       RUC_NOK : out of memory
**
**
** ##ENDDOC
*/

uint32_t ruc_sockctl_init(uint32_t maxConnection)
{
  int i,idx;
  ruc_obj_desc_t  *pnext=(ruc_obj_desc_t*)NULL;
  ruc_sockObj_t  *p;

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
     ruc_listHdrInit(&ruc_sockCtl_tabPrio[i]);
  }
  /*
  ** erase the Fd receive & xmit set
  */
  memset(&rucRdFdSet,0,sizeof(rucRdFdSet));
  memset(&sockCtrl_speculative,0,sizeof(sockCtrl_speculative));
  memset(&rucWrFdSet,0,sizeof(rucWrFdSet));   
  memset(&rucRdFdSetUnconditional,0,sizeof(rucRdFdSetUnconditional));   
  memset(&rucWrFdSetCongested,0,sizeof(rucWrFdSetCongested));   
  memset(socket_recv_table,0xff,sizeof(int)*ROZO_FD_SETSIZE);
  memset(socket_xmit_table,0xff,sizeof(int)*ROZO_FD_SETSIZE);
  memset(socket_ctx_table,0,sizeof(ruc_sockObj_t *)*ROZO_FD_SETSIZE);
  memset(socket_predictive_ctx_table,0,sizeof(ruc_sockObj_t *)*ROZO_FD_SETSIZE);
  memset(socket_predictive_ctx_table_count,0,sizeof(int)*ROZO_FD_SETSIZE);
  ruc_sockCtrl_max_poll_ctx = RUC_SOCKCTL_POLLCOUNT;
  ruc_sockctl_poll_pnextCur = NULL;
  ruc_sockCtrl_poll_period = RUC_SOCKCTL_POLLFREQ; /** period of 40 ms */
  memset(ruc_sockCtrl_nr_socket_stats,0,sizeof(uint64_t)*ROZO_FD_SETSIZE);
  /*
  ** create the connection distributor
  */
  ruc_sockCtl_freeListHead = 
              (ruc_sockObj_t *)ruc_listCreate(maxConnection,
                                              sizeof(ruc_sockObj_t));
  if (ruc_sockCtl_freeListHead == (ruc_sockObj_t*)NULL)
  {
    /*
    ** out of memory
    */
    return RUC_NOK;
  }
  /*
  **  initialize each element of the free list
  */
  idx = 0;
  while ((p = (ruc_sockObj_t*)
              ruc_objGetNext((ruc_obj_desc_t*)ruc_sockCtl_freeListHead,
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
  ruc_sockCtrl_pFirstCtx = (ruc_sockObj_t*)ruc_objGetFirst((ruc_obj_desc_t*)ruc_sockCtl_freeListHead);

  /*
  ** do the connection with the debug
  */
  ruc_sockCtrl_debug_init();
  
  ruc_sockCtrl_maxConnection = maxConnection;

  
  return RUC_OK;

}


/* #STARTDOC
**
**  #TITLE
uint32_t ruc_sockctl_connect(int socketId,
                           char *name;
                           uint32_t priority,
                           uint32_t objRef,
                           ruc_sockCallBack_t *callback);
**  #SYNOPSIS
**    creation of connection with the socket controller.
**    if there is a free connection entry, it returns RUC_OK
**
**
**   IN:
**       socketId : socket identifier returned by the socket() service
**       priority : polling priority
**       objRef : object reference provided as a callback parameter
**      *callBack : pointer to the call back functions.
**
**   OUT :
**       !=NULL : connection identifier
**       ==NULL : out of context
**
**
** ##ENDDOC
*/

// 64BITS uint32_t ruc_sockctl_connect(int socketId,
void * ruc_sockctl_connect(int socketId,
                           char *name,
                           uint32_t priority,
			   // 64BITS                            uint32_t objRef,
                           void *objRef,
                           ruc_sockCallBack_t *callback)
{

    ruc_sockObj_t *p,*pelem;

  if (socketId >= ROZO_FD_SETSIZE) {
    fatal("ruc_sockctl_connect socketId out of range %d",socketId);
    return NULL;
  }
  /*
  ** update the max socket value if needed
  */
  if (ruc_max_curr_socket < socketId) ruc_max_curr_socket = socketId;

  /*
  ** get the first element from the free list
  */


  p = (ruc_sockObj_t*)ruc_sockCtl_freeListHead;
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
    ruc_objInsert(&ruc_sockCtl_tabPrio[priority],(ruc_obj_desc_t*)pelem);
  }
  else 
  {
    ruc_objInsert(&ruc_sockCtl_tabPrio[RUC_SOCKCTL_MAXPRIO],(ruc_obj_desc_t*)pelem);
    FD_SET(pelem->socketId,&rucRdFdSetUnconditional);        
  }
  pelem->priority  = priority;



  // 64BITS   return ((uint32_t)pelem);
  /*
  ** insert the context in the context table indexed by the socket_id
  */
  socket_ctx_table[pelem->socketId] = pelem;
  /*
  ** set the socket ready for receiving by default
  */ 
  return (pelem);

}




/* #STARTDOC
**
**  #TITLE
uint32_t ruc_sockctl_disconnect(uint32_t connectionId);
**  #SYNOPSIS
**    deletion of connection with the socket controller.
**
**
**   IN:
**       connectionId : reference returned by the connection service
**
**   OUT :
**       RUC_OK : connection identifier
**
**
** ##ENDDOC
*/

// 64BITS uint32_t ruc_sockctl_disconnect(uint32_t connectionId)
uint32_t ruc_sockctl_disconnect(void * connectionId)

{
   ruc_sockObj_t *p;
 
   p = (ruc_sockObj_t*)connectionId;

   /*
   ** update PnextCur before remove the object
   */
   if (p->priority >= RUC_SOCKCTL_MAXPRIO)
   {
      ruc_sockctl_updatePnextCur(&ruc_sockCtl_tabPrio[RUC_SOCKCTL_MAXPRIO],p);
   }
   else
   {
      ruc_sockctl_updatePnextCur(&ruc_sockCtl_tabPrio[p->priority],p);
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
     FD_CLR(p->socketId,&rucRdFdSet);     
     FD_CLR(p->socketId,&sockCtrl_speculative);     
     FD_CLR(p->socketId,&rucWrFdSet);
     FD_CLR(p->socketId,&rucRdFdSetUnconditional);
     FD_CLR(p->socketId,&rucWrFdSetCongested);

     ruc_sockCtrl_remove_socket(socket_recv_table,socket_recv_count,p->socketId);
     ruc_sockCtrl_remove_socket(socket_xmit_table,socket_xmit_count,p->socketId);
     socket_ctx_table[p->socketId] = NULL;
     socket_predictive_ctx_table[p->socketId] = NULL;
     socket_predictive_ctx_table_count[p->socketId] = 0; 
     p->socketId = -1;
   }

   /*
   **  insert in the free list
   */
  ruc_objInsert((ruc_obj_desc_t*)ruc_sockCtl_freeListHead,
                (ruc_obj_desc_t*)p);

  return RUC_OK;
}


/* #STARTDOC
**
**  #TITLE
void ruc_sockCtl_checkRcvBits()
**  #SYNOPSIS
**    That function check the receive bit for each active
**    connection. If the bit is set the receive function (provided
**    as call-back function)
**
**
**   IN:
**       none
**   OUT :
**       none
**
**
** ##ENDDOC
*/

void ruc_sockCtl_checkRcvBits()
{

  int i;
  ruc_sockObj_t *p;
  ruc_sockCallBack_t *pcallBack;
  int socketId;
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  
  timeBefore = 0;
  timeAfter  = 0;

  for (i = 0; i <RUC_SOCKCTL_MAXPRIO ; i++)
  {
    ruc_sockctl_pnextCur = (ruc_obj_desc_t*)NULL;
    ruc_sockctl_prioIdxCur = RUC_SOCKCTL_MAXPRIO-1-i;

    while ((p = (ruc_sockObj_t*)
              ruc_objGetNext((ruc_obj_desc_t*)&ruc_sockCtl_tabPrio[RUC_SOCKCTL_MAXPRIO-1-i],
                             &ruc_sockctl_pnextCur))!=(ruc_sockObj_t*)NULL) 
    {
#if 0
      printf("CheckRcvBits :socketId %d, name: %s\n",p->socketId,&p->name[0]);
#endif
      socketId = p->socketId;     
      if(FD_ISSET(socketId, &rucRdFdSet))
      {
        /*
        ** the receive bit is set, call the related function
        ** and update rcv count for statistics purpose
        */
        p->rcvCount++;
        pcallBack = p->callBack;

	 gettimeofday(&timeDay,(struct timezone *)0);  
	 timeBefore = MICROLONG(timeDay);
	 
        (*(pcallBack->msgInFunc))(p->objRef,p->socketId);
         gettimeofday(&timeDay,(struct timezone *)0);  
	 timeAfter = MICROLONG(timeDay);
	 p->lastTime = (uint32_t)(timeAfter - timeBefore);
	 p->cumulatedTime += p->lastTime;
	 p->nbTimes ++;
	/*
	**  clear the corresponding bit
	*/
	FD_CLR(socketId,&rucRdFdSet);
      }
    }
  }
}

/*
**____________________________________________________________________________
*/

static inline void ruc_sockCtl_checkRcvAndXmitBits(int nbrSelect)
{

  int i;
  ruc_sockObj_t *p;
  ruc_sockCallBack_t *pcallBack;
  int socketId;
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  int loopcount;
  uint64_t cycles_before,cycles_after;
  
  timeBefore = 0;
  timeAfter  = 0;
  cycles_before = rdtsc();  
  loopcount= nbrSelect;
  for (i = 0; i <RUC_SOCKCTL_MAXPRIO ; i++)
  {
    ruc_sockctl_pnextCur = (ruc_obj_desc_t*)NULL;
    ruc_sockctl_prioIdxCur = RUC_SOCKCTL_MAXPRIO-1-i;

    while ((p = (ruc_sockObj_t*)
              ruc_objGetNext((ruc_obj_desc_t*)&ruc_sockCtl_tabPrio[RUC_SOCKCTL_MAXPRIO-1-i],
                             &ruc_sockctl_pnextCur))!=(ruc_sockObj_t*)NULL) 
    {
#if 0
      printf("CheckRcvBits :socketId %d, name: %s\n",p->socketId,&p->name[0]);
#endif
      socketId = p->socketId;
#if 0
      /*
      ** check the traffic shaper
      */
      if (ruc_applicative_traffic_shaper != NULL)
      {

       (*ruc_applicative_traffic_shaper)(timeAfter);
      }
#endif
#if APP_POLLING
      if (ruc_applicative_poller != NULL)
      {
	ruc_applicative_poller_count++;
	uint64_t cycles_start = rdtsc();  	
	(*ruc_applicative_poller)(0);
        ruc_applicative_poller_cycles += (rdtsc() - cycles_start);
      }
#endif
      if(FD_ISSET(socketId, &rucRdFdSet))
      {
        /*
        ** the receive bit is set, call the related function
        ** and update rcv count for statistics purpose
        */
        p->rcvCount++;
        pcallBack = p->callBack;

        gettimeofday(&timeDay,(struct timezone *)0);  
        timeBefore = MICROLONG(timeDay);

        (*(pcallBack->msgInFunc))(p->objRef,p->socketId);
        gettimeofday(&timeDay,(struct timezone *)0);  
        timeAfter = MICROLONG(timeDay);
        p->lastTime = (uint32_t)(timeAfter - timeBefore);
        p->cumulatedTime += p->lastTime;
        p->nbTimes ++;
        /*
        **  clear the corresponding bit
        */
        FD_CLR(socketId,&rucRdFdSet);
        loopcount--;
        if (loopcount == 0) break;
      }
      if(FD_ISSET(socketId, &rucWrFdSet))
      {
        /*
        ** the receive bit is set, call the related function
        ** and update rcv count for statistics purpose
        */
        p->xmitCount++;
        pcallBack = p->callBack;

        gettimeofday(&timeDay,(struct timezone *)0);  
        timeBefore = MICROLONG(timeDay);

        (*(pcallBack->xmitEvtFunc))(p->objRef,p->socketId);

        timeAfter = MICROLONG(timeDay);
        p->lastTime = (uint32_t)(timeAfter - timeBefore);
        p->cumulatedTime += p->lastTime;
        p->nbTimes ++;
#if 0
        p->lastTimeXmit = (uint32_t)(timeAfter - timeBefore);
        p->cumulatedTimeXmit += p->lastTimeXmit;
        p->nbTimesXmit ++;
#endif
        /*
        **  clear the corresponding bit
        */
	FD_CLR(socketId,&rucWrFdSet);
        loopcount--;
        if (loopcount == 0) break;
      }
    }
  }
  cycles_after = rdtsc();
  ruc_time_receive += (cycles_after - cycles_before);
  ruc_count_receive++;
}

/*
**____________________________________________________________________________
*/
static inline void ruc_sockCtl_checkRcvAndXmitBits_opt(int nbrSelect)
{

  int i;
  ruc_sockObj_t *p;
  ruc_sockCallBack_t *pcallBack;
  int socketId;
  int speculative_count = 0;
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
#if APP_POLLING_OPT
  uint64_t  ruc_applicative_poller_ticker = rozofs_ticker_microseconds;
#endif  
  timeBefore = 0;
  timeAfter  = 0;

  uint64_t cycles_before,cycles_after;
  cycles_before = rdtsc();
#if APP_POLLING_OPT
  if (ruc_applicative_poller != NULL)
  {
	ruc_applicative_poller_count++;
	uint64_t cycles_start = rdtsc();  	
	(*ruc_applicative_poller)(0);
        ruc_applicative_poller_cycles += (rdtsc() - cycles_start);
  }
  ruc_applicative_poller_ticker +=1;
#endif
  cycles_before = rdtsc();
  /*
  ** build the table for the receive and xmit sides
  */
  socket_recv_count = ruc_sockCtrl_build_sock_table((uint64_t *)&rucRdFdSet,socket_recv_table,nbrSelect);
  socket_xmit_count = ruc_sockCtrl_build_sock_table((uint64_t *)&rucWrFdSet,socket_xmit_table,nbrSelect);
  /*
  ** case of the speculative scheduler
  */
  if (ruc_sockCtrl_speculative_sched_enable)
  {
    speculative_count= ruc_sockCtrl_speculative_count;
    if (speculative_count > 0)
    {
      speculative_count = ruc_sockCtrl_build_sock_table((uint64_t *)&sockCtrl_speculative,socket_speculative_table,speculative_count);
    }
    if (ruc_sockCtrl_max_speculative < speculative_count) ruc_sockCtrl_max_speculative = speculative_count;

  }
  cycles_after = rdtsc();
  ruc_time_receive += (cycles_after - cycles_before);
  ruc_count_receive++;

  for (i = 0; i <socket_recv_count ; i++)
  {
    socketId = socket_recv_table[i];
    if (socketId == -1) continue;
#if 0
    /*
    ** check the traffic shaper
    */
    if (ruc_applicative_traffic_shaper != NULL)
    {
     (*ruc_applicative_traffic_shaper)(timeAfter);
    }
#endif
    p = socket_ctx_table[socketId];
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
#endif
    (*(pcallBack->msgInFunc))(p->objRef,p->socketId);
#ifdef ROZO_MES
    gettimeofday(&timeDay,(struct timezone *)0);  
    timeAfter = MICROLONG(timeDay);
    p->lastTime = (uint32_t)(timeAfter - timeBefore);
    p->cumulatedTime += p->lastTime;
    p->nbTimes ++;        
#endif
#if APP_POLLING_OPT
    if (ruc_applicative_poller != NULL)
    {
        if (ruc_applicative_poller_ticker < timeAfter)
	{
	  ruc_applicative_poller_count++;
	  uint64_t cycles_start = rdtsc();  	
	  (*ruc_applicative_poller)(0);
          ruc_applicative_poller_cycles += (rdtsc() - cycles_start);  
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
    socketId = socket_speculative_table[i];
    if (socketId == -1) continue;
    p = socket_predictive_ctx_table[socketId];
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

    (*(pcallBack->msgInFunc))(p->objRef,p->socketId);
    gettimeofday(&timeDay,(struct timezone *)0);  
    timeAfter = MICROLONG(timeDay);
    p->lastTime = (uint32_t)(timeAfter - timeBefore);
    p->cumulatedTime += p->lastTime;
    p->nbTimes ++;         
  }

  for (i = 0; i <socket_xmit_count ; i++)
  {
    socketId = socket_xmit_table[i];
    if ( socketId == -1) continue;
    p = socket_ctx_table[  socket_xmit_table[i]];
    if (p == NULL) 
    {
      FD_CLR(socketId,&rucWrFdSet);
      continue;
    }
    FD_CLR(socketId,&rucWrFdSet);
    p->xmitCount++;
    pcallBack = p->callBack;
#ifdef ROZO_MES
    gettimeofday(&timeDay,(struct timezone *)0);  
    timeBefore = MICROLONG(timeDay);
#endif
    (*(pcallBack->xmitEvtFunc))(p->objRef,p->socketId);

#ifdef ROZO_MES
    timeAfter = MICROLONG(timeDay);
    p->lastTime = (uint32_t)(timeAfter - timeBefore);
    p->cumulatedTime += p->lastTime;
    p->nbTimes ++;
#endif
  }


}

/*
**____________________________________________________________________________
*/
/* #STARTDOC
**
**  #TITLE
void ruc_sockCtl_prepareRcvBits()
**  #SYNOPSIS
**    That function calls the receiverready function of
**    each active connection. If the application replies
**    TRUE then the corresponding bit is set.
**
**   IN:
**       none
**   OUT :
**       none
**
**
** ##ENDDOC
*/


void ruc_sockCtl_prepareRcvBits()
{

  int i;
  ruc_sockObj_t *p;
  uint32_t ret;

  /*
  ** erase the Fd receive set
  */
  memset(&rucRdFdSet,0,sizeof(rucRdFdSet));

  for (i = 0; i <RUC_SOCKCTL_MAXPRIO ; i++)
  {
    ruc_sockctl_pnextCur = (ruc_obj_desc_t*)NULL;
    ruc_sockctl_prioIdxCur = RUC_SOCKCTL_MAXPRIO-1-i;

    while ((p = (ruc_sockObj_t*)
              ruc_objGetNext((ruc_obj_desc_t*)&ruc_sockCtl_tabPrio[RUC_SOCKCTL_MAXPRIO-1-i],
                             &ruc_sockctl_pnextCur))!=(ruc_sockObj_t*)NULL) 
    {
      ret = (*((p->callBack)->isRcvReadyFunc))(p->objRef,p->socketId);
      if(ret == TRUE)
      {
        /*
        ** The receiver is ready, assert the corresponding bit
        */
#if 0
      printf("prepareRcvBits :socketId %d, name: %s\n",p->socketId,&p->name[0]);
#endif

        FD_SET(p->socketId,&rucRdFdSet);
      }
    }
  }
}
/*
**____________________________________________________________________________
*/
static inline void ruc_sockCtl_prepareRcvAndXmitBits()
{

  int i;
  ruc_sockObj_t *p;
  uint32_t ret;
  int polling_cnt = 2;
  
  uint64_t time_before,time_after;
  ruc_sockCtrl_nb_socket_conditional = 0;

  time_before = rdtsc();
  /*
  ** copy the bitmap of the sockets for which the receive is unconditional
  */
  memcpy(&rucRdFdSet,&rucRdFdSetUnconditional,sizeof(rucRdFdSet));
  memcpy(&rucWrFdSet,&rucWrFdSetCongested,sizeof(rucWrFdSet));

  for (i = 0; i <RUC_SOCKCTL_MAXPRIO ; i++)
  {
    ruc_sockctl_pnextCur = (ruc_obj_desc_t*)NULL;
    ruc_sockctl_prioIdxCur = RUC_SOCKCTL_MAXPRIO-1-i;

    while ((p = (ruc_sockObj_t*)
              ruc_objGetNext((ruc_obj_desc_t*)&ruc_sockCtl_tabPrio[RUC_SOCKCTL_MAXPRIO-1-i],
                             &ruc_sockctl_pnextCur))!=(ruc_sockObj_t*)NULL) 
    {
      ruc_sockCtrl_nb_socket_conditional++;
#if APP_POLLING
      if ((polling_cnt!=0)&& (ruc_applicative_poller != NULL))
      {
        polling_cnt -=1;
	ruc_applicative_poller_count++;
	uint64_t cycles_start = rdtsc();  	
	(*ruc_applicative_poller)(0);
        ruc_applicative_poller_cycles += (rdtsc() - cycles_start);
      }
#endif
      FD_CLR(p->socketId,&rucWrFdSet);
      FD_CLR(p->socketId,&rucRdFdSet);
      ret = (*((p->callBack)->isRcvReadyFunc))(p->objRef,p->socketId);
      if(ret == TRUE)
      {
        /*
        ** The receiver is ready, assert the corresponding bit
        */
#if 0
      printf("prepareRcvBits :socketId %d, name: %s\n",p->socketId,&p->name[0]);
#endif

        FD_SET(p->socketId,&rucRdFdSet);
      }
      ret = (*((p->callBack)->isXmitReadyFunc))(p->objRef,p->socketId);
      if(ret == TRUE)
      {
        /*
        ** The receiver is ready, assert the corresponding bit
        */
        FD_SET(p->socketId,&rucWrFdSet);
      }
    }
  }
  time_after = rdtsc();
  ruc_time_prepare += (time_after - time_before);
  ruc_count_prepare++;
  
}

/*
**____________________________________________________________________________
*/
/* #STARTDOC
**
**  #TITLE
void ruc_sockCtl_checkXmitBits()
**  #SYNOPSIS
**    That function check the xmit bit for each active
**    connection. If the bit is set the xmit function (provided
**    as call-back function)
**
**
**   IN:
**       none
**   OUT :
**       none
**
**
** ##ENDDOC
*/

void ruc_sockCtl_checkXmitBits()
{

  int i;
  ruc_sockObj_t *p;
  ruc_sockCallBack_t *pcallBack;
  int socketId;


  for (i = 0; i <RUC_SOCKCTL_MAXPRIO ; i++)
  {
    ruc_sockctl_pnextCur = (ruc_obj_desc_t*)NULL;
    ruc_sockctl_prioIdxCur = RUC_SOCKCTL_MAXPRIO-1-i;

    while ((p = (ruc_sockObj_t*)
              ruc_objGetNext((ruc_obj_desc_t*)&ruc_sockCtl_tabPrio[RUC_SOCKCTL_MAXPRIO-1-i],
                             &ruc_sockctl_pnextCur))!=(ruc_sockObj_t*)NULL) 
    {
      socketId = p->socketId;
      if(FD_ISSET(socketId, &rucWrFdSet))
      {
        /*
        ** the receive bit is set, call the related function
        ** and update rcv count for statistics purpose
        */
        p->xmitCount++;
        pcallBack = p->callBack;

        (*(pcallBack->xmitEvtFunc))(p->objRef,p->socketId);
	/*
	**  clear the corresponding bit
	*/
	FD_CLR(socketId,&rucWrFdSet);
      }
    }
  }
}

/*
**____________________________________________________________________________
*/
/* #STARTDOC
**
**  #TITLE
void ruc_sockCtl_prepareXmitBits()
**  #SYNOPSIS
**    That function calls the transmitter ready function of
**    each active connection. If the application replies
**    TRUE then the corresponding bit is set.
**
**   IN:
**       none
**   OUT :
**       none
**
**
** ##ENDDOC
*/

void ruc_sockCtl_prepareXmitBits()
{

  int i;
  ruc_sockObj_t *p;
  uint32_t ret;

  /*
  ** erase the Fd receive set
  */
  memset(&rucWrFdSet,0,sizeof(rucWrFdSet));

  for (i = 0; i <RUC_SOCKCTL_MAXPRIO ; i++)
  {
    ruc_sockctl_pnextCur = (ruc_obj_desc_t*)NULL;
    ruc_sockctl_prioIdxCur = RUC_SOCKCTL_MAXPRIO-1-i;

    while ((p = (ruc_sockObj_t*)
              ruc_objGetNext((ruc_obj_desc_t*)&ruc_sockCtl_tabPrio[RUC_SOCKCTL_MAXPRIO-1-i],
                             &ruc_sockctl_pnextCur))!=(ruc_sockObj_t*)NULL) 
    {
      ret = (*((p->callBack)->isXmitReadyFunc))(p->objRef,p->socketId);
      if(ret == TRUE)
      {
        /*
        ** The receiver is ready, assert the corresponding bit
        */
        FD_SET(p->socketId,&rucWrFdSet);
      }
    }
  }
}
/*
**____________________________________________________________________________
*/
static inline void ruc_sockCtrl_roundRobbin()
{

  int i;
  ruc_sockObj_t *p;

  for (i = 0; i <RUC_SOCKCTL_MAXPRIO ; i++)
  {
    p = (ruc_sockObj_t *)
        ruc_objGetFirst((ruc_obj_desc_t*)&ruc_sockCtl_tabPrio[RUC_SOCKCTL_MAXPRIO-1-i]);
    if (p!= (ruc_sockObj_t*)NULL)
    {
       ruc_objRemove((ruc_obj_desc_t*)p);
       ruc_objInsertTail((ruc_obj_desc_t*)&ruc_sockCtl_tabPrio[RUC_SOCKCTL_MAXPRIO-1-i],
                          (ruc_obj_desc_t*)p);
    }
  }



}

/*
**____________________________________________________________________________
*/
/**
* init of the system ticker
*/
void rozofs_init_ticker()
{
  struct timeval     timeDay;

  gettimeofday(&timeDay,(struct timezone *)0);  
  rozofs_ticker_microseconds = MICROLONG(timeDay);
  rozofs_ticker_seconds = (uint64_t)timeDay.tv_sec;

}
uint64_t rozofs_ticker_microseconds = 0;  /**< ticker in microsecond ->see gettimeofday */
uint64_t rozofs_ticker_seconds = 0;  /**< ticker in second ->see gettimeofday */
/*
**____________________________________________________________________________
*/
/**
*  Main loop
*/
void ruc_sockCtrl_selectWait()
{
    int     nbrSelect;    /* nbr of events detected by select function */
    struct timeval     timeDay;
    unsigned long long timeBefore, timeAfter;
//    uint64_t cycles_before;
//    uint64_t cycles_after;
//    uint32_t  	       timeOutLoopCount;  
    unsigned long long looptimeEnd,looptimeStart = 0;   
     timeBefore = 0;
     timeAfter  = 0;
//     timeOutLoopCount = 0;

    /*
    ** update time before call select
    */
    gettimeofday(&timeDay,(struct timezone *)0);  
    timeBefore = MICROLONG(timeDay);
    rozofs_ticker_microseconds = timeBefore;
    rozofs_ticker_seconds = timeDay.tv_sec;

    while (1)
    {
      /*
      **  compute rucRdFdSet and rucWrFdSet
      */
      ruc_sockCtl_prepareRcvAndXmitBits();
//      cycles_before = rdtsc();     
      gettimeofday(&timeDay,(struct timezone *)0);  
//      cycles_after = rdtsc();  
//      gettimeofday_cycles+=  cycles_after -  cycles_before;
//      gettimeofday_count +=1;
      looptimeEnd = MICROLONG(timeDay);  
      ruc_sockCtrl_looptime= (uint32_t)(looptimeEnd - looptimeStart); 
      if (ruc_sockCtrl_looptime > ruc_sockCtrl_looptimeMax)
      {
	  ruc_sockCtrl_looptimeMax = ruc_sockCtrl_looptime;
      }	  
      /*
      ** wait for event 
      */	  
      if((nbrSelect=select(ruc_max_curr_socket+1,(fd_set *)&rucRdFdSet,
                                                 (fd_set *)&rucWrFdSet,NULL, NULL)) == 0)
      {
	/*
	** udpate time after select
	*/
	gettimeofday(&timeDay,(struct timezone *)0);  
	timeAfter = MICROLONG(timeDay); 
        rozofs_ticker_microseconds = timeAfter;
        rozofs_ticker_seconds = timeDay.tv_sec;
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
        rozofs_ticker_microseconds = looptimeStart;
        rozofs_ticker_seconds = timeDay.tv_sec;
	if (ruc_sockCtrl_max_nr_select < nbrSelect) ruc_sockCtrl_max_nr_select = nbrSelect;
        ruc_sockCtrl_nr_socket_stats[nbrSelect]++;
	
	ruc_sockCtl_checkRcvAndXmitBits_opt(nbrSelect);
	/*
	**  insert the first element of each priority list at the
	**  tail of its priority list.
	*/
//	ruc_sockCtrl_roundRobbin();
        gettimeofday(&timeDay,(struct timezone *)0);  
	timeAfter = MICROLONG(timeDay); 
        rozofs_ticker_microseconds = timeAfter;
        rozofs_ticker_seconds = timeDay.tv_sec;
      }
      /*
      ** socket polling (former receive ready callback)
      */
      if (timeAfter > (ruc_sockCtrl_lastTimeScheduler+ruc_sockCtrl_poll_period))
      {
	timeBefore = timeAfter;
	ruc_sockCtrl_lastTimeScheduler = timeAfter;
	ruc_sockCtrl_socket_poll();
	gettimeofday(&timeDay,(struct timezone *)0);  
	timeAfter = MICROLONG(timeDay);
	ruc_sockCtrl_lastCpuScheduler = (uint32_t)(timeAfter - timeBefore);
	ruc_sockCtrl_cumulatedCpuScheduler += ruc_sockCtrl_lastCpuScheduler;
	ruc_sockCtrl_nbTimesScheduler ++;     
      }   
    }
}

/**
* clear the associated fd bit in the fdset

  @param int fd : file descriptor to clear
*/
void ruc_sockCtrl_clear_rcv_bit(int fd)
{
  if (fd < 0) return;
  FD_CLR(fd,&rucRdFdSet);
}
