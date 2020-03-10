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
#ifndef SOCKET_CTRL_TH_H
#define SOCKET_CTRL_TH_H

#include <rozofs/common/types.h>
#include <sys/select.h>
#include "ruc_common.h"
#include "ruc_list.h"
#include "ruc_sockCtl_api.h"
#include "socketCtrl.h"

#define ROZOFS_MAX_SOCKCTL_DESCRIPTOR 8
extern int ruc_sockctl_current_idx;

/*
** file descriptor for receiving and transmitting events
*/
//extern rozo_fd_set  rucWrFdSetCongested;


typedef struct _rozofs_sockctrl_ctx_t
{
int   thread_owner;
uint32_t module_idx;
char *name;
/*
**  priority table
*/
ruc_obj_desc_t  ruc_sockCtl_tabPrio[RUC_SOCKCTL_MAXPRIO+1];
int ruc_sockCtrl_speculative_sched_enable;
int ruc_sockCtrl_speculative_count;
int ruc_max_curr_socket;
/*
** head of the free connection list
*/
ruc_sockObj_t   *ruc_sockCtl_freeListHead;
ruc_sockObj_t   *ruc_sockCtrl_pFirstCtx;
uint32_t          ruc_sockCtrl_maxConnection;
uint64_t         af_unix_rcv_buffered;
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
   uint64_t  ruc_time_prepare ;
   uint64_t  ruc_count_prepare;
   uint64_t  ruc_time_receive ;
   uint64_t  ruc_count_receive;
   uint32_t  ruc_sockCtrl_nb_socket_conditional;
   uint64_t gettimeofday_count;
   uint64_t gettimeofday_cycles;
   int ruc_sockCtrl_max_nr_select;
   int ruc_sockCtrl_max_speculative;


   uint32_t ruc_sockCtrl_lastCpuScheduler;
   uint32_t ruc_sockCtrl_cumulatedCpuScheduler;
   uint32_t ruc_sockCtrl_nbTimesScheduler;
   uint64_t ruc_sockCtrl_lastTimeScheduler;
   uint32_t ruc_sockCtrl_looptime;
   uint32_t ruc_sockCtrl_looptimeMax;

   ruc_scheduler_t ruc_applicative_traffic_shaper;
   ruc_scheduler_t ruc_applicative_poller;
   uint64_t ruc_applicative_poller_cycles;
   uint64_t ruc_applicative_poller_count;
   /*
   ** table used for storing the index of the socket for which there is the associated bit asserted
   */
   int socket_recv_count;
   int socket_recv_table[ROZO_FD_SETSIZE];
   int socket_speculative_table[ROZO_FD_SETSIZE];
   int socket_xmit_count;
   int socket_xmit_table[ROZO_FD_SETSIZE];
   ruc_sockObj_t *socket_ctx_table[ROZO_FD_SETSIZE];
   ruc_sockObj_t *socket_predictive_ctx_table[ROZO_FD_SETSIZE];
   int socket_predictive_ctx_table_count[ROZO_FD_SETSIZE];
   int ruc_sockCtrl_max_poll_ctx;
   ruc_obj_desc_t *ruc_sockctl_poll_pnextCur;
   uint64_t ruc_sockCtrl_poll_period;   /**< period in microseconds */ 
   uint64_t ruc_sockCtrl_nr_socket_stats[ROZO_FD_SETSIZE];
} rozofs_sockctrl_ctx_t;


extern rozofs_sockctrl_ctx_t *ruc_sockctl_ctx_tb[];
#endif
