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

/* need for crypt */
#define _XOPEN_SOURCE 500

#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <sys/un.h>
#include <unistd.h>
#include <fcntl.h> 
#include <errno.h>  
#include <stdarg.h>    
#include <string.h>  
#include <strings.h>
#include <semaphore.h>
#include <pthread.h>
#include <config.h>
#include <pthread.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/profile.h>
#include <rozofs/common/common_config.h>
#include <rozofs/core/ruc_common.h>
#include <rozofs/core/ruc_sockCtl_api.h>
#include <rozofs/core/ruc_timer_api.h>
#include <rozofs/core/uma_tcp_main_api.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/ruc_tcpServer_api.h>
#include <rozofs/core/ruc_tcp_client_api.h>
#include <rozofs/core/uma_well_known_ports_api.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/north_lbg_api.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/rozofs_rpc_non_blocking_generic_srv.h>
#include <rozofs/core/ruc_buffer_debug.h>
#include <rozofs/core/rozofs_host2ip.h>
#include <rozofs/core/rozofs_string.h>
#include <rozofs/core/rozo_launcher.h>
#include <rozofs/rpc/eproto.h>
#include <rozofs/rpc/epproto.h>
#include <rozofs/rpc/sproto.h>

#include "expbt_global.h"

#include "expbt_nblock_init.h"
#include "expbt_north_intf.h"
#include "expbt_trk_thread_intf.h"
#include <rozofs/rpc/expbt_protocol.h>
#include <rozofs/common/expbt_inode_file_tracking.h>


void * decoded_rpc_buffer_pool = NULL;
int    decoded_rpc_buffer_size;




DEFINE_PROFILING(expbt_profiler_t);

/*
 **_________________________________________________________________________
 *      PUBLIC FUNCTIONS
 **_________________________________________________________________________
 */



struct timeval Global_timeDay;
unsigned long long Global_timeBefore, Global_timeAfter;



/*
 **
 */

void fdl_debug_loop(int line) {
    int loop = 1;

    return;
    while (loop) {
        sleep(5);
        info("Fatal error on nb thread create (line %d) !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ", line);

    }


}

/*__________________________________________________________________________
*/  
#define SHOW_PROFILER_PROBE(probe) pChar += sprintf(pChar," %-12s | %15"PRIu64" | %9"PRIu64" | %18"PRIu64" | %15s |\n",\
                    #probe,\
                    gprofiler->probe[P_COUNT],\
                    gprofiler->probe[P_COUNT]?gprofiler->probe[P_ELAPSE]/gprofiler->probe[P_COUNT]:0,\
                    gprofiler->probe[P_ELAPSE]," " );

#define SHOW_PROFILER_PROBE_BYTE(probe) pChar += sprintf(pChar," %-12s | %15"PRIu64" | %9"PRIu64" | %18"PRIu64" | %15"PRIu64" |\n",\
                    #probe,\
                    gprofiler->probe[P_COUNT],\
                    gprofiler->probe[P_COUNT]?gprofiler->probe[P_ELAPSE]/gprofiler->probe[P_COUNT]:0,\
                    gprofiler->probe[P_ELAPSE],\
                    gprofiler->probe[P_BYTES]);



#define SHOW_PROFILER_PROBE_BATCH(probe) pChar += sprintf(pChar," %-12s | %15"PRIu64" | %9"PRIu64" | %18"PRIu64" | %15"PRIu64" |\n",\
                    #probe,\
                    gprofiler->probe[P_COUNT],\
                    gprofiler->probe[P_BYTES]?gprofiler->probe[P_ELAPSE]/gprofiler->probe[P_BYTES]:0,\
                    gprofiler->probe[P_ELAPSE],\
                    gprofiler->probe[P_BYTES]);
#define RESET_PROFILER_PROBE(probe) \
{ \
         gprofiler->probe[P_COUNT] = 0;\
         gprofiler->probe[P_ELAPSE] = 0; \
}

#define RESET_PROFILER_PROBE_BYTE(probe) \
{ \
   RESET_PROFILER_PROBE(probe);\
   gprofiler->probe[P_BYTES] = 0; \
}

static char * show_profiler_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"profiler reset       : reset statistics\n");
  pChar += sprintf(pChar,"profiler             : display statistics\n");  
  return pChar; 
}
void show_profiler(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();

    time_t elapse;
    int days, hours, mins, secs;
    time_t  this_time = time(0);    
    
    elapse = (int) (this_time - gprofiler->uptime);
    days = (int) (elapse / 86400);
    hours = (int) ((elapse / 3600) - (days * 24));
    mins = (int) ((elapse / 60) - (days * 1440) - (hours * 60));
    secs = (int) (elapse % 60);


    pChar += sprintf(pChar, "GPROFILER version %s uptime =  %d days, %2.2d:%2.2d:%2.2d\n", gprofiler->vers,days, hours, mins, secs);
    pChar += sprintf(pChar, "   procedure  |     count       |  time(us) | cumulated time(us) |     bytes       |\n");
    pChar += sprintf(pChar, "--------------+-----------------+-----------+--------------------+-----------------+\n");
    SHOW_PROFILER_PROBE_BATCH(file_check);
    SHOW_PROFILER_PROBE(load_dentry);
    SHOW_PROFILER_PROBE_BYTE(file_read);
    
    if (argv[1] != NULL)
    {
      if (strcmp(argv[1],"reset")==0) {
       RESET_PROFILER_PROBE_BYTE(file_check);
       RESET_PROFILER_PROBE_BYTE(file_read);
       RESET_PROFILER_PROBE(load_dentry);

	pChar += sprintf(pChar,"Reset Done\n");  
	gprofiler->uptime = this_time;   
      }
      else {
	/*
	** Help
	*/
	pChar = show_profiler_help(pChar);
      }
    }    
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}

/*
**__________________________________________________________________________
*/
uint32_t ruc_init(uint32_t test, expbt_start_conf_param_t *arg_p) {
    int ret = RUC_OK;


    uint32_t mx_tcp_client = 2;
    uint32_t mx_tcp_server = 8;
    uint32_t mx_tcp_server_cnx = 10;
    uint32_t        mx_af_unix_ctx = ROZO_AFUNIX_CTX_STORIO;
    

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
    ret = ruc_sockctl_init(ROZO_SOCKCTRL_CTX_STORIO);
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
        ret = af_unix_module_init(mx_af_unix_ctx,
                                  2,1024*1, // xmit(count,size)
                                  2,1024*1 // recv(count,size)
                                  );
        if (ret != RUC_OK) break;   
        /*
         **--------------------------------------
         **   D E B U G   M O D U L E
         **--------------------------------------
         */  
        {
          char name[32];
          sprintf(name, "expbt-%d ",arg_p->instance);	
          uma_dbg_set_name(name);

          sprintf(name, "expbt:%d ",  arg_p->instance);	
          uma_dbg_init_no_system(10,INADDR_ANY,arg_p->debug_port,name);
        }
        /*
        ** RPC SERVER MODULE INIT
        */
        ret = rozorpc_srv_module_init_ctx_only(common_config.expbt_buf_cnt);
        if (ret != RUC_OK) break; 


        break;
    }

    //#warning Start of specific application initialization code


    return ret;
}


/*
 *_______________________________________________________________________
 */

/**
 *  This function is the entry point for setting rozofs in non-blocking mode

   @param args->ch: reference of the fuse channnel
   @param args->se: reference of the fuse session
   @param args->max_transactions: max number of transactions that can be handled in parallel
   
   @retval -1 on error
   @retval : no retval -> only on fatal error

 */
int expbt_start_nb_th(void *args) {
  int ret;
  expbt_start_conf_param_t *args_p = (expbt_start_conf_param_t*) args;
  int size = 0;
  
  ALLOC_PROFILING(expbt_profiler_t);

  ret = ruc_init(FALSE, args_p);
  if (ret != RUC_OK) {
    /*
     ** fatal error
     */
    fdl_debug_loop(__LINE__);
    fatal("can't initialize non blocking thread");
    return -1;
  }
  /*
  ** Create a buffer pool to decode spproto RPC requests
  */
  size = sizeof(expbt_msg_t); 
  
  /*
  ** align the size on 8 bytes 
  */
  if ((size % 8 )!=0) size = ((size/8)*8) + 8;
  decoded_rpc_buffer_size = size;

  decoded_rpc_buffer_pool = ruc_buf_poolCreate(common_config.expbt_buf_cnt,size);
  if (decoded_rpc_buffer_pool == NULL) {
    fatal("Can not allocate decoded_rpc_buffer_pool");
    return -1;
  }
  ruc_buffer_debug_register_pool("rpcDecodedRequest",decoded_rpc_buffer_pool);

  /*
  ** Initialize the disk thread interface and start the disk threads
  */	
  ret = expbt_trk_thread_intf_create("localhost",args_p->instance,common_config.nb_expbt_thread,common_config.expbt_buf_cnt) ;
  if (ret < 0) {
    fatal("expbt_trk_thread_intf_create");
    return -1;
  }
  /*
  ** Init of the north interface (read/write request processing)
  */ 
  ret = expbt_north_interface_init(NULL,args_p->io_port,common_config.expbt_buf_cnt);
  if (ret < 0) {
    fatal("Fatal error on expbt_north_interface_init()\n");
    return -1;
  }

  /*
  ** add profiler subject 
  */
  uma_dbg_addTopic_option("profiler", show_profiler,UMA_DBG_OPTION_RESET);
  /*
  ** set the debug function for directories & files changes tracking
  */
  uma_dbg_addTopic("expbt_inode_track",show_expbt_track_inode_display_for_slice);   
  /*
  **  change the priority of the main thread
  */
    {
      struct sched_param my_priority;
      int policy=-1;
      int ret= 0;

      pthread_getschedparam(pthread_self(),&policy,&my_priority);
          DEBUG("storio main thread Scheduling policy   = %s\n",
                    (policy == SCHED_OTHER) ? "SCHED_OTHER" :
                    (policy == SCHED_FIFO)  ? "SCHED_FIFO" :
                    (policy == SCHED_RR)    ? "SCHED_RR" :
                    "???");
 #if 1
      my_priority.sched_priority= 97;
      policy = SCHED_FIFO;
      ret = pthread_setschedparam(pthread_self(),policy,&my_priority);
      if (ret < 0) 
      {
	severe("error on sched_setscheduler: %s",strerror(errno));	
      }
      pthread_getschedparam(pthread_self(),&policy,&my_priority);
          DEBUG("RozoFS thread Scheduling policy (prio %d)  = %s\n",my_priority.sched_priority,
                    (policy == SCHED_OTHER) ? "SCHED_OTHER" :
                    (policy == SCHED_FIFO)  ? "SCHED_FIFO" :
                    (policy == SCHED_RR)    ? "SCHED_RR" :
                    "???");
 #endif        
     
    }  
  /*
   ** main loop
   */
  gprofiler->uptime = time(0);
  while (1) {
      ruc_sockCtrl_selectWait();
  }
  fatal("Exit from ruc_sockCtrl_selectWait()");
  fdl_debug_loop(__LINE__);
  return -1;
}
