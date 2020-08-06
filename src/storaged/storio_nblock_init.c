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

#include "storio_nblock_init.h"
#include "storio_north_intf.h"
#include "storage_fd_cache.h"
#include "storio_disk_thread_intf.h"
#include "sproto_nb.h"
#include "config.h"
#include "sconfig.h"
#include "storage.h"
#include "storio_crc32.h"
#include "storio_device_mapping.h"
#include <rozofs/rdma/rozofs_rdma.h>
#include "storio_rdma_recv.h"

extern sconfig_t storaged_config;

void * decoded_rpc_buffer_pool = NULL;
int    decoded_rpc_buffer_size;

extern char storaged_config_file[];
DECLARE_PROFILING(spp_profiler_t);

void  storio_repair_stat_man(char * pChar);
void  storio_repair_stat_cli(char * argv[], uint32_t tcpRef, void *bufRef);


/*
** File to write a self healing command and read a selfhealing result
*/
char storio_selfhealing_result_file[128];
char storio_selfhealing_command_file[128];


/*
 **_________________________________________________________________________
 *      PUBLIC FUNCTIONS
 **_________________________________________________________________________
 */



#define sp_display_io_probe(the_profiler, the_probe)\
    {\
        uint64_t rate;\
        uint64_t cpu;\
        uint64_t throughput;\
        if ((the_profiler->the_probe[P_COUNT] == 0) || (the_profiler->the_probe[P_ELAPSE] == 0) ){\
            cpu = rate = throughput = 0;\
        } else {\
            rate = (the_profiler->the_probe[P_COUNT] * 1000000 / the_profiler->the_probe[P_ELAPSE]);\
            cpu = the_profiler->the_probe[P_ELAPSE] / the_profiler->the_probe[P_COUNT];\
            throughput = (the_profiler->the_probe[P_BYTES] / 1024 /1024 * 1000000 / the_profiler->the_probe[P_ELAPSE]);\
        }\
	*pChar++ = ' ';\
	pChar += rozofs_string_padded_append(pChar, 17, rozofs_left_alignment, #the_probe);\
	*pChar++ = '|'; \
	*pChar++ = ' '; \
	pChar += rozofs_u64_padded_append(pChar, 15, rozofs_right_alignment, the_profiler->the_probe[P_COUNT]);\
	*pChar++ = ' ';*pChar++ = '|'; *pChar++ = ' ';\
	pChar += rozofs_u64_padded_append(pChar, 12, rozofs_right_alignment, rate);\
	*pChar++ = ' ';*pChar++ = '|';*pChar++ = ' ';\
	pChar += rozofs_u64_padded_append(pChar, 12, rozofs_right_alignment, cpu);\
	*pChar++ = ' ';*pChar++ = '|';*pChar++ = ' ';\
	pChar += rozofs_u64_padded_append(pChar, 20, rozofs_right_alignment, the_profiler->the_probe[P_BYTES]);\
	*pChar++ = ' ';*pChar++ = '|';*pChar++ = ' ';\
	pChar += rozofs_u64_padded_append(pChar, 16, rozofs_right_alignment, throughput);\
	*pChar++ = ' ';*pChar++ = '|';\
	pChar += rozofs_eol(pChar); \
    }
#define sp_display_io_probe_cond(the_profiler, the_probe)\
    {\
        if (the_profiler->the_probe[P_COUNT] != 0) {\
	   sp_display_io_probe(the_profiler, the_probe)\
        }\
    }  
    
#define sp_clear_io_probe(the_profiler, the_probe)\
    {\
       the_profiler->the_probe[P_COUNT] = 0;\
       the_profiler->the_probe[P_ELAPSE] = 0;\
       the_profiler->the_probe[P_BYTES] = 0;\
    }
static char * show_profile_storaged_io_display_help(char * pChar) {
  pChar += rozofs_string_append(pChar,"usage:\nprofiler reset       : reset statistics\nprofiler             : display statistics\n");  
  return pChar; 
}
static void show_profile_storaged_io_display(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    time_t elapse;
    int days, hours, mins, secs;
    time_t  this_time = time(0);


    // Compute uptime for storaged process
    elapse = (int) (this_time - gprofiler->uptime);
    days = (int) (elapse / 86400);
    hours = (int) ((elapse / 3600) - (days * 24));
    mins = (int) ((elapse / 60) - (days * 1440) - (hours * 60));
    secs = (int) (elapse % 60);

    pChar += sprintf(pChar, "GPROFILER version %s uptime =  %d days, %2.2d:%2.2d:%2.2d\n", gprofiler->vers,days, hours, mins, secs);

    // Print header for operations profiling values for storaged
    pChar += rozofs_string_append(pChar, "                  |      CALL       | RATE(msg/s)  |   CPU(us)    |        COUNT(B)      | THROUGHPUT(MB/s) |\n");
    pChar += rozofs_string_append(pChar, "------------------+-----------------+--------------+--------------+----------------------+------------------+\n");


    // Print master storaged process profiling values
    sp_display_io_probe(gprofiler, read);
    sp_display_io_probe(gprofiler, write);
    sp_display_io_probe(gprofiler, write_empty);
    sp_display_io_probe_cond(gprofiler, truncate);
    sp_display_io_probe_cond(gprofiler, repair);
    sp_display_io_probe_cond(gprofiler, remove);
    sp_display_io_probe_cond(gprofiler, rebuild_start);
    sp_display_io_probe_cond(gprofiler, rebuild_stop);
    sp_display_io_probe_cond(gprofiler, remove_chunk);
    sp_display_io_probe_cond(gprofiler, clear_error); 
    
    if (argv[1] != NULL)
    {
      if (strcmp(argv[1],"reset")==0) {
	sp_clear_io_probe(gprofiler, read);
	sp_clear_io_probe(gprofiler, write);
	sp_clear_io_probe(gprofiler, write_empty);
	sp_clear_io_probe(gprofiler, truncate);
	sp_clear_io_probe(gprofiler, repair);
	sp_clear_io_probe(gprofiler, remove);
	sp_clear_io_probe(gprofiler, rebuild_start);
	sp_clear_io_probe(gprofiler, rebuild_stop);
	sp_clear_io_probe(gprofiler, remove_chunk);
	sp_clear_io_probe(gprofiler, clear_error);
	pChar += sprintf(pChar,"Reset Done\n");  
	gprofiler->uptime = this_time;  	      
      }
      else {
        pChar = show_profile_storaged_io_display_help(pChar);
      }          
    }
            
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}

// For trace purpose
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

uint32_t ruc_init(uint32_t test, storaged_start_conf_param_t *arg_p) {
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
           int     idx;
           int      bindOnAnyAddr = 0;
           uint32_t ip;
           char target[256];
           sprintf(target, "storio:%d", arg_p->instance_id);            

           /*
           ** Get number of configured IP addresses in config file
           */           
           int nbAddr = sconfig_get_nb_IP_address(&storaged_config);
           
           for (idx=0; idx< nbAddr; idx++) {
              ip = sconfig_get_this_IP(&storaged_config,idx);
              if (ip == INADDR_ANY) {
                bindOnAnyAddr = 1;
              }           
	      uma_dbg_init_no_system(10, ip, arg_p->debug_port, target);	                 
           }
           /*
           ** When no configuration file is given, one uses the default config file.
           ** Only one storaged and one storio of each instance can exist on this node.
           ** One can listen on 127.0.0.1 for rozodiag commands       
           */
           if ((strcmp(storaged_config_file,STORAGED_DEFAULT_CONFIG) == 0)
           &&  (bindOnAnyAddr == 0)) {         
	      uma_dbg_init_no_system(10, 0x7F000001, arg_p->debug_port, target);	                              
           }
        } 
        /*
        ** In case there is a limited number of storio
        ** listen on every required port
        */
        if (storaged_config.storio_nb != 0) {
          uint64_t  bitmask[4] = {0};
          uint8_t   cid,rank,bit; 
          list_t   *l;
         
          memset(bitmask, 0, sizeof(bitmask));
          rank = (arg_p->instance_id-1)/64;
	  bit  = (arg_p->instance_id-1)%64; 
          bitmask[rank] |= (1ULL<<bit);

          /* For each storage on configuration file */
          l = NULL;
          list_for_each_forward(l, &storaged_config.storages) {

            storage_config_t *sc = list_entry(l, storage_config_t, list);
	    cid = sc->cid;

            /* Is this storage already started */
	    rank = (cid-1)/64;
	    bit  = (cid-1)%64; 
	    if (bitmask[rank] & (1ULL<<bit)) {
	      continue;
	    }

	    bitmask[rank] |= (1ULL<<bit);                     
            {
               int     idx;
               int      bindOnAnyAddr = 0;
               uint32_t ip;

               /*
               ** Get number of configured IP addresses in config file
               */           
               int nbAddr = sconfig_get_nb_IP_address(&storaged_config);

               for (idx=0; idx< nbAddr; idx++) {
                  ip = sconfig_get_this_IP(&storaged_config,idx);
                  if (ip == INADDR_ANY) {
                    bindOnAnyAddr = 1;
                  }           
	          uma_dbg_add_listening_port(ip, arg_p->debug_port + sc->cid - arg_p->instance_id);	                 
               }
               /*
               ** When no configuration file is given, one uses the default config file.
               ** Only one storaged and one storio of each instance can exist on this node.
               ** One can listen on 127.0.0.1 for rozodiag commands       
               */
               if ((strcmp(storaged_config_file,STORAGED_DEFAULT_CONFIG) == 0)
               &&  (bindOnAnyAddr == 0)) {         
	          uma_dbg_add_listening_port(0x7F000001, arg_p->debug_port + sc->cid - arg_p->instance_id);	                              
               }
            } 
          }
        }                       
        {
            char name[256];
            char * pChar = name;
            
            pChar += sprintf(pChar, "storio%d ", arg_p->instance_id);
            pChar += rozofs_ipv4_append(pChar,sconfig_get_this_IP(&storaged_config,0));
            uma_dbg_set_name(name);
        }
	/*
	** init of the RDMA in server mode
	*/
#ifdef ROZOFS_RDMA
	ret = rozofs_rdma_init(ROZO_SOCKCTRL_CTX_STORIO,0,common_config.storio_buf_cnt+ROZOFS_MAX_SRQ_WR, ROZOFS_RPC_RDMA_MSG_SZ,storio_rdma_msg_recv_form_cq_cbk);
	if (ret < 0)
	{
	  severe("fail to initialize RDMA");
	}
	ret = RUC_OK;	
#endif	

        /*
        ** RPC SERVER MODULE INIT
        */
        ret = rozorpc_srv_module_init_ctx_only(common_config.storio_buf_cnt);
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
int storio_start_nb_th(void *args) {
  int ret;
  storaged_start_conf_param_t *args_p = (storaged_start_conf_param_t*) args;
  int size = 0;
  char IPString[20];

  rozofs_ipv4_append(IPString,sconfig_get_this_IP(&storaged_config,0));

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
  ** Start self healing process
  */
  sprintf(storio_selfhealing_command_file,"%s/storio_%d.%s.selfhealing.args", common_config.device_automount_path, args_p->instance_id, IPString);
  sprintf(storio_selfhealing_result_file,"%s/storio_%d.%s.selfhealing.status", common_config.device_automount_path, args_p->instance_id, IPString);
  // Launch process throufh rozo launcher
  {
    char cmd[256];
    char pidfile[128];
    storio_selfhealing_pid_file(pidfile, IPString, args_p->instance_id);
    sprintf(cmd,"storio_selfhealing -a %s",storio_selfhealing_command_file);
    ret = rozo_launcher_start(pidfile, cmd);
    if (ret !=0) {
      severe("rozo_launcher_start(%s,%s) %s",pidfile, cmd, strerror(errno));
    }  
  }  

  /*
  ** Create a buffer pool to decode spproto RPC requests
  */
  size = sizeof(sp_write_arg_no_bins_t);
  if (size < sizeof(sp_write_repair3_arg_no_bins_t)) size = sizeof(sp_write_repair3_arg_no_bins_t);
  if (size < sizeof(sp_read_arg_t)) size = sizeof(sp_read_arg_t);
  if (size < sizeof(sp_truncate_arg_no_bins_t)) size = sizeof(sp_truncate_arg_no_bins_t);
  if (size < sizeof(sp_remove_arg_t)) size = sizeof(sp_remove_arg_t);
  if (size < sizeof(sp_rebuild_start_arg_t)) size = sizeof(sp_rebuild_start_arg_t);
  if (size < sizeof(sp_rebuild_stop_arg_t)) size = sizeof(sp_rebuild_stop_arg_t);
  if (size < sizeof(sp_remove_chunk_arg_t)) size = sizeof(sp_remove_chunk_arg_t);
  if (size < sizeof(sp_clear_error_arg_t)) size = sizeof(sp_clear_error_arg_t);
 if (size < sizeof(sp_read_rdma_arg_t)) size = sizeof(sp_read_rdma_arg_t);
  
  /*
  ** align the size on 8 bytes 
  */
  if ((size % 8 )!=0) size = ((size/8)*8) + 8;
  decoded_rpc_buffer_size = size;
  /*
  ** add an extra length on buffer to process RDMA read/write requests
  */
  size +=ROZOFS_DECODED_BUF_RDMA_EXTRA_SIZE;

  decoded_rpc_buffer_pool = ruc_buf_poolCreate(common_config.storio_buf_cnt,size);
  if (decoded_rpc_buffer_pool == NULL) {
    fatal("Can not allocate decoded_rpc_buffer_pool");
    return -1;
  }
  ruc_buffer_debug_register_pool("rpcDecodedRequest",decoded_rpc_buffer_pool);

  /*
  ** Initialize the disk thread interface and start the disk threads
  */	
  ret = storio_disk_thread_intf_create(IPString,args_p->instance_id, common_config.nb_disk_thread) ;
  if (ret < 0) {
    fatal("storio_disk_thread_intf_create");
    return -1;
  }
#ifdef ROZOFS_RDMA
  ret = storio_rdma_rpc_cmd_intf_create(IPString,args_p->instance_id) ;
  if (ret < 0) {
    fatal("storio_rdma_rpc_cmd_intf_create");
    return -1;
  }
  ret = storio_rdma_write_disk_thread_create(IPString,common_config.nb_write_rdma_threads,args_p->instance_id) ;
  if (ret < 0) {
    fatal("storio_rdma_write_disk_thread_create");
    return -1;
  }
#endif
  /*
  ** Init of the north interface (read/write request processing)
  */ 
  ret = storio_north_interface_buffer_init(common_config.storio_buf_cnt, STORIO_BUF_RECV_SZ);
  if (ret < 0) {
    fatal("Fatal error on storio_north_interface_buffer_init()\n");
    return -1;
  }
  ret = storio_north_interface_init(IPString,args_p->instance_id);
  if (ret < 0) {
    fatal("Fatal error on storio_north_interface_init()\n");
    return -1;
  }

  /*
  ** Initialize lookup table : FID -> device mapping
  */
  storio_device_mapping_init();
  
  /*
  ** A timing counter service
  */ 
  detailed_counters_init();
  serialization_counters_init();
  /*
  ** init of the fd cache
  */
  storage_fd_cache_init(48,16);

  /*
  ** add profiler subject 
  */
  uma_dbg_addTopic_option("profiler", show_profile_storaged_io_display,UMA_DBG_OPTION_RESET);
  
  uma_dbg_addTopicAndMan("repair", storio_repair_stat_cli, storio_repair_stat_man, UMA_DBG_OPTION_RESET);

  info("storio started (instance: %d, host: %s, dbg port: %d).",
                args_p->instance_id, IPString, args_p->debug_port);

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
  while (1) {
      ruc_sockCtrl_selectWait();
  }
  fatal("Exit from ruc_sockCtrl_selectWait()");
  fdl_debug_loop(__LINE__);
}
