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
#include <limits.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/types.h>
#include <rozofs/core/ruc_common.h>
#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/rozo_launcher.h>
#include <rozofs/core/rozofs_core_files.h>
#include <rozofs/core/ruc_tcpServer_api.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include "rozo_balance.h"


int volatile expgwc_non_blocking_thread_started;

char *display_delay(char *pChar,int64_t delay)
{
    if (delay < 0)
    {
      pChar +=sprintf(pChar,"No delay specified\n");
      return pChar;
    }
    if (delay >= (3600*24))
    {
      pChar +=sprintf(pChar,"%lld days\n",(long long int)delay/(3600*24));
      return pChar;
    }
    pChar +=sprintf(pChar,"%lld minutes\n",(long long int)delay/60);
    return pChar;    
}
/*
**
*/

static char *get_file_mode(int file_mode)
{
   switch (file_mode)
   {
   case  REBALANCE_MODE_REL:
     return "Relative pathname";
   case REBALANCE_MODE_ABS:
     return "Full pathname";
   case REBALANCE_MODE_FID:
     return "Fid";
   default:
     break;
   }
   return "?unknown?";
}
char *show_conf_with_buf(char * buf)
{
     char *pChar = buf;
     char buffer[128];
     char buffer2[128];
      rozo_balancing_ctx_t *p = &rozo_balancing_ctx;
     
     pChar +=sprintf(pChar,"export configuration file  :  %s\n",p->configFileName);
     pChar +=sprintf(pChar,"verbose mode               :  %s\n",(p->verbose==0)?"Disabled":"Enabled");
     pChar +=sprintf(pChar,"file mode                  :  %s\n",get_file_mode(p->file_mode));
     pChar +=sprintf(pChar,"volume identifier          :  %d\n",p->volume_id);
     pChar +=sprintf(pChar,"polling frequenccy (secs)  :  %d\n",p->rebalance_frequency);
     pChar +=sprintf(pChar,"file mover throughput      :  %d MB/s\n",p->throughput);
     pChar +=sprintf(pChar,"rebalance free trigger     :  %d\n",p->rebalance_threshold_trigger);
     pChar +=sprintf(pChar,"rebalance threshold_config :  %d\n",p->rebalance_threshold_config);
     pChar +=sprintf(pChar,"rebalance threshold_curr   :  %d\n",p->rebalance_threshold);
     pChar +=sprintf(pChar,"max files move             :  %d\n",p->max_scanned);
     pChar +=sprintf(pChar,"max move size (cur/max)    :  %s/%s\n",display_size_not_aligned((long long unsigned int)p->max_move_size_config,buffer),
                                                                display_size_not_aligned((long long unsigned int)p->cur_move_size,buffer2));
     pChar +=sprintf(pChar,"min. file size             :  %s\n",display_size_not_aligned((long long unsigned int)p->filesize_config,buffer));
     pChar +=sprintf(pChar,"\n");
     pChar +=sprintf(pChar,"file read retry            :  V%d/C%d\n",p->eagain_volume,p->eagain_cluster);
     pChar +=sprintf(pChar,"newer delay                :  ");
     pChar =display_delay(pChar,p->newer_time_sec_config);
     pChar +=sprintf(pChar,"older delay                :  ");
     pChar = display_delay(pChar,p->older_time_sec_config);
     pChar +=sprintf(pChar,"time cpt(old/in range/new) : %lld/%lld/%lld\n",(long long  int)p->time_older_cpt,
                                                                           (long long  int)p->time_inrange_cpt,
									   (long long  int)p->time_newer_cpt);
     pChar +=sprintf(pChar,"scanned file (cur/total)   : %lld/%lld\n",(long long  int)p->current_scanned_file_cpt,
                                                                           (long long  int)p->scanned_file_cpt);
     return pChar;
}

void show_conf(char * argv[], uint32_t tcpRef, void *bufRef)
{
     char *pChar = uma_dbg_get_buffer();
      
     show_conf_with_buf(pChar);     
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
}
/*
**_______________________________________________________________________
*/
void show_cluster(char * argv[], uint32_t tcpRef, void *bufRef)
{
     char *pChar = uma_dbg_get_buffer();

    rozo_display_all_cluster_with_buf(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
}
/*
**_______________________________________________________________________
*/
void show_balancing(char * argv[], uint32_t tcpRef, void *bufRef)
{
     char *pChar = uma_dbg_get_buffer();

    display_all_cluster_balancing_stats(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
}

/*
**_______________________________________________________________________
*/
/**
*  Get a size 
  supported formats:
   1/1b/1B (bytes)
   1k/1K (Kilobytes)
   1m/1M (Megabytes)  
   1g/1G (Gigabytes)  
   
   @param str: string that contains the value to convert
   @param value: pointer to the converted value
   
   @retval 0 on success
   @retval -1 on error (see errno for details)
*/
int get_size_value(char *str,uint64_t *value)
{
     uint64_t val64;
     int err = 0;
     char *s;
     
     s=str;

      val64 = strtoull(str, &str, 10);
      if (s == str || (val64 == ULONG_MAX && errno == ERANGE)) {
 	      err = 1;
      }
      if (err)
      {
        errno = EINVAL;
	return -1;
      }
      if (*str != 0)
      {
          while(1)
	  {
            if ((*str == 'k') || (*str == 'K'))
	    {
	       val64*=1024;
	       break;
            }
            if ((*str == 'm')|| (*str == 'M'))
	    {
	       val64*=(1024*1024);
	       break;
            }
            if ((*str == 'g')|| (*str == 'G'))
	    {
	       val64*=(1024*1024*1024);
	       break;
            }
	    err = 1;
	    break;
	  } 
	  if (str[1] != 0) err=1;     
      }
      if (err )
      {
        errno=EINVAL;
	return -1;
      } 
      errno = 0;
      *value = val64;
      return 0;
 }
/*
**_______________________________________________________________________
*/
static char * set_balancing_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"set_balancing threshold <value>            : set the re-balancing threshold percentage (0..100)\n");
  pChar += sprintf(pChar,"set_balancing verbose <enable|disable>     : enable or disable verbose mode\n");
  pChar += sprintf(pChar,"set_balancing file_mode <rel|abs|fid>      : file naming: relative path/full path or fid mode\n");
  pChar += sprintf(pChar,"set_balancing frequency <value>            : set the re-balancing polling frequency (unit in seconds)\n");
  pChar += sprintf(pChar,"set_balancing trigger <value>              : set the re-balancing trigger (free percentage on storage)\n");
  pChar += sprintf(pChar,"set_balancing move_count <value>           : set the maximum of selected files before triggering a move\n");
  pChar += sprintf(pChar,"set_balancing move_size <value>            : set the maximum bytes size to move in one polling\n");
  pChar += sprintf(pChar,"set_balancing filesize <value>[k|K|m|M|g|G]: set the minimum selectable file size (B/KB/MB/GB)\n");
  pChar += sprintf(pChar,"set_balancing throughput <value>           : set the maximum file throughput (unit in MB/s)\n");
  return pChar; 
}


void set_balancing(char * argv[], uint32_t tcpRef, void *bufRef)
{
     char *pChar = uma_dbg_get_buffer();
     int value;
     int ret;
     uint64_t val64;
     rozo_balancing_ctx_t *p = &rozo_balancing_ctx;
      
      if (argv[1] == NULL) {
	set_balancing_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;  	  
    }
    if (strcmp(argv[1],"verbose")==0) 
    {   
      if (argv[2] == NULL) 
      {
	set_balancing_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;  	  
      }
      if (strcmp(argv[2],"enable")==0){
        p->verbose = 1;
        sprintf(pChar,"verbose is enabled\n");	
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
        return;   
      }
      if (strcmp(argv[2],"disable")==0){
        p->verbose = 0;
        sprintf(pChar,"verbose is disabled\n");	
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
        return;   
      }
      pChar +=sprintf(pChar,"unsupported option: %s\n",argv[2]);
      set_balancing_help(pChar);	
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;  
    }
    if (strcmp(argv[1],"file_mode")==0) 
    {   
      if (argv[2] == NULL) 
      {
	set_balancing_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;  	  
      }
      if (strcmp(argv[2],"rel")==0){
        p->file_mode = REBALANCE_MODE_REL;
        sprintf(pChar,"relative path mode is enabled\n");	
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
        return;   
      }
      if (strcmp(argv[2],"abs")==0){
        p->file_mode = REBALANCE_MODE_ABS;
        sprintf(pChar,"fullpath mode is enabled\n");	
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
        return;   
      }
      if (strcmp(argv[2],"fid")==0){
        p->file_mode = REBALANCE_MODE_FID;
        sprintf(pChar,"fid mode is enabled\n");	
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
        return;   
      }
      pChar +=sprintf(pChar,"unsupported option: %s\n",argv[2]);
      set_balancing_help(pChar);	
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;  
    }
    if (strcmp(argv[1],"threshold")==0) 
    {   
      if (argv[2] == NULL) 
      {
	set_balancing_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;  	  
      }
      ret = sscanf(argv[2], "%d", &value);
      if (ret != 1) 
      {
        set_balancing_help(pChar);	
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
        return;   
      }
      if (value > 100) 
      {
        pChar+=sprintf(pChar,"Out of range value %d\n",value);
        set_balancing_help(pChar);	
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
        return; 
      }
      p->rebalance_threshold_config = value;
      pChar+=sprintf(pChar,"threshold value is now %d\n",value);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;   
    }
    if (strcmp(argv[1],"frequency")==0) 
    {   
      if (argv[2] == NULL) 
      {
	set_balancing_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;  	  
      }
      ret = sscanf(argv[2], "%d", &value);
      if (ret != 1) 
      {
        set_balancing_help(pChar);	
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
        return;   
      }
      if (value < 0) 
      {
        pChar+=sprintf(pChar,"Out of range value %d\n",value);
        set_balancing_help(pChar);	
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
        return; 
      }
      p->rebalance_frequency = value;
      pChar+=sprintf(pChar,"polling frequency is now %d seconds\n",value);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;   
    } 	  
    if (strcmp(argv[1],"throughput")==0) 
    {   
      if (argv[2] == NULL) 
      {
	set_balancing_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;  	  
      }
      ret = sscanf(argv[2], "%d", &value);
      if (ret != 1) 
      {
        set_balancing_help(pChar);	
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
        return;   
      }
      if (value < 0) 
      {
        pChar+=sprintf(pChar,"Out of range value %d\n",value);
        set_balancing_help(pChar);	
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
        return; 
      }
      p->throughput = value;
      pChar+=sprintf(pChar,"max file throughput is now %d MB/s\n",value);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;   
    }
    if (strcmp(argv[1],"move_count")==0) 
    {   
      if (argv[2] == NULL) 
      {
	set_balancing_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;  	  
      }
      ret = sscanf(argv[2], "%d", &value);
      if (ret != 1) 
      {
        set_balancing_help(pChar);	
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
        return;   
      }
      if (value <= 0) 
      {
        pChar+=sprintf(pChar,"Out of range value %d\n",value);
        set_balancing_help(pChar);	
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
        return; 
      }
      p->max_scanned = value;
      pChar+=sprintf(pChar,"max files move is now %d\n",value);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;   
    } 	  
    
    if (strcmp(argv[1],"filesize")==0) 
    {   
      if (argv[2] == NULL) 
      {
	set_balancing_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;  	  
      }
      ret = get_size_value(argv[2],&val64);
      if (ret < 0)
      {
        pChar += sprintf(pChar,"error %s\n",strerror(errno));
	set_balancing_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;        
      }
      p->filesize_config = val64;
      pChar+=sprintf(pChar,"filesize value is now %llu Bytes\n",(long long unsigned int)p->filesize_config);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;   
    }

    if (strcmp(argv[1],"move_size")==0) 
    {   
      if (argv[2] == NULL) 
      {
	set_balancing_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;  	  
      }
      ret = get_size_value(argv[2],&val64);
      if (ret < 0)
      {
        pChar += sprintf(pChar,"error %s\n",strerror(errno));
	set_balancing_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;        
      }
      p->max_move_size_config = val64;
      pChar+=sprintf(pChar,"Max. move size value is now %llu Bytes\n",(long long unsigned int)p->max_move_size_config);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;   
    }

    set_balancing_help(pChar);	
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
}

/*
**_______________________________________________________________________
*/
uint32_t ruc_init(uint32_t test,uint16_t dbg_port,uint16_t exportd_instance) {
  int ret;


  uint32_t        mx_tcp_client = 2;
  uint32_t        mx_tcp_server = 2;
  uint32_t        mx_tcp_server_cnx = 10;
  uint32_t        mx_af_unix_ctx = 1024;
  uint32_t        mx_sockctrl_cnt=128;

  /*
  ** init of the system ticker
  */
  rozofs_init_ticker();
  /*
  ** trace buffer initialization
  */
  ruc_traceBufInit();
#if 1
 /*
 ** Not needed since there is already done
 ** by libUtil
 */

 /* catch the sigpipe signal for socket 
 ** connections with RELC(s) in this way when a RELC
 ** connection breaks an errno is set on a recv or send 
 **  socket primitive 
 */ 
  struct sigaction sigAction;
  
  sigAction.sa_flags=SA_RESTART;
  sigAction.sa_handler = SIG_IGN; /* Mask SIGPIPE */
  if(sigaction (SIGPIPE, &sigAction, NULL) < 0) 
  {
    exit(0);    
  }
#if 0
  sigAction.sa_flags=SA_RESTART;
  sigAction.sa_handler = hand; /*  */
  if(sigaction (SIGUSR1, &sigAction, NULL) < 0) 
  {
    exit(0);    
  }
#endif
#endif

   /*
   ** initialize the socket controller:
   **   for: NPS, Timer, Debug, etc...
   */
//#warning set the number of contexts for socketCtrl to 1024
   ret = ruc_sockctl_init(mx_sockctrl_cnt);
   if (ret != RUC_OK)
   {
     fatal( " socket controller init failed %d",mx_sockctrl_cnt );
   }

   /*
   **  Timer management init
   */
   ruc_timer_moduleInit(FALSE);

   while(1)
   {
     /*
     **--------------------------------------
     **  configure the number of TCP connection
     **  supported
     **--------------------------------------   
     **  
     */ 
     ret = uma_tcp_init(mx_tcp_client+mx_tcp_server+mx_tcp_server_cnx);
     if (ret != RUC_OK) break;

     /*
     **--------------------------------------
     **  configure the number of TCP server
     **  context supported
     **--------------------------------------   
     **  
     */    
     ret = ruc_tcp_server_init(mx_tcp_server);
     if (ret != RUC_OK) break;

     /*
     **--------------------------------------
     **  configure the number of AF_UNIX
     **  context supported
     **--------------------------------------   
     **  
     */    
     ret = af_unix_module_init(mx_af_unix_ctx,
                               2,1024*1, // xmit(count,size)
                               2,1024*1 // recv(count,size)
                               );
     break;   
   }
   /*
   ** internal debug init
   */
   //ruc_debug_init();


     /*
     **--------------------------------------
     **   D E B U G   M O D U L E
     **--------------------------------------
     */

     uma_dbg_init(10,INADDR_ANY,dbg_port);

    {
        char name[32];
        sprintf(name, "rebalance-%d ",  exportd_instance);	
        uma_dbg_set_name(name);
    }

//#warning Start of specific application initialization code
 

 return ret;
}



/**
*  Init of the data structure used for the non blocking entity

  @retval 0 on success
  @retval -1 on error
*/
int rozo_rebalance_non_blocking_init(uint16_t dbg_port, uint16_t exportd_instance) {
  int   ret;
//  sem_t semForEver;    /* semaphore for blocking the main thread doing nothing */


 ret = ruc_init(FALSE,dbg_port,exportd_instance);
 
 if (ret != RUC_OK) return -1;
 
 
 return 0;

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

int rozo_rebalance_start_nb_blocking_th(void *args) {


    int ret;
    //sem_t semForEver;    /* semaphore for blocking the main thread doing nothing */
    rozo_balancing_ctx_t *args_p = (rozo_balancing_ctx_t*)args;

    uma_dbg_thread_add_self("Non bloking");
 
    
    ret = rozo_rebalance_non_blocking_init(args_p->debug_port, args_p->instance);
    if (ret != RUC_OK) {
        /*
         ** fatal error
         */
         fatal("can't initialize non blocking thread");
        return -1;
    }


    info("Rebalance non-blocking thread running (instance: %d, port: %d).",
            args_p->instance, args_p->debug_port);
    
    /*
     ** main loop
     */
    rozo_balance_non_blocking_thread_started = 1;
     uma_dbg_addTopic("config",show_conf);
     uma_dbg_addTopic("show_cluster",show_cluster);
     uma_dbg_addTopic("show_balancing",show_balancing);
     uma_dbg_addTopic("set_balancing",set_balancing);
     

    while (1) {
        ruc_sockCtrl_selectWait();
    }
}


