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
#define FUSE_USE_VERSION 26


#include <stdlib.h>
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

#include <rozofs/rozofs.h>
#include <config.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/common/log.h>
//#include "file.h"
#include <rozofs/common/htable.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/rpc/sclient.h>
#include <rozofs/rpc/mclient.h>
#include <rozofs/rpc/eproto.h>
#include <rozofs/core/rozofs_tx_common.h>
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/rpc/sproto.h>
#include "rozofs_storcli.h"
#include "storage_proto.h"
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/rozofs_srv.h>
#include "rozofs_storcli_rpc.h"
#include "rozofs_storcli_lbg_cnf_supervision.h"
#include "storcli_main.h"
#include <rozofs/rdma/rozofs_rdma.h>
#include "rdma_client_send.h"
#include "standalone_client_send.h"

/*
**_________________________________________________________________________
*      PUBLIC FUNCTIONS
**_________________________________________________________________________
*/








#include <rozofs/core/ruc_common.h>
#include <rozofs/core/ruc_sockCtl_api.h>
#include <rozofs/core/ruc_timer_api.h>
#include <rozofs/core/ruc_trace_api.h>
#include <rozofs/core/uma_tcp_main_api.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/ruc_tcpServer_api.h>
#include <rozofs/core/ruc_tcp_client_api.h>
#include <rozofs/core/uma_well_known_ports_api.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/north_lbg_api.h>


// For trace purpose
struct timeval     Global_timeDay;
unsigned long long Global_timeBefore, Global_timeAfter;

/*
**
*/


uint32_t ruc_init(uint32_t test,uint16_t dbg_port,uint16_t rozofsmount_instance)
{
  int ret;


  uint32_t        mx_tcp_client = 2;
  uint32_t        mx_tcp_server = 2;
  uint32_t        mx_tcp_server_cnx = 10;
  uint32_t        mx_af_unix_ctx = ROZO_AFUNIX_CTX_STORCLI;
  uint32_t        mx_lbg_north_ctx = 256;

//#warning TCP configuration ressources is hardcoded!!
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
   **   4 connections per Relc and 32
   **   for: NPS, Timer, Debug, etc...
   */
//#warning set the number of contexts for socketCtrl to 1024
   ret = ruc_sockctl_init(ROZO_SOCKCTRL_CTX_STORCLI);
   if (ret != RUC_OK)
   {
     fatal( " socket controller init failed" );
   }
#ifdef ROZOFS_RDMA
   /*
   ** init of the RDMA in client mode
   */
   rozofs_storcli_rdma_rpc_create_receive_socket();
   
   ret = rozofs_rdma_init(ROZO_SOCKCTRL_CTX_STORCLI,1,STORCLI_CTX_CNT*rozofs_get_rozofs_safe(conf.layout)+ROZOFS_MAX_SRQ_WR, ROZOFS_RPC_RDMA_MSG_SZ,storcli_rdma_msg_recv_form_cq_cbk);
   if (ret < 0)
   {
     severe("fail to initialize RDMA");
   }
#endif
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
#if 0
     /*
     **--------------------------------------
     **  configure the number of TCP client
     **  context supported
     **--------------------------------------   
     **  
     */    
     ret = ruc_tcp_clientinit(mx_tcp_client);
     if (ret != RUC_OK) break;   
#endif

     /*
     **--------------------------------------
     **  configure the number of AF_UNIX
     **  context supported
     **--------------------------------------   
     **  
     */    
     ret = af_unix_module_init(mx_af_unix_ctx,
                               32,1024*32, // xmit(count,size)
                               32,1024*32 // recv(count,size)
                               );
     if (ret != RUC_OK) break;   

     /*
     **--------------------------------------
     **  configure the number of Load Balancer
     **  contexts supported
     **--------------------------------------   
     **  
     */ 
     /*
     ** Check if the number of configured LBG is compatible with the lbg cnx supervision table  
     */
     if ( STORCLI_MAX_LBG < mx_lbg_north_ctx)
     {
       fatal("STORCLI_MAX_LBG constant need at least to be %d",mx_lbg_north_ctx);
     }
     ret = north_lbg_module_init(mx_lbg_north_ctx);
     if (ret != RUC_OK) break;  
#ifdef ROZOFS_RDMA
     ret = rdma_lbg_tmo_table_init(mx_lbg_north_ctx);
     if (ret != RUC_OK) break;        
#endif
     ret = standalone_lbg_tmo_table_init(mx_lbg_north_ctx);
     if (ret != RUC_OK) break; 
     /*
     ** init of the storage client structure
     */
     ret = rozofs_storcli_module_init();
     if (ret != RUC_OK) break; 
    
     ret = storcli_sup_moduleInit();
     if (ret != RUC_OK) break; 
     
     ret = rozofs_tx_module_init(STORCLI_SOUTH_TX_CNT,  // transactions count
                                 STORCLI_CNF_NO_BUF_CNT,STORCLI_CNF_NO_BUF_SZ,        // xmit small [count,size]
                                 STORCLI_CNF_NO_BUF_CNT,STORCLI_CNF_NO_BUF_SZ,  // xmit large [count,size]
                                 STORCLI_CNF_NO_BUF_CNT,STORCLI_CNF_NO_BUF_SZ,        // recv small [count,size]
                                 STORCLI_CNF_NO_BUF_CNT,STORCLI_CNF_NO_BUF_SZ);  // recv large [count,size];  

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

    {
        char name[64];
	if (conf.owner == NULL)
	{
          sprintf(name, "storcli %d of rozofsmount %d", conf.module_index, conf.rozofsmount_instance);
	}
	else
	{
          sprintf(name, "storcli %d of %s %d", conf.module_index,conf.owner, conf.rozofsmount_instance);	
	}
        uma_dbg_set_name(name);

        sprintf(name, "mount:%d:%d", conf.rozofsmount_instance, conf.module_index);
        uma_dbg_init(10,INADDR_ANY,dbg_port, name);        
    }     


//#warning Start of specific application initialization code
 

 return ret;
}


int rozofs_bt_stc_init(uint16_t rozofsmount_id,int storcli_id);

/**
*  Init of the data structure used for the non blocking entity

  @retval 0 on success
  @retval -1 on error
*/
int rozofs_storcli_non_blocking_init(uint16_t dbg_port, uint16_t rozofsmount_instance)
{
  int   ret;
//  sem_t semForEver;    /* semaphore for blocking the main thread doing nothing */


 ret = ruc_init(FALSE,dbg_port,rozofsmount_instance);
 
 if (ret != RUC_OK) return -1;
 
 ret = rozofs_bt_stc_init((int)conf.rozofsmount_instance,(int) conf.module_index);
 if (ret != RUC_OK) return -1;

 /*
 ** Start the timer to relaunch polling on c
 */
 storcli_lbg_cnx_sup_tmr_init(STORCLI_LBG_STORAGE_POLL_FREQ_MS);
 
 return 0;

}


