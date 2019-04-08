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
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>
#include <semaphore.h>
#include <errno.h>
#include "rozofs_rdma.h"
#include <pthread.h>
#include <sys/types.h>    
#include <sys/socket.h>
#include <sys/time.h>
#include <rozofs/common/log.h>
#include <rozofs/core/af_unix_socket_generic.h>
/**
*  Global data
*/
rozofs_rmda_ibv_cxt_t *rozofs_rmda_ibv_tb[ROZOFS_MAX_RDMA_ADAPTOR];
rozofs_rdma_memory_reg_t *rozofs_rdma_memory_reg_tb[ROZOFS_MAX_RDMA_MEMREG];
rozofs_rdma_connection_t **rozofs_rdma_tcp_table = NULL;   /**< table of the RDMA context associated with a TCP context                */
rozofs_rdma_tcp_cnx_t **tcp_cnx_tb = NULL;                /**< table of the pseudo TCP context associated with RozoFS AF_UNIX context  */
int       rozofs_nb_rmda_tcp_context = 0;
int       rozofs_cur_act_rmda_tcp_context = 0;    /**< current number of contexts                       */
int       rozofs_cur_del_rmda_tcp_context = 0;    /**< number of context waiting for final deletion     */
static const int TIMEOUT_IN_MS = 500; /* ms */
struct rdma_event_channel *rozofs_rdma_ec=NULL;    /**< rdma event channel used to manage the connection                            */  
int       rozofs_rdma_signaling_sockpair[2];       /**< index 0 is used by the RDMA signaling thread, index 1 is used by the client */
pthread_t rozofs_rdma_signaling_thread_ctx;        /**< context of the RDMA signaling thread                                        */
int       rozofs_rdma_signalling_thread_mode;
int       rozofs_rdma_signalling_thread_started=0;
uint16_t  rozofs_rdma_listening_port; 
rozofs_rdma_sig_th_statistics_t  rozofs_rdma_sig_th_stats; /**< statistics of the RDMA signaling thread */

list_t    rozofs_rdma_del_head_list;             /**< head of the linked list used to queue the context under deletion   */
pthread_rwlock_t rozofs_rdma_del_lock;          /**< lock associated with the deletion list                              */
int  rozofs_rdma_enable= 0;                    /**< set to 1 when RDMA is enabled: we should have at least one device that supports it */
void *rdma_sig_sockctrl_p=NULL;
uint64_t rozofs_deleted_rdma_tcp_contexts = 0;   /**< total number of deleted contexts     **/
uint64_t rozofs_allocated_rdma_tcp_contexts = 0; /**< total number of allocated contexts   **/
int rdma_sig_reconnect_credit = 0;              /**< credit for reconnect attempt (client side only)            */
int rdma_sig_reconnect_credit_conf = 0;             /**< configured credit for reconnect                       */
int rdma_sig_reconnect_period_conf = 0;             /**< configured rperiod for reconnect attempt credits       */
struct rdma_cm_id *rozofs_rdma_listener[ROZOFS_RDMA_MAX_LISTENER];   /**< rdma listener contexts (server side)  */
int rozofs_rdma_listener_count = 0;                                  /**< current number of listener            */
int rozofs_rdma_listener_max = 0;                                   /**< max number of listening contexts       */
rozofs_rdma_mod_stats_t rozofs_rdma_mod_stats;                    /**< RDMA module statistics                */
uint64_t rdma_err_stats[ROZOFS_IBV_WC_MAX_ERR+1] = {0} ;             /**< RDMA error counters */
int  rozofs_rdma_qp_next_idx=0;
void *rozofs_rdma_pool_rpc=NULL;   /**< pool for sending RPC messages */
pthread_rwlock_t rozofs_rdma_pool_rpc_lock;  /**< lock on the ruc_buffer pool that contains the RPC messages */
int  rozofs_max_rpc_buffer_srq_refill;            /**< max number of number to refill on the SRQ during the processing of one message  */
rozofs_rdma_pf_rpc_cqe_done_t  rozofs_rdma_rpc_cbk = NULL;  /**< user callback for the RPC message over RDMA */

rozofs_rdma_memory_reg_t rozofs_rdma_memreg_rpc;  /**< memory descriptor for the command   */
//rozofs_qc_th_t *rozofs_cq_th_post_send[ROZOFS_CQ_THREAD_NUM];  /**< thread context of the completion queue thread associated with post_send */
//rozofs_qc_th_t *rozofs_cq_th_post_recv[ROZOFS_CQ_THREAD_NUM];  /**< thread context of the completion queue thread associated with post_recv */
/*
** local prototypes
*/
int rozofs_rdma_srv_on_event(struct rdma_cm_event *event);
int rozofs_rdma_cli_on_event(struct rdma_cm_event *event);
void * rozofs_rmda_signaling_th(void *ctx);
void * rozofs_poll_cq_th(void *ctx);
void rozofs_rdma_srv_fsm_exec(rozofs_rdma_tcp_assoc_t *assoc,struct rdma_cm_event *event,rozofs_rdma_internal_event_e evt_code);
void rozofs_rdma_cli_fsm_exec(rozofs_rdma_tcp_assoc_t *assoc,struct rdma_cm_event *event,rozofs_rdma_internal_event_e evt_code);
void rozofs_rdma_del_process(time_t del_time);

/*
**__________________________________________________
*/
/**
   Create a RDMA listener
   
   That service must be invoked once RDMA module has been started
   
   @param ip_addr: IP address of the listener or ANY_ADDR
   @param port:  listening port
   
   @retval 0 on success
   @retval -1 on error
*/
int rozofs_rdma_listening_create(uint32_t ip,uint16_t port)
{

  struct sockaddr_in addr;  
   
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = htonl(ip);
  
  if (rozofs_rdma_enable == 0)
  {
     severe("RDMA service is not enabled");
     errno = ENOTSUP;;
      return -1;
  }
  /*
  ** check if the signalling channel has been created
  */
  if (rozofs_rdma_ec == NULL)
  {
     warning("RDMA service: no signalling channel available");
     errno = ENOTSUP;
     return -1;  
  
  }
  /*
  ** check if there is still context available
  */
  if (rozofs_rdma_listener_count >= rozofs_rdma_listener_max)
  {
     warning("RDMA service: max listening context has been reached (%d)",rozofs_rdma_listener_max);
     errno = ERANGE;
     return -1;    
  }
  /*
  ** create the CM_ID: Allocate a communication identifier
  */
  RDMA_STATS_REQ(rdma_create_id);
  if (rdma_create_id(rozofs_rdma_ec, &rozofs_rdma_listener[rozofs_rdma_listener_count], NULL, RDMA_PS_TCP) < 0)
  {
    
     warning("RDMA rdma_create_id failed for %x:%d; %s",ip,port,strerror(errno));
     RDMA_STATS_RSP_NOK(rdma_create_id);
     return -1;  
  }
  RDMA_STATS_RSP_OK(rdma_create_id);  
  /*
  ** bind the IP&port with the communication context
  */
  if (rdma_bind_addr(rozofs_rdma_listener[rozofs_rdma_listener_count], (struct sockaddr *)&addr)<0)
  {
     warning("RDMA rdma_bind_addr failed for %x:%d; %s",ip,port,strerror(errno));
     return -1;    
  }
  /*
  ** attempt to listen on that port
  */
  RDMA_STATS_REQ(rdma_listen);
  if (rdma_listen(rozofs_rdma_listener[rozofs_rdma_listener_count], ROZOFS_RDMA_BACKLOG_COUNT) < 0)
  {
     RDMA_STATS_RSP_NOK(rdma_listen);
     warning("RDMA rdma_listen failed for %x:%d; %s",ip,port,strerror(errno));
     return -1;      
  }
  RDMA_STATS_RSP_OK(rdma_listen);
  rozofs_rdma_listener_count++;
  return 0;


}
/*
**__________________________________________________
*/
/**  
*  Always ready
*/
uint32_t rdma_sig_rcvReadyTcpSock(void * timerRef,int socketId)
{
    return TRUE;
}

/*
**__________________________________________________
*/

uint32_t rdma_sig_rcvMsgTcpSock(void * timerRef,int socketId)
{
  rozofs_rdma_srv_af_unix_receive_tcp();
  return TRUE;
}
/*
**__________________________________________________
*/
uint32_t rdma_sig_xmitReadyTcpSock(void * timerRef,int socketId)
{
    return FALSE;
}
/*
**__________________________________________________
*/
uint32_t rdma_sig_xmitEvtTcpSock(void * timerRef,int socketId)
{
  return FALSE;

}
/*
**  Call back function for socket controller
*/
ruc_sockCallBack_t rdma_sig_callBack_TcpSock=
  {
     rdma_sig_rcvReadyTcpSock,
     rdma_sig_rcvMsgTcpSock,
     rdma_sig_xmitReadyTcpSock,
     rdma_sig_xmitEvtTcpSock
  };

/*
**__________________________________________________
*/
/*
**  FOR TEST ONLY !!!!!
*/
void post_receives(rozofs_rdma_connection_t *conn)
{
  return;
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  rozofs_rmda_ibv_cxt_t *s_ctx;
  struct ibv_mr  *mr_p; 
  rozofs_rdma_memory_reg_t *region_p;
   
  s_ctx = conn->s_ctx;
  mr_p = s_ctx->memreg[0];
  region_p = rozofs_rdma_memory_reg_tb[0];
  
  wr.wr_id = 0;
  wr.next = NULL;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)region_p->mem;
  sge.length = region_p->len;
  sge.lkey = mr_p->lkey;;
  
  printf("addr %llu len=%u key=%x\n",(unsigned long long int)sge.addr,sge.length,sge.lkey);

  if(ibv_post_recv(conn->qp, &wr, &bad_wr)!=0)
  {
     printf("ibv_post_recv error\n");
  }
}
/*
**  FOR TEST ONLY END !!!!!
*/
/*
**_______________________________________________________________________________________________________

      R D M A    R O Z O D I A G 
**_______________________________________________________________________________________________________
*/ 

  
void show_rdma_status(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
  
  
   pChar += sprintf(pChar,"%-35s :%s\n","RDMA adaptor presence", (rozofs_rdma_enable==0)?"No":"Yes");
   pChar += sprintf(pChar,"%-35s :%s\n","RDMA mode", (rozofs_rdma_signalling_thread_mode==0)?"Server":"Client");
   pChar += sprintf(pChar,"%-35s :%d\n","number of RDMA contexts", rozofs_nb_rmda_tcp_context);
   pChar += sprintf(pChar,"%-35s :%d\n","number of RDMA active contexts", rozofs_cur_act_rmda_tcp_context);
   pChar += sprintf(pChar,"%-35s :%d\n","number of RDMA deleted contexts", rozofs_cur_del_rmda_tcp_context);   
   pChar += sprintf(pChar,"%-35s :%llu\n","RDMA deleted contexts gauge", (unsigned long long int)rozofs_deleted_rdma_tcp_contexts);
   pChar += sprintf(pChar,"%-35s :%llu\n","RDMA allocated contexts gauge",(unsigned long long int) rozofs_allocated_rdma_tcp_contexts);
   pChar += sprintf(pChar,"%-35s :%d/%d\n","signalling RDMA sockets", rozofs_rdma_signaling_sockpair[0],rozofs_rdma_signaling_sockpair[1]);
   pChar += sprintf(pChar,"%-35s :%s\n","signalling RDMA thread",(rozofs_rdma_signalling_thread_started==0)?"Not started":"Running" );

  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
} 
/*
**__________________________________________________
*/

static char * show_rdma_stats_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"rdma_statistics reset  : reset statistics\n");
  pChar += sprintf(pChar,"rdma_statistics        : display statistics\n");  
  return pChar; 
}


#define SHOW_RDMA_STATS(probe) pChar += sprintf(pChar," %-26s | %15llu | %15llu | %15llu |\n",\
                    #probe,\
                    (unsigned long long int)rozofs_rdma_mod_stats.probe[0],\
                    (unsigned long long int)rozofs_rdma_mod_stats.probe[1],\
                    (unsigned long long int)rozofs_rdma_mod_stats.probe[2] );


void show_rdma_statistics(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();



    pChar += sprintf(pChar, " - active Contexts: %d\n", rozofs_cur_act_rmda_tcp_context);
    pChar += sprintf(pChar, "      RDMA procedure        |     count       |     Success     |     Failure     |\n");
    pChar += sprintf(pChar, "----------------------------+-----------------+-----------------+-----------------+\n");

    SHOW_RDMA_STATS( rdma_connect);             /**< RDMA connect request : rdma_connect()          */
    SHOW_RDMA_STATS( rdma_reconnect);           /**< RDMA reconnect : number of client  attempts    */
    SHOW_RDMA_STATS( rdma_disconnect);          /**< RDMA disconnect : rdma_disconnect()            */
    SHOW_RDMA_STATS( rdma_accept);              /**< RDMA accept rdma_accept()                      */
    SHOW_RDMA_STATS( rdma_reject);              /**< RDMA reject rdma_reject()                      */
    SHOW_RDMA_STATS( rdma_resolve_addr);        /**< RDMA resolve destination address               */
    SHOW_RDMA_STATS( rdma_resolve_route);       /**< RDMA resolve destination route                 */
    SHOW_RDMA_STATS( ibv_reg_mr);               /**< RDMA memory registration : ibv_reg_mr()        */
    SHOW_RDMA_STATS( rdma_create_qp);           /**< RDMA queue pair creation  rdma_create_qp()     */
    SHOW_RDMA_STATS( rdma_destroy_qp);          /**< queue pair deletion :rdma_destroy_qp()         */
    SHOW_RDMA_STATS( ibv_modify_qp);            /**< RDMA queue pair modification:ibv_modify_qp()   */
    SHOW_RDMA_STATS( ibv_alloc_pd);             /**< RDMA Policy descriptor allocation              */
    SHOW_RDMA_STATS( ibv_create_comp_channel);  /**< RDMA completion channel creation               */
    SHOW_RDMA_STATS( ibv_create_cq);         /**< RDMA completion queue  creation               */
    SHOW_RDMA_STATS( rdma_create_id);            /**< cm_id context creation: rdma_create_id()      */
    SHOW_RDMA_STATS( rdma_destroy_id);            /**< cm_id contextdeletion: rdma_destroy_id       */
    SHOW_RDMA_STATS( signalling_sock_create);  /**<signalling socket creation               */
    SHOW_RDMA_STATS( rdma_listen);            /**<number of RDMA listen                     */
    SHOW_RDMA_STATS( ibv_create_srq);            /**<number of RDMA share queues             */
    SHOW_RDMA_STATS( ibv_post_srq_recv);            /**<number of RDMA post received on shared queue    */
    if (argv[1] != NULL)
    {
      if (strcmp(argv[1],"reset")==0) {
        memset(&rozofs_rdma_mod_stats,0,sizeof(rozofs_rdma_mod_stats));
      }
      else {
	/*
	** Help
	*/
	pChar = show_rdma_stats_help(pChar);
      }
    }    
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}


/*
**__________________________________________________
*/

static char * show_rdma_err_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"rdma_err reset  : reset error counters\n");
  pChar += sprintf(pChar,"rdma_err        : display RDMA error counters\n");  
  return pChar; 
}


#define SHOW_RDMA_ERR(probe) pChar += sprintf(pChar," %-22s | %15llu |\n",\
                    rozofs_rdma_err_e2String(probe),\
                    (unsigned long long int)rdma_err_stats[probe]);


void show_rdma_error(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int i;


    pChar += sprintf(pChar, "      RDMA Error        |     count       | \n");
    pChar += sprintf(pChar, "------------------------+-----------------+\n");
    for (i=0; i <= ROZOFS_IBV_WC_MAX_ERR; i++) SHOW_RDMA_ERR(i);

    if (argv[1] != NULL)
    {
      if (strcmp(argv[1],"reset")==0) {
        memset(&rdma_err_stats,0,sizeof(rdma_err_stats));
      }
      else {
	/*
	** Help
	*/
	pChar = show_rdma_err_help(pChar);
      }
    }    
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}


/*
**__________________________________________________
*/
/**
*   Query the current parameters of the RDMA device

    @param device_p: pointer to the device
    @param pChar: pointer to the display buffer
    @param index: adpator index
    @param display_ports: when asserted displays the port informations
    
    retval pointer to the next byte in display buffer
*/
char  *rozofs_ibv_query_port_debug( struct ibv_context *context, 
                                    uint8_t port_num,
				    char *pbuf);

char *rozofs_rdma_display_device_properties(struct ibv_context *device_p,char *pChar,int index,int port_display)
{
   int ret;
   struct ibv_device_attr device_attr;
   char bufall[128];
   char *pname = NULL;
   int i;
   
   memset(bufall,0,128);
   pChar +=sprintf(pChar,"adaptor #%d \n",index);
   ret = ibv_query_device(device_p,&device_attr);
   if (ret < 0)
   {
      pChar +=sprintf(pChar,"ibv_query_device error:%s\n",strerror(errno));
      return pChar;
   }
   /*
   ** get the device name
   */
   pname = (char *)ibv_get_device_name(device_p->device);
   if (pname == NULL)
   {
     pChar +=sprintf(pChar,"Device name         No name\n");
   }
   else
   {
     pChar +=sprintf(pChar,"Device name         %s\n",pname);   
   }
   if (port_display == 0)
   {
     memcpy(bufall,&device_attr.fw_ver,64);
     pChar +=sprintf(pChar,"firmware version    %s\n",bufall);
     pChar +=sprintf(pChar,"vendor              %x-%x-%x\n",device_attr.vendor_id,device_attr.vendor_part_id,device_attr.hw_ver);
     pChar +=sprintf(pChar,"max_qp_init_rd_atom %d\n",device_attr.max_qp_init_rd_atom);
     pChar +=sprintf(pChar,"max_qp_rd_atom      %d\n",device_attr.max_qp_rd_atom);
     pChar +=sprintf(pChar,"max_res_rd_atom     %d\n",device_attr.max_res_rd_atom);
     pChar +=sprintf(pChar,"max_qp              %d\n",device_attr.max_qp);
     pChar +=sprintf(pChar,"max_qp_wr           %d\n",device_attr.max_qp_wr);
     pChar +=sprintf(pChar,"max_cq              %d\n",device_attr.max_cq);
     pChar +=sprintf(pChar,"max_cqe             %d\n",device_attr.max_cqe);
     pChar +=sprintf(pChar,"phys_port_cnt       %d\n",device_attr.phys_port_cnt);
     pChar +=sprintf(pChar,"max_mr_size         %llu\n",(unsigned long long)device_attr.max_mr_size);
     return pChar;
   }
   /*
   ** display each port of the device
   */
   for (i = 0;i <device_attr.phys_port_cnt;i++)
   {
      pChar = rozofs_ibv_query_port_debug(device_p,i+1,pChar);
   }   
   return pChar;

}
/*
**__________________________________________________
*/

void show_rdma_devices(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
  int i;
  int count = 0;
  rozofs_rmda_ibv_cxt_t *ctx;
  
  for (i=0;i< ROZOFS_MAX_RDMA_ADAPTOR; i++) 
  {
     if (rozofs_rmda_ibv_tb[i]== 0) continue;
     ctx = rozofs_rmda_ibv_tb[i];
     if (ctx->ctx != NULL) count++;
  } 
  pChar+=sprintf(pChar,"number of active devices contexts: %d/%d\n",count,(int) ROZOFS_MAX_RDMA_ADAPTOR);
  if ( count == 0) goto out;
  /*
  ** Display the devices
  */
  for (i=0;i< ROZOFS_MAX_RDMA_ADAPTOR; i++) 
  {
     if (rozofs_rmda_ibv_tb[i]== 0) continue;
     ctx = rozofs_rmda_ibv_tb[i];
     if (ctx->ctx != NULL) rozofs_rdma_display_device_properties(ctx->ctx,pChar,i,0);
  }   
   
out:
    
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());      
    
}

/*
**__________________________________________________
*/
/**

  Completion queue thread statistics
*/

char *rozofs_rdma_printcq_stats_header(char *pChar)
{
  pChar += sprintf(pChar,"+----------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+------------+----------------+----------------+---------------+\n");
  pChar += sprintf(pChar,"|  CQ_ID   |   IBV_SEND     |   IBV_RECV     |  IBV_RDMA_READ | IBV_RDMA_WRITE |    ADDRESS     |  NB ENTRIES    |    SYNC_EVT    | ASYNC_EVT  |  POST_SND_CNT  |  MAX_POLL_CNT  |  ACK_COUNT    |\n");
  pChar += sprintf(pChar,"+----------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+------------+----------------+----------------+---------------+\n");
  return pChar;
}

char *rozofs_rdma_printcq_stats(char *pChar,int adaptor,int id,int recv,rozofs_cq_th_stat_t *stats,struct ibv_cq *cq_p,uint64_t post_send_counter,uint64_t max_poll_count,uint64_t ack_count)
{
  pChar += sprintf(pChar,"| %d/%d-%s    |  %12llu  |  %12llu  |  %12llu  |  %12llu  |  %12llx  |  %12d  |  %12u/%12u  |  %12llu  |  %12llu  |  %12llu  |\n",adaptor,id,(recv==0)?"S":"R",
          (long long unsigned int) stats->ibv_wc_send_count,
          (long long unsigned int)stats->ibv_wc_recv_count,
          (long long unsigned int)stats->ibv_wc_rdma_read_count,
          (long long unsigned int)stats->ibv_wc_rdma_write_count,
	  (long long unsigned int)cq_p,
	  cq_p->cqe,
	  cq_p->comp_events_completed,
	  cq_p->async_events_completed,
	  (long long unsigned int)post_send_counter,
	  (long long unsigned int)max_poll_count,	  	  
	  (long long unsigned int)ack_count	  	  

	  );
  pChar += sprintf(pChar,"+----------+----------------+----------------+----------------+----------------+----------------+----------------+----------------+------------+----------------+----------------+---------------+\n"); 
  return pChar; 
}


void show_rdma_cq_threads(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
  int i,j;
  int count = 0;
  rozofs_rmda_ibv_cxt_t *ctx;
  rozofs_qc_th_t *th_p;
  
  for (i=0;i< ROZOFS_MAX_RDMA_ADAPTOR; i++) 
  {
     if (rozofs_rmda_ibv_tb[i]== 0) continue;
     ctx = rozofs_rmda_ibv_tb[i];
     if (ctx->ctx != NULL) count++;
  } 
  pChar+=sprintf(pChar,"number of active devices contexts: %d/%d\n",count,(int) ROZOFS_MAX_RDMA_ADAPTOR);
  if ( count == 0) goto out;
  /*
  ** Display the devices
  */
  pChar = rozofs_rdma_printcq_stats_header(pChar);
  for (i=0;i< ROZOFS_MAX_RDMA_ADAPTOR; i++) 
  {
     if (rozofs_rmda_ibv_tb[i]== 0) continue;
     ctx = rozofs_rmda_ibv_tb[i];
     for (j= 0; j < ROZOFS_CQ_THREAD_NUM; j++)
     {
       th_p = ctx->rozofs_cq_th_post_send[j];
       pChar = rozofs_rdma_printcq_stats(pChar,i,j,0,&th_p->stats,ctx->cq[j],ctx->post_send_stat[j],th_p->max_wc_poll_count,th_p->ack_count);
     }
     for (j= 0; j < ROZOFS_CQ_THREAD_NUM; j++)
     {
       th_p = ctx->rozofs_cq_th_post_recv[j];
       pChar = rozofs_rdma_printcq_stats(pChar,i,j,1,&th_p->stats,ctx->cq_rpc[j],0,0,0);
     }       
  }   
   
out:
    
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());      
    
}


/*
**__________________________________________________
*/

char *rozofs_ibv_mtu2string(int ibv_mtu)
{
    switch (ibv_mtu)
    {
	case IBV_MTU_256: return "256";
	case IBV_MTU_512: return "512";
	case IBV_MTU_1024 : return "1024";
	case IBV_MTU_2048 : return "2048";
	case IBV_MTU_4096: return "4096";
	default: return "Unknown";
    }
}

#define SHOW_IBV_FIED_STRING(name,string) pbuf+=sprintf(pbuf,"   %-22s: %s\n",#name,string);
#define SHOW_IBV_FIED_INT(name) pbuf+=sprintf(pbuf,"   %-22s: %d\n",#name,p->name);
#define SHOW_IBV_FIED_UINT(name) pbuf+=sprintf(pbuf,"   %-22s: %u\n",#name,p->name);
#define SHOW_IBV_FIED_HEX(name) pbuf+=sprintf(pbuf,"   %-22s: 0x%x\n",#name,p->name);


/*
  Port capability
  ---------------
  
  enum ibv_port_cap_flags {
	IBV_PORT_SM				= 1 <<  1,
	IBV_PORT_NOTICE_SUP			= 1 <<  2,
	IBV_PORT_TRAP_SUP			= 1 <<  3,
	IBV_PORT_OPT_IPD_SUP			= 1 <<  4,
	IBV_PORT_AUTO_MIGR_SUP			= 1 <<  5,
	IBV_PORT_SL_MAP_SUP			= 1 <<  6,
	IBV_PORT_MKEY_NVRAM			= 1 <<  7,
	IBV_PORT_PKEY_NVRAM			= 1 <<  8,
	IBV_PORT_LED_INFO_SUP			= 1 <<  9,
	IBV_PORT_SYS_IMAGE_GUID_SUP		= 1 << 11,
	IBV_PORT_PKEY_SW_EXT_PORT_TRAP_SUP	= 1 << 12,
	IBV_PORT_EXTENDED_SPEEDS_SUP		= 1 << 14,
	IBV_PORT_CM_SUP				= 1 << 16,
	IBV_PORT_SNMP_TUNNEL_SUP		= 1 << 17,
	IBV_PORT_REINIT_SUP			= 1 << 18,
	IBV_PORT_DEVICE_MGMT_SUP		= 1 << 19,
	IBV_PORT_VENDOR_CLASS			= 1 << 24,
	IBV_PORT_CLIENT_REG_SUP			= 1 << 25,
	IBV_PORT_IP_BASED_GIDS			= 1 << 26,
};
*/
/*
**__________________________________________________
*/
/**
*  Query the information relative to a port

*/

char  *rozofs_ibv_query_port_debug( struct ibv_context *context, 
                                    uint8_t port_num,
				    char *pbuf)
{

   int ret;
   struct ibv_port_attr port_attr;
   struct ibv_port_attr *p;
   p = &port_attr;
   
   pbuf+=sprintf(pbuf,"\nPort %d\n",port_num);
   ret = ibv_query_port(context,port_num,&port_attr);
   if (ret < 0)
   {
      pbuf+=sprintf(pbuf,"error :%s\n",strerror(errno));
      return pbuf;
   }
	SHOW_IBV_FIED_STRING(state,ibv_port_state_str(p->state));
	SHOW_IBV_FIED_STRING(max_mtu,rozofs_ibv_mtu2string(p->max_mtu));
	SHOW_IBV_FIED_STRING(active_mtu,rozofs_ibv_mtu2string(p->active_mtu));
	SHOW_IBV_FIED_INT(gid_tbl_len);
	SHOW_IBV_FIED_HEX(port_cap_flags);
	SHOW_IBV_FIED_UINT(max_msg_sz);
	SHOW_IBV_FIED_UINT(bad_pkey_cntr);
	SHOW_IBV_FIED_UINT(qkey_viol_cntr);
	SHOW_IBV_FIED_UINT(pkey_tbl_len);
	SHOW_IBV_FIED_UINT(lid);
	SHOW_IBV_FIED_UINT(sm_lid);
	SHOW_IBV_FIED_UINT(lmc);
	SHOW_IBV_FIED_UINT(max_vl_num);
	SHOW_IBV_FIED_UINT(sm_sl);
	SHOW_IBV_FIED_UINT(subnet_timeout);
	SHOW_IBV_FIED_UINT(init_type_reply);
	SHOW_IBV_FIED_UINT(active_width);
	SHOW_IBV_FIED_UINT(active_speed);
	SHOW_IBV_FIED_UINT(phys_state);
	SHOW_IBV_FIED_UINT(link_layer);
	return pbuf;
   
}


/*
**__________________________________________________
*/

void show_rdma_ports(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
  int i;
  int count = 0;
  rozofs_rmda_ibv_cxt_t *ctx;
  
  for (i=0;i< ROZOFS_MAX_RDMA_ADAPTOR; i++) 
  {
     if (rozofs_rmda_ibv_tb[i]== 0) continue;
     ctx = rozofs_rmda_ibv_tb[i];
     if (ctx->ctx != NULL) count++;
  } 
  pChar+=sprintf(pChar,"number of active devices contexts: %d/%d\n",count,(int) ROZOFS_MAX_RDMA_ADAPTOR);
  if ( count == 0) goto out;
  /*
  ** Display the port of each devices
  */
  for (i=0;i< ROZOFS_MAX_RDMA_ADAPTOR; i++) 
  {
     if (rozofs_rmda_ibv_tb[i]== 0) continue;
     ctx = rozofs_rmda_ibv_tb[i];
     if (ctx->ctx != NULL) rozofs_rdma_display_device_properties(ctx->ctx,pChar,i,1);
  }   
   
out:
    
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());      
    
}

/*
**__________________________________________________
*/

void show_rdma_mem(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
  int i;
  rozofs_rdma_memory_reg_t  *user_reg;
  pChar +=sprintf(pChar,"registered memory regions:\n");
  for (i=0;i< ROZOFS_MAX_RDMA_MEMREG; i++) 
  {
     if (rozofs_rdma_memory_reg_tb[i]== NULL) continue;
     user_reg = rozofs_rdma_memory_reg_tb[i];
     pChar +=sprintf(pChar,"memreg #%2d %p/%llu\n",i,user_reg->mem,(unsigned long long int)user_reg->len);

  }
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());      

}


/*
**__________________________________________________
*/

char *show_memory_region(char *pChar,struct ibv_mr *mr_p,char *name)
{
   if (mr_p == NULL) return pChar;
   pChar += sprintf(pChar,"| %10s | %12llx | %12llx | %12llu | %8u | %8u |\n",name,(long long unsigned int)(mr_p->pd),
                                                                              (long long unsigned int)(mr_p->addr),
									      (long long unsigned int)mr_p->length,mr_p->lkey,mr_p->rkey);
   return pChar;

}

void show_rdma_regions(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
  int i;
  int count = 0;
  rozofs_rmda_ibv_cxt_t *ctx;
  
  for (i=0;i< ROZOFS_MAX_RDMA_ADAPTOR; i++) 
  {
     if (rozofs_rmda_ibv_tb[i]== 0) continue;
     ctx = rozofs_rmda_ibv_tb[i];
     if (ctx->ctx != NULL) count++;
  } 
  pChar+=sprintf(pChar,"number of active devices contexts: %d/%d\n",count,(int) ROZOFS_MAX_RDMA_ADAPTOR);
  if ( count == 0) goto out;

  for (i=0;i< ROZOFS_MAX_RDMA_ADAPTOR; i++) 
  {
     if (rozofs_rmda_ibv_tb[i]== 0) continue;
  /*
  ** Display the devices
  */
     pChar += sprintf(pChar,"\nAdaptor #%d\n",i);
     pChar += sprintf(pChar,"|  name      |  Prot. Dom.  |  Address     |  Length      | loc. key | rem. key |\n");
     pChar += sprintf(pChar,"+------------+--------------+--------------+--------------+----------+----------+\n");
     ctx = rozofs_rmda_ibv_tb[i];

     {
       pChar = show_memory_region(pChar,ctx->memreg[0],"Data");
       pChar += sprintf(pChar,"+------------+--------------+--------------+--------------+----------+----------+\n");       
       pChar = show_memory_region(pChar,ctx->memreg_rpc,"Rpc");
       pChar += sprintf(pChar,"+------------+--------------+--------------+--------------+----------+----------+\n");       

     }
  }   
   
out:
    
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());      
    
}

/*
**___________________________________________________________
*/
char *print_ibv_qp_state(int state)
{
   switch (state)
   {
	case IBV_QPS_RESET: return "RESET";
	case IBV_QPS_INIT: return "INIT";
	case IBV_QPS_RTR: return "RTR";
	case IBV_QPS_RTS:return "RTS";
	case IBV_QPS_SQD: return "SQD";
	case IBV_QPS_SQE: return "SQE";
	case IBV_QPS_ERR: return "ERR";
	default: return "UNKNOWN";
  }
}

/*
**___________________________________________________________
*/
char *print_ibv_mig_state(int state)
{
   switch (state)
   {
	case IBV_MIG_MIGRATED: return "MIGRATED";
	case IBV_MIG_REARM: return "REARM";
	case IBV_MIG_ARMED: return "ARMED";
	default: return "UNKNOWN";
  }
}

#define SHOW_IBV_QP_CAP_UINT(name) pbuf+=sprintf(pbuf,"   %-22s: %u\n",#name,p->name);
/*
**___________________________________________________________
*/
char *print_ibv_qp_cap(struct ibv_qp_cap *p,char *pbuf)
{
  SHOW_IBV_QP_CAP_UINT(max_send_wr);
  SHOW_IBV_QP_CAP_UINT(max_recv_wr);
  SHOW_IBV_QP_CAP_UINT(max_send_sge);
  SHOW_IBV_QP_CAP_UINT(max_recv_sge);
  SHOW_IBV_QP_CAP_UINT(max_inline_data);
  return pbuf;
}

/*
**___________________________________________________________
*/

#define SHOW_IBV_AH_FIELD_UINT(prefix,name) pbuf+=sprintf(pbuf,"   %s.%-22s: %u\n",prefix,#name,p->name);
#define SHOW_IBV_AH_FIELD_ULONG(prefix,name) pbuf+=sprintf(pbuf,"   %s.%-22s: %llu\n",prefix,#name,(long long unsigned int)p->name);
char *print_ibv_ah_attr(char *field_name,struct ibv_ah_attr *p,char *pbuf)
{

  SHOW_IBV_AH_FIELD_ULONG(field_name,grh.dgid.global.subnet_prefix);
  SHOW_IBV_AH_FIELD_ULONG(field_name,grh.dgid.global.interface_id);
  SHOW_IBV_AH_FIELD_UINT(field_name,grh.flow_label);
  SHOW_IBV_AH_FIELD_UINT(field_name,grh.sgid_index);
  SHOW_IBV_AH_FIELD_UINT(field_name,grh.hop_limit);
  SHOW_IBV_AH_FIELD_UINT(field_name,grh.traffic_class);  
  
  SHOW_IBV_AH_FIELD_UINT(field_name,dlid);
  SHOW_IBV_AH_FIELD_UINT(field_name,sl);
  SHOW_IBV_AH_FIELD_UINT(field_name,src_path_bits);
  SHOW_IBV_AH_FIELD_UINT(field_name,static_rate);
  SHOW_IBV_AH_FIELD_UINT(field_name,is_global);
  SHOW_IBV_AH_FIELD_UINT(field_name,port_num);
  return pbuf;
}


#define SHOW_IBV_QP_FIELD_STRING(name,string) pbuf+=sprintf(pbuf,"   %-22s: %s\n",#name,string);
#define SHOW_IBV_QP_FIELD_INT(name) pbuf+=sprintf(pbuf,"   %-22s: %d\n",#name,attr.name);
#define SHOW_IBV_QP_FIELD_UINT(name) pbuf+=sprintf(pbuf,"   %-22s: %u\n",#name,attr.name);
#define SHOW_IBV_QP_FIELD_HEX(name) pbuf+=sprintf(pbuf,"   %-22s: 0x%x\n",#name,attr.name);
/*
**___________________________________________________________
*/
char *rozofs_rdma_qp_display(rozofs_rdma_connection_t  *conn_p,int display_level,char *pbuf)
{
    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr init_attr;
    int ret;
    int	qp_attr_mask =
	     IBV_QP_STATE		| /* = 1 << 0 */
	     IBV_QP_CUR_STATE	| /* = 1 << 1 */
	     IBV_QP_EN_SQD_ASYNC_NOTIFY | /* = 1 << 2 */
	     IBV_QP_ACCESS_FLAGS	| /* = 1 << 3 */
	     IBV_QP_PKEY_INDEX	| /* = 1 << 4 */
	     IBV_QP_PORT		| /* = 1 << 5 */
	     IBV_QP_QKEY		| /* = 1 << 6 */
	     IBV_QP_AV		| /* = 1 << 7 */
	     IBV_QP_PATH_MTU	| /* = 1 << 8 */
	     IBV_QP_TIMEOUT	| /* = 1 << 9 */
	     IBV_QP_RETRY_CNT | /* = 1 << 10 */
	     IBV_QP_RNR_RETRY	| /* = 1 << 11 */
	     IBV_QP_RQ_PSN		| /* = 1 << 12 */
	     IBV_QP_MAX_QP_RD_ATOMIC| /* = 1 << 13 */
	     IBV_QP_ALT_PATH	| /* = 1 << 14 */
	     IBV_QP_MIN_RNR_TIMER	| /* = 1 << 15 */
	     IBV_QP_SQ_PSN		| /* = 1 << 16 */
	     IBV_QP_MAX_DEST_RD_ATOMIC| /* = 1 << 17 */
	     IBV_QP_PATH_MIG_STATE	| /* = 1 << 18 */
	     IBV_QP_CAP		| /* = 1 << 19 */
	     IBV_QP_DEST_QPN	/* = 1 << 20 */;
	     
    	     
    uint32_t ipAddr = af_unix_get_remote_ip(conn_p->tcp_index);
         
    pbuf +=sprintf(pbuf,"Queue Pair : %u (TCP ref:%d)\n",conn_p->qp->qp_num,conn_p->tcp_index);
    pbuf +=sprintf(pbuf,"Remote IP  : %u.%u.%u.%u\n", (ipAddr >> 24)&0xFF, (ipAddr >> 16)&0xFF, (ipAddr >> 8)&0xFF, (ipAddr)&0xFF);    
    memset(&attr,0,sizeof(attr));
    ret =  ibv_query_qp(conn_p->qp, &attr,
		        qp_attr_mask,
		         &init_attr);
    if (ret < 0) 
    {
      pbuf+=sprintf(pbuf,"error : %s\n",strerror(errno));
      return pbuf;
    }
    SHOW_IBV_QP_FIELD_UINT(dest_qp_num);

    SHOW_IBV_QP_FIELD_STRING(qp_state,print_ibv_qp_state(attr.qp_state));
    SHOW_IBV_QP_FIELD_STRING(cur_qp_state,print_ibv_qp_state(attr.cur_qp_state));
    SHOW_IBV_QP_FIELD_STRING(path_mtu,rozofs_ibv_mtu2string(attr.path_mtu));
    SHOW_IBV_QP_FIELD_STRING(path_mig_state,print_ibv_mig_state(attr.path_mig_state));
    SHOW_IBV_QP_FIELD_HEX(qkey);
    SHOW_IBV_QP_FIELD_HEX(rq_psn);
    SHOW_IBV_QP_FIELD_HEX(sq_psn);
    SHOW_IBV_QP_FIELD_HEX(qp_access_flags);
    /*
    ** cap, ah_attr, alt_ah_attr 
    */
    if (display_level > 0)
    {
      pbuf = print_ibv_qp_cap(&attr.cap,pbuf);
      pbuf= print_ibv_ah_attr("ah_attr",&attr.ah_attr,pbuf);
      pbuf= print_ibv_ah_attr("alt_ah_attr",&attr.alt_ah_attr,pbuf);
    }

    SHOW_IBV_QP_FIELD_INT(qp_access_flags);
    SHOW_IBV_QP_FIELD_UINT(pkey_index);
    SHOW_IBV_QP_FIELD_UINT(alt_pkey_index);
    SHOW_IBV_QP_FIELD_UINT(en_sqd_async_notify);
    SHOW_IBV_QP_FIELD_UINT(sq_draining);
    SHOW_IBV_QP_FIELD_UINT(max_rd_atomic);
    SHOW_IBV_QP_FIELD_UINT(max_dest_rd_atomic);
    SHOW_IBV_QP_FIELD_UINT(min_rnr_timer);
    SHOW_IBV_QP_FIELD_UINT(port_num);
    SHOW_IBV_QP_FIELD_UINT(timeout);
    SHOW_IBV_QP_FIELD_UINT(retry_cnt);
    SHOW_IBV_QP_FIELD_UINT(rnr_retry);
    SHOW_IBV_QP_FIELD_UINT(alt_port_num);
    SHOW_IBV_QP_FIELD_UINT(alt_timeout);
    
    return pbuf;
}


/*__________________________________________________________________________
  Show the 1rst context
  ==========================================================================*/

void rozofs_rdma_qp_debug_show_first(char *pChar) {
  int i;
  rozofs_rdma_connection_t *conn_p = NULL;

  rozofs_rdma_qp_next_idx = 0;
  
  for (i = rozofs_rdma_qp_next_idx; i < rozofs_nb_rmda_tcp_context; i++)
  {
    conn_p = rozofs_rdma_tcp_table[i];
    rozofs_rdma_qp_next_idx = i;
    if (conn_p == NULL) continue;
    rozofs_rdma_qp_display(conn_p,1,pChar);
    rozofs_rdma_qp_next_idx +=1;
    return;
  
  }
  pChar += sprintf(pChar, "END\n"); 
  rozofs_rdma_qp_next_idx = 0;
}

/*__________________________________________________________________________
  Show the next context
  ==========================================================================*/

void rozofs_rdma_qp_debug_show_next(char *pChar) {
  int i;
  rozofs_rdma_connection_t *conn_p = NULL;

  if (rozofs_rdma_qp_next_idx >= rozofs_nb_rmda_tcp_context)
  {
    pChar += sprintf(pChar, "END\n");   
  }
  
  for (i = rozofs_rdma_qp_next_idx; i < rozofs_nb_rmda_tcp_context; i++)
  {
    conn_p = rozofs_rdma_tcp_table[i];
    rozofs_rdma_qp_next_idx = i;
    if (conn_p == NULL) continue;
    rozofs_rdma_qp_display(conn_p,1,pChar);
    rozofs_rdma_qp_next_idx +=1;
    return;
  
  }
  pChar += sprintf(pChar, "END\n"); 
}

/*__________________________________________________________________________
  Search context by nickname and display it
  ==========================================================================*/
#if 0
void rozofs_rdma_qp_debug_show_name(char *pChar, char * name) {

  rozofs_rdma_connection_t *conn_p = NULL;
  uint32_t index;
  
  int ret = sscanf(name,"%u",&index);
  if (ret != 1) {
    pChar += sprintf(pChar, "this is not a valid index\n");
    return;	        
  }
  if (index >= rozofs_nb_rmda_tcp_context)
  {
    pChar += sprintf(pChar, "out of range index, max is %d\n",rozofs_nb_rmda_tcp_context-1);
    return;           
  }
  conn_p = rozofs_rdma_tcp_table[index];
  if (conn_p == NULL)
  {
    pChar += sprintf(pChar, "no context for index %d\n",index);
    return;           
  }
  rozofs_rdma_qp_display(conn_p,1,pChar);
  return;

}
#endif
void rozofs_rdma_qp_debug_show_name(char *pChar, char * name) {

  rozofs_rdma_connection_t *conn_p = NULL;
  uint32_t index;
  int i;
  
  int ret = sscanf(name,"%u",&index);
  if (ret != 1) {
    pChar += sprintf(pChar, "this is not a valid queue pair value\n");
    return;	        
  }
  for (i = 0; i < rozofs_nb_rmda_tcp_context; i++)
  {
    conn_p = rozofs_rdma_tcp_table[i];
    if (conn_p == NULL) continue;
    if (conn_p->qp->qp_num == index)
    {
      rozofs_rdma_qp_display(conn_p,1,pChar);
      return;
    }
  
  }
  pChar += sprintf(pChar, "Not found\n"); 

  return;

}

/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS:
  -
  RETURN: none
  ==========================================================================*/
void rozofs_rdma_qp_debug_man(char * pt) {
  pt += sprintf(pt,"To display information about the RDMA Queue Pairs .\n");
//  pt += sprintf(pt,"rdma_qp        : display global counters.\n");
  pt += sprintf(pt,"rdma_qp first    : display the 1rst context.\n");
  pt += sprintf(pt,"rdma_qp next     : display the next context.\n");
  pt += sprintf(pt,"rdma_qp <qp_num> : display a the context associated with <qp_num>.\n");  
}
void rozofs_rdma_qp_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
   
  if (argv[1] == NULL) {
    //rozofs_rdma_qp_debug_show_no_param(pChar);
    rozofs_rdma_qp_debug_man(pChar);   
    return   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer()); 
  }
  else if (strcmp(argv[1],"first")==0) {
    rozofs_rdma_qp_debug_show_first(pChar);        
  }
  else if (strcmp(argv[1],"next")==0) {
    rozofs_rdma_qp_debug_show_next(pChar);
  }
  else {
    rozofs_rdma_qp_debug_show_name(pChar, argv[1]);      
  }  
  
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}

/*
**__________________________________________________
*/
void rozofs_rdma_error_register(int rdma_error)
{
  switch(rdma_error)
  {
      case IBV_WC_SUCCESS: rdma_err_stats[ROZOFS_IBV_WC_SUCCESS]++; break;
      case IBV_WC_LOC_LEN_ERR: rdma_err_stats[ROZOFS_IBV_WC_LOC_LEN_ERR]++; break;
      case IBV_WC_LOC_QP_OP_ERR: rdma_err_stats[ROZOFS_IBV_WC_LOC_QP_OP_ERR]++; break;
      case IBV_WC_LOC_EEC_OP_ERR: rdma_err_stats[ROZOFS_IBV_WC_LOC_EEC_OP_ERR]++; break;
      case IBV_WC_LOC_PROT_ERR: rdma_err_stats[ROZOFS_IBV_WC_LOC_PROT_ERR]++; break;
      case IBV_WC_WR_FLUSH_ERR: rdma_err_stats[ROZOFS_IBV_WC_WR_FLUSH_ERR]++; break;
      case IBV_WC_MW_BIND_ERR: rdma_err_stats[ROZOFS_IBV_WC_MW_BIND_ERR]++; break;
      case IBV_WC_BAD_RESP_ERR: rdma_err_stats[ROZOFS_IBV_WC_BAD_RESP_ERR]++; break;
      case IBV_WC_LOC_ACCESS_ERR: rdma_err_stats[ROZOFS_IBV_WC_LOC_ACCESS_ERR]++; break;
      case IBV_WC_REM_INV_REQ_ERR: rdma_err_stats[ROZOFS_IBV_WC_REM_INV_REQ_ERR]++; break;
      case IBV_WC_REM_ACCESS_ERR: rdma_err_stats[ROZOFS_IBV_WC_REM_ACCESS_ERR]++; break;
      case IBV_WC_REM_OP_ERR: rdma_err_stats[ROZOFS_IBV_WC_REM_OP_ERR]++; break;
      case IBV_WC_RETRY_EXC_ERR: rdma_err_stats[ROZOFS_IBV_WC_RETRY_EXC_ERR]++; break;
      case IBV_WC_RNR_RETRY_EXC_ERR: rdma_err_stats[ROZOFS_IBV_WC_RNR_RETRY_EXC_ERR]++; break;
      case IBV_WC_LOC_RDD_VIOL_ERR: rdma_err_stats[ROZOFS_IBV_WC_LOC_RDD_VIOL_ERR]++; break;
      case IBV_WC_REM_INV_RD_REQ_ERR: rdma_err_stats[ROZOFS_IBV_WC_REM_INV_RD_REQ_ERR]++; break;
      case IBV_WC_REM_ABORT_ERR: rdma_err_stats[ROZOFS_IBV_WC_REM_ABORT_ERR]++; break;
      case IBV_WC_INV_EECN_ERR: rdma_err_stats[ROZOFS_IBV_WC_INV_EECN_ERR]++; break;
      case IBV_WC_INV_EEC_STATE_ERR: rdma_err_stats[ROZOFS_IBV_WC_INV_EEC_STATE_ERR]++; break;
      case IBV_WC_FATAL_ERR: rdma_err_stats[ROZOFS_IBV_WC_FATAL_ERR]++; break;
      case IBV_WC_RESP_TIMEOUT_ERR: rdma_err_stats[ROZOFS_IBV_WC_RESP_TIMEOUT_ERR]++; break;
      case IBV_WC_GENERAL_ERR:rdma_err_stats[ROZOFS_IBV_WC_GENERAL_ERR]++; break;
      default: rdma_err_stats[ROZOFS_IBV_WC_MAX_ERR]++; break;
  }
}
/*
**__________________________________________________
*/
/**
  send  a message from RDMA local side to TCP local side
  

  @param opcode 
  @param rozofs_rdma_tcp_assoc_t
  
  @retval 0 on success
  @retval -1 on error (see errno for details)
*/
int rozofs_rdma_send2tcp_side(int opcode,rozofs_rdma_tcp_assoc_t *assoc_p)
{
   rozofs_rdma_msg_t msg;
   int ret;
   
   msg.opcode = opcode;
   msg.req = ROZOFS_MSG_REQ;
   memcpy(&msg.assoc,assoc_p,sizeof(rozofs_rdma_tcp_assoc_t));
   ret = write(rozofs_rdma_signaling_sockpair[0],&msg,sizeof(msg));
   if (ret < 0) return -1;
   if (ret != sizeof(rozofs_rdma_msg_t)) return -1;
   return 0;
}

/*
**__________________________________________________
*/
/**
  send  a message from TCP local side to RDMA local side
      
  @param opcode 
  @param rozofs_rdma_tcp_assoc_t
  
  @retval 0 on success
  @retval -1 on error (see errno for details)
*/
int rozofs_rdma_send2rdma_side(int opcode,rozofs_rdma_tcp_assoc_t *assoc_p)
{
   rozofs_rdma_msg_t msg;
   int ret;
   
   msg.opcode = opcode;
   msg.req = ROZOFS_MSG_REQ;
   memcpy(&msg.assoc,assoc_p,sizeof(rozofs_rdma_tcp_assoc_t));
   ret = write(rozofs_rdma_signaling_sockpair[1],&msg,sizeof(msg));
   if (ret < 0) return -1;
   if (ret != sizeof(rozofs_rdma_msg_t)) return -1;
   return 0;

}
/*
**______________________________________________________________________________________

        R P C   O V E R   R D M A    B U F F E R   M A N A G E M E N T

**______________________________________________________________________________________
*/	
/*
**__________________________________________________
*/
/*
** Create the user pool for sending RPC messages
   The pool is common for request and response
   
   The buffers on that pool will be used by the shared received queue of the rdma
   
   @param count :number of buffer
   @param length : length of a buffer
   
   
   @retval 0 on success
   @retval < 0 on error s(see errno for details )
*/
int rozofs_rdma_pool_rpc_create(int count,int size)
{
   if (rozofs_rdma_pool_rpc != NULL)
   {
      /*
      ** Already created
      */
      return 0;
   }
   /*
   ** Init of the Mutex to protect alloc/release in a multithreaded environment
   */
   pthread_rwlock_init(&rozofs_rdma_pool_rpc_lock, NULL); 
   
   rozofs_rdma_pool_rpc = ruc_buf_poolCreate(count,size);
   if (rozofs_rdma_pool_rpc == NULL) return -1;
   /*
   ** Register the pool with rozodiag
   */
   ruc_buffer_debug_register_pool("RDMA_rpc_pool",rozofs_rdma_pool_rpc);
   /*
   ** register the user memory region
   */
   rozofs_rdma_memreg_rpc.mem = ruc_buf_get_pool_base_and_length(rozofs_rdma_pool_rpc,&rozofs_rdma_memreg_rpc.len);
   return 0;
}

/*
**__________________________________________________
*/
/**
*   Post a buffer in the shared queue for receiving a RPC message over RDMA
    
    @param s_ctx: RDMA context of the adaptor
    
    @retval 0: success
    @retval -1: error (see errno for details)    
*/    
int rozofs_ibv_post_srq_recv4rpc(rozofs_rmda_ibv_cxt_t *s_ctx)
{
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  struct ibv_mr  *mr_p; 

  void *pool;
  void *ruc_buf_p;
  int error;
  
  if (s_ctx->shared_queue4rpc == NULL)
  {
    severe("The RDMA shared queue does not exist\n");
    errno = ENOSYS;
    return -1;
  }
  /*
  ** Get the context asssociated to the type of buffer
  */

  pool = rozofs_rdma_pool_rpc;

  mr_p = s_ctx->memreg_rpc;
  /*
  ** Allocate a ruc_buffer according to the selected pool
  */
  pthread_rwlock_wrlock(&rozofs_rdma_pool_rpc_lock);
  ruc_buf_p = ruc_buf_getBuffer(pool);
  pthread_rwlock_unlock(&rozofs_rdma_pool_rpc_lock);
  if (ruc_buf_p == NULL)
  {
    errno = ENOMEM;
    return -1;
  }
  /*
  ** The reference of the ruc_buffer is moved in the wr_id field in order to be able to
  ** release it when user needs to release
  */
  wr.wr_id = (uint64_t)ruc_buf_p;
  wr.next = NULL;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)ruc_buf_getPayload(ruc_buf_p);
  sge.length = ruc_buf_getMaxPayloadLen(ruc_buf_p);
  sge.lkey = mr_p->lkey;;
  
  if(ibv_post_srq_recv(s_ctx->shared_queue4rpc, &wr, &bad_wr)!=0)
  {
     warning ("ibv_post_srq_recv error :%s",strerror(errno));
     error = errno;
     /*
     ** Release the buffer
     */
     pthread_rwlock_wrlock(&rozofs_rdma_pool_rpc_lock);
     ruc_buf_freeBuffer(ruc_buf_p);
     pthread_rwlock_unlock(&rozofs_rdma_pool_rpc_lock);
     errno = error;
     return -1;     
  }
  return 0;
}


/*
**__________________________________________________
*/
/**
*   Post a buffer in the shared queue for receiving a RPC message over RDMA
    
    @param s_ctx: RDMA context of the adaptor
    @param ruc_buf_p: reference of the allocated ruc_buffer
    
    @retval 0: success
    @retval -1: error (see errno for details)    
*/    
int rozofs_ibv_post_srq_recv4rpc_with_ruc_buf(rozofs_rmda_ibv_cxt_t *s_ctx,void *ruc_buf_p)
{
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  struct ibv_mr  *mr_p; 
  int error;
  
  if (s_ctx->shared_queue4rpc == NULL)
  {
    severe("The RDMA shared queue does not exist\n");
    errno = ENOSYS;
    return -1;
  }
  /*
  ** Get the context asssociated to the type of buffer
  */

  mr_p = s_ctx->memreg_rpc;
  /*
  ** The reference of the ruc_buffer is moved in the wr_id field in order to be able to
  ** release it when user needs to release
  */
  wr.wr_id = (uint64_t)ruc_buf_p;
  wr.next = NULL;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)ruc_buf_getPayload(ruc_buf_p);
  sge.length =ruc_buf_getMaxPayloadLen(ruc_buf_p);
  sge.lkey = mr_p->lkey;;
  
  if(ibv_post_srq_recv(s_ctx->shared_queue4rpc, &wr, &bad_wr)!=0)
  {
     warning ("ibv_post_srq_recv error :%s",strerror(errno));
     error = errno;
     /*
     ** Release the buffer
     */
     pthread_rwlock_wrlock(&rozofs_rdma_pool_rpc_lock);
     ruc_buf_freeBuffer(ruc_buf_p);
     pthread_rwlock_unlock(&rozofs_rdma_pool_rpc_lock);
     errno = error;
     return -1;     
  }
  return 0;
}
/*
**__________________________________________________
*/
/**
*  Allocate a rdma RPC buffer

   @param none
   
   @retval <> NULL pointer to the ruc_buffer
   @retval NULL: out of buffer
*/
void *rozofs_rdma_allocate_rpc_buffer()
{
   void *buf;
   
   if (rozofs_rdma_pool_rpc == NULL) return NULL;
   
   pthread_rwlock_wrlock(&rozofs_rdma_pool_rpc_lock);
   buf = ruc_buf_getBuffer(rozofs_rdma_pool_rpc);
   pthread_rwlock_unlock(&rozofs_rdma_pool_rpc_lock);
   return buf;

}

/*
**__________________________________________________
*/
/**
* Release a rdma RPC buffer

   @param buf: pointer to the ruc_buffer
   
   @retval 0 on success
   @retval<0 on error
*/
void rozofs_rdma_release_rpc_buffer(void *buf)
{
   pthread_rwlock_wrlock(&rozofs_rdma_pool_rpc_lock);
   ruc_buf_freeBuffer(buf);
   pthread_rwlock_unlock(&rozofs_rdma_pool_rpc_lock);
}
/*
**__________________________________________________
*/
/**
*check if the ruc_buffer belongs to the RPC RDMA pool

   @param buf: pointer to the ruc_buffer
   
   @retval 1 yes
   @retval 0 no
*/
int rozofs_is_rdma_rpc_buffer(void *buf)
{
   ruc_obj_desc_t* p =ruc_objGetHead(buf);
   if (NULL==p) return 0;
   if (p == rozofs_rdma_pool_rpc) return 1;
   return 0;
}
/*
**__________________________________________________
*/
/**
   init of the RDMA
   
   @param nb_rmda_tcp_context : number of RDMA TCP contexts
   @param client_mode: assert to 1 for client; 0 for server mode
   @param count : number of RPC context (for sending & receiving)
   @param size: size of the RPC buffer
   @param rpc_rdma_cbk: pointer to the callback used for RPC received over RDMA
   
   retval 0 on sucess
   retval -1 on error
*/   
int rozofs_rdma_init(uint32_t nb_rmda_tcp_context,int client_mode,int count,int size,rozofs_rdma_pf_rpc_cqe_done_t rdma_rpc_recv_cbk)
{
  int ret = 0;
  int i;
  int    fileflags;  
  rozofs_cur_act_rmda_tcp_context = 0;
  rozofs_cur_del_rmda_tcp_context = 0; 
  rozofs_max_rpc_buffer_srq_refill = ROZOFS_RDMA_BUF_SRQ_MAX_REFILL;
  rozofs_rdma_listener_count = 0;
  rozofs_rdma_listener_max   = ROZOFS_RDMA_MAX_LISTENER;
  rozofs_rdma_rpc_cbk = rdma_rpc_recv_cbk;
  
  memset(&rozofs_rdma_listener[0],0,sizeof(uint64_t*)*ROZOFS_RDMA_MAX_LISTENER);
  /*
  ** signalling thread conf. parameters
  */
  rdma_sig_reconnect_credit_conf = ROZOFS_RDMA_SIG_RECONNECT_CREDIT_COUNT;
  rdma_sig_reconnect_credit = rdma_sig_reconnect_credit_conf;
  rdma_sig_reconnect_period_conf = ROZOFS_RDMA_SIG_RECONNECT_CREDIT_PERIOD_SEC;
  /*
  ** create the pool used for RPC message over RDMA
  */
  ret = rozofs_rdma_pool_rpc_create(count,size);
  
  rozofs_rdma_tcp_table = malloc(sizeof(rozofs_rdma_connection_t*)*nb_rmda_tcp_context);
  if (rozofs_rdma_tcp_table ==NULL)
  {
    return -1;
  }
  tcp_cnx_tb = malloc(sizeof(rozofs_rdma_tcp_cnx_t*)*nb_rmda_tcp_context);
  if (tcp_cnx_tb ==NULL)
  {
    return -1;
  }
  rozofs_rdma_signalling_thread_mode = client_mode;
  memset(rozofs_rdma_tcp_table,0,sizeof(rozofs_rdma_connection_t*)*nb_rmda_tcp_context);
  rozofs_nb_rmda_tcp_context = nb_rmda_tcp_context;
  /*
  ** init of the linked list of the context waiting for deletion
  */
  list_init(&rozofs_rdma_del_head_list);            
  pthread_rwlock_init(&rozofs_rdma_del_lock, NULL); 
  
  for (i=0;i< ROZOFS_MAX_RDMA_ADAPTOR; i++) rozofs_rmda_ibv_tb[i] = 0;
  for (i=0;i< ROZOFS_MAX_RDMA_MEMREG; i++) rozofs_rdma_memory_reg_tb[i] = 0;
  
  /*
  ** create the event channel
  */
  rozofs_rdma_ec = rdma_create_event_channel();
  if (rozofs_rdma_ec == NULL)
  {
     if (errno == ENODEV) {
       info("error on rdma_create_event_channel(): %s",strerror(errno));
     }
     else {
       severe("error on rdma_create_event_channel(): %s",strerror(errno));
     }  
//     goto error;  
  }
  /* 
  ** create the socket pair
  */
  if (socketpair(  AF_UNIX,SOCK_DGRAM,0,&rozofs_rdma_signaling_sockpair[0])< 0)
  {
      severe("failed on socketpair:%s",strerror(errno));
      goto error;
  }
  while(1)
  {
    /*
    ** change socket mode to asynchronous for the socket used by the TCP side (it is the only
    ** socket index of the pair that is registered with the socket controller
    */
    if((fileflags=fcntl(rozofs_rdma_signaling_sockpair[1],F_GETFL,0))<0)
    {
      warning ("rozofs_rdma_signaling_sockpair[1]fcntl error:%s",strerror(errno));
      break;
    }
    if (fcntl(rozofs_rdma_signaling_sockpair[1],F_SETFL,(fileflags|O_NDELAY))<0)
    {
      warning ("rozofs_rdma_signaling_sockpair[1]fcntl error:%s",strerror(errno));
      break;
    }
    break;
  }
  /*
  ** perform the regsitration with the socket controller
  */
  rdma_sig_sockctrl_p = ruc_sockctl_connect(rozofs_rdma_signaling_sockpair[1],
                                	   "RDMA_SIG_TCP",
                                	    16,
                                	    NULL,
                                	    &rdma_sig_callBack_TcpSock);
    if (rdma_sig_sockctrl_p== NULL)
    {
      fatal("Cannot connect with socket controller");;
      return -1;
    }
  /*
  ** Create the RDMA signaling thread
  */
  rozofs_rdma_signalling_thread_started=0;
  TEST_NZ(pthread_create(&rozofs_rdma_signaling_thread_ctx, NULL, rozofs_rmda_signaling_th, 0));
  while(1)
  {
     /*
     ** wait for thread init done
     */
     usleep(20000);
     if (rozofs_rdma_signalling_thread_started) break;
  
  }
  /*
  ** RDMA is enable since there is at least one adapter that supports it
  */
  rozofs_rdma_enable = 1;
  /*
  ** add the rozodiag features
  */
  uma_dbg_addTopic("rdma_status", show_rdma_status);  
  uma_dbg_addTopic("rdma_statistics", show_rdma_statistics);  
  uma_dbg_addTopic("rdma_error", show_rdma_error);  
  uma_dbg_addTopic("rdma_devices", show_rdma_devices);  
  uma_dbg_addTopic("rdma_ports", show_rdma_ports);  
  uma_dbg_addTopic("rdma_mem", show_rdma_mem);  
  uma_dbg_addTopic("rdma_regions", show_rdma_regions);  
  uma_dbg_addTopic("rdma_cq_threads", show_rdma_cq_threads);  
  uma_dbg_addTopicAndMan("rdma_qp", rozofs_rdma_qp_debug, rozofs_rdma_qp_debug_man, 0);
  return ret;
error:
  rozofs_rdma_enable = 0;
  return -1;

}
/*
**__________________________________________________________________________________________________________
*/
/**
*  Process of ROZOFS RDMA signalling message received on the AF_UNIX socket (TCP client/server side)
   
   @retval 0 on success
   @retval -1 on error
*/
int rozofs_rdma_srv_af_unix_receive_tcp()
{

   rozofs_rdma_msg_t msg;
   int ret;
   ret = read(rozofs_rdma_signaling_sockpair[1],&msg,sizeof(msg));
   if (ret < 0) return -1;
   if (ret != sizeof(rozofs_rdma_msg_t)) return -1;
   /*
   ** call the FSM
   */
   if (rozofs_rdma_signalling_thread_mode) rozofs_rdma_tcp_cli_fsm_exec(&msg.assoc,msg.opcode);
   else rozofs_rdma_tcp_srv_fsm_exec(&msg.assoc,msg.opcode);
   return 0;
}

/*
**__________________________________________________________________________________________________________
*/
/**
*  Process of ROZOFS RDMA signalling message received on the AF_UNIX socket (RDMA client or server side)

   @param socket: reference of the socket
   
   @retval 0 on success
   @retval -1 on error
*/
int rozofs_rdma_cli_af_unix_receive_rdma()
{

   rozofs_rdma_msg_t msg;
   int ret;
   ret = read(rozofs_rdma_signaling_sockpair[0],&msg,sizeof(msg));
   if (ret < 0) return -1;
   if (ret != sizeof(rozofs_rdma_msg_t)) return -1;
   /*
   ** call the FSM
   */
   if (rozofs_rdma_signalling_thread_mode) rozofs_rdma_cli_fsm_exec(&msg.assoc,NULL,msg.opcode);
   else rozofs_rdma_srv_fsm_exec(&msg.assoc,NULL,msg.opcode);
   return 0;
}
/*
**__________________________________________________________________________________________________________
*/
/**
    RDMA signaling thread
    
    That thread is intended :
      1 - to process the message received on the create event channel of the RDMA (either client or server mode)
      2 - to process the message received from the main client/server thread on the index 0 of the socket pair
    
*/
void * rozofs_rmda_signaling_th(void *ctx)
{

   struct rdma_cm_event *event = NULL;
   rozo_fd_set active_fd_set, read_fd_set;
   struct rdma_cm_event event_copy; 
//   struct sockaddr_in addr;   
   char  private_data[256];  
   struct timeval timeout;   
   time_t last_del_time =0;
   time_t last_reconnect_time =0;
   struct timeval cur_time;
   int nb_select;
   memset(&rozofs_rdma_sig_th_stats,0,sizeof(rozofs_rdma_sig_th_stats));
   
   uma_dbg_thread_add_self("RDMA SIG");
   
  info("RDMA signalling thread started in mode %s rdmc_channel_fd %d\n",(rozofs_rdma_signalling_thread_mode==0)?"SERVER":"CLIENT",
                                                                        (rozofs_rdma_ec!=NULL)?rozofs_rdma_ec->fd:-1);
  if (rozofs_rdma_signalling_thread_mode ==0)
  {
#if 0
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    TEST_NZ(rdma_create_id(rozofs_rdma_ec, &rozofs_rdma_listener, NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_bind_addr(rozofs_rdma_listener, (struct sockaddr *)&addr));
    TEST_NZ(rdma_listen(rozofs_rdma_listener, 10)); /* backlog=10 is arbitrary */

    rozofs_rdma_listening_port = ntohs(rdma_get_src_port(rozofs_rdma_listener));

    info("listening on port %d.\n", rozofs_rdma_listening_port);
#endif
  }
  gettimeofday(&cur_time,NULL);
  last_del_time = cur_time.tv_sec;


   /* Initialize the set of active sockets. */
   FD_ZERO (&active_fd_set);
   if (rozofs_rdma_ec!=NULL) FD_SET (rozofs_rdma_ec->fd, &active_fd_set);
   FD_SET (rozofs_rdma_signaling_sockpair[0], &active_fd_set);
   /*
   ** indicates that the thread is started
   */
   rozofs_rdma_signalling_thread_started = 1;

   while (1)
   {
      /* Block until input arrives on one or more active sockets. */
      timeout.tv_sec = 2;
      timeout.tv_usec = 0;
      read_fd_set = active_fd_set;
      nb_select= select (FD_SETSIZE, (fd_set *)&read_fd_set, NULL, NULL, &timeout);
      if (nb_select < 0)
      {
        if (errno == EINTR) continue;
        fatal ("error on select():%s",strerror(errno));
      }
      if (nb_select !=0)
      {
	/*
	** check if there is a message received on the RDMA channel event
	*/
	if ((rozofs_rdma_ec!=NULL) && (FD_ISSET (rozofs_rdma_ec->fd, &read_fd_set)))
	{
          rozofs_rdma_sig_th_stats.rmda_msg_rcv_cnt++;

          if (rdma_get_cm_event(rozofs_rdma_ec, &event) < 0) goto error;
          memcpy(&event_copy, event, sizeof(*event));  
	  if (event->param.conn.private_data != NULL)
	  { 
	     memcpy(private_data,event->param.conn.private_data,event->param.conn.private_data_len);
	     event_copy.param.conn.private_data = private_data;
	  }
	  rdma_ack_cm_event(event);  
	  if (rozofs_rdma_signalling_thread_mode) rozofs_rdma_cli_on_event(&event_copy);
	  else rozofs_rdma_srv_on_event(&event_copy);
	}
	/*
	** check if there a message received on the internal socket pair
	*/
	if (FD_ISSET (rozofs_rdma_signaling_sockpair[0], &read_fd_set))
	{
          rozofs_rdma_sig_th_stats.int_msg_rcv_cnt++;
          rozofs_rdma_cli_af_unix_receive_rdma();     
	}
      }
      /*
      ** check if it is time to proceed with context under deletion
      */
      gettimeofday(&cur_time,NULL);
      if (cur_time.tv_sec > last_del_time+1)
      {
        last_del_time=cur_time.tv_sec;
	rozofs_rdma_del_process(last_del_time);	      
      }
      if (rozofs_rdma_signalling_thread_mode) /* only for client side */
      {
	/*
	** Check if it is time to refill the reconnect attempt counter
	*/
	if (cur_time.tv_sec > last_reconnect_time+rdma_sig_reconnect_period_conf)
	{
          last_reconnect_time=cur_time.tv_sec;
	  rdma_sig_reconnect_credit = rdma_sig_reconnect_credit_conf;	      
	}
      }      
      
   }
error:
   fatal ("error on RDMA service:%s",strerror(errno));
}     
/*
**__________________________________________________
*/
/**
*  Release of RozoFS RDMA context
 
   The release is always an asynchronous operation since it might be possible
   that the completion thread (processing of end of RDMA_READ/WRITE) can access to
   the context.
   So the context is queued in the deletion queue associated with the signalling thread
   By default the effective deletion of the context is differed by 1s.
   
   @param cnx_p : pointer to the rdma context
   
   @retval none
*/
void rozofs_rdma_release_context(rozofs_rdma_connection_t *cnx_p)
{
  struct timeval tv;
  struct rdma_cm_id *id;
  
  /*
  ** remove the context from the table
  */
  rozofs_rdma_tcp_table[cnx_p->tcp_index] = NULL;
  
  id = cnx_p->id;
  /*
  ** get the deletion timestamp
  */
  gettimeofday(&tv,NULL);
  cnx_p->del_tv_sec= tv.tv_sec;
  /*
  ** issue a rmda disconnect if the connection is established:
  ** note: the RDMA_CM_EVENT_DISCONNECTED event generated on the initiator side
  **       will certainly failed since the context is already in the error state
  */
  if (cnx_p->state == ROZOFS_RDMA_ST_ESTABLISHED)
  {
    RDMA_STATS_REQ(rdma_disconnect);
    if (rdma_disconnect(id)< 0) 
    {
      RDMA_STATS_RSP_NOK(rdma_disconnect);
    }
    else 
    {
      RDMA_STATS_RSP_OK(rdma_disconnect);
    }
  }
  if (cnx_p->state != ROZOFS_RDMA_ST_ERROR)
  {
    cnx_p->state = ROZOFS_RDMA_ST_ERROR;
    /*
    ** do not delete the queue pair here since it might be possible that
    ** some threads are sending ibv_post_send(). A SIGSEGV can be encountered 
    ** because the memory area were verbs are stored has been released during 
    ** rdma_destroy_qp() 
    */
    //rdma_destroy_qp(id);
    /*
    ** insert the connection context in the deletion list
    */
    pthread_rwlock_wrlock(&rozofs_rdma_del_lock);
    rozofs_cur_del_rmda_tcp_context++;
    rozofs_cur_act_rmda_tcp_context--;
    list_push_back(&rozofs_rdma_del_head_list, &cnx_p->list);
    pthread_rwlock_unlock(&rozofs_rdma_del_lock);
  }
}
/*
**__________________________________________________
*/
/**
*  Effective release of a rozofs rdma context

   
   @param cnx_p : pointer to the rdma context
   
   @retval none
*/
void rozofs_rdma_release_context_effective(rozofs_rdma_connection_t *cnx_p)
{

  if (cnx_p->id !=NULL) {

    if (cnx_p->id->qp !=NULL) 
    {
      RDMA_STATS_REQ(rdma_destroy_qp);
      rdma_destroy_qp(cnx_p->id);
      RDMA_STATS_RSP_OK(rdma_destroy_qp);
    }
    RDMA_STATS_REQ(rdma_destroy_id);
    if (rdma_destroy_id(cnx_p->id) < 0)
    {
      RDMA_STATS_RSP_NOK(rdma_destroy_id);
    }
    else
    {
      RDMA_STATS_RSP_OK(rdma_destroy_id);    
    }
  }
  free(cnx_p);

}
/*
**__________________________________________________
*/
void rozofs_rdma_del_process(time_t del_time)
{
   list_t *p, *q;
   rozofs_rdma_connection_t *cnx_p;
   
   pthread_rwlock_wrlock(&rozofs_rdma_del_lock);
   
   list_for_each_forward_safe(p, q,&rozofs_rdma_del_head_list)
   {
     cnx_p = list_entry(p, rozofs_rdma_connection_t, list);
#warning need to set a parameter to delay the qp deletion
     if (del_time > (cnx_p->del_tv_sec+10))
     {
       list_remove(p);
       rozofs_rdma_release_context_effective(cnx_p);
       rozofs_cur_del_rmda_tcp_context--;
       rozofs_deleted_rdma_tcp_contexts++;
//       info("FDL remove connection context %d/%d\n",rozofs_cur_del_rmda_tcp_context,rozofs_cur_act_rmda_tcp_context);

     }
     pthread_rwlock_unlock(&rozofs_rdma_del_lock);
     return ;
   }
}

/*
**_________________________________________________________________________________________________

     R D M A   C O M P L E T I O N   Q U E U E    T H R E A D (post_recv)
     
         Process the reception of the RPC messages (either storcli or storio for I/O)
	 rozofsmount & exportd for metadata (future)

**_________________________________________________________________________________________________
*/

/**
*  Update statistics on Completion queue thread

   @param opcode: RDMA opcode
   @param th_ctx_p: context of the thread

   @retval none
*/
void rozofs_rdma_cq_th_stats_update(uint32_t opcode,rozofs_qc_th_t *th_ctx_p)
{
   switch (opcode)
   {
     case IBV_WC_RECV:
        th_ctx_p->stats.ibv_wc_recv_count++;
	break;
     case IBV_WC_SEND:
        th_ctx_p->stats.ibv_wc_send_count++;
	break;
     case IBV_WC_RDMA_READ:
        th_ctx_p->stats.ibv_wc_rdma_read_count++;
	break;
     case IBV_WC_RDMA_WRITE:
        th_ctx_p->stats.ibv_wc_rdma_write_count++;
	break;
     default:
     break;
   }
}
/*
**__________________________________________________
*/
/**
  Function that process the status returned by the RDMA operation 
  That service is called under the thread context associated with a RDMA adaptor
  
  Version 2
  
  In case of error, the service can send a RDMA_DISCONNECT_REQ on the socketpair
  In any case, the service ends by a sem_post() to wake up the thread that has initiated the
  RDMA transfer.
  
 
  The opcode that can be found of a completion queue are:
  IBV_WC_RECV: Send data operation for a WR that was posted to a Receive Queue (of a QP or to an SRQ)
  
  On the storio side it might be a RPC read or write request 
  On the storcli side it might be a PRC read or write response
  
  The wr_id found in the message is the reference of ruc_buffer. The RPC message can be found in the payload of the ruc_buffer
  the reference of the qp on which the message has been sent can be found in the qp_num of the ibv_wc context
  
  @param wc : RDMA ibv_post_recv context (wc->wr_id contains the reference of the ruc buffer that has been pushed on the SRQ
  @param th_ctx_p: context of the thread: needed to refill the shared queue with Work Request buffer
  
*/

void rozofs_on_completion2_rcv(struct ibv_wc *wc,rozofs_qc_th_t *th_ctx_p)
{
  int status = 0;
  int error = 0;
  void *ruc_buf_p;
  
  rozofs_rdma_cq_th_stats_update(wc->opcode,th_ctx_p);
  
  ruc_buf_p = (void*)wc->wr_id;
  
  
  switch (wc->status)
  {
	case IBV_WC_SUCCESS:
	  break;
	case IBV_WC_LOC_LEN_ERR:
	case IBV_WC_LOC_QP_OP_ERR:
	case IBV_WC_LOC_EEC_OP_ERR:
	case IBV_WC_LOC_PROT_ERR:
	case IBV_WC_WR_FLUSH_ERR:
	case IBV_WC_MW_BIND_ERR:
	case IBV_WC_BAD_RESP_ERR:
	case IBV_WC_LOC_ACCESS_ERR:
	case IBV_WC_REM_INV_REQ_ERR:
	case IBV_WC_REM_ACCESS_ERR:
	case IBV_WC_REM_OP_ERR:
	case IBV_WC_RETRY_EXC_ERR:
	case IBV_WC_RNR_RETRY_EXC_ERR:
	case IBV_WC_LOC_RDD_VIOL_ERR:
	case IBV_WC_REM_INV_RD_REQ_ERR:
	case IBV_WC_REM_ABORT_ERR:
	case IBV_WC_INV_EECN_ERR:
	case IBV_WC_INV_EEC_STATE_ERR:
	case IBV_WC_FATAL_ERR:
	case IBV_WC_RESP_TIMEOUT_ERR:
	case IBV_WC_GENERAL_ERR: 
	default: 
	    rozofs_rdma_error_register(wc->status);
            status = -1;
	    error = wc->status;
	  break;    
  }
  /*
  ** we don't really care about the opcode that should be either 
  ** IBV_WC_RDMA_READ or IBV_WC_RDMA_WRITE
  */
  /*
  ** Now signaled the thread that has submitted the ibv_post_send
  */
  if ((status == 0)&&(rozofs_rdma_rpc_cbk!=NULL))
  {
    /*
    ** Set the payload len in the ruc buffer
    */
    ruc_buf_setPayloadLen(ruc_buf_p,wc->byte_len);
    (rozofs_rdma_rpc_cbk)(wc->opcode,ruc_buf_p,wc->qp_num,th_ctx_p->ctx_p,status,error);
  }
  else
  {
     /*
     ** there is an error or no registered callback, no to release the buffer
     */
     rozofs_rdma_release_rpc_buffer(ruc_buf_p);
  
  }
  /*
  ** Attempt to refill the Share Queue with a new buffer
  */
  th_ctx_p->rpc_buf_count2alloc++;
  {
    int ret;
    int i;
    int count = th_ctx_p->rpc_buf_count2alloc;
    if (count < 0) 
    {
       warning ("Completion Queue Thread with a wrong SRQ count %d",count);
       count = 1; 
    }
    for (i = 0; i < count; i++)
    {
       ruc_buf_p = rozofs_rdma_allocate_rpc_buffer();
       if (ruc_buf_p == NULL) break;
       ret = rozofs_ibv_post_srq_recv4rpc_with_ruc_buf(th_ctx_p->ctx_p,ruc_buf_p);
       if (ret < 0)
       {
          /*
	  ** Check if the share receive queu  is full
	  */
          if (errno == ENOMEM) th_ctx_p->rpc_buf_count2alloc = 0;
	  break;
       }
       th_ctx_p->rpc_buf_count2alloc--;
       /*
       ** Check if we have reach the maximum of the queue refill during the processing on one message
       */
       if (i >=rozofs_max_rpc_buffer_srq_refill) break;
    }  
   }
}

/*
**__________________________________________________
*/
/**
  Poll thread associated with a RDMA adapator: that thread is intended to process the RPC messages received from the SRQ
  It is typically :
      - the RPC write & Read request on the server side (storio)
      - the RPC write & read response on the client side (storcli)
  
  
  @param ctx : pointer to the RDMA context associated with the thread
  
  retval NULL on error
*/
void * rozofs_poll_cq_rpc_recv_th(void *ctx)
{
  rozofs_qc_th_t *th_ctx_p = (rozofs_qc_th_t*)ctx;
  rozofs_rmda_ibv_cxt_t *s_ctx = (rozofs_rmda_ibv_cxt_t*)th_ctx_p->ctx_p;
  struct ibv_cq *cq;
  struct ibv_wc wc;
//  int wc_to_ack = 0;
  
  info("CQ-R thread#%d started \n",th_ctx_p->thread_idx);

    uma_dbg_thread_add_self("CQ_Th_rpc");
    {
      struct sched_param my_priority;
      int policy=-1;
      int ret= 0;

      pthread_getschedparam(pthread_self(),&policy,&my_priority);
          info("storio main thread Scheduling policy   = %s\n",
                    (policy == SCHED_OTHER) ? "SCHED_OTHER" :
                    (policy == SCHED_FIFO)  ? "SCHED_FIFO" :
                    (policy == SCHED_RR)    ? "SCHED_RR" :
                    "???");

      my_priority.sched_priority= 98;
      policy = SCHED_FIFO;
      ret = pthread_setschedparam(pthread_self(),policy,&my_priority);
      if (ret < 0) 
      {
	severe("error on sched_setscheduler: %s",strerror(errno));	
      }
      pthread_getschedparam(pthread_self(),&policy,&my_priority);    
    }      

  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel_rpc[th_ctx_p->thread_idx], &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));

    while (ibv_poll_cq(cq, 1, &wc))
      rozofs_on_completion2_rcv(&wc,th_ctx_p);
  }

error:
  return NULL;
}



/*
**_________________________________________________________________________________________________

     R D M A   C O M P L E T I O N   Q U E U E    T H R E A D (post_send)
     
         IBV_WC_SEND
	 IBV_WC_RDMA_READ
	 IBV_WC_RDMA_WRITE

**_________________________________________________________________________________________________
*/
/*
**__________________________________________________
*/
/**
  Function that process the status returned by the RDMA operation 
  That service is called under the thread context associated with a RDMA adaptor
  
  Version 2
  
  In case of error, the service can send a RDMA_DISCONNECT_REQ on the socketpair
  In any case, the service ends by a sem_post() to wake up the thread that has initiated the
  RDMA transfer.
  
  The service is intended to support only two kinds of RDMA service:
  IBV_WC_SEND: Send operation for a WR that was posted to the Send Queue
  IBV_WC_RDMA_READ : RDMA Read operation for a WR that was posted to the Send Queue
  IBV_WC_RDMA_WRITE: RDMA write operation for a WR that was posted to the Send Queue
  
  @param wc : RDMA ibv_post_send context
  @param th_ctx_p : completion queue context
  
*/

void rozofs_on_completion2(struct ibv_wc *wc,rozofs_qc_th_t *th_ctx_p)
{
  rozofs_wr_id2_t *thread_wr_p = (rozofs_wr_id2_t *)(uintptr_t)wc->wr_id;
  int status = 0;
  int error = 0;

  rozofs_rdma_cq_th_stats_update(wc->opcode,th_ctx_p);
//#warning FDL_debug  rozofs_on_completion2
//  info("rozofs_on_completion2 opcode %d status %d", wc->opcode,wc->status);
  switch (wc->status)
  {
	case IBV_WC_SUCCESS:
	  break;
	case IBV_WC_LOC_LEN_ERR:
	case IBV_WC_LOC_QP_OP_ERR:
	case IBV_WC_LOC_EEC_OP_ERR:
	case IBV_WC_LOC_PROT_ERR:
	case IBV_WC_WR_FLUSH_ERR:
	case IBV_WC_MW_BIND_ERR:
	case IBV_WC_BAD_RESP_ERR:
	case IBV_WC_LOC_ACCESS_ERR:
	case IBV_WC_REM_INV_REQ_ERR:
	case IBV_WC_REM_ACCESS_ERR:
	case IBV_WC_REM_OP_ERR:
	case IBV_WC_RETRY_EXC_ERR:
	case IBV_WC_RNR_RETRY_EXC_ERR:
	case IBV_WC_LOC_RDD_VIOL_ERR:
	case IBV_WC_REM_INV_RD_REQ_ERR:
	case IBV_WC_REM_ABORT_ERR:
	case IBV_WC_INV_EECN_ERR:
	case IBV_WC_INV_EEC_STATE_ERR:
	case IBV_WC_FATAL_ERR:
	case IBV_WC_RESP_TIMEOUT_ERR:
	case IBV_WC_GENERAL_ERR: 
	default: 
	    rozofs_rdma_error_register(wc->status);
            status = -1;
	    error = wc->status;
	  break;    
  }
  /*
  ** we don't really care about the opcode that should be either 
  ** IBV_WC_RDMA_READ or IBV_WC_RDMA_WRITE
  */
  /*
  ** Now signaled the thread that has submitted the ibv_post_send
  */
  if (thread_wr_p!=NULL)
  {
    (*thread_wr_p->cqe_cbk)(thread_wr_p->user_param,status,error);
  }
  /**
  * check if RDMA connect must be turned down in case of error
  */
}
/*
**__________________________________________________
*/
/**
*  Procedure that check the status of the RDMA operation in order to
   potentially trigger a RDMA disconnect if there is an error.
   
   That procedure is intended to be called by the server side (storio) upon
   the end of either RDMA_READ or RDMA_WRITE.
   
   
   @param status: 0 : no error; -1: error
   @param error: error code .
   @param assoc_p : pointer to the association context that exists between the TCP and RDMA side
   (note: that information is found in the parameter of the RPC request associated with RDMA_READ or RDMA_WRITE)
   
   @retval none
*/
void rozofs_rdma_on_completion_check_status_of_rdma_connection(int status,int error,rozofs_rdma_tcp_assoc_t *assoc_p)
{

    /*
    ** nothing to do when there is no error
    */
    if (status == 0) return ;
    /*
    ** There is a RDMA error: for now we shut down the RDMA connection. By doing it, the client will be warned
    ** and revert to TCP mode only.
    ** In order to do it, a ROZOFS_RDMA_EV_RDMA_DEL_IND message must be sent to the RDMA signalling thread.
    ** We use the ROZOFS_RDMA_EV_RDMA_DEL_IND in order to inform the TCP side that RDMA has disconnected
    */
    
    rozofs_rdma_send2rdma_side(ROZOFS_RDMA_EV_RDMA_DEL_IND,assoc_p);
    return;
}
/*
**__________________________________________________
*/
/**
  Poll thread associated with a RDMA adapator
  
  note : that thread makes sense on the server side only since only the storio are
         intended to initiate RDMA transfer. The storcli remains always passive.
	 However the thread is also started on the storcli side.
  
  @param ctx : pointer to the RDMA context associated with the thread
  
  retval NULL on error
*/
void * rozofs_poll_cq_th(void *ctx)
{
  rozofs_qc_th_t *th_ctx_p = (rozofs_qc_th_t*)ctx;
  rozofs_rmda_ibv_cxt_t *s_ctx = (rozofs_rmda_ibv_cxt_t*)th_ctx_p->ctx_p;
  struct ibv_cq *cq;
  struct ibv_wc wc;
  uint64_t max_wc_poll_count = 0;
  int wc_to_ack = 0;
  
  info("CQ thread#%d started \n",th_ctx_p->thread_idx);
  uma_dbg_thread_add_self("CQ_Th_rw");
  /*
  ** Clear the statistics
  */
  th_ctx_p->max_wc_poll_count = 0;
  th_ctx_p->ack_count = 0;
#if 1
    {
      struct sched_param my_priority;
      int policy=-1;
      int ret= 0;

      /**
      *  The priority of the completion queue thread must be higher than
      * the priority of the disk threads otherwise we can face a deadlock in
      * the ibv_post_send() while attempting to take the spin_lock on the QP.
      * If the task that has the spin_lock cannot be schedule because all the
      * core are allocated to disk threads with an higher priority compared
      * the priority of the completion queue thread, we enter the dead lock
      */
            
      pthread_getschedparam(pthread_self(),&policy,&my_priority);
          info("storio main thread Scheduling policy   = %s\n",
                    (policy == SCHED_OTHER) ? "SCHED_OTHER" :
                    (policy == SCHED_FIFO)  ? "SCHED_FIFO" :
                    (policy == SCHED_RR)    ? "SCHED_RR" :
                    "???");
 #if 1
      my_priority.sched_priority= 98;
      policy = SCHED_FIFO;
      ret = pthread_setschedparam(pthread_self(),policy,&my_priority);
      if (ret < 0) 
      {
	severe("error on sched_setscheduler: %s",strerror(errno));	
      }
      pthread_getschedparam(pthread_self(),&policy,&my_priority);

 #endif        

    }  
#endif     

#if 1
  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel[th_ctx_p->thread_idx], &cq, &ctx));
    wc_to_ack = 0;
//    ibv_ack_cq_events(cq, 1);
//    th_ctx_p->ack_count++;
    TEST_NZ(ibv_req_notify_cq(cq, 0));
    max_wc_poll_count = 0;
    while (ibv_poll_cq(cq, 1, &wc))
    {
      max_wc_poll_count++;
      wc_to_ack++;
      rozofs_on_completion2(&wc,th_ctx_p);
      if (wc_to_ack == 16) 
      {
	ibv_ack_cq_events(cq, wc_to_ack);
	th_ctx_p->ack_count +=wc_to_ack;
	wc_to_ack = 0;
      }
    }
    if (wc_to_ack != 0)
    {
	ibv_ack_cq_events(cq, wc_to_ack);
	th_ctx_p->ack_count +=wc_to_ack;
	wc_to_ack = 0;    
    }
    if (th_ctx_p->max_wc_poll_count < max_wc_poll_count) th_ctx_p->max_wc_poll_count = max_wc_poll_count;
  }
#else
  while (1) {
    if (wc_to_ack == 16) 
    {
      ibv_ack_cq_events(cq, wc_to_ack);
      wc_to_ack = 0;
    }
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel[th_ctx_p->thread_idx], &cq, &ctx));
//    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));

    while (ibv_poll_cq(cq, 1, &wc))
    {
      wc_to_ack++;
      rozofs_on_completion2(&wc,th_ctx_p);
    }
  }


#endif
error:
  severe("FDL RDMA exit from thread");
  return NULL;
}
/*
**__________________________________________________
*/
/**
   Create a user memory region
   
   The goal of that service is to make the rozofs RDMA module aware of the memory regions
   that are intended to be used for move porjections between storcli and storio.
   That service is intended to be called after the creation of the memory pool.
   
   The caller provide the starting and and memory length within the user_reg structure. 
   From a rozofs pool standpoint, it corresponds to the full payload of a memory pool.
   
   Once the memory region has been registered, the internal index within the user_reg
   structure is updated with the index number in the rozofs_rdma_memory_reg_tb table.
   
   
   @param user_reg: user memory region
   
   retval 0 on success
   retval<0 on error
*/  

int rozofs_rdma_user_memory_register (rozofs_rdma_memory_reg_t  *user_reg)
{
  int i;
  
  for (i=0;i< ROZOFS_MAX_RDMA_MEMREG; i++) 
  {
     if (rozofs_rdma_memory_reg_tb[i]!= 0) continue;
     rozofs_rdma_memory_reg_tb[i] = malloc(sizeof(rozofs_rdma_memory_reg_t));
     if (rozofs_rdma_memory_reg_tb[i] ==NULL)
     {
        fatal("Out of memory");
     }
     memcpy(rozofs_rdma_memory_reg_tb[i],user_reg,sizeof(rozofs_rdma_memory_reg_t));
     /*
     ** register the index within the table in the context
     */
     user_reg->idx = i;
     return 0;
  }
  return -1;
}
/*
**__________________________________________________
*/
/**
   Get the RDMA context associated with an adaptor
   
   @param: ibv context returned by the adaptor
   
   retval <> NULL: pointer to the rozofs RDMA context
   retval ==NULL: not found
*/  

rozofs_rmda_ibv_cxt_t *rozofs_get_rdma_ctx(struct ibv_context *verbs)
{
  int i;
  rozofs_rmda_ibv_cxt_t *ctx;
  
  for (i=0;i< ROZOFS_MAX_RDMA_ADAPTOR; i++) 
  {
     if (rozofs_rmda_ibv_tb[i]== 0) continue;
     ctx = rozofs_rmda_ibv_tb[i];
     if (ctx->ctx == verbs) return ctx;
  }
  return NULL;
}
/*
**__________________________________________________
*/
/**
   insert the RDMA context associated with an adaptor
   
   @param ctx : rozofs RDMA context to insert
   
   retval 0 on sucess
   retval -1 on error
*/  
int rozofs_insert_rdma_ctx(rozofs_rmda_ibv_cxt_t *ctx)
{
  int i;
  
  for (i=0;i< ROZOFS_MAX_RDMA_ADAPTOR; i++) 
  {
     if (rozofs_rmda_ibv_tb[i]!= 0) continue;
     rozofs_rmda_ibv_tb[i]=ctx;
     return 0;
  }
  return -1;
}

/*
**__________________________________________________
*/
/**
*   Query the current parameters of the RDMA device

    @param device_p: pointer to the device
    
    retval none
*/
void rozofs_rdma_print_device_properties(struct ibv_context *device_p)
{
   int ret;
   struct ibv_device_attr device_attr;
   
   ret = ibv_query_device(device_p,&device_attr);
   if (ret < 0)
   {
      perror("ibv_query_device error");
      return;
   }
   
   printf("max_qp_init_rd_atom %d\n",device_attr.max_qp_init_rd_atom);
   printf("max_qp_rd_atom      %d\n",device_attr.max_qp_rd_atom);
   printf("max_res_rd_atom     %d\n",device_attr.max_res_rd_atom);
   printf("max_qp              %d\n",device_attr.max_qp);
   printf("phys_port_cnt       %d\n",device_attr.phys_port_cnt);

}
/*
**__________________________________________________
*/
/**
   buildthe RDMA context associated with an adaptor if
   it does not exist
   
   That service is intented to be called once the creation of the RDMA connection
   If the rozofs_rmda_ibv_cxt_t has already been created the service returns the pointer to the rozofs_rmda_ibv_cxt_t.
   
   If the rozofs_rmda_ibv_cxt_t does not exist, on new one is created, and the key used to identify it is the 
   input parameter.
   
   @param verbs : ibv context of the adaptor
   
   retval <> NULL pointer to the RDMA context of the adaptor 
   retval NULL on error
*/  

rozofs_rmda_ibv_cxt_t *rozofs_build_rdma_context(struct ibv_context *verbs)
{
  rozofs_rmda_ibv_cxt_t *s_ctx;
  rozofs_rdma_memory_reg_t *mem_reg_p=NULL;
  int i;
  int k;
  /*
  ** search for an existing context
  */
  s_ctx = rozofs_get_rdma_ctx(verbs);
  if (s_ctx) {
    return s_ctx;
  }
  info("FDL creating ibv_context\n");
  s_ctx = (rozofs_rmda_ibv_cxt_t*)malloc(sizeof(rozofs_rmda_ibv_cxt_t));
  if (s_ctx == NULL)
  {
    severe("Error while creating ROZOFS_RMDA_IBV_CXT:%s\n",strerror(errno));
    return NULL;
  }
  /*
  ** Clear the context before using it
  */
  memset(s_ctx,0,sizeof(rozofs_rmda_ibv_cxt_t));

  s_ctx->ctx = verbs;
  rozofs_rdma_print_device_properties(verbs);
  RDMA_STATS_REQ(ibv_alloc_pd);
  s_ctx->pd = ibv_alloc_pd(s_ctx->ctx);
  if (s_ctx->pd == NULL)
  {
    severe("RDMA ibv_alloc_pd failure for context %p:%s",s_ctx->ctx,strerror(errno));
    RDMA_STATS_RSP_NOK(ibv_alloc_pd);
    goto error;
  }
  RDMA_STATS_RSP_OK(ibv_alloc_pd);
  /*
  ** create a number of completion queue corresponding to the number of CQ threads (one set  for RDMA read/write and one set for RPC over RDMA
  */
  for (k=0; k<ROZOFS_CQ_THREAD_NUM; k++)
  {
    RDMA_STATS_REQ(ibv_create_comp_channel);
    if ((s_ctx->comp_channel[k] = ibv_create_comp_channel(s_ctx->ctx))==NULL)
    {
      severe("RDMA ibv_create_comp_channel failure for context %p:%s",s_ctx->ctx,strerror(errno));
      RDMA_STATS_RSP_NOK(ibv_create_comp_channel);
      goto error;    
    }
    RDMA_STATS_RSP_OK(ibv_create_comp_channel);
    RDMA_STATS_REQ(ibv_create_cq);
    if ((s_ctx->cq[k] = ibv_create_cq(s_ctx->ctx, ROZOFS_RDMA_COMPQ_SIZE, NULL, s_ctx->comp_channel[k], 0))==NULL) /* cqe=10 is arbitrary */
    {
      severe("RDMA ibv_create_cq failure for context %p:%s",s_ctx->ctx,strerror(errno));
      RDMA_STATS_RSP_NOK(ibv_create_cq);
      goto error;        
    }
    RDMA_STATS_RSP_OK(ibv_create_cq);
    TEST_NZ(ibv_req_notify_cq(s_ctx->cq[k], 0));
  }
  /*
  ** Create the completion queue and channel for RPC message over RDMA
  */
  for (k=0; k<ROZOFS_CQ_THREAD_NUM; k++)
  {
    RDMA_STATS_REQ(ibv_create_comp_channel);
    if ((s_ctx->comp_channel_rpc[k] = ibv_create_comp_channel(s_ctx->ctx))==NULL)
    {
      severe("RDMA ibv_create_comp_channel failure for context %p:%s",s_ctx->ctx,strerror(errno));
      RDMA_STATS_RSP_NOK(ibv_create_comp_channel);
      goto error;    
    }
    RDMA_STATS_RSP_OK(ibv_create_comp_channel);
    RDMA_STATS_REQ(ibv_create_cq);
    if ((s_ctx->cq_rpc[k] = ibv_create_cq(s_ctx->ctx, ROZOFS_RDMA_COMPQ_RPC_SIZE, NULL, s_ctx->comp_channel_rpc[k], 0))==NULL) /* cqe=10 is arbitrary */
    {
      severe("RDMA ibv_create_cq failure for context %p:%s",s_ctx->ctx,strerror(errno));
      RDMA_STATS_RSP_NOK(ibv_create_cq);
      goto error;        
    }
    RDMA_STATS_RSP_OK(ibv_create_cq);
    info("FDL index %d %p ",k,s_ctx->cq_rpc[k]);
    
    TEST_NZ(ibv_req_notify_cq(s_ctx->cq_rpc[k], 0));
  }
  /*
  ** associate the user memory region with adaptor
  */
  for (i=0; i < ROZOFS_MAX_RDMA_MEMREG; i++)
  {
    if (rozofs_rdma_memory_reg_tb[i]== 0) continue;
    mem_reg_p = rozofs_rdma_memory_reg_tb[i];
    RDMA_STATS_REQ(ibv_reg_mr);
    s_ctx->memreg[i] = ibv_reg_mr(
		       s_ctx->pd,
		       mem_reg_p->mem,
		       mem_reg_p->len,
		       IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (s_ctx->memreg[i] == 0)
    {
       severe("ibv_reg_mr idx %d error for addr %p len %llu : %s",i,mem_reg_p->mem,(unsigned long long int)mem_reg_p->len,strerror(errno));
       RDMA_STATS_RSP_NOK(ibv_reg_mr);
       goto error;  
    }
#if 0 // debug 
    {
      struct ibv_mr  *mr_p;     
      mr_p = s_ctx->memreg[i];
      info("FDL memreg idx %d addr/len %llx/%llu key %x\n",i,mem_reg_p->mem,mem_reg_p->len,mr_p->lkey);
    }
#endif
    RDMA_STATS_RSP_OK(ibv_reg_mr);    
  }
  /*
  ** check if there is at least one memory region declared
  */
  if (mem_reg_p == NULL) goto error;
  /*
  ** Create the memory region for the RPC messages over RDMA
  */
  RDMA_STATS_REQ(ibv_reg_mr);
  s_ctx->memreg_rpc = ibv_reg_mr(
		     s_ctx->pd,
		     rozofs_rdma_memreg_rpc.mem,
		     rozofs_rdma_memreg_rpc.len,
		     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
  if (s_ctx->memreg_rpc == 0)
  {
     severe("ibv_reg_mr  error for addr %p len %llu : %s",rozofs_rdma_memreg_rpc.mem,(unsigned long long int)rozofs_rdma_memreg_rpc.len,strerror(errno));
     RDMA_STATS_RSP_NOK(ibv_reg_mr);
     goto error;  
  }
#if 0 // debug
  {
     struct ibv_mr  *mr_p;     
     mr_p = s_ctx->memreg_rpc;
     info("FDL memreg idx %d addr/len %llx/%llu key %x\n",i,mem_reg_p->mem,mem_reg_p->len,mr_p->lkey);
  }
#endif
  RDMA_STATS_RSP_OK(ibv_reg_mr);
  /*
  ** Create the Shared Queue for the RPC messages over RDMA
  */
  {
    struct ibv_srq_init_attr srq_attr;
    
    RDMA_STATS_REQ(ibv_create_srq);    
    srq_attr.srq_context  = s_ctx;
    srq_attr.attr.max_wr  = ROZOFS_MAX_SRQ_WR;  /**< max of outstanding Work Request that can be posted to that shared queue */
    srq_attr.attr.max_sge = 1;
    srq_attr.attr.srq_limit = 0; /* that paramter is ignored, aonly relevant for iWARP */
    
    s_ctx->shared_queue4rpc = ibv_create_srq(s_ctx->pd,&srq_attr); 
    if (s_ctx->shared_queue4rpc == NULL)
    {
      severe("ibv_create_srq  error: %s",strerror(errno));
      RDMA_STATS_RSP_NOK(ibv_create_srq);
      goto error;          
    } 
    RDMA_STATS_RSP_OK(ibv_create_srq);
  }  
  /*
  ** Fill up the srq with the rpc buffer
  */
  {
    RDMA_STATS_REQ(ibv_post_srq_recv);    

    int count; 
    int ret;      
    for (count = 0 ; count < ROZOFS_RDMA_COMPQ_RPC_SIZE; count++)
    {
      ret = rozofs_ibv_post_srq_recv4rpc(s_ctx);
      if (ret < 0)
      {
	warning("error while filling up the SRQ ( count (cur/max) %d,%d) error: %s",count,ROZOFS_RDMA_COMPQ_RPC_SIZE,strerror(errno));
        RDMA_STATS_RSP_NOK(ibv_post_srq_recv);
	goto error;
      }
    }	       
    RDMA_STATS_RSP_OK(ibv_post_srq_recv);
  }   
  /*
  ** create the CQ threads
  */
  for (k=0; k<ROZOFS_CQ_THREAD_NUM; k++)
  {
    rozofs_qc_th_t *ctx_cur_p = malloc(sizeof(rozofs_qc_th_t));
    memset(ctx_cur_p,0,sizeof(rozofs_qc_th_t));
    ctx_cur_p->thread_idx = k;
    ctx_cur_p->ctx_p = s_ctx;
    s_ctx->rozofs_cq_th_post_send[k] = ctx_cur_p;
    TEST_NZ(pthread_create(&s_ctx->cq_poller_thread[k], NULL, rozofs_poll_cq_th, ctx_cur_p));
  }
  /*
  ** Put Code to create the completion queue thread for the processing of the RPC message
  */

  for (k=0; k<ROZOFS_CQ_THREAD_NUM; k++)
  {
    rozofs_qc_th_t *ctx_cur_p = malloc(sizeof(rozofs_qc_th_t));
    memset(ctx_cur_p,0,sizeof(rozofs_qc_th_t));
    ctx_cur_p->thread_idx = k;
    ctx_cur_p->ctx_p = s_ctx;
    s_ctx->rozofs_cq_th_post_recv[k] = ctx_cur_p;
    TEST_NZ(pthread_create(&s_ctx->cq_rpc_poller_thread[k], NULL, rozofs_poll_cq_rpc_recv_th, ctx_cur_p));
  }
  /*
  ** create the RDMA asynchronous event thread : use to get more information about RDMA error
  */
  {
    rozofs_rdma_async_event_th_t *ctx_cur_p = malloc(sizeof(rozofs_rdma_async_event_th_t)); 
    ctx_cur_p->thread_idx = 0;
    ctx_cur_p->ctx_p = s_ctx;
    s_ctx->rozofs_rdma_async_event_th = ctx_cur_p;  
    TEST_NZ(pthread_create(&s_ctx->rozofs_rdma_async_event_thread, NULL, rozofs_poll_rdma_async_event_th, ctx_cur_p));

  }
  /*
  ** insert on the table
  */
  if (rozofs_insert_rdma_ctx(s_ctx)!=0) goto error;
  return s_ctx;
  
error:
  severe("RDMA error while creating ROZOFS_RMDA_IBV_CXT : %s\n",strerror(errno));
  return NULL;
}

/*
**__________________________________________________
*/
/**
   Build the queue pair attributes
   
   @param qp_attr :pointer to queue pair attributes
   @param s_ctx :pointer to RDMA adaptor context
   
   retval none
*/  

void rozofs_build_qp_attr(struct ibv_qp_init_attr *qp_attr, rozofs_rmda_ibv_cxt_t *s_ctx)
{
  memset(qp_attr, 0, sizeof(*qp_attr));
  s_ctx->next_cq_idx++;
  qp_attr->send_cq = s_ctx->cq[s_ctx->next_cq_idx%ROZOFS_CQ_THREAD_NUM];
  qp_attr->recv_cq = s_ctx->cq_rpc[s_ctx->next_cq_idx%ROZOFS_CQ_THREAD_NUM];
  /*
  ** store the reference  of the shared queue used for RPC over RDMA
  */
  qp_attr->srq = s_ctx->shared_queue4rpc;
  qp_attr->qp_type = IBV_QPT_RC;
  /*
  **  need to adjust the number of WQ to the number of disk threads to avoid ENOEM on ibv_post_send()
  */
  qp_attr->cap.max_send_wr = 512;
  qp_attr->cap.max_recv_wr = 512;
  qp_attr->cap.max_send_sge = 1;
  qp_attr->cap.max_recv_sge = 1;
}
/*
**___________________________________________________________________________

       S E R V E R   S I D E 
**___________________________________________________________________________
*/

/*
**___________________________________________________________
*/
/**
   connect request processing on server side
   it is associated with the RDMA_CM_EVENT_CONNECT_REQUEST event
   
   @param id :pointer to rdma cm identifier
   
   retval 0 on sucess
   retval -1 on error
*/  
int rozofs_rdma_srv_on_connect_request(struct rdma_cm_id *id,struct rdma_cm_event *event)
{
  struct ibv_qp_init_attr qp_attr;
  struct rdma_conn_param cm_params;
  rozofs_rmda_ibv_cxt_t *s_ctx;  
  rozofs_rdma_connection_t *conn=NULL;
  rozofs_rdma_connection_t *prev_conn=NULL;
  uint8_t private_sz;
  rozofs_rdma_tcp_assoc_t *assoc_p;
  

//  info(" FDL receive connection request.\n");

  s_ctx = rozofs_build_rdma_context(id->verbs);
  if (s_ctx == NULL) 
  {
    severe("rozofs_build_rdma_context error");
    goto error;
  }
  /*
  ** Get the information associated with the connection
  */
  assoc_p = (rozofs_rdma_tcp_assoc_t *)event->param.conn.private_data;
  private_sz = event->param.conn.private_data_len;
  /*
  ** Check if the reference of the server reference is in range
  */
  if (assoc_p->srv_ref >= rozofs_nb_rmda_tcp_context)
  {
     /*
     ** the reference is out of range, reject the accept
     */
     severe("on_connect_request: out of range server reference : %d max %d\n",assoc_p->srv_ref,rozofs_nb_rmda_tcp_context);
     goto error;  
  }  
  /*
  ** the index of the cq that will be used is found in s_ctx->next_cq_idx
  */
  rozofs_build_qp_attr(&qp_attr,s_ctx);
  /*
  ** create the queue pair needed for communication with the peer
  */
  RDMA_STATS_REQ(rdma_create_qp);
  if(rdma_create_qp(id, s_ctx->pd, &qp_attr)<0)
  {
     RDMA_STATS_RSP_NOK(rdma_create_qp);
     severe("rdma_create_qp() error :%s\n",strerror(errno));
     goto error;
  }
  RDMA_STATS_RSP_OK(rdma_create_qp);
  id->context = conn = (rozofs_rdma_connection_t *)malloc(sizeof(rozofs_rdma_connection_t));
  if (conn == NULL)
  {
    /*
    ** out of memory--> abort
    */
    fatal("Out of memory : %s\n",strerror(errno));
  }
  rozofs_cur_act_rmda_tcp_context++;
  list_init(&conn->list);
  conn->state = ROZOFS_RDMA_ST_WAIT_ESTABLISHED;
  conn->qp = id->qp;
  /*
  ** save the reference of the QP it might be need for reception from the SRQ since the
  ** RDMA connexion is identified by the qp_num value
  */
  conn->qp_num = conn->qp->qp_num;
  conn->id = id;
  conn->tcp_index = assoc_p->srv_ref;
  /*
  ** set the index of the completion queues: for statistics purpose
  */
  conn->cq_xmit_idx = s_ctx->next_cq_idx;
  conn->cq_recv_idx = s_ctx->next_cq_idx;
  
  conn->s_ctx = s_ctx;
  info("FDL QP %llx  cli/srv_ref %d/%d port_cli/srv %d/%d len %d \n",(long long unsigned int)conn->qp,assoc_p->cli_ref,
             assoc_p->srv_ref,
	     assoc_p->port_cli,
	     assoc_p->port_srv,
	     private_sz);
  memcpy(&conn->assoc,assoc_p, sizeof(rozofs_rdma_tcp_assoc_t));
  /*
  ** insert the context in the table
  */
  prev_conn = rozofs_rdma_tcp_table[conn->tcp_index];
  rozofs_rdma_tcp_table[conn->tcp_index] = conn;
  /*
  ** Insert the index of the TCP connection in the QP hash table
  ** That information will be used upon receiving a message from storcli in order to
  ** identify the source of the request. The only case for which it is needed it when there
  ** is an error while we decode the RPC message. Since the association block is in the RPC
  ** message (that association block is the way we identify the connection) we have no way to
  ** find out the source of the wrong request if we have not that qp_num hash table.
  */
  
  if (prev_conn != NULL)
  {
    rozofs_rdma_release_context(prev_conn);
  }

  /*
  ** modify the attributes of the queue pair: only access mode are modified
  */
  {
    struct ibv_qp_attr attr;

     memset(&attr, 0, sizeof(attr));
     attr.qp_access_flags =  IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
     attr.max_dest_rd_atomic = 16;
     attr.max_rd_atomic = 16;

     if (ibv_modify_qp(id->qp, &attr,IBV_QP_ACCESS_FLAGS|IBV_QP_MAX_DEST_RD_ATOMIC| IBV_QP_MAX_QP_RD_ATOMIC)< 0) goto error;

     attr.retry_cnt = 2;
     attr.rnr_retry = 2;
     attr.min_rnr_timer = 14;
     attr.port_num = 2;
     if (ibv_modify_qp(id->qp, &attr,IBV_QP_RETRY_CNT|IBV_QP_RNR_RETRY| IBV_QP_MIN_RNR_TIMER| IBV_QP_PORT )< 0) goto error;
  }
#if 0
  /*
  ** that function does not make sense since the connectX4/5 are able to handle the case of the bonding driver and
  ** to balance the QP traffic on each port in a round robin way
  */
  {
    struct ibv_exp_qp_attr attr;

         memset(&attr, 0, sizeof(attr));
         attr.flow_entropy = 1;
	 attr.port_num = 1;
	 attr.qp_state = IBV_QPS_INIT;
	
	 if (ibv_exp_modify_qp(id->qp, &attr,IBV_QP_STATE|IBV_QP_PORT|IBV_EXP_QP_FLOW_ENTROPY )< 0) 
	 {
	    severe(" FDL cest la MERDE !!!!!!!");
	 }
	 
	 
  }
#endif
  /*
  ** post receive is intended to fill up some context for the case of WRITE_SEND (not used by RozoFS)
  */
  post_receives(conn);

  memset(&cm_params, 0, sizeof(cm_params));
  /*
  ** These 2 parameters are REQUIRED if we want to be able to perform RDMA_READ!!
  */
  cm_params.responder_resources = 16;
  cm_params.initiator_depth = 16;
  RDMA_STATS_REQ(rdma_accept);
  if (rdma_accept(id, &cm_params) < 0)
  {
    RDMA_STATS_RSP_NOK(rdma_accept);
    severe("RDMA accept error: %s",strerror(errno));
    goto error;  
  }
  RDMA_STATS_RSP_OK(rdma_accept);  
  return 0;
  
  /*
  ** error case
  */
error:
  /*
  ** reject the connection: use the id since the connection might not be allocated at the time
  ** the error is encountered
  */
  RDMA_STATS_REQ(rdma_reject);
  rdma_reject(id,NULL,0);
  if (conn != NULL)
  {
     /*
     ** remove the context from the table
     */
     rozofs_rdma_tcp_table[conn->tcp_index] = NULL;
     conn->state = ROZOFS_RDMA_ST_ERROR;
     /*
     ** release the allocated context : note that here it is possible to proceed
     ** with an immediate release since nobody can use that context
     */
     rozofs_rdma_release_context(conn);     
  }
  return -1;
}
/*
**__________________________________________________
*/
/**
   connect confirmation request processing on server side
   it is associated with the RDMA_CM_EVENT_ESTABLISHED event
   
   @param id :pointer to rdma cm identifier
   
   retval 0 on sucess
   retval -1 on error
*/  
int rozofs_rdma_srv_on_connection(void *context)
{
//  rozofs_rdma_connection_t *conn = (rozofs_rdma_connection_t *)context;
//  struct ibv_send_wr wr, *bad_wr = NULL;
//  struct ibv_sge sge;

//  snprintf(conn->send_region, BUFFER_SIZE, "message from passive/server side with pid %d", getpid());

#if 0
  memset(&wr, 0, sizeof(wr));

  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)conn->send_region;
  sge.length = BUFFER_SIZE;
  sge.lkey = conn->send_mr->lkey;

  TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
#endif
  return 0;
}
/*
**__________________________________________________
*/
/**
   disconnecyt request processing on server side
   it is associated with the RDMA_CM_EVENT_DISCONNECTED event
   
   @param id :pointer to rdma cm identifier
   
   retval 0 on sucess
   retval -1 on error
*/  
int rozofs_rdma_srv_on_disconnect_request(struct rdma_cm_id *id)
{
  rozofs_rdma_connection_t *conn = (rozofs_rdma_connection_t *)id->context;

  info("peer disconnected.\n");

  if (id->qp != NULL) rdma_destroy_qp(id);
  /*
  ** need to inform the associated TCP connection that the other side has performed a disconnection
  */
  free(conn);

  rdma_destroy_id(id);

  return 0;
}


/*
**__________________________________________________
*/
/**
   Server mode event dispatch
 
   @param event :RDMA event associated with the connection
   
   retval 0 on sucess
   retval -1 on error
*/  
int rozofs_rdma_srv_on_event(struct rdma_cm_event *event)
{
//  int r = 0;

  if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST){
    return rozofs_rdma_srv_on_connect_request(event->id,event);
  }
  /*
  ** call the FSM
  */
  rozofs_rdma_srv_fsm_exec(NULL,event,ROZOFS_RDMA_EV_RDMA_EVT);
  return 0;

}

/*
**__________________________________________________
*/
/**
*  Get the RDMA context from the file descriptor index
  
   @param fd :reference of the file descriptor
   
   @retval: pointer to the connection context
   
*/
rozofs_rdma_connection_t *rozofs_rdma_get_connection_context_from_fd(int fd)
{

   if (fd >=rozofs_nb_rmda_tcp_context)
   {
     /*
     ** out of range context
     */
     errno = ERANGE;
     return NULL;
   }
   return rozofs_rdma_tcp_table[fd];  
}
/*
**__________________________________________________
*/
/**
*  Get the TCP context from index of the AF_unix context
  
   @param af_unix_id : reference of the af_unix context
   @param alloc:   set to 1 if the context need to be allocated if is does not exists
   
   @retval: pointer to the TCP connection context
   
*/
rozofs_rdma_tcp_cnx_t *rozofs_rdma_get_tcp_connection_context_from_af_unix_id(int af_unix_id,int alloc)
{

   if (af_unix_id >=rozofs_nb_rmda_tcp_context)
   {
     /*
     ** out of range context
     */
     errno = ERANGE;
     return NULL;
   }
   /*
   ** need to allocated a context if none exists
   */
   if (tcp_cnx_tb[af_unix_id] == NULL)
   {
     if (alloc == 0) return NULL;
     /*
     ** allocate a new context
     */
     tcp_cnx_tb[af_unix_id] = malloc(sizeof(rozofs_rdma_tcp_cnx_t));
     if (tcp_cnx_tb[af_unix_id] == NULL)
     {
	severe("Cannot allocated a TCP context to associated with RDMA");
	return NULL;
     }
     memset(tcp_cnx_tb[af_unix_id],0,sizeof(rozofs_rdma_tcp_cnx_t));
   }   
   return tcp_cnx_tb[af_unix_id];     
}
/*
**__________________________________________________
*/
/**
*  Get the RDMA context from the association block for the server side
  
   @param assoc : pointer to the association context that contains the reference of the client reference
   
   @retval: pointer to the connection context
   
*/
rozofs_rdma_connection_t *rozofs_rdma_get_srv_connection_context_from_assoc(rozofs_rdma_tcp_assoc_t *assoc)
{

   if (assoc->srv_ref >=rozofs_nb_rmda_tcp_context)
   {
     /*
     ** out of range context
     */
     errno = ERANGE;
     return NULL;
   }
   return rozofs_rdma_tcp_table[assoc->srv_ref];  
}
/*
**__________________________________________________
*/
/**
*  
  @param assoc: pointer to the association context
  @param event: RDMA event context
  @param evt_code : event code
  
*/
void rozofs_rdma_srv_fsm_exec(rozofs_rdma_tcp_assoc_t *assoc,struct rdma_cm_event *event,rozofs_rdma_internal_event_e evt_code)
{
   rozofs_rdma_connection_t *conn;
   rozofs_rdma_tcp_assoc_t assoc_loc;
   int ret;

   /*
   ** Get the connection context: either from the assoc context or for event context
   */
   if (assoc != 0)
   {
     conn = rozofs_rdma_get_srv_connection_context_from_assoc(assoc);
   }
   else
   {
     conn = event->id->context;
   }
   if (conn ==NULL)
   {
     rozofs_print_rdma_fsm_state(-1,evt_code,(event==NULL)?0:event->event);
     warning("NULL context assoc %p event %p evt_code %d\n",assoc,event,evt_code); 
     /*
     ** send a RDMA DEL indication towards the TCP side
     */
     return;   
   }
   rozofs_print_rdma_fsm_state(conn->state,evt_code,(event==NULL)?0:event->event);
   memcpy(&assoc_loc,&conn->assoc,sizeof(rozofs_rdma_tcp_assoc_t));
//#warning RDMA srv trace
//   info("FDL Server CNX %p state %d  evt_code %d event %d",conn,conn->state,evt_code,(event != NULL)?event->event:-1);   
   switch (conn->state)
   {
      case ROZOFS_RDMA_ST_IDLE:
	/*
	** nothing to do for that case
	*/
	break;
       /*
       ** information is retrieved from the event context
       */
       /*
       ** ROZOFS_RDMA_ST_WAIT_ESTABLISHED
       */
       case ROZOFS_RDMA_ST_WAIT_ESTABLISHED:
        if (evt_code == ROZOFS_RDMA_EV_RDMA_EVT)
	{
	  if (event->event == RDMA_CM_EVENT_ESTABLISHED)
	  {
             /*
	     ** the connection is UP
	     */
	     ret = rozofs_rdma_srv_on_connection(event->id->context);
	     if (ret < 0)
	     {
		rozofs_rdma_release_context(conn);
		return;  	    	 
	     }
	     /*
	     ** informs the TCP side that RDMA connection is UP and Running
	     */
	     rozofs_rdma_send2tcp_side(ROZOFS_RDMA_EV_RDMA_ESTABLISHED_IND,&assoc_loc);
	     conn->state = ROZOFS_RDMA_ST_ESTABLISHED;
	     return;	       
	  }
	  rozofs_rdma_release_context(conn);
	  return;
	}
	/*
	** case of an event code received on the af_unix connection
	*/
	rozofs_rdma_release_context(conn);
	return;	    
	break;
	
       /*
       ** ROZOFS_RDMA_ST_ESTABLISHED
       */             
       case ROZOFS_RDMA_ST_ESTABLISHED:
	 /*
	 ** any message while the FSM is in that state triggers a deletion of the RDMA context
	 ** It is up to the TCP side to restart the connection process
	 */
	rozofs_rdma_release_context(conn);
	if (evt_code != ROZOFS_RDMA_EV_RDMA_DEL_REQ) rozofs_rdma_send2tcp_side(ROZOFS_RDMA_EV_RDMA_DEL_IND,&assoc_loc);
	return;
	 break;

       case ROZOFS_RDMA_ST_ERROR:
	 break;

       default:
	rozofs_rdma_release_context(conn);
	return;
	 break;   
   }
}



/*
**___________________________________________________________________________

       C L I E N T   S I D E 
**___________________________________________________________________________
*/
/**
*  RDMA connection request processing

  @param assoc_p: pointer to the RDMA/TCP association context
  
  @retval 0 on success
  @retval -1 on error
*/
int rozofs_rdma_cli_connect_req(rozofs_rdma_tcp_assoc_t *assoc_p)
{
   rozofs_rdma_connection_t *conn;
   rozofs_rdma_connection_t *prev_conn=NULL;
   struct sockaddr_in dest_addr;
   
   /*
   ** check if the client index is in range
   */
   if (assoc_p->cli_ref >=rozofs_nb_rmda_tcp_context)
   {
     /*
     ** out of range context
     */
     errno = ERANGE;
     return -1;
   }
   prev_conn = rozofs_rdma_tcp_table[assoc_p->cli_ref];  
   /*
   ** allocate a connection context
   */
   conn = (rozofs_rdma_connection_t *)malloc(sizeof(rozofs_rdma_connection_t));  
   if (conn == NULL)
   {
     /*
     ** out of memory--> abort
     */
     fatal("Out of memory : %s\n",strerror(errno));
   }
   rozofs_allocated_rdma_tcp_contexts++;
   rozofs_cur_act_rmda_tcp_context++;
   list_init(&conn->list);
   memset( conn,0,sizeof(rozofs_rdma_connection_t));
   memcpy(&conn->assoc,assoc_p,sizeof(rozofs_rdma_tcp_assoc_t));
   conn->state = ROZOFS_RDMA_ST_IDLE;
   conn->tcp_index = assoc_p->cli_ref;
   rozofs_rdma_tcp_table[assoc_p->cli_ref]=conn;
   /*
   ** release the previous context if any
   */
   if (prev_conn) 
   {
     rozofs_rdma_release_context(prev_conn);
   }
   RDMA_STATS_REQ(rdma_create_id);
   if(rdma_create_id(rozofs_rdma_ec, &conn->id, NULL, RDMA_PS_TCP) < 0)
   {
      RDMA_STATS_RSP_NOK(rdma_create_id);
      if (errno == ENODEV) {
        info("RDMA rdma_create_id error:%s",strerror(errno));
      }
      else {     
        severe("RDMA rdma_create_id error:%s",strerror(errno));
      }  
      goto error;
   }
   RDMA_STATS_RSP_OK(rdma_create_id);
   /*
   ** register the user RDMA/TCP connection in the RDMA connection context returned
   */
   conn->id->context = conn;
   dest_addr.sin_family = AF_INET;   
   dest_addr.sin_port = htons(assoc_p->port_srv);
   dest_addr.sin_addr.s_addr =  htonl(assoc_p->ip_srv);
   /*
   ** initiate the connection
   */
   RDMA_STATS_REQ(rdma_resolve_addr);
   if(rdma_resolve_addr(conn->id, NULL, (struct sockaddr*)&dest_addr, TIMEOUT_IN_MS) < 0)
   {
     RDMA_STATS_RSP_NOK(rdma_resolve_addr);
     severe("RDMA  rdma_resolve_addr error for address %x:%d:%s", assoc_p->ip_srv,assoc_p->port_srv,strerror(errno));
     goto error;
   }

  return 0;
  
error:
   if (conn)
   {
     rozofs_rdma_release_context(conn);   
   }
   return -1;
}
/*
**__________________________________________________
*/
/**
   address resolved on client side 
   it is associated with the RDMA_CM_EVENT_ADDR_RESOLVED event
   
   
   That API is intended :
    1- build the RDMA context if it does not exist
    2- prepare the queue pair attributes
    3- create the queue pair for the connection
    4- create a local connection context
    5- issue a rdma_resolve_route
   
   @param id :pointer to rdma cm identifier

	
   retval 0 on sucess
   retval -1 on error
*/ 

int rozofs_rdma_cli_on_addr_resolved(struct rdma_cm_id *id)
{
  struct ibv_qp_init_attr qp_attr;
  rozofs_rdma_connection_t *conn;
  rozofs_rmda_ibv_cxt_t *s_ctx;  

  info("address resolved.\n");

  RDMA_STATS_RSP_OK(rdma_resolve_addr);
  s_ctx = rozofs_build_rdma_context(id->verbs);
  if (s_ctx == NULL) goto error;

  rozofs_build_qp_attr(&qp_attr,s_ctx);
  
  RDMA_STATS_REQ(rdma_create_qp);
  if(rdma_create_qp(id, s_ctx->pd, &qp_attr) < 0)
  {
     /*
     ** error on queue pair creation
     */
     RDMA_STATS_RSP_NOK(rdma_create_qp);
     severe("RDMA rdma_create_qp error:%s",strerror(errno));
     goto error;
  }    
  
  RDMA_STATS_RSP_OK(rdma_create_qp);
  conn = id->context;
  conn->qp = id->qp;
  conn->s_ctx = s_ctx;
  /*
  ** Save the QP number in the connection context
  */
  conn->qp_num = conn->qp->qp_num;
  /*
  ** set the index of the completion queues: for statistics purpose
  */
  conn->cq_xmit_idx = s_ctx->next_cq_idx;
  conn->cq_recv_idx = s_ctx->next_cq_idx;
  /*
  ** Need to insert the client local reference in the QP hash table
  ** conn->assoc_p->cli_ref
  */
  {
    struct ibv_qp_attr attr;

     memset(&attr, 0, sizeof(attr));

     attr.qp_access_flags =  IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
     attr.max_dest_rd_atomic = 16;
     attr.max_rd_atomic = 16;
     RDMA_STATS_REQ(ibv_modify_qp);
     if (ibv_modify_qp(id->qp, &attr,IBV_QP_ACCESS_FLAGS|IBV_QP_MAX_DEST_RD_ATOMIC| IBV_QP_MAX_QP_RD_ATOMIC)< 0) 
     {
       RDMA_STATS_RSP_NOK(ibv_modify_qp);
       severe("RDMA ibv_modify_qp error:%s",strerror(errno));
       goto error;
     }
     RDMA_STATS_RSP_OK(ibv_modify_qp);
  }
  post_receives(conn);
  RDMA_STATS_REQ(rdma_resolve_route);
  if(rdma_resolve_route(id, TIMEOUT_IN_MS) < 0)
  {
     RDMA_STATS_RSP_NOK(rdma_resolve_route);
     severe("RDMA rdma_resolve_route error:%s",strerror(errno));
     goto error;  
  }
  return 0;
error:
  return -1;
}
/*
**__________________________________________________
*/
/**
   route resolved on client side 
   it is associated with the RDMA_CM_EVENT_ADDR_RESOLVED event

   That API is intended :
    1- build the rdma_conn_param context
    5- issue a rdma_connect
    
       
   @param id :pointer to rdma cm identifier
 struct rdma_conn_param {
	const void *private_data;
	uint8_t private_data_len;
	uint8_t responder_resources;
	uint8_t initiator_depth;
	uint8_t flow_control;
	uint8_t rnr_retry_count;
	
	uint8_t srq;
	uint32_t qp_num;  
	
   retval 0 on sucess
   retval -1 on error
*/ 

int rozofs_rdma_cli_on_route_resolved(struct rdma_cm_id *id)
{
  struct rdma_conn_param cm_params;
  rozofs_rdma_connection_t *conn;
  RDMA_STATS_RSP_OK(rdma_resolve_route);
  RDMA_STATS_REQ(rdma_connect);
//  printf("route resolved.\n");
  conn = id->context;
  memset(&cm_params, 0, sizeof(cm_params));
  cm_params.private_data =&conn->assoc;
  cm_params.private_data_len = sizeof(rozofs_rdma_tcp_assoc_t)+32;
  info ("cli/srv_ref %d/%d port_cli/srv %d/%d len %d\n",conn->assoc.cli_ref,
             conn->assoc.srv_ref,
	     conn->assoc.port_cli,
	     conn->assoc.port_srv,
	     cm_params.private_data_len);
	     
  /*
  ** note the client can provides information related to the associated TCP
  ** connection thanks private_data
  */
  cm_params.responder_resources = 16;
  cm_params.initiator_depth = 16;
  cm_params.retry_count = 2;
  cm_params.rnr_retry_count = 3;  
  
  if(rdma_connect(id, &cm_params) < 0)
  {
     severe("rdma_connect error:%s",strerror(errno));
     goto error;
  }
  RDMA_STATS_RSP_OK(rdma_connect);
  return 0;
error:
  RDMA_STATS_RSP_NOK(rdma_connect);
  return -1;
}

/*
**__________________________________________________
*/
/**
   answer to rdma_connect on client side 
   it is associated with the RDMA_CM_EVENT_ESTABLISHED event


       
   @param id :pointer to rdma cm identifier
 struct rdma_conn_param {
	const void *private_data;
	uint8_t private_data_len;
	uint8_t responder_resources;
	uint8_t initiator_depth;
	uint8_t flow_control;
	uint8_t rnr_retry_count;
	
	uint8_t srq;
	uint32_t qp_num;  
	
   retval 0 on sucess
   retval -1 on error
*/ 
int rozofs_rdma_cli_on_connection(void *context)
{
  rozofs_rdma_connection_t *conn = (rozofs_rdma_connection_t *)context;
  rozofs_rdma_tcp_assoc_t *p = &conn->assoc;
  info("RDMA %u.%u.%u.%u:%d -> %u.%u.%u.%u:%d connection ESTABLISHED....",
        p->ip_cli >> 24, (p->ip_cli >> 16)&0xFF, (p->ip_cli >> 8)&0xFF, p->ip_cli & 0xFF,p->port_cli,
        p->ip_srv >> 24, (p->ip_srv >> 16)&0xFF, (p->ip_srv >> 8)&0xFF, p->ip_srv & 0xFF,p->port_srv
	);

  return 0;
}

/*
**__________________________________________________
*/
/**
   processing of a disconnection
   it is associated with the RDMA_CM_EVENT_DISCONNECTED event

   processing:
   1- destroy the queue pair
   2-release the connection context
   3- release the cm_id context
       
   @param id :pointer to rdma cm identifier

	
   retval 0 on sucess
   retval -1 on error
*/ 
int rozofs_rdma_cli_on_disconnect(struct rdma_cm_id *id)
{
  rozofs_rdma_connection_t *conn = (rozofs_rdma_connection_t *)id->context;

  info("disconnected.\n");

  rdma_destroy_qp(id);

  free(conn);

  rdma_destroy_id(id);

  return 1; /* exit event loop */
}

/*
**__________________________________________________
*/
/**
   Client mode event dispatch
 
   @param event :RDMA event associated with the connection
   
   retval 0 on sucess
   retval -1 on error
*/  
int rozofs_rdma_cli_on_event(struct rdma_cm_event *event)
{
  rozofs_rdma_cli_fsm_exec(NULL,event,ROZOFS_RDMA_EV_RDMA_EVT);
  return 0;
  
#if 0
  int r = 0;

  if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
    r = rozofs_rdma_cli_on_addr_resolved(event->id);
  else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
    r = rozofs_rdma_cli_on_route_resolved(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
    r = rozofs_rdma_cli_on_connection(event->id->context);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
    r = rozofs_rdma_cli_on_disconnect(event->id);
  else
    die("on_event: unknown event.");

  return r;
#endif
}



/*
**__________________________________________________
*/
/**
*  Get the RDMA context from the association block
  
   @param assoc : pointer to the association context that contains the reference of the client reference
   
   @retval: pointer to the connection context
   
*/
rozofs_rdma_connection_t *rozofs_rdma_get_cli_connection_context_from_assoc(rozofs_rdma_tcp_assoc_t *assoc)
{

   if (assoc->cli_ref >=rozofs_nb_rmda_tcp_context)
   {
     /*
     ** out of range context
     */
     errno = ERANGE;
     return NULL;
   }
   return rozofs_rdma_tcp_table[assoc->cli_ref];  
}

/*
**__________________________________________________
*/
/**
*  
  @param assoc: pointer to the association context
  @param event: RDMA event context
  @param evt_code : event code
  
*/
void rozofs_rdma_cli_fsm_exec(rozofs_rdma_tcp_assoc_t *assoc,struct rdma_cm_event *event,rozofs_rdma_internal_event_e evt_code)
{
   rozofs_rdma_connection_t *conn;
   rozofs_rdma_tcp_assoc_t assoc_loc;
   int ret;

   /*
   ** Get the connection context: either from the assoc context or for event context
   */
   if (assoc != 0)
   {
     conn = rozofs_rdma_get_cli_connection_context_from_assoc(assoc);
   }
   else
   {
     conn = event->id->context;
   }
   if (conn ==NULL)
   {
      if (evt_code != ROZOFS_RDMA_EV_RDMA_CONNECT)
      {
        rozofs_print_rdma_fsm_state(-1,evt_code,(event==NULL)?0:event->event);

	warning("FDL NULL context assoc %p event %p evt_code %d\n",assoc,event,evt_code); 
	/*
	** send a RDMA DEL indication towards the TCP side
	*/
	return;   
      }
      /*
      ** if the event is ROZOFS_RDMA_EV_RDMA_CONNECT we create a connection context
      */
      ret = rozofs_rdma_cli_connect_req(assoc);
      if ( ret < 0)
      {
	 /*
	 ** send a RDMA DEL indication towards the TCP side
	 */
	 rozofs_rdma_send2tcp_side(ROZOFS_RDMA_EV_RDMA_DEL_IND,assoc);
	 return;    
      }
      /*
      ** Get the connection context
      */
      conn = rozofs_rdma_get_cli_connection_context_from_assoc(assoc);
      conn->state = ROZOFS_RDMA_ST_WAIT_ADDR_RESOLVED;
      return;

   }

//   info("FDL Client CNX %p state %d  evt_code %d event %d",conn,conn->state,evt_code,(event != NULL)?event->event:-1);   
   rozofs_print_rdma_fsm_state(conn->state,evt_code,(event==NULL)?0:event->event);
   memcpy(&assoc_loc,&conn->assoc,sizeof(rozofs_rdma_tcp_assoc_t));

   switch (conn->state)
   {
      case ROZOFS_RDMA_ST_IDLE:
	/*
	** nothing to do for that case
	*/
	break;
       /*
       ** information is retrieved from the event context
       */
       /*
       ** ROZOFS_RDMA_ST_WAIT_ADDR_RESOLVED
       */
       case ROZOFS_RDMA_ST_WAIT_ADDR_RESOLVED:
        if (evt_code == ROZOFS_RDMA_EV_RDMA_EVT)
	{
	  if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
	  {
             /*
	     ** the address has been resolved
	     */
	     ret = rozofs_rdma_cli_on_addr_resolved(event->id);
	     if (ret < 0)
	     {
		rozofs_rdma_release_context(conn);
		rozofs_rdma_send2tcp_side(ROZOFS_RDMA_EV_RDMA_DEL_IND,&assoc_loc);
		return;  	    	 
	     }
	     conn->state = ROZOFS_RDMA_ST_WAIT_ROUTE_RESOLVED;
	     return;	       
	  }
	  rozofs_rdma_release_context(conn);
	  rozofs_rdma_send2tcp_side(ROZOFS_RDMA_EV_RDMA_DEL_IND,&assoc_loc);
	  return; 
	}
	/*
	** case of an event code received on the af_unix connection
	*/
	rozofs_rdma_release_context(conn);
	if (evt_code != ROZOFS_RDMA_EV_RDMA_DEL_REQ) rozofs_rdma_send2tcp_side(ROZOFS_RDMA_EV_RDMA_DEL_IND,&assoc_loc);
	return;	       
	break;
       /*
       ** ROZOFS_RDMA_ST_WAIT_ROUTE_RESOLVED
       */
       case ROZOFS_RDMA_ST_WAIT_ROUTE_RESOLVED:
        if (evt_code == ROZOFS_RDMA_EV_RDMA_EVT)
	{
	  if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
	  {
             /*
	     ** the address has been resolved
	     */
	     ret = rozofs_rdma_cli_on_route_resolved(event->id);
	     if (ret < 0)
	     {
		rozofs_rdma_release_context(conn);
		rozofs_rdma_send2tcp_side(ROZOFS_RDMA_EV_RDMA_DEL_IND,&assoc_loc);
		return;  	    	 
	     }
	     conn->state = ROZOFS_RDMA_ST_WAIT_ESTABLISHED;
	     return;	       
	  }
	  rozofs_rdma_release_context(conn);
	  rozofs_rdma_send2tcp_side(ROZOFS_RDMA_EV_RDMA_DEL_IND,&assoc_loc);
	  return;
	}
	/*
	** case of an event code received on the af_unix connection
	*/
	rozofs_rdma_release_context(conn);
	if (evt_code != ROZOFS_RDMA_EV_RDMA_DEL_REQ) rozofs_rdma_send2tcp_side(ROZOFS_RDMA_EV_RDMA_DEL_IND,&assoc_loc);
	return;	    
	 break;
       /*
       ** ROZOFS_RDMA_ST_WAIT_ROUTE_RESOLVED
       */       
       case ROZOFS_RDMA_ST_WAIT_ESTABLISHED:
        if (evt_code == ROZOFS_RDMA_EV_RDMA_EVT)
	{
	  if (event->event == RDMA_CM_EVENT_ESTABLISHED)
	  {
             /*
	     ** the connection is UP
	     */
	     ret = rozofs_rdma_cli_on_connection(event->id);
	     if (ret < 0)
	     {
		rozofs_rdma_release_context(conn);
		rozofs_rdma_send2tcp_side(ROZOFS_RDMA_EV_RDMA_DEL_IND,&assoc_loc);
		return;  	    	 
	     }
	     /*
	     ** informs the TCP side that RDMA connection is UP and Running
	     */
	     rozofs_rdma_send2tcp_side(ROZOFS_RDMA_EV_RDMA_ESTABLISHED_IND,&assoc_loc);
	     conn->state = ROZOFS_RDMA_ST_ESTABLISHED;
	     return;	       
	  }
	  rozofs_rdma_release_context(conn);
	  rozofs_rdma_send2tcp_side(ROZOFS_RDMA_EV_RDMA_DEL_IND,&assoc_loc);
	  return;
	}
	/*
	** case of an event code received on the af_unix connection
	*/
	rozofs_rdma_release_context(conn);
	if (evt_code != ROZOFS_RDMA_EV_RDMA_DEL_REQ) rozofs_rdma_send2tcp_side(ROZOFS_RDMA_EV_RDMA_DEL_IND,&assoc_loc);
	return;	    
	break;
	
       /*
       ** ROZOFS_RDMA_ST_ESTABLISHED
       */             
       case ROZOFS_RDMA_ST_ESTABLISHED:
	 /*
	 ** any message while the FSM is in that state triggers a deletion of the RDMA context
	 ** It is up to the TCP side to restart the connection process
	 */
	 info("FDL Client Disconnect evt_code %d event %d",evt_code,(event != NULL)?event->event:-1);
	rozofs_rdma_release_context(conn);
	if (evt_code != ROZOFS_RDMA_EV_RDMA_DEL_REQ) rozofs_rdma_send2tcp_side(ROZOFS_RDMA_EV_RDMA_DEL_IND,&assoc_loc);
	return;
	 break;

       case ROZOFS_RDMA_ST_ERROR:
	 break;

       default:
	rozofs_rdma_release_context(conn);
	if (evt_code != ROZOFS_RDMA_EV_RDMA_DEL_REQ) rozofs_rdma_send2tcp_side(ROZOFS_RDMA_EV_RDMA_DEL_IND,&assoc_loc);
	return;
	 break;   
   }
}


/*
**___________________________________________________________________________

       T C P   F S M   C L I E N T   S I D E 
**___________________________________________________________________________
*/

/*
**__________________________________________________
*/
/**
*  Get the TCP context from the association block for the client side
  
   @param assoc : pointer to the association context that contains the reference of the client reference
   
   @retval: pointer to the TCP connection context
   
*/
rozofs_rdma_tcp_cnx_t *rozofs_rdma_get_cli_tcp_connection_context_from_assoc(rozofs_rdma_tcp_assoc_t *assoc)
{
   if (rozofs_rdma_signalling_thread_mode == 0)
   {
      fatal("the module is configured in RDMA server mode");
   }
   if (assoc->cli_ref >=rozofs_nb_rmda_tcp_context)
   {
     /*
     ** out of range context
     */
     errno = ERANGE;
     return NULL;
   }
   if (tcp_cnx_tb[assoc->cli_ref] == NULL)
   {
     /*
     ** allocate a new context
     */
     tcp_cnx_tb[assoc->cli_ref] = malloc(sizeof(rozofs_rdma_tcp_cnx_t));
     if (tcp_cnx_tb[assoc->cli_ref] == NULL)
     {
	severe("Cannot allocated a TCP context to associated with RDMA");
	return NULL;
     }
     memset(tcp_cnx_tb[assoc->cli_ref],0,sizeof(rozofs_rdma_tcp_cnx_t));
   }   
   return tcp_cnx_tb[assoc->cli_ref];  
}
/*
**__________________________________________________
*/
/**
 The purpose of that service is to re-establish the RDMA connection by posting
 a RDMA_CONNECT_REQ towards the RDMA signalling thread of RozoFS.
 
 That service is intended to be called when the client detects that the connection
 on the TCP side is in the ROZOFS_RDMA_ST_TCP_WAIT_RDMA_RECONNECT state
 
 It is assumed that the reference of the server side is still valid since that
 service is called when the TCP connection is still UP.
 
 @param conn: pointer to the tcp_rdma connection context
 
 @retval none
*/ 
void rozofs_rdma_tcp_cli_reconnect(rozofs_rdma_tcp_cnx_t *conn)
{
  int ret;
  
  /*
  ** check if the max attempt has been reached
  */
  if (rdma_sig_reconnect_credit < 0)
  {
    /*
    ** too many reconnect attempt
    */
    return;
  }
  rdma_sig_reconnect_credit--;
  RDMA_STATS_REQ(rdma_reconnect);
  ret = rozofs_rdma_send2rdma_side(ROZOFS_RDMA_EV_RDMA_CONNECT,&conn->assoc);
  if (ret < 0)
  {
     RDMA_STATS_RSP_NOK(rdma_reconnect);
    conn->state = ROZOFS_RDMA_ST_TCP_DEAD;
    return;
  }
  RDMA_STATS_RSP_OK(rdma_reconnect);
  conn->state = ROZOFS_RDMA_ST_TCP_WAIT_RDMA_CONNECTED;

}
/*
**__________________________________________________
*/
/**
*  
  @param assoc: pointer to the association context
  @param evt_code : event code
  
*/
void rozofs_rdma_tcp_cli_fsm_exec(rozofs_rdma_tcp_assoc_t *assoc,rozofs_rdma_internal_event_e evt_code)
{
   rozofs_rdma_tcp_cnx_t *conn=NULL;
   int ret;

   /*
   ** Get the connection context: either from the assoc context or for event context
   */
   if (assoc != 0)
   {
     conn = rozofs_rdma_get_cli_tcp_connection_context_from_assoc(assoc);
   }

   if (conn ==NULL)
   {
      rozofs_print_tcp_fsm_state(ROZOFS_RDMA_ST_TCP_MAX,evt_code);
      /*
      ** send a RDMA DEL indication towards the TCP side
      */
      return;   
   }
   rozofs_print_tcp_fsm_state(conn->state,evt_code);
   switch (conn->state)
   {
      case ROZOFS_RDMA_ST_TCP_IDLE:
	/*
	** nothing to do for that case
	*/
	break;
       /*
       ** wait response on RDMA connect request
       */
       /*
       ** ROZOFS_RDMA_ST_TCP_WAIT_RDMA_REQ_RSP
       */
       case ROZOFS_RDMA_ST_TCP_WAIT_RDMA_REQ_RSP:
         switch (evt_code)
	 {
	    case ROZOFS_RDMA_EV_TCP_RDMA_RSP_ACCEPT:
	      /*
	      ** trigger the setup of the RDMA connection
	      */
	      conn->assoc.srv_ref = assoc->srv_ref;
	      ret = rozofs_rdma_send2rdma_side(ROZOFS_RDMA_EV_RDMA_CONNECT,assoc);
	      if (ret < 0)
	      {
		conn->state = ROZOFS_RDMA_ST_TCP_DEAD;
		break;
	      }
	      conn->state = ROZOFS_RDMA_ST_TCP_WAIT_RDMA_CONNECTED;
	      break;

	    case ROZOFS_RDMA_EV_TCP_RDMA_RSP_REJECT:
	      conn->state = ROZOFS_RDMA_ST_TCP_IDLE;
	      break;

	    default:
	     break;	
	 }
	 break;
       /*
       ** ROZOFS_RDMA_ST_TCP_WAIT_RDMA_CONNECTED
       */
       case ROZOFS_RDMA_ST_TCP_WAIT_RDMA_CONNECTED:
         switch (evt_code)
	 {
	    case ROZOFS_RDMA_EV_RDMA_ESTABLISHED_IND:
	      conn->state = ROZOFS_RDMA_ST_TCP_RDMA_ESTABLISHED;
	      /*
	      ** indicates that RDMA is available 
	      */
	      if (conn->state_cbk!= NULL) (conn->state_cbk)(conn->opaque_ref,1,conn->assoc.cli_ref);
	      break;

	    case ROZOFS_RDMA_EV_RDMA_DEL_IND:
	      conn->state = ROZOFS_RDMA_ST_TCP_IDLE;
	      break;

	    case ROZOFS_RDMA_EV_TCP_DISCONNECT:
	      rozofs_rdma_send2rdma_side(ROZOFS_RDMA_EV_RDMA_DEL_REQ,assoc);
	      conn->state = ROZOFS_RDMA_ST_TCP_DEAD;
	      break;

	    default:
	     break;	
	 }       
       
	 break;
       /*
       ** ROZOFS_RDMA_ST_TCP_RDMA_ESTABLISHED
       */       
       case ROZOFS_RDMA_ST_TCP_RDMA_ESTABLISHED:
         switch (evt_code)
	 {
	    case ROZOFS_RDMA_EV_RDMA_DEL_IND:
	      /*
	      ** indicates that RDMA is DOWN
	      */
	      if (conn->state_cbk!= NULL) (conn->state_cbk)(conn->opaque_ref,0,conn->assoc.cli_ref);
//	      conn->state = ROZOFS_RDMA_ST_TCP_IDLE;
	      /*
	      ** register the time at which the RDMA has reported a down state.
	      ** It is intended to be used for retrying to establish the RDMA connection
	      */
	      conn->last_down_ts = rozofs_ticker_microseconds;
	      conn->state = ROZOFS_RDMA_ST_TCP_WAIT_RDMA_RECONNECT;
	      
	      break;

	    case ROZOFS_RDMA_EV_TCP_DISCONNECT:
	      rozofs_rdma_send2rdma_side(ROZOFS_RDMA_EV_RDMA_DEL_REQ,assoc);
	      conn->state = ROZOFS_RDMA_ST_TCP_DEAD;
	      break;

	    default:
	     break;	
	 }       
	break;

       /*
       ** ROZOFS_RDMA_ST_TCP_WAIT_RDMA_RECONNECT
       */ 
       /*
       ** The FSM enters that state when the client has received at deleition indication from the
       ** RDMA side. Only the TCP disconnect is considered when the FSM is in that state since we should
       ** not received any other message for tha RDMA connection from the RDMA side
       */      
       case ROZOFS_RDMA_ST_TCP_WAIT_RDMA_RECONNECT:
         switch (evt_code)
	 {
	    case ROZOFS_RDMA_EV_TCP_DISCONNECT:
	      rozofs_rdma_send2rdma_side(ROZOFS_RDMA_EV_RDMA_DEL_REQ,assoc);
	      conn->state = ROZOFS_RDMA_ST_TCP_DEAD;
	      break;

	    default:
	     break;		 
	 
	 }
         break;
       /*
       ** ROZOFS_RDMA_ST_TCP_DEAD
       */       
       case ROZOFS_RDMA_ST_TCP_DEAD:	
       default:
	 break;   
   }
}


/*
**___________________________________________________________________________

       T C P   F S M   S E R V E R    S I D E 
**___________________________________________________________________________
*/

/*
**__________________________________________________
*/
/**
*  Get the TCP context from the association block for the server side
  
   @param assoc : pointer to the association context that contains the reference of the server reference
   
   @retval: pointer to the TCP connection context
   
*/
rozofs_rdma_tcp_cnx_t *rozofs_rdma_get_srv_tcp_connection_context_from_assoc(rozofs_rdma_tcp_assoc_t *assoc)
{
   if (rozofs_rdma_signalling_thread_mode == 1)
   {
      fatal("the module is configured in RDMA client mode");
   }
   if (assoc->srv_ref >=rozofs_nb_rmda_tcp_context)
   {
     /*
     ** out of range context
     */
     errno = ERANGE;
     return NULL;
   }
   if (tcp_cnx_tb[assoc->srv_ref] == NULL)
   {
     /*
     ** allocate a new context
     */
     tcp_cnx_tb[assoc->srv_ref] = malloc(sizeof(rozofs_rdma_tcp_cnx_t));
     if (tcp_cnx_tb[assoc->srv_ref] == NULL)
     {
	severe("Cannot allocated a TCP context to associated with RDMA for index %d",assoc->srv_ref);
	return NULL;
     }
     memset(tcp_cnx_tb[assoc->srv_ref],0,sizeof(rozofs_rdma_tcp_cnx_t));
   }   
   return tcp_cnx_tb[assoc->srv_ref];  
}


/*
**__________________________________________________
*/
/**
*  
  @param assoc: pointer to the association context
  @param evt_code : event code
  
*/
void rozofs_rdma_tcp_srv_fsm_exec(rozofs_rdma_tcp_assoc_t *assoc,rozofs_rdma_internal_event_e evt_code)
{
   rozofs_rdma_tcp_cnx_t *conn=NULL;
//   int ret;

   /*
   ** Get the connection context: either from the assoc context or for event context
   */
   if (assoc != 0)
   {
     conn = rozofs_rdma_get_srv_tcp_connection_context_from_assoc(assoc);
   }

   if (conn ==NULL)
   {
      rozofs_print_tcp_fsm_state(ROZOFS_RDMA_ST_TCP_MAX,evt_code);
      /*
      ** send a RDMA DEL indication towards the TCP side
      */
      return;   
   }
   rozofs_print_tcp_fsm_state(conn->state,evt_code);

   switch (conn->state)
   {
      case ROZOFS_RDMA_ST_TCP_IDLE:
	/*
	** nothing to do for that case
	*/
	break;
       /*
       ** wait response on RDMA connect request
       */
       /*
       ** ROZOFS_RDMA_ST_TCP_WAIT_RDMA_REQ
       */
       case ROZOFS_RDMA_ST_TCP_WAIT_RDMA_REQ:
         switch (evt_code)
	 {
	    case ROZOFS_RDMA_EV_TCP_RDMA_REQ:
#if 0
	      /*
	      ** trigger the setup of the RDMA connection
	      */
	      ret = rozofs_rdma_send2rdma_side(ROZOFS_RDMA_EV_RDMA_CONNECT,assoc);
	      if (ret < 0)
	      {
		conn->state = ROZOFS_RDMA_ST_TCP_DEAD;
		break;
	      }
#endif
	      conn->state = ROZOFS_RDMA_ST_TCP_RDMA_CONNECTING;
	      break;

	    case ROZOFS_RDMA_EV_TCP_DISCONNECT:
	      conn->state = ROZOFS_RDMA_ST_TCP_DEAD;
	      break;

	    default:
	     break;	
	 }
	 break;
       /*
       ** ROZOFS_RDMA_ST_TCP_RDMA_CONNECTING
       */
       case ROZOFS_RDMA_ST_TCP_RDMA_CONNECTING:
         switch (evt_code)
	 {
	    case ROZOFS_RDMA_EV_RDMA_ESTABLISHED_IND:
	      conn->state = ROZOFS_RDMA_ST_TCP_RDMA_ESTABLISHED;
	      break;

	    case ROZOFS_RDMA_EV_RDMA_DEL_IND:
	      conn->state = ROZOFS_RDMA_ST_TCP_IDLE;
	      break;

	    case ROZOFS_RDMA_EV_TCP_DISCONNECT:
	      rozofs_rdma_send2rdma_side(ROZOFS_RDMA_EV_RDMA_DEL_REQ,assoc);
	      conn->state = ROZOFS_RDMA_ST_TCP_DEAD;
	      break;

	    default:
	     break;	
	 }       
       
	 break;
       /*
       ** ROZOFS_RDMA_ST_TCP_RDMA_ESTABLISHED
       */       
       case ROZOFS_RDMA_ST_TCP_RDMA_ESTABLISHED:
         switch (evt_code)
	 {
	    case ROZOFS_RDMA_EV_RDMA_DEL_IND:
	      conn->state = ROZOFS_RDMA_ST_TCP_IDLE;
	      break;

	    case ROZOFS_RDMA_EV_TCP_DISCONNECT:
	      rozofs_rdma_send2rdma_side(ROZOFS_RDMA_EV_RDMA_DEL_REQ,assoc);
	      conn->state = ROZOFS_RDMA_ST_TCP_DEAD;
	      break;

	    default:
	     break;	
	 }       
	break;
       /*
       ** ROZOFS_RDMA_ST_TCP_DEAD
       */       
       case ROZOFS_RDMA_ST_TCP_DEAD:	
       default:
	 break;   
   }
}


/*
**___________________________________________________________________________

       S E R V I C E   T O  T R I G G E R   R D M A transfert 
**___________________________________________________________________________
*/

/*
**__________________________________________________
*/
/**
*  Post either a RDMA read or write command version 2

   @param wr_th_p: pointer to the RDMA thread context need to since it contains the semaphore of the thread
   @param opcode: ROZOFS_TCP_RDMA_READ or ROZOFS_TCP_RDMA_WRITE
   @param bufref: RUC buffer reference needed to find out the reference of the local RDMA key
   @param local_addr: local address of the first byte to transfer
   @param len: length to transfer
   @param remote_addr: remote address
   @param remote_key: RDMA remote key
   
   @retval 0 on success
   @retval -1 on error
   
*/
int rozofs_rdma_post_send2(rozofs_wr_id2_t *wr_th_p,
                           uint8_t opcode,
			   rozofs_rdma_tcp_assoc_t *assoc_p,
			   void *bufref,
			   void *local_addr,
			   int len,
			   uint64_t remote_addr,
			   uint32_t remote_key)
{
    rozofs_rdma_connection_t *conn;
    rozofs_rmda_ibv_cxt_t *s_ctx;
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    struct ibv_mr  *mr_p;    
    
//    info("FDL RDMA_POST_SEND cli/srv:%d/%d ",assoc_p->cli_ref,assoc_p->srv_ref);
    /*
    **____________________________________________________________
    ** Get the RDMA context associated with the file descriptor
    **____________________________________________________________
    */
    conn = rozofs_rdma_get_connection_context_from_fd(assoc_p->srv_ref);
    if (conn ==NULL)
    {
      info("FDL RDMA_POST_SEND conn NULL");
       errno = EPROTO;
      goto error;
    }
    /*
    ** Check if the RDMA connection is still effective
    */
    if ((conn->state != ROZOFS_RDMA_ST_ESTABLISHED) && (conn->state != ROZOFS_RDMA_ST_WAIT_ESTABLISHED))
    {
       /*
       ** RDMA is no more supported on that TCP connection
       */
      info("FDL RDMA_POST_SEND bad state %d",conn->state );

       errno = ENOTSUP;
      goto error;    
    }
    /*
    ** OK the RDMA is operational with that connection so we can proceed with either a
    ** READ or WRITE
    */
    s_ctx = conn->s_ctx;
    /*
    ** get the memory descriptor that contains the local RDMA key
    */
    mr_p = s_ctx->memreg[0];
    if (mr_p == NULL)
    {
      info("FDL RDMA_POST_SEND no memory context" );
      errno = EPROTO;
      goto error;
    }
 
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uint64_t)wr_th_p;
    wr.opcode = (opcode == ROZOFS_TCP_RDMA_WRITE) ? IBV_WR_RDMA_WRITE : IBV_WR_RDMA_READ;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;  
    wr.wr.rdma.remote_addr = (uintptr_t)remote_addr;
    wr.wr.rdma.rkey = remote_key;

    sge.addr = (uintptr_t)local_addr;
    sge.length = len;
    sge.lkey = mr_p->lkey;
//#warning FDL_DBG rdma_post trace
//    info("IBV_WR_RDMA_READ opcode %d QP %llx addr %llx len=%u key=%x raddr %llx rkey %x\n",wr.opcode,conn->qp,sge.addr,sge.length,sge.lkey,wr.wr.rdma.remote_addr,wr.wr.rdma.rkey);
//    info("FDL_DBG rozofs_rdma_post_send2 qp %llx",conn->qp);
    if (ibv_post_send(conn->qp, &wr, &bad_wr) < 0)
    {
       info("FDL RDMA_POST_SEND error: %s",strerror(errno));
       goto error;    
    }
    /*
    ** update statistics
    */
    {
       __atomic_fetch_add(&s_ctx->post_send_stat[conn->cq_xmit_idx%ROZOFS_CQ_THREAD_NUM],1,__ATOMIC_SEQ_CST);
    }
    return 0;
error:
    return -1;
}



/*
**__________________________________________________
*/
/**
*  Post either a RDMA message (RPC) with IBV_WR_SEND

   @param wr_th_p: pointer to the RDMA thread context need to since it contains the semaphore of the thread
   @param bufref: RUC buffer reference needed to find out the reference of the local RDMA key
   @param tcp_cnx_idx: index of the TCP connection in the rozofs_rdma_tcp_table 
   @param local_addr: local address of the first byte to transfer
   @param len: length to transfer

   
   @retval 0 on success
   @retval -1 on error
   
*/
int rozofs_rdma_post_send_ibv_wr_send(rozofs_wr_id2_t *wr_th_p,
			   uint16_t tcp_cnx_idx,
			   void *bufref,
			   void *local_addr,
			   int len)
{
    rozofs_rdma_connection_t *conn;
    rozofs_rmda_ibv_cxt_t *s_ctx;
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    struct ibv_mr  *mr_p;    
    
//    info("FDL RDMA_POST_SEND cli/srv:%d/%d ",assoc_p->cli_ref,assoc_p->srv_ref);
    /*
    **____________________________________________________________
    ** Get the RDMA context associated with the file descriptor
    **____________________________________________________________
    */
    conn = rozofs_rdma_get_connection_context_from_fd(tcp_cnx_idx);
    if (conn ==NULL)
    {
      info("FDL RDMA_POST_SEND conn NULL index %u",tcp_cnx_idx);
       errno = EPROTO;
      goto error;
    }
    /*
    ** Check if the RDMA connection is still effective
    */
    if ((conn->state != ROZOFS_RDMA_ST_ESTABLISHED) && (conn->state != ROZOFS_RDMA_ST_WAIT_ESTABLISHED))
    {
       /*
       ** RDMA is no more supported on that TCP connection
       */
      info("FDL RDMA_POST_SEND bad state %d",conn->state );

       errno = ENOTSUP;
      goto error;    
    }
    /*
    ** OK the RDMA is operational with that connection so we can proceed with either a
    ** READ or WRITE
    */
    s_ctx = conn->s_ctx;

    mr_p = s_ctx->memreg_rpc;
    if (mr_p == NULL)
    {
      fatal("FDL RDMA_POST_SEND no memory context" );
      errno = EPROTO;
      goto error;
    }
 
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uint64_t)wr_th_p;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;  
    sge.addr = (uintptr_t)local_addr;
    sge.length = len;
    sge.lkey = mr_p->lkey;

//    info("IBV_WR_RDMA_READ QP %llx addr %llx len=%u key=%u\n",conn->qp,sge.addr,sge.length,sge.lkey);
//    info("FDL wr.wr_id %p",wr.wr_id);
    if (ibv_post_send(conn->qp, &wr, &bad_wr) < 0)
    {
       info("FDL RDMA_POST_SEND error: %s",strerror(errno));
       goto error;    
    }
    return 0;
error:
    return -1;
}

/*
**_________________________________________________________________________________________

     R D M A   Q U E U E  P A I R   C A C H E 
**_________________________________________________________________________________________
*/
#define QP_HTAB_SIZE 256
#define QP_HTAB_LOCK_SZ 64
int rozofs_rdma_qp_entries;     /**< number of ientries in the cache  */
list_t rozofs_qp_entry_list_head;  /**< linked list of the qp lookup entries */
htable_t rozofs_htable_qp;        /**< queue pair hash table  */
int rozofs_rdma_qp_cache_init_done = 0;  /**< assert to 1 when the init has been done */


static int qp_cmp(void *key1, void *key2) {
    uint32_t *k1_p,*k2_p;
    k1_p = key1;
    k2_p = key2;    
    if (*k1_p == *k2_p) return 0;
    return 1;
}

static unsigned int qp_hash(void *key) {
    uint32_t hash = 0;
    uint8_t *c;
    for (c = key; c != key + sizeof(uint32_t); c++)
        hash = *c + (hash << 6) + (hash << 16) - hash;
    return hash;
}

void qp_copy_tcp_index(void *src,void *dst)
{
   rozofs_qp_cache_entry_t *src_p;
   uint32_t *dest_p;
   dest_p = dst;
   src_p = src;
   *dest_p = src_p->cnx_idx; 
}
/*
**__________________________________________________
*/
/**
   Init of the RDMA QP cache
   That cache is needed to process message received on a queue pair for a SEND or RECV event 
   since the CQ provides the QP as a number.
   
   @param none
   
   @retval 0 on success
   @retval < 0 on error (see errno for details)
*/
int rozofs_rdma_qp_cache_init()
{
   if (rozofs_rdma_qp_cache_init_done != 0) return 0;
   
   rozofs_rdma_qp_entries = 0;
   /* Initialize list and htables for QP_entries */
   list_init(&rozofs_qp_entry_list_head);
   /*
   ** Init of the htable with mutex per cache line
   */
   htable_initialize_th(&rozofs_htable_qp, QP_HTAB_SIZE, QP_HTAB_LOCK_SZ,qp_hash, qp_cmp);
   return 0;
}

/*
**__________________________________________________
*/
/**
*   Insert a qp in the cache entry

    @param qp_num reference of the QP to insert (key)
    @param cnx_idx: index of the rozofs_rdma_tcp context for that queue pair
    
    @retval>= 0 success
    @retval < 0 error
*/
int rozofs_rdma_qp_cache_insert(uint32_t qp_num,uint32_t cnx_idx)
{
   rozofs_qp_cache_entry_t *entry_p;
   uint32_t hash = qp_hash(&qp_num);
   /*
   ** Check if the entry already exist in the cache
   */
   entry_p = rozofs_rdma_qp_cache_lookup(qp_num);
   if (entry_p !=NULL)
   {
      errno = EEXIST;
      return -1;
   }
   entry_p = xmalloc(sizeof(rozofs_qp_cache_entry_t));
   if (entry_p == NULL)
   {
      errno = ENOMEM;
      return -1;
   }
   memset(entry_p,0,sizeof(rozofs_qp_cache_entry_t));
   list_init(&entry_p->list);
   entry_p->qp_num = qp_num;
   entry_p->cnx_idx = cnx_idx;
   entry_p->he.key = &entry_p->qp_num;
   entry_p->he.value = entry_p;
   /*
   ** insert in the hash table
   */
   htable_put_entry_th(&rozofs_htable_qp,&entry_p->he,hash);
   /*
   ** we cannot use the global link list because of the multithreading....see in the future
   */
   rozofs_rdma_qp_entries++;
   return 0;
}

/*
**__________________________________________________
*/
/**
*   lookup for a qp from the cache entry

    @param qp_num reference of the QP to lookup at (key)
    
    @retval <> NULL:pointer to the cache entry that 
    @retval NULL: error (see errno for details)
*/
rozofs_qp_cache_entry_t *rozofs_rdma_qp_cache_lookup(uint32_t qp_num)
{
   uint32_t hash = qp_hash(&qp_num);
   /*
   ** get it from the hash table
   */
   return htable_get_th(&rozofs_htable_qp,&qp_num,hash);

}

/*
**__________________________________________________
*/
/**
*   lookup for a qp from the cache entry

    @param qp_num reference of the QP to lookup at (key)
    
    @retval>= 0 success
    @retval < 0 error
*/
int rozofs_rdma_qp_cache_lookup_copy(uint32_t qp_num,uint32_t *cnx_idx_p)
{
   uint32_t hash = qp_hash(&qp_num);
   /*
   ** get it from the hash table
   */
   return htable_get_copy_th(&rozofs_htable_qp,&qp_num,hash,cnx_idx_p);

}

/*
**__________________________________________________
*/
/**
*   deletion of a qp from the cache entry

    @param qp_num reference of the QP to delete (key)
    
    @retval none
*/
void rozofs_rdma_qp_cache_delete(uint32_t qp_num)
{
  uint32_t hash = qp_hash(&qp_num);
  rozofs_qp_cache_entry_t *entry_p;  
   /*
   ** get it from the hash table
   */
   entry_p = htable_del_entry_th(&rozofs_htable_qp,&qp_num,hash);
   if (entry_p == NULL) return;
   /*
   ** release the entry: take care of the race condition with a lookup
   ** we might to assert an in_use flag and a delete_pending flag to avoid releasing
   ** the memory while another context use it
   */
   xfree(entry_p);
}

/**
**______________________________________________________________________________
*   PROCESSING OF THE ASYNCHRONOUS EVENTS ASSOCIATED WITH A RDMA ADAPTOR
**______________________________________________________________________________
*

Event name	           Element type	   Event type	Protocol
IBV_EVENT_COMM_EST	       QP	   Info	         IB, RoCE
IBV_EVENT_SQ_DRAINED	       QP	   Info	         IB, RoCE
IBV_EVENT_PATH_MIG	       QP	   Info	         IB, RoCE
IBV_EVENT_QP_LAST_WQE_REACHED  QP	   Info	         IB, RoCE
IBV_EVENT_QP_FATAL	       QP	   Error	 IB, RoCE, iWARP
IBV_EVENT_QP_REQ_ERR	       QP	   Error	 IB, RoCE, iWARP
IBV_EVENT_QP_ACCESS_ERR	       QP	   Error	 IB, RoCE, iWARP
IBV_EVENT_PATH_MIG_ERR	       QP	   Error	 IB, RoCE
IBV_EVENT_CQ_ERR	       CQ	   Error	 IB, RoCE, iWARP
IBV_EVENT_SRQ_LIMIT_REACHED    SRQ	   Info	         IB, RoCE, iWARP
IBV_EVENT_SRQ_ERR	       SRQ	   Error	 IB, RoCE, iWARP
IBV_EVENT_PORT_ACTIVE	       Port	   Info	         IB, RoCE, iWARP
IBV_EVENT_LID_CHANGE	       Port	   Info	         IB
IBV_EVENT_PKEY_CHANGE	       Port	   Info	         IB
IBV_EVENT_GID_CHANGE	       Port	   Info	         IB, RoCE
IBV_EVENT_SM_CHANGE	       Port	   Info	         IB
IBV_EVENT_CLIENT_REREGISTER    Port	   Info	         IB
IBV_EVENT_PORT_ERR	       Port	   Error	 IB, RoCE, iWARP
IBV_EVENT_DEVICE_FATAL	       Device	   Error	 IB, RoCE, iWARP


**/

/* helper function to print the content of the async event */
static void print_async_event(struct ibv_context *ctx,
			      struct ibv_async_event *event)
{
	switch (event->event_type) {
	/* QP events */
	case IBV_EVENT_QP_FATAL:
		warning("QP fatal event for QP with handle %p\n", event->element.qp);
		break;
	case IBV_EVENT_QP_REQ_ERR:
		warning("QP Requestor error for QP with handle %p\n", event->element.qp);
		break;
	case IBV_EVENT_QP_ACCESS_ERR:
		warning("QP access error event for QP with handle %p\n", event->element.qp);
		break;
	case IBV_EVENT_COMM_EST:
		warning("QP communication established event for QP with handle %p\n", event->element.qp);
		break;
	case IBV_EVENT_SQ_DRAINED:
		warning("QP Send Queue drained event for QP with handle %p\n", event->element.qp);
		break;
	case IBV_EVENT_PATH_MIG:
		warning("QP Path migration loaded event for QP with handle %p\n", event->element.qp);
		break;
	case IBV_EVENT_PATH_MIG_ERR:
		warning("QP Path migration error event for QP with handle %p\n", event->element.qp);
		break;
	case IBV_EVENT_QP_LAST_WQE_REACHED:
		warning("QP last WQE reached event for QP with handle %p\n", event->element.qp);
		break;
 
	/* CQ events */
	case IBV_EVENT_CQ_ERR:
		warning("CQ error for CQ with handle %p\n", event->element.cq);
		severe("FATAL: Storio is going to be restarted because of Mellanox issue");
		fatal("See you soon!!"); 
		break;
 
	/* SRQ events */
	case IBV_EVENT_SRQ_ERR:
		warning("SRQ error for SRQ with handle %p\n", event->element.srq);
		break;
	case IBV_EVENT_SRQ_LIMIT_REACHED:
		warning("SRQ limit reached event for SRQ with handle %p\n", event->element.srq);
		break;
 
	/* Port events */
	case IBV_EVENT_PORT_ACTIVE:
		warning("Port active event for port number %d\n", event->element.port_num);
		break;
	case IBV_EVENT_PORT_ERR:
		warning("Port error event for port number %d\n", event->element.port_num);
		break;
	case IBV_EVENT_LID_CHANGE:
		warning("LID change event for port number %d\n", event->element.port_num);
		break;
	case IBV_EVENT_PKEY_CHANGE:
		warning("P_Key table change event for port number %d\n", event->element.port_num);
		break;
	case IBV_EVENT_GID_CHANGE:
		warning("GID table change event for port number %d\n", event->element.port_num);
		break;
	case IBV_EVENT_SM_CHANGE:
		warning("SM change event for port number %d\n", event->element.port_num);
		break;
	case IBV_EVENT_CLIENT_REREGISTER:
		warning("Client reregister event for port number %d\n", event->element.port_num);
		break;
 
	/* RDMA device events */
	case IBV_EVENT_DEVICE_FATAL:
		warning("Fatal error event for device %s\n", ibv_get_device_name(ctx->device));
		break;
 
	default:
		warning("Unknown event (%d)\n", event->event_type);
	}
}
 
/*
**__________________________________________________
*/
/**
  That thread is associated with a RDMA adaptor.
  Its role is to process asynchronous events received on that adaptor
  
  
  @param ctx : pointer to the RDMA context associated with the thread
  
  retval NULL on error
*/
void * rozofs_poll_rdma_async_event_th(void *ctx)
{
  rozofs_rdma_async_event_th_t *th_ctx_p = (rozofs_rdma_async_event_th_t*)ctx;
  rozofs_rmda_ibv_cxt_t *s_ctx = (rozofs_rmda_ibv_cxt_t*)th_ctx_p->ctx_p;
  int ret;
  struct ibv_async_event event;
  
//  int wc_to_ack = 0;
  
  info("Adaptor Async event started  %d \n",th_ctx_p->thread_idx);

    uma_dbg_thread_add_self("RDMA_async_evt");
    {
      struct sched_param my_priority;
      int policy=-1;
      int ret= 0;
      
      my_priority.sched_priority= 70;
      policy = SCHED_FIFO;
      ret = pthread_setschedparam(pthread_self(),policy,&my_priority);
      if (ret < 0) 
      {
	severe("error on sched_setscheduler: %s",strerror(errno));	
      }
      pthread_getschedparam(pthread_self(),&policy,&my_priority);    
    }      


     while (1) 
     {
	/* wait for the next async event */
	ret = ibv_get_async_event(s_ctx->ctx, &event);
	if (ret) {
		severe("Error, ibv_get_async_event() failed\n");
		return NULL;
	}
 
	/* print the event */
	print_async_event(s_ctx->ctx, &event);
 
	/* ack the event */
	ibv_ack_async_event(&event);
      }
}

