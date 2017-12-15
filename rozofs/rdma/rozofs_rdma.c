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
/*
**__________________________________________________
*/
/**
*   Query the current parameters of the RDMA device

    @param device_p: pointer to the device
    @param pChar: pointer to the display buffer
    
    retval pointer to the next byte in display buffer
*/
char *rozofs_rdma_display_device_properties(struct ibv_context *device_p,char *pChar,int index)
{
   int ret;
   struct ibv_device_attr device_attr;
   char bufall[128];
   
   memset(bufall,0,128);
   pChar +=sprintf(pChar,"adaptor #%d \n",index);
   ret = ibv_query_device(device_p,&device_attr);
   if (ret < 0)
   {
      pChar +=sprintf(pChar,"ibv_query_device error:%s\n",strerror(errno));
      return pChar;
   }
   memcpy(bufall,&device_attr.fw_ver,64);
   pChar +=sprintf(pChar,"firmware version    %s\n",bufall);
   pChar +=sprintf(pChar,"vendor              %x-%x-%x\n",device_attr.vendor_id,device_attr.vendor_part_id,device_attr.hw_ver);
   pChar +=sprintf(pChar,"max_qp_init_rd_atom %d\n",device_attr.max_qp_init_rd_atom);
   pChar +=sprintf(pChar,"max_qp_rd_atom      %d\n",device_attr.max_qp_rd_atom);
   pChar +=sprintf(pChar,"max_res_rd_atom     %d\n",device_attr.max_res_rd_atom);
   pChar +=sprintf(pChar,"max_qp              %d\n",device_attr.max_qp);
   pChar +=sprintf(pChar,"phys_port_cnt       %d\n",device_attr.phys_port_cnt);
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
     if (ctx->ctx != NULL) rozofs_rdma_display_device_properties(ctx->ctx,pChar,i);
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
**__________________________________________________
*/
/**
   init of the RDMA
   
   @param nb_rmda_tcp_context : number of RDMA TCP contexts
   @param client_mode: assert to 1 for client; 0 for server mode
   
   retval 0 on sucess
   retval -1 on error
*/   
int rozofs_rdma_init(uint32_t nb_rmda_tcp_context,int client_mode)
{
  int ret = 0;
  int i;
  int    fileflags;  
  rozofs_cur_act_rmda_tcp_context = 0;
  rozofs_cur_del_rmda_tcp_context = 0; 
  rozofs_rdma_listener_count = 0;
  rozofs_rdma_listener_max   = ROZOFS_RDMA_MAX_LISTENER;
  memset(&rozofs_rdma_listener[0],0,sizeof(uint64_t*)*ROZOFS_RDMA_MAX_LISTENER);
  /*
  ** signalling thread conf. parameters
  */
  rdma_sig_reconnect_credit_conf = ROZOFS_RDMA_SIG_RECONNECT_CREDIT_COUNT;
  rdma_sig_reconnect_credit = rdma_sig_reconnect_credit_conf;
  rdma_sig_reconnect_period_conf = ROZOFS_RDMA_SIG_RECONNECT_CREDIT_PERIOD_SEC;
  
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
     severe("error on rdma_create_event_channel(): %s",strerror(errno));
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
  uma_dbg_addTopic("rdma_mem", show_rdma_mem);  
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
     if (del_time > (cnx_p->del_tv_sec+1))
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
**__________________________________________________
*/
/**
  Function that process the status returned by the RDMA operation 
  That service is called under the thread context associated with a RDMA adaptor
  
  In case of error, the service can send a RDMA_DISCONNECT_REQ on the socketpair
  In any case, the service ends by a sem_post() to wake up the thread that has initiated the
  RDMA transfer.
  
  The service is intended to support only two kinds of RDMA service:
     - RDMA_READ : read data from storcli in order to perform a pwrite of projections
     - RDMA_WRITE: write data towards storcli after reading projections.
  
  @param wc : RDMA ibv_post_send context
  
*/

void rozofs_on_completion(struct ibv_wc *wc)
{
  rozofs_wr_id_t *thread_wr_p = (rozofs_wr_id_t *)(uintptr_t)wc->wr_id;
//  rozofs_rdma_connection_t *conn= NULL;
//  if (thread_wr_p!= NULL) conn = thread_wr_p->conn;
  
  switch (wc->status)
  {
	case IBV_WC_SUCCESS:
	  if (thread_wr_p!= NULL) thread_wr_p->status = 0;
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
	  if (thread_wr_p != NULL)
	  {
	    thread_wr_p->status = -1;
	    thread_wr_p->error = wc->status;
	  }
	  rozofs_rdma_error_register(wc->status);
	  goto error;    
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
    if (sem_post(thread_wr_p->sem) < 0)
    {
      /*
      ** issue a fatal since this situation MUST not occur
      */
      fatal("RDMA failure on sem_post: %s",strerror(errno));
    }
  }
  return;
  
error:  
  /*
  ** error: there was a RDMA error, the thread is signaled and a RDMA disconnect is
  ** issue
  */
  if (thread_wr_p!=NULL)
  {
    if (sem_post(thread_wr_p->sem) < 0)
    {
      /*
      ** issue a fatal since this situation MUST not occur
      */
      fatal("RDMA failure on sem_post: %s",strerror(errno));
    }
//#warning do not disconnect
//    rdma_disconnect(conn->id);
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
  
  The service is intended to support only two kinds of RDMA service:
     - RDMA_READ : read data from storcli in order to perform a pwrite of projections
     - RDMA_WRITE: write data towards storcli after reading projections.
  
  @param wc : RDMA ibv_post_send context
  
*/

void rozofs_on_completion2(struct ibv_wc *wc)
{
  rozofs_wr_id2_t *thread_wr_p = (rozofs_wr_id2_t *)(uintptr_t)wc->wr_id;
  int status = 0;
  int error = 0;
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
//  int wc_to_ack = 0;
  
  info("CQ thread#%d started \n",th_ctx_p->thread_idx);
#if 1
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
 #if 1
      my_priority.sched_priority= 96;
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
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));

    while (ibv_poll_cq(cq, 1, &wc))
      rozofs_on_completion2(&wc);
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
      rozofs_on_completion2(&wc);
    }
  }


#endif
error:
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
  ** create a number of completion queue corresponding to the number of CQ threads
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
    if (s_ctx->memreg[i] < 0)
    {
       severe("ibv_reg_mr error for addr %p len %llu : %s",mem_reg_p->mem,(unsigned long long int)mem_reg_p->len,strerror(errno));
       RDMA_STATS_RSP_NOK(ibv_reg_mr);
       goto error;  
    }
    {
     struct ibv_mr  *mr_p;     
     mr_p = s_ctx->memreg[i];
    //info("FDL memreg idx %d addr/len %llx/%llu key %x\n",i,mem_reg_p->mem,mem_reg_p->len,mr_p->lkey);
    }
    RDMA_STATS_RSP_OK(ibv_reg_mr);    
  }
  /*
  ** check if there is at least one memory region declared
  */
  if (mem_reg_p == NULL) goto error;
  /*
  ** create the CQ threads
  */
  for (k=0; k<ROZOFS_CQ_THREAD_NUM; k++)
  {
    rozofs_qc_th_t *ctx_cur_p = malloc(sizeof(rozofs_qc_th_t));
    ctx_cur_p->thread_idx = k;
    ctx_cur_p->ctx_p = s_ctx;
    TEST_NZ(pthread_create(&s_ctx->cq_poller_thread[k], NULL, rozofs_poll_cq_th, ctx_cur_p));
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
  qp_attr->recv_cq = s_ctx->cq[s_ctx->next_cq_idx%ROZOFS_CQ_THREAD_NUM];
  qp_attr->qp_type = IBV_QPT_RC;
  /*
  **  need to adjust the number of WQ to the number of disk threads to avoid ENOEM on ibv_post_send()
  */
  qp_attr->cap.max_send_wr = 64;
  qp_attr->cap.max_recv_wr = 64;
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
  if (s_ctx == NULL) goto error;
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
  conn->id = id;
  conn->tcp_index = assoc_p->srv_ref;
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
  }
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
      severe("RDMA rdma_create_id error:%s",strerror(errno));
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
*  Post either a RDMA read or write command

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
int rozofs_rdma_post_send(rozofs_wr_id_t *wr_th_p,
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
      wr_th_p->status = -1;
      wr_th_p->error = EPROTO;
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

      wr_th_p->status = -1;
      wr_th_p->error = ENOTSUP;
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
#warning always context with index 0
    mr_p = s_ctx->memreg[0];
    if (mr_p == NULL)
    {
      info("FDL RDMA_POST_SEND no memory context" );
      wr_th_p->status = -1;
      wr_th_p->error = EPROTO;
      goto error;
    }
    wr_th_p->conn = conn;
//    gettimeofday(&tv,NULL);
//    time_before = MICROLONG(tv);

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

//    info("IBV_WR_RDMA_READ QP %llx addr %llx len=%u key=%x raddr %llx rkey %x\n",conn->qp,sge.addr,sge.length,sge.lkey,wr.wr.rdma.remote_addr,wr.wr.rdma.rkey);

    if (ibv_post_send(conn->qp, &wr, &bad_wr) < 0)
    {
       info("FDL RDMA_POST_SEND error: %s",strerror(errno));
       goto error;
    
    
    }
    /*
    ** now wait of the semaphore the end to the RDMA transfert
    */
//    info("FDL RDMA_POST_SEND sem_wait" );

    sem_wait(wr_th_p->sem);

//    gettimeofday(&tv,NULL);
//    time_after = MICROLONG(tv);
//    printf("service time %llu for %d bytes\n",(unsigned long long int)(time_after-time_before),rozofs_rdma_msg_p->remote_len);
    /*
    ** check the status of the operation
    */
    if (wr_th_p->status != 0)
    {
       info ("FDL POST_SEND error on service %d\n",wr_th_p->error);
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
#warning always context with index 0
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

//    info("IBV_WR_RDMA_READ QP %llx addr %llx len=%u key=%x raddr %llx rkey %x\n",conn->qp,sge.addr,sge.length,sge.lkey,wr.wr.rdma.remote_addr,wr.wr.rdma.rkey);

    if (ibv_post_send(conn->qp, &wr, &bad_wr) < 0)
    {
       info("FDL RDMA_POST_SEND error: %s",strerror(errno));
       goto error;    
    }
    return 0;
error:
    return -1;
}
