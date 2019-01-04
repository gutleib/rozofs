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
 #ifndef ROZOFS_RDMA_H
 #define ROZOFS_RDMA_H 

#ifdef ROZOFS_RDMA
//#define FDL_COMPIL_STANDALONE 0

#include <libgen.h> 
 #include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>
#include <semaphore.h>
#ifndef FDL_COMPIL_STANDALONE
#include <rozofs/common/list.h>
#include <rozofs/common/htable.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/core/uma_dbg_api.h>
#else
#include "list.h"
#endif

#define TEST_NZ(x) do { if ( (x)) goto error; } while (0)
#define TEST_Z(x)  do { if (!(x)) goto error; } while (0)

#ifdef FDL_COMPIL_STANDALONE
/**
**____________________________________________________________________
*  For test in standalone mode
*/
#define EDEBUG      0
#define EINFO       1
#define EWARNING    2
#define ESEVERE     3
#define EFATAL      4
#ifdef __GNUC__
static const char *messages[] __attribute__ ((unused)) =
        {"debug", "info", "warning", "severe", "fatal"};
#else
static const char *messages[] =
    { "debug", "info", "warning", "severe", "fatal" };
#endif

#define logmsg(level, fmt, ...) {\
    printf( "%s - %d - %s: " fmt, basename(__FILE__), \
            __LINE__, messages[level], ##__VA_ARGS__); \
}	
#define info(fmt, ...) logmsg(EINFO, fmt, ##__VA_ARGS__)
#define warning(fmt, ...) logmsg(EWARNING, fmt, ##__VA_ARGS__)
#define severe(fmt, ...) logmsg(ESEVERE, fmt, ##__VA_ARGS__)
#define fatal(fmt, ...) {logmsg(EFATAL, fmt, ##__VA_ARGS__); abort();}
#endif
/**
*  For test in standalone mode
**____________________________________________________________________
*/

#define ROZOFS_RDMA_BACKLOG_COUNT 64  /**< max RDMA listen backlog   */
#define ROZOFS_RDMA_MAX_LISTENER 8    /**< max listening context     */
#define ROZOFS_MAX_SRQ_WR 1024 /* Max of outstanding Work requests that can be posted on a shared queue */
#define ROZOFS_RPC_RDMA_MSG_SZ 2048 /**< max size of a RPC message over RDMA */
/**
* Statistics of the RDMA signalling thread
*/
typedef struct rozofs_rdma_sig_th_statistics_t
{
   uint64_t rmda_msg_rcv_cnt;   /**< number of message received on the signaling RDMA socket  */
   uint64_t rmda_msg_send_cnt;
   uint64_t int_msg_rcv_cnt;    /**< number of message received on the signaling af_unix socket pair  */
   uint64_t int_msg_send_cnt;   /**< number of message send on the signaling af_unix socket pair  */
} rozofs_rdma_sig_th_statistics_t;

#define ROZOFS_MAX_RDMA_MEMREG 8
#define ROZOFS_MAX_RDMA_ADAPTOR 8
#define ROZOFS_RDMA_COMPQ_SIZE 512  /**< must be at least equal to the number of disk threads  */
#define ROZOFS_RDMA_COMPQ_RPC_SIZE  512 /**< number of entry per completion queue              */
#define ROZOFS_RDMA_BUF_SRQ_MAX_REFILL 2  /**< number of ruc_buffer that can be refill on the SRQ upon receiving one message */
/*
** RDMA module statistics
*/
#define RDMA_STATS_REQ(the_probe) \
{ \
    rozofs_rdma_mod_stats.the_probe[0]++;\
}
#define RDMA_STATS_RSP_OK(the_probe) \
{ \
    rozofs_rdma_mod_stats.the_probe[1]++;\
}

#define RDMA_STATS_RSP_NOK(the_probe) \
{ \
    rozofs_rdma_mod_stats.the_probe[2]++;\
}

typedef struct _rozofs_rdma_mod_stats_t {
    uint64_t   rdma_connect[3];             /**< RDMA connect request : rdma_connect()          */
    uint64_t   rdma_disconnect[3];          /**< RDMA disconnect : rdma_disconnect()            */
    uint64_t   rdma_accept[3];              /**< RDMA accept rdma_accept()                      */
    uint64_t   rdma_reject[3];              /**< RDMA reject rdma_reject()                      */
    uint64_t   rdma_resolve_addr[3];        /**< RDMA resolve destination address               */
    uint64_t   rdma_resolve_route[3];       /**< RDMA resolve destination route                 */
    uint64_t   ibv_reg_mr[3];               /**< RDMA memory registration : ibv_reg_mr()        */
    uint64_t   rdma_create_qp[3];           /**< RDMA queue pair creation  rdma_create_qp()     */
    uint64_t   rdma_destroy_qp[3];          /**< queue pair deletion :rdma_destroy_qp()         */
    uint64_t   ibv_modify_qp[3];            /**< RDMA queue pair modification:ibv_modify_qp()   */
    uint64_t   ibv_alloc_pd[3];             /**< RDMA Policy descriptor allocation              */
    uint64_t   ibv_create_comp_channel[3];  /**< RDMA completion channel creation               */
    uint64_t   ibv_create_cq[3];            /**< RDMA completion queue creation                 */
    uint64_t   rdma_create_id[3];           /**< cm_id context creation: rdma_create_id()       */
    uint64_t   rdma_destroy_id[3];          /**< cm_id contextdeletion: rdma_destroy_id         */
    uint64_t   signalling_sock_create[3];   /**<signalling socket creation                      */
    uint64_t   rdma_listen[3];              /**<number of RDMA listen                           */
    uint64_t   rdma_reconnect[3];           /**<number of RDMA reconnect attempt                */
    uint64_t   ibv_create_srq[3];           /**<number of RDMA share queue creating             */
    uint64_t   ibv_post_srq_recv[3];        /**<number of RDMA post received on shared queue    */
} rozofs_rdma_mod_stats_t;



/**
*  structure used to associate a RDMA connection with a TCP connection
*/
typedef struct _rozofs_rdma_tcp_assoc_t
{
    uint16_t cli_ref;  /**< reference of the client : local index of the TCP connection  */
    uint16_t srv_ref;  /**< reference of the serveur: local index of the TCP connection   */
    uint32_t ip_cli;   /**< client IP address   */   
    uint32_t ip_srv;   /**< server IP address   */
    uint16_t port_cli; /**< client port         */
    uint16_t port_srv; /**< server port         */
    uint64_t cli_ts;   /**< timestamp of the client */
} rozofs_rdma_tcp_assoc_t;

#define ROZOFS_CQ_THREAD_NUM 2

typedef struct _rozofs_rmda_ibv_cxt_t {
  uint32_t  next_cq_idx; 
  struct ibv_context *ctx;   /**< ibv context provided by the adaptor: use as primary key  */
  struct ibv_pd *pd;         /**< policy descriptor  */
  struct ibv_cq *cq[ROZOFS_CQ_THREAD_NUM];         /**< completion queue for RDMA READ,WRITE and ACK   */
  struct ibv_cq *cq_rpc[ROZOFS_CQ_THREAD_NUM];         /**< completion queue for RPC message over RDMA   */
  struct ibv_comp_channel *comp_channel[ROZOFS_CQ_THREAD_NUM];   /**< completion channel for RDMA READ,WRITE and ACK   */
  struct ibv_comp_channel *comp_channel_rpc[ROZOFS_CQ_THREAD_NUM];   /**< completion channel for RPC over RDMA   */
  struct ibv_mr *memreg[ROZOFS_MAX_RDMA_MEMREG];
  struct ibv_mr *memreg_rpc;                                        /**< memory region used form RPC messages      */
  pthread_t cq_poller_thread[ROZOFS_CQ_THREAD_NUM];
  pthread_t cq_rpc_poller_thread[ROZOFS_CQ_THREAD_NUM];
  struct ibv_srq *shared_queue4rpc;     /**< shared queue used for RPC over RDMA           */
  uint64_t post_send_stat[ROZOFS_CQ_THREAD_NUM];  /**< number of ibv_post_send   */
  uint64_t post_recv_stat[ROZOFS_CQ_THREAD_NUM];  /**< number of ibv_post_recv   */
  void *rozofs_cq_th_post_send[ROZOFS_CQ_THREAD_NUM];  /**< thread context of the completion queue thread associated with post_send */
  void *rozofs_cq_th_post_recv[ROZOFS_CQ_THREAD_NUM];  /**< thread context of the completion queue thread associated with post_recv */
  void *rozofs_rdma_async_event_th;                    /**< context of the asynchronous RDMA event thread                           */
  pthread_t rozofs_rdma_async_event_thread;
} rozofs_rmda_ibv_cxt_t;

/*
** constant for selecting the buffer pool for ibv_post_srq_recv()
*/
#define ROZOFS_RDMA_CMD 0
#define ROZOFS_RDMA_RSP 1
/*
** signalling thread constant
*/
#define ROZOFS_RDMA_SIG_RECONNECT_CREDIT_COUNT      100
#define ROZOFS_RDMA_SIG_RECONNECT_CREDIT_PERIOD_SEC 10


/**
*  Statistics for the completion queue thread
*/
typedef struct _rozofs_cq_th_stat_t
{
   uint64_t ibv_wc_rdma_write_count ;  /**< number of RDMA_WRITE   */
   uint64_t ibv_wc_rdma_read_count ;  /**< number of RDMA_READ   */
   uint64_t ibv_wc_send_count ;       /**< Send operation of a WR that was posted to a Send Queue   */
   uint64_t ibv_wc_recv_count ;       /**< Send data operation for a WR that was poster to a receive queue (of a QP or to an SRQ) */
} rozofs_cq_th_stat_t;
/**
*  context of the completion queue thread
*/
typedef struct _rozofs_qc_th_t
{
  int thread_idx;  /**< index of the thread : need to index the right completion queue */
  rozofs_rmda_ibv_cxt_t *ctx_p;  /**< pointer to the IBV context of the coupler        */
  rozofs_cq_th_stat_t stats;   /**< statistics for the Completion Queue Thread         */
  int rpc_buf_count2alloc;    /**< contains the number of RPC buffer that must be allocated : for SRQ management only  */
  uint64_t comp_events_completed;
  uint64_t async_events_completed;
  int cqe;     /**< number of entries in the completion queue */
} rozofs_qc_th_t;

/**
*  context of the asynchronous RDMA event thread
*/
typedef struct _rozofs_rdma_async_event_th_t
{
  int thread_idx;  /**< index of the thread : need to index the right completion queue */
  rozofs_rmda_ibv_cxt_t *ctx_p;  /**< pointer to the IBV context of the coupler        */
//  rozofs_cq_th_stat_t stats;   /**< statistics for the Completion Queue Thread         */
} rozofs_rdma_async_event_th_t;

/*
** RDMA connection state
*/
typedef enum _rozofs_rdma_cnx_state_e
{
   ROZOFS_RDMA_ST_IDLE=0,
   ROZOFS_RDMA_ST_WAIT_ADDR_RESOLVED,
   ROZOFS_RDMA_ST_WAIT_ROUTE_RESOLVED,
   ROZOFS_RDMA_ST_WAIT_ESTABLISHED,
   ROZOFS_RDMA_ST_ESTABLISHED,
   ROZOFS_RDMA_ST_ERROR,
   ROZOFS_RDMA_ST_MAX,
} rozofs_rdma_cnx_state_e;


/**
* state of the FSM on the RDMA client side
*/
typedef enum _rozofs_rdma_tcp_st_e
{
   ROZOFS_RDMA_ST_TCP_IDLE=0,
   ROZOFS_RDMA_ST_TCP_WAIT_RDMA_REQ,
   ROZOFS_RDMA_ST_TCP_WAIT_RDMA_REQ_RSP,
   ROZOFS_RDMA_ST_TCP_RDMA_CONNECTING,
   ROZOFS_RDMA_ST_TCP_WAIT_RDMA_CONNECTED,
   ROZOFS_RDMA_ST_TCP_RDMA_ESTABLISHED,
   ROZOFS_RDMA_ST_TCP_WAIT_RDMA_RECONNECT,
   ROZOFS_RDMA_ST_TCP_DEAD,
   ROZOFS_RDMA_ST_TCP_MAX,
} rozofs_rdma_tcp_st_e; 

/**
*  event on RDMA AF_UNIX signaling socket
*/
typedef enum _rozofs_rdma_internal_event_e
{
    ROZOFS_RDMA_EV_RDMA_CONNECT=0,     /**< client   */
    ROZOFS_RDMA_EV_RDMA_EVT,           
    ROZOFS_RDMA_EV_RDMA_DEL_REQ,
    ROZOFS_RDMA_EV_RDMA_DEL_IND,
    ROZOFS_RDMA_EV_RDMA_ESTABLISHED_IND,
    ROZOFS_RDMA_EV_TCP_CONNECTED,
    ROZOFS_RDMA_EV_TCP_DISCONNECTED,
    ROZOFS_RDMA_EV_TCP_RDMA_REQ,           /* TCP side only */
    ROZOFS_RDMA_EV_TCP_RDMA_RSP_ACCEPT,    /* TCP side only */
    ROZOFS_RDMA_EV_TCP_RDMA_RSP_REJECT,    /* TCP side only */
    ROZOFS_RDMA_EV_TCP_DISCONNECT,         /* TCP side only */
    ROZOFS_RDMA_EV_MAX,
} rozofs_rdma_internal_event_e; 

/*
** Mapping of the RDMA error code towards Rozofs
*/
 typedef enum _rozofs_rdma_err_e {
 ROZOFS_IBV_WC_SUCCESS=0,
 ROZOFS_IBV_WC_LOC_LEN_ERR,
 ROZOFS_IBV_WC_LOC_QP_OP_ERR,
 ROZOFS_IBV_WC_LOC_EEC_OP_ERR,
 ROZOFS_IBV_WC_LOC_PROT_ERR,
 ROZOFS_IBV_WC_WR_FLUSH_ERR,
 ROZOFS_IBV_WC_MW_BIND_ERR,
 ROZOFS_IBV_WC_BAD_RESP_ERR,
 ROZOFS_IBV_WC_LOC_ACCESS_ERR,
 ROZOFS_IBV_WC_REM_INV_REQ_ERR,
 ROZOFS_IBV_WC_REM_ACCESS_ERR,
 ROZOFS_IBV_WC_REM_OP_ERR,
 ROZOFS_IBV_WC_RETRY_EXC_ERR,
 ROZOFS_IBV_WC_RNR_RETRY_EXC_ERR,
 ROZOFS_IBV_WC_LOC_RDD_VIOL_ERR,
 ROZOFS_IBV_WC_REM_INV_RD_REQ_ERR,
 ROZOFS_IBV_WC_REM_ABORT_ERR,
 ROZOFS_IBV_WC_INV_EECN_ERR,
 ROZOFS_IBV_WC_INV_EEC_STATE_ERR,
 ROZOFS_IBV_WC_FATAL_ERR,
 ROZOFS_IBV_WC_RESP_TIMEOUT_ERR,
 ROZOFS_IBV_WC_GENERAL_ERR, 
 ROZOFS_IBV_WC_MAX_ERR
 } rozofs_rdma_err_e;

#include "rozofs_rdma_err_e2String.h"

/**
*  Per RDMA connection
*/
typedef struct _rozofs_rdma_connection_t {
  list_t  list;                     /**< use to link the context on the deletion list                               */
  time_t  del_tv_sec;               /**< time at which deletion occured                                             */
  rozofs_rdma_cnx_state_e state;    /**< RDMA connection state */
  struct rdma_cm_id *id; 
  uint32_t qp_num;                  /**< unique id for that QP on the adaptor side                                  */ 
  struct ibv_qp *qp;                /**< pointer to the QP context                                                  */
  rozofs_rmda_ibv_cxt_t *s_ctx;     /**< RDMA context of the adaptor to which the RDMA connection is associated     */
  uint32_t   tcp_index;             /**< index of the TCP context to which the RDMA context is associated           */
  uint32_t   cq_xmit_idx;            /**< index of the send completion queue                                        */              
  uint32_t   cq_recv_idx;            /**< index of the receive completion queue                                        */              
  rozofs_rdma_tcp_assoc_t assoc;    /**< RDMA/TCP connection identifier: only relevant for client connection        */
} rozofs_rdma_connection_t;


/**
*  Per TCP connection
*/

typedef void (*rozofs_rdma_cnx_statechg_pf_t)(uint32_t opaque_ref,uint32_t state,uint32_t sockctrlRef);  /**< 0: RDMA Down - 1:RDMA Up */

typedef struct _rozofs_rdma_tcp_cnx_t {
  int state;                           /**< state of the RDMA/TCP association */
  uint32_t opaque_ref;                 /**< user reference                    */
  rozofs_rdma_cnx_statechg_pf_t state_cbk; /**< callback for state change    */
  rozofs_rdma_tcp_assoc_t assoc;       /**< RDMA/TCP connection identifier: only relevant for client connection     */
  uint64_t last_down_ts;               /**< last time the connection has been detected as DOWN status reported by RDMA side upon RDMA xfer error */
} rozofs_rdma_tcp_cnx_t;

typedef struct _rozofs_rdma_memory_reg_t {             
   char *mem;       /**< start pointer */
   uint64_t len;    /**< length of the region   */
   void *opaque;    /**< user reference  */
   int idx;         /**< memory region index */
} rozofs_rdma_memory_reg_t;   


/*
** Queue Pair lookup cache entry (need to perform a full RDMA interface rather that a TCP/RDMA mix)
*/
typedef struct _rozofs_qp_cache_entry_t
{
    hash_entry_t he;
    uint32_t     qp_num; /*< qp key         */
    uint32_t     cnx_idx;   /**< index of the connection   */ 
    list_t list;   
} rozofs_qp_cache_entry_t;
/*
** structure used to fill up wr_id field of the ibv_send_wr structure
*/
#if 0
typedef struct _rozofs_wr_id_t {
    sem_t *sem;          /**< semaphore of the thread waiting fro the end of the RDMA read or write */
    void * thread_ctx_p; /**< pointer to the thread context                                         */
    int    status;       /**< status of the operation :0 success -1 error                           */ 
    int    error;        /**< rmda error code                                                       */ 
    rozofs_rdma_connection_t *conn;  /**< RDMA connection for which the message is send             */
} rozofs_wr_id_t;
#endif
typedef void (*rozofs_rdma_pf_cqe_done_t) (void *user_param,int status,int error);

typedef struct _rozofs_wr_id2_t {
    rozofs_rdma_pf_cqe_done_t  cqe_cbk;         /**< user callback upon completion queue done       */
    void                       *user_param;
} rozofs_wr_id2_t;

/*
** Callback used for RPC message received over RDMA from the completion queue thread   
*/
typedef void (*rozofs_rdma_pf_rpc_cqe_done_t) (int opcode,void *ruc_buf,uint32_t qp_num,void *rozofs_rmda_ibv_cxt_p,int status,int error);

/**
**  F O R     T E S T S
*/
typedef struct _rozofs_rdma_msg_t
{
   uint32_t size;
   uint32_t opcode;    /**< operation opcode                        */
   uint32_t req;      /**< assert to 1 for a request/ assert to 0 for a response */
   rozofs_rdma_tcp_assoc_t assoc;  /**< reference of the connection */
   int      status;   /**< status of the operation : 0 success -1: error */
   int      errcode;  /**< error code   */
   /*
   ** additionnal parameters for ROZOFS_TCP_RDMA_READ & ROZOFS_TCP_RDMA_WRITE
   */
   uint32_t rkey;     /**< remote key to use in ibv_post_send            */
   uint64_t remote_addr;  /**< remote addr                               */
   uint32_t remote_len;   /**< length of the data transfer               */
} rozofs_rdma_msg_t;

#define ROZOFS_MSG_REQ 0
#define ROZOFS_MSG_RSP 1

#define ROZOFS_TCP_RDMA_REQ 1
#define ROZOFS_TCP_RDMA_CONNECT 1
#define ROZOFS_TCP_RDMA_ACCEPTED 2
#define ROZOFS_TCP_RDMA_DELETE 3
#define ROZOFS_TCP_RDMA_READ 4
#define ROZOFS_TCP_RDMA_WRITE 5

/**
**  F O R     T E S T S
*/
/*
** Global data
*/
extern rozofs_rmda_ibv_cxt_t *rozofs_rmda_ibv_tb[];
extern rozofs_rdma_memory_reg_t *rozofs_rdma_memory_reg_tb[];
extern rozofs_rdma_connection_t **rozofs_rdma_tcp_table;
extern rozofs_rdma_tcp_cnx_t **tcp_cnx_tb;;                /**< table of the pseudo TCP context associated with RozoFS AF_UNIX context  */
extern int rozofs_rdma_signaling_sockpair[];             /**< index 0 is used by the RDMA signaling thread, index 1 is used by the client */
extern uint16_t rozofs_rdma_listening_port;
extern int       rozofs_nb_rmda_tcp_context;
extern int       rozofs_cur_act_rmda_tcp_context;    /**< current number of contexts                       */
extern int       rozofs_cur_del_rmda_tcp_context;    /**< number of context waiting for final deletion     */
extern int       rozofs_rdma_signalling_thread_mode;
extern int       rozofs_rdma_signalling_thread_started;
extern int  rozofs_rdma_enable;  
extern rozofs_rdma_mod_stats_t rozofs_rdma_mod_stats;
/*
**______________________________________
** Prototypes
**______________________________________
*/
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
int rozofs_rdma_init(uint32_t nb_rmda_tcp_context,int client_mode,int count,int size,rozofs_rdma_pf_rpc_cqe_done_t rdma_rpc_recv_cbk);

/*
**__________________________________________________
*/
/**
   Create a RDMA listener
   
   That service must be invoked once RDMA module has been started
   
   @param ip_addr: IP address of the listener or ANY_ADDR (host format)
   @param port:  listening port (host format)
   
   @retval 0 on success
   @retval -1 on error
*/
int rozofs_rdma_listening_create(uint32_t ip,uint16_t port);
/*
**__________________________________________________
*/
/**
   Create a user memory region
   
   @param user_reg: user memory region
   
   retval 0 on success
   retval<0 on error
*/  
int rozofs_rdma_user_memory_register (rozofs_rdma_memory_reg_t  *user_reg);

/*
**__________________________________________________
*/
/**
*  Get the RDMA context from the file descriptor index
  
   @param fd :reference of the file descriptor
   
   @retval: pointer to the connection context
   
*/
rozofs_rdma_connection_t *rozofs_rdma_get_connection_context_from_fd(int fd);

/*
**__________________________________________________
*/
/**
*  Get the TCP context from index of the AF_unix context
  
   @param af_unix_id : reference of the af_unix context
   @param alloc:   set to 1 if the context need to be allocated if is does not exists
   
   @retval: pointer to the TCP connection context
   
*/
rozofs_rdma_tcp_cnx_t *rozofs_rdma_get_tcp_connection_context_from_af_unix_id(int af_unix_id,int alloc);

/*
**__________________________________________________
*/
/**
*  Get the TCP context from the association block for the client side
  
   @param assoc : pointer to the association context that contains the reference of the client reference
   
   @retval: pointer to the TCP connection context
   
*/
rozofs_rdma_tcp_cnx_t *rozofs_rdma_get_cli_tcp_connection_context_from_assoc(rozofs_rdma_tcp_assoc_t *assoc);
/*
**__________________________________________________
*/
/**
*  Get the TCP context from the association block for the server side
  
   @param assoc : pointer to the association context that contains the reference of the server reference
   
   @retval: pointer to the TCP connection context
   
*/
rozofs_rdma_tcp_cnx_t *rozofs_rdma_get_srv_tcp_connection_context_from_assoc(rozofs_rdma_tcp_assoc_t *assoc);
/*
**__________________________________________________
*/
/**
   Server mode event dispatch
 
   @param event :RDMA event associated with the connection
   
   retval 0 on sucess
   retval -1 on error
*/  
int rozofs_rdma_srv_on_event(struct rdma_cm_event *event);

/*
**__________________________________________________
*/
/**
   Get the socket reference from the socketpair to use on TCP side
    
   retval > 0 reference of the socket
   ;retval -1 no socket
*/  
static inline int rozofs_rdma_get_af_unix_sock_for_tcp_side()
{
   return  rozofs_rdma_signaling_sockpair[1];

}

/*
**__________________________________________________
*/
/**
   Get the socket reference from the socketpair to use on TCP side
    
   retval > 0 reference of the socket
   ;retval -1 no socket
*/  
static inline int rozofs_rdma_get_af_unix_sock_for_rdma_side()
{
   return  rozofs_rdma_signaling_sockpair[0];

}
/*
**__________________________________________________
*/

static inline char *print_rozofs_rdma_cnx_state(int state)
{
 switch (state)
 {
   case ROZOFS_RDMA_ST_IDLE : return "ST_IDLE";
   case ROZOFS_RDMA_ST_WAIT_ADDR_RESOLVED: return "ST_WAIT_ADDR_RESOLVED";
   case ROZOFS_RDMA_ST_WAIT_ROUTE_RESOLVED: return "ST_WAIT_ROUTE_RESOLVED";
   case ROZOFS_RDMA_ST_WAIT_ESTABLISHED: return "ST_WAIT_ESTABLISHED";
   case ROZOFS_RDMA_ST_ESTABLISHED: return "ST_ESTABLISHED";
   case ROZOFS_RDMA_ST_ERROR: return "ST_ERROR";
   default: return "unkown!!";
 }
}
/*
**__________________________________________________
*/
static inline char *print_rozofs_rdma_internal_event(int evt)
{
  switch (evt)
  {
    case ROZOFS_RDMA_EV_RDMA_CONNECT: return "RDMA_CONNECT";
    case ROZOFS_RDMA_EV_RDMA_EVT: return "RDMA_EVT";           
    case ROZOFS_RDMA_EV_RDMA_DEL_REQ: return "RDMA_DEL_REQ";
    case ROZOFS_RDMA_EV_RDMA_DEL_IND: return "RDMA_DEL_IND";
    case ROZOFS_RDMA_EV_RDMA_ESTABLISHED_IND: return "RDMA_ESTABLISHED_IND";
    case ROZOFS_RDMA_EV_TCP_CONNECTED: return "TCP_CONNECTED";
    case ROZOFS_RDMA_EV_TCP_DISCONNECTED: return "TCP_DISCONNECTED";
    case ROZOFS_RDMA_EV_TCP_RDMA_REQ: return "TCP_RDMA_REQ";           /* TCP side only */
    case ROZOFS_RDMA_EV_TCP_RDMA_RSP_ACCEPT: return "TCP_RDMA_RSP_ACCEPT";    /* TCP side only */
    case ROZOFS_RDMA_EV_TCP_RDMA_RSP_REJECT: return "TCP_RDMA_RSP_REJECT";    /* TCP side only */
    case ROZOFS_RDMA_EV_TCP_DISCONNECT: return "TCP_DISCONNECT";         /* TCP side only */
    default : return "unknown!!";
  }
}
/*
**__________________________________________________
*/
static inline char *print_rozofs_rdma_tcp_st(int state)
{
  switch (state)
  {

   case ROZOFS_RDMA_ST_TCP_IDLE: return "ST_TCP_IDLE";
   case ROZOFS_RDMA_ST_TCP_WAIT_RDMA_REQ: return "ST_TCP_WAIT_RDMA_REQ";
   case ROZOFS_RDMA_ST_TCP_WAIT_RDMA_REQ_RSP: return "ST_TCP_WAIT_RDMA_REQ_RSP";
   case ROZOFS_RDMA_ST_TCP_RDMA_CONNECTING: return "ST_TCP_RDMA_CONNECTING";
   case ROZOFS_RDMA_ST_TCP_WAIT_RDMA_CONNECTED: return "ST_TCP_WAIT_RDMA_CONNECTED";
   case ROZOFS_RDMA_ST_TCP_RDMA_ESTABLISHED: return "ST_TCP_RDMA_ESTABLISHED";
   case ROZOFS_RDMA_ST_TCP_DEAD: return "ST_TCP_DEAD";
   case ROZOFS_RDMA_ST_TCP_MAX: return "ST_TCP_NOCTX";
   default : return "unknown";
  }
   return "unkown!!";
}


/*
**__________________________________________________
*/
static inline char *print_rozofs_rdma_cme_evt(int cma_evt)
{

    switch(cma_evt)
      {
      	case RDMA_CM_EVENT_ADDR_RESOLVED : return "RDMA_CM_EVENT_ADDR_RESOLVED";
	case RDMA_CM_EVENT_ADDR_ERROR: return "RDMA_CM_EVENT_ADDR_ERROR";
	case RDMA_CM_EVENT_ROUTE_RESOLVED: return "RDMA_CM_EVENT_ROUTE_RESOLVED";
	case RDMA_CM_EVENT_ROUTE_ERROR: return "RDMA_CM_EVENT_ROUTE_ERROR";
	case RDMA_CM_EVENT_CONNECT_REQUEST: return "RDMA_CM_EVENT_CONNECT_REQUEST";
	case RDMA_CM_EVENT_CONNECT_RESPONSE: return "RDMA_CM_EVENT_CONNECT_RESPONSE";
	case RDMA_CM_EVENT_CONNECT_ERROR: return "RDMA_CM_EVENT_CONNECT_ERROR";
	case RDMA_CM_EVENT_UNREACHABLE: return "RDMA_CM_EVENT_UNREACHABLE";
	case RDMA_CM_EVENT_REJECTED: return "RDMA_CM_EVENT_REJECTED";
	case RDMA_CM_EVENT_ESTABLISHED: return "RDMA_CM_EVENT_ESTABLISHED";
	case RDMA_CM_EVENT_DISCONNECTED: return "RDMA_CM_EVENT_DISCONNECTED";
	case RDMA_CM_EVENT_DEVICE_REMOVAL: return "RDMA_CM_EVENT_DEVICE_REMOVAL";
	case RDMA_CM_EVENT_MULTICAST_JOIN: return "RDMA_CM_EVENT_MULTICAST_JOIN";
	case RDMA_CM_EVENT_MULTICAST_ERROR: return "RDMA_CM_EVENT_MULTICAST_ERROR";
	case RDMA_CM_EVENT_ADDR_CHANGE: return "RDMA_CM_EVENT_ADDR_CHANGE";
	case RDMA_CM_EVENT_TIMEWAIT_EXIT: return "RDMA_CM_EVENT_TIMEWAIT_EXIT";
	default: return "RDMA_EVT";
	}
   return "unkown!!";
}
static inline void rozofs_print_rdma_fsm_state(int state,int evt,int evt_cma)
{
   if (evt == ROZOFS_RDMA_EV_RDMA_EVT)
   {
      info("RDMA: state %s evt:%s\n",print_rozofs_rdma_cnx_state(state),print_rozofs_rdma_cme_evt(evt_cma));
   }
   else
   {
      info("RDMA: state %s evt:%s\n",print_rozofs_rdma_cnx_state(state),print_rozofs_rdma_internal_event(evt));
   }
}

static inline void  rozofs_print_tcp_fsm_state(int state,int evt)
{
   printf("TCP: state %s evt:%s\n",print_rozofs_rdma_tcp_st(state),print_rozofs_rdma_internal_event(evt));
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
void rozofs_rdma_srv_fsm_exec(rozofs_rdma_tcp_assoc_t *assoc,struct rdma_cm_event *event,rozofs_rdma_internal_event_e evt_code);
/*
**__________________________________________________
*/
/**
*  
  @param assoc: pointer to the association context
  @param event: RDMA event context
  @param evt_code : event code
  
*/
void rozofs_rdma_cli_fsm_exec(rozofs_rdma_tcp_assoc_t *assoc,struct rdma_cm_event *event,rozofs_rdma_internal_event_e evt_code);
/*
**__________________________________________________
*/
/**
*  
  @param assoc: pointer to the association context
  @param evt_code : event code
  
*/
void rozofs_rdma_tcp_cli_fsm_exec(rozofs_rdma_tcp_assoc_t *assoc,rozofs_rdma_internal_event_e evt_code);
/*
**__________________________________________________
*/
/**
*  
  @param assoc: pointer to the association context
  @param evt_code : event code
  
*/
void rozofs_rdma_tcp_srv_fsm_exec(rozofs_rdma_tcp_assoc_t *assoc,rozofs_rdma_internal_event_e evt_code);
/*
**__________________________________________________________________________________________________________
*/
/**
*  Process of ROZOFS RDMA signalling message received on the AF_UNIX socket (TCP client/server side)
   
   @retval 0 on success
   @retval -1 on error
*/
int rozofs_rdma_srv_af_unix_receive_tcp();


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

int rozofs_rdma_post_send2(rozofs_wr_id2_t *wr_th_p,
                           uint8_t opcode,
			   rozofs_rdma_tcp_assoc_t *assoc_p,
			   void *bufref,
			   void *local_addr,
			   int len,
			   uint64_t remote_addr,
			   uint32_t remote_key);



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
			   int len);
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
void rozofs_rdma_on_completion_check_status_of_rdma_connection(int status,int error,rozofs_rdma_tcp_assoc_t *assoc_p);

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
void rozofs_rdma_tcp_cli_reconnect(rozofs_rdma_tcp_cnx_t *conn);

/*
**__________________________________________________
*/
/*
** Create the pool user for sending request and response
   The pool is common for request and response
   
   The buffers on that pool will be used by the shared received queue of the rdma
   
   @param count :number of buffer
   @param length : length of a buffer
   
   
   @retval 0 on success
   @retval < 0 on error s(see errno for details )
*/
int rozofs_rdma_request_response_pool_create(int count,int size);


/*
**_________________________________________________________________________________________

     R D M A   Q U E U E  P A I R   C A C H E 
**_________________________________________________________________________________________
*/
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
int rozofs_rdma_qp_cache_init();

/*
**__________________________________________________
*/
/**
*   insert a qp from the cache entry

    @param qp_num reference of the QP to lookup at (key)
    @param cnx_idx: index of the rozofs_rdma_tcp context for that queue pair
    
    @retval>= 0 success
    @retval < 0 error
*/
int rozofs_rdma_qp_cache_insert(uint32_t qp_num,uint32_t cnx_idx);
/*
**__________________________________________________
*/
/**
*   lookup for a qp from the cache entry

    @param qp_num reference of the QP to insert (key)
    
    @retval <> NULL:pointer to the cache entry that 
    @retval NULL: error (see errno for details)
*/
rozofs_qp_cache_entry_t *rozofs_rdma_qp_cache_lookup(uint32_t qp);
/*
**__________________________________________________
*/
/**
*   deletion of a qp from the cache entry

    @param qp_num reference of the QP to delete (key)
    
    @retval none
*/
void rozofs_rdma_qp_cache_delete(uint32_t qp);


/*
**__________________________________________________
*/
/**
*  Allocate a rdma RPC buffer

   @param none
   
   @retval <> NULL pointer to the ruc_buffer
   @retval NULL: out of buffer
*/
void *rozofs_rdma_allocate_rpc_buffer();
/*
**__________________________________________________
*/
/**
* Release a rdma RPC buffer

   @param buf: pointer to the ruc_buffer
   
   @retval 0 on success
   @retval<0 on error
*/

void rozofs_rdma_release_rpc_buffer(void *buf);

/*
**__________________________________________________
*/
/**
*check if the ruc_buffer belongs to the RPC RDMA pool

   @param buf: pointer to the ruc_buffer
   
   @retval 1 yes
   @retval 0 no
*/
int rozofs_is_rdma_rpc_buffer(void *buf);

/*
**__________________________________________________
*/
/**
  That thread is associated with a RDMA adaptor.
  Its role is to process asynchronous events received on that adaptor
  
  
  @param ctx : pointer to the RDMA context associated with the thread
  
  retval NULL on error
*/
void * rozofs_poll_rdma_async_event_th(void *ctx);


#else
static inline void rozofs_rdma_release_rpc_buffer(void *buf) {
  return;
}
#endif // ROZOFS_RDMA
#endif
