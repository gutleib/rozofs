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

#define _XOPEN_SOURCE 500

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stddef.h>
#include <fcntl.h>

#include <config.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/list.h>
#include <rozofs/common/log.h>
#include <rozofs/common/profile.h>
#include <rozofs/rpc/eproto.h>
#include <rozofs/core/rozofs_tx_common.h>
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/core/north_lbg_api.h>
//#include <rozofs/core/north_lbg_api_th.h>
#include <rozofs/rozofs_timer_conf.h>
#include "rozofs_fuse_api.h"
#include <rozofs/core/rozofs_host_list.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/rpc/expbt_protocol.h>
#include <rozofs/core/ruc_sockCtl_api_th.h>
#include <rozofs/core/rozofs_tx_common.h>
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/core/ruc_timer_api_th.h>
#include <rozofs/common/export_track.h>
#include <rozofs/common/mattr.h>
#include <rozofs/core/rozofs_queue.h>
#include <rozofs/core/rozofs_queue_pri.h>
#include "rozofs_bt_api.h"
#include "rozofs_bt_proto.h"
#include "rozofs_bt_inode.h"
#include "rozofs_bt_trk_reader.h"



#ifndef P_COUNT
#define P_COUNT     0
#define P_ELAPSE    1
#define P_BYTES     2
#endif
#define P_TIC 3

#define START_PROFILING_TH(stats,the_probe)\
    {\
        struct timeval tv;\
        stats->the_probe[P_COUNT]++;\
        gettimeofday(&tv,(struct timezone *)0);\
        stats->the_probe[P_TIC] = MICROLONG(tv);\
    }

#define STOP_PROFILING_TH(stats,the_probe)\
    {\
    struct timeval tv;\
    uint64_t toc;\
        gettimeofday(&tv,(struct timezone *)0);\
        toc = MICROLONG(tv);\
        stats->the_probe[P_ELAPSE] += (toc - stats->the_probe[P_TIC]);\
    }

#define STOP_PROFILING_IO_TH(stats,the_probe,the_bytes)\
    {\
    struct timeval tv;\
    uint64_t toc;\
        gettimeofday(&tv,(struct timezone *)0);\
        toc = MICROLONG(tv);\
        stats->the_probe[P_ELAPSE] += (toc - stats->the_probe[P_TIC]);\
        stats->the_probe[P_BYTES]  += the_bytes;\
    }

typedef struct  _rozofs_bt_thread_cli_reader_stats_t {
	uint64_t file_read[4];
	uint64_t file_check[4];
	uint64_t load_dentry[4];
	uint64_t poll[4];

} rozofs_bt_thread_cli_reader_stats_t;

typedef struct _rozofs_bt_thread_cli_reader_ctx_t
{
  pthread_t        thrdId; 
  int              thread_idx;
  int              mount_id;
  int              transaction_ctx_count; /**< number of the transaction context */
  int              trx_xmit_buffer_size; /**< size of the xmit buffer            */
  int              trx_recv_buffer_size; /**< size of the recv buffer            */
  int              socket_controller_nb_ctx;  /**< number of socket controller context */
  void            *sock_p;                   /**< pointer to the socket controller context */
  int             af_unix_buffer_count;     /**< number of buffers for receiving requests  */
  int             af_unix_buffer_size;     /**< number of buffers for receiving requests  */
  int             af_unix_fd;
  int             lbg_id;                 /**< reference of the load balancing to connect with export */
  rozofs_bt_thread_cli_reader_stats_t stats;
  
} rozofs_bt_thread_cli_reader_ctx_t;

struct  sockaddr_un rozofs_bt_cli_reader_socket_name[ROZOFS_BT_MAX_CLI_READER_THREADS];
rozofs_bt_thread_cli_reader_ctx_t rozofs_bt_trk_cli_reader_thread_th[ROZOFS_BT_MAX_CLI_READER_THREADS]= {0};
int rozofs_bt_trk_cli_reader_thread_count = 2;
int rozofs_bt_trk_cli_reader_thread_ready[ROZOFS_BT_MAX_CLI_READER_THREADS] = {0};

uint64_t rozofs_cli_rd_uptime = 0;
uint64_t rozofs_cli_rd_now = 0;

 /**
 * prototypes
 */
uint32_t af_unix_bt_reader_rcvReadysock(void * af_unix_bt_reader_ctx_p,int socketId);
uint32_t af_unix_bt_reader_rcvMsgsock(void * af_unix_bt_reader_ctx_p,int socketId);
uint32_t af_unix_bt_reader_xmitReadysock(void * af_unix_bt_reader_ctx_p,int socketId);
uint32_t af_unix_bt_reader_xmitEvtsock(void * af_unix_bt_reader_ctx_p,int socketId);

#define DISK_SO_SENDBUF  (300*1024)
#define FUSE_SOCKET_NICKNAME "bt_cli_rd_th"



/*__________________________________________________________________________
*/  
#define SHOW_PROFILER_RD_PROBE(probe) pChar += sprintf(pChar," %-12s | %15"PRIu64" | %9"PRIu64" | %18"PRIu64" | %15s |\n",\
                    #probe,\
                    stats->probe[P_COUNT],\
                    stats->probe[P_COUNT]?stats->probe[P_ELAPSE]/stats->probe[P_COUNT]:0,\
                    stats->probe[P_ELAPSE]," " );

#define SHOW_PROFILER_RD_PROBE_BYTE(probe) pChar += sprintf(pChar," %-12s | %15"PRIu64" | %9"PRIu64" | %18"PRIu64" | %15"PRIu64" |\n",\
                    #probe,\
                    stats->probe[P_COUNT],\
                    stats->probe[P_COUNT]?stats->probe[P_ELAPSE]/stats->probe[P_COUNT]:0,\
                    stats->probe[P_ELAPSE],\
                    stats->probe[P_BYTES]);


#define RESET_PROFILER_PROBE(probe) \
{ \
         stats->probe[P_COUNT] = 0;\
         stats->probe[P_ELAPSE] = 0; \
}

#define RESET_PROFILER_PROBE_BYTE(probe) \
{ \
   RESET_PROFILER_PROBE(probe);\
   stats->probe[P_BYTES] = 0; \
}


static char * show_trkrd_profiler_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"trkrd_profiler reset       : reset statistics\n");
  pChar += sprintf(pChar,"trkrd profiler             : display statistics\n");  
  return pChar; 
}
void show_trkrd_profiler(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    rozofs_bt_thread_cli_reader_ctx_t *th_p;
    int i;
    rozofs_bt_thread_cli_reader_stats_t *stats;
    

    time_t elapse;
    int days, hours, mins, secs;
    time_t  this_time = time(0);    
    
    elapse = (int) (this_time - rozofs_cli_rd_uptime);
    days = (int) (elapse / 86400);
    hours = (int) ((elapse / 3600) - (days * 24));
    mins = (int) ((elapse / 60) - (days * 1440) - (hours * 60));
    secs = (int) (elapse % 60);


    pChar += sprintf(pChar, " uptime =  %d days, %2.2d:%2.2d:%2.2d\n",days, hours, mins, secs);
    th_p = rozofs_bt_trk_cli_reader_thread_th;
    for (i = 0; i <rozofs_bt_trk_cli_reader_thread_count; i++,th_p++)
    {
      stats = &th_p->stats;
      pChar += sprintf(pChar,"\nTracking file client reader #%d\n",th_p->thread_idx);
      pChar += sprintf(pChar, "   procedure  |     count       |  time(us) | cumulated time(us) |     bytes       |\n");
      pChar += sprintf(pChar, "--------------+-----------------+-----------+--------------------+-----------------+\n");
      SHOW_PROFILER_RD_PROBE(file_check);
      SHOW_PROFILER_RD_PROBE(load_dentry);
      SHOW_PROFILER_RD_PROBE_BYTE(file_read);

      if (argv[1] != NULL)
      {
	if (strcmp(argv[1],"reset")==0) {
	 RESET_PROFILER_PROBE_BYTE(file_check);
	 RESET_PROFILER_PROBE_BYTE(file_read);
	 RESET_PROFILER_PROBE(load_dentry);

	  pChar += sprintf(pChar,"Reset Done\n");  
	  rozofs_cli_rd_uptime = this_time;   
	}
	else {
	  /*
	  ** Help
	  */
	  pChar = show_trkrd_profiler_help(pChar);
	}
      } 
    }   
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}


/*
**  Call back function for socket controller
*/
ruc_sockCallBack_t af_unix_bt_reader_callBack_sock=
  {
     af_unix_bt_reader_rcvReadysock,
     af_unix_bt_reader_rcvMsgsock,
     af_unix_bt_reader_xmitReadysock,
     af_unix_bt_reader_xmitEvtsock
  };
  
  /*
**__________________________________________________________________________
*/
/**
  Application callBack:

  Called from the socket controller. 

serial
  @param unused: not used
  @param socketId: reference of the socket (not used)
 
  @retval : always FALSE
*/

uint32_t af_unix_bt_reader_xmitReadysock(void * unused,int socketId)
{

    return FALSE;
}
/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   Called from the socket controller upon receiving a xmit ready event
   for the associated socket. That callback is activeted only if the application
   has replied TRUE in rozofs_bt_xmitReadysock().
   
   It typically the processing of a end of congestion on the socket

    
  @param unused: not used
  @param socketId: reference of the socket (not used)
 
   @retval :always TRUE
*/
uint32_t af_unix_bt_reader_xmitEvtsock(void * unused,int socketId)
{   
    return TRUE;
}
/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   receiver ready function: called from socket controller.
   The module is intended to return if the receiver is ready to receive a new message
   and FALSE otherwise

    
  @param thread_p: pointer to the thread context
  @param socketId: reference of the socket (not used)
 
  @retval : TRUE-> receiver ready
  @retval : FALSE-> receiver not ready
*/

uint32_t af_unix_bt_reader_rcvReadysock(void * thread_p,int socketId)
{
  int count;
  /*
  ** need to check the level of transaction context
  */
  count = rozofs_tx_get_free_ctx_number_th();
  if (count < 2) return FALSE;
  return TRUE;
}


/*
**__________________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the initial message receivde on the AF_UNIX socket
 
 @return none
 */

void rozofs_bt_check_tracking_file_remote_cbk(void *this,void *param) 
{
   expbt_msgint_full_t *msg_th_p;
   int status;
   void     *recv_buf = NULL;   
   int      bufsize;
   uint32_t thread_idx;
   rozofs_bt_thread_cli_reader_ctx_t* thread_p;
   expbt_msg_t *rsp_check_p;
   int i;
    
   /*
   ** set the pointer to the initial message
   */
   msg_th_p = (expbt_msgint_full_t*)param;  
   /*
   ** Restore opaque data
   */ 
   rozofs_tx_read_opaque_data(this,2,&thread_idx);  
   /*
   ** get the pointer to the thread context
   */
   if (ROZOFS_BT_MAX_CLI_READER_THREADS <= thread_idx)
   {
      fatal("Thread index is out of range:%d max is %u",thread_idx,ROZOFS_BT_MAX_CLI_READER_THREADS);
   } 
   thread_p= &rozofs_bt_trk_cli_reader_thread_th[thread_idx];    
   /*   
   ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
   */
   status = rozofs_tx_get_status_th(this);
   if (status < 0)
   {
      /*
      ** something wrong happened
      */
      errno = rozofs_tx_get_errno_th(this);  
      goto error; 
   }
   /*
   ** get the pointer to the receive buffer payload
   */
   recv_buf = rozofs_tx_get_recvBuf_th(this);
   if (recv_buf == NULL)
   {
      /*
      ** something wrong happened
      */
      errno = EFAULT;  
      goto error;         
   }


   rsp_check_p  = (expbt_msg_t*) ruc_buf_getPayload(recv_buf);
   /*
   ** Get the length of the payload
   */
   bufsize = (int) ruc_buf_getPayloadLen(recv_buf);
   bufsize -= sizeof(uint32_t); /* skip length*/
   if (bufsize != rsp_check_p->hdr.len)
   {
    TX_STATS_TH(ROZOFS_TX_DECODING_ERROR);
    errno = EPROTO;
    goto error;
   }
   /*
   ** Get the global status 
   */
   if (rsp_check_p->check_rsp.global_rsp.status < 0)
   {
      errno = rsp_check_p->check_rsp.global_rsp.errcode;
      goto error;
   }
   msg_th_p->rsp.check_rsp.global_rsp.status = 0;
   msg_th_p->rsp.check_rsp.global_rsp.errcode = 0;
   /*
   ** ok now goto to each command and copy the status
   */
   msg_th_p->rsp.check_rsp.rsp.nb_responses = rsp_check_p->check_rsp.rsp.nb_responses;
   for (i = 0; i < msg_th_p->req.check_rq.cmd.nb_commands;i++)
   {
     msg_th_p->rsp.check_rsp.entry[i].file_id = rsp_check_p->check_rsp.entry[i].file_id;
     msg_th_p->rsp.check_rsp.entry[i].usr_id  = rsp_check_p->check_rsp.entry[i].usr_id;
     msg_th_p->rsp.check_rsp.entry[i].type    = rsp_check_p->check_rsp.entry[i].type;
     msg_th_p->rsp.check_rsp.entry[i].status  = rsp_check_p->check_rsp.entry[i].status;
     msg_th_p->rsp.check_rsp.entry[i].errcode = rsp_check_p->check_rsp.entry[i].errcode;
 
   }
   STOP_PROFILING_IO_TH((&thread_p->stats),file_check,rsp_check_p->check_rsp.rsp.nb_responses); 
   
   goto out;
error:
   STOP_PROFILING_TH((&thread_p->stats),file_check);
   msg_th_p->rsp.check_rsp.global_rsp.status = -1;
   msg_th_p->rsp.check_rsp.global_rsp.errcode = errno;
   for (i = 0; i < msg_th_p->req.check_rq.cmd.nb_commands;i++)
   {
     msg_th_p->rsp.check_rsp.entry[i].file_id  = msg_th_p->req.check_rq.entry[i].file_id;
     msg_th_p->rsp.check_rsp.entry[i].usr_id   = msg_th_p->req.check_rq.entry[i].usr_id;
     msg_th_p->rsp.check_rsp.entry[i].type     = msg_th_p->req.check_rq.entry[i].type;
     msg_th_p->rsp.check_rsp.entry[i].status   = -1;
     msg_th_p->rsp.check_rsp.entry[i].errcode  = errno; 
   }

out:
  /*
  ** send back the response
  */
   rozofs_queue_put_prio((rozofs_queue_prio_t*)msg_th_p->hdr.rozofs_queue,msg_th_p,msg_th_p->hdr.queue_prio);	
   /*
   ** release the transaction context and the fuse context
   */
   if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);    
   rozofs_tx_free_from_ptr_th(this);
   return;
}

/*
**__________________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the initial message receivde on the AF_UNIX socket
 
 @return none
 */

void rozofs_bt_load_dirent_cbk(void *this,void *param) 
{
   expbt_msgint_full_t *msg_th_p;
   int status;
   void     *recv_buf = NULL;   
   int      bufsize;
   uint32_t thread_idx;
   rozofs_bt_thread_cli_reader_ctx_t* thread_p;
   expbt_dirent_load_rsp_t *rsp_rd_p;
   expbt_msg_hdr_t *rsp_hdr_p;
    
   /*
   ** set the pointer to the initial message
   */
   msg_th_p = (expbt_msgint_full_t*)param;  
   /*
   ** Restore opaque data
   */ 
   rozofs_tx_read_opaque_data(this,2,&thread_idx);  
   /*
   ** get the pointer to the thread context
   */
   if (ROZOFS_BT_MAX_CLI_READER_THREADS <= thread_idx)
   {
      fatal("Thread index is out of range:%d max is %u",thread_idx,ROZOFS_BT_MAX_CLI_READER_THREADS);
   } 
   thread_p= &rozofs_bt_trk_cli_reader_thread_th[thread_idx];    
   /*   
   ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
   */
   status = rozofs_tx_get_status_th(this);
   if (status < 0)
   {
      /*
      ** something wrong happened
      */
      errno = rozofs_tx_get_errno_th(this);  
      goto error; 
   }
   /*
   ** get the pointer to the receive buffer payload
   */
   recv_buf = rozofs_tx_get_recvBuf_th(this);
   if (recv_buf == NULL)
   {
      /*
      ** something wrong happened
      */
      errno = EFAULT;  
      goto error;         
   }


   rsp_hdr_p  = (expbt_msg_hdr_t*) ruc_buf_getPayload(recv_buf);
   /*
   ** Get the length of the payload
   */
   bufsize = (int) ruc_buf_getPayloadLen(recv_buf);
   bufsize -= sizeof(uint32_t); /* skip length*/
   if (bufsize != rsp_hdr_p->len)
   {
    TX_STATS_TH(ROZOFS_TX_DECODING_ERROR);
    errno = EPROTO;
    goto error;
   }
   /*
   ** go to the status of the read
   */
   rsp_rd_p = (expbt_dirent_load_rsp_t*)(rsp_hdr_p+1);
   if (rsp_rd_p->status < 0)
   {
      errno = rsp_rd_p->errcode;
      goto error;
   }
   /*
   ** set the timestamp and the cache_time
   */
   msg_th_p->rsp.dirent_rsp.rsp.status = 0;
   msg_th_p->rsp.dirent_rsp.rsp.errcode = 0; 
   STOP_PROFILING_TH((&thread_p->stats),load_dentry); 
   
   goto out;
error:
   STOP_PROFILING_TH((&thread_p->stats),load_dentry);
   msg_th_p->rsp.dirent_rsp.rsp.status = -1;
   msg_th_p->rsp.dirent_rsp.rsp.errcode = errno;   

out:
  /*
  ** send back the response
  */
   rozofs_queue_put_prio((rozofs_queue_prio_t*)msg_th_p->hdr.rozofs_queue,msg_th_p,msg_th_p->hdr.queue_prio);	
   /*
   ** release the transaction context and the fuse context
   */
   if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);    
   rozofs_tx_free_from_ptr_th(this);
   return;
}

/*
**_________________________________________________________________________________________________
*/
/**
   send a request to load the dirent file on the local client.
   The export uses rsync to do so.
   
   @param thread_p: pointer to the thread context
   @param msg_th_p: pointer to the message receive on the internal AF_UNIX socket
   
   @retval none
*/
void rozofs_bt_load_dirent(rozofs_bt_thread_cli_reader_ctx_t* thread_p, expbt_msgint_full_t *msg_th_p)
{
  void *xmit_buf = NULL;
  int ret;
  expbt_msg_t *msg_p;
  rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;  
  uint32_t xid;

  START_PROFILING_TH((&thread_p->stats),load_dentry);
  
   xmit_buf = rozofs_tx_get_small_xmit_buf_th();
   if (xmit_buf == NULL)
   {
      errno = ENOMEM;
      goto error ; 
   }
   /*
   ** allocate a transaction context
   */
   rozofs_tx_ctx_p = rozofs_tx_alloc_th();  
   if (rozofs_tx_ctx_p == NULL) 
   {
      /*
      ** out of context
      ** --> put a pending list for the future to avoid repluing ENOMEM
      */
      TX_STATS_TH(ROZOFS_TX_NO_CTX_ERROR);
      errno = ENOMEM;
      goto error;
   } 
    /*
    ** get the pointer to the payload of the buffer
    */
    msg_p  = (expbt_msg_t*) ruc_buf_getPayload(xmit_buf);
    xid = rozofs_tx_alloc_xid_th(rozofs_tx_ctx_p);
    msg_p->hdr.xid = htonl(xid);
    msg_p->hdr.opcode = EXP_BT_DIRENT_LOAD;
    msg_p->hdr.dir = 0;
    msg_p->hdr.len = sizeof(expbt_msg_hdr_t)-sizeof(uint32_t);

    msg_p->dirent_rq.req.eid       = msg_th_p->req.dirent_rq.req.eid;
    msg_p->dirent_rq.req.inode     = msg_th_p->req.dirent_rq.req.inode;
    msg_p->dirent_rq.req.ipaddr    = msg_th_p->req.dirent_rq.req.ipaddr;
    strcpy(msg_p->dirent_rq.req.client_export_root_path,msg_th_p->req.dirent_rq.req.client_export_root_path);

    msg_p->hdr.len = sizeof(expbt_msg_hdr_t)+ sizeof(expbt_dirent_load_req_t) -sizeof(uint32_t);    
    /*
    ** set the payload length in the xmit buffer
    */
    int total_len = msg_p->hdr.len+sizeof(uint32_t);
    ruc_buf_setPayloadLen(xmit_buf,total_len);
    /*
    ** store the receive call back and its associated parameter
    */
    rozofs_tx_ctx_p->recv_cbk   = rozofs_bt_load_dirent_cbk;
    rozofs_tx_ctx_p->user_param = msg_th_p;    
    rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,1,1);  /* lock */
    rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,2,thread_p->thread_idx);  /* index of the thread context */  
    
    ret = north_lbg_send(thread_p->lbg_id,xmit_buf);
    if (ret < 0)
    {
       TX_STATS_TH(ROZOFS_TX_SEND_ERROR);
       /*
       ** attempt to get the next available load balancing group
       */
       errno = EFAULT;
      goto error;  
    }
    TX_STATS(ROZOFS_TX_SEND);

    /*
    ** OK, so now finish by starting the guard timer
    */
    rozofs_tx_start_timer_th(rozofs_tx_ctx_p, ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM));
    return;
    
  error:

    msg_th_p->rsp.dirent_rsp.rsp.status = -1;
    msg_th_p->rsp.dirent_rsp.rsp.errcode = errno;       
    /*
    ** send back the response
    */
     rozofs_queue_put_prio((rozofs_queue_prio_t*)msg_th_p->hdr.rozofs_queue,msg_th_p,msg_th_p->hdr.queue_prio);	
     STOP_PROFILING_TH((&thread_p->stats),load_dentry);

    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);   
}     



/*
**_________________________________________________________________________________________________
*/
/**
   send a read request of a tracking towards the exportd
   
   @param thread_p: pointer to the thread context
   @param msg_th_p: pointer to the message receive on the internal AF_UNIX socket
   
   @retval none
*/
void rozofs_bt_check_tracking_file_remote(rozofs_bt_thread_cli_reader_ctx_t* thread_p, expbt_msgint_full_t *msg_th_p)
{
  void *xmit_buf = NULL;
  int ret;
  expbt_msg_t *msg_p;
  rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;  
  uint32_t xid;
  int i;

  START_PROFILING_TH((&thread_p->stats),file_check);
  
   xmit_buf = rozofs_tx_get_small_xmit_buf_th();
   if (xmit_buf == NULL)
   {
      errno = ENOMEM;
      goto error ; 
   }
   /*
   ** allocate a transaction context
   */
   rozofs_tx_ctx_p = rozofs_tx_alloc_th();  
   if (rozofs_tx_ctx_p == NULL) 
   {
      /*
      ** out of context
      ** --> put a pending list for the future to avoid repluing ENOMEM
      */
      TX_STATS_TH(ROZOFS_TX_NO_CTX_ERROR);
      errno = ENOMEM;
      goto error;
   } 
    /*
    ** get the pointer to the payload of the buffer
    */
    msg_p  = (expbt_msg_t*) ruc_buf_getPayload(xmit_buf);
    xid = rozofs_tx_alloc_xid_th(rozofs_tx_ctx_p);
    msg_p->hdr.xid = htonl(xid);
    msg_p->hdr.opcode = EXP_BT_TRK_CHECK;
    msg_p->hdr.dir = 0;
    msg_p->hdr.len = sizeof(expbt_msg_hdr_t)-sizeof(uint32_t);

    msg_p->check_rq.cmd.nb_commands     = msg_th_p->req.check_rq.cmd.nb_commands;
    for (i = 0; i < msg_p->check_rq.cmd.nb_commands;i++)
    {
      msg_p->check_rq.entry[i].eid     = msg_th_p->req.check_rq.entry[i].eid;
      msg_p->check_rq.entry[i].type    = msg_th_p->req.check_rq.entry[i].type;
      msg_p->check_rq.entry[i].usr_id  = msg_th_p->req.check_rq.entry[i].usr_id;
      msg_p->check_rq.entry[i].file_id = msg_th_p->req.check_rq.entry[i].file_id;
      msg_p->check_rq.entry[i].mtime = msg_th_p->req.check_rq.entry[i].mtime;
      msg_p->check_rq.entry[i].change_count = msg_th_p->req.check_rq.entry[i].change_count;

        FDL_INFO("FDL -----> type %d  mtime in message: %llu change_count %d",(int)  msg_p->check_rq.entry[i].type,(long long unsigned int)msg_p->check_rq.entry[i].mtime,(int)msg_p->check_rq.entry[i].change_count);
    }

    msg_p->hdr.len = sizeof(expbt_msg_hdr_t)+ +sizeof(expbt_trk_check_req_t)+ msg_p->check_rq.cmd.nb_commands*sizeof(expbt_trk_check_req_entry_t) -sizeof(uint32_t);    
    /*
    ** set the payload length in the xmit buffer
    */
    int total_len = msg_p->hdr.len+sizeof(uint32_t);
    
        FDL_INFO("FDL -----> tmessage len = %d",total_len);
    ruc_buf_setPayloadLen(xmit_buf,total_len);
    /*
    ** store the receive call back and its associated parameter
    */
    rozofs_tx_ctx_p->recv_cbk   = rozofs_bt_check_tracking_file_remote_cbk;
    rozofs_tx_ctx_p->user_param = msg_th_p;    
    rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,1,msg_th_p->req.check_rq.cmd.nb_commands);  /* number of commands */
    rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,2,thread_p->thread_idx);  /* index of the thread context */  
    
    ret = north_lbg_send(thread_p->lbg_id,xmit_buf);
    if (ret < 0)
    {
       TX_STATS_TH(ROZOFS_TX_SEND_ERROR);
       /*
       ** attempt to get the next available load balancing group
       */
       errno = EFAULT;
      goto error;  
    }
    TX_STATS(ROZOFS_TX_SEND);

    /*
    ** OK, so now finish by starting the guard timer
    */
    rozofs_tx_start_timer_th(rozofs_tx_ctx_p, ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM));
    return;
    
  error:
    msg_th_p->rsp.check_rsp.global_rsp.status = -1;
    msg_th_p->rsp.check_rsp.global_rsp.errcode = errno;
    for (i = 0; i < msg_th_p->req.check_rq.cmd.nb_commands;i++)
    {
      msg_th_p->rsp.check_rsp.entry[i].file_id  = msg_th_p->req.check_rq.entry[i].file_id;
      msg_th_p->rsp.check_rsp.entry[i].usr_id   = msg_th_p->req.check_rq.entry[i].usr_id;
      msg_th_p->rsp.check_rsp.entry[i].type     = msg_th_p->req.check_rq.entry[i].type;
      msg_th_p->rsp.check_rsp.entry[i].status   = -1;
      msg_th_p->rsp.check_rsp.entry[i].errcode  = errno; 
    }
    /*
    ** send back the response
    */
     rozofs_queue_put_prio((rozofs_queue_prio_t*)msg_th_p->hdr.rozofs_queue,msg_th_p,msg_th_p->hdr.queue_prio);	
     STOP_PROFILING_TH((&thread_p->stats),file_check);

    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);   
}     



/*
**__________________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the initial message receivde on the AF_UNIX socket
 
 @return none
 */

void rozofs_bt_reading_tracking_file_remote_cbk(void *this,void *param) 
{
   expbt_msgint_full_t *msg_th_p;
   int status;
   void     *recv_buf = NULL;   
   int      bufsize;
   uint32_t thread_idx;
   rozofs_bt_thread_cli_reader_ctx_t* thread_p;
   expbt_trk_read_rsp_t *rsp_rd_p;
   expbt_msg_hdr_t *rsp_hdr_p;
   uint8_t  *data_trk_src_p;
   rozofs_bt_tracking_cache_t *image_p;   
   int datalen;
    
   /*
   ** set the pointer to the initial message
   */
   msg_th_p = (expbt_msgint_full_t*)param;  
   image_p = msg_th_p->req.read_trk_rq.image_p;
//   data_trk_dst_p = (uint8_t *) (image_p+1);
   /*
   ** Restore opaque data
   */ 
   rozofs_tx_read_opaque_data(this,2,&thread_idx);  
   /*
   ** get the pointer to the thread context
   */
   if (ROZOFS_BT_MAX_CLI_READER_THREADS <= thread_idx)
   {
      fatal("Thread index is out of range:%d max is %u",thread_idx,ROZOFS_BT_MAX_CLI_READER_THREADS);
   } 
   thread_p= &rozofs_bt_trk_cli_reader_thread_th[thread_idx];    
   /*   
   ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
   */
   status = rozofs_tx_get_status_th(this);
   if (status < 0)
   {
      /*
      ** something wrong happened
      */
      errno = rozofs_tx_get_errno_th(this);  
      goto error; 
   }
   /*
   ** get the pointer to the receive buffer payload
   */
   recv_buf = rozofs_tx_get_recvBuf_th(this);
   if (recv_buf == NULL)
   {
      /*
      ** something wrong happened
      */
      errno = EFAULT;  
      goto error;         
   }


   rsp_hdr_p  = (expbt_msg_hdr_t*) ruc_buf_getPayload(recv_buf);
   /*
   ** Get the length of the payload
   */
   bufsize = (int) ruc_buf_getPayloadLen(recv_buf);
   bufsize -= sizeof(uint32_t); /* skip length*/
   if (bufsize != rsp_hdr_p->len)
   {
    TX_STATS_TH(ROZOFS_TX_DECODING_ERROR);
    errno = EPROTO;
    goto error;
   }
   /*
   ** go to the status of the read
   */
   rsp_rd_p = (expbt_trk_read_rsp_t*)(rsp_hdr_p+1);
   if (rsp_rd_p->status < 0)
   {
      errno = rsp_rd_p->errcode;
      goto error;
   }
   datalen = rsp_rd_p->status;
   /*
   ** check if there is already a memory allocated for caching the tracking file
   ** When it is the case, we have to queue that memory array to the garbage collector
   */
   {
      rozofs_bt_tracking_cache_payload_hdr_t  *old_payload;
      rozofs_bt_tracking_cache_payload_hdr_t  *new_payload;   
   
      old_payload = image_p->trk_payload;
      
      new_payload = malloc(sizeof(rozofs_bt_tracking_cache_payload_hdr_t) + datalen);
      if (new_payload == NULL)
      {
         errno = ENOMEM;
	 goto error;
      }

      /*
      ** init of the new context
      */
      list_init(&new_payload->list_delete);
      new_payload->deadline_delete = 0;
      new_payload->tracking_payload = (uint8_t*)(new_payload+1);
      /*
      ** copy the new data
      */
      data_trk_src_p = (uint8_t *)(rsp_rd_p+1);
      memcpy(new_payload->tracking_payload,data_trk_src_p,datalen); 
      new_payload->datalen = datalen;  
      /*
      ** swap the payload pointer in the tracking cache entry
      */
      image_p->trk_payload = new_payload;
      /*
      ** queue the old entry in the garbage collector if it exists
      */
      if (old_payload != NULL) rozofs_bt_rcu_queue_trk_file_payload_in_garbage_collector(old_payload);
   }
   /*
   ** set the mtime and the change count
   */
   image_p->mtime = rsp_rd_p->mtime; 
   image_p->change_count = rsp_rd_p->change_count; 
   FDL_INFO("FDL cache mtime %llu change_count %d",(long long unsigned int)image_p->mtime,(int)image_p->change_count);
   /*
   ** set the timestamp and the cache_time
   */
   image_p->timestamp = rozofs_get_ticker_us();
   image_p->cache_time = (msg_th_p->req.read_trk_rq.read_trk.type == ROZOFS_REG)?rozofs_tmr_get_attr_us(0):rozofs_tmr_get_attr_us(1);
   msg_th_p->rsp.read_trk_rsp.rsp.status = 0;
   msg_th_p->rsp.read_trk_rsp.rsp.errcode = 0; 
   msg_th_p->rsp.read_trk_rsp.rsp.mtime = rsp_rd_p->mtime;   
   msg_th_p->rsp.read_trk_rsp.rsp.mtime = rsp_rd_p->change_count;   
   STOP_PROFILING_IO_TH((&thread_p->stats),file_read,datalen); 
   
   goto out;
error:
   STOP_PROFILING_TH((&thread_p->stats),file_read);
   msg_th_p->rsp.read_trk_rsp.rsp.status = -1;
   msg_th_p->rsp.read_trk_rsp.rsp.errcode = errno;   
   image_p->errcode = errno;
   /*
   ** do not update the timestamp and cache time in case of error
   */
//   image_p->timestamp = rozofs_get_ticker_us();
//   image_p->cache_time = (msg_th_p->req.read_trk_rq.read_trk.type == ROZOFS_REG)?rozofs_tmr_get_attr_us(0):rozofs_tmr_get_attr_us(1));;      


out:
  /*
  ** send back the response
  */
   rozofs_queue_put_prio((rozofs_queue_prio_t*)msg_th_p->hdr.rozofs_queue,msg_th_p,msg_th_p->hdr.queue_prio);	
   /*
   ** release the transaction context and the fuse context
   */
   if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);    
   rozofs_tx_free_from_ptr_th(this);
   return;
}

/*
**_________________________________________________________________________________________________
*/
/**
   send a read request of a tracking towards the exportd
   
   @param thread_p: pointer to the thread context
   @param msg_th_p: pointer to the message receive on the internal AF_UNIX socket
   
   @retval none
*/
void rozofs_bt_reading_tracking_file_remote(rozofs_bt_thread_cli_reader_ctx_t* thread_p, expbt_msgint_full_t *msg_th_p)
{
  void *xmit_buf = NULL;
  int ret;
  expbt_msg_t *msg_p;
  rozofs_bt_tracking_cache_t *image_p;     
  rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;  
  image_p = msg_th_p->req.read_trk_rq.image_p;
  uint32_t xid;

  START_PROFILING_TH((&thread_p->stats),file_read);
  
   xmit_buf = rozofs_tx_get_small_xmit_buf_th();
   if (xmit_buf == NULL)
   {
      errno = ENOMEM;
      goto error ; 
   }
   /*
   ** allocate a transaction context
   */
   rozofs_tx_ctx_p = rozofs_tx_alloc_th();  
   if (rozofs_tx_ctx_p == NULL) 
   {
      /*
      ** out of context
      ** --> put a pending list for the future to avoid repluing ENOMEM
      */
      TX_STATS_TH(ROZOFS_TX_NO_CTX_ERROR);
      errno = ENOMEM;
      goto error;
   } 
    /*
    ** get the pointer to the payload of the buffer
    */
    msg_p  = (expbt_msg_t*) ruc_buf_getPayload(xmit_buf);
    xid = rozofs_tx_alloc_xid_th(rozofs_tx_ctx_p);
    msg_p->hdr.xid = htonl(xid);
    msg_p->hdr.opcode = EXP_BT_TRK_READ;
    msg_p->hdr.dir = 0;
    msg_p->hdr.len = sizeof(expbt_msg_hdr_t)-sizeof(uint32_t);

    msg_p->read_trk_rq.read_trk.eid     = msg_th_p->req.read_trk_rq.read_trk.eid;
    msg_p->read_trk_rq.read_trk.type    = msg_th_p->req.read_trk_rq.read_trk.type;
    msg_p->read_trk_rq.read_trk.usr_id  = msg_th_p->req.read_trk_rq.read_trk.usr_id;
    msg_p->read_trk_rq.read_trk.file_id = msg_th_p->req.read_trk_rq.read_trk.file_id;

    msg_p->hdr.len = sizeof(expbt_msg_hdr_t)+ sizeof(expbt_trk_read_req_t) -sizeof(uint32_t);    
    /*
    ** set the payload length in the xmit buffer
    */
    int total_len = msg_p->hdr.len+sizeof(uint32_t);
    ruc_buf_setPayloadLen(xmit_buf,total_len);
    /*
    ** store the receive call back and its associated parameter
    */
    rozofs_tx_ctx_p->recv_cbk   = rozofs_bt_reading_tracking_file_remote_cbk;
    rozofs_tx_ctx_p->user_param = msg_th_p;    
    rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,1,1);  /* lock */
    rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,2,thread_p->thread_idx);  /* index of the thread context */  
    
    ret = north_lbg_send(thread_p->lbg_id,xmit_buf);
    if (ret < 0)
    {
       TX_STATS_TH(ROZOFS_TX_SEND_ERROR);
       /*
       ** attempt to get the next available load balancing group
       */
       errno = EFAULT;
      goto error;  
    }
    TX_STATS(ROZOFS_TX_SEND);

    /*
    ** OK, so now finish by starting the guard timer
    */
    rozofs_tx_start_timer_th(rozofs_tx_ctx_p, ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM));
    return;
    
  error:

    msg_th_p->rsp.read_trk_rsp.rsp.status = -1;
    msg_th_p->rsp.read_trk_rsp.rsp.errcode = errno;   
    image_p->errcode = errno;
    /*
    ** do not update the timestamp of the tracking cache entry since there was an error
    */
//    image_p->timestamp = rozofs_get_ticker_us();
//    image_p->cache_time = (msg_th_p->req.read_trk_rq.read_trk.type == ROZOFS_REG)?rozofs_tmr_get_attr_us(0):rozofs_tmr_get_attr_us(1));      
    /*
    ** send back the response
    */
     rozofs_queue_put_prio((rozofs_queue_prio_t*)msg_th_p->hdr.rozofs_queue,msg_th_p,msg_th_p->hdr.queue_prio);	
     STOP_PROFILING_TH((&thread_p->stats),file_read);

    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);   
}     

/*
**_________________________________________________________________
*/
/**
*    Local file tracking read: only when rozofsmount and export resides on the same node

     @param eid: export identifier
     @param type: either ROZOFS_REG or ROZOFS_DIR
     @param usr_id: slice identifier
     @param file_id: index of the file
     
     @retval 0 on success
     @retval < 0 on error
*/
int expbt_read_inode_tracking_file(uint32_t eid,uint16_t type,uint16_t usr_id,uint64_t file_id,char *buf_p,uint64_t *mtime,uint32_t *change_count)
{

   int fd;
   char filename[1024];
   struct stat statbuf;
   ssize_t ret;
   
   *change_count = 0;
   
   
   while (1)
   {
     if (type == ROZOFS_REG)
     {
       sprintf(filename,"%s/host0/reg_attr/%d/trk_%llu",rozofs_bt_export_path_p,usr_id,(long long unsigned int)file_id);
       break;
     }
     if (type ==ROZOFS_DIR)
     {
       sprintf(filename,"%s/host0/dir_attr/%d/trk_%llu",rozofs_bt_export_path_p,usr_id,(long long unsigned int)file_id);
       break;
     }
     errno = ENOTSUP;
     return -1;
   }
   fd = open(filename,O_RDONLY);
   if (fd < 0) return -1;
   ret = fstat(fd,&statbuf);
   if (ret < 0)
   {
     close(fd);
     return -1;
   } 
   
   if (buf_p == NULL)
   {
     /*
     ** it was just a mtime check
     */
     *mtime = statbuf.st_mtime;
     close(fd);
     return 0;
   }
   /*
   ** Attempt to read the file
   */ 
   ret = pread( fd, buf_p, statbuf.st_size, 0);
   if (ret != statbuf.st_size)
   {
     /*
     ** we might need to check the length since it might be possible that one inode has been added or remove
     */
     close(fd);
     return -1;        
   }
   close(fd);
      
   return statbuf.st_size;
}


/*
**__________________________________________________________________________
*/

void rozofs_bt_reading_tracking_file_local(rozofs_bt_thread_cli_reader_ctx_t* thread_p, expbt_msgint_full_t *msg_p)
{
  uint64_t mtime;
  uint32_t change_count;
  int ret;
  rozofs_bt_tracking_cache_t *image_p;
  char *data_p;
  
  image_p = msg_p->req.read_trk_rq.image_p;
  data_p = (char *) (image_p+1);
  
  START_PROFILING_TH((&thread_p->stats),file_read);
  ret = expbt_read_inode_tracking_file(msg_p->req.read_trk_rq.read_trk.eid,
                                       msg_p->req.read_trk_rq.read_trk.type,
				       msg_p->req.read_trk_rq.read_trk.usr_id,
				       msg_p->req.read_trk_rq.read_trk.file_id,
				       data_p,
				       &mtime,
				       &change_count);

   if (ret < 0 )
   {
     STOP_PROFILING_TH((&thread_p->stats),file_read);
     msg_p->rsp.read_trk_rsp.rsp.status = -1;
     msg_p->rsp.read_trk_rsp.rsp.errcode = errno;   
     image_p->errcode = errno;
     warning("FDL error on tracking file reading %s\n",strerror(errno));
   
   }
   else
   {

     msg_p->rsp.read_trk_rsp.rsp.status = 0;
     msg_p->rsp.read_trk_rsp.rsp.errcode = 0; 
     msg_p->rsp.read_trk_rsp.rsp.mtime = mtime;  
     image_p->mtime = mtime; 
     STOP_PROFILING_IO_TH((&thread_p->stats),file_read,ret);   
   }
   image_p->timestamp = rozofs_get_ticker_us();
   image_p->cache_time = rozofs_tmr_get_attr_us(1);
   
   rozofs_queue_put_prio((rozofs_queue_prio_t*)msg_p->hdr.rozofs_queue,msg_p,msg_p->hdr.queue_prio);				       
}
/*
**__________________________________________________________________________
*/
void af_unix_bt_reader_resquest(rozofs_bt_thread_cli_reader_ctx_t* thread_p,void *msg)
{
   expbt_msgint_full_t *msg_p;
   
   msg_p = (expbt_msgint_full_t*)msg;
   
   switch (msg_p->hdr.opcode)
   {
     case EXP_BT_TRK_READ:
//       return rozofs_bt_reading_tracking_file_local(thread_p,msg_p);
         FDL_INFO("FDL tracking file reader read request");
         return rozofs_bt_reading_tracking_file_remote(thread_p,msg_p);
       break;
     case EXP_BT_TRK_CHECK:
        return rozofs_bt_check_tracking_file_remote(thread_p,msg_p);
       break;
     case EXP_BT_DIRENT_LOAD:
        return  rozofs_bt_load_dirent(thread_p,msg_p);
      default:
         info("FDL unexpected opcode %d",(int)msg_p->hdr.opcode);
	break;
   }

}
/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   Called from the socket controller when there is a message pending on the
   socket associated with the context provide in input arguments.
   
   That service is intended to process a response sent by a disk thread

    
  @param unused: user parameter not used by the application
  @param socketId: reference of the socket 
 
   @retval : TRUE-> xmit ready event expected
  @retval : FALSE-> xmit  ready event not expected
*/

uint32_t af_unix_bt_reader_rcvMsgsock(void * thread_p,int socketId)
{
  uint64_t                 message;
  int                      bytesRcvd;
  int eintr_count = 0;
  int count;
  int loop = 0;
  
  /*
  ** disk responses have the highest priority, loop on the socket until
  ** the socket becomes empty
  */
  while(1) {  
    /*
    ** check if there are some pending requests
    */
    count = rozofs_tx_get_free_ctx_number_th();
    if (count < 2)
    {
     return TRUE;
    }
    if (loop == 4) return TRUE;
    /*
    ** read the north disk socket
    */
    bytesRcvd = recvfrom(socketId,
			 &message,sizeof(message), 
			 0,(struct sockaddr *)NULL,NULL);
    if (bytesRcvd == -1) {
     switch (errno)
     {
       case EAGAIN:
        /*
        ** the socket is empty
        */
        return TRUE;

       case EINTR:
         /*
         ** re-attempt to read the socket
         */
         eintr_count++;
         if (eintr_count < 3) continue;
         /*
         ** here we consider it as a error
         */
         severe ("Fuse Thread Response error too many eintr_count %d",eintr_count);
         return TRUE;

       case EBADF:
       case EFAULT:
       case EINVAL:
       default:
         /*
         ** We might need to double checl if the socket must be killed
         */
         fatal("Fuse Thread Response error on recvfrom %s !!\n",strerror(errno));
         exit(0);
     }

    }
    if (bytesRcvd == 0) {
      fatal("Fuse Thread Response socket is dead %s !!\n",strerror(errno));
      exit(0);    
    } 
    loop++;
    af_unix_bt_reader_resquest((rozofs_bt_thread_cli_reader_ctx_t*)thread_p, (void*)message); 
  }       
  return TRUE;
}
/*
**__________________________________________________________________________
*/
/**
* fill the storio  AF_UNIX name in the global data

  @param hostname
  @param socketname : pointer to a sockaddr_un structure
  
  @retval none
*/
char * rozofs_bt_cli_reader_set_sockname(struct sockaddr_un *socketname,char *name,int mount_id,int thread_id)
{
  socketname->sun_family = AF_UNIX;  
  char * pChar = socketname->sun_path;

  pChar += rozofs_string_append(pChar,name);
  *pChar++ = '_';
  *pChar++ = 'R';
  *pChar++ = 'D';
  *pChar++ = '_';
  *pChar++ = 'T';
  *pChar++ = 'R';  
  *pChar++ = 'K';  
  *pChar++ = '_';
  pChar += rozofs_u32_append(pChar,mount_id);
  *pChar++ = '_';
  *pChar++ = 't';
  pChar += rozofs_u32_append(pChar,thread_id);
  return socketname->sun_path;
}
/*
**_________________________________________________________________________________________
*/
/**
* creation of the AF_UNIX socket that is attached on the socket controller

  That socket is used to receive back the response from the threads that
  perform disk operation (read/write/truncate)
  
  @param socketname : name of the socket
  @param socketname : name of the socket for soclet controller
    
  @retval >= 0 : reference of the socket
  @retval < 0 : error
*/
int rozofs_bt_cli_reader_socket_create(rozofs_bt_thread_cli_reader_ctx_t *thread_ctx_p)
{
  int fd = -1;
  void *sockctrl_ref;
  char *socketname;
  
   socketname =  rozofs_bt_cli_reader_set_sockname(&rozofs_bt_cli_reader_socket_name[thread_ctx_p->thread_idx],
                                    ROZOFS_SOCK_FAMILY_FUSE_SOUTH,thread_ctx_p->mount_id,thread_ctx_p->thread_idx);

   while (1)
   {
     /*
     ** create the socket
     */
     fd = af_unix_sock_create_internal(socketname,DISK_SO_SENDBUF);
     if (fd == -1)
     {
       break;
     }
     /*
     ** OK, we are almost done, just need to connect with the socket controller
     */
     sockctrl_ref = ruc_sockctl_connect_th(thread_ctx_p->sock_p,fd,  // Reference of the socket
                                                FUSE_SOCKET_NICKNAME,   // name of the socket
                                                2,                  // Priority within the socket controller
                                                (void*)thread_ctx_p,      // user param for socketcontroller callback
                                                &af_unix_bt_reader_callBack_sock);  // Default callbacks
      if (sockctrl_ref == NULL)
      {
         /*
         ** Fail to connect with the socket controller
         */
         fatal("error on ruc_sockctl_connect");
         break;
      }
      /*
      ** All is fine
      */
      break;
    }    
    return fd;
}


/*
**_________________________________________________________________________________________
*/
/**
*  Client reader for tracking file read and update

   Each thread owns an AF_UNIX socket on which it gets requests for reading of tracking file or to revalidate a set of tracking file
   It own a TCP connection towards the export tracking file reader that operates in active stand-by mode
   

*/
int zzz_debug=0;
static void *rozofs_bt_trk_cli_reader_thread(void *v) {

   rozofs_bt_thread_cli_reader_ctx_t * thread_ctx_p = (rozofs_bt_thread_cli_reader_ctx_t*)v;
   char name[64];
    int ret;
   
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    sprintf(name,"TRK_READER#%d",thread_ctx_p->thread_idx);
    

    uma_dbg_thread_add_self("bt_cli_reader");
#if 1
#if 1
    while(zzz_debug)
    {
       info("ROZOFSMOUNT %u\n",getpid());
       sleep(2);
    }
#endif
    /*
    ** create a socket controller context
    */
    thread_ctx_p->sock_p = ruc_sockctl_init_th(thread_ctx_p->socket_controller_nb_ctx,name);
    ruc_timer_moduleInit_th(thread_ctx_p->sock_p,1);
    north_lbg_module_init_th();

    ret = rozofs_tx_module_init_th(thread_ctx_p->transaction_ctx_count , // fuse trx + internal trx
            thread_ctx_p->transaction_ctx_count, thread_ctx_p->trx_xmit_buffer_size, // xmit small [count,size]
            1, 1024, // xmit large [count,size]
            1, 1024, // recv small [count,size]
            thread_ctx_p->transaction_ctx_count, thread_ctx_p->trx_recv_buffer_size); // recv large [count,size];  

    if (ret != RUC_OK) 
    {
      severe("FDL Cannot create transaction module");
      fatal("Bye!!");
    }
    
    thread_ctx_p->lbg_id = rozofs_bt_create_client_lbg(thread_ctx_p->sock_p);
    if (thread_ctx_p->lbg_id < 0)
    {
      fatal("Cannot create the load balancing group (%s)",strerror(errno));
    }    
    thread_ctx_p->af_unix_fd = rozofs_bt_cli_reader_socket_create(thread_ctx_p);
    if ( thread_ctx_p->af_unix_fd < 0)
    {
     fatal("cannot create the AF_UNIX socket %s: %s",rozofs_bt_cli_reader_socket_name[thread_ctx_p->thread_idx].sun_path,strerror(errno));
    }
    /*
    ** Indicates that the thread is ready
    */
    rozofs_bt_trk_cli_reader_thread_ready[thread_ctx_p->thread_idx] = 1;
    info("Thread bt_cli_reader#%d is ready",thread_ctx_p->thread_idx);
    /*
    ** entering the main loop
    */
    ruc_sockCtrl_selectWait_th(thread_ctx_p->sock_p);
#endif     

    return NULL;
}

/*
**_________________________________________________________________________________________________
*/
/**
 Create the RCU inode thread that is used to update tracking file in cache and to delet tracking files that are not used anymore
 
 @retval 0 on success
 @retval -1 on error

*/ 
int rozofs_bt_trk_cli_reader_thread_create(int nb_threads)
{
    int err;
    pthread_attr_t             attr;    
    int i;
    rozofs_bt_thread_cli_reader_ctx_t *p;

   err = pthread_attr_init(&attr);
   if (err != 0) {
     fatal("rozofs_bt_thread_create pthread_attr_init() %s",strerror(errno));
     return -1;
   } 
   
   rozofs_cli_rd_uptime = (uint64_t) time(0);
   
   if ( nb_threads> ROZOFS_BT_MAX_CLI_READER_THREADS)
   {
     nb_threads =ROZOFS_BT_MAX_CLI_READER_THREADS;
   }
   rozofs_bt_trk_cli_reader_thread_count = nb_threads;

   p = rozofs_bt_trk_cli_reader_thread_th;
#warning TESTING ONLY
   for (i = 0; i < rozofs_bt_trk_cli_reader_thread_count; i++,p++)
   {
      memset(p,0,sizeof(rozofs_bt_thread_cli_reader_ctx_t));
      p->thread_idx = i;
      p->transaction_ctx_count = ROZOFS_BT_TRK_CLI_READER_MAX_TRX;
      p->trx_xmit_buffer_size = ROZOFS_BT_TRK_CLI_SMALL_XMIT_SIZE;
      p-> trx_recv_buffer_size= ROZOFS_BT_TRK_CLI_LARGE_RECV_SIZE;
      p->mount_id = conf.instance;
      p->socket_controller_nb_ctx = ROZOFS_BT_TRK_CLI_READER__MAX_SOCK;
     err = pthread_create(&p->thrdId,&attr,rozofs_bt_trk_cli_reader_thread,p);
     if (err != 0) {
       fatal("rozofs_bt_trk_cli_reader_thread pthread_create() %s", strerror(errno));
       return -1;
     } 
   }
   return 0; 
}
