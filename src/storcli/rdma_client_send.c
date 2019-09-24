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
#include <pthread.h>
#include <sys/types.h>    
#include <sys/socket.h>
#include <sys/time.h>
#include <rozofs/common/log.h>
#include <rozofs/rpc/sproto.h>
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/core/north_lbg_api.h>
#include <rozofs/rpc/rozofs_rpc_util.h>
#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/core/rozofs_tx_common.h>
#include <rozofs/core/rozofs_tx_api.h>
#include "rozofs_storcli.h"
#include "rozofs_storcli_rpc.h"
#include <rozofs/rdma/rozofs_rdma.h>
#include "rdma_client_send.h"

void rozofs_rdma_tx_out_of_seq_on_rdma_cbk(uint32_t lbg_id,uint32_t xid);

int storage_bin_read_first_bin_to_write = 0;

int       rozofs_storcli_rdma_post_recv_sockpair[2];       /**< index 0 is used by the RDMA signaling thread, index 1 is used by the client */
void *storcli_rdma_post_recv_sockctrl_p=NULL;
/*
**__________________________________________________
*/
/**
 Send a ruc_buffer from the completion queue associated with the SRQ of RDMA
  

  @param buf buffer received from the completion (SRQ) 
  
  @retval 0 on success
  @retval -1 on error (see errno for details)
*/
int rozofs_rdma_send2mainthread(void *buf)
{

   int ret;
   

   ret = write(rozofs_storcli_rdma_post_recv_sockpair[0],buf,sizeof(buf));
   if (ret < 0) return -1;
   if (ret != sizeof(buf)) return -1;
   return 0;
}

/*
**__________________________________________________________________________
*/
/**
*  Call back used upon receiving a RPC message over RDMA
   That call-back is called under the context onf the Completion Queue thread
   attached to a SRQ
   
   @param opcode: RDMA opcode MUST be IBV_WC_RECV
   @param ruc_buf: reference of the ruc_buffer that contains the encoded RPC message
   @param qp_num: reference of the QP on which the message has been received
   @param rozofs_rmda_ibv_cxt_p: pointer to the context of the adaptor from the rozofs side
   @param status: status of the operation (0 if no error)
   @param error: error code
*/

void storcli_rdma_msg_recv_form_cq_cbk(int opcode,void *ruc_buf, uint32_t qp_num,void *rozofs_rmda_ibv_cxt_p,int status,int error)
{
   rozofs_rdma_send2mainthread(ruc_buf);

}

/*
**__________________________________________________
*/
/**  
*  Always ready
*/
uint32_t storcli_rdma_post_recv_rcvReadyTcpSock(void * timerRef,int socketId)
{
    return TRUE;
}

/*
**__________________________________________________
*/

uint32_t storcli_rdma_post_recv_rcvMsgTcpSock(void * timerRef,int socketId)
{
   int ret;
   void *msg;
   while(1)
   {
     ret = read(rozofs_storcli_rdma_post_recv_sockpair[1],&msg,sizeof(msg));
     if (ret < 0) 
     {
        if (errno == EAGAIN) break;
	if  (errno == EINTR) continue;
	fatal("unexpected error on socketpair %s",strerror(errno));
     }
     if (ret != sizeof(msg)) 
     {
       fatal("Wrong message size %d",ret);
     }
     /*
     ** Call the transaction module in order to find out the transaction based on the xid
     */
     rozofs_tx_recv_rdma_rpc_cbk(msg);

   }
  return TRUE;
}
/*
**__________________________________________________
*/
uint32_t storcli_rdma_post_recv_xmitReadyTcpSock(void * timerRef,int socketId)
{
    return FALSE;
}
/*
**__________________________________________________
*/
uint32_t storcli_rdma_post_recv_xmitEvtTcpSock(void * timerRef,int socketId)
{
  return FALSE;

}
/*
**  Call back function for socket controller
*/
ruc_sockCallBack_t storcli_rdma_post_recv_callBack=
  {
     storcli_rdma_post_recv_rcvReadyTcpSock,
     storcli_rdma_post_recv_rcvMsgTcpSock,
     storcli_rdma_post_recv_xmitReadyTcpSock,
     storcli_rdma_post_recv_xmitEvtTcpSock
  };
/*
**__________________________________________________
*/
/**
*  Create the socket pair that will be used to get the RPC responses sent over RDMA

   @retval 0: OK
   @retval < 0 error (see errno for details
*/
int rozofs_storcli_rdma_rpc_create_receive_socket()
{
  int fileflags;
  /*
  ** Put the callbacks for buffer releasing & for out of sequence reception
  */
  rozofs_tx_set_rdma_buf_free_cbk(rozofs_rdma_release_rpc_buffer);
  rozofs_tx_set_rdma_out_of_sequence_cbk(rozofs_rdma_tx_out_of_seq_on_rdma_cbk);
  /* 
  ** create the socket pair
  */
  if (socketpair(  AF_UNIX,SOCK_DGRAM,0,&rozofs_storcli_rdma_post_recv_sockpair[0])< 0)
  {
      fatal("failed on socketpair:%s",strerror(errno));
     return -1;
  }
  while(1)
  {
    /*
    ** change socket mode to asynchronous for the socket used by the main thread side (it is the only
    ** socket index of the pair that is registered with the socket controller
    */
    if((fileflags=fcntl(rozofs_storcli_rdma_post_recv_sockpair[1],F_GETFL,0))<0)
    {
      warning ("rozofs_storcli_rdma_post_recv_sockpair[1]fcntl error:%s",strerror(errno));
      break;
    }
    if (fcntl(rozofs_storcli_rdma_post_recv_sockpair[1],F_SETFL,(fileflags|O_NDELAY))<0)
    {
      warning ("rozofs_storcli_rdma_post_recv_sockpair[1]fcntl error:%s",strerror(errno));
      break;
    }
    break;
  }
  /*
  ** perform the regsitration with the socket controller
  */
  storcli_rdma_post_recv_sockctrl_p = ruc_sockctl_connect(rozofs_storcli_rdma_post_recv_sockpair[1],
                                	   "RDMA_POST_RECV",
                                	    16,
                                	    NULL,
                                	    &storcli_rdma_post_recv_callBack);
  if (storcli_rdma_post_recv_sockctrl_p== NULL)
  {
    fatal("Cannot connect with socket controller");;
    return -1;
  }
  return 0;
}

/*
**__________________________________________________________________________
*/
/**
*  Procedure to check the current state of the RDMA connection

   The purpose of that service is to return true when the RDMA connection is available
   When the RDMA connection is down, the service might attempt to retry the re-estasblish
   the RDMA connection if the context is in the ROZOFS_RDMA_ST_TCP_WAIT_RDMA_RECONNECT state
   
   The state of the connection is moved in that state when the client receives a RDMA disconnect
   event from the RDMA_CMA entity.
   
   @param lbg_idx: index of the load balancing group
   @param ref_p: when not NULL, it is the pointer to the array where the service returns the pointer to the tcp_rdma_cnx structure.
   
   @retval ref_p: pointer to the rdma_tcp context (rozofs_rdma_tcp_cnx_t structure)
*/
int storcli_lbg_is_rdma_up(int lbg_idx,uint32_t *ref_p)
{
  int status;
  rozofs_rdma_tcp_cnx_t *rdma_tcp_cnx_p;
  
  status = north_lbg_is_rdma_up(lbg_idx,ref_p);
  /*
  ** stop here if the RDMA connection is UP
  */
  if (status != 0) return status;
  /*
  ** Check if we need to restart the connection: nothing to do if there is no context
  */
  if (ref_p == NULL) return status;
  /*
  ** Check the current state of the connection: nothing to do if it is not in the ROZOFS_RDMA_ST_TCP_WAIT_RDMA_RECONNECT
  ** state
  */
   rdma_tcp_cnx_p = rozofs_rdma_get_tcp_connection_context_from_af_unix_id(*ref_p,0);
   if (rdma_tcp_cnx_p == NULL)
   {
     /*
     ** no TCP context, should revert to normal TCP
     */
     return status;
   }
  if (rdma_tcp_cnx_p->state != ROZOFS_RDMA_ST_TCP_WAIT_RDMA_RECONNECT) return status;
  /*
  ** attempt to re-establish the connection
  */
  rozofs_rdma_tcp_cli_reconnect(rdma_tcp_cnx_p);
  return status;
}
/*
**__________________________________________________________________________
*/
/**
*   That function is intenede to be called when sending SP_READ_RDMA to compute the
    first byte offset where returned data must be copy
    
    the computation is only done one the first call, then the function returns the
    computation result that it has stored in storage_bin_read_first_bin_to_write after
    the fisrt call
    
    @param none
    
    @retval offset when data must be copy
*/
int storage_get_position_of_first_byte2write_from_read_req()

{
   int position;
   if (storage_bin_read_first_bin_to_write != 0) return storage_bin_read_first_bin_to_write;
   {
      /*
      ** now get the current position in the buffer for loading the first byte of the bins 
      */  
      position =  sizeof(uint32_t); /* length header of the rpc message */
      position += rozofs_rpc_get_min_rpc_reply_hdr_len();
      position += sizeof(uint32_t);   /* length of the storage status field */
      position += (3*sizeof(uint32_t));   /* length of the alignment field (FDL) */
      position += sizeof(uint32_t);   /* length of the bins len field */

      storage_bin_read_first_bin_to_write = position;
    }
    return storage_bin_read_first_bin_to_write;
}


void rozofs_storcli_read_req_processing_cbk(void *this,void *param);
void rozofs_storcli_write_req_processing_cbk(void *this,void *param);
int rozofs_storcli_get_position_of_first_byte2write();

/**
**_____________________________________________________________________________
**    R D M A   S E C T I O N 
**_____________________________________________________________________________
*/


typedef struct _rdma_buf_tmo_entry_t
{
   list_t list;
   void *rdma_buf_ref;  /**< reference of the RDMA buffer   */
   uint64_t timestamp;  /**< timestamp that corresponds to the introduction of the buffer in the list  */
   uint32_t xid;        /**< xid of the initial transaction */
} rdma_buf_tmo_entry_t;


int rozofs_rdma_tmo_table_init_done=0;
uint32_t rozofs_rdma_lbg_context_count=0;                              /**< number of context to handle                                               */
int rozofs_rdma_tmo_buffer_count;                                      /**< number of buffer that are waiting for out of seq or TCP disconnect        */
rozofs_rdma_cli_lbg_stats_t *rozofs_rdma_cli_lbg_stats_table_p=NULL;   /**< pointer to the allocated array that contains the number of buffer blocked */
list_t   *rozofs_rdma_tmo_lbg_head_table_p=NULL;                       /**< head of the list for each potential lbg   */
rozofs_rmda_cli_stats_t rozofs_rdma_cli_stats;                         /**< glabal RDMA client statistics    */
ruc_obj_desc_t *rozofs_rdma_tx_lbg_in_progress_p = NULL ;             /**< linked list of the pending transaction on RDMA */
int rozofs_rdma_tx_count = 0;

/*
**__________________________________________________________________________
*/
/**
* Insert a transaction in the pending list that corresponds to a load balancing group

  @param lbg_id: reference of the load balancing group
  @param tx_p: pointer to the transaction context
  
  @retval none
*/
void rozofs_rdma_insert_pending_tx_on_lbg(uint32_t lbg_id,void *tx_p)
{

   ruc_obj_desc_t *phead;
   if (lbg_id >= rozofs_rdma_lbg_context_count)
   {
     severe("lbg_id is out of range :%u (max: %d)",lbg_id,rozofs_rdma_lbg_context_count);
   }
   phead = &rozofs_rdma_tx_lbg_in_progress_p[lbg_id];
   ruc_objRemove((ruc_obj_desc_t*)tx_p);
   ruc_objInsertTail((ruc_obj_desc_t*)phead,(ruc_obj_desc_t*)tx_p);  
}
/*
**__________________________________________________________________________
*/
/**
*  Purge all the pending transaction upon a QP error that triggers the teardown 
   of the RDMA connection
   
   @param lbg_id: reference of the associated load balancing group
   
   @retval none
*/
void rozofs_rdma_purge_pending_tx_on_lbg(uint32_t lbg_id)
{
   rozofs_tx_ctx_t *tx_p;
   ruc_obj_desc_t *phead;
   ruc_obj_desc_t *pnext=NULL;
   if (lbg_id >= rozofs_rdma_lbg_context_count)
   {
     severe("lbg_id is out of range :%u (max: %d)",lbg_id,rozofs_rdma_lbg_context_count);
   }
   phead = &rozofs_rdma_tx_lbg_in_progress_p[lbg_id];
   while ((tx_p = (rozofs_tx_ctx_t*) ruc_objGetNext((ruc_obj_desc_t*)phead,
                                        &pnext))
               !=NULL) 
   {         
     ruc_objRemove((ruc_obj_desc_t*)tx_p);
     if (tx_p->xmit_buf == NULL)
     {
        severe("pending transaction without xmit_buf");
	continue;
     }
     rozofs_tx_xmit_abort_rpc_cbk(NULL,lbg_id,tx_p->xmit_buf,ECONNRESET);
   }
}

/*
**__________________________________________________________________________
*/
/**
*  Callback associated with Send a RPC request over RDMA


   @param user_param: reference of the RPC buffer used for RDMA
   @param status: 0 if the RDMA transfer is OK
   @param error: RDMA error when status is not 0.
   
   @retval none

*/
void rozofs_storcli_rdma_post_send_rpc_request_cbk(void *user_param,int status, int error)   
{
  rozofs_rdma_tcp_assoc_t *assoc_p;
  rozofs_storcli_rdma_post_send_stat_t *stat_post_send_p;  
  rozofs_rdma_cli_lbg_stats_t *lbg_stats_p;
//#warning debug
//   info("FDL  rozofs_storcli_rdma_post_send_rpc_request_cbk %x", user_param);
  /*
  ** The buffer contains at the top the association block followed by the RPC message
  */
  assoc_p = (rozofs_rdma_tcp_assoc_t*)ruc_buf_getPayload(user_param);
  stat_post_send_p = (rozofs_storcli_rdma_post_send_stat_t*)(assoc_p+1);
  
  lbg_stats_p = &rozofs_rdma_cli_lbg_stats_table_p[stat_post_send_p->lbg_id];
  /*
  ** update statistics
  */
  lbg_stats_p->write_stats.attempts++;
  if (status < 0)
  {
     if (stat_post_send_p->read) lbg_stats_p->read_stats.rdma_post_send_nok++;
     else lbg_stats_p->write_stats.rdma_post_send_nok++;
     /*
     ** force the shutdown of the RDMA connection.
     */
     rozofs_rdma_on_completion_check_status_of_rdma_connection(status,-1,assoc_p);
  }
  /*
  ** Release the xmit buffer
  */
  rozofs_rdma_release_rpc_buffer(user_param);
}
/*
**__________________________________________________________________________
*/
/**
*  Send a RPC request over RDMA

   @param: xmit_buf: ruc_buffer that contains the encoded RPC message
   @param read: assert to 1 for read and 0 for write
   @param lbg_id : useless
   @param assoc_p: pointer to the association block that contains the client & server TCP connection index
   @param tx_p: pointer to the transaction context (needed to queue it in the pending transaction list of the lbg)
 
   @retval 0 on success
   @retval -1 on error  
*/
int rozofs_storcli_rdma_post_send_rpc_request(uint32_t lbg_id,int read,void *xmit_buf,rozofs_rdma_tcp_assoc_t *assoc_p,void *tx_p)
{
   uint8_t *src_p;
   int len;
   uint8_t *dst_p;
   void *rdma_rpc_buf;
   int ret;
   rozofs_wr_id2_t wr_th;
   rozofs_wr_id2_t *wr_th_p;
   rozofs_storcli_rdma_post_send_stat_t stat_post_send;
   
   stat_post_send.lbg_id = lbg_id;
   stat_post_send.read = read;
   /*
   ** need to allocate a buffer from the rpc pool registered with the RDMA adaptor
   */
   rdma_rpc_buf = rozofs_rdma_allocate_rpc_buffer();
   if (rdma_rpc_buf  == NULL)
   {
     severe("Out of RDMA RPC buffer");
     errno = ENOMEM;
     return -1;
   }
   src_p = (uint8_t *)ruc_buf_getPayload(xmit_buf);
   dst_p = (uint8_t *)ruc_buf_getPayload(rdma_rpc_buf);
   /*
   ** Copy the association block at the top of the RPC buffer
   */
   memcpy(dst_p,assoc_p,sizeof(rozofs_rdma_tcp_assoc_t));
   dst_p +=sizeof(rozofs_rdma_tcp_assoc_t);
   /*
   ** save the reference of the lbg and operation code
   */
   memcpy(dst_p,&stat_post_send,sizeof(stat_post_send));
   dst_p +=sizeof(stat_post_send);
   /*
   ** Put the end of transmission callback in the ruc buffer as well as the reference of the ruc buffer
   */
   wr_th.cqe_cbk = rozofs_storcli_rdma_post_send_rpc_request_cbk;
   wr_th.user_param = rdma_rpc_buf;   
   memcpy(dst_p,&wr_th,sizeof(wr_th));
   wr_th_p = (rozofs_wr_id2_t*) dst_p;
   dst_p +=sizeof(wr_th);
      
   len = ruc_buf_getPayloadLen(xmit_buf);
   /*
   ** Copy the rpc message
   */
   memcpy(dst_p,src_p,len);
   ruc_buf_setPayloadLen(rdma_rpc_buf,len+sizeof(rozofs_rdma_tcp_assoc_t)+sizeof(stat_post_send)+sizeof(wr_th_p));
   

   //info ("FDL Write dst_p %p len %u wr_th_p %p",dst_p,len,wr_th_p);
   ret = rozofs_rdma_post_send_ibv_wr_send(wr_th_p,assoc_p->cli_ref,rdma_rpc_buf,dst_p,len);
   if (ret < 0)
   {
     /*
     ** release the allocated RPC RDMA buffer
     */
     rozofs_rdma_release_rpc_buffer(rdma_rpc_buf);
   }
   else
   {
     rozofs_rdma_insert_pending_tx_on_lbg(lbg_id,tx_p);
   }
   return ret;
}


/**
*  SHow the buffer that are blocked becauce of lack or response from storio
*/
void show_rdma_buf_tmo(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int i;
    rozofs_rdma_cli_lbg_stats_t *lbg_stats_p;
        
    if (rozofs_rdma_tmo_table_init_done == 0)
    {
      sprintf(pChar,"The table has not been initialized\n");
      goto out;
    }
    pChar += sprintf(pChar,"Total number of buffer blocked : %u\n",rozofs_rdma_tmo_buffer_count);
    /*
    ** display per lbg the number of buffer that are blocked
    */
    
    for (i = 0; i < rozofs_rdma_lbg_context_count; i++)
    {
       lbg_stats_p = &rozofs_rdma_cli_lbg_stats_table_p[i];
       if (lbg_stats_p->rdma_tmo_buffer_count != 0)
       {
         pChar +=sprintf(pChar,"LBG #%3d : %d\n",i,lbg_stats_p->rdma_tmo_buffer_count );
    
       }    
    }
    sprintf(pChar,"\n");

out:    
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}

/*
**__________________________________________________________________________
*/



#define SHOW_RDMA_CLI_READ2(probe) pChar += sprintf(pChar," %12llu |",\
                    (unsigned long long int)lbg_stats_p->read_stats.probe);
		    
#define SHOW_RDMA_CLI_WRITE2(probe) pChar += sprintf(pChar," %12llu |",\
                    (unsigned long long int)lbg_stats_p->write_stats.probe);

/**
*  SHow the buffer that are blocked becauce of lack or response from storio
*/
void show_rdma_cli_stats2(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int i;
    rozofs_rdma_cli_lbg_stats_t *lbg_stats_p;
        
    if (rozofs_rdma_tmo_table_init_done == 0)
    {
      sprintf(pChar,"The table has not been initialized\n");
      goto out;
    }
    pChar +=sprintf(pChar,"LBG |  Ope  | attempts     | no_TCP_ctx   | no_RDMA_ctx  | bad_RDMA_st  | no_RDMA_mem. |  no_buffer   |  no_tx_ctx   | lbg_send_ok  | lbg_send_nok |\n");
    pChar +=sprintf(pChar,"----+-------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+\n");
    for (i = 0; i < rozofs_rdma_lbg_context_count; i++)
    {
       lbg_stats_p = &rozofs_rdma_cli_lbg_stats_table_p[i];
       if ((lbg_stats_p->read_stats.attempts == 0) && (lbg_stats_p->write_stats.attempts == 0)) continue;
       pChar +=sprintf(pChar,"%3d |",i);
       pChar +=sprintf(pChar," Read  |");       
       SHOW_RDMA_CLI_READ2(attempts);
       SHOW_RDMA_CLI_READ2(no_TCP_context);
       SHOW_RDMA_CLI_READ2(no_RDMA_context);
       SHOW_RDMA_CLI_READ2(bad_RDMA_state);
       SHOW_RDMA_CLI_READ2(no_RDMA_memory);
       SHOW_RDMA_CLI_READ2(no_buffer);
       SHOW_RDMA_CLI_READ2(no_tx_context);
       SHOW_RDMA_CLI_READ2(lbg_send_ok);
       SHOW_RDMA_CLI_READ2(lbg_send_nok);
       SHOW_RDMA_CLI_READ2(rdma_post_send_nok);
       pChar +=sprintf(pChar,"\n    | Write |");
       SHOW_RDMA_CLI_WRITE2(attempts);
       SHOW_RDMA_CLI_WRITE2(no_TCP_context);
       SHOW_RDMA_CLI_WRITE2(no_RDMA_context);
       SHOW_RDMA_CLI_WRITE2(bad_RDMA_state);
       SHOW_RDMA_CLI_WRITE2(no_RDMA_memory);
       SHOW_RDMA_CLI_WRITE2(no_buffer);
       SHOW_RDMA_CLI_WRITE2(no_tx_context);
       SHOW_RDMA_CLI_WRITE2(lbg_send_ok);
       SHOW_RDMA_CLI_WRITE2(lbg_send_nok);
       SHOW_RDMA_CLI_WRITE2(rdma_post_send_nok);
       pChar +=sprintf(pChar,"\n----+-------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+\n");
  
    }
    sprintf(pChar,"\n");

out:    
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}

/*
**__________________________________________________________________________
*/
/**
*   Init of the table used to deal with time-out/out of sequence and TCP disconnection
    on LBG that are using RDMA
    
    @param mx_lbg_north_ctx:  number of context to handle
    
    @retval RUC_OK on success
*/
int  rdma_lbg_tmo_table_init(uint32_t mx_lbg_north_ctx)
{

  list_t *p;
  ruc_obj_desc_t *q;
  int i;
  
  if (rozofs_rdma_tmo_table_init_done) return RUC_OK;
  
  rozofs_rdma_lbg_context_count = mx_lbg_north_ctx;
  
  /*
  ** allocate the stats table
  */
  rozofs_rdma_cli_lbg_stats_table_p = xmalloc(mx_lbg_north_ctx*sizeof(rozofs_rdma_cli_lbg_stats_t));
  memset(rozofs_rdma_cli_lbg_stats_table_p,0,mx_lbg_north_ctx*sizeof(rozofs_rdma_cli_lbg_stats_t));
  /*
  ** allocate the head of list for each load balancing group
  */
  rozofs_rdma_tmo_lbg_head_table_p =  xmalloc(mx_lbg_north_ctx*sizeof(list_t));
  /*
  ** init of the lists
  */
  p = &rozofs_rdma_tmo_lbg_head_table_p[0];
  for (i = 0; i <rozofs_rdma_lbg_context_count; i++,p++)
  {
    list_init(p);
  }

  /*
  ** allocate memory for handling transactions in progress
  */
  rozofs_rdma_tx_lbg_in_progress_p =  xmalloc(mx_lbg_north_ctx*sizeof(ruc_obj_desc_t));
  q = &rozofs_rdma_tx_lbg_in_progress_p[0];
  for (i = 0; i <mx_lbg_north_ctx; i++,q++)
  {
    ruc_listHdrInit(q);
  }    
  rozofs_rdma_tmo_buffer_count = 0;
  rozofs_rdma_tx_count = 0;
  uma_dbg_addTopic("rdma_tmo_buf",show_rdma_buf_tmo);
  uma_dbg_addTopic("rdma_lbg_stats",show_rdma_cli_stats2);
  
  rozofs_rdma_tmo_table_init_done=1;
  return RUC_OK;

}
/*
**__________________________________________________________________________
*/
/**
*  RDMA Time-out processing

  Because the buffer provided for the RDMA cannot be directly free for future re-use since
  a delayed RDMA transfer can occur while the buffer has been allocated for another transaction and
  could trigger some data corruption, the buffer used for RDMA transfer should be locked until 
  either we got a late response from storio (TX_OUT_OF_SEQ) or a TCP disconnect event from the TCP layer
  
  So the goal of that function is :
  1) to allocate a data structure to deal with defer buffer release
  2) to increment the in_use counter of the buffer (to avoid release during 
     the release of the storcli context used for the transfer) 
  3) to save the reference of the transaction in that alllocated context: needed to deal with out of seq TX)
  4) to queue the data structure at the index that corresponds to the LBG on which it has been sent
  5) to remove the reference of the rdma buffer from the transaction context
  
  @param this: pointer to the transaction context
  
  @retval none
  
*/
void rozofs_rdma_tx_tmo_proc(void *this)
{

   rdma_buf_tmo_entry_t *tmo_ctx_p = NULL;
   uint32_t lbg_id;
   rozofs_rdma_cli_lbg_stats_t *lbg_stats_p;
   
   /*
   ** Get the LBG identifier stored in the transaction context
   */
   rozofs_tx_read_opaque_data(this,2,(uint32_t*)&lbg_id);
   
   lbg_stats_p = &rozofs_rdma_cli_lbg_stats_table_p[lbg_id];
   
   tmo_ctx_p = malloc(sizeof(rdma_buf_tmo_entry_t));
   if (tmo_ctx_p == NULL)
   {
      fatal("Out of memory");
   }
   /*
   ** clear the buffer
   */ 
   list_init(&tmo_ctx_p->list);
   
   tmo_ctx_p->rdma_buf_ref = rozofs_tx_read_rdma_bufref(this);
   if (tmo_ctx_p->rdma_buf_ref == NULL)
   {
      /*
      ** this is quite strange
      */
      severe("RDMA TMO with no RDMA buffer reference");
      goto error;
   }
   tmo_ctx_p->xid = rozofs_tx_read_xid(this);
   tmo_ctx_p->timestamp = rozofs_get_ticker_us();
   /*
   ** increment the inuse counter of the buffer
   */
   ruc_buf_inuse_increment(tmo_ctx_p->rdma_buf_ref);
   /*
   ** insert the context on the corresponding load balancing group list
   */
   list_push_back(&rozofs_rdma_tmo_lbg_head_table_p[lbg_id],&tmo_ctx_p->list);
   rozofs_rdma_tmo_buffer_count++;
   lbg_stats_p->rdma_tmo_buffer_count++;
   
   return;

error:
   if (tmo_ctx_p != NULL) free(tmo_ctx_p);
}



/*
**__________________________________________________________________________
*/
/**
*  RDMA : OUT of sequence processing this addresses the case of an out of sequence reception from TCP

  That function is called upon detecting an out of sequence transaction at the tx_engine side
  The Out of sequence might not be related to a RDMA transfer.
  
  @param lbg_id : reference of the load balancing group for which out of seq transaction is detected
  @param xid: transaction identifier found in the received buffer

  @retval : none
*/
void rozofs_rdma_tx_out_of_seq_cbk(uint32_t lbg_id,uint32_t xid)
{
    list_t *lbg_list_p;
    int inuse;
    list_t *p, *q;    
    rozofs_rdma_cli_lbg_stats_t *lbg_stats_p;
    
    if (lbg_id >=rozofs_rdma_lbg_context_count )
    {
       warning("Out of range lbg_id %u (max is %u)",lbg_id,rozofs_rdma_lbg_context_count);
       return;
    }
    lbg_list_p = &rozofs_rdma_tmo_lbg_head_table_p[lbg_id];
    lbg_stats_p = &rozofs_rdma_cli_lbg_stats_table_p[lbg_id];
    
    list_for_each_forward_safe(p,q,lbg_list_p)
    {
      rdma_buf_tmo_entry_t *tmo_ctx_p= list_entry(p, rdma_buf_tmo_entry_t, list);
      /*
      ** check if the xid matches
      */
      if (tmo_ctx_p->xid != xid) continue;
      /*
      ** remove the entry and attempt to release the buffer
      */
      list_remove(p);
      /*
      ** Decrement the inuse of the buffer and attempt to release it when inuse reaches 1
      */
      inuse = ruc_buf_inuse_decrement(tmo_ctx_p->rdma_buf_ref);      
      if (inuse == 1) 
      {
         ruc_objRemove((ruc_obj_desc_t*) tmo_ctx_p->rdma_buf_ref);
         ruc_buf_freeBuffer(tmo_ctx_p->rdma_buf_ref);
      }
      /*
      ** update stats
      */ 
      rozofs_rdma_tmo_buffer_count--;
      if (rozofs_rdma_tmo_buffer_count < 0) rozofs_rdma_tmo_buffer_count=0;
      lbg_stats_p->rdma_tmo_buffer_count--;
      if (lbg_stats_p->rdma_tmo_buffer_count < 0) lbg_stats_p->rdma_tmo_buffer_count = 0;
      /*
      ** release the context
      */       
      free(tmo_ctx_p);
      return;         
    }
}


/*
**__________________________________________________________________________
*/
/**
*  RDMA : OUT of sequence processing: this addresses the case of an out of sequence reception from RDMA

  That function is called upon detecting an out of sequence transaction at the tx_engine side
  The Out of sequence might not be related to a RDMA transfer.
  
  @param lbg_id : reference of the load balancing group for which out of seq transaction is detected
  @param xid: transaction identifier found in the received buffer

  @retval : none
*/
void rozofs_rdma_tx_out_of_seq_on_rdma_cbk(uint32_t lbg_id,uint32_t xid)
{
  return rozofs_rdma_tx_out_of_seq_cbk(lbg_id,xid);
}

/*
**__________________________________________________________________________
*/
/**
*  RDMA : TCP disconnect callback processing

  That function is called upon the disconnection of the TCP connection that is used in conjunction with RDMA
  
  @param lbg_id : reference of the load balancing group for which the TCP connection is down

  @retval : none
*/
void rozofs_rdma_tcp_connection_disc_cbk(uint32_t lbg_id)
{
    list_t *lbg_list_p;
    int inuse;
    list_t *p, *q;    
    rozofs_rdma_cli_lbg_stats_t *lbg_stats_p;

    if (lbg_id >=rozofs_rdma_lbg_context_count )
    {
       warning("Out of range lbg_id %u (max is %u)",lbg_id,rozofs_rdma_lbg_context_count);
       return;
    }
    lbg_list_p = &rozofs_rdma_tmo_lbg_head_table_p[lbg_id];
    lbg_stats_p = &rozofs_rdma_cli_lbg_stats_table_p[lbg_id];
    
    list_for_each_forward_safe(p,q,lbg_list_p)
    {
      rdma_buf_tmo_entry_t *tmo_ctx_p= list_entry(p, rdma_buf_tmo_entry_t, list);
      /*
      ** remove the entry and attempt to release the buffer
      */
      list_remove(p);
      /*
      ** Decrement the inuse of the buffer and attempt to release it when inuse reaches 1
      */
      inuse = ruc_buf_inuse_decrement(tmo_ctx_p->rdma_buf_ref);      
      if (inuse == 1) 
      {
         ruc_objRemove((ruc_obj_desc_t*) tmo_ctx_p->rdma_buf_ref);
         ruc_buf_freeBuffer(tmo_ctx_p->rdma_buf_ref);
      }
      /*
      ** update stats
      */ 
      rozofs_rdma_tmo_buffer_count--;
      if (rozofs_rdma_tmo_buffer_count < 0) rozofs_rdma_tmo_buffer_count=0;
      lbg_stats_p->rdma_tmo_buffer_count--;
      if (lbg_stats_p->rdma_tmo_buffer_count < 0) lbg_stats_p->rdma_tmo_buffer_count=0;
      /*
      ** release the context
      */ 
      free(tmo_ctx_p);
    }
    /*
    ** Go through the list of the pending transaction that have sent sent with IBV_WR_SEND
    */
    rozofs_rdma_purge_pending_tx_on_lbg(lbg_id);
}

/*
**__________________________________________________________________________
*/
/**
*  That function is the call back associated with SP_READ_RDMA

   The goal of that function is to copy the data (bins) returned by a storio at
   the revelant offset in the buffer that has been allocated for RDMA transfer.
   
   When it is done, the current received buffer is released and it is replaced
   by the buffer allocated for the RDMA transfer.
   
   When it is done, the regular non RDMA callback function rozofs_storcli_read_req_processing_cbk()
   is called. By doing so, the usage of RDMA is transparent.

   @param this: transaction context
   @param param
   
   @retval none
*/
void rozofs_storcli_read_rdma_req_processing_cbk(void *this,void *param) 
{
   void *rdma_buf_ref;
   void *recv_buf;
   int status;   
   char *src_payload;
   char *dst_payload;
   uint32_t max_size;
   int error;

   /*
   ** remove the transaction from the pending list
   */
   ruc_objRemove((ruc_obj_desc_t*)this);   
   /*
   ** Get the RDMA buffer from the transaction context
   */
   rdma_buf_ref = rozofs_tx_read_rdma_bufref(this);
   
   status = rozofs_tx_get_status(this);
   if (status < 0)
   {
       error = rozofs_tx_get_errno(this);  
       if (error == ETIME)
       {
         /*
	 ** There is a time-out on the transaction. We cannot
	 ** release the RDMA buffer right away since the RDMA transfer
	 ** could happen while the buffer has been re-allocated for another transaction
	 **
	 ** We need to keep the buffer until there is disconnection or the reception of
	 ** a response for that buffer that is received out of sequence
	 */
	 rozofs_rdma_tx_tmo_proc(this);
       }
       else
       {       
	 /*
	 ** case of TCP disc: just release the release the rdma buffer
	 */
         ruc_buf_freeBuffer(rdma_buf_ref);
       }
       /*
       ** the reference from the transaction context
       */
       rozofs_tx_clear_rdma_bufref(rdma_buf_ref);
       
     return rozofs_storcli_read_req_processing_cbk(this,param);
   }
   /*
   ** Get the received buffer
   */
    recv_buf = rozofs_tx_get_recvBuf(this);
    if (recv_buf == NULL)
    {
       /*
       ** something wrong happened
       */
       ruc_buf_freeBuffer(rdma_buf_ref);
       rozofs_tx_clear_rdma_bufref(rdma_buf_ref);
       return rozofs_storcli_read_req_processing_cbk(this,param);
    }
    /*
    ** Get the payload length of the receive buffer
    */
    dst_payload  = (char*) ruc_buf_getPayload(rdma_buf_ref);
    src_payload  = (char*) ruc_buf_getPayload(recv_buf);
    /*
    ** copy the received message at the top of the RDMA buffer
    ** and then release the received buffer
    ** The RDMA buffer becomes the received buffer for the remaining
    ** of the processing.
    */
    max_size = ruc_buf_getPayloadLen(recv_buf);  
    /*
    ** if the receive size is greater than 4KB it is a clear indication that was
    ** something wrong at RDMA level, and the data have been provided by TCP, so we just need
    ** to keep the received buffer and release the buffer allocated for RDMA transfer
    */
    if (max_size > 4096)
    {    
       ruc_buf_freeBuffer(rdma_buf_ref);
       rozofs_tx_clear_rdma_bufref(rdma_buf_ref);
       rozofs_tx_put_recvBuf(this,recv_buf);
       return rozofs_storcli_read_req_processing_cbk(this,param);            
    }

    memcpy(dst_payload,src_payload,max_size);
    /*
    ** adjust the size of the payload of the destination buffer
    */
    max_size = ruc_buf_getMaxPayloadLen(rdma_buf_ref);
    max_size -=sizeof(uint32_t);
    ruc_buf_setPayloadLen(rdma_buf_ref,max_size);
    /*
    ** Release the received buffer and push the RDMA buffer as the received buffer.
    */
    if (rozofs_is_rdma_rpc_buffer(recv_buf)) rozofs_rdma_release_rpc_buffer(recv_buf);
    else ruc_buf_freeBuffer(recv_buf);

    rozofs_tx_put_recvBuf(this,rdma_buf_ref);
    /*
    ** remove the reference of the rdma_buffer
    */
    rozofs_tx_clear_rdma_bufref(rdma_buf_ref);
    /*
    ** processing now will take place on the legacy data path
    */
    rozofs_storcli_read_req_processing_cbk(this,param);
      
}
/*
**__________________________________________________________________________
*/
/**
   SP_READ_RDMA encoding

    @param lbg_id             : reference of the load balancing group
    @param socket_context_ref : socket controller reference (index) of the TCP connection for which there is an associated RDMA connection
    @param timeout_sec        : transaction timeout
    @param prog               : program
    @param vers               : program version
    @param opcode             : metadata opcode
    @param encode_fct         : encoding function
    @msg2encode_p             : pointer to the message to encode
    @param xmit_buf           : pointer to the buffer to send, in case of error that function release the buffer
    @param seqnum             : sequence number associated with the context (store as an opaque parameter in the transaction context
    @param opaque_value_idx1  : opaque value at index 1
    @param extra_len          : extra length to add after encoding RPC (must be 4 bytes aligned !!!)
    @param recv_cbk           : receive callback function

    @param user_ctx_p : pointer to the working context
   
    @retval 0 on success;
    @retval -1 on error,, errno contains the cause
    @retval -2 if non RDMA path should be used
*/
int rozofs_sorcli_sp_read_rdma(uint32_t lbg_id,uint32_t socket_context_ref, uint32_t timeout_sec, uint32_t prog,uint32_t vers,
                              int opcode,xdrproc_t encode_fct,void *msg2encode_p,
                              void *xmit_buf,
                              uint32_t seqnum,
                              uint32_t opaque_value_idx1,  
                              int      extra_len,                            
                              sys_recv_pf_t recv_cbk,void *user_ctx_p) 
{

    uint8_t           *arg_p;
    uint32_t          *header_size_p;
    rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;
    int               bufsize;
    int               ret;
    int               position;
    XDR               xdrs;         
    struct rpc_msg   call_msg;
    uint32_t         null_val = 0;
   sp_read_rdma_arg_t request_rdma ;
   rozofs_rdma_tcp_cnx_t *tcp_cnx_p;
   rozofs_rdma_connection_t *conn;
   rozofs_rmda_ibv_cxt_t *s_ctx;
   struct ibv_mr  *mr_p;
   void *south_buf=NULL;
   rozofs_rdma_cli_lbg_stats_t *lbg_stats_p; 
   
   lbg_stats_p = &rozofs_rdma_cli_lbg_stats_table_p[lbg_id];

   memcpy(&request_rdma,msg2encode_p,sizeof(sp_read_arg_t));
   /*
   ** update the statistics
   */
   lbg_stats_p->read_stats.attempts++;   
   
   /*
   ** need to fill up the RDMA key 
   */
   /*
   ** get a TCP association context to link the AF_unix side with the RDMA/TCP side
   */
   tcp_cnx_p = rozofs_rdma_get_tcp_connection_context_from_af_unix_id(socket_context_ref,0);
   if (tcp_cnx_p == NULL)
   {
     /*
     ** no TCP context, should revert to normal TCP
     */
     lbg_stats_p->read_stats.no_TCP_context++;   
     errno = EAGAIN;
     goto retry;
   }
    /*
    **____________________________________________________________
    ** Get the RDMA context associated with the file descriptor
    **____________________________________________________________
    */
    conn = rozofs_rdma_get_connection_context_from_fd(socket_context_ref);
    if (conn ==NULL)
    {
       lbg_stats_p->read_stats.no_RDMA_context++;   
       errno = EAGAIN;
       goto retry;
    }
    /*
    ** Check if the RDMA connection is still effective
    */
    if ((conn->state != ROZOFS_RDMA_ST_ESTABLISHED) && (conn->state != ROZOFS_RDMA_ST_WAIT_ESTABLISHED))
    {
       /*
       ** RDMA is no more supported on that TCP connection
       */
       lbg_stats_p->read_stats.bad_RDMA_state++;   
       errno = EAGAIN;
       goto retry;    
    }
//    info("FDL STORCLI read TCP cli/srv:%d/%d  RDMA cli/srv :%d/%d",tcp_cnx_p->assoc.cli_ref,tcp_cnx_p->assoc.srv_ref,conn->assoc.cli_ref,conn->assoc.srv_ref);
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
      lbg_stats_p->read_stats.no_RDMA_memory++;   
      errno = EAGAIN;
      goto retry;
    }
    /*
    ** copy the key that identifies the TCP/RDMA association
    */
    memcpy(&request_rdma.rdma_key,&tcp_cnx_p->assoc,sizeof(rozofs_rdma_key_t));
    /*
    ** get the key of the memory registered with RDMA
    */
    request_rdma.rkey = mr_p->lkey;
    /*
    ** allocate a south buffer
    */
    south_buf = ruc_buf_getBuffer(ROZOFS_STORCLI_SOUTH_LARGE_POOL);   
    if (south_buf == NULL)
    {
       /*
       ** out of buffer
       */
      lbg_stats_p->read_stats.no_buffer++;   
       TX_STATS(ROZOFS_TX_NO_CTX_ERROR);
       errno = ENOMEM;
       goto error;
    } 
    {
       char *pbuf;
       pbuf = ruc_buf_getPayload(south_buf); 
       pbuf += storage_get_position_of_first_byte2write_from_read_req();
       request_rdma.remote_addr = (uint64_t) pbuf;
    
    }        
    /*
    ** allocate a transaction context
    */
    rozofs_tx_ctx_p = rozofs_tx_alloc();  
    if (rozofs_tx_ctx_p == NULL) 
    {
       /*
       ** out of context
       ** --> put a pending list for the future to avoid repluing ENOMEM
       */
       lbg_stats_p->read_stats.no_tx_context++;   
       ruc_buf_freeBuffer(south_buf);
       south_buf= NULL;
       TX_STATS(ROZOFS_TX_NO_CTX_ERROR);
       errno = ENOMEM;
       goto error;
    }  
    /*
    ** store the RDMA buffer in the transaction context
    */
    rozofs_tx_set_rdma_bufref( rozofs_tx_ctx_p,south_buf);
    /*
    ** store the reference of the xmit buffer in the transaction context: might be useful
    ** in case we want to remove it from a transmit list of the underlying network stacks
    */
    rozofs_tx_save_xmitBuf(rozofs_tx_ctx_p,xmit_buf);
    /*
    ** get the pointer to the payload of the buffer
    */
    header_size_p  = (uint32_t*) ruc_buf_getPayload(xmit_buf);
    arg_p = (uint8_t*)(header_size_p+1);  
    /*
    ** create the xdr_mem structure for encoding the message
    */
    bufsize = (int)ruc_buf_getMaxPayloadLen(xmit_buf);
    bufsize -= sizeof(uint32_t); /* skip length*/
    xdrmem_create(&xdrs,(char*)arg_p,bufsize,XDR_ENCODE);
    /*
    ** fill in the rpc header
    */
    call_msg.rm_direction = CALL;
    /*
    ** allocate a xid for the transaction 
    */
	call_msg.rm_xid             = rozofs_tx_alloc_xid(rozofs_tx_ctx_p); 
	call_msg.rm_call.cb_rpcvers = RPC_MSG_VERSION;
	/* XXX: prog and vers have been long historically :-( */
	call_msg.rm_call.cb_prog = (uint32_t)prog;
	call_msg.rm_call.cb_vers = (uint32_t)vers;
	if (! xdr_callhdr(&xdrs, &call_msg))
    {
       /*
       ** THIS MUST NOT HAPPEN
       */
       TX_STATS(ROZOFS_TX_ENCODING_ERROR);
       errno = EPROTO;
       goto error;	
    }
    /*
    ** insert the procedure number, NULL credential and verifier
    */
    XDR_PUTINT32(&xdrs, (int32_t *)&opcode);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
    /*
    ** ok now call the procedure to encode the message
    */
    if ((*encode_fct)(&xdrs,&request_rdma) == FALSE)
    {
       TX_STATS(ROZOFS_TX_ENCODING_ERROR);
       errno = EPROTO;
       goto error;
    }
    /*
    ** Set the reference of the lbg in the RPC message
    */
    rozofs_rpc_set_lbg_id_in_request(xmit_buf,lbg_id);   
    /*
    ** Now get the current length and fill the header of the message
    */
    position = XDR_GETPOS(&xdrs);
    /*
    ** add the extra_len if any
    */
    position +=extra_len;
    /*
    ** update the length of the message : must be in network order
    */
    *header_size_p = htonl(0x80000000 | position);
    /*
    ** set the payload length in the xmit buffer
    */
    int total_len = sizeof(*header_size_p)+ position;
    ruc_buf_setPayloadLen(xmit_buf,total_len);
    /*
    ** store the receive call back and its associated parameter
    */
    rozofs_tx_ctx_p->recv_cbk   = recv_cbk;
    rozofs_tx_ctx_p->user_param = user_ctx_p;    
    /*
    ** store the sequence number in one of the opaque user data array of the transaction
    */
    rozofs_tx_write_opaque_data( rozofs_tx_ctx_p,0,seqnum);  
    rozofs_tx_write_opaque_data( rozofs_tx_ctx_p,1,opaque_value_idx1);  
    rozofs_tx_write_opaque_data( rozofs_tx_ctx_p,2,lbg_id);  

    /*
    ** Check the case of the full RDMA
    */
    {
      sp_read_arg_t *request = (sp_read_arg_t *)msg2encode_p;
      rozofs_storcli_trace_request(user_ctx_p, opaque_value_idx1, request->sid,rozofs_storcli_trc_req_mode_rdma);
    }
    if (common_config.rdma_full)
    {
        ret =rozofs_storcli_rdma_post_send_rpc_request(lbg_id,1,xmit_buf,&tcp_cnx_p->assoc,rozofs_tx_ctx_p);
    }
    else
    {
      /*
      ** now send the message on TCP
      */
      ret = north_lbg_send_with_shaping(lbg_id,xmit_buf,0,0);
    }
    if (ret < 0)
    {
       lbg_stats_p->read_stats.lbg_send_nok++;   
       TX_STATS(ROZOFS_TX_SEND_ERROR);
       errno = EFAULT;
      goto error;  
    }
    lbg_stats_p->read_stats.lbg_send_ok++;   
    TX_STATS(ROZOFS_TX_SEND);

    /*
    ** OK, so now finish by starting the guard timer
    */
    rozofs_tx_start_timer(rozofs_tx_ctx_p,timeout_sec);  
    return 0;  
    
  error:
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);
    if (south_buf != NULL) ruc_buf_freeBuffer(south_buf);
    return -1;       

   retry:
     return -2;

}


/*
**__________________________________________________________________________
*/
/**
*  That function is the call back associated with SP_WRITE_RDMA

   The goal of that function is to copy the data (bins) returned by a storio at
   the revelant offset in the buffer that has been allocated for RDMA transfer.
   
   When it is done, the current received buffer is released and it is replaced
   by the buffer allocated for the RDMA transfer.
   
   When it is done, the regular non RDMA callback function rozofs_storcli_read_req_processing_cbk()
   is called. By doing so, the usage of RDMA is transparent.

   @param this: transaction context
   @param param
   
   @retval none
*/
void rozofs_storcli_write_rdma_req_processing_cbk(void *this,void *param) 
{
   void *rdma_buf_ref;
   int status;   
   int error;
   
   /*
   ** remove the transaction from the pending list
   */
   ruc_objRemove((ruc_obj_desc_t*)this);
   
   /*
   ** Get the RDMA buffer from the transaction context
   */
   rdma_buf_ref = rozofs_tx_read_rdma_bufref(this);
   /*
   ** decrement the in_use counter of the RDMA buffer which is the projection buffer reference within the storcli context
   ** It might be incremented again in case of TMO
   */
   ruc_buf_inuse_decrement(rdma_buf_ref);
      
   status = rozofs_tx_get_status(this);
   if (status < 0)
   {
       error = rozofs_tx_get_errno(this);  
       if (error == ETIME)
       {
         /*
	 ** There is a time-out on the transaction. We cannot
	 ** release the RDMA buffer right away since the RDMA transfer
	 ** could happen while the buffer has been re-allocated for another transaction
	 **
	 ** We need to keep the buffer until there is disconnection or the reception of
	 ** a response for that buffer that is received out of sequence
	 */
	 rozofs_rdma_tx_tmo_proc(this);
       }
       /*
       ** the reference from the transaction context
       */
       warning("FDL RDMA error code %d",error);
       rozofs_tx_clear_rdma_bufref(rdma_buf_ref);
       
     return rozofs_storcli_write_req_processing_cbk(this,param);
   }
#if 0
   /*
   ** Get the received buffer
   */
    recv_buf = rozofs_tx_get_recvBuf(this);
    if (recv_buf == NULL)
    {
       /*
       ** something wrong happened: just clear the rdma reference from the
       ** transaction context: the buffer must not be release since the
       ** write process might attempt to use it on another lbg
       */
       warning("FDL RDMA no receive buffer");

       rozofs_tx_clear_rdma_bufref(rdma_buf_ref);
       return rozofs_storcli_write_req_processing_cbk(this,param);
    }
#endif
    /*
    ** normal case : just remove the reference of the rdma_buffer
    */
    rozofs_tx_clear_rdma_bufref(rdma_buf_ref);
    /*
    ** processing now will take place on the legacy data path
    */
    rozofs_storcli_write_req_processing_cbk(this,param);     
}

/*
**__________________________________________________________________________
*/
/**
   SP_WRITE_RDMA encoding

    @param lbg_id             : reference of the load balancing group
    @param socket_context_ref : socket controller reference (index) of the TCP connection for which there is an associated RDMA connection
    @param timeout_sec        : transaction timeout
    @param prog               : program
    @param vers               : program version
    @param opcode             : metadata opcode
    @param encode_fct         : encoding function
    @msg2encode_p             : pointer to the message to encode
    @param prj_buf            : pointer to the buffer that contains the projection
    @param seqnum             : sequence number associated with the context (store as an opaque parameter in the transaction context
    @param opaque_value_idx1  : opaque value at index 1
    @param extra_len          : extra length to add after encoding RPC (must be 4 bytes aligned !!!)
    @param recv_cbk           : receive callback function

    @param user_ctx_p : pointer to the working context
   
    @retval 0 on success;
    @retval -1 on error,, errno contains the cause
    @retval -2 if non RDMA path should be used
*/
int rozofs_sorcli_sp_write_rdma(uint32_t lbg_id,uint32_t socket_context_ref, uint32_t timeout_sec, uint32_t prog,uint32_t vers,
                              int opcode,xdrproc_t encode_fct,void *msg2encode_p,
                              void *proj_buf,
                              uint32_t seqnum,
                              uint32_t opaque_value_idx1,  
                              int      extra_len,                            
                              sys_recv_pf_t recv_cbk,void *user_ctx_p) 
{

    uint8_t           *arg_p;
    uint32_t          *header_size_p;
    rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;
    int               bufsize;
    int               ret;
    int               position;
    XDR               xdrs;         
    struct rpc_msg   call_msg;
    uint32_t         null_val = 0;
   sp_write_rdma_arg_t request_rdma ;
   rozofs_rdma_tcp_cnx_t *tcp_cnx_p;
   rozofs_rdma_connection_t *conn;
   rozofs_rmda_ibv_cxt_t *s_ctx;
   struct ibv_mr  *mr_p;
   void *xmit_buf=NULL;
   rozofs_rdma_cli_lbg_stats_t *lbg_stats_p; 
   
   lbg_stats_p = &rozofs_rdma_cli_lbg_stats_table_p[lbg_id];
   
   memcpy(&request_rdma,msg2encode_p,sizeof(sp_write_arg_no_bins_t));
   /*
   ** update statistics
   */
   lbg_stats_p->write_stats.attempts++;
   /*
   ** need to fill up the RDMA key 
   */
   /*
   ** get a TCP association context to link the AF_unix side with the RDMA/TCP side
   */
   tcp_cnx_p = rozofs_rdma_get_tcp_connection_context_from_af_unix_id(socket_context_ref,0);
   if (tcp_cnx_p == NULL)
   {
     /*
     ** no TCP context, should revert to normal TCP
     */
     lbg_stats_p->write_stats.no_TCP_context++;
     errno = EAGAIN;
     goto retry;
   }
    /*
    **____________________________________________________________
    ** Get the RDMA context associated with the file descriptor
    **____________________________________________________________
    */
    conn = rozofs_rdma_get_connection_context_from_fd(socket_context_ref);
    if (conn ==NULL)
    {
       lbg_stats_p->write_stats.no_RDMA_context++;
       errno = EAGAIN;
       goto retry;
    }
    /*
    ** Check if the RDMA connection is still effective
    */
    if ((conn->state != ROZOFS_RDMA_ST_ESTABLISHED) && (conn->state != ROZOFS_RDMA_ST_WAIT_ESTABLISHED))
    {
       /*
       ** RDMA is no more supported on that TCP connection
       */
       lbg_stats_p->write_stats.bad_RDMA_state++;
       errno = EAGAIN;
       goto retry;    
    }
//    info("FDL STORCLI write TCP cli/srv:%d/%d  RDMA cli/srv :%d/%d",tcp_cnx_p->assoc.cli_ref,tcp_cnx_p->assoc.srv_ref,conn->assoc.cli_ref,conn->assoc.srv_ref);
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
      lbg_stats_p->write_stats.no_RDMA_memory++;
      errno = EAGAIN;
      goto retry;
    }
    /*
    ** copy the key that identifies the TCP/RDMA association
    */
    memcpy(&request_rdma.rdma_key,&tcp_cnx_p->assoc,sizeof(rozofs_rdma_key_t));
    /*
    ** get the key of the memory registered with RDMA
    */
    request_rdma.rkey = mr_p->lkey;
    /*
    ** allocate a south buffer (small prefer)
    */
    xmit_buf = rozofs_storcli_any_south_buffer_allocate();
    if (xmit_buf == NULL)
    {
       /*
       ** out of buffer
       */
       lbg_stats_p->write_stats.no_buffer++;
       TX_STATS(ROZOFS_TX_NO_CTX_ERROR);
       errno = ENOMEM;
       goto error;
    } 
    /*
    ** new to get the offset of the projection data in the proj_buf buffer
    */
    {
       char *pbuf;
       pbuf = ruc_buf_getPayload(proj_buf); 
       pbuf += rozofs_storcli_get_position_of_first_byte2write();
       request_rdma.remote_addr = (uint64_t) pbuf;    
       request_rdma.remote_len = (uint32_t) extra_len;
    }    
    /*
    ** allocate a transaction context
    */
    rozofs_tx_ctx_p = rozofs_tx_alloc();  
    if (rozofs_tx_ctx_p == NULL) 
    {
       /*
       ** out of context
       ** --> put a pending list for the future to avoid repluing ENOMEM
       */
       lbg_stats_p->write_stats.no_tx_context++;
       ruc_buf_freeBuffer(xmit_buf);
       xmit_buf= NULL;
       TX_STATS(ROZOFS_TX_NO_CTX_ERROR);
       errno = ENOMEM;
       goto error;
    }  
    /*
    ** store the RDMA buffer in the transaction context
    */
    rozofs_tx_set_rdma_bufref( rozofs_tx_ctx_p,proj_buf);
    /*
    ** increment the in_use counter since the storcli context can be release while there is
    ** a transaction that has been engaged with that buffer
    */
    ruc_buf_inuse_increment(proj_buf);
    /*
    ** store the reference of the xmit buffer in the transaction context: might be useful
    ** in case we want to remove it from a transmit list of the underlying network stacks
    */
    rozofs_tx_save_xmitBuf(rozofs_tx_ctx_p,xmit_buf);
    /*
    ** get the pointer to the payload of the buffer
    */
    header_size_p  = (uint32_t*) ruc_buf_getPayload(xmit_buf);
    arg_p = (uint8_t*)(header_size_p+1);  
    /*
    ** create the xdr_mem structure for encoding the message
    */
    bufsize = (int)ruc_buf_getMaxPayloadLen(xmit_buf);
    bufsize -= sizeof(uint32_t); /* skip length*/
    xdrmem_create(&xdrs,(char*)arg_p,bufsize,XDR_ENCODE);
    /*
    ** fill in the rpc header
    */
    call_msg.rm_direction = CALL;
    /*
    ** allocate a xid for the transaction 
    */
	call_msg.rm_xid             = rozofs_tx_alloc_xid(rozofs_tx_ctx_p); 
	call_msg.rm_call.cb_rpcvers = RPC_MSG_VERSION;
	/* XXX: prog and vers have been long historically :-( */
	call_msg.rm_call.cb_prog = (uint32_t)prog;
	call_msg.rm_call.cb_vers = (uint32_t)vers;
	if (! xdr_callhdr(&xdrs, &call_msg))
    {
       /*
       ** THIS MUST NOT HAPPEN
       */
       TX_STATS(ROZOFS_TX_ENCODING_ERROR);
       errno = EPROTO;
       goto error;	
    }
    /*
    ** insert the procedure number, NULL credential and verifier
    */
    XDR_PUTINT32(&xdrs, (int32_t *)&opcode);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
        
    /*
    ** ok now call the procedure to encode the message
    */
    if ((*encode_fct)(&xdrs,&request_rdma) == FALSE)
    {
       TX_STATS(ROZOFS_TX_ENCODING_ERROR);
       errno = EPROTO;
       goto error;
    }
    /*
    ** Set the reference of the lbg in the RPC message
    */
    rozofs_rpc_set_lbg_id_in_request(xmit_buf,lbg_id);   
    /*
    ** Now get the current length and fill the header of the message
    */
    position = XDR_GETPOS(&xdrs);
    /*
    ** add the extra_len if any
    */
//    position +=extra_len;
    /*
    ** update the length of the message : must be in network order
    */
    *header_size_p = htonl(0x80000000 | position);
    /*
    ** set the payload length in the xmit buffer
    */
    int total_len = sizeof(*header_size_p)+ position;
    ruc_buf_setPayloadLen(xmit_buf,total_len);
    /*
    ** store the receive call back and its associated parameter
    */
    rozofs_tx_ctx_p->recv_cbk   = recv_cbk;
    rozofs_tx_ctx_p->user_param = user_ctx_p;    
    /*
    ** store the sequence number in one of the opaque user data array of the transaction
    */
    rozofs_tx_write_opaque_data( rozofs_tx_ctx_p,0,seqnum);  
    rozofs_tx_write_opaque_data( rozofs_tx_ctx_p,1,opaque_value_idx1);  
    rozofs_tx_write_opaque_data( rozofs_tx_ctx_p,2,lbg_id);  
    /*
    ** Check the case of the full RDMA
    */
    {
      sp_read_arg_t *request = (sp_read_arg_t *)msg2encode_p;
      rozofs_storcli_trace_request(user_ctx_p, opaque_value_idx1, request->sid,rozofs_storcli_trc_req_mode_rdma);
    }
    
    if (common_config.rdma_full)
    {
        ret =rozofs_storcli_rdma_post_send_rpc_request(lbg_id,0,xmit_buf,&tcp_cnx_p->assoc,rozofs_tx_ctx_p);
    }
    else
    {
       /*
       **  send the RDMA write request over TCP (IP or IPoIB)
       */
       ret = north_lbg_send(lbg_id,xmit_buf);
    }
    if (ret < 0)
    {
       lbg_stats_p->write_stats.lbg_send_nok++;
       TX_STATS(ROZOFS_TX_SEND_ERROR);
       errno = EFAULT;
      goto error;  
    }
    lbg_stats_p->write_stats.lbg_send_ok++;
    TX_STATS(ROZOFS_TX_SEND);

    /*
    ** OK, so now finish by starting the guard timer
    */
    rozofs_tx_start_timer(rozofs_tx_ctx_p,timeout_sec);  
    return 0;  
    
  error:
    if (rozofs_tx_ctx_p != NULL) 
    {
       /*
       ** decrement the in_use of the projection buffer since the transaction is going to be aborted
       */
       ruc_buf_inuse_decrement(proj_buf);      
       rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);
    }
    /*
    ** need to check with the tx module: since the buffer is also refernce in it
    */
    if (xmit_buf != NULL) ruc_buf_freeBuffer(xmit_buf);
    return -1;       

   retry:
     return -2;

}

/**
* API for creation a transaction towards an exportd

 The reference of the north load balancing is extracted for the client structure
 fuse_ctx_p:
 That API needs the pointer to the current fuse context. That nformation will be
 saved in the transaction context as userParam. It is intended to be used later when
 the client gets the response from the server
 encoding function;
 For making that API generic, the caller is intended to provide the function that
 will encode the message in XDR format. The source message that is encoded is 
 supposed to be pointed by msg2encode_p.
 Since the service is non-blocking, the caller MUST provide the callback function 
 that will be used for decoding the message
 

 @param lbg_id     : reference of the load balancing group
 @param timeout_sec : transaction timeout
 @param prog       : program
 @param vers       : program version
 @param opcode     : metadata opcode
 @param encode_fct : encoding function
 @msg2encode_p     : pointer to the message to encode
 @param xmit_buf : pointer to the buffer to send, in case of error that function release the buffer
 @param socket_context_ref     : reference of the AF_UNIX context
 @param opaque_value_idx1 : opaque value at index 1
 @param extra_len  : extra length to add after encoding RPC (must be 4 bytes aligned !!!)
 @param recv_cbk   : receive callback function

 @param user_ctx_p : pointer to the working context
 
 @retval 0 on success;
 @retval -1 on error,, errno contains the cause
 */

int rozofs_rdma_send_rq(uint32_t lbg_id,uint32_t timeout_sec, uint32_t prog,uint32_t vers,
                              int opcode,xdrproc_t encode_fct,void *msg2encode_p,
                              void *xmit_buf,
                              uint32_t socket_context_ref,
                              uint32_t opaque_value_idx1,  
                              int      extra_len,                            
                              sys_recv_pf_t recv_cbk,void *user_ctx_p) 
{
    
    uint8_t           *arg_p;
    uint32_t          *header_size_p;
    rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;
    int               bufsize;
    int               ret;
    int               position;
    XDR               xdrs;    
    struct rpc_msg   call_msg;
    uint32_t         null_val = 0;


    /*
    ** allocate a transaction context
    */
    rozofs_tx_ctx_p = rozofs_tx_alloc();  
    if (rozofs_tx_ctx_p == NULL) 
    {
       /*
       ** out of context
       ** --> put a pending list for the future to avoid repluing ENOMEM
       */
       TX_STATS(ROZOFS_TX_NO_CTX_ERROR);
       errno = ENOMEM;
       goto error;
    }    
    /*
    ** store the reference of the xmit buffer in the transaction context: might be useful
    ** in case we want to remove it from a transmit list of the underlying network stacks
    */
    rozofs_tx_save_xmitBuf(rozofs_tx_ctx_p,xmit_buf);
    /*
    ** get the pointer to the payload of the buffer
    */
    header_size_p  = (uint32_t*) ruc_buf_getPayload(xmit_buf);
    arg_p = (uint8_t*)(header_size_p+1);  
    /*
    ** create the xdr_mem structure for encoding the message
    */
    bufsize = (int)ruc_buf_getMaxPayloadLen(xmit_buf);
    bufsize -= sizeof(uint32_t); /* skip length*/
    xdrmem_create(&xdrs,(char*)arg_p,bufsize,XDR_ENCODE);
    /*
    ** fill in the rpc header
    */
    call_msg.rm_direction = CALL;
    /*
    ** allocate a xid for the transaction 
    */
	call_msg.rm_xid             = rozofs_tx_alloc_xid(rozofs_tx_ctx_p); 
	call_msg.rm_call.cb_rpcvers = RPC_MSG_VERSION;
	/* XXX: prog and vers have been long historically :-( */
	call_msg.rm_call.cb_prog = (uint32_t)prog;
	call_msg.rm_call.cb_vers = (uint32_t)vers;
	if (! xdr_callhdr(&xdrs, &call_msg))
    {
       /*
       ** THIS MUST NOT HAPPEN
       */
       TX_STATS(ROZOFS_TX_ENCODING_ERROR);
       errno = EPROTO;
       goto error;	
    }
    /*
    ** insert the procedure number, NULL credential and verifier
    */
    XDR_PUTINT32(&xdrs, (int32_t *)&opcode);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
        
    /*
    ** ok now call the procedure to encode the message
    */
    if ((*encode_fct)(&xdrs,msg2encode_p) == FALSE)
    {
       TX_STATS(ROZOFS_TX_ENCODING_ERROR);
       errno = EPROTO;
       goto error;
    }
    /*
    ** Set the reference of the lbg in the RPC message
    */
    rozofs_rpc_set_lbg_id_in_request(xmit_buf,lbg_id);   
    /*
    ** Now get the current length and fill the header of the message
    */
    position = XDR_GETPOS(&xdrs);
    /*
    ** add the extra_len if any
    */
    position +=extra_len;
    /*
    ** update the length of the message : must be in network order
    */
    *header_size_p = htonl(0x80000000 | position);
    /*
    ** set the payload length in the xmit buffer
    */
    int total_len = sizeof(*header_size_p)+ position;
    ruc_buf_setPayloadLen(xmit_buf,total_len);
    /*
    ** store the receive call back and its associated parameter
    */
    rozofs_tx_ctx_p->recv_cbk   = recv_cbk;
    rozofs_tx_ctx_p->user_param = user_ctx_p;    
    /*
    ** store the sequence number in one of the opaque user data array of the transaction
    */
    rozofs_tx_write_opaque_data( rozofs_tx_ctx_p,0,socket_context_ref);  
    rozofs_tx_write_opaque_data( rozofs_tx_ctx_p,1,opaque_value_idx1);  
    rozofs_tx_write_opaque_data( rozofs_tx_ctx_p,2,lbg_id);  
    /*
    ** now send the message
    */
    ret = north_lbg_send(lbg_id,xmit_buf);
    if (ret < 0)
    {
       TX_STATS(ROZOFS_TX_SEND_ERROR);
       errno = EFAULT;
      goto error;  
    }
    TX_STATS(ROZOFS_TX_SEND);

    /*
    ** OK, so now finish by starting the guard timer
    */
    rozofs_tx_start_timer(rozofs_tx_ctx_p,timeout_sec);  
    return 0;      
  error:
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);
    return -1;    
}

/*
**__________________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */

void rozofs_rdma_setup_cbk(void *this,void *param) 
{
   uint32_t   lbg_id;
   uint32_t   socket_context_ref;
   int        status;
   rozofs_rdma_tcp_cnx_t *tcp_cnx_p = NULL;
   XDR       xdrs;       
   struct rpc_msg  rpc_reply;
   uint8_t  *payload;
   int      bufsize;
   void     *recv_buf = NULL;   
   sp_rdma_setup_ret_t   response;
   int       error = 0;
   
    rpc_reply.acpted_rply.ar_results.proc = NULL;   
    /*
    ** get the sequence number and the reference of the projection id form the opaque user array
    ** of the transaction context
    */
    rozofs_tx_read_opaque_data(this,2,&lbg_id);
    rozofs_tx_read_opaque_data(this,0,&socket_context_ref);
    tcp_cnx_p = rozofs_rdma_get_tcp_connection_context_from_af_unix_id(socket_context_ref,0);
    /*    
    ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
    */
    status = rozofs_tx_get_status(this);
    if (status < 0)
    {

       errno = rozofs_tx_get_errno(this);
       if (errno == ETIME)
       {
         /*
	 **  re-attempt
	 */
       }  
       goto out_error;
    }
    /*
    ** get the pointer to the receive buffer payload
    */
    recv_buf = rozofs_tx_get_recvBuf(this);
    if (recv_buf == NULL)
    {
       /*
       ** something wrong happened
       */
       errno = EFAULT;  
       goto out_error;         
    }
    /*
    ** set the useful pointer on the received message
    */
    payload  = (uint8_t*) ruc_buf_getPayload(recv_buf);
    payload += sizeof(uint32_t); /* skip length*/
    /*
    ** OK now decode the received message
    */
    bufsize = ruc_buf_getPayloadLen(recv_buf);
    bufsize -= sizeof(uint32_t); /* skip length*/
    xdrmem_create(&xdrs,(char*)payload,bufsize,XDR_DECODE);
    while (1)
    {
      /*
      ** decode the rpc part
      */
      if (rozofs_xdr_replymsg(&xdrs,&rpc_reply) != TRUE)
      {
        TX_STATS(ROZOFS_TX_DECODING_ERROR);
        errno = EPROTO;
        error = 1;
        break;
      }
      /*
      ** decode the status of the operation
      */
      if (xdr_sp_rdma_setup_ret_t(&xdrs,&response)!= TRUE)
      {
        errno = EPROTO;
        error = 1;
        break;    
      }
      /*
      ** check the status of the operation
      */
      if ( response.status != SP_SUCCESS )
      {
        errno = response.sp_rdma_setup_ret_t_u.error;
        error = 1;
        break;    
      }
      break;
    }
    if (error) goto out_error; 
    /*
    ** The other end supports the RDMA so initiate the RDMA connection
    */
    if (tcp_cnx_p != NULL) 
    {
        rozofs_rdma_tcp_assoc_t *assoc_p;
	
	assoc_p = (rozofs_rdma_tcp_assoc_t*)&response.sp_rdma_setup_ret_t_u.rsp;
       rozofs_rdma_tcp_cli_fsm_exec(assoc_p,ROZOFS_RDMA_EV_TCP_RDMA_RSP_ACCEPT);
    }


    /*
    ** the message has not the right sequence number,so just drop the received message
    ** and release the transaction context
    */  
out:
     if(recv_buf!= NULL) ruc_buf_freeBuffer(recv_buf);       
     rozofs_tx_free_from_ptr(this);
     return;

out_error:
     if (tcp_cnx_p != NULL) tcp_cnx_p->state = ROZOFS_RDMA_ST_TCP_DEAD;
     goto out;

}

/*__________________________________________________________________________
*/
/**
*   Callback called from the RDMA FSM upon a state change on the RDMA side

    @param lng_id: lbg identifier
    @param state: 1: RDMA Up/ 0: RDMA Down
    @param socketCtrlRef: reference of the socket controller (context identifier
    
    @retval none
*/

void rozofs_client_rdma_cnx_lbg_state_change(uint32_t lbg_id,uint32_t state,uint32_t socketCtrlRef)
{
  if (state == 0) {
    north_lbg_set_rdma_down(lbg_id); 
    /*
    ** Purge any pending transaction sent by IBV_WR_SEND (rdma) on that load balancing group
    */
    rozofs_rdma_purge_pending_tx_on_lbg(lbg_id);
     
  }
  else
  {
    north_lbg_set_rdma_up(lbg_id,socketCtrlRef);  
  }
}

/*__________________________________________________________________________
*/
/**
*   RDMA/TCP  connect callback upon successful connect
     

 @param userRef : pointer to a load balancer entry
 @param socket_context_ref : index of the socket context
 @param retcode : always RUC_OK
 @param errnum : always 0
 
 @retval none
*/
void rozofs_rdma_tcp_client_connect_CBK (void *userRef,uint32_t socket_context_ref,int retcode,int errnum)
{
    rozofs_rdma_tcp_cnx_t *tcp_cnx_p;
    af_inet_connection_info_t cnx_info;
    int ret;
    int lbg_id;
    void *xmit_buf = NULL;
    sp_rdma_setup_arg_t request;
    /*
    ** get a TCP association context to link the AF_unix side with the RDMA/TCP side
    */
    tcp_cnx_p = rozofs_rdma_get_tcp_connection_context_from_af_unix_id(socket_context_ref,1);
    if (tcp_cnx_p == NULL)
    {
      /*
      ** nothing more to do--> only TCP will work, no RDMA data transfer will take place
      */
      return;
    }
    /*
    ** fill up the connection context with the source/destination IP@ as well as the src/dest ports
    */
    ret = af_inet_get_connection_info(socket_context_ref,&cnx_info);
    if (ret < 0)
    {
      severe("Bad context for af_unix %d:%s",socket_context_ref,strerror(errno));
      return;
    }
    /*
    ** prepare the tcp_connection context
    */
    tcp_cnx_p->assoc.cli_ref = socket_context_ref;
    tcp_cnx_p->assoc.srv_ref = -1;
    tcp_cnx_p->assoc.ip_cli = cnx_info.src_ip;
    tcp_cnx_p->assoc.ip_srv = cnx_info.dst_ip;      
    tcp_cnx_p->assoc.port_cli = cnx_info.src_port;
    tcp_cnx_p->assoc.port_srv = cnx_info.dst_port;
    tcp_cnx_p->assoc.cli_ts = 0;
    /*
    ** send the message to the server side: for doing it we need to get the load balancing group index from
    ** the user Reference, since that reference is the reference of one entry of the LBG
    */
    lbg_id = north_lbg_get_lbg_id_from_lbg_entry(userRef);
    if (lbg_id < 0)
    {
      severe("Bad lbg reference for user context %p",userRef);
      return;
    }
    /*
    ** register the LBG and the callback for state change tracking
    */
    tcp_cnx_p->opaque_ref = lbg_id;
    tcp_cnx_p->state_cbk  = rozofs_client_rdma_cnx_lbg_state_change;
    
    /*
    ** allocate an xmit buffer
    */
    xmit_buf = rozofs_storcli_any_south_buffer_allocate();
    if (xmit_buf == NULL)
    {
       tcp_cnx_p->state = ROZOFS_RDMA_ST_TCP_DEAD;
       return; 
    }
    tcp_cnx_p->state = ROZOFS_RDMA_ST_TCP_WAIT_RDMA_REQ_RSP;
    /*
    ** increment the inuse to avoid a release of the xmit buffer by rozofs_sorcli_send_rq_common()
    */
    ruc_buf_inuse_increment(xmit_buf);
    memcpy(&request,&tcp_cnx_p->assoc,sizeof(request));

    ret =  rozofs_rdma_send_rq(lbg_id,ROZOFS_TMR_GET(TMR_RPC_NULL_PROC_LBG),STORAGE_PROGRAM,STORAGE_VERSION,SP_RDMA_SETUP,
                                        (xdrproc_t) xdr_sp_rdma_setup_arg_t, (caddr_t) &request,
                                         xmit_buf,
                                         socket_context_ref,
                                         0,
                                         0,
                                         rozofs_rdma_setup_cbk,
                                         (void*)NULL);
    ruc_buf_inuse_decrement(xmit_buf);

   if (ret < 0)
   {
    /*
    ** direct need to free the xmit buffer
    */
    tcp_cnx_p->state = ROZOFS_RDMA_ST_TCP_DEAD;
    ruc_buf_freeBuffer(xmit_buf);    
    return;   
   }    
   /*
   ** Check if there is direct response from tx module
   */
   if (tcp_cnx_p->state == ROZOFS_RDMA_ST_TCP_DEAD)
   {
     /*
     ** release the xmit buffer since there was a direct reply from the lbg while attempting to send the buffer
     */
     ruc_buf_freeBuffer(xmit_buf);    
     return;
   }
   return; 
}



/*
 **____________________________________________________
 */
/**
 Callback upon disconnect of the TCP connection of the load balancing group

 @param userRef : pointer to a load balancer entry
 @param socket_context_ref: AF_unix context index
 @param bufRef : pointer to the packet buffer on which the error has been encountered (not used)
 @param err_no : errno associated with the disconnect
 
 @retval none
*/

void  rozofs_rdma_tcp_client_dis_CBK(void *userRef,uint32_t socket_context_ref,void *bufRef,int err_no)
{
    rozofs_rdma_tcp_cnx_t *tcp_cnx_p;
    int lbg_id;
    
    /*
    ** Get the reference of the load balancing group in order to find out if there is some buffers
    ** that have been allocated for RDMA that could be released
    */
    lbg_id = north_lbg_get_lbg_id_from_lbg_entry(userRef);
    rozofs_rdma_tcp_connection_disc_cbk(lbg_id);
    /*
    ** get a TCP association context to link the AF_unix side with the RDMA/TCP side
    */
    tcp_cnx_p = rozofs_rdma_get_tcp_connection_context_from_af_unix_id(socket_context_ref,0);
    if (tcp_cnx_p == NULL)
    {
      /*
      ** nothing more to do--> only TCP will work, no RDMA data transfer will take place
      */
      return;
    }
    north_lbg_set_rdma_down(tcp_cnx_p->opaque_ref);  
    rozofs_rdma_tcp_cli_fsm_exec(&tcp_cnx_p->assoc,ROZOFS_RDMA_EV_TCP_DISCONNECT);
}
