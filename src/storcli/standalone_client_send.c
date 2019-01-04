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
#include <rozofs/common/log.h>
#include <rozofs/core/rozofs_tx_api.h>
#include "rozofs_storcli.h"
#include "rozofs_storcli_rpc.h"
#include <rozofs/rdma/rozofs_rdma.h>
#include "standalone_client_send.h"
#include "rozofs_storcli_sharedmem.h"

int rozofs_storcli_get_position_of_first_byte2write();
/*
**__________________________________________________________________________
*/
/**
*  Procedure to check the current state of the standalone mode

   The standalone re-use the field that have been introducted for RDMA at the load balancing group level
   
   If the connection with the storio is UP and the storio has been able to resolve the shared memory of the storcli
   the connection must be up.
   
   note: the storcli provides the storio with the reference of the "south buffer" pool upon the setup of the TCP
   connection with the storio.
   
   @param lbg_idx: index of the load balancing group
   
   @retval 0 on success
   @retval -1 on error
*/
int storcli_lbg_is_standalone_up(int lbg_idx,uint32_t *ref_p)
{
  int status;
  
  status = north_lbg_is_rdma_up(lbg_idx,ref_p);
  
  return status;
}


void rozofs_storcli_read_req_processing_cbk(void *this,void *param);
void rozofs_storcli_write_req_processing_cbk(void *this,void *param);


/**
**_____________________________________________________________________________
**    S T A N D A L O N E    S E C T I O N 
**_____________________________________________________________________________
*/


typedef struct _standalone_buf_tmo_entry_t
{
   list_t list;
   void *rdma_buf_ref;  /**< reference of the RDMA buffer   */
   uint64_t timestamp;  /**< timestamp that corresponds to the introduction of the buffer in the list  */
   uint32_t xid;        /**< xid of the initial transaction */
} standalone_buf_tmo_entry_t;


int rozofs_standalone_tmo_table_init_done=0;
uint32_t rozofs_standalone_tmo_context_count=0;                              /**< number of context to handle                                               */
int rozofs_standalone_tmo_buffer_count;                                      /**< number of buffer that are waiting for out of seq or TCP disconnect        */
rozofs_standalone_cli_lbg_stats_t *rozofs_standalone_cli_lbg_stats_table_p=NULL;   /**< pointer to the allocated array that contains the number of buffer blocked */
list_t   *rozofs_standalone_tmo_lbg_head_table_p=NULL;                       /**< head of the list for each potential lbg   */
rozofs_standalone_cli_stats_t rozofs_standalone_cli_stats;                         /**< glabal RDMA client statistics    */

/**
*  SHow the buffer that are blocked becauce of lack or response from storio
*/
void show_standalone_buf_tmo(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int i;
    rozofs_standalone_cli_lbg_stats_t *lbg_stats_p;
        
    if (rozofs_standalone_tmo_table_init_done == 0)
    {
      sprintf(pChar,"The table has not been initialized\n");
      goto out;
    }
    pChar += sprintf(pChar,"Total number of buffer blocked : %u\n",rozofs_standalone_tmo_buffer_count);
    /*
    ** display per lbg the number of buffer that are blocked
    */
    
    for (i = 0; i < rozofs_standalone_tmo_context_count; i++)
    {
       lbg_stats_p = &rozofs_standalone_cli_lbg_stats_table_p[i];
       if (lbg_stats_p->standalone_tmo_buffer_count != 0)
       {
         pChar +=sprintf(pChar,"LBG #%3d : %d\n",i,lbg_stats_p->standalone_tmo_buffer_count );
    
       }    
    }
    sprintf(pChar,"\n");

out:    
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}

/*
**__________________________________________________________________________
*/

#define SHOW_STDALONE_CLI_READ(probe) pChar += sprintf(pChar,"   %-16s : %15llu \n",\
                    #probe,\
                    (unsigned long long int)lbg_stats_p->read_stats.probe);
#define SHOW_STDALONE_CLI_WRITE(probe) pChar += sprintf(pChar,"   %-16s : %15llu \n",\
                    #probe,\
                    (unsigned long long int)lbg_stats_p->write_stats.probe);


/**
*  SHow the buffer that are blocked becauce of lack or response from storio
*/
void show_standalone_cli_stats(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int i;
    rozofs_standalone_cli_lbg_stats_t *lbg_stats_p;
        
    if (rozofs_standalone_tmo_table_init_done == 0)
    {
      sprintf(pChar,"The table has not been initialized\n");
      goto out;
    }
    
    for (i = 0; i < rozofs_standalone_tmo_context_count; i++)
    {
       lbg_stats_p = &rozofs_standalone_cli_lbg_stats_table_p[i];
//       if ((lbg_stats_p->read_stats.attempts == 0) && (lbg_stats_p->write_stats.attempts == 0)) continue;
       pChar +=sprintf(pChar,"LBG #%3d\n",i);
       pChar +=sprintf(pChar,"Read Statistics:\n");
       SHOW_STDALONE_CLI_READ(attempts);
       SHOW_STDALONE_CLI_READ(no_buffer);
       SHOW_STDALONE_CLI_READ(no_tx_context);
       SHOW_STDALONE_CLI_READ(lbg_send_ok);
       SHOW_STDALONE_CLI_READ(lbg_send_nok);
       pChar +=sprintf(pChar,"\nWrite Statistics:\n");
       SHOW_STDALONE_CLI_WRITE(attempts);
       SHOW_STDALONE_CLI_WRITE(no_buffer);
       SHOW_STDALONE_CLI_WRITE(no_tx_context);
       SHOW_STDALONE_CLI_WRITE(lbg_send_ok);
       SHOW_STDALONE_CLI_WRITE(lbg_send_nok);
  
    }
    sprintf(pChar,"\n");

out:    
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}

#define SHOW_STDALONE_CLI_READ2(probe) pChar += sprintf(pChar," %12llu |",\
                    (unsigned long long int)lbg_stats_p->read_stats.probe);
		    
#define SHOW_STDALONE_CLI_WRITE2(probe) pChar += sprintf(pChar," %12llu |",\
                    (unsigned long long int)lbg_stats_p->write_stats.probe);

/**
*  SHow the buffer that are blocked becauce of lack or response from storio
*/
void show_standalone_cli_stats2(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int i;
    rozofs_standalone_cli_lbg_stats_t *lbg_stats_p;
        
    if (rozofs_standalone_tmo_table_init_done == 0)
    {
      sprintf(pChar,"The table has not been initialized\n");
      goto out;
    }
    pChar +=sprintf(pChar,"LBG |  Ope  | attempts     |   no_buffer   |  no_tx_ctx   | lbg_send_ok  | lbg_send_nok |\n");
    pChar +=sprintf(pChar,"----+-------+--------------+--------------+--------------+--------------+--------------+\n");
    for (i = 0; i < rozofs_standalone_tmo_context_count; i++)
    {
       lbg_stats_p = &rozofs_standalone_cli_lbg_stats_table_p[i];
       if ((lbg_stats_p->read_stats.attempts == 0) && (lbg_stats_p->write_stats.attempts == 0)) continue;
       pChar +=sprintf(pChar,"%3d |",i);
       pChar +=sprintf(pChar," Read  |");       
       SHOW_STDALONE_CLI_READ2(attempts);
       SHOW_STDALONE_CLI_READ2(no_buffer);
       SHOW_STDALONE_CLI_READ2(no_tx_context);
       SHOW_STDALONE_CLI_READ2(lbg_send_ok);
       SHOW_STDALONE_CLI_READ2(lbg_send_nok);
       pChar +=sprintf(pChar,"\n    | Write |");
       SHOW_STDALONE_CLI_WRITE2(attempts);
       SHOW_STDALONE_CLI_WRITE2(no_buffer);
       SHOW_STDALONE_CLI_WRITE2(no_tx_context);
       SHOW_STDALONE_CLI_WRITE2(lbg_send_ok);
       SHOW_STDALONE_CLI_WRITE2(lbg_send_nok);
       pChar +=sprintf(pChar,"\n----+-------+--------------+--------------+--------------+--------------+--------------+\n");

  
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
    on LBG that are using Standalone mode
    
    @param mx_lbg_north_ctx:  number of context to handle
    
    @retval RUC_OK on success
*/
int  standalone_lbg_tmo_table_init(uint32_t mx_lbg_north_ctx)
{

  list_t *p;
  int i;
  
  if (rozofs_standalone_tmo_table_init_done) return RUC_OK;
  
  rozofs_standalone_tmo_context_count = mx_lbg_north_ctx;
  
  /*
  ** allocate the stats table
  */
  rozofs_standalone_cli_lbg_stats_table_p = xmalloc(mx_lbg_north_ctx*sizeof(rozofs_standalone_cli_lbg_stats_t));
  memset(rozofs_standalone_cli_lbg_stats_table_p,0,mx_lbg_north_ctx*sizeof(rozofs_standalone_cli_lbg_stats_t));
  /*
  ** allocate the head of list for each load balancing group
  */
  rozofs_standalone_tmo_lbg_head_table_p =  xmalloc(mx_lbg_north_ctx*sizeof(list_t));
  /*
  ** init of the lists
  */
  p = &rozofs_standalone_tmo_lbg_head_table_p[0];
  for (i = 0; i <rozofs_standalone_tmo_context_count; i++,p++)
  {
    list_init(p);
  }
  rozofs_standalone_tmo_buffer_count = 0;
  uma_dbg_addTopic("standalone_tmo_buf",show_standalone_buf_tmo);
  uma_dbg_addTopic("standalone_lbg_stats",show_standalone_cli_stats2);
  
  rozofs_standalone_tmo_table_init_done=1;
  return RUC_OK;

}

/*__________________________________________________________________________
*/
/**
*   Callback called when there is a change of the state of the TCP connection associated
    with a LBG when RozoFS operates in standalone mode.
    
    The service uses the same field as RDMA to indicate the current state for standalone mode.

    @param lng_id: lbg identifier
    @param state: 1:  Up/ 0:  Down
    @param socketCtrlRef: reference of the socket controller (context identifier
    
    @retval none
*/

void rozofs_client_standalone_cnx_lbg_state_change(uint32_t lbg_id,uint32_t state,uint32_t socketCtrlRef)
{
  if (state == 0) {
    north_lbg_set_rdma_down(lbg_id);  
  }
  else
  {
    north_lbg_set_rdma_up(lbg_id,socketCtrlRef);  
  }
}
/*
**__________________________________________________________________________
*/
/**
*  Standalone Time-out processing

  Because the buffer provided for the Standalone mode cannot be directly free for future re-use since
  a delayed read/write transfer can occur while the buffer has been allocated for another transaction and
  could trigger some data corruption, the buffer used for standalone transfer should be locked until 
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
void rozofs_standalone_tx_tmo_proc(void *this)
{

   standalone_buf_tmo_entry_t *tmo_ctx_p = NULL;
   uint32_t lbg_id;
   rozofs_standalone_cli_lbg_stats_t *lbg_stats_p;
   
   /*
   ** Get the LBG identifier stored in the transaction context
   */
   rozofs_tx_read_opaque_data(this,2,(uint32_t*)&lbg_id);
   
   lbg_stats_p = &rozofs_standalone_cli_lbg_stats_table_p[lbg_id];
   
   tmo_ctx_p = malloc(sizeof(standalone_buf_tmo_entry_t));
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
   list_push_back(&rozofs_standalone_tmo_lbg_head_table_p[lbg_id],&tmo_ctx_p->list);
   rozofs_standalone_tmo_buffer_count++;
   lbg_stats_p->standalone_tmo_buffer_count++;
   
   return;

error:
   if (tmo_ctx_p != NULL) free(tmo_ctx_p);
}
/*
**__________________________________________________________________________
*/
/**
*  Standalone : OUT of sequence processing

  That function is called upon detecting an out of sequence transaction at the tx_engine side
  The Out of sequence might not be related to a Standalone transfer.
  
  @param lbg_id : reference of the load balancing group for which out of seq transaction is detected
  @param xid: transaction identifier found in the received buffer

  @retval : none
*/
void rozofs_standalone_tx_out_of_seq_cbk(uint32_t lbg_id,uint32_t xid)
{
    list_t *lbg_list_p;
    int inuse;
    list_t *p, *q;    
    rozofs_standalone_cli_lbg_stats_t *lbg_stats_p;
    
    if (lbg_id >=rozofs_standalone_tmo_context_count )
    {
       warning("Out of range lbg_id %u (max is %u)",lbg_id,rozofs_standalone_tmo_context_count);
       return;
    }
    lbg_list_p = &rozofs_standalone_tmo_lbg_head_table_p[lbg_id];
    lbg_stats_p = &rozofs_standalone_cli_lbg_stats_table_p[lbg_id];
    
    list_for_each_forward_safe(p,q,lbg_list_p)
    {
      standalone_buf_tmo_entry_t *tmo_ctx_p= list_entry(p, standalone_buf_tmo_entry_t, list);
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
      rozofs_standalone_tmo_buffer_count--;
      if (rozofs_standalone_tmo_buffer_count < 0) rozofs_standalone_tmo_buffer_count=0;
      lbg_stats_p->standalone_tmo_buffer_count--;
      if (lbg_stats_p->standalone_tmo_buffer_count < 0) lbg_stats_p->standalone_tmo_buffer_count = 0;
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
*  Standalone : TCP disconnect callback processing

  That function is called upon the disconnection of the TCP connection that is used in conjunction with Standalone mode
  
  @param lbg_id : reference of the load balancing group for which the TCP connection is down

  @retval : none
*/
void rozofs_standalone_tcp_connection_disc_cbk(uint32_t lbg_id)
{
    list_t *lbg_list_p;
    int inuse;
    list_t *p, *q;    
    rozofs_standalone_cli_lbg_stats_t *lbg_stats_p;

    if (lbg_id >=rozofs_standalone_tmo_context_count )
    {
       warning("Out of range lbg_id %u (max is %u)",lbg_id,rozofs_standalone_tmo_context_count);
       return;
    }
    lbg_list_p = &rozofs_standalone_tmo_lbg_head_table_p[lbg_id];
    lbg_stats_p = &rozofs_standalone_cli_lbg_stats_table_p[lbg_id];
    
    list_for_each_forward_safe(p,q,lbg_list_p)
    {
      standalone_buf_tmo_entry_t *tmo_ctx_p= list_entry(p, standalone_buf_tmo_entry_t, list);
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
      rozofs_standalone_tmo_buffer_count--;
      if (rozofs_standalone_tmo_buffer_count < 0) rozofs_standalone_tmo_buffer_count=0;
      lbg_stats_p->standalone_tmo_buffer_count--;
      if (lbg_stats_p->standalone_tmo_buffer_count < 0) lbg_stats_p->standalone_tmo_buffer_count=0;
      /*
      ** release the context
      */ 
      free(tmo_ctx_p);
    }
}

/*
**__________________________________________________________________________
*/
/**
*  That function is the call back associated with SP_READ_STANDALONE

   The goal of that function is to copy the data (bins) returned by a storio at
   the revelant offset in the buffer that has been allocated for standalone transfer.
   
   When it is done, the current received buffer is released and it is replaced
   by the buffer allocated for the Standalone transfer.
   
   When it is done, the regular non RDMA callback function rozofs_storcli_read_req_processing_cbk()
   is called. By doing so, the usage of Standalone mode is transparent.

   @param this: transaction context
   @param param
   
   @retval none
*/
void rozofs_storcli_read_standalone_req_processing_cbk(void *this,void *param) 
{
   void *rdma_buf_ref;
   void *recv_buf;
   int status;   
   char *src_payload;
   char *dst_payload;
   uint32_t max_size;
   int error;
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
	 ** release the RDMA buffer right away since the Standalone transfer
	 ** could happen while the buffer has been re-allocated for another transaction
	 **
	 ** We need to keep the buffer until there is disconnection or the reception of
	 ** a response for that buffer that is received out of sequence
	 */
	 rozofs_standalone_tx_tmo_proc(this);
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
    ** The Standalone buffer becomes the received buffer for the remaining
    ** of the processing.
    */
    max_size = ruc_buf_getPayloadLen(recv_buf);  
    memcpy(dst_payload,src_payload,max_size);
    /*
    ** adjust the size of the payload of the destination buffer
    */
    max_size = ruc_buf_getMaxPayloadLen(rdma_buf_ref);
    max_size -=sizeof(uint32_t);
    ruc_buf_setPayloadLen(rdma_buf_ref,max_size);
    /*
    ** Release the received buffer and push the Standalone buffer as the received buffer.
    */
    ruc_buf_freeBuffer(recv_buf);
    rozofs_tx_put_recvBuf(this,rdma_buf_ref);
    /*
    ** remove the reference of the rdma_buffer: standalone & RDMA uses the same flag within the transaction module
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
   SP_READ_STANDALONE encoding

    @param lbg_id             : reference of the load balancing group
    @param socket_context_ref : socket controller reference (index) of the TCP connection for which there is an associated Standalone connection
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

*/
int rozofs_sorcli_sp_read_standalone(uint32_t lbg_id,uint32_t socket_context_ref, uint32_t timeout_sec, uint32_t prog,uint32_t vers,
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
   sp_read_standalone_arg_t request_rdma ;
   void *south_buf=NULL;
   rozofs_standalone_cli_lbg_stats_t *lbg_stats_p; 
   
   lbg_stats_p = &rozofs_standalone_cli_lbg_stats_table_p[lbg_id];

   memcpy(&request_rdma,msg2encode_p,sizeof(sp_read_arg_t));
   /*
   ** update the statistics
   */
   lbg_stats_p->read_stats.attempts++;   
    /*
    ** allocate a south buffer
    */
    south_buf = rozofs_alloc_shared_storcli_buf(_ROZOFS_STORCLI_SOUTH_LARGE_POOL);   
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
    /*
    ** Get the index of the buffer
    */
    request_rdma.buf_offset = 0;
    request_rdma.share_buffer_index = rozofs_get_shared_storcli_payload_idx(south_buf,_ROZOFS_STORCLI_SOUTH_LARGE_POOL,
                                                                             NULL,
                                                                             &request_rdma.sharemem_key);    
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
    ** now send the message
    */
    ret = north_lbg_send_with_shaping(lbg_id,xmit_buf,0,0);
    if (ret < 0)
    {
       lbg_stats_p->read_stats.lbg_send_nok++;   
       TX_STATS(ROZOFS_TX_SEND_ERROR);
       errno = EFAULT;
      goto error;  
    }
    lbg_stats_p->read_stats.lbg_send_ok++;   
    TX_STATS(ROZOFS_TX_SEND);

    {
      sp_read_arg_t *request = (sp_read_arg_t *)msg2encode_p;
      rozofs_storcli_trace_request(user_ctx_p, opaque_value_idx1, request->sid);
    }
    /*
    ** OK, so now finish by starting the guard timer
    */
    rozofs_tx_start_timer(rozofs_tx_ctx_p,timeout_sec);  
    return 0;  
    
  error:
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);
    if (south_buf != NULL) ruc_buf_freeBuffer(south_buf);
    return -1;       
}

#if 1
/*
**__________________________________________________________________________
*/
/**
*  That function is the call back associated with SP_WRITE_STANDALONE

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
void rozofs_storcli_write_standalone_req_processing_cbk(void *this,void *param) 
{
   void *rdma_buf_ref;
   int status;   
   int error;

   /*
   ** Get the reference of the buffer that contains the projection from the transaction context
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
	 rozofs_standalone_tx_tmo_proc(this);
       }
       /*
       ** the reference from the transaction context
       */
       rozofs_tx_clear_rdma_bufref(rdma_buf_ref);
       
     return rozofs_storcli_write_req_processing_cbk(this,param);
   }
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
   SP_WRITE_STANDALONE encoding

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
int rozofs_sorcli_sp_write_standalone(uint32_t lbg_id,uint32_t socket_context_ref, uint32_t timeout_sec, uint32_t prog,uint32_t vers,
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
   sp_write_standalone_arg_t request_rdma ;
   void *xmit_buf=NULL;
   rozofs_standalone_cli_lbg_stats_t *lbg_stats_p; 
   
   lbg_stats_p = &rozofs_standalone_cli_lbg_stats_table_p[lbg_id];
   
   memcpy(&request_rdma,msg2encode_p,sizeof(sp_write_arg_no_bins_t));
   /*
   ** update statistics
   */
   lbg_stats_p->write_stats.attempts++;
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
    ** Get the reference of the buffer that contains the projection to write and
    ** indicates where data to write starts in that buffer
    */ 

    {
       request_rdma.buf_offset = rozofs_storcli_get_position_of_first_byte2write();
       request_rdma.share_buffer_index = rozofs_get_shared_storcli_payload_idx(proj_buf,_ROZOFS_STORCLI_SOUTH_LARGE_POOL,
                                                                               NULL,
                                                                               &request_rdma.sharemem_key);   
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
    ** store the reference of the projection buffer in the transaction context
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
    ** now send the message
    */
    ret = north_lbg_send(lbg_id,xmit_buf);
    if (ret < 0)
    {
       lbg_stats_p->write_stats.lbg_send_nok++;
       TX_STATS(ROZOFS_TX_SEND_ERROR);
       errno = EFAULT;
      goto error;  
    }
    lbg_stats_p->write_stats.lbg_send_ok++;
    TX_STATS(ROZOFS_TX_SEND);

    {
      sp_write_arg_t *request = (sp_write_arg_t *)msg2encode_p;
      rozofs_storcli_trace_request(user_ctx_p, opaque_value_idx1, request->sid);
    }
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
}
#endif

/**
* API for creation a transaction towards a storio process

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

int rozofs_standalone_send_rq(uint32_t lbg_id,uint32_t timeout_sec, uint32_t prog,uint32_t vers,
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

void rozofs_standalone_setup_cbk(void *this,void *param) 
{
   uint32_t   lbg_id;
   uint32_t   socket_context_ref;
   int        status;
   XDR       xdrs;       
   struct rpc_msg  rpc_reply;
   uint8_t  *payload;
   int      bufsize;
   void     *recv_buf = NULL;   
   sp_status_ret_t   response;
   int       error = 0;
   
    rpc_reply.acpted_rply.ar_results.proc = NULL;   
    /*
    ** get the sequence number and the reference of the projection id form the opaque user array
    ** of the transaction context
    */
    rozofs_tx_read_opaque_data(this,2,&lbg_id);
    rozofs_tx_read_opaque_data(this,0,&socket_context_ref);
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
      if (xdr_sp_status_ret_t(&xdrs,&response)!= TRUE)
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
        errno = response.sp_status_ret_t_u.error;
        error = 1;
        break;    
      }
      break;
    }
    if (error) goto out_error; 
    /*
    ** The other end supports the RDMA so initiate the RDMA connection: the LBG is available for
    ** by-pass mode to emulate a kind of rmda mode in standalone
    */
    rozofs_client_standalone_cnx_lbg_state_change(lbg_id,1,socket_context_ref);
    
out:
     if(recv_buf!= NULL) ruc_buf_freeBuffer(recv_buf);       
     rozofs_tx_free_from_ptr(this);
     return;

out_error:
     goto out;

}



/*__________________________________________________________________________
*/
/**
*   Once the Storcli is connected with storio, it should provide the information related to its shared memory
     

 @param userRef : pointer to a load balancer entry
 @param socket_context_ref : index of the socket context
 @param retcode : always RUC_OK
 @param errnum : always 0
 
 @retval none
*/
void rozofs_standalone_tcp_client_connect_CBK (void *userRef,uint32_t socket_context_ref,int retcode,int errnum)
{
    int ret;
    int lbg_id;
    void *xmit_buf = NULL;
    sp_standalone_setup_arg_t request;
    rozofs_stc_south_shared_pool_t *pool_shared_p;
    
    pool_shared_p = &rozofs_storcli_shared_mem[_ROZOFS_STORCLI_SOUTH_LARGE_POOL];
    /*
    ** Get the reference of the share memory that contains the south buffers
    */
    request.sharemem_key = (uint32_t)pool_shared_p->key;
    request.bufcount = pool_shared_p->buf_count;
    request.bufsize = pool_shared_p->buf_sz;

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
    ** allocate an xmit buffer
    */
    xmit_buf = rozofs_storcli_any_south_buffer_allocate();
    if (xmit_buf == NULL)
    {
       return; 
    }
    /*
    ** increment the inuse to avoid a release of the xmit buffer by rozofs_sorcli_send_rq_common()
    */
    ruc_buf_inuse_increment(xmit_buf);
    info("FDL SP_STANDALONE_SETUP for lbg %d",lbg_id);

    ret =  rozofs_standalone_send_rq(lbg_id,ROZOFS_TMR_GET(TMR_RPC_NULL_PROC_LBG),STORAGE_PROGRAM,STORAGE_VERSION,SP_STANDALONE_SETUP,
                                        (xdrproc_t) xdr_sp_standalone_setup_arg_t, (caddr_t) &request,
                                         xmit_buf,
                                         socket_context_ref,
                                         0,
                                         0,
                                         rozofs_standalone_setup_cbk,
                                         (void*)NULL);
    ruc_buf_inuse_decrement(xmit_buf);

   if (ret < 0)
   {
    /*
    ** direct need to free the xmit buffer
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

void  rozofs_standalone_tcp_client_dis_CBK(void *userRef,uint32_t socket_context_ref,void *bufRef,int err_no)
{
    int lbg_id;
    
    /*
    ** Get the reference of the load balancing group in order to find out if there is some buffers
    ** that have been allocated for RDMA that could be released
    */
    lbg_id = north_lbg_get_lbg_id_from_lbg_entry(userRef);
    rozofs_standalone_tcp_connection_disc_cbk(lbg_id);
    /*
    ** use the same field as rdma
    */
    north_lbg_set_rdma_down(lbg_id);    
}
