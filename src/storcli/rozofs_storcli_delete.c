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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stddef.h>
#include <fcntl.h>
#include <unistd.h>
#include <getopt.h>
#include <pthread.h>
#include <assert.h>

#include <rozofs/rozofs.h>
#include "config.h"
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
//#include "rozofs_stats.h"
#include <rozofs/core/rozofs_tx_common.h>
#include <rozofs/core/rozofs_tx_api.h>
#include "rozofs_storcli.h"
#include "storage_proto.h"
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/rozofs_srv.h>
#include "rozofs_storcli_rpc.h"
#include <rozofs/rpc/sproto.h>
#include "storcli_main.h"
#include <rozofs/rozofs_timer_conf.h>


int rozofs_storcli_get_position_of_first_byte2write_in_delete();

DECLARE_PROFILING(stcpp_profiler_t);

/*
**__________________________________________________________________________
*/
/**
* PROTOTYPES
*/


/**
*  END PROTOTYPES
*/
/*
**__________________________________________________________________________
*/

/**
* Local prototypes
*/
void rozofs_storcli_delete_req_processing_cbk(void *this,void *param) ;
void rozofs_storcli_delete_req_processing(rozofs_storcli_ctx_t *working_ctx_p);

//int rozofs_storcli_remote_rsp_cbk(void *buffer,uint32_t socket_ref,void *user_param);

void rozofs_storcli_delete_req_processing_exec(rozofs_storcli_ctx_t *working_ctx_p, char * data);
/*
**_________________________________________________________________________
*      LOCAL FUNCTIONS
**_________________________________________________________________________
*/


/*
**__________________________________________________________________________
*/
/**
* The purpose of that function is to return TRUE if there are enough delete response received for
  rebuilding a projection for future reading 
  
  @param layout : layout association with the file
  @param prj_cxt_p: pointer to the projection context (working array)
  @param *errcode: pointer to global error code to return
  
  @retval 1 if there are enough received projection
  @retval 0 when there is enough projection
*/
static inline int rozofs_storcli_all_prj_delete_check(uint8_t layout,rozofs_storcli_projection_ctx_t *prj_cxt_p,int *errcode_p)
{
  /*
  ** Get the rozofs_safe value for the layout
  */
  uint8_t   rozofs_safe = rozofs_get_rozofs_safe(layout);
  int i;
  int received = 0;
  *errcode_p = 0;
  for (i = 0; i <rozofs_safe; i++,prj_cxt_p++)
  {
    if (prj_cxt_p->prj_state == ROZOFS_PRJ_WR_DONE) 
    {
      received++;
      continue;
    }
    if (prj_cxt_p->prj_state == ROZOFS_PRJ_WR_ERROR) 
    {
      *errcode_p = prj_cxt_p->errcode;
      received++;
      continue;
    }
  }
  if (received == rozofs_safe) return 1;   

  return 0;
}

/*
**_________________________________________________________________________
*      PUBLIC FUNCTIONS
**_________________________________________________________________________
*/
/*
**__________________________________________________________________________
*/
/**
  Initial delete request
    
  @param socket_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)
  @param rozofs_storcli_remote_rsp_cbk: callback for sending out the response
 
   @retval : TRUE-> xmit ready event expected
  @retval : FALSE-> xmit  ready event not expected
*/
void rozofs_storcli_delete_req_init(uint32_t  socket_ctx_idx, void *recv_buf,rozofs_storcli_resp_pf_t rozofs_storcli_remote_rsp_cbk)
{
   rozofs_rpc_call_hdr_with_sz_t    *com_hdr_p;
   rozofs_storcli_ctx_t *working_ctx_p = NULL;
   int i;
   uint32_t  msg_len;  /* length of the rpc messsage including the header length */
   storcli_delete_arg_t *storcli_delete_rq_p = NULL;
   rozofs_rpc_call_hdr_t   hdr;   /* structure that contains the rpc header in host format */
   int      len;       /* effective length of application message               */
   uint8_t  *pmsg;     /* pointer to the first available byte in the application message */
   uint32_t header_len;
   XDR xdrs;
   int errcode = EINVAL;
   /*
   ** allocate a context for the duration of the write
   */
   working_ctx_p = rozofs_storcli_alloc_context();
   if (working_ctx_p == NULL)
   {
     /*
     ** that situation MUST not occur since there the same number of receive buffer and working context!!
     */
     severe("out of working read/write saved context");
     goto failure;
   }
   storcli_delete_rq_p = &working_ctx_p->storcli_delete_arg;
   STORCLI_START_NORTH_PROF(working_ctx_p,delete,0);

   
   /*
   ** Get the full length of the message and adjust it the the length of the applicative part (RPC header+application msg)
   */
   msg_len = ruc_buf_getPayloadLen(recv_buf);
   msg_len -=sizeof(uint32_t);

   /*
   ** save the reference of the received socket since it will be needed for sending back the
   ** response
   */
   working_ctx_p->socketRef    = socket_ctx_idx;
   working_ctx_p->user_param   = NULL;
   working_ctx_p->recv_buf     = recv_buf;
   working_ctx_p->response_cbk = rozofs_storcli_remote_rsp_cbk;
   /*
   ** Get the payload of the receive buffer and set the pointer to the array that describes the write request
   */
   com_hdr_p  = (rozofs_rpc_call_hdr_with_sz_t*) ruc_buf_getPayload(recv_buf);   
   memcpy(&hdr,&com_hdr_p->hdr,sizeof(rozofs_rpc_call_hdr_t));
   /*
   ** swap the rpc header
   */
   scv_call_hdr_ntoh(&hdr);
   pmsg = rozofs_storcli_set_ptr_on_nfs_call_msg((char*)&com_hdr_p->hdr,&header_len);
   if (pmsg == NULL)
   {
     errcode = EFAULT;
     goto failure;
   }
   /*
   ** map the memory on the first applicative RPC byte available and prepare to decode:
   ** notice that we will not call XDR_FREE since the application MUST
   ** provide a pointer for storing the file handle
   */
   len = msg_len - header_len;    
   xdrmem_create(&xdrs,(char*)pmsg,len,XDR_DECODE); 
   /*
   ** store the source transaction id needed for the reply
   */
   working_ctx_p->src_transaction_id =  hdr.hdr.xid;
   /*
   ** decode the RPC message of the delete request
   */
   if (xdr_storcli_delete_arg_t(&xdrs,storcli_delete_rq_p) == FALSE)
   {
      /*
      ** decoding error
      */
      errcode = EFAULT;
      severe("rpc trucnate request decoding error");
      goto failure;
      
   }   
   /*
   ** init of the load balancing group/ projection association table:
   ** That table is ordered: the first corresponds to the storage associated with projection 0, second with 1, etc..
   ** When build that table, we MUST consider the value of the base which is associated with the distribution
   */

   
   uint8_t   rozofs_safe = rozofs_get_rozofs_safe(storcli_delete_rq_p->layout);
   for (i = 0; i  <rozofs_safe ; i ++)
   {
    /*
    ** Get the load balancing group associated with the sid
    */
    int lbg_id = rozofs_storcli_get_lbg_for_sid(storcli_delete_rq_p->cid,storcli_delete_rq_p->dist_set[i]);
     rozofs_storcli_lbg_prj_insert_lbg_and_sid(working_ctx_p->lbg_assoc_tb,i,
                                                lbg_id,
                                                storcli_delete_rq_p->dist_set[i]);  

     rozofs_storcli_lbg_prj_insert_lbg_state(working_ctx_p->lbg_assoc_tb,
                                             i,
                                             NORTH_LBG_GET_STATE(working_ctx_p->lbg_assoc_tb[i].lbg_id));    

   }
   /*
   ** allocate a small buffer that will be used for sending the response to the delete request
   */
   working_ctx_p->xmitBuf = ruc_buf_getBuffer(ROZOFS_STORCLI_NORTH_SMALL_POOL);
   if (working_ctx_p == NULL)
   {
     /*
     ** that situation MUST not occur since there the same number of receive buffer and working context!!
     */
     errcode = ENOMEM;
     severe("out of small buffer");
     goto failure;
   }
   /*
   ** allocate a sequence number for the working context (same aas for read)
   */
   working_ctx_p->read_seqnum = rozofs_storcli_allocate_read_seqnum();
   /*
   ** set now the working variable specific for handling the projection deletion
   ** we re-use the structure used for writing even if nothing is written
   */
   for (i = 0; i < rozofs_safe; i++)
   {
     working_ctx_p->prj_ctx[i].prj_state = ROZOFS_PRJ_READ_IDLE;
     working_ctx_p->prj_ctx[i].prj_buf   = ruc_buf_getBuffer(ROZOFS_STORCLI_SOUTH_LARGE_POOL);
     if (working_ctx_p->prj_ctx[i].prj_buf == NULL)
     {
       /*
       ** that situation MUST not occur since there the same number of receive buffer and working context!!
       */
       errcode = ENOMEM;
       severe("out of large buffer");
       goto failure;
     }
     /*
     ** increment inuse counter on each buffer since we might need to re-use that packet in case
     ** of retransmission
     */
     working_ctx_p->prj_ctx[i].inuse_valid = 1;
     ruc_buf_inuse_increment(working_ctx_p->prj_ctx[i].prj_buf);
   }
   		
   /*
   ** Prepare for request serialization
   */
   memcpy(working_ctx_p->fid_key, storcli_delete_rq_p->fid, sizeof (sp_uuid_t));
   working_ctx_p->opcode_key = STORCLI_DELETE;
   {
       /**
        * lock all the file for a delete
        */
       uint64_t nb_blocks = 0;
       nb_blocks--;
       int ret;
       ret = stc_rng_insert((void*)working_ctx_p,
               STORCLI_DELETE,working_ctx_p->fid_key,
               0,nb_blocks,
               &working_ctx_p->sched_idx);
       if (ret == 0)
       {
           /*
            ** there is a current request that is processed with the same fid and there is a collision
            */
           return;
       }
       /*
        ** no request pending with that fid, so we can process it right away
        */
       return rozofs_storcli_delete_req_processing(working_ctx_p);
   }

    /*
    **_____________________________________________
    **  Exception cases
    **_____________________________________________
    */      
       

    /*
    ** there was a failure while attempting to allocate a memory ressource.
    */
failure:
     /*
     ** send back the response with the appropriated error code. 
     ** note: The received buffer (rev_buf)  is
     ** intended to be released by this service in case of error or the TCP transmitter
     ** once it has been passed to the TCP stack.
     */
     rozofs_storcli_reply_error_with_recv_buf(socket_ctx_idx,recv_buf,NULL,rozofs_storcli_remote_rsp_cbk,errcode);
     /*
     ** check if the root context was allocated. Free it if is exist
     */
     if (working_ctx_p != NULL) 
     {
        /*
        ** remove the reference to the recvbuf to avoid releasing it twice
        */
       STORCLI_STOP_NORTH_PROF(working_ctx_p,delete,0);
       working_ctx_p->recv_buf   = NULL;
       rozofs_storcli_release_context(working_ctx_p);
     }
     return;
}

/*
**__________________________________________________________________________
*/
/*
** That function is called when all the projection are ready to be sent

 @param working_ctx_p: pointer to the root context associated with the top level write request
 @param data         : pointer to the data of the last block to delete

*/
void rozofs_storcli_delete_req_processing_exec(rozofs_storcli_ctx_t *working_ctx_p, char * data)
{

  storcli_delete_arg_t *storcli_delete_rq_p = (storcli_delete_arg_t*)&working_ctx_p->storcli_delete_arg;
  uint8_t layout = storcli_delete_rq_p->layout;
  uint8_t   rozofs_safe;
  int       storage_idx;
  int       error=0;
  int       global_error;
  int       ret;
  
  rozofs_storcli_lbg_prj_assoc_t  *lbg_assoc_p = working_ctx_p->lbg_assoc_tb;
  rozofs_storcli_projection_ctx_t *prj_cxt_p   = working_ctx_p->prj_ctx;   
  
  rozofs_safe    = rozofs_get_rozofs_safe(layout);
  /*
  ** set the current state of each load balancing group belonging to the rozofs_safe group
  */
  for (storage_idx = 0; storage_idx < rozofs_safe; storage_idx++) 
  {
    /*
    ** Check the state of the load Balancing group
    */
    rozofs_storcli_lbg_prj_insert_lbg_state(lbg_assoc_p,
                                            storage_idx,
                                            NORTH_LBG_GET_STATE(lbg_assoc_p[storage_idx].lbg_id));      
  }
  /*
  ** We have enough storage, so initiate the transaction towards the storage for each
  ** projection
  */
  for (storage_idx = 0; storage_idx < rozofs_safe; storage_idx++)
  {
     sp_remove_arg_t *request; 
     sp_remove_arg_t  delete_prj_args;
     void  *xmit_buf;  
     
     /*
     ** check the state of the lbg before sending out the request
     */
     if (rozofs_storcli_lbg_prj_is_lbg_selectable(lbg_assoc_p,storage_idx,0) == 0)
     {
       prj_cxt_p[storage_idx].prj_state = ROZOFS_PRJ_WR_ERROR;
       prj_cxt_p[storage_idx].errcode = ENONET;
       continue;     
     }
      
     xmit_buf = prj_cxt_p[storage_idx].prj_buf;
     /*
     ** fill partially the common header
     */
     request   = &delete_prj_args;
     request->cid = storcli_delete_rq_p->cid;
     request->sid = (uint8_t) working_ctx_p->lbg_assoc_tb[storage_idx].sid;
     request->layout        = layout;
     memcpy(request->fid, storcli_delete_rq_p->fid, sizeof (sp_uuid_t));

     uint32_t  lbg_id = working_ctx_p->lbg_assoc_tb[storage_idx].lbg_id;
     STORCLI_START_NORTH_PROF((&working_ctx_p->prj_ctx[storage_idx]),delete_prj,0);
     /*
     ** caution we might have a direct reply if there is a direct error at load balancing group while
     ** ateempting to send the RPC message-> typically a disconnection of the TCP connection 
     ** As a consequence the response fct 'rozofs_storcli_delete_req_processing_cbk) can be called
     ** prior returning from rozofs_sorcli_send_rq_common')
     ** anticipate the status of the xmit state of the projection and lock the section to
     ** avoid a reply error before returning from rozofs_sorcli_send_rq_common() 
     ** --> need to take care because the write context is released after the reply error sent to rozofsmount
     */
     working_ctx_p->write_ctx_lock = 1;
     prj_cxt_p[storage_idx].prj_state = ROZOFS_PRJ_WR_IN_PRG;
     
     ret =  rozofs_sorcli_send_rq_common(lbg_id,ROZOFS_TMR_GET(TMR_STORAGE_PROGRAM),STORAGE_PROGRAM,STORAGE_VERSION,SP_REMOVE,
                                         (xdrproc_t) xdr_sp_remove_arg_t, (caddr_t) request,
                                          xmit_buf,
                                          working_ctx_p->read_seqnum,
                                          (uint32_t) storage_idx,
                                          0,
                                          rozofs_storcli_delete_req_processing_cbk,
                                         (void*)working_ctx_p);
     working_ctx_p->write_ctx_lock = 0;
     if (ret < 0)
     {
       /*
       ** the communication with the storage seems to be wrong (more than TCP connection temporary down
       ** save the error code in the current context and go to the next storage in sequence
       **
       */
       prj_cxt_p[storage_idx].prj_state = ROZOFS_PRJ_WR_ERROR;
       prj_cxt_p[storage_idx].errcode = errno;

       continue;
     } 
   }
   /*
   ** check if the deletion is done: it can happen in case of direct error. Typically the case
   ** where all the storage node are down
   */
    ret = rozofs_storcli_all_prj_delete_check(storcli_delete_rq_p->layout,
                                             working_ctx_p->prj_ctx,
                                             &global_error);
    if (ret == 0)
    {
      /*
      ** need to wait more projection
      */
      return;
    }
    /*
    ** all attempts have failed
    */
    error = global_error;

    rozofs_storcli_write_reply_error(working_ctx_p,error);
    /*
    ** release the root transaction context
    */
    STORCLI_STOP_NORTH_PROF(working_ctx_p,delete,0);
    rozofs_storcli_release_context(working_ctx_p);  
    return;
}
/*
**__________________________________________________________________________
*/
/*
** That function is called when all the projection are ready to be sent

 @param working_ctx_p: pointer to the root context associated with the top level write request

*/
void rozofs_storcli_delete_req_processing(rozofs_storcli_ctx_t *working_ctx_p)
{

  rozofs_storcli_delete_req_processing_exec(working_ctx_p,NULL);
}


/*
**__________________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure on a projection write request
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */

void rozofs_storcli_delete_req_processing_cbk(void *this,void *param) 
{
   uint32_t   seqnum;
   uint32_t   storage_idx;
   rozofs_storcli_projection_ctx_t  *write_prj_work_p = NULL;   
   rozofs_storcli_ctx_t *working_ctx_p = (rozofs_storcli_ctx_t*) param ;
   XDR       xdrs;       
   uint8_t  *payload;
   int      bufsize;
   sp_status_ret_t   rozofs_status;
   struct rpc_msg  rpc_reply;
   storcli_delete_arg_t *storcli_delete_rq_p = NULL;
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   int lbg_id;
   int global_error;   
   int status;
   void     *recv_buf = NULL;   
   int      ret;
   int error = 0;


    storcli_delete_rq_p = (storcli_delete_arg_t*)&working_ctx_p->storcli_delete_arg;
    /*
    ** get the sequence number and the reference of the projection id form the opaque user array
    ** of the transaction context
    */
    rozofs_tx_read_opaque_data(this,0,&seqnum);
    rozofs_tx_read_opaque_data(this,1,&storage_idx);
    rozofs_tx_read_opaque_data(this,2,(uint32_t*)&lbg_id);


    /*
    ** check if the sequence number of the transaction matches with the one saved in the tranaaction
    ** that control is required because we can receive a response from a late transaction that
    ** it now out of sequence since the system is waiting for transaction response on a next
    ** set of distribution
    ** In that case, we just drop silently the received message
    */
    if (seqnum != working_ctx_p->read_seqnum)
    {
      /*
      ** not the right sequence number, so drop the received message
      */   
      goto drop_msg;    
    }
    /*
    ** check if the delete is already done: this might happen in the case when the same projection
    ** is sent towards 2 different LBGs
    */    
    if (working_ctx_p->prj_ctx[storage_idx].prj_state == ROZOFS_PRJ_WR_DONE)
    {
      /*
      ** The reponse has already been received for that projection so we don't care about that
      ** extra reponse
      */      
      goto drop_msg;       
    }
    /*    
    ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
    */
    status = rozofs_tx_get_status(this);
    if (status < 0)
    {   

       /*
       ** something wrong happened: assert the status in the associated projection id sub-context
       */
       working_ctx_p->prj_ctx[storage_idx].prj_state = ROZOFS_PRJ_WR_ERROR;
       working_ctx_p->prj_ctx[storage_idx].errcode   = rozofs_tx_get_errno(this);
       errno = rozofs_tx_get_errno(this);  
       if (errno == ETIME)
       {
         storcli_lbg_cnx_sup_increment_tmo(lbg_id);
         STORCLI_ERR_PROF(delete_prj_tmo);
       }
       else
       {
         STORCLI_ERR_PROF(delete_prj_err);
       } 
       /*
       ** check if are done
       */      
       goto check_end; 
    }
    storcli_lbg_cnx_sup_clear_tmo(lbg_id);
    /*
    ** get the pointer to the receive buffer payload
    */
    recv_buf = rozofs_tx_get_recvBuf(this);
    if (recv_buf == NULL)
    {
       /*
       ** something wrong happened
       */
       error = EFAULT;  
       working_ctx_p->prj_ctx[storage_idx].prj_state = ROZOFS_PRJ_WR_ERROR;
       working_ctx_p->prj_ctx[storage_idx].errcode = error;
       goto fatal;         
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
      if (xdr_sp_status_ret_t(&xdrs,&rozofs_status)!= TRUE)
      {
        errno = EPROTO;
        error = 1;
        break;    
      }
      /*
      ** check the status of the operation
      */
      if ( rozofs_status.status != SP_SUCCESS )
      {
      
         errno = rozofs_status.sp_status_ret_t_u.error;
         error = 1;
        break;    
      }
      break;
    }
    /*
    ** check the status of the operation
    */
    if (error)
    {
       /*
       ** there was an error on the remote storage while attempt to delete the file
       */
       working_ctx_p->prj_ctx[storage_idx].prj_state = ROZOFS_PRJ_WR_ERROR;
       working_ctx_p->prj_ctx[storage_idx].errcode   = errno;
       /*
       ** check if we are done
       */
       goto check_end;    
    }
    STORCLI_STOP_NORTH_PROF((&working_ctx_p->prj_ctx[storage_idx]),delete_prj,0);
    /*
    ** set the pointer to the delete context associated with the projection for which a response has
    ** been received
    */
    write_prj_work_p = &working_ctx_p->prj_ctx[storage_idx];
    /*
    ** set the status of the transaction to done for that projection
    */
    write_prj_work_p->prj_state = ROZOFS_PRJ_WR_DONE;
    write_prj_work_p->errcode   = 0;
    /*
    ** OK now check if we have send enough projection
    ** if it is the case, the distribution will be valid
    */
check_end:
    ret = rozofs_storcli_all_prj_delete_check(storcli_delete_rq_p->layout,
                                             working_ctx_p->prj_ctx,
                                             &global_error);
    if (ret == 0)
    {
       /*
       ** no enough projection 
       */
       goto wait_more_projection;
    }
    /*
    ** delete is finished, send back the response to the client (rozofsmount)
    */
    if (global_error != 0) rozofs_storcli_write_reply_error(working_ctx_p,global_error);
    else rozofs_storcli_delete_reply_success(working_ctx_p);
    /*
    ** release the root context and the transaction context
    */
    if(recv_buf!= NULL) ruc_buf_freeBuffer(recv_buf);       
    rozofs_storcli_release_context(working_ctx_p);    
    rozofs_tx_free_from_ptr(this);
    return;
    
    /*
    **_____________________________________________
    **  Exception cases
    **_____________________________________________
    */    
drop_msg:
    /*
    ** the message has not the right sequence number,so just drop the received message
    ** and release the transaction context
    */  
     if(recv_buf!= NULL) ruc_buf_freeBuffer(recv_buf);       
     rozofs_tx_free_from_ptr(this);
     return;

fatal:
    /*
    ** caution lock can be asserted either by a delete attempt 
    */
    if (working_ctx_p->write_ctx_lock != 0) return;
    /*
    ** unrecoverable error : mostly a bug!!
    */  
    STORCLI_STOP_NORTH_PROF((&working_ctx_p->prj_ctx[storage_idx]),delete_prj,0);

    if(recv_buf!= NULL) ruc_buf_freeBuffer(recv_buf);       
    rozofs_tx_free_from_ptr(this);
    severe("Cannot get the pointer to the receive buffer");

    rozofs_storcli_write_reply_error(working_ctx_p,error);
    /*
    ** release the root transaction context
    */
    STORCLI_STOP_NORTH_PROF(working_ctx_p,delete,0);
    rozofs_storcli_release_context(working_ctx_p);  
    return;
        
wait_more_projection:    
    /*
    ** need to wait for some other delete transaction responses
    ** 
    */
    if(recv_buf!= NULL) ruc_buf_freeBuffer(recv_buf);           
    rozofs_tx_free_from_ptr(this);
    return;
}


