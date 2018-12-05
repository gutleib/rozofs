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
 
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/core/ruc_sockCtl_api.h>
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/core/north_lbg_api.h>
#include <rozofs/rpc/rozofs_rpc_util.h>
#include "rozofs_rpc_non_blocking_generic.h"
 

rozofs_rpc_ctx_t *rozofs_rpc_ctx_freeListHead;  /**< head of list of the free context  */
rozofs_rpc_ctx_t rozofs_rpc_ctx_activeListHead;  /**< list of the active context     */

uint32_t    rozofs_rpc_ctx_count;           /**< Max number of contexts    */
uint32_t    rozofs_rpc_ctx_allocated;      /**< current number of allocated context        */
rozofs_rpc_ctx_t *rozofs_rpc_ctx_pfirst;  /**< pointer to the first context of the pool */
uint64_t  rozofs_rpc_global_object_index = 0;



void rozofs_rpc_generic_reply_cbk(void *this,void *param);

static char    myBuf[UMA_DBG_MAX_SEND_SIZE];

/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void rozofs_rpc_debug_show(uint32_t tcpRef, void *bufRef) {
  char           *pChar=myBuf;

  pChar += sprintf(pChar,"number of transaction contexts (initial/allocated) : %u/%u\n",rozofs_rpc_ctx_count,rozofs_rpc_ctx_allocated);

  if (bufRef != NULL) uma_dbg_send(tcpRef,bufRef,TRUE,myBuf);
  else printf("%s",myBuf);

}
/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void rozofs_rpc_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
  rozofs_rpc_debug_show(tcpRef,bufRef);
}


/*__________________________________________________________________________
  Register to the debug SWBB
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void rozofs_rpc_debug_init() {
  uma_dbg_addTopic("gen_rpc_ctx", rozofs_rpc_debug); 
}


/*
**  END OF DEBUG
*/


/*-----------------------------------------------
**   rozofs_rpc_getObjCtx_p

** based on the object index, that function
** returns the pointer to the object context.
**
** That function may fails if the index is
** not a Transaction context index type.
**
@param     : MS index
@retval   : NULL if error

*/

rozofs_rpc_ctx_t *rozofs_rpc_getObjCtx_p(uint32_t object_index)
{
   uint32_t index;
   rozofs_rpc_ctx_t *p;

   /*
   **  Get the pointer to the context
   */
   index = object_index & RUC_OBJ_MASK_OBJ_IDX; 
   if ( index >= rozofs_rpc_ctx_count)
   {
      /*
      ** the MS index is out of range
      */
      severe( "rozofs_rpc_getObjCtx_p(%d): index is out of range, index max is %d",index,rozofs_rpc_ctx_count );   
     return (rozofs_rpc_ctx_t*)NULL;
   }
   p = (rozofs_rpc_ctx_t*)ruc_objGetRefFromIdx((ruc_obj_desc_t*)rozofs_rpc_ctx_freeListHead,
                                       index);
   return ((rozofs_rpc_ctx_t*)p);
}

/*-----------------------------------------------
**   rozofs_rpc_getObjCtx_ref

** based on the object index, that function
** returns the pointer to the object context.
**
** That function may fails if the index is
** not a Transaction context index type.
**
@param     : MS index
@retval   :-1 out of range

*/

uint32_t rozofs_rpc_getObjCtx_ref(rozofs_rpc_ctx_t *p)
{
   uint32_t index;
   index = (uint32_t) ( p - rozofs_rpc_ctx_pfirst);
   index = index/sizeof(rozofs_rpc_ctx_t);

   if ( index >= rozofs_rpc_ctx_count)
   {
      /*
      ** the MS index is out of range
      */
      severe( "rozofs_rpc_getObjCtx_p(%d): index is out of range, index max is %d",index,rozofs_rpc_ctx_count );   
     return (uint32_t) -1;
   }
;
   return index;
}




/*
**____________________________________________________
*/
/**
   rozofs_rpc_init

  initialize the Transaction management module

@param     : NONE
@retval   none   :
*/
void rozofs_rpc_init()
{   
   rozofs_rpc_ctx_pfirst = (rozofs_rpc_ctx_t*)NULL;

   rozofs_rpc_ctx_allocated = 0;
   rozofs_rpc_ctx_count = 0;
}

/*
**____________________________________________________
*/
/**
   rozofs_rpc_ctxInit

  create the transaction context pool

@param     : pointer to the Transaction context
@retval   : none
*/
void  rozofs_rpc_ctxInit(rozofs_rpc_ctx_t *p,uint8_t creation)
{

  p->integrity  = -1;     /* the value of this field is incremented at 
					      each MS ctx allocation */
                          
  p->response_cbk    = NULL;
  p->user_ref        = NULL;
  p->xdr_result      = NULL;  
  p->ret_len         = 0;
  p->ret_p           = NULL;
  p->profiler_probe = NULL;
  p->profiler_time  = 0;
 
}

/*
**__________________________________________________________________________
*/
/**
  allocate a  context to handle a client read/write transaction

  @param     : none
  @retval <>NULL pointer to the allocated context
  @retval NULL out of free context
*/
rozofs_rpc_ctx_t *rozofs_rpc_req_alloc()
{
   rozofs_rpc_ctx_t *p;
   

   /*
   **  Get the first free context
   */
   if ((p =(rozofs_rpc_ctx_t*)ruc_objGetFirst((ruc_obj_desc_t*)rozofs_rpc_ctx_freeListHead))
           == (rozofs_rpc_ctx_t*)NULL)
   {
     /*
     ** out of Transaction context descriptor try to free some MS
     ** context that are out of date 
     */
     severe( "not able to get a tx context" );
     return NULL;
   }
   /*
   **  reinitilisation of the context
   */
   rozofs_rpc_ctxInit(p,FALSE);   

   /*
   ** remove it for the linked list
   */
   rozofs_rpc_ctx_allocated++;
   p->free = FALSE;   
   ruc_objRemove((ruc_obj_desc_t*)p);
 
   return p;
}

/*
**__________________________________________________________________________
*/
/**
* release a read/write context that has been use for either a read or write operation

  @param : pointer to the context
  
  @retval <>NULL pointer to the allocated context
  @retval NULL out of free context
*/
void rozofs_rpc_req_free(rozofs_rpc_ctx_t *ctx_p)
{
  /*
  ** remove it from any other list and re-insert it on the free list
  */
  ruc_objRemove((ruc_obj_desc_t*) ctx_p);
   
   /*
   **  insert it in the free list
   */
   rozofs_rpc_ctx_allocated--;
   ctx_p->free = TRUE;
   /*
   ** update the profiler
   */
   STOP_PROFILING_RPC_GENERIC(ctx_p);
   
   ruc_objInsertTail((ruc_obj_desc_t*)rozofs_rpc_ctx_freeListHead,
                     (ruc_obj_desc_t*) ctx_p);
                     
}
/*
**______________________________________________________________________________
*/
/**
* ROZOFS Generic RPC Request transaction in non-blocking mode

 That service initiates RPC call towards the destination referenced by its associated load balancing group
 WHen the transaction is started, the application will received the response thanks the provided callback
 
 The first parameter is a user dependent reference and the second pointer is the pointer to the decoded
 area.
 In case of decoding error, transmission error, the second pointer is NULL and errno is asserted with the
 error.
 
 The array provided for decoding the response might be a static variable within  the user context or
 can be an allocated array. If that array has be allocated by the application it is up to the application
 to release it

 @param lbg_id     : reference of the load balancing group of the exportd
 @param prog       : program
 @param vers       : program version
 @param opcode     : metadata opcode
 @param probe     : statistics counter associated with opcode (might be NULL)
 @param encode_fct : encoding function
 @param msg2encode_p     : pointer to the message to encode
 @param decode_fct  : xdr function for message decoding
 @param ret: pointer to the array that is used for message decoding
 @parem ret_len : length of the array used for decoding
 @param recv_cbk   : receive callback function (for interpretation of the rpc result
 @param ctx_p      : pointer to the user context
 
 @retval 0 on success;
 @retval -1 on error,, errno contains the cause
 */
int rozofs_rpc_non_blocking_req_send (int lbg_id,uint32_t prog,uint32_t vers,
                                      int opcode,uint64_t *probe,
                                       xdrproc_t encode_fct,void *msg2encode_p,
                                       xdrproc_t decode_fct,void *ret,int ret_len,
                                       sys_recv_pf_t recv_cbk,void *ctx_p) 
{
    DEBUG_FUNCTION;
   
    uint8_t           *arg_p;
    uint32_t          *header_size_p;
    rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;
    void              *xmit_buf = NULL;
    int               bufsize;
    int               status;
    int               position;
    XDR               xdrs;    
	struct rpc_msg   call_msg;
    uint32_t         null_val = 0;

    rozofs_rpc_ctx_t *rpc_ctx_p = NULL;

    /*
    ** allocate a rpc context
    */
    rpc_ctx_p = (rozofs_rpc_ctx_t*)rozofs_rpc_req_alloc();  
    if (rpc_ctx_p == NULL) 
    {
       /*
       ** out of context
       */
       errno = ENOMEM;
       goto error;
    } 
    /*
    ** save the rpc parameter of the caller
    */
    rpc_ctx_p->user_ref   = ctx_p;       /* save the user reference of the caller   */    
    rpc_ctx_p->xdr_result = decode_fct;  /* save the decoding procedure  */   
    rpc_ctx_p->response_cbk = recv_cbk ;
    rpc_ctx_p->ret_len  = ret_len;
    rpc_ctx_p->ret_p  = ret;
    START_PROFILING_RPC_GENERIC(rpc_ctx_p,probe);   
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
    ** allocate an xmit buffer
    */  
    xmit_buf = ruc_buf_getBuffer(ROZOFS_TX_LARGE_TX_POOL);
    if (xmit_buf == NULL)
    {
      /*
      ** something rotten here, we exit we an error
      ** without activating the FSM
      */
      TX_STATS(ROZOFS_TX_NO_BUFFER_ERROR);
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
    bufsize = ruc_buf_getMaxPayloadLen(xmit_buf);
    bufsize -= sizeof(uint32_t);
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
    ** Now get the current length and fill the header of the message
    */
    position = XDR_GETPOS(&xdrs);
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
    rozofs_tx_ctx_p->recv_cbk   = rozofs_rpc_generic_reply_cbk;
    rozofs_tx_ctx_p->user_param = rpc_ctx_p;    
    /*
    ** now send the message
    */
    status = north_lbg_send(lbg_id,xmit_buf);
    if (status < 0)
    {
       TX_STATS(ROZOFS_TX_SEND_ERROR);
       errno = EFAULT;
      goto error;  
    }
    TX_STATS(ROZOFS_TX_SEND);

    /*
    ** OK, so now finish by starting the guard timer
    */
    rozofs_tx_start_timer(rozofs_tx_ctx_p, 25);
    return 0;  
    
  error:
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);
    if (rpc_ctx_p != NULL) rozofs_rpc_req_free(rpc_ctx_p);
    return -1;    
}

/*
**__________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rpc context
 
 @return none
 */
void rozofs_rpc_generic_reply_cbk(void *this,void *param) 
{
   struct rpc_msg  rpc_reply;
   rozofs_rpc_ctx_t *rpc_ctx_p = (rozofs_rpc_ctx_t*) param;
   void *ret;  
   int   ret_len;
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   xdrproc_t decode_proc;
   int xdr_free_done;
   rozofs_tx_ctx_t      *rozofs_tx_ctx_p = NULL;
     
   /*
   ** get the decoding function from the user rpc context
   */
   decode_proc    = rpc_ctx_p->xdr_result;
   xdr_free_done  = 0;
   /*
   ** get the memory area in which we must decode the info
   */
   ret = rpc_ctx_p->ret_p;
   ret_len = rpc_ctx_p->ret_len;
   if ((ret == NULL) || (ret_len== 0))
   {
     severe("bad argurment ret %p rel_len %d",ret,ret_len);
     errno = EINVAL;
     goto error;
   }
   /*
   ** clear the memory used for decoding the reply
   */
   memset(ret,0,ret_len);

   rpc_reply.acpted_rply.ar_results.proc = NULL;
    /*
    ** get the pointer to the transaction context:
    ** it is required to get the information related to the receive buffer
    */
    rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;     
    /*    
    ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
    */
    status = rozofs_tx_get_status(this);
    if (status < 0)
    {
       /*
       ** something wrong happened
       */
       errno = rozofs_tx_get_errno(this);  
       goto error; 
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
       goto error;         
    }
    payload  = (uint8_t*) ruc_buf_getPayload(recv_buf);
    payload += sizeof(uint32_t); /* skip length*/
    /*
    ** OK now decode the received message
    */
    bufsize = (int) ruc_buf_getPayloadLen(recv_buf);
    bufsize -= sizeof(uint32_t);    
    xdrmem_create(&xdrs,(char*)payload,bufsize,XDR_DECODE);
    /*
    ** decode the rpc part
    */
    if (rozofs_xdr_replymsg(&xdrs,&rpc_reply) != TRUE)
    {
     TX_STATS(ROZOFS_TX_DECODING_ERROR);
     errno = EPROTO;
     goto error;
    }
    /*
    ** ok now call the procedure to encode the message
    */
    if (decode_proc(&xdrs,ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       goto error;
    } 
    goto out;
    
error:
    /*
    ** release the received buffer if one was present
    */
    if (recv_buf != NULL)
    {
       ruc_buf_freeBuffer(recv_buf);
       recv_buf = NULL;
    }

out:
    /*
    ** release the transaction context and the gateway context
    */
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    /*
    ** call the user callback for returned parameter interpretation: caution recv_buf might be NULL!!
    */
    if (recv_buf == NULL)
      (*rpc_ctx_p->response_cbk)(rpc_ctx_p->user_ref,NULL); 
    else
      (*rpc_ctx_p->response_cbk)(rpc_ctx_p->user_ref,ret);      
    /*
    ** do not forget to release data allocated for xdr decoding
    */
    if (xdr_free_done == 0) xdr_free((xdrproc_t) decode_proc, (char *) ret);
    
    if (recv_buf != NULL)   ruc_buf_freeBuffer(recv_buf);
    if (rpc_ctx_p != NULL) rozofs_rpc_req_free(rpc_ctx_p);
    return;
}
/*
**__________________________________________________________________________
*/
/**
   rozofs_rpc_module_init

  create the Transaction context pool


@retval   : RUC_OK : done
@retval          RUC_NOK : out of memory
*/
uint32_t rozofs_rpc_module_init()
{
   rozofs_rpc_ctx_t *p;
   uint32_t idxCur;
   ruc_obj_desc_t *pnext;
   uint32_t ret = RUC_OK;
   

   
   rozofs_rpc_ctx_allocated = 0;
   rozofs_rpc_ctx_count = ROZOFS_RPC_GENERIC_MAX_REQ_CTX;
 
   rozofs_rpc_ctx_freeListHead = (rozofs_rpc_ctx_t*)NULL;

   /*
   **  create the active list
   */
   ruc_listHdrInit((ruc_obj_desc_t*)&rozofs_rpc_ctx_activeListHead);    

   /*
   ** create the  rpc context pool
   */
   rozofs_rpc_ctx_freeListHead = (rozofs_rpc_ctx_t*)ruc_listCreate(rozofs_rpc_ctx_count,sizeof(rozofs_rpc_ctx_t));
   if (rozofs_rpc_ctx_freeListHead == (rozofs_rpc_ctx_t*)NULL)
   {
     /* 
     **  out of memory
     */

     RUC_WARNING(rozofs_rpc_ctx_count*sizeof(rozofs_rpc_ctx_t));
     return RUC_NOK;
   }
   /*
   ** store the pointer to the first context
   */
   rozofs_rpc_ctx_pfirst = rozofs_rpc_ctx_freeListHead;

   /*
   **  initialize each entry of the free list
   */
   idxCur = 0;
   pnext = (ruc_obj_desc_t*)NULL;
   while ((p = (rozofs_rpc_ctx_t*)ruc_objGetNext((ruc_obj_desc_t*)rozofs_rpc_ctx_freeListHead,
                                        &pnext))
               !=(rozofs_rpc_ctx_t*)NULL) 
   {
  
      p->index = idxCur;
      p->free  = TRUE;
      rozofs_rpc_ctxInit(p,TRUE);
      idxCur++;
   } 
   return ret;
}

