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
#include <stdlib.h>
#include <stddef.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/ruc_buffer_api.h>
#include <rozofs/core/rozofs_tx_common.h>
#include <rozofs/rpc/rozofs_rpc_util.h>
#include <rozofs/core/rozofs_rpc_non_blocking_generic_srv.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/ruc_buffer_debug.h>
typedef struct _rozorpc_profiler_t
{
     uint64_t forward_reply[2];
} rozorpc_profiler_t;


rozorpc_srv_ctx_t *rozorpc_srv_ctx_freeListHead;  /**< head of list of the free context  */
rozorpc_srv_ctx_t rozorpc_srv_ctx_activeListHead;  /**< list of the active context     */

uint32_t    rozorpc_srv_ctx_count;           /**< Max number of contexts    */
uint32_t    rozorpc_srv_ctx_allocated;      /**< current number of allocated context        */
rozorpc_srv_ctx_t *rozorpc_srv_ctx_pfirst;  /**< pointer to the first context of the pool */
uint64_t  rozorpc_srv_global_object_index = 0;


static DEFINE_PROFILING(rozorpc_profiler_t);

uint64_t rozorpc_srv_stats[ROZORPC_SRV_COUNTER_MAX];


/**
* Buffers information
*/
int rozorpc_srv_north_small_buf_count= 0;
int rozorpc_srv_north_small_buf_sz= 0;
int rozorpc_srv_north_large_buf_count= 0;
int rozorpc_srv_north_large_buf_sz= 0;
int rozorpc_srv_south_small_buf_count= 0;
int rozorpc_srv_south_small_buf_sz= 0;
int rozorpc_srv_south_large_buf_count= 0;
int rozorpc_srv_south_large_buf_sz= 0;

#ifdef SRV_RPC_POOL
void *rozorpc_srv_pool[_ROZORPC_SRV_MAX_POOL];
#endif

uint32_t rozorpc_srv_seqnum = 1;


#define MICROLONG(time) ((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)
#define ROZORPC_SRV_DEBUG_TOPIC      "rpc_resources"
static char    myBuf[UMA_DBG_MAX_SEND_SIZE];



#define SHOW_PROFILER_PROBE(probe) pChar += sprintf(pChar," %-14s | %15llu | %9llu | %18llu |\n",\
                    #probe,\
                    (long long unsigned int)gprofiler->probe[P_COUNT],\
                    (long long unsigned int)(gprofiler->probe[P_COUNT]?gprofiler->probe[P_ELAPSE]/gprofiler->probe[P_COUNT]:0),\
                    (long long unsigned int)gprofiler->probe[P_ELAPSE]);

/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void rozorpc_srv_debug_show(uint32_t tcpRef, void *bufRef) {
  char           *pChar=myBuf;

  pChar += sprintf(pChar,"number of rpc contexts (initial/allocated) : %u/%u\n",rozorpc_srv_ctx_count,rozorpc_srv_ctx_allocated);
  pChar += sprintf(pChar,"Statistics\n");
  pChar += sprintf(pChar,"SEND           : %10llu\n",(unsigned long long int)rozorpc_srv_stats[ROZORPC_SRV_SEND]);  
  pChar += sprintf(pChar,"SEND_ERR       : %10llu\n",(unsigned long long int)rozorpc_srv_stats[ROZORPC_SRV_SEND_ERROR]);  
  pChar += sprintf(pChar,"RECV_OK        : %10llu\n",(unsigned long long int)rozorpc_srv_stats[ROZORPC_SRV_RECV_OK]);  
  pChar += sprintf(pChar,"RECV_OUT_SEQ   : %10llu\n",(unsigned long long int)rozorpc_srv_stats[ROZORPC_SRV_RECV_OUT_SEQ]);  
  pChar += sprintf(pChar,"ENCODING_ERROR : %10llu\n",(unsigned long long int)rozorpc_srv_stats[ROZORPC_SRV_ENCODING_ERROR]);  
  pChar += sprintf(pChar,"DECODING_ERROR : %10llu\n",(unsigned long long int)rozorpc_srv_stats[ROZORPC_SRV_DECODING_ERROR]);  
  pChar += sprintf(pChar,"NO_CTX_ERROR   : %10llu\n",(unsigned long long int)rozorpc_srv_stats[ROZORPC_SRV_NO_CTX_ERROR]);  
  pChar += sprintf(pChar,"NO_BUFFER_ERROR: %10llu\n",(unsigned long long int)rozorpc_srv_stats[ROZORPC_SRV_NO_BUFFER_ERROR]);  
  pChar += sprintf(pChar,"\n");
  pChar += sprintf(pChar, "   procedure    |     count       |  time(us) | cumulated time(us) |\n");
  pChar += sprintf(pChar, "----------------+-----------------+-----------+--------------------+\n");
  SHOW_PROFILER_PROBE(forward_reply);
  gprofiler->forward_reply[0] = 0;
  gprofiler->forward_reply[1] = 0;
  pChar += sprintf(pChar,"\n");

#ifdef SRV_RPC_POOL 
  pChar += sprintf(pChar,"Buffer Pool (name[size] :initial/current\n");
  pChar += sprintf(pChar,"North interface Buffers            \n");  
  pChar += sprintf(pChar,"  small[%6d]  : %6d/%d\n",rozorpc_srv_north_small_buf_sz,rozorpc_srv_north_small_buf_count,
                                                         ruc_buf_getFreeBufferCount(ROZORPC_SRV_NORTH_SMALL_POOL)); 
  pChar += sprintf(pChar,"  large[%6d]  : %6d/%d\n",rozorpc_srv_north_large_buf_sz,rozorpc_srv_north_large_buf_count, 
                                                         ruc_buf_getFreeBufferCount(ROZORPC_SRV_NORTH_LARGE_POOL)); 
  pChar += sprintf(pChar,"South interface Buffers            \n");  
  pChar += sprintf(pChar,"  small[%6d]  : %6d/%d\n",rozorpc_srv_south_small_buf_sz,rozorpc_srv_south_small_buf_count, 
                                                         ruc_buf_getFreeBufferCount(ROZORPC_SRV_SOUTH_SMALL_POOL)); 
  pChar += sprintf(pChar,"  large[%6d]  : %6d/%d\n",rozorpc_srv_south_large_buf_sz,rozorpc_srv_south_large_buf_count,
                                                         ruc_buf_getFreeBufferCount(ROZORPC_SRV_SOUTH_LARGE_POOL)); 
#endif 
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
void rozorpc_srv_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
  rozorpc_srv_debug_show(tcpRef,bufRef);
  /*
  ** Reset is requested
  */
  if ((argv[1] != NULL) && strcmp(argv[1],"reset")==0) {
    memset(rozorpc_srv_stats,0, sizeof(rozorpc_srv_stats));
  }    
}


/*__________________________________________________________________________
  Register to the debug SWBB
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void rozorpc_srv_debug_init() {
  uma_dbg_addTopic_option(ROZORPC_SRV_DEBUG_TOPIC, rozorpc_srv_debug,UMA_DBG_OPTION_RESET); 
}


/*
**  END OF DEBUG
*/



/*-----------------------------------------------
**   rozorpc_srv_getObjCtx_p

** based on the object index, that function
** returns the pointer to the object context.
**
** That function may fails if the index is
** not a Transaction context index type.
**
@param     : MS index
@retval   : NULL if error

*/

rozorpc_srv_ctx_t *rozorpc_srv_getObjCtx_p(uint32_t object_index)
{
   uint32_t index;
   rozorpc_srv_ctx_t *p;

   /*
   **  Get the pointer to the context
   */
   index = object_index & RUC_OBJ_MASK_OBJ_IDX; 
   if ( index >= rozorpc_srv_ctx_count)
   {
      /*
      ** the MS index is out of range
      */
      severe( "rozorpc_srv_getObjCtx_p(%d): index is out of range, index max is %d",index,rozorpc_srv_ctx_count );   
     return (rozorpc_srv_ctx_t*)NULL;
   }
   p = (rozorpc_srv_ctx_t*)ruc_objGetRefFromIdx((ruc_obj_desc_t*)rozorpc_srv_ctx_freeListHead,
                                       index);
   return ((rozorpc_srv_ctx_t*)p);
}

/*-----------------------------------------------
**   rozorpc_srv_getObjCtx_ref

** based on the object index, that function
** returns the pointer to the object context.
**
** That function may fails if the index is
** not a Transaction context index type.
**
@param     : MS index
@retval   :-1 out of range

*/

uint32_t rozorpc_srv_getObjCtx_ref(rozorpc_srv_ctx_t *p)
{
   uint32_t index;
   index = (uint32_t) ( p - rozorpc_srv_ctx_pfirst);
   index = index/sizeof(rozorpc_srv_ctx_t);

   if ( index >= rozorpc_srv_ctx_count)
   {
      /*
      ** the MS index is out of range
      */
      severe( "rozorpc_srv_getObjCtx_p(%d): index is out of range, index max is %d",index,rozorpc_srv_ctx_count );   
     return (uint32_t) -1;
   }
;
   return index;
}




/*
**____________________________________________________
*/
/**
   rozorpc_srv_init

  initialize the Transaction management module

@param     : NONE
@retval   none   :
*/
void rozorpc_srv_init()
{   
   rozorpc_srv_ctx_pfirst = (rozorpc_srv_ctx_t*)NULL;

   rozorpc_srv_ctx_allocated = 0;
   rozorpc_srv_ctx_count = 0;
}

/*
**____________________________________________________
*/
/**
   rozorpc_srv_ctxInit

  create the transaction context pool

@param     : pointer to the Transaction context
@retval   : none
*/
void  rozorpc_srv_ctxInit(rozorpc_srv_ctx_t *p,uint8_t creation)
{

  p->integrity  = -1;     /* the value of this field is incremented at 
					      each MS ctx allocation */
                          
  p->recv_buf     = NULL;
  p->socketRef    = -1;
  p->xmitBuf     = NULL;  
  p->opcode      = 0;
  p->src_transaction_id = 0;
  p->profiler_probe = NULL;
  p->profiler_time  = 0;
  p->decoded_arg = NULL;
  p->arg_decoder = NULL;
  list_init(&p->list);
 
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
rozorpc_srv_ctx_t *rozorpc_srv_alloc_context()
{
   rozorpc_srv_ctx_t *p;
   

   /*
   **  Get the first free context
   */
   if ((p =(rozorpc_srv_ctx_t*)ruc_objGetFirst((ruc_obj_desc_t*)rozorpc_srv_ctx_freeListHead))
           == (rozorpc_srv_ctx_t*)NULL)
   {
     /*
     ** out of Transaction context descriptor try to free some MS
     ** context that are out of date 
     */
     ROZORPC_SRV_STATS(ROZORPC_SRV_NO_CTX_ERROR);
     severe( "not able to get a tx context" );
     return NULL;
   }
   /*
   **  reinitilisation of the context
   */
   rozorpc_srv_ctxInit(p,FALSE);   

   /*
   ** remove it for the linked list
   */
   rozorpc_srv_ctx_allocated++;
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
void rozorpc_srv_release_context(rozorpc_srv_ctx_t *ctx_p)
{

  /*
  ** release the buffer that was carrying the initial request
  */
  if (ctx_p->recv_buf != NULL) 
  {
    ruc_buf_freeBuffer(ctx_p->recv_buf);
    ctx_p->recv_buf = NULL;
  }
  ctx_p->socketRef = -1;
  if (ctx_p->xmitBuf != NULL) 
  {
    ruc_buf_freeBuffer(ctx_p->xmitBuf);
    ctx_p->xmitBuf = NULL;
  }

  if ((ctx_p->arg_decoder != NULL) && (ctx_p->decoded_arg)) {
    xdr_free(ctx_p->arg_decoder, (caddr_t) ruc_buf_getPayload(ctx_p->decoded_arg));        
    ctx_p->arg_decoder = NULL;
  }
  if(ctx_p->decoded_arg) {
     ruc_buf_freeBuffer(ctx_p->decoded_arg);
     ctx_p->decoded_arg = NULL;
  }
  
  list_remove(&ctx_p->list);

  /*
  ** remove it from any other list and re-insert it on the free list
  */
  ruc_objRemove((ruc_obj_desc_t*) ctx_p);
   
   /*
   **  insert it in the free list
   */
   rozorpc_srv_ctx_allocated--;
   ctx_p->free = TRUE;
   /*
   ** update the profiler
   */
   STOP_PROFILING_ROZORPC_SRV(ctx_p);
   
   ruc_objInsert((ruc_obj_desc_t*)rozorpc_srv_ctx_freeListHead,
                     (ruc_obj_desc_t*) ctx_p);
                     
}



/*
**__________________________________________________________________________
*/
/**
*  get the arguments of the incoming request: it is mostly a rpc decode

 @param recv_buf : ruc buffer that contains the request
 @param xdr_argument : decoding procedure
 @param argument     : pointer to the array where decoded arguments will be stored
 
 @retval TRUE on success
 @retval FALSE decoding error
*/
int rozorpc_srv_getargs (void *recv_buf,xdrproc_t xdr_argument, void *argument)
{
   XDR xdrs;
   uint32_t  msg_len;  /* length of the rpc messsage including the header length */
   uint32_t header_len;
   uint8_t  *pmsg;     /* pointer to the first available byte in the application message */
   int      len;       /* effective length of application message               */
   rozofs_rpc_call_hdr_with_sz_t *com_hdr_p;
   bool_t ret;


   /*
   ** Get the full length of the message and adjust it the the length of the applicative part (RPC header+application msg)
   */
   msg_len = ruc_buf_getPayloadLen(recv_buf);
   msg_len -=sizeof(uint32_t);   

   com_hdr_p  = (rozofs_rpc_call_hdr_with_sz_t*) ruc_buf_getPayload(recv_buf);  
   pmsg = rozofs_rpc_set_ptr_on_first_byte_after_rpc_header((char*)&com_hdr_p->hdr,&header_len);
   if (pmsg == NULL)
   {
      ROZORPC_SRV_STATS(ROZORPC_SRV_DECODING_ERROR);
     errno = EFAULT;
     return FALSE;
   }
   /*
   ** map the memory on the first applicative RPC byte available and prepare to decode:
   ** notice that we will not call XDR_FREE since the application MUST
   ** provide a pointer for storing the file handle
   */
   len = msg_len - header_len;    
   xdrmem_create(&xdrs,(char*)pmsg,len,XDR_DECODE);
   ret = (*xdr_argument)(&xdrs,argument);
   if (ret == TRUE)
   {
    ROZORPC_SRV_STATS(ROZORPC_SRV_RECV_OK);
   }
   else 
   {
     ROZORPC_SRV_STATS(ROZORPC_SRV_DECODING_ERROR);
   }
   return (int) ret;
}


/*
**__________________________________________________________________________
*/
/**
*  get the arguments of the incoming request: it is mostly a rpc decode

 @param recv_buf : ruc buffer that contains the request
 @param xdr_argument : decoding procedure
 @param argument     : pointer to the array where decoded arguments will be stored
 
 @retval TRUE on success
 @retval FALSE decoding error
*/
int rozorpc_srv_getargs_with_position (void *recv_buf,xdrproc_t xdr_argument, void *argument,int *position)
{
   XDR xdrs;
   uint32_t  msg_len;  /* length of the rpc messsage including the header length */
   uint32_t header_len;
   uint8_t  *pmsg;     /* pointer to the first available byte in the application message */
   int      len;       /* effective length of application message               */
   rozofs_rpc_call_hdr_with_sz_t *com_hdr_p;
   bool_t ret;

   if (position != NULL) *position = 0;
   /*
   ** Get the full length of the message and adjust it the the length of the applicative part (RPC header+application msg)
   */
   msg_len = ruc_buf_getPayloadLen(recv_buf);
   msg_len -=sizeof(uint32_t);   

   com_hdr_p  = (rozofs_rpc_call_hdr_with_sz_t*) ruc_buf_getPayload(recv_buf);  
   pmsg = rozofs_rpc_set_ptr_on_first_byte_after_rpc_header((char*)&com_hdr_p->hdr,&header_len);
   if (pmsg == NULL)
   {
      ROZORPC_SRV_STATS(ROZORPC_SRV_DECODING_ERROR);
     errno = EFAULT;
     return FALSE;
   }
   /*
   ** map the memory on the first applicative RPC byte available and prepare to decode:
   ** notice that we will not call XDR_FREE since the application MUST
   ** provide a pointer for storing the file handle
   */
   len = msg_len - header_len;    
   xdrmem_create(&xdrs,(char*)pmsg,len,XDR_DECODE);
   ret = (*xdr_argument)(&xdrs,argument);
   if (ret == TRUE)
   {
    // Position from begin of buffer = position in xdr + size of rpc header + size of len before rpc
    if (position != NULL) *position = XDR_GETPOS(&xdrs)+header_len+sizeof(uint32_t);
    ROZORPC_SRV_STATS(ROZORPC_SRV_RECV_OK);
   }
   else 
   {
     ROZORPC_SRV_STATS(ROZORPC_SRV_DECODING_ERROR);
   }
   return (int) ret;
}

/*
**__________________________________________________________________________
*/
/**
* send a rpc reply: the encoding function MUST be found in xdr_result 
 of the gateway context

  It is assumed that the xmitBuf MUST be found in xmitBuf field
  
  In case of a success it is up to the called function to release the xmit buffer
  
  @param p : pointer to the root transaction context used for the read
  @param arg_ret : returned argument to encode 
  
  @retval none

*/
void rozorpc_srv_forward_reply_with_extra_len (rozorpc_srv_ctx_t *p,char * arg_ret,int extra_len)
{
   int ret;
   uint8_t *pbuf;           /* pointer to the part that follows the header length */
   uint32_t *header_len_p;  /* pointer to the array that contains the length of the rpc message*/
   XDR xdrs;
   int len;

   START_PROFILING(forward_reply);
   
   if (p->xmitBuf == NULL)
   {
      ROZORPC_SRV_STATS(ROZORPC_SRV_NO_BUFFER_ERROR);
      severe("no xmit buffer");
      goto error;
   } 
    /*
    ** create xdr structure on top of the buffer that will be used for sending the response
    */
    header_len_p = (uint32_t*)ruc_buf_getPayload(p->xmitBuf); 
    pbuf = (uint8_t*) (header_len_p+1);  
    
    /* Do not forget to reset the opaque authentication part */
    memset(pbuf,0,40);          
 
    len = (int)ruc_buf_getMaxPayloadLen(p->xmitBuf);
    len -= sizeof(uint32_t);
    xdrmem_create(&xdrs,(char*)pbuf,len,XDR_ENCODE); 
    if (rozofs_encode_rpc_reply(&xdrs,(xdrproc_t)p->xdr_result,(caddr_t)arg_ret,p->src_transaction_id) != TRUE)
    {
      ROZORPC_SRV_STATS(ROZORPC_SRV_ENCODING_ERROR);
      severe("rpc reply encoding error");
      goto error;     
    }       
    /*
    ** compute the total length of the message for the rpc header and add 4 bytes more bytes for
    ** the ruc buffer to take care of the header length of the rpc message.
    */
    int total_len = xdr_getpos(&xdrs) ;
    total_len +=extra_len;
    *header_len_p = htonl(0x80000000 | total_len);
    total_len +=sizeof(uint32_t);
    ruc_buf_setPayloadLen(p->xmitBuf,total_len);
//    STOP_PROFILING(forward_reply);

    /*
    ** Get the callback for sending back the response:
    ** A callback is needed since the request for read might be local or remote
    */
    ret =  af_unix_generic_send_stream_with_idx((int)p->socketRef,p->xmitBuf);  
    if (ret == 0)
    {
      /**
      * success so remove the reference of the xmit buffer since it is up to the called
      * function to release it
      */
      ROZORPC_SRV_STATS(ROZORPC_SRV_SEND);
      p->xmitBuf = NULL;
    }
    else
    {
      ROZORPC_SRV_STATS(ROZORPC_SRV_SEND_ERROR);
    }
error:
    STOP_PROFILING(forward_reply);
    return;
}

/*
**__________________________________________________________________________
*/
/**
* send a rpc reply: the encoding function MUST be found in xdr_result 
 of the gateway context

  It is assumed that the xmitBuf MUST be found in xmitBuf field
  
  In case of a success it is up to the called function to release the xmit buffer
  
  @param p : pointer to the root transaction context used for the read
  @param arg_ret : returned argument to encode 
  
  @retval none

*/
void rozorpc_srv_forward_reply (rozorpc_srv_ctx_t *p,char * arg_ret)
{
   return rozorpc_srv_forward_reply_with_extra_len(p,arg_ret,0);
}




/*
**__________________________________________________________________________
*/
/**
   rozorpc_srv_module_init

  create the Transaction context pool


@retval   : RUC_OK : done
@retval          RUC_NOK : out of memory
*/
static uint32_t rozorpc_srv_module_initialized = 0;
uint32_t rozorpc_srv_module_init()
{
   rozorpc_srv_ctx_t *p;
   uint32_t idxCur;
   ruc_obj_desc_t *pnext;
   uint32_t ret = RUC_OK;
   
   if (rozorpc_srv_module_initialized) return RUC_OK;
   rozorpc_srv_module_initialized = 1;
   
    ALLOC_PROFILING(rozorpc_profiler_t);

    rozorpc_srv_north_small_buf_count  = ROZORPC_SRV_NORTH_MOD_INTERNAL_READ_BUF_CNT ;
    rozorpc_srv_north_small_buf_sz     = ROZORPC_SRV_NORTH_MOD_INTERNAL_READ_BUF_SZ    ;
    rozorpc_srv_north_large_buf_count  = ROZORPC_SRV_NORTH_MOD_XMIT_BUF_CNT ;
    rozorpc_srv_north_large_buf_sz     = ROZORPC_SRV_NORTH_MOD_XMIT_BUF_SZ    ;
    
    rozorpc_srv_south_small_buf_count  = ROZORPC_SRV_CNF_NO_BUF_CNT ;
    rozorpc_srv_south_small_buf_sz     = ROZORPC_SRV_CNF_NO_BUF_SZ  ;
    rozorpc_srv_south_large_buf_count  = ROZORPC_SRV_SOUTH_TX_XMIT_BUF_CNT   ;
    rozorpc_srv_south_large_buf_sz     = ROZORPC_SRV_SOUTH_TX_XMIT_BUF_SZ  ;  
   
   rozorpc_srv_ctx_allocated = 0;
   rozorpc_srv_ctx_count = ROZORPC_SRV_CTX_CNT;
 
   rozorpc_srv_ctx_freeListHead = (rozorpc_srv_ctx_t*)NULL;

   /*
   **  create the active list
   */
   ruc_listHdrInit((ruc_obj_desc_t*)&rozorpc_srv_ctx_activeListHead);    

   /*
   ** create the Read/write Transaction context pool
   */
   rozorpc_srv_ctx_freeListHead = (rozorpc_srv_ctx_t*)ruc_listCreate(rozorpc_srv_ctx_count,sizeof(rozorpc_srv_ctx_t));
   if (rozorpc_srv_ctx_freeListHead == (rozorpc_srv_ctx_t*)NULL)
   {
     /* 
     **  out of memory
     */

     RUC_WARNING(rozorpc_srv_ctx_count*sizeof(rozorpc_srv_ctx_t));
     return RUC_NOK;
   }
   /*
   ** store the pointer to the first context
   */
   rozorpc_srv_ctx_pfirst = rozorpc_srv_ctx_freeListHead;

   /*
   **  initialize each entry of the free list
   */
   idxCur = 0;
   pnext = (ruc_obj_desc_t*)NULL;
   while ((p = (rozorpc_srv_ctx_t*)ruc_objGetNext((ruc_obj_desc_t*)rozorpc_srv_ctx_freeListHead,
                                        &pnext))
               !=(rozorpc_srv_ctx_t*)NULL) 
   {
  
      p->index = idxCur;
      p->free  = TRUE;
      rozorpc_srv_ctxInit(p,TRUE);
      idxCur++;
   } 

   /*
   ** Initialize the RESUME and SUSPEND timer module: 100 ms
   */
//   com_tx_tmr_init(100,15); 
   /*
   ** Clear the statistics counter
   */
   memset(rozorpc_srv_stats,0,sizeof(uint64_t)*ROZORPC_SRV_COUNTER_MAX);
   rozorpc_srv_debug_init();
      
#ifdef SRV_RPC_POOL  
   while(1)
   {
      rozorpc_srv_pool[_ROZORPC_SRV_NORTH_SMALL_POOL]= ruc_buf_poolCreate(rozorpc_srv_north_small_buf_count,rozorpc_srv_north_small_buf_sz);
      if (rozorpc_srv_pool[_ROZORPC_SRV_NORTH_SMALL_POOL] == NULL)
      {
         ret = RUC_NOK;
         severe( "xmit ruc_buf_poolCreate(%d,%d)", rozorpc_srv_north_small_buf_count, rozorpc_srv_north_small_buf_sz ); 
         break;
      }
      ruc_buffer_debug_register_pool("rpc_rcv_small",  rozorpc_srv_pool[_ROZORPC_SRV_NORTH_SMALL_POOL]);

      rozorpc_srv_pool[_ROZORPC_SRV_NORTH_LARGE_POOL] = ruc_buf_poolCreate(rozorpc_srv_north_large_buf_count,rozorpc_srv_north_large_buf_sz);
      if (rozorpc_srv_pool[_ROZORPC_SRV_NORTH_LARGE_POOL] == NULL)
      {
         ret = RUC_NOK;
         severe( "rcv ruc_buf_poolCreate(%d,%d)", rozorpc_srv_north_large_buf_count, rozorpc_srv_north_large_buf_sz ); 
	 break;
     }
     ruc_buffer_debug_register_pool("rpc_rcv_large",  rozorpc_srv_pool[_ROZORPC_SRV_NORTH_LARGE_POOL]);

      rozorpc_srv_pool[_ROZORPC_SRV_SOUTH_SMALL_POOL]= ruc_buf_poolCreate(rozorpc_srv_south_small_buf_count,rozorpc_srv_south_small_buf_sz);
      if (rozorpc_srv_pool[_ROZORPC_SRV_SOUTH_SMALL_POOL] == NULL)
      {
         ret = RUC_NOK;
         severe( "xmit ruc_buf_poolCreate(%d,%d)", rozorpc_srv_south_small_buf_count, rozorpc_srv_south_small_buf_sz ); 
         break;
      }
      ruc_buffer_debug_register_pool("rpc_xmit_small",  rozorpc_srv_pool[_ROZORPC_SRV_SOUTH_SMALL_POOL]);

      rozorpc_srv_pool[_ROZORPC_SRV_SOUTH_LARGE_POOL] = ruc_buf_poolCreate(rozorpc_srv_south_large_buf_count,rozorpc_srv_south_large_buf_sz);
      if (rozorpc_srv_pool[_ROZORPC_SRV_SOUTH_LARGE_POOL] == NULL)
      {
         ret = RUC_NOK;
         severe( "rcv ruc_buf_poolCreate(%d,%d)", rozorpc_srv_south_large_buf_count, rozorpc_srv_south_large_buf_sz ); 
	 break;
      }
      ruc_buffer_debug_register_pool("rpc_xmit_large",  rozorpc_srv_pool[_ROZORPC_SRV_SOUTH_LARGE_POOL]);

   break;
   }
#endif
   return ret;
}


/*
**__________________________________________________________________________
*/
/**
   rozorpc_srv_module_init

  create the Transaction context pool
  
  @param count: number of transaction contexts


@retval   : RUC_OK : done
@retval          RUC_NOK : out of memory
*/
uint32_t rozorpc_srv_module_init_ctx_only(uint32_t count)
{
   rozorpc_srv_ctx_t *p;
   uint32_t idxCur;
   ruc_obj_desc_t *pnext;
   uint32_t ret = RUC_OK;
   
   if (rozorpc_srv_module_initialized) return RUC_OK;
   rozorpc_srv_module_initialized = 1;
   
   ALLOC_PROFILING(rozorpc_profiler_t); 
        
   rozorpc_srv_ctx_allocated = 0;
   rozorpc_srv_ctx_count = count;
 
   rozorpc_srv_ctx_freeListHead = (rozorpc_srv_ctx_t*)NULL;

   /*
   **  create the active list
   */
   ruc_listHdrInit((ruc_obj_desc_t*)&rozorpc_srv_ctx_activeListHead);    

   /*
   ** create the Read/write Transaction context pool
   */
   rozorpc_srv_ctx_freeListHead = (rozorpc_srv_ctx_t*)ruc_listCreate(rozorpc_srv_ctx_count,sizeof(rozorpc_srv_ctx_t));
   if (rozorpc_srv_ctx_freeListHead == (rozorpc_srv_ctx_t*)NULL)
   {
     /* 
     **  out of memory
     */

     RUC_WARNING(rozorpc_srv_ctx_count*sizeof(rozorpc_srv_ctx_t));
     return RUC_NOK;
   }
   /*
   ** store the pointer to the first context
   */
   rozorpc_srv_ctx_pfirst = rozorpc_srv_ctx_freeListHead;

   /*
   **  initialize each entry of the free list
   */
   idxCur = 0;
   pnext = (ruc_obj_desc_t*)NULL;
   while ((p = (rozorpc_srv_ctx_t*)ruc_objGetNext((ruc_obj_desc_t*)rozorpc_srv_ctx_freeListHead,
                                        &pnext))
               !=(rozorpc_srv_ctx_t*)NULL) 
   {
  
      p->index = idxCur;
      p->free  = TRUE;
      rozorpc_srv_ctxInit(p,TRUE);
      idxCur++;
   } 
   /*
   ** Clear the statistics counter
   */
   memset(rozorpc_srv_stats,0,sizeof(uint64_t)*ROZORPC_SRV_COUNTER_MAX);
   rozorpc_srv_debug_init();
      
   return ret;
}




