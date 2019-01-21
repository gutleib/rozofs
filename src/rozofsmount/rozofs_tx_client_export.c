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

#include <rozofs/rpc/storcli_proto.h>
#include <rozofs/core/rozofs_fid_string.h>

#include "rozofs_fuse_api.h"
#include "rozofs_rw_load_balancing.h"
#include "rozofs_fuse_thread_intf.h"
#include "rozofs_io_error_trc.h"



DECLARE_PROFILING(mpp_profiler_t);
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
 

 @param clt        : pointer to the client structure
 @param timeout_sec : transaction timeout
 @param prog       : program
 @param vers       : program version
 @param opcode     : metadata opcode
 @param encode_fct : encoding function
 @msg2encode_p     : pointer to the message to encode
 @param recv_cbk   : receive callback function
 @param fuse_ctx_p : pointer to the fuse context
 
 @retval 0 on success;
 @retval -1 on error,, errno contains the cause
 */
int rozofs_expgateway_send_common(int lbg_id,uint32_t prog,uint32_t vers,
                              int opcode,xdrproc_t encode_fct,void *msg2encode_p,
                              sys_recv_pf_t recv_cbk,void *fuse_ctx_p) 
{
    DEBUG_FUNCTION;
   
    uint8_t           *arg_p;
    uint32_t          *header_size_p;
    rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;
    void              *xmit_buf = NULL;
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
    ** allocate an xmit buffer
    */  
    xmit_buf = ruc_buf_getBuffer(ROZOFS_TX_SMALL_TX_POOL);
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
    bufsize = rozofs_tx_get_small_buffer_size();
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
    rozofs_tx_ctx_p->recv_cbk   = recv_cbk;
    rozofs_tx_ctx_p->user_param = fuse_ctx_p;    
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
    rozofs_tx_start_timer(rozofs_tx_ctx_p, ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM));
//    if (*tx_ptr != NULL) *tx_ptr = rozofs_tx_ctx_p;
    return 0;  
    
  error:
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);
//    if (xmit_buf != NULL) ruc_buf_freeBuffer(xmit_buf);    
    return -1;    
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
 

 @param eid        : export id
 @param fid        : unique file id (directory, regular file, etc...)
 @param prog       : program
 @param vers       : program version
 @param opcode     : metadata opcode
 @param encode_fct : encoding function
 @msg2encode_p     : pointer to the message to encode
 @param recv_cbk   : receive callback function
 @param param      : parameter for call back
 
 @retval 0 on success;
 @retval -1 on error,, errno contains the cause
 */
int rozofs_expgateway_send_no_fuse_ctx(uint32_t eid,fid_t fid,uint32_t prog,uint32_t vers,
                              int opcode,xdrproc_t encode_fct,void *msg2encode_p,
                              sys_recv_pf_t recv_cbk,void *param) 
{
    DEBUG_FUNCTION;
   
    uint8_t           *arg_p;
    uint32_t          *header_size_p;
    rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;
    void              *xmit_buf = NULL;
    int               bufsize;
    int               ret;
    int               position;
    XDR               xdrs;    
	struct rpc_msg   call_msg;
    uint32_t         null_val = 0;
    int lbg_id;
    expgw_tx_routing_ctx_t local_routing_ctx;
    expgw_tx_routing_ctx_t  *routing_ctx_p;
        
    routing_ctx_p = &local_routing_ctx;
 
    /*
    ** get the available load balancing group(s) for routing the request 
    */    
    ret  = expgw_get_export_routing_lbg_info(eid,fid,routing_ctx_p);
    if (ret < 0)
    {
      /*
      ** no load balancing group available
      */
      errno = EPROTO;
      goto error;    
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
       TX_STATS(ROZOFS_TX_NO_CTX_ERROR);
       errno = ENOMEM;
       goto error;
    }    
    /*
    ** allocate an xmit buffer
    */  
    xmit_buf = ruc_buf_getBuffer(ROZOFS_TX_SMALL_TX_POOL);
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
    ** The system attempts first to forward the message toward load balancing group
    ** of an export gateway and then to the master export if the load balancing group
    ** of the export gateway is not available
    */
    lbg_id = expgw_routing_get_next(routing_ctx_p,xmit_buf);
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
    bufsize = rozofs_tx_get_small_buffer_size();
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
    rozofs_tx_ctx_p->recv_cbk   = recv_cbk;
    rozofs_tx_ctx_p->user_param = param;    
    /*
    ** now send the message
    */
reloop:
    ret = north_lbg_send(lbg_id,xmit_buf);
    if (ret < 0)
    {
       TX_STATS(ROZOFS_TX_SEND_ERROR);
       /*
       ** attempt to get the next available load balancing group
       */
       lbg_id = expgw_routing_get_next(routing_ctx_p,xmit_buf);
       if (lbg_id >= 0) goto reloop;
       
       errno = EFAULT;
      goto error;  
    }
    TX_STATS(ROZOFS_TX_SEND);

    /*
    ** OK, so now finish by starting the guard timer
    */
    if (opcode == EP_STATFS) {
      uint32_t tmr_val;
      /* df must give a response (even negative) in less than 2 seconds !!! */
      tmr_val = ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM);
      if (tmr_val > 2) tmr_val = 2; 
      rozofs_tx_start_timer(rozofs_tx_ctx_p, tmr_val);    
    }
    else {
      rozofs_tx_start_timer(rozofs_tx_ctx_p, ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM));
    }  
    return 0;  
    
  error:
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);
    return -1;    
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
 

 @param eid        : export id
 @param fid        : unique file id (directory, regular file, etc...)
 @param prog       : program
 @param vers       : program version
 @param opcode     : metadata opcode
 @param encode_fct : encoding function
 @msg2encode_p     : pointer to the message to encode
 @param recv_cbk   : receive callback function
 @param fuse_buffer_ctx_p : pointer to the fuse context
 
 @retval 0 on success;
 @retval -1 on error,, errno contains the cause
 */
int rozofs_expgateway_send_routing_common(uint32_t eid,fid_t fid,uint32_t prog,uint32_t vers,
                              int opcode,xdrproc_t encode_fct,void *msg2encode_p,
                              sys_recv_pf_t recv_cbk,void *fuse_buffer_ctx_p) 
{
    DEBUG_FUNCTION;
   
    uint8_t           *arg_p;
    uint32_t          *header_size_p;
    rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;
    void              *xmit_buf = NULL;
    int               bufsize;
    int               ret;
    int               position;
    XDR               xdrs;    
	struct rpc_msg   call_msg;
    uint32_t         null_val = 0;
    int lbg_id;
    expgw_tx_routing_ctx_t local_routing_ctx;
    expgw_tx_routing_ctx_t  *routing_ctx_p;
    
    rozofs_fuse_save_ctx_t *fuse_ctx_p=NULL;
    
    /*
    ** Retrieve fuse context when a buffer is given
    */
    fuse_ctx_p =  NULL;	
    if (fuse_buffer_ctx_p != NULL) {
      if (ruc_buf_checkBuffer(fuse_buffer_ctx_p)) {
        GET_FUSE_CTX_P(fuse_ctx_p,fuse_buffer_ctx_p);
      }	
    }  
	
    if (fuse_ctx_p != NULL) {    
      routing_ctx_p = &fuse_ctx_p->expgw_routing_ctx ;
    }
    else {
      routing_ctx_p = &local_routing_ctx;
    }  
    /*
    ** get the available load balancing group(s) for routing the request 
    */    
    ret  = expgw_get_export_routing_lbg_info(eid,fid,routing_ctx_p);
    if (ret < 0)
    {
      /*
      ** no load balancing group available
      */
      errno = EPROTO;
      goto error;    
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
       TX_STATS(ROZOFS_TX_NO_CTX_ERROR);
       errno = ENOMEM;
       goto error;
    }    
    /*
    ** allocate an xmit buffer
    */  
    xmit_buf = ruc_buf_getBuffer(ROZOFS_TX_SMALL_TX_POOL);
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
    ** The system attempts first to forward the message toward load balancing group
    ** of an export gateway and then to the master export if the load balancing group
    ** of the export gateway is not available
    */
    lbg_id = expgw_routing_get_next(routing_ctx_p,xmit_buf);
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
    bufsize = rozofs_tx_get_small_buffer_size();
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
    rozofs_tx_ctx_p->recv_cbk   = recv_cbk;
    rozofs_tx_ctx_p->user_param = fuse_buffer_ctx_p;    
    /*
    ** now send the message
    */
reloop:
    ret = north_lbg_send(lbg_id,xmit_buf);
    if (ret < 0)
    {
       TX_STATS(ROZOFS_TX_SEND_ERROR);
       /*
       ** attempt to get the next available load balancing group
       */
       lbg_id = expgw_routing_get_next(routing_ctx_p,xmit_buf);
       if (lbg_id >= 0) goto reloop;
       
       errno = EFAULT;
      goto error;  
    }
    TX_STATS(ROZOFS_TX_SEND);

    /*
    ** OK, so now finish by starting the guard timer
    */
    if (opcode == EP_STATFS) {
      uint32_t tmr_val;
      /* df must give a response (even negative) in less than 2 seconds !!! */
      tmr_val = ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM);
      if (tmr_val > 2) tmr_val = 2; 
      rozofs_tx_start_timer(rozofs_tx_ctx_p, tmr_val);    
    }
    else {
      rozofs_tx_start_timer(rozofs_tx_ctx_p, ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM));
    }  
    return 0;  
    
  error:
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);
    return -1;    
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
 

 @param rozofs_tx_ctx_p        : transaction context
 @param recv_cbk        : callback function (may be NULL)
 @param fuse_buffer_ctx_p       : buffer containing the fuse context
 @param vers       : program version
 
 @retval 0 on success;
 @retval -1 on error,, errno contains the cause
 */
int rozofs_expgateway_resend_routing_common(rozofs_tx_ctx_t *rozofs_tx_ctx_p, sys_recv_pf_t recv_cbk,void *fuse_buffer_ctx_p) 
{
    DEBUG_FUNCTION;
   
    void              *xmit_buf = NULL;
    int               ret;
    int lbg_id;    
    rozofs_fuse_save_ctx_t *fuse_ctx_p;
    
    GET_FUSE_CTX_P(fuse_ctx_p,fuse_buffer_ctx_p);    
    expgw_tx_routing_ctx_t  *routing_ctx_p = &fuse_ctx_p->expgw_routing_ctx ;


    /*
    ** get the xmit buffer from the current routing context
    */  
    xmit_buf = routing_ctx_p->xmit_buf;
    if (xmit_buf == NULL)
    {
      /*
      ** something rotten here, we exit we an error
      ** without activating the FSM
      */
      severe("rozofs_expgateway_resend_routing_common : not xmit_buf in routing context");
      TX_STATS(ROZOFS_TX_NO_BUFFER_ERROR);
      errno = ENOMEM;
      goto error;
    } 
    /*
    ** The system attempts first to forward the message toward load balancing group
    ** of an export gateway and then to the master export if the load balancing group
    ** of the export gateway is not available
    */
    lbg_id = expgw_routing_get_next(routing_ctx_p,xmit_buf);
    if (lbg_id == -1)
    {
      errno = EPROTO;
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
    rozofs_rpc_call_hdr_with_sz_t *com_hdr_p  = (rozofs_rpc_call_hdr_with_sz_t*) ruc_buf_getPayload(xmit_buf);  
    com_hdr_p->hdr.xid = ntohl(rozofs_tx_alloc_xid(rozofs_tx_ctx_p)); ; 
    /*
    ** store the receive call back and its associated parameter
    */
    if (recv_cbk != NULL) {
      rozofs_tx_ctx_p->recv_cbk   = recv_cbk;
    }
    rozofs_tx_ctx_p->user_param = fuse_buffer_ctx_p; 
    /*
    ** now send the message
    */
reloop:
    ret = north_lbg_send(lbg_id,xmit_buf);
    if (ret < 0)
    {
       TX_STATS(ROZOFS_TX_SEND_ERROR);
       /*
       ** attempt to get the next available load balancing group
       */
       lbg_id = expgw_routing_get_next(routing_ctx_p,xmit_buf);
       if (lbg_id >= 0) goto reloop;
       
       errno = EFAULT;
      goto error;  
    }
    TX_STATS(ROZOFS_TX_SEND);
    /*
    ** OK, so now finish by starting the guard timer
    */
    rozofs_tx_start_timer(rozofs_tx_ctx_p, ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM));
    return 0;  
    
  error:
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);
    return -1;    
}

/*
**__________________________________________________________________________
*/
int rozofs_storcli_wr_thread_send(int rpc_opcode,void *msg2encode_p,xdrproc_t encode_fct,
                                  sys_recv_pf_t recv_cbk,void *fuse_ctx_p,
			          int storcli_idx,fid_t fid) 			       
{
   
     rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;
    void              *xmit_buf = NULL;
    
    rozofs_fuse_wr_thread_msg_t msg_thread;
    rozofs_fuse_wr_thread_msg_t *msg_thread_p = &msg_thread;

    msg_thread_p->opcode      = ROZOFS_FUSE_WRITE_BUF;
    msg_thread_p->status      = 0;
    msg_thread_p->storcli_idx = storcli_idx;
    msg_thread_p->rozofs_fuse_cur_rcv_buf = NULL; //rozofs_fuse_cur_rcv_buf; 
    msg_thread_p->rpc_opcode  = rpc_opcode;
    msg_thread_p->encode_fct  = encode_fct;
    /*
    ** copy the RPC message to encode 
    */
    memcpy(&msg_thread_p->args,msg2encode_p,sizeof(storcli_write_arg_t));
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
    msg_thread_p->rozofs_tx_ctx_p = rozofs_tx_ctx_p;
    /*
    ** Insert this transaction so that every other following trasnactions
    ** for the same FID will follow the same path
    */
    stclbg_hash_table_insert_ctx(&rozofs_tx_ctx_p->rw_lbg,fid,storcli_idx);
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
    ** allocate a xid for the transaction 
    */
    msg_thread_p->rm_xid    = rozofs_tx_alloc_xid(rozofs_tx_ctx_p); 

    /*
    ** store the receive call back and its associated parameter
    */
    rozofs_tx_ctx_p->recv_cbk   = recv_cbk;
    rozofs_tx_ctx_p->user_param = fuse_ctx_p;    
    /*
    ** increment the number of pending request towards the storcli
    */
    rozofs_storcli_pending_req_count++;
    
    rozofs_sendto_wr_fuse_thread(msg_thread_p);
#if 0 /** not needed with IOCTL */
    /*
    ** Clear the reference of the fuse receive buffer: 
    */
    rozofs_fuse_cur_rcv_buf = NULL;
#endif
    return 0;

    
  error:
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);

    return -1;    
}

/*
**__________________________________________________________________________
*/
/**
   
   Process a write request after the copy of data from kernel to shared buffer by a write thread.
   It corresponds to the function which is called when the file has no slave inodes (multiple file)
   
   The message has been encoded by the write thread, so we just need to send the message to the selected storcli
*/   
void af_unix_fuse_write_process_response(void *msg_p)
{

    int              lbg_id;
    rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;
    void              *xmit_buf = NULL;
    void              *fuse_ctx_p;
    struct fuse_file_info  file_info;
    struct fuse_file_info  *fi = &file_info;
    int trc_idx;      
    file_t *file;
    fuse_req_t req;
    int deferred_fuse_write_response;    
    size_t size; 
    int ret;
    rozofs_fuse_rcv_buf_t *fuse_rcv_buf_p;
    
    rozofs_fuse_wr_thread_msg_t *msg = (rozofs_fuse_wr_thread_msg_t*) msg_p;
    
                
    rozofs_tx_ctx_p = msg->rozofs_tx_ctx_p;
    fuse_ctx_p = rozofs_tx_ctx_p->user_param;
    RESTORE_FUSE_PARAM(fuse_ctx_p,trc_idx);
    RESTORE_FUSE_STRUCT(fuse_ctx_p,fi,sizeof( struct fuse_file_info));            
    RESTORE_FUSE_PARAM(fuse_ctx_p,req);
    RESTORE_FUSE_PARAM(fuse_ctx_p,size);
    
    fuse_rcv_buf_p = msg->rozofs_fuse_cur_rcv_buf;
    
    file = (file_t *) (unsigned long) fi->fh;

    if (msg->status < 0)
    {
      errno = msg->errval;
      goto error;
    }
    xmit_buf = rozofs_tx_get_xmitBuf(rozofs_tx_ctx_p);
    if (xmit_buf == NULL)
    {
      /*
      ** something rotten here, we exit we an error
      ** without activating the FSM
      */
      fatal("no xmit buffer");;
      msg->errval = ENOMEM;
      goto error;
    } 
    /*
    ** Get the load balancing group reference associated with the storcli
    */
    lbg_id = storcli_lbg_get_load_balancing_reference(msg->storcli_idx);

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
    rozofs_tx_start_timer(rozofs_tx_ctx_p,ROZOFS_TMR_GET(TMR_STORCLI_PROGRAM));  
    /*
    ** Normal send, wait for the storcli response
    */
    if (file->buf_write_pending <= ROZOFS_MAX_WRITE_PENDING) {
      deferred_fuse_write_response = 0;
      SAVE_FUSE_PARAM(fuse_ctx_p,deferred_fuse_write_response);
      rozofs_trc_rsp(srv_rozofs_ll_write,(fuse_ino_t)file,file->fid,(errno==0)?0:1,trc_idx);
      fuse_reply_write(req, size);
//      write_flush_stat.non_synchroneous++;
      goto out;
    }        
    /*
    ** Maximum number of write pending is reached. 
    ** Let's differ the FUSE response until the STORCLI response
    */
//    write_flush_stat.synchroneous++;
    deferred_fuse_write_response = 1;
    SAVE_FUSE_PARAM(fuse_ctx_p,deferred_fuse_write_response);    
    STOP_PROFILING_NB(fuse_ctx_p,rozofs_ll_write);
out:
    if (fuse_rcv_buf_p != NULL) rozofs_fuse_release_rcv_buffer_pool(fuse_rcv_buf_p);
    return;  
    
error:
    file->wr_error = errno;
    rozofs_trc_rsp(srv_rozofs_ll_write,(fuse_ino_t)file,file->fid,(errno==0)?0:1,trc_idx);
    fuse_reply_err(req, errno);   
    if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);
    /*
    **  Release the fuse context and reply to the calling thread
    */
    STOP_PROFILING_NB(fuse_ctx_p,rozofs_ll_write);
    if (fuse_ctx_p != NULL) rozofs_fuse_release_saved_context(fuse_ctx_p);
    goto out;
}


/**
* API for creation a transaction towards an storcli process

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
 

 @param clt        : pointer to the client structure
 @param timeout_sec : transaction timeout
 @param prog       : program
 @param vers       : program version
 @param opcode     : metadata opcode
 @param encode_fct : encoding function
 @msg2encode_p     : pointer to the message to encode
 @param recv_cbk   : receive callback function
 @param fuse_ctx_p : pointer to the fuse context
 @param storcli_idx      : identifier of the storcli
 @param fid: file identifier: needed for the storcli load balancing context
 
 @retval 0 on success;
 @retval -1 on error,, errno contains the cause
 */

int rozofs_storcli_send_common(exportclt_t * clt,uint32_t timeout_sec,uint32_t prog,uint32_t vers,
                              int opcode,xdrproc_t encode_fct,void *msg2encode_p,
                              sys_recv_pf_t recv_cbk,void *fuse_ctx_p,
			                  int storcli_idx,fid_t fid) 			       
{
    DEBUG_FUNCTION;
   
    uint8_t           *arg_p;
    uint32_t          *header_size_p;
    rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;
    void              *xmit_buf = NULL;
    int               bufsize;
    int               ret;
    int               position;
    XDR               xdrs;    
	struct rpc_msg   call_msg;
    uint32_t         null_val = 0;
    int              lbg_id;

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
    if (common_config.storcli_read_parallel == 0)
    {
      /*
      ** Insert this transaction so that every other following trasnactions
      ** for the same FID will follow the same path
      */
      stclbg_hash_table_insert_ctx(&rozofs_tx_ctx_p->rw_lbg,fid,storcli_idx);
    }
    else
    /*
    ** Read parallel case: mainly for network with high latency
    */
    {
//#warning all is parallel when flag is asserted
      /*
      ** Write cannot follow the same behavior because of the first write when the projection does not exist on the storio side
      **  need to see how to address it in order to make it work in parallel for both cases.
      */
#if 1
      if (opcode != STORCLI_READ)
      {
        stclbg_hash_table_insert_ctx(&rozofs_tx_ctx_p->rw_lbg,fid,storcli_idx);      
      }    
#endif
    }
      
    /*
    ** Get the load balancing group reference associated with the storcli
    */
    lbg_id = storcli_lbg_get_load_balancing_reference(storcli_idx);
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
    /*
    ** check the case of the READ since, we must set the value of the xid
    ** at the top of the buffer
    */
    if ((opcode == STORCLI_READ)||(opcode == STORCLI_WRITE))
    {
       void *shared_buf_ref;
       void * kernel_fuse_write_request;
       rozofs_shared_buf_wr_hdr_t* share_wr_p;
       rozofs_shared_buf_rd_hdr_t* share_rd_p;

        RESTORE_FUSE_PARAM(fuse_ctx_p,shared_buf_ref);
        if (opcode == STORCLI_READ)
        {
           share_rd_p = (rozofs_shared_buf_rd_hdr_t*)ruc_buf_getPayload(shared_buf_ref);
           share_rd_p->cmd[0].xid = (uint32_t)call_msg.rm_xid;
	  /**
	  * copy the buffer for the case of the write
	  */
	}
	else
	{
           share_wr_p = (rozofs_shared_buf_wr_hdr_t*)ruc_buf_getPayload(shared_buf_ref);
           share_wr_p->cmd[0].xid = (uint32_t)call_msg.rm_xid;

	   storcli_write_arg_t  *wr_args = (storcli_write_arg_t*)msg2encode_p;
	   /*
	   ** get the length to copy from the sshared memory
	   */
	   int len = share_wr_p->cmd[0].write_len;
	   /*
	   ** Compute and write data offset considering 128bits alignment
	   */
	   int alignment = wr_args->off%16;
	   share_wr_p->cmd[0].offset_in_buffer = (uint32_t) alignment;
	   /*
	   ** Set pointer to the buffer start and adjust with alignment
	   */
	   uint8_t * buf_start = (uint8_t *)share_wr_p;
	   buf_start += alignment+ROZOFS_SHMEM_WRITE_PAYLOAD_OFF;
	   RESTORE_FUSE_PARAM(fuse_ctx_p,kernel_fuse_write_request);
	   /*
	   ** Check if we need to do an ioctl to get the data
	   */
	   if (kernel_fuse_write_request != NULL)
	   {
	      ioctl_big_wr_t data;

	      data.req = kernel_fuse_write_request;
              data.user_buf = buf_start;      
              data.user_bufsize = len;
	      ret = ioctl(rozofs_fuse_ctx_p->fd,5,&data); 
	      if (ret != 0)
	      {
		 errno = EIO;
        	 severe("ioctl write error %s",strerror(errno));   
		goto error;
	      } 
	   }
	   else
	   {
	     memcpy(buf_start,wr_args->data.data_val,len);	
	   }  
	}
    }
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
    rozofs_tx_ctx_p->recv_cbk   = recv_cbk;
    rozofs_tx_ctx_p->user_param = fuse_ctx_p;    
    /*
    ** now send the message
    */
//    int lbg_id = storcli_lbg_get_load_balancing_reference();
    /*
    ** increment the number of pending request towards the storcli
    */
    rozofs_storcli_pending_req_count++;
    ret = north_lbg_send(lbg_id,xmit_buf);
    if (ret < 0)
    {
       if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;
       TX_STATS(ROZOFS_TX_SEND_ERROR);
       errno = EFAULT;
      goto error;  
    }
    TX_STATS(ROZOFS_TX_SEND);

    /*
    ** OK, so now finish by starting the guard timer
    */
    rozofs_tx_start_timer(rozofs_tx_ctx_p,timeout_sec);  
//    if (*tx_ptr != NULL) *tx_ptr = rozofs_tx_ctx_p;
    return 0;  
    
  error:
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);
//    if (xmit_buf != NULL) ruc_buf_freeBuffer(xmit_buf);    
    return -1;    
}


/*
**__________________________________________________________________

      M U L T I P L  E   F I L E S    A P I
**__________________________________________________________________
*/


typedef struct _rozofs_multi_vect_t
{
  uint64_t off;    /**< offset in the slave file    */
  uint32_t len;    /**< size in bytes to transfer   */
  uint32_t byte_offset_in_shared_buf;   /**< offset where data must be copy in/out in the rozofsmount shared buffer */
  uint8_t  file_idx;  /**< index of the slave file   */
} rozofs_multi_vect_t;  

typedef struct _rozofs_iov_multi_t
{
   int nb_vectors;   /** number of vectors */
   rozofs_multi_vect_t vectors[ ROZOFS_MAX_STRIPING_FACTOR+1];
} rozofs_iov_multi_t;


/*
**__________________________________________________________________
*/
void rozofs_print_multi_vector(rozofs_iov_multi_t *vector_p,char *pbuf)
{
   int i;
   rozofs_multi_vect_t *p;   


   p = &vector_p->vectors[0];
   pbuf +=sprintf(pbuf,"+---------------+-----------+----------+--------+\n");
   pbuf +=sprintf(pbuf,"|     offset    |   length  | buf. off | f_idx  |\n");
   pbuf +=sprintf(pbuf,"+---------------+-----------+----------+--------+\n");
   for (i = 0; i < vector_p->nb_vectors; i++,p++)   
   {
     pbuf +=sprintf(pbuf,"|  %12.12llu | %8.8u  | %8.8u |   %2.2u   |\n",(long long unsigned int)p->off,p->len,p->byte_offset_in_shared_buf,p->file_idx);
   }
   pbuf +=sprintf(pbuf,"+---------------+-----------+----------+--------+\n");
}
/*
**__________________________________________________________________
*/
/**
   Build the vector for a multi file access
   
   @param off: file offset (starts on a 4KB boundary)
   @param len: length in byte to read or write
   @param vector_p : pointer to the vector that will contains the result
   @param striping_unit: striping unit in bytes 
   @param striping_factor:max  number of slave files
   @param alignment: alignment in bytes on the first block (needed to be 128 aligned for Mojette
   

   @retval 0 on success
   @retval < 0 on error (see errno for details)
*/
int rozofs_build_multiple_offset_vector(uint64_t off, uint32_t len,rozofs_iov_multi_t *vector_p,uint32_t striping_unit_bytes, uint32_t striping_factor,uint32_t alignment)
{
   int i = 0;
   rozofs_multi_vect_t *p;
   uint64_t block_number;
   uint64_t offset_in_block;
   uint64_t file_idx;
   uint32_t byte_offset_in_shared_buf = alignment;
   
   vector_p->nb_vectors = 0;
   p = &vector_p->vectors[0];
   
   /*
   ** Get the number of entries to create: it depends on the striping_size 
   */
   while (len != 0)
   {
     block_number = off/striping_unit_bytes;
     offset_in_block = off%striping_unit_bytes;
     file_idx = block_number%striping_factor;
     p->off = (block_number/striping_factor)*striping_unit_bytes + offset_in_block;
     p->file_idx = file_idx+1;
     p->byte_offset_in_shared_buf = byte_offset_in_shared_buf;
     {
       if ((offset_in_block +len) > striping_unit_bytes)
       {
	  p->len = striping_unit_bytes - offset_in_block;
	  len -= p->len;
	  off +=p->len;
       }
       else
       {
	  p->len = len;
	  len = 0;
       }
       byte_offset_in_shared_buf +=p->len;
       p++;
       i++;
     }
   }
   vector_p->nb_vectors = i;
   return 0;  
}   


/**
   Build the vector for a multi file access
   
   @param off: file offset (starts on a 4KB boundary)
   @param len: length in byte to read or write
   @param vector_p : pointer to the vector that will contains the result
   @param striping_unit: striping unit in bytes 
   @param striping_factor:max  number of slave files
   @param alignment: alignment in bytes on the first block (needed to be 128 aligned for Mojette)
   @param hybrid_size: size of the hybrid section

   @retval 0 on success
   @retval < 0 on error (see errno for details)
*/
int rozofs_build_multiple_offset_vector_hybrid(uint64_t off, uint32_t len,rozofs_iov_multi_t *vector_p,uint32_t striping_unit_bytes, uint32_t striping_factor,uint32_t alignment,uint32_t hybrid_size_bytes)
{
   int i = 0;
   rozofs_multi_vect_t *p;
   uint64_t block_number;
   uint64_t offset_in_block;
   uint64_t file_idx;
   uint32_t byte_offset_in_shared_buf = alignment;
   
   
   vector_p->nb_vectors = 0;
   p = &vector_p->vectors[0];

   
   if (off < hybrid_size_bytes)
   {
        offset_in_block = off%hybrid_size_bytes;
        p->file_idx = 0;
	p->off = off;	
	p->byte_offset_in_shared_buf = byte_offset_in_shared_buf;   
	if ((offset_in_block +len) > hybrid_size_bytes)
	{
	   p->len = hybrid_size_bytes - offset_in_block;
	   len -= p->len;
	   off +=p->len;
	}
	else
	{
	   p->len = len;
	   len = 0;
	}
	off = off -  hybrid_size_bytes + striping_unit_bytes;
        byte_offset_in_shared_buf +=p->len;
	p++;
	i++;      
   }
   else
   {
	off = off -  hybrid_size_bytes + striping_unit_bytes;   
   }
   
   /*
   ** Get the number of entries to create: it depends on the striping_size 
   */
   while (len != 0)
   {
     block_number = off/striping_unit_bytes;
     offset_in_block = off%striping_unit_bytes;
     if (block_number == 0)
     {
        p->file_idx = 0;
	p->off = off;	
	p->byte_offset_in_shared_buf = byte_offset_in_shared_buf;
	
	if ((offset_in_block +len) > striping_unit_bytes)
	{
	   p->len = striping_unit_bytes - offset_in_block;
	   len -= p->len;
	   off +=p->len;
	}
	else
	{
	   p->len = len;
	   len = 0;
	}
     }
     else
     {
       block_number -=1; // (off-striping_unit_bytes)/striping_unit_bytes;
       file_idx = block_number%(striping_factor);
       file_idx +=1;
       p->off = (block_number/(striping_factor))*striping_unit_bytes + offset_in_block;
       p->file_idx = file_idx;
       p->byte_offset_in_shared_buf = byte_offset_in_shared_buf;
       {
	 if ((offset_in_block +len) > striping_unit_bytes)
	 {
	    p->len = striping_unit_bytes - offset_in_block;
	    len -= p->len;
	    off +=p->len;
	 }
	 else
	 {
	    p->len = len;
	    len = 0;
	 }
       }
     }
     byte_offset_in_shared_buf +=p->len;
     p++;
     i++;
   }
   vector_p->nb_vectors = i;
   return 0;  
}  



/**
*  Call back function called upon a success rpc, timeout or any other rpc failure
*
   That callback is associated to a write in multiple mode

    The opaque fields of the transaction context are used as follows:
    
     - opaque[0]: command index (index within the shared buffer=
     - opaque[1]: index of the file
   
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void write_buf_multiple_nb_cbk(void *this,void *param)  
{

   rozofs_tx_ctx_t      *rozofs_tx_ctx_p;
   rozofs_fuse_save_ctx_t *fuse_save_ctx_p;
   int status;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   uint8_t  *payload;
   storcli_status_ret_t ret;
   xdrproc_t decode_proc = (xdrproc_t)xdr_storcli_status_ret_t;
//   int position;
   struct rpc_msg  rpc_reply;
   int file_idx;
   int trc_idx;
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   file_t *file;   
   ientry_t *ie;

   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_STRUCT(param,fi,sizeof( struct fuse_file_info));   
   file = (file_t *) (unsigned long)  fi->fh;  
   ie = file->ie; 
        
   rpc_reply.acpted_rply.ar_results.proc = NULL;
    /*
    ** Get the pointer to the rozofs fuse context associated with the multiple file write
    */
    fuse_save_ctx_p = (rozofs_fuse_save_ctx_t*)ruc_buf_getPayload(param);
    fuse_save_ctx_p->multiple_pending--;
    if (fuse_save_ctx_p->multiple_pending < 0)
    {
      /*
      ** This should not occur
      */
      fatal("multiple_pending is negative");
      return;
    }
    /*
    ** get the pointer to the transaction context:
    ** it is required to get the information related to the receive buffer
    */
    rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;     
    rozofs_tx_read_opaque_data(rozofs_tx_ctx_p,1,(uint32_t *)&file_idx);
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
    bufsize -= sizeof(uint32_t); /* skip length*/
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
    ** register the current position in the buffer since we will not to re-encode
    ** the response 
    */
//    position = XDR_GETPOS(&xdrs);

    memset(&ret,0, sizeof(ret));                    
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       xdr_free((xdrproc_t) decode_proc, (char *) &ret);
       goto error;
    }   
    if (ret.status == STORCLI_FAILURE) {
        errno = ret.storcli_status_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    /*
    ** no error, so get the length of the data part
    */
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);
    errno = 0;


out:
    /*
    ** trace the end of the transaction
    */
    rozofs_trc_rsp_multiple(srv_rozofs_ll_write,(fuse_ino_t)file,ie->fid,(errno==0)?0:1,trc_idx,file_idx);
    
    if (fuse_save_ctx_p->multiple_pending != 0)
    {
      if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;
      if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
      if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf); 
      return;          
    }
    /*
    ** if it is the last command with no error need do not release the receive buffer and
    ** the transaction context. In case of error, drop the receive buffer and assert a negative status on the transaction context
    */
    if (fuse_save_ctx_p->multiple_errno != 0)
    {
       rozofs_tx_set_errno(rozofs_tx_ctx_p,fuse_save_ctx_p->multiple_errno);
       if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf); 
       return (fuse_save_ctx_p->saved_cbk_of_tx_multiple)(this,param);           
    }
    /*
    ** return the last response: it should be OK for the case the write, there is no need to re-encode
    ** need to re-insert the reference of the received buffer in the transaction context
    */
    rozofs_tx_put_recvBuf(this,recv_buf);
    return (fuse_save_ctx_p->saved_cbk_of_tx_multiple)(this,param);    

error:
    /*
    ** register the errno code if noy yet asserted
    */
    
    if (fuse_save_ctx_p->multiple_errno == 0) fuse_save_ctx_p->multiple_errno = errno;
    goto out;


}
#define FDL_STRIPING_UNIT 2
#define FDL_STRIPING_FACTOR 2

/*
**__________________________________________________________________
*/
/** Send a request to the export server to know the file size
 *  adjust the write buffer to write only whole data blocks,
 *  reads blocks if necessary (incomplete blocks)
 *  and uses the function write_blocks to write data
 
    The opaque fields of the transaction context are used as follows:
    
     - opaque[0]: command index (index within the shared buffer=
     - opaque[1]: index of the file
 *
 * @param *f: pointer to the file structure
 * @param off: offset to write from
 * @param *buf: pointer where the data are be stored
 * @param len: length to write
*/
 

/*
**__________________________________________________________________________
**__________________________________________________________________________
*/

/*
**__________________________________________________________________________
*/
/**
   
   Process a write request after the copy of data from kernel to shared buffer by a write thread.
   It corresponds to the function which is called when the file has no slave inodes (multiple file)

    The message contains the following information:
   -opcode          : ROZOFS_FUSE_WRITE_BUF_MULTI
   -rozofs_tx_ctx_p : contains the pointer to the fuse_context
   -status          : 0 if not error -1 otherwise
   -errval          : errno value
   
   The message has been encoded by the write thread, so we just need to send the message to the selected storcli
*/   
void af_unix_fuse_write_process_response_multiple(void *msg_p)
{

    void              *fuse_ctx_p;
    struct fuse_file_info  file_info;
    struct fuse_file_info  *fi = &file_info;
    int trc_idx;      
    file_t *file;
    fuse_req_t req;
    int deferred_fuse_write_response;    
    uint64_t off; 
    uint32_t size;
    int64_t ret64;


    
    rozofs_fuse_wr_thread_msg_t *msg = (rozofs_fuse_wr_thread_msg_t*) msg_p;
    fuse_ctx_p = msg->rozofs_tx_ctx_p;

    /*
    ** restore the working variables associated with the request
    */
    RESTORE_FUSE_PARAM(fuse_ctx_p,off);
    RESTORE_FUSE_PARAM(fuse_ctx_p,size);    
    RESTORE_FUSE_PARAM(fuse_ctx_p,trc_idx);
    RESTORE_FUSE_PARAM(fuse_ctx_p,req);
    RESTORE_FUSE_STRUCT(fuse_ctx_p,fi,sizeof( struct fuse_file_info));   
    file = (file_t *) (unsigned long)  fi->fh;  
    /*
    ** decrement the pending storcli request count
    */
    rozofs_storcli_pending_req_count--;
    /*
    ** check the status of the write operation
    */
    if (msg->status < 0)
    {
      errno = msg->errval;
      goto error;
    }

    ret64 = write_buf_multiple_nb(fuse_ctx_p,file,  off, NULL, size,1);
    if (ret64 < 0)
    {
      goto error;
    }
    /*
    ** Normal send, wait for the storcli response
    */
    if (file->buf_write_pending <= ROZOFS_MAX_WRITE_PENDING) {
      deferred_fuse_write_response = 0;
      SAVE_FUSE_PARAM(fuse_ctx_p,deferred_fuse_write_response);
      rozofs_trc_rsp(srv_rozofs_ll_write,(fuse_ino_t)file,file->fid,(errno==0)?0:1,trc_idx);
      fuse_reply_write(req, size);
//      write_flush_stat.non_synchroneous++;
      goto out;
    }        
    /*
    ** Maximum number of write pending is reached. 
    ** Let's differ the FUSE response until the STORCLI response
    */
//    write_flush_stat.synchroneous++;
    deferred_fuse_write_response = 1;
    SAVE_FUSE_PARAM(fuse_ctx_p,deferred_fuse_write_response);    
    STOP_PROFILING_NB(fuse_ctx_p,rozofs_ll_write);
out:

    return;  
    
error:
    file->wr_error = errno;
    rozofs_trc_rsp(srv_rozofs_ll_write,(fuse_ino_t)file,file->fid,(errno==0)?0:1,trc_idx);
    fuse_reply_err(req, errno);   
    /*
    ** log the I/O error return upon the failure while attempting to submit the write request towards a storcli
    */
    rozofs_iowr_err_log(file->fid,off,size,errno,file->ie);
    /*
    **  Release the fuse context and reply to the calling thread
    */
    STOP_PROFILING_NB(fuse_ctx_p,rozofs_ll_write);
    if (fuse_ctx_p != NULL) rozofs_fuse_release_saved_context(fuse_ctx_p);
    goto out;    
}

/*
**__________________________________________________________________________
*/
/**
*   Post a write request in multiple mode towards a write thread

    In multiple mode, the write thread performs the copy of the write data from the kernel space towards 
    the allocated shared buffer, then upon receiving the response, the main thread allocates
    all the need resources to perform the write.
    The only difference between the write without threads is the copy of the data from the kernel by the thread

    The message contains the following information:
   -opcode          : ROZOFS_FUSE_WRITE_BUF_MULTI
   -rozofs_tx_ctx_p : contains the pointer to the fuse_context
   -rm_xid          : length to write
   
   all the other fields are not significant 
    
    @param fuse_ctx_p
    @param off: file offset
    @param len: length to copy
    
    @retval 0 on success
    @retval -1 on error (see errno for details)
*/
int rozofs_storcli_wr_thread_send_multiple(void *fuse_ctx_p,uint64_t off,uint32_t len) 			       
{
       
    rozofs_fuse_wr_thread_msg_t msg_thread;

    rozofs_fuse_wr_thread_msg_t *msg_thread_p = &msg_thread;

    msg_thread_p->opcode      = ROZOFS_FUSE_WRITE_BUF_MULTI;
    msg_thread_p->status      = 0;
    msg_thread_p->storcli_idx = 0; /* not used */
    msg_thread_p->rozofs_fuse_cur_rcv_buf = NULL; 
    msg_thread_p->rpc_opcode  = 0; /* Not used */
    msg_thread_p->encode_fct  = NULL; /* not used */
    msg_thread_p->rozofs_tx_ctx_p = fuse_ctx_p;
    msg_thread_p->rm_xid      = len; /* use that field to provide the length to copy */
    /*
    ** increment virtually the storcli request pending count. It must be decrement upon receiving the response
    */
    rozofs_storcli_pending_req_count++;
    
    rozofs_sendto_wr_fuse_thread(msg_thread_p);

    return 0;

}


/*
**__________________________________________________________________
*/
/** Send a request to the export server to know the file size
   adjust the write buffer to write only whole data blocks,
   reads blocks if necessary (incomplete blocks)
   and uses the function write_blocks to write data
   
   When 'thread' is asserted, we just copy the data from kernel to the allocated shared bufffer 
   and then sends back a response in order to finish the write transaction.
 
    The opaque fields of the transaction context are used as follows:
    
     - opaque[0]: command index (index within the shared buffer=
     - opaque[1]: index of the file
 
 @param *f: pointer to the file structure
 @param off: offset to write from
 @param *buf: pointer where the data are be stored
 @param len: length to write
 @param thread : assert to 1 if it is called from the write thread , 0 otherwise
*/
 
int64_t write_buf_multiple_nb(void *fuse_ctx_p,file_t * f, uint64_t off, const char *buf, uint32_t len,int thread) 
{
    storcli_write_arg_t  args;
    int ret;
    int storcli_idx;
    ientry_t *ie;
    uint8_t storcli_flags;
    rozofs_slave_inode_t *slave_inode_p = NULL;
    uint32_t striping_unit_bytes;
    uint32_t striping_factor;
    rozofs_iov_multi_t vector;
    int shared_buf_idx = -1;
    uint32_t length;
    void *shared_buf_ref;
    int alignment;
    rozofs_shared_buf_wr_hdr_t  *share_wr_p;  
    void * kernel_fuse_write_request;
    int nb_vect;
    void              *xmit_buf = NULL;
    rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;
    uint8_t           *arg_p;
    uint32_t          *header_size_p;
    int lbg_id;
    XDR               xdrs;    
    struct rpc_msg   call_msg;
    int               position;
    int               bufsize;
    int               opcode;
    uint32_t          vers;    
    uint32_t          prog;
    uint32_t timeout_sec;
    uint32_t         null_val = 0;    
    rozofs_fuse_save_ctx_t *fuse_save_ctx_p=NULL;
    fuse_end_tx_recv_pf_t  callback;
    int trc_idx;
    int use_write_thread = 0;
//    fuse_ino_t ino;

//    RESTORE_FUSE_PARAM(fuse_ctx_p,ino);
    RESTORE_FUSE_PARAM(fuse_ctx_p,trc_idx);
    RESTORE_FUSE_PARAM(fuse_ctx_p,kernel_fuse_write_request);
    
    /*
    ** Check if the function has been called from the default path (without write thread
    ** or to process the thread response
    */    
    if (thread == 0)
    {
      /*
      ** check the case of the write thread
      */
      if (ROZOFS_MAX_WRITE_THREADS != 0)
      {
       if (kernel_fuse_write_request!= NULL) use_write_thread = 1;
      }  

      /*
      ** Get the pointer to the rozofs fuse context 
      */
      fuse_save_ctx_p = (rozofs_fuse_save_ctx_t*)ruc_buf_getPayload(fuse_ctx_p);
      fuse_save_ctx_p->multiple_errno   = 0; 
      fuse_save_ctx_p->multiple_pending = 0; 

      /*
      **______________________________________________________________________________
      **  Allocation of a shared buffer that will be used to carry the data to write
      **______________________________________________________________________________
      */            
      shared_buf_idx = -1;
      shared_buf_ref = rozofs_alloc_shared_storcli_buf(SHAREMEM_IDX_WRITE);
      if (shared_buf_ref == NULL)
      {
	 /*
	 ** should not occur
	 */
	 fatal("Out of shared buffer (Write)");
	 return -1;
      }
      share_wr_p = (rozofs_shared_buf_wr_hdr_t*)ruc_buf_getPayload(shared_buf_ref);

      SAVE_FUSE_PARAM(fuse_ctx_p,shared_buf_ref);
      shared_buf_idx = rozofs_get_shared_storcli_payload_idx(shared_buf_ref,SHAREMEM_IDX_WRITE,&length);
      if (shared_buf_idx < 0)
      {
	 /*
	 ** should not occur
	 */
	 fatal("Bad buffer index (Write)");
	 return -1;
      } 
      /*
      **________________________________________________________________________________________________
      ** Save the callback that should be called up the end of processing of the last multiple command
      **________________________________________________________________________________________________
      */
      GET_FUSE_CALLBACK(fuse_ctx_p,callback);
      SAVE_FUSE_CALLBACK_MULTIPLE(fuse_ctx_p ,callback);
      /*
      **________________________________________________________________________________________________
      ** Copy the source buffer in the shared buffer used for writing. 
      ** Need to take care of the 128 bits alignement at the beginning of the buffer
      **________________________________________________________________________________________________
      */
      /*
      ** Check the case of the write thread usage
      */
      if (use_write_thread) return rozofs_storcli_wr_thread_send_multiple(fuse_ctx_p,off,len);
      /*
      ** single threaded mode
      */
      alignment = off%16;
      /*
      ** copy the data in the shared buffer 
      ** Set pointer to the buffer start and adjust with alignment
      */
      uint8_t * buf_start = (uint8_t *)share_wr_p;
      buf_start += alignment+ROZOFS_SHMEM_WRITE_PAYLOAD_OFF;

      /*
      ** Check if we need to do an ioctl to get the data
      */
      if (kernel_fuse_write_request != NULL)
      {
	 ioctl_big_wr_t data;

	 data.req = kernel_fuse_write_request;
	 data.user_buf = buf_start;      
	 data.user_bufsize = len;
	 ret = ioctl(rozofs_fuse_ctx_p->fd,5,&data); 
	 if (ret != 0)
	 {
	    errno = EIO;
	    fuse_save_ctx_p->multiple_errno = errno;
            severe("ioctl write error %s",strerror(errno));   
	   goto error;
	 } 
      }
      else
      {
	memcpy(buf_start,(char*) buf,len);	
      }
    }
    /*
    ** Case of a call done to process the write thread response in multiple mode
    */
    else
    {
      /*
      ** restore the reference of the shared buffer in order to get the pointer to the control part
      ** needed by the storcli transaction
      */
      RESTORE_FUSE_PARAM(fuse_ctx_p,shared_buf_ref);
      share_wr_p = (rozofs_shared_buf_wr_hdr_t*)ruc_buf_getPayload(shared_buf_ref);   
      /*
      ** get the index of the share buffer: it is an argument of the storcli RPC request
      */
      shared_buf_idx = rozofs_get_shared_storcli_payload_idx(shared_buf_ref,SHAREMEM_IDX_WRITE,&length);     
      /*
      ** Get the pointer to the rozofs fuse context 
      */
      fuse_save_ctx_p = (rozofs_fuse_save_ctx_t*)ruc_buf_getPayload(fuse_ctx_p);
      /*
      ** re-compute the alignment  in the shared buffer
      */
      alignment = off%16;
    } 
    /*
    ** prepare the RPC stuff
    */ 
    opcode = STORCLI_WRITE;
    vers = STORCLI_VERSION;
    prog = STORCLI_PROGRAM;
    timeout_sec = ROZOFS_TMR_GET(TMR_STORCLI_PROGRAM);    
       
    /*
    ** get the pointer to the internal representation of the rozofs inode
    */
    ie = f->ie; 
    /*
    **________________________________
    ** Update the bandwidth statistics
    **________________________________
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_WRITE_THR],(uint64_t)len);    
    /**
    * write alignement stats
    */
    rozofs_aligned_write_start[(off%ROZOFS_BSIZE_BYTES(exportclt.bsize)==0)?0:1]++;
    rozofs_aligned_write_end[((off+len)%ROZOFS_BSIZE_BYTES(exportclt.bsize)==0)?0:1]++;

    /*
    **___________________________________________________________
    ** Processing of the specific flags for write optimization 
    ** (to avoid unecessary read because of wrong alignment)
    **___________________________________________________________
    */
    /* 
    **If file was empty at opening tell it to storcli at 1rts write
    */
    args.flags = 0;
    storcli_flags = 0;
    if (f->file2create == 1) {
      storcli_flags |= STORCLI_FLAGS_EMPTY_FILE;
      f->file2create = 0;
    }
    /*
    ** No need to re-read the last block when not aligned when we extend 
    ** the file since this client is the only writter of the file
    */
    if (conf.onlyWriter) {
      if (off+len >= ie->attrs.attrs.size) {
        args.flags |= STORCLI_FLAGS_NO_END_REREAD;    
      }
    }

    slave_inode_p = rozofs_get_slave_inode_from_ie(ie);
    if (slave_inode_p == NULL)
    {
      fuse_save_ctx_p->multiple_errno = EPROTO;
      severe("ientry has no slave inode pointer (%p)",ie);
      goto error;
    }
    striping_unit_bytes = rozofs_get_striping_size_from_ie(ie);
    striping_factor = rozofs_get_striping_factor_from_ie(ie);
    /*
    **___________________________________________________________________
    **         build the  write vector
    **  The return vector indicates how many commands must be generated)
    **____________________________________________________________________
    */
    if (ie->attrs.hybrid_desc.s.no_hybrid==1)
    {
      rozofs_build_multiple_offset_vector(off, len,&vector,striping_unit_bytes,striping_factor,alignment);
    }
    else
    {
      uint32_t hybrid_size;
      hybrid_size = rozofs_get_hybrid_size_from_ie(ie);
      rozofs_build_multiple_offset_vector_hybrid(off, len,&vector,striping_unit_bytes,striping_factor,alignment,hybrid_size);    
    }

//    rozofs_print_multi_vector(&vector,bufall_debug);
//    info("FDL \n%s",bufall_debug);
    /*
    ** Common arguments for the write
    */
    args.layout = f->export->layout;
    args.bsize = exportclt.bsize;
    args.shared_buf_idx = shared_buf_idx;

    /*
    **___________________________________________________________________
    **   Build the individual storcli write command
    **____________________________________________________________________
    */
    
    
    rozofs_multi_vect_t *entry_p = &vector.vectors[0];
    for (nb_vect = 0; nb_vect < vector.nb_vectors;nb_vect++,entry_p++)    
    {
      rozofs_tx_ctx_p = NULL;
      args.cmd_idx = nb_vect;

      share_wr_p->cmd[args.cmd_idx].xid = 0;
      share_wr_p->cmd[args.cmd_idx].write_len = entry_p->len;       
      share_wr_p->cmd[args.cmd_idx].offset_in_buffer = entry_p->byte_offset_in_shared_buf;    

      /*
      ** Get the FID storage, cid and sids lists for the current file index
      */
      ret = rozofs_fill_storage_info_multiple(ie,&args.cid,args.dist_set,args.fid,entry_p->file_idx);
      /*
      ** Trace individual write request
      */
      rozofs_trc_req_io_multiple(trc_idx,srv_rozofs_ll_write,(fuse_ino_t)f,args.fid,entry_p->len,entry_p->off,entry_p->file_idx);

#if 0
#warning FDL debug
{
      char bufall_debug[1024];
      rozofs_fid2string(args.fid,bufall_debug);      
      info("FDL fid %s off %llu/%u file idx %d striping_unit %u striping_factor %u off %llu len %u off_buf %u buf_idx %u",
             bufall_debug,off,len,entry_p->file_idx,striping_unit_bytes,striping_factor,entry_p->off,entry_p->len,entry_p->byte_offset_in_shared_buf,shared_buf_idx);
}
#endif
      if (ret < 0)
      {
	severe("FDL bad storage information encoding");
	fuse_save_ctx_p->multiple_errno = EINVAL;
	goto error;
      }  
      args.off = entry_p->off;
      args.data.data_len = entry_p->len;
      /*
      ** Update the size in the ientry for that slave inode
      */
      rozofs_slave_inode_update_size_write(ie,entry_p->file_idx,entry_p->off+entry_p->len);
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
	 fuse_save_ctx_p->multiple_errno = errno;
	 goto error;
      }
      /*
      ** save the index of the command in the opaque[0] field of the transaction context
      */      
      rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,0,args.cmd_idx);
      /*
      ** save the file index  in the opaque[1] field of the transaction context
      */        
      rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,1,entry_p->file_idx);
      /*
      ** get the storcli to use for the transaction: it uses the STORIO fid as a primary key
      */
      storcli_idx = stclbg_storcli_idx_from_fid(args.fid);   
      /*
      ** Insert in the hash table to handle the case of the serialization
      */   
      stclbg_hash_table_insert_ctx(&rozofs_tx_ctx_p->rw_lbg,args.fid,storcli_idx);     
      /*
      ** Get the load balancing group reference associated with the storcli
      */
      lbg_id = storcli_lbg_get_load_balancing_reference(storcli_idx);        
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
	fuse_save_ctx_p->multiple_errno = errno;
	goto error;
      }         
      /*
      ** store the reference of the xmit buffer in the transaction context: might be useful
      ** in case we want to remove it from a transmit list of the underlying network stacks
      */
      rozofs_tx_save_xmitBuf(rozofs_tx_ctx_p,xmit_buf);
      /*
      **________________________
      ** RPC Message encoding
      **________________________
      */
      /*
      ** get the pointer to the payload of the buffer
      */
      header_size_p  = (uint32_t*) ruc_buf_getPayload(xmit_buf);
      arg_p = (uint8_t*)(header_size_p+1);  
      /*
      ** create the xdr_mem structure for encoding the message
      */
      bufsize = ruc_buf_getMaxPayloadLen(xmit_buf);
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
      share_wr_p->cmd[args.cmd_idx].xid = call_msg.rm_xid;

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
       fuse_save_ctx_p->multiple_errno = errno;
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
    if (xdr_storcli_write_arg_no_data_t(&xdrs,(storcli_write_arg_no_data_t *)&args) == FALSE)
    {
       TX_STATS(ROZOFS_TX_ENCODING_ERROR);
       errno = EPROTO;
       fuse_save_ctx_p->multiple_errno = errno;
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
    rozofs_tx_ctx_p->recv_cbk   = write_buf_multiple_nb_cbk;
    rozofs_tx_ctx_p->user_param = fuse_ctx_p;    
   /*
   **__________________________________________________________________________
   ** Submit the RPC message to the selected load balancing group (storcli)
   **___________________________________________________________________________
   */
   
    /*
    ** increment the number of pending request towards the storcli
    ** increment also the number of pending requests on the rozofs fuse context
    */
    fuse_save_ctx_p->multiple_pending++; 
    rozofs_storcli_pending_req_count++;
    ret = north_lbg_send(lbg_id,xmit_buf);
    if (ret < 0)
    {
       if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;
       /*
       ** decrement the number of pending request and assert the multiple_errno
       */
       
       fuse_save_ctx_p->multiple_pending--;
       TX_STATS(ROZOFS_TX_SEND_ERROR);
       errno = EFAULT;
       fuse_save_ctx_p->multiple_errno = errno;
      goto error;  
    }
    TX_STATS(ROZOFS_TX_SEND);

    /*
    ** OK, so now finish by starting the guard timer
    */
    rozofs_tx_start_timer(rozofs_tx_ctx_p,timeout_sec);      
  }
  /*
  ** The error will be processed by the callback, so do it as it was OK
  */
  uint64_t buf_flush_offset = off;
  uint32_t buf_flush_len = len;
  SAVE_FUSE_PARAM(fuse_ctx_p,buf_flush_offset);
  SAVE_FUSE_PARAM(fuse_ctx_p,buf_flush_len);
  f->buf_write_pending++;
  f->write_block_pending = 1;

  return 0;


error:
  /* 
  ** Release the any pending transaction context 
  */
  if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);  

  errno = fuse_save_ctx_p->multiple_errno;
  rozofs_trc_rsp(srv_rozofs_ll_write,(fuse_ino_t)f,NULL,(errno==0)?0:1,trc_idx);
  /*
  ** check if the pending is 0 in the fuse context
  */
  if (fuse_save_ctx_p->multiple_pending != 0)
  {
    /*
    ** The error will be processed by the callback, so do it as it was OK
    */
    uint64_t buf_flush_offset = off;
    uint32_t buf_flush_len = len;
    SAVE_FUSE_PARAM(fuse_ctx_p,buf_flush_offset);
    SAVE_FUSE_PARAM(fuse_ctx_p,buf_flush_len);
    f->buf_write_pending++;
    f->write_block_pending = 1;
    return 0;
  }
  /*
  ** log the I/O error return upon the failure while attempting to submit the write request towards a storcli
  */
  rozofs_iowr_err_log(f->fid,off,len,errno,f->ie);

  return -1;
}      



    
/*
**__________________________________________________________________________
*/
/**
*  Call back function called upon a success rpc, timeout or any other rpc failure
*
   That callback is associated to a write in multiple mode

    The opaque fields of the transaction context are used as follows:
    
     - opaque[0]: command index (index within the shared buffer=
     - opaque[1]: index of the file
     - opaque[2]: requested length
        
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void read_buf_multiple_nb_cbk(void *this,void *param)  
{

    rozofs_tx_ctx_t      *rozofs_tx_ctx_p;
    rozofs_fuse_save_ctx_t *fuse_save_ctx_p;
    int status;
    void     *recv_buf = NULL;   
    XDR       xdrs;    
    int      bufsize;
    uint8_t  *payload;
    storcli_read_ret_no_data_t ret;
    xdrproc_t decode_proc = (xdrproc_t)xdr_storcli_read_ret_no_data_t;
//    int position;
    struct rpc_msg  rpc_reply;
    rozofs_shared_buf_rd_hdr_t  *share_rd_p;  
    void *shared_buf_ref;
    uint32_t cmd_idx;
    uint32_t requested_length;
    char *pbuf;
    int enoent_status = 0;
    int trc_idx;
    int file_idx;
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   file_t *file;   
   ientry_t *ie;
   
   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_STRUCT(param,fi,sizeof( struct fuse_file_info));   
   
   file = (file_t *) (unsigned long)  fi->fh;  
   ie = file->ie; 
        
   rpc_reply.acpted_rply.ar_results.proc = NULL;

    RESTORE_FUSE_PARAM(param,shared_buf_ref);
    share_rd_p = (rozofs_shared_buf_rd_hdr_t*)ruc_buf_getPayload(shared_buf_ref);
    /*
    ** set the pointer to the beginning of the buffer that contains the payload
    */
    pbuf = (char*) share_rd_p;
    pbuf +=ROZOFS_SHMEM_READ_PAYLOAD_OFF;
    

    /*
    ** Get the pointer to the rozofs fuse context associated with the multiple file write
    */
    fuse_save_ctx_p = (rozofs_fuse_save_ctx_t*)ruc_buf_getPayload(param);
    fuse_save_ctx_p->multiple_pending--;
    if (fuse_save_ctx_p->multiple_pending < 0)
    {
      /*
      ** This should not occur
      */
      fatal("multiple_pending is negative");
      return;
    }
    /*
    ** get the pointer to the transaction context:
    ** it is required to get the information related to the receive buffer
    */
    rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;     
    rozofs_tx_read_opaque_data(rozofs_tx_ctx_p,0,&cmd_idx);
    rozofs_tx_read_opaque_data(rozofs_tx_ctx_p,1,(uint32_t *)&file_idx);
    rozofs_tx_read_opaque_data(rozofs_tx_ctx_p,2,&requested_length);
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
    bufsize -= sizeof(uint32_t); /* skip length*/
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
    ** register the current position in the buffer since we will not to re-encode
    ** the response 
    */
//    position = XDR_GETPOS(&xdrs);

    memset(&ret,0, sizeof(ret));                    
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       xdr_free((xdrproc_t) decode_proc, (char *) &ret);
       goto error;
    }   
    if (ret.status == STORCLI_FAILURE) {
        if (ret.storcli_read_ret_no_data_t_u.error == ENOENT) {
	  errno = 0;
	}  
	else {
          errno = ret.storcli_read_ret_no_data_t_u.error;
	}  
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    /*
    ** no error, so get the length of the data part
    */
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);
    /*
    ** Update the received len
    */
     share_rd_p->cmd[0].received_len += requested_length;
     if ((fuse_save_ctx_p->multiple_errno == 0) && (share_rd_p->cmd[cmd_idx].received_len < requested_length))
     {
       pbuf = (char*) share_rd_p;
       pbuf +=ROZOFS_SHMEM_READ_PAYLOAD_OFF;   
       /*
       ** put pbuf at the beginning of the array to reset
       */
       pbuf += share_rd_p->cmd[cmd_idx].offset_in_buffer;  
       pbuf +=share_rd_p->cmd[cmd_idx].received_len;
       memset(pbuf,0,requested_length - share_rd_p->cmd[cmd_idx].received_len);
     }
     errno = 0;

out:
    /*
    ** trace the end of the transaction
    */
    rozofs_trc_rsp_multiple(srv_rozofs_ll_read,(fuse_ino_t)file,ie->fid,(errno==0)?0:1,trc_idx,file_idx);
    
    if (fuse_save_ctx_p->multiple_pending != 0)
    {
      if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;
      if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
      if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf); 
      return;          
    }
    /*
    ** if it is the last command with no error need do not release the receive buffer and
    ** the transaction context. In case of error, drop the receive buffer and assert a negative status on the transaction context
    */
    if (fuse_save_ctx_p->multiple_errno != 0)
    {
//       info("FDL !!!!! ERRNO ASSERTED!!!! ");
       rozofs_tx_set_errno(rozofs_tx_ctx_p,fuse_save_ctx_p->multiple_errno);
       if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf); 
       return (fuse_save_ctx_p->saved_cbk_of_tx_multiple)(this,param);           
    }
    /*
    ** return the last response: it should be OK for the case the write, there is no need to re-encode
    ** need to re-insert the reference of the received buffer in the transaction context
    */
    if (enoent_status) {
        /*
	** need to re-encode the response buffer
	*/	
	uint8_t *pbuf;           /* pointer to the part that follows the header length */
	uint32_t *header_len_p;  /* pointer to the array that contains the length of the rpc message*/
	int len;
	storcli_status_t status = STORCLI_SUCCESS;
	int data_len = 0;
	uint32_t alignment= 0x53535353;;
	
//        info("FDL !!!!! ENOENT ");
	header_len_p = (uint32_t*)ruc_buf_getPayload(recv_buf); 
	pbuf = (uint8_t*) (header_len_p+1);            
	len = (int)ruc_buf_getMaxPayloadLen(recv_buf);
	len -= sizeof(uint32_t);
	xdrmem_create(&xdrs,(char*)pbuf,len,XDR_ENCODE); 
	if (rozofs_encode_rpc_reply(&xdrs,(xdrproc_t)xdr_storcli_status_t,(caddr_t)&status,0) != TRUE)
	{
	  
	  severe("rpc reply encoding error");
	  fuse_save_ctx_p->multiple_errno = EPROTO;	  
	  rozofs_tx_set_errno(rozofs_tx_ctx_p,fuse_save_ctx_p->multiple_errno);
	  if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf); 
	  return (fuse_save_ctx_p->saved_cbk_of_tx_multiple)(this,param);    
	}
	XDR_PUTINT32(&xdrs, (int32_t *)&alignment);
	XDR_PUTINT32(&xdrs, (int32_t *)&alignment);
	XDR_PUTINT32(&xdrs, (int32_t *)&alignment);       
	XDR_PUTINT32(&xdrs, (int32_t *)&data_len);	
	int total_len = xdr_getpos(&xdrs) ;
	*header_len_p = htonl(0x80000000 | total_len);
	total_len +=sizeof(uint32_t);

	ruc_buf_setPayloadLen(recv_buf,total_len);    
    }
    rozofs_tx_put_recvBuf(this,recv_buf);
//    info("FDL RECEIVED LENGTH %u",share_rd_p->cmd[0].received_len);
    return (fuse_save_ctx_p->saved_cbk_of_tx_multiple)(this,param);    

error:
   /*
   ** Check the case of the empty file: in that case errno has been set to 0
   */
   if ((errno == 0) && (fuse_save_ctx_p->multiple_errno == 0))
   {
      int error = errno;
      /*
      ** reset the array corresponding to the requested length
      */
      /*
      ** set the pointer to the beginning of the buffer that contains the payload
      */
      pbuf = (char*) share_rd_p;
      pbuf +=ROZOFS_SHMEM_READ_PAYLOAD_OFF;   
      /*
      ** put pbuf at the beginning of the array to reset
      */
//      info("FDL cmd %d buf_off %u len %u",cmd_idx,share_rd_p->cmd[cmd_idx].offset_in_buffer,requested_length);
      pbuf += share_rd_p->cmd[cmd_idx].offset_in_buffer;  
      memset(pbuf,0,requested_length);
      share_rd_p->cmd[0].received_len += requested_length;
      enoent_status = 1;
      /*
      ** restaure errno
      */
      errno = error;
      
    }     
    /*
    ** register the errno code if noy yet asserted
    */
    
    if (fuse_save_ctx_p->multiple_errno == 0) fuse_save_ctx_p->multiple_errno = errno;
    goto out;


}

void rozofs_ll_read_cbk(void *this,void *param);
/*
**__________________________________________________________________
*/
/** Send a request to the export server to know the file size
 *  adjust the write buffer to write only whole data blocks,
 *  reads blocks if necessary (incomplete blocks)
 *  and uses the function write_blocks to write data
 
    The opaque fields of the transaction context are used as follows:
    
     - opaque[0]: command index (index within the shared buffer=
     - opaque[1]: index of the file
     - opaque[2]: requested length
 *
 * @param *f: pointer to the file structure
 * @param off: offset to write from
 * @param *buf: pointer where the data are be stored
 * @param len: length to write
*/
 
int read_buf_multitple_nb(void *fuse_ctx_p,file_t * f, uint64_t off, const char *buf, uint32_t len) 
{
    storcli_read_arg_t  args;
    int ret;
    int storcli_idx;
    ientry_t *ie;
    rozofs_slave_inode_t *slave_inode_p = NULL;
    uint32_t striping_unit_bytes;
    uint32_t striping_factor;
    rozofs_iov_multi_t vector;
    int shared_buf_idx = -1;
    uint32_t length;
    void *shared_buf_ref;
    rozofs_shared_buf_rd_hdr_t  *share_rd_p;  
    int nb_vect;
    void              *xmit_buf = NULL;
    rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;
    uint8_t           *arg_p;
    uint32_t          *header_size_p;
    int lbg_id;
    XDR               xdrs;    
    struct rpc_msg   call_msg;
    int               position;
    int               bufsize;
    int               opcode;
    uint32_t          vers;    
    uint32_t          prog;
    uint32_t timeout_sec;
    uint32_t         null_val = 0;    
    rozofs_fuse_save_ctx_t *fuse_save_ctx_p=NULL;
    fuse_end_tx_recv_pf_t  callback;

    int bbytes = ROZOFS_BSIZE_BYTES(exportclt.bsize);    
    int trc_idx;

    RESTORE_FUSE_PARAM(fuse_ctx_p,trc_idx);
    /*
    ** Get the pointer to the rozofs fuse context 
    */
    fuse_save_ctx_p = (rozofs_fuse_save_ctx_t*)ruc_buf_getPayload(fuse_ctx_p);
    fuse_save_ctx_p->multiple_errno   = 0; 
    fuse_save_ctx_p->multiple_pending = 0; 
    
    /*
    **______________________________________________________________________________
    **  Allocation of a shared buffer that will be used to carry the data to write
    **______________________________________________________________________________
    */            
    shared_buf_idx = -1;
    shared_buf_ref = rozofs_alloc_shared_storcli_buf(SHAREMEM_IDX_READ);
    if (shared_buf_ref == NULL)
    {
       /*
       ** should not occur
       */
       fatal("Out of shared buffer (Write)");
       return -1;
    }
    share_rd_p = (rozofs_shared_buf_rd_hdr_t*)ruc_buf_getPayload(shared_buf_ref);
    
    SAVE_FUSE_PARAM(fuse_ctx_p,shared_buf_ref);
    shared_buf_idx = rozofs_get_shared_storcli_payload_idx(shared_buf_ref,SHAREMEM_IDX_READ,&length);
    if (shared_buf_idx < 0)
    {
       /*
       ** should not occur
       */
       fatal("Bad buffer index (Write)");
       return -1;
    } 
    args.spare     = 'S';
    args.shared_buf_idx = shared_buf_idx;	 
    /*
    **________________________________________________________________________________________________
    ** Save the callback that should be called up the end of processing of the last multiple command
    **________________________________________________________________________________________________
    */
    callback = rozofs_ll_read_cbk;
    SAVE_FUSE_CALLBACK_MULTIPLE(fuse_ctx_p ,callback);
       
    /*
    ** get the pointer to the internal representation of the rozofs inode
    */
    ie = f->ie; 
    /*
    **________________________________
    ** Update the bandwidth statistics
    **________________________________
    */
    rozofs_fuse_read_write_stats_buf.read_req_cpt++;

    slave_inode_p = rozofs_get_slave_inode_from_ie(ie);
    if (slave_inode_p == NULL)
    {
      fuse_save_ctx_p->multiple_errno = EPROTO;
      severe("ientry has no slave inode pointer (%p)",ie);
      goto error;
    }
    striping_unit_bytes = rozofs_get_striping_size_from_ie(ie);
    striping_factor = rozofs_get_striping_factor_from_ie(ie);
    /*
    **___________________________________________________________________
    **         build the  write vector
    **  The return vector indicates how many commands must be generated)
    **____________________________________________________________________
    */
    if (ie->attrs.hybrid_desc.s.no_hybrid==1)
    {
      rozofs_build_multiple_offset_vector(off, len,&vector,striping_unit_bytes,striping_factor,0);
    }
    else
    {
      uint32_t hybrid_size;
      hybrid_size = rozofs_get_hybrid_size_from_ie(ie);
      rozofs_build_multiple_offset_vector_hybrid(off, len,&vector,striping_unit_bytes,striping_factor,0,hybrid_size);    
    }

//    rozofs_print_multi_vector(&vector,bufall_debug);
//    info("FDL \n%s",bufall_debug);
    /*
    ** Common arguments for the write
    */
    args.layout = f->export->layout;
    args.bsize = exportclt.bsize;
    args.shared_buf_idx = shared_buf_idx;
    
    opcode = STORCLI_READ;
    vers = STORCLI_VERSION;
    prog = STORCLI_PROGRAM;
    timeout_sec = ROZOFS_TMR_GET(TMR_STORCLI_PROGRAM);
    share_rd_p->cmd[0].xid = 0;
    share_rd_p->cmd[0].received_len = 0;   

    /*
    **___________________________________________________________________
    **   Build the individual storcli read command
    **____________________________________________________________________
    */
    
    
    rozofs_multi_vect_t *entry_p = &vector.vectors[0];
    for (nb_vect = 0; nb_vect < vector.nb_vectors;nb_vect++,entry_p++)    
    {
      rozofs_tx_ctx_p = NULL;
      args.cmd_idx = nb_vect+1;

      share_rd_p->cmd[args.cmd_idx].xid = 0;
      share_rd_p->cmd[args.cmd_idx].received_len = 0;       
      share_rd_p->cmd[args.cmd_idx].offset_in_buffer = entry_p->byte_offset_in_shared_buf;    

      /*
      ** Get the FID storage, cid and sids lists for the current file index
      */
      ret = rozofs_fill_storage_info_multiple(ie,&args.cid,args.dist_set,args.fid,entry_p->file_idx);
      /*
      ** Trace the individual read
      */
      rozofs_trc_req_io_multiple(trc_idx,srv_rozofs_ll_read,(fuse_ino_t)f,args.fid,entry_p->len,entry_p->off,entry_p->file_idx);
#if 0      
{
      char bufall_debug[1024];
      rozofs_fid2string(args.fid,bufall_debug);
      info("FDL fid %s off %llu/%u  file idx %d striping_unit %u striping_factor %u off %llu len %u",bufall_debug,off,len,entry_p->file_idx,striping_unit_bytes,striping_factor,entry_p->off,entry_p->len);
}
#endif
      if (ret < 0)
      {
	severe("FDL bad storage information encoding");
	fuse_save_ctx_p->multiple_errno = EINVAL;
	goto error;
      }  
      args.bid = entry_p->off/bbytes;
      args.nb_proj = entry_p->len/bbytes;
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
	 fuse_save_ctx_p->multiple_errno = errno;
	 goto error;
      }
      /*
      ** save the index of the command in the opaque[0] field of the transaction context
      */      
      rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,0,args.cmd_idx);
      /*
      ** save the file index  in the opaque[1] field of the transaction context
      */        
      rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,1,entry_p->file_idx);
      /*
      ** save the requested length  in the opaque[2] field of the transaction context
      */  
      rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,2,entry_p->len);
      /*
      ** get the storcli to use for the transaction: it uses the STORIO fid as a primary key
      */
      storcli_idx = stclbg_storcli_idx_from_fid(args.fid);   
      if (common_config.storcli_read_parallel == 0)
      {
	/*
	** Insert this transaction so that every other following trasnactions
	** for the same FID will follow the same path
	*/
	stclbg_hash_table_insert_ctx(&rozofs_tx_ctx_p->rw_lbg,args.fid,storcli_idx); 
      }    
      /*
      ** Get the load balancing group reference associated with the storcli
      */
      lbg_id = storcli_lbg_get_load_balancing_reference(storcli_idx);        
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
	fuse_save_ctx_p->multiple_errno = errno;
	goto error;
      }         
      /*
      ** store the reference of the xmit buffer in the transaction context: might be useful
      ** in case we want to remove it from a transmit list of the underlying network stacks
      */
      rozofs_tx_save_xmitBuf(rozofs_tx_ctx_p,xmit_buf);
      /*
      **________________________
      ** RPC Message encoding
      **________________________
      */
      /*
      ** get the pointer to the payload of the buffer
      */
      header_size_p  = (uint32_t*) ruc_buf_getPayload(xmit_buf);
      arg_p = (uint8_t*)(header_size_p+1);  
      /*
      ** create the xdr_mem structure for encoding the message
      */
      bufsize = ruc_buf_getMaxPayloadLen(xmit_buf);
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
      share_rd_p->cmd[args.cmd_idx].xid = call_msg.rm_xid;

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
       fuse_save_ctx_p->multiple_errno = errno;
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
    if (xdr_storcli_read_arg_t(&xdrs,(storcli_read_arg_t *)&args) == FALSE)
    {
       TX_STATS(ROZOFS_TX_ENCODING_ERROR);
       errno = EPROTO;
       fuse_save_ctx_p->multiple_errno = errno;
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
    rozofs_tx_ctx_p->recv_cbk   = read_buf_multiple_nb_cbk;
    rozofs_tx_ctx_p->user_param = fuse_ctx_p;    
   /*
   **__________________________________________________________________________
   ** Submit the RPC message to the selected load balancing group (storcli)
   **___________________________________________________________________________
   */
   
    /*
    ** increment the number of pending request towards the storcli
    ** increment also the number of pending requests on the rozofs fuse context
    */
    fuse_save_ctx_p->multiple_pending++; 
    rozofs_storcli_pending_req_count++;
    ret = north_lbg_send(lbg_id,xmit_buf);
    if (ret < 0)
    {
       if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;
       /*
       ** decrement the number of pending request and assert the multiple_errno
       */
       
       fuse_save_ctx_p->multiple_pending--;
       TX_STATS(ROZOFS_TX_SEND_ERROR);
       errno = EFAULT;
       fuse_save_ctx_p->multiple_errno = errno;
      goto error;  
    }
    TX_STATS(ROZOFS_TX_SEND);

    /*
    ** OK, so now finish by starting the guard timer
    */
    rozofs_tx_start_timer(rozofs_tx_ctx_p,timeout_sec);      
  }
  /*
  ** The error will be processed by the callback, so do it as it was OK
  */
  f->buf_read_pending++;

  return 0;


error:
  /* 
  ** Release the any pending transaction context 
  */
  if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);  

  errno = fuse_save_ctx_p->multiple_errno;
  rozofs_trc_rsp(srv_rozofs_ll_read,(fuse_ino_t)f,NULL,(errno==0)?0:1,trc_idx);
  /*
  ** check if the pending is 0 in the fuse context
  */
  if (fuse_save_ctx_p->multiple_pending != 0)
  {
    /*
    ** The error will be processed by the callback, so do it as it was OK
    */
    f->buf_read_pending++;
    return 0;
  }

  return -1;
}      


/*
**__________________________________________________________________
*/
/**
   Build the vector for a multi file access
   
   @param off: file offset 
   @param vector_p : pointer to the vector that will contains the result
   @param striping_unit: striping unit in bytes 
   @param striping_factor:max  number of slave files
   

   @retval 0 on success
   @retval < 0 on error (see errno for details)
*/
int rozofs_build_truncate_multiple_offset_vector(uint64_t off,rozofs_iov_multi_t *vector_p,uint32_t striping_unit_bytes, uint32_t striping_factor)
{
   int i = 0;
   rozofs_multi_vect_t *p;
   uint64_t block_number;
   uint64_t offset_in_block;
   uint64_t file_idx;
   
   vector_p->nb_vectors = 0;
   p = &vector_p->vectors[0];

   /*
   ** compute the block idx, the offset in the block the file index
   */
   block_number = off/striping_unit_bytes;
   offset_in_block = off%striping_unit_bytes;
   file_idx = block_number%striping_factor;
   p->off = (block_number/striping_factor)*striping_unit_bytes + offset_in_block;
   p->file_idx = file_idx+1;
   p->byte_offset_in_shared_buf = offset_in_block;
   offset_in_block = 0;
   p->len = 0;
   p++;
   
   for (i = 0; i < striping_factor-1; i++,p++)
   {
     file_idx++;
     if (file_idx == striping_factor) {
       file_idx = 0;
       offset_in_block = striping_unit_bytes;
     }
     p->off = (block_number/striping_factor)*striping_unit_bytes + offset_in_block;
     p->file_idx = file_idx+1;
     p->byte_offset_in_shared_buf = offset_in_block;   
     p->len= 0;
   }
   
   vector_p->nb_vectors = striping_factor;
   return 0;  
}   


/*
**__________________________________________________________________
*/
/**
   Build the vector for a multi file access
   
   @param off: file offset 
   @param vector_p : pointer to the vector that will contains the result
   @param striping_unit: striping unit in bytes 
   @param striping_factor:max  number of slave files
   @param hybrid_sz_bytes: size of the hybrid section
   

   @retval 0 on success
   @retval < 0 on error (see errno for details)
*/
int rozofs_build_truncate_multiple_offset_vector_hybrid(uint64_t off,rozofs_iov_multi_t *vector_p,uint32_t striping_unit_bytes, uint32_t striping_factor,uint32_t hybrid_sz_bytes)
{
   int i = 0;
   rozofs_multi_vect_t *p;
   uint64_t block_number;
   uint64_t offset_in_block;
   uint64_t file_idx;
   
   vector_p->nb_vectors = 0;
   p = &vector_p->vectors[0];
   
   if (off < hybrid_sz_bytes)
   {
     /*
     ** compute the block idx, the offset in the block the file index
     */
     block_number = 0;
     offset_in_block = off%hybrid_sz_bytes;
     file_idx = block_number%hybrid_sz_bytes;
     p->off = (block_number/striping_factor)*hybrid_sz_bytes + offset_in_block;
     if (block_number == 0)
     {
	p->file_idx = 0;
	p->byte_offset_in_shared_buf = offset_in_block;
	p++;

	for (i = 0; i < striping_factor; i++,p++)
	{
          p->file_idx = i+1;
	  p->off = 0;
          p->byte_offset_in_shared_buf = 0;
	  p->len = 0;
	}
	vector_p->nb_vectors = striping_factor+1;
	return 0;   
     }
   }
   else
   {
     off= off - hybrid_sz_bytes + striping_unit_bytes;
   }
   off = off-striping_unit_bytes;
   block_number = off/striping_unit_bytes;
   offset_in_block = off%striping_unit_bytes;
   file_idx = block_number%striping_factor;
   p->off = (block_number/striping_factor)*striping_unit_bytes + offset_in_block;   
   p->file_idx = file_idx+1;
   p->byte_offset_in_shared_buf = offset_in_block;
   offset_in_block = 0;
   p->len = 0;
   p++;
   
   for (i = 0; i < striping_factor-1; i++,p++)
   {
     file_idx++;
     if (file_idx == striping_factor) {
       file_idx = 0;
       offset_in_block = striping_unit_bytes;
     }
     p->off = (block_number/striping_factor)*striping_unit_bytes + offset_in_block;
     p->file_idx = file_idx+1;
     p->byte_offset_in_shared_buf = offset_in_block;   
     p->len= 0;
   }
   
   vector_p->nb_vectors = striping_factor;
   return 0;  
}   

/*
**__________________________________________________________________
*/
/**
*  Call back function called upon a success rpc, timeout or any other rpc failure
*
   That callback is associated to a truncate in multiple mode

    The opaque fields of the transaction context are used as follows:
    
     - opaque[0]: command index (index within the shared buffer=
     - opaque[1]: index of the file
   
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void truncate_buf_multiple_nb_cbk(void *this,void *param)  
{

    rozofs_tx_ctx_t      *rozofs_tx_ctx_p;
    rozofs_fuse_save_ctx_t *fuse_save_ctx_p;
    int status;
    void     *recv_buf = NULL;   
    XDR       xdrs;    
    int      bufsize;
    uint8_t  *payload;
    storcli_status_ret_t ret;
    xdrproc_t decode_proc = (xdrproc_t)xdr_storcli_status_ret_t;
//    int position;
    struct rpc_msg  rpc_reply;
    int trc_idx;
    fuse_ino_t ino;
    int file_idx;
    rozofs_inode_t trc_fid;
        
   rpc_reply.acpted_rply.ar_results.proc = NULL;

    
    RESTORE_FUSE_PARAM(param,trc_idx);
    RESTORE_FUSE_PARAM(param,ino);
    trc_fid.fid[0] = 0;
    trc_fid.fid[1] = ino;
    /*
    ** Get the pointer to the rozofs fuse context associated with the multiple file write
    */
    fuse_save_ctx_p = (rozofs_fuse_save_ctx_t*)ruc_buf_getPayload(param);
    fuse_save_ctx_p->multiple_pending--;
    if (fuse_save_ctx_p->multiple_pending < 0)
    {
      /*
      ** This should not occur
      */
      fatal("multiple_pending is negative");
      return;
    }
    /*
    ** get the pointer to the transaction context:
    ** it is required to get the information related to the receive buffer
    */
    rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;     
    rozofs_tx_read_opaque_data(rozofs_tx_ctx_p,1,(uint32_t *)&file_idx);
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
    bufsize -= sizeof(uint32_t); /* skip length*/
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
    ** register the current position in the buffer since we will not to re-encode
    ** the response 
    */
//    position = XDR_GETPOS(&xdrs);

    memset(&ret,0, sizeof(ret));                    
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       xdr_free((xdrproc_t) decode_proc, (char *) &ret);
       goto error;
    }   
    if (ret.status == STORCLI_FAILURE) {
        errno = ret.storcli_status_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    /*
    ** no error, so get the length of the data part
    */
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);
    /*
    ** Successful truncate for a single file
    */
    errno= 0;
    rozofs_trc_rsp_multiple(srv_rozofs_ll_truncate,0,(unsigned char *)&trc_fid,0,trc_idx,file_idx);

out:
    if (fuse_save_ctx_p->multiple_pending != 0)
    {
      /*
      ** There is still some requests that are pending...so we release the ressources and exit, waiting
      ** for the next response
      */
      if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;
      if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
      if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf); 
      return;          
    }
    /*
    ** if it is the last command with no error need do not release the receive buffer and
    ** the transaction context. In case of error, drop the receive buffer and assert a negative status on the transaction context
    */
    if (fuse_save_ctx_p->multiple_errno != 0)
    {
       rozofs_tx_set_errno(rozofs_tx_ctx_p,fuse_save_ctx_p->multiple_errno);
       if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf); 
       return (fuse_save_ctx_p->saved_cbk_of_tx_multiple)(this,param);           
    }
    /*
    ** return the last response: it should be OK for the case the write, there is no need to re-encode
    ** need to re-insert the reference of the received buffer in the transaction context
    */
    rozofs_tx_put_recvBuf(this,recv_buf);
    return (fuse_save_ctx_p->saved_cbk_of_tx_multiple)(this,param);    

error:
    rozofs_trc_rsp_multiple(srv_rozofs_ll_truncate,0,(unsigned char*)&trc_fid,1,trc_idx,file_idx);
    /*
    ** register the errno code if noy yet asserted
    */    
    if (fuse_save_ctx_p->multiple_errno == 0) fuse_save_ctx_p->multiple_errno = errno;
    goto out;


}

/*
**__________________________________________________________________
*/
/** Truncate of file has been created as a multiple file
 
    The opaque fields of the transaction context are used as follows:
    
     - opaque[1]: index of the file
 *
 * @param fuse_ctx_p: pointer to the rozofs fuse context
 * @param size: new size of the file
 * @param ie: pointer to the i-node context of the client
 
*/ 
int truncate_buf_multitple_nb(void *fuse_ctx_p,ientry_t *ie, uint64_t size) 
{
    storcli_truncate_arg_t  args;
    int ret;
    int storcli_idx;
    rozofs_slave_inode_t *slave_inode_p = NULL;
    uint32_t striping_unit_bytes;
    uint32_t striping_factor;
    rozofs_iov_multi_t vector;
    int nb_vect;
    void              *xmit_buf = NULL;
    rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;
    uint8_t           *arg_p;
    uint32_t          *header_size_p;
    int lbg_id;
    XDR               xdrs;    
    struct rpc_msg   call_msg;
    int               position;
    int               bufsize;
    int               opcode;
    uint32_t          vers;    
    uint32_t          prog;
    uint32_t timeout_sec;
    uint32_t         null_val = 0;    
    rozofs_fuse_save_ctx_t *fuse_save_ctx_p=NULL;
    fuse_end_tx_recv_pf_t  callback;
    int trc_idx;
    fuse_ino_t ino;
    rozofs_inode_t trc_fid;
      
    int bbytes = ROZOFS_BSIZE_BYTES(exportclt.bsize);    
    
    RESTORE_FUSE_PARAM(fuse_ctx_p,trc_idx);
    RESTORE_FUSE_PARAM(fuse_ctx_p,ino);
    trc_fid.fid[0] = 0;
    trc_fid.fid[1] = ino;
    /*
    ** Get the pointer to the rozofs fuse context 
    */
    fuse_save_ctx_p = (rozofs_fuse_save_ctx_t*)ruc_buf_getPayload(fuse_ctx_p);
    fuse_save_ctx_p->multiple_errno   = 0; 
    fuse_save_ctx_p->multiple_pending = 0; 
    	 
    /*
    **________________________________________________________________________________________________
    ** Save the callback that should be called up the end of processing of the last multiple command
    **________________________________________________________________________________________________
    */
    callback = rozofs_ll_truncate_cbk;
    SAVE_FUSE_CALLBACK_MULTIPLE(fuse_ctx_p ,callback);
       

    slave_inode_p = rozofs_get_slave_inode_from_ie(ie);
    if (slave_inode_p == NULL)
    {
      fuse_save_ctx_p->multiple_errno = EPROTO;
      severe("ientry has no slave inode pointer (%p)",ie);
      goto error;
    }
    striping_unit_bytes = rozofs_get_striping_size_from_ie(ie);
    striping_factor = rozofs_get_striping_factor_from_ie(ie);
    /*
    **___________________________________________________________________
    **         build the  truncate vector
    **  The return vector indicates how many commands must be generated)
    **____________________________________________________________________
    */
    if (ie->attrs.hybrid_desc.s.no_hybrid==1) {
      rozofs_build_truncate_multiple_offset_vector(size,&vector,striping_unit_bytes,striping_factor);
    }
    else 
    {
      uint32_t hybrid_size;
      hybrid_size = rozofs_get_hybrid_size_from_ie(ie);
      rozofs_build_truncate_multiple_offset_vector_hybrid(size,&vector,striping_unit_bytes,striping_factor,hybrid_size);
    }
      

//    rozofs_print_multi_vector(&vector,bufall_debug);
//    info("FDL \n%s",bufall_debug);
    /*
    ** Common arguments for the write
    */
    args.layout =exportclt.layout;
    args.bsize = exportclt.bsize;
    
    opcode = STORCLI_TRUNCATE;
    vers = STORCLI_VERSION;
    prog = STORCLI_PROGRAM;
    timeout_sec = ROZOFS_TMR_GET(TMR_STORCLI_PROGRAM);
    /*
    **___________________________________________________________________
    **   Build the individual storcli read command
    **____________________________________________________________________
    */
    
    
    rozofs_multi_vect_t *entry_p = &vector.vectors[0];
    for (nb_vect = 0; nb_vect < vector.nb_vectors;nb_vect++,entry_p++)    
    {
      rozofs_tx_ctx_p = NULL;
      /*
      ** Get the FID storage, cid and sids lists for the current file index
      */

      ret = rozofs_fill_storage_info_multiple(ie,&args.cid,args.dist_set,args.fid,entry_p->file_idx);
      /*
      ** Trace each individual truncate
      */
      rozofs_trc_req_io_multiple(trc_idx, srv_rozofs_ll_truncate,0,args.fid,0,entry_p->off,entry_p->file_idx);
      
#if 0
{
       char bufall_debug[1024];
      rozofs_fid2string(args.fid,bufall_debug);
      info("FDL TRUNC fid %s size %llu  file idx %d striping_unit %u striping_factor %u off %llu lastseg %u",
            bufall_debug,size,entry_p->file_idx,striping_unit_bytes,striping_factor,entry_p->off,entry_p->off%bbytes);
}
#endif
      if (ret < 0)
      {
	severe("FDL bad storage information encoding");
	fuse_save_ctx_p->multiple_errno = EINVAL;
	goto error;
      }  
      /*
      ** Update the size in the ientry for that slave inode
      */
      rozofs_slave_inode_update_size_truncate(ie,entry_p->file_idx,entry_p->off);
      
      args.bid      = entry_p->off/bbytes;
      args.last_seg = entry_p->off%bbytes;
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
	 fuse_save_ctx_p->multiple_errno = errno;
	 goto error;
      }
      /*
      ** save the file index  in the opaque[1] field of the transaction context
      */        
      rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,1,entry_p->file_idx);
      /*
      ** get the storcli to use for the transaction: it uses the STORIO fid as a primary key
      */
      storcli_idx = stclbg_storcli_idx_from_fid(args.fid);   
      /*
      ** Insert this transaction so that every other following trasnactions
      ** for the same FID will follow the same path
      */
      stclbg_hash_table_insert_ctx(&rozofs_tx_ctx_p->rw_lbg,args.fid,storcli_idx);  
      /*
      ** Get the load balancing group reference associated with the storcli
      */
      lbg_id = storcli_lbg_get_load_balancing_reference(storcli_idx);        
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
	fuse_save_ctx_p->multiple_errno = errno;
	goto error;
      }         
      /*
      ** store the reference of the xmit buffer in the transaction context: might be useful
      ** in case we want to remove it from a transmit list of the underlying network stacks
      */
      rozofs_tx_save_xmitBuf(rozofs_tx_ctx_p,xmit_buf);
      /*
      **________________________
      ** RPC Message encoding
      **________________________
      */
      /*
      ** get the pointer to the payload of the buffer
      */
      header_size_p  = (uint32_t*) ruc_buf_getPayload(xmit_buf);
      arg_p = (uint8_t*)(header_size_p+1);  
      /*
      ** create the xdr_mem structure for encoding the message
      */
      bufsize = ruc_buf_getMaxPayloadLen(xmit_buf);
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
       fuse_save_ctx_p->multiple_errno = errno;
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
    if (xdr_storcli_truncate_arg_t(&xdrs,(storcli_truncate_arg_t *)&args) == FALSE)
    {
       TX_STATS(ROZOFS_TX_ENCODING_ERROR);
       errno = EPROTO;
       fuse_save_ctx_p->multiple_errno = errno;
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
    rozofs_tx_ctx_p->recv_cbk   = truncate_buf_multiple_nb_cbk;
    rozofs_tx_ctx_p->user_param = fuse_ctx_p;    
   /*
   **__________________________________________________________________________
   ** Submit the RPC message to the selected load balancing group (storcli)
   **___________________________________________________________________________
   */
   
    /*
    ** increment the number of pending request towards the storcli
    ** increment also the number of pending requests on the rozofs fuse context
    */
    fuse_save_ctx_p->multiple_pending++; 
    rozofs_storcli_pending_req_count++;
    ret = north_lbg_send(lbg_id,xmit_buf);
    if (ret < 0)
    {
       if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;
       /*
       ** decrement the number of pending request and assert the multiple_errno
       */
       
       fuse_save_ctx_p->multiple_pending--;
       TX_STATS(ROZOFS_TX_SEND_ERROR);
       errno = EFAULT;
       fuse_save_ctx_p->multiple_errno = errno;
      goto error;  
    }
    TX_STATS(ROZOFS_TX_SEND);

    /*
    ** OK, so now finish by starting the guard timer
    */
    rozofs_tx_start_timer(rozofs_tx_ctx_p,timeout_sec);      
  }

  return 0;


error:
  /* 
  ** Release the any pending transaction context 
  */
  if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);  

  /*
  ** assert the errno and trace the output
  */
  errno = fuse_save_ctx_p->multiple_errno;
  rozofs_trc_rsp(srv_rozofs_ll_truncate,0,(unsigned char*)&trc_fid,1,trc_idx);
  /*
  ** check if the pending is 0 in the fuse context
  */
  if (fuse_save_ctx_p->multiple_pending != 0)
  {
    /*
    ** The error will be processed by the callback, so do it as it was OK
    */
    return 0;
  }

  return -1;
}      
