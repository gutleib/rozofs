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

#include <rozofs/rpc/eproto.h>

#include "rozofs_fuse_api.h"

DECLARE_PROFILING(mpp_profiler_t);

/*
** cache value of last stats (for asynchronous case
*/
ep_statfs_t rozofs_cache_estat;
uint64_t  rozofs_cache_estat_time = 0; 
uint32_t  rozofs_cache_stat_null = 0; 

/**
* Get file system statistics
*
* Valid replies:
*   fuse_reply_statfs
*   fuse_reply_err
*
* @param req request handle
* @param ino the inode number, zero means "undefined"
*/
 void rozofs_ll_statfs_cbk(void *this,void *param) ;
 
 
void rozofs_ll_statfs_nb(fuse_req_t req, fuse_ino_t ino) {
    (void) ino;
    int    ret;        
    void *buffer_p = NULL;
    int trc_idx = rozofs_trc_req(srv_rozofs_ll_statfs,ino,NULL);
    uint64_t attr_us = common_config.statfs_period;
    struct statvfs st={0};

   attr_us = attr_us*0x100000;
    /*
    ** allocate a context for saving the fuse parameters
    */
    buffer_p = rozofs_fuse_alloc_saved_context();
    if (buffer_p == NULL)
    {
      severe("out of fuse saved context");
      errno = ENOMEM;
      goto error;
    }
    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,ino);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);

    START_PROFILING_NB(buffer_p,rozofs_ll_statfs);
    if ((rozofs_cache_estat_time+attr_us) > rozofs_get_ticker_us())
    {
       goto async_statfs;   
    }
    /*
    ** now initiates the transaction towards the remote end
    */
    

    /*
    ** In case the EXPORT LBG is down let's respond to
    ** the requester with the current available information
    */
    {
      expgw_tx_routing_ctx_t routing_ctx; 
      
      if (expgw_get_export_routing_lbg_info(exportclt.eid,NULL,&routing_ctx) != 0) {
         goto async_statfs;
      }
      if (north_lbg_get_state(routing_ctx.lbg_id[0]) != NORTH_LBG_UP) {
	goto async_statfs;           
      }      
    } 
        
#if 1
    ret = rozofs_expgateway_send_routing_common(exportclt.eid,(unsigned char*)NULL,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_STATFS,(xdrproc_t) xdr_uint32_t,(void *)&exportclt.eid,
                              rozofs_ll_statfs_cbk,buffer_p); 
#else
    ret = rozofs_export_send_common(&exportclt,ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM),EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_STATFS,(xdrproc_t) xdr_uint32_t,(void *)&exportclt.eid,
                              rozofs_ll_statfs_cbk,buffer_p); 
#endif
    if (ret < 0) {
      goto async_statfs;
    }
    /*
    ** no error just waiting for the answer
    */
    return;
error:
    fuse_reply_err(req, errno);
    /*
    ** release the buffer if has been allocated
    */
    STOP_PROFILING_NB(buffer_p,rozofs_ll_statfs);
    if (buffer_p != NULL) rozofs_fuse_release_saved_context(buffer_p);
    rozofs_trc_rsp_attr(srv_rozofs_ll_statfs,ino,NULL,1,-1,trc_idx);

    return;
    
async_statfs:
    memset(&st, 0, sizeof(struct statvfs));
    st.f_blocks = rozofs_cache_estat.blocks; // + estat.bfree;
    st.f_bavail = st.f_bfree = rozofs_cache_estat.bfree;
    st.f_frsize = st.f_bsize = rozofs_cache_estat.bsize;
    st.f_favail = st.f_ffree = rozofs_cache_estat.ffree;
    st.f_files = rozofs_cache_estat.files + rozofs_cache_estat.ffree;
    st.f_namemax = rozofs_cache_estat.namemax;

    fuse_reply_statfs(req, &st);
    errno = EAGAIN;
    /*
    ** release the buffer if has been allocated
    */
    STOP_PROFILING_NB(buffer_p,rozofs_ll_statfs);
    if (buffer_p != NULL) rozofs_fuse_release_saved_context(buffer_p);
    rozofs_trc_rsp_attr(srv_rozofs_ll_statfs,ino,NULL,1,st.f_bavail*st.f_frsize/1024,trc_idx);

}


/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
 void rozofs_ll_statfs_cbk(void *this,void *param) 
{
   fuse_req_t req; 
   struct statvfs st={0};
   ep_statfs_t estat;
   int status=-1;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   epgw_statfs_ret_t ret;
   struct rpc_msg  rpc_reply;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_statfs_ret_t;
   int trc_idx;
   fuse_ino_t ino;
   
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_PARAM(param,ino);

    /*
    ** get the pointer to the transaction context:
    ** it is required to get the information related to the receive buffer
    */
    rozofs_tx_ctx_t      *rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;     
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
       if (errno == ETIME) {
          memset(&st, 0, sizeof(struct statvfs));
          st.f_blocks = rozofs_cache_estat.blocks; // + estat.bfree;
          st.f_bavail = st.f_bfree = rozofs_cache_estat.bfree;
          st.f_frsize = st.f_bsize = rozofs_cache_estat.bsize;
          st.f_favail = st.f_ffree = rozofs_cache_estat.ffree;
          st.f_files = rozofs_cache_estat.files + rozofs_cache_estat.ffree;
          st.f_namemax = rozofs_cache_estat.namemax;

          fuse_reply_statfs(req, &st);
          goto out;          
       }
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
       status = -1;
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
     status = -1;
     goto error;
    }
    /*
    ** ok now call the procedure to encode the message
    */
    memset((char *)&ret, 0, sizeof(ret));
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       xdr_free((xdrproc_t) decode_proc, (char *) &ret);
       status = -1;
       errno = EPROTO;       
       goto error;
    }   
    if (ret.status_gw.status == EP_FAILURE) {
        errno = ret.status_gw.ep_statfs_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret); 
        status = -1;
        goto error;
    }
    memcpy(&estat, &ret.status_gw.ep_statfs_ret_t_u.stat, sizeof (ep_statfs_t));
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
    /*
    ** end of decoding section
    */
    
    /*
    ** Suddenly there is no block available. This might be due to
    ** an exportd switchover. The new exportd has not yet been able
    ** to poll every storaged.
    */
    if ((estat.blocks == 0) && (rozofs_cache_stat_null < 3)) {
      rozofs_cache_stat_null++;      
      memset(&st, 0, sizeof(struct statvfs));
      st.f_blocks = rozofs_cache_estat.blocks; // + estat.bfree;
      st.f_bavail = st.f_bfree = rozofs_cache_estat.bfree;
      st.f_frsize = st.f_bsize = rozofs_cache_estat.bsize;
      st.f_favail = st.f_ffree = rozofs_cache_estat.ffree;
      st.f_files = rozofs_cache_estat.files + rozofs_cache_estat.ffree;
      st.f_namemax = rozofs_cache_estat.namemax;
      errno = EAGAIN;
      status = -1;
    }
    else {
      rozofs_cache_stat_null = 0;
      memset(&st, 0, sizeof(struct statvfs));
      st.f_blocks = estat.blocks; // + estat.bfree;
      st.f_bavail = st.f_bfree = estat.bfree;
      st.f_frsize = st.f_bsize = estat.bsize;
      st.f_favail = st.f_ffree = estat.ffree;
      st.f_files = estat.files + estat.ffree;
      st.f_namemax = estat.namemax;
      errno = 0;
      status = 0; 
      memcpy(&rozofs_cache_estat,&estat,sizeof (ep_statfs_t));
    }   
   /*
   ** cache the last statfs
   */
   rozofs_cache_estat_time = rozofs_get_ticker_us();

    fuse_reply_statfs(req, &st);
    /*
    ** check if some more write blocks are pending
    */
    rozofs_export_write_block_list_process();    
    goto out;
error:
    fuse_reply_err(req, errno);
out:
    /*
    ** release the transaction context and the fuse context
    */
    STOP_PROFILING_NB(param,rozofs_ll_statfs);
    rozofs_trc_rsp_attr(srv_rozofs_ll_statfs,ino,NULL,status,st.f_bavail*st.f_frsize/1024,trc_idx);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    return;
}
