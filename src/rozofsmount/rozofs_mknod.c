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
#include "rozofs_kpi.h"

DECLARE_PROFILING(mpp_profiler_t);


/*
**__________________________________________________________________
*/
/**
 * Create file node
 *
 * Create a regular file, character device, block device, fifo or
 * socket node.
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the parent directory
 * @param name to create
 * @param mode file type and mode with which to create the new file
 * @param rdev the device number (only valid if created file is a device)
 */

void rozofs_ll_mknod_cbk(void *this,void *param);
 
void rozofs_ll_mknod_nb(fuse_req_t req, fuse_ino_t parent, const char *name,
        mode_t mode, dev_t rdev) 
{
    ientry_t *ie = 0;
    const struct fuse_ctx *ctx;
    ctx = fuse_req_ctx(req);
    epgw_mknod_arg_t arg;

    int    ret;
    int trc_idx = rozofs_trc_req_name(srv_rozofs_ll_mknod,parent,(char*)name);
    void *buffer_p = NULL;
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
    SAVE_FUSE_PARAM(buffer_p,parent);
    SAVE_FUSE_STRING(buffer_p,name);
    SAVE_FUSE_PARAM(buffer_p,mode);
    SAVE_FUSE_PARAM(buffer_p,rdev);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);
    
    START_PROFILING_NB(buffer_p,rozofs_ll_mknod);
    
    DEBUG("mknod (%lu,%s,%04o,%08lX)\n", (unsigned long int) parent, name,
            (unsigned int) mode, (unsigned long int) rdev);

    if (strlen(name) > ROZOFS_FILENAME_MAX) {
        errno = ENAMETOOLONG;
        goto error;
    }
    if (!(ie = get_ientry_by_inode(parent))) {
        errno = ENOENT;
        goto error;
    }
    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid = exportclt.eid;
    memcpy(arg.arg_gw.parent,ie->fid, sizeof (uuid_t));
    arg.arg_gw.name = (char*)name;    
    arg.arg_gw.uid  = ctx->uid;
    arg.arg_gw.gid  = ctx->gid;
    arg.arg_gw.mode = mode;
    arg.hdr.gateway_rank = rozofs_get_site_number();     
    /*
    ** now initiates the transaction towards the remote end
    */
#if 1
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,ie->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_MKNOD,(xdrproc_t) xdr_epgw_mknod_arg_t,(void *)&arg,
                              rozofs_ll_mknod_cbk,buffer_p); 
#else
    ret = rozofs_export_send_common(&exportclt,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_MKNOD,(xdrproc_t) xdr_epgw_mknod_arg_t,(void *)&arg,
                              rozofs_ll_mknod_cbk,buffer_p); 
#endif
    if (ret < 0) goto error;
    

    /*
    ** no error just waiting for the answer
    */


    return;
error:
    rozofs_trc_rsp(srv_rozofs_ll_mknod,parent,NULL,1,trc_idx);
    fuse_reply_err(req, errno);
    STOP_PROFILING_NB(buffer_p,rozofs_ll_mknod);
    if (buffer_p != NULL) rozofs_fuse_release_saved_context(buffer_p);
    return;
}
        
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_mknod_cbk(void *this,void *param)
{

   struct fuse_entry_param fep;
   ientry_t *nie = 0;
   ientry_t *pie = 0;
   struct stat stbuf;
   fuse_req_t req; 
   epgw_mattr_ret_t ret ;
   struct rpc_msg  rpc_reply;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_mattr_ret_t;

   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   struct inode_internal_t  attrs;
   struct inode_internal_t  pattrs;
   rozofs_fuse_save_ctx_t *fuse_ctx_p;
   errno = 0;
   int trc_idx;
    
   GET_FUSE_CTX_P(fuse_ctx_p,param);    
   
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,trc_idx);
    /*
    ** get the pointer to the transaction context:
    ** it is required to get the information related to the receive buffer
    */
    rozofs_tx_ctx_t  *rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;     
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
    */
    memset(&ret,0, sizeof(ret));                            
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       xdr_free((xdrproc_t) decode_proc, (char *) &ret);
       goto error;
    }   
    /*
    **  This gateway do not support the required eid 
    */    
    if (ret.status_gw.status == EP_FAILURE_EID_NOT_SUPPORTED) {    

        /*
        ** Do not try to select this server again for the eid
        ** but directly send to the exportd
        */
        expgw_routing_expgw_for_eid(&fuse_ctx_p->expgw_routing_ctx, ret.hdr.eid, EXPGW_DOES_NOT_SUPPORT_EID);       

        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    

        /* 
        ** Attempt to re-send the request to the exportd and wait being
        ** called back again. One will use the same buffer, just changing
        ** the xid.
        */
        status = rozofs_expgateway_resend_routing_common(rozofs_tx_ctx_p, NULL,param); 
        if (status == 0)
        {
          /*
          ** do not forget to release the received buffer
          */
          ruc_buf_freeBuffer(recv_buf);
          recv_buf = NULL;
          return;
        }           
        /*
        ** Not able to resend the request
        */
        errno = EPROTO; /* What else ? */
        goto error;
         
    }

    if (ret.status_gw.status == EP_FAILURE) {
        errno = ret.status_gw.ep_mattr_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    
    /*
    ** Update eid free quota
    */
    eid_set_free_quota(ret.free_quota);  
        
    memcpy(&attrs, &ret.status_gw.ep_mattr_ret_t_u.attrs, sizeof (struct inode_internal_t));
    /*
    ** get the parent attributes
    */
    memcpy(&pattrs, &ret.parent_attr.ep_mattr_ret_t_u.attrs, sizeof (struct inode_internal_t));

    xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
    /*
    ** end of decoding
    */
    if (!(nie = get_ientry_by_fid(attrs.attrs.fid))) {
        nie = alloc_ientry(attrs.attrs.fid);
    }
    else {
      recycle_ientry(nie,attrs.attrs.fid);
    }
    
    memset(&fep, 0, sizeof (fep));
    fep.ino = nie->inode;
    mattr_to_stat(&attrs, &stbuf,exportclt.bsize);
    stbuf.st_ino = nie->inode;
    /*
    ** update the attributes in the ientry
    */
    memcpy(&nie->attrs,&attrs, sizeof (struct inode_internal_t));
    /**
    *  update the timestamp in the ientry context
    */
    nie->timestamp = rozofs_get_ticker_us();
    /*
    ** get the parent attributes
    */
    pie = get_ientry_by_fid(pattrs.attrs.fid);
    if (pie != NULL)
    {
      memcpy(&pie->attrs,&pattrs, sizeof (struct inode_internal_t));
      /**
      *  update the timestamp in the ientry context
      */
      pie->timestamp = rozofs_get_ticker_us();
      ientry_update_parent(nie,pie->fid);
    }    
    /*
    ** check the length of the file, and update the ientry if the file size returned
    ** by the export is greater than the one found in ientry
    */
    if (nie->attrs.attrs.size < stbuf.st_size) nie->attrs.attrs.size = stbuf.st_size;
    stbuf.st_size = nie->attrs.attrs.size;
        
    fep.attr_timeout = rozofs_tmr_get_attr(0);
    fep.entry_timeout = rozofs_tmr_get_entry(0);
    memcpy(&fep.attr, &stbuf, sizeof (struct stat));
    nie->nlookup++;
    
    rozofs_inode_t * finode = (rozofs_inode_t *) nie->attrs.attrs.fid;
    fep.generation = finode->fid[0];  
    /*
    ** update the statistics
    */
    rzkpi_file_stat_update(nie->pfid,(int)0,RZKPI_MKNOD);
        
    fuse_reply_entry(req, &fep);
    goto out;
error:
    fuse_reply_err(req, errno);
out:
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_mknod,(nie==NULL)?0:nie->inode,(nie==NULL)?NULL:nie->attrs.attrs.fid,status,trc_idx);

    STOP_PROFILING_NB(param,rozofs_ll_mknod);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);        
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    return;
}
