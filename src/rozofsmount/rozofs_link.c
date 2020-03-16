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
 * Create a hard link
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the old inode number
 * @param newparent inode number of the new parent directory
 * @param newname new name to create
 */
 void rozofs_ll_link_cbk(void *this,void *param);

void rozofs_ll_link_nb(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
        const char *newname) 
{
    ientry_t *npie = 0;
    ientry_t *ie = 0;
    epgw_link_arg_t arg;

    int    ret;        
    void *buffer_p = NULL;

    /*
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_LINK], 1);

    /*
    ** allocate a context for saving the fuse parameters
    */
    int trc_idx = rozofs_trc_req_name(srv_rozofs_ll_link,ino,(char*)newname);
    buffer_p = rozofs_fuse_alloc_saved_context();
    if (buffer_p == NULL)
    {
      severe("out of fuse saved context");
      errno = ENOMEM;
      goto error;
    }
    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,ino);
    SAVE_FUSE_PARAM(buffer_p,newparent);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);
    SAVE_FUSE_STRING(buffer_p,newname);
    
    START_PROFILING_NB(buffer_p,rozofs_ll_link);

    if (strlen(newname) > ROZOFS_FILENAME_MAX) {
        errno = ENAMETOOLONG;
        goto error;
    }

    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }

    if (!(npie = get_ientry_by_inode(newparent))) {
        errno = ENOENT;
        goto error;
    }
    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid = exportclt.eid;
    memcpy(arg.arg_gw.inode,  ie->fid, sizeof (uuid_t));
    memcpy(arg.arg_gw.newparent, npie->fid, sizeof (uuid_t));
    arg.arg_gw.newname = (char*)newname;
    /*
    ** now initiates the transaction towards the remote end
    */
#if 1
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)npie->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_LINK,(xdrproc_t) xdr_epgw_link_arg_t,(void *)&arg,
                              rozofs_ll_link_cbk,buffer_p); 
#else
    ret = rozofs_export_send_common(&exportclt,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_LINK,(xdrproc_t) xdr_epgw_link_arg_t,(void *)&arg,
                              rozofs_ll_link_cbk,buffer_p); 
#endif
    if (ret < 0) goto error;
    
    /*
    ** no error just waiting for the answer
    */
    return;

error:
    fuse_reply_err(req, errno);
    /*
    ** release the buffer if has been allocated
    */
    rozofs_trc_rsp(srv_rozofs_ll_link,ino,NULL,1,trc_idx);
    STOP_PROFILING_NB(buffer_p,rozofs_ll_link);
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
void rozofs_ll_link_cbk(void *this,void *param)
{
   struct fuse_entry_param fep;
   ientry_t *ie = 0;
   ientry_t *pie = 0;
   struct stat stbuf;
   fuse_req_t req; 
   epgw_mattr_ret_no_data_t ret ;
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   struct inode_internal_t  attrs;
   struct inode_internal_t  pattrs;
   struct rpc_msg  rpc_reply;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_mattr_ret_no_data_t;
   rozofs_fuse_save_ctx_t *fuse_ctx_p;
   errno = 0;
   int trc_idx;
   fuse_ino_t ino;
       
   GET_FUSE_CTX_P(fuse_ctx_p,param);    
   
   rpc_reply.acpted_rply.ar_results.proc = NULL;

   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,ino);
   RESTORE_FUSE_PARAM(param,trc_idx);
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
    memcpy(&pattrs, &ret.parent_attr.ep_mattr_ret_t_u.attrs, sizeof (struct inode_internal_t));
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
    /*
    ** end of decoding
    */
    if (!(ie = get_ientry_by_fid(attrs.attrs.fid))) {
        ie = alloc_ientry(attrs.attrs.fid);
    }
    
    memset(&fep, 0, sizeof (fep));
    fep.ino = ie->inode;
    mattr_to_stat(&attrs, &stbuf, exportclt.bsize);
    stbuf.st_ino = ie->inode;
    /*
    ** update the attributes in the ientry
    */
    rozofs_ientry_update(ie,&attrs);  
    stbuf.st_size = ie->attrs.attrs.size;
    if (ret.slave_ino_len !=0)
    {
      /*
      ** copy the slave inode information in the ientry of the master inode
      */
      int position;
      position = XDR_GETPOS(&xdrs); 
      rozofs_ientry_slave_inode_write(ie,ret.slave_ino_len,payload+position);
    }
    /*
    ** get the parent attributes
    */
    pie = get_ientry_by_fid(pattrs.attrs.fid);
    if (pie != NULL)
    {
      memcpy(&pie->attrs,&pattrs, sizeof (struct inode_internal_t));
      rozofs_update_timestamp(pie);
    }   
    
    fep.attr_timeout = rozofs_get_linux_caching_time_second(ie);
    /*
    Don't keep entry in cache (just for pjdtest)
    see: http://sourceforge.net/mailarchive/message.php?msg_id=28704462
    */
    fep.entry_timeout = 0;
    memcpy(&fep.attr, &stbuf, sizeof (struct stat));
    ie->nlookup++;

    rozofs_inode_t * finode = (rozofs_inode_t *) ie->attrs.attrs.fid;
    fep.generation = finode->fid[0];    

    fuse_reply_entry(req, &fep);
    goto out;
error:
    fuse_reply_err(req, errno);
out:
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_link,ino,(ie==NULL)?NULL:ie->attrs.attrs.fid,status,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_link);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
   
    return;
}
/*
**__________________________________________________________________
*/
/**
* Read symbolic link
*
* Valid replies:
*   fuse_reply_readlink
*   fuse_reply_err
*
* @param req request handle
* @param ino the inode number
*/
void rozofs_ll_readlink_cbk(void *this,void *param);

void rozofs_ll_readlink_nb(fuse_req_t req, fuse_ino_t ino) {
    ientry_t *ie = NULL;
    epgw_mfile_arg_t arg;

    int    ret;
    void *buffer_p = NULL;

    int trc_idx = rozofs_trc_req(srv_rozofs_ll_readlink,ino,NULL);

    /*
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_LINK], 1);
    
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
    
    START_PROFILING_NB(buffer_p,rozofs_ll_readlink);

    DEBUG("readlink (%lu)\n", (unsigned long int) ino);

    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }

    /*
    ** In case we have already asked the readlink few times ago,
    ** the information may still be valid 
    ** 
    */
    if (ie->symlink_target) {
      if
      ( 
        (rozofs_mode == 1) 
      || 	
	/*
	** The symlink target cached must not be over aged
	*/
	((ie->symlink_ts+rozofs_tmr_get(TMR_LINK_CACHE)*1000) > rozofs_get_ticker_us())
      )
      {
        fuse_reply_readlink(req, (char *) ie->symlink_target);
        rozofs_trc_rsp_name(srv_rozofs_ll_readlink,ino,ie->symlink_target,0,trc_idx);    
        goto out;
      }
    }

    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid = exportclt.eid;
    memcpy(arg.arg_gw.fid,ie->fid, sizeof (uuid_t));
    /*
    ** now initiates the transaction towards the remote end
    */
#if 1
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)ie->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_READLINK,(xdrproc_t) xdr_epgw_mfile_arg_t,(void *)&arg,
                              rozofs_ll_readlink_cbk,buffer_p); 
#else
    ret = rozofs_export_send_common(&exportclt,ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM),EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_READLINK,(xdrproc_t) xdr_epgw_mfile_arg_t,(void *)&arg,
                              rozofs_ll_readlink_cbk,buffer_p); 
#endif
    if (ret < 0) goto error;
    
    /*
    ** no error just waiting for the answer
    */
    return;    
error:
    rozofs_trc_rsp(srv_rozofs_ll_readlink,ino,NULL,1,trc_idx);
    fuse_reply_err(req, errno);
    
out:    
    STOP_PROFILING_NB(buffer_p,rozofs_ll_readlink);
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
void rozofs_ll_readlink_cbk(void *this,void *param)
{   
   fuse_req_t req; 
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   epgw_readlink_ret_t  ret;
   struct rpc_msg  rpc_reply;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_readlink_ret_t;
   int trc_idx;
   fuse_ino_t ino;
   ientry_t *ie = NULL;
   char     *pChar;
    
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_PARAM(param,ino);
   
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
    if (ret.status_gw.status == EP_FAILURE) {
        errno = ret.status_gw.ep_readlink_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    
    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }
    
    /*
    ** Invert pointers from ie and rpc response
    */
    /* save ie symlink pointer */
    pChar = ie->symlink_target;
    /* stole pointer from the response for the ie */
    ie->symlink_target = ret.status_gw.ep_readlink_ret_t_u.link;
    /* Give old symlink pointer to the response */
    ret.status_gw.ep_readlink_ret_t_u.link = pChar;
    /* If there is no old ie pointer, simulate a failure to avoid xdfree to free */
    if (ret.status_gw.ep_readlink_ret_t_u.link == NULL) {
      ret.status_gw.status = EP_FAILURE;
    }
    ie->symlink_ts = rozofs_get_ticker_us();
      
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
              
    /*
    ** end of decoding
    */
    fuse_reply_readlink(req, (char *) ie->symlink_target);
    goto out;
error:
    fuse_reply_err(req, errno);
out:
    /*
    ** release the transaction context and the fuse context
    */
    STOP_PROFILING_NB(param,rozofs_ll_readlink);
    rozofs_trc_rsp_name(srv_rozofs_ll_readlink,ino,ie?ie->symlink_target:NULL,status,trc_idx);    
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);     
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    return;
}


/*
**__________________________________________________________________
*/
/**
* Create a symbolic link
*
* Valid replies:
*   fuse_reply_entry
*   fuse_reply_err
*
* @param req request handle
* @param link the contents of the symbolic link
* @param parent inode number of the parent directory
* @param name to create
*/
void rozofs_ll_symlink_cbk(void *this,void *param) ;

void rozofs_ll_symlink_nb(fuse_req_t req, const char *link, fuse_ino_t parent,
        const char *name) 
{
    ientry_t *ie = 0;
    epgw_symlink2_arg_t arg;
    const struct fuse_ctx *ctx;
    ctx = fuse_req_ctx(req);
    int    ret;        
    void *buffer_p = NULL;

    /*
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_LINK], 1);

    int trc_idx = rozofs_trc_req_name(srv_rozofs_ll_symlink,parent,(char*)name);

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

    START_PROFILING_NB(buffer_p,rozofs_ll_symlink);

    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,parent);
    SAVE_FUSE_STRING(buffer_p,name);
    char * newname = (char*) link;
    SAVE_FUSE_STRING(buffer_p,newname);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);

    DEBUG("symlink (%s,%lu,%s)", link, (unsigned long int) parent, name);

    if (strlen(name) > ROZOFS_FILENAME_MAX) {
        errno = ENAMETOOLONG;
        goto error;
    }
    
    if (strlen(link) > ROZOFS_PATH_MAX) {
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
    arg.arg_gw.link = (char*)link;
    arg.arg_gw.name = (char*)name;    
    arg.arg_gw.uid  = ctx->uid;
    arg.arg_gw.gid  = ctx->gid;
        /*
    ** now initiates the transaction towards the remote end
    */
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)ie->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_SYMLINK2,(xdrproc_t) xdr_epgw_symlink2_arg_t,(void *)&arg,
                              rozofs_ll_symlink_cbk,buffer_p); 

    if (ret < 0) goto error;
    
    /*
    ** no error just waiting for the answer
    */
    return;
error:
    rozofs_trc_rsp(srv_rozofs_ll_symlink,parent,NULL,1,trc_idx);

    fuse_reply_err(req, errno);
    /*
    ** release the buffer if has been allocated
    */
    STOP_PROFILING_NB(buffer_p,rozofs_ll_symlink);
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

void rozofs_ll_symlink_cbk(void *this,void *param) 
{
   struct fuse_entry_param fep;
   ientry_t *nie = 0;
   ientry_t *pie = 0;
   struct stat stbuf;
   fuse_req_t req; 
   epgw_mattr_ret_no_data_t ret ;
   struct rpc_msg  rpc_reply;
   int trc_idx;
   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   struct inode_internal_t  attrs;
   struct inode_internal_t  pattrs;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_mattr_ret_no_data_t;
   rozofs_fuse_save_ctx_t *fuse_ctx_p;

   errno = 0;
       
   GET_FUSE_CTX_P(fuse_ctx_p,param);    
   
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,trc_idx);
      
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
    memcpy(&pattrs, &ret.parent_attr.ep_mattr_ret_t_u.attrs, sizeof (struct inode_internal_t));
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
    /*
    ** end of message decoding
    */

    if (!(nie = get_ientry_by_fid(attrs.attrs.fid))) {
        nie = alloc_ientry(attrs.attrs.fid);
    }
    else {
      recycle_ientry(nie,attrs.attrs.fid);
    }
        
    uint64_t time_us = rozofs_get_ticker_us();
    
    /*
    ** Save target of symbolic link in ie
    */
    nie->symlink_target  = fuse_ctx_p->newname;
    fuse_ctx_p->newname  = NULL;
    nie->symlink_ts      = time_us;
    
    memset(&fep, 0, sizeof (fep));
    fep.ino = nie->inode;
    mattr_to_stat(&attrs, &stbuf, exportclt.bsize);
    stbuf.st_ino = nie->inode;
    /*
    ** update the attributes in the ientry
    */
    rozofs_ientry_update(nie,&attrs);  
    stbuf.st_size = nie->attrs.attrs.size;
    /*
    ** get the parent attributes
    */
    pie = get_ientry_by_fid(pattrs.attrs.fid);
    if (pie != NULL)
    {
      memcpy(&pie->attrs,&pattrs, sizeof (struct inode_internal_t));
      rozofs_update_timestamp(pie);
    }  
        
    fep.attr_timeout = rozofs_get_linux_caching_time_second(nie);
    fep.entry_timeout = rozofs_tmr_get_entry(0);
    memcpy(&fep.attr, &stbuf, sizeof (struct stat));
    nie->nlookup++;
    fuse_reply_entry(req, &fep);
    goto out;
error:

    fuse_reply_err(req, errno);
out:
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_symlink,(nie==NULL)?0:nie->inode,(nie==NULL)?NULL:nie->attrs.attrs.fid,status,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_symlink);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    
    return;
}


/*
**__________________________________________________________________
*/
/**
 * Remove a link
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the old inode number
 * @param newparent inode number of the new parent directory
 * @param newname new name to create
 */
void rozofs_ll_unlink_cbk(void *this,void *param);

void rozofs_ll_unlink_nb(fuse_req_t req, fuse_ino_t parent, const char *name) {
    ientry_t *ie = 0;
    epgw_unlink_arg_t arg;    
    void *buffer_p = NULL;
    int    ret;        

    /*
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_FDEL], 1);
    
    int trc_idx = rozofs_trc_req_name(srv_rozofs_ll_unlink,parent,(char*)name);
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
    SAVE_FUSE_PARAM(buffer_p,trc_idx);
    SAVE_FUSE_STRING(buffer_p,name);
    
    START_PROFILING_NB(buffer_p,rozofs_ll_unlink);

    DEBUG("unlink (%lu,%s)\n", (unsigned long int) parent, name);

    if (!(ie = get_ientry_by_inode(parent))) {
        errno = ENOENT;
        goto error;
    }
    if (strlen(name) > ROZOFS_FILENAME_MAX) {
        errno = ENAMETOOLONG;
        goto error;
    }    

    /*
    ** update the statistics
    */
    rzkpi_file_stat_update(ie->pfid,(int)0,RZKPI_DELETE);
            
    arg.arg_gw.eid = exportclt.eid;
    memcpy(arg.arg_gw.pfid, ie->fid, sizeof (uuid_t));
    arg.arg_gw.name = (char*)name;    
        
    /*
    ** now initiates the transaction towards the remote end
    */
#if 1
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)ie->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_UNLINK,(xdrproc_t) xdr_epgw_unlink_arg_t,(void *)&arg,
                              rozofs_ll_unlink_cbk,buffer_p); 
#else
    ret = rozofs_export_send_common(&exportclt,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_UNLINK,(xdrproc_t) xdr_epgw_unlink_arg_t,(void *)&arg,
                              rozofs_ll_unlink_cbk,buffer_p); 
#endif
    if (ret < 0) goto error;
    
    /*
    ** no error : anticipate unlink response
    */
    fuse_reply_err(req, 0);
    return;
    
error:
    fuse_reply_err(req, errno);
    /*
    ** release the buffer if has been allocated
    */
    rozofs_trc_rsp(srv_rozofs_ll_unlink,parent,NULL,1,trc_idx);
    STOP_PROFILING_NB(buffer_p,rozofs_ll_unlink);
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
void rozofs_ll_unlink_cbk(void *this,void *param)
{
//   fuse_req_t req; 
   epgw_fid_ret_t ret ;
   int status;
   ientry_t *pie = 0;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   struct rpc_msg  rpc_reply;
   ientry_t *ie = 0;
   fid_t     fid;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_fid_ret_t;
   rozofs_fuse_save_ctx_t *fuse_ctx_p;
   int trc_idx;
   fuse_ino_t parent;
   errno = 0;
   struct inode_internal_t  pattrs;
       
   GET_FUSE_CTX_P(fuse_ctx_p,param);    
   
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   //RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,parent);
   RESTORE_FUSE_PARAM(param,trc_idx);
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
    ** ok now call the procedure to decode the message
    */
    memset(&ret,0, sizeof(ret));                
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       xdr_free(decode_proc, (char *) &ret);
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
        errno = ret.status_gw.ep_fid_ret_t_u.error;
        xdr_free(decode_proc, (char *) &ret);    
        goto error;
    }
    memcpy(fid, &ret.status_gw.ep_fid_ret_t_u.fid, sizeof (fid_t));
    /*
    ** end of decoding
    */
    if ((ie = get_ientry_by_fid(fid))) {
        // Update nlookup
        //ie->nlookup--;
        // Invalidate attrs cache
        ie->timestamp = 0;
        // Update nlink
        if (ie->attrs.attrs.nlink > 0)
            ie->attrs.attrs.nlink--;
    }
    /*
    ** get the parent attributes
    */
    memcpy(&pattrs, &ret.parent_attr.ep_mattr_ret_t_u.attrs, sizeof (struct inode_internal_t));
    xdr_free(decode_proc, (char *) &ret);    
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
      rozofs_update_timestamp(pie);
    }    
    goto out;
error:
out:
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_unlink,parent,(ie==NULL)?NULL:ie->attrs.attrs.fid,status,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_unlink);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
   
    return;
}
