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
#include <rozofs/rpc/storcli_proto.h>

#include "rozofs_fuse_api.h"
#include "rozofs_modeblock_cache.h"
#include "rozofs_rw_load_balancing.h"
#include "rozofs_io_error_trc.h"

DECLARE_PROFILING(mpp_profiler_t);
int rozofs_max_getattr_pending = 0;
uint64_t rozofs_max_getattr_duplicate = 0;

/*
**__________________________________________________________________
*/
/**
 * Get file attributes
 *
 * Valid replies:
 *   fuse_reply_attr
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi for future use, currently always NULL
 */
 void rozofs_ll_getattr_cbk(void *this,void *param); 

 
 void rozofs_ll_getattr_nb(fuse_req_t req, fuse_ino_t ino,
        struct fuse_file_info *fi) 
{
    (void) fi;
    ientry_t *ie = 0;
    epgw_mfile_arg_t arg;
    int ret;
    struct stat stbuf;
    int trc_idx;
    errno = 0;
    uint64_t attr_us = rozofs_tmr_get_attr_us(rozofs_is_directory_inode(ino));

    /*
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_GETATTR], 1);

    trc_idx = rozofs_trc_req(srv_rozofs_ll_getattr,ino,NULL);
    DEBUG("getattr for inode: %lu\n", (unsigned long int) ino);
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
    SAVE_FUSE_PARAM(buffer_p,ino);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);
    SAVE_FUSE_STRUCT(buffer_p,fi,sizeof( struct fuse_file_info));
    START_PROFILING_NB(buffer_p,rozofs_ll_getattr);

    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }
    /*
    ** In block mode the attributes of regular files are directly retrieved 
    ** from the ie entry. For directories and links one ask to the exportd
    ** 
    */
    if (
         /* check regular file */
         ((((ie->timestamp+attr_us) > rozofs_get_ticker_us()) || (rozofs_mode == 1))&&(S_ISREG(ie->attrs.attrs.mode))) ||
	 /* check directory */
	 (((ie->pending_getattr_cnt>0)||((ie->timestamp+attr_us) > rozofs_get_ticker_us()))&&(S_ISDIR(ie->attrs.attrs.mode)))
	 ) 
    {
      mattr_to_stat(&ie->attrs, &stbuf, exportclt.bsize);
      stbuf.st_ino = ino; 
      rz_fuse_reply_attr(req, &stbuf, rozofs_tmr_get_attr(rozofs_is_directory_inode(ino)));
      goto out;   
    }
    /*
    ** fill up the structure that will be used for creating the xdr message
    */  
    arg.arg_gw.eid = exportclt.eid;
    memcpy(arg.arg_gw.fid, ie->fid, sizeof (uuid_t));
    /*
    ** now initiates the transaction towards the remote end
    */

    /*
    ** In case the EXPORT LBG is down and we know this ientry, let's respond to
    ** the requester with the current available information
    */
    if (common_config.client_fast_reconnect) {
      expgw_tx_routing_ctx_t routing_ctx; 
      
      if (expgw_get_export_routing_lbg_info(arg.arg_gw.eid,ie->fid,&routing_ctx) != 0) {
         goto error;
      }
      if (north_lbg_get_state(routing_ctx.lbg_id[0]) != NORTH_LBG_UP) {
        if (ie->attrs.attrs.mtime != 0) {
	  mattr_to_stat(&ie->attrs, &stbuf, exportclt.bsize);
	  stbuf.st_ino = ino; 
          rz_fuse_reply_attr(req, &stbuf, rozofs_tmr_get_attr(rozofs_is_directory_inode(ino)));
	  goto out;           
	} 
      }	     
    }  

#if 1
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,ie->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_GETATTR,(xdrproc_t) xdr_epgw_mfile_arg_t,(void *)&arg,
                              rozofs_ll_getattr_cbk,buffer_p); 
    
#else
    ret = rozofs_export_send_common(&exportclt,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_GETATTR,(xdrproc_t) xdr_epgw_mfile_arg_t,(void *)&arg,
                              rozofs_ll_getattr_cbk,buffer_p); 
#endif
    if (ret < 0) {
      /*
      ** In case of fast reconnect mode let's respond with the previously knows 
      ** parameters instead of failing
      */
      if (common_config.client_fast_reconnect) {
        if (ie->attrs.attrs.mtime != 0) {      
	  mattr_to_stat(&ie->attrs, &stbuf, exportclt.bsize);
	  stbuf.st_ino = ino; 
	  rz_fuse_reply_attr(req, &stbuf, rozofs_tmr_get_attr(rozofs_is_directory_inode(ino)));
	  goto out;           
	}
      }	
      goto error;    
    }  
    /*
    ** no error just waiting for the answer: increment the pending counter of getattr
    */
    //ie->pending_getattr_cnt++;
    //if (ie->pending_getattr_cnt > rozofs_max_getattr_pending) rozofs_max_getattr_pending = ie->pending_getattr_cnt;
    //if (ie->pending_getattr_cnt > 1) rozofs_max_getattr_duplicate++;
    return;

error:
    fuse_reply_err(req, errno);
    /*
    ** release the buffer if has been allocated
    */
out:
    rozofs_trc_rsp_attr(srv_rozofs_ll_getattr,0/*ino*/,(ie==NULL)?NULL:ie->attrs.attrs.fid,(errno==0)?0:1,(ie==NULL)?-1:ie->attrs.attrs.size,trc_idx);
    STOP_PROFILING_NB(buffer_p,rozofs_ll_getattr);
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
void rozofs_ll_getattr_cbk(void *this,void *param) 
{
   fuse_ino_t ino;
   struct stat stbuf;
   fuse_req_t req; 
   epgw_mattr_ret_no_data_t ret ;
   int status;
   ientry_t *ie = 0;
   ientry_t *pie = 0;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_mattr_ret_no_data_t;
   
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   struct inode_internal_t  attr;
   struct inode_internal_t  pattr;
   int      parent_attribute_valid = 0;
   struct rpc_msg  rpc_reply;
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   rozofs_fuse_save_ctx_t *fuse_ctx_p;
   int trc_idx;
   errno = 0;
    
   GET_FUSE_CTX_P(fuse_ctx_p,param);    
   
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
       /*
       ** In case of fast reconnect mode let's respond with the previously knows 
       ** parameters instead of failing
       */
       if ((common_config.client_fast_reconnect)&&(errno==ETIME)) {
         ie = get_ientry_by_inode(ino);
	 if ((ie != NULL) && (ie->attrs.attrs.mtime != 0)) { 
 	   mattr_to_stat(&ie->attrs, &stbuf, exportclt.bsize);
	   stbuf.st_ino = ino; 
	   rz_fuse_reply_attr(req, &stbuf, rozofs_tmr_get_attr(rozofs_is_directory_inode(ino)));
	   errno = EAGAIN;	   
	   goto out; 
	 }            
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
       goto error;         
    }
    payload  = (uint8_t*) ruc_buf_getPayload(recv_buf);
    payload += sizeof(uint32_t); /* skip length*/

    /*
    ** OK now decode the received message
    */
    bufsize = rozofs_tx_get_small_buffer_size();
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
        
    memcpy(&attr, &ret.status_gw.ep_mattr_ret_t_u.attrs, sizeof (struct inode_internal_t));
    /*
    ** get the parent attributes
    */
    if (ret.parent_attr.status == EP_SUCCESS)
    {
       memcpy(&pattr, &ret.parent_attr.ep_mattr_ret_t_u.attrs, sizeof (struct inode_internal_t));
       parent_attribute_valid = 1;
    }
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
    /*
    ** end of the the decoding part
    */
    if (parent_attribute_valid)
    {
      /*
      ** get the parent attributes
      */
      pie = get_ientry_by_fid(pattr.attrs.fid);
      if (pie != NULL)
      {
	memcpy(&pie->attrs,&pattr, sizeof (struct inode_internal_t));
	/**
	*  update the timestamp in the ientry context
	*/
	pie->timestamp = rozofs_get_ticker_us();
      }           
    }
    /*
    ** store the decoded information in the array that will be
    ** returned to the caller
    */
    mattr_to_stat(&attr, &stbuf, exportclt.bsize);
    stbuf.st_ino = ino;
    /*
    ** get the ientry associated with the fuse_inode
    */

    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }
    /*
    ** update the attributes in the ientry
    */
    rozofs_ientry_update(ie,&attr);  
    if (ret.slave_ino_len !=0)
    {
      /*
      ** copy the slave inode information in the ientry of the master inode
      */
      int position;
      position = XDR_GETPOS(&xdrs); 
      rozofs_ientry_slave_inode_write(ie,ret.slave_ino_len,payload+position);
    }
    stbuf.st_size = ie->attrs.attrs.size;
    /*
    ** update the getattr pending count
    */
    //if (ie->pending_getattr_cnt>=0) ie->pending_getattr_cnt--;
    rz_fuse_reply_attr(req, &stbuf, rozofs_tmr_get_attr(rozofs_is_directory_inode(ino)));
    goto out;
error:
    fuse_reply_err(req, errno);
out:
    rozofs_trc_rsp_attr(srv_rozofs_ll_getattr,ino,(ie==NULL)?NULL:ie->attrs.attrs.fid,status,(ie==NULL)?-1:ie->attrs.attrs.size,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_getattr);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    
    return;
}


/**
*  Flush all write pending on a given ientry 

 @param ie : pointer to the ientry in the cache
 
 @retval none

 */

void rozofs_truncate_pending_write_buffer(ientry_t * ie) {
    file_t              * f;
    uint64_t              attr_size;

    f = ie->write_pending;
    
    if (f == NULL) return; 

    if (f->ie != ie) return;
    /*
    ** Get the new size from the ientry
    */
    attr_size = ie->attrs.attrs.size;
    /*
    ** Check if the content of the buffer shoudl be ignored
    */
    if (f->write_from > attr_size) 
    {
      /*
      **  ignore the content of the buffer
      */
      CLEAR_WRITE(f);
      CLEAR_READ(f);
      return;
    }
    /*
    ** check the write_pos
    */
    if (f->write_pos >= attr_size) 
    {
      f->write_pos = attr_size;
      return;
    }
}


/*
**__________________________________________________________________
*/
/**
* Set file attributes
*
* In the 'attr' argument only members indicated by the 'to_set'
* bitmask contain valid values.  Other members contain undefined
* values.
*
* If the setattr was invoked from the ftruncate() system call
* under Linux kernel versions 2.6.15 or later, the fi->fh will
* contain the value set by the open method or will be undefined
* if the open method didn't set any value.  Otherwise (not
* ftruncate call, or kernel version earlier than 2.6.15) the fi
* parameter will be NULL.
*
* Valid replies:
*   fuse_reply_attr
*   fuse_reply_err
*
* @param req request handle
* @param ino the inode number
* @param attr the attributes
* @param to_set bit mask of attributes which should be set
* @param fi file information, or NULL
*
* Changed in version 2.5:
*     file information filled in for ftruncate
*/
void rozofs_ll_setattr_cbk(void *this,void *param); 
void rozofs_ll_truncate_cbk(void *this,void *param); 
void rozofs_resize(fuse_req_t req, ientry_t *ie, void *buffer_p, int trc_idx) ;

void rozofs_ll_setattr_nb(fuse_req_t req, fuse_ino_t ino, struct stat *stbuf,
        int to_set, struct fuse_file_info *fi) 
{
    ientry_t *ie = 0;
    mattr_t attr;
    epgw_setattr_arg_t arg;
    int     ret;
    void *buffer_p = NULL;
    int trc_idx;
    int lkup_cpt = 0;
    struct stat o_stbuf;
    uint32_t readahead = 0;
    uint64_t buf_flush_offset;
    /*
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_OTHER], 1);

    uint32_t bbytes = ROZOFS_BSIZE_BYTES(exportclt.bsize);    
    /*
    ** set to attr the attributes that must be set: indicated by to_set
    */
    stat_to_mattr(stbuf, &attr, to_set);
    trc_idx = rozofs_trc_req_setattr(srv_rozofs_ll_setattr,ino,NULL,to_set, &attr);    

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
    SAVE_FUSE_PARAM(buffer_p,readahead);
    SAVE_FUSE_STRUCT(buffer_p,fi,sizeof( struct fuse_file_info));
    START_PROFILING_NB(buffer_p,rozofs_ll_setattr);
    /*
    ** If the client is configured to operate in asynchronous mode for the setattr, it is indicated
    ** by setting lkup_cpt to 1
    */
    if ((common_config.async_setattr) || (conf.asyncsetattr)) lkup_cpt = 1;
    SAVE_FUSE_PARAM(buffer_p,lkup_cpt);   
     
    DEBUG("setattr for inode: %lu\n", (unsigned long int) ino);

    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }
    
    
    /*
    ** W A R N I N G
    **
    ** This awfull trick is to recompute the file size from the read size on the
    ** storages in case the export is not up to date after a switchover
    **
    ** W A R N I N G
    **
    ** This request will require resources on the STOTCLI side while it has
    ** been read from fuse because EXPORT side resources are available !!!!!
    ** The xon/xoff mechanism can not be used with FUSE kernel module
    **
    */
    if ((to_set & FUSE_SET_ATTR_ATIME) && (to_set & FUSE_SET_ATTR_ATIME)
    &&  (attr.mtime == ROZOFS_RESIZEM)  && (attr.atime == ROZOFS_RESIZEA)) {
       return rozofs_resize(req, ie, buffer_p, trc_idx); 
    }
    
    /*
    ** address the case of the file truncate: update the size of the ientry
    ** when the file is truncated
    */
    if (to_set & FUSE_SET_ATTR_SIZE)
    {
      /*
      ** case of the truncate
      ** start by updating the storaged and then updates the exportd
      ** need to store the setattr attributes in the fuse context
      */

      // Check file size 
      if (attr.size >= ROZOFS_FILESIZE_MAX) {
        errno = EFBIG;
        goto error;
      } 

      /*
      ** Check quota are respected
      */
      if (!eid_check_free_quota(exportclt.bsize, ie->attrs.attrs.size, attr.size)) {
        goto error;// errno is already set
      }         

      /*
      ** indicates that there is a pending file size update
      */
      ie->mtime_locked = 1;      
      ie->attrs.attrs.size           = attr.size;
      ie->file_extend_running = 1;
      ie->file_extend_pending = 0; 
      ie->file_extend_size    = 0;               
      /*
      ** adjust the write_from & write_pos according to the new size
      ** before flushing iny data on disk since in asynchronous mode
      ** we can have a wrong size in the local ientry
      */
      rozofs_truncate_pending_write_buffer(ie);
      /*
      ** Flush on disk any pending data in any buffer open on this file 
      ** before reading.
      */
      flush_write_ientry(ie); 
      /*
      ** increment the read_consistency counter to inform opened file descriptor that
      ** their data must be read from disk
      */
      ie->read_consistency++;  
      
      /*
      ** When no0trunc is configured, do not send truncate to zero to the storages.
      ** Only send the set attribute to the exportd
      */
      if((!conf.no0trunc)||(attr.size!=0)) {

	storcli_truncate_arg_t  args;
	int ret;
	int storcli_idx;
	uint64_t bid;
	uint16_t last_seg;
	/*
	** flush the entry from the modeblock cache: to goal it to avoid returning
	** non zero data when the file has been truncated. Another way to do it was
	** to find out the 8K blocks that are impacted, but this implies more complexity
	** in the cache management for something with is not frequent. So it is better 
	** to flush the entry and keep the performance of the caceh for regular usage
	*/
	rozofs_mbcache_remove(ie->fid);
	/*
	** translate the size in a block index for the storaged
	*/
	bid = attr.size / bbytes;
	last_seg = (attr.size % bbytes);
	/*
	** save the size as the offset to write, the length is always 0
	*/
	buf_flush_offset =  attr.size;
	SAVE_FUSE_PARAM(buffer_p,buf_flush_offset);

	SAVE_FUSE_PARAM(buffer_p,to_set);
	SAVE_FUSE_STRUCT(buffer_p,stbuf,sizeof(struct stat ));      

       /*
	** indicates that there is a pending truncate: so the settattr that will come later on, will have
	** the readahead flag asserted
	*/
	readahead = 1;
	SAVE_FUSE_PARAM(buffer_p,readahead);
	/*
	** increment the number of pending setattr for which the file size is impacted
	*/
	ie->pending_setattr_with_size_update++;
	/*
	** Check if the ientry designates a file with multiple inodes
	*/	
        if(ie->attrs.multi_desc.common.master != 0)
	{
	   /*
	   ** multiple inode case: multiple inodes per user's file
	   */
	   ret = truncate_buf_multitple_nb(buffer_p, ie,attr.size);
	}
	else
	{
           /*
	   **  Default mode: one inde per user file
	   **
	   **  Fill the truncate request:
	   **  we take the file information in terms of cid, distribution from the attributes
	   **  saved in the ientry
	   */
	   args.cid = ie->attrs.attrs.cid;
	   args.layout = exportclt.layout;
	   args.bsize = exportclt.bsize;
	   memcpy(args.dist_set, ie->attrs.attrs.sids, sizeof (sid_t) * ROZOFS_SAFE_MAX);
	   memcpy(args.fid, ie->fid, sizeof (fid_t));
	   ret = rozofs_fill_storage_info(ie,&args.cid,args.dist_set,args.fid);
	   if (ret < 0)
	   {
	     severe("bad storage information encoding");
	     goto error;
	   }
	   args.bid      = bid;
	   args.last_seg = last_seg;
	   /*
	   ** get the storcli to use for the transaction
	   */      
	   storcli_idx = stclbg_storcli_idx_from_fid(ie->fid);
	   /*
	   ** now initiates the transaction towards the remote end
	   */
	   ret = rozofs_storcli_send_common(NULL,ROZOFS_TMR_GET(TMR_STORCLI_PROGRAM),STORCLI_PROGRAM, STORCLI_VERSION,
                                     STORCLI_TRUNCATE,(xdrproc_t) xdr_storcli_truncate_arg_t,(void *)&args,
                                     rozofs_ll_truncate_cbk,buffer_p,storcli_idx,ie->fid); 
	}
	if (ret < 0) { 
	  goto error;
        }
        if (lkup_cpt) goto async_setattr;
	/*
	** all is fine, wait from the response of the storcli and then updates the exportd upon
	** receiving the answer from storcli
	*/
	return;
      }	
    }

       
    /*
    ** This is a MTIME restoration 
    */
    if (to_set & FUSE_SET_ATTR_MTIME) {  
      file_t              * f;
	 
      /*
      ** Set the lock on mtime to prevent previous writes 
      ** to trigger a write block that would update the mtime
      ** to the current time and cancal the mtime restoration.
      */      
      ie->mtime_locked = 1; 
      /*
      ** The size is not given as argument of settattr, 
      ** nevertheless a size modification is pending or running.
      ** The mtime_locked will prevent sending the write_block 
      ** so let's send the size modification along with 
      ** the other modified attributes.
      */
      if ((ie->file_extend_pending)||(ie->file_extend_running)) {
	to_set |= FUSE_SET_ATTR_SIZE;
	attr.size = ie->attrs.attrs.size;  
	stbuf->st_size = attr.size;
      }	    

      /*
      ** Check whether any write is pending in some buffer open on this file by any application
      */
      if ((f = ie->write_pending) != NULL) 
      {     	
	 /*
	 ** flush any pending data
	 */
	 flush_write_ientry(ie); 
	 /*
	 ** increment the read_consistency counter to inform opened file descriptor that
	 ** their data must be read from disk
	 */
	 ie->read_consistency++;  
      }
    }      
    /*
    ** set the argument to encode
    */ 
    arg.arg_gw.eid = exportclt.eid;
    memcpy(&arg.arg_gw.attrs, &attr, sizeof (mattr_t));
    memcpy(arg.arg_gw.attrs.fid, ie->fid, sizeof (fid_t));
    arg.arg_gw.to_set = to_set;
    /*
    ** now initiates the transaction towards the remote end
    */
#if 1
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,ie->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_SETATTR,(xdrproc_t) xdr_epgw_setattr_arg_t,(void *)&arg,
                              rozofs_ll_setattr_cbk,buffer_p); 

#else
    ret = rozofs_export_send_common(&exportclt,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_SETATTR,(xdrproc_t) xdr_epgw_setattr_arg_t,(void *)&arg,
                              rozofs_ll_setattr_cbk,buffer_p); 
#endif

    if (ret < 0) goto error;
    /*
    ** Request has been sent, and size modification has been added to the messages
    */
    if (ie->mtime_locked) {  
      if (ie->file_extend_pending) {
	ie->file_extend_running = 1;
	ie->file_extend_pending = 0; 
	ie->file_extend_size    = 0;        
      }  
    }     
    if (lkup_cpt) goto async_setattr;
    /*
    ** no error just waiting for the answer
    */
    return;    
error:
    if ((readahead) && (ie!=NULL))
    {
      /*
      ** decrement the number of pending setattr for which the file size is impacted
      */
      ie->pending_setattr_with_size_update--;
      if ( ie->pending_setattr_with_size_update <= 0)
      {
	ie->pending_setattr_with_size_update = 0;
      }
      /*
      ** log the I/O error return upon the failure while attempting to submit the write request towards a storcli
      */
      {
	int save_errno = errno;

	rozofs_iowr_err_log(ie->fid,buf_flush_offset,0,errno);   
	errno = save_errno; 
      }          
    }
    fuse_reply_err(req, errno);
    /*
    ** release the buffer if has been allocated
    */
    rozofs_trc_rsp(srv_rozofs_ll_setattr,ino,NULL,1,trc_idx);
    STOP_PROFILING_NB(buffer_p,rozofs_ll_setattr);
    if (buffer_p != NULL) rozofs_fuse_release_saved_context(buffer_p);
    return;

async_setattr:
    /*
    ** update the attributes in the ientry
    */
    stat_to_mattr(stbuf, &ie->attrs.attrs, to_set);
    mattr_to_stat(&ie->attrs, &o_stbuf, exportclt.bsize);
    o_stbuf.st_ino = ino;
    rz_fuse_reply_attr(req, &o_stbuf, rozofs_tmr_get_attr(rozofs_is_directory_inode(ino)));
    rozofs_trc_rsp(srv_rozofs_ll_setattr,ino,NULL,1,trc_idx);
    STOP_PROFILING_NB(buffer_p,rozofs_ll_setattr);
}

/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_setattr_cbk(void *this,void *param) 
{
    fuse_ino_t ino;
    ientry_t *ie = 0;
    ientry_t *pie = 0;
//    struct fuse_file_info *fi = NULL;
    struct stat o_stbuf;
    fuse_req_t req; 
    epgw_mattr_ret_no_data_t ret ;
    struct inode_internal_t  attr;
    uint32_t readahead = 0;
    struct inode_internal_t  pattr;
    int      parent_attribute_valid = 0;   
    int status;
    uint8_t  *payload;
    void     *recv_buf = NULL;   
    XDR       xdrs;    
    int      bufsize;
   struct rpc_msg  rpc_reply;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_mattr_ret_no_data_t;
   rozofs_fuse_save_ctx_t *fuse_ctx_p;
   errno = 0;
   int lkup_cpt = 0;
   int trc_idx;
    
   GET_FUSE_CTX_P(fuse_ctx_p,param);    

   rpc_reply.acpted_rply.ar_results.proc = NULL;

    RESTORE_FUSE_PARAM(param,req);
    RESTORE_FUSE_PARAM(param,ino);
    RESTORE_FUSE_PARAM(param,trc_idx);
    RESTORE_FUSE_PARAM(param,lkup_cpt);   
    RESTORE_FUSE_PARAM(param,readahead);   
    
//    RESTORE_FUSE_STRUCT_PTR(param,fi);
    /*
    ** get the pointer to the transaction context:
    ** it is required to get the information related to the receive buffer
    */
    rozofs_tx_ctx_t      *rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;     
    /*
    ** get the ientry associated with the inode (will be used later)
    */
    ie = get_ientry_by_inode(ino);
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
    payload += sizeof(uint32_t); /* skip length */
    /*
    ** OK now decode the received message
    */
    bufsize = rozofs_tx_get_small_buffer_size();
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
    
    memcpy(&attr, &ret.status_gw.ep_mattr_ret_t_u.attrs, sizeof (struct inode_internal_t));  
    /*
    ** get the parent attributes
    */
    if (ret.parent_attr.status == EP_SUCCESS)
    {
       memcpy(&pattr, &ret.parent_attr.ep_mattr_ret_t_u.attrs, sizeof (struct inode_internal_t));
       parent_attribute_valid = 1;
    } 
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
    /*
    ** end of the the decoding part
    */
    if (parent_attribute_valid)
    {
      /*
      ** get the parent attributes
      */
      pie = get_ientry_by_fid(pattr.attrs.fid);
      if (pie != NULL)
      {
	memcpy(&pie->attrs,&pattr, sizeof (struct inode_internal_t));
	/**
	*  update the timestamp in the ientry context
	*/
	pie->timestamp = rozofs_get_ticker_us();
      }           
    }    
    mattr_to_stat(&attr, &o_stbuf, exportclt.bsize);
    o_stbuf.st_ino = ino;
    /*
    ** get the ientry associated with the fuse_inode
    */
    if (ie== NULL) {
        errno = ENOENT;
        goto error;
    }
    /*
    ** update the attributes in the ientry
    */
    rozofs_ientry_update(ie,&attr);    
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
    ** clear the running flag in case of a time modification
    */
    if (readahead)
    {
      /*
      ** decrement the number of pending setattr for which the file size is impacted
      */
      ie->pending_setattr_with_size_update--;
      if ( ie->pending_setattr_with_size_update <= 0)
      {
	ie->pending_setattr_with_size_update = 0;
      }    
    }
    if (ie->mtime_locked) {  
      if (ie->pending_setattr_with_size_update == 0) ie->file_extend_running = 0;
    }
    /*
    ** update the size in the buffer returned to fuse
    */
    o_stbuf.st_size = ie->attrs.attrs.size;

    if (lkup_cpt == 0) 
    {
      rz_fuse_reply_attr(req, &o_stbuf, rozofs_tmr_get_attr(rozofs_is_directory_inode(ino)));
    }

    goto out;
error:
    if (lkup_cpt == 0) fuse_reply_err(req, errno);
    
    if ((readahead) && (ie!=NULL))
    {
      /*
      ** decrement the number of pending setattr for which the file size is impacted
      */
      ie->pending_setattr_with_size_update--;
      if ( ie->pending_setattr_with_size_update <= 0)
      {
	ie->pending_setattr_with_size_update = 0;
      }     
    }
out:
    rozofs_trc_rsp_attr(srv_rozofs_ll_setattr,ino,(ie==NULL)?NULL:ie->attrs.attrs.fid,status,(ie==NULL)?-1:ie->attrs.attrs.size,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_setattr);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);  
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
     return;
}



/*
**__________________________________________________________________
*/
/**
* Truncate  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_truncate_cbk(void *this,void *param) 
{
   struct rpc_msg  rpc_reply;
   struct stat *stbuf;
   fuse_ino_t ino;
   ientry_t *ie = 0;
   fuse_req_t req; 
   mattr_t attr;
   int to_set;
   epgw_setattr_arg_t arg;
   uint64_t buf_flush_offset;
       
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   storcli_status_ret_t ret;
   int retcode;
   int lkup_cpt=0;
   int trc_idx;
   xdrproc_t decode_proc = (xdrproc_t)xdr_storcli_status_ret_t;
   uint32_t readahead = 0;

   rpc_reply.acpted_rply.ar_results.proc = NULL;
   /*
   ** update the number of storcli pending request
   */
   if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;

   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,ino);
   RESTORE_FUSE_PARAM(param,to_set);
   RESTORE_FUSE_STRUCT_PTR(param,stbuf);      
   RESTORE_FUSE_PARAM(param,lkup_cpt);   
    RESTORE_FUSE_PARAM(param,trc_idx);
    RESTORE_FUSE_PARAM(param,readahead);
    RESTORE_FUSE_PARAM(param,buf_flush_offset);
    /*
    ** Update the exportd with the filesize if that one has changed
    */ 
    ie = get_ientry_by_inode(ino);
    if (ie != NULL)
    {
      ie->file_extend_pending = 0;
      ie->file_extend_size    = 0;       
    }
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
       //severe(" transaction error %s",strerror(errno));
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
       severe(" transaction error %s",strerror(errno));
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
     severe(" transaction error %s",strerror(errno));
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
       severe(" transaction error %s",strerror(errno));
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
    ** Keep the fuse context since we need to trigger the update of 
    ** the attributes of the file
    */
    rozofs_tx_free_from_ptr(rozofs_tx_ctx_p); 
    ruc_buf_freeBuffer(recv_buf);  
    /*
    ** exit in error if the ientry context is not found
    */
    if (ie == NULL) {
        errno = ENOENT;
        goto error;
    }
    rozofs_trc_rsp(srv_rozofs_ll_setattr,ino,NULL,1,trc_idx);

    /*
    ** set to attr the attributes that must be set: indicated by to_set
    */
    stat_to_mattr(stbuf, &attr,to_set);
    /*
    ** set the argument to encode
    */
    arg.arg_gw.eid = exportclt.eid;
    memcpy(&arg.arg_gw.attrs, &attr, sizeof (mattr_t));
    memcpy(arg.arg_gw.attrs.fid, ie->fid, sizeof (fid_t));
    arg.arg_gw.to_set = to_set;
    /*
    ** now initiates the transaction towards the remote end
    */
#if 1
    retcode = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,ie->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_SETATTR,(xdrproc_t) xdr_epgw_setattr_arg_t,(void *)&arg,
                              rozofs_ll_setattr_cbk,param); 

#else
    retcode = rozofs_export_send_common(&exportclt,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_SETATTR,(xdrproc_t) xdr_epgw_setattr_arg_t,(void *)&arg,
                              rozofs_ll_setattr_cbk,param); 
#endif

    if (retcode < 0) goto error;
    /*
    ** indicates that an file size update is in progress: in that case the file size to consider
    ** is the one found in the ientry context
    */
    ie->file_extend_running = 1;
    /*
    ** now wait for the response of the exportd
    */
    return;

error:
    if ((readahead) && (ie!=NULL))
    {
      /*
      ** decrement the number of pending setattr for which the file size is impacted
      */
      ie->pending_setattr_with_size_update--;
      if ( ie->pending_setattr_with_size_update <= 0)
      {
	ie->pending_setattr_with_size_update = 0;
      }
      /*
      ** log the I/O error return upon the failure while attempting to submit the write request towards a storcli
      */
      {
	int save_errno = errno;

	rozofs_iowr_err_log(ie->fid,buf_flush_offset,0,errno);   
	errno = save_errno; 
      }             
    }
    /*
    ** reply to fuse and release the transaction context and the fuse context
    */
    if (lkup_cpt == 0) fuse_reply_err(req, errno);
    STOP_PROFILING_NB(param,rozofs_ll_setattr);
    
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    
    return;
}
