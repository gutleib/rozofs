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

#include <inttypes.h>
#include <malloc.h>

#include <rozofs/rpc/eproto.h>

#include "rozofs_fuse_api.h"
#include <rozofs/core/rozofs_string.h>
#include <rozofs/common/xmalloc.h>
#include "rozofs_kpi.h"
#include "rozofs_cachetrack.h"

DECLARE_PROFILING(mpp_profiler_t);

void rozofs_assert_open_mode(struct fuse_file_info *fi)
{
      switch (rozofs_cache_mode)
      {
         default:
	 case 0:
	   if (fi->flags & O_WRONLY)
	   {
	     /*
	     ** NFS case to handle the case of the writeback cache : invalidate cache for reader case only
	     */
	     fi->keep_cache = 1;
	   }
	  break;
	 
	 /* Direct I/O */
	 case 1:
	  fi->direct_io = 1;
	 break;
	 
	 /* Keep cache */
	 case 2:
	  fi->keep_cache = 1;
	 break;
      
      }
}

/**
* Open a file
*
* Open flags (with the exception of O_CREAT, O_EXCL, O_NOCTTY and
* O_TRUNC) are available in fi->flags.
*
* Filesystem may store an arbitrary file handle (pointer, index,
* etc) in fi->fh, and use this in other all other file operations
* (read, write, flush, release, fsync).
*
* Filesystem may also implement stateless file I/O and not store
* anything in fi->fh.
*
* There are also some flags (direct_io, keep_cache) which the
* filesystem may set in fi, to change the way the file is opened.
* See fuse_file_info structure in <fuse_common.h> for more details.
*
* Valid replies:
*   fuse_reply_open
*   fuse_reply_err
*
* @param req request handle
* @param ino the inode number
* @param fi file information
*/
void rozofs_ll_open_cbk(void *this,void *param);
void rozofs_ll_open_nb(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    ientry_t *ie = 0;
    int    ret;        
    void *buffer_p = NULL;
    epgw_mfile_arg_t arg;
    file_t *file = NULL;
    errno = 0;

    /*
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_OTHER], 1);

    int trc_idx = rozofs_trc_req_flags(srv_rozofs_ll_open,ino,NULL,fi->flags);
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

    START_PROFILING_NB(buffer_p,rozofs_ll_open);

    DEBUG("open (%lu)\n", (unsigned long int) ino);

    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }
    /*
    ** check if it is configured in block mode, in that case we avoid
    ** a transaction with the exportd
    */
    if ((rozofs_mode == 1) ||
       ((ie->timestamp+rozofs_tmr_get_attr_us(rozofs_is_directory_inode(ino))) > rozofs_get_ticker_us()))
    {
      /*
      ** allocate a context for the file descriptor
      */
      file = rozofs_file_working_var_init(ie,ie->fid);
      rozofs_assert_open_mode(fi);
      /*
      ** save the opening flags
      */
      file->open_flags = fi->flags;

      fi->fh = (unsigned long) file;
      /*
      ** update the statistics
      */
      rzkpi_file_stat_update(ie->pfid,(int)0,RZKPI_OPEN);
      /*
      ** Take care of the file caching cache for hybrid SSD/HDD configuration
      */
      rzcachetrack_file(ie->pfid,ie->attrs.attrs.size,ie->attrs.attrs.mtime);
      /*
      ** send back response to fuse
      */
      rz_fuse_reply_open(req, fi);
      goto out; 
    }    
    /*
    ** get the attributes of the file
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
	goto short_cut;           
      }      
    } 

#if 1
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,ie->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_GETATTR,(xdrproc_t) xdr_epgw_mfile_arg_t,(void *)&arg,
                              rozofs_ll_open_cbk,buffer_p); 
#else
    ret = rozofs_export_send_common(&exportclt,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_GETATTR,(xdrproc_t) xdr_epgw_mfile_arg_t,(void *)&arg,
                              rozofs_ll_open_cbk,buffer_p); 

#endif
    if (ret < 0) {
      /*
      ** In case of fast reconnect mode let's respond with the previously knows 
      ** parameters instead of failing
      */
      if (common_config.client_fast_reconnect) {
short_cut:
	/*
	** allocate a context for the file descriptor
	*/
	file = rozofs_file_working_var_init(ie,ie->fid);
        rozofs_assert_open_mode(fi);
	/*
	** save the opening flags
	*/
	file->open_flags = fi->flags;
	fi->fh = (unsigned long) file;
	/*
	** update the statistics
	*/
	rzkpi_file_stat_update(ie->pfid,(int)0,RZKPI_OPEN);
	/*
	** Take care of the file caching cache for hybrid SSD/HDD configuration
	*/
	rzcachetrack_file(ie->pfid,ie->attrs.attrs.size,ie->attrs.attrs.mtime);
	/*
	** send back response to fuse
	*/
	rz_fuse_reply_open(req, fi);
	goto out;         
      }
      goto error;  
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
out:
    rozofs_trc_rsp_attr(srv_rozofs_ll_open,(fuse_ino_t)file,(ie==NULL)?NULL:ie->attrs.attrs.fid,(errno==0)?0:1,(ie==NULL)?-1:ie->attrs.attrs.size,trc_idx);
    STOP_PROFILING_NB(buffer_p,rozofs_ll_open);
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
void rozofs_ll_open_cbk(void *this,void *param) 
{    
  ientry_t *ie = 0;
  file_t *file = NULL;
  fuse_ino_t ino;
   fuse_req_t req; 
   struct fuse_file_info *fi ;  
   epgw_mattr_ret_no_data_t ret ;
   int status;
   struct rpc_msg  rpc_reply;
   
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   struct inode_internal_t  attr;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_mattr_ret_no_data_t;
   rozofs_fuse_save_ctx_t *fuse_ctx_p;
   errno = 0;
   int trc_idx;
       
   GET_FUSE_CTX_P(fuse_ctx_p,param);    
      
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,ino);
   RESTORE_FUSE_PARAM(param,fi);
   RESTORE_FUSE_PARAM(param,trc_idx);
   
//    uint8_t rozofs_safe = rozofs_get_rozofs_safe(exportclt.layout);
//    uint8_t rozofs_forward = rozofs_get_rozofs_forward(exportclt.layout);
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
       
       if ((errno == ETIME) && (common_config.client_fast_reconnect)) {
	 /*
	 ** In case of fast reconnect mode let's respond with the previously knows 
	 ** parameters instead of failing
	 */
	 if (!(ie = get_ientry_by_inode(ino))) {
             errno = ENOENT;
             goto error;
	 }
	 /*
	 ** allocate a context for the file descriptor
	 */
	 file = rozofs_file_working_var_init(ie,ie->fid);
         rozofs_assert_open_mode(fi);
	 fi->fh = (unsigned long) file;
         file->open_flags = fi->flags;
	 /*
	 ** update the statistics
	 */
	 rzkpi_file_stat_update(ie->pfid,(int)0,RZKPI_OPEN);
	 /*
	 ** send back response to fuse
	 */
	 rz_fuse_reply_open(req, fi);
	 errno = EAGAIN;
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
    memcpy(&attr, &ret.status_gw.ep_mattr_ret_t_u.attrs, sizeof (struct inode_internal_t));
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
    /*
    ** end of the the decoding part
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
    if (rozofs_bugwatch){
        char fid_str[37];
        rozofs_uuid_unparse(file->fid, fid_str);
        severe("BUGROZOFSWATCH (open:%p),FID(%s) size=%"PRIu64"", file,
                    fid_str, ie->attrs.attrs.size);
    }
    
    /*
    ** allocate a context for the file descriptor
    */
    file = rozofs_file_working_var_init(ie,ie->fid);
    
    /*
    ** init of the variable used for buffer management
    */
    rozofs_assert_open_mode(fi);    
    fi->fh = (unsigned long) file;
    file->open_flags = fi->flags;
    /*
    ** update the statistics
    */
    rzkpi_file_stat_update(ie->pfid,(int)0,RZKPI_OPEN);
    /*
    ** Take care of the file caching cache for hybrid SSD/HDD configuration
    */
    rzcachetrack_file(ie->pfid,ie->attrs.attrs.size,ie->attrs.attrs.mtime);    
    
    rz_fuse_reply_open(req, fi);
    goto out;
error:
    if (file)
    {
       /*
       ** need to release the file structure and the buffer
       */
       int xerrno = errno;
       file_close(file);
       errno = xerrno;      
    }
    fuse_reply_err(req, errno);
out:
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp_attr(srv_rozofs_ll_open,(fuse_ino_t)file,(ie==NULL)?NULL:ie->attrs.attrs.fid,status,(ie==NULL)?-1:ie->attrs.attrs.size,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_open);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    return;
}
