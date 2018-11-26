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

/** Rename a file
*
* If the target exists it should be atomically replaced. If
* the target's inode's lookup count is non-zero, the file
* system is expected to postpone any removal of the inode
* until the lookup count reaches zero (see description of the
* forget function).
*
* Valid replies:
*   fuse_reply_err
*
* @param req request handle
* @param parent inode number of the old parent directory
* @param name old name
* @param newparent inode number of the new parent directory
* @param newname new name
*/
void rozofs_ll_rename_cbk(void *this,void *param) ;


void rozofs_ll_rename_nb(fuse_req_t req, fuse_ino_t parent, const char *name,
        fuse_ino_t newparent, const char *newname) 
{
    epgw_rename_arg_t arg;
    ientry_t *pie = 0;
    ientry_t *npie = 0;
    int    ret;        
    void *buffer_p = NULL;

    /*
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_OTHER], 1);

    /*
    ** allocate a context for saving the fuse parameters
    */
    int trc_idx = rozofs_trc_req_name(srv_rozofs_ll_rename,parent,(char*)newname);
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

    START_PROFILING_NB(buffer_p,rozofs_ll_rename);

    DEBUG("rename (%lu,%s,%lu,%s)\n", (unsigned long int) parent, name,
            (unsigned long int) newparent, newname);

    if (strlen(name) > ROZOFS_FILENAME_MAX ||
            strlen(newname) > ROZOFS_FILENAME_MAX) {
        errno = ENAMETOOLONG;
        goto error;
    }
    if (!(pie = get_ientry_by_inode(parent))) {
        errno = ENOENT;
        goto error;
    }
    if (!(npie = get_ientry_by_inode(newparent))) {
        errno = ENOENT;
        goto error;
    }

    /*
    ** Clear the timestamp of the new parent & old parent to force RozoFS to
    ** get the last attributes of each of them
    */
    pie->timestamp = 0;
    npie->timestamp = 0;

    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid = exportclt.eid;
    memcpy(arg.arg_gw.pfid,pie->fid, sizeof (uuid_t));
    arg.arg_gw.name = (char*)name;    
    memcpy(arg.arg_gw.npfid, npie->fid, sizeof (fid_t));
    arg.arg_gw.newname = (char*)newname;    
    /*
    ** now initiates the transaction towards the remote end
    */
#if 1
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char *)arg.arg_gw.pfid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_RENAME,(xdrproc_t) xdr_epgw_rename_arg_t,(void *)&arg,
                              rozofs_ll_rename_cbk,buffer_p); 
#else
    ret = rozofs_export_send_common(&exportclt,ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM),EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_RENAME,(xdrproc_t) xdr_epgw_rename_arg_t,(void *)&arg,
                              rozofs_ll_rename_cbk,buffer_p); 
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
    rozofs_trc_rsp(srv_rozofs_ll_rename,parent,NULL,1,trc_idx);
    STOP_PROFILING_NB(buffer_p,rozofs_ll_rename);
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
void rozofs_ll_rename_cbk(void *this,void *param) 
{    
   ientry_t *old_ie = 0;
   ientry_t *nie = 0;
   fid_t fid;
   fuse_req_t req; 
   epgw_rename_ret_t ret;
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   struct rpc_msg  rpc_reply;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_rename_ret_t;
   fuse_ino_t parent;
   int trc_idx;
   errno = 0;
   
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   RESTORE_FUSE_PARAM(param,req);
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
        errno = ret.status_gw.ep_fid_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    memcpy(fid, &ret.status_gw.ep_fid_ret_t_u.fid, sizeof (fid_t));   
    /*
    ** end of decoding section
    */
    if ((old_ie = get_ientry_by_fid(fid))) {
        //old_ie->nlookup--;
    }

    /*
    ** Update renamed FID attributes
    */
    memcpy(fid, &ret.child_attr.ep_mattr_ret_t_u.attrs.fid, sizeof (fid_t));
    if ((nie = get_ientry_by_fid(fid))) {
      /*
      ** update the attributes in the ientry
      */
      rozofs_ientry_update(nie,(struct inode_internal_t  *)&ret.child_attr.ep_mattr_ret_t_u.attrs);  
    }
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);     
    fuse_reply_err(req, 0);
    goto out;
error:
    fuse_reply_err(req, errno);
out:
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_rename,parent,(old_ie==0)?NULL:old_ie->attrs.attrs.fid,status,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_rename);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    return;
}
