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

//#define TRACE_FS_READ_WRITE 1
//#warning TRACE_FS_READ_WRITE active
#include <inttypes.h>

#include <rozofs/rpc/eproto.h>
#include <rozofs/rpc/storcli_proto.h>

#include "rozofs_fuse_api.h"
#include "rozofs_sharedmem.h"
#include "rozofs_modeblock_cache.h"
#include "rozofs_cache.h"
#include "rozofs_rw_load_balancing.h"
#include "rozofs_fuse_thread_intf.h"
#include "rozofs_kpi.h"

DECLARE_PROFILING(mpp_profiler_t);

int export_force_write_block_asynchrone(void *fuse_ctx_p,  ientry_t *ientry, uint64_t from, uint64_t to);

/*
**__________________________________________________________________
** Resize final call back 
**
** @param this        pointer to the transaction context
** @param param       pointer to the associated rozofs_fuse_context
** 
**__________________________________________________________________
*/
void rozofs_ll_resize_cbk(void *this,void *param) {
  int                           trc_idx;
  fuse_req_t                    req; 
  struct rpc_msg                rpc_reply;
  int                           status;
  uint8_t                     * payload;
  void                        * recv_buf = NULL;   
  XDR                           xdrs;    
  int                           bufsize;
  storcli_read_ret_no_data_t    ret;
  xdrproc_t                     decode_proc = (xdrproc_t)xdr_storcli_read_ret_no_data_t;
  ientry_t                    * ie=NULL;
  fuse_ino_t                    ino;
  struct stat                   o_stbuf;
  uint64_t                      old_size = 0;
  int                           write_block=0;  
  rozofs_tx_ctx_t             * rozofs_tx_ctx_p;
  uint64_t                      new_size = 0;
  rozofs_shared_buf_rd_hdr_t  * share_rd_p; 
  rozofs_fuse_save_ctx_t      * fuse_save_ctx_p;
  int                           cmdIdx;
  
  rpc_reply.acpted_rply.ar_results.proc = NULL;

  /*
  ** Get the pointer to the rozofs fuse context associated with the multiple file write
  */
  fuse_save_ctx_p = (rozofs_fuse_save_ctx_t*)ruc_buf_getPayload(param);
  trc_idx         = fuse_save_ctx_p->trc_idx;
  ino             = fuse_save_ctx_p->ino;
  share_rd_p      = (rozofs_shared_buf_rd_hdr_t*)ruc_buf_getPayload(fuse_save_ctx_p->shared_buf_ref);
  req             = fuse_save_ctx_p->req;
  
  /*
  ** update the number of storcli pending request
  */
  if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;

  errno = 0;
    
  /*
  ** get the pointer to the transaction context:
  ** it is required to get the information related to the receive buffer
  */
  rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;     
  /*    
  ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
  */
  status = rozofs_tx_get_status(this);
  if (status < 0) {
     /*
     ** something wrong happened
     */
     errno = rozofs_tx_get_errno(this);  
     fuse_reply_err(req, errno);
     goto out; 
  }
  /*
  ** get the pointer to the receive buffer payload
  */
  recv_buf = rozofs_tx_get_recvBuf(this);
  if (recv_buf == NULL) {
     /*
     ** something wrong happened
     */
     errno = EFAULT;  
     fuse_reply_err(req, errno);
     goto out;         
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
  if (rozofs_xdr_replymsg(&xdrs,&rpc_reply) != TRUE) {
    TX_STATS(ROZOFS_TX_DECODING_ERROR);
    errno = EPROTO;
    fuse_reply_err(req, errno);
    goto out; 
  }
  /*
  ** ok now call the procedure to encode the message
  */
  memset(&ret,0, sizeof(ret));                    
  if (decode_proc(&xdrs,&ret) == FALSE) {
     TX_STATS(ROZOFS_TX_DECODING_ERROR);
     errno = EPROTO;
     xdr_free((xdrproc_t) decode_proc, (char *) &ret);
     fuse_reply_err(req, errno);
     goto out; 
  }   
  if (ret.status == STORCLI_FAILURE) {
    /*
    ** ENOENT means size 0 an musr not be treated as an error
    */
    if (ret.storcli_read_ret_no_data_t_u.error != ENOENT) {  
      errno = ret.storcli_read_ret_no_data_t_u.error;
      xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
      fuse_reply_err(req, errno);
      goto out;
    }  	
  }
  xdr_free((xdrproc_t) decode_proc, (char *) &ret);

  /*
  ** Retrieve ientry
  */
  if (!(ie = get_ientry_by_inode(ino))) {
     errno = ENOENT;
     fuse_reply_err(req, errno);
     goto out; 
  }  
  
  
  new_size = 0;
  cmdIdx   = 0;
  /*
  ** Retrieve the number of slave and master inodes to process
  */
  if (ie->attrs.multi_desc.byte == 0) {   
    /*
    ** Just one subfile in the master inode
    */
    new_size = share_rd_p->cmd[0].received_len;
  }
  else {
    int       file_idx;    
    uint32_t  hybrid_size = 0;
    uint64_t  estimated_size;
    uint64_t  nb_strip;
    uint32_t  striping_unit_bytes;
    uint32_t  striping_factor;

    striping_unit_bytes = rozofs_get_striping_size_from_ie(ie);
    striping_factor     = rozofs_get_striping_factor_from_ie(ie);
  
    /*
    ** In hybrid mode the master inode has one strip
    ** at the begining of the file
    */
    if (ie->attrs.hybrid_desc.s.no_hybrid==0) {
      new_size   = share_rd_p->cmd[cmdIdx].received_len;  
      hybrid_size = rozofs_get_hybrid_size_from_ie(ie);    
      cmdIdx++;
    }
    
    for (file_idx=1; file_idx <= striping_factor; file_idx++,cmdIdx++) {
      uint64_t len;
      len = share_rd_p->cmd[cmdIdx].received_len;
      /*
      ** Get the number of full strips before the last strip
      */
      if (len == 0) {
        /*
        ** File empty ? 
        */
        estimated_size = 0;
      }
      else {
        /*
        ** Exact number of strip
        */
        if ((len % striping_unit_bytes) == 0) {
          estimated_size = hybrid_size;
          nb_strip = (len-1)/striping_unit_bytes;
          nb_strip *= striping_factor;
          nb_strip += file_idx;
          estimated_size += (nb_strip * striping_unit_bytes);
        }
        else {  
          estimated_size = hybrid_size;
          nb_strip = len/striping_unit_bytes;
          nb_strip *= striping_factor;
          nb_strip += (file_idx-1);
          estimated_size += (nb_strip * striping_unit_bytes);
          estimated_size += (len % striping_unit_bytes);
        }
      }  
      if (estimated_size > new_size) {
        new_size =  estimated_size;
      } 
    }  
  }  

  /*
  ** Update ientry
  */


  old_size = ie->attrs.attrs.size;
    
  //info("New size %llu old size %llu",(long long unsigned int)new_size,(long long unsigned int)old_size);
  
  if (old_size < new_size) {
    //info("New size %d old size %d",old_size,new_size);
    ie->attrs.attrs.size = new_size;
    /*
    ** Update exportd
    */
    export_force_write_block_asynchrone(param,ie, old_size, new_size);
    write_block = 1;
  }
  mattr_to_stat(&ie->attrs, &o_stbuf, exportclt.bsize);
  o_stbuf.st_ino = ino;
  rz_fuse_reply_attr(req, &o_stbuf, rozofs_tmr_get_attr(0)); 

out:
  rozofs_trc_rsp_attr(srv_rozofs_ll_setattr,ino,(ie==NULL)?0:ie->attrs.attrs.fid,(errno==0)?0:1,new_size,trc_idx);
  STOP_PROFILING_NB(param,rozofs_ll_setattr);
  /*
  ** Release fuse context when no size update oward export
  */
  if (write_block == 0) {
    rozofs_fuse_release_saved_context(param);
  } 
  if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
  if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
  return;
}
/*
**__________________________________________________________________
** Resize transaction call back 
**
** @param this        pointer to the transaction context
** @param param       pointer to the associated rozofs_fuse_context
**
** opaque fields of the transaction
**  - opaque[0]: command index (index within the shared buffer)
**  - opaque[1]: index of the file
**  - opaque[2]: received length
** 
**__________________________________________________________________
*/
void rozofs_do_resize_multiple_cbk(void *this,void *param)  {
  rozofs_fuse_save_ctx_t * fuse_save_ctx_p;
  int                      status;
  void                   * recv_buf = NULL;   
  XDR                      xdrs;    
  int                      bufsize;
  uint8_t                * payload;
  storcli_read_ret_no_data_t ret;
  xdrproc_t                decode_proc = (xdrproc_t)xdr_storcli_read_ret_no_data_t;
  struct rpc_msg           rpc_reply;
  int                      trc_idx;
  fuse_ino_t               ino;
  int                      file_idx=0;
  rozofs_shared_buf_rd_hdr_t  * share_rd_p;  
  uint32_t                 cmd_idx; 
  uint32_t                 length;
  ientry_t               * ie;
  rozofs_tx_ctx_t        * rozofs_tx_ctx_p ;
    
  rpc_reply.acpted_rply.ar_results.proc = NULL;
  
  /*
  ** Get the pointer to the rozofs fuse context associated with the multiple file write
  */
  fuse_save_ctx_p = (rozofs_fuse_save_ctx_t*)ruc_buf_getPayload(param);
  trc_idx         = fuse_save_ctx_p->trc_idx;
  ino             = fuse_save_ctx_p->ino;
  share_rd_p      = (rozofs_shared_buf_rd_hdr_t*)ruc_buf_getPayload(fuse_save_ctx_p->shared_buf_ref);
  
  fuse_save_ctx_p->multiple_pending--;
  if (fuse_save_ctx_p->multiple_pending < 0) {
    /*
    ** This should not occur
    */
    fatal("multiple_pending is negative");
    return;
  }  
  
  /*
  ** Retrieve the file index in the opaque field
  */
  rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;     
  rozofs_tx_read_opaque_data(rozofs_tx_ctx_p,0,&cmd_idx);
  rozofs_tx_read_opaque_data(rozofs_tx_ctx_p,1,(uint32_t *)&file_idx);
  rozofs_tx_read_opaque_data(rozofs_tx_ctx_p,2,&length);
  /*
  ** Retrieve ientry
  */
  if (!(ie = get_ientry_by_inode(ino))) {
    errno = ENOENT;
    goto error;
  } 
       
  /*    
  ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
  */
  status = rozofs_tx_get_status(this);
  if (status < 0){
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
  if (recv_buf == NULL) {
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
  if (rozofs_xdr_replymsg(&xdrs,&rpc_reply) != TRUE){
    TX_STATS(ROZOFS_TX_DECODING_ERROR);
    errno = EPROTO;
    goto error;
  }
  /*
  ** Decode the rpc message
  */
  memset(&ret,0, sizeof(ret));                    
  if (decode_proc(&xdrs,&ret) == FALSE) {
    TX_STATS(ROZOFS_TX_DECODING_ERROR);
    errno = EPROTO;
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);
    goto error;
  }   
  if (ret.status == STORCLI_FAILURE) {
    /*
    ** ENOENT means size 0 an musr not be treated as an error
    */
    if (ret.storcli_read_ret_no_data_t_u.error != ENOENT) {
      xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
      goto error;	
    }    
      
    share_rd_p->cmd[cmd_idx].received_len = 0;
  }
  /*
  ** no error, so get the length of the data part
  */
  xdr_free((xdrproc_t) decode_proc, (char *) &ret);
  /*
  ** Successful truncate for a single file
  */
  errno = 0;
  rozofs_trc_rsp_attr_multiple(srv_rozofs_ll_read,ie->inode,NULL,1,share_rd_p->cmd[cmd_idx].received_len,trc_idx,file_idx);

out:
  if (fuse_save_ctx_p->multiple_pending != 0) {
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
  if (fuse_save_ctx_p->multiple_errno != 0) {
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
  rozofs_trc_rsp_multiple(srv_rozofs_ll_read,ino,NULL,1,trc_idx,file_idx);
  /*
  ** register the errno code if noy yet asserted
  */    
  if (fuse_save_ctx_p->multiple_errno == 0) fuse_save_ctx_p->multiple_errno = errno;
  goto out;
}
/*
**__________________________________________________________________
** Resize procedure
** 
** One must send a request to the storages for each subfile in multifile
** mode in order to get the the size of each subfile and so deduce the 
** real size of the file in order to update it on the exportd
**
** @param req         Pointer to the fuse request
** @param ie          Pointer to the i-node context of the client
** @param fuse_ctx_p  Fuse context
** @param trc_idx     Trace index
** 
** @retval -1 on error.
**__________________________________________________________________
*/
int rozofs_do_resize_multiple(fuse_req_t req, ientry_t *ie, void * fuse_ctx_p, int trc_idx) {
  storcli_read_arg_t      args;
  int                     ret;
  int                     storcli_idx;
  int                     nb_slave = 0;
  int                     nb_master = 0;
  void                 *  xmit_buf = NULL;
  rozofs_tx_ctx_t      *  rozofs_tx_ctx_p = NULL;
  uint8_t              *  arg_p;
  uint32_t             *  header_size_p;
  int                     lbg_id;
  XDR                     xdrs;    
  struct rpc_msg          call_msg;
  int                     position;
  int                     bufsize;
  int                     opcode;
  uint32_t                timeout_sec;
  uint32_t                null_val = 0;    
  rozofs_fuse_save_ctx_t *fuse_save_ctx_p=NULL;
  uint32_t                file_idx;  
  int                           shared_buf_idx = -1;
  void                        * shared_buf_ref;
  rozofs_shared_buf_rd_hdr_t  * share_rd_p;  
  uint32_t                      length;  
  int                           cmdIdx;
      
  /*
  ** Get the pointer to the rozofs fuse context 
  */
  fuse_save_ctx_p = (rozofs_fuse_save_ctx_t*)ruc_buf_getPayload(fuse_ctx_p);
    	 
  /*
  ** Retrieve the number of slave and master inodes to process
  */
  if (ie->attrs.multi_desc.byte == 0) {   
    /*
    ** Just one subfile in the master inode
    */
    nb_slave  = 0;
    nb_master = 1;
  }
  else {
  
    /*
    ** Resize target can only be master the master inode
    */
    if (ie->attrs.multi_desc.common.master == 0) {
      fuse_save_ctx_p->multiple_errno = EINVAL;
      return -1;
    }
          
    /*
    ** Check existence of slave inode in the ientry context
    */
    if (rozofs_get_slave_inode_from_ie(ie) == NULL) {
      fuse_save_ctx_p->multiple_errno = EINVAL;
      severe("ientry has no slave ! inode(%llu)",(long long unsigned int) ie->inode);
      return -1;
    }
  
    nb_slave = rozofs_get_striping_factor_from_ie(ie);
    if (ie->attrs.hybrid_desc.s.no_hybrid==0) {
      /*
      ** In hybrid mode the master inode needs to be processed
      */
      nb_master = 1;
    }
  }
  
  /*
  **  Allocation of a shared buffer 
  */
  shared_buf_ref = rozofs_alloc_shared_storcli_buf(SHAREMEM_IDX_READ);
  fuse_save_ctx_p->shared_buf_ref = shared_buf_ref;
  if (shared_buf_ref == NULL) {
    fuse_save_ctx_p->multiple_errno = ENOMEM;
    return -1;
  }

  share_rd_p     = (rozofs_shared_buf_rd_hdr_t*)ruc_buf_getPayload(shared_buf_ref);
  shared_buf_idx = rozofs_get_shared_storcli_payload_idx(shared_buf_ref,SHAREMEM_IDX_READ,&length);
  if (shared_buf_idx < 0) {
    fatal("Bad buffer index");
    return -1;
  } 
    
  /*
  ** Prepare common parameters for all requests 
  ** cid, dist_set and fid depends on the subfile
  */
  args.spare          = 'S';
  args.shared_buf_idx = shared_buf_idx;	   
  args.sid            = 0; // rotate
//  args.cid     = ie->attrs.attrs.cid;
  args.layout         = exportclt.layout;
  args.bsize          = exportclt.bsize;    
  args.proj_id        = 0; // Shared buffer idx
  args.bid            = 0; // bid = 0 + nb_proj = 0
  args.nb_proj        = 0; // means resize service
//  memcpy(args.dist_set, ie->attrs.attrs.sids, sizeof (sid_t) * ROZOFS_SAFE_MAX);
//  memcpy(args.fid, ie->fid, sizeof (fid_t));

    
  opcode      = STORCLI_READ;
  timeout_sec = ROZOFS_TMR_GET(TMR_STORCLI_PROGRAM);

  share_rd_p->cmd[0].xid = 0;
  share_rd_p->cmd[0].received_len = 0;   

  /*
  **___________________________________________________________________
  **   Build the individual storcli read command
  **____________________________________________________________________
  */
  cmdIdx = 0;
  for (file_idx = 0; file_idx < (nb_slave+1) ;file_idx++)  {
  
    rozofs_tx_ctx_p = NULL;
    
    /*
    ** Index 0 is the master inode : Only valid in case of single or hybrid file
    */
    if ((file_idx==0) && (nb_master == 0)) continue;
    
    args.cmd_idx = cmdIdx;
    cmdIdx++;
    
    share_rd_p->cmd[args.cmd_idx].xid              = 0;
    share_rd_p->cmd[args.cmd_idx].received_len     = 0;       
    share_rd_p->cmd[args.cmd_idx].offset_in_buffer = 0;    
      
    /*
    ** Get the FID storage, cid and sids lists for the current file index
    */
    ret = rozofs_fill_storage_info_multiple(ie,&args.cid,args.dist_set,args.fid,file_idx);
    rozofs_trc_req_io_multiple(trc_idx, srv_rozofs_ll_read, ie->inode, args.fid, 0, 0, file_idx);
      
    if (ret < 0) {
      fuse_save_ctx_p->multiple_errno = EINVAL;
      goto error;
    }  
      
    /*
    ** allocate a transaction context
    */
    rozofs_tx_ctx_p = rozofs_tx_alloc();  
    if (rozofs_tx_ctx_p == NULL) {
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
    ** save the file index and command index in the opaque[] fields of the transaction context
    */        
    rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,0,args.cmd_idx);
    rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,1,file_idx);
    rozofs_tx_write_opaque_data(rozofs_tx_ctx_p,2,0); // requested length
    /*
    ** get the storcli to use for the transaction: it uses the STORIO fid as a primary key
    */
    storcli_idx = stclbg_storcli_idx_from_fid(args.fid);   
    if (common_config.storcli_read_parallel == 0) {
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
    if (xmit_buf == NULL) {
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
    call_msg.rm_xid = rozofs_tx_alloc_xid(rozofs_tx_ctx_p);     
    share_rd_p->cmd[args.cmd_idx].xid = call_msg.rm_xid;
    
    call_msg.rm_call.cb_rpcvers = RPC_MSG_VERSION;
    call_msg.rm_call.cb_prog = (uint32_t)STORCLI_PROGRAM;
    call_msg.rm_call.cb_vers = (uint32_t)STORCLI_VERSION;
    if (! xdr_callhdr(&xdrs, &call_msg)) {
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
    if (xdr_storcli_read_arg_t(&xdrs,&args) == FALSE) {
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
    rozofs_tx_ctx_p->recv_cbk   = rozofs_do_resize_multiple_cbk;
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
    if (ret < 0) {
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
    ** Transaction timer start
    */
    rozofs_tx_start_timer(rozofs_tx_ctx_p,timeout_sec);      
  }
  return 0;


error:
  /* 
  ** Release the pending transaction context 
  */
  if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);  
  /*
  ** assert the errno and trace the output
  */
  errno = fuse_save_ctx_p->multiple_errno;
  rozofs_trc_rsp_multiple(srv_rozofs_ll_read,ie->inode,args.fid,1,trc_idx,file_idx);
  /*
  ** When some transactions have been sent wait for the responses
  ** to process the error
  */
  if (fuse_save_ctx_p->multiple_pending != 0) {
    return 0;
  }
  return -1;
}      
/*
**__________________________________________________________________
** Resize procedure
** 
** One must send a request to the storages for each subfile in multifile
** mode in order to get the the size of each subfile and so deduce the 
** real size of the file in order to update it on the exportd
**
** @param req         Pointer to the fuse request
** @param ie          Pointer to the i-node context of the client
** @param buffer_p    Fuse context
** @param trc_idx     Trace index
** 
** @retval none
*/
void rozofs_ll_resize_nb(fuse_req_t req, ientry_t *ie, void * buffer_p, int trc_idx) {
  rozofs_fuse_save_ctx_t      * fuse_save_ctx_p=NULL;
  
  /*
  ** Save the callback that should be called up the end of processing of the last multiple command
  */
  fuse_save_ctx_p = (rozofs_fuse_save_ctx_t*)ruc_buf_getPayload(buffer_p);
  fuse_save_ctx_p->multiple_errno   = 0; 
  fuse_save_ctx_p->multiple_pending = 0; 
  fuse_save_ctx_p->saved_cbk_of_tx_multiple = (fuse_end_tx_recv_pf_t) rozofs_ll_resize_cbk;  

  /*
  ** Send a read request for resize purpose in multiple mode
  */
  if (rozofs_do_resize_multiple(req, ie, buffer_p, trc_idx) == 0) return;


  /*
  ** Error cases
  */
  errno = fuse_save_ctx_p->multiple_errno;
  fuse_reply_err(req, errno); 

  /*
  ** release the buffer if has been allocated
  */
  errno = fuse_save_ctx_p->multiple_errno;
  rozofs_trc_rsp_attr(srv_rozofs_ll_setattr,ie->inode,ie->attrs.attrs.fid,-1,ie->attrs.attrs.size,trc_idx);

  STOP_PROFILING_NB(buffer_p,rozofs_ll_setattr);
  rozofs_fuse_release_saved_context(buffer_p);
  return;
}
