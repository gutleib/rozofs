

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
#include <rozofs/rpc/eproto.h>
#include <rozofs/rpc/storcli_proto.h>
#include <rozofs/core/rozofs_fid_string.h>
#include <rozofs/rozofs.h>
#include <rozofs/core/rozofs_tx_common.h>
//#include "rozofs_fuse_api.h"
#include "rozofs_rw_load_balancing.h"
#include "rozofs_fuse_thread_intf.h"
#include "rozofs_io_error_trc.h"

#include "rozofs_sharedmem.h"
#include "rozofs_modeblock_cache.h"
#include "rozofs_cache.h"
#include "rozofs_kpi.h"
#include "rozofs_bt_proto.h"
#include "rozofs_bt_inode.h"




DECLARE_PROFILING(mpp_profiler_t);

int rozofs_bt_endio_cbk(rozofs_bt_ioctx_t *p,int cmd_idx,void *buffer,int size,int errcode);
int rozofs_bt_iosubmit_internal(rozofs_bt_ioctx_t *p);

#define ROZOFS_MAX_QUEUE_DEPTH 8

#define ROZOFS_BT_GET_IO_WORKING_CTX(ruc_buffer) \
   (rozofs_bt_io_working_ctx_t*)ruc_buf_getPayload(ruc_buffer);


#define BT_PROFILING_MICRO(buffer,val)\
{ \
  unsigned long long timeAfter;\
  struct timeval     timeDay;  \
  if (buffer != NULL)\
  { \
    gettimeofday(&timeDay,(struct timezone *)0);  \
    timeAfter = MICROLONG(timeDay); \
    val = (timeAfter-buffer->time); \
  }\
  else val = 0;\
}

/**
*  Macro METADATA start non blocking case
*/
#define BT_START_PROFILING_NB(buffer,the_probe)\
{ \
  unsigned long long time;\
  struct timeval     timeDay;  \
  gprofiler->the_probe[P_COUNT]++;\
  if (buffer != NULL)\
  { \
    gettimeofday(&timeDay,(struct timezone *)0);  \
    time = MICROLONG(timeDay); \
    buffer->time=time;\
  }\
}

#define BT_START_PROFILING_IO_NB(buffer,the_probe, the_bytes)\
 { \
  unsigned long long time;\
  struct timeval     timeDay;  \
  gprofiler->the_probe[P_COUNT]++;\
  if (buffer != NULL)\
    {\
        gettimeofday(&timeDay,(struct timezone *)0);  \
        time = MICROLONG(timeDay); \
        buffer->time=time;\
        gprofiler->the_probe[P_BYTES] += the_bytes;\
    }\
}

#define BT_STOP_PROFILING_NB(buffer,the_probe)\
{ \
  unsigned long long timeAfter,time;\
  struct timeval     timeDay;  \
  if (buffer != NULL)\
  { \
    gettimeofday(&timeDay,(struct timezone *)0);  \
    timeAfter = MICROLONG(timeDay); \
    time = buffer->time;\
    gprofiler->the_probe[P_ELAPSE] += (timeAfter-time); \
  }\
}

/*
**__________________________________________________________________________
*/
/**
   allocate an io_cmd buffer used by the batch application
   
   @param : none
   
   @retval : <> NULL: pointer to a ruc_buffer
   @retval: NULL: out of buffer
   
*/
void *rozofs_bt_buf_io_cmd_alloc()
{
   void *bt_buf_io_cmd_p;
   rozofs_bt_io_working_ctx_t *bt_io_cmd_p;


   bt_buf_io_cmd_p = ruc_buf_getBuffer(rozofs_bt_io_storcli_pool_p); 
   if (bt_buf_io_cmd_p == NULL)
   {
     errno = ENOMEM;
     return NULL;
   }
   bt_io_cmd_p = ROZOFS_BT_GET_IO_WORKING_CTX(bt_buf_io_cmd_p);
   
   memset(bt_io_cmd_p,0,sizeof(rozofs_bt_io_working_ctx_t));
   return bt_buf_io_cmd_p;

}
/*
**__________________________________________________________________________
*/
/**
   release an io_cmd buffer used by the batch application
   
   @param bt_buf_io_cmd_p: pointer to the ruc_buffer
   
   @retval : none
*/
void rozofs_bt_buf_io_cmd_release(void *bt_buf_io_cmd_p)
{
   rozofs_bt_io_working_ctx_t *bt_io_cmd_p;

   bt_io_cmd_p = ROZOFS_BT_GET_IO_WORKING_CTX(bt_buf_io_cmd_p);
   
  ruc_objRemove((ruc_obj_desc_t *) bt_buf_io_cmd_p);
   
  if (bt_io_cmd_p->shared_buf_ref!= NULL) 
  {
    uint32_t *p32 = (uint32_t*)ruc_buf_getPayload(bt_io_cmd_p->shared_buf_ref);    
    /*
    ** clear the timestamp
    */
    *p32 = 0;
    ruc_buf_freeBuffer(bt_io_cmd_p->shared_buf_ref);
    bt_io_cmd_p->shared_buf_ref = NULL;
  }
  ruc_buf_freeBuffer(bt_buf_io_cmd_p);  
}



#define ROZOFS_BT_GET_IO_CTX(ruc_buffer) \
   (rozofs_bt_ioctx_t*)ruc_buf_getPayload(ruc_buffer);

/*
**_________________________________________________________________________________________________
*/
rozofs_bt_ioctx_t *rozofs_bt_ioctx_alloc(void *recv_buf,int socket)
{

   rozo_batch_hdr_t *hdr_p;
   rozo_io_cmd_t *cmd_p;
   rozofs_bt_ioctx_t *p;
   void *bt_buf_ioctx_p;
   int i;
   

   bt_buf_ioctx_p = ruc_buf_getBuffer(rozofs_bt_rd_buffer_pool_p); 
   if (bt_buf_ioctx_p == NULL)
   {
     errno = ENOMEM;
     return NULL;
   }
   p = ROZOFS_BT_GET_IO_CTX(bt_buf_ioctx_p);
   memset(p,0,sizeof(rozofs_bt_ioctx_t));

   p->my_ruc_buf_p = bt_buf_ioctx_p;
   p->io_batch_p = ruc_buf_getPayload(recv_buf);
   p->io_ruc_buffer = recv_buf;
   p->socket = socket;
   
   hdr_p = p->io_batch_p;
   cmd_p = (rozo_io_cmd_t*)(hdr_p+1);
   for (i = 0; i < hdr_p->nb_commands; i++)
   {
     p->io_entry[i].cmd_idx = i;  /** not useful!!*/
     p->io_entry[i].next_off = cmd_p[i].offset;
     p->io_entry[i].inode_p = 0;
     p->io_entry[i].status = 0;
     p->io_entry[i].errcode = 0;   
   }
   p->cur_cmd_idx = 0;
   return p;

}


/*
**_________________________________________________________________________________________________
*/
void rozofs_bt_ioctx_release(rozofs_bt_ioctx_t  *buffer)
{

  ruc_buf_freeBuffer(buffer->my_ruc_buf_p);  
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
void rozofs_bt_read_buf_multiple_nb_cbk(void *this,void *param)  
{

    rozofs_tx_ctx_t      *rozofs_tx_ctx_p;
    int status;
    void     *recv_buf = NULL;   
    XDR       xdrs;    
    int      bufsize;
    uint8_t  *payload;
    storcli_read_ret_no_data_t ret;
    xdrproc_t decode_proc = (xdrproc_t)xdr_storcli_read_ret_no_data_t;
    struct rpc_msg  rpc_reply;
    rozofs_shared_buf_rd_hdr_t  *share_rd_p;  
    void *shared_buf_ref;
    uint32_t cmd_idx;
    uint32_t requested_length;
    char *pbuf;
    int enoent_status = 0;
    int trc_idx;
    int file_idx;
    rozofs_bt_io_working_ctx_t *bt_io_cmd_p;
    ext_mattr_t *inode_p;
   

    bt_io_cmd_p = ROZOFS_BT_GET_IO_WORKING_CTX(param);
    trc_idx        = bt_io_cmd_p->trc_idx;
    inode_p        = bt_io_cmd_p->inode_p;
    shared_buf_ref = bt_io_cmd_p->shared_buf_ref; 

    rpc_reply.acpted_rply.ar_results.proc = NULL;

    share_rd_p = (rozofs_shared_buf_rd_hdr_t*)ruc_buf_getPayload(shared_buf_ref);
    /*
    ** set the pointer to the beginning of the buffer that contains the payload
    */
    pbuf = (char*) share_rd_p;
    pbuf +=ROZOFS_SHMEM_READ_PAYLOAD_OFF;    

    bt_io_cmd_p->multiple_pending--;
    if (bt_io_cmd_p->multiple_pending < 0)
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
     if ((bt_io_cmd_p->multiple_errno == 0) && (share_rd_p->cmd[cmd_idx].received_len < requested_length))
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
    rozofs_trc_rsp_multiple(srv_rozofs_ll_read,(fuse_ino_t)bt_io_cmd_p->ino,inode_p->s.attrs.fid,(errno==0)?0:1,trc_idx,file_idx);
    
    if (bt_io_cmd_p->multiple_pending != 0)
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
    if (bt_io_cmd_p->multiple_errno != 0)
    {
//       info("FDL !!!!! ERRNO ASSERTED!!!! ");
       rozofs_tx_set_errno(rozofs_tx_ctx_p,bt_io_cmd_p->multiple_errno);
       if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf); 
       (bt_io_cmd_p->saved_cbk_of_tx_multiple)(this,param);    
       return;       
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
	  bt_io_cmd_p->multiple_errno = EPROTO;	  
	  rozofs_tx_set_errno(rozofs_tx_ctx_p,bt_io_cmd_p->multiple_errno);
	  if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf); 
	  return (bt_io_cmd_p->saved_cbk_of_tx_multiple)(this,param);    
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
    return (bt_io_cmd_p->saved_cbk_of_tx_multiple)(this,param);    

error:
   /*
   ** Check the case of the empty file: in that case errno has been set to 0
   */
   if ((errno == 0) && (bt_io_cmd_p->multiple_errno == 0))
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
    
    if (bt_io_cmd_p->multiple_errno == 0) bt_io_cmd_p->multiple_errno = errno;
    goto out;


}




/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_bt_read_cbk(void *this,void *param) 
{
   struct rpc_msg  rpc_reply;
   uint64_t off;
   void *shared_buf_ref;   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   storcli_read_ret_no_data_t ret;
   xdrproc_t decode_proc = (xdrproc_t)xdr_storcli_read_ret_no_data_t;
   int trc_idx;
   errno =0;
   int bbytes = ROZOFS_BSIZE_BYTES(exportclt.bsize);
   ext_mattr_t *inode_p;
   int update_pending_buffer_todo = 1;  
   rozofs_bt_io_working_ctx_t *bt_io_cmd_p;
   uint64_t next_read_from;
       
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   
    bt_io_cmd_p = ROZOFS_BT_GET_IO_WORKING_CTX(param);
    trc_idx        = bt_io_cmd_p->trc_idx;
    inode_p        = bt_io_cmd_p->inode_p;
    shared_buf_ref = bt_io_cmd_p->shared_buf_ref; 
    off            = bt_io_cmd_p->off;
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
       xdr_free((xdrproc_t) decode_proc, (char *) &ret);
       goto error;
    }   
    if (ret.status == STORCLI_FAILURE) {
    
        /*
	** Case of the file that has not been written to disk 
	*/
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
    ** no error, so get the length of the data part: need to consider the case
    ** of the shared memory since for its case, the length of the returned data
    ** is not in the rpc buffer. It is detected by th epresence of the 0x53535353
    ** pattern in the alignment field of the rpc buffer
    */
    int received_len = ret.storcli_read_ret_no_data_t_u.len.len;
    uint32_t alignment = (uint32_t) ret.storcli_read_ret_no_data_t_u.len.alignment;
    xdr_free((xdrproc_t) decode_proc, (char *) &ret); 

    if (alignment == 0x53535353)
    {
      /*
      ** case of the shared memory
      */
      uint32_t *p32 = (uint32_t*)ruc_buf_getPayload(shared_buf_ref);;
      received_len = p32[1];
      payload = (uint8_t*)&p32[4096/4];
    }
    else
    { 
       /*
       ** fatal: RozoFS always uses shared buffer
       */
       fatal("Not a shared buffer response %x\n",alignment);
    }
    /*
    ** Update the bandwidth/IO statistics
    */
    {
      uint64_t delta;
      BT_PROFILING_MICRO(bt_io_cmd_p,delta);
      rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_READ_IO], 1);
      rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_READ_THR],(uint64_t)received_len);
      rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_READ_LATENCY],delta);  
    }   

    /*
    ** check the length: caution, the received length can
    ** be zero by it might be possible that the information 
    ** is in the pending write section of the buffer
    */
    if ((received_len == 0)  || (inode_p->s.attrs.size == 0))
    {
      /*
      ** empty file case
      */
      errno = 0;
      goto error;   
    }
    /*
    ** Get off requested to storcli (equal off after alignment)
    */
    next_read_from = (off/bbytes)*bbytes;
    /*
    ** Truncate the received length to the known EOF as stored in
    ** the file context
    */
    if ((next_read_from + received_len) > inode_p->s.attrs.size)
    {
       received_len = inode_p->s.attrs.size - next_read_from;    
    }
    /*
    ** re-evaluate the EOF case
    */
    if (received_len <= 0)
    {
        int recv_len_ok = 0;

        if (received_len < 0) {
            uint64_t file_size;
            uint32_t *p32 = (uint32_t*) ruc_buf_getPayload(shared_buf_ref);
            int received_len_orig = p32[1];
            file_size = inode_p->s.attrs.size;
            received_len = received_len_orig;
            if ((next_read_from + received_len) > file_size) {
                received_len = file_size - next_read_from;
            }
            if (received_len > 0)
                recv_len_ok = 1;
        }

      /*
      ** end of filenext_read_pos
      */
      if (recv_len_ok == 0)
      {
	errno = 0;
	goto error; 
      }  
    }    
    
   
    goto out;
error:

    /*
    **__________________________
    ** Common exit
    **__________________________
    */
out:
    
    /*
    ** end of processing
    */
    if (errno != 0)
    {
      rozofs_bt_endio_cbk(bt_io_cmd_p->io_ctx_p,bt_io_cmd_p->cmd_idx,NULL,-1,errno);
    
    }
    else
    {
      rozofs_bt_endio_cbk(bt_io_cmd_p->io_ctx_p,bt_io_cmd_p->cmd_idx,payload,received_len,0);    
    }

    if (update_pending_buffer_todo)
    {
      /*
      ** update the number of storcli pending request
      */
      if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;
    }
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_read,(fuse_ino_t)bt_io_cmd_p->ino,inode_p->s.attrs.fid,(errno==0)?0:1,trc_idx);
    BT_STOP_PROFILING_NB(bt_io_cmd_p,rozofs_ll_read);

    rozofs_bt_buf_io_cmd_release(param);

    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   

    return;
}



/*
**__________________________________________________________________

    M U L T I P L E   F I L E  S E R V I C E S
**__________________________________________________________________
*/    


/*
**__________________________________________________________________
*/
/**
*   Fill up the information needed by storio in order to read/write a file

    @param inode_p: pointer to the inode entry that contains file information
    @param cid: pointer to the array where the cluster_id is returned
    @param sids_p: pointer to the array where storage id are returned
    @param fid_storage: pointer to the array where the fid of the file on storage is returned
    
    @retval 0 on success
    @retval < 0 error (see errno for details)
*/
static inline int rozofs_bt_fill_storage_info(ext_mattr_t *inode_p,cid_t *cid,uint8_t *sids_p,fid_t fid_storage)
{
  mattr_t *attrs_p;
  rozofs_inode_t *rozofs_inode_p;
  int key = ROZOFS_PRIMARY_FID;
  int ret;
  rozofs_mover_sids_t *dist_mv_p;
  
  
  attrs_p = &inode_p->s.attrs;
  rozofs_inode_p = (rozofs_inode_t*)attrs_p->fid; 
  
  if (rozofs_inode_p->s.key == ROZOFS_REG_D_MOVER) key = ROZOFS_MOVER_FID; 
  ret = rozofs_build_storage_fid_from_attr(attrs_p,fid_storage,key);
  if (ret < 0) return ret;
  /*
  ** get the cluster and the list of the sid
  */
  if (key == ROZOFS_MOVER_FID)
  {
    dist_mv_p = (rozofs_mover_sids_t*)attrs_p->sids;
    *cid = dist_mv_p->dist_t.mover_cid;
    memcpy(sids_p,dist_mv_p->dist_t.mover_sids,ROZOFS_SAFE_MAX_STORCLI);
  }
  else
  {
    *cid = attrs_p->cid;
    memcpy(sids_p,attrs_p->sids,ROZOFS_SAFE_MAX_STORCLI);  
  }
  return 0;
}

/*
**__________________________________________________________________
*/
/** That function returns the striping size of the file
    That information is found in one attributes of the main inode
    
    @param inode_p: pointer to the inode information
    
    @retval > 0: value of the striping size in byte
    @retval < 0 : error (see errno for details)
 */
static inline int rozofs_bt_get_striping_size_from_inode(ext_mattr_t *inode_p)
{
  rozofs_multiple_desc_t *p;  
  
  p = &inode_p->s.multi_desc;

  return rozofs_get_striping_size(p);
}


/*
**__________________________________________________________________
*/
/** That function returns the hybrid size portion of a file
    That information is found in one attributes of the main inode
    
    @param inode_p: pointer to the inode information
    
    @retval  0: the file in not hybrid
    @retval > 0: value of the hybrid size in byte
    @retval < 0 : error (see errno for details)
 */
static inline int rozofs_bt_get_hybrid_size_from_inode(ext_mattr_t *inode_p)
{
  rozofs_multiple_desc_t *p;  
  rozofs_hybrid_desc_t *q;  
  
  p = &inode_p->s.multi_desc;
  q = &inode_p->s.hybrid_desc;

  return rozofs_get_hybrid_size(p,q);
}

/*
**__________________________________________________________________
*/
/**
  Get the striping factor: that value indicates the number of secondary inode that
  are associated with the primary inode
  
  The FID of the secondary inodes are in the same file index as the primary inode*
  the first secondary inode is found has the next index in sequence in the file index
  that contains the primary inode

    @param inode_p: pointer to the inode information
    
    @retval > 0: value of the striping size in byte
    @retval < 0 : error (see errno for details)
*/

static inline int rozofs_bt_get_striping_factor_from_inode(ext_mattr_t *inode_p)
{
  rozofs_multiple_desc_t *p;
  p = &inode_p->s.multi_desc;
  return rozofs_get_striping_factor(p);

}


#if 0

/*
**__________________________________________________________________
*/
/**
  get slave inode contexts
  
  That service allocates memory to save the information related to the slave i-node.
  The size of the arry depends on the striping factor of the master inode.
  

    @param ie: pointer to the inode information
    
    @retval <>NULL :: pointer to the inode slave contexts
    @retval NULL : not slave inode contexts
*/

static inline rozofs_slave_inode_t *rozofs_get_slave_inode_from_ie(ientry_t *ie)
{

  return ie->slave_inode_p;
}

#endif


void bt_print_distribution(int file_idx,cid_t *cid,uint8_t *sids_p)
{
   char buffer[1024];
   char *buf = buffer;
   int i;
   
   buf += sprintf(buf,"file idx %d cid :%d sid: ",file_idx ,(int)*cid);
   for (i = 0; i < ROZOFS_SAFE_MAX_STORCLI; i++)
   {
     buf +=sprintf(buf," %d",(int)sids_p[i]);
   }
   info("%s\n",buffer);
}
/*
**__________________________________________________________________
*/
/**
*   Fill up the information needed by storio in order to read/write a file

    @param inode_p: pointer to the inode entry that contains file information
    @param cid: pointer to the array where the cluster_id is returned
    @param sids_p: pointer to the array where storage id are returned
    @param fid_storage: pointer to the array where the fid of the file on storage is returned
    @param file_index
    
    @retval 0 on success
    @retval < 0 error (see errno for details)
*/
static inline int rozofs_bt_fill_storage_info_multiple(ext_mattr_t *inode_p,cid_t *cid,uint8_t *sids_p,fid_t fid_storage,uint16_t file_idx)
{
  mattr_t *attrs_p;
  rozofs_inode_t *rozofs_inode_p;
  rozofs_mover_children_t mover_idx;   
  ext_mattr_t *slave_inode_p = NULL; 
   
  if (file_idx == 0) return rozofs_bt_fill_storage_info(inode_p,cid,sids_p,fid_storage);
  
  /*
  ** case of a slave file
  */
  /*
  ** Get the pointer to the slave inodes
  */
  slave_inode_p  =inode_p +file_idx;
  mover_idx.u32 = slave_inode_p->s.attrs.children;
  
  attrs_p = &inode_p->s.attrs;  
  /*
  ** get the cluster and the list of the sid
  */
  memcpy(fid_storage,attrs_p->fid,sizeof(fid_t));
  /*
  ** append the file index to the fid to get the real fid of the slave inode on the storage side
  */
  rozofs_inode_p = (rozofs_inode_t*)fid_storage;
  rozofs_inode_p->s.idx += (file_idx);

  rozofs_build_storage_fid(fid_storage,mover_idx.fid_st_idx.primary_idx);   
  
  *cid = slave_inode_p->s.attrs.cid;
  memcpy(sids_p,slave_inode_p->s.attrs.sids,ROZOFS_SAFE_MAX_STORCLI);  
//  bt_print_distribution(file_idx,cid,sids_p);
  return 0;
}



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
 * @param bt_buf_io_cmd_p: pointer to the file structure
 * @param off: offset to write from
 * @param *buf: pointer where the data are be stored
 * @param len: length to write
*/
 
int rozofs_bt_read_buf_multiple_nb(void *bt_buf_io_cmd_p, uint64_t off, uint32_t len) 
{
    storcli_read_arg_t  args;
    int ret;
    int storcli_idx;
    int no_striping;
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
    uint32_t         null_val = 0;    
    uint64_t         ino;
    ext_mattr_t *inode_p;

    int bbytes = ROZOFS_BSIZE_BYTES(exportclt.bsize);    
    int trc_idx;

    rozofs_bt_io_working_ctx_t *bt_io_cmd_p;

    bt_io_cmd_p = ROZOFS_BT_GET_IO_WORKING_CTX(bt_buf_io_cmd_p);
    
    trc_idx = bt_io_cmd_p->trc_idx;
    /*
    ** Restore the ino value that might be needed by storcli to perfrom a direct write in the 
    ** page cache during mojette transform
    */
    ino = bt_io_cmd_p->ino; 
    inode_p = bt_io_cmd_p->inode_p;
    /*
    ** Get the pointer to the rozofs fuse context 
    */
    bt_io_cmd_p->multiple_errno   = 0; 
    bt_io_cmd_p->multiple_pending = 0; 
    
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
    
    bt_io_cmd_p->shared_buf_ref= shared_buf_ref;
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
     bt_io_cmd_p->saved_cbk_of_tx_multiple =  rozofs_bt_read_cbk;
    /*
    **________________________________
    ** Update the bandwidth statistics
    **________________________________
    */
    rozofs_fuse_read_write_stats_buf.read_req_cpt++;
    /*
    ** Check the case of the default mode using that API because the read length exceeds the buffer
    ** size of the storcli at storio level
    */
    
    if (inode_p->s.multi_desc.common.master != 0)
    {
      no_striping = 0;
      striping_unit_bytes = rozofs_bt_get_striping_size_from_inode(inode_p);
      striping_factor = rozofs_bt_get_striping_factor_from_inode(inode_p);
    }
    else
    {
       /*
       ** case of the file without striping
       */
      no_striping = 1;
      striping_unit_bytes = ROZOFS_MAX_FILE_BUF_SZ_READ;
      striping_factor = 1;    
    }
    /*
    **___________________________________________________________________
    **         build the  write vector
    **  The return vector indicates how many commands must be generated)
    **____________________________________________________________________
    */
    if (inode_p->s.hybrid_desc.s.no_hybrid==1)
    {
      rozofs_build_multiple_offset_vector(off, len,&vector,striping_unit_bytes,striping_factor,0);
    }
    else
    {
      uint32_t hybrid_size;
      hybrid_size = rozofs_bt_get_hybrid_size_from_inode(inode_p);
      rozofs_build_multiple_offset_vector_hybrid(off, len,&vector,striping_unit_bytes,striping_factor,0,hybrid_size);    
    }
    /*
    ** Common arguments for the write
    */
    args.layout = exportclt.layout;
    args.bsize = exportclt.bsize;
    args.shared_buf_idx = shared_buf_idx;
    
    opcode = STORCLI_READ;
    vers = STORCLI_VERSION;
    prog = STORCLI_PROGRAM;

    share_rd_p->cmd[0].xid = 0;
    share_rd_p->cmd[0].received_len = 0;   
    share_rd_p->cmd[0].inode = (uint64_t) bt_io_cmd_p->rozofs_shm_ref;
    share_rd_p->cmd[0].f_offset = (uint64_t) bt_io_cmd_p->remote_buf;
    /*
    **___________________________________________________________________
    **   Build the individual storcli read command
    **____________________________________________________________________
    */
    
    
    rozofs_multi_vect_t *entry_p = &vector.vectors[0];
    for (nb_vect = 0; nb_vect < vector.nb_vectors;nb_vect++,entry_p++)    
    {
      uint64_t buf_u64;
      rozofs_tx_ctx_p = NULL;
      args.cmd_idx = nb_vect+1;
      
      buf_u64 = (uint64_t) bt_io_cmd_p->remote_buf;

      share_rd_p->cmd[args.cmd_idx].xid = 0;
      share_rd_p->cmd[args.cmd_idx].received_len = 0;       
      share_rd_p->cmd[args.cmd_idx].offset_in_buffer = entry_p->byte_offset_in_shared_buf;  
      share_rd_p->cmd[args.cmd_idx].inode = (uint64_t) bt_io_cmd_p->rozofs_shm_ref;  
      share_rd_p->cmd[args.cmd_idx].f_offset = buf_u64+ entry_p->byte_offset_in_shared_buf;  
      /*
      ** Get the FID storage, cid and sids lists for the current file index: the file_idx MUST be 0 for file without striping
      */
      ret = rozofs_bt_fill_storage_info_multiple(inode_p,&args.cid,args.dist_set,args.fid,(no_striping)?0:entry_p->file_idx);
      /*
      ** Trace the individual read
      */
      rozofs_trc_req_io_multiple(trc_idx,srv_rozofs_ll_read,(fuse_ino_t)ino,args.fid,entry_p->len,entry_p->off,entry_p->file_idx);
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
	bt_io_cmd_p->multiple_errno = EINVAL;
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
	 bt_io_cmd_p->multiple_errno = errno;
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
	bt_io_cmd_p->multiple_errno = errno;
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
       bt_io_cmd_p->multiple_errno = errno;
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
       bt_io_cmd_p->multiple_errno = errno;
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
    rozofs_tx_ctx_p->recv_cbk   = rozofs_bt_read_buf_multiple_nb_cbk;
    rozofs_tx_ctx_p->user_param = bt_buf_io_cmd_p;    
   /*
   **__________________________________________________________________________
   ** Submit the RPC message to the selected load balancing group (storcli)
   **___________________________________________________________________________
   */
   
    /*
    ** increment the number of pending request towards the storcli
    ** increment also the number of pending requests on the rozofs fuse context
    */
    bt_io_cmd_p->multiple_pending++; 
    rozofs_storcli_pending_req_count++;
    ret = north_lbg_send(lbg_id,xmit_buf);
    if (ret < 0)
    {
       if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;
       /*
       ** decrement the number of pending request and assert the multiple_errno
       */
       
       bt_io_cmd_p->multiple_pending--;
       TX_STATS(ROZOFS_TX_SEND_ERROR);
       errno = EFAULT;
       bt_io_cmd_p->multiple_errno = errno;
      goto error;  
    }
    TX_STATS(ROZOFS_TX_SEND);

  }

  return 0;


error:
  /* 
  ** Release the any pending transaction context 
  */
  if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);  

  errno = bt_io_cmd_p->multiple_errno;
  rozofs_trc_rsp(srv_rozofs_ll_read,(fuse_ino_t)ino,NULL,(errno==0)?0:1,trc_idx);
  /*
  ** check if the pending is 0 in the fuse context
  */
  if (bt_io_cmd_p->multiple_pending != 0)
  {
    /*
    ** The error will be processed by the callback, so do it as it was OK
    */

    return 0;
  }

  return -1;
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
 @param bt_buf_io_cmd_p : pointer to the io command buffer
 @param storcli_idx      : identifier of the storcli
 @param fid: file identifier: needed for the storcli load balancing context
 
 @retval 0 on success;
 @retval -1 on error,, errno contains the cause
 */

int rozofs_bt_storcli_send_common(exportclt_t * clt,uint32_t timeout_sec,uint32_t prog,uint32_t vers,
                              int opcode,xdrproc_t encode_fct,void *msg2encode_p,
                              sys_recv_pf_t recv_cbk,void *bt_buf_io_cmd_p,
			                  int storcli_idx,fid_t fid) 			       
{
   
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
    rozofs_bt_io_working_ctx_t *bt_io_cmd_p;

    bt_io_cmd_p = ROZOFS_BT_GET_IO_WORKING_CTX(bt_buf_io_cmd_p);
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
      ** Insert this transaction so that every other following transactions
      ** for the same FID will follow the same path
      */
      stclbg_hash_table_insert_ctx(&rozofs_tx_ctx_p->rw_lbg,fid,storcli_idx);
    }
    else
    /*
    ** Read parallel case: mainly for network with high latency
    */
    {
      /*
      ** Write cannot follow the same behavior because of the first write when the projection does not exist on the storio side
      **  need to see how to address it in order to make it work in parallel for both cases.
      */
      if (opcode != STORCLI_READ)
      {
        stclbg_hash_table_insert_ctx(&rozofs_tx_ctx_p->rw_lbg,fid,storcli_idx);      
      }    
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
       void * kernel_fuse_write_request= NULL;
       rozofs_shared_buf_wr_hdr_t* share_wr_p;
       rozofs_shared_buf_rd_hdr_t* share_rd_p;

        if (opcode == STORCLI_READ)
        {
           share_rd_p = (rozofs_shared_buf_rd_hdr_t*)ruc_buf_getPayload(bt_io_cmd_p->shared_buf_ref);
           share_rd_p->cmd[0].xid = (uint32_t)call_msg.rm_xid;
	  /**
	  * copy the buffer for the case of the write
	  */
	}
	else
	{
           share_wr_p = (rozofs_shared_buf_wr_hdr_t*)ruc_buf_getPayload(bt_io_cmd_p->shared_buf_ref);
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
	  //  RESTORE_FUSE_PARAM(fuse_ctx_p,kernel_fuse_write_request);
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
    rozofs_tx_ctx_p->user_param = bt_buf_io_cmd_p;    
    /*
    ** now send the message
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

    return 0;  
    
  error:
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);  
    return -1;    
}





/** Reads the distributions on the export server,
 *  adjust the read buffer to read only whole data blocks
 *  and uses the function read_blocks to read data
 *
 * @param bt_io_cmd_p: pointer to a io_cmd buffer associated with a batch read request (it contains the command for reading data for a single inode)
 * @param off: offset to read from
 * @param *buf: pointer where the data will be stored: buffer associated with the file_t structure
 * @param len: length to read: (correspond to the max buffer size defined in the exportd parameters
 * @param *last_block_size_p: pointer to store the size of the last block size
 *  read
 *
 * @return: the length read on success, -1 otherwise (errno is set)
 */
 
static int rozofs_bt_read_buf_nb(void *bt_buf_io_cmd_p,ext_mattr_t *inode_p) 
{
   uint64_t bid = 0;
   uint32_t nb_prj = 0;
   storcli_read_arg_t  args;
   int ret;
   int storcli_idx;
   int bbytes = ROZOFS_BSIZE_BYTES(exportclt.bsize);
   int max_prj = ROZOFS_MAX_BLOCK_PER_MSG;
   int shared_buf_idx;
   rozofs_shared_buf_rd_hdr_t  *share_rd_p;   
   uint32_t length;
   rozofs_bt_io_working_ctx_t *bt_io_cmd_p;

   uint64_t off;
   uint32_t len;
   
   bt_io_cmd_p = ROZOFS_BT_GET_IO_WORKING_CTX(bt_buf_io_cmd_p);
   
   off = bt_io_cmd_p->off;
   len = bt_io_cmd_p->size;
   bt_io_cmd_p->trc_idx = rozofs_trc_req_io(srv_rozofs_ll_read,(fuse_ino_t)bt_io_cmd_p,inode_p->s.attrs.fid,len,off);
   /*
   ** Check the case of the multifile mode: there is a master inode with several slave inodes associated with it
   */
   if (inode_p->s.multi_desc.common.master != 0)
   {   
      ret =  rozofs_bt_read_buf_multiple_nb(bt_buf_io_cmd_p,off,len);
      if (ret == 0) return len;
      return ret;
      
   }
   if (len > ROZOFS_MAX_FILE_BUF_SZ_READ)
   {
     ret = rozofs_bt_read_buf_multiple_nb(bt_buf_io_cmd_p,off,len);
     if (ret== 0) return len;
     return ret;
   }
   // Nb. of the first block to read
   bid = off / bbytes;
   nb_prj = len / bbytes;
   if (nb_prj > max_prj)
   {
     severe("bad nb_prj %d max %d bid %llu off %llu len %u",nb_prj,max_prj,(long long unsigned int)bid,(long long unsigned int)off,len);   
   }

    args.sid = 0;
    /*
    ** prepare the request for storcli 
    */
    args.cid = inode_p->s.attrs.cid;
    args.layout =exportclt.layout;
    args.bsize = exportclt.bsize;    
    memcpy(args.dist_set, inode_p->s.attrs.sids, sizeof (sid_t) * ROZOFS_SAFE_MAX);
    memcpy(args.fid, inode_p->s.attrs.fid, sizeof (fid_t));
    args.proj_id = 0; // N.S
    args.bid = bid;
    args.nb_proj = nb_prj;
    
    ret = rozofs_bt_fill_storage_info(inode_p,&args.cid,args.dist_set,args.fid);
    if (ret < 0)
    {
      severe("FDL bad storage information encoding");
      errno = EINVAL;
      return ret;
    }
    /*
    ** Get the index of the storcli that will process the request
    */
    storcli_idx = stclbg_storcli_idx_from_fid(inode_p->s.attrs.fid);
    /*
    ** allocate a shared buffer for reading
    */
    void *shared_buf_ref = rozofs_alloc_shared_storcli_buf(SHAREMEM_IDX_READ);
    if (shared_buf_ref != NULL)
    {
      /*
      ** clear the first 4 bytes of the array that is supposed to contain
      ** the reference of the transaction
      */
       args.cmd_idx = 0;
       share_rd_p = (rozofs_shared_buf_rd_hdr_t *)ruc_buf_getPayload(shared_buf_ref);
       share_rd_p->cmd[args.cmd_idx].xid = 0;
       share_rd_p->cmd[args.cmd_idx].received_len = 0;       
       share_rd_p->cmd[args.cmd_idx].offset_in_buffer = 0;
       share_rd_p->cmd[args.cmd_idx].inode = (uint64_t) bt_io_cmd_p->rozofs_shm_ref;
       share_rd_p->cmd[args.cmd_idx].f_offset = (uint64_t) bt_io_cmd_p->remote_buf;
       /*
       ** get the index of the shared payload in buffer
       */
       shared_buf_idx = rozofs_get_shared_storcli_payload_idx(shared_buf_ref,SHAREMEM_IDX_READ,&length);
       if (shared_buf_idx != -1)
       {
         /*
         ** save the reference of the shared buffer in the fuse context
         */
         bt_io_cmd_p->shared_buf_ref = shared_buf_ref;
         args.spare     = 'S';
         args.shared_buf_idx = shared_buf_idx;	 
       }
    }
    else
    {
       fatal("FDL Out of a shared buffer");
    
    }
    rozofs_fuse_read_write_stats_buf.read_req_cpt++;
    /*
    ** now initiates the transaction towards the remote end
    */
    ret = rozofs_bt_storcli_send_common(NULL,ROZOFS_TMR_GET(TMR_STORCLI_PROGRAM),STORCLI_PROGRAM, STORCLI_VERSION,
                              STORCLI_READ,(xdrproc_t) xdr_storcli_read_arg_t,(void *)&args,
                              rozofs_bt_read_cbk,bt_buf_io_cmd_p,storcli_idx,inode_p->s.attrs.fid); 
    if (ret < 0) goto error;
    
    /*
    ** no error just waiting for the answer
    */
    ret = len;
    return ret;    
error:
    return ret;

}




/*
**_________________________________________________________________________________________________
*/
/**
   Submit a request towards a storcli
   
   @param p: pointer to the IO btach context
   @param id: index within the batch command
   @param io_buf_p: reference of the IO buffer used for receiving data
   
   @retval < 0 : error
   @retval >=0 : data size requested
*/
#define MAX_IO_SIZE (1024*1024)
int rozofs_bt_send_read(rozofs_bt_ioctx_t *p,int idx,void *io_buf_p)
{

   rozo_batch_hdr_t *hdr_p;
   rozo_io_cmd_t *cmd_p;
   rozofs_bt_io_entry_t *entry_p;
   size_t size = 0;
   void *bt_buf_io_cmd_p;
   rozofs_bt_io_working_ctx_t *bt_io_cmd_p;
   rozofs_memreg_t *mem_p;

   hdr_p = p->io_batch_p;
   cmd_p = (rozo_io_cmd_t*)(hdr_p+1);
   /*
   ** go to the relative entry
   */
   cmd_p += idx;
   entry_p = &p->io_entry[idx];
   /*
   ** Get the pointer to the shared memory context
   */
   mem_p = p->mem_p[idx];
   if (mem_p == NULL)
   {
     /*
     ** attempt to get the context
     */
     p->mem_p[idx] = rozofs_shm_lookup(cmd_p->rozofs_shm_ref);
     if (p->mem_p[idx] == NULL)
     {
       errno = ENOMEM;
       return -1;
     }
     mem_p = p->mem_p[idx];
   }
   /*
   ** check if the inode context is known
   */
   if (entry_p->inode_p == NULL)
   {
      /*
      ** do a lookup of the inode
      */
      entry_p->inode_p = rozofs_bt_lookup_inode_internal(cmd_p->inode);
      if (entry_p->inode_p == NULL)
      {
        errno = ENOENT;
	return -1;
     }   
   }
   if (entry_p->next_off >= cmd_p->offset+cmd_p->length) return size;
   size = entry_p->next_off - cmd_p->offset;
   size = cmd_p->length - size;
   if (size > MAX_IO_SIZE) size = MAX_IO_SIZE;
   /*
   ** allocate a buffer to control the transaction with the storcli
   */
   bt_buf_io_cmd_p = rozofs_bt_buf_io_cmd_alloc(); 
   if (bt_buf_io_cmd_p == NULL)
   {
     return -1;
   }
   bt_io_cmd_p =  ROZOFS_BT_GET_IO_WORKING_CTX(bt_buf_io_cmd_p);
   
   bt_io_cmd_p->shared_buf_ref = NULL;
   bt_io_cmd_p->io_ctx_p = p;
   bt_io_cmd_p->off = entry_p->next_off;
   bt_io_cmd_p->size = size;
   bt_io_cmd_p->cmd_idx = idx;
   bt_io_cmd_p->inode_p = entry_p->inode_p;
   bt_io_cmd_p->remote_buf = cmd_p->buf;
   bt_io_cmd_p->rozofs_shm_ref = cmd_p->rozofs_shm_ref;
   /*
   ** check if the address+length is in the range of the shared memory
   */
   {
      uint8_t *local_address;
      
      local_address = rozofs_bt_map_to_local(mem_p,cmd_p->buf,(uint64_t) bt_io_cmd_p->size);
      if (local_address == NULL)
      {
         rozofs_bt_buf_io_cmd_release(bt_buf_io_cmd_p);
	 return -1;      
      }
   }
   
   BT_START_PROFILING_IO_NB(bt_io_cmd_p,rozofs_ll_read, size);

   size = rozofs_bt_read_buf_nb(bt_buf_io_cmd_p,entry_p->inode_p);
   if (size < 0)
   {
     /*
     ** need to release the io context that has been allocated to communicate with storcli
     */
     BT_STOP_PROFILING_NB(bt_io_cmd_p,rozofs_ll_read);
     rozofs_bt_buf_io_cmd_release(bt_buf_io_cmd_p);
   }
   return (int)size;
}


/*
**_________________________________________________________________________________________________
*/
int rozofs_bt_endio_check_alldone(rozofs_bt_ioctx_t *p)
{

   rozo_batch_hdr_t *hdr_p;
   rozo_io_cmd_t *cmd_p;
   int i;
   int alldone = 1;

   hdr_p = p->io_batch_p;
   cmd_p = (rozo_io_cmd_t*)(hdr_p+1);
   
   for (i = 0; i < hdr_p->nb_commands; i++)
   {
     if (p->io_entry[i].status < 0) continue;
     if ((p->io_entry[i].next_off < (cmd_p[i].offset+cmd_p[i].length)) || (p->io_entry[i].pending_request != 0) )
     {
       alldone = 0;
       break;
     }
   }
   return alldone;
}

/*
**_________________________________________________________________________________________________
*/
int rozofs_bt_endio_cbk(rozofs_bt_ioctx_t *p,int cmd_idx,void *buffer,int size,int errcode)
{

   rozo_batch_hdr_t *hdr_p;
   rozo_io_cmd_t *cmd_p;
   int alldone;
//   int nb_cmd;
   int i;
   int ret;
   rozofs_bt_rsp_buf response;
   
   hdr_p = p->io_batch_p;
   cmd_p = (rozo_io_cmd_t*)(hdr_p+1);
   
   if (size < 0) 
   {
     p->io_entry[cmd_idx].status = -1;
     p->io_entry[cmd_idx].errcode = errcode;
   }
   p->io_entry[cmd_idx].pending_request--;
   p->global_pending_request--;
   if (p->global_pending_request == 0)
   {
      /*
      ** check if it is the end
      */
      alldone = rozofs_bt_endio_check_alldone(p);
      if (alldone) goto done;
   }
   /*
   ** Attempt to submit new requests
   */
   rozofs_bt_iosubmit_internal(p);
//   info("--> nb_cmd %d\n",nb_cmd);
   return 0;

done:
   memcpy(&response.hdr,hdr_p,sizeof(rozo_batch_hdr_t));
   for (i = 0; i <hdr_p->nb_commands; i++,cmd_p++)
   {
        response.res[i].data = cmd_p->data;
	if (p->io_entry[i].status < 0)
	{
	  response.res[i].status = -1;
	  response.res[i].size = p->io_entry[i].errcode;
	}
	else
	{  
	  response.res[i].status = 0;  
	  response.res[i].size = p->io_entry[i].next_off;
	}
   }
   response.hdr.msg_sz = sizeof(rozo_batch_hdr_t)-sizeof(uint32_t)+hdr_p->nb_commands*sizeof(rozo_io_res_t);
   errno = 0;
   ret = send(p->socket,&response,response.hdr.msg_sz+sizeof(uint32_t),0);
   if (ret < 0)
   {
      severe("Error on af_unix sned (%s)",strerror(errno));
   }
//   info("message send socket : %d %d: %s\n",p->socket,ret,strerror(errno));
   rozofs_bt_free_receive_buf(p->io_ruc_buffer);
   /*
   ** release the global io context
   */
   rozofs_bt_ioctx_release(p);
   return 1;

}
/*
**_________________________________________________________________________________________________
*/
/**
   The number of buffer submitted depends on a configured queue depth for one IO batch and the number of available buffer
   
*/
int rozofs_bt_iosubmit_internal(rozofs_bt_ioctx_t *p)
{
   void *io_buf_p= NULL;
   int cur_cmd_idx;
   rozo_batch_hdr_t *hdr_p;
   rozo_io_cmd_t *cmd_p;
   int size;
   int global_pending_request = 0;
   int done = 0;
   int alldone;
   int i;
   
   cur_cmd_idx = p->cur_cmd_idx;
   hdr_p = p->io_batch_p;
   cmd_p = (rozo_io_cmd_t*)(hdr_p+1);
   
   while (!done)
   {
     for (i = 0; i < hdr_p->nb_commands; i++)
     {
       alldone = 1;
       /*
       ** check if there is enough storcli context: it is based on the amount of read/write shared buffers
       */
       if (rozofs_storcli_pending_req_count >= rozofs_max_storcli_tx)
       {
          done = 1;
	  break;
       }
       if ((p->io_entry[cur_cmd_idx].next_off == (cmd_p[cur_cmd_idx].offset+cmd_p[cur_cmd_idx].length)) || (p->io_entry[cur_cmd_idx].status < 0))
       {
          cur_cmd_idx++;
	  if (cur_cmd_idx == hdr_p->nb_commands) cur_cmd_idx = 0;
          continue;
       }
       alldone = 0;
       /*
       ** attempt to send the read request
       */
       size = rozofs_bt_send_read(p,cur_cmd_idx,io_buf_p);

       if (size < 0)
       {
	  p->io_entry[cur_cmd_idx].status = -1;
	  p->io_entry[cur_cmd_idx].errcode = errno;
       }
       else
       {
#if 0
         info("inode %4.4llu  off : %8llu   len: %8d\n",
	              (long long unsigned int)cmd_p[cur_cmd_idx].inode, 
		      (long long unsigned int)p->io_entry[cur_cmd_idx].next_off,(int)size);
#endif
	 p->io_entry[cur_cmd_idx].next_off += size;
	 p->io_entry[cur_cmd_idx].pending_request++;
	 cmd_p[cur_cmd_idx].buf += size;
	 
	 global_pending_request++;
       }
       /*
       ** go to the next entry
       */
       cur_cmd_idx++;
       if (cur_cmd_idx ==  hdr_p->nb_commands) cur_cmd_idx = 0;
       if ((global_pending_request + p->global_pending_request) >= ROZOFS_MAX_QUEUE_DEPTH)
       {
          done = 1;
	  break;
       }
     }
     if (alldone) done = 1;
   }
   p->global_pending_request += global_pending_request;
   p->cur_cmd_idx = cur_cmd_idx;
   return global_pending_request;
   

}



/**
**_______________________________________________________________________________________
*/

/**
  Process an IO read in batch mode
  
  @param recv_buf: pointer to the buffer that contains the command
  @param socket_id: file descriptor used for sending back the response
  
  @retval none
*/
void rozofs_bt_process_io_read(void *recv_buf,int socket_id)
{
   rozo_batch_hdr_t *hdr_p;
   rozo_io_cmd_t *cmd_p;
   int i;
   int ret;
   rozofs_bt_rsp_buf response;
   rozofs_bt_ioctx_t *bt_ioctx_p = NULL;
   int pending_requests;

   
   bt_ioctx_p = rozofs_bt_ioctx_alloc(recv_buf,socket_id);
   if (bt_ioctx_p == NULL)
   {
      errno = ENOMEM;
      goto error;
   }
   /*
   ** submit the IO
   */
  pending_requests = rozofs_bt_iosubmit_internal(bt_ioctx_p);
  if (pending_requests == 0)
  {
   hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(recv_buf); 
   cmd_p = (rozo_io_cmd_t*)(hdr_p+1);
   memcpy(&response.hdr,hdr_p,sizeof(rozo_batch_hdr_t));
   for (i = 0; i <hdr_p->nb_commands; i++,cmd_p++)
   {
        response.res[i].data = cmd_p->data;
	if (bt_ioctx_p->io_entry[i].status < 0)
	{
	  response.res[i].status = -1;
	  response.res[i].size = bt_ioctx_p->io_entry[i].errcode;
	}
	else
	{  
	  response.res[i].status = 0;  
	  response.res[i].size = bt_ioctx_p->io_entry[i].next_off;
	}
   }
   response.hdr.msg_sz = sizeof(rozo_batch_hdr_t)-sizeof(uint32_t)+hdr_p->nb_commands*sizeof(rozo_io_res_t);
   errno = 0;
   ret = send(socket_id,&response,response.hdr.msg_sz+sizeof(uint32_t),0);
   rozofs_bt_free_receive_buf(bt_ioctx_p->io_ruc_buffer);
   /*
   ** release the global io context
   */
   rozofs_bt_ioctx_release(bt_ioctx_p);
  }
  return;
    
error:   
   hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(recv_buf); 
   cmd_p = (rozo_io_cmd_t*)(hdr_p+1);
   memcpy(&response.hdr,hdr_p,sizeof(rozo_batch_hdr_t));
   info("Nb command %d\n",hdr_p->nb_commands);
   for (i = 0; i <hdr_p->nb_commands; i++,cmd_p++)
   {
        response.res[i].data = cmd_p->data;
	response.res[i].status = -1;
	response.res[i].size = errno;  
   }
   response.hdr.msg_sz = sizeof(rozo_batch_hdr_t)-sizeof(uint32_t)+hdr_p->nb_commands*sizeof(rozo_io_res_t);
   errno = 0;
   ret = send(socket_id,&response,response.hdr.msg_sz+sizeof(uint32_t),0);
   info("message send socket : %d %d: %s\n",socket_id,ret,strerror(errno));
   rozofs_bt_free_receive_buf(recv_buf);
   return;
   

 
}   



