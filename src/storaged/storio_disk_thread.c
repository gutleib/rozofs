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

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <fcntl.h> 
#include <sys/un.h>             
#include <errno.h>  
#include <time.h>
#include <pthread.h> 
#include <rozofs/core/ruc_common.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/rozofs_socket_family.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rpc/rozofs_rpc_util.h>
#include <rozofs/rpc/sproto.h>
#include "storio_disk_thread_intf.h" 
#include "storage.h" 
#include "storio_device_mapping.h" 
#include "storio_north_intf.h"
#include <rozofs/core/ruc_buffer_debug.h>
#include "storio_serialization.h"

int af_unix_disk_socket_ref = -1;
 
 #define MICROLONG(time) ((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)

uint64_t   af_unix_disk_parallel_req        = 0;
uint64_t   af_unix_disk_parallel_req_tbl[ROZOFS_MAX_DISK_THREADS] = {0};

storage_t *storaged_lookup(cid_t cid, sid_t sid) ;

/**
*  Thread table
*/
rozofs_disk_thread_ctx_t rozofs_disk_thread_ctx_tb[ROZOFS_MAX_DISK_THREADS];


void storio_disk_serial(rozofs_disk_thread_ctx_t *ctx_p,storio_disk_thread_msg_t * msg_in);
/*
**__________________________________________________________________________
*/
/**
   Creation of a AF_UNIX socket Datagram  in non blocking mode

   For the disk the socket is created in blocking mode
     
   @param name0fSocket : name of the AF_UNIX socket
   @param size: size in byte of the xmit buffer (the service double that value
   
    retval: >0 reference of the created AF_UNIX socket
    retval < 0 : error on socket creation 

*/
int af_unix_disk_sock_create_internal(char *nameOfSocket,int size)
{
  int ret;    
  int fd=-1;  
  struct sockaddr_un addr;
  int fdsize;
  unsigned int optionsize=sizeof(fdsize);

  /* 
  ** create a datagram socket 
  */ 
  fd=socket(PF_UNIX,SOCK_DGRAM,0);
  if(fd<0)
  {
    warning("af_unix_disk_sock_create_internal socket(%s) %s", nameOfSocket, strerror(errno));
    return -1;
  }
  /* 
  ** remove fd if it already exists 
  */
  ret = unlink(nameOfSocket);
  /* 
  ** named the socket reception side 
  */
  addr.sun_family= AF_UNIX;
  strcpy(addr.sun_path,nameOfSocket);
  ret=bind(fd,(struct sockaddr*)&addr,sizeof(addr));
  if(ret<0)
  {
    warning("af_unix_disk_sock_create_internal bind(%s) %s", nameOfSocket, strerror(errno));
    return -1;
  }
  /*
  ** change the length for the send buffer, nothing to do for receive buf
  ** since it is out of the scope of the AF_SOCKET
  */
  ret= getsockopt(fd,SOL_SOCKET,SO_SNDBUF,(char*)&fdsize,&optionsize);
  if(ret<0)
  {
    warning("af_unix_disk_sock_create_internal getsockopt(%s) %s", nameOfSocket, strerror(errno));
    return -1;
  }
  /*
  ** update the size, always the double of the input
  */
  fdsize=2*size;
  
  /* 
  ** set a new size for emission and 
  ** reception socket's buffer 
  */
  ret=setsockopt(fd,SOL_SOCKET,SO_SNDBUF,(char*)&fdsize,sizeof(int));
  if(ret<0)
  {
    warning("af_unix_disk_sock_create_internal setsockopt(%s,%d) %s", nameOfSocket, fdsize, strerror(errno));
    return -1;
  }

  return(fd);
}  


/*
**__________________________________________________________________________
*/
/**
* encode the RCP reply
    
  @param p       : pointer to the generic rpc context
  @param arg_ret : returned argument to encode 
  
  @retval none

*/
int storio_encode_rpc_response (rozorpc_srv_ctx_t *p,char * arg_ret) {
   uint8_t *pbuf;           /* pointer to the part that follows the header length */
   uint32_t *header_len_p;  /* pointer to the array that contains the length of the rpc message*/
   XDR xdrs;
   int len;

   if (p->xmitBuf == NULL) {
     // STAT
     severe("no xmit buffer");
     return -1;
   } 
   
   /*
   ** create xdr structure on top of the buffer that will be used for sending the response
   */ 
   header_len_p = (uint32_t*)ruc_buf_getPayload(p->xmitBuf); 
   pbuf = (uint8_t*) (header_len_p+1);            
   len = (int)ruc_buf_getMaxPayloadLen(p->xmitBuf);
   len -= sizeof(uint32_t);
   xdrmem_create(&xdrs,(char*)pbuf,len,XDR_ENCODE); 
   
   if (rozofs_encode_rpc_reply(&xdrs,(xdrproc_t)p->xdr_result,(caddr_t)arg_ret,p->src_transaction_id) != TRUE) {
     ROZORPC_SRV_STATS(ROZORPC_SRV_ENCODING_ERROR);
     severe("rpc reply encoding error");
     return -1;    
   }       
   /*
   ** compute the total length of the message for the rpc header and add 4 bytes more bytes for
   ** the ruc buffer to take care of the header length of the rpc message.
   */
   int total_len = xdr_getpos(&xdrs);
   *header_len_p = htonl(0x80000000 | total_len);
   total_len += sizeof(uint32_t);
   ruc_buf_setPayloadLen(p->xmitBuf,total_len);
   
   return 0;
}
/**
*  Rebuild stop when relocation was requested

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storio_disk_rebuild_stop(rozofs_disk_thread_ctx_t *thread_ctx_p,storio_disk_thread_msg_t * msg) {
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  storage_t *st = 0;
  sp_rebuild_stop_arg_t  * args;
  rozorpc_srv_ctx_t      * rpcCtx;
  sp_rebuild_stop_ret_t    ret;
  storio_device_mapping_t * fidCtx;
  int                       rebuildIdx;
  STORIO_REBUILD_T        * pRebuild=NULL;
    
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);

  ret.status = SP_FAILURE;          
  
  /*
  ** update statistics
  */
  thread_ctx_p->stat.rebStop_count++;
  
  rpcCtx = msg->rpcCtx;
  args   = (sp_rebuild_stop_arg_t*) ruc_buf_getPayload(rpcCtx->decoded_arg);

  fidCtx = storio_device_mapping_ctx_retrieve(msg->fidIdx);
  if (fidCtx == NULL) {
    ret.sp_rebuild_stop_ret_t_u.error = EIO;
    severe("Bad FID ctx index %d",msg->fidIdx); 
    thread_ctx_p->stat.rebStop_error++ ;   
    goto out;
  } 

  /*
  ** Retrieve the rebuild context from the index hiden in the device field
  */
  rebuildIdx = args->rebuild_ref-1;
  if (rebuildIdx >= MAX_FID_PARALLEL_REBUILD) {
    ret.sp_rebuild_stop_ret_t_u.error = EIO;
    severe("Bad rebuild index %d",rebuildIdx); 
    thread_ctx_p->stat.rebStop_error++ ;   
    goto out;
  } 
  
  pRebuild = storio_rebuild_ctx_retrieve(fidCtx->storio_rebuild_ref.u8[rebuildIdx],(char*)args->fid);
  if (pRebuild == NULL) {
    ret.sp_rebuild_stop_ret_t_u.error = EIO;
    severe("Bad rebuild context %d",rebuildIdx); 
    thread_ctx_p->stat.rebStop_error++ ;   
    goto out;    
  }
  
  // Get the storage for the couple (cid;sid)
  if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
    ret.sp_rebuild_stop_ret_t_u.error = errno;
    thread_ctx_p->stat.rebStop_badCidSid++ ;   
    goto out;
  }  


  ret.status = SP_SUCCESS;


  /* 
  ** well ? Nothing to do
  */
  if (pRebuild->old_device == storio_get_dev(fidCtx,pRebuild->chunk)) {
    goto out;
  }    
  
  /*
  ** This is not the same recycle counter, so better not touch anything
  ** since this rebuild is no more the owner of the file !!!
  */
  if (fidCtx->recycle_cpt != rozofs_get_recycle_from_fid(args->fid)) {
    goto out;
  }
   
  /*
  ** Depending on the rebuild result one has to remove 
  ** old or new data file
  */
  if (args->status == SP_SUCCESS) {   

   /*
    ** Do not test return code, since the device is probably unreachable
    ** or un writable, and so the unlink will probably fail...
    */
    storage_rm_data_chunk(st, pRebuild->old_device, (unsigned char*)args->fid, pRebuild->spare, pRebuild->chunk,0/* No errlog*/);        

    goto out;
  }   

  /*
  ** The rebuild is failed, so let's remove the newly created file 
  ** and restore the old device 
  */
  storage_restore_chunk(st, fidCtx,(unsigned char*)args->fid, pRebuild->spare, 
                                 pRebuild->chunk, pRebuild->old_device);

out:           
  
  storio_encode_rpc_response(rpcCtx,(char*)&ret);  
  storio_send_response(thread_ctx_p,msg,0);

 /*
  ** Update statistics
  */
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.rebStop_time +=(timeAfter-timeBefore);  
}  

/*__________________________________________________________________________
*/
/**
*  Read data from a file

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storio_disk_read(rozofs_disk_thread_ctx_t *thread_ctx_p,storio_disk_thread_msg_t * msg) {
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  storage_t *st = 0;
  sp_read_arg_t          * args;
  rozorpc_srv_ctx_t      * rpcCtx;
  sp_read_ret_t            ret;
  int                      is_fid_faulty;
  storio_device_mapping_t * fidCtx;
    
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);

  ret.status = SP_FAILURE;      
	          
  /*
  ** update statistics
  */
  thread_ctx_p->stat.read_count++;
  
  rpcCtx = msg->rpcCtx;
  args   = (sp_read_arg_t*) ruc_buf_getPayload(rpcCtx->decoded_arg);

  fidCtx = storio_device_mapping_ctx_retrieve(msg->fidIdx);
  if (fidCtx == NULL) {
    ret.sp_read_ret_t_u.error = EIO;
    severe("Bad FID ctx index %d",msg->fidIdx); 
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.read_error++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }  

  // Get the storage for the couple (cid;sid)
  if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
    ret.sp_read_ret_t_u.error = errno;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.read_badCidSid++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
  
  // Check whether this is exactly the same FID
  // This is to handle the case when the read request has been serialized
  // After serialization, the recycling counter may not be the same !!!
  if (storio_get_dev(fidCtx,0) != ROZOFS_UNKNOWN_CHUNK) {
    if (rozofs_get_recycle_from_fid(args->fid) != fidCtx->recycle_cpt) {
      // This is not the same recycling counter, so not the same file
      ret.sp_read_ret_t_u.error = ENOENT;
      storio_encode_rpc_response(rpcCtx,(char*)&ret); 
      thread_ctx_p->stat.read_nosuchfile++ ;          
      storio_send_response(thread_ctx_p,msg,-1);
      return;
    }
  }  
    
  /*
  ** set the pointer to the bins
  */
  char *pbuf = ruc_buf_getPayload(rpcCtx->xmitBuf);
  pbuf += rpcCtx->position;     
  /*
  ** clear the length of the bins and set the pointer where data must be returned
  */  
  ret.sp_read_ret_t_u.rsp.bins.bins_val = pbuf;
  ret.sp_read_ret_t_u.rsp.bins.bins_len = 0;
#if 0 // for future usage with distributed cache 
  /*
  ** clear the optimization array
  */
  ret.sp_read_ret_t_u.rsp.optim.optim_val = (char*)sp_optim;
  ret.sp_read_ret_t_u.rsp.optim.optim_len = 0;
#endif   


  // Lookup for the device id for this FID
  // Read projections
  if (storage_read(st, fidCtx, args->layout, args->bsize,(sid_t *) args->dist_set, args->spare,
            (unsigned char *) args->fid, args->bid, args->nb_proj,
            (bin_t *) ret.sp_read_ret_t_u.rsp.bins.bins_val,
            (size_t *) & ret.sp_read_ret_t_u.rsp.bins.bins_len,
            &ret.sp_read_ret_t_u.rsp.file_size, &is_fid_faulty) != 0) 
  {
    ret.sp_read_ret_t_u.error = errno;
    if (errno == ENOENT)    thread_ctx_p->stat.read_nosuchfile++;
    else if (!args->spare)  thread_ctx_p->stat.read_error++;
    else                    thread_ctx_p->stat.read_error_spare++;
    if (is_fid_faulty) {
      storio_register_faulty_fid(thread_ctx_p->thread_idx,
				 args->cid,
				 args->sid,
				 (uint8_t*)args->fid);
    }     
    storio_encode_rpc_response(rpcCtx,(char*)&ret);
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }  
 
  ret.status = SP_SUCCESS;  
  msg->size = ret.sp_read_ret_t_u.rsp.bins.bins_len;        
  storio_encode_rpc_response(rpcCtx,(char*)&ret);  
  thread_ctx_p->stat.read_Byte_count += ret.sp_read_ret_t_u.rsp.bins.bins_len;
  storio_send_response(thread_ctx_p,msg,0);

  /*
  ** Update statistics
  */
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.read_time +=(timeAfter-timeBefore);  
}
/*__________________________________________________________________________
*/
/**
*  Resize file from daa length

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storio_disk_resize(rozofs_disk_thread_ctx_t *thread_ctx_p,storio_disk_thread_msg_t * msg) {
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  storage_t *st = 0;
  sp_read_arg_t          * args;
  rozorpc_srv_ctx_t      * rpcCtx;
  sp_read_ret_t            ret;
  int                      is_fid_faulty;
  storio_device_mapping_t * fidCtx;
     
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);

  ret.status = SP_FAILURE;      
	          
  /*
  ** update statistics
  */
  thread_ctx_p->stat.read_count++;
  
  rpcCtx = msg->rpcCtx;
  args   = (sp_read_arg_t*) ruc_buf_getPayload(rpcCtx->decoded_arg);

  fidCtx = storio_device_mapping_ctx_retrieve(msg->fidIdx);
  if (fidCtx == NULL) {
    ret.sp_read_ret_t_u.error = EIO;
    severe("Bad FID ctx index %d",msg->fidIdx); 
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.read_error++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }  

  // Get the storage for the couple (cid;sid)
  if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
    ret.sp_read_ret_t_u.error = errno;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.read_badCidSid++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
  
  // Check whether this is exactly the same FID
  // This is to handle the case when the read request has been serialized
  // After serialization, the recycling counter may not be the same !!!
  if (storio_get_dev(fidCtx,0) != ROZOFS_UNKNOWN_CHUNK) {
    if (rozofs_get_recycle_from_fid(args->fid) != fidCtx->recycle_cpt) {
      // This is not the same recycling counter, so not the same file
      ret.sp_read_ret_t_u.error = ENOENT;
      storio_encode_rpc_response(rpcCtx,(char*)&ret); 
      thread_ctx_p->stat.read_nosuchfile++ ;          
      storio_send_response(thread_ctx_p,msg,-1);
      return;
    }
  }  
    
  /*
  ** set the pointer to the bins
  */
  char *pbuf = ruc_buf_getPayload(rpcCtx->xmitBuf);
  pbuf += rpcCtx->position;     
  /*
  ** clear the length of the bins and set the pointer where data must be returned
  */  
  ret.sp_read_ret_t_u.rsp.bins.bins_val = pbuf;
  ret.sp_read_ret_t_u.rsp.bins.bins_len = 0;
#if 0 // for future usage with distributed cache 
  /*
  ** clear the optimization array
  */
  ret.sp_read_ret_t_u.rsp.optim.optim_val = (char*)sp_optim;
  ret.sp_read_ret_t_u.rsp.optim.optim_len = 0;
#endif   


  // Lookup for the device id for this FID
  // Read projections
  if (storage_resize(st, fidCtx, args->layout, args->bsize,(sid_t *) args->dist_set, args->spare,
            (unsigned char *) args->fid, (bin_t*) pbuf,
	    & ret.sp_read_ret_t_u.rsp.filler1, 
	    & ret.sp_read_ret_t_u.rsp.filler2, 
	    &is_fid_faulty) != 0) 
  {
    ret.sp_read_ret_t_u.error = errno;
    if (errno == ENOENT)    thread_ctx_p->stat.read_nosuchfile++;
    else if (!args->spare)  thread_ctx_p->stat.read_error++;
    else                    thread_ctx_p->stat.read_error_spare++;
    if (is_fid_faulty) {
      storio_register_faulty_fid(thread_ctx_p->thread_idx,
				 args->cid,
				 args->sid,
				 (uint8_t*)args->fid);
    }     
    storio_encode_rpc_response(rpcCtx,(char*)&ret);
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }  
 
  ret.status = SP_SUCCESS;  
  msg->size = 0;        
  storio_encode_rpc_response(rpcCtx,(char*)&ret);  
  thread_ctx_p->stat.read_Byte_count += ret.sp_read_ret_t_u.rsp.bins.bins_len;
  storio_send_response(thread_ctx_p,msg,0);

  /*
  ** Update statistics
  */
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.read_time +=(timeAfter-timeBefore);  
}
/*__________________________________________________________________________
*/
/**
*  Write data to a file

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storio_disk_write(rozofs_disk_thread_ctx_t *thread_ctx_p,storio_disk_thread_msg_t * msg) {
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  storage_t *st = 0;
  sp_write_arg_no_bins_t * args;
  rozorpc_srv_ctx_t      * rpcCtx;
  sp_write_ret_t           ret;
  uint8_t                  version = 0;
  int                      size;
  int                      is_fid_faulty;
  storio_device_mapping_t * fidCtx;
    
  
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);

  ret.status = SP_FAILURE;          
  
  /*
  ** update statistics
  */
  thread_ctx_p->stat.write_count++;
  
  rpcCtx = msg->rpcCtx;
  args   = (sp_write_arg_no_bins_t*) ruc_buf_getPayload(rpcCtx->decoded_arg);

  fidCtx = storio_device_mapping_ctx_retrieve(msg->fidIdx);
  if (fidCtx == NULL) {
    ret.sp_write_ret_t_u.error = EIO;
    severe("Bad FID ctx index %d",msg->fidIdx); 
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.write_error++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }  
    
  /*
  ** set the pointer to the bins that are in the xmit buffer
  ** since received bufer is also used for the response
  */
  char *pbuf = ruc_buf_getPayload(rpcCtx->xmitBuf); 
  pbuf += rpcCtx->position;

  /*
  ** Check that the received data length is consistent with the bins length
  */
  size = ruc_buf_getPayloadLen(rpcCtx->xmitBuf) - rpcCtx->position;
  if (size != args->len) {
    severe("Inconsistent bins length %d > %d = payloadLen(%d) - position(%d)",
            args->len, size, ruc_buf_getPayloadLen(rpcCtx->xmitBuf), rpcCtx->position);
    ret.sp_write_ret_t_u.error = EPIPE;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.write_error++; 
    storio_send_response(thread_ctx_p,msg,-1); 
    return;   
  }

  /*
  ** Check number of projection is consistent with the bins length
  */
  {
     uint16_t proj_psize = rozofs_get_max_psize_in_msg(args->layout,args->bsize);  
     size =  args->nb_proj * proj_psize;
	    
     if (size > args->len) {
       severe("Inconsistent bins length %d < %d = nb_proj(%d) x proj_size(%d)",
               args->len, size, args->nb_proj, proj_psize);
       ret.sp_write_ret_t_u.error = EIO;
       storio_encode_rpc_response(rpcCtx,(char*)&ret);  
       thread_ctx_p->stat.write_error++; 
       storio_send_response(thread_ctx_p,msg,-1); 
       return;         
     }	        
  } 
  

  // Get the storage for the couple (cid;sid)
  if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
    ret.sp_write_ret_t_u.error = errno;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.write_badCidSid++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
  
  
  // Write projections
  size =  storage_write(st, fidCtx, args->layout, args->bsize, (sid_t *) args->dist_set, args->spare,
          (unsigned char *) args->fid, args->bid, args->nb_proj, version,
          &ret.sp_write_ret_t_u.file_size,(bin_t *) pbuf, &is_fid_faulty);
  if (size <= 0)  {
    ret.sp_write_ret_t_u.error = errno;
    if (errno == ENOSPC)
      thread_ctx_p->stat.write_nospace++;
    else   
      thread_ctx_p->stat.write_error++; 
    if (is_fid_faulty) {
      storio_register_faulty_fid(thread_ctx_p->thread_idx,
				 args->cid,
				 args->sid,
				 (uint8_t*)args->fid);
    }       
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
  msg->size = size;   
    
  ret.status = SP_SUCCESS;  
  ret.sp_write_ret_t_u.file_size = 0;
           
  storio_encode_rpc_response(rpcCtx,(char*)&ret);  
  thread_ctx_p->stat.write_Byte_count += size;
  storio_send_response(thread_ctx_p,msg,0);

  /*
  ** Update statistics
  */
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.write_time +=(timeAfter-timeBefore);  
} 
/*__________________________________________________________________________
*/
/**
*  Write data to a file

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storio_disk_write_empty(rozofs_disk_thread_ctx_t *thread_ctx_p,storio_disk_thread_msg_t * msg) {
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  storage_t *st = 0;
  sp_write_arg_no_bins_t * args;
  rozorpc_srv_ctx_t      * rpcCtx;
  sp_write_ret_t           ret;
  uint8_t                  version = 0;
  int                      size;
  int                      is_fid_faulty;
  storio_device_mapping_t * fidCtx;
    
  
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);

  ret.status = SP_FAILURE;          
  
  /*
  ** update statistics
  */
  thread_ctx_p->stat.write_count++;
  
  rpcCtx = msg->rpcCtx;
  args   = (sp_write_arg_no_bins_t*) ruc_buf_getPayload(rpcCtx->decoded_arg);

  fidCtx = storio_device_mapping_ctx_retrieve(msg->fidIdx);
  if (fidCtx == NULL) {
    ret.sp_write_ret_t_u.error = EIO;
    severe("Bad FID ctx index %d",msg->fidIdx); 
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.write_error++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }  

  /*
  ** Check that the received data length is consistent with the bins length
  */
  size = ruc_buf_getPayloadLen(rpcCtx->xmitBuf) - rpcCtx->position;
  if (size != 0) {
    severe("Inconsistent bins length SP_WRITE_EMPTY  %d = payloadLen(%d) - position(%d)",
            size, ruc_buf_getPayloadLen(rpcCtx->xmitBuf), rpcCtx->position);
    ret.sp_write_ret_t_u.error = EPIPE;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.write_error++; 
    storio_send_response(thread_ctx_p,msg,-1); 
    return;   
  }

  /*
  ** Check number of projection is consistent with the bins length
  */
  {
     uint16_t proj_psize = rozofs_get_max_psize_in_msg(args->layout,args->bsize);  
     size =  args->nb_proj * proj_psize;
	    
     if (size > args->len) {
       severe("Inconsistent bins length %d < %d = nb_proj(%d) x proj_size(%d)",
               args->len, size, args->nb_proj, proj_psize);
       ret.sp_write_ret_t_u.error = EIO;
       storio_encode_rpc_response(rpcCtx,(char*)&ret);  
       thread_ctx_p->stat.write_error++; 
       storio_send_response(thread_ctx_p,msg,-1); 
       return;         
     }	        
  } 
  

  // Get the storage for the couple (cid;sid)
  if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
    ret.sp_write_ret_t_u.error = errno;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.write_badCidSid++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
  
  
  // Write projections
  size =  storage_write_empty(st, fidCtx, args->layout, args->bsize, (sid_t *) args->dist_set, args->spare,
          (unsigned char *) args->fid, args->bid, args->nb_proj, version,
          &ret.sp_write_ret_t_u.file_size, &is_fid_faulty);
  if (size < 0)  {
    ret.sp_write_ret_t_u.error = errno;
    if (errno == ENOSPC)
      thread_ctx_p->stat.write_nospace++;
    else   
      thread_ctx_p->stat.write_error++; 
    if (is_fid_faulty) {
      storio_register_faulty_fid(thread_ctx_p->thread_idx,
				 args->cid,
				 args->sid,
				 (uint8_t*)args->fid);
    }       
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
  msg->size = size;   
    
  ret.status = SP_SUCCESS;  
  ret.sp_write_ret_t_u.file_size = 0;
           
  storio_encode_rpc_response(rpcCtx,(char*)&ret);  
  thread_ctx_p->stat.write_Byte_count += size;
  storio_send_response(thread_ctx_p,msg,0);

  /*
  ** Update statistics
  */
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.write_time +=(timeAfter-timeBefore);  
} 
/*__________________________________________________________________________
*/
/**
*  1rst write within a rebuild with relocation. The header re-write must
   be done now.

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storio_disk_rebuild_start(rozofs_disk_thread_ctx_t *thread_ctx_p,storio_disk_thread_msg_t * msg) {
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  storage_t *st = 0; 
  sp_write_arg_no_bins_t * args;
  rozorpc_srv_ctx_t       * rpcCtx;
  sp_write_ret_t            ret;
  storio_device_mapping_t * fidCtx;
  int                       rebuildIdx;
  STORIO_REBUILD_T        * pRebuild;
  int                       res;  
  int                       size;
  
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);

  ret.status = SP_FAILURE;          
  
  /*
  ** update statistics
  */
  thread_ctx_p->stat.rebStart_count++;
  
  rpcCtx = msg->rpcCtx;
  args   = (sp_write_arg_no_bins_t*) ruc_buf_getPayload(rpcCtx->decoded_arg);

  /*
  ** Retrieve the FID context
  */
  fidCtx = storio_device_mapping_ctx_retrieve(msg->fidIdx);
  if (fidCtx == NULL) {
    ret.sp_write_ret_t_u.error = EIO;
    severe("Bad FID ctx index %d",msg->fidIdx); 
    thread_ctx_p->stat.rebStart_error++ ;   
    goto out;
  } 
  
  /*
  ** Retrieve the rebuild context from the index hiden in the device field
  */
  rebuildIdx = args->rebuild_ref-1;
  if (rebuildIdx >= MAX_FID_PARALLEL_REBUILD) {
    ret.sp_write_ret_t_u.error = EIO;
    severe("Bad rebuild index %d",rebuildIdx); 
    thread_ctx_p->stat.rebStart_error++ ;   
    goto out;
  } 
  
  pRebuild = storio_rebuild_ctx_retrieve(fidCtx->storio_rebuild_ref.u8[rebuildIdx],(char*)args->fid);
  if (pRebuild == NULL) {
    ret.sp_write_ret_t_u.error = EIO;
    severe("Bad rebuild context %d",rebuildIdx); 
    thread_ctx_p->stat.rebStart_error++ ;   
    goto out;    
  }


  // Get the storage for the couple (cid;sid)
  if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
    ret.sp_write_ret_t_u.error = errno;
    thread_ctx_p->stat.rebStart_badCidSid++ ;   
    goto out;
  }
  
  if (pRebuild->relocate == RBS_TO_RELOCATE) {
  
    int block_per_chunk = ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(args->bsize);
    int chunk           = args->bid/block_per_chunk;
      
    res = storage_relocate_chunk(st, fidCtx,(unsigned char *)args->fid, args->spare, 
                                 chunk, &pRebuild->old_device);
    if (res != 0)  {
      ret.sp_write_ret_t_u.error = errno;
      thread_ctx_p->stat.rebStart_error++;   
      goto out;
    }
    
    pRebuild->relocate = RBS_RELOCATED;
    pRebuild->chunk    = chunk;    
  }

  
  /*
  ** Process to the write now
  */
  size = ruc_buf_getPayloadLen(rpcCtx->xmitBuf) - rpcCtx->position;
  if (size == 0) {
    return storio_disk_write_empty(thread_ctx_p,msg);
  }   
  return storio_disk_write(thread_ctx_p,msg);   

out:           
  storio_encode_rpc_response(rpcCtx,(char*)&ret);  
  storio_send_response(thread_ctx_p,msg,0);

  /*
  ** Update statistics
  */
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.rebStart_time +=(timeAfter-timeBefore);  
}
/*__________________________________________________________________________
*/
/**
*  repair data of a file

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storio_disk_write_repair3(rozofs_disk_thread_ctx_t *thread_ctx_p,storio_disk_thread_msg_t * msg) {
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  storage_t *st = 0;
  sp_write_repair3_arg_no_bins_t * args;
  rozorpc_srv_ctx_t      * rpcCtx;
  sp_write_ret_t           ret;
  uint8_t                  version = 0;
  int                      size;
  int                      is_fid_faulty;
  storio_device_mapping_t * fidCtx;
    
  
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);

  ret.status = SP_FAILURE;          
  
  /*
  ** update statistics
  */
  thread_ctx_p->stat.diskRepair_count++;
  
  rpcCtx = msg->rpcCtx;
  args   = (sp_write_repair3_arg_no_bins_t*) ruc_buf_getPayload(rpcCtx->decoded_arg);

  fidCtx = storio_device_mapping_ctx_retrieve(msg->fidIdx);
  if (fidCtx == NULL) {
    ret.sp_write_ret_t_u.error = EIO;
    severe("Bad FID ctx index %d",msg->fidIdx); 
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.diskRepair_error++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }  
    
  /*
  ** set the pointer to the bins that are in the xmit buffer
  ** since received bufer is also used for the response
  */
  char *pbuf = ruc_buf_getPayload(rpcCtx->xmitBuf); 
  pbuf += rpcCtx->position;
  

  // Get the storage for the couple (cid;sid)
  if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
    ret.sp_write_ret_t_u.error = errno;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.diskRepair_badCidSid++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
  
  
  // Write projections
  size =  storage_write_repair3(st, fidCtx, args->layout, args->bsize, (sid_t *) args->dist_set, args->spare,
          (unsigned char *) args->fid, args->bid, args->nb_proj, args->blk2repair, version,
          &ret.sp_write_ret_t_u.file_size,(bin_t *) pbuf, &is_fid_faulty);
  if (size < 0)  {
    ret.sp_write_ret_t_u.error = errno;
    thread_ctx_p->stat.diskRepair_error++; 
    if (is_fid_faulty) {
      storio_register_faulty_fid(thread_ctx_p->thread_idx,
				 args->cid,
				 args->sid,
				 (uint8_t*)args->fid);
    }       
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
  msg->size = size;   
    
  ret.status = SP_SUCCESS;  
  ret.sp_write_ret_t_u.file_size = 0;
           
  storio_encode_rpc_response(rpcCtx,(char*)&ret);  
  thread_ctx_p->stat.diskRepair_Byte_count += size;
  storio_send_response(thread_ctx_p,msg,0);

  /*
  ** Update statistics
  */
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.diskRepair_time +=(timeAfter-timeBefore);  
}    
/**
*  Truncate a file

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storio_disk_truncate(rozofs_disk_thread_ctx_t *thread_ctx_p,storio_disk_thread_msg_t * msg) {
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  storage_t *st = 0;
  sp_truncate_arg_no_bins_t      * args;
  rozorpc_srv_ctx_t      * rpcCtx;
  sp_status_ret_t          ret;
  uint8_t                  version = 0;
  int                      result;
  int                      is_fid_faulty;
  storio_device_mapping_t * fidCtx;
    
  
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);

  ret.status = SP_FAILURE;          
  
  /*
  ** update statistics
  */
  thread_ctx_p->stat.truncate_count++;
  
  rpcCtx = msg->rpcCtx;
  args   = (sp_truncate_arg_no_bins_t*) ruc_buf_getPayload(rpcCtx->decoded_arg);


  fidCtx = storio_device_mapping_ctx_retrieve(msg->fidIdx);
  if (fidCtx == NULL) {
    ret.sp_status_ret_t_u.error = EIO;
    severe("Bad FID ctx index %d",msg->fidIdx); 
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.read_error++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }  
    
  /*
  ** set the pointer to the bins that are in the xmit buffer
  ** since received bufer is also used for the response
  */
  char *pbuf = ruc_buf_getPayload(rpcCtx->xmitBuf); 
  pbuf += rpcCtx->position;
    

  // Get the storage for the couple (cid;sid)
  if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
    ret.sp_status_ret_t_u.error = errno;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.truncate_badCidSid++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }

  // Truncate bins file
  result = storage_truncate(st, fidCtx, args->layout, args->bsize, (sid_t *) args->dist_set,
        		    args->spare, (unsigned char *) args->fid, args->proj_id,
        		    args->bid,version,args->last_seg,args->last_timestamp,
			    args->len, pbuf, &is_fid_faulty);
  if (result != 0) {
    ret.sp_status_ret_t_u.error = errno;
    thread_ctx_p->stat.truncate_error++; 
    if (is_fid_faulty) {
      storio_register_faulty_fid(thread_ctx_p->thread_idx,
				 args->cid,
				 args->sid,
				 (uint8_t*)args->fid);
    }           
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
  
  ret.status = SP_SUCCESS;          
  storio_encode_rpc_response(rpcCtx,(char*)&ret);  
  storio_send_response(thread_ctx_p,msg,0);

  /*
  ** Update statistics
  */
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.truncate_time +=(timeAfter-timeBefore);  
}    


/**
*  Remove a file

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storio_disk_remove(rozofs_disk_thread_ctx_t *thread_ctx_p,storio_disk_thread_msg_t * msg) {
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  storage_t *st = 0;
  sp_remove_arg_t      * args;
  rozorpc_srv_ctx_t      * rpcCtx;
  sp_status_ret_t          ret;
  int                      result;
  //storio_device_mapping_t * fidCtx;
  
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);

  ret.status = SP_FAILURE;          
  
  /*
  ** update statistics
  */
  thread_ctx_p->stat.remove_count++;
  
  rpcCtx = msg->rpcCtx;
  args   = (sp_remove_arg_t*) ruc_buf_getPayload(rpcCtx->decoded_arg);
  //fidCtx = msg->fidCtx;
  

  // Get the storage for the couple (cid;sid)
  if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
    ret.sp_status_ret_t_u.error = errno;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.remove_badCidSid++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
  

  // remove bins file
  result = storage_rm_file(st,(unsigned char *) args->fid);
  if (result != 0) {
    ret.sp_status_ret_t_u.error = errno;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.remove_error++; 
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
  
  ret.status = SP_SUCCESS;          
  storio_encode_rpc_response(rpcCtx,(char*)&ret);  
  storio_send_response(thread_ctx_p,msg,0);

  /*
  ** Update statistics
  */
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.remove_time +=(timeAfter-timeBefore);  
}    
/**
*  Remove a chunk file

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storio_disk_remove_chunk(rozofs_disk_thread_ctx_t *thread_ctx_p,storio_disk_thread_msg_t * msg) {
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  storage_t *st = 0;
  sp_remove_chunk_arg_t      * args;
  rozorpc_srv_ctx_t      * rpcCtx;
  sp_status_ret_t          ret;
  int                      result;
  int                      is_fid_faulty;
  storio_device_mapping_t * fidCtx;
  
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);

  ret.status = SP_FAILURE;          
  
  /*
  ** update statistics
  */
  thread_ctx_p->stat.remove_chunk_count++;
  
  rpcCtx = msg->rpcCtx;
  args   = (sp_remove_chunk_arg_t*) ruc_buf_getPayload(rpcCtx->decoded_arg);

  fidCtx = storio_device_mapping_ctx_retrieve(msg->fidIdx);
  if (fidCtx == NULL) {
    ret.sp_status_ret_t_u.error = EIO;
    severe("Bad FID ctx index %d",msg->fidIdx); 
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.remove_chunk_error++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  } 
  
  // Get the storage for the couple (cid;sid)
  if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
    ret.sp_status_ret_t_u.error = errno;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.remove_chunk_badCidSid++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
  

  // remove chunk file
  result = storage_rm_chunk(st, fidCtx, 
                            args->layout, args->bsize, args->spare,
			    (sid_t *)args->dist_set, (unsigned char*)args->fid,
			    args->chunk, &is_fid_faulty);
  if (result != 0) {
    if (is_fid_faulty) {
      storio_register_faulty_fid(thread_ctx_p->thread_idx,
				 args->cid,
				 args->sid,
				 (uint8_t*)args->fid);
    }          
    ret.sp_status_ret_t_u.error = errno;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.remove_chunk_error++; 
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
  
  ret.status = SP_SUCCESS;          
  storio_encode_rpc_response(rpcCtx,(char*)&ret);  
  storio_send_response(thread_ctx_p,msg,0);

  /*
  ** Update statistics
  */
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.remove_chunk_time +=(timeAfter-timeBefore);  
}    

/*
**   D I S K   T H R E A D
*/

void *storio_disk_thread(void *arg) {
  storio_disk_thread_msg_t   msg;
  rozofs_disk_thread_ctx_t * ctx_p = (rozofs_disk_thread_ctx_t*)arg;
  int                        bytesRcvd;
  uint64_t                   newval;

  uma_dbg_thread_add_self("Disk thread");

#if 1
    {
      struct sched_param my_priority;
      int policy=-1;
      int ret= 0;

     pthread_getschedparam(pthread_self(),&policy,&my_priority);
          DEBUG("storio main thread Scheduling policy   = %s\n",
                    (policy == SCHED_OTHER) ? "SCHED_OTHER" :
                    (policy == SCHED_FIFO)  ? "SCHED_FIFO" :
                    (policy == SCHED_RR)    ? "SCHED_RR" :
                    "???");
 #if 1
      my_priority.sched_priority= 98;
      policy = SCHED_FIFO;
      ret = pthread_setschedparam(pthread_self(),policy,&my_priority);
      if (ret < 0) 
      {
	severe("error on sched_setscheduler: %s",strerror(errno));	
      }
      pthread_getschedparam(pthread_self(),&policy,&my_priority);
          DEBUG("RozoFS thread Scheduling policy (prio %d)  = %s\n",my_priority.sched_priority,
                    (policy == SCHED_OTHER) ? "SCHED_OTHER" :
                    (policy == SCHED_FIFO)  ? "SCHED_FIFO" :
                    (policy == SCHED_RR)    ? "SCHED_RR" :
                    "???");
 #endif        

    }  
#endif     

  //info("Disk Thread %d Started !!\n",ctx_p->thread_idx);
  
  while(1) {
  
    /*
    ** read the north disk socket
    */
    bytesRcvd = recvfrom(af_unix_disk_socket_ref,
			 &msg,sizeof(msg), 
			 0,(struct sockaddr *)NULL,NULL);
    if (bytesRcvd == -1) {
      fatal("Disk Thread %d recvfrom %s !!\n",ctx_p->thread_idx,strerror(errno));
      exit(0);
    }
    if (bytesRcvd != sizeof(msg)) {
      fatal("Disk Thread %d socket is dead (%d/%d) %s !!\n",ctx_p->thread_idx,bytesRcvd,(int)sizeof(msg),strerror(errno));
      exit(0);    
    }
      
    newval = __atomic_fetch_add(&af_unix_disk_parallel_req,1,__ATOMIC_SEQ_CST);
    newval++;
    af_unix_disk_parallel_req_tbl[newval]++;
        
    msg.size = 0;
    
    switch (msg.opcode) {
    
      case STORIO_DISK_THREAD_READ:
        storio_disk_read(ctx_p,&msg);
        break;
	
      case STORIO_DISK_THREAD_RESIZE:
        storio_disk_resize(ctx_p,&msg);
        break;	
	
      case STORIO_DISK_THREAD_WRITE:
        storio_disk_write(ctx_p,&msg);
        break;
	
      case STORIO_DISK_THREAD_WRITE_EMPTY:
        storio_disk_write_empty(ctx_p,&msg);
        break;
	
      case STORIO_DISK_THREAD_TRUNCATE:
        storio_disk_truncate(ctx_p,&msg);
        break;

        
      case STORIO_DISK_THREAD_WRITE_REPAIR3:
        storio_disk_write_repair3(ctx_p,&msg);
        break;
	
      case STORIO_DISK_THREAD_REMOVE:
        storio_disk_remove(ctx_p,&msg);
        break;
	       	
      case STORIO_DISK_THREAD_REMOVE_CHUNK:
        storio_disk_remove_chunk(ctx_p,&msg);
        break;
	
      case STORIO_DISK_THREAD_REBUILD_START:
        storio_disk_rebuild_start(ctx_p,&msg);
        break;
	
      case STORIO_DISK_THREAD_REBUILD_STOP:
        storio_disk_rebuild_stop(ctx_p,&msg);
        break;

      case STORIO_DISK_THREAD_FID:
        storio_disk_serial(ctx_p,&msg);
        break;	
      default:
        fatal(" unexpected opcode : %d\n",msg.opcode);
        exit(0);       
    }
    newval = __atomic_fetch_sub(&af_unix_disk_parallel_req,1,__ATOMIC_SEQ_CST)-1;
//    sched_yield();
  }
}
/*
** Create the threads that will handle all the disk requests

* @param hostname    storio hostname (for tests)
* @param nb_threads  number of threads to create
*  
* @retval 0 on success -1 in case of error
*/
int storio_disk_thread_create(char * hostname, int nb_threads, int instance_id) {
   int                        i;
   int                        err;
   pthread_attr_t             attr;
   rozofs_disk_thread_ctx_t * thread_ctx_p;
   char                       socketName[128];

   /*
   ** clear the thread table
   */
   memset(rozofs_disk_thread_ctx_tb,0,sizeof(rozofs_disk_thread_ctx_tb));
   /*
   ** create the common socket to receive requests on
   */
   char * pChar = socketName;
   pChar += rozofs_string_append(pChar,ROZOFS_SOCK_FAMILY_DISK_NORTH);
   *pChar++ = '_';
   pChar += rozofs_u32_append(pChar,instance_id);
   *pChar++ = '_';  
   pChar += rozofs_string_append(pChar,hostname);
   af_unix_disk_socket_ref = af_unix_disk_sock_create_internal(socketName,1024*32);
   if (af_unix_disk_socket_ref < 0) {
      fatal("af_unix_disk_thread_create af_unix_disk_sock_create_internal(%s) %s",socketName,strerror(errno));
      return -1;   
   }
   /*
   ** Now create the threads
   */
   thread_ctx_p = rozofs_disk_thread_ctx_tb;
   for (i = 0; i < nb_threads ; i++) {
   
     thread_ctx_p->hostname = hostname;
     /*
     ** create the thread specific socket to send the response from 
     */
     pChar = socketName;
     pChar += rozofs_string_append(pChar,ROZOFS_SOCK_FAMILY_DISK_NORTH);
     *pChar++ = '_';
     pChar += rozofs_u32_append(pChar,instance_id);
     *pChar++ = '_';  
     pChar += rozofs_string_append(pChar,hostname);
     *pChar++ = '_'; 
     pChar += rozofs_u32_append(pChar,i);
     thread_ctx_p->sendSocket = af_unix_disk_sock_create_internal(socketName,1024*32);
     if (thread_ctx_p->sendSocket < 0) {
	fatal("af_unix_disk_thread_create af_unix_disk_sock_create_internal(%s) %s",socketName, strerror(errno));
	return -1;   
     }   
   
     err = pthread_attr_init(&attr);
     if (err != 0) {
       fatal("af_unix_disk_thread_create pthread_attr_init(%d) %s",i,strerror(errno));
       return -1;
     }  

     thread_ctx_p->thread_idx = i;
     err = pthread_create(&thread_ctx_p->thrdId,&attr,storio_disk_thread,thread_ctx_p);
     if (err != 0) {
       fatal("af_unix_disk_thread_create pthread_create(%d) %s",i, strerror(errno));
       return -1;
     }  
     
     thread_ctx_p++;
  }
  return 0;
}
 
 

/*__________________________________________________________________________
*/
/**
*  Process requests found in the serial_pending_request list of a FID

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
void storio_disk_serial(rozofs_disk_thread_ctx_t *ctx_p,storio_disk_thread_msg_t * msg_in) {

  storio_device_mapping_t * fidCtx;
  list_t diskthread_list;
  int empty;
  rozorpc_srv_ctx_t      * rpcCtx;
  storio_disk_thread_msg_t   msg;  
  int fdl_count = 0;
  
  memcpy(&msg,msg_in,sizeof(storio_disk_thread_msg_t));
  /*
  ** get the FID context where fits the pending request list
  */
  fidCtx = storio_device_mapping_ctx_retrieve(msg.fidIdx);
  if (fidCtx == NULL) {
    fatal("Bad FID ctx index %d",msg.fidIdx); 
    return;
  }
  /*
  ** init of the local list
  */
  list_init(&diskthread_list);
  /*
  ** process the requests
  */
  while(1)
  {
     
     empty = storio_get_pending_request_list(fidCtx,&diskthread_list);
     if (empty) break;
     
     rpcCtx = list_first_entry(&diskthread_list,rozorpc_srv_ctx_t,list);
     list_remove(&rpcCtx->list);
     fdl_count++;
     
     msg.opcode = rpcCtx->opcode; 
     msg.size   = 0;
     msg.rpcCtx = rpcCtx;
     msg.timeStart = rpcCtx->profiler_time;
     
     switch (msg.opcode) {

       case STORIO_DISK_THREAD_READ:
         storio_disk_read(ctx_p,&msg);
         break;

       case STORIO_DISK_THREAD_RESIZE:
         storio_disk_resize(ctx_p,&msg);
         break;	

       case STORIO_DISK_THREAD_WRITE:
         storio_disk_write(ctx_p,&msg);
         break;

       case STORIO_DISK_THREAD_WRITE_EMPTY:
         storio_disk_write_empty(ctx_p,&msg);
         break;

       case STORIO_DISK_THREAD_TRUNCATE:
         storio_disk_truncate(ctx_p,&msg);
         break;

       case STORIO_DISK_THREAD_WRITE_REPAIR3:
         storio_disk_write_repair3(ctx_p,&msg);
         break;

       case STORIO_DISK_THREAD_REMOVE:
         storio_disk_remove(ctx_p,&msg);
         break;

       case STORIO_DISK_THREAD_REMOVE_CHUNK:
         storio_disk_remove_chunk(ctx_p,&msg);
         break;

       case STORIO_DISK_THREAD_REBUILD_START:
         storio_disk_rebuild_start(ctx_p,&msg);
         break;

       case STORIO_DISK_THREAD_REBUILD_STOP:
         storio_disk_rebuild_stop(ctx_p,&msg);
         break;

       default:
         fatal(" unexpected opcode : %d\n",msg.opcode);
         exit(0);       
     }       
  } 
//  severe("FDL storio count %d for %p",fdl_count, fidCtx);  
}
 
