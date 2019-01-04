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

#include <limits.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <inttypes.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/common/profile.h>
#include <rozofs/rpc/spproto.h>
#include <rozofs/rpc/sproto.h>
#include <rozofs/core/uma_dbg_api.h>

#include "storage.h"
#include "storaged.h"
#include "sproto_nb.h"
#include "storaged_north_intf.h"
#include "storage_fd_cache.h"
#include "storio_disk_thread_intf.h"
#include "storio_device_mapping.h"
#include "storio_serialization.h"
#include "storage_enumeration.h"

/*
** Detailed time counters for read and write operation
*/
#define STORIO_DETAILED_COUNTER_MAX  60
#define STORIO_DETAILED_READ_SLICE   64
#define STORIO_DETAILED_WRITE_SLICE 128
typedef struct _STORIO_DETAILED_COUNTERS_T {
  uint64_t     write[STORIO_DETAILED_COUNTER_MAX+1];
  uint64_t     read[STORIO_DETAILED_COUNTER_MAX+1];  
} STORIO_DETAILED_COUNTERS_T;
static STORIO_DETAILED_COUNTERS_T storio_detailed_counters;


/*_______________________________________________________________________
* Update detailed time counters for wrtite operation
* @param delay delay in us of the write operation
*/
void update_write_detailed_counters(uint64_t delay) {
  delay = delay / STORIO_DETAILED_READ_SLICE;
  if (delay >= STORIO_DETAILED_COUNTER_MAX) delay = STORIO_DETAILED_COUNTER_MAX;
  storio_detailed_counters.write[delay]++;
}

/*_______________________________________________________________________
* Update detailed time counters for wrtite operation
* @param delay delay in us of the write operation
*/
void update_read_detailed_counters(uint64_t delay) {
  delay = delay / STORIO_DETAILED_WRITE_SLICE;
  if (delay > STORIO_DETAILED_COUNTER_MAX) delay = STORIO_DETAILED_COUNTER_MAX;
  storio_detailed_counters.read[delay]++;
}

/*_______________________________________________________________________
* Update detailed time counters for wrtite operation
* @param delay delay in us of the write operation
*/
static inline void reset_detailed_counters(void) {
  memset(&storio_detailed_counters,0,sizeof(storio_detailed_counters));
}

/*_______________________________________________________________________
* Update detailed time counters for wrtite operation
* @param delay delay in us of the write operation
*/
static char * display_detailed_counters_help(char * pChar) {
  pChar += rozofs_string_append(pChar,"usage:\ndetailedTiming reset       : reset statistics\ndetailedTiming             : display statistics\n");  
  return pChar; 
}
/*_______________________________________________________________________

*/
void display_detailed_counters (char * argv[], uint32_t tcpRef, void *bufRef) {
  char          * p = uma_dbg_get_buffer();
  int             i;
  int             start_read,stop_read;
  int             start_write,stop_write;
  int             doreset=0;
  
  if (argv[1] != NULL) {
    if (strcmp(argv[1],"reset")==0) {
      doreset = 1;
    }
    else {
      p = display_detailed_counters_help(p);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;    
    }  
  }  
  
  start_read = start_write = 0;
  stop_read  = STORIO_DETAILED_READ_SLICE;
  stop_write  = STORIO_DETAILED_WRITE_SLICE;

  p += rozofs_string_append(p, "    READ                                 WRITE\n");  
  for (i=0; i<=STORIO_DETAILED_COUNTER_MAX; i++) {
  
    p += rozofs_u64_padded_append(p, 4, rozofs_right_alignment,start_read);
    p += rozofs_string_append(p, "..");
    p += rozofs_u64_padded_append(p, 4, rozofs_left_alignment,stop_read);
    p += rozofs_string_append(p, " : ");
    p += rozofs_u64_padded_append(p, 16, rozofs_right_alignment, storio_detailed_counters.read[i]);
    p += rozofs_string_append(p, "        ");
    p += rozofs_u64_padded_append(p, 4, rozofs_right_alignment,start_write);
    p += rozofs_string_append(p, "..");
    p += rozofs_u64_padded_append(p, 4, rozofs_left_alignment,stop_write);
    p += rozofs_string_append(p, " : ");
    p += rozofs_u64_padded_append(p, 16, rozofs_right_alignment, storio_detailed_counters.write[i]);
    p += rozofs_eol(p);

    start_read = stop_read;
    stop_read += STORIO_DETAILED_READ_SLICE; 
    start_write = stop_write;
    stop_write += STORIO_DETAILED_WRITE_SLICE;       
  }
  
  if (doreset) {
    reset_detailed_counters();
    p += rozofs_string_append(p,"Reset done\n");  
  }
  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());    
}
/*_______________________________________________________________________
* Initialize detailed time counters service
*/
void detailed_counters_init(void) {
  reset_detailed_counters();
  uma_dbg_addTopic_option("detailedTiming", display_detailed_counters,UMA_DBG_OPTION_RESET); 
}
/*_______________________________________________________________________
*/
DECLARE_PROFILING(spp_profiler_t);

int rozofs_storcli_fake_encode(xdrproc_t encode_fct,void *msg2encode_p)
{
    uint8_t *buf= NULL;
    XDR               xdrs;    
    struct rpc_msg   call_msg;
    int              opcode = 0;
    uint32_t         null_val = 0;
    int              position = -1;
    
    buf = malloc(2048);
    if (buf == NULL)
    {
       severe("Out of memory");
       goto out;
    
    }
    xdrmem_create(&xdrs,(char*)buf,2048,XDR_ENCODE);
    /*
    ** fill in the rpc header
    */
    call_msg.rm_direction = CALL;
    /*
    ** allocate a xid for the transaction 
    */
	call_msg.rm_xid             = 1; 
	call_msg.rm_call.cb_rpcvers = RPC_MSG_VERSION;
	/* XXX: prog and vers have been long historically :-( */
	call_msg.rm_call.cb_prog = (uint32_t)0;
	call_msg.rm_call.cb_vers = (uint32_t)0;
	if (! xdr_callhdr(&xdrs, &call_msg))
    {
       /*
       ** THIS MUST NOT HAPPEN
       */
       severe("fake encoding error");
       goto out;	
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
       severe("fake encoding error");
       goto out;
    }
    /*
    ** Now get the current length and fill the header of the message
    */
    position = XDR_GETPOS(&xdrs);
    /*
    ** add the length that corresponds to the field that contains the length part
    ** of the rpc message
    */
    position += sizeof(uint32_t);
out:
    if (buf != NULL) free(buf);
    return position;    
}


int storage_bin_write_first_bin_to_read = 0;

/*
**__________________________________________________________________________
*/
/**
*  That service is intended to be called by the write service
*  The goal is to provide the position of the first byte to write on disk

  @param none
  @retval position of the first byte
*/
int storage_get_position_of_first_byte2write_from_write_req()
{
  sp_write_arg_no_bins_t *request; 
  sp_write_arg_no_bins_t  write_prj_args;
  int position;
  
  
  if (storage_bin_write_first_bin_to_read == 0)
  {
    request = &write_prj_args;
    memset(request,0,sizeof(sp_write_arg_no_bins_t));
    position = rozofs_storcli_fake_encode((xdrproc_t) xdr_sp_write_arg_no_bins_t, (caddr_t) request);
    if (position < 0)
    {
      fatal("Cannot get the size of the rpc header for writing");
      return 0;    
    }
    storage_bin_write_first_bin_to_read = position;
  }
  return storage_bin_write_first_bin_to_read;

}
/*
**__________________________________________________________________________
*/

int storage_bin_read_first_bin_to_write = 0;

int storage_get_position_of_first_byte2write_from_read_req()

{
   int position;
   if (storage_bin_read_first_bin_to_write != 0) return storage_bin_read_first_bin_to_write;
   {
      /*
      ** now get the current position in the buffer for loading the first byte of the bins 
      */  
      position =  sizeof(uint32_t); /* length header of the rpc message */
      position += rozofs_rpc_get_min_rpc_reply_hdr_len();
      position += sizeof(uint32_t);   /* length of the storage status field */
      position += (3*sizeof(uint32_t));   /* length of the alignment field (FDL) */
      position += sizeof(uint32_t);   /* length of the bins len field */

      storage_bin_read_first_bin_to_write = position;
    }
    return storage_bin_read_first_bin_to_write;
}

/*
**__________________________________________________________________________
*/
/**
* send a rpc reply: the encoding function MUST be found in xdr_result 
 of the gateway context

  It is assumed that the xmitBuf MUST be found in xmitBuf field
  
  In case of a success it is up to the called function to release the xmit buffer
  
  @param p : pointer to the root transaction context used for the read
  @param arg_ret : returned argument to encode 
  
  @retval none

*/
void storaged_srv_forward_read_success (rozorpc_srv_ctx_t *p,sp_read_ret_t * arg_ret)
{
   int ret;
   uint8_t *pbuf;           /* pointer to the part that follows the header length */
   uint32_t *header_len_p;  /* pointer to the array that contains the length of the rpc message*/
   XDR xdrs;
   int len;
   int cur_len;

   if (p->xmitBuf == NULL)
   {
      ROZORPC_SRV_STATS(ROZORPC_SRV_NO_BUFFER_ERROR);
      severe("no xmit buffer");
      goto error;
   } 
    /*
    ** create xdr structure on top of the buffer that will be used for sending the response
    */
    header_len_p = (uint32_t*)ruc_buf_getPayload(p->xmitBuf); 
    pbuf = (uint8_t*) (header_len_p+1);            
    len = (int)ruc_buf_getMaxPayloadLen(p->xmitBuf);
    len -= sizeof(uint32_t);
    xdrmem_create(&xdrs,(char*)pbuf,len,XDR_ENCODE); 
    /*
    ** just encode the header
    */
    if (rozofs_encode_rpc_reply(&xdrs,(xdrproc_t)NULL,(caddr_t)NULL,p->src_transaction_id) != TRUE)
    {
      ROZORPC_SRV_STATS(ROZORPC_SRV_ENCODING_ERROR);
      severe("rpc reply encoding error");
      goto error;     
    }
    /*
    ** OK now starts encoding the response
    */
    xdr_sp_status_t (&xdrs, &arg_ret->status);
    xdr_uint32_t (&xdrs, &arg_ret->sp_read_ret_t_u.rsp.filler);          
    xdr_uint32_t (&xdrs, &arg_ret->sp_read_ret_t_u.rsp.bins.bins_len); 
    /*
    ** skip the bins
    */
    cur_len =  xdr_getpos(&xdrs) ;     
    cur_len +=  arg_ret->sp_read_ret_t_u.rsp.bins.bins_len;
    xdr_setpos(&xdrs,cur_len);
    /*
    ** encode the length of the file
    */
    xdr_uint64_t (&xdrs, &arg_ret->sp_read_ret_t_u.rsp.file_size);
#if 0 // for future usage with distributed cache 
    /*
    ** encode the optim array
    */
    xdr_bytes (&xdrs, (char **)&arg_ret->sp_read_ret_t_u.rsp.optim.optim_val, 
                       (u_int *) &arg_ret->sp_read_ret_t_u.rsp.optim.optim_len, ~0);  
#endif
    /*
    ** compute the total length of the message for the rpc header and add 4 bytes more bytes for
    ** the ruc buffer to take care of the header length of the rpc message.
    */
    int total_len = xdr_getpos(&xdrs) ;
    *header_len_p = htonl(0x80000000 | total_len);
    total_len +=sizeof(uint32_t);
    ruc_buf_setPayloadLen(p->xmitBuf,total_len);

    /*
    ** Get the callback for sending back the response:
    ** A callback is needed since the request for read might be local or remote
    */
    ret =  af_unix_generic_send_stream_with_idx((int)p->socketRef,p->xmitBuf);  
    if (ret == 0)
    {
      /**
      * success so remove the reference of the xmit buffer since it is up to the called
      * function to release it
      */
      ROZORPC_SRV_STATS(ROZORPC_SRV_SEND);
      p->xmitBuf = NULL;
    }
    else
    {
      ROZORPC_SRV_STATS(ROZORPC_SRV_SEND_ERROR);
    }
error:
    return;
}

/*
**___________________________________________________________
*/

void sp_null_1_svc_nb(void *args, rozorpc_srv_ctx_t *req_ctx_p) {
    DEBUG_FUNCTION;
    static void *result = NULL;
     req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
     req_ctx_p->recv_buf = NULL;
     
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&result); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);     
    return ;
}
/*
**___________________________________________________________
*/

void sp_write_empty_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_write_ret_t ret;
    storio_device_mapping_t * dev_map_p = NULL;
    sp_write_arg_t          * write_arg_p = (sp_write_arg_t *) pt;
    uint8_t                   nb_rebuild;
    uint8_t                   storio_rebuild_ref;
    STORIO_REBUILD_T        * pRebuild; 
    int                       same_recycle_cpt;
    int                       write_last;
    int                       write_first;
    
    START_PROFILING(write_empty);

    /*
    ** Use received buffer for the response
    */
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;
    req_ctx_p->opcode   = STORIO_DISK_THREAD_WRITE_EMPTY;
  
    /*
    ** Rebuild reference is 0 when the write comes from the STORCLI.
    ** When the write comes from a rebuild process, this reference is 
    ** the index of the rebuild context whithin the FID context {1..4}
    */
    nb_rebuild = write_arg_p->rebuild_ref;
    if (nb_rebuild > MAX_FID_PARALLEL_REBUILD) {
      /* bad reference ?? */
      severe("Bad rebuild ref %d",nb_rebuild);
      errno = EAGAIN; // Break this rebuild
      goto error;	
    } 

    /*
    ** Lookup for the FID context in the lookup table. 
    */
    dev_map_p = storio_device_mapping_search(write_arg_p->cid, 
                                             write_arg_p->sid, 
					     write_arg_p->fid,
					     &same_recycle_cpt);


    write_first = write_arg_p->bid;
    write_last  = write_first + write_arg_p->nb_proj - 1;

    /*
    ** THIS IS A WRITE FROM A REBUILD PROCESS
    */
    if (nb_rebuild != 0) {
    
      /*
      ** No mapping context, so no such valid rebuild
      */
      if (dev_map_p == NULL) {
	errno = EAGAIN;  // Break this rebuild
	goto error;
      }   
          
      /*
      ** Retrieve the rebuild context from the rebuild index
      */
      nb_rebuild--;
      storio_rebuild_ref = dev_map_p->storio_rebuild_ref.u8[nb_rebuild];
      pRebuild = storio_rebuild_ctx_retrieve(storio_rebuild_ref, (char*)write_arg_p->fid);
      if (pRebuild == NULL) {
	/* This context is running for this FID */
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;
	errno = EAGAIN;
	goto error;
      }      
     
      /* check that the rebuild does not overrun the rebuild area... */
      /* ... after ... */ 
      if (((pRebuild->stop_block != -1) &&  (write_last>pRebuild->stop_block))
      /* ... or before. */
      ||  (write_first < pRebuild->start_block)) {
	storio_rebuild_ctx_free(pRebuild);
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;
	errno = EAGAIN;
	goto error;
      }
      
      /*
      ** Rebuild still on going. Update the time stamp
      ** and starting point of the on going rebuild
      */        
      pRebuild->rebuild_ts  = time(NULL);
      pRebuild->start_block = write_first;
      
      /*
      ** Forward the request to the disk thread.
      ** On the 1rzt write of a rebuild with relocate
      */
      
      if (pRebuild->relocate == RBS_TO_RELOCATE) {
        req_ctx_p->opcode = STORIO_DISK_THREAD_REBUILD_START;
      }
      goto send_to_disk_thread;   
    }
    
      
    /*
    ** THIS IS A WRITE WRITE FROM THE STORCLI.
    */ 

    /*
    ** No mapping context exist, so create one
    */
    if (dev_map_p==NULL) {
      dev_map_p = storio_device_mapping_insert (write_arg_p->cid, 
                                                write_arg_p->sid, 
						write_arg_p->fid);
      if (dev_map_p == NULL) { 
        goto error;// errno already set
      }
      goto send_to_disk_thread;
    } 

    /*
    ** A mapping context exist containing information synchronized
    ** with the disk header file content.
    */    
    if (storio_get_dev(dev_map_p,0) != ROZOFS_UNKNOWN_CHUNK) {
      /*
      ** This is not the same recycling counter, so let's recycle
      */
      if (!same_recycle_cpt) {
	/* 
	** This is an old file that is being recycled.
	** Let's clear the chunk distribution to force a header file update
	** and a data truncate.
	*/
        storio_free_dev_mapping(dev_map_p);
        dev_map_p->recycle_cpt =  rozofs_get_recycle_from_fid(write_arg_p->fid);
      }
    } 

    /*
    ** Check whether this write breaks a running rebuild
    */
    if (dev_map_p->storio_rebuild_ref.u64 == 0xFFFFFFFFFFFFFFFF ) {
      // No running rebuild
      goto send_to_disk_thread;
    }
    
    /*
    ** Check for compatibility with all the running rebuilds
    */    
    for (nb_rebuild=0; nb_rebuild < MAX_FID_PARALLEL_REBUILD; nb_rebuild++) {

      storio_rebuild_ref = dev_map_p->storio_rebuild_ref.u8[nb_rebuild];

      /* This context is free */
      if (storio_rebuild_ref == 0xFF) {
	continue;
      }

      /*
      ** Retrieve the rebuild context  
      */
      pRebuild = storio_rebuild_ctx_retrieve(storio_rebuild_ref, (char*)write_arg_p->fid);
      if (pRebuild == NULL) {
	/* This context is not allocated for this FID,
	** or not the same recycling counter */
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF; // break this rebuild
        continue;
      }

      /*
      ** Writing the area that the rebuild process is trying to rebuild
      ** breaks the rebuild.
      */
      if ((write_first > pRebuild->start_block) 
       && ((pRebuild->stop_block==-1)||(write_first <= pRebuild->stop_block))) {
        /* 
	** Writing within the rebuild area !
	** Let's shrink the rebuild area to the 1rst written block
	*/
	pRebuild->stop_block = write_first-1;    
      }  
      else if ((write_first <= pRebuild->start_block) && (write_last >= pRebuild->start_block)) {
        /* 
	** Writing at the beginning of the rebuild area !
	** it breaks the rebuild
	*/        
	storio_rebuild_ctx_free(pRebuild);
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF; // break this rebuild	
      }
    }         


send_to_disk_thread:
        
    /*
    ** If any request is already running, chain this request on the FID context
    */
    if (!storio_serialization_begin(dev_map_p,req_ctx_p)){
      goto out;
    }   

    if (storio_disk_thread_intf_send(dev_map_p, req_ctx_p, tic) == 0) {
      goto out;
    }  
    severe("storio_disk_thread_intf_send %s", strerror(errno));


error:    
    
    ret.status                 = SP_FAILURE;            
    ret.sp_write_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(write_empty);

out:
    return;
}
/*
**___________________________________________________________
*/

void sp_write_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_write_ret_t ret;
    storio_device_mapping_t * dev_map_p = NULL;
    sp_write_arg_t          * write_arg_p = (sp_write_arg_t *) pt;
    uint8_t                   nb_rebuild;
    uint8_t                   storio_rebuild_ref;
    STORIO_REBUILD_T        * pRebuild; 
    int                       same_recycle_cpt;
    int                       write_last;
    int                       write_first;
    
    START_PROFILING(write);

    /*
    ** Use received buffer for the response
    */
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;
    req_ctx_p->opcode   = STORIO_DISK_THREAD_WRITE;
  
    /*
    ** Rebuild reference is 0 when the write comes from the STORCLI.
    ** When the write comes from a rebuild process, this reference is 
    ** the index of the rebuild context whithin the FID context {1..4}
    */
    nb_rebuild = write_arg_p->rebuild_ref;
    if (nb_rebuild > MAX_FID_PARALLEL_REBUILD) {
      /* bad reference ?? */
      severe("Bad rebuild ref %d",nb_rebuild);
      errno = EAGAIN; // Break this rebuild
      goto error;	
    } 

    /*
    ** Lookup for the FID context in the lookup table. 
    */
    dev_map_p = storio_device_mapping_search(write_arg_p->cid, 
                                             write_arg_p->sid, 
					     write_arg_p->fid,
					     &same_recycle_cpt);


    write_first = write_arg_p->bid;
    write_last  = write_first + write_arg_p->nb_proj - 1;

    /*
    ** THIS IS A WRITE FROM A REBUILD PROCESS
    */
    if (nb_rebuild != 0) {
    
      /*
      ** No mapping context, so no such valid rebuild
      */
      if (dev_map_p == NULL) {
	errno = EAGAIN;  // Break this rebuild
	goto error;
      }   
          
      /*
      ** Retrieve the rebuild context from the rebuild index
      */
      nb_rebuild--;
      storio_rebuild_ref = dev_map_p->storio_rebuild_ref.u8[nb_rebuild];
      pRebuild = storio_rebuild_ctx_retrieve(storio_rebuild_ref, (char*)write_arg_p->fid);
      if (pRebuild == NULL) {
	/* This context is running for this FID */
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;
	errno = EAGAIN;
	goto error;
      }      
     
      /* check that the rebuild does not overrun the rebuild area... */
      /* ... after ... */ 
      if (((pRebuild->stop_block != -1) &&  (write_last>pRebuild->stop_block))
      /* ... or before. */
      ||  (write_first < pRebuild->start_block)) {
	storio_rebuild_ctx_free(pRebuild);
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;
	errno = EAGAIN;
	goto error;
      }
      
      /*
      ** Rebuild still on going. Update the time stamp
      ** and starting point of the on going rebuild
      */        
      pRebuild->rebuild_ts  = time(NULL);
      pRebuild->start_block = write_first;
      
      /*
      ** Forward the request to the disk thread.
      ** On the 1rzt write of a rebuild with relocate
      */
      
      if (pRebuild->relocate == RBS_TO_RELOCATE) {
        req_ctx_p->opcode = STORIO_DISK_THREAD_REBUILD_START;
      }
      goto send_to_disk_thread;   
    }
    
      
    /*
    ** THIS IS A WRITE WRITE FROM THE STORCLI.
    */ 

    /*
    ** No mapping context exist, so create one
    */
    if (dev_map_p==NULL) {
      dev_map_p = storio_device_mapping_insert (write_arg_p->cid, 
                                                write_arg_p->sid, 
						write_arg_p->fid);
      if (dev_map_p == NULL) { 
        goto error;// errno already set
      }
      goto send_to_disk_thread;
    } 

    /*
    ** A mapping context exist containing information synchronized
    ** with the disk header file content.
    */    
    if (storio_get_dev(dev_map_p,0) != ROZOFS_UNKNOWN_CHUNK) {
      /*
      ** This is not the same recycling counter, so let's recycle
      */
      if (!same_recycle_cpt) {
	/* 
	** This is an old file that is being recycled.
	** Let's clear the chunk distribution to force a header file update
	** and a data truncate.
	*/
        storio_free_dev_mapping(dev_map_p);
        dev_map_p->recycle_cpt =  rozofs_get_recycle_from_fid(write_arg_p->fid);
      }
    } 

    /*
    ** Check whether this write breaks a running rebuild
    */
    if (dev_map_p->storio_rebuild_ref.u64 == 0xFFFFFFFFFFFFFFFF ) {
      // No running rebuild
      goto send_to_disk_thread;
    }
    
    /*
    ** Check for compatibility with all the running rebuilds
    */    
    for (nb_rebuild=0; nb_rebuild < MAX_FID_PARALLEL_REBUILD; nb_rebuild++) {

      storio_rebuild_ref = dev_map_p->storio_rebuild_ref.u8[nb_rebuild];

      /* This context is free */
      if (storio_rebuild_ref == 0xFF) {
	continue;
      }

      /*
      ** Retrieve the rebuild context  
      */
      pRebuild = storio_rebuild_ctx_retrieve(storio_rebuild_ref, (char*)write_arg_p->fid);
      if (pRebuild == NULL) {
	/* This context is not allocated for this FID,
	** or not the same recycling counter */
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF; // break this rebuild
        continue;
      }

      /*
      ** Writing the area that the rebuild process is trying to rebuild
      ** breaks the rebuild.
      */
      if ((write_first > pRebuild->start_block) 
       && ((pRebuild->stop_block==-1)||(write_first <= pRebuild->stop_block))) {
        /* 
	** Writing within the rebuild area !
	** Let's shrink the rebuild area to the 1rst written block
	*/
	pRebuild->stop_block = write_first-1;    
      }  
      else if ((write_first <= pRebuild->start_block) && (write_last >= pRebuild->start_block)) {
        /* 
	** Writing at the beginning of the rebuild area !
	** it breaks the rebuild
	*/        
	storio_rebuild_ctx_free(pRebuild);
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF; // break this rebuild	
      }
    }         


send_to_disk_thread:
        
    /*
    ** If any request is already running, chain this request on the FID context
    */
    if (!storio_serialization_begin(dev_map_p,req_ctx_p)){
      goto out;
    }   

    if (storio_disk_thread_intf_send(dev_map_p, req_ctx_p, tic) == 0) {
      goto out;
    }  
    severe("storio_disk_thread_intf_send %s", strerror(errno));


error:    
    
    ret.status                 = SP_FAILURE;            
    ret.sp_write_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(write);

out:
    return;
}
/*
**___________________________________________________________
*/

//static char sp_optim[512];

void storage_check_readahead()
{
   
   if (current_storage_fd_cache_p == NULL) return;
   

   posix_fadvise(current_storage_fd_cache_p->fd,                      
                    current_storage_fd_cache_p->offset,
                    current_storage_fd_cache_p->len_read,
                    POSIX_FADV_WILLNEED);


   current_storage_fd_cache_p->len_read = 0;

}


/*
**___________________________________________________________
*/

void sp_read_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_read_ret_t ret;
    storio_device_mapping_t * dev_map_p = NULL;
    sp_read_arg_t           * read_arg_p = (sp_read_arg_t *) pt;
    int                       same_recycle_cpt;
    
    START_PROFILING(read);
            
    /*
    ** Use received buffer for the response
    */
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;
 
    /*
    ** Set the position where the data has to be written in the xmit buffer 
    */
    req_ctx_p->position = storage_get_position_of_first_byte2write_from_read_req();

    /*
    ** Lookup for the cid/sid/FID in the lookup table. 
    ** Do not update the entry, but tell whether the reycling counter has the same
    ** value in the context and in the request 
    */ 
    dev_map_p = storio_device_mapping_search(read_arg_p->cid,
                                             read_arg_p->sid,
					     read_arg_p->fid,
					     &same_recycle_cpt);
    /*
    ** A mapping context exist containing information synchronized
    ** with the disk header file content.
    */    
    if ((dev_map_p) && (storio_get_dev(dev_map_p,0) != ROZOFS_UNKNOWN_CHUNK)) {
      /*
      ** This is not the same recycling counter, 
      ** so the requested FID does not exist.
      */
      if (!same_recycle_cpt) {
        errno = ENOENT;
	goto error;
      }
    }
    
    /*
    ** No mapping context exist, so create one
    */
    if (dev_map_p == NULL) { 
      dev_map_p = storio_device_mapping_insert (read_arg_p->cid, 
                                                read_arg_p->sid, 
						read_arg_p->fid);
      if (dev_map_p == NULL) { 
        errno = ENOMEM;
        goto error;
      }
    }  
    
    /*
    ** Case of the resize procedure
    */
    if ((read_arg_p->bid==0) && (read_arg_p->nb_proj==0)) {
      req_ctx_p->opcode = STORIO_DISK_THREAD_RESIZE;
    }
    else { 
      req_ctx_p->opcode = STORIO_DISK_THREAD_READ;
    }
        
    /*
    ** If any request is already running, chain this request on the FID context
    */
    if (!storio_serialization_begin(dev_map_p,req_ctx_p)){
      goto out;
    }  
    
    if (storio_disk_thread_intf_send(dev_map_p, req_ctx_p, tic) == 0) {
      goto out;
    }
    severe("storio_disk_thread_intf_send %s", strerror(errno));
    
error:
    ret.status                = SP_FAILURE;            
    ret.sp_read_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(read);

out:
    return;    
}


/*
**___________________________________________________________
*/
/**
*  READ with RDMA support
*/
void sp_read_rdma_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_read_ret_t ret;
    storio_device_mapping_t * dev_map_p = NULL;
    sp_read_rdma_arg_t           * read_arg_p = (sp_read_rdma_arg_t *) pt;
    int                       same_recycle_cpt;
    
    START_PROFILING(read);
            
    /*
    ** Use received buffer for the response
    */
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;
 
    /*
    ** Set the position where the data has to be written in the xmit buffer 
    */
    req_ctx_p->position = storage_get_position_of_first_byte2write_from_read_req();

    /*
    ** Lookup for the cid/sid/FID in the lookup table. 
    ** Do not update the entry, but tell whether the reycling counter has the same
    ** value in the context and in the request 
    */ 
    dev_map_p = storio_device_mapping_search(read_arg_p->cid,
                                             read_arg_p->sid,
					     read_arg_p->fid,
					     &same_recycle_cpt);
    /*
    ** A mapping context exist containing information synchronized
    ** with the disk header file content.
    */    
    if ((dev_map_p) && (storio_get_dev(dev_map_p,0) != ROZOFS_UNKNOWN_CHUNK)) {
      /*
      ** This is not the same recycling counter, 
      ** so the requested FID does not exist.
      */
      if (!same_recycle_cpt) {
        errno = ENOENT;
	goto error;
      }
    }
    
    /*
    ** No mapping context exist, so create one
    */
    if (dev_map_p == NULL) { 
      dev_map_p = storio_device_mapping_insert (read_arg_p->cid, 
                                                read_arg_p->sid, 
						read_arg_p->fid);
      if (dev_map_p == NULL) { 
        errno = ENOMEM;
        goto error;
      }
    }  
    req_ctx_p->opcode = STORIO_DISK_THREAD_READ_RDMA;
        
    /*
    ** If any request is already running, chain this request on the FID context
    */
    if (!storio_serialization_begin(dev_map_p,req_ctx_p)){
      goto out;
    }  
    
    if (storio_disk_thread_intf_send(dev_map_p, req_ctx_p, tic) == 0) {
      goto out;
    }
    severe("storio_disk_thread_intf_send %s", strerror(errno));
    
error:
    ret.status                = SP_FAILURE;            
    ret.sp_read_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(read);

out:
    return;    
}

/*
**___________________________________________________________
*/
/**
*  WRITE with RDMA support
*/
/*
**___________________________________________________________
*/

void sp_write_rdma_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_write_ret_t ret;
    storio_device_mapping_t * dev_map_p = NULL;
    sp_write_rdma_arg_t          * write_arg_p = (sp_write_rdma_arg_t *) pt;
    uint8_t                   nb_rebuild;
    uint8_t                   storio_rebuild_ref;
    STORIO_REBUILD_T        * pRebuild; 
    int                       same_recycle_cpt;
    int                       write_last;
    int                       write_first;
    
    START_PROFILING(write);

    /*
    ** need to swap the buffer when the read request has been received over RDMA
    */
    if (req_ctx_p->rdma)
    {
      /*
      ** allocate the buffer that might be used for reading or writing
      */
      req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
      req_ctx_p->recv_buf = ruc_buf_getBuffer(storage_receive_buffer_pool_p);                         
    } 
    else
    { 
      /*
      ** Use received buffer for the response
      */
      req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
      req_ctx_p->recv_buf = NULL;
    }

    req_ctx_p->opcode   = STORIO_DISK_THREAD_WRITE_RDMA;
  
    /*
    ** Rebuild reference is 0 when the write comes from the STORCLI.
    ** When the write comes from a rebuild process, this reference is 
    ** the index of the rebuild context whithin the FID context {1..4}
    */
    nb_rebuild = write_arg_p->rebuild_ref;
    if (nb_rebuild > MAX_FID_PARALLEL_REBUILD) {
      /* bad reference ?? */
      severe("Bad rebuild ref %d",nb_rebuild);
      errno = EAGAIN; // Break this rebuild
      goto error;	
    } 

    /*
    ** Lookup for the FID context in the lookup table. 
    */
    dev_map_p = storio_device_mapping_search(write_arg_p->cid, 
                                             write_arg_p->sid, 
					     write_arg_p->fid,
					     &same_recycle_cpt);


    write_first = write_arg_p->bid;
    write_last  = write_first + write_arg_p->nb_proj - 1;

    /*
    ** THIS IS A WRITE FROM A REBUILD PROCESS
    */
    if (nb_rebuild != 0) {
    
      /*
      ** No mapping context, so no such valid rebuild
      */
      if (dev_map_p == NULL) {
	errno = EAGAIN;  // Break this rebuild
	goto error;
      }   
          
      /*
      ** Retrieve the rebuild context from the rebuild index
      */
      nb_rebuild--;
      storio_rebuild_ref = dev_map_p->storio_rebuild_ref.u8[nb_rebuild];
      pRebuild = storio_rebuild_ctx_retrieve(storio_rebuild_ref, (char*)write_arg_p->fid);
      if (pRebuild == NULL) {
	/* This context is running for this FID */
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;
	errno = EAGAIN;
	goto error;
      }      
     
      /* check that the rebuild does not overrun the rebuild area... */
      /* ... after ... */ 
      if (((pRebuild->stop_block != -1) &&  (write_last>pRebuild->stop_block))
      /* ... or before. */
      ||  (write_first < pRebuild->start_block)) {
	storio_rebuild_ctx_free(pRebuild);
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;
	errno = EAGAIN;
	goto error;
      }
      
      /*
      ** Rebuild still on going. Update the time stamp
      ** and starting point of the on going rebuild
      */        
      pRebuild->rebuild_ts  = time(NULL);
      pRebuild->start_block = write_first;
      
      /*
      ** Forward the request to the disk thread.
      ** On the 1rzt write of a rebuild with relocate
      */
      
      if (pRebuild->relocate == RBS_TO_RELOCATE) {
        req_ctx_p->opcode = STORIO_DISK_THREAD_REBUILD_START;
      }
      goto send_to_disk_thread;   
    }
    
      
    /*
    ** THIS IS A WRITE WRITE FROM THE STORCLI.
    */ 

    /*
    ** No mapping context exist, so create one
    */
    if (dev_map_p==NULL) {
      dev_map_p = storio_device_mapping_insert (write_arg_p->cid, 
                                                write_arg_p->sid, 
						write_arg_p->fid);
      if (dev_map_p == NULL) { 
        goto error;// errno already set
      }
      goto send_to_disk_thread;
    } 

    /*
    ** A mapping context exist containing information synchronized
    ** with the disk header file content.
    */   
    if (storio_get_dev(dev_map_p,0) != ROZOFS_UNKNOWN_CHUNK) { 
      /*
      ** This is not the same recycling counter, so let's recycle
      */
      if (!same_recycle_cpt) {
	/* 
	** This is an old file that is being recycled.
	** Let's clear the chunk distribution to force a header file update
	** and a data truncate.
	*/
        storio_free_dev_mapping(dev_map_p);
	dev_map_p->recycle_cpt =  rozofs_get_recycle_from_fid(write_arg_p->fid);
      }
    } 

    /*
    ** Check whether this write breaks a running rebuild
    */
    if (dev_map_p->storio_rebuild_ref.u64 == 0xFFFFFFFFFFFFFFFF ) {
      // No running rebuild
      goto send_to_disk_thread;
    }
    
    /*
    ** Check for compatibility with all the running rebuilds
    */    
    for (nb_rebuild=0; nb_rebuild < MAX_FID_PARALLEL_REBUILD; nb_rebuild++) {

      storio_rebuild_ref = dev_map_p->storio_rebuild_ref.u8[nb_rebuild];

      /* This context is free */
      if (storio_rebuild_ref == 0xFF) {
	continue;
      }

      /*
      ** Retrieve the rebuild context  
      */
      pRebuild = storio_rebuild_ctx_retrieve(storio_rebuild_ref, (char*)write_arg_p->fid);
      if (pRebuild == NULL) {
	/* This context is not allocated for this FID,
	** or not the same recycling counter */
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF; // break this rebuild
        continue;
      }

      /*
      ** Writing the area that the rebuild process is trying to rebuild
      ** breaks the rebuild.
      */
      if ((write_first > pRebuild->start_block) 
       && ((pRebuild->stop_block==-1)||(write_first <= pRebuild->stop_block))) {
        /* 
	** Writing within the rebuild area !
	** Let's shrink the rebuild area to the 1rst written block
	*/
	pRebuild->stop_block = write_first-1;    
      }  
      else if ((write_first <= pRebuild->start_block) && (write_last >= pRebuild->start_block)) {
        /* 
	** Writing at the beginning of the rebuild area !
	** it breaks the rebuild
	*/        
	storio_rebuild_ctx_free(pRebuild);
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF; // break this rebuild	
      }
    }         


send_to_disk_thread:
        
    /*
    ** If any request is already running, chain this request on the FID context
    */
    if (!storio_serialization_begin(dev_map_p,req_ctx_p)){
      goto out;
    }   

    if (storio_disk_thread_intf_send(dev_map_p, req_ctx_p, tic) == 0) {
      goto out;
    }  
    severe("storio_disk_thread_intf_send %s", strerror(errno));


error:    
    
    ret.status                 = SP_FAILURE;            
    ret.sp_write_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(write);

out:
    return;
}
/*
**___________________________________________________________
*/
void sp_rebuild_start_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_rebuild_start_ret_t   ret;
    storio_device_mapping_t       * dev_map_p = NULL;
    sp_rebuild_start_arg_t        * rebuild_start_arg_p = (sp_rebuild_start_arg_t *) pt;
    uint32_t                        ts;    
    uint8_t                         nb_rebuild;
    int                             selected_index=-1;
    uint8_t                         storio_rebuild_ref;
    STORIO_REBUILD_T              * pRebuild;
    int                             same_recycle_cpt;
    
    START_PROFILING(rebuild_start);
                  
    /*
    ** Use received buffer for the response
    */
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;
    
    /*
    ** Lookup in the FID cache for the FID context
    */ 
    dev_map_p = storio_device_mapping_search(rebuild_start_arg_p->cid, 
                                             rebuild_start_arg_p->sid, 
					     rebuild_start_arg_p->fid,
					     &same_recycle_cpt);
    if (dev_map_p == NULL) { 
      /*
      ** Missing ! Create one entry
      */
      dev_map_p = storio_device_mapping_insert (rebuild_start_arg_p->cid, 
                                                rebuild_start_arg_p->sid, 
						rebuild_start_arg_p->fid);
      if (dev_map_p == NULL) { 
        errno = ENOMEM;      
        goto error;
      }
    }  
    
    if ((rebuild_start_arg_p->stop_bid != -1)
    &&  (rebuild_start_arg_p->stop_bid<rebuild_start_arg_p->start_bid)) {
      severe("Bad interval [%lu:%lu]",rebuild_start_arg_p->start_bid,rebuild_start_arg_p->stop_bid);
      errno = EINVAL;
      goto error;	  
    }
    /*
    ** Get current time in sec 
    */
    ts = time(NULL);

    /*
    ** Check for compatibility with the already running rebuilds
    */    
    for (nb_rebuild=0; nb_rebuild < MAX_FID_PARALLEL_REBUILD; nb_rebuild++) {

      storio_rebuild_ref = dev_map_p->storio_rebuild_ref.u8[nb_rebuild];

      /* This context is free */
      if (storio_rebuild_ref == 0xFF) {
        selected_index = nb_rebuild; // This entry can be used
	continue;
      }

      /*
      ** Retrieve the rebuild context from its index 
      */
      pRebuild = storio_rebuild_ctx_retrieve(storio_rebuild_ref, (char*)dev_map_p->key.fid);
      if (pRebuild == NULL) {
	/* This context is not allocated for this FID */
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF; // reset entry
	selected_index = nb_rebuild;                         // This entry can be used
        continue;
      }

      /*
      ** Check the time stamp
      */
      uint32_t delay;
      if (pRebuild->spare) delay = 5*60;
      else                 delay = 20;
      if ((ts - pRebuild->rebuild_ts) > delay) {
      
	/* The context is inactive for a too long time */
	storio_rebuild_ctx_aborted(pRebuild,ts - pRebuild->rebuild_ts);
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;  // reset entry
	selected_index = nb_rebuild;                          // This entry can be used
        continue;
      } 

      
      /*
      ** Check overlapping of the area to rebuild
      */
      if (pRebuild->stop_block==-1) {
        if (rebuild_start_arg_p->stop_bid==-1) {
	  errno = EAGAIN;
	  goto error;        
	}   
	if (rebuild_start_arg_p->stop_bid >= pRebuild->start_block) {
	  errno = EAGAIN;
	  goto error;        
	}  
      }
      else if (rebuild_start_arg_p->stop_bid==-1) {
	if (rebuild_start_arg_p->start_bid  >= pRebuild->stop_block) {
	  errno = EAGAIN;
	  goto error;  	  
	}               
      }
      else if ((rebuild_start_arg_p->start_bid <= pRebuild->stop_block)
        &&  (rebuild_start_arg_p->stop_bid >=  pRebuild->start_block)) {
	  /* Those rebuilds overlap => refuse new rebuild */
	errno = EAGAIN;
	goto error;        
      }           
    }
        

    if (selected_index == -1) {
      /* All FID rebuild entries are active. Can not start a new rebuild on this FID */      
      errno = EAGAIN;
      goto error;
    }
      	          
    

    /* A free FID entry has been found, let's find a free storio rebuild context */
    pRebuild = storio_rebuild_ctx_allocate();
    if (pRebuild == NULL) {
      /* No free context */
      errno = EAGAIN;
      goto error;	  
    }

    /*
    ** Fill the rebuild reference and store request time
    */
    pRebuild->rebuild_ts    = ts;
    pRebuild->start_block   = rebuild_start_arg_p->start_bid;
    pRebuild->stop_block    = rebuild_start_arg_p->stop_bid;
    pRebuild->spare         = rebuild_start_arg_p->spare;
    memcpy(pRebuild->fid,rebuild_start_arg_p->fid,sizeof(fid_t));

    dev_map_p->storio_rebuild_ref.u8[selected_index] = pRebuild->ref;
        
    /*
    ** In case the same device is to be used, just send 
    ** the response back to the rebuilder
    */
    if (rebuild_start_arg_p->device == SP_SAME_DEVICE) {
      pRebuild->relocate = RBS_NO_RELOCATE;          
    }
    else {    
      pRebuild->relocate = RBS_TO_RELOCATE;          
    }

    ret.status                               = SP_SUCCESS;            
    ret.sp_rebuild_start_ret_t_u.rebuild_ref = selected_index+1;
    goto send_response;             
    	        
    
error:
    ret.status                         = SP_FAILURE;            
    ret.sp_rebuild_start_ret_t_u.error = errno;
    
send_response:    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(rebuild_start);    
 
    return;        
}
/*
**___________________________________________________________
*/
void sp_rebuild_stop_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_rebuild_stop_ret_t    ret;
    storio_device_mapping_t       * dev_map_p = NULL;
    sp_rebuild_stop_arg_t         * rebuild_stop_arg_p = (sp_rebuild_stop_arg_t *) pt;
    uint8_t                         nb_rebuild;
    uint8_t                         storio_rebuild_ref;
    STORIO_REBUILD_T              * pRebuild;
    int                             same_recycle_cpt;
       
    START_PROFILING(rebuild_stop);
            
    /*
    ** Use received buffer for the response
    */
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;

    /*
    ** Lookup for the FID context in the lookup table
    */ 
    dev_map_p = storio_device_mapping_search(rebuild_stop_arg_p->cid, 
                                             rebuild_stop_arg_p->sid, 
					     rebuild_stop_arg_p->fid,
					     &same_recycle_cpt);
    if (dev_map_p == NULL) { 
      /*
      ** The file is currently being rebuilt
      ** A FID context should exist !!!
      */
      errno = ENOENT;
      goto error;
    }  
    
    /*
    ** This is not the same FID recycling counter, as the file that has been read
    ** Better not touch the file
    */
    if (!same_recycle_cpt) {
      errno = EAGAIN;
      goto error;
    }
    
    /*
    ** Check for compatibility with the already running rebuilds
    */    
    nb_rebuild = rebuild_stop_arg_p->rebuild_ref;
    nb_rebuild--;
    if (nb_rebuild > MAX_FID_PARALLEL_REBUILD) {
      errno = EAGAIN;    
      goto error;
    }

    /* This context is free */
    storio_rebuild_ref = dev_map_p->storio_rebuild_ref.u8[nb_rebuild];
    if (storio_rebuild_ref == 0xFF) {
      errno = EAGAIN;    
      goto error;
    }
      
    /*
    ** Retrieve the rebuild context  
    */
    pRebuild = storio_rebuild_ctx_retrieve(storio_rebuild_ref, (char*)rebuild_stop_arg_p->fid);
    if (pRebuild == NULL) {
      errno = EAGAIN;          
      /* This context is not allocated for this FID */
      dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;
      goto error;
    }
	 
    /*
    ** In case no relocation has been done, or relocation has been done on the same
    ** device, just send back the response
    */	 
    if ((pRebuild->relocate != RBS_RELOCATED)
    ||  (pRebuild->old_device == storio_get_dev(dev_map_p,pRebuild->chunk))) {
    	           
      storio_rebuild_ctx_free (pRebuild);
      dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;    

      ret.status = SP_SUCCESS;            
      goto send_response;   
      
    }	 
	     	 
    /*
    ** When relocation was requested one has to delete
    ** either old or new data file
    */
    req_ctx_p->opcode = STORIO_DISK_THREAD_REBUILD_STOP;
    
    /*
    ** If any request is already running, chain this request on the FID context
    */
    if (!storio_serialization_begin(dev_map_p,req_ctx_p)){
      goto out;
    }  
    
    if (storio_disk_thread_intf_send(dev_map_p, req_ctx_p, tic) == 0) {
      goto out;
    }
    severe("storio_disk_thread_intf_send %s", strerror(errno));	  
    
error:
    ret.status                        = SP_FAILURE;            
    ret.sp_rebuild_stop_ret_t_u.error = errno;
    
send_response:    
    ret.sp_rebuild_stop_ret_t_u.rebuild_ref = rebuild_stop_arg_p->rebuild_ref;
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(rebuild_stop);

out:    
   return ;
}
/*
**___________________________________________________________
** Recevive stop response from disk thread
*/
void sp_rebuild_stop_response(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    storio_device_mapping_t       * dev_map_p = (storio_device_mapping_t*) pt;
    sp_rebuild_stop_arg_t         * rebuild_stop_arg_p;
    uint8_t                         nb_rebuild;
    uint8_t                         storio_rebuild_ref;
    STORIO_REBUILD_T              * pRebuild;
            
    /*
    ** Check for compatibility with the already running rebuilds
    */    
    rebuild_stop_arg_p = (sp_rebuild_stop_arg_t*) ruc_buf_getPayload(req_ctx_p->decoded_arg);
    nb_rebuild = rebuild_stop_arg_p->rebuild_ref;
    nb_rebuild--;
    if (nb_rebuild > MAX_FID_PARALLEL_REBUILD) {
      severe("sp_rebuild_stop_response bad rebuild ref %d",nb_rebuild);
      return;
    }

    /* This context is free */
    storio_rebuild_ref = dev_map_p->storio_rebuild_ref.u8[nb_rebuild];
    if (storio_rebuild_ref == 0xFF) {
      severe("sp_rebuild_stop_response bad rebuild ctx index %d",storio_rebuild_ref);
      return;
    }
      
    /*
    ** Retrieve the rebuild context  
    */
    pRebuild = storio_rebuild_ctx_retrieve(storio_rebuild_ref, (char*)rebuild_stop_arg_p->fid);
    if (pRebuild == NULL) {
      /* This context is not allocated for this FID */
      dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;
      severe("sp_rebuild_stop_response stollen rebuild ctx %d",storio_rebuild_ref);
      return;
    }
	   
    storio_rebuild_ctx_free (pRebuild);
    dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;    
    return ;
}
/*
**___________________________________________________________
*/

void sp_truncate_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_status_ret_t ret;
    storio_device_mapping_t * dev_map_p = NULL;
    sp_truncate_arg_t       * truncate_arg_p = (sp_truncate_arg_t *) pt;
    uint8_t                         nb_rebuild;
    uint8_t                         storio_rebuild_ref;
    STORIO_REBUILD_T              * pRebuild; 
    int                             same_recycle_cpt;

    START_PROFILING(truncate);
    
    /*
    ** Use received buffer for the response
    */
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;


    /*
    ** Lookup for the FID context in the lookup table. 
    */
    dev_map_p = storio_device_mapping_search(truncate_arg_p->cid, 
                                             truncate_arg_p->sid, 
					     truncate_arg_p->fid,
					     &same_recycle_cpt);

    /*
    ** No mapping context exist, so create one
    */    
    if (dev_map_p == NULL) { 
      dev_map_p = storio_device_mapping_insert(truncate_arg_p->cid, 
                                               truncate_arg_p->sid, 
					       truncate_arg_p->fid);
      if (dev_map_p == NULL) { 
        goto error;
      }
    } 


    /*
    ** All rebuild processes are broken by this request
    */   
    if (dev_map_p->storio_rebuild_ref.u64 != 0xFFFFFFFFFFFFFFFF) {
      for (nb_rebuild=0; nb_rebuild < MAX_FID_PARALLEL_REBUILD; nb_rebuild++) {

	/* This context is free */
	storio_rebuild_ref = dev_map_p->storio_rebuild_ref.u8[nb_rebuild];
	if (storio_rebuild_ref == 0xFF) {
	  continue;
	}
      
	/*
	** Retrieve the rebuild context  
	*/
	pRebuild = storio_rebuild_ctx_retrieve(storio_rebuild_ref, (char*)truncate_arg_p->fid);
	if (pRebuild == NULL) {
	  /* This context is not allocated for this FID */
	  dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;
          continue;
	}

        storio_rebuild_ctx_free (pRebuild);
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;    
      }
    } 

  
    req_ctx_p->opcode = STORIO_DISK_THREAD_TRUNCATE;        
        
    /*
    ** If any request is already running, chain this request on the FID context
    */
    if (!storio_serialization_begin(dev_map_p,req_ctx_p)){
      goto out;
    }  
    
    
    if (storio_disk_thread_intf_send(dev_map_p, req_ctx_p, tic) == 0) {
      goto out;
    }      
    severe("storio_disk_thread_intf_send %s", strerror(errno));

error:    
    severe("sp_truncate_1_svc_disk_thread storio_disk_thread_intf_send %s", strerror(errno));
    
    ret.status                  = SP_FAILURE;            
    ret.sp_status_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(truncate);
    
out:
    return ;
}


/*
**___________________________________________________________
** File remove request from geo-replication client to update
** remote site in case of a file deletion. This is a definitive
** file deletion; no recycling of the FID can occur.
*/
void sp_remove_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_status_ret_t ret;
    sp_remove_arg_t         * remove_arg_p = (sp_remove_arg_t *) pt;
    storio_device_mapping_t * dev_map_p = NULL;
    uint8_t                   nb_rebuild;
    uint8_t                   storio_rebuild_ref;
    STORIO_REBUILD_T        * pRebuild; 
    int                       same_recycle_cpt;
    
    START_PROFILING(remove);
    
    /*
    ** Use received buffer for the response
    */
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;

    dev_map_p = storio_device_mapping_search(remove_arg_p->cid, 
                                             remove_arg_p->sid, 
					     remove_arg_p->fid,
					     &same_recycle_cpt);

    if (dev_map_p == NULL) { 
      dev_map_p = storio_device_mapping_insert (remove_arg_p->cid, 
                                                remove_arg_p->sid, 
						remove_arg_p->fid);
      if (dev_map_p == NULL) { 
        goto error;
      }
    } 
         
    /*
    ** All rebuild process are broken by this request
    */   
    if (dev_map_p->storio_rebuild_ref.u64 != 0xFFFFFFFFFFFFFFFF ) {
      for (nb_rebuild=0; nb_rebuild < MAX_FID_PARALLEL_REBUILD; nb_rebuild++) {

	/* This context is free */
	storio_rebuild_ref = dev_map_p->storio_rebuild_ref.u8[nb_rebuild];
	if (storio_rebuild_ref == 0xFF) {
	  continue;
	}
      
	/*
	** Retrieve the rebuild context  
	*/
	pRebuild = storio_rebuild_ctx_retrieve(storio_rebuild_ref, (char*)remove_arg_p->fid);
	if (pRebuild == NULL) {
	  /* This context is not allocated for this FID */
	  dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;
          continue;
	}

        storio_rebuild_ctx_free (pRebuild);
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;    
      }
    } 

        
    req_ctx_p->opcode = STORIO_DISK_THREAD_REMOVE;
        
    /*
    ** If any request is already running, chain this request on the FID context
    */
    if (!storio_serialization_begin(dev_map_p,req_ctx_p)){
      goto out;
    }  
    
    if (storio_disk_thread_intf_send(dev_map_p, req_ctx_p, tic) == 0) {
      goto out;
    }        
    severe("storio_disk_thread_intf_send %s", strerror(errno));

    
error:    
    severe("sp_remove_1_svc_disk_thread storio_disk_thread_intf_send %s", strerror(errno));
    
    ret.status                  = SP_FAILURE;            
    ret.sp_status_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(remove);

out:
    return ;
}
/*
**___________________________________________________________
*/

void sp_remove_chunk_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_status_ret_t ret;
    sp_remove_chunk_arg_t   * remove_chunk_arg_p = (sp_remove_chunk_arg_t *) pt;
    storio_device_mapping_t * dev_map_p = NULL;
    uint8_t                   nb_rebuild=0;
    uint8_t                   storio_rebuild_ref=0;
    STORIO_REBUILD_T        * pRebuild; 
    int                       same_recycle_cpt;                    
    
    START_PROFILING(remove_chunk);
    
    /*
    ** Use received buffer for the response
    */    
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;

    /*
    ** Lookup for the FID context in the lookup table
    */ 
    dev_map_p = storio_device_mapping_search(remove_chunk_arg_p->cid, 
                                             remove_chunk_arg_p->sid, 
					     remove_chunk_arg_p->fid,
					     &same_recycle_cpt);
    if (dev_map_p == NULL) { 
      /*
      ** The file is supposed to be in rebuild !!!
      */
      errno = ENOENT;
      goto error;
    }  

    /*
    ** Bad rebuild ref
    */   
    nb_rebuild = remove_chunk_arg_p->rebuild_ref;
    nb_rebuild--;
    if (nb_rebuild >= MAX_FID_PARALLEL_REBUILD) {
      errno = EAGAIN;
      goto error;
    }  
      
    storio_rebuild_ref = dev_map_p->storio_rebuild_ref.u8[nb_rebuild];        
    /* This context is free */
    if (storio_rebuild_ref == 0xFF) {
      errno = EAGAIN;
      goto error;
    }  
    
    /*
    ** Retrieve the rebuild context  
    */
    pRebuild = storio_rebuild_ctx_retrieve(storio_rebuild_ref, (char*)remove_chunk_arg_p->fid);
    if (pRebuild == NULL) {
      /* This context is not allocated for this FID */
      dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;
      errno = EAGAIN;
      goto error;
    }  
        
    req_ctx_p->opcode = STORIO_DISK_THREAD_REMOVE_CHUNK;
        
    /*
    ** If any request is already running, chain this request on the FID context
    */
    if (!storio_serialization_begin(dev_map_p,req_ctx_p)){
      goto out;
    }  
    
    if (storio_disk_thread_intf_send(dev_map_p, req_ctx_p, tic) == 0) {
      goto out;
    }        
    severe("storio_disk_thread_intf_send %s", strerror(errno));

    
error:    
    severe("sp_remove_chunk_1_svc_disk_thread %d %d %s", nb_rebuild, storio_rebuild_ref, strerror(errno));
    
    ret.status                  = SP_FAILURE;            
    ret.sp_status_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(remove);

out:
    return ;
}

/*
**___________________________________________________________
*/

void sp_write_repair3_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_write_ret_t ret;
    storio_device_mapping_t * dev_map_p = NULL;
    sp_write_repair3_arg_no_bins_t * repair3_arg_p = (sp_write_repair3_arg_no_bins_t *) pt;
    int                       same_recycle_cpt;                    
    
    START_PROFILING(repair);

    /*
    ** Use received buffer for the response
    */
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;
  
    /*
    ** Lookup for the FID context in the lookup table
    */ 
    dev_map_p = storio_device_mapping_search(repair3_arg_p->cid,
                                             repair3_arg_p->sid,
					     repair3_arg_p->fid,
					     &same_recycle_cpt);
    if (dev_map_p == NULL) { 
      /*
      ** The file is supposed to have just been read, and a CRC32 error has been detected
      ** that the STORCLI want to fix. A FID context should exist !!!
      */
      errno = ENOENT;
      goto error;
    }  
    
    /*
    ** This is not the same FID recycling counter, as the file that has been read
    ** Better not modify the file
    */
    if (!same_recycle_cpt) {
      errno = EAGAIN;
      goto error;
    }
    
    /*
    ** This write repairs the file, and so can not break any rebuild process
    ** that repairs the file too.
    */
            
    req_ctx_p->opcode = STORIO_DISK_THREAD_WRITE_REPAIR3;
        
    /*
    ** If any request is already running, chain this request on the FID context
    */
    if (!storio_serialization_begin(dev_map_p,req_ctx_p)){
      goto out;
    }   

    if (storio_disk_thread_intf_send(dev_map_p, req_ctx_p, tic) == 0) {
      goto out;
    }  
    severe("storio_disk_thread_intf_send %s", strerror(errno));


error:    
    
    ret.status                = SP_FAILURE;            
    ret.sp_write_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(repair);

out:
    return;
}

/*
**___________________________________________________________
*/
void sp_clear_error_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_status_ret_t    ret;
    sp_clear_error_arg_t    * clear_error_arg_p = (sp_clear_error_arg_t *) pt;
    storage_t               * st = 0;
    int                       idx;
    uint8_t                   action;
      
    START_PROFILING(clear_error);
    
    /*
    ** Use received buffer for the response
    */    
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;

    /*
    ** Only reset error counters, or re initializae device status automaton
    */
    if (clear_error_arg_p->reinit) {
      action = STORAGE_DEVICE_REINIT;
    }
    else {
      action = STORAGE_DEVICE_RESET_ERRORS;
    }   

    /*
    ** Retrieve the storage context 
    */
    if ((st = storaged_lookup(clear_error_arg_p->cid, clear_error_arg_p->sid)) == 0) {
      severe("sp_clear_error_1_svc_disk_thread cid %d sid %d", 
	        clear_error_arg_p->cid, clear_error_arg_p->sid);    
      ret.sp_status_ret_t_u.error = errno;
      goto error;
    }  

    /*
    ** On reinit, for re-enumeration
    */
    re_enumration_required = 1;
    
    /*
    ** All devices are to be processd
    */
    if (clear_error_arg_p->dev == 0xFF) {
      for (idx=0; idx< st->device_number;idx++) {
        if (st->device_ctx[idx].action < action) {
	  st->device_ctx[idx].action = action;
	}  
      }
    }
    /*
    ** Just one device
    */
    else {
      idx = clear_error_arg_p->dev;
      if (idx >= st->device_number) {
        severe("sp_clear_error_1_svc_disk_thread cid %d sid %d dev %d max %d", 
	        clear_error_arg_p->cid, clear_error_arg_p->sid,
		idx, st->device_number);
        goto error;
      }
      if (st->device_ctx[idx].action < action) {
	st->device_ctx[idx].action = action;
      }  
    }
    
    ret.status = SP_SUCCESS;
    goto out;
    
error:    
    
    ret.status                  = SP_FAILURE;            
    ret.sp_status_ret_t_u.error = errno;

out:    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);

    STOP_PROFILING(clear_error);
    return ;
}




/*
**___________________________________________________________
*/

typedef struct __rozofs_rdma_tcp_assoc_t
{
    uint16_t cli_ref;  /**< reference of the client : local index of the TCP connection  */
    uint16_t srv_ref;  /**< reference of the serveur: local index of the TCP connection   */
    uint32_t ip_cli;   /**< client IP address   */   
    uint32_t ip_srv;   /**< server IP address   */
    uint16_t port_cli; /**< client port         */
    uint16_t port_srv; /**< server port         */
    uint64_t cli_ts;   /**< timestamp of the client */
} FDL_rozofs_rdma_tcp_assoc_t;

void sp_rdma_setup(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_rdma_setup_ret_t    ret;
    sp_rdma_setup_arg_t    * sp_rdma_setup_arg_p = (sp_rdma_setup_arg_t *) pt;
    FDL_rozofs_rdma_tcp_assoc_t *assoc_p;

      
//    START_PROFILING(clear_error);
    

    /*
    ** Use received buffer for the response
    */    
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;

    /*
    ** Check if the server supports the RDMA
    */
    /*
    ** copy the RDMA key
    */
    memcpy(&ret.sp_rdma_setup_ret_t_u.rsp,sp_rdma_setup_arg_p,sizeof(sp_rdma_setup_arg_t));
    assoc_p = (FDL_rozofs_rdma_tcp_assoc_t*)&ret.sp_rdma_setup_ret_t_u.rsp;
    /*
    ** set the client reference
    */
    assoc_p->srv_ref = req_ctx_p->socketRef;
    ret.status = SP_SUCCESS;
    goto out;
    
/*
error:    
    
    ret.status                  = SP_FAILURE;            
    ret.sp_rdma_setup_ret_t_u.error = errno;
*/
out:    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);

//    STOP_PROFILING(clear_error);
    return ;
}


/*
**________________________________________________________________________

     S T A N D A L O N E   M O D E 
**________________________________________________________________________
*/

/*
**________________________________________________________________________
*/
/**
*  That service is intended to be called by the storcli when RozoFS operates
   in standalone mode
   
   The storcli uses that API to provide the information relative to the share
   memory that contains the buffers that it uses for readling and writing from/to the storio
   
   The storcli is intended to provide the key of the shared memory, the number of buffers and 
   the size of each buffer
   
   On the storio side, one register this information along with the TCP connection context
   That operation takes place only if the rozofs.conf file indicates that the standalone mode
   is supported.
*/
   
void sp_standalone_setup(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_status_ret_t    ret;
    sp_standalone_setup_arg_t *arg_p = (sp_standalone_setup_arg_t *) pt;
    rozofs_storio_share_mem_ctx_t *sharemem_p=NULL;

      
//    START_PROFILING(clear_error);
    

    /*
    ** Use received buffer for the response
    */    
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;

#warning check that the remote IP@ is local  otherwise reject the service
#if 0
    /*
    ** Check if the standalone mode is configure
    */
    if (common_config.standalone == 0)
    {
       errno = ENOTSUP;
       goto error;
    }
#endif
    /*
    ** allocate a context to store the reference of the shared memory
    */
    sharemem_p = malloc(sizeof(rozofs_storio_share_mem_ctx_t));
    if (sharemem_p == NULL)
    {
      errno = ENOMEM;
      goto error;
    }
    memset(sharemem_p,0,sizeof(rozofs_storio_share_mem_ctx_t));
    sharemem_p->key_stdalone = ROZOFS_STORIO_STANDALONE_KEY;
    sharemem_p->sharemem_key = arg_p->sharemem_key;
    sharemem_p->bufcount     = arg_p->bufcount;
    sharemem_p->bufsize      = arg_p->bufsize;
    /*
    ** do all the stuff to get an address for that shared memory
    */
    sharemem_p->shmid = shmget(sharemem_p->sharemem_key, 
                        	sharemem_p->bufcount*sharemem_p->bufsize, 
                        	0666);
    if (sharemem_p->shmid < 0) goto error;
    
     sharemem_p->addr_p = shmat(sharemem_p->shmid, NULL, 0);
     if (sharemem_p->addr_p == (char *) -1) goto error;
     
    /*
    ** store the reference of the shared memory in the userRef of the TCP connection.
    */
    if (af_inet_set_connection_user_ref(req_ctx_p->socketRef,sharemem_p) < 0)
    {
       shmdt((const void*)sharemem_p->addr_p);
       goto error;
    }
    ret.status = SP_SUCCESS;
    goto out;
    

error:        
    ret.status                  = SP_FAILURE;            
    ret.sp_status_ret_t_u.error = errno;
    if (sharemem_p != NULL) free(sharemem_p);
    
out:    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);

//    STOP_PROFILING(clear_error);
    return ;
}

     
/*
**___________________________________________________________
*/
/**
*  READ in standalone mode
*/
void sp_read_standalone_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_read_ret_t ret;
    storio_device_mapping_t * dev_map_p = NULL;
    sp_read_standalone_arg_t           * read_arg_p = (sp_read_standalone_arg_t *) pt;
    int                       same_recycle_cpt;
    
    START_PROFILING(read);
            
    /*
    ** Use received buffer for the response
    */
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;
 
    /*
    ** Set the position where the data has to be written in the xmit buffer 
    */
    req_ctx_p->position = storage_get_position_of_first_byte2write_from_read_req();

    /*
    ** Lookup for the cid/sid/FID in the lookup table. 
    ** Do not update the entry, but tell whether the reycling counter has the same
    ** value in the context and in the request 
    */ 
    dev_map_p = storio_device_mapping_search(read_arg_p->cid,
                                             read_arg_p->sid,
					     read_arg_p->fid,
					     &same_recycle_cpt);
    /*
    ** A mapping context exist containing information synchronized
    ** with the disk header file content.
    */    
    if ((dev_map_p) && (storio_get_dev(dev_map_p,0) != ROZOFS_UNKNOWN_CHUNK)) {
      /*
      ** This is not the same recycling counter, 
      ** so the requested FID does not exist.
      */
      if (!same_recycle_cpt) {
        errno = ENOENT;
	goto error;
      }
    }
    
    /*
    ** No mapping context exist, so create one
    */
    if (dev_map_p == NULL) { 
      dev_map_p = storio_device_mapping_insert (read_arg_p->cid, 
                                                read_arg_p->sid, 
						read_arg_p->fid);
      if (dev_map_p == NULL) { 
        errno = ENOMEM;
        goto error;
      }
    }  
    req_ctx_p->opcode = STORIO_DISK_THREAD_READ_STDALONE;
        
    /*
    ** If any request is already running, chain this request on the FID context
    */
    if (!storio_serialization_begin(dev_map_p,req_ctx_p)){
      goto out;
    }  
    
    if (storio_disk_thread_intf_send(dev_map_p, req_ctx_p, tic) == 0) {
      goto out;
    }
    severe("storio_disk_thread_intf_send %s", strerror(errno));
    
error:
    ret.status                = SP_FAILURE;            
    ret.sp_read_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(read);

out:
    return;    
}


/*
**___________________________________________________________
*/
/**
*  WRITE with standalone mode
*/
/*
**___________________________________________________________
*/

void sp_write_standalone_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_write_ret_t ret;
    storio_device_mapping_t * dev_map_p = NULL;
    sp_write_standalone_arg_t          * write_arg_p = (sp_write_standalone_arg_t *) pt;
    uint8_t                   nb_rebuild;
    uint8_t                   storio_rebuild_ref;
    STORIO_REBUILD_T        * pRebuild; 
    int                       same_recycle_cpt;
    int                       write_last;
    int                       write_first;
    
    START_PROFILING(write);

    /*
    ** Use received buffer for the response
    */
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;
    req_ctx_p->opcode   = STORIO_DISK_THREAD_WRITE_STDALONE;
  
    /*
    ** Rebuild reference is 0 when the write comes from the STORCLI.
    ** When the write comes from a rebuild process, this reference is 
    ** the index of the rebuild context whithin the FID context {1..4}
    */
    nb_rebuild = write_arg_p->rebuild_ref;
    if (nb_rebuild > MAX_FID_PARALLEL_REBUILD) {
      /* bad reference ?? */
      severe("Bad rebuild ref %d",nb_rebuild);
      errno = EAGAIN; // Break this rebuild
      goto error;	
    } 

    /*
    ** Lookup for the FID context in the lookup table. 
    */
    dev_map_p = storio_device_mapping_search(write_arg_p->cid, 
                                             write_arg_p->sid, 
					     write_arg_p->fid,
					     &same_recycle_cpt);


    write_first = write_arg_p->bid;
    write_last  = write_first + write_arg_p->nb_proj - 1;

    /*
    ** THIS IS A WRITE FROM A REBUILD PROCESS
    */
    if (nb_rebuild != 0) {
    
      /*
      ** No mapping context, so no such valid rebuild
      */
      if (dev_map_p == NULL) {
	errno = EAGAIN;  // Break this rebuild
	goto error;
      }   
          
      /*
      ** Retrieve the rebuild context from the rebuild index
      */
      nb_rebuild--;
      storio_rebuild_ref = dev_map_p->storio_rebuild_ref.u8[nb_rebuild];
      pRebuild = storio_rebuild_ctx_retrieve(storio_rebuild_ref, (char*)write_arg_p->fid);
      if (pRebuild == NULL) {
	/* This context is running for this FID */
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;
	errno = EAGAIN;
	goto error;
      }      
     
      /* check that the rebuild does not overrun the rebuild area... */
      /* ... after ... */ 
      if (((pRebuild->stop_block != -1) &&  (write_last>pRebuild->stop_block))
      /* ... or before. */
      ||  (write_first < pRebuild->start_block)) {
	storio_rebuild_ctx_free(pRebuild);
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF;
	errno = EAGAIN;
	goto error;
      }
      
      /*
      ** Rebuild still on going. Update the time stamp
      ** and starting point of the on going rebuild
      */        
      pRebuild->rebuild_ts  = time(NULL);
      pRebuild->start_block = write_first;
      
      /*
      ** Forward the request to the disk thread.
      ** On the 1rzt write of a rebuild with relocate
      */
      
      if (pRebuild->relocate == RBS_TO_RELOCATE) {
        req_ctx_p->opcode = STORIO_DISK_THREAD_REBUILD_START;
      }
      goto send_to_disk_thread;   
    }
    
      
    /*
    ** THIS IS A WRITE WRITE FROM THE STORCLI.
    */ 

    /*
    ** No mapping context exist, so create one
    */
    if (dev_map_p==NULL) {
      dev_map_p = storio_device_mapping_insert (write_arg_p->cid, 
                                                write_arg_p->sid, 
						write_arg_p->fid);
      if (dev_map_p == NULL) { 
        goto error;// errno already set
      }
      goto send_to_disk_thread;
    } 

    /*
    ** A mapping context exist containing information synchronized
    ** with the disk header file content.
    */   
    if (storio_get_dev(dev_map_p,0) != ROZOFS_UNKNOWN_CHUNK) { 
      /*
      ** This is not the same recycling counter, so let's recycle
      */
      if (!same_recycle_cpt) {
	/* 
	** This is an old file that is being recycled.
	** Let's clear the chunk distribution to force a header file update
	** and a data truncate.
	*/
        storio_free_dev_mapping(dev_map_p);
	dev_map_p->recycle_cpt =  rozofs_get_recycle_from_fid(write_arg_p->fid);
      }
    } 

    /*
    ** Check whether this write breaks a running rebuild
    */
    if (dev_map_p->storio_rebuild_ref.u64 == 0xFFFFFFFFFFFFFFFF ) {
      // No running rebuild
      goto send_to_disk_thread;
    }
    
    /*
    ** Check for compatibility with all the running rebuilds
    */    
    for (nb_rebuild=0; nb_rebuild < MAX_FID_PARALLEL_REBUILD; nb_rebuild++) {

      storio_rebuild_ref = dev_map_p->storio_rebuild_ref.u8[nb_rebuild];

      /* This context is free */
      if (storio_rebuild_ref == 0xFF) {
	continue;
      }

      /*
      ** Retrieve the rebuild context  
      */
      pRebuild = storio_rebuild_ctx_retrieve(storio_rebuild_ref, (char*)write_arg_p->fid);
      if (pRebuild == NULL) {
	/* This context is not allocated for this FID,
	** or not the same recycling counter */
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF; // break this rebuild
        continue;
      }

      /*
      ** Writing the area that the rebuild process is trying to rebuild
      ** breaks the rebuild.
      */
      if ((write_first > pRebuild->start_block) 
       && ((pRebuild->stop_block==-1)||(write_first <= pRebuild->stop_block))) {
        /* 
	** Writing within the rebuild area !
	** Let's shrink the rebuild area to the 1rst written block
	*/
	pRebuild->stop_block = write_first-1;    
      }  
      else if ((write_first <= pRebuild->start_block) && (write_last >= pRebuild->start_block)) {
        /* 
	** Writing at the beginning of the rebuild area !
	** it breaks the rebuild
	*/        
	storio_rebuild_ctx_free(pRebuild);
	dev_map_p->storio_rebuild_ref.u8[nb_rebuild] = 0xFF; // break this rebuild	
      }
    }         


send_to_disk_thread:
        
    /*
    ** If any request is already running, chain this request on the FID context
    */
    if (!storio_serialization_begin(dev_map_p,req_ctx_p)){
      goto out;
    }   

    if (storio_disk_thread_intf_send(dev_map_p, req_ctx_p, tic) == 0) {
      goto out;
    }  
    severe("storio_disk_thread_intf_send %s", strerror(errno));


error:    
    
    ret.status                 = SP_FAILURE;            
    ret.sp_write_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(write);

out:
    return;
}
