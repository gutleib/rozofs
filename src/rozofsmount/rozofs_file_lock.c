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

#include "rozofs_fuse_api.h"

extern uint64_t   rozofs_client_hash;
static ruc_obj_desc_t pending_lock_list;  
static int loop_count=0; 
extern struct fuse_chan *rozofsmount_fuse_chanel;

int rozofs_ll_setlk_internal(file_t * file);
void rozofs_ll_setlk_internal_cbk(void *this,void * param);

DECLARE_PROFILING(mpp_profiler_t);

typedef struct _LOCK_STATISTICS_T {
  uint64_t      bsd_set_passing_lock;
  uint64_t      bsd_set_blocking_lock;
  uint64_t      posix_set_passing_lock;
  uint64_t      posix_set_blocking_lock;
  uint64_t      posix_get_lock;
  uint64_t      flush_required;
  uint64_t      set_lock_refused;
  uint64_t      set_lock_success;  
  uint64_t      set_lock_error;
  uint64_t      set_lock_interrupted;  
  uint64_t      get_lock_refused;  
  uint64_t      get_lock_success; 
  uint64_t      get_lock_error;   
  uint64_t      enoent;
  uint64_t      ebadf;
  uint64_t      enomem;  
  uint64_t      einval;
  uint64_t      mandatory;
  uint64_t      flush_error;
  uint64_t      buf_flush;
  uint64_t      write_block_error;
  uint64_t      send_common;
} LOCK_STATISTICS_T;

static LOCK_STATISTICS_T lock_stat;

/*
**____________________________________________________
* reset lock statistics
*
*/
void reset_lock_stat(void) {
  memset((char*)&lock_stat,0,sizeof(lock_stat));
}

/*
**____________________________________________________
* Display lock statistics
*
*/
#define DISPLAY_LOCK_STAT(name) p += sprintf(p,"%-24s = %"PRIu64"\n",#name,lock_stat.name);
char * display_lock_stat(char * p) {
  DISPLAY_LOCK_STAT(bsd_set_passing_lock);
  DISPLAY_LOCK_STAT(bsd_set_blocking_lock);  
  DISPLAY_LOCK_STAT(posix_set_passing_lock);    
  DISPLAY_LOCK_STAT(posix_set_blocking_lock);
  DISPLAY_LOCK_STAT(set_lock_refused);
  DISPLAY_LOCK_STAT(set_lock_success);
  DISPLAY_LOCK_STAT(set_lock_error);
  DISPLAY_LOCK_STAT(set_lock_interrupted);
  DISPLAY_LOCK_STAT(posix_get_lock);  
  DISPLAY_LOCK_STAT(flush_required);  
  DISPLAY_LOCK_STAT(get_lock_refused);
  DISPLAY_LOCK_STAT(get_lock_success);
  DISPLAY_LOCK_STAT(get_lock_error);
  DISPLAY_LOCK_STAT(enoent);
  DISPLAY_LOCK_STAT(ebadf);
  DISPLAY_LOCK_STAT(enomem);
  DISPLAY_LOCK_STAT(einval);
  DISPLAY_LOCK_STAT(mandatory);
  DISPLAY_LOCK_STAT(flush_error);
  DISPLAY_LOCK_STAT(buf_flush);
  DISPLAY_LOCK_STAT(write_block_error);
  DISPLAY_LOCK_STAT(send_common);
  return p;
}
/*
*___________________________________________________________________
* Recompute the effective range of the lock from the user range
*___________________________________________________________________
*/
void compute_effective_lock_range(int bsize_e, struct ep_lock_t * lock) {  
  int bbytes = ROZOFS_BSIZE_BYTES(bsize_e);
  

  
  if (lock->user_range.size == EP_LOCK_TOTAL) {
    lock->effective_range.offset_start = 0;  
    lock->effective_range.offset_stop = 0;
   lock->effective_range.size = EP_LOCK_TOTAL;   
   return;
  }
    
  lock->effective_range.offset_start = lock->user_range.offset_start / bbytes;
  
  if (lock->user_range.size == EP_LOCK_TO_END) {
    lock->effective_range.offset_stop = 0;
    if (lock->effective_range.offset_start == 0) lock->effective_range.size = EP_LOCK_TOTAL;   
    else                                         lock->effective_range.size = EP_LOCK_TO_END;
    return;   
  }
  

  if (lock->user_range.offset_stop % bbytes == 0) {
    lock->effective_range.offset_stop = lock->user_range.offset_stop / bbytes;
  }
  else {
    lock->effective_range.offset_stop = lock->user_range.offset_stop / bbytes + 1;  
  }   

  if (lock->effective_range.offset_start == 0) {
    lock->effective_range.size = EP_LOCK_FROM_START;
  }
  else {
    lock->effective_range.size = EP_LOCK_PARTIAL;  
  }  
}
/**
**____________________________________________________
*  Compute flocks start and stop
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
ep_lock_size_t rozofs_flock_canonical(struct flock * lock, file_t * f, int64_t * start, int64_t * stop) {

  *stop = 0;
  ientry_t *ie;
  
  ie=f->ie;
  
  if  (lock->l_whence == SEEK_CUR) {
    *start = f->current_pos;
  }
  else if (lock->l_whence == SEEK_END)  {
    *start = ie->attrs.size;
  }  
  else  {
    *start = 0;
  }
  *start += lock->l_start;
  if (*start < 0) return EP_LOCK_NULL;

  if (lock->l_len < 0) {
    *stop = *start;
    *start += lock->l_len;
    if (*start < 0) return EP_LOCK_NULL;
  }
  else if (lock->l_len > 0) {
    *stop = *start + lock->l_len;
  }
  if (*stop == 0) {
    if (*start == 0) return EP_LOCK_TOTAL;
    return EP_LOCK_TO_END;
  }
  if (*start == 0) return EP_LOCK_FROM_START;
  return EP_LOCK_PARTIAL;
}
/**
**____________________________________________________
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */

void rozofs_poll_cbk(void *this,void *param) 
{
   void     *recv_buf = NULL;   
   int       status;

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


error:
  
    /*
    ** release the transaction context and the fuse context
    */
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);        
    return;
}
/*
**____________________________________________________
*/
/*
  
*/
char * rozofs_flock_service_display_lock_list(char * pChar) {
  file_t            * file;
  ruc_obj_desc_t    * link;
 
  /*
  ** Re attemp a file lock if any is pending 
  */
  link = ruc_objGetFirst(&pending_lock_list);
  if (link == NULL) {    
    pChar += sprintf(pChar,"no pending lock request\n");
    return pChar;
  }   
  
  file = (file_t *) ruc_listGetAssoc(link);
  pChar += sprintf(pChar,"1rst pending lock request:\n");
  pChar += sprintf(pChar,"  type  = %s\n", (file->lock_type==EP_LOCK_WRITE)?"WRITE":"READ");
  pChar += sprintf(pChar,"  owner = %llx\n", (long long unsigned int)file->lock_owner_ref);
  return pChar;
}  

/*
**____________________________________________________
*/
/*
  Periodicaly send a pool file lock to the export
*/
void rozofs_flock_service_periodic(void * ns) {
  epgw_lock_arg_t     arg;
  file_t            * file;
  ruc_obj_desc_t    * link, * linkNext;
  int                 nbTxCredits;
  fid_t               fake_fid;
 
  loop_count++;  
  memset(fake_fid,0,sizeof(fid_t));
  
  /*
  ** Check there is some Tx credits to start a transaction
  */
  nbTxCredits = rozofs_tx_get_free_ctx_number() - rozofs_fuse_get_free_ctx_number();
  if (nbTxCredits <= 0) return;
  
  /* Get one credit for the polling */
  nbTxCredits--;
  
  /* No more than 3 requests at a time */
  if (nbTxCredits > 10) nbTxCredits = 10;

  linkNext = NULL;
  while (((link = ruc_objGetNext(&pending_lock_list, &linkNext)) != NULL) && (nbTxCredits)) {

    file = (file_t *) ruc_listGetAssoc(link);

    /*
    ** Check that this entry is still valid
    */
    if ((file->chekWord != FILE_CHECK_WORD)
    ||  (file->fuse_req == NULL)
    ||  (file->lock_owner_ref == 0)) {
      lock_stat.set_lock_interrupted++; 
      ruc_objRemove(link);   
      continue;
    }
    
    /*
    ** Check whether it is time to process this request 
    */
    
    if (file->lock_delay > 0) { 
      file->lock_delay--; 
      continue;
    }        
    
    ruc_objRemove(link);       
    rozofs_ll_setlk_internal(file);
    nbTxCredits--;    
  }
  
  /* We have previously booked one credit to send the poll */
  if (loop_count < 600) return;
  
  /*
  ** Send the periodic poll message to the export in order to keep
  ** the lock active
  */
  loop_count = 0;
  arg.arg_gw.eid             = exportclt.eid;
  strncpy(arg.arg_gw.client_info.vers,VERSION,ROZOFS_VERSION_STRING_LENGTH);    
  arg.arg_gw.client_info.diag_port = rozofsmount_diag_port;    
  arg.arg_gw.lock.client_ref = rozofs_client_hash;
  /*
  ** now initiates the transaction towards the remote end
  */
#if 1
  rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)fake_fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_POLL_FILE_LOCK,(xdrproc_t) xdr_epgw_lock_arg_t,(void *)&arg,
                              rozofs_poll_cbk,NULL); 
#else
  rozofs_export_send_common(&exportclt,ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM),EXPORT_PROGRAM, EXPORT_VERSION,
                            EP_POLL_FILE_LOCK,(xdrproc_t) xdr_epgw_lock_arg_t,(void *)&arg, 
			    rozofs_poll_cbk,NULL); 
#endif
}

/**
*  Initialize the file lock service
 
*/
void rozofs_flock_service_init(void) {
  struct timer_cell * periodic_timer;

  reset_lock_stat();  
  
  /*
  ** Initialize the head of list of file waiting for a blocking lock 
  */ 
  ruc_listHdrInit(&pending_lock_list);

  /*
  ** Start a periodic timer every 100 ms
  */
  periodic_timer = ruc_timer_alloc(0,0);
  if (periodic_timer == NULL) {
    severe("no timer");
    return;
  }
  ruc_periodic_timer_start (periodic_timer, 50,rozofs_flock_service_periodic,0);
}
/**
*  File lock testing

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param parent : inode parent provided by rozofsmount
 @param name : name to search in the parent directory
 
 @retval none
*/
void rozofs_ll_getlk_cbk(void *this,void *param);

void rozofs_ll_getlk_nb(fuse_req_t req, 
                        fuse_ino_t ino, 
                        struct fuse_file_info *fi,
                        struct flock *flock) {
    ientry_t *ie = 0;
    epgw_lock_arg_t arg;
    int    ret;        
    void *buffer_p = NULL;
    int64_t start,stop;
    file_t      * file;

    int lock_trc_idx = 0;
    
    lock_stat.posix_get_lock++;
        
    /*
    ** allocate a context for saving the fuse parameters
    */
    buffer_p = rozofs_fuse_alloc_saved_context();
    if (buffer_p == NULL)
    {
      errno = ENOMEM;
      lock_stat.enomem++;      
      goto error;
    }
    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,ino);
    SAVE_FUSE_STRUCT(buffer_p,fi,sizeof(struct fuse_file_info));    
    SAVE_FUSE_STRUCT(buffer_p,flock,sizeof(struct flock));    
    
    START_PROFILING_NB(buffer_p,rozofs_ll_getlk);

    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
	lock_stat.enoent++;
        goto error;
    }
    
    file = (file_t *) (unsigned long) fi->fh;
        
    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid = exportclt.eid;
    strncpy(arg.arg_gw.client_info.vers,VERSION,ROZOFS_VERSION_STRING_LENGTH);    
    arg.arg_gw.client_info.diag_port = rozofsmount_diag_port;    
    memcpy(arg.arg_gw.fid, ie->fid, sizeof (uuid_t));
    switch(flock->l_type) {
      case F_RDLCK:
        arg.arg_gw.lock.mode = EP_LOCK_READ;
	break;
      case F_WRLCK:
        arg.arg_gw.lock.mode = EP_LOCK_WRITE;
	break;	
      case F_UNLCK:
        arg.arg_gw.lock.mode = EP_LOCK_FREE;
        break;
      default:
        errno= EINVAL;
	lock_stat.einval++;
        goto error;
    }

    lock_trc_idx = rozofs_trc_req_flock(srv_rozofs_ll_getlk,ino,file->fid, flock->l_start, flock->l_len, arg.arg_gw.lock.mode, 0);
    file->lock_trc_idx = lock_trc_idx; 
    
    arg.arg_gw.lock.client_ref   = rozofs_client_hash;
    arg.arg_gw.lock.owner_ref    = fi->lock_owner;
    arg.arg_gw.lock.user_range.size = rozofs_flock_canonical(flock,file, &start, &stop);
    if (arg.arg_gw.lock.user_range.size == EP_LOCK_NULL) {
      lock_stat.einval++;
      errno= EINVAL;
      goto error;
    }	
    arg.arg_gw.lock.user_range.offset_start = start;
    arg.arg_gw.lock.user_range.offset_stop  = stop;      
    compute_effective_lock_range(exportclt.bsize,&arg.arg_gw.lock);    			
    /*
    ** now initiates the transaction towards the remote end
    */
#if 1
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)file->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_GET_FILE_LOCK,(xdrproc_t) xdr_epgw_lock_arg_t,(void *)&arg,
                              rozofs_ll_getlk_cbk,buffer_p); 
#else
    ret = rozofs_export_send_common(&exportclt,ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM),EXPORT_PROGRAM, EXPORT_VERSION,
                                    EP_GET_FILE_LOCK,(xdrproc_t) xdr_epgw_lock_arg_t,(void *)&arg, 
			            rozofs_ll_getlk_cbk,buffer_p); 
			      
#endif
    if (ret < 0) {
      lock_stat.send_common++;    
      goto error;
    }
    /*
    ** no error just waiting for the answer
    */
    return;

error:
    fuse_reply_err(req, errno);
    if (lock_trc_idx) rozofs_trc_rsp(srv_rozofs_ll_getlk,ino,file->fid,1,lock_trc_idx);    

    /*
    ** release the buffer if has been allocated
    */
    STOP_PROFILING_NB(buffer_p,rozofs_ll_getlk);
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

void rozofs_ll_getlk_cbk(void *this,void *param) 
{
   struct flock * flock;
   fuse_req_t req; 
   epgw_lock_ret_t ret ;
   struct rpc_msg  rpc_reply;   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_lock_ret_t;
   fuse_ino_t ino;
   struct fuse_file_info finfo;
   struct fuse_file_info * fi = &finfo;
   file_t * file=NULL;
      
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_STRUCT_PTR(param,flock);
   RESTORE_FUSE_PARAM(param,ino);
   RESTORE_FUSE_STRUCT(param,fi,sizeof(struct fuse_file_info));
    
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
        
    if (ret.gw_status.status == EP_SUCCESS) {
      flock->l_type = F_UNLCK; 
      lock_stat.get_lock_success++;       
    }        
    else if (ret.gw_status.status == EP_EAGAIN) {
      switch (ret.gw_status.ep_lock_ret_t_u.lock.mode) {
	case EP_LOCK_READ: flock->l_type = F_RDLCK; break;
	case EP_LOCK_WRITE: flock->l_type = F_WRLCK; break;
        default: flock->l_type = F_UNLCK;
      }
      flock->l_whence = SEEK_SET;
      flock->l_start  = 0;
      flock->l_len    = 0;	
      flock->l_pid = ret.gw_status.ep_lock_ret_t_u.lock.owner_ref;
      lock_stat.get_lock_refused++;       
    }
    else {  
      errno = ret.gw_status.ep_lock_ret_t_u.error;
      xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
      goto error;
    }     
 
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
    fuse_reply_lock(req, flock);

    file = (file_t *) (unsigned long) fi->fh;
    if (file!= NULL) {       
      errno = 0;
      rozofs_trc_rsp(srv_rozofs_ll_getlk,ino,file->fid,0,file->lock_trc_idx);    
    } 
    goto out;
error:
    lock_stat.get_lock_error++;
    fuse_reply_err(req, errno);
 
    file = (file_t *) (unsigned long) fi->fh;
    if (file!= NULL) {       
      rozofs_trc_rsp(srv_rozofs_ll_getlk,ino,file->fid,1,file->lock_trc_idx);    
    }      
out:
    /*
    ** release the transaction context and the fuse context
    */
    STOP_PROFILING_NB(param,rozofs_ll_getlk);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    
    return;
}
/**
*  Put a lock request in the list of pending lock for the 1rst attempt
*
*
 @retval none
*/
int prepare_lock_for_pending_list(fuse_req_t              req,
                                  struct fuse_file_info * fi,
                                  struct flock          * flock,
                                  int                     sleep) {
  file_t                * file;
  int64_t                 start,stop;  
	
  		
  file = (file_t*) (unsigned long) fi->fh;
  if (file == NULL) return -1;
  
  file->lock_size = rozofs_flock_canonical(flock,file, &start, &stop);
  if (file->lock_size == EP_LOCK_NULL) return -1;
  			
  file->fuse_req        = req;
  file->lock_owner_ref  = fi->lock_owner;   
  file->lock_sleep      = sleep;
  file->lock_start      = start;
  file->lock_stop       = stop;
//  info("FDL lock start %llu end %llu ",(unsigned long long int)start,(unsigned long long int)stop);
    
  switch(flock->l_type) {
    case F_WRLCK: 
      file->lock_delay = 0;
      file->lock_type = EP_LOCK_WRITE;
      break;
    case F_RDLCK:
      file->lock_delay = 0;    
      file->lock_type = EP_LOCK_READ;
      break;
    default:   
      file->lock_delay = 1;    
      file->lock_type = EP_LOCK_FREE;
  }    
  return 0;
}
/**
*  BSD flock() File lock setting

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param parent : inode parent provided by rozofsmount
 @param name : name to search in the parent directory
 
 @retval none
*/
void rozofs_ll_flock_nb(fuse_req_t req, 
                              fuse_ino_t ino,
		              struct fuse_file_info *fi, 
		              int input_op) {
    int          sleep ;
    struct flock flock;
    int          op = input_op;


    /*
    ** Just keep significant bits
    ** and clear all other
    */
    op = op & (LOCK_NB|LOCK_SH|LOCK_EX|LOCK_UN);
    
    /*
    ** Blocking or not blocking ? 
    */
    if ((op & LOCK_NB) == LOCK_NB) {
      sleep = 0;
      op &= ~LOCK_NB;
      lock_stat.bsd_set_passing_lock++;
    }  
    else {
      sleep = 1;
      lock_stat.bsd_set_blocking_lock++;      
    }

    /*
    ** Translate operation
    */
    switch(op) {
      case LOCK_SH:
	flock.l_type = F_RDLCK;
	break;
      case LOCK_EX:
        flock.l_type = F_WRLCK;
	break;	
      case LOCK_UN:
        flock.l_type = LOCK_UN;
        break;
      default:
	lock_stat.einval++;      
        if (lock_stat.einval%(128*1024)==1) {
          severe("rozofs_ll_flock_nb op = 0x%x (count %llu)",
                 (unsigned int)input_op,
                 (unsigned long long)lock_stat.einval);
        }
        fuse_reply_err(req, EINVAL);
        return;
    }
    flock.l_whence = SEEK_SET;
    flock.l_start  = 0;
    flock.l_len    = 0;
    flock.l_pid    = fi->lock_owner; 
    
    if (sleep) lock_stat.posix_set_blocking_lock--;   
    else       lock_stat.posix_set_passing_lock--;    
    rozofs_ll_setlk_nb( req, ino, fi, &flock, sleep);

}
/**
*  File lock setting

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param parent : inode parent provided by rozofsmount
 @param name : name to search in the parent directory
 
 @retval none
*/
void rozofs_ll_setlk_after_flush(void *this,void *param);
void rozofs_ll_setlk_after_write_block(void *this,void *param) ;


char *print_whence(int whence)
{
    switch(whence) {
      case SEEK_SET:
        return "SEEK_SET";
	break;
      case SEEK_CUR:
        return "SEEK_CUR";
	break;	
      case SEEK_END:
        return "SEEK_END";
        break;
      default:
        break;
    }
        return "Unknown";
}

char *print_lock_type(int type)
{
    switch(type) {
      case F_RDLCK:
        return "EP_LOCK_READ";
	break;
      case F_WRLCK:
        return "EP_LOCK_WRITE";
	break;	
      case F_UNLCK:
        return "EP_LOCK_FREE";
        break;
      default:
        break;
    }
        return "Unknown";
}
void rozofs_ll_setlk_nb(fuse_req_t req, 
                        fuse_ino_t ino, 
                        struct fuse_file_info *fi,
                        struct flock *flock,
			int sleep) {
    ientry_t *ie = 0;
    int    ret;        
    void *buffer_p = NULL;
    file_t      * f = NULL;
    int      lock_trc_idx=0;
    
    
//    severe("FDL lock : type %s whence %s start %llu len %llu",print_lock_type(flock->l_type),print_whence(flock->l_whence),
//           (unsigned long long int)flock->l_start,flock->l_len);
    if (sleep) lock_stat.posix_set_blocking_lock++;   
    else       lock_stat.posix_set_passing_lock++; 


    /*
    ** allocate a context for saving the fuse parameters
    */
    buffer_p = rozofs_fuse_alloc_saved_context();
    if (buffer_p == NULL)
    {
      lock_stat.enomem++;   
      errno = ENOMEM;
      goto error;
    }
    START_PROFILING_NB(buffer_p,rozofs_ll_setlk);

    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,sleep);
    SAVE_FUSE_PARAM(buffer_p,ino);
    SAVE_FUSE_STRUCT(buffer_p,fi,sizeof(struct fuse_file_info));    
    SAVE_FUSE_STRUCT(buffer_p,flock,sizeof(struct flock));   

    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
	lock_stat.enoent++;
        goto error;
    }

    f = (file_t *) (unsigned long) fi->fh;
    if (f == NULL) {
        errno = EBADF;
	lock_stat.ebadf++;
        goto error;
    }
    
    if (flock->l_type & LOCK_MAND) {
        errno = EOPNOTSUPP;
	lock_stat.mandatory++;
        goto error;
    }
    
    if (prepare_lock_for_pending_list(req,fi,flock,sleep)<0) {
        errno = EINVAL;
	lock_stat.einval++;
        goto error;
    }

    lock_trc_idx = rozofs_trc_req_flock(srv_rozofs_ll_setlk,ino,f->fid, flock->l_start, flock->l_len, 
                                        f->lock_type, sleep);
    f->lock_trc_idx = lock_trc_idx; 
    
    /*
    ** Flush the buffer if some data is pending
    */
    if (f->buf_write_wait!= 0) {
      lock_stat.flush_required++;
      /*
      ** Install a callback to called after the flush si done
      */ 
      SAVE_FUSE_CALLBACK(buffer_p,rozofs_ll_setlk_after_flush);
      ret = buf_flush(buffer_p,f);
      if (ret == 0) return;
      
      lock_stat.buf_flush++;  
      warning("rozofs_ll_setlk_nb buf_flush %s",strerror(errno));      
    }   
    
    clear_read_data(f);  
    rozofs_ll_setlk_internal(f);  
    goto out;

error:
    fuse_reply_err(req, errno);
    if (lock_trc_idx) {
      if (f) rozofs_trc_rsp(srv_rozofs_ll_setlk,ino,f->fid,1,lock_trc_idx);    
      else   rozofs_trc_rsp(srv_rozofs_ll_setlk,ino,NULL,1,lock_trc_idx);    
    }
out:    
    /*
    ** release the buffer if has been allocated
    */
    STOP_PROFILING_NB(buffer_p,rozofs_ll_setlk);
    if (buffer_p != NULL) rozofs_fuse_release_saved_context(buffer_p);
}
/*
**__________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_setlk_after_flush(void *this,void *param) 
{
   struct rpc_msg  rpc_reply;
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   storcli_status_ret_t ret;
   xdrproc_t decode_proc = (xdrproc_t)xdr_storcli_status_ret_t;
   file_t *file = NULL;

   rpc_reply.acpted_rply.ar_results.proc = NULL;
   RESTORE_FUSE_STRUCT_PTR(param,fi);    
   /*
   ** update the number of storcli pending request
   */
   if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;

   file = (file_t *) (unsigned long)  fi->fh;   
   file->buf_write_pending--;
   if (file->buf_write_pending < 0)
   {
     severe("buf_write_pending mismatch, %d",file->buf_write_pending);
     file->buf_write_pending = 0;     
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
       severe(" transaction error %s",strerror(errno));
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
    ** ok now call the procedure to decode the message
    */
    memset(&ret,0, sizeof(ret));                    
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       severe(" transaction error %s",strerror(errno));
       xdr_free((xdrproc_t) decode_proc, (char *) &ret);       
       goto error;
    }   
    if (ret.status == STORCLI_FAILURE) {
        errno = ret.storcli_status_ret_t_u.error;
	severe("storcli error %s", strerror(errno));    
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);
        goto error;
    }
    /*
    ** no error, so get the length of the data part
    */
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);

    /*
    ** If the file size is increased, we must update the exportd
    */
    if (export_write_block_asynchrone(param,file,rozofs_ll_setlk_after_write_block)==0) {
      goto out;
    }    
    severe("rozofs_ll_setlk_after_flush export_write_block_asynchrone %s",strerror(errno));
    
error:

    lock_stat.flush_error++;       

    clear_read_data(file);         
    rozofs_ll_setlk_internal(file);    
    STOP_PROFILING_NB(param,rozofs_ll_setlk);
    rozofs_fuse_release_saved_context(param);
    goto out;
     
out:    
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
}
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_setlk_after_write_block(void *this,void *param) {
   epgw_io_ret_t ret ;
   struct rpc_msg  rpc_reply;
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_io_ret_t;
   rozofs_fuse_save_ctx_t *fuse_ctx_p;
   struct fuse_file_info * fi;    
   file_t * file = NULL;
   int      trc_idx;
   fuse_ino_t ino;
    
   GET_FUSE_CTX_P(fuse_ctx_p,param);    
   
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   
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
        errno = ret.status_gw.ep_io_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);  
    errno = 0;     
    goto out;
        
error:
    lock_stat.write_block_error++;      
    goto out; 
    
out: 
    // Trace the write block response
    RESTORE_FUSE_PARAM(param,ino);
    RESTORE_FUSE_PARAM(param,trc_idx);   
    rozofs_trc_rsp(srv_rozofs_ll_ioctl,ino,NULL,(errno==0)?0:1,trc_idx);


    RESTORE_FUSE_STRUCT_PTR(param,fi); 
    file = (file_t*) (unsigned long) fi->fh;
    clear_read_data(file);             
    rozofs_ll_setlk_internal(file);
    STOP_PROFILING_NB(param,rozofs_ll_setlk);
    rozofs_fuse_release_saved_context(param);    

    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
}
/**
*  re attemp a blocking file lock setting

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param parent : inode parent provided by rozofsmount
 @param name : name to search in the parent directory
 
 @retval none
*/
int rozofs_ll_setlk_internal(file_t * file) {
    epgw_lock_arg_t arg;
    struct timeval tv;
    int ret;

    gprofiler->rozofs_ll_setlk_int[P_COUNT]++;
    gettimeofday(&tv,(struct timezone *)0);
    file->timeStamp = MICROLONG(tv);

    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid                          = exportclt.eid;
    strncpy(arg.arg_gw.client_info.vers,VERSION,ROZOFS_VERSION_STRING_LENGTH);    
    arg.arg_gw.client_info.diag_port = rozofsmount_diag_port;    
    memcpy(arg.arg_gw.fid, file->fid, sizeof (uuid_t));
    arg.arg_gw.lock.mode                    = file->lock_type;
    arg.arg_gw.lock.client_ref              = rozofs_client_hash;
    arg.arg_gw.lock.owner_ref               = file->lock_owner_ref;
    arg.arg_gw.lock.user_range.size         = file->lock_size;
    arg.arg_gw.lock.user_range.offset_start = file->lock_start;
    arg.arg_gw.lock.user_range.offset_stop  = file->lock_stop;   
    compute_effective_lock_range(exportclt.bsize,&arg.arg_gw.lock);    			
		
    /*
    ** now initiates the transaction towards the remote end
    */
#if 1
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)file->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_SET_FILE_LOCK,(xdrproc_t) xdr_epgw_lock_arg_t,(void *)&arg,
                              rozofs_ll_setlk_internal_cbk,file); 
#else
    ret = rozofs_export_send_common(&exportclt,ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM),EXPORT_PROGRAM, EXPORT_VERSION,
                                     EP_SET_FILE_LOCK,(xdrproc_t) xdr_epgw_lock_arg_t,(void *)&arg, 
			             rozofs_ll_setlk_internal_cbk,file); 

#endif
   if (ret == 0) return ret;
   gettimeofday(&tv,(struct timezone *)0); 
   gprofiler->rozofs_ll_setlk_int[P_ELAPSE] += (MICROLONG(tv)-file->timeStamp);  
   /* If lock request can not be sent, rechain the lock request immediatly */
   ruc_objInsertTail(&pending_lock_list,&file->pending_lock);
   return ret;   				         
}
/**
*  Invalidate the kernel cache once the lock has been granted
*

 @return none
 */
void rozofs_flock_invalidate_cache(file_t * file) { 
 
  ientry_t *ie = 0;
  struct ep_lock_t flock;
  uint32_t bbytes = ROZOFS_BSIZE_BYTES(exportclt.bsize);
  /*
  ** Lock has been aquired. It case it not a fre
  ** empty the kernel cache
  */
  if (file->lock_type == EP_LOCK_FREE) return;
  

  ie = get_ientry_by_fid(file->fid);
  if (ie == NULL) {
    warning ("rozofs_flock_invalidate_cache no ie entry");
    return;
  }
  
  flock.user_range.offset_start = file->lock_start;
  flock.user_range.offset_stop  = file->lock_stop;	
  if (flock.user_range.offset_start == 0) {
    if (flock.user_range.offset_stop == 0) {
      flock.user_range.size = EP_LOCK_TOTAL;
    }
    else {
      flock.user_range.size = EP_LOCK_FROM_START;
    }  
  }
  else {
    if (flock.user_range.offset_stop == 0) {
      flock.user_range.size = EP_LOCK_TO_END;
    }
    else {
      flock.user_range.size = EP_LOCK_PARTIAL;
    }  
  }    
  compute_effective_lock_range(exportclt.bsize,&flock);   
  rozofs_fuse_invalidate_inode_cache(ie->inode, 
                                     flock.effective_range.offset_start*bbytes,
				     (flock.effective_range.offset_stop-flock.effective_range.offset_start)*bbytes);				            
}
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */

void rozofs_ll_setlk_internal_cbk(void *this,void * param) 
{
   epgw_lock_ret_t ret ;
   struct rpc_msg  rpc_reply;   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_lock_ret_t;
   file_t * file = (file_t*) param;
   struct timeval tv;

    /*
    ** get the pointer to the transaction context:
    ** it is required to get the information related to the receive buffer
    */
    rozofs_tx_ctx_t      *rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;     
 
    /*
    ** Check that this entry is still valid
    */
    if ((file->chekWord != FILE_CHECK_WORD)
    ||  (file->fuse_req == NULL)
    ||  (file->lock_owner_ref == 0)) {
      lock_stat.set_lock_interrupted++;        
      goto out;
    }
           
    rpc_reply.acpted_rply.ar_results.proc = NULL;

   /*    
    ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
    */
    status = rozofs_tx_get_status(this);
    if (status < 0) 
    {
       goto again; 
    }

    /*
    ** get the pointer to the receive buffer payload
    */
    recv_buf = rozofs_tx_get_recvBuf(this);
    if (recv_buf == NULL) 
    {
      goto again;   
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
    if (ret.gw_status.status == EP_SUCCESS) {
      fuse_reply_err(file->fuse_req, 0);
      errno = 0;
      rozofs_trc_rsp(srv_rozofs_ll_setlk,0/*ino*/,file->fid,0,file->lock_trc_idx);
      file->fuse_req = NULL;      
      xdr_free((xdrproc_t) decode_proc, (char *) &ret);   
      lock_stat.set_lock_success++;       
      /*
      ** Lock has been aquired. It case it not a fre
      ** empty the kernel cache
      */
      //rozofs_flock_invalidate_cache(file);				           
      goto out;
    }
    
    if (ret.gw_status.status != EP_EAGAIN) {
      errno = ret.gw_status.ep_lock_ret_t_u.error;    	
      xdr_free((xdrproc_t) decode_proc, (char *) &ret); 
      goto error;
    }
    /*
    ** we got EAGAIN, check if a "sleep" was requested. When it is
    ** no requested, then report the EAGAIN error
    */
    if (file->lock_sleep== 0) 
    {
      fuse_reply_err(file->fuse_req, EAGAIN);
      errno = EAGAIN;
      rozofs_trc_rsp(srv_rozofs_ll_setlk,0/*ino*/,file->fid,1,file->lock_trc_idx);          
      file->fuse_req = NULL;      
      xdr_free((xdrproc_t) decode_proc, (char *) &ret);   
      lock_stat.set_lock_refused++;    
      goto  out;  
    }
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);   
again:
    /* Rechain the lock request  */
    ruc_objInsertTail(&pending_lock_list,&file->pending_lock);    
    goto out;  
    
error:
    lock_stat.set_lock_error++;
    fuse_reply_err(file->fuse_req, errno);
    file->fuse_req = NULL;
    rozofs_trc_rsp(srv_rozofs_ll_setlk,0/*ino*/,file->fid,1,file->lock_trc_idx);          
    
out:   
    gettimeofday(&tv,(struct timezone *)0); 
    gprofiler->rozofs_ll_setlk_int[P_ELAPSE] += (MICROLONG(tv)-file->timeStamp);   
     
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);           
}
/**
*  reset all the file locks of a given owner (at file release for instance)
*  This is an internal request that do not trigger any response toward fuse


 @param fid     :FID of thye file on which the locks are to be removed
 @param owner   :reference of the owner of the lock
 
 @retval none
*/
void rozofs_clear_file_lock_owner_cbk(void *this,void *param);

void rozofs_clear_file_lock_owner(file_t * f) {
    epgw_lock_arg_t arg;
    int    ret;        
    void *buffer_p = NULL;
    
    /*
    ** In case a blocking lock request is pending on this file,
    ** stop requesting exportd for the lock
    */
    if (f->fuse_req != NULL) {
    
      /* 
      ** Reply to fuse 
      */
      fuse_reply_err(f->fuse_req, EINTR);
      f->fuse_req = NULL;
 
      /* 
      ** Eventualy remove it from the list of request waiting for a lock
      */
      ruc_objRemove(&f->pending_lock);
    }
    
    
    
    /*
    ** Now ask the exportd to forget all the locks of this guy
    */
    
    
    /*
    ** allocate a context for saving the fuse parameters
    */
    buffer_p = rozofs_fuse_alloc_saved_context();
    if (buffer_p == NULL)
    {
      lock_stat.enomem++;   
      errno = ENOMEM;
      goto error;
    }  
    
    START_PROFILING_NB(buffer_p,rozofs_ll_clearlkowner);

    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid               = exportclt.eid;
    strncpy(arg.arg_gw.client_info.vers,VERSION,ROZOFS_VERSION_STRING_LENGTH);    
    arg.arg_gw.client_info.diag_port = rozofsmount_diag_port;    
    memcpy(arg.arg_gw.fid, f->fid, sizeof (uuid_t));
    arg.arg_gw.lock.mode         = EP_LOCK_FREE;
    arg.arg_gw.lock.client_ref   = rozofs_client_hash;
    arg.arg_gw.lock.owner_ref    = f->lock_owner_ref;
    arg.arg_gw.lock.user_range.size         = EP_LOCK_TOTAL;    
    arg.arg_gw.lock.user_range.offset_start = 0;
    arg.arg_gw.lock.user_range.offset_stop  = 0;      			
    compute_effective_lock_range(exportclt.bsize,&arg.arg_gw.lock);    			

    /*
    ** now initiates the transaction towards the remote end
    */
#if 1
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)f->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_CLEAR_OWNER_FILE_LOCK,(xdrproc_t) xdr_epgw_lock_arg_t,(void *)&arg,
                              rozofs_clear_file_lock_owner_cbk,buffer_p); 
#else
    ret = rozofs_export_send_common(&exportclt,ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM),EXPORT_PROGRAM, EXPORT_VERSION,
                                    EP_CLEAR_OWNER_FILE_LOCK,(xdrproc_t) xdr_epgw_lock_arg_t,(void *)&arg, 
			            rozofs_clear_file_lock_owner_cbk,buffer_p);       
#endif
    if (ret >= 0) return;

error:
    /*
    ** release the buffer if has been allocated
    */
    f->lock_owner_ref = 0;  
    STOP_PROFILING_NB(buffer_p,rozofs_ll_clearlkowner);
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

void rozofs_clear_file_lock_owner_cbk(void *this,void *param) 
{
   void     *recv_buf = NULL;   
   int       status;

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


error:
  
    /*
    ** release the transaction context and the fuse context
    */
    STOP_PROFILING_NB(param,rozofs_ll_clearlkowner);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);    
    
    return;
}
/**
*  reset all the locks of a given client
*  This is an internal request that do not trigger any response toward fuse


 @param eid           :eid this client is mounted on
 @param client_hash   :reference of the client
 
 @retval none
*/
void rozofs_ll_clear_client_file_lock(int eid, uint64_t client_hash) {
    epgw_lock_arg_t arg;

    memset(&arg,0,sizeof(epgw_lock_arg_t));
    arg.arg_gw.eid             = exportclt.eid;
    arg.arg_gw.lock.client_ref = rozofs_client_hash; 
    strncpy(arg.arg_gw.client_info.vers,VERSION,ROZOFS_VERSION_STRING_LENGTH);    
    arg.arg_gw.client_info.diag_port = rozofsmount_diag_port;    

    /*
    ** now initiates the transaction towards the remote end
    */
    rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)NULL,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_CLEAR_CLIENT_FILE_LOCK,(xdrproc_t) xdr_epgw_lock_arg_t,(void *)&arg,
                              rozofs_poll_cbk,NULL); 

    return;
}
