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

#include "rozofs_fuse_api.h"

#define ROZOFSMOUNT_LOCK_POLL_PERIOD ((common_config.client_flock_timeout/2)-1)

extern uint64_t   rozofs_client_hash;

/*
** List of file descriptors on which some locks are requested but not yet granted
** Rozofsmount has to periodicaly re-request the lock to the exportd
*/
static list_t         rozofsmount_requested_lock_list; 

/*
** List of ientries on which someone owns a lock from that rozofsmount point.
** Rozofsmount has to periodicaly poll the export to maintain these locks
*/ 
static list_t         rozofsmount_granted_lock_list;


extern struct fuse_chan *rozofsmount_fuse_chanel;

int rozofs_ll_setlk_internal(file_t * file);
void rozofs_ll_setlk_internal_cbk(void *this,void * param);

static int is_old_export = 1;

DECLARE_PROFILING(mpp_profiler_t);

typedef struct _LOCK_STATISTICS_T {
  uint64_t      bsd_set_passing_lock;
  uint64_t      bsd_set_blocking_lock;
  uint64_t      posix_set_passing_lock;
  uint64_t      posix_set_blocking_lock;
  uint64_t      posix_get_lock;
  uint64_t      flush_required;
  uint64_t      set_lock_internal;
  uint64_t      set_lock_again;  
  uint64_t      set_lock_refused;
  uint64_t      set_lock_success;  
  uint64_t      set_lock_error;
  uint64_t      set_lock_interrupted;  
  uint64_t      get_lock_refused;  
  uint64_t      get_lock_success; 
  uint64_t      get_lock_error;  
  uint64_t      poll_owner_send;
  uint64_t      poll_owner_send_error;
  uint64_t      poll_owner_recv;     
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


/*______________________________________________________
** Granted locks
** rozofsmount_lock_owner_table_t is to be chained on the this_ie_lock_owners
** list of the ientry_t 
*/

typedef struct _rozofsmount_lock_owner_t {
  uint64_t      owner_ref;   /**< Lock owner reference */
  time_t        next_poll;   /**< Next time twhen he poll should be sent */
} rozofsmount_lock_owner_t;

/*
** Up to 32 lock owners can reside in this table. 
** This table is linked on the ie
*/
#define ROZOFSMOUNT_NB_LOCK_OWNER   32 
typedef struct _rozofsmount_lock_owner_table_t {
  list_t                      list;
  uint32_t                    bitmap;
  rozofsmount_lock_owner_t    owners[ROZOFSMOUNT_NB_LOCK_OWNER];
} rozofsmount_lock_owner_table_t;
    
    

/*_____________________________________________________________
** Requested locks
** rozofsmount_lock_requested_t is to be chained on the this_file_requested_locks
** list of the file_t
*/
typedef struct _rozofsmount_lock_requested_t {
    list_t           list;
    int              lock_type;      /**< Type of requested lock : EP_LOCK_READ or EP_LOCK_WRITE */   
    uint64_t         owner_ref;      /**< Owner reference requesting the lock */
    int              size;           /**< King of lock interval */
    uint64_t         start;          /**< Start offset */    
    uint64_t         stop;           /**< Stop offset */   
    fuse_req_t       req;            /**< Fuse request */ 
    int              sleep;          /**< Blocking lock (sleep=1) or non blocking (sleep=0) */ 
    int              delay;
    int              trc_idx;
} rozofsmount_lock_requested_t;			

		    
void rozofs_clear_file_lock_one_owner(fid_t  fid, uint64_t owner_ref) ;

/*
**__________________________________________________________________________
** whence 2 string
**
** @param valid     
*/
static inline char *print_whence(int whence) {
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
/*
**__________________________________________________________________________
** Update or remove a given owner from the list of lock owners of a given
** file. The owners have to periodicaly poll the export to keep their granted
** locks alive
**
** @param valid      whether these locks are still valid(1) or must be removed(0)
** @param owner_ref  lock owner reference
** @param ie         ientry of the file
*/
void rozofsmount_validate_owner(uint64_t valid,uint64_t owner_ref, ientry_t * ie) {
  list_t                          * p;
  rozofsmount_lock_owner_table_t  * tbl = NULL;
  int                               idx;  
     
  /* 
  ** Search the given owner in the list of granted locks on this file
  */
  list_for_each_forward(p, &ie->this_ie_lock_owners) {

    tbl = list_entry(p, rozofsmount_lock_owner_table_t, list);	
    for (idx = 0; idx< ROZOFSMOUNT_NB_LOCK_OWNER; idx++) {
    
      /*
      ** Search the given owner reference in this context
      */
      if (tbl->owners[idx].owner_ref != owner_ref) {
        continue;
      }  

      /*
      ** Found
      */
      
      if (valid) {
        /*
        ** Revalidate this owner
        */
        tbl->owners[idx].next_poll = time(NULL) + ROZOFSMOUNT_LOCK_POLL_PERIOD;
        return;
      }
      /*
      ** This owner has no more locks.
      ** Remove it from the table
      */
      tbl->owners[idx].owner_ref = 0;
      tbl->bitmap &= ~(1<<idx);
      /*
      ** This table is empty and so should be removed
      */            
      if (tbl->bitmap == 0) {
        list_remove(&tbl->list);
        xfree(tbl);
      }
      /*
      ** If no more locks on this ientry, remove this file from the
      ** list of files owning a lock
      */
      if (list_empty(&ie->this_ie_lock_owners)) {
        list_remove(&ie->next_ie_in_granted_list);
      }
      return;
    }
  }
  
  if (valid == 0) {
    /*
    ** This guy is not an owner of this file
    ** Nothing to do.
    */
    return;
  }
  
  /*
  ** Insert this guy in a table of lock owner on this file
  */
  
  /* 
  ** Search a free entry 
  */
  list_for_each_forward(p, &ie->this_ie_lock_owners) {

    tbl = list_entry(p, rozofsmount_lock_owner_table_t, list);	
    if (tbl->bitmap == 0xFFFFFFFF) continue; // This table is full
    for (idx = 0; idx< ROZOFSMOUNT_NB_LOCK_OWNER; idx++) {
      /*
      ** Search en entry to 0
      */
      if (tbl->owners[idx].owner_ref == 0) {
        tbl->owners[idx].next_poll = time(NULL) + ROZOFSMOUNT_LOCK_POLL_PERIOD;
        tbl->owners[idx].owner_ref = owner_ref;
        tbl->bitmap |= (1<<idx); 
        return;
      }
    }
  }
  
  /*
  ** Allocate a table of owner for this file
  */
  tbl = xmalloc(sizeof(rozofsmount_lock_owner_table_t));
  memset(tbl,0,sizeof(rozofsmount_lock_owner_table_t));
  list_init(&tbl->list);
  tbl->owners[0].owner_ref = owner_ref;
  tbl->owners[0].next_poll = time(NULL) + ROZOFSMOUNT_LOCK_POLL_PERIOD;
  tbl->bitmap = 1;
  /*
  ** Insert this table in the list of owner on this file
  */
  list_push_back(&ie->this_ie_lock_owners, &tbl->list);
  /*
  ** Insert this file in the list of file owning a lock
  */
  list_remove(&ie->next_ie_in_granted_list);
  list_push_back(&rozofsmount_granted_lock_list,&ie->next_ie_in_granted_list);
  return;   
}
/*
**__________________________________________________________________________
** Invalidate all lock owners on this file
**
** @param ie      The ie whose locks must be invalidated
*/
void rozofsmount_invalidate_all_owners(ientry_t * ie) {
  rozofsmount_lock_owner_table_t  * tbl = NULL;
  int                               idx;  

  /* 
  ** loop on the lock owners of the file
  */
  while (!list_empty(&ie->this_ie_lock_owners)) {
  
    tbl = list_first_entry(&ie->this_ie_lock_owners,rozofsmount_lock_owner_table_t, list);   
    for (idx = 0; idx< ROZOFSMOUNT_NB_LOCK_OWNER; idx++) {
      /*
      ** Search a valid owner reference
      */
      if (tbl->owners[idx].owner_ref == 0) continue;
      /*
      ** send owner invalidation to exportd
      ** Best effort. If the message can not be sent, the lock will be automatically 
      ** relesed on polling timeout on the export
      */
      rozofs_clear_file_lock_one_owner(ie->fid, tbl->owners[idx].owner_ref);
      tbl->owners[idx].owner_ref = 0;
    }
    
    list_remove(&tbl->list);
    xfree(tbl);
  }
  /*
  ** Remove the file from the list of file descriptor owning some locks
  */
  list_remove(&ie->next_ie_in_granted_list);
}
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
#define DISPLAY_LOCK_STAT(name) { \
  if (firstie) firstie = 0;\
  else p += rozofs_string_append(p,",\n");\
  p += rozofs_string_append(p,"      \"");\
  p += rozofs_string_append(p,#name);\
  if (strlen(#name)<=6) p += rozofs_string_append(p,"\" \t\t\t: ");\
  else if (strlen(#name)<=14) p += rozofs_string_append(p,"\" \t\t: ");\
  else if (strlen(#name)<=22) p += rozofs_string_append(p,"\" \t: ");\
  else p += rozofs_string_append(p,"\" : ");\
  p += rozofs_u64_append(p,lock_stat.name);\
}
char * display_lock_stat(char * p) {
  list_t                          * lie;
  list_t                          * lowner;
  ientry_t                        * ie;
  file_t                          * file;
  rozofsmount_lock_owner_table_t  * tbl = NULL;
  int                               idx;  
  char                              fidString[40];
  time_t                            now = time(NULL); 
  int                               firstie=1;
  int                               firstowner=1;

  p += rozofs_string_append(p,"{ \"file locks\" : {\n");
  if (is_old_export) {
    p += rozofs_string_append(p,"    \"exportd\" : \"older than 2.13.2 / 3.0.~alpha55\",\n"); 
  }
  p += rozofs_string_append(p,"    \"statistics\" : {\n"); 
  DISPLAY_LOCK_STAT(bsd_set_passing_lock);
  DISPLAY_LOCK_STAT(bsd_set_blocking_lock);  
  DISPLAY_LOCK_STAT(posix_set_passing_lock);    
  DISPLAY_LOCK_STAT(posix_set_blocking_lock);
  DISPLAY_LOCK_STAT(set_lock_internal);
  DISPLAY_LOCK_STAT(set_lock_again);    
  DISPLAY_LOCK_STAT(set_lock_refused);
  DISPLAY_LOCK_STAT(set_lock_success);
  DISPLAY_LOCK_STAT(set_lock_error);
  DISPLAY_LOCK_STAT(set_lock_interrupted);
  DISPLAY_LOCK_STAT(flush_required);  
  DISPLAY_LOCK_STAT(posix_get_lock);  
  DISPLAY_LOCK_STAT(get_lock_refused);
  DISPLAY_LOCK_STAT(get_lock_success);
  DISPLAY_LOCK_STAT(get_lock_error);
  DISPLAY_LOCK_STAT(poll_owner_send);
  DISPLAY_LOCK_STAT(poll_owner_send_error);
  DISPLAY_LOCK_STAT(poll_owner_recv);       
  DISPLAY_LOCK_STAT(enoent);
  DISPLAY_LOCK_STAT(ebadf);
  DISPLAY_LOCK_STAT(enomem);
  DISPLAY_LOCK_STAT(einval);
  DISPLAY_LOCK_STAT(mandatory);
  DISPLAY_LOCK_STAT(flush_error);
  DISPLAY_LOCK_STAT(buf_flush);
  DISPLAY_LOCK_STAT(write_block_error);
  DISPLAY_LOCK_STAT(send_common);
  p += rozofs_string_append(p,"\n    },\n"); 
 
  /* 
  ** Search the given owner in the list of lock owner of this file
  */
  p += rozofs_string_append(p,"    \"client\" \t\t\t: \""); 
  p += rozofs_x64_append(p,rozofs_client_hash);
  p += rozofs_string_append(p,"\",\n    \"poll period\" \t\t: "); 
  p += rozofs_u32_append(p,ROZOFSMOUNT_LOCK_POLL_PERIOD);
  
  p += rozofs_string_append(p,",\n    \"granted locks\" : ["); 
  firstie = 1;
  list_for_each_forward(lie, &rozofsmount_granted_lock_list) {
  
    ie = list_entry(lie, ientry_t, next_ie_in_granted_list);	 
      
    if (firstie) {
     firstie = 0;  
    }  
    else {
      p += rozofs_string_append(p,",");
    }  
    p += rozofs_string_append(p,"\n        { \"fid\" : \"");  
    rozofs_fid_append(fidString,ie->fid);
    p += rozofs_string_append(p,fidString);
    p += rozofs_string_append(p,"\",\n"); 
    p += rozofs_string_append(p,"          \"owners\" : ["); 
    
    /* 
    ** Search the given owner in the list of lock owner of this file
    */
    firstowner = 1;
    list_for_each_forward(lowner, &ie->this_ie_lock_owners) {

      tbl = list_entry(lowner, rozofsmount_lock_owner_table_t, list);	
      for (idx = 0; idx< ROZOFSMOUNT_NB_LOCK_OWNER; idx++) {
        /*
        ** Search the given owner reference in this context
        */
        if (tbl->owners[idx].owner_ref == 0) continue;
        if (firstowner) {
         firstowner = 0;  
        }  
        else {
          p += rozofs_string_append(p,",");
        }  
        p += rozofs_string_append(p,"\n            { \"ref\" : \""); 
        p += rozofs_x64_append(p,tbl->owners[idx].owner_ref);
        p += rozofs_string_append(p,"\", \"next poll\" : "); 
        p += rozofs_i32_append(p,tbl->owners[idx].next_poll-now);
        p += rozofs_string_append(p,"}"); 
      }
    }      
    p += rozofs_string_append(p,"\n          ]");  
    p += rozofs_string_append(p,"\n        }");  
  }
  p += rozofs_string_append(p,"\n    ],\n");
  firstie = 1;
  p += rozofs_string_append(p,"    \"requested locks\" : ["); 
  
  list_for_each_forward(lie, &rozofsmount_requested_lock_list) {
  
    file = list_entry(lie, file_t, next_file_in_requested_list);	 
      
    if (firstie) {
     firstie = 0;  
    }  
    else {
      p += rozofs_string_append(p,",");
    }  
    p += rozofs_string_append(p,"\n        { \"fid\" : \"");  
    rozofs_fid_append(fidString,file->fid);
    p += rozofs_string_append(p,fidString);
    p += rozofs_string_append(p,"\", \"owner\" : \""); 
    p += rozofs_x64_append(p,file->lock_owner_ref);
    p += rozofs_string_append(p,"\", \"type\" : "); 
    switch(file->lock_type) {
      case EP_LOCK_FREE: 
        p += rozofs_string_append(p," \"free\""); 
        break;
      case EP_LOCK_READ:
        p += rozofs_string_append(p," \"read\""); 
        break;
      case EP_LOCK_WRITE:
        p += rozofs_string_append(p," \"write\""); 
        break;
      default:
        p += rozofs_u32_append(p,file->lock_type); 
        break;
    }      
    p += rozofs_string_append(p,", \"start\" : "); 
    p += rozofs_u64_append(p,file->lock_start);
    p += rozofs_string_append(p,", \"stop\" : "); 
    p += rozofs_u64_append(p,file->lock_stop);
    p += rozofs_string_append(p,"}"); 
  }
  p += rozofs_string_append(p,"\n    ]");  
  p += rozofs_string_append(p,"\n  }\n}\n");
  return p;
}
/*
*___________________________________________________________________
** Recompute the effective range of the lock from the user range
** A lock, locks a whole block so effective lock range must increase
** the requested range to reacch the block bundaries
**
** @param bsize_e  the block size (enumeated list)
** @param lock     the requested lock
**
**___________________________________________________________________
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
** Compute flocks start and stop
**
** @param lock     The requested lock
** @param start    Returned start offset
** @param stop     Returned stop offset
** 
** @return the range type 
*/
ep_lock_size_t rozofs_flock_canonical(struct flock * lock, file_t * f, int64_t * start, int64_t * stop) {

  *stop = 0;
  ientry_t *ie;
  
  ie=f->ie;
  
  if  (lock->l_whence == SEEK_CUR) {
    *start = f->current_pos;
  }
  else if (lock->l_whence == SEEK_END)  {
    *start = ie->attrs.attrs.size;
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
** Poll response from the exportd
**
** @param this : pointer to the transaction context
** @param param: pointer to the associated rozofs_fuse_context
** 
** @return none
*/
void rozofs_poll_owner_cbk(void *this,void *param)  {
  void          * recv_buf = NULL;   
  int             status;
  xdrproc_t       decode_proc = (xdrproc_t)xdr_epgw_lock_ret_t;
  uint8_t       * payload;
  XDR             xdrs;    
  int             bufsize;
  struct rpc_msg  rpc_reply;
  epgw_lock_ret_t ret ;
  fuse_ino_t      inode = (fuse_ino_t) param;
  ientry_t      * ie = NULL;

  /*
  ** Inode is the param
  ** Retrieve the ientry from the inode
  */
  ie = get_ientry_by_inode(inode);
 
  rpc_reply.acpted_rply.ar_results.proc = NULL;

  /*
  ** get the pointer to the transaction context:
  ** it is required to get the information related to the receive buffer
  */
  rozofs_tx_ctx_t *rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;  

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
  ** Decode the received message
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

  lock_stat.poll_owner_recv++;
  
  /*
  ** Call the procedure to decode the message
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
  ** When ientry is found and result is success
  ** revalidate or invalidate the lock owners
  */
  if ((ret.gw_status.status == EP_SUCCESS) && (ie!=NULL)) {    
    rozofsmount_validate_owner(ret.gw_status.ep_lock_ret_t_u.lock.client_ref,ret.gw_status.ep_lock_ret_t_u.lock.owner_ref, ie);      
  }        

error:

  /*
  ** release the transaction context and the fuse context
  */
  if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
  if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);        
  return;
}
/**
**____________________________________________________
** Old poll mode call back used when export is old
**
** @param this : pointer to the transaction context
** @param param: pointer to the associated rozofs_fuse_context
** 
** @return none
*/
void rozofs_poll_cbk(void *this,void *param)  {
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
  if (status < 0) {
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
**
** Periodicaly pool to maintain the granted locks
**
** @param now    Current time in seconds
*/
time_t next_poll = 0;
time_t next_poll_null = 0;
static inline void rozofs_flock_periodic_poll_granted_locks(time_t now) {
  list_t                          * lie;
  list_t                          * lowner;
  ientry_t                        * ie;
  rozofsmount_lock_owner_table_t  * tbl = NULL;
  int                               idx;  
  epgw_lock_arg_t                   arg;
  fuse_ino_t                        inode; 
  int                               ret;
  
  /*
  ** Just once every 5 seconds
  */
  if (now < next_poll) return;
  next_poll = now + 5;

  /*
  ** Old export case. Send a global old poll message for all granted locks
  */
  if (is_old_export) {
    fid_t fake_fid;
    memset(fake_fid,0,sizeof(fid_t));

    next_poll += 8;
    
    arg.arg_gw.eid = exportclt.eid;
    strncpy(arg.arg_gw.client_info.vers,VERSION,ROZOFS_VERSION_STRING_LENGTH);    
    arg.arg_gw.client_info.diag_port = rozofsmount_diag_port;    
    arg.arg_gw.lock.client_ref = rozofs_client_hash;
    rozofs_expgateway_send_no_fuse_ctx(arg.arg_gw.eid,(unsigned char*)fake_fid,
                                       EXPORT_PROGRAM, EXPORT_VERSION, EP_POLL_FILE_LOCK,
                                       (xdrproc_t) xdr_epgw_lock_arg_t,(void *)&arg,
                                       rozofs_poll_cbk,NULL);
    return;                             
  }


    
  /*
  ** When no lock is set, just send a clear client lock
  */
  if (list_empty(&rozofsmount_granted_lock_list)) {
     
    /*
    **  Once every 8 polling period
    */
    if (now < next_poll_null) return;
    next_poll_null = now + (ROZOFSMOUNT_LOCK_POLL_PERIOD*8);

    /*
    ** FID is set to 0 and owner ref is NULL
    */
    arg.arg_gw.eid = exportclt.eid;
    memset(arg.arg_gw.fid, 0, sizeof (uuid_t));
    strncpy(arg.arg_gw.client_info.vers,VERSION,ROZOFS_VERSION_STRING_LENGTH);    
    arg.arg_gw.client_info.diag_port = rozofsmount_diag_port;    
    arg.arg_gw.lock.client_ref = rozofs_client_hash;
    arg.arg_gw.lock.owner_ref  = 0;

    ret = rozofs_expgateway_send_no_fuse_ctx(arg.arg_gw.eid,(unsigned char *)arg.arg_gw.fid,
                                          EXPORT_PROGRAM, EXPORT_VERSION, EP_POLL_OWNER_LOCK,
                                          (xdrproc_t) xdr_epgw_lock_arg_t,(void *)&arg,
                                          rozofs_poll_owner_cbk,(void*)0);  
    if (ret == 0) {
      lock_stat.poll_owner_send++;
    }
    else {
      lock_stat.poll_owner_send_error++;          
    }   
    return;
  }
    
  /*
  ** Loop on ientries that have some granted locks
  */
  list_for_each_forward(lie, &rozofsmount_granted_lock_list) {
  
    /*
    ** Fetch ientry
    */
    ie = list_entry(lie, ientry_t, next_ie_in_granted_list);	 
     
    /*
    ** Loop on lock owners of this file
    */ 
    list_for_each_forward(lowner, &ie->this_ie_lock_owners) {

      tbl = list_entry(lowner, rozofsmount_lock_owner_table_t, list);	
      for (idx = 0; idx< ROZOFSMOUNT_NB_LOCK_OWNER; idx++) {
        /*
        ** Search the owners that must be refreshed
        */
        if (tbl->owners[idx].owner_ref == 0) continue;
        if (now < tbl->owners[idx].next_poll) continue;

        arg.arg_gw.eid = exportclt.eid;
        memcpy(arg.arg_gw.fid, ie->fid, sizeof (uuid_t));
        strncpy(arg.arg_gw.client_info.vers,VERSION,ROZOFS_VERSION_STRING_LENGTH);    
        arg.arg_gw.client_info.diag_port = rozofsmount_diag_port;    
        arg.arg_gw.lock.client_ref = rozofs_client_hash;
        arg.arg_gw.lock.owner_ref  = tbl->owners[idx].owner_ref;

        inode = (fuse_ino_t)  *((fuse_ino_t *) &ie->fid[8]); 
        ret = rozofs_expgateway_send_no_fuse_ctx(arg.arg_gw.eid,ie->fid,
                                              EXPORT_PROGRAM, EXPORT_VERSION, EP_POLL_OWNER_LOCK,
                                              (xdrproc_t) xdr_epgw_lock_arg_t,(void *)&arg,
                                              rozofs_poll_owner_cbk,(void*)inode);  
        if (ret == 0) {
          lock_stat.poll_owner_send++;
        }
        else {
          lock_stat.poll_owner_send_error++;  
          break;        
        }          
      }
    }      
  }
}   
/*
**____________________________________________________
**
** Periodic treatments
** - send poll to maintain granted locks
** - send lock requests to acquire requested locks
**
*/
void rozofs_flock_service_periodic(void * ns) {
  file_t            * file;
  int                 nbTxCredits;
  list_t            * p;
  list_t            * q;

  /*
  ** Poll the granted locks
  */
  rozofs_flock_periodic_poll_granted_locks(time(NULL));
   
  /*
  ** Check if there is some Tx credits to start a transaction
  */
  nbTxCredits = rozofs_tx_get_free_ctx_number() - rozofs_fuse_get_free_ctx_number();
  if (nbTxCredits <= 0) return;
    
  /* No more than 3 requests at a time */
  if (nbTxCredits > 3) nbTxCredits = 3;

  /* 
  ** Search the given owner in the list of owner of this file
  */
  list_for_each_forward_safe(p, q, &rozofsmount_requested_lock_list) {

    file = list_entry(p, file_t, next_file_in_requested_list);	   
    if (nbTxCredits <= 0) break;

    /*
    ** Check that this entry is still valid
    */
    if ((file->chekWord != FILE_CHECK_WORD)
    ||  (file->fuse_req == NULL)
    ||  (file->lock_owner_ref == 0)) {
      lock_stat.set_lock_interrupted++; 
      list_remove(&file->next_file_in_requested_list);   
      continue;
    }
    
    /*
    ** Check whether it is time to process this request 
    */
    
    if (file->lock_delay > 0) { 
      file->lock_delay--; 
      continue;
    }        
    
    list_remove(&file->next_file_in_requested_list);
    rozofs_ll_setlk_internal(file);
    nbTxCredits--;    
  }

}

/*
**____________________________________________________
*  Initialize the file lock service 
*/
void rozofs_flock_service_init(void) {
  struct timer_cell * periodic_timer;

  reset_lock_stat();  
  
  /*
  ** Initialize the head of list of file waiting for a blocking lock 
  */ 
  list_init(&rozofsmount_requested_lock_list);

  /*
  ** Initialize list of file descriptor with owner of a lock that has to be polled
  ** in order to keep the loks allive
  */
  list_init(&rozofsmount_granted_lock_list);

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
/*
**____________________________________________________
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

    /*
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_OTHER], 1);

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
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)file->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_GET_FILE_LOCK,(xdrproc_t) xdr_epgw_lock_arg_t,(void *)&arg,
                              rozofs_ll_getlk_cbk,buffer_p); 
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

/*
**____________________________________________________
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
/*
**____________________________________________________
*  Prepare a lock request context from inpu parameters
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
/*
**____________________________________________________
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
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_OTHER], 1);


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
          warning("rozofs_ll_flock_nb op = 0x%x (count %llu)",
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
/*
**____________________________________________________
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
    
    /*
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_OTHER], 1);
    
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
    ret = rozofs_expgateway_send_no_fuse_ctx(arg.arg_gw.eid,(unsigned char*)file->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_SET_FILE_LOCK,(xdrproc_t) xdr_epgw_lock_arg_t,(void *)&arg,
                              rozofs_ll_setlk_internal_cbk,file); 
   if (ret == 0) {
     lock_stat.set_lock_internal++;       
     return ret;
   }  
   gettimeofday(&tv,(struct timezone *)0); 
   gprofiler->rozofs_ll_setlk_int[P_ELAPSE] += (MICROLONG(tv)-file->timeStamp);  
   /* If lock request can not be sent, rechain the lock request immediatly */
   list_push_back(&rozofsmount_requested_lock_list,&file->next_file_in_requested_list);
   return ret;   				         
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
      
      /*
      ** New export set's ret.gw_status.ep_lock_ret_t_u.lock.client_ref to 0
      ** when some lock is left for this owner on this FID. 
      ** Old client always return the input client ref
      */
      if (is_old_export) {
        if (ret.gw_status.ep_lock_ret_t_u.lock.client_ref == 0) {
          is_old_export = 0;
        }
      }
      
      /*
      ** Check whether some locks still exit for this owner on this fid 
      */
      if (is_old_export) {
        if (ret.gw_status.ep_lock_ret_t_u.lock.mode == EP_LOCK_FREE) {
          rozofsmount_validate_owner(0,ret.gw_status.ep_lock_ret_t_u.lock.owner_ref, (ientry_t*)file->ie);
        }
        else {
          rozofsmount_validate_owner(1,ret.gw_status.ep_lock_ret_t_u.lock.owner_ref, (ientry_t*)file->ie);        
        }
      }
      else {
        rozofsmount_validate_owner(ret.gw_status.ep_lock_ret_t_u.lock.client_ref,ret.gw_status.ep_lock_ret_t_u.lock.owner_ref, (ientry_t*)file->ie);
      }
      
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
    lock_stat.set_lock_again++;
    /* Rechain the lock request  */
    list_push_back(&rozofsmount_requested_lock_list,&file->next_file_in_requested_list);
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
      list_remove(&f->next_file_in_requested_list);
    }
    
    if (f->lock_owner_ref == 0) return;
    
    /*
    ** Do not validate any more the locks owned by this owner
    ** on this file
    */
    rozofsmount_validate_owner(0,f->lock_owner_ref,(ientry_t*)f->ie);
    /*
    ** send owner invalidation to exportd
    */
    rozofs_clear_file_lock_one_owner(f->fid, f->lock_owner_ref);    
    /*
    ** release the buffer if has been allocated
    */
    f->lock_owner_ref = 0;  
}
/**
*  reset locks of a given owner on a file


 @param fid     :FID of thye file on which the locks are to be removed
 @param owner   :reference of the owner of the lock
 
 @retval none
*/
void rozofs_clear_file_lock_one_owner(fid_t fid, uint64_t owner_ref) {
    epgw_lock_arg_t arg;
    int    ret;        
    void *buffer_p = NULL;
           
    
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
    memcpy(arg.arg_gw.fid,  fid, sizeof (uuid_t));
    arg.arg_gw.lock.mode         = EP_LOCK_FREE;
    arg.arg_gw.lock.client_ref   = rozofs_client_hash;
    arg.arg_gw.lock.owner_ref    = owner_ref;
    arg.arg_gw.lock.user_range.size         = EP_LOCK_TOTAL;    
    arg.arg_gw.lock.user_range.offset_start = 0;
    arg.arg_gw.lock.user_range.offset_stop  = 0;      			
    compute_effective_lock_range(exportclt.bsize,&arg.arg_gw.lock);    			

    /*
    ** now initiates the transaction towards the remote end
    */
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_CLEAR_OWNER_FILE_LOCK,(xdrproc_t) xdr_epgw_lock_arg_t,(void *)&arg,
                              rozofs_clear_file_lock_owner_cbk,buffer_p); 
    if (ret >= 0) {
      return;
    }
error:
    /*
    ** release the buffer if has been allocated
    */
    STOP_PROFILING_NB(buffer_p,rozofs_ll_clearlkowner);
    if (buffer_p != NULL) rozofs_fuse_release_saved_context(buffer_p);
    {
       char fidString[40];
       rozofs_fid2string(fid,fidString);
       warning("Clear owner ref %llu FID %s ERROR",(long long unsigned int) owner_ref, fidString);  
    }
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
**____________________________________________________
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */

void rozofs_clear_client_file_lock_cbk(void *this,void *param) 
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
    rozofs_expgateway_send_no_fuse_ctx(arg.arg_gw.eid,(unsigned char*)NULL,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_CLEAR_CLIENT_FILE_LOCK,(xdrproc_t) xdr_epgw_lock_arg_t,(void *)&arg,
                              rozofs_clear_client_file_lock_cbk,NULL); 

    return;
}
