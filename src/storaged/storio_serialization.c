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
#include <rozofs/core/rozofs_string.h>

#include "storio_serialization.h"


uint64_t   storage_unqueued_req[STORIO_DISK_THREAD_MAX_OPCODE]={0};
uint64_t   storage_queued_req[STORIO_DISK_THREAD_MAX_OPCODE]={0};
uint64_t   storage_direct_req[STORIO_DISK_THREAD_MAX_OPCODE]={0};


/*_______________________________________________________________________
* Display serialization counter debug help
*/
static char * display_serialization_counters_help(char * pChar) {
  pChar += rozofs_string_append(pChar,"usage:\nserialization reset       : reset serialization counter\n");
  return pChar; 
}
/*_______________________________________________________________________
* Reset serialization counters
*/
static inline void reset_serialization_counters(void) {
  memset(storage_queued_req,0, sizeof(storage_queued_req));
  memset(storage_unqueued_req,0, sizeof(storage_unqueued_req));
  memset(storage_direct_req,0, sizeof(storage_direct_req));
}
/*_______________________________________________________________________
* Display opcode
*/
char * serialize_opcode_string(int opcode) {
  switch(opcode) {
    case STORIO_DISK_THREAD_READ: return "read";
    case STORIO_DISK_THREAD_READ_RDMA: return "read";
    case STORIO_DISK_THREAD_WRITE: return "write";
    case STORIO_DISK_THREAD_WRITE_RDMA: return "write";
    case STORIO_DISK_THREAD_TRUNCATE: return "truncate";
    case STORIO_DISK_THREAD_REMOVE: return "remove";
    case STORIO_DISK_THREAD_REMOVE_CHUNK: return "remove_chunk";
    case STORIO_DISK_THREAD_WRITE_REPAIR: return "write repair";
    case STORIO_DISK_THREAD_WRITE_REPAIR2: return "write repair2";
    case STORIO_DISK_REBUILD_START: return "rebuild start";
    case STORIO_DISK_REBUILD_STOP: return "rebuild stop";
    default: return "Unknown";
  }
}
/*_______________________________________________________________________
* Serialization debug function
*/
void display_serialization_counters (char * argv[], uint32_t tcpRef, void *bufRef) {
  char          * p = uma_dbg_get_buffer();
  int             opcode;
  char          * sep = "+----------------+------------------+------------------+------------------+\n";
  int             doreset=0;
  
  if (argv[1] != NULL) {
    if (strcmp(argv[1],"reset")==0) {
      doreset = 1;
    }
    else {  
      p = display_serialization_counters_help(p);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;   
    }   
  } 
      
  p += rozofs_string_append(p, sep);
  p += rozofs_string_append(p,"|    request     |     direct       |     queued       |     unqueued     |\n");
  p += rozofs_string_append(p, sep); 
  for (opcode=1; opcode<STORIO_DISK_THREAD_MAX_OPCODE; opcode++) {  
    *p++ = '|'; *p++ = ' ';
    p += rozofs_string_padded_append(p,15,rozofs_left_alignment,serialize_opcode_string(opcode));
    *p++ = '|'; 
    p += rozofs_u64_padded_append(p,17,rozofs_right_alignment,storage_direct_req[opcode]);
    *p++ = ' '; *p++ = '|';
    p += rozofs_u64_padded_append(p,17,rozofs_right_alignment,storage_queued_req[opcode]);
    *p++ = ' '; *p++ = '|';    
    p += rozofs_u64_padded_append(p,17,rozofs_right_alignment,storage_unqueued_req[opcode]);
    *p++ = ' '; *p++ = '|'; 
    p += rozofs_eol(p);;      
  }
  p += rozofs_string_append(p, sep);
    
  if (doreset) {
    reset_serialization_counters();
    p += rozofs_string_append(p,"Reset Done\n");
  }  
  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());    
}
/*_______________________________________________________________________
* Initialize dserialization counters 
*/
void serialization_counters_init(void) {
  reset_serialization_counters();
  uma_dbg_addTopic_option("serialization", display_serialization_counters, UMA_DBG_OPTION_RESET); 
}
/*
**___________________________________________________________
** Put a request in the run queue
*/
static inline int storio_serialization_direct_run(storio_device_mapping_t * dev_map_p, rozorpc_srv_ctx_t *req_ctx_p) {
  list_remove(&req_ctx_p->list);
  list_push_back(&dev_map_p->running_request,&req_ctx_p->list);
  storage_direct_req[req_ctx_p->opcode]++;  
  return 1;  
}
/*
**___________________________________________________________
** Put a request in the run queue
*/
static inline int storio_serialization_unqueue_run(storio_device_mapping_t * dev_map_p, rozorpc_srv_ctx_t *req_ctx_p, uint64_t toc) {
  list_remove(&req_ctx_p->list);
  list_push_back(&dev_map_p->running_request,&req_ctx_p->list);
  storage_unqueued_req[req_ctx_p->opcode]++;    

  storio_disk_thread_intf_send(dev_map_p,req_ctx_p,toc) ;
  return 1;  
}
/*
**___________________________________________________________
** Put a request in the wait queue
*/
static inline int storio_serialization_wait(storio_device_mapping_t * dev_map_p, rozorpc_srv_ctx_t *req_ctx_p) {
  list_remove(&req_ctx_p->list);
  list_push_back(&dev_map_p->waiting_request,&req_ctx_p->list);
  storage_queued_req[req_ctx_p->opcode]++;
  return 0;
}
/*
**___________________________________________________________
** Get chunk information from a write request
*/
static inline int storio_check_write_allocate_chunk(storio_device_mapping_t * dev_map_p, rozorpc_srv_ctx_t *req_ctx_p) {
  sp_write_arg_t * write_arg_p = (sp_write_arg_t *) ruc_buf_getPayload(req_ctx_p->decoded_arg);
  int block_per_chunk          = ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(write_arg_p->bsize);
  int chunk                    = write_arg_p->bid/block_per_chunk;

  /* Is chunk number valid */
  if (chunk >= ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) return 0;

  /* Is this chunk already allocated */
  if ((dev_map_p->device[chunk] == ROZOFS_EOF_CHUNK) || (dev_map_p->device[chunk] == ROZOFS_EMPTY_CHUNK)) {
    return 1;
  }

  /* Does the request requires more than one chunk */    
  if (((write_arg_p->bid%block_per_chunk)+write_arg_p->nb_proj) <= block_per_chunk){
    return 0;
  }     

  /* Is next chunk valid */   
  chunk++; 
  if (chunk >= ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) return 0;

  /* Is the next chunk already allocated */
  if ((dev_map_p->device[chunk] == ROZOFS_EOF_CHUNK) || (dev_map_p->device[chunk] == ROZOFS_EMPTY_CHUNK)) {
    return 1;
  }
  return 0;
} 
/*
**___________________________________________________________
** Check whether request must run alone
*/
static inline int storio_is_request_exclusive(storio_device_mapping_t * dev_map_p, rozorpc_srv_ctx_t *req_ctx_p) {

  /*
  ** While file location is unknown. Let' serialize every request
  */
  if (dev_map_p->device[0] == ROZOFS_UNKNOWN_CHUNK) {
    return 1;
  }

  switch (req_ctx_p->opcode) {
  
    /*
    ** Read is non exclusive
    */
    case STORIO_DISK_THREAD_READ: return 0;
    case STORIO_DISK_THREAD_READ_RDMA: return 0;

    /*
    ** Write are exclusive when a new chunk is to be allocated
    */
    case STORIO_DISK_THREAD_WRITE_RDMA:
    case STORIO_DISK_THREAD_WRITE:
    {
      sp_write_arg_t * write_arg_p = (sp_write_arg_t *) ruc_buf_getPayload(req_ctx_p->decoded_arg);
      int block_per_chunk          = ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(write_arg_p->bsize);
      int chunk                    = write_arg_p->bid/block_per_chunk;

      /* Is chunk number valid */
      if (chunk >= ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) return 0;

      /* Is this chunk already allocated */
      if ((dev_map_p->device[chunk] == ROZOFS_EOF_CHUNK) || (dev_map_p->device[chunk] == ROZOFS_EMPTY_CHUNK)) {
	return 1;
      }

      /* Does the request requires more than one chunk */    
      if (((write_arg_p->bid%block_per_chunk)+write_arg_p->nb_proj) <= block_per_chunk){
	return 0;
      }     

      /* Is next chunk valid */   
      chunk++; 
      if (chunk >= ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) return 0;

      /* Is the next chunk already allocated */
      if ((dev_map_p->device[chunk] == ROZOFS_EOF_CHUNK) || (dev_map_p->device[chunk] == ROZOFS_EMPTY_CHUNK)) {
	return 1;
      }
      return 0;      
    }

    /*
    ** Other requests are exclusive for incompatibility reasons
    **
    ** case STORIO_DISK_THREAD_TRUNCATE:
    ** case STORIO_DISK_THREAD_REMOVE:
    ** case STORIO_DISK_THREAD_REMOVE_CHUNK:
    ** case STORIO_DISK_REBUILD_START:
    ** case STORIO_DISK_REBUILD_STOP:
    */
    default:
      return 1;
  }    
    

} 
/*
**___________________________________________________________
*/
extern int        af_unix_disk_pending_req_count;
#define MAX_PENDING_REQUEST     64
extern uint64_t   af_unix_disk_pending_req_tbl[];

int storio_serialization_begin(storio_device_mapping_t * dev_map_p, rozorpc_srv_ctx_t *req_ctx_p) {
  list_t            * p;
  rozorpc_srv_ctx_t * req;
  
  /*
  ** put the start timestamp in the current context
  */
  req_ctx_p->profiler_time = rozofs_get_ticker_us();
  /*
  ** update the number of pending request
  */
  af_unix_disk_pending_req_count++;
  if (af_unix_disk_pending_req_count<MAX_PENDING_REQUEST) {
    af_unix_disk_pending_req_tbl[af_unix_disk_pending_req_count]++;
  }
  else {
    af_unix_disk_pending_req_tbl[MAX_PENDING_REQUEST-1]++;    
  }  
  /*
  ** queue the RPC request in the FID context
  */  
  if (storio_insert_pending_request_list(dev_map_p,&req_ctx_p->list))
  {
     storage_direct_req[req_ctx_p->opcode]++; 
    storio_disk_thread_intf_serial_send(dev_map_p,0);
  }
  else
  {
    storage_queued_req[req_ctx_p->opcode]++;
  }
  return 0;
   
  /*
  ** When waiting queue is not empty, put the request behind
  */
  if (!list_empty(&dev_map_p->waiting_request)) {
    return storio_serialization_wait(dev_map_p,req_ctx_p);
  } 

  /*
  ** Waiting queue is empty. If running queue too, please go
  */
  if (list_empty(&dev_map_p->running_request)) { 
    return storio_serialization_direct_run(dev_map_p,req_ctx_p);
  }  

  /*
  ** Waiting queue is empty and some requests are running
  */  
  
  
  /*
  ** The new request can not run with an other one
  */
  if (storio_is_request_exclusive(dev_map_p,req_ctx_p)) {
    return storio_serialization_wait(dev_map_p,req_ctx_p);  
  }

  /* 
  ** The new request can run with an other one. 
  ** Check the running requests can too.
  */
  list_for_each_forward(p, &dev_map_p->running_request) {
 
    req = list_entry(p, rozorpc_srv_ctx_t, list);

    if (storio_is_request_exclusive(dev_map_p,req)) {
      return storio_serialization_wait(dev_map_p,req_ctx_p);
    }
      
  }    
    
  return storio_serialization_direct_run(dev_map_p,req_ctx_p);      
}
/*
**___________________________________________________________
*/
void storio_serialization_end(storio_device_mapping_t * dev_map_p, rozorpc_srv_ctx_t *req_ctx_p) {	
  uint64_t            toc;    
  struct timeval      tv;
  list_t            * p, * q;
  rozorpc_srv_ctx_t * req;
  
  if (dev_map_p == NULL) return;
  
  /*
  ** Remove this request
  */
  list_remove(&req_ctx_p->list);
  
  if (list_empty(&dev_map_p->waiting_request)) {
  
    /*
    ** No waiting request to run
    */    
    return;
  }  
  
  /*
  ** Waiting list is not empty
  */


  /* 
  ** Check running requests are not exclusive 
  */
  p = NULL;
  list_for_each_forward(p, &dev_map_p->running_request) {

    req = list_entry(p, rozorpc_srv_ctx_t, list);

    /*
    ** Is this request exclusive
    */      
    if (storio_is_request_exclusive(dev_map_p,req)) {
      return;
    }
  }
    

  gettimeofday(&tv,(struct timezone *)0);
  toc = MICROLONG(tv);


  /* 
  ** Loop on waiting requests 
  */
  p = q = NULL;
  list_for_each_forward_safe(p, q, &dev_map_p->waiting_request) {

    req = list_entry(p, rozorpc_srv_ctx_t, list);

    /*
    ** No running request. This request can be unqueued
    */
    if (list_empty(&dev_map_p->running_request)) {
      storio_serialization_unqueue_run(dev_map_p,req, toc);
      /*
      ** When 1rst dequeued request is excluse, 
      ** no need to consider the following requests
      */
      if (storio_is_request_exclusive(dev_map_p,req)) {
        return;
      }
      continue;
    }

    /*
    ** There are some already running requets.
    ** Check if this request is exclusive
    */
    if (storio_is_request_exclusive(dev_map_p,req)) {
      return;  
    }
    
    storio_serialization_unqueue_run(dev_map_p,req, toc);
  }
  
  return;    
}





/*
**_________________________________________________________________________________
**_________________________________________________________________________________
*/
/*
**_______________________________________________________
*/
int storio_active_delay(int loop)
{
    int val = 0;
    while(val < loop)
    {
       val++;
    }
    return val;
}
/*
**_______________________________________________________
*/
void storio_get_serial_lock(pthread_rwlock_t *lock)
{
    while (pthread_rwlock_trywrlock(lock)!=0)
    {
       storio_active_delay(200);
    }
    return;

}

/*
**_______________________________________________________
*/

/**
  That function is intended to be used by the diskthreads
  
  @param p: pointer the FID context that contains the requests lisk
  @param diskthread_list: pointer to the current disk thread list

  @retval 1: no more request to process
  @retval 0: the list is not empty
*/
int storio_get_pending_request_list(storio_device_mapping_t *p,list_t *diskthread_list)
{
   /*
   ** check if the serial_pending_request list is empty
   */
   if (list_empty(&p->serial_pending_request))
   {
      /*
      ** check if it is the end of processing: this true with the
      ** diskthread list is empty
      */
      if (list_empty(diskthread_list))
      {
        /* LOCK */
        storio_get_serial_lock(&p->serial_lock);
	if (list_empty(&p->serial_pending_request))
	{
	  /*
	  ** no more request to process-> go the IDLE state for that context
	  */
	  p->serial_is_running=0;
          /* UNLOCK */
	  pthread_rwlock_unlock(&p->serial_lock);
	  
	  return 1;	
	}
	/*
	** there is some more request to process
	*/
        list_move_nocheck(diskthread_list,&p->serial_pending_request);
        /* UNLOCK */
	pthread_rwlock_unlock(&p->serial_lock);	
	/*
	** need to re-order the diskthread_list
	*/
	goto reorder;
      }
      /*
      ** the disk thread list is not empty: nothing to re-order
      */
      return 0;
   }
   /*
   ** there are new requests pending
   */
   /* LOCK */
   storio_get_serial_lock(&p->serial_lock);
   /*
   ** append the pending list to the disk thread list
   */
   list_move_nocheck(diskthread_list,&p->serial_pending_request);
   /* UNLOCK */
   pthread_rwlock_unlock(&p->serial_lock);	
   /*
   ** need to re-order the diskthread_list
   */
   goto reorder;
   
   /*
   ** re-ordering of the requests 
   */
reorder:
#warning reorder of requests not needed->done at rozofsmount level by doing serialisation at FID level by marking the READ opcode
   return 0;
}


/*
**_______________________________________________________
*/

/**
  That function is intended to be used by the main thread
  
  @param p: pointer the FID context that contains the requests lisk
  @param request: pointer to the request to append to the FID context

  @retval 0: FID context already active
  @retval 1: not to post a message to activate the FID context 
*/
int storio_insert_pending_request_list(storio_device_mapping_t *p,list_t *request)
{
   int need2activate = 0;
   list_init(request);
    /* LOCK */
    storio_get_serial_lock(&p->serial_lock);
    if (p->serial_is_running==0)
    {
      p->serial_is_running=1;
      need2activate = 1;
    }
    list_push_back(&p->serial_pending_request,request);
    pthread_rwlock_unlock(&p->serial_lock);

    return need2activate;
}


