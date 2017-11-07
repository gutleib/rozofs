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
    case STORIO_DISK_THREAD_WRITE: return "write";
    case STORIO_DISK_THREAD_TRUNCATE: return "truncate";
    case STORIO_DISK_THREAD_REMOVE: return "remove";
    case STORIO_DISK_THREAD_REMOVE_CHUNK: return "remove_chunk";
    case STORIO_DISK_THREAD_WRITE_REPAIR3: return "write repair";
    case STORIO_DISK_THREAD_REBUILD_START: return "rebuild start";
    case STORIO_DISK_THREAD_REBUILD_STOP: return "rebuild stop";
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
*/
int storio_serialization_begin(storio_device_mapping_t * dev_map_p, rozorpc_srv_ctx_t *req_ctx_p) {   

  /*
  ** Waiting queue is empty. If running queue too, please go
  */
  if (list_empty(&dev_map_p->running_request)) { 
    return storio_serialization_direct_run(dev_map_p,req_ctx_p);
  }  

  /*
  ** Waiting queue is empty and some requests are running
  */  
  return storio_serialization_wait(dev_map_p,req_ctx_p);    
}
/*
**___________________________________________________________
*/
void storio_serialization_end(storio_device_mapping_t * dev_map_p, rozorpc_srv_ctx_t *req_ctx_p) {	
  uint64_t            toc;    
  struct timeval      tv;
  rozorpc_srv_ctx_t * req;
  
  if (dev_map_p == NULL) return;
  
  /*
  ** Remove this request
  */
  list_remove(&req_ctx_p->list);
  
  if (!list_empty(&dev_map_p->running_request)) {
    return;
  }  
    

  if (list_empty(&dev_map_p->waiting_request)) {
    return;
  }  
   
  gettimeofday(&tv,(struct timezone *)0);
  toc = MICROLONG(tv);


  req = list_first_entry(&dev_map_p->waiting_request, rozorpc_srv_ctx_t, list);
  storio_serialization_unqueue_run(dev_map_p,req, toc);
  return;    
}
