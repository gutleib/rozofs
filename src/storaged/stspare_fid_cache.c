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
 <http://www.storage_fid_debuggnu.org/licenses/>.
 */
#define _XOPEN_SOURCE 500

#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h>
#include <uuid/uuid.h>
#include <sys/vfs.h> 
#include <pthread.h> 
#include <sys/wait.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/list.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/profile.h>
#include <rozofs/common/mattr.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/ruc_timer_api.h>
#include <rozofs/core/rozofs_string.h>

#include "stspare_fid_cache.h"
#include "storage.h"

stspare_fid_cache_stat_t       stspare_fid_cache_stat = {0};
 
ruc_obj_desc_t                 stspare_fid_cache_running_list;
stspare_fid_cache_t          * stspare_fid_cache_free_list;

uint32_t STSPARE_FID_CACHE_MAX_ENTRIES = 0;

stspare_fid_cache_t          * stspare_fid_cache_next_list_entry = NULL;


/*
**____________________________________________________
**
** Debug 
**____________________________________________________  
*/
char * stspare_fid_cache_display(char * pChar, stspare_fid_cache_t * fidCtx, time_t now) {
  uint32_t  mask;
  int       first=1;
  int       idx;
  
  if (fidCtx == NULL) return pChar;

  pChar += rozofs_string_append(pChar,"    { \"cid\":");
  pChar += rozofs_u32_append(pChar,fidCtx->data.key.cid);
  pChar += rozofs_string_append(pChar,", \"sid\":");
  pChar += rozofs_u32_append(pChar,fidCtx->data.key.sid);
  pChar += rozofs_string_append(pChar,", \"fid\":\"");
  pChar += rozofs_fid_append(pChar,fidCtx->data.key.fid);
  pChar += rozofs_string_append(pChar,"\", \"modified\":");
  pChar += rozofs_u32_append(pChar,now-fidCtx->data.mtime);
  pChar += rozofs_string_append(pChar," , \"projections\":[\n");
  mask=1;
  for (idx=0; idx<32; idx++) {
    if (fidCtx->data.prj_bitmap & mask) {
      if (first) first = 0;
      else {  
        pChar += rozofs_string_append(pChar,",\n");
      }
      pChar += rozofs_string_append(pChar,"         { \"pjrId\":");
      pChar += rozofs_u32_append(pChar,idx);
      pChar += rozofs_string_append(pChar,", \"sid\":");
      pChar += rozofs_u32_append(pChar,fidCtx->data.dist[idx]);
      pChar += rozofs_string_append(pChar,", \"start\":");
      pChar += rozofs_u32_append(pChar,fidCtx->data.prj[idx].start);
      pChar += rozofs_string_append(pChar,", \"stop\":");
      pChar += rozofs_u32_append(pChar,fidCtx->data.prj[idx].stop);
      pChar += rozofs_string_append(pChar,"}");      
    }
    mask = mask << 1;
  }
  pChar += rozofs_string_append(pChar,"\n      ]\n    }");
  return pChar;
}  
/*
**____________________________________________________
**
** Man
**____________________________________________________
*/
void stspare_fid_cache_man(char * pChar) {
  pChar += rozofs_string_append           (pChar,"Display information related to the FID cache\n");
  pChar += rozofs_string_append_underscore(pChar,"\nUsage:\n");
  pChar += rozofs_string_append_bold      (pChar,"\tfid");     
  pChar += rozofs_string_append           (pChar,"\t\tdisplay cache statistics\n");     
  pChar += rozofs_string_append_bold      (pChar,"\tfid first");     
  pChar += rozofs_string_append           (pChar,"\tdisplay a first list of FID in the cache\n");     
  pChar += rozofs_string_append_bold      (pChar,"\tfid next");     
  pChar += rozofs_string_append           (pChar,"\tdisplay the next list of FID in the cache\n");     
  pChar += rozofs_string_append_underscore(pChar,"\nDisplay statistics:\n");
  pChar += rozofs_string_append_bold      (pChar,"statistics\n");
  pChar += rozofs_string_append_bold      (pChar,"\tcount ");
  pChar += rozofs_string_append           (pChar,"\t# of entries used in the cache\n");  
  pChar += rozofs_string_append_bold      (pChar,"\thit");
  pChar += rozofs_string_append           (pChar,"\t# of cache hit\n");
  pChar += rozofs_string_append_bold      (pChar,"\tmiss");
  pChar += rozofs_string_append           (pChar,"\t# of cache miss\n");
  pChar += rozofs_string_append_bold      (pChar,"\tbkts");
  pChar += rozofs_string_append           (pChar,"\t# of allocated sub buckets\n");  
  pChar += rozofs_string_append_bold      (pChar,"\tmxbkt");
  pChar += rozofs_string_append           (pChar,"\tmaximum # of sub buckets in a row\n");  
  pChar += rozofs_string_append_bold      (pChar,"\tmxcol");
  pChar += rozofs_string_append           (pChar,"\tmaximum collision count encountered\n");  
  pChar += rozofs_string_append_bold      (pChar,"usage\n");
  pChar += rozofs_string_append_bold      (pChar,"\tentries\t");
  pChar += rozofs_string_append           (pChar,"\tmaximum # of entries of the cache\n");  
  pChar += rozofs_string_append_bold      (pChar,"\tsize\t");
  pChar += rozofs_string_append           (pChar,"\tsize of an entry in the cache\n");  
  pChar += rozofs_string_append_bold      (pChar,"\ttotal\t");
  pChar += rozofs_string_append           (pChar,"\ttotal cache size\n");  
  pChar += rozofs_string_append_bold      (pChar,"\tfree\t");
  pChar += rozofs_string_append           (pChar,"\t# of free entries\n");  
  pChar += rozofs_string_append_bold      (pChar,"\tallocation");
  pChar += rozofs_string_append           (pChar,"\t# of allocation processed\n");  
  pChar += rozofs_string_append_bold      (pChar,"\trelease\t");
  pChar += rozofs_string_append           (pChar,"\t# of release processed\n");  
  pChar += rozofs_string_append_underscore(pChar,"\nDisplay list of FID:\n");
  pChar += rozofs_string_append           (pChar,"\nFor each entry of the cache\n");
  pChar += rozofs_string_append_bold      (pChar,"\tcid\t");
  pChar += rozofs_string_append           (pChar,"\tcluster identifier\n");  
  pChar += rozofs_string_append_bold      (pChar,"\tsid\t");
  pChar += rozofs_string_append           (pChar,"\tstorage identifier\n");  
  pChar += rozofs_string_append_bold      (pChar,"\tfid\t");
  pChar += rozofs_string_append           (pChar,"\tfile unic identifier\n");  
  pChar += rozofs_string_append_bold      (pChar,"\tmodified");
  pChar += rozofs_string_append           (pChar,"\tdelay in seconds since last file modification\n");  
  pChar += rozofs_string_append_bold      (pChar,"\tprojections\n");
  pChar += rozofs_string_append_bold      (pChar,"\t\tpjrId");
  pChar += rozofs_string_append           (pChar,"\tprojection identifier present in the file\n");  
  pChar += rozofs_string_append_bold      (pChar,"\t\tsid");
  pChar += rozofs_string_append           (pChar,"\tnominal storage identifier for this projection\n");  
  pChar += rozofs_string_append_bold      (pChar,"\t\tstart");
  pChar += rozofs_string_append           (pChar,"\t1rst block in the file of this projection\n");  
  pChar += rozofs_string_append_bold      (pChar,"\t\tstop");
  pChar += rozofs_string_append           (pChar,"\tlast block in the file of this projection\n");  
  pChar += rozofs_string_append_bold      (pChar,"end\t");
  pChar += rozofs_string_append           (pChar,"\t\twhether this list has a next list\n");  

}
/*
**____________________________________________________
**
** Debug 
**____________________________________________________  
*/
void stspare_fid_cache_debug(char * argv[], uint32_t tcpRef, void *bufRef) {


  char                         * pChar=uma_dbg_get_buffer();
  stspare_fid_cache_t          * p = NULL;
  int                            idx;  
  int                            first=1;

  time_t now = time(NULL);
  
  if (argv[1] == NULL) {
    pChar += rozofs_string_append(pChar,"{ \n");
  
    pChar = display_cache_fid_stat(pChar);

    pChar += rozofs_string_append(pChar,"  ,\n");
    
    pChar += rozofs_string_append(pChar,"  \"usage\" : {\n    \"entries\": ");
    pChar += rozofs_u32_append(pChar,STSPARE_FID_CACHE_MAX_ENTRIES);
    pChar += rozofs_string_append(pChar,",\n    \"size\":");
    pChar += rozofs_u32_append(pChar,sizeof(stspare_fid_cache_t));
    pChar += rozofs_string_append(pChar,",\n    \"total\":");    
    pChar += rozofs_u32_append(pChar,STSPARE_FID_CACHE_MAX_ENTRIES * sizeof(stspare_fid_cache_t));
    pChar += rozofs_string_append(pChar,",\n    \"free\":");
    pChar += rozofs_u64_append(pChar,stspare_fid_cache_stat.free);
    pChar += rozofs_string_append(pChar,",\n    \"allocation\":");
    pChar += rozofs_u64_append(pChar,stspare_fid_cache_stat.allocation);
    pChar += rozofs_string_append(pChar,",\n    \"release\":");
    pChar += rozofs_u64_append(pChar,stspare_fid_cache_stat.release);
    pChar += rozofs_string_append(pChar,"\n  }\n");
    pChar += rozofs_string_append(pChar,"}\n");

    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
    return;       
  }     
  
  if (strcmp(argv[1],"first")==0) {
    stspare_fid_cache_set1rst();
  }
  
  else if (strcmp(argv[1],"next")!=0) {
    pChar += rozofs_string_append(pChar,"Bad syntax\n");    
    uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());
    return; 
  }

  pChar += rozofs_string_append(pChar,"{\n  \"fids\":[\n");     
  for (idx=0;idx<12; idx++) {
    p = stspare_fid_cache_getnext();
    if (p==NULL) break;
    if (first) first = 0;
    else pChar += rozofs_string_append(pChar,",\n");
    pChar = stspare_fid_cache_display(pChar, p, now);
  } 
  pChar += rozofs_string_append(pChar,"\n  ],\n");  
  if (p==NULL) pChar += sprintf(pChar," \"end\": true\n");
  else         pChar += sprintf(pChar," \"end\": false\n");
  pChar += sprintf(pChar,"}\n");
  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());
  return;            
}


/*
**____________________________________________________
**
** Function to check the exact match of a given key and the one
** of FID context at a given index
**
** @param key      The key to check against the index
** @param index    The context index to check against the given key
**____________________________________________________
*/
uint32_t stspare_fid_cache_exact_match(void *key ,uint32_t index) {
  stspare_fid_cache_t   * p;  
  
  p = stspare_fid_cache_retrieve(index);
  if (p == NULL) return 0;
    
  if (memcmp(&p->data.key,key, sizeof(stspare_fid_cache_key_t)) != 0) {
    return 0;
  }
  /*
  ** Match !!
  */
  return 1;
}
/*
**____________________________________________________
**
** Function to release a FID cache context from is index
**
** @param index    The context index to release
**____________________________________________________
*/
uint32_t stspare_fid_cache_delete_req(uint32_t index) {
  stspare_fid_cache_t   * p;  
  
  /*
  ** Retrieve the context from the index
  */
  p = stspare_fid_cache_retrieve(index);
  if (p == NULL) return 0;
    
  /*
  ** Release the context 
  */  
  stspare_fid_cache_release(p);
  return 1;
}	
/*
**____________________________________________________
**
** Initialize distributor of FID context
**
** @param index    The coontext index
**____________________________________________________
*/
static inline void stspare_fid_cache_distributor_init(uint32_t nbCtx) {
  stspare_fid_cache_t          * p;
  int                            idx;


  STSPARE_FID_CACHE_MAX_ENTRIES = nbCtx*1024;

  /*
  ** Init list heads
  */
  ruc_listHdrInit(&stspare_fid_cache_running_list);
  
  /*
  ** Reset stattistics 
  */
  memset(&stspare_fid_cache_stat, 0, sizeof(stspare_fid_cache_stat));
  stspare_fid_cache_stat.free = STSPARE_FID_CACHE_MAX_ENTRIES;
  
  /*
  ** Allocate memory
  */
  stspare_fid_cache_free_list = (stspare_fid_cache_t*) ruc_listCreate(STSPARE_FID_CACHE_MAX_ENTRIES,sizeof(stspare_fid_cache_t));
  if (stspare_fid_cache_free_list == NULL) {
    /*
    ** error on distributor creation
    */
    fatal( "ruc_listCreate(%d,%d)", STSPARE_FID_CACHE_MAX_ENTRIES,(int)sizeof(stspare_fid_cache_t) );
  }
  
  
  for (idx=0; idx<STSPARE_FID_CACHE_MAX_ENTRIES; idx++) {
    p = stspare_fid_cache_retrieve(idx);
    p->index  = idx;
    p->status = STSPARE_FID_CACHE_FREE;
    stspare_fid_cache_reset(p);
  }  
}
 
/*
**____________________________________________________
**
** creation of the FID cache
** That API is intented to be called during the initialization of the module
**____________________________________________________
*/
uint32_t stspare_fid_cache_init(uint32_t nbCtx) {

  stspare_fid_cache_next_list_entry = (stspare_fid_cache_t *) &stspare_fid_cache_running_list;
  
  /*
  ** Initialize the FID cache 
  */
  storio_fid_cache_init(stspare_fid_cache_exact_match, stspare_fid_cache_delete_req);

  /*
  ** Initialize context distributor
  */
  stspare_fid_cache_distributor_init(nbCtx);
    
  /*
  ** Add a debug topic
  */
  uma_dbg_addTopicAndMan("fidCache", stspare_fid_cache_debug, stspare_fid_cache_man, 0); 
  return 0;
}
