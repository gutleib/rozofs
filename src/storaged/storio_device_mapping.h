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
 
#ifndef STORIO_DEVICE_MAPPING_H
#define STORIO_DEVICE_MAPPING_H


#ifdef __cplusplus
extern "C" {
#endif /*__cplusplus*/


#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h>
#include <uuid/uuid.h>
#include <pthread.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/list.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/profile.h>
#include <rozofs/common/mattr.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/common/list.h>
#include <rozofs/core/rozofs_string.h>

#include "storio_fid_cache.h"

//__Specific values in the chunk to device array
// Used in the interface When the FID is not inserted in the cache and so the
// header file will have to be read from disk.
#define ROZOFS_UNKNOWN_CHUNK  255
// Used in the device per chunk array when no device has been allocated 
// because the chunk is after the end of file
#define ROZOFS_EOF_CHUNK      254
// Used in the device per chunk array when no device has been allocated 
// because it is included in a whole of the file
#define ROZOFS_EMPTY_CHUNK    253

/**
* Attributes cache constants
*/
#define STORIO_DEVICE_MAPPING_LVL0_SZ_POWER_OF_2  12
extern int STORIO_DEVICE_MAPPING_MAX_ENTRIES;

#define STORIO_DEVICE_MAPPING_LVL0_SZ  (1 << STORIO_DEVICE_MAPPING_LVL0_SZ_POWER_OF_2) 
#define STORIO_DEVICE_MAPPING_LVL0_MASK  (STORIO_DEVICE_MAPPING_LVL0_SZ-1)

extern uint32_t STORIO_DEVICE_PERIOD;

void storio_clear_faulty_fid();
int storio_device_mapping_monitor_thread_start();

/*
** Possible values for reloacte field
*/
#define RBS_NO_RELOCATE 0
#define RBS_TO_RELOCATE 1
#define RBS_RELOCATED   2

typedef struct storio_rebuild_t {
  ruc_obj_desc_t      link;
  uint8_t             ref;
  uint8_t             spare;
  uint8_t             chunk;
  uint8_t             relocate;
  uint8_t             old_device;
  uint32_t            rebuild_ts;
  uint64_t            start_block;
  uint64_t            stop_block;
  fid_t               fid;
} STORIO_REBUILD_T;

#define MAX_STORIO_PARALLEL_REBUILD   64
STORIO_REBUILD_T * storio_rebuild_ctx_free_list;

typedef struct storio_rebuild_stat_s {
  uint64_t             allocated;
  uint64_t             stollen;
  uint64_t             out_of_ctx;
  uint64_t             released;
  uint64_t             aborted;
  uint64_t             lookup_hit;
  uint64_t             lookup_miss;
  uint64_t             lookup_bad_index;
  
} STORIO_REBUILD_STAT_S;
extern STORIO_REBUILD_STAT_S        storio_rebuild_stat;

#define MAX_FID_PARALLEL_REBUILD 8
typedef union storio_rebuild_ref_u {
  uint8_t     u8[MAX_FID_PARALLEL_REBUILD];
  uint64_t    u64;
} STORIO_REBUILD_REF_U;

#define    STORIO_FID_FREE     0
#define    STORIO_FID_RUNNING  1
#define    STORIO_FID_INACTIVE 2
typedef struct _storio_device_mapping_key_t
{
  fid_t                fid;
  uint8_t              cid;
  uint8_t              sid;
} storio_device_mapping_key_t;
  
typedef struct _storio_device_mapping_t
{
  list_t               link;  
  uint32_t             status:4;
  uint32_t             padding:2;
  uint32_t             recycle_cpt:2;
  uint32_t             index:24;
  storio_device_mapping_key_t key;
  uint8_t              device[ROZOFS_STORAGE_MAX_CHUNK_PER_FILE];   /**< Device number to write the data on        */
  list_t               running_request;
  list_t               waiting_request;
  /*
  ** storio serialise
  */
  list_t               serial_pending_request;  /**< list the pending request for the FID   */
  uint32_t             serial_is_running:1;     /**< assert to one when a disk thread is processing the requests         */
  pthread_rwlock_t     serial_lock;             /**< lock associated with serial_pending_request list & running flag     */
    
//  uint64_t             consistency;
  STORIO_REBUILD_REF_U storio_rebuild_ref;
} storio_device_mapping_t;

list_distributor_t storio_device_mapping_ctx_distributor;
list_t             storio_device_mapping_ctx_running_list;
list_t             storio_device_mapping_ctx_inactive_list;


typedef struct _storio_device_mapping_stat_t
{
//  uint64_t            consistency; 
  uint64_t            inactive;
  uint64_t            running;
  uint64_t            free;
  uint64_t            allocation;
  uint64_t            release;
  uint64_t            out_of_ctx;
//  uint64_t            inconsistent;   
} storio_device_mapping_stat_t;

extern storio_device_mapping_stat_t storio_device_mapping_stat;



/*
**______________________________________________________________________________
*/
/**
* fid entry hash compute 

  @param p : pointer to the user cache entry 
  
  @retval hash value
  
*/
static inline uint32_t storio_device_mapping_hash32bits_compute(storio_device_mapping_key_t *usr_key) {
  uint32_t        h = 2166136261;
  unsigned char * d = (unsigned char *) usr_key->fid;
  int             i;
  rozofs_inode_t * fake_inode_p;
  
  fake_inode_p = (rozofs_inode_t*)usr_key->fid;
  rozofs_reset_recycle_on_fid(fake_inode_p);
//  fake_inode_p->s.recycle_cpt = 0;
  
  /*
   ** hash on fid
   */
  for (i=0; i<sizeof(fid_t); i++,d++) {
    h = (h * 16777619)^ *d;
  }
  h += usr_key->sid; 
  return h;
}

/*
**______________________________________________________________________________
*/
/**
* Put the FID context in the correct list
*
* @param idx The context index
*
* @return the rebuild context address or NULL
*/
static inline int storio_device_mapping_ctx_check_running(storio_device_mapping_t * p) {
      
  if ((p->storio_rebuild_ref.u64 != 0xFFFFFFFFFFFFFFFF)
  ||  (!list_empty(&p->running_request))
  ||  (!list_empty(&p->waiting_request))) {
    return 1; 
  }
  return 0;  
}

/*
**______________________________________________________________________________
*/
/**
* Put the FID context in the correct list
*
* @param idx The context index
*
* @return the rebuild context address or NULL
*/
static inline void storio_device_mapping_ctx_evaluate(storio_device_mapping_t * p) {
   
  if (p == NULL) return;
  
   
  /*
  ** The context was not running. Check whether it is running now
  */
  if (p->status == STORIO_FID_INACTIVE) { 
 
    /*
    ** Is there any running requests
    */
    if (storio_device_mapping_ctx_check_running(p)) {
      p->status = STORIO_FID_RUNNING;
      storio_device_mapping_stat.inactive--;
      storio_device_mapping_stat.running++; 
      list_remove(&p->link);        
      list_push_back(&storio_device_mapping_ctx_running_list,&p->link); 
    }
    return;
  }
  
  /*
  ** The context was running. Check whether it is still running
  */
  if (p->status == STORIO_FID_RUNNING) { 
 
    /*
    ** Is there any running requests
    */
    if (!storio_device_mapping_ctx_check_running(p)) {
      p->status = STORIO_FID_INACTIVE;
      storio_device_mapping_stat.inactive++;
      storio_device_mapping_stat.running--; 
      list_remove(&p->link);        
      list_push_back(&storio_device_mapping_ctx_inactive_list,&p->link); 
    }
    return;
  }   
}

/*
**______________________________________________________________________________
*/
/**
* Reset a storio device_mapping context

  @param p the device_mapping context to initialize
 
*/
static inline void storio_device_mapping_ctx_reset(storio_device_mapping_t * p) {

  
  memset(&p->key,0,sizeof(storio_device_mapping_key_t));
  memset(p->device,ROZOFS_UNKNOWN_CHUNK,ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);
//  p->consistency   = storio_device_mapping_stat.consistency;
  list_init(&p->running_request);
  list_init(&p->waiting_request);
  list_init(&p->serial_pending_request);
  p->serial_is_running = 0;

  p->storio_rebuild_ref.u64 = 0xFFFFFFFFFFFFFFFF;
}

/*
**______________________________________________________________________________
*/
/**
* release an entry (called from the application)

  @param p : pointer to the user cache entry 
  
*/
static inline void storio_device_mapping_release_entry(storio_device_mapping_t *p) {
  uint32_t hash;

  storio_device_mapping_stat.release++;
  
  /*
  ** Release the cache entry
  */
  hash = storio_device_mapping_hash32bits_compute(&p->key);
  if (storio_fid_cache_remove(hash, &p->key)==-1) {
    severe("storio_fid_cache_remove");
  }
     

  if (storio_device_mapping_ctx_check_running(p)) {
    severe("storio_device_mapping_ctx_free but ctx is running");
  }
   
  /*
  ** The context was not running. Check whether it is running now
  */
  if (p->status == STORIO_FID_INACTIVE) { 
    storio_device_mapping_stat.inactive--;
  }
  
  /*
  ** The context was running. Check whether it is still running
  */
  else if (p->status == STORIO_FID_RUNNING) { 
    storio_device_mapping_stat.running--;
  }
   

  storio_device_mapping_stat.free++;  

  /*
  ** Unchain the context
  */
  list_remove(&p->link);  
    
  /*
  ** Put it in the free list
  */    
  list_push_front(&storio_device_mapping_ctx_distributor.list,&p->link);
} 

/*
**______________________________________________________________________________
*/
/**
* Allocate a storio device_mapping context from the distributor

 
 @retval the pointer to the device_mapping context or NULL in case of error
*/
static inline storio_device_mapping_t * storio_device_mapping_ctx_allocate() {
  storio_device_mapping_t * p;
  
  /*
  ** No free context
  */
  if (list_empty(&storio_device_mapping_ctx_distributor.list)) {
    /*
    ** No more free context. Let's recycle an unused one
    */
    if (list_empty(&storio_device_mapping_ctx_inactive_list)) {
      storio_device_mapping_stat.out_of_ctx++;
      return NULL;
    }
    p = list_first_entry(&storio_device_mapping_ctx_inactive_list,storio_device_mapping_t, link);
    storio_device_mapping_release_entry(p);
  }    

  p = list_first_entry(&storio_device_mapping_ctx_distributor.list,storio_device_mapping_t, link);
  storio_device_mapping_ctx_reset(p);

  /*
  ** Default is to create the context in running mode
  */
  p->status  = STORIO_FID_RUNNING;   
  storio_device_mapping_stat.free--;
  storio_device_mapping_stat.running++; 
  storio_device_mapping_stat.allocation++;
  list_remove(&p->link);        
  list_push_back(&storio_device_mapping_ctx_running_list,&p->link);   
  return p;
}
/*
**______________________________________________________________________________
*/
/**
* Retrieve a context from its index
*
* @param idx The context index
*
* @return the rebuild context address or NULL
*/
static inline storio_device_mapping_t * storio_device_mapping_ctx_retrieve(int idx) {
  storio_device_mapping_t * p;

  p = (storio_device_mapping_t*) list_distributor_get(&storio_device_mapping_ctx_distributor,idx);  
  if (p == NULL) {
    warning("Bad device mapping context index %d/%d",idx,storio_device_mapping_ctx_distributor.nb_ctx);
  }  
  return p;  
}

/*
**______________________________________________________________________________
*/
/**
* Initialize the storio device_mapping context distributor

 
 retval 0 on success
 retval < 0 on error
*/
static inline void storio_device_mapping_ctx_distributor_init(int nbCtx) {
  storio_device_mapping_t * p;
  int                       idx;


  STORIO_DEVICE_MAPPING_MAX_ENTRIES = nbCtx * 1024;

  /*
  ** Init list heads
  */
  list_init(&storio_device_mapping_ctx_running_list);
  list_init(&storio_device_mapping_ctx_inactive_list);
  
  /*
  ** Reset stattistics 
  */
  memset(&storio_device_mapping_stat, 0, sizeof(storio_device_mapping_stat));
  storio_device_mapping_stat.free = STORIO_DEVICE_MAPPING_MAX_ENTRIES;
  
  /*
  ** Allocate memory
  */
  list_distributor_create((&storio_device_mapping_ctx_distributor), 
                          STORIO_DEVICE_MAPPING_MAX_ENTRIES, 
                          storio_device_mapping_t, 
                          link);  
  
  for (idx=0; idx<STORIO_DEVICE_MAPPING_MAX_ENTRIES; idx++) {
    p = storio_device_mapping_ctx_retrieve(idx);
    pthread_rwlock_init(&p->serial_lock, NULL);
    p->index  = idx;
    p->status = STORIO_FID_FREE;
    storio_device_mapping_ctx_reset(p);
  }  
}


/*
**______________________________________________________________________________
*/
/**
* Insert an entry in the cache if it does not yet exist
* 
*  @param fid the FID
*  @param device_id The device number 
*
*/
static inline storio_device_mapping_t * storio_device_mapping_insert(uint8_t cid, uint8_t sid, void * fid) {
  storio_device_mapping_t            * p;  
  uint32_t hash;
  rozofs_inode_t * fake_inode_p;
  
  /*
  ** allocate an entry
  */
  p = storio_device_mapping_ctx_allocate();
  if (p == NULL) {
    errno = ENOMEM;
    return NULL;
  }
  fake_inode_p = (rozofs_inode_t*)fid;
  p->key.cid = cid;
  p->key.sid = sid;  
  memcpy(&p->key.fid,fid,sizeof(fid_t));
  p->recycle_cpt = rozofs_get_recycle_from_fid(fake_inode_p);
//  p->recycle_cpt = fake_inode_p->s.recycle_cpt;

  hash = storio_device_mapping_hash32bits_compute(&p->key);  
  if (storio_fid_cache_insert(hash, p->index) != 0) {
     severe("storio_fid_cache_insert"); 
     storio_device_mapping_release_entry(p);
     return NULL;
  }
  return p;
}
/*
**______________________________________________________________________________
* Search an entry in the FID cache 
* 
*
*  @param cid  The cluster identifier
*  @param sid  The storage identifier within the cluster
*  @param fid  the FID
*  @param same whether the recycling counter is the same or not in the context
*              as the given one
*
*  @retval found entry or NULL
*/
static inline storio_device_mapping_t * storio_device_mapping_search(uint8_t cid, uint8_t sid, void * fid, int * same) {
  storio_device_mapping_t   * p;  
  uint32_t hash;
  uint32_t index;
  storio_device_mapping_key_t key;

  key.cid = cid;
  key.sid = sid;
  memcpy(key.fid,fid,sizeof(key.fid));
  
  hash = storio_device_mapping_hash32bits_compute(&key);

  /*
  ** Lookup for an entry
  */
  index = storio_fid_cache_search(hash, &key) ;
  if (index == -1) {
    return NULL;
  }
 
  p = storio_device_mapping_ctx_retrieve(index);

  /*
  ** Check whether the recycle counter is the same
  */
  if (rozofs_get_recycle_from_fid(fid) != p->recycle_cpt) {
    *same = 0;
  }
  else {
    *same = 1;
  } 
  return p;
}
/*
**______________________________________________________________________________
*/
/**
* creation of the FID cache
 That API is intented to be called during the initialization of the module
 
 The max number of entries is given the storio_device_mapping_MAX_ENTRIES constant
 and the size of the level 0 entry set is given by storio_device_mapping_LVL0_SZ_POWER_OF_2 constant
 
 retval 0 on success
 retval < 0 on error
*/
 
uint32_t storio_device_mapping_init();
/*
**____________________________________________________
*/
/*
* Register the FID that has encountered an error
  
   @param threadNb the thread number
   @param cid      the faulty cid 
   @param sid      the faulty sid      
   @param fid      the FID in fault   
*/
void storio_register_faulty_fid(int threadNb, uint8_t cid, uint8_t sid, fid_t fid) ;









/*
** - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
*
* Rebuild contexts
*
** - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
*/






/*
**______________________________________________________________________________
*/
/**
* Reset a storio rebuild context

  @param p the rebuild context to initialize
 
*/
static inline void storio_rebuild_ctx_reset(STORIO_REBUILD_T * p) {
  ruc_objRemove(&p->link);  
  p->rebuild_ts  = 0;
  p->spare       = 0;
  p->relocate    = RBS_NO_RELOCATE;
  p->chunk       = ROZOFS_UNKNOWN_CHUNK;
  p->old_device  = ROZOFS_UNKNOWN_CHUNK;
  p->start_block = 0;
  p->stop_block  = 0;
  memset(p->fid,0, sizeof(fid_t));
}
/*
**______________________________________________________________________________
*/
/**
* Free a storio rebuild context 
*/
static inline void storio_rebuild_ctx_free(STORIO_REBUILD_T * p) {
  storio_rebuild_stat.released++;
  storio_rebuild_ctx_reset(p);
  ruc_objInsert(&storio_rebuild_ctx_free_list->link,&p->link);
}
/*
**______________________________________________________________________________
*/
/**
* A rebuild context is beeing stollen
 
 @retval the pointer to the rebuild context or NULL in case of error
*/
static inline void storio_rebuild_ctx_stollen(STORIO_REBUILD_T * p, uint32_t delay) {
  char fid_string[128];

  storio_rebuild_stat.stollen++;

  /*
  ** When a relocation of a device was running, we should try to restore the
  ** the old_device when possible !!!
  */
  
  rozofs_uuid_unparse(p->fid,fid_string);    
  severe("rebuild stollen FID %s spare %d relocate %d chunk %d old device %d delay %u",
            fid_string, p->spare, p->relocate, p->chunk, p->old_device, delay);
  
  storio_rebuild_ctx_reset(p);
}
/*
**______________________________________________________________________________
*/
/**
* A rebuild context is beeing aborted
 
 @retval the pointer to the rebuild context or NULL in case of error
*/
static inline void storio_rebuild_ctx_aborted(STORIO_REBUILD_T * p, uint32_t delay) {
  char fid_string[128];

  storio_rebuild_stat.aborted++;

  /*
  ** When a relocation of a device was running, we should try to restore the
  ** the old_device when possible !!!
  */
  
  rozofs_uuid_unparse(p->fid,fid_string);    
  severe("rebuild aborted FID %s spare %d relocate %d chunk %d old device %d delay %u",
            fid_string, p->spare, p->relocate, p->chunk, p->old_device, delay);
  
  storio_rebuild_ctx_free(p);
}
/*
**______________________________________________________________________________
*/
/**
* Free a storio rebuild context 
*
* @param idx The context index
* @param fid The fid the context should rebuild or NULL 
*
* @return the rebuild context address or NULL
*/
static inline STORIO_REBUILD_T * storio_rebuild_ctx_retrieve(int idx, char * fid) {
  STORIO_REBUILD_T * p;
 
  if (idx>=MAX_STORIO_PARALLEL_REBUILD) {
    storio_rebuild_stat.lookup_bad_index++;
    return NULL;
  }  

  p = (STORIO_REBUILD_T*) ruc_objGetRefFromIdx(&storio_rebuild_ctx_free_list->link,idx);  
  /*
  ** Check FID if any is given as input 
  */
  if (fid == NULL) {
    return p;
  }
  
  if (memcmp(fid,p->fid,sizeof(fid_t)) == 0) {
    storio_rebuild_stat.lookup_hit++;
    return p; 
  }  
  
  return NULL;  
}
/*
**______________________________________________________________________________
*/
/**
* Allocate a storio rebuild context from the distributor

 
 @retval the pointer to the rebuild context or NULL in case of error
*/
static inline STORIO_REBUILD_T * storio_rebuild_ctx_allocate() {
  STORIO_REBUILD_T * p;
  int                storio_rebuild_ref;
  uint32_t           delay;
  
  /*
  ** Get first free context
  */
  p = (STORIO_REBUILD_T*) ruc_objGetFirst(&storio_rebuild_ctx_free_list->link);
  if (p != NULL) {
    storio_rebuild_stat.allocated++;
    storio_rebuild_ctx_reset(p);  
    return p;
  }

  /* No Free context found. Let's check whether a context is old enough to be stollen */
  uint32_t ts = time(NULL);

  /*
  ** Look for a non spare file rebuild that has not been written for some seconds
  */  
  for (storio_rebuild_ref=0; storio_rebuild_ref<MAX_STORIO_PARALLEL_REBUILD; storio_rebuild_ref++,p++) {
    p = storio_rebuild_ctx_retrieve(storio_rebuild_ref, NULL);
    if (p->spare) continue;

    delay = ts - p->rebuild_ts;
    if (delay > 20) {
      storio_rebuild_ctx_stollen(p,delay);  
      return p;      
    }
  }

  /*
  ** Look for a spare file rebuild that has not been written for some minutes
  */  
  for (storio_rebuild_ref=0; storio_rebuild_ref<MAX_STORIO_PARALLEL_REBUILD; storio_rebuild_ref++,p++) {
    p = storio_rebuild_ctx_retrieve(storio_rebuild_ref, NULL);
    if (!p->spare) continue;

    delay = ts - p->rebuild_ts;
    if (delay > (5*60)) {
      storio_rebuild_ctx_stollen(p,delay);  
      return p;      
    }
  }  
  
  storio_rebuild_stat.out_of_ctx++;
  return NULL;
}

/*
**______________________________________________________________________________
*/
/**
* Initialize the storio rebuild context distributor

 
 retval 0 on success
 retval < 0 on error
*/
static inline void storio_rebuild_ctx_distributor_init() {
  STORIO_REBUILD_T * p;
  uint8_t            storio_rebuild_ref;


  /*
  ** Allocate memory
  */
  storio_rebuild_ctx_free_list = (STORIO_REBUILD_T*) ruc_listCreate(MAX_STORIO_PARALLEL_REBUILD,sizeof(STORIO_REBUILD_T));
  if (storio_rebuild_ctx_free_list == NULL) {
    /*
    ** error on distributor creation
    */
    fatal( "ruc_listCreate(%d,%d)", MAX_STORIO_PARALLEL_REBUILD,(int)sizeof(STORIO_REBUILD_T) );
  }
  
  p = (STORIO_REBUILD_T*) ruc_objGetRefFromIdx(&storio_rebuild_ctx_free_list->link,0);
  for (storio_rebuild_ref=0; storio_rebuild_ref<MAX_STORIO_PARALLEL_REBUILD; storio_rebuild_ref++,p++) {
    p->ref = storio_rebuild_ref;
    storio_rebuild_ctx_free(p); 
  }
  storio_rebuild_stat.released = 0;
}



#ifdef __cplusplus
}
#endif /*__cplusplus */

#endif
