
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
#include "storage_header.h"


/**
* Attributes cache constants
*/
#define STORIO_DEVICE_MAPPING_LVL0_SZ_POWER_OF_2  12
extern int STORIO_DEVICE_MAPPING_MAX_ENTRIES;

#define STORIO_DEVICE_MAPPING_LVL0_SZ  (1 << STORIO_DEVICE_MAPPING_LVL0_SZ_POWER_OF_2) 
#define STORIO_DEVICE_MAPPING_LVL0_MASK  (STORIO_DEVICE_MAPPING_LVL0_SZ-1)

extern uint32_t STORIO_DEVICE_PERIOD;

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

typedef struct _storio_device_mapping_key_t
{
  fid_t                fid;
  uint8_t              cid;
  uint8_t              sid;
} storio_device_mapping_key_t;
  
  
/*
** Depending on small_device_array field in storio_device_mapping_t
** 1 only the very first devices are stored in the context
** 0 a pointer to a big list of ROZOFS_STORAGE_MAX_CHUNK_PER_FILE devices is stored
*/ 
#define ROZOFS_MAX_SMALL_ARRAY_CHUNK  7 // Maximum chunk number in the small list
typedef union _storio_device_u {
    uint8_t * ptr;                                    // Pointer to a big array of ROZOFS_STORAGE_MAX_CHUNK_PER_FILE devices
    uint8_t   small[ROZOFS_MAX_SMALL_ARRAY_CHUNK+1];  // A small array of the first (ROZOFS_MAX_SMALL_ARRAY_CHUNK+1) devices
} storio_device_u;

#define ROZOFS_FIDCTX_QUEUE_NB      (8)
#define ROZOFS_FIDCTX_DEFAULT_QUEUE (0)
typedef struct _storio_device_mapping_t
{
  list_t               link;  
//  uint32_t             padding:4;
  uint32_t             recycle_cpt:2;
  uint32_t             small_device_array:1;    // how to read device union
  uint32_t             device_unknown:1;        // Set to 1 when device distr. is unknown
  uint32_t             index:24;
  storio_device_mapping_key_t key;
  storio_device_u      device;                  // List of devices per chunk number
  /*
  ** storio serialise
  */  
  uint8_t              serial_is_running[ROZOFS_FIDCTX_QUEUE_NB];  /**< Whether the queues are active   */
  uint8_t              nextReadQ;                                  /**< To spread the extra reads evenly on the occupied queues */
  list_t               serial_pending_request[ROZOFS_FIDCTX_QUEUE_NB];  /**< list the pending request for the FID   */
  pthread_rwlock_t     serial_lock;             /**< lock associated with serial_pending_request list & running flag     */
    
  STORIO_REBUILD_REF_U storio_rebuild_ref;
} storio_device_mapping_t;

list_distributor_t storio_device_mapping_ctx_distributor;
list_t             storio_device_mapping_ctx_initialized_list;



/*
**_______________________________________________________________________
** Free the device mapping in the FID ctx
**
** @param p       The device mapping context
**
*/
static inline void storio_free_dev_mapping(storio_device_mapping_t * p) {
  uint8_t  * ptr = NULL;;

  /*
  ** Save big array pointer to release it later
  */    
  if (!p->device_unknown && !p->small_device_array) {
    ptr = p->device.ptr;
  }  
  
  p->device_unknown     = 1;
  p->small_device_array = 1;
  p->device.ptr         = NULL;  

  /*
  ** Free big array 
  */
  if (ptr) {
    xfree(ptr);
    ptr = NULL;
  }  
}
/*
**_______________________________________________________________________
** Get the device number stored in the mapping context for a given chunk
**
** @param p       The device mapping context
** @param chunk   The chunk for which we are looking for the device
**
** @retval the device of this chunk
*/
static inline uint8_t storio_get_dev(storio_device_mapping_t * p, int chunk) {
 
  /*
  ** Distribution is unkown yet
  */
  if (p->device_unknown) return ROZOFS_UNKNOWN_CHUNK;
  if (chunk<0)           return ROZOFS_UNKNOWN_CHUNK;
 
  /*
  ** Distribution is known and is stored in a small device list
  */
  if (p->small_device_array) {
    if (chunk > ROZOFS_MAX_SMALL_ARRAY_CHUNK) return ROZOFS_EOF_CHUNK;
    return p->device.small[chunk];
  }

  /*
  ** Distribution is know and is stored in a big device list
  */
  if (chunk >= ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) return ROZOFS_EOF_CHUNK;
  return p->device.ptr[chunk];  
}
/*
**_______________________________________________________________________
** Get the last significant chunk number 
**
** @param p       The device mapping context
** @param dev     The device of the last chunk when chunk is valid
**
** @retval the last valid chunk
*/
static inline int storio_get_last_chunk(storio_device_mapping_t * p,
                                        uint8_t                 * dev) {
  int        chunk;
  uint8_t * pDev;
  
  /*
  ** Distribution is unkown yet
  */
  if (p->device_unknown) return -1;
  
  /*
  ** Distribution is known and is stored in a small device list
  */  
  if (p->small_device_array) {
    chunk = ROZOFS_MAX_SMALL_ARRAY_CHUNK;
    pDev  = p->device.small;
  }
  /*
  ** Distribution is known and is stored in a big device list
  */  
  else {
    chunk = ROZOFS_STORAGE_MAX_CHUNK_PER_FILE-1;
    pDev  = p->device.ptr;
  }
  while (chunk>=0) {
    if (pDev[chunk] != ROZOFS_EOF_CHUNK) break;
    chunk--;
  }
  if (chunk>=0) {
    *dev = pDev[chunk];
  }  
  return chunk;
}
/*
**_______________________________________________________________________
** Store distribution from disk into the FID mapping context
**
** @param p           The device mapping context
** @param devices     The device array from disk
**
** @retval the last valid chunk
*/
static inline void storio_store_to_ctx(storio_device_mapping_t * p, uint32_t nbChunks, uint8_t * devices) {
  uint8_t  * ptr = NULL;
    
  
  /*
  ** Few valid chunks
  */
  if (nbChunks <= (ROZOFS_MAX_SMALL_ARRAY_CHUNK+1)) {

    /*
    ** Save big array pointer to release it later
    */    
    if (!p->device_unknown && !p->small_device_array) {
      ptr = p->device.ptr;
      p->device_unknown = 1;
    }

    /*
    ** Save localy the chunk distribution
    */
    p->small_device_array = 1;
    memcpy(p->device.small, devices, ROZOFS_MAX_SMALL_ARRAY_CHUNK+1);
    p->device_unknown = 0;
       
    /*
    ** Free big array 
    */
    if (ptr) {
      xfree(ptr);
      ptr = NULL;
    }  
    return;
  }
 
  /*
  ** Need a big array
  */  
  if (!p->device_unknown && !p->small_device_array) {
    ptr = p->device.ptr;
  }
  else {  
    ptr = xmalloc(ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);
  }
  /*
  ** Copy the device distribution
  */
  memcpy(ptr, devices, ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);
  p->small_device_array = 0;
  p->device.ptr        = ptr;
  p->device_unknown = 0;
}
/*
**_______________________________________________________________________
** Store distribution from FID mapping context to a big device array
**
** @param p           The device mapping context
** @param devices     The device array from disk
**
** @retval the last valid chunk
*/
static inline void storio_read_from_ctx(storio_device_mapping_t * p, uint8_t * nbChunk, uint8_t * devices) {
  int idx;
  /*
  ** Distribution is unknown
  */
  if (p->device_unknown) {
    memset(devices,ROZOFS_EOF_CHUNK,ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);
    * nbChunk = ROZOFS_STORAGE_MAX_CHUNK_PER_FILE;
    return;
  } 
  
  /*
  ** Few valid chunks
  */
  if (p->small_device_array) {
    * nbChunk = rozofs_st_header_roundup_chunk_number(ROZOFS_MAX_SMALL_ARRAY_CHUNK+1);
    memcpy(devices, p->device.small, ROZOFS_MAX_SMALL_ARRAY_CHUNK+1);
    memset(&devices[ROZOFS_MAX_SMALL_ARRAY_CHUNK+1], ROZOFS_EOF_CHUNK, ROZOFS_STORAGE_MAX_CHUNK_PER_FILE-ROZOFS_MAX_SMALL_ARRAY_CHUNK-1);
    return;
  }
  
  /*
  ** Copy devices until EOF
  */
  for (idx = 0; idx < ROZOFS_STORAGE_MAX_CHUNK_PER_FILE; idx ++) {
    devices[idx] = p->device.ptr[idx];
    if (devices[idx] == ROZOFS_EOF_CHUNK) {
      break;
    }
  }
  * nbChunk = rozofs_st_header_roundup_chunk_number(idx);
  /*
  ** Set rest of array to EOF
  */
  memset(&devices[idx], ROZOFS_EOF_CHUNK, (ROZOFS_STORAGE_MAX_CHUNK_PER_FILE-idx));
}



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
* Reset a storio device_mapping context

  @param p the device_mapping context to initialize
 
*/
static inline void storio_device_mapping_ctx_reset(storio_device_mapping_t * p) {
  int idx;
  
  memset(&p->key,0,sizeof(storio_device_mapping_key_t));
  p->device_unknown     = 1;
  p->small_device_array = 1;
  p->device.ptr         = NULL;
//  p->consistency   = storio_device_mapping_stat.consistency;
  for (idx=0; idx<ROZOFS_FIDCTX_QUEUE_NB; idx++) { 
    list_init(&p->serial_pending_request[idx]);
    p->serial_is_running[idx] = 0;
  }  
  p->nextReadQ = 1;
  p->storio_rebuild_ref.u64 = 0xFFFFFFFFFFFFFFFF;
}
/*
**______________________________________________________________________________
*/
/**
* Refresh context in the list of allocated context when used

  @param p : pointer to the user cache entry   
*/
static inline void storio_device_mapping_refresh(storio_device_mapping_t *p) {
  list_remove(&p->link);        
  list_push_back(&storio_device_mapping_ctx_initialized_list,&p->link);   
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
  int idx;

  storio_device_mapping_stat.release++;
  
  /*
  ** Release the cache entry
  */
  hash = storio_device_mapping_hash32bits_compute(&p->key);
  if (storio_fid_cache_remove(hash, &p->key)==-1) {
    severe("storio_fid_cache_remove");
  }
     
  for (idx=0; idx<ROZOFS_FIDCTX_QUEUE_NB; idx++) { 
    if ((p->serial_is_running[idx])||(!list_empty(&p->serial_pending_request[idx])))
    {
      severe("storio_device_mapping_ctx_free but queue %d ctx is running",idx);
    }
  }  
   
  /*
  ** Unchain the context
  */
  list_remove(&p->link);  
   /*
  ** Free pointer to the devices if any
  */
  storio_free_dev_mapping(p);
   
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
    if (list_empty(&storio_device_mapping_ctx_initialized_list)) {
      storio_device_mapping_stat.out_of_ctx++;
      return NULL;
    }
    p = list_first_entry(&storio_device_mapping_ctx_initialized_list,storio_device_mapping_t, link);
    storio_device_mapping_release_entry(p);
  }    

  p = list_first_entry(&storio_device_mapping_ctx_distributor.list,storio_device_mapping_t, link);
  storio_device_mapping_ctx_reset(p);

  storio_device_mapping_stat.allocation++;
  
  /*
  ** Default is to create the context in running mode
  */
  storio_device_mapping_refresh(p);
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
  list_init(&storio_device_mapping_ctx_initialized_list);
  
  /*
  ** Reset stattistics 
  */
  memset(&storio_device_mapping_stat, 0, sizeof(storio_device_mapping_stat));

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
** - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
*
* Faulty FID table to recorda list of failed FID
*
** - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
*/

/*
**____________________________________________________
**
** Register the FID that has encountered an error
**  
** @param NS       UNUSED
** @param cid      the faulty cid 
** @param sid      the faulty sid
** @param fid      the FID in fault   
*/
void storio_register_faulty_fid(int NS, uint8_t cid, uint8_t sid, fid_t fid) ;
/*
**____________________________________________________
** Reset the Faulty FID table
**
*/
void storio_clear_faulty_fid() ;
/*
**____________________________________________________
**
** Initialize the table of faulty FID at start up
**
*/
void storio_faulty_fid_init();







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
