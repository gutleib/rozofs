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
 
#ifndef STSPARE_FID_CACHE_H
#define STSPARE_FID_CACHE_H


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

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/list.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/profile.h>
#include <rozofs/common/mattr.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/core/rozofs_string.h>

#include "storio_fid_cache.h"

/**
* Attributes cache constants
*/
#define STSPARE_FID_CACHE_LVL0_SZ_POWER_OF_2  12
extern uint32_t STSPARE_FID_CACHE_MAX_ENTRIES;

#define STSPARE_FID_CACHE_LVL0_SZ  (1 << STSPARE_FID_CACHE_LVL0_SZ_POWER_OF_2) 
#define STSPARE_FID_CACHE_LVL0_MASK  (STSPARE_FID_CACHE_LVL0_SZ-1)


/*
** Status of a FID cache context
*/
#define    STSPARE_FID_CACHE_FREE     0
#define    STSPARE_FID_CACHE_RUNNING  1
#define    STSPARE_FID_CACHE_INACTIVE 2


/*
** The key to access the FID cache context
*/
typedef struct _stspare_fid_cache_key_t
{
  fid_t                fid;
  uint8_t              chunk;
  uint8_t              cid;
  uint8_t              sid;
} stspare_fid_cache_key_t;
  
/*
** Information related to a projection found in the spare file
*/  
typedef struct _stspare_fid_cache_prj_t {
  uint32_t    start;   /* 1rst blok with this projection id */
  uint32_t    stop;    /* Last block with this projection id */
} stspare_fid_cache_prj_t;

#define STSPARE_SAFE_MAX (ROZOFS_SAFE_MAX/2)

/*
** User data in the FID cache cobtext
*/
typedef struct _stspare_fid_data_t
{
  stspare_fid_cache_key_t key;             //< Access key in hash table
  time_t                  mtime;           //< Last knowned modification time
  uint8_t                 layout;          //< Layout of the file 
  uint8_t                 bsize;           //< block size of the file
  uint8_t                 nb_prj;          //< Number of prj number found include wholes
  uint8_t                 pad;             //< Padding
  uint32_t                prj_bitmap;      //< Bitmap of projections in file
  sid_t                   dist[ROZOFS_SAFE_MAX]; //< file distribution
  stspare_fid_cache_prj_t prj[STSPARE_SAFE_MAX];
} stspare_fid_data_t;

/*
** FID cache cntext
*/
typedef struct _stspare_fid_cache_t
{
  ruc_obj_desc_t                    link;  
  uint32_t                          status:8;
  uint32_t                          index:24;
  stspare_fid_data_t                data;
} stspare_fid_cache_t;

extern ruc_obj_desc_t                 stspare_fid_cache_running_list;
extern stspare_fid_cache_t          * stspare_fid_cache_free_list;

typedef struct _stspare_fid_cache_stat_t
{
  uint64_t            free;
  uint64_t            allocation;
  uint64_t            release;
} stspare_fid_cache_stat_t;

extern stspare_fid_cache_stat_t   stspare_fid_cache_stat;
extern stspare_fid_cache_t      * stspare_fid_cache_next_list_entry;
 

/*
**____________________________________________________
**
** fid entry hash compute 
**
** @param usr_key : pointer to the key
**  
** @retval hash value on 32 bits
**  
**____________________________________________________
*/
static inline uint32_t stspare_fid_cache_hash(stspare_fid_cache_key_t *usr_key) {
  uint32_t         h = 2166136261;
  unsigned char  * d = (unsigned char *) usr_key->fid;
  int              i;
  rozofs_inode_t * fake_inode_p;
  
  fake_inode_p = (rozofs_inode_t*)usr_key->fid;
  rozofs_reset_recycle_on_fid(fake_inode_p);
  
  /*
  ** hash on fid
  */
  for (i=0; i<sizeof(fid_t); i++,d++) {
    h = (h * 16777619)^ *d;
  }
  /*
  ** Add something for the chunk as well as the sid 
  */
  h += (usr_key->sid+usr_key->chunk); 
  return h;
}

/*
**____________________________________________________
**
** Reset a FID cache entry
**
** @param p  pointer to the FID cache entry
** 
**____________________________________________________
*/
static inline void stspare_fid_cache_reset(stspare_fid_cache_t * p) {
  memset(&p->data,0,sizeof(stspare_fid_data_t));
}
/*
**____________________________________________________
**
** Get next entry in cache
**
** @retval the pointer to the FID cache entry or NULL when no more entry
**____________________________________________________
*/
static inline stspare_fid_cache_t * stspare_fid_cache_getnext() {
  stspare_fid_cache_t          * stspare_fid_cache_current_entry = NULL;

  
  /*
  ** Get first
  */
  if (stspare_fid_cache_next_list_entry == (stspare_fid_cache_t*) &stspare_fid_cache_running_list) {  
    /*
    ** Get 1Rst entry in the list
    */
    stspare_fid_cache_current_entry   = (stspare_fid_cache_t*) ruc_objGetFirst(&stspare_fid_cache_running_list);
    if (stspare_fid_cache_current_entry == NULL) return NULL;
    /*
    ** Prepare next entry
    */
    stspare_fid_cache_next_list_entry = (stspare_fid_cache_t *)stspare_fid_cache_current_entry->link.ps; 
    // no next entry
    if (stspare_fid_cache_next_list_entry == (stspare_fid_cache_t*)&stspare_fid_cache_running_list) {
      stspare_fid_cache_next_list_entry = NULL;
    }
    return stspare_fid_cache_current_entry;
  }    

  /*
  ** Get next
  */
  stspare_fid_cache_current_entry = stspare_fid_cache_next_list_entry;
  if (stspare_fid_cache_current_entry == NULL) { 
    stspare_fid_cache_next_list_entry = (stspare_fid_cache_t*)&stspare_fid_cache_running_list;
  }
  else {
    stspare_fid_cache_next_list_entry = (stspare_fid_cache_t *) stspare_fid_cache_current_entry->link.ps;
  }
  if (stspare_fid_cache_next_list_entry == (stspare_fid_cache_t*)&stspare_fid_cache_running_list) {
    stspare_fid_cache_next_list_entry = NULL;
  }    
  return stspare_fid_cache_current_entry;
}
/*
**____________________________________________________
**
** Get next entry in cache
**
** @retval the pointer to the FID cache entry or NULL when no more entry
**____________________________________________________
*/
static inline void stspare_fid_cache_set1rst() {
  stspare_fid_cache_next_list_entry = (stspare_fid_cache_t *)&stspare_fid_cache_running_list;
}
/*
**____________________________________________________
**
** release FID cache entry (called from the application)
**
** @param p  pointer to the FID cache entry
** 
**____________________________________________________
*/
static inline void stspare_fid_cache_release(stspare_fid_cache_t *p) {
  uint32_t hash;

  if (p == NULL) return; 
  
  stspare_fid_cache_stat.release++;
  
  /*
  ** Case it is the current listing entry
  */
  if (p == stspare_fid_cache_next_list_entry) {
    /*
    ** Go ahead to the next entry
    */
    stspare_fid_cache_getnext();
  }
  
  /*
  ** Release the cache entry
  */
  hash = stspare_fid_cache_hash(&p->data.key);
  if (storio_fid_cache_remove(hash, &p->data.key)==-1) {
    severe("storio_fid_cache_remove");
  }
     
  stspare_fid_cache_stat.free++;  

  /*
  ** Unchain the context
  */
  ruc_objRemove(&p->link);  
    
  /*
  ** Put it in the free list
  */    
  ruc_objInsert(&stspare_fid_cache_free_list->link,&p->link);
} 

/*
**____________________________________________________
**
** Allocate a FID cache entry from the distributor
**
** @retval the pointer to the FID cache entry or NULL when out of memory
**____________________________________________________
*/
static inline stspare_fid_cache_t * stspare_fid_cache_allocate() {
  stspare_fid_cache_t * p;
  
  /*
  ** Get first free context
  */
  p = (stspare_fid_cache_t*) ruc_objGetFirst(&stspare_fid_cache_free_list->link);
  if (p == NULL) {
    return NULL;
  }    

  stspare_fid_cache_reset(p);

  /*
  ** Default is to create the context in running mode
  */
  p->status  = STSPARE_FID_CACHE_RUNNING;   
  stspare_fid_cache_stat.free--;
  stspare_fid_cache_stat.allocation++;
  ruc_objRemove(&p->link);        
  ruc_objInsertTail(&stspare_fid_cache_running_list,&p->link);   
  return p;
}

/*
**____________________________________________________
**
** Check whether FID cache distributor is empty or not
** 
**  @retval 1 when empty / 0 when not empty
**____________________________________________________
*/
static inline int stspare_fid_cache_distributor_empty() {
  
  /*
  ** Get first free context
  */
  if (ruc_objIsEmptyList(&stspare_fid_cache_free_list->link) == TRUE) {
     return 1;
  }
  return 0;   
}
/*
**____________________________________________________
**
** Retrieve a context from its index
**
** @param idx The context index
**
** @return the rebuild context address or NULL
**____________________________________________________
*/
static inline stspare_fid_cache_t * stspare_fid_cache_retrieve(int idx) {
  stspare_fid_cache_t * p;
 
  if (idx>=STSPARE_FID_CACHE_MAX_ENTRIES) {
    return NULL;
  }  

  p = (stspare_fid_cache_t*) ruc_objGetRefFromIdx(&stspare_fid_cache_free_list->link,idx);  
  return p;  
}
/*
**____________________________________________________
**
** Insert an entry in the cache if it does not yet exist
** 
** @param cid     Cluster id of the entry to insert
** @param sid     Sid of the entry to insert
** @param fid     FID of the entry to insert
** @param chunk   chunk of the entry to insert
**
** @param The allocated FID cache context or NULL when out of memory
**
**____________________________________________________
*/
static inline stspare_fid_cache_t * stspare_fid_cache_insert(uint8_t cid, uint8_t sid, void * fid, uint8_t chunk) {
  stspare_fid_cache_t            * p;  
  uint32_t hash;
  
  /*
  ** allocate an entry
  */
  p = stspare_fid_cache_allocate();
  if (p == NULL) {
    errno = ENOMEM;
    return NULL;
  }
  p->data.key.cid   = cid;
  p->data.key.sid   = sid; 
  p->data.key.chunk =  chunk;
  memcpy(&p->data.key.fid,fid,sizeof(fid_t));

  hash = stspare_fid_cache_hash(&p->data.key);  
  if (storio_fid_cache_insert(hash, p->index) != 0) {
     severe("storio_fid_cache_insert"); 
     stspare_fid_cache_release(p);
     return NULL;
  }
  return p;
}
/*
**____________________________________________________
**
** Search an entry in the FID cache 
** 
**
** @param cid   The cluster identifier
** @param sid   The storage identifier within the cluster
** @param fid   the FID
** @param chunk Chunk number
**
** @retval found entry or NULL
**____________________________________________________
*/
static inline stspare_fid_cache_t * stspare_fid_cache_search(uint8_t cid, uint8_t sid, void * fid, uint8_t chunk) {
  stspare_fid_cache_t   * p;  
  uint32_t hash;
  uint32_t index;
  stspare_fid_cache_key_t key;

  key.cid   = cid;
  key.sid   = sid;
  key.chunk = chunk;
  memcpy(key.fid,fid,sizeof(key.fid));
  
  hash = stspare_fid_cache_hash(&key);

  /*
  ** Lookup for an entry
  */
  index = storio_fid_cache_search(hash, &key) ;
  if (index == -1) {
    return NULL;
  }
 
  p = stspare_fid_cache_retrieve(index);

  return p;
}
/*
**____________________________________________________
**
** creation of the FID cache
** That API is intented to be called during the initialization of the module
**____________________________________________________
*/
uint32_t stspare_fid_cache_init(uint32_t nbCtx);


#ifdef __cplusplus
}
#endif /*__cplusplus */

#endif
