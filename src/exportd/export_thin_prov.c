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
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include <sys/stat.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/common_config.h>
#include <rozofs/rpc/eproto.h>
#include <rozofs/rpc/epproto.h>
#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/rpc/export_profiler.h>
#include <rozofs/common/profile.h>
#include <rozofs/rpc/rpcclt.h>
#include <rozofs/rpc/mproto.h>
#include <rozofs/rpc/mclient.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/rozofs_cpu.h>
#include "config.h"
#include "exportd.h"
#include "export.h"


#define EXPTHIN_MAX_SID 9
#define ROZO_NB_EXPTHIN_THREAD 4 /**< number of thin provisioning threads */
/* Frequency calls of expthin_get_block_count function */
#define EXPTHIN_PTHREAD_FREQUENCY_SEC 2

#define EXPTHIN_SCAN_FILES_MAX 1000


/**
* context state
*/
typedef enum 
{
  EXPTHIN_ST_IDLE=0,
  EXPTHIN_ST_UPDATE_REQ,   /**< the number of blocks must be recomputed             */
  EXPTHIN_ST_DEL_REQ,      /**< the entry has to be deleted                         */
  EXPTHIN_ST_IN_PRG,       /**< recomputing of the number of blocks is in progress  */
  EXPTHIN_ST_MAX,
} expthin_ctx_state_e;

typedef struct _expthin_ctx_t
{
   list_t   list;  /**< needed to link the entry on a hash entry  */
   fid_t    fid;   /**< inode unique identifier                   */
   cid_t    cid;   /**< cluster identifier                        */
   sid_t    sids[EXPTHIN_MAX_SID]; /**< list of the sids: limited to the first 9 SIDs */
   uint8_t  state:4;        /**< context state: IDLE/DELETING/... */
   uint8_t  nb_fwd:4;       /**< number of projection: forward value only    */
   uint32_t nb_4KB_blocks; /**< number of allocated blocks (unit is 4KB) */
   uint64_t timestamp;     /**< last update timestamp                    */
} expthin_ctx_t;

typedef struct _expthin_global_context_t
{
    int      init_done;  /**< assert to one when it is ready to use   */
    htable_t htable; 
    uint64_t      nb_entries;  /**< number of active entries               */
    pthread_rwlock_t *hash_line_lock_p;
    int      debug_mode;   /**< assert to one to set up the debug mode     */
} expthin_global_context_t;


typedef struct _expthin_thread_t {
  int           idx;        /**< Thread index */
  pthread_t     thId;       /**< Thread id    */
  uint64_t      nb_run;     /**< Nb of run processes */
  uint64_t      last_count; /**< Last run deletion count */
  uint64_t      last_usec;  /**< Last run duration */
  uint64_t      total_count;/**< Total deletion count */
  uint64_t      total_usec; /**< Total run duration */

} expthin_thread_t;

typedef struct _expthin_blocks_compute_t
{
   uint32_t occurences;
   uint32_t file_size_in_blocks;
   uint64_t allocated_sectors[EXPTHIN_MAX_SID];
} expthin_blocks_compute_t;

extern int mstoraged_setup_cnx(volume_t *volume, list_t * cnx);
/*
**______________________________________________________
    Global Data
**______________________________________________________
*/
expthin_global_context_t *expthin_global_context_p=NULL;

/*
** export thin thread context tabel
*/
expthin_thread_t expthin_thread[ROZO_NB_EXPTHIN_THREAD];
uint64_t expthin_last_count;
uint64_t expthin_last_ticks;

/*
**________________________________________________________________
*/
/**
*   Insert an export thin provisioning context in the hash table

   @param value: context to insert
   
   @retval 0 on success
   @retval -1 on error
*/
int expthin_htable_insert_th(expthin_ctx_t *value)
{
   list_t *bucket;
   htable_t *h=NULL;
   uint32_t hash;
   
   hash = lv2_hash(value->fid);
   /*
   ** check if the hash table is ready to use
   */
   if (expthin_global_context_p == NULL)
   {
      warning("EXPTHIN: no feature context available");
      return -1;
   }
   if (expthin_global_context_p ->init_done == 0)
   {
      warning("EXPTHIN: not yet initialised");
      return -1;
   }
   h = &expthin_global_context_p->htable;   
   bucket = h->buckets + (hash % h->size);
   
   /*
   ** insert in the table
   */
   pthread_rwlock_wrlock(&h->lock[hash%h->lock_size]);
   list_push_back(bucket, &value->list);
   pthread_rwlock_unlock(&h->lock[hash%h->lock_size]);
   __atomic_fetch_add(&expthin_global_context_p->nb_entries,1,__ATOMIC_SEQ_CST);
   return 0;
}

/*
**________________________________________________________________
*/
/**
*  Init of the hash table for multi thread environment

   @param size : size of the hash table
   @param lock_size: max number of locks
   @param hash : pointer to the hash function
   @param cmp : compare to the match function
   
   @retval 0 on success
   @retval < 0 error (see errno for details)
*/
int expthin_htable_initialize_th(uint32_t size,uint32_t lock_size, uint32_t(*hash) (void *),
                       int (*cmp) (void *, void *)) {
    list_t *it;
    int i;
    int ret;
    htable_t * h;
    expthin_global_context_t *alloc_ctx_p;

    alloc_ctx_p = xmalloc(sizeof(expthin_global_context_t));
    if (alloc_ctx_p == NULL)
    {
       warning("ExpThin: cannot allocate memory");
       return -1;
    }
    memset(alloc_ctx_p,0,sizeof(expthin_global_context_t));
    /*
    ** save it to the global in order to make it visible from the threads
    */
    expthin_global_context_p = alloc_ctx_p;
    h = &expthin_global_context_p->htable;

    h->hash = hash;
    h->cmp = cmp;
    h->size = size;
    h->buckets = xmalloc(size * sizeof (list_t));
    for (it = h->buckets; it != h->buckets + size; it++)
        list_init(it);
    h->lock_size = (lock_size > ROZOFS_HTABLE_MAX_LOCK)?ROZOFS_HTABLE_MAX_LOCK:lock_size;
    for (i = 0; i < h->lock_size;i++)
    {
      ret = pthread_rwlock_init(&h->lock[i], NULL);
      if (ret != 0) return -1;
    } 
    /*
    ** allocate the memory for the hash line lock
    */
    expthin_global_context_p->hash_line_lock_p = xmalloc(sizeof(pthread_rwlock_t)*h->size);
    if (expthin_global_context_p->hash_line_lock_p == NULL)
    {
       warning("ExpThin: cannot allocate memory for hash line locking");
       return -1;
    }
    /*
    ** init of the lock on each hash entry (beginning of each list)
    */
    pthread_rwlock_t *hash_line_lock_cur_p = expthin_global_context_p->hash_line_lock_p;
    for (i = 0; i < h->size;i++,hash_line_lock_cur_p++)
    {
      ret = pthread_rwlock_init(hash_line_lock_cur_p, NULL);
      if (ret != 0) return -1;
    } 
    expthin_global_context_p->init_done = 1;
          
    return 0;
}

/*
**__________________________________________________________________
*/
/*
** allocate an entry for thin provisioning computation
 
   
   @param lv2_entry_p: pointer to the level 2 cache entry
   @param e: pointer to the export configuration in memory
   
   @retval <> NULL: pointer to the context
   @retval NULL : no context (out of memory)
   
*/
expthin_ctx_t *expthin_allocate_context(export_t *e,lv2_entry_t *lv2_entry_p)
{
  expthin_ctx_t *ctx_p;
  
  /*
  ** allocate memory for the context
  */
  ctx_p = xmalloc(sizeof(expthin_ctx_t));
  if (ctx_p == NULL)
  {
     warning("Out of memory while allocation %d bytes (%s)",(int)sizeof(expthin_ctx_t),strerror(errno));
     return NULL;
  }
  /*
  ** fill up the entry
  */
  memcpy(ctx_p->fid,lv2_entry_p->attributes.s.attrs.fid,sizeof(fid_t));
  ctx_p->cid = lv2_entry_p->attributes.s.attrs.cid;
  memcpy(ctx_p->sids,lv2_entry_p->attributes.s.attrs.sids,EXPTHIN_MAX_SID);
  /*
  ** copy the current number of block from the disk inode in order to be able to
  ** compute the rozofs quota on the fly without indexing the full exportd
  */
  ctx_p->timestamp = lv2_entry_p->attributes.s.attrs.mtime;
  ctx_p->nb_4KB_blocks = lv2_entry_p->attributes.s.attrs.children;
  ctx_p->state = EXPTHIN_ST_IDLE;
  list_init(&ctx_p->list);
  /*
  ** insert the entry on the right hash entry level
  */
  if (expthin_htable_insert_th(ctx_p) < 0)
  {
    /*
    ** release the allocated context
    */
    xfree(ctx_p);
    return NULL;
  }
  return ctx_p;
}
  
/*
**__________________________________________________________________
*/
/*
** Check if the file has an entry in the thin provisioning table
 
   That function is intended to be called by lookup, getattr and write_block API
   
   @param lv2_entry_p: pointer to the level 2 cache entry
   @param e: pointer to the export configuration in memory
   @param write_block_flag: assert to one when the file is either extended or a write_block occurs
   @param nb_block_p : pointer to array where the difference in 4KB is reported when there is a change
   @param dir_p: pointer to the location used to indicate in the number of blocks must be added or substracted
   
   These 2 last fields are revelant only when there is a change in the number of blocks.
   
   @retval 0: nothing to write
   @retval 1: there is a change: inode must be written on disk to update the number of blocks
   
*/
int expthin_check_entry(export_t *e,lv2_entry_t *lv2_entry_p,int write_block_flag,uint32_t *nb_block_p,int *dir_p)
{
  expthin_ctx_t *ctx_p;
  int update = 0;
  
  *dir_p = 0;
  *nb_block_p = 0;
  /*
  ** Check if the export is configured with thin provisioning option
  */
  if (e->thin == 0) return 0;

  /*
  ** Only the regular files are concerned by this feature
  */
  if (!S_ISREG(lv2_entry_p->attributes.s.attrs.mode)) return 0;
  /*
  ** check if the entry has been allocated for the file
  */
  if (lv2_entry_p->thin_provisioning_ctx_p == NULL)
  {
     /*
     ** need to allocated a fresh entry
     */
     lv2_entry_p->thin_provisioning_ctx_p =  expthin_allocate_context(e,lv2_entry_p);
   }
   if (lv2_entry_p->thin_provisioning_ctx_p == NULL)
   {
     /*
     ** something wrong : runs out of memory
     */
     return 0;
   }
   ctx_p = lv2_entry_p->thin_provisioning_ctx_p;
   /*
   ** Check if there is a change in the number of blocks
   */
   if ( ctx_p->nb_4KB_blocks != lv2_entry_p->attributes.s.attrs.children)
   {
   
      /*
      ** need to compute the delta in order to update the rozofs quota
      */
      if (ctx_p->nb_4KB_blocks > lv2_entry_p->attributes.s.attrs.children)
      {
         *dir_p = 1;
	 *nb_block_p = ctx_p->nb_4KB_blocks - lv2_entry_p->attributes.s.attrs.children;
      }
      else
      {
         *dir_p = -1;
	 *nb_block_p = lv2_entry_p->attributes.s.attrs.children - ctx_p->nb_4KB_blocks;      
      }
      lv2_entry_p->attributes.s.attrs.children = ctx_p->nb_4KB_blocks;
      update = 1;
   }
   /*
   ** check if a size recomputation is required
   */
   if ((write_block_flag) || lv2_entry_p->attributes.s.attrs.children==0)
   {
     /*
     ** inform the thread that the number of block must be recomputed
     */
      ctx_p->state = EXPTHIN_ST_UPDATE_REQ;
   }
   return update;     
}


/*
**__________________________________________________________________
*/
/*
** remove an entry 
 
   That function is intended to be called when the level2 cache entry is removed (it could be
   because the file is deleted or because of LRU)
   That service is intended to be called for regular files only, useless for all other file types.
   
   @param lv2_entry_p: pointer to the level 2 cache entry

   @retval none
   
*/
void expthin_remove_entry(lv2_entry_t *lv2_entry_p)
{
  expthin_ctx_t *ctx_p;
  /*
  ** nothing to do if there is no thin provisioning context attach with the entry
  */
  if (lv2_entry_p->thin_provisioning_ctx_p == NULL) return;

  ctx_p = lv2_entry_p->thin_provisioning_ctx_p;
  ctx_p->state = EXPTHIN_ST_DEL_REQ;
  /*
  ** re-attempt if there is a collision with a thread
  */
  if (ctx_p->state != EXPTHIN_ST_DEL_REQ) ctx_p->state = EXPTHIN_ST_DEL_REQ; 
}

/*
**_______________________________________________________________________________
*/

static inline int expthin_block_compute_hash_line( uint8_t * cnx_init, list_t * connexions, uint32_t bucket_idx, int * processed_files) {
  int          i = 0;
  list_t       todo;
  expthin_ctx_t * entry;
  int k;
  list_t      * p, *n;
  time_t        now;
  htable_t * h;
  mp_size_rsp_t  mp_size_table[EXPTHIN_MAX_SID];
  int  sid_status[EXPTHIN_MAX_SID];
  expthin_blocks_compute_t block_compute_table[EXPTHIN_MAX_SID];
  int found;
  int nb_occurences;
  int occurence_index;
  int next;
  eid_t eid;
  int inverse_count;
  
  export_t * e;
  uint16_t rozofs_inverse; 
  uint16_t rozofs_forward; 
  /*
  ** Initialize the working lists
  */
  list_init(&todo);
  
  h = &expthin_global_context_p->htable;
  /* 
  ** Move the whole bucket list to the working list
  */   
  if ((errno = pthread_rwlock_wrlock(&h->lock[bucket_idx%h->lock_size])) != 0) {
    severe("pthread_rwlock_wrlock failed: %s", strerror(errno));
    return -1;
  }
  list_move(&todo,&h->buckets[bucket_idx]);
  if ((errno = pthread_rwlock_unlock(&h->lock[bucket_idx%h->lock_size])) != 0) {
    severe("pthread_rwlock_unlock failed: %s", strerror(errno));
  }

  now = time(NULL);

  /*
  ** get every entry
  */	
  list_for_each_forward_safe(p, n, &todo) {

    next = 0;
    entry = list_entry(p,expthin_ctx_t,list);
    switch (entry->state)
    {
    /*
    ** check if the context has to be deleted
    */
    case EXPTHIN_ST_DEL_REQ: 
       /*
       ** remove it from the bucket list and release the context
       */
       list_remove(&entry->list);
       __atomic_fetch_sub(&expthin_global_context_p->nb_entries,1,__ATOMIC_SEQ_CST);
       xfree(entry);
       next = 1;
       break;
 
    case EXPTHIN_ST_IDLE:
       /*
       ** nothing to do for that context
       */
       next = 1;
       break;
    
    case EXPTHIN_ST_UPDATE_REQ:
       /*
       ** check if it is time to recompute the number of blocks
       */
       if ((now - entry->timestamp) < common_config.expthin_guard_delay_sec)
       {
         /*
	 ** not the time the recompute
	 */
	 next = 1;
	 break;
       }
       /*
       ** acknowledge the UPDATE request:need to take the lock associated at the bucket
       */
       pthread_rwlock_wrlock(&h->lock[bucket_idx%h->lock_size]);
       if (entry->state == EXPTHIN_ST_UPDATE_REQ) entry->state = EXPTHIN_ST_IN_PRG;
       else next = 1;
       pthread_rwlock_unlock(&h->lock[bucket_idx%h->lock_size]);       
       break; 

    default :
       next = 1;
       break;
    }
    /*
    ** Check if we need to recompute the number of blocks for the entry or attempt to get the next
    ** from the list
    */
    if (next) continue;
    /*
    ** OK, it is time to recompute the number of blocks of the file
    */
    /*
    ** get the export for the fid
    */
    eid = rozofs_get_eid_from_fid(entry->fid);
    e = exports_lookup_export(eid);
    if (e == NULL)
    {
       warning("Unknown EID %u",eid);
       continue;
    }
    rozofs_inverse = rozofs_get_rozofs_inverse(e->layout);
    rozofs_forward = rozofs_get_rozofs_forward(e->layout);
    /*
    ** clear the working variables
    */
    memset(mp_size_table,0,sizeof(mp_size_rsp_t)*EXPTHIN_MAX_SID);
    memset(sid_status,-1,sizeof(int)*EXPTHIN_MAX_SID);
    memset(block_compute_table,0,sizeof(expthin_blocks_compute_t)*EXPTHIN_MAX_SID);

    /*
    ** This entry needs to be processed
    ** setup connections if not yet done
    */
    if (*cnx_init == 0) {
      *cnx_init = 1;
      if (mstoraged_setup_cnx(e->volume, connexions) != 0) {
        goto out;
      }
    }
    
    // For each storage associated with this file
    inverse_count = 0;
    nb_occurences = 0;
    occurence_index = 0;
    for (i = 0; i < rozofs_forward; i++) {

      mstorage_client_t * stor = NULL;

      if (0 == entry->sids[i]) {
        continue; // The bins file has already been deleted for this server
      }

      if ((stor = mstoraged_lookup_cnx(connexions, entry->cid, entry->sids[i])) == NULL) {
        char   text[512];
	char * p = text;

        p += rozofs_string_append(p,"lookup_cnx failed FID ");
        rozofs_uuid_unparse(entry->fid,p);
	p += 36;
	p += rozofs_string_append(p," cid ");
	p += rozofs_u32_append(p,entry->cid);
	p += rozofs_string_append(p," sid ");	
	p += rozofs_u32_append(p,entry->sids[i]);	
        severe("%s",text);	
	/*
	** Invalid cid/sid
	*/
        continue;// lookup_cnx failed !!! 
      }

      if (0 == stor->status) {
        continue; // This storage is down
      }

      // Send remove request
      int spare;
      if (i<rozofs_forward) spare = 0;
      else                  spare = 1;
      
      if (mstoraged_client_get_file_size(stor, entry->cid, entry->sids[i], entry->fid, spare,&mp_size_table[i]) != 0) {
        if (errno != ENOENT) {
          char msgString[128];
          char * pChar = msgString;
          pChar += sprintf(pChar,"mstoraged_client_get_file_size cid:%u sid:%u fid:", entry->cid, entry->sids[i]);
          pChar += rozofs_fid_append(pChar,entry->fid);
          pChar += sprintf(pChar," error %s", strerror(errno));
          warning("%s",msgString);
	  /*
	  ** Say this storage is down not to use it again 
	  ** during this run; this would fill up the log file.
	  */
	  stor->status = 0; 
          continue;
        }
        /*
        ** File does not exist
        */  
        memset(&mp_size_table[i], 0, sizeof(mp_size_table[i]));
      }
      
      /*
      ** we got a positive response , so increment the inverse count and check if inverse has been rearched
      */
      inverse_count+=1;
      found = 0;
      for (k= 0; k < i; k++)
      {
         if (block_compute_table[k].file_size_in_blocks == mp_size_table[i].file_size_in_blocks)
	 {
	    found = 1;
	    block_compute_table[k].file_size_in_blocks = mp_size_table[i].file_size_in_blocks;
	    block_compute_table[k].allocated_sectors[block_compute_table[k].occurences] = mp_size_table[i].allocated_sectors;	    
	    block_compute_table[k].occurences +=1;
	    nb_occurences = block_compute_table[k].occurences;
	    occurence_index = k;
	    break;
	  }
      }
      if (found == 0)
      {
	 block_compute_table[k].file_size_in_blocks = mp_size_table[i].file_size_in_blocks;
	 block_compute_table[k].occurences +=1;      
      }
      /*
      ** Check if we have enough results to report the good value
      */
      if (nb_occurences >= ((rozofs_inverse>>1)+1))
      {
         break;
      }
    }
    /*
    ** OK , we have finish for that FID, now we need to check if we have enough information to compute
    ** the file size in 4KB blocks
    */
    if (nb_occurences < ((rozofs_inverse>>1)+1))
    {
       /*
       ** no enough data to compute the number of blocks, we wait for the next run
       */
       continue;
    }

    // Update the nb. of files that have been processed.
    (*processed_files)++; 
    /*
    ** Update the entry 
    */
    entry->timestamp = time(NULL);
    /*
    ** compute the number of 4KB blocks: we take the greatest one
    */
    {
       int z;
       uint64_t max_allocated_sectors = 0;
       for (z= 0; z < block_compute_table[occurence_index].occurences; z++)
       {
          if (block_compute_table[occurence_index].allocated_sectors[z] <= max_allocated_sectors) continue;
	  max_allocated_sectors = block_compute_table[occurence_index].allocated_sectors[z];       
       }   
       /*
       ** get the value in 4KB units
       */
       entry->nb_4KB_blocks = (uint32_t) max_allocated_sectors/8;
       if ((max_allocated_sectors%8) !=0) entry->nb_4KB_blocks+=1;
       entry->nb_4KB_blocks *=rozofs_inverse;
       if (expthin_global_context_p->debug_mode)
       {
         char buf[64];
	 rozofs_uuid_unparse(entry->fid,buf);
	 info("expthin fid %s len = %u %llu",buf,entry->nb_4KB_blocks,(unsigned long long int)max_allocated_sectors);
       }   
    }
    /*
    ** attempt the change the state to IDLE if it is still in the IN progress state
    */
    if (entry->state == EXPTHIN_ST_IN_PRG)
    {
       pthread_rwlock_wrlock(&h->lock[bucket_idx%h->lock_size]);
       if (entry->state == EXPTHIN_ST_IN_PRG) entry->state = EXPTHIN_ST_IDLE;
       pthread_rwlock_unlock(&h->lock[bucket_idx%h->lock_size]);                  
    }
  }  
  
out:

  /*
  ** Bucket totaly processed successfully
  */
  if (list_empty(&todo)) return 0;
  
  
  /*
  ** Push back the non processed entries in the bucket
  */    
  if ((errno = pthread_rwlock_wrlock(&h->lock[bucket_idx%h->lock_size])) != 0) {
    severe("pthread_rwlock_wrlock failed: %s", strerror(errno));
  }
  list_move(&h->buckets[bucket_idx],&todo);
  if ((errno = pthread_rwlock_unlock(&h->lock[bucket_idx%h->lock_size])) != 0) {
    severe("pthread_rwlock_unlock failed: %s", strerror(errno));
  }
  return 0;      
}

/*
**_______________________________________________________________________________
*/

uint64_t expthin_block_compute(uint16_t * first_bucket_idx, expthin_thread_t * thCtx) {
    uint16_t     bucket_idx = 0;
    uint8_t      cnx_init = 0;
    list_t       connexions;
    uint16_t     idx = 0;
    htable_t * h;

    int          processed_files = 0;
    
    list_init(&connexions);  

    // Loop on every bucket
    h = &expthin_global_context_p->htable;
    for (idx=1; idx <=h->size; idx++) {
	              
        /*
	** compute the bucket index to check
	*/
        bucket_idx = (idx+*first_bucket_idx) % h->size;

        // Check if the bucket is empty
	if (list_empty(&h->buckets[bucket_idx])) {
	  continue;
	}
	/*
	** attempt to lock the bucket because there is a race condition between the 
	** export thin threads
	*/
	if ((errno = pthread_rwlock_trywrlock(&expthin_global_context_p->hash_line_lock_p[bucket_idx])) != 0) {  
	   /*
	   ** cannot get the lock on that line-> attempt the next one
	   */
	   continue;	
	}
	
	expthin_block_compute_hash_line(&cnx_init, &connexions, bucket_idx, &processed_files);
        /*
	** Check whether the allowed count of trashed files has benn already done
	*/
	if ((errno = pthread_rwlock_unlock(&expthin_global_context_p->hash_line_lock_p[bucket_idx])) != 0) {  
	   /*
	   ** cannot release the lock
	   */
	   warning("pthread_rwlock_wrlock error %s",strerror(errno));	
	}
        if (processed_files >= common_config.thin_scan_file_per_run) break; // Exit from the loop	
    }

    // Update the first bucket index to use for the next call
    if (0 == processed_files) {
        // If no files removed 
        // The next first bucket index will be 0
        // not necessary but better for debug
        *first_bucket_idx = 0;
    } else {
        *first_bucket_idx = bucket_idx;
    }

    // Release storage connexions
    mstoraged_release_cnx(&connexions);
    
    return processed_files;
}



/*
 *_______________________________________________________________________
 */
/** Thread to compute the number of used blocks for exportd configured with
**  the thin provisioning option
 */
static void *expthin_thread_entry(void *v) {
  pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
  char              thName[16];
  expthin_thread_t * thCtx;
  uint64_t          before,after,credit;
  uint64_t          processed;

  thCtx = (expthin_thread_t *) v;
  uint16_t bucket_idx = 0;

  /*
  ** Record thread name
  */
  sprintf(thName, "ExpThin%d", thCtx->idx);    
  uma_dbg_thread_add_self(thName);
  /*
  ** Thread time shifting.
  ** Thread 0 do not sleep.
  ** Thread 1 sleeps 2 sec. 
  ** Thread 2 sleeps 4 sec.
  ** ...
  */ 
  sleep(thCtx->idx * 2);

  for (;;) {


    if (thCtx->idx >= common_config.nb_trash_thread) {
      usleep(EXPTHIN_PTHREAD_FREQUENCY_SEC*1000000*ROZO_NB_EXPTHIN_THREAD);
      continue;
    } 
    /*
    ** Check if all the data is ready, otherwise sleep for a while and then retry
    */
    if (expthin_global_context_p == NULL)
    {
      sleep(thCtx->idx * 2);
      continue;    
    } 
    if (expthin_global_context_p->init_done == 0)
    {
      sleep(thCtx->idx * 2);
      continue;        
    }

    processed  = 0;

    /*
    ** One run time credit
    */
    credit = EXPTHIN_PTHREAD_FREQUENCY_SEC*1000000*common_config.nb_trash_thread;

    /*
    ** Read ticker before loop
    */
    GETMICROLONG(before);
    processed += expthin_block_compute(&bucket_idx, thCtx);

    /*
    ** Read ticker after loop
    */
    GETMICROLONG(after); 
    thCtx->total_usec  += thCtx->last_usec;
    thCtx->total_count += thCtx->last_count;

    thCtx->last_usec  = after - before;
    thCtx->last_count = processed;

    thCtx->nb_run++; 

    /*
    ** Compte credit for a loop 
    */
    if (thCtx->last_usec < credit) {
      uint64_t delay;
      /*
      ** The loop did last less than the credit
      ** Wait a little...
      */ 
      delay = credit - thCtx->last_usec;
      usleep(delay);
    }  
  }
  return 0;
}


/*
**_______________________________________________________________________
**
** Start every required exportd thin provisioning thread
**
*/
void start_all_expthin_thread() {
  int idx;
  
  for (idx=0; idx < ROZO_NB_EXPTHIN_THREAD; idx++) {
  
    expthin_thread[idx].idx  = idx;
    expthin_thread[idx].thId = 0;
    
    if ((errno = pthread_create(&expthin_thread[idx].thId, NULL, expthin_thread_entry, &expthin_thread[idx])) != 0) {
      fatal("can't create export thin provisoning thread %d %s", idx, strerror(errno));  
    }	
  }
}
/*
**_______________________________________________________________________
**
** Stop every started  exportd thin provisioning thread
**
*/
void stop_all_expthin_thread() {
  int idx;
  
  for (idx=0; idx < ROZO_NB_EXPTHIN_THREAD; idx++) {
    if (expthin_thread[idx].thId) {
      // Canceled the remove bins pthread before reload list of exports
      if ((errno = pthread_cancel(expthin_thread[idx].thId)) != 0) {
        severe("can't canceled export thin provisoning thread %d : %s", idx, strerror(errno));
      }  
    }
    expthin_thread[idx].thId = 0;
  }
}  



/*
**_____________________________________________________________________________________________

     ROZODIAG SECTION
     
**_____________________________________________________________________________________________
*/
/*
**_______________________________________________________________________________
*/
/**
*  trash statistics display

   @param buf : pointer to the buffer that will contains the statistics
*/
#if 0
char *expthin_stats_json(char *pChar)
{
   uint64_t new_us;
   uint64_t new_count;
   uint64_t new_trashed;
   int      idx; 
   int      first=1;
   
   pChar += rozofs_string_append(pChar, "{ \"trash\" : {\n");    
   pChar += rozofs_string_append(pChar, "    \"nb threads\"    : ");
   pChar += rozofs_u32_append(pChar, common_config.nb_trash_thread);
   pChar += rozofs_string_append(pChar, ",\n    \"thread period\" : ");
   pChar += rozofs_u32_append(pChar,RM_BINS_PTHREAD_FREQUENCY_SEC*common_config.nb_trash_thread);
   pChar += rozofs_string_append(pChar, ",\n    \"max per run\"   : ");  
   pChar += rozofs_u32_append(pChar, common_config.thin_scan_file_per_run);  
   
   pChar += rozofs_string_append(pChar, ",\n    \"threads\": [\n");   
   for (idx=0; idx < ROZO_NB_RMBINS_THREAD; idx++) {

     /*
     ** Only diplay threads that have worked
     */
     if (rmbins_thread[idx].total_usec == 0) continue;
     
     if (first) {
       first = 0;
     }  
     else {
       pChar += rozofs_string_append(pChar, ",\n"); 
     }
       
     pChar += rozofs_string_append(pChar, "       {\"idx\": ");   
     pChar += rozofs_u32_append(pChar, idx);   
     pChar += rozofs_string_append(pChar, ", \"nb run\": ");   
     pChar += rozofs_u64_append(pChar, rmbins_thread[idx].nb_run);   
     pChar += rozofs_string_append(pChar, ", \"last count\": ");   
     pChar += rozofs_u64_append(pChar, rmbins_thread[idx].last_count);   
     pChar += rozofs_string_append(pChar, ", \"last usec\": ");   
     pChar += rozofs_u64_append(pChar, rmbins_thread[idx].last_usec);   
     pChar += rozofs_string_append(pChar, ", \"total count\": ");   
     pChar += rozofs_u64_append(pChar, rmbins_thread[idx].total_count);   
     pChar += rozofs_string_append(pChar, ", \"total usec\": ");   
     pChar += rozofs_u64_append(pChar, rmbins_thread[idx].total_usec);   
     pChar += rozofs_string_append(pChar, "}"); 
   }
   pChar += rozofs_string_append(pChar, "\n    ],\n");
      
   GETMICROLONG(new_us);

   pChar += rozofs_string_append(pChar, "    \"delete delay\"  : ");   
   pChar += rozofs_u64_append(pChar, (unsigned long long int)common_config.deletion_delay);   
   
   pChar += rozofs_string_append(pChar, ",\n    \"reloaded count\": "); 
   pChar += rozofs_u64_append(pChar, (unsigned long long int) export_rm_bins_reload_count);

   /*
   ** Trashed
   */

   pChar += rozofs_string_append(pChar, ",\n    \"trashed count\" : "); 
   new_trashed = export_rm_bins_trashed_count;
   pChar += rozofs_u64_append(pChar, (unsigned long long int) new_trashed);
   

   pChar += rozofs_string_append(pChar, ",\t    \"trashed/min\"   : ");      
   if ((export_last_us!=0) &&(new_us>export_last_us)) {
     pChar += rozofs_u64_append(pChar, (unsigned long long int) (new_trashed-export_last_trashed_json)*(60000000)/(new_us-export_last_us));
   }
   else {
     pChar += rozofs_string_append(pChar, "0"); 
   }  
   export_last_trashed_json = new_trashed;
   /*

   ** Actually deleted
   */
   
   pChar += rozofs_string_append(pChar, ",\n    \"deleted count\" : "); 
   new_count   = export_rm_bins_done_count;
   pChar += rozofs_u64_append(pChar, (unsigned long long int) new_count);
   
   
   pChar += rozofs_string_append(pChar, ",\t    \"deleted/min\"   : ");   
   if ((export_last_us!=0) &&(new_us>export_last_us)) {
     pChar += rozofs_u64_append(pChar, (unsigned long long int) (new_count-expthin_last_count_json)*(60000000)/(new_us-export_last_us));
   }
   else {
     pChar += rozofs_string_append(pChar, "0"); 
   }  
   expthin_last_count_json = new_count;
   
     
   /*
   ** Still pending
   */   
   
   pChar += rozofs_string_append(pChar, ",\n    \"pending\"       : ");   
   pChar += rozofs_u64_append(pChar, (unsigned long long int) (export_rm_bins_reload_count+export_rm_bins_trashed_count)-export_rm_bins_done_count);

   /*
   ** Recyclinging
   */   

   if (common_config.fid_recycle) {
     pChar += rozofs_string_append(pChar, ",\n    \"recycling\" : {\n");
     pChar += rozofs_string_append(pChar, "       \"threshold\" : ");   
     pChar += rozofs_u64_append(pChar, (unsigned long long int)export_rm_bins_threshold_high);   
     
     pChar += rozofs_string_append(pChar, ",\n       \"reloaded\"  : "); 
     pChar += rozofs_u64_append(pChar, (unsigned long long int) export_fid_recycle_reload_count);
     pChar += rozofs_string_append(pChar, ",\n       \"recycled\"  : "); 
     pChar += rozofs_u64_append(pChar, (unsigned long long int) export_recycle_pending_count);
     pChar += rozofs_string_append(pChar, ",\n       \"done\"      : "); 
     pChar += rozofs_u64_append(pChar, (unsigned long long int) export_recycle_done_count);
     pChar += rozofs_string_append(pChar, ",\n       \"pending\"   : ");   
     pChar += rozofs_u64_append(pChar, (unsigned long long int) (export_fid_recycle_reload_count+export_recycle_pending_count)-export_recycle_done_count);
     pChar += rozofs_string_append(pChar, "\n    }");
   }
   pChar += rozofs_string_append(pChar, "\n  }\n}\n");

   export_last_us = new_us;
   return pChar;
}
#endif

char *expthin_stats(char *pChar)
{
   uint64_t new_ticks;
   uint64_t new_count;   
   pChar += sprintf(pChar,"Thin provisoning thread period         : %d seconds\n",(int)EXPTHIN_PTHREAD_FREQUENCY_SEC);
   pChar += sprintf(pChar,"thin provisioning rate                 : %d\n",(int)common_config.thin_scan_file_per_run);   
   pChar += sprintf(pChar,"thin provisioning debug mode           : %s\n",(expthin_global_context_p->debug_mode!=0)?"ON":"OFF");   
   pChar += sprintf(pChar,"thin guard delay                       : %d\n",(int)common_config.expthin_guard_delay_sec);         
   pChar += sprintf(pChar,"level 1 hash table size                : %u\n",(unsigned int ) ( 1 << common_config.thin_lv1_hash_tb_size));        
   pChar += sprintf(pChar,"number of entries                      : %llu\n",(unsigned long long int)expthin_global_context_p->nb_entries);   

   new_ticks = rdtsc();
   new_count = expthin_global_context_p->nb_entries;
   
   if ((expthin_last_ticks!=0) &&(new_ticks>expthin_last_ticks)) {
     pChar += sprintf(pChar,"throughput                  : %llu/min\n",
              (unsigned long long int) (new_count-expthin_last_count)*rozofs_get_cpu_frequency()*60
	      /(new_ticks-expthin_last_ticks));
   } 
   expthin_last_ticks = new_ticks;
   expthin_last_count = new_count;
     
   return pChar;
}


/*
*_______________________________________________________________________
*/
/**
*  thin provisioning statistics 
*/

static char * show_thin_prov_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"thin_prov rate [nb]     : number of files scanned per period (default:%d)\n",RM_FILES_MAX);
  pChar += sprintf(pChar,"thin_prov               : display statistics\n");  
  pChar += sprintf(pChar,"thin_prov debug on|off  : turn on and off the debug mode\n");  
  return pChar; 
}

void show_thin_prov(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int limit;
    int ret;

    /*
    ** check if the service is available
    */
    if (expthin_global_context_p == NULL)
    {
      uma_dbg_send(tcpRef, bufRef, TRUE, "Service not available\n");   	  
      return;    
    
    }
    if (expthin_global_context_p->init_done == 0)
    {
      uma_dbg_send(tcpRef, bufRef, TRUE, "Service not yet initialised\n");   	  
      return;    
    
    }
    if (argv[1] == NULL) {
      expthin_stats(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
      return;
    }
#if 0    
    if (strcmp(argv[1],"json")==0) {
      expthin_stats_json(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
      return;  
    }
#endif    
    
    if (strcmp(argv[1],"debug")==0) {
      if (argv[2] == NULL) {
       show_thin_prov_help(pChar);	
       uma_dbg_send(tcpRef, bufRef, TRUE,uma_dbg_get_buffer());
       return;
      }
      if (strcmp(argv[2],"on")==0)
      {
         expthin_global_context_p->debug_mode = 1;
	 sprintf(pChar," mode debug is ON \n");
	 uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	 return;	 
      }
      if (strcmp(argv[2],"off")==0)
      {
         expthin_global_context_p->debug_mode = 0;
	 sprintf(pChar," mode debug is OFF \n");
	 uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	 return;      
      }
      show_thin_prov_help(pChar);	
      uma_dbg_send(tcpRef, bufRef, TRUE,uma_dbg_get_buffer());
      return;  
    }
   
    if (strcmp(argv[1],"rate")==0) {

      if (argv[2] == NULL) {
        common_config.thin_scan_file_per_run = EXPTHIN_SCAN_FILES_MAX;
	sprintf(pChar," revert to default (%d) \n",EXPTHIN_SCAN_FILES_MAX);
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;	 
      }
      	     
      ret = sscanf(argv[2], "%d", &limit);
      if (ret != 1) {
        show_thin_prov_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;   
      }
      common_config.thin_scan_file_per_run = limit;
      uma_dbg_send(tcpRef, bufRef, TRUE, "Done\n");   	  
      return;
    }
    show_thin_prov_help(pChar);	
    uma_dbg_send(tcpRef, bufRef, TRUE,uma_dbg_get_buffer());
}

/*
**_____________________________________________________________________________________________

     INIT SECTION
     
**_____________________________________________________________________________________________
*/
/**
*  Init of the thin provisioning feature

   @param none
   
   @retval 0 on success
   @retval -1 on error
   
*/
int expthin_init()
{
   uint32_t hash_table_size;
   int ret;
   
   /*
   ** init of the hash table
   */
   hash_table_size =  1 << common_config.thin_lv1_hash_tb_size;
   ret =  expthin_htable_initialize_th(hash_table_size,hash_table_size>>2,lv2_hash,lv2_cmp);
   if (ret < 0)
   {
      severe("Thin provisioning feature not available");
      return ret;
   }
   return 0;

}
