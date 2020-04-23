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
 #include <stdint.h>
 #include <unistd.h>
#include <pthread.h>
#include <rozofs/common/htable.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/common_config.h>
#include "rozofs_bt_dirent.h"
#include "rozofs_bt_proto.h"
#include "../exportd/mdirent.h"

mdirents_cache_entry_t *bt_dirent_cache_bucket_search_entry(fid_t fid, uint16_t index);
int bt_dirent_put_root_entry_to_cache(fid_t fid, int root_idx, mdirents_cache_entry_t *root_p);
int bt_dirent_remove_root_entry_from_cache(fid_t fid, int root_idx);


#define ROZOFS_BT_HTABLE_MAX_LOCK 256
typedef struct _rozofs_bt_htable {
    uint32_t(*hash) (void *);       /**< hash compute program */
    int (*cmp) (void *, void *);    /**< compare program      */
    void (*copy) (void *, void *);  /**< copy program (for htable_get_copy_th) */
    uint32_t size;
    uint32_t lock_size;
    pthread_rwlock_t *lock_p; /**< lock used for insertion handling */
    list_t *buckets;
} rozofs_bt_htable;

/*
 *
 * used to keep track of open file descriptors and corresponding attributes
 */
//#define ROZOFS_BT_DIRENT_GARBAGE_DEADLINE_SEC 10
//#define ROZOFS_BT_MAX_CACHE_ENTRIES (1024*256)
#define ROZOFS_BT_LEVEL0_HASH (64*1024)
typedef struct rozofs_bt_dirent_cache_t {
    int max;            /**<  max entries in the cache  */
    int size;           /**<  current number of entries */
    uint64_t   hit;     /**< hit counter                */
    uint64_t   miss;    /**< miss counter               */
    uint64_t   lru_del; /**< lru deletion               */
    uint64_t   garbage_collector_deadline_delay_sec; 
    list_t     lru;     /**< lru linked list            */ 
    list_t     del_list;  /**< list of the entries to delete */
    pthread_rwlock_t lru_lock;  /**< lock for LRU handling */
    pthread_rwlock_t del_lock;  /**< lock for entry deletion handling */
    /*
    ** case of multi-threads
    */
    rozofs_bt_htable htable;    ///< entries hashing
} rozofs_bt_dirent_cache_t;

 #define ROZOFS_BT_DIRENT_GARBAGE_PTHREAD_FREQUENCY_SEC 3
 
typedef struct _rozofs_bt_dirent_garbage_private_t {
   uint64_t period_sec;  /**< polling period en seconds */
   uint64_t garbage_collector_deletion;
   uint64_t garbage_collector_btmap_deletion;
   uint64_t polling_count;
   uint64_t deletion_count;
   uint64_t polling_time;
   int      deletion_enable;
} rozofs_bt_dirent_garbage_private_t;

/*
**___________________________________________________________________
     Global Data
**___________________________________________________________________
*/     
/*
** dirent cache used by the batch client rozofsmount
*/
rozofs_bt_dirent_cache_t rozofs_bt_dirent_cache;
rozofs_bt_thread_ctx_t rozofs_bt_dirent_garbage_collector_thread_ctx;
/*
**___________________________________________________________________
    Prototypes
**___________________________________________________________________
*/ 
static void *rozofs_bt_dirent_garbage_collector_thread(void *v);
/*
*_______________________________________________________________________
*/

/*
 ** Print the dirent cache bucket statistics
 */
char* rozofs_bt_dirent_cache_display(char *pChar) {

    uint64_t malloc_size;
    rozofs_bt_thread_ctx_t * thread_ctx_p;
    rozofs_bt_dirent_garbage_private_t *private_p;
    
    thread_ctx_p = &rozofs_bt_dirent_garbage_collector_thread_ctx;  
    private_p = thread_ctx_p->thread_private;
    malloc_size = DIRENT_MALLOC_GET_CURRENT_SIZE();
    
    pChar+=sprintf(pChar,"Malloc size (MB/B)             : %llu/%llu\n",(long long unsigned int)malloc_size/(1024*1024), 
                   (long long unsigned int)malloc_size);    
/*    pChar+=sprintf(pChar,"Level 0 cache state            : %s\n", (dirent_bucket_cache_enable == 0) ? "Disabled" : "Enabled"); */
    pChar+=sprintf(pChar,"Max entries                    : %u\n",rozofs_bt_dirent_cache.max);
    pChar+=sprintf(pChar,"Cur entries                    : %u\n",rozofs_bt_dirent_cache.size);
    pChar+=sprintf(pChar,"hit/miss                       : %llu/%llu\n", 
            (long long unsigned int) rozofs_bt_dirent_cache.hit,
            (long long unsigned int) rozofs_bt_dirent_cache.miss);
    pChar+=sprintf(pChar,"garbage retention delay(s)     : %llu\n",(long long unsigned int)rozofs_bt_dirent_cache.garbage_collector_deadline_delay_sec);
 /*
    pChar+=sprintf(pChar,"collisions cumul level0/level1 : %llu/%llu\n", 
                  (long long unsigned int) dirent_bucket_cache_collision_level0_counter,
                  (long long unsigned int) dirent_bucket_cache_collision_counter);
    pChar+=sprintf(pChar,"LRU stats \n");
    pChar+=sprintf(pChar,"global cpt (ok/err)            : %llu/%llu\n", 
                  (long long unsigned int) dirent_bucket_cache_lru_counter_global,
                  (long long unsigned int) dirent_bucket_cache_lru_global_error);
    pChar+=sprintf(pChar,"coll cpt   (ok/err)            : %llu/%llu\n", 
                  (long long unsigned int) dirent_bucket_cache_lru_counter_coll,
                  (long long unsigned int) dirent_bucket_cache_lru_coll_error);
    pChar+=sprintf(pChar,"collisions Max level0/level1   : %u/%u\n", 
                   dirent_bucket_cache_max_level0_collisions, dirent_bucket_cache_max_level1_collisions);
*/
    pChar+=sprintf(pChar,"pending deletion               : %llu\n", 
            (long long unsigned int) rozofs_bt_dirent_cache.lru_del);
    pChar+=sprintf(pChar,"effective deletion             : %llu\n", 
            (long long unsigned int) private_p->garbage_collector_deletion);

    pChar+=sprintf(pChar,"btmap pending deletion         : %llu\n", 
            (long long unsigned int) rozofs_bt_root_btmap_garbage_collector_count);
    pChar+=sprintf(pChar,"btmap effective deletion       : %llu\n", 
            (long long unsigned int) private_p->garbage_collector_btmap_deletion);
    pChar+=sprintf(pChar,"Name chunk size                : %u\n",(unsigned int)MDIRENTS_NAME_CHUNK_SZ);
    pChar+=sprintf(pChar,"Name chunk max                 : %u\n",(unsigned int)MDIRENTS_NAME_CHUNK_MAX);
    pChar+=sprintf(pChar,"Sectors (nb sectors/size)      : %u/%u Bytes\n",
                                                			 (unsigned int)DIRENT_FILE_MAX_SECTORS,
									 (unsigned int)DIRENT_FILE_MAX_SECTORS*MDIRENT_SECTOR_SIZE);
    pChar += sprintf(pChar,"------------------+----------------------+--------------+\n");
    pChar += sprintf(pChar,"  field name      | start sector(offset) | sector count |\n");
    pChar += sprintf(pChar,"------------------+----------------------+--------------+\n");
    pChar += sprintf(pChar," %-16s |  %8u (0x%-4x)   |  %9u   |\n","header",(unsigned int)DIRENT_HEADER_BASE_SECTOR,
                                                                            (unsigned int)DIRENT_HEADER_BASE_SECTOR*MDIRENT_SECTOR_SIZE,
									    (unsigned int)DIRENT_HEADER_SECTOR_CNT);
    pChar += sprintf(pChar," %-16s |  %8u (0x%-4x)   |  %9u   |\n","name bitmap",(unsigned int)DIRENT_NAME_BITMAP_BASE_SECTOR,
                                                                                 (unsigned int)DIRENT_NAME_BITMAP_BASE_SECTOR*MDIRENT_SECTOR_SIZE,
										 (unsigned int)DIRENT_NAME_BITMAP_SECTOR_CNT);
    pChar += sprintf(pChar," %-16s |  %8u (0x%-4x)   |  %9u   |\n","hash buckets",(unsigned int)DIRENT_HASH_BUCKET_BASE_SECTOR,
                                                                                  (unsigned int)DIRENT_HASH_BUCKET_BASE_SECTOR*MDIRENT_SECTOR_SIZE,
										  (unsigned int)DIRENT_HASH_BUCKET_SECTOR_CNT);
    pChar += sprintf(pChar," %-16s |  %8u (0x%-4x)   |  %9u   |\n","hash entries",(unsigned int)DIRENT_HASH_ENTRIES_BASE_SECTOR,
                                                                                   (unsigned int)DIRENT_HASH_ENTRIES_BASE_SECTOR*MDIRENT_SECTOR_SIZE,
										   (unsigned int)DIRENT_HASH_ENTRIES_SECTOR_CNT);
    pChar += sprintf(pChar," %-16s |  %8u (0x%-4x)   |  %9u   |\n","name chunks",(unsigned int)DIRENT_HASH_NAME_BASE_SECTOR,
                                                                                 (unsigned int)DIRENT_HASH_NAME_BASE_SECTOR*MDIRENT_SECTOR_SIZE,
										 (unsigned int)DIRENT_HASH_NAME_SECTOR_CNT);
    pChar += sprintf(pChar,"------------------+----------------------+--------------+\n");


/*
    pChar += sprintf(pChar,"\n------------+-------------+---------------------+\n");
    pChar += sprintf(pChar," file_limit | mask        |      put count      |\n");
    pChar += sprintf(pChar,"------------+-------------+---------------------+\n");
    for (i = 0; i < DIRENT_MAX_RANGE; i++)
    {
    
      pChar += sprintf(pChar," %10llu |  %8x   |  %16llu   |\n",
                       (long long unsigned int)dirent_range_table[i].file_limit,
                       dirent_range_table[i].mask,
                       (long long unsigned int)dirent_range_table[i].count);
		         
    }
    pChar += sprintf(pChar,"------------+-------------+---------------------+\n");
*/
    return pChar;
}
/*
*_______________________________________________________________________
*/
/**
* dirent cache
*/
char *dirent_disk_display_stats(char *pChar);

void rozofs_bt_show_dirent_cache(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    pChar = rozofs_bt_dirent_cache_display(pChar);
    pChar = dirent_disk_display_stats(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
}



/*
 **______________________________________________________________________________
 */

/**
 *   Compute the hash values for the name and fid

 @param h :previous hash computation result
 @param key1 : pointer to the fid
 @param key2 : pointer to the file index

 @retval primary hash value
 */
static inline uint32_t dirent_cache_bucket_hash_fnv(uint32_t h, void *key1, uint16_t *key2) {

    unsigned char *d = (unsigned char *) key1;
    int i = 0;

    if (h == 0) h = 2166136261U;

    /*
     ** hash on fid
     */
    for (d = key1; d != key1 + 16; d++) {
        h = (h * 16777619)^ *d;

    }
    /*
     ** hash on index
     */
    d = (unsigned char *) key2;
    for (i = 0; i < sizeof (uint16_t); d++, i++) {
        h = (h * 16777619)^ *d;

    }

    return h;
}


/*
**________________________________________________________________
*/
int rozofs_bt_dirent_cache_cmp(void *k1, void *k2) {

    int ret;
    ret =  memcmp(k1,k2,sizeof(mdirents_cache_key_t));
      if (ret != 0) {
//          hash_inode_collisions_count++;
//	  hash_inode_cur_collisions++;
	  return ret;
      }
//      if (hash_inode_max_collisions < hash_inode_cur_collisions) hash_inode_max_collisions = hash_inode_cur_collisions;
      return ret;
}
/*
**________________________________________________________________
*/
/**
*  Init of the hash table for multi thread environment

   @param h: pointer to the hash table context
   @param size : size of the hash table
   @param lock_size: max number of locks
   @param hash : pointer to the hash function
   @param cmp : compare to the match function
   
   @retval 0 on success
   @retval < 0 error (see errno for details)
*/
int rozofs_bt_htable_initialize_th(rozofs_bt_htable * h, uint32_t size,uint32_t lock_size, uint32_t(*hash) (void *),
                       int (*cmp) (void *, void *)) {
    list_t *it;
    int i;
    int ret;
    pthread_rwlock_t *pthread_rwlock_p;

    h->hash = hash;
    h->cmp = cmp;
    h->copy = NULL;
    h->size = size;
    h->buckets = xmalloc(size * sizeof (list_t));
    for (it = h->buckets; it != h->buckets + size; it++)
        list_init(it);
    h->lock_size = (lock_size > ROZOFS_BT_HTABLE_MAX_LOCK)?ROZOFS_BT_HTABLE_MAX_LOCK:lock_size;
    /*
    ** allocate the memory for the locks
    */
    h->lock_p = xmalloc(h->lock_size * sizeof(pthread_rwlock_t));
    if (h->lock_p == NULL)
    {
      errno = ENOMEM;
      return -1;
    }
    pthread_rwlock_p = h->lock_p;
    /*
    ** init of the cache line lock
    */
    for (i = 0; i < h->lock_size;i++,pthread_rwlock_p++)
    {
      ret = pthread_rwlock_init(pthread_rwlock_p, NULL);
      if (ret != 0) return -1;
    }

    return 0;

}


/*
**_____________________________________________________________
** Put a hash entry in the hastable in multithreaded mode
**
** @param h     : pointer to the hash table
** @param entry : The address of the hash entry
*/
static inline void rozofs_bt_htable_put_entry_th(rozofs_bt_htable * h, hash_entry_t * entry,uint32_t hash) {
  list_t *bucket;
  pthread_rwlock_t *pthread_rwlock_p;
  uint32_t lock_idx;
  
  bucket = h->buckets + (hash % h->size);
  pthread_rwlock_p = h->lock_p;
  lock_idx = hash%h->lock_size;
  pthread_rwlock_p+=lock_idx;

  /*______________________________________________________
  **
  ** -- W A R N I N G -- W A R N I N G -- W A R N I N G --
  **
  **    THE EXISTENCE OF THE ENTRY SHOULD HAVE BEEN  
  **       PREVIOUSLY TESTED THANKS TO A LOOKUP
  **
  ** -- W A R N I N G -- W A R N I N G -- W A R N I N G --
  **______________________________________________________  
  */
  list_init(&entry->list);
  pthread_rwlock_wrlock(pthread_rwlock_p);  
  list_push_front(bucket, &entry->list);
  pthread_rwlock_unlock(pthread_rwlock_p);
}


/*
**________________________________________________________________
*/
/**
*  Get an entry from the hash table

  @param h: pointer to the hash table
  @param key: key to search
  @param hash : hash value of the key
  
  @retval NULL if not found
  @retval <> NULL : entry found
*/
void *rozofs_bt_htable_get_th(rozofs_bt_htable * h, void *key,uint32_t hash) {
    list_t *p;
    pthread_rwlock_t *pthread_rwlock_p;
    uint32_t lock_idx;
    
    pthread_rwlock_p = h->lock_p;
    lock_idx = hash%h->lock_size;
    pthread_rwlock_p+=lock_idx;
    /*
    ** take the read lock because of LRU handling
    */
    pthread_rwlock_rdlock(pthread_rwlock_p);
    list_for_each_forward(p, h->buckets + (hash % h->size)) {
        hash_entry_t *he = list_entry(p, hash_entry_t, list);
        if (h->cmp(he->key, key) == 0) {
            pthread_rwlock_unlock(pthread_rwlock_p);
            return he->value;
        }
    }
    pthread_rwlock_unlock(pthread_rwlock_p);
    return 0;
}

/*
**_____________________________________________________________
** Remove a hash entry from the hastable multithreaded mode
**
  @param key: key to search
  @param hash : hash value of the key

  @retval NULL if not found
  @retval <> NULL : entry found  
*/
void *rozofs_htable_del_entry_th(rozofs_bt_htable * h, void *key,uint32_t hash) {
    void *value = NULL;
    list_t *p, *q;
    pthread_rwlock_t *pthread_rwlock_p;
    uint32_t lock_idx;
    
    pthread_rwlock_p = h->lock_p;
    lock_idx = hash%h->lock_size;
    pthread_rwlock_p+=lock_idx;
    
    pthread_rwlock_wrlock(pthread_rwlock_p);

    list_for_each_forward_safe(p, q, h->buckets + (hash % h->size)) {
        hash_entry_t *he = list_entry(p, hash_entry_t, list);
        if (h->cmp(he->key, key) == 0) {
            value = he->value;
            list_remove(p);
            break;
        }
    }

    pthread_rwlock_unlock(pthread_rwlock_p);
    return value;
}

/*
**__________________________________________________________________
*/
/**
*   init of an exportd attribute cache

    @param: pointer to the cache context
    
    @retval none
*/
int rozofs_bt_dirent_cache_initialize() {
    int ret;
    rozofs_bt_dirent_cache_t *cache = &rozofs_bt_dirent_cache;
    rozofs_bt_thread_ctx_t * thread_ctx_p;
    int                        err;
    pthread_attr_t             attr;
   rozofs_bt_dirent_garbage_private_t *private_p;
    
    thread_ctx_p = &rozofs_bt_dirent_garbage_collector_thread_ctx;
    
    memset(cache,0,sizeof(rozofs_bt_dirent_cache_t));
    cache->max = common_config.dirent_cache_size; // ROZOFS_BT_MAX_CACHE_ENTRIES;
    cache->garbage_collector_deadline_delay_sec =common_config.dirent_garbage_delay; // ROZOFS_BT_DIRENT_GARBAGE_DEADLINE_SEC;
    
    ret = rozofs_bt_htable_initialize_th(&cache->htable, ROZOFS_BT_LEVEL0_HASH,ROZOFS_BT_LEVEL0_HASH, NULL, rozofs_bt_dirent_cache_cmp);
    if (ret < 0) return ret;

    list_init(&cache->lru);
    list_init(&cache->del_list);   
    /*
    ** init of the lru & deletion lock
    */
    ret = pthread_rwlock_init(&cache->lru_lock, NULL);
    if (ret < 0) return ret;
    ret = pthread_rwlock_init(&cache->del_lock, NULL);
    if (ret < 0) return ret;    
    /*
    ** prepare the lookup call back
    */
    dirent_bt_cache_cbk.batch_mode_enable = 1;
    dirent_bt_cache_cbk.get = bt_dirent_cache_bucket_search_entry;
    dirent_bt_cache_cbk.put = bt_dirent_put_root_entry_to_cache;
    dirent_bt_cache_cbk.remove = bt_dirent_remove_root_entry_from_cache;
    /*
    ** create the garbage collector thread
    */
    thread_ctx_p->thread_private = malloc(sizeof(rozofs_bt_dirent_garbage_private_t));
    if (thread_ctx_p->thread_private == NULL) 
    {
       fatal("Out of memory");
       return -1;
    }
    memset(thread_ctx_p->thread_private,0,sizeof(rozofs_bt_dirent_garbage_private_t));
    private_p = thread_ctx_p->thread_private;
    private_p->period_sec = ROZOFS_BT_DIRENT_GARBAGE_PTHREAD_FREQUENCY_SEC;   
    private_p->deletion_enable = 1;
    err = pthread_attr_init(&attr);
    if (err != 0) {
      fatal("rozofs_bt_thread_create pthread_attr_init %s",strerror(errno));
      return -1;
    }  

    err = pthread_create(&thread_ctx_p->thrdId,&attr,rozofs_bt_dirent_garbage_collector_thread,thread_ctx_p);
    if (err != 0) {
      fatal("rozofs_bt_thread_create pthread_create %s", strerror(errno));
      return -1;
    }      
    
    return 0;
}

/*
**__________________________________________________________________
*/
/**
 *  Search a root dirent file reference in the cache
 *
   @param cache : pointer to the main cache structure
   @param index : index of the root dirent file
   @param fid   : fid of the parent directory

   @retval <>NULL: pointer to the root dirent cache entry
   @retval ==NULL:no entry
 */
mdirents_cache_entry_t *bt_dirent_cache_bucket_search_entry(fid_t fid, uint16_t index)
{
  uint32_t hash_value;
  rozofs_bt_dirent_cache_t *cache = &rozofs_bt_dirent_cache;
  mdirents_cache_entry_t *dentry_p;
  mdirents_cache_key_t key;


    memcpy(key.dir_fid,fid,sizeof(fid_t));
    key.dirent_root_idx = index;
    /*
     ** compute the hash value for the bucket and the bucket_entry
     */
    hash_value = dirent_cache_bucket_hash_fnv(0, fid, &index);
    
    dentry_p = rozofs_bt_htable_get_th(&cache->htable,&key,hash_value);
    if (dentry_p != NULL)
    {
      /*
      ** if the entry is found, update the lru
      */
      pthread_rwlock_wrlock(&cache->lru_lock); 
      list_remove(&dentry_p->lru_link); 
      list_push_front(&cache->lru, &dentry_p->lru_link);
      pthread_rwlock_unlock(&cache->lru_lock);    
      cache->hit++;
      return dentry_p;
    }
    cache->miss++;
    return NULL;
}


/*
 **______________________________________________________________________________
 */

/**
 * Attempt to put in cache entry associated with the root dirent file
 *
 * @param dirfd: file descriptor of the parent directory
 * @param *name: pointer to the name of the mdirentry to put
 * @param fid_parent: unique identifier of the parent directory
 *  @param fid: unique identifier of the mdirentry to put
 * @param type: type of the mdirentry to put
 *
 * @retval  <>NULL: pointer to the dirent cache entry
 * @retval NULL-> not found
 */

int bt_dirent_put_root_entry_to_cache(fid_t fid, int root_idx, mdirents_cache_entry_t *root_p)
{

  rozofs_bt_dirent_cache_t *cache = &rozofs_bt_dirent_cache;
  mdirents_cache_key_t key;
  rozofs_bt_htable * h;
  uint32_t lock_idx;
  pthread_rwlock_t *pthread_rwlock_p;    
  uint16_t index;
  
  index = (uint16_t) root_idx;
  h = &cache->htable;
  while (cache->size >= cache->max)
  {
    mdirents_cache_entry_t *lru;
    pthread_rwlock_wrlock(&cache->lru_lock); 
    if (list_empty(&cache->lru))
    {
      pthread_rwlock_unlock(&cache->lru_lock); 
      break;
    }    
    /*
    ** remove the entry from the LRU linked list and update the number of entries on the cache
    */        
    lru = list_entry(cache->lru.prev, mdirents_cache_entry_t, lru_link);  
    list_remove(&lru->lru_link); 
    cache->size--;
    pthread_rwlock_unlock(&cache->lru_lock); 
    /*
    ** remove it from the hash cache line
    */
    pthread_rwlock_p = h->lock_p;
    lock_idx = lru->hash%h->lock_size;
    pthread_rwlock_p+=lock_idx;    
    
    pthread_rwlock_wrlock(pthread_rwlock_p);    
    list_remove(&lru->he.list);     
    pthread_rwlock_unlock(pthread_rwlock_p);  
    /*
    ** insert the entry in to delete list
    */
    pthread_rwlock_wrlock(&cache->del_lock);  
    /*
    ** set the deadline time before deleting the cache 
    */
    lru->deadline_timestamp = time(NULL);
    list_push_back(&cache->del_list, &lru->he.list);
    cache->lru_del++;
    pthread_rwlock_unlock(&cache->del_lock);          
  }
  /*
  ** now insert the new entry in the hash table
  */
  memcpy(key.dir_fid,fid,sizeof(fid_t));
  key.dirent_root_idx = index;
  /*
   ** compute the hash value for the bucket and the bucket_entry
   */
  root_p->hash = dirent_cache_bucket_hash_fnv(0, fid, &index);  
  list_init(&root_p->lru_link);
  root_p->he.key   = &root_p->key;
  root_p->he.value = root_p;
  /*
  ** insert on the lru
  */
  pthread_rwlock_wrlock(&cache->lru_lock);  
  list_push_front(&cache->lru, &root_p->lru_link);
  pthread_rwlock_unlock(&cache->lru_lock);    
  cache->size++;
  /*
  ** insert in the hash table
  */
  rozofs_bt_htable_put_entry_th(h, &root_p->he,root_p->hash);
  return 0;
}


/*
 **______________________________________________________________________________
 */

/**
 * Attempt to release the cache entry associated with the root dirent file
 *
 * @param dirfd: file descriptor of the parent directory
 * @param *name: pointer to the name of the mdirentry to put
 * @param fid_parent: unique identifier of the parent directory
 *  @param fid: unique identifier of the mdirentry to put
 * @param type: type of the mdirentry to put
 *
 * @retval  0 -> success
 * @retval -1 -> the entry does not exist
 */

int bt_dirent_remove_root_entry_from_cache(fid_t fid, int root_idx)
{
  rozofs_bt_dirent_cache_t *cache = &rozofs_bt_dirent_cache;
  mdirents_cache_entry_t *dentry_p;
  mdirents_cache_key_t key;
  rozofs_bt_htable * h;
  uint32_t hash;
  uint16_t index = (uint16_t) root_idx;

  h = &cache->htable;
  /*
  ** now insert the new entry in the hash table
  */
  memcpy(key.dir_fid,fid,sizeof(fid_t));
  key.dirent_root_idx = index;
  /*
   ** compute the hash value for the bucket and the bucket_entry
   */
  hash = dirent_cache_bucket_hash_fnv(0, fid, &index);  
  dentry_p = rozofs_htable_del_entry_th(h,&key,hash);
  if (dentry_p == NULL)
  {
    errno = ENOENT;
    return -1;
  }
  /*
  ** remove the entry from the LRU
  */
  pthread_rwlock_wrlock(&cache->lru_lock);   
  list_remove(&dentry_p->lru_link); 
  cache->size--;  
  pthread_rwlock_unlock(&cache->lru_lock); 
  /*
  ** insert in the to delete list
  */
  pthread_rwlock_wrlock(&cache->del_lock);  
  /*
  ** set the deadline time before deleting the cache 
  */
  dentry_p->deadline_timestamp = time(NULL);
  list_push_back(&cache->del_list, &dentry_p->he.list);
  cache->lru_del++;
  pthread_rwlock_unlock(&cache->del_lock);   
  return 0;      
}



/**
**_______________________________________________________________________________________
*/
/**
  Flush any entry that has expired within the tracking file payload garbage collector
  
  @param thread_ctx_p: pointer to the rcu thread context
  
  @retval none
*/
#define ROZOFS_BT_DIRENT_MAX_FLUSH 32

void rozofs_bt_dirent_flush_garbage_collector(rozofs_bt_thread_ctx_t *thread_ctx_p)
{
    list_t *p, *q;
    time_t cur_time;
    rozofs_bt_dirent_garbage_private_t *private_p = NULL;
    rozofs_bt_dirent_cache_t *cache = &rozofs_bt_dirent_cache;
    mdirents_cache_entry_t *dentry_p;
    mdirents_cache_entry_t *dentry_tab[ROZOFS_BT_DIRENT_MAX_FLUSH];
    int current_count = 0;
    int i;

    
    private_p = thread_ctx_p->thread_private;
    
    if (cache->lru_del == 0) 
    {
      /*
      ** the garbage collector is empty
      */
      return;
    }
    cur_time = time(NULL);
    /*
    ** go through the garbage collector remove all the entries that have reached their deadline
    */
    pthread_rwlock_wrlock(&cache->del_lock);  ;

    list_for_each_forward_safe(p, q, &cache->del_list) {
        dentry_p = list_entry(p, mdirents_cache_entry_t, he.list);
        /*
	** check if the deadline has been reached
	*/
	if ((dentry_p->deadline_timestamp +cache->garbage_collector_deadline_delay_sec) > cur_time) break;
	/*
	** the deadline is reached, remove it from the garbage collector and release the memory
	*/
	list_remove(p);
	cache->lru_del--;
	private_p->garbage_collector_deletion++;
	dentry_tab[current_count] = dentry_p;
	current_count++;
	if (current_count >= ROZOFS_BT_DIRENT_MAX_FLUSH) break;

    }

   pthread_rwlock_unlock(&cache->del_lock);
   /*
   ** now release the memory
   */
   for (i = 0;i < current_count; i++)
   {
     dentry_p = dentry_tab[i];
     /*
     ** check if it is still queued on the lru
     **  That situation might happen when we need to release some entries because
     **  we reach the max cache size and at the same time we have a get on the entry
     **  that is released because of the cache size.
     **  Since there are 2 locks (one for the hash entry and one for the lru) the operation
     **  cannot be atomic
     ** So it might be possible that the entry has been requeued on the LRU, so we have to
     ** check that case at the time we release the memory of the entry.
     */
     if (list_empty(&dentry_p->lru_link) == 0)
     {
        /*
	** Remove the entry from LRU
	*/
	pthread_rwlock_wrlock(&cache->lru_lock);   
	list_remove(&dentry_p->lru_link); 
	pthread_rwlock_unlock(&cache->lru_lock); 
     }
     /*
     ** now release the memory allocated to cache the dirent files
     */
     dirent_cache_release_entry(dentry_p);
   }
}


/*
 *_______________________________________________________________________
 */
/** Share memory deletion polling thread
 */

 
static void *rozofs_bt_dirent_garbage_collector_thread(void *v) {

   rozofs_bt_thread_ctx_t * thread_ctx_p = (rozofs_bt_thread_ctx_t*)v;

   struct timespec ts = {ROZOFS_BT_DIRENT_GARBAGE_PTHREAD_FREQUENCY_SEC, 0};
   rozofs_bt_dirent_garbage_private_t *private_p = NULL;
   uint64_t tic,toc;
  struct timeval tc;
    rozofs_bt_dirent_cache_t *cache = &rozofs_bt_dirent_cache;
   
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    
    private_p = thread_ctx_p->thread_private;

    uma_dbg_thread_add_self("dentry_garbage");
    for (;;) {
	ts.tv_sec = private_p->period_sec;    

        nanosleep(&ts, NULL);  
	/*
	** reload the max cache size and the garbage collector delay since it might have changed
	*/
	cache->max = common_config.dirent_cache_size;
	rozofs_root_bmap_max_ientries = common_config.dirent_cache_size;
	cache->garbage_collector_deadline_delay_sec =common_config.dirent_garbage_delay;
	rozofs_bt_root_btmap_thread_garbage_collector_deadline_delay_sec = common_config.dirent_garbage_delay;
	/*
	** if deletion is forbibden, do nanosleep again
	*/
	if  (private_p->deletion_enable ==0) continue;
	tic = rozofs_get_ticker_us();
	/*
	** Garbage collector check
	*/
	rozofs_bt_dirent_flush_garbage_collector(thread_ctx_p);
	/*
	** Flush the garbage collector of the root bitmap entries
	*/
	private_p->garbage_collector_btmap_deletion+= rozofs_bt_root_btmap_flush_garbage_collector();
	gettimeofday(&tc,(struct timezone *)0);
        toc = rozofs_get_ticker_us();
	private_p->polling_count++;
	private_p->polling_time +=(toc-tic);
    }
    return 0;
}

