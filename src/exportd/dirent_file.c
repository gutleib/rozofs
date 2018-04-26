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

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/export_track.h>
#include <rozofs/rpc/epproto.h>
#include <rozofs/rpc/export_profiler.h>
#include <rozofs/core/rozofs_string.h>

#include "mdir.h"
#include "mdirent.h"
#include "dirent_journal.h"

/** @defgroup DIRENT_CACHE_LVL0 Level 0 cache
 *  This module provides services related to level 0 cache\n

   The Level 0 cache is used to cache mdirent root file only. When a mdirent root file has some
   collision midrent file associated with it, they are referenced inside the memory representation
   of the mdirent file.
   As the consequence, the  insertion/removal  of a root mdirent file in the level cache 0 implies
   also the insertion of the associated collision file, however to access to a mdirent collision
   entry is always performed indirectly accross its associated mdirent root file cache entry.\n

   The level 0 cache is organized as follows\n
   - 64K buckets\n
   - Each bucket supports up to 256 collisions entries\n

   For an application standpoint, it is possible to enable/disable the Level 0 cache. \n

   Here is an example of the output of the level 0 cache statistics:\n
 <div class="fragment"><pre class="fragment">
Level 0 cache state : Enabled
Number of entries insert in level 0 cache 4096
hit/miss 1568768/4096
collisions cumul  level0/level1 54959/0
collisions Max level0/level1 1/0
 </pre></div>
 */
#define DIRENT_BUCKET_DEPTH_IN_BIT   16

#define DIRENT_BUCKET_MAX_ROOT_DIRENT (256000)
//#define DIRENT_BUCKET_MAX_ROOT_DIRENT (4)

#define DIRENT_BUCKET_MAX_COLLISIONS  256  /**< number of collisions that can be supported by a bucket  */
#define DIRENT_BUCKET_MAX_COLLISIONS_BYTES  (DIRENT_BUCKET_MAX_COLLISIONS/8)  /**< number of collisions that can be supported by a bucket  */
#define DIRENT_BUCKET_NB_ENTRY_PER_ARRAY  32 /**< number of dirent_cache_bucket_entry_t strcuture per memory array */
#define DIRENT_BUCKET_ENTRY_MAX_ARRAY (DIRENT_BUCKET_MAX_COLLISIONS/DIRENT_BUCKET_NB_ENTRY_PER_ARRAY)

typedef struct _dirent_cache_bucket_entry_t {
    uint16_t hash_value_table[DIRENT_BUCKET_NB_ENTRY_PER_ARRAY]; /**< table of the hash value applied to the parent_fid and index */
    void *entry_ptr_table[DIRENT_BUCKET_NB_ENTRY_PER_ARRAY]; /**< table of the dirent cache entries: used for doing the exact match */
} dirent_cache_bucket_entry_t;

/**
 *  dirent cache structure
 */
typedef struct _dirent_cache_bucket_t {
    list_t bucket_lru_link; /**< link list for bucket LRU  */
    uint8_t bucket_free_bitmap[DIRENT_BUCKET_MAX_COLLISIONS_BYTES]; /**< bitmap of the free entries  */
    dirent_cache_bucket_entry_t * entry_tb[DIRENT_BUCKET_ENTRY_MAX_ARRAY]; /**< pointer to the memory array that contains the entries */

} dirent_cache_bucket_t;

typedef struct _dirent_cache_main_t {
    uint32_t max; /**< maximum number of entries in the cache */
    uint32_t size; /**< current number of entries in the cache */
    list_t global_lru_link; /**< entries cached: used for LRU           */
    dirent_cache_bucket_t *htable; /**< pointer to the bucket array of the cache */
} dirent_cache_main_t;

int dirent_cache_bucket_remove_entry(dirent_cache_main_t *cache, fid_t fid, uint16_t index);

/*
 ** Dirent level 0 cache
 */
dirent_cache_main_t dirent_cache_level0;

uint32_t dirent_buckect_cache_initialized = 0; /**< assert to 1 once cache is initialized */
uint32_t dirent_bucket_cache_append_counter = 0;
uint64_t dirent_bucket_cache_hit_counter = 0;
uint64_t dirent_bucket_cache_miss_counter = 0;
uint64_t dirent_bucket_cache_collision_counter = 0;
uint64_t dirent_bucket_cache_lru_counter_global = 0;
uint64_t dirent_bucket_cache_lru_counter_coll = 0;
uint64_t dirent_bucket_cache_lru_global_error = 0;
uint64_t dirent_bucket_cache_lru_coll_error = 0;
uint64_t dirent_bucket_cache_collision_level0_counter = 0;
int dirent_bucket_cache_max_level0_collisions = 0; /**< max number of collision at level 0  */
int dirent_bucket_cache_max_level1_collisions = 0; /**< max number of collision at level 1  */

int dirent_root_read_only = 0;
void *dirent_cur_root_idx_bitmap_p;


#if 1
uint32_t dirent_bucket_cache_enable = 1;
#else
#warning dirent cache is disable
uint32_t dirent_bucket_cache_enable = 0;
#endif
uint8_t dirent_cache_safe_enable = 1;



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
 **______________________________________________________________________________
 */

/**
 *  API for init of the dirent level 0 cache
 @param cache: pointer to the cache descriptor

 @retval none
 */
void dirent_cache_level0_initialize() {
    /*
     ** check if the cache has already been initialized
     */
    if (dirent_buckect_cache_initialized) return;

    dirent_cache_bucket_t *p;
    dirent_cache_main_t *cache = &dirent_cache_level0;
    cache->max = DIRENT_BUCKET_MAX_ROOT_DIRENT;
    cache->size = 0;
    list_init(&cache->global_lru_link);
    /*
     ** Allocate the memory to handle the buckets
     */
    cache->htable = xmalloc(sizeof (dirent_cache_bucket_t)*(1 << DIRENT_BUCKET_DEPTH_IN_BIT));
    if (cache->htable == NULL) {
        DIRENT_SEVERE("dirent_cache_level0_initialize out of memory (%u) at line %d\n", (unsigned int) sizeof (dirent_cache_bucket_t)*(1 << DIRENT_BUCKET_DEPTH_IN_BIT),
                __LINE__);
        exit(0);
    }
    /*
     ** init of the buckets
     */
    int i;
    p = cache->htable;
    for (i = 0; i < (1 << DIRENT_BUCKET_DEPTH_IN_BIT); i++, p++) {
        list_init(&p->bucket_lru_link);
        memset(&p->bucket_free_bitmap, 0xff, DIRENT_BUCKET_MAX_COLLISIONS_BYTES);
        memset(&p->entry_tb, 0, DIRENT_BUCKET_ENTRY_MAX_ARRAY * sizeof (void*));
    }
    dirent_buckect_cache_initialized = 1;
}

/**
 *  Insert a root dirent file reference in the cache
 *  Note : the bitmap is aligned on a 8 byte boundary, so we can perform
  control by using a uint64_t

   @param cache : pointer to the main cache structure
   @param index : index of the root dirent file
   @param fid   : fid of the parent directory
   @param entry   : pointer to the entry to insert

   @retval 0 -> success
   @retval -1 -> failure, entry has not been inserted  .
 */
int fdl_bug_max_coll = 0;

int dirent_cache_bucket_insert_entry(dirent_cache_main_t *cache, fid_t fid, uint16_t index, void *entry) {
    uint8_t *bitmap_p;
    dirent_cache_bucket_t *bucket_p;
    int coll_idx = 0;
    int next_coll_idx = 0;
    uint8_t chunk_u8_idx;
    int bit_idx;
    int loop_cnt;
    int bucket_entry_arrray_idx;
    dirent_cache_bucket_entry_t *cache_bucket_entry_p;
    int local_idx;

    uint32_t hash_value;
    uint16_t hash_bucket;
    uint16_t hash_bucket_entry;
    

    /*
     ** compute the hash value for the bucket and the bucket_entry
     */
    hash_value = dirent_cache_bucket_hash_fnv(0, fid, &index);
    hash_bucket = (hash_value >> 16) ^ (hash_value & 0xffff);

    hash_bucket_entry = (uint16_t) (hash_value & 0xffff);
    
    /*
    ** LRU handling: check for cache full condition: release one entry from the global link
    */
    if (cache->size >= cache->max)
    {
      int ret;
      mdirents_cache_entry_t *cache_entry_lru_p = list_entry(cache->global_lru_link.prev, mdirents_cache_entry_t, cache_link);
      ret = dirent_cache_bucket_remove_entry(cache,cache_entry_lru_p->key.dir_fid,cache_entry_lru_p->key.dirent_root_idx);
      if (ret == -1) 
      {
        /*
        ** not really normal
        */
        dirent_bucket_cache_lru_global_error++;
        severe("Debug fail to Remove %p index %d ",cache_entry_lru_p,-1 );
        return -1;      
      }
      /*
      ** release the memory allocated for storing the dirent file
      */
      dirent_bucket_cache_lru_counter_global++;
      dirent_cache_release_entry(cache_entry_lru_p);
    }
    /*
     ** set the pointer to the bucket and load up the pointer to the bitmap
     */
reloop:
    bucket_p = &cache->htable[hash_bucket];
    bitmap_p = bucket_p->bucket_free_bitmap;
    coll_idx = 0;
    next_coll_idx = 0;

    while (coll_idx < DIRENT_BUCKET_MAX_COLLISIONS) {
        if (coll_idx % 8 == 0) {
            /*
             ** skip the entries that are alreadt allocated
             */
            next_coll_idx = check_bytes_val(bitmap_p, coll_idx, DIRENT_BUCKET_MAX_COLLISIONS, &loop_cnt, 0);
            if (next_coll_idx < 0) break;
            coll_idx = next_coll_idx;
        }
        /*
         ** check if the return bit is free
         */
        chunk_u8_idx = coll_idx / 8;
        bit_idx = coll_idx % 8;
        if ((bitmap_p[chunk_u8_idx] & (1 << bit_idx)) == 0) {
            /*
             ** the entry is busy, check the next one
             */
            coll_idx++;
            continue;
        }
#if 1
        if (coll_idx > dirent_bucket_cache_max_level0_collisions) {
            dirent_bucket_cache_max_level0_collisions = coll_idx;
        }

#endif
        /*
         ** allocate the entry by clearing the associated bit
         */
        dirent_clear_chunk_bit(coll_idx, bitmap_p);
        /*
         **  OK we found one, check if the memory has been allocated to store the entry
         ** this will depend on the value of the coll_idx
         */
        bucket_entry_arrray_idx = coll_idx / DIRENT_BUCKET_NB_ENTRY_PER_ARRAY;
        /*
         ** we need to allocated memory of the associated pointer is NULL
         */
        cache_bucket_entry_p = bucket_p->entry_tb[bucket_entry_arrray_idx];
        if (cache_bucket_entry_p == (dirent_cache_bucket_entry_t*) NULL) {
            cache_bucket_entry_p = (dirent_cache_bucket_entry_t*) malloc(sizeof (dirent_cache_bucket_entry_t));
            if (cache_bucket_entry_p == NULL) {
                DIRENT_WARN("dirent_cache_buckat_allocate_entry out of memory at %d\n", __LINE__);
                return -1;
            }
            bucket_p->entry_tb[bucket_entry_arrray_idx] = cache_bucket_entry_p;
            memset(cache_bucket_entry_p, 0, sizeof (dirent_cache_bucket_entry_t));
        }
        /*
         ** OK, now insert the entry
         */
        local_idx = coll_idx % DIRENT_BUCKET_NB_ENTRY_PER_ARRAY;
        cache_bucket_entry_p->hash_value_table[local_idx] = hash_bucket_entry;
        cache_bucket_entry_p->entry_ptr_table[local_idx] = entry;
        /*
        ** do the job for LRU
        */
        {
          mdirents_cache_entry_t *dirent_cache_p = (mdirents_cache_entry_t*)entry;
          list_push_front(&cache->global_lru_link, &dirent_cache_p->cache_link);
          list_push_front(&bucket_p->bucket_lru_link, &dirent_cache_p->coll_link);
          cache->size++;
        }
        return 0;
    }
    /*
    ** Out of entries-> need to go through bucket LRU-> remove the oldest one
    */
    {
      int ret;
      mdirents_cache_entry_t *cache_entry_lru_p = list_entry(bucket_p->bucket_lru_link.prev, mdirents_cache_entry_t, coll_link);
      ret = dirent_cache_bucket_remove_entry(cache,cache_entry_lru_p->key.dir_fid,cache_entry_lru_p->key.dirent_root_idx);
      if (ret == -1) 
      {
        /*
        ** not really normal
        */
        dirent_bucket_cache_lru_coll_error++;
        return -1;      
      }
      /*
      ** release the memory allocated for storing the dirent file
      */
      dirent_bucket_cache_lru_counter_coll++;
      dirent_cache_release_entry(cache_entry_lru_p);
    }
    goto reloop;
}

/**
 *  Search a root dirent file reference in the cache
 *
   @param cache : pointer to the main cache structure
   @param index : index of the root dirent file
   @param fid   : fid of the parent directory

   @retval <>NULL: pointer to the root dirent cache entry
   @retval ==NULL:no entry
 */
mdirents_cache_entry_t *dirent_cache_bucket_search_entry(dirent_cache_main_t *cache, fid_t fid, uint16_t index) {
    uint8_t *bitmap_p;
    dirent_cache_bucket_t *bucket_p;
    int coll_idx = 0;
    int next_coll_idx = 0;
    int coll_idx_level1 = 0;
    uint8_t chunk_u8_idx;
    int bit_idx;
    int loop_cnt;
    int bucket_entry_arrray_idx;
    dirent_cache_bucket_entry_t *cache_bucket_entry_p;
    int local_idx;

    uint32_t hash_value;
    uint16_t hash_bucket;
    uint16_t hash_bucket_entry;

    /*
     ** compute the hash value for the bucket and the bucket_entry
     */
    hash_value = dirent_cache_bucket_hash_fnv(0, fid, &index);
    hash_bucket = (hash_value >> 16) ^ (hash_value & 0xffff);

    hash_bucket_entry = (uint16_t) (hash_value & 0xffff);

    /*
     ** set the pointer to the bucket and load up the pointer to the bitmap
     */
    bucket_p = &cache->htable[hash_bucket];
    bitmap_p = bucket_p->bucket_free_bitmap;

    /*
     ** search among the bit that indicates a busy entry
     */
    while (coll_idx < DIRENT_BUCKET_MAX_COLLISIONS) {
        if (coll_idx % 8 == 0) {
            /*
             ** skip the entries that are free
             */
            next_coll_idx = check_bytes_val(bitmap_p, coll_idx, DIRENT_BUCKET_MAX_COLLISIONS, &loop_cnt, 1);
            if (next_coll_idx < 0) break;
            coll_idx = next_coll_idx;
        }
        /*
         ** check if the return bit is busy
         */
        chunk_u8_idx = coll_idx / 8;
        bit_idx = coll_idx % 8;
        if ((bitmap_p[chunk_u8_idx] & (1 << bit_idx)) != 0) {
            /*
             ** the entry is free, check the next one
             */
            coll_idx++;
            continue;
        }
        /*
         ** we have a busy entry: check the hash value of the entry
         */
        bucket_entry_arrray_idx = coll_idx / DIRENT_BUCKET_NB_ENTRY_PER_ARRAY;
        cache_bucket_entry_p = bucket_p->entry_tb[bucket_entry_arrray_idx];
        if (cache_bucket_entry_p == NULL) {
            DIRENT_SEVERE("dirent_cache_bucket_search_entry: busy entry but no pointer at line %d\n", __LINE__);
            return NULL;
        }
        /*
         ** OK, now check the entry
         */
        local_idx = coll_idx % DIRENT_BUCKET_NB_ENTRY_PER_ARRAY;
        if (cache_bucket_entry_p->hash_value_table[local_idx] != hash_bucket_entry) {
            /*
             ** not the right hash value, check the next entry
             */
            dirent_bucket_cache_collision_level0_counter++;
            coll_idx++;
            continue;
        }
        /*
         ** There is match on the hash entry: check the fid and the index of the dirent cache entry reference in that
         ** bucket entry
         */

        mdirents_cache_entry_t *cache_entry_p = cache_bucket_entry_p->entry_ptr_table[local_idx];
        if (cache_entry_p->key.dirent_root_idx != index) {
            /*
             ** not the right entry, check next one
             */
            coll_idx_level1++;
            dirent_bucket_cache_collision_counter++;
            coll_idx++;
            continue;
        }
        /*
         ** check the fid
         */
        if (uuid_compare(fid, cache_entry_p->key.dir_fid) != 0) {
            /*
             ** not the right entry, check next one
             */
            coll_idx++;
            coll_idx_level1++;
            dirent_bucket_cache_collision_counter++;
            continue;
        }
        /*
         ** OK, we got the match, return the pointer to the entry
         */
#if 1
        if (coll_idx_level1 > dirent_bucket_cache_max_level1_collisions) {
            dirent_bucket_cache_max_level1_collisions = coll_idx_level1;
        }
#endif
        dirent_bucket_cache_hit_counter++;
        /*
        ** do the job for LRU
        */
        {
          list_remove(&cache_entry_p->cache_link);
          list_remove(&cache_entry_p->coll_link);
          list_push_front(&cache->global_lru_link, &cache_entry_p->cache_link);
          list_push_front(&bucket_p->bucket_lru_link, &cache_entry_p->coll_link);
        }
        return cache_entry_p;

    }
    dirent_bucket_cache_miss_counter++;
    return NULL;
}

/**
 *  Remove a root dirent file reference from the cache
 *
   @param cache : pointer to the main cache structure
   @param index : index of the root dirent file
   @param fid   : fid of the parent directory

   @retval 0 : success
   @retval -1 : not found
 */
int dirent_cache_bucket_remove_entry(dirent_cache_main_t *cache, fid_t fid, uint16_t index) {
    uint8_t *bitmap_p;
    dirent_cache_bucket_t *bucket_p;
    int coll_idx = 0;
    int next_coll_idx = 0;
    uint8_t chunk_u8_idx;
    int bit_idx;
    int loop_cnt;
    int bucket_entry_arrray_idx;
    dirent_cache_bucket_entry_t *cache_bucket_entry_p;
    int local_idx;

    uint32_t hash_value;
    uint16_t hash_bucket;
    uint16_t hash_bucket_entry;

    /*
     ** compute the hash value for the bucket and the bucket_entry
     */
    hash_value = dirent_cache_bucket_hash_fnv(0, fid, &index);
    hash_bucket = (hash_value >> 16) ^ (hash_value & 0xffff);

    hash_bucket_entry = (uint16_t) (hash_value & 0xffff);
    /*
     ** set the pointer to the bucket and load up the pointer to the bitmap
     */
    bucket_p = &cache->htable[hash_bucket];
    bitmap_p = bucket_p->bucket_free_bitmap;
    /*
     ** search among the bit that indicates a busy entry
     */

    while (coll_idx < DIRENT_BUCKET_MAX_COLLISIONS) {
        if (coll_idx % 8 == 0) {
            /*
             ** skip the entries that are free
             */
            next_coll_idx = check_bytes_val(bitmap_p, coll_idx, DIRENT_BUCKET_MAX_COLLISIONS, &loop_cnt, 1);
            if (next_coll_idx < 0) break;
            coll_idx = next_coll_idx;
        }
        /*
         ** check if the return bit is busy
         */
        chunk_u8_idx = coll_idx / 8;
        bit_idx = coll_idx % 8;
        if ((bitmap_p[chunk_u8_idx] & (1 << bit_idx)) != 0) {
            /*
             ** the entry is free, check the next one
             */
            coll_idx++;
            continue;
        }
        /*
         ** we have a busy entry: check the hash value of the entry
         */
        bucket_entry_arrray_idx = coll_idx / DIRENT_BUCKET_NB_ENTRY_PER_ARRAY;
        cache_bucket_entry_p = bucket_p->entry_tb[bucket_entry_arrray_idx];
        if (cache_bucket_entry_p == NULL) {
            DIRENT_SEVERE("dirent_cache_bucket_remove_entry: busy entry but no pointer at line %d\n", __LINE__);
            return -1;
        }
        /*
         ** OK, now check the entry
         */
        local_idx = coll_idx % DIRENT_BUCKET_NB_ENTRY_PER_ARRAY;
        if (cache_bucket_entry_p->hash_value_table[local_idx] != hash_bucket_entry) {
            /*
             ** not the right hash value, check the next entry
             */
            dirent_bucket_cache_collision_level0_counter++;
            coll_idx++;
            continue;
        }
        /*
         ** There is match on the hash entry: check the fid and the index of the dirent cache entry reference in that
         ** bucket entry
         */
        mdirents_cache_entry_t *cache_entry_p = cache_bucket_entry_p->entry_ptr_table[local_idx];
        if (cache_entry_p->key.dirent_root_idx != index) {
            /*
             ** not the right entry, check next one
             */
            dirent_bucket_cache_collision_counter++;
            coll_idx++;
            continue;
        }
        /*
         ** check the fid
         */
        if (uuid_compare(fid, cache_entry_p->key.dir_fid) != 0) {
            /*
             ** not the right entry, check next one
             */
            coll_idx++;
            dirent_bucket_cache_collision_counter++;
            continue;
        }
        /*
        ** do the job for LRU
        */
        {
          list_remove(&cache_entry_p->cache_link);
          list_remove(&cache_entry_p->coll_link);          
          cache->size--;
        }
        /*
         **________________________________________________
         ** OK, we got the match, remove it from cache
         **________________________________________________
         */
        /*
         ** clear the entry by clearing the associated bit
         */
        dirent_set_chunk_bit(coll_idx, bitmap_p);
        cache_bucket_entry_p->hash_value_table[local_idx] = 0;
        /*
         **  check if the bucket array has to be released
         */
        int release_req = 1;
        int i;
        for (i = 0; i < DIRENT_BUCKET_NB_ENTRY_PER_ARRAY / 8; i++) {
            if (bitmap_p[bucket_entry_arrray_idx + i] == 0xff) continue;
            release_req = 0;
            break;

        }
        if (release_req) {
            free(bucket_p->entry_tb[bucket_entry_arrray_idx]);
            bucket_p->entry_tb[bucket_entry_arrray_idx] = NULL;
        }
        dirent_bucket_cache_hit_counter++;

        return 0;
    }
    /*
     ** nothing found
     */
    dirent_bucket_cache_miss_counter++;
    return -1;
}

/**
 *   Compute the hash values for the name and fid: called from listdir

 @param key1 : pointer to a string 
 @param key2 : pointer to a fid (16 bytes)
 @param hash2 : pointer to the second hash value that is returned
 @param len : len of the key 1(trailing \0 must not be included)

 @retval primary hash value
 */
static inline uint32_t filename_uuid_hash_fnv_with_len(uint32_t h, void *key1, void *key2, uint32_t *hash2, int len) {

    unsigned char *d = (unsigned char *) key1;
    int i = 0;

    if (h == 0) h = 2166136261U;
    /*
     ** hash on name
     */
    d = key1;
    for (i = 0; i <len ; d++, i++) {
        h = (h * 16777619)^ *d;

    }

    *hash2 = h;
    /*
     ** hash on fid
     */
    d = (unsigned char *) key2;
    for (d = key2; d != key2 + 16; d++) {
        h = (h * 16777619)^ *d;

    }
    return h;
}


#define DIRENT_ROOT_FILE_IDX_SHIFT 12
//warning DIRENT_ROOT_FILE_IDX_SHIFT is 12 
//#define DIRENT_ROOT_FILE_IDX_SHIFT 12

//#define DIRENT_ROOT_FILE_IDX_SHIFT 10
#define DIRENT_ROOT_FILE_IDX_MASK ((1<<DIRENT_ROOT_FILE_IDX_SHIFT)-1)
#define DIRENT_ROOT_FILE_IDX_MAX   (1 << DIRENT_ROOT_FILE_IDX_SHIFT)

#define DIRENT_ROOT_BUCKET_IDX_MASK ((1<<8)-1)


typedef struct _dirent_range_t
{
   uint64_t file_limit;  /**< max limit of file for the range  */
   int      mask;        /**< mask associated with the limit   */
   uint64_t count;       /**< put_dentry statistics            */
} dirent_range_t;
   
#define DIRENT_MAX_RANGE 3
dirent_range_t dirent_range_table[] = {
     {10000,0x1,0},   /**< 2 dirent root files max */
     {100000,0xf,0}, /**< 8 dirent root files max */
     {0,DIRENT_ROOT_FILE_IDX_MASK,0}  /**< 4096 dirent root file max */
};
/*
 **______________________________________________________________________________
 */

/**
 * Attempt to get the cache entry associated with the root dirent file
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

mdirents_cache_entry_t * dirent_get_root_entry_from_cache(fid_t fid, int root_idx) {
    if (dirent_bucket_cache_enable == 0) return NULL;
    return dirent_cache_bucket_search_entry(&dirent_cache_level0, fid, (uint16_t) root_idx);
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

int dirent_remove_root_entry_from_cache(fid_t fid, int root_idx) {
    if (dirent_bucket_cache_enable == 0) return 0;
    return dirent_cache_bucket_remove_entry(&dirent_cache_level0, fid, (uint16_t) root_idx);
}

/*
 ** Print the dirent cache bucket statistics
 */
char* dirent_cache_display(char *pChar) {
    int i;
    uint64_t malloc_size;
    
    malloc_size = DIRENT_MALLOC_GET_CURRENT_SIZE();
    
    pChar+=sprintf(pChar,"Malloc size (MB/B)             : %llu/%llu\n",(long long unsigned int)malloc_size/(1024*1024), 
                   (long long unsigned int)malloc_size);    
    pChar+=sprintf(pChar,"Level 0 cache state            : %s\n", (dirent_bucket_cache_enable == 0) ? "Disabled" : "Enabled");
    pChar+=sprintf(pChar,"Number of entries level 0      : %u\n", dirent_bucket_cache_append_counter);
    pChar+=sprintf(pChar,"hit/miss                       : %llu/%llu\n", 
            (long long unsigned int) dirent_bucket_cache_hit_counter,
            (long long unsigned int) dirent_bucket_cache_miss_counter);
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

    return pChar;
}

void dirent_cache_bucket_print_stats()
{
   char buffer[1024];
   dirent_cache_display(buffer);
   printf("%s\n",buffer);

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

int dirent_put_root_entry_to_cache(fid_t fid, int root_idx, mdirents_cache_entry_t *root_p) {
    if (dirent_bucket_cache_enable == 0) return -1;
    dirent_bucket_cache_append_counter++;
    return dirent_cache_bucket_insert_entry(&dirent_cache_level0, fid, (uint16_t) root_idx, (void*) root_p);
}

int dirent_append_entry = 0;
int dirent_update_entry = 0;

/*
**______________________________________________________________________________
*/
/**
*  Get the mask to apply according to the current number of file in the directory

  @param children : number of files
  @param hash1 : hash value for fid and name
  
  @retval root_idx : root file index
*/
int dirent_get_root_idx(uint64_t children,uint32_t hash1)
{
    int range;
    int root_idx;
    
    for (range = 0; range < DIRENT_MAX_RANGE;range++)
    {
       if (dirent_range_table[range].file_limit == 0) break;
       if (dirent_range_table[range].file_limit >= children) break;    
    } 
    /*
     ** attempt to get the root dirent file from cache
     */
    root_idx = hash1 & dirent_range_table[range].mask;
    return root_idx;
}

/*
 **______________________________________________________________________________
 */

/**
 * API to put a mdirentry in one parent directory
 *
  @param dirfd: file descriptor of the parent directory
  @param *name: pointer to the name of the mdirentry to put
  @param fid_parent: unique identifier of the parent directory
  @param fid: unique identifier of the mdirentry to put
  @param children: number of children
  @param type: type of the mdirentry to put
  @param mask : range index in which the entry has been stored
 *
 * @retval  0 on success (mask contains the mask of the dirent root file)
 * @retval -1 on failure
 */
int put_mdirentry(void *root_idx_bitmap_p,int dirfd, fid_t fid_parent_in, char * name, fid_t fid, uint32_t type,
                  mdirent_fid_name_info_t *fid_name_info_p,uint64_t children,int *mask) 
{

    int root_idx = 0;
    int bucket_idx = 0;
    int cached = 0;
    int status = -1;
    uint32_t hash1;
    uint32_t hash2;
    int len;
    int local_idx;
    mdirents_header_new_t dirent_hdr;
    mdirents_cache_entry_t *root_entry_p;
    mdirents_cache_entry_t *cache_entry_p;
    mdirents_cache_entry_t *last_modified_cache_entry;
    mdirents_name_entry_t name_entry;
    mdirents_name_entry_t *name_entry_p = NULL;
    mdirents_hash_entry_t *hash_entry_p = NULL;
    mdirents_hash_ptr_t mdirents_hash_ptr;
    int range;
    fid_t fid_parent;
    int may_exist= 1;
       
    *mask = -1; /* unknown mask */
    
    START_PROFILING(put_mdirentry);
    /*
    ** deassert de delete pending bit of the parent
    */
    memcpy(fid_parent,fid_parent_in,sizeof(fid_t));
    exp_metadata_inode_del_deassert(fid_parent);
    rozofs_inode_set_dir(fid_parent);
    
    if (fid_name_info_p != NULL)
    {
       memset(fid_name_info_p,0,sizeof(mdirent_fid_name_info_t));
    }
   /*
   ** set the pointer to the root idx bitmap
   */
   dirent_set_root_idx_bitmap_ptr(root_idx_bitmap_p);

    /*
    ** Allow a priori to read and write on the root cache entry
    ** In case of some error while reading the dirent files from 
    ** disk, the rigths may be downgraded to read only.
    */
    DIRENT_ROOT_SET_READ_WRITE();
    
    /*
     ** dirfd is the file descriptor associated with the parent directory
     */
    /*
     ** build a hash value based on the fid of the parent directory and the name to search
     */
    hash1 = filename_uuid_hash_fnv(0, name, fid_parent, &hash2, &len);
    for (range = 0; range < DIRENT_MAX_RANGE;range++)
    {
       if (dirent_range_table[range].file_limit == 0) break;
       if (dirent_range_table[range].file_limit >= children) break;    
    } 
    /*
     ** attempt to get the root dirent file from cache
     */
    root_idx = hash1 & dirent_range_table[range].mask;
    dirent_range_table[range].count++;
    *mask = dirent_range_table[range].mask;
    bucket_idx = ((hash2 >> 16) ^ (hash2 & 0xffff)) & DIRENT_ROOT_BUCKET_IDX_MASK;
    
    if (fid_name_info_p!= NULL) fid_name_info_p->root_idx = root_idx;

    root_entry_p = dirent_get_root_entry_from_cache(fid_parent, root_idx);
    if (root_entry_p == NULL) {
        /*
         ** dirent file is not in the cache need to read it from disk
         */
        dirent_hdr.type = MDIRENT_CACHE_FILE_TYPE;
        dirent_hdr.level_index = 0;
        dirent_hdr.dirent_idx[0] = root_idx;
        dirent_hdr.dirent_idx[1] = 0;
        root_entry_p = read_mdirents_file(dirfd, &dirent_hdr,fid_parent);
    } else {
        /*
         ** indicate the entry is present in the cache
         */
        cached = 1;
    }
    /*
     ** ok now it depends if the entry exist on not
     */
    if (root_entry_p != NULL) {
        if (cached == 0) {
            /**
             * fill up the key associated with the file
             */
            memcpy(root_entry_p->key.dir_fid, fid_parent, sizeof (fid_t));
            root_entry_p->key.dirent_root_idx = root_idx;
            /*
             ** attempt to insert it in the cache if not in cache
             */
            if (dirent_put_root_entry_to_cache(fid_parent, root_idx, root_entry_p) == 0) {
                /*
                 ** indicates that entry is present in the cache
                 */
                cached = 1;
            }
        }

        if (may_exist)
	{
          /*
           ** search if the entry exist and if so just replace the content of fid
           */
          cache_entry_p = dirent_cache_search_hash_entry(dirfd, root_entry_p,
                  bucket_idx,
                  hash2,
                  &local_idx,
                  (uint8_t *) name, (uint16_t) len,
                  &name_entry_p,
                  &hash_entry_p);
          if (cache_entry_p != NULL) {
              /*
               ** OK, we have found the entry either in one of the dirent_file associated with
               ** the dirent root file or in the dirent root file itself:
               ** We just need to update the fid and re-write the associated name entry array
               ** on disk
               */
              memcpy(name_entry_p->fid, fid, sizeof (fid_t));
              dirent_update_entry += 1;
	      if (fid_name_info_p != NULL)
	      {
		/*
		** save the information related to fid&name
		*/
		if (cache_entry_p->header.level_index != 0)
		{
		  fid_name_info_p->coll = 1;
		  fid_name_info_p->coll_idx = cache_entry_p->header.dirent_idx[1] ; 
		}
		fid_name_info_p->chunk_idx = hash_entry_p->chunk_idx ;
		fid_name_info_p->nb_chunk = hash_entry_p->nb_chunk ;
	      }
              /*
               ** just need to re-write the sector
               */
              if (dirent_write_name_array_to_disk(dirfd, cache_entry_p, hash_entry_p->chunk_idx) < 0) {
                  goto out;
              }
              status = 0;
              goto out;
          }
	}
    }
    /*
     ** The entry does not exist, we need to allocate a free hash entry from on of the dirent file
     ** associated with the root dirent file. If all the current dirent file (collision) are full
     ** a new dirent collision file (an a cache entry) will be created.
     **
     ** The creation is forbidden where the read_only flag is asserted.
     */
    if (root_entry_p == NULL) {
        if (DIRENT_ROOT_IS_READ_ONLY())
	{
	   /*
	   ** cannot write
	   */
	   errno = EIO;
	   goto out;
	
	}
        root_entry_p = dirent_cache_create_entry(&dirent_hdr,fid_parent);
        if (root_entry_p == NULL) {
            DIRENT_SEVERE("put_mdirentry at line %d\n", __LINE__);
            return -1;
        }
        /**
         * fill up the key associated with the file
         */
        memcpy(root_entry_p->key.dir_fid, fid_parent, sizeof (fid_t));
        root_entry_p->key.dirent_root_idx = root_idx;
        /**
         * try to insert it in the cache
         */
        if (dirent_put_root_entry_to_cache(fid_parent, root_idx, root_entry_p) == 0) {
            /*
             ** indicates that entry is present in the cache
             */
            cached = 1;
        }
    }
    dirent_append_entry += 1;
    cache_entry_p = dirent_cache_alloc_name_entry_idx(root_entry_p, bucket_idx, &mdirents_hash_ptr, &local_idx);
    if (cache_entry_p == NULL) {
        DIRENT_SEVERE("put_mdirentry at line %d\n", __LINE__);
        goto out;
    }

    /*
     ** insert it in the linked list
     */
    last_modified_cache_entry = dirent_cache_insert_hash_entry(dirfd, root_entry_p,
            cache_entry_p,
            bucket_idx,
            &mdirents_hash_ptr,
            local_idx);
    if (last_modified_cache_entry == NULL) {
        DIRENT_SEVERE("put_mdirentry at line %d \n", __LINE__);
        goto out;
    }
    /*
     ** insert the hahs value into the allocated hash entry
     */
    hash_entry_p = (mdirents_hash_entry_t*) DIRENT_CACHE_GET_HASH_ENTRY_PTR(cache_entry_p, local_idx);
    if (hash_entry_p == NULL) {
        /*
         ** something wrong!! (either the index is out of range and the memory array has been released
         */
        DIRENT_SEVERE("put_mdirentry at line %d\n", __LINE__);
        goto out;
    }

    hash_entry_p->hash = hash2;
    /*
     ** insert the name entry
     */
    name_entry_p = &name_entry;
    /*
     ** copy the name and the fid
     */
    memcpy(name_entry_p->fid, fid, sizeof (fid_t));
    memcpy(name_entry_p->name, name, len);
    name_entry_p->type = type;
    name_entry_p->len = len;


    uint8_t *p8;

    p8 = (uint8_t*) dirent_create_entry_name(dirfd, cache_entry_p,
            name_entry_p,
            hash_entry_p);
    if (p8 == NULL) {

        DIRENT_SEVERE("put_mdirentry at line %d\n", __LINE__);
        goto out;
    }
    /*
     ** OK now re-write on disk all the impacted dirent cache entries:
     **  root : in case of collision file allocation, hash entry allocation
     **  collision entry where hash entry has been inserted
     **  previous collision or root cache entry: because of link list update
     */
    {
        int ret;
        /*
         ** Write the file on disk: it might the root and collision file, root only, collision file only
         */
        /*
         ** write the dirent file on which the entry has been inserted:
         */
        ret = write_mdirents_file(dirfd, cache_entry_p);
        if (ret < 0) {
            DIRENT_SEVERE("put_mdirentry at line %d\n", __LINE__);
            goto out;

        }
	/*
	** save the information related to the name array if requested by the caller
	*/
	if (fid_name_info_p != NULL)
	{
	  /*
	  ** save the information related to fid&name
	  */
	  if (cache_entry_p->header.level_index != 0)
	  {
	    fid_name_info_p->coll = 1;
	    fid_name_info_p->coll_idx = cache_entry_p->header.dirent_idx[1] ; 
	  }
	  fid_name_info_p->chunk_idx = hash_entry_p->chunk_idx ;
	  fid_name_info_p->nb_chunk = hash_entry_p->nb_chunk ;
	}
        /*
         ** check if another dirent cache entry  needs to be re-written on disk
         */
        if (last_modified_cache_entry != cache_entry_p) {
            /*
             ** write the cache entry for which there was a link list impact:
             */
            ret = write_mdirents_file(dirfd, last_modified_cache_entry);
            if (ret < 0) {
                DIRENT_SEVERE("put_mdirentry at line %d\n", __LINE__);
                goto out;
            }
        }
        /*
         ** Check if root needs to be re-written on disk :
         **   - adding a new collision file
         **   - update of a bucket entry
         **   - update of a hash entry (pnext)
         */
        if (DIRENT_IS_ROOT_UPDATE_REQ(root_entry_p)) {
            ret = write_mdirents_file(dirfd, root_entry_p);
            if (ret < 0) {
                DIRENT_SEVERE("put_mdirentry at line %d\n", __LINE__);
                goto out;
            }
        }
    }
    /*
     ** All is fine
     */
    status = 0;
out:
    /*
     ** do not release the entry if it is already in the cache
     */
    if (cached == 1) 
    {
      /*
      ** the content of the dirent file (root+ collision) is in the cache
      ** need to remove from cache if read only is asserted
      */
      if (DIRENT_ROOT_IS_READ_ONLY())
      {
        dirent_remove_root_entry_from_cache(fid_parent, root_idx);
	dirent_cache_release_entry(root_entry_p);
      }
      STOP_PROFILING(put_mdirentry);
      return status;
    }

    if (root_entry_p != NULL) {
        int ret = 0;
        ret = dirent_cache_release_entry(root_entry_p);
        if (ret < 0) {
            DIRENT_WARN(" get_mdirentry failed to release cache entry\n");
        }
    }
    STOP_PROFILING(put_mdirentry);
    return status;
}


/*
 **______________________________________________________________________________
 */
/**
 * API for get a mdirentry in one parent directory
 *
 * @param mdir: pointer to the mdirent structure for directory specific attributes
 * @param name: (key) pointer to the name to search
 * @param *fid: pointer to the unique identifier for this mdirentry
 * @param *type: pointer to the type for this mdirentry
 *
 * @retval  0 on success
 * @retval -1 on failure
 */
#if 0
#warning DEBUG for repair
int fdl_root_idx = -1;
int fdl_root_count = 0;
#endif

int get_mdirentry_internal(void *root_idx_bitmap_p,int dirfd, fid_t fid_parent, char * name, fid_t fid, 
                           uint32_t * type,int mask,int len,uint32_t hash1,uint32_t hash2);

int get_mdirentry(void *root_idx_bitmap_p,int dirfd, fid_t fid_parent_in, char * name, fid_t fid, uint32_t * type,int *mask_ret) 
{

  uint32_t hash1;
  uint32_t hash2;
  int range_idx;
  int len;
  int status = -1;
  int mask;
  int ret;
  int root_idx_bit;
  fid_t fid_parent;

  START_PROFILING(get_mdirentry);
  *mask_ret = 0;

  /*
  ** deassert de delete pending bit of the parent
  */
  memcpy(fid_parent,fid_parent_in,sizeof(fid_t));
  exp_metadata_inode_del_deassert(fid_parent);
  rozofs_inode_set_dir(fid_parent);
  /*
  ** file is unknown by default
  */
  errno = ENOENT;
   /*
   ** set the pointer to the root idx bitmap
   */
   dirent_set_root_idx_bitmap_ptr(root_idx_bitmap_p);

  /*
  ** compute the hash of the entry to search
  */
  hash1 = filename_uuid_hash_fnv(0, name, fid_parent, &hash2, &len);
  /*
  ** check if the entry is the different range
  */
  for (range_idx = 0; range_idx < DIRENT_MAX_RANGE;range_idx++)
  {
    mask = dirent_range_table[range_idx].mask;
    if (mask == 0) goto out;
    /*
    ** check if the file exists, otherwise skip it
    */
    root_idx_bit = dirent_check_root_idx_bit(hash1 & mask);
    if (root_idx_bit == 0) continue;  
    ret = get_mdirentry_internal(root_idx_bitmap_p,dirfd,fid_parent,name,fid,type,mask,len,hash1,hash2);
    if (ret == 0)
    {
      *mask_ret = mask;
      status = 0;
      errno = 0;
      goto out;
    }  
  }
out:
  STOP_PROFILING(get_mdirentry);
  return status;

}
/*
 **______________________________________________________________________________
 */

int get_mdirentry_internal(void *root_idx_bitmap_p,int dirfd, fid_t fid_parent, char * name, fid_t fid, 
                           uint32_t * type,int mask,int len,uint32_t hash1,uint32_t hash2) 
{
    int root_idx = 0;
    int status = -1;
    int cached = 0;
    int bucket_idx = 0;
    int local_idx;
    mdirents_header_new_t dirent_hdr;
    mdirents_cache_entry_t *root_entry_p;
    mdirents_cache_entry_t *cache_entry_p;
    mdirents_name_entry_t *name_entry_p = NULL;
    mdirents_hash_entry_t *hash_entry_p = NULL;
    
//    START_PROFILING(get_mdirentry);

   /*
   ** set the pointer to the root idx bitmap
   */
   dirent_set_root_idx_bitmap_ptr(root_idx_bitmap_p);
    /*
     ** dirfd is the file descriptor associated with the parent directory
     */
    /*
    ** Allow a priori to read and write on the root cache entry
    ** In case of some error while reading the dirent files from 
    ** disk, the rigths may be downgraded to read only.
    */
    DIRENT_ROOT_SET_READ_WRITE();
    /*
     ** attempt to get the root dirent file from cache
     */
    root_idx = hash1 & mask;
    bucket_idx = ((hash2 >> 16) ^ (hash2 & 0xffff)) & DIRENT_ROOT_BUCKET_IDX_MASK;

    root_entry_p = dirent_get_root_entry_from_cache(fid_parent, root_idx);
    if (root_entry_p == NULL) {
        /*
         ** dirent file is not in the cache need to read it from disk
         */
        dirent_hdr.type = MDIRENT_CACHE_FILE_TYPE;
        dirent_hdr.level_index = 0;
        dirent_hdr.dirent_idx[0] = root_idx;
        dirent_hdr.dirent_idx[1] = 0;
        root_entry_p = read_mdirents_file(dirfd, &dirent_hdr,fid_parent);
    } else {
        cached = 1;
    }
    /*
     ** ok now it depends if the entry exist on not
     */
    if (root_entry_p == NULL) {
        /*
	** cleat the bit in the root_idx bitmap
	*/
        dirent_clear_root_idx_bit(root_idx);    
        goto out;
    }
    /*
    ** set the corresponding bit in the root_idx bitmap. The goal
    ** is to fix an issue that can happen if the bitmap file was
    ** not correctly updated on disk
    */
    dirent_set_root_idx_bit(root_idx);    

    /*
     ** Check if the entry has to be inserted in the cache
     */
    if (cached == 0) {
        /**
         * fill up the key associated with the file
         */
        memcpy(root_entry_p->key.dir_fid, fid_parent, sizeof (fid_t));
        root_entry_p->key.dirent_root_idx = root_idx;
        /*
         ** attempt to insert it in the cache if not in cache
         */
        if (dirent_put_root_entry_to_cache(fid_parent, root_idx, root_entry_p) == 0) {
            /*
             ** indicates that entry is present in the cache
             */
            cached = 1;
        }
    }

    /*
     ** search if the entry exist and if so just replace the content of fid
     */
    cache_entry_p = dirent_cache_search_hash_entry(dirfd, root_entry_p,
            bucket_idx,
            hash2,
            &local_idx,
            (uint8_t *) name, (uint16_t) len,
            &name_entry_p,
            &hash_entry_p);
    if (cache_entry_p != NULL) {
        /*
         ** OK, we have found the entry either in one of the dirent_file associated with
         ** the dirent root file or in the dirent root file itself:
         ** We just need to update the fid and re-write the associated name entry array
         ** on disk
         */
        memcpy(fid, name_entry_p->fid, sizeof (fid_t));
        *type = name_entry_p->type;
#if 0
#warning release name memory array after each get
        while (1) {
            /*
             ** check in the memory that contains the name and file must be released
             */
            int first_chunk_of_array;
            uint8_t *mem_p;

            first_chunk_of_array = hash_entry_p->chunk_idx / MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY;
            mem_p = (uint8_t*) dirent_get_entry_name_ptr(dirfd, cache_entry_p,
                    first_chunk_of_array*MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY,
                    DIRENT_CHUNK_NO_ALLOC);
            if (mem_p == NULL) {
                /*
                 ** something wrong that must not occur
                 */
                DIRENT_WARN("get_mdirentry:  chunk error at line %d\n", __LINE__);
                break;
            }
            uint8_t *free_p = mem_p;
            mem_p = (uint8_t*) dirent_cache_del_ptr(&cache_entry_p->name_entry_lvl0_p[0],
                    &mdirent_cache_name_ptr_distrib,
                    first_chunk_of_array, (void *) mem_p);
            if (mem_p != NULL) {
                /*
                 ** that case must not happen because we just get it before calling deletion
                 */
                DIRENT_SEVERE("get_mdirentry error at line %d\n", __LINE__);
                break;

            }
            /*
             ** release the memory
             */
            DIRENT_FREE((void*) free_p);
            break;

        }


#endif

        status = 0;
        goto out;
    }

    errno = ENOENT;
    /*
     ** not found
     */
out:
    if (cached == 1) 
    {
      /*
      ** the content of the dirent file (root+ collision) is in the cache
      ** need to remove from cache if read only is asserted
      */
      if (DIRENT_ROOT_IS_READ_ONLY())
      {
        dirent_remove_root_entry_from_cache(fid_parent, root_idx);
	dirent_cache_release_entry(root_entry_p);
      }
//      STOP_PROFILING(get_mdirentry);
      return status;        
    }
    if (root_entry_p != NULL) {
        int ret = 0;
        ret = dirent_cache_release_entry(root_entry_p);
        if (ret < 0) {
            DIRENT_SEVERE(" get_mdirentry failed to release cache entry for %s",name);
        }
    }
//    STOP_PROFILING(get_mdirentry);
    return status;
}


#if 1
/*
static inline void dirent_dbg_check_cache_entry(fid_t fid_parent, int root_idx) {
    mdirents_cache_entry_t*p = dirent_get_root_entry_from_cache(fid_parent, root_idx);
    if (p == NULL) {
        printf("FDL_BUG root idx %d not in cache !!!\n", root_idx);

    }

}
*/
/*
 **______________________________________________________________________________
 */

/**
 * API for delete a mdirentry in one parent directory
 *
 * @param mdir: pointer to the mdirent structure for directory specific attributes
 * @param name: (key) pointer to the name of mdirentry to delete
 * @param *fid: pointer to the unique identifier fo this mdirentry
 * @param *type: pointer to the type for this mdirentry
 *
 * @retval  0 on success
 * @retval -1 on failure
 */

int del_mdirentry_internal(void *root_idx_bitmap_p,int dirfd, fid_t fid_parent, char * name, fid_t fid, uint32_t * type,int mask);


int del_mdirentry(void *root_idx_bitmap_p,int dirfd, fid_t fid_parent_in, char * name, fid_t fid, uint32_t * type,int mask) 
{

  int range_idx;
  int status = -1;
  int ret;
  fid_t fid_parent;
  START_PROFILING(del_mdirentry);
  /*
  ** deassert de delete pending bit of the parent
  */
  memcpy(fid_parent,fid_parent_in,sizeof(fid_t));
  exp_metadata_inode_del_deassert(fid_parent);
  rozofs_inode_set_dir(fid_parent);

  if (mask != 0)
  {
    status = del_mdirentry_internal(root_idx_bitmap_p,dirfd,fid_parent,name,fid,type,mask);
    goto out;  
  }

  /*
  ** The mask is unknown so scan the different ranges
  */
  for (range_idx = 0; range_idx < DIRENT_MAX_RANGE;range_idx++)
  {
    mask = dirent_range_table[range_idx].mask;
    if (mask == 0) goto out;
    ret = del_mdirentry_internal(root_idx_bitmap_p,dirfd,fid_parent,name,fid,type,mask);
    if (ret == 0)
    {
      status = 0;
      goto out;
    }  
  }
out:
  STOP_PROFILING(del_mdirentry);
  return status;

}

/*
 **______________________________________________________________________________
 */
 
int del_mdirentry_internal(void *root_idx_bitmap_p,int dirfd, fid_t fid_parent, char * name, fid_t fid, uint32_t * type,int mask) 
{
    int root_idx = 0;
    int cached = 0;
    int status = -1;
    int bucket_idx = 0;
    uint32_t hash1;
    uint32_t hash2;
    int hash_entry_match_idx;
    int len;
    uint32_t mode;
    int ret;
    mdirents_header_new_t dirent_hdr;
    mdirents_cache_entry_t *root_entry_p;
    mdirents_cache_entry_t *cache_entry_p;
    mdirents_cache_entry_t *returned_prev_entry_p;
    
//    START_PROFILING(del_mdirentry);
   /*
   ** set the pointer to the root idx bitmap
   */
   dirent_set_root_idx_bitmap_ptr(root_idx_bitmap_p);
    
    /*
    ** Allow a priori to read and write on the root cache entry
    ** In case of some error while reading the dirent files from 
    ** disk, the rigths may be downgraded to read only.
    */
    DIRENT_ROOT_SET_READ_WRITE();

    /*
     ** dirfd is the file descriptor associated with the parent directory
     */
    /*
     ** build a hash value based on the fid of the parent directory and the name to search
     */

    hash1 = filename_uuid_hash_fnv(0, name, fid_parent, &hash2, &len);
    /*
     ** attempt to get the root dirent file from cache
     */
//    root_idx = hash1 & DIRENT_ROOT_FILE_IDX_MASK;
    root_idx = hash1 & mask;
    bucket_idx = ((hash2 >> 16) ^ (hash2 & 0xffff)) & DIRENT_ROOT_BUCKET_IDX_MASK;

    root_entry_p = dirent_get_root_entry_from_cache(fid_parent, root_idx);
    if (root_entry_p == NULL) {
        /*
         ** dirent file is not in the cache need to read it from disk
         */
        dirent_hdr.type = MDIRENT_CACHE_FILE_TYPE;
        dirent_hdr.level_index = 0;
        dirent_hdr.dirent_idx[0] = root_idx;
        dirent_hdr.dirent_idx[1] = 0;
        root_entry_p = read_mdirents_file(dirfd, &dirent_hdr,fid_parent);
    } else {
        cached = 1;

    }
    /*
     ** ok now it depends if the entry exist on not
     */
    if (root_entry_p == NULL) {
        DIRENT_WARN("Root file does not exist( line %d\n)", __LINE__);
        goto out;
    }
    /*
     ** search and delete the entry related to the hash and name:
     ** that API mainipulates the bitmaps but does not release the memory
     ** ressource related to the deleted entry.
     */
    cache_entry_p = dirent_cache_delete_hash_entry(dirfd, root_entry_p,
            bucket_idx,
            hash2,
            &hash_entry_match_idx,
            &returned_prev_entry_p,
            (uint8_t *) name, (uint16_t) len,
            fid,
            &mode);

    if (cache_entry_p == NULL) {
        /*
         ** the entry does not exist
         */
#if 0
        DIRENT_WARN("Entry does not exist for root idx %d bucket_idx %d (cache %d) ( line %d): %s\n",
                root_idx, bucket_idx, cached, __LINE__, name);
#endif
        //XXX: integration tests
        errno = ENOENT;
        goto out;
    }
    /*
     ** check if the dirent cache entry from which the entry has been removed is now empty
     */
    ret = dirent_cache_entry_check_empty(cache_entry_p);
    switch (ret) {
        case 0:
            /*
             ** not empty
             */
            break;
        case 1:
            /*
             ** empty:if the entry is not root: we need to update the bitmap of the root
             */
            if (cache_entry_p != root_entry_p) {

                if (dirent_cache_del_collision_ptr(root_entry_p, cache_entry_p) != NULL) {
                    DIRENT_WARN(" ERROR  while deleting the collision ptr at line %d\n", __LINE__);
                    goto out;
                }
            }
            /*
             ** check the case of root_entry to figure out if level 0 cache must be updated
             */
            if ((cache_entry_p == root_entry_p) && (cached == 1)) {
                /*
                 ** remove from cache
                 */
                if (dirent_remove_root_entry_from_cache(fid_parent, root_idx) < 0) {
                    DIRENT_WARN("del_mdirentry: Root not found in level0 cache at line %d\n", __LINE__);
                }
		cached = 0;
            }
            /*
             ** remove the file
             */
        {
            char pathname_dentry_file[64];
            char *path_d;
            char *path_full;
            char path[PATH_MAX];
            /*
             ** build the filename of the dirent file to read
             */
	    path_full = path;
            path_d = dirent_build_filename(&cache_entry_p->header, pathname_dentry_file);
	    mdirent_resolve_path(fid_parent,(char*)pathname_dentry_file,path_full);

	    if (cache_entry_p->header.level_index == 0) 
	    {
	       dirent_clear_root_idx_bit(cache_entry_p->header.dirent_idx[0]);    
	    }
#ifndef DIRENT_SKIP_DISK
            int ret;
	    /*
	    ** before removing the file need to check the presence in the cache
	    ** for that purpose we request a cache flush before removing the file
	    */
	    if (dirent_writeback_cache_enable != 0)
	    {
	      dirent_wbcache_check_invalidate_on_unlink(dirent_current_eid,(char *)path_d,cache_entry_p->header.dirent_idx[0],cache_entry_p->key.dir_fid);
	    }
            ret = unlink( path_full);
            if (ret < 0) {
//                DIRENT_SEVERE("Cannot remove file %s: %s( line %d\n)", path_p, strerror(errno), __LINE__);
            }
#endif
        }
            /*
             ** OK, now release the associated memory
             */
            if (dirent_cache_release_entry(cache_entry_p) < 0) {
                DIRENT_WARN(" ERROR  dirent_cache_release_entry\at line %d\n", __LINE__);
            }
            if (cache_entry_p == root_entry_p) root_entry_p = NULL;
            cache_entry_p = NULL;
            break;

        default:
            DIRENT_SEVERE("Error on line %d : sector0 pointer is wrong\n", __LINE__);
            goto out;
            break;

    }
    /*
     ** Write the file on disk: it might the root and collision file, root only, collision file only
     */
    /*
     ** write the dirent file on which the entry has been inserted:
     */
    if ((cache_entry_p != NULL) && (DIRENT_IS_ROOT_UPDATE_REQ(cache_entry_p))) {
        ret = write_mdirents_file(dirfd, cache_entry_p);
        if (ret < 0) {
            DIRENT_WARN("Error on writing file at line %d\n", __LINE__);
        }
    }
    /*
     ** check if another dirent cache entry  needs to be re-written on disk
     */
    if ((returned_prev_entry_p != NULL) && (DIRENT_IS_ROOT_UPDATE_REQ(returned_prev_entry_p))) {
        /*
         ** check if it is the case of the root file because it might be necessary to delete
         ** it rather than updated it on disk
         */
        if (returned_prev_entry_p == root_entry_p) {
            ret = dirent_cache_entry_check_empty(root_entry_p);
            if (ret == 1) {
                /*
                 ** root file is empty : delete the file
                 */
                char pathname_dentry_file[64];
                char *path_d;
        	char path[PATH_MAX];
                /*
                 ** if (cached == 1)--> remove the entry from the level 0 cache
                 */
                if (cached == 1) {
                    /*
                     ** remove from cache
                     */
                    if (dirent_remove_root_entry_from_cache(fid_parent, root_idx) < 0) {
                        DIRENT_WARN("del_mdirentry: Root not found in level0 cache at line %d\n", __LINE__);
                    }
		    cached = 0;
                }

                /*
                 ** build the filename of the dirent file to read
                 */
                path_d = dirent_build_filename(&root_entry_p->header, pathname_dentry_file);
	        mdirent_resolve_path(fid_parent,(char*)pathname_dentry_file,path);
		if (root_entry_p->header.level_index == 0) 
		{
		   dirent_clear_root_idx_bit(root_entry_p->header.dirent_idx[0]);    
		}
		/*
		** invalidate the writeback cache for that entry
		*/
		if (dirent_writeback_cache_enable != 0)
		{
		  dirent_wbcache_check_invalidate_on_unlink(dirent_current_eid,(char *)path_d,root_entry_p->header.dirent_idx[0],
		                                            root_entry_p->key.dir_fid);
		}
                /*
                 ** now release the associated memory
                 */
                if (dirent_cache_release_entry(root_entry_p) < 0) {
                    DIRENT_WARN(" ERROR  dirent_cache_release_entry\at line %d\n", __LINE__);
                }
                /*
                 ** clear the pointer to the root entry
                 */
                root_entry_p = NULL;
#ifndef DIRENT_SKIP_DISK
                int ret;

                ret = unlink(path);
                if ((ret < 0) && (errno != ENOENT)){
		    /*
		    ** it might be possible that the file does not exist because of the writeback cache behavior
		    */
                    DIRENT_SEVERE("Cannot remove file %s: %s( line %d\n)", path, strerror(errno), __LINE__);		    
                    goto out;
                }
#endif

                status = 0;
                goto out;
            }
        }
        /*
         ** write the returned_prev_entry_p file:
         */
        ret = write_mdirents_file(dirfd, returned_prev_entry_p);
        if (ret < 0) {
            DIRENT_WARN("Error on writing file at line %d\n", __LINE__);
        }
    }
    /*
     ** Check if root needs to be re-written on disk :
     **   - adding a new collision file
     **   - update of a bucket entry
     **   - update of a hash entry (pnext)
     */
    if ((root_entry_p != NULL) && (DIRENT_IS_ROOT_UPDATE_REQ(root_entry_p))) {
        ret = write_mdirents_file(dirfd, root_entry_p);
        if (ret < 0) {
            DIRENT_WARN("Error on writing file at line %d\n", __LINE__);
        }
    }
    /**
     ** all is fine
     */
    status = 0;

out:
    if (cached == 1) 
    {
      /*
      ** the content of the dirent file (root+ collision) is in the cache
      ** need to remove from cache if read only is asserted
      */
      if (DIRENT_ROOT_IS_READ_ONLY())
      {
        dirent_remove_root_entry_from_cache(fid_parent, root_idx);
	dirent_cache_release_entry(root_entry_p);
      }
//      STOP_PROFILING(del_mdirentry);
      return status;        
    }
    if (root_entry_p != NULL) {
        int ret = 0;
        ret = dirent_cache_release_entry(root_entry_p);
        if (ret < 0) {
            DIRENT_SEVERE(" get_mdirentry failed to release cache entry\n");
        }
    }
//    STOP_PROFILING(del_mdirentry);
    return status;
}

#endif


#if 1
 
struct rozofs_fuse_dirent {
	uint64_t	ino;
	uint64_t	off;
	uint32_t	namelen;
	uint32_t	type;
	char name[];
};

#define FUSE_NAME_OFFSET offsetof(struct rozofs_fuse_dirent, name)
#define FUSE_DIRENT_ALIGN(x) (((x) + sizeof(uint64_t) - 1) & ~(sizeof(uint64_t) - 1))
#define FUSE_DIRENT_SIZE(d) \
	FUSE_DIRENT_ALIGN(FUSE_NAME_OFFSET + (d)->namelen)


static inline size_t rozofs_fuse_dirent_size(size_t namelen)
{
	return FUSE_DIRENT_ALIGN(FUSE_NAME_OFFSET + namelen);
}
/**
  add an entry in the readdir buffer
  
  @param buf: pointer to the readdir buffer
  @param ino: inode value
  @param name: pointer to the name to insert
  @param off: cookie associated to the entry
  
  @reval: pointer to the next free entry
*/
char *rozofs_fuse_add_dirent(char *buf, uint64_t ino,const char *name,uint32_t namelen,off_t off)
{
	unsigned entlen = FUSE_NAME_OFFSET + namelen;
	unsigned entsize = rozofs_fuse_dirent_size(namelen);
	unsigned padlen = entsize - entlen;
	struct rozofs_fuse_dirent *dirent = (struct rozofs_fuse_dirent *) buf;

	dirent->ino = ino;
	dirent->off = off;
	dirent->namelen = namelen;
	dirent->type = 0;
	strncpy(dirent->name, name, namelen);
	if (padlen)
		memset(buf + entlen, 0, padlen);

	return buf + entsize;
}
/*
 **______________________________________________________________________________
 */

/**
 * API for get a mdirentry in one parent directory
 *
 * @param mdir: pointer to the mdirent structure for directory specific attributes
 * @param name: (key) pointer to the name to search
 * @param *fid: pointer to the unique identifier for this mdirentry
 * @param *type: pointer to the type for this mdirentry
 *
 * @retval  0 on success
 * @retval -1 on failure
 */
typedef union _dirent_list_cookie_t {
    uint64_t val64;

    struct {
        uint64_t index_level : 1; /**< 0: root file, 1 : collision file          */
        uint64_t root_idx : 12; /**< currenr root file index                   */
        uint64_t coll_idx : 11; /**< index of the next collision file to test  */
        uint64_t hash_entry_idx : 10; /**< index of the next bitmap entry to test    */
        uint64_t valid_entry : 1; /**< always 1                         */
        uint64_t filler : 29; /**< for future usage                         */

    } s;
} dirent_list_cookie_t;

int list_mdirentries(void *root_idx_bitmap_p,int dir_fd, fid_t fid_parent_in, child_t ** children, uint64_t *cookie, uint8_t * eof) {
    int root_idx = 0;
    int cached = 0;
    child_t ** iterator;
    dirent_list_cookie_t dirent_cookie;
    mdirents_header_new_t dirent_hdr;
    mdirents_cache_entry_t *root_entry_p = NULL;
    mdirents_cache_entry_t *cache_entry_p;
    mdirents_hash_entry_t *hash_entry_p = NULL;
    mdirent_sector0_not_aligned_t *sect0_p;
    int hash_entry_idx = 0;
    int index_level = 0;
    int read_file = 0;
    int coll_idx;
    int loop_cnt = 0;
    int next_coll_idx = 0;
    int bit_idx;
    int chunk_u8_idx;
    uint8_t *coll_bitmap_p;
    int next_hash_entry_idx;
    int root_idx_bit;
    int deleted_dir;
    int deleted_obj;
    fid_t fid_parent;

    START_PROFILING(list_mdirentries);
    /*
    ** check if the delete pending flag is asserted on the parent directory
    */
    memcpy(fid_parent,fid_parent_in,sizeof(fid_t));
    deleted_dir = exp_metadata_inode_is_del_pending(fid_parent);
    exp_metadata_inode_del_deassert(fid_parent);
    rozofs_inode_set_dir(fid_parent);
   /*
   ** set the pointer to the root idx bitmap
   */
   dirent_set_root_idx_bitmap_ptr(root_idx_bitmap_p);

    dirent_readdir_stats_call_count++;
    /*
     ** load up the cookie to figure out where to start the read
     */
    dirent_cookie.val64 = *cookie;
    dirent_cookie.s.filler = 0;
    iterator = children;

    /*
     ** set the different parameter
     */
    root_idx = dirent_cookie.s.root_idx;
    hash_entry_idx = dirent_cookie.s.hash_entry_idx;
    index_level = dirent_cookie.s.index_level;
    coll_idx = dirent_cookie.s.coll_idx;
    *eof = 0;
    /*
     **___________________________________________________________
     **  loop through the potential root file index
     **  We exit fro the while loop once one has been found or if
     **  the last dirent root file index has been reached
     **___________________________________________________________
     */
    while (read_file < MAX_DIR_ENTRIES) {
        while (root_idx < DIRENT_ROOT_FILE_IDX_MAX) {

	   /*
	   ** Allow a priori to read and write on the root cache entry
	   ** In case of some error while reading the dirent files from 
	   ** disk, the rigths may be downgraded to read only.
	   */
	   DIRENT_ROOT_SET_READ_WRITE();
	   
	   /*
	   ** check if the bit is asserted for the root_idx
	   */
	   root_idx_bit = dirent_check_root_idx_bit(root_idx);
	   if (root_idx_bit == 1)
	   {
              /*
               ** attempt to get the dirent root file from the cache
               */
              root_entry_p = dirent_get_root_entry_from_cache(fid_parent, root_idx);
              if (root_entry_p == NULL) {
                  /*
                   ** dirent file is not in the cache need to read it from disk
                   */
                  dirent_hdr.type = MDIRENT_CACHE_FILE_TYPE;
                  dirent_hdr.level_index = 0;
                  dirent_hdr.dirent_idx[0] = root_idx;
                  dirent_hdr.dirent_idx[1] = 0;
                  root_entry_p = read_mdirents_file(dir_fd, &dirent_hdr,fid_parent);
              } else {
                  /*
                   ** found one, so process its content
                   */
                  cached = 1;
                  break;
              }
              /*
               ** ok now it depends if the entry exist on not
               */
              if (root_entry_p != NULL) {
                  break;
              }
	    }
            /*
             ** That root file does not exist-> need to check the next root_idx
             */
            root_idx++;
        }
        /*
         **_____________________________________________
         ** Either there is an entry or there is nothing
         **_____________________________________________
         */
        if (root_entry_p == NULL) {
            /*
             ** we are done
             */
            *eof = 1;
            break;
        }
        /*
         ** There is a valid entry, but before doing the job, check if the entry has
         **  been extract from the cache or read from disk. If entry has been read
         ** from disk, we attempt to insert it in the cache.
         */
        if (cached == 0) {
            /**
             * fill up the key associated with the file
             */
            memcpy(root_entry_p->key.dir_fid, fid_parent, sizeof (fid_t));
            root_entry_p->key.dirent_root_idx = root_idx;
            /*
             ** attempt to insert it in the cache if not in cache
             */
            if (dirent_put_root_entry_to_cache(fid_parent, root_idx, root_entry_p) == 0) {
                /*
                 ** indicates that entry is present in the cache
                 */
                cached = 1;
            }
        }
        /*
         **___________________________________________________________________________
         ** OK, now start the real job where we start scanning the collision file and
         ** the allocated hash entries. Here there is no readon to follow the link list
         ** of the bucket, checking the bitmap of the hash entries is enough.
         ** There is the same approach for the case of the collision file
         **___________________________________________________________________________
         */
        /*
         ** Need to get the pointer to the hash entry bitmap to figure out which
         ** entries are allocated
         */
        sect0_p = DIRENT_VIRT_TO_PHY_OFF(root_entry_p, sect0_p);
        if (sect0_p == (mdirent_sector0_not_aligned_t*) NULL) {
            DIRENT_SEVERE("list_mdirentries sector 0 ptr does not exist( line %d\n)", __LINE__);
	    if (cached == 1) 
	    {
	      /*
	      ** the content of the dirent file (root+ collision) is in the cache
	      ** need to remove from cache if read only is asserted
	      */
	      if (DIRENT_ROOT_IS_READ_ONLY())
	      {
        	dirent_remove_root_entry_from_cache(fid_parent, root_idx);
		dirent_cache_release_entry(root_entry_p);
	      }
	      cached = 0;
	    }
            root_idx++;
            root_entry_p = NULL;
            continue;
        }
        coll_bitmap_p = (uint8_t*) & sect0_p->coll_bitmap;
        cache_entry_p = root_entry_p;

get_next_collidx:
        if (index_level != 0) {
            /*
             ** case of the collision file, so need to go through the bitmap of the
             ** dirent root file
             */
            cache_entry_p = NULL;
            while (coll_idx < MDIRENTS_MAX_COLLS_IDX) {
                chunk_u8_idx = coll_idx / 8;
                bit_idx = coll_idx % 8;
                /*
                 ** there is no collision dirent entry or the collision dirent entry exist and is not full
                 */
                if ((coll_bitmap_p[chunk_u8_idx] & (1 << bit_idx)) != 0) {
                    /*
                     ** That entry is free, need to find out the next entry that is busy (0: busy, 1:free)
                     */
                    if (coll_idx % 8 == 0) {
                        next_coll_idx = check_bytes_val(coll_bitmap_p, coll_idx, MDIRENTS_MAX_COLLS_IDX, &loop_cnt, 1);
                        if (next_coll_idx < 0) break;
                        /*
                         ** next  chunk
                         */
                        if (next_coll_idx == coll_idx) coll_idx++;
                        else coll_idx = next_coll_idx;
                        continue;
                    }
                    /*
                     ** next chunk
                     */
                    hash_entry_idx = 0;
                    coll_idx++;
                    continue;
                }
                /*
                 ** one collision idx has been found
                 ** need to get the entry associated with the collision index
                 */
                cache_entry_p = dirent_cache_get_collision_ptr(root_entry_p, coll_idx);
                if (cache_entry_p == NULL) {
                    /*
                     ** something is rotten in the cache since the pointer to the collision dirent cache
                     ** does not exist
                     */
                    DIRENT_SEVERE("list_mdirentries not collisiob file %d\n", coll_idx);
                    /*
                     ** OK, do not break the analysis, skip that collision entry and try the next if any
                     */
                    hash_entry_idx = 0;
                    coll_idx++;
                    continue;
                }
                break;
            }
        }
        /*
         ** OK either we have one dirent entry or nothing: for the nothing case we go to
         ** the next root_idx
         */
        if (cache_entry_p == NULL) {
            /*
             ** check the next root index
             */
            coll_idx = 0;
            hash_entry_idx = 0;
            index_level = 0;
	    if (cached == 1) 
	    {
	      /*
	      ** the content of the dirent file (root+ collision) is in the cache
	      ** need to remove from cache if read only is asserted
	      */
	      if (DIRENT_ROOT_IS_READ_ONLY())
	      {
        	dirent_remove_root_entry_from_cache(fid_parent, root_idx);
		dirent_cache_release_entry(root_entry_p);
	      }
	      cached = 0;
	    }
            root_entry_p = NULL;
            root_idx++;
            continue;
        }
        sect0_p = DIRENT_VIRT_TO_PHY_OFF(cache_entry_p, sect0_p);
        if (sect0_p == (mdirent_sector0_not_aligned_t*) NULL) {
            DIRENT_SEVERE("list_mdirentries sector 0 ptr does not exist( line %d\n)", __LINE__);
            /*
             ** do break the walktrhough, try either the next root entry and collision entry
             */
            cache_entry_p = NULL;
            hash_entry_idx = 0;
            if (index_level == 0) {
                coll_idx = 0;
                index_level = 1;
                goto get_next_collidx;
            }
            coll_idx++;
            goto get_next_collidx;
        }
        /*
         ** Get the pointer to the hash entry bitmap
         */
        while ((hash_entry_idx < MDIRENTS_ENTRIES_COUNT) && (read_file < MAX_DIR_ENTRIES)) {
            next_hash_entry_idx = DIRENT_CACHE_GETNEXT_ALLOCATED_HASH_ENTRY_IDX(&sect0_p->hash_bitmap, hash_entry_idx);
            if (next_hash_entry_idx < 0) {
                /*
                 ** all the entry of that dirent cache entry have been scanned, need to check the next collision file if
                 ** any
                 */
                cache_entry_p = NULL;
                hash_entry_idx = 0;
                /*
                 ** check the next
                 */
                if (index_level == 0) {
                    coll_idx = 0;
                    index_level = 1;
                    goto get_next_collidx;
                }
                coll_idx++;
                goto get_next_collidx;
            }
            hash_entry_idx = next_hash_entry_idx;
            /*
             ** need to get the hash entry context and then the pointer to the name entry. The hash entry context is
             ** needed since it contains the reference of the starting chunk of the name entry
             */
            hash_entry_p = (mdirents_hash_entry_t*) DIRENT_CACHE_GET_HASH_ENTRY_PTR(cache_entry_p, hash_entry_idx);
            if (hash_entry_p == NULL) {
                /*
                 ** something wrong!! (either the index is out of range and the memory array has been released
                 */
                DIRENT_SEVERE("list_mdirentries pointer does not exist at %d\n", __LINE__);
                /*
                 ** ok, let check the next hash_entry
                 */
                hash_entry_idx++;
                continue;
            }

            /*
            ** something wrong that must not occur !!!
            ** A dentry is allocated but no chunk has been allocated
            */
 	        if (hash_entry_p->nb_chunk==0) {
                hash_entry_idx++;
                continue;
	        }
		     
            /*
             ** OK, now, get the pointer to the name array
             */
            mdirents_name_entry_t *name_entry_p;
            name_entry_p = (mdirents_name_entry_t*) dirent_get_entry_name_ptr(dir_fd, cache_entry_p, hash_entry_p->chunk_idx, DIRENT_CHUNK_NO_ALLOC);
            if (name_entry_p == (mdirents_name_entry_t*) NULL) {
                /*
                 ** something wrong that must not occur
                 */
                severe("list_mdirentries: pointer does not exist");
                /*
                 ** ok, let check the next hash_entry
                 */
                hash_entry_idx++;
                continue;
            }

            /**
             *  that's songs good, copy the content of the name in the result buffer
             */
            /*
	    ** check the length of the filename, if the length is 0, it indicates that we read a truncate dirent file
	    ** so we skip that entry and goes to the next one
	    */
	    if (name_entry_p->len == 0)
	    {
	       char fidstr[37];
	       rozofs_uuid_unparse(fid_parent, fidstr);

                severe("empty name entry in directory %s at hash_idx %d in file d_%d collision idx %d chunk_idx %d",
		        fidstr,hash_entry_idx,root_idx,coll_idx,hash_entry_p->chunk_idx);
                hash_entry_idx++;
                continue;	    
	    
	    }
	    /*
	    ** check if the entry has to be reported to the caller according the state of the delete pending
	    ** flag of the object and of the parent
	    */
	    deleted_obj = exp_metadata_inode_is_del_pending(name_entry_p->fid);
#if 1
	    if (deleted_dir != deleted_obj) 
	    {
                hash_entry_idx++;
                continue;	    
	    }
#endif
            *iterator = xmalloc(sizeof (child_t));
            memset(*iterator, 0, sizeof (child_t));
            memcpy((*iterator)->fid, name_entry_p->fid, sizeof (fid_t));
            (*iterator)->name = strndup(name_entry_p->name, name_entry_p->len);
            // Go to next entry
            iterator = &(*iterator)->next;
            /*
             ** increment the number of file and try to get the next one
             */
            hash_entry_idx++;
            read_file++;
            dirent_readdir_stats_file_count++;

        }
        /*
         ** Check if the amount of file has been read
         */
        if (read_file >= MAX_DIR_ENTRIES) {
            /*
             ** We are done
             */
            break;
        }
        /*
         ** No the end and there still some room in the output buffer
         ** Here we need to check the next collision entry if any
         */
        cache_entry_p = NULL;
        hash_entry_idx = 0;
        /*
         ** check the next
         */
        if (index_level == 0) {
            coll_idx = 0;
            index_level = 1;
            goto get_next_collidx;
        }
        coll_idx++;
        goto get_next_collidx;
    }
    /*
     ** done
     */
    /*
     ** set the different parameter
     */
    dirent_cookie.s.root_idx = root_idx;
    dirent_cookie.s.hash_entry_idx = hash_entry_idx;
    dirent_cookie.s.index_level = index_level;
    dirent_cookie.s.coll_idx = coll_idx;
    *cookie = dirent_cookie.val64;

    /*
     ** check the cache status to figure out if root entry need to be released
     */
    if (cached == 1) 
    {
      /*
      ** the content of the dirent file (root+ collision) is in the cache
      ** need to remove from cache if read only is asserted
      */
      if (DIRENT_ROOT_IS_READ_ONLY())
      {
        dirent_remove_root_entry_from_cache(fid_parent, root_idx);
	dirent_cache_release_entry(root_entry_p);
      }
      STOP_PROFILING(list_mdirentries);
      return 0;
    }
    if (root_entry_p != NULL) {
        int ret = 0;
        ret = dirent_cache_release_entry(root_entry_p);
        if (ret < 0) {
            DIRENT_SEVERE(" get_mdirentry failed to release cache entry\n");
        }
    }
    STOP_PROFILING(list_mdirentries);
    return 0;
}

#endif

/*
 **______________________________________________________________________________
 */

/**
 * API for get a mdirentry in one parent directory (version2)
 *
 * @param mdir: pointer to the mdirent structure for directory specific attributes
 * @param name: (key) pointer to the name to search
 * @param *fid: pointer to the unique identifier for this mdirentry
 * @param *type: pointer to the type for this mdirentry
 *
 * @retval  0 on success
 * @retval -1 on failure
 */

#define ROZOFS_READDIR_MAX_BYTES (64*1024-4096)
#define MAX_DIR_ENTRIES_VERS2 (128+64)

int list_mdirentries2(void *root_idx_bitmap_p,int dir_fd, fid_t fid_parent_in, char *buf_readdir_in, uint64_t *cookie, uint8_t * eof,ext_mattr_t *parent) {
    int root_idx = 0;
    int cached = 0;
    dirent_list_cookie_t dirent_cookie;
    mdirents_header_new_t dirent_hdr;
    mdirents_cache_entry_t *root_entry_p = NULL;
    mdirents_cache_entry_t *cache_entry_p;
    mdirents_hash_entry_t *hash_entry_p = NULL;
    mdirent_sector0_not_aligned_t *sect0_p;
    int hash_entry_idx = 0;
    int index_level = 0;
    int read_file = 0;
    int coll_idx;
    int loop_cnt = 0;
    int next_coll_idx = 0;
    int bit_idx;
    int chunk_u8_idx;
    char *buf_readdir_p = buf_readdir_in;
    uint8_t *coll_bitmap_p;
    int next_hash_entry_idx;
    int root_idx_bit;
    int deleted_dir;
    int show_trash_dir = 0;
    int deleted_obj;
    fid_t fid_parent;
    rozofs_inode_t *inode_p ;
    fid_t      null_fid = {0};
    int mangle_name = 0;
        
    START_PROFILING(list_mdirentries);
    /*
    ** check if the delete pending flag is asserted on the parent directory
    */
    memcpy(fid_parent,fid_parent_in,sizeof(fid_t));
    deleted_dir = 0;
    if (exp_metadata_inode_is_del_pending(fid_parent) ||  exp_metadata_inode_is_del_pending(parent->s.attrs.fid))
    {
       deleted_dir = 1;
    }
    /*
    ** check if there is a trash associated with the directory
    */
    if (deleted_dir == 0)
    {
      if (rozofs_has_root_trash(&parent->s.attrs.sids[0])== 0) show_trash_dir=0;
      else 
      {
	show_trash_dir =1;
      }
    } 
    /*
    ** clear the delete pending bit & force the key to ROZOFS_DIR
    */
    exp_metadata_inode_del_deassert(fid_parent);
    rozofs_inode_set_dir(fid_parent);
    /*
    ** set the pointer to the root idx bitmap
    */
    dirent_set_root_idx_bitmap_ptr(root_idx_bitmap_p);

    dirent_readdir_stats_call_count++;
    /*
     ** load up the cookie to figure out where to start the read
     */
    dirent_cookie.val64 = *cookie;
    /*
    ** check if we start from the beginning because in that case we must provide
    ** the fid for "." and ".."
    */
    if ((dirent_cookie.s.index_level == 0) && 
        (dirent_cookie.s.root_idx == 0) &&
        (dirent_cookie.s.coll_idx == 0) &&
        (dirent_cookie.s.hash_entry_idx == 0))
    {
      if (dirent_cookie.s.filler == 0) 
      {
	      inode_p = (rozofs_inode_t*) parent->s.attrs.fid;
	      dirent_cookie.s.filler = 1;
	      buf_readdir_p = rozofs_fuse_add_dirent(buf_readdir_p,inode_p->fid[1],".",1,dirent_cookie.val64);	    
      }
      if (dirent_cookie.s.filler == 1) 
      {    

	 if (memcmp(parent->s.pfid,null_fid,sizeof(fid_t))==0) {
           inode_p = (rozofs_inode_t*)parent->s.attrs.fid;	      
	 }
	 else {
	  inode_p = (rozofs_inode_t*)parent->s.pfid;
	 }
	 dirent_cookie.s.filler = 2;
	 buf_readdir_p = rozofs_fuse_add_dirent(buf_readdir_p,inode_p->fid[1],"..",2,dirent_cookie.val64);	    
      }
#if 1
      if ((dirent_cookie.s.filler == 2) && (show_trash_dir==1))
      {    
	 dirent_cookie.s.filler = 3;
	 inode_p = (rozofs_inode_t*)fid_parent_in;
         exp_metadata_inode_del_assert(fid_parent_in);
	 rozofs_inode_set_trash(fid_parent_in);
	 buf_readdir_p = rozofs_fuse_add_dirent(buf_readdir_p,inode_p->fid[1],ROZOFS_DIR_TRASH,sizeof(ROZOFS_DIR_TRASH),dirent_cookie.val64);	
	 rozofs_inode_set_dir(fid_parent_in);    
         exp_metadata_inode_del_deassert(fid_parent_in);

      }
#endif
    }
    dirent_cookie.s.filler = 0;
    /*
     ** set the different parameter
     */
    root_idx = dirent_cookie.s.root_idx;
    hash_entry_idx = dirent_cookie.s.hash_entry_idx;
    index_level = dirent_cookie.s.index_level;
    coll_idx = dirent_cookie.s.coll_idx;
    *eof = 0;
    /*
     **___________________________________________________________
     **  loop through the potential root file index
     **  We exit fro the while loop once one has been found or if
     **  the last dirent root file index has been reached
     **___________________________________________________________
     */
    while ((read_file < MAX_DIR_ENTRIES_VERS2) && ((int)(buf_readdir_p -buf_readdir_in) < ROZOFS_READDIR_MAX_BYTES)) {
        while (root_idx < DIRENT_ROOT_FILE_IDX_MAX) {

	   /*
	   ** Allow a priori to read and write on the root cache entry
	   ** In case of some error while reading the dirent files from 
	   ** disk, the rigths may be downgraded to read only.
	   */
	   DIRENT_ROOT_SET_READ_WRITE();
	   
	   /*
	   ** check if the bit is asserted for the root_idx
	   */
	   root_idx_bit = dirent_check_root_idx_bit(root_idx);
	   if (root_idx_bit == 1)
	   {
              /*
               ** attempt to get the dirent root file from the cache
               */
              root_entry_p = dirent_get_root_entry_from_cache(fid_parent, root_idx);
              if (root_entry_p == NULL) {
                  /*
                   ** dirent file is not in the cache need to read it from disk
                   */
                  dirent_hdr.type = MDIRENT_CACHE_FILE_TYPE;
                  dirent_hdr.level_index = 0;
                  dirent_hdr.dirent_idx[0] = root_idx;
                  dirent_hdr.dirent_idx[1] = 0;
                  root_entry_p = read_mdirents_file(dir_fd, &dirent_hdr,fid_parent);
              } else {
                  /*
                   ** found one, so process its content
                   */
                  cached = 1;
                  break;
              }
              /*
               ** ok now it depends if the entry exist on not
               */
              if (root_entry_p != NULL) {
                  break;
              }
	    }
            /*
             ** That root file does not exist-> need to check the next root_idx
             */
            root_idx++;
        }
        /*
         **_____________________________________________
         ** Either there is an entry or there is nothing
         **_____________________________________________
         */
        if (root_entry_p == NULL) {
            /*
             ** we are done
             */
            *eof = 1;
            break;
        }
        /*
         ** There is a valid entry, but before doing the job, check if the entry has
         **  been extract from the cache or read from disk. If entry has been read
         ** from disk, we attempt to insert it in the cache.
         */
        if (cached == 0) {
            /**
             * fill up the key associated with the file
             */
            memcpy(root_entry_p->key.dir_fid, fid_parent, sizeof (fid_t));
            root_entry_p->key.dirent_root_idx = root_idx;
            /*
             ** attempt to insert it in the cache if not in cache
             */
            if (dirent_put_root_entry_to_cache(fid_parent, root_idx, root_entry_p) == 0) {
                /*
                 ** indicates that entry is present in the cache
                 */
                cached = 1;
            }
        }
        /*
         **___________________________________________________________________________
         ** OK, now start the real job where we start scanning the collision file and
         ** the allocated hash entries. Here there is no readon to follow the link list
         ** of the bucket, checking the bitmap of the hash entries is enough.
         ** There is the same approach for the case of the collision file
         **___________________________________________________________________________
         */
        /*
         ** Need to get the pointer to the hash entry bitmap to figure out which
         ** entries are allocated
         */
        sect0_p = DIRENT_VIRT_TO_PHY_OFF(root_entry_p, sect0_p);
        if (sect0_p == (mdirent_sector0_not_aligned_t*) NULL) {
            DIRENT_SEVERE("list_mdirentries sector 0 ptr does not exist( line %d\n)", __LINE__);
	    if (cached == 1) 
	    {
	      /*
	      ** the content of the dirent file (root+ collision) is in the cache
	      ** need to remove from cache if read only is asserted
	      */
	      if (DIRENT_ROOT_IS_READ_ONLY())
	      {
        	dirent_remove_root_entry_from_cache(fid_parent, root_idx);
		dirent_cache_release_entry(root_entry_p);
	      }
	      cached = 0;
	    }
            root_idx++;
            root_entry_p = NULL;
            continue;
        }
        coll_bitmap_p = (uint8_t*) & sect0_p->coll_bitmap;
        cache_entry_p = root_entry_p;

get_next_collidx:
        if (index_level != 0) {
            /*
             ** case of the collision file, so need to go through the bitmap of the
             ** dirent root file
             */
            cache_entry_p = NULL;
            while (coll_idx < MDIRENTS_MAX_COLLS_IDX) {
                chunk_u8_idx = coll_idx / 8;
                bit_idx = coll_idx % 8;
                /*
                 ** there is no collision dirent entry or the collision dirent entry exist and is not full
                 */
                if ((coll_bitmap_p[chunk_u8_idx] & (1 << bit_idx)) != 0) {
                    /*
                     ** That entry is free, need to find out the next entry that is busy (0: busy, 1:free)
                     */
                    if (coll_idx % 8 == 0) {
                        next_coll_idx = check_bytes_val(coll_bitmap_p, coll_idx, MDIRENTS_MAX_COLLS_IDX, &loop_cnt, 1);
                        if (next_coll_idx < 0) break;
                        /*
                         ** next  chunk
                         */
                        if (next_coll_idx == coll_idx) coll_idx++;
                        else coll_idx = next_coll_idx;
                        continue;
                    }
                    /*
                     ** next chunk
                     */
                    hash_entry_idx = 0;
                    coll_idx++;
                    continue;
                }
                /*
                 ** one collision idx has been found
                 ** need to get the entry associated with the collision index
                 */
                cache_entry_p = dirent_cache_get_collision_ptr(root_entry_p, coll_idx);
                if (cache_entry_p == NULL) {
                    /*
                     ** something is rotten in the cache since the pointer to the collision dirent cache
                     ** does not exist
                     */
                    DIRENT_SEVERE("list_mdirentries not collisiob file %d\n", coll_idx);
                    /*
                     ** OK, do not break the analysis, skip that collision entry and try the next if any
                     */
                    hash_entry_idx = 0;
                    coll_idx++;
                    continue;
                }
                break;
            }
        }
        /*
         ** OK either we have one dirent entry or nothing: for the nothing case we go to
         ** the next root_idx
         */
        if (cache_entry_p == NULL) {
            /*
             ** check the next root index
             */
            coll_idx = 0;
            hash_entry_idx = 0;
            index_level = 0;
	    if (cached == 1) 
	    {
	      /*
	      ** the content of the dirent file (root+ collision) is in the cache
	      ** need to remove from cache if read only is asserted
	      */
	      if (DIRENT_ROOT_IS_READ_ONLY())
	      {
        	dirent_remove_root_entry_from_cache(fid_parent, root_idx);
		dirent_cache_release_entry(root_entry_p);
	      }
	      cached = 0;
	    }
            root_entry_p = NULL;
            root_idx++;
            continue;
        }
        sect0_p = DIRENT_VIRT_TO_PHY_OFF(cache_entry_p, sect0_p);
        if (sect0_p == (mdirent_sector0_not_aligned_t*) NULL) {
            DIRENT_SEVERE("list_mdirentries sector 0 ptr does not exist( line %d\n)", __LINE__);
            /*
             ** do break the walktrhough, try either the next root entry and collision entry
             */
            cache_entry_p = NULL;
            hash_entry_idx = 0;
            if (index_level == 0) {
                coll_idx = 0;
                index_level = 1;
                goto get_next_collidx;
            }
            coll_idx++;
            goto get_next_collidx;
        }
        /*
         ** Get the pointer to the hash entry bitmap
         */
        while (
	       (hash_entry_idx < MDIRENTS_ENTRIES_COUNT) 
	       && 
	       ((read_file < MAX_DIR_ENTRIES_VERS2) &&((int)(buf_readdir_p -buf_readdir_in) < ROZOFS_READDIR_MAX_BYTES))
	       ) {
            next_hash_entry_idx = DIRENT_CACHE_GETNEXT_ALLOCATED_HASH_ENTRY_IDX(&sect0_p->hash_bitmap, hash_entry_idx);
            if (next_hash_entry_idx < 0) {
                /*
                 ** all the entry of that dirent cache entry have been scanned, need to check the next collision file if
                 ** any
                 */
                cache_entry_p = NULL;
                hash_entry_idx = 0;
                /*
                 ** check the next
                 */
                if (index_level == 0) {
                    coll_idx = 0;
                    index_level = 1;
                    goto get_next_collidx;
                }
                coll_idx++;
                goto get_next_collidx;
            }
            hash_entry_idx = next_hash_entry_idx;
            /*
             ** need to get the hash entry context and then the pointer to the name entry. The hash entry context is
             ** needed since it contains the reference of the starting chunk of the name entry
             */
            hash_entry_p = (mdirents_hash_entry_t*) DIRENT_CACHE_GET_HASH_ENTRY_PTR(cache_entry_p, hash_entry_idx);
            if (hash_entry_p == NULL) {
                /*
                 ** something wrong!! (either the index is out of range and the memory array has been released
                 */
                DIRENT_SEVERE("list_mdirentries pointer does not exist at %d\n", __LINE__);
                /*
                 ** ok, let check the next hash_entry
                 */
                hash_entry_idx++;
                continue;
            }

            /*
            ** something wrong that must not occur !!!
            ** A dentry is allocated but no chunk has been allocated
            */
 	        if (hash_entry_p->nb_chunk==0) {
                hash_entry_idx++;
                continue;
	        }
		     
            /*
             ** OK, now, get the pointer to the name array
             */
            mdirents_name_entry_t *name_entry_p;
            name_entry_p = (mdirents_name_entry_t*) dirent_get_entry_name_ptr(dir_fd, cache_entry_p, hash_entry_p->chunk_idx, DIRENT_CHUNK_NO_ALLOC);
            if (name_entry_p == (mdirents_name_entry_t*) NULL) {
                /*
                 ** something wrong that must not occur
                 */
                severe("list_mdirentries: pointer does not exist");
                /*
                 ** ok, let check the next hash_entry
                 */
                hash_entry_idx++;
                continue;
            }

            /**
             *  that's songs good, copy the content of the name in the result buffer
             */
            /*
	    ** check the length of the filename, if the length is 0, it indicates that we read a truncate dirent file
	    ** so we skip that entry and goes to the next one
	    */
	    if (name_entry_p->len == 0)
	    {
	       char fidstr[37];
	       rozofs_uuid_unparse(fid_parent, fidstr);

                severe("empty name entry in directory %s at hash_idx %d in file d_%d collision idx %d chunk_idx %d",
		        fidstr,hash_entry_idx,root_idx,coll_idx,hash_entry_p->chunk_idx);
                hash_entry_idx++;
                continue;	    
	    
	    }
	    /*
	    ** check if the entry has to be reported to the caller according the state of the delete pending
	    ** flag of the object and of the parent
	    */
	    deleted_obj = exp_metadata_inode_is_del_pending(name_entry_p->fid);
	    /*
	    ** get the inode value for that entry
	    */
	    inode_p = (rozofs_inode_t*) name_entry_p->fid;
#if 1
            /**
	    * check if the parent is a deleted dir
	    */
	    if (deleted_dir)
	    {
	       /*
	       ** if the object is a not a a deleted we need to check if it is a directory
	       */
	       if (deleted_obj == 0)
	       {
	          if (inode_p->s.key != ROZOFS_DIR)
		  {
		     hash_entry_idx++;
		     continue;
		  }
		  mangle_name = 1;
	       }		  	    
	    }
	    else
	    {
	      /*
	      ** This is an active directory
	      */
	      if (deleted_obj)
	      {
		 hash_entry_idx++;
		 continue;	      
	      }	    
	    }
#endif
#if 0
	    if (deleted_dir != deleted_obj) 
	    {
                hash_entry_idx++;
                continue;	    
	    }
#endif
	    {
		/*
		** fill up the reference of the entry within the dirent file
		*/
		dirent_cookie.s.root_idx = root_idx;
		/*
		** to set the pointer to the next entry
		*/
		dirent_cookie.s.hash_entry_idx = hash_entry_idx+1;
		dirent_cookie.s.index_level = index_level;
		dirent_cookie.s.coll_idx = coll_idx;
		dirent_cookie.s.valid_entry = 1;
		if (mangle_name)
		{
		  char bufname[1024];
		  int len = strlen(ROZOFS_DIR_TRASH);
		  memcpy(bufname,ROZOFS_DIR_TRASH,len);
		  memcpy(&bufname[len],name_entry_p->name,name_entry_p->len);
                  buf_readdir_p = rozofs_fuse_add_dirent(buf_readdir_p,inode_p->fid[1],bufname,name_entry_p->len+len,dirent_cookie.val64);
		  mangle_name = 0;
		
		}
		else
		{		
		  buf_readdir_p = rozofs_fuse_add_dirent(buf_readdir_p,inode_p->fid[1],name_entry_p->name,name_entry_p->len,dirent_cookie.val64);
		}
	    }
            /*
             ** increment the number of file and try to get the next one
             */
            hash_entry_idx++;
            read_file++;
            dirent_readdir_stats_file_count++;

        }
        /*
         ** Check if the amount of file has been read
         */
        if ((read_file >= MAX_DIR_ENTRIES_VERS2) || ((int)(buf_readdir_p -buf_readdir_in) >= ROZOFS_READDIR_MAX_BYTES)) {
            /*
             ** We are done
             */
            break;
        }
        /*
         ** No the end and there still some room in the output buffer
         ** Here we need to check the next collision entry if any
         */
        cache_entry_p = NULL;
        hash_entry_idx = 0;
        /*
         ** check the next
         */
        if (index_level == 0) {
            coll_idx = 0;
            index_level = 1;
            goto get_next_collidx;
        }
        coll_idx++;
        goto get_next_collidx;
    }
    /*
     ** done
     */
    /*
     ** set the different parameter
     */
    dirent_cookie.s.root_idx = root_idx;
    dirent_cookie.s.hash_entry_idx = hash_entry_idx;
    dirent_cookie.s.index_level = index_level;
    dirent_cookie.s.coll_idx = coll_idx;
    *cookie = dirent_cookie.val64;

    /*
     ** check the cache status to figure out if root entry need to be released
     */
    if (cached == 1) 
    {
      /*
      ** the content of the dirent file (root+ collision) is in the cache
      ** need to remove from cache if read only is asserted
      */
      if (DIRENT_ROOT_IS_READ_ONLY())
      {
        dirent_remove_root_entry_from_cache(fid_parent, root_idx);
	dirent_cache_release_entry(root_entry_p);
      }
      STOP_PROFILING(list_mdirentries);
      return (int)(buf_readdir_p -buf_readdir_in);
    }
    if (root_entry_p != NULL) {
        int ret = 0;
        ret = dirent_cache_release_entry(root_entry_p);
        if (ret < 0) {
            DIRENT_SEVERE(" get_mdirentry failed to release cache entry\n");
        }
    }
    STOP_PROFILING(list_mdirentries);
    return  (int)(buf_readdir_p -buf_readdir_in);;
}
