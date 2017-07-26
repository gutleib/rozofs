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

#include <string.h>

#include "xmalloc.h"
#include "log.h"
#include "list.h"
#include "htable.h"


typedef struct hash_entry {
    void *key;
    void *value;
    list_t list;
} hash_entry_t;

inline void htable_initialize(htable_t * h, uint32_t size, uint32_t(*hash) (void *),
                       int (*cmp) (void *, void *)) {
    list_t *it;
    DEBUG_FUNCTION;

    h->hash = hash;
    h->cmp = cmp;
    h->size = size;
    h->lock_size = ROZOFS_HTABLE_MAX_LOCK;
    h->buckets = xmalloc(size * sizeof (list_t));
    for (it = h->buckets; it != h->buckets + size; it++)
        list_init(it);

}

inline void htable_release(htable_t * h) {
    list_t *it;
    DEBUG_FUNCTION;

    for (it = h->buckets; it != h->buckets + h->size; it++) {
        list_t *p, *q;
        list_for_each_forward_safe(p, q, it) {
            hash_entry_t *he = list_entry(p, hash_entry_t, list);
            list_remove(p);
            xfree(he);
        }
    }
    xfree(h->buckets);
    h->buckets = 0;
    h->hash = 0;
    h->cmp = 0;
    h->size = 0;

    return;
}

inline void htable_put(htable_t * h, void *key, void *value) {
    list_t *bucket, *p;
    hash_entry_t *he = 0;

    DEBUG_FUNCTION;

    bucket = h->buckets + (h->hash(key) % h->size);
    // If entry exits replace value.
    list_for_each_forward(p, bucket) {
        he = list_entry(p, hash_entry_t, list);
        if (h->cmp(he->key, key) == 0) {
            //warning("duplicate entry in htable");
            he->value = value;
            return;
        }
    }

    // Else create a new one.
    he = xmalloc(sizeof (hash_entry_t));
    he->key = key;
    he->value = value;
    list_init(&he->list);
    list_push_back(bucket, &he->list);
}

inline void *htable_get(htable_t * h, void *key) {
    list_t *p;
    DEBUG_FUNCTION;

    list_for_each_forward(p, h->buckets + (h->hash(key) % h->size)) {
        hash_entry_t *he = list_entry(p, hash_entry_t, list);
        if (h->cmp(he->key, key) == 0) {
            return he->value;
        }
    }
    return 0;
}

// Return the removed value or NULL if not found.
inline void *htable_del(htable_t * h, void *key) {
    void *value = NULL;
    list_t *p, *q;
    DEBUG_FUNCTION;

    list_for_each_forward_safe(p, q, h->buckets + (h->hash(key) % h->size)) {
        hash_entry_t *he = list_entry(p, hash_entry_t, list);
        if (h->cmp(he->key, key) == 0) {
            value = he->value;
            list_remove(p);
            xfree(he);
            break;
        }
    }

    return value;
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
int htable_initialize_th(htable_t * h, uint32_t size,uint32_t lock_size, uint32_t(*hash) (void *),
                       int (*cmp) (void *, void *)) {
    list_t *it;
    int i;
    int ret;
    DEBUG_FUNCTION;

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
    return 0;

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
void *htable_get_th(htable_t * h, void *key,uint32_t hash) {
    list_t *p;
    DEBUG_FUNCTION;
    /*
    ** take the read lock because of LRU handling
    */
    pthread_rwlock_rdlock(&h->lock[hash%h->lock_size]);
    list_for_each_forward(p, h->buckets + (hash % h->size)) {
        hash_entry_t *he = list_entry(p, hash_entry_t, list);
        if (h->cmp(he->key, key) == 0) {
            pthread_rwlock_unlock(&h->lock[hash%h->lock_size]);
            return he->value;
        }
    }
    pthread_rwlock_unlock(&h->lock[hash%h->lock_size]);
    return 0;
}

/*
**________________________________________________________________
*/
/**
*  Remove an entry from the hash table

  @param h: pointer to the hash table
  @param key: key to search
  @param hash : hash value of the key
  
  @retval NULL if not found
  @retval <> NULL : entry found
*/
void *htable_del_th(htable_t * h, void *key,uint32_t hash) {
    void *value = NULL;
    list_t *p, *q;
    DEBUG_FUNCTION;

    pthread_rwlock_wrlock(&h->lock[hash%h->lock_size]);

    list_for_each_forward_safe(p, q, h->buckets + (hash % h->size)) {
        hash_entry_t *he = list_entry(p, hash_entry_t, list);
        if (h->cmp(he->key, key) == 0) {
            value = he->value;
            list_remove(p);
            xfree(he);
            break;
        }
    }

    pthread_rwlock_unlock(&h->lock[hash%h->lock_size]);
    return value;
}
/*
**________________________________________________________________
*/
/**
*  Insert an entry in the hash table

  @param h: pointer to the hash table
  @param key: key to search
  @param value: pointer to the element to insert
  @param hash : hash value of the key
  
*/
void htable_put_th(htable_t * h, void *key, void *value,uint32_t hash) {
    list_t *bucket, *p;
    hash_entry_t *he = 0;

    DEBUG_FUNCTION;

    bucket = h->buckets + (hash % h->size);
    // If entry exits replace value.
    
    pthread_rwlock_rdlock(&h->lock[hash%h->lock_size]);

    list_for_each_forward(p, bucket) {
        he = list_entry(p, hash_entry_t, list);
        if (h->cmp(he->key, key) == 0) {
            //warning("duplicate entry in htable");
            he->value = value;
             pthread_rwlock_unlock(&h->lock[hash%h->lock_size]);
            return;
        }
    }
    pthread_rwlock_unlock(&h->lock[hash%h->lock_size]);

    // Else create a new one.
    he = xmalloc(sizeof (hash_entry_t));
    he->key = key;
    he->value = value;
    list_init(&he->list);
    
    pthread_rwlock_wrlock(&h->lock[hash%h->lock_size]);
    list_push_back(bucket, &he->list);
    pthread_rwlock_unlock(&h->lock[hash%h->lock_size]);

}
