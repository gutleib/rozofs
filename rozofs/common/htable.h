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

#ifndef _HTABLE
#define _HTABLE

#include <stdint.h>
#include <pthread.h>
#include "list.h"

#define ROZOFS_HTABLE_MAX_LOCK 16
typedef struct htable {
    uint32_t(*hash) (void *);
    int (*cmp) (void *, void *);
    uint32_t size;
    uint32_t lock_size;
    pthread_rwlock_t lock[ROZOFS_HTABLE_MAX_LOCK]; /**< lock used for insertion/LRU handling */
    list_t *buckets;
} htable_t;

typedef struct hash_entry {
    void *key;
    void *value;
    list_t list;
} hash_entry_t;

/*
**_____________________________________________________________
** Put a hash entry in the hastable
**
** @param h     : pointer to the hash table
** @param entry : The address of the hash entry
*/
static inline void htable_put_entry(htable_t * h, hash_entry_t * entry) {
  list_t *bucket;

  bucket = h->buckets + (h->hash(entry->key) % h->size);

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
  list_push_front(bucket, &entry->list);
}
/*
**_____________________________________________________________
** Remove a hash entry from the hastable
**
** @param h     : pointer to the hash table
** @param entry : The address of the hash entry
*/
static inline void htable_del_entry(htable_t * h, hash_entry_t * entry) {  
  list_remove(&entry->list);
}
void htable_initialize(htable_t * h, uint32_t size, uint32_t(*hash) (void *),
                       int (*cmp) (void *, void *));

void htable_release(htable_t * h);

void htable_put(htable_t * h, void *key, void *value);

void *htable_get(htable_t * h, void *key);

void *htable_del(htable_t * h, void *key);
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
                       int (*cmp) (void *, void *));
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
void *htable_get_th(htable_t * h, void *key,uint32_t hash);
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
void *htable_del_th(htable_t * h, void *key,uint32_t hash);		       
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
void htable_put_th(htable_t * h, void *key, void *value,uint32_t hash);
#endif
