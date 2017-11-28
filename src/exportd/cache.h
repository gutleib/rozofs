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

#ifndef _CACHE_H
#define _CACHE_H

#include <rozofs/rozofs.h>
#include <rozofs/common/list.h>
#include <rozofs/common/htable.h>
#include <rozofs/common/mattr.h>
#include <rozofs/rpc/eproto.h>

#include "mreg.h"
#include "mdir.h"
#include "mslnk.h"
#include "exp_cache.h"

#define FILE_LOCK_POLL_DELAY_MAX  (common_config.client_flock_timeout)

typedef struct _rozofs_file_lock_t {
  list_t           next_fid_lock;
  list_t           next_client_lock;
  struct ep_lock_t lock;
} rozofs_file_lock_t;


void                 lv2_cache_free_file_lock(uint32_t eid,rozofs_file_lock_t * lock) ;
rozofs_file_lock_t * lv2_cache_allocate_file_lock(uint32_t eid, ep_lock_t * lock, ep_client_info_t * info) ;

/** API lv2 cache management functions.
 *
 * lv2 cache is common to several exports to take care of max fd opened.
 */
 
 
#if 0
/** lv2 entry cached */
typedef struct lv2_entry {
    mattr_t attributes; ///< attributes of this entry
    union {
        mreg_t mreg;    ///< regular file
        mdir_t mdir;    ///< directory
        mslnk_t mslnk;  ///< symlink
    } container;
    list_t list;        ///< list used by cache
    
    /* 
    ** File locking
    */
    int            nb_locks;    ///< Number of locks on the FID
    list_t         file_lock;   ///< List of the lock on the FID
} lv2_entry_t;

/** lv2 cache
 *
 * used to keep track of open file descriptors and corresponding attributes
 */
typedef struct lv2_cache {
    int max;            ///< max entries in the cache
    int size;           ///< current number of entries
    uint64_t   hit;
    uint64_t   miss;
    uint64_t   lru_del;
    list_t entries;     ///< entries cached
    htable_t htable;    ///< entries hashing
} lv2_cache_t;

extern lv2_cache_t cache;

/** initialize a new empty lv2 cache
 *
 * @param cache: the cache to initialize
 */
void lv2_cache_initialize(lv2_cache_t *cache);

/** release a lv2 cache
 *
 * @param cache: the cache to release
 */
void lv2_cache_release(lv2_cache_t *cache);

/** put an entry from the given lv2 path
 *
 * @param cache: the cache to search in
 * @param fid: fid to be cached
 * @param path: path to the underlying file
 *
 * @return: a pointer to the lv2_entry or null on error (errno is set)
 */
lv2_entry_t *lv2_cache_put(lv2_cache_t *cache, fid_t fid, const char *path);

/** get an entry from the given lv2 cache
 *
 * if the entry is not cached, it will be retrieved from the underlying
 * file system.
 *
 * @param export: the export we rely on
 * @param cache: the cache to search in
 * @param fid: the fid we are looking for
 *
 * @return: a pointer to the lv2_entry or null on error (errno is set)
 */
lv2_entry_t *lv2_cache_get(lv2_cache_t *cache, fid_t fid);

/** delete an entry from the given lv2 cache
 *
 * has no effect if the entry is not cached, otherwise entry is removed
 * and freed
 *
 * @param export: the export we rely on
 * @param cache: the cache to search in
 * @param fid: the fid we are looking for
 *
 */
void lv2_cache_del(lv2_cache_t *cache, fid_t fid);
#endif
/** Format statistics information about the lv2 cache
 *
 *
 * @param cache: the cache context
 * @param pChar: where to format the output
 *
 * @retval the end of the output string
 */
char * lv2_cache_display(lv2_cache_t *cache, char * pChar) ;


/*
*___________________________________________________________________
* lock service init 
*
* @param none
*___________________________________________________________________
*/
void file_lock_service_init(void);
/*
*___________________________________________________________________
* lock remove fid locks 
*
* @param lock_list: linked list of the locks
*___________________________________________________________________
*/
void file_lock_remove_fid_locks(list_t * lock_list);
/*
*___________________________________________________________________
* Remove all the locks of a client and then remove the client 
*
* @param client_ref reference of the client to remove
*___________________________________________________________________
*/
void file_lock_remove_client(uint32_t eid, uint64_t client_ref) ;
/*
*___________________________________________________________________
* Receive a poll request from a client
*
* @param client_ref reference of the client to remove
*___________________________________________________________________
*/
void file_lock_poll_client(uint32_t eid, uint64_t client_ref, ep_client_info_t * info) ;
/*
*___________________________________________________________________
* Check whether two lock2 must free or update lock1
*
* @param bsize       The blok size as defined in ROZOFS_BSIZE_E
* @param lock_free   The free lock operation
* @param lock_set    The set lock that must be checked
*
* @retval 1 when locks are compatible, 0 else
*___________________________________________________________________
*/
int must_file_lock_be_removed(uint32_t eid,uint8_t bsize, struct ep_lock_t * lock_free, struct ep_lock_t * lock_set, rozofs_file_lock_t ** new_lock_ctx, ep_client_info_t * info) ;
/*
*___________________________________________________________________
* Check whether two locks are compatible in oreder to set a new one.
* We have to check the effective range and not the user range
*
* @param lock1   1rst lock
* @param lock2   2nd lock
* 
* @retval 1 when locks are compatible, 0 else
*___________________________________________________________________
*/
int are_file_locks_compatible(struct ep_lock_t * lock1, struct ep_lock_t * lock2) ;
/*
*___________________________________________________________________
* Check whether two locks are overlapping. This has to be check at user 
* level in order to merge the different requested locks into one.
*
* @param lock1   1rst lock
* @param lock2   2nd lock
*
* @retval 1 when locks overlap, 0 else
*___________________________________________________________________
*/
int are_file_locks_overlapping(struct ep_lock_t * lock1, struct ep_lock_t * lock2);
/*
*___________________________________________________________________
* Try to concatenate overlapping locks in lock1
*
* @param bsize   The blok size as defined in ROZOFS_BSIZE_E
* @param lock1   1rst lock
* @param lock2   2nd lock
*
* @retval 1 when locks overlap, 0 else
*___________________________________________________________________
*/
int try_file_locks_concatenate(uint8_t bsize, struct ep_lock_t * lock1, struct ep_lock_t * lock2);
/*
*___________________________________________________________________
* To be called after a relaod in order to clean up deleted export
*
*___________________________________________________________________
*/
void file_lock_reload();

char * display_file_lock(char * pChar) ;
char * display_file_lock_clients(char * pChar);
#endif
