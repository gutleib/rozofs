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

#ifndef ROZOFS_CACHETRACK_H
#define ROZOFS_CACHETRACK_H
#include <rozofs/rozofs.h>
#include <rozofs/common/list.h>
#include <rozofs/common/htable.h>

typedef enum _rzcachetrack_state_e
{
   RZCACHE_IDLE_S=0,
   RZCACHE_PREPARE2CACHE_S,
   RZCACHE_CACHED_S,
   RZCACHE_MAX_S
} rzcachetrack_state_e;

typedef struct _rzcachetrack_conf_t
{
    uint32_t min_children; /**< minimum of children for file caching  */
    uint64_t min_filesize; /**< min file size in bytes                */
    uint64_t max_filesize; /**< max file size in bytes                */
    uint32_t opencount;    /**< open file count within the caching window   */
    uint32_t openperiod_in_ms;  /**< file opening observation period for triggering caching of files  */
    uint32_t min_creation_delay_in_ms; /**< minimum between the creation of the file and file usage before taking it into account for caching */
    uint32_t cachedelay_in_sec;        /**< time during while the file caching is valid  */
} rzcachetrack_conf_t;

typedef struct _rozofs_file_cachetrack_t
{
    hash_entry_t he;   /**< hash context to avoid a malloc of the hash_entry */
    fid_t fid;          /**< unique file identifier of the directory  */
    uint64_t maxfilesize;  /**< max file size : used to compute the amount of 4K blocks that can be cached   */
    uint32_t children;  /**< number of children under the directory   */
    uint16_t state;
    uint16_t opencount;  /**< current number of opened file during an observation period  */
    uint64_t timestamp;
    list_t list;        ///< list used by cache    
} rozofs_file_cachetrack_t;


/** rzkpi lv2 cache
 *
 * used to keep track of open file descriptors and corresponding attributes
 */
#define RZCACHETRACK_LV2_MAX_ENTRIES (16*1024)
#define RZCACHETRACK_LV2_BUKETS (1024*16)
#define RZCACHETRACK_LV2_MAX_LOCK ROZOFS_HTABLE_MAX_LOCK
typedef struct rzcachetrack_lv2_cache {
    int max;            ///< max entries in the cache
    int size;           ///< current number of entries
    uint64_t   hit;
    uint64_t   miss;
    uint64_t   lru_del;
    list_t     lru;     ///< LRU 
    /*
    ** case of multi-threads
    */
    list_t     lru_th[RZCACHETRACK_LV2_MAX_LOCK];     ///< LRU 
    uint64_t   hash_stats[RZCACHETRACK_LV2_MAX_LOCK];
    htable_t htable;    ///< entries hashing
} rzcachetrack_lv2_cache_t;


extern int rzcachetrack_service_enable;


/*
**____________________________________________________________
*/
/**
*   Update information related to file caching within a directory

   @param fid: fid of the directory
   @param filesize: current file size
   @param cr8time creation time of the file

   @retval none
*/
void _rzcachetrack_file(fid_t fid,uint64_t filesize,uint64_t cr8time);

static inline void rzcachetrack_file(fid_t fid,uint64_t filesize,uint64_t cr8time)
{
  if (rzcachetrack_service_enable) return _rzcachetrack_file( fid, filesize, cr8time);
}
/*
**____________________________________________________________
*/
/**
*   Init of the kpi service 


   @retval 0 on success
   @retval -1 on error
*/
int rzcachetrack_file_service_init();


void show_rzcachetrack_file(char * argv[], uint32_t tcpRef, void *bufRef);

extern rzcachetrack_conf_t rzcachetrack_conf;
#endif
