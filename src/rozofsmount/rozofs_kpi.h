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

#ifndef ROZOFS_KPI_H
#define ROZOFS_KPI_H
#include <rozofs/rozofs.h>
#include <rozofs/common/list.h>
#include <rozofs/common/htable.h>

typedef enum _rzkpi_ope_e
{
   RZKPI_READ=0,
   RZKPI_WRITE,
   RZKPI_OPEN,
   RZKPI_CREATE,
   RZKPI_MKNOD,
   RZKPI_DELETE,
   RZKPI_CLOSE,
   RZKPI_MAX_OPE
} rzkpi_ope_e;

typedef struct _rozofs_file_kpi_t
{
    hash_entry_t he;/**< hash context to avoid a malloc of the hash_entry */
    fid_t fid; ///< unique file identifier associated with the file or directory
    uint64_t read_bytes_cpt;
    uint64_t write_bytes_cpt;
    uint64_t read_io_cpt;
    uint64_t write_io_cpt;
    int64_t opened_file_count;
    uint64_t created_file_count;
    uint64_t deleted_file_count;
    list_t list;        ///< list used by cache    
} rozofs_file_kpi_t;


/** rzkpi lv2 cache
 *
 * used to keep track of open file descriptors and corresponding attributes
 */
#define RZKPI_LV2_MAX_ENTRIES (16*1024)
#define RZKPI_LV2_BUKETS (1024*16)
#define RZKPI_LV2_MAX_LOCK ROZOFS_HTABLE_MAX_LOCK
typedef struct rzkpi_lv2_cache {
    int max;            ///< max entries in the cache
    int size;           ///< current number of entries
    uint64_t   hit;
    uint64_t   miss;
    uint64_t   lru_del;
    list_t     lru;     ///< LRU 
    /*
    ** case of multi-threads
    */
    list_t     lru_th[RZKPI_LV2_MAX_LOCK];     ///< LRU 
    uint64_t   hash_stats[RZKPI_LV2_MAX_LOCK];
    htable_t htable;    ///< entries hashing
} rzkpi_lv2_cache_t;


extern rozofs_file_kpi_t rzkpi_all_stats;
extern int rzkpi_service_enable;

/*
**____________________________________________________________
*/
/**
*   Update file stats

   @param fid: fid of the file
   @param bytes_count: number of byte to read or write
   @param operation: 1: read/ 0 write

   @retval none
*/
void _rzkpi_file_stat_update(fid_t fid,int bytes_count,int operation);

static inline void rzkpi_file_stat_update(fid_t fid,int bytes_count,int operation)
{
  if (rzkpi_service_enable) return _rzkpi_file_stat_update(fid,bytes_count,operation);
}

/*
**____________________________________________________________
*/
/**
*   Init of the kpi service 


   @retval 0 on success
   @retval -1 on error
*/
int rzkpi_file_service_init();


void show_rzkpi_file(char * argv[], uint32_t tcpRef, void *bufRef);
#endif
