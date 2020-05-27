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

#ifndef DIRENT_VERS1_H
#define DIRENT_VERS1_H

#include <string.h>
#include <pthread.h>
#include <malloc.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/list.h>
#include <rozofs/common/htable.h>
#include <rozofs/core/rozofs_string.h>
#include "mdir.h"
#include <rozofs/common/mattr.h>
#ifdef __i386__
#define PTR2INT (uint32_t)
#else
#define PTR2INT
#endif

/**
 *  Flag in indicate if disk read/write is enable
 */
#define DIRENT_NO_DISK 0
#if DIRENT_NO_DISK
#warning Disk read/write are disabled
#define DIRENT_SKIP_DISK

#endif

#define DIRENT_SEVERE severe    /// just for test purpose
#define DIRENT_WARN severe    /// just for test purpose
#define DIRENT_REPAIR(cache_entry_p) severe("Repair need for cache_entry_p(%d.%d)\n",cache_entry_p->header.dirent_idx[0],cache_entry_p->header.dirent_idx[1]);



/**< Max number of hash entry that can be scanned in a list before asserting a loop detection */
#define DIRENT_MAX_SUPPORTED_COLL  900

/**
 *__________________________________________________
 *  API RELATED TO THE MEMORY ALLOCATION/STATISTICS
 *__________________________________________________
 */
/** @defgroup DIRENT_MALLOC Malloc/free services for cache handling
 *  This module provides services to allocate/release and control the memory used by the dirent cache
 */

/**
 * @brief Dirent Cache Memory management
 @file

 @ingroup DIRENT_MALLOC

 Services:\n
 dirent_malloc() \n
 dirent_mem_print_stats_per_size()\n
 dirent_malloc(int size,int line);\n
 void dirent_free(uint64_t *p,int line);\n
 void dirent_mem_clear_stats();\n
 dirent_mem_print_stats_per_size()\n

 */
/**
 * @ingroup DIRENT_MALLOC
 */
extern uint64_t malloc_size; /**< cumulative allocated bytes */
extern uint32_t malloc_size_tb[]; /**< @ingroup DIRENT_MALLOC per memory block statistics */

extern int dirent_current_eid;  /**< current eid: used by dirent writeback cache */
extern char *dirent_export_root_path[] ;  /**< pointer to the root path of the export */

/*
 *__________________________________________________
 */
/** @ingroup DIRENT_MALLOC
 *   Dirent_malloc:
 *  That API uses malloc(), by it also tracks the amount of memory
 requested by the application by appending the allocated size on
 top of the allocated memory block

 @param size : requested size
 @param line : source line number of caller

 @retval <>NULL pointer to the memroy block allocated (the 8 bytes above the returned ptr, contains the length)
 @retval NULL-> out of memory
 */
static inline void *dirent_malloc(int size, int line) {
    uint64_t *p;
    int idx;

    idx = (size - 1) / 64 + 1;
    malloc_size_tb[idx] += 1;
    p = memalign(32,size + 8);
    if (p == NULL )
        printf("Out of memory at line %d\n", line);
    malloc_size += (uint64_t) size;
    p[0] = (uint64_t) size;
    return p + 1;
}
/*
 *__________________________________________________
 */
/** @ingroup DIRENT_MALLOC
 *   Dirent_free:
 *  That API uses free(), by it also tracks the amount of memory
 used by the application by checking the allocated size on
 top of the allocated memory block

 @param p : pointer to the memory block to release
 @param line : source line number of caller

 @retval none
 */
static inline void dirent_free(uint64_t *p, int line) {
    uint64_t size;
    int idx;

    p -= 1;
    size = *p;
    idx = (size - 1) / 64 + 1;
    malloc_size_tb[idx] -= 1;
    malloc_size -= size;
    free(p);
}

/**
 *  Memory statistics
 */
#define DIRENT_MEM_MAX_IDX  32
/*
 *__________________________________________________
 */
/**  @ingroup DIRENT_MALLOC
 *  Clear the per size memory statistics table
 */
static inline void dirent_mem_clear_stats() {
    memset(malloc_size_tb, 0, sizeof(uint32_t) * DIRENT_MEM_MAX_IDX);
}
/*
 *__________________________________________________
 */
/**  @ingroup DIRENT_MALLOC
 *  Display the per size memory statistics table
 */
static inline void dirent_mem_print_stats_per_size() {
    int i;
    printf("MemStat per block size:\n");
    for (i = 0; i < DIRENT_MEM_MAX_IDX; i++) {
        if (malloc_size_tb[i] == 0)
            continue;
        printf("Count for Block size %d:%d\n", i * 64, malloc_size_tb[i]);
    }
    printf("MemStat per block size Done:\n");

}

/*
 *__________________________________________________
 */
/**  @ingroup DIRENT_MALLOC
 *  that API returns the current malloc size used by the cache
 */
static inline uint64_t DIRENT_MALLOC_GET_CURRENT_SIZE() {
    return malloc_size;

}

#define DIRENT_MALLOC_SIZE_PRINT printf("Malloc size  %llu MBytes (%llu Bytes)\n", \
                                        (long long unsigned int)malloc_size/(1024*1024), \
                                        (long long unsigned int)malloc_size);

#define DIRENT_FREE(p)  dirent_free((uint64_t*)p,__LINE__);

#define DIRENT_MALLOC(length)  dirent_malloc((int)length,__LINE__);
//#define DIRENT_MALLOC_SIZE_CLEAR malloc_size= 0;
#define DIRENT_MALLOC_SIZE_CLEAR
/**
 *__________________________________________________
 *  API RELATED TO LEVEL  0 CACHE
 *__________________________________________________
 */

/**
 @ingroup DIRENT_CACHE_LVL0

 *  API for init of the dirent level 0 cache
 @param cache: pointer to the cache descriptor

 @retval none
 */
void dirent_cache_level0_initialize();

/**
 @ingroup DIRENT_CACHE_LVL0

 * Print the dirent cache bucket statistics (cache level 0)
 */
void dirent_cache_bucket_print_stats();
/**
 *__________________________________________________
 *  API RELATED TO LOOKUP STATISTICS
 *__________________________________________________
 */
/**    @ingroup DIRENT_CACHE_LVL0  */
extern uint64_t dirent_coll_stats_cumul_lookups; /**< total number of lookup operations  */
/**    @ingroup DIRENT_CACHE_LVL0  */
extern uint64_t dirent_coll_stats_cumul_collisions; /**< total number of collision on hash entries */
/**    @ingroup DIRENT_CACHE_LVL0  */
extern uint64_t dirent_coll_stats_tb[]; /**< per collision range statistics table */
/**    @ingroup DIRENT_CACHE_LVL0  */
extern uint64_t dirent_coll_stats_hash_name_collisions; /**< total number of collision on entry name entries (after hash entry match
 */
#define DIRENT_COLL_STAT_MAX_IDX  16

/*
 *__________________________________________________
 */
/**
 @ingroup DIRENT_CACHE_LVL0
 *  Clear lookup statistics counter
 @param none
 @retval none
 */
static inline void dirent_cache_lookup_clear_stats() {
    memset(dirent_coll_stats_tb, 0,
            sizeof(uint64_t) * DIRENT_COLL_STAT_MAX_IDX);
    dirent_coll_stats_cumul_lookups = 0;
    dirent_coll_stats_cumul_collisions = 0;
    dirent_coll_stats_hash_name_collisions = 0;
}
/*
 *__________________________________________________
 */
/**
 @ingroup DIRENT_CACHE_LVL0

 *  Display lookup statistics counters
 @param none
 @retval none
 */
static inline void dirent_cache_lookup_print_stats() {
    int i;
    printf("Lookup collision statistics per range\n");
    for (i = 0; i < DIRENT_COLL_STAT_MAX_IDX; i++) {
        if (dirent_coll_stats_tb[i] == 0)
            continue;
        printf("Count with %8.8d  Collisions: %llu\n", 1 << i,
                (long long unsigned int) dirent_coll_stats_tb[i]);
    }
    printf(
            "Total Lookups :%llu  Total Collisions :%llu average collisions counter :%llu\n",
            (long long unsigned int) dirent_coll_stats_cumul_lookups,
            (long long unsigned int) dirent_coll_stats_cumul_collisions,
            (dirent_coll_stats_cumul_lookups == 0) ?
                    0 :
                    (long long unsigned int) dirent_coll_stats_cumul_collisions
                            / dirent_coll_stats_cumul_lookups);
    printf(" Total number of collisions after hash hit : %llu\n",
            (long long unsigned int) dirent_coll_stats_hash_name_collisions);
    printf("Lookup collision statistics Done:\n");

}

extern uint64_t dirent_readdir_stats_call_count; /**< number of time read has been called */
extern uint64_t dirent_readdir_stats_file_count; /**< total numebr of file read by readdir */

/**
 * Readdir statistics
 */
static inline void dirent_readdir_stats_clear() {
    dirent_readdir_stats_call_count = 0;
    dirent_readdir_stats_file_count = 0;

}

/**
 *  Display the readdir statistics
 */
static inline void dirent_readdir_stats_print() {

    printf("Readdir stats ->Total Calls: %llu   Number of file read :%llu\n",
            (long long unsigned int) dirent_readdir_stats_call_count,
            (long long unsigned int) dirent_readdir_stats_file_count);
}
/*
 *__________________________________________________
 */
/**
 *  Update the lookup statistics
 @param coll_cnt : number of collision to add
 @retval none
 */
static inline void dirent_cache_lookup_update_stats(uint32_t coll_cnt) {
    int i;
    uint32_t val = coll_cnt;
    for (i = 0; i < DIRENT_COLL_STAT_MAX_IDX - 1; i++) {
        val = val >> 1;
        if (val == 0)
            break;
    }
    dirent_coll_stats_tb[i] += 1;
    dirent_coll_stats_cumul_lookups += 1; /* numbers of lookups   */
    dirent_coll_stats_cumul_collisions += coll_cnt; /* numbers of collisions   */
}

#define DIRENT_CACHE_LOOKUP_UPDATE_STATS(coll_cnt) dirent_cache_lookup_update_stats(coll_cnt);

/**
 *_______________________________________________________________________________________
 *
 *  D A T A    S T R U C T U R E S
 *_______________________________________________________________________________________
 */

/** @defgroup DIRENT_FILE_STR  Dirent file structure
 *  \n
 *  Mdirents structure: the mdirents file is a file that is associated with a directory\n
 *  it contains information related to childs of the directory: directory, regular file
 *  and symlink\n
 * The size of a mdirents file is fixed: 4K, and thus might contain up to 14 entries.
 * There are 2 type of mdirents file:\n
 * - MDIRENTS_FILE_ROOT_TYPE : \n
 *  this is the root dirent file whose name corresponds to an index
 *  obtained from a hash function perform on the name of an entry (dir. or file)\n
 * - MDIRENTS_FILE_COLL_TYPE:\n
 *  this is a collision mdirents file that can be either reference from
 *  the root mdirents file or from another collision mdirent file.\n
 *   \n
 *   The structure of a mdirents file is the same whatever its type is.\n
 *   struct _mdirents_file_t is the main structure\n

 <div class="fragment"><pre class="fragment">
 byte 0..1
 version:3        --> Version of the dirent file
 type: 1;         -->  Type of the entry 0: file; 1: memory
 level_index:12   --> 0: root, 1: coll1, 2: coll 2
 byte 2..3
 dirent_idx[0]    -->< Level 0 index
 byte 4..5
 dirent_idx[1]    -->< Level 1 index
 byte 6..7
 dirent_idx[2]    -->< Level 2 index (not used)
 byte 8..9
 max_number_of_hash_entries--> number of hash entries supported by the file
 byte 10..11
 sector_offset_of_name_entry -->index of the first name entry sector
 byte 12..15
 filler--> reserved for future usage

 * </pre></div>
 */

#define MDIRENTS_FILE_ROOT_TYPE 0  ///< root dirents file
#define MDIRENTS_FILE_COLL_TYPE 1  ///< coll dirents file
#define MAX_LV3_BUCKETS 256  ///< coll dirents file
#define MAX_LV4_BUCKETS 256  ///< coll dirents file
/**
 *  structure that define the type of a mdirents file used as linked list
 */
typedef struct _mdirents_file_link_t {
    uint8_t link_type; ///< either MDIRENTS_FILE_ROOT_TYPE or MDIRENTS_FILE_COLL_TYPE

    union {
        uint32_t hash_name_idx; ///< for MDIRENTS_FILE_ROOT_TYPE case
        fid_t fid; ///<  for MDIRENTS_FILE_COLL_TYPE case
    } link_ref;
} mdirents_file_link_t;

#if 0 // Obsolete
/**
 * mdirents file header structure: same structure on disk and memory
 */
typedef struct _mdirents_header_t {
    uint32_t mdirents_type; /// type of the mdirents file: ROOT or COLL
    mdirents_file_link_t fid_cur;///< unique file ID of the current mdirents file
    mdirents_file_link_t fid_next;///< unique file ID of the next mdirents file (coll) or 0 if none
    mdirents_file_link_t fid_prev;///< unique file ID of the previous mdirents file (coll) or 0 if none
}mdirents_header_t;

#endif
/**
 * mdirent header new
 */
#define MDIRENTS_MAX_LEVEL  3          /**< 3 level only: rott, coll1 and coll2 */
#define MDIRENTS_MAX_COLL_LEVEL 1      /**< one collision level only        */
#ifndef FDL_COLLISION_64
#define MDIRENTS_MAX_COLLS_IDX   2048  /**< max number of dirent collision file per dirent file */
#else
#define MDIRENTS_MAX_COLLS_IDX   64  /**< max number of dirent collision file per dirent file */

#endif
/**
 @ingroup DIRENT_FILE_STR
 *  dirent type: file or memory only
 *  that type is used when the system is facing a virtula pointer that is NULL , for the case
 *  of a file it should not be considered as an error since while load root dirent file, if there is
 *  any collision files there are not loaded in memory for performance reason. Remember that it could
 *  have up to 2048 collision files, so loading all the collision files in the cache might have a
 *  big impact on the system. The system rather loads them on deman when needed: either during a search
 *  or when it needs to allocate a free entry.
 */
#define MDIRENT_CACHE_MEM_TYPE   0  ///< the type of the dirent is memory : generic cache
#define MDIRENT_CACHE_FILE_TYPE  1  ///< the type of the dirent is a file,
#define MDIRENT_FILE_VERSION_0   0  ///< current version of the dirent file
/**
 @ingroup DIRENT_FILE_STR
 *  Header used for dirent file
 */
typedef struct _mdirents_header_new_t {
    uint16_t version :3; /// Version of the dirent file
    uint16_t type :1; ///0: file; 1: memory
    uint16_t level_index :12; /// 0: root, 1: coll1, 2: coll 2
    uint16_t dirent_idx[MDIRENTS_MAX_LEVEL]; ///< index of each level
    uint64_t max_number_of_hash_entries :16; ///< number of hash entries supported by the file
    uint64_t sector_offset_of_name_entry :16; ///< index of the first name entry sector
    uint64_t filler :32;
} mdirents_header_new_t;


#define MDIRENTS_BITMAP_COLL_DIRENT_LAST_BYTE  (((MDIRENTS_MAX_COLLS_IDX-1)/8)+1) //< index of the last byte of the bitmap
/**
 *  bitmap of the mdirent collision files (one bit asserted indicates the presence of a file)
 */
typedef struct _mdirents_btmap_coll_dirent_t {
    uint8_t bitmap[MDIRENTS_BITMAP_COLL_DIRENT_LAST_BYTE];
} mdirents_btmap_coll_dirent_t;
//#define DIRENT_LARGE 1
#ifndef DIRENT_LARGE
#define MDIRENTS_ENTRIES_COUNT  384  ///< @ingroup DIRENT_FILE_STR number of entries in a dirent file
#else
#warning DIRENT_LARGE -> 640 entries per file
#define MDIRENTS_ENTRIES_COUNT  640  ///< @ingroup DIRENT_FILE_STR number of entries in a dirent file
#endif
#define MDIRENTS_BITMAP_FREE_HASH_SZ (((MDIRENTS_ENTRIES_COUNT-1)/8)+1)
#define MDIRENTS_BITMAP_FREE_HASH_LAST_BIT_IDX MDIRENTS_ENTRIES_COUNT  //< index of the last valid bit
#define MDIRENTS_IN_BLOCK_LOOPCNT  (MDIRENTS_ENTRIES_COUNT+4)

/**
 *  bitmap of the free hash entries of a dirent file
 */
typedef struct _mdirents_btmap_free_hash_t {
    uint8_t bitmap[MDIRENTS_BITMAP_FREE_HASH_SZ];
} mdirents_btmap_free_hash_t;

/**
 @ingroup DIRENT_FILE_STR
 * structure of a mdirent entry with name, fid, type
 */
typedef struct _mdirents_name_entry_t {
    uint32_t type :20; ///< type of the entry: directory, regular file, symlink, etc...
    uint32_t len :12; ///< length of name without null termination
    fid_t fid; ///< unique ID allocated to the file or directory
    char name[ROZOFS_FILENAME_MAX*2]; ///< name of the directory or file
} mdirents_name_entry_t;

#define MDIRENTS_NAME_CHUNK_SZ  64  ///< chunk size of a block used for storing name and fid
#define MDIRENTS_NAME_CHUNK_MAX  (((sizeof(mdirents_name_entry_t)-1)/MDIRENTS_NAME_CHUNK_SZ)+1) ///< max number of chunk for the max name length
#ifndef DIRENT_LARGE
#if 0
#define MDIRENTS_NAME_CHUNK_MAX_CNT  (MDIRENTS_NAME_CHUNK_MAX*MDIRENTS_ENTRIES_COUNT) ///< max number of chunks for MDIRENTS_ENTRIES_COUNT
#define MDIRENTS_BITMAP_FREE_NAME_SZ ((((MDIRENTS_ENTRIES_COUNT*MDIRENTS_NAME_CHUNK_MAX)-1)/8)+1) ///< bitmap size in bytes
#define MDIRENTS_BITMAP_FREE_NAME_LAST_BIT_IDX ((MDIRENTS_ENTRIES_COUNT*MDIRENTS_NAME_CHUNK_MAX)-1)  //< index of the last valid bit
#endif
#define MDIRENTS_NAME_CHUNK_MAX_CNT 4096 ///< max number of chunks
#define MDIRENTS_BITMAP_FREE_NAME_SZ (MDIRENTS_NAME_CHUNK_MAX_CNT/8)
#define MDIRENTS_BITMAP_FREE_NAME_LAST_BIT_IDX (MDIRENTS_NAME_CHUNK_MAX_CNT-1)
#else
#define MDIRENTS_NAME_CHUNK_MAX_CNT 4096 ///< max number of chunks
#define MDIRENTS_BITMAP_FREE_NAME_SZ (MDIRENTS_NAME_CHUNK_MAX_CNT/8)
#define MDIRENTS_BITMAP_FREE_NAME_LAST_BIT_IDX (MDIRENTS_NAME_CHUNK_MAX_CNT-1)
#endif
/**
 *  bitmap of the free name and fid entries of a dirent file
 */
typedef struct _mdirents_btmap_free_chunk_t {
    uint8_t bitmap[MDIRENTS_BITMAP_FREE_NAME_SZ];
} mdirents_btmap_free_chunk_t;

#define MDIRENTS_HASH_PTR_EOF    0   ///<  end of list or empty
#define MDIRENTS_HASH_PTR_LOCAL  1   ///< index is local to the dirent file
#define MDIRENTS_HASH_PTR_COLL   2   ///< the next entry is found in a dirent collision file (index is not significant
#define MDIRENTS_HASH_PTR_FREE   3   ///<  free entry
/**
 *  hash table logical pointer
 */
typedef struct _mdirents_hash_ptr_t {
    uint16_t bucket_idx_low :4; ///< for hash entry usage only not significant on hash bucket entry
    uint16_t type :2; ///< type of the hash pointer : local, collision or eof (see above)
    uint16_t idx :10; ///< index of the hash entry within the local dirent file or index of the collision dirent entry
} mdirents_hash_ptr_t;

#define MDIRENTS_HASH_TB_INT_SZ 256
/**
 *  dirent hash table pointer structure
 */
typedef struct _mdirents_hash_tab_t {
    mdirents_hash_ptr_t first[MDIRENTS_HASH_TB_INT_SZ]; ///< pointer to the first entry for the corresponding hash value
} mdirents_hash_tab_t;

/**
 @ingroup DIRENT_FILE_STR
 * structure of a mdirent hash entry
 */
#if 0
typedef struct _mdirents_hash_entry_t {
    uint32_t hash; ///< value of the hash applied to the filename or directory
    uint16_t chunk_idx:12;///< index of the first block where the entry with name/type/fid is found
    uint16_t nb_chunk:4;///< number of "name_blocks" allocated for the entry
    mdirents_hash_ptr_t next;///< next entry with the same level1/2 hash value
}mdirents_hash_entry_t;
#endif
#define DIRENT_ENTRY_HASH_MASK ((1<< 28)-1)
typedef struct _mdirents_hash_entry_t {
    uint32_t bucket_idx_high :4; ///< highest bit of the bucket_idx
    uint32_t hash :28; ///< value of the hash applied to the filename or directory
    uint16_t chunk_idx :12; ///< index of the first block where the entry with name/type/fid is found
    uint16_t nb_chunk :4; ///< number of "name_blocks" allocated for the entry
    mdirents_hash_ptr_t next; ///< next entry with the same level1/2 hash value
} mdirents_hash_entry_t;

#define DIRENT_HASH_ENTRY_GET_BUCKET_IDX(hash_entry_p) ((hash_entry_p->bucket_idx_high << 4) | hash_entry_p->next.bucket_idx_low)
#define DIRENT_HASH_ENTRY_SET_BUCKET_IDX(hash_entry_p,bucket_idx) {hash_entry_p->bucket_idx_high = bucket_idx>>4;hash_entry_p->next.bucket_idx_low = bucket_idx;}
#if 0 // Obsolete
/**
 *  structure of a mdirents file: type is a fixed size
 */

typedef struct _mdirents_file_old_t {
    mdirents_header_t header; ///< header of the dirent file: mainly management information
    mdirents_btmap_free_hash_t hash_bitmap;///< bitmap of the free hash entries
    mdirents_btmap_free_chunk_t name_bitmap;///< bitmap of the free name/fid/type entries
    mdirents_hash_tab_t hash_tbl;///< hash table: set of of 256 logical pointers
    mdirents_hash_entry_t hash_entry[MDIRENTS_ENTRIES_COUNT];///< table of hash entries

}mdirents_file_old_t;
#endif

#define MDIRENT_SECTOR_SIZE  512

/**
 *  Sector 0 contains the header of the dirent file and the hash entry bitmap
 */
typedef struct _mdirent_sector0__not_aligned_t {
    mdirents_header_new_t header; ///< header of the dirent file: mainly management information
    mdirents_btmap_coll_dirent_t coll_bitmap; ///< bitmap of the dirent collision file
    mdirents_btmap_free_hash_t hash_bitmap; ///< bitmap of the free hash entries
} mdirent_sector0_not_aligned_t;

typedef union _mdirent_sector0_t {
    uint8_t u8[((sizeof(mdirents_header_new_t)
            + sizeof(mdirents_btmap_free_hash_t) - 1) / MDIRENT_SECTOR_SIZE + 1)
            * MDIRENT_SECTOR_SIZE];
    mdirent_sector0_not_aligned_t s;

} mdirent_sector0_t;

/**
 *  Sector 1 contains name entry bitmap
 */
typedef union _mdirent_sector1_t {
    uint8_t u8[((sizeof(mdirents_btmap_free_chunk_t) - 1) / MDIRENT_SECTOR_SIZE
            + 1) * MDIRENT_SECTOR_SIZE];
    struct {
        mdirents_btmap_free_chunk_t name_bitmap; ///< bitmap of the free name/fid/type entries
    } s;
} mdirent_sector1_t;

/**
 *  Sector 2 contains the hash entry buckets
 */
typedef union _mdirent_sector2_t {
    uint8_t u8[((sizeof(mdirents_hash_tab_t) - 1) / MDIRENT_SECTOR_SIZE + 1)
            * MDIRENT_SECTOR_SIZE];
    struct {
        mdirents_hash_tab_t hash_tbl; ///< hash table: set of of 256 logical pointers
    } s;
} mdirent_sector2_t;

/**
 *  Sector 3 and 4 contains the hash entries
 */
typedef union _mdirent_sector3_t {
    uint8_t u8[((sizeof(mdirents_hash_entry_t) * MDIRENTS_ENTRIES_COUNT - 1)
            / MDIRENT_SECTOR_SIZE + 1) * MDIRENT_SECTOR_SIZE];
    struct {
        mdirents_hash_entry_t hash_entry[MDIRENTS_ENTRIES_COUNT]; ///< table of hash entries
    } s;
} mdirent_sector3_t;

/**
 @ingroup DIRENT_FILE_STR
 * Main structure of the dirent file
 */
typedef struct _mdirents_file_t { ///< Main structure of the dirent file
    mdirent_sector0_t sect0; ///< dirent header + coll_entry bitmap+hash entry bitmap
    mdirent_sector1_t sect1; ///< bitmap of the free name/fid/type entries
    mdirent_sector2_t sect2; ///< hash table: set of of 256 logical pointers
    mdirent_sector3_t sect3; ///< table of hash entries

} mdirents_file_t;

/*
 ** base sectors
 */
#define DIRENT_HEADER_BASE_SECTOR 0   /**< index of the header+coll entry+hash entry bitmap */
#define DIRENT_NAME_BITMAP_BASE_SECTOR (sizeof(mdirent_sector0_t)/MDIRENT_SECTOR_SIZE+DIRENT_HEADER_BASE_SECTOR)   /**< bitmap of the free name:fid/type */
#define DIRENT_HASH_BUCKET_BASE_SECTOR (sizeof(mdirent_sector1_t)/MDIRENT_SECTOR_SIZE+DIRENT_NAME_BITMAP_BASE_SECTOR)   /**< 256 hash buckets*/
#define DIRENT_HASH_ENTRIES_BASE_SECTOR (sizeof(mdirent_sector2_t)/MDIRENT_SECTOR_SIZE+DIRENT_HASH_BUCKET_BASE_SECTOR)   /**< 384 hash entries*/
#define DIRENT_HASH_NAME_BASE_SECTOR (sizeof(mdirent_sector3_t)/MDIRENT_SECTOR_SIZE+DIRENT_HASH_ENTRIES_BASE_SECTOR)   /**< 384 name/fid entries*/

/**
 * number of sectors for each type of entry
 */
#define DIRENT_HEADER_SECTOR_CNT  (sizeof(mdirent_sector0_t)/MDIRENT_SECTOR_SIZE)
#define DIRENT_NAME_BITMAP_SECTOR_CNT  (sizeof(mdirent_sector1_t)/MDIRENT_SECTOR_SIZE)   /**< bitmap of the free name:fid/type */
#define DIRENT_HASH_BUCKET_SECTOR_CNT (sizeof(mdirent_sector2_t)/MDIRENT_SECTOR_SIZE)   /**< 256 hash buckets*/
#define DIRENT_HASH_ENTRIES_SECTOR_CNT (sizeof(mdirent_sector3_t)/MDIRENT_SECTOR_SIZE)   /**< 384 hash entries*/
#define DIRENT_HASH_NAME_SECTOR_CNT ((((MDIRENTS_NAME_CHUNK_MAX_CNT*MDIRENTS_NAME_CHUNK_SZ)-1)/MDIRENT_SECTOR_SIZE)+1)  /**< 384 name/fid entries*/

#define DIRENT_FILE_MAX_SECTORS (DIRENT_HASH_NAME_BASE_SECTOR +DIRENT_HASH_NAME_SECTOR_CNT)

/**
 **-----------------------------------------------------------------------------------------
 DIRENT CACHE SECTION
 **-----------------------------------------------------------------------------------------
 */
/** @defgroup DIRENT_FILE_CACHE_STR  Dirent File Cache structure
 \n
 The main structure of the cache is defined in struct _mdirents_cache_entry_t\n
 - the cache supports up to 256 buckets \n
 - the maximum number of file collisions is 2048 \n
 - each cache entry might contains up to 384 entries \n

 \n

 */

#define DIRENT_CACHE_BUKETS (1024*32)        /**< max number of bucket in the hash table  */
#define DIRENT_CACHE_MAX_ENTRIES (1024*64)  /**< max number of entries in the cache       */
/**
 * dirent cache structure: hash logical pointers:
 */
#define MDIRENTS_HASH_TB_CACHE_MAX_ENTRY  64 ///< max number of hash logical pointer per cache pointer entry
#define MDIRENTS_HASH_TB_CACHE_MAX_IDX (((MDIRENTS_HASH_TB_INT_SZ-1)/MDIRENTS_HASH_TB_CACHE_MAX_ENTRY)+1) ///< number of pointer per cache entry

/**
 * dirent cache structure: hash entries:
 */

#define MDIRENTS_HASH_CACHE_MAX_ENTRY  64 ///< max number of hash entries per cache array
#define MDIRENTS_HASH_CACHE_MAX_IDX (((MDIRENTS_ENTRIES_COUNT-1)/MDIRENTS_HASH_CACHE_MAX_ENTRY)+1) ///< number of pointer per cache entry
/**
 *  name_entry_lvl0_p array
 */
#define MDIRENTS_CACHE_CHUNK_INDIRECT_CNT 2
#define MDIRENTS_CACHE_CHUNK_SECTOR_CNT  4    ///< number of sector per chunk array
#define MDIRENTS_CACHE_CHUNK_ARRAY_SZ (MDIRENT_SECTOR_SIZE*4)  ///< memory size allocated in memory for a chunk array
#define MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY (MDIRENTS_CACHE_CHUNK_ARRAY_SZ/MDIRENTS_NAME_CHUNK_SZ) /// number of chunk per chunk memory array
#define MDIRENTS_NAME_PTR_LVL0_NB_BIT  3  /// 8 pointers
#define MDIRENTS_NAME_PTR_LVL0_NB_PTR  (1<<MDIRENTS_NAME_PTR_LVL0_NB_BIT) ///< number of level 0 pointer of level1 pointers
#define MDIRENTS_NAME_PTR_LVL1_NB_BIT  4  /// 16 pointers
#define MDIRENTS_NAME_PTR_LVL1_NB_PTR  (1<< MDIRENTS_NAME_PTR_LVL1_NB_BIT) ///<number chunk memory array pointer at level 1
#define MDIRENTS_NAME_PTR_MAX  (MDIRENTS_NAME_PTR_LVL1_NB_PTR*MDIRENTS_NAME_PTR_LVL0_NB_PTR)
#define MDIRENTS_NAME_ARRAY_BITMAP_BYTE_MAX   (MDIRENTS_NAME_PTR_MAX/8)

//#define MDIRENTS_LEVEL0_NAME_CHUNK_CNT  (MDIRENTS_NAME_PTR_LVL1_NB_PTR*MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY) ///<number of pointer per level0 array
//#define MDIRENTS_LEVEL0_PTR_CNT  (((MDIRENTS_ENTRIES_COUNT*MDIRENTS_NAME_CHUNK_MAX-1)/MDIRENTS_LEVEL0_NAME_CHUNK_CNT) +1)

/**
 dirent collision array size:
 there are 3 level of pointer, the last level corresponds to a pointer to a dirent cache entry (or memory represenation
 of a dirent file
 */
#ifndef FDL_COLLISION_64
//#warning "Collision Max is 2048"
#define MDIRENTS_CACHE_DIRENT_COLL_INDIRECT_CNT 3
#define MDIRENTS_CACHE_DIRENT_COLL_LVL0_NB_BIT  4  /// 16 pointers
#define MDIRENTS_CACHE_DIRENT_COLL_LVL0_NB_PTR  (1<<MDIRENTS_CACHE_DIRENT_COLL_LVL0_NB_BIT)
#define MDIRENTS_CACHE_DIRENT_COLL_LVL1_NB_BIT  2  /// 4 pointers
#define MDIRENTS_CACHE_DIRENT_COLL_LVL1_NB_PTR  (1<<MDIRENTS_CACHE_DIRENT_COLL_LVL1_NB_BIT)
#define MDIRENTS_CACHE_DIRENT_COLL_LVL2_NB_BIT  5  /// 32 pointers
#define MDIRENTS_CACHE_DIRENT_COLL_LVL2_NB_PTR (MDIRENTS_MAX_COLLS_IDX/(MDIRENTS_CACHE_DIRENT_COLL_LVL0_NB_PTR*MDIRENTS_CACHE_DIRENT_COLL_LVL1_NB_PTR))

#else
#warning "Collision Max is 64"
#define MDIRENTS_CACHE_DIRENT_COLL_INDIRECT_CNT 3
#define MDIRENTS_CACHE_DIRENT_COLL_LVL0_NB_BIT  1  /// 2 pointers
#define MDIRENTS_CACHE_DIRENT_COLL_LVL0_NB_PTR  (1<<MDIRENTS_CACHE_DIRENT_COLL_LVL0_NB_BIT)
#define MDIRENTS_CACHE_DIRENT_COLL_LVL1_NB_BIT  2  /// 4 pointers
#define MDIRENTS_CACHE_DIRENT_COLL_LVL1_NB_PTR  (1<<MDIRENTS_CACHE_DIRENT_COLL_LVL1_NB_BIT)
#define MDIRENTS_CACHE_DIRENT_COLL_LVL2_NB_BIT  3  /// 8 pointers
#define MDIRENTS_CACHE_DIRENT_COLL_LVL2_NB_PTR (MDIRENTS_MAX_COLLS_IDX/(MDIRENTS_CACHE_DIRENT_COLL_LVL0_NB_PTR*MDIRENTS_CACHE_DIRENT_COLL_LVL1_NB_PTR))

#endif
/**
 @ingroup DIRENT_FILE_CACHE_STR
 * generic pointer used by dirent cache:
 */
typedef union {
    uint64_t u64;
    struct {
        uint64_t rd :1; ///<  assert to 1 for read request and 0 for write request
        uint64_t dirty :1; ///< entry need to be flushed or read on/from disk depnding on the value of rd.
        uint64_t val :62; ///< pointer to the memory array
    } s;
} mdirent_cache_ptr_t;

/**
 *  Macro to convert virtual pointer to physical pointer with an offset
 *   p : pointer to a structure with virtual pointer
 *   offset : offset of the field where there is the virtual pointer
 */
//#define DIRENT_VIRT_TO_PHY_OFF(p,offset)  (p->offset.s.val==0)?0:(void*)(uint64_t)(p->offset.s.val);
// TEST
#ifdef __i386__
#define DIRENT_VIRT_TO_PHY_OFF(p,offset)  (p->offset.s.val==0)?0:(void*)(uint32_t)(p->offset.s.val);
#else
#define DIRENT_VIRT_TO_PHY_OFF(p,offset)  (p->offset.s.val==0)?0:(void*)(uint64_t)(p->offset.s.val);
#endif

/**
 *  Macro to convert virtual pointer to physical pointer
 */
#ifdef __i386__
#define DIRENT_VIRT_TO_PHY(vir_p)  (vir_p.s.val==0)?0:(void*)(uint32_t)(vir_p.s.val);
#else
#define DIRENT_VIRT_TO_PHY(vir_p)  (vir_p.s.val==0)?0:(void*)(uint64_t)(vir_p.s.val);
#endif

/**
 *  clear a virtual pointer
 */
#define DIRENT_CLEAR_VIRT_PTR(vir_p) vir_p.u64 =0;
/**
 *  structure of the primary key to access dirent cache
 */
typedef struct _mdirents_cache_key_t {
    fid_t dir_fid; ///< fid of the parent directory
    uint32_t dirent_root_idx; ///< index of the dirent root file
} mdirents_cache_key_t;

/**
 @ingroup DIRENT_FILE_CACHE_STR
 * Dirent cache entry structure
 */
typedef struct _mdirents_cache_entry_t {
    hash_entry_t he;       /**< BATCH mode use only */
    uint32_t     hash;     /**< BATCH mode use only */
    uint64_t     deadline_timestamp; /**< BATCH mode use only */
    list_t       lru_link;      /**< BATCH mode : lru list   */

    list_t cache_link; /**< linked list of the root dirent cache entries   */
    list_t coll_link; /**< linked list of the dirent collision entries    */
//    void    *hash_ptr;    /**< pointer to the hash entry                      */
    mdirents_cache_key_t key; /**<  primary key */
    uint64_t writeback_ref; /**< reference of the write back buffer */
    mdirents_header_new_t header; ///< header of the dirent file: mainly management information
    uint8_t  *bucket_safe_bitmap_p; /**< only allocated for root entry */
    uint32_t hash_entry_full :1; /**< assert to 1 when all the entries of the parent have been allocated  */
    uint32_t root_updated_requested :1; /**< that bit is assert when root cache entry must be re-written */
    uint32_t filler0 :30; /**< for future usage */
    uint8_t name_entry_array_btmap_presence[MDIRENTS_NAME_ARRAY_BITMAP_BYTE_MAX]; /**< bitmap of the name entry array presence: each array can contain up to 32 chunks  */
    uint8_t name_entry_array_btmap_wr_req[MDIRENTS_NAME_ARRAY_BITMAP_BYTE_MAX]; /**< bitmap of the name entry array that needs to be re-write on disk  */

//    mdirent_cache_ptr_t     hash_bitmap_p;  ///< bitmap of the free hash entries
    mdirent_cache_ptr_t sect0_p; ///< header+coll entry bitmap+hash entry bitmap
    mdirent_cache_ptr_t coll_bitmap_hash_full_p; ///< bitmap of the collision dirent for which all hash entries are busy
    mdirent_cache_ptr_t name_bitmap_p; ///< bitmap of the free name/fid/type entries
    mdirent_cache_ptr_t hash_tbl_p[MDIRENTS_HASH_TB_CACHE_MAX_IDX]; ///< set of pointer to an cache array of 64 hash logical pointers
    mdirent_cache_ptr_t hash_entry_p[MDIRENTS_HASH_CACHE_MAX_IDX]; ///< table of hash entries
    mdirent_cache_ptr_t name_entry_lvl0_p[MDIRENTS_NAME_PTR_LVL0_NB_PTR]; ///< table of hash entries
    mdirent_cache_ptr_t dirent_coll_lvl0_p[MDIRENTS_CACHE_DIRENT_COLL_LVL0_NB_PTR]; ///< dirent collision file array pointers */

} mdirents_cache_entry_t;


/*
** structure needed by the batch mode on the rozofsmount client: the rozofsmount uses a different cache structure
**  Its cache structure supports parallel lookup which not the case for the export
*/
typedef int (*dirent_remove_root_entry_from_cache_t)(fid_t fid, int root_idx);
typedef  int (*dirent_put_root_entry_to_cache_t)(fid_t fid, int root_idx, mdirents_cache_entry_t *root_p);
typedef mdirents_cache_entry_t * (*dirent_cache_bucket_search_entry_t)(fid_t fid, uint16_t index);


typedef struct _dirent_bt_cache_cbk_t
{
    int batch_mode_enable;  /**< assert to 1 for the client rozofsmount and 0 for the exportd  */
    dirent_put_root_entry_to_cache_t  put;
    dirent_cache_bucket_search_entry_t get;
    dirent_remove_root_entry_from_cache_t remove;
} dirent_bt_cache_cbk_t;

extern dirent_bt_cache_cbk_t dirent_bt_cache_cbk;


#define DIRENT_ROOT_UPDATE_REQ(root)  if (root != NULL) root->root_updated_requested = 1;
#define DIRENT_ROOT_UPDATE_DONE(root) if (root != NULL) root->root_updated_requested = 0;
#define DIRENT_IS_ROOT_UPDATE_REQ(root) (root->root_updated_requested == 1)
/** dirent cache
 *
 * use to keep track of the file/directory associated with a parent directory
 */
typedef struct dirent_cache {
    int max; ///< max entries in the cache
    int size; ///< current number of entries
    list_t entries; ///< entries cached
    htable_t htable; ///< entries hashing
} dirent_cache_t;

/**
 *  public API:
 Build a dirent file name.
 the filename has the following structure
 level0 (root): dirent_"dirent_idx[0]"
 level1 (coll1): dirent_"dirent_idx[0]"_"dirent_idx[1]"
 level2 (coll2): dirent_"dirent_idx[0]"_"dirent_idx[1]"_"dirent_idx[2]"

 @param *p: pointer to the dirent header
 @param *buf: pointer to the output buffer

 @retval NULL if header has wrong information
 @retval <>NULL pointer to the beginning of the dirent filename
 */
static inline char *dirent_build_filename(mdirents_header_new_t *p, char *buf) {
    char *pdata;
    int i;
    if (p->level_index > MDIRENTS_MAX_COLL_LEVEL)
        return NULL ;
    pdata = buf;
    pdata += sprintf(pdata, "d");
    for (i = 0; i < p->level_index + 1; i++)
        pdata += sprintf(pdata, "_%d", p->dirent_idx[i]);
    return buf;
}
/**
 *  @defgroup DIRENT_BITMAP Bitmap manipulation low level services
 These services are intended to be used by the following entities:
 - allocation/release of a hash entries (1 bit only allocated or released)\n
 - allocation/release of a consecutive serie of bits: typically name entries\n
 - allocation/rlease of a collision file\n
 \n
 -list of the services:\n
 -
 - check_bytes():skips the allocated bitmap entries of a bitmap and returns the fist free \n
 - check_bytes_val(): same a check_bytes() but provides in argument the type that must be skipped\n
 - dirent_allocate_chunks(): allocate one or more bits in a bitmap\n
 - dirent_allocate_chunks_with_alignment(): same as previous but avoid spanning on two memory array\n
 note: that service is mainly used for the name entry since the system needs to allocate more\n
 that one chunk for storing the name/fid and mode. Since we want to avoid a name entry to be\n
 split on two different 1K memory array, we perform an allocated with an alignment\n
 - dirent_release_chunks(): release one or more bits in a bitmap\n
 - dirent_set_chunk_bit() : assert a bit in a bitmap\n
 - dirent_clear_chunk_bit() : deassert a bit in a bitmap\n
 - dirent_test_chunk_bit() : return the current value of a bit\n

 */

/**
 * statistics
 */
extern uint32_t check_bytes_call;
extern uint32_t check_bytes_val_call;
extern uint32_t dirent_skip_64_cnt;
extern uint32_t dirent_skip_32_cnt;
extern uint32_t dirent_skip_16_cnt;

#define CHECK_BYTES_CALL(p) p++;
#define DIRENT_SKIP_STATS_INC(p) p++;
#define DIRENT_SKIP_STATS_CLEAR { check_bytes_call = 0;\
                                  check_bytes_val_call = 0;dirent_skip_64_cnt =0;dirent_skip_32_cnt=0;dirent_skip_16_cnt=0;}

#if 0

static uint8_t firstBitOfChar[256] = {
  0,
  0,
  1,1,
  2,2,2,2,
  3,3,3,3,3,3,3,3,
  4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,
  5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,
  6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,
  6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,
  7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
  7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
  7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
  7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7
};

/** @ingroup DIRENT_BITMAP
 * Find the first byte that is set to 1 in the two bit maps

 @param p1    : pointer to the first bitmap 
 @param p2    : pointer to the second bitmap 
 @param from  : 1rst bit to start the search from (0 is the very first bit)
 @param size  : size in of the bitmap in number of 64bits


 @retval -1 no bit found
 @retval index of the bit found
 */
static inline int select_collision_index(void *pvAllocated, void *pvNotFull, int * is_allocated) {
  uint32_t   value;
  int        idx32, idx8;
  uint32_t * pAllocated = pvAllocated;
  uint32_t * pNotFull   = pvNotFull
  uint8_t  * p8;
  
  /* Find 1rst 32 bit valid */
  for (idx32=0; idx32 < size; idx32++, pAllocated++, pNotFull++) {
    
    /* Take the first allocated not full */
    value = ~*pAllocated & *pNotFull;
    if (value != 0) {
      *is_allocated = 1;
      break;
    }
    
    /* take the first free */
    value = ~*pAllocated;
    if (value != 0) {
      *is_allocated = 0;
      break;
    }
  }  
  
  /* Have not found any bit set */
  if (idx32 == size) return -1;  
  
  /* A bit is set */
  p8 = (uint8_t *) &value;  
  for (idx8=0; idx8 < 4; idx8++, p8++) {
    if (*p8 != 0) break;
  }    
  return firstBitOfChar[*p8] + (idx8*8) + (idx32*32);
}

#endif



/*
 **______________________________________________________________________________
 */

/**
 *   Compute the hash values for the name and fid

 @param key1 : pointer to a string ended by \0
 @param key2 : pointer to a fid (16 bytes)
 @param hash2 : pointer to the second hash value that is returned
 @param len : pointer an array when the length of the filename will be returned (it includes \0)

 @retval primary hash value
 */
static inline uint32_t filename_uuid_hash_fnv(uint32_t h, void *key1, void *key2, uint32_t *hash2, int *len) {

    unsigned char *d = (unsigned char *) key1;
    int i = 0;

    if (h == 0) h = 2166136261U;
    /*
     ** hash on name
     */
    for (d = key1; *d != '\0'; d++, i++) {
        h = (h * 16777619)^ *d;

    }
    *len = i;

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


/** @ingroup DIRENT_BITMAP
 *  That function go throught the bitmap of free chunk by skipping allocated chunks
 it returns the next chunk to check or -1 if there is no more free chunk



 @param p : pointer to the bitmap of the free chunk (must be aligned on a 8 bytes boundary)
 @param first_chunk : first chunk to test
 @param last_chunk: last allocated chunk
 @param loop_cnt : number of busy chunks that has been skipped (must be cleared by the caller

 @retval next_chunk < 0 : out of free chunk (last_chunk has been reached
 @retval next_chunk !=0 next free chunk bit to test
 */
static inline int check_bytes(uint8_t *p, int first_chunk, int last_chunk,
        int *loop_cnt) {
    int chunk_idx = first_chunk;
    int chunk_u8_idx = 0;
    int align;
//  *loop_cnt = 0;

    CHECK_BYTES_CALL(check_bytes_call);
    while (chunk_idx < last_chunk) {
        chunk_u8_idx = chunk_idx / 8;
        align = chunk_u8_idx & 0x07;
        switch (align) {
        case 0:
            if (((uint64_t*) p)[chunk_idx / 64] == 0) {
                chunk_idx += 64;
                DIRENT_SKIP_STATS_INC(dirent_skip_64_cnt);
                break;
            }
            if (((uint32_t*) p)[chunk_idx / 32] == 0) {
                DIRENT_SKIP_STATS_INC(dirent_skip_32_cnt);
                chunk_idx += 32;
                break;
            }
            if (((uint16_t*) p)[chunk_idx / 16] == 0) {
                DIRENT_SKIP_STATS_INC(dirent_skip_16_cnt);
                chunk_idx += 16;
                break;
            }
            if (((uint8_t*) p)[chunk_idx / 8] == 0) {
                chunk_idx += 8;
                break;
            }
            if ((((uint8_t*) p)[chunk_idx / 8] & 0xf) == 0) {
                chunk_idx += 4;
            }
            return chunk_idx;

        case 2:
        case 6:
            if (((uint16_t*) p)[chunk_idx / 16] == 0) {
                DIRENT_SKIP_STATS_INC(dirent_skip_16_cnt);
                chunk_idx += 16;
                break;
            }
            if (((uint8_t*) p)[chunk_idx / 8] == 0) {
                chunk_idx += 8;
                break;
            }
            if ((((uint8_t*) p)[chunk_idx / 8] & 0xf) == 0) {
                chunk_idx += 4;
            }
            return chunk_idx;

        case 4:
            if (((uint32_t*) p)[chunk_idx / 32] == 0) {
                DIRENT_SKIP_STATS_INC(dirent_skip_32_cnt);
                chunk_idx += 32;
                break;
            }
            if (((uint16_t*) p)[chunk_idx / 16] == 0) {
                DIRENT_SKIP_STATS_INC(dirent_skip_16_cnt);
                chunk_idx += 16;
                break;
            }
            if (((uint8_t*) p)[chunk_idx / 8] == 0) {
                chunk_idx += 8;
                break;
            }
            if ((((uint8_t*) p)[chunk_idx / 8] & 0xf) == 0) {
                chunk_idx += 4;
            }
            return chunk_idx;
        case 1:
        case 3:
        case 5:
        case 7:
            if (((uint8_t*) p)[chunk_idx / 8] == 0) {
                chunk_idx += 8;
                break;
            }
            if ((((uint8_t*) p)[chunk_idx / 8] & 0xf) == 0) {
                chunk_idx += 4;
            }
            return chunk_idx;
        }
        *loop_cnt += 1;
    }
    return chunk_idx;
}

/** @ingroup DIRENT_BITMAP
 *  That function go throught the bitmap of free chunk by skipping allocated chunks
 it returns the next chunk to check or -1 if there is no more free chunk



 @param p : pointer to the bitmap of the free chunk (must be aligned on a 8 bytes boundary)
 @param first_chunk : first chunk to test
 @param last_chunk: last allocated chunk
 @param loop_cnt : number of busy chunks that has been skipped (must be cleared by the caller
 @param empty :assert to 1 for searching for free chunk or 0 for allocated chunk

 @retval next_chunk < 0 : out of free chunk (last_chunk has been reached
 @retval next_chunk !=0 next free chunk bit to test
 */
static inline int check_bytes_val(uint8_t *p, int first_chunk, int last_chunk,
        int *loop_cnt, uint8_t empty) {
    int chunk_idx = first_chunk;
    int chunk_u8_idx = 0;
    int align;
    uint64_t val = 0;

    /*
     ** check if the search is for the next free array or the next busy array
     */
    if (empty)
        val = ~val;
//  *loop_cnt = 0;
    CHECK_BYTES_CALL(check_bytes_val_call);

    while (chunk_idx < last_chunk) {
        chunk_u8_idx = chunk_idx / 8;
        align = chunk_u8_idx & 0x07;
        switch (align) {
        case 0:
            if (((uint64_t*) p)[chunk_idx / 64] == val) {
                DIRENT_SKIP_STATS_INC(dirent_skip_64_cnt);
                chunk_idx += 64;
                break;
            }
            if (((uint32_t*) p)[chunk_idx / 32] == (val & 0xffffffff)) {
                DIRENT_SKIP_STATS_INC(dirent_skip_32_cnt);
                chunk_idx += 32;
                break;
            }
            if (((uint16_t*) p)[chunk_idx / 16] == (val & 0xffff)) {
                DIRENT_SKIP_STATS_INC(dirent_skip_16_cnt);
                chunk_idx += 16;
                break;
            }
            if (((uint8_t*) p)[chunk_idx / 8] == (val & 0xff)) {
                chunk_idx += 8;
                break;
            }
            if ((((uint8_t*) p)[chunk_idx / 8] & 0xf) == (val & 0xf)) {
                chunk_idx += 4;
            }
            return chunk_idx;

        case 2:
        case 6:
            if (((uint16_t*) p)[chunk_idx / 16] == (val & 0xffff)) {
                DIRENT_SKIP_STATS_INC(dirent_skip_16_cnt);
                chunk_idx += 16;
                break;
            }
            if (((uint8_t*) p)[chunk_idx / 8] == (val & 0xff)) {
                chunk_idx += 8;
                break;
            }
            if ((((uint8_t*) p)[chunk_idx / 8] & 0xf) == (val & 0xf)) {
                chunk_idx += 4;
            }
            return chunk_idx;

        case 4:
            if (((uint32_t*) p)[chunk_idx / 32] == (val & 0xffffffff)) {
                DIRENT_SKIP_STATS_INC(dirent_skip_32_cnt);
                chunk_idx += 32;
                break;
            }
            if (((uint16_t*) p)[chunk_idx / 16] == (val & 0xffff)) {
                DIRENT_SKIP_STATS_INC(dirent_skip_16_cnt);
                chunk_idx += 16;
                break;
            }
            if (((uint8_t*) p)[chunk_idx / 8] == (val & 0xff)) {
                chunk_idx += 8;
                break;
            }
            if ((((uint8_t*) p)[chunk_idx / 8] & 0xf) == (val & 0xf)) {
                chunk_idx += 4;
            }
            return chunk_idx;
        case 1:
        case 3:
        case 5:
        case 7:
            if (((uint8_t*) p)[chunk_idx / 8] == (val & 0xff)) {
                chunk_idx += 8;
                break;
            }
            if ((((uint8_t*) p)[chunk_idx / 8] & 0xf) == (val & 0xf)) {
                chunk_idx += 4;
            }
            return chunk_idx;
        }
        *loop_cnt += 1;
    }
    /*
     ** all the bits have been skipped
     */
    return -1;
}

/** @ingroup DIRENT_BITMAP
 *  API to allocate a consecutive number of chunks

 @param nb_chunks : number of requested chunks
 @param last_chunk : last chunk index
 @param p : pointer to the bitmap of the free chunks

 @retval !=-1 -> reference of the first index that has been allocated
 @retval =-1 -> allocation failure
 */
static inline int dirent_allocate_chunks(uint8_t nb_chunks, uint8_t *p,
        int last_chunk) {
    uint8_t chunk_u8_idx;
    int chunk_idx = 0;
    int next_chunk_idx = 0;
    int start_idx = -1;
    int loop_cnt = 0;
//    int  loop_while_cnt  = 0;
    int bit_idx;
    int count_idx = 0;
    int k;

    /*
     ** search if there a range of free chunk with a size of nb_chunk
     */
    while (chunk_idx < last_chunk) {
        chunk_u8_idx = chunk_idx / 8;
        bit_idx = chunk_idx % 8;
        loop_cnt++;
        if ((p[chunk_u8_idx] & (1 << bit_idx)) == 0) {
            /*
             ** that bit is not free, so restart from the begin (relative)
             */
            start_idx = -1;
            count_idx = 0;
            /*
             ** check if the index matches with chunk_u64_idx+1
             ** in that case we exit the while loop if no free bit has not yet been found
             */
            if (chunk_idx % 8 == 0) {
                next_chunk_idx = check_bytes_val(p, chunk_idx, last_chunk,
                        &loop_cnt, 0);
                if (next_chunk_idx < 0)
                    break;
                /*
                 ** next  chunk
                 */
                if (next_chunk_idx == chunk_idx)
                    chunk_idx++;
                else
                    chunk_idx = next_chunk_idx;
                continue;
            }
            /*
             ** next chunk
             */
            chunk_idx++;
            continue;

        }
        /*
         ** the bit is free, so check if is the fist one or the next in the list
         ** of the requested consecutive chunks
         */
        if (start_idx == -1) {
            /**
             * check if it remains enough bits to proceed with the allocation
             */
            if (chunk_idx + nb_chunks > last_chunk) {
                /*
                 ** there is no enough bits to allocated
                 */
                break;
            }
            start_idx = (int) chunk_idx;
            count_idx = 1;
        } else {
            count_idx += 1;
        }
        /*
         ** check if the requested number of chunks has been reached
         */
        if (count_idx < nb_chunks) {
            /*
             ** need more bits
             */
            chunk_idx++;
            continue;
        }
        /*
         ** all the needed chunks hasve been found, so exit from while loop
         */
        break;
    }
    /*
     ** check if the allocation succeeds
     */
    if (start_idx == -1) {
        /*
         ** that situation must not occur:
         */
//       printf("out of chunk loop_cnt %d\n",loop_cnt);
        return -1;
    }
    /*
     ** OK, now proceed with the reservation of the bits
     */
    for (k = start_idx; k < (start_idx + nb_chunks); k++) {
        chunk_u8_idx = k / 8;
        bit_idx = k % 8;
        (p[chunk_u8_idx] &= ~(1 << bit_idx));

    }
//    printf("loop_cnt %d\n",loop_cnt);
    return start_idx;

}

/** @ingroup DIRENT_BITMAP
 *  API to Get the next chunk whose bit value is indicated by val

 @param first_chunk :starting chunk index
 @param last_chunk : last chunk index
 @param p : pointer to the bitmap of the free chunks
 @param val: 0: search for entry with bit cleared, 1: search for entry with bit asserted (no other value accepted)

 @retval !=-1 -> reference of the first index that has been allocated
 @retval =-1 -> allocation failure
 */
static inline int dirent_getnext_chunk(int first_chunk, uint8_t *p,
        int last_chunk, int val) {
    uint8_t chunk_u8_idx;
    int chunk_idx = first_chunk;
    int next_chunk_idx = 0;
    //int start_idx = -1;
    int loop_cnt = 0;
//    int  loop_while_cnt  = 0;
    int bit_idx;
    int bit_res;

    /*
     ** search if there a range of free chunk with a size of nb_chunk
     */
    while (chunk_idx < last_chunk) {
        chunk_u8_idx = chunk_idx / 8;
        bit_idx = chunk_idx % 8;
        loop_cnt++;
        bit_res = (p[chunk_u8_idx] & (1 << bit_idx)) ? 1 : 0;
        if (bit_res != val) {
            /*
             ** no the expected value so restart from the begin (relative)
             */
            //start_idx = -1;
            /*
             ** if we start on a byte boundary, we call check_bytes_val to try to skip
             ** up to a maximun of 64 bits to speed up the search. The value to skip is
             ** the complement of val (assuming val is 1 bit)
             */
            if (chunk_idx % 8 == 0) {
                next_chunk_idx = check_bytes_val(p, chunk_idx, last_chunk,
                        &loop_cnt, (1 - val));
                if (next_chunk_idx < 0)
                    break;
                /*
                 ** next  chunk
                 */
                if (next_chunk_idx == chunk_idx)
                    chunk_idx++;
                else
                    chunk_idx = next_chunk_idx;
                continue;
            }
            /*
             ** next chunk
             */
            chunk_idx++;
            continue;

        }
        /*
         ** We got an entry with the expected value-> so all is fine, just return the value
         */
        return chunk_idx;
    }
    /*
     ** unlucky, all the chunks of the expected value have already been scanned
     */
    return -1;

}

/** @ingroup DIRENT_BITMAP
 *  API to allocate a consecutive number of chunks with a given alignment

 @param nb_chunks : number of requested chunks
 @param last_chunk : last chunk index
 @param alignment: requested alignment (must be a power of 2) : 1,2,4,8,16,32......
 @param p : pointer to the bitmap of the free chunks

 @retval !=-1 -> reference of the first index that has been allocated
 @retval =-1 -> allocation failure
 */
static inline int dirent_allocate_chunks_with_alignment(uint8_t nb_chunks,
        uint32_t alignment, uint8_t *p, int last_chunk) {
    uint16_t chunk_u8_idx;
    int chunk_idx = 0;
    int next_chunk_idx = 0;
    int start_idx = -1;
    int loop_cnt = 0;
//    int  loop_while_cnt  = 0;
    int bit_idx;
    int count_idx = 0;
    int k;
    int mask = alignment - 1;

    mask = ~mask;
    /*
     ** search if there a range of free chunk with a size of nb_chunk
     */
    while (chunk_idx < last_chunk) {
        chunk_u8_idx = chunk_idx / 8;
        bit_idx = chunk_idx % 8;
        loop_cnt++;
        if ((p[chunk_u8_idx] & (1 << bit_idx)) == 0) {
            /*
             ** that bit is not free, so restart from the begin (relative)
             */
            start_idx = -1;
            count_idx = 0;
            /*
             ** check if the index matches with chunk_u64_idx+1
             ** in that case we exit the while loop if no free bit has not yet been found
             */
            if (chunk_idx % 8 == 0) {
                next_chunk_idx = check_bytes_val(p, chunk_idx, last_chunk,
                        &loop_cnt, 0);
                if (next_chunk_idx < 0)
                    break;
                /*
                 ** next  chunk
                 */
                if (next_chunk_idx == chunk_idx)
                    chunk_idx++;
                else
                    chunk_idx = next_chunk_idx;
                continue;
            }
            /*
             ** next chunk
             */
            chunk_idx++;
            continue;

        }
        /*
         ** the bit is free, so check if is the fist one or the next in the list
         ** of the requested consecutive chunks
         */
        if (start_idx == -1) {
            /**
             * check if it remains enough bits to proceed with the allocation
             */
            if (chunk_idx + nb_chunks > last_chunk) {
                /*
                 ** there is no enough bits to allocated
                 */
                break;
            }
            /*
             ** check now the alignment
             */
            {
                int first = chunk_idx & mask;
                int last = (chunk_idx + nb_chunks) & mask;
                if (first != last) {
                    /*
                     ** need to skip to the beginning of the next range
                     */
                    chunk_idx = last;
                    continue;
                }
            }
            start_idx = (int) chunk_idx;
            count_idx = 1;
        } else {
            count_idx += 1;
        }
        /*
         ** check if the requested number of chunks has been reached
         */
        if (count_idx < nb_chunks) {
            /*
             ** need more bits
             */
            chunk_idx++;
            continue;
        }
        /*
         ** all the needed chunks hasve been found, so exit from while loop
         */
        break;
    }
    /*
     ** check if the allocation succeeds
     */
    if (start_idx == -1) {
        /*
         ** that situation must not occur:
         */
//       printf("out of chunk loop_cnt %d\n",loop_cnt);
        return -1;
    }
    /*
     ** OK, now proceed with the reservation of the bits
     */
    for (k = start_idx; k < (start_idx + nb_chunks); k++) {
        chunk_u8_idx = k / 8;
        bit_idx = k % 8;
        (p[chunk_u8_idx] &= ~(1 << bit_idx));

    }
//    printf("loop_cnt %d\n",loop_cnt);
    return start_idx;

}

/** @ingroup DIRENT_BITMAP
 *  API to release a consecutive number of chunks

 @param start_chunk : index of the first chunk to release
 @param nb_chunks : number of requested chunks
 @param last_chunk : last chunk index
 @param p : pointer to the bitmap of the free chunks

 @retval -1 on error
 @retval 0 on success
 */

static inline int dirent_release_chunks(int start_chunk, uint8_t nb_chunks,
        uint8_t *p, int last_chunk) {
    int k;
    int chunk_u8_idx;
    int bit_idx;

    if ((start_chunk + nb_chunks) > last_chunk)
        return -1;
    /*
     ** OK, now proceed with the reservation of the bits
     */
    for (k = start_chunk; k < (start_chunk + nb_chunks); k++) {
        chunk_u8_idx = k / 8;
        bit_idx = k % 8;
        (p[chunk_u8_idx] |= (1 << bit_idx));
    }
    return 0;
}

/** @ingroup DIRENT_BITMAP
 *  set a bit in a bitmap

 @param start_chunk : index of the entry that must be set
 @param *p  : pointer to the bitmap array

 @retval none
 */
static inline void dirent_set_chunk_bit(int start_chunk, uint8_t *p) {
    (p[start_chunk / 8] |= (1 << start_chunk % 8));
}

/** @ingroup DIRENT_BITMAP
 *  clear a bit in a bitmap

 @param start_chunk : index of the entry that must be set
 @param *p  : pointer to the bitmap array

 @retval none
 */
static inline void dirent_clear_chunk_bit(int start_chunk, uint8_t *p) {
    (p[start_chunk / 8] &= ~(1 << start_chunk % 8));

}

/** @ingroup DIRENT_BITMAP
 * Check a bit in a bitmap

 @param start_chunk : index of the entry that must be set
 @param *p  : pointer to the bitmap array

 @retval 0 if bit is cleared
 @retval <>0 if bit is asserted
 */
static inline int dirent_test_chunk_bit(int start_chunk, uint8_t *p) {
    return ((p[start_chunk / 8] & (1 << start_chunk % 8)));

}

/**
 *  API to check a range of chunk bits
 The caller must provide the first chunk value that must be 8 bits aligned and
 the number of chunk bit to test that must also be 8 bits aligned

 @param start_chunk : index of the first chunk to release
 @param nb_chunks : number of requested chunks
 @param last_chunk : last chunk index
 @param p : pointer to the bitmap of the free chunks

 @retval -1 on error
 @retval 1 if all the range is empty
 @retval 0 if there is at least one chunk that is in used
 */

static inline int dirent_check_empty_chunks(int start_chunk, uint8_t nb_chunks,
        uint8_t *p, int last_chunk) {
    int i;
    int start_u8_chunk;
    int loop_cnt;

    if ((start_chunk + nb_chunks) > last_chunk)
        return -1;
    start_u8_chunk = start_chunk / 8;
    if (nb_chunks % 8)
        return -1;
    loop_cnt = nb_chunks / 8;
    if (nb_chunks % 8)
        return -1;
    /*
     ** OK, lets check if all of them are empty
     */
    for (i = 0; i < loop_cnt; i++) {
        if (p[start_u8_chunk + i] != 0xff)
            return 0;
    }
    return 1;
}

/**
 *  Generic API
 *  that function returns the pointer to the base pointer of an array
 *
 @param idx : absolute index of the hash entry
 @param nb_elem_per_array : number of element sper array
 @param max_idx : index max of the array of virtual pointer
 @param p: pointer to table of the hash entries virtual pointers
 @param idx_rel_p : pointer where the relative index within the array will be stored
 @param virt_ptr_p: pointer to an array with virtual pointer can be stored or NULL if not requested

 @revtal NULL: no memory allocated for the array
 @retval <>NULL pointer to the beginning of the memory array

 */
static inline void *dirent_cache_get_entry_base_ptr(mdirent_cache_ptr_t *p,
        uint16_t nb_elem_per_array, uint16_t max_idx, uint16_t idx,
        uint16_t *idx_rel_p, mdirent_cache_ptr_t **virt_p) {
    uint16_t idx_array;

    if (idx >= (max_idx * nb_elem_per_array))
        return NULL ;

    idx_array = idx / nb_elem_per_array;
    *idx_rel_p = idx % nb_elem_per_array;
    if (p[idx_array].s.val == 0)
        return NULL ;
    uint64_t val = (uint64_t) p[idx_array].s.val;
    if (virt_p != NULL )
        *virt_p = &p[idx_array];
    return (void*) PTR2INT val;
}

/**
 *  Generic API
 *  that function returns the pointer to the requested element referenced by idx
 *
 @param idx : absolute index of the hash entry
 @param nb_elem_per_array : number of element sper array
 @param max_idx : index max of the array of virtual pointer
 @param p: pointer to table of the hash entries virtual pointers
 @param element_size : size of an element of the memory array
 @param virt_ptr_p: pointer to an array with virtual pointer can be stored or NULL if not requested

 @revtal NULL: no memory allocated for the arry
 @retval <>NULL pointer to the beginning of the element in the memory array

 */
static inline void *dirent_cache_get_entry_ptr(mdirent_cache_ptr_t *p,
        uint16_t nb_elem_per_array, uint16_t max_idx, uint16_t element_size,
        uint16_t idx, mdirent_cache_ptr_t **virt_p) {
    uint8_t *elem_p;
    uint16_t rel_idx;

    elem_p = (uint8_t*) dirent_cache_get_entry_base_ptr(p, nb_elem_per_array,
            max_idx, idx, &rel_idx, virt_p);
    if (elem_p == (uint8_t*) NULL )
        return (void*) NULL ;
    elem_p += (element_size * rel_idx);
    return (void*) elem_p;
}

/**
 *  Generic API
 *  The purpose of that function is to allocate a memory array that will be used
 for storing element of the given type

 Here it is assumed that the caller has checked that the array was not allocated
 If the allocation is successfull, the function return the physical pointer
 associated with the object referenced by its index.
 *  that function returns the pointer to the requested element referenced by idx

 @param idx : absolute index of the object that for which a memory array needs to be created
 @param nb_elem_per_array : number of element per array
 @param max_idx : index max of the array of virtual pointer
 @param p: pointer to table of the hash entries virtual pointers
 @param element_size : size of an element of the memory array

 @revtal NULL: no memory allocated for the arry
 @retval <>NULL pointer to the beginning of the element in the memory array

 */
static inline void *dirent_cache_allocate_entry_array(mdirent_cache_ptr_t *p,
        uint16_t nb_elem_per_array, uint16_t max_idx, uint16_t element_size,
        uint16_t idx) {
    uint8_t *elem_p;
    uint16_t rel_idx;
    uint16_t idx_array;
    uint64_t val;

    elem_p = (uint8_t*) dirent_cache_get_entry_base_ptr(p, nb_elem_per_array,
            max_idx, idx, &rel_idx, NULL );
    if (elem_p == (uint8_t*) NULL ) {
        /*
         ** allocate the memory array
         */
        elem_p = DIRENT_MALLOC(element_size*nb_elem_per_array)
        ;
        if (elem_p == NULL ) {
            /*
             ** out of memory
             */
            return NULL ;
        }
#if 1
        /*
         ** do we really need a memset to 0 since all is driven by the presence
         ** bit of the associated bitmap
         */
        memset(elem_p, 0, element_size * nb_elem_per_array);
#endif
        val = (uint64_t) (uintptr_t) elem_p;
        idx_array = idx / nb_elem_per_array;
        rel_idx = idx % nb_elem_per_array;
        p[idx_array].s.val = val;
    }
    elem_p += (element_size * rel_idx);
    return (void*) elem_p;
}

/**
 *  Generic API
 *  The purpose of that function is to release a memory array that will be used
 for storing element of the given type

 Here it is assumed that the caller has checked that the array was not allocated
 If the allocation is successfull, the function return the physical pointer
 associated with the object referenced by its index.
 *  that function returns the pointer to the requested element referenced by idx

 @param idx : absolute index of the object that for which a memory array needs to be released
 @param bitmap_p: pointer to a bitmap (a 1 indicates the element is free) or NULL if no bitmap available
 @param nb_elem_per_array : number of element per array
 @param max_idx : index max of the array of virtual pointer
 @param p: pointer to table of the hash entries virtual pointers
 @param element_size : size of an element of the memory array

 @revtal NULL: no memory allocated for the arry
 @retval <>NULL pointer to the beginning of the element in the memory array

 */
static inline int dirent_cache_release_entry_array(mdirent_cache_ptr_t *p,
        uint64_t *bitmap_p, uint16_t nb_elem_per_array, uint16_t max_idx,
        uint16_t element_size, uint16_t idx) {
    uint8_t *elem_p;
    uint16_t rel_idx;
    uint16_t idx_array;
    uint64_t val;
    int i;
    /*
     ** When there is a bitmap : all the work relies on the bitmap
     */
    if ((bitmap_p != NULL )&& (nb_elem_per_array%64 == 0)){
    int count = nb_elem_per_array/64;
    val = 0;
    idx_array = idx/nb_elem_per_array;
    val =~val;

    for (i = 0; i < count; i++)
    {
        if (bitmap_p[i+idx_array] != val) return 0;
    }
    /*
     ** all the elements have been released so release the memory array
     */
    if (p[idx_array].s.val == 0)
    {
        /*
         ** That case must not occur
         */
        return -1;
    }
    /*
     ** ned to release the memory block
     */
    val = p[idx_array].s.val;
    DIRENT_FREE((void*) PTR2INT val);
    p[idx_array].s.val = 0;
    /*
     ** indicates that the memory array must be re-written on disk with the default value for the array
     */
    p[idx_array].s.dirty = 1;
    p[idx_array].s.rd = 0;

    return 0;
}
    /*
     ** Case where there is no bitmap
     */
    elem_p = (uint8_t*) dirent_cache_get_entry_base_ptr(p, nb_elem_per_array,
            max_idx, idx, &rel_idx, NULL );
    if (elem_p == (uint8_t*) NULL ) {
        /*
         ** Abnormal case
         */
        return -1;

    }
    /*
     ** clear the corresponding entry
     */
    memset(&elem_p[element_size * rel_idx], 0, element_size);
    /*
     ** Now check if the array is empty
     */
    for (i = 0; i < nb_elem_per_array * element_size; i++) {
        if (elem_p[i] != 0)
            return 0;
    }
    /*
     ** release the memory block
     */
    DIRENT_FREE(elem_p);
    /*
     ** clear the virtual pointer
     */
    idx_array = idx / nb_elem_per_array;
    p[idx_array].s.val = 0;
    /*
     ** indicates that the memory array must be re-written on disk with the default value for the array
     */
    p[idx_array].s.dirty = 1;
    p[idx_array].s.rd = 0;

    return 0;
}
/**
 *___________________________________________________________________________

 API related to hash table buckets (hash_tbl_p)
 *___________________________________________________________________________
 */
/**
 *  Macro to get the pointer to the bucket associated with an index
 *  @param p : pointer to the begining of the dirent cache entry
 *  @param idx : index of the entry to search for/
 */
#define DIRENT_CACHE_GET_BUCKET_PTR(p,idx) dirent_cache_get_entry_ptr(p->hash_tbl_p, \
                                             MDIRENTS_HASH_TB_CACHE_MAX_ENTRY,\
                                             MDIRENTS_HASH_TB_CACHE_MAX_IDX,\
                                             sizeof(mdirents_hash_ptr_t), \
                                             idx,NULL);

#define DIRENT_CACHE_GET_BUCKET_PTR_WITH_VIRT(p,idx,virt_p) dirent_cache_get_entry_ptr(p->hash_tbl_p, \
                                             MDIRENTS_HASH_TB_CACHE_MAX_ENTRY,\
                                             MDIRENTS_HASH_TB_CACHE_MAX_IDX,\
                                             sizeof(mdirents_hash_ptr_t), \
                                             idx,virt_p);
/**
 *  Macro to allocate in memory an array for storing information related to an object
 *  If there is already an allocated memory array, it just returns the pointer to entry associated with idx
 */
#define DIRENT_CACHE_ALLOCATE_BUCKET_ARRAY(p,idx) dirent_cache_allocate_entry_array(p->hash_tbl_p, \
                                             MDIRENTS_HASH_TB_CACHE_MAX_ENTRY, /* max elements within the memory array*/\
                                             MDIRENTS_HASH_TB_CACHE_MAX_IDX,  /* max number of memory array for the element type*/\
                                             sizeof(mdirents_hash_ptr_t), /* size of one element of the array*/ \
                                             idx);

/**
 *  API to release all the memory blocks that have been allocated for storing
 the hash bucket entries

 @param  dirent_entry_p: pointer to the dirent cache entry for which hash entries memory must be released

 @retval 0 on success
 @retval -1 on error
 */
static inline void dirent_cache_release_all_hash_tbl_memory(
        mdirents_cache_entry_t *dirent_entry_p) {
    mdirent_cache_ptr_t *hash_entry_virt_mem_p = &dirent_entry_p->hash_tbl_p[0];
    uint8_t *mem_p;
    int i;

    for (i = 0; i < MDIRENTS_HASH_TB_CACHE_MAX_IDX; i++) {

        mem_p = DIRENT_VIRT_TO_PHY(hash_entry_virt_mem_p[i])
        ;
        if (mem_p == NULL )
            continue;
        DIRENT_FREE(mem_p);
        DIRENT_CLEAR_VIRT_PTR(hash_entry_virt_mem_p[i]);

    }
}

/**
 *___________________________________________________________________________

 GENERIC API related to indirect cache pointer store/get and deletion
 *___________________________________________________________________________
 */
#define MDIRENT_INDIRECT_DEPTH_MAX 8  /**< max number of indirection   */
typedef struct _mdirent_indirect_ptr_template_t {
    uint32_t last_idx; /**< index of the last valid entry    */
    uint32_t entry_size; /**< size of the element in bytes */
    uint8_t levels; /**< number of indirections            */
    uint32_t level[MDIRENT_INDIRECT_DEPTH_MAX]; /**< number of virtual pointer per level  */
} mdirent_indirect_ptr_template_t;
/*
 **______________________________________________________________________________
 */
/**
 *  That service provides the set of index within the cache to access to
 the pointer of an element for which indirect tables are used
 The maximum number of supported indirection 8 (see.MDIRENT_INDIRECT_DEPTH_MAX)

 The caller is expected to allocate the right table size for the returned table

 @param  idx : local index for with indirect indexes are request
 @param  *idx_tab_res_p: pointer to the table in with the function will store the indexes
 @param  *p : template associated to the memory representationthe

 @retwal < 0 : the index is out of range
 @retval = 0 : idx_tab_res_p contains the different indexes
 */

static inline int dirent_get_cache_indexes(uint32_t idx, uint8_t *idx_tab_res_p,
        mdirent_indirect_ptr_template_t *p) {
    int output_idx = 0;
    uint32_t shift;
    uint32_t val;
    int i;

    if (idx > (p->last_idx - 1))
        return -1;
    shift = 0;
    for (i = 0; i < p->levels; i++)
        shift += p->level[i];
    for (output_idx = 0; output_idx < p->levels; output_idx++) {
        val = idx;
        shift -= p->level[output_idx];
        val = val >> shift;
        val &= ((1 << p->level[output_idx]) - 1);
        idx_tab_res_p[output_idx] = val;
    }
    return 0;
}

/*
 **______________________________________________________________________________
 */
/**
 *  Generic API to store a pointer to a data structure by using an index associated with that pointer
 The index can be see has the primary key of that data structure to store

 @param idx : index of the object to store
 @param *ptr : pointer of the object to store
 @param *virt_head_p : pointer to the head of virtual pointers (level0)
 @param *indirection_template_p : pointer to the structure that description the indirection levels

 @retval NULL on success
 @retval <>NULL -> if the value is ptr, it indicates that either the idx is out of range or there was no enough memory
 @retval <>NULL -> if the value is different from ptr, ptr has been stored  but the entry was not entry. The
 returned value is the pointer to the old entry.
 */
static inline void *dirent_cache_store_ptr(mdirent_cache_ptr_t *virt_head_p,
        mdirent_indirect_ptr_template_t *indirection_template_p, int idx,
        void *ptr) {
    int res;
    uint8_t tab_idx[MDIRENT_INDIRECT_DEPTH_MAX];
    mdirent_cache_ptr_t *p = NULL;
    int nb_levels;
    uint64_t val;
    uint64_t old_val;
    int i;

    nb_levels = indirection_template_p->levels;
    /*
     ** get the reference of the collision idx to setup up on the parent side
     */
    res = dirent_get_cache_indexes(idx, tab_idx, indirection_template_p);
    if (res == -1) {
        /*
         ** the index is out of range
         */
	errno = EPROTO;
        return ptr;
    }
    p = virt_head_p;

    for (i = 0; i < nb_levels - 1; i++) {
        /*
         ** get the base pointer
         */
        if (p[tab_idx[i]].s.val == 0) {
            /*
             ** need to allocated a memory chunk for array
             */
            mdirent_cache_ptr_t *ptr;
            int size = (1 << indirection_template_p->level[i + 1])
                    * sizeof(mdirent_cache_ptr_t);
            ptr = (mdirent_cache_ptr_t*) DIRENT_MALLOC(size)
            ;
            if (ptr == (mdirent_cache_ptr_t*) NULL ) {
                /*
                 ** out of memory
                 */
		errno = ENOMEM; 
                return ptr;
            }
            memset(ptr, 0, size);
            val = (uint64_t) (uintptr_t) ptr;
            p[tab_idx[i]].s.val = val;
        }
        /*
         ** OK, let's go to the next array
         */
        val = (uint64_t) p[tab_idx[i]].s.val;
        p = (mdirent_cache_ptr_t*) PTR2INT val;
    }
    /*
     ** OK now store the logical pointer associated with the dirent entry
     ** if the entry was not empty, return the pointer that was stored
     */
    val = (uint64_t) (uintptr_t) ptr;
    old_val = p[tab_idx[nb_levels - 1]].s.val;
    p[tab_idx[nb_levels - 1]].s.val = val;
    /*
     ** need to write on disk the new array
     */
    p[tab_idx[nb_levels - 1]].s.dirty = 1;
    p[tab_idx[nb_levels - 1]].s.rd = 0;
    return (void *) PTR2INT old_val;
}
/*
 **______________________________________________________________________________
 */
/**
 *  API to get the pointer to an object that is referenced by its index (idx)
 *
 @param idx : index of the object to get
 @param *virt_head_p : pointer to the head of virtual pointers (level0)
 @param *indirection_template_p : pointer to the structure that description the indirection levels

 @retval NULL associated dirent cache entry does not exist
 @retval <> NULL pointer to object associated with idx
 */
static inline void *dirent_cache_get_ptr(mdirent_cache_ptr_t *virt_head_p,
        mdirent_indirect_ptr_template_t *indirection_template_p, int idx) {
    int res;
    uint8_t tab_idx[MDIRENT_INDIRECT_DEPTH_MAX];
    mdirent_cache_ptr_t *p = NULL;
    uint64_t val;
    int i;
    int nb_levels;

    nb_levels = indirection_template_p->levels;

    /*
     ** get the reference of the collision idx to setup up on the parent side
     */
    res = dirent_get_cache_indexes(idx, tab_idx, indirection_template_p);
    if (res == -1) {
        /*
         ** the index is out of range
         */
        return NULL ;
    }
    p = virt_head_p;

    for (i = 0; i < nb_levels - 1; i++) {
        /*
         ** get the base pointer
         */
        if (p[tab_idx[i]].s.val == 0) {
            /*
             ** the dirent collision file is not in the cache
             */
            return NULL ;
        }
        /*
         ** OK, let's go to the next array
         */
        val = (uint64_t) p[tab_idx[i]].s.val;
        p = (mdirent_cache_ptr_t*) PTR2INT val;
    }
    /*
     ** OK now get  the logical pointer associated with the dirent entry
     */
    val = p[tab_idx[nb_levels - 1]].s.val;

    return (void*) PTR2INT val;
}

/*
 **______________________________________________________________________________
 */
/**
 *  API to get the virtual pointer to an object that is referenced by its index (idx)
 *
 @param idx : index of the object to get
 @param *virt_head_p : pointer to the head of virtual pointers (level0)
 @param *indirection_template_p : pointer to the structure that description the indirection levels

 @retval value of the virtual pointer, the caller has to check val value and dirty/rd bits to figure out if it is valid
 @retval <> NULL pointer to object associated with idx
 */
static inline mdirent_cache_ptr_t dirent_cache_get_virtual_ptr(
        mdirent_cache_ptr_t *virt_head_p,
        mdirent_indirect_ptr_template_t *indirection_template_p, int idx) {
    int res;
    uint8_t tab_idx[MDIRENT_INDIRECT_DEPTH_MAX];
    mdirent_cache_ptr_t *p = NULL;
    mdirent_cache_ptr_t val_ret;
    uint64_t val;
    int i;
    int nb_levels;

    val_ret.u64 = 0;

    nb_levels = indirection_template_p->levels;

    /*
     ** get the reference of the collision idx to setup up on the parent side
     */
    res = dirent_get_cache_indexes(idx, tab_idx, indirection_template_p);
    if (res == -1) {
        /*
         ** the index is out of range
         */
        return val_ret;
    }
    p = virt_head_p;

    for (i = 0; i < nb_levels - 1; i++) {
        /*
         ** get the base pointer
         */
        if (p[tab_idx[i]].s.val == 0) {
            /*
             ** the dirent collision file is not in the cache
             */
            return val_ret;
        }
        /*
         ** OK, let's go to the next array
         */
        val = (uint64_t) p[tab_idx[i]].s.val;
        p = (mdirent_cache_ptr_t*) PTR2INT val;
    }
    /*
     ** OK now get  the logical pointer associated with the dirent entry
     */
    return (p[tab_idx[nb_levels - 1]]);

}

/*
 **______________________________________________________________________________
 */
/**
 *  API to remove a pointer from an indirection table by using the index of the object
 The value of the pointer is just there for controlling that the pointer to removed is
 the same as the one that will be found in the entry
 *

 @param idx : index of the object to delete
 @param *ptr : pointer of the object to delete (not used ?)
 @param *virt_head_p : pointer to the head of virtual pointers (level0)
 @param *indirection_template_p : pointer to the structure that description the indirection levels

 @retval NULL on success
 @retval <>NULL on error : value of the input pointer if the not reference in the indirection array, or value of
 the current entry if the entry does not match with the input pointer (ptr)
 */

static inline void *dirent_cache_del_ptr(mdirent_cache_ptr_t *virt_head_p,
        mdirent_indirect_ptr_template_t *indirection_template_p, int idx,
        void *ptr) {
    int res;
    uint8_t tab_idx[MDIRENT_INDIRECT_DEPTH_MAX];
    mdirent_cache_ptr_t *ptr_tab[MDIRENT_INDIRECT_DEPTH_MAX];
    int nb_levels;
    int i;
    mdirent_cache_ptr_t *p = NULL;
    uint64_t val;

    nb_levels = indirection_template_p->levels;

    /*
     ** get the reference of the collision idx to setup up on the parent side
     */
    res = dirent_get_cache_indexes(idx, tab_idx, indirection_template_p);
    if (res == -1) {
        /*
         ** the index is out of range
         */
        return ptr;
    }
    p = virt_head_p;

    for (i = 0; i < nb_levels - 1; i++) {
        /*
         ** get the base pointer
         */
        ptr_tab[i] = p;
        if (p[tab_idx[i]].s.val == 0) {
            /*
             **  Not in cache
             */
            return ptr;
        }
        /*
         ** OK, let's go to the next array
         */
        val = (uint64_t) p[tab_idx[i]].s.val;
        p = (mdirent_cache_ptr_t*) PTR2INT val;
    }
    /*
     ** OK now remove the entry
     */
    val = (uint64_t) (uintptr_t) ptr;
    if (val != p[tab_idx[nb_levels - 1]].s.val)
#if 1
            {
        /*
         ** not the right one
         */
        val = p[tab_idx[nb_levels - 1]].s.val;
        return (void*) PTR2INT val;

    }
#endif
    /*
     ** clear the entry
     */
    p[tab_idx[nb_levels - 1]].s.val = 0;
    /*
     ** indicates that the array must be re-write on disk with its associated default value
     */
    p[tab_idx[nb_levels - 1]].s.dirty = 1;
    p[tab_idx[nb_levels - 1]].s.rd = 0;
    /*
     ** check if the array is empty and release it if it the case
     */
    for (i = 0; i < nb_levels - 1; i++) {
        int empty;
        int level;
        int k;

        empty = 1;
        level = indirection_template_p->level[nb_levels - 1 - i];
        for (k = 0; k < (1 << level); k++) {
            if (p[k].s.val != 0) {
                empty = 0;
                break;
            }
        }
        if (!empty)
            break;
        /*
         ** all the array is empty, so release it and clear its associated pointer on
         **  the parent array
         */
        DIRENT_FREE(p);
        p = ptr_tab[nb_levels - 2 - i];
        /*
         ** clear the entry on the parent
         */
        p[tab_idx[nb_levels - 2 - i]].s.val = 0;

    }
    /*
     ** all is fine
     */
    return NULL ;
}

/**
 *___________________________________________________________________________

 API related to name entry management
 *___________________________________________________________________________
 */

typedef struct _name_entry_cache_t {
    uint8_t idx0; /**< pointer index of level 0  */
    uint8_t idx1; /**< pointer index within the 16 pointers of the level 0 pointer array */
    uint16_t byte_offset; /**< offset of the byte within the 1024 byte array  */
    uint16_t len; /**< len within the array */

} name_entry_cache_t;

extern mdirent_indirect_ptr_template_t mdirent_cache_name_ptr_distrib;

extern int dirent_read_name_array_from_disk(int dirfd,
        mdirents_cache_entry_t *dirent_p, int first_chunk_of_array);
#if 0
/*
 **______________________________________________________________________________
 */
/**
 * Read the sectors associated with a chunk array from disk
 *
 * @param dirfd: file descriptor of the parent directory
 * @param *pathname: pointer to the pathname to read
 *
 * @retval 0 success
 * @retval -1 error
 */

static inline int dirent_read_name_array_from_disk(int dirfd,
        mdirents_cache_entry_t *dirent_p,
        int first_chunk_of_array)

{
    int fd = -1;
    int flag = O_RDONLY;
    mdirents_header_new_t *dirent_hdr_p = &dirent_p->header;
    char pathname[64];
    char *path_p;
    off_t offset;
    uint8_t *mem_p;

    /*
     ** build the filename of the dirent file to read
     */
    path_p = dirent_build_filename(dirent_hdr_p,pathname);
    if (path_p == NULL)
    {
        /*
         ** something wrong that must not happen
         */
        DIRENT_SEVERE("Cannot build pathname line %d\n",__LINE__);
        goto error;
    }
    /*
     ** OK, let's allocate memory for storing the name entries into chunks
     */
    mem_p = DIRENT_MALLOC(MDIRENTS_CACHE_CHUNK_ARRAY_SZ);
    if (mem_p == NULL)
    {
        /*
         ** fatal error we run out of memory
         */
        DIRENT_SEVERE("dirent_get_entry_name_ptr: out of memory at line %d\n",__LINE__);
        goto error;
    }
    if (dirent_cache_store_ptr(&dirent_p->name_entry_lvl0_p[0],
                    &mdirent_cache_name_ptr_distrib,
                    first_chunk_of_array,(void *)mem_p) != NULL)
    {
        /*
         ** fatal error
         */
        DIRENT_SEVERE("dirent_get_entry_name_ptr: out of memory at line %d\n",__LINE__);
        goto error;
    }

    if ((fd = openat(dirfd, path_p, flag, S_IRWXU)) == -1)
    {
        DIRENT_SEVERE("Cannot open the file %s with fd %d, error %s at line %d\n",path_p,dirfd,strerror(errno),__LINE__);
        goto error;
    }
    /*
     ** read the fixed part of the dirent file
     */
    offset = DIRENT_HASH_NAME_BASE_SECTOR*MDIRENT_SECTOR_SIZE
    + MDIRENTS_CACHE_CHUNK_ARRAY_SZ*(first_chunk_of_array);

    if (DIRENT_PREAD(fd, mem_p, MDIRENTS_CACHE_CHUNK_ARRAY_SZ, offset) != MDIRENTS_CACHE_CHUNK_ARRAY_SZ) {
        DIRENT_SEVERE("pread failed in file %s: %s", pathname, strerror(errno));

        goto error;
    }
    /*
     ** indicate that the memory array exists
     */
//    printf("dirent_set_chunk_bit first_chunk_of_array %d (bit %d)\n",
//    first_chunk_of_array,first_chunk_of_array/MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY);
    dirent_set_chunk_bit(first_chunk_of_array/MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY,
            dirent_p->name_entry_array_btmap_presence);

    /*
     ** that's OK
     */
    if (fd != -1)
    close(fd);
    return 0;

    error:
    if (fd != -1) close(fd);
    return -1;
}

#endif

#if 0 // obsolete
/**
 *  API to get the reference of the memory array where a entry name can be found
 */
static inline int dirent_get_entry_name_ptr(int start_chunk,uint8_t nb_chunks,name_entry_cache_t *result)
{
    int remain;
    int bit_offset;
    int first_level0_idx;
    int last_level0_idx;

    result[0].len = 0;
    result[1].len = 0;
    /*
     ** first of all, need to get the index relative to the indirection array knowning that
     ** the memory allocated for storing chunks can contains up to MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY
     */
    first_level0_idx = start_chunk/MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY;
    last_level0_idx = (start_chunk+nb_chunks)/MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY;

    result[0].idx0 = first_level0_idx/MDIRENTS_NAME_PTR_LVL1_NB_PTR;
    result[1].idx0 = last_level0_idx/MDIRENTS_NAME_PTR_LVL1_NB_PTR;
    result[0].idx1 = first_level0_idx%MDIRENTS_NAME_PTR_LVL1_NB_PTR;
    result[1].idx1 = last_level0_idx%MDIRENTS_NAME_PTR_LVL1_NB_PTR;
    bit_offset = start_chunk%MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY;
    result[0].byte_offset = bit_offset*MDIRENTS_NAME_CHUNK_SZ;
    result[1].byte_offset = 0;
    if ((bit_offset+nb_chunks)> MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY)
    {
        remain = (bit_offset+nb_chunks)- MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY;
        result[0].len = (nb_chunks- remain)*MDIRENTS_NAME_CHUNK_SZ;
        result[1].len = remain*MDIRENTS_NAME_CHUNK_SZ;
        return 0;
    }
    result[0].len = nb_chunks*MDIRENTS_NAME_CHUNK_SZ;
    return 0;

}
#endif
/*
 **______________________________________________________________________________
 */
/**
 *  API to release all the blocks used for storing name/fid/type of a file or directory
 *
 @param dirent_entry_p: pointer to the dirent cache entry

 retval -1 on error
 retval 0 on success
 */
static inline void dirent_cache_release_name_all_memory(
        mdirents_cache_entry_t *dirent_entry_p) {

    mdirent_indirect_ptr_template_t *p = &mdirent_cache_name_ptr_distrib;
    int i, k;
    mdirent_cache_ptr_t *cache_entry_p;
    mdirent_cache_ptr_t *cache_lvl1;
    mdirent_cache_ptr_t *cache_lvl0 = &dirent_entry_p->name_entry_lvl0_p[0];

    for (i = 0; i < (1 << p->level[0]); i++) {
        cache_lvl1 = DIRENT_VIRT_TO_PHY(cache_lvl0[i])
        ;
        if (cache_lvl1 == NULL )
            continue;
        for (k = 0; k < (1 << p->level[1]); k++) {
            cache_entry_p = DIRENT_VIRT_TO_PHY(cache_lvl1[k])
            ;
            if (cache_entry_p == NULL )
                continue;
            /*
             ** release the context
             */
            DIRENT_FREE(cache_entry_p);
        }
        /*
         ** release the level 1 array
         */
        DIRENT_FREE(cache_lvl1);
        DIRENT_CLEAR_VIRT_PTR(cache_lvl0[i]);
    }
}
/*
 **______________________________________________________________________________
 */
/**
 *   API to allocate consecutive chunks for storing a name entry structure

 @param  p : pointer to the name entry bitmap
 @param  nb_chunks : number of chunk to allocate

 @retval !=-1 -> reference of the first index that has been allocated
 @retval =-1 -> allocation failure
 */
#define DIRENT_CACHE_ALLOCATE_NAME_ENTRY_CHUNKS(p,nb_chunks) dirent_allocate_chunks_with_alignment(nb_chunks,MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY,\
                                                                                                  (uint8_t*)p,MDIRENTS_NAME_CHUNK_MAX_CNT);
/*
 **______________________________________________________________________________
 */
/**
 *   API to release consecutive chunks for storing a name entry structure

 @param  p : pointer to the name entry bitmap
 @param  chunk : first chunk to release
 @param  nb_chunks : number of chunk to allocate

 @retval -1 on error
 @retval 0 on success
 */
#define DIRENT_CACHE_RELEASE_NAME_ENTRY_CHUNKS(p,chunk,nb_chunks) dirent_release_chunks(chunk,nb_chunks,(uint8_t*)p,MDIRENTS_NAME_CHUNK_MAX_CNT);
/*
 **______________________________________________________________________________
 */
/**
 *  API to get the pointer to the beginning a an entry name in the cache. If there is no memory yet allocated,
 *  the caller can request to allocate the memory by asserting the alloc_req flag in the input argument

 @param  dir_fd : file descriptor of the parent directory
 @param  dirent_entry_p : poiinter to the current dirent cache entry
 @param  start_chunk_idx : index of the first chunk associated with tha entry name (notice that a name is not split between 2 memory arrays)
 @param  alloc_req : allocate an memory array for storing chunk if alloc_req is 1

 */
#define DIRENT_CHUNK_ALLOC 1  // indicate that memory needs to be allocated if the memory array has not yet been created
#define DIRENT_CHUNK_NO_ALLOC 0  // no allocation, just for lookup purpose
static inline void *dirent_get_entry_name_ptr(int dir_fd,
        mdirents_cache_entry_t *dirent_entry_p, uint32_t start_chunk_idx,
        int alloc_req) {
    int ret;
    uint32_t first_chunk_of_array = start_chunk_idx
            / MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY;
    uint32_t chunk_offset = start_chunk_idx
            - first_chunk_of_array * MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY;
    uint8_t *mem_p;

    /*
     ** attempt to get a valid pointer from memory
     */
    mem_p = (uint8_t *) dirent_cache_get_ptr(
            &dirent_entry_p->name_entry_lvl0_p[0],
            &mdirent_cache_name_ptr_distrib, first_chunk_of_array);
    if (mem_p != NULL ) {
        /*
         ** adjust the pointer to point out the memory array starting at start_chunk_idx
         */

        return (void*) (&mem_p[chunk_offset * MDIRENTS_NAME_CHUNK_SZ]);
    }
    /*
     ** the return value is null, so either it is the first time that we store a name in that memory array, or
     ** it exists but it has not been yet loaded from disk
     */

    ret = dirent_test_chunk_bit(first_chunk_of_array,
            dirent_entry_p->name_entry_array_btmap_presence);
    if (ret != 0) {
        /*
         ** here is the situation where the name entry array has to be read from disk
         */

        ret = dirent_read_name_array_from_disk(dir_fd, dirent_entry_p,
                first_chunk_of_array);
        if (ret == -1) {
            /*
             ** something wrong while attempting to read the file: either we run out of memory of the file does not exist
             */
            return (mdirents_cache_entry_t*) NULL ;
        }
        /*
         ** the file has been loaded in memory and the virtual pointer has been updated in the parent cache entry, just need to get it
         */
        mem_p = (uint8_t*) dirent_cache_get_ptr(
                &dirent_entry_p->name_entry_lvl0_p[0],
                &mdirent_cache_name_ptr_distrib, first_chunk_of_array);

        if (mem_p != NULL ) {
            /*
             ** adjust the pointer to point out the memory array starting at start_chunk_idx
             */
            return (void*) (&mem_p[chunk_offset * MDIRENTS_NAME_CHUNK_SZ]);
        }
        /**
         ** something wrong
         */DIRENT_SEVERE("dirent_get_entry_name_ptr at line %d\n",__LINE__)
        ;
        return NULL ;
    }
    /*
     **  There is no presence of that chunk array neither on disk nor memory
     **  if alloc_req is asserted we allocate one memory array entry for storing name entry into the chunks
     **  if alloc_req is not asserted, this must be considered as a error
     */
    if (alloc_req == 0) {
        /*
         ** there no name entry associated with the hash entry--> error
         */
        DIRENT_SEVERE("dirent_get_entry_name_ptr at line %d\n",__LINE__)
        ;
        return NULL ;
    }
    /*
     ** OK, let's allocate memory for storing the name entries into chunks
     */
    mem_p = DIRENT_MALLOC(MDIRENTS_CACHE_CHUNK_ARRAY_SZ)
    ;
    if (mem_p == NULL ) {
        /*
         ** fatal error we run out of memory
         */
        DIRENT_SEVERE("dirent_get_entry_name_ptr: out of memory at line %d\n",__LINE__)
        ;
        return NULL ;
    }
    if (dirent_cache_store_ptr(&dirent_entry_p->name_entry_lvl0_p[0],
            &mdirent_cache_name_ptr_distrib, first_chunk_of_array,
            (void *) mem_p) != NULL ) {
        /*
         ** fatal error
         */
        DIRENT_SEVERE("dirent_get_entry_name_ptr: out of memory at line %d\n",__LINE__)
        ;
        return NULL ;
    }
    /*
     ** all is fine, return the pointer to the index of start_index
     */
    return (void*) (&mem_p[chunk_offset * MDIRENTS_NAME_CHUNK_SZ]);

}

/*
 **______________________________________________________________________________
 */
/**
 *  API to insert a name entry in a dirent cache entry:
 That function:
 - allocate the needed chunk
 - allocated memory for chunk if needed
 - update the hash entry with the reference of the first chunk and the number of allocated chunks,

 @param  dir_fd : file descriptor of the parent directory (needed if disk read is need in case of cache miss)
 @param  dirent_entry_p : poiinter to the current dirent cache entry
 @param  name_entry_p : pointer to the entry to store
 @param  hash_entry_p : pointer to the hash entry associated with the name_entry

 @param <>NULL-> success, pointer to the name_entry image in cache
 @param NULL-> error (out of memory, inconsistent ptr...

 */

static inline void *dirent_create_entry_name(int dir_fd,
        mdirents_cache_entry_t *dirent_entry_p,
        mdirents_name_entry_t *name_entry_p,
        mdirents_hash_entry_t *hash_entry_p) {
    uint32_t *dest_mem_p;
    uint32_t *src_mem_p = (uint32_t*) name_entry_p;
    uint32_t first_chunk_of_array;
    uint8_t *mem_p;
    uint32_t entry_size;
    int nb_chunks;
    int start_chunk;
    int i;

    mem_p = DIRENT_VIRT_TO_PHY(dirent_entry_p->name_bitmap_p)
    ;
    if (mem_p == NULL ) {
        /*
         ** something wrong in the cache entry context
         */
        DIRENT_SEVERE("dirent_create_entry_name error at line %d\n",__LINE__)
        ;
        return NULL ;
    }
    /*
     ** first of all allocate the request number of chunks
     */
    entry_size = sizeof(uint32_t) + sizeof(fid_t) + name_entry_p->len;
    nb_chunks = ((entry_size - 1) / MDIRENTS_NAME_CHUNK_SZ) + 1;
    start_chunk = DIRENT_CACHE_ALLOCATE_NAME_ENTRY_CHUNKS(mem_p,nb_chunks)
    ;
    if (start_chunk == -1) {
        /*
         ** out of chunk-> that situation MUST not occur
         */
        DIRENT_SEVERE("dirent_create_entry_name: out of chunk error at line %d\n",__LINE__)
        ;
        return NULL ;
    }
    /*
     ** OK, store the start chunk and the number of chunk in the hash entry
     */
    hash_entry_p->chunk_idx = start_chunk;
    hash_entry_p->nb_chunk = nb_chunks;
    /*
     ** Now, get the pointer to the memory array where the name entry will be copied
     */

    dest_mem_p = (uint32_t*) dirent_get_entry_name_ptr(dir_fd, dirent_entry_p,
            start_chunk, DIRENT_CHUNK_ALLOC);
    if (dest_mem_p == NULL ) {
        /*
         ** something wrong that must not occur
         */
        DIRENT_SEVERE("dirent_create_entry_name: out of chunk error at line %d\n",__LINE__)
        ;
        return NULL ;
    }
    /*
     ** copy the entry: always on 4 bytes alignment
     */
    for (i = 0; i < (((entry_size - 1) >> 2) + 1); i++)
        dest_mem_p[i] = *src_mem_p++;
    /*
     ** indicates that the memory array must be updated on disk
     */
    first_chunk_of_array =
            start_chunk / MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY;
    dirent_set_chunk_bit(first_chunk_of_array,
            dirent_entry_p->name_entry_array_btmap_wr_req);

    /*
     ** all is fine, return the pointer to the index of start_index
     */
    return (void*) dest_mem_p;

}

/*
 **______________________________________________________________________________
 */
/**
 *  API to release the chunks allocated for storing a name entry
 *  The memory allocated for storing the chunks is released if all the chunks relative to that memory array are free.
 *
 @param *dirent_entry_p : pointer to the dirent entry associated with dirent_coll_file_idx
 @param start_chunk_idx : index of the first chunk
 @param nb_chunks : number of chunk to release

 @retval 0 on success
 @retval -1 on error :
 */
static inline int dirent_cache_del_entry_name(
        mdirents_cache_entry_t *dirent_entry_p, uint32_t start_chunk_idx,
        uint32_t nb_chunks) {

    uint8_t *mem_p;
    //int ret;
    mdirent_cache_ptr_t virt_ptr;
    int first_chunk_of_array;
    uint64_t val;

    mem_p = DIRENT_VIRT_TO_PHY(dirent_entry_p->name_bitmap_p)
    ;
    if (mem_p == NULL ) {
        /*
         ** something wrong in the cache entry context
         */
        DIRENT_SEVERE("dirent_cache_del_entry_name_ptr error at line %d\n",__LINE__)
        ;
        return -1;
    }
    /*
     ** release the chunk allocated for the name entry
     */
    /*ret =*/
            DIRENT_CACHE_RELEASE_NAME_ENTRY_CHUNKS(mem_p,start_chunk_idx,nb_chunks)
    ;
    if (mem_p == NULL ) {
        /*
         ** something wrong in the cache entry context
         */
        DIRENT_SEVERE("dirent_cache_del_entry_name_ptr error at line %d\n",__LINE__)
        ;
        return -1;
    }
    /*
     ** need to update parent on disk
     */
    DIRENT_ROOT_UPDATE_REQ(dirent_entry_p);
    /*
     ** check if the memory allocated for storing the name entry can be released:
     ** for that we need to check that all the chunks associated with that array are free
     ** for this we need to get the reference of the first chunk associated with the memory
     *  array and then to check if the following chunk up the the max number of chunk per
     ** memoryt are free.
     */
    {

        first_chunk_of_array = start_chunk_idx
                / MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY;
        int i;

        for (i = 0; i < MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY / 8; i++) {
            if (mem_p[(first_chunk_of_array
                    * (MDIRENTS_CACHE_NB_CHUNK_PER_CHUNK_ARRAY / 8)) + i]
                    != 0xff) {
                /*
                 ** at least one chunk is still in use, so do not release the memory array
                 */
                return 0;
            }
        }
    }
    /*
     ** clear the bit associated to the memory array to avoid re-write on disk
     */
    dirent_clear_chunk_bit(first_chunk_of_array,
            dirent_entry_p->name_entry_array_btmap_presence);
    dirent_clear_chunk_bit(first_chunk_of_array,
            dirent_entry_p->name_entry_array_btmap_wr_req);

    /*
     ** OK, release it, but before we need to get the pointer to the beginning of the memory array
     ** because dirent_cache_del_ptr does not release it: it just clear the reference and eventually
     ** release the buffer that was storing the pointers to the name entries array
     */
    virt_ptr = dirent_cache_get_virtual_ptr(
            &dirent_entry_p->name_entry_lvl0_p[0],
            &mdirent_cache_name_ptr_distrib, first_chunk_of_array);
    if (virt_ptr.s.val == 0) {
        /*
         ** The pointer has already been released !!
         */
        DIRENT_SEVERE("dirent_cache_del_entry_name_ptr error at line %d\n",__LINE__)
        ;
        return 0;

    }
    val = virt_ptr.s.val;

    mem_p = (uint8_t*) dirent_cache_del_ptr(
            &dirent_entry_p->name_entry_lvl0_p[0],
            &mdirent_cache_name_ptr_distrib, first_chunk_of_array,
            (void *) PTR2INT val);
    if (mem_p != NULL ) {
        /*
         ** that case must not happen because we just get it before calling deletion
         */
        DIRENT_SEVERE("dirent_cache_del_entry_name_ptr error at line %d\n",__LINE__)
        ;
        return -1;

    }
    /*
     ** OK release the memory block
     */
    DIRENT_FREE((void*) PTR2INT val);
    return 0;

}

/**
 *___________________________________________________________________________

 API related to dirent cache entry
 *___________________________________________________________________________
 */

/**
 *   The purpose of that API is to check if the dirent cache entry is empty or no

 @param dirent_entry_p: pointer to the dirent cache entry

 @retval 1: the entry is empty
 @retval 0 : the entry is not empty
 @retval -1 : sector0 pointer does not exist
 */
static inline int dirent_cache_entry_check_empty(
        mdirents_cache_entry_t *dirent_entry_p) {
    mdirent_sector0_not_aligned_t *sect0_p;
    uint64_t *hash_bitmap_p;
    uint64_t *coll_bitmap_p;
    int i;
    /*
     ** There are 2 cases: root dirent cache entry and collision cache entry
     ** For the case of a collision cache entry the API checks that the bitmap of the hash_entries is empty
     ** For the case of the root cache entry, the API must also check that all the collision dirent files
     ** have been released-> the asooicated bitmap on the dirent root cache entry must be empty
     **
     ** It is assumed that the bitmap starts on a 8 bytes boundary to speed-up the control and also that
     ** that the bitmap  size is a multiple of 8 bytes
     */

    sect0_p = DIRENT_VIRT_TO_PHY_OFF(dirent_entry_p,sect0_p)
    ;
    if (sect0_p == (mdirent_sector0_not_aligned_t*) NULL ) {
        DIRENT_SEVERE("dirent_cache_entry_check_empty error at line %d\n",__LINE__)
        ;
        return -1;
    }
    uint64_t val = 0;
    val = ~val;
    hash_bitmap_p = (uint64_t*) &sect0_p->hash_bitmap;
    for (i = 0; i < MDIRENTS_ENTRIES_COUNT / 64; i++) {
        if (hash_bitmap_p[i] != val)
            return 0;
    }
    /*
     ** all the hash entries of the dirent cache entry have been release, check if it the root
     ** because we need also to check the collision bitmap
     */
    if (sect0_p->header.level_index == 0) {
        /*
         ** this the root file
         */
        coll_bitmap_p = (uint64_t*) &sect0_p->coll_bitmap;
        for (i = 0; i < MDIRENTS_MAX_COLLS_IDX / 64; i++) {
            if (coll_bitmap_p[i] != val)
                return 0;
        }
    }
    /*
     ** the dirent cache entry is empty (either root or collision) so it can be released
     */
    return 1;
}

/*
 **______________________________________________________________________________
 */
/**
 *   The purpose of that API is to check if the collision bitmap of the root is empty (DEBUG Function)

 @param dirent_entry_p: pointer to the dirent cache entry

 @retval 1: the entry is empty
 @retval 0 : the entry is not empty
 @retval -1 : sector0 pointer does not exist
 */
static inline int dirent_cache_check_coll_bitmap_empty(
        mdirents_cache_entry_t *root_p) {
    mdirent_sector0_not_aligned_t *sect0_p;
    uint64_t *coll_bitmap_p;
    int i;

    sect0_p = DIRENT_VIRT_TO_PHY_OFF(root_p,sect0_p)
    ;
    if (sect0_p == (mdirent_sector0_not_aligned_t*) NULL ) {
        DIRENT_SEVERE("dirent_cache_entry_check_empty error at line %d\n",__LINE__)
        ;
        return -1;
    }
    if (sect0_p->header.level_index != 0) {
        /*
         ** not the root entry
         */
        return -1;
    }

    uint64_t val = 0;
    val = ~val;
    /*
     ** this the root file
     */
    coll_bitmap_p = (uint64_t*) &sect0_p->coll_bitmap;
    for (i = 0; i < MDIRENTS_MAX_COLLS_IDX / 64; i++) {
        if (coll_bitmap_p[i] != val)
            return 0;
    }
    /*
     ** the dirent cache entry is empty (either root or collision) so it can be released
     */
    return 1;
}

/**
 *___________________________________________________________________________

 API RELATED TO HASH ENTRY MANAGEMENT
 *___________________________________________________________________________
 */

/*
 **______________________________________________________________________________
 */
/**
 *  Macro to get the pointer to the hash entry associated with an index
 *  @param p : pointer to the begining of the dirent cache entry
 *  @param idx : index of the entry to search for/
 */
#define DIRENT_CACHE_GET_HASH_ENTRY_PTR(p,idx) dirent_cache_get_entry_ptr(p->hash_entry_p, \
                                             MDIRENTS_HASH_CACHE_MAX_ENTRY,\
                                             MDIRENTS_HASH_CACHE_MAX_IDX,\
                                             sizeof(mdirents_hash_entry_t), \
                                             idx,NULL);

#define DIRENT_CACHE_GET_HASH_ENTRY_PTR_WITH_VIRT(p,idx,virt_p) dirent_cache_get_entry_ptr(p->hash_entry_p, \
                                             MDIRENTS_HASH_CACHE_MAX_ENTRY,\
                                             MDIRENTS_HASH_CACHE_MAX_IDX,\
                                             sizeof(mdirents_hash_entry_t), \
                                             idx,virt_p);
/*
 **______________________________________________________________________________
 */
/**
 *  Macro to allocate in memory an array for storing information related to an object
 *  If there is already an allocated memory array, it just returns the pointer to entry associated with idx
 */
#define DIRENT_CACHE_ALLOCATE_HASH_ENTRY_ARRAY(p,idx) dirent_cache_allocate_entry_array(p->hash_entry_p, \
                                             MDIRENTS_HASH_CACHE_MAX_ENTRY, /* max elements within the memory array*/\
                                             MDIRENTS_HASH_CACHE_MAX_IDX,  /* max number of memory array for the element type*/\
                                             sizeof(mdirents_hash_entry_t), /* size of one element of the array*/ \
                                             idx);
/*
 **______________________________________________________________________________
 */
/**
 *  Macro to release the memory associated with a hash entry
 */
#define DIRENT_CACHE_RELEASE_HASH_ENTRY_ARRAY(p,bitmap_p,idx) dirent_cache_release_entry_array(p->hash_entry_p, \
                                             (uint64_t*)bitmap_p,  /* pointer to the bitmap of the hash entries */ \
                                             MDIRENTS_HASH_CACHE_MAX_ENTRY, /* max elements within the memory array*/\
                                             MDIRENTS_HASH_CACHE_MAX_IDX,  /* max number of memory array for the element type*/\
                                             sizeof(mdirents_hash_entry_t), /* size of one element of the array*/ \
                                             idx);

#define DIRENT_CACHE_ALLOCATE_HASH_ENTRY_IDX(p) dirent_allocate_chunks(1,(uint8_t*)p,MDIRENTS_BITMAP_FREE_HASH_LAST_BIT_IDX);
/**
 * API to get the next allocated chunk index starting at a given index within the bitmap
 */
#define DIRENT_CACHE_GETNEXT_ALLOCATED_HASH_ENTRY_IDX(p,start) dirent_getnext_chunk(start,(uint8_t*)p,MDIRENTS_BITMAP_FREE_HASH_LAST_BIT_IDX,0);
/**
 * API to get the next free chunk index starting at a given index within the bitmap
 */
#define DIRENT_CACHE_GETNEXT_FREE_HASH_ENTRY_IDX(p,start) dirent_getnext_chunk(start,(uint8_t*)p,MDIRENTS_BITMAP_FREE_HASH_LAST_BIT_IDX,1);

#define DIRENT_CACHE_RELEASE_HASH_ENTRY_IDX(p,chunk) dirent_release_chunks(chunk,1,(uint8_t*)p,MDIRENTS_BITMAP_FREE_HASH_LAST_BIT_IDX);
/*
 **______________________________________________________________________________
 */
/**
 *  API to release all the memory blocks that have been allocated for storing
 the hash entries

 @param  dirent_entry_p: pointer to the dirent cache entry for which hash entries memory must be released

 @retval 0 on success
 @retval -1 on error
 */
static inline void dirent_cache_release_all_hash_entries_memory(
        mdirents_cache_entry_t *dirent_entry_p) {
    mdirent_cache_ptr_t *hash_entry_virt_mem_p =
            &dirent_entry_p->hash_entry_p[0];
    uint8_t *mem_p;
    int i;

    for (i = 0; i < MDIRENTS_HASH_CACHE_MAX_IDX; i++) {

        mem_p = DIRENT_VIRT_TO_PHY(hash_entry_virt_mem_p[i])
        ;
        if (mem_p == NULL )
            continue;
        DIRENT_FREE(mem_p);
        DIRENT_CLEAR_VIRT_PTR(hash_entry_virt_mem_p[i]);
    }
}
/*
 **______________________________________________________________________________
 */
/**
 *  API to create a dirent entry from scratch
 *
 @param dirent_hdr_p : pointer to the dirent header that contains its reference
 @param parent : fid of the parent directory (needed by writeback cache)

 @retval  <>NULL  pointer to the dirent cache entry
 @retval NULL : out of cache entries
 */
mdirents_cache_entry_t *dirent_cache_create_entry(
        mdirents_header_new_t *dirent_hdr_p,fid_t parent);

/*
 **______________________________________________________________________________
 */
/**
 *  API to allocate an entry in a dirent cache entry associated with the dirent file.

 hash_p:
 the type is set to EOF if no entry has been found:
 - idx and level are not significant

 the type is set to LOC if the entry has been found in the parent dirent entry:
 - idx :reference of the hash entry within the parent dirent file
 - level: no significant

 the type is set to COLL if the entry is found is a dirent cache entry that is different from parent dirent entry:
 - idx :reference of the collision dirent file
 - level: level of the dirent collision file (max is 1 today)

 The reference of the local hash entry is always found in the content of local_idx_p as output argument. It corresponds
 to the allocated hash entry with the dirent file cache pointer provided as returned argument.


 @param parent : pointer to the parent dirent entry
 @param hash_p : pointer to the result of the search
 @param bucket_idx : index of the hash bucket : taken from the lower 8 bits of the hash value applied to the name of the directory/file
 @param local_idx_p : pointer to the bitmap of the free chunks

 @retval <> NULL: the entry has been allocated and the returned pointer is the pointer to the dirent cache entry of the allocation
 @retval NULL: out of entries
 */
mdirents_cache_entry_t *dirent_cache_alloc_name_entry_idx(
        mdirents_cache_entry_t *parent, int bucket_idx,
        mdirents_hash_ptr_t *hash_p, int *local_idx_p);

/**
 *___________________________________________________________________________

 API RELATED TO HASH ENTRY SEARCHING/INSERTING AND DELETING
 *___________________________________________________________________________
 */

/*
 **______________________________________________________________________________
 */
/**
 *   Insert a hash entry in the linked associated with a bucket entry
 *

 @param dir_fd : file handle of the parent directory
 @param root : pointer to the root dirent entry
 @param target_cache_entry : pointer to target dirent cache entry that is associated with the local index
 @param hash_p : pointer to the result of the search
 @param bucket_idx : index of the hash bucket : taken from the lower 8 bits of the hash value applied to the name of the directory/file
 @param local_idx : local hash entry index within the target_cache_entry cache entry

 @retval <> NULL: the entry has been allocated and the returned pointer is the pointer to the dirent cache entry of the allocation
 @retval NULL: out of entries
 */
mdirents_cache_entry_t *dirent_cache_insert_hash_entry(int dir_fd,
        mdirents_cache_entry_t *root,
        mdirents_cache_entry_t *target_cache_entry, int bucket_idx,
        mdirents_hash_ptr_t *hash_p, int local_idx);

/*
 **______________________________________________________________________________
 */
/**
 *   Search a hash entry in the linked associated with a bucket entry, the key being the value of the hash
 *
 @param  dir_fd : file descriptor of the parent directory
 @param root : pointer to the root dirent entry
 @param hash_value : hash value to search
 @param bucket_idx : index of the hash bucket : taken from the lower 8 bits of the hash value applied to the name of the directory/file
 @param hash_entry_match_idx_p : pointer to an array where the local idx of the hash entry will be returned
 @param name : pointer to the name to search or NULL if lookup hash is requested only
 @param len  : len of name
 @param user_name_entry_p: pointer provided by the caller that will be filled with the pointer to the name entry in the cache entru
 in case of success

 @param user_hash_entry_p: pointer provided by the caller that will be filled with the pointer to the hash entry in the cache entru
 in case of success


 @retval <> NULL: pointer to the dirent cache entry where hash entry can be found, local idx is found in hash_entry_match_idx_p
 @retval NULL: not found
 */
mdirents_cache_entry_t *dirent_cache_search_hash_entry(int dir_fd,
        mdirents_cache_entry_t *root, int bucket_idx, uint32_t hash_value,
        int *hash_entry_match_idx_p, uint8_t *name, uint16_t len,
        mdirents_name_entry_t **user_name_entry_p,
        mdirents_hash_entry_t **user_hash_entry_p);

/*
 **______________________________________________________________________________
 */
/**
 * That structure is used when the caller wants to allocated a free entry in the dirent cache
 *  if during the search a free entry is found in a dirent cache entry, the service returns
 *  the pointer to that dirent cache entry otherwise a NULL pointer is returned.
 *  In case of a cache miss, the application will need to allocated a hash entry from fresh
 *  collision dirent file. In that situation, to speed up the insertion, the service returns the
 *  pointer to the dirent cache entry as well as the local index where the EOF has been found.
 *  These 2 last parameters (cache_entry_last_p/last_entry_idx_in_last) are significant when there is a
 *  cache MISS only.
 */
typedef struct _dirent_cache_search_alloc_t {
    mdirents_cache_entry_t *cache_entry_free_p; /**< pointer to the dirent entry that contains a free entry */
    mdirents_cache_entry_t *cache_entry_last_p; /**< pointer to the drent entry that the last hash entry     */
    uint16_t last_entry_idx_in_last; /**< local index of the hash entry that has the EOF */

} dirent_cache_search_alloc_t;



/*
 **______________________________________________________________________________
 */
/**
 *   delete  a hash entry in the linked associated with a bucket entry, the key being the value of the hash
 *
 @param  dir_fd : file descriptor of the parent directory
 @param root : pointer to the root dirent entry
 @param hash_value : hash value to search
 @param bucket_idx : index of the hash bucket : taken from the lower 8 bits of the hash value applied to the name of the directory/file
 @param hash_entry_match_idx_p : pointer to an array where the local idx of the hash entry will be returned
 @param cache_entry_prev_ret : pointer to an array where the pointer to the previous cache entry will be stored, a NULL
 pointer indicates that the previous dirent cache is not impacted by the removing of the hash entry
 @param name : pointer to the name to search or NULL if lookup hash is requested only
 @param len  : len of name
 @param fid: pointer to array used for storing fid of the entry to remove (nULL if not expected to be returned)
 @param mode_p: pointer to the array where mode will be copied

 @retval <> NULL: pointer to the dirent cache entry where hash entry can be found, local idx is found in hash_entry_match_idx_p
 @retval NULL: not found
 */
mdirents_cache_entry_t *dirent_cache_delete_hash_entry(int dir_fd,
        mdirents_cache_entry_t *root, int bucket_idx, uint32_t hash_value,
        int *hash_entry_match_idx_p,
        mdirents_cache_entry_t **cache_entry_prev_ret, uint8_t *name,
        uint16_t len, void *fid, uint32_t *mode_p);

/**
 *   Display a  bucket_idx linked list starting for root cache entry
 *

 @param root : pointer to the root dirent entry
 @param bucket_idx : index of the hash bucket : taken from the lower 8 bits of the hash value applied to the name of the directory/file

 @retval <> NULL: pointer to the dirent cache entry where hash entry can be found, local idx is found in hash_entry_match_idx_p
 @retval NULL: not found
 */
mdirents_cache_entry_t *dirent_cache_print_bucket_list(
        mdirents_cache_entry_t *root, int bucket_idx);
/*
 **______________________________________________________________________________
 */
/**
 * Release of a dirent file cache entry

 @param dirent_entry_p : pointer to the dirent cache entry

 @retval  -1 on error
 @retval 0 on success
 */
int dirent_cache_release_entry(mdirents_cache_entry_t *dirent_entry_p);
/**
 *___________________________________________________________________________

 API related to the dirent collision file and cache management
 *___________________________________________________________________________
 */

static inline uint32_t dirent_read_from_disk(mdirents_cache_entry_t *parent,
        uint32_t coll_idx) {
    return 0;

}

#define DIRENT_CACHE_ALLOCATE_COLL_ENTRY_IDX(p) dirent_allocate_chunks(1,(uint8_t*)p,MDIRENTS_MAX_COLLS_IDX);
#define DIRENT_CACHE_RELEASE_COLL_ENTRY_IDX(p,chunk) dirent_release_chunks(chunk,1,(uint8_t*)p,MDIRENTS_MAX_COLLS_IDX);
#define DIRENT_CACHE_SET_ALLOCATED_COLL_ENTRY_IDX(p,bit) dirent_clear_chunk_bit(bit,(uint8_t*)p);

#define DIRENT_CACHE_SET_COLL_ENTRY_FULL(p,bit) dirent_set_chunk_bit(bit,(uint8_t*)p);
#define DIRENT_CACHE_SET_COLL_ENTRY_NOT_FULL(p,bit) dirent_clear_chunk_bit(bit,(uint8_t*)p);

/**
 *  API to store the reference of a collision dirent entry in the dirent parent cache entry
 *
 @param *dirent_entry : pointer to the dirent entry associated with dirent_coll_file_idx
 @param *parent : pointer to the parent file

 @retval NULL on success
 @retval <>NULL -> if the value is ptr, it indicates that either the idx is out of range or there was no enough memory
 @retval <>NULL -> if the value is different from ptr, ptr has been stored  but the entry was not entry. The
 returned value is the pointer to the old entry.
 */
extern mdirent_indirect_ptr_template_t mdirent_cache_indirect_coll_distrib;

static inline mdirents_cache_entry_t *dirent_cache_store_collision_ptr(
        mdirents_cache_entry_t *parent, mdirents_cache_entry_t *dirent_entry) {
    int dirent_coll_file_idx;
    mdirents_cache_entry_t *returned_ptr;

    dirent_coll_file_idx =
            dirent_entry->header.dirent_idx[dirent_entry->header.level_index];
    returned_ptr = dirent_cache_store_ptr(&parent->dirent_coll_lvl0_p[0],
            &mdirent_cache_indirect_coll_distrib, dirent_coll_file_idx,
            (void *) dirent_entry);
    return returned_ptr;

}

/**
 *  API to delete the reference of a collision dirent entry in the dirent parent cache entry
 *
 @param *dirent_entry : pointer to the dirent entry associated with dirent_coll_file_idx
 @param *parent : pointer to the parent file

 @retval NULL on success
 @retval <>NULL on error : value of the input pointer if the not reference in the indirection array, or value of
 the current entry if the entry does not match with the input pointer (ptr)
 */
static inline mdirents_cache_entry_t *dirent_cache_del_collision_ptr(
        mdirents_cache_entry_t *parent, mdirents_cache_entry_t *dirent_entry) {

    int dirent_coll_file_idx;
    mdirents_cache_entry_t *returned_ptr;
    mdirent_sector0_not_aligned_t *sect0_p;

    sect0_p = DIRENT_VIRT_TO_PHY_OFF(parent,sect0_p)
    ;
    if (sect0_p == (mdirent_sector0_not_aligned_t*) NULL ) {
        DIRENT_SEVERE("dirent_cache_del_collision_ptr error at line %d\n",__LINE__)
        ;
        return (mdirents_cache_entry_t*) -1;
    }

    dirent_coll_file_idx =
            dirent_entry->header.dirent_idx[dirent_entry->header.level_index];
    returned_ptr = (mdirents_cache_entry_t*) dirent_cache_del_ptr(
            &parent->dirent_coll_lvl0_p[0],
            &mdirent_cache_indirect_coll_distrib, dirent_coll_file_idx,
            (void *) dirent_entry);
    /*
     ** set the entry free in the collision bitmap
     */
    dirent_set_chunk_bit(dirent_coll_file_idx,
            (uint8_t*) &sect0_p->coll_bitmap);
    /*
     ** need to update parent on disk
     */
    DIRENT_ROOT_UPDATE_REQ(parent);

    return returned_ptr;

}

/**
 *  API to get the pointer to a dirent collision cache entry
 *
 @param coll_idx: reference of the collision file within the dirent parent file
 @param *dirent_entry : pointer to the dirent entry associated with dirent_coll_file_idx
 @param *parent : pointer to the parent file

 @retval NULL associated dirent cache entry does not exist
 @retval <> NULL pointer to the dirent cache structure of the collision dirent file
 */

static inline mdirents_cache_entry_t *dirent_cache_get_collision_ptr(
        mdirents_cache_entry_t *parent, uint32_t coll_idx) {
    mdirent_cache_ptr_t virt_ptr;
    uint32_t ret;
    /*
     ** attempt to get a valid pointer from memory
     */

    virt_ptr = dirent_cache_get_virtual_ptr(&parent->dirent_coll_lvl0_p[0],
            &mdirent_cache_indirect_coll_distrib, coll_idx);
    if (virt_ptr.s.val != 0) {
        uint64_t val;
        val = virt_ptr.s.val;
        return (mdirents_cache_entry_t*) PTR2INT val;

    }
    /*
     ** the return value is null, so now it depends on the type of the dirent cache root entry. If it has been created from
     ** a file, in that case we attempt to read the associated file from disk and allocate a dirent cache entry in memory.
     ** When the root dirent cache entry is memory only we don't do it.
     ** This mainly depends on the value of the dirty and rd bits of the virtual pointer
     **
     */
    if ((virt_ptr.s.rd == 1) && (virt_ptr.s.dirty == 1)) {
        /*
         ** here is the situation where the collision entry has to be read from disk
         */
        ret = dirent_read_from_disk(parent, coll_idx);
        if (ret == -1) {
            /*
             ** something wrong while attempting to read the file: either we run out of memory of the file does not exist
             */
            return (mdirents_cache_entry_t*) NULL ;
        }
        /*
         ** the file has been loaded in memory and the virtual pointer has been updated in the parent cache entry, just need to get it
         */
        return (mdirents_cache_entry_t*) dirent_cache_get_ptr(
                &parent->dirent_coll_lvl0_p[0],
                &mdirent_cache_indirect_coll_distrib, coll_idx);
    }
    /*
     ** The cache is synced, and thus there no collision entry for the requested coll_idx
     */
    return (mdirents_cache_entry_t*) NULL ;

}

/*
 *______________________________________________________________________________
 M D I R E N T    F I L E   S E R V I C E S
 *______________________________________________________________________________
 */
/**
 *  @defgroup DIRENT_HIGH_LVL_API Dirent File external API
 Thess API are intended to be used by rozofs to get and store information related to a file\n
 or directory. These information are the following:\n
 - the FID (unique file identifier)\n
 - the mode\n

 These information are stored on disk in file called "dirent_file". these files contains the \n
 following information:\n
 - name of file (Primary Key) \n
 - the FID (unique file identifier)\n
 - the mode \n

 These files are stored in the parent directory. They are up to 4096 dirent file and each
 root dirent file might have up to 2048 collision dirent file (theorically). \n
 the name of a dirent file is build as follows(where each dirent file can contain up to 384 entrie):\n
 - d_<root_idx>_<coll_idx> with root_idx=0..4095 and coll_idx=0..2047\n
 \n

 It provides the following services: \n
 - put_mdirentry(): insertion of FID and mode associated with a name relative to the parent directory\n
 - get_mdirentry(): get the information relative to a name within a given parent directory\n
 - del_mdirentry(): remove from a dirent file the information relative to name within a parent directory\n
 - list_mdirentries(): list the entries relative to a parent directory (equivalent of readdir)\n

 */

/*
** flag that is used to indicate read/write permission for the hash associated with a root dirent file 
**
** read only asserted : only lookup/replace and deletion are accepted, no new insertion
*/

extern int dirent_root_read_only;
#define DIRENT_ROOT_IS_READ_ONLY() (dirent_root_read_only == 1)
#define DIRENT_ROOT_SET_READ_WRITE() dirent_root_read_only = 0
#define DIRENT_ROOT_SET_READ_ONLY() dirent_root_read_only = 1

/**
 * Read the mdirents file on disk
 *
 * @param dirfd: file descriptor of the parent directory
 * @param *pathname: pointer to the pathname to read
 * @param parent: fid of the parent directory (needed by writeback cache
 *
 * @retval NULL if this mdirents file doesn't exist
 * @retval pointer to the mdirents file
 */
mdirents_cache_entry_t * read_mdirents_file(int dirfd,
        mdirents_header_new_t *dirent_hdr_p,fid_t parent);

/*
 **______________________________________________________________________________
 */
/**
 * Write a mdirents file on disk
 *
 * @param dirfd: file descriptor of the parent directory
 * @param *dirent_cache_p: pointer to cache entry to re-write
 *
 * @retval 0 on success
 * @retval -1 on error
 */
int write_mdirents_file(int dirfd, mdirents_cache_entry_t *dirent_cache_p);
/**
 * Write a name entry array on disk. That API is intended to be called when
 there no change on the management sector of a dirent file. It is typically
 the case when the entry needs to be updated when the fid of the name entry is
 change. In fact that change does not impact the chunks that have been previously
 allocated for the name entry
 *
 * @param dirfd: file descriptor of the parent directory
 * @param *dirent_cache_p: pointer to cache entry to re-write
 * @param start_chunk_idx : reference of the first chunk associated with the name entry
 *
 * @retval 0 on success
 * @retval -1 on error
 */
int dirent_write_name_array_to_disk(int dirfd,
        mdirents_cache_entry_t *dirent_cache_p, uint16_t start_chunk_idx);


/*
**______________________________________________________________________________
*/
/**
*  Get the mask to apply according to the current number of file in the directory

  @param children : number of files
  @param hash1 : hash value for fid and name
  
  @retval root_idx : root file index
*/
int dirent_get_root_idx(uint64_t children,uint32_t hash1);
/*
 **______________________________________________________________________________
 */

/** @ingroup DIRENT_HIGH_LVL_API
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
int put_mdirentry(void *root_idx_bitmap_p,int dirfd, fid_t fid_parent, char * name, fid_t fid, uint32_t type,
                  mdirent_fid_name_info_t *fid_name_info_p,uint64_t children,int *mask);

/*
 **______________________________________________________________________________
 */
/** @ingroup DIRENT_HIGH_LVL_API
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
int get_mdirentry(void *root_idx_bitmap_p,int dirfd, fid_t fid_parent, char * name, 
                  fid_t fid, uint32_t * type,int *mask_ret) ;

/*
 **______________________________________________________________________________
 */
/** @ingroup DIRENT_HIGH_LVL_API
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
int del_mdirentry(void *root_idx_bitmap_p,int dirfd, fid_t fid_parent, char * name, fid_t fid, 
         uint32_t * type,int mask);

/*
 **______________________________________________________________________________
 */
/** @ingroup DIRENT_HIGH_LVL_API
 * API for list mdirentries of one directory
 *
 * @param mdir: pointer to the mdirent structure for directory specific attributes
 * @param children: pointer to pointer where the first children we will stored
 * @param cookie: index mdirentries where we must begin to list the mdirentries
 * @param *eof: pointer that indicates if we list all the entries or not
 *
 * @retval  0 on success
 * @retval -1 on failure
 */
int list_mdirentries(void *root_idx_bitmap_p,int dir_fd, fid_t fid_parent, child_t ** children,
        uint64_t *cookie, uint8_t * eof);

int list_mdirentries2(void *root_idx_bitmap_p,int dir_fd, fid_t fid_parent_in, char *buf_readdir_in, uint64_t *cookie, uint8_t * eof,ext_mattr_t *parent);
/*
 *___________________________________________________________________
 DIRENT CACHE  API
 *___________________________________________________________________
 */
/**
 * Allocation of a dirent file cache entry

 @param dirent_hdr_p : pointer to the dirent header that contains its reference
 @param parent : fid of the parent directory (needed by writeback cache)

 @retval  <>NULL  pointer to the dirent cache entry
 @retval NULL : out of cache entries
 */
mdirents_cache_entry_t *dirent_cache_allocate_entry(
        mdirents_header_new_t *dirent_hdr_p,fid_t parent);

/*
 *___________________________________________________________________
 WRITE BACK CACHE  API
 *___________________________________________________________________
 */

/*
 ** Print the dirent cache bucket statistics
 */
void writebck_cache_print_stats();

/**
 *  API for init of the dirent level 0 cache
 @param cache: pointer to the cache descriptor

 @retval none
 */
void writebck_cache_level0_initialize();

mdirents_file_t *writebck_cache_bucket_get_entry(uint64_t *key_ext,
        uint16_t index);
int writebck_cache_bucket_is_write_needed(uint64_t *key_ext, uint16_t index);

int writebck_cache_bucket_flush_entry(uint64_t *key, uint16_t index);
void writebck_cache_print_access_stats();
void writebck_cache_print_per_count_stats();

/*
 *___________________________________________________________________
 DIRENT REPAIR API
 *___________________________________________________________________
 */

extern int dirent_repair_print_enable;
/**
 * Dirent repair causes
 */
typedef enum _dirent_file_repair_cause_e {
    DIRENT_REPAIR_NONE = 0, /**< no cause  */
    DIRENT_REPAIR_LOOP, /**< loop detection  */
    DIRENT_REPAIR_FREE, /**< free hash entry */
    DIRENT_REPAIR_MEM, /**< no memory       */
    DIRENT_REPAIR_BUCKET_IDX_MISMATCH, /**< mismatch on bucket idx due to a free entry in the linked list       */
    DIRENT_REPAIR_NO_COLL_FILE, DIRENT_REPAIR_NO_EOF, DIRENT_REPAIR_MAX
} dirent_file_repair_cause_e;



/*
 *______________________________________________________________________________
 
    SAFE CONTROL: check the bucket list when the dirent file are loaded from disk
 *______________________________________________________________________________
 */
extern uint8_t dirent_cache_safe_enable ;

 int dirent_cache_is_bucket_idx_safe ( int dir_fd,
                                  mdirents_cache_entry_t *root,
                                  int       bucket_idx);

/*
 **______________________________________________________________________________
 */
/*
 **______________________________________________________________________________
 */
/**
 * API to repair a faulty dirent file (collision or root)
 *
 * @param fd_dir: file handle of the parent directory
 * @param root: pointer to the mdirent root entry
 * @param cache_entry_p: pointer to the mdirent entry to repair
 * @param bucket_idx : bucket idx for which the issue has been detected
 *
 * @retval  0 on success
 * @retval -1 on failure
 */
void dirent_cache_repair_cache_entry_for_bucket_idx(int fd_dir,
        mdirents_cache_entry_t *root, mdirents_cache_entry_t *cache_entry_p,
        int bucket_idx, mdirents_hash_entry_t **hash_entry_last_p,
        int *first_index);

/*
 **______________________________________________________________________________
 */
/**
 *  PRIVATE API: Check if the linked list for a bucket within a cache enty needed to be repaired

 @param target_cache_entry : pointer to target dirent cache entry that is associated with the local index
 @param bucket_idx : index of the hash bucket : taken from the lower 8 bits of the hash value applied to the name of the directory/file

 @retval 0 safe
 @retval -1 not safe
 */

static inline dirent_file_repair_cause_e dirent_cache_check_repair_needed(
        mdirents_cache_entry_t *root, mdirents_cache_entry_t *cache_entry_p,
        mdirents_hash_ptr_t *hash_bucket_p, int bucket_idx)

{
    mdirents_cache_entry_t *cache_coll_entry = NULL;
    dirent_file_repair_cause_e cause = DIRENT_REPAIR_NONE;
    mdirent_sector0_not_aligned_t *sect0_p;
    uint8_t *hash_bitmap_p;
    mdirents_hash_entry_t *hash_entry_next_p = NULL;
    int hash_entry_bucket_idx_next;
    int cur_hash_entry_idx_repair = -1;

    sect0_p = DIRENT_VIRT_TO_PHY_OFF(cache_entry_p,sect0_p)
    ;
    hash_bitmap_p = (uint8_t*) &sect0_p->hash_bitmap;
    /*
     ** Check if the entry is valid on not by checking the bitmap
     */
    switch (hash_bucket_p->type) {
    case MDIRENTS_HASH_PTR_LOCAL:
        /*
         ** need to check the state of the hash entry in the bitmap
         */
        cur_hash_entry_idx_repair = hash_bucket_p->idx;
        if (dirent_test_chunk_bit(cur_hash_entry_idx_repair, hash_bitmap_p)
                != 0) {
            cause = DIRENT_REPAIR_FREE;
            break;
        }
        /*
         ** check if the next belongs to the same list
         */
        hash_entry_next_p =
                (mdirents_hash_entry_t*) DIRENT_CACHE_GET_HASH_ENTRY_PTR(cache_entry_p,cur_hash_entry_idx_repair)
        ;
        if (hash_entry_next_p == NULL ) {
            cause = DIRENT_REPAIR_FREE;
            break;
        }

        hash_entry_bucket_idx_next =
                DIRENT_HASH_ENTRY_GET_BUCKET_IDX(hash_entry_next_p);
        if (hash_entry_bucket_idx_next != bucket_idx) {
            cause = DIRENT_REPAIR_BUCKET_IDX_MISMATCH;
            break;
        }
        break;

    case MDIRENTS_HASH_PTR_COLL:
        /*
         ** need to check if the collision file exist and then check the state of the hash entry in the
         ** collision file
         */
        cache_coll_entry = dirent_cache_get_collision_ptr(root,
                hash_bucket_p->idx);
        if (cache_coll_entry == NULL ) {
            /*
             ** something is rotten in the cache since the pointer to the collision dirent cache
             ** does not exist
             */
            DIRENT_WARN("dirent_cache_check_repair_needed(%d) collision entry does not exist %d \n",bucket_idx,hash_bucket_p->idx)
            ;
            cause = DIRENT_REPAIR_NO_COLL_FILE;
            break;
        }

        /**
         * all is fine !!!
         */
        break;

    case MDIRENTS_HASH_PTR_EOF:
        /*
         ** No issue there, nothing to control--> will not OCCUR !!!!!!
         */
        break;

    case MDIRENTS_HASH_PTR_FREE:
         /*
	 ** this could happen while attempt to insert a hash entry
         */
         cause = DIRENT_REPAIR_FREE;
         break;
	 
    default:
        /*
         ** force EOF
         */
        DIRENT_WARN("dirent_cache_check_repair_needed(%d) wrong type for to EOF %d \n",__LINE__,hash_bucket_p->type)
        ;
        hash_bucket_p->type = MDIRENTS_HASH_PTR_EOF;
        break;
    }
    return cause;
}

/**
 * API to rebuild the linked list of the hash entry within the linked list of the cache entry
 for a given bucket index.
 Each modified dirent cache entry will be re-written on disk during that procedure
 *
 * @param dir_fd: file handle of the parent directory
 * @param bucket_idx: bucket index for which the rebuild is requested
 * @param *root_entry_p: pointer to root dirent cache entry
 * @param cause: cause that triggers the dirent file repair
 *
 * @retval none
 */
void dirent_file_repair(int dir_fd, mdirents_cache_entry_t *root_entry_p,
        int bucket_idx, dirent_file_repair_cause_e cause);

/**
 * Internal API to check the linked list of the hash entry within the linked list of the cache entry
 for a given bucket index.
 Each modified dirent cache entry will be re-written on disk during that procedure
 *
 * @param dir_fd: file handle of the parent directory
 * @param bucket_idx: bucket index for which the rebuild is requested
 * @param *root_entry_p: pointer to root dirent cache entry
 * @param cause: cause that triggers the dirent file repair
 * @param repair: assert to 1 to repair 0 to check
 *
 * @retval none
 */
void dirent_file_check(int dir_fd, mdirents_cache_entry_t *root_entry_p,
        int bucket_idx);

/*
 **______________________________________________________________________________
 */
/**
 *  API: clear dirent file repair statistics
 *
 * @retval none
 */
void dirent_file_repair_stats_clear();
/*
 **______________________________________________________________________________
 */
/**
 *  API: print out the dirent file repair statistics
 *
 * @retval none
 */
void dirent_file_repair_stats_print();

/*
 **______________________________________________________________________________
 
    DIRENT  WRITEBACK CACHE SECTION
 **______________________________________________________________________________
*/    

#define DIRENT_CACHE_MAX_ENTRY   4096
#define DIRENT_CACHE_MAX_CHUNK   16
#define DIRENT_MAX_NAME 32
#define DIRENT_WBCACHE_FD_MASK   0x4000000   /**< indicates that the fd comes from cache */

typedef struct _dirent_chunk_cache_t
{
    uint32_t  wr_cpt;   /**< incremented on each write / clear when synced   */
    uint32_t  size;     /**< size of the block */
    off_t  off;         /**< offset of the chunk  */
    char   *chunk_p;    /**< pointer to the chunk array  */
}  dirent_chunk_cache_t;

/**
*  write back cache entry
*/
typedef struct _dirent_writeback_entry_t
{
     int fd;     /**< current fd: used as a key during the write  */
     pthread_rwlock_t lock;  /**< entry lock  */
     int dir_fd; /**< file descriptor of the directory          */
     int state;  /**< O free: 1 busy*/
     /*
     ** identifier of the dirent file
     */
     fid_t     dir_fid;  /**< fid of the directory     */
     uint16_t   eid;     /**< reference of the export  */
     char       pathname[DIRENT_MAX_NAME]; /**< dirent file local pathname */
     /*
     **  pointer to the header of the dirent file (mdirents_file_t)
     */
     uint32_t  wr_cpt;   /**< incremented on each write / clear when synced   */
     uint32_t  size;     /**< size of the block */
     char *dirent_header;
     dirent_chunk_cache_t  chunk[DIRENT_CACHE_MAX_CHUNK];
} dirent_writeback_entry_t;


/*
** pointer to the dirent write back cache
*/
extern dirent_writeback_entry_t   *dirent_writeback_cache_p ;
extern int dirent_writeback_cache_enable;
extern int dirent_writeback_cache_initialized;
extern uint64_t dirent_wbcache_hit_counter;
extern uint64_t dirent_wbcache_miss_counter;
extern uint64_t dirent_wb_cache_write_bytes_count;
extern uint64_t dirent_wb_write_count;
extern uint64_t dirent_wb_write_chunk_count;  /**< incremented each time a chunk need to be flushed for making some room */
extern uint64_t dirent_wbcache_flush_counter;
extern uint64_t dirent_wbcache_invalidate_counter;

/**
*____________________________________________________________
*/
static inline char *dirent_wbcache_display_stats(char *pChar) {
    pChar+=sprintf(pChar,"WriteBack cache statistics:\n");
    pChar+=sprintf(pChar,"state          :%s\n",(dirent_writeback_cache_enable==0)?"Disabled":"Enabled");
    pChar+=sprintf(pChar,"NB entries     :%d\n",DIRENT_CACHE_MAX_ENTRY);
    pChar+=sprintf(pChar,"hit/miss/flush : %llu/%llu/%llu\n",
            (long long unsigned int) dirent_wbcache_hit_counter,
            (long long unsigned int) dirent_wbcache_miss_counter,
            (long long unsigned int) dirent_wbcache_flush_counter
	    );
    pChar+=sprintf(pChar,"invalidate     : %llu\n",
            (long long unsigned int) dirent_wbcache_invalidate_counter
	    );
    pChar+=sprintf(pChar,"total Write : memory %llu MBytes (%llu Bytes) requests %llu ejected chunks %llu\n\n",
            (long long unsigned int) dirent_wb_cache_write_bytes_count / (1024*1024),
            (long long unsigned int) dirent_wb_cache_write_bytes_count,
            (long long unsigned int) dirent_wb_write_count,
	    (long long unsigned int) dirent_wb_write_chunk_count);
    return pChar;
}

void show_wbcache_thread(char * argv[], uint32_t tcpRef, void *bufRef);
/**
*____________________________________________________________
*/
/**
*  create cache entry
   
  @param dir_fid : fid of the directory
  @param fd : file descriptor 
  @param eid : export identifier
  @param pathname : local pathname of the dirent file
  @param fd_dir : file descriptor of the directory 

*/
int dirent_wbcache_open(int fd_dir,int fd,int eid,char *pathname,fid_t dir_fid,int root_idx);
/**
*____________________________________________________________
*/
/**
*  write the in the cache

   @param fd : file descriptor
   @param buf: buffer to write
   @param count : length to write
   @param offset: offset within the file
   
   @retval >= 0 : number of bytes written in the writeback cache
   @retval < 0 : error see errno for details
*/
int dirent_wbcache_write(int fd,void *buf,size_t count,off_t offset);

/**
*____________________________________________________________
*/
/**
*  check if the write back cache must be flushed on disk
   
  @param eid : export identifier
  @param pathname : local pathname of the dirent file
  @param root_idx : root index of the file (key)
  @param dir_fid : fid of the directory (key)

*/
int dirent_wbcache_check_flush_on_read(int eid,char *pathname,int root_idx,fid_t dir_fid);

/**
*____________________________________________________________
*/
/**
*  check if the write back cache must be invalidated
   
  @param eid : export identifier
  @param pathname : local pathname of the dirent file
  @param root_idx : root index of the file (key)
  @param dir_fid : fid of the directory (key)

*/
int dirent_wbcache_check_invalidate_on_unlink(int eid,char *pathname,int root_idx,fid_t dir_fid);
/**
*____________________________________________________________
*/
/**
*  close a file associated with a dirent writeback cache entry

   @param fd : file descriptor
   
   @retval >= 0 : number of bytes written in the writeback cache
   @retval < 0 : error see errno for details
*/
int dirent_wbcache_close(int fd);

/**
*____________________________________________________________
*/
/**
* init of the write back cache

  @retval 0 on success
  @retval < 0 error (see errno for details)
*/
int dirent_wbcache_init();
/**
*____________________________________________________________
*/
/**
*  writeback cache enable
*/
static inline void dirent_wbcache_enable()
{
  if (dirent_writeback_cache_initialized) dirent_writeback_cache_enable = 1;
}
/**
*____________________________________________________________
*/
/**
*  writeback cache enable
*/
static inline void dirent_wbcache_disable()
{
  dirent_writeback_cache_enable = 0;

}
/**
 *__________________________________________________
 *  API RELATED TO PREAD/PRWITE
 *__________________________________________________
 */

/** @defgroup DIRENT_RW_DISK  Disk Read/write
 *  This module provides services related to disk read/write operations
 */

extern uint64_t dirent_read_bytes_count; /**< @ingroup DIRENT_RW_DISK cumulative number of bytes read */
extern uint64_t dirent_pread_count; /**< @ingroup DIRENT_RW_DISK cumaltive number of read requests  */
extern uint64_t dirent_write_bytes_count; /**< @ingroup DIRENT_RW_DISK cumulative number of bytes written */
extern uint64_t dirent_write_count; /**< @ingroup DIRENT_RW_DISK cumaltive number of write requests  */
/**
*____________________________________________________________
*/
/**
 * @ingroup DIRENT_RW_DISK
 *  API for reading on disk: see pread() man for details
 */
static inline ssize_t dirent_pread(int fd, void *buf, size_t count,
        off_t offset) {
    dirent_read_bytes_count += count;
    dirent_pread_count++;
#if DIRENT_NO_DISK
    return count;
#else
    return pread(fd, buf, count, offset);
#endif
}

#define DIRENT_PREAD dirent_pread  /**<@ingroup DIRENT_RW_DISK */
/**
*____________________________________________________________
*/
/**
 * @ingroup DIRENT_RW_DISK
 *  API for reading on disk: see pwrite() man for details
 */
static inline ssize_t dirent_pwrite(int fd, const void *buf, size_t count,
        off_t offset) {

//  printf("FDL BUG dirent_pwrite count %d offset %d (%x) \n",(int)count,(int)offset,(unsigned int)offset);
#if DIRENT_NO_DISK
    return count;
#else
    /*
    ** check if the fd comes from the writeback cache
    */
    if (fd & DIRENT_WBCACHE_FD_MASK)
    {
      fd  = fd &(~DIRENT_WBCACHE_FD_MASK);

//      severe("FDL %s[%d]=%d write offset %llu len %u",dirent_writeback_cache_p[fd].pathname,
//              fd,dirent_writeback_cache_p[fd].fd,offset,count);
      return dirent_wbcache_write(fd,(void*)buf, count, offset);
    }
    /*
    ** no write back cache
    */
    dirent_write_bytes_count += count;
    dirent_write_count++;
    return pwrite(fd, buf, count, offset);
#endif
}

#define DIRENT_PWRITE dirent_pwrite  /**<@ingroup DIRENT_RW_DISK */


static inline void mdirent_resolve_path(fid_t fid,char *pathname_dentry,char *path)
{
    char str[37];
    uint32_t slice;
    uint32_t subslice;
    eid_t eid;
    char *root_path;
    
    
    eid = rozofs_get_eid_from_fid(fid);    
    
    if (eid > EXPGW_EID_MAX_IDX) eid = 0;
    root_path = dirent_export_root_path[eid];
    if (root_path == NULL)
    {
       severe("no root for eid %d",eid);
       sprintf(path,"/bad_eid/%d",eid);
       return;
    
    }
    mstor_get_slice_and_subslice(fid, &slice, &subslice);
    rozofs_uuid_unparse(fid, str);
    
    sprintf(path, "%s/%d/%s/%s", root_path, slice, str,pathname_dentry);

}
/**
*____________________________________________________________
*/
/**
 * @ingroup DIRENT_RW_DISK
 *  API for opening a dirent file for writing
 
   @param dirfd : file descriptor of the directory
   @param pathname: pathname of the dirent file relative to the directory
   @param flags : opening flags
   @param mode :  mode
   @param dir_fid : fid of the directory
   
 */
static inline int dirent_openat(int dirfd, const char *pathname, int flags, mode_t mode,fid_t dir_fid,int root_idx)
{
  int fd;
  char path[PATH_MAX];
  
  if (dirent_writeback_cache_enable == 0)
  {
    mdirent_resolve_path(dir_fid,(char*)pathname,path);
    fd = open(path,flags,mode);
    return fd;
  }
  /*
  ** write back cache is enable, so attempt to take one entry
  */   
  int fd_local = dirent_wbcache_open(dirfd,0,dirent_current_eid,(char*)pathname,dir_fid,root_idx);
  if (fd_local < 0)
  {
    /*
    ** writeback cache is full, so by-pass it
    */
    mdirent_resolve_path(dir_fid,(char*)pathname,path);
    fd = open(path,flags,mode);
    return fd;
  }
  /*
  ** write back cache is used, assert the bit that indicates that the fd comes from the writeback cache
  */
  fd_local |= DIRENT_WBCACHE_FD_MASK;
  return fd_local;
}
#define DIRENT_OPENAT dirent_openat


/**
*____________________________________________________________
*/
/**
 * @ingroup DIRENT_RW_DISK
 *  API for opening a dirent file for reading
 
   @param dirfd : file descriptor of the directory
   @param pathname: pathname of the dirent file relative to the directory
   @param flags : opening flags
   @param mode :  mode
   @param dir_fid : fid of the directory
   
 */
static inline int dirent_openat_read(int dirfd, const char *pathname, int flags, mode_t mode,fid_t dir_fid,int root_idx)
{
  int fd;
  char path[PATH_MAX];

  if (dirent_writeback_cache_enable != 0)
  {
    /*
    ** check if the write back cachemust be flushed
    */   
    dirent_wbcache_check_flush_on_read(dirent_current_eid,(char *)pathname,root_idx,dir_fid);
  }
  mdirent_resolve_path(dir_fid,(char*)pathname,path);
  fd = open(path,flags,mode);
  return fd;
}
#define DIRENT_OPENAT_READ dirent_openat_read

/**
*____________________________________________________________
*/
/**
 * @ingroup DIRENT_RW_DISK
 *  API for closing a dirent file opened for writing
 
   @param fd : file descriptor of the dirent file

   
 */
static inline int dirent_close(int fd)
{
    /*
    ** check if the fd comes from the writeback cache
    */
    if (fd& DIRENT_WBCACHE_FD_MASK)
    {
      fd  = fd &(~DIRENT_WBCACHE_FD_MASK);
      return dirent_wbcache_close(fd);
    }
    /*
    ** the write back cache was not used
    */
    return close(fd);      
}
#define DIRENT_CLOSE dirent_close





/**
 @ingroup DIRENT_RW_DISK
 *
 *  clear the disk read/write statistics
 */
static inline void dirent_disk_clear_stats() {

    dirent_read_bytes_count = 0; /**< cumulative number of bytes read */
    dirent_pread_count = 0; /**< cumaltive number of read requests  */
    dirent_write_bytes_count = 0; /**< cumulative number of bytes written */
    dirent_write_count = 0; /**< cumaltive number of write requests  */

}

/**
 @ingroup DIRENT_RW_DISK

 *  Print  disk read/write statistics
 */
static inline void dirent_disk_print_stats() {
    printf("File System usage statistics:\n");
    printf("Total Read : memory %llu (%llu) requests %llu\n",
            (long long unsigned int) dirent_read_bytes_count / 1000000,
            (long long unsigned int) dirent_read_bytes_count,
            (long long unsigned int) dirent_pread_count);
    printf("Total Write: memory %llu (%llu) requests %llu\n\n",
            (long long unsigned int) dirent_write_bytes_count / 1000000,
            (long long unsigned int) dirent_write_bytes_count,
            (long long unsigned int) dirent_write_count);
}

static inline char *dirent_disk_display_stats(char *pChar) {
    pChar+=sprintf(pChar,"File System usage statistics:\n");
    pChar+=sprintf(pChar,"Total Read : memory %llu MBytes (%llu Bytes) requests %llu\n",
            (long long unsigned int) dirent_read_bytes_count / (1024*1024),
            (long long unsigned int) dirent_read_bytes_count,
            (long long unsigned int) dirent_pread_count);
    pChar+=sprintf(pChar,"Total Write: memory %llu MBytes (%llu Bytes) requests %llu\n\n",
            (long long unsigned int) dirent_write_bytes_count / (1024*1024),
            (long long unsigned int) dirent_write_bytes_count,
            (long long unsigned int) dirent_write_count);
    return pChar;
}

/*
________________________________________________________________________

*  BITMAP MANAGEMENT OF THE ROOT FILE INDEXES
________________________________________________________________________
*/
/**
*  set the pointer to the root_idx bitmap
*/
extern void *dirent_cur_root_idx_bitmap_p;

static inline void dirent_set_root_idx_bitmap_ptr(void *root_idx_bitmap_p)
{
  dirent_cur_root_idx_bitmap_p = root_idx_bitmap_p;

}
void export_dir_update_root_idx_bitmap(void *ctx_p,int root_idx,int set);

/**
* clear the root_idx bit in the current directory
*/
static inline void dirent_clear_root_idx_bit(int root_idx)
{
   export_dir_update_root_idx_bitmap(dirent_cur_root_idx_bitmap_p,root_idx,0);
}

/**
* set the root_idx bit in the current directory
*/

static inline void dirent_set_root_idx_bit(int root_idx)
{
   export_dir_update_root_idx_bitmap(dirent_cur_root_idx_bitmap_p,root_idx,1);
}

int export_dir_check_root_idx_bitmap_bit(void *ctx_p,int root_idx);
static inline int dirent_check_root_idx_bit(int root_idx)
{
   return export_dir_check_root_idx_bitmap_bit(dirent_cur_root_idx_bitmap_p,root_idx);
}
#endif
