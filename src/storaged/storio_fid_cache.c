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
#include <rozofs/core/rozofs_string.h>

#include "storio_fid_cache.h"

STORIO_FID_CACHE_BUCKET_T   * storio_fid_cache;

storio_fid_exact_match_fct    storio_fid_exact_match;
storio_fid_delete_request_fct storio_fid_delete_request;

STORIO_FID_STAT_T storio_fid_cache_stat;

/*
** lowest0 gives the rank of the lowest 0 in a 8bit value
** lowest1 gives the rank of the lowest 1 in a 8bit value
*/
uint8_t lowest0[0xFF];
uint8_t lowest1[0xFF];

/*
**______________________________________________________________________________
** Display hash statistics
**
** @param pChar where to format the display
** @retval The end of the display
*/
#define DISPLAY_CACHE_FID_STAT(x,end) {\
  pChar += rozofs_string_append(pChar,"    \"");\
  pChar += rozofs_string_padded_append(pChar, 6, rozofs_left_alignment, #x);\
  pChar += rozofs_string_append(pChar,"\":");\
  pChar += rozofs_u64_append(pChar, storio_fid_cache_stat.x);\
  pChar += rozofs_string_append(pChar,end);\
}  
char * display_cache_fid_stat(char * pChar) {
  pChar += rozofs_string_append(pChar,"  \"cache statistics\" : {\n");
  DISPLAY_CACHE_FID_STAT(count,",\n");
  DISPLAY_CACHE_FID_STAT(hit,",\n");
  DISPLAY_CACHE_FID_STAT(miss,",\n");
  DISPLAY_CACHE_FID_STAT(bkts,",\n");
  DISPLAY_CACHE_FID_STAT(mxbkt,",\n");
  DISPLAY_CACHE_FID_STAT(mxcol,"\n")
  pChar += rozofs_string_append(pChar,"  }");
  *pChar = 0;
  return pChar;
}  
/*
**______________________________________________________________________________
** Initializae the lowest0 and lowest1 tables
** lowest0 gives the rank of the lowest 0 in a 8bit value
** lowest1 gives the rank of the lowest 1 in a 8bit value
*/
void storio_fid_cache_init_lowest_arrays() {
  uint32_t    val;
  uint8_t    bit;
  
  for (val = 0; val < 0x100; val++) {
  
    /*
    ** Look for lowest 1 in val 
    */
    for (bit=0; bit<8; bit++) {
      if (val & (1<<bit)) {
        lowest1[val] = bit;
	break;
      }
    }
    
    /*
    ** Look for lowest 0 in val 
    */
    for (bit=0; bit<8; bit++) {
      if (!(val & (1<<bit))) {
        lowest0[val] = bit;
	break;
      }
    }    
    
  }
}
/*
**______________________________________________________________________________
** Insert a FID context reference in the cache 
** 
** @param hash   A 32 bits hash value of the entry
** @param index  The index to store
**
** @retval 0  on success
** @retval -1 on error 
*/
int storio_fid_cache_insert(uint32_t hash, uint32_t index) {
  STORIO_FID_CACHE_BUCKET_T     * pBucket;
  STORIO_FID_CACHE_SUB_BUCKET_T * pSub;
  STORIO_FID_CACHE_ENTRY_U      * pEntry;
  int                        bbyte, bbit;
  int                        sbyte, sbit;
  int                        subIdx;
  int                        entryIdx;
  int                        idx;

  /* 
  ** Get the bucket entry from the hash value
  */
  pBucket = storio_fid_cache;
  pBucket += (hash & ((1<<STORIO_FID_CACHE_LVL0_SZ_POWER_OF_2)-1));

  /*
  ** Subhash within the bucket. Keep upper bits for the entry hash in the bucket
  */  
  uint32_t subhash = (hash >> STORIO_FID_CACHE_LVL0_SZ_POWER_OF_2) & ((1<<STORIO_FID_CACHE_HASH_BITS)-1);
   
restart:  
  /*
  ** Find the 1rst not full sub bucket 
  */
  for (bbyte=0; bbyte < STORIO_FID_CACHE_MAX_SUB_BUCKET_BITMAP; bbyte++) {
    if (pBucket->full[bbyte] != 0xFF) break;
  }
  if (bbyte == STORIO_FID_CACHE_MAX_SUB_BUCKET_BITMAP) {
    /*
    ** All sub buckets are full. Should free an entry
    */
    for (bbyte=0; bbyte < STORIO_FID_CACHE_MAX_SUB_BUCKET_BITMAP; bbyte++) {
      for (bbit=0; bbit< 8; bbit++) {
      
        subIdx = 8 * bbyte + bbit;
	if (subIdx==0) pSub = &pBucket->bucket0;
        else           pSub = pBucket->xtra_bucket[subIdx-1]; 

        for (sbyte=0; sbyte < STORIO_FID_CACHE_MAX_SUB_BUCKET_ENTRIES_BITMAP; sbyte++) {
          for (sbit=0; sbit< 8; sbit++) {
            entryIdx = 8 * sbyte + sbit;
            pEntry = &pSub->entry[entryIdx]; 
	    if (storio_fid_delete_request(pEntry->s.index)) break;
	  }
	  if (sbit != 8) break;
	}
	if (sbyte != STORIO_FID_CACHE_MAX_SUB_BUCKET_ENTRIES_BITMAP) break;
      }
      if (bbit != 8) break;
    }  	  
  }
  if (bbyte == STORIO_FID_CACHE_MAX_SUB_BUCKET_BITMAP) {
    /*
    ** Bucket full and no entry freed !!! 
    */
    severe("bucket full");
    return -1;
  }
  
  /*
  ** Get 1rst not full sub bucket
  */
  bbit = lowest0[pBucket->full[bbyte]];
  subIdx = 8 * bbyte + bbit;
  if (subIdx==0) pSub = &pBucket->bucket0;
  else           pSub = pBucket->xtra_bucket[subIdx-1]; 
  
  /*
  ** Allocate the sub bucket if needed
  */
  if (pSub == 0) {
  
    pSub = malloc(sizeof(STORIO_FID_CACHE_SUB_BUCKET_T));
    pBucket->xtra_bucket[subIdx-1] = pSub;
    if (pSub == NULL) {
      severe("Out of memory");
      return -1;
    }
    storio_fid_cache_stat.bkts++;
    
    if (subIdx > storio_fid_cache_stat.mxbkt) {
      storio_fid_cache_stat.mxbkt = subIdx;
    } 
    
    memset(pSub,0,sizeof(STORIO_FID_CACHE_SUB_BUCKET_T));  
  }
  
    
  /*
  ** Find a free entry in the sub bucket
  */
  for (sbyte=0; sbyte < STORIO_FID_CACHE_MAX_SUB_BUCKET_ENTRIES_BITMAP; sbyte++) {
    if (pSub->allocated[sbyte] != 0xFF) break;
  }
  if (sbyte == STORIO_FID_CACHE_MAX_SUB_BUCKET_ENTRIES_BITMAP) {
    /*
    ** Sub bucket was not indicated as full !!!
    */
    severe("sub bucket full");
    pBucket->full[bbyte] |= (1<<bbit);
    pBucket->empty[bbyte] &= ~(1<<bbit);    
    goto restart;
  }   
  
  /*
  ** Get 1rst free entry
  */
  sbit = lowest0[pSub->allocated[sbyte]];
  entryIdx = 8 * sbyte + sbit;
  pEntry = &pSub->entry[entryIdx]; 
  
  /*
  ** Write entry
  */
  pEntry->s.hash  = subhash;
  pEntry->s.index = index; 
  
  
  /*
  ** Update sub subcket bit map
  */
  pSub->allocated[sbyte] |= (1<<sbit);
  
  /*
  ** Is the sub bucket full
  */
  for (idx=0; idx < STORIO_FID_CACHE_MAX_SUB_BUCKET_ENTRIES_BITMAP; idx++) {
    if (pSub->allocated[idx] != 0xFF) break;
  }
  if (idx == STORIO_FID_CACHE_MAX_SUB_BUCKET_ENTRIES_BITMAP) {
    pBucket->full[bbyte] |= (1<<bbit);
  }  
  
  /* 
  ** Sub bucket is not empty
  */
  if (pBucket->empty[bbyte] & (1<<bbit)) {
    pBucket->empty[bbyte] &= ~(1<<bbit);
  }
  storio_fid_cache_stat.count++;
  return 0;
}
/*
**______________________________________________________________________________
** Search or remove a FID context reference in the cache 
** 
** @param hash   A 32 bits hash value of the search entry
** @param key    Pointer to the key for exact match 
** @param search Whether it is a search or remove
**
** @retval    The index found in the cache
** @retval    -1 when no entry found
*/
static inline uint32_t storio_fid_cache_searchOrRemove(uint32_t hash, void * key, int search) {
  STORIO_FID_CACHE_BUCKET_T     * pBucket;
  STORIO_FID_CACHE_SUB_BUCKET_T * pSub;
  STORIO_FID_CACHE_ENTRY_U      * pEntry;
  int                        bbyte, bbit;
  int                        sbyte, sbit;
  int                        subIdx;
  int                        entryIdx;
  uint8_t                    valb;
  uint8_t                    vals;
  int                        collision=0;
  uint32_t                   retval=-1;

  /* 
  ** Get the bucket entry from the hash value
  */
  pBucket = storio_fid_cache;
  pBucket += (hash & ((1<<STORIO_FID_CACHE_LVL0_SZ_POWER_OF_2)-1));
  
  /*
  ** Subhash within the bucket. Keep upper bits for the entry hash in the bucket
  */  
  hash = (hash >> STORIO_FID_CACHE_LVL0_SZ_POWER_OF_2) & ((1<<STORIO_FID_CACHE_HASH_BITS)-1);
  
  /*
  ** look for the entry in not empty sub buckets 
  */
  bbyte = 0;
  for (;bbyte < STORIO_FID_CACHE_MAX_SUB_BUCKET_BITMAP; bbyte++) {
  
    valb = pBucket->empty[bbyte];
    while (valb != 0xFF) {
    
      /*
      ** Get 1rst non empty entry and
      ** set this sub bucket as processed for next loop
      */
      bbit = lowest0[valb];
      valb |= (1<<bbit); 
      
      /*
      ** Get sub bucket entry
      */
      subIdx = 8 * bbyte + bbit;
      if (subIdx==0) pSub = &pBucket->bucket0;
      else           pSub = pBucket->xtra_bucket[subIdx-1]; 
      if (pSub == 0) {
        severe("Empty sub bucket");
	pBucket->empty[bbyte] &= (1<<bbit);
        continue;
      }	 	
  
      /*
      ** Check for allocated entries in the sub bucket
      */
      for (sbyte=0; sbyte < STORIO_FID_CACHE_MAX_SUB_BUCKET_ENTRIES_BITMAP; sbyte++) {
      
        vals = pSub->allocated[sbyte];
	while (vals != 0) {

	  /*
	  ** Get 1rst allocated entry and
	  ** set this entry as processed for next loop
	  */
	  sbit = lowest1[vals];
	  vals &= ~(1<<sbit);

	  /*
	  ** Get entry and compare the hash value
	  */
	  entryIdx = 8 * sbyte + sbit;
	  pEntry = &pSub->entry[entryIdx];
	  if (pEntry->s.hash != hash) {
	    continue;
	  }  

	  /*
	  ** Hash match so compare the total key value
	  */
	  if (!storio_fid_exact_match(key,pEntry->s.index)) { 
	    collision++;
            if (collision>storio_fid_cache_stat.mxcol) {
	      storio_fid_cache_stat.mxcol = collision;
	    }	    
            continue;
	  }
	  	  
	  storio_fid_cache_stat.hit++;	  
	  retval = pEntry->s.index;
	  
	  /*
	  ** It is finished for the case of the search
	  */
	  if (search) {
	    return retval;
	  }

	  /* 
	  ** Case of the remove
	  */
	  pSub->allocated[sbyte] &= ~(1<<sbit);
          storio_fid_cache_stat.count--;

	  /* 
	  ** Is the sub bucket empty now
	  */
	  int idx;
          for (idx=0; idx < STORIO_FID_CACHE_MAX_SUB_BUCKET_ENTRIES_BITMAP; idx++) {
            if (pSub->allocated[idx] != 0) break;
          }
	  if (idx == STORIO_FID_CACHE_MAX_SUB_BUCKET_ENTRIES_BITMAP) {
	    /*
	    ** Bucket is empty now
	    */
	    pBucket->empty[bbyte] |= (1<<bbit);
	    /*
	    ** Free sub bucket. Except the 1rst
	    */
	    if (subIdx != 0) { 
	      free(pSub);
	      storio_fid_cache_stat.bkts--;
	      pBucket->xtra_bucket[subIdx-1] = NULL;
	    }
	  }  

	  /* 
	  ** Sub bucket is not full
	  */
	  if (pBucket->full[bbyte] & (1<<bbit)) {
	    pBucket->full[bbyte] &= ~(1<<bbit);
	  }
	  
	  return retval;
	}
      }
    }
  }
  
  storio_fid_cache_stat.miss++;
  return retval;
}
/*
**______________________________________________________________________________
** Search a FID context reference in the cache 
** 
** @param hash   A 32 bits hash value of the search entry
** @param key    Pointer to the key for exact match 
**
** @retval    The index found in the cache
** @retval    -1 when no entry found
*/
uint32_t storio_fid_cache_search(uint32_t hash, void * key) {
 return storio_fid_cache_searchOrRemove(hash, key, 1);
}
/*
**______________________________________________________________________________
** Remove a FID context reference from the cache 
** 
** @param hash   A 32 bits hash value of the entry to remove
** @param key    Pointer to the key for exact match 
**
** @retval    The index of the removed entry
** @retval    -1 when no entry found
*/
uint32_t storio_fid_cache_remove(uint32_t hash, void * key) {
 return storio_fid_cache_searchOrRemove(hash, key, 0);
}
/*
**______________________________________________________________________________
** Cache initialization 
** 
** @param exact_match_fct     A function to be called for checking the exact match
** @param delete_request_fct  A function to be called to request a context deletion
**                            if possible
**
*/
void storio_fid_cache_init(storio_fid_exact_match_fct exact_match_fct, storio_fid_delete_request_fct delete_request_fct) {
  int count = (1<<STORIO_FID_CACHE_LVL0_SZ_POWER_OF_2);
  int size  = sizeof(STORIO_FID_CACHE_BUCKET_T) * count;
  STORIO_FID_CACHE_BUCKET_T     * pBucket;
  int  i,j;

  /*
  ** Store exact match and delete request functions
  */
  storio_fid_exact_match = exact_match_fct;
  storio_fid_delete_request = delete_request_fct;

  storio_fid_cache_init_lowest_arrays();

  /*
  ** Allocate cache table
  */
  storio_fid_cache = malloc(size);
  if (storio_fid_cache == NULL) {
    fatal(" out of memory %d", size);   
  }
  memset(storio_fid_cache,0,size);

  /*
  ** init of the buckets
  */
  pBucket = storio_fid_cache;
  for (i = 0; i < count; i++, pBucket++) {
    for (j=0; j < STORIO_FID_CACHE_MAX_SUB_BUCKET_BITMAP; j++) {
      pBucket->empty[j] = 0xFF;
    }
  }
}
