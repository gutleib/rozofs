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

#include "rozofs_cachetrack.h"
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/list.h>
#include <rozofs/common/htable.h>
#include "rozofsmount.h"

rzcachetrack_lv2_cache_t rzcachetrack_cache;
int rzcachetrack_service_enable = 0;
rzcachetrack_conf_t rzcachetrack_conf = {
   20,              /* min_children             */
   2*1024*1024,     /* min_filesize             */
   100*1024*1024,   /* max_filesize             */
   8,               /* opencount                */
   2,            /* openperiod_in_ms         */
   5,            /* min_creation_delay_in_ms */
   3600*2,          /* cachedelay_in_sec        */
};

/*
**__________________________________________________________________
*/
/**
 * hashing function used to find lv2 entry in the cache
 */
static inline uint32_t lv2_hash(void *key) {
    uint32_t       hash = 0;
    uint8_t       *c;
    int            i;
    rozofs_inode_t fake_inode;
    
    /*
    ** Clear recycle counter in key (which is a FID)
    */
    memcpy(&fake_inode,key,sizeof(rozofs_inode_t));
    rozofs_reset_recycle_on_fid(&fake_inode);
    /*
    ** clear the delete pending bit
    */
    fake_inode.s.del = 0;

    c = (uint8_t *) &fake_inode;
    for (i = 0; i < sizeof(rozofs_inode_t); c++,i++)
        hash = *c + (hash << 6) + (hash << 16) - hash;
    return hash;
}
/*
**__________________________________________________________________
*/
static inline int lv2_cmp(void *k1, void *k2) {
    rozofs_inode_t fake_inode1;
    rozofs_inode_t fake_inode2;  
      
    /*
    ** Clear recycle counter in keys (which are FIDs)
    */
    memcpy(&fake_inode1,k1,sizeof(rozofs_inode_t));
    rozofs_reset_recycle_on_fid(&fake_inode1);
    fake_inode1.s.del = 0;
    memcpy(&fake_inode2,k2,sizeof(rozofs_inode_t));
    rozofs_reset_recycle_on_fid(&fake_inode2);
    fake_inode2.s.del = 0;    
    return uuid_compare((uint8_t*)&fake_inode1, (uint8_t*)&fake_inode2);
}

/*
**__________________________________________________________________
*/
char * show_rzcachetrack_cache_entry(rozofs_file_cachetrack_t *entry, char * pChar) {

  rozofs_uuid_unparse(entry->fid,pChar);
  pChar += 36;
  *pChar++ = ';';
  
  pChar += sprintf(pChar, "%llu;%u;%d;%d;%llu\n",
           (long long unsigned int)entry->maxfilesize,
            entry->children,
            entry->state,
            entry->opencount,
           (long long unsigned int)entry->timestamp);
  return pChar;		   
}
/*
**__________________________________________________________________
*/
char * show_rzcachetrack_cache(rzcachetrack_lv2_cache_t *cache, char * pChar) {


  pChar += sprintf(pChar, "lv2 attributes cache : current/max %u/%u\n",cache->size, cache->max);
  pChar += sprintf(pChar, "hit %llu / miss %llu / lru_del %llu\n",
                   (long long unsigned int) cache->hit, 
		   (long long unsigned int)cache->miss,
		   (long long unsigned int)cache->lru_del);
  pChar += sprintf(pChar, "entry size %u - current size %u - maximum size %u\n", 
                   (unsigned int) sizeof(rozofs_file_cachetrack_t), 
		   (unsigned int)sizeof(rozofs_file_cachetrack_t)*cache->size, 
		   (unsigned int)sizeof(rozofs_file_cachetrack_t)*cache->max);

  memset(cache->hash_stats,0,sizeof(uint64_t)*RZCACHETRACK_LV2_MAX_LOCK);
  return pChar;		   
}
/*
**__________________________________________________________________
*/
char *rzcachetrack_display_entries(char *pChar,int cnt,rzcachetrack_lv2_cache_t *cache)
{
   int count;
   list_t *bucket, *p;
   rozofs_file_cachetrack_t *lru; 
     
   bucket = &cache->lru;        
   count = 0;
 
   list_for_each_forward(p, bucket) 
   {
       lru = list_entry(p, rozofs_file_cachetrack_t, list);
       pChar =show_rzcachetrack_cache_entry(lru,pChar);
       count++;
       if (cnt== count) break;
   }
   pChar +=sprintf(pChar,"\nCount %d\n",count);
   return pChar;
}

static char * show_rzcachetrack_file_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"dir_cachetrack [enable|disable] : enable/disable the file caching service\n");
  pChar += sprintf(pChar,"dir_cachetrack display [count]  : display the content of the cache\n");
  pChar += sprintf(pChar,"dir_cachetrack                  : display statistics\n");
  return pChar; 
}


void show_rzcachetrack_file(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int ret;
    *pChar = 0;
    uint32_t count;

    if (argv[1] == NULL) {
      pChar += sprintf(pChar,"File caching: %s\n",(rzcachetrack_service_enable==1)?"Enabled":"Disabled");
      pChar = show_rzcachetrack_cache(&rzcachetrack_cache,pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
      return;
    }

    if (strcmp(argv[1],"enable")==0) {
      rzcachetrack_service_enable = 1;
      pChar += sprintf(pChar,"File caching service is now enabled");
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
      return;
    }   
    if (strcmp(argv[1],"disable")==0) {
      rzcachetrack_service_enable = 0;
      pChar += sprintf(pChar,"File caching service is now disabled");
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
      return;
    }  
    if (strcmp(argv[1],"display")==0) {

      if (argv[2] == NULL) {
        rzcachetrack_display_entries(pChar,RZCACHETRACK_LV2_MAX_ENTRIES,&rzcachetrack_cache);
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;	 
      }
      ret = sscanf(argv[2], "%d", &count);
      if (ret != 1) {
        show_rzcachetrack_file_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;   
      }
      rzcachetrack_display_entries(pChar,count,&rzcachetrack_cache);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
      return;
    }
    show_rzcachetrack_file_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  	  
    return;
}

/*
**__________________________________________________________________
*/
/**
*   init of an exportd attribute cache

    @param: pointer to the cache context
    
    @retval none
*/
void rzcachetrack_cache_initialize(rzcachetrack_lv2_cache_t *cache) {
    cache->max = RZCACHETRACK_LV2_MAX_ENTRIES;
    cache->size = 0;
    cache->hit  = 0;
    cache->miss = 0;
    cache->lru_del = 0;
    list_init(&cache->lru);
    htable_initialize(&cache->htable,RZCACHETRACK_LV2_BUKETS , lv2_hash, lv2_cmp);
    memset(cache->hash_stats,0,sizeof(uint64_t)*RZCACHETRACK_LV2_MAX_LOCK);

}

/*
**__________________________________________________________________
*/
/**
*   Remove an entry from the file kpi cache

    @param: pointer to the cache context
    @param: pointer to entry to remove
    
    @retval none
*/
static inline void rzcachetrack_lv2_cache_unlink(rzcachetrack_lv2_cache_t *cache,rozofs_file_cachetrack_t *entry) {

  list_remove(&entry->list);
  free(entry);
  cache->size--;  
}
/*
 *___________________________________________________________________
 * Put the entry in front of the lru list when no lock is set
 *
 * @param cache: the cache context
 * @param entry: the cache entry
 *___________________________________________________________________
 */
static inline void rzcachetrack_lv2_cache_update_lru(rzcachetrack_lv2_cache_t *cache, rozofs_file_cachetrack_t *entry) {
    list_remove(&entry->list);
    list_push_front(&cache->lru, &entry->list);    
}
/*
**__________________________________________________________________
*/
/**
*   Get an enry from the file kpi cache

    @param: pointer to the cache context
    @param: fid : key of the element to find
    
    @retval <>NULL : pointer to the cache entry that contains the attributes
    @retval NULL: not found
*/
rozofs_file_cachetrack_t *rzcachetrack_lv2_cache_get(rzcachetrack_lv2_cache_t *cache, fid_t fid) 
{
    rozofs_file_cachetrack_t *entry = 0;

    if ((entry = htable_get(&cache->htable, fid)) != 0) {
        // Update the lru
        rzcachetrack_lv2_cache_update_lru(cache,entry); 
	cache->hit++;
    }
    else {
      cache->miss++;
    }
    return entry;
}
/*
**__________________________________________________________________
*/
/**
*   The purpose of that service is to store object attributes in file kpi cache

  @param cache : pointer to the export attributes cache
  @param fid : unique identifier of the object
  @param bytes_count: number of byte to read or write
  @param operation: 1: read/ 0 write  
  
  @retval none
*/

void rzcachetrack_lv2_cache_put(rzcachetrack_lv2_cache_t *cache, fid_t fid) 
{
    rozofs_file_cachetrack_t *entry;
    int count=0;

    entry = malloc(sizeof(rozofs_file_cachetrack_t));
    if (entry == NULL)
    {
       return;
    }
    memset(entry,0,sizeof(rozofs_file_cachetrack_t));
    list_init(&entry->list);
    memcpy(&entry->fid,fid,sizeof(fid_t));
    /*
    ** init of the hash_entry
    */
    entry->he.key = entry->fid;
    entry->he.value = entry;
    /*
    ** Try to remove older entries
    */
    count = 0;
    while ((cache->size >= cache->max) && (!list_empty(&cache->lru))){ 
      rozofs_file_cachetrack_t *lru;
		
	  lru = list_entry(cache->lru.prev, rozofs_file_cachetrack_t, list);             
	  htable_del_entry(&cache->htable, &lru->he);
	  rzcachetrack_lv2_cache_unlink(cache,lru);
	  cache->lru_del++;

	  count++;
	  if (count >= 3) break;
    }
    /*
    ** Insert the new entry
    */
    rzcachetrack_lv2_cache_update_lru(cache,entry);
    htable_put_entry(&cache->htable,&entry->he);
    cache->size++;    
}




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
void _rzcachetrack_file(fid_t fid,uint64_t filesize,uint64_t cr8time)
{
   rozofs_file_cachetrack_t *entry;
   uint32_t children;
   ientry_t *ie = NULL;
   uint64_t curtime = time(NULL);
   
   ie = get_ientry_by_fid(fid);
   if (ie == NULL) return;
   
   children = ie->attrs.attrs.children;
   /*
   ** check if the number of children matches the minimum threshold
   */
   if (children < rzcachetrack_conf.min_children) return;
   /*
   ** Check if the minimum file size has been reached
   */
   if (filesize < rzcachetrack_conf.min_filesize) return;
   /*
   ** Check if the max file size has been reached
   */
   if (filesize > rzcachetrack_conf.max_filesize) return;  
   /*
   ** check the creation time of the file: caution: here it is assumed that the exportd and
   ** the rozofsmount are almost synced
   */
   if ( cr8time >= (curtime + rzcachetrack_conf.min_creation_delay_in_ms)) 
   {
      return;
   }
   /*
   ** Get the entry from caceh or allocate an entry if none is found
   */
   if (!(entry = rzcachetrack_lv2_cache_get(&rzcachetrack_cache, fid))) 
   {
      /*
      ** not cached, find it an cache it
      */
      rzcachetrack_lv2_cache_put(&rzcachetrack_cache,fid);
      return; 
   }
   /*
   ** OK we have the entry ,so proceed according to the current state of the cache entry
   */
   switch (entry->state)
   {
      case RZCACHE_IDLE_S:
	/*
	** Get the current time stamp
	*/
	entry->timestamp = curtime;
	entry->children = children;
	entry->timestamp += rzcachetrack_conf.openperiod_in_ms;
	entry->opencount = 0;
	entry->maxfilesize = filesize;
	entry->state = RZCACHE_PREPARE2CACHE_S;
      break;
      
      case RZCACHE_PREPARE2CACHE_S:
	/*
	** check if the open rate matches
	*/
	entry->children = children;
	entry->opencount +=1;
	if (filesize > entry->maxfilesize) entry->maxfilesize = filesize;
	curtime = time(NULL);
	if (curtime < entry->timestamp)
	{
          /*
	  ** nothing more to do
	  */
	  return;
	}
	/*
	** check if we get the required opening rate
	*/
	if (entry->opencount < rzcachetrack_conf.opencount)
	{
           /*
	   ** not the good rate, return to idle state
	   */
	   entry->state = RZCACHE_IDLE_S;
	   return;
	}
	/*
	** OK, we have all the condition for caching the file within that directory
	*/
	entry->timestamp = curtime;
	entry->timestamp += rzcachetrack_conf.cachedelay_in_sec;   
	entry->state = RZCACHE_CACHED_S;  
      break; 
      
      case RZCACHE_CACHED_S:
        /*
	** rearm the guard timer
	*/
	entry->children = children;
	entry->timestamp = curtime;
	entry->timestamp += rzcachetrack_conf.cachedelay_in_sec;   
	break;
      default:
        break;
    }	
}

/*
**____________________________________________________________
*/
/**
*   Init of the kpi service 


   @retval 0 on success
   @retval -1 on error
*/
int rzcachetrack_file_service_init()
{
   rzcachetrack_service_enable = 1;
   rzcachetrack_cache_initialize(&rzcachetrack_cache);
   
   uma_dbg_addTopic("dir_cachetrack",show_rzcachetrack_file);
   return 0;
}
