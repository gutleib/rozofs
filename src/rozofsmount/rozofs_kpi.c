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

#include "rozofs_kpi.h"
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/list.h>
#include <rozofs/common/htable.h>

rozofs_file_kpi_t *rzkpi_all_stats_p = NULL;
rzkpi_lv2_cache_t rzkpi_cache;
int rzkpi_service_enable = 0;
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
char * show_rzkpi_cache_entry(rozofs_file_kpi_t *entry, char * pChar) {

  rozofs_uuid_unparse(entry->fid,pChar);
  pChar += 36;
  *pChar++ = ';';
  
  pChar += sprintf(pChar, "%llu;%llu;%llu;%llu;%llu;%llu;%llu\n",
           (long long unsigned int)entry->read_bytes_cpt,
           (long long unsigned int)entry->write_bytes_cpt,
           (long long unsigned int)entry->read_io_cpt,
           (long long unsigned int)entry->write_io_cpt,
           (long long unsigned int)entry->opened_file_count,
           (long long unsigned int)entry->created_file_count,
           (long long unsigned int)entry->deleted_file_count);
  return pChar;		   
}
/*
**__________________________________________________________________
*/
char * show_rzkpi_cache(rzkpi_lv2_cache_t *cache, char * pChar) {


  pChar += sprintf(pChar, "lv2 attributes cache : current/max %u/%u\n",cache->size, cache->max);
  pChar += sprintf(pChar, "hit %llu / miss %llu / lru_del %llu\n",
                   (long long unsigned int) cache->hit, 
		   (long long unsigned int)cache->miss,
		   (long long unsigned int)cache->lru_del);
  pChar += sprintf(pChar, "entry size %u - current size %u - maximum size %u\n", 
                   (unsigned int) sizeof(rozofs_file_kpi_t), 
		   (unsigned int)sizeof(rozofs_file_kpi_t)*cache->size, 
		   (unsigned int)sizeof(rozofs_file_kpi_t)*cache->max);

  memset(cache->hash_stats,0,sizeof(uint64_t)*RZKPI_LV2_MAX_LOCK);
  return pChar;		   
}
/*
**__________________________________________________________________
*/
char *rzkpi_display_entries(char *pChar,int cnt,rzkpi_lv2_cache_t *cache)
{
   int count;
   list_t *bucket, *p;
   rozofs_file_kpi_t *lru; 
     
   bucket = &cache->lru;        
   count = 0;
 
   list_for_each_forward(p, bucket) 
   {
       lru = list_entry(p, rozofs_file_kpi_t, list);
       pChar =show_rzkpi_cache_entry(lru,pChar);
       count++;
       if (cnt== count) break;
   }
   pChar +=sprintf(pChar,"\nCount %d\n",count);
   return pChar;
}

static char * show_rzkpi_file_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"file_kpi [enable|disable] : enable/disable the file KPI service\n");
  pChar += sprintf(pChar,"file_kpi display [count]  : display the content of the cache\n");
  pChar += sprintf(pChar,"file_kpi                  : display statistics\n");
  return pChar; 
}


void show_rzkpi_file(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int ret;
    *pChar = 0;
    uint32_t count;

    if (argv[1] == NULL) {
      pChar += sprintf(pChar,"File KPI: %s\n",(rzkpi_service_enable==1)?"Enabled":"Disabled");
      pChar = show_rzkpi_cache(&rzkpi_cache,pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
      return;
    }

    if (strcmp(argv[1],"enable")==0) {
      rzkpi_service_enable = 1;
      pChar += sprintf(pChar,"File KPI service is now enabled");
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
      return;
    }   
    if (strcmp(argv[1],"disable")==0) {
      rzkpi_service_enable = 0;
      pChar += sprintf(pChar,"File KPI service is now disabled");
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
      return;
    }  
    if (strcmp(argv[1],"display")==0) {

      if (argv[2] == NULL) {
        rzkpi_display_entries(pChar,RZKPI_LV2_MAX_ENTRIES,&rzkpi_cache);
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;	 
      }
      ret = sscanf(argv[2], "%d", &count);
      if (ret != 1) {
        show_rzkpi_file_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;   
      }
      rzkpi_display_entries(pChar,count,&rzkpi_cache);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
      return;
    }
    show_rzkpi_file_help(pChar);
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
void rzkpi_cache_initialize(rzkpi_lv2_cache_t *cache) {
    cache->max = RZKPI_LV2_MAX_ENTRIES;
    cache->size = 0;
    cache->hit  = 0;
    cache->miss = 0;
    cache->lru_del = 0;
    list_init(&cache->lru);
    htable_initialize(&cache->htable,RZKPI_LV2_BUKETS , lv2_hash, lv2_cmp);
    memset(cache->hash_stats,0,sizeof(uint64_t)*RZKPI_LV2_MAX_LOCK);

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
static inline void rzkpi_lv2_cache_unlink(rzkpi_lv2_cache_t *cache,rozofs_file_kpi_t *entry) {

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
static inline void rzkpi_lv2_cache_update_lru(rzkpi_lv2_cache_t *cache, rozofs_file_kpi_t *entry) {
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
rozofs_file_kpi_t *rzkpi_lv2_cache_get(rzkpi_lv2_cache_t *cache, fid_t fid) 
{
    rozofs_file_kpi_t *entry = 0;

    if ((entry = htable_get(&cache->htable, fid)) != 0) {
        // Update the lru
        rzkpi_lv2_cache_update_lru(cache,entry); 
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

void rzkpi_lv2_cache_put(rzkpi_lv2_cache_t *cache, fid_t fid,int bytes_count,int operation) 
{
    rozofs_file_kpi_t *entry;
    int count=0;

    entry = malloc(sizeof(rozofs_file_kpi_t));
    if (entry == NULL)
    {
       return;
    }
    memset(entry,0,sizeof(rozofs_file_kpi_t));
    list_init(&entry->list);
    memcpy(&entry->fid,fid,sizeof(fid_t));
    /*
    ** init of the hash_entry
    */
    entry->he.key = entry->fid;
    entry->he.value = entry;
    switch (operation)
    {
      case RZKPI_READ:
	entry->read_bytes_cpt +=bytes_count;
	entry->read_io_cpt +=1;
	break;
      case RZKPI_WRITE:
	entry->write_bytes_cpt +=bytes_count;
	entry->write_io_cpt +=1; 
	break;   
      case RZKPI_OPEN:
	entry->opened_file_count +=1; 
	break;  
      case RZKPI_CREATE:
	entry->opened_file_count +=1; 
	entry->created_file_count +=1; 
	break;  
      case RZKPI_DELETE:
	entry->deleted_file_count +=1; 
	break;  
      case RZKPI_CLOSE:
	entry->opened_file_count -=1; 
	if (entry->opened_file_count < 0) entry->opened_file_count = 0;
	break;  
    }
    /*
    ** Try to remove older entries
    */
    count = 0;
    while ((cache->size >= cache->max) && (!list_empty(&cache->lru))){ 
      rozofs_file_kpi_t *lru;
		
	  lru = list_entry(cache->lru.prev, rozofs_file_kpi_t, list);             
	  htable_del_entry(&cache->htable, &lru->he);
	  rzkpi_lv2_cache_unlink(cache,lru);
	  cache->lru_del++;

	  count++;
	  if (count >= 3) break;
    }
    /*
    ** Insert the new entry
    */
    rzkpi_lv2_cache_update_lru(cache,entry);
    htable_put_entry(&cache->htable,&entry->he);
    cache->size++;    
}

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
void _rzkpi_file_stat_update(fid_t fid,int bytes_count,int operation)
{
   rozofs_file_kpi_t *entry;

  if (rzkpi_all_stats_p == NULL) return;

  switch (operation)
  {
    case RZKPI_READ:
      rzkpi_all_stats_p->read_bytes_cpt +=bytes_count;
      rzkpi_all_stats_p->read_io_cpt +=1;
      break;
    case RZKPI_WRITE:
      rzkpi_all_stats_p->write_bytes_cpt +=bytes_count;
      rzkpi_all_stats_p->write_io_cpt +=1; 
      break;   
    case RZKPI_OPEN:
      rzkpi_all_stats_p->opened_file_count +=1; 
      break;  
    case RZKPI_CREATE:
      rzkpi_all_stats_p->opened_file_count +=1; 
      rzkpi_all_stats_p->created_file_count +=1; 
      break;  
    case RZKPI_DELETE:
      rzkpi_all_stats_p->deleted_file_count +=1; 
      break;  
    case RZKPI_CLOSE:
      rzkpi_all_stats_p->opened_file_count -=1; 
      if (rzkpi_all_stats_p->opened_file_count < 0) rzkpi_all_stats_p->opened_file_count = 0;
      break;  
  }

  /*
  ** Get the entry from caceh or allocate an entry if none is found
  */
  if (!(entry = rzkpi_lv2_cache_get(&rzkpi_cache, fid))) 
  {
     /*
     ** not cached, find it an cache it
     */
     rzkpi_lv2_cache_put(&rzkpi_cache, fid,bytes_count,operation);
     return; 
  }
  /*
  ** we got the entry: update the context
  */
  switch (operation)
  {
    case RZKPI_READ:
      entry->read_bytes_cpt +=bytes_count;
      entry->read_io_cpt +=1;
      break;
    case RZKPI_WRITE:
      entry->write_bytes_cpt +=bytes_count;
      entry->write_io_cpt +=1; 
      break;   
    case RZKPI_OPEN:
      entry->opened_file_count +=1; 
      break;  
    case RZKPI_CREATE:
      entry->opened_file_count +=1; 
      entry->created_file_count +=1; 
      break;  
    case RZKPI_DELETE:
      entry->deleted_file_count +=1; 
      break;  
    case RZKPI_CLOSE:
      entry->opened_file_count -=1; 
      if (entry->opened_file_count < 0) entry->opened_file_count = 0;
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
int rzkpi_file_service_init()
{
   rzkpi_service_enable = 0;
   rzkpi_cache_initialize(&rzkpi_cache);
   
   rzkpi_all_stats_p = malloc(sizeof(rozofs_file_kpi_t));
   if (rzkpi_all_stats_p == NULL) return -1;
   memset(rzkpi_all_stats_p,0,sizeof(rozofs_file_kpi_t));
   uma_dbg_addTopic("file_kpi",show_rzkpi_file);
   return 0;
}
