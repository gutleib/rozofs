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
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/htable.h>
#include <rozofs/common/log.h>
#include "rozofs_quota.h"
#include "rozofs_quota_api.h"
#include <rozofs/core/disk_table_service.h>
#include <rozofs/common/types.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rpc/export_profiler.h>
 
/*
** quota file default names
*/ 
char *quotatypes[] = INITQFNAMES;
char *quotainfotypes[] = ROZOFS_INITQFNAMES;

rozofs_qt_export_t *export_quota_table[EXPGW_EID_MAX_IDX+1] = {0};
int rozofs_qt_init_done = 0;
rozofs_qt_cache_t rozofs_qt_cache;  /**< quota cache */

/*
 *_______________________________________________________________________
 */
char * rozofs_qt_cache_display(rozofs_qt_cache_t *cache, char * pChar) {

  pChar += sprintf(pChar, "quota cache : current/max %u/%u\n",cache->size, cache->max);
  pChar += sprintf(pChar, "hit %llu / miss %llu / lru_del %llu\n",
                   (long long unsigned int) cache->hit, 
		   (long long unsigned int)cache->miss,
		   (long long unsigned int)cache->lru_del);
  pChar += sprintf(pChar, "entry size %u - current size %u - maximum size %u\n", 
                   (unsigned int) sizeof(rozofs_qt_cache_entry_t), 
		   (unsigned int)sizeof(rozofs_qt_cache_entry_t)*cache->size, 
		   (unsigned int)sizeof(rozofs_qt_cache_entry_t)*cache->max); 
  return pChar;		   
}

/*
 *_______________________________________________________________________
 */
/**
*   Quota cache statistics

  @param argv : standard argv[] params of debug callback
  @param tcpRef : reference of the TCP debug connection
  @param bufRef : reference of an output buffer 
  
  @retval none
*/
void show_quota_cache(char * argv[], uint32_t tcpRef, void *bufRef) {
  rozofs_qt_cache_display( &rozofs_qt_cache, uma_dbg_get_buffer());
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}


static char * rw_quota_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"quota_get eid <eid> {group|user} <value>: get user or group quota within an eid\n");
  return pChar; 
}

/*
 *_______________________________________________________________________
 */
/**
*   Read quota user or group

  @param argv : standard argv[] params of debug callback
  @param tcpRef : reference of the TCP debug connection
  @param bufRef : reference of an output buffer 
  
  @retval none
*/
void rw_quota_entry(char * argv[], uint32_t tcpRef, void *bufRef) {
  
    char *pChar = uma_dbg_get_buffer();
    int eid;
    int type;
    int qid;
    rozofs_qt_cache_entry_t *dquot=NULL;
    rozofs_quota_key_t key;
    rozofs_qt_export_t *p;
    int ret;

      if (argv[1] == NULL) {
	rw_quota_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;  	  
    }

    if (strcmp(argv[1],"eid")==0) 
    {   
      if (argv[2] == NULL) 
      {
	rw_quota_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;  	  
      }
      ret = sscanf(argv[2], "%d", &eid);
      if (ret != 1) 
      {
        rw_quota_help(pChar);	
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
        return;   
      }
    }
    else
    {
      rw_quota_help(pChar);	
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;       
    }
    while(1)
    {
      if (argv[3] == NULL) 
      {
	rw_quota_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;  	  
      }
      if (strcmp(argv[3],"user")==0) {   
           type = USRQUOTA;
	   break;
      }
      if (strcmp(argv[3],"group")==0) {   
           type = GRPQUOTA;
	   break;
      }      
      if (strcmp(argv[3],"share")==0) {   
           type = SHRQUOTA;
	   break;
      }      
      rw_quota_help(pChar);	
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;  
    }
    /*
    ** check the id
    */
    if (argv[4] == NULL) {
      rw_quota_help(pChar);	
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;  	  
    }
    ret = sscanf(argv[4], "%d", &qid);
    if (ret != 1) {
      rw_quota_help(pChar);	
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;   
    }
    /*
    ** check the presence of the exportd
    */
    if (rozofs_qt_init_done == 0)
    {
      sprintf(pChar,"Tne Quota module is not active\n");
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;
    }
    if (eid > EXPGW_EID_MAX_IDX)
    {
      sprintf(pChar,"Tne EID is out of range (max is %u)\n",EXPGW_EID_MAX_IDX);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;
    }
    if (export_quota_table[eid]== NULL)
    {
      sprintf(pChar,"Quota does not exist for EID %d\n",eid);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;
    }
    /*
    ** attempt to read the quota
    */
    p = export_quota_table[eid];
    key.u64 = 0;
    key.s.qid = qid;
    key.s.eid = eid;
    key.s.type = type;    
    dquot = rozofs_qt_cache_get (&rozofs_qt_cache,p->quota_inode[type],&key);
    if (dquot == NULL)
    {
      sprintf(pChar,"Cannot read for the requested id\n");
    }
    else
    {
      pChar +=sprintf(pChar,"Displaying quota for %s %u of exportd %d\n",quotatypes[type],qid,eid);
      rozofs_qt_print(pChar,&dquot->dquot);
    }
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}

/**
 * hashing function used to find lv2 entry in the cache
 */
static inline uint32_t rozofs_qt_hash(void *key) {
    uint32_t hash = 0;
    uint8_t *c;

    for (c = key; c != key + sizeof(rozofs_quota_key_t); c++)
        hash = *c + (hash << 6) + (hash << 16) - hash;
    return hash;
}

static inline int rozofs_qt_cmp(void *k1, void *k2) {
   
  return memcmp((const void*)k1,(const void *)k2,sizeof(rozofs_quota_key_t));
   
}

/*
 *___________________________________________________________________
 * Put the entry in front of the lru list when no lock is set
 *
 * @param cache: the cache context
 * @param entry: the cache entry
 *___________________________________________________________________
 */
static inline void rozofs_qt_cache_update_lru(rozofs_qt_cache_t *cache, rozofs_qt_cache_entry_t *entry) {
    list_remove(&entry->list);
    list_push_front(&cache->lru, &entry->list);
}
/*
**__________________________________________________________________
*/
/**
*   Remove an entry from the quota cache

    @param: pointer to the cache context
    @param: pointer to entry to remove
    
    @retval none
*/
static inline void rozofs_qt_cache_unlink(rozofs_qt_cache_t *cache,rozofs_qt_cache_entry_t *entry) {

  list_remove(&entry->list);
  free(entry);
  cache->size--;  
}

/*
**__________________________________________________________________
*/
/**
*   allocate a quota cache entry

    @param: pointer to the cache context
    @param: pointer to entry to remove
    
    @retval none
*/
rozofs_qt_cache_entry_t *rozofs_qt_cache_entry_alloc() 
{

    rozofs_qt_cache_entry_t *entry=NULL;
    
    entry = malloc(sizeof(rozofs_qt_cache_entry_t));
    if (entry == NULL)
    {
      /*
      ** out of memory
      */
      severe("Out of memory");
      return NULL;       
    }  
    memset(entry,0,sizeof(rozofs_qt_cache_entry_t));
    list_init(&entry->list);
    return entry;
}

/*
**__________________________________________________________________
*/
/**
*   init of a QUOTA cache

    @param: pointer to the cache context
    
    @retval none
*/
void rozofs_qt_cache_initialize(rozofs_qt_cache_t *cache) {
    cache->max = ROZOFS_QT_MAX_ENTRIES;
    cache->size = 0;
    cache->hit  = 0;
    cache->miss = 0;
    cache->lru_del = 0;
    list_init(&cache->lru);
    htable_initialize(&cache->htable, ROZOFS_QT_BUKETS, rozofs_qt_hash, rozofs_qt_cmp);
}

/*
**__________________________________________________________________
*/
/**
*   delete of an exportd attribute cache

    @param: pointer to the cache context
    
    @retval none
*/
void rozofs_qt_cache_release(rozofs_qt_cache_t *cache) {
    list_t *p, *q;

    list_for_each_forward_safe(p, q, &cache->lru) {
        rozofs_qt_cache_entry_t *entry = list_entry(p, rozofs_qt_cache_entry_t, list);
        htable_del(&cache->htable, &entry->dquot.key.u64);
	rozofs_qt_cache_unlink(cache,entry);
    }
}

/*
**__________________________________________________________________
*/
/**
*   Get an entry from the quota cache

    @param: pointer to the cache context
    @param disk_p: quota disk table associated with the type
    @param: key : the key contains the eid,type and quota identifier within the type
    
    @retval <>NULL : pointer to the cache entry that contains the quota
    @retval NULL: not found
*/
rozofs_qt_cache_entry_t *rozofs_qt_cache_get(rozofs_qt_cache_t *cache,
                                             disk_table_header_t *disk_p,
					     rozofs_quota_key_t *key) 
{
    rozofs_qt_cache_entry_t *entry = 0;
    int ret;
    int count=0;


    if ((entry = htable_get(&cache->htable, key)) != 0) {
        // Update the lru
        rozofs_qt_cache_update_lru(cache,entry); 
	cache->hit++;
    }
    else 
    {
       entry = rozofs_qt_cache_entry_alloc();
       if (entry == NULL)
       {
	 return NULL;       
       }
       entry->dquot.key.u64 = key->u64;
      ret = quota_wbcache_read(disk_p,&entry->dquot,sizeof(rozofs_dquot_t));
      if (ret < 0)
      {
	 /*
	 ** warn but keep the data in cache
	 */
	 severe("quota writeback cache read failure qid/type %u/%u", entry->dquot.key.s.qid, entry->dquot.key.s.type);
	 free(entry);
	 return NULL;
      }
      cache->miss++;
      /*
      ** insert the entry in the cache
      */
      while ((cache->size >= cache->max) && (!list_empty(&cache->lru))){ 
	rozofs_qt_cache_entry_t *lru;

	    lru = list_entry(cache->lru.prev, rozofs_qt_cache_entry_t, list);  
	    htable_del(&cache->htable, &lru->dquot.key.u64);
	    rozofs_qt_cache_unlink(cache,lru);
	    cache->lru_del++;
	    count++;
	    if (count >= 3) break;
      }
      /*
      ** Insert the new entry
      */
      rozofs_qt_cache_update_lru(cache,entry);
      htable_put(&cache->htable, &entry->dquot.key.u64, entry);
      cache->size++;      
    }
    return entry;
}

/*
**__________________________________________________________________
*/
/**
*   The purpose of that service is to read object attributes and store them in the attributes cache

  @param cache : pointer to the export attributes cache
  @param disk_p: quota disk table associated with the type or NULL if cache update only
  @param entry:pointer to the entry to insert (the key must be provisioned)
  
  @retval <> NULL: attributes of the object
  @retval == NULL : no attribute returned for the object (see errno for details)
*/

rozofs_qt_cache_entry_t *rozofs_qt_cache_put(rozofs_qt_cache_t *cache,
                                             disk_table_header_t *disk_p,
                                             rozofs_qt_cache_entry_t *entry) 
{
    /*
    ** push the data in the quota write back cache
    */
    int ret;
    ret = quota_wbcache_write(disk_p,&entry->dquot,sizeof(rozofs_dquot_t));
    if (ret < 0)
    {
       /*
       ** warn but keep the data in cache
       */
       severe("quota writeback cache write failure");
    }
    return entry;
}
/*
**__________________________________________________________________
*/
/**
*   Write the quota info on disk

   @param root_path : root path of the exportd
   @param quota_info_p : pointer to the quota info
   @param type : quota type: USRQUOTA or GRPQUOTA

  @retval 0 on success
  @retval -1 on error (see erno for details
*/
int rozofs_qt_write_quota_info(char *root_path,rozofs_quota_info_t *quota_info_p,int type)
{

   char pathname[ROZOFS_PATH_MAX];
   int fd;
   
   sprintf(pathname,"%s/%s_%s",root_path,ROZOFS_QUOTA_INFO_NAME,quotainfotypes[type]);
   if ((fd = open(pathname, O_RDWR/* | NO_ATIME */| O_CREAT, S_IRWXU)) < 1) 
   {
      severe("cannot open quota info file %s: %s",pathname,strerror(errno));
      return -1;
   }
   if (write(fd, quota_info_p, sizeof (rozofs_quota_info_t)) != sizeof (rozofs_quota_info_t)) 
   {
      severe("write error of quota info file %s: %s",pathname,strerror(errno));
       close(fd);
       return -1;
   }
   close(fd);
   return 0;
}

/*
**__________________________________________________________________
*/
/**
*   Read the quota info from disk

   @param root_path : root path of the exportd
   @param quota_info_p : pointer to the quota info
   @param type : quota type: USRQUOTA or GRPQUOTA

  @retval 0 on success
  @retval -1 on error (see erno for details
*/
int rozofs_qt_read_quota_info(char *root_path,rozofs_quota_info_t *quota_info_p,int type)
{

   char pathname[ROZOFS_PATH_MAX];
   int fd;
   
   sprintf(pathname,"%s/%s_%s",root_path,ROZOFS_QUOTA_INFO_NAME,quotainfotypes[type]);
   if ((fd = open(pathname, O_RDWR,S_IRWXU)) < 1) 
   {
      warning("cannot open quota info file %s: %s",pathname,strerror(errno));
      return -1;
   }
   if (read(fd, quota_info_p, sizeof (rozofs_quota_info_t)) != sizeof (rozofs_quota_info_t)) 
   {
      severe("write error of quota info file %s: %s",pathname,strerror(errno));
       close(fd);
       return -1;
   }
   close(fd);
   return 0;
}

/*
**__________________________________________________________________
*/
/**
*   Set the quota information related to either a group or a user
    
    @param eid: export identifier    
    @param type : user or group
    @param identifier :identifier within the type
    @param sqa_qcmd : bitmap of the information to modify
    @param src : quota parameters
    
    @retval 0 on success
    @retval-1 on error: see errno for details
 */
int rozofs_qt_set_quotainfo(int eid,int type,int identifier,int sqa_cmd, sq_dqblk *src )
{
   rozofs_qt_export_t *p;
   int ret;
   
   export_profiler_eid = 0;
   
   /*
   ** check if the entry already exists: this is done to address the case of the exportd reload
   */
   if (eid > EXPGW_EID_MAX_IDX) 
   {
      /*
      ** eid value is out of range
      */
      errno = ERANGE;
      return  -1;
   }
   export_profiler_eid = eid;
   START_PROFILING(quota_setinfo);   

   if (export_quota_table[eid]== NULL)
   {
     errno = ENOENT;
     goto error;
   }
   p = export_quota_table[eid];
   
   p->quota_super[type].dqi_bgrace = src->rq_btimeleft;
   p->quota_super[type].dqi_igrace = src->rq_ftimeleft;
   /*
   ** write data back on disk
   */
   ret = rozofs_qt_write_quota_info(p->root_path,&p->quota_super[type],type);
   if (ret < 0) goto error;

   STOP_PROFILING(quota_setinfo);
   return 0;

error:
  STOP_PROFILING(quota_setinfo);
  return -1;
  
}

/*
**__________________________________________________________________
*/
/**
*   Set the quota state related to either a group or a user
    
    @param eid: export identifier    
    @param type : user or group
    @param cmd : ROZOFS_QUOTA_ON or ROZOFS_QUOTA_OFF
    
    @retval 0 on success
    @retval-1 on error: see errno for details
 */
int rozofs_qt_set_quotastate(int eid,int type,int cmd)
{
   rozofs_qt_export_t *p;
   int ret;
   
   export_profiler_eid = 0;
   
   /*
   ** check if the entry already exists: this is done to address the case of the exportd reload
   */
   if (eid > EXPGW_EID_MAX_IDX) 
   {
      /*
      ** eid value is out of range
      */
      errno = ERANGE;
      return  -1;
   }
   export_profiler_eid = eid;
   START_PROFILING(quota_setinfo);   

   if (export_quota_table[eid]== NULL)
   {
     errno = ENOENT;
     goto error;
   }
   p = export_quota_table[eid];
   
   p->quota_super[type].enable = cmd;
   /*
   ** write data back on disk
   */
   ret = rozofs_qt_write_quota_info(p->root_path,&p->quota_super[type],type);
   if (ret < 0) goto error;

   STOP_PROFILING(quota_setinfo);
   return 0;

error:
  STOP_PROFILING(quota_setinfo);
  return -1;
  
}




/*
**__________________________________________________________________
*/
/**
*  Export quota table create

   That service is called at export creation time. Its purpose is to allocate
   data structure for export attributes management.
   
   @param eid : export identifier
   @param root_path : root path of the export
   @param create_flag : assert to 1 if quota files MUST be created
   
   @retval <> NULL: pointer to the attributes tracking table
   @retval == NULL : error (see errno for details)
*/
void *rozofs_qt_alloc_context(uint16_t eid, char *root_path, int create)
{
   rozofs_qt_export_t *tab_p = NULL; 
   int ret; 
   int i;
   
   /*
   ** if the init of the quota module has not yet been done do it now
   */

   if (rozofs_qt_init_done == 0)
   {
     severe("Quota Not ready");
     return NULL;
   }
   /*
   ** check if the entry already exists: this is done to address the case of the exportd reload
   */
   if (eid > EXPGW_EID_MAX_IDX) 
   {
      /*
      ** eid value is out of range
      */
      severe("failed to create ressource: eid %d is out of range max is %d",eid,EXPGW_EID_MAX_IDX);
      return NULL;
   }
   if (export_quota_table[eid]!= NULL)
   {
      /*
      ** the context is already allocated: nothing more to done:
      **  note: it is not foreseen the change the root path of an exportd !!
      */
      return (void*)export_quota_table[eid];
   }
   
   tab_p = malloc(sizeof(rozofs_qt_export_t));
   if (tab_p == NULL)
   {
     /*
     ** out of memory
     */
     severe(" Out of memory");
     return NULL;
   }
   memset(tab_p,0,sizeof(rozofs_qt_export_t));
   /*
   ** duplicate the root path
   */
   tab_p->root_path = strdup(root_path);
   if (tab_p->root_path == NULL)
   {
     severe("out of memory");
     goto error;
   }

   /*
   ** set the limit in quota_info
   */
   for (i = 0; i < MAXQUOTAS; i++)
   {
      tab_p->quota_super[i].dqi_maxblimit = 0xffffffffffffffffULL;
      tab_p->quota_super[i].dqi_maxilimit = 0xffffffffffffffffULL;
      tab_p->quota_super[i].dqi_bgrace = ROZOFS_MAX_DQ_TIME;
      tab_p->quota_super[i].dqi_igrace = ROZOFS_MAX_IQ_TIME;
      tab_p->quota_super[i].enable = 1;
   }
   for (i = 0; i < MAXQUOTAS; i++)
   {
     /*
     ** read the quota info file from disk
     */
     ret = rozofs_qt_read_quota_info(root_path,&tab_p->quota_super[i],i);
     if (ret < 0)
     {
	if (errno == ENOENT)
	{
           /*
	   ** create the file
	   */
	   ret = rozofs_qt_write_quota_info(root_path,&tab_p->quota_super[i],i);
	   if (ret < 0)
	   {
	     severe("fail to create quotainfo file for eid %d, quota is disabled by default",eid);
	     tab_p->quota_super[i].enable = 0;
           }
	}

     }
   }
   for (i = 0; i < MAXQUOTAS; i++)
   {
     tab_p->quota_inode[i] = disk_tb_ctx_allocate(root_path,quotatypes[i],sizeof(rozofs_dquot_t),ROZOFS_QUOTA_DISK_TB_SZ_POWER2);
     if (tab_p->quota_inode[i] == NULL) goto error;

   }
   /*
   ** everything is fine, so store the reference of the context in the table at the index of the eid
   */
   export_quota_table[eid]= tab_p;
   
   return (void*)tab_p;

error:
   warning("rozofs_qt_release_context(tab_p) not yet implemented");
   return NULL;
}

/*
**__________________________________________________________________
*/

static inline int rozofs_qt_dqot_inode_update(disk_table_header_t *disk_p,
                                              int eid,int type,int qid,
					      uint64_t count,int action,
                                              rozofs_quota_info_t *quota_info_p)
{
    rozofs_qt_cache_entry_t *dquot;
    rozofs_quota_key_t key;
    
    key.u64 = 0;
    key.s.qid = qid;
    key.s.eid = eid;
    key.s.type = type;
    
    int status = -1;
    while (1)
    {
      dquot = rozofs_qt_cache_get (&rozofs_qt_cache,disk_p,&key);
      if (dquot == NULL)
      {
        /*
	** should not happen
	*/
        break;
      }
      if (action == ROZOFS_QT_INC)
      {
        dquot->dquot.quota.dqb_curinodes +=count; 
      }  
      else
      {
        dquot->dquot.quota.dqb_curinodes -=count; 
	if (dquot->dquot.quota.dqb_curinodes<0)
	{
	  dquot->dquot.quota.dqb_curinodes =0; 
	} 
      } 
      /*
      ** update the grace period if needed
      */
       rozofs_quota_update_grace_times_inodes(&dquot->dquot.quota,quota_info_p);      
      /*
      ** let's write qota on disk
      */ 
      rozofs_qt_cache_put(&rozofs_qt_cache,disk_p,dquot);  
      status = 0;
      break; 
    } 
    return status;
}
/*
**__________________________________________________________________
*/
/**
*   update inode quota
    
    @param eid: export identifier
    
    @param usr_id : user quota
    @param grp_id : group quota
    @param nb_inode
    @param action: 1: increment, 0 decrement 
    @param share: share identifier (0 if none)
    
    @retval : 0 on success
    @retval < 0 on error
 */
int rozofs_qt_inode_update(int eid,int user_id,int grp_id,int nb_inode,int action,uint16_t share)
{
   rozofs_qt_export_t *p;
   /*
   ** get the pointer to the quota context associated with the eid
   */
   /*
   ** check if the entry already exists: this is done to address the case of the exportd reload
   */
   if (eid > EXPGW_EID_MAX_IDX) 
   {
      /*
      ** eid value is out of range
      */
      return  -1;
   }
   if (export_quota_table[eid]== NULL)
   {
      return -1;
   }
   p = export_quota_table[eid];
   /*
   ** update user account
   */
   if (user_id != -1)
   {
      rozofs_qt_dqot_inode_update(p->quota_inode[USRQUOTA],eid,USRQUOTA,user_id,nb_inode,action,
                                  &p->quota_super[USRQUOTA]);
   }
   if (grp_id != -1)
   {
      rozofs_qt_dqot_inode_update(p->quota_inode[GRPQUOTA],eid,GRPQUOTA,grp_id,nb_inode,action,
                                  &p->quota_super[GRPQUOTA]);
   }
   if (share !=0)
   {
      rozofs_qt_dqot_inode_update(p->quota_inode[SHRQUOTA],eid,SHRQUOTA,(int)share,nb_inode,action,
                                  &p->quota_super[SHRQUOTA]);   
   }
   return 0;

}

/*
**__________________________________________________________________
*/

static inline int rozofs_qt_dqot_block_update(disk_table_header_t *disk_p,
                                              int eid,int type,int qid,
					      uint64_t count,int action,
					      rozofs_quota_info_t *quota_info_p
					      )
{
    rozofs_qt_cache_entry_t *dquot;
    rozofs_quota_key_t key;
    
    key.u64 = 0;
    key.s.qid = qid;
    key.s.eid = eid;
    key.s.type = type;
    
    int status = -1;
    while (1)
    {
      dquot = rozofs_qt_cache_get (&rozofs_qt_cache,disk_p,&key);
      if (dquot == NULL)
      {
        /*
	** should not happen
	*/
        break;
      }
      if (action == ROZOFS_QT_INC)
      {
        dquot->dquot.quota.dqb_curspace +=count; 
      }  
      else
      {
        dquot->dquot.quota.dqb_curspace -=count; 
	if (dquot->dquot.quota.dqb_curspace<0)
	{
	  dquot->dquot.quota.dqb_curspace =0; 
	} 
      } 
      /*
      ** update the grace period if needed
      */
      rozofs_quota_update_grace_times_blocks(&dquot->dquot.quota,quota_info_p);  
      /*
      ** let's write qota on disk
      */ 
      rozofs_qt_cache_put(&rozofs_qt_cache,disk_p,dquot);  
      status = 0;
      break; 
    } 
    return status;
}
/*
**__________________________________________________________________
*/
/**
*   update size (blocks) quota
    
    @param eid: export identifier
    
    @param usr_id : user quota
    @param grp_id : group quota
    @param nb_inode
    @param action: 1: increment, 0 decrement 
    @param share: share identifier (0 if none)
        
    @retval : 0 on success
    @retval < 0 on error
 */
int rozofs_qt_block_update(int eid,int user_id,int grp_id,uint64_t size,int action,uint16_t share)
{
   rozofs_qt_export_t *p;
   /*
   ** get the pointer to the quota context associated with the eid
   */
   
   /*
   ** check if the entry already exists: this is done to address the case of the exportd reload
   */
   if (eid > EXPGW_EID_MAX_IDX) 
   {
      /*
      ** eid value is out of range
      */
      return  -1;
   }
   if (export_quota_table[eid]== NULL)
   {
      return -1;
   }
   p = export_quota_table[eid];
   /*
   ** update user account
   */
   if (user_id != -1)
   {
      rozofs_qt_dqot_block_update(p->quota_inode[USRQUOTA],eid,USRQUOTA,user_id,size,action,
                                  &p->quota_super[USRQUOTA]);
   }
   if (grp_id != -1)
   {
      rozofs_qt_dqot_block_update(p->quota_inode[GRPQUOTA],eid,GRPQUOTA,user_id,size,action,
                                  &p->quota_super[GRPQUOTA]);
   }
   if (share !=0)
   {
      rozofs_qt_dqot_block_update(p->quota_inode[SHRQUOTA],eid,SHRQUOTA,(int)share,size,action,
                                  &p->quota_super[SHRQUOTA]);   
   }
   return 0;

}

/*
**__________________________________________________________________
*/
/**
*   Get the quota information related to either a group or a user
    
    @param eid: export identifier    
    @param type : user or group
    @param identifier :identifier within the type
    
    @retval <> NULL on success
    @retval NULL on error: see errno for details
 */
rozofs_qt_cache_entry_t *rozofs_qt_get_quota(int eid,int type,int identifier)
{
   rozofs_qt_export_t *p;
   rozofs_qt_cache_entry_t *dquot= NULL;
   rozofs_quota_key_t key;
   
   /*
   ** check if the entry already exists: this is done to address the case of the exportd reload
   */
   if (eid > EXPGW_EID_MAX_IDX) 
   {
      /*
      ** eid value is out of range
      */
      errno = ERANGE;
      return  dquot;
   }
   
   export_profiler_eid = eid;
   START_PROFILING(quota_get);  
    
   if (export_quota_table[eid]== NULL)
   {
      errno = ENOENT;
      goto error;
   }
   p = export_quota_table[eid];

   key.u64 = 0;
   key.s.qid = identifier;
   key.s.eid = eid;
   key.s.type = type;
   dquot = rozofs_qt_cache_get (&rozofs_qt_cache,p->quota_inode[type],&key);
   if (dquot == NULL)
   {
     errno = EFAULT;
   }
error:
   STOP_PROFILING(quota_get);   
   return dquot;
}
/*
**__________________________________________________________________
*/
/**
*  check grace time and hard limit upon file/directory creation

  @param dqot
  
  @retval 0 : if grace time or hardlimit not exceeded
  @retval -1: one of the limits is exhausted
*/
int rozofs_quota_check_grace_times(rozo_mem_dqblk *q)
{
    time_t now;

    time(&now);
    /*
    ** check hard limit: blocks
    */
    if (q->dqb_bhardlimit && rozofs_toqb(q->dqb_curspace) > q->dqb_bhardlimit) 
    {
       return -1 ;
    }
    /*
    ** check soft limit
    */
    if (q->dqb_bsoftlimit && rozofs_toqb(q->dqb_curspace) > q->dqb_bsoftlimit) 
    {
      if (q->dqb_btime < now )
      {
	 return -1 ;
      }
    }
    /*
    ** check hard limit: files
    */
    if (q->dqb_ihardlimit && q->dqb_curinodes > q->dqb_ihardlimit) 
    {
       return -1 ;
    }
    /*
    ** check the soft limit
    */
    if (q->dqb_isoftlimit && q->dqb_curinodes > q->dqb_isoftlimit) {
      if (q->dqb_itime < now )
      {
	return -1;
      }
    }
    return 0;
}

/*
**__________________________________________________________________
*/
/**
*   check quota upon file creation
    
    @param eid: export identifier
    
    @param usr_id : user quota
    @param grp_id : group quota
    
    @retval : 0 on success
    @retval < 0 on error
 */
int rozofs_qt_check_quota(int eid,int user_id,int grp_id)
{
   rozofs_qt_export_t *p;
   rozofs_qt_cache_entry_t *dquot_usr= NULL;
   rozofs_qt_cache_entry_t *dquot_grp= NULL;
   rozofs_quota_key_t key;
   int ret;
   /*
   ** get the pointer to the quota context associated with the eid
   */
   /*
   ** check if the entry already exists: this is done to address the case of the exportd reload
   */
   if (eid > EXPGW_EID_MAX_IDX) 
   {
      /*
      ** eid value is out of range
      */
      return  -1;
   }
   if (export_quota_table[eid]== NULL)
   {
      return -1;
   }
   p = export_quota_table[eid];
   /*
   ** check if quota is enable for that exported filesystem
   */
   if (p->quota_super[USRQUOTA].enable == 0) 
   {
     /*
     ** no quota
     */
     return 0;
   }
   /*
   ** THere is quota : get user  and group account
   */
   key.u64 = 0;
   key.s.qid = grp_id;
   key.s.eid = eid;
   key.s.type = USRQUOTA;
   dquot_usr = rozofs_qt_cache_get (&rozofs_qt_cache,p->quota_inode[USRQUOTA],&key);
   if (dquot_usr == NULL)
   {
     errno = EFAULT;
     goto error;
   }
   ret = rozofs_quota_check_grace_times(&dquot_usr->dquot.quota);
   if (ret < 0)
   {
     return -1;
   }
   /*
   ** check if quota is enable for that exported filesystem
   */
   if (p->quota_super[GRPQUOTA].enable == 0) 
   {
     /*
     ** no quota
     */
     return 0;
   }
   /*
   ** get group account
   */
   key.u64 = 0;
   key.s.qid = user_id;
   key.s.eid = eid;
   key.s.type = GRPQUOTA;
   dquot_grp = rozofs_qt_cache_get (&rozofs_qt_cache,p->quota_inode[GRPQUOTA],&key);
   if (dquot_grp == NULL)
   {
     errno = EFAULT;
     goto error;
   }
   ret = rozofs_quota_check_grace_times(&dquot_grp->dquot.quota);
   if (ret < 0)
   {
     return -1;
   }
   return 0;
error:

   return -1;

}

/*
**__________________________________________________________________
*/
/**
*  update the blocks grace time when there is a change in the quota limits

  @param q: user or group quota
  @param quota_info_p : pointer to the quota info associated with the type  
  
  @retval none
*/
 void rozofs_quota_update_grace_times_blocks(rozo_mem_dqblk *q,rozofs_quota_info_t *quota_info_p)
{
     time_t now;

     time(&now);
     if (q->dqb_bsoftlimit && rozofs_toqb(q->dqb_curspace)> q->dqb_bsoftlimit) {
	     if (!q->dqb_btime)
		     q->dqb_btime = now +  quota_info_p->dqi_bgrace ;
     }
     else
	     q->dqb_btime = 0;
}
/*
**__________________________________________________________________
*/
/**
*  update the inode grace time when there is a change in the quota limits

  @param q: user or group quota
  @param quota_info_p : pointer to the quota info associated with the type
    
  @retval none
*/
 void rozofs_quota_update_grace_times_inodes(rozo_mem_dqblk *q,rozofs_quota_info_t *quota_info_p)
{
    time_t now;

    time(&now);
    if (q->dqb_isoftlimit && q->dqb_curinodes > q->dqb_isoftlimit) {
	    if (!q->dqb_itime)
		    q->dqb_itime = now + quota_info_p->dqi_igrace;
    }
    else
	    q->dqb_itime = 0;
}
/*
**__________________________________________________________________
*/
/**
*  update the grace time when there is a change in the quota limits

  @param q: user or group quota
  @param quota_info_p : pointer to the quota info associated with the type
  
  @retval none
*/
 void rozofs_quota_update_grace_times(rozo_mem_dqblk *q,rozofs_quota_info_t *quota_info_p)
{
   rozofs_quota_update_grace_times_inodes(q,quota_info_p);
   rozofs_quota_update_grace_times_blocks(q,quota_info_p);
}

/*
**__________________________________________________________________
*/
/**
*   Set the quota information related to either a group or a user
    
    @param eid: export identifier    
    @param type : user or group
    @param identifier :identifier within the type
    @param sqa_qcmd : bitmap of the information to modify
    @param src : quota parameters
    
    @retval 0 on success
    @retval-1 on error: see errno for details
 */
int rozofs_qt_set_quota(int eid,int type,int identifier,int sqa_cmd, sq_dqblk *src )
{
   rozofs_qt_export_t *p;
   rozofs_qt_cache_entry_t *dquot= NULL;
   rozofs_quota_key_t key;
   
   export_profiler_eid = 0;
   
   /*
   ** check if the entry already exists: this is done to address the case of the exportd reload
   */
   if (eid > EXPGW_EID_MAX_IDX) 
   {
      /*
      ** eid value is out of range
      */
      errno = ERANGE;
      return  -1;
   }
   export_profiler_eid = eid;
   START_PROFILING(quota_set);   

   if (export_quota_table[eid]== NULL)
   {
     errno = ENOENT;
     goto error;
   }
   p = export_quota_table[eid];

   key.u64 = 0;
   key.s.qid = identifier;
   key.s.eid = eid;
   key.s.type = type;

   dquot = rozofs_qt_cache_get (&rozofs_qt_cache,p->quota_inode[type],&key);
   if (dquot == NULL)
   {
     errno = EFAULT;
     goto error;
   }
   /*
   ** update the information in the quota context 
   */
   if (sqa_cmd & QIF_BLIMITS) 
   /* FS_DQ_BSOFT | FS_DQ_BHARD */
   {
      dquot->dquot.quota.dqb_bhardlimit = src->rq_bhardlimit;
      dquot->dquot.quota.dqb_bsoftlimit = src->rq_bsoftlimit;	
   }
#if 0
   if (sqa_cmd & QIF_SPACE)
	   dst->d_fieldmask |= FS_DQ_BCOUNT;
#endif
   if (sqa_cmd & QIF_ILIMITS)
   /* FS_DQ_ISOFT | FS_DQ_IHARD */
   {
      dquot->dquot.quota.dqb_ihardlimit = src->rq_fhardlimit;
      dquot->dquot.quota.dqb_isoftlimit = src->rq_fsoftlimit;	
   }
#if 0
   if (sqa_cmd & QIF_INODES)
	   dst->d_fieldmask |= FS_DQ_ICOUNT;
   if (sqa_cmd & QIF_BTIME)
	   dst->d_fieldmask |= FS_DQ_BTIMER;
   if (sqa_cmd & QIF_ITIME)
	   dst->d_fieldmask |= FS_DQ_ITIMER;
#endif
   /*
   ** update the grace time if needed
   */
   rozofs_quota_update_grace_times(&dquot->dquot.quota,&p->quota_super[type]);
   /*
   ** let's write qota on disk
   */ 
   rozofs_qt_cache_put(&rozofs_qt_cache,p->quota_inode[type],dquot);  
   STOP_PROFILING(quota_set);
   return 0;

error:
  STOP_PROFILING(quota_set);
  return -1;
  
}

/*
**__________________________________________________________________
*/
/**
*   Init of the quota module of RozoFS
    
    @retval 0 on success
    @retval -1 on error
*/
int rozofs_qt_init()
{
    int ret;

    if (rozofs_qt_init_done == 1) return 0;
    /*
    ** allocate the cache
    */
    rozofs_qt_cache_initialize(&rozofs_qt_cache);
    
    /*
    ** init of the writeback cache+ thread
    */
    ret = quota_wbcache_init();
    if (ret < 0)
    {
      severe("error on writeback quota cache init\n");
      return -1;
    } 
    rozofs_qt_init_done = 1;
  return 0;    
}
