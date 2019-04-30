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

#ifndef ROZOFS_QUOTA_H
#define ROZOFS_QUOTA_H

#include <string.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <stdint.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h>
#include <rozofs/common/htable.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/core/disk_table_service.h>
#include <linux/quota.h>
#include "rozofs_quota_intf.h"

#undef MAXQUOTAS
#define MAXQUOTAS 3
#define SHRQUOTA 2
#undef  INITQFNAMES
#define INITQFNAMES { \
	"user",    /* USRQUOTA */ \
	"group",   /* GRPQUOTA */ \
	"share", \
	"undefined", \
};

#define ROZOFS_INITQFNAMES { \
	"usr",    /* USRQUOTA */ \
	"grp",   /* GRPQUOTA */ \
	"shr", \
	"und", \
};


/* Size of blocks in which are counted size limits in generic utility parts */
#define ROZOFS_QUOTABLOCK_BITS 10
#define ROZOFS_QUOTABLOCK_SIZE (1 << ROZOFS_QUOTABLOCK_BITS)

/* Conversion routines from and to quota blocks */
#define rozofs_qb2kb(x) ((x) << (ROZOFS_QUOTABLOCK_BITS-10))
#define rozofs_kb2qb(x) ((x) >> (ROZOFS_QUOTABLOCK_BITS-10))
#define rozofs_toqb(x) (((x) + ROZOFS_QUOTABLOCK_SIZE - 1) >> ROZOFS_QUOTABLOCK_BITS)


#define ROZOFS_QT_BUKETS (32*1024)
#define ROZOFS_QT_MAX_ENTRIES (64*1024)
#define ROZOFS_MAX_IQ_TIME  604800	/* (7*24*60*60) 1 week */
#define ROZOFS_MAX_DQ_TIME  604800	/* (7*24*60*60) 1 week */
#define ROZOFS_QUOTA_DISK_TB_SZ_POWER2 18

typedef union _rozofs_quota_key_t
{
   uint64_t u64;
   struct {
   
   uint64_t  eid:10;   /**< export identifier     */
   uint64_t  type:4;   /**< type of the quota : user/group, etc;.. */
   uint64_t  qid:32;   /**< identifier within the group            */
   uint64_t  filler:18; /**< unused                                */   
   }s;
} rozofs_quota_key_t;
/*
 * Data for one user/group kept in memory
 */
 
typedef struct _rozo_mem_dqblk {
	int64_t dqb_bhardlimit;	/* absolute limit on disk blks alloc */
	int64_t dqb_bsoftlimit;	/* preferred limit on disk blks */
	int64_t dqb_curspace;	/* current used space */
	int64_t dqb_rsvspace;   /* current reserved space for delalloc*/
	int64_t dqb_ihardlimit;	/* absolute limit on allocated inodes */
	int64_t dqb_isoftlimit;	/* preferred inode limit */
	int64_t dqb_curinodes;	/* current # allocated inodes */
	time_t dqb_btime;	/* time limit for excessive disk use */
	time_t dqb_itime;	/* time limit for excessive inode use */
} rozo_mem_dqblk;


typedef struct _rozofs_dquot_t
{
     rozofs_quota_key_t  key;  /**< key of the quota: contains the eid,group et quota identifier  */
     rozo_mem_dqblk      quota;
} rozofs_dquot_t;

/** API QUOTA cache management functions.
 *
 *  QUOTA cache is common to several exports 
 */

/** QUOTA cache entry  */
typedef struct rozofs_qt_cache_entry {
    rozofs_dquot_t   dquot;   /**< quota entry    */
    list_t list;        /**< list used by cache   */    
} rozofs_qt_cache_entry_t;

/** QUOTA cache
 *
 * used to keep track of user/group/directory quotas
 */
typedef struct rozofs_qt_cache {
    int max;            ///< max entries in the cache
    int size;           ///< current number of entries
    uint64_t   hit;
    uint64_t   miss;
    uint64_t   lru_del;
    list_t     lru;     ///< LRU 
    htable_t htable;    ///< entries hashing
} rozofs_qt_cache_t;

#define ROZOFS_QUOTA_INFO_NAME "quotainfo"
/**
*  quota information for each exported filesystem
*/
typedef struct _rozofs_quota_info_t
{
	unsigned long dqi_flags;
	unsigned int dqi_bgrace;
	unsigned int dqi_igrace;
	uint64_t dqi_maxblimit;
	uint64_t dqi_maxilimit;
	int  enable;  /* 0 disable / 1 enable */

} rozofs_quota_info_t;

/**
   Quota structure used for one export
*/
typedef struct rozofs_qt_export_t
{
   char *root_path;    /**< pointer to the root path */
   disk_table_header_t *quota_inode[MAXQUOTAS];
   rozofs_quota_info_t  quota_super[MAXQUOTAS];
} rozofs_qt_export_t;


static inline char *rozofs_qt_print(char *pbuf,rozofs_dquot_t *dquot)
{

  char *buf = pbuf;
  
  buf +=sprintf(buf,"identifier: %d/%d/%d\n",dquot->key.s.eid,dquot->key.s.type,dquot->key.s.qid);
  buf +=sprintf(buf,"  bhardlimit %llu\n",(unsigned int long long)dquot->quota.dqb_bhardlimit);;  
  buf +=sprintf(buf,"  bsoftlimit %llu\n",(unsigned int long long)dquot->quota.dqb_bsoftlimit);;  
  buf +=sprintf(buf,"  curspace   %llu\n",(unsigned int long long)dquot->quota.dqb_curspace);;  
  buf +=sprintf(buf,"  ihardlimit %llu\n",(unsigned int long long)dquot->quota.dqb_ihardlimit);;  
  buf +=sprintf(buf,"  isoftlimit %llu\n",(unsigned int long long)dquot->quota.dqb_isoftlimit);;  
  buf +=sprintf(buf,"  curinodes  %llu\n",(unsigned int long long)dquot->quota.dqb_curinodes);;  
  buf +=sprintf(buf,"  btime      %llu\n",(unsigned int long long)dquot->quota.dqb_btime);;  
  buf +=sprintf(buf,"  itime      %llu\n",(unsigned int long long)dquot->quota.dqb_itime);;  

  return pbuf;
}
/*
**__________________________________________________________________
*/
/**
*   The purpose of that service is to read object attributes and store them in the attributes cache

  @param cache : pointer to the export attributes cache
  @param disk_p: quota disk table associated with the type
  @param entry:pointer to the entry to insert (the key must be provisioned)
  
  @retval <> NULL: attributes of the object
  @retval == NULL : no attribute returned for the object (see errno for details)
*/

rozofs_qt_cache_entry_t *rozofs_qt_cache_put(rozofs_qt_cache_t *cache,
                                             disk_table_header_t *disk_p,
                                             rozofs_qt_cache_entry_t *entry);

/*
**__________________________________________________________________
*/
/**
*   Get an enry from the quota cache

    @param: pointer to the cache context
    @param disk_p: quota disk table associated with the type
    @param: key : the key contains the eid,type and quota identifier within the type
    
    @retval <>NULL : pointer to the cache entry that contains the quota
    @retval NULL: not found
*/
rozofs_qt_cache_entry_t *rozofs_qt_cache_get(rozofs_qt_cache_t *cache,
                                             disk_table_header_t *disk_p,
					     rozofs_quota_key_t *key);


/*
**__________________________________________________________________
*/
/**
*   init of a QUOTA cache

    @param: pointer to the cache context
    
    @retval none
*/
void rozofs_qt_cache_initialize(rozofs_qt_cache_t *cache);

/*
**__________________________________________________________________
*/
/**
*   delete of an exportd attribute cache

    @param: pointer to the cache context
    
    @retval none
*/
void rozofs_qt_cache_release(rozofs_qt_cache_t *cache);
/*
 **______________________________________________________________________________
 
    QUOTA  WRITEBACK CACHE SECTION
 **______________________________________________________________________________
*/    

#define QUOTA_CACHE_MAX_ENTRY   4096
#define QUOTA_CACHE_MAX_CHUNK   16

typedef struct _quota_chunk_cache_t
{
    uint32_t  wr_cpt;   /**< incremented on each write / clear when synced   */
    uint32_t  size;   /**< size of the entry (for stats purpose)   */
    uint64_t  key;      /**<key of the entry  */
    void   *ctx_p;     /**< disk table context */
    rozofs_dquot_t   *chunk_p;    /**< pointer to the chunk array  */
}  quota_chunk_cache_t;

/**
*  write back cache entry
*/
typedef struct _quota_writeback_entry_t
{
     pthread_rwlock_t lock;  /**< entry lock  */
     int state;  /**< O free: 1 busy*/
     /*
     **  pointer to the header of the dirent file (mdirents_file_t)
     */
     quota_chunk_cache_t  chunk[QUOTA_CACHE_MAX_CHUNK];
} quota_wbcache_entry_t;


/**
*____________________________________________________________
*/
/**
* init of the QUOTA write back cache

  @retval 0 on success
  @retval < 0 error (see errno for details)
*/

int quota_wbcache_init();
/**
*____________________________________________________________
*/
/**
*  write the quota in the writeback cache

   @param disk_p : disk table context
   @param buf: buffer to write
   @param count : length to write
   
   @retval >= 0 : number of bytes written in the writeback cache
   @retval < 0 : error see errno for details
*/
int quota_wbcache_write(disk_table_header_t  *disk_p,rozofs_dquot_t *buf,int count);
/**
*____________________________________________________________
*/
/**
*  attempt to get the quota from write back cache, otherwise need to
   read it from disk
   
   @param disk_p : disk table context
   @param buf: buffer to write
   @param count : length to write

*/
int quota_wbcache_read(disk_table_header_t  *disk_p,rozofs_dquot_t *buf,int count);
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
int rozofs_qt_set_quota(int eid,int type,int identifier,int sqa_cmd, sq_dqblk *src );
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
rozofs_qt_cache_entry_t *rozofs_qt_get_quota(int eid,int type,int identifier);
/*__________________________________________________________________________
* Initialize the quota thread interface
*
* @param slave_id : reference of the slave exportd
*
*  @retval 0 on success -1 in case of error
*/
int rozofs_qt_thread_intf_create(int slave_id);
/*
**__________________________________________________________________
*/
/**
*  update the grace time when there is a change in the quota limits

  @param q: user or group quota
  @param quota_info_p : pointer to the quota info associated with the type
  
  @retval none
*/
 void rozofs_quota_update_grace_times(rozo_mem_dqblk *q,rozofs_quota_info_t *quota_info_p);

/*
**__________________________________________________________________
*/
/**
*  update the blocks grace time when there is a change in the quota limits

  @param q: user or group quota
  @param quota_info_p : pointer to the quota info associated with the type  
  
  @retval none
*/
 void rozofs_quota_update_grace_times_blocks(rozo_mem_dqblk *q,rozofs_quota_info_t *quota_info_p);
/*
**__________________________________________________________________
*/
/**
*  update the inode grace time when there is a change in the quota limits

  @param q: user or group quota
  @param quota_info_p : pointer to the quota info associated with the type
    
  @retval none
*/
 void rozofs_quota_update_grace_times_inodes(rozo_mem_dqblk *q,rozofs_quota_info_t *quota_info_p);

/*
**__________________________________________________________________
*/
/**
*   check quota upon file creation
    
    @param eid: export identifier
    
    @param usr_id : user quota
    @param grp_id : group quota
    @param share_id : share_id quota
    
    @retval : 0 on success
    @retval < 0 on error
 */
int rozofs_qt_check_quota(int eid,int user_id,int grp_id,int share_id);

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
int rozofs_qt_set_quotainfo(int eid,int type,int identifier,int sqa_cmd, sq_dqblk *src );
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
int rozofs_qt_set_quotastate(int eid,int type,int cmd);
/*
**__________________________________________________________________
*/
/**
*   Fix an entry in the quota cache

    @param  cache:  pointer to the cache context
    @param  disk_p: quota disk table associated with the type
    @param  quota_info_p pointer to the quota info
    @param  entry : pointer to the cache entry that contains the quota
    @param  inode : new number of inode
    @param  size  : new size
    
    @retval 0 on success -1 in case of error
*/
int rozofs_qt_cache_fix(rozofs_qt_cache_t       * cache,
                        disk_table_header_t     * disk_p,
                        rozofs_quota_info_t     * quota_info_p,
                        rozofs_qt_cache_entry_t * entry,
                        uint64_t                  inode,
			uint64_t                  size);

#endif
