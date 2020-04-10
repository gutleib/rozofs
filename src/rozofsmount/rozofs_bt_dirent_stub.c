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
 #include <stdint.h>
 #include <unistd.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/rpc/export_profiler.h>
#include <src/exportd/export.h>
#include "rozofs_bt_dirent.h"
#include "../exportd/mdirent.h"



int rozofs_bt_remove_dirent_from_cache(fid_t fid,int *remove_count_p);

/**
**______________________________________________________________
*  Global data
**______________________________________________________________

*/


 uint32_t export_profiler_eid;
/*
** table that contains the export path of the client
*/
export_one_profiler_t  * export_profiler[EXPGW_EID_MAX_IDX+1] = { 0 };


char *rozofs_bt_dirent_export_rootpath = NULL;
int rozofs_bt_dirent_eid = 0;
/*
** hash table that contains the root bitmap of the directories
*/
htable_t htable_root_bitmap;
uint64_t rozofs_root_bmap_ientries_count = 0;  /**< number of active entries in the hash table */
list_t root_bitmap_entries;
uint64_t hash_root_btmap_collisions_count;
uint64_t hash_root_btmap_max_collisions;
uint64_t hash_root_btmap_cur_collisions;
static pthread_rwlock_t rozofs_bt_root_btmap_lock;
int rozofs_bt_dirent_load_debug = 1;


/*
**________________________________________________________________
*/

/** build a full path based on export root and fid of the lv2 file
 *
 * lv2 is the second level of files or directories in storage of metadata
 * they are acceded thru mreg or mdir API according to their type.
 *
 * @param root_path: root path of the exportd
 * @param fid: the fid we are looking for
 * @param path: the path to fill in
 */

static inline int export_lv2_resolve_path_internal(char *root_path, fid_t fid, char *path) {
    uint32_t slice;
    uint32_t subslice;
    char str[37];

    /*
     ** extract the slice and subsclie from the fid
     */
    mstor_get_slice_and_subslice(fid, &slice, &subslice);
    /*
     ** convert the fid in ascii
     */
    uuid_unparse(fid, str);
    sprintf(path, "%s/%d/%s", root_path, slice, str);
    return 0;

    return -1;
}

/** build a full path based on export root and fid of the lv2 file
 *
 * lv2 is the second level of files or directories in storage of metadata
 * they are acceded thru mreg or mdir API according to their type.
 *
 * @param export: the export we are searching on
 * @param fid: the fid we are looking for
 * @param path: the path to fill in
 */


static int export_lv2_resolve_path(fid_t fid, char *path) {
    int ret;

    START_PROFILING(export_lv2_resolve_path);

    ret = export_lv2_resolve_path_internal(rozofs_bt_dirent_export_rootpath, fid, path);

    STOP_PROFILING(export_lv2_resolve_path);
    return ret;
}



/*
 **__________________________________________________________________
 */
/**
* service to check if the bitmap for root_idx must be loaded

  @param inode : inode of the directory (without the eid part)
  @param bitmap_p:   pointer to array where the bitmap must be loaded
  
  @retval 0 on success
  @retval < 0 on error
*/

int rozofs_bt_dirent_load_root_idx_bitmap(uint64_t inode,dirent_dir_root_idx_bitmap_t *bitmap_p)
{
   int fd = -1;
   char node_path[PATH_MAX];
   char lv3_path[PATH_MAX];
   rozofs_inode_t rozofs_inode;
   
   rozofs_inode.fid[0] = 0;
   rozofs_inode.fid[1] = inode;
   rozofs_inode.s.eid = rozofs_bt_dirent_eid;
   /*
   ** read the bitmap from disk
   */    
   if (export_lv2_resolve_path((unsigned char *)&rozofs_inode.fid[0], node_path) != 0) goto error;
   sprintf(lv3_path, "%s/%s", node_path, MDIR_ATTRS_FNAME);   
   if ((fd = open(lv3_path, O_RDONLY | O_NOATIME, S_IRWXU)) < 0) 
   {
     goto error;
   }
   ssize_t len = pread(fd,bitmap_p->bitmap,DIRENT_FILE_BYTE_BITMAP_SZ,0);
   if (len != DIRENT_FILE_BYTE_BITMAP_SZ) goto error;
   /*
   ** clear the dirty bit
   */
   bitmap_p->dirty = 0;
   /*
   ** close the file
   */
   close(fd);
   return 0;
   
error:
   if (fd != -1) close(fd);

   return -1;
}

/*
**________________________________________________________________
*/
static inline int dirent_ino_cmp(void *v1, void *v2) {
      int ret;
      ret =  memcmp(v1, v2, sizeof (uint64_t));
      if (ret != 0) {
          hash_root_btmap_collisions_count++;
	  hash_root_btmap_cur_collisions++;
	  return ret;
      }
      if (hash_root_btmap_max_collisions < hash_root_btmap_cur_collisions) hash_root_btmap_max_collisions = hash_root_btmap_cur_collisions;
      return ret;
}


/*
**________________________________________________________________
*/
static inline uint32_t dirent_ino_hash_fnv_with_len( void *key1) {

    unsigned char *d = (unsigned char *) key1;
    int i = 0;
    int h;

     h = 2166136261U;
    /*
     ** hash on name
     */
    d = key1;
    for (i = 0; i <sizeof (uint64_t) ; d++, i++) {
        h = (h * 16777619)^ *d;
    }
    return (uint32_t) h;
}

/*
**________________________________________________________________
*/
static inline uint32_t dirent_ino_hash(void *n) {
    return dirent_ino_hash_fnv_with_len(n);
}
/*
**________________________________________________________________
*/
/**
   Insert a root bitmap entry in the hash table
   
   @param root_p: pointer to the root bitmap context
   
   @retval none
*/

static inline void put_btmap_entry(dentry_btmap_t * ie) {

    pthread_rwlock_wrlock(&rozofs_bt_root_btmap_lock);

    rozofs_root_bmap_ientries_count++;
    ie->he.key   = &ie->inode;
    ie->he.value = ie;
    htable_put_entry(&htable_root_bitmap, &ie->he);
    list_push_front(&root_bitmap_entries, &ie->list);

    pthread_rwlock_unlock(&rozofs_bt_root_btmap_lock);
}
/*
**________________________________________________________________
*/
/**
   Remove a root bitmap entry from the hash table
   
   @param root_p: pointer to the root bitmap context
   
   @retval none
*/
static inline void del_btmap_entry(dentry_btmap_t * ie) {
    
    pthread_rwlock_wrlock(&rozofs_bt_root_btmap_lock);

    rozofs_root_bmap_ientries_count--;
    htable_del_entry(&htable_root_bitmap, &ie->he);
    list_remove(&ie->list);

    xfree(ie);    

    pthread_rwlock_unlock(&rozofs_bt_root_btmap_lock);

}

/*
**________________________________________________________________
*/
/**
   Get the context of a root bitmap entry from fuse inode (64 bits)
   
   @param ino: fuse inode of the directory
   
   @retval <> NULL : pointer to the root bitmap entry
   @retval NULL: not found
*/

static inline dentry_btmap_t *get_btmap_entry_by_inode(uint64_t ino) {
    uint64_t inode;
    
    dentry_btmap_t *p;
    
    inode = rozofs_bt_inode_normalize(ino);
    hash_root_btmap_cur_collisions = 0;
    pthread_rwlock_rdlock(&rozofs_bt_root_btmap_lock);
    
    p = htable_get(&htable_root_bitmap, &inode);

    pthread_rwlock_unlock(&rozofs_bt_root_btmap_lock);
    return p;
}

/*
**________________________________________________________________
*/
/**
   Get the context of a root bitmap entry from rozofs fid (128 bits)
   
   @param fid: fid of the directory
   
   @retval <> NULL : pointer to the root bitmap entry
   @retval NULL: not found
*/
static inline dentry_btmap_t *get_btmap_entry_by_fid(fid_t fid) {
    rozofs_inode_t *fake_id = (rozofs_inode_t *) fid;
    /*
    ** check if empty inode to address the case of invalid attributes returned by exportd
    */
    if (fake_id->fid[1] == 0) return NULL; 
    return get_btmap_entry_by_inode(fake_id->fid[1]);
}

/*
**________________________________________________________________
*/
/**
   Get the context of a root bitmap entry from rozofs fid (128 bits)
   
   @param fid: fid of the directory
   
   @retval <> NULL : pointer to the root bitmap entry
   @retval NULL: not found
*/
dentry_btmap_t *get_btmap_entry_by_fid_external(fid_t fid)
{
  return get_btmap_entry_by_fid(fid);
}

/*
**________________________________________________________________
*/
/**
   allocate a root bitmap entry
   
   @param fid: rozofs fid of the directory
   
   @retval <> NULL : pointer to the root bitmap entry
   @retval NULL: not found
*/
static inline dentry_btmap_t *alloc_btmap_entry_by_fid(fid_t fid) {
	dentry_btmap_t *ie = NULL;
	rozofs_inode_t *inode_p ;
	uint64_t inode;
	int ret;
	
	inode_p = (rozofs_inode_t*) fid;
	inode = rozofs_bt_inode_normalize(inode_p->fid[1]);

	ie = xmalloc(sizeof(dentry_btmap_t));
	if (ie == NULL) return NULL;
	
	ie->inode = inode; 
	list_init(&ie->list);
	/*
	** load the bitmap of the dirent
	*/
	ret = rozofs_bt_dirent_load_root_idx_bitmap(inode,&ie->btmap);
	if (ret < 0)
	{
	  warning("error while loading dirent bitmap for inode %llx:%s",(unsigned long long int)inode,strerror(errno));
	  goto error;
        }
	put_btmap_entry(ie);
	return ie;
error:
        xfree(ie);
	return NULL;
}



/*
**________________________________________________________________
*/
/**
   allocate a root bitmap entry
   
   @param ino: fuse inode value (64 bits)
   @param mtime: mtime of the owner directory
   
   @retval <> NULL : pointer to the root bitmap entry
   @retval NULL: not found
*/
static inline dentry_btmap_t *alloc_btmap_entry_by_inode(uint64_t ino,uint64_t mtime) {
	dentry_btmap_t *ie;
	uint64_t inode;
	int ret;
	
	inode = rozofs_bt_inode_normalize(ino);

	ie = xmalloc(sizeof(dentry_btmap_t));
	if (ie == NULL) return NULL;
	
	ie->inode = inode; 
	ie->mtime = mtime;
	list_init(&ie->list);
	/*
	** load the bitmap of the dirent
	*/
	ret = rozofs_bt_dirent_load_root_idx_bitmap(inode,&ie->btmap);
	if (ret < 0)
	{
	  warning("error while loading dirent bitmap for inode %llx:%s",(unsigned long long int)inode,strerror(errno));
	  goto error;
	}
	put_btmap_entry(ie);

	return ie;
error:
        xfree(ie);
	return NULL;
}
/*
**__________________________________________________________________
*/
/**
*   update the root_idx bitmap in memory

   @param ctx_p: pointer to the level2 cache entry
   @param root_idx : root index to update
   @param set : assert to 1 when the root_idx is new/ 0 for removing
   

*/
void export_dir_update_root_idx_bitmap(void *ctx_p,int root_idx,int set)
{
    uint16_t byte_idx;
    int bit_idx ;
    dirent_dir_root_idx_bitmap_t *bitmap_p;
    
    if (ctx_p == NULL) return;
    
    bitmap_p = (dirent_dir_root_idx_bitmap_t*)ctx_p;
    
    if (root_idx >DIRENT_MAX_IDX_FOR_EXPORT) return;
    
    byte_idx = root_idx/8;
    bit_idx =  root_idx%8;
    if (set)
    {
       if (bitmap_p->bitmap[byte_idx] & (1<<bit_idx)) return;
       bitmap_p->bitmap[byte_idx] |= 1<<bit_idx;    
    }
    else
    {
       bitmap_p->bitmap[byte_idx] &=~(1<<bit_idx);        
    }
    bitmap_p->dirty = 1;
}
/*
**__________________________________________________________________
*/
/**
*   check the presence of a root_idx  in the bitmap 

   @param ctx_p: pointer to the level2 cache entry
   @param root_idx : root index to update

  @retval 1 asserted
  @retval 0 not set   

*/
int export_dir_check_root_idx_bitmap_bit(void *ctx_p,int root_idx)
{
    uint16_t byte_idx;
    int bit_idx ;
    dirent_dir_root_idx_bitmap_t *bitmap_p;
    
    if (ctx_p == NULL) return 1;
    
    bitmap_p = (dirent_dir_root_idx_bitmap_t*)ctx_p;
    if (root_idx >DIRENT_MAX_IDX_FOR_EXPORT) return 1;
    
    byte_idx = root_idx/8;
    bit_idx =  root_idx%8;

    if (bitmap_p->bitmap[byte_idx] & (1<<bit_idx)) 
    {
      return 1;
    }
    return 0;
}
/*
**__________________________________________________________________
*/
/**
*  compute the number or root idx files in a root bitmap entry

   @param bitmap_p: pointer to the bitmap array
   
   @retval number of entries
*/
int export_dir_get_root_idx_count(dirent_dir_root_idx_bitmap_t *bitmap_p)
{

  int byte = 0;
  int bit_idx = 0;
  int count = 0;
  uint8_t byte_value;
  
  for (byte = 0; byte <DIRENT_FILE_BYTE_BITMAP_SZ;byte++)
  {
     byte_value =  bitmap_p->bitmap[byte];
     if (byte_value == 0) continue;
     for (bit_idx = 0; bit_idx < 8; bit_idx++)
     {
       if ((byte_value & 1<<bit_idx) == 0) continue;
       count++; 
     }
  }
  return count;
}


  
#define SHOW_BT_STATS_PROBE_INO(probe) pChar += sprintf(pChar," %-28s | %15"PRIu64" | %15"PRIu64" |\n",\
                    #probe,\
		    dirent_stats_p->probe[0],dirent_stats_p->probe[1]);

#define SHOW_BT_STATS_PROBE(probe) pChar += sprintf(pChar," %-28s | %15"PRIu64" |\n",\
                    #probe,\
		    dirent_stats_p->probe);

/*__________________________________________________________________________
*/  
static char * show_trkrd_stats_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"trkrd_stats          : display statistics\n");
  pChar += sprintf(pChar,"trkrd_stats reset    : display statistics and reset the counters\n");
  pChar += sprintf(pChar,"trkrd_stats dbgon    : turn on the debug mode\n");
  pChar += sprintf(pChar,"trkrd_stats dbgoff   : turn off the debug mode\n");

  return pChar; 
}

/*__________________________________________________________________________
*/ 

void show_trkrd_stats(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
  int reset = 0;

  if (argv[1] != NULL) 
  {
    while(1)
    {
      if (strcmp(argv[1],"reset")==0) 
      {
	 reset = 1;  
	 break;      
      }
      if (strcmp(argv[1],"dbgon")==0) 
      {
	 rozofs_bt_dirent_load_debug = 1;  
	 pChar += sprintf(pChar,"Debug mode is now on\n");
         uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
         return;  
      }
      if (strcmp(argv[1],"dbgoff")==0) 
      {
	 rozofs_bt_dirent_load_debug = 0;  
	 pChar += sprintf(pChar,"Debug mode is now off\n");
         uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
         return;     
      }
      pChar = show_trkrd_stats_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;   
    }
  } 
  pChar +=sprintf(pChar,"Debug mode : %s\n\n",(rozofs_bt_dirent_load_debug)?"ON":"OFF");
  pChar +=sprintf(pChar,"Main thread stats\n");
  pChar +=sprintf(pChar,"------------------------------+-----------------+-----------------+\n");
  pChar +=sprintf(pChar,"     counter_id               |     DIR         |      REG        |\n");
  pChar +=sprintf(pChar,"------------------------------+-----------------+-----------------+\n");

  SHOW_BT_STATS_PROBE_INO(lookup_inode_hit);
  SHOW_BT_STATS_PROBE_INO(lookup_inode_miss);
  SHOW_BT_STATS_PROBE(lookup_bitmap_hit);
  SHOW_BT_STATS_PROBE(lookup_bitmap_miss);
  SHOW_BT_STATS_PROBE(lookup_dir_bad_mtime);
  pChar +=sprintf(pChar,"\nDirent thread stats\n");
  pChar +=sprintf(pChar,"------------------------------+-----------------+-----------------+\n");
  pChar +=sprintf(pChar,"     counter_id               |     DIR         |      REG        |\n");
  pChar +=sprintf(pChar,"------------------------------+-----------------+-----------------+\n");

  SHOW_BT_STATS_PROBE_INO(get_inode_attempts);
  SHOW_BT_STATS_PROBE_INO(get_inode_duplicated);
  SHOW_BT_STATS_PROBE_INO(get_tracking_file_attempts);
  SHOW_BT_STATS_PROBE_INO(get_tracking_file_success);
  SHOW_BT_STATS_PROBE_INO(get_tracking_file_failure);
  SHOW_BT_STATS_PROBE(check_dirent_root_valid);
  SHOW_BT_STATS_PROBE(check_dirent_root_valid_ok);
  SHOW_BT_STATS_PROBE(check_dirent_root_valid_nok);
  SHOW_BT_STATS_PROBE(check_dirent_too_early);  
  SHOW_BT_STATS_PROBE(get_dirent_file_attempts);
  SHOW_BT_STATS_PROBE(get_dirent_file_success);
  SHOW_BT_STATS_PROBE(get_dirent_file_failure);
  if (reset)
  {
    memset(dirent_stats_p,0,sizeof(*dirent_stats_p));
    pChar +=sprintf(pChar,"\n\n reset done!\n");
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());         
  }
  else
  {
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());         
  
  }
}


/*__________________________________________________________________________
*/  
static char * show_btmap_entry_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"btmap_entry count         : display dirent bitmap entry count\n");
  pChar += sprintf(pChar,"btmap_entry coll          : display dirent bitmap entry collisions\n");
  pChar += sprintf(pChar,"btmap_entry fid <fid>     : display dirent bitmap entry by FID\n");  
  pChar += sprintf(pChar,"btmap_entry inode <inode> : display dirent bitmap entry by inode\n");  
  pChar += sprintf(pChar,"btmap_entry nb <nb>       : display dirent bitmap entry number <nb> in list\n");  
  pChar += sprintf(pChar,"btmap_entry del fid <fid>     : remove dentries from dirent cache for <fid>\n");  
  pChar += sprintf(pChar,"btmap_entry del inode <inode> : remove dentries from dirent cache for <inode>\n");  
  pChar += sprintf(pChar,"btmap_entry del nb <nb>       : remove dentries from dirent cache for entry <nb>\n");  
  return pChar; 
}
/*__________________________________________________________________________
*/ 
void show_btmap_entry_delete(char * argv[], uint32_t tcpRef, void *bufRef) {

  char *pChar = uma_dbg_get_buffer();
  fid_t fid;
  long long unsigned int   inode;
  dentry_btmap_t * ie = NULL;
  char fid_str[64];
  rozofs_inode_t  rozofs_inode;
  int ret;
  int remove_count;

  if (argv[3] == NULL) {
      pChar = show_btmap_entry_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;
  } 
  
  if (strcmp(argv[2],"fid")==0) {
      if (rozofs_uuid_parse(argv[3],fid)==-1) {
          pChar += sprintf(pChar, "this is not a valid FID\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;	        
      }
      ie = get_btmap_entry_by_fid(fid);
      if (ie == NULL) {
          pChar += sprintf(pChar, "No btmap_entry for this FID\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;        
      }
  }        
  else if (strcmp(argv[2],"inode")==0){
      int ret = sscanf(argv[3],"%llu",&inode);
      if (ret != 1) {
          pChar += sprintf(pChar, "this is not a valid inode\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;	        
      }
      ie = get_btmap_entry_by_inode(inode);
      if (ie == NULL) {
          pChar += sprintf(pChar, "No btmap_entry for this inode\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;        
      }      
  } 
  else if (strcmp(argv[2],"nb")==0){
      int ret = sscanf(argv[3],"%llu",&inode);
      if (ret != 1) {
          pChar += sprintf(pChar, "this is not a valid number\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;	        
      }
      if (inode>rozofs_root_bmap_ientries_count) {
          pChar += sprintf(pChar, "this is not a valid number\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;	        
      }
      list_t * p;
      ie = NULL;
      list_for_each_forward(p, &root_bitmap_entries) {
        inode--;
	if (inode==0) {
	  ie = list_entry(p, dentry_btmap_t, list);
	  break;
	}    
      }	
      if (ie == NULL) {
          pChar += sprintf(pChar, "No btmap_entry for this number\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;        
      }      
  }
  else
  {
      pChar += sprintf(pChar, "unsupported option\n");
      pChar = show_btmap_entry_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;  
  }   
  rozofs_inode.fid[0] = 0;
  rozofs_inode.fid[1] = ie->inode;
  rozofs_inode.s.eid = rozofs_bt_dirent_eid;
  rozofs_uuid_unparse((unsigned char*) &rozofs_inode.fid[0],fid_str);
  /*
  ** now delete the entry
  */
  ret = rozofs_bt_remove_dirent_from_cache((unsigned char*)&rozofs_inode.fid[0],&remove_count);
  if (ret < 0)
  {
    pChar += sprintf(pChar,"error while removing fid %s:%s\n",fid_str,strerror(errno));
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;      
  }
  pChar += sprintf(pChar,"fid %s has been successfully removed (dirent cache remove count:%d)\n",fid_str,remove_count);
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());       
}
/*__________________________________________________________________________
*/ 
void show_btmap_entry(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
  fid_t fid;
  long long unsigned int   inode;
  dentry_btmap_t * ie = NULL;
  char fid_str[64];
  rozofs_inode_t  rozofs_inode;
  int delete = 0;
  char pathname[1024];
  int count = 0;
  
  if (argv[1] == NULL) {
      pChar = show_btmap_entry_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;
  } 
  if (strcmp(argv[1],"coll")==0) {
      pChar += sprintf(pChar, "btmap_entry collisions: %llu\n", (long long unsigned int) hash_root_btmap_collisions_count);
      pChar += sprintf(pChar, "btmap_entry max colls : %llu\n", (long long unsigned int) hash_root_btmap_max_collisions);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer()); 
      hash_root_btmap_collisions_count = 0;  
      hash_root_btmap_max_collisions = 0;
      return;
  }
  if (strcmp(argv[1],"count")==0) {
      pChar += sprintf(pChar, "btmap_entry counter: %llu\n", (long long unsigned int) rozofs_root_bmap_ientries_count);
      pChar += sprintf(pChar, "btmap_entry_size    : %u\n",  (unsigned int)sizeof(dirent_dir_root_idx_bitmap_t));
            
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;
  }
  if (strcmp(argv[1],"del")==0) {
      delete = 1;
  }
  if (delete) return show_btmap_entry_delete(argv,tcpRef, bufRef);
  
  
  if (argv[2] == NULL) {
      pChar = show_btmap_entry_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;
  } 
  
  if (strcmp(argv[1],"fid")==0) {
      if (rozofs_uuid_parse(argv[2],fid)==-1) {
          pChar += sprintf(pChar, "this is not a valid FID\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;	        
      }
      ie = get_btmap_entry_by_fid(fid);
      if (ie == NULL) {
          pChar += sprintf(pChar, "No btmap_entry for this FID\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;        
      }
  }        
  else if (strcmp(argv[1],"inode")==0){
      int ret = sscanf(argv[2],"%llu",&inode);
      if (ret != 1) {
          pChar += sprintf(pChar, "this is not a valid inode\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;	        
      }
      ie = get_btmap_entry_by_inode(inode);
      if (ie == NULL) {
          pChar += sprintf(pChar, "No btmap_entry for this inode\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;        
      }      
  } 
  else if (strcmp(argv[1],"nb")==0){
      int ret = sscanf(argv[2],"%llu",&inode);
      if (ret != 1) {
          pChar += sprintf(pChar, "this is not a valid number\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;	        
      }
      if (inode>rozofs_root_bmap_ientries_count) {
          pChar += sprintf(pChar, "this is not a valid number\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;	        
      }
      list_t * p;
      ie = NULL;
      list_for_each_forward(p, &root_bitmap_entries) {
        inode--;
	if (inode==0) {
	  ie = list_entry(p, dentry_btmap_t, list);
	  break;
	}    
      }	
      if (ie == NULL) {
          pChar += sprintf(pChar, "No btmap_entry for this number\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;        
      }      
  }   
  else {
      pChar += sprintf(pChar, "btmap_entry counter: %llu\n", (long long unsigned int) rozofs_root_bmap_ientries_count);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;    
  }
  rozofs_inode.fid[0] = 0;
  rozofs_inode.fid[1] = ie->inode;
  rozofs_inode.s.eid = rozofs_bt_dirent_eid;
  
  rozofs_uuid_unparse((unsigned char*) &rozofs_inode.fid[0],fid_str);
  
  count = export_dir_get_root_idx_count(&ie->btmap);
  pathname[0] = 0;
  export_lv2_resolve_path((unsigned char*) &rozofs_inode.fid[0],pathname);
     
  pChar += sprintf(pChar, "%-15s : %llu\n", "inode", (long long unsigned int)ie->inode);
  pChar += sprintf(pChar, "%-15s : %s\n", "fid", fid_str);
  pChar += sprintf(pChar, "%-15s : %d\n", "count", count);
  pChar += sprintf(pChar, "%-15s : %s\n", "path", pathname);

  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
  return;
  
}  



/*
*_______________________________________________________________________
*/
/**
* dirent cache
*/
char *dirent_cache_display(char *pChar);

void show_dirent_cache(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    pChar = dirent_cache_display(pChar);
    pChar = dirent_disk_display_stats(pChar);
    pChar = dirent_wbcache_display_stats(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
}


/*
*_______________________________________________________________________
*/

#define SHOW_PROFILER_PROBE(probe) \
  if (prof->probe[P_COUNT]) {\
    *pChar++ = ' ';\
    pChar += rozofs_string_padded_append(pChar, 25, rozofs_left_alignment, #probe);\
    *pChar++ = '|';*pChar++ = ' ';\
    pChar += rozofs_u64_padded_append(pChar, 16, rozofs_right_alignment, prof->probe[P_COUNT]);\
    *pChar++ = ' '; *pChar++ = '|'; *pChar++ = ' ';\
    pChar += rozofs_u64_padded_append(pChar, 9, rozofs_right_alignment, prof->probe[P_COUNT]?prof->probe[P_ELAPSE]/prof->probe[P_COUNT]:0);\
    *pChar++ = ' '; *pChar++ = '|'; *pChar++ = ' ';\
    pChar += rozofs_u64_padded_append(pChar, 19, rozofs_right_alignment, prof->probe[P_ELAPSE]);\
    *pChar++ = ' '; *pChar++ = '|'; *pChar++ = ' ';\
    pChar += rozofs_string_padded_append(pChar, 15, rozofs_right_alignment, " ");\
    *pChar++ = '\n';\
    *pChar = 0;\
  }

#define SHOW_PROFILER_PROBE_BYTE(probe) \
  if (prof->probe[P_COUNT]) {\
    *pChar++ = ' ';\
    pChar += rozofs_string_padded_append(pChar, 25, rozofs_left_alignment, #probe);\
    *pChar++ = '|';*pChar++ = ' ';\
    pChar += rozofs_u64_padded_append(pChar, 16, rozofs_right_alignment, prof->probe[P_COUNT]);\
    *pChar++ = ' '; *pChar++ = '|'; *pChar++ = ' ';\
    pChar += rozofs_u64_padded_append(pChar, 9, rozofs_right_alignment, prof->probe[P_COUNT]?prof->probe[P_ELAPSE]/prof->probe[P_COUNT]:0);\
    *pChar++ = ' '; *pChar++ = '|'; *pChar++ = ' ';\
    pChar += rozofs_u64_padded_append(pChar, 19, rozofs_right_alignment, prof->probe[P_ELAPSE]);\
    *pChar++ = ' '; *pChar++ = '|'; *pChar++ = ' ';\
    pChar += rozofs_u64_padded_append(pChar, 15, rozofs_right_alignment, prof->probe[P_BYTES]);\
    *pChar++ = '\n';\
    *pChar = 0;\
  }

/*
*_______________________________________________________________________
*/
char * show_profiler_one(char * pChar, uint32_t eid) {
    export_one_profiler_t * prof;   


    prof = export_profiler[eid];
    if (prof == NULL) return pChar;
        

    // Compute uptime for storaged process
    pChar += rozofs_string_append(pChar, "_______________________ EID = ");
    pChar += rozofs_u32_append(pChar,eid);
    pChar += rozofs_string_append(pChar, " _______________________ \n   procedure              |      count       |  time(us) |  cumulated time(us) |     bytes       |\n--------------------------+------------------+-----------+---------------------+-----------------+\n");

    SHOW_PROFILER_PROBE(get_mdirentry);
    SHOW_PROFILER_PROBE(put_mdirentry);
    SHOW_PROFILER_PROBE(del_mdirentry);
    SHOW_PROFILER_PROBE(list_mdirentries);    
    return pChar;
}
/*
*_______________________________________________________________________
*/
static char * show_dirent_profiler_help(char * pChar) {
  pChar += rozofs_string_append(pChar,"usage:\ndirent_profiler reset  : reset statistics\ndirent_profiler        : display statistics\n");  
  return pChar; 
}
/*
*_______________________________________________________________________
*/
void show_dirent_profiler(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    *pChar = 0;

    if (argv[1] == NULL) {
      pChar = show_profiler_one(pChar,rozofs_bt_dirent_eid);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
      return;
    }

    if (strcmp(argv[1],"reset")==0) {

      if (argv[2] == NULL) {
        pChar = show_profiler_one(pChar,rozofs_bt_dirent_eid);
	export_profiler_reset_all();
	pChar += sprintf(pChar,"Reset done\n");
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;	 
      }
    }
//    pChar = show_profiler_one(pChar,rozofs_bt_dirent_eid);
    show_dirent_profiler_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
    return;
}


/*
**________________________________________________________________
*/
/**
*   Init of the hash table used for caching the dirent root content associated with the directory

    @param eid: export identifier
    @param export_root_path: export root path on the client side
    
    @retval 0 on success
    @retval -1 on error
*/
int rozofs_bt_dirent_hash_init(int eid, char *export_root_path)
{


    rozofs_bt_dirent_export_rootpath = strdup(export_root_path);
    rozofs_bt_dirent_eid = eid;
    pthread_rwlock_init(&rozofs_bt_root_btmap_lock, NULL);

    

    /* Initialize list and htables for root_bitmap_entries */
    list_init(&root_bitmap_entries);
    htable_initialize(&htable_root_bitmap, ROOT_BTMAP_HSIZE, dirent_ino_hash, dirent_ino_cmp);
    hash_root_btmap_collisions_count = 0;
    hash_root_btmap_max_collisions = 0;
    hash_root_btmap_cur_collisions = 0;
    rozofs_root_bmap_ientries_count = 0;
    uma_dbg_addTopic("trkrd_btmap_entry", show_btmap_entry);
    rozofs_bt_dirent_load_debug = 0;
    uma_dbg_addTopic("trkrd_stats", show_trkrd_stats);
    /*
    ** dirent cache stats
    */
    uma_dbg_addTopic("dirent_cache",show_dirent_cache);   
    
    export_profiler_eid = eid;
    export_profiler[export_profiler_eid] = malloc(sizeof(export_one_profiler_t));
    if (export_profiler[export_profiler_eid] != NULL)
    {
       export_one_profiler_t  * p = export_profiler[export_profiler_eid];
       memset(p,0,sizeof(*p));
    }
    uma_dbg_addTopic("dirent_profiler",show_dirent_profiler);        
    dirent_cache_level0_initialize();
//    dirent_wbcache_init();
    dirent_wbcache_disable();
    return 0;
}
/*
**________________________________________________________________
*/
/**
  Load in the cache the content of the bitmap file of the directory
  
  @param inode: directory in node in RozoFS format
  @param mtime of the directory
  
  @retval 0 on success
  @retval -1 on error (see errno for details
*/
int rozofs_bt_load_dirent_root_bitmap_file(uint64_t inode,uint64_t mtime)
{
   dentry_btmap_t *bt_map_p;
   int remove_count;
   rozofs_inode_t rozofs_inode;

   rozofs_inode.fid[0] = 0;
   rozofs_inode.fid[1] = inode;
   rozofs_inode.s.eid = rozofs_bt_dirent_eid;

   /*
   ** we must release the old entries before put the new dirent entries
   ** it might be possible that the cache is empty
   */
   rozofs_bt_remove_dirent_from_cache((unsigned char *)&rozofs_inode.fid[0],&remove_count);
   info("FDL load dirent remove before loading (count %d)",remove_count);
   /*
   ** need to allocate an entry
   */
   bt_map_p = alloc_btmap_entry_by_inode(inode, mtime);
   if (bt_map_p != NULL)
   {
     return 0;
   }
   return -1;
}

/*
**________________________________________________________________
*/
/*
** attempt to get the inode value from the local dirent
  @param fid_parent: fid of the parent directory
  @param name: name to search
  @param fid: returned fid
  
*/
int rozofs_bt_get_mdirentry(fid_t fid_parent, char * name, fid_t fid)    
{
   int ret;
   dentry_btmap_t *bt_map_p;
   int dirfd = -1;
   uint32_t type;
   int mask_ret;
//   char fid_str[128];
   /*
   ** try to get the root bitmap of the directory
   */
   bt_map_p = get_btmap_entry_by_fid(fid_parent);
   if (bt_map_p == NULL)
   {
     errno = EAGAIN;
     return -1;
   }
   /*
   ** go to the local dirent
   */
   ret = get_mdirentry(&bt_map_p->btmap,dirfd,fid_parent,name,fid,&type,&mask_ret);
   if (ret == 0)
   {
     //rozofs_uuid_unparse((unsigned char*) fid,fid_str);
     //info("FDL ret %d mask %d name %s: @rozofs_uuid@%s\n",ret,mask_ret,name,fid_str);
   }
   else
   {
     //info("FDL ret %d mask %d name %s not found\n",ret,mask_ret,name);   
     errno = ENOENT;
   }
   return ret;
}   

/*
**_______________________________________________________________
*/
#define DIRENT_BUCKET_DEPTH_IN_BIT   16

#define DIRENT_BUCKET_MAX_ROOT_DIRENT (256000)
//#define DIRENT_BUCKET_MAX_ROOT_DIRENT (4)

#define DIRENT_BUCKET_MAX_COLLISIONS  256  /**< number of collisions that can be supported by a bucket  */
#define DIRENT_BUCKET_MAX_COLLISIONS_BYTES  (DIRENT_BUCKET_MAX_COLLISIONS/8)  /**< number of collisions that can be supported by a bucket  */
#define DIRENT_BUCKET_NB_ENTRY_PER_ARRAY  32 /**< number of dirent_cache_bucket_entry_t strcuture per memory array */
#define DIRENT_BUCKET_ENTRY_MAX_ARRAY (DIRENT_BUCKET_MAX_COLLISIONS/DIRENT_BUCKET_NB_ENTRY_PER_ARRAY)

typedef struct _dirent_cache_bucket_entry_t {
    uint16_t hash_value_table[DIRENT_BUCKET_NB_ENTRY_PER_ARRAY]; /**< table of the hash value applied to the parent_fid and index */
    void *entry_ptr_table[DIRENT_BUCKET_NB_ENTRY_PER_ARRAY]; /**< table of the dirent cache entries: used for doing the exact match */
} dirent_cache_bucket_entry_t;

/**
 *  dirent cache structure
 */
typedef struct _dirent_cache_bucket_t {
    list_t bucket_lru_link; /**< link list for bucket LRU  */
    uint8_t bucket_free_bitmap[DIRENT_BUCKET_MAX_COLLISIONS_BYTES]; /**< bitmap of the free entries  */
    dirent_cache_bucket_entry_t * entry_tb[DIRENT_BUCKET_ENTRY_MAX_ARRAY]; /**< pointer to the memory array that contains the entries */

} dirent_cache_bucket_t;

typedef struct _dirent_cache_main_t {
    uint32_t max; /**< maximum number of entries in the cache */
    uint32_t size; /**< current number of entries in the cache */
    list_t global_lru_link; /**< entries cached: used for LRU           */
    dirent_cache_bucket_t *htable; /**< pointer to the bucket array of the cache */
} dirent_cache_main_t;


extern dirent_cache_main_t dirent_cache_level0;
mdirents_cache_entry_t *dirent_cache_bucket_search_entry(dirent_cache_main_t *cache, fid_t fid, uint16_t index);
int dirent_remove_root_entry_from_cache(fid_t fid, int root_idx);

/*
**________________________________________________________________
*/
/*
** remove the dirent entries from the dirent cache

  @param fid: fid of the directory
  
  @retval 0 on success
  @retval < 0 on error (see errno for details)
  
*/
int rozofs_bt_remove_dirent_from_cache(fid_t fid,int *remove_count_p)	
{

  dentry_btmap_t *btmap_entry_p = NULL;
  dirent_dir_root_idx_bitmap_t *btmap_p;
  mdirents_cache_entry_t *dirent_p;
  uint8_t byte_value;  
  int remove_count = 0;
  int byte;
  int bit_idx;
  int root_idx;
  
  if (remove_count_p!= NULL) *remove_count_p = 0;

  btmap_entry_p = get_btmap_entry_by_fid(fid);
  if (btmap_entry_p == NULL)
  {
    /*
    ** nothing there
    */
    errno = ENOENT;
    return -1;
  }
  btmap_p = &btmap_entry_p->btmap;
  root_idx = 0;
  for (byte = 0; byte <DIRENT_FILE_BYTE_BITMAP_SZ;byte++)
  {
     byte_value =  btmap_p->bitmap[byte];
     if (byte_value == 0) continue;
     for (bit_idx = 0; bit_idx < 8; bit_idx++)
     {
       if ((byte_value & 1<<bit_idx) == 0) continue;
       root_idx = byte*8+bit_idx;
       dirent_p = dirent_cache_bucket_search_entry(&dirent_cache_level0,fid,root_idx);
       if (dirent_p == NULL)
       {
	  /*
	  ** nothing in the cache : go to the next entry
	  */
	  continue;
       }
       /*
       ** Remove the entry from the hash table
       */
       dirent_remove_root_entry_from_cache(fid,root_idx);
       /*
       ** now release the memory allocated to cache the dirent files
       */
       dirent_cache_release_entry(dirent_p);
       remove_count++;
     }
  }
  /*
  ** now remove the bitmap entry from the cache
  */
  del_btmap_entry(btmap_entry_p);
  if (remove_count_p!= NULL) *remove_count_p = remove_count;
  return 0;
}
