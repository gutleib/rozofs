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

#define _XOPEN_SOURCE 700
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <attr/xattr.h>
#include <sys/vfs.h>
#include <uuid/uuid.h>
#include <sys/types.h>
#include <inttypes.h>
#include <dirent.h>
#include <time.h>
#include <semaphore.h>

#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include <rozofs/common/common_config.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/rpc/export_profiler.h>
#include <rozofs/common/export_track.h>
#include <rozofs/rpc/epproto.h>
#include <rozofs/rpc/mclient.h>
#include <rozofs/core/rozofs_string.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/rozofs_cpu.h>
#include <rozofs/core/rozofs_flock.h>
#include "config.h"
#include "export.h"
#include "cache.h"
#include "mdirent.h"
#include "xattr_main.h"
#include "rozofs_quota_api.h"
#include "export_quota_thread_api.h"
#include "rozofs_exp_mover.h"
#include "export_thin_prov_api.h"
#include "exportd.h"
#include "rozofs_suffix.h"

#define ROZOFS_DIR_STATS 1
#ifdef ROZOFS_DIR_STATS
#warning Works with directory tracking
#endif

#include <rozofs/common/acl.h>
int rozofs_acl_access_check(const char *name, const char *value, size_t size,mode_t *mode_p);
/** Max entries of lv1 directory structure (nb. of buckets) */
#define MAX_LV1_BUCKETS 1024
#define LV1_NOCREATE 0
#define LV1_CREATE 1

/** Default mode for export root directory */
#define EXPORT_DEFAULT_ROOT_MODE S_IFDIR | S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;

extern epp_profiler_t  gprofiler;
/*
** Trash counters
*/
uint64_t export_rm_bins_trashed_count = 0; /**< Nb files put in trash from startup  */
uint64_t export_rm_bins_done_count    = 0; /**< Nb files actually deleted from startup  */
uint64_t export_rm_bins_reload_count  = 0; /**< Nb files dicovered in trash at startup  */
uint64_t export_rm_bins_in_ram        = 0; /**< Nb files in RAM to be deleted */
uint64_t export_rm_bins_in_flash      = 0; /**< Nb files in flash but not in RAM to be deleted */

uint64_t export_recycle_pending_count = 0; /**< recycle thread statistics  */
uint64_t export_recycle_done_count = 0; /**< recycle thread statistics  */
uint64_t export_rm_bins_threshold_high = 10; /**< trash threshold  where FID recycling starts */
uint64_t export_last_ticks = 0;
uint64_t export_last_us = 0;
uint64_t export_last_count = 0;
uint64_t export_last_count_json = 0; 
uint64_t export_last_trashed_json = 0; 
uint64_t export_fid_recycle_reload_count = 0; /**< number of recycle fid reloaded */
int export_fid_recycle_ready = 0; /**< assert to 1 when fid recycle have been reloaded */
int export_limit_rm_files;


uint64_t exportd_nb_directory_bad_children_count = 0;


typedef struct cnxentry {
    mclient_t *cnx;
    list_t list;
} cnxentry_t;


int export_lv2_resolve_path(export_t *export, fid_t fid, char *path);
/**
* internal structure used for bitmap root_idx: only for directory
*/
#define DIRENT_MAX_IDX_FOR_EXPORT 4096
#define DIRENT_MASK_FOR_EXPORT (4096-1)
#define DIRENT_FILE_BYTE_BITMAP_SZ (DIRENT_MAX_IDX_FOR_EXPORT/8)
typedef struct _dirent_dir_root_idx_bitmap_t
{
   int dirty; /**< assert to one if the bitmap must be re-written on disk */
   char bitmap[DIRENT_FILE_BYTE_BITMAP_SZ];
} dirent_dir_root_idx_bitmap_t;

int export_recycle_remove_from_tracking_file(export_t * e,recycle_mem_t *entry);


 

/*
**__________________________________________________________________

     A T T R I B U T E  W R I T E - B A C K    T H R E A D
**__________________________________________________________________
*/
#define EXPORT_MAX_ATT_THREADS  8
typedef struct _attr_writeback_ctx_t
{
  pthread_t               thrdId; 
  int                     thread_idx:31; 
  int                     sync:1;    
  lv2_entry_t             *lv2; 
  export_tracking_table_t *trk_tb_p;
  uint64_t                 wakeup_count;
  uint64_t                 err_count;
  uint64_t                 busy_count;
  sem_t                    export_attr_wr_ready;
  sem_t                    export_attr_wr_rq;
} attr_writeback_ctx_t;


attr_writeback_ctx_t rozofs_attr_thread_ctx_tb[EXPORT_MAX_ATT_THREADS];

/*
**__________________________________________________________________
*/
/*
** When either i_state or i_file_acl is set some extended attribute exist
** else no extended is set on this entry
*/
static inline int test_no_extended_attr(lv2_entry_t *lv2) {
  if ((lv2->attributes.s.i_state == 0)&&(lv2->attributes.s.i_file_acl == 0)) return 1;
  return 0;  
}
/*__________________________________________________________________
** Recopy the attributes of a lv2 cache entry into a structure attribute
** destinated to the rozofsmount. 
**
** @param lv2           The lv2 cache enty
** @param p             The attributes to be returned to the rozofsmount
**
*/
static inline void export_recopy_extended_attributes(export_t *e,lv2_entry_t *lv2,struct inode_internal_t * attrs) {    
  
  /*
  ** Re-copy the whole exportd attribute structure except the xattributes
  **
  ** The attrs pointer should actualy be an ep_mattr_t structure of a response 
  ** to rozofsmount and so have a size of 264
  **
  ** The struct inode_internal_t used by the export has a size of 240
  */
  memcpy(attrs, &lv2->attributes.s.attrs, sizeof(struct inode_internal_t)); 

  /*
  ** Clear extended attribute flag in mode field , when none is set
  */
  if (test_no_extended_attr(lv2)) rozofs_clear_xattr_flag(&attrs->attrs.mode);
  
  /*
  ** In case thin provisioning is not configured, 
  ** compute the number of blocks of a file from its size
  */
  if (S_ISREG(lv2->attributes.s.attrs.mode)) {
    if (e->thin == 0) {
      attrs->hpc_reserved.reg.nb_blocks_thin = (attrs->attrs.size+ROZOFS_BSIZE_BYTES(e->bsize)-1) / ROZOFS_BSIZE_BYTES(e->bsize);
    }
  }
}  
/*
**__________________________________________________________________
*/
static char *show_attr_thread_usage(char *pChar)
{
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"attr_thread <enable|disable> :enable/disable write behind attributes threads \n");
  pChar += sprintf(pChar,"attr_thread                  :display threads statistics \n");
  return pChar;
}
/*
**__________________________________________________________________
*/
/**
*   Evaluate the state of parent for to figure out the delete state

  state 0: VFS active/RozoFS active
  state 1: VFS active/RozoFS deleted --> the directory has been deleted
  state 2: VFS delete/RozoFS active --> designates the trash of an active directory: dir/@rozofs-trash@
  state 3: VFS delete/RozoFS delete --> designates the trash of a deleted directory --> not supported
  
  @param fid_interface : fid provided by the client
  @param fid_inode: fid from the inode context
  
  @retval state
*/  
static inline int rozofs_export_eval_parent_delete_state(fid_t fid_interface,fid_t fid_inode)
{
    int parent_state = 0;
    
    if (exp_metadata_inode_is_del_pending(fid_inode))parent_state = 1; /* bit 0: export */
    if (exp_metadata_inode_is_del_pending(fid_interface)) parent_state |=0x2;                       /* bit 1: VFS    */
    return parent_state;
}

/*
**__________________________________________________________________
*/
void show_attr_thread(char * argv[], uint32_t tcpRef, void *bufRef) 
{
    char *pChar = uma_dbg_get_buffer();
    attr_writeback_ctx_t       *thread_ctx_p;
    int i;
    int value1,value2;
    
    if (argv[1] != NULL)
    {
      if (strcmp(argv[1],"enable")==0) 
      {
	 common_config.export_attr_thread = 1;
         pChar += sprintf(pChar, "attribute threads are enabled\n");
         uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	 return;   	     
      }     
      if (strcmp(argv[1],"disable")==0) 
      {
	 common_config.export_attr_thread = 0;
         pChar += sprintf(pChar, "attribute threads are disabled\n");
         uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	 return;   	     
      }        
      if (strcmp(argv[1],"?")==0) 
      {
        show_attr_thread_usage(pChar);
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	return;   	     
      }  
      pChar += sprintf(pChar, "unsupported command: %s\n",argv[1]);
      show_attr_thread_usage(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
      return;    
    }
    
    thread_ctx_p = rozofs_attr_thread_ctx_tb;
    /*
    ** search if the lv2 is already under the control of one thread
    */
    pChar +=sprintf(pChar," attribute threads state: %s\n",(common_config.export_attr_thread)?"ENABLED":"DISABLED");
    pChar +=sprintf(pChar,"| thread | rdy |in_prg| wake-up cnt | err. cnt  |  busy cnt |\n");
    pChar +=sprintf(pChar,"+--------+-----+------+-------------+-----------+-----------+\n");
    for (i = 0; i < EXPORT_MAX_ATT_THREADS; i++,thread_ctx_p++)
    { 
       sem_getvalue(&thread_ctx_p->export_attr_wr_ready,&value1);
       sem_getvalue(&thread_ctx_p->export_attr_wr_rq,&value2);
       pChar +=sprintf(pChar,"|   %d    |  %d  |   %d  |  %8.8llu   | %8.8llu  | %8.8llu  |\n",
               i,value1,value2,
	       (unsigned long long int)thread_ctx_p->wakeup_count,
	       (unsigned long long int)thread_ctx_p->err_count,
	       (unsigned long long int)thread_ctx_p->busy_count);
       thread_ctx_p->busy_count = 0;
       thread_ctx_p->wakeup_count = 0;
    }
    pChar +=sprintf(pChar,"+--------+-----+------+-------------+-----------+-----------+\n");
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	     
}
/*
**__________________________________________________________________
*/
/**
*  Writeback thread used for storing attributes on disk

   That thread uses the export_attr_writeback_p context that describes
   the attributes to write back to disk: (child and/or parent attributes
   
   @param arg: pointer to the thread context
*/
void *export_wr_attr_th(void *arg) {    

   attr_writeback_ctx_t * ctx_p = (attr_writeback_ctx_t*)arg;
   char bufname[64];
    int ret;
    sprintf(bufname,"Attr. thread#%d",ctx_p->thread_idx);
    uma_dbg_thread_add_self(bufname);
  /*
  **  change the priority of the main thread
  */
#if 1
    {
      struct sched_param my_priority;
      int policy=-1;
      int ret= 0;

      pthread_getschedparam(pthread_self(),&policy,&my_priority);
          DEBUG("fuse reply thread Scheduling policy   = %s\n",
                    (policy == SCHED_OTHER) ? "SCHED_OTHER" :
                    (policy == SCHED_FIFO)  ? "SCHED_FIFO" :
                    (policy == SCHED_RR)    ? "SCHED_RR" :
                    "???");
 #if 1
      my_priority.sched_priority= 97;
      policy = SCHED_RR;
      ret = pthread_setschedparam(pthread_self(),policy,&my_priority);
      if (ret < 0) 
      {
	severe("error on sched_setscheduler: %s",strerror(errno));	
      }
      pthread_getschedparam(pthread_self(),&policy,&my_priority);
          info("fuse reply thread Scheduling policy (prio %d)  = %s\n",my_priority.sched_priority,
                    (policy == SCHED_OTHER) ? "SCHED_OTHER" :
                    (policy == SCHED_FIFO)  ? "SCHED_FIFO" :
                    (policy == SCHED_RR)    ? "SCHED_RR" :
                    "???");
 #endif        
     
    }  
#endif
    while(1)
    {  
      sem_post(&ctx_p->export_attr_wr_ready);
      /*
      ** wait for a command
      */
      sem_wait(&ctx_p->export_attr_wr_rq); 
      /*
      ** write the attributes of the child
      */
      if(ctx_p->lv2 != NULL)
      {
        ctx_p->wakeup_count++;
	ret = export_lv2_write_attributes(ctx_p->trk_tb_p,ctx_p->lv2,ctx_p->sync);
	if (ret < 0)
	{ 
	  if (errno != ENOENT) 
	  { 
	     severe("failed while writing child attributes %s %p",strerror(errno),ctx_p->lv2);
	     ctx_p->err_count++;
	  }
	}
	ctx_p->lv2 = NULL; 
      }
    }           
}

/*
**__________________________________________________________________
*/
/**
    Submit a write attribute to an attribute writeback thread
    
    @param lv2: level2 entry to write
    @param trk_tb_p: pointer to the tracking context tale
    @param sync: whether to force sync on disk of attributes
 
*/
/*static inline */void export_attr_thread_submit(lv2_entry_t*lv2,export_tracking_table_t *trk_tb_p, int sync)
{
    int i;
   attr_writeback_ctx_t       *thread_ctx_p;  
    int found = -1;
    int ret;
    
    /*
    ** check if the writeback threads are enabled
    */
    if (common_config.export_attr_thread == 0)
    {
	ret = export_lv2_write_attributes(trk_tb_p,lv2,sync);
	if (ret < 0)
	{ 
	  severe("failed while writing child attributes %s",strerror(errno));
	  rozofs_attr_thread_ctx_tb[0].err_count++;
	}
	return;        
    }
    
    thread_ctx_p = rozofs_attr_thread_ctx_tb;
    /*
    ** search if the lv2 is already under the control of one thread
    */
    for (i = 0; i < EXPORT_MAX_ATT_THREADS; i++,thread_ctx_p++)
    {
       if (thread_ctx_p->lv2 == lv2)
       {
	  sem_wait(&thread_ctx_p->export_attr_wr_ready);
	  thread_ctx_p->lv2 = lv2;
	  thread_ctx_p->sync = sync;
	  thread_ctx_p->trk_tb_p = trk_tb_p;
	  sem_post(&thread_ctx_p->export_attr_wr_rq); 
	  return;             
       } 
       if ((found < 0) && (thread_ctx_p->lv2 == NULL)) found = i;          
    }
    thread_ctx_p = &rozofs_attr_thread_ctx_tb[(found <0)?0:found];
    ret = sem_trywait(&thread_ctx_p->export_attr_wr_ready);
    if (ret < 0)
    {
       thread_ctx_p->busy_count++;
       sem_wait(&thread_ctx_p->export_attr_wr_ready);
    }
    thread_ctx_p->lv2 = lv2;
    thread_ctx_p->trk_tb_p = trk_tb_p;
    thread_ctx_p->sync = sync;
    sem_post(&thread_ctx_p->export_attr_wr_rq);     
}

/*
**__________________________________________________________________
*/
/**
    check for entry presence in thread contexts
    
    @param lv2: level2 entry to search
 
    @retval 1 on sucess
    @retval 0 if not found
*/
static inline int export_attr_thread_check_context(lv2_entry_t*lv2)
{
    int i;
    attr_writeback_ctx_t       *thread_ctx_p;  
    thread_ctx_p = rozofs_attr_thread_ctx_tb;
    
    if (lv2 == NULL) 
    {
      return 0;
    }
    /*
    ** search if the lv2 is already under the control of one thread
    */
    for (i = 0; i < EXPORT_MAX_ATT_THREADS; i++,thread_ctx_p++)
    {
       if (thread_ctx_p->lv2 == lv2)
       {
	  return 1;             
       }          
    }
    return 0;
}
/*
**__________________________________________________________________
*/
/**
*  Init of the attribute writeback thread

   @param none
   
   @retval 0 on success
   @retval -1 on error (see errno for details
*/
int export_wr_attr_th_init()
{
   int status = 0;
   pthread_attr_t             attr;
   int                        i,err;
   attr_writeback_ctx_t       *thread_ctx_p;  
  /*
  ** clear the thread table
  */
  memset(rozofs_attr_thread_ctx_tb,0,sizeof(rozofs_attr_thread_ctx_tb));
  /*
  ** Now create the threads
  */
  thread_ctx_p = rozofs_attr_thread_ctx_tb;
  for (i = 0; i < EXPORT_MAX_ATT_THREADS ; i++,thread_ctx_p++) 
  {
     err = pthread_attr_init(&attr);
     if (err != 0) {
       fatal("attr thread: pthread_attr_init(%d) %s",i,strerror(errno));
       return -1;
     }  
     /*
     ** init of the semaphore
     */
     sem_init(&thread_ctx_p->export_attr_wr_ready, 0, 0);
     sem_init(&thread_ctx_p->export_attr_wr_rq, 0, 0);       
     thread_ctx_p->thread_idx = i;
     err = pthread_create(&thread_ctx_p->thrdId,&attr,export_wr_attr_th,thread_ctx_p);
     if (err != 0) {
       fatal("attr thread: pthread_create(%d) %s",i, strerror(errno));
       return -1;
     }    
  }
  return status;
} 
 
/*
**__________________________________________________________________

     A T T R I B U T E  W R I T E - B A C K    T H R E A D  E N D
**__________________________________________________________________
*/

/*
 **__________________________________________________________________
 ** Missing directory repair statistics
 */

uint64_t export_repair_missing_directory_success_counter = 0;
uint64_t export_repair_missing_directory_failure_counter = 0;
/*
 **__________________________________________________________________
 */

/**
* service to repair a missing directory. This directory is referred 
  in a direntry of its parent, but /root/<slice>/<FID> directory does
  not exist in the meta data.

  @param node_path : The missing directoy path
  
  @retval 0 on success
  @retval < 0 on error
*/

int export_repair_missing_directory(char * node_path) {

    dirent_dir_root_idx_bitmap_t root_idx_bitmap;
    dirent_dir_root_idx_bitmap_t *root_idx_bitmap_p = &root_idx_bitmap;
    mdir_t node_mdir;
    
    if (mkdir(node_path, S_IRWXU) != 0) {
        export_repair_missing_directory_failure_counter++;
        return -1;
    }	   
    // write attributes to mdir file
    if (mdir_open(&node_mdir, node_path) < 0) {
        export_repair_missing_directory_failure_counter++;
	severe("mdir_open(%s) (success=%llu,failure=%llu) %s",
		node_path,
	       (unsigned long long int)export_repair_missing_directory_success_counter, 
	       (unsigned long long int)export_repair_missing_directory_failure_counter,	       
	       strerror(errno));
        return -1;
	
    }
    /*
    ** write the initial bitmap on disk
    */
    memset(&root_idx_bitmap,0,sizeof(dirent_dir_root_idx_bitmap_t));
    ssize_t lenbit = pwrite(node_mdir.fdattrs, root_idx_bitmap_p->bitmap,DIRENT_FILE_BYTE_BITMAP_SZ,0);
    if (lenbit != DIRENT_FILE_BYTE_BITMAP_SZ)
    {
       export_repair_missing_directory_failure_counter++;
       severe("write root_idx bitmap failure (%s) (success=%llu,failure=%llu) %s",
               node_path,
	       (unsigned long long int)export_repair_missing_directory_success_counter, 
	       (unsigned long long int)export_repair_missing_directory_failure_counter,	       
	       strerror(errno));
       mdir_close(&node_mdir);
       return -1;       
    }
    mdir_close(&node_mdir);
    
    export_repair_missing_directory_success_counter++;
    severe("export_repair_missing_directory(%s) success (success=%llu,failure=%llu)",
            node_path, 
	    (unsigned long long int)export_repair_missing_directory_success_counter, 
	    (unsigned long long int)export_repair_missing_directory_failure_counter);   
    return 0;
}
/*
 **__________________________________________________________________
 ** Missing attribute file repair statistics
 */

uint64_t export_repair_missing_attribute_file_success_counter = 0;
uint64_t export_repair_missing_attribute_file_failure_counter = 0;
/*
 **__________________________________________________________________
 */

/**
* service to repair a directory which attribute file is missing or
  corrupted (wrong size)

  @param node_path : The missing directoy path
  
  @retval 0 on success
  @retval < 0 on error
*/

int export_repair_missing_attribute_file(char * node_path) {
    dirent_dir_root_idx_bitmap_t root_idx_bitmap;
    dirent_dir_root_idx_bitmap_t *root_idx_bitmap_p = &root_idx_bitmap;
    mdir_t node_mdir;
    char   direntry_name[32];
    int idx;
    
    // Create attributes to mdir file
    if (mdir_open(&node_mdir, node_path) < 0) {
        export_repair_missing_attribute_file_failure_counter++;
	severe("mdir_open(%s) (success=%llu,failure=%llu) %s",
		node_path,
	       (unsigned long long int)export_repair_missing_attribute_file_success_counter, 
	       (unsigned long long int)export_repair_missing_attribute_file_failure_counter,       
	       strerror(errno));
        return -1;
	
    }
    /*
    ** Initialize the initial bitmap on disk
    */
    memset(&root_idx_bitmap,0,sizeof(dirent_dir_root_idx_bitmap_t));
    
    /*
    ** Loop on the direntry files
    */
    for (idx=0; idx < DIRENT_MAX_IDX_FOR_EXPORT; idx++) {
      
      sprintf(direntry_name,"d_%d", idx);
    
      /*
      ** Check whether the root direntry file exist
      */
      if (faccessat(node_mdir.fdp,direntry_name,W_OK,AT_EACCESS) == 0) {
        export_dir_update_root_idx_bitmap(&root_idx_bitmap,idx,1);
      }
    }
    
    /*
    ** Write the bitmap on flash
    */
    ssize_t lenbit = pwrite(node_mdir.fdattrs, root_idx_bitmap_p->bitmap,DIRENT_FILE_BYTE_BITMAP_SZ,0);
    if (lenbit != DIRENT_FILE_BYTE_BITMAP_SZ)
    {
       export_repair_missing_attribute_file_failure_counter++;
       severe("write root_idx bitmap failure (%s) (success=%llu,failure=%llu) %s",
               node_path,
	       (unsigned long long int)export_repair_missing_attribute_file_success_counter, 
	       (unsigned long long int)export_repair_missing_attribute_file_failure_counter,	       
	       strerror(errno));
       mdir_close(&node_mdir);
       return -1;       
    }
    mdir_close(&node_mdir);
    
    export_repair_missing_attribute_file_success_counter++;
    severe("export_repair_missing_attribute_file(%s) success (success=%llu,failure=%llu)",
            node_path, 
	    (unsigned long long int)export_repair_missing_attribute_file_success_counter, 
	    (unsigned long long int)export_repair_missing_attribute_file_failure_counter);	       
    return 0;
}
/*
 **__________________________________________________________________
 */
/**
* service to check if the bitmap for root_idx must be loaded

  @param lvl2 : level 2 entry
  @param fid : file id of the directory--> not used anymore
  @param e:   pointer to the export structure
  
  @retval 0 on success
  @retval < 0 on error
*/

int export_dir_load_root_idx_bitmap(export_t *e,fid_t fid,lv2_entry_t *lvl2)
{
   int fd = -1;
   char node_path[PATH_MAX];
   char lv3_path[PATH_MAX];
   dirent_dir_root_idx_bitmap_t *bitmap_p;
   int loop_control = 0;

   if (lvl2->dirent_root_idx_p != NULL)
   {
     /*
     ** already loaded
     */
     return 0;   
   }
   /*
   ** the entry must be a directory
   */
   if (!S_ISDIR(lvl2->attributes.s.attrs.mode)) 
   {
     return 0;
   }
   /*
   ** allocate the memory
   */
   lvl2->dirent_root_idx_p = memalign(32,sizeof(dirent_dir_root_idx_bitmap_t));
   if (lvl2->dirent_root_idx_p == NULL) goto error;
   bitmap_p = (dirent_dir_root_idx_bitmap_t*)lvl2->dirent_root_idx_p;
   /*
   ** read the bitmap from disk
   */    
   if (export_lv2_resolve_path(e, lvl2->attributes.s.attrs.fid, node_path) != 0) goto error;
   
   /*
   ** Check whether the directory exist
   */
   if (access(node_path,W_OK) != 0) {
     switch(errno) {
       case ENOENT:
       {
         if (export_repair_missing_directory(node_path) != 0) {
	   goto error;
	 }  
         break;
       }
       
       default:
         severe("acces(%s) %s",node_path,strerror(errno));
	 goto error;
     }
   }
   
   sprintf(lv3_path, "%s/%s", node_path, MDIR_ATTRS_FNAME);  
   loop_control = 0;
reloop:
   if ((fd = open(lv3_path, O_RDONLY | O_NOATIME, S_IRWXU)) < 0) 
   {
     switch(errno) {
       case ENOENT:
       {
          if (loop_control == 1) goto error;
	  loop_control = 1;
	  	  
          if (export_repair_missing_attribute_file(node_path) != 0) {
	    goto error;
	  }  
	  goto reloop;
          break;
       }
       default:
         severe("open(%s) %s",lv3_path,strerror(errno));
	 goto error;       
     }
   }
   
   ssize_t len = pread(fd,bitmap_p->bitmap,DIRENT_FILE_BYTE_BITMAP_SZ,0);
   if (len != DIRENT_FILE_BYTE_BITMAP_SZ) {
     if (loop_control == 1) goto error;

     close(fd);
     fd = -1;    	
     loop_control = 1;  	  
     if (export_repair_missing_attribute_file(node_path) != 0) {
	goto error;
     }  
     goto reloop;       
   }
     
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
   if (lvl2->dirent_root_idx_p != NULL)
   {
      free(lvl2->dirent_root_idx_p);
      lvl2->dirent_root_idx_p = NULL;  
   }
   return -1;
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
       if ((bitmap_p->bitmap[byte_idx] & (1<<bit_idx))==0) return;
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
    
    if (ctx_p == NULL) 
    {
       severe("No bitmap pointer");
       return 1;
    }
    
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
* service to flush on disk the root_idx bitmap if it is dirty

  @param bitmap_p : pointer to the root_idx bitmap
  @param fid : file id of the directory
  @param e:   pointer to the export structure
  
  @retval 0 on success
  @retval < 0 on error
*/

int export_dir_flush_root_idx_bitmap(export_t *e,fid_t fid,dirent_dir_root_idx_bitmap_t *bitmap_p)
{
   int fd = -1;
   char node_path[PATH_MAX];
   char lv3_path[PATH_MAX];

   if (bitmap_p == NULL)
   {
     /*
     ** nothing to flush
     */
     return 0;   
   }
   if (bitmap_p->dirty == 0) return 0;
   /*
   ** bitmap has changed :write the bitmap on disk
   */    
   if (export_lv2_resolve_path(e, fid, node_path) != 0) goto error;
   
   sprintf(lv3_path, "%s/%s", node_path, MDIR_ATTRS_FNAME);   
   if ((fd = open(lv3_path, O_WRONLY | O_CREAT | O_NOATIME, S_IRWXU)) < 0) {
        goto error;
   }
   ssize_t len = pwrite(fd,bitmap_p->bitmap,DIRENT_FILE_BYTE_BITMAP_SZ,0);
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
 **__________________________________________________________________
 */

/** get the lv1 directory.
 *
 * lv1 entries are first level directories of an export root named by uint32_t
 * string value and used has entry of a hash table storing the export
 * meta data files.
 *
 * @param root_path: root path of the exportd
 * @param slice: value of the slice
 *
 * @return 0 on success otherwise -1
 */
static inline int mstor_slice_resolve_entry(char *root_path, uint32_t slice) {
    char path[PATH_MAX];
    sprintf(path, "%s/%"PRId32"", root_path, slice);
    if (access(path, F_OK) == -1) {
        if (errno == ENOENT) {
            /*
             ** it is the fisrt time we access to the slice
             **  we need to create the level 1 directory and the 
             ** timestamp file
             */
            if (mkdir(path, S_IRUSR | S_IWUSR | S_IXUSR) != 0) {
                severe("mkdir (%s): %s", path, strerror(errno));
                return -1;
            }
            //          mstor_ts_srv_create_slice_timestamp_file(export,slice); 
            return 0;
        }
        /*
         ** any other error
         */
        severe("access (%s): %s", path, strerror(errno));
        return -1;
    }
    return 0;
}

/*
 **__________________________________________________________________
 */

/** get the subslice directory index.
 *
 * lv1 entries are first level directories of an export root named by uint32_t
 * string value and used has entry of a hash table storing the export
 * meta data files.
 *
 * @param root_path: root path of the exportd
 * @param fid: the search fid
 *
 * @return 0 on success otherwise -1
 */
static inline int mstor_subslice_resolve_entry(char *root_path, fid_t fid, uint32_t slice, uint32_t subslice) {
    char path[PATH_MAX];


    sprintf(path, "%s/%d", root_path, slice);
    if (access(path, F_OK) == -1) {
        if (errno == ENOENT) {
            /*
             ** it is the fisrt time we access to the subslice
             **  we need to create the associated directory 
             */
            if (mkdir(path, S_IRUSR | S_IWUSR | S_IXUSR) != 0) {
                severe("mkdir (%s): %s", path, strerror(errno));
                return -1;
            }
            return 0;
        }
        severe("access (%s): %s", path, strerror(errno));
        return -1;
    }
    return 0;
}
/*
**__________________________________________________________________
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
    fid_t fid_save;
   
    /*
    ** clear the delete pending bit for path resolution
    */
    memcpy(fid_save,fid,sizeof(fid_t));
    exp_metadata_inode_del_deassert(fid_save);
    /*
     ** extract the slice and subsclie from the fid
     */
    mstor_get_slice_and_subslice(fid_save, &slice, &subslice);
    /*
     ** convert the fid in ascii
     */
    rozofs_uuid_unparse(fid_save, str);
    sprintf(path, "%s/%d/%s", root_path, slice, str);
    return 0;

    return -1;
}
/*
**__________________________________________________________________
*/
/** build a full path based on export root and fid of the lv2 file
 *
 * lv2 is the second level of files or directories in storage of metadata
 * they are acceded thru mreg or mdir API according to their type.
 *
 * @param export: the export we are searching on
 * @param fid: the fid we are looking for
 * @param path: the path to fill in
 */

int export_lv2_resolve_path(export_t *export, fid_t fid, char *path) {
    int ret;

    START_PROFILING(export_lv2_resolve_path);

    ret = export_lv2_resolve_path_internal(export->root, fid, path);

    STOP_PROFILING(export_lv2_resolve_path);
    return ret;
}

/*
**__________________________________________________________________
*/
/**
*  open the parent directory

   @param e : pointer to the export structure
   @param parent : fid of the parent directory
   
   @retval > 0 : fd of the directory
   @retval < 0 error
*/
int export_open_parent_directory(export_t *e,fid_t parent)
{    
    dirent_current_eid = e->eid;
//    dirent_export_root_path = &e->root[0]; 
    return -1;
}

/*
**__________________________________________________________________

     D I R E C T O R Y   S T A T I S T I C S 
**__________________________________________________________________
*/
/**
*  The goal of the following service is to keep that of the following:
     - total number of bytes within a directory
     - tracking any change that could occur on a child that belongs to the directory
*/

/*
**__________________________________________________________________
*/
/**

 adjust the cumulative byte sizes of a directory. The goal is to maintain the directory
 statistics. The child can be either a directory, a regular file
 or a symbolic link.
 
 @param dir: pointer to the directory i-node (cache structure)
 @param size: size in bytes units to remove (regular file only)
 @param add: 1:add/0: remove


*/
void export_dir_adjust_child_size(lv2_entry_t *dir,uint64_t size,int add,int bbytes)
{
   ext_dir_mattr_t *stats_attr_p;
   uint64_t rounded_size;
   
   if (size == 0) return;
   if (size%bbytes)
   {   
     rounded_size = size/bbytes+1;
     rounded_size *=bbytes;
   }
   else
   {
     rounded_size = size;
   }

   stats_attr_p = (ext_dir_mattr_t *)&dir->attributes.s.attrs.sids[0];

    if (add)
    {
      stats_attr_p->s.nb_bytes +=rounded_size;
    } 
    else
    {
      if (stats_attr_p->s.nb_bytes < rounded_size) stats_attr_p->s.nb_bytes = 0;
      else stats_attr_p->s.nb_bytes -=rounded_size;    
    
    }
    /*
    ** mark the entry as dirty
    */
    dir->dirty_bit = 1;
}
/*
**__________________________________________________________________
*/
/**

 adjust the modification time of a directory. The goal is to maintain the directory
 statistics. The child can be either a directory, a regular file
 or a symbolic link.
 
 @param dir: pointer to the directory i-node (cache structure)



*/
void export_dir_update_time(lv2_entry_t *dir)
{
   ext_dir_mattr_t *stats_attr_p;
   uint64_t cur_time;

   stats_attr_p = (ext_dir_mattr_t *)&dir->attributes.s.attrs.sids[0];
   cur_time = time(NULL);   
   /*
   ** update the time only if it is not in the configured period
   */
   if (stats_attr_p->s.update_time < cur_time)
   {
      if (stats_attr_p->s.version == 0) stats_attr_p->s.version = ROZOFS_DIR_VERSION_1;
      stats_attr_p->s.update_time = time(NULL)+common_config.expdir_guard_delay_sec;
   }
   dir->dirty_bit = 0;
}
/*
**__________________________________________________________________
*/
/**
*  Get the parent directory for statistics update

   @param e: pointer to the export configuration
   @param child_lv2: pointer to the lv2 entry of the child
   
   @retval NULL: not found or the export is not configure with backup feature
   @retval <> NULL : pointer to the lv2 entry that contains the parent attributes
*/
lv2_entry_t *export_dir_get_parent(export_t *e, lv2_entry_t *child_lv2)
{
   lv2_entry_t *plv2;
   
//   if (e->backup == 0) return NULL;
   plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, child_lv2->attributes.s.pfid);
   return plv2;
}
   
/*
**__________________________________________________________________
*/
/**

 Re-write the directory attributes if it is time to do so
 
 @param e: pointer to texport context
 @param dir: pointer to the directory i-node (cache structure)


*/
void export_dir_async_write(export_t *e,lv2_entry_t *dir)
{
   ext_dir_mattr_t *stats_attr_p;
   uint64_t cur_time;

   stats_attr_p = (ext_dir_mattr_t *)&dir->attributes.s.attrs.sids[0];
   /*
   ** check if is time type to update the i-node on disk
   */
   cur_time = time(NULL);
   if (stats_attr_p->s.update_time < cur_time) 
   {
     START_PROFILING_EID(dir_wr_on_time,e->eid);
     if (stats_attr_p->s.version == 0) stats_attr_p->s.version = ROZOFS_DIR_VERSION_1;
      /*
      ** re-write attributes on disk
      */
      dir->dirty_bit = 0;
      stats_attr_p->s.update_time = common_config.expdir_guard_delay_sec+cur_time;
      export_attr_thread_submit(dir,e->trk_tb_p,0);   
      STOP_PROFILING_EID(dir_wr_on_time,e->eid);

   }
}

/*
**__________________________________________________________________
*/
/**

 check for Re-write the directory attributes if it is time to do so
 
 @param e: pointer to texport context
 @param dir: pointer to the directory i-node (cache structure)


*/
void export_dir_check_async_write(export_t *e,lv2_entry_t *dir)
{
   ext_dir_mattr_t *stats_attr_p;
   
   stats_attr_p = (ext_dir_mattr_t *)&dir->attributes.s.attrs.sids[0];
   
   /*
   ** check dirty_bit
   */
   if (dir->dirty_bit == 0) return;
   /*
   ** check if is time type to update the i-node on disk: we do not change the update time since it assumed
   ** that is was already done
   */
   if (stats_attr_p->s.update_time < time(NULL)) 
   {
     START_PROFILING_EID(dir_wr_on_check,e->eid);

     if (stats_attr_p->s.version == 0) stats_attr_p->s.version = ROZOFS_DIR_VERSION_1;
      /*
      ** re-write attributes on disk
      */
      dir->dirty_bit = 0;
      export_attr_thread_submit(dir,e->trk_tb_p,0);   
      STOP_PROFILING_EID(dir_wr_on_check,e->eid);
   }
}


/*
**__________________________________________________________________
*/
/**

 check for Re-write the directory attributes if it is time to do so when the entry is removed from lv2 cache
 
 @param dir: pointer to the directory i-node (cache structure)


*/
void export_dir_check_sync_write_on_lru(lv2_entry_t *dir)
{
   ext_dir_mattr_t *stats_attr_p;
   export_t *e;
   eid_t eid;  

   /*
   ** check dirty_bit
   */
   if (dir->dirty_bit == 0) return;
      
    
   stats_attr_p = (ext_dir_mattr_t *)&dir->attributes.s.attrs.sids[0];
   
   /*
   ** check if is time type to update the i-node on disk: we do not change the update time since it assumed
   ** that is was already done
   */
   {
      eid = rozofs_get_eid_from_fid(dir->attributes.s.attrs.fid);
      e = exports_lookup_export(eid);
      if (e == NULL)
      {
	 /*
	 ** quite strange, but since it is for statistics, just return without warning
	 */
	 return;
      }

     START_PROFILING_EID(dir_wr_on_lru,e->eid);

     if (stats_attr_p->s.version == 0) stats_attr_p->s.version = ROZOFS_DIR_VERSION_1;
      /*
      ** re-write attributes on disk
      */
      dir->dirty_bit = 0;
      export_lv2_write_attributes(e->trk_tb_p,dir,0);   
      STOP_PROFILING_EID(dir_wr_on_lru,e->eid);
   }
}
/*
**__________________________________________________________________

     D I R E C T O R Y   S T A T I S T I C S  END
**__________________________________________________________________
*/
/*
**__________________________________________________________________
*/
/** update the number of files in file system
 *
 * @param e: the export to update
 * @param n: number of files
   @param children: contains the indication if the file has been allocated on a fast volume
 *
 * @return 0 on success -1 otherwise
 */
static int export_update_files(export_t *e, int32_t n,uint32_t children) {
    int status = -1;
    rozofs_mover_children_t vid_fast;
    
    START_PROFILING(export_update_files);
    vid_fast.u32 = children;
    if (n > 0) {
      status = export_fstat_create_files(e->eid,n,vid_fast.fid_st_idx.vid_fast);
    }
    else {
      status = export_fstat_delete_files(e->eid,-n,vid_fast.fid_st_idx.vid_fast);
    }
      

    STOP_PROFILING(export_update_files);
    return status;
}
/*
**__________________________________________________________________
*/
/** update the number of blocks in file system
 *
  @param e: the export to update
  @param newblocks: new number of blocks
  @param oldblocks: old number of blocks
  @param children: children field of a regular file that might contains the reference of a fast volume
 
 * @return 0 on success -1 otherwise
 */
static int export_update_blocks(export_t * e, uint64_t newblocks, uint64_t oldblocks,uint32_t children) {
    int status = -1;
    rozofs_mover_children_t vid_fast;

    if (oldblocks == newblocks) return 0;
    
    START_PROFILING(export_update_blocks);
    vid_fast.u32 = children;
     status = export_fstat_update_blocks(e->eid, newblocks, oldblocks,e->thin,vid_fast.fid_st_idx.vid_fast);
    STOP_PROFILING(export_update_blocks);
    return status;
}

/*
**__________________________________________________________________
*/
/** update the number of blocks in file system for an exportd that
    has been configured with thin provisioning
 
  @param e: the export to update
  @param nb_blocks: number of blocks (4KB unit)
  @param dir: 1: add/-1: substract
 
  @return 0 on success -1 otherwise
 */
static int expthin_update_blocks(export_t * e, uint32_t nb_blocks, int dir) {
    int status = -1;
    
    START_PROFILING(export_update_blocks);
     status = expthin_fstat_update_blocks(e->eid, nb_blocks, dir);
    STOP_PROFILING(export_update_blocks);
    return status;
}

/*
**__________________________________________________________________
*/
/** constants of the export */
typedef struct export_const {
    char version[20]; ///< rozofs version
    fid_t rfid; ///< root id
} export_const_t;

int export_is_valid(const char *root) {
    char path[PATH_MAX];
    char fstat_path[PATH_MAX];
    char const_path[PATH_MAX];
    int i;

    if (!realpath(root, path))
        return -1;

    if (access(path, R_OK | W_OK | X_OK) != 0)
        return -1;
    for (i = 1 ; i <= EXPORT_SLICE_PROCESS_NB;i++)
    {
      // check fstat file
      sprintf(fstat_path, "%s/%s_%d", path, FSTAT_FNAME,i);
      if (access(fstat_path, F_OK) != 0)
          return -1;
    }
    // check const file
    sprintf(const_path, "%s/%s", path, CONST_FNAME);
    if (access(const_path, F_OK) != 0)
        return -1;

    return 0;
}

/*
**__________________________________________________________________
*/
int export_create(const char *root,export_t * e,lv2_cache_t *lv2_cache) {
    const char *version = VERSION;
    char path[PATH_MAX];
    char trash_path[PATH_MAX];
    char fstat_path[PATH_MAX];
    char const_path[PATH_MAX];
    char root_path[PATH_MAX];
    char slice_path[PATH_MAX];
    char root_export_host_id[PATH_MAX];

    export_fstat_t est;
    export_const_t ect;
    int fd = -1;
    mdir_t root_mdir;
    ext_mattr_t ext_attrs;
    uint32_t pslice = 0;
    dirent_dir_root_idx_bitmap_t root_idx_bitmap;

    memset(&root_idx_bitmap,0,sizeof(dirent_dir_root_idx_bitmap_t));
    
    e->trk_tb_p = NULL;
    e->lv2_cache = lv2_cache;

    int i;

    if (!realpath(root, path))
        return -1;
    /*
    ** set the eid and the root path in the associated global variables
    */
    if (!realpath(root, e->root))
        return -1;
    
    export_open_parent_directory(e,NULL);
	
    if ( expgwc_non_blocking_conf.slave == 0)
    {
      /*
      ** create the tracking context of the export
      */
      sprintf(root_export_host_id,"%s/host%d",root,rozofs_get_export_host_id());
      if (mkdir(root_export_host_id, S_IRUSR | S_IWUSR | S_IXUSR) != 0) {
	 severe("error on directory creation %s: %s\n",root_export_host_id,strerror(errno));
	 return -1;      
      }
      e->trk_tb_p = exp_create_attributes_tracking_context(e->eid,(char*)root_export_host_id,1);
      if (e->trk_tb_p == NULL)
      {
	 severe("error on tracking context allocation: %s\n",strerror(errno));
	 return -1;  
      }
      /*
      ** create the context for managing user and group quota
      */
      e->quota_p = rozofs_qt_alloc_context(e->eid,(char*)root,1);
      if (e->quota_p == NULL)
      {
	 severe("error on quota context allocation: %s\n",strerror(errno));
	 return -1;  
      }
      for (i = 0 ; i <= EXPORT_SLICE_PROCESS_NB;i++)
      {
	// create trash directory
	sprintf(trash_path, "%s/%s_%d", path, TRASH_DNAME,i);
	if (mkdir(trash_path, S_IRUSR | S_IWUSR | S_IXUSR) < 0) {
            return -1;
	}
      }
      /*
      ** create the directories slices
      */
      for (i = 0 ; i <= MAX_SLICE_NB;i++)
      {
	// create slices for directories
	sprintf(slice_path, "%s/%d", path,i);
	if (mkdir(slice_path, S_IRUSR | S_IWUSR | S_IXUSR) < 0) {
            return -1;
	}
      }
            
      for (i = 0 ; i <= EXPORT_SLICE_PROCESS_NB;i++)
      {
	// create fstat file
	sprintf(fstat_path, "%s/%s_%d", path, FSTAT_FNAME,i);
	if ((fd = open(fstat_path, O_RDWR | O_CREAT, S_IRWXU)) < 1) {
            return -1;
	}

	est.blocks = est.files = 0;
	if (write(fd, &est, sizeof (export_fstat_t)) != sizeof (export_fstat_t)) {
            close(fd);
            return -1;
	}
	close(fd);
      }

      //create root
      memset(&ext_attrs, 0, sizeof (ext_attrs));
      ext_attrs.s.attrs.cid = 0;
      memset(ext_attrs.s.attrs.sids, 0, ROZOFS_SAFE_MAX * sizeof (sid_t));

      // Put the default mode for the root directory
      ext_attrs.s.attrs.mode = EXPORT_DEFAULT_ROOT_MODE;
      rozofs_clear_xattr_flag(&ext_attrs.s.attrs.mode);

      ext_attrs.s.attrs.nlink = 2;
      ext_attrs.s.attrs.uid = 0; // root
      ext_attrs.s.attrs.gid = 0; // root
      if ((ext_attrs.s.attrs.ctime = ext_attrs.s.attrs.atime = ext_attrs.s.attrs.mtime = ext_attrs.s.cr8time =time(NULL)) == -1)
          return -1;
      ext_attrs.s.attrs.size = ROZOFS_DIR_SIZE;
      // Set children count to 0
      ext_attrs.s.attrs.children = 0;

      if(exp_attr_create_write_cond(e->trk_tb_p,pslice,&ext_attrs,ROZOFS_DIR,NULL,1) < 0)
      {
        severe("cannot allocate an inode for root directory");
	return -1;
      }
      /*
       ** create the slice and subslice directory for root if they don't exist
       ** and then create the "fid" directory or the root
       */
      if (export_lv2_resolve_path_internal(path, ext_attrs.s.attrs.fid, root_path) != 0)
          return -1;
	  
      if (mkdir(root_path, S_IRUSR | S_IWUSR | S_IXUSR) < 0) {
          return -1;
      }
      // open the root mdir
      if (mdir_open(&root_mdir, root_path) != 0) {
          severe("cannot open %s: %s",root_path,strerror(errno));
          return -1;
      }
      // Initialize the dirent level 0 cache
      dirent_cache_level0_initialize();
      dirent_wbcache_init();
      dirent_wbcache_disable();

     lv2_cache_put_forced(e->lv2_cache,ext_attrs.s.attrs.fid,&ext_attrs);
      /*
      ** write root idx bitmap on disk
      */
       ssize_t lenbit = pwrite(root_mdir.fdattrs,root_idx_bitmap.bitmap,DIRENT_FILE_BYTE_BITMAP_SZ,0);
       if (lenbit != DIRENT_FILE_BYTE_BITMAP_SZ)
       {
          severe("write root_idx bitmap failure %s",root_path);
          mdir_close(&root_mdir);
          return -1;       
       }
       mdir_close(&root_mdir);

      // create const file.
      memset(&ect, 0, sizeof (export_const_t));
      uuid_copy(ect.rfid, ext_attrs.s.attrs.fid);
      strncpy(ect.version, version, 20);
      sprintf(const_path, "%s/%s", path, CONST_FNAME);
      if ((fd = open(const_path, O_RDWR | O_CREAT, S_IRWXU)) < 1) {
          severe("open failure for %s: %s",const_path,strerror(errno));
          return -1;
      }

      if (write(fd, &ect, sizeof (export_const_t)) != sizeof (export_const_t)) {
          severe("write failure for %s: %s",const_path,strerror(errno));
          close(fd);
          return -1;
      }
      close(fd);
    }
    return 0;
}

static void *load_trash_dir_thread(void *v) {

    export_t *export = (export_t*) v;

    uma_dbg_thread_add_self("Reload trash");

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

    // Load files to recycle in the fid recycle list
    if (export_load_recycle_entry(export) != 0) {
        severe("export_load_recycle_entry failed: %s", strerror(errno));
        return 0;
    }

    // Load files to delete in trash list
    if (export_load_rmfentry(export) != 0) {
        severe("export_load_rmfentry failed: %s", strerror(errno));
        return 0;
    }

    export->load_trash_thread = 0;
    
    uma_dbg_thread_remove_self();

    info("Load trash directory pthread completed successfully (eid=%d)",
            export->eid);

    return 0;
}
/*
**__________________________________________________________________
*/
int export_initialize(export_t * e, volume_t *volume, uint8_t layout, ROZOFS_BSIZE_E bsize,
        lv2_cache_t *lv2_cache, uint32_t eid, const char *root, const char *name, const char *md5,
        uint64_t squota, uint64_t hquota, char * filter_name, uint8_t thin, volume_t *volume_fast,uint64_t hquota_fast,int suffix_file, uint8_t flockp) {

    char fstat_path[PATH_MAX];
    char const_path[PATH_MAX];
    char root_path[PATH_MAX];
    char root_export_host_id[PATH_MAX];
    export_const_t ect;
    int fd = -1;
    int i = 0;
    /*
    ** do it for eid if the process is master. For the slaves do it for
    ** the eid that are in their scope only
    */
    if (exportd_is_master()== 0) 
    {   
      if (exportd_is_eid_match_with_instance(eid) ==0) return 0;
    }

    if (!realpath(root, e->root))
    {
        severe("realpath failure for %s : %s",root,strerror(errno));
        return -1;
    }
    /*
    ** save the root in dirent_export_root_path table
    */
    {
      char * new_path = strdup(e->root);
      char * old_path = dirent_export_root_path[eid];
      dirent_export_root_path[eid]= new_path;
      if (old_path != NULL) {
        free(old_path);
      }
    }    
    
    strcpy(e->name, name);
    
    /*
    ** set the eid and the root path in the associated global varoiables
    */
    export_open_parent_directory(e,NULL);
    
    e->eid = eid;
    e->volume = volume;
    e->volume_fast = volume_fast;
    e->bsize = bsize;
    e->thin = thin;
    e->flockp = flockp;
    e->lv2_cache = lv2_cache;
    if (layout<LAYOUT_MAX) {
      e->layout = layout; // Layout used for this volume
    }
    else {
      e->layout = volume->layout; // Layout used for this volume
    }  
    /*
    ** Retrieve filter tree to use for this export from its name
    ** when name does not exist tree is NULL and IP is allowed.
    */
    e->filter_tree = rozofs_ip4_flt_get_tree(filter_name);   
    
    e->load_trash_thread = 0;
    
    /*
    ** Meta-data device resource supervision
    */
    memset(&e->space_left, 0, sizeof(meta_resources_t));

    
    /*
    ** init of the replication context
    */
    {
      int k;
      for (k = 0; k < EXPORT_GEO_MAX_CTX; k++)
      {
        e->geo_replication_tb[k] = geo_rep_init(eid,k,(char*)root);
	if (e->geo_replication_tb[k] == NULL)
	{
	   return -1;
	}
	geo_rep_dbg_add(e->geo_replication_tb[k]);      
      }    
    }
    /*
    ** create the tracking context of the export
    */
     if (e->trk_tb_p == NULL)
     {
       sprintf(root_export_host_id,"%s/host%d",root,rozofs_get_export_host_id());

       e->trk_tb_p = exp_create_attributes_tracking_context(e->eid,(char*)root_export_host_id,1);
       if (e->trk_tb_p == NULL)
       {
	  severe("error on tracking context allocation: %s\n",strerror(errno));
	  return -1;  
       }
     }
     if (e->quota_p == NULL)
     {
       /*
       ** create the context for managing user and group quota
       */
       e->quota_p = rozofs_qt_alloc_context(e->eid,(char*)root,1);
       if (e->quota_p == NULL)
       {
	  severe("error on quota context allocation: %s\n",strerror(errno));
	  return -1;  
       }
     }

    // Initialize the dirent level 0 cache
    dirent_cache_level0_initialize();
    dirent_wbcache_init();

    if (strlen(md5) == 0) {
        memcpy(e->md5, ROZOFS_MD5_NONE, ROZOFS_MD5_SIZE);
    } else {
        memcpy(e->md5, md5, ROZOFS_MD5_SIZE);
    }
    e->squota = squota;
    e->hquota = hquota;
    e->hquota_fast = hquota_fast;
    e->suffix_file_idx = suffix_file;
    /*
    ** inform that the suffix file need to be parsed
    */
    if (e->suffix_file_idx != -1) rozofs_suffix_file_parse_req( e->suffix_file_idx);
    // open the export_stat file an load it
    sprintf(fstat_path, "%s/%s_%d", e->root, FSTAT_FNAME,(int)expgwc_non_blocking_conf.instance);
    if ((e->fdstat = open(fstat_path, O_RDWR)) < 0)
    {
        severe("open failure for %s : %s",fstat_path,strerror(errno));
        return -1;
    }
#if 0    
    if (pread(e->fdstat, &e->fstat, sizeof (export_fstat_t), 0)
            != sizeof (export_fstat_t))
        return -1;
#endif	
    /*
    ** register the export with the periodic quota thread
    */
    if (export_fstat_alloc_context(e->eid,fstat_path,hquota,squota,hquota_fast,0) == NULL)
    {
       severe("cannot allocate context for eid in quota thread");
       return -1;
    }
    // Register the root
    sprintf(const_path, "%s/%s", e->root, CONST_FNAME);
    if ((fd = open(const_path, O_RDWR, S_IRWXU)) < 1) {
        severe("open failure for %s : %s",const_path,strerror(errno));
        return -1;
    }

    if (read(fd, &ect, sizeof (export_const_t)) != sizeof (export_const_t)) {
        close(fd);
        return -1;
    }
    close(fd);
    uuid_copy(e->rfid, ect.rfid);

    if (export_lv2_resolve_path(e, e->rfid, root_path) != 0) {
        severe("open failure for %s : %s",root_path,strerror(errno));
        close(e->fdstat);
        return -1;
    }

    if (!lv2_cache_put(e->trk_tb_p,e->lv2_cache, e->rfid)) {
        severe("open failure for %s : %s",root_path,strerror(errno));
        close(e->fdstat);
        return -1;
    }

    /*
    ** Allocate trash buckets for this export.
    ** One bucket per STORIO slice number
    */
    int sz = sizeof(trash_bucket_t)*common_config.storio_slice_number;
    e->trash_buckets = malloc(sz); 
    export_rm_bins_threshold_high = common_config.trash_high_threshold;
    if (e->trash_buckets == NULL) {
	severe("out of memory export %d %s. %d slices -> sz %d",eid,root_path,common_config.storio_slice_number,sz);
	close(e->fdstat);
	return -1;
    }
    /*
    ** Initialize each trash bucket 
    */
    for (i = 0; i < common_config.storio_slice_number; i++) {
        // Initialize list of files to delete
        list_init(&e->trash_buckets[i].rmfiles);
        // Initialize lock for the list of files to delete
        if ((errno = pthread_rwlock_init(&e->trash_buckets[i].rm_lock, NULL)) != 0) {
            severe("pthread_rwlock_init failed: %s", strerror(errno));
            return -1;
        }
    }
    if (common_config.fid_recycle)
    {
      /*
      ** Allocate recycle buckets for this export.
      ** One bucket per STORIO slice number
      */
      int sz = sizeof(recycle_bucket_t)*MAX_SLICE_NB;
      e->recycle_buckets = malloc(sz); 
      if (e->recycle_buckets == NULL) {
	  severe("out of memory export %d %s. %d slices -> sz %d",eid,root_path,MAX_SLICE_NB,sz);
	  close(e->fdstat);
	  return -1;
      }
      /*
      ** Initialize each recycle bucket 
      */
      for (i = 0; i < MAX_SLICE_NB; i++) {
          // Initialize list of files to delete
          list_init(&e->recycle_buckets[i].rmfiles);

      }
    }
    else
    {
      e->recycle_buckets = NULL;
    }
    export_recycle_pending_count = 0;



    // Initialize pthread for load files to remove

    export_limit_rm_files = common_config.trashed_file_per_run;
    
    if (exportd_is_master()== 0) 
    {   

      if ((errno = pthread_create(&e->load_trash_thread, NULL,
              load_trash_dir_thread, e)) != 0) {
          severe("can't create load trash pthread: %s", strerror(errno));
          return -1;
      }
      else {
        e->load_trash_thread = 0;
      }
    } 
    if (exportd_is_master()== 0) 
    {
      export_start_one_trashd(eid);   
    }    
    return 0;
}
/*
**__________________________________________________________________
*/
void export_release(export_t * e) {
    close(e->fdstat);
    // TODO set members to clean values
}
/*
**__________________________________________________________________
*/
int export_stat(export_t * e, ep_statfs_t * st) {
    int status = -1;
    struct statfs stfs;
    volume_stat_t vstat;
    uint64_t      used;
    uint64_t      free;
    START_PROFILING_EID(export_stat,e->eid);
    
    export_fstat_t * estats = export_fstat_get_stat(e->eid);
    if (estats == NULL) goto out;

    st->bsize = ROZOFS_BSIZE_BYTES(e->bsize);
    if (statfs(e->root, &stfs) != 0)
        goto out;

    // may be ROZOFS_FILENAME_MAX should be stfs.f_namelen
    //st->namemax = stfs.f_namelen;
    st->namemax = ROZOFS_FILENAME_MAX;
    st->ffree = stfs.f_ffree;
    st->files = estats->files;    

    volume_stat(e->volume, &vstat);

    /* 
    ** TNumber of blocks on the volume
    */
    st->blocks = vstat.blocks;
    st->blocks /= (4<<e->bsize);
        
    /*
    ** Number of free blocks on the volume
    */
    free = vstat.bfree;
    free /= (4<<e->bsize);
    
    /* 
    ** Do not show more free blocks than total blocks !
    */
    if (free > st->blocks) {
      free = st->blocks;
    }
    
    /*
    ** No quota. Every single available block of the 
    ** volume can be used by this export
    */
    if (e->hquota == 0) {
      st->bfree = free;
      status = 0;
      goto out;
    }

    /*
    ** When some quota are defined, not all the volume 
    ** can be used by this export.
    */
    if (e->hquota < st->blocks) {
      st->blocks = e->hquota;
    }
    /*
    ** take care of the thin provisioning configuration
    */
    if (e->thin == 0)
    {
      used = estats->blocks; // Written blocks of data
    }
    else
    {
      used = estats->blocks_thin; // Written blocks of data    
    }
    st->bfree = st->blocks - used;
    if (st->bfree > free) {
      st->bfree = free;
    }      
    status = 0;
    
out:
    STOP_PROFILING_EID(export_stat,e->eid);
    return status;
}
/*
**__________________________________________________________________
*/
int export_lookup(export_t *e, fid_t pfid, char *name, struct inode_internal_t *attrs,struct inode_internal_t *pattrs) {
    int status = -1;
    lv2_entry_t *plv2 = 0;
    lv2_entry_t *pplv2 = 0;
    lv2_entry_t *lv2 = 0;
    fid_t child_fid;
    uint32_t child_type;
    int fdp = -1;     /* file descriptor of the parent directory */
    START_PROFILING(export_lookup);
    int root_dirent_mask=0;
    int show_trash_dir = 0;
    /*
    ** Trash case pfid delete bit:
    **  0: the parent is an active directory
    **  1: the parent is implicitely the directory trash designated by @rozofs-trash
    **  
    */ 

    // get the lv2 parent
    if (!(plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, pfid))) {
        goto out;
    }
    /*
    ** load the root_idx bitmap of the old parent
    */
    export_dir_load_root_idx_bitmap(e,pfid,plv2);
    /*
    ** Check the case of the nfs inode revalidate
    */
    if (strcmp(name,"..")==0 || (strcmp(name,".")==0))
    {
       if (strcmp(name,".")==0)
       {       
          export_recopy_extended_attributes(e,plv2, attrs);
	  memset(pattrs->attrs.fid,0, sizeof(fid_t));     
       }
       else
       {
          /*
	  ** the parent attrs
	  */
	  if (!(pplv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, plv2->attributes.s.pfid))) {
              goto out;
	  }
          export_recopy_extended_attributes(e,pplv2, attrs);          
	  memset(pattrs->attrs.fid,0, sizeof(fid_t));     	         
       }
       status = 0;
       goto out;    
    }
    /*
    ** copy the parent attributes
    */
    export_recopy_extended_attributes(e,plv2, pattrs);
    int parent_state = 0;
    /*
    ** The parent fid might not have the delete bit asserted even if it has been deleted
    ** The only case where the parent file has the delete bit pending is when the parent is @rozofs-trash@, otherwise
    ** we have to test the fid from the i-node context
    */
    parent_state = rozofs_export_eval_parent_delete_state(pfid,plv2->attributes.s.attrs.fid);
    switch (parent_state)
    {
       /*
       ** the VFS targets a directory that is active
       */
       case 0:
        break;
	
       /*
       ** the directory has been trashed
       */
       case 1:
	 // pattrs->mode &=~(S_IWUSR|S_IWGRP|S_IWOTH);
         break;    

       /*
       ** the directory is active and the VFS indicates "@rozofs-trash@
       */
       case 2:
         break;    
       /*
       ** try to enter in the trash of a already trashed directory-> not supported
       */
       default:	 
       case 3:
	  errno = ENOENT;
	  goto out;        
    
    }
    /*
    ** check direct access
    */
    if (strncmp(name,"@rozofs_uuid@",13) == 0)
    {
        fid_t fid_direct;
	int ret;
	
	ret = rozofs_uuid_parse(&name[13],fid_direct);
	if (ret < 0)
	{
	  errno = EINVAL;
	  goto out;
	}
	lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid_direct);
	if (lv2 == NULL)
	{
	  /*
	  ** cannot get the attributes: need to log the returned errno
	  */
	  errno = ENOENT;
	  goto out;
	}
         
        export_recopy_extended_attributes(e,lv2, attrs);
	/*
	** copy the fid provided in the input argument
	*/
	memcpy(attrs->attrs.fid,fid_direct,sizeof(fid_t));
        status = 0;  
        goto out;      
    }
    /*
    ** check for root of delete pending object
    */
    if (strncmp(name,ROZOFS_DIR_TRASH,strlen(ROZOFS_DIR_TRASH)) == 0)
    {
       if ((strlen(name) == strlen(ROZOFS_DIR_TRASH)))
       {
	 /*
	 ** check if the parent has a trash for the case of an active directory
	 */
	 switch (parent_state)
	 {
           case 0:
	     if (rozofs_has_trash(&plv2->attributes.s.attrs.sids[0]) == 0)
	     {
  //             errno = ENOENT;
  //	     goto out;
	     }	   
	     break;
	   default:
             /*
	     ** there is no trash for a directory that is in the trash
	     */
	     errno = ENOENT;
	     goto out;       
             break;
	 }
	 /*
	 ** assert the del pending bit in the returned attributes of the parent and return the parent attributes
	 */
         export_recopy_extended_attributes(e,plv2, attrs);
	 exp_metadata_inode_del_assert(attrs->attrs.fid);
	 rozofs_inode_set_trash(attrs->attrs.fid);
	 status = 0;
	 goto out; 
       } 
       else
       {
          /*
	  ** adjust the name
	  */
	  name +=strlen(ROZOFS_DIR_TRASH);
	  show_trash_dir = 1;       
       }  
    }

    fdp = export_open_parent_directory(e,pfid);
    if (get_mdirentry(plv2->dirent_root_idx_p,fdp, pfid, name, child_fid, &child_type,&root_dirent_mask) != 0) {
        goto out;
    }
    // get the lv2
    if (!(lv2 = export_lookup_fid_no_invalidate_mover(e->trk_tb_p,e->lv2_cache, child_fid))) {
        /*
         ** It might be possible that the file is still referenced in the dirent file but 
         ** not present on disk: its FID has been released (and the associated file deleted)
         ** In that case when attempt to read that fid file, we get a ENOENT error.
         ** So for that particular case, we remove the entry from the dirent file
         **
         **  open point : that issue is not related to regular file but also applied to directory
         ** 
         */
        int xerrno;
        uint32_t type;
        fid_t fid;
        if (errno == ENOENT) {
            /*
             ** save the initial errno and remove that entry
             */
            xerrno = errno;
            del_mdirentry(plv2->dirent_root_idx_p,fdp, pfid, name, fid, &type,root_dirent_mask);
            errno = xerrno;
        }
        goto out;
    }
    /*
    ** take care of the case of the mover
    */
    rozofs_mover_check_for_validation(e,lv2,child_fid);
    export_recopy_extended_attributes(e,lv2, attrs);
    /*
    ** check if the file has the delete pending bit asserted: if it is the
    ** case the file MUST be in READ only mode
    */
#if 1
    switch (parent_state)
    {
       /*
       ** the VFS targets a directory that is active
       */
       case 0:
	 if (exp_metadata_inode_is_del_pending(child_fid))
	 {
            /*
	    ** attempt to do a lookup on a deleted file while parent is not the trash
	    */
            errno = ENOENT;
	    goto out;  
	 }     
        break;
	
       /*
       ** the directory has been trashed: if the child is a regular file it must be read-only
       */
       case 1:
         /**
	 * do not clear write mode because of SAMBA
	 */
         // if (!S_ISDIR(lv2->attributes.s.attrs.mode)) attrs->mode &=~(S_IWUSR|S_IWGRP|S_IWOTH);
         break;    

       /*
       ** the directory is active and the VFS indicates "@rozofs-trash@
       */
       case 2:
	 if (exp_metadata_inode_is_del_pending(child_fid)==0)
	 {
	   /*
	   ** Check if the child is a directory for which we need to show its trash: do not assert delete bit
	   */
	   if (show_trash_dir)
	   {
	     if (S_ISDIR(lv2->attributes.s.attrs.mode))
	     {
	         rozofs_inode_set_trash(attrs->attrs.fid);
		 break;	     
	     }
	    }
	    /*
	    ** do not show the active file when the target is the trash
	    */
            errno = ENOENT;
	    goto out;         
	 }
	 /*
	 ** do not clear write mode because of SAMBA
	 */
	 //if (!S_ISDIR(lv2->attributes.s.attrs.mode))attrs->mode &=~(S_IWUSR|S_IWGRP|S_IWOTH);
         break;    
       /*
       ** try to enter in the trash of a already trashed directory-> not supported
       */
       default:	 
       case 3:
	  errno = ENOENT;
	  goto out;        
    }
#endif
    if (test_no_extended_attr(lv2)) rozofs_clear_xattr_flag(&attrs->attrs.mode);
    status = 0;
out:
    /*
    ** check if parent root idx bitmap must be updated
    */
    if (plv2 != NULL)export_dir_flush_root_idx_bitmap(e,pfid,plv2->dirent_root_idx_p);
    /*
    ** check the case of the thin_provisioning
    */
    if ((e->thin != 0) && (lv2 != NULL))
    {
      uint32_t nb_blocks;
      int dir;
      int retcode;
      retcode = expthin_check_entry(e,lv2,0,&nb_blocks,&dir);
      if (retcode == 1)
      {
         expthin_update_blocks(e,nb_blocks,dir);
	 /*
	 ** write inode attributes on disk
	 */
	 export_attr_thread_submit(lv2,e->trk_tb_p, 0);      
      }    
    }    

#ifdef ROZOFS_DIR_STATS
   /*
   ** check if the parent attributes must be written back on disk because of directory statistics
   */
   if ((lv2 != NULL) && S_ISDIR(lv2->attributes.s.attrs.mode) &&(lv2->dirty_bit))
   {
     export_dir_check_async_write(e,lv2);
   }
#endif
    /*
    ** close the parent directory
    */
    if (fdp != -1) close(fdp);
    STOP_PROFILING(export_lookup);
    return status;
}
/*
**__________________________________________________________________
*/
/** get parent attributes of a managed file (internal function)
    In case of failure, the returned attributes are filled with 0
 *
 * @param e: the export managing the file
 * @param pfid: the id of the directory
 * @param pattrs: attributes to fill.
 *
 * @return: none
 */
uint64_t  last_export_getattr_log = 0;
fid_t      rozofs_null_fid = {0};

void export_get_parent_attributes(export_t *e, fid_t pfid, struct inode_internal_t *pattrs)
{
    uint64_t     ts;
    char fidstring[256];
    
   if (memcmp(rozofs_null_fid,pfid,sizeof(fid_t))==0)
   {
     memset(pattrs->attrs.fid, 0, sizeof (fid_t));
     return;
   }
    
   lv2_entry_t *plv2 = 0;
   if (!(plv2 = export_lookup_fid_no_invalidate_mover(e->trk_tb_p,e->lv2_cache,pfid))) 
   {

     ts = rdtsc();
     if (ts>last_export_getattr_log+5000000000UL) {
       fid2string(pfid,fidstring);
       warning("export_getattr_parent failed: %s %s", fidstring, strerror(errno));
       last_export_getattr_log = ts;
     }
     /*
     ** Clear the attributes to avoid a match on get_ientry_by_fid() on rozofsmount
     */
     memset(pattrs->attrs.fid, 0, sizeof (fid_t));
     return;
   }
   export_recopy_extended_attributes(e,plv2, pattrs);

#ifdef ROZOFS_DIR_STATS
   /*
   ** check if the parent attributes must be written back on disk because of directory statistics
   */
   if (plv2->dirty_bit)
   {
     export_dir_check_async_write(e,plv2);
   }
#endif               
}
/*
**__________________________________________________________________
*/
/** get attributes of a managed file
 *
 * @param e: the export managing the file
 * @param fid: the id of the file
 * @param attrs: attributes to fill.
 * @param pattrs: parent attributes to fill.
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_getattr(export_t *e, fid_t fid, struct inode_internal_t *attrs,struct inode_internal_t * pattrs) {
    int status = -1;
    lv2_entry_t *lv2 = 0;
    START_PROFILING(export_getattr);
    uint64_t     ts;
    char fidstring[256];
    
    if (!(lv2 = export_lookup_fid_no_invalidate_mover(e->trk_tb_p,e->lv2_cache, fid))) {
    
      ts = rdtsc();
      if (ts>last_export_getattr_log+5000000000UL) {
	fid2string(fid,fidstring);
        warning("export_getattr failed: %s %s", fidstring, strerror(errno));
	last_export_getattr_log = ts;
      }
      goto out;
    } 
    /*
    ** take care of the case of the mover
    */
    rozofs_mover_check_for_validation(e,lv2,fid);  
    /*
    ** Recopy the nb of blocks in the children field 
    ** which is where the rozofsmount expects it to be in case of thin provisioning
    */
    export_recopy_extended_attributes(e,lv2,attrs);   
    memcpy(attrs->attrs.fid,fid,sizeof(fid_t));
    /*
    ** check if the file has the delete pending bit asserted: if it is the
    ** case the file MUST be in READ only mode
    */
    if (exp_metadata_inode_is_del_pending(lv2->attributes.s.attrs.fid) || exp_metadata_inode_is_del_pending(fid))
    {      
        /*
	** do not  clear write mode because of SAMBA
	*/
       //if (!S_ISDIR(lv2->attributes.s.attrs.mode)) attrs->mode &=~(S_IWUSR|S_IWGRP|S_IWOTH);
    }
    /*
    ** Get the attributes of the parent
    */
    export_get_parent_attributes(e,lv2->attributes.s.pfid,pattrs);

    status = 0;
    if ((e->thin != 0) && (lv2 != NULL))
    {
      uint32_t nb_blocks;
      int dir;
      int retcode;
      retcode = expthin_check_entry(e,lv2,0,&nb_blocks,&dir);
      if (retcode == 1)
      {
         expthin_update_blocks(e,nb_blocks,dir);
	 /*
	 ** write inode attributes on disk
	 */
	 export_attr_thread_submit(lv2,e->trk_tb_p, 0);      
      }    
    }    
out:
    STOP_PROFILING(export_getattr);
    return status;
}
/*
**__________________________________________________________________
*/
/** set attributes of a managed file
 *
 * @param e: the export managing the file
 * @param fid: the id of the file
 * @param attrs: attributes to set.
 * @param to_set: fields to set in attributes
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_setattr(export_t *e, fid_t fid, mattr_t *attrs, int to_set) {
    int status = -1;
    lv2_entry_t *lv2 = 0;
    int bbytes = ROZOFS_BSIZE_BYTES(e->bsize);
    lv2_entry_t *plv2 = NULL;    
    int quota_uid=-1;
    int quota_gid=-1;
    uint64_t nrb_new = 0;
    uint64_t nrb_old = 0;
    int      sync = 0;
    uint16_t share = 0;
       
    START_PROFILING(export_setattr);

    if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid))) {
        goto out;
    }
    /*
    ** reject the service if the directory has the delete pending bit asserted
    */
    if (exp_metadata_inode_is_del_pending(lv2->attributes.s.attrs.fid))
    {
       errno = EROFS;
       goto out;
    }
    /*
    ** check if the inode reference a Mover
    */
    {
      rozofs_inode_t *inode_p = (rozofs_inode_t*)fid;
      if ((inode_p->s.key == ROZOFS_REG_S_MOVER) || (inode_p->s.key == ROZOFS_REG_D_MOVER))
      {
	 /*
	 ** Mover cannot  change the i-node
	 */
	 status = 0;
	 goto out;
      }
    }
#ifdef ROZOFS_DIR_STATS
    /*
    ** attempt to get the parent attribute to address the case of the asynchronous fast replication
    */
    plv2 = export_dir_get_parent(e,lv2);
#endif
    if ((to_set & EXPORT_SET_ATTR_SIZE) && S_ISREG(lv2->attributes.s.attrs.mode)) {
        
        // Check new file size
        if (attrs->size >= ROZOFS_FILESIZE_MAX) {
            errno = EFBIG;
            goto out;
        }
	/*
	** Get the parent i-node
	*/
	if (plv2 == NULL)
	{
	   plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, lv2->attributes.s.pfid); 
	}       
	if (plv2!=NULL) share= plv2->attributes.s.attrs.cid;
	
        nrb_new = ((attrs->size + bbytes - 1) / bbytes);
        nrb_old = ((lv2->attributes.s.attrs.size + bbytes - 1) / bbytes);
	if (nrb_new > nrb_old)
	{
          rozofs_qt_block_update(e->eid,lv2->attributes.s.attrs.uid,lv2->attributes.s.attrs.gid,(nrb_new-nrb_old)*bbytes,ROZOFS_QT_INC,share); 
	  /*
	  ** adjust the directory statistics
	  */
	  if (plv2) export_dir_adjust_child_size(plv2,(nrb_new-nrb_old)*bbytes,1,bbytes);
	}
	else
	{
          rozofs_qt_block_update(e->eid,lv2->attributes.s.attrs.uid,lv2->attributes.s.attrs.gid,(nrb_old-nrb_new)*bbytes,ROZOFS_QT_DEC,share); 	
#ifdef ROZOFS_DIR_STATS
	  /*
	  ** adjust the directory statistics
	  */	  
	  if (plv2) export_dir_adjust_child_size(plv2,(nrb_old-nrb_new)*bbytes,0,bbytes);
#endif
	}  
	/*
	** provides the children field since it contains the indication on which volume the file has been allocated
	*/    		
        if (export_update_blocks(e, nrb_new, nrb_old,lv2->attributes.s.attrs.children)!= 0)
            goto out;

        lv2->attributes.s.attrs.size = attrs->size;
	sync = 1; /* Need to sync on disk now */
    }

    if (to_set & EXPORT_SET_ATTR_MODE)
    {
        if (rozofs_has_xattr(lv2->attributes.s.attrs.mode)) rozofs_set_xattr_flag(&attrs->mode);
	else rozofs_clear_xattr_flag(&attrs->mode);
        lv2->attributes.s.attrs.mode = attrs->mode;
    }
    if (to_set & EXPORT_SET_ATTR_UID)
    {
        quota_uid = lv2->attributes.s.attrs.uid;
        lv2->attributes.s.attrs.uid = attrs->uid;
    }
    if (to_set & EXPORT_SET_ATTR_GID){
        quota_gid = lv2->attributes.s.attrs.gid;
        lv2->attributes.s.attrs.gid = attrs->gid; 
    }   
    if (to_set & EXPORT_SET_ATTR_ATIME)
        lv2->attributes.s.attrs.atime = attrs->atime;
    if (to_set & EXPORT_SET_ATTR_MTIME)
        lv2->attributes.s.attrs.mtime = attrs->mtime;
    
    lv2->attributes.s.attrs.ctime = time(NULL);
    /*
    ** check the case of the quota: accounting only
    */
    if ((quota_gid !=-1) || (quota_uid!=-1))
    {
       rozofs_qt_inode_update(e->eid,quota_uid,quota_gid,1,ROZOFS_QT_DEC,0);
       rozofs_qt_inode_update(e->eid,lv2->attributes.s.attrs.uid,lv2->attributes.s.attrs.gid,1,ROZOFS_QT_INC,0);
       if (S_ISREG(lv2->attributes.s.attrs.mode))
       {
	 rozofs_qt_block_update(e->eid,quota_uid,quota_gid,lv2->attributes.s.attrs.size,ROZOFS_QT_DEC,0);
	 rozofs_qt_block_update(e->eid,lv2->attributes.s.attrs.uid,lv2->attributes.s.attrs.gid,lv2->attributes.s.attrs.size,ROZOFS_QT_INC,0);       
       }       
    }
    /*
    ** check the case of the truncate with thin provisioning
    */
    if (e->thin)
    {
      uint32_t nb_blocks;
      int dir;
      int retcode;
      if (to_set & EXPORT_SET_ATTR_SIZE)
      {
	retcode = expthin_check_entry(e,lv2,1,&nb_blocks,&dir);
	if (retcode == 1)
	{
	   expthin_update_blocks(e,nb_blocks,dir);
	}
      }
    }    
#ifdef ROZOFS_DIR_STATS
    /*
    ** adjust the directory statistics
    */
    if(plv2) 
    {
      export_dir_async_write(e,plv2);
    }
#endif
    status = export_lv2_write_attributes(e->trk_tb_p,lv2,sync);
out:
    STOP_PROFILING(export_setattr);
    return status;
}
/*
**__________________________________________________________________
*/
/** create a hard link
 *
 * @param e: the export managing the file
 * @param inode: the id of the file we want to be link on
 * @param newparent: parent od the new file (the link)
 * @param newname: the name of the new file
 * @param[out] attrs:  to fill (child attributes used by upper level functions)
 * @param[out] pattrs:  to fill (parent attributes)
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_link(export_t *e, fid_t inode, fid_t newparent, char *newname, struct inode_internal_t *attrs,struct inode_internal_t *pattrs) {
    int status = -1;
    lv2_entry_t *target = NULL;
    lv2_entry_t *plv2 = NULL;
    fid_t child_fid;
    uint32_t child_type;
    int fdp= -1;
    mdirent_fid_name_info_t fid_name_info;
    int root_dirent_mask = 0;
    
    START_PROFILING(export_link);

    // Get the lv2 inode
    if (!(target = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, inode)))
        goto out;

    // Verify that the target is not a directory
    if (S_ISDIR(target->attributes.s.attrs.mode)) {
        errno = EPERM;
        goto out;
    }

    // Get the lv2 parent
    if (!(plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, newparent)))
        goto out;
    /*
    ** load the root_idx bitmap of the old parent
    */
    export_dir_load_root_idx_bitmap(e,newparent,plv2);

    // Verify that the mdirentry does not already exist
    fdp = export_open_parent_directory(e,newparent);
    if (get_mdirentry(plv2->dirent_root_idx_p,fdp, newparent, newname, child_fid, &child_type,&root_dirent_mask) != -1) {
        errno = EEXIST;
        goto out;
    }
    /*
     ** nothing has been found, need to check the read only flag:
     ** that flag is asserted if some parts of dirent files are unreadable 
     */
    if (DIRENT_ROOT_IS_READ_ONLY()) {
        errno = EIO;
        goto out;
    }
    /*
    ** update the bit in the root_idx bitmap of the parent directory
    */
    uint32_t hash1,hash2;
    int root_idx;
    int len;
    
    hash1 = filename_uuid_hash_fnv(0, newname,newparent, &hash2, &len);
    root_idx = dirent_get_root_idx(plv2->attributes.s.attrs.children,hash1);
    export_dir_update_root_idx_bitmap(plv2->dirent_root_idx_p,root_idx,1);
    if (export_dir_flush_root_idx_bitmap(e,newparent,plv2->dirent_root_idx_p) < 0)
    {
       errno = EPROTO; 
       goto out;
    }
    // Put the new mdirentry
    if (put_mdirentry(plv2->dirent_root_idx_p,fdp, newparent, newname, 
                      target->attributes.s.attrs.fid, target->attributes.s.attrs.mode,&fid_name_info,
		      plv2->attributes.s.attrs.children,&root_dirent_mask) != 0)
        goto out;

    // Update nlink and ctime for inode
    target->attributes.s.attrs.nlink++;
    target->attributes.s.attrs.ctime = time(NULL);

    // Write attributes of target
    if (export_lv2_write_attributes(e->trk_tb_p,target,0/* No sync */) != 0)
        goto out;

    // Update parent
    plv2->attributes.s.attrs.children++;
    plv2->attributes.s.attrs.mtime = plv2->attributes.s.attrs.ctime = time(NULL);

#ifdef ROZOFS_DIR_STATS
    /*
    ** Update the directory statistics
    */
    export_dir_adjust_child_size(plv2,target->attributes.s.attrs.size,1,ROZOFS_BSIZE_BYTES(e->bsize));
    export_dir_update_time(plv2);
#endif
    // Write attributes of parents
    if (export_lv2_write_attributes(e->trk_tb_p,plv2,0/* No sync */) != 0)
        goto out;

    // Return attributes
    export_recopy_extended_attributes(e,target,attrs);
    
    /*
    ** return the parent attributes
    */
    export_recopy_extended_attributes(e,plv2,pattrs);
    status = 0;

out:
    /*
    ** check if parent root idx bitmap must be updated
    */
    if (plv2 != NULL) export_dir_flush_root_idx_bitmap(e,newparent,plv2->dirent_root_idx_p);
    
    if (fdp != -1) close(fdp);
    STOP_PROFILING(export_link);
    return status;
}



int rozofs_parse_object_name(char *name,char **basename,int *file_count)
{
  int count;
  char *pnext;
  char *cur = name;


  while(1)
  {
    /*
    ** get the file count
    */
    errno = 0;
    count = strtoul(cur,&pnext,10);
    if (errno != 0) break;
    if (*pnext !='-') 
    {
      errno = EINVAL;
      break;
    }
    break;
  }
  if (errno!=0) 
  {
    return -1;
  }
  *file_count = count;
  pnext++;
  *basename = pnext;
  return 0;

}

int export_mknod_multiple(export_t *e,uint32_t site_number,fid_t pfid, char *name, uint32_t uid,
        uint32_t gid, mode_t mode, struct inode_internal_t *attrs,struct inode_internal_t *pattrs,lv2_entry_t *plv2) {
    int status = -1;
    fid_t node_fid;
    int xerrno = errno;
    uint32_t type;
    int fdp= -1;
    uint32_t pslice;
    int inode_allocated = 0;
    mdirent_fid_name_info_t fid_name_info;
    rozofs_inode_t *fake_inode;
    exp_trck_top_header_t *p = NULL;
    int ret;
    int root_dirent_mask = 0;
    char filename[1024];
    char *basename;
    int filecount;
    char *filename_p;
    int k;
    ext_mattr_t *buf_attr_p = NULL;
    ext_mattr_t *buf_attr_work_p = NULL;
    int64_t file_id = 0;
    
    file_id--;
    filename_p = name;
    
    // get the lv2 parent
    if (!(plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, pfid)))
        goto error;

    /*
    ** Check parent GID bit
    */
    if (plv2->attributes.s.attrs.mode & S_ISGID) {
      gid   = plv2->attributes.s.attrs.gid;
    }  
    
    /*
    ** search for rozofs key in the filename
    */
    if (strncmp(name,".@rozofs-mf@",12) == 0)
    {
      ret = rozofs_parse_object_name((char*)(name+12),&basename,&filecount);
      if (ret < 0)
      {
	xerrno = ENOTSUP;
	goto error;
      }
    }
    /*
    ** allocate the buffer
    */
    buf_attr_p = memalign(32,sizeof(ext_mattr_t)*(filecount+1));
    if (buf_attr_p == NULL)
    {
      xerrno=ENOMEM;
      goto error;
    }
    memset(buf_attr_p,0,sizeof(ext_mattr_t)*(filecount+1));
    buf_attr_work_p = buf_attr_p;
         
    /*
    ** load the root_idx bitmap of the parent
    */
    export_dir_load_root_idx_bitmap(e,pfid,plv2);
 
    /*
    ** set global variables associated with the export
    */
    fdp = export_open_parent_directory(e,pfid);

    if (get_mdirentry(plv2->dirent_root_idx_p,fdp, pfid, name, node_fid, &type,&root_dirent_mask) == 0) {
        errno = EEXIST;
        goto error;
    }
    for (k = 0; k < filecount ; k++)
    {
      sprintf(filename,"%s.%d",basename,k);
      if (get_mdirentry(plv2->dirent_root_idx_p,fdp, pfid, filename, node_fid, &type,&root_dirent_mask) == 0) {
          errno = EEXIST;
          goto error;
      }
    }
    /*
     ** nothing has been found, need to check the read only flag:
     ** that flag is asserted if some parts of dirent files are unreadable 
     */
    if (DIRENT_ROOT_IS_READ_ONLY()) {
        xerrno = EIO;
        goto error_read_only;
    }
    if (!S_ISREG(mode)) {
        xerrno = ENOTSUP;
        goto error;
    }    
    /*
    ** Check that some space os left for the new file in case a hard quota is set
    ** that care of the thin-provisioning option for the exportd
    */
    if (e->hquota) {
      if (e->thin == 0)
      {
	export_fstat_t * estats = export_fstat_get_stat(e->eid);
	if ((estats != NULL)&&(estats->blocks >= e->hquota)) {
          errno = ENOSPC;
          goto error;
	}
      }
      else
      {
	export_fstat_t * estats = export_fstat_get_stat(e->eid);
	if ((estats != NULL)&&(estats->blocks_thin >= e->hquota)) {
          errno = ENOSPC;
          goto error;
	}
      }      
    }
    /*
    **  check user and group quota
    */
    {
       int ret;
       
       ret = rozofs_qt_check_quota(e->eid,uid,gid,plv2->attributes.s.attrs.cid);
       if (ret < 0)
       {
         errno = ENOSPC;
         goto error;
       }         
    }
    /*
    ** get the slice of the parent
    */
    exp_trck_get_slice(pfid,&pslice);


    int offset_in_buffer = 0;
    int attr_count = 0; 

    for (k = 0; k < filecount+1 ; k++,buf_attr_work_p++)
    {
      if (volume_distribute(e->layout,e->volume,site_number, &buf_attr_work_p->s.attrs.cid, buf_attr_work_p->s.attrs.sids) != 0)
          goto error;
      if (k !=0) 
      {
         sprintf(filename,"%s.%d",basename,k);
	 filename_p = filename;
      }
      /*
      ** copy the parent fid of the regular file
      */
      memcpy(&buf_attr_work_p->s.pfid,pfid,sizeof(fid_t));
      /*
      ** Put the reference of the share (or project) in the i-node
      */
      buf_attr_work_p->s.hpc_reserved.reg.share_id = plv2->attributes.s.attrs.cid;    
      /*
      ** get the distribution for the file
      */
      buf_attr_work_p->s.attrs.mode = mode;
      rozofs_clear_xattr_flag(&buf_attr_work_p->s.attrs.mode);
      buf_attr_work_p->s.attrs.uid = uid;
      buf_attr_work_p->s.attrs.gid = gid;
      buf_attr_work_p->s.attrs.nlink = 1;
      buf_attr_work_p->s.i_extra_isize = ROZOFS_I_EXTRA_ISIZE;
      buf_attr_work_p->s.i_state = 0;
      buf_attr_work_p->s.i_file_acl = 0;
      buf_attr_work_p->s.i_link_name = 0;
     /*
     ** set atime,ctime and mtime
     */
      if ((buf_attr_work_p->s.attrs.ctime = buf_attr_work_p->s.attrs.atime = buf_attr_work_p->s.attrs.mtime = buf_attr_work_p->s.cr8time = time(NULL)) == -1)
          goto error;
      buf_attr_work_p->s.attrs.size = 0;
      /*
      ** create the inode and write the attributes on disk
      */

      if(exp_attr_create_write_cond(e->trk_tb_p,pslice,buf_attr_work_p,ROZOFS_REG,NULL,0) < 0)
          goto error;
      /*
      ** increment the number of inode allocated. Need in case of error to release the allocated inodes
      */
      inode_allocated++;
      /*
      ** update the bit in the root_idx bitmap of the parent directory
      */
      uint32_t hash1,hash2;
      int root_idx;
      int len;
      
      hash1 = filename_uuid_hash_fnv(0, filename_p,pfid, &hash2, &len);
      root_idx = dirent_get_root_idx(plv2->attributes.s.attrs.children,hash1);
      export_dir_update_root_idx_bitmap(plv2->dirent_root_idx_p,root_idx,1);
      if (export_dir_flush_root_idx_bitmap(e,pfid,plv2->dirent_root_idx_p) < 0)
      {
	 errno = EPROTO; 
	 goto error;
      }
      // update the parent
      // add the new child to the parent
      if (put_mdirentry(plv2->dirent_root_idx_p,fdp, pfid, filename_p, 
                	buf_attr_work_p->s.attrs.fid, attrs->attrs.mode,&fid_name_info,
			plv2->attributes.s.attrs.children,
			&root_dirent_mask) != 0) {
          goto error;
      }
      /*
      ** store the name of the file in the inode: the format depends on the size of the filename
      **  If the size of the file is less than ROZOFS_OBJ_NAME_MAX (60 bytes) then rozofs stores the 
      **  filename in the inode otherwise it stores all the information that permits to retrieve the
      **  filename from the dirent file associated with the created file
      */
      exp_store_fname_in_inode(&buf_attr_work_p->s.fname,filename_p,&fid_name_info);

  #if 0
  #warning push the mknod attribute in cache
      lv2_cache_put_forced(e->lv2_cache,buf_attr_work_p->s.attrs.fid,buf_attr_work_p);
  #endif
      /*
      ** flush the inode on disk
      */
      fake_inode = (rozofs_inode_t*)buf_attr_work_p->s.attrs.fid;
      p = e->trk_tb_p->tracking_table[fake_inode->s.key];
      if (file_id < 0)
      {
          file_id = fake_inode->s.file_id;
	  offset_in_buffer = k;
	  attr_count = 1;      
      }
      else
      {
         if (file_id != fake_inode->s.file_id)
	 {
	    /*
	    ** push on disk
	    */
	    ret = exp_metadata_create_attributes_burst(p,fake_inode,&buf_attr_p[offset_in_buffer],sizeof(ext_mattr_t)*attr_count, 1 /* sync */);
	    if (ret < 0)
	    { 
	      goto error;
	      offset_in_buffer = k;
	      attr_count = 1; 
	      file_id = fake_inode->s.file_id;
	    }  	 	 
	 } 
	 else
	 {
	   attr_count++;
	 }     
      }
    }
    /*
    ** check if there is some attributes to write on disk
    */
    if (attr_count != 0)
    {
       fake_inode = (rozofs_inode_t*)buf_attr_p[offset_in_buffer].s.attrs.fid;
       ret = exp_metadata_create_attributes_burst(p,fake_inode,&buf_attr_p[offset_in_buffer],sizeof(ext_mattr_t)*attr_count, 1 /* sync */);
       if (ret < 0)
       { 
	 goto error;    
       }    
    }
    // Update children nb. and times of parent
    plv2->attributes.s.attrs.children += filecount+1;    
    plv2->attributes.s.attrs.mtime = plv2->attributes.s.attrs.ctime = time(NULL);
    /*
    ** write the attributes on disk
    */
    export_attr_thread_submit(plv2,e->trk_tb_p,0 /* No sync */);

    // update export files
    export_update_files(e, filecount+1,0);
    
    rozofs_qt_inode_update(e->eid,uid,gid,filecount+1,ROZOFS_QT_INC,plv2->attributes.s.attrs.cid);
    status = 0;
    /*
    ** return the parent attributes and the child attributes
    */
    export_recopy_extended_attributes(e,plv2,pattrs);
    export_recopy_extended_attributes(e,(lv2_entry_t*)&buf_attr_p,attrs);
    goto out;

error:
    xerrno = errno;
    if (inode_allocated)
    {
       fid_t child_fid;
       uint32_t child_type;
       /*
       ** release the inodes that has been allocated
       */
       root_dirent_mask = 0;
       export_tracking_table_t *trk_tb_p;   
       trk_tb_p = e->trk_tb_p;
       filename_p = name;
       for (k = 0; k < inode_allocated; k++)
       {
         exp_attr_delete(trk_tb_p,buf_attr_p[k].s.attrs.fid);     
	 if (k !=0) 
	 {
	    sprintf(filename,"%s.%d",basename,k);
	    filename_p = filename;
	 }
	 del_mdirentry(plv2->dirent_root_idx_p,fdp, pfid, filename_p, child_fid, &child_type,root_dirent_mask);
       }
   
    }

error_read_only:
    errno = xerrno;

out:
    /*
    ** check if parent root idx bitmap must be updated
    */
    if (plv2 != NULL) export_dir_flush_root_idx_bitmap(e,pfid,plv2->dirent_root_idx_p);    
    if(fdp != -1) close(fdp);
    if (buf_attr_p != NULL) free(buf_attr_p);
    return status;
}

/*
**__________________________________________________________________
*/
/** create a new file by using a recycled fid for a better world
 *
 * @param e: the export managing the file
 * @param pfid: the id of the parent
 * @param ext_attrs: pointer to an array where current attributes are returned
  
 * @return: NULL or level2 entry pointer associated with the recycle fid
 */
lv2_entry_t *  export_get_recycled_inode(export_t *e,fid_t pfid,ext_mattr_t *ext_attrs)
{   
   recycle_mem_t *rmfe; 
   lv2_entry_t *lv2;
   uint32_t pslice;

   /*
   ** check if fid recycle is active
   */
   if (common_config.fid_recycle == 0) return NULL;
   /*
   ** check if there is reload in progress
   */
   if (export_fid_recycle_ready == 0) return NULL;
   /*
   ** get the slice of the parent
   */
   exp_trck_get_slice(pfid,&pslice);
   rmfe = list_1rst_entry(&e->recycle_buckets[pslice].rmfiles,recycle_mem_t, list);
   if (rmfe == NULL) return NULL;
   /*
   ** remove the entry from the fid recyle list and update the entry on disk
   */
   export_recycle_done_count++;
   list_remove(&rmfe->list);
   export_recycle_remove_from_tracking_file(e,rmfe);
   /*
   ** load up the entry in cache
   */
   if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, rmfe->fid)))
   {
      severe("cannot find fid on disk");
      free(rmfe);
      return NULL;
   }
   memcpy(ext_attrs,&lv2->attributes,sizeof(ext_mattr_t));
   free(rmfe);
   return lv2; 
}


/*
**__________________________________________________________________
*/
/** create a new file
 *
 * @param e: the export managing the file
 * @param site_number: site number for geo-replication
 * @param pfid: the id of the parent
 * @param name: the name of this file.
 * @param uid: the user id
 * @param gid: the group id
 * @param mode: mode of this file
 * @param[out] attrs:  to fill (child attributes used by upper level functions)
 * @param[out] pattrs:  to fill (parent attributes)
  
 * @return: 0 on success -1 otherwise (errno is set)
 */
#define ROZOFS_SLICE "@rozofs_slice@"

int export_mknod(export_t *e,uint32_t site_number,fid_t pfid, char *name, uint32_t uid,
        uint32_t gid, mode_t mode, struct inode_internal_t *attrs,struct inode_internal_t *pattrs) {
    int status = -1;
    lv2_entry_t *plv2=NULL;
    lv2_entry_t *lv2_child = NULL;
    fid_t node_fid;
    int xerrno = errno;
    uint32_t type;
    int fdp= -1;
    ext_mattr_t ext_attrs;
    uint32_t pslice;
    int inode_allocated = 0;
    mdirent_fid_name_info_t fid_name_info;
    int root_dirent_mask = 0;
    lv2_entry_t *lv2_recycle=NULL;

   
    START_PROFILING(export_mknod);
    
    /*
    ** reject the service if the directory has the delete pending bit asserted
    */
    if (exp_metadata_inode_is_del_pending(pfid))
    {
       errno = EROFS;
       goto error;
    }

    // get the lv2 parent
    if (!(plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, pfid)))
        goto error;

    /*
    ** Check parent GID bit
    */
    if (plv2->attributes.s.attrs.mode & S_ISGID) {
      gid   = plv2->attributes.s.attrs.gid;
    }  
    if (strncmp(name,".@rozofs-mf@",12) == 0)
    {
      int ret;
      ret = export_mknod_multiple(e,site_number,pfid,name,uid,gid,mode,attrs,pattrs,plv2);
      STOP_PROFILING(export_mknod);
      return ret;
    }

    /*
    ** Check meta data device left size
    */
    if (export_metadata_device_full(e,tic)) {
      errno = ENOSPC;
      goto error;      
    }

    /*
    ** load the root_idx bitmap of the parent
    */
    export_dir_load_root_idx_bitmap(e,pfid,plv2);
 
    /*
    ** set global variables associated with the export
    */
    fdp = export_open_parent_directory(e,pfid);

    if (get_mdirentry(plv2->dirent_root_idx_p,fdp, pfid, name, node_fid, &type,&root_dirent_mask) == 0) {
        /*
	** Creation may have been asked from several mount points in //
	** This is typical in HPC case where the same program is executed 
	** on several compute nodes at the same time.
	*/
	lv2_child = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, node_fid);
	if(lv2_child !=NULL) {
	    while (common_config.mknod_ok_instead_of_eexist) {
		/*
		** Check creation date, uid, gid
		*/
		if ((time(NULL) - lv2_child->attributes.s.cr8time) > 15) break;
		if (lv2_child->attributes.s.attrs.uid != uid)            break;
		if (lv2_child->attributes.s.attrs.gid != gid)            break;

		/*
		** Let's respond the file has been created by this request
		*/
                export_recopy_extended_attributes(e,lv2_child,attrs);
                export_recopy_extended_attributes(e,plv2,pattrs);
		status = 0;
		goto out;	   
	    }

	    /*
	    ** Let's respond that the file is already created
	    */
            errno = EEXIST;
            goto error;
	}
    }
    /*
     ** nothing has been found, need to check the read only flag:
     ** that flag is asserted if some parts of dirent files are unreadable 
     */
    if (DIRENT_ROOT_IS_READ_ONLY()) {
        xerrno = EIO;
        goto error_read_only;
    }
    /*
    ** Check that some space os left for the new file in case a hard quota is set
    */
    if (e->hquota) {
      export_fstat_t * estats = export_fstat_get_stat(e->eid); 
      /*
      ** check the case of the thin provisioning versus not thin provisioning..
      ** When thin-provisioning is enabled for the exportd, the number of blocks
      ** that we should compare is in the blocks_thin field versus blocks field
      ** for the default mode
      */
      if (e->thin == 0)
      {   
	if ((estats!=NULL) && (estats->blocks >= e->hquota)) {
          errno = ENOSPC;
          goto error;
	}
      }
      else
      {
	if ((estats!=NULL) && (estats->blocks_thin >= e->hquota)) {
          errno = ENOSPC;
          goto error;
	}
      }
    }
    /*
    **  check user and group quota
    */
    {
       int ret;
       
       ret = rozofs_qt_check_quota(e->eid,uid,gid,plv2->attributes.s.attrs.cid);
       if (ret < 0)
       {
         errno = ENOSPC;
         goto error;
       }         
    }
    /*
    ** get the slice in the name
    */
    if (strncmp(name,ROZOFS_SLICE,strlen(ROZOFS_SLICE)) == 0) {
      /*
      ** Slice is provided
      */
      char * pt = name + strlen(ROZOFS_SLICE);
      sscanf(pt,"%d", &pslice);  
    }
     /*
     ** get the slice of the parent
     */
    else {
      exp_trck_get_slice(pfid,&pslice);
    }
    /*
    ** copy the parent fid of the regular file
    */
    memset(&ext_attrs,0x00,sizeof(ext_attrs));
    memcpy(&ext_attrs.s.pfid,pfid,sizeof(fid_t));
    /*
    ** Put the reference of the share (or project) in the i-node
    */
    ext_attrs.s.hpc_reserved.reg.share_id = plv2->attributes.s.attrs.cid;    
    /*
    ** check if there some fid to recycle
    */
    lv2_recycle = export_get_recycled_inode(e,pfid,&ext_attrs);
    if (lv2_recycle == NULL)
    {
      /*
      ** get the distribution for the file:
      **  When the export has a fast volume, we check the suffix of the file to figure out if the file can be allocated
      **  with the fast volume. When the fast volume is full or because the fast quota is reached, we allocate the file
      **  within the default volume associated with the exportd
      */
      while (1)
      {
	if ((e->volume_fast != NULL) && (rozofs_htable_tab_p[e->suffix_file_idx] !=NULL))
	{
           /*
	   ** we check if the suffix of the file can be candidate for fast volume
	   */
	   if (export_file_create_check_fast_htable(name,e->suffix_file_idx) >= 0)
	   {
	     /*
	     ** Check that some space os left for the new file in case a hard quota is set for the fast volume
	     */
	     if (e->hquota_fast) {
	       export_fstat_t * estats = export_fstat_get_stat(e->eid); 
	       /*
	       ** the thin provisioning does not apply to fast volume
	       */         
	       if ((estats!=NULL) && (estats->blocks_fast >= e->hquota_fast)) {
        	 /*
		 ** no space left: use the regular procedure
		 */
		 if (volume_distribute(e->layout,e->volume,site_number, &ext_attrs.s.attrs.cid, ext_attrs.s.attrs.sids) != 0)
        	     goto error;
		 break;
	       }
	     }	     
	     /*
	     ** attempt to allocate on fast volume
	     */
	     if (volume_distribute(e->layout,e->volume_fast,site_number, &ext_attrs.s.attrs.cid, ext_attrs.s.attrs.sids) == 0)
	     {
	       rozofs_mover_children_t *children_p;
	       /*
	       ** OK we got a distribution: so store the reference of the volume fast in the i-node: needed to deal with quota
	       */
	       children_p = (rozofs_mover_children_t*) &ext_attrs.s.attrs.children;
	       children_p->fid_st_idx.vid_fast = e->volume_fast->vid;
	       break;
	     }
	   }
	}	
	/*
	** default case
	*/ 
	if (volume_distribute(e->layout,e->volume,site_number, &ext_attrs.s.attrs.cid, ext_attrs.s.attrs.sids) != 0)
            goto error;
	break;
      }
    }
    ext_attrs.s.attrs.mode = mode;
    rozofs_clear_xattr_flag(&ext_attrs.s.attrs.mode);
    ext_attrs.s.attrs.uid = uid;
    ext_attrs.s.attrs.gid = gid;
    ext_attrs.s.attrs.nlink = 1;
    ext_attrs.s.i_extra_isize = ROZOFS_I_EXTRA_ISIZE;
    ext_attrs.s.i_state = 0;
    ext_attrs.s.i_file_acl = 0;
    ext_attrs.s.i_link_name = 0;
   /*
   ** set atime,ctime and mtime
   */
    if ((ext_attrs.s.attrs.ctime = ext_attrs.s.attrs.atime = ext_attrs.s.attrs.mtime = ext_attrs.s.cr8time= time(NULL)) == -1)
        goto error;
    ext_attrs.s.attrs.size = 0;
    if (lv2_recycle == NULL)
    {
      /*
      ** create the inode and write the attributes on disk
      */
      if(exp_attr_create_write_cond(e->trk_tb_p,pslice,&ext_attrs,ROZOFS_REG,NULL,0) < 0)
          goto error;
      inode_allocated = 1;
    }
    /*
    ** update the bit in the root_idx bitmap of the parent directory
    */
    uint32_t hash1,hash2;
    int root_idx;
    int len;
    
    hash1 = filename_uuid_hash_fnv(0, name,pfid, &hash2, &len);
    /*
    ** store the hash values in the i-node
    */
    ext_attrs.s.hash1 = hash1;
    ext_attrs.s.hash2 = hash2;
    root_idx = dirent_get_root_idx(plv2->attributes.s.attrs.children,hash1);
    export_dir_update_root_idx_bitmap(plv2->dirent_root_idx_p,root_idx,1);
    if (export_dir_flush_root_idx_bitmap(e,pfid,plv2->dirent_root_idx_p) < 0)
    {
       errno = EPROTO; 
       goto error;
    }
    // update the parent
    // add the new child to the parent
    if (put_mdirentry(plv2->dirent_root_idx_p,fdp, pfid, name, 
                      ext_attrs.s.attrs.fid, attrs->attrs.mode,&fid_name_info,
		      plv2->attributes.s.attrs.children,
		      &root_dirent_mask) != 0) {
        goto error;
    }
    /*
    ** store the name of the file in the inode: the format depends on the size of the filename
    **  If the size of the file is less than ROZOFS_OBJ_NAME_MAX (60 bytes) then rozofs stores the 
    **  filename in the inode otherwise it stores all the information that permits to retrieve the
    **  filename from the dirent file associated with the created file
    */
    exp_store_fname_in_inode(&ext_attrs.s.fname,name,&fid_name_info);
    /*
    ** flush the inode on disk
    */
    if (lv2_recycle == NULL)
    {  
      lv2_child = lv2_cache_put_forced(e->lv2_cache,ext_attrs.s.attrs.fid,&ext_attrs);
    }
    else
    {
      memcpy(&lv2_recycle->attributes,&ext_attrs,sizeof(ext_mattr_t));
      lv2_child = lv2_recycle;    
    }
    /*
    ** write child attributes on disk
    */
    export_attr_thread_submit(lv2_child,e->trk_tb_p,1 /* sync */);

    // Update children nb. and times of parent
    plv2->attributes.s.attrs.children++;
    plv2->attributes.s.attrs.mtime = plv2->attributes.s.attrs.ctime = time(NULL);

    /*
    ** Update the directory statistics
    */
#ifdef ROZOFS_DIR_STATS
    export_dir_update_time(plv2);
#endif
    /*
    ** write parent attributes on disk
    */
    export_attr_thread_submit(plv2,e->trk_tb_p,0 /* No sync */);

    // update export files
    export_update_files(e, 1,ext_attrs.s.attrs.children);

    rozofs_qt_inode_update(e->eid,uid,gid,1,ROZOFS_QT_INC,plv2->attributes.s.attrs.cid);
    
    status = 0;
    /*
    ** return the parent attributes and the child attributes
    */
    export_recopy_extended_attributes(e,plv2,pattrs);
    export_recopy_extended_attributes(e,(lv2_entry_t*)&ext_attrs,attrs);
    goto out;

error:
    xerrno = errno;
    if (inode_allocated)
    {
       export_tracking_table_t *trk_tb_p;   
       trk_tb_p = e->trk_tb_p;
       exp_attr_delete(trk_tb_p,ext_attrs.s.attrs.fid);        
    }
error_read_only:
    errno = xerrno;

out:
    /*
    ** check if parent root idx bitmap must be updated
    */
    if (plv2 != NULL) export_dir_flush_root_idx_bitmap(e,pfid,plv2->dirent_root_idx_p);    
    if(fdp != -1) close(fdp);
    STOP_PROFILING(export_mknod);
    return status;
}
/*
**__________________________________________________________________
*/
/** create a new directory
 *
 * @param e: the export managing the file
 * @param pfid: the id of the parent
 * @param name: the name of this file.
 * @param uid: the user id
 * @param gid: the group id
 * @param mode: mode of this file
 * @param[out] attrs:  to fill (child attributes used by upper level functions)
 * @param[out] pattrs:  to fill (parent attributes)
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_mkdir(export_t *e, fid_t pfid, char *name, uint32_t uid,
        uint32_t gid, mode_t mode, struct inode_internal_t * attrs,struct inode_internal_t * pattrs) {
    int status = -1;
    lv2_entry_t *plv2= NULL;
    lv2_entry_t *lv2= NULL;
    char node_path[PATH_MAX];
    mdir_t node_mdir;
    int xerrno = errno;
    int fdp = -1;
    ext_mattr_t ext_attrs;
    uint32_t pslice;
    fid_t node_fid;
    int inode_allocated = 0;
    dirent_dir_root_idx_bitmap_t root_idx_bitmap;
    dirent_dir_root_idx_bitmap_t *root_idx_bitmap_p = NULL;
    mdirent_fid_name_info_t fid_name_info;
    rozofs_inode_t *fake_inode;
    exp_trck_top_header_t *p = NULL;
    int ret;
    int root_dirent_mask = 0;
   
    START_PROFILING(export_mkdir);
    mdir_init(&node_mdir);
    
    if (exp_metadata_inode_is_del_pending(pfid))
    {    
      errno = EROFS;
      goto error;
    }
    memset(&root_idx_bitmap,0,sizeof(dirent_dir_root_idx_bitmap_t));

    /*
    ** Check meta data device left size
    */
    if (export_metadata_device_full(e,tic)) {
      errno = ENOSPC;
      goto error;      
    }

    // get the lv2 parent
    if (!(plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, pfid)))
        goto error;
    /*
    ** Check the case of the trash
    */
    if (strcmp(name,ROZOFS_DIR_TRASH)==0)
    {
      /*
      ** the creation MUST be rejected if the parent directory is already in the trash
      */
      if (exp_metadata_inode_is_del_pending(pfid))
      {
        errno=EPERM;
	goto error;
      }
      /*
      ** check if the directory has already a trash
      */
      if (rozofs_has_trash(&plv2->attributes.s.attrs.sids[0]) != 0)
      {
        errno = EEXIST;
	goto error;
      }
      /*
      ** get the attributes of the directory and parent directory
      */
      export_recopy_extended_attributes(e,plv2,attrs);
      export_recopy_extended_attributes(e,plv2,pattrs);
      /*
      ** assert the delete pending bit on the pseudo trash directory and set the key to ROZOFS_TRASH
      */
      exp_metadata_inode_del_assert(attrs->attrs.fid);
      rozofs_inode_set_trash(attrs->attrs.fid);
      /*
      ** re-write the parent attributes since the trash has been added
      */
      rozofs_set_trash_sid0(&plv2->attributes.s.attrs.sids[0]);
      export_lv2_write_attributes(e->trk_tb_p,plv2,0/* No sync */);
      status = 0;
      /*
      ** we are done
      */
      goto trash_out;	       
    
    }    
    /*
    ** Check parent GID bit
    */
    if (plv2->attributes.s.attrs.mode & S_ISGID) {
      gid   = plv2->attributes.s.attrs.gid;
      mode |= S_ISGID;
    }        
    
    /*
    ** load the root_idx bitmap of the old parent
    */
    export_dir_load_root_idx_bitmap(e,pfid,plv2);
    /* 
    ** assert the global variables associated with the current export
    */
    fdp = export_open_parent_directory(e,pfid);
    if (get_mdirentry(plv2->dirent_root_idx_p,fdp, pfid, name, node_fid, &attrs->attrs.mode,&root_dirent_mask) == 0) {
        /*
	** if the directory already exist, it should not be an issue because from a VFS standpoint, a lookup
	** took place before the mkdir. So if we enter here it is because the lookup returns ENOENT, so the 
	** VFS can proceed with a mkdir. But in the meantime there was another node that did the same. So the
	** safe way here is to get the i-node attribute of the directory and makes the mkdir successful.
	*/
	lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, node_fid);
	if (lv2 !=NULL) {
	  if (common_config.mkdir_ok_instead_of_eexist) {
	     /*
	     ** get the attributes of the directory and parent directory
	     */
             export_recopy_extended_attributes(e,lv2,attrs);
             export_recopy_extended_attributes(e,plv2,pattrs);
	     status = 0;
	     goto out;	   
  	  }
  	  /*
	  **  The directory is not found in the inode file:
	  **
	  ** It might be possible that the file is still referenced in the dirent file but 
	  ** not present on disk: its FID has been released (and the associated file deleted)
	  ** In that case when attempt to read that fid file, we get a ENOENT error.
	  ** So for that particular case, we remove the entry from the dirent file
	  **
	  **  open point : that issue is not related to regular file but also applied to directory
	  ** 
	  */
          int xerrno;
          uint32_t type;
          fid_t fid;
	  if (errno != ENOENT) goto error;
	  /*
	  **  The entry was not existing: save the initial errno and remove that entry
	  */
          xerrno = errno;
          del_mdirentry(plv2->dirent_root_idx_p,fdp, pfid, name, fid, &type,root_dirent_mask);
          errno = xerrno;
	}  
    }
    /*
     ** nothing has been found, need to check the read only flag:
     ** that flag is asserted if some parts of dirent files are unreadable 
     */
    if (DIRENT_ROOT_IS_READ_ONLY()) {
        xerrno = EIO;
        goto error_read_only;
    }    
    /*
    ** Check that some space is left for the new file in case a hard quota is set
    ** take care of the tin provisioning configuration case for the exportd
    */
    if (e->hquota) {
      if (e->thin==0)
      {
	export_fstat_t * estats = export_fstat_get_stat(e->eid);    
	if ((estats!=NULL) && (estats->blocks >= e->hquota)) {
          errno = ENOSPC;
	 goto error;
	}
      }
      else
      {
	export_fstat_t * estats = export_fstat_get_stat(e->eid);    
	if ((estats!=NULL) && (estats->blocks_thin >= e->hquota)) {
          errno = ENOSPC;
	 goto error;
	}
      }      
    }    
    /*
    **  check user and group quota
    */
    {
       int ret;
       
       ret = rozofs_qt_check_quota(e->eid,uid,gid,plv2->attributes.s.attrs.cid);
       if (ret < 0)
       {
         errno = ENOSPC;
         goto error;
       }         
    }
    /*
    ** get the slice of the parent
    */
    exp_trck_get_slice(pfid,&pslice);
    /*
    ** copy the parent fid and the name of the regular file
    */
    memset(&ext_attrs,0x00,sizeof(ext_attrs));    
    memcpy(&ext_attrs.s.pfid,pfid,sizeof(fid_t));
    attrs->attrs.cid = 0;
    ext_attrs.s.attrs.cid =plv2->attributes.s.attrs.cid;
    
    memset(&ext_attrs.s.attrs.sids, 0, ROZOFS_SAFE_MAX * sizeof (sid_t));
    /*
    ** check if the parent has the backup extended attribute set (take care of the recursive case only)
    */
    if (((rozofs_dir0_sids_t*)&plv2->attributes.s.attrs.sids[0])->s.backup == ROZOFS_DIR_BACKUP_RECURSIVE) 
    {
         ((rozofs_dir0_sids_t*)&ext_attrs.s.attrs.sids[0])->s.backup = ROZOFS_DIR_BACKUP_RECURSIVE;
    }
    /*
    ** check if the parent has the trash extended attribute set (take care of the recursive case only)
    */
    if (((rozofs_dir0_sids_t*)&plv2->attributes.s.attrs.sids[0])->s.trash == ROZOFS_DIR_TRASH_RECURSIVE) 
    {
         ((rozofs_dir0_sids_t*)&ext_attrs.s.attrs.sids[0])->s.trash = ROZOFS_DIR_TRASH_RECURSIVE;
    }
    ext_attrs.s.i_extra_isize = ROZOFS_I_EXTRA_ISIZE;
    ext_attrs.s.i_state = 0;
    ext_attrs.s.i_file_acl = 0;
    ext_attrs.s.i_link_name = 0;
    ext_attrs.s.attrs.mode = mode;
    rozofs_clear_xattr_flag(&ext_attrs.s.attrs.mode);
    ext_attrs.s.attrs.uid = uid;
    ext_attrs.s.attrs.gid = gid;
    ext_attrs.s.attrs.nlink = 2;
    if ((ext_attrs.s.attrs.ctime = ext_attrs.s.attrs.atime = ext_attrs.s.attrs.mtime = ext_attrs.s.cr8time=  time(NULL)) == -1)
        goto error;
    ext_attrs.s.attrs.size = ROZOFS_DIR_SIZE;
    ext_attrs.s.attrs.children = 0;
    /*
    ** create the inode and write the attributes on disk
    */
    if(exp_attr_create_write_cond(e->trk_tb_p,pslice,&ext_attrs,ROZOFS_DIR,NULL,0) < 0)
        goto error;
    /*
    ** indicates that the inode has been allocated: needed in case of error to release it
    */
    inode_allocated = 1;
    // create the lv2 directory
    if (export_lv2_resolve_path(e, ext_attrs.s.attrs.fid, node_path) != 0)
        goto error;

    if (mkdir(node_path, S_IRWXU) != 0)
        goto error;
	    
    // write attributes to mdir file
    if (mdir_open(&node_mdir, node_path) < 0)
        goto error;
    /**
    * clear the root idx bitmap before creating the directory.
    * the bitmap is written on disk before creation the dirent since if we do
    * it at the end, in case of failure the object could not be listed by list_direntry
    */
    uint32_t hash1,hash2;
    int root_idx;
    int len;
    memset(&root_idx_bitmap,0,sizeof(dirent_dir_root_idx_bitmap_t));
    /*
    ** update the root_idx bitmap of the parent
    */
    hash1 = filename_uuid_hash_fnv(0, name, pfid, &hash2, &len);
    /*
    ** store the hash values in the i-node
    */
    ext_attrs.s.hash1 = hash1;
    ext_attrs.s.hash2 = hash2;
    root_idx = dirent_get_root_idx(plv2->attributes.s.attrs.children,hash1);
    export_dir_update_root_idx_bitmap(plv2->dirent_root_idx_p,root_idx,1);
    if (export_dir_flush_root_idx_bitmap(e,pfid,plv2->dirent_root_idx_p) < 0)
    {
       errno = EPROTO; 
       goto error;
    }
    // update the parent
    // add the new child to the parent
    if (put_mdirentry(plv2->dirent_root_idx_p,fdp, pfid, name, ext_attrs.s.attrs.fid, 
                      ext_attrs.s.attrs.mode,&fid_name_info,
                      plv2->attributes.s.attrs.children,		      
		      &root_dirent_mask) != 0) {
        goto error;
    }
    /*
    ** store the name of the directory and write the inode on disk
    */
    exp_store_dname_in_inode(&ext_attrs.s.fname,name,&fid_name_info);
    /*
    ** flush the inode on disk
    */
    fake_inode = (rozofs_inode_t*)ext_attrs.s.attrs.fid;
    p = e->trk_tb_p->tracking_table[fake_inode->s.key];
#ifdef ROZOFS_DIR_STATS
{
   ext_dir_mattr_t *stats_attr_p;

   stats_attr_p = (ext_dir_mattr_t *)&ext_attrs.s.attrs.sids[0];
   /*
   ** update the time only if it is not in the configured period
   */
   stats_attr_p->s.version = ROZOFS_DIR_VERSION_1;
   stats_attr_p->s.update_time = ext_attrs.s.cr8time+common_config.expdir_guard_delay_sec;
}
#endif
    ret = exp_metadata_write_attributes(p,fake_inode,&ext_attrs,sizeof(ext_mattr_t), 1 /* sync */);
    if (ret < 0)
    { 
      goto error;
    }  
    /*
    ** push the mknod attribute in cache
    */
    lv2_cache_put_forced(e->lv2_cache,ext_attrs.s.attrs.fid,&ext_attrs);   
    
    plv2->attributes.s.attrs.children++;
    plv2->attributes.s.attrs.nlink++;
    plv2->attributes.s.attrs.mtime = plv2->attributes.s.attrs.ctime = time(NULL);
    /*
    ** Update the directory statistics
    */
#ifdef ROZOFS_DIR_STATS
    export_dir_update_time(plv2);
#endif
    if (export_lv2_write_attributes(e->trk_tb_p,plv2,0/* No sync */) != 0)
        goto error;

    // update export files
    export_update_files(e, 1,0);
    rozofs_qt_inode_update(e->eid,uid,gid,1,ROZOFS_QT_INC,plv2->attributes.s.attrs.cid);

    /*
    ** write the initial bitmap on disk
    */
    root_idx_bitmap_p = &root_idx_bitmap;
    ssize_t lenbit = pwrite(node_mdir.fdattrs, root_idx_bitmap_p->bitmap,DIRENT_FILE_BYTE_BITMAP_SZ,0);
    if (lenbit != DIRENT_FILE_BYTE_BITMAP_SZ)
    {
       severe("write root_idx bitmap failure (fd %d) %s",node_mdir.fdattrs,strerror(errno));
       mdir_close(&node_mdir);
       return -1;       
    }
    mdir_close(&node_mdir);
    status = 0;
    /*
    ** return the parent and child attributes
    */
    export_recopy_extended_attributes(e,(lv2_entry_t *)&ext_attrs,attrs);
    export_recopy_extended_attributes(e,plv2,pattrs);
    goto out;

error:
    /*
    ** Cose dire and attribute file if they were open
    */
    mdir_close(&node_mdir);
    
    xerrno = errno;
    if (inode_allocated)
    {
       export_tracking_table_t *trk_tb_p;   
       trk_tb_p = e->trk_tb_p;
       exp_attr_delete(trk_tb_p,ext_attrs.s.attrs.fid);        
    }
    if (xerrno != EEXIST) {
        char fname[PATH_MAX];
        mdir_t node_mdir_del;
        // XXX: put version
        sprintf(fname, "%s/%s", node_path, MDIR_ATTRS_FNAME);
        unlink(fname);
        node_mdir_del.fdp = open(node_path, O_RDONLY, S_IRWXU);
        rmdir(node_path);
        mdir_close(&node_mdir_del);
    }
error_read_only:
    errno = xerrno;

out:
    /*
    ** check if parent root idx bitmap must be updated
    */
    if (plv2 != NULL) export_dir_flush_root_idx_bitmap(e,pfid,plv2->dirent_root_idx_p);  
trash_out:
    if(fdp != -1) close(fdp);
    STOP_PROFILING(export_mkdir);
    return status;
}
/*
**__________________________________________________________________
*/
/**
*  remove the metadata associated with a file
   this corresponds to:
     the removing of extra block used for extended attributes
     the remove of the block associated with a symbolic link
   
   e : pointer to the exportd associated with the file
   lvl2: entry associated with the file
*/
int exp_delete_file(export_t * e, lv2_entry_t *lvl2)
{
   rozofs_inode_t fake_inode; 
   export_tracking_table_t *trk_tb_p;
   fid_t fid;     
   
    trk_tb_p = e->trk_tb_p;
    /*
    ** get the pointer to the attributes of the file
    */
    ext_mattr_t *rozofs_attr_p;
    
    rozofs_attr_p = &lvl2->attributes;
    /*
    ** check the presence of the extended attributes block
    */
    if (rozofs_attr_p->s.i_file_acl)
    {
       /*
       ** release the block used for extended attributes
       */
       fake_inode.fid[1] = rozofs_attr_p->s.i_file_acl;
       memcpy(fid,&fake_inode.fid[0],sizeof(fid_t));
       exp_attr_delete(trk_tb_p,fid);    
    }
    if (rozofs_attr_p->s.i_link_name)
    {
       /*
       ** release the block used for symbolic link
       */
       fake_inode.fid[1] = rozofs_attr_p->s.i_link_name;
       memcpy(fid,&fake_inode.fid[0],sizeof(fid_t));
       exp_attr_delete(trk_tb_p,fid);        
    }
    /*
    ** now delete the inode that contains the main attributes
    */
    exp_attr_delete(trk_tb_p,rozofs_attr_p->s.attrs.fid);
    return 0;        
}  
 
/*
**__________________________________________________________________
*/
/** Allocate a rmfentry_t structure to chain the deletion job
 *
 * @param trash_entry     The disk context of removed file
 * 
 * @return: the address of the allocated structure
 */
rmfentry_t * export_alloc_rmentry(rmfentry_disk_t * trash_entry) {
  rmfentry_t *rmfe = xmalloc(sizeof (rmfentry_t));
  export_rm_bins_trashed_count++;
  memcpy(rmfe->fid, trash_entry->fid, sizeof (fid_t));
  rmfe->cid = trash_entry->cid;
  memcpy(rmfe->initial_dist_set, trash_entry->initial_dist_set,
          sizeof (sid_t) * ROZOFS_SAFE_MAX);
  memcpy(rmfe->current_dist_set, trash_entry->current_dist_set,
          sizeof (sid_t) * ROZOFS_SAFE_MAX);
  memcpy(rmfe->trash_inode,trash_entry->trash_inode,sizeof(fid_t));
  list_init(&rmfe->list);
  rmfe->time = time(NULL)+common_config.deletion_delay;
  return rmfe;
}
/*
**__________________________________________________________________
*/
/** remove multiple files
 *
 * @param e: the export managing the file
 * @param pfid: the id of the parent
 * @param name: the name of this file.
 * @param[out] fid: the fid of the removed file
 * @param[out] pattrs:  to fill (parent attributes)
 * @param[in] plv2: parent attributes
 * 
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_unlink_multiple(export_t * e, fid_t parent, char *name, fid_t fid,struct inode_internal_t * pattrs,lv2_entry_t *plv2) {

    int status = -1;
    lv2_entry_t *lv2=NULL;
    fid_t child_fid;
    uint32_t child_type;
    int fdp = -1;
    int ret;
    rozofs_inode_t *fake_inode_p;
    rmfentry_disk_t trash_entry;
    int root_dirent_mask = 0;
    int unknown_fid = 0;
    int deleted_fid_count = 0;
    char filename[1024];
    char *basename_p;
    int filecount;
    char *filename_p;
    int k;    
    int quota_uid = -1;
    int quota_gid = -1;
    uint64_t quota_size = 0;
    int quota_usr_grp_catched = 0;
    /*
    ** search for rozofs key in the filename
    */
    if (strncmp(name,".@rozofs-mf@",12) == 0)
    {
      ret = rozofs_parse_object_name((char*)(name+12),&basename_p,&filecount);
      if (ret < 0)
      {
	errno = ENOTSUP;
	goto out;
      }
    }
    /*
    ** load the root_idx bitmap of the old parent
    */
    export_dir_load_root_idx_bitmap(e,parent,plv2);
    /*
    ** set global variables associated with the export
    */
    fdp = export_open_parent_directory(e,parent);
    if (get_mdirentry(plv2->dirent_root_idx_p,fdp, parent, name, child_fid, &child_type,&root_dirent_mask) != 0)
        goto out;

    if (S_ISDIR(child_type)) {
        errno = EISDIR;
        goto out;
    }
    /*
    ** do a loop on the file count
    */
    for (k = 0; k < filecount ; k++)
    {
      sprintf(filename,"%s.%d",basename_p,k+1);
      filename_p = filename;
      // Delete the mdirentry if exist
      ret =del_mdirentry(plv2->dirent_root_idx_p,fdp, parent, filename_p, child_fid, &child_type,root_dirent_mask);
      if (ret != 0)
      {
	  if (errno != ENOENT) goto out;
	  /*
	  ** check the next entry
	  */
	  continue;
      }
      // Get mattrs of child to delete
      if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, child_fid)))
      {
	    unknown_fid++;
	    /*
	    ** check the next entry
	    */
	    continue;
      }
      {
         /*
	 ** get the group and user : all the file must share the same group and user id!!
	 */
	 if (quota_usr_grp_catched == 0)
	 {
	   quota_uid = lv2->attributes.s.attrs.uid;	 
	   quota_gid = lv2->attributes.s.attrs.uid;
	   quota_usr_grp_catched = 1;	
	 } 
	 quota_size += lv2->attributes.s.attrs.size;      
      }
      deleted_fid_count++;    

      // Compute hash value for this fid
      uint32_t hash = rozofs_storage_fid_slice(lv2->attributes.s.attrs.fid);
      
      /*
      ** prepare the trash entry
      */
      trash_entry.size = lv2->attributes.s.attrs.size;
      memcpy(trash_entry.fid, lv2->attributes.s.attrs.fid, sizeof (fid_t));
      trash_entry.cid = lv2->attributes.s.attrs.cid;
      memcpy(trash_entry.initial_dist_set, lv2->attributes.s.attrs.sids,
              sizeof (sid_t) * ROZOFS_SAFE_MAX);
      memcpy(trash_entry.current_dist_set, lv2->attributes.s.attrs.sids,
              sizeof (sid_t) * ROZOFS_SAFE_MAX);
      fake_inode_p =  (rozofs_inode_t *)parent;   
      ret = exp_trash_entry_create(e->trk_tb_p,fake_inode_p->s.usr_id,&trash_entry); 
      if (ret < 0)
      {
	 /*
	 ** error while inserting entry in trash file
	 */
	 severe("error on trash insertion name %s error %s",name,strerror(errno)); 
      }
      /*
      ** delete the metadata associated with the file
      */
      ret = exp_delete_file(e,lv2);
      /*
      * In case of geo replication, insert a delete request from the 2 sites 
      */
      if (e->volume->georep) 
      {
	/*
	** update the geo replication: set start=end=0 to indicate a deletion 
	*/
	geo_rep_insert_fid(e->geo_replication_tb[0],
                	   lv2->attributes.s.attrs.fid,
			   0/*start*/,0/*end*/,
			   e->layout,
			   lv2->attributes.s.attrs.cid,
			   lv2->attributes.s.attrs.sids);
	/*
	** update the geo replication: set start=end=0 to indicate a deletion 
	*/
	geo_rep_insert_fid(e->geo_replication_tb[1],
                	   lv2->attributes.s.attrs.fid,
			   0/*start*/,0/*end*/,
			   e->layout,
			   lv2->attributes.s.attrs.cid,
			   lv2->attributes.s.attrs.sids);
      }	
      /*
      ** Preparation of the rmfentry
      */
      rmfentry_t *rmfe = export_alloc_rmentry(&trash_entry);
      
      /* Acquire lock on bucket trash list
      */
      if ((errno = pthread_rwlock_wrlock
              (&e->trash_buckets[hash].rm_lock)) != 0) {
	  severe("pthread_rwlock_wrlock failed: %s", strerror(errno));
	  // Best effort
      }
      /*
      ** Check size of file 
      */
      if (lv2->attributes.s.attrs.size >= RM_FILE_SIZE_TRESHOLD) {
	  // Add to front of list
	  list_push_front(&e->trash_buckets[hash].rmfiles, &rmfe->list);
      } else {
	  // Add to back of list
	  list_push_back(&e->trash_buckets[hash].rmfiles, &rmfe->list);
      }

      if ((errno = pthread_rwlock_unlock
              (&e->trash_buckets[hash].rm_lock)) != 0) {
	  severe("pthread_rwlock_unlock failed: %s", strerror(errno));
	  // Best effort
      }
      // Update the nb. of blocks
      if (export_update_blocks(e,0,
              (((int64_t) lv2->attributes.s.attrs.size + ROZOFS_BSIZE_BYTES(e->bsize) - 1)/ ROZOFS_BSIZE_BYTES(e->bsize)),lv2->attributes.s.attrs.children) != 0) {
	  severe("export_update_blocks failed: %s", strerror(errno));
	  // Best effort
      }
      // Remove from the cache (will be closed and freed)
      if (export_attr_thread_check_context(lv2)==0) lv2_cache_del(e->lv2_cache, child_fid);
    }  
    /*
    ** all the subfile have been deleted so  Update export files
    */
    export_update_files(e, 0-deleted_fid_count,0);
    rozofs_qt_inode_update(e->eid,quota_uid,quota_gid,deleted_fid_count,ROZOFS_QT_DEC,plv2->attributes.s.attrs.cid);
    rozofs_qt_inode_update(e->eid,quota_uid,quota_gid,quota_size,ROZOFS_QT_DEC,plv2->attributes.s.attrs.cid);

    // Update parent
    plv2->attributes.s.attrs.mtime = plv2->attributes.s.attrs.ctime = time(NULL);
    plv2->attributes.s.attrs.children--;

    // Write attributes of parents
    if (export_lv2_write_attributes(e->trk_tb_p,plv2,0/* No sync */) != 0)
        goto out;
    /*
    ** return the parent attributes
    */
    export_recopy_extended_attributes(e,plv2,pattrs);
    status = 0;

out:
    /*
    ** check if parent root idx bitmap must be updated
    */
    if (plv2 != NULL) export_dir_flush_root_idx_bitmap(e,parent,plv2->dirent_root_idx_p);
    
    if(fdp != -1) close(fdp);
    return status;
}
/*
**__________________________________________________________________
*/
/** attempt to recycle the fid associated with a file
 *
 * @param e: the export managing the file
 * @param lvl2: attributes of the fid within the cache 
 * 
 * @return: 0 not recycled
 * @return: 1  recycled
 
 */
int export_fid_recycle_attempt(export_t * e,lv2_entry_t *lv2)
{
   rozofs_inode_t *fake_inode_p;
   recycle_disk_t  recycle_entry;
   uint64_t count;
   rozofs_inode_t fake_inode; 

   int ret;
     
   if (common_config.fid_recycle == 0) return 0;
   /*
   ** do not attempt to recycle fid while reload from disk is in progress
   */
   if (export_fid_recycle_ready == 0) return 0;
   
   memset(&recycle_entry,0,sizeof (recycle_disk_t));

   /*
   ** check the number of pending fid in trash
   */
   count = (export_rm_bins_reload_count+export_rm_bins_trashed_count)-export_rm_bins_done_count;
   if (count < export_rm_bins_threshold_high)
   {
     return 0;
   }
   /*
   ** update the version of the fid in the attributes
   */
   fake_inode_p =  (rozofs_inode_t *)lv2->attributes.s.attrs.fid;   
   rozofs_inc_recycle_on_fid(fake_inode_p);
   
   memcpy(recycle_entry.fid, lv2->attributes.s.attrs.fid, sizeof (fid_t));
   ret = exp_recycle_entry_create(e->trk_tb_p,fake_inode_p->s.usr_id,&recycle_entry); 
   if (ret < 0)
   {
      /*
      ** error while inserting entry in recycle file
      */
      severe("error on recycle insertion error %s",strerror(errno)); 
   }
   /*
   ** clear the creation time to indicate that the i-node is a recycled inode
   */
   lv2->attributes.s.cr8time = 0;
   export_tracking_table_t *trk_tb_p;
   
   trk_tb_p = e->trk_tb_p;
   /*
   ** check the presence of the extended attributes block
   */
   if (lv2->attributes.s.i_file_acl)
   {
      /*
      ** release the block used for extended attributes
      ** note: don't care about the upper 64bits of the fid!!
      */
      fid_t fid;
      fake_inode.fid[1] = lv2->attributes.s.i_file_acl;
      memcpy(fid,&fake_inode.fid[0],sizeof(fid_t));
      exp_attr_delete(trk_tb_p,fid);    
      lv2->attributes.s.i_file_acl = 0;
   }
   /*
   ** write back attributes on disk
   */ 
   if (export_lv2_write_attributes(e->trk_tb_p,lv2, 1/* sync */) != 0)
   {
     severe("fail to write attributes on disk");
   }
   /*
   ** save it in memory
   */
   /*
   ** Preparation of the recycle entry in memory
   */
   recycle_mem_t *rmfe = xmalloc(sizeof (recycle_mem_t));
   memset(rmfe,0,sizeof (recycle_mem_t));
   export_recycle_pending_count++;
   memcpy(rmfe->fid, recycle_entry.fid, sizeof (fid_t));
   memcpy(rmfe->recycle_inode,recycle_entry.recycle_inode,sizeof(fid_t));
   list_init(&rmfe->list);   
   /*
   ** insert it the list
   */   
   list_push_back(&e->recycle_buckets[fake_inode_p->s.usr_id].rmfiles, &rmfe->list);
   return 1;
}

char *export_build_deleted_name(char *buffer,uint64_t *inode_val,char *name)
{
    time_t secondes;
    struct tm instant;
    char bufall[128];
    
    time(&secondes);
    instant=*localtime(&secondes);
    instant.tm_sec = 0;
    instant.tm_min = 0;
    secondes = mktime(&instant);
    instant=*localtime(&secondes);
    strftime(bufall,80,"%F-%H", &instant);
    if (inode_val==NULL) sprintf(buffer,"@%s@@%s",bufall,name);
    else sprintf(buffer,"@%s@%llx@%s",bufall,(unsigned long long int)*inode_val,name);
    return buffer;
}
/*
**__________________________________________________________________
*/
/** remove a file for which there was a duplicate fid
    it is assumed that the fid has the delete pending bit asserted
 
  @param e: the export managing the file
  @param plv2: attributes of the parent
  @param parent: the fid of parent
  @param child: the fid of the removed file
  
  @revtal none
*/
void export_unlink_duplicate_fid(export_t * e,lv2_entry_t  *plv2,fid_t parent, fid_t child_fid)
{
  lv2_entry_t  *lv2=NULL;
  int ret;
  int fid_has_been_recycled = 0;  
  rmfentry_disk_t trash_entry;
  int quota_uid=-1;
  int quota_gid=-1;
  uint64_t quota_size = 0;
  rozofs_inode_t *fake_inode_p;
      
  if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, child_fid)))
    return;
  
  if (!S_ISREG(lv2->attributes.s.attrs.mode))
  {
    severe("not a regular file");
    return;
  }
   /*
   ** clear the delete pending bit from the lv2 entry
   */
   exp_metadata_inode_del_deassert(lv2->attributes.s.attrs.fid);
   /*
   ** check the case of the fid recycle
   */
   fid_has_been_recycled = export_fid_recycle_attempt(e,lv2);
   if (fid_has_been_recycled == 0)
   {
     /*
     ** take care of the mover: the "mover" distribution must be pushed in trash when it exits
     */
     rozofs_mover_unlink_mover_distribution(e,lv2);
     // Compute hash value for this fid
     uint32_t hash = rozofs_storage_fid_slice(lv2->attributes.s.attrs.fid);

     /*
     ** prepare the trash entry
     */
     trash_entry.size = lv2->attributes.s.attrs.size;
     memcpy(trash_entry.fid, lv2->attributes.s.attrs.fid, sizeof (fid_t));
     /*
     ** compute the storage fid
     */
     {
       rozofs_mover_children_t mover_idx;
       mover_idx.u32 = lv2->attributes.s.attrs.children;
       rozofs_build_storage_fid(trash_entry.fid,mover_idx.fid_st_idx.primary_idx);
     }
     trash_entry.cid = lv2->attributes.s.attrs.cid;
     memcpy(trash_entry.initial_dist_set, lv2->attributes.s.attrs.sids,
             sizeof (sid_t) * ROZOFS_SAFE_MAX);
     memcpy(trash_entry.current_dist_set, lv2->attributes.s.attrs.sids,
             sizeof (sid_t) * ROZOFS_SAFE_MAX);
     fake_inode_p =  (rozofs_inode_t *)parent;   
     ret = exp_trash_entry_create(e->trk_tb_p,fake_inode_p->s.usr_id,&trash_entry); 
     if (ret < 0)
     {
	/*
	** error while inserting entry in trash file
	*/
	severe("error on trash insertion  error %s",strerror(errno)); 
     }
     /*
     ** delete the metadata associated with the file
     */
     ret = exp_delete_file(e,lv2);
     /*
     * In case of geo replication, insert a delete request from the 2 sites 
     */
     if (e->volume->georep) 
     {
       /*
       ** update the geo replication: set start=end=0 to indicate a deletion 
       */
       geo_rep_insert_fid(e->geo_replication_tb[0],
                	  lv2->attributes.s.attrs.fid,
			  0/*start*/,0/*end*/,
			  e->layout,
			  lv2->attributes.s.attrs.cid,
			  lv2->attributes.s.attrs.sids);
       /*
       ** update the geo replication: set start=end=0 to indicate a deletion 
       */
       geo_rep_insert_fid(e->geo_replication_tb[1],
                	  lv2->attributes.s.attrs.fid,
			  0/*start*/,0/*end*/,
			  e->layout,
			  lv2->attributes.s.attrs.cid,
			  lv2->attributes.s.attrs.sids);
     }	
     /*
     ** Preparation of the rmfentry
     */
     rmfentry_t *rmfe = export_alloc_rmentry(&trash_entry);

     /* Acquire lock on bucket trash list
     */
     if ((errno = pthread_rwlock_wrlock
             (&e->trash_buckets[hash].rm_lock)) != 0) {
         severe("pthread_rwlock_wrlock failed: %s", strerror(errno));
         // Best effort
     }
     /*
     ** Check size of file 
     */
     if (hash >= common_config.storio_slice_number )
     {	      
       severe("bad hash value %d (max %d)",hash,common_config.storio_slice_number);
     }
     if (lv2->attributes.s.attrs.size >= RM_FILE_SIZE_TRESHOLD) {
         // Add to front of list
         list_push_front(&e->trash_buckets[hash].rmfiles, &rmfe->list);
     } else {
         // Add to back of list
         list_push_back(&e->trash_buckets[hash].rmfiles, &rmfe->list);
     }

     if ((errno = pthread_rwlock_unlock
             (&e->trash_buckets[hash].rm_lock)) != 0) {
         severe("pthread_rwlock_unlock failed: %s", strerror(errno));
         // Best effort
     }
   }
   /*
   ** Update the nb. of blocks
   */
   if (export_update_blocks(e,0,
           (((int64_t) lv2->attributes.s.attrs.size + ROZOFS_BSIZE_BYTES(e->bsize) - 1)
           / ROZOFS_BSIZE_BYTES(e->bsize)),lv2->attributes.s.attrs.children) != 0) {
       severe("export_update_blocks failed: %s", strerror(errno));
       // Best effort
   }
   /*
   ** update the quota: only if file is really deleted
   */
   rozofs_qt_inode_update(e->eid,quota_uid,quota_gid,1,ROZOFS_QT_DEC,plv2->attributes.s.attrs.cid);
   rozofs_qt_block_update(e->eid,quota_uid,quota_gid,quota_size,ROZOFS_QT_DEC,plv2->attributes.s.attrs.cid);
   /*
   ** Update export files
   */
   export_update_files(e, -1,lv2->attributes.s.attrs.children);
   /*
   ** Remove from the cache when deleted (will be closed and freed)
   */
   if (fid_has_been_recycled == 0)
   {
     if (export_attr_thread_check_context(lv2)==0) lv2_cache_del(e->lv2_cache, child_fid);
   }
   plv2->attributes.s.hpc_reserved.dir.nb_deleted_files--;    
}
/*
**__________________________________________________________________
*/
/** remove a file
 *
 * @param e: the export managing the file
 * @param pfid: the id of the parent
 * @param name: the name of this file.
 * @param[out] fid: the fid of the removed file
 * @param[out] pattrs:  to fill (parent attributes)
 * 
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_unlink(export_t * e, fid_t parent, char *name, fid_t fid,struct inode_internal_t * pattrs) {
    int status = -1;
    lv2_entry_t *plv2=NULL, *lv2=NULL;
    fid_t child_fid;
    uint32_t child_type;
    uint16_t nlink = 0;
    int fdp = -1;
    int ret;
    rozofs_inode_t *fake_inode_p;
    rmfentry_disk_t trash_entry;
    int root_dirent_mask = 0;
    int quota_uid=-1;
    int quota_gid=-1;
    uint64_t quota_size = 0;
    int update_children = 1;
    int rename = 0;
    int write_parent_attributes = 0;
    int parent_state;

    START_PROFILING(export_unlink);

    // Get the lv2 parent
    if (!(plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, parent)))
        goto out;

    if (strncmp(name,".@rozofs-mf@",12) == 0)
    {
      int ret;
      ret = export_unlink_multiple(e,parent,name, fid,pattrs,plv2);
      if (ret !=0)
      {
	STOP_PROFILING(export_unlink);
	return ret;
      }
    }
    /*
    ** check for rename:
    **
    */
    parent_state = rozofs_export_eval_parent_delete_state(parent,plv2->attributes.s.attrs.fid);
    switch (parent_state)
    {
       /*
       ** VFS active/RozoFS active
       */
       case 0:
	 if ((common_config.export_versioning) || (rozofs_has_trash(&plv2->attributes.s.attrs.sids[0]) != 0)) rename = 1;
	 else rename = 0;
	 update_children = 1; 
	 break;   
       /*
       ** VFS active/RozoFS deleted --> the directory has been deleted
       */
       case 1:
	 rename = 0;
	 update_children = 0;
	 break;   
       /*
       ** VFS delete/RozoFS active --> designates the trash of an active directory: dir/@rozofs-trash@
       */
       case 2:
	 rename = 0;
	 update_children = 0;
	 break;          
       /*
       **  VFS delete/RozoFS delete --> designates the trash of a deleted directory --> supported because of the trash process
       */
       case 3:
	 rename = 0;
	 update_children = 0;
	 break;   
       default:
	 errno = EPERM;
         goto out;
	 break;       
    }
    /*
    ** load the root_idx bitmap of the old parent
    */
    export_dir_load_root_idx_bitmap(e,parent,plv2);
    /*
    ** set global variables associated with the export
    */
    fdp = export_open_parent_directory(e,parent);
    if (get_mdirentry(plv2->dirent_root_idx_p,fdp, parent, name, child_fid, &child_type,&root_dirent_mask) != 0)
        goto out;

    if (S_ISDIR(child_type)) {
        errno = EISDIR;
        goto out;
    }
    /*
    ** check if the file has its delete pending bit asserted
    */
    if ((exp_metadata_inode_is_del_pending(child_fid)==1) && (rename==1))
    {
      /*
      ** do not accept the rename of a file that has its delete pendng bit asserted
      */
      errno=EACCES;
      goto out;    
    }
    /*
    ** Avoid to delete a an active file if it is referenced with a parent that targets deleted directory 
    ** or the trash
    */
    if ((parent_state==1) ||(parent_state==2))
    {
      /*
      ** the parent directory is either ROZOFS_DIR_TRASH or a deleted directory
      */
      if (exp_metadata_inode_is_del_pending(child_fid)==0)
      {
         /*
	 ** cannot reference an active file in a trash!
	 */
	errno=EACCES;
	goto out;    
      }     
    }

    // Delete the mdirentry if exist
    if (del_mdirentry(plv2->dirent_root_idx_p,fdp, parent, name, child_fid, &child_type,root_dirent_mask) != 0)
        goto out;

    // Get mattrs of child to delete
    if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, child_fid)))
        goto out;
    /*
    ** get the user id/group id and size for quota management
    */
    quota_size = lv2->attributes.s.attrs.size;
    quota_uid = lv2->attributes.s.attrs.uid;
    quota_gid = lv2->attributes.s.attrs.gid; 

    // Return the fid of deleted file
    memcpy(fid, child_fid, sizeof (fid_t));


    // Get nlink
    nlink = lv2->attributes.s.attrs.nlink;

    // 2 cases:
    // nlink > 1, it's a hardlink -> not delete the lv2 file
    // nlink=1, it's not a harlink -> put the lv2 file on trash directory

    // Not a hardlink
duplicate_deleted_file:
    if (nlink == 1) {
        int fid_has_been_recycled = 0;

        if (S_ISREG(lv2->attributes.s.attrs.mode)) {
	   /*
	   ** if the file size is 0 clear the rename flag
	   */
	   if (lv2->attributes.s.attrs.size == 0)
	   {
	     rename = 0;
	     update_children = 1;       	   
	   }
	   if (rename == 0)
	   {
	      /*
	      ** clear the delete pending bit from the lv2 entry
	      */
	      exp_metadata_inode_del_deassert(lv2->attributes.s.attrs.fid);
	      /*
	      ** check the case of the fid recycle
	      */
	      fid_has_been_recycled = export_fid_recycle_attempt(e,lv2);
	      if (fid_has_been_recycled == 0)
	      {
	        /*
		** take care of the mover: the "mover" distribution must be pushed in trash when it exists
		*/
	        rozofs_mover_unlink_mover_distribution(e,lv2);
        	// Compute hash value for this fid
        	uint32_t hash = rozofs_storage_fid_slice(lv2->attributes.s.attrs.fid);

        	/*
		** prepare the trash entry
		*/
		trash_entry.size = lv2->attributes.s.attrs.size;
        	memcpy(trash_entry.fid, lv2->attributes.s.attrs.fid, sizeof (fid_t));
		/*
		** compute the storage fid
		*/
		{
		  rozofs_mover_children_t mover_idx;
		  mover_idx.u32 = lv2->attributes.s.attrs.children;
		  rozofs_build_storage_fid(trash_entry.fid,mover_idx.fid_st_idx.primary_idx);
		}

        	trash_entry.cid = lv2->attributes.s.attrs.cid;
        	memcpy(trash_entry.initial_dist_set, lv2->attributes.s.attrs.sids,
                	sizeof (sid_t) * ROZOFS_SAFE_MAX);
        	memcpy(trash_entry.current_dist_set, lv2->attributes.s.attrs.sids,
                	sizeof (sid_t) * ROZOFS_SAFE_MAX);
		fake_inode_p =  (rozofs_inode_t *)parent;   
        	ret = exp_trash_entry_create(e->trk_tb_p,fake_inode_p->s.usr_id,&trash_entry); 
		if (ret < 0)
		{
		   /*
		   ** error while inserting entry in trash file
		   */
		   severe("error on trash insertion name %s error %s",name,strerror(errno)); 
        	}
        	/*
		** delete the metadata associated with the file
		*/
		ret = exp_delete_file(e,lv2);
		/*
		* In case of geo replication, insert a delete request from the 2 sites 
		*/
		if (e->volume->georep) 
		{
		  /*
		  ** update the geo replication: set start=end=0 to indicate a deletion 
		  */
		  geo_rep_insert_fid(e->geo_replication_tb[0],
                		     lv2->attributes.s.attrs.fid,
				     0/*start*/,0/*end*/,
				     e->layout,
				     lv2->attributes.s.attrs.cid,
				     lv2->attributes.s.attrs.sids);
		  /*
		  ** update the geo replication: set start=end=0 to indicate a deletion 
		  */
		  geo_rep_insert_fid(e->geo_replication_tb[1],
                		     lv2->attributes.s.attrs.fid,
				     0/*start*/,0/*end*/,
				     e->layout,
				     lv2->attributes.s.attrs.cid,
				     lv2->attributes.s.attrs.sids);
		}	
        	/*
		** Preparation of the rmfentry
		*/
                rmfentry_t *rmfe = export_alloc_rmentry(&trash_entry);

        	/* Acquire lock on bucket trash list
		*/
        	if ((errno = pthread_rwlock_wrlock
                	(&e->trash_buckets[hash].rm_lock)) != 0) {
                    severe("pthread_rwlock_wrlock failed: %s", strerror(errno));
                    // Best effort
        	}
        	/*
		** Check size of file 
		*/
		if (hash >= common_config.storio_slice_number )
		{	      
	          severe(" bad hash value %d (max %d)",hash,common_config.storio_slice_number);
		}
        	if (lv2->attributes.s.attrs.size >= RM_FILE_SIZE_TRESHOLD) {
                    // Add to front of list
                    list_push_front(&e->trash_buckets[hash].rmfiles, &rmfe->list);
        	} else {
                    // Add to back of list
                    list_push_back(&e->trash_buckets[hash].rmfiles, &rmfe->list);
        	}

        	if ((errno = pthread_rwlock_unlock
                	(&e->trash_buckets[hash].rm_lock)) != 0) {
                    severe("pthread_rwlock_unlock failed: %s", strerror(errno));
                    // Best effort
        	}
	      }
	    }
            /*
	    ** Update the nb. of blocks: that action is taken only the the file is deleted
	    */
	    if (rename == 0)
	    {
              if (export_update_blocks(e,0,
                      (((int64_t) lv2->attributes.s.attrs.size + ROZOFS_BSIZE_BYTES(e->bsize) - 1)
                      / ROZOFS_BSIZE_BYTES(e->bsize)),lv2->attributes.s.attrs.children) != 0) {
                  severe("export_update_blocks failed: %s", strerror(errno));
                  // Best effort
              }
	      /*
	      ** check the case of the exportd configured with thoin provisioning
	      */
	      if (e->thin)
	      {
	        expthin_update_blocks(e,(uint32_t)lv2->attributes.s.hpc_reserved.reg.nb_blocks_thin,-1);  // Thin prov fix
	      }
	      /*
	      ** Update the directory statistics
	      */
#ifdef ROZOFS_DIR_STATS
              export_dir_adjust_child_size(plv2,lv2->attributes.s.attrs.size,0,ROZOFS_BSIZE_BYTES(e->bsize));
#endif
	    }
        } 
	else 
	{
	    /*
	    ** release the inode entry: case of the symbolic link: no delete defer for symbolic link
	    */
	    rename = 0;
	    update_children = 1;
	    quota_size = 0;
	    if (exp_delete_file(e,lv2) < 0)
	    {
	       severe("error on inode %s release : %s",name,strerror(errno));
	    }
        }
	/*
	** update the quota: only if file is really deleted
	*/
	if (rename == 0)
	{
           rozofs_qt_inode_update(e->eid,quota_uid,quota_gid,1,ROZOFS_QT_DEC,plv2->attributes.s.attrs.cid);
           rozofs_qt_block_update(e->eid,quota_uid,quota_gid,quota_size,ROZOFS_QT_DEC,plv2->attributes.s.attrs.cid);
           // Update export files
          export_update_files(e, -1,lv2->attributes.s.attrs.children);

          // Remove from the cache when deleted (will be closed and freed)
          if (fid_has_been_recycled == 0)
	  {
            if (export_attr_thread_check_context(lv2)==0) {
              lv2_cache_del(e->lv2_cache, child_fid);
            }
            else {
              /*
              ** Remove an entry from the attribute cache without deleting it
              ** since a thread is taking care of it
              */
              lv2_cache_remove_hash(e->lv2_cache, child_fid);
            }           
	  } 
        } 
    }
    /*
    **  IT'S A HARDLINK
    */
    if (nlink > 1) {
        rename = 0;
	update_children = 1;
        lv2->attributes.s.attrs.nlink--;
        lv2->attributes.s.attrs.ctime = time(NULL);
        export_lv2_write_attributes(e->trk_tb_p,lv2, 0/* No sync */);
#ifdef ROZOFS_DIR_STATS
        export_dir_adjust_child_size(plv2,lv2->attributes.s.attrs.size,0,ROZOFS_BSIZE_BYTES(e->bsize));
#endif
        // Return a empty fid because no inode has been deleted
        //memset(fid, 0, sizeof (fid_t));
    }

    // Update parent
    plv2->attributes.s.attrs.mtime = plv2->attributes.s.attrs.ctime = time(NULL);
    if (update_children == 1) 
    {
      plv2->attributes.s.attrs.children--;
    }
    /*
    ** check if the count of deleted file must be updated
    */
    if (rename == 1)
    {
      plv2->attributes.s.hpc_reserved.dir.nb_deleted_files++;    
    }
    else
    {
      /*
      ** update the deleted count if the directory was either a deleted directory or the 
      ** pseudo trash of an active directory
      */
      if ((parent_state==1) ||(parent_state==2)||(parent_state==3))
      {
	 plv2->attributes.s.hpc_reserved.dir.nb_deleted_files--;          
      }    
    }

    // Write attributes of parents
    write_parent_attributes = 1;
    /*
    ** return the parent attributes
    */
    export_recopy_extended_attributes(e,plv2,pattrs);
    status = 0;
    /*
    ** CHECK THE CASE OF THE RENAME
    */
    if (rename)
    {
       mdirent_fid_name_info_t fid_name_info;
       char            del_name[2048];
       uint32_t        hash1,hash2;
       int             root_idx;
       int             len;  
       rozofs_inode_t *inode_p;
       uint64_t        key_del; 
       fid_t           old_child_fid;
       uint32_t        old_child_type;
           
       /*
       ** Get the attributes of the object to remove
       */
       if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, child_fid)))
       {
          goto out;
       }       
       inode_p = (rozofs_inode_t*)fid;
       key_del= inode_p->s.file_id;
       key_del = key_del<<11;
       key_del |= inode_p->s.idx;
       /*
       ** append the delete pending prefix to the object name
       */
       export_build_deleted_name(del_name,NULL,name);
       /*
       ** update the bit in the root_idx bitmap of the parent directory
       */
       hash1 = filename_uuid_hash_fnv(0, del_name,parent, &hash2, &len);    
       lv2->attributes.s.hash1 = hash1;
       lv2->attributes.s.hash2 = hash2;
       root_idx = dirent_get_root_idx(plv2->attributes.s.attrs.children,hash1);
       export_dir_update_root_idx_bitmap(plv2->dirent_root_idx_p,root_idx,1);
       if (export_dir_flush_root_idx_bitmap(e,parent,plv2->dirent_root_idx_p) < 0)
       {
	  errno = EPROTO; 
	  goto out;
       }
       /*
       ** assert the delete pending bit in the i-node
       */
       exp_metadata_inode_del_assert(child_fid);
       memcpy(lv2->attributes.s.attrs.fid,child_fid,sizeof(fid_t));
       /*
       **  Check if the deleted file is already in the dentry
       */
       if (get_mdirentry(plv2->dirent_root_idx_p,fdp, parent, del_name, old_child_fid, &old_child_type,&root_dirent_mask) == 0)
       {
         //export_unlink_duplicate_fid(e,plv2,parent,old_child_fid);
	 write_parent_attributes = 1;  
	 /*
	 ** remove the current file since there already one with the same name
	 ** the current number of deleted file must be updated since it has incremented by anticipation
	 ** turning off the rename flag implies that the file will be deleted
	 ** clear the update_children flag since the count of file within the directory has already been updated on the first pass.
	 */
	 plv2->attributes.s.hpc_reserved.dir.nb_deleted_files--;
	 rename = 0;
	 update_children = 0;
	 goto duplicate_deleted_file;  
       }
       /*
       **  update the parent
       ** add the new child to the parent
       */
       if (put_mdirentry(plv2->dirent_root_idx_p,fdp, parent, del_name, 
                	 child_fid, child_type,&fid_name_info,
			 plv2->attributes.s.attrs.children,
			 &root_dirent_mask) != 0) {
           goto out;
       } 
       /*
       ** update name in the inode structure
       */
       exp_store_fname_in_inode(&lv2->attributes.s.fname,del_name,&fid_name_info);           
       /*
       ** Update the atime of the deleted file to the trash time. To be used by the periodic trash flush process.
       **  Write attributes of deleted file (pending)
       */
       lv2->attributes.s.attrs.atime =  time(NULL);
       export_lv2_write_attributes(e->trk_tb_p,lv2, 0/* No sync */) ;
    }
out:
    /*
    ** check if parent root idx bitmap must be updated
    */
    if (plv2 != NULL) 
    {
      if (write_parent_attributes) 
      {
	/*
	** Update the directory statistics
	*/
#ifdef ROZOFS_DIR_STATS
        export_dir_update_time(plv2);
#endif
        /*
	** write parent attributes on disk
	*/
	export_attr_thread_submit(plv2,e->trk_tb_p,0 /* No sync */);
      }
      export_dir_flush_root_idx_bitmap(e,parent,plv2->dirent_root_idx_p);
    }
    
    if(fdp != -1) close(fdp);
    STOP_PROFILING(export_unlink);
    return status;
}
/*
**____________________________________________________________________________
**
** Build the list of mstorage clients to reach every storaged required on this volume
**
** @param volume       The volume we are to process
** @param cnx          The head of the list of mstorage clients to build
**
** @retval 0 on success / -1 on error
*/
int mstoraged_setup_cnx(volume_t *volume, list_t * cnx) {
  list_t *p, *q;
  int     status = 0;
  int     i;
  mstorage_client_t * mclt;

  list_init(cnx);

  if ((errno = pthread_rwlock_rdlock(&volume->lock)) != 0) {
    severe("pthread_rwlock_rdlock failed (vid: %d): %s", volume->vid, strerror(errno));
    return -1;
  }

  list_for_each_forward(p, &volume->clusters) {

    cluster_t *cluster = list_entry(p, cluster_t, list);

    for (i = 0; i < ROZOFS_GEOREP_MAX_SITE;i++) {
      list_for_each_forward(q, (&cluster->storages[i])) {

        volume_storage_t *vs = list_entry(q, volume_storage_t, list);

        mclt = mstorage_client_get(cnx, vs->host, cluster->cid, vs->sid);
	if (mclt == NULL) {
	  severe("out of memory");
          status = -1;
	  goto out;		
        }
      }
    }
  }

out:
  if ((errno = pthread_rwlock_unlock(&volume->lock)) != 0) {
    severe("pthread_rwlock_unlock failed (vid: %d): %s", volume->vid, strerror(errno));
  }

  return status;
}

/*
**_______________________________________________________________________________
*/
/**
* remove the file from the tracking file associated with the trash

   top_hdr: pointer to the top header for all slices for a given object type
   usr_id : slice to consider
   rmfentry_t *entry: pointer to the entry to remove
   
*/   
int export_rmbins_remove_from_tracking_file(export_t * e,rmfentry_t *entry)
{

   rozofs_inode_t *fake_inode;
   int nb_entries;
   int ret;
   exp_trck_file_header_t tracking_buffer_src;
   int current_count;
   exp_trck_header_memory_t *slice_hdr_p;
   char pathname[1024];
   exp_trck_top_header_t *trash_p = e->trk_tb_p->tracking_table[ROZOFS_TRASH];
   
   fake_inode = (rozofs_inode_t*)&entry->trash_inode;
   
   slice_hdr_p = trash_p->entry_p[fake_inode->s.usr_id];
   
   ret = exp_attr_delete(e->trk_tb_p,entry->trash_inode);
   if (ret < 0)
   {
      severe("error while remove file from trash tacjing file ; %s",strerror(errno));   
   }
   /*
   ** check if the tracking file must be release
   */
   ret = exp_metadata_get_tracking_file_header(trash_p,fake_inode->s.usr_id,fake_inode->s.file_id,&tracking_buffer_src,&nb_entries);
   if (ret < 0)
   {
     if (errno != ENOENT)
     {
        printf("error while reading metadata header %s\n",strerror(errno));
       return 0;
     }
     /*
     ** nothing the delete there, so continue with the next one
     */
     return 0;
   }
   /*
   ** get the current count of file to delete
   */
   current_count= exp_metadata_get_tracking_file_count(&tracking_buffer_src);
   /*
   ** if the current count is 0 and if the current file does not correspond to the last main index
   ** the file is deleted
   */
   if ((current_count == 0) && (nb_entries == EXP_TRCK_MAX_INODE_PER_FILE))
   {
      /*
      ** delete trashing file and update the first index if that one was the first
      */
      sprintf(pathname,"%s/%d/trk_%llu",trash_p->root_path,fake_inode->s.usr_id,(long long unsigned int)fake_inode->s.file_id);
      ret = unlink(pathname);
      if (ret < 0)
      {
	severe("cannot delete %s:%s\n",pathname,strerror(errno));
      }
      /*
      ** check if the file tracking correspond to the first index of the main tracking file
      */
      if (slice_hdr_p->entry.first_idx == fake_inode->s.file_id)
      {
	/*
	** update the main tracking file
	*/
	slice_hdr_p->entry.first_idx++;
	exp_trck_write_main_tracking_file(trash_p->root_path,fake_inode->s.usr_id,0,sizeof(uint64_t),&slice_hdr_p->entry.first_idx);
      }
   }
   return 0;
}
/*
**_______________________________________________________________________________
*/
/**
* remove a recycle entry  from the tracking file 

   e: pointer to exportd data structure
   entry: pointer to the entry to remove
   
*/   
int export_recycle_remove_from_tracking_file(export_t * e,recycle_mem_t *entry)
{

   rozofs_inode_t *fake_inode;
   int nb_entries;
   int ret;
   exp_trck_file_header_t tracking_buffer_src;
   int current_count;
   exp_trck_header_memory_t *slice_hdr_p;
   char pathname[1024];
   exp_trck_top_header_t *trash_p = e->trk_tb_p->tracking_table[ROZOFS_RECYCLE];
   
   fake_inode = (rozofs_inode_t*)&entry->recycle_inode;
   
   slice_hdr_p = trash_p->entry_p[fake_inode->s.usr_id];
   
   ret = exp_attr_delete(e->trk_tb_p,entry->recycle_inode);
   if (ret < 0)
   {
      severe("error while remove file from recycle tracking file ; %s",strerror(errno));   
   }
   /*
   ** check if the tracking file must be released
   */
   ret = exp_metadata_get_tracking_file_header(trash_p,fake_inode->s.usr_id,fake_inode->s.file_id,&tracking_buffer_src,&nb_entries);
   if (ret < 0)
   {
     if (errno != ENOENT)
     {
        printf("error while reading metadata header %s\n",strerror(errno));
       return 0;
     }
     /*
     ** nothing the delete there, so continue with the next one
     */
     return 0;
   }
   /*
   ** get the current count of file to delete
   */
   current_count= exp_metadata_get_tracking_file_count(&tracking_buffer_src);
   /*
   ** if the current count is 0 and if the current file does not correspond to the last main index
   ** the file is deleted
   */
   if ((current_count == 0) && (nb_entries == EXP_TRCK_MAX_INODE_PER_FILE))
   {
      /*
      ** delete trashing file and update the first index if that one was the first
      */
      sprintf(pathname,"%s/%d/trk_%llu",trash_p->root_path,fake_inode->s.usr_id,(long long unsigned int)fake_inode->s.file_id);
      ret = unlink(pathname);
      if (ret < 0)
      {
	severe("cannot delete %s:%s\n",pathname,strerror(errno));
      }
      /*
      ** check if the file tracking correspond to the first index of the main tracking file
      */
      if (slice_hdr_p->entry.first_idx == fake_inode->s.file_id)
      {
	/*
	** update the main tracking file
	*/
	slice_hdr_p->entry.first_idx++;
	exp_trck_write_main_tracking_file(trash_p->root_path,fake_inode->s.usr_id,0,sizeof(uint64_t),&slice_hdr_p->entry.first_idx);
      }
   }
   return 0;
}
/*
**_______________________________________________________________________________
*/
/**
*  trash statistics display

   @param buf : pointer to the buffer that will contains the statistics
*/
char *export_rm_bins_stats_json(char *pChar)
{
   uint64_t new_us;
   uint64_t new_count;
   uint64_t new_trashed;
   int      idx; 
   int      first=1;
   
   pChar += rozofs_string_append(pChar, "{ \"trash\" : {\n");    
   pChar += rozofs_string_append(pChar, "    \"nb threads\"    : ");
   pChar += rozofs_u32_append(pChar, common_config.nb_trash_thread);
   pChar += rozofs_string_append(pChar, ",\n    \"thread period\" : ");
   pChar += rozofs_u32_append(pChar,RM_BINS_PTHREAD_FREQUENCY_SEC*common_config.nb_trash_thread);
   pChar += rozofs_string_append(pChar, ",\n    \"max per run\"   : ");  
   pChar += rozofs_u32_append(pChar, export_limit_rm_files);  
   
   pChar += rozofs_string_append(pChar, ",\n    \"threads\": [\n");   
   for (idx=0; idx < ROZO_NB_RMBINS_THREAD; idx++) {

     /*
     ** Only diplay threads that have worked
     */
     if (rmbins_thread[idx].total_usec == 0) continue;
     
     if (first) {
       first = 0;
     }  
     else {
       pChar += rozofs_string_append(pChar, ",\n"); 
     }
       
     pChar += rozofs_string_append(pChar, "       {\"idx\": ");   
     pChar += rozofs_u32_append(pChar, idx);   
     pChar += rozofs_string_append(pChar, ", \"nb run\": ");   
     pChar += rozofs_u64_append(pChar, rmbins_thread[idx].nb_run);   
     pChar += rozofs_string_append(pChar, ", \"last count\": ");   
     pChar += rozofs_u64_append(pChar, rmbins_thread[idx].last_count);   
     pChar += rozofs_string_append(pChar, ", \"last usec\": ");   
     pChar += rozofs_u64_append(pChar, rmbins_thread[idx].last_usec);   
     pChar += rozofs_string_append(pChar, ", \"total count\": ");   
     pChar += rozofs_u64_append(pChar, rmbins_thread[idx].total_count);   
     pChar += rozofs_string_append(pChar, ", \"total usec\": ");   
     pChar += rozofs_u64_append(pChar, rmbins_thread[idx].total_usec);   
     pChar += rozofs_string_append(pChar, "}"); 
   }
   pChar += rozofs_string_append(pChar, "\n    ],\n");
      
   GETMICROLONG(new_us);

   pChar += rozofs_string_append(pChar, "    \"delete delay\"  : ");   
   pChar += rozofs_u64_append(pChar, (unsigned long long int)common_config.deletion_delay);   
   
   pChar += rozofs_string_append(pChar, ",\n    \"reloaded count\": "); 
   pChar += rozofs_u64_append(pChar, (unsigned long long int) export_rm_bins_reload_count);

   /*
   ** Trashed
   */

   pChar += rozofs_string_append(pChar, ",\n    \"trashed count\" : "); 
   new_trashed = export_rm_bins_trashed_count;
   pChar += rozofs_u64_append(pChar, (unsigned long long int) new_trashed);
   

   pChar += rozofs_string_append(pChar, ",\t    \"trashed/min\"   : ");      
   if ((export_last_us!=0) &&(new_us>export_last_us)) {
     pChar += rozofs_u64_append(pChar, (unsigned long long int) (new_trashed-export_last_trashed_json)*(60000000)/(new_us-export_last_us));
   }
   else {
     pChar += rozofs_string_append(pChar, "0"); 
   }  
   export_last_trashed_json = new_trashed;
   /*

   ** Actually deleted
   */
   
   pChar += rozofs_string_append(pChar, ",\n    \"deleted count\" : "); 
   new_count   = export_rm_bins_done_count;
   pChar += rozofs_u64_append(pChar, (unsigned long long int) new_count);
   
   
   pChar += rozofs_string_append(pChar, ",\t    \"deleted/min\"   : ");   
   if ((export_last_us!=0) &&(new_us>export_last_us)) {
     pChar += rozofs_u64_append(pChar, (unsigned long long int) (new_count-export_last_count_json)*(60000000)/(new_us-export_last_us));
   }
   else {
     pChar += rozofs_string_append(pChar, "0"); 
   }  
   export_last_count_json = new_count;
   
     
   /*
   ** Still pending
   */   
   
   pChar += rozofs_string_append(pChar, ",\n    \"pending\"       : ");   
   pChar += rozofs_u64_append(pChar, (unsigned long long int) (export_rm_bins_reload_count+export_rm_bins_trashed_count)-export_rm_bins_done_count);

   /*
   ** Recyclinging
   */   

   if (common_config.fid_recycle) {
     pChar += rozofs_string_append(pChar, ",\n    \"recycling\" : {\n");
     pChar += rozofs_string_append(pChar, "       \"threshold\" : ");   
     pChar += rozofs_u64_append(pChar, (unsigned long long int)export_rm_bins_threshold_high);   
     
     pChar += rozofs_string_append(pChar, ",\n       \"reloaded\"  : "); 
     pChar += rozofs_u64_append(pChar, (unsigned long long int) export_fid_recycle_reload_count);
     pChar += rozofs_string_append(pChar, ",\n       \"recycled\"  : "); 
     pChar += rozofs_u64_append(pChar, (unsigned long long int) export_recycle_pending_count);
     pChar += rozofs_string_append(pChar, ",\n       \"done\"      : "); 
     pChar += rozofs_u64_append(pChar, (unsigned long long int) export_recycle_done_count);
     pChar += rozofs_string_append(pChar, ",\n       \"pending\"   : ");   
     pChar += rozofs_u64_append(pChar, (unsigned long long int) (export_fid_recycle_reload_count+export_recycle_pending_count)-export_recycle_done_count);
     pChar += rozofs_string_append(pChar, "\n    }");
   }
   pChar += rozofs_string_append(pChar, "\n  }\n}\n");

   export_last_us = new_us;
   return pChar;
}
char *export_rm_bins_stats(char *pChar)
{
   uint64_t new_ticks;
   uint64_t new_count;   
   pChar += sprintf(pChar,"Trash thread period         : %d seconds\n",RM_BINS_PTHREAD_FREQUENCY_SEC);
   pChar += sprintf(pChar,"trash rate                  : %d\n",export_limit_rm_files);   
   pChar += sprintf(pChar,"trash limit                 : %llu\n",(unsigned long long int)export_rm_bins_threshold_high);         
   pChar += sprintf(pChar,"trash delay                 : %llu\n",(unsigned long long int)common_config.deletion_delay);   

   new_ticks = rdtsc();
   new_count = export_rm_bins_done_count;
   
   if ((export_last_ticks!=0) &&(new_ticks>export_last_ticks)) {
     pChar += sprintf(pChar,"throughput                  : %llu/min\n",
              (unsigned long long int) (new_count-export_last_count)*rozofs_get_cpu_frequency()*60
	      /(new_ticks-export_last_ticks));
   } 
   export_last_ticks = new_ticks;
   export_last_count = new_count;
     
   pChar += sprintf(pChar,"trash stats:\n");
   pChar += sprintf(pChar,"  - reloaded = %llu\n", (unsigned long long int) export_rm_bins_reload_count);
   pChar += sprintf(pChar,"  - trashed  = %llu\n", (unsigned long long int) export_rm_bins_trashed_count);
   pChar += sprintf(pChar,"  - done     = %llu\n", (unsigned long long int) export_rm_bins_done_count);
   pChar += sprintf(pChar,"  - pending  = %llu\n", 
        (unsigned long long int) (export_rm_bins_reload_count+export_rm_bins_trashed_count)-export_rm_bins_done_count);

   if (common_config.fid_recycle) {
     pChar += sprintf(pChar,"recycle stats:\n");
     pChar += sprintf(pChar,"  - reloaded = %llu\n", (unsigned long long int) export_fid_recycle_reload_count);
     pChar += sprintf(pChar,"  - recycled = %llu\n", (unsigned long long int) export_recycle_pending_count);
     pChar += sprintf(pChar,"  - done     = %llu\n", (unsigned long long int) export_recycle_done_count);
     pChar += sprintf(pChar,"  - pending  = %llu\n", 
          (unsigned long long int) (export_fid_recycle_reload_count+export_recycle_pending_count)-export_recycle_done_count);   
   }
   else {
     pChar += sprintf(pChar,"No FID recycling.\n");     
   }   
   return pChar;
}
/*
**_______________________________________________________________________________
*/

static inline int export_rm_bucket(export_t * e, uint8_t * cnx_init, list_t * connexions, int bucket_idx, uint8_t safe, uint8_t forward, int * processed_files) {
  int          sid_count = 0;
  int          i = 0;
  list_t       todo;
  list_t       failed;
  rmfentry_t * entry;
//  cid_t        cid;
  list_t      * p, *n;
  time_t        now;

  /*
  ** Initialize the working lists
  */
  list_init(&todo);
  list_init(&failed);

  /* 
  ** Move the whole bucket list to the working list
  */
  if ((errno = pthread_rwlock_wrlock(&e->trash_buckets[bucket_idx].rm_lock)) != 0) {
    severe("pthread_rwlock_wrlock failed: %s", strerror(errno));
    return -1;
  }
  list_move(&todo,&e->trash_buckets[bucket_idx].rmfiles);
  if ((errno = pthread_rwlock_unlock(&e->trash_buckets[bucket_idx].rm_lock)) != 0) {
    severe("pthread_rwlock_unlock failed: %s", strerror(errno));
  }

  now = time(NULL);

  /*
  ** get every entry
  */	
  list_for_each_forward_safe(p, n, &todo) {

    entry = list_entry(p,rmfentry_t,list);
    list_remove(&entry->list);

    // Nb. of bins files removed for this file
    sid_count = 0;
    
    // Not yet time to delete this file
    if ((entry->time) && (entry->time >= now)) {
      /*
      ** Put this entry in the failed list
      */
      list_push_back(&failed, &entry->list);
      continue;
    }
    
    /*
    ** This entry needs to be processed
    ** setup connections if not yet done
    */
    if (*cnx_init == 0) {
      *cnx_init = 1;
      if (mstoraged_setup_cnx(e->volume, connexions) != 0) {
        goto out;
      }
    }
    
    // For each storage associated with this file
    for (i = 0; i < safe; i++) {

      mstorage_client_t * stor = NULL;

      if (0 == entry->current_dist_set[i]) {
        sid_count++;
        continue; // The bins file has already been deleted for this server
      }

      if ((stor = mstoraged_lookup_cnx(connexions, entry->cid, entry->current_dist_set[i])) == NULL) {
        char   text[512];
	char * p = text;

        p += rozofs_string_append(p,"lookup_cnx failed FID ");
        rozofs_uuid_unparse(entry->fid,p);
	p += 36;
	p += rozofs_string_append(p," trash inode ");
        rozofs_uuid_unparse(entry->trash_inode,p);
	p += 36;	
	p += rozofs_string_append(p," cid ");
	p += rozofs_u32_append(p,entry->cid);
	p += rozofs_string_append(p," sid ");	
	p += rozofs_u32_append(p,entry->current_dist_set[i]);	
        severe("%s",text);
	
	/*
	** Invalid cid/sid
	*/
	sid_count++;
        continue;// lookup_cnx failed !!! 
      }

      if (0 == stor->status) {
        continue; // This storage is down
      }

      // Send remove request
      int spare;
      if (i<forward) spare = 0;
      else           spare = 1;
      
      if (mstoraged_client_remove2(stor, entry->cid, entry->current_dist_set[i], entry->fid, spare) != 0) {
        warning("mclient_remove failed (cid: %u; sid: %u): %s",
                entry->cid, entry->current_dist_set[i], strerror(errno));
	/*
	** Say this storage is down not to use it again 
	** during this run; this would fill up the log file.
	*/
	stor->status = 0; 
        continue; // Go to the next storage
      }

      // The bins file has been deleted successfully
      // Update distribution and nb. of bins file deleted
      entry->current_dist_set[i] = 0;
      sid_count++;
    }

    // Update the nb. of files that have been tested to be deleted.
    (*processed_files)++; 

    if (sid_count == safe) {
      /*
      ** remove the entry from the trash file
      */
      __atomic_fetch_add(&export_rm_bins_done_count, 1, __ATOMIC_SEQ_CST);
      export_rmbins_remove_from_tracking_file(e,entry); 
      xfree(entry);
    }
    else {
      /*
      ** Put this entry in the failed list
      */
      list_push_back(&failed, &entry->list);
    }

    /*
    ** Limit of processed file reached
    */
    if (*processed_files >= export_limit_rm_files) goto out;
  }  
  
out:

  /*
  ** Bucket totaly processed successfully
  */
  if (list_empty(&todo) && list_empty(&failed)) return 0;
  
  
  /*
  ** Push back the non processed entries in the bucket
  */    
  if ((errno = pthread_rwlock_wrlock(&e->trash_buckets[bucket_idx].rm_lock)) != 0) {
    severe("pthread_rwlock_wrlock failed: %s", strerror(errno));
  }
  list_move(&todo,&e->trash_buckets[bucket_idx].rmfiles);
  list_move(&todo,&failed);
  list_move(&e->trash_buckets[bucket_idx].rmfiles,&todo);
  if ((errno = pthread_rwlock_unlock(&e->trash_buckets[bucket_idx].rm_lock)) != 0) {
    severe("pthread_rwlock_unlock failed: %s", strerror(errno));
  }	    
  return 0;      
}
/*
**_______________________________________________________________________________
*/

uint64_t export_rm_bins(export_t * e, uint16_t * first_bucket_idx, rmbins_thread_t * thCtx) {
    uint16_t     bucket_idx = 0;
    uint8_t      cnx_init = 0;
    list_t       connexions;
    uint16_t     idx = 0;

    int          processed_files = 0;
    
    list_init(&connexions);
    
    // Get the nb. of safe storages for this layout
    uint16_t rozofs_safe    = rozofs_get_rozofs_safe(e->layout);
    uint16_t rozofs_forward = rozofs_get_rozofs_forward(e->layout);
   
    DEBUG_FUNCTION;

    // Loop on every bucket
    for (idx=1; idx <= common_config.storio_slice_number; idx++) {
	              
        /*
	** compute the bucket index to check
	*/
        bucket_idx = (idx+*first_bucket_idx) % common_config.storio_slice_number;

        // Check if the bucket is empty
	if (list_empty(&e->trash_buckets[bucket_idx].rmfiles)) {
	  continue;
	}  
	
	export_rm_bucket(e, &cnx_init, &connexions, bucket_idx, rozofs_safe, rozofs_forward, &processed_files);
        /*
	** Check whether the allowed count of trashed files has benn already done
	*/
        if (processed_files >= export_limit_rm_files) break; // Exit from the loop	
    }

    // Update the first bucket index to use for the next call
    if (0 == processed_files) {
        // If no files removed 
        // The next first bucket index will be 0
        // not necessary but better for debug
        *first_bucket_idx = 0;
    } else {
        *first_bucket_idx = bucket_idx;
    }

    // Release storage connexions
    mstoraged_release_cnx(&connexions);
    
    return processed_files;
}


/*
**______________________________________________________________________________
*/
/**
*   exportd rmdir: delete a directory

    @param pfid : fid of the parent and directory  name 
    @param name : fid of the parent and directory  name 
    
    @param[out] fid:  fid of the deleted directory 
    @param[out] pattrs:  attributes of the parent 
    
    @retval: 0 : success
    @retval: <0 error see errno
*/
int export_rmdir(export_t *e, fid_t pfid, char *name, fid_t fid,struct inode_internal_t * pattrs) {
    int status = -1;
    lv2_entry_t *plv2=NULL;
    lv2_entry_t *lv2=NULL;
    fid_t fake_fid;
    uint32_t fake_type;
    char lv2_path[PATH_MAX];
    char lv3_path[PATH_MAX];
    int fdp = -1;
    int root_dirent_mask = 0;
    int quota_uid,quota_gid;
    int rename = 0;
    int write_parent_attributes = 0;
    int update_children = 0;
    int parent_state = 0;
    
    START_PROFILING(export_rmdir);
    

    // get the lv2 parent
    if (!(plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, pfid)))
        goto out;
    /*
    ** load the root_idx bitmap of the  parent
    */
    export_dir_load_root_idx_bitmap(e,pfid,plv2);    
    /*
    ** check if it is an attempt to remove the pseudo trash directory
    ** If is the case, do not complain
    */
    if (strcmp(name,ROZOFS_DIR_TRASH)==0)
    {
      export_recopy_extended_attributes(e,plv2,pattrs);
// #warning clear the trash flag on rmdir rozofs-trash
//      if (plv2->attributes.s.attrs.sids[1]!= 0) {
//         plv2->attributes.s.attrs.sids[1] = 2;
//      }
      status = 0;      
      goto out;      
    }
    /*
    ** check for rename
    */
    parent_state = rozofs_export_eval_parent_delete_state(pfid,plv2->attributes.s.attrs.fid);
    switch (parent_state)
    {
       /*
       ** VFS active/RozoFS active
       */
       case 0:
	 if ((common_config.export_versioning) || rozofs_has_trash(&plv2->attributes.s.attrs.sids[0])) rename = 1;
	 else rename = 0;
	 update_children = 1; 
	 break;   
       /*
       ** VFS active/RozoFS deleted --> the directory has been deleted
       */
       case 1:
	 rename = 0;
	 update_children = 0;
	 break;   
       /*
       ** VFS delete/RozoFS active --> designates the trash of an active directory: dir/@rozofs-trash@
       */
       case 2:
	 rename = 0;
	 update_children = 0;
	 break;          
       /*
       **  VFS delete/RozoFS delete --> designates the trash of a deleted directory -->  supported because of trash process
       */
       case 3:
	 rename = 0;
	 update_children = 0;
	 break;    

       default:
	 errno = EPERM;
         goto out;
	 break;       
    }
    /*
    ** set global variables associated with the export
    */
    fdp = export_open_parent_directory(e,pfid);
    if (get_mdirentry(plv2->dirent_root_idx_p,fdp, pfid, name, fid, &fake_type,&root_dirent_mask) != 0)
        goto out;
    /*
    ** check if the file has its delete pending bit asserted
    */
    if ((exp_metadata_inode_is_del_pending(fid)==1) && (rename==1))
    {
      /*
      ** do not accept the rename of a directory that has its delete pendng bit asserted
      */
      errno=EACCES;
      goto out;    
    }

    // get the lv2
    if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid)))
        goto out;

    /*
    ** get the usr and group for quota management
    */
    quota_uid = lv2->attributes.s.attrs.uid;
    quota_gid = lv2->attributes.s.attrs.gid; 
    
    // sanity checks (is a directory and lv3 is empty)
    if (!S_ISDIR(lv2->attributes.s.attrs.mode)) {
        errno = ENOTDIR;
        goto out;
    }

    if (lv2->attributes.s.attrs.children != 0) {
        errno = ENOTEMPTY;
        goto out;
    }
    if (rename == 0)
    {
      if (lv2->attributes.s.hpc_reserved.dir.nb_deleted_files != 0) {
          /*
	  ** Check if the remove is done on the active directory: if it is the case we move it directory
	  ** in the local trash of the current directory even if that directory has no associated trash
	  */
	  if (parent_state == 0) 
	  {
	     update_children = 1;
	     rename = 1;
	  }
	  else
	  {
            errno = ENOTEMPTY;
            goto out;
	  }
      }
    }
    /*
    ** effective delete of the directory if that one is empty
    */
    if ((lv2->attributes.s.hpc_reserved.dir.nb_deleted_files == 0) && (rename==1))
    {
      rename = 0;
      update_children = 1;    
    }
    
    if (rename == 0)
    {
      // remove lv2
      if (export_lv2_resolve_path(e, fid, lv2_path) != 0)
          goto out;
      /*
       ** once the attributes file has been removed 
       ** consider that the directory is deleted, all the remaining is best effort
       */
      sprintf(lv3_path, "%s/%s", lv2_path, MDIR_ATTRS_FNAME);

      if (unlink(lv3_path) != 0) {
          if (errno != ENOENT) goto out;
      }
      // remove from the cache (will be closed and freed)
      if (export_attr_thread_check_context(lv2)==0) lv2_cache_del(e->lv2_cache, fid);
      /*
       ** rmdir is best effort since it might possible that some dirent file with empty entries remain
       */
      rmdir(lv2_path);
      /**
      * releas the inode allocated for storing the directory attributes
      */
      if (exp_attr_delete(e->trk_tb_p,fid) < 0)
      {
	 severe("error on inode %s release : %s",name,strerror(errno));
      }
    }

    // update parent:
    /*
     ** attributes of the parent must be updated first otherwise we can afce the situation where
     ** parent directory cannot be removed because the number of children is not 0
     */
    if (update_children == 1) 
    {
    if (plv2->attributes.s.attrs.children > 0) plv2->attributes.s.attrs.children--;
    plv2->attributes.s.attrs.nlink--;
    }
    if (rename==1)
    {
      plv2->attributes.s.hpc_reserved.dir.nb_deleted_files++;        
    }
    else
    {
      /*
      ** update the deleted count if the directory was either a deleted directory or the 
      ** pseudo trash of an active directory
      */
      if ((parent_state==1) ||(parent_state==2)||(parent_state==3))
      {
	 plv2->attributes.s.hpc_reserved.dir.nb_deleted_files--;          
      }     
    }
    plv2->attributes.s.attrs.mtime = plv2->attributes.s.attrs.ctime = time(NULL);
    /*
    **  Write attributes of parent
    */
    write_parent_attributes = 1;
    if (rename == 0)
    {
      // update export nb files: best effort mode
      export_update_files(e, -1,0);
      /*
      ** update the quota
      */
      rozofs_qt_inode_update(e->eid,quota_uid,quota_gid,1,ROZOFS_QT_DEC,plv2->attributes.s.attrs.cid);
    }
    /*
     ** remove the entry from the parent directory: best effort
     */
    del_mdirentry(plv2->dirent_root_idx_p,fdp, pfid, name, fake_fid, &fake_type,root_dirent_mask);
    /*
    ** return the parent attributes
    */
    export_recopy_extended_attributes(e,plv2,pattrs);
    status = 0;
    /*
    ** Check the case of the rename
    */
    if (rename)
    {
       mdirent_fid_name_info_t fid_name_info;
       char del_name[2048];
       uint32_t hash1,hash2;
       int root_idx;
       int len;   
       rozofs_inode_t *inode_p;
       uint64_t key_del;
       
       inode_p = (rozofs_inode_t*)fid;
       key_del= inode_p->s.file_id;
       key_del = key_del<<11;
       key_del |= inode_p->s.idx;
       
       /*
       ** append the delete pending prefix to the object name
       */
       export_build_deleted_name(del_name,&key_del,name);
       /*
       ** update the bit in the root_idx bitmap of the parent directory
       */
       hash1 = filename_uuid_hash_fnv(0, del_name,pfid, &hash2, &len);    
       lv2->attributes.s.hash1 = hash1;
       lv2->attributes.s.hash2 = hash2;
       root_idx = dirent_get_root_idx(plv2->attributes.s.attrs.children,hash1);
       export_dir_update_root_idx_bitmap(plv2->dirent_root_idx_p,root_idx,1);
       if (export_dir_flush_root_idx_bitmap(e,pfid,plv2->dirent_root_idx_p) < 0)
       {
	  errno = EPROTO; 
	  goto out;
       }
       /*
       ** assert the delete pending bit in the i-node
       */
       exp_metadata_inode_del_assert(fid);
       memcpy(lv2->attributes.s.attrs.fid,fid,sizeof(fid_t));
       /*
       **  update the parent
       ** add the new child to the parent
       */
       if (put_mdirentry(plv2->dirent_root_idx_p,fdp, pfid, del_name, 
                	 fid, fake_type,&fid_name_info,
			 plv2->attributes.s.attrs.children,
			 &root_dirent_mask) != 0) {
           goto out;
       } 
       /*
       ** update name in the inode structure
       */
       exp_store_fname_in_inode(&lv2->attributes.s.fname,del_name,&fid_name_info);           
       /*
       ** Update the atime of the deleted file to the trash time. To be used by the periodic trash flush process.
       **  Write attributes of deleted file (pending)
       */
       lv2->attributes.s.attrs.atime =  time(NULL);
       export_lv2_write_attributes(e->trk_tb_p,lv2,0/* No sync */) ;
       /*
       ** update the parent attributes
       */
       write_parent_attributes = 1;
       goto out;   
    }

out:
    /*
    ** check if parent root idx bitmap must be updated
    */
    if (plv2 != NULL) 
    {
       if (write_parent_attributes) 
       {
	 /*
	 ** Update the directory statistics
	 */
#ifdef ROZOFS_DIR_STATS
          export_dir_update_time(plv2);
#endif
          export_lv2_write_attributes(e->trk_tb_p,plv2,0/* No sync */);
       }
       export_dir_flush_root_idx_bitmap(e,pfid,plv2->dirent_root_idx_p);
    }
    
    if(fdp != -1) close(fdp);
    STOP_PROFILING(export_rmdir);

    return status;
}
/*
**______________________________________________________________________________
*/
/** create a symlink
 *
 * @param e: the export managing the file
 * @param link: target name
 * @param pfid: the id of the parent
 * @param name: the name of the file to link.
 * @param[out] attrs:  to fill (child attributes used by upper level functions)
 * @param[out] pattrs:  to fill (parent attributes)
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_symlink(export_t * e, char *link, fid_t pfid, char *name,
        struct inode_internal_t * attrs,struct inode_internal_t *pattrs, 
	uint32_t uid, uint32_t gid) {

    int status = -1;
    lv2_entry_t *plv2=NULL;
    lv2_entry_t *lv2=NULL;
    fid_t node_fid;
    int xerrno = errno;
    int fdp = -1;
    ext_mattr_t ext_attrs;
    uint32_t pslice;
    int inode_allocated = 0;
    mdirent_fid_name_info_t fid_name_info;
    rozofs_inode_t *fake_inode;
    exp_trck_top_header_t *p = NULL;
    int ret;
    int root_dirent_mask = 0;
        
    START_PROFILING(export_symlink);
    /*
    ** reject the service if the directory has the delete pending bit asserted
    */
    if (exp_metadata_inode_is_del_pending(pfid))
    {
       errno = EROFS;
       goto error;
    }
    // get the lv2 parent
    if (!(plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, pfid)))
        goto error;
        
    /*
    ** Check meta data device left size
    */
    if (export_metadata_device_full(e,tic)) {
      errno = ENOSPC;
      goto error;      
    }
            
     /*
     ** load the root_idx bitmap of the parent
     */
     export_dir_load_root_idx_bitmap(e,pfid,plv2);

    /*
    ** set global variables associated with the export
    */
    fdp = export_open_parent_directory(e,pfid);
    if (get_mdirentry(plv2->dirent_root_idx_p,fdp, pfid, name, node_fid, &attrs->attrs.mode,&root_dirent_mask) == 0) {
        errno = EEXIST;
        goto error;
    }
    /*
     ** nothing has been found, need to check the read only flag:
     ** that flag is asserted if some parts of dirent files are unreadable 
     */
    if (DIRENT_ROOT_IS_READ_ONLY()) {
        xerrno = EIO;
        goto error_read_only;
    }
    
    /*
    ** Check parent GID bit
    */
    if (plv2->attributes.s.attrs.mode & S_ISGID) {
      gid   = plv2->attributes.s.attrs.gid;
    }  

    /*
    **  check user, group and share quota enforcement
    */
    ret = rozofs_qt_check_quota(e->eid,uid,gid,plv2->attributes.s.attrs.cid);
    if (ret < 0)
    {
      errno = ENOSPC;
      goto error;
    }         
            
    /*
    ** get the slice of the parent
    */
    exp_trck_get_slice(pfid,&pslice);
    /*
    ** copy the parent fid and the name of the regular file
    */
    memset(&ext_attrs,0x00,sizeof(ext_attrs));
    memcpy(&ext_attrs.s.pfid,pfid,sizeof(fid_t));

    ext_attrs.s.attrs.cid = 0;
    memset(ext_attrs.s.attrs.sids, 0, ROZOFS_SAFE_MAX * sizeof (sid_t));
    ext_attrs.s.attrs.mode = S_IFLNK | S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IWGRP |
            S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH;
    rozofs_clear_xattr_flag(&ext_attrs.s.attrs.mode);

    ext_attrs.s.attrs.uid = uid;
    ext_attrs.s.attrs.gid = gid;
    ext_attrs.s.attrs.nlink = 1;
    if ((ext_attrs.s.attrs.ctime = ext_attrs.s.attrs.atime = ext_attrs.s.attrs.mtime = ext_attrs.s.cr8time = time(NULL)) == -1)
        goto error;
    ext_attrs.s.attrs.size = strlen(link);
    ext_attrs.s.i_extra_isize = ROZOFS_I_EXTRA_ISIZE;
    ext_attrs.s.i_state = 0;
    ext_attrs.s.i_file_acl = 0;
    ext_attrs.s.i_link_name = 0;
    /*
    ** create the inode and write the attributes on disk
    */
    if(exp_attr_create_write_cond(e->trk_tb_p,pslice,&ext_attrs,ROZOFS_REG,link,0) < 0)
        goto error;
		
    inode_allocated = 1;
    /*
    ** update the bit in the root_idx bitmap of the parent directory
    */
    uint32_t hash1,hash2;
    int root_idx;
    int len;
    
    hash1 = filename_uuid_hash_fnv(0, name,pfid, &hash2, &len);
    /*
    ** store the hash values in the i-node
    */
    ext_attrs.s.hash1 = hash1;
    ext_attrs.s.hash2 = hash2;
    root_idx = dirent_get_root_idx(plv2->attributes.s.attrs.children,hash1);
    export_dir_update_root_idx_bitmap(plv2->dirent_root_idx_p,root_idx,1);
    if (export_dir_flush_root_idx_bitmap(e,pfid,plv2->dirent_root_idx_p) < 0)
    {
       errno = EPROTO; 
       goto error;
    }

    // update the parent
    // add the new child to the parent
    if (put_mdirentry(plv2->dirent_root_idx_p,fdp, pfid, name, 
                      ext_attrs.s.attrs.fid, attrs->attrs.mode,&fid_name_info,
		      plv2->attributes.s.attrs.children,
		      &root_dirent_mask) != 0)
        goto error;
    /*
    ** store the name of the directory and write the inode on disk
    */
    exp_store_dname_in_inode(&ext_attrs.s.fname,name,&fid_name_info);
    /*
    ** flush the inode on disk
    */
    fake_inode = (rozofs_inode_t*)ext_attrs.s.attrs.fid;
    p = e->trk_tb_p->tracking_table[fake_inode->s.key];
    ret = exp_metadata_write_attributes(p,fake_inode,&ext_attrs,sizeof(ext_mattr_t), 1 /* sync */);
    if (ret < 0)
    { 
      goto error;
    }  
    plv2->attributes.s.attrs.children++;
    // update times of parent
    plv2->attributes.s.attrs.mtime = plv2->attributes.s.attrs.ctime = time(NULL);
    /*
    ** Update the directory statistics
    */
#ifdef ROZOFS_DIR_STATS
    export_dir_update_time(plv2);
#endif

    if (export_lv2_write_attributes(e->trk_tb_p,plv2,0/* No sync */) != 0)
        goto error;

    /*
    ** Put the created symbolic link in the cache
    */
    lv2 = lv2_cache_put_forced(e->lv2_cache,ext_attrs.s.attrs.fid,&ext_attrs);
    if (lv2) {
      /* 
      ** Save the symbolic link target name in the cache
      */
      lv2->symlink_target = strdup(link);
    }

    /*
    ** update the inode quota 
    */
    rozofs_qt_inode_update(e->eid,ext_attrs.s.attrs.uid,ext_attrs.s.attrs.gid,1,ROZOFS_QT_INC,plv2->attributes.s.attrs.cid);
    // update export files
    export_update_files(e, 1,0);

    status = 0;
    /*
    ** return the parent and child attributes
    */
    export_recopy_extended_attributes(e,(lv2_entry_t*)&ext_attrs,attrs);
    export_recopy_extended_attributes(e,plv2,pattrs);    
    goto out;

error:
    xerrno = errno;
    if (inode_allocated)
    {
       export_tracking_table_t *trk_tb_p;
   
        trk_tb_p = e->trk_tb_p;
        exp_attr_delete(trk_tb_p,ext_attrs.s.attrs.fid);        
    }
error_read_only:
    errno = xerrno;

out:
    /*
    ** check if parent root idx bitmap must be updated
    */
    if (plv2 != NULL) export_dir_flush_root_idx_bitmap(e,pfid,plv2->dirent_root_idx_p);
    
    if(fdp != -1) close(fdp);
    STOP_PROFILING(export_symlink);

    return status;
}
/*
**______________________________________________________________________________
*/
/** read a symbolic link
 *
 * @param e: the export managing the file
 * @param fid: file id
 * @param link: link to fill
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_readlink(export_t *e, fid_t fid, char *link) {
    int status = -1;
    lv2_entry_t *lv2 = 0;
    export_tracking_table_t *trk_tb_p = e->trk_tb_p;
    rozofs_inode_t fake_inode;
    exp_trck_top_header_t *p = NULL;
    int ret;
     
    START_PROFILING(export_readlink);

    if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid))) {
        goto out;
    }
    /**
    * read the link: the size of the link is found in the common attributes
    */
    if (lv2->attributes.s.i_link_name == 0)
    {
      errno = EINVAL;
      goto out;    
    }
    fake_inode.fid[1] = lv2->attributes.s.i_link_name;

    if (fake_inode.s.key != ROZOFS_SLNK)
    {
      errno = EINVAL;
      goto out;    
    }
    
    /*
    ** Get the symlink target name from the cache when present
    */
    if (lv2->symlink_target) {
      strcpy(link,lv2->symlink_target);
      status = 0;
      goto out;
    }
    
    p = trk_tb_p->tracking_table[fake_inode.s.key];
    if (p == NULL)
    {
      errno = EINVAL;
      goto out;    
    }  
    /*
    ** read the link from disk
    */
    ret = exp_metadata_read_attributes(p,&fake_inode,link,lv2->attributes.s.attrs.size);
    if (ret < 0)
    { 
      goto out;    
    }
    link[lv2->attributes.s.attrs.size] = 0; 
    /*
    ** Save the target name of the symbolic link in the lv2 cache 
    ** for further call to readlink
    */
    lv2->symlink_target = strdup(link);    
    status = 0;

out:
    STOP_PROFILING(export_readlink);

    return status;
}


/*
**______________________________________________________________________________
*/
/** rename (move) a file when the destination is ROZOFS_DIR_TRASH

*/
int export_rename_trash(export_t *e, fid_t pfid, char *name, fid_t npfid,
        char *newname_in, fid_t fid,
	struct inode_internal_t * attrs)
{
    int status = -1;
    lv2_entry_t *lv2_old_parent = 0;
    lv2_entry_t *lv2_to_rename = 0;
    int write_parent_attributes = 0;
    fid_t fid_to_rename;
    int root_dirent_mask_name= 0;
    uint32_t type_to_rename;
    uint32_t fake_type;
    char lv2_path[PATH_MAX];
    char lv3_path[PATH_MAX];    
    fid_t fake_fid;
    int old_parent_fdp = -1;
    int quota_uid=-1;
    int quota_gid=-1;
    
    // Get the lv2 entry of old parent
    if (!(lv2_old_parent = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, pfid))) {
        goto out;
    }
    if (exp_metadata_inode_is_del_pending(pfid))
    {
      errno=EPERM;
      goto out;
    }
    /*
    ** old parent and new parent MUST be the same
    */
    if (memcmp(pfid, npfid, sizeof (fid_t)) != 0) 
    {
      errno=EPERM;
      goto out;        
    }
    /*
    ** check if the parent directory has already an active trash
    */
    if (rozofs_has_trash(&lv2_old_parent->attributes.s.attrs.sids[0]) != 0)
    {
      errno=EPERM;
      goto out;        
    }
    /*
    ** we have the same parent and the parent has no trash
    ** We must check if the source name corresponds to an empty directory
    */
    /*
    ** load the root_idx bitmap of the old parent
    */
    export_dir_load_root_idx_bitmap(e,pfid,lv2_old_parent);
    /*
    ** get the file descriptor associated with the old parent
    */
    old_parent_fdp = export_open_parent_directory(e,pfid);
    // Check if the file/dir to rename exist
    if (get_mdirentry(lv2_old_parent->dirent_root_idx_p,old_parent_fdp, pfid, name, fid_to_rename, 
                       &type_to_rename,&root_dirent_mask_name) != 0)
        goto out;
    /*
    ** OK we got the fid of the object to rename, check if is a directory
    */
    if (!(lv2_to_rename = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid_to_rename)))
        goto out;

    if (!S_ISDIR(lv2_to_rename->attributes.s.attrs.mode))  
    {
      /*
      ** we expect that the source is a directory
      */
      errno=EPERM;
      goto out;                  
    }    
    /*
    ** check if the directory is empty
    */       
    if ((lv2_to_rename->attributes.s.hpc_reserved.dir.nb_deleted_files!=0) || (lv2_to_rename->attributes.s.attrs.children!=0))
    {
      /*
      ** not empty
      */
      errno=EPERM;
      goto out;          
    } 
    /*
    ** get the usr and group for quota management
    */
    quota_uid = lv2_to_rename->attributes.s.attrs.uid;
    quota_gid = lv2_to_rename->attributes.s.attrs.gid; 
    /*
    ** OK the directory is empty, so we can remove it
    */
    if (export_lv2_resolve_path(e, fid, lv2_path) != 0)
          goto out;
    /*
    ** once the attributes file has been removed 
    ** consider that the directory is deleted, all the remaining is best effort
    */
    sprintf(lv3_path, "%s/%s", lv2_path, MDIR_ATTRS_FNAME);
    unlink(lv3_path);    
    // remove from the cache (will be closed and freed)
    if (export_attr_thread_check_context(lv2_to_rename)==0) lv2_cache_del(e->lv2_cache, fid_to_rename);
    /*
     ** rmdir is best effort since it might possible that some dirent file with empty entries remain
     */
    rmdir(lv2_path);
    /**
    * releas the inode allocated for storing the directory attributes
    */
    if (exp_attr_delete(e->trk_tb_p,fid_to_rename) < 0)
    {
       severe("error on inode %s release : %s",name,strerror(errno));
    }
    /*
    ** attributes of the parent must be updated first otherwise we can face the situation where
    ** parent directory cannot be removed because the number of children is not 0
    */
    if (lv2_old_parent->attributes.s.attrs.children > 0) lv2_old_parent->attributes.s.attrs.children--;
    lv2_old_parent->attributes.s.attrs.nlink--;    
    /*
    ** update export nb files: best effort mode
    */
    export_update_files(e, -1,0);
    /*
    ** update the quota
    */
    rozofs_qt_inode_update(e->eid,quota_uid,quota_gid,1,ROZOFS_QT_DEC,lv2_old_parent->attributes.s.attrs.cid);
    /*
    ** remove the entry from the parent directory: best effort
    */
    del_mdirentry(lv2_old_parent->dirent_root_idx_p,old_parent_fdp, pfid, name, fake_fid, &fake_type,root_dirent_mask_name);
    /*
    ** Assert the trash flag on the parent directory
    */
    rozofs_set_trash_sid0(&lv2_old_parent->attributes.s.attrs.sids[0]);
    write_parent_attributes =1;
    /*
    ** provide the attributes of the parent directory and assert the deleted by on the fid;
    */
    export_recopy_extended_attributes(e,lv2_old_parent,attrs);    
    exp_metadata_inode_del_assert(attrs->attrs.fid);
    rozofs_inode_set_trash(attrs->attrs.fid);
    
    status = 0;

out:
    /*
    ** check if parent root idx bitmap must be updated
    */
    if (lv2_old_parent != NULL) 
    {
       if (write_parent_attributes) export_lv2_write_attributes(e->trk_tb_p,lv2_old_parent,0/* No sync */);
       export_dir_flush_root_idx_bitmap(e,pfid,lv2_old_parent->dirent_root_idx_p);
    }
    
    if(old_parent_fdp != -1) close(old_parent_fdp);  

    return status;

}
/*
**______________________________________________________________________________
*/
/** rename (move) a file
 *
 * @param e: the export managing the file
 * @param pfid: parent file id
 * @param name: file name
 * @param npfid: target parent file id
 * @param newname: target file name
 * @param fid: file id
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_rename(export_t *e, fid_t pfid, char *name, fid_t npfid,
        char *newname_in, fid_t fid,
	struct inode_internal_t * attrs) {
    int status = -1;
    lv2_entry_t *lv2_old_parent = 0;
    lv2_entry_t *lv2_new_parent = 0;
    lv2_entry_t *lv2_to_rename = 0;
    lv2_entry_t *lv2_to_replace = 0;
    fid_t fid_to_rename;
    uint32_t type_to_rename;
    fid_t fid_to_replace;
    uint32_t type_to_replace;
    int old_parent_fdp = -1;
    int new_parent_fdp = -1;
    int to_replace_fdp = -1;
    mdirent_fid_name_info_t fid_name_info;
    int root_dirent_mask_name= 0;
    int root_dirent_mask_newname =0;
    int xerrno = 0;
    int deleted_object = 0;
    int source_is_trash = 0;
    int restore_from_trash_with_same_name = 0;
    char *newname = newname_in;

    START_PROFILING(export_rename);
    
    /*
    ** check the case of a rename towards the trash
    */    
    if (strcmp(newname,ROZOFS_DIR_TRASH) == 0)
    {
        status = export_rename_trash(e,pfid,name,npfid,newname_in,fid,attrs);
        goto out;            
    }

    // Get the lv2 entry of old parent
    if (!(lv2_old_parent = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, pfid))) {
        goto out;
    }
    // Get the lv2 entry of newparent
    if (!(lv2_new_parent = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, npfid)))
        goto out;

    /*
    ** The parent could be either @rozofs-trash@ or a deleted directory, so we have to evaluate the fid within the context
    ** when the parent fid is not the trash to figure-out if source is trash
    */
    if (exp_metadata_inode_is_del_pending(lv2_old_parent->attributes.s.attrs.fid) || exp_metadata_inode_is_del_pending(pfid))
    {
      source_is_trash = 1;
    }
    /*
    ** clear the delete bit and set the key to ROZOFS_DIR since it is the trash
    ** it is needed  to compare the old_parent with the new_parent
    */
    exp_metadata_inode_del_deassert(pfid);
    rozofs_inode_set_dir(pfid);

    // Verify that the old parent is a directory
    if (!S_ISDIR(lv2_old_parent->attributes.s.attrs.mode)) {
        errno = ENOTDIR;
        goto out;
    }
    /*
    ** check if the new parent is a deleted directory: if it is the case
    ** reject the service: it can be either a deleted directory or @rozofs-trash@
    */
    if (exp_metadata_inode_is_del_pending(lv2_new_parent->attributes.s.attrs.fid) || exp_metadata_inode_is_del_pending(npfid))
    {
        errno = EPERM;
        goto out;    
    }
    /*
    ** check that the source name does not match with the rozofs-del prefix
    */
    if (strcmp(name,ROZOFS_DIR_TRASH) == 0)
    {
        errno = EPERM;
        goto out;            
    }
    /*
    ** check that the destination name does not match with the rozofs-del prefix
    */
    if (strcmp(newname,ROZOFS_DIR_TRASH) == 0)
    {
        errno = EROFS;
        goto out;            
    }
    if (source_is_trash)
    {
       /*
       ** do not care about the fact that parent are the same: we strip the prefix appended
       ** by rozofs when both source name and destination are the same
       */
       /*if (memcmp(pfid,npfid,sizeof(fid_t)) == 0) */
       {
          /*
	  ** check the filenames
	  */
	  restore_from_trash_with_same_name = 1;
	  if (strcmp(name,newname)== 0)
	  {
	     char *p;
	     int count;
	     /*
	     ** same name: it is an untrash: strip the rozofs-del pattern
	     */
	     count = 0;
	     p=newname;
	     while (*p != 0)
	     {
	        if (*p == '@') 
		{
		  count++;
		  if (count == 3) {
		     p++;
		     break;
		   }
		}
		p++;	     
	     }
	     if (count !=3) 
	     {
	       errno = EROFS;
	       goto out;
	     }
	     newname = p;	  
	  }       
       }    
    }
    /*
    ** load the root_idx bitmap of the old parent
    */
    export_dir_load_root_idx_bitmap(e,pfid,lv2_old_parent);
    /*
    ** get the file descriptor associated with the old parent
    */
    old_parent_fdp = export_open_parent_directory(e,pfid);
    // Check if the file/dir to rename exist
    if (get_mdirentry(lv2_old_parent->dirent_root_idx_p,old_parent_fdp, pfid, name, fid_to_rename, 
                       &type_to_rename,&root_dirent_mask_name) != 0)
        goto out;

    // Get the lv2 entry of file/dir to rename
    if (!(lv2_to_rename = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid_to_rename)))
        goto out;
    /*
    ** open the fid to rename if it is a directory
    */
    if (S_ISDIR(lv2_to_rename->attributes.s.attrs.mode)) 
    {   
      /*
      ** load the root_idx bitmap of the fid to rename
      */
      export_dir_load_root_idx_bitmap(e,fid_to_rename,lv2_to_rename);
      export_open_parent_directory(e,fid_to_rename);
    }  

    // Verify that the new parent is a directory
    if (!S_ISDIR(lv2_new_parent->attributes.s.attrs.mode)) {
        errno = ENOTDIR;
        goto out;
    }
    /*
    ** load the root_idx bitmap of the old parent
    */
    export_dir_load_root_idx_bitmap(e,npfid,lv2_new_parent);

    memset(fid, 0, sizeof (fid_t));
    /*
    ** open the directory
    */
    new_parent_fdp = export_open_parent_directory(e,npfid);
    /*
    ** Check if the destination already exist
    **  In the case of a rename where the source is a deleted file and when both source and destination are the
    **  same the service fails if the destination already exists
    */
    if (get_mdirentry(lv2_new_parent->dirent_root_idx_p,new_parent_fdp, npfid, newname, 
                      fid_to_replace, &type_to_replace,&root_dirent_mask_newname) == 0) {
	/*
	** check if the source is a file/directory of the trash and if the user want the same name as newname
	** In that case we reject the rename
	*/
	if (restore_from_trash_with_same_name)
	{
	  errno = EPERM; /** EEXIST could be fine with Linux but seems to not working with SAMBA */
	  goto out;
	}

        // We must delete the old entry

        // Get mattrs of entry to delete
        if (!(lv2_to_replace = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid_to_replace)))
            goto out;
	/*
	** load the root_idx bitmap of the old parent
	*/
	export_dir_load_root_idx_bitmap(e,fid_to_replace,lv2_to_replace);
	
        // The entry (to replace) is an existing directory
        if (S_ISDIR(lv2_to_replace->attributes.s.attrs.mode)) {

            // The entry to rename must be a directory
            if (!S_ISDIR(lv2_to_rename->attributes.s.attrs.mode)) {
                errno = EISDIR;
                goto out;
            }

            // The entry to replace must be a empty directory
            if (lv2_to_replace->attributes.s.attrs.children != 0) {
                errno = ENOTEMPTY;
                goto out;
            }
            // Update parent directory
            lv2_new_parent->attributes.s.attrs.nlink--;
            lv2_new_parent->attributes.s.attrs.children--;

            // We'll write attributes of parents after

            // Update export files
            if (export_update_files(e, -1,0) != 0)
                goto out;

            char lv2_path[PATH_MAX];
            char lv3_path[PATH_MAX];

            if (export_lv2_resolve_path(e, lv2_to_replace->attributes.s.attrs.fid, lv2_path) != 0)
                goto out;

            sprintf(lv3_path, "%s/%s", lv2_path, MDIR_ATTRS_FNAME);

            if (unlink(lv3_path) != 0)
                goto out;

            if (rmdir(lv2_path) != 0)
                goto out;

            // Remove the dir to replace from the cache (will be closed and freed)
            if (export_attr_thread_check_context(lv2_to_replace)==0)lv2_cache_del(e->lv2_cache, fid_to_replace);
	    lv2_to_replace = 0;

            // Return the fid of deleted directory
            memcpy(fid, fid_to_replace, sizeof (fid_t));

        } else {
	    /*
	    **________________________________________________
              THE ENTRY (TO REPLACE) IS AN EXISTING FILE
	    **________________________________________________
	    */
            if (S_ISREG(lv2_to_replace->attributes.s.attrs.mode) || S_ISLNK(lv2_to_replace->attributes.s.attrs.mode)) 
            {
	       fid_t fake_fid;
	       struct inode_internal_t  fake_pattr;
	       
	       /*
	       ** use the regular export_unlink function since it addresses the case of the trash and mover
	       ** However, we that call, the parent attributes will be written twice (in async mode)
	       */
               export_unlink(e,npfid, newname,fake_fid,&fake_pattr);	    
	    }
            lv2_to_replace = 0;
        }
    } else {
        /*
         ** nothing has been found, need to check the read only flag:
         ** that flag is asserted if some parts of dirent files are unreadable 
         */
        if (DIRENT_ROOT_IS_READ_ONLY()) {
            errno = EIO;
            goto out;
        }
    }

    /*
    **  Put the mdirentry: need to clear the delete pending bit before insertion in dentry
    */
    deleted_object = exp_metadata_inode_is_del_pending(lv2_to_rename->attributes.s.attrs.fid);
    exp_metadata_inode_del_deassert(lv2_to_rename->attributes.s.attrs.fid);
    
    if (put_mdirentry(lv2_new_parent->dirent_root_idx_p,new_parent_fdp, npfid, newname, 
                      lv2_to_rename->attributes.s.attrs.fid,
		      lv2_to_rename->attributes.s.attrs.mode,&fid_name_info,
		      lv2_new_parent->attributes.s.attrs.children,
		      &root_dirent_mask_newname) != 0) {
        goto out;
    }

    // Delete the mdirentry
    if (del_mdirentry(lv2_old_parent->dirent_root_idx_p,
                       old_parent_fdp, 
		       pfid, name, fid_to_rename, &type_to_rename,root_dirent_mask_name) != 0)
        goto out;

    if (memcmp(pfid, npfid, sizeof (fid_t)) != 0) {       
        if (deleted_object)
	{
          lv2_new_parent->attributes.s.attrs.children++;
          if (S_ISDIR(lv2_to_rename->attributes.s.attrs.mode)) {	
	    lv2_new_parent->attributes.s.attrs.nlink++;
	  }
	  lv2_old_parent->attributes.s.hpc_reserved.dir.nb_deleted_files--;		  
	}
	else
	{	
	  lv2_new_parent->attributes.s.attrs.children++;
	  lv2_old_parent->attributes.s.attrs.children--;
	  if (S_ISDIR(lv2_to_rename->attributes.s.attrs.mode)) {
	    lv2_new_parent->attributes.s.attrs.nlink++;
	    lv2_old_parent->attributes.s.attrs.nlink--;
	  }	
	}
        lv2_new_parent->attributes.s.attrs.mtime = lv2_new_parent->attributes.s.attrs.ctime = time(NULL);
        lv2_old_parent->attributes.s.attrs.mtime = lv2_old_parent->attributes.s.attrs.ctime = time(NULL);

#ifdef ROZOFS_DIR_STATS
	 /*
	 ** Update the directory statistics: old & new parent
	 */
           if (S_ISREG(lv2_to_rename->attributes.s.attrs.mode))
	   {
	     export_dir_adjust_child_size(lv2_new_parent,lv2_to_rename->attributes.s.attrs.size,1,ROZOFS_BSIZE_BYTES(e->bsize));
	     export_dir_adjust_child_size(lv2_old_parent,lv2_to_rename->attributes.s.attrs.size,0,ROZOFS_BSIZE_BYTES(e->bsize));
	     if (lv2_old_parent->attributes.s.attrs.cid != lv2_new_parent->attributes.s.attrs.cid)
	     {
	       /*
	       ** case of a regular file, need to update the share_id in the i-node as well as the share quota
	       */
	       rozofs_qt_inode_update(e->eid,-1,-1,1,ROZOFS_QT_DEC,lv2_old_parent->attributes.s.attrs.cid);
               rozofs_qt_block_update(e->eid,-1,-1,lv2_to_rename->attributes.s.attrs.size,ROZOFS_QT_DEC,lv2_old_parent->attributes.s.attrs.cid);
	       
	       rozofs_qt_inode_update(e->eid,-1,-1,1,ROZOFS_QT_INC,lv2_new_parent->attributes.s.attrs.cid);
               rozofs_qt_block_update(e->eid,-1,-1,lv2_to_rename->attributes.s.attrs.size,ROZOFS_QT_INC,lv2_new_parent->attributes.s.attrs.cid);
	       
	       lv2_to_rename->attributes.s.hpc_reserved.reg.share_id = lv2_new_parent->attributes.s.attrs.cid;
	     }	  	   
	   }
           export_dir_update_time(lv2_new_parent);
           export_dir_update_time(lv2_old_parent);
#endif

        if (export_lv2_write_attributes(e->trk_tb_p,lv2_new_parent,0/* No sync */) != 0)
            goto out;

        if (export_lv2_write_attributes(e->trk_tb_p,lv2_old_parent,0/* No sync */) != 0)
            goto out;
    } else {
	/*
	** we have the same parent directory: check if the inode to move was a deleted inode:
	** if it is the case we need to update the count of children and the nlink (if directory)
	** of the parent directory.
	*/
	if (deleted_object)
	{
	  lv2_old_parent->attributes.s.hpc_reserved.dir.nb_deleted_files--;
          lv2_new_parent->attributes.s.attrs.children++;
          if (S_ISDIR(lv2_to_rename->attributes.s.attrs.mode)) {
              lv2_new_parent->attributes.s.attrs.nlink++;
          }              
	}
#ifdef ROZOFS_DIR_STATS
	 /*
	 ** Update the directory statistics:  new parent
	 */
         export_dir_update_time(lv2_new_parent);
#endif
        lv2_new_parent->attributes.s.attrs.mtime = lv2_new_parent->attributes.s.attrs.ctime = time(NULL);

        if (export_lv2_write_attributes(e->trk_tb_p,lv2_new_parent,0/* No sync */) != 0)
            goto out;
    }

    // Update ctime of renamed file/directory
    lv2_to_rename->attributes.s.attrs.ctime = time(NULL);
    {
       /*
       ** udpate the hash values and the new name in the inode attributes
       */
      uint32_t hash1,hash2;
      int len;
      hash1 = filename_uuid_hash_fnv(0, newname,npfid, &hash2, &len);
      lv2_to_rename->attributes.s.hash1 = hash1;
      lv2_to_rename->attributes.s.hash2 = hash2;
      exp_store_fname_in_inode(&lv2_to_rename->attributes.s.fname,newname,&fid_name_info);           
    
    }
    /*
    ** update the parent fid of the inode that has been renamed
    */
    memcpy(&lv2_to_rename->attributes.s.pfid,npfid,sizeof(fid_t));
        
    // Write attributes of renamed file
    if (export_lv2_write_attributes(e->trk_tb_p,lv2_to_rename, 1/* sync */) != 0)
        goto out;

    export_recopy_extended_attributes(e,lv2_to_rename,attrs);    
    status = 0;

out:
    xerrno = errno;
    if( old_parent_fdp !=1) close(old_parent_fdp);
    if( new_parent_fdp !=1) close(new_parent_fdp);
    if( to_replace_fdp !=1) close(to_replace_fdp);
    if(lv2_old_parent!=0 ) export_dir_flush_root_idx_bitmap(e,pfid,lv2_old_parent->dirent_root_idx_p);
    if(lv2_new_parent!=0 ) export_dir_flush_root_idx_bitmap(e,npfid,lv2_new_parent->dirent_root_idx_p);
    if(lv2_to_rename!=0 )  export_dir_flush_root_idx_bitmap(e,fid_to_rename,lv2_to_rename->dirent_root_idx_p);
    if(lv2_to_replace!=0 )  export_dir_flush_root_idx_bitmap(e,fid_to_replace,lv2_to_replace->dirent_root_idx_p);
    errno = xerrno;

    STOP_PROFILING(export_rename);

    return status;
}

/*
**______________________________________________________________________________
*/
int64_t export_read(export_t * e, fid_t fid, uint64_t offset, uint32_t len,
        uint64_t * first_blk, uint32_t * nb_blks) {
    lv2_entry_t *lv2 = NULL;
    int64_t length = -1;
    uint64_t i_first_blk = 0;
    uint64_t i_last_blk = 0;
    uint32_t i_nb_blks = 0;

    START_PROFILING(export_read);

    // Get the lv2 entry
    if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid))) {
        goto error;
    }

    // EOF ?
    if (offset > lv2->attributes.s.attrs.size) {
        errno = 0;
        goto error;
    }

    // Length to read
    length = (offset + len < lv2->attributes.s.attrs.size ? len : lv2->attributes.s.attrs.size - offset);
    // Nb. of the first block to read
    i_first_blk = offset / ROZOFS_BSIZE_BYTES(e->bsize);
    // Nb. of the last block to read
    i_last_blk = (offset + length) / ROZOFS_BSIZE_BYTES(e->bsize) + ((offset + length) % ROZOFS_BSIZE_BYTES(e->bsize) == 0 ? -1 : 0);
    // Nb. of blocks to read
    i_nb_blks = (i_last_blk - i_first_blk) + 1;

    *first_blk = i_first_blk;
    *nb_blks = i_nb_blks;

    // Managed access time
    if ((lv2->attributes.s.attrs.atime = time(NULL)) == -1)
        goto error;

    // Write attributes of file
    if (export_lv2_write_attributes(e->trk_tb_p,lv2,0/* No sync */) != 0)
        goto error;

    // Return the length that can be read
    goto out;

error:
    length = -1;
out:
    STOP_PROFILING(export_read);

    return length;
}

/* not used anymore
int64_t export_write(export_t *e, fid_t fid, uint64_t off, uint32_t len) {
    lv2_entry_t *lv2;

    if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid))) {
        return -1;
    }

    if (off + len > lv2->attributes.s.attrs.size) {
        // Don't skip intermediate computation to keep ceil rounded
        uint64_t nbold = (lv2->attributes.s.attrs.size + ROZOFS_BSIZE - 1) / ROZOFS_BSIZE;
        uint64_t nbnew = (off + len + ROZOFS_BSIZE - 1) / ROZOFS_BSIZE;

        if (export_update_blocks(e, nbnew - nbold) != 0)
            return -1;

        lv2->attributes.s.attrs.size = off + len;
    }

    lv2->attributes.s.attrs.mtime = lv2->attributes.s.attrs.ctime = time(NULL);

    if (export_lv2_write_attributes(e->trk_tb_p,lv2) != 0)
        return -1;

    return len;
}*/
/*
**______________________________________________________________________________
*/
/**  update the file size, mtime and ctime
 *
 * dist is the same for all blocks
 *
 * @param e: the export managing the file
 * @param fid: id of the file to read
 * @param bid: first block address (from the start of the file)
 * @param n: number of blocks
 * @param d: distribution to set
 * @param off: offset to write from
 * @param len: length written
 * @param site_number: siet number for geo-replication
 * @param geo_wr_start: write start offset
 * @param geo_wr_end: write end offset
 * @param[out] attrs: updated attributes of the file
 *
 * @return: the written length on success or -1 otherwise (errno is set)
 */
int64_t export_write_block(export_t *e, fid_t fid, uint64_t bid, uint32_t n,
                           dist_t d, uint64_t off, uint32_t len,
			   uint32_t site_number,uint64_t geo_wr_start,uint64_t geo_wr_end,
	                   struct inode_internal_t *attrs) {
    int64_t length = -1;
    lv2_entry_t *lv2 = NULL;
    int          sync = 0;
   lv2_entry_t *plv2 = NULL;
    uint16_t share = 0;
       
    START_PROFILING(export_write_block);

    // Get the lv2 entry
    if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid)))
        goto out;
#ifdef ROZOFS_DIR_STATS
    /*
    ** attempt to get the parent attribute to address the case of the asynchronous fast replication
    */
    plv2 = export_dir_get_parent(e,lv2);
#endif
    /*
    ** check the case of the file mover
    */
    {
       rozofs_inode_t *inode_p = (rozofs_inode_t*)fid;
       if (inode_p->s.key == ROZOFS_REG_D_MOVER)
       {
          /*
	  ** write block should be ignored
	  */
	  length = len;
          export_recopy_extended_attributes(e,lv2,attrs);    
          memcpy(attrs->attrs.fid,fid,sizeof(fid_t));
	  goto out;	  
       }
    }

    // Update size of file
    if (off + len > lv2->attributes.s.attrs.size) {
        // Don't skip intermediate computation to keep ceil rounded
        uint64_t nbold = (lv2->attributes.s.attrs.size + ROZOFS_BSIZE_BYTES(e->bsize) - 1) / ROZOFS_BSIZE_BYTES(e->bsize);
        uint64_t nbnew = (off + len + ROZOFS_BSIZE_BYTES(e->bsize) - 1) / ROZOFS_BSIZE_BYTES(e->bsize);
	/*
	** Get the parent i-node
	*/
	
	if (plv2 == NULL)
	{
	   plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, lv2->attributes.s.pfid); 
	}
	if (plv2!=NULL) share= plv2->attributes.s.attrs.cid;
	/*
	** update user and group quota
	*/
	rozofs_qt_block_update(e->eid,lv2->attributes.s.attrs.uid,lv2->attributes.s.attrs.gid,
	                       (off + len - lv2->attributes.s.attrs.size),ROZOFS_QT_INC,share);

#ifdef ROZOFS_DIR_STATS
	  /*
	  ** adjust the directory statistics
	  */	  
	  if (plv2) export_dir_adjust_child_size(plv2,(nbnew-nbold)*ROZOFS_BSIZE_BYTES(e->bsize),1,ROZOFS_BSIZE_BYTES(e->bsize));
#endif
        if (export_update_blocks(e, nbnew, nbold,lv2->attributes.s.attrs.children) != 0)
            goto out;

        lv2->attributes.s.attrs.size = off + len;
	sync = 1;
    }
    // Update mtime and ctime
    lv2->attributes.s.attrs.mtime = lv2->attributes.s.attrs.ctime = time(NULL);
    /*
    ** check the case of the thin provisioning
    */
    if (e->thin)
    {
      uint32_t nb_blocks;
      int dir;
      int retcode;
      retcode = expthin_check_entry(e,lv2,1,&nb_blocks,&dir);
      if (retcode == 1)
      {
         retcode = expthin_update_blocks(e,nb_blocks,dir);
	 if (retcode != 0)
	 {
	   /*
	   ** no quota left! (errno is asserted by expthin_update_blocks())
	   ** note: the number
	   ** of blocks might not reflect the effective number of blocks
	   ** this might be updated on the next lookup of the file.
	   ** The inode is re_written on disk to reflect the exact number of blocks
	   ** that are used. If we do not do it, we need to have a next write to see
	   ** them recomputed by a thin provisioning thread.
	   */
	   export_attr_thread_submit(lv2,e->trk_tb_p, sync);
	   goto out;
	 }
      }
    }
#ifdef ROZOFS_DIR_STATS
    /*
    ** adjust the directory statistics
    */
    if(plv2) 
    {
      export_dir_async_write(e,plv2);
    }
#endif
    /*
    ** write inode attributes on disk
    */
    export_attr_thread_submit(lv2,e->trk_tb_p, sync);
    /*
    ** return the parent attributes
    */
    export_recopy_extended_attributes(e,lv2,attrs);       
    length = len;
    if (e->volume->georep) 
    {
      /*
      ** update the geo replication
      */
      geo_rep_insert_fid(e->geo_replication_tb[site_number],
                	 fid,geo_wr_start,geo_wr_end,
			 e->layout,
			 lv2->attributes.s.attrs.cid,
			 lv2->attributes.s.attrs.sids);
    }
out:
    STOP_PROFILING(export_write_block);

    return length;
}
/*
**______________________________________________________________________________
*/
/** read a directory
 *
 * @param e: the export managing the file
 * @param fid: the id of the directory
 * @param children: pointer to pointer where the first children we will stored
 * @param cookie: index mdirentries where we must begin to list the mdirentries
 * @param eof: pointer that indicates if we list all the entries or not
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_readdir(export_t * e, fid_t fid, uint64_t * cookie,
        child_t ** children, uint8_t * eof) {
    int status = -1;
    lv2_entry_t *parent = NULL;
    int fdp = -1;
    child_t ** iterator;
    fid_t      null_fid = {0};
    
    iterator = children;
        
    START_PROFILING(export_readdir);

    // Get the lv2 inode
    if (!(parent = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid))) {
        severe("export_readdir failed: %s", strerror(errno));
        goto out;
    }
    /*
    ** load the root_idx bitmap of the old parent
    */
    export_dir_load_root_idx_bitmap(e,fid,parent);
    
    // Verify that the target is a directory
    if (!S_ISDIR(parent->attributes.s.attrs.mode)) {
        severe("export_readdir failed: %s", strerror(errno));
        errno = ENOTDIR;
        goto out;
    }

    /*
    ** check if we start from the beginning because in that case we must provide
    ** the fid for "." and ".."
    */
    if (*cookie == 0) 
    {
            *iterator = xmalloc(sizeof (child_t));
            memset(*iterator, 0, sizeof (child_t));
            memcpy((*iterator)->fid, parent->attributes.s.attrs.fid, sizeof (fid_t)); 
	    (*iterator)->name = strdup(".");
            // Go to next entry
            iterator = &(*iterator)->next;

            *iterator = xmalloc(sizeof (child_t));
            memset(*iterator, 0, sizeof (child_t));
	    if (memcmp(parent->attributes.s.pfid,null_fid,sizeof(fid_t))==0) {
              memcpy((*iterator)->fid, parent->attributes.s.attrs.fid, sizeof (fid_t));	      
	    }
	    else {
              memcpy((*iterator)->fid, parent->attributes.s.pfid, sizeof (fid_t));
	    }     
	    (*iterator)->name = strdup("..");
            // Go to next entry
            iterator = &(*iterator)->next;
    }
    /*
    ** set global variables associated with the export
    */
    fdp = export_open_parent_directory(e,fid);
    if (list_mdirentries(parent->dirent_root_idx_p,fdp, fid, iterator, cookie, eof) != 0) {
        goto out;
    }
    // Access time of the directory is not changed any more on readdir    
    // Update atime of parent
    //parent->attributes.atime = time(NULL);
    //if (export_lv2_write_attributes(e->trk_tb_p,parent) != 0)
    //    goto out;

    status = 0;
out:
    if (parent != NULL) export_dir_flush_root_idx_bitmap(e,fid,parent->dirent_root_idx_p);

    if (fdp != -1) close(fdp);
    STOP_PROFILING(export_readdir);

    return status;
}

/*
**______________________________________________________________________________
*/
/** read a directory (version 2)
 *
 * @param e: the export managing the file
 * @param fid: the id of the directory
 * @param buf_readdir: pointer to pointer where the first children we will stored
 * @param cookie: index mdirentries where we must begin to list the mdirentries
 * @param eof: pointer that indicates if we list all the entries or not
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_readdir2(export_t * e, fid_t fid, uint64_t * cookie,
        char *buf_readdir, uint8_t * eof) {
    int status = -1;
    lv2_entry_t *parent = NULL;
    int fdp = -1;
    int readdir_from_start;
    
    /*
    ** When cookie is zero it a readdir from the start
    ** else it is a readdir continuation
    */
    if (*cookie == 0) {
      readdir_from_start = 1;
    }  
    else {
      readdir_from_start = 0;
    }  
        
    START_PROFILING(export_readdir);

    // Get the lv2 inode
    if (!(parent = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid))) {
        severe("export_readdir failed: %s", strerror(errno));
        goto out;
    }
    /*
    ** load the root_idx bitmap of the old parent
    */
    export_dir_load_root_idx_bitmap(e,fid,parent);
    
    // Verify that the target is a directory
    if (!S_ISDIR(parent->attributes.s.attrs.mode)) {
        severe("export_readdir failed: %s", strerror(errno));
        errno = ENOTDIR;
        goto out;
    }

    /*
    ** set global variables associated with the export
    */
    fdp = export_open_parent_directory(e,fid);
    status =list_mdirentries2(parent->dirent_root_idx_p,fdp, fid, buf_readdir, cookie, eof,&parent->attributes);

    /*
    ** The directory is empty
    ** It contains only one 32 bytes entry for "." and one 32 bytes entry for ".."
    */
    if ((readdir_from_start) && (status == (2*32))) {

     /*
     ** Check if it is a non-deleted directory or a deleted directory
     */
     if (rozofs_inode_is_trash(fid) == 0)
     {
	/*
	** Active directory  The number of chilrden should be zero
	*/
	if (parent->attributes.s.attrs.children != 0) {
          char fidstring[256];
          fid2string(fid,fidstring);
          exportd_nb_directory_bad_children_count++;
          warning("(%llu) Children count should be 0 instead of %d for %s", 
                  (long long unsigned int)exportd_nb_directory_bad_children_count,
                  parent->attributes.s.attrs.children, 
                  fidstring);
          parent->attributes.s.attrs.children = 0;
	}
      }
      else
      {
         /*
	 ** Deleted directory
	 */
	 if (parent->attributes.s.hpc_reserved.dir.nb_deleted_files != 0) {
          char fidstring[256];
          fid2string(fid,fidstring);
          exportd_nb_directory_bad_children_count++;
          warning("(%llu) Children count should be 0 instead of %d for %s", 
                  (long long unsigned int)exportd_nb_directory_bad_children_count,
                  parent->attributes.s.attrs.children, 
                  fidstring);
          parent->attributes.s.hpc_reserved.dir.nb_deleted_files = 0;
	}      
      }
    }
out:
    if (parent != NULL) export_dir_flush_root_idx_bitmap(e,fid,parent->dirent_root_idx_p);

    if (fdp != -1) close(fdp);
    STOP_PROFILING(export_readdir);
    return status;
}
/*
**______________________________________________________________________________
*/
/** Display RozoFS special xattribute 
 *
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */


#define DISPLAY_ATTR_TITLE(name) {\
  p += rozofs_string_padded_append(p,8,rozofs_left_alignment,name); \
  *p++ = ':';\
  *p++ = ' ';\
}

#define DISPLAY_ATTR_ULONG(name,val) {\
  DISPLAY_ATTR_TITLE(name); \
  p += rozofs_u64_append(p,val); \
  p += rozofs_eol(p);\
}

#define DISPLAY_ATTR_INT(name,val) {\
  DISPLAY_ATTR_TITLE(name); \
  p += rozofs_i32_append(p,val); \
  p += rozofs_eol(p);\
}

#define DISPLAY_ATTR_UINT(name,val) {\
  DISPLAY_ATTR_TITLE(name); \
  p += rozofs_u32_append(p,val); \
  p += rozofs_eol(p);\
}
#define DISPLAY_ATTR_2INT(name,val1,val2) {\
  DISPLAY_ATTR_TITLE(name); \
  p += rozofs_i32_append(p,val1); \
  *p++='/'; \
  p += rozofs_i32_append(p,val2); \
  p += rozofs_eol(p);\
}  
#define DISPLAY_ATTR_2UINT(name,val1,val2) {\
  DISPLAY_ATTR_TITLE(name); \
  p += rozofs_u32_append(p,val1); \
  *p++='/'; \
  p += rozofs_u32_append(p,val2); \
  p += rozofs_eol(p);\
}   
#define DISPLAY_ATTR_HASH(name,val1,val2,val3) {\
  DISPLAY_ATTR_TITLE(name); \
  p += rozofs_x32_append(p,val1); \
  *p++='/'; \
  p += rozofs_x32_append(p,val2); \
  *p++=' '; \
  *p++='('; \
  p += rozofs_x32_append(p,val3); \
  *p++=')'; \
  p += rozofs_eol(p);\
}  
   
   
#define DISPLAY_ATTR_HEX(name,val) {\
  DISPLAY_ATTR_TITLE(name); \
  p += rozofs_x32_append(p,val); \
  p += rozofs_eol(p);\
}  
#define DISPLAY_ATTR_TXT(name,val) {\
  DISPLAY_ATTR_TITLE(name); \
  p += rozofs_string_append(p,val); \
  p += rozofs_eol(p); \
}
  
#define DISPLAY_ATTR_TXT_NOCR(name,val) {\
  DISPLAY_ATTR_TITLE(name); \
  p += rozofs_string_append(p,val);\
}  


static int print_inode_name(ext_mattr_t *ino_p,char *buf)
{
   rozofs_inode_fname_t *name_p;
   
   name_p = &ino_p->s.fname;
   if (name_p->name_type == ROZOFS_FNAME_TYPE_DIRECT)
   {     
     memcpy(buf,name_p->name,name_p->len);
     buf[name_p->len] = 0;
     return name_p->len;
   }
   return 0;
}  
  
static inline int get_rozofs_xattr(export_t *e, lv2_entry_t *lv2, char * value, int size) {
  char    * p=value;
  int       idx;
  char      bufall[128];
  uint8_t   rozofs_safe = rozofs_get_rozofs_safe(e->layout);
  rozofs_inode_t inode; 
  
  DISPLAY_ATTR_UINT("EID", e->eid);
  DISPLAY_ATTR_UINT("VID", e->volume->vid);
  DISPLAY_ATTR_UINT("LAYOUT", e->layout);  
  DISPLAY_ATTR_UINT("BSIZE", e->bsize);  
  if (print_inode_name(&lv2->attributes,bufall) != 0)
  {
    DISPLAY_ATTR_TXT("NAME",bufall);
  }
  DISPLAY_ATTR_TITLE( "PARENT"); 
  rozofs_uuid_unparse(lv2->attributes.s.pfid,p);
  p += 36;
  *p++ = '\n';    
  DISPLAY_ATTR_TITLE( "FID"); 
  rozofs_uuid_unparse(lv2->attributes.s.attrs.fid,p);
  p += 36;
  *p++ = '\n';
  if (S_ISREG(lv2->attributes.s.attrs.mode)) {

     fid_t fidtmp;
     rozofs_inode_t *fake_inode = (rozofs_inode_t*)fidtmp;
     memcpy(fidtmp,lv2->attributes.s.attrs.fid,sizeof(fid_t));
     fake_inode->s.key = ROZOFS_REG_S_MOVER;
     fake_inode->s.del = 0;
     DISPLAY_ATTR_TITLE( "FID_S"); 
     rozofs_uuid_unparse(fidtmp,p);
     p += 36;
     *p++ = '\n';     
     fake_inode->s.key = ROZOFS_REG_D_MOVER;
     fake_inode->s.del = 0;
     DISPLAY_ATTR_TITLE( "FID_M"); 
     rozofs_uuid_unparse(fidtmp,p);
     p += 36;
     *p++ = '\n';                 
  }
  rozofs_inode_t * fake_inode_p =  (rozofs_inode_t *) lv2->attributes.s.attrs.fid;
  DISPLAY_ATTR_TITLE( "SPLIT");   
  p += rozofs_string_append(p,"vers=");
  p += rozofs_u64_append(p,fake_inode_p->s.vers);
  p += rozofs_string_append(p," mover_idx=");
  p += rozofs_u64_append(p,fake_inode_p->s.mover_idx);
  p += rozofs_string_append(p," fid_high=");
  p += rozofs_u64_append(p,fake_inode_p->s.fid_high);
  p += rozofs_string_append(p," recycle=");
  p += rozofs_u64_append(p,rozofs_get_recycle_from_fid(fake_inode_p));  
  p += rozofs_string_append(p," opcode=");
  p += rozofs_u64_append(p,fake_inode_p->s.opcode);
  p += rozofs_string_append(p," exp_id=");
  p += rozofs_u64_append(p,fake_inode_p->s.exp_id);
  p += rozofs_string_append(p," eid=");
  p += rozofs_u64_append(p,fake_inode_p->s.eid);
  p += rozofs_string_append(p," usr_id=");
  p += rozofs_u64_append(p,fake_inode_p->s.usr_id);
  p += rozofs_string_append(p," file_id=");
  p += rozofs_u64_append(p,fake_inode_p->s.file_id);
  p += rozofs_string_append(p," idx=");
  p += rozofs_u64_append(p,fake_inode_p->s.idx);
  p += rozofs_string_append(p," del=");
  p += rozofs_u64_append(p,fake_inode_p->s.del);
  p += rozofs_string_append(p," key=");
  p += rozofs_u64_append(p,fake_inode_p->s.key);
  p += rozofs_string_append(p," (");
  p += rozofs_string_append(p,export_attr_type2String(fake_inode_p->s.key));
  p += rozofs_string_append(p,")");     
  p += rozofs_eol(p);
    
  uint32_t bucket_idx = ((lv2->attributes.s.hash2 >> 16) ^ (lv2->attributes.s.hash2 & 0xffff))&((1<<8)-1);
  DISPLAY_ATTR_HASH("HASH1/2",lv2->attributes.s.hash1,lv2->attributes.s.hash2,bucket_idx);
  DISPLAY_ATTR_2UINT("UID/GID",lv2->attributes.s.attrs.uid,lv2->attributes.s.attrs.gid);
  bufall[0] = 0;
  ctime_r((const time_t *)&lv2->attributes.s.cr8time,bufall);
  DISPLAY_ATTR_TXT_NOCR("CREATE", bufall);
  ctime_r((const time_t *)&lv2->attributes.s.attrs.mtime,bufall);
  DISPLAY_ATTR_TXT_NOCR("MTIME ", bufall);
  ctime_r((const time_t *)&lv2->attributes.s.attrs.atime,bufall);
  DISPLAY_ATTR_TXT_NOCR("ATIME ", bufall);
  if (test_no_extended_attr(lv2)) {
    DISPLAY_ATTR_TXT("XATTR", "NO");  
  }
  else
  {
    DISPLAY_ATTR_TXT("XATTR", "YES");    
  }
  DISPLAY_ATTR_INT("I-STATE",lv2->attributes.s.i_state);
  if (S_ISDIR(lv2->attributes.s.attrs.mode)) {
    rozofs_dir0_sids_t byte_sid0;
    int backup_val;
    ext_dir_mattr_t *ext_dir_mattr_p = (ext_dir_mattr_t*)lv2->attributes.s.attrs.sids;
    byte_sid0.byte = lv2->attributes.s.attrs.sids[0];
    backup_val = byte_sid0.s.backup;
    
    DISPLAY_ATTR_TXT("MODE", "DIRECTORY");
    switch (backup_val)
    {
       case 0:
       default:
         DISPLAY_ATTR_TXT("BACKUP", "NO");
         break;
       case 1:
         DISPLAY_ATTR_TXT("BACKUP", "DIR-ONLY");
         break;       
       case 2:
         DISPLAY_ATTR_TXT("BACKUP", "DIR-RECURSIVE");
	 break;     
    }  

//    if (lv2->attributes.s.attrs.cid != 0)
    {
       DISPLAY_ATTR_INT("PROJECT",lv2->attributes.s.attrs.cid);    
    }
    /*
    ** display trash information
    */
    backup_val = byte_sid0.s.trash;
    switch (backup_val)
    {
       case 0:
       default:
         DISPLAY_ATTR_TXT("TRASH", "NO");
         break;
       case 1:
         DISPLAY_ATTR_TXT("TRASH", "YES");
         break;       
       case 2:
         DISPLAY_ATTR_TXT("TRASH", "YES (RECURSIVE)");
	 break;     
    }  
    if (byte_sid0.s.root_trash)
    {
      DISPLAY_ATTR_TXT("R_TRASH", "YES");
    }
    else
    {
      DISPLAY_ATTR_TXT("R_TRASH", "NO");    
    }
    DISPLAY_ATTR_UINT("CHILDREN",lv2->attributes.s.attrs.children);
    DISPLAY_ATTR_UINT("NLINK",lv2->attributes.s.attrs.nlink);
    DISPLAY_ATTR_ULONG("SIZE",lv2->attributes.s.attrs.size);
    DISPLAY_ATTR_ULONG("DELETED",lv2->attributes.s.hpc_reserved.dir.nb_deleted_files);
    /*
    ** display the directory statistics (update time & nb bytes)
    */
    ctime_r((const time_t *)&ext_dir_mattr_p->s.update_time,bufall);
    DISPLAY_ATTR_TXT_NOCR("UTIME ", bufall);    
    DISPLAY_ATTR_ULONG("TSIZE",ext_dir_mattr_p->s.nb_bytes);
   return (p-value);  
  }

  if (S_ISLNK(lv2->attributes.s.attrs.mode)) {
    DISPLAY_ATTR_TXT("MODE", "SYMBOLIC LINK");
  }  
  else {
    DISPLAY_ATTR_TXT("MODE", "REGULAR FILE");
    DISPLAY_ATTR_INT("PROJECT",lv2->attributes.s.hpc_reserved.reg.share_id);    
    if (e->thin)     DISPLAY_ATTR_UINT("NB_BLOCKS",lv2->attributes.s.hpc_reserved.reg.nb_blocks_thin); // Thin prov fix
    DISPLAY_ATTR_HEX("MODE",lv2->attributes.s.attrs.mode);
  }
  
  /*
  ** File only
  */
  DISPLAY_ATTR_UINT("CLUSTER",lv2->attributes.s.attrs.cid);
  DISPLAY_ATTR_TITLE("STORAGE");
  p += rozofs_u32_padded_append(p,3, rozofs_zero,lv2->attributes.s.attrs.sids[0]); 
  for (idx = 1; idx < rozofs_safe; idx++) {
    *p++ = '-';
    p += rozofs_u32_padded_append(p,3, rozofs_zero,lv2->attributes.s.attrs.sids[idx]);
  } 
  p += rozofs_eol(p);
  /*
  ** Check the case of a file that is currently move internally
  */
  {
  rozofs_mover_sids_t *dist_mv_p;
  dist_mv_p = (rozofs_mover_sids_t*)&lv2->attributes.s.attrs.sids;
  if (dist_mv_p->dist_t.mover_cid != 0)
  {  
    DISPLAY_ATTR_UINT("CLUSTERM",dist_mv_p->dist_t.mover_cid);
    DISPLAY_ATTR_TITLE("STORAGEM");
    p += rozofs_u32_padded_append(p,3,rozofs_zero, dist_mv_p->dist_t.mover_sids[0]); 
    for (idx = 1; idx < rozofs_safe; idx++) {
      *p++ = '-';
      p += rozofs_u32_padded_append(p,3, rozofs_zero,dist_mv_p->dist_t.mover_sids[idx]);
    } 
    p += rozofs_eol(p);
    }
  }
  DISPLAY_ATTR_HEX("CHILD",lv2->attributes.s.attrs.children);
  if (lv2->time_move != 0)
  {
    bufall[0] = 0;
    ctime_r((const time_t *)&lv2->time_move,bufall);
    DISPLAY_ATTR_TXT_NOCR("MOVETM", bufall);  
  }
  /*
  ** display the FID used for the storage
  */
  {
     fid_t fid_storage;
     int retcode;
     
     rozofs_build_storage_fid_from_attr(&lv2->attributes.s.attrs,fid_storage,ROZOFS_PRIMARY_FID);
     DISPLAY_ATTR_TITLE( "FID_SP"); 
     rozofs_uuid_unparse(fid_storage,p);
     p += 36;
     *p++ = '\n';     
     retcode = rozofs_build_storage_fid_from_attr(&lv2->attributes.s.attrs,fid_storage,ROZOFS_MOVER_FID);
     if (retcode == 0)
     {
       DISPLAY_ATTR_TITLE( "FID_SM"); 
       rozofs_uuid_unparse(fid_storage,p);
       p += 36;
       *p++ = '\n';             
     }  
  }
  DISPLAY_ATTR_TITLE("ST.SLICE");
  p += rozofs_u32_append(p,rozofs_storage_fid_slice(lv2->attributes.s.attrs.fid)); 
  p += rozofs_eol(p);
  
  DISPLAY_ATTR_TITLE("ST.NAME");
  memcpy(&inode,fake_inode_p,sizeof(rozofs_inode_t));
  rozofs_reset_recycle_on_fid(&inode);
  rozofs_uuid_unparse((unsigned char *)&inode,p);
  p += 36;
  *p++ = '\n';

  DISPLAY_ATTR_UINT("NLINK",lv2->attributes.s.attrs.nlink);
  DISPLAY_ATTR_ULONG("SIZE",lv2->attributes.s.attrs.size);

  if (lv2->attributes.s.bitfield1 & ROZOFS_BITFIELD1_PERSISTENT_FLOCK){
    DISPLAY_ATTR_TXT("FLOCKP", "This file");
  }
  else if (e->flockp)  {
    DISPLAY_ATTR_TXT("FLOCKP", "export.conf");
  }
  else if (common_config.persistent_file_locks) {
    DISPLAY_ATTR_TXT("FLOCKP", "rozofs.conf");  
  }
  else {
    DISPLAY_ATTR_TXT("FLOCKP", "NOT PERSISTENT");
  }
  
  DISPLAY_ATTR_UINT("LOCK",lv2->nb_locks);  
  if (lv2->nb_locks != 0) {
     DISPLAY_ATTR_TITLE( "SETLK"); 
     p += rozofs_format_flockp_string(p,lv2);
    *p++ = '\n';   
  }
  return (p-value);  
} 
/*
** Change the target of a symlink when it exist
*/
static inline int set_rozofs_link_from_fid(export_t *e, lv2_entry_t *lv2, char * link,int length,epgw_setxattr_symlink_t *symlink) {
    export_tracking_table_t *trk_tb_p = e->trk_tb_p;
    rozofs_inode_t fake_inode;
    exp_trck_top_header_t *p = NULL;
    int ret;
     
    /*
    * read the link: the size of the link is found in the common attributes
    */
    if (lv2->attributes.s.i_link_name == 0)
    {
      errno = EBADF;
      return -1;    
    }
    fake_inode.fid[1] = lv2->attributes.s.i_link_name;

    if (fake_inode.s.key != ROZOFS_SLNK)
    {
      errno = EBADF;
      return -1;      
    }
    p = trk_tb_p->tracking_table[fake_inode.s.key];
    if (p == NULL)
    {
      errno = EINVAL;
      return -1;      
    }   
      
    /*
    ** write the new link name on disk
    */
    ret = exp_metadata_write_attributes(p,&fake_inode,link,length, 0 /* no sync */);
    if (ret < 0)
    { 
      return -1;      
    }

    /*
    ** Update the symlink name in the lv2 cache
    */

    /* The size of the target is increased, 
    ** so let's free the target to allocate a bigger one
    */
    if ((lv2->symlink_target) && (lv2->attributes.s.attrs.size < length)) {
      free(lv2->symlink_target);
      lv2->symlink_target = NULL; 
    }  
    /*
    ** Need to allocate memory to store the new target
    */
    if (lv2->symlink_target==NULL) {
      lv2->symlink_target = malloc(length+1);
    }  
    /*
    ** Save the new target name
    */
    if (lv2->symlink_target) {
      memcpy(lv2->symlink_target,link,length);
      lv2->symlink_target[length] = 0;
    }  
      
    /*
    ** If link length has change update it 
    */
    if (lv2->attributes.s.attrs.size != length) {
      lv2->attributes.s.attrs.size = length;
      /*
      ** Save new size on disk
      */
      ret = export_lv2_write_attributes(e->trk_tb_p,lv2, 0/* no sync */);
      if (ret < 0)
      { 
        return -1;      
      }
    }  
    
    /*
    ** Update symlink in returned response
    */
    symlink->status = EP_SUCCESS;
    memcpy(symlink->epgw_setxattr_symlink_t_u.info.symlink_fid,lv2->attributes.s.attrs.fid,sizeof(fid_t));
    symlink->epgw_setxattr_symlink_t_u.info.target.target_len = length+1;
    symlink->epgw_setxattr_symlink_t_u.info.target.target_val = strdup(lv2->symlink_target);    
    return 0;  
}
/*
** Change the target of a symlink when it exist
*/
int set_rozofs_link_from_name(export_t * e, lv2_entry_t *plv2, char * link,int length, epgw_setxattr_symlink_t *symlink) {
  lv2_entry_t *lv2;
  int status = -1;
  fid_t child_fid;
  uint32_t child_type;
  int fdp = -1;     /* file descriptor of the parent directory */
  int root_dirent_mask=0;
      
  /*
  ** Check the plv2 entry is a directory
  */
  if (!S_ISDIR(plv2->attributes.s.attrs.mode)) {
    errno = ENOTDIR;
    return -1;
  }

  /*
  ** Parse link content 
  */
  char * name = link;
  /*
  ** Skip name and got to ' ' 
  */
  while ((length) && (*link != ' ')) {
    link++;
    length--;
  }
  if (length==0) {
    errno = EINVAL;
    return -1;
  }
  *link = 0;
  link++;
  length--;
  
  /*
  ** skip ' ' and go to new target name 
  */
  while ((length) && (*link == ' ')) {
    link++;
    length--;
  }    

  if (length==0) {
    errno = EINVAL;
    return -1;
  }

  /*
  ** load the root_idx bitmap of the old parent
  */
  export_dir_load_root_idx_bitmap(e,plv2->attributes.s.attrs.fid,plv2);


  fdp = export_open_parent_directory(e,plv2->attributes.s.attrs.fid);
  if (get_mdirentry(plv2->dirent_root_idx_p,fdp, plv2->attributes.s.attrs.fid, name, child_fid, &child_type,&root_dirent_mask) != 0) {
    goto out;
  }
  
  // get the lv2
  if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, child_fid))) {
      /*
       ** It might be possible that the file is still referenced in the dirent file but 
       ** not present on disk: its FID has been released (and the associated file deleted)
       ** In that case when attempt to read that fid file, we get a ENOENT error.
       ** So for that particular case, we remove the entry from the dirent file
       **
       **  open point : that issue is not related to regular file but also applied to directory
       ** 
       */
      int xerrno;
      uint32_t type;
      fid_t fid;
      if (errno == ENOENT) {
          /*
           ** save the initial errno and remove that entry
           */
          xerrno = errno;
          del_mdirentry(plv2->dirent_root_idx_p,fdp, plv2->attributes.s.attrs.fid, name, fid, &type,root_dirent_mask);
          errno = xerrno;
      }
      goto out;
  }
  status = set_rozofs_link_from_fid(e, lv2, link, length,symlink);

out:
  /*
  ** check if parent root idx bitmap must be updated
  */
  if (plv2 != NULL) export_dir_flush_root_idx_bitmap(e,plv2->attributes.s.attrs.fid,plv2->dirent_root_idx_p);
  /*
  ** close the parent directory
  */
  if (fdp != -1) close(fdp);
  return status;    
}
static char buf_xattr[1024];

/*
**__________________________________________________________________________________
**
** Remove locks saved in "root.RozoFLOCK extended attributes
**
** @param e     export context
** @param lv2   entry to remove locks from
**
**__________________________________________________________________________________
*/
static inline void rozofs_remove_flocks_in_xattr(export_t *e, lv2_entry_t *lv2) { 
  export_removexattr(e, lv2->attributes.s.attrs.fid, ROZOFS_XATTR_FLOCKP);
}
/*
**__________________________________________________________________________________
**
** Check whether a file should have permananet file locks
**
** @param e     export context
** @param lv2   lv2 entry of the file
**
**__________________________________________________________________________________
*/
int rozofs_are_persistent_file_locks_configured(export_t *e, lv2_entry_t *lv2) {
  /*
  ** Check wether persistent file locks are configured
  ** either common configuration (rozofs.conf)
  ** or this export configuration (export.conf)
  ** or this file (attr -s rozofs -V "flockp = 1 "
  */
  if (e->flockp) return 1; 
  if (common_config.persistent_file_locks) return 1;
  if ((lv2->attributes.s.bitfield1 & ROZOFS_BITFIELD1_PERSISTENT_FLOCK) != 0) return 1;
  return 0;
}     
/*
**__________________________________________________________________________________
**
** Save locks in in root.RozoFLOCK extended attributes
**
** @param e     export context
** @param lv2   entry to remove locks from
**
**__________________________________________________________________________________
*/
int rozofs_save_flocks_in_xattr(export_t *e, lv2_entry_t *lv2) {
  char * p = buf_xattr;

  /*
  ** Check wether persistent file locks are configured
  */
  if (rozofs_are_persistent_file_locks_configured(e,lv2) == 0) {
    /*
    ** No persistent flocks
    */
    return 0;
  }
    
  /*
  ** Remove old locks
  */
  rozofs_remove_flocks_in_xattr(e,lv2);
    
  /*
  ** Format new ones
  */  
  p += rozofs_format_flockp_string(p,lv2);  
  if (p == buf_xattr) return 0;

  /*
  ** Save formated string
  */  
  export_setxattr(e, lv2->attributes.s.attrs.fid, ROZOFS_XATTR_FLOCKP, buf_xattr, p-buf_xattr, 0, NULL);
  return p-buf_xattr;
}
/*
**__________________________________________________________________________________
**
** Set rozofs proprietary attribute
**
**__________________________________________________________________________________
*/
static inline int set_rozofs_xattr(export_t *e, lv2_entry_t *lv2, char * input_buf,int length) {
  char       * p;
  int          idx,jdx;
  int          new_cid;
  int          new_sids[ROZOFS_SAFE_MAX]; 
  uint8_t      rozofs_safe;
  uint64_t     valu64;
  char * value=buf_xattr;
  
  p=value;
  memcpy(buf_xattr,input_buf,length);
  buf_xattr[length]=0;
  /*
  ** Is this a backup mode change : 0: no backup/ 1: backup file of this directory only/ 2: backup recursive
  */  
  if ((sscanf(p," share = %llu", (long long unsigned int *)&valu64) == 1) 
  ||  (sscanf(p," project = %llu", (long long unsigned int *)&valu64) == 1))
  {
    if (valu64 > ((1024*64)-1))
    {
      errno = ERANGE;
      return -1;        
    }
    if (!S_ISDIR(lv2->attributes.s.attrs.mode)) {
      errno = ENOTDIR;
      return -1;
    }
    if (lv2->attributes.s.attrs.children != 0)
    {
      errno = EPERM;
      return -1;
    
    }   
    /*
    ** Save new distribution on disk
    */
    lv2->attributes.s.attrs.cid=(cid_t)valu64;
    return export_lv2_write_attributes(e->trk_tb_p,lv2,0/* No sync */);
  }


  /*
  ** Set persistent file lock on this file. Lock are kept even on export switchover
  */
  if (sscanf(p," flockp = %llu", (long long unsigned int *)&valu64) == 1) {
  
    /*
    ** Remove persistent lock
    */
    if (valu64 == 0) {

      if ((lv2->attributes.s.bitfield1 & ROZOFS_BITFIELD1_PERSISTENT_FLOCK)==0) {
        /* Already done */
        return 0;
      }  
      /* Remove bit */
      lv2->attributes.s.bitfield1 &= ~ROZOFS_BITFIELD1_PERSISTENT_FLOCK;

      /* 
      ** When persistent file locks are not set any more
      ** remove specific RozoFS xattribute for persistent locks if any.
      */
      if (rozofs_are_persistent_file_locks_configured(e, lv2) == 0) {
        rozofs_remove_flocks_in_xattr(e, lv2); 
      }     
      return export_lv2_write_attributes(e->trk_tb_p,lv2,0/* No sync */);  
    }
    
    /*
    ** Set persistent lock
    */
    if ((lv2->attributes.s.bitfield1 & ROZOFS_BITFIELD1_PERSISTENT_FLOCK)!=0) {
      /* ALready done */
      return 0;
    }  

    /* Set bit */
    lv2->attributes.s.bitfield1 |= ROZOFS_BITFIELD1_PERSISTENT_FLOCK;
    
    /* Format and save current locks in specific RozoFS xattribute */
    rozofs_save_flocks_in_xattr(e, lv2);

    /*
    ** Save new distribution on disk
    */
    return export_lv2_write_attributes(e->trk_tb_p,lv2,0/* No sync */);
  }
  
  /*
  ** Is this a backup mode change : 0: no backup/ 1: backup file of this directory only/ 2: backup recursive
  */  

  if (sscanf(p," backup = %llu", (long long unsigned int *)&valu64) == 1) 
  {
    if (valu64 > 2)
    {
      errno = ERANGE;
      return -1;        
    }
    if (!S_ISDIR(lv2->attributes.s.attrs.mode)) {
      errno = ENOTDIR;
      return -1;
    }
    if (((rozofs_dir0_sids_t*)&lv2->attributes.s.attrs.sids[0])->s.backup!= valu64)
    {
      /*
      ** Save new distribution on disk
      */
      ((rozofs_dir0_sids_t*)&lv2->attributes.s.attrs.sids[0])->s.backup= valu64;
      return export_lv2_write_attributes(e->trk_tb_p,lv2,0/* No sync */);
    }
    return 0;
  }
  /*
  ** case of the trash
  */
  if (sscanf(p," trash = %llu", (long long unsigned int *)&valu64) == 1) 
  {
    if (valu64 > 2)
    {
      errno = ERANGE;
      return -1;        
    }
    if (!S_ISDIR(lv2->attributes.s.attrs.mode)) {
      errno = ENOTDIR;
      return -1;
    }
    if (((rozofs_dir0_sids_t*)&lv2->attributes.s.attrs.sids[0])->s.trash!= (sid_t)valu64)
    {
      /*
      ** Save new distribution on disk
      */
      ((rozofs_dir0_sids_t*)&lv2->attributes.s.attrs.sids[0])->s.trash=(sid_t)valu64;
      return export_lv2_write_attributes(e->trk_tb_p,lv2,0/* No sync */);
    }
    return 0;
  }

  /*
  ** case of the root trash
  */
  if (sscanf(p," root-trash = %llu", (long long unsigned int *)&valu64) == 1) 
  {
    if (valu64 > 1)
    {
      errno = ERANGE;
      return -1;        
    }
    if (!S_ISDIR(lv2->attributes.s.attrs.mode)) {
      errno = ENOTDIR;
      return -1;
    }
    if (((rozofs_dir0_sids_t*)&lv2->attributes.s.attrs.sids[0])->s.root_trash!= (sid_t)valu64)
    {
      /*
      ** Save new distribution on disk
      */
      ((rozofs_dir0_sids_t*)&lv2->attributes.s.attrs.sids[0])->s.root_trash=(sid_t)valu64;
      return export_lv2_write_attributes(e->trk_tb_p,lv2,0/* No sync */);
    }
    return 0;
  }
  /*
  ** Is this an uid change 
  */  
  if (sscanf(p," uid = %llu", (long long unsigned int *) &valu64) == 1) {
    if (valu64 > 0xFFFFFFFF) {
      errno = ERANGE;
      return -1;            
    }
    if (lv2->attributes.s.attrs.uid != valu64) {
      lv2->attributes.s.attrs.uid = valu64;
      /*
      ** Save new distribution on disk
      */
      return export_lv2_write_attributes(e->trk_tb_p,lv2,0/* No sync */);

    }
    return 0;
  }

  /*
  ** Is this an gid change 
  */  
  if (sscanf(p," gid = %llu", (long long unsigned int *) &valu64) == 1) {
    if (valu64 > 0xFFFFFFFF) {
      errno = ERANGE;
      return -1;            
    }
    if (lv2->attributes.s.attrs.gid != valu64) {
      lv2->attributes.s.attrs.gid = valu64;
      /*
      ** Save new distribution on disk
      */
      return export_lv2_write_attributes(e->trk_tb_p,lv2,0/* No sync */);

    }
    return 0;
  }
  
  /*
  ** Is this an children change 
  */  
  if (sscanf(p," children = %llu", (long long unsigned int *) &valu64) == 1) {
    if (valu64 > 0xFFFFFFFF) {
      errno = ERANGE;
      return -1;            
    }
    if (lv2->attributes.s.attrs.children != valu64) {
      lv2->attributes.s.attrs.children = valu64;
      /*
      ** Save new distribution on disk
      */
      return export_lv2_write_attributes(e->trk_tb_p,lv2,0/* No sync */);

    }
    return 0;
  }
  
  /*
  ** Is this a nlink change 
  */  
  if (sscanf(p," nlink = %llu", (long long unsigned int *) &valu64) == 1) {
    if (valu64 > 0xFFFF) {
      errno = ERANGE;
      return -1;            
    }
    if (lv2->attributes.s.attrs.nlink != valu64) {
      lv2->attributes.s.attrs.nlink = valu64;
      /*
      ** Save new distribution on disk
      */
      return export_lv2_write_attributes(e->trk_tb_p,lv2,0/* No sync */);

    }
    return 0;
  }
  if (sscanf(p," size = %llu", (long long unsigned int *) &valu64) == 1) {
    if (valu64 >= ROZOFS_FILESIZE_MAX) {
      errno = EFBIG;
      return -1;            
    }
    if (lv2->attributes.s.attrs.size != valu64) {
      lv2->attributes.s.attrs.size = valu64;
      /*
      ** Save new distribution on disk
      */
      return export_lv2_write_attributes(e->trk_tb_p,lv2,0/* No sync */);

    }
    return 0;
  }

  if (S_ISDIR(lv2->attributes.s.attrs.mode)) {
    errno = EISDIR;
    return -1;
  }
     
  if (S_ISLNK(lv2->attributes.s.attrs.mode)) {
    errno = EMLINK;
    return -1;
  }
  /*
  ** case of the mover: allocation of the new distribution for the file to move
  */
  if (sscanf(p," mover_allocate = %d", &new_cid) == 1)
  {
     return (rozofs_mover_allocate_scan(value,p,length,e,lv2,new_cid));  
  }
  if (sscanf(p," mover_invalidate = %llu", (long long unsigned int *)&valu64) == 1)
  {
     return (rozofs_mover_invalid_scan(e,lv2,valu64));  
  }

  if (sscanf(p," mover_validate = %llu", (long long unsigned int *)&valu64) == 1)
  {
     return (rozofs_mover_valid_scan(e,lv2,valu64));  
  }

  /*
  ** File must not yet be written 
  */
  if (lv2->attributes.s.attrs.size != 0) {
    errno = EFBIG;
    return -1;
  } 
  
  /*
  ** Scan value
  */
  rozofs_safe = rozofs_get_rozofs_safe(e->layout);
  memset (new_sids,0,sizeof(new_sids));
  new_cid = 0;

  errno = 0;
  new_cid = strtol(p,&p,10);
  if (errno != 0) return -1; 
  
  for (idx=0; idx < rozofs_safe; idx++) {
  
    if ((p-value)>=length) {
      errno = EINVAL;
      break;
    }

    new_sids[idx] = strtol(p,&p,10);
    if (errno != 0) return -1;
    if (new_sids[idx]<0) new_sids[idx] *= -1;
  }
  
  /* Only cluster id is given */
  if (idx == 0) {
    for (idx=0; idx < rozofs_safe; idx++) {
      new_sids[idx] = lv2->attributes.s.attrs.sids[idx];
    }
  }
   
  /* Not enough sid in the list */
  else if (idx != rozofs_safe) {
    return -1;
  }
  
  
  /*
  ** Check the same sid is not set 2 times
  */
  for (idx=0; idx < rozofs_safe; idx++) {
    for (jdx=idx+1; jdx < rozofs_safe; jdx++) {
      if (new_sids[idx] == new_sids[jdx]) {
        errno = EINVAL;
	return -1;
      }
    }
  }  

  /*
  ** Check cluster and sid exist
  */
  if (volume_distribution_check(e->volume, rozofs_safe, new_cid, new_sids) != 0) return -1;
  
  /*
  ** OK for the new distribution
  */
  lv2->attributes.s.attrs.cid = new_cid;
  for (idx=0; idx < rozofs_safe; idx++) {
    lv2->attributes.s.attrs.sids[idx] = new_sids[idx];
  }
  
  /*
  ** Save new distribution on disk
  */
  return export_lv2_write_attributes(e->trk_tb_p,lv2,0/* No sync */);  
} 
/*
**______________________________________________________________________________
*/
static inline int get_rozofs_xattr_internal_id(export_t *e, lv2_entry_t *lv2, char * value, int size) {
  char    * p=value;
  int       idx;
  uint8_t   rozofs_safe = rozofs_get_rozofs_safe(e->layout);
  
  
  p+=sprintf(p,"@rozofs@%d-%d-%d-",e->eid,lv2->attributes.s.attrs.cid,e->layout);
  for (idx = 0; idx < rozofs_safe; idx++) {
    p += sprintf(p,"%d-", lv2->attributes.s.attrs.sids[idx]);
  }
  rozofs_uuid_unparse(lv2->attributes.s.attrs.fid,p); 
  p+=36;
  /*
  ** put the length
  */
  p+=sprintf(p,"-%llu",(long long unsigned int)lv2->attributes.s.attrs.size);
  
  return (p-value);  
} 
/*
**______________________________________________________________________________
*/
static inline int get_rozofs_xattr_max_size(export_t *e, lv2_entry_t *lv2, char * value, int size) {
  char    * p=value;
  int       idx;
  uint64_t  free=-1;
  uint8_t   rozofs_forward = rozofs_get_rozofs_forward(e->layout);
  int       rozofs_psize;
  volume_t * volume; 
  epp_sstat_t * storage_stat = NULL; 
  int        idx_stor;
  int        found = 0;
  int        quota=0;
  

  if (S_ISDIR(lv2->attributes.s.attrs.mode)) {
    p += sprintf(p,"rozof_max_size is not applicable to a directory."); 
    return (p-value);  
  }

  if (S_ISLNK(lv2->attributes.s.attrs.mode)) {
    p += sprintf(p,"rozof_max_size is not applicable to a symbolic link."); 
    return (p-value);
  }  
  

  volume = e->volume;

  /*
  ** Find out the volume of this file
  */
  int nb_volumes = gprofiler.nb_volumes;
  for (idx=0; idx < nb_volumes; idx++) { 
    if (gprofiler.vstats[idx].vid == volume->vid) break;
  }
  if (idx == nb_volumes) {
    p += sprintf(p,"Temporary unavailable. Please retry."); 
    return (p-value);
  }  


  /*
  ** Loop on the storages of this volume to check those
  ** of the file distribution and get the one that has the
  ** less free size.
  */
  int nb_storages = gprofiler.vstats[idx].nb_storages;  
  storage_stat = &gprofiler.vstats[idx].sstats[0];;   
  
  for (idx=0; idx <  nb_storages; idx++,storage_stat++) {
  
    if (storage_stat->cid != lv2->attributes.s.attrs.cid) continue;

    for(idx_stor=0; idx_stor < rozofs_forward; idx_stor++) {
    
      if (storage_stat->sid != lv2->attributes.s.attrs.sids[idx_stor]) continue;
      
      /*
      ** Get the storage that has the less free space
      */
      if (free > storage_stat->free) free = storage_stat->free;
      found++;
      
      if (found == rozofs_forward) break;
    }
    if (found == rozofs_forward) break;        
  } 
  
  
  if (idx == nb_storages) { 
    p += sprintf(p,"Temporary unavailable. Please retry."); 
    return (p-value);
  }

  /*
  ** This is the max number of blocks that we can still write
  ** for this distribution
  */
  rozofs_psize = rozofs_get_max_psize_on_disk(e->layout,e->bsize);
  
  free /= rozofs_psize;
        
  /*
  ** Respect the hard quota
  */	
  if (e->hquota > 0) {
    export_fstat_t * estats = export_fstat_get_stat(e->eid);    
    if (estats!=NULL) {  
      uint64_t quota_left;
      /*
      ** take care of the thin provisioning
      */
      if (e->thin == 0)
        quota_left = (e->hquota - estats->blocks);
      else
        quota_left = (e->hquota - estats->blocks_thin);      
      if (free > quota_left) {
        free = quota_left;
        quota = 1;
      }
    }
  }
  
  /*
  ** Find out the rigth unit to display the result
  */
  free *= ROZOFS_BSIZE_BYTES(e->bsize);
  if (free > ROZOFS_STORAGE_FILE_MAX_SIZE) free = ROZOFS_STORAGE_FILE_MAX_SIZE;
  
  if (free < 1024) {
    p += sprintf(p,"Remaining available space %llu B%s", (unsigned long long int) free, quota?" (hard quota).":"."); 
    return (p-value);   
  } 
  
  free /= 1024;
  if (free < 1024) {
    p += sprintf(p,"Remaining available space %llu KiB%s", (unsigned long long int) free, quota?" (hard quota).":"."); 
    return (p-value);   
  } 

  free /= 1024;    
  if (free < 1024) {
    p += sprintf(p,"Remaining available space %llu MiB%s", (unsigned long long int) free, quota?" (hard quota).":"."); 
    return (p-value);   
  }  
    
  free /= 1024;
  if (free < 1024) {
    p += sprintf(p,"Remaining available space %llu GiB%s", (unsigned long long int) free, quota?" (hard quota).":"."); 
    return (p-value);   
  }
  
  free /= 1024;
  p += sprintf(p,"Remaining available space %llu TiB%s", (unsigned long long int) free, quota?" (hard quota).":"."); 
  return (p-value);    
}  
/*
**______________________________________________________________________________
*/
/** retrieve an extended attribute value.
 *
 * @param e: the export managing the file or directory.
 * @param fid: the id of the file or directory.
 * @param name: the extended attribute name.
 * @param value: the value of this extended attribute.
 * @param size: the size of a buffer to hold the value associated
 *  with this extended attribute.
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */
ssize_t export_getxattr(export_t *e, fid_t fid, const char *name, void *value, size_t size) {
    ssize_t status = -1;
    lv2_entry_t *lv2 = 0;
    void * buffer;

    START_PROFILING(export_getxattr);
    /*
    ** check if the request is just for the xattr len: if it the case set the buffer to NULL
    */
    if (size == 0) buffer = 0;
    else buffer = value;

    if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid))) {
        severe("export_getattr failed: %s", strerror(errno));
        goto out;
    }

    if ((strcmp(name,ROZOFS_XATTR)==0)||(strcmp(name,ROZOFS_USER_XATTR)==0)||(strcmp(name,ROZOFS_ROOT_XATTR)==0)) {
      status = get_rozofs_xattr(e,lv2,value,size);
      goto out;
    }  
    if ((strcmp(name,ROZOFS_XATTR_ID)==0)||(strcmp(name,ROZOFS_USER_XATTR_ID)==0)||(strcmp(name,ROZOFS_ROOT_XATTR_ID)==0)) {
      status = get_rozofs_xattr_internal_id(e,lv2,value,size);
      goto out;
    } 
    if ((strcmp(name,ROZOFS_XATTR_MAX_SIZE)==0)||(strcmp(name,ROZOFS_USER_XATTR_MAX_SIZE)==0)||(strcmp(name,ROZOFS_ROOT_XATTR_MAX_SIZE)==0)) {
      status = get_rozofs_xattr_max_size(e,lv2,value,size);
      goto out;
    }
    {
      struct dentry entry;
      entry.d_inode = lv2;
      entry.trk_tb_p = e->trk_tb_p;
    
      if ((status = rozofs_getxattr(&entry, name, buffer, size)) != 0) {
          goto out;
      }
    }
out:
    STOP_PROFILING(export_getxattr);

    return status;
}

/*
**______________________________________________________________________________
*/
/** retrieve an extended attribute value in raw mode.
 *
 * @param e: the export managing the file or directory.
 * @param fid: the id of the file or directory.
 * @param name: the extended attribute name.
 * @param value: the value of this extended attribute.
 * @param size: the size of a buffer to hold the value associated
 * @param ret: pointer to the array where extended attributes must be returned
 *  with this extended attribute.
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */
ssize_t export_getxattr_raw(export_t *e, fid_t fid, const char *name, void *value, size_t size,epgw_getxattr_raw_ret_t *ret_p) {
    ssize_t status = -1;
    int ret;
    lv2_entry_t *lv2 = 0;

    START_PROFILING(export_getxattr);


    if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid))) {
        severe("export_getattr failed: %s", strerror(errno));
        goto out;
    }
    
    /*
    ** save the current tracking context needed for allocation
    */
    xattr_set_tracking_context_direct(e->trk_tb_p);

    ret = ext4_xattr_ibody_get_raw(lv2,
                                   ret_p->status_gw.ep_getxattr_raw_ret_t_u.raw.inode_xattr.inode_xattr_val,
				   &ret_p->status_gw.ep_getxattr_raw_ret_t_u.raw.inode_xattr.inode_xattr_len);
    if ((ret < 0)&&(ret!= -ENODATA)) {
      
      status = 0;
      goto out;
    } 
    errno = 0;
    ext4_xattr_block_get_raw(lv2,
                             ret_p->status_gw.ep_getxattr_raw_ret_t_u.raw.inode_xattr_block.inode_xattr_block_val,
			     &ret_p->status_gw.ep_getxattr_raw_ret_t_u.raw.inode_xattr_block.inode_xattr_block_len);
    if (errno== 0) status = 0;

out:
    /*
    ** release the memory allocated for exetnded block if it exists
    */
    if (lv2) {
      if (lv2->extended_attr_p != NULL) 
      {
        free(lv2->extended_attr_p);
        lv2->extended_attr_p = NULL;
      }
    }  
    STOP_PROFILING(export_getxattr);

    return status;
}	    	       
/*
**______________________________________________________________________________
*/
/** Check whether ther is some pending locks for this client/owner on this given file
 *
 * @param lv2:  the file lvé entry
 * @param lock_requested: the reference of the client and owner
 * 
 * @retval -1 On failure, errno is set appropriately.
 * @retval 0 No pending lock for this client/owner 
 * @retval 1 Some pendong lock exist for this client/owner
 */
int export_check_pending_file_lock(lv2_entry_t *lv2, ep_lock_t * lock_requested) {
  list_t             * p;
  rozofs_file_lock_t * lock_elt;
     
  /* Search the given lock */
  list_for_each_forward(p, &lv2->file_lock) {

    lock_elt = list_entry(p, rozofs_file_lock_t, next_fid_lock);	
    if (lock_elt->lock.client_ref != lock_requested->client_ref) continue;
    if (lock_elt->lock.owner_ref != lock_requested->owner_ref) continue;
    
    /*
    ** Yes ther is some lock from this clieent/owner
    */
    return 1;
  }
  return 0;
}   	       
/*
**______________________________________________________________________________
*/
/** Set/remove a lock on a file
 *
 * @param e: the export managing the file or directory.
 * @param fid: the id of the file or directory.
 * @param lock: the lock to set/remove
 * 
 * @retval -1 On failure, errno is set appropriately.
 * @retval 0 Success and this clienr/owner does not hold any more locks on this file
 * @retval 1 Success and this clienr/owner still holds some locks on this file
 */
int export_set_file_lock(export_t *e, fid_t fid, ep_lock_t * lock_requested, ep_lock_t * blocking_lock, ep_client_info_t * info) {
    ssize_t status = -1;
    lv2_entry_t *lv2 = 0;
    list_t      *p,*q;
    rozofs_file_lock_t * lock_elt;
    rozofs_file_lock_t * new_lock;
    int                  overlap=0;
    char                 string[256];
    int                  updated = 0;
    time_t               now = time(NULL);

    START_PROFILING(export_set_file_lock);
    if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid))) {
	flock_request2string(string,fid,lock_requested);
        severe("export_set_file_lock : LOOKUP_FID %s - %s", string, strerror(errno));
        goto out;
    }

    /*
    ** Freeing a lock 
    */
    if (lock_requested->mode == EP_LOCK_FREE) {
    
      /* Always succcess */
      status = 0;

      /* Already free */
      if (lv2->nb_locks == 0) {
	goto out;
      }
      if (list_empty(&lv2->file_lock)) {
	lv2->nb_locks = 0;
	goto out;
      }  
      
reloop:       
      /* Search the given lock */
      list_for_each_forward_safe(p, q, &lv2->file_lock) {
      
        lock_elt = list_entry(p, rozofs_file_lock_t, next_fid_lock);	
        if ((now-lock_elt->last_poll_time) > FILE_LOCK_POLL_DELAY_MAX) {
          /*
          ** Remove this out dated lock
          */
  	  flock_request2string(string,fid,&lock_elt->lock);
          warning("outdated flock %s",string);
          lv2_cache_free_file_lock(e->eid,lock_elt);
          lv2->nb_locks--;
          updated = 1;
	  if (list_empty(&lv2->file_lock)) {
	    lv2->nb_locks = 0;
	    goto out;
	  }          
          continue;
        }
        	
	if (must_file_lock_be_removed(lv2, e->eid,e->bsize,lock_requested, &lock_elt->lock, &new_lock, info)) {
	  lv2_cache_free_file_lock(e->eid,lock_elt);
	  lv2->nb_locks--;
          updated = 1;
	  if (list_empty(&lv2->file_lock)) {
	    lv2->nb_locks = 0;
	    goto out;
	  }
	  goto reloop;
	}

	if (new_lock) {
          updated = 1;
	}
      }
      goto out; 
    }

    /*
    ** Setting a new lock. Check its compatibility against every already set lock
    */
    list_for_each_forward_safe(p, q,&lv2->file_lock) {
    
      lock_elt = list_entry(p, rozofs_file_lock_t, next_fid_lock);
      if ((now-lock_elt->last_poll_time) > FILE_LOCK_POLL_DELAY_MAX) {
        /*
        ** Remove this out dated lock
        */
  	flock_request2string(string,fid,&lock_elt->lock);
        warning("outdated flock %s",string);
        lv2_cache_free_file_lock(e->eid,lock_elt);
        lv2->nb_locks--;
        continue;
      }      
      /*
      ** Check compatibility between 2 different applications
      */
      if ((lock_elt->lock.client_ref != lock_requested->client_ref) 
      ||  (lock_elt->lock.owner_ref != lock_requested->owner_ref)) { 
	if (!are_file_locks_compatible(&lock_elt->lock,lock_requested)) {
	  memcpy(blocking_lock,&lock_elt->lock,sizeof(ep_lock_t));     
          errno = EWOULDBLOCK;
	  goto out;      
	} 
    	continue;    
      }
      
      /*
      ** Check compatibility of 2 locks of a same application
      */

      /*
      ** Two read or two write locks. Check whether they overlap
      */
      if (lock_elt->lock.mode == lock_requested->mode) {
        if (are_file_locks_overlapping(lock_requested,&lock_elt->lock)) {
	  overlap++;
	}  
        continue;
      }
      /*
      ** do not check when client and process match.
      */
#if 0      
      /*
      ** One read and one write
      */
      if (!are_file_locks_compatible(&lock_elt->lock,lock_requested)) {
	memcpy(blocking_lock,&lock_elt->lock,sizeof(ep_lock_t));     
        errno = EWOULDBLOCK;
	goto out;      
      }     
#endif
      continue; 			  
    }

    /*
    ** This lock overlaps with a least one existing lock of the same application.
    ** Let's concatenate all those locks
    */  
concatenate:  
    if (overlap != 0) {
      list_for_each_forward(p, &lv2->file_lock) {

	lock_elt = list_entry(p, rozofs_file_lock_t, next_fid_lock);

	if ((lock_elt->lock.client_ref != lock_requested->client_ref) 
	||  (lock_elt->lock.owner_ref != lock_requested->owner_ref)) continue;

	if (lock_elt->lock.mode != lock_requested->mode) continue;

	if (try_file_locks_concatenate(e->bsize,lock_requested,&lock_elt->lock)) {
          overlap--;
	  lv2_cache_free_file_lock(e->eid,lock_elt);
	  lv2->nb_locks--;
          updated = 1;
	  if (list_empty(&lv2->file_lock)) {
	    lv2->nb_locks = 0;
	  }
	  goto concatenate;	  
	}
      } 
    }   
        
    /*
    ** Since we have reached this point all the locks are compatibles with the new one.
    ** and it does not overlap any more with an other lock. Let's insert this new lock
    */
    lock_elt = lv2_cache_allocate_file_lock(lv2,e->eid,lock_requested, info);
    updated = 1;
    status = 0; 
    
out:
    if (lv2) {   
    
      /*
      ** Save persistent locks in rozofs specific extended attributte when configure
      */
      if (updated) {
        rozofs_save_flocks_in_xattr(e, lv2);
      }
    
      /*
      ** Check whether there are some remaining locks for this client/owner tupple 
      ** on this FID.
      */
      if (status == 0) {
        status = export_check_pending_file_lock(lv2, lock_requested);
      }

      lv2_cache_update_lru(e->lv2_cache, lv2);
    } 	
    STOP_PROFILING(export_set_file_lock);
    return status;
}
/*
**______________________________________________________________________________
*/
/** Get a lock on a file
 *
 * @param e: the export managing the file or directory.
 * @param fid: the id of the file or directory.
 * @param lock: the lock to set/remove
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */
int export_get_file_lock(export_t *e, fid_t fid, ep_lock_t * lock_requested, ep_lock_t * blocking_lock) {
    ssize_t status = -1;
    lv2_entry_t *lv2 = 0;
    rozofs_file_lock_t *lock_elt;
    list_t * p, *q;
    char                 string[256];
    time_t               now = time(NULL);

    START_PROFILING(export_get_file_lock);
    if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid))) {
	flock_request2string(string,fid,lock_requested);
        severe("EXPORT_LOOKUP_FID get %s - %s", string, strerror(errno));
        goto out;
    }

    /*
    ** Freeing a lock 
    */
    if (lock_requested->mode == EP_LOCK_FREE) {    
      /* Always succcess */
      status = 0;
      goto out; 
    }

    /*
    ** Setting a new lock. Check its compatibility against every already set lock
    */
    list_for_each_forward_safe(p, q, &lv2->file_lock) {
    
      lock_elt = list_entry(p, rozofs_file_lock_t, next_fid_lock);
      if ((now-lock_elt->last_poll_time) > FILE_LOCK_POLL_DELAY_MAX) {
        /*
        ** Remove this out dated lock
        */
  	flock_request2string(string,fid,&lock_elt->lock);
        warning("outdated flock %s",string);        
        lv2_cache_free_file_lock(e->eid,lock_elt);
        lv2->nb_locks--;
        continue;
      }

      if (!are_file_locks_compatible(&lock_elt->lock,lock_requested)) {
	memcpy(blocking_lock,&lock_elt->lock,sizeof(ep_lock_t));     
        errno = EWOULDBLOCK;
	goto out;      
      }     
    }
    status = 0;
    
out:
    STOP_PROFILING(export_get_file_lock);
    return status;
}
/*
**______________________________________________________________________________
*/
/** reset a lock from a client
 *
 * @param e: the export managing the file or directory.
 * @param lock: the identifier of the client whose locks are to remove
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */
int export_clear_client_file_lock(export_t *e, ep_lock_t * lock_requested, ep_client_info_t * info) {

    START_PROFILING(export_clearclient_flock);
    file_lock_clear_client_file_lock(e->eid,lock_requested->client_ref,info);
    STOP_PROFILING(export_clearclient_flock);
    return 0;
}
/*
**______________________________________________________________________________
*/
/** reset all the locks from an owner
 *
 * @param e: the export managing the file or directory.
 * @param lock: the identifier of the client whose locks are to remove
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */
int export_clear_owner_file_lock(export_t *e, fid_t fid, ep_lock_t * lock_requested) {
    int status = -1;
    lv2_entry_t *lv2 = 0;
    list_t * p;
    rozofs_file_lock_t *lock_elt;
    char                 string[256];
    int                  updated = 0;
    
    START_PROFILING(export_clearowner_flock);
    if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid))) {
	if (errno==ENOENT) {
	  /* File does not exist. Just respond SUCCESS */
	  status = 0;
	  goto out;
	}	
	flock_request2string(string,fid,lock_requested);
        warning("EXPORT_LOOKUP_FID clear %s - %s", string, strerror(errno));
        goto out;
    }
    
    status = 0;

reloop:    
    /* Search the given lock */
    list_for_each_forward(p, &lv2->file_lock) {
      lock_elt = list_entry(p, rozofs_file_lock_t, next_fid_lock);
      if ((lock_elt->lock.client_ref == lock_requested->client_ref) &&
          (lock_elt->lock.owner_ref == lock_requested->owner_ref)) {
	  /* Found a lock to free */
  	  flock_request2string(string,fid,&lock_elt->lock);
	  lv2_cache_free_file_lock(e->eid,lock_elt);
	  lv2->nb_locks--;
          updated = 1;
	  if (list_empty(&lv2->file_lock)) {
	    lv2->nb_locks = 0;
	    // Remove it from the lru
	    lv2_cache_update_lru(e->lv2_cache, lv2);	    
	    break;
	  }
	  goto reloop;
      }       
    }    

out:
    /*
    ** Save persistent locks in rozofs specific extended attributte when configure
    */
    if (updated) {
      rozofs_save_flocks_in_xattr(e, lv2);
    }
    STOP_PROFILING(export_clearowner_flock);
    return status;
}
/*
**______________________________________________________________________________
*/
/** Old client pooling. This single poll message renews the lease of all the locks
** from any application for this client
 *
 * @param e: the export managing the file or directory.
 * @param lock: the lock to set/remove
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */
int export_poll_file_lock(export_t *e, ep_lock_t * lock_requested, ep_client_info_t * info) {

    START_PROFILING(export_poll_file_lock);
    file_lock_poll_client(e->eid,lock_requested->client_ref,info);
    STOP_PROFILING(export_poll_file_lock);
    return 0;
}
/*
**______________________________________________________________________________
*/
/** New cleint polling. This poll message renews every locks of the given
 ** client/woner/fid tupple
 **
 * @param e: the export managing the file or directory.
 * @param lock: the lock to set/remove
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */
int export_poll_owner_lock(export_t *e, fid_t fid, ep_lock_t * lock_requested, ep_client_info_t * info) {
    int                         result = 0;
    lv2_entry_t               * lv2 = 0;  
    fid_t                       fid_null;
    
    START_PROFILING(export_poll_owner_lock);

    /*
    ** Find out the lv2 entry for this FID
    */
    memset(fid_null, 0, sizeof(fid_t));
    if ((memcmp(fid_null, fid, sizeof(fid_t))==0) && (lock_requested->owner_ref == 0)) {
      /*
      ** FID is NULL and owner ref also. This poll is just to upgrade the client
      ** that owns no locks
      */
      result = file_lock_poll_owner(e->eid, NULL, lock_requested, info);
      goto out;
    }  

    if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid))) {
      /*
      ** FID is not given but owner is. The file has been deleted....
      */
      if (lock_requested->owner_ref != 0) {
         goto out;
      }
      /*
      ** neither FID not owner is given . Just update the client presence
      */
    } 
      
    result = file_lock_poll_owner(e->eid, lv2, lock_requested, info);

out:
    STOP_PROFILING(export_poll_owner_lock);
    return result;
}
/*
**______________________________________________________________________________
*/
/** set an extended attribute value for a file or directory.
 *
 * @param e: the export managing the file or directory.
 * @param fid: the id of the file or directory.
 * @param name: the extended attribute name.
 * @param value: the value of this extended attribute.
 * @param size: the size of a buffer to hold the value associated
 *  with this extended attribute.
 * @param flags: parameter can be used to refine the semantics of the operation.
 * @param symlink value returned when it makes sense
 * 
 * @return: On success, zero is returned.  On failure, -1 is returned.
 */
int export_setxattr(export_t *e, fid_t fid, char *name, const void *value, size_t size, int flags, epgw_setxattr_symlink_t *symlink) {
    int status = -1;
    lv2_entry_t *lv2 = 0;    
    int need_parent_update = 0;

    START_PROFILING(export_setxattr);

    if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid))) {
        severe("export_getattr failed: %s", strerror(errno));
        goto out;
    }

    /*
    ** Special XATTR to change the target of a symbolic link
    ** POSIX does not allow to change the target of a symbolic link
    */
    if ((strcmp(name,ROZOFS_ROOT_DIRSYMLINK)==0)||(strcmp(name,ROZOFS_USER_DIRSYMLINK)==0)) {
      status = set_rozofs_link_from_name(e,lv2,(char *)value,size,symlink);
      goto out;
    }           
    if (strcmp(name,ROZOFS_ROOT_SYMLINK)==0) {
      status = set_rozofs_link_from_fid(e,lv2,(char *)value,size,symlink);
      goto out;
    }   
    if ((strcmp(name,ROZOFS_XATTR)==0)||(strcmp(name,ROZOFS_USER_XATTR)==0)||(strcmp(name,ROZOFS_ROOT_XATTR)==0)) {
      status = set_rozofs_xattr(e,lv2,(char *)value,size);
      goto out;
    }  
    
    if (strcmp(name,POSIX_ACL_XATTR_ACCESS)==0)
    {
       int ret;
       ret = rozofs_acl_access_check(name,value,size,&lv2->attributes.s.attrs.mode);
       if ((ret == 0) || (ret == 1))
       {
	 /*
	 ** write child attributes on disk
	 */
	 need_parent_update = 1;
	 export_attr_thread_submit(lv2,e->trk_tb_p, 0 /* No sync */);
       }
       if (ret == 0)
       {
	 struct dentry entry;
	 entry.d_inode = lv2;
	 entry.trk_tb_p = e->trk_tb_p;         
	 rozofs_removexattr(&entry, name);         
	 status = 0;
	 goto out;
       }     
    }    
      
    {
      struct dentry entry;
      entry.d_inode = lv2;
      entry.trk_tb_p = e->trk_tb_p;
      rozofs_set_xattr_flag(&lv2->attributes.s.attrs.mode);
      if ((status = rozofs_setxattr(&entry, name, value, size, flags)) != 0) {
          goto out;
      }
    }
    need_parent_update = 1;
    status = 0;

out:
    if (need_parent_update)
    {
#ifdef ROZOFS_DIR_STATS
      /*
      ** Update the directory statistics
      */
      lv2_entry_t *plv2  = export_dir_get_parent(e,lv2);
      if (plv2 != NULL)
      {
         export_dir_async_write(e,plv2);
      }
#endif
    }    
    STOP_PROFILING(export_setxattr);

    return status;
}
/*
**______________________________________________________________________________
*/
/** remove an extended attribute from a file or directory.
 *
 * @param e: the export managing the file or directory.
 * @param fid: the id of the file or directory.
 * @param name: the extended attribute name.
 * 
 * @return: On success, zero is returned.  On failure, -1 is returned.
 */
int export_removexattr(export_t *e, fid_t fid, char *name) {
    int status = -1;
    lv2_entry_t *lv2 = 0;

    START_PROFILING(export_removexattr);

    if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid))) {
        severe("export_getattr failed: %s", strerror(errno));
        goto out;
    }

    {
      struct dentry entry;
      entry.d_inode = lv2;
      entry.trk_tb_p = e->trk_tb_p;
    
      if ((status = rozofs_removexattr(&entry, name)) != 0) {
          goto out;
      }
    }
#ifdef ROZOFS_DIR_STATS
      /*
      ** Update the directory statistics
      */
      lv2_entry_t *plv2  = export_dir_get_parent(e,lv2);
      if (plv2 != NULL)
      {
         export_dir_async_write(e,plv2);
      }
#endif

    status = 0;
out:
    STOP_PROFILING(export_removexattr);

    return status;
}
/*
**______________________________________________________________________________
*/
/** list extended attribute names from the lv2 regular file.
 *
 * @param e: the export managing the file or directory.
 * @param fid: the id of the file or directory.
 * @param list: list of extended attribute names associated with this file/dir.
 * @param size: the size of a buffer to hold the list of extended attributes.
 * 
 * @return: On success, the size of the extended attribute name list.
 * On failure, -1 is returned and errno is set appropriately.
 */
ssize_t export_listxattr(export_t *e, fid_t fid, void *list, size_t size) {
    ssize_t status = -1;
    lv2_entry_t *lv2 = 0;

    START_PROFILING(export_listxattr);

    if (!(lv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,e->lv2_cache, fid))) {
        severe("export_getattr failed: %s", strerror(errno));
        goto out;
    }
    {
      struct dentry entry;
      entry.d_inode = lv2;
      entry.trk_tb_p = e->trk_tb_p;
    
      if ((status = rozofs_listxattr(&entry, list,size)) != 0) {
          goto out;
      }
    }


out:
    STOP_PROFILING(export_listxattr);
    return status;
}

/*
int export_open(export_t * e, fid_t fid) {
    int flag;

    flag = O_RDWR;

    if (!(mfe = export_get_mfentry_by_id(e, fid))) {
        severe("export_open failed: export_get_mfentry_by_id failed");
        goto out;
    }

    if (mfe->fd == -1) {
        if ((mfe->fd = open(mfe->path, flag)) < 0) {
            severe("export_open failed for file %s: %s", mfe->path,
                    strerror(errno));
            goto out;
        }
    }

    mfe->cnt++;

    status = 0;
out:

    return status;
}

int export_close(export_t * e, fid_t fid) {
    int status = -1;
    mfentry_t *mfe = 0;
    DEBUG_FUNCTION;

    if (!(mfe = export_get_mfentry_by_id(e, fid))) {
        severe("export_close failed: export_get_mfentry_by_id failed");
        goto out;
    }

    if (mfe->cnt == 1) {
        if (close(mfe->fd) != 0) {
            goto out;
        }
        mfe->fd = -1;
    }

    mfe->cnt--;

    status = 0;
out:
    return status;
}
 */
