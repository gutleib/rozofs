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

#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/rpc/export_profiler.h>
#include <rozofs/common/export_track.h>
#include <rozofs/rpc/epproto.h>
#include <rozofs/rpc/mclient.h>

#include "config.h"
#include "export.h"
#include "cache.h"
#include "mdirent.h"
#include "xattr_main.h"

/*____________________________________________________________
*/
/**
*  Reload in memory the files that have not yet been deleted

   @param e : pointer to the export structure
   
*/
int loop_fdl = 0;

extern uint64_t export_rm_bins_reload_count;
extern uint64_t export_rm_bins_reloaded_size;
int export_load_rmfentry(export_t * e) 
{
   int ret=0;
   int user_id;
   uint64_t count = 0;
   uint64_t file_id;
   rozofs_inode_t inode;
   int i;
   rmfentry_disk_t trash_entry;
   exp_trck_top_header_t *tracking_trash_p; 
   exp_trck_header_memory_t *slice_hdr_p;
   exp_trck_file_header_t tracking_buffer;
   rmfentry_t *rmfe;   
   time_t      when = time(NULL)+common_config.deletion_delay;   
   /*
   ** get the pointer to the tracking context associated with the 
   ** export
   */
   tracking_trash_p = e->trk_tb_p->tracking_table[ROZOFS_TRASH];   
   /*
   ** go through all the slices of the export check for all the file
   ** that are under deletion 
   ** The slices correspond to the case of the trash only
   */ 
   for (user_id = 0; user_id < EXP_TRCK_MAX_USER_ID; user_id++)
   {
     inode.s.usr_id = user_id;
     file_id = 0;
    
     /*
     ** read the main tracking file of each slices: the main tracking file contains the 
     ** first and list index of individual tracking file that contains the information
     ** relative to the file to delete. There are a maximum of 2044 files per tracking
     * file
     */
     slice_hdr_p = tracking_trash_p->entry_p[user_id];
     for (file_id = slice_hdr_p->entry.first_idx; file_id <= slice_hdr_p->entry.last_idx; file_id++)
     {
       ret = exp_metadata_get_tracking_file_header(tracking_trash_p,user_id,file_id,&tracking_buffer,NULL);
       if (ret < 0)
       {
	 if (errno != ENOENT)
	 {
            severe("error while main tracking file header of slice %d %s\n",user_id,strerror(errno));
	    continue;
	 }
	 ret = 0;
	 continue;
       }
       /*
       ** get the current count within the tracking file
       */
       {
          while(loop_fdl)
	  {
	     sleep(5);
	     severe("FDL bug wait for gdb");
	  }
       }
       count +=exp_metadata_get_tracking_file_count(&tracking_buffer);
       inode.s.file_id = file_id;

       for (i = 0; i < EXP_TRCK_MAX_INODE_PER_FILE; i++)
       {
          inode.s.idx = i;
	  if (tracking_buffer.inode_idx_table[i] == 0xffff) continue;
	  ret = exp_metadata_read_attributes(tracking_trash_p,&inode,&trash_entry,sizeof(trash_entry));
	  if (ret < 0)
	  {
	    severe("error while reading attributes at idx %d for trash slice %d in file %llu: %s\n",
	            inode.s.idx,inode.s.usr_id,
	            (long long unsigned int)inode.s.file_id,
		    strerror(errno));
		    continue;
	  }
	  /*
	  ** allocate memory for the file to delete
	  */
            for(;;)
	    {
              rmfe = xmalloc(sizeof (rmfentry_t));
	      if (rmfe == NULL)
	      {
		 /*
		 ** out of memory: just wait for a while and then retry
		 */
		 sleep(2);
	      }
	      break;
	    }
#if 0
	    {
	       char buf_fid[64];
	       rozofs_uuid_unparse(trash_entry.fid,buf_fid);
	       severe("FDL slice %u file %llu index %d  trash fid %s ",user_id,file_id,i, buf_fid);
	    }
#endif
            memcpy(rmfe->fid, trash_entry.fid, sizeof (fid_t));
            rmfe->cid = trash_entry.cid;
            rmfe->size = trash_entry.size;
            memcpy(rmfe->initial_dist_set, trash_entry.initial_dist_set,
                    sizeof (sid_t) * ROZOFS_SAFE_MAX);
            memcpy(rmfe->current_dist_set, trash_entry.current_dist_set,
                    sizeof (sid_t) * ROZOFS_SAFE_MAX);
            memcpy(rmfe->trash_inode,trash_entry.trash_inode,sizeof(fid_t));
            list_init(&rmfe->list);
	    rmfe->time = when;
	    /*
	    **  Compute hash value for this fid
	    */
            uint32_t hash = rozofs_storage_fid_slice(trash_entry.fid);

            export_rm_bins_reload_count++;
            export_rm_bins_reloaded_size += rmfe->size;
	  
            /* Acquire lock on bucket trash list
	    */
            if ((errno = pthread_rwlock_wrlock
                    (&e->trash_buckets[hash].rm_lock)) != 0) {
                severe("pthread_rwlock_wrlock failed: %s", strerror(errno));
                // Best effort
            }
            /*
	    ** Check size of file : TODO: cannot be done here since the 
	    ** file size is not save on disk
	    */
            if (trash_entry.size >= RM_FILE_SIZE_TRESHOLD) {
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
       ** try the next
       */
     }   
   }
   return ret;
}

/*____________________________________________________________
*/
/**
*  Reload in memory the files that could have their fid recycled

   @param e : pointer to the export structure
   
*/
extern uint64_t export_recycle_pending_count;
extern uint64_t export_fid_recycle_reload_count;
extern int export_fid_recycle_ready;
int export_load_recycle_entry(export_t * e) 
{
   int ret=0;
   int user_id;
   uint64_t count = 0;
   uint64_t file_id;
   rozofs_inode_t inode;
   int i;
   recycle_disk_t recycle_entry;
   exp_trck_top_header_t *tracking_recycle_p; 
   exp_trck_header_memory_t *slice_hdr_p;
   exp_trck_file_header_t tracking_buffer;
   recycle_mem_t *rmfe; 
   rozofs_inode_t * fake_inode_p;
   
   if (common_config.fid_recycle == 0) return 0;
     
   /*
   ** get the pointer to the tracking context associated with the 
   ** export
   */
   tracking_recycle_p = e->trk_tb_p->tracking_table[ROZOFS_RECYCLE];   
   /*
   ** go through all the slices of the export check for all the file
   ** that are under recycle 
   */ 
   for (user_id = 0; user_id < EXP_TRCK_MAX_USER_ID; user_id++)
   {
     inode.s.usr_id = user_id;
     file_id = 0;
    
     /*
     ** read the main tracking file of each slices: the main tracking file contains the 
     ** first and list index of individual tracking file that contains the information
     ** relative to the file to delete. There are a maximum of 2044 files per tracking
     * file
     */
     slice_hdr_p = tracking_recycle_p->entry_p[user_id];
     for (file_id = slice_hdr_p->entry.first_idx; file_id <= slice_hdr_p->entry.last_idx; file_id++)
     {
       ret = exp_metadata_get_tracking_file_header(tracking_recycle_p,user_id,file_id,&tracking_buffer,NULL);
       if (ret < 0)
       {
	 if (errno != ENOENT)
	 {
            severe("error while main tracking file header of slice %d %s\n",user_id,strerror(errno));
	    continue;
	 }
	 ret = 0;
	 continue;
       }
       /*
       ** get the current count within the tracking file
       */

       count +=exp_metadata_get_tracking_file_count(&tracking_buffer);
       inode.s.file_id = file_id;

       for (i = 0; i < EXP_TRCK_MAX_INODE_PER_FILE; i++)
       {
          inode.s.idx = i;
	  if (tracking_buffer.inode_idx_table[i] == 0xffff) continue;
	  ret = exp_metadata_read_attributes(tracking_recycle_p,&inode,&recycle_entry,sizeof(recycle_entry));
	  if (ret < 0)
	  {
	    severe("error while reading attributes at idx %d for recycle slice %d in file %llu: %s\n",
	            inode.s.idx,inode.s.usr_id,
	            (long long unsigned int)inode.s.file_id,
		    strerror(errno));
		    continue;
	  }
	  /*
	  ** allocate memory for the file to recycle
	  */
            for(;;)
	    {
              rmfe = malloc(sizeof (recycle_mem_t));
	      if (rmfe == NULL)
	      {
		 /*
		 ** out of memory: just wait for a while and then retry
		 */
		 sleep(2);
	      }
	      break;
	    }
	    export_fid_recycle_reload_count++;
	    memcpy(rmfe->fid, recycle_entry.fid, sizeof (fid_t));
	    memcpy(rmfe->recycle_inode,recycle_entry.recycle_inode,sizeof(fid_t));
            list_init(&rmfe->list);

	    fake_inode_p =  (rozofs_inode_t *)recycle_entry.fid;   
	    list_push_back(&e->recycle_buckets[fake_inode_p->s.usr_id].rmfiles, &rmfe->list);
       }
       /*
       ** try the next
       */
     }   
   }
   /*
   ** indicates that fid recycle is now effective
   */
   export_fid_recycle_ready = 1;
   return ret;
}
