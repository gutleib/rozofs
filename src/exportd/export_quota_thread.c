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
#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h>
#include <inttypes.h>
#include <pthread.h>
#include <time.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/htable.h>
#include <rozofs/common/log.h>
#include "export.h"
#include "exportd.h"
#include "rozofs_quota.h"
#include "rozofs_quota_api.h"
#include <rozofs/core/disk_table_service.h>
#include <rozofs/common/types.h>
#include <rozofs/core/uma_dbg_api.h>


#define EXPORT_QUOTA_DELAY_WARN_SEC  (60*5)

#define START_PROFILING_TH(the_probe)\
    uint64_t tic, toc;\
    struct timeval tv;\
    {\
        the_probe[P_COUNT]++;\
        gettimeofday(&tv,(struct timezone *)0);\
        tic = MICROLONG(tv);\
    }

#define STOP_PROFILING_TH(the_probe)\
    {\
        gettimeofday(&tv,(struct timezone *)0);\
        toc = MICROLONG(tv);\
        the_probe[P_ELAPSE] += (toc - tic);\
    }

int export_fstat_init();

typedef struct _export_fstat_stats_t
{
    uint64_t  file_count;
    uint64_t  block_count;
    uint64_t  thread_update_count;
    uint64_t  eid_out_of_range_err;
    uint64_t  no_eid_err;
    uint64_t  negative_file_count_err;
    uint64_t  negative_block_count_err;
    uint64_t  open_err;
    uint64_t  read_err;
    uint64_t  write_err;
} export_fstat_stats_t;

/** stat of an export
 * these values are independent of volume
 */
typedef struct export_fstat_ctx_t {

    export_fstat_t memory;  /**< written by export non blocking thread  */
    export_fstat_t last_written;  /**< last written value by periodic thread  */
    uint64_t hquota;             /**< hardware quota               */
    uint64_t squota;             /**< software quota               */
    int    quota_exceeded_flag;  /**< assert to one if quota is exceeded  */
    time_t quota_exceeded_time;  /**< last time of quota exceeded  */
    /*
    ** field used for fast volume management
    */
    uint64_t hquota_fast;             /**< hardware quota               */
    int    quota_exceeded_flag_fast;  /**< assert to one if quota is exceeded  */
    time_t quota_exceeded_time_fast;  /**< last time of quota exceeded  */    
    
    int fd;
    char *pathname;     /**< full pathname of the stats file  */
} export_fstat_ctx_t;

export_fstat_stats_t export_fstat_stats;
time_t export_fstat_quota_delay = EXPORT_QUOTA_DELAY_WARN_SEC;
int export_fstat_thread_period_count;
export_fstat_ctx_t *export_fstat_table[EXPGW_EID_MAX_IDX+1] = {0};
int export_fstat_init_done = 0;
uint64_t export_fstat_poll_stats[2];

static pthread_t export_fstat_ctx_thread;

#define SHOW_STATS(probe) if (export_fstat_stats.probe) pChar += sprintf(pChar,"   %-24s:%llu\n",\
                    #probe,(unsigned long long int)export_fstat_stats.probe);

static char * show_export_fstat_thread_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"fstat_thread                      : display statistics\n");
  pChar += sprintf(pChar,"fstat_thread eid <value>          : display eid filesystem statistics\n");
  pChar += sprintf(pChar,"fstat_thread reset                : reset statistics\n");
  pChar += sprintf(pChar,"fstat_thread period [ <period> ]  : change thread period(unit is second)\n");  
  return pChar; 
}

char *show_export_fstat_entry(char *pChar,export_fstat_ctx_t *tab_p,uint16_t eid)
{
   export_t *exp;
   
   pChar += sprintf(pChar,"file statistics for eid %d(%p) :%s\n",eid,tab_p,tab_p->pathname);
   export_fstat_t *p;
   p = &tab_p->memory;
   pChar += sprintf(pChar," number of blocks (max/allocated) (mem) : %llu/%llu\n",(unsigned long long int) p->blocks,(unsigned long long int) p->blocks_thin);
   pChar += sprintf(pChar," number of files                  (mem) : %llu\n",(unsigned long long int) p->files);
   int i;
   pChar += sprintf(pChar,"   nb files per number of block\n");
   if (p->file_per_size[0] != 0) {
     pChar += sprintf(pChar,"\t[ %d .. %d [ \t%llu\n", 0, 1,(long long unsigned int)p->file_per_size[0]);   
   }
   for (i=1; i<ROZOFS_MAX_BLOCK_BITS; i++) {    
     if (p->file_per_size[i] != 0) {
       pChar += sprintf(pChar,"\t[ %llu .. %llu [ \t%llu\n", (1ULL<<(i-1)), (1ULL<<i),(long long unsigned int)p->file_per_size[i]);
     }
   }      
   
   p = &tab_p->last_written;
   pChar += sprintf(pChar," number of blocks (max/allocated) (disk): %llu/%llu\n",(unsigned long long int) p->blocks,(unsigned long long int) p->blocks_thin);
   pChar += sprintf(pChar," number of files                  (disk): %llu\n",(unsigned long long int) p->files);
   pChar += sprintf(pChar," hardware quota                         : %llu\n",(unsigned long long int) tab_p->hquota);
   pChar += sprintf(pChar," software quota                         : %llu\n",(unsigned long long int) tab_p->squota);
   pChar += sprintf(pChar," quota exceeded                         : %s\n",(tab_p->quota_exceeded_flag==0)?"NO":"YES");
   exp = exports_lookup_export(eid);
   if (exp != NULL)
   {
     /*
     ** check if there is a fast volume associated with the export
     */
     if (exp->volume_fast != NULL)
     {
       pChar += sprintf(pChar,"\nFast Volume statistics (volume %d):\n",exp->volume_fast->vid);
       pChar += sprintf(pChar," number of blocks                 (disk): %llu\n",(unsigned long long int) p->blocks_fast);
       pChar += sprintf(pChar," number of files                  (disk): %llu\n",(unsigned long long int) p->files_fast);
       pChar += sprintf(pChar," hardware quota                         : %llu\n",(unsigned long long int) tab_p->hquota_fast);
       pChar += sprintf(pChar," quota exceeded                         : %s\n",(tab_p->quota_exceeded_flag_fast==0)?"NO":"YES");          
     }
   
   }


   return pChar;
}
/*
**_______________________________________________________________
*/
char * show_export_fstat_quota_thread_stats_display(char *pChar)
{
     /*
     ** display the statistics of the thread
     */
     pChar += sprintf(pChar,"period     : %d second(s) \n",export_fstat_thread_period_count);
     pChar += sprintf(pChar," - activation counter:%llu\n",
              (long long unsigned int)export_fstat_poll_stats[P_COUNT]);
     pChar += sprintf(pChar," - average time (us) :%llu\n",
                      (long long unsigned int)(export_fstat_poll_stats[P_COUNT]?
		      export_fstat_poll_stats[P_ELAPSE]/export_fstat_poll_stats[P_COUNT]:0));
     pChar += sprintf(pChar," - total time (us)   :%llu\n",(long long unsigned int)export_fstat_poll_stats[P_ELAPSE]);
     
     
     
     pChar += sprintf(pChar,"statistics :\n");
    SHOW_STATS(  file_count);
    SHOW_STATS(  block_count);
    SHOW_STATS(  thread_update_count);
    SHOW_STATS(  eid_out_of_range_err);
    SHOW_STATS(  no_eid_err);
    SHOW_STATS(  negative_file_count_err);
    SHOW_STATS(  negative_block_count_err);
    SHOW_STATS(  open_err);
    SHOW_STATS(  read_err);
    SHOW_STATS(  write_err);
    return pChar;
}

/*
**_______________________________________________________________
*/
void show_export_fstat_thread(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int ret;
    int period;
    int eid;
    export_fstat_ctx_t *tab_p;
    
    
    if (argv[1] == NULL) {
      show_export_fstat_quota_thread_stats_display(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer()); 
      return;  	  
    }

    if (strcmp(argv[1],"reset")==0) {
      memset(&export_fstat_stats,0,sizeof(export_fstat_stats));
      pChar +=sprintf(pChar,"\nStatistics have been cleared\n");
      export_fstat_poll_stats[0] = 0;
      export_fstat_poll_stats[1] = 0;
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());  
      return;   
    }
    if (strcmp(argv[1],"period")==0) {   
	if (argv[2] == NULL) {
	show_export_fstat_thread_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;  	  
      }
      ret = sscanf(argv[2], "%d", &period);
      if (ret != 1) {
	show_export_fstat_thread_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;   
      }
      /*
      ** change the period of the thread
      */
      if (period == 0)
      {
        uma_dbg_send(tcpRef, bufRef, TRUE, "value not supported\n");
        return;
      }
      
      export_fstat_thread_period_count = period;
      uma_dbg_send(tcpRef, bufRef, TRUE, "Done\n");   	  
      return;
    }
    if (strcmp(argv[1],"eid")==0) {   
	if (argv[2] == NULL) {
	show_export_fstat_thread_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;  	  
      }
      ret = sscanf(argv[2], "%d", &eid);
      if (ret != 1) {
	show_export_fstat_thread_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;   
      }
      /*
      ** change the period of the thread
      */
      if (eid > EXPGW_EID_MAX_IDX)
      {
        uma_dbg_send_format(tcpRef, bufRef, TRUE, "value not supported, max value is %u\n",EXPGW_EID_MAX_IDX);
        return;
      }
      tab_p = export_fstat_table[eid];
      if (tab_p==NULL)
      {
        uma_dbg_send_format(tcpRef, bufRef, TRUE, "export %u does not exist\n",eid);
        return;            
      }
      show_export_fstat_entry(pChar,tab_p,(uint16_t)eid);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
      return;
    }

    show_export_fstat_thread_help(pChar);	
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());    
    return;
}

/*
**__________________________________________________________________
*/
/** Get pointer to the export statistics in memory
 *
 * @param eid: the export to get statistics from
 *
 * @return the pointer to the statitics of NULL 
 */
export_fstat_t * export_fstat_get_stat(uint16_t eid)
{
    export_fstat_ctx_t *tab_p;
    
   if (export_fstat_init_done == 0)
   {
      return NULL;
   }
   /*
   ** check the index of the eid
   */
   if (eid > EXPGW_EID_MAX_IDX) 
   {
      /*
      ** eid value is out of range
      */
      export_fstat_stats.eid_out_of_range_err++;
      return NULL;
   }

   tab_p = export_fstat_table[eid];
   if (tab_p== NULL)  
   {
      export_fstat_stats.no_eid_err++;
      return NULL;   
   } 

   return &tab_p->memory;
}
/*
**__________________________________________________________________
*/
/** Delete the number of files in file system
 *
 * @param eid: the export to update
 * @param n: number of files
   @param vid_fast: reference of a fast volume (0: not significant)

 * @return always 0
 */
int export_fstat_delete_files(uint16_t eid, uint32_t n,uint8_t vid_fast) 
{
    export_fstat_ctx_t *tab_p;
    
   if (export_fstat_init_done == 0)
   {
      return 0;
   }
   /*
   ** check the index of the eid
   */
   if (eid > EXPGW_EID_MAX_IDX) 
   {
      /*
      ** eid value is out of range
      */
      export_fstat_stats.eid_out_of_range_err++;
      return 0;
   }
   if (export_fstat_table[eid]== NULL)  
   {
      export_fstat_stats.no_eid_err++;
      return 0;   
   } 
   tab_p = export_fstat_table[eid];

   /*
   ** Releasing more files than existing !!!
   */
   if (n > tab_p->memory.files) {
      export_fstat_stats.negative_file_count_err++;
      n = tab_p->memory.files;
   }
   tab_p->memory.files -= n;
   
   if (n > tab_p->memory.file_per_size[0]) {
     n = tab_p->memory.file_per_size[0]; 
   }
   tab_p->memory.file_per_size[0] -= n; 
   
   if (vid_fast)
   {
     if (n > tab_p->memory.files_fast) {
	export_fstat_stats.negative_file_count_err++;
	n = tab_p->memory.files_fast;
     }
     tab_p->memory.files_fast -= n;      
   }
    return 0;
}
/*
** Compute the number of leading 0
*/
static inline int my_lzcnt64(uint64_t val) {
  int res = 0;
  
  if (val > 0xFFFFFFFF) {
    val = val >> 32;
  }
  else {
    res += 32;
  }
  
  if (val > 0xFFFF) {
    val = val >> 16;
  }
  else {  
    res += 16;
  }
  
  if (val > 0xFF) {
    val = val >> 8;
  }  
  else { 
    res += 8;
  }  

  if (val > 0xF) {
    val = val >> 4;
  }  
  else {
    res += 4;
  }   
  
  if (val > 3) {
    val = val >> 2;
  }  
  else { 
    res += 2;
  } 
  
  if (val > 1) {
    val = val >> 1;
  }  
  else {
    res += 1;
  } 
  if (val != 1) {
    res += 1;
  }      
  return res;   
}
/*
**__________________________________________________________________
*/
/** Create some empty files 
 *
 * @param eid: the export to update
 * @param n: number of created files
   @param vid_fast: reference of a fast volume (0: not significant)
  
 * @return 0 on success -1 otherwise
 */
int export_fstat_create_files(uint16_t eid, uint32_t n,uint8_t vid_fast) {
    export_fstat_ctx_t *tab_p;
    
   if (export_fstat_init_done == 0)
   {
      return 0;
   }
   /*
   ** check the index of the eid
   */
   if (eid > EXPGW_EID_MAX_IDX) 
   {
      /*
      ** eid value is out of range
      */
      export_fstat_stats.eid_out_of_range_err++;
      return 0;
   }
   if (export_fstat_table[eid]== NULL)  
   {
      export_fstat_stats.no_eid_err++;
      return 0;   
   } 
   tab_p = export_fstat_table[eid];
   tab_p->memory.file_per_size[0] += n;  
   tab_p->memory.files += n;   
   if (vid_fast)  tab_p->memory.files_fast += n;     
   return 0;
}       
/*
**__________________________________________________________________
*/
/** update the number of blocks in file system
 *
  @param eid: the export to update
  @param newblocks: new number of blocks
  @param oldblocks: old number of blocks
  @param thin_provisioning: assert to 1 if export is configured with thin provisioning
   @param vid_fast: reference of a fast volume (0 is not significant)

 * @return 0 on success -1 otherwise
 */
int export_fstat_update_blocks(uint16_t eid, uint64_t newblocks, uint64_t oldblocks,int thin_provisioning,uint8_t vid_fast) {
    int status = -1;
    export_fstat_ctx_t *tab_p;
    time_t timecur;
    long long int n = newblocks - oldblocks;

   if (n == 0) return 0;   
    
   if (export_fstat_init_done == 0)
   {
      return 0;
   }
   /*
   ** check the index of the eid
   */
   if (eid > EXPGW_EID_MAX_IDX) 
   {
      /*
      ** eid value is out of range
      */
      export_fstat_stats.eid_out_of_range_err++;
      return 0;
   }
   if (export_fstat_table[eid]== NULL)  
   {
      export_fstat_stats.no_eid_err++;
      return 0;   
   } 
   tab_p = export_fstat_table[eid];
   
   
   /*
   ** Update number of file per file size
   */
   uint32_t old_idx = 64 - my_lzcnt64(oldblocks);
   uint32_t new_idx = 64 - my_lzcnt64(newblocks);
   if (old_idx != new_idx) {
     if (tab_p->memory.file_per_size[old_idx] > 0) {
       tab_p->memory.file_per_size[old_idx] -= 1;
     }
     tab_p->memory.file_per_size[new_idx] += 1;
   }
  
    /*
    ** Releasing some blocks 
    */
    if (n<0) {
    
      n = -n;

      /*
      ** Releasing more blocks than allocated !!!
      */
      if (n > tab_p->memory.blocks) {
        severe("export %s blocks %"PRIu64" files %"PRIu64". Releasing %lld blocks",
	      tab_p->pathname, tab_p->memory.blocks, tab_p->memory.files, n); 
        n = tab_p->memory.blocks;
      }

      tab_p->memory.blocks -= n;
      /*
      ** check the case of the fast volume
      */
      if (vid_fast)
      {
	if (n > tab_p->memory.blocks) {
          severe("export %s blocks %"PRIu64" files %"PRIu64". Releasing %lld blocks for fast volume",
		tab_p->pathname, tab_p->memory.blocks_fast, tab_p->memory.files_fast, n); 
          n = tab_p->memory.blocks_fast;
	}

	tab_p->memory.blocks_fast -= n;            
      }
    }
    else 
    {
      /*
      ** do that control only for exportd that do not support thin provisioning
      */
      if (thin_provisioning == 0)
      {
	if (tab_p->hquota > 0 && tab_p->memory.blocks + n > tab_p->hquota) 
	{
           if (tab_p->quota_exceeded_flag == 0)
	   {
	     tab_p->quota_exceeded_time = time(NULL); 
	   }
	   /*
	   ** send a warning if is time to do it
	   */
	   timecur = time(NULL);
	   if (( tab_p->quota_exceeded_flag == 1) || 
	      ((timecur -tab_p->quota_exceeded_time ) > export_fstat_quota_delay))
	   {
              warning("quota exceed: %llu over %llu", tab_p->memory.blocks + n,
                       (long long unsigned int)tab_p->hquota);
  	      tab_p->quota_exceeded_time = time(NULL); 
	      tab_p->quota_exceeded_flag = 1;
           }
           errno = EDQUOT;
           goto out;
	}
	else
	{
          tab_p->quota_exceeded_flag = 0;
	}
      }
      tab_p->memory.blocks += n; 
      /*
      ** check the case of the fast volume
      */
      if (vid_fast)
      {
	if (thin_provisioning == 0)
	{
	  if (tab_p->hquota_fast > 0 && tab_p->memory.blocks_fast + n > tab_p->hquota_fast) 
	  {
             if (tab_p->quota_exceeded_flag_fast == 0)
	     {
	       tab_p->quota_exceeded_time_fast = time(NULL); 
	     }
	     /*
	     ** send a warning if is time to do it
	     */
	     timecur = time(NULL);
	     if (( tab_p->quota_exceeded_flag_fast == 1) || 
		((timecur -tab_p->quota_exceeded_time_fast ) > export_fstat_quota_delay))
	     {
        	warning("quota exceed for fast volume: %llu over %llu", tab_p->memory.blocks_fast + n,
                	 (long long unsigned int)tab_p->hquota_fast);
  		tab_p->quota_exceeded_time_fast = time(NULL); 
		tab_p->quota_exceeded_flag_fast = 1;
             }
             /*
	     ** do report quota error: the control is done at creation file only
	     */
	  }
	  else
	  {
            tab_p->quota_exceeded_flag_fast = 0;
	  }
	}
	tab_p->memory.blocks_fast += n;       
      }           
    }
    status = 0;
out:
    return status;
}

/*
**__________________________________________________________________
*/
/** update the number of blocks in file system
 *
 * @param eid: the export to update
 * @param n: number of blocks
 *
 * @return 0 on success -1 otherwise
 */
int expthin_fstat_update_blocks(uint16_t eid,uint32_t nb_blocks, int dir) {
    int status = -1;
    export_fstat_ctx_t *tab_p;
    time_t timecur;
    long long int n = nb_blocks;

   if (n == 0) return 0;   
    
   if (export_fstat_init_done == 0)
   {
      return 0;
   }
   /*
   ** check the index of the eid
   */
   if (eid > EXPGW_EID_MAX_IDX) 
   {
      /*
      ** eid value is out of range
      */
      export_fstat_stats.eid_out_of_range_err++;
      return 0;
   }
   if (export_fstat_table[eid]== NULL)  
   {
      export_fstat_stats.no_eid_err++;
      return 0;   
   } 
   tab_p = export_fstat_table[eid];
  
    /*
    ** Check if we release or add some blocks 
    */
    if (dir<0) {    
      /*
      ** Releasing more blocks than allocated !!!
      */
      if (n > tab_p->memory.blocks_thin) {
        warning("export thin %s blocks %"PRIu64" files %"PRIu64". Releasing %lld blocks",
	      tab_p->pathname, tab_p->memory.blocks_thin, tab_p->memory.files, n); 
        n = tab_p->memory.blocks_thin;
      }

      tab_p->memory.blocks_thin -= n;
    }
    else 
    {

      if (tab_p->hquota > 0 && tab_p->memory.blocks_thin + n > tab_p->hquota) 
      {
         if (tab_p->quota_exceeded_flag == 0)
	 {
	   tab_p->quota_exceeded_time = time(NULL); 
	 }
	 /*
	 ** send a warning if is time to do it
	 */
	 timecur = time(NULL);
	 if (( tab_p->quota_exceeded_flag == 1) || 
	    ((timecur -tab_p->quota_exceeded_time ) > export_fstat_quota_delay))
	 {
            warning("(thin provisioning) quota exceed: %llu over %llu", tab_p->memory.blocks_thin + n,
                     (long long unsigned int)tab_p->hquota);
  	    tab_p->quota_exceeded_time = time(NULL); 
	    tab_p->quota_exceeded_flag = 1;
         }
         errno = EDQUOT;
         goto out;
      }
      else
      {
        tab_p->quota_exceeded_flag = 0;
      }
      tab_p->memory.blocks_thin += n;      
    }
    status = 0;
out:
    return status;
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
   @param create_flag : assert to 1 if  file MUST be created
   @param hquota : hardware quota in blocks
   @param squota : software quota in blocks
   @param hquota_fast : hardware quota in blocks for fast volume
   
   @retval <> NULL: pointer to the attributes tracking table
   @retval == NULL : error (see errno for details)
*/
void *export_fstat_alloc_context(uint16_t eid, char *root_path,uint64_t hquota,uint64_t squota,uint64_t hquota_fast,int create)
{
   export_fstat_ctx_t *tab_p = NULL; 
   int ret; 
   int fd;
   /*
   ** if the init of the quota module has not yet been done do it now
   */

   if (export_fstat_init_done == 0)
   {
     ret = export_fstat_init();
     if (ret < 0) return NULL;
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
   if (export_fstat_table[eid]!= NULL)
   {
      /*
      ** the context is already allocated: nothing more to do:
      **  note: it is not foreseen the change the root path of an exportd !!
      */
      tab_p = export_fstat_table[eid];
      tab_p->squota = squota;
      tab_p->hquota = hquota; 
      tab_p->hquota_fast = hquota_fast; 
      return (void*)tab_p;
   }
   
   tab_p = malloc(sizeof(export_fstat_ctx_t));
   if (tab_p == NULL)
   {
     /*
     ** out of memory
     */
     return NULL;
   }
   memset(tab_p,0,sizeof(export_fstat_ctx_t));
   /*
   ** duplicate the path
   */
   tab_p->pathname = strdup(root_path);
   if (tab_p->pathname == NULL)
   {
     severe("out of memory");
     goto error;
   }
   if (create)
   {
      /*
      ** need to create an empty file
      */
      if ((fd = open(tab_p->pathname, O_RDWR/* | NO_ATIME */| O_CREAT, S_IRWXU)) < 1) {
          export_fstat_stats.open_err++;
          return NULL;
      }
      if (write(fd, &tab_p->memory, sizeof (export_fstat_t)) != sizeof (export_fstat_t)) {
          close(fd);
          export_fstat_stats.write_err++;
          return NULL;
      }
      close(fd);
   }
   else
   {
      /*
      ** the context already exists, need to read it from disk
      */
      if ((fd = open(tab_p->pathname, O_RDWR /*| NO_ATIME*/, S_IRWXU)) < 1) {
          export_fstat_stats.open_err++;
          return NULL;
      }
      int len = read(fd, &tab_p->memory, sizeof (export_fstat_t));
      close(fd);      
      if (len < (2*sizeof (uint64_t))) {
          export_fstat_stats.read_err++;
          return NULL;
      }
      memcpy(&tab_p->last_written,&tab_p->memory,sizeof(export_fstat_t));      
   }
   /*
   ** everything is fine, so store the reference of the context in the table at the index of the eid
   */
   export_fstat_table[eid]= tab_p;
   tab_p->squota = squota;
   tab_p->hquota = hquota;   
   tab_p->hquota_fast = hquota_fast; 
   return (void*)tab_p;

error:
  warning("export_fstat_release_context(tab_p) not yet implemented");
   return NULL;
}
/*
**__________________________________________________________________
*/
/** update the number of blocks in file system
 *
 * @param eid: the export to update
 * @param n: number of blocks
 *
 * @return 0 on success -1 otherwise
 */
void export_fstat_thread_update(export_fstat_ctx_t *tab_p)
{

    int fd;
   /*
   ** check if there is a change
   */
   if ((tab_p->memory.blocks!= tab_p->last_written.blocks) || (tab_p->memory.files!= tab_p->last_written.files) 
       || (tab_p->memory.blocks_thin!= tab_p->last_written.blocks_thin)
       || (tab_p->memory.blocks_fast!= tab_p->last_written.blocks_fast) || (tab_p->memory.files_fast!= tab_p->last_written.files_fast))
   {
      if ((fd = open(tab_p->pathname, O_RDWR /*| NO_ATIME*/ | O_CREAT, S_IRWXU)) < 1) {
          export_fstat_stats.open_err++;
          return ;
      }
      if (write(fd, &tab_p->memory, sizeof (export_fstat_t)) != sizeof (export_fstat_t)) {
          close(fd);
          export_fstat_stats.write_err++;
          return ;
      }
      close(fd);      
      export_fstat_stats.thread_update_count++;
      memcpy(&tab_p->last_written,&tab_p->memory,sizeof(export_fstat_t));      
   }
}
/*
 *_______________________________________________________________________
 */
/** writeback thread
 */
 #define EXPORT_FSTAT_PTHREAD_FREQUENCY_SEC 1
static void *export_fstat_thread(void *v) {

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    int export_fstat_thread_period_current_count = 0;
    // Set the frequency of calls
    struct timespec ts = {EXPORT_FSTAT_PTHREAD_FREQUENCY_SEC, 0};
   int i;

    uma_dbg_thread_add_self("Quota");
    
    export_fstat_thread_period_count = EXPORT_FSTAT_PTHREAD_FREQUENCY_SEC*5;
    export_fstat_poll_stats[0] = 0;
    export_fstat_poll_stats[1] = 0;
    info("quota periodic thread started ");

    for (;;) {
	if (export_fstat_init_done == 0)  nanosleep(&ts, NULL);
        export_fstat_thread_period_current_count++;
	if (export_fstat_thread_period_current_count >= export_fstat_thread_period_count)
	{
          START_PROFILING_TH(export_fstat_poll_stats);
          for (i = 0; i < EXPGW_EID_MAX_IDX;i++)
	  {
	     
	     if (export_fstat_table[i]==NULL) continue;
	      export_fstat_thread_update(export_fstat_table[i]);  
	  }

	  STOP_PROFILING_TH(export_fstat_poll_stats);
	  export_fstat_thread_period_current_count = 0;
	}
        nanosleep(&ts, NULL);
    }
    return 0;
}
/*
**__________________________________________________________________
*/
/**
*   Init of the quota periodic thread of RozoFS

    @param none
    
    @retval 0 on success
    @retval -1 on error
*/
int export_fstat_init()
{

    if (export_fstat_init_done == 1) return 0;
    memset(&export_fstat_stats,0,sizeof(export_fstat_stats));
    /*
    ** create the period thread
    */
    if ((errno = pthread_create(&export_fstat_ctx_thread, NULL,
          export_fstat_thread, NULL)) != 0) {
      severe("can't create fstat periodic thread %s", strerror(errno));
      return -1;
    }
    export_fstat_init_done = 1;
  return 0;    
}




