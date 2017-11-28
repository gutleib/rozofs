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
#include <rozofs/core/uma_dbg_api.h>

#include "mdir.h"
#include "mdirent.h"
#include "export.h"
#include "exportd.h"


/*
** pointer to the dirent write back cache
*/
dirent_writeback_entry_t   *dirent_writeback_cache_p = NULL;
int dirent_writeback_cache_initialized = 0;
int dirent_writeback_cache_enable = 0;
uint64_t dirent_wbcache_hit_counter = 0;
uint64_t dirent_wbcache_miss_counter = 0;
uint64_t dirent_wb_cache_write_bytes_count = 0;
uint64_t dirent_wb_write_count = 0;
uint64_t dirent_wb_write_chunk_count = 0;  /**< incremented each time a chunk need to be flushed for making some room */
uint64_t dirent_wbcache_flush_counter = 0;
uint64_t dirent_wbcache_invalidate_counter = 0;
uint64_t dirent_wb_total_chunk_write_cpt = 0;
uint64_t dirent_wb_total_chunk_write_only_cpt = 0;

int dirent_wbcache_thread_period_count;
uint64_t dirent_wbcache_poll_stats[2];
 uint64_t dirent_wb_cache_th_write_bytes_count = 0;
 uint64_t dirent_wb_write_th_count = 0;
 
static pthread_t dirent_wbcache_ctx_thread;
int      export_tracking_thread_period_count;   /**< current period in seconds  */
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

static char * show_wbcache_thread_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"dirent_wbthread reset                : reset statistics\n");
  pChar += sprintf(pChar,"dirent_wbthread disable              : disable writeback dirent cache\n");
  pChar += sprintf(pChar,"dirent_wbthread enable               : enable writeback dirent cache\n");
  pChar += sprintf(pChar,"dirent_wbthread period [ <period> ]  : change thread period(unit is second)\n");  
  return pChar; 
}
/*
**_______________________________________________________________
*/
char * show_wbcache_thread_stats_display(char *pChar)
{
     /*
     ** display the statistics of the thread
     */
     pChar += sprintf(pChar,"period     : %d second(s) \n",dirent_wbcache_thread_period_count);
     pChar += sprintf(pChar,"statistics :\n");
     pChar += sprintf(pChar," - wr chunk counter  :%llu\n",
              (long long unsigned int)dirent_wb_total_chunk_write_cpt);
     pChar += sprintf(pChar," - write (hit/miss)  :%llu/%llu\n",
              (long long unsigned int)dirent_wbcache_hit_counter,
              (long long unsigned int)dirent_wbcache_miss_counter);

     pChar += sprintf(pChar," - chunk flush count :%llu/%llu\n",
              (long long unsigned int)dirent_wb_write_chunk_count, (long long unsigned int)dirent_wb_total_chunk_write_only_cpt);

     /*
     ** read and clear counters update
     */
     dirent_wbcache_hit_counter = 0;
     dirent_wbcache_miss_counter = 0;
     dirent_wb_write_chunk_count = 0;
     dirent_wb_total_chunk_write_cpt = 0;



     pChar += sprintf(pChar," - activation counter:%llu\n",
              (long long unsigned int)dirent_wbcache_poll_stats[P_COUNT]);
     pChar += sprintf(pChar," - average time (us) :%llu\n",
                      (long long unsigned int)(dirent_wbcache_poll_stats[P_COUNT]?
		      dirent_wbcache_poll_stats[P_ELAPSE]/dirent_wbcache_poll_stats[P_COUNT]:0));
     pChar += sprintf(pChar," - total time (us)   :%llu\n",(long long unsigned int)dirent_wbcache_poll_stats[P_ELAPSE]);
     pChar+=sprintf(pChar,"total Write            : memory %llu MBytes (%llu Bytes) requests %llu\n\n",
             (long long unsigned int) dirent_wb_cache_th_write_bytes_count / (1024*1024),
             (long long unsigned int) dirent_wb_cache_th_write_bytes_count,
             (long long unsigned int) dirent_wb_write_th_count);
     return pChar;


}

/**
*  Get the hash idx for the writeback cache

   @param fid
   
   @retval index in the writeback cache
*/
static inline int dirent_wbcache_get_index(void *fid,char *path)
{

    uint32_t h;

    unsigned char *d = (unsigned char *) fid;
    int i = 0;

    h = 2166136261U;
    /*
     ** hash on name
     */
    for (i=0; i < sizeof(fid_t); d++, i++) {
        h = (h * 16777619)^ *d;

    }
    d = (unsigned char *)path;
    for (i=0; *d != 0; d++, i++) {
        h = (h * 16777619)^ *d;

    }
    return (int)(h%DIRENT_CACHE_MAX_ENTRY);
}
/*
**_______________________________________________________________
*/
/**
* check if there is some write pending for the cache entry

  @param cache_p : pointer to the cache entry
  
  @retval 1 : write pending
  @retval 0 : no write pending
*/ 
static inline int dirent_wbcache_check_write_pending(dirent_writeback_entry_t  *cache_p)
{
   int i;
   for (i = 0; i < DIRENT_CACHE_MAX_CHUNK; i++)
   {
      if (cache_p->chunk[i].wr_cpt != 0) return 1;
   }
   return 0;
 }
/*
**_______________________________________________________________
*/
void show_wbcache_thread(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int ret;
    int period;
    
    
    if (argv[1] == NULL) {
      show_wbcache_thread_stats_display(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer()); 
      return;  	  
    }

    if (strcmp(argv[1],"reset")==0) {
      pChar = show_wbcache_thread_stats_display(pChar);
      pChar +=sprintf(pChar,"\nStatistics have been cleared\n");
      dirent_wbcache_poll_stats[0] = 0;
      dirent_wbcache_poll_stats[1] = 0;
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());  
      return;   
    }
    if (strcmp(argv[1],"period")==0) {   
	if (argv[2] == NULL) {
	show_wbcache_thread_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;  	  
      }
      ret = sscanf(argv[2], "%d", &period);
      if (ret != 1) {
	show_wbcache_thread_help(pChar);	
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
      
      dirent_wbcache_thread_period_count = period;
      uma_dbg_send(tcpRef, bufRef, TRUE, "Done\n");   	  
      return;
    }
    if (strcmp(argv[1],"disable")==0) {   
	dirent_writeback_cache_enable = 0;
	uma_dbg_send(tcpRef, bufRef, TRUE,"dirent writeback cache is disabled");   
	return;   
    }
    if (strcmp(argv[1],"enable")==0) {   
	dirent_writeback_cache_enable = 1;
	uma_dbg_send(tcpRef, bufRef, TRUE,"dirent writeback cache is enabled");   
	return;   
    }
    show_wbcache_thread_help(pChar);	
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());    
    return;
}


/*
 *_______________________________________________________________________
 */
/**
* flush the content of a cache entry on disk

  @param cache_p: pointer to the cache entry
*/
int dirent_wbcache_diskflush(dirent_writeback_entry_t  *cache_p)
{
  int fd = -1;
  int flag = O_WRONLY | O_CREAT | O_NOATIME;
  dirent_chunk_cache_t *chunk_p;
  int i;
  int status = -1;
  char path[PATH_MAX];

  /*
  ** take the lock associated with the entry
  */
  if ((errno = pthread_rwlock_wrlock(&cache_p->lock)) != 0) {
      severe("can't lock writeback cache entry: %s", strerror(errno));
      return -1;
  }
  /*
  ** check if the flush already occur: race between thread and check for flush
  */
  if ((cache_p->wr_cpt == 0)&&(dirent_wbcache_check_write_pending(cache_p) == 0))
  {
    if ((errno = pthread_rwlock_unlock(&cache_p->lock)) != 0) {
	severe("can't lock writeback cache entry: %s", strerror(errno));
	return -1;
    }
    return 0;     
  }

  /*
  ** open the dirent file
  */
  mdirent_resolve_path(dirent_export_root_path,cache_p->dir_fid,(char*)cache_p->pathname,path);
  if ((fd = open(path, flag, S_IRWXU)) == -1) 
  {
     severe("cannot open %s",path);
     goto error;  
  } 
  /*
  ** goto the entry and find out the section to write on disk
  */
   if (cache_p->wr_cpt != 0)
   {
     if (pwrite(fd, cache_p->dirent_header, cache_p->size, 0) != cache_p->size) 
     {
       severe("bad write returned value for %s (len %d): error %s",cache_p->pathname,cache_p->size,strerror(errno));
       goto error;        
     } 
     dirent_wb_cache_th_write_bytes_count+= cache_p->size;
     dirent_wb_write_th_count++;
   }
   else
   {
      dirent_wb_total_chunk_write_only_cpt++;   
   }
   /*
   ** go through the chunk
   */
   chunk_p = &cache_p->chunk[0];
   for (i = 0; i < DIRENT_CACHE_MAX_CHUNK; i++,chunk_p++)
   {
      if (chunk_p->wr_cpt == 0) continue;
      if (pwrite(fd, chunk_p->chunk_p, chunk_p->size, chunk_p->off) != chunk_p->size) 
      {
	severe("bad write returned value for %s (len %d): error %s",cache_p->pathname,chunk_p->size,strerror(errno));
	goto error;        
      }       
      chunk_p->wr_cpt = 0; 
      dirent_wb_cache_th_write_bytes_count+= chunk_p->size;
      dirent_wb_write_th_count++;
   }
   /*
   ** clear the header write pointer here to avoid race condition with check for flush
   */
   cache_p->wr_cpt = 0;
   status = 0;

out:
  if(fd != -1) close(fd);
  if ((errno = pthread_rwlock_unlock(&cache_p->lock)) != 0) {
      severe("can't lock writeback cache entry: %s", strerror(errno));
      return -1;
  } 
  return status;   

error:
  /*
  ** Here we release the cache entry otherwise the flush thread will try forever to flush the cache 
  ** entry: the source is either on file open or write
  */
   chunk_p = &cache_p->chunk[0];
   for (i = 0; i < DIRENT_CACHE_MAX_CHUNK; i++,chunk_p++) chunk_p->wr_cpt = 0; 
   cache_p->wr_cpt = 0;
   goto out;
}
/*
 *_______________________________________________________________________
 */
/** writeback thread
 */
 #define DIRENT_WBCACHE_PTHREAD_FREQUENCY_SEC 1
static void *dirent_wbcache_thread(void *v) {

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    int dirent_wbcache_thread_period_current_count = 0;
    dirent_wbcache_thread_period_count = 1;
    // Set the frequency of calls
    struct timespec ts = {DIRENT_WBCACHE_PTHREAD_FREQUENCY_SEC, 0};
   dirent_writeback_entry_t  *cache_p;
   int i;

    uma_dbg_thread_add_self("Dirent wr.back");
    
    dirent_wbcache_thread_period_count = DIRENT_WBCACHE_PTHREAD_FREQUENCY_SEC;
    dirent_wbcache_poll_stats[0] = 0;
    dirent_wbcache_poll_stats[1] = 0;
    info("WriteBack cache thread started ");

    for (;;) {
        dirent_wbcache_thread_period_current_count++;
	if (dirent_wbcache_thread_period_current_count >= dirent_wbcache_thread_period_count)
	{
          START_PROFILING_TH(dirent_wbcache_poll_stats);
          cache_p = &dirent_writeback_cache_p[0];
          for (i = 0; i < DIRENT_CACHE_MAX_ENTRY;i++,cache_p++)
	  {
	     if (cache_p->state == 0) continue;
	     if ((cache_p->wr_cpt == 0)
	     &&  (dirent_wbcache_check_write_pending(cache_p) == 0))
	     {
	        continue;
	     }
	     dirent_wbcache_diskflush(cache_p);
	  
	  
	  }
	  STOP_PROFILING_TH(dirent_wbcache_poll_stats);
	  dirent_wbcache_thread_period_current_count = 0;
	}
        nanosleep(&ts, NULL);
    }
    return 0;
}

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
int dirent_wbcache_check_flush_on_read(int eid,char *pathname,int root_idx,fid_t dir_fid)
{

   dirent_writeback_entry_t  *cache_p;
   int i;

//   i = root_idx%DIRENT_CACHE_MAX_ENTRY ;
   i = dirent_wbcache_get_index(dir_fid,pathname);
   cache_p = &dirent_writeback_cache_p[i];
   if ((cache_p->state == 0)|| ((cache_p->wr_cpt == 0)&&(dirent_wbcache_check_write_pending(cache_p) == 0))) return 0;
   /*
   ** the entry is busy : check if it matches
   */
   if (eid != cache_p->eid) return 0;
   if (strcmp(pathname,cache_p->pathname)!=0) return 0;   
   if (memcmp(dir_fid, cache_p->dir_fid, sizeof (fid_t))!=0)  return 0;  

   /*
   ** it matches -> flush on disk before read
   */
   dirent_wbcache_diskflush(cache_p);

   dirent_wbcache_flush_counter++;
   return i;

}


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
int dirent_wbcache_check_invalidate_on_unlink(int eid,char *pathname,int root_idx,fid_t dir_fid)
{

   dirent_writeback_entry_t  *cache_p;
   dirent_chunk_cache_t *chunk_p;
   int i;

//   i = root_idx%DIRENT_CACHE_MAX_ENTRY ;  
   i = dirent_wbcache_get_index(dir_fid,pathname);
   cache_p = &dirent_writeback_cache_p[i];
   if ((cache_p->state == 0)|| ((cache_p->wr_cpt == 0)&&(dirent_wbcache_check_write_pending(cache_p) == 0))) return 0;
   /*
   ** the entry is busy : check if it matches
   */
   if (eid != cache_p->eid) return 0;
   if (strcmp(pathname,cache_p->pathname)!=0) return 0;   
   if (memcmp(dir_fid, cache_p->dir_fid, sizeof (fid_t))!=0)  return 0;  

   /*
   ** it matches -> invalidate the entry
   ** take the lock associated with the entry
   */
   if ((errno = pthread_rwlock_wrlock(&cache_p->lock)) != 0) {
       severe("can't lock writeback cache entry: %s", strerror(errno));
       return -1;
   }
   chunk_p = &cache_p->chunk[0];
   for (i = 0; i < DIRENT_CACHE_MAX_CHUNK; i++,chunk_p++) chunk_p->wr_cpt = 0;
   /*
   ** clear the header write pointer here to avoid race condition with check for flush
   */
   cache_p->wr_cpt = 0;
   cache_p->state = 0;

  if ((errno = pthread_rwlock_unlock(&cache_p->lock)) != 0) {
      severe("can't lock writeback cache entry: %s", strerror(errno));
      return -1;
  }

   dirent_wbcache_invalidate_counter++;
   return i;

}

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
int dirent_wbcache_open(int fd_dir,int fd,int eid,char *pathname,fid_t dir_fid,int root_idx)
{

   dirent_writeback_entry_t  *cache_p;
   int i;

//   i = root_idx%DIRENT_CACHE_MAX_ENTRY ;  
   i = dirent_wbcache_get_index(dir_fid,pathname);
   cache_p = &dirent_writeback_cache_p[i];
   if ((cache_p->state == 0)|| ((cache_p->wr_cpt == 0)&&(dirent_wbcache_check_write_pending ( cache_p) == 0)))
   { 
     cache_p->fd = fd;
     strcpy(cache_p->pathname,pathname);
     memcpy(cache_p->dir_fid,dir_fid,sizeof(fid_t));
     cache_p->eid = eid;   
     cache_p->state = 1; 
    if ((errno = pthread_rwlock_wrlock(&cache_p->lock)) != 0) {
        severe("can't lock writeback cache entry: %s", strerror(errno));
        return -1;
    }
     return i;
   }   
   /*
   ** the entry is busy : check if it matches
   */
   if (eid != cache_p->eid) goto out;
   if (strcmp(pathname,cache_p->pathname)!=0) goto out;
   if (memcmp(dir_fid, cache_p->dir_fid, sizeof (fid_t))!=0)  goto out;
   
   /*
   ** it matches
   */
   if ((errno = pthread_rwlock_wrlock(&cache_p->lock)) != 0) {
       severe("can't lock writeback cache entry: %s", strerror(errno));
       return -1;
   }
   dirent_wbcache_hit_counter++;
   return i;
out:
   if (cache_p->fd == -1)
   {
     /*
     ** the entry is free, so push it on disk
     */
     dirent_wbcache_diskflush(cache_p);
     dirent_wbcache_flush_counter++;
     if ((errno = pthread_rwlock_wrlock(&cache_p->lock)) != 0) {
	 severe("can't lock writeback cache entry: %s", strerror(errno));
	 return -1;
     }
     cache_p->fd = fd;
     strcpy(cache_p->pathname,pathname);
     memcpy(cache_p->dir_fid,dir_fid,sizeof(fid_t));
     cache_p->eid = eid;   
     cache_p->state = 1; 
     return i;
   } 
   dirent_wbcache_miss_counter++;
   return -1;
}

/*
 *_______________________________________________________________________
 */
/**
* local API to flush on disk a chunk to make room for a new one
  The lock is already taken at the time this service is called

  @param cache_p: pointer to the cache entry
*/
int dirent_wbcache_chunk_diskflush(dirent_writeback_entry_t  *cache_p)
{
  int fd = -1;
  int flag = O_WRONLY | O_CREAT | O_NOATIME;
  dirent_chunk_cache_t *chunk_p;
  int status = -1;
  char path[PATH_MAX];
  /*
  ** open the dirent file
  */
  mdirent_resolve_path(dirent_export_root_path,cache_p->dir_fid,(char*)cache_p->pathname,path);
  if ((fd = open(path, flag, S_IRWXU)) == -1) 
  {
     severe("cannot open %s",path);
     goto error;  
  } 
  chunk_p = &cache_p->chunk[0];
  if (pwrite(fd, chunk_p->chunk_p, chunk_p->size, chunk_p->off) != chunk_p->size) 
  {
    severe("bad write returned value for %s (len %d): error %s",cache_p->pathname,chunk_p->size,strerror(errno));
    goto error;        
  }       
//      info("FDL write chunk%d  %s offset %llu len %u",i,cache_p->pathname,chunk_p->off,chunk_p->size);
  chunk_p->wr_cpt = 0;      
  dirent_wb_cache_th_write_bytes_count+= chunk_p->size;
  dirent_wb_write_chunk_count++;
  status = 0;

error:
  if(fd != -1) close(fd);

  return status;   

}

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
int dirent_wbcache_write(int fd,void *buf,size_t count,off_t offset)
{
   dirent_writeback_entry_t  *cache_p;
   dirent_chunk_cache_t       *chunk_p;
   int i;
   int free_chunk = -1;
   int ret;

  /*
  ** check if the fd is OK
  */
  if (fd < 0)
  {
     errno = EBADF;
     return -1;
  }
  if (fd >= DIRENT_CACHE_MAX_ENTRY)
  {
    errno = EBADF;
    return -1;
  }
  /*
  ** get the cache entry
  */
  cache_p = &dirent_writeback_cache_p[fd];

  /*
  ** check the offset
  */
  if (offset == 0)
  {
     /*
     ** header of the dirent file
     */
     if (cache_p->dirent_header == NULL)
     {
        cache_p->dirent_header = malloc(count);
	if (cache_p->dirent_header == NULL) goto error;
     } 
//     info("FDL header cache %s : offset %llu size %u",cache_p->pathname,offset,count);
     dirent_wb_cache_write_bytes_count+=count;
     memcpy(cache_p->dirent_header,buf,count);
     dirent_wb_write_count++;
     cache_p->size = count;
     cache_p->wr_cpt+=1;
     goto out; 
  }
  /*
  ** this a chunk: search for a chunk with the same offset
  */
  dirent_wb_total_chunk_write_cpt++;
  chunk_p = cache_p->chunk;
  free_chunk = -1;
  for (i = 0; i < DIRENT_CACHE_MAX_CHUNK; i++)
  {
     if (chunk_p[i].off == offset)
     {
        /*
	** there is a match
	*/
	if (chunk_p[i].chunk_p == NULL)
	{
           chunk_p[i].chunk_p = malloc(count);
	   if (chunk_p[i].chunk_p== NULL) goto error;
	} 
	memcpy(chunk_p[i].chunk_p,buf,count);
//        info("FDL chunk cache %s : offset %llu size %u",cache_p->pathname,chunk_p[i].off,count);

        dirent_wb_cache_write_bytes_count+=count;
        dirent_wb_write_count++;
        chunk_p[i].size = count;
        chunk_p[i].wr_cpt+=1;

	goto out;      
     }
     if (((chunk_p[i].off == 0)|| (chunk_p[i].wr_cpt==0)) && ( free_chunk == -1))
     {
       free_chunk = i;
     }
  }
  /*
  ** not found : check if there is a free chunk
  */
reloop:
  if (free_chunk >= 0)
  {
    if (chunk_p[free_chunk].chunk_p == NULL)
    {
       chunk_p[free_chunk].chunk_p = malloc(count);
       if (chunk_p[free_chunk].chunk_p== NULL) return -1;
    } 
    memcpy(chunk_p[free_chunk].chunk_p,buf,count);
    chunk_p[free_chunk].off = offset;

    dirent_wb_cache_write_bytes_count+=count;
    chunk_p[free_chunk].size = count;
    dirent_wb_write_count++;
    chunk_p[free_chunk].wr_cpt+=1;
    goto out;     
  }
  /*
  ** need to flush one chunk in order to get some room: always flush chunk 0
  */
  ret = dirent_wbcache_chunk_diskflush(cache_p);
  if (ret < 0) goto error;
  /*
  ** fill the entry in chunk 0
  */
  free_chunk = 0;
  goto reloop;
  
out :  
  return count;

error:
  count = -1;
  goto  out;
}

/**
*____________________________________________________________
*/
/**
*  close a file associated with a dirent writeback cache entry

   @param fd : file descriptor
   
   @retval >= 0 : number of bytes written in the writeback cache
   @retval < 0 : error see errno for details
*/
int dirent_wbcache_close(int fd)
{
   dirent_writeback_entry_t  *cache_p;

  /*
  ** check if the fd is OK
  */
  if (fd < 0)
  {
     errno = EBADF;
     return -1;
  }
  if (fd >= DIRENT_CACHE_MAX_ENTRY)
  {
    errno = EBADF;
    return -1;
  }
  /*
  ** get the cache entry
  */
  cache_p = &dirent_writeback_cache_p[fd];
  if (cache_p->fd != -1) 
  {
     cache_p->fd = -1;  
  }
  if ((errno = pthread_rwlock_unlock(&cache_p->lock)) != 0) {
      severe("can't unlock writeback cache entry: %s", strerror(errno));
  }
  return 0;
}



/*
 *_______________________________________________________________________
 */
/**
* flush the content of a cache entry on disk

  @param cache_p: pointer to the cache entry
*/
int dirent_wbcache_diskflush_best_effort(dirent_writeback_entry_t  *cache_p)
{
  int fd = -1;
  int flag = O_WRONLY | O_CREAT | O_NOATIME;
  dirent_chunk_cache_t *chunk_p;
  int i;
  int status = -1;
  char path[PATH_MAX];


  /*
  ** check if the flush already occur: race between thread and check for flush
  */
  if ((cache_p->wr_cpt == 0)&&(dirent_wbcache_check_write_pending(cache_p) == 0))
  {
    return 0;     
  }
  /*
  ** open the dirent file
  */
  mdirent_resolve_path(dirent_export_root_path,cache_p->dir_fid,(char*)cache_p->pathname,path);
  if ((fd = open(path, flag, S_IRWXU)) == -1) 
  {
     goto error;  
  } 
  /*
  ** goto the entry and find out the section to write on disk
  */
  if (cache_p->wr_cpt != 0)
  {
     if (pwrite(fd, cache_p->dirent_header, cache_p->size, 0) != cache_p->size) 
     {
       goto error;        
     } 
     dirent_wb_cache_th_write_bytes_count+= cache_p->size;
     dirent_wb_write_th_count++;
   }
   /*
   ** go through the chunk
   */
   chunk_p = &cache_p->chunk[0];
   for (i = 0; i < DIRENT_CACHE_MAX_CHUNK; i++,chunk_p++)
   {
      if (chunk_p->wr_cpt == 0) continue;
      if (pwrite(fd, chunk_p->chunk_p, chunk_p->size, chunk_p->off) != chunk_p->size) 
      {
	goto error;        
      }       
      chunk_p->wr_cpt = 0; 
      dirent_wb_cache_th_write_bytes_count+= chunk_p->size;
      dirent_wb_write_th_count++;
   }
   /*
   ** clear the header write pointer here to avoid race condition with check for flush
   */
   cache_p->wr_cpt = 0;
   status = 0;

error:
  if(fd != -1) close(fd);
  return status;   

}
/**
*____________________________________________________________
*/
/**
*  Flush the write back cache on stop
*/
void dirent_wbcache_flush_on_stop()
{
   dirent_writeback_entry_t  *cache_p;
   int i;

  if (dirent_writeback_cache_initialized==0) return ;

   cache_p = &dirent_writeback_cache_p[0];
   for (i = 0; i < DIRENT_CACHE_MAX_ENTRY;i++,cache_p++)
   {
      if (cache_p->state == 0) continue;
      if ((cache_p->wr_cpt == 0)
      &&  (dirent_wbcache_check_write_pending(cache_p) == 0))
      {
	 continue;
      }
      dirent_wbcache_diskflush_best_effort(cache_p);
   }
}
/**
*____________________________________________________________
*/
/**
* init of the write back cache

  @retval 0 on success
  @retval < 0 error (see errno for details)
*/

int dirent_wbcache_init()
{
  int i;

  if (dirent_writeback_cache_initialized) return 0;
  dirent_writeback_cache_p = malloc(sizeof(dirent_writeback_entry_t)*DIRENT_CACHE_MAX_ENTRY);
  if (dirent_writeback_cache_p == NULL)
  {
     fatal("Out of memory");
  }
  memset(dirent_writeback_cache_p,0,sizeof(dirent_writeback_entry_t)*DIRENT_CACHE_MAX_ENTRY);

  for (i = 0; i < DIRENT_CACHE_MAX_ENTRY; i++) 
  {
    dirent_writeback_cache_p[i].fd = -1;
    if (pthread_rwlock_init(&dirent_writeback_cache_p[i].lock, NULL) != 0) {
        return -1;
    }    
  }
  /**
  * create the writeback thread
  */
  dirent_writeback_cache_initialized = 1;
  dirent_writeback_cache_enable = 1;
  if ((errno = pthread_create(&dirent_wbcache_ctx_thread, NULL,
        dirent_wbcache_thread, NULL)) != 0) {
    severe("can't create writeback cache thread %s", strerror(errno));
    return -1;
  }
  return  0;

}
