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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <unistd.h>
#include <libintl.h>
#include <libconfig.h>
#include <getopt.h>
#include <inttypes.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/vfs.h>
#include <malloc.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdarg.h>
#include <mntent.h>
#include <sys/mount.h>
#include <attr/xattr.h>
#include <pthread.h>

#include <rozofs/rozofs_srv.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include <rozofs/common/common_config.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/rozofs_core_files.h>

#include "rozofs_mover.h"


/*
** Working buffer for read/write
*/
#define ROZOFS_MOVER_BUFFER_SIZE_MB 2
#define ROZOFS_MOVER_1MB            (1024*1024)
#define ROZOFS_MOVER_BUFFER_SIZE   (ROZOFS_MOVER_BUFFER_SIZE_MB*ROZOFS_MOVER_1MB)
static char buf[ROZOFS_MOVER_BUFFER_SIZE];


void rozofs_mover_apply_throughput(int size);


#define MAX_XATTR_SIZE 4096

typedef struct _mover_thread_ctx_t
{
    int thread_id;
//    char buf[ROZOFS_MOVER_BUFFER_SIZE];        /**< buffer used for data transfer */
    char *pbuf;
    char         src_fname[128];
    char         failed_fname[128];  
    uint64_t         last_offset;  
    uint64_t         last_size;  
} mover_thread_ctx_t;


typedef struct _rozofs_cmove_cmd_file_t
{
   char *path;
   uint64_t mtime;
} rozofs_cmove_cmd_file_t;

typedef struct _rozofs_mover_stat_t {
  uint64_t         submited;    /**< incremented each time the main thread submits a file to move */
  uint64_t         not_mounted; /**< incremented if the mountpath is not mounted */ 
  uint64_t         updated;     /**< incremented each time the file to move has been updated  */
  uint64_t         xattr;       /**< not used */
  uint64_t         error;       /**< incremented each time an error is encountered during the file move */
  uint64_t         success;     /**< incremented eadch time the file has been successfully moved */
  uint64_t         bytes_thread; /**< incremented in realtime by thread while updating the throughput */
  uint64_t         bytes;        /**< incremented at the end of the file move with the total size of the moved file */
  uint64_t         seconds;      /**< elasped time in seconds    */
  float            throughput;   /**< move throughput    */
  uint64_t         round;        /**< number of time the mover thread has been called  */
  uint64_t         last_offset;  /**< not used */
  uint64_t         last_size;    /**< not used */
  uint64_t         sleep_count;  /**< incremented each time a thread has to sleep because it consumes its credit */
  uint64_t         sleep_time_us; /**< cumulated sleep time in microseconds     */
  int              nb_threads;    /**< Current number of threads */
} rozofs_mover_stat_t;



static rozofs_mover_stat_t stats;
rozofs_mover_stat_t *fmove_stats_p = NULL;


static char         tmp_fname[256]={0};
static char         mount_path[1024]={0};
static int          dynamic_mount = 0;
static char         src_fname[128];
static char         failed_fname[128];
static char         *mover_path;


uint64_t     rozofs_mover_throughput;
uint64_t     rozofs_mover_new_throughput;
uint64_t     rozofs_mover_time_reference;
uint64_t     rozofs_mover_total_credit;
uint64_t     rozofs_mover_one_credit;

/*
** need a mutex in multithread mode to handle the throughput
*/
static pthread_mutex_t throughput_lock;

rozofs_cmove_cmd_file_t throughput_file;
rozofs_cmove_cmd_file_t stats_file;
rozofs_cmove_cmd_file_t nb_threads_file;
rozofs_cmove_cmd_file_t pause_file;
/*
** Queue used between the mover threads and the main thread
*/
struct queue {
    pthread_mutex_t   lock;
    pthread_cond_t    wait_room;
    pthread_cond_t    wait_data;
    unsigned int      size;
    unsigned int      head;
    unsigned int      tail;
    void       **queue;
};

void *queue_get(struct queue * q);
void queue_put(struct queue *q, void *j);
void * response_thread(void * ctx);

void * mover_thread(void * ctx);


#define QUEUE_RING_SZ 256
#define THREADS_LIMIT 100
mover_thread_ctx_t thread_context[ROZOFS_MAX_MOVER_THREADS];
uint32_t pending_request=0;
pthread_mutex_t   pending_lock;
uint32_t received_response = 0;
// int nb_threads = ROZOFS_MAX_MOVER_THREADS;
struct queue queue_request;
struct queue queue_response;
int use_threads = 1;

void rozofs_mover_throughput_init(uint64_t throughput);


void log_error(rozofs_mover_job_t *job)
{
   info("Error %s",strerror(job->error));
}   


/*
**__________________________________________________________________
*/


int rozofs_cmove_init_throughput(char *rootpath,int throughput)
{
   FILE *fp;
   int ret;
   struct stat statbuf;   
   
   if (rootpath == NULL) return 0;
   throughput_file.mtime = 0;
   
   throughput_file.path = malloc(2048);
   if (throughput_file.path == NULL) return -1;
   sprintf(throughput_file.path,"%s/throughput",rootpath);
   /*
   ** Create the file or truncate it
   */
   fp = fopen(throughput_file.path,"w");
   if (fp == NULL) return -1;
   /*
   ** Write the throughput in the file
   */
   fprintf(fp,"%d\n",throughput);
   /*
   ** Close the file and register the mtime
   */
   fclose(fp);

   ret = stat(throughput_file.path,&statbuf);
   if (ret < 0)
   {
     return -1;
   } 
   throughput_file.mtime = statbuf.st_mtime;
   /*
   ** Initialize throughput computation
   */
   rozofs_mover_throughput_init((uint64_t)throughput);
   rozofs_mover_throughput_update_request((uint64_t)throughput);

   return 0;
}

/*
**__________________________________________________________________
*/
void rozofs_cmove_read_throughput()
{

   FILE *fp;
   int ret;
   struct stat statbuf;   
   int throughput;
   
  if (throughput_file.path == NULL) return;

  /*
  ** Check if the mtime of the file has been changed
  */
  ret = stat(throughput_file.path,&statbuf);
  if (ret < 0)
  {
    return ;
  }
  if (throughput_file.mtime == statbuf.st_mtime) return;  
  /*
  ** mtime has changed so read the file
  */
  fp = fopen(throughput_file.path,"r");
  ret = fscanf(fp,"%d\n",&throughput);
  if ((ret == EOF) || ret == 0)
  {
     /*
     ** rewrite with the current throughput
     */
     rewind(fp);
     throughput = (int)rozofs_mover_throughput;
     fprintf(fp,"%d\n",throughput);
     /*
     ** Close the file and register the mtime
     */
     fclose(fp);     
     ret = stat(throughput_file.path,&statbuf);
     if (ret < 0)
     {
       return ;
     } 
     throughput_file.mtime = statbuf.st_mtime;
  }
  rozofs_mover_throughput_update_request((uint64_t)throughput);
  
}

/*
**__________________________________________________________________
*/


int rozofs_cmove_init_stats(char *rootpath)
{
   FILE *fp;
   int ret;
   struct stat statbuf;  
   char buffer[1024]; 
   
   if (rootpath == NULL) return 0;
   stats_file.mtime = 0;
   
   stats_file.path = malloc(2048);
   if (stats_file.path == NULL) return -1;
   sprintf(stats_file.path,"%s/stats.json",rootpath);
   /*
   ** Create the file or truncate it
   */
   fp = fopen(stats_file.path,"w");
   if (fp == NULL) return -1;
   /*
   ** Write  file
   */
   rozofs_mover_print_stat(buffer);
   fprintf(fp,"%s",buffer);
   /*
   ** Close the file and register the mtime
   */
   fclose(fp);

   return 0;
}

/*
**__________________________________________________________________
*/
void rozofs_cmove_read_stats()
{

   FILE *fp;
   char buffer[1024]; 
      
  if (stats_file.path == NULL) return;
  /*
  ** Create the file or truncate it
  */
  fp = fopen(stats_file.path,"w");
  if (fp == NULL) return;
  /*
  ** Write  file
  */
  rozofs_mover_print_stat(buffer);
  fprintf(fp,"%s",buffer);
  /*
  ** Close the file and register the mtime
  */
  fclose(fp);
  
}

/*
**__________________________________________________________________
*/


int rozofs_cmove_init_nb_threads(char *rootpath,int nb_threads)
{
   FILE *fp;
   int ret;
   struct stat statbuf;   
   
   if (rootpath == NULL) return 0;
   nb_threads_file.mtime = 0;
   
   nb_threads_file.path = malloc(2048);
   if (nb_threads_file.path == NULL) return -1;
   sprintf(nb_threads_file.path,"%s/nb_threads",rootpath);
   /*
   ** Create the file or truncate it
   */
   fp = fopen(nb_threads_file.path,"w");
   if (fp == NULL) return -1;
   /*
   ** Write the nb_threads in the file
   */
   fprintf(fp,"%d\n",nb_threads);
   /*
   ** Close the file and register the mtime
   */
   fclose(fp);

   ret = stat(nb_threads_file.path,&statbuf);
   if (ret < 0)
   {
     return -1;
   } 
   nb_threads_file.mtime = statbuf.st_mtime;
   return 0;
}

/*
**__________________________________________________________________
*/
void rozofs_cmove_read_nb_threads()
{

   FILE *fp;
   int ret;
   struct stat statbuf;   
   int nb_threads;
   
  if (nb_threads_file.path == NULL) return;

  /*
  ** Check if the mtime of the file has been changed
  */
  ret = stat(nb_threads_file.path,&statbuf);
  if (ret < 0)
  {
    return ;
  }
  if (nb_threads_file.mtime == statbuf.st_mtime) return;  
  /*
  ** mtime has changed so read the file
  */
  fp = fopen(nb_threads_file.path,"r");
  ret = fscanf(fp,"%d",&nb_threads);
  if ((ret == EOF) || ret == 0)
  {
     /*
     ** rewrite with the current nb_threads
     */
     rewind(fp);
     nb_threads = (int)fmove_stats_p->nb_threads;
     fprintf(fp,"%d\n",nb_threads);
     /*
     ** Close the file and register the mtime
     */
     fclose(fp);     
     ret = stat(nb_threads_file.path,&statbuf);
     if (ret < 0)
     {
       return ;
     } 
     nb_threads_file.mtime = statbuf.st_mtime;
  }
  if (nb_threads == 0) nb_threads =1;
  if (nb_threads > ROZOFS_MAX_MOVER_THREADS) nb_threads = ROZOFS_MAX_MOVER_THREADS;
  
  fmove_stats_p->nb_threads = nb_threads;
  
}
/*
**__________________________________________________________________
*/
int queue_init(struct queue * q, const unsigned int slots)
{
    if (!q || slots < 1U)
        return errno = EINVAL;

    q->queue = malloc(sizeof (void *) * (size_t)(slots + 1));
    if (!q->queue)
        return errno = ENOMEM;

    q->size = slots+ 1U; 
    q->head = 0U;
    q->tail = 0U;

    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->wait_room, NULL);
    pthread_cond_init(&q->wait_data, NULL);

    return 0;
}
/*
**__________________________________________________________________
*/
void *queue_get(struct queue * q)
{
    void *j;

    pthread_mutex_lock(&q->lock);
    while (q->head == q->tail)
        pthread_cond_wait(&q->wait_data,&q->lock);

    j = q->queue[q->tail];
    q->queue[q->tail] = NULL;
    q->tail = (q->tail + 1U) % q->size;

    pthread_cond_signal(&q->wait_room);

    pthread_mutex_unlock(&q->lock);
    return j;
}
/*
**__________________________________________________________________
*/
void queue_put(struct queue *q, void *j)
{
    pthread_mutex_lock(&q->lock);
    while ((q->head + 1U) % q->size == q->tail)
        pthread_cond_wait(&q->wait_room,&q->lock);


    q->queue[q->head] = j;
    q->head = (q->head + 1U) % q->size;

    pthread_cond_signal(&q->wait_data);

    pthread_mutex_unlock(&q->lock);
    return;
}

/*-----------------------------------------------------------------------------
**
** Move one file in fid mode
**
** @param job               Description of the file to move
** @param throughput        Throughput limitation or zero when no limit
   @param buf               data buffer for the transfer
**
**----------------------------------------------------------------------------
*/
int rozofs_do_move_one_file_fid_mode_th(rozofs_mover_job_t * job, int throughput,mover_thread_ctx_t *ctx_p) {
  char       * pChar;
  int          i;
  off_t        offset=0;
  int          ret;
  tmp_fname[0] = 0;
  char dst_fname[128];
  char *src_fname; 
  char *failed_fname;
  char buf_fid[64];  
  char *buf;
  int xerrno;
  int          src=-1;
  int          dst=-1;  
  
  job->status = MOVER_ERROR_E;
  buf = ctx_p->pbuf;
  src_fname = ctx_p->src_fname;
  failed_fname = ctx_p->failed_fname;
  
  rozofs_inode_t *inode_p = (rozofs_inode_t*)job->name;

  inode_p->s.key = ROZOFS_REG_S_MOVER;
  rozofs_uuid_unparse((unsigned char *)job->name,buf_fid);
  sprintf(src_fname,"@rozofs_uuid@%s",buf_fid);
  ctx_p->last_offset = 0;
  ctx_p->last_size   = job->size;
  
  inode_p->s.key = ROZOFS_REG_D_MOVER;
  rozofs_uuid_unparse((unsigned char *)job->name,buf_fid);
  sprintf(dst_fname,"@rozofs_uuid@%s",buf_fid);
   
  /*
  ** Check file name exist
  */
  if (access(src_fname,R_OK) != 0) {
    xerrno = errno;
    severe("Can not access file \"%s\"",src_fname);
    goto generic_error;
  }  
  /*
  ** Open source file for reading
  */
  src = open(src_fname,O_RDONLY);
  if (src < 0) {
    xerrno = errno;
    severe("open(%s) %s",src_fname,strerror(errno));
    goto generic_error;
  }

  /*
  ** Set the new distribution 
  */
  pChar = buf;
  pChar += sprintf(pChar,"mover_allocate = %d",job->cid);
  i=0;
  while ((i<ROZOFS_SAFE_MAX)&&(job->sid[i]!=0)) {
    pChar += sprintf(pChar," %d", job->sid[i]);
    i++;
  }
  if (setxattr(dst_fname, "user.rozofs", buf, strlen(buf),0)<0) {
    if (errno==EINVAL) {
       xerrno = errno;
      severe("invalid distibution %s:%s",dst_fname,buf);
      goto generic_error;   
    }
    xerrno = errno;
    severe("fsetxattr(%s) %s",dst_fname,strerror(errno));   
    goto generic_error;
  }
  
  /*
  ** Open destination file
  */
  dst = open(dst_fname,O_RDWR,0766);
  if (dst < 0) {
    xerrno = errno;
    severe("open(%s) %s",dst_fname,strerror(errno));
    goto abort;    
  }   

  /*
  ** Copy the file
  */
  while (1) {
    int size;

    /*
    ** Read a whole buffer
    */      
    size = pread(src, buf, ROZOFS_MOVER_BUFFER_SIZE, offset);
    if (size < 0) {
      xerrno = errno;
      severe("pread(%s) %s",src_fname,strerror(errno)); 
      goto abort;         
    }
    
    /*
    ** End of file
    */      
    if (size == 0) break;
    
    /*
    ** Write the data
    */      
    if (pwrite(dst, buf, size, offset)<0) {
      xerrno = errno;
      severe("pwrite(%s,%d) %s",dst_fname,size,strerror(errno));     
      goto abort;       
    }
    
    offset += size;
    ctx_p->last_offset = offset;
    
    /*
    ** When throughput limitation is set adapdt the speed accordingly
    ** by sleeping for a while
    */
    rozofs_mover_apply_throughput(size);         
  }

  close(src);
  src = -1;
  ret = close(dst);
  dst = -1;
  if (ret < 0) {
    xerrno = errno;
    goto abort;       
  }
    
  pChar = buf;
  pChar += sprintf(pChar,"mover_validate = 0");
  if (setxattr(dst_fname, "user.rozofs", buf, strlen(buf),0)<0) {
    xerrno = errno;
    if (errno == EACCES)
    {
       //fmove_stats_p->updated++;
       job->status = MOVER_UPDATED_E; 
       goto specific_error;
    }
    severe("fsetxattr(%s,%s) %s",dst_fname,buf,strerror(errno));   
    goto generic_error;
  }
//  fmove_stats_p->success++;
//  fmove_stats_p->bytes += offset;
    
  /*
  ** Done
  */
  job->status = MOVER_SUCCESS_E;
  return 0;

abort:
  if (setxattr(dst_fname, "user.rozofs", "mover_invalidate = 0", strlen("mover_invalidate"),0)<0) {
    severe("fsetxattr(%s,mover_invalidate) %s",dst_fname,strerror(errno));   
  }
  
generic_error:
//  fmove_stats_p->error++;
  
specific_error:  

  strcpy(failed_fname,src_fname);
  /*
  ** Close opened files
  */

  if (src > 0) {    
    close(src);
    src = -1;
  }

  if (dst > 0) {
    close(dst);
    dst = -1;
  } 
  errno = xerrno;
  
  job->error = errno;
  return -1;
}

/*
**__________________________________________________________________
*/
/**
*  Create a set of mover threads that will take care of the mover jobs

  @param count: number of thread to create
  
  @retval 0 on success
  @retval -1 on error
  
*/
int create_mover_threads(int count) {
   int                        i;
   int                        err;
   pthread_attr_t             attr;
   pthread_t                  thread;
   mover_thread_ctx_t         *p_ctx;

   
   err = pthread_attr_init(&attr);
   if (err != 0) {
     return -1;
   }  
   for (i = 0; i < count; i++)
   {
     p_ctx = &thread_context[i];
     p_ctx->thread_id = i;
     err = pthread_create(&thread,&attr,mover_thread,p_ctx);   
     if (err != 0) {
       severe("pthread_create %d %s",i,strerror(errno));
       return -1;
     }  
   }
  return 0;
}

/*
**__________________________________________________________________
*/
/**
*   Create the response thread that takes care of the mover response

    @param none
    
    @retval 0 on success
    @retval -1 on error
*/
int create_response_thread() {
   int                        err;
   pthread_attr_t             attr;
   pthread_t                  thread;
   
   err = pthread_attr_init(&attr);
   if (err != 0) {
     return -1;
   }  

   err = pthread_create(&thread,&attr,response_thread,NULL);   
   if (err != 0) {
     info("pthread_create (mover response thread) %s",strerror(errno));
     return -1;
   }  
  return 0;
}

/*
**__________________________________________________________________
*/
/**
*  Periodic Mover thread

*/
void *mover_thread_periodic(void * ctx)
{

  int fd_bytes = -1;
  int fd_filecount= -1;
  int fd_throughput = -1;

  struct timespec ts = {3, 0};

  /*
  ** Check that mover_path
  */
  if (mover_path != NULL)
  {
     /*
     ** Open the byte file, throughput and status file
     */
  }
  for (;;) {
     nanosleep(&ts, NULL);
     rozofs_cmove_read_throughput();
     rozofs_cmove_read_stats();
     rozofs_cmove_read_nb_threads();     
  }
}
/*
**__________________________________________________________________
*/
/**
*   Create the periodic thread that takes care of the mover files

    @param none
    
    @retval 0 on success
    @retval -1 on error
*/
int create_periodic_thread() {
   int                        err;
   pthread_attr_t             attr;
   pthread_t                  thread;
   
   err = pthread_attr_init(&attr);
   if (err != 0) {
     return -1;
   }  

   err = pthread_create(&thread,&attr,mover_thread_periodic,NULL);   
   if (err != 0) {
     info("pthread_create (mover periodic thread) %s",strerror(errno));
     return -1;
   }  
  return 0;
}




/*
**__________________________________________________________________
*/
/**
*   create mover  thread

*/
void * mover_thread(void * ctx) {
  rozofs_mover_job_t *job_p;
  mover_thread_ctx_t * pCtx = (mover_thread_ctx_t *) ctx;
  /*
  ** allocate the resources to handle the file move
  */
  pCtx->pbuf = memalign(4096,ROZOFS_MOVER_BUFFER_SIZE);
  
  while(1) 
  {
   if (pCtx->thread_id >= fmove_stats_p->nb_threads)
   {
      sleep(10);
      continue;
   }
   job_p = queue_get(&queue_request);
   /*
   ** launch the file move
   */
   rozofs_do_move_one_file_fid_mode_th(job_p,0,pCtx);

   queue_put(&queue_response,job_p);   
  }
}


/*
 *_______________________________________________________________________
 */
 /**
 *  Submit a job to the mover threads
 
   @param job_p: pointer to the job
*/
void submit_request( rozofs_mover_job_t *job_p)
{
   
   pthread_mutex_lock(&pending_lock);
   pending_request++;
   pthread_mutex_unlock(&pending_lock);
   queue_put(&queue_request,job_p); 
}


/*
**__________________________________________________________________
*/
void * response_thread(void * ctx) {

  rozofs_mover_job_t *job_p;
  info("thread response started!!\n");
  
  while(1) 
  {
  
   job_p = queue_get(&queue_response);
   switch (job_p->status)
   {
      case MOVER_SUCCESS_E:
        fmove_stats_p->success++;
	fmove_stats_p->bytes += job_p->size;	
	break;
      default:
      case MOVER_UPDATED_E:
        fmove_stats_p->updated++;
	break;
      case MOVER_ERROR_E:
	fmove_stats_p->error++;
	log_error(job_p);
	break;
    }
   received_response++;
   /*
   ** Free this job
   */
   free(job_p->name);
   free(job_p);
   
   pthread_mutex_lock(&pending_lock);
   pending_request--;
   pthread_mutex_unlock(&pending_lock);

  }
}



/*-----------------------------------------------------------------------------
**
** Get time in micro seconds
**
** @param from  when set compute the delta in micro seconds from this time to now
**              when zero just return current time in usec
**----------------------------------------------------------------------------
*/
uint64_t rozofs_mover_get_us(uint64_t from) {
  struct timeval     timeDay;
  uint64_t           us;
  
  /*
  ** Get current time in us
  */
  gettimeofday(&timeDay,(struct timezone *)0); 
  us = ((unsigned long long)timeDay.tv_sec * 1000000 + timeDay.tv_usec);

  /*
  ** When from is given compute the delta
  */
  if (from) {
    us -= from;
  }  
  
  return us;	
}
/*-----------------------------------------------------------------------------
**
** Request for a throughput value change
**
** @param throughput        Requested throughput limitation or zero when no limit
**
**----------------------------------------------------------------------------
*/
void rozofs_mover_throughput_update_request(uint64_t throughput) {
  /*
  ** Store new requested throughput set through rozodiag
  */
  rozofs_mover_new_throughput = throughput;
} 
/*-----------------------------------------------------------------------------
**
** Reset mover time references for throughput computation
** 
** @param throughput   The new throughput that is to take place
**
**----------------------------------------------------------------------------
*/
void rozofs_mover_throughput_init(uint64_t throughput) {

  /*
  ** Lock the throughput for multi-threaded mode
  */
  pthread_mutex_lock(&throughput_lock);
  /*
  ** Store throughput
  */
  rozofs_mover_throughput = throughput;
  
  if (rozofs_mover_throughput > 0) {
    /*
    ** Get starting time 
    */
    rozofs_mover_time_reference = rozofs_mover_get_us(0);
    rozofs_mover_total_credit   = 0;
    /*
    ** Compute the credit in us for copying the buffer of size ROZOFS_MOVER_BUFFER_SIZE_MB
    */
    rozofs_mover_one_credit = 1000000 * ROZOFS_MOVER_BUFFER_SIZE_MB/rozofs_mover_throughput;
  } 
   pthread_mutex_unlock(&throughput_lock); 
} 
/*-----------------------------------------------------------------------------
**
** Apply the throughput limitation, and eventually hange throughput when requested
**
**----------------------------------------------------------------------------
*/
void rozofs_mover_apply_throughput(int size) {
  int64_t sleep_time_us;
  
  /*
  ** Check whether throughput limitation is set
  */
  if (rozofs_mover_throughput > 0) {
  
    pthread_mutex_lock(&throughput_lock);
   
    /*
    ** Add credit to the total time credit
    */
    if (size == ROZOFS_MOVER_BUFFER_SIZE) {
      rozofs_mover_total_credit += rozofs_mover_one_credit;
    }  
    else {
      rozofs_mover_total_credit += ((rozofs_mover_one_credit*size)/ROZOFS_MOVER_BUFFER_SIZE);
    }  
    
    /*
    ** Compute the advance in time 
    */
    sleep_time_us = rozofs_mover_total_credit - rozofs_mover_get_us(rozofs_mover_time_reference);
    /*
    ** Update the byte_thread counter
    */
    fmove_stats_p->bytes_thread +=size;
    
    pthread_mutex_unlock(&throughput_lock);
        
    /*
    ** Sleep a while if we are too fast
    */
    if (sleep_time_us >= 10000) {
      fmove_stats_p->sleep_count++;
      fmove_stats_p->sleep_time_us+= sleep_time_us;
      usleep(sleep_time_us);
    }
  }   

  /*
  ** Check whether throughput has changed
  */
  if (rozofs_mover_new_throughput != rozofs_mover_throughput) {
    rozofs_mover_throughput_init(rozofs_mover_new_throughput);
  }
} 
/*-----------------------------------------------------------------------------
**
** Check whether a mount path is actually mounted by reading /proc/mounts
** 
** @param   mnt_path    Mount path
**
**----------------------------------------------------------------------------
*/
int rozofs_mover_is_mounted (char * mnt_path) {
  FILE          * mtab = NULL;
  struct mntent * part = NULL;
  int             is_mounted = 0;

  /*
  ** Read file
  */
  mtab = setmntent("/proc/mounts", "r");
  if (mtab == NULL) return 0;
  
  /*
  ** Loop on entries to find out the mount path we are looking for
  */
  while ( ( part = getmntent ( mtab) ) != NULL) {
    if (!part->mnt_fsname) continue;
    if (strcmp(part->mnt_fsname,"rozofs")!=0) continue;
    if (!part->mnt_dir) continue;
    if (strcmp(part->mnt_dir, mnt_path) == 0) {
      is_mounted = 1;
      break;
    }
  }

  endmntent ( mtab);
  return is_mounted;
}
/*-----------------------------------------------------------------------------
**
** Remove a rozofsmount mount point
** 
** @param   mnt_path    Mount path
**
**----------------------------------------------------------------------------
*/
void rozofs_mover_remove_mount_point(char * mnt_path) {

  /*
  ** No mount path
  */
  if (mnt_path[0] == 0) return;

  /*
  ** When mount point is mounted, unmount it  
  */
  if (rozofs_mover_is_mounted(mnt_path)) {

    /*
    ** Unmount it
    */
    if (umount(mnt_path)==-1) {}  
  
    /*
    ** When mount point is still mounted, force unmount 
    */
    if (rozofs_mover_is_mounted(mnt_path)) {
      /*
      ** Unmount it
      */
      if (umount2(mnt_path,MNT_FORCE)==-1) {}  
    }
  }
  
  /*
  ** Remove directory
  */    
  rmdir(mnt_path);
}
/*-----------------------------------------------------------------------------
**
**  Get a free mount point instance by scanning the running rozofsmount instances
**
**----------------------------------------------------------------------------
*/
int rozofs_get_free_rozofsmount_intance(void) {
  char     fname[128];
  char     cmd[256];
  int      fd;
  uint64_t mask = 1; // Do not take instance 0
  int      size;
  int      instance;
  char   * pChar;

  sprintf(fname, "/tmp/rozofs.%d",getpid());

  /*
  ** Check rozofsmount instances in use
  */
  sprintf(cmd,"ps -fC rozofsmount > %s",fname);
  if (system(cmd)==0) {}

  /*
  ** Read result file and then remove it
  */
  fd = open(fname,O_RDONLY);
  if (fd < 0) {
    severe("open(%s) %s",fname, strerror(errno));
    return 0;
  }
  
  size = pread(fd,buf,ROZOFS_MOVER_BUFFER_SIZE,0);
  if (size < 0) {
    severe("pread(%s) %s",fname, strerror(errno));
    close(fd);
    return 0;
  }
  close(fd);
  buf[size] = 0;
  unlink(fname);
  
  /*
  ** Scan for instance in the buffer
  */
  pChar = buf;
  while (*pChar != 0) {

    pChar = strstr(pChar,"instance=");
    if (pChar == NULL) break;
    
    int ret = sscanf(pChar+9,"%d",&instance);
    if (ret != 1) {
      severe("Bad instance option \"%s\"", pChar);
      continue;
    } 
    pChar += 9; 
    if (instance < 64) mask |= (1ULL<<instance);  
  }

  /*
  ** Find a free instance starting at instance 10
  */
  for (instance=62; instance>0; instance=instance-2) {
    if ((mask & (1ULL<<instance))==0) return instance;
  }
  /*
  ** No free instance
  */
  return 0;
}
/*-----------------------------------------------------------------------------
**
** Create a emporary rozofsmount mount point for RozoFS rebalancing
**
** @param   export_hosts  names or addresses of the exports
** @param   export_path   export path (eid) to mount toward
** @param   mnt_path      Mount path to use for mounting
** @param   mnt_instance  rozofsmount instance
**
**----------------------------------------------------------------------------
*/
int rozofs_mover_create_mount_point(char * export_hosts, char * export_path, char * mnt_path,int mnt_instance) {
  char     cmd[512];
  char   * pChar = cmd; 
  int      i;

  dynamic_mount = 0;
  /*
  ** Check whether this path exist
  */
  if (access(mnt_path,W_OK) == 0) {
    /*
    ** The path already exist !!! Unmount just in case
    */
    rozofs_mover_remove_mount_point(mnt_path);   
  }


  /*
  ** Create the temporary mount path
  */
  if (mkdir(mnt_path,S_IRUSR | S_IWUSR | S_IXUSR)<0) {
    severe("mkdir(%s) %s", mnt_path, strerror(errno));
    return -1;
  }  
  
  /*
  ** Prepare the rozofsmount command 
  */
  pChar += sprintf(pChar,"rozofsmount -H %s -E %s %s", export_hosts, export_path, mnt_path);
  pChar += sprintf(pChar," -o rozofsexporttimeout=60,rozofsstoragetimeout=40,rozofsstorclitimeout=50,rozofsnbstorcli=1,auto_unmount,noReadFaultTolerant");
  pChar += sprintf(pChar,",instance=%d",mnt_instance);

  /*
  ** Start the rozofsmount instance
  */
  if (system(cmd)==0) {}
  
  
  /*
  ** Wait for the mount point to be actually mounted
  */
  for (i=0; i < 2000; i++) {
    usleep(1000*20);
    if (rozofs_mover_is_mounted(mnt_path)) break; 
  }  
  if (i==2000) {
    severe("mount %s failed", mnt_path);
    return -1;
  }
  
  /*
  ** Change local directory to the mount point
  */
  if (chdir(mnt_path)!= 0) {
    severe("chdir(%s) %s",mnt_path,strerror(errno));
    if (chdir("/")!= 0) {}
    return -1;
  } 
  
    dynamic_mount = 1;
  return 0; 
  
}


/*-----------------------------------------------------------------------------
**
** Move a list a file to a new location for rebalancing purpose
**
** @param exportd_hosts     exportd host name or addresses (from configuration file)
** @param export_path       the export path to mount
** @param throughput        throughput litation in MB. 0 is no limitation.
** @param jobs              list of files along with their destination
**
**----------------------------------------------------------------------------
*/


int rozofs_do_move_one_export_fid_mode_multithreaded_mounted(char * exportd_hosts, char * export_path, int throughput, list_t * jobs,char *mount_path_in) {
  int                  instance;
  list_t             * p;
  list_t             * n;
  rozofs_mover_job_t * job;
  time_t               start;
  
  
  
  if (list_empty(jobs)) {
     return 0;
  }
  if ( mount_path_in == NULL)
  {
    /*
    ** Find out a free rozofsmount instance
    */
    instance = rozofs_get_free_rozofsmount_intance();
    sprintf(mount_path,"/tmp/rozofs_mover_pid%d_instance%d", getpid(), instance);    

    /*
    ** Mount it 
    */
    rozofs_mover_create_mount_point(exportd_hosts , export_path, mount_path, instance);
  }
  else
  {
    dynamic_mount = 0;
    strcpy(mount_path,mount_path_in);        
    /*
    ** Change local directory to the mount point
    */
    if (chdir(mount_path)!= 0) {
      severe("chdir(%s) %s",mount_path,strerror(errno));
      if (chdir("/")!= 0) {}
      return -1;
    } 
  }

  /*
  ** Initialize throughput computation
  */
//  rozofs_mover_throughput_init(throughput);
//  rozofs_mover_throughput_update_request(throughput);

  start = time(NULL);
  
  /*
  ** Loop on the file to move
  */
  list_for_each_forward_safe(p,n,jobs) {
    
    job = (rozofs_mover_job_t *) list_entry(p, rozofs_mover_job_t, list);
    fmove_stats_p->submited++;
    /*
    ** remove the job from the linked list (since it will be removed by the response thread
    */
    list_remove(&job->list);
    /*
    ** Process the job
    */
    if (rozofs_mover_is_mounted(mount_path)) {
      submit_request(job); 
    }
    else {
      fmove_stats_p->not_mounted++;    
    }
   
  }
  /*
  ** Wait for the end of all the mover
  */
  while(1)
  {
    if (pending_request == 0) break;
    sleep(1);
  }

  fmove_stats_p->round++;
  fmove_stats_p->seconds += (time(NULL)-start);
  if (fmove_stats_p->seconds!=0) {
    float actual_throughput; 
    actual_throughput = fmove_stats_p->bytes;
    actual_throughput /= (1024*1024);
    actual_throughput /= fmove_stats_p->seconds;
    fmove_stats_p->throughput = actual_throughput;
  }     
  /*
  ** Get out of the mountpoint before removing it
  */
  if (chdir("/")!= 0) {}

  if (mount_path_in == 0)
  {
    /*
    ** Unmount the temporary mount path 
    */
    rozofs_mover_remove_mount_point(mount_path);
  }
  /*
  ** Clear mount path name
  */
  mount_path[0] = 0;
  return 0;
}

/*-----------------------------------------------------------------------------
**
** Man function
**
**----------------------------------------------------------------------------
*/
void rozofs_mover_man(char * pChar) {
  pChar += rozofs_string_append(pChar,"mover       : display mover statistics\n");  
  pChar += rozofs_string_append(pChar,"mover reset : reset mover statistics\n");    
}  
/*-----------------------------------------------------------------------------
**
** Statistics formating
**
**----------------------------------------------------------------------------
*/
void rozofs_mover_print_stat(char * pChar) {
  pChar += sprintf(pChar,"{ \"mover\" : \n");
  pChar += sprintf(pChar,"   {\n");
  pChar += sprintf(pChar,"     \"submitted files\": %llu,\n",(unsigned long long)fmove_stats_p->submited);
  pChar += sprintf(pChar,"     \"mount error\"    : %llu,\n",(unsigned long long)fmove_stats_p->not_mounted);
  pChar += sprintf(pChar,"     \"updated files\"  : %llu,\n",(unsigned long long)fmove_stats_p->updated);
  pChar += sprintf(pChar,"     \"other error\"    : %llu,\n",(unsigned long long)fmove_stats_p->error);
  pChar += sprintf(pChar,"     \"success\"        : %llu,\n",(unsigned long long)fmove_stats_p->success);
  pChar += sprintf(pChar,"     \"bytes moved\"    : %llu,\n",(unsigned long long)fmove_stats_p->bytes);
  pChar += sprintf(pChar,"     \"bytes in move\"  : %llu,\n",(unsigned long long)fmove_stats_p->bytes_thread);
  pChar += sprintf(pChar,"     \"round\"          : %llu,\n",(unsigned long long)fmove_stats_p->round);
  pChar += sprintf(pChar,"     \"throughput MiB\" : %.1f,\n",fmove_stats_p->throughput);
  pChar += sprintf(pChar,"     \"request_bw MiB\" : %llu,\n",(unsigned long long)rozofs_mover_throughput);
  pChar += sprintf(pChar,"     \"sleep_count   \" : %llu,\n",(unsigned long long)fmove_stats_p->sleep_count);
  pChar += sprintf(pChar,"     \"sleep_time(us)\" : %llu,\n",(unsigned long long)fmove_stats_p->sleep_time_us);
  pChar += sprintf(pChar,"     \"duration secs\"  : %llu\n",(long long unsigned int) fmove_stats_p->seconds);
  pChar += sprintf(pChar,"   }\n}\n");
}  
/*-----------------------------------------------------------------------------
**
** Diagnostic function
**
**----------------------------------------------------------------------------
*/
void rozofs_mover_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
  char           *pChar=uma_dbg_get_buffer();
 
  if (argv[1]!=0) {
    if (strcmp(argv[1],"reset")==0) {
      memset(&stats,0,sizeof(stats));
    }
  }
  rozofs_mover_print_stat(pChar);
  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());
}
/*-----------------------------------------------------------------------------
**
** Function called on exception in order to cleaup the mess
**
**----------------------------------------------------------------------------
*/
static void on_crash(int sig) {

  
  /*
  ** Remove the eventualy created temporary file
  */ 
  if (tmp_fname[0] != 0) {
    unlink(tmp_fname);   
  } 
  tmp_fname[0] = 0;
  
  
  /*
  ** Remove move point
  */
  if (dynamic_mount) rozofs_mover_remove_mount_point(mount_path);
} 
/*-----------------------------------------------------------------------------
**
** Service initialize
   
   @param path: root pathname for the mover control files (throughput, nb_threads, pause, stats ....
   @param throughput: throughput in MB/s
   @param nb_threads: number of mover threads
**
**----------------------------------------------------------------------------
*/
int rozofs_mover_init_th(char *path,int throughput,int nb_threads) {

  src_fname[0]    = 0;
  failed_fname[0] = 0;
  int ret;
  
  mover_path = NULL;
  pthread_mutex_init(&throughput_lock, NULL);
  
  /*
  ** Initialize global variables
  */
  memset(&stats,0, sizeof(stats));
  memset(mount_path,0,sizeof(mount_path));
  memset(tmp_fname,0,sizeof(tmp_fname));
  fmove_stats_p = &stats;
  mover_path = path;
  /*
  ** Create the stats in the /var/run/rozofs_kpi directory
  */
  if (path != NULL)
  {
//    sprintf(path,"%s/mover/c_mov_%d",ROZOFS_KPI_ROOT_PATH,path);
//    fmove_stats_p = malloc(sizeof(rozofs_mover_stat_t));
    if (fmove_stats_p == NULL)
    {
       /*
       ** Revert to memory
       */
       fmove_stats_p = &stats; 
    }
  }
  fmove_stats_p->nb_threads = nb_threads;
  /*
  ** init of the files that are used to communicated with the mover
  */
  rozofs_cmove_init_throughput(path,throughput);
  rozofs_cmove_init_stats(path);  
  rozofs_cmove_init_nb_threads(path,fmove_stats_p->nb_threads);    
  
  pthread_mutex_init(&pending_lock, NULL);
  pending_request = 0;

  ret = queue_init(&queue_request,QUEUE_RING_SZ);
  if (ret < 0)
  {
     fatal("Error on queue init \n");
     exit(-1);
  }
  ret = queue_init(&queue_response,QUEUE_RING_SZ);
  if (ret < 0)
  {
     fatal("Error on queue init \n");
     exit(-1);
  }
  /*
  ** Create the threads
  */
  ret = create_mover_threads(fmove_stats_p->nb_threads);
  if (ret < 0)
  {
     fatal("Error on thread creation \n");
     exit(-1);
  } 
  ret = create_response_thread(0);
  if (ret < 0)
  {
     fatal("Error on thread creation \n");
     exit(-1);
  }       
  ret = create_periodic_thread(0);
  if (ret < 0)
  {
     fatal("Error on thread creation \n");
     exit(-1);
  } 
  
  /*
  ** Add debug function
  */  
  uma_dbg_addTopicAndMan("mover", rozofs_mover_debug, rozofs_mover_man, 0);  
  
  /*
  ** Add crash call back
  */
  rozofs_attach_crash_cbk(on_crash);
  
  return 0;
}
