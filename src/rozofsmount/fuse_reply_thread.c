
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
#include <semaphore.h>
#include "rozofs_fuse_api.h"
#include <rozofs/core/rozofs_queue.h>

typedef struct _rozofs_reply_create_t
{
   struct fuse_entry_param param;
   struct fuse_file_info   fi;
} rozofs_reply_create_t;


typedef struct _rozofs_reply_attr_t
{
   struct stat attr;
   double      attr_timeout;
} rozofs_reply_attr_t;

typedef struct  _rozofs_fuse_thr_reply_t
{
   fuse_req_t  req;
   int         cmd;
   int         err;
   union 
   {
      struct fuse_entry_param   f_reply_entry;
      struct fuse_file_info     f_reply_open;
      struct statvfs            f_reply_statfs;
      size_t                    f_reply_write;
      rozofs_reply_create_t     f_reply_create;
      rozofs_reply_attr_t       f_reply_attr;
   } s;
} rozofs_fuse_thr_reply_t;



/*
**__________________________________________________________________

     A T T R I B U T E  W R I T E - B A C K    T H R E A D
**__________________________________________________________________
*/
#define ROZOFSMOUNT_MAX_ATT_THREADS  4
#define ROZOFS_FUSE_REP_QUEUE_RING_SZ 4096
#define ROZOFS_FUSE_REP_BUF_POOL_COUNT (ROZOFS_FUSE_REP_QUEUE_RING_SZ+4)


rozofs_queue_t rozofs_fuse_reply_queue;
rozofs_fuse_thr_reply_t *rozofs_reply_pool_p;
uint32_t rozofs_reply_pool_idx_cur;

typedef enum _rz_fuse_reply_cmd_e 
{
   RZ_FUSE_REPLY_NONE= 0,
   RZ_FUSE_REPLY_ERR,
   RZ_FUSE_REPLY_ATTR,
   RZ_FUSE_REPLY_ENTRY,
   RZ_FUSE_REPLY_CREATE,
   RZ_FUSE_REPLY_OPEN,
   RZ_FUSE_REPLY_MAX
} rz_fuse_reply_cmd_e;
 
typedef struct _fuse_reply_writeback_ctx_t
{
  pthread_t               thrdId; 
  int                     thread_idx;     
  uint64_t                 stats[RZ_FUSE_REPLY_MAX];

} fuse_reply_writeback_ctx_t;


fuse_reply_writeback_ctx_t rs_fuse_reply_ctx_tb[ROZOFSMOUNT_MAX_ATT_THREADS];

static inline rozofs_fuse_thr_reply_t *rozofs_fuse_reply_get_next_slot()
{
  rozofs_fuse_thr_reply_t *p = &rozofs_reply_pool_p[rozofs_reply_pool_idx_cur];
  rozofs_reply_pool_idx_cur++;
  if (rozofs_reply_pool_idx_cur == ROZOFS_FUSE_REP_BUF_POOL_COUNT) rozofs_reply_pool_idx_cur = 0;
  return p;

}
/*
**__________________________________________________________________
*/
static char * show_fuse_reply_thread_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"fuse_reply_thread             : display threads statistics\n");  
  return pChar; 
}
extern int rozofs_max_getattr_pending;
extern uint64_t rozofs_max_getattr_duplicate;
void show_fuse_reply_thread(char * argv[], uint32_t tcpRef, void *bufRef) 
{
    char *pChar = uma_dbg_get_buffer();
    fuse_reply_writeback_ctx_t       *thread_ctx_p;
    int i;
    int value1,value2;
    int new_val;


#if 0
  if (argv[1] != NULL)
  {

      if (strcmp(argv[1],"set")==0) 
      {
	 errno = 0;
	 if (argv[2] == NULL)
	 {
           pChar += sprintf(pChar, "argument is missing\n\n");
	   show_fuse_reply_thread_help(pChar);
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;	  	  
	 }
	 new_val = (int) strtol(argv[2], (char **) NULL, 10);   
	 if (errno != 0) {
           pChar += sprintf(pChar, "bad value %s\n\n",argv[2]);
	   show_fuse_reply_thread_help(pChar);
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;
	 }
	 /*
	 ** 
	 */
	 if (new_val > ROZOFSMOUNT_MAX_ATT_THREADS) {
           pChar += sprintf(pChar, "unsupported value %s max is %d\n\n",argv[2],ROZOFSMOUNT_MAX_ATT_THREADS);
	   show_fuse_reply_thread_help(pChar);
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;
	 }	 
	 rz_fuse_nb_threads = new_val;
         pChar += sprintf(pChar, "current number of threads is now %d\n\n",rz_fuse_nb_threads);
	 uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	 return;
      }
      show_fuse_reply_thread_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
      return;
    }
#endif
    thread_ctx_p = rs_fuse_reply_ctx_tb;
    /*
    ** search if the lv2 is already under the control of one thread
    */
    pChar +=sprintf(pChar,"Current number of threads: %d\n",common_config.rozofsmount_fuse_reply_thread);
    pChar +=sprintf(pChar,"Max getattr pending      : %d\n",rozofs_max_getattr_pending);
    pChar +=sprintf(pChar,"Max getattr duplicated   : %llu\n",(unsigned long long int)rozofs_max_getattr_duplicate);
    rozofs_max_getattr_duplicate = 0;
    rozofs_max_getattr_pending = 0;
    pChar +=sprintf(pChar,"| thread | err. cnt  |  rep err  |  attr cnt |  lkup cnt | create cnt| open  cnt |\n");
    pChar +=sprintf(pChar,"+--------+-----------+-----------+-----------+-----------+-----------+-----------+\n");
    for (i = 0; i < ROZOFSMOUNT_MAX_ATT_THREADS; i++,thread_ctx_p++)
    { 
       pChar +=sprintf(pChar,"|   %d    | %8.8llu  | %8.8llu  | %8.8llu  | %8.8llu  | %8.8llu  | %8.8llu  |\n",
               i,
	       (unsigned long long int)thread_ctx_p->stats[RZ_FUSE_REPLY_NONE],
	       (unsigned long long int)thread_ctx_p->stats[RZ_FUSE_REPLY_ERR],
	       (unsigned long long int)thread_ctx_p->stats[RZ_FUSE_REPLY_ATTR],
	       (unsigned long long int)thread_ctx_p->stats[RZ_FUSE_REPLY_ENTRY],
	       (unsigned long long int)thread_ctx_p->stats[RZ_FUSE_REPLY_CREATE],
	       (unsigned long long int)thread_ctx_p->stats[RZ_FUSE_REPLY_OPEN]);

    }
    pChar +=sprintf(pChar,"+--------+-----------+-----------+-----------+-----------+-----------+-----------+\n");
    pChar +=sprintf(pChar,"Fuse Reply payload size %u\n",sizeof(rozofs_fuse_thr_reply_t));
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
void *fuse_reply_thread(void *arg) {    

   fuse_reply_writeback_ctx_t * ctx_p = (fuse_reply_writeback_ctx_t*)arg;
   int value= -1;
   char bufname[64];
   rozofs_fuse_thr_reply_t *job_p;

    sprintf(bufname,"Attr. thread#%d",ctx_p->thread_idx);
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
      my_priority.sched_priority= 99;
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

    uma_dbg_thread_add_self(bufname);
    while(1)
    {  
      if ((ctx_p->thread_idx!= 0) && (ctx_p->thread_idx >= common_config.rozofsmount_fuse_reply_thread))
      {
	 sleep(30);
	 continue;
      }
      /*
      ** Read some data from the queue
      */
      job_p = rozofs_queue_get(&rozofs_fuse_reply_queue);  
      /*
      ** Execute the command associated with the context
      */
      switch( job_p->cmd)
      {
         case RZ_FUSE_REPLY_ERR:
	   fuse_reply_err(job_p->req, job_p->err);
	   ctx_p->stats[RZ_FUSE_REPLY_ERR]++;
	   break;
	 case RZ_FUSE_REPLY_ATTR:
	   fuse_reply_attr(job_p->req, &job_p->s.f_reply_attr.attr,job_p->s.f_reply_attr.attr_timeout);
	   ctx_p->stats[RZ_FUSE_REPLY_ATTR]++;
	   break;
	 case RZ_FUSE_REPLY_ENTRY:
	   fuse_reply_entry(job_p->req, &job_p->s.f_reply_entry);
	   ctx_p->stats[RZ_FUSE_REPLY_ENTRY]++;
	   break;
	 case RZ_FUSE_REPLY_CREATE:
	   fuse_reply_create(job_p->req, &job_p->s.f_reply_create.param,&job_p->s.f_reply_create.fi);
	   ctx_p->stats[RZ_FUSE_REPLY_CREATE]++;
	   break;
	 case RZ_FUSE_REPLY_OPEN:
	   fuse_reply_open(job_p->req, &job_p->s.f_reply_open);
	   ctx_p->stats[RZ_FUSE_REPLY_OPEN]++;
	   break;
	 default:
	 ctx_p->stats[RZ_FUSE_REPLY_NONE]++;
	 break;
      }
    }           
}

/*
**__________________________________________________________________
*/
/**
 * Reply with attributes
 *
 * Possible requests:
 *   getattr, setattr
 *
 * @param req request handle
 * @param attr the attributes
 * @param attr_timeout	validity timeout (in seconds) for the attributes
 * @return zero for success, -errno for failure to send reply
 */
void rz_fuse_reply_attr(fuse_req_t req, const struct stat *attr,
		    double attr_timeout)
{
    rozofs_fuse_thr_reply_t *slot_p;		    

    if (common_config.rozofsmount_fuse_reply_thread== 0)
    {
       fuse_reply_attr(req,attr,attr_timeout);
       return;
    }
    /*
    ** Get the next available slot buffer to fill up the data to send back to fuse
    */
    slot_p = rozofs_fuse_reply_get_next_slot();
    
    slot_p->cmd =RZ_FUSE_REPLY_ATTR;
    memcpy(&slot_p->s.f_reply_attr.attr,attr,sizeof(struct stat));
    slot_p->s.f_reply_attr.attr_timeout = attr_timeout;
    slot_p->req = req;
    /*
    ** post the request to the thread
    */ 
    rozofs_queue_put(&rozofs_fuse_reply_queue,slot_p);    

}
/*
**__________________________________________________________________
*/
/**
 * Reply with a directory entry
 *
 * Possible requests:
 *   lookup, mknod, mkdir, symlink, link
 *
 * Side effects:
 *   increments the lookup count on success
 *
 * @param req request handle
 * @param e the entry parameters
 * @return zero for success, -errno for failure to send reply
 */
void rz_fuse_reply_entry(fuse_req_t req, const struct fuse_entry_param *e)
{
    rozofs_fuse_thr_reply_t *slot_p;		    

    if (common_config.rozofsmount_fuse_reply_thread== 0)
    {
       fuse_reply_entry(req,e);
       return;
    }
    /*
    ** Get the next available slot buffer to fill up the data to send back to fuse
    */
    slot_p = rozofs_fuse_reply_get_next_slot();
    /*
    ** copy the data in the thread context
    */
    slot_p->cmd =RZ_FUSE_REPLY_ENTRY;
    memcpy(&slot_p->s.f_reply_entry,e,sizeof(struct fuse_entry_param));
    slot_p->req = req;
    /*
    ** post the request to the thread
    */ 
    rozofs_queue_put(&rozofs_fuse_reply_queue,slot_p);    
}


/*
**__________________________________________________________________
*/
/**
 * Reply on file opening
 *
 * Possible requests:
 *   fuse_ll_open
 *
 *
 * @param req request handle
 * @param fi opened parameters
 * @return none
 */
void rz_fuse_reply_open(fuse_req_t req, struct fuse_file_info *fi)
{
    rozofs_fuse_thr_reply_t *slot_p;		    

    if (common_config.rozofsmount_fuse_reply_thread== 0)
    {
       fuse_reply_open(req,fi);
       return;
    }
    /*
    ** Get the next available slot buffer to fill up the data to send back to fuse
    */
    slot_p = rozofs_fuse_reply_get_next_slot();
    /*
    ** copy the data in the thread context
    */
    slot_p->cmd =RZ_FUSE_REPLY_OPEN;
    memcpy(&slot_p->s.f_reply_open,fi,sizeof(struct fuse_file_info));
    slot_p->req = req;
    /*
    ** post the request to the thread
    */ 
    rozofs_queue_put(&rozofs_fuse_reply_queue,slot_p);    
}

/*
**__________________________________________________________________
*/
/**
 * Reply with a file entry
 *
 * Possible requests:
 *   create
 *
 * Side effects:
 *   increments the lookup count on success
 *
 * @param req request handle
 * @param e the entry parameters
 * @return zero for success, -errno for failure to send reply
 */
void rz_fuse_reply_create(fuse_req_t req, const struct fuse_entry_param *e,struct fuse_file_info *fi)
{

    rozofs_fuse_thr_reply_t *slot_p;		    

    if (common_config.rozofsmount_fuse_reply_thread== 0)
    {
       fuse_reply_create(req,e,fi);
       return;
    }
    /*
    ** Get the next available slot buffer to fill up the data to send back to fuse
    */
    slot_p = rozofs_fuse_reply_get_next_slot();
    /*
    ** copy the data in the thread context
    */
    slot_p->cmd =RZ_FUSE_REPLY_CREATE;
    memcpy(&slot_p->s.f_reply_create.param,e,sizeof(struct fuse_entry_param));
    memcpy(&slot_p->s.f_reply_create.fi,fi,sizeof(struct fuse_file_info));
    slot_p->req = req;
    /*
    ** post the request to the thread
    */ 
    rozofs_queue_put(&rozofs_fuse_reply_queue,slot_p);    
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
int fuse_reply_thread_init()
{
   int status = 0;
   pthread_attr_t             attr;
   int                        i,err;
   int ret;
   fuse_reply_writeback_ctx_t       *thread_ctx_p;  
  /*
  ** clear the thread table
  */
  memset(rs_fuse_reply_ctx_tb,0,sizeof(rs_fuse_reply_ctx_tb));

  ret = rozofs_queue_init(&rozofs_fuse_reply_queue,ROZOFS_FUSE_REP_QUEUE_RING_SZ);
  if (ret < 0)
  {
     severe("Cannot create queue for fuse reply thread \n");
     return (-1);
  }
  /*
  ** Create the pool of buffer
  */
  rozofs_reply_pool_p = xmalloc(sizeof(rozofs_fuse_thr_reply_t)*(ROZOFS_FUSE_REP_BUF_POOL_COUNT));
  if (rozofs_reply_pool_p == NULL)
  {
     severe("Cannot allocated memory for fuse reply threads \n");
     return (-1);
  }  
  rozofs_reply_pool_idx_cur = 0;
  /*
  ** Now create the threads
  */
  thread_ctx_p = rs_fuse_reply_ctx_tb;
  for (i = 0; i < ROZOFSMOUNT_MAX_ATT_THREADS ; i++,thread_ctx_p++) 
  {
     err = pthread_attr_init(&attr);
     if (err != 0) {
       fatal("fuse reply thread: pthread_attr_init(%d) %s",i,strerror(errno));
       return -1;
     }  
     thread_ctx_p->thread_idx = i;
     err = pthread_create(&thread_ctx_p->thrdId,&attr,fuse_reply_thread,thread_ctx_p);
     if (err != 0) {
       fatal("fuse reply  thread: pthread_create(%d) %s",i, strerror(errno));
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
