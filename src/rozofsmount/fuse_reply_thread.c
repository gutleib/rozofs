
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
 
/*
**__________________________________________________________________

     A T T R I B U T E  W R I T E - B A C K    T H R E A D
**__________________________________________________________________
*/
#define ROZOFSMOUNT_MAX_ATT_THREADS  4
typedef enum _rz_fuse_reply_cmd_e 
{
   RZ_FUSE_REPLY_NONE= 0,
   RZ_FUSE_REPLY_ERR,
   RZ_FUSE_REPLY_ATTR,
   RZ_FUSE_REPLY_ENTRY,
   RZ_FUSE_REPLY_CREATE,
   RZ_FUSE_REPLY_MAX
} rz_fuse_reply_cmd_e;
 
typedef struct _fuse_reply_writeback_ctx_t
{
  pthread_t               thrdId; 
  int                     thread_idx;     
  int                     cmd;
  fuse_req_t              req; 
  /*
  ** case of the attributes: fuse_reply_attr
  */
  struct stat            attr;
  double                 attr_timeout;
  /*
  ** case of the attributes: fuse_reply_error
  */
  int                      error;  
  /*
  ** case of the attributes: fuse_reply_entry
  */  
  struct fuse_entry_param  fep;
  struct fuse_file_info    fi;
  uint64_t                 wakeup_count;
  uint64_t                 err_count;
  uint64_t                 busy_count;
  uint64_t                 stats[RZ_FUSE_REPLY_MAX];
  sem_t                    fuse_reply_wr_ready;
  sem_t                    fuse_reply_wr_rq;
} fuse_reply_writeback_ctx_t;


fuse_reply_writeback_ctx_t rs_fuse_reply_ctx_tb[ROZOFSMOUNT_MAX_ATT_THREADS];
int  rz_fuse_nb_threads = 0;
/*
**__________________________________________________________________
*/
static char * show_fuse_reply_thread_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"fuse_reply_thread set <value> : set the max number of threads (default:%u)\n",ROZOFSMOUNT_MAX_ATT_THREADS);
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
    thread_ctx_p = rs_fuse_reply_ctx_tb;
    /*
    ** search if the lv2 is already under the control of one thread
    */
    pChar +=sprintf(pChar,"Current number of threads: %d\n",rz_fuse_nb_threads);
    pChar +=sprintf(pChar,"Max getattr pending      : %d\n",rozofs_max_getattr_pending);
    pChar +=sprintf(pChar,"Max getattr duplicated   : %llu\n",(unsigned long long int)rozofs_max_getattr_duplicate);
    rozofs_max_getattr_duplicate = 0;
    rozofs_max_getattr_pending = 0;
    pChar +=sprintf(pChar,"| thread | rdy |in_prg| wake-up cnt | err. cnt  |  busy cnt |  attr cnt |  lkup cnt |\n");
    pChar +=sprintf(pChar,"+--------+-----+------+-------------+-----------+-----------+-----------+-----------+\n");
    for (i = 0; i < ROZOFSMOUNT_MAX_ATT_THREADS; i++,thread_ctx_p++)
    { 
       sem_getvalue(&thread_ctx_p->fuse_reply_wr_ready,&value1);
       sem_getvalue(&thread_ctx_p->fuse_reply_wr_rq,&value2);
       pChar +=sprintf(pChar,"|   %d    |  %d  |   %d  |  %8.8llu   | %8.8llu  | %8.8llu  | %8.8llu  | %8.8llu  |\n",
               i,value1,value2,
	       (unsigned long long int)thread_ctx_p->wakeup_count,
	       (unsigned long long int)thread_ctx_p->err_count,
	       (unsigned long long int)thread_ctx_p->busy_count,
	       (unsigned long long int)thread_ctx_p->stats[RZ_FUSE_REPLY_ATTR],
	       (unsigned long long int)thread_ctx_p->stats[RZ_FUSE_REPLY_ENTRY]);
       thread_ctx_p->busy_count = 0;
       thread_ctx_p->wakeup_count = 0;
    }
    pChar +=sprintf(pChar,"+--------+-----+------+-------------+-----------+-----------+-----------+-----------+\n");
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
//      sem_post(&ctx_p->fuse_reply_wr_ready);
      /*
      ** wait for a command
      */
      sem_getvalue(&ctx_p->fuse_reply_wr_ready,&value);
      sem_wait(&ctx_p->fuse_reply_wr_rq);
      /*
      ** Execute the command associated with the context
      */
      switch( ctx_p->cmd)
      {
         case RZ_FUSE_REPLY_ERR:
	   fuse_reply_err(ctx_p->req, ctx_p->error);
	   ctx_p->stats[RZ_FUSE_REPLY_ERR]++;
	   break;
	 case RZ_FUSE_REPLY_ATTR:
	   fuse_reply_attr(ctx_p->req, &ctx_p->attr,ctx_p->attr_timeout);
	   ctx_p->stats[RZ_FUSE_REPLY_ATTR]++;
	   break;
	 case RZ_FUSE_REPLY_ENTRY:
	   fuse_reply_entry(ctx_p->req, &ctx_p->fep);
	   ctx_p->stats[RZ_FUSE_REPLY_ENTRY]++;
	   break;
	 case RZ_FUSE_REPLY_CREATE:
	   fuse_reply_create(ctx_p->req, &ctx_p->fep,&ctx_p->fi);
	   ctx_p->stats[RZ_FUSE_REPLY_CREATE]++;
	   break;
	 default:
	 ctx_p->err_count++;
	 break;
      }
      /*
      ** release the context for that thread
      */
      ctx_p->cmd = RZ_FUSE_REPLY_NONE;
    }           
}

/*
**__________________________________________________________________
*/
/**
    Get the context of a free thread
    
    @retval NULL if no thread available
    @retval <>NULL pointer to the thread context
 
*/
static inline fuse_reply_writeback_ctx_t *fuse_reply_get_thread_context()
{
    int i;
   fuse_reply_writeback_ctx_t       *thread_ctx_p;  
    
    thread_ctx_p = rs_fuse_reply_ctx_tb;
    /*
    ** search if the lv2 is already under the control of one thread
    */
    for (i = 0; i < rz_fuse_nb_threads; i++,thread_ctx_p++)
    {
       if (thread_ctx_p->cmd == RZ_FUSE_REPLY_NONE)
       {
	  return thread_ctx_p;             
       } 
    }
    return NULL;
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
    fuse_reply_writeback_ctx_t       *thread_ctx_p;
    
    thread_ctx_p = fuse_reply_get_thread_context();
    if (thread_ctx_p == NULL)
    {
       fuse_reply_attr(req,attr,attr_timeout);
       return;
    }
    /*
    ** copy the data in the thread context
    */
    thread_ctx_p->cmd =RZ_FUSE_REPLY_ATTR;
    memcpy(&thread_ctx_p->attr,attr,sizeof(struct stat));
    thread_ctx_p->attr_timeout = attr_timeout;
    thread_ctx_p->req = req;
    /*
    ** post the request to the thread
    */    
//    sem_wait(&thread_ctx_p->fuse_reply_wr_ready);
    sem_post(&thread_ctx_p->fuse_reply_wr_rq); 
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

    fuse_reply_writeback_ctx_t       *thread_ctx_p;
    
    thread_ctx_p = fuse_reply_get_thread_context();
    if (thread_ctx_p == NULL)
    {
       fuse_reply_entry(req,e);
       return;
    }
    /*
    ** copy the data in the thread context
    */
    thread_ctx_p->cmd =RZ_FUSE_REPLY_ENTRY;
    memcpy(&thread_ctx_p->fep,e,sizeof(struct fuse_entry_param));
    thread_ctx_p->req = req;
    /*
    ** post the request to the thread
    */    
//    sem_wait(&thread_ctx_p->fuse_reply_wr_ready);
    sem_post(&thread_ctx_p->fuse_reply_wr_rq); 


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

    fuse_reply_writeback_ctx_t       *thread_ctx_p;
    
    thread_ctx_p = fuse_reply_get_thread_context();
    if (thread_ctx_p == NULL)
    {
       fuse_reply_create(req,e,fi);
       return;
    }
    /*
    ** copy the data in the thread context
    */
    thread_ctx_p->cmd =RZ_FUSE_REPLY_CREATE;
    memcpy(&thread_ctx_p->fep,e,sizeof(struct fuse_entry_param));
    memcpy(&thread_ctx_p->fi,fi,sizeof(struct fuse_file_info));
    thread_ctx_p->req = req;
    /*
    ** post the request to the thread
    */    
//    sem_wait(&thread_ctx_p->fuse_reply_wr_ready);
    sem_post(&thread_ctx_p->fuse_reply_wr_rq); 


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
   fuse_reply_writeback_ctx_t       *thread_ctx_p;  
  /*
  ** clear the thread table
  */
  memset(rs_fuse_reply_ctx_tb,0,sizeof(rs_fuse_reply_ctx_tb));
  if (common_config.rozofsmount_fuse_reply_thread == 0) rz_fuse_nb_threads = 0;
  else rz_fuse_nb_threads = ROZOFSMOUNT_MAX_ATT_THREADS;
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
     /*
     ** init of the semaphore
     */
     sem_init(&thread_ctx_p->fuse_reply_wr_ready, 0, 0);
     sem_init(&thread_ctx_p->fuse_reply_wr_rq, 0, 0);       
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
