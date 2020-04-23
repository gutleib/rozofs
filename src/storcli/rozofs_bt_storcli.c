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
#include <sys/types.h> 
#include <sys/socket.h>
#include <fcntl.h> 
#include <sys/un.h>             
#include <errno.h>  

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/core/ruc_buffer_api.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/ruc_buffer_debug.h>
#include "../rozofsmount/rozofs_bt_api.h"
#include "../rozofsmount/rozofs_bt_proto.h"

rozofs_queue_t rozofs_bt_rq_queue;
int rozofs_bt_threads_count;
uint32_t rozofs_bt_cmd_ring_size = 0;
uint32_t rozofs_bt_cmd_ring_idx = 0; 
int rozofs_bt_storcli_idx = 0;


extern pthread_rwlock_t rozofs_bt_shm_lock;

/**
*  Thread table
*/
rozofs_bt_thread_ctx_t rozofs_bt_thread_ctx_tb[1];
/*
 *_______________________________________________________________________
 */
/**
  Do a share memory registration 
  
  @param key: key of the shared memory to register
  @param rozofsmount_share_p: pointer to the share memory context to register (might be NULL)
  
  @retval 0 on success
  @retval -1 on error
*/
int rozofs_bt_storcli_memreg(rozofs_mem_key key,rozofs_stc_memreg_t *rozofsmount_share_p)
{

   int ret;
   int i;
   rozofs_stc_memreg_t *p;
      
   p = rozofsmount_share_p;
   if (p == NULL)
   {
     /*
     ** need to find out the entry by using the key
     */
     rozofs_memreg_t *q = rozofs_bt_stc_mem_p;
     rozofs_stc_memreg_t *tmp;
     tmp = (rozofs_stc_memreg_t*)q->addr;
     errno = ENOENT;
     for (i = 0; i <  ROZOFS_SHARED_MEMORY_REG_MAX; i ++,tmp++)
     {
       if (tmp->rozofs_key.u32 == key.u32)
       {
          if (tmp->name[0] == 0) break;
	  p = tmp;
	  errno = 0;
	  break; 
       }
     }
     if (p == NULL) return -1;    
   }   

   /*
   ** check if it has already been done by some other
   */	
   pthread_rwlock_wrlock(&rozofs_bt_shm_lock);  
   
   if (p->storcli_done[rozofs_bt_storcli_idx] != 0)
   {
     pthread_rwlock_unlock(&rozofs_bt_shm_lock); 	   
     return 0;  
   }

   /*
   ** start the registration
   */
   ret = rozofs_shm_stc_register(p->rozofs_key,p->name,p->remote_addr,p->length);
   if (ret > 0)
   {
     p->storcli_done[rozofs_bt_storcli_idx] =1;
   }
   else
   {
      severe("Error on shm_register %s: %s",p->name,strerror(errno));
   }	    
   pthread_rwlock_unlock(&rozofs_bt_shm_lock); 	
   return 0;    
}



/*
 *_______________________________________________________________________
 */
/** Share memory deletion polling thread
 */
 #define STORCLI_BT_MEMREG_MS ((1000*1000)*40)
static void *rozofs_bt_stc_thread(void *v) {

   rozofs_memreg_t *q = rozofs_bt_stc_mem_p;
   rozofs_stc_memreg_t *p;
   rozofs_bt_thread_ctx_t * thread_ctx_p = (rozofs_bt_thread_ctx_t*)v;
   rozofs_memreg_t *mem_unreg_p; 

   


    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    
    // Set the frequency of calls
    struct timespec ts = {0, STORCLI_BT_MEMREG_MS};
   int i;

    uma_dbg_thread_add_self("shm_thread");
    
    for (;;) {
	nanosleep(&ts, NULL);
	p= (rozofs_stc_memreg_t*)q->addr;
	for (i = 0; i <  ROZOFS_SHARED_MEMORY_REG_MAX; i ++,p++)
	{
	  if (p->name[0] == 0) continue;
	  if ((p->delete_req != 0) && (p->storcli_ack[thread_ctx_p->thread_idx] == 0))
	  {
            info("FDL sharemem /dev/shm/%s disconnection request on storcli %d",p->name,thread_ctx_p->thread_idx);
	    /*
	    ** need to perform the memory unregistration
	    */
	    mem_unreg_p =  &rozofs_shm_tb[p->rozofs_key.s.ctx_idx];

	     pthread_rwlock_wrlock(&rozofs_bt_shm_lock);  

	     if (mem_unreg_p->name!=NULL)
	     {
               munmap(mem_unreg_p->addr,mem_unreg_p->length);
               if (mem_unreg_p->fd_share) {
               close(mem_unreg_p->fd_share);
	       mem_unreg_p->fd_share = 0;
               shm_unlink(mem_unreg_p->name);
	       rozofs_shm_count--;
	       free(mem_unreg_p->name);
	       mem_unreg_p->name = NULL;
               info("FDL sharemem /dev/shm/%s disconnected !! on storcli %d",mem_unreg_p->name,thread_ctx_p->thread_idx);
               }

	      }
	      p->storcli_ack[thread_ctx_p->thread_idx] =1;
	      p->storcli_done[thread_ctx_p->thread_idx] =0;

	      pthread_rwlock_unlock(&rozofs_bt_shm_lock); 
	      continue;
	    } 
	    if ((p->delete_req == 0) && (p->storcli_done[thread_ctx_p->thread_idx] == 0))
	    {
	      /*
	      ** need to perform the memory unregistration
	      */
              info("FDL sharemem /dev/shm/%s registration request on storcli %d",p->name,thread_ctx_p->thread_idx);
	      rozofs_bt_storcli_memreg(p->rozofs_key,p);
#if 0
	      mem_unreg_p =  &rozofs_shm_tb[p->rozofs_key.s.ctx_idx];

	      pthread_rwlock_wrlock(&rozofs_bt_shm_lock);  
	      
	      /*
	      ** check if it has already been done by some other
	      */	    
	    
	      ret = rozofs_shm_stc_register(p->rozofs_key,p->name,p->remote_addr,p->length);
	      if (ret > 0)
	      {
	        p->storcli_done[thread_ctx_p->thread_idx] =1;
	      }
	      else
	      {
	         severe("Error on shm_register %s: %s",p->name,strerror(errno));
	      }	    
	      pthread_rwlock_unlock(&rozofs_bt_shm_lock); 	    
#endif	    
	    }	  	
	}
    }
    return 0;
}

/*
**_________________________________________________________________________________________________
*/
/*
** Create the threads that will handle all the batch requests

* @param hostname    storio hostname (for tests)
* @param eid    reference of the export
* @param storcli_idx    relative index of the storcli process
* @param nb_threads  number of threads to create
*  
* @retval 0 on success -1 in case of error
*/
int rozofs_bt_stc_thread_create(int storcli_id) {

   int                        err;
   pthread_attr_t             attr;
   rozofs_bt_thread_ctx_t * thread_ctx_p;


   /*
   ** clear the thread table
   */
   memset(rozofs_bt_thread_ctx_tb,0,sizeof(rozofs_bt_thread_ctx_t));
   /*
   ** Now create the threads
   */
   thread_ctx_p = rozofs_bt_thread_ctx_tb;
  
   err = pthread_attr_init(&attr);
   if (err != 0) {
     fatal("rozofs_bt_thread_create pthread_attr_init() %s",strerror(errno));
     return -1;
   }  

   thread_ctx_p->thread_idx = (storcli_id-1);
   err = pthread_create(&thread_ctx_p->thrdId,&attr,rozofs_bt_stc_thread,thread_ctx_p);
   if (err != 0) {
     fatal("rozofs_bt_thread_create pthread_create() %s", strerror(errno));
     return -1;
   }  
     
  return 0;
}

/*
**_________________________________________________________________________________________________
*/
int rozofs_bt_stc_init(uint16_t rozofsmount_id,int storcli_id)
{
   int ret;
   rozofs_memreg_t *q; 
   rozofs_stc_memreg_t *p;   
   int i ; 
   
   rozofs_bt_storcli_idx = storcli_id-1;
   /*
   ** ask for the creation of the shared memory for the case of rozofsmount, just a registration for the storcli
   */
   ret = rozofs_shm_init((int) rozofsmount_id,0);
   if (ret < 0) return RUC_NOK;
   /*
   ** Clear the done bit
   */
   q = rozofs_bt_stc_mem_p;
   p = (rozofs_stc_memreg_t*)q->addr;   
   for (i = 0; i <  ROZOFS_SHARED_MEMORY_REG_MAX; i ++,p++)
   {
     p->storcli_done[rozofs_bt_storcli_idx] = 0;
   }

#if 0
   /*
   ** create the queues
   */
   ret = rozofs_queue_init(&rozofs_bt_rq_queue,nb_contexts);
   if (ret < 0) return RUC_NOK;   
  /*
  ** Create the pool of command  buffer for the threads
  */
  rozofs_bt_cmd_ring_size = nb_contexts + 8;
#endif
  /*
  ** Now create the threads
  */

  ret = rozofs_bt_stc_thread_create(storcli_id);

   if (ret < 0) return RUC_NOK;
   

   return RUC_OK;
}
