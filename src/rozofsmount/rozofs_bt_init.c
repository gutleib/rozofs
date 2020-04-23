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
#include "rozofs_bt_api.h"
#include "rozofs_bt_proto.h"
#include "rozofs_bt_inode.h"
#include "rozofs_bt_dirent.h"



rozofs_queue_t rozofs_bt_rq_queue;
int rozofs_bt_threads_count;
uint32_t rozofs_bt_cmd_ring_size = 0;
uint32_t rozofs_bt_cmd_ring_idx = 0; 
rozofs_bt_thread_msg_t *rozofs_bt_req_pool_p = NULL;

int fdl_debug = 1;
int rozofs_bt_debug = 0;

/**
*  Thread table
*/
rozofs_bt_thread_ctx_t rozofs_bt_thread_ctx_tb[ROZOFS_BT_MAX_THREADS];


/*
**_________________________________________________
*/
/*
**  BATCH    T H R E A D
*/

void *rozofs_bt_thread(void *arg) 
{
//  rozofs_bt_thread_ctx_t * ctx_p = (rozofs_bt_thread_ctx_t*)arg;
  rozofs_bt_thread_msg_t   *msg_p;
   rozo_batch_hdr_t *hdr_p;
   int socket_id;

  uma_dbg_thread_add_self("Batch_cmd");
  while(1) {
#if 0
    if ((ctx_p->thread_idx != 0) && (ctx_p->thread_idx >= common_config.mojette_thread_count))
    {
       sleep(30);
       continue;
    }
#endif
    /*
    ** Read some data from the queue
    */
    msg_p = rozofs_queue_get(&rozofs_bt_rq_queue);  
    hdr_p = (rozo_batch_hdr_t*) ruc_buf_getPayload(msg_p); 
    socket_id = (int)hdr_p->private;
    switch (hdr_p->opcode) {
    
      case ROZO_BATCH_MEMREG:
        rozofs_bt_process_memreg(msg_p,socket_id);
        break;

      case ROZO_BATCH_MEMCREATE:
        rozofs_bt_process_memcreate(msg_p,socket_id);
        break;
       	       	
      default:
        fatal(" unexpected opcode : %d\n",msg_p->opcode);
        exit(0);       
    }
  }
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
int rozofs_bt_thread_create(int nb_threads) {
   int                        i;
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
   for (i = 0; i < nb_threads ; i++) {
     /*
     ** create the socket that the thread will use for sending back response 
     */
     thread_ctx_p->sendSocket = socket(AF_UNIX,SOCK_DGRAM,0);
     if (thread_ctx_p->sendSocket < 0) {
	fatal("rozofs_bt_thread_create fail to create socket: %s", strerror(errno));
	return -1;   
     }   
   
     err = pthread_attr_init(&attr);
     if (err != 0) {
       fatal("rozofs_bt_thread_create pthread_attr_init(%d) %s",i,strerror(errno));
       return -1;
     }  

     thread_ctx_p->thread_idx = i;
     err = pthread_create(&thread_ctx_p->thrdId,&attr,rozofs_bt_thread,thread_ctx_p);
     if (err != 0) {
       fatal("rozofs_bt_thread_create pthread_create(%d) %s",i, strerror(errno));
       return -1;
     }  
     
     thread_ctx_p++;
  }
  return 0;
}

/*
**_________________________________________________________________________________________________
*/
int rozofs_bt_init(uint16_t instance,int eid,int nb_contexts)
{
   int ret;

   /*
   ** ask for the creation of the shared memory for the case of rozofsmount, just a registration for the storcli
   */   
//   ret = rozofs_shm_init((int) instance,1);
//   if (ret < 0) return RUC_NOK;
   warning("nbe context %d\n",nb_contexts);
   ret = rozofs_bt_north_interface_init(eid,instance,nb_contexts);
   if (ret < 0) return RUC_NOK;
   /*
   ** create the queues
   */
   ret = rozofs_queue_init(&rozofs_bt_rq_queue,nb_contexts);
   if (ret < 0) return RUC_NOK;   
  /*
  ** Create the pool of command  buffer for the threads
  */
  rozofs_bt_cmd_ring_size = nb_contexts + 8;
  rozofs_bt_req_pool_p = malloc(sizeof(rozofs_bt_thread_msg_t)*(rozofs_bt_cmd_ring_size));
  if (rozofs_bt_req_pool_p == NULL)
  {
     severe("Cannot allocated memory for Mojette threads \n");
     return RUC_NOK;
  } 
  rozofs_bt_cmd_ring_idx = 0; 
  /*
  ** init of the inode section
  */
  ret = rozofs_bt_inode_init(nb_contexts,eid);
  if (ret < 0) return RUC_NOK;

  /*
  ** Now create the threads
  */
  ret = rozofs_bt_thread_intf_create("localhost",instance,ROZOFS_BT_MAX_THREADS);
//  ret = rozofs_bt_thread_create(ROZOFS_BT_MAX_THREADS);
   if (ret < 0) return RUC_NOK;
   
       uma_dbg_addTopic_option("trkrd_profiler", show_trkrd_profiler,UMA_DBG_OPTION_RESET);

       uma_dbg_addTopic("trkrd_cache", show_trkrd_cache);

   /*
   ** create the dirent thread
   */
   ret = rozofs_bt_dirent_init(32,"localhost",eid);
   if (ret != 0)
   {
     return RUC_NOK;
   }
   /*
   ** Create the thread for READDIR & LOOKUP (dirent files)
   */   
   ret = rozofs_bt_dirent_thread_intf_create("localhost",instance,1);
   if (ret != 0)
   {
     return RUC_NOK;
   }
   
   return RUC_OK;
}
