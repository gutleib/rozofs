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
#include <time.h>
#include <pthread.h> 
#include <rozofs/core/ruc_common.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/rozofs_socket_family.h>
#include <rozofs/core/uma_dbg_api.h>
#include "rozofs_storcli_mojette_thread_intf.h" 
#include "rozofs_storcli.h"
#include "storcli_main.h"

int af_unix_disk_socket_ref = -1;
 
 #define MICROLONG(time) ((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)


/**
*  Thread table
*/
rozofs_mojette_thread_ctx_t rozofs_mojette_thread_ctx_tb[ROZOFS_MAX_DISK_THREADS];

/*
**__________________________________________________________________________
*/
/**
   Creation of a AF_UNIX socket Datagram  in non blocking mode

   For the Mojette the socket is created in blocking mode
     
   @param name0fSocket : name of the AF_UNIX socket
   @param size: size in byte of the xmit buffer (the service double that value
   
    retval: >0 reference of the created AF_UNIX socket
    retval < 0 : error on socket creation 

*/
int af_unix_mojette_sock_create_internal(char *nameOfSocket,int size)
{
  int ret;    
  int fd=-1;  
  struct sockaddr_un addr;
  int fdsize;
  unsigned int optionsize=sizeof(fdsize);

  /* 
  ** create a datagram socket 
  */ 
  fd=socket(PF_UNIX,SOCK_DGRAM,0);
  if(fd<0)
  {
    warning("af_unix_mojette_sock_create_internal socket(%s) %s", nameOfSocket, strerror(errno));
    return -1;
  }
  /* 
  ** remove fd if it already exists 
  */
  ret = unlink(nameOfSocket);
  /* 
  ** named the socket reception side 
  */
  addr.sun_family= AF_UNIX;
  strcpy(addr.sun_path,nameOfSocket);
  ret=bind(fd,(struct sockaddr*)&addr,sizeof(addr));
  if(ret<0)
  {
    warning("af_unix_mojette_sock_create_internal bind(%s) %s", nameOfSocket, strerror(errno));
    return -1;
  }
  /*
  ** change the length for the send buffer, nothing to do for receive buf
  ** since it is out of the scope of the AF_SOCKET
  */
  ret= getsockopt(fd,SOL_SOCKET,SO_SNDBUF,(char*)&fdsize,&optionsize);
  if(ret<0)
  {
    warning("af_unix_mojette_sock_create_internal getsockopt(%s) %s", nameOfSocket, strerror(errno));
    return -1;
  }
  /*
  ** update the size, always the double of the input
  */
  fdsize=2*size;
  
  /* 
  ** set a new size for emission and 
  ** reception socket's buffer 
  */
  ret=setsockopt(fd,SOL_SOCKET,SO_SNDBUF,(char*)&fdsize,sizeof(int));
  if(ret<0)
  {
    warning("af_unix_mojette_sock_create_internal setsockopt(%s,%d) %s", nameOfSocket, fdsize, strerror(errno));
    return -1;
  }

  return(fd);
} 


/*
**__________________________________________________________________________
*/

/**
* callback for sending a response to a read ta remote entity from a mojette thread

 potential failure case:
  - socket_ref is out of range
  - connection is down
  
 @param buffer : pointer to the ruc_buffer that cointains the response
 @param socket_ref : index of the scoket context with the caller is remode, non significant for local caller
 @param user_param_p : pointer to a user opaque parameter (non significant for a remote access)
 
 @retval 0 : successfully submitted to the transport layer
 @retval < 0 error, the caller is intended to release the buffer
 */
int rozofs_storcli_mojette_thread_remote_rsp_cbk(void *buffer,uint32_t socket_ref,void *user_param_p)
{
   int write_len;
   af_unix_ctx_generic_t *this;
   char *pbuf;
   int bytes;

   write_len  = (int)ruc_buf_getPayloadLen(buffer);
   pbuf = (char *)ruc_buf_getPayload(buffer);
   this = af_unix_getObjCtx_p(socket_ref);
   if (this == NULL)
   {
      fatal("The socket does not exist");
   }
   bytes = send(this->socketRef,pbuf,(int)write_len,0);
  if (bytes == 0)
  {
     /*
     ** the other end is probably dead
     */
     fatal("Mojette thread cannot send back the response");
  }
  if (bytes > 0)
  {
     if (bytes != write_len)
     {
       fatal("Mojette thread socket partial send not supported when sending from mojette thread (%d/%d)",bytes,write_len);      
     }
     return 0;
  }
  /*
  ** error cases
  */
  fatal("Mojette thread socket send: sending error (%s)",strerror(errno));
  return -1;

  
} 
/*__________________________________________________________________________
*/
/**
*  Perform a mojette transform inverse

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storcli_mojette_inverse(rozofs_mojette_thread_ctx_t *thread_ctx_p,rozofs_stcmoj_thread_msg_t * msg) {
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  rozofs_storcli_ctx_t      * working_ctx_p;
  unsigned long long cycleBefore, cycleAfter;
  storcli_read_arg_t *storcli_read_rq_p;
  rozofs_storcli_resp_pf_t  saved_response_cbk;
  void *saved_xmitBuf;

    
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);

  cycleBefore = rdtsc();
	          
  /*
  ** update statistics
  */
  thread_ctx_p->stat.MojetteInverse_count++;
  
  working_ctx_p = msg->working_ctx;
  storcli_read_rq_p = (storcli_read_arg_t*)&working_ctx_p->storcli_read_arg;
  uint8_t layout         = storcli_read_rq_p->layout;
  
  rozofs_storcli_transform_inverse(working_ctx_p->prj_ctx,
                                   layout, storcli_read_rq_p->bsize,
                                   working_ctx_p->cur_nmbs2read,
                                   working_ctx_p->nb_projections2read,
                                   working_ctx_p->block_ctx_table,
                                   working_ctx_p->data_read_p,
                                   &working_ctx_p->effective_number_of_blocks,
				   &working_ctx_p->rozofs_storcli_prj_idx_table[0]);

  /*
  ** Update statistics
  */
  cycleAfter = rdtsc();
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.MojetteInverse_Byte_count += (working_ctx_p->effective_number_of_blocks*ROZOFS_BSIZE_BYTES(storcli_read_rq_p->bsize));
  thread_ctx_p->stat.MojetteInverse_cycle +=(cycleAfter-cycleBefore);  
  thread_ctx_p->stat.MojetteInverse_time +=(timeAfter-timeBefore);  
  /*
  ** Check if we can send the response from the Mojette thread. For this we MUST not have any projection to repair
  */
  {
    storcli_read_arg_t *storcli_read_rq_p;
    storcli_read_rq_p = (storcli_read_arg_t*)&working_ctx_p->storcli_read_arg;
    uint8_t layout         = storcli_read_rq_p->layout;
    uint8_t rozofs_safe    = rozofs_get_rozofs_safe(layout);
    
    int ret = rozofs_storcli_check_repair(working_ctx_p,rozofs_safe);  
    if (ret != 0)
    { 
      /*
      ** the reply will be done in the main thread for the case of the repair
      */
      goto out;          
    }

  } 
  /*
  ** Here is the case of the direct reply done from the thread
  ** First of all we must save the current callback and the xmit buffer since the called procedure remove
  ** the reference of the xmit buffer from the storcli context
  */
  saved_response_cbk = working_ctx_p->response_cbk;
  saved_xmitBuf = working_ctx_p->xmitBuf;
  working_ctx_p->response_cbk = rozofs_storcli_mojette_thread_remote_rsp_cbk;
  /*
  ** send the response
  */
  rozofs_storcli_read_reply_success(working_ctx_p);
  /*
  ** restore the xmit buffer reference in order to release it at working context release time
  */
  working_ctx_p->xmitBuf= saved_xmitBuf;
  /*
  ** change the opcode to indicate that only the release must take place
  */
  msg->opcode =STORCLI_MOJETTE_THREAD_INV_RELEASE;
   
out:
  /*
  ** send the response
  */
  storio_send_response(thread_ctx_p,msg,0);

}
/*__________________________________________________________________________
*/
/**
*  Perform a mojette transform forward

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storcli_mojette_forward(rozofs_mojette_thread_ctx_t *thread_ctx_p,rozofs_stcmoj_thread_msg_t * msg) {
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  unsigned long long cycleBefore, cycleAfter;
  rozofs_storcli_ctx_t      * working_ctx_p;
  rozofs_storcli_ingress_write_buf_t  *wr_proj_buf_p;;
  storcli_write_arg_no_data_t *storcli_write_rq_p;;
  uint8_t layout;;
  int i;
  int block_count = 0;
    
  
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);  
  cycleBefore = rdtsc();
  /*
  ** update statistics
  */
  thread_ctx_p->stat.MojetteForward_count++; 
  working_ctx_p  = msg->working_ctx;
  wr_proj_buf_p = working_ctx_p->wr_proj_buf;
  storcli_write_rq_p = &working_ctx_p->storcli_write_arg;
  layout = storcli_write_rq_p->layout;
  uint32_t bbytes = ROZOFS_BSIZE_BYTES(storcli_write_rq_p->bsize);

  /*
  ** Just to address the case of the buffer on which the fransform must apply
  */
  for (i = 0; i < ROZOFS_WR_MAX; i++)
  {
    if ( wr_proj_buf_p[i].state == ROZOFS_WR_ST_TRANSFORM_REQ)
    {
//       STORCLI_START_KPI(storcli_kpi_transform_forward);

       block_count += wr_proj_buf_p[i].number_of_blocks;
       rozofs_storcli_transform_forward(working_ctx_p->prj_ctx,  
                                               layout,storcli_write_rq_p->bsize,
                                               wr_proj_buf_p[i].first_block_idx, 
                                               wr_proj_buf_p[i].number_of_blocks, 
                                               working_ctx_p->timestamp,
                                               wr_proj_buf_p[i].last_block_size,
                                               wr_proj_buf_p[i].data);  
       wr_proj_buf_p[i].state =  ROZOFS_WR_ST_TRANSFORM_DONE; 
//       STORCLI_STOP_KPI(storcli_kpi_transform_forward,0);
    }    
  }
  msg->size = block_count*bbytes;
  thread_ctx_p->stat.MojetteForward_Byte_count += (block_count*bbytes);
  /*
  ** Update statistics
  */
  cycleAfter = rdtsc();

  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);

  thread_ctx_p->stat.MojetteForward_time +=(timeAfter-timeBefore);  
  thread_ctx_p->stat.MojetteForward_cycle +=(cycleAfter-cycleBefore);  

  storio_send_response(thread_ctx_p,msg,0);
/*
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.MojetteForward_time +=(timeAfter-timeBefore);  
*/
}    
/*
**_________________________________________________
*/
/*
**  MOJETTE    T H R E A D
*/

void *rozofs_stcmoj_thread(void *arg) {
  rozofs_mojette_thread_ctx_t * ctx_p = (rozofs_mojette_thread_ctx_t*)arg;
#ifdef STORCLI_MOJ_QUEUE
  rozofs_stcmoj_thread_msg_t   *msg_p;
#else
  int                        bytesRcvd;
  rozofs_stcmoj_thread_msg_t   msg;
#endif
  uma_dbg_thread_add_self("Mojette");

  //info("Disk Thread %d Started !!\n",ctx_p->thread_idx);

    /*
    ** change the scheduling policy
    */
    {
      struct sched_param my_priority;
      int policy=-1;
      int ret= 0;

      pthread_getschedparam(pthread_self(),&policy,&my_priority);
          DEBUG("Mojette thread Scheduling policy   = %s\n",
                    (policy == SCHED_OTHER) ? "SCHED_OTHER" :
                    (policy == SCHED_FIFO)  ? "SCHED_FIFO" :
                    (policy == SCHED_RR)    ? "SCHED_RR" :
                    "???");
 #if 1
      my_priority.sched_priority= 98;
      policy = SCHED_FIFO;
      ret = pthread_setschedparam(pthread_self(),policy,&my_priority);
      if (ret < 0) 
      {
	severe("error on sched_setscheduler: %s",strerror(errno));	
      }
      pthread_getschedparam(pthread_self(),&policy,&my_priority);
          DEBUG("RozoFS thread Scheduling policy (prio %d)  = %s\n",my_priority.sched_priority,
                    (policy == SCHED_OTHER) ? "SCHED_OTHER" :
                    (policy == SCHED_FIFO)  ? "SCHED_FIFO" :
                    (policy == SCHED_RR)    ? "SCHED_RR" :
                    "???");
 #endif        
     
    }
  while(1) {
    if ((ctx_p->thread_idx != 0) && (ctx_p->thread_idx >= common_config.mojette_thread_count))
    {
       sleep(30);
       continue;
    }
#ifdef STORCLI_MOJ_QUEUE
#warning FDL new code for Mojette request submit  
    /*
    ** Read some data from the queue
    */
    msg_p = rozofs_queue_get(&rozofs_storcli_mojette_req_queue);  
    switch (msg_p->opcode) {
    
      case STORCLI_MOJETTE_THREAD_INV:
        storcli_mojette_inverse(ctx_p,msg_p);
        break;
	
      case STORCLI_MOJETTE_THREAD_FWD:
        storcli_mojette_forward(ctx_p,msg_p);
        break;
       	
      default:
        fatal(" unexpected opcode : %d\n",msg_p->opcode);
        exit(0);       
    }
#else
    /*
    ** read the north disk socket
    */
    bytesRcvd = recvfrom(af_unix_disk_socket_ref,
			 &msg,sizeof(msg), 
			 0,(struct sockaddr *)NULL,NULL);
    if (bytesRcvd == -1) {
      fatal("Disk Thread %d recvfrom %s !!\n",ctx_p->thread_idx,strerror(errno));
      exit(0);
    }
    if (bytesRcvd == 0) {
      fatal("Disk Thread %d socket is dead %s !!\n",ctx_p->thread_idx,strerror(errno));
      exit(0);    
    }

    switch (msg.opcode) {
    
      case STORCLI_MOJETTE_THREAD_INV:
        storcli_mojette_inverse(ctx_p,&msg);
        break;
	
      case STORCLI_MOJETTE_THREAD_FWD:
        storcli_mojette_forward(ctx_p,&msg);
        break;
       	
      default:
        fatal(" unexpected opcode : %d\n",msg.opcode);
        exit(0);       
    }
    sched_yield();
#endif
  }
}
/*
** Create the threads that will handle all the disk requests

* @param hostname    storio hostname (for tests)
* @param eid    reference of the export
* @param storcli_idx    relative index of the storcli process
* @param nb_threads  number of threads to create
*  
* @retval 0 on success -1 in case of error
*/
int rozofs_stcmoj_thread_create(char * hostname,int eid,int storcli_idx, int nb_threads) {
   int                        i;
   int                        err;
   pthread_attr_t             attr;
   rozofs_mojette_thread_ctx_t * thread_ctx_p;
   char                       socketName[128];

   /*
   ** clear the thread table
   */
   memset(rozofs_mojette_thread_ctx_tb,0,sizeof(rozofs_mojette_thread_ctx_tb));
   /*
   ** create the common socket to receive requests on
   */
   sprintf(socketName,"%s_%s_%d_%d",ROZOFS_SOCK_FAMILY_STORCLI_MOJETTE_NORTH_SUNPATH,storcli_get_owner(),eid,storcli_idx);
   af_unix_disk_socket_ref = af_unix_mojette_sock_create_internal(socketName,1024*32);
   if (af_unix_disk_socket_ref < 0) {
      fatal("af_unix_disk_thread_create af_unix_mojette_sock_create_internal(%s) %s",socketName,strerror(errno));
      return -1;   
   }
   /*
   ** Now create the threads
   */
   thread_ctx_p = rozofs_mojette_thread_ctx_tb;
   for (i = 0; i < nb_threads ; i++) {
   
     thread_ctx_p->hostname = hostname;
     thread_ctx_p->eid = eid;
     thread_ctx_p->storcli_idx = storcli_idx;

     /*
     ** create the thread specific socket to send the response from 
     */
     sprintf(socketName,"%s_%s_%d_%d_%d",ROZOFS_SOCK_FAMILY_STORCLI_MOJETTE_NORTH_SUNPATH,storcli_get_owner(),eid,storcli_idx,i);
     thread_ctx_p->sendSocket = af_unix_mojette_sock_create_internal(socketName,1024*32);
     if (thread_ctx_p->sendSocket < 0) {
	fatal("af_unix_disk_thread_create af_unix_mojette_sock_create_internal(%s) %s",socketName, strerror(errno));
	return -1;   
     }   
   
     err = pthread_attr_init(&attr);
     if (err != 0) {
       fatal("af_unix_disk_thread_create pthread_attr_init(%d) %s",i,strerror(errno));
       return -1;
     }  

     thread_ctx_p->thread_idx = i;
     err = pthread_create(&thread_ctx_p->thrdId,&attr,rozofs_stcmoj_thread,thread_ctx_p);
     if (err != 0) {
       fatal("af_unix_disk_thread_create pthread_create(%d) %s",i, strerror(errno));
       return -1;
     }  
     
     thread_ctx_p++;
  }
  return 0;
}
 
