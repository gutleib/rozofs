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
#include "rozofs_fuse_thread_intf.h" 

int af_unix_fuse_socket_ref = -1;
 
 #define MICROLONG(time) ((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)


/**
*  Thread table
*/
rozofs_fuse_thread_ctx_t rozofs_fuse_thread_ctx_tb[ROZOFS_MAX_FUSE_THREADS];

/*
**__________________________________________________________________________
*/
/**
   Creation of a AF_UNIX socket Datagram  in non blocking mode

   For the disk the socket is created in blocking mode
     
   @param name0fSocket : name of the AF_UNIX socket
   @param size: size in byte of the xmit buffer (the service double that value
   
    retval: >0 reference of the created AF_UNIX socket
    retval < 0 : error on socket creation 

*/
int af_unix_fuse_sock_create_internal(char *nameOfSocket,int size)
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
    warning("af_unix_fuse_sock_create_internal socket(%s) %s", nameOfSocket, strerror(errno));
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
    warning("af_unix_fuse_sock_create_internal bind(%s) %s", nameOfSocket, strerror(errno));
    return -1;
  }
  /*
  ** change the length for the send buffer, nothing to do for receive buf
  ** since it is out of the scope of the AF_SOCKET
  */
  ret= getsockopt(fd,SOL_SOCKET,SO_SNDBUF,(char*)&fdsize,&optionsize);
  if(ret<0)
  {
    warning("af_unix_fuse_sock_create_internal getsockopt(%s) %s", nameOfSocket, strerror(errno));
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
    warning("af_unix_fuse_sock_create_internal setsockopt(%s,%d) %s", nameOfSocket, fdsize, strerror(errno));
    return -1;
  }

  return(fd);
}  

/*__________________________________________________________________________
*/
/**
*  Read data from a file

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void rozofs_fuse_th_fuse_reply_buf(rozofs_fuse_thread_ctx_t *thread_ctx_p,rozofs_fuse_thread_msg_t * msg) {
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  char *buf_sharem = (char *)msg->payload;
      
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);	          
  /*
  ** update statistics
  */
  thread_ctx_p->stat.write_count++;     
  thread_ctx_p->stat.write_Byte_count+=msg->size; 
  /*
  ** Check if the storcli did a direct write in the page cache
  */
  if (msg->use_page_cache)  
  {
    thread_ctx_p->stat.use_page_cache_count++;
    fuse_reply_err( msg->req,EREMOTE); 
  } 
  else  fuse_reply_buf(msg->req, (char *) buf_sharem, msg->size);

  rozofs_fuse_th_send_response(thread_ctx_p,msg,0);

  /*
  ** Update statistics
  */
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.write_time +=(timeAfter-timeBefore);  
}



/*
**   F U S E  R E P L Y    T H R E A D
*/

void *rozofs_fuse_thread(void *arg) {
  rozofs_fuse_thread_msg_t   msg;
  rozofs_fuse_thread_ctx_t * ctx_p = (rozofs_fuse_thread_ctx_t*)arg;
  int                        bytesRcvd;

#if 1
    {
      struct sched_param my_priority;
      int policy=-1;
      int ret= 0;
      uma_dbg_thread_add_self("Fuse_Rsp");
      
      pthread_getschedparam(pthread_self(),&policy,&my_priority);
          info("storio main thread Scheduling policy   = %s\n",
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
#endif     

  //info("Fuse Thread %d Started !!\n",ctx_p->thread_idx);
  
  while(1) {
    if (ctx_p->thread_idx >= common_config.reply_thread_count)
    {
       sleep(30);
       continue;
    }  
    /*
    ** read the north disk socket
    */
    bytesRcvd = recvfrom(af_unix_fuse_socket_ref,
			 &msg,sizeof(msg), 
			 0,(struct sockaddr *)NULL,NULL);
    if (bytesRcvd == -1) {
      fatal("Read Thread %d recvfrom %s !!\n",ctx_p->thread_idx,strerror(errno));
      exit(0);
    }
    if (bytesRcvd != sizeof(msg)) {
      fatal("Read Thread %d socket is dead (%d/%d) %s !!\n",ctx_p->thread_idx,bytesRcvd,(int)sizeof(msg),strerror(errno));
      exit(0);    
    }
    
    switch (msg.opcode) {
    
      case ROZOFS_FUSE_REPLY_BUF:
        rozofs_fuse_th_fuse_reply_buf(ctx_p,&msg);
        break;
	
      default:
        fatal(" unexpected opcode : %d\n",msg.opcode);
        exit(0);       
    }
//    sched_yield();
  }
}
/*
** Create the threads that will handle all the disk requests

* @param hostname    storio hostname (for tests)
* @param nb_threads  number of threads to create
*  
* @retval 0 on success -1 in case of error
*/
int rozofs_fuse_thread_create(char * hostname, int nb_threads, int instance_id) {
   int                        i;
   int                        err;
   pthread_attr_t             attr;
   rozofs_fuse_thread_ctx_t * thread_ctx_p;
   char                       socketName[128];

   /*
   ** clear the thread table
   */
   memset(rozofs_fuse_thread_ctx_tb,0,sizeof(rozofs_fuse_thread_ctx_tb));
   /*
   ** create the common socket to receive requests on
   */
   char * pChar = socketName;
   pChar += rozofs_string_append(pChar,ROZOFS_SOCK_FAMILY_FUSE_NORTH);
   *pChar++ = '_';
   pChar += rozofs_u32_append(pChar,instance_id);
   *pChar++ = '_';  
   pChar += rozofs_string_append(pChar,hostname);
   af_unix_fuse_socket_ref = af_unix_fuse_sock_create_internal(socketName,1024*32);
   if (af_unix_fuse_socket_ref < 0) {
      fatal("af_unix_fuse_thread_create af_unix_fuse_sock_create_internal(%s) %s",socketName,strerror(errno));
      return -1;   
   }
   /*
   ** Now create the threads
   */
   thread_ctx_p = rozofs_fuse_thread_ctx_tb;
   for (i = 0; i < nb_threads ; i++) {
   
     thread_ctx_p->hostname = hostname;
     /*
     ** create the thread specific socket to send the response from 
     */
     pChar = socketName;
     pChar += rozofs_string_append(pChar,ROZOFS_SOCK_FAMILY_FUSE_NORTH);
     *pChar++ = '_';
     pChar += rozofs_u32_append(pChar,instance_id);
     *pChar++ = '_';  
     pChar += rozofs_string_append(pChar,hostname);
     *pChar++ = '_'; 
     pChar += rozofs_u32_append(pChar,i);
     thread_ctx_p->sendSocket = af_unix_fuse_sock_create_internal(socketName,1024*32);
     if (thread_ctx_p->sendSocket < 0) {
	fatal("af_unix_fuse_thread_create af_unix_fuse_sock_create_internal(%s) %s",socketName, strerror(errno));
	return -1;   
     }   
   
     err = pthread_attr_init(&attr);
     if (err != 0) {
       fatal("af_unix_fuse_thread_create pthread_attr_init(%d) %s",i,strerror(errno));
       return -1;
     }  

     thread_ctx_p->thread_idx = i;
     err = pthread_create(&thread_ctx_p->thrdId,&attr,rozofs_fuse_thread,thread_ctx_p);
     if (err != 0) {
       fatal("af_unix_fuse_thread_create pthread_create(%d) %s",i, strerror(errno));
       return -1;
     }  
     
     thread_ctx_p++;
  }
  return 0;
}
 
