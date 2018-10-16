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
#include <rozofs/rpc/mproto.h>
#include <rozofs/rpc/rozofs_rpc_util.h>

#include "storaged_sub_thread_intf.h"


int storaged_sub_thread_socket_req = -1;
 
#define MICROLONG(time) ((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)


/**
*  Thread table
*/
storaged_sub_thread_ctx_t storaged_sub_thread_ctx_tb[STORAGED_MAX_SUB_THREADS];

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
int storaged_sub_thread_sock_create_internal(char *nameOfSocket,int size)
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
    warning("storaged_sub_thread_sock_create_internal socket(%s) %s", nameOfSocket, strerror(errno));
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
    warning("storaged_sub_thread_sock_create_internal bind(%s) %s", nameOfSocket, strerror(errno));
    return -1;
  }
  /*
  ** change the length for the send buffer, nothing to do for receive buf
  ** since it is out of the scope of the AF_SOCKET
  */
  ret= getsockopt(fd,SOL_SOCKET,SO_SNDBUF,(char*)&fdsize,&optionsize);
  if(ret<0)
  {
    warning("storaged_sub_thread_sock_create_internal getsockopt(%s) %s", nameOfSocket, strerror(errno));
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
    warning("storaged_sub_thread_sock_create_internal setsockopt(%s,%d) %s", nameOfSocket, fdsize, strerror(errno));
    return -1;
  }

  return(fd);
}  
/*
**__________________________________________________________________________
*/
/**
* encode the RCP reply
    
  @param p       : pointer to the generic rpc context
  @param arg_ret : returned argument to encode 
  
  @retval none

*/
int storaged_sub_thread_encode_rpc_response (rozorpc_srv_ctx_t *p,char * arg_ret) {
   uint8_t *pbuf;           /* pointer to the part that follows the header length */
   uint32_t *header_len_p;  /* pointer to the array that contains the length of the rpc message*/
   XDR xdrs;
   int len;

   if (p->xmitBuf == NULL) {
     // STAT
     severe("no xmit buffer");
     return -1;
   } 
   
   /*
   ** create xdr structure on top of the buffer that will be used for sending the response
   */ 
   header_len_p = (uint32_t*)ruc_buf_getPayload(p->xmitBuf); 
   pbuf = (uint8_t*) (header_len_p+1);            
   len = (int)ruc_buf_getMaxPayloadLen(p->xmitBuf);
   len -= sizeof(uint32_t);
   xdrmem_create(&xdrs,(char*)pbuf,len,XDR_ENCODE); 
   
   if (rozofs_encode_rpc_reply(&xdrs,(xdrproc_t)p->xdr_result,(caddr_t)arg_ret,p->src_transaction_id) != TRUE) {
     ROZORPC_SRV_STATS(ROZORPC_SRV_ENCODING_ERROR);
     severe("rpc reply encoding error");
     return -1;    
   }       
   /*
   ** compute the total length of the message for the rpc header and add 4 bytes more bytes for
   ** the ruc buffer to take care of the header length of the rpc message.
   */
   int total_len = xdr_getpos(&xdrs);
   *header_len_p = htonl(0x80000000 | total_len);
   total_len += sizeof(uint32_t);
   ruc_buf_setPayloadLen(p->xmitBuf,total_len);
   
   return 0;
}
/*__________________________________________________________________________
*/
/**
*  Perform a file remove

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storaged_sub_thread_remove2(storaged_sub_thread_ctx_t *thread_ctx_p,storaged_sub_thread_msg_t * msg) {
  struct timeval                 timeDay;
  unsigned long long             timeBefore, timeAfter;
  rozorpc_srv_ctx_t            * rpcCtx;
  mp_remove2_arg_t             * args;  
  mp_status_ret_t                ret;
  int                            status=-1;
      
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);  

  ret.status = MP_FAILURE;          
 
   /*
  ** update statistics
  */
  thread_ctx_p->stat.remove_count++; 
  
  rpcCtx = msg->rpcCtx;
  args   = (mp_remove2_arg_t*) ruc_buf_getPayload(rpcCtx->decoded_arg);

  if (storage_rm2_file(msg->st, (unsigned char *) args->fid, args->spare) != 0) {
    ret.mp_status_ret_t_u.error = errno;
    thread_ctx_p->stat.remove_errors++ ;   
    goto out;   
  }    
  ret.status = MP_SUCCESS;
  
out:  
  storaged_sub_thread_encode_rpc_response(rpcCtx,(char*)&ret);  

  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.remove_time +=(timeAfter-timeBefore);  
  storaged_sub_thread_intf_send_response(thread_ctx_p,msg,status);
}  
/*__________________________________________________________________________
*/
/**
*  Perform a file remove

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storaged_sub_thread_remove(storaged_sub_thread_ctx_t *thread_ctx_p,storaged_sub_thread_msg_t * msg) {
  struct timeval                 timeDay;
  unsigned long long             timeBefore, timeAfter;
  rozorpc_srv_ctx_t            * rpcCtx;
  mp_remove_arg_t              * args;  
  mp_status_ret_t                ret;
  int                            status=-1;
      
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);  

  ret.status = MP_FAILURE;          
 
   /*
  ** update statistics
  */
  thread_ctx_p->stat.remove_count++; 
  
  rpcCtx = msg->rpcCtx;
  args   = (mp_remove_arg_t*) ruc_buf_getPayload(rpcCtx->decoded_arg);

  if (storage_rm_file(msg->st, (unsigned char *) args->fid) != 0) {
    ret.mp_status_ret_t_u.error = errno;
    thread_ctx_p->stat.remove_errors++ ;   
    goto out;   
  } 
  ret.status = MP_SUCCESS;
  
out:  
  storaged_sub_thread_encode_rpc_response(rpcCtx,(char*)&ret);  
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.remove_time +=(timeAfter-timeBefore);  
  storaged_sub_thread_intf_send_response(thread_ctx_p,msg,status);
} 
/*__________________________________________________________________________
*/
/**
*  Compute file size and allocated sectors

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storaged_sub_thread_size(storaged_sub_thread_ctx_t *thread_ctx_p,storaged_sub_thread_msg_t * msg) {
  struct timeval                 timeDay;
  unsigned long long             timeBefore, timeAfter;
  rozorpc_srv_ctx_t            * rpcCtx;
  mp_size_arg_t                * args;  
  mp_size_ret_t                  ret;
 int                             status=-1;
      
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);  

  ret.status = MP_FAILURE;          
 
   /*
  ** update statistics
  */
  thread_ctx_p->stat.remove_count++; 
  
  rpcCtx = msg->rpcCtx;
  args   = (mp_size_arg_t*) ruc_buf_getPayload(rpcCtx->decoded_arg);

  if (storage_size_file(msg->st, (unsigned char *) args->fid, args->spare, 
                        &ret.mp_size_ret_t_u.rsp.nb_chunk, 
                        &ret.mp_size_ret_t_u.rsp.file_size_in_blocks, 
                        &ret.mp_size_ret_t_u.rsp.allocated_sectors) != 0) {
    ret.mp_size_ret_t_u.error = errno;
    thread_ctx_p->stat.size_errors++ ;   
    goto out;   
  } 
  ret.status = MP_SUCCESS;
  
out:  
  storaged_sub_thread_encode_rpc_response(rpcCtx,(char*)&ret);  
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.size_time +=(timeAfter-timeBefore);  
  storaged_sub_thread_intf_send_response(thread_ctx_p,msg,status);
} 
/*__________________________________________________________________________
*/
/**
*  Perform a file remove

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storaged_sub_thread_list_bins(storaged_sub_thread_ctx_t *thread_ctx_p,storaged_sub_thread_msg_t * msg) {
  struct timeval                  timeDay;
  unsigned long long timeBefore,  timeAfter;
  rozorpc_srv_ctx_t             * rpcCtx;
  mp_list_bins_files_arg_t      * args;  
  mp_list_bins_files_ret_t        ret;
      
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);  

  ret.status = MP_FAILURE;          
 
   /*
  ** update statistics
  */
  thread_ctx_p->stat.list_bins_count++; 
  
  rpcCtx = msg->rpcCtx;
  args   = (mp_list_bins_files_arg_t*) ruc_buf_getPayload(rpcCtx->decoded_arg);

  // It's necessary
  memset(&ret, 0, sizeof(mp_list_bins_files_ret_t));

  if (storage_list_bins_files_to_rebuild(
                    msg->st, 
        	    args->rebuild_sid,
        	    &args->device,
        	    &args->spare,
		    &args->slice,
        	    &args->cookie,
        	    (bins_file_rebuild_t **)& ret.mp_list_bins_files_ret_t_u.reply.children,
        	    (uint8_t *) & ret.mp_list_bins_files_ret_t_u.reply.eof) != 0) {
    ret.mp_list_bins_files_ret_t_u.error = errno;
    thread_ctx_p->stat.list_bins_errors++ ;   
    goto out;   
  }   
  ret.status = MP_SUCCESS;
  ret.mp_list_bins_files_ret_t_u.reply.cookie = args->cookie;
  ret.mp_list_bins_files_ret_t_u.reply.spare  = args->spare;    
  ret.mp_list_bins_files_ret_t_u.reply.device = args->device;
  ret.mp_list_bins_files_ret_t_u.reply.slice  = args->slice;
		    
out:
  storaged_sub_thread_encode_rpc_response(rpcCtx,(char*)&ret);
  
  //xdr_free((xdrproc_t)rpcCtx->xdr_result, (char *) &ret);   
  mp_children_t child = ret.mp_list_bins_files_ret_t_u.reply.children;
  mp_children_t next;
  while (child) {
    next = child->next;
    xfree(child);
    child = next;
  }

  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.list_bins_time +=(timeAfter-timeBefore);  
  storaged_sub_thread_intf_send_response(thread_ctx_p,msg,0);
}    
   
/*
**_________________________________________________
*/
/*
**  MOJETTE    T H R E A D
*/

void *storaged_sub_thread(void *arg) {
  storaged_sub_thread_msg_t   msg;
  storaged_sub_thread_ctx_t * ctx_p = (storaged_sub_thread_ctx_t*)arg;
  int                        bytesRcvd;
  char                       name[16];

  sprintf(name,"remove%d",ctx_p->thread_idx);
  uma_dbg_thread_add_self(name);

  //info("Disk Thread %d Started !!\n",ctx_p->thread_idx);

    /*
    ** change the scheduling policy
    */
    {
      struct sched_param my_priority;
      int policy=-1;
      int ret= 0;

      pthread_getschedparam(pthread_self(),&policy,&my_priority);
          DEBUG("sub thread Scheduling policy   = %s\n",
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
          DEBUG("sub thread Scheduling policy (prio %d)  = %s\n",my_priority.sched_priority,
                    (policy == SCHED_OTHER) ? "SCHED_OTHER" :
                    (policy == SCHED_FIFO)  ? "SCHED_FIFO" :
                    (policy == SCHED_RR)    ? "SCHED_RR" :
                    "???");
 #endif        
     
    }
  while(1) {
    /*
    ** read the north disk socket
    */
    bytesRcvd = recvfrom(storaged_sub_thread_socket_req,
			 &msg,sizeof(msg), 
			 0,(struct sockaddr *)NULL,NULL);
    if (bytesRcvd == -1) {
      fatal("sub Thread %d recvfrom %s !!\n",ctx_p->thread_idx,strerror(errno));
      exit(0);
    }
    if (bytesRcvd == 0) {
      fatal("sub Thread %d socket is dead %s !!\n",ctx_p->thread_idx,strerror(errno));
      exit(0);    
    }

    switch (msg.opcode) {
    
      case MP_REMOVE2:
        storaged_sub_thread_remove2(ctx_p,&msg);
        break;
      case MP_REMOVE:
        storaged_sub_thread_remove(ctx_p,&msg);
        break;
      case MP_LIST_BINS_FILES:
        storaged_sub_thread_list_bins(ctx_p,&msg);
        break;		
      case MP_SIZE:
        storaged_sub_thread_size(ctx_p,&msg);
        break;
       	
      default:
        fatal(" unexpected opcode : %d\n",msg.opcode);
        exit(0);       
    }
    sched_yield();
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
int storaged_sub_thread_create(char * sockName, char * hostname, int nb_threads) {
   int                        i;
   int                        err;
   pthread_attr_t             attr;
   storaged_sub_thread_ctx_t * thread_ctx_p;
   char                       socketName[128];

   /*
   ** clear the thread table
   */
   memset(storaged_sub_thread_ctx_tb,0,sizeof(storaged_sub_thread_ctx_tb));
   /*
   ** create the common socket to receive requests on
   */
   storaged_sub_thread_socket_req = storaged_sub_thread_sock_create_internal(sockName,1024*32);
   if (storaged_sub_thread_socket_req < 0) {
      fatal("af_unix_disk_thread_create storaged_sub_thread_sock_create_internal(%s) %s",socketName,strerror(errno));
      return -1;   
   }
   /*
   ** Now create the threads
   */
   thread_ctx_p = storaged_sub_thread_ctx_tb;
   for (i = 0; i < nb_threads ; i++) {
   
     thread_ctx_p->hostname = strdup(hostname);

     /*
     ** create the thread specific socket to send the response from 
     */
     sprintf(socketName,"%s_%s_%d",ROZOFS_SOCK_FAMILY_STORAGED,hostname,i);
     thread_ctx_p->sendSocket = storaged_sub_thread_sock_create_internal(socketName,1024*32);
     if (thread_ctx_p->sendSocket < 0) {
	fatal("af_unix_disk_thread_create storaged_sub_thread_sock_create_internal(%s) %s",socketName, strerror(errno));
	return -1;   
     }   
   
     err = pthread_attr_init(&attr);
     if (err != 0) {
       fatal("af_unix_disk_thread_create pthread_attr_init(%d) %s",i,strerror(errno));
       return -1;
     }  

     thread_ctx_p->thread_idx = i;
     err = pthread_create(&thread_ctx_p->thrdId,&attr,storaged_sub_thread,thread_ctx_p);
     if (err != 0) {
       fatal("af_unix_disk_thread_create pthread_create(%d) %s",i, strerror(errno));
       return -1;
     }  
     
     thread_ctx_p++;
  }
  return 0;
}
 
