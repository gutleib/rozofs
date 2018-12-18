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

int af_unix_fuse_wr_socket_ref = -1;
 
 #define MICROLONG(time) ((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)


/**
*  Thread table
*/
rozofs_fuse_thread_ctx_t rozofs_fuse_wr_thread_ctx_tb[ROZOFS_MAX_FUSE_WR_THREADS];
int ROZOFS_MAX_WRITE_THREADS = 0;

/*
**__________________________________________________________________
*/
/**
   Initialize the current numnber of active write threads
*/
void init_write_thread_active(int nb_writeThreads){
  if (nb_writeThreads > ROZOFS_MAX_FUSE_WR_THREADS)  
  { 
    ROZOFS_MAX_WRITE_THREADS = ROZOFS_MAX_FUSE_WR_THREADS;
    return;
  }
  if (nb_writeThreads < 0) 
  { 
    ROZOFS_MAX_WRITE_THREADS = 0;
    return;
  }    
  ROZOFS_MAX_WRITE_THREADS = nb_writeThreads ;
  
}
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
int af_unix_fuse_wr_sock_create_internal(char *nameOfSocket,int size)
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
    warning("af_unix_fuse_wr_sock_create_internal socket(%s) %s", nameOfSocket, strerror(errno));
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
    warning("af_unix_fuse_wr_sock_create_internal bind(%s) %s", nameOfSocket, strerror(errno));
    return -1;
  }
  /*
  ** change the length for the send buffer, nothing to do for receive buf
  ** since it is out of the scope of the AF_SOCKET
  */
  ret= getsockopt(fd,SOL_SOCKET,SO_SNDBUF,(char*)&fdsize,&optionsize);
  if(ret<0)
  {
    warning("af_unix_fuse_wr_sock_create_internal getsockopt(%s) %s", nameOfSocket, strerror(errno));
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
    warning("af_unix_fuse_wr_sock_create_internal setsockopt(%s,%d) %s", nameOfSocket, fdsize, strerror(errno));
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
static inline void rozofs_fuse_th_fuse_write_buf(rozofs_fuse_thread_ctx_t *thread_ctx_p,rozofs_fuse_wr_thread_msg_t * msg) 
{
    struct timeval     timeDay;
    unsigned long long timeBefore, timeAfter;
    uint8_t           *arg_p;
    uint32_t          *header_size_p;
    rozofs_tx_ctx_t   *rozofs_tx_ctx_p = NULL;
    void              *xmit_buf = NULL;
    int               bufsize;
    int               position;
    XDR               xdrs;    
    struct rpc_msg   call_msg;
    uint32_t         null_val = 0;
    void *shared_buf_ref;
    void *fuse_ctx_p;
    void * kernel_fuse_write_request;
    int ret;
    rozofs_shared_buf_wr_hdr_t  *share_wr_p;    
    rozofs_tx_ctx_p = msg->rozofs_tx_ctx_p;

    gettimeofday(&timeDay,(struct timezone *)0);  
    timeBefore = MICROLONG(timeDay);	          
    /*
    ** update statistics
    */
    thread_ctx_p->stat.write_count++;     

    fuse_ctx_p = rozofs_tx_ctx_p->user_param;
    /*
    ** aget the reference of the xmit buffer
    */  
    xmit_buf = rozofs_tx_get_xmitBuf(rozofs_tx_ctx_p);
    if (xmit_buf == NULL)
    {
      /*
      ** something rotten here, we exit we an error
      ** without activating the FSM
      */
      fatal("no xmit buffer");;
      msg->errval = ENOMEM;
      goto error;
    } 
    /*
    ** get the pointer to the payload of the buffer
    */
    header_size_p  = (uint32_t*) ruc_buf_getPayload(xmit_buf);
    arg_p = (uint8_t*)(header_size_p+1);  
    /*
    ** create the xdr_mem structure for encoding the message
    */
    bufsize = ruc_buf_getMaxPayloadLen(xmit_buf);
    bufsize -= sizeof(uint32_t); /* skip length*/   
    xdrmem_create(&xdrs,(char*)arg_p,bufsize,XDR_ENCODE);
    /*
    ** fill in the rpc header
    */
    call_msg.rm_direction = CALL;
    /*
    ** allocate a xid for the transaction 
    */
    call_msg.rm_xid             = msg->rm_xid; 
    /*
    ** check the case of the READ since, we must set the value of the xid
    ** at the top of the buffer
    */
    RESTORE_FUSE_PARAM(fuse_ctx_p,shared_buf_ref);
    storcli_write_arg_t  *wr_args = (storcli_write_arg_t*)&msg->args;    
    if (shared_buf_ref == NULL)
    {
       fatal("No shared buffer reference");
    }
    share_wr_p = (rozofs_shared_buf_wr_hdr_t*)ruc_buf_getPayload(shared_buf_ref);
    share_wr_p->cmd[wr_args->cmd_idx].xid = (uint32_t)call_msg.rm_xid;
    /**
    * copy the buffer 
    */

    /*
    ** get the length to copy from the sshared memory
    */
    int len = share_wr_p->cmd[wr_args->cmd_idx].write_len;
    msg->size = (uint32_t)len;
    thread_ctx_p->stat.write_Byte_count+=msg->size;    
    /*
    ** Compute and write data offset considering 128bits alignment
    */
    int alignment = wr_args->off%16;
    share_wr_p->cmd[wr_args->cmd_idx].offset_in_buffer = alignment;
    /*
    ** Set pointer to the buffer start and adjust with alignment
    */
    uint8_t * buf_start = (uint8_t *)share_wr_p;
    buf_start += alignment+ROZOFS_SHMEM_WRITE_PAYLOAD_OFF;
    
    RESTORE_FUSE_PARAM(fuse_ctx_p,kernel_fuse_write_request);
    if (kernel_fuse_write_request != NULL)
    {
       ioctl_big_wr_t data;

       data.req = kernel_fuse_write_request;
       data.user_buf = buf_start;      
       data.user_bufsize = len;
       ret = ioctl(rozofs_fuse_ctx_p->fd,5,&data); 
       if (ret != 0)
       {
          msg->errval = EIO;
	  goto error;
       }
    }
    else
    {
       fatal("kernel_fuse_write_request NULL not supported !");
       memcpy(buf_start,wr_args->data.data_val,len);	  
    }

    call_msg.rm_call.cb_rpcvers = RPC_MSG_VERSION;
    /* XXX: prog and vers have been long historically :-( */
    call_msg.rm_call.cb_prog = (uint32_t)STORCLI_PROGRAM;
    call_msg.rm_call.cb_vers = (uint32_t)STORCLI_VERSION;
    if (! xdr_callhdr(&xdrs, &call_msg))
    {
       /*
       ** THIS MUST NOT HAPPEN
       */
       TX_STATS(ROZOFS_TX_ENCODING_ERROR);
       msg->errval = EPROTO;
       goto error;	
    }
    /*
    ** insert the procedure number, NULL credential and verifier
    */
    XDR_PUTINT32(&xdrs, (int32_t *)&msg->rpc_opcode);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
        
    /*
    ** ok now call the procedure to encode the message
    */
    if ((*msg->encode_fct)(&xdrs,&msg->args) == FALSE)
    {
       TX_STATS(ROZOFS_TX_ENCODING_ERROR);
       msg->errval = EPROTO;
       goto error;
    }
    /*
    ** Now get the current length and fill the header of the message
    */
    position = XDR_GETPOS(&xdrs);
    /*
    ** update the length of the message : must be in network order
    */
    *header_size_p = htonl(0x80000000 | position);
    /*
    ** set the payload length in the xmit buffer
    */
    int total_len = sizeof(*header_size_p)+ position;
    ruc_buf_setPayloadLen(xmit_buf,total_len);

    msg->status = 0;
out:
    /*
    ** Update statistics
    */
    gettimeofday(&timeDay,(struct timezone *)0);  
    timeAfter = MICROLONG(timeDay);
    thread_ctx_p->stat.write_time +=(timeAfter-timeBefore);  
    return rozofs_fuse_wr_th_send_response(thread_ctx_p,msg);

    
  error:
    msg->status = -1;
    goto out;      


}



/*
**   F U S E   W R I T E    T H R E A D
*/

void *rozofs_fuse_wr_thread(void *arg) {
  rozofs_fuse_wr_thread_msg_t   msg;
  rozofs_fuse_thread_ctx_t * ctx_p = (rozofs_fuse_thread_ctx_t*)arg;
  int                        bytesRcvd;

#if 1
    {
      struct sched_param my_priority;
      int policy=-1;
      int ret= 0;
      uma_dbg_thread_add_self("Fuse_Wr");
      
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

  //info("Fuse Write Thread %d Started !!\n",ctx_p->thread_idx);
  
  while(1) {
    if (ctx_p->thread_idx >= ROZOFS_MAX_WRITE_THREADS)
    {
       sleep(30);
       continue;
    }    
    /*
    ** read the north disk socket
    */
    bytesRcvd = recvfrom(af_unix_fuse_wr_socket_ref,
			 &msg,sizeof(msg), 
			 0,(struct sockaddr *)NULL,NULL);
    if (bytesRcvd == -1) {
      fatal("Write Thread %d recvfrom %s !!\n",ctx_p->thread_idx,strerror(errno));
      exit(0);
    }
    if (bytesRcvd != sizeof(msg)) {
      fatal("Write Thread %d socket is dead (%d/%d) %s !!\n",ctx_p->thread_idx,bytesRcvd,(int)sizeof(msg),strerror(errno));
      exit(0);    
    }
    
    switch (msg.opcode) {
    
      case ROZOFS_FUSE_WRITE_BUF:
        rozofs_fuse_th_fuse_write_buf(ctx_p,&msg);
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
int rozofs_fuse_wr_thread_create(char * hostname, int nb_threads, int instance_id) {
   int                        i;
   int                        err;
   pthread_attr_t             attr;
   rozofs_fuse_thread_ctx_t * thread_ctx_p;
   char                       socketName[128];

   /*
   ** clear the thread table
   */
   memset(rozofs_fuse_wr_thread_ctx_tb,0,sizeof(rozofs_fuse_wr_thread_ctx_tb));
   /*
   ** create the common socket to receive requests on
   */
   char * pChar = socketName;
   pChar += rozofs_string_append(pChar,ROZOFS_SOCK_FAMILY_FUSE_NORTH);
   *pChar++ = '_';
   *pChar++ = 'w';
   *pChar++ = 'r';
   *pChar++ = '_';
   pChar += rozofs_u32_append(pChar,instance_id);
   *pChar++ = '_';  
   pChar += rozofs_string_append(pChar,hostname);
   af_unix_fuse_wr_socket_ref = af_unix_fuse_wr_sock_create_internal(socketName,1024*32);
   if (af_unix_fuse_wr_socket_ref < 0) {
      fatal("af_unix_fuse_thread_create af_unix_fuse_wr_sock_create_internal(%s) %s",socketName,strerror(errno));
      return -1;   
   }
   /*
   ** Now create the threads
   */
   thread_ctx_p = rozofs_fuse_wr_thread_ctx_tb;
   for (i = 0; i < nb_threads ; i++) {
   
     thread_ctx_p->hostname = hostname;
     /*
     ** create the thread specific socket to send the response from 
     */
     pChar = socketName;
     pChar += rozofs_string_append(pChar,ROZOFS_SOCK_FAMILY_FUSE_NORTH);
     *pChar++ = '_';
     *pChar++ = 'w';
     *pChar++ = 'r';
     *pChar++ = '_';
     pChar += rozofs_u32_append(pChar,instance_id);
     *pChar++ = '_';  
     pChar += rozofs_string_append(pChar,hostname);
     *pChar++ = '_'; 
     pChar += rozofs_u32_append(pChar,i);
     thread_ctx_p->sendSocket = af_unix_fuse_wr_sock_create_internal(socketName,1024*32);
     if (thread_ctx_p->sendSocket < 0) {
	fatal("af_unix_fuse_thread_create af_unix_fuse_wr_sock_create_internal(%s) %s",socketName, strerror(errno));
	return -1;   
     }   
   
     err = pthread_attr_init(&attr);
     if (err != 0) {
       fatal("af_unix_fuse_thread_create pthread_attr_init(%d) %s",i,strerror(errno));
       return -1;
     }  

     thread_ctx_p->thread_idx = i;
     err = pthread_create(&thread_ctx_p->thrdId,&attr,rozofs_fuse_wr_thread,thread_ctx_p);
     if (err != 0) {
       fatal("af_unix_fuse_thread_create pthread_create(%d) %s",i, strerror(errno));
       return -1;
     }  
     
     thread_ctx_p++;
  }
  return 0;
}
 
