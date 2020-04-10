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

#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <errno.h>
#include <sched.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/profile.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/rozofs_rpc_non_blocking_generic_srv.h>
#include <rozofs/core/ruc_buffer_debug.h>
#include "expbt_global.h"
#include "expbt_north_intf.h"
#include <rozofs/rpc/expbt_protocol.h>
#include "expbt_trk_thread_intf.h"

DECLARE_PROFILING(expbt_profiler_t); 

#define DISK_SO_SENDBUF  (300*1024) 

int        af_unix_trk_thread_count=0;
int        af_unix_trk_pending_req_count = 0;

 
int expbt_trk_thread_create(char * hostname, int nb_threads) ;

void *expbt_recv_sockctrl_p=NULL;
 
expbt_trk_thread_ctx_t expbt_trk_thread_ctx_tb[EXPBT_MAX_TRK_THREADS];

struct  sockaddr_un expbt_recv_af_unix_socket_name;
int expbt_recv_af_unix_socket_ref = -1;
int expbt_xmit_af_unix_socket_ref = -1;

 /**
 * prototypes
 */
uint32_t af_unix_trk_rcvReadysock(void * af_unix_trk_ctx_p,int socketId);
uint32_t af_unix_trk_rcvMsgsock(void * af_unix_trk_ctx_p,int socketId);
uint32_t af_unix_trk_xmitReadysock(void * af_unix_trk_ctx_p,int socketId);
uint32_t af_unix_trk_xmitEvtsock(void * af_unix_trk_ctx_p,int socketId);

/*
**  Call back function for socket controller
*/
ruc_sockCallBack_t af_unix_trk_callBack_sock=
  {
     af_unix_trk_rcvReadysock,
     af_unix_trk_rcvMsgsock,
     af_unix_trk_xmitReadysock,
     af_unix_trk_xmitEvtsock
  };
  
  /*
**__________________________________________________________________________
*/
/**
  Application callBack:

  Called from the socket controller. 

serial
  @param unused: not used
  @param socketId: reference of the socket (not used)
 
  @retval : always FALSE
*/

uint32_t af_unix_trk_xmitReadysock(void * unused,int socketId)
{

    return FALSE;
}
/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   Called from the socket controller upon receiving a xmit ready event
   for the associated socket. That callback is activeted only if the application
   has replied TRUE in rozofs_fuse_xmitReadysock().
   
   It typically the processing of a end of congestion on the socket

    
  @param unused: not used
  @param socketId: reference of the socket (not used)
 
   @retval :always TRUE
*/
uint32_t af_unix_trk_xmitEvtsock(void * unused,int socketId)
{
   
    return TRUE;
}
/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   receiver ready function: called from socket controller.
   The module is intended to return if the receiver is ready to receive a new message
   and FALSE otherwise

    
  @param unused: not used
  @param socketId: reference of the socket (not used)
 
  @retval : TRUE-> receiver ready
  @retval : FALSE-> receiver not ready
*/

uint32_t af_unix_trk_rcvReadysock(void * unused,int socketId)
{
  return TRUE;
}

/*
**__________________________________________________________________________
*/
/**
  Processes a disk response

   Called from the socket controller when there is a response from a disk thread
   the response is either for a disk read or write
    
  @param rozorpc_srv_ctx_p: pointer RPC context that contains the command & the response
 
  @retval :none
*/
void af_unix_trk_response(rozorpc_srv_ctx_t *rozorpc_srv_ctx_p) 
{
  int                            ret;
  uint64_t                       tic, toc;  
  struct timeval                 tv;  
  expbt_msg_hdr_t   *hdr_p,*hdr_rsp_p;

  tic = rozorpc_srv_ctx_p->profiler_time;
  hdr_rsp_p =  (expbt_msg_hdr_t*)ruc_buf_getPayload(rozorpc_srv_ctx_p->recv_buf);
  hdr_p = (expbt_msg_hdr_t   *)ruc_buf_getPayload(rozorpc_srv_ctx_p->decoded_arg);   


  switch (hdr_p->opcode) {
  
    case EXP_BT_TRK_READ:
    {
      STOP_PROFILING_IO(file_read,hdr_rsp_p->len-(sizeof(expbt_msg_hdr_t)-sizeof(expbt_trk_main_rsp_t)));

      break;
    }  
  
    case EXP_BT_TRK_CHECK:
    {
      expbt_msg_t *rsp_msg_p;
      
      rsp_msg_p = (expbt_msg_t*) hdr_rsp_p;
      STOP_PROFILING_IO(file_check,rsp_msg_p->check_rsp.rsp.nb_responses);
      break;
    }  
    case EXP_BT_DIRENT_LOAD:
    {
      STOP_PROFILING(load_dentry);
      break;
    }  
    default:
      severe("Unexpected opcode %d", hdr_p->opcode);
  }
 
  /*
  ** send the response towards the storcli process that initiates the disk operation
  */
  ret = af_unix_generic_send_stream_with_idx((int)rozorpc_srv_ctx_p->socketRef,rozorpc_srv_ctx_p->recv_buf); 
  if (ret == 0) {
    /**
    * success so remove the reference of the xmit buffer since it is up to the called
    * function to release it
    */
    ROZORPC_SRV_STATS(ROZORPC_SRV_SEND);
    rozorpc_srv_ctx_p->recv_buf = NULL;
  }
  else {
    ROZORPC_SRV_STATS(ROZORPC_SRV_SEND_ERROR);
  }    
  rozorpc_srv_release_context(rozorpc_srv_ctx_p);          
}

/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   Called from the socket controller when there is a message pending on the
   socket associated with the context provide in input arguments.
   
   That service is intended to process a response sent by a disk thread

    
  @param unused: user parameter not used by the application
  @param socketId: reference of the socket 
 
   @retval : TRUE-> xmit ready event expected
  @retval : FALSE-> xmit  ready event not expected
*/

uint32_t af_unix_trk_rcvMsgsock(void * unused,int socketId)
{
rozorpc_srv_ctx_t *rozorpc_srv_ctx_p = NULL;
  int                        bytesRcvd;
  int eintr_count = 0;

  /*
  ** disk responses have the highest priority, loop on the socket until
  ** the socket becomes empty
  */
  while(1) {  
    /*
    ** check if there are some pending requests
    */
    if (af_unix_trk_pending_req_count == 0)
    {
     goto out;
    }
    /*
    ** read the north disk socket
    */
    bytesRcvd = recvfrom(socketId,&rozorpc_srv_ctx_p,sizeof(rozorpc_srv_ctx_t *),0,(struct sockaddr *)NULL,NULL);
    if (bytesRcvd == -1) {
     switch (errno)
     {
       case EAGAIN:
        /*
        ** the socket is empty
        */
        goto out;

       case EINTR:
         /*
         ** re-attempt to read the socket
         */
         eintr_count++;
         if (eintr_count < 3) continue;
         /*
         ** here we consider it as a error
         */
         severe ("Thread Response error too many eintr_count %d",eintr_count);
         goto out;

       case EBADF:
       case EFAULT:
       case EINVAL:
       default:
         /*
         ** We might need to double checl if the socket must be killed
         */
         fatal(" Thread Response error on read %s !!\n",strerror(errno));
         exit(0);
     }

    }
    if (bytesRcvd == 0) {
      fatal(" Thread Response socket is dead %s !!\n",strerror(errno));
      exit(0);    
    } 
    if (bytesRcvd != sizeof(rozorpc_srv_ctx_t *))
    {
      fatal(" Thread receive length unexpected %d (%d expected) !!\n",bytesRcvd,(int)sizeof(rozorpc_srv_ctx_t *));
      exit(0);        
    }
    af_unix_trk_pending_req_count--;
    if ( af_unix_trk_pending_req_count < 0) af_unix_trk_pending_req_count = 0;
    af_unix_trk_response(rozorpc_srv_ctx_p); 
  }    
  
out:
  return TRUE;
}

/*
**__________________________________________________________________________
*/
/**
*   That function is intended to be called when the storio runs out of TCP buffer
    We wait for at least 16 buffers before re-attempting to allocated a buffer for
    a TCP receive.
    That function is called by storio_north_RcvAllocBufCallBack
    
    @param none: 
    
    @retval none;
*/
void af_unix_trk_pool_socket_on_receive_buffer_depletion()
{
    rozo_fd_set    localRdFdSet;   
    int            nbrSelect;
    uint32_t       free_count;
    struct timeval timeout;
    uint32_t       initial_pending_count;

    /*
    ** Nothing is expected from the disk threads
    */    
    if (af_unix_trk_pending_req_count <= 0) return;
    
    /*
    ** erase the Fd receive set
    */
    memset(&localRdFdSet,0,sizeof(localRdFdSet));
    initial_pending_count = af_unix_trk_pending_req_count;
   
    /*
    ** Some buffers may be sent to the disk threads, while some others may be chained on 
    ** a congested TCP congestion. So try to get some requests back from the disk threads 
    ** if any is pending. This may free some buffers and anyway during this time the 
    ** congestions may be fixed.
    */   
    while(af_unix_trk_pending_req_count > 0)
    {        
      /*
      ** wait for event 
      */
      FD_SET(expbt_recv_af_unix_socket_ref,&localRdFdSet);   	  
      /*
      ** Set a 15 seconds time out
      */
      timeout.tv_sec  = 15;
      timeout.tv_usec = 0;
      
      nbrSelect=select(expbt_recv_af_unix_socket_ref+1,(fd_set *)&localRdFdSet,(fd_set *)NULL,NULL, &timeout);
      if (nbrSelect < 0) 
      {
         if (errno == EINTR) 
	 {
           //RUC_WARNING(errno);
           continue;
         }
         
         fatal("Buffer depletion case. Error on select(%s)",strerror(errno));
         return;
      }
      /*
      ** No responses within 15 seconds...
      */
      if (nbrSelect == 0) {
        free_count  = ruc_buf_getFreeBufferCount(expbt_buffer_pool_p);  
        severe("Buffer depletion timeout free count %u pending requests %d initial pending requests %u", 
               free_count, 
               af_unix_trk_pending_req_count, 
               initial_pending_count);
        if (af_unix_trk_pending_req_count == initial_pending_count) {
          /*
          ** No response has been received from the disk threads although some are expected
          */
          fatal("Buffer depletion");
        }      
        /*
        ** Some responses have been previously received from the disk threads
        */
        return;
      }
      /*
      ** attempt to process a response from the disk thread queue
      */
      af_unix_trk_rcvMsgsock(NULL,expbt_recv_af_unix_socket_ref);
      /*
      ** When 16 responses have been received from the disk threads, go on
      */
      if ((initial_pending_count - af_unix_trk_pending_req_count) >= 16) return;
    }
    
    /*
    ** Nothing is expected any more from the tracking file reading threads
    */    
}


/*
**__________________________________________________________________________
*/
/**
* 
   Send back the result of an operation done by the tracking file threads
   
   The response message has been formatted by the thread, the main thread has just to send it back to the source
   
   @param rpc_ctx_p: pointer to the RPC context
   
   @retval none
*/

int expbt_send2mainthread(void *rpc_ctx_p)
{

   int ret;
   

   ret = sendto(expbt_xmit_af_unix_socket_ref,rpc_ctx_p,sizeof(rpc_ctx_p),0,(struct sockaddr*)&expbt_recv_af_unix_socket_name,sizeof(expbt_recv_af_unix_socket_name));
   if (ret < 0) return -1;
   if (ret != sizeof(rpc_ctx_p)) {
     fatal("expbt_send2mainthread  write error on  socket: %s",strerror(errno));
     exit(0);  
   }
   return 0;
}


/*__________________________________________________________________________
*/
/**
*   entry point for disk response socket polling
*

   @param current_time : current time provided by the socket controller
   
   
   @retval none
*/
void af_unix_trk_scheduler_entry_point(uint64_t current_time)
{
  af_unix_trk_rcvMsgsock(NULL,expbt_recv_af_unix_socket_ref);
}

/*
**_________________________________________________________________
*/
/**
  send back a response with error code because the request is not supported
  This should bnot occur here!!
   
   @param rozorpc_srv_ctx_p: RPC context associated with the request
   
   @retval none
*/
void expb_trk_unk_in_thread(rozorpc_srv_ctx_t *rozorpc_srv_ctx_p)
{

   expbt_msg_hdr_t   *hdr_p,*hdr_rsp_p;
   expbt_trk_read_rsp_t *rsp_p;
   uint32_t returned_size;
   
   hdr_rsp_p =  (expbt_msg_hdr_t*)ruc_buf_getPayload(rozorpc_srv_ctx_p->recv_buf);
   rsp_p     = (expbt_trk_read_rsp_t*) (hdr_rsp_p +1);
   
   hdr_p = rozorpc_srv_ctx_p->decoded_arg;   

   memcpy(hdr_rsp_p,hdr_p,sizeof(expbt_msg_hdr_t));
   hdr_rsp_p->dir = 1;
   hdr_rsp_p->len = 0;
   rsp_p->status = -1;
   rsp_p->errcode = ENOTSUP;
   returned_size = sizeof(expbt_msg_hdr_t)+sizeof(expbt_trk_read_rsp_t);     
   hdr_rsp_p->len = returned_size;
   ruc_buf_setPayloadLen(rozorpc_srv_ctx_p->recv_buf,hdr_rsp_p->len+sizeof(uint32_t));      
   /*
   ** send back the context towards the main thread by using the af_unix socket
   */
   expbt_send2mainthread(rozorpc_srv_ctx_p);
   
} 

/*
**__________________________________________________________________
*/
/**
*  tracking file reading thread 


   
   @param arg: pointer to the thread context
*/
void *expbt_trk_thread(void *arg) {    

   expbt_trk_thread_ctx_t * ctx_p = (expbt_trk_thread_ctx_t*)arg;
   char bufname[64];
   rozorpc_srv_ctx_t *rozorpc_srv_ctx_p;
   
   expbt_msg_hdr_t   *hdr_p;
   
  sprintf(bufname,"Trk thread#%d",ctx_p->thread_idx);
  /*
  **  change the priority of the main thread
  */
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
      my_priority.sched_priority= 90;
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
     
    }  
    uma_dbg_thread_add_self(bufname);
    while(1)
    {  
#warning need a parameter to define the number of tracking file reader threads
#if 0
      if ((ctx_p->thread_idx!= 0) && (ctx_p->thread_idx >= common_config.rozofsmount_fuse_reply_thread))
      {
	 sleep(30);
	 continue;
      }
#endif
      /*
      ** Read some data from the queue
      */
      rozorpc_srv_ctx_p = rozofs_queue_get(&expbt_trk_cmd_ring);  
      
      hdr_p = (expbt_msg_hdr_t*)ruc_buf_getPayload(rozorpc_srv_ctx_p->decoded_arg);
      if (hdr_p == NULL)
      {
        fatal("empty header in thread %d",ctx_p->thread_idx);
      }
        
      /*
      ** Execute the command associated with the context
      */
      switch( hdr_p->opcode)
      {
         case EXP_BT_TRK_READ:
	   expb_trk_read_in_thread(rozorpc_srv_ctx_p);
	   ctx_p->stat.read_count++;
	   break;
	 case EXP_BT_TRK_CHECK:
	   expb_trk_check_in_thread(rozorpc_srv_ctx_p);
	   ctx_p->stat.check_count++;
	   break;

	 case EXP_BT_DIRENT_LOAD:
	   expbt_load_dentry_in_thread(rozorpc_srv_ctx_p);
	   ctx_p->stat.load_dentry_count++;
	   break;
	   	   
	   

	 default:
	   expb_trk_unk_in_thread(rozorpc_srv_ctx_p);
	   ctx_p->stat.unk_count++;
	 break;
      }
    }           
}
/*
**__________________________________________________
*/
/*
** Create the threads that are in charge of the tracking file reading

* @param hostname    storio hostname (for tests)
* @param nb_threads  number of threads to create

*  
* @retval 0 on success -1 in case of error
*/
int expbt_trk_thread_create(char * hostname, int nb_threads) {
   int                        i;
   int                        err;
   pthread_attr_t             attr;
   expbt_trk_thread_ctx_t * thread_ctx_p;

          
   /*
   ** clear the thread table
   */
   memset(expbt_trk_thread_ctx_tb,0,sizeof(expbt_trk_thread_ctx_t));
   /*
   ** Now create the threads
   */
   thread_ctx_p = expbt_trk_thread_ctx_tb;
   for (i = 0; i < nb_threads ; i++) {
   
     thread_ctx_p->hostname = hostname;
     thread_ctx_p->sendSocket =  expbt_xmit_af_unix_socket_ref; 
     err = pthread_attr_init(&attr);
     if (err != 0) {
       fatal("expbt_trk_thread_create pthread_attr_init(%d) %s",i,strerror(errno));
       return -1;
     }  

     thread_ctx_p->thread_idx = i;
     
     err = pthread_create(&thread_ctx_p->thrdId,&attr,expbt_trk_thread,thread_ctx_p);
     if (err != 0) {
       fatal("expbt_trk_thread pthread_create(%d) %s",i, strerror(errno));
       return -1;
     }  
     
     thread_ctx_p++;
  }
  return 0;
}


/*
**__________________________________________________________________________
*/
/**
* fill the storio  AF_UNIX name in the global data

  @param hostname
  @param socketname : pointer to a sockaddr_un structure
  
  @retval none
*/
void expbt_set_sockname_with_hostname(struct sockaddr_un *socketname,char *name,char *hostname,int instance_id)
{
  socketname->sun_family = AF_UNIX;  
  char * pChar = socketname->sun_path;
  pChar += rozofs_string_append(pChar,name);
  *pChar++ = '_';
  pChar += rozofs_u32_append(pChar,instance_id);
  *pChar++ = '_';  
  pChar += rozofs_string_append(pChar,hostname);
}


/*
**__________________________________________________________________________
*/

/**
* creation of the AF_UNIX socket that is attached on the socket controller

  That socket is used to receive back the response from the threads that
  perform disk operation (read/write/truncate)
  
  @param socketname : name of the socket
  
  @retval >= 0 : reference of the socket
  @retval < 0 : error
*/
int af_unix_bt_response_socket_create(char *socketname)
{
  int len;
  int fd = -1;

   len = strlen(socketname);
   if (len >= AF_UNIX_SOCKET_NAME_SIZE)
   {
      /*
      ** name is too big!!
      */
      severe("socket name %s is too long: %d (max is %d)",socketname,len,AF_UNIX_SOCKET_NAME_SIZE);
      return -1;
   }
   while (1)
   {
     /*
     ** create the socket
     */
     fd = af_unix_sock_create_internal(socketname,DISK_SO_SENDBUF);
     if (fd == -1)
     {
       break;
     }
     /*
     ** OK, we are almost done, just need to connect with the socket controller
     */
     expbt_recv_sockctrl_p = ruc_sockctl_connect(fd,
                                	      "EXP_BT_TH_RSP",
                                	       16,
                                	       NULL,
                                	       &af_unix_trk_callBack_sock);
     if (expbt_recv_sockctrl_p== NULL)
     {
       fatal("Cannot connect with socket controller");;
       return -1;
     }
     break;
   }
   return fd;
}

/*__________________________________________________________________________
* Initialize the disk thread interface
*
* @param hostname    storio hostname (for tests)
  @param instance_id: expbt instance identifier
* @param nb_threads  Number of threads that can process the disk requests
  @param cmdring_size: size of the ring command (depends on the number of received buffers.
  *
*  @retval 0 on success -1 in case of error
*/
int expbt_trk_thread_intf_create(char * hostname, int instance_id,int nb_threads,int cmdring_size) {

  af_unix_trk_thread_count = nb_threads;
  
  
  
  expbt_set_sockname_with_hostname (&expbt_recv_af_unix_socket_name,"/tmp/expbt_sock",hostname,instance_id);

  expbt_recv_af_unix_socket_ref = af_unix_bt_response_socket_create(expbt_recv_af_unix_socket_name.sun_path);
  if (expbt_recv_af_unix_socket_ref < 0) {
    fatal("expbt_trk_thread_intf_create af_unix_sock_create(%s) %s",expbt_recv_af_unix_socket_name.sun_path, strerror(errno));
    return -1;
  }
  
  expbt_xmit_af_unix_socket_ref = socket(AF_UNIX,SOCK_DGRAM,0);
  if (expbt_xmit_af_unix_socket_ref < 0) {
     fatal("expbt_trk_thread_intf_create fail to create socket: %s", strerror(errno));
     return -1;   
  } 
  /*
  ** Init of the common ring buffer used by RDMA write (to get response from the RDMA completion thread
  */
  if (rozofs_queue_init(&expbt_trk_cmd_ring,cmdring_size) < 0)
  {
     fatal("cannot create ring buffer");
     return -1;
  }   
  /*
  ** attach the callback on socket controller
  */
  ruc_sockCtrl_attach_applicative_poller(af_unix_trk_scheduler_entry_point);  
   
  return expbt_trk_thread_create(hostname, nb_threads);
}
