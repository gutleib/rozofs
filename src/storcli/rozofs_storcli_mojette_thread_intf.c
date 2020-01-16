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
#include <rozofs/core/ruc_buffer_debug.h>
#include <rozofs/core/com_cache.h>
#include <rozofs/rozofs_srv.h>

#include "rozofs_storcli_mojette_thread_intf.h"
#include "config.h"
#include "rozofs_storcli.h"
#include "storcli_main.h"

DECLARE_PROFILING(spp_profiler_t); 
 
static int transactionId = 1; 
int        af_unix_mojette_south_socket_ref = -1;
//char       destination_socketName[128];
int        af_unix_mojette_thread_count=0;
int        af_unix_mojette_pending_req_count = 0;
int        af_unix_mojette_pending_req_max_count = 0;
int        af_unix_mojette_empty_recv_count = 0;

struct  sockaddr_un storio_south_socket_name;
struct  sockaddr_un storio_north_socket_name;
rozofs_stcmoj_thread_msg_t *rozofs_storcli_moj_req_pool_p=NULL;
uint32_t rozofs_storcli_moj_pool_pool_idx_cur = 0;
rozofs_queue_t rozofs_storcli_mojette_req_queue;
 
int rozofs_stcmoj_thread_create(char * hostname,int eid,int storcli_idx, int nb_threads) ;
 


void * af_unix_mojette_pool_send = NULL;
void * af_unix_mojette_pool_recv = NULL;
#define DEFAULT_STCMO_THREAD_WRITE       1
#define DEFAULT_STCMO_THREAD_READ        0
#define DEFAULT_STCMO_THREAD_THRESHOLD  (64*1024)
int rozofs_stcmoj_thread_write_enable=DEFAULT_STCMO_THREAD_WRITE;
int rozofs_stcmoj_thread_read_enable=DEFAULT_STCMO_THREAD_READ;
uint32_t rozofs_stcmoj_thread_len_threshold=DEFAULT_STCMO_THREAD_THRESHOLD;

/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
#define new_line(title)  pChar += sprintf(pChar,"\n%-24s |", title)
#define display_val(val) pChar += sprintf(pChar," %16lld |", (long long unsigned int) val)
#define display_div(val1,val2) if (val2==0) display_val(0);else display_val(val1/val2)
#define display_txt(txt) pChar += sprintf(pChar," %16s |", (char *) txt)

#define display_line_topic(title) \
  new_line(title);\
  for (i=0; i<=af_unix_mojette_thread_count; i++) {\
    pChar += sprintf(pChar,"__________________|");\
  }
  
#define display_line_val(title,val) \
  new_line(title);\
  sum1 = 0;\
  for (i=0; i<af_unix_mojette_thread_count; i++) {\
    sum1 += p[i].stat.val;\
    display_val(p[i].stat.val);\
  }
    
#define display_line_val_and_sum(title,val) \
  display_line_val(title,val);\
  display_val(sum1)

#define display_line_div(title,val1,val2) \
  new_line(title);\
  sum1 = sum2 = 0;\
  for (i=0; i<af_unix_mojette_thread_count; i++) {\
    sum1 += p[i].stat.val1;\
    sum2 += p[i].stat.val2;\
    display_div(p[i].stat.val1,p[i].stat.val2);\
  }
  
#define display_line_div_and_sum(title,val1,val2) \
  display_line_div(title,val1,val2);\
  display_div(sum1,sum2)
static char * mojette_thread_debug_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"MojetteThreads reset                : reset statistics\n");
  pChar += sprintf(pChar,"MojetteThreads <read|write> enable  : enable Mojette threads \n");
  pChar += sprintf(pChar,"MojetteThreads <read|write> disable : disable Mojette threads\n");
  pChar += sprintf(pChar,"MojetteThreads                      : display statistics\n");  
  pChar += sprintf(pChar,"MojetteThreads count <count>        : adjust the bytes threshold for thread activation (unit byte)\n");  
  return pChar; 
}  
void mojette_thread_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
  char           *pChar=uma_dbg_get_buffer();
  int i;
  uint64_t        sum1,sum2;
  int new_val;
  rozofs_mojette_thread_ctx_t *p = rozofs_mojette_thread_ctx_tb;
  int             doreset=0;
  
  while (argv[1] != NULL) {
    if (strcmp(argv[1],"reset")==0) {
      doreset = 1;
      break;
    }
    if (strcmp(argv[1],"read")==0) {
      if(argv[2]!= NULL)
      {
	if (strcmp(argv[2],"enable")==0) {
	  rozofs_stcmoj_thread_read_enable = 1;        
	  uma_dbg_send(tcpRef,bufRef,TRUE,"Mojette read Threads are enabled\n");
	  return;
	}
	if (strcmp(argv[2],"disable")==0) {
	  rozofs_stcmoj_thread_read_enable = 0;        
	  uma_dbg_send(tcpRef,bufRef,TRUE,"Mojette read Threads are disabled\n");
	  return;
	}
        pChar += sprintf(pChar, "unexpected value %s\n",argv[2]);
	pChar = mojette_thread_debug_help(pChar);
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	return;
      }
      pChar += sprintf(pChar, "missing parameter\n");
      pChar = mojette_thread_debug_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
      return;
    }
    if (strcmp(argv[1],"write")==0) {
      if(argv[2]!= NULL)
      {
	if (strcmp(argv[2],"enable")==0) {
	  rozofs_stcmoj_thread_write_enable = 1;        
	  uma_dbg_send(tcpRef,bufRef,TRUE,"Mojette write Threads are enabled\n");
	  return;
	}
	if (strcmp(argv[2],"disable")==0) {
	  rozofs_stcmoj_thread_write_enable = 0;        
	  uma_dbg_send(tcpRef,bufRef,TRUE,"Mojette write Threads are disabled\n");
	  return;
	}
        pChar += sprintf(pChar, "unexpected value %s\n",argv[2]);
	pChar = mojette_thread_debug_help(pChar);
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	return;
      }
      pChar += sprintf(pChar, "missing parameter\n");
      pChar = mojette_thread_debug_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
      return;
    }
    if (strcmp(argv[1],"count")==0) {
       errno = 0;
       if (argv[2] == NULL)
       {
         pChar += sprintf(pChar, "argument is missing\n");
	 pChar = mojette_thread_debug_help(pChar);
	 uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	 return;	  	  
       }
       new_val = (int) strtol(argv[2], (char **) NULL, 10);   
       if (errno != 0) {
         pChar += sprintf(pChar, "bad value %s\n",argv[2]);
	 pChar = mojette_thread_debug_help(pChar);
	 uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	 return;
       }
       /*
       ** change the bytes threshold
       */
       rozofs_stcmoj_thread_len_threshold = new_val;
       uma_dbg_send(tcpRef,bufRef,TRUE,"bytes threshold changed\n");
       return;
    }
    pChar = mojette_thread_debug_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
    return;      
  }
  if ((rozofs_stcmoj_thread_write_enable==0)&&(rozofs_stcmoj_thread_read_enable==0)) 
  {  
     uma_dbg_send(tcpRef,bufRef,TRUE,"Mojette Threads are disabled\n");
     return;  
  }
  pChar+=sprintf(pChar,"Thread activation threshold: %u bytes\n",rozofs_stcmoj_thread_len_threshold);
  pChar+=sprintf(pChar,"max pending Mojette req cnt: %u\n",af_unix_mojette_pending_req_max_count);
  pChar+=sprintf(pChar,"receive empty counter      : %u\n",af_unix_mojette_empty_recv_count);
  pChar+=sprintf(pChar,"read/write thread status   : %s/%s\n",
         (rozofs_stcmoj_thread_read_enable==1)?"ENABLE":"DISABLE",
         (rozofs_stcmoj_thread_write_enable==1)?"ENABLE":"DISABLE"
	 );

  af_unix_mojette_pending_req_max_count = 0;
  af_unix_mojette_empty_recv_count = 0;
  new_line("Thread number");
  for (i=0; i<af_unix_mojette_thread_count; i++) {
    display_val(p[i].thread_idx);
  }    
  display_txt("TOTAL");
  
  display_line_topic("Read Requests");  
  display_line_val_and_sum("   number", MojetteInverse_count);
  display_line_val_and_sum("   Bytes",MojetteInverse_Byte_count);      
  display_line_val_and_sum("   Cumulative Time (us)",MojetteInverse_time);
  display_line_div_and_sum("   Average Bytes",MojetteInverse_Byte_count,MojetteInverse_count);  
  display_line_div_and_sum("   Average Time (us)",MojetteInverse_time,MojetteInverse_count);
  display_line_div_and_sum("   Average Cycle",MojetteInverse_cycle,MojetteInverse_count);
  display_line_div_and_sum("   Throughput (MBytes/s)",MojetteInverse_Byte_count,MojetteInverse_time);  
  
  display_line_topic("Write Requests");  
  display_line_val_and_sum("   number", MojetteForward_count);
  display_line_val_and_sum("   Bytes",MojetteForward_Byte_count);      
  display_line_val_and_sum("   Cumulative Time (us)",MojetteForward_time);
  display_line_div_and_sum("   Average Bytes",MojetteForward_Byte_count,MojetteForward_count); 
  display_line_div_and_sum("   Average Time (us)",MojetteForward_time,MojetteForward_count);
  display_line_div_and_sum("   Average Cycle",MojetteForward_cycle,MojetteForward_count);
  display_line_div_and_sum("   Throughput (MBytes/s)",MojetteForward_Byte_count,MojetteForward_time);  
  
  display_line_topic("");  
  pChar += sprintf(pChar,"\n");
  
  if (doreset) {
    for (i=0; i<af_unix_mojette_thread_count; i++) {
      memset(&p[i].stat,0,sizeof(p[i].stat));
    }          
    pChar += sprintf(pChar,"Reset Done\n");
  }

  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());
}

/*__________________________________________________________________________
*/
/**
* test function for allocatiing a buffer in the client space

 The service might reject the buffer allocation because the pool runs
 out of buffer or because there is no pool with a buffer that is large enough
 for receiving the message because of a out of range size.

 @param userRef : pointer to a user reference: not used here
 @param socket_context_ref: socket context reference
 @param len : length of the incoming message
 
 @retval <>NULL pointer to a receive buffer
 @retval == NULL no buffer
*/
void * af_unix_disk_userRcvAllocBufCallBack(void *userRef,uint32_t socket_context_ref,uint32_t len) {
  return ruc_buf_getBuffer(af_unix_mojette_pool_recv);   
}


 /**
 * prototypes
 */
uint32_t af_unix_disk_rcvReadysock(void * af_unix_disk_ctx_p,int socketId);
uint32_t af_unix_disk_rcvMsgsock(void * af_unix_disk_ctx_p,int socketId);
uint32_t af_unix_disk_xmitReadysock(void * af_unix_disk_ctx_p,int socketId);
uint32_t af_unix_disk_xmitEvtsock(void * af_unix_disk_ctx_p,int socketId);

#define DISK_SO_SENDBUF  (300*1024)
#define DISK_SOCKET_NICKNAME "mojette_resp_th"
/*
**  Call back function for socket controller
*/
ruc_sockCallBack_t af_unix_disk_callBack_sock=
  {
     af_unix_disk_rcvReadysock,
     af_unix_disk_rcvMsgsock,
     af_unix_disk_xmitReadysock,
     af_unix_disk_xmitEvtsock
  };
  
  /*
**__________________________________________________________________________
*/
/**
  Application callBack:

  Called from the socket controller. 


  @param unused: not used
  @param socketId: reference of the socket (not used)
 
  @retval : always FALSE
*/

uint32_t af_unix_disk_xmitReadysock(void * unused,int socketId)
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
uint32_t af_unix_disk_xmitEvtsock(void * unused,int socketId)
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

uint32_t af_unix_disk_rcvReadysock(void * unused,int socketId)
{
  return TRUE;
}
/**
*  process the end of Mojette inverse transform process

  @param working_ctx_p : pointer to the working context of the read
  @param send_needed: when asserted the send must be done from the main thread, otherwise we just do the release
  @retval none:
*/
void rozofs_storcli_inverse_threaded_end(rozofs_storcli_ctx_t * working_ctx_p,int send_needed)
{
   uint32_t   projection_id;
   storcli_read_arg_t *storcli_read_rq_p;

   storcli_read_rq_p = (storcli_read_arg_t*)&working_ctx_p->storcli_read_arg;
   uint8_t layout         = storcli_read_rq_p->layout;
   uint8_t rozofs_safe    = rozofs_get_rozofs_safe(layout);

    /*
    ** Check we just need to proceed with the context release : case when Mojette thread
    ** sent back the reponse to rozofsmount by itself
    */
    if (send_needed == 0) goto release;

    /*
    ** update the index of the next block to read
    */
    working_ctx_p->cur_nmbs2read += working_ctx_p->nb_projections2read;
    /*
    ** check if it was the last read
    */
    if (working_ctx_p->cur_nmbs2read < storcli_read_rq_p->nb_proj)
    {
      /*
      ** attempt to read block with the next distribution
      */
      return rozofs_storcli_read_req_processing(working_ctx_p);        
    }
        
    /*
    ** check for auto-repair because of potential crc error
    */
    {
      int ret = rozofs_storcli_check_repair(working_ctx_p,rozofs_safe);  
      if (ret != 0)
      {    
         rozofs_storcli_repair_req_init(working_ctx_p);
	 return;
      }
    
    }
        
    /*
    ** read is finished, send back the buffer to the client (rozofsmount)
    */       
    rozofs_storcli_read_reply_success(working_ctx_p);
release:
    /*
    ** release the root context and the transaction context
    */
    rozofs_storcli_release_context(working_ctx_p);  

}
/*
**__________________________________________________________________________
*/
/**
  Processes a disk response

   Called from the socket controller when there is a response from a disk thread
   the response is either for a disk read or write
    
  @param msg: pointer to disk response message
 
  @retval :none
*/
void af_unix_mojette_thread_response(rozofs_stcmoj_thread_msg_t *msg) 
{

  rozofs_stcmoj_thread_request_e   opcode;
  rozofs_storcli_ctx_t            * working_ctx_p;
  working_ctx_p = msg->working_ctx;

  opcode = msg->opcode;

  switch (opcode) {
    case STORCLI_MOJETTE_THREAD_FWD:
       /*
       ** send the projection to the storage nodes
       */
       rozofs_storcli_write_req_processing(working_ctx_p);
       break;     

    case STORCLI_MOJETTE_THREAD_INV:
       rozofs_storcli_inverse_threaded_end(working_ctx_p,1);
       break;
    case STORCLI_MOJETTE_THREAD_INV_RELEASE:
       rozofs_storcli_inverse_threaded_end(working_ctx_p,0);
       break;

    default:
      severe("Unexpected opcode %d", opcode);
  }

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

uint32_t af_unix_disk_rcvMsgsock(void * unused,int socketId)
{
  rozofs_stcmoj_thread_msg_t   msg;
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
    if (af_unix_mojette_pending_req_count == 0)
    {
     return TRUE;
    }
    /*
    ** read the north disk socket
    */
    bytesRcvd = recvfrom(socketId,
			 &msg,sizeof(msg), 
			 0,(struct sockaddr *)NULL,NULL);
    if (bytesRcvd == -1) {
     switch (errno)
     {
       case EAGAIN:
        /*
        ** the socket is empty
        */
	af_unix_mojette_empty_recv_count++;
        return TRUE;

       case EINTR:
         /*
         ** re-attempt to read the socket
         */
         eintr_count++;
         if (eintr_count < 3) continue;
         /*
         ** here we consider it as a error
         */
         severe ("Disk Thread Response error too many eintr_count %d",eintr_count);
         return TRUE;

       case EBADF:
       case EFAULT:
       case EINVAL:
       default:
         /*
         ** We might need to double checl if the socket must be killed
         */
         fatal("Disk Thread Response error on recvfrom %s !!\n",strerror(errno));
         exit(0);
     }

    }
    if (bytesRcvd == 0) {
      fatal("Disk Thread Response socket is dead %s !!\n",strerror(errno));
      exit(0);    
    } 
    /*
    ** clear the fd in the receive set to avoid computing it twice
    */
    ruc_sockCtrl_clear_rcv_bit(socketId);
    
    af_unix_mojette_pending_req_count--;
    if (  af_unix_mojette_pending_req_count < 0) 
    {
      severe("af_unix_mojette_pending_req_count is negative");
      af_unix_mojette_pending_req_count = 0;
    }
    af_unix_mojette_thread_response(&msg); 
  }       
  return TRUE;
}


/*
**__________________________________________________________________________
*/
/**
* fill the storio  AF_UNIX name in the global data

  @param hostname
  @param socketname : pointer to a sockaddr_un structure
  @param eid : reference of the exportd
  @param storcli_idx : storcli_idx
  
  @retval none
*/
void storcli_set_socket_name_with_eid_stc_id(struct sockaddr_un *socketname,char *name,char *hostname,int eid,int storcli_idx)
{
  socketname->sun_family = AF_UNIX;  
  sprintf(socketname->sun_path,"%s_%s_%d_%d",name,storcli_get_owner(),eid,storcli_idx);
}

/*
**__________________________________________________________________________
*/
/**
*  Thar API is intended to be used by a disk thread for sending back a 
   disk response (read/write or truncate) towards the main thread
   
   @param thread_ctx_p: pointer to the thread context (contains the thread source socket )
   @param msg: pointer to the message that contains the disk response
   @param status : status of the disk operation
   
   @retval none
*/
void storio_send_response (rozofs_mojette_thread_ctx_t *thread_ctx_p, rozofs_stcmoj_thread_msg_t * msg, int status) 
{
  int                     ret;
  
  msg->status = status;
  
  /*
  ** send back the response
  */  
  ret = sendto(thread_ctx_p->sendSocket,msg, sizeof(*msg),0,(struct sockaddr*)&storio_south_socket_name,sizeof(storio_south_socket_name));
  if (ret <= 0) {
     fatal("storio_send_response %d sendto(%s) %s", thread_ctx_p->thread_idx, storio_south_socket_name.sun_path, strerror(errno));
     exit(0);  
  }
  sched_yield();
}

/*__________________________________________________________________________
*/
/**
*  Send a disk request to the disk threads
*
* @param opcode     the request operation code
* @param working_ctx     pointer to the generic rpc context
* @param timeStart  time stamp when the request has been decoded
*
* @retval 0 on success -1 in case of error
*  
*/
int rozofs_stcmoj_thread_intf_send(rozofs_stcmoj_thread_request_e   opcode, 
                                   rozofs_storcli_ctx_t            * working_ctx,
				   uint64_t                       timeStart) 
{

#ifdef STORCLI_MOJ_QUEUE
#warning FDL new code for Mojette request submit  
  /*
  ** allocate a slot in the queue to submit the request
  */
  rozofs_stcmoj_thread_msg_t    *msg_p; 
  msg_p = rozofs_storcli_mojette_req_get_next_slot();
  /* Fill the message */
  msg_p->msg_len         = sizeof(rozofs_stcmoj_thread_msg_t)-sizeof(msg_p->msg_len);
  msg_p->opcode          = opcode;
  msg_p->status          = 0;
  msg_p->transaction_id  = transactionId++;
  msg_p->timeStart       = timeStart;
  msg_p->working_ctx     = working_ctx;
  /*
  ** post the request to the thread
  */ 
  rozofs_queue_put(&rozofs_storcli_mojette_req_queue,msg_p);  

#else 
  rozofs_stcmoj_thread_msg_t    msg;
  /* Fill the message */
  msg.msg_len         = sizeof(rozofs_stcmoj_thread_msg_t)-sizeof(msg.msg_len);
  msg.opcode          = opcode;
  msg.status          = 0;
  msg.transaction_id  = transactionId++;
  msg.timeStart       = timeStart;
  msg.working_ctx     = working_ctx;
  
  /* Send the buffer to its destination */
  ret = sendto(af_unix_mojette_south_socket_ref,&msg, sizeof(msg),0,(struct sockaddr*)&storio_north_socket_name,sizeof(storio_north_socket_name));
  if (ret <= 0) {
     fatal("rozofs_stcmoj_thread_intf_send count %d sendto(%s) %s", af_unix_mojette_pending_req_count,
                                                                    storio_north_socket_name.sun_path, strerror(errno));
     exit(0);  
  }
#endif  
  af_unix_mojette_pending_req_count++;
  if (af_unix_mojette_pending_req_count > af_unix_mojette_pending_req_max_count)
      af_unix_mojette_pending_req_max_count = af_unix_mojette_pending_req_count;
//  sched_yield();
  return 0;
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
int af_unix_mojette_thread_response_socket_create(char *socketname)
{
  int len;
  int fd = -1;
  void *sockctrl_ref;

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
     sockctrl_ref = ruc_sockctl_connect(fd,  // Reference of the socket
                                                DISK_SOCKET_NICKNAME,   // name of the socket
                                                16,                  // Priority within the socket controller
                                                (void*)NULL,      // user param for socketcontroller callback
                                                &af_unix_disk_callBack_sock);  // Default callbacks
      if (sockctrl_ref == NULL)
      {
         /*
         ** Fail to connect with the socket controller
         */
         fatal("error on ruc_sockctl_connect");
         break;
      }
      /*
      ** All is fine
      */
      break;
    }    
    return fd;
}

/*__________________________________________________________________________
*/
/**
*   entry point for disk response socket polling
*

   @param current_time : current time provided by the socket controller
   
   
   @retval none
*/
void af_unix_mojette_scheduler_entry_point(uint64_t current_time)
{
  af_unix_disk_rcvMsgsock(NULL,af_unix_mojette_south_socket_ref);
}

/*__________________________________________________________________________
* Initialize the disk thread interface
*
* @param hostname    storio hostname (for tests)
* @param nb_threads  Number of threads that can process the disk requests
* @param nb_buffer   Number of buffer for sending and number of receiving buffer
*
*  @retval 0 on success -1 in case of error
*/
int rozofs_stcmoj_thread_intf_create(char * hostname,int eid,int storcli_idx, int nb_threads, int nb_buffer) {
  char socketName[128];
  int ret;

  af_unix_mojette_thread_count = nb_threads;

  af_unix_mojette_pool_send = ruc_buf_poolCreate(nb_buffer,sizeof(rozofs_stcmoj_thread_msg_t));
  if (af_unix_mojette_pool_send == NULL) {
    fatal("rozofs_stcmoj_thread_intf_create af_unix_mojette_pool_send (%d,%d)", nb_buffer, (int)sizeof(rozofs_stcmoj_thread_msg_t));
    return -1;
  }
  ruc_buffer_debug_register_pool("MojetteSendPool",af_unix_mojette_pool_send);   
  
  af_unix_mojette_pool_recv = ruc_buf_poolCreate(1,sizeof(rozofs_stcmoj_thread_msg_t));
  if (af_unix_mojette_pool_recv == NULL) {
    fatal("rozofs_stcmoj_thread_intf_create af_unix_mojette_pool_recv (1,%d)", (int)sizeof(rozofs_stcmoj_thread_msg_t));
    return -1;
  }
  ruc_buffer_debug_register_pool("MojetteRecvPool",af_unix_mojette_pool_recv);   
   
  /*
  ** hostname is required for the case when several storaged run on the same server
  ** as is the case of test on one server only
  */ 
//  sprintf(destination_socketName,"%s_%s", ROZOFS_SOCK_FAMILY_STORCLI_MOJETTE_NORTH_SUNPATH, hostname);
  
  sprintf(socketName,"%s_%s_%d_%d", ROZOFS_SOCK_FAMILY_STORCLI_MOJETTE_SOUTH_SUNPATH,storcli_get_owner(), eid,storcli_idx);
  af_unix_mojette_south_socket_ref = af_unix_mojette_thread_response_socket_create(socketName);
  if (af_unix_mojette_south_socket_ref < 0) {
    fatal("storio_create_disk_thread_intf af_unix_sock_create(%s) %s",socketName, strerror(errno));
    return -1;
  }
  /*
  ** init of the AF_UNIX sockaddr associated with the south socket (socket used for disk response receive)
  */
  storcli_set_socket_name_with_eid_stc_id(&storio_south_socket_name,ROZOFS_SOCK_FAMILY_STORCLI_MOJETTE_SOUTH_SUNPATH,hostname,eid,storcli_idx);
  /*
  ** init of the AF_UNIX sockaddr associated with the north socket (socket used for disk request receive)
  */
  storcli_set_socket_name_with_eid_stc_id(&storio_north_socket_name,ROZOFS_SOCK_FAMILY_STORCLI_MOJETTE_NORTH_SUNPATH,hostname,eid,storcli_idx);
  
  uma_dbg_addTopic_option("MojetteThreads", mojette_thread_debug, UMA_DBG_OPTION_RESET); 
#if 0
  /*
  ** attach the callback on socket controller
  */
  ruc_sockCtrl_attach_applicative_poller(af_unix_mojette_scheduler_entry_point);  
#endif
  /*
  ** create the common queue used for submitting the mojette transform requests towards the Mojette threads
  */
  ret = rozofs_queue_init(&rozofs_storcli_mojette_req_queue,ROZOFS_MOJETTE_QUEUE_RING_SZ);
  if (ret < 0)
  {
     severe("Cannot create queue for Mojette threads \n");
     return (-1);
  }
  /*
  ** Create the pool of buffer
  */
  rozofs_storcli_moj_req_pool_p = xmalloc(sizeof(rozofs_stcmoj_thread_msg_t)*(ROZOFS_MOJETTE_QUEUE_BUF_COUNT));
  if (rozofs_storcli_moj_req_pool_p == NULL)
  {
     severe("Cannot allocated memory for Mojette threads \n");
     return (-1);
  } 
  rozofs_storcli_moj_pool_pool_idx_cur = 0; 
   
  return rozofs_stcmoj_thread_create(hostname,eid,storcli_idx, nb_threads);
}
/*__________________________________________________________________________
* Enable/disable the mojette threads for write
*
* @param enable      1 to enable, 0 to disable 
*/
void rozofs_stcmoj_thread_enable_write(int enable) {
  rozofs_stcmoj_thread_write_enable = enable;
}
/*__________________________________________________________________________
* Enable/disable the mojette threads for read
*
* @param enable      1 to enable, 0 to disable 
*/
void rozofs_stcmoj_thread_enable_read(int enable) {
  rozofs_stcmoj_thread_read_enable = enable;
}
/*__________________________________________________________________________
* Set the threshold to call the mojette threads
*
* @param threshold      The threshold in number of blocks
*/
void rozofs_stcmoj_thread_set_threshold(int threshold) {
  rozofs_stcmoj_thread_len_threshold = threshold;
}
/*__________________________________________________________________________
* Reset to the default parameters
*
*/
void rozofs_stcmoj_thread_set_default() {
  rozofs_stcmoj_thread_enable_write(DEFAULT_STCMO_THREAD_WRITE);
  rozofs_stcmoj_thread_enable_read(DEFAULT_STCMO_THREAD_READ);
  rozofs_stcmoj_thread_set_threshold(DEFAULT_STCMO_THREAD_READ); 
}
