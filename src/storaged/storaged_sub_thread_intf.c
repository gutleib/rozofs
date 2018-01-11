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

#include "storaged_sub_thread_intf.h"
#include "config.h"


DECLARE_PROFILING(spp_profiler_t); 
 
static int transactionId = 1; 

int                 storaged_sub_thread_rsp_socket = -1;
struct  sockaddr_un storaged_sub_thread_rsp_socket_name;


struct  sockaddr_un storaged_sub_thread_req_socket_name;


int        storaged_sub_thread_count=0;
int        storaged_sub_thread_pending_req_count = 0;
int        storaged_sub_thread_pending_req_max_count = 0;
int        storaged_sub_thread_empty_recv_count = 0;


 
int storaged_sub_thread_create(char * sockName, char * hostname,int nb_threads) ;
 


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
  for (i=0; i<=storaged_sub_thread_count; i++) {\
    pChar += sprintf(pChar,"__________________|");\
  }
  
#define display_line_val(title,val) \
  new_line(title);\
  sum1 = 0;\
  for (i=0; i<storaged_sub_thread_count; i++) {\
    sum1 += p[i].stat.val;\
    display_val(p[i].stat.val);\
  }
    
#define display_line_val_and_sum(title,val) \
  display_line_val(title,val);\
  display_val(sum1)

#define display_line_div(title,val1,val2) \
  new_line(title);\
  sum1 = sum2 = 0;\
  for (i=0; i<storaged_sub_thread_count; i++) {\
    sum1 += p[i].stat.val1;\
    sum2 += p[i].stat.val2;\
    display_div(p[i].stat.val1,p[i].stat.val2);\
  }
  
#define display_line_div_and_sum(title,val1,val2) \
  display_line_div(title,val1,val2);\
  display_div(sum1,sum2)
static char * storaged_sub_thread_debug_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"subThread reset                : reset statistics\n");
  pChar += sprintf(pChar,"subThread                      : display statistics\n");    
  return pChar; 
}  
void storaged_sub_thread_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
  char           *pChar=uma_dbg_get_buffer();
  int i;
  uint64_t        sum1,sum2;
  storaged_sub_thread_ctx_t *p = storaged_sub_thread_ctx_tb;
  int             doreset=0;
  
  while (argv[1] != NULL) {
    if (strcmp(argv[1],"reset")==0) {
      doreset = 1;
      break;
    }

    storaged_sub_thread_debug_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
    return;      
  }
  pChar += sprintf(pChar,"cur pending request count = %d\n",storaged_sub_thread_pending_req_count);
  pChar += sprintf(pChar,"max pending request count = %d\n",storaged_sub_thread_pending_req_max_count);

  new_line("Thread number");
  for (i=0; i<storaged_sub_thread_count; i++) {
    display_val(p[i].thread_idx);
  }    
  display_txt("TOTAL");
  
  display_line_topic("Remove");  
  display_line_val_and_sum("   number", remove_count);
  display_line_val_and_sum("   errors", remove_errors);
  display_line_val_and_sum("   Cumulative Time (us)",remove_time);
  display_line_div_and_sum("   Average Time (us)",remove_time,remove_count);

  display_line_topic("List bins");  
  display_line_val_and_sum("   number", list_bins_count);
  display_line_val_and_sum("   errors", list_bins_errors);
  display_line_val_and_sum("   Cumulative Time (us)",list_bins_time);
  display_line_div_and_sum("   Average Time (us)",list_bins_time,list_bins_count);
  display_line_topic("size");  
  display_line_val_and_sum("   number", size_count);
  display_line_val_and_sum("   errors", size_errors);
  display_line_val_and_sum("   Cumulative Time (us)",size_time);
  display_line_div_and_sum("   Average Time (us)",size_time,size_count);
  
  display_line_topic("");  
  pChar += sprintf(pChar,"\n");
  
  if (doreset) {
    for (i=0; i<storaged_sub_thread_count; i++) {
      memset(&p[i].stat,0,sizeof(p[i].stat));
    }          
    pChar += sprintf(pChar,"Reset Done\n");
  }

  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());
}

 /**
 * prototypes
 */
uint32_t storaged_sub_thread_rcvReadysock(void * storaged_sub_thread_ctx_p,int socketId);
uint32_t storaged_sub_thread_rcvMsgsock(void * storaged_sub_thread_ctx_p,int socketId);
uint32_t storaged_sub_thread_xmitReadysock(void * storaged_sub_thread_ctx_p,int socketId);
uint32_t storaged_sub_thread_xmitEvtsock(void * storaged_sub_thread_ctx_p,int socketId);

#define DISK_SO_SENDBUF  (300*1024)

/*
**  Call back function for socket controller
*/
ruc_sockCallBack_t storaged_sub_thread_callBack_sock=
  {
     storaged_sub_thread_rcvReadysock,
     storaged_sub_thread_rcvMsgsock,
     storaged_sub_thread_xmitReadysock,
     storaged_sub_thread_xmitEvtsock
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

uint32_t storaged_sub_thread_xmitReadysock(void * unused,int socketId)
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
uint32_t storaged_sub_thread_xmitEvtsock(void * unused,int socketId)
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

uint32_t storaged_sub_thread_rcvReadysock(void * unused,int socketId)
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
    
  @param msg: pointer to disk response message
 
  @retval :none
*/
void storaged_sub_thread_read_response(storaged_sub_thread_msg_t *msg) 
{
  int ret;
  uint32_t                       opcode;
  rozorpc_srv_ctx_t            * rpcCtx = msg->rpcCtx;
  uint64_t                       tic, toc;  
  struct timeval                 tv;  
  
  rpcCtx = msg->rpcCtx;
  opcode = msg->opcode;
  tic    = msg->timeStart; 

  opcode = msg->opcode;

  switch (opcode) {
    case MP_REMOVE2:
    case MP_REMOVE:
       STOP_PROFILING(remove);
       break;
    case MP_LIST_BINS_FILES:
       STOP_PROFILING(list_bins_files);
       break; 
    case MP_SIZE:
       STOP_PROFILING(size);
       break; 
    default:
      severe("Unexpected opcode %d", opcode);
  }
  
  /*
  ** send the response towards the storcli process that initiates the disk operation
  */   
  ret = af_unix_generic_send_stream_with_idx((int)rpcCtx->socketRef,rpcCtx->xmitBuf); 
  if (ret == 0) {
    /**
    * success so remove the reference of the xmit buffer since it is up to the called
    * function to release it
    */
    ROZORPC_SRV_STATS(ROZORPC_SRV_SEND);
    rpcCtx->xmitBuf = NULL;
  }
  else {
    ROZORPC_SRV_STATS(ROZORPC_SRV_SEND_ERROR);
  }
    
  rozorpc_srv_release_context(rpcCtx);   
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

uint32_t storaged_sub_thread_rcvMsgsock(void * unused,int socketId)
{
  storaged_sub_thread_msg_t   msg;
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
    if (storaged_sub_thread_pending_req_count == 0)
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
	storaged_sub_thread_empty_recv_count++;
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
    
    storaged_sub_thread_pending_req_count--;
    if (  storaged_sub_thread_pending_req_count < 0) 
    {
      severe("storaged_sub_thread_pending_req_count is negative");
      storaged_sub_thread_pending_req_count = 0;
    }
    storaged_sub_thread_read_response(&msg); 
  }       
  return TRUE;
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
void storaged_sub_thread_intf_send_response (storaged_sub_thread_ctx_t *thread_ctx_p, storaged_sub_thread_msg_t * msg, int status) 
{
  int                     ret;
  
  msg->status = status;
  
  /*
  ** send back the response
  */  
  ret = sendto(thread_ctx_p->sendSocket,msg, sizeof(*msg),0,(struct sockaddr*)&storaged_sub_thread_rsp_socket_name,sizeof(storaged_sub_thread_rsp_socket_name));
  if (ret <= 0) {
     fatal("storaged_sub_thread_intf_send_response %d sendto(%s) %s", thread_ctx_p->thread_idx, storaged_sub_thread_rsp_socket_name.sun_path, strerror(errno));
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
int storaged_sub_thread_intf_send_req(uint32_t                      opcode, 
                                 rozorpc_srv_ctx_t            * rpcCtx,
				 storage_t                    * st,
				   uint64_t                     timeStart) 
{
  int                         ret;
  storaged_sub_thread_msg_t    msg;
 
  /* Fill the message */
  msg.msg_len         = sizeof(storaged_sub_thread_msg_t)-sizeof(msg.msg_len);
  msg.opcode          = opcode;
  msg.status          = 0;
  msg.transaction_id  = transactionId++;
  msg.timeStart       = timeStart;
  msg.rpcCtx          = rpcCtx;
  msg.st              = st;
  
  /* Send the buffer to its destination */
  ret = sendto(storaged_sub_thread_rsp_socket,&msg, sizeof(msg),0,(struct sockaddr*)&storaged_sub_thread_req_socket_name,sizeof(storaged_sub_thread_req_socket_name));
  if (ret <= 0) {
     fatal("storaged_sub_thread_intf_send_req count %d sendto(%s) %s", storaged_sub_thread_pending_req_count,
                                                                    storaged_sub_thread_req_socket_name.sun_path, strerror(errno));
     exit(0);  
  }
  
  storaged_sub_thread_pending_req_count++;
  if (storaged_sub_thread_pending_req_count > storaged_sub_thread_pending_req_max_count)
      storaged_sub_thread_pending_req_max_count = storaged_sub_thread_pending_req_count;
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
int storaged_sub_thread_rsp_socket_create(char *socketname)
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
                                                "subThreadRsp",   // name of the socket
                                                16,                  // Priority within the socket controller
                                                (void*)NULL,      // user param for socketcontroller callback
                                                &storaged_sub_thread_callBack_sock);  // Default callbacks
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
void storaged_sub_thread_scheduler_entry_point(uint64_t current_time)
{
  storaged_sub_thread_rcvMsgsock(NULL,storaged_sub_thread_rsp_socket);
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
int storaged_sub_thread_intf_create(char * hostname, int nb_threads) {

  storaged_sub_thread_count = nb_threads;
   
  /*
  ** Create and name the response socket where the threads will post their responses
  */
  storaged_sub_thread_rsp_socket_name.sun_family = AF_UNIX;  
  sprintf(storaged_sub_thread_rsp_socket_name.sun_path,"%s_%s_rsp",ROZOFS_SOCK_FAMILY_STORAGED,hostname);       
  storaged_sub_thread_rsp_socket = storaged_sub_thread_rsp_socket_create(storaged_sub_thread_rsp_socket_name.sun_path);
  if (storaged_sub_thread_rsp_socket < 0) {
    fatal("storaged_sub_thread_intf_create storaged_sub_thread_rsp_socket_create(%s) %s",storaged_sub_thread_rsp_socket_name.sun_path, strerror(errno));
    return -1;
  }

  /*
  ** Create and name the request socket where the threads will read the requests
  */  
  storaged_sub_thread_req_socket_name.sun_family = AF_UNIX;   
  sprintf(storaged_sub_thread_req_socket_name.sun_path,"%s_%s_req",ROZOFS_SOCK_FAMILY_STORAGED,hostname);       
  
  
  /*
  ** init of the AF_UNIX sockaddr associated with the south socket (socket used for disk response receive)
  */
    
  uma_dbg_addTopic_option("subThread", storaged_sub_thread_debug, UMA_DBG_OPTION_RESET); 
  /*
  ** attach the callback on socket controller
  */
  ruc_sockCtrl_attach_applicative_poller(storaged_sub_thread_scheduler_entry_point);  
   
  return storaged_sub_thread_create(storaged_sub_thread_req_socket_name.sun_path, hostname,nb_threads);
}
