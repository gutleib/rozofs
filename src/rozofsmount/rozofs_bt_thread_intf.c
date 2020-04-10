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
#include "rozofs_bt_proto.h"
//#include "rozofs_bt_thread_intf.h"
#include <rozofs/core/rozofs_socket_family.h>
#include "rozofs_bt_inode.h"
 
int        af_unix_bt_south_socket_ref = -1;
int        af_unix_bt_thread_count=0;
int        af_unix_bt_pending_req_count = 0;

struct  sockaddr_un rozofs_bt_south_socket_name;
 
int rozofs_bt_thread_create( int nb_threads) ;


#if 0
/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
#define new_line(title,empty) { \
  if (lineEmpty) { pChar = pLine;}\
  lineEmpty = empty;\
  pLine = pChar;\
  *pChar++ = '\n';\
  pChar += rozofs_string_padded_append(pChar,25,rozofs_left_alignment,title);\
  *pChar++ = '|';\
}  
    
#define display_val(val){\
  pChar += rozofs_u64_padded_append(pChar, 17, rozofs_right_alignment, val);\
  *pChar++ = ' ';\
  *pChar++ = '|';\
}
  
#define display_div(val1,val2) if (val2==0) { display_val(0)} else { display_val(val1/val2)}
#define display_txt(txt) {\
  pChar += rozofs_string_padded_append(pChar,17, rozofs_right_alignment, txt);\
  *pChar++ = ' ';\
  *pChar++ = '|';\
}

#define display_line_topic(title) \
  new_line(title,0);\
  for (i=startIdx; i<(stopIdx+last); i++) {\
    pChar += rozofs_string_append(pChar,"__________________|");\
  }
    
#define display_line_val(title,val) \
  new_line(title,1);\
  for (i=startIdx; i<stopIdx; i++) {\
    sum.val += p[i].stat.val;\
    display_val(p[i].stat.val);\
  }\
  if (sum.val!=0) { lineEmpty=0; }\
  if (last) { display_val(sum.val);}
    

#define display_line_div(title,val1,val2) \
  new_line(title,1);\
  for (i=startIdx; i<stopIdx; i++) {\
    display_div(p[i].stat.val1,p[i].stat.val2);\
  }\
  if (sum.val1!=0) { lineEmpty=0; }\
  if (last) { display_div(sum.val1,sum.val2); }

 
static char * rozofs_bt_thread_debug_help(char * pChar) {
  pChar += rozofs_string_append(pChar,"usage:\nfuseThreads reset       : reset statistics\nfuseThreads             : display statistics\n");  
  return pChar; 
}  
#define THREAD_PER_LINE 6
void rozofs_bt_thread_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
  char           *pChar=uma_dbg_get_buffer();
  char           *pLine=pChar;
  int             lineEmpty=0;
  int             doreset=0;
  int i;
  rozofs_bt_thread_ctx_t *p = rozofs_bt_thread_ctx_tb;
  int startIdx,stopIdx;
  rozofs_bt_thread_stat_t sum;
  int                       last=0;
  
  if (argv[1] != NULL) {
    if (strcmp(argv[1],"reset")==0) {
      doreset = 1;
    }
    else {  
      pChar = rozofs_bt_thread_debug_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
      return;
    }        
  }
  
  memset(&sum, 0, sizeof(sum));
  stopIdx  = 0;
  last = 0;
   
  while (last == 0) {
  
    startIdx = stopIdx;
    if ((af_unix_bt_thread_count - startIdx) > THREAD_PER_LINE) {
      stopIdx = startIdx + THREAD_PER_LINE;
    }  
    else {
      stopIdx = af_unix_bt_thread_count;
      last = 1;
    }  
    
    new_line("Thread number",0);
    for (i=startIdx; i<stopIdx; i++) {
      display_val(p[i].thread_idx);
    } 
    if (last) {
      display_txt("Total");
    }   
#if 1
    display_line_topic("Read Requests");  
    display_line_val("   number", write_count);
    display_line_val("   Bytes",write_Byte_count);      
    display_line_val("   Cumulative Time (us)",write_time);
    display_line_div("   Average Bytes",write_Byte_count,write_count);  
    display_line_div("   Average Time (us)",write_time,write_count);
    display_line_div("   Throughput (MBytes/s)",write_Byte_count,write_time);  
    display_line_val("   Page cache",use_page_cache_count);      
#endif
 
    display_line_topic("");  
    *pChar++= '\n';
    *pChar = 0;
  }

  if (doreset) {
    for (i=0; i<af_unix_bt_thread_count; i++) {
      memset(&p[i].stat,0,sizeof(p[i].stat));
    } 
    pChar += sprintf(pChar,"reset done\n");    
  }
  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());
}

#endif

 /**
 * prototypes
 */
uint32_t af_unix_bt_rcvReadysock(void * af_unix_bt_ctx_p,int socketId);
uint32_t af_unix_bt_rcvMsgsock(void * af_unix_bt_ctx_p,int socketId);
uint32_t af_unix_bt_xmitReadysock(void * af_unix_bt_ctx_p,int socketId);
uint32_t af_unix_bt_xmitEvtsock(void * af_unix_bt_ctx_p,int socketId);

#define DISK_SO_SENDBUF  (300*1024)
#define MAIN_BATCH_SOCKET_NICKNAME "batch_rsp_th"
/*
**  Call back function for socket controller
*/
ruc_sockCallBack_t af_unix_bt_callBack_sock=
  {
     af_unix_bt_rcvReadysock,
     af_unix_bt_rcvMsgsock,
     af_unix_bt_xmitReadysock,
     af_unix_bt_xmitEvtsock
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

uint32_t af_unix_bt_xmitReadysock(void * unused,int socketId)
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
   has replied TRUE in rozofs_bt_xmitReadysock().
   
   It typically the processing of a end of congestion on the socket

    
  @param unused: not used
  @param socketId: reference of the socket (not used)
 
   @retval :always TRUE
*/
uint32_t af_unix_bt_xmitEvtsock(void * unused,int socketId)
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

uint32_t af_unix_bt_rcvReadysock(void * unused,int socketId)
{
  return TRUE;
}



/*
**__________________________________________________________________________
*/
/**
  Processes either a read or write response

   Called from the socket controller when there is a response from a disk thread
   the response is either for a disk read or write
    
  @param msg: pointer to disk response message
 
  @retval :none
*/


void af_unix_bt_response(rozofs_bt_thread_msg_t *msg) 
{
  switch (msg->opcode) {
  
    case ROZO_BATCH_TRK_FILE_READ:
    {
      FDL_INFO("FDL iosubmit_tracking_file_read_cbk received!!");
      return rozofs_bt_iosubmit_tracking_file_read_cbk(msg);
      break;
    }  
    default:
      severe("Unexpected opcode %d", msg->opcode);
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

uint32_t af_unix_bt_rcvMsgsock(void * unused,int socketId)
{
  rozofs_bt_thread_msg_t   msg;
  int                      bytesRcvd;
  int eintr_count = 0;
  


  /*
  ** disk responses have the highest priority, loop on the socket until
  ** the socket becomes empty
  */
  while(1) {  
    /*
    ** check if there are some pending requests
    */
//    if (af_unix_bt_pending_req_count == 0)
//    {
//     return TRUE;
//    }
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
         severe ("Fuse Thread Response error too many eintr_count %d",eintr_count);
         return TRUE;

       case EBADF:
       case EFAULT:
       case EINVAL:
       default:
         /*
         ** We might need to double checl if the socket must be killed
         */
         fatal("Fuse Thread Response error on recvfrom %s !!\n",strerror(errno));
         exit(0);
     }

    }
    if (bytesRcvd == 0) {
      fatal("Fuse Thread Response socket is dead %s !!\n",strerror(errno));
      exit(0);    
    } 
//    af_unix_bt_pending_req_count--;
//    if (  af_unix_bt_pending_req_count < 0) af_unix_bt_pending_req_count = 0;
    af_unix_bt_response(&msg); 
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
  
  @retval none
*/
void rozofs_bt_set_sockname_with_hostname(struct sockaddr_un *socketname,char *name,char *hostname,int instance_id)
{
  socketname->sun_family = AF_UNIX;  
  char * pChar = socketname->sun_path;
  pChar += rozofs_string_append(pChar,name);
  *pChar++ = '_';
  *pChar++ = 'B';
  *pChar++ = 'A';
  *pChar++ = 'T';
  *pChar++ = 'C';
  *pChar++ = 'H';  
  *pChar++ = '_';
  pChar += rozofs_u32_append(pChar,instance_id);
  *pChar++ = '_';  
  pChar += rozofs_string_append(pChar,hostname);
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
void rozofs_bt_th_send_response (rozofs_bt_thread_ctx_t *thread_ctx_p, rozofs_bt_thread_msg_t * msg, int status) 
{
  int                     ret;
//    uint64_t tic;
    struct timeval tv;
    gettimeofday(&tv,(struct timezone *)0);
//     tic = MICROLONG(tv);
  
  msg->status = status;
//  msg->timeResp = tic;
  
  /*
  ** send back the response
  */  
  ret = sendto(thread_ctx_p->sendSocket,msg, sizeof(*msg),0,(struct sockaddr*)&rozofs_bt_south_socket_name,sizeof(rozofs_bt_south_socket_name));
  if (ret <= 0) {
     fatal("rozofs_bt_th_send_response %d sendto(%s) %s", thread_ctx_p->thread_idx, rozofs_bt_south_socket_name.sun_path, strerror(errno));
     exit(0);  
  }
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
                                                MAIN_BATCH_SOCKET_NICKNAME,   // name of the socket
                                                16,                  // Priority within the socket controller
                                                (void*)NULL,      // user param for socketcontroller callback
                                                &af_unix_bt_callBack_sock);  // Default callbacks
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
** Initialize the disk thread interface
 
  @param hostname    hostname (for tests)
  @param instance_id : expbt instance
  @param nb_threads  Number of threads that can process the read or write requests
*
*  @retval 0 on success -1 in case of error
*/
int rozofs_bt_thread_intf_create(char * hostname, int instance_id, int nb_threads) {

  af_unix_bt_thread_count = nb_threads;
  int ret;

  /*
  ** init of the AF_UNIX sockaddr associated with the south socket (socket used for disk response receive)
  */
  rozofs_bt_set_sockname_with_hostname(&rozofs_bt_south_socket_name,ROZOFS_SOCK_FAMILY_FUSE_SOUTH,hostname, instance_id);
    
  /*
  ** hostname is required for the case when several storaged run on the same server
  ** as is the case of test on one server only
  */   
  af_unix_bt_south_socket_ref = af_unix_bt_response_socket_create(rozofs_bt_south_socket_name.sun_path);
  if (af_unix_bt_south_socket_ref < 0) {
    fatal("rozofs_bt_thread_intf_create af_unix_sock_create(%s) %s",rozofs_bt_south_socket_name.sun_path, strerror(errno));
    return -1;
  }
  
//  uma_dbg_addTopic_option("batchThreads", rozofs_bt_thread_debug,UMA_DBG_OPTION_RESET); 
   

  ret =  rozofs_bt_thread_create(nb_threads);
  if (ret < 0) return ret;

  return ret;

  
}



