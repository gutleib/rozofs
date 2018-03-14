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
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/core/rozofs_socket_family.h>
#include "export.h"
#include "rozofs_quota_api.h"
 
int        af_unix_quota_north_socket_ref = -1;
int        af_unix_quota_empty_recv_count = 0;


 

 /**
 * prototypes
 */
uint32_t af_unix_quota_rcvReadysock(void * af_unix_quota_ctx_p,int socketId);
uint32_t af_unix_quota_rcvMsgsock(void * af_unix_quota_ctx_p,int socketId);
uint32_t af_unix_quota_xmitReadysock(void * af_unix_quota_ctx_p,int socketId);
uint32_t af_unix_quota_xmitEvtsock(void * af_unix_quota_ctx_p,int socketId);

#define QUOTA_SO_SENDBUF  (300*1024)
#define QUOTA_SOCKET_NICKNAME "quota_srv"
/*
**  Call back function for socket controller
*/
ruc_sockCallBack_t af_unix_quota_callBack_sock=
  {
     af_unix_quota_rcvReadysock,
     af_unix_quota_rcvMsgsock,
     af_unix_quota_xmitReadysock,
     af_unix_quota_xmitEvtsock
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

uint32_t af_unix_quota_xmitReadysock(void * unused,int socketId)
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
uint32_t af_unix_quota_xmitEvtsock(void * unused,int socketId)
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

uint32_t af_unix_quota_rcvReadysock(void * unused,int socketId)
{
  return TRUE;
}
/*
**__________________________________________________________________________
*/
/**
*  Thar API is intended to be used by the main thread of the exportd for sending back a 
   response associated with a quota request
   
   The header must contains the following information:
      xid : xid of the initial request
      opcode : command opcode
      path: sunpath of the AF_UNIX socket of the requester
      length : total length of the message including the header
   
   @param hdr_p: pointer to the thread context (contains the thread source socket )
   
   @retval none
*/
void rozofs_qt_send_response (rozofs_qt_header_t  *hdr_p) 
{
  int  ret; 
  struct  sockaddr_un path;
  /*
  ** build the path of the socket: needed by the receiver for sending back the
  ** response
  */
  rozofs_qt_set_socket_name_with_pid_of_requester(&path,hdr_p->pid);
  /*
  ** send back the response
  */ 
  ret = sendto(af_unix_quota_north_socket_ref,hdr_p, hdr_p->length,0,(struct sockaddr*)&path,sizeof(path));
  if (ret <= 0) {
     /*
     ** Just send a warning: it might be possible that the requester has closed its socket
     */
     severe("rozofs_qt_send_responsesendto(%s) %s", path.sun_path, strerror(errno));
  }
}

/*
**__________________________________________________________________________
*/
/**
*   set Quota message

    @param msg : message associated with the request
    
    @retval none

*/
void rozofs_qt_set_quota_msg_process(rozofs_qt_header_t *msg)
{
    rozofs_setquota_req_t *msg_in = (rozofs_setquota_req_t*) msg;
    rozofs_setquota_rsp_t msg_out ;    
    int ret = -1;
    
    errno = 0;
    /*
    ** copy the header in response message
    */
    memcpy(&msg_out,msg,sizeof(rozofs_qt_header_t));
    
    if ((msg_in->sqa_type != USRQUOTA) && (msg_in->sqa_type != GRPQUOTA)&&(msg_in->sqa_type != SHRQUOTA))
    {
       severe("bad quota type %d",msg_in->sqa_type);
       errno = EPROTO;
       goto out;
    }
    
    ret = rozofs_qt_set_quota(msg_in->eid,msg_in->sqa_type,msg_in->sqa_id,msg_in->sqa_qcmd,&msg_in->sqa_dqblk);
out:
    msg_out.status  = ret;
    msg_out.errcode = errno;
    /*
    ** send back the response
    */
   rozofs_qt_send_response((rozofs_qt_header_t*)&msg_out);

}

/*
**__________________________________________________________________________
*/
/**
*   get Quota message

    @param msg : message associated with the request
    
    @retval none

*/
void rozofs_qt_get_quota_msg_process(rozofs_qt_header_t *msg)
{
    rozofs_getquota_req_t *msg_in = (rozofs_getquota_req_t*) msg;
    rozofs_getquota_rsp_t msg_out ; 
    rozofs_qt_cache_entry_t *dquot =NULL;   
    
    errno = 0;
    /*
    ** copy the header in response message
    */
    memset(&msg_out,0,sizeof(msg_out));
    memcpy(&msg_out,msg,sizeof(rozofs_qt_header_t));
    
    if ((msg_in->gqa_type != USRQUOTA) && (msg_in->gqa_type != GRPQUOTA)&&(msg_in->gqa_type != SHRQUOTA))
    {
       msg_out.status  = -1;
       severe(" bad type %d ",msg_in->gqa_type);
       msg_out.errcode = EPROTO;
       goto out;
    }
    
    dquot = rozofs_qt_get_quota(msg_in->eid,msg_in->gqa_type,msg_in->gqa_id);
    if (dquot != NULL)
    {
       
       msg_out.status  = 0;
       msg_out.errcode = 0;   
       msg_out.quota_data.rq_bsize = 1024; 
       msg_out.quota_data.rq_active = 1; 
       msg_out.quota_data.rq_bhardlimit = dquot->dquot.quota.dqb_bhardlimit; 
       msg_out.quota_data.rq_bsoftlimit = dquot->dquot.quota.dqb_bsoftlimit; 
       msg_out.quota_data.rq_curblocks  = dquot->dquot.quota. dqb_curspace;
       msg_out.quota_data.rq_fhardlimit = dquot->dquot.quota.dqb_ihardlimit; 
       msg_out.quota_data.rq_fsoftlimit = dquot->dquot.quota.dqb_isoftlimit; 
       msg_out.quota_data.rq_curfiles   = dquot->dquot.quota.dqb_curinodes; 
       msg_out.quota_data.rq_btimeleft  = dquot->dquot.quota.dqb_btime; 
       msg_out.quota_data.rq_ftimeleft  = dquot->dquot.quota.dqb_itime ;
    }
    else
    {
       msg_out.status  = (errno==0)?0:-1;
       msg_out.errcode = errno;    
    
    }
out:
    /*
    ** send back the response
    */
    rozofs_qt_send_response((rozofs_qt_header_t*)&msg_out);
}

/*
**__________________________________________________________________________
*/
/**
*   get grace period message

    @param msg : message associated with the request
    
    @retval none

*/
void rozofs_qt_get_grace_msg_process(rozofs_qt_header_t *msg)
{
    rozofs_getgrace_rsp_t msg_out ; 
        /*
    ** copy the header in response message
    */
    memset(&msg_out,0,sizeof(msg_out));
    memcpy(&msg_out,msg,sizeof(rozofs_qt_header_t));
    
    msg_out.status  = -1;
    msg_out.errcode = EPROTO;
    /*
    ** send back the response
    */
    rozofs_qt_send_response((rozofs_qt_header_t*)&msg_out);
}

/*
**__________________________________________________________________________
*/
/**
*   set grace period message

    @param msg : message associated with the request
    
    @retval none

*/
void rozofs_qt_set_grace_msg_process(rozofs_qt_header_t *msg)
{
    rozofs_setgrace_req_t *msg_in = (rozofs_setgrace_req_t*) msg;
    rozofs_setgrace_rsp_t msg_out ;
    int ret=-1; 
    /*
    ** copy the header in response message
    */
    memset(&msg_out,0,sizeof(msg_out));
    memcpy(&msg_out,msg,sizeof(rozofs_qt_header_t));
    if ((msg_in->sqa_type != USRQUOTA) && (msg_in->sqa_type != GRPQUOTA)&&(msg_in->sqa_type != SHRQUOTA))
    {
       severe("bad quota type sqa_type:%d",msg_in->sqa_type);
       errno = EPROTO;
       goto out;
    }    

    ret = rozofs_qt_set_quotainfo(msg_in->eid,msg_in->sqa_type,msg_in->sqa_id,msg_in->sqa_qcmd,&msg_in->sqa_dqblk);
out:
    msg_out.status  = ret;
    msg_out.errcode = errno;
    /*
    ** send back the response
    */
    rozofs_qt_send_response((rozofs_qt_header_t*)&msg_out);
}

/*
**__________________________________________________________________________
*/
/**
*   set quota state : on or off

    @param msg : message associated with the request
    
    @retval none

*/
void rozofs_qt_set_quota_state_msg_process(rozofs_qt_header_t *msg)
{
    rozofs_setquota_state_req_t *msg_in = (rozofs_setquota_state_req_t*) msg;
    rozofs_setquota_state_rsp_t msg_out ;
    int ret=-1; 
    /*
    ** copy the header in response message
    */
    memset(&msg_out,0,sizeof(msg_out));
    memcpy(&msg_out,msg,sizeof(rozofs_qt_header_t));
    if ((msg_in->sqa_type != USRQUOTA) && (msg_in->sqa_type != GRPQUOTA)&&(msg_in->sqa_type != SHRQUOTA))
    {
       severe("bad quota type sqa_type:%d",msg_in->sqa_type);
       errno = EPROTO;
       goto out;
    }    

    ret = rozofs_qt_set_quotastate(msg_in->eid,msg_in->sqa_type,msg_in->cmd);
out:
    msg_out.status  = ret;
    msg_out.errcode = errno;
    /*
    ** send back the response
    */
    rozofs_qt_send_response((rozofs_qt_header_t*)&msg_out);
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
void af_unix_quota_cmd_process(rozofs_qt_header_t *msg) 
{
 int opcode;
  /*
  ** dispatch the processing according to the opcode
  */
  opcode = msg->opcode;

  switch (opcode) {
    case ROZOFS_QUOTA_GETQUOTA:
       /*
       ** Get quota for either a user or a group
       */
       rozofs_qt_get_quota_msg_process(msg);
       break;   

    case ROZOFS_QUOTA_SETQUOTA:
       /*
       ** Set quota for a user or a group
       */  
       rozofs_qt_set_quota_msg_process(msg);
       break;

    case ROZOFS_QUOTA_GETGRACE:
       /*
       ** Get quota grace period for all user or group
       */
       rozofs_qt_get_grace_msg_process(msg);
       break;   

    case ROZOFS_QUOTA_SETGRACE:
       /*
       ** Set quota for a user or a group
       */  
       rozofs_qt_set_grace_msg_process(msg);
       break;
    case ROZOFS_QUOTA_SET:
       /*
       ** Set quota on/off for a user or a group
       */  
       rozofs_qt_set_quota_state_msg_process(msg);
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
   
   That service is intended to process a command related to quota

    
  @param unused: user parameter not used by the application
  @param socketId: reference of the socket 
 
   @retval : TRUE-> xmit ready event expected
  @retval : FALSE-> xmit  ready event not expected
*/

uint32_t af_unix_quota_rcvMsgsock(void * unused,int socketId)
{

  int                        bytesRcvd;
  int eintr_count = 0;
  union {  
  rozofs_getquota_req_t rozofs_getquota_req_msg;
  rozofs_setquota_req_t rozofs_setquota_req_msg;
  rozofs_getgrace_req_t rozofs_getgrace_req_msg;
  rozofs_setgrace_req_t  rozofs_setgrace_req_msg;
  } msg;
  /*
  ** disk responses have the highest priority, loop on the socket until
  ** the socket becomes empty
  */
  while(1) {  
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
	af_unix_quota_empty_recv_count++;
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
         severe ("quota server error too many eintr_count %d",eintr_count);
         return TRUE;

       case EBADF:
       case EFAULT:
       case EINVAL:
       default:
         /*
         ** We might need to double checl if the socket must be killed
         */
         fatal("quota server error on recvfrom %s !!\n",strerror(errno));
         exit(0);
     }

    }
    if (bytesRcvd == 0) {
      fatal("quota server socket is dead %s !!\n",strerror(errno));
      exit(0);    
    } 
    /*
    ** clear the fd in the receive set to avoid computing it twice
    */
    ruc_sockCtrl_clear_rcv_bit(socketId);
    
    af_unix_quota_cmd_process((rozofs_qt_header_t*)&msg); 
  }       
  return TRUE;
}



/*
**__________________________________________________________________________
*/

/**
* creation of the AF_UNIX socket that is attached on the socket controller

  That socket is used to receive the quota request from the threads that
  relay the operator commands concering quota service
  
  @param socketname : name of the socket
  
  @retval >= 0 : reference of the socket
  @retval < 0 : error
*/
int af_unix_quota_north_socket_create(char *socketname)
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
     fd = af_unix_sock_create_internal(socketname,QUOTA_SO_SENDBUF);
     if (fd == -1)
     {
       break;
     }
     /*
     ** OK, we are almost done, just need to connect with the socket controller
     */
     sockctrl_ref = ruc_sockctl_connect(fd,  // Reference of the socket
                                                QUOTA_SOCKET_NICKNAME,   // name of the socket
                                                0,                  // Priority within the socket controller
                                                (void*)NULL,       // user param for socketcontroller callback
                                                &af_unix_quota_callBack_sock);  // Default callbacks
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
* Initialize the quota thread interface
*
* @param slave_id : reference of the slave exportd
*
*  @retval 0 on success -1 in case of error
*/
int rozofs_qt_thread_intf_create(int slave_id) {
  char socketName[128];

  /*
  ** create the socket for receiving quota commands
  */ 
  
  sprintf(socketName,"%s_%d", ROZOFS_SOCK_FAMILY_QUOTA_NORTH_SUNPATH,slave_id);
  af_unix_quota_north_socket_ref = af_unix_quota_north_socket_create(socketName);
  if (af_unix_quota_north_socket_ref < 0) {
    fatal("rozofs_qt_thread_intf_create af_unix_sock_create(%s) %s",socketName, strerror(errno));
    return -1;
  }
     
  return 0;
}



