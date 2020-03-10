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
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <sys/un.h>
#include <signal.h>
#include <errno.h>
#include <stdarg.h>
#include <unistd.h>
#include "uma_tcp.h"

#include <rozofs/common/types.h>

#include "ruc_common.h"
#include "af_unix_socket_generic.h"
#include "socketCtrl.h"
#include "ruc_sockCtl_api_th.h"


 /*
**__________________________________________________________________________
*/
/**
 Send a message to a destination AF_UNIX socket


  @param fd : source socket
  @param pMsg: pointer to the message to send
  @param lgth : length of the message to send
  @param destSock : pathname of the destination AF_UNIX socket

@retval RUC_OK : message sent
@retval RUC_WOULDBLOCK : congested (not sent)
@retval RUC_DISC : bad destination
**
**--------------------------------------------
*/
uint32_t af_unix_send_generic(int fd,
                            char *pMsg,
                            uint32_t lgth,
                            char* destSock)
{
  struct  sockaddr_un     dest;
  int                     ret;

  dest.sun_family= AF_UNIX;
  bcopy(destSock,dest.sun_path,(strlen(destSock)+1));


  ret=sendto(fd,pMsg,(int)lgth,0,(struct sockaddr*)&dest,sizeof(dest));


  if (ret == -1)
  {
    /*
    ** 2 cases only : WOULD BLOCK or deconnection
    */
    if (errno==EWOULDBLOCK)
    {
      /*
      ** congestion detected: the message has not been
      ** sent
      */
      return RUC_WOULDBLOCK;
    }
    else
    {
 //     RUC_WARNING(errno);
      switch (errno)
      {
        case ENOENT:
        /*
        ** the destination might not be there
        ** -> the north socket has not been yet created
        */
        case ECONNREFUSED:

        /*
        ** socket is still here but the process is certainly down
        */
        default :
        return RUC_DISC;
        break;
      }
      RUC_WARNING(errno);
      return RUC_DISC;
    }
  }
  /*
  ** the message has been sent. Double check
  ** that everything has been sent
  */
  if (ret == (int)lgth)
  {
    /*
    ** it is OK
    */
    return RUC_OK;
  }
  /*
  ** For datagram type either the full message is sent or nothing!!
  */
  fatal( " Cannot block in the message of a message " );

  return RUC_WOULDBLOCK;

 }



 /*
**__________________________________________________________________________
*/
void af_unix_send_fsm(af_unix_ctx_generic_t *socket_p,com_xmit_template_t *xmit_p)
{
  char *pbuf;
  int len;
  int ret;
  char *sockname_p;

  while(1)
  {

    switch (xmit_p->state)
    {
      case XMIT_READY:
      /*
      ** the transmitter is ready to send however we need to double if there is a
      ** current buffer to send (because we just exit from congestion or if there
      ** some buffer in the xmit pending queue
      */
      /*
      ** Check if there is a current buffer to send
      */
      if (xmit_p->bufRefCurrent != NULL)
      {
         pbuf = (char *)ruc_buf_getPayload(xmit_p->bufRefCurrent);
         len  = (int)ruc_buf_getPayloadLen(xmit_p->bufRefCurrent);
         /*
         ** Get the reference of the destination socket (name) from the ruc_buffer)
         */
         socket_p->stats.totalXmitAttempts++;
         sockname_p = ruc_buf_get_usrDestInfo(xmit_p->bufRefCurrent);
         ret  = af_unix_send_generic(socket_p->socketRef,pbuf,len, sockname_p);
         xmit_p->xmit_credit++;

        switch (ret)
        {
          case RUC_OK:
          /*
          ** release the buffer that has been sent
          */
          ruc_buf_freeBuffer(xmit_p->bufRefCurrent);
          xmit_p->bufRefCurrent = NULL;
          socket_p->stats.totalXmitSuccess++;
          socket_p->stats.totalXmitBytes += len;

          xmit_p->state = XMIT_CHECK_XMITQ;
          break;

          case RUC_WOULDBLOCK:
          /*
          ** the socket is congested-> so exit
          */
          socket_p->stats.totalXmitCongested++;
          xmit_p->congested_flag = 1;
          xmit_p->eoc_flag       = 0;
          xmit_p->eoc_threshold  = AF_UNIX_CONGESTION_DEFAULT_THRESHOLD;
          xmit_p->state = XMIT_CONGESTED;
	  RUC_SOCKCTL_CONGESTED(socket_p->socketCtrl_p,socket_p->socketRef);
          return ;

          case RUC_DISC:

          /*
          ** something wrong on sending: if the user has a callback use it
          */
          socket_p->stats.totalXmitError++;
          if (socket_p->userDiscCallBack != NULL)
          {
             void *bufref = xmit_p->bufRefCurrent;
             xmit_p->bufRefCurrent = NULL;
             /*
             ** it is up to the application to release the buffer if the error is fatal
             */
             (socket_p->userDiscCallBack)(socket_p->userRef,socket_p->index,bufref,errno);
          }
          else
          {
            ruc_buf_freeBuffer(xmit_p->bufRefCurrent);
            xmit_p->bufRefCurrent = NULL;

          }
          /*
          ** need to clean the socket queue
          */
          xmit_p->state = XMIT_CHECK_XMITQ;

          /*
          ** need to clean the socket queue
          */

//          xmit_p->state = XMIT_DEAD;
//          return ;
            break;
        }
      }
      else
      {
        /*
        ** nothing to send !!
        */
        return;
      }
      break;

      case XMIT_IN_PRG:

        /*
        ** Check if there is a current buffer to send
        */
        socket_p->stats.totalXmitAttempts++;
        pbuf = (char *)ruc_buf_getPayload(xmit_p->bufRefCurrent);
        len  = (int)ruc_buf_getPayloadLen(xmit_p->bufRefCurrent);
        /*
        ** Get the reference of the destination socket (name) from the ruc_buffer)
        */
        sockname_p = ruc_buf_get_usrDestInfo(xmit_p->bufRefCurrent);
        ret  = af_unix_send_generic(socket_p->socketRef,pbuf,len, sockname_p);
        xmit_p->xmit_credit++;
        switch (ret)
        {
          case RUC_OK:
          /*
          ** release the buffer that has been sent
          */
          ruc_buf_freeBuffer(xmit_p->bufRefCurrent);
          xmit_p->bufRefCurrent = NULL;
          socket_p->stats.totalXmitSuccess++;
          socket_p->stats.totalXmitBytes += len;
          xmit_p->state = XMIT_CHECK_XMITQ;
          break;

          case RUC_WOULDBLOCK:
          /*
          ** the socket is congested-> so exit
          */
          socket_p->stats.totalXmitCongested++;
          xmit_p->congested_flag = 1;
          xmit_p->eoc_flag       = 0;
          xmit_p->eoc_threshold  = AF_UNIX_CONGESTION_DEFAULT_THRESHOLD;
          xmit_p->state = XMIT_CONGESTED;
	  RUC_SOCKCTL_CONGESTED(socket_p->socketCtrl_p,socket_p->socketRef);
          return ;

          case RUC_DISC:
          /*
          ** something wrong on sending: if the user has a callback use it
          */
          socket_p->stats.totalXmitError++;
          if (socket_p->userDiscCallBack != NULL)
          {
             void *bufref = xmit_p->bufRefCurrent;
             xmit_p->bufRefCurrent = NULL;
             /*
             ** it is up to the application to release the buffer if the error is fatal
             */
             (socket_p->userDiscCallBack)(socket_p->userRef,socket_p->index,bufref,errno);
          }
          else
          {
            ruc_buf_freeBuffer(xmit_p->bufRefCurrent);
            xmit_p->bufRefCurrent = NULL;

          }
          /*
          ** need to clean the socket queue
          */
          xmit_p->state = XMIT_CHECK_XMITQ;

 //         xmit_p->state = XMIT_DEAD;
 //         return ;
          break;

        }
        break;

      case XMIT_CHECK_XMITQ:
#if 0
        /*
        ** Check the xmit credit
        */
        if (xmit_p->xmit_credit >= xmit_p->xmit_credit_conf)
        {
          xmit_p->xmit_credit = 0;
          /*
          ** asser the flag to request a re-activation on the next run of the socket
          ** controller
          */
          xmit_p->xmit_req_flag = 1;
          return;
        }
#endif
        /*
        ** check if there is a pending buffer (case found if there was a previous congestion
        */
        if (xmit_p->bufRefCurrent != NULL)
        {
          /*
          * lest's go and send it
          */
          xmit_p->state =  XMIT_IN_PRG;
          break;
        }
        /*
        ** read the pending Xmit queue (only priority 0 is considered in the current implementation
        */
        xmit_p->bufRefCurrent = com_xmit_pendingQueue_get(xmit_p,0);
        if (xmit_p->bufRefCurrent == NULL)
        {
          /*
          ** queue is empty
          */
          xmit_p->xmit_credit = 0;
          xmit_p->state =  XMIT_READY;
          return;
        }
        /*
        ** OK, go back to send that new bufffer
        */
        xmit_p->state =  XMIT_IN_PRG;
        break;


      case XMIT_CONGESTED:
        /*
        ** the transmitter is congested: check of the threshold has reached 0
        */
#if 0
        while(1)
	{
#warning loop on congestion
	  nanosleep(5);
	  printf("FDL congested\n");

	}
#endif
        xmit_p->eoc_threshold--;
        if (xmit_p->eoc_threshold == 0)
        {
           xmit_p->eoc_flag  = 1;
           xmit_p->congested_flag = 0;
           xmit_p->state = XMIT_CHECK_XMITQ;
	   RUC_SOCKCTL_EOC(socket_p->socketCtrl_p,socket_p->socketRef);
           break;
        }
	else
	{
	  RUC_SOCKCTL_CONGESTED(socket_p->socketCtrl_p,socket_p->socketRef);
	}
        return;

       case XMIT_DEAD:
        /*
        ** the transmitter is dead
        */
        return;

    }
  }
}

/*
**__________________________________________________________________________
*/
/**
*  AF_UNIX generic send:
  the ruc_buffer is supposed to contain the reference of the destination socket
  in the usrDestInfo field of the ruc_buffer header (for AF_UNIX it is the
  pathname of the destination socket)
  .
  To obtain the pointer to that array use the ruc_buf_get_usrDestInfo() service API
  Notice that the length of that array is limited to 64 bytes.

  @param this : pointer to the AF UNIX socket context
  @param buf_p: pointer to the buffer to send

  retval 0 : success
  retval -1 : error
*/
int af_unix_generic_send(af_unix_ctx_generic_t *this,void *buf_p)
{
   com_xmit_template_t *xmit_p = &this->xmit;

   switch (xmit_p->state)
   {
      case XMIT_IDLE:
        /*
        ** not normal the socket has not been yet created
        */
        return -1;

      case XMIT_READY:
        xmit_p->bufRefCurrent = buf_p;
        af_unix_send_fsm(this,xmit_p);
        return 0;

      case XMIT_IN_PRG:
      case XMIT_CHECK_XMITQ:
      case XMIT_CONGESTED:
        /*
        ** there is already at least one buffer in transmitter
        */
        com_xmit_pendingQueue_buffer_insert(xmit_p,buf_p,0);
        return 0;

      case XMIT_DEAD:
        /*
        ** the destination socket is dead
        */
        return -1;
      default:
        /*
        ** unknown state
        ** MUST not occur
        */
        return -1;
   }
   return -1;

}


/*
**__________________________________________________________________________
*/
/**
*  AF_UNIX generic send with idx:
  the ruc_buffer is supposed to contain the reference of the destination socket
  in the usrDestInfo field of the ruc_buffer header (for AF_UNIX it is the
  pathname of the destination socket)
  .
  To obtain the pointer to that array use the ruc_buf_get_usrDestInfo() service API
  Notice that the length of that array is limited to 64 bytes.

  @param af_unix_socket_idx : index of the AF UNIX socket context
  @param buf_p: pointer to the buffer to send

  retval 0 : success
  retval -1 : error
*/
int af_unix_generic_send_with_idx(int  socket_ctx_id,void *buf_p)
{

  af_unix_ctx_generic_t *this = af_unix_getObjCtx_p(socket_ctx_id);
  if (this == NULL)
  {
    RUC_WARNING(-1);
    return -1;
  }
  return af_unix_generic_send(this,buf_p);

}
