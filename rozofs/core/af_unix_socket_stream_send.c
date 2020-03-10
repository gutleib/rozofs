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
#include <rozofs/common/log.h>

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
  @param len_sent_p : contains the effective length

@retval RUC_OK : message sent
@retval RUC_PARTIAL : message partially sent
@retval RUC_WOULDBLOCK : congested (not sent)
@retval RUC_DISC : bad destination
**
**--------------------------------------------
*/
uint32_t af_unix_send_stream_generic(int fd,char *pMsg,int lgth,int *len_sent_p)
{
  int                     ret;

  *len_sent_p = 0;
  ret=send(fd,pMsg,(int)lgth,0);
  if (ret == 0)
  {
     /*
     ** the other end is probably dead
     */
     return RUC_DISC;
  }
  if (ret > 0)
  {
     *len_sent_p = ret;
     if (ret == lgth) return RUC_OK;
     return RUC_PARTIAL;
  }
  /*
  ** error cases
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
  return RUC_DISC;
 }



 /*
**__________________________________________________________________________
*/
void af_unix_send_stream_fsm(af_unix_ctx_generic_t *socket_p,com_xmit_template_t *xmit_p)
{
  char *pbuf;
  int write_len;
  int ret;
  int inuse;
  uint64_t cycles_before;
  uint64_t cycles_after;

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
         write_len  = (int)ruc_buf_getPayloadLen(xmit_p->bufRefCurrent);
         /*
         ** Get the reference of the destination socket (name) from the ruc_buffer)
         */
         xmit_p->nbWrite  = 0;
         xmit_p->nb2Write = write_len;
         xmit_p->state = XMIT_IN_PRG;
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
        socket_p->stats.totalXmitAttemptsCycles++;
        pbuf = (char *)ruc_buf_getPayload(xmit_p->bufRefCurrent);
        cycles_before = ruc_rdtsc();
        ret  = af_unix_send_stream_generic(socket_p->socketRef,pbuf+xmit_p->nbWrite,xmit_p->nb2Write - xmit_p->nbWrite, &write_len);
        cycles_after = ruc_rdtsc();
        socket_p->stats.totalXmitCycles+= (cycles_after - cycles_before);
        
        switch (ret)
        {
          case RUC_OK:
	  /*
	  ** update speculative scheduler
	  */
	  ruc_sockCtrl_speculative_scheduler_insert(socket_p->connectionId);
          /*
          ** release the buffer that has been sent
          */
          xmit_p->xmit_credit++;
          inuse = ruc_buf_inuse_decrement(xmit_p->bufRefCurrent);
          if (inuse < 0)
          {
            /*
	    ** inuse MUST never be negative so EXIT !!!!!
	    */
            fatal("Inuse is negative %d",inuse);
          }
          if (socket_p->userXmitDoneCallBack != NULL)
          {
             /*
             ** caution: in that case it is up to the application that provides the callback to release
             ** the xmit buffer
             */
	         if (ruc_buf_get_opaque_ref(xmit_p->bufRefCurrent) == socket_p) 
             {
                   (socket_p->userXmitDoneCallBack)(socket_p->userRef,socket_p->index,xmit_p->bufRefCurrent);
	         }
	         else 
             {
                if (inuse == 1) 
                {
                  /*
                  ** need an obj remove since that buffer might still queue somewhere : typically
                  ** in the xmit list of a load balacner entry.
                  */
                  ruc_objRemove((ruc_obj_desc_t*)xmit_p->bufRefCurrent);
                  ruc_buf_freeBuffer(xmit_p->bufRefCurrent);	
                }        
	         }  
          }
          else
          {
            if (inuse == 1) 
            {
              ruc_objRemove((ruc_obj_desc_t*)xmit_p->bufRefCurrent);
              ruc_buf_freeBuffer(xmit_p->bufRefCurrent);
            }
          }
          xmit_p->bufRefCurrent = NULL;
          xmit_p->nbWrite  = 0;
          xmit_p->nb2Write = 0;
          socket_p->stats.totalXmitSuccess++;
          socket_p->stats.totalXmitBytes += write_len;
          xmit_p->state = XMIT_CHECK_XMITQ;
          break;

          case RUC_PARTIAL:
          /*
          ** need to re-attempt writing
          */
          xmit_p->nbWrite  += write_len;
          socket_p->stats.totalXmitBytes += write_len;
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
          ** something wrong on sending: if the user has a callback use it:
          ** the transmitter is no more the owner of the buffer
          */
          inuse = ruc_buf_inuse_decrement(xmit_p->bufRefCurrent);
          if (inuse < 0)
          {
            /*
	        * inuse MUST never be negative so EXIT !!!!!
	        */
            fatal("Inuse is negative %d",inuse);
          }
          socket_p->stats.totalXmitError++;
          if (socket_p->userDiscCallBack != NULL)
          {
            void *bufref = xmit_p->bufRefCurrent;
            xmit_p->bufRefCurrent = NULL;	     
            if (ruc_buf_get_opaque_ref(bufref) != socket_p) 
            {
              /*
              ** the buffer is affected to another socket, however it might possible
              ** that the real owner of the buffer has finished while the buffer is
              ** still used by that old connection. So it might be necessary to release
              ** the buffer.
              ** However in any case the application must not be inform that there was
              ** an issue while sendig that buffer since the connection is not considered
              ** anymore.
              */ 
              if (inuse == 1) 
              {
                ruc_objRemove((ruc_obj_desc_t*)bufref);
                ruc_buf_freeBuffer(bufref);
              }
              bufref = NULL;
            }
            /*
            ** it is up to the application to release the buffer if the error is fatal:
            ** caution the internal disconnection MUST be called before the application since
            ** the application might attempt to perform a direct re-connection
            */
            xmit_p->state = XMIT_DEAD;
            af_unix_sock_stream_disconnect_internal(socket_p);
            (socket_p->userDiscCallBack)(socket_p->userRef,socket_p->index,bufref,errno);
            return;
          }
          else
          {
              if (inuse == 1) 
              {
                ruc_objRemove((ruc_obj_desc_t*)xmit_p->bufRefCurrent);
                ruc_buf_freeBuffer(xmit_p->bufRefCurrent);
              }            
              xmit_p->bufRefCurrent = NULL;
          }
          /*
          ** general disconnection->need to clean the socket queue
          */
          xmit_p->state = XMIT_DEAD;
          af_unix_sock_stream_disconnect_internal(socket_p);
          return ;
          break;

        }
        break;

      case XMIT_CHECK_XMITQ:
        /*
        ** Check the xmit credit
        */
#if 0
        if (xmit_p->xmit_credit >= xmit_p->xmit_credit_conf)
        {
          xmit_p->xmit_credit = 0;
          /*
          ** asser the flag to request a re-activation on the next run of the socket
          ** controller
          */
          xmit_p->xmit_req_flag = 1;
	  RUC_SOCKCTL_CONGESTED(socket_p->socketCtrl_p,socket_p->socketRef);
          return;
        }
#endif
	RUC_SOCKCTL_EOC(socket_p->socketCtrl_p,socket_p->socketRef);

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
        ruc_buf_inuse_increment(xmit_p->bufRefCurrent);
        xmit_p->state =  XMIT_READY;
        break;


      case XMIT_CONGESTED:
        /*
        ** the transmitter is congested: check of the threshold has reached 0
        */
//        xmit_p->eoc_threshold--;
//        if (xmit_p->eoc_threshold == 0)
        {
           xmit_p->eoc_flag  = 1;
           xmit_p->congested_flag = 0;
           xmit_p->state = XMIT_IN_PRG;
	   RUC_SOCKCTL_EOC(socket_p->socketCtrl_p,socket_p->socketRef);
           break;
        }
#if 0
	else
	{
	  RUC_SOCKCTL_CONGESTED(socket_p->socketCtrl_p,socket_p->socketRef);
	}
#endif
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
int af_unix_generic_stream_send(af_unix_ctx_generic_t *this,void *buf_p)
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
        /*
        ** lock the buffer to avoid a delete from the application while
        ** the xmit is in progress. That situation might occur when a
        ** transaction times out while the buffer is currently the one
        ** that is sent. Removing it will break the Strean (AF_UNIX or TCP)
        */
        ruc_buf_inuse_increment(buf_p);
        xmit_p->bufRefCurrent = buf_p;
	    ruc_buf_set_opaque_ref(buf_p,this);
        af_unix_send_stream_fsm(this,xmit_p);
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
int af_unix_generic_send_stream_with_idx(int  socket_ctx_id,void *buf_p)
{

  af_unix_ctx_generic_t *this = af_unix_getObjCtx_p(socket_ctx_id);
  if (this == NULL)
  {
    RUC_WARNING(-1);
    return -1;
  }
  return af_unix_generic_stream_send(this,buf_p);

}
