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

#define UMA_TCP_MAIN_C

/*
** I N C L U D E S
*/
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <errno.h>
#include <stdarg.h>
#include <unistd.h>

#include <rozofs/common/types.h>
#include <rozofs/common/log.h>

#include "ruc_common.h"
#include "ruc_list.h"
#include "ruc_sockCtl_api.h"
#include "ruc_buffer_api.h"
#include "ruc_timer_api.h"
#include "uma_fsm_framework.h"
#include "uma_tcp.h"
#include "uma_tcp_main_api.h"
#include "ruc_trace_api.h"
//#include "uma_unc_config_parameters.h"
#include "socketCtrl.h"

/*
**  local functions prototypes
*/

/*
**  local structure
*/
typedef struct _uma_tcp_socketName_t
{
   uint8_t  name[16];
   uint32_t priority;
   ruc_sockCallBack_t *pCallBack;
} uma_tcp_socketName_t;


/*
** G L O B A L  D A T A
*/

/*
**  head of the free list of the Relay-c context
*/
uma_tcp_t *uma_tcp_freeList= (uma_tcp_t*)NULL;
/*
** head of the active TCP  context
*/
uma_tcp_t  *uma_tcp_activeList= (uma_tcp_t*)NULL;

/*
**   number of context
*/
uint16_t uma_tcp_nbContext;
/*
**  Init flag
*/
int uma_tcpInitDone = FALSE;

/*
**  Call back function for socket controller
*/
ruc_sockCallBack_t uma_tcp_callBack=
  {
     uma_tcp_rcvReady,
     uma_tcp_rcvMsg,
     uma_tcp_xmitReady,
     uma_tcp_xmitEvt
  };




uma_tcp_socketName_t uma_tcp_sockNameAndPriorityTab[] ={
              {"UPPS_SOCK",2,&uma_tcp_callBack},
              };

uint32_t uma_tcp_trace = FALSE;

void uma_tcp_nullMonTxtCbk (char * topic, char *fmt, ... );
uma_mon_txt_cbk uma_tcp_monTxtCbk = uma_tcp_nullMonTxtCbk;

/*
**--------------------------------------
**    P R I V A T E   F U N C T I O N S
**--------------------------------------
*/



void uma_tcp_no_recvCBK(uint32_t userRef,uint32_t bufRef)
{

}

void uma_tcp_no_discCBK(uint32_t userRef)
{


}
/*----------------------------------------------
   uma_tcp_getObjRef
**---------------------------------------------
** that function returns the pointer to the
** TCP CNX context
**
**  IN : tcpRef : TCP CNX object reference
**
**  OUT: NULL : out of range index
**       !=NULL : pointer to the object
**----------------------------------------------
*/
uma_tcp_t *uma_tcp_getObjRef(uint32_t tcpRef)
{

   uint32_t indexType;
   uint32_t index;
   uma_tcp_t *p;

   indexType = (tcpRef >> RUC_OBJ_SHIFT_OBJ_TYPE);
   if (indexType != UMA_TCP_CTX_TYPE)
   {
     /*
     ** not a TCP CNX index type
     */
     return (uma_tcp_t*)NULL;
   }
   /*
   **  Get the pointer to the context
   */
   index = tcpRef & RUC_OBJ_MASK_OBJ_IDX;
   p = (uma_tcp_t*)ruc_objGetRefFromIdx((ruc_obj_desc_t*)uma_tcp_freeList,
                                       index);
   return ((uma_tcp_t*)p);
}
/*--------------------------------
**   TCP connection callbacks
**---------------------------------
*/
//64BITS uint32_t uma_tcp_rcvReady(uint32 tcpRef,int socketId)
uint32_t uma_tcp_rcvReady(void *opaque,int socketId)
{
   uma_tcp_t *p;
   uint32_t   ret;
   uint64_t tcpRef = (uint64_t) opaque;

  /*
  **  Get the pointer to the relc Object
  */
  p = uma_tcp_getObjRef((uint32_t)tcpRef);
  if (p == (uma_tcp_t*)NULL)
  {
    /*
    ** bad reference (we are DEAD !!!
    */
    RUC_WARNING(tcpRef);
    return FALSE;
  }
  ret = uma_tcp_rcvFsm_check_bufferDepletion(p);
  if (ret == TRUE)
  {
    return FALSE;
  }

   /*
   ** test if the user has attache its own call back
   */
   if (p->userRcvReadyCallBack != (ruc_pf_sock_t)NULL)
   {
//64BITS
     uint64_t userRef= (uint64_t) p->userRef;
     return((p->userRcvReadyCallBack)((void*)userRef,socketId));
   }
   return TRUE;

}


//64BITS uint32_t uma_tcp_rcvMsg(uint32 tcpRef,int socketId)
uint32_t uma_tcp_rcvMsg(void *opaque,int socketId)
{
  uma_tcp_t *p;
 uint64_t tcpRef = (uint64_t) opaque;



  UMA_TCP_TRC("relc_rcvMsg",tcpRef,-1,-1,-1);

  /*
  **  Get the pointer to the relc Object
  */
  p = uma_tcp_getObjRef((uint32_t)tcpRef);
  if (p == (uma_tcp_t*)NULL)
  {
    /*
    ** bad reference
    */
    UMA_TCP_TRC("relc_rcvMsgSIG_err",tcpRef,-1,-1,-1);
    RUC_WARNING(tcpRef);
    return TRUE;
  }

   /*
   ** that 's OK, the receive FSM can be called
   */
   uma_tcp_rcvFsm_msgInAssert(p);
   return TRUE;
}


//64BITS uint32_t uma_tcp_xmitReady(uint32 tcpRef,int socketId)
uint32_t uma_tcp_xmitReady(void *opaque,int socketId)
{
 uint64_t tcpRef = (uint64_t) opaque;
  uma_tcp_t *p;

  /*
  ** return TRUE if congested or if one of the pending
  ** queue is not empty
  */
  /*
  **  Get the pointer to the relc Object
  */
  p = uma_tcp_getObjRef((uint32_t)tcpRef);
  if (p == (uma_tcp_t*)NULL)
  {
    /*
    ** bad reference
    */
    UMA_TCP_TRC("relc_xmitReadySIG_err",tcpRef,-1,-1,-1);
    RUC_WARNING(tcpRef);
    return FALSE;
  }
  if ((p->congested == TRUE)||
      (ruc_objIsEmptyList(&p->xmitList[0])!=TRUE) ||
      (ruc_objIsEmptyList(&p->xmitList[1])!=TRUE))
  {
    return TRUE;
  }
  return FALSE /*FALSE */;
}


//64BITS   uint32_t uma_tcp_xmitEvt(uint32 tcpRef,int socketId)
uint32_t uma_tcp_xmitEvt(void * opaque,int socketId)
{
  uma_tcp_t *p;
 uint64_t tcpRef = (uint64_t) opaque;

  /*
  ** end of congestion detected. call the automaton
  */

  /*
  **  Get the pointer to the relc Object
  */
  p = uma_tcp_getObjRef((uint32_t)tcpRef);
  if (p == (uma_tcp_t*)NULL)
  {
    /*
    ** bad reference
    */
    UMA_TCP_TRC("relc_xmitEvtSIG_err",tcpRef,-1,-1,-1);
    RUC_WARNING(tcpRef);
    return TRUE;
  }

  /*
  ** Decrement the end of congestion counter till null
  */

  if (p->eocCounter != 0) {
        p->eocCounter--;
	FD_SET(socketId,&rucWrFdSetCongested);
  	return TRUE;
  }
  FD_CLR(socketId,&rucWrFdSetCongested);

  /*
  ** call the FSM when the end of congestion counter is null
  */
/*  printf("*\n"); */
  uma_tcp_xmit_endOfCongAssert(p);
  return TRUE;

}




void uma_tcp_purge()
{



}

/*
**----------------------------------------------------------
**   F S M    SIGNALS ASSERTION
**----------------------------------------------------------
*/
void uma_tcp_sockDiscAssert(uma_tcp_t *pObj)
{

   pObj->sockDiscRcv = TRUE;
   pObj->sockDiscXmit = TRUE;
   uma_fsm_engine(pObj,&pObj->xmitFsm);
   uma_fsm_engine(pObj,&pObj->rcvFsm);
}


void uma_tcp_sockResetAssert(uma_tcp_t *pObj)
{

   pObj->xmitReset = TRUE;
   pObj->rcvReset = TRUE;
   uma_fsm_engine(pObj,&pObj->xmitFsm);
   uma_fsm_engine(pObj,&pObj->rcvFsm);
}


void uma_tcp_sockRestartAssert(uma_tcp_t *pObj)
{

   pObj->xmitRestart = TRUE;
   pObj->rcvRestart = TRUE;
   uma_fsm_engine(pObj,&pObj->xmitFsm);
   uma_fsm_engine(pObj,&pObj->rcvFsm);
}


void  uma_tcp_fsm_relc_tcpUpAssert(uma_tcp_t *pObj)
{
   uma_fsm_engine(pObj,&pObj->xmitFsm);
   uma_fsm_engine(pObj,&pObj->rcvFsm);

}




/*----------------------------------------------
   uma_tcp_tuneTcpSocket
**---------------------------------------------
** Tune the configuration of the socket with:
**   - TCP KeepAlive,
**   - asynchrounous xmit/receive,
**   -  new sizeof  buffer for xmit/receive
**
**  IN : socketId
**
**  OUT: RUC_OK : success
**       RUC_NOK : error
**----------------------------------------------
*/
uint32_t uma_tcp_tuneTcpSocket(int socketId)
{
  int YES = 1;
#ifndef sol7
#ifndef ins_sol7
  int IDLE = 2;
  int INTVL = 2;
  int COUNT = 3;
#endif
#endif
  int sockSndSize = UMA_TCP_SOCKET_SIZE;
  int sockRcvdSize = UMA_TCP_SOCKET_SIZE;
  int fileflags;

 /*
  ** active keepalive on the new connection
  */
  if (setsockopt (socketId,SOL_SOCKET,
                  SO_KEEPALIVE,&YES,sizeof(int)) == -1)
  {
    return RUC_NOK;
  }

	/* TCP_KEEP.... defined in /netinet/tcp.h for cge21 target but not for Sol7.
		 This source does not compile for Solaris if these Litterals are kept */
#ifndef sol7
#ifndef ins_sol7
  if (setsockopt (socketId,IPPROTO_TCP,
                  TCP_KEEPIDLE,&IDLE,sizeof(int)) == -1)
  {
    return RUC_NOK;
  }
  if (setsockopt (socketId,IPPROTO_TCP,
                  TCP_KEEPINTVL,&INTVL,sizeof(int)) == -1)
  {
    return RUC_NOK;
  }
  if (setsockopt (socketId,IPPROTO_TCP,
                  TCP_KEEPCNT,&COUNT,sizeof(int)) == -1)
  {
    return RUC_NOK;
  }
#if 0
  int UMA_TCP_NODELAY = 1;
  if (setsockopt (socketId,IPPROTO_TCP,
                  TCP_NODELAY,&UMA_TCP_NODELAY,sizeof(int)) == -1)
  {
    perror("TCP_NODELAY");
    return RUC_NOK;
  }
#endif
#endif
#endif

  /*
  ** change sizeof the buffer of socket for sending
  */
  if (setsockopt (socketId,SOL_SOCKET,
                  SO_SNDBUF,&sockSndSize,sizeof(int)) == -1)
  {
    return RUC_NOK;
  }
  /*
  ** change sizeof the buffer of socket for receiving
  */
  if (setsockopt (socketId,SOL_SOCKET,
                  SO_RCVBUF,&sockRcvdSize,sizeof(int)) == -1)
  {
    return RUC_NOK;
  }



  /*
  ** change socket mode to asynchronous
  */
  if((fileflags=fcntl(socketId,F_GETFL,0))==-1)
  {
    return RUC_NOK;
  }
  if((fcntl(socketId,F_SETFL,fileflags|O_NDELAY))==-1)
  {
    return RUC_NOK;
  }

  return RUC_OK;

}

/*
**------------------------------------------------------
** uint32_t uma_tcp_disconnect( uma_tcp_t *p)
**-------------------------------------------------------
**  #SYNOPSIS
**  This is a private API that is called upon the detection
**  of the TCP disconnection. That function closes the
**  socket and disconnects from socket controller
**
**
**   IN:
**       p : context of the TCP connection
**
**   OUT :
**     none
**
**   In case of error, the calling module is responsible
**   for socket deletion (socket provided within the
**   input parameter)
**
** #ENDDOC
**-------------------------------------------------------
*/

void uma_tcp_disconnect( uma_tcp_t *p)
{

  /*
  **  close the socket
  */
  if (p->socketRef != -1)
  {
    /*
    ** there is no active connection
    */
    shutdown(p->socketRef,2);
    close(p->socketRef);
    p->socketRef = -1;
  }
  else
  {
    /*
    ** there is no active connection
    */
    RUC_WARNING(p->relcRef);
  }

  /*
  ** deconnection from the socketController
  */
  if (p->connectionId ==  NULL)
  {
    /*
    ** something wrong
    */
  }
  else
  {
    ruc_sockctl_disconnect(p->connectionId);
    p->connectionId =  NULL;

  }
  /*
  ** call the user that has a callback on disconnectioon
  */
//64BITS  (p->userDiscCallBack)(p->userRef,p->relcRef);
 (p->userDiscCallBack)(p->userRef,p->relcRef);



  return;
}


/*
**------------------------------------------------------
** uint32_t uma_tcp_endOfFsmDeletion( uma_tcp_t *p)
**-------------------------------------------------------
**  #SYNOPSIS
** that procedure is called as the response to the delete
** request when all the FSM resources have been released
**
**
**   IN:
**       p : context of the TCP connection
**
**   OUT :
**     none
**
**
** #ENDDOC
**-------------------------------------------------------
*/

void uma_tcp_endOfFsmDeletion( uma_tcp_t *p)
{
  /*
  **  close the socket
  */
  if (p->socketRef != -1)
  {
    /*
    ** should not occur
    */
    close(p->socketRef);
    p->socketRef = -1;
   RUC_WARNING(p->relcRef);
  }

  /*
  ** deconnection from the socketController
  */
  if (p->connectionId !=  NULL)
  {
    /*
    ** should not occur
    */
     ruc_sockctl_disconnect(p->connectionId);
     p->connectionId =  NULL;
     RUC_WARNING(p->relcRef);
  }

  /*
  ** remove it from the active list and insert it in the free list
  */
  ruc_objRemove((ruc_obj_desc_t*)p);
  ruc_objInsertTail((ruc_obj_desc_t*)uma_tcp_freeList,(ruc_obj_desc_t*)p);

  return;
}


/*----------------------------------------------
   uma_tcp_contextInit
**---------------------------------------------
** initialize a TCP connection context.
**
**  - reset of the transmitter FSM
**  - reset of the receiver FSM
**  - reset of the working variable
**
**  That function is call during the
** initialisation of the module only

**
**  IN : tcp context
**
**  OUT: none
**----------------------------------------------
*/

void uma_tcp_contextInit(uma_tcp_t *pObj)
{

  int            i;

     pObj->IPaddr = -1; /* XXXXX */

     /*
     **  initialisation of the transmit queue header
     */
     for (i=0; i< UMA_MAX_TCP_XMIT_PRIO; i++)
     {
        ruc_listHdrInit(&pObj->xmitList[i]);
     }
     pObj->socketRef= -1;
     pObj->connectionId =  NULL;

     /*
     **  receive variables
     */

     pObj->rcvBufRef = NULL;
     pObj->nbRead = 0;
     pObj->nbToRead = 0;
     pObj->sockDiscRcv = FALSE;
     pObj->stats.nbBufRcv = 0;
     pObj->stats.nbByteRcv = 0;
     pObj->msg_in = FALSE;
     pObj->header_rcv = FALSE;
     pObj->full_msg_rcv = FALSE;
     pObj->rcvLock = FALSE;
     pObj->rcvReset = FALSE;
     pObj->rcvRestart = FALSE;
     /*
     ** init of the default pool
     */
     pObj->xmitPoolRef = pObj->xmitPoolOrigin;
     pObj->rcvBufHead = pObj->rcvPoolOrigin;

     uma_tcp_eval_rcv_otherFlags(pObj);

     /*
     ** FDL : put code here for stream rcv
     **       automaton init
     */
     uma_tcp_rcvFsm_init(pObj,&pObj->rcvFsm);


     /*
     **  xmit variables
     */
     pObj->congested = FALSE;
     pObj->xmitPending = FALSE;
     pObj->transmitCredit = -1;
     pObj->timerRef = -1;
     pObj->timeCredit = -1;
     pObj->xmitBufRef = NULL;
     pObj->xmitBufRefCur = NULL;
     pObj->nbSent = 0;
     pObj->stats.nbBufXmit = 0;
     pObj->stats.nbByteXmit = 0;
     pObj->stats.nbCongested = 0;
     pObj->sockDiscXmit = FALSE;
     pObj->xmitErr = FALSE;
     pObj->xmitWouldBlock = FALSE;
     pObj->xmitDone = FALSE;
     pObj->xmitDead = FALSE;
     pObj->xmitReset = FALSE;
     pObj->xmitRestart = FALSE;

     pObj->xmitBufRefType =0;
     pObj->creditPrio0 =  RUC_RELC_XMIT_CREDIT_PRIO_0;
     pObj->creditPrio1 =  RUC_RELC_XMIT_CREDIT_PRIO_1;
     pObj->prio = 0;

     uma_tcp_eval_xmit_otherFlags(pObj);
     /*
     **  xmit FSM init
     */
     uma_tcp_xmitFsm_init(pObj,&pObj->xmitFsm);



     /*
     ** put some code for clearing the working variables
     */
     pObj->cnxName[0] = 0;
     pObj->integrity = 0;
//     pObj->moduleId = MODULE_ID_UPC;
//#warning moduleId obsolete
     pObj->headerSize = 0;
     pObj->msgLenOffset = 0;
     pObj->bufSize = 0;
     pObj->userRcvCallBack = (uma_tcp_recvCBK_t) uma_tcp_no_recvCBK;
     pObj->userDiscCallBack = (uma_tcp_discCBK_t) uma_tcp_no_discCBK;
     pObj->userRef = 0;
}


/*----------------------------------------------
   uma_tcp_contextCreate
**---------------------------------------------
** initialize a TCP connection context.
**
**  - reset of the transmitter FSM
**  - reset of the receiver FSM
**  - reset of the working variable
**
**  That function is called when a new TCP
** connection creation is in progress

**  IN : context of the TCP connection
**
**  OUT: none
**----------------------------------------------
*/

void uma_tcp_contextCreate(uma_tcp_t *pObj)
{


   pObj->IPaddr = -1;


   pObj->socketRef= -1;
   pObj->connectionId = NULL;
     /*
     ** FDL : put code here for stream xmit/rcv
     **       automaton init
     */

     /*
     **  receive variables
     */

     pObj->rcvBufRef = NULL;
     pObj->nbRead = 0;
     pObj->nbToRead = 0;
     pObj->sockDiscRcv = FALSE;
     pObj->stats.nbBufRcv = 0;
     pObj->stats.nbByteRcv = 0;
     pObj->msg_in = FALSE;
     pObj->header_rcv = FALSE;
     pObj->full_msg_rcv = FALSE;
     pObj->rcvLock = FALSE;
     pObj->rcvReset = FALSE;
     pObj->rcvRestart = FALSE;

     uma_tcp_eval_rcv_otherFlags(pObj);

     /*
     ** FDL : put code here for stream rcv
     **       automaton init
     */
     uma_tcp_rcvFsm_init(pObj,&pObj->rcvFsm);
     /*
     **  receive variables
     */
     pObj->congested = FALSE;
     pObj->xmitPending = FALSE;
     pObj->transmitCredit = -1;
     pObj->timerRef = -1;
     pObj->timeCredit = -1;
     pObj->xmitBufRef = NULL;
     pObj->xmitBufRefCur = NULL;
     pObj->nbSent = 0;
     pObj->stats.nbBufXmit = 0;
     pObj->stats.nbByteXmit = 0;
     pObj->stats.nbCongested = 0;
     pObj->sockDiscXmit = FALSE;
     pObj->xmitErr = FALSE;
     pObj->xmitWouldBlock = FALSE;
     pObj->xmitDone = FALSE;
     pObj->xmitDead = FALSE;
     pObj->xmitReset = FALSE;
     pObj->xmitRestart = FALSE;
     pObj->xmitBufRefType =0;
     pObj->creditPrio0 =  RUC_RELC_XMIT_CREDIT_PRIO_0;
     pObj->creditPrio1 =  RUC_RELC_XMIT_CREDIT_PRIO_1;

     uma_tcp_eval_xmit_otherFlags(pObj);
     /*
     **  xmit FSM init
     */
     uma_tcp_xmitFsm_init(pObj,&pObj->xmitFsm);

}

/*
**--------------------------------------
**    P U B L I C   F U N C T I O N S
**--------------------------------------
*/

/*----------------------------------------------
**
** uint32_t uma_tcp_init(uint32 nbElements)
**----------------------------------------------
**  #SYNOPSIS
**   That function allocates all the necessary
**   resources for UPPS and UPH TCP connections
**   management
**
**   IN:
**       nbElements : number of UPPS TCP connections
**                    supported
**
**
**   OUT :
**      RUC_NOK : error during the initialization
**      RUC_OK : module up and ready.
**
**----------------------------------------------
*/

uint32_t uma_tcp_init(uint32_t nbElements)
{
  uint32_t         ret = RUC_OK;
  uint32_t         curRef;
  ruc_obj_desc_t *pnext ;
  uma_tcp_t    *p;

  if (uma_tcpInitDone != FALSE)
  {
    return RUC_NOK;
  }
  while (1)
  {
    /*
    ** allocate the free connection distributor
    */
    uma_tcp_freeList = (uma_tcp_t*)ruc_listCreate(nbElements,sizeof(uma_tcp_t));
    if (uma_tcp_freeList == (uma_tcp_t*)NULL)
    {
      /*
      ** error on distributor creation
      */
      severe( "ruc_listCreate(%d,%d)", (int)nbElements,(int)sizeof(uma_tcp_t) );
      ret = RUC_NOK;
      break;
    }
    /*
    ** init of the active list
    */
    uma_tcp_activeList = (uma_tcp_t*)malloc(sizeof(uma_tcp_t));
    if (uma_tcp_activeList == (uma_tcp_t*)NULL)
    {
      /*
      ** out of memory
      */
      severe( "uma_tcp_activeList = malloc(%d)",(int) sizeof(uma_tcp_t) );
      ret = RUC_NOK;
      break;
    }
    ruc_listHdrInit((ruc_obj_desc_t*)uma_tcp_activeList);

    /*
    ** initialize each entry of the distributor
    */
    curRef = 0;
    pnext = (ruc_obj_desc_t*)NULL;
    while ((p = (uma_tcp_t*)ruc_objGetNext((ruc_obj_desc_t*)uma_tcp_freeList,
                                             &pnext))
               !=(uma_tcp_t*)NULL)
    {
      p->relcRef = curRef | (UMA_TCP_CTX_TYPE<<RUC_OBJ_SHIFT_OBJ_TYPE);
      p->xmitPoolRef = NULL;
      p->xmitBufPoolEmpty = TRUE;
      p->rcvBufHead =NULL;
      p->rcvBufPoolEmpty= TRUE;

      curRef++;
    }


    /*
    ** OK, now do the same loop, but allocate memory for
    ** the linked list and memory distributors (xmit/rcv)
    */
    pnext = (ruc_obj_desc_t*)NULL;
    while ((p = (uma_tcp_t*)ruc_objGetNext((ruc_obj_desc_t*)uma_tcp_freeList,
                                             &pnext))
               !=(uma_tcp_t*)NULL)
    {
      p->xmitPoolOrigin = ruc_buf_poolCreate(UMA_TCP_XMIT_BUFCOUNT,UMA_TCP_BUFSIZE);
      if (p->xmitPoolOrigin == NULL)
      {
         ret = RUC_NOK;
         severe( "xmit ruc_buf_poolCreate(%d,%d)", UMA_TCP_XMIT_BUFCOUNT, UMA_TCP_BUFSIZE );
         break;
      }
      p->rcvPoolOrigin = ruc_buf_poolCreate(UMA_TCP_RCV_BUFCOUNT,UMA_TCP_BUFSIZE);
      if (p->rcvPoolOrigin == NULL)
      {
         ret = RUC_NOK;
         severe( "rcv ruc_buf_poolCreate(%d,%d)", UMA_TCP_RCV_BUFCOUNT, UMA_TCP_BUFSIZE );
	 break;
      }
      uma_tcp_contextInit(p);
    }
    /*
    **  all is done
    */
    break;
  }
  /*
  ** check if there was an error during the initialization
  */
  if (ret != RUC_OK)
  {
    /*
    ** an error has been encountered: purge everything
    */
    uma_tcp_purge();
    return RUC_NOK;
  }
  /*     ruc_sockctl_disconnect(p->connectionId);
      p->connectionId = (uint32_t) NULL;
  ** everything is OK: marked it
  */
  uma_tcpInitDone = TRUE;
  return ret;
}




/*
**--------------------------------------------------------------------
** uma_tcp_create(uma_tcp_create_t *pconf)
**--------------------------------------------------------------------
**  #SYNOPSIS
**   that function allocate a free TCP context. It returns the reference
**   of the TCP object that has been allocated.
**
** That service is called during the creation of a TCP connection.
** It corresponds to the context allocation and initialisation.
**
**  There is no information about the socket and no connection is
**  performed with the socket controller.
**
**  These actions will take place later when the service
**  uma_tcp_createTcpConnection() is called
**
**  That service returns the reference of the TCP connection object.
**
**
**   IN:
**       pconf : pointer to the configuration array.
**
**   OUT :
**      -1 : error during creation
**      !=1 :reference if the TCP object.
**
**--------------------------------------------------------------------
*/

uint32_t uma_tcp_create(uma_tcp_create_t *pconf)
{
  uma_tcp_t *p;


  /*
  ** get the first free Relci context
  */
  p = (uma_tcp_t*)ruc_objGetFirst((ruc_obj_desc_t*)uma_tcp_freeList);
  if (p == (uma_tcp_t*)NULL)
  {
    /*
    ** out of free context
    */
    RUC_WARNING(pconf->IPaddr);
    return (uint32_t)-1;
  }
  /*
  ** set the xmit and receive pool to default
  */

   p->xmitPoolRef = p->xmitPoolOrigin;
   p->rcvBufHead = p->rcvPoolOrigin;

  /*
  ** clean the context
  */
  uma_tcp_contextCreate(p);
  /*
  ** register the Ip address
  */
  p->IPaddr = pconf->IPaddr;


  /*
  **   record the configuration parameters
  */
     /*
     **  FDL
     */

     p->headerSize = pconf->headerSize;
     p->msgLenOffset= pconf->msgLenOffset ;
     p->msgLenSize =pconf->msgLenSize ;
     p->bufSize = pconf->bufSize ;
     p->userRcvCallBack = pconf->userRcvCallBack ;
     p->userDiscCallBack = pconf->userDiscCallBack ;
     p->userRcvReadyCallBack = (ruc_pf_sock_t)NULL ;
     /*
     ** set the pool to the default pool reference
     */

     p->userRef = pconf->userRef ;
     p->socketRef = pconf->socketRef ;
  /*
  ** remove it from the free list and insert it in the active list
  */
  ruc_objRemove((ruc_obj_desc_t*)p);
  ruc_objInsertTail((ruc_obj_desc_t*)uma_tcp_activeList,(ruc_obj_desc_t*)p);

  return (uint32_t)p->relcRef;
}


/*
**--------------------------------------------------------------------
** uma_tcp_create_rcvRdy(uma_tcp_create_t *pconf)
**--------------------------------------------------------------------
**  #SYNOPSIS
**   that function allocate a free TCP context. It returns the reference
**   of the TCP object that has been allocated.
**
** That service is called during the creation of a TCP connection.
** It corresponds to the context allocation and initialisation.
**
**  There is no information about the socket and no connection is
**  performed with the socket controller.
**
**  These actions will take place later when the service
**  uma_tcp_createTcpConnection() is called
**
**  That service returns the reference of the TCP connection object.
**
**
**   IN:
**       pconf : pointer to the configuration array.
**
**   OUT :
**      -1 : error during creation
**      !=1 :reference if the TCP object.
**
**--------------------------------------------------------------------
*/

uint32_t uma_tcp_create_rcvRdy(uma_tcp_create_t *pconf)
{
  uma_tcp_t *p;


  /*
  ** get the first free Relci context
  */
  p = (uma_tcp_t*)ruc_objGetFirst((ruc_obj_desc_t*)uma_tcp_freeList);
  if (p == (uma_tcp_t*)NULL)
  {
    /*
    ** out of free context
    */
    RUC_WARNING(pconf->IPaddr);
    return (uint32_t)-1;
  }
  /*
  ** set the xmit and receive pool to default
  */

   p->xmitPoolRef = p->xmitPoolOrigin;
   p->rcvBufHead = p->rcvPoolOrigin;
  /*
  ** clean the context
  */
  uma_tcp_contextCreate(p);
  /*
  ** register the Ip address
  */
  p->IPaddr = pconf->IPaddr;


  /*
  **   record the configuration parameters
  */
     /*
     **  FDL
     */

     p->headerSize = pconf->headerSize;
     p->msgLenOffset= pconf->msgLenOffset ;
     p->msgLenSize =pconf->msgLenSize ;
     p->bufSize = pconf->bufSize ;
     p->userRcvCallBack = pconf->userRcvCallBack ;
     p->userDiscCallBack = pconf->userDiscCallBack ;
     p->userRcvReadyCallBack = pconf->userRcvReadyCallBack ;

     p->userRef = pconf->userRef ;
     p->socketRef = pconf->socketRef ;
  /*
  ** remove it from the free list and insert it in the active list
  */
  ruc_objRemove((ruc_obj_desc_t*)p);
  ruc_objInsertTail((ruc_obj_desc_t*)uma_tcp_activeList,(ruc_obj_desc_t*)p);

  return (uint32_t)p->relcRef;
}

/*
**--------------------------------------------------------------------
** uma_tcp_create_rcvRdy_bufPool(uma_tcp_create_t *pconf)
**--------------------------------------------------------------------
**  #SYNOPSIS
**   that function allocate a free TCP context. It returns the reference
**   of the TCP object that has been allocated.
**
** That service is called during the creation of a TCP connection.
** It corresponds to the context allocation and initialisation.
**
**  There is no information about the socket and no connection is
**  performed with the socket controller.
**
**  These actions will take place later when the service
**  uma_tcp_createTcpConnection() is called
**
** with that API, the user may provide optionally the following
** information
**   receiver ready callback (NULL implies default callback)
**   xmit buffer pool reference (-1 default pool)
**   recv buffer pool reference (-1 default pool)
**
**  That service returns the reference of the TCP connection object.
**
**
**   IN:
**       pconf : pointer to the configuration array.
**
**   OUT :
**      -1 : error during creation
**      !=1 :reference if the TCP object.
**
**--------------------------------------------------------------------
*/

uint32_t uma_tcp_create_rcvRdy_bufPool(uma_tcp_create_t *pconf)
{
  uma_tcp_t *p;


  /*
  ** get the first free Relci context
  */
  p = (uma_tcp_t*)ruc_objGetFirst((ruc_obj_desc_t*)uma_tcp_freeList);
  if (p == (uma_tcp_t*)NULL)
  {
    /*
    ** out of free context
    */
    RUC_WARNING(pconf->IPaddr);
    return (uint32_t)-1;
  }
  /*
  ** set the xmit and receive pool to default
  */

   p->xmitPoolRef = p->xmitPoolOrigin;
   p->rcvBufHead = p->rcvPoolOrigin;
  /*
  ** clean the context
  */
  uma_tcp_contextCreate(p);
  /*
  ** register the Ip address
  */
  p->IPaddr = pconf->IPaddr;


  /*
  **   record the configuration parameters
  */
     /*
     **  FDL
     */

     p->headerSize = pconf->headerSize;
     p->msgLenOffset= pconf->msgLenOffset ;
     p->msgLenSize =pconf->msgLenSize ;
     p->bufSize = pconf->bufSize ;
     p->userRcvCallBack = pconf->userRcvCallBack ;
     p->userDiscCallBack = pconf->userDiscCallBack ;
     p->userRcvReadyCallBack = pconf->userRcvReadyCallBack ;
     /*
     ** check what pools should be used
     */
     if (pconf->xmitPool != NULL)
     {
       p->xmitPoolRef = pconf->xmitPool;
     }

     if (pconf->recvPool != NULL)
     {
       p->rcvBufHead = pconf->recvPool;
     }

     p->userRef = pconf->userRef ;
     p->socketRef = pconf->socketRef ;
  /*
  ** remove it from the free list and insert it in the active list
  */
  ruc_objRemove((ruc_obj_desc_t*)p);
  ruc_objInsertTail((ruc_obj_desc_t*)uma_tcp_activeList,(ruc_obj_desc_t*)p);

  return (uint32_t)p->relcRef;
}


/*
***-------------------------------------------------------------------------
** uint32_t uma_tcp_deleteReq(uint32 tcpIdx)
**--------------------------------------------------------------------------
**  #SYNOPSIS
**   That service performs the deletion of a TCP connection. The input
** argument is the reference of the TCP connection that has been returned
**  during the creation time.
**
** That service asserts the disconnect event to each  FSM (xmit and receive).
**  Once all the resources have been released, then the socket is closed
**  and the connection with the socket controller is deleted.
**
**  If there is some pending xmit request or buffer each caller is warned
**  with an error code indicating that the TCP connection disconnection is
**  in progress
**
**   IN:
**       tcpIdx : reference of the TCP connection
**
**   OUT :
**         RUC_OK : success
**         RUC_NOK : failure
**                    - bad TCP connection reference
**----------------------------------------------------------------------------
*/

uint32_t uma_tcp_deleteReq(uint32_t tcpIdx)
{
  uma_tcp_t *p;

  /*
  **  Get the pointer to the relc Object
  */
  p = uma_tcp_getObjRef(tcpIdx);
  if (p == (uma_tcp_t*)NULL)
  {
    /*
    ** bad reference
    */
    return RUC_NOK;
  }
    uma_tcp_sockResetAssert(p);
    return RUC_OK;
}

/*
***-------------------------------------------------------------------------
** uint32_t uma_tcp_disconnectReq(uint32 tcpIdx)
**--------------------------------------------------------------------------
**  #SYNOPSIS
**   That service performs the deletion of a TCP connection. The input
** argument is the reference of the TCP connection that has been returned
**  during the creation time.
**
** That service asserts the disconnect event to each  FSM (xmit and receive).
**  Once all the resources have been released, then the socket is closed
**  and the connection with the socket controller is deleted.
**
**  If there is some pending xmit request or buffer each caller is warned
**  with an error code indicating that the TCP connection disconnection is
**  in progress
**
**   IN:
**       tcpIdx : reference of the TCP connection
**
**   OUT :
**         RUC_OK : success
**         RUC_NOK : failure
**                    - bad TCP connection reference
**----------------------------------------------------------------------------
*/

uint32_t uma_tcp_disconnectReq(uint32_t tcpIdx)
{
  uma_tcp_t *p;

  /*
  **  Get the pointer to the relc Object
  */
  p = uma_tcp_getObjRef(tcpIdx);
  if (p == (uma_tcp_t*)NULL)
  {
    /*
    ** bad reference
    */
    return RUC_NOK;
  }
  uma_tcp_sockDiscAssert(p);
  return RUC_OK;
}



/*--------------------------------------------------------------------------
**
**   uint32_t uma_tcp_createTcpConnection(uint32 tcpRef, char * name)
**--------------------------------------------------------------------------
**  #SYNOPSIS
**   The purpose of that function is to create the TCP connection
**   (mainly TCP socket configuration). It is assumed that the TCP
**   connection object has already been created when that service is called.
**
**   It finishes the tuning of the TCP socket and then performs the connection
**   with the socket controller. Once this has been done, the TCP connection
**   is up and running.
**
**  The actions are:
**     configure the socket as non-blocking
**     configure the xmit/receive buffer count
**     create the connection with the socket controller
**     initialize the automaton related to xmit/receive
**     and TCP connection supervision.
**
**
**   IN:
**       tcpIdx : reference of the Relci instance
**
**   OUT :
**      RUC_OK : TCP connection configured
**      RUC_NOK : error.
**
**   In case of error, the calling module is responsible
**   for socket deletion (socket provided within the
**   input parameter)
**
**--------------------------------------------------------------------------
*/
uint32_t uma_tcp_createTcpConnection(uint32_t tcpIdx, char * name)
{
  uma_tcp_t *p;
  uint32_t ret;


  /*
  **  Get the pointer to the relc Object
  */
  p = uma_tcp_getObjRef(tcpIdx);
  if (p == (uma_tcp_t*)NULL)
  {
    /*
    ** bad reference
    */
    RUC_WARNING(tcpIdx);
    return RUC_NOK;
  }

   /* Save the TCP connection name */
   memcpy (p->cnxName, name, UMA_CNX_NAME_MAX);
   p->cnxName[UMA_CNX_NAME_MAX-1] = 0;

  /*
  ** tune the socket
  */
  ret = uma_tcp_tuneTcpSocket(p->socketRef);
  if (ret != RUC_OK)
  {
    /*
    ** something wrong. Clear the socket reference in
    ** order to avoid the socket deletion at the time
    ** the context is released. For that service the deletion
    ** will take place in the function that performs the accept()
    */
    p->socketRef = -1;
    RUC_WARNING(tcpIdx);
    return RUC_NOK;
  }
  /*
  ** perform the connection with the socketController
  */
  //64BITS
  uint64_t val64 = (uint64_t)tcpIdx;
  p->connectionId = ruc_sockctl_connect(p->socketRef,
                                        name,
                                        3, //uma_tcp_sockNameAndPriorityTab[0].priority,
                                        (void*)val64,
                                        uma_tcp_sockNameAndPriorityTab[0].pCallBack);

  if (p->connectionId == NULL)
  {
    /*
    ** something wrong
    */
    p->socketRef = -1;
    RUC_WARNING(tcpIdx);
    return RUC_NOK;
  }

  /*
  ** all is fine: assert the tcp signal associated to the connection
  */
  uma_tcp_fsm_relc_tcpUpAssert(p);

  return RUC_OK;
}




/*------------------------------------------------------------------------
**
**   uint32_t uma_tcp_updateTcpConnection(uint32 tcpRef, int socketId, char * name)
**-------------------------------------------------------------------------
**  #SYNOPSIS
**   That service is called when the TCP connection object still
**   exist but the socket has been closed. A new socket has been
**   allocated, so that service is intended to perform the
**   socket tuning and to re-connect with the socket controller.
**
**  The actions are:
**     configure the socket as non-blocking
**     configure the xmit/receive buffer count
**     create the connection with the socket controller
**     initialize the automaton related to xmit/receive
**     and TCP connection supervision.
**
**  In case of success
**
**   IN:
**       tcpIdx : reference of the TCP connection object
**       socketId : socket identifier.
**
**   OUT :
**      RUC_OK : TCP connection configured
**      RUC_NOK : error.
**
**   In case of error, the calling module is responsible
**   for socket deletion (socket provided within the
**   input parameter)
**
**-------------------------------------------------------------------------
*/
uint32_t uma_tcp_updateTcpConnection(uint32_t tcpIdx,int socketId, char * name)
{
  uma_tcp_t *p;
  uint32_t ret;

  /*
  **  Get the pointer to the relc Object
  */
  p = uma_tcp_getObjRef(tcpIdx);
  if (p == (uma_tcp_t*)NULL)
  {
    /*
    ** bad reference
    */
    RUC_WARNING(tcpIdx);
    return RUC_NOK;
  }
  /*
  ** record the new socket ID and perform the connection with
  ** the socket controller
  */
  if (p->socketRef !=  -1)
  {
    /*
    ** there is no active connection
    */
    shutdown(p->socketRef,2);
    close(p->socketRef);
    p->socketRef = -1;

  }
  /*
  ** record the new socket
  */
  p->socketRef = socketId;
  /*
  ** tune the socket
  */
  ret = uma_tcp_tuneTcpSocket(p->socketRef);
  if (ret != RUC_OK)
  {
    /*
    ** something wrong. Clear the socket reference in
    ** order to avoid the socket deletion at the time
    ** the context is released. For that service the deletion
    ** will take place in the function that performs the accept()
    */
    p->socketRef = -1;
    RUC_WARNING(tcpIdx);
    return RUC_NOK;
  }
  if (p->connectionId != NULL)
  {
     /*
     **  that is not normal and should not occur
     */
      ruc_sockctl_disconnect(p->connectionId);
      p->connectionId =  NULL;
   }
  /*
  ** perform the connection with the socketController
  */
  //64BITS
  uint64_t val64 = (uint64_t)tcpIdx;
  p->connectionId = ruc_sockctl_connect(p->socketRef,
                                        name,
                                        3,//uma_tcp_sockNameAndPriorityTab[0].priority,
                                        (void*)val64,
                                        uma_tcp_sockNameAndPriorityTab[0].pCallBack);

  if (p->connectionId == NULL)
  {
    /*
    ** something wrong
    */
    p->socketRef = -1;
    RUC_WARNING(tcpIdx);
    return RUC_NOK;
  }

  /*
  ** all is fine: assert the tcp signal associated to the connection
  */
  uma_tcp_sockRestartAssert(p);   /*XXXXX */

  return RUC_OK;
}




/*
**-----------------------------------------------------------------
**
** uint32_t uma_tcp_sendSocket(uint32 tcpIdx,
**	                     uint32_t xmitBufRef,
**			     uint8_t prio)
**-----------------------------------------------------------------
**
**  #SYNOPSIS
**   The purpose of that function is to send a message on a TCP
**   connection. If the TCP connection is in the disconnected state
**   the buffer will be queued until the connection recovers of
**   the connection deletion.
**
**
**   IN:
**       tcpIdx : reference of the Relci instance
**       xmitBufRef : reference of the xmit buffer
**       prio : the priority queue to queue the message on
**              in case of congestion
**
**   OUT :
**      RUC_OK : submitted to the transmitter
**      RUC_NOK : bad tcpIdx, xmitref is released
**
**
**-----------------------------------------------------------------
*/
//64BITS uint32_t uma_tcp_sendSocket(uint32 tcpIdx,uint32 xmitBufRef,uint8_t prio)
uint32_t uma_tcp_sendSocket(uint32_t tcpIdx,void *xmitBufRef,uint8_t prio)
{
  uma_tcp_t *p;

  /*
  **  Get the pointer to the relc Object
  */
  p = uma_tcp_getObjRef(tcpIdx);
  if (p == (uma_tcp_t*)NULL)
  {
    /*
    ** bad reference, release the buffer
    */
    UMA_TCP_TRC("sendCharSocket_err",tcpIdx,-1,-1,-1);
    RUC_WARNING(tcpIdx);
    ruc_buf_freeBuffer(xmitBufRef);
    return RUC_NOK;
  }

  if (p->xmitBufRef !=  NULL)
  {
    /*
    **  automaton error
    */
    RUC_WARNING(tcpIdx);
    return RUC_NOK;
  }
  /*
  ** call the FSM
  */

  uma_tcp_xmitPendingAssert(p,xmitBufRef,prio);
  return RUC_OK;
}



/*
**-----------------------------------------------------------------
**
** uint32_t uma_tcp_sendSocketNoBuf(uint32 tcpIdx,
**	                          uma_xmit_assoc_t *pAssoc,
**			           uint8_t prio)
**-----------------------------------------------------------------
**
**  #SYNOPSIS
**   The purpose of that function is to send a message
**   on a TCP connection. That service is intended to be
**   used by application that does not own a transmit buffer
**
**   For that purpose the caller provides an association block
** in which it fills a callBack function that will be called
** by the transmitter when a buffer will become available.
**
       typedef struct _uma_xmit_assoc_t
       {
	 ruc_obj_desc_t            link;
	 uint32_t		    userRef;
	 uma_xmit_CBK_t            xmitCall;
       } uma_xmit_assoc_t;

**
**  The parameter of the callback are:
**     - a caller reference
**     - the reference of the allocated buffer
**     - a status (input): RUC_OK : the buffer is significant
**                         RUC_NOK : error at transmitter level
**
**  The application fills in information in the buffer
**  and then when the callback returns, the buffer is sent
**  by the TCP xmit FSM.
**
**   IN:
**       tcpIdx : reference of the Relci instance
**       pAssoc : reference of the association context
**       prio : the priority queue to queue the message on
**              in case of congestion
**
**   OUT :
**      RUC_OK : submitted to the transmitter
**      RUC_NOK : bad tcpIdx, xmitref is released
**
**
**-----------------------------------------------------------------
*/
uint32_t uma_tcp_sendSocketNoBuf(uint32_t tcpIdx,uma_xmit_assoc_t *pAssoc,uint8_t prio)
{
  uma_tcp_t *p;

  /*
  **  Get the pointer to the relc Object
  */
  p = uma_tcp_getObjRef(tcpIdx);
  if (p == (uma_tcp_t*)NULL)
  {
    /*
    ** bad reference
    */
    UMA_TCP_TRC("sendCharSocket_err",tcpIdx,-1,-1,-1);
    RUC_WARNING(tcpIdx);
    return RUC_NOK;
  }


  /*
  ** call the FSM
  */

  uma_tcp_xmitBufferReqPendingAssert(p,pAssoc,prio);
  return RUC_OK;
}

/*
**-----------------------------------------------------------------
**
**  uint32_t uma_tcp_SchangePrioXmitBuf(uint32 tcpIdx,
			              uint32_t xmitBufRef,
				      uint32_t prio)
**-----------------------------------------------------------------
**  #SYNOPSIS
**   The purpose of that function is to move a message pending
**   in one queue toward a new priority queue
**
**   IN:
**       tcpIdx : reference of the Relci instance
**       xmitBufRef : reference of the xmit buffer
**       prio : the priority queue to queue the message on
**              in case of congestion
**
**   OUT :
**      RUC_OK : submitted to the transmitter
**      RUC_NOK : bad tcpIdx, xmitref is released
**
**
**-----------------------------------------------------------------
*/
uint32_t uma_tcp_SchangePrioXmitBuf(uint32_t tcpIdx,
//64BITS				  uint32_t xmitBufRef,
				  void *xmitBufRef,
				  uint32_t prio)
{
  /*
  ** Remove the buffer from its current queue
  */
  ruc_objRemove((ruc_obj_desc_t*)xmitBufRef);
  return(uma_tcp_sendSocket(tcpIdx,xmitBufRef,prio));
}


/*
**-----------------------------------------------------------------
**
**  uint32_t uma_tcp_isReceivePoolEmpty(uint32 tcpIdx)

**-----------------------------------------------------------------
**  #SYNOPSIS
**   The purpose of that function is to move a message pending
**   in one queue toward a new priority queue
**
**   IN:
**       tcpIdx : reference of the Relci instance
**       xmitBufRef : reference of the xmit buffer
**       prio : the priority queue to queue the message on
**              in case of congestion
**
**   OUT :
**      TRUE : empty
**      FALSE: not empty
**
**
**-----------------------------------------------------------------
*/

uint32_t uma_tcp_isReceivePoolEmpty(uint32_t tcpIdx)
{
  uma_tcp_t *p;

  /*
  **  Get the pointer to the relc Object
  */
  p = uma_tcp_getObjRef(tcpIdx);
  if (p == (uma_tcp_t*)NULL)
  {
    /*
    ** bad reference, release the buffer
    */
    UMA_TCP_TRC("uma_tcp_isReceivePoolEmpty",tcpIdx,-1,-1,-1);
    RUC_WARNING(tcpIdx);
    return TRUE;
  }
 return(ruc_buf_isPoolEmpty(p->rcvBufHead));
}


/*
**-----------------------------------------------------------------
**
**  uint32_t uma_tcp_getBufferFromXmitPool(uint32 tcpIdx)
**-----------------------------------------------------------------
**  #SYNOPSIS
**   The purpose of that function is to allocate a buffer
**   from the Xmit pool of the TCP connection.
**  That function does not take care of the state of the
**  TCP connection.
**
**   IN:
**       tcpIdx : reference of the Relci instance

**
**   OUT :
**      != NULL : reference of the xmit buffer
**      == NULL out of buffer
**
**
**-----------------------------------------------------------------
*/
//64BITS uint32_t uma_tcp_getBufferFromXmitPool(uint32 tcpIdx)

void *uma_tcp_getBufferFromXmitPool(uint32_t tcpIdx)
{
  uma_tcp_t *p;
//64BITS  uint32_t     bufRef;
  void      *bufRef;

  /*
  **  Get the pointer to the Object
  */
  p = uma_tcp_getObjRef(tcpIdx);
  if (p == (uma_tcp_t*)NULL)
  {
    /*
    ** bad reference, release the buffer
    */
    RUC_WARNING(tcpIdx);
    return  NULL;
  }
   bufRef = ruc_buf_getBuffer(p->xmitPoolRef);
   if (bufRef == NULL)
   {
     /*
     **  out of xmit buffer, that 's not normal
     */
     return NULL;

   }
   return(bufRef);
}
/*
**-----------------------------------------------------------------
**
**  void uma_tcp_declareMonTxtCbk()
**-----------------------------------------------------------------
**  #SYNOPSIS
**   The purpose of that function is to declare the call back
**   of the application monitoring SWBB in order to trace TCP
**   connections
**   IN:
**
**   OUT :
**      != NULL : reference of the xmit buffer
**      == NULL out of buffer
**
**
**-----------------------------------------------------------------
*/
void uma_tcp_declareMonTxtCbk(uma_mon_txt_cbk cbk)
{
  uma_tcp_monTxtCbk = cbk;
}
/*
**-----------------------------------------------------------------
**
**  void uma_tcp_nullMonTxtCbk()
**-----------------------------------------------------------------
**  #SYNOPSIS
**   This the null callback for monitoring
**-----------------------------------------------------------------
*/
void uma_tcp_nullMonTxtCbk (char * topic, char *fmt, ... ) {}
