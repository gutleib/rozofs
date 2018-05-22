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
#ifndef UMA_TCP_MAIN_API_H
#define UMA_TCP_MAIN_API_H

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

#include "ruc_common.h"
#include "ruc_list.h"
#include "ruc_sockCtl_api.h"
#include "ruc_buffer_api.h"
#include "ruc_timer_api.h"
#include "uma_fsm_framework.h"

/*
** call back types
*/

//64BITS typedef void (*uma_tcp_recvCBK_t)(uint32_t userRef,uint32 tcpCnxRef,uint32 bufRef);
//64BITS typedef void (*uma_tcp_discCBK_t)(uint32_t userRef,uint32 tcpRef);
//64BITS typedef void (*uma_xmit_CBK_t)(uint32_t userRef,uint32 buffer,uint8_t status);

typedef void (*uma_tcp_recvCBK_t)(void *userRef,uint32_t tcpCnxRef,void *bufRef);
typedef void (*uma_tcp_discCBK_t)(void *userRef,uint32_t tcpRef);
typedef void (*uma_xmit_CBK_t)(uint32_t userRef,void * buffer,uint8_t status);


typedef void (*uma_mon_txt_cbk) (char * topic, char *fmt, ... ) ;



/*
**  P U B L I C    A P I
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

uint32_t uma_tcp_init(uint32_t nbElements);




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
typedef struct _uma_tcp_create_t
{
  uint32_t        headerSize;       /* size of the header to read                 */
  uint32_t        msgLenOffset;     /* offset where the message length fits       */
  uint32_t        msgLenSize;       /* size of the message length field in bytes  */
  uint32_t        bufSize;         /* length of buffer (xmit and received)        */
  uma_tcp_recvCBK_t   userRcvCallBack;   /* callback provided by the connection owner block */
  uma_tcp_discCBK_t   userDiscCallBack; /* callBack for TCP disconnection detection         */
  ruc_pf_sock_t       userRcvReadyCallBack; /* NULL for default callback                    */
//64BITS  uint32_t        userRef;           /* user reference that must be recalled in the callbacks */
  void          *userRef;           /* user reference that must be recalled in the callbacks */
  int           socketRef;
  uint32_t        IPaddr;
//64BITS  uint32_t        xmitPool; /* user pool reference or -1 */
//64BITS  uint32_t        recvPool; /* user pool reference or -1 */
  void        *xmitPool; /* user pool reference or -1 */
  void        *recvPool; /* user pool reference or -1 */
} uma_tcp_create_t;

uint32_t uma_tcp_create(uma_tcp_create_t *pconf);

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
** note/in that case the userRcvReadyCallBack field must be
**      significant
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

uint32_t uma_tcp_create_rcvRdy(uma_tcp_create_t *pconf);


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

uint32_t uma_tcp_create_rcvRdy_bufPool(uma_tcp_create_t *pconf);
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
**-------------------------------------------------------------------------
*/

uint32_t uma_tcp_deleteReq(uint32_t tcpIdx);
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

uint32_t uma_tcp_disconnectReq(uint32_t tcpIdx);

/*--------------------------------------------------------------------------
**
**   uint32_t uma_tcp_createTcpConnection(uint32 tcpRef)
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
uint32_t uma_tcp_createTcpConnection(uint32_t tcpIdx, char * name);


/*------------------------------------------------------------------------
**
**   uint32_t uma_tcp_updateTcpConnection(uint32 tcpRef, int socketId)
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
uint32_t uma_tcp_updateTcpConnection(uint32_t tcpIdx,int socketId, char * name);



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
// 64BITS uint32_t uma_tcp_sendSocket(uint32 tcpIdx,uint32 xmitBufRef,uint8_t prio);
uint32_t uma_tcp_sendSocket(uint32_t tcpIdx,void *xmitBufRef,uint8_t prio);



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
/*
**  association block for xmit call back
*/
typedef struct _uma_xmit_assoc_t
{
  ruc_obj_desc_t            link;
  uint32_t		    userRef;
  uma_xmit_CBK_t            xmitCall;   /* xmit call back when buffer is ready */
} uma_xmit_assoc_t;


uint32_t uma_tcp_sendSocketNoBuf(uint32_t tcpIdx, uma_xmit_assoc_t *pAssoc, uint8_t prio);



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
				  uint32_t prio);


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

uint32_t uma_tcp_isReceivePoolEmpty(uint32_t tcpIdx);


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

//64BITS uint32_t uma_tcp_getBufferFromXmitPool(uint32 tcpIdx);
void  *uma_tcp_getBufferFromXmitPool(uint32_t tcpIdx);

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
void uma_tcp_declareMonTxtCbk(uma_mon_txt_cbk cbk);
#endif
