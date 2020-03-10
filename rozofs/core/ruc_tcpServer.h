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
/*
** I N C L U D E S
*/


#ifndef RUC_TCP_SERVER_H
#define RUC_TCP_SERVER_H


#include <rozofs/common/types.h>

#include "ruc_common.h"
#include "ruc_sockCtl_api.h"
#include "ruc_sockCtl_api_th.h"
#include "ruc_tcpServer_api.h"
/*
**  backlog for listen : 5 connection awaiting
*/
#define RUC_TCP_SERVER_BACKLOG 5





typedef struct _ruc_tcp_server_t
{
  ruc_obj_desc_t            link;          /* To be able to chain the MS context on any list */
  uint32_t                    ref;         /* Index of the context*/
  uint8_t                     cnxName[RUC_TCP_SERVER_NAME];       /* ascii pattern */
  uint32_t                    integrity;     /* the value of this field is incremented at
					      each MS ctx allocation */
  ruc_tcp_server_recvCBK_t  accept_CBK;
  uint32_t                    userRef;
  int			    socketId;
  uint16_t                    tcpPort;
  uint8_t                     priority;
  uint8_t                     filler;
  uint32_t                    ipAddr;
//64BITS  uint32_t		    connectId;  /* socket controller reference */
  void*                     connectId;  /* socket controller reference */
 } ruc_tcp_server_t;

/*
**  Prototypes
*/
/*
**  PRIVATE   SERVICES
*/
ruc_tcp_server_t *ruc_tcp_server_getObjRef(uint32_t tcpRef);
uint32_t ruc_tcp_server_createSocket_retry(uint16_t tcpPort,uint32_t ipAddr, int retry);
#define ruc_tcp_server_createSocket(tcpPort,ipAddr) ruc_tcp_server_createSocket_retry(tcpPort,ipAddr, 0);

// 64BITS uint32_t ruc_tcp_server_acceptIn(uint32 objRef,int socketId);
// 64BITS uint32_t ruc_tcp_server_isRcvReady(uint32 ref,int socketId);
// 64BITS uint32_t ruc_tcp_server_isXmitReady(uint32 ref,int socketId);
// 64BITS uint32_t ruc_tcp_server_xmitEvent(uint32 ref,int socketId);


uint32_t ruc_tcp_server_acceptIn(void *objRef,int socketId);
uint32_t ruc_tcp_server_isRcvReady(void * ref,int socketId);
uint32_t ruc_tcp_server_isXmitReady(void * ref,int socketId);
uint32_t ruc_tcp_server_xmitEvent(void * ref,int socketId);
#endif
