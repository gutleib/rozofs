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




#ifndef RUC_TCP_SERVER_API_H
#define RUC_TCP_SERVER_API_H


#include <rozofs/common/types.h>

#include "ruc_common.h"
#include "ruc_sockCtl_api.h"
#include "ruc_sockCtl_api_th.h"


/*
**  Obj reference for socket controller connection
*/

#define RUC_TCP_SERVER_NAME 32




/*----------------------------------------------
**
** uint32_t ruc_tcp_server_init(uint32 nbElements)
**----------------------------------------------
**  #SYNOPSIS
**   That service allocates resources in order
**   to handle nbElements of server connection.
**
**  That service must be called once only during the
**  application startup.
**
**   IN:
**       nbElements : number of server connection supported
**
**
**   OUT :
**      RUC_NOK : error during the initialization
**      RUC_OK : module up and ready.
**
**----------------------------------------------
*/

uint32_t ruc_tcp_server_init(uint32_t nbElements);


/*--------------------------------------------------------------------
**
** uint32_t ruc_tcp_server_connect(ruc_tcp_server_connect_t *pconnect)
**--------------------------------------------------------------------
**  #SYNOPSIS
**   That function performs the creation of a server port. The caller
**  should provide the port and the IPaddr. The connection can be
**  performed for any input interface (ANY_INADDR).
**
** On success, the reference of the server connection is returned.
**
**  The caller provides a callback for accept(). When a new connection
**  is received, then the TCP server connection handler, calls that
**  callback with the following arguments:
**
**     - user reference given in the connection block;
**     - new socket identifier
**     - address and port of the initiator
**
**  The application may accept or reject the new connection. If the
** connection is accepted, then RUC_OK must returned, otherwise
** RUC_NOK is returned.
**
**  note: there is no need for socket deletion in case of reject, this
**  is doen by the TCP server handler.
**
**  The following action are performed:
**   - creation of the socket
**   - do the listen
**   - do the connection with the socket controller
**
**  the caller should provide the following information:
**    - name of the connection (16 max)
**    - callback for accept service
**    - IP address (host format)
**    - TCP port host format)
**    - userRef (for accept callback)
**
**   IN:
**      pconnect: pointer to the connection info block
**
**
**   OUT :
**      -1: error
**      !=-1: reference of the connection block
**
**----------------------------------------------
*/


/*
** call back types
*/

typedef uint32_t (*ruc_tcp_server_recvCBK_t)(uint32_t userRef,
					  int socketId,
					  struct sockaddr * sockAddr);

typedef struct _ruc_tcp_server_connect_t
{
  uint8_t                     cnxName[RUC_TCP_SERVER_NAME];       /* ascii pattern */
  ruc_tcp_server_recvCBK_t  accept_CBK;
  uint32_t                    userRef;
  uint16_t                    tcpPort;
  uint8_t                     priority;
  uint32_t                    ipAddr;
 } ruc_tcp_server_connect_t;

uint32_t ruc_tcp_server_connect(ruc_tcp_server_connect_t *pconnect);
/*--------------------------------------------------------------------
**
** uint32_t ruc_tcp_server_disconnect(uint32 cnxRef)
**--------------------------------------------------------------------
**  #SYNOPSIS
**   That function performs the disconnection of a TCP server
**
**   IN:
**      cnxRef: reference returned on the ruc_tcp_server_connect()
**
**   OUT : RUC_OK/RUC_NOK
**
**----------------------------------------------
*/
uint32_t ruc_tcp_server_disconnect (uint32_t cnxRef);

#endif
