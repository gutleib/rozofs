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

 #ifndef AF_INET_STREAM_API_H
 #define AF_INET_STREAM_API_H
#include <stdio.h>
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
#include <string.h>

#include <rozofs/common/types.h>

#include "ruc_common.h"
#include "ruc_list.h"
#include "af_unix_socket_generic.h"
#include "af_unix_socket_generic_api.h"


/*
**__________________________________________________________________________
*
*    P R I V A T E    A P I
**__________________________________________________________________________
*/

/*
**__________________________________________________________________________
*/
/**
*
** Tune the configuration of the socket with:
**   - TCP KeepAlive,
**   - asynchrounous xmit/receive,
**   -  new sizeof  buffer for xmit/receive
**
**  IN : socketId
**
**  OUT: RUC_OK : success
**       RUC_NOK : error
*/
uint32_t af_inet_tcp_tuneTcpSocket(int socketId,int size);


int af_inet_sock_stream_client_create_internal(af_unix_ctx_generic_t *sock_p,int size);

/*
**__________________________________________________________________________
*
*    P U B L I C     A P I
**__________________________________________________________________________
*/
/*
**__________________________________________________________________________
*/
/**
   Creation of a AF_UNIX stream socket in non blocking mode

   see /proc/sys/net/core for socket parameters:
     - wmem_default: default xmit buffer size
     - wmem_max : max allocatable
     Changing the max is still possible with root privilege:
     either edit /etc/sysctl.conf (permanent) :
     (write):
     net.core.wmem_default = xxxx
     net.core.wmem_max = xxxx
     (read):
     net.core.rmem_default = xxxx
     net.core.rmem_max = xxxx

     or temporary with:
     echo <new_value> > /proc/sys/net/core/<your_param>

   @param nickname : name of socket for socket controller display name
   @param src_ipaddr_host : IP address in host format
   @param src_port_host : port in host format
   @param size: size in byte of the xmit buffer (the service double that value

    retval: !=0 reference of the created socket context
    retval < 0 : error on socket context creation

*/
int af_inet_sock_listening_create(char *nickname,
                                  uint32_t src_ipaddr_host,uint16_t src_port_host,
                                  af_unix_socket_conf_t *conf_p);

int af_inet_sock_listening_create_th(char *nickname,
                                  uint32_t src_ipaddr_host,uint16_t src_port_host,
                                  af_unix_socket_conf_t *conf_p);

/*
**__________________________________________________________________________
*/
/**
   Creation of a AF_INET socket stream  in non blocking mode

   see /proc/sys/net/core for socket parameters:
     - wmem_default: default xmit buffer size
     - wmem_max : max allocatable
     Changing the max is still possible with root privilege:
     either edit /etc/sysctl.conf (permanent) :
     (write):
     net.core.wmem_default = xxxx
     net.core.wmem_max = xxxx
     (read):
     net.core.rmem_default = xxxx
     net.core.rmem_max = xxxx

     or temporary with:
     echo <new_value> > /proc/sys/net/core/<your_param>

     The bind on the source is only done if the source IP address address is different from INADDR_ANY

   @param nickname : name of socket for socket controller display name
   @param src_ipaddr_host : IP address in host format
   @param src_port_host : port in host format
   @param remote_ipaddr_host : IP address in host format
   @param remote_port_host : port in host format
   @param size: size in byte of the xmit buffer (the service double that value

    retval: !=0 reference of the created socket context
    retval < 0 : error on socket context creation

*/
int af_inet_sock_client_create(char *nickname,
                                uint32_t src_ipaddr_host,uint16_t src_port_host,
                                uint32_t remote_ipaddr_host,uint16_t remote_port_host,
                                af_unix_socket_conf_t *conf_p);

int af_inet_sock_client_create_th(char *nickname,
                                uint32_t src_ipaddr_host,uint16_t src_port_host,
                                uint32_t remote_ipaddr_host,uint16_t remote_port_host,
                                af_unix_socket_conf_t *conf_p);
/*
**__________________________________________________________________________
*/
/**
   Modify the destination port of a client AF_INET socket
   
   @param sockRef : socket reference 
   @param remote_port_host : port in host format

    retval: 0 when done
    retval < 0 :if socket do not exist

*/
int af_inet_sock_client_modify_destination_port(int sockRef, uint16_t remote_port_host);
#endif
