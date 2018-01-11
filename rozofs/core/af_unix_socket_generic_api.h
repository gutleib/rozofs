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


#ifndef AF_UNIX_SOCKET_GENERIC_API_H
#define AF_UNIX_SOCKET_GENERIC_API_H
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
#include "af_unix_socket_generic.h"


/*
**________________________________________________________________________________
**________________________________________________________________________________
**
**     C O M M O N   A P I
**
**________________________________________________________________________________
**________________________________________________________________________________
*/
/*
**__________________________________________________________________________
*/
/**
* delete a socket context

 @param socket_ctx_id : reference of the socket context

 @retval 0 : success
 @retval < 0 : error
 */
 int af_unix_delete_socket(uint32_t socket_ctx_id);

/*
**__________________________________________________________________________
*/
/**
* Perform a disconnection from the socket controller:

 the socket is also closed if that one is still reference in
 the socket context

 @param socket_ctx_id : reference of the socket context

 @retval 0 : success
 @retval < 0 : error
 */
 int af_unix_disconnect_from_socketCtrl(uint32_t socket_ctx_id);

/*
**____________________________________________________
*/
/**
* allocate a xmit_buffer from the default AF_UNIX pool
*
 @param none

 @retval <> NULL address of the xmit buffer
 @retval == NULL out of buffer
*/
void *af_unix_alloc_xmit_buf();

/*
**____________________________________________________
*/
/**
* allocate a receive_buffer from the default AF_UNIX pool
*
 @param none

 @retval <> NULL address of the xmit buffer
 @retval == NULL out of buffer
*/
void *af_unix_alloc_recv_buf();


/*
**____________________________________________________
*/
/**
   af_unix_module_init

  create the AF UNIX context pool

@param     : af_unix_ctx_count : number of Transaction context
@retval   : RUC_OK : done
@retval          RUC_NOK : out of memory
*/

uint32_t af_unix_module_init(uint32_t af_unix_ctx_count,
                           int    max_xmit_buf_count,int max_xmit_buf_size,
                           int    max_recv_buf_count,int max_recv_buf_size
                            );
/*
**________________________________________________________________________________
**________________________________________________________________________________
**
**      N O N C O N N E C T E D    M O D E   A  P  I  (DATAGRAM)
**
**________________________________________________________________________________
**________________________________________________________________________________
*/

/*
**__________________________________________________________________________
*/
/**
   Creation of a AF_UNIX socket Datagram  in non blocking mode

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

   @param name0fSocket : name of the AF_UNIX socket
   @param size: size in byte of the xmit buffer (the service double that value

    retval: >0 reference of the created AF_UNIX socket
    retval < 0 : error on socket creation

*/
int af_unix_sock_create(char *nameOfSocket,af_unix_socket_conf_t *conf_p);



/**
*  Create a bunch of AF_UNIX socket associated with a Family

 @param  basename_p : Base name of the family
 @param  base_instance: index of the first instance
 @param  nb_instances: number of instance in the family
 @param  socket_tb_p : pointer to an array were the socket references will be stored
 @param  xmit_size : size for the sending buffer (SO_SNDBUF parameter)

 @retval: 0 success, all the socket have been created
 @retval < 0 error on at least one socket creation
*/
int af_unix_socket_family_create (char *basename_p, int base_instance,int nb_instances,
                                  int *socket_tb_p,af_unix_socket_conf_t *conf_p);





/*
**________________________________________________________________________________
**________________________________________________________________________________
**
**      C O N N E C T E D    M O D E   A  P  I  (STREAM)
**
**________________________________________________________________________________
**________________________________________________________________________________
*/


int af_unix_generic_stream_send(af_unix_ctx_generic_t *this,void *buf_p);
int af_unix_generic_send_stream_with_idx(int  socket_ctx_id,void *buf_p);

/*
**________________________________________________________________________________
**
**      C L I E N T  SIDE
**
**________________________________________________________________________________
*/
/*
**__________________________________________________________________________
*/
/**
   Creation of a AF_UNIX socket Datagram  in non blocking mode

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
   @param remote_sun_path : name of remote  the AF_UNIX socket
   @param size: size in byte of the xmit buffer (the service double that value

    retval: !=0 reference of the created socket context
    retval < 0 : error on socket context creation

*/
int af_unix_sock_client_create(char *nickname,char *remote_sun_path,af_unix_socket_conf_t *conf_p);
/**
*  Create a bunch of AF_UNIX socket associated with a Family

 @param  nicknamebase_p : Base name of the family
 @param  basename_p : Base name of the remote sunpath
 @param  base_instance: index of the first instance
 @param  nb_instances: number of instance in the family
 @param  socket_tb_p : pointer to an array were the socket references will be stored
 @param  xmit_size : size for the sending buffer (SO_SNDBUF parameter)

 @retval: 0 success, all the socket have been created
 @retval < 0 error on at least one socket creation
*/
int af_unix_socket_client_family_create (char *nicknamebase_p, char *basename_p, int base_instance,int nb_instances,
                                         int *socket_ctx_tb_p,af_unix_socket_conf_t *conf_p);

/*
**__________________________________________________________________________
*/
/**
   Attempt to reconnect to the server

   In case of failure, it is up to the application to call the service
   for releasing the socket context and the associated resources
   Cleaning the socket context will automatically close the socket and
   remove the binding with the socket controller.
   It also release any recv/xmit buffer attached with the socket context.


   @param af_unix_ctx_id : reference tf the socket context

    retval: !=0 reference of the created socket context
    retval < 0 : error on reconnect

*/
int af_unix_sock_client_reconnect(uint32_t af_unix_ctx_id);

/*
**________________________________________________________________________________
**
**     S E R V E R  CREATION
**
**________________________________________________________________________________
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
   @param remote_sun_path : name of remote  the AF_UNIX socket
   @param size: size in byte of the xmit buffer (the service double that value

    retval: !=0 reference of the created socket context
    retval < 0 : error on socket context creation

*/
int af_unix_sock_listening_create(char *nickname,char *remote_sun_path,af_unix_socket_conf_t *conf_p);

int af_unix_socket_listening_family_create (char *nicknamebase_p,char *basename_p, int base_instance,int nb_instances,
                                             int *socket_ctx_tb_p,af_unix_socket_conf_t *conf_p);

void af_unix_info_getsockopt(int fd,char *file,int line);

/*
**__________________________________________________________________________
*/
/**
*  Set the DSCP value for a TCP connection

   @param fd: reference of the socket
   @param dscp: TOS value
*/
void af_inet_sock_set_dscp(int fd,uint8_t dscp);
/*
**__________________________________________________________________________
*/
/**
*  Set the DSCP value for a TCP connection

   @param fd: reference of the socket
   
   @retval dscp: TOS value or 0xff if error
*/
int af_inet_sock_get_dscp(int fd);

/*
**__________________________________________________________________________
*/
typedef struct _af_inet_connection_info_t
{
   uint32_t src_ip;
   uint32_t dst_ip;
   uint16_t src_port;
   uint16_t dst_port;
} af_inet_connection_info_t;

/**
*  Get the source/destination IP addresses and ports of a connection

  @param cnx_idx: connection index  
  @param p: pointer to the array when connection information are returned
  
  @retval 0 on success
  @eetval -1 on error
*/
int af_inet_get_connection_info(uint32_t cnx_idx,af_inet_connection_info_t *p);
/*
**__________________________________________________________________________
*/
/**
*   Set the user reference of a TCP connection context

   @param cnx_idx: connection index  
   @param userRef: pointer to an opaque value
   
  @retval 0 on success
  @eetval -1 on error
*/
int af_inet_set_connection_user_ref(uint32_t cnx_idx,void *userRef);

/*
**__________________________________________________________________________
*/
/**
*   Set the user reference of a TCP connection context

   @param cnx_idx: connection index  
   @param userRef: pointer to an opaque value
   
  @retval pointer to the userRef or NULL
*/
void *af_inet_get_connection_user_ref(uint32_t cnx_idx);    
#endif
