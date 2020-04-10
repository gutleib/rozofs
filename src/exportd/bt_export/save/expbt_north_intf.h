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
 
#ifndef EXP_BT_NORTH_INTF_H
#define EXP_BT_NORTH_INTF_H
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h> 
#include <sys/socket.h>        
#include <errno.h>  
#include <rozofs/rozofs.h>
#include <rozofs/core/rozofs_queue.h>
#include <pthread.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/core/ruc_buffer_api.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/ruc_buffer_debug.h>
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/core/rozofs_socket_family.h>


/*
**____________________________________________________
*/
/**
   expbt_north_interface_init

  create the Transaction context pool

@param     : cmd_buf_count : number of read/write buffer
@param host : IP address in dot notation or hostname
@param port:    listening port value


@retval   : RUC_OK : done
@retval          RUC_NOK : out of memory
*/

int expbt_north_interface_init(char *host,uint16_t port,int cmd_buf_count);


#endif
