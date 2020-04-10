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
 
#ifndef EXP_BT_GLOBAL_H
#define EXP_BT_GLOBAL_H
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h> 
#include <sys/socket.h>        
#include <errno.h>  
#include <rozofs/rozofs.h>
#include <rozofs/core/rozofs_queue.h>

/**
* Buffers information
*/
extern int expbt_cmd_buf_count;   /**< number of buffer allocated for read/write on north interface */
extern int expbt_cmd_buf_sz;      /**<read:write buffer size on north interface */

extern void *expbt_buffer_pool_p ;  /**< reference of the read/write buffer pool */
extern uint64_t tcp_receive_depletion_count ;
extern int       expbt_recv_sockpair[];       /**< index 0 is used by the RDMA signaling thread, index 1 is used by the client */
rozofs_queue_t expbt_trk_cmd_ring;

extern void * decoded_rpc_buffer_pool;
extern int    decoded_rpc_buffer_size;
extern int    af_unix_trk_pending_req_count; /**< number of simultaneous requests */


/*
** Expbt profiler
*/

typedef struct  _expbt_profiler_t {
	uint64_t uptime;
	uint64_t now;
	uint8_t vers[20];
	uint64_t file_read[3];
	uint64_t file_check[3];
	uint64_t load_dentry[3];
	uint64_t poll[3];

} expbt_profiler_t;

#define EXPBT_START_PROFILING(the_probe)\
    {\
        gprofiler->the_probe[P_COUNT]++;\
    }

#endif
