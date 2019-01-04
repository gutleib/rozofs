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

/**
   That source is intended to process the RPC Read & Write messages
   that has been sent from the storcli in RDMA mode
   
*/

#ifndef STORIO_RDMA_RECV_H
#define STORIO_RDMA_RECV_H
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <errno.h>
#include <sched.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/profile.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/rozofs_rpc_non_blocking_generic_srv.h>
 
uint32_t rdma_rpc_cmd_rcvReadysock(void * unused,int socketId);
uint32_t rdma_rpc_cmd_rcvMsgsock(void * unused,int socketId);
uint32_t rdma_rpc_cmd_xmitReadysock(void * unused,int socketId);
uint32_t rdma_rpc_cmd_xmitEvtsock(void * unused,int socketId);
 /*
**__________________________________________________________________________
*/
/**
*   Call back used by the completion queue thread for post a rdma received
    from the completion queue
    That function is intended to refill also the share received queue with a ruc_buffer
    In case the pool is empty this will be done at the time the processing of the request
    will end.
    
    @param opcode: RDMA opcode
    @param ruc_buf: pointer to the ruc_buffer that contains the rpc message
    @param qp_num: reference of the queue pair on which the message has been received
    @param status: 0 for success -1 for error
    @param error: rdma error code when the status is -1 , 0 otherwise
    
    
    @retval none
*/

void storio_rdma_read_write_req_post_to_main_thread_cbk(uint32_t opcode,void *ruc_buf, uint32_t qp_num,int status,int error);

/*__________________________________________________________________________
* Initialize the rdma READ/WRITE thread interface
*
* @param hostname    storio hostname (for tests)

*
*  @retval 0 on success -1 in case of error
*/
int storio_rdma_rpc_cmd_intf_create(char * hostname, int instance_id);
  


#endif
