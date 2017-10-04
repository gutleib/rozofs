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
 #ifndef RDMA_CLIENT_SEND_H
 #define RDMA_CLIENT_SEND_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
 
 typedef struct _rozofs_rmda_cli_stats_t
 {
    uint64_t attempts;        /**< number of RDMA transfer attempts                   */
    uint64_t no_TCP_context;   /**< number of time there was no available TCP context */
    uint64_t no_RDMA_context;  /**< no RDMA context available                         */
    uint64_t bad_RDMA_state;   /**< RDMA current state does not permit to use it      */
    uint64_t no_RDMA_memory;   /**< memory region is not found                        */
    uint64_t no_tx_context;    /**< out of transaction context                        */
    uint64_t no_buffer;        /**< cannot allocate a south buffer                    */
    uint64_t lbg_send_ok;      /**< successfully submitted to lbg                     */
    uint64_t lbg_send_nok;     /**< error in lbg send                                 */
} rozofs_rmda_cli_stats_t;
 
/*
** Per lbg statistics
*/
typedef struct _rozofs_rdma_cli_lbg_stats_t{
  int rdma_tmo_buffer_count;           /**< number of buffer that are waiting for out of seq or TCP disconnect  */
  rozofs_rmda_cli_stats_t read_stats;  /**< read statistics  */
  rozofs_rmda_cli_stats_t write_stats; /**< write statistics  */
} rozofs_rdma_cli_lbg_stats_t;
/*
**__________________________________________________________________________
*/
/**
*   Init of the table used to deal with time-out/out of sequence and TCP disconnection
    on LBG that are using RDMA
    
    @param mx_lbg_north_ctx:  number of context to handle
    
    @retval RUC_OK on success
*/
int  rdma_lbg_tmo_table_init(uint32_t mx_lbg_north_ctx);
 
 /*
**__________________________________________________________________________
*/
/**
*  RDMA Time-out processing

  Because the buffer provided for the RDMA cannot be directly free for future re-use since
  a delayed RDMA transfer can occur while the buffer has been allocated for another transaction and
  could trigger some data corruption, the buffer used for RDMA transfer should be locked until 
  either we got a late response from storio (TX_OUT_OF_SEQ) or a TCP disconnect event from the TCP layer
  
  So the goal of that function is :
  1) to allocate a data structure to deal with defer buffer release
  2) to increment the in_use counter of the buffer (to avoid release during 
     the release of the storcli context used for the transfer) 
  3) to save the reference of the transaction in that alllocated context: needed to deal with out of seq TX)
  4) to ;queue the data structure at the index that corresponds to the LBG on which it has been sent
  5) to remove the reference of the rdma buffer from the transaction context
  
*/
void rozofs_rdma_tx_tmo_proc(void *this);
/*
**__________________________________________________________________________
*/
/**
*  RDMA : OUT of sequence processing

  That function is called upon detecting an out of sequence transaction at the tx_engine side
  The Out of sequence might not be related to a RDMA transfer.
  
  @param lbg_id : reference of the load balancing group for which out of seq transaction is detected
  @param xid: transaction identifier found in the received buffer

  @retval : none
*/
void rozofs_rdma_tx_out_of_seq_cbk(uint32_t lbg_id,uint32_t xid);
/*
**__________________________________________________________________________
*/
/**
*  RDMA : TCP disconnect callback processing

  That function is called upon the disconnection of the TCP connection that is used in conjunction with RDMA
  
  @param lbg_id : reference of the load balancing group for which the TCP connection is down

  @retval : none
*/
void rozofs_rdma_tcp_connection_disc_cbk(uint32_t lbg_id);
/*
**__________________________________________________________________________
*/
/**
   SP_READ_RDMA encoding

    @param lbg_id             : reference of the load balancing group
    @param socket_context_ref : socket controller reference (index) of the TCP connection for which there is an associated RDMA connection
    @param timeout_sec        : transaction timeout
    @param prog               : program
    @param vers               : program version
    @param opcode             : metadata opcode
    @param encode_fct         : encoding function
    @msg2encode_p             : pointer to the message to encode
    @param xmit_buf           : pointer to the buffer to send, in case of error that function release the buffer
    @param seqnum             : sequence number associated with the context (store as an opaque parameter in the transaction context
    @param opaque_value_idx1  : opaque value at index 1
    @param extra_len          : extra length to add after encoding RPC (must be 4 bytes aligned !!!)
    @param recv_cbk           : receive callback function

    @param user_ctx_p : pointer to the working context
   
    @retval 0 on success;
    @retval -1 on error,, errno contains the cause
    @retval -2 if non RDMA path should be used
*/
int rozofs_sorcli_sp_read_rdma(uint32_t lbg_id,uint32_t socket_context_ref, uint32_t timeout_sec, uint32_t prog,uint32_t vers,
                              int opcode,xdrproc_t encode_fct,void *msg2encode_p,
                              void *xmit_buf,
                              uint32_t seqnum,
                              uint32_t opaque_value_idx1,  
                              int      extra_len,                            
                              sys_recv_pf_t recv_cbk,void *user_ctx_p);
			      

/*
**__________________________________________________________________________
*/
/**
*  That function is the call back associated with SP_READ_RDMA

   The goal of that function is to copy the data (bins) returned by a storio at
   the revelant offset in the buffer that has been allocated for RDMA transfer.
   
   When it is done, the current received buffer is released and it is replaced
   by the buffer allocated for the RDMA transfer.
   
   When it is done, the regular non RDMA callback function rozofs_storcli_read_req_processing_cbk()
   is called. By doing so, the usage of RDMA is transparent.

   @param this: transaction context
   @param param
   
   @retval none
*/
void rozofs_storcli_read_rdma_req_processing_cbk(void *this,void *param) ;
 
 /*__________________________________________________________________________
*/
/**
*   RDMÂ/TCP  connect callback upon successful connect
     

 @param userRef : pointer to a load balancer entry
 @param socket_context_ref : index of the socket context
 @param retcode : always RUC_OK
 @param errnum : always 0
 
 @retval none
*/
void rozofs_rdma_tcp_client_connect_CBK (void *userRef,uint32_t socket_context_ref,int retcode,int errnum);
/*
 **____________________________________________________
 */
/**
 Callback upon disconnect of the TCP connection of the load balancing group

 @param userRef : pointer to a load balancer entry
 @param socket_context_ref: AF_unix context index
 @param bufRef : pointer to the packet buffer on which the error has been encountered (not used)
 @param err_no : errno associated with the disconnect
 
 @retval none
*/

void  rozofs_rdma_tcp_client_dis_CBK(void *userRef,uint32_t socket_context_ref,void *bufRef,int err_no);

/*
**__________________________________________________________________________
*/
/**
   SP_WRITE_RDMA encoding

    @param lbg_id             : reference of the load balancing group
    @param socket_context_ref : socket controller reference (index) of the TCP connection for which there is an associated RDMA connection
    @param timeout_sec        : transaction timeout
    @param prog               : program
    @param vers               : program version
    @param opcode             : metadata opcode
    @param encode_fct         : encoding function
    @msg2encode_p             : pointer to the message to encode
    @param prj_buf            : pointer to the buffer that contains the projection
    @param seqnum             : sequence number associated with the context (store as an opaque parameter in the transaction context
    @param opaque_value_idx1  : opaque value at index 1
    @param extra_len          : extra length to add after encoding RPC (must be 4 bytes aligned !!!)
    @param recv_cbk           : receive callback function

    @param user_ctx_p : pointer to the working context
   
    @retval 0 on success;
    @retval -1 on error,, errno contains the cause
    @retval -2 if non RDMA path should be used
*/
int rozofs_sorcli_sp_write_rdma(uint32_t lbg_id,uint32_t socket_context_ref, uint32_t timeout_sec, uint32_t prog,uint32_t vers,
                              int opcode,xdrproc_t encode_fct,void *msg2encode_p,
                              void *proj_buf,
                              uint32_t seqnum,
                              uint32_t opaque_value_idx1,  
                              int      extra_len,                            
                              sys_recv_pf_t recv_cbk,void *user_ctx_p) ;
			      


/*
**__________________________________________________________________________
*/
/**
*  That function is the call back associated with SP_WRITE_RDMA

   The goal of that function is to copy the data (bins) returned by a storio at
   the revelant offset in the buffer that has been allocated for RDMA transfer.
   
   When it is done, the current received buffer is released and it is replaced
   by the buffer allocated for the RDMA transfer.
   
   When it is done, the regular non RDMA callback function rozofs_storcli_read_req_processing_cbk()
   is called. By doing so, the usage of RDMA is transparent.

;   @param this: transaction context
   @param param
   
   @retval none
*/
void rozofs_storcli_write_rdma_req_processing_cbk(void *this,void *param);	      
/*
**__________________________________________________________________________
*/
/**
*   Init of the table used to deal with time-out/out of sequence and TCP disconnection
    on LBG that are using RDMA
    
    @param mx_lbg_north_ctx:  number of context to handle
    
    @retval RUC_OK on success
*/
int  rdma_lbg_tmo_table_init(uint32_t mx_lbg_north_ctx);


 #endif 
