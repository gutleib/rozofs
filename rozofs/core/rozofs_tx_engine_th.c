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

#include <stdlib.h>
#include <stddef.h>
#include <arpa/inet.h>

#include <rozofs/common/log.h>
#include <rozofs/rpc/rozofs_rpc_util.h>

#include "rozofs_tx_common.h"
#include "com_tx_timer_api.h"
#include "rozofs_tx_api.h"
#include "uma_dbg_api.h"
#include "ruc_buffer_debug.h"
#include "socketCtrl_th.h"
#include "ruc_sockCtl_api_th.h"


typedef struct _rozofs_tx_ctx_th_t
{
   int thread_owner;
   int module_idx;
   rozofs_tx_ctx_t *rozofs_tx_context_freeListHead; /**< head of list of the free context  */

   rozofs_tx_ctx_t rozofs_tx_context_activeListHead; /**< list of the active context     */

   uint32_t rozofs_tx_context_count; /**< Max number of contexts    */
   uint32_t rozofs_tx_context_allocated; /**< current number of allocated context        */
   rozofs_tx_ctx_t *rozofs_tx_context_pfirst; /**< pointer to the first context of the pool */
   uint32_t rozofs_tx_global_transaction_id;

   uint64_t rozofs_tx_stats[ROZOFS_TX_COUNTER_MAX];
   /**
    * Buffers information
    */
   int rozofs_small_tx_xmit_count ;
   int rozofs_small_tx_xmit_size ;
   int rozofs_large_tx_xmit_count;
   int rozofs_large_tx_xmit_size ;
   int rozofs_small_tx_recv_count ;
   int rozofs_small_tx_recv_size ;
   int rozofs_large_tx_recv_count;
   int rozofs_large_tx_recv_size ;

   void *rozofs_tx_pool[_ROZOFS_TX_MAX_POOL];
   ruc_pf_void_t rozofs_tx_rdma_recv_freeBuffer_cbk;
   ruc_pf_2uint32_t rozofs_tx_rdma_out_of_sequence_cbk;
} rozofs_tx_ctx_th_t;

int debug_declare_done= 0;

rozofs_tx_ctx_th_t *rozofs_tx_ctx_th_tb[ROZOFS_MAX_SOCKCTL_DESCRIPTOR]={NULL};

#define MICROLONG(time) ((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)
#define ROZOFS_TX_DEBUG_TOPIC      "trx_th"
#define ROZOFS_TX_DEBUG_TOPIC2      "tx_test"

/*__________________________________________________________________________
 
*/
/**
*   Get the pointer to the socket controller based on the threadID
*/
void *_rozofs_tx_get_ctx_for_thread(char *file,int line)
{
  int module_idx;
  module_idx = ruc_sockctl_get_thread_module_idx_th();
  if (module_idx < 0)
  {
    fatal("No transaction module for thread %lu (name: %s:%d)\n",pthread_self(),file,line);  
  }
  if (rozofs_tx_ctx_th_tb[module_idx] == NULL)
  {
    fatal("No transaction module for thread %lu (name: %s:%d)\n",pthread_self(),file,line);  
  }
  return rozofs_tx_ctx_th_tb[module_idx];
}

/*
**______________________________________________________________________
*/

void rozofs_tx_stats_th(int counter)
{
    rozofs_tx_ctx_th_t *rz_tx_ctx_th_p = rozofs_tx_get_ctx_for_thread();
    rz_tx_ctx_th_p->rozofs_tx_stats[counter]++;

}
/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void rozofs_tx_debug_show_th(rozofs_tx_ctx_th_t *rz_tx_ctx_th_p,uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();

    pChar += sprintf(pChar, "Module Index                                       : %d\n", rz_tx_ctx_th_p->module_idx);
    pChar += sprintf(pChar, "number of transaction contexts (initial/allocated) : %u/%u\n", rz_tx_ctx_th_p->rozofs_tx_context_count, rz_tx_ctx_th_p->rozofs_tx_context_allocated);
    pChar += sprintf(pChar, "context size (bytes)                               : %u\n", (unsigned int) sizeof (rozofs_tx_ctx_t));
    ;
    pChar += sprintf(pChar, "Total memory size (bytes)                          : %u\n", (unsigned int) sizeof (rozofs_tx_ctx_t) * rz_tx_ctx_th_p->rozofs_tx_context_count);
    ;
    pChar += sprintf(pChar, "Statistics\n");
    pChar += sprintf(pChar, "TX_SEND           : %10llu\n", (unsigned long long int) rz_tx_ctx_th_p->rozofs_tx_stats[ROZOFS_TX_SEND]);
    pChar += sprintf(pChar, "TX_SEND_ERR       : %10llu\n", (unsigned long long int) rz_tx_ctx_th_p->rozofs_tx_stats[ROZOFS_TX_SEND_ERROR]);
    pChar += sprintf(pChar, "TX_RECV_OK        : %10llu\n", (unsigned long long int) rz_tx_ctx_th_p->rozofs_tx_stats[ROZOFS_TX_RECV_OK]);
    pChar += sprintf(pChar, "TX_RECV_OUT_SEQ   : %10llu\n", (unsigned long long int) rz_tx_ctx_th_p->rozofs_tx_stats[ROZOFS_TX_RECV_OUT_SEQ]);
    pChar += sprintf(pChar, "TX_TIMEOUT        : %10llu\n", (unsigned long long int) rz_tx_ctx_th_p->rozofs_tx_stats[ROZOFS_TX_TIMEOUT]);
    pChar += sprintf(pChar, "TX_ENCODING_ERROR : %10llu\n", (unsigned long long int) rz_tx_ctx_th_p->rozofs_tx_stats[ROZOFS_TX_ENCODING_ERROR]);
    pChar += sprintf(pChar, "TX_DECODING_ERROR : %10llu\n", (unsigned long long int) rz_tx_ctx_th_p->rozofs_tx_stats[ROZOFS_TX_DECODING_ERROR]);
    pChar += sprintf(pChar, "TX_CTX_MISMATCH   : %10llu\n", (unsigned long long int) rz_tx_ctx_th_p->rozofs_tx_stats[ROZOFS_TX_CTX_MISMATCH]);
    pChar += sprintf(pChar, "TX_NO_CTX_ERROR   : %10llu\n", (unsigned long long int) rz_tx_ctx_th_p->rozofs_tx_stats[ROZOFS_TX_NO_CTX_ERROR]);
    pChar += sprintf(pChar, "TX_NO_BUFFER_ERROR: %10llu\n", (unsigned long long int) rz_tx_ctx_th_p->rozofs_tx_stats[ROZOFS_TX_NO_BUFFER_ERROR]);
    pChar += sprintf(pChar, "\n");
    pChar += sprintf(pChar, "Buffer Pool (name[size] :initial/current\n");
    pChar += sprintf(pChar, "Xmit Buffer            \n");
    pChar += sprintf(pChar, "  small[%6d]  : %6d/%d\n", rz_tx_ctx_th_p->rozofs_small_tx_xmit_size, rz_tx_ctx_th_p->rozofs_small_tx_xmit_count,
            ruc_buf_getFreeBufferCount(ROZOFS_TX_SMALL_TX_POOL_TH(rz_tx_ctx_th_p)));
    pChar += sprintf(pChar, "  large[%6d]  : %6d/%d\n", rz_tx_ctx_th_p->rozofs_large_tx_xmit_size, rz_tx_ctx_th_p->rozofs_large_tx_xmit_count,
            ruc_buf_getFreeBufferCount(ROZOFS_TX_LARGE_TX_POOL_TH(rz_tx_ctx_th_p)));
    pChar += sprintf(pChar, "Recv Buffer            \n");
    pChar += sprintf(pChar, "  small[%6d]  : %6d/%d\n", rz_tx_ctx_th_p->rozofs_small_tx_recv_size, rz_tx_ctx_th_p->rozofs_small_tx_recv_count,
            ruc_buf_getFreeBufferCount(ROZOFS_TX_SMALL_RX_POOL_TH(rz_tx_ctx_th_p)));
    pChar += sprintf(pChar, "  large[%6d]  : %6d/%d\n", rz_tx_ctx_th_p->rozofs_large_tx_recv_size, rz_tx_ctx_th_p->rozofs_large_tx_recv_count,
            ruc_buf_getFreeBufferCount(ROZOFS_TX_LARGE_RX_POOL_TH(rz_tx_ctx_th_p)));
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());

}

/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void rozofs_tx_debug_th(char * argv[], uint32_t tcpRef, void *bufRef) {

  char *pChar = uma_dbg_get_buffer();
  int val;
  errno = 0;   
  if (argv[1] == 0)
  {
     sprintf(pChar,"Module index is missing\n");
     uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
     return; 
  }         
  val = (int) strtol(argv[1], (char **) NULL, 10);   
  if (errno != 0) {
   pChar += sprintf(pChar,"bad value %s\n",argv[1]);    
   uma_dbg_send(tcpRef, bufRef, TRUE,uma_dbg_get_buffer());
   return;     
  }
  
  if (val >= ROZOFS_MAX_SOCKCTL_DESCRIPTOR) 
  {
   pChar += sprintf(pChar,"out of range value %s (max is %u)\n",argv[1],ROZOFS_MAX_SOCKCTL_DESCRIPTOR-1);    
   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
   return;         
  }
  if (rozofs_tx_ctx_th_tb[val] == NULL)
  {
   pChar += sprintf(pChar,"no transaction context for module %s\n",argv[1]);    
   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
   return;           
  
  }
  return rozofs_tx_debug_show_th(rozofs_tx_ctx_th_tb[val],tcpRef, bufRef);
}

/*__________________________________________________________________________
  Register to the debug SWBB
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void rozofs_tx_debug_init_th() {

  
  if (debug_declare_done == 0){
    uma_dbg_addTopic(ROZOFS_TX_DEBUG_TOPIC, rozofs_tx_debug_th);
    debug_declare_done = 1;
  }
}



/*
 **  END OF DEBUG
 */

/*-----------------------------------------------
 **   rozofs_tx_getObjCtx_p_th

 ** based on the object index, that function
 ** returns the pointer to the object context.
 **
 ** That function may fails if the index is
 ** not a Transaction context index type.
 **
@param     : MS index
@retval   : NULL if error

 */

rozofs_tx_ctx_t *rozofs_tx_getObjCtx_p_th(uint32_t transaction_id) {
    uint32_t index;
    rozofs_tx_ctx_t *p;

    rozofs_tx_ctx_th_t *rz_tx_ctx_th_p = rozofs_tx_get_ctx_for_thread();
    /*
     **  Get the pointer to the context
     */
    index = transaction_id & RUC_OBJ_MASK_OBJ_IDX;
    if (index >= rz_tx_ctx_th_p->rozofs_tx_context_count) {
        /*
         ** the MS index is out of range
         */
        severe( "rozofs_tx_getObjCtx_p_th(%d): index is out of range, index max is %d", index, rz_tx_ctx_th_p->rozofs_tx_context_count );
        return (rozofs_tx_ctx_t*) NULL;
    }
    p = (rozofs_tx_ctx_t*) ruc_objGetRefFromIdx((ruc_obj_desc_t*) rz_tx_ctx_th_p->rozofs_tx_context_freeListHead,
            index);
    return ((rozofs_tx_ctx_t*) p);
}

/*-----------------------------------------------
 **   rozofs_tx_getObjCtx_ref_th

 ** based on the object index, that function
 ** returns the pointer to the object context.
 **
 ** That function may fails if the index is
 ** not a Transaction context index type.
 **
@param     : MS index
@retval   :-1 out of range

 */

uint32_t rozofs_tx_getObjCtx_ref_th(rozofs_tx_ctx_t *p) {
    uint32_t index;
    rozofs_tx_ctx_th_t *rz_tx_ctx_th_p = rozofs_tx_get_ctx_for_thread();
    
    index = (uint32_t) (p - rz_tx_ctx_th_p->rozofs_tx_context_pfirst);
    index -= 1;

    if (index >= rz_tx_ctx_th_p->rozofs_tx_context_count) {
        /*
         ** the MS index is out of range
         */
        severe( "rozofs_tx_getObjCtx_p_th(%d): index is out of range, index max is %d", index, rz_tx_ctx_th_p->rozofs_tx_context_count );
        return (uint32_t) - 1;
    }
    ;
    return index;
}




/*
 **____________________________________________________
 */

/**
   rozofs_tx_init

  initialize the Transaction management module

@param     : NONE
@retval   none   :
 */
void rozofs_tx_init_th() {

    rozofs_tx_ctx_th_t *rz_tx_ctx_th_p = rozofs_tx_get_ctx_for_thread();
    
    rz_tx_ctx_th_p->rozofs_tx_context_pfirst = (rozofs_tx_ctx_t*) NULL;

    rz_tx_ctx_th_p->rozofs_tx_context_allocated = 0;
    rz_tx_ctx_th_p->rozofs_tx_context_count = 0;
}



/*-----------------------------------------------
 **   rozofs_tx_alloc

 **  create a Transaction context
 **   That function tries to allocate a free PDP
 **   context. In case of success, it returns the
 **   index of the Transaction context.
 **
@param     : recli index
@param       relayCref : RELAY-C ref of the context
@retval   : MS controller reference (if OK)
@retval    NULL if out of context.
 */
rozofs_tx_ctx_t *rozofs_tx_alloc_th() {
    rozofs_tx_ctx_t *p;

    rozofs_tx_ctx_th_t *rz_tx_ctx_th_p = rozofs_tx_get_ctx_for_thread();
    /*
     **  Get the first free context
     */
    if ((p = (rozofs_tx_ctx_t*) ruc_objGetFirst((ruc_obj_desc_t*) rz_tx_ctx_th_p->rozofs_tx_context_freeListHead))
            == (rozofs_tx_ctx_t*) NULL) {
        /*
         ** out of Transaction context descriptor try to free some MS
         ** context that are out of date 
         */
        severe( "NOT ABLE TO GET a TX CONTEXT" );
        return NULL;
    }
    /*
     **  reinitilisation of the context
     */
    rozofs_tx_ctxInit(p, FALSE);
    /*
     ** remove it for the linked list
     */
    rz_tx_ctx_th_p->rozofs_tx_context_allocated++;
    p->free = FALSE;


    ruc_objRemove((ruc_obj_desc_t*) p);

    return p;
}
/*
 **____________________________________________________
 */

/**
   rozofs_tx_createIndex

  create a Transaction context given by index 
   That function tries to allocate a free PDP
   context. In case of success, it returns the
   index of the Transaction context.

@param     : transaction_id is the reference of the context
@retval   : MS controller reference (if OK)
retval     -1 if out of context.
 */
uint32_t rozofs_tx_createIndex_th(uint32_t transaction_id) {
    rozofs_tx_ctx_t *p;

    rozofs_tx_ctx_th_t *rz_tx_ctx_th_p = rozofs_tx_get_ctx_for_thread();
    /*
     **  Get the first free context
     */
    p = rozofs_tx_getObjCtx_p_th(transaction_id);
    if (p == NULL) {
        severe( "MS ref out of range: %u", transaction_id );
        return RUC_NOK;
    }
    /*
     ** return an error if the context is not free
     */
    if (p->free == FALSE) {
        severe( "the context is not free : %u", transaction_id );
        return RUC_NOK;
    }
    /*
     **  reinitilisation of the context
     */
    rozofs_tx_ctxInit(p, FALSE);
    /*
     ** remove it for the linked list
     */
    rz_tx_ctx_th_p->rozofs_tx_context_allocated++;


    p->free = FALSE;
    ruc_objRemove((ruc_obj_desc_t*) p);

    return RUC_OK;
}


/*
 **____________________________________________________
 */

/**
   delete a Transaction context
   
   That function is intended to be called when
   a Transaction context is deleted. It returns the
   Transaction context to the free list. The delete
   procedure of the MS automaton and
   controller are called by that service.

   If the Transaction context is out of limit, and 
   error is returned.

@param     : MS Index
@retval   : RUC_OK : context has been deleted
@retval     RUC_NOK : out of limit index.
 */
uint32_t rozofs_tx_free_from_idx_th(uint32_t transaction_id) {
    rozofs_tx_ctx_t *p;
    rozofs_tx_ctx_th_t *rz_tx_ctx_th_p = rozofs_tx_get_ctx_for_thread();
    
    if (transaction_id >= rz_tx_ctx_th_p->rozofs_tx_context_count) {
        /*
         ** index is out of limits
         */
        return RUC_NOK;
    }
    /*
     ** get the reference from idx
     */
    p = rozofs_tx_getObjCtx_p_th(transaction_id);

    /*
     **  remove the xmit block
     */
    //   ruc_objRemove((ruc_obj_desc_t *)&p->xmitCtx);

    /*
     ** remove it from the active list
     */
    ruc_objRemove((ruc_obj_desc_t*) p);
    /*
     ** release the receive buffer is still in the context
     */
    if (p->recv_buf != NULL) {
        ruc_buf_freeBuffer(p->recv_buf);
        p->recv_buf = NULL;
    }
    if (p->xmit_buf != NULL) {
        /*
         ** decrement the inuse counter
         */
        int inuse = ruc_buf_inuse_decrement(p->xmit_buf);
        if (inuse == 1) {
            ruc_objRemove((ruc_obj_desc_t*) p->xmit_buf);
            ruc_buf_freeBuffer(p->xmit_buf);
        }
        p->xmit_buf = NULL;
    }
    /*
     ** clear the expected xid
     */
    p->xid = 0;
    /*
    ** remove the rw load balancing context from any list
    */
     ruc_objRemove(&p->rw_lbg.link);
   
    /*
     **  insert it in the free list
     */
    rz_tx_ctx_th_p->rozofs_tx_context_allocated--;


    p->free = TRUE;
    ruc_objInsertTail((ruc_obj_desc_t*) rz_tx_ctx_th_p->rozofs_tx_context_freeListHead,
            (ruc_obj_desc_t*) p);

    return RUC_OK;

}
/*
 **____________________________________________________
 */

/**
   rozofs_tx_free_from_ptr

   delete a Transaction context
   That function is intended to be called when
   a Transaction context is deleted. It returns the
   Transaction context to the free list. The delete
   procedure of the MS automaton and
   controller are called by that service.

   If the Transaction context is out of limit, and 
   error is returned.

@param     : pointer to the transaction context
@retval   : RUC_OK : context has been deleted
@retval     RUC_NOK : out of limit index.

 */
uint32_t rozofs_tx_free_from_ptr_th(rozofs_tx_ctx_t *p) {
    uint32_t transaction_id;

    transaction_id = rozofs_tx_getObjCtx_ref_th(p);
    if (transaction_id == (uint32_t) - 1) {
        return RUC_NOK;
    }
    return (rozofs_tx_free_from_idx_th(transaction_id));

}




/*
 **____________________________________________________
 */

/*
    Timeout call back associated with a transaction

@param     :  tx_p : pointer to the transaction context
 */

void rozofs_tx_timeout_CBK_th(void *opaque) {
    rozofs_tx_ctx_t *pObj = (rozofs_tx_ctx_t*) opaque;
    pObj->rpc_guard_timer_flg = TRUE;
    /*
     ** Attempt to remove and free the current xmit buffer if it has been queued 
     ** in the transaction context
     */
    if (pObj->xmit_buf != NULL) {
        /*
         ** decrement the inuse counter
         */
        int inuse = ruc_buf_inuse_decrement(pObj->xmit_buf);
        if (inuse == 1) {
            ruc_objRemove((ruc_obj_desc_t*) pObj->xmit_buf);
            ruc_buf_freeBuffer(pObj->xmit_buf);
        } else {
            /* This buffer may be in a queue somewhere */
            ruc_objRemove((ruc_obj_desc_t*) pObj->xmit_buf);
            /* Prevent transmitter to call a xmit done call back 
              that may queue this buffer somewhere */
            ruc_buf_set_opaque_ref(pObj->xmit_buf, NULL);
        }
        pObj->xmit_buf = NULL;
    }
    /*
     ** Process the current time-out for that transaction
     */

    //  uma_fsm_engine(pObj,&pObj->resumeFsm);
    pObj->status = -1;
    pObj->tx_errno = ETIME;
    /*
     ** Update global statistics
     */
    TX_STATS_TH(ROZOFS_TX_TIMEOUT);
    (*(pObj->recv_cbk))(pObj, pObj->user_param);
}

/*
 **____________________________________________________
 */

/*
  stop the guard timer associated with the transaction

@param     :  tx_p : pointer to the transaction context
@retval   : none
 */

void rozofs_tx_stop_timer_th(rozofs_tx_ctx_t *pObj) {

    pObj->rpc_guard_timer_flg = FALSE;
    com_tx_tmr_stop(&pObj->rpc_guard_timer);
}

/*
 **____________________________________________________
 */

/*
  start the guard timer associated with the transaction

@param     : tx_p : pointer to the transaction context
@param     : uint32_t  : delay in seconds (??)
@retval   : none
 */
void rozofs_tx_start_timer_th(rozofs_tx_ctx_t *tx_p, uint32_t time_ms) {
    uint8_t slot;
    /*
     ** check if the context is still allocated, it might be possible
     ** that the receive callback of the application can be called before
     ** the application starts the timer, in that case we must
     ** prevent the application to start the timer
     */
    if (tx_p->free == TRUE) {
        /*
         ** context has been release
         */
        return;
    }
    /*
     **  remove the timer from its current list
     */
    slot = time_ms / 5;
    if (slot >= COM_TX_TMR_SLOT5) slot = COM_TX_TMR_SLOT5;

    tx_p->rpc_guard_timer_flg = FALSE;
    com_tx_tmr_stop(&tx_p->rpc_guard_timer);
    com_tx_tmr_start(slot,
            &tx_p->rpc_guard_timer,
            time_ms * 1000,
            rozofs_tx_timeout_CBK_th,
            (void*) tx_p);

}

/*
 **____________________________________________________
 */

/**
 *  transaction receive callback associated with the RPC protocol
  This corresponds to the callback that is call upon the
  reception of the transaction reply from the remote end
  
  The input parameter is a receive buffer belonging to
  the transaction egine module
  
  @param userRef: pointer to the rdma_out_of_seq_CallBack (might be NULL)
  @param lbg_id: reference of the load balancing group
  @param recv_buf: pointer to the receive buffer
 */
typedef struct _rozofs_rpc_common_t {
    uint32_t msg_sz; /**< size of the rpc message */
    uint32_t xid; /**< transaction identifier */
} rozofs_rpc_common_t;

void rozofs_tx_recv_rpc_cbk_th(void *userRef, uint32_t lbg_id, void *recv_buf) {
    rozofs_rpc_common_t *com_hdr_p;
    rozofs_tx_ctx_t *this;
    uint32_t recv_xid;
    uint32_t ctx_idx;

    /*
     ** get the pointer to the payload of the buffer
     */
    com_hdr_p = (rozofs_rpc_common_t*) ruc_buf_getPayload(recv_buf);
    /*
     ** extract the xid and get the reference of the transaction context from it
     ** caution: need to swap to have it in host order
     */
    recv_xid = ntohl(com_hdr_p->xid);
    ctx_idx = rozofs_tx_get_tx_idx_from_xid(recv_xid);
    this = rozofs_tx_getObjCtx_p_th(ctx_idx);
    if (this == NULL) {
        /*
         ** that case should not occur, just release the received buffer
         */
        TX_STATS_TH(ROZOFS_TX_CTX_MISMATCH);
        ruc_buf_freeBuffer(recv_buf);
        return;
    }
    /*
     ** Check if the received xid matches with the one of the transacttion context
     */
    if (this->xid != recv_xid) {
        /*
         ** it might be an old transaction id -> drop the received buffer
         */
	 /*
	 ** check if there is an associated out of sequence callback with the connection it comes from
	 */
	 if (userRef!= NULL)
	 {
	    ruc_pf_2uint32_t cbk;
	    
	    cbk = (ruc_pf_2uint32_t) userRef;
	    (*cbk)(lbg_id,this->xid);	 
	 }
        TX_STATS_TH(ROZOFS_TX_RECV_OUT_SEQ);
        ruc_buf_freeBuffer(recv_buf);
        return;
    }
    /*
     ** update receive stats
     */
    TX_STATS_TH(ROZOFS_TX_RECV_OK);
    /*
     ** store the reference of the received buffer in the transaction context
     */
    this->recv_buf = recv_buf;
    /*
     ** set the status and errno to 0
     */
    this->status = 0;
    this->tx_errno = 0;
    /*
     ** OK, that transaction is the one associated with the context
     ** stop the rpc guard timer and dispatch the processing 
     ** according to the message opcode
     */
    rozofs_tx_stop_timer(this);
    /*
     ** remove the reference of the xmit buffer if that one has been saved in the transaction context
     */
    if (this->xmit_buf != NULL) {
        /*
         ** decrement the inuse counter<
         */
        int inuse = ruc_buf_inuse_decrement(this->xmit_buf);
        if (inuse == 1) {
            ruc_objRemove((ruc_obj_desc_t*) this->xmit_buf);
            ruc_buf_freeBuffer(this->xmit_buf);
        } else {
            /* This buffer may be in a queue somewhere */
            ruc_objRemove((ruc_obj_desc_t*) this->xmit_buf);
            /* Prevent transmitter to call a xmit done call back 
              that may queue this buffer somewhere */
            ruc_buf_set_opaque_ref(this->xmit_buf, NULL);
        }
        this->xmit_buf = NULL;
    }
    /*
     ** OK, let's get the receive callback associated with the transaction context and call it
     */
    (*(this->recv_cbk))(this, this->user_param);

    return;
}

/*
 **____________________________________________________
 */

/**
 *  transaction receive callback associated with the RPC protocol over RDMA
  This corresponds to the callback that is call upon the
  reception of the transaction reply from the remote end
  
  The input parameter is a receive buffer belonging to
  the transaction egine module
  
  @param recv_buf: pointer to the receive buffer
 */
void rozofs_tx_recv_rdma_rpc_cbk_th(void *recv_buf) {
    rozofs_rpc_common_t *com_hdr_p;
    rozofs_tx_ctx_t *this;
    uint32_t recv_xid;
    uint32_t ctx_idx;

    /*
     ** get the pointer to the payload of the buffer
     */
    com_hdr_p = (rozofs_rpc_common_t*) ruc_buf_getPayload(recv_buf);
    /*
     ** extract the xid and get the reference of the transaction context from it
     ** caution: need to swap to have it in host order
     */
    recv_xid = ntohl(com_hdr_p->xid);
    ctx_idx = rozofs_tx_get_tx_idx_from_xid(recv_xid);
    this = rozofs_tx_getObjCtx_p_th(ctx_idx);
    if (this == NULL) {
        /*
         ** that case should not occur, just release the received buffer
         */
        TX_STATS_TH(ROZOFS_TX_CTX_MISMATCH);
        (*rozofs_tx_rdma_recv_freeBuffer_cbk)(recv_buf);
        return;
    }
    /*
     ** Check if the received xid matches with the one of the transacttion context
     */
    if (this->xid != recv_xid) {
        /*
         ** it might be an old transaction id -> drop the received buffer
         */
	 /*
	 ** check if there is an associated out of sequence callback with the connection it comes from
	 */
	 if (rozofs_tx_rdma_out_of_sequence_cbk!= NULL)
	 {
	    /*
	    ** extract the lbg_id from the RPC reply message 
	    */
	    uint32_t lbg_id = rozofs_rpc_get_lbg_id_in_reply(recv_buf);
	    (*rozofs_tx_rdma_out_of_sequence_cbk)(lbg_id,this->xid);	 
	 }
        TX_STATS_TH(ROZOFS_TX_RECV_OUT_SEQ);
        (*rozofs_tx_rdma_recv_freeBuffer_cbk)(recv_buf);
        return;
    }
    /*
     ** update receive stats
     */
    TX_STATS_TH(ROZOFS_TX_RECV_OK);
    /*
     ** store the reference of the received buffer in the transaction context
     */
    this->recv_buf = recv_buf;
    /*
     ** set the status and errno to 0
     */
    this->status = 0;
    this->tx_errno = 0;
    /*
     ** OK, that transaction is the one associated with the context
     ** stop the rpc guard timer and dispatch the processing 
     ** according to the message opcode
     */
    rozofs_tx_stop_timer(this);
    /*
     ** remove the reference of the xmit buffer if that one has been saved in the transaction context
     */
    if (this->xmit_buf != NULL) {
        /*
         ** decrement the inuse counter<
         */
        int inuse = ruc_buf_inuse_decrement(this->xmit_buf);
        if (inuse == 1) {
            ruc_objRemove((ruc_obj_desc_t*) this->xmit_buf);
            ruc_buf_freeBuffer(this->xmit_buf);
        } else {
            /* This buffer may be in a queue somewhere */
            ruc_objRemove((ruc_obj_desc_t*) this->xmit_buf);
            /* Prevent transmitter to call a xmit done call back 
              that may queue this buffer somewhere */
            ruc_buf_set_opaque_ref(this->xmit_buf, NULL);
        }
        this->xmit_buf = NULL;
    }
    /*
     ** OK, let's get the receive callback associated with the transaction context and call it
     */
    (*(this->recv_cbk))(this, this->user_param);

    return;
}





/*
 **____________________________________________________
 */

/**
 * Transaction abort callback:

  That callback is called when the load balancing group fails to send a rpc request
  Thaemajor cause of the failure is that all the TCP connections are down and retry counter of the request
  is exhauted.
 
  
  @param userRef: not used (not significant, always NULL) 
  @param lbg_id: reference of the load balancing group
  @param xmit_buf: pointer to the xmit buffer that contains the RPC request
  @param errcode: error encountered (errno value)
 */
void rozofs_tx_xmit_abort_rpc_cbk_th(void *userRef, uint32_t lbg_id, void *xmit_buf, int errcode) {
    rozofs_rpc_common_t *com_hdr_p;
    rozofs_tx_ctx_t *this;
    uint32_t recv_xid;
    uint32_t ctx_idx;

    /*
     ** Check if the reference of the buf is NULL: if the reference of the buffer is NULL
     ** just drop the processing of that event
     */
    if (xmit_buf == NULL) return;
    /*
     ** get the pointer to the payload of the buffer
     */
    com_hdr_p = (rozofs_rpc_common_t*) ruc_buf_getPayload(xmit_buf);
    /*
     ** extract the xid and get the reference of the transaction context from it
     ** caution: need to swap to have it in host order
     */
    recv_xid = ntohl(com_hdr_p->xid);
    ctx_idx = rozofs_tx_get_tx_idx_from_xid(recv_xid);
    this = rozofs_tx_getObjCtx_p_th(ctx_idx);
    if (this == NULL) {
        /*
         ** that case should not occur, just release the xmit buffer
         */
        ruc_buf_freeBuffer(xmit_buf);
        return;
    }
    /*
     ** Check if the received xid matches with the one of the transacttion context
     */
    if (this->xid != recv_xid) {
        /*
         ** it might be an old transaction id -> drop the received buffer
         */
        TX_STATS_TH(ROZOFS_TX_RECV_OUT_SEQ);
        ruc_buf_freeBuffer(xmit_buf);
        return;
    }
    /*
     ** update xmit  stats
     */
    TX_STATS_TH(ROZOFS_TX_SEND_ERROR);
    /*
     ** set the status and errno to 0
     */
    this->status = -1;
    this->tx_errno = errcode;
    /*
     ** OK, that transaction is the one associated with the context
     ** stop the rpc guard timer and dispatch the processing 
     ** according to the message opcode
     */
    rozofs_tx_stop_timer(this);
    /*
     ** remove the reference of the xmit buffer if that one has been saved in the transaction context
     */
    if (this->xmit_buf != NULL) {
        if (this->xmit_buf != xmit_buf) {
            severe("xmit buffer mismatch");
        }
        /*
         ** decrement the inuse counter
         */
        int inuse = ruc_buf_inuse_decrement(this->xmit_buf);
        if (inuse == 1) {
            ruc_objRemove((ruc_obj_desc_t*) this->xmit_buf);
            ruc_buf_freeBuffer(this->xmit_buf);
        }
        this->xmit_buf = NULL;
    } else {
        /*
         ** we are in the situation where the application does not care about the xmit buffer
         ** but has provided a callback to be warned in case of xmit failure
         ** In that case, we just have to release the buffer
         */
        ruc_buf_freeBuffer(xmit_buf);
    }
    /*
     ** OK, let's get the receive callback associated with the transaction context and call it
     */
    (*(this->recv_cbk))(this, this->user_param);

    return;
}

/*
 **____________________________________________________
 */

/**
 *  
  Callback to allocate a buffer for receiving a rpc message (mainly a RPC response
 
 
 The service might reject the buffer allocation because the pool runs
 out of buffer or because there is no pool with a buffer that is large enough
 for receiving the message because of a out of range size.

 @param userRef : pointer to a user reference: not used here
 @param socket_context_ref: socket context reference
 @param len : length of the incoming message
 
 @retval <>NULL pointer to a receive buffer
 @retval == NULL no buffer 
 */
void * rozofs_tx_userRcvAllocBufCallBack_th(void *userRef, uint32_t socket_context_ref, uint32_t len) {


    rozofs_tx_ctx_th_t *rz_tx_ctx_th_p = rozofs_tx_get_ctx_for_thread();
    /*
     ** check if a small or a large buffer must be allocated
     */
    if (len <= rz_tx_ctx_th_p->rozofs_small_tx_recv_size) {
        return ruc_buf_getBuffer(ROZOFS_TX_SMALL_RX_POOL_TH(rz_tx_ctx_th_p));
    }

    if (len <= rz_tx_ctx_th_p->rozofs_large_tx_recv_size) {
        return ruc_buf_getBuffer(ROZOFS_TX_LARGE_RX_POOL_TH(rz_tx_ctx_th_p));
    }
    return NULL;
}



/**
*  API to get the size of the small xmit buffer
 @param none
 
 @retval sise of the small buffer
*/
int rozofs_tx_get_small_buffer_size_th()
{
    rozofs_tx_ctx_th_t *rz_tx_ctx_th_p = rozofs_tx_get_ctx_for_thread();  
    return rz_tx_ctx_th_p->rozofs_small_tx_xmit_size;
}



/**
   rozofs_tx_module_init

  create the Transaction context pool

@param     : transaction_count : number of Transaction context
@retval   : RUC_OK : done
@retval          RUC_NOK : out of memory
 */
uint32_t rozofs_tx_module_init_th(uint32_t transaction_count,
        int max_small_tx_xmit_count, int max_small_tx_xmit_size,
        int max_large_tx_xmit_count, int max_large_tx_xmit_size,
        int max_small_tx_recv_count, int max_small_tx_recv_size,
        int max_large_tx_recv_count, int max_large_recv_size
        ) {
    rozofs_tx_ctx_t *p;
    uint32_t idxCur;
    ruc_obj_desc_t *pnext;
    uint32_t ret = RUC_OK;
    rozofs_tx_ctx_th_t *rz_tx_ctx_th_p = NULL;
    char name[128];
    
    int module_idx = ruc_sockctl_get_thread_module_idx_th();
     
    if (module_idx < 0)
    {
       fatal("Not module index for thread %lu",pthread_self());
    }
    
    /*
    ** allocate the context 
    */
    rz_tx_ctx_th_p = (rozofs_tx_ctx_th_t*)malloc(sizeof(rozofs_tx_ctx_th_t));
    if (rz_tx_ctx_th_p == NULL)
    {
       fatal("Out of memory");
    }
    memset(rz_tx_ctx_th_p,0,sizeof(rozofs_tx_ctx_th_t));
    
    rozofs_tx_ctx_th_tb[module_idx] =rz_tx_ctx_th_p;
    
    rz_tx_ctx_th_p->module_idx = module_idx;


    rz_tx_ctx_th_p->rozofs_small_tx_xmit_count = max_small_tx_xmit_count;
    rz_tx_ctx_th_p->rozofs_small_tx_xmit_size = max_small_tx_xmit_size;
    rz_tx_ctx_th_p->rozofs_large_tx_xmit_count = max_large_tx_xmit_count;
    rz_tx_ctx_th_p->rozofs_large_tx_xmit_size = max_large_tx_xmit_size;
    rz_tx_ctx_th_p->rozofs_small_tx_recv_count = max_small_tx_recv_count;
    rz_tx_ctx_th_p->rozofs_small_tx_recv_size = max_small_tx_recv_size;
    rz_tx_ctx_th_p->rozofs_large_tx_recv_count = max_large_tx_recv_count;
    rz_tx_ctx_th_p->rozofs_large_tx_recv_size = max_large_recv_size;

    rz_tx_ctx_th_p->rozofs_tx_context_allocated = 0;
    rz_tx_ctx_th_p->rozofs_tx_context_count = transaction_count;

    rz_tx_ctx_th_p->rozofs_tx_context_freeListHead = (rozofs_tx_ctx_t*) NULL;

    /*
     **  create the active list
     */
    ruc_listHdrInit((ruc_obj_desc_t*) & rz_tx_ctx_th_p->rozofs_tx_context_activeListHead);

    /*
     ** create the Transaction context pool
     */
    rz_tx_ctx_th_p->rozofs_tx_context_freeListHead = (rozofs_tx_ctx_t*) ruc_listCreate(transaction_count, sizeof (rozofs_tx_ctx_t));
    if (rz_tx_ctx_th_p->rozofs_tx_context_freeListHead == (rozofs_tx_ctx_t*) NULL) {
        /* 
         **  out of memory
         */

        RUC_WARNING(transaction_count * sizeof (rozofs_tx_ctx_t));
        return RUC_NOK;
    }
    /*
     ** store the pointer to the first context
     */
    rz_tx_ctx_th_p->rozofs_tx_context_pfirst = rz_tx_ctx_th_p->rozofs_tx_context_freeListHead;

    /*
     **  initialize each entry of the free list
     */
    idxCur = 0;
    pnext = (ruc_obj_desc_t*) NULL;
    while ((p = (rozofs_tx_ctx_t*) ruc_objGetNext((ruc_obj_desc_t*) rz_tx_ctx_th_p->rozofs_tx_context_freeListHead,
            &pnext))
            != (rozofs_tx_ctx_t*) NULL) {

        p->index = idxCur;
        p->free = TRUE;
        rozofs_tx_ctxInit(p, TRUE);
        idxCur++;
    }

    /*
     ** Initialize the RESUME and SUSPEND timer module: 100 ms
     */
    com_tx_tmr_init_th(100, 15);
    /*
     ** Clear the statistics counter
     */
    memset(rz_tx_ctx_th_p->rozofs_tx_stats, 0, sizeof (uint64_t) * ROZOFS_TX_COUNTER_MAX);
    rozofs_tx_debug_init_th();
    while (1) {
        rz_tx_ctx_th_p->rozofs_tx_pool[_ROZOFS_TX_SMALL_TX_POOL] = ruc_buf_poolCreate(rz_tx_ctx_th_p->rozofs_small_tx_xmit_count, rz_tx_ctx_th_p->rozofs_small_tx_xmit_size);
        if (rz_tx_ctx_th_p->rozofs_tx_pool[_ROZOFS_TX_SMALL_TX_POOL] == NULL) {
            ret = RUC_NOK;
            severe( "xmit ruc_buf_poolCreate(%d,%d)", rz_tx_ctx_th_p->rozofs_small_tx_xmit_count, rz_tx_ctx_th_p->rozofs_small_tx_xmit_size );
            break;
        }
	sprintf(name,"TxXmitSmall_%d",module_idx);
        ruc_buffer_debug_register_pool(name, rz_tx_ctx_th_p->rozofs_tx_pool[_ROZOFS_TX_SMALL_TX_POOL]);	
        rz_tx_ctx_th_p->rozofs_tx_pool[_ROZOFS_TX_LARGE_TX_POOL] = ruc_buf_poolCreate(rz_tx_ctx_th_p->rozofs_large_tx_xmit_count, rz_tx_ctx_th_p->rozofs_large_tx_xmit_size);
        if (rz_tx_ctx_th_p->rozofs_tx_pool[_ROZOFS_TX_LARGE_TX_POOL] == NULL) {
            ret = RUC_NOK;
            severe( "rcv ruc_buf_poolCreate(%d,%d)", rz_tx_ctx_th_p->rozofs_large_tx_xmit_count, rz_tx_ctx_th_p->rozofs_large_tx_xmit_size );
            break;
        }
	sprintf(name,"TxXmitLarge_%d",module_idx);
	ruc_buffer_debug_register_pool(name, rz_tx_ctx_th_p->rozofs_tx_pool[_ROZOFS_TX_LARGE_TX_POOL]);	
	
        rz_tx_ctx_th_p->rozofs_tx_pool[_ROZOFS_TX_SMALL_RX_POOL] = ruc_buf_poolCreate(rz_tx_ctx_th_p->rozofs_small_tx_recv_count, rz_tx_ctx_th_p->rozofs_small_tx_xmit_size);
        if (rz_tx_ctx_th_p->rozofs_tx_pool[_ROZOFS_TX_SMALL_RX_POOL] == NULL) {
            ret = RUC_NOK;
            severe( "xmit ruc_buf_poolCreate(%d,%d)", rz_tx_ctx_th_p->rozofs_small_tx_recv_count, rz_tx_ctx_th_p->rozofs_small_tx_xmit_size );
            break;
        }
	sprintf(name,"TxRcvSmall_%d",module_idx);
        ruc_buffer_debug_register_pool(name, rz_tx_ctx_th_p->rozofs_tx_pool[_ROZOFS_TX_SMALL_RX_POOL]);	

        rz_tx_ctx_th_p->rozofs_tx_pool[_ROZOFS_TX_LARGE_RX_POOL] = ruc_buf_poolCreate(rz_tx_ctx_th_p->rozofs_large_tx_recv_count, rz_tx_ctx_th_p->rozofs_large_tx_recv_size);
        if (rz_tx_ctx_th_p->rozofs_tx_pool[_ROZOFS_TX_LARGE_RX_POOL] == NULL) {
            ret = RUC_NOK;
            severe( "rcv ruc_buf_poolCreate(%d,%d)", rz_tx_ctx_th_p->rozofs_large_tx_recv_count, rz_tx_ctx_th_p->rozofs_large_tx_recv_size );
            break;
        }
	sprintf(name,"TxRcvLarge_%d",module_idx);
        ruc_buffer_debug_register_pool(name, rz_tx_ctx_th_p->rozofs_tx_pool[_ROZOFS_TX_LARGE_RX_POOL]);	
        break;
    }
    return ret;
}
/**
**____________________________________________________
*  Get the number of free context in the transaction context distributor

  @param none
  @retval <>NULL, success->pointer to the allocated context
  @retval NULL, error ->out of context
*/
int rozofs_tx_get_free_ctx_number_th(void){
    rozofs_tx_ctx_th_t *rz_tx_ctx_th_p = rozofs_tx_get_ctx_for_thread();
  return (rz_tx_ctx_th_p->rozofs_tx_context_count-rz_tx_ctx_th_p->rozofs_tx_context_allocated);
}

/**
**____________________________________________________
* 
   Allocation of a small xmit buffer
  
   @param none
   @retval <> NULL pointer to the ruc buffer
   @retval NULL no buffer
*/   
void *rozofs_tx_get_small_xmit_buf_th()
{
      rozofs_tx_ctx_th_t *rz_tx_ctx_th_p = rozofs_tx_get_ctx_for_thread();
     return ruc_buf_getBuffer(ROZOFS_TX_SMALL_TX_POOL_TH(rz_tx_ctx_th_p)); 

}
/**
**____________________________________________________
* 
   Allocation of a large xmit buffer
  
   @param none
   @retval <> NULL pointer to the ruc buffer
   @retval NULL no buffer
*/ 
void *rozofs_tx_get_large_xmit_buf_th()
{
      rozofs_tx_ctx_th_t *rz_tx_ctx_th_p = rozofs_tx_get_ctx_for_thread();
     return ruc_buf_getBuffer(ROZOFS_TX_LARGE_TX_POOL_TH(rz_tx_ctx_th_p)); 

}
