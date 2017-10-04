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

#include "rozofs_tx_common.h"
#include "com_tx_timer_api.h"
#include "rozofs_tx_api.h"
#include "uma_dbg_api.h"
#include "ruc_buffer_debug.h"

rozofs_tx_ctx_t *rozofs_tx_context_freeListHead; /**< head of list of the free context  */
rozofs_tx_ctx_t rozofs_tx_context_activeListHead; /**< list of the active context     */

uint32_t rozofs_tx_context_count; /**< Max number of contexts    */
uint32_t rozofs_tx_context_allocated; /**< current number of allocated context        */
rozofs_tx_ctx_t *rozofs_tx_context_pfirst; /**< pointer to the first context of the pool */
uint32_t rozofs_tx_global_transaction_id = 0;

uint64_t rozofs_tx_stats[ROZOFS_TX_COUNTER_MAX];
/**
 * Buffers information
 */
int rozofs_small_tx_xmit_count = 0;
int rozofs_small_tx_xmit_size = 0;
int rozofs_large_tx_xmit_count = 0;
int rozofs_large_tx_xmit_size = 0;
int rozofs_small_tx_recv_count = 0;
int rozofs_small_tx_recv_size = 0;
int rozofs_large_tx_recv_count = 0;
int rozofs_large_tx_recv_size = 0;

void *rozofs_tx_pool[_ROZOFS_TX_MAX_POOL];


#define MICROLONG(time) ((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)
#define ROZOFS_TX_DEBUG_TOPIC      "trx"
#define ROZOFS_TX_DEBUG_TOPIC2      "tx_test"

/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void rozofs_tx_debug_show(uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();

    pChar += sprintf(pChar, "number of transaction contexts (initial/allocated) : %u/%u\n", rozofs_tx_context_count, rozofs_tx_context_allocated);
    pChar += sprintf(pChar, "context size (bytes)                               : %u\n", (unsigned int) sizeof (rozofs_tx_ctx_t));
    ;
    pChar += sprintf(pChar, "Total memory size (bytes)                          : %u\n", (unsigned int) sizeof (rozofs_tx_ctx_t) * rozofs_tx_context_count);
    ;
    pChar += sprintf(pChar, "Statistics\n");
    pChar += sprintf(pChar, "TX_SEND           : %10llu\n", (unsigned long long int) rozofs_tx_stats[ROZOFS_TX_SEND]);
    pChar += sprintf(pChar, "TX_SEND_ERR       : %10llu\n", (unsigned long long int) rozofs_tx_stats[ROZOFS_TX_SEND_ERROR]);
    pChar += sprintf(pChar, "TX_RECV_OK        : %10llu\n", (unsigned long long int) rozofs_tx_stats[ROZOFS_TX_RECV_OK]);
    pChar += sprintf(pChar, "TX_RECV_OUT_SEQ   : %10llu\n", (unsigned long long int) rozofs_tx_stats[ROZOFS_TX_RECV_OUT_SEQ]);
    pChar += sprintf(pChar, "TX_TIMEOUT        : %10llu\n", (unsigned long long int) rozofs_tx_stats[ROZOFS_TX_TIMEOUT]);
    pChar += sprintf(pChar, "TX_ENCODING_ERROR : %10llu\n", (unsigned long long int) rozofs_tx_stats[ROZOFS_TX_ENCODING_ERROR]);
    pChar += sprintf(pChar, "TX_DECODING_ERROR : %10llu\n", (unsigned long long int) rozofs_tx_stats[ROZOFS_TX_DECODING_ERROR]);
    pChar += sprintf(pChar, "TX_CTX_MISMATCH   : %10llu\n", (unsigned long long int) rozofs_tx_stats[ROZOFS_TX_CTX_MISMATCH]);
    pChar += sprintf(pChar, "TX_NO_CTX_ERROR   : %10llu\n", (unsigned long long int) rozofs_tx_stats[ROZOFS_TX_NO_CTX_ERROR]);
    pChar += sprintf(pChar, "TX_NO_BUFFER_ERROR: %10llu\n", (unsigned long long int) rozofs_tx_stats[ROZOFS_TX_NO_BUFFER_ERROR]);
    pChar += sprintf(pChar, "\n");
    pChar += sprintf(pChar, "Buffer Pool (name[size] :initial/current\n");
    pChar += sprintf(pChar, "Xmit Buffer            \n");
    pChar += sprintf(pChar, "  small[%6d]  : %6d/%d\n", rozofs_small_tx_xmit_size, rozofs_small_tx_xmit_count,
            ruc_buf_getFreeBufferCount(ROZOFS_TX_SMALL_TX_POOL));
    pChar += sprintf(pChar, "  large[%6d]  : %6d/%d\n", rozofs_large_tx_xmit_size, rozofs_large_tx_xmit_count,
            ruc_buf_getFreeBufferCount(ROZOFS_TX_LARGE_TX_POOL));
    pChar += sprintf(pChar, "Recv Buffer            \n");
    pChar += sprintf(pChar, "  small[%6d]  : %6d/%d\n", rozofs_small_tx_recv_size, rozofs_small_tx_recv_count,
            ruc_buf_getFreeBufferCount(ROZOFS_TX_SMALL_RX_POOL));
    pChar += sprintf(pChar, "  large[%6d]  : %6d/%d\n", rozofs_large_tx_recv_size, rozofs_large_tx_recv_count,
            ruc_buf_getFreeBufferCount(ROZOFS_TX_LARGE_RX_POOL));
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());

}

/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void rozofs_tx_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
    rozofs_tx_debug_show(tcpRef, bufRef);
}

/*__________________________________________________________________________
  Register to the debug SWBB
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void rozofs_tx_debug_init() {
    uma_dbg_addTopic(ROZOFS_TX_DEBUG_TOPIC, rozofs_tx_debug);
}




/*
 **  END OF DEBUG
 */

/*-----------------------------------------------
 **   rozofs_tx_getObjCtx_p

 ** based on the object index, that function
 ** returns the pointer to the object context.
 **
 ** That function may fails if the index is
 ** not a Transaction context index type.
 **
@param     : MS index
@retval   : NULL if error

 */

rozofs_tx_ctx_t *rozofs_tx_getObjCtx_p(uint32_t transaction_id) {
    uint32_t index;
    rozofs_tx_ctx_t *p;

    /*
     **  Get the pointer to the context
     */
    index = transaction_id & RUC_OBJ_MASK_OBJ_IDX;
    if (index >= rozofs_tx_context_count) {
        /*
         ** the MS index is out of range
         */
        severe( "rozofs_tx_getObjCtx_p(%d): index is out of range, index max is %d", index, rozofs_tx_context_count );
        return (rozofs_tx_ctx_t*) NULL;
    }
    p = (rozofs_tx_ctx_t*) ruc_objGetRefFromIdx((ruc_obj_desc_t*) rozofs_tx_context_freeListHead,
            index);
    return ((rozofs_tx_ctx_t*) p);
}

/*-----------------------------------------------
 **   rozofs_tx_getObjCtx_ref

 ** based on the object index, that function
 ** returns the pointer to the object context.
 **
 ** That function may fails if the index is
 ** not a Transaction context index type.
 **
@param     : MS index
@retval   :-1 out of range

 */

uint32_t rozofs_tx_getObjCtx_ref(rozofs_tx_ctx_t *p) {
    uint32_t index;
    index = (uint32_t) (p - rozofs_tx_context_pfirst);
    index -= 1;

    if (index >= rozofs_tx_context_count) {
        /*
         ** the MS index is out of range
         */
        severe( "rozofs_tx_getObjCtx_p(%d): index is out of range, index max is %d", index, rozofs_tx_context_count );
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
void rozofs_tx_init() {
    rozofs_tx_context_pfirst = (rozofs_tx_ctx_t*) NULL;

    rozofs_tx_context_allocated = 0;
    rozofs_tx_context_count = 0;
}

/*
 **____________________________________________________
 */

/**
   rozofs_tx_ctxInit

  create the transaction context pool

@param     : pointer to the Transaction context
@retval   : none
 */
void rozofs_tx_ctxInit(rozofs_tx_ctx_t *p, uint8_t creation) {

    p->integrity = -1; /* the value of this field is incremented at 
					      each MS ctx allocation */
    /*
      _______Event flags
     */
    /* MSG from UNC-CS */
    p->rpc_guard_timer_flg = FALSE;
    p->rpc_recv_flg = FALSE;

    p->timeStamp = 0;
    /**
     * opcode and transaction_id of the current on-going request
     */
    //   p->opcode = 0;
    //   p->transaction_id = 0;
    p->xid = 0;
    if (creation) p->xid_low = 0;

    /*
     ** xmit buffer: release by the transmitter or upon a TMO
     */
    p->xmit_buf = NULL;
    /*
     ** response part
     */
    p->recv_buf = NULL; /**< allocated by the receiver -> no to be release by the application */
    p->status = 0; /**< status of the operation */
    p->tx_errno = 0; /**< status of the operation */
    p->recv_cbk = (sys_recv_pf_t) NULL; /**< receive callback */
    p->user_param = NULL; /**< user param to provide upon reception */
    /* FSM */
    /*
     ** timer cell
     */
    ruc_listEltInit((ruc_obj_desc_t *) & p->rpc_guard_timer);
    /*
    ** load balancing context init
    */
    ruc_listEltInitAssoc( &p->rw_lbg.link,p);
    /*
    ** clear the reference of the RDMA buffer
    */
    p->rdma_bufref = NULL;
    

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
rozofs_tx_ctx_t *rozofs_tx_alloc() {
    rozofs_tx_ctx_t *p;

    /*
     **  Get the first free context
     */
    if ((p = (rozofs_tx_ctx_t*) ruc_objGetFirst((ruc_obj_desc_t*) rozofs_tx_context_freeListHead))
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
    rozofs_tx_context_allocated++;
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
uint32_t rozofs_tx_createIndex(uint32_t transaction_id) {
    rozofs_tx_ctx_t *p;

    /*
     **  Get the first free context
     */
    p = rozofs_tx_getObjCtx_p(transaction_id);
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
    rozofs_tx_context_allocated++;


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
uint32_t rozofs_tx_free_from_idx(uint32_t transaction_id) {
    rozofs_tx_ctx_t *p;

    if (transaction_id >= rozofs_tx_context_count) {
        /*
         ** index is out of limits
         */
        return RUC_NOK;
    }
    /*
     ** get the reference from idx
     */
    p = rozofs_tx_getObjCtx_p(transaction_id);

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
    rozofs_tx_context_allocated--;


    p->free = TRUE;
    ruc_objInsertTail((ruc_obj_desc_t*) rozofs_tx_context_freeListHead,
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
uint32_t rozofs_tx_free_from_ptr(rozofs_tx_ctx_t *p) {
    uint32_t transaction_id;

    transaction_id = rozofs_tx_getObjCtx_ref(p);
    if (transaction_id == (uint32_t) - 1) {
        return RUC_NOK;
    }
    return (rozofs_tx_free_from_idx(transaction_id));

}




/*
 **____________________________________________________
 */

/*
    Timeout call back associated with a transaction

@param     :  tx_p : pointer to the transaction context
 */

void rozofs_tx_timeout_CBK(void *opaque) {
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
    TX_STATS(ROZOFS_TX_TIMEOUT);
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

void rozofs_tx_stop_timer(rozofs_tx_ctx_t *pObj) {

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
void rozofs_tx_start_timer(rozofs_tx_ctx_t *tx_p, uint32_t time_ms) {
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
            rozofs_tx_timeout_CBK,
            (void*) tx_p);

}

/*
 **____________________________________________________
 */
// OBSOLETE
#if 0

/**
 *  transaction receive callback
  This corresponds to the callback that is call upon the
  reception of the transaction request from the remote end
  
  The input parameter is a receive buffer belonging to
  the transaction egine module
  
  @param recv_buf: pointer to the receive buffer
 */
void rozofs_tx_recv_cbk(void *recv_buf) {
    rozofs_com_hdr_t *com_hdr_p;
    rozofs_tx_ctx_t *this;
    /*
     ** get the pointer to the payload of the buffer
     */
    com_hdr_p = (rozofs_com_hdr_t*) ruc_buf_getPayload(recv_buf);
    /*
     ** extract the reference of the object and check if the transaction 
     ** match with the current one
     */
    /*
     ** WARNING :It might be better to use the service that permit to retrieve a context by its index
     ** rather than using directly its pointer !!! This is obvious when the message comes for an external box
     ** or process
     */
    this = (rozofs_tx_ctx_t*) com_hdr_p->sender_reference;
    /*
     ** WARNING : RSP_code MUST be REQ_code+1
     */
    if ((this->transaction_id != com_hdr_p->transaction_id) ||
            (this->opcode != (com_hdr_p->opcode) - 1)) {
        /*
         ** it might be an old transaction id -> drop the received buffer
         */
        TX_STATS(ROZOFS_TX_RECV_OUT_SEQ);
        ruc_buf_freeBuffer(recv_buf);
        return;
    }
    /*
     ** update receive stats
     */
    TX_STATS(ROZOFS_TX_RECV_OK);
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
         ** decrement the inuse counter
         */
        int inuse = ruc_buf_inuse_decrement(this->xmit_buf);
        if (inuse == 1) {
            ruc_objRemove((ruc_obj_desc_t*) this->xmit_buf);
            ruc_buf_freeBuffer(this->xmit_buf);
        }
        this->xmit_buf = NULL;
    }
    /*
     ** OK, let's get the receive callback associated with the transaction context and call it
     */
    (*(this->recv_cbk))(this, this->user_param);

    return;
}
#endif


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

void rozofs_tx_recv_rpc_cbk(void *userRef, uint32_t lbg_id, void *recv_buf) {
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
    this = rozofs_tx_getObjCtx_p(ctx_idx);
    if (this == NULL) {
        /*
         ** that case should not occur, just release the received buffer
         */
        TX_STATS(ROZOFS_TX_CTX_MISMATCH);
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
        TX_STATS(ROZOFS_TX_RECV_OUT_SEQ);
        ruc_buf_freeBuffer(recv_buf);
        return;
    }
    /*
     ** update receive stats
     */
    TX_STATS(ROZOFS_TX_RECV_OK);
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
void rozofs_tx_xmit_abort_rpc_cbk(void *userRef, uint32_t lbg_id, void *xmit_buf, int errcode) {
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
    this = rozofs_tx_getObjCtx_p(ctx_idx);
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
        TX_STATS(ROZOFS_TX_RECV_OUT_SEQ);
        ruc_buf_freeBuffer(xmit_buf);
        return;
    }
    /*
     ** update xmit  stats
     */
    TX_STATS(ROZOFS_TX_SEND_ERROR);
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
  Analyze the rpc header to extract the length of the rpc message
  The RPC header is a partial header and contains only the length of the rpc message
  That header is in network order format
  
  @param hdr_p: pointer to the beginning of a rpc message (length field)
  
  @retval length of the rpc payload
 */
uint32_t rozofs_tx_get_rpc_msg_len_cbk(char *hdr_p) {

    uint32_t rpc_len;
    uint32_t *msg_len_p;

    msg_len_p = (uint32_t*) hdr_p;
    rpc_len = ntohl(*msg_len_p);
    /*
     ** clear the bit 31
     */
    rpc_len &= 0x7fffffff;
    return rpc_len;
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
void * rozofs_tx_userRcvAllocBufCallBack(void *userRef, uint32_t socket_context_ref, uint32_t len) {

    /*
     ** check if a small or a large buffer must be allocated
     */
    if (len <= rozofs_small_tx_recv_size) {
        return ruc_buf_getBuffer(ROZOFS_TX_SMALL_RX_POOL);
    }

    if (len <= rozofs_large_tx_recv_size) {
        return ruc_buf_getBuffer(ROZOFS_TX_LARGE_RX_POOL);
    }
    return NULL;
}

/**
   rozofs_tx_module_init

  create the Transaction context pool

@param     : transaction_count : number of Transaction context
@retval   : RUC_OK : done
@retval          RUC_NOK : out of memory
 */
uint32_t rozofs_tx_module_init(uint32_t transaction_count,
        int max_small_tx_xmit_count, int max_small_tx_xmit_size,
        int max_large_tx_xmit_count, int max_large_tx_xmit_size,
        int max_small_tx_recv_count, int max_small_tx_recv_size,
        int max_large_tx_recv_count, int max_large_recv_size
        ) {
    rozofs_tx_ctx_t *p;
    uint32_t idxCur;
    ruc_obj_desc_t *pnext;
    uint32_t ret = RUC_OK;


    rozofs_small_tx_xmit_count = max_small_tx_xmit_count;
    rozofs_small_tx_xmit_size = max_small_tx_xmit_size;
    rozofs_large_tx_xmit_count = max_large_tx_xmit_count;
    rozofs_large_tx_xmit_size = max_large_tx_xmit_size;
    rozofs_small_tx_recv_count = max_small_tx_recv_count;
    rozofs_small_tx_recv_size = max_small_tx_recv_size;
    rozofs_large_tx_recv_count = max_large_tx_recv_count;
    rozofs_large_tx_recv_size = max_large_recv_size;

    rozofs_tx_context_allocated = 0;
    rozofs_tx_context_count = transaction_count;

    rozofs_tx_context_freeListHead = (rozofs_tx_ctx_t*) NULL;

    /*
     **  create the active list
     */
    ruc_listHdrInit((ruc_obj_desc_t*) & rozofs_tx_context_activeListHead);

    /*
     ** create the Transaction context pool
     */
    rozofs_tx_context_freeListHead = (rozofs_tx_ctx_t*) ruc_listCreate(transaction_count, sizeof (rozofs_tx_ctx_t));
    if (rozofs_tx_context_freeListHead == (rozofs_tx_ctx_t*) NULL) {
        /* 
         **  out of memory
         */

        RUC_WARNING(transaction_count * sizeof (rozofs_tx_ctx_t));
        return RUC_NOK;
    }
    /*
     ** store the pointer to the first context
     */
    rozofs_tx_context_pfirst = rozofs_tx_context_freeListHead;

    /*
     **  initialize each entry of the free list
     */
    idxCur = 0;
    pnext = (ruc_obj_desc_t*) NULL;
    while ((p = (rozofs_tx_ctx_t*) ruc_objGetNext((ruc_obj_desc_t*) rozofs_tx_context_freeListHead,
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
    com_tx_tmr_init(100, 15);
    /*
     ** Clear the statistics counter
     */
    memset(rozofs_tx_stats, 0, sizeof (uint64_t) * ROZOFS_TX_COUNTER_MAX);
    rozofs_tx_debug_init();
    while (1) {
        rozofs_tx_pool[_ROZOFS_TX_SMALL_TX_POOL] = ruc_buf_poolCreate(rozofs_small_tx_xmit_count, rozofs_small_tx_xmit_size);
        if (rozofs_tx_pool[_ROZOFS_TX_SMALL_TX_POOL] == NULL) {
            ret = RUC_NOK;
            severe( "xmit ruc_buf_poolCreate(%d,%d)", rozofs_small_tx_xmit_count, rozofs_small_tx_xmit_size );
            break;
        }
        ruc_buffer_debug_register_pool("TxXmitSmall", rozofs_tx_pool[_ROZOFS_TX_SMALL_TX_POOL]);	
        rozofs_tx_pool[_ROZOFS_TX_LARGE_TX_POOL] = ruc_buf_poolCreate(rozofs_large_tx_xmit_count, rozofs_large_tx_xmit_size);
        if (rozofs_tx_pool[_ROZOFS_TX_LARGE_TX_POOL] == NULL) {
            ret = RUC_NOK;
            severe( "rcv ruc_buf_poolCreate(%d,%d)", rozofs_large_tx_xmit_count, rozofs_large_tx_xmit_size );
            break;
        }
        ruc_buffer_debug_register_pool("TxXmitLarge", rozofs_tx_pool[_ROZOFS_TX_LARGE_TX_POOL]);	
        rozofs_tx_pool[_ROZOFS_TX_SMALL_RX_POOL] = ruc_buf_poolCreate(rozofs_small_tx_recv_count, rozofs_small_tx_xmit_size);
        if (rozofs_tx_pool[_ROZOFS_TX_SMALL_RX_POOL] == NULL) {
            ret = RUC_NOK;
            severe( "xmit ruc_buf_poolCreate(%d,%d)", rozofs_small_tx_recv_count, rozofs_small_tx_xmit_size );
            break;
        }
        ruc_buffer_debug_register_pool("TxRcvSmall", rozofs_tx_pool[_ROZOFS_TX_SMALL_RX_POOL]);	
        rozofs_tx_pool[_ROZOFS_TX_LARGE_RX_POOL] = ruc_buf_poolCreate(rozofs_large_tx_recv_count, rozofs_large_tx_recv_size);
        if (rozofs_tx_pool[_ROZOFS_TX_LARGE_RX_POOL] == NULL) {
            ret = RUC_NOK;
            severe( "rcv ruc_buf_poolCreate(%d,%d)", rozofs_large_tx_recv_count, rozofs_large_tx_recv_size );
            break;
        }
        ruc_buffer_debug_register_pool("TxRcvLarge", rozofs_tx_pool[_ROZOFS_TX_LARGE_RX_POOL]);	
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
int rozofs_tx_get_free_ctx_number(void){
  return (rozofs_tx_context_count-rozofs_tx_context_allocated);
}

