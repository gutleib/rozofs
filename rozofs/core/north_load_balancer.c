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

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <sys/un.h>             
#include <sys/ioctl.h>
#include <linux/sockios.h>
#include <rozofs/common/types.h>
#include <rozofs/common/log.h>

#include "ruc_common.h"
#include "ruc_list.h"
#include "af_unix_socket_generic_api.h"
#include "af_unix_socket_generic.h"
#include "rozofs_socket_family.h"
#include "uma_dbg_api.h"
#include "north_lbg.h"

north_lbg_ctx_t *north_lbg_context_freeListHead; /**< head of list of the free context  */
north_lbg_ctx_t north_lbg_context_activeListHead; /**< list of the active context     */

void *north_lbg_buffer_pool_tb[2]; /**< xmit and receive buffer pool */

uint32_t north_lbg_context_count; /**< Max number of contexts    */
uint32_t north_lbg_context_allocated; /**< current number of allocated context        */
north_lbg_ctx_t *north_lbg_context_pfirst; /**< pointer to the first context of the pool */


#define MICROLONG(time) ((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)
#define NORTH_LBG_DEBUG_TOPIC      "lbg"

char * lbg_north_state2String(int x) {

    switch (x) {
        case NORTH_LBG_DEPENDENCY: return "DEPENDENCY";
        case NORTH_LBG_UP: return "        UP";
        case NORTH_LBG_DOWN: return "      DOWN";

            /* Value out of range */
        default: return "?UNK?";
    }
}

/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void north_lbg_debug_show(char * argv[],uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int state;
    

    pChar += sprintf(pChar, "number of North Load Balancer contexts [size](initial/allocated) :[%u] %u/%u\n",
            (unsigned int) sizeof (north_lbg_ctx_t), (unsigned int) north_lbg_context_count,
            (unsigned int) (north_lbg_context_allocated));

    {
        north_lbg_ctx_t *lbg_p;
        ruc_obj_desc_t *pnext;
        int i;


        pnext = (ruc_obj_desc_t*) NULL;
        while ((lbg_p = (north_lbg_ctx_t*) ruc_objGetNext((ruc_obj_desc_t*) & north_lbg_context_activeListHead,
                &pnext))
                != (north_lbg_ctx_t*) NULL) {
            if (lbg_p->nb_entries_conf == 0) continue;

            state = north_lbg_eval_global_state(lbg_p);
            pChar += sprintf(pChar, "NAME: %-34s %s\n", lbg_p->name, lbg_north_state2String(state));
	    pChar += sprintf(pChar, "      %-25s: %d - active entry %d\n", "active/standby", lbg_p->active_standby_mode, lbg_p->active_lbg_entry);
            pChar += sprintf(pChar, "      %-25s: %s\n", "local/remote",lbg_p->local?"local":"remote");	    
            pChar += sprintf(pChar, "      size                     : %12u\n", lbg_p->nb_entries_conf);
            pChar += sprintf(pChar, "      total Up/Down Transitions: %12llu\n", (unsigned long long int) lbg_p->stats.totalUpDownTransition);
            north_lbg_entry_ctx_t *entry_p = lbg_p->entry_tb;
            pChar += sprintf(pChar, "      Main Queue               : %s\n", ruc_objIsEmptyList((ruc_obj_desc_t*) & lbg_p->xmitList[0]) ? "EMPTY" : "NON EMPTY");
            for (i = 0; i < lbg_p->nb_entries_conf; i++, entry_p++) {

                north_lbg_stats_t *stats_p = &entry_p->stats;
		if (stats_p->timestampCount == 0) continue;
                pChar += sprintf(pChar, "   Entry[%d]\n", i);
                pChar += sprintf(pChar, "       state                    : %s\n", lbg_north_state2String(entry_p->state));
                pChar += sprintf(pChar, "       Queue_entry              : %s\n", ruc_objIsEmptyList((ruc_obj_desc_t*) & entry_p->xmitList) ? "EMPTY" : "NON EMPTY");
                pChar += sprintf(pChar, "       Cnx  Attempts            : %12llu\n", (unsigned long long int) entry_p->stats.totalConnectAttempts);
                pChar += sprintf(pChar, "       Xmit messages            : %12llu\n", (unsigned long long int) entry_p->stats.totalXmit);
                pChar += sprintf(pChar, "       Recv messages            : %12llu\n", (unsigned long long int) entry_p->stats.totalRecv);
                pChar += sprintf(pChar, "       Xmit Perf. (count/time)  : %"PRIu64" / %"PRIu64" us / cumul %"PRIu64" us\n",
                        stats_p->timestampCount,
                        stats_p->timestampCount ? stats_p->timestampElasped / stats_p->timestampCount : 0,
                        stats_p->timestampElasped);  
		stats_p->timestampCount = 0;
		stats_p->timestampElasped = 0;
	 
            }
            pChar += sprintf(pChar, "  Cumulated\n");

            pChar += sprintf(pChar, "       total Xmit messages      : %12llu\n", (unsigned long long int) lbg_p->stats.totalXmit);
            pChar += sprintf(pChar, "       Main Xmit Queue Len      : %12llu\n", (unsigned long long int) lbg_p->stats.xmitQueuelen);
            pChar += sprintf(pChar, "       total Xmit retries       : %12llu\n", (unsigned long long int) lbg_p->stats.totalXmitRetries);
            pChar += sprintf(pChar, "       total Xmit aborted       : %12llu\n", (unsigned long long int) lbg_p->stats.totalXmitAborts);
            pChar += sprintf(pChar, "       total Xmit error         : %12llu\n", (unsigned long long int) lbg_p->stats.totalXmitError);
            pChar += sprintf(pChar, "       total Recv messages      : %12llu\n", (unsigned long long int) lbg_p->stats.totalRecv);
            //      pChar += sprintf(pChar,"      total Xmit Bytes         : %12llu\n",(unsigned long long int)lbg_p->stats.totalXmitBytes); 
            pChar += sprintf(pChar, "\n");
        }
    }
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());

}

void north_lbg_entries_debug_show(uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int siocinq_value;
    int siocoutq_value;

    {
        north_lbg_ctx_t *lbg_p;
        ruc_obj_desc_t *pnext;
        int i;
        pChar += sprintf(pChar, "  LBG Name                | lbg_id | idx  | sock |    state   | rdy |    Queue  | Cnx Attpts | Xmit Attpts | Recv count  |   Recv-Q  |   Send-Q  |\n");
        pChar += sprintf(pChar, "--------------------------+--------+------+------+------------+-----+-----------+------------+-------------+-------------+-----------+-----------+\n");
        pnext = (ruc_obj_desc_t*) NULL;
        while ((lbg_p = (north_lbg_ctx_t*) ruc_objGetNext((ruc_obj_desc_t*) & north_lbg_context_activeListHead,
                &pnext))
                != (north_lbg_ctx_t*) NULL) {
            if (lbg_p->nb_entries_conf == 0) continue;
            north_lbg_entry_ctx_t *entry_p = lbg_p->entry_tb;
            af_unix_ctx_generic_t *sock_p;


            for (i = 0; i < lbg_p->nb_entries_conf; i++, entry_p++) {
                sock_p = af_unix_getObjCtx_p(entry_p->sock_ctx_ref);
                pChar += sprintf(pChar, " %-24s |", lbg_p->name); 
                pChar += sprintf(pChar, "  %4d  |", lbg_p->index);
		if (lbg_p->active_lbg_entry == i) {
                  pChar += sprintf(pChar, "  %2d *|", i);
                }
		else {
                  pChar += sprintf(pChar, "  %2d  |", i);
		}  
                pChar += sprintf(pChar, " %4d |", sock_p->socketRef); /** socket */

                pChar += sprintf(pChar, " %s |", lbg_north_state2String(entry_p->state));
		while(1)
		{
		  af_unix_ctx_generic_t *sock_p ;
		  sock_p = af_unix_getObjCtx_p(entry_p->sock_ctx_ref);
		  if (sock_p == NULL)
		  {
                    pChar += sprintf(pChar, " %s |","???");
		     break;
		  }
                  pChar += sprintf(pChar, " %s |",(sock_p->cnx_availability_state == AF_UNIX_CNX_AVAILABLE )?"YES":" NO");
		  break;
		}
                pChar += sprintf(pChar, " %s |", ruc_objIsEmptyList((ruc_obj_desc_t*) & entry_p->xmitList) ? "    EMPTY" : "NON EMPTY");
                pChar += sprintf(pChar, " %10llu |", (unsigned long long int) entry_p->stats.totalConnectAttempts);
                pChar += sprintf(pChar, "  %10llu |", (unsigned long long int) entry_p->stats.totalXmit);
                pChar += sprintf(pChar, "  %10llu |", (unsigned long long int) entry_p->stats.totalRecv);
		siocinq_value = 0;
		siocoutq_value = 0;
		if (sock_p->socketRef)
		{
		  ioctl(sock_p->socketRef,SIOCINQ,&siocinq_value);
		  ioctl(sock_p->socketRef,SIOCOUTQ,&siocoutq_value);
		}
                pChar += sprintf(pChar, "  %8d |",  siocinq_value);
                pChar += sprintf(pChar, "  %8d |\n",siocoutq_value);
            }


        }
    }
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());

}

/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void north_lbg_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
    north_lbg_debug_show(argv,tcpRef, bufRef);
}

void north_lbg_entries_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
    north_lbg_entries_debug_show(tcpRef, bufRef);
}

/*__________________________________________________________________________
  Register to the debug SWBB
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void north_lbg_debug_init() {
    uma_dbg_addTopic(NORTH_LBG_DEBUG_TOPIC, north_lbg_debug);
    uma_dbg_addTopic("lbg_entries", north_lbg_entries_debug);
}




/*
 **  END OF DEBUG
 */


/*
 **_____________________________________________
 */

/**
 based on the object index, that function
 returns the pointer to the object context.

 That function may fails if the index is
 not a Transaction context index type.
 **
@param     : af_unix socket context index
@retval   : NULL if error
 */
north_lbg_ctx_t *north_lbg_getObjCtx_p(uint32_t north_lbg_ctx_id) {
    uint32_t index;
    north_lbg_ctx_t *p;

    /*
     **  Get the pointer to the context
     */
    index = north_lbg_ctx_id & RUC_OBJ_MASK_OBJ_IDX;
    if (index >= north_lbg_context_count) {
        /*
         ** the MS index is out of range
         */
        //severe( "north_lbg_getObjCtx_p(%d): index is out of range, index max is %d", index, north_lbg_context_count);
        return (north_lbg_ctx_t*) NULL;
    }
    p = (north_lbg_ctx_t*) ruc_objGetRefFromIdx((ruc_obj_desc_t*) north_lbg_context_freeListHead,
            index);
    return ((north_lbg_ctx_t*) p);
}

/*
 ** Procedures to set rechain_when_lbg_gets_down indicator
 * @param  idx  index of the load balancing group
 */
void north_lbg_rechain_when_lbg_gets_down(int idx) {
    north_lbg_ctx_t * lbg;

    lbg = north_lbg_getObjCtx_p(idx);
    if (lbg == NULL) return;

    lbg->rechain_when_lbg_gets_down = 1;
}
/*
 **_____________________________________________
 */

/**
 based on the object index, that function
 returns the pointer to the object context.

 That function may fails if the index is
 not a Transaction context index type.

@param     : af_unix socket context index
@retval   :-1 out of range
 */

uint32_t north_lbg_getObjCtx_ref(north_lbg_ctx_t *p) {
    uint32_t index;
    index = (uint32_t) (p - north_lbg_context_pfirst);
    //  index = index/sizeof(north_lbg_ctx_t);
    index -= 1;

    if (index >= north_lbg_context_count) {
        /*
         ** the MS index is out of range
         */
        severe( "north_lbg_getObjCtx_p(%d): index is out of range, index max is %d", index, north_lbg_context_count );
        return (uint32_t) - 1;
    }
    ;
    return index;
}




/*
 **____________________________________________________
 */

/**
   north_lbg_init

  initialize the Transaction management module

@param     : NONE
@retval   none   :
 */
void north_lbg_init() {
    north_lbg_context_pfirst = (north_lbg_ctx_t*) NULL;

    north_lbg_context_allocated = 0;
    north_lbg_context_count = 0;
}

/**
 *  init of a load balancer entry

  @param entry_p: pointer to the load balancer entry
  @param index: relative index within the parent load balancer
  @param parent : pointer to the parent north load balancer

  @retval none
 */
void north_lbg_entry_init(void *parent, north_lbg_entry_ctx_t *entry_p, uint32_t index) {

    ruc_listEltInit((ruc_obj_desc_t*) entry_p);

    entry_p->index = index;
    entry_p->free = TRUE;
    entry_p->sock_ctx_ref = -1;
    entry_p->last_reconnect_time = 0;
    entry_p->state = NORTH_LBG_DEPENDENCY;
    memset(&entry_p->stats, 0, sizeof (north_lbg_stats_t));
    entry_p->parent = parent;
    ruc_listEltInit((ruc_obj_desc_t *) & entry_p->rpc_guard_timer);
    ruc_listHdrInit((ruc_obj_desc_t *) & entry_p->xmitList);

}
/*
 **____________________________________________________
 */

/**
   north_lbg_ctxInit

  create the transaction context pool

@param     : pointer to the Transaction context
@retval   : none
 */
void north_lbg_ctxInit(north_lbg_ctx_t *p, uint8_t creation) {
    int i;

    p->family = -1; /**< identifier of the socket family    */
    p->name[0] = 0;

    p->nb_entries_conf = 0; /* number of configured entries  */
    p->nb_active_entries = 0;
    p->next_entry_idx = 0;
    
    p->next_global_entry_idx_p = NULL;

    p->state = NORTH_LBG_DOWN;
    p->userPollingCallBack = NULL;
    p->available_state = 1;
    memset(&p->stats, 0, sizeof (north_lbg_stats_t));

    p->rechain_when_lbg_gets_down = 0;
    p->active_lbg_entry = -1;
    p->active_standby_mode = 0;
    p->local = 0;

    /*
     ** clear the state bitmap
     */
    for (i = 0; i < NORTH__LBG_TB_MAX_ENTRY; i++) p->entry_bitmap_state[i] = 0;


    for (i = 0; i < NORTH_LBG_MAX_PRIO; i++) {
        ruc_listHdrInit((ruc_obj_desc_t *) & p->xmitList[i]);
    }
    /*
     ** init of the context
     */
    for (i = 0; i < NORTH__LBG_MAX_ENTRY; i++) north_lbg_entry_init(p, &p->entry_tb[i], (uint32_t) i);
}

/*-----------------------------------------------
 **   north_lbg_alloc

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
north_lbg_ctx_t *north_lbg_alloc() {
    north_lbg_ctx_t *p;

    /*
     **  Get the first free context
     */
    if ((p = (north_lbg_ctx_t*) ruc_objGetFirst((ruc_obj_desc_t*) north_lbg_context_freeListHead))
            == (north_lbg_ctx_t*) NULL) {
        /*
         ** out of Transaction context descriptor try to free some MS
         ** context that are out of date 
         */
        severe( "NOT ABLE TO GET an AF_UNIX CONTEXT" );
        return NULL;
    }
    /*
     **  reinitilisation of the context
     */
    north_lbg_ctxInit(p, FALSE);
    /*
     ** remove it for the linked list
     */
    north_lbg_context_allocated++;
    p->free = FALSE;


    ruc_objRemove((ruc_obj_desc_t*) p);
    /*
     ** insert in the active list the new element created
     */
    ruc_objInsertTail((ruc_obj_desc_t*) & north_lbg_context_activeListHead, (ruc_obj_desc_t*) p);
    return p;
}
/*
 **____________________________________________________
 */

/**
   north_lbg_createIndex

  create a AF UNIX context given by index 
   That function tries to allocate a free PDP
   context. In case of success, it returns the
   index of the Transaction context.

@param     : north_lbg_ctx_id is the reference of the context
@retval   : MS controller reference (if OK)
retval     -1 if out of context.
 */
uint32_t north_lbg_createIndex(uint32_t north_lbg_ctx_id) {
    north_lbg_ctx_t *p;

    /*
     **  Get the first free context
     */
    p = north_lbg_getObjCtx_p(north_lbg_ctx_id);
    if (p == NULL) {
        severe( "MS ref out of range: %u", north_lbg_ctx_id );
        return RUC_NOK;
    }
    /*
     ** return an error if the context is not free
     */
    if (p->free == FALSE) {
        severe( "the context is not free : %u", north_lbg_ctx_id );
        return RUC_NOK;
    }
    /*
     **  reinitilisation of the context
     */
    north_lbg_ctxInit(p, FALSE);
    /*
     ** remove it for the linked list
     */
    north_lbg_context_allocated++;


    p->free = FALSE;
    ruc_objRemove((ruc_obj_desc_t*) p);

    return RUC_OK;
}


/*
 **____________________________________________________
 */

/**
   delete a load balancer
   
   That function is intended to be called when
   a Transaction context is deleted. It returns the
   Transaction context to the free list. The delete
   procedure of the MS automaton and
   controller are called by that service.

   If the Transaction context is out of limit, and 
   error is returned.

@param     : load balancer object index
@retval   : RUC_OK : context has been deleted
@retval     RUC_NOK : out of limit index.
 */
uint32_t north_lbg_free_from_idx(uint32_t north_lbg_ctx_id) {
    north_lbg_ctx_t *p;

    if (north_lbg_ctx_id >= north_lbg_context_count) {
        /*
         ** index is out of limits
         */
        return RUC_NOK;
    }
    /*
     ** get the reference from idx
     */
    p = north_lbg_getObjCtx_p(north_lbg_ctx_id);
    /*
     **  remove the xmit block
     */
    //   ruc_objRemove((ruc_obj_desc_t *)&p->xmitCtx);

    /*
     ** remove it from the active list
     */
    ruc_objRemove((ruc_obj_desc_t*) p);
    p->state = NORTH_LBG_DEPENDENCY;

    /*
     **  insert it in the free list
     */
    north_lbg_context_allocated--;


    p->free = TRUE;
    ruc_objInsertTail((ruc_obj_desc_t*) north_lbg_context_freeListHead,
            (ruc_obj_desc_t*) p);

    return RUC_OK;

}
/*
 **____________________________________________________
 */

/**
   north_lbg_free_from_ptr

   delete a load balancer context


@param     : pointer to the transaction context
@retval   : RUC_OK : context has been deleted
@retval     RUC_NOK : out of limit index.

 */
uint32_t north_lbg_free_from_ptr(north_lbg_ctx_t *p) {
    uint32_t north_lbg_ctx_id;

    north_lbg_ctx_id = north_lbg_getObjCtx_ref(p);
    if (north_lbg_ctx_id == (uint32_t) - 1) {
        return RUC_NOK;
    }
    return (north_lbg_free_from_idx(north_lbg_ctx_id));

}




/*
 **____________________________________________________
 */

/**
   north_lbg_module_init

  create the Transaction context pool

@param     : north_lbg_ctx_count  : number of Transaction context


@retval   : RUC_OK : done
@retval          RUC_NOK : out of memory
 */
uint32_t north_lbg_module_init(uint32_t north_lbg_ctx_count) {
    north_lbg_ctx_t *p;
    uint32_t idxCur;
    ruc_obj_desc_t *pnext;
    uint32_t ret = RUC_OK;




    north_lbg_context_allocated = 0;
    north_lbg_context_count = north_lbg_ctx_count;

    north_lbg_context_freeListHead = (north_lbg_ctx_t*) NULL;

    /*
     **  create the active list
     */
    ruc_listHdrInit((ruc_obj_desc_t*) & north_lbg_context_activeListHead);

    /*
     ** create the af unix context pool
     */
    north_lbg_context_freeListHead = (north_lbg_ctx_t*) ruc_listCreate(north_lbg_ctx_count, sizeof (north_lbg_ctx_t));
    if (north_lbg_context_freeListHead == (north_lbg_ctx_t*) NULL) {
        /* 
         **  out of memory
         */

        RUC_WARNING(north_lbg_ctx_count * sizeof (north_lbg_ctx_t));
        return RUC_NOK;
    }
    /*
     ** store the pointer to the first context
     */
    north_lbg_context_pfirst = north_lbg_context_freeListHead;

    /*
     **  initialize each entry of the free list
     */
    idxCur = 0;
    pnext = (ruc_obj_desc_t*) NULL;
    while ((p = (north_lbg_ctx_t*) ruc_objGetNext((ruc_obj_desc_t*) north_lbg_context_freeListHead,
            &pnext))
            != (north_lbg_ctx_t*) NULL) {

        p->index = idxCur;
        p->free = TRUE;
        north_lbg_ctxInit(p, TRUE);
        p->state = NORTH_LBG_DEPENDENCY;
        idxCur++;
    }

    north_lbg_debug_init();
    /*
     ** timer mode init
     */
    north_lbg_tmr_init(200, 15);


    return ret;
}
