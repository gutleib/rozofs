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

#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/ruc_buffer_api.h>
#include <rozofs/core/ruc_buffer_debug.h>

#include "rozofs_storcli.h"
#include <rozofs/rozofs_srv.h>
#include <rozofs/rdma/rozofs_rdma.h>
#include "rozofs_storcli_sharedmem.h"

rozofs_storcli_ctx_t *rozofs_storcli_ctx_freeListHead;  /**< head of list of the free context  */
rozofs_storcli_ctx_t rozofs_storcli_ctx_activeListHead;  /**< list of the active context     */

uint32_t    rozofs_storcli_ctx_count;          /**< Max number of contexts                                */
uint32_t    rozofs_storcli_ctx_allocated;      /**< current number of allocated context                   */
int         rozofs_storcli_ctx_rsvd = 0;       /**< current number of pre-reserved storcli contexts       */
rozofs_storcli_ctx_t *rozofs_storcli_ctx_pfirst;  /**< pointer to the first context of the pool */
uint64_t  rozofs_storcli_global_object_index = 0;

uint64_t storcli_hash_lookup_hit_count = 0;
uint32_t storcli_serialization_forced = 0;     /**< assert to 1 to force serialisation for all request whitout taking care of the fid */
uint64_t storcli_buf_depletion_count = 0; /**< buffer depletion on storcli buffers */
uint64_t storcli_rng_full_count = 0; /**< ring request full counter */
uint64_t rozofs_storcli_ctx_wrap_count_err = 0;
/*
** Table should probably be allocated 
** with a length depending on the number of entry given at nfs_lbg_cache_ctx_init
*/
ruc_obj_desc_t storcli_hash_table[STORCLI_HASH_SIZE];

uint64_t rozofs_storcli_stats[ROZOFS_STORCLI_COUNTER_MAX];


/**
* Buffers information
*/
int rozofs_storcli_north_small_buf_count= 0;
int rozofs_storcli_north_small_buf_sz= 0;
int rozofs_storcli_north_large_buf_count= 0;
int rozofs_storcli_north_large_buf_sz= 0;
int rozofs_storcli_south_small_buf_count= 0;
int rozofs_storcli_south_small_buf_sz= 0;
int rozofs_storcli_south_large_buf_count= 0;
int rozofs_storcli_south_large_buf_sz= 0;

void *rozofs_storcli_pool[_ROZOFS_STORCLI_MAX_POOL];

uint32_t rozofs_storcli_seqnum = 1;


#define MICROLONG(time) ((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)
#define ROZOFS_STORCLI_DEBUG_TOPIC      "storcli_buf"

/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void rozofs_storcli_debug_show(uint32_t tcpRef, void *bufRef) {
  char           *pChar=uma_dbg_get_buffer();

  pChar += sprintf(pChar,"number of transaction contexts (initial/allocated/reserved) : %u/%u/%d\n",rozofs_storcli_ctx_count,
                                                                                                    rozofs_storcli_ctx_allocated,
												    rozofs_storcli_ctx_rsvd);
  pChar += sprintf(pChar,"Statistics\n");
//  pChar += sprintf(pChar,"req serialized : %10llu\n",(unsigned long long int)storcli_hash_lookup_hit_count);
//  pChar += sprintf(pChar,"serialize mode : %s\n",(storcli_serialization_forced==0)?"NORMAL":"FORCED");
  pChar += sprintf(pChar,"rsvd wrap error: %llu\n",(unsigned long long int)rozofs_storcli_ctx_wrap_count_err);
  pChar += sprintf(pChar,"serialize mode : %s\n",(stc_rng_serialize==0)?"NORMAL":"FORCED");
  pChar += sprintf(pChar,"req submit/coll: %10llu/%llu\n",
                   (unsigned long long int)stc_rng_submit_count,
                   (unsigned long long int)stc_rng_collision_count);
  pChar += sprintf(pChar,"FID in parallel: %10llu\n",(unsigned long long int)stc_rng_parallel_count);
  pChar += sprintf(pChar,"buf. depletion : %10llu\n",(unsigned long long int)storcli_buf_depletion_count);
  pChar += sprintf(pChar,"ring full      : %10llu\n",(unsigned long long int)storcli_rng_full_count);
  pChar += sprintf(pChar,"ring hash coll : %10llu\n",(unsigned long long int)stc_rng_hash_collision_count);
  pChar += sprintf(pChar,"SEND           : %10llu\n",(unsigned long long int)rozofs_storcli_stats[ROZOFS_STORCLI_SEND]);  
  pChar += sprintf(pChar,"SEND_ERR       : %10llu\n",(unsigned long long int)rozofs_storcli_stats[ROZOFS_STORCLI_SEND_ERROR]);  
  pChar += sprintf(pChar,"RECV_OK        : %10llu\n",(unsigned long long int)rozofs_storcli_stats[ROZOFS_STORCLI_RECV_OK]);  
  pChar += sprintf(pChar,"RECV_OUT_SEQ   : %10llu\n",(unsigned long long int)rozofs_storcli_stats[ROZOFS_STORCLI_RECV_OUT_SEQ]);  
  pChar += sprintf(pChar,"RTIMEOUT       : %10llu\n",(unsigned long long int)rozofs_storcli_stats[ROZOFS_STORCLI_TIMEOUT]);  
  pChar += sprintf(pChar,"EMPTY READ     : %10llu\n",(unsigned long long int)rozofs_storcli_stats[ROZOFS_STORCLI_EMPTY_READ]);  
  pChar += sprintf(pChar,"EMPTY WRITE    : %10llu\n",(unsigned long long int)rozofs_storcli_stats[ROZOFS_STORCLI_EMPTY_WRITE]);
  rozofs_storcli_stats[ROZOFS_STORCLI_EMPTY_READ] = 0;
  rozofs_storcli_stats[ROZOFS_STORCLI_EMPTY_WRITE] = 0;  
  pChar += sprintf(pChar,"\n");
  pChar += sprintf(pChar,"Buffer Pool (name[size] :initial/current\n");
  pChar += sprintf(pChar,"North interface Buffers            \n");  
  pChar += sprintf(pChar,"  small[%6d]  : %6d/%d\n",rozofs_storcli_north_small_buf_sz,rozofs_storcli_north_small_buf_count,
                                                         ruc_buf_getFreeBufferCount(ROZOFS_STORCLI_NORTH_SMALL_POOL)); 
  pChar += sprintf(pChar,"  large[%6d]  : %6d/%d\n",rozofs_storcli_north_large_buf_sz,rozofs_storcli_north_large_buf_count, 
                                                         ruc_buf_getFreeBufferCount(ROZOFS_STORCLI_NORTH_LARGE_POOL)); 
  pChar += sprintf(pChar,"South interface Buffers            \n");  
  pChar += sprintf(pChar,"  small[%6d]  : %6d/%d\n",rozofs_storcli_south_small_buf_sz,rozofs_storcli_south_small_buf_count, 
                                                         ruc_buf_getFreeBufferCount(ROZOFS_STORCLI_SOUTH_SMALL_POOL)); 
  pChar += sprintf(pChar,"  large[%6d]  : %6d/%d\n",rozofs_storcli_south_large_buf_sz,rozofs_storcli_south_large_buf_count,
                                                         ruc_buf_getFreeBufferCount(ROZOFS_STORCLI_SOUTH_LARGE_POOL)); 
  if (bufRef != NULL) uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());
  else printf("%s",uma_dbg_get_buffer());

}

/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS:
  -
  RETURN: none
  ==========================================================================*/
static char * rozofs_storcli_debug_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"storcli_buf             : display statistics\n");
  pChar += sprintf(pChar,"storcli_buf serialize   : serialize the requests for the same FID \n");
  pChar += sprintf(pChar,"storcli_buf parallel    : process in parallel the requests for the same FID \n");
  return pChar;
}

/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void rozofs_storcli_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    if (argv[1] != NULL)
    {
      if (strcmp(argv[1],"serialize")==0) {
        stc_rng_serialize = 1;
        uma_dbg_send(tcpRef, bufRef, TRUE, "requests are serialized\n");
        return;

      }
      if (strcmp(argv[1],"parallel")==0) {
        stc_rng_serialize = 0;
        uma_dbg_send(tcpRef, bufRef, TRUE, "requests are processed in parallel\n");
        return;

      }
      if (strcmp(argv[1],"reset")==0) {
        stc_rng_submit_count = 0;
        stc_rng_parallel_count = 0;
        stc_rng_collision_count = 0;
        storcli_rng_full_count = 0;
        storcli_buf_depletion_count = 0;
        uma_dbg_send(tcpRef, bufRef, TRUE, "stats scheduler reset done\n");
        return;

      }
      if (strcmp(argv[1],"?")==0) {
        rozofs_storcli_debug_help(pChar);
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
        return;
      }
      rozofs_storcli_debug_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
      return;
    }
  rozofs_storcli_debug_show(tcpRef,bufRef);
}


/*__________________________________________________________________________
  Register to the debug SWBB
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
void rozofs_storcli_debug_init() {
  uma_dbg_addTopic_option(ROZOFS_STORCLI_DEBUG_TOPIC, rozofs_storcli_debug,UMA_DBG_OPTION_RESET); 
}


/*
**  END OF DEBUG
*/
/*
*________________________________________________________
*/
/**
    Get the pointer to the ring entry located in the storcli working context
    
    @param p: pointer to the working context
    
    @retval pointer to the ring entry 
*/
void *stc_rng_get_entry_from_obj_ctx(void *p)
{
   rozofs_storcli_ctx_t *bid_p = (rozofs_storcli_ctx_t*)p;
   return &bid_p->ring;
}


static inline int fid_cmp(void *key1, void *key2) {
    return memcmp(key1, key2, sizeof (fid_t));
}

static unsigned int fid_hash(void *key) {
    uint32_t hash = 0;
    uint8_t *c;
    for (c = key; c != key + 16; c++)
        hash = *c + (hash << 6) + (hash << 16) - hash;
    return hash;
}

/*
*________________________________________________________
*/
/**
  Search for a call context with the xid as a key

  @param fid: file id to search
   
  @retval <>NULL pointer to searched context
  @retval NULL context is not found
*/
rozofs_storcli_ctx_t *storcli_hash_table_search_ctx(fid_t fid)
{
   unsigned int       hashIdx;
   ruc_obj_desc_t   * phead;
   ruc_obj_desc_t   * elt;
   ruc_obj_desc_t   * pnext;
   rozofs_storcli_ctx_t  * p;
   
   /*
   *  Compute the hash from the file handle
   */

   hashIdx = fid_hash((void*)fid);
   hashIdx = hashIdx%STORCLI_HASH_SIZE;   
   if (storcli_serialization_forced)
   {
     hashIdx = 0;
   } 
   /*
   ** Get the head of list
   */
   phead = &storcli_hash_table[hashIdx];   
   pnext = (ruc_obj_desc_t*)NULL;
   while ((elt = ruc_objGetNext(phead, &pnext)) != NULL) 
   {
      p = (rozofs_storcli_ctx_t*) elt;  
      if (storcli_serialization_forced)
      {
        storcli_hash_lookup_hit_count++;
        return p;
      }    
      /*
      ** Check fid value
      */      
      if (memcmp(p->fid_key, fid, sizeof (fid_t)) == 0) 
      {
        /* 
        ** This is our guy. Refresh this entry now
        */
        storcli_hash_lookup_hit_count++;
        return p;
      }      
   } 
//   nfs_lbg_cache_stats_table.lookup_miss_count++;
   return NULL;
}

/*
*________________________________________________________
*/
/**
  Insert the current request context has the end of its
  associated hash queue.
  That context must be removed from
  that list at the end of the processing of the request
  If there is some pendong request on the same hash queue
  the system must take the first one of the queue and
  activate the processing of that request.
  By construction, the system does not accept more that
  one operation on the same fid (read/write or truncate
  

  @param ctx_p: pointer to the context to insert
   
 
  @retval none
*/
void storcli_hash_table_insert_ctx(rozofs_storcli_ctx_t *ctx_p)
{
   unsigned int       hashIdx;
   ruc_obj_desc_t   * phead;   
   /*
   *  Compute the hash from the file handle
   */
   hashIdx = fid_hash((void*)ctx_p->fid_key);
   hashIdx = hashIdx%STORCLI_HASH_SIZE;   
   if (storcli_serialization_forced)
   {
     hashIdx = 0;
   } 
   /*
   ** Get the head of list and insert the context at the tail of the queue
   */
   phead = &storcli_hash_table[hashIdx];  
   ruc_objInsertTail(phead,(ruc_obj_desc_t*)ctx_p);
}

#if 0
/*
*________________________________________________________
*/
/**
  remove the request provided as input argument from
  its hash queue and return the first pending
  request from that hash queue if there is any.


  @param ctx_p: pointer to the context to remove
   
 
  @retval NULL:  the hash queue is empty
  @retval <>NULL pointer to the first request that was pending on that queue
*/
rozofs_storcli_ctx_t *storcli_hash_table_remove_ctx(rozofs_storcli_ctx_t *ctx_p)
{
   unsigned int       hashIdx;
   ruc_obj_desc_t   * phead;   
   rozofs_storcli_ctx_t  * p = NULL;

   /*
   *  Compute the hash from the file handle
   */
   hashIdx = fid_hash((void*)ctx_p->fid_key);
   hashIdx = hashIdx%STORCLI_HASH_SIZE;   
   /*
   ** remove the context from the hash queue list
   */
   ruc_objRemove((ruc_obj_desc_t*)ctx_p);
   /*
   ** search the next request with the same fid
   */   
   p = storcli_hash_table_search_ctx(ctx_p->fid_key);

   return p;
}
#endif

/*-----------------------------------------------
**   rozofs_storcli_getObjCtx_p

** based on the object index, that function
** returns the pointer to the object context.
**
** That function may fails if the index is
** not a Transaction context index type.
**
@param     : MS index
@retval   : NULL if error

*/

rozofs_storcli_ctx_t *rozofs_storcli_getObjCtx_p(uint32_t object_index)
{
   uint32_t index;
   rozofs_storcli_ctx_t *p;

   /*
   **  Get the pointer to the context
   */
   index = object_index & RUC_OBJ_MASK_OBJ_IDX; 
   if ( index >= rozofs_storcli_ctx_count)
   {
      /*
      ** the MS index is out of range
      */
      severe( "rozofs_storcli_getObjCtx_p(%d): index is out of range, index max is %d",index,rozofs_storcli_ctx_count );   
     return (rozofs_storcli_ctx_t*)NULL;
   }
   p = (rozofs_storcli_ctx_t*)ruc_objGetRefFromIdx((ruc_obj_desc_t*)rozofs_storcli_ctx_freeListHead,
                                       index);
   return ((rozofs_storcli_ctx_t*)p);
}

/*-----------------------------------------------
**   rozofs_storcli_getObjCtx_ref

** based on the object index, that function
** returns the pointer to the object context.
**
** That function may fails if the index is
** not a Transaction context index type.
**
@param     : MS index
@retval   :-1 out of range

*/

uint32_t rozofs_storcli_getObjCtx_ref(rozofs_storcli_ctx_t *p)
{
   uint32_t index;
   index = (uint32_t) ( p - rozofs_storcli_ctx_pfirst);
   index = index/sizeof(rozofs_storcli_ctx_t);

   if ( index >= rozofs_storcli_ctx_count)
   {
      /*
      ** the MS index is out of range
      */
      severe( "rozofs_storcli_getObjCtx_p(%d): index is out of range, index max is %d",index,rozofs_storcli_ctx_count );   
     return (uint32_t) -1;
   }
;
   return index;
}




/*
**____________________________________________________
*/
/**
   rozofs_storcli_init

  initialize the Transaction management module

@param     : NONE
@retval   none   :
*/
void rozofs_storcli_init()
{   
   rozofs_storcli_ctx_pfirst = (rozofs_storcli_ctx_t*)NULL;

   rozofs_storcli_ctx_allocated = 0;
   rozofs_storcli_ctx_count = 0;
}

/*
**____________________________________________________
*/
/**
   rozofs_storcli_rsvd_context_alloc

  Pre-reserve a set of storcli context

@param     p: main storcli context
@param     count: number of context to reserve
@retval   : none
*/
void  rozofs_storcli_rsvd_context_alloc(rozofs_storcli_ctx_t *p,int count)
{
    p->rsvd_ctx_count = count;
    rozofs_storcli_ctx_rsvd +=count;
}

/*
**____________________________________________________
*/
/**
   rozofs_storcli_rsvd_context_release

  Pre-reserve a set of storcli context

@param     p: main storcli context
@param     count: number of context to release
@retval   : none
*/
void  rozofs_storcli_rsvd_context_release(rozofs_storcli_ctx_t *p)
{
    if (p->rsvd_ctx_count == 0) return;
    rozofs_storcli_ctx_rsvd -=p->rsvd_ctx_count;
    p->rsvd_ctx_count = 0;
    if (rozofs_storcli_ctx_rsvd < 0) rozofs_storcli_ctx_rsvd = 0;
}

/*
**____________________________________________________
*/
/**
   rozofs_storcli_ctxInit

  create the transaction context pool

@param     : pointer to the Transaction context
@retval   : none
*/
void  rozofs_storcli_ctxInit(rozofs_storcli_ctx_t *p,uint8_t creation)
{

  p->integrity  = -1;     /* the value of this field is incremented at 
					      each MS ctx allocation */
                          
  p->recv_buf     = NULL;
  p->socketRef    = -1;
//  p->read_rq_p    = NULL;
//  p->write_rq_p = NULL;

  p->xmitBuf     = NULL;
  p->data_read_p = NULL;
  p->data_read_p = 0;
  memset(p->prj_ctx,0,sizeof(rozofs_storcli_projection_ctx_t)*ROZOFS_SAFE_MAX_STORCLI);
  /*
  ** clear the array that contains the association between projection_id and load balancing group
  */
  rozofs_storcli_lbg_prj_clear(p->lbg_assoc_tb);
  /*
  ** working variables for read
  */
  p->cur_nmbs2read = 0;
  p->cur_nmbs = 0;
  p->nb_projections2read = 0;
  p->redundancyStorageIdxCur = 0;
  p->redundancyStorageIdxCur = 0;
  p->read_seqnum    = 0;
  p->reply_done     = 0;
  p->write_ctx_lock = 0;
  p->read_ctx_lock  = 0;
  p->enoent_count   = 0;
  memset(p->fid_key,0, sizeof (sp_uuid_t));
  /*
   ** clear the scheduler idx: -1 indicates that the entry is not present
   ** in the request scheduler table
   */
  p->sched_idx = -1;
  p->rsvd_ctx_count= 0;
  
  p->opcode_key = STORCLI_NULL;
   p->shared_mem_p = NULL;

   /*
   ** timer cell
   */
  ruc_listEltInitAssoc((ruc_obj_desc_t *)&p->timer_list,p);
  
  p->traceSize = 0;
  memset(p->traceBuffer,0,sizeof(p->traceBuffer));
}

/*
**__________________________________________________________________________
*/
/**
  allocate a  context to handle a client read/write transaction

  @param     : none
  @retval <>NULL pointer to the allocated context
  @retval NULL out of free context
*/
rozofs_storcli_ctx_t *rozofs_storcli_alloc_context()
{
   rozofs_storcli_ctx_t *p;

   /*
   **  Get the first free context
   */
   if ((p =(rozofs_storcli_ctx_t*)ruc_objGetFirst((ruc_obj_desc_t*)rozofs_storcli_ctx_freeListHead))
           == (rozofs_storcli_ctx_t*)NULL)
   {
     /*
     ** out of Transaction context descriptor try to free some MS
     ** context that are out of date 
     */
     severe( "not able to get a tx context" );
     return NULL;
   }
   /*
   **  reinitilisation of the context
   */
   rozofs_storcli_ctxInit(p,FALSE);   
   /*
   ** remove it for the linked list
   */
   rozofs_storcli_ctx_allocated++;
   p->free = FALSE;
   p->read_seqnum = 0;
  
   
   ruc_objRemove((ruc_obj_desc_t*)p);
 
   return p;
}

/*
**__________________________________________________________________________
*/
/**
* release a read/write context that has been use for either a read or write operation

  @param : pointer to the context
  
  @retval <>NULL pointer to the allocated context
  @retval NULL out of free context
*/
void rozofs_storcli_release_context(rozofs_storcli_ctx_t *ctx_p)
{
  int i;
  int inuse;  
  
  /*
  ** Remove the context from the timer list
  */
  rozofs_storcli_stop_read_guard_timer(ctx_p);
  /*
  ** release the buffer that was carrying the initial request
  */
  if (ctx_p->recv_buf != NULL) 
  {
    ruc_buf_freeBuffer(ctx_p->recv_buf);
    ctx_p->recv_buf = NULL;
  }
  ctx_p->socketRef = -1;
  if (ctx_p->xmitBuf != NULL) 
  {
    ruc_buf_freeBuffer(ctx_p->xmitBuf);
    ctx_p->xmitBuf = NULL;
  }
  /*
  ** check if there is some buffer to release in the projection context
  */
  for (i = 0; i < ROZOFS_SAFE_MAX_STORCLI ; i++)
  {
    if (ctx_p->prj_ctx[i].prj_buf != NULL)  
    {
      if (ctx_p->prj_ctx[i].inuse_valid == 1)
      {
        inuse = ruc_buf_inuse_decrement(ctx_p->prj_ctx[i].prj_buf);
        if(inuse == 1) 
        {
          ruc_objRemove((ruc_obj_desc_t*)ctx_p->prj_ctx[i].prj_buf);
          ruc_buf_freeBuffer(ctx_p->prj_ctx[i].prj_buf);
        }
      }
      else
      {
        inuse = ruc_buf_inuse_get(ctx_p->prj_ctx[i].prj_buf);
        if (inuse == 1) 
        {
          ruc_objRemove((ruc_obj_desc_t*)ctx_p->prj_ctx[i].prj_buf);
          ruc_buf_freeBuffer(ctx_p->prj_ctx[i].prj_buf);
        }      
      }
      ctx_p->prj_ctx[i].prj_buf = NULL;
    }
    /*
    ** case of the buffer used when missing response condition is encountered
    */
    if (ctx_p->prj_ctx[i].prj_buf_missing != NULL)  
    {
      if (ctx_p->prj_ctx[i].inuse_valid_missing == 1)
      {
        inuse = ruc_buf_inuse_decrement(ctx_p->prj_ctx[i].prj_buf_missing);
        if(inuse == 1) 
        {
          ruc_objRemove((ruc_obj_desc_t*)ctx_p->prj_ctx[i].prj_buf_missing);
          ruc_buf_freeBuffer(ctx_p->prj_ctx[i].prj_buf_missing);
        }
      }
      else
      {
        inuse = ruc_buf_inuse_get(ctx_p->prj_ctx[i].prj_buf_missing);
        if (inuse == 1) 
        {
          ruc_objRemove((ruc_obj_desc_t*)ctx_p->prj_ctx[i].prj_buf_missing);
          ruc_buf_freeBuffer(ctx_p->prj_ctx[i].prj_buf_missing);
        }      
      }
      ctx_p->prj_ctx[i].prj_buf_missing = NULL;
    }
  }

  /*
  ** remove any buffer that has been allocated for reading in the case of the write
  */
  {  
    rozofs_storcli_ingress_write_buf_t  *wr_proj_buf_p = ctx_p->wr_proj_buf;
    for (i = 0; i < ROZOFS_WR_MAX ; i++,wr_proj_buf_p++)
    {
    
      wr_proj_buf_p->transaction_id = 0;
      wr_proj_buf_p->state          = ROZOFS_WR_ST_IDLE;
      wr_proj_buf_p->data           = NULL;            
      if (wr_proj_buf_p->read_buf != NULL)  ruc_buf_freeBuffer(wr_proj_buf_p->read_buf);
      wr_proj_buf_p->read_buf       = NULL;
    
    }
  }
  /*
  ** remove it from any other list and re-insert it on the free list
  */
  ruc_objRemove((ruc_obj_desc_t*) ctx_p);
   
   /*
   **  insert it in the free list
   */
   rozofs_storcli_ctx_allocated--;
   if (ctx_p->rsvd_ctx_count != 0) rozofs_storcli_rsvd_context_release(ctx_p);
   /*
   ** check the lock
   */
   if (ctx_p->write_ctx_lock != 0)
   {
    severe("bad write_ctx_lock value %d",ctx_p->write_ctx_lock);
   
   }
 
    if (ctx_p->read_ctx_lock != 0)
   {
    severe("bad read_ctx_lock value %d",ctx_p->read_ctx_lock);   
   }  
   ctx_p->free = TRUE;
   ctx_p->read_seqnum = 0;
   ruc_objInsert((ruc_obj_desc_t*)rozofs_storcli_ctx_freeListHead,
                     (ruc_obj_desc_t*) ctx_p);
                     
   /*
   ** check if there is request with the same fid that is waiting for execution
   */
   {

       rozofs_storcli_ctx_t *next_p;
       uint8_t opcode;

       if (ctx_p->sched_idx == -1) return;
       stc_rng_release_entry(ctx_p->sched_idx,(void**) &next_p,&opcode,&ctx_p->ring);
       if ( next_p != NULL)
       {
           switch (next_p->opcode_key)
           {
           case STORCLI_READ:
               rozofs_storcli_read_req_processing(next_p);
               return;
           case STORCLI_WRITE:
               rozofs_storcli_write_req_processing_exec(next_p);
               return;
           case STORCLI_TRUNCATE:
               rozofs_storcli_truncate_req_processing(next_p);
               return;
           case STORCLI_DELETE:
               rozofs_storcli_delete_req_processing(next_p);
               return;
           default:
               return;
           }
       }
   }
}


#if 0 // Not used
/*
**____________________________________________________
*/
/*
    Timeout call back associated with a transaction

@param     :  tx_p : pointer to the transaction context
*/

void rozofs_storcli_timeout_CBK (void *opaque)
{
  rozofs_storcli_ctx_t *pObj = (rozofs_storcli_ctx_t*)opaque;
  pObj->rpc_guard_timer_flg = TRUE;
  /*
  ** Process the current time-out for that transaction
  */
  
//  uma_fsm_engine(pObj,&pObj->resumeFsm);
   pObj->status = -1;
   pObj->tx_errno  =  ETIME;
   /*
   ** Update global statistics
   */
       TX_STATS(ROZOFS_TX_TIMEOUT);

       (*(pObj->recv_cbk))(pObj,pObj->user_param);
}

/*
**____________________________________________________
*/
/*
  stop the guard timer associated with the transaction

@param     :  tx_p : pointer to the transaction context
@retval   : none
*/

void rozofs_storcli_stop_timer(rozofs_storcli_ctx_t *pObj)
{
 
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
void rozofs_storcli_start_timer(rozofs_storcli_ctx_t *tx_p,uint32_t time_ms) 
{
 uint8 slot;
  /*
  **  remove the timer from its current list
  */
  slot = COM_TX_TMR_SLOT0;

  tx_p->rpc_guard_timer_flg = FALSE;
  com_tx_tmr_stop(&tx_p->rpc_guard_timer);
  com_tx_tmr_start(slot,
                  &tx_p->rpc_guard_timer,
		  time_ms*1000,
                  rozofs_storcli_timeout_CBK,
		  (void*) tx_p);

}
#endif

/**
   rozofs_storcli_module_init

  create the Transaction context pool


@retval   : RUC_OK : done
@retval          RUC_NOK : out of memory
*/
uint32_t rozofs_storcli_module_init()
{
   rozofs_storcli_ctx_t *p;
   uint32_t idxCur;
   ruc_obj_desc_t *pnext;
   uint32_t ret = RUC_OK;
   
    rozofs_storcli_read_init_timer_module();
    int count = STORCLI_CTX_CNT*rozofs_get_rozofs_safe(conf.layout);
    int bufsize = ROZOFS_MAX_FILE_BUF_SZ_READ/rozofs_get_rozofs_inverse(conf.layout);
    /*
    ** add space for RPC encoding and projection headers
    */
//    bufsize +=(16*1024);
    bufsize = ROZOFS_MAX_BLOCK_PER_MSG*rozofs_get_max_psize_in_msg(conf.layout,0);
    /*
    ** add space for RPC header
    */
    bufsize+=4096;

    rozofs_storcli_north_small_buf_count  = STORCLI_NORTH_MOD_INTERNAL_READ_BUF_CNT ;
    rozofs_storcli_north_small_buf_sz     = STORCLI_NORTH_MOD_INTERNAL_READ_BUF_SZ    ;
    rozofs_storcli_north_large_buf_count  = STORCLI_NORTH_MOD_XMIT_BUF_CNT ;
    rozofs_storcli_north_large_buf_sz     = STORCLI_NORTH_MOD_XMIT_BUF_SZ    ;
    
    rozofs_storcli_south_small_buf_count  = count * 2;
    rozofs_storcli_south_small_buf_sz     = STORCLI_SOUTH_TX_XMIT_SMALL_BUF_SZ  ;
    rozofs_storcli_south_large_buf_count  = count   ;
    rozofs_storcli_south_large_buf_sz     = bufsize  ;  
   
   rozofs_storcli_ctx_allocated = 0;
   rozofs_storcli_ctx_count = STORCLI_CTX_CNT;
 
   rozofs_storcli_ctx_freeListHead = (rozofs_storcli_ctx_t*)NULL;

   /*
   **  create the active list
   */
   ruc_listHdrInit((ruc_obj_desc_t*)&rozofs_storcli_ctx_activeListHead);    

   /*
   ** create the Read/write Transaction context pool
   */
   rozofs_storcli_ctx_freeListHead = (rozofs_storcli_ctx_t*)ruc_listCreate(rozofs_storcli_ctx_count,sizeof(rozofs_storcli_ctx_t));
   if (rozofs_storcli_ctx_freeListHead == (rozofs_storcli_ctx_t*)NULL)
   {
     /* 
     **  out of memory
     */

     RUC_WARNING(rozofs_storcli_ctx_count*sizeof(rozofs_storcli_ctx_t));
     return RUC_NOK;
   }
   /*
   ** store the pointer to the first context
   */
   rozofs_storcli_ctx_pfirst = rozofs_storcli_ctx_freeListHead;

   /*
   **  initialize each entry of the free list
   */
   idxCur = 0;
   pnext = (ruc_obj_desc_t*)NULL;
   while ((p = (rozofs_storcli_ctx_t*)ruc_objGetNext((ruc_obj_desc_t*)rozofs_storcli_ctx_freeListHead,
                                        &pnext))
               !=(rozofs_storcli_ctx_t*)NULL) 
   {
  
      p->index = idxCur;
      p->free  = TRUE;
      rozofs_storcli_ctxInit(p,TRUE);
      idxCur++;
   } 

   /*
   ** Initialize the RESUME and SUSPEND timer module: 100 ms
   */
//   com_tx_tmr_init(100,15); 
   /*
   ** Clear the statistics counter
   */
   memset(rozofs_storcli_stats,0,sizeof(uint64_t)*ROZOFS_STORCLI_COUNTER_MAX);
   rozofs_storcli_debug_init();
      
   /*
   ** Initialie the cache table entries
   */
   {
     for (idxCur=0; idxCur<STORCLI_HASH_SIZE; idxCur++) 
     {
        ruc_listHdrInit(&storcli_hash_table[idxCur]);
     }
   }
   
   while(1)
   {
      rozofs_storcli_pool[_ROZOFS_STORCLI_NORTH_SMALL_POOL]= ruc_buf_poolCreate(rozofs_storcli_north_small_buf_count,rozofs_storcli_north_small_buf_sz);
      if (rozofs_storcli_pool[_ROZOFS_STORCLI_NORTH_SMALL_POOL] == NULL)
      {
         ret = RUC_NOK;
         severe( "xmit ruc_buf_poolCreate(%d,%d)", rozofs_storcli_north_small_buf_count, rozofs_storcli_north_small_buf_sz ); 
         break;
      }
      ruc_buffer_debug_register_pool("NorthSmall",rozofs_storcli_pool[_ROZOFS_STORCLI_NORTH_SMALL_POOL]);
      rozofs_storcli_pool[_ROZOFS_STORCLI_NORTH_LARGE_POOL] = ruc_buf_poolCreate(rozofs_storcli_north_large_buf_count,rozofs_storcli_north_large_buf_sz);
      if (rozofs_storcli_pool[_ROZOFS_STORCLI_NORTH_LARGE_POOL] == NULL)
      {
         ret = RUC_NOK;
         severe( "rcv ruc_buf_poolCreate(%d,%d)", rozofs_storcli_north_large_buf_count, rozofs_storcli_north_large_buf_sz ); 
	 break;
     }
      ruc_buffer_debug_register_pool("NorthLarge",rozofs_storcli_pool[_ROZOFS_STORCLI_NORTH_LARGE_POOL]);
      rozofs_storcli_pool[_ROZOFS_STORCLI_SOUTH_SMALL_POOL]= ruc_buf_poolCreate(rozofs_storcli_south_small_buf_count,rozofs_storcli_south_small_buf_sz);
      if (rozofs_storcli_pool[_ROZOFS_STORCLI_SOUTH_SMALL_POOL] == NULL)
      {
         ret = RUC_NOK;
         severe( "xmit ruc_buf_poolCreate(%d,%d)", rozofs_storcli_south_small_buf_count, rozofs_storcli_south_small_buf_sz ); 
         break;
      }
      ruc_buffer_debug_register_pool("SouthSmall",rozofs_storcli_pool[_ROZOFS_STORCLI_SOUTH_SMALL_POOL]);
      /*
      ** create the pool in shared memory if RozoFS operates in standalone mode
      */
//      if (common_config.standalone)
      {
         int key = (storcli_conf_p->rozofsmount_instance << 8) | storcli_conf_p->module_index;
     
      rozofs_storcli_pool[_ROZOFS_STORCLI_SOUTH_LARGE_POOL] = 
          rozofs_create_shared_memory(key,_ROZOFS_STORCLI_SOUTH_LARGE_POOL,rozofs_storcli_south_large_buf_count,rozofs_storcli_south_large_buf_sz,"SouthLarge");
      }
#if 0
      else
      {
        rozofs_storcli_pool[_ROZOFS_STORCLI_SOUTH_LARGE_POOL] = ruc_buf_poolCreate(rozofs_storcli_south_large_buf_count,rozofs_storcli_south_large_buf_sz);
	if (rozofs_storcli_pool[_ROZOFS_STORCLI_SOUTH_LARGE_POOL] != NULL)
	   ruc_buffer_debug_register_pool("SouthLarge",rozofs_storcli_pool[_ROZOFS_STORCLI_SOUTH_LARGE_POOL]); 
      }
#endif
      if (rozofs_storcli_pool[_ROZOFS_STORCLI_SOUTH_LARGE_POOL] == NULL)
      {
         ret = RUC_NOK;
         severe( "rcv ruc_buf_poolCreate(%d,%d)", rozofs_storcli_south_large_buf_count, rozofs_storcli_south_large_buf_sz ); 
	 break;
      }
           
#ifdef ROZOFS_RDMA
      /*
      ** registration with the RDMA module
      */
      {
	rozofs_rdma_memory_reg_t rdma_reg_ctx;

	rdma_reg_ctx.mem = ruc_buf_get_pool_base_and_length(rozofs_storcli_pool[_ROZOFS_STORCLI_SOUTH_LARGE_POOL],&rdma_reg_ctx.len);
	if (rdma_reg_ctx.mem != NULL)
	{
          rozofs_rdma_user_memory_register(&rdma_reg_ctx);
	}
      }
#endif
   break;
   }
   return ret;
}
