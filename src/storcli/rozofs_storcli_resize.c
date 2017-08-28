/*
  Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
  This file is part of Rozofs.

  Rozofs is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published
  by the Free Software Foundation, version 2.

  Rozofs is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without L1406even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see
  <http://www.gnu.org/licenses/>.
 */

/* need for crypt */
#define _XOPEN_SOURCE 500
#define FUSE_USE_VERSION 26

#include <fuse/fuse_lowlevel.h>
#include <fuse/fuse_opt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stddef.h>
#include <fcntl.h>
#include <unistd.h>
#include <getopt.h>
#include <pthread.h>
#include <assert.h>

#include <rozofs/rozofs.h>
#include "config.h"
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/core/rozofs_tx_common.h>
#include <rozofs/core/rozofs_tx_api.h>
#include "rozofs_storcli.h"
#include "storage_proto.h"
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/ruc_timer_api.h>
#include <rozofs/core/ruc_traffic_shaping.h>
#include <rozofs/rozofs_srv.h>
#include "rozofs_storcli_rpc.h"
#include <rozofs/rpc/sproto.h>
#include "storcli_main.h"
#include <rozofs/rozofs_timer_conf.h>
#include "rozofs_storcli_mojette_thread_intf.h"

DECLARE_PROFILING(stcpp_profiler_t);


extern uint64_t  corrupted_log_filter;

/*
** Buffer to trace read/write errors
*/
extern storcli_rw_err_t storcli_rw_error;


/*
**__________________________________________________________________________
*/
/**
* PROTOTYPES
*/


/**
* allocate a sequence number for the read. The sequence number is associated to
* the read context and is common to all the request concerning the projections of a particular set of distribution
 @retval sequence number
*/
extern uint32_t rozofs_storcli_allocate_read_seqnum();



/**
*  END PROTOTYPES
*/
/*
**__________________________________________________________________________
*/

/**
* Local prototypes
*/
void rozofs_storcli_resize_req_processing_cbk(void *this,void *param) ;
void rozofs_storcli_resize_req_processing(rozofs_storcli_ctx_t *working_ctx_p);



/*
**_________________________________________________________________________
*      LOCAL FUNCTIONS
**_________________________________________________________________________
*/



/*
**_________________________________________________________________________
*      PUBLIC FUNCTIONS
**_________________________________________________________________________
*/


/*
**__________________________________________________________________________
*/
/**
* The purpose of that function is to return TRUE if there is atlbg_in_distribution least one projection
   for which we expect a response from a storage
  
  @param layout : layout association with the file
  @param prj_cxt_p: pointer to the projection context (working array)
  
  @retval number of received projection
*/
static inline int rozofs_storcli_check_resize_in_progress_projections(uint8_t layout,rozofs_storcli_projection_ctx_t *prj_cxt_p)
{
  /*
  ** Get the rozofs_inverse and rozofs_forward value for the layout
  */
  uint8_t   rozofs_safe = rozofs_get_rozofs_safe(layout);
  int i;
  
  for (i = 0; i <rozofs_safe; i++,prj_cxt_p++)
  {
    if (prj_cxt_p->prj_state == ROZOFS_PRJ_READ_IN_PRG) return 1;
  }
  return 0;
}
/*
**__________________________________________________________________________
*/
/**
* The purpose of that function is to return the number of projection received
  rebuilding the associated initial message
  
  @param layout : layout association with the file
  @param prj_cxt_p: pointer to the projection context (working array)
  
  @retval number of received projection
*/
static inline int rozofs_storcli_resize_check(uint8_t layout,rozofs_storcli_projection_ctx_t *prj_cxt_p)
{
  /*
  ** Get the rozofs_inverse and rozofs_forward value for the layout
  */
  uint8_t   rozofs_inverse = rozofs_get_rozofs_inverse(layout);
  uint8_t   rozofs_safe = rozofs_get_rozofs_safe(layout);
  int i;
  int received = 0;
  int enoent   = 0;
  
  for (i = 0; i <rozofs_safe; i++,prj_cxt_p++)
  {
    if (prj_cxt_p->prj_state == ROZOFS_PRJ_READ_DONE) { 
      received++;
      if (received == rozofs_inverse) return received;  
    }   
    if (prj_cxt_p->prj_state == ROZOFS_PRJ_READ_ENOENT) {
      enoent++;
      if (enoent > rozofs_inverse) return enoent;   
    } 
  }
  return received;
}
/*
**__________________________________________________________________________
*/

void rozofs_storcli_resize_req_processing(rozofs_storcli_ctx_t *working_ctx_p)
{

  storcli_read_arg_t *storcli_read_rq_p;
  uint32_t  cur_nmbs;
  uint32_t  nmbs;
  bid_t     bid;
  uint32_t  nb_projections2read = 0;
  uint8_t   rozofs_inverse;
  uint8_t   rozofs_forward;
  uint8_t   rozofs_safe;
  uint8_t   projection_id;
  int       error;
  int i;
  int       rotate=0;
  rozofs_storcli_lbg_prj_assoc_t  *lbg_assoc_p = working_ctx_p->lbg_assoc_tb;
  rozofs_storcli_projection_ctx_t *prj_cxt_p   = working_ctx_p->prj_ctx;   
  uint8_t used_dist_set[ROZOFS_SAFE_MAX_STORCLI];
  uint8_t rotate_modulo;
  uint8_t local_storage_idx;
  int     j;

  STORCLI_ERR_PROF(resize);

  storcli_read_rq_p = (storcli_read_arg_t*)&working_ctx_p->storcli_read_arg;
     
  storcli_read_rq_p->bid = 0;
  storcli_read_rq_p->nb_proj = 0;
     
  /*
  ** compute the number of blocks with the same distribution starting
  ** from the current block index
  */
  cur_nmbs       = working_ctx_p->cur_nmbs;
  rozofs_inverse = rozofs_get_rozofs_inverse(storcli_read_rq_p->layout);
  rozofs_forward = rozofs_get_rozofs_forward(storcli_read_rq_p->layout);
  rozofs_safe    = rozofs_get_rozofs_safe(storcli_read_rq_p->layout);
  bid            = storcli_read_rq_p->bid;
  nmbs           = storcli_read_rq_p->nb_proj;

  nb_projections2read = nmbs;
  /*
  ** OK, now we known the number of blocks that must read (nb_projections2read) starting at
  ** bid+cur_nmbs block id.
  ** dist_iterator points to the last distribution that matches
  */
#if 0
  if (nb_projections2read == 0) 
  {
    /*
    * That's the end since there is no block to read
    */
    goto end ;
  }  
#endif  
  /*
  ** Rotate the distribution set for using all the first forward storages
  */ 
#if 1
  rotate = storcli_read_rq_p->sid; 
  i = 0;
  j = 0;
  
  /*
  ** Multi site mode. Read preferentially serveurs on local site
  */
  if (rozofs_get_msite()) {
    for (i = 0; i  <rozofs_forward ; i ++) {
	
	  int         site=0;
	  mstorage_t *mstor = storage_direct_get(storcli_read_rq_p->cid,storcli_read_rq_p->dist_set[i]);
	  if (mstor == NULL) {
	    severe("storage_direct_get(%d,%d)",(int)storcli_read_rq_p->cid,(int)storcli_read_rq_p->dist_set[i]);
	  }
	  else {
	    site =  mstor->site;   
	  }
	  if (site == conf.site) {
	    used_dist_set[j] = storcli_read_rq_p->dist_set[i];
		j++;
	  }
	  else {
	    used_dist_set[rozofs_forward-1-i+j] = storcli_read_rq_p->dist_set[i];
	  }
    }
  }
  /*
  ** Local preference is configured
  */
  else if (conf.localPreference) {

    while (1) {
      /*
      ** Check whether one storage is local in the 1rst foward storages of the distribution
      */
      local_storage_idx = 0xFF;
      for (i = 0; i  <rozofs_forward ; i ++)
      {
		int lbg_id = rozofs_storcli_get_lbg_for_sid(storcli_read_rq_p->cid,storcli_read_rq_p->dist_set[i]);
		if (north_lbg_is_local(lbg_id)) {
		  local_storage_idx = i;
		  break;
		}
      } 
      /*
      ** One local storage is found => always use this guy 1rst
      */ 
      if (local_storage_idx != 0xFF) {
		used_dist_set[0] = storcli_read_rq_p->dist_set[local_storage_idx];    
		for (i = 1; i  <rozofs_forward ; i ++)
		{
		  rotate_modulo = (rotate+i) % (rozofs_forward-1);
		  used_dist_set[rotate_modulo+1] = storcli_read_rq_p->dist_set[(local_storage_idx+i) % rozofs_forward]; 
		} 
		break;       
      }
      /*
      ** No local storage is found => use every possible storage 
      */
      for (i = 0; i  <rozofs_forward ; i ++)
      {
		rotate_modulo = (i + rotate) % rozofs_forward;
		used_dist_set[i] = storcli_read_rq_p->dist_set[rotate_modulo];     
      } 
      break;
    }  
  }  

  /*
  ** Fullfill the distribution with the spare storages
  */
  for (; i  <rozofs_safe ; i ++) {
    used_dist_set[i] = storcli_read_rq_p->dist_set[i];     
  } 
#endif  
  /*
  ** init of the load balancing group/ projection association table with the state of each lbg
  ** search in the current distribution the relative reference of the storage
  ** the first "rozofs_forward" bits correspnd to the optimal distribution and the 
  ** bits between "rozofs_forward" and "rozofs_safe" correpond to the spare storage(s).
  ** The number of spare storages depends on rozofs layout.
  */
  int lbg_in_distribution = 0;
  for (i = 0; i  <rozofs_safe ; i ++)
  {
    /*
    ** FDL do we need to check the value of the sid ????
    */
    /*
    ** Get the load balancing group associated with the sid
    */
    int lbg_id = rozofs_storcli_get_lbg_for_sid(storcli_read_rq_p->cid,used_dist_set[i]);
    if (lbg_id >= 0) {
      lbg_in_distribution++;
    }
      
    rozofs_storcli_lbg_prj_insert_lbg_and_sid(working_ctx_p->lbg_assoc_tb,i,
                                              lbg_id,
                                              used_dist_set[i]);   
    rozofs_storcli_lbg_prj_insert_lbg_state(lbg_assoc_p,
                                            i,
                                            NORTH_LBG_GET_STATE(lbg_assoc_p[i].lbg_id));    
    

  }
  if (lbg_in_distribution < rozofs_inverse)
  {
    /*
    ** we must have  rozofs_inverse storages in the distribution vector !!
    */
    error = EINVAL;
    STORCLI_ERR_PROF(resize_prj_err);
    storcli_trace_error(__LINE__,error, working_ctx_p);     
    goto fatal;  
  }  
  /*
  ** Now we have to select the storages that will be used for reading the projection. There are
  ** nb_projections2read to read
  ** The system search for a set of rozofs_forward storage, but it will trigger the read
  ** on a rozofs_inverse set for reading. In case of failure it will use the storages that
  ** are included between rozofs_inverse and rozofs_forward. (we might more than one
  ** spare storages depending of the selected rozofs layout).
  */
  for (projection_id = 0; projection_id < rozofs_inverse; projection_id++) 
  {
    if (rozofs_storcli_select_storage_idx (working_ctx_p,rozofs_safe,projection_id) < 0)
    {
      /*
      ** Out of storage !!-> too many storage down
      */
      error = EIO;
      STORCLI_ERR_PROF(resize_prj_err);
      storcli_trace_error(__LINE__,error, working_ctx_p);     
      goto fatal;
    }  
  }
  /*
  ** All the storages on which we can get the projection have been identified, so we can start the
  ** transactions towards the storages. Only rozofs_inverse transactions are intiated. the other
  ** storage(s) might be used if one storage of the rozofs_inverse interval fails.
  */
  working_ctx_p->redundancyStorageIdxCur = 0;
  working_ctx_p->nb_projections2read     = nb_projections2read;
  /*
  ** to be able to rebuild the initial data, the system must read rozofs_inverse different projections
  ** A) Optimal case
  ** in the optimal case the system gets it by read the projection on the "rozofs_inverse" fisrt storages on its allocated distribution
  ** B) first level failure
  ** This corresponds to the failure of one storage that belongs to the first "rozofs_forward" storages
  ** C) second level failure
  ** This corresponds to the situation where there is no enough valid projection in the first "rozofs_forward" storages.
  ** In that case the system will attempt to read up the "rozofs_safe"
  **
  ** Notice that one storage can return on out of date projection. In order to be able to rebuild the initial message
  ** all the projection MUST have the same timestamp
  
  */


  int sent = 0;
  for (projection_id = 0;projection_id < rozofs_inverse; projection_id++) 
  {
     void  *xmit_buf;  
     int ret;  
     sp_read_arg_t read_prj_args;
     sp_read_arg_t *request;    
      
     xmit_buf = ruc_buf_getBuffer(ROZOFS_STORCLI_SOUTH_LARGE_POOL);
     if (xmit_buf == NULL)
     {
       /*
       ** fatal error since the ressource control already took place
       */
       error = ENOMEM;
       storcli_trace_error(__LINE__,error, working_ctx_p);     
       goto fatal;     
     }

retry:
     /*
     ** fills the header of the request
     */
     
     request   = &read_prj_args;
     request->cid = storcli_read_rq_p->cid;
     request->sid = (uint8_t) rozofs_storcli_lbg_prj_get_sid(working_ctx_p->lbg_assoc_tb,prj_cxt_p[projection_id].stor_idx);;
     request->layout        = storcli_read_rq_p->layout;
     request->bsize         = storcli_read_rq_p->bsize;
     if (prj_cxt_p[projection_id].stor_idx >= rozofs_forward) request->spare = 1;
//     if (projection_id >= rozofs_forward) request->spare = 1;
     else request->spare = 0;
     memcpy(request->dist_set, storcli_read_rq_p->dist_set, ROZOFS_SAFE_MAX_STORCLI*sizeof (uint8_t));
     memcpy(request->fid, storcli_read_rq_p->fid, sizeof (sp_uuid_t));
     request->bid = bid+cur_nmbs;
     request->nb_proj  = nb_projections2read;
     uint32_t  lbg_id = rozofs_storcli_lbg_prj_get_lbg(working_ctx_p->lbg_assoc_tb,prj_cxt_p[projection_id].stor_idx);
     STORCLI_START_NORTH_PROF((&working_ctx_p->prj_ctx[projection_id]),resize_prj,0);
     /*
     ** assert by anticipation the expected state for the projection. It might be possible
     ** that the state would be changed at the time rozofs_sorcli_send_rq_common() by 
     ** rozofs_storcli_resize_req_processing_cbk() ,if we get a tcp disconnection without 
     ** any other TCP connection up in the lbg.
     ** In that case the state of the projection is changed to ROZOFS_PRJ_READ_ERROR
     **
     ** We also increment the inuse counter of the buffer to avoid a release of the buffer
     ** while releasing the transaction context if there is an error either  while submitting the 
     ** buffer to the lbg or if there is a direct reply to the transaction due to a transmission failure
     ** that usage of the inuse is mandatory since in case of failure the function re-use the same
     ** xmit buffer to attempt a transmission on another lbg
     */
     working_ctx_p->read_ctx_lock++;
     prj_cxt_p[projection_id].prj_state = ROZOFS_PRJ_READ_IN_PRG;
     ruc_buf_inuse_increment(xmit_buf);
     
     ret =  rozofs_sorcli_send_rq_common(lbg_id,ROZOFS_TMR_GET(TMR_STORAGE_PROGRAM),STORAGE_PROGRAM,STORAGE_VERSION,SP_READ,
                                         (xdrproc_t) xdr_sp_read_arg_t, (caddr_t) request,
                                          xmit_buf,
                                          working_ctx_p->read_seqnum,
                                         (uint32_t)projection_id,
                                         0,
                                         rozofs_storcli_resize_req_processing_cbk,
                                         (void*)working_ctx_p);
     working_ctx_p->read_ctx_lock--;
     ruc_buf_inuse_decrement(xmit_buf);

     if (ret < 0)
     {
       /*
       ** the communication with the storage seems to be wrong (more than TCP connection temporary down
       ** attempt to select a new storage
       **
       */
       warning("FDL error on send to lbg %d",lbg_id);
       STORCLI_ERR_PROF(resize_prj_err);       
       STORCLI_STOP_NORTH_PROF(&working_ctx_p->prj_ctx[projection_id],resize_prj,0);
       prj_cxt_p[projection_id].prj_state = ROZOFS_PRJ_READ_ERROR;
       prj_cxt_p[projection_id].errcode = errno;
       if (rozofs_storcli_select_storage_idx (working_ctx_p,rozofs_safe,projection_id) < 0)
       {
         /*
         ** Out of storage !!-> too many storages are down
         ** release the allocated xmit buffer and then reply with appropriated error code
         */ 
         STORCLI_ERR_PROF(resize_prj_err); 
         severe("FDL error on send: EIO returned");
         error = EIO;
         ruc_buf_freeBuffer(xmit_buf);
	 storcli_trace_error(__LINE__,error, working_ctx_p); 
         goto fatal;
       } 
       /*
       ** retry for that projection with a new storage index: WARNING: we assume that xmit buffer has not been released !!!
       */
       goto retry;
     } 
     /*
     ** check if the  state of the read buffer has not been changed. That change is possible when
     ** all the connection of the load balancing group are down. In that case we attempt to select
     ** another available load balancing group associated with a spare storage for reading the projection
     */
     if ( prj_cxt_p[projection_id].prj_state == ROZOFS_PRJ_READ_ERROR)
     {
         /*
         ** retry to send the request to another storage (spare) 
         */
       if (rozofs_storcli_select_storage_idx (working_ctx_p,rozofs_safe,projection_id) < 0)
       {
         /*
         ** Out of storage !!-> too many storages are down
         ** release the allocated xmit buffer and then reply with appropriated error code
         */
         STORCLI_ERR_PROF(resize_prj_err); 
         severe("FDL error on send: EIO returned");
         error = EIO;
         ruc_buf_freeBuffer(xmit_buf);
         storcli_trace_error(__LINE__,error, working_ctx_p);     
         goto fatal;
       }
       /*
       ** there is some spare storage available (their associated load group is UP)
       ** so try to read the projection on that spare storage
       */
       goto retry; 
     }
     else
     {
       /*
       ** the projection has been submitted to the load balancing group without error, just need to wait
       ** for the response. Send read request of the next projection to read (until rozofs_inverse)
       */
       sent +=1;
     }
   }
    /*
    ** All projection read request have been sent, just wait for the answers
    */
    return; 
    
    /*
    **_____________________________________________
    **  Exception cases
    **_____________________________________________
    */      
    
fatal:  
     /*
     ** we fall in that case when we run out of  resource
     */
     rozofs_storcli_read_reply_error(working_ctx_p,error);
     /*
     ** release the root transaction context
     */
     STORCLI_STOP_NORTH_PROF(working_ctx_p,read,0);
     rozofs_storcli_release_context(working_ctx_p);  
     return;  

     /*
     ** That case should not append here since the contol is done of the projection reception after rebuilding
     ** the rozofs block. That situation might after a read of 0 block is requested!!
     */     
     rozofs_storcli_read_reply_success(working_ctx_p);
     rozofs_storcli_release_context(working_ctx_p);  
     return;  

}


/*
**__________________________________________________________________________
*/
/**
* Projection read retry: that procedure is called upon the reading failure
  of one projection. The system attempts to read in sequence the next available
  projection if any. 
  The index of the next projection to read is given by redundancyStorageIdxCur
  
  @param  working_ctx_p : pointer to the root transaction context
  @param  projection_id : index of the projection
  @param same_storage_retry_acceptable : assert to 1 if retry on the same storage is acceptable
  
  @retval  0 : show must go on!!
  @retval < 0 : context has been released
*/

int rozofs_storcli_resize_projection_retry(rozofs_storcli_ctx_t *working_ctx_p,uint8_t projection_id,int same_storage_retry_acceptable)
{
    uint8_t   rozofs_safe;
    uint8_t   layout;
    uint8_t   rozofs_forward;
    uint8_t   rozofs_inverse;
    storcli_read_arg_t *storcli_read_rq_p;
    int error;
    int line=0;

    storcli_read_rq_p = (storcli_read_arg_t*)&working_ctx_p->storcli_read_arg;
    rozofs_storcli_projection_ctx_t *prj_cxt_p   = working_ctx_p->prj_ctx;   

    layout         = storcli_read_rq_p->layout;
    rozofs_safe    = rozofs_get_rozofs_safe(layout);
    rozofs_forward = rozofs_get_rozofs_forward(layout);
    rozofs_inverse = rozofs_get_rozofs_inverse(layout);
    /*
    ** Now update the state of each load balancing group since it might be possible
    ** that some experience a state change
    */
    rozofs_storcli_update_lbg_for_safe_range(working_ctx_p,rozofs_safe);
    /**
    * attempt to select a new storage
    */
    if (rozofs_storcli_select_storage_idx (working_ctx_p,rozofs_safe,projection_id) < 0)
    {
    
      /*
      ** In case of a rozofsmount write request, when the data flushed are not a 
      ** multiple of the rozofs block size, an internal read request is done in 
      ** order to complement the partial blocks. 
      ** In the case it is the very 1rst write of a file on disk, the file does
      ** not yet exist and one must receive back from every storage the error
      */
      {
        int i;
	int enoent=0;
        for (i=0; i< rozofs_safe; i++) {
	  if (prj_cxt_p[i].prj_state == ROZOFS_PRJ_READ_ENOENT) {
	    enoent++;
	    // 1rst inverse tell ENOENT 
	    if ((i<rozofs_inverse)&&(enoent==rozofs_inverse)) {
	      error = ENOENT;
              //storcli_trace_error(__LINE__,error, working_ctx_p);           
	      goto reject;	      
	    }
	    // More than inverse tell ENOENT
	    if (enoent>rozofs_inverse) {
	      error = ENOENT;
              //storcli_trace_error(__LINE__,error, working_ctx_p);     
	      goto reject;	      
	    }
	    	    
	  }  
        }	
      }
      
      /*
      ** Cannot select a new storage: OK so now double check if the retry on the same storage is
      ** acceptable.When it is the case, check if the max retry has not been yet reached
      ** Otherwise, we are in deep shit-> reject the read request
      */
      if (same_storage_retry_acceptable == 0) 
      {
        error = EIO;
	line = __LINE__;
        goto reject;     	 
      }
      if (++prj_cxt_p[projection_id].retry_cpt >= ROZOFS_STORCLI_MAX_RETRY)
      {
        error = EIO;
	line = __LINE__;
        goto reject;          
      }
    } 
    /*
    ** we are lucky since either a get a new storage or the retry counter is not exhausted
    */
    sp_read_arg_t *request; 
    sp_read_arg_t  read_prj_args;   
    void  *xmit_buf;  
    int ret;  

    xmit_buf = ruc_buf_getBuffer(ROZOFS_STORCLI_SOUTH_LARGE_POOL);
    if (xmit_buf == NULL)
    {
      /*
      ** fatal error since the ressource control already took place
      */
      severe("Out of small buffer");
      error = ENOMEM;
      line = __LINE__;
      goto fatal;
    }
    /*
    ** fill partially the common header
    */

  retry:
    /*
    ** fills the header of the request
    */
     request   = &read_prj_args;
     request->cid = storcli_read_rq_p->cid;
     request->sid = (uint8_t) rozofs_storcli_lbg_prj_get_sid(working_ctx_p->lbg_assoc_tb,prj_cxt_p[projection_id].stor_idx);;
     request->layout        = storcli_read_rq_p->layout;
     request->bsize         = storcli_read_rq_p->bsize;
     if (prj_cxt_p[projection_id].stor_idx >= rozofs_forward) request->spare = 1;
     else request->spare = 0;
     memcpy(request->dist_set, storcli_read_rq_p->dist_set, ROZOFS_SAFE_MAX_STORCLI*sizeof (uint8_t));
     memcpy(request->fid, storcli_read_rq_p->fid, sizeof (sp_uuid_t));
     request->bid = storcli_read_rq_p->bid+working_ctx_p->cur_nmbs;
     request->nb_proj  = working_ctx_p->nb_projections2read;
     uint32_t  lbg_id = rozofs_storcli_lbg_prj_get_lbg(working_ctx_p->lbg_assoc_tb,prj_cxt_p[projection_id].stor_idx);

     STORCLI_START_NORTH_PROF(&working_ctx_p->prj_ctx[projection_id],resize_prj,0);
     /*
     ** assert by anticipation the expected state for the projection. It might be possible
     ** that the state would be changed at the time rozofs_sorcli_send_rq_common() by 
     ** rozofs_storcli_resize_req_processing_cbk() ,if we get a tcp disconnection without 
     ** any other TCP connection up in the lbg.
     ** In that case the state of the projection is changed to ROZOFS_PRJ_READ_ERROR
     */
     working_ctx_p->read_ctx_lock++;
     ruc_buf_inuse_increment(xmit_buf);
     prj_cxt_p[projection_id].prj_state = ROZOFS_PRJ_READ_IN_PRG;
     
     ret =  rozofs_sorcli_send_rq_common(lbg_id,ROZOFS_TMR_GET(TMR_STORAGE_PROGRAM),STORAGE_PROGRAM,STORAGE_VERSION,SP_READ,
                                         (xdrproc_t) xdr_sp_read_arg_t, (caddr_t) request,
                                          xmit_buf,
                                          working_ctx_p->read_seqnum,
                                         (uint32_t)projection_id,
                                         0,
                                         rozofs_storcli_resize_req_processing_cbk,
                                         (void*)working_ctx_p);

    working_ctx_p->read_ctx_lock--;
     ruc_buf_inuse_decrement(xmit_buf);
    if (ret < 0)
    {
      /*
      ** the communication with the storage seems to be wrong (more than TCP connection temporary down
      ** attempt to select a new storage
      **
      */
      STORCLI_ERR_PROF(resize_prj_err);       
      STORCLI_STOP_NORTH_PROF(&working_ctx_p->prj_ctx[projection_id],resize_prj,0);
      prj_cxt_p[projection_id].prj_state = ROZOFS_PRJ_READ_ERROR;
      prj_cxt_p[projection_id].errcode = errno;
      /*
      ** retry to send the request to another storage (spare) 
      */       
      if (rozofs_storcli_select_storage_idx (working_ctx_p,rozofs_safe,projection_id) < 0)
      {
        /*
        ** Out of storage !!-> too many storages are down
        */
        severe("FDL error on send: EIO returned");
        ruc_buf_freeBuffer(xmit_buf);
        error = EIO;
        line = __LINE__;     
        goto reject;
      } 
      goto retry;
    } 
    /*
    ** check if the  state of the read buffer has not been changed. That change is possible when
    ** all the connection of the load balancing group are down. In that case we attempt to select
    ** another available load balancing group associated with a spare storage for reading the projection
    */
    if ( prj_cxt_p[projection_id].prj_state == ROZOFS_PRJ_READ_ERROR)
    {
      /*
      ** retry to send the request to another storage (spare) 
      */
      if (rozofs_storcli_select_storage_idx (working_ctx_p,rozofs_safe,projection_id) < 0)
      {
        /*
        ** Out of storage !!-> too many storages are down
        ** release the allocated xmit buffer and then reply with appropriated error code
        */
        error = EIO;
        ruc_buf_freeBuffer(xmit_buf);
        line = __LINE__;     
        goto reject;
      }
      /*
      ** there is some spare storage available (their associated load group is UP)
      ** so try to read the projection on that spare storage
      */
      goto retry; 
    }
    /*
    ** All's is fine, just wait for the response
    */
    prj_cxt_p[projection_id].prj_state = ROZOFS_PRJ_READ_IN_PRG;

    return 0;
    /*
    **_____________________________________________
    **  Exception cases
    **_____________________________________________
    */      
    
reject:  
     /*
     ** Check it there is projection for which we expect a response from storage
     ** that situation can occur because of some anticipation introduced by the read
     ** guard timer mechanism
     */
     if (rozofs_storcli_check_resize_in_progress_projections(layout,working_ctx_p->prj_ctx) == 0)
     {
       if (line) {   
         storcli_trace_error(line,error,working_ctx_p);     
       }	    
       /*
       ** we fall in that case when we run out of  storage
       */
       rozofs_storcli_read_reply_error(working_ctx_p,error);
       /*
       ** release the root transaction context
       */
        STORCLI_STOP_NORTH_PROF(working_ctx_p,read,0);
       rozofs_storcli_release_context(working_ctx_p);  
       return -1;
     }
     return 0; 
      
fatal:
    if (line) {   
       storcli_trace_error(line,error,working_ctx_p);     
     }	    

     /*
     ** we fall in that case when we run out of  resource-> that case is a BUG !!
     */
     rozofs_storcli_read_reply_error(working_ctx_p,error);
     /*
     ** release the root transaction context
     */
      STORCLI_STOP_NORTH_PROF(working_ctx_p,read,0);
     rozofs_storcli_release_context(working_ctx_p);  
     return -1; 

}
/*
**__________________________________________________________________________
*/

typedef struct _storcli_resize_rsp_t {
  uint32_t       nb_blocks;
  uint32_t       last_block_size;
  uint32_t       count;
} storcli_resize_rsp_t;


inline int rozofs_storcli_get_file_size(rozofs_storcli_projection_ctx_t *prj_ctx_p,  
                                       uint8_t    layout,
				       uint32_t * nb_blocks_ret,
				       uint32_t * last_block_size_ret)
{
    uint8_t                prj_ctx_idx;
    uint8_t                rozofs_storcli_next_free_idx = 0;    
    storcli_resize_rsp_t   rsp_tbl[ROZOFS_SAFE_MAX_STORCLI];
    storcli_resize_rsp_t * p;
    int                    entry;
    uint32_t               nb_blocks;
    uint32_t               last_block_size;
    uint8_t rozofs_safe    = rozofs_get_rozofs_safe(layout);
    uint8_t rozofs_inverse = rozofs_get_rozofs_inverse(layout);
     
    for (prj_ctx_idx = 0; prj_ctx_idx < rozofs_safe; prj_ctx_idx++)
    {
      if (prj_ctx_p[prj_ctx_idx].prj_state != ROZOFS_PRJ_READ_DONE)
      {
        /*
        ** that projection context does not contain valid data, so skip it
        */
        continue;      
      } 
      
      nb_blocks       = prj_ctx_p[prj_ctx_idx].timestamp >> 32;
      last_block_size = prj_ctx_p[prj_ctx_idx].timestamp & 0xFFFFFFLL;
      
      if (rozofs_storcli_next_free_idx == 0)
      {
        /*
        ** first entry
        */
	p = &rsp_tbl[0];
        p->nb_blocks       = nb_blocks;	
        p->last_block_size = last_block_size;
        p->count           = 1;
        rozofs_storcli_next_free_idx++;
        continue;      
      }
            
      /*
      ** more than 1 entry in the timestamp table
      */
      p = &rsp_tbl[0];
      for (entry = 0; entry < rozofs_storcli_next_free_idx;entry++,p++)
      {
        if ((p->nb_blocks != nb_blocks) || (p->last_block_size != last_block_size)) continue;
        p->count++;
        if (p->count == rozofs_inverse) return 0;
	break;
      }
      /*
      ** that timestamp does not exist, so create an entry for it
      */
      if (entry == rozofs_storcli_next_free_idx) {
        p = &rsp_tbl[entry];
        p->nb_blocks       = nb_blocks;	
        p->last_block_size = last_block_size;
        p->count           = 1;
        rozofs_storcli_next_free_idx++;
      }
    }
    return -1;
}

/*
**__________________________________________________________________________
*
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */

void rozofs_storcli_resize_req_processing_cbk(void *this,void *param) 
{
   uint32_t   seqnum;
   uint32_t   projection_id;
   rozofs_storcli_projection_ctx_t  *read_prj_work_p = NULL;
   rozofs_storcli_ctx_t *working_ctx_p = (rozofs_storcli_ctx_t*) param ;
   storcli_read_arg_t *storcli_read_rq_p;
   int status;
   XDR       xdrs;       
   uint8_t  *payload;
   int      bufsize;
   void     *recv_buf = NULL;   
   int      ret;
   int      same_storage_retry_acceptable = 0;
   sp_status_ret_t   rozofs_status;
   int error = 0;
   struct rpc_msg  rpc_reply;
   bin_t   *bins_p;
   uint64_t raw_file_size;
   int bins_len = 0;
   int lbg_id;
   uint32_t    nb_blocks       = 0;
   uint32_t    last_block_size = 0;
   /*
   ** take care of the rescheduling of the pending frames
   */
   trshape_schedule_on_response();

   rpc_reply.acpted_rply.ar_results.proc = NULL;

   storcli_read_rq_p = (storcli_read_arg_t*)&working_ctx_p->storcli_read_arg;
   
   uint8_t layout         = storcli_read_rq_p->layout;
   uint8_t rozofs_safe    = rozofs_get_rozofs_safe(layout);
   uint8_t rozofs_inverse = rozofs_get_rozofs_inverse(layout);
    /*
    ** get the sequence number and the reference of the projection id form the opaque user array
    ** of the transaction context
    */
    rozofs_tx_read_opaque_data(this,0,&seqnum);
    rozofs_tx_read_opaque_data(this,1,&projection_id);
    rozofs_tx_read_opaque_data(this,2,(uint32_t*)&lbg_id);
    
    /*
    ** check if the sequence number of the transaction matches with the one saved in the tranaaction
    ** that control is required because we can receive a response from a late transaction that
    ** it now out of sequence since the system is waiting for transaction response on a next
    ** set of distribution
    ** In that case, we just drop silently the received message
    */
    if (seqnum != working_ctx_p->read_seqnum)
    {
      /*
      ** not the right sequence number, so drop the received message but before check the status of the
      ** operation since we might decide to put the LBG in quarantine
      */
      status = rozofs_tx_get_status(this);
      if (status < 0)
      {
         /*
         ** something wrong happened: assert the status in the associated projection id sub-context
         ** now, double check if it is possible to retry on a new storage
         */
         errno = rozofs_tx_get_errno(this);  
         if (errno == ETIME)
         {
           storcli_lbg_cnx_sup_increment_tmo(lbg_id);
         }
      }
      else
      {
        storcli_lbg_cnx_sup_clear_tmo(lbg_id);
      }
      goto drop_msg;    
    }
    /*    
    ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
    */
    status = rozofs_tx_get_status(this);
    if (status < 0)
    {
       /*
       ** something wrong happened: assert the status in the associated projection id sub-context
       ** now, double check if it is possible to retry on a new storage
       */
       
       working_ctx_p->prj_ctx[projection_id].prj_state = ROZOFS_PRJ_READ_ERROR;
       errno = rozofs_tx_get_errno(this);  
       working_ctx_p->prj_ctx[projection_id].errcode = errno;
       if (errno == ETIME)
       {
         storcli_lbg_cnx_sup_increment_tmo(lbg_id);
         STORCLI_ERR_PROF(resize_prj_err);
       }
       else
       {
         STORCLI_ERR_PROF(resize_prj_err);
       }       
       same_storage_retry_acceptable = 1;
       goto retry_attempt; 
    }
    storcli_lbg_cnx_sup_clear_tmo(lbg_id);
    /*
    ** get the pointer to the receive buffer payload
    */
    recv_buf = rozofs_tx_get_recvBuf(this);
    if (recv_buf == NULL)
    {
       /*
       ** something wrong happened
       */
       STORCLI_ERR_PROF(resize_prj_err);       
       errno = EFAULT;  
       goto fatal;         
    }
    /*
    ** set the useful pointer on the received message
    */
    payload  = (uint8_t*) ruc_buf_getPayload(recv_buf);
    payload += sizeof(uint32_t); /* skip length*/
    /*
    ** OK now decode the received message
    */
    bufsize = ruc_buf_getPayloadLen(recv_buf);
    bufsize -= sizeof(uint32_t); /* skip length*/
    xdrmem_create(&xdrs,(char*)payload,bufsize,XDR_DECODE);
    while (1)
    {
      /*
      ** decode the rpc part
      */
      if (rozofs_xdr_replymsg(&xdrs,&rpc_reply) != TRUE)
      {
       STORCLI_ERR_PROF(resize_prj_err);       
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
        error = 1;
        break;
      }
      /*
      ** decode the status of the operation
      */
      if (xdr_sp_status_ret_t(&xdrs,&rozofs_status)!= TRUE)
      {
       errno = EPROTO;
       STORCLI_ERR_PROF(resize_prj_err);       
        error = 1;
        break;    
      }
      /*
      ** check th estatus of the operation
      */
      if ( rozofs_status.status != SP_SUCCESS )
      {
        errno = rozofs_status.sp_status_ret_t_u.error;
        if (errno != ENOENT) {
          STORCLI_ERR_PROF(resize_prj_err); 
	}      
        error = 1;
        break;    
      }
      {
	int alignment;
	/*
	** skip the alignment
	*/
	//for (k=0; k<3; k++) {
          if (xdr_int(&xdrs, &alignment) != TRUE)
	  {
            errno = EPROTO;
            STORCLI_ERR_PROF(resize_prj_err);       
            error = 1;
            break;          
	  }
          if (xdr_int(&xdrs, (int*)&nb_blocks) != TRUE)
	  {
            errno = EPROTO;
            STORCLI_ERR_PROF(resize_prj_err);       
            error = 1;
            break;          
	  }	
          if (xdr_int(&xdrs, (int*)&last_block_size) != TRUE)
	  {
            errno = EPROTO;
            STORCLI_ERR_PROF(resize_prj_err);       
            error = 1;
            break;          
	  }	    
      }

      /*
      ** Now get the length of the part that has been read
      */
      if (xdr_int(&xdrs, &bins_len) != TRUE)
      {
        errno = EPROTO;
        STORCLI_ERR_PROF(resize_prj_err);       
        error = 1;
        break;          
      }
      int position = xdr_getpos(&xdrs);
      /*
      ** get the pointer to the first byte available in the bins array
      */
      bins_p = (bin_t*)(payload+position);
      /*
      ** Get the file size
      */
      position += ((bins_len+(sizeof(uint32_t)-1))/sizeof(uint32_t))*sizeof(uint32_t);
      xdr_setpos(&xdrs,position);      
      xdr_uint64_t(&xdrs,&raw_file_size);
      break;
    }
    /*
    ** check the status of the operation
    */
    if (error)
    {
       /*
       ** there was an error on the remote storage while attempt to read the file
       ** try to read the projection on another storaged
       */
       if (errno == ENOENT) {
         working_ctx_p->prj_ctx[projection_id].prj_state = ROZOFS_PRJ_READ_ENOENT;     
       }
       else {
         working_ctx_p->prj_ctx[projection_id].prj_state = ROZOFS_PRJ_READ_ERROR; 
       }	 
       working_ctx_p->prj_ctx[projection_id].errcode = errno;
       /**
       * The error has been reported by the remote, we cannot retry on the same storage
       ** we imperatively need to select a different one. So if cannot select a different storage
       ** we report a reading error.
       */
       same_storage_retry_acceptable = 0;
       /*
       ** assert the projection rebuild flag if the selected storage index is the same as the
       ** index of the projection
       */
       if (working_ctx_p->prj_ctx[projection_id].stor_idx == projection_id)
       {
         working_ctx_p->prj_ctx[projection_id].rebuild_req = 1;       
       }
       goto retry_attempt;    	 
    }

    working_ctx_p->prj_ctx[projection_id].timestamp = nb_blocks;
    working_ctx_p->prj_ctx[projection_id].timestamp <<= 32;
    working_ctx_p->prj_ctx[projection_id].timestamp += last_block_size;

    /*
    ** set the pointer to the read context associated with the projection for which a response has
    ** been received
    */
    STORCLI_STOP_NORTH_PROF(&working_ctx_p->prj_ctx[projection_id],resize_prj,bins_len);
    read_prj_work_p = &working_ctx_p->prj_ctx[projection_id];
    /*
    ** save the reference of the receive buffer that contains the projection data in the root transaction context
    */
    read_prj_work_p->prj_buf = recv_buf;
    read_prj_work_p->prj_state = ROZOFS_PRJ_READ_DONE;
    read_prj_work_p->bins = bins_p;

    /*
    ** OK now check if we have enough projection to rebuild the initial message
    */
    ret = rozofs_storcli_resize_check(layout,working_ctx_p->prj_ctx);
    if (ret <rozofs_inverse)
    {
      /*
      ** start the timer on the first received projection
      */
      if (ret == 1) 
      {
        rozofs_storcli_start_read_guard_timer(working_ctx_p);
      }
       /*
       ** no enough projection 
       */
       goto wait_more_projection;
    }

    /*
    ** stop the guard timer since enough projection have been received
    */
    rozofs_storcli_stop_read_guard_timer(working_ctx_p);

    /*
    ** That's fine, all the projections have been received start rebuild the initial message
    ** for the case of the shared memory, we must check if the rozofsmount has not aborted the request
    */
    if (working_ctx_p->shared_mem_p != NULL)
    {
      uint32_t *xid_p = (uint32_t*)working_ctx_p->shared_mem_p;
      if (*xid_p !=  working_ctx_p->src_transaction_id)
      {
        /*
        ** the source has aborted the request
        */
        error = EPROTO;
        storcli_trace_error(__LINE__,errno,working_ctx_p);            
        goto io_error;
      }    
    }  
        
    /*
    ** check if we can proceed with transform inverse
    */
    ret = rozofs_storcli_get_file_size(working_ctx_p->prj_ctx, 
                                       layout,
				       &nb_blocks,
				       &last_block_size);
    if (ret < 0)
    {
      /*
      ** check if we have some read request pending. If it is the case wait for the 
      ** response of the request pending, otherwise attempt to read one more
      */
      if (rozofs_storcli_check_resize_in_progress_projections(layout,
         working_ctx_p->prj_ctx) != 0) {
	 rozofs_tx_free_from_ptr(this);
	 return;
      }
       /*
       ** There is no enough projection to rebuild the initial message
       ** check if we still have storage on which we can read some more projection
       */
       if( working_ctx_p->redundancyStorageIdxCur + rozofs_inverse >= rozofs_safe)
       {
         /*
         ** there are no enough valid storages to be able to rebuild the initial message
         */
         severe("FDL error on send: EIO returned");
         STORCLI_ERR_PROF(resize_prj_err);       
         error = EIO;
         storcli_trace_error(__LINE__,error, working_ctx_p);            
         goto io_error;
       }         
       /*
       ** we can take a new entry for a projection on a another storage
       */   
       projection_id = rozofs_inverse+ working_ctx_p->redundancyStorageIdxCur;
       working_ctx_p->redundancyStorageIdxCur++;    
       /*
        * do not forget to release the context of the transaction
       */
       rozofs_tx_free_from_ptr(this);
       rozofs_storcli_resize_projection_retry(working_ctx_p,projection_id,0);   
       return;     
    }
    /*
    ** we have all the projections with the good timestamp so proceed with
    ** the inverse transform
    */

    /*
    ** now the inverse transform is finished, release the allocated ressources used for
    ** rebuild
    */
    read_prj_work_p = working_ctx_p->prj_ctx;
    for (projection_id = 0; projection_id < rozofs_safe; projection_id++)
    {
      if  (read_prj_work_p[projection_id].prj_buf != NULL) {
        ruc_buf_freeBuffer(read_prj_work_p[projection_id].prj_buf);
      }	
      read_prj_work_p[projection_id].prj_buf = NULL;
      read_prj_work_p[projection_id].prj_state = ROZOFS_PRJ_READ_IDLE;
    }

    /*
    ** update the index of the next block to read
    */
    working_ctx_p->cur_nmbs2read += working_ctx_p->nb_projections2read;
    /*
    ** check if it was the last read
    */
    if (working_ctx_p->cur_nmbs2read < storcli_read_rq_p->nb_proj)
    {
      /*
      ** attempt to read block with the next distribution
      */
      return rozofs_storcli_resize_req_processing(working_ctx_p);        
    } 

    
    rozofs_storcli_resize_reply_success(working_ctx_p, nb_blocks, last_block_size);
    
    /*
    ** release the root context and the transaction context
    */
    rozofs_storcli_release_context(working_ctx_p);    
    rozofs_tx_free_from_ptr(this);
    return;
    
    /*
    **_____________________________________________
    **  Exception cases
    **_____________________________________________
    */    
drop_msg:
    /*
    ** the message has not the right sequence number,so just drop the received message
    ** and release the transaction context
    */  
     if(recv_buf!= NULL) ruc_buf_freeBuffer(recv_buf);       
     rozofs_tx_free_from_ptr(this);
     return;

fatal:
    /*
    ** unrecoverable error : mostly a bug!!
    */  
    STORCLI_STOP_NORTH_PROF(&working_ctx_p->prj_ctx[projection_id],resize_prj,0);
    if(recv_buf!= NULL) ruc_buf_freeBuffer(recv_buf);       
    rozofs_tx_free_from_ptr(this);
    if (working_ctx_p->read_ctx_lock != 0) return;
    fatal("Cannot get the pointer to the receive buffer");
    return;
    
retry_attempt:    
    /*
    ** There was a read errr for that projection so attempt to find out another storage
    ** but first of all release the ressources related to the current transaction
    */
    STORCLI_STOP_NORTH_PROF(&working_ctx_p->prj_ctx[projection_id],resize_prj,0);

    if(recv_buf!= NULL) ruc_buf_freeBuffer(recv_buf);       
    rozofs_tx_free_from_ptr(this);
    /**
    * attempt to select a new storage: a call to a read retry is done only if the
    ** error is not direct. If the system is in the sending process we do not call
    ** the retry attempt.    
    */
    if (working_ctx_p->read_ctx_lock != 0) return;

    rozofs_storcli_resize_projection_retry(working_ctx_p,projection_id,same_storage_retry_acceptable);
    return;

io_error:
    /*
    ** issue with connection towards storages and the projections are not consistent
    */
    if(recv_buf!= NULL) ruc_buf_freeBuffer(recv_buf);       
    rozofs_tx_free_from_ptr(this);
    
     rozofs_storcli_read_reply_error(working_ctx_p,error);
     /*
     ** release the root transaction context
     */
      STORCLI_STOP_NORTH_PROF(working_ctx_p,read,0);
     rozofs_storcli_release_context(working_ctx_p);      
    return;
        
wait_more_projection:

    /*
     ** Check it there is projection for which we expect a response from storage
     ** that situation can occur because of some anticipation introduced by the read
     ** guard timer mechanism
     */
    if (rozofs_storcli_check_resize_in_progress_projections(layout,
            working_ctx_p->prj_ctx) == 0) {
        /*
         ** we fall in that case when we run out of storage
         */
        error = EIO;
        storcli_trace_error(__LINE__,error, working_ctx_p);            
        rozofs_storcli_read_reply_error(working_ctx_p, error);
        /*
         ** release the root transaction context
         */
        STORCLI_STOP_NORTH_PROF(working_ctx_p, read, 0);
        rozofs_storcli_release_context(working_ctx_p);
    }

    /*
    ** there is no enough projection to rebuild the block, release the transaction
    ** and waiting for more projection read replies
    */
    rozofs_tx_free_from_ptr(this);
    return;


}



/*
**__________________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */

void rozofs_storcli_resize_timeout(rozofs_storcli_ctx_t *working_ctx_p) 
{
    uint8_t   rozofs_safe;
    uint8_t   layout;
    uint8_t   rozofs_inverse;
    storcli_read_arg_t *storcli_read_rq_p;
    uint32_t   projection_id;
    int missing;
    int ret;
    int i;
    int nb_received;

    storcli_read_rq_p = (storcli_read_arg_t*)&working_ctx_p->storcli_read_arg;

    layout         = storcli_read_rq_p->layout;
    rozofs_safe    = rozofs_get_rozofs_safe(layout);
    rozofs_inverse = rozofs_get_rozofs_inverse(layout);

    nb_received = rozofs_storcli_resize_check(layout,working_ctx_p->prj_ctx);
    
    missing = rozofs_inverse - nb_received;
    
    for (i = 0; i < missing; i++)
    {

      /*
      ** Check if it is possible to read from another storage
      ** if we cannot, we just leave without raising an error since the system may already
      ** ask to spare and is waiting for its response
      */
      if( working_ctx_p->redundancyStorageIdxCur + rozofs_inverse >= rozofs_safe)
      {
        return;
      }         
      /*
      ** we can take a new entry for a projection on a another storage
      */   
      projection_id = rozofs_inverse+ working_ctx_p->redundancyStorageIdxCur;
      working_ctx_p->redundancyStorageIdxCur++;  
      ret = rozofs_storcli_resize_projection_retry(working_ctx_p,projection_id,0);
      if (ret < 0)
      {
        /*
        ** the read context has been release, so give up
        */
        break;
      }
    }    
    return;    
}        
