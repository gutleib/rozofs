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

/* need for crypt */
#define _XOPEN_SOURCE 500

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
#include <malloc.h>

#include <rozofs/rozofs.h>
#include "config.h"
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
//#include "rozofs_stats.h"
#include <rozofs/core/rozofs_tx_common.h>
#include <rozofs/core/rozofs_tx_api.h>
#include "rozofs_storcli.h"
#include "storage_proto.h"
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/rozofs_srv.h>
#include "rozofs_storcli_rpc.h"
#include <rozofs/rpc/sproto.h>
#include "storcli_main.h"
#include <rozofs/rozofs_timer_conf.h>
#include "rozofs_storcli_mojette_thread_intf.h"


#include <rozofs/core/rozofs_fid_string.h>

DECLARE_PROFILING(stcpp_profiler_t);

typedef struct _storcli_repair_stat_t {
  uint64_t      empty_blocks;
  uint64_t      full_blocks;
  uint64_t      small_blocks;
  uint64_t      nb_projections;  
  uint64_t      req_sent;
  uint64_t      recv_success;
  uint64_t      recv_failure;
} storcli_repair_stat_t;

storcli_repair_stat_t storcli_repair_stat = { 0 };

/* 
**____________________________________________________
** REPAIR man
**
*/
void storcli_man_repair(char * pChar) {
  pChar += rozofs_string_append(pChar,"repair          : display repair statistics.\n");
  pChar += rozofs_string_append(pChar,"repair reset    : display, then clears repair statistics\n");
}
/*
**____________________________________________________
** Display counters and configuration
**
*/
char * storcli_display_repair_stat(char * pChar) {
  

  pChar += rozofs_string_append(pChar, "{ \"repair\" : {\n");
  pChar += rozofs_string_append(pChar, "    \"blocks\" : { ");
  pChar += rozofs_string_append(pChar, "\n      \"empty\" : ");    
  pChar += rozofs_u64_append(pChar, storcli_repair_stat.empty_blocks);
  pChar += rozofs_string_append(pChar, ",\n      \"full\"  : ");    
  pChar += rozofs_u64_append(pChar, storcli_repair_stat.full_blocks);
  pChar += rozofs_string_append(pChar, ",\n      \"small\" : ");    
  pChar += rozofs_u64_append(pChar, storcli_repair_stat.small_blocks);
  pChar += rozofs_string_append(pChar, ",\n      \"total\" : ");    
  pChar += rozofs_u64_append(pChar, storcli_repair_stat.small_blocks+storcli_repair_stat.full_blocks+storcli_repair_stat.empty_blocks);
  pChar += rozofs_string_append(pChar, "\n    },\n    \"projections\" : ");    
  pChar += rozofs_u64_append(pChar, storcli_repair_stat.nb_projections);
  pChar += rozofs_string_append(pChar, ",\n    \"sent\"        : ");    
  pChar += rozofs_u64_append(pChar, storcli_repair_stat.req_sent);
  pChar += rozofs_string_append(pChar, ",\n    \"success\"     : ");    
  pChar += rozofs_u64_append(pChar, storcli_repair_stat.recv_success);
  pChar += rozofs_string_append(pChar, ",\n    \"failure\"     : ");    
  pChar += rozofs_u64_append(pChar, storcli_repair_stat.recv_failure);
  pChar += rozofs_string_append(pChar, "\n  }\n}\n");  
  
  return pChar;
  
}
/*
**____________________________________________________
** Corrupted block CLI
**
*/
void storcli_cli_repair(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();

  if (argv[1] != NULL) {

    /*
    ** Reset counter
    */
    if (strcasecmp(argv[1],"reset")==0) {
      pChar = storcli_display_repair_stat(pChar);
      memset(&storcli_repair_stat,0, sizeof(storcli_repair_stat));     
    }

    /*
    ** Help
    */      
    else {
      storcli_man_repair(pChar);  
    }	 
  }  
  else {
    pChar = storcli_display_repair_stat(pChar);    
  } 
  
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}
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
extern int rozofs_storcli_fake_encode(xdrproc_t encode_fct,void *msg2encode_p);

/**
*  END PROTOTYPES
*/
/*
**__________________________________________________________________________
*/

/**
* Local prototypes
*/
void rozofs_storcli_write_repair_req_processing_cbk(void *this,void *param) ;
void rozofs_storcli_write_repair_req_processing(rozofs_storcli_ctx_t *working_ctx_p);

/*
**_________________________________________________________________________
*      LOCAL FUNCTIONS
**_________________________________________________________________________
*/


int storcli_write_repair3_bin_first_byte = 0;
int rozofs_storcli_repair3_get_position_of_first_byte2write()
{
  sp_write_repair3_arg_no_bins_t *request; 
  sp_write_repair3_arg_no_bins_t  repair3_prj_args;
  int position;
  
  
  if (storcli_write_repair3_bin_first_byte == 0)
  {
    request = &repair3_prj_args;
    memset(request,0,sizeof(sp_write_repair3_arg_no_bins_t));
    position = rozofs_storcli_fake_encode((xdrproc_t) xdr_sp_write_repair3_arg_no_bins_t, (caddr_t) request);
    if (position < 0)
    {
      fatal("Cannot get the size of the rpc header for repair3");
      return 0;    
    }
    storcli_write_repair3_bin_first_byte = position;
  }
  return storcli_write_repair3_bin_first_byte;

}
/*
**__________________________________________________________________________
*/
/**"
* The purpose of that function is to return TRUE if there are enough projection received for
  rebuilding the associated initial message
  
  @param layout : layout association with the file
  @param prj_cxt_p: pointer to the projection context (working array)
  
  @retval 1 if there are enough received projection
  @retval 0 when there is enough projection
*/
static inline int rozofs_storcli_all_prj_write_repair_check(uint8_t layout,rozofs_storcli_projection_ctx_t *prj_cxt_p)
{
  /*
  ** Get the rozofs_forward value for the layout
  */
  uint8_t   rozofs_forward = rozofs_get_rozofs_forward(layout);
  int i;
  
  for (i = 0; i <rozofs_forward; i++,prj_cxt_p++)
  {
    if (prj_cxt_p->prj_state == ROZOFS_PRJ_WR_IN_PRG) 
    {
      return 0;
    }
  }
  return 1;
}
/*
**__________________________________________________________________________
*/
/** 
  Regenerate a block without zeroing non valid parts of the block
  
 * 
 * @param working_ctx_p STORCLI working context
 * @param layout        The file to repair layout
 * @param bsize         Block size type of the file to repair
 * @param blockIdx      The relative block index to rebuild
 * @param data          Pointer to the regenerated data
 *
 * @return: the length written on success, -1 otherwise (errno is set)
 */
int rozofs_storcli_regenerate_initial_block(rozofs_storcli_ctx_t             * working_ctx_p,
                                	    uint8_t                            layout,
                                            uint8_t                            bsize,
                                            int                                blockIdx,
                                            char                             * data) {
  int          idx;
  int          prjIdx;  
  int          projection_id;
  projection_t inverse_projections[ROZOFS_SAFE_MAX];                  
  uint8_t      rozofs_safe     = rozofs_get_rozofs_safe(layout);
  uint8_t      rozofs_inverse  = rozofs_get_rozofs_inverse(layout);
  int          prj_size_in_msg = rozofs_get_max_psize_in_msg(layout,bsize);
  uint32_t     bbytes          = ROZOFS_BSIZE_BYTES(bsize);
  char      * pChar;
   
  /*
  ** Loop on the read projecttions to find out the ones that have been used
  ** to regenrate the user data
  */
  prjIdx = 0;
  for (idx = 0; idx < rozofs_safe; idx++) {
              
    /*
    ** Skip contexts that have no projection read
    */
    if (working_ctx_p->prj_ctx[idx].prj_state != ROZOFS_PRJ_READ_DONE) {
      continue;
    }   

    /*
    ** Skip contexts where this block is not valid
    */
    if (ROZOFS_BITMAP64_TEST1(blockIdx, working_ctx_p->prj_ctx[idx].crc_err_bitmap)) {
      continue;
    }
    
    /*
    ** Skip contexts where this block has not the timestamp used to rebuild
    */      
    if (working_ctx_p->prj_ctx[idx].block_hdr_tab[blockIdx].s.timestamp != working_ctx_p->block_ctx_table[blockIdx].timestamp) {
      continue;
    }

    /*
    ** Get this projection
    */
    projection_id = working_ctx_p->prj_ctx[idx].block_hdr_tab[blockIdx].s.projection_id;

    inverse_projections[prjIdx].angle.p = rozofs_get_angles_p(layout,projection_id);
    inverse_projections[prjIdx].angle.q = rozofs_get_angles_q(layout,projection_id);
    inverse_projections[prjIdx].size    = rozofs_get_128bits_psizes(layout,bsize,projection_id);
    pChar = (char *) working_ctx_p->prj_ctx[idx].bins;
    pChar += ((blockIdx*prj_size_in_msg) + sizeof(rozofs_stor_bins_hdr_t));
    inverse_projections[prjIdx].bins    = (bin_t*)pChar;
    prjIdx++;
  }
  
  if (prjIdx < rozofs_inverse) {
    severe("rozofs_storcli_regenerate_initial_block");
    return -1;
  }  
   
  // Inverse data for the block (first_repair_block_idx + repair_block_idx)
  transform128_inverse_copy((pxl_t *) data,
                	    rozofs_inverse,
                	    bbytes / rozofs_inverse / sizeof (pxl_t),
                	    rozofs_inverse, inverse_projections,
			    rozofs_get_max_psize(layout,bsize)*sizeof(bin_t));
          	    
  return 0;	  
}
/*
**__________________________________________________________________________
*/
/** 
  Apply the transform to a buffer starting at "data". That buffer MUST be ROZOFS_BSIZE
  aligned.
  The first_repair_block_idx is the index of a ROZOFS_BSIZE array in the output buffer
  The number_of_blocks is the number of ROZOFS_BSIZE that must be transform
  Notice that the first_repair_block_idx offset applies to the output transform buffer only
  not to the input buffer pointed by "data".
  
 * 
 * @param *working_ctx_p: storcli working context
 * @param repair_prj_ctx: allocated buffer for repair projections
 * @param layout: The file to repair layout
 * @param *data: pointer to the source data that must be transformed
 *
 * @return: the length written on success, -1 otherwise (errno is set)
 */
 void rozofs_storcli_transform_forward_repair(rozofs_storcli_ctx_t             * working_ctx_p,
                                              rozofs_storcli_projection_ctx_t  * repair_prj_ctx, 
                                	      uint8_t                            layout,
                                	      char                             * data) 
 {
    projection_t projections[ROZOFS_SAFE_MAX_STORCLI];
    uint16_t projection_id = 0;
    uint32_t blockIdx = 0;    
    uint8_t rozofs_forward = rozofs_get_rozofs_forward(layout);
    uint8_t rozofs_safe    = rozofs_get_rozofs_forward(layout);
    uint8_t rozofs_inverse = rozofs_get_rozofs_inverse(layout);
    rozofs_storcli_projection_ctx_t *prj_ctx_p = &working_ctx_p->prj_ctx[0];
    int empty_block = 0;
    uint8_t sid;
    int moj_prj_id;
    int repair_block_idx;
    int k;
    storcli_read_arg_t *storcli_read_rq_p = (storcli_read_arg_t*)&working_ctx_p->storcli_read_arg;
    uint8_t  bsize  = storcli_read_rq_p->bsize;
    uint32_t bbytes = ROZOFS_BSIZE_BYTES(bsize);
    int prj_size_in_msg = rozofs_get_max_psize_in_msg(layout,bsize);
    char * pRegeneratedBlock = NULL;
    uint32_t number_of_blocks = working_ctx_p->effective_number_of_blocks;
    

    // For each projection
    for (projection_id = 0; projection_id < rozofs_forward; projection_id++) {
        projections[projection_id].angle.p =  rozofs_get_angles_p(layout,projection_id);
        projections[projection_id].angle.q =  rozofs_get_angles_q(layout,projection_id);
        projections[projection_id].size    =  rozofs_get_128bits_psizes(layout,bsize,projection_id);
    }
        
    /*
    ** now go through all projection set to find out if there is something to regenerate
    */
    for (k = 0; k < rozofs_safe; k++)
    {
       repair_block_idx = 0;
       
       /*
       ** No need to repair this projection
       */
       if (ROZOFS_BITMAP64_TEST_ALL0(prj_ctx_p[k].crc_err_bitmap)) {
         continue;
       }
       
       /*
       ** No buffer allocated for this projection
       */
       if (repair_prj_ctx[k].bins == NULL) {
         continue;
       }
       
       /*
       **  Get the sid associated with the read projection context
       */
       sid = (uint8_t) rozofs_storcli_lbg_prj_get_sid(working_ctx_p->lbg_assoc_tb, prj_ctx_p[k].stor_idx);
       /*
       ** Get the reference of the Mojette projection_id
       */
       moj_prj_id = rozofs_storcli_get_mojette_proj_id(storcli_read_rq_p->dist_set,sid,rozofs_forward);
       if  (moj_prj_id < 0)
       {
          /*
	  ** it is the reference of a spare sid, so go to the next projection context
	  */
          ROZOFS_BITMAP64_ALL_RESET(prj_ctx_p[k].crc_err_bitmap) ;        
	  continue;
       }
       
       storcli_repair_stat.nb_projections++; 
       
       for (blockIdx = 0; blockIdx < number_of_blocks; blockIdx++) 
       {
          if (ROZOFS_BITMAP64_TEST0(blockIdx,prj_ctx_p[k].crc_err_bitmap)) {
            continue;
          }
          
	  /*
	  ** check for empty block
	  */
          empty_block = rozofs_data_block_check_empty(data + (blockIdx * bbytes), bbytes);
	  /**
	  * regenerate the projection for the block for which a crc error has been detected
	  */
          projections[moj_prj_id].bins = repair_prj_ctx[k].bins + 
                                         (prj_size_in_msg/sizeof(bin_t)* (0+repair_block_idx));
          rozofs_stor_bins_hdr_t *rozofs_bins_hdr_p = (rozofs_stor_bins_hdr_t*)projections[moj_prj_id].bins;   
          /*
          ** check if the user data block is empty: if the data block is empty no need to transform
          */
          if (empty_block)
          {
            rozofs_bins_hdr_p->s.projection_id = 0;
            rozofs_bins_hdr_p->s.timestamp     = 0;          
            rozofs_bins_hdr_p->s.effective_length = 0;    
            rozofs_bins_hdr_p->s.filler = 0;    
            rozofs_bins_hdr_p->s.version = 0;
	    repair_block_idx++;  
            storcli_repair_stat.empty_blocks++;  
            continue;   
          }
          	 
          /*
          ** fill the header of the projection
          */
          rozofs_bins_hdr_p->s.projection_id     = moj_prj_id;
          rozofs_bins_hdr_p->s.timestamp         = working_ctx_p->block_ctx_table[blockIdx].timestamp; 
          rozofs_bins_hdr_p->s.effective_length  = working_ctx_p->block_ctx_table[blockIdx].effective_length;
          rozofs_bins_hdr_p->s.filler = 0;    
          rozofs_bins_hdr_p->s.version = 0;    	 
          /*
          ** update the pointer to point out the first bins
          */
          projections[moj_prj_id].bins += sizeof(rozofs_stor_bins_hdr_t)/sizeof(bin_t);
          /*
          ** This is a full block. Let' regenerate the projection to repair
          */
          if (working_ctx_p->block_ctx_table[blockIdx].effective_length == bbytes) {
            /*
            ** Apply the erasure code transform for the block i
            */
            transform128_forward_one_proj((pxl_t *) (data + (blockIdx * bbytes)),
                    rozofs_inverse,
                    bbytes / rozofs_inverse / sizeof (pxl_t),
                    moj_prj_id, projections);
            /*
	    ** add the footer at the end of the repaired projection
	    */
            rozofs_stor_bins_footer_t *rozofs_bins_foot_p;
            rozofs_bins_foot_p = (rozofs_stor_bins_footer_t*) (projections[moj_prj_id].bins
	                                                      + rozofs_get_psizes(layout,bsize,moj_prj_id));
            rozofs_bins_foot_p->timestamp      = rozofs_bins_hdr_p->s.timestamp;	
            storcli_repair_stat.full_blocks++;  
	    repair_block_idx++; 
            continue;   
          }
          
          /*
          ** When effective size is not a complete block, the inverse trasnformation has filled with zero 
          ** the end of the block. So the regenerated data may not be the one that have been used to generate
          ** the projections. We need exactly the same block to regenerate a compatible projection
          */

          /*
          ** Need to allocate a block for regeneration
          */
          if (pRegeneratedBlock == NULL) {
            pRegeneratedBlock = memalign(4096,bbytes);              
          }

          /*
          ** Let's forget about this block !
          */
          if (pRegeneratedBlock == NULL) {
            severe("Out of memory");
            ROZOFS_BITMAP64_RESET(blockIdx,prj_ctx_p[k].crc_err_bitmap);
            continue;
          }  
	  
          /*
          ** Regenerate the initial block from the read projection
          */    
          if (rozofs_storcli_regenerate_initial_block(working_ctx_p,
                                                      layout, 
                                                      bsize, 
                                                      blockIdx, 
                                                      pRegeneratedBlock) < 0) {
            severe("rozofs_storcli_regenerate_initial_block %d",blockIdx);
            ROZOFS_BITMAP64_RESET(blockIdx,prj_ctx_p[k].crc_err_bitmap);
            continue;
          } 
          
          /*
          ** Apply the erasure code transform for the block i
          */
          transform128_forward_one_proj((pxl_t *) pRegeneratedBlock,
                  rozofs_inverse,
                  bbytes / rozofs_inverse / sizeof (pxl_t),
                  moj_prj_id, projections);
          /*
	  ** add the footer at the end of the repaired projection
	  */
          rozofs_stor_bins_footer_t *rozofs_bins_foot_p;
          rozofs_bins_foot_p = (rozofs_stor_bins_footer_t*) (projections[moj_prj_id].bins
	                                                    + rozofs_get_psizes(layout,bsize,moj_prj_id));
          rozofs_bins_foot_p->timestamp      = rozofs_bins_hdr_p->s.timestamp;	

          storcli_repair_stat.small_blocks++; 
	  repair_block_idx++;                                                           
        }
    }
    
    /*
    ** Free the regerated block when one has been allocated
    */
    if (pRegeneratedBlock) {
       free(pRegeneratedBlock); 
       pRegeneratedBlock = NULL;            
    }
    
}
/*
**__________________________________________________________________________
 *
 * Build the list of block to rebuild in the repair3 message from the STORCLI
 * toward the storio for one projection  
 * 
 * @param working_ctx_p: storcli working context
 * @param prj_ctx_p: pointer to the projection to repair
 * @param blk2repair: Array of old block header to be filled by this procedure
 *
 * @return: the number of blocks
 */
int rozofs_storcli_copy_old_timestamps       (rozofs_storcli_ctx_t            * working_ctx_p,
                                              rozofs_storcli_projection_ctx_t * prj_ctx_p,
                                              sp_b2rep_t                      * blk2repair,
                                              int                               ctxid,
                                              int                               prjId)
{
    uint32_t blk = 0;    
    int      count = 0;
      
    memset(blk2repair, -1, ROZOFS_MAX_REPAIR_BLOCKS*sizeof(sp_b2rep_t)); 

    /*
    ** No block to repair
    */
    if (ROZOFS_BITMAP64_TEST_ALL0(prj_ctx_p->crc_err_bitmap)) return count;

    /*
    ** Loop on gthe blocks to copy the old headers of those 
    ** which will be repaired
    */
    for (blk = 0; blk < working_ctx_p->effective_number_of_blocks; blk++) 
    {
      if (ROZOFS_BITMAP64_TEST0(blk,prj_ctx_p->crc_err_bitmap)) 
      {
	/*
	** nothing to generate for that block
	*/
	continue;
      }
      
      /*
      ** fill the header of the projection
      */
      memcpy(blk2repair[count].hdr,&prj_ctx_p->rcv_hdr[blk],sizeof(rozofs_stor_bins_hdr_t));
      blk2repair[count].relative_bid  = blk;
#if 0
      {
           rozofs_stor_bins_hdr_t * p = (rozofs_stor_bins_hdr_t *)&blk2repair[count].hdr[0];
           info ("CRC REPAIR    prjId %d ctx %d indist %d bid %llu TS %llu prj %d size %d vers %d CRC %x", prjId, ctxid, prj_ctx_p->stor_idx, (long long unsigned int)blk,
                 p->s.timestamp,
                 p->s.projection_id, p->s.effective_length, p->s.version,p->s.filler);
      }           
#endif
      count++;
      if (count >= ROZOFS_MAX_REPAIR_BLOCKS) break;  
    }
    
    return count;
} 
/*
**_________________________________________________________________________
*      PUBLIC FUNCTIONS
**_________________________________________________________________________
*/

/*
**__________________________________________________________________________
*/
/**
*  Get the Mojette projection identifier according to the distribution

   @param dist_p : pointer to the distribution set
   @param sid : reference of the sid within the cluster
   @param fwd : number of projections for a forward
   
   @retval >= 0 : Mojette projection id
   @retval < 0 the sid belongs to the spare part of the distribution set
*/

int rozofs_storcli_get_mojette_proj_id(uint8_t *dist_p,uint8_t sid,uint8_t fwd)
{
   int prj_id;
   
   for (prj_id = 0; prj_id < fwd; prj_id++)
   {
      if (dist_p[prj_id] == sid) return prj_id;
   }
   return -1;
}
/*
**__________________________________________________________________________
*/
/**
*   That function check if a repair block procedure has to be launched after a successfull read
    The goal is to detect the block for which the storage node has reported a crc error
    
    @param working_ctx_p: storcli working context of the read request
    @param rozofs_safe : max number of context to check
    
    @retval 0 : no crc error 
    @retval 1 : there is at least one block with a crc error
*/
int rozofs_storcli_check_repair(rozofs_storcli_ctx_t *working_ctx_p,int rozofs_safe)
{

    rozofs_storcli_projection_ctx_t *prj_ctx_p   = working_ctx_p->prj_ctx;   
    int prj_ctx_idx;


    // Storages support repair2 procedure 
    for (prj_ctx_idx = 0; prj_ctx_idx < rozofs_safe; prj_ctx_idx++,prj_ctx_p++)
    {      
      /*
      ** check for crc error
      */
      if (!ROZOFS_BITMAP64_TEST_ALL0(prj_ctx_p->crc_err_bitmap)) {
        return 1;
      }	
    }
    return 0;
	  
}
/*
**__________________________________________________________________________
*/
/**
  Initial write repair request


  Here it is assumed that storclo is working with the context that has been allocated 
  @param  working_ctx_p: pointer to the working context of a read transaction
 
   @retval : TRUE-> xmit ready event expected
  @retval : FALSE-> xmit  ready event not expected
*/
void rozofs_storcli_repair_req_init(rozofs_storcli_ctx_t *working_ctx_p)
{
   int                                i;
   storcli_read_arg_t               * storcli_read_rq_p = (storcli_read_arg_t*)&working_ctx_p->storcli_read_arg;
   rozofs_storcli_projection_ctx_t    repair_prj_ctx[ROZOFS_SAFE_MAX_STORCLI];
   

   STORCLI_START_NORTH_PROF(working_ctx_p,repair,0);

   /*
   ** set the pointer to to first available data (decoded data)
   */
   working_ctx_p->data_write_p  = working_ctx_p->data_read_p; 
   
   /*
   ** Allocate buffers for the projections that needs a repair
   */
   uint8_t forward_projection = rozofs_get_rozofs_forward(storcli_read_rq_p->layout);
   int     position           = rozofs_storcli_repair3_get_position_of_first_byte2write();
   for (i = 0; i < forward_projection; i++)
   {
     repair_prj_ctx[i].prj_state = ROZOFS_PRJ_WR_IDLE;
     repair_prj_ctx[i].prj_buf   = NULL;
     repair_prj_ctx[i].bins      = NULL;
     
     /*
     ** This projection needs no repair
     */
     if (ROZOFS_BITMAP64_TEST_ALL0(working_ctx_p->prj_ctx[i].crc_err_bitmap)) {
        continue;
     }	   
       
     /*
     ** This projection needs a repair so allocate a buffer
     */
     repair_prj_ctx[i].prj_buf = ruc_buf_getBuffer(ROZOFS_STORCLI_SOUTH_LARGE_POOL);
     if (repair_prj_ctx[i].prj_buf == NULL)
     {
	 /*
	 ** that situation MUST not occur since there the same number of receive buffer and working context!!
	 */
	 severe("out of large buffer");
         /*
         ** Release allocated buffers
         */
         i--;
         while (i>=0) {
           if (repair_prj_ctx[i].prj_buf != NULL) {
             ruc_buf_freeBuffer(repair_prj_ctx[i].prj_buf);
             repair_prj_ctx[i].prj_buf = NULL;
             i--;
           }
         }
	 goto failure;
     }
     /*
     ** set the pointer to the bins
     */
     uint8_t *pbuf = (uint8_t*)ruc_buf_getPayload(repair_prj_ctx[i].prj_buf); 
     repair_prj_ctx[i].bins = (bin_t*)(pbuf+position);   
   }	
   
   /*
   **  now regenerate the projections that were in error
   */
   rozofs_storcli_transform_forward_repair(working_ctx_p,
                                           repair_prj_ctx,
                                           storcli_read_rq_p->layout,
                                           (char *)working_ctx_p->data_write_p); 
                                           
   /*
   ** One can free th read projections now that the projections to repair 
   ** have been regenerated
   */ 
   rozofs_storcli_release_prj_buf(working_ctx_p,storcli_read_rq_p->layout); 

   /*
   ** Let's install the repair projections in the working context
   */
   for (i = 0; i < forward_projection; i++) {
     working_ctx_p->prj_ctx[i].prj_state = repair_prj_ctx[i].prj_state;
     working_ctx_p->prj_ctx[i].prj_buf   = repair_prj_ctx[i].prj_buf;
     working_ctx_p->prj_ctx[i].bins      = repair_prj_ctx[i].bins;
   } 
                                             			
   /*
   ** starts the sending of the repaired projections
   */
   rozofs_storcli_write_repair_req_processing(working_ctx_p);
   return;


failure:
  /*
  ** Release the allocated read buffer
  */
  rozofs_storcli_release_prj_buf(working_ctx_p,storcli_read_rq_p->layout);
   
  /*
  ** send back the response of the read request towards rozofsmount
  */
  rozofs_storcli_read_reply_success(working_ctx_p);
  /*
  ** release the root transaction context
  */
  STORCLI_STOP_NORTH_PROF(working_ctx_p,repair,0);
  rozofs_storcli_release_context(working_ctx_p);  
}

/*
**__________________________________________________________________________
*/
/*
** That function is called when all the projection are ready to be sent

 @param working_ctx_p: pointer to the root context associated with the top level write request

*/
void rozofs_storcli_write_repair_req_processing(rozofs_storcli_ctx_t *working_ctx_p)
{

  storcli_read_arg_t *storcli_read_rq_p = (storcli_read_arg_t*)&working_ctx_p->storcli_read_arg;
  uint8_t layout = storcli_read_rq_p->layout;
  uint8_t   rozofs_forward;
  uint8_t   projection_id;
  int       error=0;
  int       ret;
  rozofs_storcli_projection_ctx_t *prj_cxt_p   = working_ctx_p->prj_ctx;   
  uint8_t  bsize  = storcli_read_rq_p->bsize;
  int prj_size_in_msg = rozofs_get_max_psize_in_msg(layout,bsize);
  sp_write_repair3_arg_no_bins_t  *repair3; 
  sp_write_repair3_arg_no_bins_t  repair3_prj_args;
      
  rozofs_forward = rozofs_get_rozofs_forward(layout);
  /*
  ** check if the buffer is still valid: we might face the situation where the rozofsmount
  ** time-out and re-allocate the write buffer located in shared memory for another
  ** transaction (either read or write:
  ** the control must take place only where here is the presence of a shared memory for the write
  */
  error  = 0;
  if (working_ctx_p->shared_mem_p!= NULL)
  {
      uint32_t *xid_p = (uint32_t*)working_ctx_p->shared_mem_p;
      if (*xid_p !=  working_ctx_p->src_transaction_id)
      {
        /*
        ** the source has aborted the request
        */
        error = EPROTO;
      }      
  } 
  /*
  ** send back the response of the read request towards rozofsmount
  */
  rozofs_storcli_read_reply_success(working_ctx_p);
   /*
   ** allocate a sequence number for the working context:
   **   This is mandatory to avoid any confusion with a late response of the previous read request
   */
   working_ctx_p->read_seqnum = rozofs_storcli_allocate_read_seqnum();
  /*
  ** check if it make sense to send the repaired blocks
  */
  if (error)
  {
    /*
    ** the requester has released the buffer and it could be possible that the
    ** rozofsmount uses it for another purpose, so the data that have been repaired
    ** might be wrong, so don't take the right to write wrong data for which we can can 
    ** a good crc !!
    */
    goto fail;
  }
  /*
  ** We have enough storage, so initiate the transaction towards the storage for each
  ** projection
  */
  for (projection_id = 0; projection_id < rozofs_forward; projection_id++)
  {
     void  *xmit_buf;  
     int ret; 


      /*
      ** Do not fix the spare
      */
      if (prj_cxt_p[projection_id].stor_idx >= rozofs_forward) continue;
     /*
     ** skip the projections for which no error has been detected 
     */
     if (ROZOFS_BITMAP64_TEST_ALL0(working_ctx_p->prj_ctx[projection_id].crc_err_bitmap)) continue;	 
	 
     xmit_buf = prj_cxt_p[projection_id].prj_buf;
     if (xmit_buf == NULL)
     {
       /*
       ** fatal error since the ressource control already took place
       */       
       error = EIO;
       goto fail;     
     }
       
     /*
     ** fill partially the common header
     */
     repair3   = &repair3_prj_args;
     repair3->cid = storcli_read_rq_p->cid;
     repair3->sid = (uint8_t) rozofs_storcli_lbg_prj_get_sid(working_ctx_p->lbg_assoc_tb,prj_cxt_p[projection_id].stor_idx);
     repair3->layout        = storcli_read_rq_p->layout;
     repair3->bsize         = storcli_read_rq_p->bsize;
     /*
     ** the case of spare 1 must not occur because repair is done for th eoptimal distribution only
     */
     if (prj_cxt_p[projection_id].stor_idx >= rozofs_forward) repair3->spare = 1;
     else repair3->spare = 0;
     memcpy(repair3->dist_set, storcli_read_rq_p->dist_set, ROZOFS_SAFE_MAX_STORCLI*sizeof (uint8_t));
     memcpy(repair3->fid, storcli_read_rq_p->fid, sizeof (sp_uuid_t));
//CRCrequest->proj_id = projection_id;
     repair3->proj_id = rozofs_storcli_get_mojette_proj_id(storcli_read_rq_p->dist_set,repair3->sid,rozofs_forward);
     repair3->bid     = storcli_read_rq_p->bid;

     repair3->nb_proj = rozofs_storcli_copy_old_timestamps(working_ctx_p, &prj_cxt_p[projection_id], repair3->blk2repair,projection_id,repair3->proj_id);   

     /*
     ** set the length of the bins part: need to compute the number of blocks
     */

     int bins_len = (prj_size_in_msg * repair3->nb_proj);
     repair3->len = bins_len; /**< bins length MUST be in bytes !!! */
     uint32_t  lbg_id = rozofs_storcli_lbg_prj_get_lbg(working_ctx_p->lbg_assoc_tb,prj_cxt_p[projection_id].stor_idx);
     STORCLI_START_NORTH_PROF((&working_ctx_p->prj_ctx[projection_id]),repair_prj,bins_len);
     /*
     ** caution we might have a direct reply if there is a direct error at load balancing group while
     ** ateempting to send the RPC message-> typically a disconnection of the TCP connection 
     ** As a consequence the response fct 'rozofs_storcli_write_repair_req_processing_cbk) can be called
     ** prior returning from rozofs_sorcli_send_rq_common')
     ** anticipate the status of the xmit state of the projection and lock the section to
     ** avoid a reply error before returning from rozofs_sorcli_send_rq_common() 
     ** --> need to take care because the write context is released after the reply error sent to rozofsmount
     */
     working_ctx_p->write_ctx_lock = 1;
     prj_cxt_p[projection_id].prj_state = ROZOFS_PRJ_WR_IN_PRG;

     ret =  rozofs_sorcli_send_rq_common(lbg_id,ROZOFS_TMR_GET(TMR_STORAGE_PROGRAM),STORAGE_PROGRAM,STORAGE_VERSION,SP_WRITE_REPAIR3,
                                         (xdrproc_t) xdr_sp_write_repair3_arg_no_bins_t, (caddr_t) repair3,
                                              xmit_buf,
                                              working_ctx_p->read_seqnum,
                                              (uint32_t) projection_id,
                                              bins_len,
                                              rozofs_storcli_write_repair_req_processing_cbk,
                                         (void*)working_ctx_p);
								   

     working_ctx_p->write_ctx_lock = 0;
     if (ret < 0)
     {
        /*
	** there is no retry, just keep on with a potential other projection to repair
	*/
        STORCLI_ERR_PROF(repair_prj_err);
        STORCLI_STOP_NORTH_PROF((&working_ctx_p->prj_ctx[projection_id]),repair_prj,0);
	prj_cxt_p[projection_id].prj_state = ROZOFS_PRJ_WR_ERROR;
	continue;
     } 
     else
     {
       storcli_repair_stat.req_sent++; 
       
       /*
       ** check if the state has not been changed: -> it might be possible to get a direct error
       */
       if (prj_cxt_p[projection_id].prj_state == ROZOFS_PRJ_WR_ERROR)
       {
          /*
	  ** it looks like that we cannot repair that preojection, check if there is some other
	  */
          STORCLI_STOP_NORTH_PROF((&working_ctx_p->prj_ctx[projection_id]),repair_prj,0);

       }      
     }
   }
   /*
   ** check if there some write repair request pending, in such a case we wait for the end of the repair
   ** (answer from the storage node
   */
    ret = rozofs_storcli_all_prj_write_repair_check(storcli_read_rq_p->layout,
                                                    working_ctx_p->prj_ctx);
    if (ret == 0)
    {
       /*
       ** there is some pending write
       */
       return;
    }   
  
fail:
     /*
     ** release the root transaction context
     */
     STORCLI_STOP_NORTH_PROF(working_ctx_p,repair,0);
     rozofs_storcli_release_context(working_ctx_p);  
  return;

}

/*
**__________________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure on a projection write request
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */

void rozofs_storcli_write_repair_req_processing_cbk(void *this,void *param) 
{
   uint32_t   seqnum;
   uint32_t   projection_id;
   rozofs_storcli_projection_ctx_t  *repair_prj_work_p = NULL;   
   rozofs_storcli_ctx_t *working_ctx_p = (rozofs_storcli_ctx_t*) param ;
   XDR       xdrs;       
   uint8_t  *payload;
   int      bufsize;
   sp_status_ret_t   rozofs_status;
   struct rpc_msg  rpc_reply;
   storcli_read_arg_t *storcli_read_rq_p = NULL;
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   int lbg_id;

   
   int status;
   void     *recv_buf = NULL;   
   int      ret;
   int error = 0;

    storcli_read_rq_p = (storcli_read_arg_t*)&working_ctx_p->storcli_read_arg;
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
      ** not the right sequence number, so drop the received message
      */
      goto drop_msg;    
    }
    /*
    ** check if the write is already doen: this might happen in the case when the same projection
    ** is sent twoards 2 different LBG
    */    
    if (working_ctx_p->prj_ctx[projection_id].prj_state == ROZOFS_PRJ_WR_DONE)
    {
      /*
      ** The reponse has already been received for that projection so we don't care about that
      ** extra reponse
      */
      goto drop_msg;       
    }
    /*    
    ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
    */

    STORCLI_STOP_NORTH_PROF((&working_ctx_p->prj_ctx[projection_id]),repair_prj,0);
    status = rozofs_tx_get_status(this);
    if (status < 0)
    {

       /*
       ** something wrong happened: assert the status in the associated projection id sub-context
       ** now, double check if it is possible to retry on a new storage
       */
       working_ctx_p->prj_ctx[projection_id].prj_state = ROZOFS_PRJ_WR_ERROR;
       working_ctx_p->prj_ctx[projection_id].errcode   = rozofs_tx_get_errno(this);
       errno = rozofs_tx_get_errno(this);  
       if (errno == ETIME)
       {
         storcli_lbg_cnx_sup_increment_tmo(lbg_id);
         STORCLI_ERR_PROF(repair_prj_tmo);
       }
       else
       {
         STORCLI_ERR_PROF(repair_prj_err);
       } 
       error = 1;      
    }
    else
    {    
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
	 error = EFAULT;  
	 working_ctx_p->prj_ctx[projection_id].prj_state = ROZOFS_PRJ_WR_ERROR;
	 working_ctx_p->prj_ctx[projection_id].errcode = error;
	 STORCLI_ERR_PROF(repair_prj_err);
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
	   TX_STATS(ROZOFS_TX_DECODING_ERROR);
	   errno = EPROTO;
	   STORCLI_ERR_PROF(repair_prj_err);
           error = 1;
           break;
	}
	/*
	** decode the status of the operation
	*/
	if (xdr_sp_status_ret_t(&xdrs,&rozofs_status)!= TRUE)
	{
          errno = EPROTO;
  	  STORCLI_ERR_PROF(repair_prj_err);
          error = 1;
          break;    
	}
	/*
	** check th estatus of the operation
	*/
	if ( rozofs_status.status != SP_SUCCESS )
	{
           errno = rozofs_status.sp_status_ret_t_u.error;
           error = 1;
   	   STORCLI_ERR_PROF(repair_prj_err);
           break;    
	}
	break;
      }
    }
    /*
    ** check the status of the operation
    */
    if (error)
    {
       storcli_repair_stat.recv_failure++; 
       /*
       ** there was an error on the remote storage while attempt to write the file
       ** try to write the projection on another storaged
       */
       working_ctx_p->prj_ctx[projection_id].prj_state = ROZOFS_PRJ_WR_ERROR;
       working_ctx_p->prj_ctx[projection_id].errcode   = errno;  
    }
    else
    {
       storcli_repair_stat.recv_success++; 
       /*
       ** set the pointer to the read context associated with the projection for which a response has
       ** been received
       */
       repair_prj_work_p = &working_ctx_p->prj_ctx[projection_id];
       /*
       ** set the status of the transaction to done for that projection
       */
       repair_prj_work_p->prj_state = ROZOFS_PRJ_WR_DONE;
       repair_prj_work_p->errcode   = errno;
    }
    /*
    ** OK now check if we have send enough projection
    ** if it is the case, the distribution will be valid
    */
    ret = rozofs_storcli_all_prj_write_repair_check(storcli_read_rq_p->layout,
                                                    working_ctx_p->prj_ctx);
    if (ret == 0)
    {
       /*
       ** no enough projection 
       */
       goto wait_more_projection;
    }
    /*
    ** caution lock can be asserted either by a write retry attempt or an initial attempt
    */
    if (working_ctx_p->write_ctx_lock != 0) goto wait_more_projection;
    /*
    ** repair is finished,
    ** release the root context and the transaction context
    */
    if(recv_buf!= NULL) ruc_buf_freeBuffer(recv_buf);       

    STORCLI_STOP_NORTH_PROF(working_ctx_p,repair,0);
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
    if(recv_buf!= NULL) ruc_buf_freeBuffer(recv_buf);       
    rozofs_tx_free_from_ptr(this);
    /*
    ** caution lock can be asserted either by a write retry attempt or an initial attempt
    */
    if (working_ctx_p->write_ctx_lock != 0) return;
    /*
    ** release the root transaction context
    */
    STORCLI_STOP_NORTH_PROF(working_ctx_p,repair,0);
    rozofs_storcli_release_context(working_ctx_p);  
    return;

        
wait_more_projection:    
    /*
    ** need to wait for some other write transaction responses
    ** 
    */
    if(recv_buf!= NULL) ruc_buf_freeBuffer(recv_buf);           
    rozofs_tx_free_from_ptr(this);
    return;


}

