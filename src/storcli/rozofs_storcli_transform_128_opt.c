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
#define FUSE_USE_VERSION 26
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>

#include <rozofs/rozofs.h>
#include <rozofs/rozofs_srv.h>

#include "rozofs_storcli_transform.h"
#include "rozofs_storcli.h"

/**
* Local variables
*/




/*
**__________________________________________________________________________
*/
/** 
  Apply the transform to a buffer starting at "data". That buffer MUST be ROZOFS_BSIZE
  aligned.
  The first_block_idx is the index of a ROZOFS_BSIZE array in the output buffer
  The number_of_blocks is the number of ROZOFS_BSIZE that must be transform
  Notice that the first_block_idx offset applies to the output transform buffer only
  not to the input buffer pointed by "data".
  
 * 
 * @param *prj_ctx_p: pointer to the working array of the projection
 * @param first_block_idx: index of the first block to transform
 * @param number_of_blocks: number of blocks to write
 * @param *data: pointer to the source data that must be transformed
   @param *number_of_blocks_p: pointer to the array where the function returns number of blocks on which the transform was applied
  @param *rozofs_storcli_prj_idx_table: pointer to the array used for storing the projections index for inverse process
 *
 * @return: the length written on success, -1 otherwise (errno is set)
 */
 int rozofs_storcli_transform_inverse(rozofs_storcli_projection_ctx_t *prj_ctx_p,  
                                       uint8_t layout, uint32_t bsize,
                                       uint32_t first_block_idx, 
                                       uint32_t number_of_blocks, 
                                       rozofs_storcli_inverse_block_t *block_ctx_p,
                                       char *data,
                                       uint32_t *number_of_blocks_p,
				       uint8_t  *rozofs_storcli_prj_idx_table) 

 {

    int block_idx;
    uint16_t projection_id = 0;
    int prj_ctx_idx;
    *number_of_blocks_p = 0;    
    uint8_t rozofs_inverse = rozofs_get_rozofs_inverse(layout);
    uint8_t rozofs_forward = rozofs_get_rozofs_forward(layout);
    uint8_t rozofs_safe = rozofs_get_rozofs_safe(layout);        
    uint32_t bbytes = ROZOFS_BSIZE_BYTES(bsize);

    int prj_size_in_msg = rozofs_get_max_psize_in_msg(layout,bsize);

    /*
    ** Proceed the inverse data transform for the nb_projections2read blocks.
    */
    for (block_idx = 0; block_idx < number_of_blocks; block_idx++) {
        if (block_ctx_p[block_idx].state == ROZOFS_BLK_TRANSFORM_DONE)
        {
          /*
          ** transformation has already been done for that block of ROZOFS_BSIZE siz
          ** check the next one
          */
          continue;        
        }
        /*
        ** Check the case of the file that has no data (there is a hole in the file), this is indicated by
        ** reporting a timestamp of 0
        */
        if ((block_ctx_p[block_idx].timestamp == 0)  && (block_ctx_p[block_idx].effective_length == bbytes ))
        {
          /*
          ** clear the memory
          */
          ROZOFS_STORCLI_STATS(ROZOFS_STORCLI_EMPTY_READ);
          memset( data + (bbytes * (first_block_idx + block_idx)),0,bbytes);
          block_ctx_p[block_idx].state = ROZOFS_BLK_TRANSFORM_DONE;
          continue;
        
        }	                                                              
        if ((block_ctx_p[block_idx].timestamp == 0)  && (block_ctx_p[block_idx].effective_length == 0 ))
        {
          /*
          ** we have reached end of file
          */
          block_ctx_p[block_idx].state = ROZOFS_BLK_TRANSFORM_DONE;
          *number_of_blocks_p = (block_idx++);
          
          return 0;        
        }      
	
        /*
        ** Here we have to take care, since the index of the projection_id use to address
        ** prj_ctx_p is NOT the real projection_id. The projection ID is found in the header of
        ** each bins, so for a set of projections pointed by bins, we might have a different
        ** projection id in the header of the projections contains in the bins array that has
        ** been read!!
        */
	transform_inverse_proc(&rozofs_storcli_prj_idx_table[ROZOFS_SAFE_MAX_STORCLI*block_idx],
			       prj_ctx_p,
			       prj_size_in_msg,
			       layout,
			       bbytes,
			       first_block_idx,
			       block_idx,
			       data);
        /*
        ** indicate that transform has been done for the projection
        */
        block_ctx_p[block_idx].state = ROZOFS_BLK_TRANSFORM_DONE;
        /*
        ** check the case of a block that is not full: need to zero's that part
        */
        if (block_ctx_p[block_idx].effective_length < bbytes)
        {
           /*
           ** clear the memory
           */
           char *raz_p = data + (bbytes * (first_block_idx + block_idx)) + block_ctx_p[block_idx].effective_length;
           memset( raz_p,0,(bbytes-block_ctx_p[block_idx].effective_length) );
        }
    }
    /*
    ** now the inverse transform is finished, release the allocated ressources used for
    ** rebuild
    */
    *number_of_blocks_p = number_of_blocks;
    
    /*
    ** Check whether a block should be repaired
    */
    rozofs_storcli_check_block_2_repair(prj_ctx_p, rozofs_inverse, rozofs_forward, rozofs_safe, prj_size_in_msg, number_of_blocks, block_ctx_p);
    
    return 0;   
}


/*
**__________________________________________________________________________
*/
/** 
  Apply the transform to a buffer starting at "data". That buffer MUST be ROZOFS_BSIZE
  aligned.
  The first_block_idx is the index of a ROZOFS_BSIZE array in the output buffer
  The number_of_blocks is the number of ROZOFS_BSIZE that must be transform
  Notice that the first_block_idx offset applies to the output transform buffer only
  not to the input buffer pointed by "data".
  
 * 
 * @param *prj_ctx_p: pointer to the working array of the projection
 * @param first_block_idx: index of the first block to transform
 * @param number_of_blocks: number of blocks to write
 * @param timestamp: date in microseconds
   @param last_block_size: effective length of the last block
 * @param *data: pointer to the source data that must be transformed
 *
 * @return: the length written on success, -1 otherwise (errno is set)
 */
 int rozofs_storcli_transform_forward(rozofs_storcli_projection_ctx_t *prj_ctx_p,  
                                       uint8_t layout,uint32_t bsize,
                                       uint32_t first_block_idx, 
                                       uint32_t number_of_blocks,
                                       uint64_t timestamp, 
                                       uint16_t last_block_size,
                                       char *data) 
 {

    projection_t rozofs_fwd_projections[ROZOFS_SAFE_MAX_STORCLI];
    projection_t *projections; // Table of projections used to transform data
    uint16_t projection_id = 0;
    uint32_t i = 0;    
    uint8_t rozofs_forward = rozofs_get_rozofs_forward(layout);
    uint8_t rozofs_inverse = rozofs_get_rozofs_inverse(layout);
    int empty_block = 0;
    uint32_t bbytes = ROZOFS_BSIZE_BYTES(bsize);

    projections = rozofs_fwd_projections;
    int prj_size_in_msg = rozofs_get_max_psize_in_msg(layout,bsize);
    
    /* Transform the data */
    // For each block to send
    for (i = 0; i < number_of_blocks; i++) 
    {
         empty_block = rozofs_data_block_check_empty(data + (i * bbytes), bbytes);

        // seek bins for each projection
        for (projection_id = 0; projection_id < rozofs_forward; projection_id++) 
        {
          /*
          ** Indicates the memory area where the transformed data must be stored
          */
          projections[projection_id].bins = prj_ctx_p[projection_id].bins 
	                                  + (prj_size_in_msg/sizeof(bin_t)) * (first_block_idx+i);
          rozofs_stor_bins_hdr_t *rozofs_bins_hdr_p = (rozofs_stor_bins_hdr_t*)projections[projection_id].bins;
          rozofs_stor_bins_footer_t *rozofs_bins_foot_p = (rozofs_stor_bins_footer_t*)
	                                                  ((bin_t*)(rozofs_bins_hdr_p+1)+
							  rozofs_get_psizes(layout,bsize,projection_id));
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
            continue;   
          }
          /*
          ** fill the header of the projection
          */
          rozofs_bins_hdr_p->s.projection_id = projection_id;
          rozofs_bins_hdr_p->s.timestamp     = timestamp;
          rozofs_bins_hdr_p->s.filler = 0;    
          rozofs_bins_hdr_p->s.version = 0;   
	  /*
          ** set the effective size of the block. It is always ROZOFS_BSIZE except for the last block
          */
          if (i == (number_of_blocks-1))
          {
            rozofs_bins_hdr_p->s.effective_length = last_block_size;
          }
          else
          {
            rozofs_bins_hdr_p->s.effective_length = bbytes;          
          } 
          /*
          ** update the pointer to point out the first bins
          */
          projections[projection_id].bins += sizeof(rozofs_stor_bins_hdr_t)/sizeof(bin_t);
	  rozofs_bins_foot_p = (rozofs_stor_bins_footer_t*) (projections[projection_id].bins
	                                                      + rozofs_get_psizes(layout,bsize,projection_id));
          rozofs_bins_foot_p->timestamp      = timestamp;                                                     
        }
        /*
        ** do not apply transform for empty block
        */
        if (empty_block == 0)
        {
	  transform_forward_proc(layout,data + (i * bbytes),bbytes,projections);
	} 

    }
    return 0;
}
 
