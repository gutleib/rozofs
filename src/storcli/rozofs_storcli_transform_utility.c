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
DECLARE_PROFILING(stcpp_profiler_t);


/**
* Local variables
*/

/*
**__________________________________________________________________________
*/
/**
*   API to update in the internal structure associated with the projection
    the header of each blocks
    That function is required since the read can return less blocks than expected
    so we might face the situation where the system check headers in memory
    on an array that has not be updated
    We need also to consider the case of the end of file as well as the 
    case where blocks has been reserved but not yet written (file with holes).
    For these two cases we might have a timestam of 0 so we need to use
    the effective length to discriminate between a hole (0's array on BSIZE length)
    and a EOF case where length is set to 0.
    
    @param prj_ctx_p : pointer to the projection context
    @param layout : layout associated with the file
    @param number_of_blocks_returned : number of blocks in the projection
    @param number_of_blocks_requested : number of blocks requested
    @param raw_file_size : raw file_size reported from a fstat on the projection file (on storage)
    
    @retval none
*/     
void rozofs_storcli_transform_update_headers(rozofs_storcli_projection_ctx_t *prj_ctx_p, 
                                             uint8_t  layout, uint32_t bsize,
                                             uint32_t number_of_blocks_returned,
                                             uint32_t number_of_blocks_requested,
                                             uint64_t raw_file_size)
{

    int block_idx;
    rozofs_stor_bins_footer_t *rozofs_bins_foot_p;
    prj_ctx_p->raw_file_size = raw_file_size;
    uint32_t bbytes = ROZOFS_BSIZE_BYTES(bsize);
    rozofs_stor_bins_hdr_t* rozofs_bins_hdr_p;
    int prj_size_in_msg =  rozofs_get_max_psize_in_msg(layout,bsize);
    int prj_effective_size = 0; 
    		        		    	       
    for (block_idx = 0; block_idx < number_of_blocks_returned; block_idx++) 
    {
      /*
      ** Get the pointer to the beginning of the block and extract its header
      */
      rozofs_bins_hdr_p = (rozofs_stor_bins_hdr_t*) (prj_ctx_p->bins 
                        + (prj_size_in_msg/sizeof(bin_t)) * block_idx);

      memcpy(&prj_ctx_p->rcv_hdr[block_idx], rozofs_bins_hdr_p, sizeof(rozofs_stor_bins_hdr_t));
      
      /*
      ** take care of the crc errors
      */
      if (rozofs_bins_hdr_p->s.projection_id == 0xff)
      {
        ROZOFS_BITMAP64_SET(block_idx,prj_ctx_p->crc_err_bitmap);
        prj_ctx_p->block_hdr_tab[block_idx].s.projection_id = 0;      	
        prj_ctx_p->block_hdr_tab[block_idx].s.timestamp = 0;
        prj_ctx_p->block_hdr_tab[block_idx].s.effective_length = bbytes;
	STORCLI_ERR_PROF(read_blk_crc); 
	continue;	  
      }
            
      /*
      ** Empty block
      */	    
      if (rozofs_bins_hdr_p->s.timestamp == 0)
      {
        prj_ctx_p->block_hdr_tab[block_idx].s.projection_id = 0;      
        prj_ctx_p->block_hdr_tab[block_idx].s.timestamp = rozofs_bins_hdr_p->s.timestamp;
        prj_ctx_p->block_hdr_tab[block_idx].s.effective_length = bbytes; 
	continue;         
      }
      /*
      ** Retrieve effective projection size from the projection id
      ** Should only occur when CRC32 feature is not enabled since prj id is protected by CRC
      */
      prj_effective_size = rozofs_get_psizes(layout,bsize,rozofs_bins_hdr_p->s.projection_id);
      
      /*
      ** Out of range projection id
      */
      if (prj_effective_size == 0) {
        prj_ctx_p->block_hdr_tab[block_idx].s.projection_id = 0;      	
        prj_ctx_p->block_hdr_tab[block_idx].s.timestamp = 0;
        prj_ctx_p->block_hdr_tab[block_idx].s.effective_length = bbytes;
	STORCLI_ERR_PROF(read_blk_prjid);         
        continue;
      }
      	
      rozofs_bins_foot_p = (rozofs_stor_bins_footer_t*) ((bin_t*)(rozofs_bins_hdr_p+1)+prj_effective_size);

      /*
      ** Not consistent Header and footer
      ** Should only occur when CRC32 feature is not enabled
      */
      if (rozofs_bins_foot_p->timestamp != rozofs_bins_hdr_p->s.timestamp) 
      {
        prj_ctx_p->block_hdr_tab[block_idx].s.projection_id = 0;      	
        prj_ctx_p->block_hdr_tab[block_idx].s.timestamp = 0;
        prj_ctx_p->block_hdr_tab[block_idx].s.effective_length = bbytes;
	STORCLI_ERR_PROF(read_blk_footer);  
	continue;      
      }
      /*
      ** Normal case
      */
      prj_ctx_p->block_hdr_tab[block_idx].s.projection_id = rozofs_bins_hdr_p->s.projection_id;      	
      prj_ctx_p->block_hdr_tab[block_idx].s.timestamp = rozofs_bins_hdr_p->s.timestamp;
      prj_ctx_p->block_hdr_tab[block_idx].s.effective_length = rozofs_bins_hdr_p->s.effective_length;                 
    }
    /*
    ** clear the part that is after number of returned block (assume end of file)
    */
    for (block_idx = number_of_blocks_returned; block_idx < number_of_blocks_requested; block_idx++)
    {    
      prj_ctx_p->block_hdr_tab[block_idx].s.projection_id = 0;      	
      prj_ctx_p->block_hdr_tab[block_idx].s.timestamp = 0;
      prj_ctx_p->block_hdr_tab[block_idx].s.effective_length = 0;      
    } 
}    

 

/*
**__________________________________________________________________________
*/
/**
*
*/
static inline int rozofs_storcli_transform_inverse_check_timestamp_tb(rozofs_storcli_projection_ctx_t *prj_ctx_p,  
                                       uint8_t layout,
				       uint8_t  bsize,
                                       uint32_t block_idx, 
                                       uint8_t *prj_idx_tb_p,
                                       uint64_t *timestamp_p,
                                       uint16_t *effective_len_p,
				       uint32_t * corrupted_blocks)
{
    uint8_t prj_ctx_idx;
    uint8_t prjid;
    uint8_t timestamp_entry;
    *timestamp_p = 0;
    uint8_t rozofs_inverse = rozofs_get_rozofs_inverse(layout);
    uint8_t rozofs_safe = rozofs_get_rozofs_safe(layout);
    uint8_t rozofs_forward = rozofs_get_rozofs_forward(layout);
    rozofs_storcli_timestamp_ctx_t *p;
    int eof = 1;
    rozofs_storcli_timestamp_ctx_t rozofs_storcli_timestamp_tb[ROZOFS_SAFE_MAX_STORCLI];
    uint8_t  rozofs_storcli_timestamp_next_free_idx=0;
    int      left_storage = rozofs_safe;
    
    for (prj_ctx_idx = 0; prj_ctx_idx < rozofs_safe; prj_ctx_idx++)
    {
      if (prj_ctx_p[prj_ctx_idx].prj_state == ROZOFS_PRJ_READ_ENOENT) 
      {
        left_storage--;
	continue;
      }
      
      if (prj_ctx_p[prj_ctx_idx].prj_state != ROZOFS_PRJ_READ_DONE)
      {
        /*
        ** that projection context does not contain valid data, so skip it
        */
        continue;      
      } 
      left_storage--;

      /*
      ** Get the pointer to the projection header
      */      
      rozofs_stor_bins_hdr_t *rozofs_bins_hdr_p = (rozofs_stor_bins_hdr_t*)&prj_ctx_p[prj_ctx_idx].block_hdr_tab[block_idx];
      /*
      ** check if the current block of the projection contains valid data. The block is invalid when the timestamp and the
      ** effective length are 0. That situation can occur when a storage was in fault at the writing time, so we can face
      ** the situation where the projections read on the different storages do not return the same number of block.
      */
      //if ((rozofs_bins_hdr_p->s.timestamp == 0)&&(rozofs_bins_hdr_p->s.effective_length == 0))  continue;
      /*
      ** check the case of CRC error
      */
      if (rozofs_bins_hdr_p->s.projection_id == 0xff) continue;
      prjid = rozofs_bins_hdr_p->s.projection_id;
      
      if (rozofs_storcli_timestamp_next_free_idx == 0)
      {
        /*
        ** first entry
        */
        eof = 0;
        p = &rozofs_storcli_timestamp_tb[rozofs_storcli_timestamp_next_free_idx];  
	p->prjid_bitmap  = (1<<prjid); // set the projection id in the bitmap      
        p->timestamp     = rozofs_bins_hdr_p->s.timestamp;
        p->effective_length = rozofs_bins_hdr_p->s.effective_length;
        p->count         = 0;
        p->prj_idx_tb[p->count]= prj_ctx_idx;
        p->count++;
        rozofs_storcli_timestamp_next_free_idx++;
        continue;      
      }
      /*
      ** more than 1 entry in the timestamp table
      */
      for(timestamp_entry = 0; timestamp_entry < rozofs_storcli_timestamp_next_free_idx;timestamp_entry++)
      {
        p = &rozofs_storcli_timestamp_tb[timestamp_entry];        
        if ((rozofs_bins_hdr_p->s.timestamp != p->timestamp) || (rozofs_bins_hdr_p->s.effective_length != p->effective_length)) continue;
	
	/*
	** Check whether the same projection id is already in the list
	*/
	if ((rozofs_bins_hdr_p->s.timestamp!=0)&&(p->prjid_bitmap & (1<<prjid))) {	  
	  break;
        }
	p->prjid_bitmap |= (1<<prjid); // set the projection id in the bitmap      	
	
        /*
        ** same timestamp and length: register the projection index and check if we have reached rozofs_inverse projections
        ** to stop the search
        */
        p->prj_idx_tb[p->count]= prj_ctx_idx;
        p->count++;
        if (p->count == rozofs_inverse)
        {
          /*
          ** OK we have the right number of projection so we can leave
          */
          memcpy(prj_idx_tb_p,p->prj_idx_tb,rozofs_inverse);
          /*
          ** assert the timestamp that is common to all projections used to rebuild that block
          */
          *timestamp_p     = p->timestamp;
          *effective_len_p = p->effective_length;
          /*
          ** Mark the projection that MUST be rebuilt
          */
          rozofs_storcli_mark_projection2rebuild(prj_ctx_p,rozofs_storcli_timestamp_tb,timestamp_entry,rozofs_storcli_timestamp_next_free_idx);
          return 1;       
        }
        /*
        ** try next
        */
	break;
      }
      /*
      ** that timestamp does not exist, so create an entry for it
      */
      if (timestamp_entry == rozofs_storcli_timestamp_next_free_idx) {
	p = &rozofs_storcli_timestamp_tb[rozofs_storcli_timestamp_next_free_idx];        
	p->timestamp     = rozofs_bins_hdr_p->s.timestamp;
	p->prjid_bitmap  = (1<<prjid); // set the projection id in the bitmap      	
	p->effective_length = rozofs_bins_hdr_p->s.effective_length;
	p->count     = 0;
	p->prj_idx_tb[p->count]= prj_ctx_idx;
	p->count++;
	rozofs_storcli_timestamp_next_free_idx++;
      }
    }
    /*
    ** take care of the case where we try to read after the end of file
    */
    if (eof) {
      return 0;
    }
    /*
    ** unlucky, we did not find rozof_inverse projections with the same timestamp
    ** we need to read one more projection unless we already attempt to read rozofs_safe
    ** projection or we run out of storage that are up among the set of rozofs_safe storage   
    */
    
    /*
    *** Check whether there is enough letf storage to have a chance to decode this block one day
    */
    for(timestamp_entry = 0; timestamp_entry < rozofs_storcli_timestamp_next_free_idx;timestamp_entry++) {

      p = &rozofs_storcli_timestamp_tb[timestamp_entry];
      /*
      ** A valid TS must be written forward times !
      ** so the number of time a TS is found + the number of non readable storage
      */
      if (left_storage+ p->count >= rozofs_forward) {
          // info("Bloc %d TS %llu has count %d and left %d : this could work", block_idx, p->timestamp, p->count, left_storage);
	  return -1;      
      }
    }
      
    // No way. Even with the left storage, nothing will match...
    // Let's tell the block is empty

    *timestamp_p     = 0;
    *effective_len_p = ROZOFS_BSIZE_BYTES(bsize);
    *corrupted_blocks = *corrupted_blocks + 1;
    // info("Bloc %d Said to be empty",block_idx);      
    return 0;
}



/*
**__________________________________________________________________________
*/
/**
*
*/
static inline int rozofs_storcli_transform_inverse_check(rozofs_storcli_projection_ctx_t *prj_ctx_p,  
                                       uint8_t layout,
				       uint8_t bsize,
                                       uint32_t block_idx, 
                                       uint8_t *prj_idx_tb_p,
                                       uint64_t *timestamp_p,
                                       uint16_t *effective_len_p,
				       uint32_t * corrupted_blocks)
{
    uint8_t prj_ctx_idx;
    uint8_t nb_projection_with_same_timestamp = 0;
    uint8_t rozofs_inverse = rozofs_get_rozofs_inverse(layout);
    uint8_t rozofs_safe = rozofs_get_rozofs_safe(layout);
    int ret;
    int eof = 1;
    *timestamp_p = 0;
    *effective_len_p = 0;
    rozofs_storcli_timestamp_ctx_t ref_ctx={0};        
    rozofs_storcli_timestamp_ctx_t *ref_ctx_p = &ref_ctx;        
    rozofs_storcli_timestamp_ctx_t rozofs_storcli_timestamp_tb[ROZOFS_SAFE_MAX_STORCLI];
    uint8_t  rozofs_storcli_timestamp_next_free_idx=0;
    uint8_t prjid;    
    ref_ctx_p->count = 0;
    
    /*
    ** clean data used for tracking projection to rebuild
    */
    rozofs_storcli_timestamp_ctx_t *p = &rozofs_storcli_timestamp_tb[rozofs_storcli_timestamp_next_free_idx];        
    p->timestamp = 0;
    p->count     = 0;

    for (prj_ctx_idx = 0; prj_ctx_idx < rozofs_safe; prj_ctx_idx++)
    {
      if (prj_ctx_p[prj_ctx_idx].prj_state != ROZOFS_PRJ_READ_DONE)
      {
        /*
        ** that projection context does not contain valid data, so skip it
        */
        continue;      
      }
      /*
      ** Get the pointer to the projection header
      */
      rozofs_stor_bins_hdr_t *rozofs_bins_hdr_p = (rozofs_stor_bins_hdr_t*)&prj_ctx_p[prj_ctx_idx].block_hdr_tab[block_idx];
      prjid = rozofs_bins_hdr_p->s.projection_id;
      /*
      ** skip the invalid blocks
      */
      if ((rozofs_bins_hdr_p->s.timestamp == 0) && (rozofs_bins_hdr_p->s.effective_length==0)) continue;
      /*
      ** check the case of CRC error
      */
      if (rozofs_bins_hdr_p->s.projection_id == 0xff) continue;
      
      if (ref_ctx_p->count == 0)
      {
        /*
        ** first projection found
        */
        eof = 0;
        ref_ctx_p->timestamp     = rozofs_bins_hdr_p->s.timestamp;
	ref_ctx_p->prjid_bitmap  = (1<<prjid); // set the projection id in the bitmap 
        ref_ctx_p->effective_length = rozofs_bins_hdr_p->s.effective_length;
        ref_ctx_p->count++;
        prj_idx_tb_p[nb_projection_with_same_timestamp++] = prj_ctx_idx; 
        continue;            
      }
      /*
      ** the entry is not empty check if the timestamp and the effective length of the block belonging to 
      ** projection prj_ctx_idx matches
      */
      if ((rozofs_bins_hdr_p->s.timestamp == ref_ctx_p->timestamp) &&(rozofs_bins_hdr_p->s.effective_length == ref_ctx_p->effective_length))
      {
	/*
	** Check whether the same projection id is already in the list
	*/
	if ((rozofs_bins_hdr_p->s.timestamp!=0) && (ref_ctx_p->prjid_bitmap & (1<<prjid))) {
	  continue;
        }
	ref_ctx_p->prjid_bitmap |= (1<<prjid); // set the projection id in the bitmap      	
      
        /*
        ** there is a match, store the projection index and check if we have reach rozofs_inverse blocks with the 
        ** same timestamp and length
        */
        ref_ctx_p->count++;
        prj_idx_tb_p[nb_projection_with_same_timestamp++] = prj_ctx_idx; 

        if (nb_projection_with_same_timestamp == rozofs_inverse)
        {
          /*
          ** ok we have found all the projection for the best case
          */
          *timestamp_p     = ref_ctx_p->timestamp;
          *effective_len_p = ref_ctx_p->effective_length;
          /*
          ** Mark the projection that MUST be rebuilt
          */
          if (rozofs_storcli_timestamp_next_free_idx)
          {
             rozofs_storcli_mark_projection2rebuild(prj_ctx_p,
                                                    rozofs_storcli_timestamp_tb,
                                                    rozofs_storcli_timestamp_next_free_idx+1,
                                                    rozofs_storcli_timestamp_next_free_idx);
          }	  
          return (int)rozofs_inverse;        
        }
        continue;      
      }
      /*
      ** Either the length of the timestamp does not match
      ** log the reference of the projection index in order to address a potential rebuild of the
      ** projection
      */
      p->prj_idx_tb[p->count]= prj_ctx_idx;
      p->count++;      
      if (rozofs_storcli_timestamp_next_free_idx == 0)
      {
         rozofs_storcli_timestamp_next_free_idx = 1;
      }        
    }
    /*
    ** check th eof case
    */
    if (eof) return 0;
    /*
    ** unlucky, we did not find rozof_inverse projections with the same timestamp
    ** so we have to find out the projection(s) that are out of sequence
    */
    ret =  rozofs_storcli_transform_inverse_check_timestamp_tb( prj_ctx_p,  
                                        layout,
					bsize,
                                        block_idx, 
                                        prj_idx_tb_p,
                                        timestamp_p,
                                        effective_len_p,
					corrupted_blocks);
    return ret;
}

/*
**__________________________________________________________________________
*/
/**
*  that procedure check if the received projections permit to rebuild
   the initial message

  @param *prj_ctx_p: pointer to the working array of the projection
  @param first_block_idx: index of the first block to transform
  @param number_of_blocks: number of blocks to write
  @param *number_of_blocks_p: pointer to the array where the function returns number of blocks on which the transform was applied
  @param *rozofs_storcli_prj_idx_table: pointer to the array used for storing the projections index for inverse process
  @param *uint32_t *corrupted_blocks : returns the number of corrupted blocks within the read blocks
 
  @return: the length written on success, -1 otherwise (errno is set)
*/
 int rozofs_storcli_transform_inverse_check_for_thread(rozofs_storcli_projection_ctx_t *prj_ctx_p,  
                                       uint8_t layout, uint8_t bsize,
                                       uint32_t first_block_idx, 
                                       uint32_t number_of_blocks, 
                                       rozofs_storcli_inverse_block_t *block_ctx_p,
                                       uint32_t *number_of_blocks_p,
				       uint8_t  *rozofs_storcli_prj_idx_table,
				       uint32_t *corrupted_blocks) 

{

    int block_idx;
    int ret;
   
    *number_of_blocks_p = 0;
    
    for (block_idx = 0; block_idx < number_of_blocks; block_idx++) {
        if (block_ctx_p[block_idx].state == ROZOFS_BLK_TRANSFORM_DONE)
        {
          /*
	  ** that case must not occur!!
	  */
          continue;        
        }
        ret =  rozofs_storcli_transform_inverse_check(prj_ctx_p,layout, bsize,
                                                      block_idx, &rozofs_storcli_prj_idx_table[block_idx*ROZOFS_SAFE_MAX_STORCLI],
                                                      &block_ctx_p[block_idx].timestamp,
                                                      &block_ctx_p[block_idx].effective_length,
						      corrupted_blocks);
        if (ret < 0)
        {
          /*
          ** the set of projection that have been read does not permit to rebuild, need to read more
          */
          return -1;        
        } 
	/*
	** check for end of file
	*/
        if ((block_ctx_p[block_idx].timestamp == 0)  && (block_ctx_p[block_idx].effective_length == 0 ))
        {
          /*
          ** we have reached end of file
          */
          //block_ctx_p[block_idx].state = ROZOFS_BLK_TRANSFORM_DONE;
          *number_of_blocks_p = (block_idx++);
          
          return 0;        
        }      	
    }
    *number_of_blocks_p = (block_idx++);
    return 0;
}


 
