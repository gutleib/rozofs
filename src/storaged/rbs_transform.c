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

#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>

#include <rozofs/rozofs.h>
#include <rozofs/rozofs_srv.h>
#include "rbs_transform.h"

// Local variables
rbs_timestamp_ctx_t rbs_timestamp_tb[ROZOFS_SAFE_MAX];
uint8_t rbs_timestamp_next_free_idx;

projection_t rbs_projections[ROZOFS_SAFE_MAX];
angle_t rbs_angles[ROZOFS_SAFE_MAX];
uint16_t rbs_psizes[ROZOFS_SAFE_MAX];
uint8_t rbs_prj_idx_table[ROZOFS_SAFE_MAX];

int rbs_check_timestamp_tb(rbs_projection_ctx_t *prj_ctx_p, uint8_t layout, uint32_t bsize,
        uint32_t block_idx, uint8_t *prj_idx_tb_p, uint64_t *timestamp_p,
        uint16_t *effective_length_p) {

    uint8_t prj_ctx_idx = 0;
    uint8_t prjid;
    uint8_t ts_entry_idx = 0;
    rbs_timestamp_ctx_t *ts_ctx_p = NULL;
    uint64_t ts_empty_count = 0;
    uint8_t rozofs_inverse = rozofs_get_rozofs_inverse(layout);
    uint8_t rozofs_safe = rozofs_get_rozofs_safe(layout);
    uint8_t rozofs_forward = rozofs_get_rozofs_forward(layout);
    uint16_t rozofs_max_psize_in_msg = rozofs_get_max_psize_in_msg(layout,bsize);
    
    *timestamp_p = 0;
    rbs_timestamp_next_free_idx = 0;

    // For each projection
    for (prj_ctx_idx = 0; prj_ctx_idx < rozofs_safe; prj_ctx_idx++) {

        // Check projection state
        if (prj_ctx_p[prj_ctx_idx].prj_state != PRJ_READ_DONE) {
            // This projection context does not contain valid data, so skip it
            continue;
        }
        if ((block_idx+1) > prj_ctx_p[prj_ctx_idx].nbBlocks) {
	  continue;
	} 

        // Get the pointer to the projection header
        rozofs_stor_bins_hdr_t *rozofs_bins_hdr_p = (rozofs_stor_bins_hdr_t*)
                (prj_ctx_p[prj_ctx_idx].bins + ((rozofs_max_psize_in_msg/sizeof(bin_t)) * block_idx));
		
        if (rozofs_bins_hdr_p->s.projection_id < rozofs_forward) {	

          rozofs_stor_bins_footer_t *rozofs_bins_foot_p = (rozofs_stor_bins_footer_t*)
	          ((bin_t*)(rozofs_bins_hdr_p+1)+rozofs_get_psizes(layout,bsize,rozofs_bins_hdr_p->s.projection_id));

          // Header and footer have different time stamp.
	  // Let's consider this block invalid
          if (rozofs_bins_hdr_p->s.timestamp != rozofs_bins_foot_p->timestamp) {
	      rozofs_bins_hdr_p->s.timestamp = 0;
	  }
	}
	else {
	  rozofs_bins_hdr_p->s.timestamp = 0;
	}  

        prjid = rozofs_bins_hdr_p->s.projection_id;
	/*
	** check the case of the crc error
	*/
	if (prjid == 0xff) continue;
	
        // Case of ts = 0
        if (rozofs_bins_hdr_p->s.timestamp == 0) {
                // Update count
                ts_empty_count += 1;

                if (ts_empty_count == rozofs_inverse) {
                    // OK we have found enough projections
                    *timestamp_p = 0;
                    *effective_length_p = rozofs_bins_hdr_p->s.effective_length;
                    return 1;
                } else {
                    // No enough projections
                    continue;
                }
        }

        // First valid projection
        if (rbs_timestamp_next_free_idx == 0) {
            ts_ctx_p = &rbs_timestamp_tb[rbs_timestamp_next_free_idx];
            ts_ctx_p->timestamp = rozofs_bins_hdr_p->s.timestamp;
	    ts_ctx_p->prjid_bitmap  = (1<<prjid); // set the projection id in the bitmap      	    
            ts_ctx_p->count = 0;
            ts_ctx_p->prj_idx_tb[ts_ctx_p->count] = prj_ctx_idx;
            ts_ctx_p->count++;
            rbs_timestamp_next_free_idx++;
            continue;
        }

        // More than 1 entry in the timestamp table
        for (ts_entry_idx = 0; ts_entry_idx < rbs_timestamp_next_free_idx;
                ts_entry_idx++) {

            ts_ctx_p = &rbs_timestamp_tb[ts_entry_idx];

            if (rozofs_bins_hdr_p->s.timestamp != ts_ctx_p->timestamp)
                    continue;
	    /*
	    ** Check whether the same projection id is already in the list
	    */
	    if ((rozofs_bins_hdr_p->s.timestamp!=0)&&(ts_ctx_p->prjid_bitmap & (1<<prjid))) {	  
	      break;
            }
	    ts_ctx_p->prjid_bitmap |= (1<<prjid); // set the projection id in the bitmap      	
	

            // Same timestamp: register the projection index and check if we
            // have reached rozofs_inverse projections to stop the search
            ts_ctx_p->prj_idx_tb[ts_ctx_p->count] = prj_ctx_idx;
            ts_ctx_p->count++;

            if (ts_ctx_p->count == rozofs_inverse) {

                // OK we have the right number of projection so we can leave
                memcpy(prj_idx_tb_p, ts_ctx_p->prj_idx_tb, rozofs_inverse);

                // Assert the timestamp that is common to all projections used
                // to rebuild that block
                *timestamp_p = ts_ctx_p->timestamp;
                *effective_length_p = rozofs_bins_hdr_p->s.effective_length;
                return 1;
            }
	    break;
            // Try next
        }

        if (ts_entry_idx == rbs_timestamp_next_free_idx) {
            // This timestamp does not exist, so create an entry for it
            ts_ctx_p = &rbs_timestamp_tb[rbs_timestamp_next_free_idx];
	    ts_ctx_p->prjid_bitmap  = (1<<prjid); // set the projection id in the bitmap
            ts_ctx_p->timestamp = rozofs_bins_hdr_p->s.timestamp;
            ts_ctx_p->count = 0;
            ts_ctx_p->prj_idx_tb[ts_ctx_p->count] = prj_ctx_idx;
            ts_ctx_p->count++;
            rbs_timestamp_next_free_idx++;
	}
    }


    // We did not find rozof_inverse projections with the same timestamp
    // we need to read one more projection unless we already attempt to read
    // rozofs_safe projection or we run out of storage that are up among the set
    // of rozofs_safe storage

    return -1;
}
int rbs_count_timestamp_tb(rbs_projection_ctx_t *prj_ctx_p, int spare_idx, uint8_t layout, uint32_t bsize,
        uint32_t block_idx, uint8_t *prj_idx_tb_p, uint64_t *timestamp_p,
        uint16_t *effective_length_p) {

    uint8_t prj_ctx_idx = 0;
    uint8_t ts_entry_idx = 0;
    rbs_timestamp_ctx_t *ts_ctx_p = NULL;
    uint8_t rozofs_inverse = rozofs_get_rozofs_inverse(layout);
    uint8_t rozofs_safe = rozofs_get_rozofs_safe(layout);
    uint8_t rozofs_forward = rozofs_get_rozofs_forward(layout);
    uint8_t and_the_winner_is = -1;
    uint8_t prjid;    
    uint16_t rozofs_max_psize_in_msg = rozofs_get_max_psize_in_msg(layout,bsize);

    *timestamp_p = 0;
    rbs_timestamp_next_free_idx = 0;

    // For each projection
    for (prj_ctx_idx = 0; prj_ctx_idx < rozofs_safe; prj_ctx_idx++) {
    
        if (spare_idx == prj_ctx_idx) continue;

        // Check projection state
        if (prj_ctx_p[prj_ctx_idx].prj_state != PRJ_READ_DONE) {
            // This projection context does not contain valid data, so skip it
            continue;
        }

        // Get the pointer to the projection header
        rozofs_stor_bins_hdr_t *rozofs_bins_hdr_p = (rozofs_stor_bins_hdr_t*)
                (prj_ctx_p[prj_ctx_idx].bins + ((rozofs_max_psize_in_msg/sizeof(bin_t)) * block_idx));
		
        if (rozofs_bins_hdr_p->s.projection_id < rozofs_forward) {	

          rozofs_stor_bins_footer_t *rozofs_bins_foot_p = (rozofs_stor_bins_footer_t*)
	          ((bin_t*)(rozofs_bins_hdr_p+1)+rozofs_get_psizes(layout,bsize,rozofs_bins_hdr_p->s.projection_id));

          // Header and footer have different time stamp.
	  // Let's consider this block invalid
          if (rozofs_bins_hdr_p->s.timestamp != rozofs_bins_foot_p->timestamp) {
	      rozofs_bins_hdr_p->s.timestamp = 0;
	  }
	}
	else {
	  rozofs_bins_hdr_p->s.timestamp = 0;
	}  
        prjid = rozofs_bins_hdr_p->s.projection_id;		

	/*
	** check the case of the crc error
	*/
	if (prjid == 0xff) continue;
        // First valid projection
        if (rbs_timestamp_next_free_idx == 0) {
            ts_ctx_p = &rbs_timestamp_tb[rbs_timestamp_next_free_idx];
	    ts_ctx_p->prjid_bitmap  = (1<<prjid); // set the projection id in the bitmap 	    
            ts_ctx_p->timestamp = rozofs_bins_hdr_p->s.timestamp;
            ts_ctx_p->count = 0;
            ts_ctx_p->prj_idx_tb[ts_ctx_p->count] = prj_ctx_idx;
            ts_ctx_p->count++;
            rbs_timestamp_next_free_idx++;
            continue;
        }

        // More than 1 entry in the timestamp table
        for (ts_entry_idx = 0; ts_entry_idx < rbs_timestamp_next_free_idx;
                ts_entry_idx++) {

            ts_ctx_p = &rbs_timestamp_tb[ts_entry_idx];

            if (rozofs_bins_hdr_p->s.timestamp != ts_ctx_p->timestamp)
                continue;
            /*
	    ** Check whether the same projection id is already in the list
	    */
	    if ((rozofs_bins_hdr_p->s.timestamp!=0) && (ts_ctx_p->prjid_bitmap & (1<<prjid))) {
	      continue;
            }
	    ts_ctx_p->prjid_bitmap |= (1<<prjid); // set the projection id in the bitmap      	

            // Same timestamp: register the projection index and check if we
            // have reached rozofs_inverse projections to stop the search
            ts_ctx_p->prj_idx_tb[ts_ctx_p->count] = prj_ctx_idx;
            ts_ctx_p->count++;

            if (ts_ctx_p->count >= rozofs_inverse) {

                and_the_winner_is = ts_entry_idx;

                // Assert the timestamp that is common to all projections used
                // to rebuild that block
                *timestamp_p = ts_ctx_p->timestamp;
                *effective_length_p = rozofs_bins_hdr_p->s.effective_length;
            }
	    break;
        }
	if (ts_entry_idx < rbs_timestamp_next_free_idx) continue;

        // This timestamp does not exist, so create an entry for it
        ts_ctx_p = &rbs_timestamp_tb[rbs_timestamp_next_free_idx];
	ts_ctx_p->prjid_bitmap  = (1<<prjid); // set the projection id in the bitmap 	    	
        ts_ctx_p->timestamp = rozofs_bins_hdr_p->s.timestamp;
        ts_ctx_p->count = 0;
        ts_ctx_p->prj_idx_tb[ts_ctx_p->count] = prj_ctx_idx;
        ts_ctx_p->count++;
        rbs_timestamp_next_free_idx++;
    }

    // Is there a winer ?
    if (and_the_winner_is != -1) {
    
       ts_ctx_p = &rbs_timestamp_tb[and_the_winner_is];
       memcpy(prj_idx_tb_p, ts_ctx_p->prj_idx_tb, ts_ctx_p->count);
       return ts_ctx_p->count;
    }
    return -1;
}
/*_____________________________________________________________________________________
  Apply the transform to a buffer starting at "data". That buffer MUST be
  ROZOFS_BSIZE aligned.
  The first_block_idx is the index of a ROZOFS_BSIZE array in the output buffer
  The number_of_blocks is the number of ROZOFS_BSIZE that must be transform
  Notice that the first_block_idx offset applies to the output transform buffer
  only not to the input buffer pointed by "data".
  
 * 
 * @param *prj_ctx_p: pointer to the working array of the projections set
 * @param layout: rozofs layout used for store data
 * @param bsize: Block size as define in enum ROZOFS_BSIZE_E 
 * @param first_block_idx: index of the first block to transform
 * @param number_of_blocks: number of blocks to write
 * @param *block_ctx_p: pointer to the working array of blocks
 * @param *data: pointer to the source data that must be transformed
 * param  *empty: returns whether this is a whole set of empty blocks
 *
 * @return: 0 on success -1 otherwise
 */
int rbs_transform_inverse(rbs_projection_ctx_t *prj_ctx_p, uint8_t layout, uint32_t bsize,
        uint32_t first_block_idx, uint32_t number_of_blocks,
        rbs_inverse_block_t *block_ctx_p, char *data, int * empty) {

    projection_t *projections = NULL;
    int block_idx = 0;
    uint16_t projection_id = 0;
    int prj_ctx_idx = 0;
    int ret = -1;
    int bbytes = ROZOFS_BSIZE_BYTES(bsize);
    uint16_t rozofs_max_psize_in_msg = rozofs_get_max_psize_in_msg(layout,bsize);

    /*
    ** The blocks are empty (a priori)
    */
    * empty = 1;
    
    uint8_t rozofs_inverse = rozofs_get_rozofs_inverse(layout);

    projections = rbs_projections;

    // Proceed the inverse data transform for the number_of_blocks blocks.
    for (block_idx = 0; block_idx < number_of_blocks; block_idx++) {

        if (block_ctx_p[block_idx].state == BLK_TRANSFORM_DONE) {
            /*
            ** Count the number of empty blocks
            */
            if (block_ctx_p[block_idx].timestamp != 0) * empty = 0;
            // Transformation has already been done for that block
            // check the next one
            continue;
        }

        // Check if we can find out a set of rozofs_inverse projections that
        // will permit to rebuild the current block of ROZOFS_BSIZE size
        // For this we check if we can find at least rozofs_inverse projections
        // with the same time stamp and with different angles(projection id)
        // If there is no enough valid projection we need to read a new
        // projection on the next storage in sequence.
        // It might be possible that we run out of storage since rozofs_safe
        // has been reached and we have not reached rozofs_inserse projection !!


        ret = rbs_check_timestamp_tb(prj_ctx_p, layout, bsize, block_idx,
                rbs_prj_idx_table, &block_ctx_p[block_idx].timestamp,
                &block_ctx_p[block_idx].effective_length);
        if (ret < 0) {
            // The set of projection that have been read does not permit to
            // rebuild the block, need to read more
            return -1;
        }

        // Check the case of the file that has no data (there is a hole in the
        // file), this is indicated by reporting a timestamp of 0
        if (block_ctx_p[block_idx].timestamp == 0) {
            // Clear the memory
            memset(data + (bbytes * (first_block_idx + block_idx)), 0,
                    bbytes);
            block_ctx_p[block_idx].state = BLK_TRANSFORM_DONE;  
            continue;
        }

        // Here we have to take care, since the index of the projection_id use
        // to address prj_ctx_p is NOT the real projection_id.
        // The projection ID is found in the header of each bins, so for a set
        // of projections pointed by bins, we might have a different projection
        // id in the header of the projections contains in the bins array that
        // has been read!!

        int prj_count = 0;
        for (prj_count = 0; prj_count < rozofs_inverse; prj_count++) {

            // Get the pointer to the beginning of the projection and extract
            // the projection ID
            prj_ctx_idx = rbs_prj_idx_table[prj_count];

            rozofs_stor_bins_hdr_t *rozofs_bins_hdr_p =
                    (rozofs_stor_bins_hdr_t*) (prj_ctx_p[prj_ctx_idx].bins + ((rozofs_max_psize_in_msg/sizeof(bin_t)) * block_idx));

            // Extract the projection_id from the header and fill the table
            // of projections for the block block_idx for each projection
            projection_id = rozofs_bins_hdr_p->s.projection_id;
            projections[prj_count].angle.p = rozofs_get_angles_p(layout,
                    projection_id);
            projections[prj_count].angle.q = rozofs_get_angles_q(layout,
                    projection_id);
            projections[prj_count].size = rozofs_get_psizes(layout,bsize,
                    projection_id);
            projections[prj_count].bins = (bin_t*) (rozofs_bins_hdr_p + 1);
        }


        // Inverse data for the block (first_block_idx + block_idx)
        transform128_inverse_copy((pxl_t *) (data + (bbytes *(first_block_idx + block_idx))),
                rozofs_inverse,
                bbytes / rozofs_inverse / sizeof (pxl_t),
                rozofs_inverse, projections,
		rozofs_get_max_psize(layout,bsize)*sizeof(bin_t));

        // Indicate that transform has been done for the projection
        block_ctx_p[block_idx].state = BLK_TRANSFORM_DONE;
        /*
        ** This is not an empty block
        */
        * empty = 0;
    }

    // Now the inverse transform is finished
    return 0;
}

int rbs_transform_forward_one_proj(rbs_projection_ctx_t * prj_ctx_p,
        rbs_inverse_block_t * block_ctx_p, uint8_t layout, uint32_t bsize,
        uint32_t first_block_idx, uint32_t number_of_blocks,
        tid_t projection_id, char *data) {

    projection_t *projections = NULL; // Table of projections used
    uint32_t i = 0;
    uint16_t rozofs_max_psize_in_msg = rozofs_get_max_psize_in_msg(layout,bsize);

    int bbytes = ROZOFS_BSIZE_BYTES(bsize);
    uint8_t rozofs_inverse = rozofs_get_rozofs_inverse(layout);
    projections = rbs_projections;
    projections[projection_id].angle.p = rozofs_get_angles_p(layout,
            projection_id);
    projections[projection_id].angle.q = rozofs_get_angles_q(layout,
            projection_id);
    projections[projection_id].size = rozofs_get_128bits_psizes(layout, bsize, projection_id);

    // For each block to send
    for (i = 0; i < number_of_blocks; i++) {

        // Indicates the memory area where the transformed data must be stored
        projections[projection_id].bins = prj_ctx_p[projection_id].bins +
                ((rozofs_max_psize_in_msg/sizeof(bin_t))*(first_block_idx + i));

        rozofs_stor_bins_hdr_t *rozofs_bins_hdr_p = (rozofs_stor_bins_hdr_t*)
                projections[projection_id].bins;
        rozofs_stor_bins_footer_t *rozofs_bins_foot_p = (rozofs_stor_bins_footer_t*)
                ((bin_t*)(rozofs_bins_hdr_p+1)+rozofs_get_psizes(layout,bsize,projection_id));

        // Fill the header of the projection
        rozofs_bins_hdr_p->s.projection_id = projection_id;
        rozofs_bins_hdr_p->s.timestamp =
                block_ctx_p[first_block_idx + i].timestamp;
        rozofs_bins_hdr_p->s.version = 0;
        rozofs_bins_hdr_p->s.filler = 0;

        // Set the effective size for this block
        rozofs_bins_hdr_p->s.effective_length =
                block_ctx_p[first_block_idx + i].effective_length;

        // Update the pointer to point out the first bins
        projections[projection_id].bins +=
                (sizeof (rozofs_stor_bins_hdr_t) / sizeof (bin_t));

        transform128_forward_one_proj((pxl_t *) (data + (i * bbytes)),
                rozofs_inverse,
                bbytes / rozofs_inverse / sizeof (pxl_t),
                projection_id, projections);

        // Write footer after transform
        rozofs_bins_foot_p->timestamp =
                block_ctx_p[first_block_idx + i].timestamp;



    }
    return 0;
}
