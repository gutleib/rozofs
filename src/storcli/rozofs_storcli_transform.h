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

#ifndef ROZOFS_STORCLI_TRANSFORM_H
#define ROZOFS_STORCLI_TRANSFORM_H

#include <rozofs/rpc/sclient.h>
#include "storage_proto.h"
typedef enum
{
  ROZOFS_BLK_TRANSFORM_REQ = 0,  /**<inverse transformation is requested  */
  ROZOFS_BLK_TRANSFORM_DONE,    /**< storaqe read request issued          */
  ROZOFS_BLK_ERROR,      /**< unable to perform the inverse transformation      */
  ROZOFS_BLK_MAX
} rozofs_blk_transform_state_e;
/**
* structure that tracks the invere transformation state of a block for a set of projection
*/
typedef struct _rozofs_storcli_inverse_block_t
{
  uint8_t state;
  uint16_t effective_length;   /**< effective length of the block --> must be the same on each projection   */
  uint64_t timestamp;   /**< timestamp association with the block -> must be the same on each projection */
} rozofs_storcli_inverse_block_t;

typedef enum
{
  ROZOFS_PRJ_READ_IDLE = 0,  /**< no read initiated for the prjection  */
  ROZOFS_PRJ_READ_IN_PRG,    /**< storaqe read request issued          */
  ROZOFS_PRJ_READ_DONE,      /**< storage has answered                 */
  ROZOFS_PRJ_READ_ERROR,     /**< read error reported by storage      */
  ROZOFS_PRJ_READ_ENOENT,    /**< file does not exist reported by storage      */
  ROZOFS_PRJ_READ_MAX
} rozofs_read_req_state_e;



typedef enum
{
  ROZOFS_PRJ_WR_IDLE = 0,  /**< no write initiated for the prjection  */
  ROZOFS_PRJ_WR_IN_PRG,    /**< storaqe write request issued          */
  ROZOFS_PRJ_WR_DONE,      /**< storage has answered                 */
  ROZOFS_PRJ_WR_ERROR,     /**< write error reported by storage      */
  ROZOFS_PRJ_WR_MAX
} rozofs_write_req_state_e;



typedef struct _rozofs_storcli_projection_ctx_t
{
   uint8_t valid_stor_idx:1; /**< assert to 1 if the relative storage idx is valid */
   uint8_t inuse_valid:1;    /**< assert to 1 if the inuse counter of the buffer has been incremented: just for write case */
   uint8_t stor_idx:6;       /**< relative index of the storage                    */

   uint8_t inuse_valid_missing:1;    /**< assert to 1 if the inuse counter of the buffer has been incremented: just for write case */
   uint8_t retry_cpt:6;        /**< current retry counter                                      */
   uint8_t rebuild_req:1;      /**< a rebuild is needed for that storage for this projection   */

   uint8_t prj_state;        /**< read state see rozofs_read_req_state_e enum      */
   int     errcode;          /**< errno value associated with the ROZOFS_PRJ_WR_ERROR or ROZOFS_PRJ_READ_ERROR state */
   void    *prj_buf;         /**< ruc buffer that contains the payload             */ 
   bin_t  *bins;             /**< pointer to the payload (data read)               */
   void    *prj_buf_missing;         /**< ruc buffer that contains the payload             */ 
   uint64_t timestamp;       /**< monitoring timestamp                             */
   rozofs_stor_bins_hdr_t block_hdr_tab[ROZOFS_MAX_BLOCK_PER_MSG];
   rozofs_stor_bins_hdr_t rcv_hdr[ROZOFS_MAX_BLOCK_PER_MSG];
   uint64_t raw_file_size;    /**< file size reported from a fstat on the projection file */
   uint64_t crc_err_bitmap[ROZOFS_BLOCK_BITMAP_NB_UINT64];   /**< bitmap of the blocks on which a crc error has detected by storaged */
} rozofs_storcli_projection_ctx_t;


/**
* structure used to keep track of the projections that must be used to rebuild a block
*/
typedef struct _rozofs_storcli_timestamp_ctx_t
{
  uint64_t timestamp;      /**< key */
  uint16_t effective_length;  /**< effective length of the block */
  uint8_t  count;      /**< number of projection with the same timestamp */
  uint8_t  prj_idx_tb[ROZOFS_SAFE_MAX_STORCLI];  /**< table of the projection index that have the same timestamp */
  uint32_t prjid_bitmap;
} rozofs_storcli_timestamp_ctx_t;




/**
* Local variables
*/
extern rozofs_storcli_timestamp_ctx_t rozofs_storcli_timestamp_tb[];
extern uint8_t  rozofs_storcli_timestamp_next_free_idx;

extern projection_t rozofs_storcli_projections[];
extern angle_t      rozofs_storcli_angles[];
extern uint16_t     rozofs_storcli_psizes[];
extern uint8_t      rozofs_storcli_prj_idx_table[];

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
                                       char *data) ;
                                       
                                       

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
				       uint8_t  *rozofs_storcli_prj_idx_table);

                 
                                       
/**
*_____________________________________
  REBUILD SECTION
*_____________________________________
*/

/**
* Internal function that identifies the Projections that Must be rebuilt

  @param prj_ctx_p: pointer to the projection table context
  @param timestamp_tb_p: pointer to the timestamp table that contains the projection with the same timestamp
  @param found_entry_idx : index of the entry that we be used for rebuilding the data (must be excluded)
  @param nb_entries : number of entries in the timestamp table
*/

void rozofs_storcli_mark_projection2rebuild(rozofs_storcli_projection_ctx_t *prj_ctx_p,
                                            rozofs_storcli_timestamp_ctx_t *timestamp_tb_p,
                                            uint8_t found_entry_idx,
                                            uint8_t nb_entries);


/*
**__________________________________________________________________________
*/
/**
* Function that check if there is some projection that needs to be rebuilt

  @param prj_ctx_p: pointer to the projection table context
  @param rozof_safe : max number of entries
  
  @retval 0 : no projection to rebuild
  @retval <> 0 : number of projection to rebuild
*/

int rozofs_storcli_check_projection2rebuild(rozofs_storcli_projection_ctx_t *prj_ctx_p,uint8_t rozof_safe);

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
    @param bsize : Block size as defined in ROZOFS_BSIZE_E
    @param number_of_blocks_returned : number of blocks in the projection
    @param number_of_blocks_requested : number of blocks requested
    @param raw_file_size : raw file_size reported from a fstat on the projection file (on storage)
    
    @retval none
*/     
void rozofs_storcli_transform_update_headers(rozofs_storcli_projection_ctx_t *prj_ctx_p, 
                                             uint8_t  layout, uint32_t bsize,
                                             uint32_t number_of_blocks_returned,
                                             uint32_t number_of_blocks_requested,
                                             uint64_t raw_file_size);

/*
**__________________________________________________________________________
*/
/**
    API to get the exact read size from an application standpoint.
     That function go through the effective length of each blocks and
     cumulates that length on the number of effective read blocks provided in the input arguments.
     
     if EOF is encountered, the eof flag is asserted
         
    @param prj_ctx_p : pointer to the projection context
    @param eof_p : pointer to the array where end of file state is reported (asserted to 1 if EOF is encountered
    
    @retval len of the data read
*/     
int  rozofs_storcli_transform_get_read_len_in_bytes(rozofs_storcli_inverse_block_t *inverse_res_p, 
                                                    uint32_t number_of_blocks_read,uint8_t *eof_p);
 
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
				       uint32_t *corrupted_blocks);         
                                       
/*
**__________________________________________________________________________
*/
/** 
 * Check out whether a block needs to be repaired in the read projections
 * At the time this function is called, the data have been transformed back 
 * succesfully
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
static inline void rozofs_storcli_check_block_2_repair(rozofs_storcli_projection_ctx_t * prj_ctx_p,  
                                         uint8_t                           rozofs_inverse, 
                                         uint8_t                           rozofs_forward,
                                         uint8_t                           rozofs_safe,
                                         int                               prj_size_in_msg,
                                         uint32_t                          number_of_blocks, 
                                         rozofs_storcli_inverse_block_t  * block_ctx_p)
 {
    int                      block_idx;
    int                      prj_ctx_idx;
    int                      received        = 0;
    rozofs_stor_bins_hdr_t * hdr;      
    int                      prj2repair = -1;
    int                      blk2repair = -1;
        
    /*
    ** One must have read at least 3 blocks to decide to rebuild a block of a projection
    */
    if (number_of_blocks < 5) return;

    /*
    ** When no more than inverse projections has been read, every block is perfect
    */ 
    for (prj_ctx_idx = 0; prj_ctx_idx <rozofs_safe; prj_ctx_idx++) {
        if (prj_ctx_p[prj_ctx_idx].prj_state == ROZOFS_PRJ_READ_DONE) received++;
    }
    if (received <= rozofs_inverse) return;
    

    /*
    ** Proceed examination block per block
    */
    for (block_idx = 0; block_idx < number_of_blocks; block_idx++) {

        /*
        ** We are looking for a non empty blocks to be repaired
        */
        if (block_ctx_p[block_idx].timestamp == 0) continue;
             
        /*
        ** Loop on the received projections
        */     
        for (prj_ctx_idx = 0; prj_ctx_idx <rozofs_safe; prj_ctx_idx++) {
            
            /*
            ** Only the received ones
            */
            if (prj_ctx_p[prj_ctx_idx].prj_state != ROZOFS_PRJ_READ_DONE) continue;
            
            /*
            ** Not those from the spare SIDs
            */
            if (prj_ctx_p[prj_ctx_idx].stor_idx >= rozofs_forward) continue;

            /*
            ** Point to the received header
            */
            hdr = (rozofs_stor_bins_hdr_t*)(prj_ctx_p[prj_ctx_idx].bins + (prj_size_in_msg/sizeof(bin_t)) * block_idx);
            
            /*
            ** This block comes from a regular SID 
            */
            if (hdr->s.timestamp == 0) {
               /*
               ** Let's declare a fault to repair on this projection and block
               */
               if (blk2repair == -1) {
                  blk2repair = block_idx;
                  prj2repair = prj_ctx_idx;
               }
               else {
                 /*
                 ** To much errors. Do not try to repair anything
                 */
                 return;
               }  
            }   
    	}	
    }
    
    /*
    ** Only one block in one projection to repair
    */
    if (blk2repair != -1) {
      ROZOFS_BITMAP64_SET(blk2repair,prj_ctx_p[prj2repair].crc_err_bitmap);
    }      
}         
                                                      
#endif
