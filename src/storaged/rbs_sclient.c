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
#include <stdio.h>
#include <errno.h>
#include <malloc.h>
 
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/list.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/rpc/rpcclt.h>
#include "rozofs/rpc/mproto.h"
#include <rozofs/rpc/sclient.h>
#include <rozofs/rpc/mclient.h>

#include <src/storaged/storage.h>

#include "rbs.h"
#include "rbs_sclient.h"

uint64_t totalReadSize = 0;


/** Get one storage connection for a given SID and given random value
 *
 * @param *storages: list of storages
 * @param sid: storage ID
 * @param rand_value: random value
 *
 * @return: storage connection on success, NULL otherwise
 */
static sclient_t *rbs_get_stor_cnt(list_t * storages, sid_t sid,
        int rand_value) {

    list_t *iterator;

    list_for_each_forward(iterator, storages) {

        rb_stor_t *entry = list_entry(iterator, rb_stor_t, list);

        if (sid == entry->sid) {
            // The good storage node is find
            // modulo between all connections for this node
            if (entry->sclients_nb > 0) {
                rand_value %= entry->sclients_nb;
                return &entry->sclients[rand_value];
            } else {
                // We find a storage node for this sid but
                // it don't have connection.
                // It's only possible when all connections for one storage
                // node are down when we mount the filesystem or we don't
                // have get ports for this storage when we mount the
                // the filesystem
                //severe("No connection found for storage (sid: %u)", sid);
                return NULL;
            }
        }

    }

    return NULL;
}

int rbs_get_rb_entry_cnts(rb_entry_t * rb_entry,
        list_t * clusters_list,
        cid_t cid_to_search,
        sid_t sid_to_rebuild,
        uint8_t nb_cnt_required) {
    int i = 0;
    int connected = 0;
    long int rand_value = 0;
    uint8_t rozofs_safe = 0;
    list_t * storages = NULL;
    list_t *r = NULL;
    int    local_index=-1;

    rozofs_safe = rozofs_get_rozofs_safe(rb_entry->layout);

    // Get a pseudo-random integer in the range 0 to RAND_MAX inclusive
    rand_value = rand();

    // Get the good storages list for this cid

    list_for_each_forward(r, clusters_list) {
        rb_cluster_t *clu = list_entry(r, rb_cluster_t, list);
        if (clu->cid == cid_to_search) {
            storages = &clu->storages;
	    break;
	}    
    }

    if (!storages) {
        errno = EINVAL;
        return -1;
    }

    // For each storage associated with this file
    for (i = 0; i < rozofs_safe; i++) {

        // Get connection for this storage
        if ((rb_entry->storages[i] = rbs_get_stor_cnt(storages,
                rb_entry->dist_set_current[i],
                rand_value)) != NULL) {

            // Check connection status
	    
            if (rb_entry->dist_set_current[i] == sid_to_rebuild) {
	      if ((rb_entry->storages[i]->status != 1) 
	      ||  (rb_entry->storages[i]->rpcclt.client == 0)) {
	        severe("Can not connect to local storage sid %d",
		       sid_to_rebuild);
	        connected = 0;
		break;
	      }
	      local_index = i;
	      continue;
            }

            if (rb_entry->storages[i]->status == 1 &&
                    rb_entry->storages[i]->rpcclt.client != 0)
                connected++; // This storage seems to be OK
        }
    }

    if (connected < nb_cnt_required) {
        errno = EPROTO;
        return -1;
    }

    return local_index;
}
int rbs_get_rb_entry_list(mclient_t * mclt, cid_t cid, sid_t sid,
        sid_t rebuild_sid, uint8_t * device, uint8_t * spare, uint16_t * slice, uint64_t * cookie,
        bins_file_rebuild_t ** children, uint8_t * eof) {

    int status = -1;
    mp_list_bins_files_arg_t arg;
    mp_list_bins_files_ret_t *ret = 0;
    mp_children_t it1;
    bins_file_rebuild_t **it2;

    DEBUG_FUNCTION;

    // Args of request
    arg.cid = cid;
    arg.sid = sid;
    arg.rebuild_sid = rebuild_sid;
    arg.cookie = *cookie;
    arg.spare = *spare;
    arg.device = *device;
    arg.slice = *slice;

    // Send request to storage server
    ret = mp_list_bins_files_1(&arg, mclt->rpcclt.client);
    if (ret == 0) {
        errno = EPROTO;
        goto out;
    }
    if (ret->status == MP_FAILURE) {
        errno = ret->mp_list_bins_files_ret_t_u.error;
        goto out;
    }

    // Copy list of rebuild entries
    it2 = children;
    it1 = ret->mp_list_bins_files_ret_t_u.reply.children;
    while (it1 != NULL) {
        *it2 = xmalloc(sizeof (bins_file_rebuild_t));
        memcpy((*it2)->fid, it1->fid, sizeof (fid_t));
        memcpy((*it2)->dist_set_current, it1->dist_set, sizeof (sid_t) * ROZOFS_SAFE_MAX);
        (*it2)->layout = it1->layout;
	(*it2)->bsize = it1->bsize;
        it2 = &(*it2)->next;
        it1 = it1->next;
    }
    *it2 = NULL;

    // Update index that represents where you are in the list
    *eof = ret->mp_list_bins_files_ret_t_u.reply.eof;
    *cookie = ret->mp_list_bins_files_ret_t_u.reply.cookie;
    *slice = ret->mp_list_bins_files_ret_t_u.reply.slice;
    *spare = ret->mp_list_bins_files_ret_t_u.reply.spare;
    *device = ret->mp_list_bins_files_ret_t_u.reply.device;

    status = 0;
out:
    if (ret)
        xdr_free((xdrproc_t) xdr_mp_list_bins_files_ret_t, (char *) ret);
    return status;
}

int rbs_read_proj(sclient_t *storage, cid_t cid, sid_t sid, uint8_t stor_idx,
        uint8_t layout, uint32_t bsize, sid_t dist_set[ROZOFS_SAFE_MAX], fid_t fid,
        bid_t first_block_idx, uint32_t nb_blocks_2_read,
        uint32_t * nb_blocks_read, rbs_projection_ctx_t * proj_ctx_p,
	uint64_t * size_read) {

    int status = -1;
    int ret = 0;
    uint8_t spare = 0;
    uint64_t size;
    uint16_t rozofs_max_psize_in_msg = rozofs_get_max_psize_in_msg(layout,bsize);
    bin_t * bins = NULL;
    DEBUG_FUNCTION;
    
    proj_ctx_p->nbBlocks = 0;

    // Check connection
    if (!storage || !storage->rpcclt.client || storage->status != 1) {
        proj_ctx_p->prj_state = PRJ_READ_ERROR;
        goto out;
    }

    /*
    ** Allocate memory for the response when none is given
    */	
    if (proj_ctx_p->bins == NULL) {        
      size = rozofs_max_psize_in_msg * nb_blocks_2_read;
      bins = memalign(32,size);
    }
    /*
    ** Else use given memory buffer
    */
    else {
      bins = proj_ctx_p->bins;
    }

    // Is it a spare storage ?
    if (stor_idx >= rozofs_get_rozofs_forward(layout)) {
        spare = 1;
    }

    // Read request
    ret = sclient_read_rbs(storage, cid, sid, layout, bsize, spare, dist_set, fid,
            first_block_idx, nb_blocks_2_read, nb_blocks_read, bins);
    // Error
    if (ret != 0) {
        proj_ctx_p->prj_state = PRJ_READ_ERROR;
        /*
        ** Memory was allocated localy. Free it
        */
        if (proj_ctx_p->bins == NULL) { 
          free(bins);
          bins = NULL;
        }  
        goto out;
    }

    // Set the useful pointer on the received message
    proj_ctx_p->bins = bins;
    proj_ctx_p->prj_state = PRJ_READ_DONE;
    proj_ctx_p->nbBlocks = *nb_blocks_read;
    
    *size_read += (proj_ctx_p->nbBlocks * rozofs_max_psize_in_msg);

    totalReadSize += (proj_ctx_p->nbBlocks * rozofs_max_psize_in_msg);
    status = 0;
out:
    return status;
}

// Internal structure

typedef struct rbs_blocks_recv_ctx {
    uint32_t nb_blocks_recv; /**< key */
    uint8_t count; /**< number of response with the same nb. of blocks */
} rbs_blocks_recv_ctx_t;

rbs_blocks_recv_ctx_t rbs_blocks_recv_tb[ROZOFS_SAFE_MAX];

static int rbs_read_proj_set(sclient_t **storages, int local_idx, uint8_t layout, uint32_t bsize, cid_t cid,
        sid_t dist_set[ROZOFS_SAFE_MAX], fid_t fid, bid_t first_block_idx,
        uint32_t nb_blocks_2_read, uint32_t * nb_blocks_read, int retry_nb,
        rbs_storcli_ctx_t * working_ctx_p,
	uint64_t          * size_read) {
    int status = -1;
    int i = 0;
    uint8_t nb_diff_nb_blocks_recv = 0;
    uint8_t stor_idx = 0;
    int retry = 0;
    int file_exist=0;

    DEBUG_FUNCTION;

    // Get the parameters relative to this layout
    uint8_t rozofs_inverse = rozofs_get_rozofs_inverse(layout);
    uint8_t rozofs_safe = rozofs_get_rozofs_safe(layout);

    *nb_blocks_read = 0;
    memset(&rbs_blocks_recv_tb, 0, ROZOFS_SAFE_MAX *
            sizeof (rbs_blocks_recv_ctx_t));

    do {
        // For each existent storage associated with this entry
        for (stor_idx = 0; stor_idx < rozofs_safe; stor_idx++) {

            uint32_t curr_nb_blocks_read = 0;
	    
	    /* Do not read local block */
            if (stor_idx == local_idx) {
              working_ctx_p->prj_ctx[stor_idx].prj_state = PRJ_READ_ERROR;	    
	      continue;	    
            }
	    
            // Check if a spare storage
            if (stor_idx >= rozofs_inverse) {
                working_ctx_p->redundancy_stor_idx_current = stor_idx;
                working_ctx_p->redundancy_stor_idx_current++;
            }

            // Check if the projection is already read
            if (working_ctx_p->prj_ctx[stor_idx].prj_state == PRJ_READ_DONE)
                continue;

            // Send one read request
            if (rbs_read_proj(storages[stor_idx], cid, dist_set[stor_idx],
                    stor_idx, layout, bsize, dist_set, fid, first_block_idx,
                    nb_blocks_2_read, &curr_nb_blocks_read,
                    &working_ctx_p->prj_ctx[stor_idx], size_read) != 0) {
		if (errno == ENOENT) {
		  /*
		  ** In case the file does not exist set the size to -1
		  */
		  curr_nb_blocks_read = -1;
		}
		else {
                  continue; // Problem; try with the next storage;
		}  
            }
	    else {	
	      file_exist = 1; 
            }
	    
            // If it's the first request received
            if (nb_diff_nb_blocks_recv == 0) {
                // Save the nb. of blocks received
                rbs_blocks_recv_tb[nb_diff_nb_blocks_recv].nb_blocks_recv =
                        curr_nb_blocks_read;
                rbs_blocks_recv_tb[nb_diff_nb_blocks_recv].count++;
                nb_diff_nb_blocks_recv++;

                // Send others requests
                continue;

            } else { // It's not the first response received

                uint8_t another_nb_of_blocks = 1;

                // Search if we have another response with the same
                // nb. of blocks read
                for (i = 0; i < nb_diff_nb_blocks_recv; i++) {

                    if (curr_nb_blocks_read == rbs_blocks_recv_tb[i].nb_blocks_recv) {
                        // OK, we have already received a another response 
                        // with the same nb. of blocks returned
                        // So increment the counter
                        rbs_blocks_recv_tb[i].count++;
                        another_nb_of_blocks = 0;

                        // Check if we have enough responses to rebuild block
                        if (rbs_blocks_recv_tb[i].count >= rozofs_inverse) {
                            *nb_blocks_read = rbs_blocks_recv_tb[i].nb_blocks_recv;
			    /*
			    ** Case of deleted file
			    */
			    if ((*nb_blocks_read == -1)&&(file_exist)) {
			      /* Some storage know the file and some not !!! */
			      status = -1; 
			      goto out;
			    }	            
                            status = 0; // OK return
                            goto out;
                        }
                        // No enough responses to rebuild
                        // send another read request    
                        break;
                    }
                }
                // We received a response with a nb. of blocks different
                if (another_nb_of_blocks) {
                    // Save th nb. of blocks received
                    rbs_blocks_recv_tb[nb_diff_nb_blocks_recv].nb_blocks_recv
                            = curr_nb_blocks_read;
                    rbs_blocks_recv_tb[nb_diff_nb_blocks_recv].count++;
                    nb_diff_nb_blocks_recv++;
                }
            }
        }
    } while (retry++ < retry_nb);

    // Here, error we don't have enough responses

out:
    return status;
}
int rbs_read_all_available_proj(sclient_t **storages, int spare_idx, uint8_t layout, uint32_t bsize, cid_t cid,
        sid_t dist_set[ROZOFS_SAFE_MAX], fid_t fid, bid_t first_block_idx,
        uint32_t nb_blocks_2_read, uint32_t * nb_blocks_read, 
        rbs_storcli_ctx_t * working_ctx_p,
	uint64_t * size_read) {
    int status = -1;
    int i = 0;
    uint8_t nb_diff_nb_blocks_recv = 0;
    uint8_t stor_idx = 0;
    int     success = 0;

    DEBUG_FUNCTION;

    // Get the parameters relative to this layout
    uint8_t rozofs_inverse = rozofs_get_rozofs_inverse(layout);
    uint8_t rozofs_safe = rozofs_get_rozofs_safe(layout);

    *nb_blocks_read = 0;
    memset(&rbs_blocks_recv_tb, 0, ROZOFS_SAFE_MAX *
            sizeof (rbs_blocks_recv_ctx_t));

    // For each existent storage associated with this entry
    for (stor_idx = 0; stor_idx < rozofs_safe; stor_idx++) {

        if (stor_idx == spare_idx) {
          working_ctx_p->prj_ctx[spare_idx].prj_state = PRJ_READ_ERROR;	
	  continue;
        }
		
        uint32_t curr_nb_blocks_read = 0;

        // Check if the projection is already read
        if (working_ctx_p->prj_ctx[stor_idx].prj_state == PRJ_READ_DONE) {
            success++;	
            continue;
        }
	
        // Send one read request
        if (rbs_read_proj(storages[stor_idx], cid, dist_set[stor_idx],
                stor_idx, layout, bsize, dist_set, fid, first_block_idx,
                nb_blocks_2_read, &curr_nb_blocks_read,
                &working_ctx_p->prj_ctx[stor_idx],
		size_read) != 0) {
	  if (errno == ENOENT) {
	    curr_nb_blocks_read = -1;
	  }
	  else {	
            continue; // Problem; try with the next storage;
	  }  
        }
	
	success++;
	
	

        // If it's the first request received
        if (nb_diff_nb_blocks_recv == 0) {
            // Save the nb. of blocks received
            rbs_blocks_recv_tb[nb_diff_nb_blocks_recv].nb_blocks_recv =
                    curr_nb_blocks_read;
            rbs_blocks_recv_tb[nb_diff_nb_blocks_recv].count++;
            nb_diff_nb_blocks_recv++;

            // Send others requests
            continue;

        } 
	
	// It's not the first response received
        // Search if we have another response with the same
        // nb. of blocks read
        for (i = 0; i < nb_diff_nb_blocks_recv; i++) {

            if (curr_nb_blocks_read ==
                    rbs_blocks_recv_tb[i].nb_blocks_recv) {
                // OK, we have already received a another response 
                // with the same nb. of blocks returned
                // So increment the counter
                rbs_blocks_recv_tb[i].count++;
		break;
            }
        }
        // We received a response with a nb. of blocks different
        if (i == nb_diff_nb_blocks_recv) {
            // Save th nb. of blocks received
            rbs_blocks_recv_tb[nb_diff_nb_blocks_recv].nb_blocks_recv
                    = curr_nb_blocks_read;
            rbs_blocks_recv_tb[nb_diff_nb_blocks_recv].count++;
            nb_diff_nb_blocks_recv++;
        }
        
    }
    
    if (success < rozofs_inverse) {
      /* Not enough projection read to rebuild anything */
      goto out;
    } 
    
    for (i = 0; i < nb_diff_nb_blocks_recv; i++) {
      if (rbs_blocks_recv_tb[i].count >= rozofs_inverse) {
        if (rbs_blocks_recv_tb[i].nb_blocks_recv == -1) {
	  /*
	  ** File has been deleted 
	  */
	  *nb_blocks_read = -1;
	  status = 0;
	}
        else if (rbs_blocks_recv_tb[i].nb_blocks_recv >= *nb_blocks_read) {
	  *nb_blocks_read = rbs_blocks_recv_tb[i].nb_blocks_recv;
	  status = 0;
	}  
      }
    }

out:
    return status;
}
/*_____________________________________________________________________________
 * Read a set of projections from the available storages in order to reconstitute
 * the initial data of a given number of block.
 *
 * @param storages         The storages information in the cluster of the targeted file
 * @param local_idx        The index of the storage to rebuild within the storages array
 * @param layout           The file layout
 * @param bsize            The file block size
 * @param cid              The file cluster identifier
 * @param dist_set         The file SID distribution set
 * @param fid              The file FID
 * @param first_block_idx  The starting block index to read in the file
 * @param nb_blocks_2_read The number of blocks to read
 * @param nb_blocks_read   The number of blocks actually read
 * @param retry_nb         The number of retry attempt allowed
 * @param working_ctx_p    The file rebuild working context
 * @param size_read        On return gives the total ize of the read data
 * @param empty            On return tells whether all read blocks are empty
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int rbs_read_blocks(sclient_t **storages, int local_idx, uint8_t layout, uint32_t bsize, cid_t cid,
        sid_t dist_set[ROZOFS_SAFE_MAX], fid_t fid, bid_t first_block_idx,
        uint32_t nb_blocks_2_read, uint32_t * nb_blocks_read, int retry_nb,
        rbs_storcli_ctx_t * working_ctx_p, 
	uint64_t          * size_read,
        int               * empty) {

    int status = -1;
    int ret = -1;
    
    *empty = 0;
    
    // Nb. of blocks read on storages
    uint32_t real_nb_blocks_read = 0;

    // Get rozofs layout parameters
    uint8_t rozofs_safe = rozofs_get_rozofs_safe(layout);
    uint8_t rozofs_inverse = rozofs_get_rozofs_inverse(layout);

    // Read projections on storages
    ret = rbs_read_proj_set(storages, local_idx, layout, bsize, cid, dist_set, fid,
            first_block_idx, nb_blocks_2_read, &real_nb_blocks_read,
            retry_nb, working_ctx_p,size_read);

    // Error
    if (ret != 0) {
        // There is no enough projection(s) to rebuild blocks
        errno = EIO;
        goto out;
    }


    // File has been deleted
    if (real_nb_blocks_read == -1) {
      *nb_blocks_read = real_nb_blocks_read;
      status = 0;
      goto out;
    }

    // Check the numbers of blocks read
    if (real_nb_blocks_read == 0) {
        // End of file
        // No need to perform inverse transform
        *nb_blocks_read = real_nb_blocks_read;
        status = 0;
        goto out;
    }

    // Memory allocation for store reconstructed blocks
    if (working_ctx_p->data_read_p == NULL) {
      working_ctx_p->data_read_p = memalign(32,real_nb_blocks_read * ROZOFS_BSIZE_BYTES(bsize));
    }
    
transform_inverse:

    // Check timestamp and perform transform inverse
    ret = rbs_transform_inverse(working_ctx_p->prj_ctx, layout, bsize, 0,
            real_nb_blocks_read, working_ctx_p->block_ctx_table,
            working_ctx_p->data_read_p, empty);
    if (ret < 0) {
        // There is no enough projection to rebuild the initial message

        // While we still have storage on which we can read some more projection
        // Try to get one projection more and do a transform inverse
        while (working_ctx_p->redundancy_stor_idx_current < rozofs_safe) {

            // We can take a new entry for a projection on a another storage
            if (working_ctx_p->redundancy_stor_idx_current == 0)
                working_ctx_p->redundancy_stor_idx_current = rozofs_inverse;

            uint8_t stor_idx = working_ctx_p->redundancy_stor_idx_current;
            uint32_t nb_blocks_read = 0;

            // Increment the nb. of redundancy_stor_idx_current
            working_ctx_p->redundancy_stor_idx_current++;
	    
	    if (stor_idx == local_idx) continue;

            // Send one another read request
            if (rbs_read_proj(storages[stor_idx], cid, dist_set[stor_idx],
                    stor_idx, layout, bsize, dist_set, fid, first_block_idx,
                    nb_blocks_2_read, &nb_blocks_read,
                    &working_ctx_p->prj_ctx[stor_idx],
		    size_read) != 0) {
                continue; // Problem: try with the next storage;
            }

            // Receive a response but the nb_blocks_read == 0
            // And we know the we must to get real_nb_blocks_read blocks
            // so this response is not good
            if (nb_blocks_read == 0 && nb_blocks_read != real_nb_blocks_read)
                continue;

            // OK, we have received at least one valid projection
            // Try to do the transform inverse
            goto transform_inverse;
        }

        // There are no enough valid storages to be able
        // to rebuild the initial message
        errno = EIO;
        goto out;
    }

    // Now the inverse transform is finished, release the allocated
    // ressources used for rebuild
    *nb_blocks_read = real_nb_blocks_read;

    status = 0;
out:
    return status;
}
