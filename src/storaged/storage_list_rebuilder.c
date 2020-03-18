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
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <pthread.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <sys/resource.h>
#include <unistd.h>
#include <libintl.h>
#include <sys/poll.h>
#include <rpc/rpc.h>
#include <rpc/pmap_clnt.h>
#include <libconfig.h>
#include <getopt.h>
#include <inttypes.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/vfs.h>
#include <malloc.h>


#include <rozofs/rozofs_srv.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include <rozofs/common/common_config.h>
#include <rozofs/rpc/mproto.h>
#include <rozofs/rpc/sproto.h>
#include <rozofs/rpc/spproto.h>
#include <rozofs/core/rozofs_core_files.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/core/rozofs_fid_string.h>
#include <rozofs/core/rozofs_string.h>

#include "config.h"
#include "storage.h"
#include "rbs.h"
#include "rbs_eclient.h"
#include "rbs_sclient.h"

/*
** Some input parameters
*/
static int rebuildRef = -1;     /* Rebuild process reference */
static int instance   = -1;     /* List rebuilder instance within the rebuild process */
static int throughput = 0;      /* Rebuild throughput limitation in MB/s */

/*
** For enforcing throughput limitation
*/
extern uint64_t totalReadSize;
static uint64_t startTime     = 0;


char        rebuild_directory_path[FILENAME_MAX]; 
char        fid_list[FILENAME_MAX]; 
char        statFilename[FILENAME_MAX]; 
 
uint8_t prj_id_present[ROZOFS_SAFE_MAX];
int         quiet=0;


char                command[1];
char              * fid2rebuild_string=NULL;
uint8_t storio_nb_threads = 0;
uint8_t storaged_nb_ports = 0;
uint8_t storaged_nb_io_processes = 0;
/* nb. of retries for get bins on storages */
static uint32_t retries = 10;

// Rebuild storage variables

int sigusr_received=0;
rpcclt_t   rpcclt_export;
  
rbs_file_type_e ftype = rbs_file_type_all;

/*
** Just because storage.h needs it
*/
storage_t storaged_storages[0];
uint16_t  storaged_nrstorages=0;
int nolog = 0;

int relocate = 0;
int resecure = 0;
list_t     cluster_entries;
char     * pExport_hostname = NULL;
char     * export_param = NULL;
char       throughputFile[FILENAME_MAX] = {0};
time_t     throughputLastModificationTime = 0;

/*-----------------------------------------------------------------------------
**
**  SIGUSR1 receiving handler
**
**----------------------------------------------------------------------------
*/
void rbs_cath_sigusr(int sig){
  sigusr_received = 1;
  signal(SIGUSR1, rbs_cath_sigusr);  
}


typedef struct rbs_error_t {
  uint64_t spare_start;
  uint64_t spare_start_again;
  uint64_t spare_read;
  uint64_t spare_read_enoent;
  uint64_t spare_read_no_enough;
  uint64_t spare_write_empty;
  uint64_t spare_write_proj;
  uint64_t spare_write_broken;  

  uint64_t nom_start;
  uint64_t nom_start_again;
  uint64_t nom_read;
  uint64_t nom_read_enoent;
  uint64_t nom_transform;
  uint64_t nom_write;  
  uint64_t nom_write_broken;  

} RBS_ERROR_T;

static RBS_ERROR_T rbs_error = {0};

#define RBS_DISPLAY_ERROR(x) {if (rbs_error.x) REBUILD_MSG("%20s = %llu", #x, (long long unsigned int) rbs_error.x);}

void display_rbs_errors() {
  RBS_DISPLAY_ERROR(spare_start);
  RBS_DISPLAY_ERROR(spare_start_again);
  RBS_DISPLAY_ERROR(spare_read);
  RBS_DISPLAY_ERROR(spare_read_enoent);
  RBS_DISPLAY_ERROR(spare_read_no_enough);
  RBS_DISPLAY_ERROR(spare_write_empty);
  RBS_DISPLAY_ERROR(spare_write_proj);
  RBS_DISPLAY_ERROR(spare_write_broken);
  RBS_DISPLAY_ERROR(nom_start);
  RBS_DISPLAY_ERROR(nom_start_again);
  RBS_DISPLAY_ERROR(nom_read);
  RBS_DISPLAY_ERROR(nom_read_enoent);
  RBS_DISPLAY_ERROR(nom_transform);
  RBS_DISPLAY_ERROR(nom_write); 
  RBS_DISPLAY_ERROR(nom_write_broken); 
}

typedef enum _rbs_exe_code_e {
  RBS_EXE_SUCCESS,
  RBS_EXE_FAILED,
  RBS_EXE_ENOENT,
  RBS_EXE_BROKEN
} RBS_EXE_CODE_E;
/*
**____________________________________________________
**
** Get time in micro seconds
**
** @param from  when set compute the delta in micro seconds from this time to now
**              when zero just return current time in usec
**----------------------------------------------------------------------------
*/
static inline uint64_t get_us(uint64_t from) {
  struct timeval     timeDay;
  uint64_t           us;
  
  /*
  ** Get current time in us
  */
  gettimeofday(&timeDay,(struct timezone *)0); 
  us = ((unsigned long long)timeDay.tv_sec * 1000000 + timeDay.tv_usec);

  /*
  ** When from is given compute the delta
  */
  if (from) {
    us -= from;
  }  
  
  return us;	
}
/*-----------------------------------------------------------------------------
**
** Check for new throughput value 
**
**----------------------------------------------------------------------------
*/
static inline void check_for_new_throughput() {
  struct stat buf;
  char   throughputString[64];
  int    fd;
  int    throughputValue;
  int    ret;
  
  /*
  ** Initializae the thoughput file name if not yet done
  */
  if (throughputFile[0] == 0) {
    char * pDirectory = get_rebuild_directory_name(rebuildRef); 
    sprintf(throughputFile, "%s/throughput",pDirectory);
  }
  
  /*
  ** Open the file if it exists
  */
  fd = open(throughputFile,O_RDONLY);
  if (fd < 0) return;

  /*
  ** Get its last modification time
  */
  if (fstat(fd, &buf) != 0) {
    close(fd);
    return;
  }
  
  /*
  ** No modification
  */
  if (buf.st_mtime == throughputLastModificationTime) {
    close(fd);
    return;
  }
  
  ret = pread(fd,&throughputString,sizeof(throughputString),0);  
  close(fd);
  if (ret <= 0) return;
  
  if (sscanf(throughputString,"%d",&throughputValue)!=1) return;
  if (throughputValue<0) return;
  
  throughput = throughputValue;
  throughputLastModificationTime = buf.st_mtime;
    
  /*
  ** Reset read size as well as start time to enforce the new throughput from now
  */ 
  totalReadSize = 0;
  startTime     = get_us(0); 
  
}
/*-----------------------------------------------------------------------------
**
** Enforce the requested throughput
**
**----------------------------------------------------------------------------
*/
static inline void enforce_throughput() {
  uint64_t   delay;
  uint64_t   enforcement_delay;

  check_for_new_throughput();
  
  /*
  ** No throughput limitation
  */
  if (throughput == 0) return;

  /*
  ** Nothing yet read
  */
  if (totalReadSize == 0) return;
  
  /*
  ** Compute delay from the start time
  */
  delay = get_us(startTime);
  
  /*
  ** Compute the time credit for the size read 
  */
  enforcement_delay = totalReadSize / throughput;
  
  /*
  ** Check we are not ready too fast.
  ** If we are less than 10ms too fast just go ahead...
  */
  if (enforcement_delay >= (delay + 50000L)) {
    usleep(enforcement_delay-delay);
  }
}
/*-----------------------------------------------------------------------------
**
** Data flush macro
**
** Used to flush either a sequence of empty blocks at the time a non empty block 
** is detected, or a sequence of non empty blocks at the time an empty block is
** detected
**
** @param firstBlock  First block to flush from *block_start
** @param pBins       Address of the data to flush (NULL for empty blocks)
** @param spare       0 for nominal files, 1 for spare files 
**
**----------------------------------------------------------------------------
*/
#define rbs_restore_flush(firstBlock,pBins,spare) {                 \
   /* Check a sequence has actualy been detected */                 \
   if (firstBlock != -1) {                                          \
     /* There is some data to flush  */                             \
     if (pBins == NULL) {                                           \
       /* Empty blocks are to be flushed */                         \
       ret = sclient_write_empty_rbs(re->storages[local_idx],       \
                                     cid, sid,              \
	                             re->layout, re->bsize, spare,  \
			             re->dist_set_current, re->fid, \
			             firstBlock+*block_start,       \
                                     nbBlocks,                      \
			             rebuild_ref);                  \
     }                                                              \
     else {                                                         \
       /* Data blocks are to be flushed  */                         \
       ret = sclient_write_rbs(re->storages[local_idx],             \
                               cid, sid,                    \
	                       re->layout, re->bsize, spare,        \
			       re->dist_set_current, re->fid,       \
			       firstBlock+*block_start,             \
                               nbBlocks,                            \
			       (const bin_t *)pBins,                \
			       rebuild_ref);                        \
     }                                                              \
                                                                    \
     /* Handle error cases */                                       \
     if (ret < 0) {                                                 \
                                                                    \
       /* rebuild has been broken by a normal concurrent write */   \
       if (errno == EAGAIN) {                                       \
         rbs_error.spare_write_broken++;                            \
         *error = rozofs_rbs_error_rebuild_broken;                  \
         status = RBS_EXE_BROKEN;                                   \
         goto out;                                                  \
       }                                                            \
                                                                    \
       /* Other unexpected error */                                 \
       if (pBins == NULL) {                                         \
         rbs_error.spare_write_empty++;                             \
       }                                                            \
       else {                                                       \
         rbs_error.spare_write_proj++;                              \
       }                                                            \
       *error = rozofs_rbs_error_write_failed;	                    \
       status = RBS_EXE_FAILED;                                     \
       goto out;                                                    \
     }                                                              \
                                                                    \
     /* Update written size */                                      \
     *size_written += (nbBlocks*disk_block_size);                   \
     firstBlock  = -1;                                              \
     nbBlocks    = 0;                                               \
   }                                                                \
}

/*-----------------------------------------------------------------------------
**
** Spare file rebuilding
**
** @param cid          Cluster to rebuild
** @param sid          target SID to rebuild
** @param layout       Layout of the file
** @param local_idx    Index of the storio within the storage array in re structure
** @param block_start  First block to rebuild. 
** @param block_end    Last block to rebuild. 
** @param re           Descriptor of the file to rebuild
** @param spare_idx    Index of the storio to rebuild within the file SID distribution
**                     Actualy should be equal to local_idx !
** @param size_written To return the written bytes count                
** @param size_read    To return the read bytes count
** @param error        Returned error core
**
**----------------------------------------------------------------------------
*/                               
RBS_EXE_CODE_E rbs_restore_one_spare_entry(cid_t             cid, 
                                           sid_t             sid,
                                           uint8_t           layout,
                                	   int               local_idx, 
			        	   uint32_t        * block_start,
			        	   uint32_t          block_end, 
			     		   rb_entry_t      * re, 
					   uint8_t           spare_idx,
					   uint64_t        * size_written,
					   uint64_t        * size_read,
					   uint8_t         * error,
                                           int             * more_prj2rebuild) {
    RBS_EXE_CODE_E status = RBS_EXE_FAILED;
    int i = 0;
    int ret = -1;
    rbs_storcli_ctx_t working_ctx;
    int block_idx = 0;
    uint8_t rbs_prj_idx_table[ROZOFS_SAFE_MAX];
    int     count;
    rbs_inverse_block_t * pBlock;
    int prj_count;
    uint16_t prj_ctx_idx;
    uint16_t projection_id;
    char   *  pforward = NULL;
    rozofs_stor_bins_hdr_t * rozofs_bins_hdr_p;
    rozofs_stor_bins_footer_t * rozofs_bins_foot_p;
    int    remove_file=0;
    uint32_t   rebuild_ref=0;
    uint32_t   firstEmptyBlock = -1;
    uint32_t   firstDataBlock  = -1;
    uint32_t   nbBlocks        =  0;
    
    *more_prj2rebuild = 0;
            
    // Get rozofs layout parameters
    uint32_t bsize             = re->bsize;
    uint32_t bbytes            = ROZOFS_BSIZE_BYTES(bsize);
    uint8_t  rozofs_safe       = rozofs_get_rozofs_safe(layout);
    uint8_t  rozofs_forward    = rozofs_get_rozofs_forward(layout);
    uint8_t  rozofs_inverse    = rozofs_get_rozofs_inverse(layout);
    uint16_t disk_block_size   = rozofs_get_max_psize_in_msg(layout,bsize);
    uint32_t requested_blocks  = ROZOFS_MAX_BLOCK_PER_MSG;
    uint32_t nb_blocks_read_distant = requested_blocks;
    uint32_t block_per_chunk = ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(re->bsize);
    uint32_t chunk_stop;
    uint32_t chunk;
    uint16_t rozofs_msg_psize  = rozofs_get_max_psize_in_msg(layout,bsize);

    // Clear the working context
    memset(&working_ctx, 0, sizeof (working_ctx));

    /*
    ** Compute starting and stopping chunks
    */    
    chunk = *block_start / block_per_chunk;
    if (block_end==0xFFFFFFFF) chunk_stop = 0xFFFFFFFF; // whole file
    else                       chunk_stop = chunk;      // one chunk
          
    /*
    ** Loop on chunks
    */      
    while (chunk<=chunk_stop) {

      *block_start = chunk * block_per_chunk;
      block_end    = *block_start + block_per_chunk - 1;
      
      remove_file = 1; /* Should possibly remove the chunk at the end */

      /*
      ** Ask for a rebuild
      */
      rebuild_ref = sclient_rebuild_start_rbs(re->storages[local_idx], cid, sid, re->fid,
                                              relocate?SP_NEW_DEVICE:SP_SAME_DEVICE, chunk, 1 , 
					      *block_start, block_end);
      /*
      ** Rebuild is refused
      */                                        
      if (rebuild_ref == 0) {
	remove_file = 0; // keep the file 
	if (errno == EAGAIN) {
	  rbs_error.spare_start_again++;
	  *error = rozofs_rbs_error_file_to_much_running_rebuild;
	}  
	else {
	  rbs_error.spare_start++;
	  *error = rozofs_rbs_error_rebuild_start_failed;
        }	  
	goto out;
      }     

      // While we can read in the bins file
      while(*block_start <= block_end) {

          /*
          ** Allocate memory for projections
          */
          for (i = 0; i < rozofs_safe; i++) {
            if (working_ctx.prj_ctx[i].bins == NULL) {
               working_ctx.prj_ctx[i].bins = memalign(32,rozofs_msg_psize*(ROZOFS_MAX_BLOCK_PER_MSG+1));
	    }	   
            working_ctx.prj_ctx[i].prj_state = PRJ_READ_IDLE;
          }


	  if ((block_end-*block_start+1) < requested_blocks){
	    requested_blocks = (block_end-*block_start+1);
	  }

          /*
	  ** Enforce throughput limitation
	  */
          enforce_throughput();

          // Read every available bins
	  ret = rbs_read_all_available_proj(re->storages, spare_idx, layout, bsize, cid,
                                	    re->dist_set_current, re->fid, *block_start,
                                	    requested_blocks, &nb_blocks_read_distant,
                                	    &working_ctx,
					    size_read);

	  // Reading at least inverse projection has failed				  
          if (ret != 0) {
              remove_file = 0;// Better keep the file	
              errno = EIO;
 	      rbs_error.spare_read++;
	      *error = rozofs_rbs_error_read_error;		      
              goto out;
          }

	  if (nb_blocks_read_distant == 0) break; // End of chunk
	  
          if (nb_blocks_read_distant == -1) {
	     /*
	     ** File has been deleted
	     */
	     errno = ENOENT;
	     rbs_error.spare_read_enoent++;
	     status = RBS_EXE_ENOENT;
	     goto out;
          }	  	

          /*
          ** No empty block sequence neither data blok sequence detected up to now 
          ** in this read blocks since not yet inspected !!!!
          */
          firstEmptyBlock = -1;
          firstDataBlock  = -1;
          nbBlocks        =  0;

	  // Loop on the received blocks
          pBlock = &working_ctx.block_ctx_table[0];	
          for (block_idx = 0; block_idx < nb_blocks_read_distant; block_idx++,pBlock++) {

              count = rbs_count_timestamp_tb(working_ctx.prj_ctx, spare_idx, layout, bsize, block_idx,
                                             rbs_prj_idx_table, 
			  		     &pBlock->timestamp,
                                             &pBlock->effective_length);

	      // Less than rozofs_inverse projection. Can not regenerate anything 
	      // from what has been read				   	
	      if (count < 0) {
                  remove_file = 0;// Better keep the file	    
        	  errno = EIO;
		  rbs_error.spare_read_no_enough++;
	          *error = rozofs_rbs_error_not_enough_projection_read;	  
        	  goto out;	      
	      }

	      // All projections have been read. Nothing to regenerate for this block
	      if (count >= rozofs_forward) {              
                 /*
                 ** If a sequence of empty blocks or a sequence of data block was detected up to here, 
                 ** we need to write it now
                 */
                 rbs_restore_flush(firstEmptyBlock,NULL,1);
                 rbs_restore_flush(firstDataBlock,pforward,1);
                 continue;
              }
              
              if ((rozofs_forward - count) > 1) {
                *more_prj2rebuild = 1;
              }  

              // Case of the empty block
              if (pBlock->timestamp == 0) {
              
        	 remove_file = 0;	// This file must exist   
                 
                 /*
                 ** If a sequence of data blocks was detected up to here, we need to write it now
                 */
                 rbs_restore_flush(firstDataBlock,pforward,1);
                     
                 /*
                 ** We are starting a new empty block sequence, 
                 ** or just continuing one
                 */
                 if (firstEmptyBlock == -1) {
                   firstEmptyBlock = block_idx;
                 }
                 nbBlocks++;
		 continue;
	      } 
              
              /*
              ** Here is a non empty block. 
              ** If a sequence of empty blocks was detected up to here, we need to write it now
              */
              rbs_restore_flush(firstEmptyBlock,NULL,1);
              
	      // Need to regenerate a projection and need 1rst to regenerate initial data.	

	      // Allocate memory for initial data
              if (working_ctx.data_read_p == NULL) {
		working_ctx.data_read_p = memalign(32,bbytes);
	      }		


              memset(prj_id_present,0,sizeof(prj_id_present));

              for (prj_count = 0; prj_count < count; prj_count++) {

        	  // Get the pointer to the beginning of the projection and extract
        	  // the projection ID
        	  prj_ctx_idx = rbs_prj_idx_table[prj_count];

        	  rozofs_stor_bins_hdr_t *rozofs_bins_hdr_p =
                	  (rozofs_stor_bins_hdr_t*) (working_ctx.prj_ctx[prj_ctx_idx].bins
                	  + (rozofs_msg_psize/sizeof(bin_t) * block_idx));

        	  // Extract the projection_id from the header and fill the table
        	  // of projections for the block block_idx for each projection
        	  projection_id = rozofs_bins_hdr_p->s.projection_id;

		  prj_id_present[projection_id] = 1;

		  if (prj_count < rozofs_inverse) {
        	      rbs_projections[prj_count].angle.p = rozofs_get_angles_p(layout,projection_id);
        	      rbs_projections[prj_count].angle.q = rozofs_get_angles_q(layout,projection_id);
        	      rbs_projections[prj_count].size    = rozofs_get_psizes(layout,bsize,projection_id);
        	      rbs_projections[prj_count].bins    = (bin_t*) (rozofs_bins_hdr_p + 1);
		  }   
              }

              // Inverse data for the block (first_block_idx + block_idx)
              transform128_inverse_copy((pxl_t *) working_ctx.data_read_p,
                		rozofs_inverse,
                		bbytes / rozofs_inverse / sizeof (pxl_t),
                		rozofs_inverse, rbs_projections,
				rozofs_get_max_psize(layout,bsize)*sizeof(bin_t));

	      // Find out which projection id to regenerate
              for (projection_id = 0; projection_id < rozofs_safe; projection_id++) {
	          if (prj_id_present[projection_id] == 0) break;
	      }

     
              /*
              ** Allocate memory for regenerated projections
              */
     	      if (pforward == NULL) {
	        pforward = memalign(32,rozofs_msg_psize*(ROZOFS_MAX_BLOCK_PER_MSG+1));
	      }	
              
              /*
              ** Point to this block in the write message
              */
	      rozofs_bins_hdr_p  = (rozofs_stor_bins_hdr_t *) (pforward + (nbBlocks * rozofs_msg_psize));
	      rozofs_bins_foot_p = (rozofs_stor_bins_footer_t*)((bin_t*)(rozofs_bins_hdr_p+1)+rozofs_get_psizes(layout,bsize,projection_id));	

	      // Describe projection to rebuild 
              rbs_projections[projection_id].angle.p = rozofs_get_angles_p(layout,projection_id);
              rbs_projections[projection_id].angle.q = rozofs_get_angles_q(layout,projection_id);
              rbs_projections[projection_id].size    = rozofs_get_128bits_psizes(layout,bsize,projection_id);
              rbs_projections[projection_id].bins    = (bin_t*) (rozofs_bins_hdr_p + 1);

              // Generate projections to rebuild
              transform128_forward_one_proj((bin_t *)working_ctx.data_read_p, 
	                        	 rozofs_inverse, 
	                        	 bbytes / rozofs_inverse / sizeof (pxl_t),
					 projection_id, 
					 rbs_projections);

	      // Fill projection header			       
	      rozofs_bins_hdr_p->s.projection_id     = projection_id;			       
	      rozofs_bins_hdr_p->s.effective_length  = pBlock->effective_length;
	      rozofs_bins_hdr_p->s.timestamp         = pBlock->timestamp;			       
              rozofs_bins_hdr_p->s.version           = 0;
              rozofs_bins_hdr_p->s.filler            = 0;

              rozofs_bins_foot_p->timestamp          = pBlock->timestamp;	

              remove_file = 0;	// This file must exist   

              /*
              ** We are starting a new data block sequence, 
              ** or just continuing one
              */
              if (firstDataBlock == -1) {
                firstDataBlock = block_idx;
              }
              nbBlocks++;
          }
          
          /*
          ** If a sequence of empty blocks was detected up to here, we need to write it now
          */
          rbs_restore_flush(firstEmptyBlock,NULL,1);
          
          /*
          ** If a sequence of data blocks was detected up to here, we need to write it now
          */
          rbs_restore_flush(firstDataBlock,pforward,1);                           

	  *block_start += nb_blocks_read_distant;

      }	
      
      /*
      ** Spare chunk no more be usefull
      */
      if (remove_file) {
	ret = sclient_remove_chunk_rbs(re->storages[local_idx], cid, sid, layout, 
	                               1/*spare*/, re->bsize,
	                               re->dist_set_current, re->fid, chunk, rebuild_ref);
      }

      if (rebuild_ref != 0) {
        /*
        ** If a sequence of empty blocks was detected up to here, we need to write it now
        */
        rbs_restore_flush(firstEmptyBlock,NULL,1);

        /*
        ** If a sequence of data blocks was detected up to here, we need to write it now
        */
        rbs_restore_flush(firstDataBlock,pforward,1);  

        /*
        ** Rebuild is successfull
        */               
	sclient_rebuild_stop_rbs(re->storages[local_idx], cid, sid, re->fid, 
	                         rebuild_ref, SP_SUCCESS);
				 
	rebuild_ref = 0;
      } 
      
      // end of file
      if (*block_start == chunk * block_per_chunk) {
        break;
      }
      
      // Next chunk
      chunk++;        
    }
      
    status = RBS_EXE_SUCCESS;
    			      
out:                                  

    *block_start = chunk * block_per_chunk;    
    
    if (rebuild_ref != 0) {
      /*
      ** Rebuild is failed
      */               
      sclient_rebuild_stop_rbs(re->storages[local_idx], cid, sid, re->fid, 
                               rebuild_ref, SP_FAILURE);
    }    
    
    // Clear the working context
    if (working_ctx.data_read_p) {
      free(working_ctx.data_read_p);
      working_ctx.data_read_p = NULL;	
    }	 
      
    /*
    ** Free memory allocated for pojection reading
    */   
    for (i = 0; i < rozofs_safe; i++) {
        if (working_ctx.prj_ctx[i].bins) {
            free(working_ctx.prj_ctx[i].bins);
	    working_ctx.prj_ctx[i].bins = NULL;
	}	   
    }    
     
    /*
    ** Free memory allocated for regenerated projections
    */ 
    if (pforward) {
      free(pforward);
      pforward = NULL;
    }

    return status;
}
/*-----------------------------------------------------------------------------
**
** Nominal file rebuilding
**
** @param cid          Cluster to rebuild
** @param sid          target SID to rebuild
** @param layout       Layout of the file
** @param local_idx    Index of the storio within the storage array in re structure
** @param block_start  First block to rebuild. 
** @param block_end    Last block to rebuild. 
** @param re           Descriptor of the file to rebuild
** @param spare_idx    Index of the storio to rebuild within the file SID distribution
**                     Actualy should be equal to local_idx !
** @param size_written To return the written bytes count                
** @param size_read    To return the read bytes count
** @param error        Returned error core
**
**----------------------------------------------------------------------------
*/    
RBS_EXE_CODE_E rbs_restore_one_rb_entry(cid_t             cid, 
                                        sid_t             sid, 
                                        uint8_t           layout,
                        		int               local_idx, 
					uint32_t        * block_start,
					uint32_t          block_end, 
					rb_entry_t      * re, 
					uint8_t           proj_id_to_rebuild,
					uint64_t        * size_written,
					uint64_t        * size_read,
					uint8_t         * error) {
    RBS_EXE_CODE_E    status = RBS_EXE_FAILED;
    int               ret    = -1;
    rbs_storcli_ctx_t working_ctx;
    uint32_t          rebuild_ref;
  
    // Get rozofs layout parameters
    uint32_t bsize                  = re->bsize;
    uint16_t rozofs_disk_psize      = rozofs_get_psizes(layout,bsize,proj_id_to_rebuild);
    uint16_t disk_block_size        = (rozofs_disk_psize+3) * 8;   
    uint8_t  rozofs_safe            = rozofs_get_rozofs_safe(layout);
    uint16_t rozofs_max_psize       = rozofs_get_max_psize_in_msg(layout,bsize);
    uint32_t requested_blocks       = ROZOFS_MAX_BLOCK_PER_MSG;
    uint32_t nb_blocks_read_distant = requested_blocks;
    int     i;   
    uint32_t block_per_chunk = ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(re->bsize);
    uint32_t chunk           = *block_start / block_per_chunk;;
    int      empty = 0;
    uint32_t   firstEmptyBlock = -1;
    uint32_t   firstDataBlock  = -1;
    uint32_t   nbBlocks        =  0;
    rbs_inverse_block_t * pBlock;
    char                * pforward;
    int                   block_idx;
       
    // Clear the working context
    memset(&working_ctx, 0, sizeof (working_ctx));


    /*
    ** Ask for a rebuild
    */
    rebuild_ref = sclient_rebuild_start_rbs(re->storages[local_idx], cid, sid, re->fid, 
                                            relocate?SP_NEW_DEVICE:SP_SAME_DEVICE, chunk, 0 ,  
					    *block_start, block_end);
    /*
    ** Rebuild is refused
    */                                        
    if (rebuild_ref == 0) {
      if (errno == EAGAIN) {
        rbs_error.nom_start_again++;
	*error = rozofs_rbs_error_file_to_much_running_rebuild;
      }
      else {
        rbs_error.nom_start++;
	*error = rozofs_rbs_error_rebuild_start_failed;	
      }
      goto out;
    }

    /*
    ** Allocate memory for projections
    */
    for (i = 0; i < rozofs_safe; i++) {
      working_ctx.prj_ctx[i].bins = memalign(32,rozofs_max_psize*(ROZOFS_MAX_BLOCK_PER_MSG+1));
    }

    /*
    ** Allocate memory for the regenerated user data
    */    
    working_ctx.data_read_p = memalign(32,(ROZOFS_MAX_BLOCK_PER_MSG+1) * ROZOFS_BSIZE_BYTES(bsize));
        
    // While we can read in the bins file
    while(*block_start <= block_end) {
         
        /* 
        ** Reset the working context
        */
	for (i = 0; i < rozofs_safe; i++) {
          working_ctx.prj_ctx[i].prj_state = PRJ_READ_IDLE;
	}
        memset(&working_ctx.block_ctx_table, 0, sizeof (working_ctx.block_ctx_table));
        working_ctx.redundancy_stor_idx_current = 0;	
        
	if ((block_end!=0xFFFFFFFF) && ((block_end-*block_start+1) < requested_blocks)) {
	  requested_blocks = (block_end-*block_start+1);
	}

        /*
	** Enforce throughput limitation
	*/
        enforce_throughput();

        // Try to read blocks on others storages
        ret = rbs_read_blocks(re->storages, local_idx, layout, bsize, cid,
                	      re->dist_set_current, re->fid, *block_start,
                	      requested_blocks, &nb_blocks_read_distant, retries,
                	      &working_ctx,
			      size_read, 
                              &empty);

        if (ret != 0) {
	  rbs_error.nom_read++;
	  *error = rozofs_rbs_error_read_error;	
	  goto out;
	}
        
        if (nb_blocks_read_distant == 0) break; // End of file
        
        if (nb_blocks_read_distant == -1) { // File deleted
	   status = RBS_EXE_ENOENT;
	   rbs_error.nom_read_enoent++;
	   goto out;
        }
	
        /*
        ** In case all read blocks are empty, just ask for an empty write
        */
        if (empty) {

            // Write empty blocks
            nbBlocks        = nb_blocks_read_distant;
            firstEmptyBlock = 0;
            rbs_restore_flush(firstEmptyBlock,NULL,0);
            
	    *block_start += nb_blocks_read_distant;	
            continue;             
        }
        
        /*
        ** Reset projection to rebuild
        */
        memset(working_ctx.prj_ctx[proj_id_to_rebuild].bins, 0,rozofs_max_psize * requested_blocks);

        /*
        ** Re-generate projections to rebuild
        */
        ret = rbs_transform_forward_one_proj(working_ctx.prj_ctx,
                			     working_ctx.block_ctx_table,
                			     layout,bsize,0,
                			     nb_blocks_read_distant,
                			     proj_id_to_rebuild,
                			     working_ctx.data_read_p);
        if (ret != 0) {
            severe("rbs_transform_forward_one_proj failed: %s",strerror(errno));
	    rbs_error.nom_transform++;
	    *error = rozofs_rbs_error_transform_error;
            goto out;
        }

        /*
        ** Loop on the re-generated blocks and split the writing into continuous sequences 
        ** of empty blocks or non empty blocks 
        */
        pBlock = &working_ctx.block_ctx_table[0];		
        for (block_idx = 0; block_idx < nb_blocks_read_distant; block_idx++,pBlock++) {           
          /*
          ** This is an empty block 
          */
          if (pBlock->timestamp == 0) {
            /*
            ** If a sequence of data blocks was detected up to here, we need to write it now
            */
            rbs_restore_flush(firstDataBlock,pforward,0);

            /*
            ** We are starting a new empty block sequence, 
            ** or just continuing one
            */
            if (firstEmptyBlock == -1) {
              firstEmptyBlock = block_idx;
            }
            nbBlocks++;
	    continue;
	  } 

          /*
          ** This is not an empty block
          ** If a sequence of empty blocks was detected up to here, we need to write it now
          */
          rbs_restore_flush(firstEmptyBlock,NULL,0);

          if (firstDataBlock == -1) {
            firstDataBlock = block_idx;
            pforward       = (char *)working_ctx.prj_ctx[proj_id_to_rebuild].bins;
            pforward       += (block_idx * rozofs_max_psize);
          }
          nbBlocks++;
       }
       
       /*
       ** If a sequence of data blocks was detected up to here, we need to write it now
       */
       rbs_restore_flush(firstDataBlock,pforward,0);
       /*
       ** If a sequence of empty blocks was detected up to here, we need to write it now
       */
       rbs_restore_flush(firstEmptyBlock,NULL,0);  
				
       *block_start += nb_blocks_read_distant;	             
    }
    status = RBS_EXE_SUCCESS;
out:
    if (rebuild_ref != 0) {
      sclient_rebuild_stop_rbs(re->storages[local_idx], cid, sid, re->fid, rebuild_ref, 
                               (status==0)?SP_SUCCESS:SP_FAILURE);
    }      
    
    /*
    ** Free memory for the regenerated user data
    */    
    if (working_ctx.data_read_p) {
      free(working_ctx.data_read_p);
      working_ctx.data_read_p = NULL;
    } 
     
    /*
    ** Free memory for projections
    */
    for (i = 0; i < rozofs_safe; i++) {
      if (working_ctx.prj_ctx[i].bins) {
        free(working_ctx.prj_ctx[i].bins);
        working_ctx.prj_ctx[i].bins = NULL;
      }  
    }     
    return status;
}
/*-----------------------------------------------------------------------------
**
** Compact the job list when not completed
**
**----------------------------------------------------------------------------
*/
void storaged_rebuild_compact_list(char * fid_list, int fd) {
  uint64_t   write_offset;
  uint64_t   read_offset;
  int        entry_size=0;
  rozofs_rebuild_entry_file_t   file_entry;
  fid_t      null_fid={0};
        
  write_offset = 0;
  read_offset  = 0;
  
  while (1) {
  
    if (pread(fd,&file_entry,sizeof(file_entry),read_offset) <= 0) break;
    
    entry_size = rbs_entry_size_from_layout(file_entry.layout);   
    read_offset += entry_size; 
       
    if (file_entry.todo == 0) continue;
    if (memcmp(null_fid,file_entry.fid,sizeof(fid_t))==0) {
      continue;
    }

    if (pwrite(fd, &file_entry, entry_size, write_offset)!=entry_size) {
      severe("pwrite size %lu offset %llu %s",(unsigned long int)entry_size, 
             (unsigned long long int)write_offset, strerror(errno));
    }     
    write_offset += entry_size;
  }
    
    
  if (ftruncate(fd, write_offset) < 0) {
    severe("ftruncate(%s,%llu) %s",fid_list,(long long unsigned int)write_offset,strerror(errno));
  }    
}
/*-----------------------------------------------------------------------------
**
** Check the existence of a FID from the export
** Just in case it has been deleted
**
** @param fid                  The fid to check
**
** @retval 1 when the file has been deleted / 0 when the file  still exist
**
**----------------------------------------------------------------------------
*/
int check_fid_deleted_from_export(fid_t fid) {
  uint32_t   bsize;
  uint8_t    layout; 
  ep_mattr_t attr;
  int        ret;

  // Resolve this FID thanks to the exportd
  errno = 0;
  ret = rbs_get_fid_attr(export_param, fid, &attr, &bsize, &layout);
 
  if ((ret != 0)&&(errno == ENOENT)) {
    return 1;
  }  
  return 0;
}
/*-----------------------------------------------------------------------------
**
** Rebuild a list of FID 
**
** @param   fid_list        File containing the list of FID to rebuild
** @param   statFilename    File containing statistics related to this rebuild list
**
** @retval 0 when rebuild is successfull / 1 when rebuild is not completed
**
**----------------------------------------------------------------------------
*/
int storaged_rebuild_list(cid_t cid, sid_t sid, char * fid_list, char * statFilename) {
  int        fdlist = -1;
  int        fdstat = -1;  
  int        nbJobs=0;
  int        nbSuccess=0;
  uint64_t   offset;
  uint64_t   next_offset;
  ROZOFS_RBS_COUNTERS_T         statistics;
  rozofs_rebuild_entry_file_t   file_entry;
  rozofs_rebuild_entry_file_t   file_entry_saved;
  int        ret;
  uint8_t    rozofs_safe,rozofs_forward,rozofs_inverse; 
  uint8_t    prj;
  int        spare;
  int        local_index=-1;    
  rb_entry_t re;
  fid_t      null_fid={0};
  int        failed,available;  
  uint64_t   size_written = 0;
  uint64_t   size_read    = 0;
  int        more_prj2rebuild;
        
  fdlist = open(fid_list,O_RDWR);
  if (fdlist < 0) {
    if (errno == ENOENT) {
      REBUILD_MSG("  <-> %s no file to rebuild",fid_list);    
      return 0;
    }
    
    severe("Can not open file %s %s",fid_list,strerror(errno));
    goto error;
  } 

        
  fdstat = open(statFilename,O_RDWR | O_CREAT, 0755);
  if (fdstat < 0) {
      severe("Can not open file %s %s",statFilename,strerror(errno));
      goto error;
  }
  memset(&statistics,0,sizeof(statistics));
  ret = pread(fdstat,&statistics,sizeof(statistics),0);  
  if ((ret != 0) && (ret != sizeof(statistics))) {
      severe("Can not read statistics in file %s %s",statFilename,strerror(errno));
      goto error;
  }  
    
  // Get connections for this given cluster  
  rbs_init_cluster_cnts(&cluster_entries, cid, sid, &failed,&available);
  
  REBUILD_MSG("   -> %s rebuild start",fid_list);
  
  nbJobs    = 0;
  nbSuccess = 0;
  offset    = 0;

  next_offset = 0;
    
  while (1) {

    
    offset = next_offset;
    if (pread(fdlist,&file_entry,sizeof(file_entry),offset) <= 0) {
      break;
    } 
           
    /*
    ** Check that enough servers are available
    */
    int entry_size = rbs_entry_size_from_layout(file_entry.layout);
    next_offset = offset + entry_size; 
    if (file_entry.todo == 0) continue;
    if (memcmp(null_fid,file_entry.fid,sizeof(fid_t))==0) {
      severe("Null entry");
      continue;
    }
 
    rozofs_get_rozofs_invers_forward_safe(file_entry.layout, &rozofs_inverse, &rozofs_forward, &rozofs_safe);

    nbJobs++;
    
    if (available<rozofs_inverse) {
      /*
      ** Not possible to rebuild any thing
      */
      file_entry.error = rozofs_rbs_error_not_enough_storages_up;
      if (pwrite(fdlist, &file_entry, entry_size, offset)!=entry_size) {
	severe("pwrite size %lu offset %llu %s",(unsigned long int)entry_size, 
               (unsigned long long int) offset-entry_size, strerror(errno));
      }
      continue;
    }

   // Padd end of distibution with 0. Just in case...
    memset(&file_entry.dist_set_current[rozofs_safe],0,ROZOFS_SAFE_MAX-rozofs_safe); 
    memcpy(&file_entry_saved,&file_entry,entry_size);

    memcpy(re.fid,file_entry.fid, sizeof(re.fid));
    memcpy(re.dist_set_current,file_entry.dist_set_current, sizeof(re.dist_set_current));
    re.bsize  = file_entry.bsize;
    re.layout = file_entry.layout;
  

    if (sigusr_received) {
      goto error;
    }    

    local_index = rbs_get_rb_entry_cnts(&re, &cluster_entries, cid, sid, rozofs_inverse);  
    if (local_index == -1) {
      if      (errno==EINVAL) file_entry.error = rozofs_rbs_error_no_such_cluster;
      else if (errno==EPROTO) file_entry.error = rozofs_rbs_error_not_enough_storages_up;
      else {                  
        file_entry.error = rozofs_rbs_error_unknown;
        severe( "rbs_get_rb_entry_cnts failed cid/sid %d/%d %s", cid, sid, strerror(errno));
      }  
      /*
      ** Re write entry in input file
      */
      if (pwrite(fdlist, &file_entry, entry_size, offset)!=entry_size) {
	severe("pwrite size %lu offset %llu %s",(unsigned long int)entry_size, 
               (unsigned long long int) offset-entry_size, strerror(errno));
      }                                
      continue; // Try with the next
    }
    
    // Compute the proj_id to rebuild
    // Check if the storage to rebuild is
    // a spare for this entry
    for (prj = 0; prj < rozofs_safe; prj++) {
      if (re.dist_set_current[prj] == sid)  break;
    }  
    if (prj >= rozofs_forward) spare = 1;
    else                       spare = 0;


    int retry = 0; 
    while (retry < 1) { /* reloop 3 times immediatly on broken rebuild */
      
      retry++; 
     
      size_written = 0;
      size_read    = 0;
      
      // Restore this entry
      uint32_t block_start = file_entry.block_start;
      if (spare == 1) {
        more_prj2rebuild = 0;
	ret = rbs_restore_one_spare_entry(cid, sid,
	                                  file_entry.layout, local_index, 
                                          &block_start, file_entry.block_end, 
					  &re, prj,
					  &size_written,
					  &size_read,
					  &file_entry.error,
                                          &more_prj2rebuild);                                             
      }
      else {
	ret = rbs_restore_one_rb_entry(cid, sid,
	                               file_entry.layout, local_index,
                                       &block_start, file_entry.block_end, 
				       &re, prj, 
				       &size_written,
				       &size_read,
				       &file_entry.error);
      } 
	  
      /*
      ** In case the rebuild failed, check with the export whether 
      ** the FID still exists
      */
      if (ret == RBS_EXE_FAILED) {
        if (check_fid_deleted_from_export(file_entry.fid)) {   
	  //char fidString[64];
	  //rozofs_fid2string(re.fid,fidString);
	  //warning("@rozofs_uuid@%s does no more exist",fidString); 	
	  ret = RBS_EXE_ENOENT;
	}  
      }
      /* 
      ** Rebuild is successfull
      */
      switch(ret) {
      
	case RBS_EXE_SUCCESS:
	case RBS_EXE_ENOENT:	
	
	  if (ret == RBS_EXE_ENOENT) {
	    file_entry.error = rozofs_rbs_error_file_deleted;
	    statistics.deleted++;
	  }  
	  else {                      
	    file_entry.error = rozofs_rbs_error_none;
	  }
	  nbSuccess++;
	  // Update counters in header file 
	  statistics.done_files++;
	  if (spare == 1) {
            statistics.written_spare += size_written;
	    statistics.read_spare    += size_read;
	  }
	  statistics.written += size_written;
	  statistics.read    += size_read;       

	  if (pwrite(fdstat, &statistics, sizeof(statistics), 0)!= sizeof(statistics)) {
            severe("pwrite %s %s",statFilename,strerror(errno));
	  }          

	  if ((nbSuccess % (16*1024)) == 0) {
            REBUILD_MSG("  ~ %s %d/%d",fid_list,nbSuccess,nbJobs);
	  } 
	  /*
	  ** This file has been rebuilt so remove it from the job list
	  */
	  file_entry.todo = 0;
          break;


	/*
	** Rebuild is failed, nevetherless some pieces of the file may have
	** been successfully rebuilt and needs not to be rebuilt again on a
	** next trial
	*/
	default:
	  severe("Unexpected return code %d.",ret);	  
	case RBS_EXE_FAILED:
	case RBS_EXE_BROKEN:
	  /*
	  ** In case of file relocation, the new data chunk file has been removed 
	  ** and the previous data chunk file location has been restored
	  ** when the rebuild has failed. So we are back to the starting point of
	  ** the rebuilt and there has been no improvment...
	  ** When no relocation was requested, the block_start has increased up to 
	  ** where the rebuild has failed. These part before block_start is rebuilt 
	  ** and needs not to be redone although the glocal rebuild has failed. 
	  */
	  if (!relocate) {
            file_entry.block_start = block_start;
	  }
	  break;  
      }

      /*
      ** Update input job file if any change
      */
      if (memcmp(&file_entry_saved,&file_entry, entry_size) != 0) {
	if (pwrite(fdlist, &file_entry, entry_size, offset)!=entry_size) {
	  severe("pwrite size %lu offset %llu %s",(unsigned long int)entry_size, 
        	 (unsigned long long int) offset-entry_size, strerror(errno));
	}
      }
      
      if (ret != RBS_EXE_BROKEN) break;
      /* reloop 3 times immediatly on broken rebuild */
    } 
    
    /* Next file to rebuild */     
  }
  
  if (nbSuccess == nbJobs) {
    close(fdlist);
    unlink(fid_list);
    close(fdstat);	
    REBUILD_MSG("  <- %s rebuild success of %d files",fid_list,nbSuccess);    
    return 0;
  }
  
  /*
  ** Truncate the file after the last failed entry
  */
  if (nbSuccess!=0) {
    storaged_rebuild_compact_list(fid_list, fdlist);
  }
   
error:
 
  if (fdlist != -1) close(fdlist);
  if (fdstat != -1) close(fdstat);	
    
  if (sigusr_received) {
    REBUILD_MSG("  <- %s rebuild paused. %d done.",fid_list,nbSuccess);    
  }
  else {
    REBUILD_MSG("  <- %s rebuild failed. %d failed /%d.",fid_list,nbJobs-nbSuccess,nbJobs);
    display_rbs_errors();
  }
  
	
  return 1;
}
   
      
/*-----------------------------------------------------------------------------
**
** Rebuild a list of FID 
**
** @param   fid_list        File containing the list of FID to rebuild
** @param   statFilename    File containing statistics related to this rebuild list
**
** @retval 0 when rebuild is successfull / 1 when rebuild is not completed
**
**----------------------------------------------------------------------------
*/
int storaged_resecure_list(cid_t cid, sid_t sid, char * fid_list, char * statFilename) {
  int        fdlist = -1;
  int        fdstat = -1;  
  int        nbJobs=0;
  int        nbSuccess=0;
  uint64_t   offset;
  uint64_t   next_offset;
  ROZOFS_RBS_COUNTERS_T         statistics;
  rozofs_rebuild_entry_file_t   file_entry;
  rozofs_rebuild_entry_file_t   file_entry_saved;
  int        ret;
  uint8_t    rozofs_safe,rozofs_forward,rozofs_inverse; 
  int        local_index=-1;    
  rb_entry_t re;
  fid_t      null_fid={0};
  int        failed,available;  
  uint64_t   size_written = 0;
  uint64_t   size_read    = 0;
  int        more_prj2rebuild;
  sid_t      sid2rebuild[16];
  sid_t      sidIdxInDist[16];
  int        result[16];
  uint8_t    sidIdx;
  int        nbSid2rebuild;
        
  fdlist = open(fid_list,O_RDWR);
  if (fdlist < 0) {
    if (errno == ENOENT) {
      REBUILD_MSG("  <-> %s no file to resecure",fid_list);    
      return 0;
    }
    
    severe("Can not open file %s %s",fid_list,strerror(errno));
    goto error;
  } 

        
  fdstat = open(statFilename,O_RDWR | O_CREAT, 0755);
  if (fdstat < 0) {
      severe("Can not open file %s %s",statFilename,strerror(errno));
      goto error;
  }
  memset(&statistics,0,sizeof(statistics));
  ret = pread(fdstat,&statistics,sizeof(statistics),0);  
  if ((ret != 0) && (ret != sizeof(statistics))) {
      severe("Can not read statistics in file %s %s",statFilename,strerror(errno));
      goto error;
  }  

    
  // Get connections for this given cluster  
  rbs_init_cluster_cnts(&cluster_entries, cid, sid, &failed,&available);
  
  REBUILD_MSG("   -> %s resecure start",fid_list);
  
  nbJobs    = 0;
  nbSuccess = 0;
  offset    = 0;

  next_offset = 0;
    
  while (1) {

    
    offset = next_offset;
    if (pread(fdlist,&file_entry,sizeof(file_entry),offset) <= 0) {
      break;
    } 
           
    /*
    ** Check that enough servers are available
    */
    int entry_size = rbs_entry_size_from_layout(file_entry.layout);
    next_offset = offset + entry_size; 
    if (file_entry.todo == 0) continue;
    if (memcmp(null_fid,file_entry.fid,sizeof(fid_t))==0) {
      severe("Null entry");
      continue;
    }
 
    rozofs_get_rozofs_invers_forward_safe(file_entry.layout, &rozofs_inverse, &rozofs_forward, &rozofs_safe);

    nbJobs++;
    
    if (available<rozofs_inverse) {
      /*
      ** Not possible to rebuild any thing
      */
      file_entry.error = rozofs_rbs_error_not_enough_storages_up;
      if (pwrite(fdlist, &file_entry, entry_size, offset)!=entry_size) {
	severe("pwrite size %lu offset %llu %s",(unsigned long int)entry_size, 
               (unsigned long long int) offset-entry_size, strerror(errno));
      }
      continue;
    }

    // Padd end of distibution with 0. Just in case...
    memset(&file_entry.dist_set_current[rozofs_safe],0,ROZOFS_SAFE_MAX-rozofs_safe); 
    memcpy(&file_entry_saved,&file_entry,entry_size);

    memcpy(re.fid,file_entry.fid, sizeof(re.fid));
    memcpy(re.dist_set_current,file_entry.dist_set_current, sizeof(re.dist_set_current));
    re.bsize  = file_entry.bsize;
    re.layout = file_entry.layout;
  

    if (sigusr_received) {
      goto error;
    }   
     
    /*
    ** Find out the spare sids that have to be used for resecuring
    */ 
    nbSid2rebuild = 0;
    for (sidIdx=rozofs_safe-1; sidIdx>=rozofs_forward; sidIdx--) {
      sid2rebuild[nbSid2rebuild]  = re.dist_set_current[sidIdx];
      sidIdxInDist[nbSid2rebuild] = sidIdx;
      result[nbSid2rebuild]       = RBS_EXE_SUCCESS;
      /*
      ** No use to try rebuilding the failed SID !!
      */
      if (sid != sid2rebuild[nbSid2rebuild]) {
        nbSid2rebuild++;
      }
    }   

    /*
    ** Indicates that the file is not completly resecured.
    ** It will be set to 0 in rbs_restore_one_spare_entry
    ** if it is found out that all projections are present
    */ 
    more_prj2rebuild = 1;

    size_written = 0;
    size_read    = 0;

    for (sidIdx = 0; sidIdx < nbSid2rebuild; sidIdx++) { 
      sid_t    currentSid;
      uint32_t block_start;

      /*
      ** The file is completly resecured. Stop rebuilding loop
      */
      if (more_prj2rebuild == 0) {
        break;
      }      
        
      /*
      **  Next spare sid
      */     
      currentSid = sid2rebuild[sidIdx];

      local_index = rbs_get_rb_entry_cnts(&re, &cluster_entries, cid, currentSid, rozofs_inverse);  
      if (local_index == -1) {
        if      (errno==EINVAL)  file_entry.error = rozofs_rbs_error_no_such_cluster;
        else if (errno==EPROTO)  file_entry.error = rozofs_rbs_error_not_enough_storages_up;
        else {                  
          file_entry.error = rozofs_rbs_error_unknown;
          severe( "rbs_get_rb_entry_cnts failed cid/sid %d/%d %s", cid, currentSid, strerror(errno));
        }  
        /*
        ** Re write entry in input file
        */
        if (pwrite(fdlist, &file_entry, entry_size, offset)!=entry_size) {
	  severe("pwrite size %lu offset %llu %s",(unsigned long int)entry_size, 
                 (unsigned long long int) offset-entry_size, strerror(errno));
        }  
        result[sidIdx] = RBS_EXE_FAILED;                             
        continue; // Try next sid
      }


      // Restore this entry
      block_start = file_entry.block_start;
      more_prj2rebuild = 0;
      ret = rbs_restore_one_spare_entry(cid, currentSid,
	                                file_entry.layout, local_index, 
                                        &block_start, file_entry.block_end, 
					&re, sidIdxInDist[sidIdx],
					&size_written,
					&size_read,
					&file_entry.error,
                                        &more_prj2rebuild);                                             


      /*
      ** In case the rebuild failed, check with the export whether 
      ** the FID still exists
      */
      if (ret == RBS_EXE_FAILED) {
        if (check_fid_deleted_from_export(file_entry.fid)) {  
          file_entry.error = rozofs_rbs_error_file_deleted;
          statistics.deleted++;
          more_prj2rebuild = 0;
	  ret = RBS_EXE_ENOENT;
          break;
	}  
      }
      file_entry.error = rozofs_rbs_error_none;
      /* 
      ** Rebuild is successfull and total
      */
      if ((ret == RBS_EXE_SUCCESS) && (more_prj2rebuild == 0)) break;
      
      /*
      ** Need more rebuilding
      */
      more_prj2rebuild = 1;
      result[sidIdx]   = ret;        
    }

    if (more_prj2rebuild == 0) {
      statistics.resecured++;
    }
    /*
    ** File is not resecured 
    */
    if (more_prj2rebuild == 1) {
      for (sidIdx = 0; sidIdx < nbSid2rebuild; sidIdx++) { 
        if (result[sidIdx] == RBS_EXE_SUCCESS) {
          file_entry.error = rozofs_rbs_error_none;
          more_prj2rebuild = 0;
          break;       
        }
      }
    }
    
    if (more_prj2rebuild == 0) {
      nbSuccess++;
      // Update counters in header file 
      statistics.done_files++;
      statistics.written_spare += size_written;
      statistics.read_spare    += size_read;
      statistics.written       += size_written;
      statistics.read          += size_read;       
      file_entry.todo = 0;
    }      
    if (pwrite(fdstat, &statistics, sizeof(statistics), 0)!= sizeof(statistics)) {
      severe("pwrite %s %s",statFilename,strerror(errno));
    }          

    /*
    ** Update input job file if any change
    */
    if (memcmp(&file_entry_saved,&file_entry, entry_size) != 0) {
      if (pwrite(fdlist, &file_entry, entry_size, offset)!=entry_size) {
	severe("pwrite size %lu offset %llu %s",(unsigned long int)entry_size, 
               (unsigned long long int) offset-entry_size, strerror(errno));
      }
    }
      
    if ((nbSuccess % (16*1024)) == 0) {
      REBUILD_MSG("  ~ %s %d/%d",fid_list,nbSuccess,nbJobs);
    } 
   
  }
  
    
  if (nbSuccess == nbJobs) {
    close(fdlist);
    unlink(fid_list);
    close(fdstat);	
    REBUILD_MSG("  <- %s resecure success of %d files",fid_list,nbSuccess);    
    return 0;
  }
  
  /*
  ** Truncate the file after the last failed entry
  */
  if (nbSuccess!=0) {
    storaged_rebuild_compact_list(fid_list, fdlist);
  }
   
error:
 
  if (fdlist != -1) close(fdlist);
  if (fdstat != -1) close(fdstat);	
    
  if (sigusr_received) {
    REBUILD_MSG("  <- %s resecure paused. %d done.",fid_list,nbSuccess);    
  }
  else {
    REBUILD_MSG("  <- %s resecure failed. %d failed /%d.",fid_list,nbJobs-nbSuccess,nbJobs);
    display_rbs_errors();
  }
	
  return 1;
}
/*-----------------------------------------------------------------------------
**
**  Stop handler
**
**----------------------------------------------------------------------------
*/
static void on_stop() {
    rbs_release_cluster_list(&cluster_entries);
    rozofs_layout_release();
    closelog();
}
/*-----------------------------------------------------------------------------
**
**  Display usage
**
**----------------------------------------------------------------------------
*/
char * utility_name=NULL;
void usage(char * fmt, ...) {
    va_list   args;
    char      error_buffer[512];

    /*
    ** Display optionnal error message if any
    */
    if (fmt) {
      va_start(args,fmt);
      vsprintf(error_buffer, fmt, args);
      va_end(args);   
      severe("%s",error_buffer);
      printf("%s\n",error_buffer);
    }

    printf("RozoFS storage rebuilder - %s\n", VERSION);
    printf("Usage: %s [OPTIONS]\n\n",utility_name);
    printf("   -h, --help\t\t\tprint this message.\n");
    printf("   -c, --cid\tcid to rebuild.\n");
    printf("   -s, --sid\tsid to rebuild.\n");
    printf("   -f, --ftype\tfile type to rebuild <nominal|spare|all>.\n");
    printf("   -r, --ref\trebuild refence.\n");
    printf("   -i, --instance\trebuild instance number.\n");    
    printf("   -q, --quiet \tDo not print.\n");    
    printf("   -t, --throughput\tThroughput limitation in MB/s.\n");    
    printf("   -R  --relocate\tFor relocating files on other devices of the same SID\n");    
    printf("   -S  --reSecure\tFor a resecuring files on their spare storage\n");    
    printf("   -e  --export\tExportd host names\n");    

    if (fmt) exit(EXIT_FAILURE);
    exit(EXIT_SUCCESS); 
}
/*-----------------------------------------------------------------------------
**
**  M A I N
**
**----------------------------------------------------------------------------
*/
int main(int argc, char *argv[]) {
    int    c;
    char * dir;  
    int cid        = -1;     /* Cluster id to rebuild */
    int sid        = -1;     /* Sid to rebuild within the cluster id */
	 
    /*
    ** Change local directory to "/"
    */
    if (chdir("/")!= 0) {}
    rozofs_layout_initialize();
        
    static struct option long_options[] = {
        { "help", no_argument, 0, 'h'},
        { "cid", required_argument, 0, 'c'},	
        { "sid", required_argument, 0, 's'},	
        { "ftype", required_argument, 0, 'f'},	
        { "ref", required_argument, 0, 'r'},	
        { "quiet", no_argument, 0, 'q'},
        { "instance", required_argument, 0, 'i'},	
        { "throughput", required_argument, 0, 't'},
        { "nolog", no_argument, 0, 'N'},	
        { "relocate", no_argument, 0, 'R'},	
        { "reSecure", no_argument, 0, 'S'},	
        { "export", required_argument, 0, 'e'},	
        { 0, 0, 0, 0}
    };

    // Get utility name
    utility_name = basename(argv[0]);   

    // Init of the timer configuration
    rozofs_tmr_init_configuration();
   
    while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "NhH:c:s:r:i:q:f:t:SRe:", long_options, &option_index);

      if (c == -1)
          break;

      switch (c) {

        case 'h':
          usage(NULL);
          break;

        case 'c':
	  if (sscanf(optarg,"%d",&cid)!=1) {
	    usage("Bad cluster id \"%s\"",optarg);
          }
	  break;

        case 'e':
          export_param = optarg;
          break;
          
        case 's':
	  if (sscanf(optarg,"%d",&sid)!=1) {
	    usage("Bad storage id \"%s\"",optarg);
          }
	  break;

        case 'r':
	  if (sscanf(optarg,"%d",&rebuildRef)!=1) {
	    usage("Bad rebuild reference \"%s\"",optarg);
          }
	  break;

        case 'R':
	  relocate = 1;
	  break;

        case 'S':
	  resecure = 1;
	  break;

        case 'f':
	  if (strcmp(optarg,"nominal")==0) {
	    ftype = rbs_file_type_nominal;
	  }
	  else if (strcmp(optarg,"spare")==0) {
	    ftype = rbs_file_type_spare;
	  }
	  else if (strcmp(optarg,"all")==0) {
	    ftype = rbs_file_type_all;
	  }
	  else {
	    usage("Bad file type \"%s\"",optarg);
	  }
	  break;			  			  

        case 'i':
	  if (sscanf(optarg,"%d",&instance)!=1) {
	    usage("Bad instance number \"%s\"",optarg);
	  }
	  break;
	
	case 't':  				  
	  if (sscanf(optarg,"%d",&throughput)!=1) {
	    usage("Bad throughput value \"%s\"",optarg);
	  }
	  break;
        case 'N':
          nolog = 1;
          quiet = 1;
          break;  
	case 'q':
	  quiet = 1;
	  break;

        case '?':
          usage(NULL);
          break;
        default:
          usage("Unexpected parameter \"%c\"",c);
          break;
      }
    }
    uma_dbg_record_syslog_name("RBS_LIST");
    
    
    /*
    ** Check parameter consistency
    */
    if (cid == -1){
        usage("storage_rebuilder failed. Missing --cid option");
    }  
    if (sid == -1){
        usage("storage_rebuilder failed. Missing --sid option");
    }  
    if (export_param == NULL) {
        usage("storage_rebuilder failed. Missing --export option");
    }  
    if (rebuildRef == -1){
        usage("storage_rebuilder failed. Missing --rebuildRef option");
    }  
    if (instance == -1){
        usage("storage_rebuilder failed. Missing --instance option");
    }              
    if ((relocate == 1)&&(resecure == 1)){
        usage("storage_rebuilder failed. resecure and relocate are exclusive options.");
    }              
    /*
    ** read common config file
    */
    common_config_read(NULL);    
 
    /*
    ** Read storage configuration file
    */
    dir = get_rebuild_sid_directory_name(rebuildRef,cid,sid,ftype);	

    // Initialize the list of cluster(s)
    list_init(&cluster_entries);
    memset(&rpcclt_export,0,sizeof(rpcclt_export));

    /*
    ** Try to get the list of storages for this cluster ID
    */
    pExport_hostname = rbs_get_cluster_list(&rpcclt_export, export_param, 0,cid, &cluster_entries);			   
    if (pExport_hostname == NULL) {			   
      severe("Can't get list of others cluster members from export server (%s) for storage to rebuild (cid:%u; sid:%u): %s\n",
             export_param, cid, sid, strerror(errno));
      usage("Can not connect export %s to get cluster %d configuration", export_param, cid);
    }	
    
    
    sprintf(fid_list, "%s/job%d",dir,instance);
    sprintf(statFilename, "%s/stat%d",dir,instance);

    /*
    ** SIGUSR handler
    */
    signal(SIGUSR1, rbs_cath_sigusr);  


    /*
    ** Gt start time to insure throughput the throughput limitation
    */ 
    totalReadSize = 0;
    startTime     = get_us(0); 
  	
    /*
    ** Resecure mode  
    */
    if (resecure) {
      if (storaged_resecure_list(cid, sid, fid_list,statFilename) != 0) goto error;  
      on_stop();
      exit(EXIT_SUCCESS);
    }
    
    /*
    ** Usual rebuild mode 
    */
    if (storaged_rebuild_list(cid, sid, fid_list,statFilename) != 0) goto error;    
    on_stop();
    exit(EXIT_SUCCESS);
    
error:
    exit(EXIT_FAILURE);
}
