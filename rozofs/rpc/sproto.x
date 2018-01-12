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

%#include <rozofs/rozofs.h>

typedef uint32_t sp_uuid_t[ROZOFS_UUID_SIZE_RPC];
typedef uint32_t rozofs_rdma_key_t[6];
enum sp_status_t {
    SP_SUCCESS = 0,
    SP_FAILURE = 1
};

union sp_status_ret_t switch (sp_status_t status) {
    case SP_FAILURE:    int error;
    default:            void;
};

struct sp_write_arg_t {
    uint16_t    cid;
    uint8_t     sid;          
    uint8_t     layout;
    uint8_t     spare;
    uint32_t    rebuild_ref;
    uint32_t    alignment1;
    uint32_t    dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid;        
    uint8_t     proj_id;     
    uint64_t    bid;
    uint32_t    nb_proj;
    uint32_t    bsize;
    opaque      bins<>;
};


/*
** write structure without the bins -> use for storcli
*/
struct sp_write_arg_no_bins_t {
    uint16_t    cid;
    uint8_t     sid;          
    uint8_t     layout;
    uint8_t     spare;
    uint32_t    rebuild_ref;
    uint32_t    alignment1;
    uint32_t    dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid;        
    uint8_t     proj_id;     
    uint64_t    bid;
    uint32_t    nb_proj;
    uint32_t    bsize;   
    uint32_t    len;
};

struct sp_write_repair_arg_t {
    uint16_t    cid;
    uint8_t     sid;          
    uint8_t     layout;
    uint8_t     spare;
    uint32_t    dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid;        
    uint8_t     proj_id;     
    uint64_t    bid;
    uint32_t    nb_proj;
    uint32_t    bsize;
    uint64_t    bitmap;
    opaque      bins<>;
};
struct sp_write_repair2_arg_t {
    uint16_t    cid;
    uint8_t     sid;          
    uint8_t     layout;
    uint8_t     spare;
    uint32_t    dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid;        
    uint8_t     proj_id;     
    uint64_t    bid;
    uint32_t    nb_proj;
    uint32_t    bsize;
    uint64_t    bitmap[3];
    opaque      bins<>;
};

struct sp_b2rep_t {
    uint64_t hdr[2];
    uint32_t relative_bid;
};    

/*
** If you modify this constant, it will modify the position of
** bins field in sp_write_repair3_arg_t. 
**
** The bins HAS TO BE ALIGNED ON 128 bits (16 bytes)
**
*/
const ROZOFS_MAX_REPAIR_BLOCKS = 6;

struct sp_write_repair3_arg_t {
    uint16_t    cid;
    uint8_t     sid;          
    uint8_t     layout;
    uint8_t     spare;
    uint32_t    dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid;        
    uint8_t     proj_id; 
    uint64_t    bid;        
    uint32_t    nb_proj;
    uint32_t    bsize;
    sp_b2rep_t  blk2repair[ROZOFS_MAX_REPAIR_BLOCKS];
    opaque      bins<>;
};
/*
** write repair structure without the bins -> use for storcli
*/
struct sp_write_repair_arg_no_bins_t {
    uint16_t    cid;
    uint8_t     sid;          
    uint8_t     layout;
    uint8_t     spare;
    uint32_t    dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid;        
    uint8_t     proj_id;     
    uint64_t    bid;
    uint32_t    nb_proj;
    uint32_t    bsize;    
    uint64_t    bitmap;
    uint32_t    len;
};

struct sp_write_repair2_arg_no_bins_t {
    uint16_t    cid;
    uint8_t     sid;          
    uint8_t     layout;
    uint8_t     spare;
    uint32_t    dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid;        
    uint8_t     proj_id;     
    uint64_t    bid;
    uint32_t    nb_proj;
    uint32_t    bsize;    
    uint64_t    bitmap[3];
    uint32_t    len;
};
struct sp_write_repair3_arg_no_bins_t {
    uint16_t    cid;
    uint8_t     sid;          
    uint8_t     layout;
    uint8_t     spare;
    uint32_t    dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid;        
    uint8_t     proj_id; 
    uint64_t    bid;        
    uint32_t    nb_proj;
    uint32_t    bsize;
    sp_b2rep_t  blk2repair[ROZOFS_MAX_REPAIR_BLOCKS];
    uint32_t    len;
};

struct sp_read_arg_t {
    uint16_t    cid;
    uint8_t     sid;
    uint8_t     layout;
    uint8_t     bsize;    
    uint8_t     spare;
    uint32_t    dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid; 
    uint64_t    bid;
    uint32_t    nb_proj;
};

struct sp_truncate_arg_no_bins_t {
    uint16_t    cid;
    uint8_t     sid;
    uint8_t     layout;
    uint8_t     bsize;    
    uint8_t     spare;
    uint32_t    dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid; 
    uint8_t     proj_id;
    uint16_t    last_seg;
    uint64_t    last_timestamp; 
    uint64_t    bid; 
    uint32_t    len;
};
    
struct sp_truncate_arg_t {
    uint16_t    cid;
    uint8_t     sid;
    uint8_t     layout;
    uint8_t     bsize;
    uint8_t     spare;
    uint32_t    dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid; 
    uint8_t     proj_id;
    uint32_t    last_seg;
    uint64_t    last_timestamp; 
    uint64_t    bid;     
    opaque      bins<>;    
};

struct sp_remove_arg_t {
    uint16_t    cid;
    uint8_t     sid;
    uint8_t     layout;
    sp_uuid_t   fid;
};

struct sp_remove_chunk_arg_t {
    uint16_t    cid;
    uint8_t     sid;
    uint8_t     layout;  
    uint8_t     bsize;      
    uint8_t     spare;
    uint32_t    dist_set[ROZOFS_SAFE_MAX_RPC];   
    sp_uuid_t   fid;
    uint32_t    rebuild_ref;
    uint32_t    chunk;
};

struct sp_clear_error_arg_t {
    uint8_t     cid;
    uint8_t     sid;
    uint8_t     dev;
    uint8_t     reinit;
};

enum sp_device_e {
    SP_SAME_DEVICE = 0,
    SP_NEW_DEVICE  = 1
};
struct sp_rebuild_start_arg_t {
    uint16_t    cid;
    uint8_t     sid;
    sp_uuid_t   fid;
    sp_device_e device;
    uint8_t     chunk; /* valid when SP_NEW_DEVICE is set */
    uint8_t     spare; /* valid when SP_NEW_DEVICE is set */
    uint64_t    start_bid;
    uint64_t    stop_bid;
};

union sp_rebuild_start_ret_t switch (sp_status_t status) {
    case SP_SUCCESS:    uint32_t rebuild_ref;
    case SP_FAILURE:    int      error;
    default:            void;
};

struct sp_rebuild_stop_arg_t {
    uint16_t    cid;
    uint8_t     sid;
    sp_uuid_t   fid;
    sp_status_t status;
    uint32_t    rebuild_ref;
};

union sp_rebuild_stop_ret_t switch (sp_status_t status) {
    case SP_SUCCESS:    uint32_t rebuild_ref;
    case SP_FAILURE:    int      error;
    default:            void;
};


struct sp_read_t {
    uint32_t    filler;
    uint32_t    filler1;
    uint32_t    filler2;    
    opaque      bins<>;
    uint64_t    file_size;
};

union sp_read_ret_t switch (sp_status_t status) {
    case SP_SUCCESS:    sp_read_t  rsp;
    case SP_FAILURE:    int     error;
    default:            void;
};

union sp_write_ret_t switch (sp_status_t status) {
    case SP_SUCCESS:    uint64_t    file_size;
    case SP_FAILURE:    int         error;
    default:            void;
};

struct sp_rdma_setup_arg_t
{
   rozofs_rdma_key_t rdma_key;
};
   
union sp_rdma_setup_ret_t switch (sp_status_t status) {
    case SP_SUCCESS:    sp_rdma_setup_arg_t  rsp;
    case SP_FAILURE:    int     error;
    default:            void;
};


struct sp_write_rdma_arg_t {
    uint16_t    cid;
    uint8_t     sid;          
    uint8_t     layout;
    uint8_t     spare;
    uint32_t    rebuild_ref;
    uint32_t    alignment1;
    uint32_t    dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid;        
    uint8_t     proj_id;     
    uint64_t    bid;
    uint32_t    nb_proj;
    uint32_t    bsize;   
    rozofs_rdma_key_t rdma_key;
    uint32_t rkey;         /**< remote key to use in ibv_post_send            */
    uint64_t remote_addr;  /**< remote addr                               */
    uint32_t remote_len;   /**< length of the data transfer =  nb_proj*blocksize */
};


struct sp_read_rdma_arg_t {
    uint16_t    cid;
    uint8_t     sid;
    uint8_t     layout;
    uint8_t     bsize;    
    uint8_t     spare;
    uint32_t    dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid; 
    uint64_t    bid;
    uint32_t    nb_proj;
    rozofs_rdma_key_t rdma_key;
    uint32_t rkey;         /**< remote key to use in ibv_post_send            */
    uint64_t remote_addr;  /**< remote addr                               */
    uint32_t remote_len;   /**< length of the data transfer =  nb_proj*blocksize */
};




struct sp_standalone_setup_arg_t
{
   uint32_t   sharemem_key;  /**< reference of the share memory key    */
   uint32_t   bufcount;      /**< number of buffer                     */
   uint32_t   bufsize;       /**< buffer size                          */
};


struct sp_read_standalone_arg_t {
    uint16_t    cid;
    uint8_t     sid;
    uint8_t     layout;
    uint8_t     bsize;    
    uint8_t     spare;
    uint32_t    dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid; 
    uint64_t    bid;
    uint32_t    nb_proj;
    uint32_t    share_buffer_index;   /**< index of the buffer in the share memory   */
    uint32_t   sharemem_key;          /**< reference of the share memory key         */
    uint32_t   buf_offset;            /**< data offset in buffer                     */
};


struct sp_write_standalone_arg_t {
    uint16_t    cid;
    uint8_t     sid;          
    uint8_t     layout;
    uint8_t     spare;
    uint32_t    rebuild_ref;
    uint32_t    alignment1;
    uint32_t    dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid;        
    uint8_t     proj_id;     
    uint64_t    bid;
    uint32_t    nb_proj;
    uint32_t    bsize;   
    uint32_t    share_buffer_index;   /**< index of the buffer in the share memory   */
    uint32_t   sharemem_key;          /**< reference of the share memory key         */
    uint32_t   buf_offset;            /**< data offset in buffer                     */

};


program STORAGE_PROGRAM {
    version STORAGE_VERSION {
        void
        SP_NULL(void)                   = 0;

        sp_write_ret_t
        SP_WRITE(sp_write_arg_t)        = 1;

        sp_read_ret_t
        SP_READ(sp_read_arg_t)          = 2;

        sp_status_ret_t
        SP_TRUNCATE(sp_truncate_arg_t)  = 3;

        sp_write_ret_t
        SP_WRITE_REPAIR(sp_write_repair_arg_t)        = 4;

        sp_status_ret_t
        SP_REMOVE(sp_remove_arg_t)  = 5;
	
        sp_rebuild_start_ret_t
        SP_REBUILD_START(sp_rebuild_start_arg_t)  = 6;

        sp_rebuild_stop_ret_t
        SP_REBUILD_STOP(sp_rebuild_stop_arg_t)  = 7;
			
        sp_status_ret_t
        SP_REMOVE_CHUNK(sp_remove_chunk_arg_t)  = 8;		
			
        sp_status_ret_t
        SP_CLEAR_ERROR(sp_clear_error_arg_t)  = 9;
	
	sp_write_ret_t
        SP_WRITE_REPAIR2(sp_write_repair2_arg_t)        = 10;

	sp_write_ret_t
        SP_WRITE_REPAIR3(sp_write_repair3_arg_t)        = 11;

        sp_read_ret_t
        SP_READ_RDMA(sp_read_rdma_arg_t)          = 12;

        sp_write_ret_t
        SP_WRITE_RDMA(sp_write_rdma_arg_t)        = 13;

        sp_rdma_setup_ret_t
        SP_RDMA_SETUP(sp_rdma_setup_arg_t)  = 14;	
	
        sp_status_ret_t
        SP_STANDALONE_SETUP(sp_standalone_setup_arg_t)  = 15;

        sp_read_ret_t
        SP_READ_STANDALONE(sp_read_standalone_arg_t)          = 16;

        sp_write_ret_t
        SP_WRITE_STANDALONE(sp_write_standalone_arg_t)        = 17;

       sp_write_ret_t
        SP_WRITE_EMPTY(sp_write_arg_no_bins_t)        = 18;		

    }=1;
} = 0x20000002;
