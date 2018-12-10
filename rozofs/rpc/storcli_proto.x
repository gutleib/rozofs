/*
  Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
  This file is part of Rozofs.

  Rozofs is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published
  by the Free Software Foundation; either version 3 of the License,
  or (at your option) any later version.

  Rozofs is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see
  <http://www.gnu.org/licenses/>.
*/

%#include <rozofs/rozofs.h>

typedef unsigned char storcli_uuid_t[ROZOFS_UUID_SIZE];

enum storcli_status_t {
    STORCLI_SUCCESS = 0,
    STORCLI_FAILURE = 1
};

union storcli_status_ret_t switch (storcli_status_t status) {
    case STORCLI_FAILURE:    int error;
    default:            void;
};

/*
** Flags to be set in flags field
*/
enum storcli_flags_e {
    STORCLI_FLAGS_NONE=0,
    STORCLI_FLAGS_EMPTY_FILE=1,
    STORCLI_FLAGS_NO_END_REREAD=2
};

struct storcli_write_arg_t {
    uint16_t    cid;
    uint8_t     sid;
    uint8_t     flags;           
    uint8_t     layout;
    uint8_t     bsize; /* Block size as define in enum ROZOFS_BSIZE_E */
    uint8_t     dist_set[ROZOFS_SAFE_MAX];
    storcli_uuid_t   fid;        
    uint64_t    off;
    opaque      data<>;
};

struct storcli_write_arg_no_data_t {
    uint16_t    cid;
    uint8_t     sid;
    uint8_t     flags;                         
    uint8_t     layout;
    uint8_t     bsize; /* Block size as define in enum ROZOFS_BSIZE_E */   
    uint8_t     dist_set[ROZOFS_SAFE_MAX];
    storcli_uuid_t   fid;        
    uint64_t    off;
    uint32_t    len;
};



struct storcli_read_arg_t {
    uint16_t    cid;
    uint8_t     sid;
    uint8_t     layout;
    uint8_t     spare;
    uint8_t     bsize; /* Block size as define in enum ROZOFS_BSIZE_E */       
    uint8_t     dist_set[ROZOFS_SAFE_MAX];
    storcli_uuid_t   fid; 
    uint32_t     proj_id; 
    uint64_t    bid;
    uint32_t    nb_proj;
};

struct storcli_truncate_arg_t {
    uint16_t    cid;
    uint8_t     sid;
    uint8_t     layout;
    uint8_t     spare;
    uint8_t     bsize; /* Block size as define in enum ROZOFS_BSIZE_E */           
    uint8_t     dist_set[ROZOFS_SAFE_MAX];
    storcli_uuid_t   fid; 
    uint16_t     last_seg; 
    uint64_t    bid; 
};

struct storcli_delete_arg_t {
    uint16_t    cid;
    uint8_t     sid;
    uint8_t     layout;
    uint8_t    dist_set[ROZOFS_SAFE_MAX];
    storcli_uuid_t   fid;
};


struct storcli_read_no_data_ret_t
{
   uint32_t alignment;
   uint32_t alignment1;
   uint32_t alignment2;   
   uint32_t len;
};

union storcli_read_ret_no_data_t switch (storcli_status_t status) {
    case STORCLI_SUCCESS:    storcli_read_no_data_ret_t     len;
    case STORCLI_FAILURE:    int     error;
    default:            void;
};

struct storcli_read_data_ret_t
{
   uint32_t alignment;
   uint32_t alignment1;
   uint32_t alignment2;      
   opaque  dara<>;
};

union storcli_read_ret_t switch (storcli_status_t status) {
    case STORCLI_SUCCESS:   struct storcli_read_data_ret_t  data;
    case STORCLI_FAILURE:    int     error;
    default:            void;
};

program STORCLI_PROGRAM {
    version STORCLI_VERSION {
        void
        STORCLI_NULL(void)                   = 0;

        storcli_status_ret_t
        STORCLI_WRITE(storcli_write_arg_t)        = 1;

        storcli_read_ret_t
        STORCLI_READ(storcli_read_arg_t)          = 2;

        storcli_status_ret_t
        STORCLI_TRUNCATE(storcli_truncate_arg_t)  = 3;

        storcli_status_ret_t
        STORCLI_DELETE(storcli_delete_arg_t)  = 4;

    }=1;
} = 0x20000007;
