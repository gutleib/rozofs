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

#ifndef ROZOFS_STC_SHAREDMEM_H
#define ROZOFS_STC_SHAREDMEM_H
#include <rozofs/core/ruc_buffer_api.h>
#include "rozofs_storcli.h"
//#include <rozofs/rpc/storcli_lbg_prototypes.h>

/**
* structure for shared memory management
*/
typedef struct _rozofs_stc_south_shared_pool_t
{
   key_t key;           /**< key of the shared memory pool */
   char *name;          /**< name of the pool              */
   void *pool_p;        /**< reference of the pool         */
   uint32_t buf_sz;     /**< size of a buffer              */
   uint32_t buf_count;  /**< number of buffer              */
   void *data_p;        /**< pointer to the beginning of the shared memory     */
   uint64_t read_stats; /**< allocate statistics: incremented each time it is allocated     */
} rozofs_stc_south_shared_pool_t;

/**
* array used for storing information related to the storcli shared memory
*/
extern rozofs_stc_south_shared_pool_t rozofs_storcli_shared_mem[];
extern int rozofs_stc_south_shared_mem_init_done;

/*__________________________________________________________________________
*/
 /**
 *  API to create the shared memory used for reading/writing data to/from storio
 
   note: the instance of the shared memory is the concatenantion of the rozofsmount 
         instance and storcli instance: (rozofsmount<<1 | storcli_instance) 
         (assuming of max of 2 storclis per rozofsmount)
         
   @param : key_instance : instance of the shared memory (lower byte of the key)
   @param : pool_id : instance of the shared memory
   @param buf_nb: number of buffers
   @param buf_sz: size of the buffer payload
   @param name: pool name
   
   @retval <> NULL pool pointer
   @retval NULL error
 */
 
void  *rozofs_create_shared_memory(int key_instance,int pool_idx,uint32_t buf_nb, uint32_t buf_sz,char *name);


/*__________________________________________________________________________
*/
/**
* display the configuration of the shared memories associated with the storcli
*/
void rozofs_stc_south_shared_mem_display(char * argv[], uint32_t tcpRef, void *bufRef);
/*
 *________________________________________________________
 */
/**
*  Init of the shared memory structure seen by Rozofsmount

  @param none
  
  @retval none
*/
void rozofs_init_shared_memory();
/*
 *________________________________________________________
 */
/**
*  Allocate a packet buffer from a shared memory for a given storcli

   @param pool_id : index of pool (0: read/1:write 
   
   @retval <>NULL reference of the buffer
   @retval NULL : no buffer
*/
static inline void  *rozofs_alloc_shared_storcli_buf(int pool_id)
{
   if (pool_id >= _ROZOFS_STORCLI_MAX_POOL)
   {
      severe("rozofs_alloc_shared_storcli_buf: pool_id %d out of range",pool_id);
      return NULL;
   }
   if (rozofs_storcli_shared_mem[pool_id].key == 0)
   {
      /*
      ** shared memory is inactive 
      */
      return NULL;
   }
   /*
   ** OK, allocate one buffer and updates the statistics
   */  
   rozofs_storcli_shared_mem[pool_id].read_stats++; 
   return (ruc_buf_getBuffer(rozofs_storcli_shared_mem[pool_id].pool_p));
}
/*
 *________________________________________________________
 */
/**
*  Get the amount of remaining shared buffer within a given pool

   @param pool_id : index of pool (0: read/1:write 
   
   @retval buffer_count
*/
static inline int rozofs_get_shared_storcli_buf_free(int pool_id)
{
   if (pool_id >= _ROZOFS_STORCLI_MAX_POOL)
   {
      severe("bad pool: pool_id %d out of range",pool_id);
      return 0;
   }
   if (rozofs_storcli_shared_mem[pool_id].key == 0)
   {
      /*
      ** shared memory is inactive 
      */
      return 0;
   }
   /*
   ** Get the count of free buffer
   */  
   
   return (ruc_buf_getFreeBufferCount(rozofs_storcli_shared_mem[pool_id].pool_p));
}

/*
 *________________________________________________________
 */
/**
*  Convert the address of the beginning of a payload into
*  a buffer index

   @param ruc_buffer : reference of the ruc_buffer 
   @param pool_id : 0:read/1:write
   
   @retval < 0 : bad buffer
   @retval >= 0 : index of the buffer in the data array
*/
static inline int rozofs_get_shared_storcli_payload_idx(void *ruc_buf,int pool_id,uint32_t *length_p,uint32_t *key_p)
{
   int buffer_idx;

   char *payload = ruc_buf_getPayload(ruc_buf);
   if (payload == NULL) return -1;
   /*
   ** convert it into a buffer index
   */   
   if (pool_id >= _ROZOFS_STORCLI_MAX_POOL)
   {
      severe("rozofs_get_shared_storcli_payload_idx: pool_id %d out of range",pool_id);
      return -1;
   }
   if (rozofs_storcli_shared_mem[pool_id].key == 0)
   {
      /*
      ** shared memory is inactive 
      */
      return -1;
   }
   char *data_p = rozofs_storcli_shared_mem[pool_id].data_p;
   buffer_idx = ( payload - data_p)/rozofs_storcli_shared_mem[pool_id].buf_sz;
   if (buffer_idx > rozofs_storcli_shared_mem[pool_id].buf_count)
   {
      severe("rozofs_get_shared_storcli_payload_idx: bad pointers bufsize %d pool bufsize %d",ruc_buf_getMaxPayloadLen(ruc_buf),rozofs_storcli_shared_mem[pool_id].buf_sz);
      return -1;   
   }
   if (length_p!=NULL) *length_p = rozofs_storcli_shared_mem[pool_id].buf_sz;
   if (key_p!= NULL) *key_p = rozofs_storcli_shared_mem[pool_id].key;
   return buffer_idx;
}


   
#endif
