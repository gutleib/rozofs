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

#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/ruc_buffer_debug.h>

#include "rozofs_storcli_sharedmem.h"
/**
* array used for storing information related to the storcli shared memory
*/
rozofs_stc_south_shared_pool_t rozofs_storcli_shared_mem[_ROZOFS_STORCLI_MAX_POOL];
int rozofs_stc_south_shared_mem_init_done = 0;

/*__________________________________________________________________________
*/
/**
* display the configuration of the shared memories associated with the storcli
*/
void rozofs_stc_south_shared_mem_display(char * argv[], uint32_t tcpRef, void *bufRef)
{
    char *pChar = uma_dbg_get_buffer();
    int i;


    pChar += sprintf(pChar, "   pool     |     key     |  size    | count |    address     |      stats     |\n");
    pChar += sprintf(pChar, "------------+-------------+----------+-------+----------------+----------------+\n");
    for (i = 0; i < _ROZOFS_STORCLI_MAX_POOL; i ++)
    {
      if (rozofs_storcli_shared_mem[i].key == 0) continue;
      pChar +=sprintf(pChar," %10s | %8.8d  | %8.8d |%4d   | %p |%15llu |\n",rozofs_storcli_shared_mem[i].name,
                      rozofs_storcli_shared_mem[i].key,
                      rozofs_storcli_shared_mem[i].buf_sz,
                      rozofs_storcli_shared_mem[i].buf_count,
                      rozofs_storcli_shared_mem[i].data_p,
                      (long long unsigned int)rozofs_storcli_shared_mem[i].read_stats);    
    
    }
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());

}
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
 
void  *rozofs_create_shared_memory(int key_instance,int pool_idx,uint32_t buf_nb, uint32_t buf_sz,char *name)
{
   key_t key = 0x724F0000 | key_instance;

   rozofs_storcli_shared_mem[pool_idx].key          = key;
   rozofs_storcli_shared_mem[pool_idx].name         = strdup(name);
   rozofs_storcli_shared_mem[pool_idx].buf_sz       = buf_sz;
   rozofs_storcli_shared_mem[pool_idx].buf_count    = buf_nb;
   rozofs_storcli_shared_mem[pool_idx].pool_p = ruc_buf_poolCreate_shared(buf_nb,buf_sz,key);
   if (rozofs_storcli_shared_mem[pool_idx].pool_p == NULL) return NULL;
   /*
   ** register the pool with the debug
   */
   ruc_buffer_debug_register_pool(name, rozofs_storcli_shared_mem[pool_idx].pool_p);
   rozofs_storcli_shared_mem[pool_idx].data_p = ruc_buf_get_pool_base_data(rozofs_storcli_shared_mem[pool_idx].pool_p);
   return rozofs_storcli_shared_mem[pool_idx].pool_p;
  
}

/*
 *________________________________________________________
 */
/**
*  Init of the shared memory structure seen by Rozofsmount

  @param none
  
  @retval none
*/
void rozofs_init_shared_memory()
{
  int i;
  
  for (i = 0; i < _ROZOFS_STORCLI_MAX_POOL; i++)
  {
    memset(&rozofs_storcli_shared_mem[i],0,sizeof(rozofs_stc_south_shared_pool_t));
  
  }
  uma_dbg_addTopic("shared_south", rozofs_stc_south_shared_mem_display);
}
