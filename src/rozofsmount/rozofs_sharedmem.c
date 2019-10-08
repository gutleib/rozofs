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
#include <rozofs/core/rozofs_share_memory.h>
#include <rozofs/core/rozofs_numa.h>
#include "rozofs_sharedmem.h"
#include <numa.h>
#include <numaif.h>
/**
* array used for storing information related to the storcli shared memory
*/
rozofs_shared_pool_t rozofs_storcli_shared_mem[SHAREMEM_PER_FSMOUNT];
int rozofs_shared_mem_init_done = 0;

/*
**__________________________________________________________________________________
*/
/**
*  create a shared pool that is numa aware

     @param nbuf : number of buffers
     @param bufsize: size of the buffer
     @param key : shared memory key
     @param membind_p: contains the information for numa, might be NULL
     
     (see mbind(2))
     mode: 

*/
#define MB_1 (1024*1024)
#define MB_2 (2*MB_1)     
void * ruc_buf_poolCreate_shared_numa(uint32_t nbBuf, uint32_t bufsize, key_t key, rozofs_numa_mem_t *membind_p/*,ruc_pf_buf_t init_fct*/)
{
  ruc_buf_t  *poolRef;
  ruc_obj_desc_t  *pnext=(ruc_obj_desc_t*)NULL;
  char *pusrData;
  char *pBufCur;
  ruc_buf_t  *p;
  int shmid;
  int available;
  int ret;

   /*
   **   create the control part of the buffer pool
   */
   poolRef = (ruc_buf_t*)ruc_listCreate(nbBuf,sizeof(ruc_buf_t));
   if (poolRef==(ruc_buf_t*)NULL)
   {
     /*
     ** cannot create the buffer pool
     */
     RUC_WARNING(-1);
     return NULL;
   }
   poolRef->type = BUF_POOL_HEAD;
   /*
   **  create the usrData part
   */
   /*
   **  bufsize MUST long word aligned
   */
   if ((bufsize & 0x3) != 0)
   {
     bufsize = ((bufsize & (~0x3)) + 4 );
   }
  /*
  ** test that the size does not exceed 32 bits
  */
  {
    uint32_t nbElementOrig;
    uint32_t NbElements;
    uint32_t memRequested;

    nbElementOrig = nbBuf;
    if (nbElementOrig == 0)
    {
      RUC_WARNING(-1);
      return NULL;
    }

    memRequested = bufsize*(nbElementOrig);
    NbElements = memRequested/(bufsize);
    if (NbElements != nbElementOrig)
    {
      /*
      ** overlap
      */
      RUC_WARNING(-1);
      return NULL;
    }
  }
  /*
  ** delete existing shared memory
  */
  rozofs_share_memory_free_from_key(key,NULL);
  /*
  ** create the shared memory
  */
  int size = bufsize*nbBuf;
  int page_count = size/MB_2;
  if (size%MB_2 != 0) page_count+=1;
  int allocated_size = page_count*MB_2;
  if ((shmid = shmget(key, allocated_size, IPC_CREAT/*|SHM_HUGETLB */| 0666 )) < 0) {
      fatal("ruc_buf_poolCreate_shared :shmget %s %d",strerror(errno),allocated_size);
      return (ruc_obj_desc_t*)NULL;
  }
  /*
  * Now we attach the segment to our data space.
  */
  if ((pusrData = shmat(shmid, NULL, 0)) == (char *) -1)
  {
     /*
     **  out of memory, free the pool
     */    
    severe("shmat failure for key %x (%s)",key,strerror(errno));
    RUC_WARNING(errno);
    ruc_listDelete_shared((ruc_obj_desc_t*)poolRef);
    return (ruc_obj_desc_t*)NULL;
  }
  /*
  ** map the memory on the specified node
  */
  available = numa_available();
  if ((available >= 0) && (membind_p!=NULL))
  {
    ret = mbind(pusrData,allocated_size,membind_p->mode,&membind_p->nodemask,membind_p->maxnode,membind_p->flags);
    if (ret < 0)
    {
      severe("numa_mbind failure: %s\n",strerror(errno));    
    } 
    /*
    ** clear the memory
    */
    memset( pusrData,0,allocated_size);
  }
   /*
   ** store the pointer address on the head
   */
   poolRef->ptr = (uint8_t*)pusrData;
   poolRef->bufCount = nbBuf;
   poolRef->len = (uint32_t)nbBuf*bufsize;
   poolRef->usrLen = nbBuf;

   pBufCur = pusrData;
   /*
   ** init of the payload pointers
   */
   while ((p = (ruc_buf_t*)ruc_objGetNext((ruc_obj_desc_t*)poolRef,&pnext))
               !=(ruc_buf_t*)NULL)
   {
      p->ptr = (uint8_t*)pBufCur;
      p->state = BUF_FREE;
      p->bufCount  = (uint16_t)bufsize;
      p->type = BUF_ELEM;
      p->callBackFct = (ruc_pf_buf_t)NULL;
#if 0
      /*
      ** call the init function associated with the buffer
      */
      if (init_fct != NULL) (*init_fct)(pBufCur);
#endif
      pBufCur += bufsize;
   }
   /*
   **  return the reference of the buffer pool
   */
  RUC_BUF_TRC("buf_poolCreate_out",poolRef,poolRef->ptr,poolRef->len,-1);
  // 64BITS return (uint32_t)poolRef;
  return poolRef;
}
/*__________________________________________________________________________
*/
/**
* display the configuration of the shared memories associated with the storcli
*/
void rozofs_shared_mem_display(char * argv[], uint32_t tcpRef, void *bufRef)
{
    char *pChar = uma_dbg_get_buffer();
    int i;


    pChar += sprintf(pChar, "   pool     |     key     |  size    | count |    address     |      stats     | nodes |\n");
    pChar += sprintf(pChar, "------------+-------------+----------+-------+----------------+----------------+-------+\n");
    for (i = 0; i < SHAREMEM_PER_FSMOUNT; i ++)
    {
      pChar +=sprintf(pChar," %10s | %8.8d  | %8.8d |%4d   | %p |%15llu |%4x |\n",(i==0)?"Read":"Write",
                      rozofs_storcli_shared_mem[i].key,
                      rozofs_storcli_shared_mem[i].buf_sz,
                      rozofs_storcli_shared_mem[i].buf_count,
                      rozofs_storcli_shared_mem[i].data_p,
                      (long long unsigned int)rozofs_storcli_shared_mem[i].read_stats,
		      rozofs_storcli_shared_mem[i].numa_node);    
    
    }
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());

}
/*
 *________________________________________________________
 */
 /**
 *  API to create the shared memory used for reading data from storcli's
 
   note: the instance of the shared memory is the concatenantion of the rozofsmount 
         instance and storcli instance: (rozofsmount<<1 | storcli_instance) 
         (assuming of max of 2 storclis per rozofsmount)
         
   @param : key_instance : instance of the shared memory (lower byte of the key)
   @param : pool_idx : instance of the shared memory (0: read/ 1:write)
   @param buf_nb: number of buffers
   @param buf_sz: size of the buffer payload
   
   @retval 0 on success
   @retval < 0 on error (see errno for details
 */
 
int rozofs_create_shared_memory(int key_instance,int pool_idx,uint32_t buf_nb, uint32_t buf_sz)
{
   key_t key = 0x524F5A00 | key_instance;

   rozofs_numa_mem_t membind_data;
   int numa_requested = 0;
   int i;
   int configured_nodes;
   
   rozofs_storcli_shared_mem[pool_idx].numa_node = -1;
   if (common_config.processor_model == common_config_processor_model_EPYC)
   {
     /*
     ** EPYC socket
     */
     if ((common_config.adaptor_numa_node >= 0) && (numa_available()>=0))
     {
       membind_data.nodemask = 0;
       membind_data.mode = MPOL_INTERLEAVE;

       configured_nodes = numa_num_configured_nodes(); 
       for (i = 0; i < configured_nodes;i++)
       {
           if (i == common_config.adaptor_numa_node) continue;
	   membind_data.nodemask |= (1 <<  i); 
       }
       membind_data.maxnode = 32;
       membind_data.flags = 0; //  MPOL_F_STATIC_NODES; 
       numa_requested = 1;
       rozofs_storcli_shared_mem[pool_idx].numa_node = membind_data.nodemask;
     } 
   }

   rozofs_storcli_shared_mem[pool_idx].key          = key;
   rozofs_storcli_shared_mem[pool_idx].buf_sz       = buf_sz;
   rozofs_storcli_shared_mem[pool_idx].buf_count    = buf_nb;
   rozofs_storcli_shared_mem[pool_idx].pool_p = ruc_buf_poolCreate_shared_numa(buf_nb,buf_sz,key,((numa_requested!=0)?&membind_data:NULL));
   if (rozofs_storcli_shared_mem[pool_idx].pool_p == NULL) return -1;
   switch(pool_idx) {
     case SHAREMEM_IDX_READ: ruc_buffer_debug_register_pool("read_pool_shared",  rozofs_storcli_shared_mem[pool_idx].pool_p); break;
     case SHAREMEM_IDX_WRITE: ruc_buffer_debug_register_pool("write_pool_shared",  rozofs_storcli_shared_mem[pool_idx].pool_p); break;
     default:
       break;
   }  
   rozofs_storcli_shared_mem[pool_idx].data_p = ruc_buf_get_pool_base_data(rozofs_storcli_shared_mem[pool_idx].pool_p);
   return 0;
  
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
  
  for (i = 0; i < SHAREMEM_PER_FSMOUNT; i++)
  {
    memset(&rozofs_storcli_shared_mem[i],0,sizeof(rozofs_shared_pool_t));
  
  }
}
