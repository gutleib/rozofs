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

/*
**   I N C L U D E  F I L E S
*/

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <errno.h>
#include <rozofs/common/log.h>

#include "ruc_common.h"
#include "ruc_buffer.h"
#include "ruc_list.h"
#include "ruc_buffer_api.h"
#include "ruc_trace_api.h"
#include "rozofs_share_memory.h"


uint32_t ruc_buffer_trace = FALSE;

/*
**  activate/de-activate the trace for the buffer services
**
**   TRUE = active /FALSE: inactive
*/

void ruc_buf_set_trace(uint32_t flag)
{
  ruc_buffer_trace = flag;
}


/*
**__________________________________________________________
*/
#define MB_1 (1024*1024)
#define MB_2 (2*MB_1)
void * ruc_buf_poolCreate_shared(uint32_t nbBuf, uint32_t bufsize, key_t key/*,ruc_pf_buf_t init_fct*/)
{
  ruc_buf_t  *poolRef;
  ruc_obj_desc_t  *pnext=(ruc_obj_desc_t*)NULL;
  char *pusrData;
  char *pBufCur;
  ruc_buf_t  *p;
  int shmid;

//   RUC_BUF_TRC("buf_poolCreate",nbBuf,bufsize,-1,-1);
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
     // 64BITS return (uint32_t)NULL;
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
      // 64BITS return (uint32_t)NULL;
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
      // 64BITS return (uint32_t)NULL;
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
     */    perror("shmat");
    RUC_WARNING(errno);
    ruc_listDelete_shared((ruc_obj_desc_t*)poolRef);
    return (ruc_obj_desc_t*)NULL;
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
      p->bufCount  = bufsize;
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
