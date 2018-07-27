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
 
#ifndef RUC_BUFFER_API_H
#define RUC_BUFFER_API_H

#include <stdint.h>
#include <stdlib.h>
#include  <sys/mman.h>

#include <rozofs/common/types.h>
#include <rozofs/common/log.h>

#include "ruc_common.h"
#include "ruc_buffer.h"
#include "ruc_list.h"
#include "ruc_buffer_api.h"
#include "ruc_trace_api.h"

#define ROZOFS_HUGE_PAGE_SIZE  (2UL*1024*1024)

#define RUC_BUF_TRC(name,p1,p2,p3,p4) { if (ruc_buffer_trace==TRUE) \
                                        ruc_trace(name,(uint64_t)(long)p1,(uint64_t)(long)p2,(uint64_t)(long)p3,(uint64_t)(long)p4); }

extern uint32_t ruc_buffer_trace ;


// 64BITS typedef void (*ruc_pf_buf_t)(uint32);
//typedef void (*ruc_pf_buf_t)(void *);

void * ruc_buf_poolCreate_shared(uint32_t nbBuf,uint32_t bufsize,key_t key);

// 64BITS uint32_t ruc_buf_poolCreate(uint32_t nbBuf,uint32_t bufsize);
static inline void * ruc_buf_poolCreate(uint32_t nbBuf,uint32_t bufsize);
// 64BITS uint32_t ruc_buf_poolDelete(uint32_t poolRef);
static inline uint32_t ruc_buf_poolDelete(void * poolRef);
// 64BITS uint32_t ruc_buf_isPoolEmpty(uint32_t poolRef);
static inline uint32_t ruc_buf_isPoolEmpty(void * poolRef);
// 64BITS uint32_t ruc_buf_getBuffer(uint32_t poolRef);
static inline void * ruc_buf_getBuffer(void * poolRef);
// 64BITS uint32_t ruc_buf_freeBuffer(uint32_t bufRef);
static inline uint32_t ruc_buf_freeBuffer_internal(char * file, int line, void * bufRef);

// 64BITS char * ruc_buf_getPayload(uint32_t bufRef);
static inline char * ruc_buf_getPayload_internal(char * file, int line, void * bufRef);
// 64BITS uint32_t ruc_buf_getPayloadLen(uint32_t bufRef);
static inline uint32_t ruc_buf_getPayloadLen(void * bufRef);
// 64BITS uint32_t ruc_buf_setPayloadLen(uint32_t bufRef,uint32_t len);
static inline uint32_t ruc_buf_setPayloadLen_internal(char * file, int line, void * bufRef,uint32_t len);
// 64BITS uint32_t ruc_buf_poolRecover(uint32_t poolRef);
static inline uint32_t ruc_buf_poolRecover(void * poolRef);
// 64BITS uint32_t ruc_buf_getFreeBufferCount(uint32_t poolRef);
static inline uint32_t ruc_buf_getFreeBufferCount(void * poolRef);
// 64BITS uint32_t ruc_buf_getInitBufferCount(uint32_t poolRef);
static inline uint32_t ruc_buf_getInitBufferCount(void * poolRef);
// 64BITS uint32_t ruc_buf_setCallBack(uint32_t bufRef,ruc_pf_buf_t pfct, uint32_t par);
static inline uint32_t ruc_buf_setCallBack(void * bufRef,ruc_pf_buf_t pfct, void * par);



// 64BITS uint32_t ruc_buf_poolRecover(uint32_t poolRef)
static inline uint32_t ruc_buf_poolRecover(void * poolRef)
{
  ruc_buf_t *phead;
  ruc_obj_desc_t  *pnext=(ruc_obj_desc_t*)NULL;
  ruc_buf_t  *p;
  uint32_t ret;


  phead = (ruc_buf_t*)poolRef;

  RUC_BUF_TRC("buf_poolRecover",poolRef,phead->ptr,-1,-1);

  if (phead->type != BUF_POOL_HEAD)
  {
    /*
    **  not a buffer pool reference
    */
    RUC_WARNING(phead->type);
    return RUC_NOK;
  }
  ret = ruc_listRecover((ruc_obj_desc_t *)phead,TRUE);
  if (ret == RUC_NOK)
  {
    /*
    **  unable to recover the buffer pool
    */
    RUC_WARNING(phead);
    return RUC_NOK;
  }
  /*
  ** change the buffer state to BUF_FREE
  */
   while ((p = (ruc_buf_t*)ruc_objGetNext((ruc_obj_desc_t*)phead,&pnext))
               !=(ruc_buf_t*)NULL) 
   {
      p->state = BUF_FREE;
   }
   return RUC_OK;

}


/*
**    note : bufsize MUST be long word aligned
*/

// 64BITS uint32_t ruc_buf_poolCreate(uint32_t nbBuf,uint32_t bufsize)
static inline void * ruc_buf_poolCreate(uint32_t nbBuf,uint32_t bufsize)
{
  ruc_buf_t  *poolRef;
  ruc_obj_desc_t  *pnext=(ruc_obj_desc_t*)NULL;
  char *pusrData;
  char *pBufCur;
  ruc_buf_t  *p;
  int huge_page = 0;

  
   RUC_BUF_TRC("buf_poolCreate",nbBuf,bufsize,-1,-1);
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
     // 64BITS return (uint32)NULL;
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
      // 64BITS return (uint32)NULL;
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
      // 64BITS return (uint32)NULL;
      return NULL;
    }
  }
  /*
  ** check for hugepage
  */
  size_t alloc_size = bufsize*nbBuf;
#if 1
  if (alloc_size > ROZOFS_HUGE_PAGE_SIZE)
  {
     int count = alloc_size/ROZOFS_HUGE_PAGE_SIZE;
     if (alloc_size%ROZOFS_HUGE_PAGE_SIZE)
     {
       count+=1;
     }
     alloc_size = ROZOFS_HUGE_PAGE_SIZE*count;
     huge_page = 1;  
  }
#endif
   if (posix_memalign((void**)&pusrData,4096,alloc_size))
   {
     /*
     **  out of memory, free the pool
     */
     RUC_WARNING(-1);
     ruc_listDelete((ruc_obj_desc_t*)poolRef);
     return NULL;
   }
    if (huge_page) {
#ifdef MADV_HUGEPAGE
        madvise(pusrData, alloc_size, MADV_HUGEPAGE);
#endif
    }
    memset(pusrData,0,alloc_size);
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
      p->retry_counter     = 0;
      p->opaque_ref = NULL;
      p->type = BUF_ELEM;
      p->callBackFct = (ruc_pf_buf_t)NULL;
      pBufCur += bufsize;
   }
   /*
   **  return the reference of the buffer pool
   */
  RUC_BUF_TRC("buf_poolCreate_out",poolRef,poolRef->ptr,poolRef->len,-1);
  return poolRef;
}



// 64BITS uint32_t ruc_buf_poolDelete(uint32_t poolRef)
static inline uint32_t ruc_buf_poolDelete(void * poolRef)
{
  ruc_buf_t *p;

  p = (ruc_buf_t*)poolRef;

  RUC_BUF_TRC("buf_poolDelete",poolRef,p->ptr,-1,-1);

  if (p->type != BUF_POOL_HEAD)
  {
    /*
    **  not a buffer pool reference
    */
    RUC_WARNING(p->type);
    return RUC_NOK;
   }
   /*
   **  free the usrData part of the pool
   */
   free ((char*)p->ptr);
   /*
   **  free the buffer list
   */
   ruc_listDelete((ruc_obj_desc_t*)p);
   return RUC_OK;
}

// 64BITS uint32_t ruc_buf_isPoolEmpty(uint32_t poolRef)
static inline uint32_t ruc_buf_isPoolEmpty(void * poolRef)
{
  ruc_buf_t *p;

  p = (ruc_buf_t*)poolRef;

  if (p->type != BUF_POOL_HEAD)
  {
    /*
    **  not a buffer pool reference
    */
    RUC_WARNING(p->type);
    return TRUE;
  }
  return (ruc_objIsEmptyList((ruc_obj_desc_t*)p));
}


// 64BITS uint32_t ruc_buf_isPoolEmpty(uint32_t poolRef)
/**
*  that service returns the pointer to the beginning of the user data array 

  @param poolRef : pointer to the pool 
  
  @retval NULL if it is not a pool
  @retval <>NULL : pointer to the first byte of the user data array
*/
static inline void *ruc_buf_get_pool_base_data(void * poolRef)
{
  ruc_buf_t *p;

  p = (ruc_buf_t*)poolRef;

  if (p->type != BUF_POOL_HEAD)
  {
    /*
    **  not a buffer pool reference
    */
    RUC_WARNING(p->type);
    return NULL;
  }
  return p->ptr;
}

/**
*  that service returns the pointer to the beginning of the user data array and the total length

  @param poolRef : pointer to the pool 
  @param length_p: not NULL if length is reuquested
  
  @retval NULL if it is not a pool
  @retval <>NULL : pointer to the first byte of the user data array
*/
static inline void *ruc_buf_get_pool_base_and_length(void * poolRef,uint64_t *length_p)
{
  ruc_buf_t *p;

  p = (ruc_buf_t*)poolRef;

  if (p->type != BUF_POOL_HEAD)
  {
    /*
    **  not a buffer pool reference
    */
    RUC_WARNING(p->type);
    return NULL;
  }
  if (length_p != NULL) *length_p = (uint64_t)p->len;
  return p->ptr;
}


// 64BITS uint32_t ruc_buf_getBuffer(uint32_t poolRef)
static inline void * ruc_buf_getBuffer(void * poolRef)
{
  ruc_buf_t *p,*pelem;

  p = (ruc_buf_t*)poolRef;
  pelem = (ruc_buf_t*)ruc_objGetFirst((ruc_obj_desc_t*)p);
  if (pelem == (ruc_buf_t* )NULL)
  {
    RUC_BUF_TRC("buf_getBuffer_NULL",poolRef,-1,-1,-1);
    // 64BITS return (uint32) NULL;
    return NULL;
  }
  /*
  **  remove the buffer from the list
  */
  ruc_objRemove((ruc_obj_desc_t*)pelem);
  pelem->state = BUF_ALLOC;
  /*
  ** set the current payload to 0
  */
  pelem->usrLen     = 0;
  pelem->retry_counter      = 0;
  pelem->opaque_ref  = NULL;
  pelem->inuse      = 1;
  /*
  **update the current number of buffer in the buffer pool
  */
   p->usrLen--;
  /*
  ** clear the callback (normally this is done on delete
  */
  p->callBackFct = (ruc_pf_buf_t)NULL;
  // 64BITS p->callBackParam = 0;
  p->callBackParam = NULL;

  RUC_BUF_TRC("buf_getBuffer",poolRef,pelem->ptr,pelem,-1);
  // 64BITS return ((uint32)pelem);
  return (pelem);
}





// 64BITS uint32_t ruc_buf_freeBuffer(uint32_t bufRef)
#define ruc_buf_freeBuffer(bufRef) ruc_buf_freeBuffer_internal(__FILE__,__LINE__,bufRef)
static inline uint32_t ruc_buf_freeBuffer_internal(char * file, int line, void * bufRef)
{

  ruc_buf_t *phead;
  ruc_buf_t *pelem;
 
  RUC_BUF_TRC("buf_freeBuffer",bufRef,-1,-1,-1);
  pelem = (ruc_buf_t*)bufRef;
  /*
  **  check if it is a buffer element
  */
  if (pelem->type != BUF_ELEM)
  {
    warning("%s:%d %p is not a buffer %d", file, line, pelem, pelem->type);
    return RUC_NOK;
  }
  /*
  ** get the reference of the pool
  ** from buffer
  */
  phead = (ruc_buf_t*)ruc_objGetHead((ruc_obj_desc_t*)pelem);
  if (phead == (ruc_buf_t*)NULL)
  {
    /*
    **  not a buffer pool reference
    */
    warning("%s:%d %p has no head of list %p", file, line, pelem, pelem->obj.sysRef);
    return RUC_NOK;
  }
  /*
  ** check the inuse counter
  */
  if (pelem->inuse > 1)
  {
    /*
    ** do not release the buffer some other application
    ** still use it
    */
//#warning set a syslog on ruc_buf_freeBuffer for inuse > 1
    warning("%s:%d %p is in use %d", file, line, pelem, pelem->inuse);
    return  RUC_OK;
  
  }
  pelem->inuse  = 0;
  /*
  **  check if the buffer has be already released
  */
  if (pelem->state == BUF_FREE)
  {
    /*
    ** reject the service
    */
    warning("%s:%d %p is already free", file, line, pelem);
    return RUC_NOK;
  }
  /*
  ** set buffer state to FREE
  */
  pelem->state = BUF_FREE;

  /*
  **  test if there is a callback function attached to
  **  the buffer. In that case call it before putting
  **  back the buffer in the free list
  */
  if (pelem->callBackFct != (ruc_pf_buf_t)NULL)
  {
     (*pelem->callBackFct)(pelem->callBackParam);
  }

//  ruc_objInsertTail((ruc_obj_desc_t*)phead,(ruc_obj_desc_t*)pelem);
  ruc_objInsert((ruc_obj_desc_t*)phead,(ruc_obj_desc_t*)pelem);

  /*
  **update the current number of buffer in the buffer pool
  */
   phead->usrLen++;

  return RUC_OK;
}

// 64BITS char * ruc_buf_getPayload(uint32_t bufRef)
#define ruc_buf_getPayload(bufRef) ruc_buf_getPayload_internal(__FILE__,__LINE__,bufRef)
static inline char * ruc_buf_getPayload_internal(char * file, int line, void * bufRef)
{
  ruc_buf_t *pelem;

  pelem = (ruc_buf_t*)bufRef;
#ifndef NO_PARANOIA
  if (pelem->type != BUF_ELEM)
  {
    /*
    ** unlucky guy!!
    */
    warning("%s:%d %p is not a buffer %d", file, line, pelem, pelem->type);
    return (char*)NULL;
  }
  if (pelem->state != BUF_ALLOC)
  {
    /*
    ** unlucky guy!!
    */
    warning("%s:%d %p is not allocated %d", file, line, pelem, pelem->state);
    return (char*)NULL;
  }
#endif
  return (char*)pelem->ptr;
}
static inline int ruc_buf_checkBuffer(void * bufRef)
{
  ruc_buf_t *pelem = (ruc_buf_t*)bufRef;
  return (pelem->type == BUF_ELEM); 
}
    


// 64BITS uint32_t ruc_buf_getPayloadLen(uint32_t bufRef)
static inline uint32_t ruc_buf_getPayloadLen(void * bufRef)
{
  ruc_buf_t *pelem;

  pelem = (ruc_buf_t*)bufRef;
#ifndef NO_PARANOIA

  if (pelem->type != BUF_ELEM)
  {
    /*
    ** unlucky guy!!
    */
    RUC_WARNING(bufRef);
    return 0;
  }
  if (pelem->state != BUF_ALLOC)
  {
    /*
    ** unlucky guy!!
    */
    RUC_WARNING(bufRef);
    return 0;
  }
#endif
  return pelem->usrLen;
}

/*
**__________________________________________________________________________
*/

static inline uint32_t ruc_buf_getMaxPayloadLen(void * bufRef)
{
  ruc_buf_t *pelem;

  pelem = (ruc_buf_t*)bufRef;
#ifndef NO_PARANOIA

  if (pelem->type != BUF_ELEM)
  {
    /*
    ** unlucky guy!!
    */
    RUC_WARNING(bufRef);
    return 0;
  }
#endif
  /*
  **
  */
  return(pelem->bufCount);
}


/*
**__________________________________________________________________________
*/
// 64BITS uint32_t ruc_buf_setPayloadLen(uint32_t bufRef,uint32_t len)
#define ruc_buf_setPayloadLen(bufRef, len) ruc_buf_setPayloadLen_internal(__FILE__,__LINE__,bufRef,len)
static inline uint32_t ruc_buf_setPayloadLen_internal(char * file, int line, void * bufRef,uint32_t len)
{
  ruc_buf_t *pelem;

  pelem = (ruc_buf_t*)bufRef;
#ifndef NO_PARANOIA

  if (pelem->type != BUF_ELEM)
  {
    /*
    ** unlucky guy!!
    */
    warning("%s:%d %p is not a buffer %d", file, line, pelem, pelem->type);
    return RUC_NOK;
  }
  if (pelem->state != BUF_ALLOC)
  {
    /*
    ** unlucky guy!!
    */
    warning("%s:%d %p is not allocated %d", file, line, pelem, pelem->state);
    return RUC_NOK;
  }
#endif
  pelem->usrLen = len;
  return RUC_OK;
}

/*
**   That function permits to associate a call back function
**   that will be called when the buffer is released.
**
**    INPUT:
**          buffer reference
**          call back function
**          input parameter of the callback function.
*/


// 64BITS uint32_t ruc_buf_setCallBack(uint32_t bufRef,ruc_pf_buf_t pfct, uint32_t par)
static inline uint32_t ruc_buf_setCallBack(void * bufRef,ruc_pf_buf_t pfct, void * par)
{
  ruc_buf_t *pelem;

  pelem = (ruc_buf_t*)bufRef;
#ifndef NO_PARANOIA

  if (pelem->type != BUF_ELEM)
  {
    /*
    ** unlucky guy!!
    */
    RUC_WARNING(bufRef);
    return RUC_NOK;
  }
  if (pelem->state != BUF_ALLOC)
  {
    /*
    ** unlucky guy!!
    */
    RUC_WARNING(bufRef);
    return RUC_NOK;
  }
#endif
  pelem->callBackFct = pfct;
  pelem->callBackParam = par;

  return RUC_OK;
}
/*
**__________________________________________________________________________
*/

// 64BITS uint32_t ruc_buf_getFreeBufferCount(uint32_t poolRef)
static inline uint32_t ruc_buf_getFreeBufferCount(void * poolRef)
{
  ruc_buf_t *p;

  p = (ruc_buf_t*)poolRef;
#ifndef NO_PARANOIA

  if (p->type != BUF_POOL_HEAD)
  {
    /*
    **  not a buffer pool reference
    */
    RUC_WARNING(p->type);
    return 0;
  }
#endif
  return(p->usrLen);
}
/*
**__________________________________________________________________________
*/

// 64BITS uint32_t ruc_buf_getInitBufferCount(uint32_t poolRef)
static inline uint32_t ruc_buf_getInitBufferCount(void * poolRef)
{
  ruc_buf_t *p;

  p = (ruc_buf_t*)poolRef;

  if (p->type != BUF_POOL_HEAD)
  {
    /*
    **  not a buffer pool reference
    */
    RUC_WARNING(p->type);
    return 0;
  }
  /*
  **
  */
  return(p->bufCount);
}
/*
**__________________________________________________________________________
*/
/**
*  API to get the pointer to the destination user information
 That array is intended to contain either the pathname of an AF_UNIX
 socket or the IP address and port of an AF_INET socket

 note : RUC_BUFFER_USER_ARRAY_SIZE is the max size of the array

 @param bufRef: pointer to the ruc_buffer structure
 
 @retval <>NULL pointer to the user Destination info
 @retval NULL bad buffer reference
*/
static inline void * ruc_buf_get_usrDestInfo(void * bufRef)
{
  ruc_buf_t *pelem;

  pelem = (ruc_buf_t*)bufRef;
  if (pelem->type != BUF_ELEM)
  {
    /*
    ** unlucky guy!!
    */
    RUC_WARNING(bufRef);
    return (void*)NULL;
  }
  if (pelem->state != BUF_ALLOC)
  {
    /*
    ** unlucky guy!!
    */
    RUC_WARNING(bufRef);
    return (void*)NULL;
  }

  return (void*)pelem->usrDestInfo;
}
  

/*
**__________________________________________________________________________
*/
/**
*  API to get the pointer to the source user information
 That array is intended to contain either the pathname of an AF_UNIX
 socket or the IP address and port of an AF_INET socket
 
 note : RUC_BUFFER_USER_ARRAY_SIZE is the max size of the array
 
 @param bufRef: pointer to the ruc_buffer structure
 
 @retval <>NULL pointer to the user Destination info
 @retval NULL bad buffer reference
*/
static inline void * ruc_buf_get_usrSrcInfo(void * bufRef)
{
  ruc_buf_t *pelem;

  pelem = (ruc_buf_t*)bufRef;
  if (pelem->type != BUF_ELEM)
  {
    /*
    ** unlucky guy!!
    */
    RUC_WARNING(bufRef);
    return (void*)NULL;
  }
  if (pelem->state != BUF_ALLOC)
  {
    /*
    ** unlucky guy!!
    */
    RUC_WARNING(bufRef);
    return (void*)NULL;
  }

  return (void*)pelem->usrSrcInfo;
}
  



/*
**__________________________________________________________________________
*/
/**
  Get the retry counter associated with a packet buffer
 @param bufRef: pointer to the ruc_buffer structure
 
 @retval counter value of the retry counter
*/
static inline uint8_t ruc_buf_get_retryCounter(void * bufRef)
{
  ruc_buf_t *pelem;

  pelem = (ruc_buf_t*)bufRef;
  return pelem->retry_counter;
}
/*
**__________________________________________________________________________
*/
/**
  set the retry counter associated with a packet buffer
 @param bufRef: pointer to the ruc_buffer structure
 @param retry_counter: retry counter value
 
 @retval none
*/
static inline void ruc_buf_set_retryCounter(void * bufRef,uint8_t retry_counter)
{
  ruc_buf_t *pelem;

  pelem = (ruc_buf_t*)bufRef;
  pelem->retry_counter = retry_counter;
}


/*
**__________________________________________________________________________
*/
/**
  Get the opaque user reference associated with a packet buffer
 @param bufRef: pointer to the ruc_buffer structure
 
 @retval the opaque reference
*/
static inline void * ruc_buf_get_opaque_ref(void * bufRef)
{
  ruc_buf_t *pelem;

  pelem = (ruc_buf_t*)bufRef;
  return pelem->opaque_ref;
}
/*
**__________________________________________________________________________
*/
/**
  set the opaque user reference associated with a packet buffer
 @param bufRef: pointer to the ruc_buffer structure
 @param opaque_ref: the opaque user reference
 
 @retval none
*/
static inline void ruc_buf_set_opaque_ref(void * bufRef,void * opaque_ref)
{
  ruc_buf_t *pelem;

  pelem = (ruc_buf_t*)bufRef;
  pelem->opaque_ref = opaque_ref;
}


/**
*   increment the inuse counter of a ruc buffer
  @param bufRef : pointer to the buffer
  
  retval > 0: current inuse value
  retval -1 error
*/
#define ruc_buf_inuse_increment(bufRef) ruc_buf_inuse_increment_internal(__FILE__,__LINE__,bufRef)
static inline int ruc_buf_inuse_increment_internal(char * file, int line, void * bufRef)
{

  ruc_buf_t *pelem;
 
  pelem = (ruc_buf_t*)bufRef;
#ifndef NO_PARANOIA

  /*
  **  check if it is a buffer element
  */
  if (pelem->type != BUF_ELEM)
  {
    severe("%s:%d %p is not a buffer %d", file, line, pelem, pelem->type);
    fatal("Not a buffer");
    return -1;
  }
  /*
  **  check if the buffer has be already released
  */
  if (pelem->state == BUF_FREE)
  {
    /*
    ** reject the service
    */
    severe("%s:%d %p is not free %d", file, line, pelem, pelem->state);
    fatal("Not a free buffer");
    return -1;
  }  
#endif
  pelem->inuse++;
  return (pelem->inuse);
}

/**
*   decrement the inuse counter of a ruc buffer
  @param bufRef : pointer to the buffer
  
  retval >= 0: current inuse value
  retval -1 error
*/
#define ruc_buf_inuse_decrement(bufRef) ruc_buf_inuse_decrement_internal(__FILE__,__LINE__,bufRef)
static inline uint32_t ruc_buf_inuse_decrement_internal(char * file, int line, void * bufRef)
{

  ruc_buf_t *pelem;
 
  pelem = (ruc_buf_t*)bufRef;
#ifndef NO_PARANOIA

  /*
  **  check if it is a buffer element
  */
  if (pelem->type != BUF_ELEM)
  {
    warning("%s:%d %p is not a buffer %d", file, line, pelem, pelem->type);
    return -1;
  }
  /*
  **  check if the buffer has be already released
  */
  if (pelem->state == BUF_FREE)
  {
    /*
    ** reject the service
    */
    warning("%s:%d %p is not allocated %d", file, line, pelem, pelem->state);
    return -1;
  }  
#endif
  if (pelem->inuse <= 1)
  {
    warning("%s:%d %p in use %d", file, line, pelem, pelem->inuse);
    for(;;);
    return -1;  
  
  }
  pelem->inuse--;
  return pelem->inuse;

}

/**
*   decrement the inuse counter of a ruc buffer
  @param bufRef : pointer to the buffer
  
  retval >= 0: current inuse value
  retval -1 error
*/
static inline int ruc_buf_inuse_get(void * bufRef)
{

  ruc_buf_t *pelem;
 
  pelem = (ruc_buf_t*)bufRef;
  return pelem->inuse;

}



/*
**__________________________________________________________________________
*/
/**
  Get the traffic shaping sub-context
 @param bufRef: pointer to the ruc_buffer structure
 
 @retval the traffic shaping subcontext address
*/
static inline ruc_buf_shaping_ctx_t * ruc_buffer_get_shaping_ctx(void * bufRef)
{
  ruc_buf_t *pelem;

  pelem = (ruc_buf_t*)bufRef;
  return &pelem->shaping_ctx;
}
#endif
