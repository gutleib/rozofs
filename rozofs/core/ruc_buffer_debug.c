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
#include <errno.h>
#include <rozofs/core/ruc_buffer_debug.h>
#include <rozofs/common/log.h> 

#define RUC_BUFFER_DEBUG_2ND_ENTRIES_NB   16
#define RUC_BUFFER_DEBUG_1RST_ENTRIES_NB  16


typedef struct _ruc_registered_buffer_pool_t {
  char *      name;
  ruc_buf_t * poolRef;
} ruc_registered_buffer_pool_t;

ruc_registered_buffer_pool_t ** ruc_registered_buffer_pool = NULL;
static int                      ruc_registered_buffer_pool_entries=0;


/*
**__________________________________________________________
* Format debug information about a buffer pool
* @param poolRef      Reference of the buffer pool
* @param displayName  Name of the buffer pool (for display)
* @param p            Where to format the output 
*/
static inline char * ruc_buf_poolDisplay(ruc_buf_t* poolRef, char * displayName, char * p)
{
  p += sprintf(p, "%20s - user data addr/len %16p /%9d - nb buff %3d/%3d size %6d\n",displayName,
               poolRef->ptr, poolRef->len,
               poolRef->usrLen, poolRef->bufCount, poolRef->len/poolRef->bufCount);
  return p;	       
}

/*
**__________________________________________________________
* Format debug information about a buffer pool
* @param poolRef      Reference of the buffer pool
* @param p            Where to format the output 
*/
static inline char * ruc_buf_poolContentDisplay(ruc_buf_t* poolRef, char * p)
{
  int    i;
  ruc_buf_t* pBuf = (ruc_buf_t*)poolRef;
  pBuf++;
  
  for (i=0; i< poolRef->bufCount; i++,pBuf++) {  
    p += sprintf(p, "%3d %s payload %16p len %6d/%6d in use %d\n", 
                 i,
                (pBuf->state == BUF_FREE)?"free":"busy", 
		 pBuf->ptr, pBuf->usrLen, pBuf->bufCount, pBuf->inuse);
  }		 
  return p;	       
}
/*
**__________________________________________________________
* Format debug information about a buffer pool
* @param poolRef      Reference of the buffer pool
* @param buffIdx      Buffer index
* @param p            Where to format the output 
*/
#define MAX_LEN  (((UMA_DBG_MAX_SEND_SIZE-1024)/80)*16)
static inline char * ruc_buf_bufferContentDisplay(ruc_buf_t* poolRef, int buffIdx, char * p)
{
  int    size;
  ruc_buf_t* pBuf = (ruc_buf_t*)poolRef;
  pBuf += (1+buffIdx),

  p += sprintf(p, "%3d %s payload %16p len %d/%d in use %d\n", 
                 buffIdx,
                (pBuf->state == BUF_FREE)?"free":"busy", 
		 pBuf->ptr, pBuf->usrLen, pBuf->bufCount, pBuf->inuse);
  size = pBuf->usrLen;
  if (size > MAX_LEN) size = MAX_LEN;
  p = uma_dbg_hexdump(pBuf->ptr, size, p);  	 
  return p;	       
}
/*
**__________________________________________________________
* Retrieve a registered buffer pool from its name
*
* @param name    The name of the buffer pool
* @retval        The index of the buffer pool in thee table or -1
*
*/
static inline ruc_buf_t * ruc_buffer_debug_get_pool_from_name(char * name) {
  int idx1; 
  int idx2;
  
  if (ruc_registered_buffer_pool == NULL) return NULL;
  
  for (idx1=0; idx1<RUC_BUFFER_DEBUG_1RST_ENTRIES_NB; idx1++) {
  
    if (ruc_registered_buffer_pool[idx1] == NULL) return NULL;
      
    for (idx2=0; idx2<RUC_BUFFER_DEBUG_2ND_ENTRIES_NB; idx2++) {
      if (ruc_registered_buffer_pool[idx1][idx2].name == NULL) {   
        return NULL;
      }	
      if (strcmp(ruc_registered_buffer_pool[idx1][idx2].name,name)==0)  {   
        return ruc_registered_buffer_pool[idx1][idx2].poolRef;
      }	
    }
  }
  return NULL;
}
/*
**__________________________________________________________
* Format debug information about a buffer pool
*/
static char * show_ruc_buffer_debug_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"buffer                      : display list of buffer pools\n");
  pChar += sprintf(pChar,"buffer <PoolName>           : display buffer list of a buffer pool\n");
  pChar += sprintf(pChar,"buffer <PoolName> <buffIdx> : display buffer content of a buffer pool\n");
  return pChar; 
}
void show_ruc_buffer_debug_man(char * pt) {
  pt += sprintf(pt , "Display some buffer pool information\n");
  show_ruc_buffer_debug_help(pt);
}
void show_ruc_buffer_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
  ruc_buf_t   * poolRef; 
  char        * pChar = uma_dbg_get_buffer();
  int           idx1; 
  int           idx2;  
  
  if (ruc_registered_buffer_pool == NULL) {
    uma_dbg_send(tcpRef, bufRef, TRUE, "Service not initialized\n");
    return;    
  }
  
  if (argv[1] != 0) {
  
    poolRef = ruc_buffer_debug_get_pool_from_name(argv[1]); 
    if (poolRef == NULL) {
      pChar += sprintf(pChar, "No such buffer pool name \"%s\"\n",argv[1]);
      pChar = show_ruc_buffer_debug_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());  
      return;    
    }     
 
    if (argv[2] != 0) {
      errno = 0;
      int buffIdx = (int) strtol(argv[2], (char **) NULL, 10);   
      if (errno != 0) {
	pChar += sprintf(pChar, "bad buffer index \"%s\" \n",argv[2]); 
	pChar = show_ruc_buffer_debug_help(pChar);
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());  
	return;   
      }  
      if ((buffIdx<0) || (buffIdx >= poolRef->bufCount)) {
	pChar += sprintf(pChar, "buffer index out of range (%d). Should be within [0..%d[\n",buffIdx,poolRef->bufCount);  
	pChar = show_ruc_buffer_debug_help(pChar);
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());  
	return;          
      }  
      pChar = ruc_buf_poolDisplay(poolRef,argv[1], pChar);
      pChar = ruc_buf_bufferContentDisplay(poolRef,buffIdx,pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
      return;
    }     
    
    pChar = ruc_buf_poolDisplay(poolRef,argv[1], pChar);
    pChar = ruc_buf_poolContentDisplay(poolRef,pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
    return;
  }

  
  for (idx1=0; idx1<RUC_BUFFER_DEBUG_1RST_ENTRIES_NB; idx1++) {
  
    if (ruc_registered_buffer_pool[idx1] == NULL) break;
      
    for (idx2=0; idx2<RUC_BUFFER_DEBUG_2ND_ENTRIES_NB; idx2++) {
      if (ruc_registered_buffer_pool[idx1][idx2].name == NULL) {   
        break;
      }	
      pChar = ruc_buf_poolDisplay(ruc_registered_buffer_pool[idx1][idx2].poolRef,ruc_registered_buffer_pool[idx1][idx2].name, pChar);
    }
  }	 
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
  return;
}

/*
**__________________________________________________________
* Register a new buffer pool to this debug service
*
* @param name    The name of the buffer pool
* @param poolRef Reference of the registered pool
*
*/
void ruc_buffer_debug_register_pool(char * name , void * poolRef) {
  int idx1;
  int idx2;
  int size;
  ruc_registered_buffer_pool_t * tbl2;
  
  /*
  ** Allocate 1rst level point table to arrays of registered buffer pool 
  */
  if (ruc_registered_buffer_pool_entries == 0) {
  
    size = sizeof(void *) * RUC_BUFFER_DEBUG_1RST_ENTRIES_NB;
    ruc_registered_buffer_pool = malloc(size);
    if (ruc_registered_buffer_pool == NULL) {
      severe("ruc_buffer_debug_register_pool out of memory %d",size);
      return;
    }
    
    memset(ruc_registered_buffer_pool,0,size);    
    uma_dbg_addTopicAndMan("buffer",  show_ruc_buffer_debug, show_ruc_buffer_debug_man, 0);      
  }
  
  idx1 = ruc_registered_buffer_pool_entries / RUC_BUFFER_DEBUG_2ND_ENTRIES_NB;
  idx2 = ruc_registered_buffer_pool_entries % RUC_BUFFER_DEBUG_2ND_ENTRIES_NB;
  tbl2 = ruc_registered_buffer_pool[idx1];
  
  /*
  ** Allocate 2nd level arrays of registered buffer pool 
  */
  if (tbl2 == 0) {
  
    size = sizeof(ruc_registered_buffer_pool_t) * RUC_BUFFER_DEBUG_2ND_ENTRIES_NB;
    tbl2 = malloc(size);
    if (tbl2 == NULL) {
      severe("ruc_buffer_debug_register_pool out of memory %d",size);
      return;
    }
    
    memset(tbl2,0,size);
    ruc_registered_buffer_pool[idx1] = tbl2;
  }  

  /*
  ** Save information in table
  */
  tbl2[idx2].name    = strdup(name);
  tbl2[idx2].poolRef = (ruc_buf_t *) poolRef;
  ruc_registered_buffer_pool_entries++;
  return;
}  


