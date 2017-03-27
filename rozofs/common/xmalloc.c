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
#include <string.h>
#include <stdlib.h>


#include "xmalloc.h"
#include "log.h"
#include <malloc.h>
#include <rozofs/core/uma_dbg_api.h>


xmalloc_stats_t *xmalloc_size_table_p = NULL;
uint32_t         xmalloc_entries = 0;
static int       traced_idx=-1;
/*__________________________________________________________________________
*/
void man_xmalloc(char * pChar) {
  pChar += rozofs_string_append(pChar,"Track memory allocated using xmalloc()/xfree() services.\n");
  pChar += rozofs_string_append(pChar,"Each line displays an allocation size along with its allocation count.\n");
}
/*__________________________________________________________________________
*/
void show_xmalloc(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int i;
    xmalloc_stats_t *p = xmalloc_size_table_p;
    uint64_t         totalSize = 0;

    if (xmalloc_size_table_p == NULL) {
        pChar += rozofs_string_append(pChar, "xmalloc stats not available\n");
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
        return;
    }
    
    if (argv[1] != NULL) {
       if (strcmp(argv[1],"trace") !=0 ) goto syntax;
       if (argv[2] == NULL) traced_idx = -1;
       else if (sscanf(argv[2],"%d",&traced_idx)!=1) goto syntax;
    }

    for (i = 0; i < xmalloc_entries; i++,p++) {
        if (i==traced_idx) pChar += sprintf(pChar, "->%2d) ",i);
	else               pChar += sprintf(pChar, "  %2d) ",i);
        pChar += sprintf(pChar, "size %8.8u count %10.10llu = %llu\n", 
	                 p->size, 
			 (long long unsigned int) p->count,
	                 (long long unsigned int)(p->size * (long long unsigned int)p->count));
        totalSize += (p->size * (long long unsigned int)p->count);            
    }    
    pChar += sprintf(pChar, "Total size %8llu\n", (long long unsigned int)totalSize); 
    
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
    return;
    
syntax:
    pChar += sprintf(pChar,"xmalloc [ trace [<traceIdx>] ]\n");       
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
    return;
}
/*__________________________________________________________________________
*/
void *xmalloc_internal(char * file, int line, size_t n) {
    void *p = 0;
    if (xmalloc_size_table_p == NULL)
    {
      xmalloc_size_table_p = memalign(32,sizeof(xmalloc_stats_t)*XMALLOC_MAX_SIZE);
      memset(xmalloc_size_table_p,0,sizeof(xmalloc_stats_t)*XMALLOC_MAX_SIZE);   
      uma_dbg_addTopicAndMan("xmalloc", show_xmalloc,man_xmalloc,0); 
    }

    p = memalign(32,n);
    check_memory(p);

    if (xmalloc_stats_insert(malloc_usable_size(p))==traced_idx) {
      info("%s:%d xmalloc(%d) -> %p",file,line,(int)n,p);
    }
    return p;
}
/*__________________________________________________________________________
*/
void *xstrdup_internal(char * file, int line, size_t n, const char * src) {
    void *p = 0;
    if (xmalloc_size_table_p == NULL)
    {
      xmalloc_size_table_p = memalign(32,sizeof(xmalloc_stats_t)*XMALLOC_MAX_SIZE);
      memset(xmalloc_size_table_p,0,sizeof(xmalloc_stats_t)*XMALLOC_MAX_SIZE);   
      uma_dbg_addTopicAndMan("xmalloc", show_xmalloc,man_xmalloc,0); 
    }

    p = memalign(32,n+1);
    check_memory(p);

    if (xmalloc_stats_insert(malloc_usable_size(p))==traced_idx) {
      info("%s:%d xstrdup(%d) -> %p",file,line,(int)n+1,p);
    }
    
    memcpy(p,src,n);
    ((char*)p)[n] = 0;
    return p;
}
/*__________________________________________________________________________
*/
void xfree(void * p) {
    xmalloc_stats_release(malloc_usable_size(p));
    free(p);
}

#if 0
void *xcalloc(size_t n, size_t s) {
    void *p = 0;

    p = calloc(n, s);
    check_memory(p);
    return p;
}

void *xrealloc(void *p, size_t n) {
    p = realloc(p, n);
    check_memory(p);
    return p;
}

#endif
