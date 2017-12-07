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

#ifndef _LIST_H
#define _LIST_H

#include <stdlib.h>
#include <stddef.h>

typedef struct list {
    struct list *next, *prev;
} list_t;

static inline void list_init(list_t * list) {
    list->next = list;
    list->prev = list;
}

static inline void list_insert(list_t * new, list_t * prev, list_t * next) {
    next->prev = new;
    new->next = next;
    new->prev = prev;
    prev->next = new;
}

static inline void list_push_front(list_t * head, list_t * new) {
    list_insert(new, head, head->next);
}

static inline void list_push_back(list_t * head, list_t * new) {
    list_insert(new, head->prev, head);
}

static inline void list_remove(list_t * list) {
    list->next->prev = list->prev;
    list->prev->next = list->next;
    list->next = (void *) list;
    list->prev = (void *) list;
}

static inline int list_empty(list_t * head) {
    return head->next == head;
}



static inline void list_move_nocheck(list_t * to, list_t * from) {
    
    to->prev->next   = from->next;
    from->next->prev = to->prev;
    
    from->prev->next = to;
    to->prev         = from->prev;
    
    list_init(from);
}
static inline void list_move(list_t * to, list_t * from) {

    if (list_empty(from)) return;
    
    list_move_nocheck(to,from);
}


static inline int list_size(list_t * head) {
    list_t *iterator;
    int size;
    for (iterator = (head)->next, size = 0; iterator != (head);
         iterator = iterator->next, size++);
    return size;
}

static inline void list_sort(list_t * head,
                             int (*cmp) (list_t * a, list_t * b)) {
    list_t *p, *q, *e, *list, *tail, *oldhead;
    int insize, nmerges, psize, qsize, i;

    if (list_empty(head))
        return;

    list = head->next;
    list_remove(head);
    insize = 1;
    for (;;) {
        p = oldhead = list;
        list = tail = NULL;
        nmerges = 0;

        while (p) {
            nmerges++;
            q = p;
            psize = 0;
            for (i = 0; i < insize; i++) {
                psize++;
                q = q->next == oldhead ? NULL : q->next;
                if (!q)
                    break;
            }

            qsize = insize;
            while (psize > 0 || (qsize > 0 && q)) {
                if (!psize) {
                    e = q;
                    q = q->next;
                    qsize--;
                    if (q == oldhead)
                        q = NULL;
                } else if (!qsize || !q) {
                    e = p;
                    p = p->next;
                    psize--;
                    if (p == oldhead)
                        p = NULL;
                } else if (cmp(p, q) <= 0) {
                    e = p;
                    p = p->next;
                    psize--;
                    if (p == oldhead)
                        p = NULL;
                } else {
                    e = q;
                    q = q->next;
                    qsize--;
                    if (q == oldhead)
                        q = NULL;
                }
                if (tail)
                    tail->next = e;
                else
                    list = e;
                e->prev = tail;
                tail = e;
            }
            p = q;
        }

        tail->next = list;
        list->prev = tail;

        if (nmerges <= 1)
            break;

        insize *= 2;
    }

    head->next = list;
    head->prev = list->prev;
    list->prev->next = head;
    list->prev = head;
}

#define list_entry(ptr, type, member) \
	((type *)((char *)(ptr)-(unsigned long)(&((type *)0)->member)))

#define list_first_entry(ptr, type, member) \
        list_entry((ptr)->next, type, member)

#define list_last_entry(head, type, member) \
        ((head)->prev==(head))? NULL : list_entry((head)->prev, type, member);
#define list_1rst_entry(head, type, member) \
        ((head)->prev==(head))? NULL : list_entry((head)->next, type, member);
	
#define list_for_each_forward(pos, head) \
	for (pos = (head)->next; pos != (head); pos = pos->next)

#define list_for_each_backward(pos, head) \
	for (pos = (head)->prev; pos != (head); pos = pos->prev)

#define list_for_each_forward_safe(pos, n, head)\
    for (pos = (head)->next, n = pos->next; pos != (head); \
    	pos = n, n = pos->next)

#define list_for_each_backward_safe(pos, n, head)\
    for (pos = (head)->prev, n = pos->prev; pos != (head); \
    	pos = n, n = pos->prev)


/*
** Distributor of contexts
*/
typedef struct _list_distributor_t {
  list_t    list;
  uint32_t  nb_ctx;
  uint32_t  ctx_size;
  void *    first_ctx;
} list_distributor_t;

#define list_distributor_create( pDist, nbElements, structure, member) { \
  uint32_t       alloc_size; \
  structure    * pCtx;\
  uint8_t      * pByte;\
  int            i;\
\
  list_init(&(pDist->list));\
\
  alloc_size = sizeof(structure);\
  if ((alloc_size & 0x3) != 0) {\
     alloc_size = ((alloc_size & (~0x3)) + 4 );\
  }\
  pDist->ctx_size = alloc_size;\
  pDist->nb_ctx   = nbElements;\
  alloc_size *= nbElements;\
\
  pDist->first_ctx = xmalloc (alloc_size); \
  if (pDist->first_ctx == NULL) { \
    fatal("Out of memeory. Size %llu",(long long unsigned int)alloc_size);\
  }\
\
  pByte = pDist->first_ctx;\
  for (i = 0; i < nbElements; i++, pByte += pDist->ctx_size) {\
    pCtx = (structure*) pByte;\
    list_init(&pCtx->member);\
    list_push_back(&(pDist->list),&pCtx->member);\
  }\
} 

static inline void * list_distributor_get(list_distributor_t * dist, uint32_t idx) {
  uint8_t        * pByte;

  /*
  **  get the max number of element of the list
  */
  if (idx >= dist->nb_ctx) {
    return NULL;
  }
  /*
  ** all is fine, get the pointer to the head of the list
  ** and compute the real address of the object
  */
  pByte = (uint8_t*) dist->first_ctx;
  pByte += (dist->ctx_size*idx);
  return pByte;
}
#endif
