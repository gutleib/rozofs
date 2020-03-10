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
 
 #ifndef _ROZOFS_QUEUE_PRI_H
 #define _ROZOFS_QUEUE_PRI_H
 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <pthread.h> 
#include <errno.h>

#define ROZOFS_MAX_PRIO 8


typedef struct {

    unsigned int      head;
    unsigned int      tail;
    void       **queue;
} rozofs_queue_internal_t;


typedef struct {
    int               nb_prio;
    unsigned int      prio_mask;
    unsigned int      size;
    pthread_mutex_t   lock;
    pthread_cond_t    wait_room;
    pthread_cond_t    wait_data;
    rozofs_queue_internal_t queue_ctx[ROZOFS_MAX_PRIO];
} rozofs_queue_prio_t;



void *rozofs_queue_get_prio(rozofs_queue_prio_t * q,int *prio);
int rozofs_queue_put_prio(rozofs_queue_prio_t *q, void *j,int prio);
int rozofs_queue_init_prio(rozofs_queue_prio_t * q, const unsigned int slots,int nb_prio);






#endif
