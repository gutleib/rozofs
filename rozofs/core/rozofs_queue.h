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
 
 #ifndef _ROZOFS_QUEUE_H
 #define _ROZOFS_QUEUE_H
 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <pthread.h> 
#include <errno.h>


typedef struct {
    pthread_mutex_t   lock;
    pthread_cond_t    wait_room;
    pthread_cond_t    wait_data;
    unsigned int      size;
    unsigned int      head;
    unsigned int      tail;
    void       **queue;
} rozofs_queue_t;

void *rozofs_queue_get(rozofs_queue_t * q);
void rozofs_queue_put(rozofs_queue_t *q, void *j);
int rozofs_queue_init(rozofs_queue_t * q, const unsigned int slots);




#endif
