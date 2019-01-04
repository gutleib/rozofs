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

#include "rozofs_queue.h"

/*
**__________________________________________________________________
*/
int rozofs_queue_init(rozofs_queue_t * q, const unsigned int slots)
{
    if (!q || slots < 1U)
        return errno = EINVAL;

    q->queue = malloc(sizeof (void *) * (size_t)(slots + 1));
    if (!q->queue)
        return errno = ENOMEM;

    q->size = slots+ 1U; 
    q->head = 0U;
    q->tail = 0U;

    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->wait_room, NULL);
    pthread_cond_init(&q->wait_data, NULL);

    return 0;
}

/*
**__________________________________________________________________
*/
void *rozofs_queue_get(rozofs_queue_t * q)
{
    void *j;

    pthread_mutex_lock(&q->lock);
    while (q->head == q->tail)
        pthread_cond_wait(&q->wait_data,&q->lock);

    j = q->queue[q->tail];
    q->queue[q->tail] = NULL;
    q->tail = (q->tail + 1U) % q->size;

    pthread_cond_signal(&q->wait_room);

    pthread_mutex_unlock(&q->lock);
    return j;
}

/*
**__________________________________________________________________
*/
void rozofs_queue_put(rozofs_queue_t *q, void *j)
{
    pthread_mutex_lock(&q->lock);
    while ((q->head + 1U) % q->size == q->tail)
        pthread_cond_wait(&q->wait_room,&q->lock);


    q->queue[q->head] = j;
    q->head = (q->head + 1U) % q->size;

    pthread_cond_signal(&q->wait_data);

    pthread_mutex_unlock(&q->lock);
    return;
}

