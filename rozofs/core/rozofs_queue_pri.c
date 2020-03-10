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

#include "rozofs_queue_pri.h"

/*
**__________________________________________________________________
*/
int rozofs_queue_init_prio(rozofs_queue_prio_t * q, const unsigned int slots,int nb_prio)
{
    int i;
    rozofs_queue_internal_t *q_int_p;
    if (!q || slots < 1U)
        return errno = EINVAL;
    if (nb_prio > ROZOFS_MAX_PRIO)
        return errno=EINVAL;
    
    q->nb_prio = nb_prio;
    q->size = slots+ 1U; 
    q_int_p = q->queue_ctx;
    for (i = 0; i < q->nb_prio; i++,q_int_p++)
    {
      q_int_p->queue = malloc(sizeof (void *) * ((size_t)(slots + 1)));
      if (!q_int_p->queue)
          return errno = ENOMEM;

      q_int_p->head = 0U;
      q_int_p->tail = 0U;
    }
    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->wait_room, NULL);
    pthread_cond_init(&q->wait_data, NULL);

    return 0;
}

/*
**__________________________________________________________________
*/


void *rozofs_queue_get_internal(rozofs_queue_internal_t * q,unsigned int size, int *full)
{
    void *j;
    *full = 0;

    if (q->head == q->tail) return NULL;

    if ((q->head + 1U) % size == q->tail) 
    {
       *full = 1;
    }
    j = q->queue[q->tail];
    q->queue[q->tail] = NULL;
    q->tail = (q->tail + 1U) % size;
   
    return j;
}


/*
**__________________________________________________________________
*/
void *rozofs_queue_get_prio(rozofs_queue_prio_t * q, int *prio)
{
    void *j = NULL;
    int full = 0;
    int i;

    *prio = -1;
    rozofs_queue_internal_t *q_int_p;
    
    pthread_mutex_lock(&q->lock);
    while (j==NULL)
    {
      q_int_p =q->queue_ctx;
      for (i = 0; i < q->nb_prio; i++,q_int_p++)
      {
	j = rozofs_queue_get_internal(q_int_p,q->size,&full);
	if (j != NULL) {
	*prio = i;
	goto done; 
	}   
      }
      pthread_cond_wait(&q->wait_data,&q->lock);
    }
done:    
    pthread_mutex_unlock(&q->lock);

    if (full) pthread_cond_signal(&q->wait_room);

    return j;
}

/*
**__________________________________________________________________
*/

int rozofs_queue_put_prio(rozofs_queue_prio_t *q, void *j,int prio)
{
    int empty = 0;
    rozofs_queue_internal_t *q_int_p;
        
    if (prio >= q->nb_prio) return errno=EINVAL;
    q_int_p = &q->queue_ctx[prio];
    
    pthread_mutex_lock(&q->lock);
    while ((q_int_p->head + 1U) % q->size == q_int_p->tail)
        pthread_cond_wait(&q->wait_room,&q->lock);

    if (q_int_p->head == q_int_p->tail) 
    {
      empty = 1;
//      empty_stats++;
    }  
    q_int_p->queue[q_int_p->head] = j;
    q_int_p->head = (q_int_p->head + 1U) % q->size;
    pthread_mutex_unlock(&q->lock);
    
    if (empty) pthread_cond_signal(&q->wait_data);

    return 0;
}
