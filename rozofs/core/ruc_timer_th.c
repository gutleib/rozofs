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

/*--------------------------------------------------------------------------*
 *                       M O D U L E   H E A D E R
 *
 * filename - ruc_timer.c
 *
 * function(s) :
 *
 *     timer_alloc - allocation of a timer cell.
 *     timer_free  - release of a timer cell.
 *
 *     timer_start - starting or restarting of a timer.
 *     periodic_timer_start - starting or restarting
 *                   of a periodic timer.
 *     timer_stop  - implements the stop of a timer.
 *
 *     timer_process - implements the cyclic timer process.
 *
 *
 *-------------------------------------------------------------------------*/


#include "ruc_timer.h"
#include "socketCtrl_th.h"
#include "ruc_sockCtl_api_th.h"
#include "ruc_timer_api_th.h"

/* Redefine uint type */

			/* Access to the thread header values using an index */

#define Head_first_th(x)		tmr_ctx_p->p_timer_slot[x].p_first
#define Head_a_cell_th(x)		(struct timer_cell  *)(tmr_ctx_p->p_timer_slot[x])

int ruc_tmr_th_current_idx = -1;
ruc_timer_th_ctx_t *ruc_timer_th_ctx_tb[ROZOFS_MAX_SOCKCTL_DESCRIPTOR]={NULL};


#define Lock_timer_data()      
#define Unlock_timer_data()	


	/* Internal functions prototypes */

static void timer_insert (struct timer_cell  *p_cell, 
		   ulong to_val);



/*
**____________________________________________________________
*/
int ruc_timer_gobal_init_th()
{   
   if (ruc_tmr_th_current_idx < 0)
   { 
     memset(ruc_timer_th_ctx_tb,0,sizeof(ruc_timer_th_ctx_tb));   
     ruc_tmr_th_current_idx = 0;
   }
   return 0;
}

/*
**___________________________________________________________________________
*/
/**
*   Get the pointer to the socket controller based on the threadID
*/
void *ruc_timer_get_ctx_th()
{
  int i;
  int threadId =pthread_self();
  for (i=0; i< ruc_tmr_th_current_idx; i++)
  {
    if (ruc_timer_th_ctx_tb[i]->thread_owner == threadId) return (void*)ruc_timer_th_ctx_tb[i];  
  }
  return NULL;
}




/*--------------------------------------------------------------------------*
					 F U N C T I O N   H E A D E R

Name            timer_init - 
                     initialization of time slot array

Usage        
	      	void timer_init ()

Related

Return value    

Common constants and declarations : l2team-timer.h

Common objects	: timer_head[] table containing the thread top pointer
		  tmr_ctx_p->timer_x_hand_clock containing the current position of the
		    hand clock (index of timer_head[]).


*--------------------------------------------------------------------------*/

void  ruc_timer_init_th (TIMER_TICK_VALUE_E timer_application_tick,
		      TIMER_SLOT_SIZE_E timer_slot_size1)
{
  int i = 1 ;
  ulong timer_2n ;
  ruc_timer_th_ctx_t *tmr_ctx_p;
  
        ruc_timer_gobal_init_th();
	tmr_ctx_p = malloc(sizeof(ruc_timer_th_ctx_t));
	if (tmr_ctx_p == NULL)
	{
	   exit(-1);
	}
	memset(tmr_ctx_p,0,sizeof(ruc_timer_th_ctx_t));
	tmr_ctx_p->thread_owner = pthread_self();
	// P(sem)
	ruc_timer_th_ctx_tb[ruc_tmr_th_current_idx] = tmr_ctx_p;
	ruc_tmr_th_current_idx++;
	// V(sem)
	
               /* Timer cell initialization */
        tmr_ctx_p->ruc_timer_modulo 		        = 0;  /* for periodic timer only */
        tmr_ctx_p->timer_x_hand_clock                      = 0;
        tmr_ctx_p->timer_slot_size                         = timer_slot_size1;
        tmr_ctx_p->timer_slot_size_mask                    = tmr_ctx_p->timer_slot_size - 1;
	
        tmr_ctx_p->timer_system_tick     = timer_application_tick;

	timer_2n = tmr_ctx_p->timer_slot_size ;
	if (timer_2n== 0)
	  {
	    printf( "\n init timer error : tmr_ctx_p->timer_slot_size = 0)  \n");
	    return;
	  }
	while (((timer_2n = (timer_2n >> 1)) &1) !=1)
	  {
	    i++ ;
	  }
        tmr_ctx_p->timer_slot_size_2n    = i ;


        if ((tmr_ctx_p->p_timer_slot = (struct timer_head *)malloc (
			   	(tmr_ctx_p->timer_slot_size * sizeof (struct timer_head)))) == P_NIL) {
                printf( "\n timer init error : malloc \n");
        }
	memset((char*)tmr_ctx_p->p_timer_slot,0,(size_t)(tmr_ctx_p->timer_slot_size * sizeof (struct timer_head)));


}

/*--------------------------------------------------------------------------*
					 F U N C T I O N   H E A D E R

Name            timer_alloc and timer_free - 
                       implement the allocation of a cell to a timer and 
                       the liberation of a previously allocated cell.

Usage           uint8_t  *timer_alloc (uchar entity_id, nai_t nai, 
                                        sapi_t sapi,uint8_t add);

				void timer_free (uint8_t  *timer_id)

Related
functions usage Alloc_timer_cell (); 			memory space allocation
				Free_timer_cell (timer_id); 	memory space release

Return value    timer_alloc returns the allocated timer identification (address
				of the allocated cell)

Common constants and declarations : l2team-timer.h

*--------------------------------------------------------------------------*/

struct timer_cell *ruc_timer_alloc_th (
	char	  	app_id,    	/* entity ID */
	char		snap_id)	/* connection address */
{
	struct timer_cell	 *p_cell;    /* temporary pointer to the current timer cell */

	/* Get a free cell */
	p_cell = (struct timer_cell *) malloc (sizeof (struct timer_cell)); 
	if (p_cell== P_NIL) return (P_NIL);

		/* Init the allocated cell */

	Cell_app_id 	= app_id;	/* Application ID */
	Cell_snap_id 	= snap_id;	/* SNAP ID */


	Cell_next	= Cell_prior = P_NIL;	/* pointers to NIL */
	Cell_x_head	= X_NIL;	       /* index to NIL */

	return (( struct timer_cell *)p_cell);	/* return timer ID (address of the cell) */

}

/*-------------------------------------------------------------------------*/

void ruc_timer_free_th (
		 struct timer_cell   *p_cell)		/* pointer on the cell to release */
{
       /*
       ** Get the context of the timer service
       */
       ruc_timer_th_ctx_t *tmr_ctx_p;
       tmr_ctx_p = ruc_timer_get_ctx_th();
  
		/* Free timer ? */

	if (p_cell == P_NIL ) return;

		/* If the cell is in a thread removes it */

	ruc_timer_stop_th (p_cell);

		/* Release the cell */

        /*
        ** the cellule is released if it is not the
        ** current cellule used by timer_process. 
        ** Otherwise, that cellule will be released
        ** when the application returns to timer process.
        */
        if (tmr_ctx_p->rucTmr_curCellProcess == p_cell)
        {
           /* 
           ** case of the current cellule:
           ** Just store the reference of the
           ** cellule to delete
           */
           tmr_ctx_p->rucTmr_curCellToDelete = p_cell;
           return;
        }

	free (p_cell);

}

/*--------------------------------------------------------------------------*
					 F U N C T I O N   H E A D E R

Name            timer_start - implements the starting of a previously
			      allocated timer.

		timer_stop - implements the stoping of a pending timer

Usage           timer_start	(uint8_t  *timer_id,timer_val_t to_val,
				 uint8_t sn, code_t ev_id);

		timer_stop	(uint8_t  *timer_id)

Return value    void

Common constants and declarations : l2team-timer.h

Common objects	: timer_head[] table containing the thread top pointer
		  tmr_ctx_p->timer_x_hand_clock containing the current position of the
		    hand clock (index of timer_head[]).

*--------------------------------------------------------------------------*/
void ruc_timer_start_th (struct timer_cell  *p_cell, /* timer ID = address of the cell */
		 ulong to_val, /* time-out value */
                 void (*p_fct) (void * fct_param) ,
		 void * fct_param) /* parameter passed to the function */
{
       
	if (p_cell == P_NIL) return;

	/* Lock the shared data */

	Lock_timer_data ();

	/* If already linked --> stop timer */

	if (Cell_x_head != X_NIL) {
		Unlock_timer_data 	();
		ruc_timer_stop	    	(p_cell);
		Lock_timer_data		();
	}

       	Cell_period_flag = TIMER_OFF;		 /* Set one shot timer */
       	Cell_to_val	 = to_val;   /* save time-out value  */
	Cell_p_fct	 = p_fct;
	Cell_fct_param	 = fct_param;

	/* insert the timer cell into the timer thread */
	timer_insert (p_cell, to_val);

	/* Unlock data timer */
	Unlock_timer_data ();
}


/*-------------------------------------------------------------------------*/

void ruc_timer_stop_th (struct timer_cell  *p_cell)	/* timer ID = address of the cell */
{
       /*
       ** Get the context of the timer service
       */
       ruc_timer_th_ctx_t *tmr_ctx_p;
       tmr_ctx_p = ruc_timer_get_ctx_th();

	if (p_cell == P_NIL) return;

	/* Lock the shared data */

	Lock_timer_data ();

       	/* Is the cell in a thread ? */

       	if (Cell_x_head == X_NIL) {
	       	Unlock_timer_data ();
	       	return;
		}

       	/* Yes - remove from the thread */
        /*
        ** take care of the case of the stop timer of
        ** the next cellule used by timer_process
        */
        if (tmr_ctx_p->rucTmr_p_cell_new == p_cell)
        { 
          tmr_ctx_p->rucTmr_p_cell_new = Cell_next;
        }

	if (Cell_next != P_NIL) {
		Prior (Cell_next) = Cell_prior;
		}

	if (Cell_prior != P_NIL) {
		Next (Cell_prior) = Cell_next;
		} else {
	       	Head_first_th (Cell_x_head) = Cell_next;
		}

	/* Reset the cell pointers */

	Cell_next 	= Cell_prior = P_NIL;
	Cell_x_head = X_NIL;

	/* Unlock the shared data */
	Unlock_timer_data ();
}

/*--------------------------------------------------------------------------*
					 F U N C T I O N   H E A D E R

Name            periodic_timer_start - implements the starting of a previously
				allocated periodic timer.

Usage           periodic_timer_start (uint8_t  *timer_id,
				      timer_val_t to_val,
				      void (*p_fct) (),
				      ulong fct_param);

Return value    void

Common constants and declarations : l2team-timer.h

Common objects	:

*--------------------------------------------------------------------------*/

 void ruc_periodic_timer_start_th (struct timer_cell *p_cell, 
					 unsigned long to_val, 
 					 void (*p_fct) (void * fct_param) ,
 					 void * fct_param) 
   
{
	ulong  to_val_init;
       /*
       ** Get the context of the timer service
       */
       ruc_timer_th_ctx_t *tmr_ctx_p;
       tmr_ctx_p = ruc_timer_get_ctx_th();

	if (p_cell == P_NIL) return;

	/* Lock the shared data */

	Lock_timer_data ();

	/* If already linked --> stop timer */

	if (Cell_x_head != X_NIL) {
		Unlock_timer_data 	();
		ruc_timer_stop	    	(p_cell);
		Lock_timer_data		();
	}

       	Cell_period_flag = TIMER_ON;		 /* Set periodic timer flag */
       	Cell_to_val	 = to_val;   /* save time-out value for next starting */
	Cell_p_fct	 = p_fct;
	Cell_fct_param	 = fct_param;

	/* insert the timer cell into the timer thread */

        /* add the modulo for periodic timer to avoid having all queued on
        ** the same slot
        */
        tmr_ctx_p->ruc_timer_modulo++;
	tmr_ctx_p->ruc_timer_modulo =tmr_ctx_p->ruc_timer_modulo%20;  /* 20  slots (tick = 50 ms)*/
	to_val_init = to_val + (ulong)((tmr_ctx_p->timer_system_tick*tmr_ctx_p->ruc_timer_modulo));
	timer_insert (p_cell, to_val_init);

	/* Unlock data timer */
	Unlock_timer_data ();
}


/*--------------------------------------------------------------------------*
					 F U N C T I O N   H E A D E R

Name            timer_insert - inserts a cell into the timer threads.

Usage           timer_insert (uint8_t  *timer_id, timer_val_t to_val);

Return value    void

Related
functions		called by timer_start() and
		       	periodic_timer_start() functions.

Common constants and declarations : l2team-timer.h

Common objects	: - timer_head[] table containing the thread top pointer
		  - tmr_ctx_p->timer_x_hand_clock contains the current position of the
		    hand clock (index of timer_head[]).


*--------------------------------------------------------------------------*/

static void timer_insert (struct timer_cell  *p_cell, /* address of the cell */
		   ulong to_val)/* time-out value */
   
{
	ulong delta;
	ulong nb;
       /*
       ** Get the context of the timer service
       */
       ruc_timer_th_ctx_t *tmr_ctx_p;
       tmr_ctx_p = ruc_timer_get_ctx_th();

#if 0
    printf ("!! timer_insert to_val=%d\n",(int)to_val);
#endif

		/* Evaluate the delta in tmr_ctx_p->timer_system_tick unit  */
          delta  = ((ulong)to_val / (ulong)tmr_ctx_p->timer_system_tick) ;
       
        if (delta == 0)
          delta = 1;

	if (delta > tmr_ctx_p->timer_slot_size) {
		 /*
		       	'delta' is beyond the nb of slots :
		       	Chain the cell in the slot given by modulo tmr_ctx_p->timer_slot_size ,
		       	and add in cell nb of
	       	*/
		Cell_x_head = (tmr_ctx_p->timer_x_hand_clock + ( delta & (tmr_ctx_p->timer_slot_size_mask)));
		nb = (delta >> tmr_ctx_p->timer_slot_size_2n) ; /*  modulo #  */

	} else {
	        Cell_x_head = (tmr_ctx_p->timer_x_hand_clock  + delta) ;
		nb = 0 ;
	}
	Cell_x_head &= tmr_ctx_p->timer_slot_size_mask ;
	Cell_to_mod_nb	= nb;   /* update modulo nb */

	/* Insert the cell into the top of the thread */

	Cell_next	= Head_first_th (Cell_x_head);
	Cell_prior	= P_NIL;

	if (Head_first_th (Cell_x_head) != P_NIL) {
		Prior (Head_first_th (Cell_x_head)) = (struct timer_cell  *)p_cell;
	}

	Head_first_th (Cell_x_head) = (struct timer_cell  *)p_cell;

}

/*--------------------------------------------------------------------------*
					 F U N C T I O N   H E A D E R

Name            timer_process - implements the cyclic process which increments
				the current hand clock position by one and processes the
				pending time_out.

Usage           void timer_process (void);

Related
functions usage 

Return value    void

Common objects	: timer_head[] table contains the thread top pointer.

		  tmr_ctx_p->timer_x_hand_clock contains the current position of the
		  hand clock (index of timer_head[]).

		  timer_last_time_clock contains the system clock time of the last
		  delay expiration.


*--------------------------------------------------------------------------*/

void ruc_timer_process_th ()     
{
    /* local variable definition */

    struct timer_cell 	 *  p_cell;   	/* temporary timer cell pointer */
    /*
    ** Get the context of the timer service
    */
    ruc_timer_th_ctx_t *tmr_ctx_p;
    tmr_ctx_p = ruc_timer_get_ctx_th();
    
	/* Lock the shared data */
/*
    printf ("!! timer_process tmr_ctx_p->timer_x_hand_clock=%d\n",tmr_ctx_p->timer_x_hand_clock);
*/
	Lock_timer_data ();

        /*
        ** Update the variable that are used to avoid 
        ** freing of the current cellule (when the
        ** timer has expired and the application is
        ** currently processing its time-out
        */
      	tmr_ctx_p->rucTmr_curCellToDelete = P_NIL;
        tmr_ctx_p->rucTmr_curCellProcess = P_NIL;
        tmr_ctx_p->rucTmr_p_cell_new = P_NIL;

	
	if (++tmr_ctx_p->timer_x_hand_clock == tmr_ctx_p->timer_slot_size) 
        {
	    tmr_ctx_p->timer_x_hand_clock = 0;
	}
#if 0

    printf ("!! timer_process tmr_ctx_p->timer_x_hand_clock=%d\n",tmr_ctx_p->timer_x_hand_clock);
#endif


	/* Any cell linked to the current slot ? */

	if ((p_cell = Head_first_th (tmr_ctx_p->timer_x_hand_clock)) != P_NIL) 
        {

	    /* The slot is not empty */
#if 0
	    
	    /* Reset the head of thread */
	    
	    Head_first_th (tmr_ctx_p->timer_x_hand_clock) = P_NIL;
#endif
	    
       	    /* Scan the cell thread */
	    
	    while (p_cell != P_NIL) 
            {
	      tmr_ctx_p->rucTmr_p_cell_new = Cell_next;
#if 0
              printf("pfirst 0x%x p_cell: %x pnext: 0x%x pprev: 0x%x nb = %d\n",Head_first_th (tmr_ctx_p->timer_x_hand_clock),p_cell,p_cell->p_next,p_cell->p_prior,p_cell->to_mod_nb);
#endif
		
	      if (Cell_to_mod_nb== 0) 
              {    
		/* 
                ** This timer will expire during the current tick
                */
                /* 
                ** call the timer_stop function in order
                ** to take care of the prior pointer.
                ** If the prior field of the next
                ** timer cellule. In fact, if the next
                ** timer cellule is released furing the
                ** same tick processing, it will update the
                ** link fields (pnext and prior) of the 
                ** current cellule of timer_process
                */
                ruc_timer_stop_th(p_cell);

                /*
                ** save the current cellule since
                ** application may try to release it
                */
      	        tmr_ctx_p->rucTmr_curCellProcess = p_cell;
      	        tmr_ctx_p->rucTmr_curCellToDelete = P_NIL;

		/*
	        **   call the user function  
	        */
		(*Cell_p_fct) (Cell_fct_param);

                /*
                ** looks if the cellule has to be
                ** deleted
                */
                if (tmr_ctx_p->rucTmr_curCellToDelete != P_NIL)
                {
                  /*
                  ** the application has requested
                  ** to delete the current cellule
                  */

                   free(tmr_ctx_p->rucTmr_curCellProcess);
                   tmr_ctx_p->rucTmr_curCellToDelete = P_NIL;
                 }
                 else
                 {
		   if (Cell_period_flag == TIMER_ON) 
		   {
	             /* restart the timer	*/
		     /* Re-insert the timer cell into the timer thread. */
		     timer_insert (p_cell, Cell_to_val);
			
		   } 
                 }
                 /*
                 ** update for the next attempt
                 */
                 tmr_ctx_p->rucTmr_curCellProcess = P_NIL;
		    
	      }
	      else
	      {
		Cell_to_mod_nb--;
#if 0
	    	/* 
                ** Insert the cell into the top of the thread 
                */
	        Cell_next	= Head_first_th (Cell_x_head);
		Cell_prior	= P_NIL;

		if (Head_first_th (Cell_x_head) != P_NIL) 
                {
		  Prior (Head_first_th (Cell_x_head)) = (struct timer_cell  *)p_cell;
		}
		Head_first_th (Cell_x_head) = (struct timer_cell  *)p_cell;
#endif

	      }
		
              /* Next cell */
		
              p_cell = tmr_ctx_p->rucTmr_p_cell_new;
		
	    } /* While p cell != P_NIL */
	    
	}
 
        else 
        {
	    
	  /* 
          ** The slot is empty : nothing to do
          */
	  	    
	}
        tmr_ctx_p->rucTmr_p_cell_new = P_NIL;
	Unlock_timer_data ();   
}


