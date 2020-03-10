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
#ifndef VSTK_TIMERH_TH
#define VSTK_TIMERH_TH


#ifdef __cplusplus
extern "C" {
#endif /*__cplusplus*/
#include "ruc_timer.h"
#include "ruc_timer_api.h"

typedef struct _ruc_timer_th_ctx_t
{
  int thread_owner;
  TICK_TAB_DESCRIPTOR_S  rcvTick ;    /* tick mgt inside application */
  struct  timer_head  * p_timer_slot;	/* timer slot table */
  uint	timer_slot_size;	        /* size of the timer slot table */
  uint	timer_slot_size_mask;	        /* size -1 */
  uint	timer_slot_size_2n;	        /* size in 2^n */
  uint	timer_system_tick;		/* value of the system tick */
  uint	ruc_timer_modulo;		/* for periodic timer       */

			/* Current hand clock position */

  uint 	timer_x_hand_clock;	       	/* hand clock value */
  struct timer_cell 	 *rucTmr_curCellToDelete;
  struct timer_cell 	 *rucTmr_curCellProcess;
  struct timer_cell 	 *rucTmr_p_cell_new ;
} ruc_timer_th_ctx_t;



			/* Timer management functions */

extern int ruc_timer_init_appli_th (TIMER_TICK_VALUE_E timer_application_tick,
		       TIMER_SLOT_SIZE_E timer_slot_size1,
			char app_id,
			char snap_id,
		        TIMER_MODE_SOCKET_E socket_mode) ;

extern  int ruc_timer_send_registration_th(int socket_id,
			TIMER_TICK_VALUE_E timer_application_tick,
			char app_id,
			char snap_id,
		        TIMER_MODE_SOCKET_E socket_mode) ;


extern struct timer_cell  *ruc_timer_alloc_th (char app_id, char snap_id);

extern void ruc_timer_free_th ( struct timer_cell   *p_cell) ;

extern void ruc_timer_start_th (struct timer_cell  *p_cell,
				unsigned long to_val,
				void (*p_fct) (void * fct_param) ,
				void * fct_param);





extern void ruc_timer_stop_th ( struct timer_cell  *p_cell);

extern void ruc_periodic_timer_start_th (struct timer_cell *p_cell,
					 unsigned long to_val,
					 void (*p_fct) (void * fct_param) ,
					 void * fct_param) ;


extern void ruc_timer_process_th() ;

 /*
**___________________________________________________________________________
*/
/**
*   Get the pointer to the socket controller based on the threadID
*/
extern void *ruc_timer_get_ctx_th();

void ruc_timer_init_th (TIMER_TICK_VALUE_E timer_application_tick,
			 TIMER_SLOT_SIZE_E timer_slot_size1);


uint32_t ruc_timer_moduleInit_th(void *sock_ctx_p,uint32_t active);
  

#ifdef __cplusplus
}
#endif /*__cplusplus */

#endif
