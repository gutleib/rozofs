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
#ifndef com_tx_tmr_H
#define com_tx_tmr_H

//#include "integer_types.h"
#include "ruc_common.h"
#include "ruc_list.h"
#include "ruc_timer_api.h"
#include "com_tx_timer_api.h"

/*
**  Sgsn Include
*/



/****************************/
/* charging timer variables  */
/****************************/

typedef struct _com_tx_tmr_var_t {
 /* configuration variables */
    int              module_idx;
    uint32_t         trace;
    uint32_t         period_ms;                      /* period between two lookup */
                                                   /* to the to queue           */
    uint32_t         credit;                         /* nb of element processed   */
                                                   /* at each look up           */             
 /* working variables */
    ruc_obj_desc_t queue[COM_TX_TMR_SLOT_MAX];	   /* queue                     */
    struct timer_cell * p_periodic_timCell;        /* periodic timer cell       */	 
} com_tx_tmr_var_t;



/*
**  prototypes (private)
*/

extern void com_tx_tmr_periodic(void *ns);
extern void com_tx_tmr_periodic_th(void *ns);

extern com_tx_tmr_var_t com_tx_tmr;


#endif
