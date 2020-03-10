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
#ifndef com_tx_tmr_APIH
#define com_tx_tmr_APIH

#include "ruc_list.h"
#include "ruc_timer_api.h"

/*
** there is one timer slot per timr type
*/
#define  COM_TX_TMR_SLOT0         0
#define  COM_TX_TMR_SLOT1         1
#define  COM_TX_TMR_SLOT2         2
#define  COM_TX_TMR_SLOT3         3
#define  COM_TX_TMR_SLOT4         4
#define  COM_TX_TMR_SLOT5         5

#define COM_TX_TMR_SLOT_MAX       (COM_TX_TMR_SLOT5+1)

         /*----------------------*/
         /* charging timer types  */
         /*----------------------*/

typedef void (*com_tx_tmr_callBack_t)(void*) ; /* call back type */


         /*----------------------*/
         /* Timer cell structure */
         /*----------------------*/

typedef struct _com_tx_tmr_cell_t {
    ruc_obj_desc_t         listHead;    /* header used by list service for queing */
    uint32_t                 date_s;      /* time out date in system milliseconds */
    uint32_t                 delay;      /* delay requested in ms      */
    com_tx_tmr_callBack_t p_callBack;  /* call back to be used at time out */
    void                 *cBParam;     /* parameter to be provided at time out */
} com_tx_tmr_cell_t;

#define com_tx_tmr_CELL_LGTH		sizeof (struct com_tx_tmr_cell_t)


/*--------------------------------------------------------------------------*/


    /* Prepaid Timer management APIs */


/*
**  IN : tmr_slot : index of the timer slot.
**       p_refTim   : reference of the timer cell to use
**       date_s     : requested time out date, in seconds
**       p_callBack : client call back to call at time out
**       cBParam    : client parameter to provide at time out
**
**  OUT : OK/NOK
*/
extern uint32_t com_tx_tmr_start (uint8_t tmr_slot,
                               com_tx_tmr_cell_t *p_refTim, 
                               uint32_t date_s,
                               com_tx_tmr_callBack_t p_callBack,
                               void *cBParam);


extern uint32_t  com_tx_tmr_start_th (uint8_t   tmr_slot,
                        com_tx_tmr_cell_t *p_refTim, 
                        uint32_t  date_s,
                        com_tx_tmr_callBack_t p_callBack,
                        void *cBParam
			);
/*
**  IN : reference of the timer cell to stop
**
**  OUT : OK/NOK
*/
extern uint32_t com_tx_tmr_stop (com_tx_tmr_cell_t *p_refTim);


/*
**  IN : period_ms : period between two queue sequence reading in ms
**       credit    : pdp credit processing number
**
**  OUT : OK/NOK
*/

extern int com_tx_tmr_init (uint32_t period_ms,
                             uint32_t credit) ;

extern int com_tx_tmr_init_th(uint32_t period_ms,
                    uint32_t credit);
#endif
