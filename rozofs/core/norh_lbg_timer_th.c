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
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <sys/times.h>
#include <sys/time.h>

#include <rozofs/common/types.h>
#include <rozofs/common/log.h>

#include "ruc_common.h"
#include "ruc_list.h"
#include "north_lbg_timer_api.h"
#include "north_lbg_timer.h"
#include "socketCtrl_th.h"
#include "ruc_sockCtl_api_th.h"
#include "ruc_timer_api_th.h"

#define NORTH_LBG_TIMER_MAX_DATE 0xFFFFFFFF


/*
**   G L O B A L    D A T A
*/

/*
**  chartim variables (trace=false)
*/


north_lbg_tmr_var_t *north_lbg_tmr_ctx_tb[ROZOFS_MAX_SOCKCTL_DESCRIPTOR]={NULL};

#define MILLISECLONG(time) (((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)/1000)


/************************/
/* internal functions   */
/************************/

uint32_t north_lbg_time();


/*----------------------------------------------
**  north_lbg_tmr_periodic
**----------------------------------------------
**
**  It perform the processing of a limited number
**  of queued elements
**
**  IN : not significant
**
**  OUT : none
**
**-----------------------------------------------
*/
void north_lbg_tmr_processSlot(ruc_obj_desc_t* pTmrQueue) ;

void north_lbg_tmr_periodic_th(void *tmr_ctx_p)
{

  north_lbg_tmr_var_t *north_lbg_tmr_p;
  
  north_lbg_tmr_p = (north_lbg_tmr_var_t*)tmr_ctx_p;
//  printf("north_lbg_tmr_periodic Ticker!\n");
  int i;

  for ( i = 0; i < NORTH_LBG_TMR_SLOT_MAX; i++)
  {
     north_lbg_tmr_processSlot(&north_lbg_tmr_p->queue[i]);
  }

}

/************************/
/* APIS                 */
/************************/

/*
**----------------------------------------------
**  north_lbg_tmr_start
**----------------------------------------------
**
**  charging timer service starting request
**
**  IN : p_refTim   : reference of the timer cell to use
**       date_s     : requested time out date, in seconds
**       p_callBack : client call back to call at time out
**       cBParam    : client parameter to provide at time out
**
**  OUT : OK/NOK
**
**-----------------------------------------------
*/
uint32_t north_lbg_tmr_start_th (uint8_t   tmr_slot,
                        north_lbg_tmr_cell_t *p_refTim,
                        uint32_t date_s,
                        north_lbg_tmr_callBack_t p_callBack,
                        void *cBParam
			)
{
     uint32_t ret;
     north_lbg_tmr_var_t * north_lbg_tmr_p;
     int module_idx = ruc_sockctl_get_thread_module_idx_th();
     
     if (module_idx < 0)
     {
        fatal("Not module index for thread %lu",pthread_self());
     }
    /*
    ** get the Realtime clock context of this thread
    */
    north_lbg_tmr_p =  north_lbg_tmr_ctx_tb[module_idx];
    if (north_lbg_tmr_p == NULL)
    {
       fatal("something rotten for thread %lu",pthread_self());
    }
    
    north_lbg_tmr_ctx_tb[module_idx] = north_lbg_tmr_p;

     if (tmr_slot >= NORTH_LBG_TMR_SLOT_MAX)
     {
        severe( "north_lbg_tmr_start : slot out of range : %d ",tmr_slot );
	return RUC_NOK;
     }
    /*
    ** timer cell dequeing from its current queue
    ** This dequeing is always attempted, even if not queued
    ** As the list primitive is protected, no initial check
    ** need to be done
    */
    ruc_objRemove((ruc_obj_desc_t *)p_refTim);

    /*
    ** initialize context
    */
    p_refTim->date_s = date_s + north_lbg_time();
    p_refTim->date_s &= NORTH_LBG_TIMER_MAX_DATE;
    p_refTim->delay = date_s;

    p_refTim->p_callBack = p_callBack;
    p_refTim->cBParam = cBParam;


    ret=ruc_objInsertTail(&north_lbg_tmr_p->queue[tmr_slot],(ruc_obj_desc_t*)p_refTim);
    if(ret!=RUC_OK){
        severe( "Pb while inserting cell in queue, ret=%u", ret );
        return(RUC_NOK);
    }
    return(RUC_OK);
}


/*
**----------------------------------------------
**  north_lbg_tmr_init_th
**----------------------------------------------
**
**   charging timer service initialisation request
**
**  IN : period_ms : period between two queue sequence reading in ms
**       credit    : pdp credit processing number
**
**  OUT : OK/NOK
**
**-----------------------------------------------
*/
int north_lbg_tmr_init_th(uint32_t period_ms,
                    uint32_t credit)
{

    int i;

    int module_idx = ruc_sockctl_get_thread_module_idx_th();
    
    if (module_idx < 0)
    {
      fatal("bad module index: the socket controller might not be created (%lu)",pthread_self());
    }
    /*
    ** get the Realtime clock context of this thread
    */
    north_lbg_tmr_var_t * north_lbg_tmr_p = malloc(sizeof(north_lbg_tmr_var_t));
    if (north_lbg_tmr_p == NULL)
    {
       fatal("Out of memory");
    }
    
    north_lbg_tmr_ctx_tb[module_idx] = north_lbg_tmr_p;
    
    /**************************/
    /* configuration variable */
    /* initialization         */
    /**************************/


    /*
    ** time between to look up to the charging timer queue
    */
    if (period_ms!=0){
        north_lbg_tmr_p->period_ms=period_ms;
    } else {
        severe( "bad provided timer period (0 ms), I continue with 100 ms" );
        north_lbg_tmr_p->period_ms=100;
    }

    /*
    ** Number of pdp context processed at each look up;
    */
    if (credit!=0){
        north_lbg_tmr_p->credit=credit;
    } else {
        severe( "bad provided  credit (0), I continue with 1" );
        north_lbg_tmr_p->credit=1;
    }

    /**************************/
    /* working variable       */
    /* initialization         */
    /**************************/


    /*
    **  queue initialization
    */
    for (i = 0; i < NORTH_LBG_TMR_SLOT_MAX; i++)
    {
      ruc_listHdrInit(&north_lbg_tmr_p->queue[i]);
    }

    /*
    ** charging timer periodic launching
    */
    north_lbg_tmr_p->p_periodic_timCell=ruc_timer_alloc(0,0);
    if (north_lbg_tmr_p->p_periodic_timCell == (struct timer_cell *)NULL){
        severe( "No timer available for MS timer periodic" );
        return(RUC_NOK);
    }
    ruc_periodic_timer_start_th(north_lbg_tmr_p->p_periodic_timCell,
	      (north_lbg_tmr_p->period_ms*TIMER_TICK_VALUE_100MS/100),
	      north_lbg_tmr_periodic_th,
	      north_lbg_tmr_p);

    return(RUC_OK);
}

