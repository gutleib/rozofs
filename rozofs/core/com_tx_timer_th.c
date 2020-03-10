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


#include "ruc_common.h"
#include "ruc_list.h"
#include "com_tx_timer_api.h"
#include "com_tx_timer.h"
#include <rozofs/common/log.h>
#include "socketCtrl_th.h"
#include "ruc_sockCtl_api_th.h"
#include "ruc_timer_api_th.h"

#define COM_TX_TIMER_MAX_DATE 0xFFFFFFFF


/*
**   G L O B A L    D A T A 
*/

/*
**  chartim variables (trace=false)
*/

com_tx_tmr_var_t *com_tx_tmr_ctx_tb[ROZOFS_MAX_SOCKCTL_DESCRIPTOR]={NULL};

#define MILLISECLONG(time) (((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)/1000)


/************************/
/* internal functions   */
/************************/

uint32_t  com_tx_time();


/*----------------------------------------------
**  com_tx_tmr_periodic_th
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
void com_tx_tmr_processSlot(ruc_obj_desc_t* pTmrQueue) ;

void com_tx_tmr_periodic_th(void *ctx_p) 
{
 
  com_tx_tmr_var_t *com_tx_tmr_p = (com_tx_tmr_var_t*)ctx_p; 
  int i;
  
  for ( i = 0; i < COM_TX_TMR_SLOT_MAX; i++)
  {
     com_tx_tmr_processSlot(&com_tx_tmr_p->queue[i]);
  }

}


/************************/
/* APIS                 */
/************************/

/*
**----------------------------------------------
**  com_tx_tmr_start
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
uint32_t  com_tx_tmr_start_th (uint8_t   tmr_slot,
                        com_tx_tmr_cell_t *p_refTim, 
                        uint32_t  date_s,
                        com_tx_tmr_callBack_t p_callBack,
                        void *cBParam
			)
{
     uint32_t ret;       
     com_tx_tmr_var_t * com_tx_tmr_p;
     int module_idx = ruc_sockctl_get_thread_module_idx_th();
     
     if (module_idx < 0)
     {
        fatal("Not module index for thread %lu",pthread_self());
     }
    /*
    ** get the Realtime clock context of this thread
    */
    com_tx_tmr_p =  com_tx_tmr_ctx_tb[module_idx];
    if (com_tx_tmr_p == NULL)
    {
       fatal("something rotten for thread %lu",pthread_self());
    }     
     if (tmr_slot >= COM_TX_TMR_SLOT_MAX)
     {
        severe( "com_tx_tmr_start : slot out of range : %d ",tmr_slot );
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
    p_refTim->date_s = date_s + com_tx_time();
    p_refTim->date_s &= COM_TX_TIMER_MAX_DATE;
    p_refTim->delay = date_s;
    
    p_refTim->p_callBack = p_callBack;
    p_refTim->cBParam = cBParam;


    ret=ruc_objInsertTail(&com_tx_tmr_p->queue[tmr_slot],(ruc_obj_desc_t*)p_refTim);
    if(ret!=RUC_OK){
        severe( "Pb while inserting cell in queue, ret=%u", ret );
        return(RUC_NOK);
    }
    return(RUC_OK);
}


/*
**----------------------------------------------
**  com_tx_tmr_init
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
int com_tx_tmr_init_th(uint32_t period_ms,
                    uint32_t credit)
{

    int i;
    
    /**************************/
    /* configuration variable */
    /* initialization         */
    /**************************/
     int module_idx = ruc_sockctl_get_thread_module_idx_th();
     
     if (module_idx < 0)
     {
        fatal("Not module index for thread %lu",pthread_self());
     }
     com_tx_tmr_var_t * com_tx_tmr_p = malloc(sizeof(com_tx_tmr_var_t));
     if (com_tx_tmr_p == NULL)
     {
	fatal("Out of memory");
     }
    
    com_tx_tmr_ctx_tb[module_idx] = com_tx_tmr_p;
    com_tx_tmr_p->module_idx = module_idx;

    /*
    ** time between to look up to the charging timer queue
    */
    if (period_ms!=0){
        com_tx_tmr_p->period_ms=period_ms;
    } else {
        severe( "bad provided timer period (0 ms), I continue with 100 ms" );
        com_tx_tmr_p->period_ms=100;
    }

    /*
    ** Number of pdp context processed at each look up;
    */
    if (credit!=0){
        com_tx_tmr_p->credit=credit;
    } else {
        severe( "bad provided  credit (0), I continue with 1" );
        com_tx_tmr_p->credit=1;
    }

    /**************************/
    /* working variable       */
    /* initialization         */
    /**************************/


    /*
    **  queue initialization
    */
    for (i = 0; i < COM_TX_TMR_SLOT_MAX; i++)
    {
      ruc_listHdrInit(&com_tx_tmr_p->queue[i]);
    }

    /*
    ** charging timer periodic launching
    */
    com_tx_tmr_p->p_periodic_timCell=ruc_timer_alloc(0,0);
    if (com_tx_tmr_p->p_periodic_timCell == (struct timer_cell *)NULL){
        severe( "No timer available for MS timer periodic" );
        return(RUC_NOK);
    }
    info("FDLBT start periodic tmr module %d context %p",com_tx_tmr_p->module_idx,com_tx_tmr_p->p_periodic_timCell);
    ruc_periodic_timer_start_th(com_tx_tmr_p->p_periodic_timCell,
	      (com_tx_tmr_p->period_ms*TIMER_TICK_VALUE_100MS/100),
	      &com_tx_tmr_periodic_th,
	      com_tx_tmr_p);
	      
    return(RUC_OK);
}

