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
 
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <fcntl.h> 
#include <sys/un.h>             
#include <errno.h>  
#include <time.h>
#include <rozofs/common/types.h>
#include <rozofs/common/log.h>
#include "ruc_common.h"
#include "ruc_list.h"
#include "af_unix_socket_generic_api.h"
#include "af_unix_socket_generic.h"
#include "rozofs_socket_family.h"
#include "uma_dbg_api.h"
#include "north_lbg_timer.h"
#include "north_lbg_timer_api.h"
#include "north_lbg.h"
#include "north_lbg_api.h"
#include "af_inet_stream_api.h"
#include <rozofs/rozofs_timer_conf.h>
#include "ruc_traffic_shaping.h"


void north_lbg_entry_start_timer(north_lbg_entry_ctx_t *entry_p,uint32_t time_ms) ;
void north_lbg_entry_timeout_CBK (void *opaque);
void north_lbg_entry_stop_timer(north_lbg_entry_ctx_t *pObj);
int north_lbg_attach_app_sup_cbk_on_entries(north_lbg_ctx_t  *lbg_p);

/*__________________________________________________________________________
*/
/**
*  API to display the load balancing group id and its current state

  @param buffer : output buffer
  @param lbg_id : index of the load balancing group
  
  @retval : pointer to the output buffer
*
*/
char *north_lbg_display_lbg_id_and_state(char * pchar,int lbg_id)
{
   char *buffer = pchar;
   if (lbg_id== -1)
   {
      buffer += sprintf(buffer," lbg_id ??? state DOWN ");     
   }
   else
   {
      int lbg_state = north_lbg_get_state(lbg_id);
      switch (lbg_state)
      {
        case NORTH_LBG_UP:
         buffer += sprintf(buffer," lbg_id %3d state UP   ",lbg_id);  
         break; 
         default:
        case NORTH_LBG_DOWN:
         buffer += sprintf(buffer," lbg_id %3d state DOWN ",lbg_id);  
         break;          
      }
   }
   return pchar;
}



/*__________________________________________________________________________
*/
/**
*  API to display the load balancing group id and its current state

  @param buffer : output buffer
  @param lbg_id : index of the load balancing group
  
  @retval : pointer to the output buffer
*
*/
char *north_lbg_display_lbg_state(char * pchar,int lbg_id)
{
   char *buffer = pchar;
   if (lbg_id== -1)
   {
      buffer += sprintf(buffer,"DOWN");     
   }
   else
   {
      int lbg_state = north_lbg_get_state(lbg_id);
      switch (lbg_state)
      {
        case NORTH_LBG_UP:
         buffer += sprintf(buffer,"UP  ");  
         break; 
         default:
        case NORTH_LBG_DOWN:
         buffer += sprintf(buffer,"DOWN");  
         break;          
      }
   }
   return pchar;
}


/*__________________________________________________________________________
*/
/**
* Check if there is some pending buffer in the global pending xmit queue

  @param lbg_p: pointer to a load balancing group
  @param entry_p: pointer to an element of a load balancing group
  @param xmit_credit : number of element that can be removed from the pending xmit list
  
  @retvak none;

*/

void north_lbg_poll_xmit_queue(north_lbg_ctx_t  *lbg_p, north_lbg_entry_ctx_t  *entry_p,int xmit_credit)
{
   ruc_obj_desc_t *pnext;
   int ret;
   int i;
   void *buf_p;

   pnext = (ruc_obj_desc_t*)NULL;
   for (i = 0; i < xmit_credit; i++)
   {
   while ((buf_p = (void*) ruc_objGetNext((ruc_obj_desc_t*)&lbg_p->xmitList[0],
                                        &pnext))
               !=NULL) 
   {         
     ruc_objRemove((ruc_obj_desc_t*)buf_p);
     lbg_p->stats.xmitQueuelen--;
     ret = af_unix_generic_send_stream_with_idx(entry_p->sock_ctx_ref,buf_p);  
     if (ret ==  0)
     {
       lbg_p->stats.totalXmit++;
       entry_p->stats.totalXmit++; 
       continue;
     } 
     /*
     ** there is an error, the interface is down again, so requeue the buffer at the head
     ** do we need to udpate the retry counter of the buffer ??
     */  
     lbg_p->stats.totalXmitError++; 
     lbg_p->stats.xmitQueuelen++;
     ruc_objInsert(&lbg_p->xmitList[0],(ruc_obj_desc_t*) buf_p);
     return;   
   }
   return; 
   }
 }
 
/*__________________________________________________________________________
*/
/**
  Application callBack:

   Called from tcp or af_unix when a buffer has been successfully received

    
  @param userRef : pointer to a load balancer entry
 @param socket_context_ref: socket context reference
 @param bufRef : pointer to the packet buffer on which the error has been encountered
 @param err_no : errno has reported by the sendto().
 
*/
void  north_lbg_userRecvCallBack(void *userRef,uint32_t  socket_ctx_idx, void *bufRef)
{
   north_lbg_entry_ctx_t *entry_p = (north_lbg_entry_ctx_t*)userRef;
   north_lbg_ctx_t       *lbg_p   = (north_lbg_ctx_t*)entry_p->parent;
   /*
   ** update the statistics
   */
   lbg_p->stats.totalRecv++;
   entry_p->stats.totalRecv++;     
   /*
   ** check if there is some message to pull out from the global queue
   */
   north_lbg_poll_xmit_queue((north_lbg_ctx_t*)entry_p->parent,entry_p,1);
   /*
   ** OK now call the application
   */
   (lbg_p->userRcvCallBack)(NULL,lbg_p->index,bufRef);
}

/*__________________________________________________________________________
*/
/**
* test function that is called upon a failure on sending

 The application might use that callback if it has some other
 destination that can be used in case of failure of the current one
 If the application has no other destination to select, it is up to the
 application to release the buffer.
 

 @param userRef : pointer to a load balancer entry
 @param socket_context_ref: socket context reference
 @param bufRef : pointer to the packet buffer on which the error has been encountered
 @param err_no : errno has reported by the sendto().
 
 @retval none
*/

void  north_lbg_userDiscCallBack(void *userRef,uint32_t socket_context_ref,void *bufRef,int err_no)
{
//    int len;
    int ret;
    int8_t retry_counter;
   north_lbg_entry_ctx_t *entry_p = (north_lbg_entry_ctx_t*)userRef;
   north_lbg_ctx_t       *lbg_p   = (north_lbg_ctx_t*)entry_p->parent;
   ruc_obj_desc_t        *pnext = (ruc_obj_desc_t*)NULL;
   int up2down_transition = 0;

    /*
    ** change the state to DOWN
    */
    if (entry_p->state != NORTH_LBG_DOWN) 
    { 
      north_lbg_entry_state_change(entry_p,NORTH_LBG_DOWN);
      warning("north_lbg_userDiscCallBack : LBG %s entry %d \n",lbg_p->name, entry_p->index);
      up2down_transition = 1;
    }
    /*
    **get the pointer to the destination stored in the buffer
    */
    while (bufRef != NULL) 
    {
       /*
       ** get the retry counter of the buffer
       */
       retry_counter = ruc_buf_get_retryCounter(bufRef);
       
       if ((lbg_p->state == NORTH_LBG_DOWN) && (lbg_p->rechain_when_lbg_gets_down == 1)) {
         /* 
	 ** The lbg is down but we re-send on it hoping it will comme back up soon 
	 */
	 if (retry_counter < lbg_p->nb_entries_conf) {
           retry_counter +=1;
           ruc_buf_set_retryCounter(bufRef,retry_counter);
           /*
           ** resend by selecting a new destination
           */
           lbg_p->stats.totalXmitRetries++;
	   //info("rechain on LBG %d",lbg_p->index);
           north_lbg_send(lbg_p->index,bufRef);	   
	   break;
	 }
       }
       
//       if ((retry_counter >= NORTH_LBG_MAX_RETRY) || (lbg_p->state == NORTH_LBG_DOWN))
       if ((retry_counter >= lbg_p->nb_entries_conf) || (lbg_p->state == NORTH_LBG_DOWN))
       {       
         /*
         ** inform the application or release the buffer
         */
         lbg_p->stats.totalXmitAborts++;
         if (lbg_p->userDiscCallBack!= NULL)
         {
         
          (lbg_p->userDiscCallBack)(NULL,lbg_p->index,bufRef, err_no); 
          break;        
         }
         /*
         ** release the buffer
         */
//#warning Need to check the in_use counter of the buffer before the release
         ruc_buf_freeBuffer(bufRef); 
         break;               
       }
       /*
       ** the lbg is still up and there the retry count is not exhausted
       */
       retry_counter +=1;
       ruc_buf_set_retryCounter(bufRef,retry_counter);
       /*
       ** resend by selecting a new destination
       */
       lbg_p->stats.totalXmitRetries++;
       north_lbg_send(lbg_p->index,bufRef);
       break;
    }
    /*
    ** OK, now go the buffer that might be queued on that entry and do the same
    */    
    while ((bufRef = (void*) ruc_objGetNext((ruc_obj_desc_t*)&entry_p->xmitList,
                                         &pnext))
                !=NULL) 
    { 
      /*
      ** remove it from the list because it might be queued afterwards on a new queue
      */        
      ruc_objRemove((ruc_obj_desc_t*)bufRef);
      while (1) 
      {
         /*
         ** get the retry counter of the buffer
         */
         retry_counter = ruc_buf_get_retryCounter(bufRef);
         if ((lbg_p->state == NORTH_LBG_DOWN) && (lbg_p->rechain_when_lbg_gets_down == 1)) {
           /* 
	   ** The lbg is down but we re-send on it hoping it will comme back up soon 
	   */
	   if (retry_counter < lbg_p->nb_entries_conf) {
             retry_counter +=1;
             ruc_buf_set_retryCounter(bufRef,retry_counter);
             /*
             ** resend by selecting a new destination
             */
             lbg_p->stats.totalXmitRetries++;
	     #
	    //info("rechain on LBG %d",lbg_p->index);
             north_lbg_send(lbg_p->index,bufRef);	   
	     break;
	   }
         }
	 	
	 //         if ((retry_counter >= NORTH_LBG_MAX_RETRY) || (lbg_p->state == NORTH_LBG_DOWN))
         if ((retry_counter >= lbg_p->nb_entries_conf) || (lbg_p->state == NORTH_LBG_DOWN))
         {
           /*
           ** inform the application or release the buffer
           */
           lbg_p->stats.totalXmitAborts++;
           if (lbg_p->userDiscCallBack!= NULL)
           {
            (lbg_p->userDiscCallBack)(NULL,lbg_p->index,bufRef, err_no); 
            break;        
           }
           /*
           ** release the buffer
           */
           ruc_buf_freeBuffer(bufRef); 
           break;               
         }
         /*
         ** the lbg is still up and there the retry count is not exhausted
         */
         retry_counter +=1;
         ruc_buf_set_retryCounter(bufRef,retry_counter);
         /*
         ** resend by selecting a new destination
         */
         lbg_p->stats.totalXmitRetries++;
         north_lbg_send(lbg_p->index,bufRef);
         break;
      }   
    }   
    /*
    ** do the reconnect on UP 2 DOWN transition only
    */
  if (up2down_transition) 
  {
    entry_p->stats.totalConnectAttempts++;
    ret = af_unix_sock_client_reconnect(socket_context_ref);
    if (ret < 0)
    {
 //  printf("north_lbg_userDiscCallBack->fatal error on reconnect\n");
      north_lbg_entry_start_timer(entry_p,ROZOFS_TMR_GET(TMR_TCP_FIRST_RECONNECT)); 
    }
  }
}



/*__________________________________________________________________________
*/
/**
* Load Balncing group deletion API

  - delete all the TCP of AF_UNIX conections
  - stop the timer  assoicated with each connection
  - release all the xmit pending buffers associated with the load balancing group 

 @param lbg_id : user ereference of the load balancing group
 
 @retval 0 : success
 @retval < 0  errno (see errno for details)
*/
int  north_lbg_delete(int lbg_id)
{
    int ret;
   north_lbg_entry_ctx_t *entry_p ;
   ruc_obj_desc_t        *pnext = (ruc_obj_desc_t*)NULL;
   north_lbg_ctx_t       *lbg_p;
   int i;
   void *bufRef;
   
   lbg_p = north_lbg_getObjCtx_p(lbg_id);
   if (lbg_p == NULL) 
   {
     errno = EINVAL;
     return -1;
   }

    lbg_p->state = NORTH_LBG_SHUTTING_DOWN;
    /*
    **get the pointer to the destination stored in the buffer
    */

    /*
    ** OK, now go the buffer that might be queued on that entry and do the same
    */
    for (i = 0;  i < lbg_p->nb_entries_conf; i++)
    {
      entry_p = &lbg_p->entry_tb[i];

      /*
      ** stop the timer
      */
      north_lbg_entry_stop_timer(entry_p);      
      /*
      ** delete the TCP or AF_UNIX connection
      */
      ret = af_unix_delete_socket(entry_p->sock_ctx_ref);
      if (ret < 0) severe("failure on af_unix_delete_socket()entry  %d",i);
      entry_p->sock_ctx_ref = -1;
      /*
      ** Purge the buffer that are queued in the xmitlist done of the entry
      */
      while ((bufRef = (void*) ruc_objGetNext((ruc_obj_desc_t*)&entry_p->xmitList,
                                           &pnext))
                  !=NULL) 
      { 
        /*
        ** remove it from the list because it might be queued afterwards on a new queue
        */        
        ruc_objRemove((ruc_obj_desc_t*)bufRef);
        while (1) 
        {	 	 
             if (lbg_p->userDiscCallBack!= NULL)
             {
              (lbg_p->userDiscCallBack)(NULL,lbg_p->index,bufRef, EPIPE); 
              break;        
             }
             /*
             ** release the buffer
             */
             ruc_buf_freeBuffer(bufRef); 
             break;               
        }   
      }
    }
    /*
    ** Purge the pending xmit list of the load balancer
    */
    for (i = 0; i < NORTH_LBG_MAX_PRIO; i++)
    {
      pnext = (ruc_obj_desc_t*)NULL;
      while ((bufRef = (void*) ruc_objGetNext((ruc_obj_desc_t*)&lbg_p->xmitList[i],
                                           &pnext))
                  !=NULL) 
      { 
        /*
        ** remove it from the list because it might be queued afterwards on a new queue
        */        
        ruc_objRemove((ruc_obj_desc_t*)bufRef);
        while (1) 
        {	 	 
             if (lbg_p->userDiscCallBack!= NULL)
             {
              (lbg_p->userDiscCallBack)(NULL,lbg_p->index,bufRef, EPIPE); 
              break;        
             }
             /*
             ** release the buffer
             */
             ruc_buf_freeBuffer(bufRef); 
             break;               
        }   
      }
    }
    /*
    ** release the lbg context
    */
    north_lbg_free_from_ptr(lbg_p);
    return 0;

}

/*__________________________________________________________________________
*/
/**
* that callback is called upon the successful transmission of a buffer
  Depending on the inuse value of the buffer, the application can
  either release the message or queue it in its local xmit queue.
  The main purpose of queueing the message is to address the case of
  a disconnection of the remote end in order to re-balance the buffer
  on another equivalent destinatio
 

 @param userRef : pointer to a load balancer entry
 @param socket_context_ref: socket context reference
 @param bufRef : pointer to the packet buffer on which the error has been encountered
  
 @retval none
*/
void  north_lbg_userXmiDoneCallBack(void *userRef,uint32_t socket_context_ref,void *bufRef)
{
    int8_t inuse;
    
   north_lbg_entry_ctx_t *entry_p = (north_lbg_entry_ctx_t*)userRef;

    /*
    ** check the inuse value of the buffer, if inuse is 1, then release it
    */
    inuse = ruc_buf_inuse_get(bufRef);
    if (inuse < 0)
    {
//#warning  inuse MUST never be negative so EXIT !!!!!
     fatal("fatal error on buffer management: inuse counter %d",inuse );          
    }
    if (inuse == 1) 
    {
       ruc_buf_freeBuffer(bufRef);  
    } 
    else
    {
      /*
      ** queue it to the local xmit list of the entry 
      */
      ruc_objInsertTail((ruc_obj_desc_t*)&entry_p->xmitList,(ruc_obj_desc_t*)bufRef);    
    } 
}

/*__________________________________________________________________________
*/
/**
*   Application connect callback
    The retcode contains the result of the connect() operation started 
    in asynchronous mode. That value is one of the following:
    
    - RUC_OK : success
    - RUC_NOK : failure(see errnum for details

 @param userRef : pointer to a load balancer entry
 @param socket_context_ref : index of the socket context
 @param retcode : connection status
 @param errnum : value of the errno in case of error
 
 @retval none
*/
void north_lbg_connect_cbk (void *userRef,uint32_t socket_context_ref,int retcode,int errnum)
{
   north_lbg_entry_ctx_t *entry_p = (north_lbg_entry_ctx_t*)userRef;
   uint8_t fake_buf[16];
   int len;
   int status;
   /*
   ** in case of success starts sending the messages
   */
   if (retcode != RUC_OK)
   {
//     printf("error on connect() %s\n",strerror(errnum));
     /*
     ** restart the delay timer and then retry
     */
     north_lbg_entry_start_timer(entry_p,ROZOFS_TMR_GET(TMR_TCP_RECONNECT));
     return;    
   }
   /**
   * do a fake read to be sure that the socket is up
   */
   af_unix_ctx_generic_t *sock_p = af_unix_getObjCtx_p(socket_context_ref);
    
   status = af_unix_recv_stream_sock_recv(sock_p,fake_buf,2,MSG_PEEK,&len);
   switch(status)
   {
     case RUC_OK:   
     case RUC_WOULDBLOCK:
     case RUC_PARTIAL:
      north_lbg_entry_stop_timer(entry_p);
      sock_p->stats.totalUpDownTransition++;
      north_lbg_entry_state_change(entry_p,NORTH_LBG_UP);
      /*
      ** attempt the fill the xmit queue of that entry
      */
      north_lbg_poll_xmit_queue((north_lbg_ctx_t*)entry_p->parent,entry_p,NORTH_LBG_MAX_XMIT_ON_UP_TRANSITION+2);
      
//      entry_p->state = NORTH_LBG_UP ;
//      entry_p->stats.totalUpDownTransition++;
//      printf("Successful reconnection!!!\n");  
      return;
      
     case RUC_DISC:
     default:
       /*
       ** still deconnected-> remove from socket controller and restart the timer
       */
       af_unix_disconnect_from_socketCtrl(socket_context_ref);
        north_lbg_entry_start_timer(entry_p,ROZOFS_TMR_GET(TMR_TCP_RECONNECT));
   }
}

/*
**____________________________________________________
*/
/*
    Timeout call back associated with a transaction

@param     :  tx_p : pointer to the transaction context
*/

void north_lbg_entry_timeout_CBK (void *opaque)
{
  int ret;  
  north_lbg_entry_ctx_t *entry_p = (north_lbg_entry_ctx_t*)opaque;
//  entry_p->rpc_guard_timer_flg = TRUE;

   entry_p->stats.totalConnectAttempts++;
   ret = af_unix_sock_client_reconnect(entry_p->sock_ctx_ref);
   entry_p->last_reconnect_time = time(NULL); // Save time when we reconnect 
   if (ret < 0)
   {
//      printf("north_lbg_entry_timeout_CBK-->fatal error on reconnect\n");
      /*
      ** restart the timer
      */
      north_lbg_entry_start_timer(entry_p,ROZOFS_TMR_GET(TMR_TCP_RECONNECT));
   }
}
/*
**____________________________________________________
*/
/*
  stop the guard timer associated with the transaction

@param     :  entry_p : pointer to the transaction context
@retval   : none
*/

void north_lbg_entry_stop_timer(north_lbg_entry_ctx_t *pObj)
{
 
//  pObj->rpc_guard_timer_flg = FALSE;
  north_lbg_tmr_stop(&pObj->rpc_guard_timer); 
}

/*
**____________________________________________________
*/
/*
  start the guard timer associated with the transaction

@param     : entry_p : pointer to the transaction context
@param     : uint32_t  : delay in seconds (??)
@retval   : none
*/
void north_lbg_entry_start_timer(north_lbg_entry_ctx_t *entry_p,uint32_t time_ms) 
{
 uint8_t slot;
 time_t delay;
  /*
  **  remove the timer from its current list
  */
  slot = NORTH_LBG_TMR_SLOT0;

//  entry_p->rpc_guard_timer_flg = FALSE;
  north_lbg_tmr_stop(&entry_p->rpc_guard_timer);
  
  // Take into account the last reconnect attemp time 
  // to adjust timer duration in order to reconnect 
  // every time_ms
  delay = (time(NULL) - entry_p->last_reconnect_time)*1024;
  time_ms *= 1024;
  if (delay >= 0) {
    if   (delay < time_ms) time_ms -= delay;
    else                   time_ms = 100;
  }
  north_lbg_tmr_start(slot,
                  &entry_p->rpc_guard_timer,
		  time_ms,
                  north_lbg_entry_timeout_CBK,
		  (void*) entry_p);

}


/*__________________________________________________________________________
*/
/**
*  create a north load balancing object

  @param @name : name of the load balancer
  @param  basename_p : Base name of the remote sunpath
  @param @family of the load balancer
  @param first_instance: index of the first instance
  @param nb_instances: number of instances
  
  @retval >= reference of the load balancer object
  @retval < 0 error (out of context ??)
*/
int north_lbg_create_af_unix(char *name,char *basename_p,int family,int first_instance,int  nb_instances,af_unix_socket_conf_t *usr_conf_p)
{
  char sun_path[128];
  char nickname[128];
  north_lbg_ctx_t  *lbg_p;
  int    i;
  north_lbg_entry_ctx_t *entry_p;
  af_unix_socket_conf_t *conf_p; 
  
  if (nb_instances == 0)
  {
    /*
    ** no instances!!
    */
    warning("north_lbg_create_af_unix: no instances");
    return -1;   
  }
  if (nb_instances >= NORTH__LBG_MAX_ENTRY)
  {
    /*
    ** to many instances!!
    */
    warning("north_lbg_create_af_unix: to many instances : %d max %d ",nb_instances,NORTH__LBG_MAX_ENTRY);
    return -1;   
  }  
  /*
  ** allocate a load balancer context
  */
  lbg_p = north_lbg_alloc();
  if (lbg_p == NULL) 
  {
    /*
    ** out of context
    */
    fatal("north_lbg_create_af_unix: out of load balancing group context");
    return -1; 
  }
  lbg_p->family = family;
  lbg_p->nb_entries_conf = nb_instances;
  strcpy(lbg_p->name,name);
  /*
  ** save the configuration of the lbg
  */
  conf_p = &lbg_p->lbg_conf;
  memcpy(conf_p,usr_conf_p,sizeof(af_unix_socket_conf_t));

  /*
  ** install the load balancer callback but keep in the context the 
  ** user callback for deconnection
  */
  lbg_p->userDiscCallBack       = conf_p->userDiscCallBack;
  conf_p->userDiscCallBack    = north_lbg_userDiscCallBack;
  lbg_p->userRcvCallBack      = conf_p->userRcvCallBack;
  conf_p->userRcvCallBack     = north_lbg_userRecvCallBack;
  conf_p->userConnectCallBack = north_lbg_connect_cbk;
  conf_p->userXmitDoneCallBack = north_lbg_userXmiDoneCallBack;
  
  entry_p = lbg_p->entry_tb;
  for (i = 0; i < nb_instances ; i++,entry_p++)
  {
     sprintf(sun_path,"%s_inst_%d",basename_p,i+first_instance);
     sprintf(nickname,"%s_%d",name,i+first_instance);
     conf_p->instance_id = i+first_instance;
     conf_p->userRef     = entry_p;
     entry_p->sock_ctx_ref = af_unix_sock_client_create(nickname,sun_path,conf_p); 
     if (entry_p->sock_ctx_ref >= 0)  
     {
       north_lbg_entry_start_timer(entry_p,ROZOFS_TMR_GET(TMR_TCP_RECONNECT));
       north_lbg_entry_state_change(entry_p,NORTH_LBG_DOWN);
//       entry_p->state = NORTH_LBG_DOWN; 
     }  
  }
  /*
  ** attach the application callback if any is declared
  */
  north_lbg_attach_app_sup_cbk_on_entries(lbg_p);
  
  return (lbg_p->index);
}





/*__________________________________________________________________________
*/
/**
*  create a north load balancing object with AF_INET

  @param @name : name of the load balancer
  @param  basename_p : Base name of the remote sunpath
  @param @family of the load balancer
  
  @retval >= reference of the load balancer object
  @retval < 0 error (out of context ??)
*/
int north_lbg_create_af_inet(char *name,
                             uint32_t src_ipaddr_host,
                             uint16_t src_port_host,
                             north_remote_ip_list_t *remote_ip_p,
                             int family,int  nb_instances,af_unix_socket_conf_t *usr_conf_p)
{
  north_lbg_ctx_t  *lbg_p;
  int    i;
  north_lbg_entry_ctx_t *entry_p;  
  af_unix_socket_conf_t *conf_p; 
   
  if (nb_instances == 0)
  {
    /*
    ** no instances!!
    */
    warning("north_lbg_create_af_inet: no instances");
    return -1;   
  }
  if (nb_instances >= NORTH__LBG_MAX_ENTRY)
  {
    /*
    ** to many instances!!
    */
    warning("north_lbg_create_af_inet: to many instances : %d max %d ",nb_instances,NORTH__LBG_MAX_ENTRY);
    return -1;   
  }  
  /*
  ** allocate a load balancer context
  */
  lbg_p = north_lbg_alloc();
  if (lbg_p == NULL) 
  {
    /*
    ** out of context
    */
    fatal("north_lbg_create_af_inet: out of load balancing group context");
    return -1; 
  }
  /*
  ** save the configuration of the lbg
  */
  conf_p = &lbg_p->lbg_conf;
  memcpy(conf_p,usr_conf_p,sizeof(af_unix_socket_conf_t));
  
  lbg_p->family = family;
  lbg_p->nb_entries_conf = nb_instances;
  strcpy(lbg_p->name,name);
  /*
  ** install the  callbacks of the load balancer
  */
  /*
  ** install the load balancer callback but keep in the context the 
  ** user callback for deconnection
  */
  lbg_p->userDiscCallBack     = conf_p->userDiscCallBack;
  conf_p->userDiscCallBack    = north_lbg_userDiscCallBack;
  lbg_p->userRcvCallBack      = conf_p->userRcvCallBack;
  conf_p->userRcvCallBack     = north_lbg_userRecvCallBack;

  conf_p->userConnectCallBack = north_lbg_connect_cbk;
  conf_p->userXmitDoneCallBack = north_lbg_userXmiDoneCallBack;

  
  entry_p = lbg_p->entry_tb;
  for (i = 0; i < nb_instances ; i++,entry_p++,remote_ip_p++)
  {

     conf_p->userRef     = entry_p;
     entry_p->sock_ctx_ref = af_inet_sock_client_create(name,
                                                     src_ipaddr_host,
                                                     src_port_host,
                                                     remote_ip_p->remote_ipaddr_host,
                                                     remote_ip_p->remote_port_host,
                                                     conf_p); 
     if (entry_p->sock_ctx_ref >= 0)  
     {
       north_lbg_entry_start_timer(entry_p,ROZOFS_TMR_GET(TMR_TCP_RECONNECT));
       north_lbg_entry_state_change(entry_p,NORTH_LBG_DOWN);
//       entry_p->state = NORTH_LBG_DOWN; 
     }  
  }
  /*
  ** attach the application callback if any is declared
  */
  north_lbg_attach_app_sup_cbk_on_entries(lbg_p);
  
  return (lbg_p->index);
}




/**
*  API that provide the current state of a load balancing Group

 @param lbg_idx : index of the load balancing group
 
 @retval    NORTH_LBG_DEPENDENCY : lbg is idle or the reference is out of range
 @retval    NORTH_LBG_UP : at least one connection is UP
 @retval    NORTH_LBG_DOWN : all the connection are down
*/
int north_lbg_get_state(int lbg_idx)
{

  north_lbg_ctx_t  *lbg_p;


  lbg_p = north_lbg_getObjCtx_p(lbg_idx);
  if (lbg_p == NULL) 
  {
    //warning("north_lbg_get_state: no such instance %d ",lbg_idx);
    return NORTH_LBG_DEPENDENCY;
  }
  return lbg_p->state;

}



/**
*  API that provide the current state of a load balancing Group

 @param lbg_idx : index of the load balancing group
 
 @retval   1 : available
 @retval   0 : unavailable
*/
int north_lbg_is_available(int lbg_idx)
{

  north_lbg_ctx_t  *lbg_p;

  lbg_p = north_lbg_getObjCtx_p(lbg_idx);
  if (lbg_p == NULL) 
  {
    //warning("north_lbg_is_available: no such instance %d ",lbg_idx);
    return 0;
  }
  if (lbg_p->state != NORTH_LBG_UP) return 0;
  return lbg_p->available_state;
}
  



/**
*  API that provide the current state of a load balancing Group

 @param lbg_idx : index of the load balancing group
 
 @retval   none
*/
void north_lbg_update_available_state(uint32_t lbg_idx)
{

  north_lbg_ctx_t  *lbg_p;
  
  lbg_p = north_lbg_getObjCtx_p(lbg_idx);
  if (lbg_p == NULL) 
  {
    warning("north_lbg_update_available_state: no such instance %d ",lbg_idx);
    return;
  }
  if (lbg_p->userPollingCallBack == NULL) return;
  
  int    i;
  north_lbg_entry_ctx_t *entry_p;
  af_unix_ctx_generic_t *sock_p ;
 
  /* configure each stream connection */
  entry_p = lbg_p->entry_tb;
  for (i = 0; i < lbg_p->nb_entries_conf ; i++,entry_p++) {
     /*
     ** get the reference from idx
     */
     sock_p = af_unix_getObjCtx_p(entry_p->sock_ctx_ref);
     if (sock_p == NULL)
     {
        continue;
     }
     if (sock_p->cnx_availability_state == AF_UNIX_CNX_AVAILABLE)
     {
       lbg_p->available_state = 1;
       /*
       ** for the future when we will move the supervision contexts in the load balancer
       ** we change the state of the global availability of the LBG to STORCLI_LBG_RUNNING
       ** Up to now all that code is in storcli.
       */
       
       return;
     }
   }
   lbg_p->available_state = 0;
}  
  
 /**
*  API to allocate a  load balancing Group context with no configuration 
  
  Once the context is allocated, the state of the object is set to NORTH_LBG_DEPENDENCY.

 @param none
 
  @retval >= reference of the load balancer object
  @retval < 0 error (out of context ??)
*/
int north_lbg_create_no_conf()
{

  north_lbg_ctx_t  *lbg_p;
   
  /*
  ** allocate a load balancer context
  */
  lbg_p = north_lbg_alloc();
  if (lbg_p == NULL) 
  {
    /*
    ** out of context
    */
    warning("north_lbg_create_no_conf: out of load balancing context");
    return -1; 
  }

  lbg_p->state  = NORTH_LBG_DEPENDENCY;
  return (lbg_p->index);
}
 
/*__________________________________________________________________________
*/ 
 /**
*  API to configure a load balancing group.
   The load balancing group must have been created previously with north_lbg_create_no_conf() 
  
 @param none
 
  @retval >= reference of the load balancer object
  @retval < 0 error (out of context ??)
*/
int north_lbg_configure_af_inet(int lbg_idx,char *name,
                                uint32_t src_ipaddr_host,
                                uint16_t src_port_host,
                                north_remote_ip_list_t *remote_ip_p,
                                int family,int  nb_instances,af_unix_socket_conf_t *usr_conf_p, int local)
{
  north_lbg_ctx_t  *lbg_p;
  int    i;
  north_lbg_entry_ctx_t *entry_p;
  
  af_unix_socket_conf_t *conf_p;
  
  lbg_p = north_lbg_getObjCtx_p(lbg_idx);
  if (lbg_p == NULL) 
  {
    warning("north_lbg_configure_af_inet: no such instance %d ",lbg_idx);
    return -1;
  }
  
  lbg_p->local = local;  
  conf_p = &lbg_p->lbg_conf;
  memcpy(conf_p,usr_conf_p,sizeof(af_unix_socket_conf_t));

  if (lbg_p->state != NORTH_LBG_DEPENDENCY)
  {
    warning("north_lbg_configure_af_inet: unexpected state %d ",lbg_p->state);
    return -1;  
  }
  if (nb_instances == 0)
  {
    /*
    ** no instances!!
    */
    warning("north_lbg_configure_af_inet: no instance for lbg %d ",lbg_idx);
    return -1;   
  }
  if (nb_instances >= NORTH__LBG_MAX_ENTRY)
  {
    /*
    ** to many instances!!
    */
    warning("north_lbg_configure_af_inet: too many instances (%d max %d) for lbg %d ",nb_instances,NORTH__LBG_MAX_ENTRY,lbg_idx);
    return -1;   
  }  
  /*
  ** restore the init state
  */
  lbg_p->state  = NORTH_LBG_DOWN;
  
  lbg_p->family = family;
  lbg_p->nb_entries_conf = nb_instances;
  strcpy(lbg_p->name,name);
  /*
  ** install the  callbacks of the load balancer
  */
  /*
  ** install the load balancer callback but keep in the context the 
  ** user callback for deconnection
  */
  lbg_p->userDiscCallBack     = conf_p->userDiscCallBack;
  conf_p->userDiscCallBack    = north_lbg_userDiscCallBack;
  lbg_p->userRcvCallBack      = conf_p->userRcvCallBack;
  conf_p->userRcvCallBack     = north_lbg_userRecvCallBack;

  conf_p->userConnectCallBack = north_lbg_connect_cbk;
  conf_p->userXmitDoneCallBack = north_lbg_userXmiDoneCallBack;

  
  entry_p = lbg_p->entry_tb;
  for (i = 0; i < nb_instances ; i++,entry_p++,remote_ip_p++)
  {

     conf_p->userRef     = entry_p;
     entry_p->sock_ctx_ref = af_inet_sock_client_create(name,
                                                     src_ipaddr_host,
                                                     src_port_host,
                                                     remote_ip_p->remote_ipaddr_host,
                                                     remote_ip_p->remote_port_host,
                                                     conf_p); 
     if (entry_p->sock_ctx_ref >= 0)  
     {
       north_lbg_entry_start_timer(entry_p,ROZOFS_TMR_GET(TMR_TCP_RECONNECT));
       north_lbg_entry_state_change(entry_p,NORTH_LBG_DOWN);
//       entry_p->state = NORTH_LBG_DOWN; 
     }  
  }
  /*
  ** attach the application callback if any is declared
  */
  north_lbg_attach_app_sup_cbk_on_entries(lbg_p);
  
  return (lbg_p->index);

}

/*__________________________________________________________________________
*/ 
 /**
*  API to configure a load balancing group.
   The load balancing group must have been created previously with north_lbg_create_no_conf() 
  
 @param lbg_idx             Index of the load balancing group
 @param next_global_entry_idx_p Pointer to the next lbg entry to select
 
  @retval >= reference of the load balancer object
  @retval < 0 error (out of context ??)
*/
int north_lbg_set_next_global_entry_idx_p(int lbg_idx, int * next_global_entry_idx_p)
{
  north_lbg_ctx_t  *lbg_p;
  
  lbg_p = north_lbg_getObjCtx_p(lbg_idx);
  if (lbg_p == NULL) 
  {
    warning("north_lbg_set_next_global_entry_idx_p: no such instance %d ",lbg_idx);
    return -1;
  }

  lbg_p->next_global_entry_idx_p = next_global_entry_idx_p;
  * next_global_entry_idx_p = 0;

  return 0;

} 

/*__________________________________________________________________________
*/ 
 /**
*  API to re-configure the destination ports of a load balancing group.
   The load balancing group must have been configured previously with north_lbg_configure_af_inet() 
  
 @param lbg_idx index of the load balancing group
 @param remote_ip_p table of new destination ports
 @param nb_instances number of instances in remote_ip_p table
 
  @retval -1 when LBG reference does not exist
  @retval 0 else
*/
int north_lbg_re_configure_af_inet_destination_port(int lbg_idx,north_remote_ip_list_t *remote_ip_p, int  nb_instances)
{
  north_lbg_ctx_t  *lbg_p;
  int    i;
  north_lbg_entry_ctx_t *entry_p;
    
  lbg_p = north_lbg_getObjCtx_p(lbg_idx);
  if (lbg_p == NULL) 
  {
    severe("north_lbg_re_configure_af_inet_destination_port bad lbg idx %d", lbg_idx);
    return -1;
  }
  /* Do not change the number of instance */  
  if (nb_instances != lbg_p->nb_entries_conf)
  {
    /*
    ** no instances!!
    */
    severe("north_lbg_re_configure_af_inet_destination_port %d instances while waiting for %d",
                    nb_instances, lbg_p->nb_entries_conf);
    return -1;   
  }

  /* Modify the destination ports of each entrie */
  entry_p = lbg_p->entry_tb;
  for (i = 0; i < nb_instances ; i++,entry_p++) {
    af_inet_sock_client_modify_destination_port(entry_p->sock_ctx_ref, remote_ip_p[i].remote_port_host); 
  }
  return 0;
}
/*
**__________________________________________________________________________
*/
/**
*  Attach the application call back on each entries of the lbg
  Note : the callback must be previously stored in the lbg context

 @param lbg_p : context of the load balancing group
 
   retval 0 : success
  retval -1 : error  
*/
int north_lbg_attach_app_sup_cbk_on_entries(north_lbg_ctx_t  *lbg_p)
{
  int    i;
  north_lbg_entry_ctx_t *entry_p;
  af_unix_ctx_generic_t *sock_p ;
  
  if (lbg_p->userPollingCallBack == NULL) return 0;
  
  /* configure each stream connection */
  entry_p = lbg_p->entry_tb;
  for (i = 0; i < lbg_p->nb_entries_conf ; i++,entry_p++) {
   /*
   ** get the reference from idx
   */
   sock_p = af_unix_getObjCtx_p(entry_p->sock_ctx_ref);
   if (sock_p == NULL)
   {
     /*
     ** socket reference is out of range
     */
     severe("north_lbg_attach_app_sup_cbk_on_entries socket index %d does not exist",entry_p->sock_ctx_ref);
     return -1;
   }
    af_inet_attach_application_supervision_callback(sock_p,lbg_p->userPollingCallBack, lbg_p->active_standby_mode);
    /*
    ** attach the callabck of the lbg for availability supervision
    */
    af_inet_attach_application_availability_callback(sock_p,north_lbg_update_available_state,lbg_p->index);

  }
  return 0;
}

/*
**__________________________________________________________________________
*/
/**
*  Attach a supervision Application callback with the load balancing group
   That callback is configured on each entry of the LBG
   
   @param lbg_idx: reference of the load balancing group
   @param supervision_callback supervision_callback

  retval 0 : success
  retval -1 : error
*/
int  north_lbg_attach_application_supervision_callback(int lbg_idx,af_stream_poll_CBK_t supervision_callback)
{
  north_lbg_ctx_t  *lbg_p;
  int ret;
    
  lbg_p = north_lbg_getObjCtx_p(lbg_idx);
  if (lbg_p == NULL) 
  {
    severe("north_lbg_attach_application_supervision_callback bad lbg idx %d", lbg_idx);
    return -1;
  }
  /*
  ** save the reference of the callback in the lbg context
  */
  lbg_p->userPollingCallBack = supervision_callback;
  if (lbg_p->nb_entries_conf== 0) return 0;
  ret = north_lbg_attach_app_sup_cbk_on_entries(lbg_p);
  return ret;

}

/*
**__________________________________________________________________________
*/
/**
*  Configure the TMO of the application for connexion supervision
   
   @param lbg_idx: reference of the load balancing group
   @param tmo_sec : timeout value

  retval 0 : success
  retval -1 : error
*/
int  north_lbg_set_application_tmo4supervision(int lbg_idx,int tmo_sec)
{
  north_lbg_ctx_t  *lbg_p;
    
  lbg_p = north_lbg_getObjCtx_p(lbg_idx);
  if (lbg_p == NULL) 
  {
    severe("north_lbg_set_application_tmo4supervision bad lbg idx %d", lbg_idx);
    return -1;
  }
  /*
  ** save the reference of the callback in the lbg context
  */
  lbg_p->tmo_supervision_in_sec = tmo_sec;
  return 0;

}

/*__________________________________________________________________________
*/
/**
*  create a north load balancing object with AF_INET

  @param lbg_idx : reference of the load balancing group
  @param buf_p: pointer to the buffer to send
  
  retval 0 : success
  retval -1 : error
*/
int north_lbg_send(int  lbg_idx,void *buf_p)
{                             
  north_lbg_ctx_t  *lbg_p;
  int entry_idx;
  int ret = 0;

  /*
  ** remove the buffer from any pending list
  */
  ruc_objRemove((ruc_obj_desc_t*)buf_p);
  
  lbg_p = north_lbg_getObjCtx_p(lbg_idx);
  if (lbg_p == NULL) 
  {
    severe("north_lbg_send: no such instance %d ",lbg_idx);
    return -1;
  }
  if ((lbg_p->state == NORTH_LBG_SHUTTING_DOWN) || (lbg_p->free == TRUE))
  {
     return -1;
  }
reloop:
  /*
  ** we have the context, search for a valid entry
  */
  if (lbg_p->state == NORTH_LBG_DOWN)
  {
    /*
    ** Insert the buffer in the global pending list of the load balancing froup
    */
    ruc_objInsertTail((ruc_obj_desc_t*)&lbg_p->xmitList[0],(ruc_obj_desc_t*)buf_p);  
    /*
    ** update statistics
    */
    lbg_p->stats.xmitQueuelen++;     
    return 0;  
  }
  /*
  ** check if the main queue is empty if the queue is not empty just queue our message
  ** at the tail of the load balancer main queue
  */
  if (!ruc_objIsEmptyList((ruc_obj_desc_t*)&lbg_p->xmitList[0]))
  {
     /*
     ** queue the message at the tail
     */
     ruc_objInsertTail((ruc_obj_desc_t*)&lbg_p->xmitList[0],(ruc_obj_desc_t*)buf_p);  
    /*
    ** update statistics
    */
    lbg_p->stats.xmitQueuelen++;     
    return 0;  
  }
  
  
  /*
  ** Check whether this LBG is in active/standby mode
  ** in this case send the message on the active connection
  */
  if (lbg_p->active_standby_mode == 1) {
  
    /*
    ** No actibve entry elected. The LBG should nbe down !!!
    */
    if (lbg_p->active_lbg_entry < 0) {
      /*
      ** queue the message at the tail
      */
      ruc_objInsertTail((ruc_obj_desc_t*)&lbg_p->xmitList[0],(ruc_obj_desc_t*)buf_p);  
      /*
      ** update statistics
      */
      lbg_p->stats.xmitQueuelen++;     
      return 0;  
    }
    
    /*
    ** Get the active socket to send on
    */
    entry_idx = lbg_p->active_lbg_entry;
  }
  
  else {

     /*
     ** OK there is at least one entry that is free, so get the next valid entry
     */
     entry_idx = north_lbg_get_next_valid_entry(lbg_p);
     if (entry_idx < 0)
     {
       /*
       ** that situation must not occur since there is at leat one entry that is UP!!!!
       */
   //    RUC_WARNING(-1);
       return -1;    
     }
     
  }   
  /*
  ** That's fine, get the pointer to the entry in order to get its socket context reference
  */
  north_lbg_entry_ctx_t  *entry_p = &lbg_p->entry_tb[entry_idx];
  NORTH_LBG_START_PROF((&entry_p->stats));
  ret = af_unix_generic_send_stream_with_idx(entry_p->sock_ctx_ref,buf_p); 
  NORTH_LBG_STOP_PROF((&entry_p->stats));
  if (ret == 0)
  {
    /*
    ** set the timer to supervise the connection (it only affects client connections)
    */
    if (lbg_p->userPollingCallBack != NULL)
    {
      af_unix_ctx_generic_t *this = af_unix_getObjCtx_p(entry_p->sock_ctx_ref);
      af_inet_enable_cnx_supervision(this);
      af_inet_set_cnx_tmo(this,lbg_p->tmo_supervision_in_sec*10*5);
    }
    lbg_p->stats.totalXmit++; 
    entry_p->stats.totalXmit++;     
    return 0; 
  } 
  /*
  ** retry on a next entry
  ** we might need to update the retry counter of the buffer ??
  */
  lbg_p->stats.totalXmitError++; 
  goto reloop;
  
  return 0;
}

/*__________________________________________________________________________
*/

int north_lbg_send_from_shaper(int  lbg_idx,void *buf_p)
{

  north_lbg_ctx_t  *lbg_p;
  int entry_idx;
  int ret = 0;
  /*
  ** remove the buffer from any pending list
  */
  ruc_objRemove((ruc_obj_desc_t*)buf_p);
  
  lbg_p = north_lbg_getObjCtx_p(lbg_idx);
  if (lbg_p == NULL) 
  {
    severe("north_lbg_send: no such instance %d ",lbg_idx);
    return -1;
  }
  /*
  ** OK there is at least one entry that is free, so get the next valid entry
  */
reloop:
  entry_idx = north_lbg_get_next_valid_entry(lbg_p);
  if (entry_idx < 0)
  {
    /*
    ** that situation must not occur since there is at leat one entry that is UP!!!!
    */
//    RUC_WARNING(-1);
    return -1;    
  }
  /*
  ** That's fine, get the pointer to the entry in order to get its socket context reference
  */
  north_lbg_entry_ctx_t  *entry_p = &lbg_p->entry_tb[entry_idx];
  NORTH_LBG_START_PROF((&entry_p->stats));
  ret = af_unix_generic_send_stream_with_idx(entry_p->sock_ctx_ref,buf_p); 
  NORTH_LBG_STOP_PROF((&entry_p->stats));
  if (ret == 0)
  {
    /*
    ** set the timer to supervise the connection (it only affects client connections)
    */
    if (lbg_p->userPollingCallBack != NULL)
    {
      af_unix_ctx_generic_t *this = af_unix_getObjCtx_p(entry_p->sock_ctx_ref);
      af_inet_enable_cnx_supervision(this);
      af_inet_set_cnx_tmo(this,lbg_p->tmo_supervision_in_sec*10*5);
    }
    lbg_p->stats.totalXmit++; 
    entry_p->stats.totalXmit++;     
    return 0; 
  } 
  /*
  ** retry on a next entry
  ** we might need to update the retry counter of the buffer ??
  */
  lbg_p->stats.totalXmitError++; 
  goto reloop;
  
  return 0;
}


/*__________________________________________________________________________
*/
/**
*  create a north load balancing object with AF_INET

  @param lbg_idx : reference of the load balancing group
  @param buf_p: pointer to the buffer to send
  @param rsp_size : expected response size in byte
  @param disk_time: estimated disk_time in us
  
  retval 0 : success
  retval -1 : error
*/
int north_lbg_send_with_shaping(int  lbg_idx,void *buf_p,uint32_t rsp_size,uint32_t disk_time)
{                             
  north_lbg_ctx_t  *lbg_p;
  int ret = 0;


  lbg_p = north_lbg_getObjCtx_p(lbg_idx);
  if (lbg_p == NULL) 
  {
    severe("north_lbg_send: no such instance %d ",lbg_idx);
    return -1;
  }
  if ((lbg_p->state == NORTH_LBG_SHUTTING_DOWN) || (lbg_p->free == TRUE))
  {
     return -1;
  }
  /*
  ** we have the context, search for a valid entry
  */
  if (lbg_p->state == NORTH_LBG_DOWN)
  {
    /*
    ** Insert the buffer in the global pending list of the load balancing froup
    */
    ruc_objInsertTail((ruc_obj_desc_t*)&lbg_p->xmitList[0],(ruc_obj_desc_t*)buf_p);  
    /*
    ** update statistics
    */
    lbg_p->stats.xmitQueuelen++;     
    return 0;  
  }
  /*
  ** check if the main queue is empty if the queue is not empty just queue our message
  ** at the tail of the load balancer main queue
  */
  if (!ruc_objIsEmptyList((ruc_obj_desc_t*)&lbg_p->xmitList[0]))
  {
     /*
     ** queue the message at the tail
     */
     ruc_objInsertTail((ruc_obj_desc_t*)&lbg_p->xmitList[0],(ruc_obj_desc_t*)buf_p);  
    /*
    ** update statistics
    */
    lbg_p->stats.xmitQueuelen++;     
    return 0;  
  }
  /*
  ** call the traffic shaper
  */
  ret = trshape_queue_buf(0,buf_p,rsp_size,disk_time,north_lbg_send_from_shaper,lbg_idx);
  return ret;
}
/*__________________________________________________________________________
*/
/**
*  Tells whether the lbg target is local to this server or not

  @param entry_idx : index of the entry that must be set
  @param *p  : pointer to the bitmap array

  @retval none
*/
int north_lbg_is_local(int  lbg_idx)
{
  north_lbg_ctx_t  *lbg_p;
  
  lbg_p = north_lbg_getObjCtx_p(lbg_idx);
  if (lbg_p == NULL) 
  {
    warning("north_lbg_is_local: no such instance %d ",lbg_idx);
    return 0;
  }
  return lbg_p->local;
}
/*__________________________________________________________________________
*/
/**
*  Set the lbg entry to use when sending

  @param lbg_idx : reference of the load balancing group


  @retval none
*/
void north_lbg_set_active_entry(int  lbg_idx, int sock_idx_in_lbg)
{
  north_lbg_ctx_t  *lbg_p;
  
  lbg_p = north_lbg_getObjCtx_p(lbg_idx);
  if (lbg_p == NULL) 
  {
    severe("north_lbg_set_active_entry: no such instance %d ",lbg_idx);
    return;
  }
  //info("JPM lbg %d %s Set active %d",lbg_idx,lbg_p->name,sock_idx_in_lbg);
  
  lbg_p->active_lbg_entry = sock_idx_in_lbg;
  
  /*
  ** When the socket becomes active, stop the polling
  */ 
  if (lbg_p->active_lbg_entry != -1) {
    af_unix_ctx_generic_t *sock_p = af_unix_getObjCtx_p(lbg_p->entry_tb[sock_idx_in_lbg].sock_ctx_ref);
    if (sock_p == NULL)
    {
      severe("north_lbg_set_active_entry: no such socket %d in lbg %d entry %d",
              lbg_p->entry_tb[sock_idx_in_lbg].sock_ctx_ref,
	      lbg_idx,sock_idx_in_lbg);
      return;
    }    
    sock_p->cnx_supevision.s.check_cnx_rq = 0;
  }
  return;  
}
/*__________________________________________________________________________
*/
/**
*  Get the lbg entry to use when sending

  @param lbg_idx : reference of the load balancing group


  @retval none
*/
int north_lbg_get_active_entry(int  lbg_idx)
{
  north_lbg_ctx_t  *lbg_p;
  
  lbg_p = north_lbg_getObjCtx_p(lbg_idx);
  if (lbg_p == NULL) 
  {
    warning("north_lbg_get_active_entry: no such instance %d ",lbg_idx);
    return -1;
  }
  //info("JPM lbg %d %s Get active %d",lbg_idx,lbg_p->name,lbg_p->active_lbg_entry);

  return lbg_p->active_lbg_entry;
}
/*__________________________________________________________________________
*/
/**
*  Set the lbg mode in active/standby

  @param lbg_idx : reference of the load balancing group


  @retval none
*/
void north_lbg_set_active_standby_mode(int  lbg_idx)
{
  north_lbg_ctx_t  *lbg_p;
  
  lbg_p = north_lbg_getObjCtx_p(lbg_idx);
  if (lbg_p == NULL) 
  {
    warning("north_lbg_set_active_standby_mode: no such instance %d ",lbg_idx);
    return;
  }

  lbg_p->active_standby_mode = 1;
}
/*__________________________________________________________________________
*/
/**
*  Set the lbg mode in active/standby

  @param lbg_idx : reference of the load balancing group


  @retval none
*/
int north_lbg_get_active_standby_mode(int  lbg_idx)
{
  north_lbg_ctx_t  *lbg_p;
  
  lbg_p = north_lbg_getObjCtx_p(lbg_idx);
  if (lbg_p == NULL) 
  {
    return 0;
  }

  return lbg_p->active_standby_mode;
}


/*__________________________________________________________________________
*/
/**
*  Get the IP@ of active entry of the lbg

  @param lbg_idx : reference of the load balancing group


  @retval != 0 IP@ of the active export
  @retval == 0 no IP@
*/
uint32_t north_lbg_get_remote_ip_address(int  lbg_idx)
{
  north_lbg_ctx_t  *lbg_p;
  
  lbg_p = north_lbg_getObjCtx_p(lbg_idx);
  if (lbg_p == NULL) 
  {
    return 0;
  }

  if (lbg_p->active_standby_mode < 0) return 0;  
  /*
  ** When the socket becomes active, stop the polling
  */ 
  if (lbg_p->active_lbg_entry != -1) {
    af_unix_ctx_generic_t *sock_p = af_unix_getObjCtx_p(lbg_p->entry_tb[lbg_p->active_lbg_entry].sock_ctx_ref);
    if (sock_p == NULL)
    {
      return 0;
    }    
    return (sock_p->remote_ipaddr_host);
  }
  return 0;
}
