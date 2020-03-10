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
int north_lbg_attach_app_sup_cbk_on_entries(north_lbg_ctx_t  *lbg_p);


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
int north_lbg_create_af_unix_th(char *name,char *basename_p,int family,int first_instance,int  nb_instances,af_unix_socket_conf_t *usr_conf_p)
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
     entry_p->sock_ctx_ref = af_unix_sock_client_create_th(nickname,sun_path,conf_p); 
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
int north_lbg_create_af_inet_th(char *name,
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
     entry_p->sock_ctx_ref = af_inet_sock_client_create_th(name,
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
  
 @param none
 
  @retval >= reference of the load balancer object
  @retval < 0 error (out of context ??)
*/
int north_lbg_configure_af_inet_th(int lbg_idx,char *name,
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
     entry_p->sock_ctx_ref = af_inet_sock_client_create_th(name,
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
