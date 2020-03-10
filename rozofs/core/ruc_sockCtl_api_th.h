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
#ifndef SOCKET_CTRL_API_TH_H
#define SOCKET_CTRL_API_TH_H
#include <rozofs/common/types.h>
#include <sys/select.h>
#include "ruc_common.h"
#include "ruc_list.h"
#include "rozofs_fdset.h"
#include "ruc_sockCtl_api.h"
#include <pthread.h>
/*
**   PUBLIC SERVICES API
*/

/*
**___________________________________________________________
*/
/**
    creation of connection with the socket controller in multithreaded mode.
    if there is a free connection entry, it returns the pointer to the allocated context

     @param sock_p : pointer to the socket controller context
     @param socketId : socket identifier returned by the socket() service
     @param  priority : polling priority
     @param  objRef : object reference provided as a callback parameter
     @param *callBack : pointer to the call back functions.

       @retval !=NULL : connection identifier
      @retval ==NULL : out of context

*/
void * ruc_sockctl_connect_th(void *ctx_p,int socketId,
                           char *name,
                           uint32_t priority,
                           void *objRef,
                           ruc_sockCallBack_t *callback);
			   

/*
**_____________________________________________________________
*/
/**

    deletion of connection with the socket controller.
    
     @param sock_p : pointer to the socket controller context
     @param connectionId : reference returned by the connection service

    @retval RUC_OK on success
    @retval RUC_NOK on error
*/

uint32_t ruc_sockctl_disconnect_th(void *ctx_p, void * connectionId);


/*
**____________________________________________________________
*/
/**
    ruc_sockctl_init_th(uint32_t maxConnection)
     
     creation of the socket controller distributor for multi-threaded case
  
        @param maxConnection : number of elements to create
 
    @retval: <> NULL pointer to the socket controller context :
    @retval NULL: error see errno for details 
*/

void *ruc_sockctl_init_th(uint32_t maxConnection,char *name);
/*
**____________________________________________________________
*/
/**
*  Main loop
*/
void ruc_sockCtrl_selectWait_th(void *ctx);

/*
**____________________________________________________________
*/
/**
    ruc_sockCtrl_attach_applicative_poller
     
    attach an applicative poller function to the socket controller
       
    @param ctx_p : pointer to the socket controller context
 
    @retval:none
*/
void ruc_sockCtrl_attach_applicative_poller_th(void *ctx_p,ruc_scheduler_t callback);


/*
**____________________________________________________________
*/

static inline void * RUC_SOCKCTL_CONNECT(void *ctx_p,int socketId,
                           char *name,
                           uint32_t priority,
                           void *objRef,
                           ruc_sockCallBack_t *callback)
{

   if (ctx_p == NULL)
   {
     return ruc_sockctl_connect(socketId,name,priority,objRef,callback);
   }
   return ruc_sockctl_connect_th(ctx_p,socketId,name,priority,objRef,callback);
}

/*
**____________________________________________________________
*/
static inline uint32_t RUC_SOCKCTL_DISCONNECT(void *ctx_p,void *connectId)
{

   if (ctx_p == NULL)
   {
     return ruc_sockctl_disconnect(connectId);
   }
   return ruc_sockctl_disconnect_th(ctx_p,connectId);
}

/*
** file descriptor for receiving and transmitting events
*/
extern rozo_fd_set  rucWrFdSetCongested;
void ruc_sockctl_congested_th(void *ctx_p,int socket);
void ruc_sockctl_eoc_th(void *ctx_p,int socket);

/*
**________________________________________
*/
/**
*  Begin of congestion

    @param ctx_p: socket controller context
    @param int socket : file descriptor to set
*/
static inline void RUC_SOCKCTL_CONGESTED(void *ctx_p,int socket)
{
   if (ctx_p == NULL)
   {
     return FD_SET(socket,&rucWrFdSetCongested);
   }
   return  ruc_sockctl_congested_th(ctx_p,socket);
}


/*
**________________________________________
*/
/**
*  End of congestion

    @param ctx_p: socket controller context
    @param int socket : file descriptor to set
*/
static inline void RUC_SOCKCTL_EOC(void *ctx_p,int socket)
{
   if (ctx_p == NULL)
   {
     return FD_CLR(socket,&rucWrFdSetCongested);
   }
   return  ruc_sockctl_eoc_th(ctx_p,socket);
}

/*
**________________________________________
*/
/**
*   Get the pointer to the socket controller based on the threadID

   @param none
   
   @retval <> NULL found
   @retval NULL not found
*/
void *ruc_sockctl_get_ctx_th();
/*
**________________________________________
*/
/**
*   Get the module index of the socket controller
*/
int ruc_sockctl_get_module_idx_th(void *p);
/*
**________________________________________
*/
/**
*   Get the module index of the socket controller associated to a thread
*/
int ruc_sockctl_get_thread_module_idx_th();


/**
*   Get the module name of the socket controller
*/
char *ruc_sockctl_get_module_name_th(void *p);
#endif
