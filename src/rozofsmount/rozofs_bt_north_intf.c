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
#include <pthread.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/core/ruc_buffer_api.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/ruc_buffer_debug.h>
#include "rozofs_bt_api.h"
#include "rozofs_bt_proto.h"
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/core/rozofs_socket_family.h>
#include "rozofs_rw_load_balancing.h"
#include "rozofs_bt_inode.h"

/**
* Buffers information
*/
int rozofs_bt_cmd_buf_count= 0;   /**< number of buffer allocated for read/write on north interface */
int rozofs_bt_cmd_buf_sz= 0;      /**<read:write buffer size on north interface */

void *rozofs_bt_buffer_pool_p = NULL;  /**< reference of the read/write buffer pool */
void *rozofs_bt_rd_buffer_pool_p = NULL;  /**< reference of the read/write buffer pool */
void *rozofs_bt_io_storcli_pool_p = NULL;
pthread_rwlock_t rozofs_bt_buf_pool_lock;

rozofs_bt_thread_ctx_t rozofs_bt_stc_polling_thread;

/*
**____________________________________________________
*/

void rozofs_bt_free_receive_buf(void *buf)
{
   pthread_rwlock_wrlock(&rozofs_bt_buf_pool_lock);
   ruc_buf_freeBuffer(buf); 
   pthread_rwlock_unlock(&rozofs_bt_buf_pool_lock);   
}
/*
**____________________________________________________
*/
/**
*  
  Callback to allocate a buffer for receiving a rpc message (mainly a RPC response
 
 
 The service might reject the buffer allocation because the pool runs
 out of buffer or because there is no pool with a buffer that is large enough
 for receiving the message because of a out of range size.

 @param userRef : pointer to a user reference: not used here
 @param socket_context_ref: socket context reference
 @param len : length of the incoming message
 
 @retval <>NULL pointer to a receive buffer
 @retval == NULL no buffer 
*/
void * rozofs_bt_north_RcvAllocBufCallBack(void *userRef,uint32_t socket_context_ref,uint32_t len)
{
    uint32_t free_count; 
    void *p;

    /*
    ** Check that the is enough buffer on the storio side: we need to have at least 2 buffers since we might need to trigger
    ** the reading of a tracking that have been removed from the cache when there is a cache miss during the io_read in batch mode.
    */
    free_count = ruc_buf_getFreeBufferCount(rozofs_bt_buffer_pool_p);

    if (free_count < 2) {
        return NULL;
    }
    
   /*
   ** check if a small or a large buffer must be allocated
   */
   if (len >  rozofs_bt_cmd_buf_sz)
   {   
     return NULL;   
   }
   pthread_rwlock_wrlock(&rozofs_bt_buf_pool_lock);
   p = ruc_buf_getBuffer(rozofs_bt_buffer_pool_p); 
   pthread_rwlock_unlock(&rozofs_bt_buf_pool_lock); 
   return p;  
}

/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   receiver ready function: called from socket controller.
   The module is intended to return if the receiver is ready to receive a new message
   and FALSE otherwise
   
   The application is ready to received if the north read/write buffer pool is not empty


  @param socket_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)

  @retval : TRUE-> receiver ready
  @retval : FALSE-> receiver not ready
*/
uint32_t rozofs_bt_north_userRcvReadyCallBack(void * socket_ctx_p,int socketId)
{

    if (ruc_buf_isPoolEmpty(rozofs_bt_buffer_pool_p))
    {
      return FALSE;
    
    }
    return TRUE;
}



/*
 *_______________________________________________________________________
 */
/** Share memory deletion polling thread
 */
 #define EXPORT_FSTAT_PTHREAD_FREQUENCY_SEC 1
static void *rozofs_bt_mem_deregister_polling_thread(void *v) {

   volatile rozofs_memreg_t *q;
   rozofs_stc_memreg_t *p;
   rozofs_memreg_t *mem_unreg_p; 
   volatile uint32_t nb_storcli;
   int done;
   
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    
    // Set the frequency of calls
    struct timespec ts = {EXPORT_FSTAT_PTHREAD_FREQUENCY_SEC, 0};
   int i,k;

    uma_dbg_thread_add_self("shm_thread");

    for (;;) {
	q = rozofs_bt_stc_mem_p;
	if (q == NULL) {
	  nanosleep(&ts, NULL);
	  continue;
	}
	
	nb_storcli = stclbg_get_storcli_number();   
	p= (rozofs_stc_memreg_t*)q->addr;
	
	for (i = 0; i <  ROZOFS_SHARED_MEMORY_REG_MAX; i ++,p++)
	{
	  if (p->name[0] == 0) continue;
	  if (p->delete_req == 0) continue;
	  done = 1;
	  for (k= 0; k <nb_storcli;k++)
	  {
	    if (p->storcli_ack[k] == 0)
	    {
	       done = 0;
	       break;
	    }
	  }
	  	   
	  if (done)
	  {
	    /*
	    ** need to perform the memory unregistration
	    */
	    mem_unreg_p =  &rozofs_shm_tb[p->rozofs_key.s.ctx_idx];

	     pthread_rwlock_wrlock(&rozofs_bt_shm_lock);  

	     if (mem_unreg_p->name!=NULL)
	     {
        	 munmap(mem_unreg_p->addr,mem_unreg_p->length);
        	 if (mem_unreg_p->fd_share) {
        	 close(mem_unreg_p->fd_share);
		 mem_unreg_p->fd_share = 0;
        	 shm_unlink(mem_unreg_p->name);
		 rozofs_shm_count--;
		 info("sharemem /dev/shm/%s disconnected !! on all storcli",mem_unreg_p->name);
		 free(mem_unreg_p->name);
		 mem_unreg_p->name = NULL;
        	 
        	 }
	      }
	      p->name[0] = 0;
	      p->delete_req =0;

	      pthread_rwlock_unlock(&rozofs_bt_shm_lock);  	  	
	  }
      }
      nanosleep(&ts, NULL);
      
    }
    return 0;
}

/*
**_________________________________________________________________________________________________
*/
void rozofs_shm_deregister_on_disconnect(int fd)
{
    int i,k;
    rozofs_memreg_t *p = &rozofs_shm_tb[0];
    rozofs_memreg_t *q = rozofs_bt_stc_mem_p;
    rozofs_stc_memreg_t *stc_shm_p;
    uint32_t nb_storcli;

    stc_shm_p = (rozofs_stc_memreg_t*)q->addr;
    
    pthread_rwlock_wrlock(&rozofs_bt_shm_lock);  
    nb_storcli = stclbg_get_storcli_number(); 
     
    for (i = 0; i < ROZOFS_SHARED_MEMORY_REG_MAX; i++,p++)
    {
      if (p->fd !=fd) continue;

#if 0      
      munmap(p->addr,p->length);
      close(p->fd_share);
      shm_unlink(p->name);
      info("sharemem /dev/shm/%s disconnected !!",p->name);
      p->fd= -1;
      rozofs_shm_count--;
      free(p->name);
      p->name = NULL;
#endif
      /*
      ** set the pointer to the context to release
      */
      stc_shm_p = stc_shm_p+p->rozofs_key.s.ctx_idx;

//#warning FDL_TEST without storcli
//      for (k= 0; k <nb_storcli;k++) stc_shm_p->storcli_ack[k] = 1;
      for (k= 0; k <nb_storcli;k++) stc_shm_p->storcli_ack[k] = 0;
      stc_shm_p->delete_req = 1;
   
    }
    pthread_rwlock_unlock(&rozofs_bt_shm_lock);  
}
/*__________________________________________________________________________
*/
/**
* test function that is called upon a failure on sending

 The application might use that callback if it has some other
 destination that can be used in case of failure of the current one
 If the application has no other destination to select, it is up to the
 application to release the buffer.
 

 @param userRef : pointer to a user reference: not used here
 @param socket_context_ref: socket context reference
 @param bufRef : pointer to the packet buffer on which the error has been encountered
 @param err_no : errno has reported by the sendto().
 
 @retval none
*/
void rozofs_shm_deregister_on_disconnect(int fd);
void  rozofs_bt_north_userDiscCallBack(void *userRef,uint32_t socket_context_ref,void *bufRef,int err_no)
{
   af_unix_ctx_generic_t *this;
    /*
    ** release the current buffer if significant
    */
    if (bufRef != NULL) ruc_buf_freeBuffer(bufRef);
    
    severe("remote end disconnection");;
   
    this = af_unix_getObjCtx_p(socket_context_ref);
    if (this == NULL)
    {
       fatal("The socket does not exist");
    }
    rozofs_shm_deregister_on_disconnect(this->socketRef);
    /*
    ** release the context now and clean up all the attached buffer
    */
    af_unix_delete_socket(socket_context_ref);   
}

/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   THis the callback that is activated upon the recption of a disk
   operation from a remote client: There is 2 kinds of requests that
   are supported by this function:
   READ and WRITE

    
  @param socket_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)
 
   @retval : TRUE-> xmit ready event expected
  @retval : FALSE-> xmit  ready event not expected
*/
void rozofs_bt_process_memreg(void *recv_buf,int socket_ctx_idx);
void rozofs_bt_process_io_read(void *recv_buf,int socket_ctx_idx,void *p);

void rozofs_bt_req_rcv_cbk(void *userRef,uint32_t  socket_ctx_idx, void *recv_buf)
{

   rozo_batch_hdr_t *hdr_p;
   af_unix_ctx_generic_t *this;
   
   this = af_unix_getObjCtx_p(socket_ctx_idx);
   if (this == NULL)
   {
      fatal("The socket does not exist");
   }

    hdr_p  = (rozo_batch_hdr_t*) ruc_buf_getPayload(recv_buf); 

    /*
    ** get the opcode requested and dispatch the processing accordling to that opcode
    */
    switch (hdr_p->opcode)
    {
       case ROZO_BATCH_MEMREG:
        //info("ROZO_BATCH_MEMREG received socket %d",this->socketRef);
	//rozofs_bt_process_memreg(recv_buf,this->socketRef);
	rozofs_bt_process_memreg_from_main_thread(recv_buf,this->socketRef);
        return;

       case ROZO_BATCH_MEMCREATE:
        //info("ROZO_BATCH_MEMCREATE received socket %d",this->socketRef);
	//rozofs_bt_process_memcreate(recv_buf,this->socketRef);
        rozofs_bt_process_memcreate_from_main_thread(recv_buf,this->socketRef);
        return;

       case ROZO_BATCH_MEMADDR_REG:
        //info("ROZO_BATCH_MEMCREATE received socket %d",this->socketRef);
	rozofs_bt_process_mem_register_addr(recv_buf,this->socketRef);
        return;

       case ROZO_BATCH_INIT:
        //info("ROZO_BATCH_INIT received socket %d",this->socketRef);
	rozofs_bt_process_init(recv_buf,this->socketRef);
        return;       
	       
       case ROZO_BATCH_GETATTR:
        //info("ROZO_BATCH_GETATTR received socket %d",this->socketRef);
	rozofs_bt_process_getattr_from_main_thread(recv_buf,this->socketRef);
//	rozofs_bt_process_getattr(recv_buf,this->socketRef);
        return;  
       case ROZO_BATCH_READ:
        //info("ROZO_BATCH_READ received socket %d",this->socketRef);
        rozofs_bt_process_io_read(recv_buf,this->socketRef,NULL);

       default:
        /*
        ** Put code here to format a reply with an error message
        */
        //rozofs_bt_reply_error_with_recv_buf(socket_ctx_idx,recv_buf,NULL,rozofs_bt_remote_rsp_cbk,errcode);
        return;   
    }
    return;
}


/*
**_________________________________________________________________________________________________
*/
/*
** Create the threads that will handle all the batch requests

* @param hostname    storio hostname (for tests)
* @param eid    reference of the export
* @param storcli_idx    relative index of the storcli process
* @param nb_threads  number of threads to create
*  
* @retval 0 on success -1 in case of error
*/
int rozofs_bt_stc_polling_thread_create() {

   int                        err;
   pthread_attr_t             attr;
   rozofs_bt_thread_ctx_t * thread_ctx_p;


   /*
   ** clear the thread table
   */
   memset(&rozofs_bt_stc_polling_thread,0,sizeof(rozofs_bt_thread_ctx_t));
   /*
   ** Now create the threads
   */
   thread_ctx_p = &rozofs_bt_stc_polling_thread;
  
   err = pthread_attr_init(&attr);
   if (err != 0) {
     fatal("rozofs_bt_thread_create pthread_attr_init() %s",strerror(errno));
     return -1;
   }  

   thread_ctx_p->thread_idx = 0;
   err = pthread_create(&thread_ctx_p->thrdId,&attr,rozofs_bt_mem_deregister_polling_thread,thread_ctx_p);
   if (err != 0) {
     fatal("rozofs_bt_mem_deregister_polling_thread pthread_create() %s", strerror(errno));
     return -1;
   }  
     
  return 0;
}



 /**
 *  socket configuration for the family
 */
 af_unix_socket_conf_t  af_unix_test_family =
{
  1,  //           family: identifier of the socket family    */
  0,         /**< instance number within the family   */
  sizeof(uint32_t),  /* headerSize  -> size of the header to read                 */
  0,                 /* msgLenOffset->  offset where the message length fits      */
  sizeof(uint32_t),  /* msgLenSize  -> size of the message length field in bytes  */

  (1024*256), //        bufSize;         /* length of buffer (xmit and received)        */
  (300*1024), //        so_sendbufsize;  /* length of buffer (xmit and received)        */
  rozofs_bt_north_RcvAllocBufCallBack,  //    userRcvAllocBufCallBack; /* user callback for buffer allocation */
  rozofs_bt_req_rcv_cbk,           //    userRcvCallBack;   /* callback provided by the connection owner block */
  rozofs_bt_north_userDiscCallBack,   //    userDiscCallBack; /* callBack for TCP disconnection detection         */
  NULL,   //userConnectCallBack;     /**< callback for client connection only         */
  NULL,  //    userXmitDoneCallBack; /**< optional call that must be set when the application when to be warned when packet has been sent */
  rozofs_bt_north_userRcvReadyCallBack,  //    userRcvReadyCallBack; /* NULL for default callback                     */
  NULL,  //    userXmitReadyCallBack; /* NULL for default callback                    */
  NULL,  //    userXmitEventCallBack; /* NULL for default callback                    */
  NULL, // rozofs_tx_get_rpc_msg_len_cbk,        /* userHdrAnalyzerCallBack ->NULL by default, function that analyse the received header that returns the payload  length  */
  ROZOFS_GENERIC_SRV,       /* recv_srv_type ---> service type for reception : ROZOFS_RPC_SRV or ROZOFS_GENERIC_SRV  */
  (1024*256),       /*   rpc_recv_max_sz ----> max rpc reception buffer size : required for ROZOFS_RPC_SRV only */

  NULL,  //    *userRef;             /* user reference that must be recalled in the callbacks */
  NULL,  //    *xmitPool; /* user pool reference or -1 */
  NULL,   //    *recvPool; /* user pool reference or -1 */
  .priority = 3,  /** Set the af_unix socket priority */
}; 

/*
**____________________________________________________
*/
/**
   rozofs_bt_north_interface_init

  create the Transaction context pool

@param     : cmd_buf_count : number of read/write buffer
@param     : cmd_buf_sz : size of a read/write buffer
@param     : eid : unique identifier of the export to which the storcli process is associated
@param     : rozofsmount_instance : instance number is needed when several reozfsmount runni ng oin the same share the export


@retval   : RUC_OK : done
@retval          RUC_NOK : out of memory
*/

int rozofs_bt_north_interface_init(uint32_t eid,uint16_t rozofsmount_instance,int cmd_buf_count)
{
   int ret = 0;
   char sunpath[AF_UNIX_SOCKET_NAME_SIZE];
   int cmd_buf_sz;
   union {
    rozo_io_cmd_t io;
    rozo_attr_cmd_t attr;
    rozo_memreg_cmd_t reg;
    rozo_io_res_t res;
   } rozo_batch_t;
   
   cmd_buf_sz = sizeof(rozo_batch_t)*ROZOFS_MAX_BATCH_CMD + sizeof(rozo_batch_hdr_t);

    rozofs_bt_cmd_buf_count  = cmd_buf_count;
    rozofs_bt_cmd_buf_sz     = cmd_buf_sz    ;
    while(1)
    {
    rozofs_bt_buffer_pool_p = ruc_buf_poolCreate(rozofs_bt_cmd_buf_count,rozofs_bt_cmd_buf_sz);
    if (rozofs_bt_buffer_pool_p == NULL)
    {
       ret = -1;
       severe( "ruc_buf_poolCreate(%d,%d)", rozofs_bt_cmd_buf_count, rozofs_bt_cmd_buf_sz ); 
       break;
    }
    /*
    ** register the pool
    */
    ruc_buffer_debug_register_pool("Rozofs_bt_rcv",rozofs_bt_buffer_pool_p);

    rozofs_bt_rd_buffer_pool_p = ruc_buf_poolCreate(cmd_buf_count,sizeof(rozofs_bt_ioctx_t));
    if (rozofs_bt_rd_buffer_pool_p == NULL)
    {
       ret = -1;
       severe( "ruc_buf_poolCreate(%d,%d)", cmd_buf_count, (int)(sizeof(rozofs_bt_ioctx_t)) ); 
       break;
    }
    /*
    ** register the pool
    */
    ruc_buffer_debug_register_pool("Rozofs_bt_read",rozofs_bt_rd_buffer_pool_p);    


    rozofs_bt_io_storcli_pool_p = ruc_buf_poolCreate(cmd_buf_count,sizeof(rozofs_bt_io_working_ctx_t));
    if (rozofs_bt_io_storcli_pool_p == NULL)
    {
       ret = -1;
       severe( "ruc_buf_poolCreate(%d,%d)", cmd_buf_count, (int)(sizeof(rozofs_bt_io_working_ctx_t)) ); 
       break;
    }
    
    ret = rozofs_bt_stc_polling_thread_create();
    if (rozofs_bt_io_storcli_pool_p == NULL)
    {
       ret = -1;
       severe( "rozofs_bt_stc_polling_thread_create()"); 
       break;
    }
    /*
    ** register the pool
    */
    ruc_buffer_debug_register_pool("Rozofs_bt_io",rozofs_bt_io_storcli_pool_p);    

    pthread_rwlock_init(&rozofs_bt_buf_pool_lock,NULL);

    /*
    ** create the listening af unix socket on the north interface
    */
    af_unix_test_family.rpc_recv_max_sz = rozofs_bt_cmd_buf_sz;
    sprintf(sunpath,"%sexport_%d_mount_%d",ROZOFS_SOCK_FAMILY_BATCH_NORTH,
                     eid,rozofsmount_instance);
    ret =  af_unix_sock_listening_create("BATCH_NORTH",
                                          sunpath, 
                                          &af_unix_test_family   
                                          );

    break; 
    
    }

    return ret;

}


