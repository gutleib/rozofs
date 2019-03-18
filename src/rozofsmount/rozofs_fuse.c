/*
  Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
  This file is part of Rozofs.

  Rozofs is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published
  by the Free Software Foundation, version 2.

  Rozofs is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see
  <http://www.gnu.org/licenses/>.
 */

#include <assert.h>
#include <sys/ioctl.h>
#include <rozofs/core/ruc_buffer_debug.h>

#include "rozofs_fuse.h"
#include "rozofs_fuse_api.h"
#include "rozofs_sharedmem.h"

rozofs_fuse_ctx_t  *rozofs_fuse_ctx_p = NULL;  /**< pointer to the rozofs_fuse saved contexts   */
uint64_t rozofs_write_merge_stats_tab[RZ_FUSE_WRITE_MAX]; /**< read/write merge stats table */
uint64_t rozofs_write_buf_section_table[ROZOFS_FUSE_NB_OF_BUSIZE_SECTION_MAX];
uint64_t rozofs_read_buf_section_table[ROZOFS_FUSE_NB_OF_BUSIZE_SECTION_MAX];
rozofs_fuse_read_write_stats  rozofs_fuse_read_write_stats_buf;
 /**
 * prototypes
 */
uint32_t rozofs_fuse_rcvReadysock(void * rozofs_fuse_ctx_p,int socketId);
uint32_t rozofs_fuse_rcvMsgsock(void * rozofs_fuse_ctx_p,int socketId);
int rozofs_fuse_session_loop(rozofs_fuse_ctx_t *ctx_p, int *empty);
uint32_t rozofs_fuse_xmitReadysock(void * rozofs_fuse_ctx_p,int socketId);
uint32_t rozofs_fuse_xmitEvtsock(void * rozofs_fuse_ctx_p,int socketId);

rozofs_fuse_save_ctx_t *rozofs_fuse_usr_ctx_table[ROZOFS_FUSE_CTX_MAX];
uint32_t rozofs_fuse_usr_ctx_idx = 0;

uint64_t rozofs_fuse_req_count = 0;
uint64_t rozofs_fuse_req_byte_in = 0;
uint64_t rozofs_fuse_req_eagain_count = 0;
uint64_t rozofs_fuse_req_enoent_count = 0;
uint64_t rozofs_fuse_req_tic = 0;
uint64_t rozofs_fuse_buffer_depletion_count = 0;
uint64_t rozofs_fuse_rcv_buf_depletion_count = 0;
uint64_t rozofs_storcli_buffer_depletion_count = 0;
int rozofs_fuse_loop_count = 2;
int fuse_sharemem_init_done = 0;
int fuse_sharemem_enable = 1;
uint64_t fuse_profile[3];
int rozofs_storcli_pending_req_count= 0;
uint64_t rozofs_storcli_xoff_count= 0;
uint64_t rozofs_storcli_xon_count= 0;

void  rozofs_fuse_share_mem_init();

/*
**  Call back function for socket controller
*/
ruc_sockCallBack_t rozofs_fuse_callBack_sock=
  {
     rozofs_fuse_rcvReadysock,
     rozofs_fuse_rcvMsgsock,
     rozofs_fuse_xmitReadysock,
     rozofs_fuse_xmitEvtsock
  };


void rozofs_fuse_get_ticker()
{
  struct timeval     timeDay;  

  gettimeofday(&timeDay,(struct timezone *)0);  
  rozofs_fuse_req_tic = MICROLONG(timeDay); 
}


list_t rozofs_fuse_rcv_buf_head;    /**< head of the receive buffer   */
int    rozofs_fuse_rcv_buf_count;
rozofs_fuse_rcv_buf_t *rozofs_fuse_cur_rcv_buf = NULL;  /**< current receive buffer */




/*
**__________________________________________________________________________
*/
/**
*  Init of the fuse receive buffer distributor

   @param count: number of buffer to setup
   @param size: size of each buffer

   @retval 0 on success
   @retval -1 on error (see errno )
*/
int rozofs_fuse_init_rcv_buffer_pool(int count,int size)
{
   int effective_size = size+sizeof(list_t);
   int i;
   rozofs_fuse_rcv_buf_t *p_rcv_buf;
   
   /*
   ** init of the head
   */
    list_init(&rozofs_fuse_rcv_buf_head);
    rozofs_fuse_rcv_buf_count = count;
    
    for (i = 0; i < count;i++)
    {
       p_rcv_buf = memalign(4096,effective_size);
       if (p_rcv_buf== NULL)
       {
          errno = ENOMEM;
	  return -1;
       }
       list_init(&p_rcv_buf->list);
       list_push_front(&rozofs_fuse_rcv_buf_head,&p_rcv_buf->list);        
    }
    return 0;
}
/*
**__________________________________________________________________________
*/
/**
*  get a  fuse receive buffer from distributor


   @retval <> NULL on success
   @retval  NULL if no buffer 
*/
rozofs_fuse_rcv_buf_t *rozofs_fuse_alloc_rcv_buffer_pool()
{
    rozofs_fuse_rcv_buf_t *p_rcv_buf = NULL;
    if (list_empty(&rozofs_fuse_rcv_buf_head)) return NULL;
    rozofs_fuse_rcv_buf_count--;
    p_rcv_buf = list_first_entry(&rozofs_fuse_rcv_buf_head,rozofs_fuse_rcv_buf_t,list);
    list_remove(&p_rcv_buf->list);
    return p_rcv_buf;
}

/*
**__________________________________________________________________________
*/
/**
*  release a  fuse receive buffer to the distributor


   @retval <> NULL on success
   @retval  NULL if no buffer 
*/
void rozofs_fuse_release_rcv_buffer_pool(rozofs_fuse_rcv_buf_t *p_rcv_buf)
{
    list_remove(&p_rcv_buf->list);
    list_push_front(&rozofs_fuse_rcv_buf_head,&p_rcv_buf->list);     
    rozofs_fuse_rcv_buf_count++;
}



/**
* rozofs fuse xmit and receive channel callbacks for non blocking case
*/
int rozofs_fuse_kern_chan_receive(struct fuse_chan **chp, char *buf,size_t size);
int rozofs_fuse_kern_chan_send(struct fuse_chan *ch, const struct iovec iov[],size_t count);
void rozofs_fuse_kern_chan_destroy(struct fuse_chan *ch);

struct fuse_chan_ops rozofs_fuse_ch_ops = {
	/**
	 * Hook for receiving a raw request
	 *
	 * @param ch pointer to the channel
	 * @param buf the buffer to store the request in
	 * @param size the size of the buffer
	 * @return the actual size of the raw request, or -1 on error
	 */
	.receive = rozofs_fuse_kern_chan_receive,

	/**
	 * Hook for sending a raw reply
	 *
	 * A return value of -ENOENT means, that the request was
	 * interrupted, and the reply was discarded
	 *
	 * @param ch the channel
	 * @param iov vector of blocks
	 * @param count the number of blocks in vector
	 * @return zero on success, -errno on failure
	 */
     .send= rozofs_fuse_kern_chan_send,

	/**
	 * Destroy the channel
	 *
	 * @param ch the channel
	 */
    .destroy = rozofs_fuse_kern_chan_destroy
};






/*
**__________________________________________________________________________

    F U S E   C H A N N E L   C A L L B A C K S
**__________________________________________________________________________
*/
/*

**__________________________________________________________________________
*/
/**
* internal function that is called from processing a message that 
  has been queued on the /dev/fuse socket. That function is
  inherited from fuse_kern_chan_receive
  
  @param chp : pointer to the channel
  @param  buf: pointer to the buffer where data will be copied
  @param  size : max size of the receive buffer
 
  @retval > 0 : number of byte read
  @retval = 0 : session has been exited
  @retval  < 0 : error
 */
int rozofs_fuse_kern_chan_receive(struct fuse_chan **chp, char *buf,
				  size_t size)
{
	struct fuse_chan *ch = *chp;
	int err;
	ssize_t res;
	struct fuse_session *se = fuse_chan_session(ch);
	assert(se != NULL);

restart:
	res = read(fuse_chan_fd(ch), buf, size);

	if (fuse_session_exited(se))
		return 0;
	if (res == -1) 
	{
	  /* ENOENT means the operation was interrupted, it's safe
	  to restart */
	  err = errno;
	  if (err == ENOENT)
	  {
	    rozofs_fuse_req_enoent_count++;
	    goto restart;
	  }
	  if (err == ENODEV) {
	    info("Exit from RozofsMount required!!!");
	    fuse_session_exit(se);
	    rozofs_exit();
	    return 0;
	  }
	  /* Errors occurring during normal operation: EINTR (read
	     interrupted), EAGAIN (nonblocking I/O), ENODEV (filesystem
	     umounted) */
	  if (err != EINTR && err != EAGAIN) severe("fuse: reading device");
	  if ((err == EAGAIN)|| (err == EINTR)) rozofs_fuse_req_eagain_count++;
	  return -err;
	}
#if 0
	if ((size_t) res < sizeof(struct fuse_in_header)) {
		fprintf(stderr, "short read on fuse device\n");
		return -EIO;
	}
#endif
	rozofs_fuse_req_count++;
	rozofs_fuse_req_byte_in+=res;
	return res;
}
/*
**__________________________________________________________________________
*/
/**
*  Rozofs_fuse channel send:
*  Since Rozofs operates in non-blocking mode it cannot rely on the 
   default fuse_kern_chan_send() operation of fuse since if there is
   a congestion on the fuse device, the response or notification will
   be lost since the caller release the ressource allocated for sending
   the response once it returns from fuse_kern_chan_send().
   To avoid that issue, rozofs MUST be tracked of the response that has
   not been sent and must save it in some internals buffers.
   
    @param ch: fuse channel (contains the reference of the file descriptor to use
    @param iov: list of the vectors to send
    @param count: number of vectors to send
    
    @retval 0 on success 
    @retval < 0 on error
*/
int rozofs_fuse_kern_chan_send(struct fuse_chan *ch, const struct iovec iov[],
			       size_t count)
{
	if (iov) {
		ssize_t res = writev(fuse_chan_fd(ch), iov, count);
		int err = errno;

		if (res == -1) {
			struct fuse_session *se = fuse_chan_session(ch);

			assert(se != NULL);
            
            if(err == EAGAIN)
            {
              /*
              ** fuse device is congestion, so we store the reply and assert 
              ** the congestion flag in the rozofs_fuse context
              */
              
              return 0;
            
            }
			/* ENOENT means the operation was interrupted */
			if (!fuse_session_exited(se) && err != ENOENT)
				perror("fuse: writing device");
			return -err;
		}
	}
	/*
	** check if share mem init must be done
	*/
	if (fuse_sharemem_init_done == 0)
	{
	   rozofs_fuse_share_mem_init();
	}
	return 0;
}

/*
**__________________________________________________________________________
*/
/**
*  callback that must be called on channel close
   @param ch the channel
*/
void rozofs_fuse_kern_chan_destroy(struct fuse_chan *ch)
{
	close(fuse_chan_fd(ch));
}


/*
**__________________________________________________________________________

    S O C K E T   C O N T R O L L E R    C A L L B A C K S
**__________________________________________________________________________
*/

/*
**__________________________________________________________________________
*/
/**
  Invalidate the linux cache of a given inode
 
*/

void rozofs_fuse_invalidate_inode_cache(fuse_ino_t ino, uint64_t offset, uint64_t len)
{
    if (rozofs_fuse_ctx_p  == NULL) {
      warning("rozofs_fuse_invalidate_inode_cache no fuse ctx");
      return;
    }  
    fuse_lowlevel_notify_inval_inode(rozofs_fuse_ctx_p->ch, ino, offset, len);
}

static inline uint32_t rozofs_xoff()
{
   if (rozofs_fuse_ctx_p->ioctl_supported)
   {
      if (rozofs_fuse_ctx_p->data_xon == 1)
      {
         rozofs_fuse_ctx_p->data_xon = 0;
	 rozofs_storcli_xoff_count++;
	 ioctl(rozofs_fuse_ctx_p->fd,rozofs_fuse_ctx_p->data_xon,NULL);      
      }
      return TRUE;
   }
   return FALSE;

}

static inline void rozofs_xon()
{
   if (rozofs_fuse_ctx_p->ioctl_supported)
   {
      if (rozofs_fuse_ctx_p->data_xon == 0)
      {
         rozofs_fuse_ctx_p->data_xon = 1;
	 rozofs_storcli_xon_count++;
	 ioctl(rozofs_fuse_ctx_p->fd,rozofs_fuse_ctx_p->data_xon,NULL);      
      }
   }
}
/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   receiver ready function: called from socket controller.
   The module is intended to return if the receiver is ready to receive a new message
   and FALSE otherwise

    
  @param rozofs_fuse_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)
 
  @retval : TRUE-> receiver ready
  @retval : FALSE-> receiver not ready
*/

uint32_t rozofs_fuse_rcvReadysock(void * rozofs_fuse_ctx_p,int socketId)
{
    rozofs_fuse_ctx_t  *ctx_p;
    uint32_t            buffer_count;
    uint32_t            status;
    
    
    ctx_p = (rozofs_fuse_ctx_t*)rozofs_fuse_ctx_p;
    
    /*
    ** check if the session has been exited
    */
    if (fuse_session_exited(ctx_p->se))
    {
      /*
      ** session is dead, so stop receiving fuse request
      */
      return FALSE;
    }
    /*
    ** There is no specific buffer pool needed for receiving the fuse request
    ** since the fuse library allocates memory to store the incoming request.
    ** The only element that can prevent a fuse request to be processed is the 
    ** amount of transaction context. So the system has to check how many transaction
    ** contexts are remaining in the transaction context buffer pool. 
    ** When there is no enough contexts, then the system stops looking at the 
    ** fuse "socket". 
    */
    buffer_count = ruc_buf_getFreeBufferCount(ctx_p->fuseReqPoolRef);
    /*
    ** 2 fuse contexts are required :
    ** - 1 to process the incoming request
    ** - 1 to eventualy process an internal asynchronous flush
    */
    if (buffer_count < 2) 
    {
      rozofs_fuse_buffer_depletion_count++;
      return FALSE;
    }
     /*
    ** Check if rozofsmount does not run out of fuse receive buffer
    */
    
    if (list_empty(&rozofs_fuse_rcv_buf_head))
    {
      rozofs_fuse_rcv_buf_depletion_count++;
      return FALSE;    
    
    }    
    /*
    ** check the number of requests towards the storcli
    */
    if (rozofs_storcli_pending_req_count >= rozofs_max_storcli_tx)
    {
      status = rozofs_xoff();
      rozofs_storcli_buffer_depletion_count++;
      return status;
    }   
    /*
    ** Check the amount of read buffer (shared pool)
    */
    buffer_count = rozofs_get_shared_storcli_buf_free(SHAREMEM_IDX_READ);
    if (buffer_count < 2) 
    {
      status = rozofs_xoff();
      rozofs_storcli_buffer_depletion_count++;
      return status;
    }          
    /*
    ** Check the amount of read buffer (shared pool)
    */
    buffer_count = rozofs_get_shared_storcli_buf_free(SHAREMEM_IDX_WRITE);
    if (buffer_count < 2) 
    {
      status = rozofs_xoff();
      rozofs_storcli_buffer_depletion_count++;
      return status;
    }
    rozofs_xon();   
    return TRUE;
}
  
/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   Called from the socket controller when there is a message pending on the
   socket associated with the context provide in input arguments.


    
  @param rozofs_fuse_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)
 
   @retval : TRUE-> xmit ready event expected
  @retval : FALSE-> xmit  ready event not expected
*/

uint32_t rozofs_fuse_rcvMsgsock(void * rozofs_fuse_ctx_p,int socketId)
{
    rozofs_fuse_ctx_t  *ctx_p;
    int k;
    uint32_t            buffer_count;
    int empty = 0;
    uint32_t status;
    
    ctx_p = (rozofs_fuse_ctx_t*)rozofs_fuse_ctx_p;   
     
     for (k = 0; k < rozofs_fuse_loop_count; k++)
     {
       buffer_count = ruc_buf_getFreeBufferCount(ctx_p->fuseReqPoolRef);
       if (buffer_count < 2) 
       {
	 return TRUE;
       }
       /*
       ** check the number of requests towards the storcli
       */
       if (rozofs_storcli_pending_req_count >= rozofs_max_storcli_tx)
       {
	 status = rozofs_xoff();
	 if (status== FALSE) 
	 {
	   return TRUE;
	 }
       }   
       /*
       ** Check the amount of read buffer (shared pool)
       */
       buffer_count = rozofs_get_shared_storcli_buf_free(SHAREMEM_IDX_READ);
       if (buffer_count < 2) 
       {
	 status = rozofs_xoff();
	 if (status== FALSE) 
	 {
	   return TRUE;
	 }
       }           
       buffer_count = rozofs_get_shared_storcli_buf_free(SHAREMEM_IDX_WRITE);
       if (buffer_count < 2) 
       {
	 status = rozofs_xoff();
	 if (status== FALSE) 
	 {
	   return TRUE;
	 }
       }
       rozofs_fuse_session_loop(ctx_p,&empty);
       if (empty) return TRUE;
     }
    
    return TRUE;
}




/*
**__________________________________________________________________________
*/
int rozofs_fuse_session_loop(rozofs_fuse_ctx_t *ctx_p, int * empty)
{
	int res = 0;
    char *buf;
    struct fuse_buf fbuf;
    int exit_req = 0;
    struct fuse_session *se = ctx_p->se;
	struct fuse_chan *ch = fuse_session_next_chan(se, NULL);
    
    *empty = 0;
    
    /*
    ** Get a buffer from the rozofs_fuse context. That buffer is unique and is allocated
    ** at startup.
    */
//    START_PROFILING_FUSE();
//    buf = ctx_p->buf_fuse_req_p;

    if (rozofs_fuse_cur_rcv_buf == NULL)
    {
      /*
      ** Allocate a buffer for receiving message from fuse kernel
      */

      rozofs_fuse_cur_rcv_buf = rozofs_fuse_alloc_rcv_buffer_pool();
      if (rozofs_fuse_cur_rcv_buf == NULL) 
      {
	 /*
	 ** force empty in order to exit from the polling loop
	 */
	 *empty = 1;
	 return 0;
      }
    }
    buf = rozofs_fuse_cur_rcv_buf->buf;
    while (1) 
    {
        struct fuse_chan *tmpch = ch;

        /*
        ** set the reference of the buffer that will be used by fuse
        */
        fbuf.mem     = buf;
        fbuf.flags   = 0;
        fbuf.size    = ctx_p->bufsize;
	res = fuse_session_receive_buf(se, &fbuf, &tmpch);
        if (res == 0)
        {
           /*
           ** session has been exited
           */
           exit_req = 1;
           break;                   
        }
        if (res < 0)
        {
          switch(errno)
          {
            case EINTR:
             continue;
            case EAGAIN:
             /*
             ** the fuse queue is empty
             */
	     *empty = 1;
             return 0;
             break;   
             default:
             /*
             ** fatal error
             */
             exit_req = 1;
             break;        
          }
        }
        /*
        ** OK it looks like that there is a valid message
        */
//        STOP_PROFILING_FUSE();
        
	if ( exit_req == 0) fuse_session_process_buf(se, &fbuf, tmpch);
        if (fuse_session_exited(se) == 1)
        {
           exit_req = 1;
           break;
        
        }
        break;
    }
    /*
    ** Check if there is some data to process
    ** on the af_unix response socket (either release of a read buffer or sending a write request
    */
    af_unix_fuse_scheduler_entry_point(0);
/*
** to be reworked
*/
//	free(buf);
//	fuse_session_reset(se);
	return res < 0 ? -1 : 0;

}



/*
**__________________________________________________________________________
*/
/**
  Application callBack:

  Called from the socket controller. The application is intended to
  indicate if it wants to be warn when the transmitter is ready
  Typically after facing a congestion on xmit, the application will
  generally request from xmit ready event from the socket layer


  @param rozofs_fuse_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)
 
  @retval : always TRUE
*/

uint32_t rozofs_fuse_xmitReadysock(void * rozofs_fuse_ctx_p,int socketId)
{
    rozofs_fuse_ctx_t  *ctx_p;
    uint32_t ret = TRUE;
    
    ctx_p = (rozofs_fuse_ctx_t*)rozofs_fuse_ctx_p;
    
    if (ctx_p->congested) ret = FALSE;

    return ret;
}


/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   Called from the socket controller upon receiving a xmit ready event
   for the associated socket. That callback is activeted only if the application
   has replied TRUE in rozofs_fuse_xmitReadysock().
   
   It typically the processing of a end of congestion on the socket

    
  @param rozofs_fuse_ctx_p: pointer to the af unix socket
  @param socketId: reference of the socket (not used)
 
   @retval :always TRUE
*/

uint32_t rozofs_fuse_xmitEvtsock(void * rozofs_fuse_ctx_p,int socketId)
{
    rozofs_fuse_ctx_t  *ctx_p;

    ctx_p = (rozofs_fuse_ctx_t*)rozofs_fuse_ctx_p;
    /*
    ** active the fsm for end of congestion (xmit ready and credit reload
    */
    ctx_p->congested = 1;
    /*
    ** attempt to xmit the pending message
    */
//#warning put code to restart the transmission

    
    return TRUE;
}

static char *rozofs_fuse_show_usage(char *pChar)
{
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"fuse kernel               : generate a printk from the rozofs fuse.ko module \n");
  pChar += sprintf(pChar,"fuse loop <count>         : set the max. fuse requests polled from the device queue(default:2) \n");
  pChar += sprintf(pChar,"fuse bypass [size]        : set the minimum size in bytes of write size before using ioctl (default %u)\n",ROZOFS_MAX_FILE_BUF_SZ);
  pChar += sprintf(pChar,"fuse dir <enable|disable> :enable/disable dir attributes invalidation on mkdir/rmdir/unlink/create and mknod \n");
  pChar += sprintf(pChar,"fuse cnx                  :display connection info \n");
  pChar += sprintf(pChar,"fuse ra_pages <nb_pages>  :set the max readahead (unit is Linux page) \n");
  pChar += sprintf(pChar,"fuse max_background <count>        :set fuse max background requests count \n");
  pChar += sprintf(pChar,"fuse congestion_threshold <count>  :set fuse congestion threshold \n");
  pChar += sprintf(pChar,"fuse pagecache <enable|disable>    :enable or disable page cache direct write for storcli on read greater than 256KB \n");
  pChar += sprintf(pChar,"fuse                      :display statistics \n");
  return pChar;
}

void rozofs_fuse_show(char * argv[], uint32_t tcpRef, void *bufRef) {
  uint32_t            buffer_count=0;
  char                status[16];
  int   new_val; 
  int   ret;
  unsigned long rozofs_bypass_size;
  rozofs_cnx_param_t cnx_param;
  
  char *pChar = uma_dbg_get_buffer();

  if (argv[1] != NULL)
  {
      if (strcmp(argv[1],"kernel")==0) 
      {
	 if (rozofs_fuse_ctx_p->ioctl_supported)
	 {
	   ioctl(rozofs_fuse_ctx_p->fd,100,NULL);  
           pChar += sprintf(pChar, "check result in dmesg: ROZOFS_FUSE...\n");
	 } 
	 else
	 { 
           pChar += sprintf(pChar, "ioctl not supported with that fuse kernel version\n");
	 }

	 uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	 return;
      }

      if (strcmp(argv[1],"cnx")==0) 
      {
	 if (rozofs_fuse_ctx_p->ioctl_supported)
	 {
	   cnx_param.read = 1;
	   ret = ioctl(rozofs_fuse_ctx_p->fd,7,&cnx_param);  
	   if (ret == 0)
	   {
	     pChar +=sprintf(pChar,"Device path          : /sys/fs/fuse/connections/%u (%s)\n",cnx_param.dev,
	                                                   (rozofs_fuse_ctx_p->fuse_path_solved==0)?"NOT SOLVED":"SOLVED");
	     pChar +=sprintf(pChar,"ra_pages             : %llu\n",(long long unsigned int)cnx_param.ra_pages);
	     pChar +=sprintf(pChar,"max_background       : %u\n",cnx_param.max_background);
	     pChar +=sprintf(pChar,"congestion_threshold : %u\n",cnx_param.congestion_threshold);	   	   
	   }
	   else
	   {
           pChar += sprintf(pChar, "IOCTL error\n");
	   }
	 } 
	 else
	 { 
           pChar += sprintf(pChar, "ioctl not supported with that fuse kernel version\n");
	 }

	 uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	 return;
      }

      if (strcmp(argv[1],"ra_pages")==0) 
      {
	 errno = 0;
	 if (rozofs_fuse_ctx_p->ioctl_supported == 0)
	 {
	   pChar += sprintf(pChar, "ioctl not supported with that fuse kernel version\n");
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;	 
	 }
	 if (argv[2] == NULL)
	 {
           pChar += sprintf(pChar, "argument is missing\n");
	   rozofs_fuse_show_usage(pChar);
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;	  	  
	 }
	 new_val = (int) strtol(argv[2], (char **) NULL, 10);   
	 if (errno != 0) {
           pChar += sprintf(pChar, "bad value %s\n",argv[2]);
	   rozofs_fuse_show_usage(pChar);
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;
	 }
	 cnx_param.read = 0;
	 cnx_param.to_set = ROZOFS_RA_PAGE;
	 cnx_param.ra_pages = new_val;
	 
	 ret = ioctl(rozofs_fuse_ctx_p->fd,7,&cnx_param);   
         if (ret != 0)
	 {
	   pChar += sprintf(pChar,"error!! (%s)\n",strerror(ret));
	 }
	 else
           pChar += sprintf(pChar, "Done!\n");
	 uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	 return;
      }

      if (strcmp(argv[1],"max_background")==0) 
      {
	 errno = 0;
	 if (rozofs_fuse_ctx_p->ioctl_supported == 0)
	 {
	   pChar += sprintf(pChar, "ioctl not supported with that fuse kernel version\n");
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;	 
	 }
	 if (argv[2] == NULL)
	 {
           pChar += sprintf(pChar, "argument is missing\n");
	   rozofs_fuse_show_usage(pChar);
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;	  	  
	 }
	 new_val = (int) strtol(argv[2], (char **) NULL, 10);   
	 if (errno != 0) {
           pChar += sprintf(pChar, "bad value %s\n",argv[2]);
	   rozofs_fuse_show_usage(pChar);
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;
	 }
	 if (new_val == 0)
	 {
	    pChar += sprintf(pChar, "max_background must be different from 0!!\n");
            uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	    return;	 
	 }
	 cnx_param.read = 0;
	 cnx_param.to_set = ROZOFS_MAX_BACKGROUND;
	 cnx_param.max_background = new_val;
	 
	 ret = ioctl(rozofs_fuse_ctx_p->fd,7,&cnx_param);   
         if (ret != 0)
	 {
	   pChar += sprintf(pChar,"error!! (%s)\n",strerror(ret));
	 }
	 else
           pChar += sprintf(pChar, "Done!\n");
	 uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	 return;
      }
      if (strcmp(argv[1],"congestion_threshold")==0) 
      {
	 errno = 0;
	 if (rozofs_fuse_ctx_p->ioctl_supported == 0)
	 {
	   pChar += sprintf(pChar, "ioctl not supported with that fuse kernel version\n");
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;	 
	 }
	 if (argv[2] == NULL)
	 {
           pChar += sprintf(pChar, "argument is missing\n");
	   rozofs_fuse_show_usage(pChar);
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;	  	  
	 }
	 new_val = (int) strtol(argv[2], (char **) NULL, 10);   
	 if (errno != 0) {
           pChar += sprintf(pChar, "bad value %s\n",argv[2]);
	   rozofs_fuse_show_usage(pChar);
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;
	 }
	 cnx_param.read = 0;
	 cnx_param.to_set = ROZOFS_CONGESTION_THRESHOLD;
	 cnx_param.congestion_threshold = new_val;
	 
	 ret = ioctl(rozofs_fuse_ctx_p->fd,7,&cnx_param);   
         if (ret != 0)
	 {
	   pChar += sprintf(pChar,"error!! (%s)\n",strerror(ret));
	 }
	 else
           pChar += sprintf(pChar, "Done!\n");
	 uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	 return;
      }
      if (strcmp(argv[1],"loop")==0) 
      {
	 errno = 0;
	 if (argv[2] == NULL)
	 {
           pChar += sprintf(pChar, "argument is missing\n");
	   rozofs_fuse_show_usage(pChar);
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;	  	  
	 }
	 new_val = (int) strtol(argv[2], (char **) NULL, 10);   
	 if (errno != 0) {
           pChar += sprintf(pChar, "bad value %s\n",argv[2]);
	   rozofs_fuse_show_usage(pChar);
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;
	 }
	 /*
	 ** 
	 */
	 if (new_val == 0) {
           pChar += sprintf(pChar, "unsupported value %s\n",argv[2]);
	   rozofs_fuse_show_usage(pChar);
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;
	 }	 
	 rozofs_fuse_loop_count = new_val;
         pChar += sprintf(pChar, "new polling request count %d\n",rozofs_fuse_loop_count);
	 uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	 return;
      }

      if (strcmp(argv[1],"bypass")==0) 
      {
	 errno = 0;
	 if (argv[2] == NULL)
	 {
	   rozofs_bypass_size = ROZOFS_MAX_FILE_BUF_SZ;
	   conf.rozofs_bypass_size = rozofs_bypass_size;
	   ioctl(rozofs_fuse_ctx_p->fd,6,&rozofs_bypass_size);
           pChar += sprintf(pChar, "Done!\n");
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;	  	  
	 }
	 new_val = (int) strtol(argv[2], (char **) NULL, 10);   
	 if (errno != 0) {
           pChar += sprintf(pChar, "bad value %s\n",argv[2]);
	   rozofs_fuse_show_usage(pChar);
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;
	 }
 
	 rozofs_bypass_size = new_val;
	 conf.rozofs_bypass_size = rozofs_bypass_size;
	 ioctl(rozofs_fuse_ctx_p->fd,6,&rozofs_bypass_size);
         pChar += sprintf(pChar, "Done!\n");
	 uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	 return;
      }

      if (strcmp(argv[1],"dir")==0) 
      {
	 if (rozofs_fuse_ctx_p->ioctl_supported==0)
	 {
           pChar += sprintf(pChar, "ioctl not supported with that fuse kernel version\n");
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;
	 }
	 errno = 0;
	 if (argv[2] == NULL)
	 {
           pChar += sprintf(pChar, "argument is missing\n");
	   rozofs_fuse_show_usage(pChar);
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;	  	  
	 }
	 if (strcmp(argv[2],"enable")==0) 
	 {
	   ret = ioctl(rozofs_fuse_ctx_p->fd,4,NULL);  
	   if (ret < 0) 
	   {
            pChar += sprintf(pChar, "ioctl failed %s\n",strerror(errno));
	   }
	   else
	   {
             pChar += sprintf(pChar, "directory attributes are invalidated on mknod/rmdir/unlink...\n");
	     rozofs_fuse_ctx_p->dir_attr_invalidate = 1;
	   }
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;
         }
	 if (strcmp(argv[2],"disable")==0) 
	 {
	   ret = ioctl(rozofs_fuse_ctx_p->fd,3,NULL);  
	   if (ret < 0) 
	   {
            pChar += sprintf(pChar, "ioctl failed %s\n",strerror(errno));
	   }
	   else
	   {
             pChar += sprintf(pChar, "directory attributes are not invalidated on mknod/rmdir/unlink...\n");
	     rozofs_fuse_ctx_p->dir_attr_invalidate = 0;
	   }
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;
         }
         pChar += sprintf(pChar, "unsupported argument %s\n",argv[2]);
	 rozofs_fuse_show_usage(pChar);
	 uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	 return;
      }
      if (strcmp(argv[1],"pagecache")==0) 
      {
	 if (rozofs_fuse_ctx_p->fuse_path_solved==0)
	 {
           pChar += sprintf(pChar, "ioctl not supported with that fuse kernel version\n");
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;
	 }
	 errno = 0;
	 if (argv[2] == NULL)
	 {
           pChar += sprintf(pChar, "argument is missing\n");
	   rozofs_fuse_show_usage(pChar);
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;	  	  
	 }
	 if (strcmp(argv[2],"enable")==0) 
	 {
	   conf.pagecache = 1;
           pChar += sprintf(pChar, "direct page cache write is now enabled\n");
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;
         }
	 if (strcmp(argv[2],"disable")==0) 
	 {
	   conf.pagecache = 0;
           pChar += sprintf(pChar, "direct page cache write is now disabled\n");
	   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	   return;
         }
         pChar += sprintf(pChar, "unsupported argument %s\n",argv[2]);
	 rozofs_fuse_show_usage(pChar);
	 uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	 return;
      }
 
 
      pChar += sprintf(pChar, "unsupported command %s\n",argv[1]);
      rozofs_fuse_show_usage(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
      return;
  }
  uint64_t old_ticker = rozofs_fuse_req_tic;
  rozofs_fuse_get_ticker();  
  buffer_count      = ruc_buf_getFreeBufferCount(rozofs_fuse_ctx_p->fuseReqPoolRef);
  /*
  ** check if the session has been exited
  */
  if (fuse_session_exited(rozofs_fuse_ctx_p->se)) sprintf(status,"exited");
  else                                            sprintf(status,"running");
  
  pChar +=  sprintf(pChar,"FUSE %8s - %d/%d ctx remaining\n",
               status, buffer_count, rozofs_fuse_ctx_p->initBufCount);
  /*
  ** print the fusectl channel path
  */
  if (rozofs_fuse_ctx_p->dev < 0)
  {
    pChar +=  sprintf(pChar,"fusectl    : Unknown\n");  
  }
  else
  {
    pChar +=  sprintf(pChar,"fusectl    : /sys/fs/fuse/connections/%d (%s)\n",rozofs_fuse_ctx_p->dev,(rozofs_fuse_ctx_p->fuse_path_solved==0)?"NOT SOLVED":"SOLVED");  
  
  }
    pChar +=  sprintf(pChar,"pagecache  : %s/%s\n",(conf.pagecache)?"Enabled":"Disabled",(rozofs_fuse_ctx_p->dev > 0)?"Supported":"Not supported");    	       
  /*
  ** display the cache mode
  */
  pChar +=  sprintf(pChar,"buffer sz  : %d\n",rozofs_fuse_ctx_p->bufsize); 
  pChar +=  sprintf(pChar,"poll count : %d\n",rozofs_fuse_loop_count); 
  pChar +=  sprintf(pChar,"dir attr   : %s\n",(rozofs_fuse_ctx_p->dir_attr_invalidate)?"INVALIDATE":"KEEP"); 
  pChar +=  sprintf(pChar,"FS Mode    : "); 
  if (rozofs_mode== 0)
  {
    pChar +=  sprintf(pChar,"standard\n");    
  }
  else
  {
    pChar +=  sprintf(pChar,"Block\n");      
  }  
  pChar +=  sprintf(pChar,"FS Xattr   : %s\n",(rozofs_xattr_disable==1)?"Disabled":"Enabled");   
  pChar +=  sprintf(pChar,"cache Mode : ");      
    switch (rozofs_cache_mode)
  {
    default:
    case 0:
     pChar +=  sprintf(pChar,"default\n");  
     break;    
   case 1:
     pChar +=  sprintf(pChar,"direct_io\n");  
     break;    
   case 2:
     pChar +=  sprintf(pChar,"keep_cache\n");  
     break;    
  }
  int i;
  for (i = 0; i < RZ_FUSE_WRITE_MAX; i++)
  {
     pChar +=sprintf(pChar,"cpt_%d: %8llu\n",i,(long long unsigned int)rozofs_write_merge_stats_tab[i]);  
  }
  /**
  * clear the stats
  */
  uint64_t  delay = rozofs_fuse_req_tic-old_ticker;

  memset(rozofs_write_merge_stats_tab,0,sizeof(uint64_t)*RZ_FUSE_WRITE_MAX);
  pChar +=sprintf(pChar,"fuse req_in (count/bytes): %8llu/%llu\n",(long long unsigned int)rozofs_fuse_req_count,
                                                    (long long unsigned int)rozofs_fuse_req_byte_in);  
  pChar +=sprintf(pChar,"fuse time                :%8llu (%llu)\n",
          (long long unsigned int)(fuse_profile[P_COUNT]?fuse_profile[P_ELAPSE]/fuse_profile[P_COUNT]:0),
          (long long unsigned int)fuse_profile[P_COUNT]);

  if (delay)
  {
  pChar +=sprintf(pChar,"fuse req_in/s            : %8llu/%llu\n",(long long unsigned int)(rozofs_fuse_req_count*1000000/delay),
                                                   (long long unsigned int)(rozofs_fuse_req_byte_in*1000000/delay));
  }

  pChar +=sprintf(pChar,"fuse req_in EAGAIN/ENOENT: %8llu/%llu\n",(long long unsigned int)rozofs_fuse_req_eagain_count,
                                                     (long long unsigned int)rozofs_fuse_req_enoent_count);  

  pChar +=sprintf(pChar,"fuse buffer depletion    : %8llu\n",(long long unsigned int)rozofs_fuse_buffer_depletion_count);  
  pChar +=sprintf(pChar,"fuse rcvbuf depletion    : %8llu\n",(long long unsigned int)rozofs_fuse_rcv_buf_depletion_count);  
  pChar +=sprintf(pChar,"storcli buffer depletion : %8llu\n",(long long unsigned int)rozofs_storcli_buffer_depletion_count);
  pChar +=sprintf(pChar,"pending storcli requests : %8d\n",rozofs_storcli_pending_req_count);
  pChar +=sprintf(pChar,"fuse kernel xoff/xon     : %8llu/%llu\n",(long long unsigned int)rozofs_storcli_xoff_count,
                                                                   (long long unsigned int)rozofs_storcli_xon_count);

  rozofs_storcli_buffer_depletion_count =0;
  rozofs_fuse_buffer_depletion_count =0;
  rozofs_fuse_rcv_buf_depletion_count =0;
  rozofs_fuse_req_count = 0;
  rozofs_fuse_req_byte_in = 0;
  rozofs_fuse_req_eagain_count = 0;
  rozofs_fuse_req_enoent_count = 0;
  rozofs_storcli_xoff_count = 0;
  rozofs_storcli_xon_count = 0;
  /**
  *  read/write statistics
  */
  pChar +=sprintf(pChar,"big write count           : %8llu\n",(long long unsigned int)rozofs_fuse_read_write_stats_buf.big_write_cpt);  
  pChar +=sprintf(pChar,"big write count iotcl     : %8llu\n",(long long unsigned int)rozofs_fuse_read_write_stats_buf.big_write_ioctl_cpt);  
  pChar +=sprintf(pChar,"small write count ioctl   : %8llu\n",(long long unsigned int)rozofs_fuse_read_write_stats_buf.small_write_ioctl_cpt);  
  pChar +=sprintf(pChar,"flush buf. count          : %8llu\n",(long long unsigned int)rozofs_fuse_read_write_stats_buf.flush_buf_cpt);  
  pChar +=sprintf(pChar,"  start aligned/unaligned : %8llu/%llu\n",
                 (long long unsigned int)rozofs_aligned_write_start[0],
                 (long long unsigned int)rozofs_aligned_write_start[1]
		 );  
  pChar +=sprintf(pChar,"  end aligned/unaligned   : %8llu/%llu\n",
                (long long unsigned int)rozofs_aligned_write_end[0],
                (long long unsigned int)rozofs_aligned_write_end[1]
		);  
  pChar +=sprintf(pChar,"readahead count           : %8llu\n",(long long unsigned int)rozofs_fuse_read_write_stats_buf.readahead_cpt);  
  pChar +=sprintf(pChar,"read req. count           : %8llu\n",(long long unsigned int)rozofs_fuse_read_write_stats_buf.read_req_cpt);  
  pChar +=sprintf(pChar,"read fuse count           : %8llu\n",(long long unsigned int)rozofs_fuse_read_write_stats_buf.read_fuse_cpt);  
  
  memset(&rozofs_fuse_read_write_stats_buf,0,sizeof(rozofs_fuse_read_write_stats));
  {
    int k;
    for (k= 0;k< 2;k++)
    {
      rozofs_aligned_write_start[k] = 0;
      rozofs_aligned_write_end[k] = 0;
    }
  }
  /*
  ** Per array statistics
  */
  pChar +=sprintf(pChar,"Per Read Array statitics:\n" );  
  for (i = 0; i < ROZOFS_FUSE_NB_OF_BUSIZE_SECTION_MAX; i++)
  {
     if (rozofs_read_buf_section_table[i]!= 0)
       pChar +=sprintf(pChar,"  %6d: %8llu\n",(i+1)*ROZOFS_PAGE_SZ,(long long unsigned int)rozofs_read_buf_section_table[i]);  
  }
  pChar +=sprintf(pChar,"Per Write Array statitics:\n" );  
  for (i = 0; i < ROZOFS_FUSE_NB_OF_BUSIZE_SECTION_MAX; i++)
  {
     if (rozofs_write_buf_section_table[i]!= 0)
       pChar +=sprintf(pChar,"  %6d: %8llu\n",(i+1)*ROZOFS_PAGE_SZ,(long long unsigned int)rozofs_write_buf_section_table[i]);  
  }
  memset (rozofs_write_buf_section_table,0,sizeof(uint64_t)*ROZOFS_FUSE_NB_OF_BUSIZE_SECTION_MAX);
  memset (rozofs_read_buf_section_table,0,sizeof(uint64_t)*ROZOFS_FUSE_NB_OF_BUSIZE_SECTION_MAX);
  
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}





/*__________________________________________________________________________
*/
/**
*   entry point for fuse socket polling
*

   @param current_time : current time provided by the socket controller
   
   
   @retval none
*/
void rozofs_fuse_scheduler_entry_point(uint64_t current_time)
{
  rozofs_fuse_rcvMsgsock((void*)rozofs_fuse_ctx_p,rozofs_fuse_ctx_p->fd);
}

/*
**__________________________________________________________________________
*/
/**
*  Init of the pseudo fuse thread 

  @param ch : initial channel
  @param se : initial session
  @param rozofs_fuse_buffer_count : number of request buffers (corresponds to the number of fuse save context)  
  
  @retval 0 on success
  @retval -1 on error
*/
int rozofs_fuse_init(struct fuse_chan *ch,struct fuse_session *se,int rozofs_fuse_buffer_count)
{
  int status = 0;
  int i;
  rozofs_cnx_param_t cnx_param;
  
//   return 0;

  fuse_sharemem_init_done = 0;   
  
  int fileflags;
  rozofs_fuse_ctx_p = malloc(sizeof (rozofs_fuse_ctx_t));
  if (rozofs_fuse_ctx_p == NULL) 
  {
    /*
    ** cannot allocate memory for fuse rozofs context
    */
    return -1;
  }
  memset(rozofs_fuse_ctx_p,0,sizeof (rozofs_fuse_ctx_t));
  /*
  ** clear read/write merge stats table
  */
  memset(rozofs_write_merge_stats_tab,0,sizeof(uint64_t)*RZ_FUSE_WRITE_MAX);
  memset (rozofs_write_buf_section_table,0,sizeof(uint64_t)*ROZOFS_FUSE_NB_OF_BUSIZE_SECTION_MAX);
  memset (rozofs_read_buf_section_table,0,sizeof(uint64_t)*ROZOFS_FUSE_NB_OF_BUSIZE_SECTION_MAX);
  memset(&rozofs_fuse_read_write_stats_buf,0,sizeof(rozofs_fuse_read_write_stats));
  /*
  ** init of the context
  */
  rozofs_fuse_ctx_p->fuseReqPoolRef = NULL;
  rozofs_fuse_ctx_p->ch             = NULL;
  rozofs_fuse_ctx_p->se             = se;
  rozofs_fuse_ctx_p->bufsize        = 0; 
  rozofs_fuse_ctx_p->buf_fuse_req_p = NULL;
  rozofs_fuse_ctx_p->dir_attr_invalidate = 0;
  rozofs_fuse_ctx_p->initBufCount   = rozofs_fuse_buffer_count;
  
  while (1)
  {
     /*
     ** get the receive buffer size for former channel in order to create the request distributor:
     ** note: by default the fuse buffer is 4K+128K: for RozoFS the payload can reach 512K (x4)
     */
     int bufsize = fuse_chan_bufsize(ch)*8;
     rozofs_fuse_ctx_p->bufsize = bufsize;
     /*
     ** create the distributor fro receiving data from fuse kernel
     */
     status = rozofs_fuse_init_rcv_buffer_pool(ROZOFS_FUSE_RECV_BUF_COUNT,bufsize);
     if (status < 0)
     {
        severe( "rozofs_fuse_init fuse buffer pool creation error(%d,%d)", (int)ROZOFS_FUSE_RECV_BUF_COUNT, (int)bufsize ) ;
        status = -1;
        break;     
     
     }
     /*
     ** create the pool
     */
     rozofs_fuse_ctx_p->fuseReqPoolRef = ruc_buf_poolCreate(rozofs_fuse_buffer_count,sizeof(rozofs_fuse_save_ctx_t));
     if (rozofs_fuse_ctx_p->fuseReqPoolRef == NULL)
     {
        severe( "rozofs_fuse_init buffer pool creation error(%d,%d)", (int)rozofs_fuse_buffer_count, (int)sizeof(rozofs_fuse_save_ctx_t) ) ;
        status = -1;
        break;
     }
     ruc_buffer_debug_register_pool("fuseCtx",  rozofs_fuse_ctx_p->fuseReqPoolRef);
     /*
     ** Allocate a head of list for queueing pending lookup requests
     */
     for (i=0; i< ROZOFS_MAX_LKUP_QUEUE ; i++)
     {
       ruc_listHdrInit(&rozofs_lookup_queue[i]);
     }
     
     
     /*
     ** allocate a buffer for receiving the fuse request
     */
     rozofs_fuse_ctx_p->buf_fuse_req_p = malloc(bufsize);
     if (rozofs_fuse_ctx_p == NULL) 
     {     
        severe( "rozofs_fuse_init out of memory %d", bufsize ) ;
        status = -1;
        break;     
     }
     /*
     ** get the fd of the channel
     */
     rozofs_fuse_ctx_p->fd = fuse_chan_fd(ch);
     /*
     ** wait the end of the share memroy init prior providing it to fuse
     */
     while (rozofs_shared_mem_init_done == 0) sleep(1);  
     /*
     ** create a new channel with the specific operation for rozofs (non-blocking)
     */  
     rozofs_fuse_ctx_p->ch = fuse_chan_new(&rozofs_fuse_ch_ops,fuse_chan_fd(ch),bufsize,rozofs_fuse_ctx_p);  
     if (rozofs_fuse_ctx_p->ch == NULL)
     {
        severe( "rozofs_fuse_init fuse_chan_new error"  ) ;
        status = -1;
        break;          
     }
     /*
     ** remove the association between the initial session and channel
     */
     fuse_session_remove_chan(ch);  
     /*
     ** OK, now add the new channel
     */
     fuse_session_add_chan(se,rozofs_fuse_ctx_p->ch );  
     /*
     ** set the channel in non blocking mode
     */
     if((fileflags=fcntl(rozofs_fuse_ctx_p->fd,F_GETFL,0))==-1)
     {
       RUC_WARNING(errno);
       status = -1; 
       break;   
     }

     if((fcntl(rozofs_fuse_ctx_p->fd,F_SETFL,fileflags|O_NDELAY))==-1)
     {
       RUC_WARNING(errno);
       status = -1; 
       break;   
     }
     /*
     ** send XON to the fuse channel
     */
     {
        unsigned long rozofs_bypass_size;
        int ret;
	rozofs_fuse_ctx_p->ioctl_supported = 1;
	rozofs_fuse_ctx_p->data_xon        = 1;
     
        while(1)
	{	
	  ret = ioctl(rozofs_fuse_ctx_p->fd,1,NULL);
	  if (ret < 0) 
	  {
	     warning("ioctl error %s",strerror(errno));
	     rozofs_fuse_ctx_p->ioctl_supported = 0;
	     break;

	  }
#if 0
          if (rozofs_fuse_ctx_p->dir_attr_invalidate == 0)
	  {
	    ret = ioctl(rozofs_fuse_ctx_p->fd,3,NULL);
	    if (ret < 0) 
	    {
	       warning("ioctl error %s",strerror(errno));
//	       rozofs_fuse_ctx_p->ioctl_supported = 0;
	       rozofs_fuse_ctx_p->dir_attr_invalidate = 1;
	       break;
	    }		
	  }
#endif
	   rozofs_bypass_size = ROZOFS_MAX_FILE_BUF_SZ;
	   ioctl(rozofs_fuse_ctx_p->fd,6,&rozofs_bypass_size);
         /*
	 ** apply the fuse profile
	 */
	 {
	    rozofs_fuse_profile_t *fuse_profile_p;
	    
	    fuse_profile_p = &fuse_kern_profile[conf.idx_fuse_profile];
	    
	    cnx_param.read = 0;
	    cnx_param.to_set = ROZOFS_RA_PAGE | ROZOFS_MAX_BACKGROUND|ROZOFS_CONGESTION_THRESHOLD ;
	    cnx_param.ra_pages = fuse_profile_p->ra_pages;
	    cnx_param.max_background = fuse_profile_p->max_background;
	    cnx_param.congestion_threshold = fuse_profile_p->congestion_threshold;
	    ioctl(rozofs_fuse_ctx_p->fd,7,&cnx_param);
	  }
	  break;     
        }
     }
    /*
    **  Get the device within fusectl mount point
    */
    while(1)
    {
       rozofs_cnx_param_t  rozofs_fuse_cnx_param;
       rozofs_fuse_ctx_p->dev = -1;
       ioctl_rozofs_mountpath_t ioctl_mount;
       int fd_fusectl;
       char pathname[1024];
       /*
       **  RozoFS fuse connection parameters
       */

       memset(&rozofs_fuse_cnx_param,0,sizeof(rozofs_cnx_param_t));

       if (rozofs_fuse_ctx_p->ioctl_supported == 0) break;
	rozofs_fuse_cnx_param.read = 1;
	ioctl(rozofs_fuse_ctx_p->fd,7,&rozofs_fuse_cnx_param);  
	if (rozofs_fuse_cnx_param.dev == 0) break ;
	rozofs_fuse_ctx_p->dev = rozofs_fuse_cnx_param.dev;
	/*
	** Now attempt to fill up the mount path on the fuse connection. It is needed by
	** the storcli in order to be able to open the inode on which mojette transform should apply
	*/
	sprintf(pathname,"/sys/fs/fuse/connections/%d/rozofs",rozofs_fuse_ctx_p->dev);  
	if ((fd_fusectl = open(pathname,O_RDONLY)) < 0)
	{
	    info("RozoFS Cannot open %s (%s)",pathname,strerror(errno));
            break;
	}
	strcpy(ioctl_mount.name,rozofs_mountpoint);
	ioctl_mount.status = -1;
        ioctl(fd_fusectl,ROZOFS_FUSECTL_IOCTL_MOUNT,&ioctl_mount); 
	/*
	** check the the kernel has been able to solve the mountpath in order to get the mount structure of the kernel
	*/
	if (ioctl_mount.status == 0) rozofs_fuse_ctx_p->fuse_path_solved = 1;
	else
	{
	    info("RozoFS error while providing mountpath to kernel: direct inode open disabled");	
	}
	/*
	** close the channel open on fusectl for that mountpoint
	*/
	close(fd_fusectl);
	break;
    } 
    /*
     ** perform the connection with the socket controller
     */
   /*
   ** OK, we are almost done, just need to connect with the socket controller
   */
   rozofs_fuse_ctx_p->connectionId = ruc_sockctl_connect(rozofs_fuse_ctx_p->fd,  // Reference of the socket
                                              "rozofs_fuse",                 // name of the socket
                                              3,                             // Priority within the socket controller
                                              (void*)rozofs_fuse_ctx_p,      // user param for socketcontroller callback
                                              &rozofs_fuse_callBack_sock);   // Default callbacks
    if (rozofs_fuse_ctx_p->connectionId == NULL)
    {
       /*
       ** Fail to connect with the socket controller
       */
       RUC_WARNING(-1);
       status = -1; 
       break;   
    } 
    rozofs_fuse_get_ticker();

     status = 0;
     break;
  }
  /*
  ** attach the callback on socket controller
  */
//#warning no poller
  ruc_sockCtrl_attach_applicative_poller(rozofs_fuse_scheduler_entry_point); 
  for(i = 0; i < 3;i++) fuse_profile[i] = 0;
  
  uma_dbg_addTopic("fuse", rozofs_fuse_show);
  return status;
  
}


void rozofs_fuse_share_mem_init()
{
  if (fuse_sharemem_enable==0) 
  {
     /*
     ** fake init done
     */
     fuse_sharemem_init_done = 1;
     return;
  }
  if (fuse_sharemem_init_done == 1) return;

  fuse_ino_t ino = rozofs_storcli_shared_mem[SHAREMEM_IDX_READ].buf_sz;
  ino *=rozofs_storcli_shared_mem[SHAREMEM_IDX_READ].buf_count;
  struct fuse_bufvec bufvec_fake;
  off_t offset = (off_t) rozofs_storcli_shared_mem[SHAREMEM_IDX_READ].data_p; 
  int ret;  
  char bufall[16];
  memset(&bufvec_fake,0,sizeof(bufvec_fake));
  bufvec_fake.count = 1;
  bufvec_fake.buf[0].size=16;
  bufvec_fake.buf[0].mem=&bufall;
  bufvec_fake.buf[0].fd=-1;
  bufvec_fake.buf[0].pos=0;

  errno = 0;
  fuse_sharemem_init_done =1;
#if 0
  ret = fuse_lowlevel_notify_store(rozofs_fuse_ctx_p->ch, ino,
			     offset, &bufvec_fake,
			     0);
  info("Fuse share memory init %s",(ret==0)?"Success":strerror(errno));
#endif 
}
