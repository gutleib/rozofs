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
#include <stddef.h>
#include <stdint.h>
#include <errno.h>
#include <sched.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/profile.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/core/ruc_buffer_debug.h>
#include "rozofs_bt_proto.h"
//#include "rozofs_bt_thread_intf.h"
#include <rozofs/core/rozofs_socket_family.h>
#include "rozofs_bt_inode.h"
#include "rozofs_bt_dirent.h"
#include "rozofs_fuse_api.h"
#include <rozofs/rpc/eproto.h>
#include <rozofs/core/rozofs_fid_string.h>


 
int rozofs_bt_thread_create( int nb_threads) ;


 /**
 * prototypes
 */
uint32_t af_unix_bt_dirent_rcvReadysock(void * af_unix_bt_dirent_ctx_p,int socketId);
uint32_t af_unix_bt_dirent_rcvMsgsock(void * af_unix_bt_dirent_ctx_p,int socketId);
uint32_t af_unix_bt_dirent_xmitReadysock(void * af_unix_bt_dirent_ctx_p,int socketId);
uint32_t af_unix_bt_dirent_xmitEvtsock(void * af_unix_bt_dirent_ctx_p,int socketId);
static int af_unix_bt_dirent_response_socket_create(char *socketname);
void af_unix_bt_dirent_response(void *ruc_buffer);
int rozofs_parse_dirfile(fuse_req_t req,dirbuf_t *db,uint64_t cookie_client,int client_size);
int rozofs_ll_readdir2_send_to_export_no_batch(fid_t fid, uint64_t cookie,void	 *buffer_p);
/*
** external Prototypes
*/  
int rozofs_bt_get_slave_inode(rozofs_bt_tracking_cache_t *tracking_ret_p,rozofs_inode_t *rozofs_inode_p,ientry_t *ie);


/*
** data structures & definition
*/
#define ROZOFS_READDIR2_BUFSIZE (64*1024)
DECLARE_PROFILING(mpp_profiler_t);


#define DISK_SO_SENDBUF  (300*1024)
#define MAIN_DIRENT_SOCKET_NICKNAME "dirent_rsp_th"
#define ROZOFS_BT_DIRENT_LKPUP_MAX_THREADS 1
#define ROZOFS_BT_DIRENT_QDEPTH (1024+512)

/*
** Global data
*/
int        af_unix_bt_dirent_south_socket_ref = -1;
int        af_unix_bt_dirent_thread_count=0;
int        af_unix_bt_dirent_pending_req_count = 0;
int        dirent_lookup_thread_ready = 0;

struct  sockaddr_un rozofs_bt_dirent_south_socket_name;
rozofs_queue_t dirent_queue_lookup;
rozofs_bt_thread_ctx_t rozofs_bt_dirent_lkup_thread_ctx_tb[ROZOFS_BT_DIRENT_LKPUP_MAX_THREADS];

/*
**  Call back function for socket controller
*/
ruc_sockCallBack_t af_unix_bt_dirent_callBack_sock=
  {
     af_unix_bt_dirent_rcvReadysock,
     af_unix_bt_dirent_rcvMsgsock,
     af_unix_bt_dirent_xmitReadysock,
     af_unix_bt_dirent_xmitEvtsock
  };

  /*
**__________________________________________________________________________
*/
/**
  Application callBack:

  Called from the socket controller. 

serial
  @param unused: not used
  @param socketId: reference of the socket (not used)
 
  @retval : always FALSE
*/

uint32_t af_unix_bt_dirent_xmitReadysock(void * unused,int socketId)
{

    return FALSE;
}


/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   Called from the socket controller upon receiving a xmit ready event
   for the associated socket. That callback is activeted only if the application
   has replied TRUE in rozofs_bt_xmitReadysock().
   
   It typically the processing of a end of congestion on the socket

    
  @param unused: not used
  @param socketId: reference of the socket (not used)
 
   @retval :always TRUE
*/
uint32_t af_unix_bt_dirent_xmitEvtsock(void * unused,int socketId)
{
   
    return TRUE;
}
/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   receiver ready function: called from socket controller.
   The module is intended to return if the receiver is ready to receive a new message
   and FALSE otherwise

    
  @param unused: not used
  @param socketId: reference of the socket (not used)
 
  @retval : TRUE-> receiver ready
  @retval : FALSE-> receiver not ready
*/

uint32_t af_unix_bt_dirent_rcvReadysock(void * unused,int socketId)
{
  return TRUE;
}



/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   Called from the socket controller when there is a message pending on the
   socket associated with the context provide in input arguments.
   
   That service is intended to process a response sent by a disk thread

    
  @param unused: user parameter not used by the application
  @param socketId: reference of the socket 
 
   @retval : TRUE-> xmit ready event expected
  @retval : FALSE-> xmit  ready event not expected
*/

uint32_t af_unix_bt_dirent_rcvMsgsock(void * unused,int socketId)
{
  uint64_t   msg;
  int                      bytesRcvd;
  int eintr_count = 0;
  


  /*
  ** disk responses have the highest priority, loop on the socket until
  ** the socket becomes empty
  */
  while(1) {  
    /*
    ** check if there are some pending requests
    */
//    if (af_unix_bt_dirent_pending_req_count == 0)
//    {
//     return TRUE;
//    }
    /*
    ** read the north disk socket
    */
    bytesRcvd = recvfrom(socketId,
			 &msg,sizeof(msg), 
			 0,(struct sockaddr *)NULL,NULL);
    if (bytesRcvd == -1) {
     switch (errno)
     {
       case EAGAIN:
        /*
        ** the socket is empty
        */
        return TRUE;

       case EINTR:
         /*
         ** re-attempt to read the socket
         */
         eintr_count++;
         if (eintr_count < 3) continue;
         /*
         ** here we consider it as a error
         */
         severe ("Fuse Thread Response error too many eintr_count %d",eintr_count);
         return TRUE;

       case EBADF:
       case EFAULT:
       case EINVAL:
       default:
         /*
         ** We might need to double checl if the socket must be killed
         */
         fatal("Fuse Thread Response error on recvfrom %s !!\n",strerror(errno));
         exit(0);
     }

    }
    if (bytesRcvd == 0) {
      fatal("Fuse Thread Response socket is dead %s !!\n",strerror(errno));
      exit(0);    
    } 
//    af_unix_bt_dirent_pending_req_count--;
//    if (  af_unix_bt_dirent_pending_req_count < 0) af_unix_bt_dirent_pending_req_count = 0;
    af_unix_bt_dirent_response((void*)msg); 
  }       
  return TRUE;
}


/*
**__________________________________________________________________________
*/
/**
* fill the storio  AF_UNIX name in the global data

  @param hostname
  @param socketname : pointer to a sockaddr_un structure
  
  @retval none
*/
static void rozofs_bt_dirent_set_sockname_with_hostname(struct sockaddr_un *socketname,char *name,char *hostname,int instance_id)
{
  socketname->sun_family = AF_UNIX;  
  char * pChar = socketname->sun_path;
  pChar += rozofs_string_append(pChar,name);
  *pChar++ = '_';
  *pChar++ = 'D';
  *pChar++ = 'I';
  *pChar++ = 'R';
  *pChar++ = 'E';
  *pChar++ = 'N';  
  *pChar++ = 'T';  
  *pChar++ = '_';
  pChar += rozofs_u32_append(pChar,instance_id);
  *pChar++ = '_';  
  pChar += rozofs_string_append(pChar,hostname);
}

/*
**__________________________________________________________________________
*/
/**
*  Thar API is intended to be used by a dirent thread for sending the response to ROZOFS_BT_DIRENT_READDIR or ROZOFS_BT_DIRENT_GET_DENTRY

   
   @param thread_ctx_p: pointer to the thread context (contains the thread source socket )
   @param ruc_buffer: pointer to the ruc_buffer that contains the response
   
   @retval none
*/
void rozofs_bt_dirent_th_send_response (rozofs_bt_thread_ctx_t *thread_ctx_p, void *ruc_buffer) 
{  
  int ret;
  /*
  ** send back the response
  */  
  ret = sendto(thread_ctx_p->sendSocket,ruc_buffer, sizeof(void *),0,(struct sockaddr*)&rozofs_bt_dirent_south_socket_name,sizeof(rozofs_bt_dirent_south_socket_name));
  if (ret <= 0) {
     fatal("rozofs_bt_dirent_th_send_response %d sendto(%s) %s", thread_ctx_p->thread_idx, rozofs_bt_dirent_south_socket_name.sun_path, strerror(errno));
     exit(0);  
  }
}



/*
**__________________________________________________________________________
*/

/**
* creation of the AF_UNIX socket that is attached on the socket controller

  That socket is used to receive back the response from the threads that
  perform disk operation (read/write/truncate)
  
  @param socketname : name of the socket
  
  @retval >= 0 : reference of the socket
  @retval < 0 : error
*/
static int af_unix_bt_dirent_response_socket_create(char *socketname)
{
  int len;
  int fd = -1;
  void *sockctrl_ref;

   len = strlen(socketname);
   if (len >= AF_UNIX_SOCKET_NAME_SIZE)
   {
      /*
      ** name is too big!!
      */
      severe("socket name %s is too long: %d (max is %d)",socketname,len,AF_UNIX_SOCKET_NAME_SIZE);
      return -1;
   }
   while (1)
   {
     /*
     ** create the socket
     */
     fd = af_unix_sock_create_internal(socketname,DISK_SO_SENDBUF);
     if (fd == -1)
     {
       break;
     }
     /*
     ** OK, we are almost done, just need to connect with the socket controller
     */
     sockctrl_ref = ruc_sockctl_connect(fd,  // Reference of the socket
                                                MAIN_DIRENT_SOCKET_NICKNAME,   // name of the socket
                                                16,                  // Priority within the socket controller
                                                (void*)NULL,      // user param for socketcontroller callback
                                                &af_unix_bt_dirent_callBack_sock);  // Default callbacks
      if (sockctrl_ref == NULL)
      {
         /*
         ** Fail to connect with the socket controller
         */
         fatal("error on ruc_sockctl_connect");
         break;
      }
      /*
      ** All is fine
      */
      break;
    }    
    return fd;
}



/*
**____________________________________________________________________
*/
/**
*  attempt to perform a local readdir if the dirent files of the directory are available

   @param  eid: export identifier
   @param  parent_fid: fid of the parent directory
   @param  name: name for which we want the inode
   @param  fuse_ctx_p: fuse context
         
   @retval 0 on success
   @retval -1 on error (see errno for details)
*/
int rozofs_bt_lookup_req_from_main_thread(uint32_t eid,fid_t parent_fid,char *name,void *fuse_ctx_p)
{

   rozofs_inode_t *rozofs_inode_p;   
   ext_mattr_t * ext_attr_parent_p;
   int dirent_valid;
   uint64_t inode;
   void *xmit_buf;
   rozofs_bt_tracking_cache_t *tracking_ret_p = NULL;
   bt_dirent_msg_t  *lookup_rq_p;

   /*
   ** we do not care about @rozofs_uuid@xxxx-xxxxx-xxxx : this request is always sent to the export
   */
  if ((strncmp(name,"@rozofs_uuid@",13) == 0) ||(strncmp(name,ROZOFS_DIR_TRASH,strlen(ROZOFS_DIR_TRASH)) == 0))
  {
    errno = EAGAIN;
    return -1;
  }   
   rozofs_inode_p = (rozofs_inode_t*)parent_fid;
   inode = rozofs_inode_p->fid[1];
   ext_attr_parent_p = rozofs_bt_load_dirent_from_main_thread(inode,&tracking_ret_p,&dirent_valid);
   /*
   ** we stop here if either the parent attributes are not available or the dirent file are not up to date
   */
   if ((ext_attr_parent_p == NULL) || (dirent_valid == 0))
   {
     FDL_INFO ("FDL rozofs_bt_lookup_req_from_main_thread not valid ext_attr_parent_p %p dirent_valid %d\n",ext_attr_parent_p,dirent_valid);
     return -1;
   }
  /*
  ** allocate a buffer for the readdir: we allocate a large buffer since it is also used
  ** to provide the result of the readdir
  */
  /*
  ** allocate a buffer for the readdir: we allocate a large buffer since it is also used
  ** to provide the result of the readdir
  */
  xmit_buf = ruc_buf_getBuffer(ROZOFS_TX_LARGE_RX_POOL);
  if (xmit_buf == NULL)
  {
     TX_STATS(ROZOFS_TX_NO_BUFFER_ERROR);
     severe("Out of large receive buffer in transaction pool");
     errno = ENOMEM;
     return -1;
  }
  lookup_rq_p  = (bt_dirent_msg_t*) ruc_buf_getPayload(xmit_buf);

  lookup_rq_p->hdr.xid          = 0; /**< not used */
  lookup_rq_p->hdr.opcode       = ROZOFS_BT_DIRENT_GET_DENTRY;
  lookup_rq_p->hdr.user_ctx     = fuse_ctx_p;
  lookup_rq_p->hdr.queue_prio   = 0; /* not used */
  lookup_rq_p->hdr.rozofs_queue = NULL; /* not used */
  /*
  ** arguments
  */
  lookup_rq_p->s.lookup_rq.eid = eid;
  memcpy(lookup_rq_p->s.lookup_rq.parent_fid,parent_fid,sizeof(fid_t));
  lookup_rq_p->s.lookup_rq.name = name;
  /*
  ** send the message on the dirent thread queue
  */
  rozofs_queue_put(&dirent_queue_lookup,xmit_buf);

  return 0;

}


/*
 *_______________________________________________________________________
 */
 /**
 *  Perform a lookup on the local dirent file
 
   @param ruc_buffer: buffer that contains the lookup request, the same buffer is used for the response
     eid: export identifier
     parent_fid : inode of the parent directory
     name: name to search
     
   @retval none
*/ 
 void rozofs_bt_lookup_req(rozofs_bt_thread_ctx_t * thread_ctx_p,void *ruc_buffer)
 {


    bt_dirent_msg_t  *lookup_rq_p;
    int ret;
    bt_lookup_ret_t  *lookup_rsp_p;
    fid_t child_fid;
    fid_t parent_fid;
    char *name;
    
    lookup_rq_p  = (bt_dirent_msg_t*) ruc_buf_getPayload(ruc_buffer);
    lookup_rsp_p  = &lookup_rq_p->s.lookup_rsp;
    memcpy(parent_fid,lookup_rq_p->s.lookup_rq.parent_fid,sizeof(fid_t));      

    /*
    ** now attempt to get the inode for the required name
    */
    name = lookup_rq_p->s.lookup_rq.name;
    memcpy(lookup_rsp_p->parent_fid,parent_fid,sizeof(fid_t));   
    ret = rozofs_bt_get_mdirentry(parent_fid,name,child_fid);
    if (ret < 0)
    {
      lookup_rsp_p->status = -1;
      lookup_rsp_p->s.errcode = errno;
      FDL_INFO("FDL lookup fail:%s : %s",lookup_rq_p->s.lookup_rq.name,strerror(errno));
    }
    else
    {
      lookup_rsp_p->status = 0;      
      memcpy(lookup_rsp_p->s.child_fid,child_fid,sizeof(fid_t));   
      {      
	char bufall_debug[64];
	char bufall_debug2[64];
	rozofs_fid2string(child_fid,bufall_debug);      
	rozofs_fid2string(lookup_rsp_p->parent_fid,bufall_debug2);      
	FDL_INFO ("FDL rozofs_bt_lookup_req %s fid :%s pfid:%s  ret %d : %s\n",name,bufall_debug,bufall_debug2,ret,strerror(errno));
      }
    }
    /*
    ** send the response to the main thread
    */
    rozofs_bt_dirent_th_send_response(thread_ctx_p,ruc_buffer); 
 }


/*
 *_______________________________________________________________________
 */
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param ruc_buffer : pointer to ruc_buffer that contains the response
 
 @return none
 */

void rozofs_ll_lookup_cbk(void *this,void *param);

void rozofs_bt_lookup_cbk(void *ruc_buffer)
{
   fuse_req_t req; 
   int ret;
   int trc_idx;                /**< index within the trace buffer */
   bt_dirent_msg_t *msg_p;
   void *param;                /**< fuse context associated with the transaction */
   struct fuse_entry_param fep; 
   ientry_t *child_ie = NULL;   /**< child ientry */
   ientry_t    *pie = NULL;     /**< parent ientry */
   fid_t child_fid;             /**< child inode  */
   struct stat stbuf;           /**< buffer use for sending back the attributes of the inode to the kernel */
   rozofs_bt_tracking_cache_t *tracking_ret_p = NULL;  /**< pointer to the tracking cache context that contains the inode */
   int dirent_valid;           /**< assert to one if the dirent file is valid */
   char *name;                 /**< name of the inode to search for */
   errno = 0;
   fuse_ino_t parent;
   rozofs_inode_t *rozofs_inode_p;  
   uint64_t inode; 
   ext_mattr_t * ext_attr_child_p=NULL;
   rozofs_fuse_save_ctx_t *fuse_ctx_p;
   int errcode;
   int status;
   fid_t parent_fid;
   
    /*
    ** get the pointer to the readdir response
    */
    msg_p = (bt_dirent_msg_t*) ruc_buf_getPayload(ruc_buffer);
    /*
    ** restore the fuse context
    */
    param = msg_p->hdr.user_ctx;
    /*
    ** the pointer to the fuse context: need if there is an aggregated lookup for the same name
    */
    GET_FUSE_CTX_P(fuse_ctx_p,param);  
    /*
    ** restore the parameter of the calling application
    */           
    RESTORE_FUSE_PARAM(param,req);
    RESTORE_FUSE_PARAM(param,trc_idx);    
    RESTORE_FUSE_PARAM(param,name);    
    RESTORE_FUSE_PARAM(param,parent);
    /*
    ** check the readdir status
    */
    if (msg_p->s.lookup_rsp.status < 0) {
        errno = msg_p->s.lookup_rsp.s.errcode;
	/*
	** need to take care of the EAGAIN case, since it indicates that the dirent file where not available
	*/	
	if (errno == EAGAIN) goto enoent;
        goto resend2export;
    } 
    /*
    ** We got the inode of the child
    */
    memcpy(child_fid,msg_p->s.lookup_rsp.s.child_fid,sizeof(fid_t));
    /*
    ** We have found the inode, attempt to get the attributes now
    */
    rozofs_inode_p = (rozofs_inode_t*)child_fid;
    inode = rozofs_inode_p->fid[1];
    ext_attr_child_p = rozofs_bt_load_dirent_from_main_thread(inode,&tracking_ret_p,&dirent_valid);
    if (ext_attr_child_p == NULL)
    {
      {      
	char bufall_debug[64];
	rozofs_fid2string(child_fid,bufall_debug);      
	FDL_INFO ("FDL rozofs_bt_lookup_cbk %s fid :%s   ext_attr_child_p %p dirent_valid %s\n",name,bufall_debug,ext_attr_child_p,(dirent_valid!=0)?"VALID":"INVALID");
      }
      goto resend2export;
    }       
    /*
    ** we have both attributes
    */
    if (!(child_ie = get_ientry_by_fid(ext_attr_child_p->s.attrs.fid))) {
	child_ie = alloc_ientry(ext_attr_child_p->s.attrs.fid);
    } 
    /*
    ** among the update, the cache time is also asserted according to the mtime of the regular file, we should do the same for the directories
    */
    rozofs_ientry_update(child_ie,(struct inode_internal_t*)ext_attr_child_p);     
    /*
    ** need to take care of the slave inode here: only for the case of the regular file
    */
    if (S_ISREG(ext_attr_child_p->s.attrs.mode))
    {
       ret = rozofs_bt_get_slave_inode(tracking_ret_p,rozofs_inode_p,child_ie);
    }   
    memset(&fep, 0, sizeof (fep));
    mattr_to_stat((struct inode_internal_t*)ext_attr_child_p, &stbuf,exportclt.bsize);
    /*
    ** use the timestamp of the tracking file as timestamp of the ientry
    */
    child_ie->timestamp = tracking_ret_p->timestamp;
    stbuf.st_ino = child_ie->inode;
    /*
    ** check the case of the directory
    */
    if ((S_ISDIR(ext_attr_child_p->s.attrs.mode)) &&(strncmp(name,"@rozofs_uuid@",13) == 0))
    {
        rozofs_inode_t fake_id;
		
	fake_id.fid[1]= child_ie->inode;
	fake_id.s.key = ROZOFS_DIR_FID;
        fep.ino = fake_id.fid[1];  
    }
    else
    {
      fep.ino = child_ie->inode;
    }
    stbuf.st_size = child_ie->attrs.attrs.size;
    rozofs_inode_p = (rozofs_inode_t *)ext_attr_child_p->s.attrs.fid;
    if (rozofs_inode_p->s.del)
    {
      fep.attr_timeout  = 0;
      fep.entry_timeout = 0;
    }
    else
    {
      fep.attr_timeout = rozofs_get_linux_caching_time_second(child_ie);
      fep.entry_timeout = rozofs_tmr_get_entry(rozofs_is_directory_inode(child_ie->inode));    
    }
    memcpy(&fep.attr, &stbuf, sizeof (struct stat));
    child_ie->nlookup++;

    rozofs_inode_t * finode = (rozofs_inode_t *) child_ie->attrs.attrs.fid;
    fep.generation = finode->fid[0]; 
    pie = get_ientry_by_inode(parent);
    if (pie != NULL)
    {
      /*
      ** set the parent update time in the ientry of the child inode
      */
      child_ie->parent_update_time = rozofs_get_parent_update_time_from_ie(pie);
    }
    rz_fuse_reply_entry(req, &fep);  
    /*
    ** OK now let's check if there was some other lookup request for the same
    ** object
    */

    {
      int trc_idx,i;    
      status = 0;  
      for (i = 0; i < fuse_ctx_p->lkup_cpt;i++)
      {
	 /*
	 ** Check if the inode and the name are the same
	 */
         rz_fuse_reply_entry(fuse_ctx_p->lookup_tb[i].req, &fep);
         child_ie->nlookup++;
	 trc_idx = fuse_ctx_p->lookup_tb[i].trc_idx;
         rozofs_trc_rsp_attr(srv_rozofs_ll_lookup,0xfacebeef,(child_ie==NULL)?NULL:child_ie->attrs.attrs.fid,status,(child_ie==NULL)?-1:child_ie->attrs.attrs.size,trc_idx);
      } 
    }      
    errno = 0;
    goto out;
    
    /*
    **________________________________________________________________________________________________
    ** the child inode information cannot be get locally need to send the request towards the export
    **________________________________________________________________________________________________
    */
resend2export:
    {
      epgw_lookup_arg_t arg;
      memcpy(parent_fid, msg_p->s.lookup_rsp.parent_fid,sizeof(fid_t)); 
      /*
      ** release the buffer that has been allocated from the transaction pool
      */      
      ruc_buf_freeBuffer(ruc_buffer);   
      ruc_buffer = NULL;
      /*
      ** fill up the structure that will be used for creating the xdr message
      */    
      arg.arg_gw.eid = exportclt.eid;
      memcpy(arg.arg_gw.parent,parent_fid, sizeof (uuid_t));
      arg.arg_gw.name = (char*)name;    
      ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,parent_fid,EXPORT_PROGRAM, EXPORT_VERSION,
                        	EP_LOOKUP,(xdrproc_t) xdr_epgw_lookup_arg_t,(void *)&arg,
                        	rozofs_ll_lookup_cbk,param); 

       if (ret < 0) goto error;
       /*
       ** no error just waiting for the answer
       */
       return; 
    }
    /*
    **______________________________________________________________________________
    ** there was an error while we attempt to send the request towards the exportd
    **______________________________________________________________________________
    */
error:
    errcode = errno;
    fuse_reply_err(req, errno);
    /*
    ** OK now let's check if there was some other lookup request for the same
    ** object
    */
    {
      status = -1;
      int trc_idx,i;      
      for (i = 0; i < fuse_ctx_p->lkup_cpt;i++)
      {
	 /*
	 ** Check if the inode and the name are the same
	 */
         fuse_reply_err(fuse_ctx_p->lookup_tb[i].req,errcode);
	 trc_idx = fuse_ctx_p->lookup_tb[i].trc_idx;
	 errno=errcode;
         rozofs_trc_rsp_attr(srv_rozofs_ll_lookup,0xdeadbeef,(child_ie==NULL)?NULL:child_ie->attrs.attrs.fid,status,(child_ie==NULL)?-1:child_ie->attrs.attrs.size,trc_idx);
      }        
    }
    /*
    ** remove the context from the lookup queue
    */
    if (param != NULL) ruc_objRemove(param);
out:

    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp_attr(srv_rozofs_ll_lookup,0xfacebeef,(child_ie==NULL)?NULL:child_ie->attrs.attrs.fid,0,(child_ie==NULL)?-1:child_ie->attrs.attrs.size,trc_idx);   
    STOP_PROFILING_NB(param,rozofs_ll_lookup);
    rozofs_fuse_release_saved_context(param);        
    /*
    ** release the buffer if has been allocated
    */
    if (ruc_buffer != NULL) ruc_buf_freeBuffer(ruc_buffer);   
   
    return;

enoent:
   /*
   ** Case of non existent entry. 
   ** Tell FUSE to keep responding ENOENT for this name for a few seconds
   */
   memset(&fep, 0, sizeof (fep));
   fep.ino = 0;
   fep.attr_timeout  = rozofs_tmr_get_enoent();
   fep.entry_timeout = rozofs_tmr_get_enoent();
   rz_fuse_reply_entry(req, &fep);
   errno = ENOENT;
   errcode = errno;
   /*
   ** OK now let's check if there was some other lookup request for the same
   ** object
   */
   {
     status = -1;
     int trc_idx,i;      
     for (i = 0; i < fuse_ctx_p->lkup_cpt;i++)
     {
	/*
	** Check if the inode and the name are the same
	*/
        rz_fuse_reply_entry(fuse_ctx_p->lookup_tb[i].req, &fep);
	trc_idx = fuse_ctx_p->lookup_tb[i].trc_idx;
	errno=errcode;
        rozofs_trc_rsp_attr(srv_rozofs_ll_lookup,0xdeadbeef,(child_ie==NULL)?NULL:child_ie->attrs.attrs.fid,status,(child_ie==NULL)?-1:child_ie->attrs.attrs.size,trc_idx);
     } 
     errno = errcode;       
   }
   goto out;
}

/*
**____________________________________________________________________
*/
/**
*  attempt to perform a local readdir if the dirent files of the directory are available

   @param fid: fid of the directory
   @param cookie: readdir cookie (opaque to client)
   @param fuse_ctx_p : pointer to the fuse context to handle the readdir request of the application
      
   @retval 0 on success
   @retval -1 on error (see errno for details)
*/
int rozofs_bt_readdir_req_from_main_thread(fid_t fid,uint64_t cookie,void *fuse_ctx_p)
{

  bt_dirent_msg_t  *readdir_rq_p;
  ext_mattr_t * ext_attr_parent_p;
  int dirent_valid;  
  rozofs_inode_t *rozofs_inode_p;
  uint64_t inode;
  void *xmit_buf;
  dirbuf_t   *db=NULL;
  struct fuse_file_info *fi ;
  dir_t *dir_p = NULL;

  RESTORE_FUSE_PARAM(fuse_ctx_p,fi);

  dir_p = (dir_t*)fi->fh;
  dir_p->readdir_pending = 0;
  db = &dir_p->db;
  /*
  ** allocate the readdir buffer if not yet done
  */
  if (db->p == NULL)
  {
    db->p = xmalloc(ROZOFS_READDIR2_BUFSIZE);
  }
  db->size = 0;    
   
  rozofs_inode_p = (rozofs_inode_t*)fid;
  inode = rozofs_inode_p->fid[1];
  
  ext_attr_parent_p = rozofs_bt_load_dirent_from_main_thread(inode,NULL,&dirent_valid);
  if ((ext_attr_parent_p == NULL) || (dirent_valid == 0))
  {
    errno = EAGAIN;
    return -1;
  }
  /*
  ** allocate a buffer for the readdir: we allocate a large buffer since it is also used
  ** to provide the result of the readdir
  */
    xmit_buf = ruc_buf_getBuffer(ROZOFS_TX_LARGE_RX_POOL);
    if (xmit_buf == NULL)
    {
       TX_STATS(ROZOFS_TX_NO_BUFFER_ERROR);
       severe("Out of large receive buffer in transaction pool");
       errno = ENOMEM;
       return -1;
    }
    readdir_rq_p  = (bt_dirent_msg_t*) ruc_buf_getPayload(xmit_buf);
    
    readdir_rq_p->hdr.xid          = 0; /**< not used */
    readdir_rq_p->hdr.opcode       = ROZOFS_BT_DIRENT_READDIR;
    readdir_rq_p->hdr.user_ctx     = fuse_ctx_p;
    readdir_rq_p->hdr.queue_prio   = 0; /* not used */
    readdir_rq_p->hdr.rozofs_queue = NULL; /* not used */
    /*
    ** arguments
    */
    readdir_rq_p->s.readdir_rq.eid = rozofs_bt_dirent_eid;
    memcpy(readdir_rq_p->s.readdir_rq.fid,fid,sizeof(fid_t));
    readdir_rq_p->s.readdir_rq.cookie = cookie;
    readdir_rq_p->s.readdir_rq.db_buffer = db->p;
    /*
    ** send the message on the dirent thread queue
    */
    rozofs_queue_put(&dirent_queue_lookup,xmit_buf);

    return 0;
}


/*
 *_______________________________________________________________________
 */
/*
 **______________________________________________________________________________
 */

/**
 * API for get a mdirentry in one parent directory (version2)
 *
 * @param mdir: pointer to the mdirent structure for directory specific attributes
 * @param name: (key) pointer to the name to search
 * @param *fid: pointer to the unique identifier for this mdirentry
 * @param *type: pointer to the type for this mdirentry
 *
 * @retval  0 on success
 * @retval -1 on failure
 */
int list_mdirentries2(void *root_idx_bitmap_p,int dir_fd, fid_t fid_parent_in, char *buf_readdir_in, uint64_t *cookie, uint8_t * eof,ext_mattr_t *parent);

int rozofs_bt_list_mdirentries2(fid_t fid_parent_in, char *buf_readdir_in, uint64_t *cookie, uint8_t * eof)
{

   int ret;
   dentry_btmap_t *bt_map_p;
   int dirfd = -1;
   rozofs_inode_t *rozofs_inode_p;
   uint64_t inode;
   ext_mattr_t * ext_attr_p;
    
   rozofs_inode_p = (rozofs_inode_t*) fid_parent_in;
   inode = rozofs_inode_p->fid[1];
      
   /*
   ** try to get the root bitmap of the directory
   */
   bt_map_p = get_btmap_entry_by_fid_external(fid_parent_in);
   if (bt_map_p == NULL)
   {
     errno = EAGAIN;
     return -1;
   }
      /*
   ** normalize the inode value
   */
   inode = rozofs_bt_inode_normalize(inode);
   /*
   ** check if the inode can be found in the tracking file cache
   */
   ext_attr_p = rozofs_bt_lookup_inode_internal_with_tracking_entry(inode,NULL);
   if (ext_attr_p == NULL)
   {
     return -1;
   }   
   ret = list_mdirentries2(&bt_map_p->btmap,dirfd,fid_parent_in,buf_readdir_in,cookie,eof,ext_attr_p);
   return ret;   
}
 

/*
 *_______________________________________________________________________
 */
 
 void rozofs_bt_readdir_req(rozofs_bt_thread_ctx_t * thread_ctx_p,void *ruc_buffer)
 {


    bt_dirent_msg_t  *readdir_rq_p;
    int ret;
//    char *buf_readdir;
    bt_readdir2_ret  *readdir_rsp_p;
    uint8_t eof = 0;
    uint64_t cookie;
     
    readdir_rq_p  = (bt_dirent_msg_t*) ruc_buf_getPayload(ruc_buffer);
    readdir_rsp_p  = &readdir_rq_p->s.readdir_rsp;
//    buf_readdir   = (char *)(readdir_rsp_p+1);
    cookie = readdir_rq_p->s.readdir_rq.cookie;
    if (readdir_rq_p->s.readdir_rq.db_buffer == NULL)
    {
      errno =EPROTO;
      readdir_rsp_p->status = -1;
      readdir_rsp_p->s.errcode = errno;
      goto out;
    }
    ret = rozofs_bt_list_mdirentries2(readdir_rq_p->s.readdir_rq.fid,readdir_rq_p->s.readdir_rq.db_buffer,&cookie,&eof);
    if (ret < 0)
    {
      readdir_rsp_p->status = -1;
      readdir_rsp_p->s.errcode = errno;
    }
    else
    {
      readdir_rsp_p->status = 0;      
      readdir_rsp_p->s.reply.eof= eof;      
      readdir_rsp_p->s.reply.cookie= cookie;      
      readdir_rsp_p->s.reply.value_len= ret;      
    }
    /*
    ** send the response to the main thread
    */
out:
    rozofs_bt_dirent_th_send_response(thread_ctx_p,ruc_buffer); 
 }




/*
 *_______________________________________________________________________
 */
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param ruc_buffer : pointer to ruc_buffer that contains the response
 
 @return none
 */
void rozofs_bt_readdir2_cbk(void *ruc_buffer)
{
   fuse_req_t req; 
   
   int status;
   int ret;
   fuse_ino_t   ino;
   size_t       size;
   off_t        off;
   ientry_t    *ie = 0;
    dirbuf_t   *db=NULL;
    int trc_idx;
    struct fuse_file_info *fi ;
    dir_t *dir_p = NULL;
    
    bt_dirent_msg_t *msg_p;
    void *param;
    int copy_len;
//    char *buf_readdir;
//    bt_readdir2_ret  *readdir_rsp_p;    
    
    errno = 0;
    /*
    ** get the pointer to the readdir response
    */
    msg_p = (bt_dirent_msg_t*) ruc_buf_getPayload(ruc_buffer);
//    readdir_rsp_p  = &msg_p->s.readdir_rsp;
//    buf_readdir   = (char *)(readdir_rsp_p+1);

    /*
    ** restore the fuse context
    */
    param = msg_p->hdr.user_ctx;
    /*
    ** restore the parameter of the calling application
    */           
    RESTORE_FUSE_PARAM(param,req);
    RESTORE_FUSE_PARAM(param,ino);
    RESTORE_FUSE_PARAM(param,size);
    RESTORE_FUSE_PARAM(param,off);    
    RESTORE_FUSE_PARAM(param,trc_idx);    
    RESTORE_FUSE_PARAM(param,fi);

    dir_p = (dir_t*)fi->fh;
    dir_p->readdir_pending = 0;
    db = &dir_p->db;
    /*
    ** allocate the readdir buffer if not yet done
    */
    if (db->p == NULL)
    {
//      db->p = xmalloc(ROZOFS_READDIR2_BUFSIZE);
       severe("dentry buffer is not allocated");
       errno = EPROTO;
       goto error;
    }
    db->size = 0;    
    /*
    **  Get ientry of the directory
    */
    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }
    /*
    ** check the readdir status
    */
    if (msg_p->s.readdir_rsp.status < 0) {
        errno = msg_p->s.readdir_rsp.s.errcode;
	/*
	** need to take care of the EAGAIN case, since it indicates that the dirent file where not available
	*/
        goto error;
    }    
    copy_len = msg_p->s.readdir_rsp.s.reply.value_len;    
    if (msg_p->s.readdir_rsp.s.reply.value_len > ROZOFS_READDIR2_BUFSIZE) {
      severe("rozofs_ll_readdir2_cbk receive %d while expecting %d max",
              msg_p->s.readdir_rsp.s.reply.value_len,
              ROZOFS_READDIR2_BUFSIZE);
      copy_len = ROZOFS_READDIR2_BUFSIZE;
    }
#if 0
    /*
    ** copy the data in the readdir buffer
    */
    memcpy(db->p,buf_readdir,copy_len);
#endif                
    db->eof    = msg_p->s.readdir_rsp.s.reply.eof;
    db->cookie = msg_p->s.readdir_rsp.s.reply.cookie;
    db->size = copy_len;
    db->last_cookie_buf = off;
    db->cookie_offset_buf = 0;
    status = rozofs_parse_dirfile( req,db,(uint64_t) off,(int) size);
    if (status < 0) goto error;

    goto out;
    
error:
    if (errno == EAGAIN)
    {
    
       ret =rozofs_ll_readdir2_send_to_export_no_batch(ie->fid,off,param);
       if (ret >= 0)
       {
          ruc_buf_freeBuffer(ruc_buffer); 
	  return ;
       }  	    
    }
    fuse_reply_err(req, errno);
out:
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_readdir,ino,NULL,status,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_readdir);
    rozofs_fuse_release_saved_context(param);        
    ruc_buf_freeBuffer(ruc_buffer);   
   
    return;
}


/*
**__________________________________________________________________________
*/
/**
  Processes either a ROZOFS_BT_DIRENT_READDIR or ROZOFS_BT_DIRENT_GET_DENTRY reponse

   Called from the socket controller when there is a response from a dirent thread
   the response is found in the ruc_buffer received on the AF_UNIX socket
   
   It is up to the receiver to release the ruc_buffer
    
  @param ruc_buffer: pointer to the ruc_buffer
 
  @retval :none
*/


void af_unix_bt_dirent_response(void *ruc_buffer) 
{

  expbt_msgint_hdr_t *hdr_p;
  
  hdr_p  = (expbt_msgint_hdr_t*) ruc_buf_getPayload(ruc_buffer);
  switch (hdr_p->opcode) {
  
    case ROZOFS_BT_DIRENT_READDIR:
    {
      FDL_INFO("FDL ROZOFS_BT_DIRENT_READDIR received!!");
      return rozofs_bt_readdir2_cbk(ruc_buffer);
      break;
    }  
    case ROZOFS_BT_DIRENT_GET_DENTRY:
    {
      FDL_INFO("FDL ROZOFS_BT_DIRENT_GET_DENTRY received!!");
      return rozofs_bt_lookup_cbk(ruc_buffer);
      break;
    }
    default:
      severe("Unexpected opcode %d", hdr_p->opcode);
      ruc_buf_freeBuffer(ruc_buffer);
      break;
  }

}
/*
 *_______________________________________________________________________
 */
static void *rozofs_bt_dirent_lookup_thread(void *v) {

   rozofs_bt_thread_ctx_t * thread_ctx_p = (rozofs_bt_thread_ctx_t*)v; 
   void *ruc_buffer;
   expbt_msgint_hdr_t *hdr_p;
   
   uma_dbg_thread_add_self("bt_lkup_dirent");
   dirent_lookup_thread_ready = 1;
   
   while(1)
   {

     ruc_buffer = rozofs_queue_get(&dirent_queue_lookup);
     hdr_p  = (expbt_msgint_hdr_t*) ruc_buf_getPayload(ruc_buffer);
     switch (hdr_p->opcode)
     {
       case ROZOFS_BT_DIRENT_READDIR:	    
	 FDL_INFO("ROZOFS_BT_DIRENT_READDIR\n");
	 rozofs_bt_readdir_req(thread_ctx_p,ruc_buffer); 
	 break; 
       case ROZOFS_BT_DIRENT_GET_DENTRY:
	 rozofs_bt_lookup_req(thread_ctx_p,ruc_buffer); 
	 break;  
       default:
	fatal("unexpected  opcode:%d\n",hdr_p->opcode);
      }
   
   }

}

/*
**__________________________________________________________________________
*/

int rozofs_bt_dirent_thread_create(int nb_threads)
{
   int                        i;
   int                        err;
   pthread_attr_t             attr;
   rozofs_bt_thread_ctx_t * thread_ctx_p;
   /*
   ** only 1 thread
   */
   nb_threads = 1;

   rozofs_queue_init(&dirent_queue_lookup,ROZOFS_BT_DIRENT_QDEPTH);
   /*
   ** clear the thread table
   */
   memset(rozofs_bt_dirent_lkup_thread_ctx_tb,0,sizeof(rozofs_bt_thread_ctx_t)*ROZOFS_BT_DIRENT_LKPUP_MAX_THREADS);
   /*
   ** Now create the threads
   */
   thread_ctx_p = rozofs_bt_dirent_lkup_thread_ctx_tb;
   for (i = 0; i < nb_threads ; i++) {
     /*
     ** create the socket that the thread will use for sending back response 
     */
     thread_ctx_p->sendSocket = socket(AF_UNIX,SOCK_DGRAM,0);
     if (thread_ctx_p->sendSocket < 0) {
	fatal("rozofs_bt_thread_create fail to create socket: %s", strerror(errno));
	return -1;   
     } 
     err = pthread_attr_init(&attr);
     if (err != 0) {
       fatal("rozofs_bt_thread_create pthread_attr_init(%d) %s",i,strerror(errno));
       return -1;
     }  

     thread_ctx_p->thread_idx = i;
     err = pthread_create(&thread_ctx_p->thrdId,&attr,rozofs_bt_dirent_lookup_thread,thread_ctx_p);
     if (err != 0) {
       fatal("rozofs_bt_thread_create pthread_create(%d) %s",i, strerror(errno));
       return -1;
     }  
     
     thread_ctx_p++;
  }
  return 0;
}

/*__________________________________________________________________________
** Initialize the dirent thread interface (used for READDIR & LOOKUP)
 
  @param hostname    hostname (for tests)
  @param instance_id : expbt instance
  @param nb_threads  Number of threads that can process the read or write requests
*
*  @retval 0 on success -1 in case of error
*/
int rozofs_bt_dirent_cache_initialize();

int rozofs_bt_dirent_thread_intf_create(char * hostname, int instance_id, int nb_threads) {

  af_unix_bt_dirent_thread_count = nb_threads;
  int ret;
  
  /*
  ** create the dirent cache hash table
  */
  ret = rozofs_bt_dirent_cache_initialize();
  if (ret < 0)
  {
     severe("Cannot create the dirent hash table");
  }
  /*
  ** init of the AF_UNIX sockaddr associated with the south socket (socket used for disk response receive)
  */
  rozofs_bt_dirent_set_sockname_with_hostname(&rozofs_bt_dirent_south_socket_name,ROZOFS_SOCK_FAMILY_FUSE_SOUTH,hostname, instance_id);
    
  /*
  ** hostname is required for the case when several storaged run on the same server
  ** as is the case of test on one server only
  */   
  af_unix_bt_dirent_south_socket_ref = af_unix_bt_dirent_response_socket_create(rozofs_bt_dirent_south_socket_name.sun_path);
  if (af_unix_bt_dirent_south_socket_ref < 0) {
    fatal("rozofs_bt_dirent_thread_intf_create af_unix_sock_create(%s) %s",rozofs_bt_dirent_south_socket_name.sun_path, strerror(errno));
    return -1;
  }
  
   

  ret =  rozofs_bt_dirent_thread_create(nb_threads);
  if (ret < 0) return ret;

  return ret;

  
}



