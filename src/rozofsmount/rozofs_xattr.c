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

#include <rozofs/rpc/eproto.h>
#include <rozofs/core/rozofs_host_list.h>
#include "rozofs_fuse_api.h"
#include "rozofs_xattr_flt.h"
#include "rozofs_ext4.h"
#include "xattr.h"
#include "xattr_main.h"
#include <rozofs/core/rozofs_fid_string.h>

DECLARE_PROFILING(mpp_profiler_t);

void rozofs_ll_getxattr_raw_cbk(void *this,void *param);


#define ROZOFS_XATTR "rozofs"
#define ROZOFS_USER_XATTR "user.rozofs"
#define ROZOFS_ROOT_XATTR "trusted.rozofs"
#define ROZOFS_USER_INOTIFY "user.rozofs.inotify"
#define ROZOFS_EXPORT_XATTR "rozofs.export"
#define ROZOFS_USER_EXPORT_XATTR "user.rozofs.export"

typedef enum 
{
   ROZOFS_XATTR_NONE_E = 0,
   ROZOFS_XATTR_STD_E,
   ROZOFS_XATTR_NOTIFY_E,
   ROZOFS_XATTR_EXPORT_E
} rozofs_xattr_e;
/*
**__________________________________________________________________
*/
/**
*   Check if the input ascii buffer contains a RozoFS specific extended attribute

   @param input: extended attribut name
   
   @retval: 0 not a rozofs extended attribute
   @retval <>0: index of the rozofs extended attribute
*/
static inline int rozofs_is_rozofs_xattr(const char *input)
{
      char *name = (char*)input;	
      if (strncmp(name,ROZOFS_USER_XATTR,11)==0)
      {
	  name +=11;
      }
      else if (strncmp(name,ROZOFS_ROOT_XATTR,14)==0)  
      {
	  name += 14;	
      }
      else
      {  
        return ROZOFS_XATTR_NONE_E;
      }
      if (name[0] == 0) return ROZOFS_XATTR_STD_E;
      if (strcmp(name,".inotify") == 0) return ROZOFS_XATTR_NOTIFY_E;
      if (strcmp(name,".export") == 0) return ROZOFS_XATTR_EXPORT_E;
      return ROZOFS_XATTR_NONE_E;
}	

/*
**__________________________________________________________________
*/
/**
 * Set the value of an extended attribute to a file
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode of the file to set attribute too
 * @param name the name of the attribute
 * @param value the value of the attribute
 * @param size the size of the value of the attribute
 * @param flags XATTR_CREATE or XATTR_REPLACE
 */
 void rozofs_ll_setxattr_cbk(void *this,void *param);

void rozofs_ll_setxattr_nb(fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags) 
{
    ientry_t         *ie = 0;
    int               ret;        
    void             *buffer_p = NULL;
    epgw_setxattr_arg_t arg;


    DEBUG("setxattr (inode: %lu, name: %s, value: %s, size: %llu)\n",
            (unsigned long int) ino, name, value,
            (unsigned long long int) size);

   /*
   ** Check if it is a fake setxattr to trigger inotify for the case of SMB
   */
   if ((strcmp(name,ROZOFS_USER_INOTIFY)==0)) {
     fuse_reply_err(req, 0);
     return;
    }
	    
    /*
    ** allocate a context for saving the fuse parameters
    */
    int trc_idx = rozofs_trc_req_name(srv_rozofs_ll_setxattr,ino,(char*)name);
    buffer_p = rozofs_fuse_alloc_saved_context();
    if (buffer_p == NULL)
    {
      severe("out of fuse saved context");
      errno = ENOMEM;
      goto error;
    }
    START_PROFILING_NB(buffer_p,rozofs_ll_setxattr);

    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }

    
    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,ino);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);

    // Invalidate ientry
    if ((strcmp(name,ROZOFS_XATTR)==0)||(strcmp(name,ROZOFS_USER_XATTR)==0)||(strcmp(name,ROZOFS_ROOT_XATTR)==0)) {
        ie->timestamp=0;
    }
    /*
    ** update the mode at the ientry level when it extended attribute is ACL
    */
    rozofs_acl_access_check(name,value,size,(mode_t*)&ie->attrs.mode);
    /*
    ** Set xattr indicator in ientry
    */
    rozofs_set_xattr_flag(&ie->attrs.mode);
    /*
    ** clear the timestamp
    */
    ie->xattr_timestamp = 0;
    
    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid = exportclt.eid;
    memcpy(arg.arg_gw.fid,  ie->fid, sizeof (uuid_t));
    arg.arg_gw.name = (char *)name;
    arg.arg_gw.value.value_len = size;
    arg.arg_gw.value.value_val = (char *)value;
    arg.arg_gw.flags = flags;    
    
    /*
    ** now initiates the transaction towards the remote end
    */
#if 1
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)arg.arg_gw.fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_SETXATTR,(xdrproc_t) xdr_epgw_setxattr_arg_t,(void *)&arg,
                              rozofs_ll_setxattr_cbk,buffer_p); 
#else
    ret = rozofs_export_send_common(&exportclt,ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM),EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_SETXATTR,(xdrproc_t) xdr_epgw_setxattr_arg_t,(void *)&arg,
                              rozofs_ll_setxattr_cbk,buffer_p); 
#endif
    if (ret < 0) goto error;
    
    /*
    ** no error just waiting for the answer
    */
    return;

error:
    fuse_reply_err(req, errno);
    /*
    ** release the buffer if has been allocated
    */
    rozofs_trc_rsp(srv_rozofs_ll_setxattr,ino,NULL,1,trc_idx);
    STOP_PROFILING_NB(buffer_p,rozofs_ll_setxattr);
    if (buffer_p != NULL) rozofs_fuse_release_saved_context(buffer_p);
    return;
}


/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_setxattr_cbk(void *this,void *param)
{
   fuse_req_t req; 
   epgw_setxattr_ret_t ret ;
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   struct rpc_msg  rpc_reply;
   xdrproc_t decode_proc = (xdrproc_t) xdr_epgw_setxattr_ret_t;
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   int trc_idx;
   fuse_ino_t ino;

   errno = 0;

   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,ino);
   RESTORE_FUSE_PARAM(param,trc_idx);
    /*
    ** get the pointer to the transaction context:
    ** it is required to get the information related to the receive buffer
    */
    rozofs_tx_ctx_t      *rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;     
    /*    
    ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
    */
    status = rozofs_tx_get_status(this);
    if (status < 0)
    {
       /*
       ** something wrong happened
       */
       errno = rozofs_tx_get_errno(this);  
       goto error; 
    }
    /*
    ** get the pointer to the receive buffer payload
    */
    recv_buf = rozofs_tx_get_recvBuf(this);
    if (recv_buf == NULL)
    {
       /*
       ** something wrong happened
       */
       errno = EFAULT;  
       goto error;         
    }


    payload  = (uint8_t*) ruc_buf_getPayload(recv_buf);
    payload += sizeof(uint32_t); /* skip length*/
    /*
    ** OK now decode the received message
    */
    bufsize = (int) ruc_buf_getPayloadLen(recv_buf);
    bufsize -= sizeof(uint32_t); /* skip length*/
    xdrmem_create(&xdrs,(char*)payload,bufsize,XDR_DECODE);
    /*
    ** decode the rpc part
    */
    if (rozofs_xdr_replymsg(&xdrs,&rpc_reply) != TRUE)
    {
     TX_STATS(ROZOFS_TX_DECODING_ERROR);
     errno = EPROTO;
     goto error;
    }
    /*
    ** ok now call the procedure to encode the message
    */
    memset(&ret,0, sizeof(ret));    
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       xdr_free(decode_proc, (char *) &ret);
       goto error;
    }   
    if (ret.status_gw.status == EP_FAILURE) {
        errno = ret.status_gw.ep_status_ret_t_u.error;
        xdr_free(decode_proc, (char *) &ret);    
        goto error;
    }
    
    /*
    ** Case of a setxattr that has modified the target of a symbolic link.
    ** The new target is in the response, so update the ientry
    */
    if ((ret.symlink.status == EP_SUCCESS) 
    &&  (ret.symlink.epgw_setxattr_symlink_t_u.info.target.target_val != NULL)) {
      ientry_t * ie = get_ientry_by_fid((unsigned char*)ret.symlink.epgw_setxattr_symlink_t_u.info.symlink_fid);
      if (ie != NULL) {
        /*
	** Invert pointers from response and ientry
	*/
        char * pt = ie->symlink_target;
	ie->symlink_target = ret.symlink.epgw_setxattr_symlink_t_u.info.target.target_val;
	ret.symlink.epgw_setxattr_symlink_t_u.info.target.target_val = pt;
	if (pt == NULL) ret.symlink.status = EP_EMPTY;
	ie->symlink_ts = rozofs_get_ticker_us();
      }       
    } 
    
    xdr_free(decode_proc, (char *) &ret);   
     
    fuse_reply_err(req, 0);
    goto out;
error:
    fuse_reply_err(req, errno);
out:
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_setxattr,ino,NULL,status,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_setxattr);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
   
    return;
}

/*
**__________________________________________________________________
*/
/**
   Check if the client uses raw api for gettingh extended atttributes
   
   @param name: name of the extended attribute
   @param ie: ientry of the inode
   
   @retval 0 : not raw
   @retval 1: raw
*/


int rozofs_ll_getxattr_check_for_raw_getxattr(const char *name,ientry_t *ie)
{

    if (ie->ientry_long == 0) return 0;

    if ((strncmp(name,ROZOFS_XATTR,6)==0)||(strncmp(name,ROZOFS_USER_XATTR,11)==0)||(strncmp(name,ROZOFS_ROOT_XATTR,14)==0))      
    {
       return 0;
    }  
    return 1;
}
/*
**__________________________________________________________________
*/
/**
 * 
 * Get the value of an extended attribute of a file
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode of the file to get attribute from
 * @param name the name of the attribute
 * @param size the size of the attribute
 */
 void rozofs_ll_getxattr_cbk(void *this,void *param);

static char buf_export_attr[4096];
void rozofs_ll_getxattr_nb(fuse_req_t req, fuse_ino_t ino, const char *name, size_t size) 
{
    ientry_t         *ie = 0;
    int               ret;        
    void             *buffer_p = NULL;
    epgw_getxattr_arg_t arg;
    lv2_entry_t *fake_lv2_p = NULL;
    int value_size;
    char *buffer;
    int  rozofs_ea_value;

    uint64_t attr_us = rozofs_tmr_get_attr_us(rozofs_is_directory_inode(ino));
    /*
    ** allocate a context for saving the fuse parameters
    */
    int trc_idx = rozofs_trc_req_name(srv_rozofs_ll_getxattr,ino,(char*)name);
    buffer_p = rozofs_fuse_alloc_saved_context();
    if (buffer_p == NULL)
    {
      severe("out of fuse saved context");
      errno = ENOMEM;
      goto error;
    }
    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,size);
    SAVE_FUSE_PARAM(buffer_p,ino);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);
    SAVE_FUSE_STRING(buffer_p,name);

    
    START_PROFILING_NB(buffer_p,rozofs_ll_getxattr);

    DEBUG("getxattr (inode: %lu, name: %s, size: %llu) \n",
            (unsigned long int) ino, name, (unsigned long long int) size);
    /*
    ** check if xattr are enable
    ** when it is disabled anwser with ENOSYS will imply that xattr will
    ** disabled at FS level by fuse: note is does not affect set_xattr and list_xattr
    */
    if (rozofs_xattr_disable) 
    {
      errno = ENOSYS;
      goto error;
    }

    /// XXX: respond with the error ENODATA for these calls
    // to avoid that the getxattr called on export at each write to this file
    // But these calls have overhead (each one requires a context switch)
    // It's seems to be a bug in kernel.
    if (strcmp("security.capability", name) == 0) {
        errno = ENODATA;
        goto error;
    }
    	    
    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    } 
    /*
    ** Get the rozofstype of the extended attribute
    */
    rozofs_ea_value = rozofs_is_rozofs_xattr(name);
    
    /*
    ** Check whether ientry is still valid 
    */
    if ((rozofs_mode == 1) || 
         (((ie->timestamp+rozofs_tmr_get_attr_us(rozofs_is_directory_inode(ino))) > rozofs_get_ticker_us())&&(S_ISREG(ie->attrs.mode))) ||
	 (((ie->timestamp+500) > rozofs_get_ticker_us())&&(S_ISDIR(ie->attrs.mode)))
	 )
    {
      /*
      ** Check if the i-node has extended attributs that are not the rozofs extended attributes
      */
      if (rozofs_has_xattr(ie->attrs.mode)==0)
      {
	if (rozofs_ea_value == 0)  
	{
          errno = ENODATA;
          goto error;  
	}
      }
    }
    if (rozofs_ea_value > ROZOFS_XATTR_STD_E)
    {
    
      if (rozofs_ea_value == ROZOFS_XATTR_EXPORT_E)
      {
	 char *pChar = buf_export_attr;
	 int export_index;
	 char * pHost = NULL;
	 /*
	 ** Get the IP address of the active exportd
	 */    
	 for (export_index=0; export_index < ROZOFS_HOST_LIST_MAX_HOST; export_index++) { 

	     pHost = rozofs_host_list_get_host(export_index);
	     if (pHost == NULL) break;
	     if (export_index==0) pChar += sprintf(pChar, "%s", pHost);
	     else                 pChar += sprintf(pChar, "/%s", pHost);
	 }
	 pChar += sprintf(pChar, " %u ",exportclt.eid);
	 pChar += sprintf(pChar, "%s ",conf.export);
	 pChar += sprintf(pChar, "%d\n",conf.instance);
	 fuse_reply_buf(req, (char *)buf_export_attr, strlen(buf_export_attr));   
	 goto out; 

      }
      if (rozofs_ea_value == ROZOFS_XATTR_NOTIFY_E)
      {
	 char *pChar = buf_export_attr;
	 /*
	 ** provide the fid of the object : @rozofs_uuid@<FID>
	 */
          pChar += sprintf(pChar, "%s/@rozofs_uuid@",rozofs_mountpoint);
	  rozofs_fid2string(ie->fid,pChar);
	  fuse_reply_buf(req, (char *)buf_export_attr, strlen(buf_export_attr)+1);   
	  goto out;     
      }  
    }
    
    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid = exportclt.eid;
    memcpy(arg.arg_gw.fid,  ie->fid, sizeof (uuid_t));
    arg.arg_gw.name = (char *)name;
    arg.arg_gw.size = size;  
    
    if (rozofs_ll_getxattr_check_for_raw_getxattr(name,ie) == 0)
    {
    
      /*
      ** now initiates the transaction towards the remote end
      */
      ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)arg.arg_gw.fid,EXPORT_PROGRAM, EXPORT_VERSION,
                        	EP_GETXATTR,(xdrproc_t) xdr_epgw_getxattr_arg_t,(void *)&arg,
                        	rozofs_ll_getxattr_cbk,buffer_p); 
    }
    else
    {
      if ((ie->xattr_timestamp+attr_us) > rozofs_get_ticker_us()) goto local_getxattr;
      /*
      ** now initiates the transaction towards the remote end
      */
      ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)arg.arg_gw.fid,EXPORT_PROGRAM, EXPORT_VERSION,
                        	EP_GETXATTR_RAW,(xdrproc_t) xdr_epgw_getxattr_arg_t,(void *)&arg,
                        	rozofs_ll_getxattr_raw_cbk,buffer_p); 
    }

    if (ret < 0) goto error;
    
    /*
    ** no error just waiting for the answer
    */
    return;

error:
    fuse_reply_err(req, errno);
    /*
    ** release the buffer if has been allocated
    */
out:
    rozofs_trc_rsp(srv_rozofs_ll_getxattr,ino,NULL,1,trc_idx);
    STOP_PROFILING_NB(buffer_p,rozofs_ll_getxattr);
    if (buffer_p != NULL) rozofs_fuse_release_saved_context(buffer_p);
    return;

local_getxattr:
    /*
    ** Now search for the requested extended attribute
    */
    {
      struct dentry entry;
      fake_lv2_p = (lv2_entry_t*)&ie->attrs;
      entry.d_inode = fake_lv2_p;
      /*
      ** check if the request is just for the xattr len: if it the case set the buffer to NULL
      */
      if (size == 0) buffer = 0;
      else buffer = buf_export_attr;
    
      if ((value_size = rozofs_getxattr(&entry, name, buffer, size)) < 0) {
          goto error;
      }
    }    
    if (size == 0) {
        fuse_reply_xattr(req, value_size);
	errno = EAGAIN;
        goto out;
    }       
    
    if (value_size > size) {
        errno = ERANGE;
        goto error;
    }

    fuse_reply_buf(req, (char *)buf_export_attr, value_size);
    errno = EAGAIN;
    goto out;
}


/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_getxattr_cbk(void *this,void *param)
{
   fuse_req_t req; 
   epgw_getxattr_ret_t ret ;
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   struct rpc_msg  rpc_reply;
   uint64_t value_size = 0;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_getxattr_ret_t;
   size_t size;
   int    trc_idx;
   fuse_ino_t ino;

   errno = 0;          
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,size);   
   RESTORE_FUSE_PARAM(param,ino);   
   RESTORE_FUSE_PARAM(param,trc_idx);   
    /*
    ** get the pointer to the transaction context:
    ** it is required to get the information related to the receive buffer
    */
    rozofs_tx_ctx_t      *rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;     
    /*    
    ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
    */
    status = rozofs_tx_get_status(this);
    if (status < 0)
    {
       /*
       ** something wrong happened
       */
       errno = rozofs_tx_get_errno(this);  
       goto error; 
    }
    /*
    ** get the pointer to the receive buffer payload
    */
    recv_buf = rozofs_tx_get_recvBuf(this);
    if (recv_buf == NULL)
    {
       /*
       ** something wrong happened
       */
       errno = EFAULT;  
       goto error;         
    }


    payload  = (uint8_t*) ruc_buf_getPayload(recv_buf);
    payload += sizeof(uint32_t); /* skip length*/
    /*
    ** OK now decode the received message
    */
    bufsize = (int) ruc_buf_getPayloadLen(recv_buf);
    bufsize -= sizeof(uint32_t); /* skip length*/
    xdrmem_create(&xdrs,(char*)payload,bufsize,XDR_DECODE);
    /*
    ** decode the rpc part
    */
    if (rozofs_xdr_replymsg(&xdrs,&rpc_reply) != TRUE)
    {
     TX_STATS(ROZOFS_TX_DECODING_ERROR);
     errno = EPROTO;
     goto error;
    }
    /*
    ** ok now call the procedure to encode the message
    */
    memset(&ret,0, sizeof(ret));
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       xdr_free(decode_proc, (char *) &ret);
       goto error;
    }   
    if (ret.status_gw.status == EP_FAILURE) {
        errno = ret.status_gw.ep_getxattr_ret_t_u.error;
        xdr_free(decode_proc, (char *) &ret);    
        goto error;
    }
    value_size = ret.status_gw.ep_getxattr_ret_t_u.value.value_len;
    
    if (size == 0) {
        fuse_reply_xattr(req, value_size);
        goto out;
    }       
    
    if (value_size > size) {
        errno = ERANGE;
        xdr_free(decode_proc, (char *) &ret);    
        goto error;
    }

    fuse_reply_buf(req, (char *)ret.status_gw.ep_getxattr_ret_t_u.value.value_val, value_size);
    xdr_free(decode_proc, (char *) &ret);   
    goto out;
    
error:
    fuse_reply_err(req, errno);
out:
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_getxattr,ino,NULL,status,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_getxattr);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
   
    return;
}


/**
*  Getxattr in raw mode callback
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_getxattr_raw_cbk(void *this,void *param)
{
   fuse_req_t req; 
   epgw_getxattr_raw_ret_t ret ;
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   struct rpc_msg  rpc_reply;
   int value_size = 0;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_getxattr_raw_ret_t;
   size_t size;
   int    trc_idx;
   fuse_ino_t ino;
   char *buffer;
   char *name;
   ientry_t *ie = 0;
   lv2_entry_t *fake_lv2_p;

   errno = 0;          
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,size);   
   RESTORE_FUSE_PARAM(param,ino);   
   RESTORE_FUSE_PARAM(param,trc_idx);   
   RESTORE_FUSE_STRUCT_PTR(param,name);
    /*
    ** get the pointer to the transaction context:
    ** it is required to get the information related to the receive buffer
    */
    rozofs_tx_ctx_t      *rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;     
    /*    
    ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
    */
    status = rozofs_tx_get_status(this);
    if (status < 0)
    {
       /*
       ** something wrong happened
       */
       errno = rozofs_tx_get_errno(this);  
       goto error; 
    }
    /*
    ** get the pointer to the receive buffer payload
    */
    recv_buf = rozofs_tx_get_recvBuf(this);
    if (recv_buf == NULL)
    {
       /*
       ** something wrong happened
       */
       errno = EFAULT;  
       goto error;         
    }


    payload  = (uint8_t*) ruc_buf_getPayload(recv_buf);
    payload += sizeof(uint32_t); /* skip length*/
    /*
    ** OK now decode the received message
    */
    bufsize = (int) ruc_buf_getPayloadLen(recv_buf);
    bufsize -= sizeof(uint32_t); /* skip length*/
    xdrmem_create(&xdrs,(char*)payload,bufsize,XDR_DECODE);
    /*
    ** decode the rpc part
    */
    if (rozofs_xdr_replymsg(&xdrs,&rpc_reply) != TRUE)
    {
     TX_STATS(ROZOFS_TX_DECODING_ERROR);
     errno = EPROTO;
     goto error;
    }
    /*
    ** ok now call the procedure to encode the message
    */
    memset(&ret,0, sizeof(ret));
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       xdr_free(decode_proc, (char *) &ret);
       goto error;
    }   
    if (ret.status_gw.status == EP_FAILURE) {
        errno = ret.status_gw.ep_getxattr_raw_ret_t_u.error;
        xdr_free(decode_proc, (char *) &ret);    
        goto error;
    }
    /*
    ** Get the arrays that contain the extended attributes associated with the inode
    */
    {
      char *dst;
        

      if (!(ie = get_ientry_by_inode(ino))) {
          errno = ENOENT;
          goto error;
      }
      if (ie->ientry_long == 0)
      {
          errno = EINVAL;
          goto error;
      }
      /*
      ** update the timestamp of the xattr
      */
      ie->xattr_timestamp = rozofs_get_ticker_us();
      fake_lv2_p = (lv2_entry_t*)&ie->attrs;
      dst = (char*)fake_lv2_p;
      dst += ROZOFS_I_EXTRA_ISIZE;
      /*
      ** check if there some data in the inode
      */
      if (ret.status_gw.ep_getxattr_raw_ret_t_u.raw.inode_xattr.inode_xattr_len == 0)
      {
         /*
	 ** clear the array
	 */
         memset(dst,0,ROZOFS_INODE_SZ - ROZOFS_I_EXTRA_ISIZE);
	 if (fake_lv2_p->extended_attr_p != NULL) xfree(fake_lv2_p->extended_attr_p);    
	 fake_lv2_p->extended_attr_p = NULL;
	 fake_lv2_p->attributes.s.i_file_acl = 0; 
	 ext4_clear_inode_state(fake_lv2_p, EXT4_STATE_XATTR);
      } 
      else
      {
        /*
	** copy it on the ientry on the inode section
	*/
	memcpy(dst,ret.status_gw.ep_getxattr_raw_ret_t_u.raw.inode_xattr.inode_xattr_val,ROZOFS_INODE_SZ - ROZOFS_I_EXTRA_ISIZE);
	ext4_set_inode_state(fake_lv2_p, EXT4_STATE_XATTR);
      
      }             
      if (ret.status_gw.ep_getxattr_raw_ret_t_u.raw.inode_xattr_block.inode_xattr_block_len == 0)
      {
         /*
	 ** clear the array: release the extended block if one was allocated
	 */
	 if (fake_lv2_p->extended_attr_p != NULL) xfree(fake_lv2_p->extended_attr_p);    
	 fake_lv2_p->extended_attr_p = NULL;
	 fake_lv2_p->attributes.s.i_file_acl = 0;      
      } 
      else
      {
        /*
	** allocated a 4KB block for storing the extended attributes & copy the data
	*/
	if (fake_lv2_p->extended_attr_p == NULL) fake_lv2_p->extended_attr_p = xmalloc(ROZOFS_XATTR_BLOCK_SZ);
	memcpy(fake_lv2_p->extended_attr_p,ret.status_gw.ep_getxattr_raw_ret_t_u.raw.inode_xattr_block.inode_xattr_block_val,ROZOFS_XATTR_BLOCK_SZ);
	/*
	** indicates that there is an extension block for the xattributes
	*/
	fake_lv2_p->attributes.s.i_file_acl = 1;	      
      }   

    }
    /*
    ** Now search for the requested extended attribute
    */
    {
      struct dentry entry;
      entry.d_inode = fake_lv2_p;
      /*
      ** check if the request is just for the xattr len: if it the case set the buffer to NULL
      */
      if (size == 0) buffer = 0;
      else buffer = buf_export_attr;
    
      if ((value_size = rozofs_getxattr(&entry, name, buffer, size)) < 0) {
          goto error;
      }
    }    
    if (size == 0) {
        fuse_reply_xattr(req, value_size);
	errno = 0;
        goto out;
    }       
    
    if (value_size > size) {
        errno = ERANGE;
        xdr_free(decode_proc, (char *) &ret);    
        goto error;
    }

    fuse_reply_buf(req, (char *)buf_export_attr, value_size);
    xdr_free(decode_proc, (char *) &ret);   
    errno = 0;
    goto out;
    
error:
    fuse_reply_err(req, errno);
out:
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_getxattr,ino,NULL,status,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_getxattr);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
   
    return;
}

/*
**__________________________________________________________________
*/
/**
 * 
 * Remove the value of an extended attribute of a file
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode of the file to remove attribute from
 * @param name the name of the attribute 
 */
 void rozofs_ll_removexattr_cbk(void *this,void *param);

void rozofs_ll_removexattr_nb(fuse_req_t req, fuse_ino_t ino, const char *name) 
{
    ientry_t         *ie = 0;
    int               ret;        
    void             *buffer_p = NULL;
    epgw_removexattr_arg_t arg;
    /*
    ** allocate a context for saving the fuse parameters
    */
    int trc_idx = rozofs_trc_req_name(srv_rozofs_ll_removexattr,ino,(char*)name);
    buffer_p = rozofs_fuse_alloc_saved_context();
    if (buffer_p == NULL)
    {
      severe("out of fuse saved context");
      errno = ENOMEM;
      goto error;
    }
    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);
    SAVE_FUSE_PARAM(buffer_p,ino);
    
    START_PROFILING_NB(buffer_p,rozofs_ll_removexattr);

    DEBUG("removexattr (inode: %lu, name: %s)\n", (unsigned long int) ino, name);

    	    
    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }
    /*
    ** clear the timestamp
    */
    ie->xattr_timestamp = 0;

    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid = exportclt.eid;
    memcpy(arg.arg_gw.fid,  ie->fid, sizeof (uuid_t));
    arg.arg_gw.name = (char *)name;
    
    /*
    ** now initiates the transaction towards the remote end
    */
#if 1
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)arg.arg_gw.fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_REMOVEXATTR,(xdrproc_t) xdr_epgw_removexattr_arg_t,(void *)&arg,
                              rozofs_ll_removexattr_cbk,buffer_p); 
#else
    ret =
    rozofs_export_send_common(&exportclt,ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM),EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_REMOVEXATTR,(xdrproc_t) xdr_epgw_removexattr_arg_t,(void *)&arg,
                              rozofs_ll_removexattr_cbk,buffer_p); 
#endif
    if (ret < 0) goto error;
    
    /*
    ** no error just waiting for the answer
    */
    return;

error:
    fuse_reply_err(req, errno);
    /*
    ** release the buffer if has been allocated
    */
    rozofs_trc_rsp(srv_rozofs_ll_removexattr,ino,NULL,1,trc_idx);
    STOP_PROFILING_NB(buffer_p,rozofs_ll_removexattr);
    if (buffer_p != NULL) rozofs_fuse_release_saved_context(buffer_p);
    return;
}


/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_removexattr_cbk(void *this,void *param)
{
   fuse_req_t req; 
   epgw_status_ret_t ret ;
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   struct rpc_msg  rpc_reply;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_status_ret_t;
   int trc_idx;
   fuse_ino_t ino;
   
   errno = 0;   
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,ino);
   RESTORE_FUSE_PARAM(param,trc_idx);
    /*
    ** get the pointer to the transaction context:
    ** it is required to get the information related to the receive buffer
    */
    rozofs_tx_ctx_t      *rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;     
    /*    
    ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
    */
    status = rozofs_tx_get_status(this);
    if (status < 0)
    {
       /*
       ** something wrong happened
       */
       errno = rozofs_tx_get_errno(this);  
       goto error; 
    }
    /*
    ** get the pointer to the receive buffer payload
    */
    recv_buf = rozofs_tx_get_recvBuf(this);
    if (recv_buf == NULL)
    {
       /*
       ** something wrong happened
       */
       errno = EFAULT;  
       goto error;         
    }


    payload  = (uint8_t*) ruc_buf_getPayload(recv_buf);
    payload += sizeof(uint32_t); /* skip length*/
    /*
    ** OK now decode the received message
    */
    bufsize = (int) ruc_buf_getPayloadLen(recv_buf);
    bufsize -= sizeof(uint32_t); /* skip length*/
    xdrmem_create(&xdrs,(char*)payload,bufsize,XDR_DECODE);
    /*
    ** decode the rpc part
    */
    if (rozofs_xdr_replymsg(&xdrs,&rpc_reply) != TRUE)
    {
     TX_STATS(ROZOFS_TX_DECODING_ERROR);
     errno = EPROTO;
     goto error;
    }
    /*
    ** ok now call the procedure to encode the message
    */
    memset(&ret,0, sizeof(ret));    
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       xdr_free(decode_proc, (char *) &ret);
       goto error;
    }   
    if (ret.status_gw.status == EP_FAILURE) {
        errno = ret.status_gw.ep_status_ret_t_u.error;
        xdr_free(decode_proc, (char *) &ret);    
        goto error;
    }
    xdr_free(decode_proc, (char *) &ret);
      
    fuse_reply_err(req, 0);
    goto out;
error:
    fuse_reply_err(req, errno);
out:
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_removexattr,ino,NULL,status,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_removexattr);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
   
    return;
}
/*
**__________________________________________________________________
*/
/**
 * 
 * List the extended attributes of a file
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode of the file to remove attribute from
 * @param size 
 */
 void rozofs_ll_listxattr_cbk(void *this,void *param);

void rozofs_ll_listxattr_nb(fuse_req_t req, fuse_ino_t ino, size_t size) 
{
    ientry_t         *ie = 0;
    int               ret;        
    void             *buffer_p = NULL;
    epgw_listxattr_arg_t arg;
    int trc_idx = rozofs_trc_req_io(srv_rozofs_ll_listxattr,ino,NULL,size,0);
    /*
    ** allocate a context for saving the fuse parameters
    */
    buffer_p = rozofs_fuse_alloc_saved_context();
    if (buffer_p == NULL)
    {
      severe("out of fuse saved context");
      errno = ENOMEM;
      goto error;
    }
    SAVE_FUSE_PARAM(buffer_p,ino);
    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,size);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);
    
    START_PROFILING_NB(buffer_p,rozofs_ll_listxattr);


    DEBUG("listxattr (inode: %lu, size: %llu)\n", (unsigned long int) ino,(unsigned long long int) size);

    	    
    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }

    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid = exportclt.eid;
    memcpy(arg.arg_gw.fid,  ie->fid, sizeof (uuid_t));
    if (size == 0) {
      arg.arg_gw.size = 8192;
    }
    else {
      arg.arg_gw.size = size;
    }  
    /*
    ** now initiates the transaction towards the remote end
    */
#if 1
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)arg.arg_gw.fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_LISTXATTR,(xdrproc_t) xdr_epgw_listxattr_arg_t,(void *)&arg,
                              rozofs_ll_listxattr_cbk,buffer_p); 
#else
    ret = rozofs_export_send_common(&exportclt,ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM),EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_LISTXATTR,(xdrproc_t) xdr_epgw_listxattr_arg_t,(void *)&arg,
                              rozofs_ll_listxattr_cbk,buffer_p); 
#endif
    if (ret < 0) goto error;
    
    /*
    ** no error just waiting for the answer
    */
    return;

error:
    fuse_reply_err(req, errno);
    rozofs_trc_rsp(srv_rozofs_ll_listxattr,ino,NULL,1,trc_idx);
    /*
    ** release the buffer if has been allocated
    */
    STOP_PROFILING_NB(buffer_p,rozofs_ll_listxattr);
    if (buffer_p != NULL) rozofs_fuse_release_saved_context(buffer_p);
    return;
}


/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_listxattr_cbk(void *this,void *param)
{
   fuse_req_t req; 
   epgw_listxattr_ret_t ret;
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   struct rpc_msg  rpc_reply;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_listxattr_ret_t;
   uint64_t list_size = 0;
   size_t size=0;
   int trc_idx;
   fuse_ino_t ino;

   rpc_reply.acpted_rply.ar_results.proc = NULL;        
   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,size);
   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_PARAM(param,ino);
    /*
    ** get the pointer to the transaction context:
    ** it is required to get the information related to the receive buffer
    */
    rozofs_tx_ctx_t      *rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;     

    /*    
    ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
    */
    status = rozofs_tx_get_status(this);
    if (status < 0)
    {
       /*
       ** something wrong happened
       */
       errno = rozofs_tx_get_errno(this);  
       goto error; 
    }
    /*
    ** get the pointer to the receive buffer payload
    */
    recv_buf = rozofs_tx_get_recvBuf(this);
    if (recv_buf == NULL)
    {
       /*
       ** something wrong happened
       */
       errno = EFAULT;  
       goto error;         
    }


    payload  = (uint8_t*) ruc_buf_getPayload(recv_buf);
    payload += sizeof(uint32_t); /* skip length*/
    /*
    ** OK now decode the received message
    */
    bufsize = (int) ruc_buf_getPayloadLen(recv_buf);
    bufsize -= sizeof(uint32_t); /* skip length*/
    xdrmem_create(&xdrs,(char*)payload,bufsize,XDR_DECODE);
    /*
    ** decode the rpc part
    */
    if (rozofs_xdr_replymsg(&xdrs,&rpc_reply) != TRUE)
    {
     TX_STATS(ROZOFS_TX_DECODING_ERROR);
     errno = EPROTO;
     goto error;
    }
    /*
    ** ok now call the procedure to encode the message
    */
    memset(&ret,0, sizeof(ret));    
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       xdr_free(decode_proc, (char *) &ret);
       goto error;
    }   
    if (ret.status_gw.status == EP_FAILURE) {
        errno = ret.status_gw.ep_listxattr_ret_t_u.error;
        xdr_free(decode_proc, (char *) &ret);    
        goto error;
    }
    list_size = ret.status_gw.ep_listxattr_ret_t_u.list.list_len;
    
    if (size == 0) {
        xdr_free(decode_proc, (char *) &ret);        
        fuse_reply_xattr(req, list_size);
        goto out;
    }
        
    if (list_size > size) {
        errno = ERANGE;
        xdr_free(decode_proc, (char *) &ret);    
        goto error;
    }
    
    fuse_reply_buf(req, (char *) ret.status_gw.ep_listxattr_ret_t_u.list.list_val, list_size);    
    xdr_free(decode_proc, (char *) &ret);	
    goto out;
error:
    fuse_reply_err(req, errno);
    rozofs_trc_rsp(srv_rozofs_ll_listxattr,ino,NULL,1,trc_idx);
    
out:
    /*
    ** release the transaction context and the fuse context
    */
    errno = 0;
    rozofs_trc_rsp_attr(srv_rozofs_ll_listxattr,ino,NULL,0,list_size,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_listxattr);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
   
    return;
}
