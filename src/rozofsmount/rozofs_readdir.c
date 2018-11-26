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

#include "rozofs_fuse_api.h"

DECLARE_PROFILING(mpp_profiler_t);

static int old_rozofs_readdir_flag = 0;

void rozofs_ll_readdir_cbk(void *this,void *param);

typedef enum {
  ROZOFS_READIR_FROM_SCRATCH,
  ROZOFS_READIR_FROM_IE,
} ROZOFS_READIR_START_MODE_E; 

#define rozofs_init_db(db) memset(db,0,sizeof(dirbuf_t))

void rozofs_free_db(dirbuf_t * db) { 
  if (db->p) xfree(db->p);
  rozofs_init_db(db);
} 
void rozofs_transfer_db (dirbuf_t * to, dirbuf_t * from) {

  if (to->p) free(to->p);
  
  to->p      = from->p;
  to->size   = from->size;
  to->eof    = from->eof;
  to->cookie = from->cookie;
  rozofs_init_db(from);
}



static void dirbuf_add(fuse_req_t req, dirbuf_t *b, const char *name,
        fuse_ino_t ino, mattr_t * attrs) {

    // Get oldsize of buffer
    size_t oldsize = b->size;
    // Set the inode number in stbuf
    struct stat stbuf;
//    mattr_to_stat(attrs, &stbuf, exportclt.bsize);
    stbuf.st_ino = ino;
    // Get the size for this entry
    b->size += fuse_add_direntry(req, NULL, 0, name, &stbuf, 0);
    // Realloc dirbuf
    b->p = (char *) realloc(b->p, b->size);
    // Add this entry
    fuse_add_direntry(req, b->p + oldsize, b->size - oldsize, name, &stbuf, b->size);
}

#define min(x, y) ((x) < (y) ? (x) : (y))

static int reply_buf_limited(fuse_req_t req, struct dirbuf *b, off_t off,
        size_t maxsize) {
    if (off < b->size) {
        return fuse_reply_buf(req, b->p + off, min(b->size - off, maxsize));
    } else {
        // At the end
        // Free buffer
        if (b->p != NULL) {
            free(b->p);
            b->size = 0;
            b->eof = 0;
            b->cookie = 0;
            b->p = NULL;
        }
        return fuse_reply_buf(req, NULL, 0);
    }
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
 * @param req  request handle
 * @param ino  the inode of the directory to read
 * @param size the maximum size of the data to return 
 * @param off  off to start to read from
 * @param fi   
 */
 
int rozofs_ll_readdir_send_to_export(fid_t fid, uint64_t cookie,void	 *buffer_p) {
    int               ret;        
    epgw_readdir_arg_t  arg;

    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid = exportclt.eid;
    memcpy(arg.arg_gw.fid,  fid, sizeof (fid_t));
    arg.arg_gw.cookie = cookie;
    
    /*
    ** now initiates the transaction towards the remote end
    */
#if 1
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)arg.arg_gw.fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_READDIR,(xdrproc_t) xdr_epgw_readdir_arg_t,(void *)&arg,
                              rozofs_ll_readdir_cbk,buffer_p); 
#else
    ret = rozofs_export_send_common(&exportclt,ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM),EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_READDIR,(xdrproc_t) xdr_epgw_readdir_arg_t,(void *)&arg,
                              rozofs_ll_readdir_cbk,buffer_p); 
#endif
    return ret;  
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
 * @param req  request handle
 * @param ie pointer to the inetry of the directory
 * @param ino  the inode of the directory to read
 * @param size the maximum size of the data to return 
 * @param off  off to start to read from
 * @param fi   
 */
int rozofs_ll_readdir_from_export(ientry_t * ie,dir_t * dir_p, fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi,int trc_idx) {
    int               ret;        
    void             *buffer_p = NULL;
    dirbuf_t         *db=NULL;

    /*
    ** allocate a context for saving the fuse parameters
    */
    buffer_p = rozofs_fuse_alloc_saved_context();
    if (buffer_p == NULL)
    {
      severe("out of fuse saved context");
      errno = ENOMEM;
      return -1;
    }
    
    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,ino);
    SAVE_FUSE_PARAM(buffer_p,size);
    SAVE_FUSE_PARAM(buffer_p,off);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);
    SAVE_FUSE_STRUCT(buffer_p,fi,sizeof( struct fuse_file_info));
    
    db= &dir_p->db;
    /*
    ** now initiates the transaction towards the remote end
    */  
    ret = rozofs_ll_readdir_send_to_export (ie->fid, db->cookie, buffer_p);    
    if (ret >= 0) {
       dir_p->readdir_pending = 1;
      START_PROFILING_NB(buffer_p,rozofs_ll_readdir);    
      return 0;
    }
    /* Error case */
      
    if (buffer_p != NULL) {
      rozofs_fuse_release_saved_context(buffer_p);    
    }      
    return -1;
    
} 

void OLD_rozofs_ll_readdir_nb(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                          struct fuse_file_info *fi) {
			  
    int               ret; 
    ientry_t          *ie = NULL;
    errno = 0;     
     dir_t *dir_p = NULL;  

    int trc_idx = rozofs_trc_req_io(srv_rozofs_ll_readdir,ino,NULL,size,off);
    DEBUG("readdir (%lu, size:%llu, off:%llu)\n", (unsigned long int) ino,
            (unsigned long long int) size, (unsigned long long int) off);

    // Get ientry
    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }

    /*
    ** get the pointer to the dir buf (save in the fh attribute)
    */
    dir_p = (dir_t*)fi->fh;

    // If offset is 0, it maybe the first time the caller read the dir but
    // it might also have already read the a chunk of dir but wants to
    // read from 0 again. it might be overkill but to be sure not using
    // buffer content force exportd readdir call.
    if (off == 0) {
      ret = rozofs_ll_readdir_from_export(ie, dir_p,req, ino, size, off,fi,trc_idx);
      if (ret == 0) return;
      goto error;
    }

    // If all required data is available in the ie, just send the response back
    if (((off + size) <= dir_p->db.size) || (dir_p->db.eof != 0))  {
      // Reply with data
      reply_buf_limited(req, &dir_p->db, off, size);
      return;
    }  

    // let's read from export  and start from the saved point in ie
    ret = rozofs_ll_readdir_from_export(ie,dir_p, req, ino, size, off,fi,trc_idx);
    if (ret == 0) return;

error:
    fuse_reply_err(req, errno);
    /*
    ** release the buffer if has been allocated
    */
    rozofs_trc_rsp(srv_rozofs_ll_readdir,ino,NULL,1,trc_idx);
    return;
}


/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_readdir_cbk(void *this,void *param)
{
   fuse_req_t req; 
   epgw_readdir_ret_t ret ;
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   struct rpc_msg  rpc_reply;
   xdrproc_t decode_proc = (xdrproc_t) xdr_epgw_readdir_ret_t;
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   fuse_ino_t   ino;
   size_t       size;
   off_t        off;
   ientry_t    *ie = 0;
//   ientry_t    *ie2 = 0;
   ep_child_t  *iterator = NULL;
    mattr_t     attrs;
    dirbuf_t   *db=NULL;
    int trc_idx;
    struct fuse_file_info *fi ;
    dir_t *dir_p = NULL;
    
    errno = 0;
                
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

    // Get ientry
    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
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
        errno = ret.status_gw.ep_readdir_ret_t_u.error;
        xdr_free(decode_proc, (char *) &ret);    
        goto error;
    }
            
    db->eof    = ret.status_gw.ep_readdir_ret_t_u.reply.eof;
    db->cookie = ret.status_gw.ep_readdir_ret_t_u.reply.cookie;
 
    // Process the list of children
    iterator = ret.status_gw.ep_readdir_ret_t_u.reply.children;
      memset(&attrs, 0, sizeof (mattr_t));
    while (iterator != NULL) {


#if 0 // FDL useless might cause some error with lookup
      // May be already cached
      if (!(ie2 = get_ientry_by_fid((unsigned char *)iterator->fid))) {
        // If not, cache it
        ie2 =  alloc_ientry((unsigned char *)iterator->fid); 
      }
#endif      
      memcpy(attrs.fid, iterator->fid, sizeof (fid_t));

      rozofs_inode_t *inode_p ;
      inode_p = (rozofs_inode_t*) attrs.fid;

      // Add this directory entry to the buffer
      dirbuf_add(req, db, iterator->name, inode_p->fid[1], &attrs);
     
      iterator = iterator->next;
    }

    // Free the xdr response structure
    xdr_free(decode_proc, (char *) &ret);

    // When we reach the end of this current child list but the
    // end of stream is not reached and the requested size is greater
    // than the current size of buffer then send another request
    if ((db->eof == 0) && ((off + size) > db->size)) {

      if (rozofs_ll_readdir_send_to_export (ie->fid, db->cookie, param) == 0) {
       dir_p->readdir_pending = 1;
        goto loop_on;
      }
      goto error;
      
    }

    // If all required data is available in the ie, just send the response back
    reply_buf_limited(req,db, off, size);    
    goto out;
    
error:
    fuse_reply_err(req, errno);
out:
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_readdir,ino,NULL,status,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_readdir);
    rozofs_fuse_release_saved_context(param);   

loop_on:    
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
   
    return;
}

#if 1
/*
**____________________________________________________________________________________________________________
**____________________________________________________________________________________________________________

*/

struct rozofs_fuse_dirent {
	uint64_t	ino;
	uint64_t	off;
	uint32_t	namelen;
	uint32_t	type;
	char name[];
};



#define FUSE_NAME_OFFSET offsetof(struct rozofs_fuse_dirent, name)
#define FUSE_DIRENT_ALIGN(x) (((x) + sizeof(uint64_t) - 1) & ~(sizeof(uint64_t) - 1))
#define FUSE_DIRENT_SIZE(d) \
	FUSE_DIRENT_ALIGN(FUSE_NAME_OFFSET + (d)->namelen)

int rozofs_parse_dirfile(fuse_req_t req,dirbuf_t *db,uint64_t cookie_client,int client_size)
{
     char *buf;
     int left_size = client_size;
     int nbytes;
     uint64_t cur_cookie_buf = 0;
     size_t cur_cookie_offset_buf = db->cookie_offset_buf;
     
     if (db->p == NULL)
     {
       errno = EAGAIN;
       return -1;
     }
     /*
     ** Check of the cookie requested by the client is the last provided
     ** by a previous readdir
     */
     if (cookie_client != db->last_cookie_buf)
     {
	/*
	** need to get data from export
	*/
	errno = EAGAIN;
	return -1;
     }
     /*
     ** set the pointer to the beginning of the entry corresponding to the
     ** provided entry
     */
     buf =db->p+db->cookie_offset_buf;
     nbytes = db->size-db->cookie_offset_buf;

     while (nbytes >= FUSE_NAME_OFFSET) {
	     struct rozofs_fuse_dirent *dirent = (struct rozofs_fuse_dirent *) buf;
	     size_t reclen = FUSE_DIRENT_SIZE(dirent);
	     if (reclen > left_size)
		     break;

	     buf += reclen;
	     nbytes -= reclen;
	     left_size-= reclen;
	     cur_cookie_offset_buf += reclen;
	     cur_cookie_buf = dirent->off;
     }
     if (client_size == left_size)
     {
	if (db->eof == 0) 
	{
	   errno = EAGAIN;
	   return -1;
	}
	fuse_reply_buf(req, NULL, 0);
	return 0;
     }
     fuse_reply_buf(req, db->p+db->cookie_offset_buf, client_size - left_size);
     db->last_cookie_buf = cur_cookie_buf;
     db->cookie_offset_buf = cur_cookie_offset_buf;
     return 0;
}	
	   
#define ROZOFS_READDIR2_BUFSIZE (64*1024)

/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_readdir2_cbk(void *this,void *param)
{
   fuse_req_t req; 
   epgw_readdir2_ret_t ret ;
   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   struct rpc_msg  rpc_reply;
   xdrproc_t decode_proc = (xdrproc_t) xdr_epgw_readdir2_ret_t;
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   fuse_ino_t   ino;
   size_t       size;
   off_t        off;
   ientry_t    *ie = 0;
    dirbuf_t   *db=NULL;
    int trc_idx;
    struct fuse_file_info *fi ;
    dir_t *dir_p = NULL;
    
    errno = 0;
                
    RESTORE_FUSE_PARAM(param,req);
    RESTORE_FUSE_PARAM(param,ino);
    RESTORE_FUSE_PARAM(param,size);
    RESTORE_FUSE_PARAM(param,off);    
    RESTORE_FUSE_PARAM(param,trc_idx);    
    RESTORE_FUSE_PARAM(param,fi);

    dir_p = (dir_t*)fi->fh;
    dir_p->readdir_pending = 0;
    db = &dir_p->db;
    if (db->p == NULL)
    {
      db->p = xmalloc(ROZOFS_READDIR2_BUFSIZE);
    }
    db->size = 0;    
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

    // Get ientry
    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }
    
    payload  = (uint8_t*) ruc_buf_getPayload(recv_buf);
    payload += sizeof(uint32_t); /* skip length*/
    /*
    ** OK now decode the received message
    */
    bufsize = (int) ruc_buf_getPayloadLen(recv_buf);
    bufsize-= sizeof(uint32_t); /* skip length*/
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
    ** ok now call the procedure to decode the message
    */
    memset(&ret,0, sizeof(ret));    
    ret.status_gw.ep_readdir2_ret_t_u.reply.value.value_val = db->p;
    
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       ret.status_gw.ep_readdir2_ret_t_u.reply.value.value_val = NULL;
       xdr_free(decode_proc, (char *) &ret);
       goto error;
    }   
    
    if (ret.status_gw.ep_readdir2_ret_t_u.reply.value.value_len > ROZOFS_READDIR2_BUFSIZE) {
      severe("rozofs_ll_readdir2_cbk receive %d while expecting %d max",
              ret.status_gw.ep_readdir2_ret_t_u.reply.value.value_len,
              ROZOFS_READDIR2_BUFSIZE);
    }
    
    if (ret.status_gw.status == EP_FAILURE) {
        errno = ret.status_gw.ep_readdir2_ret_t_u.error;
        ret.status_gw.ep_readdir2_ret_t_u.reply.value.value_val = NULL;
        xdr_free(decode_proc, (char *) &ret);    
        goto error;
    }
            
    db->eof    = ret.status_gw.ep_readdir2_ret_t_u.reply.eof;
    db->cookie = ret.status_gw.ep_readdir2_ret_t_u.reply.cookie;
    /*
    ** the received buffer has been copied in dp->p during the decoding procedure
    */
    ret.status_gw.ep_readdir2_ret_t_u.reply.value.value_val = NULL;
    db->size = ret.status_gw.ep_readdir2_ret_t_u.reply.value.value_len;
    xdr_free(decode_proc, (char *) &ret);
    db->last_cookie_buf = off;
    db->cookie_offset_buf = 0;
    status = rozofs_parse_dirfile( req,db,(uint64_t) off,(int) size);
    if (status < 0) goto error;

    goto out;
    
error:
    if (errno == ENOSYS)
    {
       /*
       ** revert to the old callback
       */
       struct fuse_file_info file_info;
       /*
       ** need to copy the file_info before releasing the fuse context
       */
       memcpy(&file_info,fi,sizeof(struct fuse_file_info));
       /*
       ** indicates that old API should be used
       */
       old_rozofs_readdir_flag = 1;
       rozofs_trc_rsp(srv_rozofs_ll_readdir,ino,NULL,status,trc_idx);
       STOP_PROFILING_NB(param,rozofs_ll_readdir);  
       /*
       ** release the fuse context and the received buffer
       */  
       rozofs_fuse_release_saved_context(param);     
       if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
       if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);  
       /*
       ** forward the readdir to the exportd using the old format
       */ 
       status = rozofs_ll_readdir_from_export(ie, dir_p,req, ino, size, off,fi,trc_idx);
       if (status == 0) return;
       fuse_reply_err(req, errno);
       /*
       ** release the buffer if has been allocated
       */
       rozofs_trc_rsp(srv_rozofs_ll_readdir,ino,NULL,1,trc_idx);
       return;
    }
    fuse_reply_err(req, errno);
out:
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_readdir,ino,NULL,status,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_readdir);
    rozofs_fuse_release_saved_context(param);     
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
   
    return;
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
 * @param req  request handle
 * @param ino  the inode of the directory to read
 * @param size the maximum size of the data to return 
 * @param off  off to start to read from
 * @param fi   
 */
 
int rozofs_ll_readdir2_send_to_export(fid_t fid, uint64_t cookie,void	 *buffer_p) {
    int               ret;        
    epgw_readdir_arg_t  arg;

    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid = exportclt.eid;
    memcpy(arg.arg_gw.fid,  fid, sizeof (fid_t));
    arg.arg_gw.cookie = cookie;
    
    /*
    ** now initiates the transaction towards the remote end
    */
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,(unsigned char*)arg.arg_gw.fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_READDIR2,(xdrproc_t) xdr_epgw_readdir_arg_t,(void *)&arg,
                              rozofs_ll_readdir2_cbk,buffer_p); 

    return ret;  
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
 * @param req  request handle
 * @param ie pointer to the inetry of the directory
 * @param ino  the inode of the directory to read
 * @param size the maximum size of the data to return 
 * @param off  off to start to read from
 * @param fi   
 */
int rozofs_ll_readdir2_from_export(ientry_t * ie,dir_t * dir_p, fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi,int trc_idx) {
    int               ret;        
    void             *buffer_p = NULL;

    /*
    ** allocate a context for saving the fuse parameters
    */
    buffer_p = rozofs_fuse_alloc_saved_context();
    if (buffer_p == NULL)
    {
      severe("out of fuse saved context");
      errno = ENOMEM;
      return -1;
    }
    
    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,ino);
    SAVE_FUSE_PARAM(buffer_p,size);
    SAVE_FUSE_PARAM(buffer_p,off);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);
    SAVE_FUSE_STRUCT(buffer_p,fi,sizeof( struct fuse_file_info));

    /*
    ** now initiates the transaction towards the remote end
    */  
    ret = rozofs_ll_readdir2_send_to_export (ie->fid, off, buffer_p);    
    if (ret >= 0) {
       dir_p->readdir_pending = 1;
      START_PROFILING_NB(buffer_p,rozofs_ll_readdir);    
      return 0;
    }
    /* Error case */
      
    if (buffer_p != NULL) {
      rozofs_fuse_release_saved_context(buffer_p);    
    }      
    return -1;
    
} 

void rozofs_ll_readdir_nb(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                          struct fuse_file_info *fi) {
			  
    int               ret; 
    ientry_t          *ie = NULL;
    errno = 0;     
     dir_t *dir_p = NULL; 
      
    /*
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_OTHER], 1);
     
     if (old_rozofs_readdir_flag!=0)
     {
       /*
       ** the exportd does not support the new readdir format
       */
       return OLD_rozofs_ll_readdir_nb(req, ino,size,off,fi);     
     }

    int trc_idx = rozofs_trc_req_io(srv_rozofs_ll_readdir,ino,NULL,size,off);

    // Get ientry
    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }

    /*
    ** get the pointer to the dir buf (save in the fh attribute)
    */
    dir_p = (dir_t*)fi->fh;

    // If offset is 0, it maybe the first time the caller read the dir but
    // it might also have already read the a chunk of dir but wants to
    // read from 0 again. it might be overkill but to be sure not using
    // buffer content force exportd readdir call.
    if (off == 0) {
      ret = rozofs_ll_readdir2_from_export(ie, dir_p,req, ino, size, off,fi,trc_idx);
      if (ret == 0) return;
      goto error;
    }

    ret = rozofs_parse_dirfile(req,&dir_p->db,(uint64_t)off,(int)size);
    if (ret == 0) goto out;
    
    if (errno != EAGAIN) goto error;
    /*
    ** let's read from export  and start from the saved point in ie
    */
    ret = rozofs_ll_readdir2_from_export(ie,dir_p, req, ino, size, off,fi,trc_idx);
    if (ret == 0) return;

error:
    fuse_reply_err(req, errno);
    
out:
    /*
    ** release the buffer if has been allocated
    */
    rozofs_trc_rsp(srv_rozofs_ll_readdir,ino,NULL,1,trc_idx);
    return;
}
#endif
