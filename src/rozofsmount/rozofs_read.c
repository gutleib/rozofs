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

//#define TRACE_FS_READ_WRITE 1
//#warning TRACE_FS_READ_WRITE active
#include <inttypes.h>
#include <linux/fuse.h>
#include <rozofs/rpc/eproto.h>
#include <rozofs/rpc/storcli_proto.h>

#include "rozofs_fuse_api.h"
#include "rozofs_sharedmem.h"
#include "rozofs_modeblock_cache.h"
#include "rozofs_cache.h"
#include "rozofs_rw_load_balancing.h"
#include "rozofs_fuse_thread_intf.h"
#include "rozofs_kpi.h"

DECLARE_PROFILING(mpp_profiler_t);


void rozofs_ll_read_cbk(void *this,void *param);


static char *local_buf_flush= NULL;  /**< flush buffer for asynchronous flush */

int rozofs_ll_read_defer_first(void *param,uint8_t *buffer,int received_len,void *shared_buf_ref);

/**
* Allocation of the flush buffer
  @param size_kB : size of the flush buffer in KiloBytes
  
  @retval none
*/
void rozofs_allocate_flush_buf(int size_kB)
{
  local_buf_flush = xmalloc(1024*size_kB);
}
/**
* Align off as well as len to read on blocksize bundary
  @param[in]  off         : offset to read
  @param[in]  len         : length to read
  @param[out] off_aligned : aligned read offset
  @param[out] len_aligned : aligned length to read  
  
  @retval none
*/
static inline void rozofs_align_off_and_len(uint64_t off, int len, uint64_t * off_aligned, int * len_aligned) {

  *off_aligned = (off/ROZOFS_CACHE_BSIZE)*ROZOFS_CACHE_BSIZE;       
  *len_aligned = len + (off-*off_aligned);
  if ((*len_aligned % ROZOFS_CACHE_BSIZE) == 0) return;
  *len_aligned = ((*len_aligned/ROZOFS_CACHE_BSIZE)+1)*ROZOFS_CACHE_BSIZE;
}

/** Reads the distributions on the export server,
 *  adjust the read buffer to read only whole data blocks
 *  and uses the function read_blocks to read data
 *
 * @param *f: pointer to the file structure
 * @param off: offset to read from
 * @param *buf: pointer where the data will be stored: buffer associated with the file_t structure
 * @param len: length to read: (correspond to the max buffer size defined in the exportd parameters
 * @param *last_block_size_p: pointer to store the size of the last block size
 *  read
 *
 * @return: the length read on success, -1 otherwise (errno is set)
 */
 
static int read_buf_nb(void *buffer_p,file_t * f, uint64_t off, char *buf, uint32_t len) 
{
   uint64_t bid = 0;
   uint32_t nb_prj = 0;
   storcli_read_arg_t  args;
   ientry_t *ie;
   int ret;
   int storcli_idx;
   int bbytes = ROZOFS_BSIZE_BYTES(exportclt.bsize);
   int max_prj = ROZOFS_MAX_BLOCK_PER_MSG;
   uint64_t ino;
   /*
   ** get the inode saved in the fuse context: it might needed for the case when storcli needs
   ** to open the inode during Mojette Transform
   */
   RESTORE_FUSE_PARAM(buffer_p,ino);
   // Nb. of the first block to read
   bid = off / bbytes;
   nb_prj = len / bbytes;
   if (nb_prj > max_prj)
   {
     severe("bad nb_prj %d max %d bid %llu off %llu len %u",nb_prj,max_prj,(long long unsigned int)bid,(long long unsigned int)off,len);   
   }
   /*
   ** Check the case of the multifile mode: there is a master inode with several slave inodes associated with it
   */
   ie = f->ie; 
   if (ie->attrs.multi_desc.common.master != 0)
   {   
      return  read_buf_multitple_nb(buffer_p,f, off, buf,len);
   }
   /*
   ** default case: only the master inode with no slave inodes
   */
    if (rozofs_rotation_read_modulo == 0) {
      f->rotation_idx = 0;
    }
    else {
      f->rotation_counter++;
      if ((f->rotation_counter % rozofs_rotation_read_modulo)==0) {
	f->rotation_idx++;
      }
    }  
    args.sid = f->rotation_idx;

    // Fill request
    args.cid = ie->attrs.attrs.cid;
    args.layout = f->export->layout;
    args.bsize = exportclt.bsize;    
    memcpy(args.dist_set, ie->attrs.attrs.sids, sizeof (sid_t) * ROZOFS_SAFE_MAX);
    memcpy(args.fid, f->fid, sizeof (fid_t));
    args.proj_id = 0; // N.S
    args.bid = bid;
    args.nb_proj = nb_prj;
    
    ret = rozofs_fill_storage_info(ie,&args.cid,args.dist_set,args.fid);
    if (ret < 0)
    {
      severe("FDL bad storage information encoding");
      return ret;
    }

 //   lbg_id = storcli_lbg_get_lbg_from_fid(f->fid);
    storcli_idx = stclbg_storcli_idx_from_fid(f->fid);
    /*
    ** allocate a shared buffer for reading
    */
#if 1

//    int stor_idx =storcli_get_storcli_idx_from_fid(f->fid);
    int shared_buf_idx;
    rozofs_shared_buf_rd_hdr_t  *share_rd_p;   
    uint32_t length;
    void *shared_buf_ref = rozofs_alloc_shared_storcli_buf(SHAREMEM_IDX_READ);
    if (shared_buf_ref != NULL)
    {
      /*
      ** clear the first 4 bytes of the array that is supposed to contain
      ** the reference of the transaction
      */
       args.cmd_idx = 0;
       share_rd_p = (rozofs_shared_buf_rd_hdr_t *)ruc_buf_getPayload(shared_buf_ref);
       share_rd_p->cmd[args.cmd_idx].xid = 0;
       share_rd_p->cmd[args.cmd_idx].received_len = 0;       
       share_rd_p->cmd[args.cmd_idx].offset_in_buffer = 0;
       /*
       ** Check if the inode must be filled in the storcli request in order to have a direct write in the
       ** page cache of Linux
       */
       rozofs_check_for_shared_buffer_by_pass(&share_rd_p->cmd[args.cmd_idx].inode,ino);
       /*
       ** get the index of the shared payload in buffer
       */
       shared_buf_idx = rozofs_get_shared_storcli_payload_idx(shared_buf_ref,SHAREMEM_IDX_READ,&length);
       if (shared_buf_idx != -1)
       {
         /*
         ** save the reference of the shared buffer in the fuse context
         */
         SAVE_FUSE_PARAM(buffer_p,shared_buf_ref);
         args.spare     = 'S';
         args.shared_buf_idx = shared_buf_idx;	 
       }
    }
    else
    {
       fatal("FDL Out of a shared buffer");
    
    }
#endif
    rozofs_fuse_read_write_stats_buf.read_req_cpt++;

    /*
    ** now initiates the transaction towards the remote end
    */
    f->buf_read_pending++;
    ret = rozofs_storcli_send_common(NULL,ROZOFS_TMR_GET(TMR_STORCLI_PROGRAM),STORCLI_PROGRAM, STORCLI_VERSION,
                              STORCLI_READ,(xdrproc_t) xdr_storcli_read_arg_t,(void *)&args,
                              rozofs_ll_read_cbk,buffer_p,storcli_idx,f->fid); 
    if (ret < 0) goto error;
    
    /*
    ** no error just waiting for the answer
    */
    if (f->buf_read_pending > 1)
    {
    
//      severe("FDL Read ahead detected %d", f->buf_read_pending);
    }
    return ret;    
error:
    f->buf_read_pending--;
    return ret;

}

/**
*
*
 @retval 0 : read is done and length_p has the read length
 @retval 1 : read is in progress
*/
int file_read_nb(void *buffer_p,file_t * f, uint64_t off, char **buf, uint32_t len, size_t *length_p) 
{
    int64_t length = -1;
    DEBUG_FUNCTION;
    ientry_t * ie=NULL;    
    *length_p = -1;
    int ret;

    /*
    ** Flush on disk any pending data in any buffer open on this file 
    ** before reading.
    */
    ie = (ientry_t*)f->ie;
    flush_write_ientry(ie);    
    
    /*
    ** Check whether the buffer content is valid or if it must be forgotten
    */
    if (f->read_consistency != ie->read_consistency) {
      /* The file has been modified since this buffer has been read. The data
      ** it contains are questionable. Better forget them.
      */
      f->read_from = f->read_pos = 0;
      f->read_consistency = ie->read_consistency;

    }

    if ((off < f->read_from) || (off > f->read_pos) ||((off+len) >  f->read_pos ))
    {
       /*
       ** need to read data from disk
       **  1- check if there is some pending data to write
       **  2- trigger a read
       */
        if (f->buf_write_wait) {
          warning("buf_write_wait is set after flush_write_ientry");
        }
       /*
       ** The file has just been created and is empty so far
       ** Don't request the read to the storio, this would trigger an io error
       ** since the file do not yet exist on disk
       ** For the case of the writeback cache, it might be possible to have some reads that take
       ** place because the off+size is not aligned on a 4KB boundary.
       ** In that case if the offset is greater or equal to the known file size, we return with a length of 0
       ** This will avoid an I/O error if there is a pending write that has been triggered by the writeback cache
       ** flush       
       */
       if (ie->attrs.attrs.size <= off) {
         *length_p = 0;
         return 0;       
       }
    
       /*
       ** check if there is pending read in progress, in such a case, we queue the
       ** current request and we process it later upon the receiving of the response
       */
       if ((f->buf_read_pending > 0)&& (len < f->export->bufsize))
       {
#ifdef TRACE_FS_READ_WRITE
      info("FUSE READ_QUEUED read[%llx:%llx],wrb%d[%llx:%llx],rdb[%llx:%llx]",
            (long long unsigned int)off,(long long unsigned int)(off+len),
            f->buf_write_wait,
            (long long unsigned int)f->write_from,(long long unsigned int)f->write_pos,
            (long long unsigned int)f->read_from,(long long unsigned int)f->read_pos);     
#endif
          /*
          ** some read are in progress; queue this one
          */
          fuse_ctx_read_pending_queue_insert(f,buffer_p);
          return 1;      
       }
       /*
       ** check if the expected data are in the cache
       ** need to round up the offset to a 8K boundary
       ** adjust the length to 8K
       */
//#warning bad instruction off_aligned = (((off-1)/ROZOFS_CACHE_BSIZE)+1)* ROZOFS_CACHE_BSIZE;
       uint64_t off_aligned;
       int      len_aligned;
       rozofs_align_off_and_len(off,len, &off_aligned, &len_aligned);  
#if 0
       if (len_aligned  < 2*ROZOFS_CACHE_BSIZE) 
       {
          /*
          ** min length is 16K
          */
           len_aligned =  2*ROZOFS_CACHE_BSIZE;
       }
#endif       
       /*
       ** we have to control the returned length because if the returned length is less than
       ** the one expected by fuse we need to read from disk
       */
       int read_cache_len = rozofs_mbcache_get(f->fid,off_aligned,len_aligned,(uint8_t*)f->buffer);
       if (read_cache_len >= 0) 
       {         
         /*
         ** OK we got data from cache: now we must check that it corrsponds  to the
         ** expected data from fuse.
         ** if the length is less than expected, we must read from disk except
         ** if we hit the end of file (for this we check against the size the
         ** file
         */
         if (((off_aligned + read_cache_len) == ie->attrs.attrs.size) || (read_cache_len >= len))
         {
           f->read_from = off_aligned;
           f->read_pos = off_aligned+read_cache_len;
           /*
           ** give back data to the application
           */
           length =(len <= (f->read_pos - off )) ? len : (f->read_pos - off);
           *buf = f->buffer + (off - f->read_from);    
           *length_p = (size_t)length;
            return 0; 
          }        
       }
       
       /*
       ** either nothing in the cache or not enough data -> read from remote storage
       */
       //ret = read_buf_nb(buffer_p,f,off, f->buffer, f->export->bufsize);

       /* Let's read ahead : when half of the buffer size is requested, let's read read a whole buffer size */
       if (len_aligned <= (f->export->bufsize)) 
       {
         if (len_aligned >= (f->export->bufsize/2)) len_aligned = f->export->bufsize;
         /* when requested size is too small, let's read the minimum read configured size */
         if (len_aligned < f->export->min_read_size) len_aligned = f->export->min_read_size;
       }
       
       ret = read_buf_nb(buffer_p,f,off_aligned, f->buffer, len_aligned);
       if (ret < 0)
       {
         *length_p = -1;
          return 0;
       }
       /*
       ** read is in progress
       */
       f->buf_read_wait = 1;
#ifdef TRACE_FS_READ_WRITE
      info("FUSE READ_IN_PRG read[%llx:%llx],wrb%d[%llx:%llx],rdb[%llx:%llx]",
            (long long unsigned int)off,(long long unsigned int)(off+len),
            f->buf_write_wait,
            (long long unsigned int)f->write_from,(long long unsigned int)f->write_pos,
            (long long unsigned int)f->read_from,(long long unsigned int)f->read_pos);     
#endif
       return 1;
    }     
    /*
    ** data are available in the current buffer
    */    
#ifdef TRACE_FS_READ_WRITE
      info("FUSE READ_DIRECT read[%llx:%llx],wrb%d[%llx:%llx],rdb[%llx:%llx]",
            (long long unsigned int)off,(long long unsigned int)(off+len),
            f->buf_write_wait,
            (long long unsigned int)f->write_from,(long long unsigned int)f->write_pos,
            (long long unsigned int)f->read_from,(long long unsigned int)f->read_pos);     
#endif
    length =(len <= (f->read_pos - off )) ? len : (f->read_pos - off);
    *buf = f->buffer + (off - f->read_from);    
    *length_p = (size_t)length;
    /*
    ** check for end of buffer to trigger a readahead
    */
//#warning no readahead
//    return 0;
    
    if ((f->buf_read_pending ==0) &&(len >= (f->export->bufsize/2)))
    {
      if ((off+length) == f->read_pos)
      {
        /*
        ** stats
        */
        rozofs_fuse_read_write_stats_buf.readahead_cpt++;
       
        uint32_t readahead = 1;
        SAVE_FUSE_PARAM(buffer_p,readahead);


      }    
    }
    return 0;
}




/**
*  data read deferred

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param parent : inode parent provided by rozofsmount
 @param name : name to search in the parent directory
 
 @retval none
*/

void rozofs_ll_read_defer(void *param) 
{
   fuse_req_t req; 
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   size_t size;
   uint64_t off;   
   file_t *file;
   int read_in_progress = 0;
   char *buff;
   size_t length = 0;
   uint32_t readahead ;
   int trc_idx;

   while(param)
   {   
   RESTORE_FUSE_PARAM(param,readahead);
   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,size);
   RESTORE_FUSE_PARAM(param,off);
   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_STRUCT(param,fi,sizeof( struct fuse_file_info));    

   file = (file_t *) (unsigned long) fi->fh;   

    buff = NULL;
    read_in_progress = file_read_nb(param,file, off, &buff, size,&length);
    if (read_in_progress)
    {
      /*
      ** no error just waiting for the answer
      */
      return;    
    }
     /*
     ** The data were available in the current buffer
     */
     if (length == -1)
         goto error;
     fuse_reply_buf(req, (char *) buff, length);
     goto out;           

error:
    fuse_reply_err(req, errno);
    /*
    ** release the buffer if has been allocated
    */
out:
    rozofs_trc_rsp(srv_rozofs_ll_read,0/*ino*/,file->fid,(errno==0)?0:1,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_read);

    if (param != NULL) 
    {
      /*
      ** check readahead case
      */
      RESTORE_FUSE_PARAM(param,readahead);
      if (readahead == 0) 
      {
        rozofs_fuse_release_saved_context(param);
      }
      else
      {
        int ret;
        off = file->read_pos;
        size_t size = file->export->bufsize;
        ret = rozofs_mbcache_check(file->fid,off,size);
        if (ret == 0)
        {
          /*
          ** data are in the cache: we are done, release the context
          */
          rozofs_fuse_release_saved_context(param);
        }
        else
        {
          SAVE_FUSE_PARAM(param,off);
          SAVE_FUSE_PARAM(param,size); 
          /*
          ** attempt to read
          */  
          trc_idx = rozofs_trc_req_io(srv_rozofs_ll_read,0/*ino*/,file->fid,size,off);          
          SAVE_FUSE_PARAM(param,trc_idx); 
	  ret = read_buf_nb(param,file,off, file->buffer, size);      
          if (ret < 0)
          {
             /*
             ** read error --> release the context
             */
             rozofs_trc_rsp(srv_rozofs_ll_read,0/* ino */,file->fid,(errno==0)?0:1,trc_idx);
             rozofs_fuse_release_saved_context(param);
          }
        }   
      }
    }
    /*
    ** Check if there is some other read request that are pending
    */

      param = fuse_ctx_read_pending_queue_get(file);
    }
    return;
}


/**
*  data read

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param parent : inode parent provided by rozofsmount
 @param name : name to search in the parent directory
 
 @retval none
*/

void rozofs_ll_read_nb(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
        struct fuse_file_info *fi) 
{
    ientry_t *ie = 0;
    void *buffer_p = NULL;
    int read_in_progress = 0;
    char *buff;
    size_t length = 0;
    uint32_t readahead =0;
    errno = 0;
    file_t *file = (file_t *) (unsigned long) fi->fh;
    int trc_idx = rozofs_trc_req_io(srv_rozofs_ll_read,(fuse_ino_t)file,file->fid,size,off);

#if 0   
    /*
    ** Get the inode from the read request: It might be possible that the value is not there
    */
    void *fuse_req_in = rozofs_get_fuse_req_receive_buf();
    if (fuse_req_in == NULL)
    {
       fatal ("No fuse receive buffer !!");
    }
    struct fuse_read_in *fuse_kern_rd_p;
    struct fuse_in_header *fuse_in_hdr_p;
    void * kernel_fuse_inode_addr_p = NULL;  
    fuse_in_hdr_p = (struct fuse_in_header* )fuse_req_in;
    fuse_in_hdr_p +=1; 
    fuse_kern_rd_p = (struct fuse_read_in*) fuse_in_hdr_p;
    if (fuse_kern_rd_p->padding)
    {
      kernel_fuse_inode_addr_p = (void*)fuse_kern_rd_p->lock_owner; 
      info("FDL inode addr:%p ino: %llu ",kernel_fuse_inode_addr_p,(unsigned long long int)ino);
    }
#endif
    /*
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_READ_IO], 1);

    /*
    ** allocate a context for saving the fuse parameters
    */
    buffer_p = _rozofs_fuse_alloc_saved_context("rozofs_ll_read_nb");
    if (buffer_p == NULL)
    {
      severe("out of fuse saved context");
      errno = ENOMEM;
      goto error;
    }
    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,ino); /**< needed by storcli to open the inode during Mojette Transform  */
    SAVE_FUSE_PARAM(buffer_p,size);
    SAVE_FUSE_PARAM(buffer_p,off);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);
    SAVE_FUSE_PARAM(buffer_p,readahead);
    SAVE_FUSE_STRUCT(buffer_p,fi,sizeof( struct fuse_file_info));    

    /*
    ** stats
    */
    ROZOFS_READ_STATS_ARRAY(size);
    rozofs_fuse_read_write_stats_buf.read_fuse_cpt++;
    
    DEBUG("read to inode %lu %llu bytes at position %llu\n",
            (unsigned long int) ino, (unsigned long long int) size,
            (unsigned long long int) off);

    START_PROFILING_IO_NB(buffer_p,rozofs_ll_read, size);

    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }
    /*
    ** file KPI 
    */
    rzkpi_file_stat_update(ie->pfid,(int)size,RZKPI_READ);
    /*
    ** check if the application is attempting to read atfer a close (_ll_release)
    */
    if (rozofs_is_file_closing(file))
    {
      errno = EBADF;
      goto error;        
    }
    buff = NULL;
    read_in_progress = file_read_nb(buffer_p,file, off, &buff, size,&length);
    if (read_in_progress)
    {
      /*
      ** no error just waiting for the answer
      */
      return;    
    }
     /*
     ** The data were available in the current buffer
     */
     if (length == -1)
         goto error;
     fuse_reply_buf(req, (char *) buff, length);
     file->current_pos = (off+length);
     goto out;           

error:
    fuse_reply_err(req, errno);
    /*
    ** release the buffer if has been allocated
    */
out:
    rozofs_trc_rsp(srv_rozofs_ll_read,(fuse_ino_t)file,file->fid,(errno==0)?0:1,trc_idx);
    if (buffer_p != NULL) 
    {
      /*
      ** check readahead case
      */
      RESTORE_FUSE_PARAM(buffer_p,readahead);
      if (readahead == 0) 
      {
        /*
        ** release the context
        */
        rozofs_fuse_release_saved_context(buffer_p);
      }
      else
      {
        int ret;
        off = file->read_pos;
        size_t size = file->export->bufsize;
        /*
        ** check the presence of the data in the cache
        */
        ret = rozofs_mbcache_check(file->fid,off,size);
        if (ret == 0)
        {
          /*
          ** data are in the cache: we are done, release the context
          */
          rozofs_fuse_release_saved_context(buffer_p);
        }
        else
        {
          SAVE_FUSE_PARAM(buffer_p,off);
          SAVE_FUSE_PARAM(buffer_p,size);      
          /**
          * attempt to read
          */
          trc_idx = rozofs_trc_req_io(srv_rozofs_ll_read,ino,file->fid,size,off);
	  SAVE_FUSE_PARAM(buffer_p,trc_idx);      
          ret = read_buf_nb(buffer_p,file,off, file->buffer, size);      
          if (ret < 0)
          {
             /*
             ** read error --> release the context
             */
             rozofs_trc_rsp(srv_rozofs_ll_read,(fuse_ino_t)file,file->fid,(errno==0)?0:1,trc_idx);
             rozofs_fuse_release_saved_context(buffer_p);
          }
        }
      }
    }

    return;
}

/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_read_cbk(void *this,void *param) 
{
   fuse_req_t req; 
   struct rpc_msg  rpc_reply;
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   size_t size;
   uint64_t off;
   uint64_t next_read_from, next_read_pos;
   uint64_t offset_buf_wr_start,offset_end;
   char *buff;
   size_t length;
//   int len_zero;
   uint8_t *src_p,*dst_p;
   int len;
   void *shared_buf_ref;
   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   storcli_read_ret_no_data_t ret;
   xdrproc_t decode_proc = (xdrproc_t)xdr_storcli_read_ret_no_data_t;
   file_t *file;
   uint32_t readahead;
   int position ;
   int trc_idx;
   errno =0;
   int bbytes = ROZOFS_BSIZE_BYTES(exportclt.bsize);
   ientry_t *ie;
   int update_pending_buffer_todo = 1;

   
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,size);
   RESTORE_FUSE_PARAM(param,readahead);
   RESTORE_FUSE_STRUCT(param,fi,sizeof( struct fuse_file_info));    
   RESTORE_FUSE_PARAM(param,off);
   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_PARAM(param,shared_buf_ref);

   file = (file_t *) (unsigned long)  fi->fh;  
   ie = file->ie; 
   file->buf_read_pending--;
   if (file->buf_read_pending < 0)
   {
     severe("buf_read_pending mismatch, %d",file->buf_read_pending);
     file->buf_read_pending = 0;     
   }
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
       xdr_free((xdrproc_t) decode_proc, (char *) &ret);
       goto error;
    }   
    if (ret.status == STORCLI_FAILURE) {
    
        /*
	** Case of the file that has not been written to disk 
	*/
        if (ret.storcli_read_ret_no_data_t_u.error == ENOENT) {
	  errno = 0;
	}  
	else {
          errno = ret.storcli_read_ret_no_data_t_u.error;
	}  
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;	
    }
    /*
    ** no error, so get the length of the data part: need to consider the case
    ** of the shared memory since for its case, the length of the returned data
    ** is not in the rpc buffer. It is detected by th epresence of the 0x53535353
    ** pattern in the alignment field of the rpc buffer
    */
    int received_len = ret.storcli_read_ret_no_data_t_u.len.len;
    uint32_t alignment = (uint32_t) ret.storcli_read_ret_no_data_t_u.len.alignment;
    xdr_free((xdrproc_t) decode_proc, (char *) &ret); 

    if (alignment == 0x53535353)
    {
      /*
      ** case of the shared memory
      */
      uint32_t *p32 = (uint32_t*)ruc_buf_getPayload(shared_buf_ref);;
      received_len = p32[1];
      position = 0;
      payload = (uint8_t*)&p32[4096/4];
    }
    else
    { 
      /*
      ** case without shared memory
      */
      position = XDR_GETPOS(&xdrs);
    }
    /*
    ** Update the bandwidth statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_READ_THR],(uint64_t)received_len);
    /*
    ** check the length: caution, the received length can
    ** be zero by it might be possible that the information 
    ** is in the pending write section of the buffer
    */
    if ((received_len == 0)  || (ie->attrs.attrs.size == 0))
    {
      /*
      ** end of filenext_read_pos
      */
      errno = 0;
      goto error;   
    }
    /*
    ** Get off requested to storcli (equal off after alignment)
    */
    next_read_from = (off/bbytes)*bbytes;
    /*
    ** Truncate the received length to the known EOF as stored in
    ** the file context
    */
    if ((next_read_from + received_len) > ie->attrs.attrs.size)
    {
       received_len = ie->attrs.attrs.size - next_read_from;    
    }
    /*
    ** re-evaluate the EOF case
    */
    if (received_len <= 0)
    {
        int recv_len_ok = 0;

        if (received_len < 0) {
            ientry_t *ie2 = 0;
            uint64_t file_size;
            uint32_t *p32 = (uint32_t*) ruc_buf_getPayload(shared_buf_ref);
            int received_len_orig = p32[1];
            ie2 = get_ientry_by_fid(ie->attrs.attrs.fid);
            if ((ie2 == NULL)) {
                file_size = 0;
            } else {
                file_size = ie2->attrs.attrs.size;
            }
#if 0	    
            severe("BUGROZOFSWATCH(%p) , received_len=%d,"
                    " next_read_from=%"PRIu64", file->attrs.size=%"PRIu64","
                    " received_len_orig=%d,readahead=%d," " ie->attrs.attrs.size=%"PRIu64"",
                    file, received_len, next_read_from, ie2->attrs.attrs.size,
                    received_len_orig, readahead, file_size);
#endif
            received_len = received_len_orig;
            if ((next_read_from + received_len) > file_size) {
                received_len = file_size - next_read_from;
            }
            if (received_len > 0)
                recv_len_ok = 1;
        }

      /*
      ** end of filenext_read_pos
      */
      if (recv_len_ok == 0)
      {
	errno = 0;
	goto error; 
      }  
    }    
    
    next_read_pos  = next_read_from+(uint64_t)received_len; 
#ifdef TRACE_FS_READ_WRITE
      info("FUSE READ_CBK read_rq[%llx:%llx],read_rcv[%llx:%llx],wrb%d[%llx:%llx],rdb[%llx:%llx]",
            (long long unsigned int)off,(long long unsigned int)(off+size),
            (long long unsigned int)next_read_from,(long long unsigned int)(next_read_pos),
            file->buf_write_wait,
            (long long unsigned int)file->write_from,(long long unsigned int)file->write_pos,
            (long long unsigned int)file->read_from,(long long unsigned int)file->read_pos);     
#endif
    /*
    ** check if the received length is greater than the max buffer size used for storing
    ** data in the file_t structure
    **
    **  When returned size is greater than the max buffer size, rozofsmount does not store
    **  it in the file_t structure.
    **  The data are returned to the caller thanks the fuse_reply_threads.
    **
    **  If there are some write data pending, it is not an issue since the read request has
    **  been sent before the write, so returning some "old" data in that situation is not 
    **  an error
    **  Since we proceed with a direct reply, the read_pos and read_from pointers MUST not
    **  be changed since the data in the buffer will not reflect that data corresponding
    **  to these read_from/read_pos offsets.
    **
    **  When the read request has been triggered by an internal readahead, the max read size
    **  is equal to the max buffer size. In that case, we MUST not do a direct reply since
    **  the buffer has to be install in the allocated buffer of the file_t structure.
    **  That makes the data available for a next read.
    */
    if ((readahead == 0) && (received_len > ROZOFS_MAX_FILE_BUF_SZ))
    {   
        /*
	**______________________________________________
	**  Big read case (>=256KB)
	**______________________________________________
	*/
        uint8_t *src_p = payload+position;
        length = 0;
	/*
	** use local read_from & read_pos to avoid modifying the one of the file_t structure
	*/
        uint64_t read_pos; 
        uint64_t read_from; 
        /*
        ** preset the data
        */
        file->buf_read_wait = 0;
        read_from =(off/bbytes)*bbytes;
        read_pos  = read_from+(uint64_t)received_len;
        buff =(char*)( src_p + (off - read_from));

        /*
        ** User has requested some data: give it back its requested data by avoiding
        ** a copy in the fd's buffer.
        */     
        if (off < read_pos)
        {
          length = (size <=(read_pos - off)) ? size :(read_pos - off);
        }
       /*
       ** process with a direct reply to the application within the fuse_reply threads
       */
       void *save_shared_buf_ref = shared_buf_ref;
       if (shared_buf_ref!= NULL) 
       {
	 uint32_t *p32 = (uint32_t*)ruc_buf_getPayload(shared_buf_ref);    
	 /*
	 ** clear the timestamp
	 */
	 *p32 = 0;
	 shared_buf_ref = NULL;
	 SAVE_FUSE_PARAM(param,shared_buf_ref); 
	 shared_buf_ref = save_shared_buf_ref; 
       }
      /*
      ** do not update the shared buffer counter since it will be released just
      ** after the reply read thread processing
      */
      update_pending_buffer_todo = 0;
      rozofs_thread_fuse_reply_buf(req, (char *) buff, length,shared_buf_ref,0); 
      /*
      ** update the current position in the file
      */
      file->current_pos = (off+length);
      goto out;            
    }
    /*
    ** __________________________________________________
    ** Some data may not yet be saved on disk : flush it
    ** __________________________________________________
    */
    if (file->buf_write_wait) 
    { 
      while(1)
      {
        /*
        ** first of all: Put the data in the cache
        */
        src_p = payload+position;
        if (received_len != 0)
        {
          rozofs_mbcache_insert(file->fid,next_read_from,(uint32_t)received_len,(uint8_t*)src_p);
        } 

      /**
      *  wr_f                wr_p
      *   +-------------------+
      *                       +-------------------+
      *                      nxt_rd_f           nxt_rd_p
      *
      *  Flush the write pending data on disk
      *  Install the received read buffer instead
      */
      if (file->write_pos <= next_read_from)
      {
        ROZOFS_WRITE_MERGE_STATS(RZ_FUSE_WRITE_0);
        rozofs_asynchronous_flush(fi);
        file->write_pos  = 0;
        file->write_from = 0;
        /*
        ** copy the received buffer in the file descriptor context
        */
        src_p = payload+position;
        dst_p = (uint8_t*)file->buffer;
        rozofs_write_in_buffer(file,dst_p,src_p,received_len);
        file->buf_read_wait = 0;
        file->read_from = next_read_from;
        file->read_pos  = next_read_pos;	
        break;      
      }
      /**
      *  wr_f                wr_p
      *   +-------------------+
      *                   +-------------------+
      *                 nxt_rd_f           nxt_rd_p
      *
      *  Flush the write pending data on disk
      *  Install the received read buffer instead
      */
      if (((file->write_from <= next_read_from) && (next_read_from <= file->write_pos)) &&
         (next_read_pos > file->write_pos))
      {
        ROZOFS_WRITE_MERGE_STATS(RZ_FUSE_WRITE_1);
        /*
        ** possible optimization: extend the write pos to a User Data Blcok Boundary
        */
        rozofs_asynchronous_flush(fi);
        /*
        ** save in buf flush thesection between read_from and write_pos
        */
        len = file->write_pos - next_read_from;  
        offset_buf_wr_start =  file->write_from - file->read_from ; 
        offset_buf_wr_start +=  (next_read_from- file->write_from);
        src_p =(uint8_t *)( file->buffer + offset_buf_wr_start);
        memcpy(local_buf_flush,src_p,len);   
        file->write_pos  = 0;
        file->write_from = 0;
        /*
        ** copy the received buffer in the file descriptor context
        */
        src_p = payload+position;
        dst_p = (uint8_t*)file->buffer;
        rozofs_write_in_buffer(file,dst_p,src_p,received_len);
        file->buf_read_wait = 0;
        /**
        * merge the with the buf flush
        */
        rozofs_write_in_buffer(file,file->buffer,local_buf_flush,len); 
          
        file->read_from = next_read_from;
        file->read_pos  = next_read_pos;	             
        break;      
      }      
      /**
      *  wr_f                                       wr_p
      *   +------------------------------------------+
      *                   +-------------------+
      *                 nxt_rd_f           nxt_rd_p
      *
      *  Discard the received data since it is included in the write pending data
      */
      if (
         ((file->write_from <= next_read_from) && (next_read_from <= file->write_pos)) && 
         ((file->write_from <= next_read_pos) && (next_read_pos <= file->write_pos)))
      {
        ROZOFS_WRITE_MERGE_STATS(RZ_FUSE_WRITE_2);
   
        break;      
      }      

      /**
      *    wr_f                  wr_p
      *      +-------------------+
      *   +-------------------------+
      *   nxt_rd_f           nxt_rd_p
      *
      *  The read and write buffer must be merged
      *  keep the write data without flushing them
      */
      if (
         ((next_read_from <=file->write_from  ) && (file->write_from <= next_read_pos )) && 
         ((next_read_from <=file->write_pos  ) && (file->write_pos <= next_read_pos )))
      {
        ROZOFS_WRITE_MERGE_STATS(RZ_FUSE_WRITE_3);
        /*
        ** save in buf flush the section between write_from and write_pos
        */
        len = file->write_pos - file->write_from;      
        src_p =(uint8_t *)( file->buffer + (file->write_from- file->read_from));
        memcpy(local_buf_flush,src_p,len);   
        /*
        ** copy the received buffer in the file descriptor context
        */
        src_p = payload+position;
        dst_p = (uint8_t*)file->buffer;
        rozofs_write_in_buffer(file,dst_p,src_p,received_len);
        file->buf_read_wait = 0;
        /**
        * merge the with the buf flush
        */
        dst_p = (uint8_t*)(file->buffer + (file->write_from - next_read_from));
        rozofs_write_in_buffer(file,dst_p,local_buf_flush,len); 
          
        file->read_from = next_read_from;
        file->read_pos  = next_read_pos;	  
        break;      
      }            
      /** 4
      *    wr_f                        wr_p
      *      +---------------------------+
      *   +-------------------+
      *   nxt_rd_f         nxt_rd_p
      *
      *  
      */
      if (
         ((next_read_from <=file->write_from  ) && (file->write_from <= next_read_pos )) && 
          (file->write_pos > next_read_pos ))
      { 
        /** 4.1
        *    wr_f                        wr_p
        *      +---------------------------+
        *   +-------------------+
        *   nxt_rd_f         nxt_rd_p
        *   +----------------------------------+  BUFSIZE
        *  
        *  Merge the write and read in the file descriptor buffer, do not flush
        *  
        */
        if ( file->write_pos -next_read_from <= file->export->bufsize)
        {      
          ROZOFS_WRITE_MERGE_STATS(RZ_FUSE_WRITE_4);
          /*
          ** save in buf flush the section between write_from and write_pos
          */
          len = file->write_pos - file->write_from;      
          src_p =(uint8_t *)( file->buffer + (file->write_from- file->read_from));
          memcpy(local_buf_flush,src_p,len);   
          /*
          ** copy the received buffer in the file descriptor context
          */
          src_p = payload+position;
          dst_p = (uint8_t*)file->buffer;
          rozofs_write_in_buffer(file,dst_p,src_p,received_len);
          file->buf_read_wait = 0;
          /**
          * merge the with the buf flush
          */
          dst_p = (uint8_t*)(file->buffer + (file->write_from - next_read_from));
          rozofs_write_in_buffer(file,dst_p,local_buf_flush,len); 

          file->read_from = next_read_from;
          file->read_pos  = file->write_pos;	  
          break;      
        }       
        /** 4.2
        *    wr_f                        wr_p
        *      +---------------------------+
        *   +-------------------+
        *   nxt_rd_f         nxt_rd_p
        *   +---------------------------+  BUFSIZE
        *  
        *  Flush the write data on disk
        *  Merge the remaining (read and write) in the file descriptor buffer
        *  
        */
        ROZOFS_WRITE_MERGE_STATS(RZ_FUSE_WRITE_5);
        rozofs_asynchronous_flush(fi);        
        /*
        ** save in buf flush the section between write_from and write_pos
        */
        offset_end = next_read_from + file->export->bufsize;
        
        len = offset_end - file->write_from;      
        src_p =(uint8_t *)( file->buffer + (file->write_from- file->read_from));
        memcpy(local_buf_flush,src_p,len);   
        /*
        ** copy the received buffer in the file descriptor context
        */
        src_p = payload+position;
        dst_p = (uint8_t*)file->buffer;
        rozofs_write_in_buffer(file,dst_p,src_p,received_len);
        file->buf_read_wait = 0;
        /**
        * merge the with the buf flush
        */
        dst_p = (uint8_t*)(file->buffer + (file->write_from - next_read_from));
        rozofs_write_in_buffer(file,dst_p,local_buf_flush,len); 

        file->write_pos  = 0;
        file->write_from = 0;
        file->read_from = next_read_from;
        file->read_pos  = offset_end;	  
        break;         
      }      
      /**
      *                               wr_f                  wr_p
      *                                 +-------------------+
      *   +-------------------------+
      *   nxt_rd_f           nxt_rd_p
      *
      */
      if (next_read_pos <=file->write_from)
      {
        /**
        *                                  wr_f                  wr_p
        *                                   +-------------------+
        *   +----------------------+
        *   nxt_rd_f           nxt_rd_p
        *   +---------------------------+  BUFSIZE
        *
        *  Flush the write data
        *  copy the received data into the file descriptor buffer
        *
        */
        if (file->write_from >= (next_read_from + file->export->bufsize))
        {
          ROZOFS_WRITE_MERGE_STATS(RZ_FUSE_WRITE_6);
          rozofs_asynchronous_flush(fi);        
          /*
          ** copy the received buffer in the file descriptor context
          */
          src_p = payload+position;
          dst_p = (uint8_t*)file->buffer;
          rozofs_write_in_buffer(file,dst_p,src_p,received_len);
          file->buf_read_wait = 0;

          file->write_pos  = 0;
          file->write_from = 0;
          file->read_from = next_read_from;
          file->read_pos  = next_read_pos;	  
          break;  
        } 
        /**
        *                          wr_f                  wr_p
        *                           +-------------------+
        *   +------------------+
        *   nxt_rd_f         nxt_rd_p
        *   +---------------------------+  BUFSIZE
        *
        *  Flush the write data
        *  and install read data in the buffer
        *
        */
        if (file->write_pos - next_read_from > file->export->bufsize)
        {
          ROZOFS_WRITE_MERGE_STATS(RZ_FUSE_WRITE_7);
          rozofs_asynchronous_flush(fi);        
          /*
          ** copy the received buffer in the file descriptor context
          */
          src_p = payload+position;
          dst_p = (uint8_t*)file->buffer;
          rozofs_write_in_buffer(file,dst_p,src_p,received_len);
          file->buf_read_wait = 0;

          file->write_pos  = 0;
          file->write_from = 0;
          file->read_from = next_read_from;
          file->read_pos  = next_read_pos;	  
          break; 
        }         
        /**
        *                          wr_f                  wr_p
        *                           +-------------------+
        *   +------------------+
        *   nxt_rd_f         nxt_rd_p
        *   +-----------------------------------------------+  BUFSIZE
        *
        *  Flush the write data
        *  and install read data in the buffer
        */
        /*
        ** save in buf flush the section between write_from and write_pos
        */
        ROZOFS_WRITE_MERGE_STATS(RZ_FUSE_WRITE_8);
          rozofs_asynchronous_flush(fi);        
          /*
          ** copy the received buffer in the file descriptor context
          */
          src_p = payload+position;
          dst_p = (uint8_t*)file->buffer;
          rozofs_write_in_buffer(file,dst_p,src_p,received_len);
          file->buf_read_wait = 0;

          file->write_pos  = 0;
          file->write_from = 0;
          file->read_from = next_read_from;
          file->read_pos  = next_read_pos;	  
	  
        break;   
      }   
      ROZOFS_WRITE_MERGE_STATS(RZ_FUSE_WRITE_9);
      severe("Something Rotten in the Rozfs Kingdom %llu %llu %llu %llu",
             (long long unsigned int)next_read_from,(long long unsigned int)next_read_pos,
             (long long unsigned int)file->write_from,(long long unsigned int)file->write_pos);
      break;
      }
      /*
      * compute the length that will be returned to fuse
      */
      if (off >= file->read_pos)
      {
        /*
        ** end of file
        */
        length = 0;
        buff = file->buffer;
      }
      else
      {
        length = (size <=(file->read_pos - off)) ? size :(file->read_pos - off);
        buff = file->buffer + (off - file->read_from);
      }
    }
    else
    /**
    ** __________________________________________________
    *  Normal case of the sequential read (no pending write)
    ** __________________________________________________
    */
    {
      /*
      ** check the case of the readahead: for readahead, we just copy the data
      ** into the fd's buffer.
      */
      while(1)
      {
        uint8_t *src_p = payload+position;
        uint8_t *dst_p = (uint8_t*)file->buffer;
        length = 0;
        /*
        ** preset the data
        */

        file->buf_read_wait = 0;
        file->read_from = (off/bbytes)*bbytes;
        file->read_pos  = file->read_from+(uint64_t)received_len;
        buff =(char*)( src_p + (off - file->read_from));
            
        if (readahead == 1)
        {
	  /*
	  **___________________________________________
	  ** Readahead case
	  **___________________________________________
	  */
          /*
          ** set the pointer to the beginning of the data array on the rpc buffer
          */
          /*
          ** Put the data in the cache
          */
          if (received_len != 0)
          {
            rozofs_mbcache_insert(file->fid,file->read_from,(uint32_t)received_len,(uint8_t*)src_p);
          } 
          /*
          ** when the cache is enable the memcpy is useless: may we must avoid updating
          ** the read_from and read_pos in the file structure, and just copy the data in the cache
          ** by this way we can avoid the extra memcpy at that time
          */   
          rozofs_write_in_buffer(file,dst_p,src_p,received_len);

          goto out;
        }
	/*
	**__________________________________________________
	** Normal case where user has called for some data
	**__________________________________________________
        */	
        /*
        ** User has request some data: give it back its requested data by avoiding
        ** a copy in the fd's buffer, just the un-read part is filled in.
        */     
        if (off < file->read_pos)
        {
          length = (size <=(file->read_pos - off)) ? size :(file->read_pos - off);
        }
        /*
        ** provide fuse with the request data
        */
#if 1
	if (length >=  ROZOFS_MAX_FILE_BUF_SZ)
	{
	  /*
	  ** use the response thread
	  */
	   void *save_shared_buf_ref = shared_buf_ref;
	   if (shared_buf_ref!= NULL) 
	   {
	     uint32_t *p32 = (uint32_t*)ruc_buf_getPayload(shared_buf_ref);    
	     /*
	     ** clear the timestamp
	     */
	     *p32 = 0;
	     shared_buf_ref = NULL;
	     SAVE_FUSE_PARAM(param,shared_buf_ref); 
	     shared_buf_ref = save_shared_buf_ref; 
	   }
	  /*
	  ** do not update the shared buffer counter since it will be released just
	  ** after the reply read thread processing
	  */
          update_pending_buffer_todo = 0;
	  rozofs_thread_fuse_reply_buf(req, (char *) buff, length,shared_buf_ref,0);
	}
	else
#endif
	{
          char *buf_sharem = buff;

          fuse_reply_buf(req, (char *) buf_sharem, length);
	}
        file->current_pos = (off+length);
        /*
        ** Put the data in the cache
        */
        if (received_len != 0)
        {
          rozofs_mbcache_insert(file->fid,file->read_from,(uint32_t)received_len,(uint8_t*)src_p);
        }       
        /*
        ** copy the remaining data in the fd's buffer by taking into accuting the block size
        ** alignment
        */
        off_t new_offset = off + length;
        uint64_t new_read_from = (new_offset/bbytes)*bbytes;
        src_p += new_read_from - file->read_from;
        length = file->read_pos - new_read_from;
        rozofs_write_in_buffer(file,dst_p,src_p,length);
        file->read_from = new_read_from;                
        goto out;        
      }

    }
    /*
    ** check readhead case : do not return the data since fuse did not request anything
    */
    if (readahead == 0)
    {
      fuse_reply_buf(req, (char *) buff, length);
#ifdef TRACE_FS_READ_WRITE
      info("FUSE READ_CBK_OUT ,read_rcv[%llx:%llx],wrb%d[%llx:%llx],rdb[%llx:%llx]",
            (long long unsigned int)off,(long long unsigned int)(off+length),
            file->buf_write_wait,
            (long long unsigned int)file->write_from,(long long unsigned int)file->write_pos,
            (long long unsigned int)file->read_from,(long long unsigned int)file->read_pos);     
#endif
    }    
    goto out;
error:
    if (readahead == 0)
    {
      fuse_reply_err(req, errno);
    }
    /*
    **__________________________
    ** Common exit
    **__________________________
    */
out:
    if (update_pending_buffer_todo)
    {
      /*
      ** update the number of storcli pending request
      */
      if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;
    }
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_read,(fuse_ino_t)file/*ino*/,file->fid,(errno==0)?0:1,trc_idx);
    if (readahead == 0)
    {
       STOP_PROFILING_NB(param,rozofs_ll_read);
    }
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    /*
    ** Check if there is some other read request that are pending
    */
    {
      void *buffer_p = fuse_ctx_read_pending_queue_get(file);
      if (buffer_p  != NULL) return  rozofs_ll_read_defer(buffer_p) ;
    }
    /*
    ** since it operates in async mode, we might defer the release of the fd
    ** so we need to figure out if closing has been request and call a file_close()
    ** in that case. However the close might effective since there is some other
    ** requests that are pending (here it is pending write).
    */
    if (rozofs_is_file_closing(file))
    {
       file_close(file);
    }
    return;
}



/**
*  data read deferred

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param parent : inode parent provided by rozofsmount
 @param name : name to search in the parent directory
 
 @retval none
*/

int rozofs_ll_read_defer_first(void *param,uint8_t *buffer,int received_len,void *shared_buf_ref) 
{
   fuse_req_t req; 
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   int ret;
   size_t size;
   uint64_t off;   
   file_t *file;
   uint32_t readahead ;
   int trc_idx;

   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,size);
   RESTORE_FUSE_PARAM(param,off);
   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_STRUCT(param,fi,sizeof( struct fuse_file_info));    

   file = (file_t *) (unsigned long) fi->fh; 
   /*
   ** check if the offset matches with the one of the reqyest
   */
   if (off != file->read_from)
   {
      /*
      ** we don't care
      */
      return 0;
   }  
   /*
   ** it matches the requested off, so copy the buffer
   */
   fuse_reply_buf(req, (char *) buffer, received_len);
   rozofs_trc_rsp(srv_rozofs_ll_read,0/*ino*/,file->fid,(errno==0)?0:1,trc_idx);
   STOP_PROFILING_NB(param,rozofs_ll_read);
   /*
   ** remove the context from the pending read queue
   */
   fuse_ctx_read_pending_queue_get(file);
   /*
   ** indicates that the buffer is empty
   */
   file->read_from = file->read_pos;
   /*
   ** check if there is some read pending, it that case we just have to 
   ** leave
   */
   if (file->buf_read_pending > 0)
   {
     /*
     ** remove the context from the link list
     ** and release the context
     */
     rozofs_fuse_release_saved_context(param);     
     return 1;
   }
   /*
   ** there is no pending read, so attempt a readahead
   */
   readahead =1;
   rozofs_fuse_read_write_stats_buf.readahead_cpt++;       
   off = file->read_pos;
   size = file->export->bufsize;
   SAVE_FUSE_PARAM(param,readahead);
   SAVE_FUSE_PARAM(param,off);
   SAVE_FUSE_PARAM(param,size); 
   if (shared_buf_ref!= NULL) 
   {
     uint32_t *p32 = (uint32_t*)ruc_buf_getPayload(shared_buf_ref);    
     /*
     ** clear the timestamp
     */
     *p32 = 0;
     ruc_buf_freeBuffer(shared_buf_ref);
     shared_buf_ref = NULL;
     SAVE_FUSE_PARAM(param,shared_buf_ref);   
   }
   /*
   ** attempt to read
   */  
   trc_idx = rozofs_trc_req_io(srv_rozofs_ll_read,0/*ino*/,file->fid,size,off);          
   SAVE_FUSE_PARAM(param,trc_idx); 
   ret = read_buf_nb(param,file,off, file->buffer, size);      
   if (ret < 0)
   {
      /*
      ** read error --> release the context
      */
      rozofs_trc_rsp(srv_rozofs_ll_read,0/* ino */,file->fid,(errno==0)?0:1,trc_idx);
      rozofs_fuse_release_saved_context(param);
   } 
   return 2;  
}
