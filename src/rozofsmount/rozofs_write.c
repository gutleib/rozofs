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

#include <rozofs/rpc/eproto.h>
#include <rozofs/rpc/storcli_proto.h>

#include "rozofs_fuse_api.h"
#include "rozofs_modeblock_cache.h"
#include "rozofs_rw_load_balancing.h"
#include "rozofs_sharedmem.h"
#include "rozofs_kpi.h"
DECLARE_PROFILING(mpp_profiler_t);

void export_write_block_nb(void *fuse_ctx_p, file_t *file_p);
void export_write_block_error_nb(void *fuse_ctx_p, file_t *file_p);
char *rozofs_buf_scratch_p=NULL;
static int64_t write_buf_nb(void *buffer_p,file_t * f, uint64_t off, const char *buf, uint32_t len);

void rozofs_ll_write_cbk(void *this,void *param);
void export_write_block_cbk(void *this,void *param);

/*
** write alignment statistics
*/
uint64_t    rozofs_write_buf_flush = 0;
uint64_t    rozofs_aligned_write_start[2];
uint64_t    rozofs_aligned_write_end[2];



int ROZOFS_MAX_WRITE_PENDING = 1;
typedef struct _WRITE_FLUSH_STAT_T {
  uint64_t non_synchroneous;
  uint64_t synchroneous;
  uint64_t synchroneous_success;
  uint64_t synchroneous_error;
} WRITE_FLUSH_STAT_T;

static WRITE_FLUSH_STAT_T write_flush_stat;
/*
**__________________________________________________________________
*/
/**
   Reset write synch stats
*/
static inline void reset_write_flush_stat(void) {
  memset(&write_flush_stat,0,sizeof(write_flush_stat));
}
/*
**__________________________________________________________________
*/
/**
   Display write synch stats
*/
static char * display_write_flush_stat_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"write_flush set <value> : set new write pending maximum value\n");
  pChar += sprintf(pChar,"write_flush reset       : reset statistics\n");
  pChar += sprintf(pChar,"write_flush             : display statistics\n");  
  return pChar; 
}
#define SHOW_STAT_WRITE_FLUSH(name) pChar += sprintf(pChar,"%-26s :  %10llu\n","  "#name ,(long long unsigned int) write_flush_stat.name);
static void display_write_flush_stat(char * argv[], uint32_t tcpRef, void *bufRef){
  char *pChar = uma_dbg_get_buffer();
  int   new_val;

  if (argv[1] != NULL) {
    if (strcmp(argv[1],"reset")==0) {
      reset_write_flush_stat();
      uma_dbg_send(tcpRef, bufRef, TRUE, "Reset done\n");  
      return;         
    }
    if (strcmp(argv[1],"set")==0) {
      if (argv[2] == NULL) {
        uma_dbg_send_format(tcpRef, bufRef, TRUE, "Value required\n"); 
        return;       
      }
      errno = 0;       
      new_val = (int) strtol(argv[2], (char **) NULL, 10);   
      if (errno != 0) {
        pChar += sprintf(pChar, "bad value %s\n",argv[2]);
        pChar = display_write_flush_stat_help(pChar);
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
        return;
      } 
      ROZOFS_MAX_WRITE_PENDING = new_val;
      reset_write_flush_stat();
      uma_dbg_send_format(tcpRef, bufRef, TRUE, "New write pending maximum value is %d\n",ROZOFS_MAX_WRITE_PENDING); 
      return;   
    } 
    /*
    ** Help
    */
    pChar = display_write_flush_stat_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;
  }
  pChar += sprintf(pChar,"Write pending maximum value is %d\n",ROZOFS_MAX_WRITE_PENDING);
  SHOW_STAT_WRITE_FLUSH(non_synchroneous);
  SHOW_STAT_WRITE_FLUSH(synchroneous);
  SHOW_STAT_WRITE_FLUSH(synchroneous_success);
  SHOW_STAT_WRITE_FLUSH(synchroneous_error);    
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());  
}  

/*
**__________________________________________________________________
*/
/**
   Bugwatch flag set/reset
*/
int rozofs_bugwatch = 0;

static char * bugwatch_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"bugwatch enable  : set bugwatch_flag\n");
  pChar += sprintf(pChar,"bugwatch disable : reset bugwatch flag\n");
  pChar += sprintf(pChar,"bugwatch         : display current state\n");  
  return pChar; 
}


static void bugwatch_proc(char * argv[], uint32_t tcpRef, void *bufRef){
  char *pChar = uma_dbg_get_buffer();

  if (argv[1] != NULL) {
    if (strcmp(argv[1],"enable")==0) {
      rozofs_bugwatch = 1;
      uma_dbg_send(tcpRef, bufRef, TRUE, "bugwatch enabled\n");  
      return;         
    }
    if (strcmp(argv[1],"disable")==0) {
      rozofs_bugwatch = 1;
      uma_dbg_send(tcpRef, bufRef, TRUE, "bugwatch disabled\n");   
      return;     
    }   
    /*
    ** Help
    */
    pChar = bugwatch_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;
  }
  uma_dbg_send_format(tcpRef, bufRef, TRUE, "BUGROZOFSWATCH is %s\n",(rozofs_bugwatch==0)?"disabled":"enabled"); 
}  
/*
**__________________________________________________________________
*/
/**
   Initialize write synch stat service
*/
void init_write_flush_stat(int max_write_pending){
  ROZOFS_MAX_WRITE_PENDING = max_write_pending;
  reset_write_flush_stat();
  uma_dbg_addTopic_option("write_flush", display_write_flush_stat,UMA_DBG_OPTION_RESET);  
  uma_dbg_addTopic("bugwatch", bugwatch_proc);  
}
 

/*
**__________________________________________________________________
*/
/**
   API to clear the buffer after a flush
   If some data is pending in the buffer the clear is not done
   
   @param *f : pointer to the file structure where read buffer information can be retrieved
   
   @retval -1 some data to write is pending
   @retval 0 if the read buffer is not empty
*/
int clear_read_data(file_t *f)
{
  f->read_from = f->read_pos = 0;
  return 0;
}
/*
**__________________________________________________________________
*/
/**
   API to check if the read section is empty
   
   @param *p : pointer to the file structure where read buffer information can be retrieved
   
   @retval 1 if the read buffer is empty
   @retval 0 if the read buffer is not empty
*/
static inline int read_section_empty(file_t *p)
{
   return (p->read_from == p->read_pos)?1:0;
}

/*
**__________________________________________________________________
*/
/**
   API to check if the write section is empty
   
   @param *p : pointer to the file structure where write buffer information can be retrieved
   
   @retval 1 if the write buffer is empty
   @retval 0 if the write buffer is not empty
*/
static inline int write_section_empty(file_t *p)
{
   return (p->write_from == p->write_pos)?1:0;
}

/*
**__________________________________________________________________
*/
/**
*  flush the content of the buffer to the disk

  @param fuse_ctx_p: pointer to the fuse transaction context
  @param p : pointer to the file structure that contains buffer information
  
  @retval len_write >= total data length push to the disk
  @retval < 0 --> error while attempting to initiate a write request towards storcli 
*/
int buf_flush(void *fuse_ctx_p,file_t *p)
{
//  if (p->buf_write_wait == 0) return 0;
  
  uint64_t flush_off = p->write_from;
  uint32_t flush_len = (uint32_t)(p->write_pos - p->write_from);
  uint32_t flush_off_buf = (uint32_t)(p->write_from - p->read_from);
  char *buffer;
  ientry_t *ie;
  /*
  ** stats
  */
  rozofs_fuse_read_write_stats_buf.flush_buf_cpt++;
  
  buffer = p->buffer+flush_off_buf;
  /*
  ** Push the data in the cache
  */
  rozofs_mbcache_insert(p->fid,flush_off,(uint32_t)flush_len,(uint8_t*)buffer);  
  /*
  ** trigger the write
  */
  if ((write_buf_nb(fuse_ctx_p,p, flush_off, buffer, flush_len)) < 0) {
    return -1;
  }
  /*
  ** since the buffer has been pushed on disk, clean the write variables
  */
  p->buf_write_wait = 0;
  p->write_from     = 0;
  p->write_pos      = 0;  
  ie = (ientry_t *) p->ie;
  if (ie->write_pending == p) ie->write_pending = NULL;
  return 0;


}
/*
**__________________________________________________________________
*/
/**
*  unqueue the next pending requests waiting on the file descriptor
   and execute them

  @param file  The file descriptor
  
*/
void check_last_write_pending(file_t *file) {
  fuse_end_tx_recv_pf_t   callback;
  void                  * pending_fuse_ctx_p;
  
  /*
  ** Check if there some request (flush or release waiting fro the last write to take place
  */
  while (file->buf_write_pending == 0) {
  

    pending_fuse_ctx_p = fuse_ctx_write_pending_queue_get(file);
    if (pending_fuse_ctx_p == NULL) break;
    
    GET_FUSE_CALLBACK(pending_fuse_ctx_p,callback);
    (*callback)(NULL,pending_fuse_ctx_p);
  }
}    
/*
**__________________________________________________________________
*/
/**
*  API to align the write_from pointer to a ROZOFS_BSIZE boundary

  @param p : pointer to the file structure that contains buffer information
  
  @retval none
*/
static inline void buf_align_write_from(file_t *p)
{
   uint64_t off2start_w;
   uint64_t off2start;
   uint32_t bbytes = ROZOFS_BSIZE_BYTES(exportclt.bsize);

   off2start_w = (p->write_from/bbytes);
   off2start = off2start_w*bbytes;
   if ( off2start >= p->read_from)
   {
     p->write_from = off2start;
   }      
}
/*
**__________________________________________________________________
*/
/**
*  API to align the write_pos pointer to a ROZOFS_BSIZE boundary
  it is assumed that write_pos,write_from, read_from and read_pos are already computed
  
  @param p : pointer to the file structure that contains buffer information
  
  @retval none
*/
static inline void buf_align_write_pos(file_t *p)
{        
   uint64_t off2end;
   uint32_t bbytes = ROZOFS_BSIZE_BYTES(exportclt.bsize);

   /*
   ** attempt to align write_pos on a block boundary
   */
   if (p->write_pos%bbytes)
   {
      /*
      ** not on a block boundary
      */
      off2end = ((p->write_pos/bbytes)+1)*bbytes;
      if (off2end <= p->read_pos)
      {
        /*
        ** align on block boundary
        */
        p->write_pos = off2end;           
      }
   }
}
/*
**__________________________________________________________________
*/
/**
*  API to write data towards disks in asynchronous mode
  
  The asynchronous write is triggered for the following case
    - the data to write does not fit in the current buffer-> a previous write is pending and a write
      is trigger for that pending write while the new data are inserted in the cache buffer associated
      with the file_t structure.
    - the data to write fits partially in the buffer. SO we ned to flush the data that are been filled
      at the end of the current buffer before copying the remaining data in the buffer

  @param ie: ientry of the file
  @param fuse_ctx_p: pointer to the fuse transaction context
  @param p : pointer to the file structure that contains buffer information
  @param off : first byte to write
  @param len : data write length
  @param buf : user buffer where write data are found
  @param status_p: pointer to the reported status
     BUF_STATUS_DONE: the data has be written in the cache buffer without triggered any write to disk
     BUF_STATUS_WR_IN_PRG: the data has be written in the cache buffer and a write request is in progress (async)
     BUF_STATUS_FAILURE : error while writing data (see errcode in structure rozo_buf_rw_status_t)
      
  @retval none
*/
void buf_file_write_nb(ientry_t * ie,
                    void *fuse_ctx_p,
                    rozo_buf_rw_status_t *status_p,
                    file_t * p,
                    uint64_t off, 
                    const char *buf, 
                    uint32_t len,
		    int creating_hole) 
{ 

  uint64_t off_requested = off;
  uint64_t pos_requested = off+len;
  int state;
  int action;
  uint64_t off2end;
  uint32_t buf_off;
  int ret;
  char *dest_p;
  char *src_p;
  uint32_t len_alignment;

  /*
  ** Check whether some pending data written by an other application
  ** on the same file is pending. In that case flush it immediatly.
  */
  if (ie->write_pending != p) {
    flush_write_ientry(ie);  
  }

  /*
  ** Write is bigger than the buffer size.
  ** Flush the buffer if needed and reset buffer pointers
  ** so the buffer is empty. 
  ** => action BUF_ACT_COPY_EMPTY will be called
  */
  if (len >= ROZOFS_MAX_FILE_BUF_SZ)
  {
    flush_write_ientry(ie);
    p->read_from = p->read_pos = 0;    
  }
  
      
  /*
  ** Check whether the buffer content is valid or if it must be forgotten
  */
  if (p->read_consistency != ie->read_consistency) {
    /* The file has been modified since this buffer has been read. The data
    ** it contains are questionable. Better forget them.
    */
    p->read_from = p->read_pos = 0;
  }
    
  ie->read_consistency++;
  p->read_consistency = ie->read_consistency;
  

 status_p->status = BUF_STATUS_FAILURE;
 status_p->errcode = EINVAL ;
  /*
  ** check the start point 
  */
  while(1)
  {
    if (!read_section_empty(p))
    {
      /*
      ** we have either a read or write section, need to check read section first
      */
      if (off_requested >= p->read_from) 
      {
         if (off_requested <= p->read_pos)
         {
           state = BUF_ST_READ_INSIDE;  
           break;   
         } 
         state = BUF_ST_READ_AFTER;
         break;    
      }
      /*
      ** the write start off is out side of the cache buffer
      */
      state = BUF_ST_READ_BEFORE;
      break;
    }
    /*
    ** the buffer is empty (neither read nor write occurs
    */
    state = BUF_ST_EMPTY;
    break;
  }
  
  /*
  ** Those data will create a hole in the file. 
  ** Let's make the flush of these data exclusive in the STORCLI.
  */
  if (creating_hole) p->file2create = 1;
  /*
  ** ok, now check the end
  */
//  printf("state %s\n",rozofs_buf_read_write_state_e2String(state));
  action = -1;
  switch (state)
  {
    case BUF_ST_EMPTY:
      action = BUF_ACT_COPY_EMPTY;
      break;
    case BUF_ST_READ_BEFORE:
        action = BUF_ACT_FLUSH_THEN_COPY_NEW;
        break;    

    case BUF_ST_READ_INSIDE:
      /*
      ** the start is inside the write buffer check if the len does not exceed the buffer length
      */
      if ((pos_requested - p->read_from) >= p->export->bufsize) 
      {
        if (write_section_empty(p)== 0)
        {        
          action = BUF_ACT_FLUSH_ALIGN_THEN_COPY_NEW;
          break;      
        }
        action = BUF_ACT_COPY_OVERLAP;
        break;
      }
      action = BUF_ACT_COPY;
      break;
      
    case BUF_ST_READ_AFTER:
        action = BUF_ACT_FLUSH_THEN_COPY_NEW;
        break;      
  }
//  printf("action %s\n",rozofs_buf_read_write_action_e2String(action));
  /*
  ** OK now perform the required action
  */
  switch (action)
  {
    case BUF_ACT_COPY_EMPTY:
#ifdef TRACE_FS_READ_WRITE
      info("BUF_ACT_COPY_EMPTY state %d write[%llx:%llx],wrb%d[%llx:%llx],rdb[%llx:%llx]",
            state,(long long unsigned int)off,(long long unsigned int)(off+len),
            p->buf_write_wait,
            (long long unsigned int)p->write_from,(long long unsigned int)p->write_pos,
            (long long unsigned int)p->read_from,(long long unsigned int)p->read_pos);
#endif 
      /*
      ** fill the status section
      */
      status_p->status = BUF_STATUS_DONE;
      status_p->errcode = 0;
      
      if (len < ROZOFS_MAX_FILE_BUF_SZ)
      {           
	p->write_from = off_requested; 
	p->write_pos  = pos_requested; 
	p->buf_write_wait = 1;
	p->read_from = off_requested; 
	p->read_pos  = pos_requested; 
	/*
	** copy the buffer
	*/
	rozofs_write_in_buffer(p,p->buffer,buf,len);
      }
      else
      {
         /*
	 ** flush the buffer without storing the
	 ** data in the buffer associated with the opened file
	 */
	 rozofs_fuse_read_write_stats_buf.big_write_cpt++;
         if ((write_buf_nb(fuse_ctx_p,p, off_requested, buf, len)) < 0) 
	 { 
	  status_p->status = BUF_STATUS_FAILURE;
	  status_p->errcode = errno;
	 }     
	 else
	 {
	  status_p->status = BUF_STATUS_WR_IN_PRG;	 
	 }    
      }
      break;

    case BUF_ACT_COPY:
        /** 
        *      off_req                        pos_req
        *            +-----------------------------+

        *      off_req                 pos_req
        *            +-------------------+
        *
        *   +--------------------------------+
        *   rd_f                             rd_p
        *   +-------------------------------------------+  BUFSIZE
        *  
        *  
        */
#ifdef TRACE_FS_READ_WRITE
      info("BUF_ACT_COPY state %d write[%llx:%llx],wrb%d[%llx:%llx],rdb[%llx:%llx]",
            state,(long long unsigned int)off,(long long unsigned int)(off+len),
            p->buf_write_wait,
            (long long unsigned int)p->write_from,(long long unsigned int)p->write_pos,
            (long long unsigned int)p->read_from,(long long unsigned int)p->read_pos);
#endif
      if (write_section_empty(p))
      {
        p->write_from = off_requested; 
        p->write_pos  = pos_requested; 
      }
      else
      {
        if (p->write_from > off_requested) p->write_from = off_requested;      
        if (p->write_pos  < pos_requested) p->write_pos  = pos_requested;      
      }
      buf_align_write_from(p);
      if (p->write_pos > p->read_pos) p->read_pos = p->write_pos;
      else
      {
        /*
        ** attempt to align write_pos on a block boundary
        */
        buf_align_write_pos(p);
      }
      p->buf_write_wait = 1;
      /*
      ** copy the buffer
      */
      dest_p = p->buffer + (off_requested - p->read_from);
      rozofs_write_in_buffer(p,dest_p,buf,len);
      /*
      ** fill the status section
      */
      status_p->status = BUF_STATUS_DONE;
      status_p->errcode = 0;
      break;  

      /*
      **  there is no write_wait in the buffer
      */
    case BUF_ACT_COPY_OVERLAP:
        /** 
        *      off_req                        pos_req
        *            +-----------------------------+
        *   +-------------------+
        *   rd_f               rd_p
        *   +----------------------------------+  BUFSIZE
        *  
        *  
        */

      /*
      ** check for off_requested alignment on a block size boundary 
      */
#ifdef TRACE_FS_READ_WRITE
      info("BUF_ACT_COPY_OVERLAP state %d write[%llx:%llx],wrb%d[%llx:%llx],rdb[%llx:%llx]",
            state,(long long unsigned int)off,(long long unsigned int)(off+len),
            p->buf_write_wait,
            (long long unsigned int)p->write_from,(long long unsigned int)p->write_pos,
            (long long unsigned int)p->read_from,(long long unsigned int)p->read_pos);      
#endif
      p->write_from = off_requested;
      buf_align_write_from(p);
      if (off_requested == p->write_from)
      {
        /**
        * forget previous buffer content, replace with new content
        */
        dest_p = p->buffer;
        rozofs_write_in_buffer(p,dest_p,buf,len);
        /*
        ** update the offsets
        */
        p->write_pos  = pos_requested; 
        p->buf_write_wait = 1;
        p->read_from = p->write_from; 
        p->read_pos  = p->write_pos;        
        /*
        ** fill the status section
        */
        status_p->status = BUF_STATUS_DONE;
        status_p->errcode = 0;
        break;      
      }
      /*
      ** check the length need for alignment
      */
      len_alignment =  off_requested - p->write_from;
      if ((len+ len_alignment) > p->export->bufsize)
      {
         /**
         *  we cannot align the data to write so copy it unaligned
        * forget previous buffer content, replace with new content
        */
        dest_p = p->buffer;
        rozofs_write_in_buffer(p,dest_p,buf,len);
        /*
        ** update the offsets
        */
        p->write_from = off_requested;
        p->write_pos  = pos_requested; 
        p->buf_write_wait = 1;
        p->read_from = p->write_from; 
        p->read_pos  = p->write_pos;        
        /*
        ** fill the status section
        */
        status_p->status = BUF_STATUS_DONE;
        status_p->errcode = 0;
        break;                     
      }  
      /*
      ** there is enough room in the buffer so move the alignment portion
      ** to the front of the buffer
      */ 
      dest_p = p->buffer;
      src_p = p->buffer + (off_requested - len_alignment -p->read_from);
      rozofs_write_in_buffer(p,dest_p,src_p,len_alignment); 
      /*
      ** now copy the write buffer
      */
      dest_p = p->buffer+len_alignment;
      rozofs_write_in_buffer(p,dest_p,buf,len);                 
      /*
      ** update the offsets
      */
      p->write_from = off_requested - len_alignment;
      p->write_pos  = pos_requested; 
      p->buf_write_wait = 1;
      p->read_from = p->write_from; 
      p->read_pos  = p->write_pos;        
      /*
      ** fill the status section
      */
      status_p->status = BUF_STATUS_DONE;
      status_p->errcode = 0;
      break;                     
  
  
    case BUF_ACT_FLUSH_THEN_COPY_NEW:
        /** 
        *  Before:
        *  -------
        *  off_req                                             pos_req
        *    +----------------------------------------------------+
        *  off_req                 pos_req
        *    +----------------------+
        *   off_req   pos_req
        *    +----------+
        *                 +--------------------------------+
        *               rd_f                             rd_p
        *   
        *  
        *  After:
        *  -------
        *                                 off_req   pos_req
        *                                    +----------+
        *        +----------------------+
        *        rd_f                  rd_p
        *   
        *  
        */

      /*
      ** flush
      */
#ifdef TRACE_FS_READ_WRITE
      info("BUF_ACT_FLUSH_THEN_COPY_NEW state %d write[%llx:%llx],wrb%d[%llx:%llx],rdb[%llx:%llx]",
            state,(long long unsigned int)off,(long long unsigned int)(off+len),
            p->buf_write_wait,
            (long long unsigned int)p->write_from,(long long unsigned int)p->write_pos,
            (long long unsigned int)p->read_from,(long long unsigned int)p->read_pos);     
#endif
      if (p->buf_write_wait != 0) 
      {
        if (len < ROZOFS_MAX_FILE_BUF_SZ)
	{
	  ret = buf_flush(fuse_ctx_p,p);
	}
	else
	{
	  ret = flush_write_ientry(ie);
	  if (ret == 0) ret = -1;
	}
        if (ret < 0)
        {
          status_p->status = BUF_STATUS_FAILURE;
          status_p->errcode = errno;
          break;                  
        }
        /*
        ** write is in progress
        */
        status_p->status = BUF_STATUS_WR_IN_PRG;
        status_p->errcode = 0;            
      }
      else
      {
        /*
        ** fill the status section
        */
        status_p->status = BUF_STATUS_DONE;
        status_p->errcode = 0;            
      }
      if (len < ROZOFS_MAX_FILE_BUF_SZ)
      {
	p->write_from = off_requested; 
	p->write_pos  = pos_requested; 
	p->read_from = off_requested; 
	p->read_pos  = pos_requested; 
	p->buf_write_wait = 1;
	/*
	** copy the buffer
	*/
	rozofs_write_in_buffer(p,p->buffer,buf,len);
      }
      else
      {
         /*
	 ** flush the buffer without storing the
	 ** data in the buffer associated with the opened file
	 */
	 rozofs_fuse_read_write_stats_buf.big_write_cpt++;
         if ((write_buf_nb(fuse_ctx_p,p, off_requested, buf, len)) < 0) 
	 { 
	  status_p->status = BUF_STATUS_FAILURE;
	  status_p->errcode = errno;
	 } 
	 else
	 {
	  status_p->status = BUF_STATUS_WR_IN_PRG;	 
	 }        
      }

      break;    
  
    case BUF_ACT_FLUSH_ALIGN_THEN_COPY_NEW:
        /** 
        *  There is some pending data to write in the current buffer
        *  -------
        *                 off_req                           pos_req
        *                  +------------------------------------+
        *      +--------------------------------+
        *     rd_f                             rd_p
        *      +-------------------------------------------+  BUFSIZE
        *          +-----------------------+ 
        *         wr_f                    wr_pos
        *   
        *  
        */

      /*
      ** partial flush
      */
#ifdef TRACE_FS_READ_WRITE
      info("BUF_ACT_FLUSH_ALIGN_THEN_COPY_NEW state %d write[%llx:%llx],wrb%d[%llx:%llx],rdb[%llx:%llx]",
            state,(long long unsigned int)off,(long long unsigned int)(off+len),
            p->buf_write_wait,
            (long long unsigned int)p->write_from,(long long unsigned int)p->write_pos,
            (long long unsigned int)p->read_from,(long long unsigned int)p->read_pos);     
#endif
      if (write_section_empty(p))
      {
        p->write_from = off_requested; 
      }
      else
      {
        if (p->write_from > off_requested) p->write_from = off_requested;      
      }
      buf_align_write_from(p);
      off2end = p->read_from + p->export->bufsize;
      
      if (off2end < p->write_from)
      {
       severe ("Bug!!!! off2start %llu off2end: %llu\n",(long long unsigned int)p->write_from,
                                                        (long long unsigned int)off2end);      
      }
      else
      {
//        printf("offset start %llu len %u\n",p->write_from,(off2end-p->write_from));            
      }
      /*
      ** partial copy of the buffer in the cache
      */
      buf_off = (uint32_t)(off_requested - p->read_from);
      rozofs_write_in_buffer(p,p->buffer+buf_off,buf,(size_t)(off2end - off_requested));
            
      p->write_pos  = off2end; 
      p->buf_write_wait = 1;
      ret = buf_flush(fuse_ctx_p,p);
      if (ret < 0)
      {
        status_p->status = BUF_STATUS_FAILURE;
        status_p->errcode = errno;
        break;                  
      }      
     /*
      ** write in progress
      */
      status_p->status = BUF_STATUS_WR_IN_PRG;
      status_p->errcode = 0;
      /*
      ** copy the buffer
      */
      buf_off = (uint32_t)(off2end - off_requested);
      rozofs_write_in_buffer(p,p->buffer,buf+buf_off,(len-buf_off));

      p->write_from = off2end; 
      p->write_pos  = pos_requested; 
      p->read_from = off2end; 
      p->read_pos  = pos_requested; 
      if (write_section_empty(p) == 0) p->buf_write_wait = 1;      
 
      break;  
    default:
#ifdef TRACE_FS_READ_WRITE
      info("UNKNOWN %d state %d write[%llx:%llx],wrb%d[%llx:%llx],rdb[%llx:%llx]",
            action,state,(long long unsigned int)off,(long long unsigned int)(off+len),
            p->buf_write_wait,
            (long long unsigned int)p->write_from,(long long unsigned int)p->write_pos,
            (long long unsigned int)p->read_from,(long long unsigned int)p->read_pos);     
#endif
      break;    
  
  }

  /*
  ** Those data will create a hole in the file. 
  ** Let's make the flush of these data exclusive in the STORCLI.
  */  
  if (creating_hole) {
    if (p == ie->write_pending) {
      p->file2create = 1;
    }  
  }
  
  
  /*
  ** At the end of the write procedure, if some data is waiting to be
  ** written on disk, save the file descriptor reference in the ie.
  ** The flush will occur on a further read or write.
  */
  if (p->buf_write_wait == 1) ie->write_pending = p;
} 



/*
**__________________________________________________________________
*/
/** Send a request to the export server to know the file size
 *  adjust the write buffer to write only whole data blocks,
 *  reads blocks if necessary (incomplete blocks)
 *  and uses the function write_blocks to write data
 *
 * @param *f: pointer to the file structure
 * @param off: offset to write from
 * @param *buf: pointer where the data are be stored
 * @param len: length to write
*/
 
static int64_t write_buf_nb(void *buffer_p,file_t * f, uint64_t off, const char *buf, uint32_t len) 
{
   storcli_write_arg_t  args;
   int ret;
   fuse_end_tx_recv_pf_t  callback;
   int storcli_idx;
   ientry_t *ie;
   int use_write_thread = 0 ;
   
   if (ROZOFS_MAX_WRITE_THREADS != 0)
   {
     if (rozofs_fuse_is_current_rcv_buffer((char*)buf) != 0) use_write_thread = 1;
   }  

    // Fill request
    ie = f->ie;
    args.cid = ie->attrs.attrs.cid;
    args.layout = f->export->layout;
    args.bsize = exportclt.bsize;
    memcpy(args.dist_set, ie->attrs.attrs.sids, sizeof (sid_t) * ROZOFS_SAFE_MAX);
    memcpy(args.fid, f->fid, sizeof (fid_t));
    args.off = off;
    args.data.data_len = len;
    ret = rozofs_fill_storage_info(ie,&args.cid,args.dist_set,args.fid);
    if (ret < 0)
    {
      severe("FDL bad storage information encoding");
      return ret;
    }    
    /*
    ** Update the bandwidth statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_WRITE_THR],(uint64_t)len);    
    /**
    * write alignement stats
    */
    rozofs_aligned_write_start[(off%ROZOFS_BSIZE_BYTES(exportclt.bsize)==0)?0:1]++;
    rozofs_aligned_write_end[((off+len)%ROZOFS_BSIZE_BYTES(exportclt.bsize)==0)?0:1]++;
    
    args.data.data_val = (char*)buf;      
    /* 
    **If file was empty at opening tell it to storcli at 1rts write
    */
    args.flags = 0;
    if (f->file2create == 1) {
      args.flags |= STORCLI_FLAGS_EMPTY_FILE;
      f->file2create = 0;
    }
    /*
    ** No need to re-read the last block when not aligned when we extend 
    ** the file since this client is the only writter of the file
    */
    if (conf.onlyWriter) {
      if (off+len >= ie->attrs.attrs.size) {
        args.flags |= STORCLI_FLAGS_NO_END_REREAD;    
      }
    }
    /*
    ** get the storcli to use for the transaction
    */
    storcli_idx = stclbg_storcli_idx_from_fid(f->fid);
//    lbg_id = storcli_lbg_get_lbg_from_fid(f->fid);
    /*
    ** allocate a shared buffer for writing
    */
#if 1
    uint32_t *p32;
    int shared_buf_idx = -1;
    uint32_t length;
    void *shared_buf_ref = rozofs_alloc_shared_storcli_buf(SHAREMEM_IDX_WRITE);
    if (shared_buf_ref != NULL)
    {
      /*
      ** clear the first 4 bytes of the array that is supposed to contain
      ** the reference of the transaction
      */
       p32 = (uint32_t *)ruc_buf_getPayload(shared_buf_ref);
       *p32 = 0;
       /*
       ** store the length to write 
       */
       p32[1] = args.data.data_len;
       /*
       ** get the index of the shared payload in buffer
       */
       shared_buf_idx = rozofs_get_shared_storcli_payload_idx(shared_buf_ref,SHAREMEM_IDX_WRITE,&length);
       if (shared_buf_idx != -1)
       {
	 /*
	 ** indicate to the transmitter that shared memory request type must be used
	 */
	 args.data.data_len = 0x80000000 | shared_buf_idx;
         /*
         ** save the reference of the shared buffer in the fuse context
         */
         SAVE_FUSE_PARAM(buffer_p,shared_buf_ref);
       }
    }
#endif 
    /*
    ** now initiates the transaction towards the remote end
    */
    if (shared_buf_ref == NULL) severe("Out of buffer");
    GET_FUSE_CALLBACK(buffer_p,callback);
    f->buf_write_pending++;
    f->write_block_pending = 1;
    /*
    ** Check iof rozofsmount is configured with write threads
    */
    if (use_write_thread)
    {
      ret = rozofs_storcli_wr_thread_send(STORCLI_WRITE,(void *)&args,(xdrproc_t) xdr_storcli_write_arg_no_data_t,
                                  callback,buffer_p,
			          storcli_idx,f->fid);
    
    
    }
    else
    {
      ret = rozofs_storcli_send_common(NULL,ROZOFS_TMR_GET(TMR_STORCLI_PROGRAM),STORCLI_PROGRAM, STORCLI_VERSION,
                        	STORCLI_WRITE,
				(shared_buf_idx!=-1)?(xdrproc_t) xdr_storcli_write_arg_no_data_t: (xdrproc_t)xdr_storcli_write_arg_t,
				(void *)&args,
                        	callback,buffer_p,storcli_idx,f->fid); 
    }
    if (ret < 0) goto error;
    
    /*
    ** no error just waiting for the answer
    */
    uint64_t buf_flush_offset = off;
    uint32_t buf_flush_len = len;
    SAVE_FUSE_PARAM(buffer_p,buf_flush_offset);
    SAVE_FUSE_PARAM(buffer_p,buf_flush_len);

    return ret;    
error:
    f->buf_write_pending--;
    return ret;

}



/*
**__________________________________________________________________
*/
/**
*  data write

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param ino : inode  provided by rozofsmount
 @param buf : pointer to the buffer that contains the data to write
 @param off : absolute offset in the file
 @param size : size of the data to write
 @param fi : file info structure where information related to the file can be found (file_t structure)
 
 @retval none
*/
void rozofs_ll_write_nb(fuse_req_t req, fuse_ino_t ino, const char *buf,
        size_t size, off_t off, struct fuse_file_info *fi) 
{
    ientry_t *ie = 0;
    void *buffer_p = NULL;
    off_t off_aligned;
    rozo_buf_rw_status_t status;
   int deferred_fuse_write_response;
   errno = 0;

   const char *buf_out = buf;
   off_t off_out       = off;
   size_t size_out     = size;
   int    creating_hole = 0;

    file_t *file = (file_t *) (unsigned long) fi->fh;

    int trc_idx = rozofs_trc_req_io(srv_rozofs_ll_write,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,size,off);

    DEBUG("write to inode %lu %llu bytes at position %llu\n",
            (unsigned long int) ino, (unsigned long long int) size,
            (unsigned long long int) off);

    /*
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_WRITE_IO], 1);

   /*
   ** stats
   */
   ROZOFS_WRITE_STATS_ARRAY(size);

    /*
    ** allocate a context for saving the fuse parameters
    */
    buffer_p = _rozofs_fuse_alloc_saved_context("rozofs_ll_write_nb");
    if (buffer_p == NULL)
    {
      severe("out of fuse saved context");
      errno = ENOMEM;
      goto error;
    }
    START_PROFILING_IO_NB(buffer_p,rozofs_ll_write, size);

    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,size);
    SAVE_FUSE_PARAM(buffer_p,off);
    SAVE_FUSE_PARAM(buffer_p,ino);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);
    SAVE_FUSE_STRUCT(buffer_p,fi,sizeof( struct fuse_file_info)); 
    
    /*
    ** install the callback 
    */
    SAVE_FUSE_CALLBACK(buffer_p,rozofs_ll_write_cbk);

    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }
    /*
    ** clear the lock on mtime since a modification just occurs 
    ** that require a mtime update
    */
    ie->mtime_locked = 0;
    
    /*
    ** file KPI 
    */
    rzkpi_file_stat_update(ie->pfid,(int)size,RZKPI_WRITE);

/*    if (conf.onlyWriter) */
    {
      int bbytes= ROZOFS_BSIZE_BYTES(exportclt.bsize);
      if ((size == 1) && ((off+1)%bbytes == 0))
      {
	off_aligned = (off/bbytes)*bbytes;
	if (ie->attrs.attrs.size <= off_aligned)
	{
           if (rozofs_buf_scratch_p == NULL) 
	   {
	     rozofs_buf_scratch_p = malloc(bbytes);
	     if (rozofs_buf_scratch_p == NULL) fatal("out of memory");
             memset(rozofs_buf_scratch_p,0,bbytes);
	   }	    
           rozofs_buf_scratch_p[bbytes-1] = buf[0];
	   buf_out = rozofs_buf_scratch_p;
	   off_out = off_aligned;
	   size_out = bbytes;	 
	} 
      }
    }
    if (ie->attrs.attrs.size < (off + size)) {
    
        /*
	** Check whether the size extension is compatible 
	** with the export hard quota
	*/
        if (! eid_check_free_quota(exportclt.bsize, ie->attrs.attrs.size - ie->file_extend_size, off + size)) {
          goto error; // errno is already set	  
	}
	/*
	** if the size is 0, assert file2create: it will be checked by storcli in order
	** to exclude any other request concerning the same fid: fix a potential issue
	** when storcli is configure to handle request in parallel
	*/
	if (ie->attrs.attrs.size == 0) 
	{
	  file->file2create = 1;
	}
	
	if (off > ie->attrs.attrs.size) {
	  creating_hole = 1;
	}
	ie->file_extend_pending = 1;
	ie->file_extend_size   += ((off + size) - ie->attrs.attrs.size);
        ie->attrs.attrs.size          = (off + size);
    }

    /*
    ** check the status of the last write operation
    */
    if (file->wr_error!= 0)
    {
      /*
      ** that error is permanent, once can read the file but any
      ** attempt to write will return that error code
      */
      errno = file->wr_error;
      goto error;    
    }
    /*
    ** check if the application is attempting to write atfer a close (_ll_release)
    */
    if (rozofs_is_file_closing(file))
    {
      errno = EBADF;
      goto error;        
    }
    /*
    ** update off start and end to address the case of the geo-replication
    */
    rozofs_geo_write_update(file,off,size);
    
    buf_file_write_nb(ie,buffer_p,&status,file,off_out,buf_out,size_out,creating_hole);
    /*
    ** check the returned status
    */
    if (status.status == BUF_STATUS_FAILURE)
    {
      errno = status.errcode;
      goto error;
    }  
    
    file->current_pos = (off+size);
    /*
    ** clear errno because of the trace
    */
    errno = 0; 
    /*
    **  The data have been written in the buffer and 
    **  no write is pending toward the STORCLI
    */
    if (status.status == BUF_STATUS_DONE)
    {
      rozofs_trc_rsp(srv_rozofs_ll_write,(fuse_ino_t)file,file->fid,(errno==0)?0:1,trc_idx);
      fuse_reply_write(req, size);
      goto out;
    }
    /*
    ** Check the case of the big write wthen the write thread are active
    */
    if (ROZOFS_MAX_WRITE_THREADS != 0)
    {
      if (size >=ROZOFS_MAX_FILE_BUF_SZ)
      {
	 /*
	 ** clear the reference of the buffer since it is under the control of the fuse
	 ** write thread
	 */
	 buffer_p = NULL;
	 goto out;    
      }
    }    
    /*
    ** A write toward the STORCLI is pending 
    ** so we must keep the the fuse context until the STORCLI response
    */
    if (file->buf_write_pending <= ROZOFS_MAX_WRITE_PENDING) {
      deferred_fuse_write_response = 0;
      SAVE_FUSE_PARAM(buffer_p,deferred_fuse_write_response);
      rozofs_trc_rsp(srv_rozofs_ll_write,(fuse_ino_t)file,file->fid,(errno==0)?0:1,trc_idx);
      fuse_reply_write(req, size);
      write_flush_stat.non_synchroneous++;
      buffer_p = NULL;
      goto out;
    }        
    /*
    ** Maximum number of write pending is reached. 
    ** Let's differ the FUSE response until the STORCLI response
    */
    write_flush_stat.synchroneous++;
    deferred_fuse_write_response = 1;
    SAVE_FUSE_PARAM(buffer_p,deferred_fuse_write_response);    
    buffer_p = NULL;
    goto out;
    
error:
    rozofs_trc_rsp(srv_rozofs_ll_write,(fuse_ino_t)file,file->fid,(errno==0)?0:1,trc_idx);
    fuse_reply_err(req, errno);
out:
    STOP_PROFILING_NB(buffer_p,rozofs_ll_write);
    if (buffer_p != NULL) rozofs_fuse_release_saved_context(buffer_p);
    return;
}



/*
**__________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_write_cbk(void *this,void *param) 
{
   struct rpc_msg  rpc_reply;
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   fuse_req_t req; 
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   storcli_status_ret_t ret;
   xdrproc_t decode_proc = (xdrproc_t)xdr_storcli_status_ret_t;
   file_t *file = NULL;
   int deferred_fuse_write_response;
   size_t size;
   errno = 0;
   int trc_idx;
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   /*
   ** update the number of storcli pending request
   */
   if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;
   

   RESTORE_FUSE_STRUCT(param,fi,sizeof( struct fuse_file_info));    
   RESTORE_FUSE_PARAM(param,trc_idx);
       
   file = (file_t *) (unsigned long)  fi->fh;   
   file->buf_write_pending--;
   if (file->buf_write_pending < 0)
   {
     severe("buf_write_pending mismatch, %d",file->buf_write_pending);
     file->buf_write_pending = 0;     
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
       severe(" transaction error %s",strerror(errno));
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
       severe(" transaction error %s",strerror(errno));
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
     severe(" transaction error %s",strerror(errno));
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
       severe(" transaction error %s",strerror(errno));
       goto error;
    }   
    if (ret.status == STORCLI_FAILURE) {
        errno = ret.storcli_status_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    
    /*
    ** When a fuse response is expected send it
    */
    rozofs_trc_rsp(srv_rozofs_ll_write,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,(errno==0)?0:1,trc_idx);
    RESTORE_FUSE_PARAM(param,deferred_fuse_write_response);
    if (deferred_fuse_write_response) {
      RESTORE_FUSE_PARAM(param,req);
      RESTORE_FUSE_PARAM(param,size);
      fuse_reply_write(req, size);
      write_flush_stat.synchroneous_success++;      
    }    

    /*
    ** Update the latency statistics
    */
    {
      uint64_t delta;
      PROFILING_MICRO(param,delta);
      rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_WRITE_LATENCY_COUNT], 1);
      rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_WRITE_LATENCY],delta);  
      //info("rozofs_ll_write_cbk %llu", (long long unsigned int)delta);
    }   
    /*
    ** no error, so get the length of the data part
    */
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);
    /*
    ** Keep the fuse context since we need to trigger the update of 
    ** the metadata of the file
    */
    rozofs_tx_free_from_ptr(rozofs_tx_ctx_p); 
    ruc_buf_freeBuffer(recv_buf);  
    /*
    ** Update the exportd with the filesize if that one has changed
    */ 
    export_write_block_nb(param,file);
    /*
    ** check if there is some request waiting for the last pending write to complete
    */
    return check_last_write_pending(file);

error:
    if (file != NULL)
    {
       file->wr_error = errno; 
    }
    
    /*
    ** When a fuse response is expected send it
    */
    rozofs_trc_rsp(srv_rozofs_ll_write,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,(errno==0)?0:1,trc_idx);
    RESTORE_FUSE_PARAM(param,deferred_fuse_write_response);
    if (deferred_fuse_write_response) {
      RESTORE_FUSE_PARAM(param,req);
      fuse_reply_err(req, errno);
      write_flush_stat.synchroneous_error++;      
    }
      
    /*
    ** release the transaction context and the fuse context
    */
 //   STOP_PROFILING_NB(param,rozofs_ll_write);

    /*
    ** Update the exportd with the written filesize if some succesfull write 
    ** have occured previously.
    */ 
    export_write_block_error_nb(param,file);
    //rozofs_fuse_release_saved_context(param);
    
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    
    return check_last_write_pending(file);
}



void rozofs_ll_flush_cbk(void *this,void *param) ;
void rozofs_ll_flush_defer(void *ns,void *param) ;
void rozofs_asynchronous_flush_cbk(void *ns,void *param) ;
/*
**__________________________________________________________________
*/
/**
*  data flush

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param ino : inode  provided by rozofsmount
 @param fi : file info structure where information related to the file can be found (file_t structure)
 
 @retval none
*/

void rozofs_ll_flush_nb(fuse_req_t req, fuse_ino_t ino,
        struct fuse_file_info *fi) {
    file_t *f;
    ientry_t *ie = 0;
    void *buffer_p= NULL;
    int ret;
    int trc_idx=0;
    errno = 0;
    /*
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_OTHER], 1);

    DEBUG_FUNCTION;
    if (!(f = (file_t *) (unsigned long) fi->fh)) {
        rozofs_trc_req(srv_rozofs_ll_flush,(fuse_ino_t)f,(f==NULL)?NULL:f->fid);
        errno = EBADF;
        goto error;
    }
    {
      uint64_t flush_off = 0;
      uint32_t flush_len = 0;
      if (f->buf_write_wait!= 0)
      {      
        flush_off = f->write_from;
        flush_len = (uint32_t)(f->write_pos - f->write_from);
      }
      trc_idx = rozofs_trc_req_io(srv_rozofs_ll_flush,(fuse_ino_t)f,(f==NULL)?NULL:f->fid,flush_len,flush_off);
    }
    /*
    ** allocate a context for saving the fuse parameters
    */
    buffer_p = _rozofs_fuse_alloc_saved_context("rozofs_ll_flush_nb");
    if (buffer_p == NULL)
    {
      severe("out of fuse saved context");
      errno = ENOMEM;
      goto error;
    }
    START_PROFILING_NB(buffer_p,rozofs_ll_flush);
    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);
    SAVE_FUSE_PARAM(buffer_p,ino);
    SAVE_FUSE_STRUCT(buffer_p,fi,sizeof( struct fuse_file_info));    

    /*
    ** install the callback 
    */
    SAVE_FUSE_CALLBACK(buffer_p,rozofs_ll_flush_cbk);
    
    // Sanity check
    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }

    //info("flush lock_owner %llx", (unsigned long long)fi->lock_owner);

    /*
    ** check the status of the last write operation
    */
    if (f->wr_error!= 0)
    {
      /*
      ** that error is permanent, once can read the file but any
      ** attempt to write will return that error code
      */
      errno = f->wr_error;
      goto error;    
    }
    /*
    ** check if there some pendinag data to write in the buffer
    */
    if (f->buf_write_wait!= 0)
    {
      /*
      ** flush to disk and wait for the response
      */
      ret = buf_flush(buffer_p,f);
      if (ret < 0) goto error;
    /*
    ** wait for the end of the flush: just return evreything will take palce
    ** upon receiving the flush response (see rozofs_ll_flush_cbk())
    */
      return;
    }
    /*
    ** nothing to flush to disk, but we might some write transactions running
    ** since it is a sync. We have to wait up to the end of the last running transaction
    */
    if (f->buf_write_pending > 0) 
    {
      /*
      ** install the call for the flush response deferred until receiving the last write response
      */ 
      SAVE_FUSE_CALLBACK(buffer_p,rozofs_ll_flush_defer); 
      fuse_ctx_write_pending_queue_insert(f,buffer_p);
      /*
      ** the response is deferred until the last write transaction is responding 
      */
      return;
    }
    /*
    ** nothing pending, so we can reply immediately and we force the update of the
    ** metadata server
    */
    rozofs_trc_rsp(srv_rozofs_ll_flush,(fuse_ino_t)f,(f==NULL)?NULL:f->fid,(errno==0)?0:1,trc_idx);
    STOP_PROFILING_NB(buffer_p,rozofs_ll_flush);
    
    f->write_block_req = 1;  
    export_write_block_nb(buffer_p,f);
    fuse_reply_err(req, 0);
    return;
    
error:
    fuse_reply_err(req, errno);
    rozofs_trc_rsp(srv_rozofs_ll_flush,(fuse_ino_t)f,(f==NULL)?NULL:f->fid,(errno==0)?0:1,trc_idx);
    STOP_PROFILING_NB(buffer_p,rozofs_ll_flush);
    if (buffer_p != NULL) rozofs_fuse_release_saved_context(buffer_p);
    return;
}

/*
**__________________________________________________________________
*/
/**
*  Some request may trigger an internal flush before beeing executed.

   That's the case of a read request while the file buffer contains
   some data that have not yet been saved on disk, but do not contain 
   the data that the read wants. 

   No fuse reply is expected

 @param fi   file info structure where information related to the file can be found (file_t structure)
 
 @retval 0 in case of failure 1 on success
*/

int rozofs_asynchronous_flush(struct fuse_file_info *fi) {
  file_t * f;
  void   * buffer_p= 0;
  int      ret;

  DEBUG_FUNCTION;

  /*
  ** Retrieve file context
  */
  if (!(f = (file_t *) (unsigned long) fi->fh)) {
    errno = EBADF;
    return 0;
  }

  /*
  ** check the status of the last write operation
  */
  if (f->wr_error!= 0) {
    /*
    ** that error is permanent, once can read the file but any
    ** attempt to write will return that error code
    */
    errno = f->wr_error;
    return 0;
  }

  /*
  ** check that there is actually some pending data to write in the buffer
  */
  if (f->buf_write_wait == 0) return 1; 

  /*
  ** allocate a context for processing the internal flush
  */
  buffer_p = _rozofs_fuse_alloc_saved_context("rozofs_asynchronous_flush");
  if (buffer_p == NULL) {
    severe("out of fuse context");
    errno = ENOMEM;
    return 0;
  }    

  /*
  ** Initialize the so called fuse context
  */
  SAVE_FUSE_STRUCT(buffer_p,fi,sizeof( struct fuse_file_info));    
  SAVE_FUSE_CALLBACK(buffer_p,rozofs_asynchronous_flush_cbk)
  START_PROFILING_NB(buffer_p,rozofs_ll_flush);

  /*
  ** flush to disk and wait for the response
  */
  ret = buf_flush(buffer_p,f);
  if (ret < 0) {
    rozofs_fuse_release_saved_context(buffer_p);
    return 0;
  }
  return 1;
}
/*
**__________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_asynchronous_flush_cbk(void *this,void *param) 
{
//   fuse_req_t req; 
   struct rpc_msg  rpc_reply;
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   storcli_status_ret_t ret;
   xdrproc_t decode_proc = (xdrproc_t)xdr_storcli_status_ret_t;
   file_t *file = NULL;

   /*
   ** update the number of storcli pending request
   */
   if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;
   
   rpc_reply.acpted_rply.ar_results.proc = NULL;
//   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_STRUCT(param,fi,sizeof( struct fuse_file_info));    

   file = (file_t *) (unsigned long)  fi->fh;   
   file->buf_write_pending--;
   if (file->buf_write_pending < 0)
   {
     severe("buf_write_pending mismatch, %d",file->buf_write_pending);
     file->buf_write_pending = 0;     
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
       severe(" transaction error %s",strerror(errno));
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
       severe(" transaction error %s",strerror(errno));
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
     severe(" transaction error %s",strerror(errno));
     goto error;
    }
    /*
    ** ok now call the procedure to decode the message
    */
    memset(&ret,0, sizeof(ret));                    
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       xdr_free((xdrproc_t) decode_proc, (char *) &ret);
       severe(" transaction error %s",strerror(errno));
       goto error;
    }   
    if (ret.status == STORCLI_FAILURE) {
        errno = ret.storcli_status_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    /*
    ** no error, so get the length of the data part
    */
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);
//    fuse_reply_err(req, 0);

    /*
    ** Update the latency statistics
    */
    {
      uint64_t delta;
      PROFILING_MICRO(param,delta);
      rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_WRITE_LATENCY_COUNT], 1);
      rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_WRITE_LATENCY],delta);  
      //info("rozofs_asynchronous_flush_cbk %llu", (long long unsigned int)delta);
    }  
    /*
    ** Keep the fuse context since we need to trigger the update of 
    ** the metadata of the file
    */
    rozofs_tx_free_from_ptr(rozofs_tx_ctx_p); 
    ruc_buf_freeBuffer(recv_buf);   
    /*
    ** Update the exportd with the filesize if that one has changed
    */ 
    export_write_block_nb(param,file);
    /*
    ** check if there is some request waiting for the last pending write to complete
    */
    return check_last_write_pending(file);
    
error:
    if (file != NULL)
    {
       file->wr_error = errno; 
    }
//    fuse_reply_err(req, errno);
    /*
    ** release the transaction context and the fuse context
    */
//    STOP_PROFILING_NB(param,rozofs_ll_flush);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);     
    
    return check_last_write_pending(file);
}
/*
**__________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_flush_cbk(void *this,void *param) 
{
   fuse_req_t req; 
   struct rpc_msg  rpc_reply;
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   storcli_status_ret_t ret;
   xdrproc_t decode_proc = (xdrproc_t)xdr_storcli_status_ret_t;
   file_t *file = NULL;
   int trc_idx;
   errno = 0;
   /*
   ** update the number of storcli pending request
   */
   if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;
      
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_STRUCT(param,fi,sizeof( struct fuse_file_info));    

   file = (file_t *) (unsigned long)  fi->fh;   
   file->buf_write_pending--;
   if (file->buf_write_pending < 0)
   {
     severe("buf_write_pending mismatch, %d",file->buf_write_pending);
     file->buf_write_pending = 0;     
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
       severe(" transaction error %s",strerror(errno));
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
       severe(" transaction error %s",strerror(errno));
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
     severe(" transaction error %s",strerror(errno));
     goto error;
    }
    /*
    ** ok now call the procedure to decode the message
    */
    memset(&ret,0, sizeof(ret));                    
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       xdr_free((xdrproc_t) decode_proc, (char *) &ret);
       severe(" transaction error %s",strerror(errno));
       goto error;
    }   
    if (ret.status == STORCLI_FAILURE) {
        errno = ret.storcli_status_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    /*
    ** no error, so get the length of the data part
    */
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);
    fuse_reply_err(req, 0);
    /*
    ** Keep the fuse context since we need to trigger the update of 
    ** the metadata of the file
    */
    rozofs_trc_rsp(srv_rozofs_ll_flush,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,(errno==0)?0:1,trc_idx);
    rozofs_tx_free_from_ptr(rozofs_tx_ctx_p); 
    ruc_buf_freeBuffer(recv_buf); 

    /*
    ** Update the latency statistics
    */
    {
      uint64_t delta;
      PROFILING_MICRO(param,delta);
      rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_WRITE_LATENCY_COUNT], 1);
      rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_WRITE_LATENCY],delta);  
      //info("rozofs_ll_flush_cbk %llu", (long long unsigned int)delta);
    }  

    /*
    ** force the flush on disk
    */
    file->write_block_req = 1;  
    export_write_block_nb(param,file);
    
    /*
    ** check if there is some request waiting for the last pending write to complete
    */
    return check_last_write_pending(file);
    
error:
    if (file != NULL)
    {
       file->wr_error = errno; 
    }
    fuse_reply_err(req, errno);
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_flush,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,(errno==0)?0:1,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_flush);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   

    return check_last_write_pending(file);
}


/*
**__________________________________________________________________
*/
/**
*  Call back function associated to a flsh waiting for end of write
*
 @param ns : Not significant
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_flush_defer(void *ns,void *param) 
{
   fuse_req_t req; 
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   int trc_idx;
   errno = 0;
   
   file_t *file = NULL;

   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_STRUCT(param,fi,sizeof( struct fuse_file_info));    

   file = (file_t *) (unsigned long) fi->fh;
    /*
    ** check the status of the last write operation
    */
    errno = 0;
    if (file->wr_error!= 0)
    {
      /*
      ** that error is permanent, once can read the file but any
      ** attempt to write will return that error code
      */
      errno = file->wr_error;
    }
    fuse_reply_err(req, errno) ;    
    /*
    ** udpate the metadata service 
    */
    rozofs_trc_rsp(srv_rozofs_ll_flush,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,(errno==0)?0:1,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_flush);
    file->write_block_req = 1;  
    export_write_block_nb(param,file);    
//    rozofs_fuse_release_saved_context(param);    
    return;
}

/*
**__________________________________________________________________
*/
/**
*  release

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param ino : inode  provided by rozofsmount
 @param fi : file info structure where information related to the file can be found (file_t structure)
 
 @retval none
*/
void rozofs_ll_release_cbk(void *this,void *param) ;
void rozofs_ll_release_defer(void *ns,void *param); 

void rozofs_ll_release_nb(fuse_req_t req, fuse_ino_t ino,
        struct fuse_file_info *fi) {
    file_t *f = NULL;
    ientry_t *ie = 0;
    void *buffer_p = NULL;
    int ret;
    errno = 0;  
    int xerrno = 0;

    /*
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_OTHER], 1);
    
    int trc_idx = rozofs_trc_req(srv_rozofs_ll_release,(fuse_ino_t)fi->fh,NULL);

    DEBUG("release (%lu)\n", (unsigned long int) ino);

    if (!(f = (file_t *) (unsigned long) fi->fh)) {
        errno = EBADF;
        goto error;
    }

    /*
    ** allocate a context for saving the fuse parameters
    */
    buffer_p = _rozofs_fuse_alloc_saved_context("rozofs_ll_release_nb");
    if (buffer_p == NULL)
    {
      severe("out of fuse saved context");
      errno = ENOMEM;
      goto error;
    }
    START_PROFILING_NB(buffer_p,rozofs_ll_release);
    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);
    SAVE_FUSE_PARAM(buffer_p,ino);    
    SAVE_FUSE_STRUCT(buffer_p,fi,sizeof( struct fuse_file_info));    
    /*
    ** install the callback 
    */
    SAVE_FUSE_CALLBACK(buffer_p,rozofs_ll_release_cbk);
    
    
    // Sanity check
    if (!(ie = get_ientry_by_inode(ino))) {
        char fidString[64];
	rozofs_uuid_unparse(f->fid,fidString);
	severe("rozofs_ll_release_nb no such inode %llu FID %s",
	        (long long unsigned int)ino, fidString);
        errno = ENOENT;
        goto error;
    }    
    /*
    ** update the statistics
    */
    rzkpi_file_stat_update(ie->pfid,(int)0,RZKPI_CLOSE);
    
   if (rozofs_bugwatch) severe("BUGROZOFSWATCH release(%p) , buf_write_wait=%d, buf_write_pending=%d,",
                                   f,f->buf_write_wait,f->buf_write_pending);

    /*
    ** check if there some pendinag data to write in the buffer
    */
    if (f->buf_write_wait!= 0)
    {
      /*
      ** flush to disk and wait for the response
      */
      ret = buf_flush(buffer_p,f);
      if (ret < 0) goto error;
    /*
    ** wait for the end of the flush: just return evreything will take palce
    ** upon receiving the flush response (see rozofs_ll_flush_cbk())
    */
      return;
    }
    /*
    ** nothing to flush to disk, but we might some write transactions running
    ** since it is a sync. We have to wait up to the end of the last running transaction
    */
    if (f->buf_write_pending > 0) 
    {
      /*
      ** install the call for the flush response deferred until receiving the last write response
      */ 
      SAVE_FUSE_CALLBACK(buffer_p,rozofs_ll_release_defer); 
      fuse_ctx_write_pending_queue_insert(f,buffer_p);
      /*
      ** the response is deferred until the last write transaction is responding 
      */
      return;
    }
    /*
    ** release the data structure associated with the file descriptor
    */
    STOP_PROFILING_NB(buffer_p,rozofs_ll_release);
    f->write_block_req = 1;  
    export_write_block_nb(buffer_p,f);    
    /*
    ** report the status of the last write operation
    */
    xerrno = f->wr_error;    
    file_close(f);
    fuse_reply_err(req, xerrno);
    errno=xerrno;
    rozofs_trc_rsp(srv_rozofs_ll_release,(fuse_ino_t)f,(ie==NULL)?NULL:ie->attrs.attrs.fid,1,trc_idx);
//    STOP_PROFILING_NB(buffer_p,rozofs_ll_release);
    return;

error:
    /*
    ** release the data structure associated with the file descriptor
    */
    file_close(f);
    fuse_reply_err(req, errno);
    rozofs_trc_rsp(srv_rozofs_ll_release,(fuse_ino_t)f,(ie==NULL)?NULL:ie->attrs.attrs.fid,1,trc_idx);
    STOP_PROFILING_NB(buffer_p,rozofs_ll_release);
    if (buffer_p != NULL) rozofs_fuse_release_saved_context(buffer_p);   
    return;
}

/*
**__________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_release_cbk(void *this,void *param) 
{
   fuse_req_t req; 
   struct rpc_msg  rpc_reply;
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   storcli_status_ret_t ret;
   xdrproc_t decode_proc = (xdrproc_t)xdr_storcli_status_ret_t;
   file_t *file = NULL;
   errno = 0;
   int trc_idx;
   /*
   ** update the number of storcli pending request
   */
   if (rozofs_storcli_pending_req_count > 0) rozofs_storcli_pending_req_count--;
   
   
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_STRUCT(param,fi,sizeof( struct fuse_file_info));    

   file = (file_t *) (unsigned long)  fi->fh;   
   file->buf_write_pending--;
   if (file->buf_write_pending < 0)
   {
     severe("buf_write_pending mismatch, %d",file->buf_write_pending);
     file->buf_write_pending = 0;     
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
       severe(" transaction error %s",strerror(errno));
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
       severe(" transaction error %s",strerror(errno));
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
     severe(" transaction error %s",strerror(errno));
     goto error;
    }
    /*
    ** ok now call the procedure to decode the message
    */
    memset(&ret,0, sizeof(ret));                    
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       xdr_free((xdrproc_t) decode_proc, (char *) &ret);
       severe(" transaction error %s",strerror(errno));
       goto error;
    }   
    if (ret.status == STORCLI_FAILURE) {
        errno = ret.storcli_status_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    /*
    ** no error, so get the length of the data part
    */
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);

    /*
    ** Update the latency statistics
    */
    {
      uint64_t delta;
      PROFILING_MICRO(param,delta);
      rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_WRITE_LATENCY_COUNT], 1);
      rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_WRITE_LATENCY],delta);  
      //info("rozofs_ll_release_cbk %llu", (long long unsigned int)delta);
    }  

    /*
    ** It might be possible that the write triggered on the close is not the last one since
    ** write are performed in parallel by the storcli.
    ** We should defer the close response, since if we do it, the VFS can issue a forget that will
    ** remove the ientry. The consequence of that, is memory access during the processing of the other
    ** write responses while the ientry memory has been released.
    */
    if (file->buf_write_pending > 0)
    {
      /*
      ** install the call for the flush response deferred until receiving the last write response
      */ 
      SAVE_FUSE_CALLBACK(param,rozofs_ll_release_defer); 
      fuse_ctx_write_pending_queue_insert(file,param);
      /*
      ** the response is deferred until the last write transaction is responding 
      */
      rozofs_trc_rsp(srv_rozofs_ll_release,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,status,trc_idx);    
      rozofs_tx_free_from_ptr(rozofs_tx_ctx_p); 
      ruc_buf_freeBuffer(recv_buf); 
      return;    
    
    }
    fuse_reply_err(req, 0);

    /*
    ** Keep the fuse context since we need to trigger the update of 
    ** the metadata of the file
    */
    rozofs_trc_rsp(srv_rozofs_ll_release,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,status,trc_idx);    
    rozofs_tx_free_from_ptr(rozofs_tx_ctx_p); 
    ruc_buf_freeBuffer(recv_buf); 
    STOP_PROFILING_NB(param,rozofs_ll_release); 
    file->write_block_req = 1;     
    export_write_block_nb(param,file);
    file_close(file);
    return;
    
error:
    if (file != NULL)
    {
       file->wr_error = errno; 
    }
    fuse_reply_err(req, errno);

    /*
    ** release the transaction context and the fuse context
    ** and release the file descriptor
    */
    rozofs_trc_rsp(srv_rozofs_ll_release,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,status,trc_idx);    
    file_close(file);    
    STOP_PROFILING_NB(param,rozofs_ll_release);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    
    return;
}


/*
**__________________________________________________________________
*/
/**
*  Call back function associated to a flsh waiting for end of write
*
 @param ns : Not significant
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_release_defer(void *ns,void *param) 
{
   fuse_req_t req; 
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   int trc_idx;
   
   file_t *file = NULL;

   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_STRUCT(param,fi,sizeof( struct fuse_file_info));    

    file = (file_t *) (unsigned long) fi->fh;
    /*
    ** check the status of the last write operation
    */
    errno = 0;
    if (file->wr_error!= 0)
    {
      /*
      ** that error is permanent, once can read the file but any
      ** attempt to write will return that error code
      */
      errno = file->wr_error;
    }
    fuse_reply_err(req, errno) ; 
    /*
    ** trigger the update of the metadata server
    */
    rozofs_trc_rsp(srv_rozofs_ll_release,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,(errno==0)?0:1,trc_idx);    
    file->write_block_req = 1;     
    STOP_PROFILING_NB(param,rozofs_ll_release);
    export_write_block_nb(param,file);   
    /*
    ** release the transaction context and the fuse context
    ** and release the file descriptor
    */
    file_close(file);
    /*
    ** do not release the transaction context and the fuse context; this is done by export_write_block_cbk()
    */
//    rozofs_fuse_release_saved_context(param);    
    return;
}







/*
**___________________________R E S I Z E_________________________________
*/








/**
*  metadata update -> need to update the file size

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param parent : inode parent provided by rozofsmount
 @param name : name to search in the parent directory
 
 @retval none
*/
void export_force_write_block_cbk(void *this,void *param)
{
  epgw_mattr_ret_t ret ;
  struct rpc_msg  rpc_reply;


  int status;
  uint8_t  *payload;
  void     *recv_buf = NULL;   
  XDR       xdrs;    
  int      bufsize;
  xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_mattr_ret_t;
  rozofs_fuse_save_ctx_t *fuse_ctx_p;
  errno = 0;
  int trc_idx;
  ientry_t *ie = 0;

  GET_FUSE_CTX_P(fuse_ctx_p,param);    
  RESTORE_FUSE_PARAM(param,trc_idx);

  rpc_reply.acpted_rply.ar_results.proc = NULL;
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
     ** Attempt to queue the ientry in the write_block retry list
     */
     
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
  bufsize = rozofs_tx_get_small_buffer_size();
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
  /*
  **  This gateway do not support the required eid 
  */    
  if (ret.status_gw.status == EP_FAILURE_EID_NOT_SUPPORTED) {    

      /*
      ** Do not try to select this server again for the eid
      ** but directly send to the exportd
      */
      expgw_routing_expgw_for_eid(&fuse_ctx_p->expgw_routing_ctx, ret.hdr.eid, EXPGW_DOES_NOT_SUPPORT_EID);       

      xdr_free((xdrproc_t) decode_proc, (char *) &ret);    

      /* 
      ** Attempt to re-send the request to the exportd and wait being
      ** called back again. One will use the same buffer, just changing
      ** the xid.
      */
      status = rozofs_expgateway_resend_routing_common(rozofs_tx_ctx_p, NULL,param); 
      if (status == 0)
      {
        /*
        ** do not forget to release the received buffer
        */
        ruc_buf_freeBuffer(recv_buf);
        recv_buf = NULL;
        return;
      }           
      /*
      ** Not able to resend the request
      */
      errno = EPROTO; /* What else ? */
      goto error;

  }



  if (ret.status_gw.status == EP_FAILURE) {
      errno = ret.status_gw.ep_mattr_ret_t_u.error;
      xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
      goto error;
  }

  /*
  ** Update eid free quota
  */
  eid_set_free_quota(ret.free_quota);


  /*
  ** Update cache entry
  */
  ie = get_ientry_by_fid((unsigned char*) ret.status_gw.ep_mattr_ret_t_u.attrs.fid);
  if (ie) {
    /*
    ** update the attributes in the ientry
    */
    rozofs_ientry_update(ie,(struct inode_internal_t *)&ret.status_gw.ep_mattr_ret_t_u.attrs);  
    //ie->file_extend_running = 0;
  }

  xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
//out:

error:
  /*
  ** release the transaction context and the fuse context
  */
  rozofs_trc_rsp(srv_rozofs_ll_ioctl,0/*ino*/,NULL,(errno==0)?0:1,trc_idx);
  STOP_PROFILING_NB(param,rozofs_ll_ioctl);
  rozofs_fuse_release_saved_context(param);
  if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
  if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
  return;
}
/**
*  metadata update -> need to update the file size

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param parent : inode parent provided by rozofsmount
 @param name : name to search in the parent directory
 
 @retval none
*/
int export_force_write_block_asynchrone(void *fuse_ctx_p,  ientry_t *ientry, uint64_t from, uint64_t to) 
{
    epgw_write_block_arg_t arg;
    int                    ret;        

    int trc_idx = rozofs_trc_req_io(srv_rozofs_ll_ioctl, ientry->inode, ientry->fid, to,0);
    SAVE_FUSE_PARAM(fuse_ctx_p,trc_idx);
    /*
    ** insert the site number in the argument
    */
    arg.hdr.gateway_rank = rozofs_get_site_number(); 
//    severe("FDL site_number %d",arg.hdr.gateway_rank);
    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid = exportclt.eid;
    memcpy(arg.arg_gw.fid, ientry->fid, sizeof (fid_t));
    arg.arg_gw.bid = 0;
    arg.arg_gw.nrb = 1;
    arg.arg_gw.length = 0; //buf_flush_len;
    arg.arg_gw.offset = to; //buf_flush_offset;
    arg.arg_gw.geo_wr_start = from;
    arg.arg_gw.geo_wr_end   = to;
    /*
    ** now initiates the transaction towards the remote end
    */
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,ientry->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_WRITE_BLOCK,(xdrproc_t) xdr_epgw_write_block_arg_t,(void *)&arg,
                              export_force_write_block_cbk,fuse_ctx_p); 
    return ret;  
}

/**
**____________________________________________________________
      Write Block Retry section
**____________________________________________________________
*/      
/*
**__________________________________________________________________
*/
/**
  Queue a pending write block in the pending list
 
   The entry parameter should contain the inode value provide by VFS
 
  @param param : fuse context
  
  @retval 0 on success
  @retvak -1 on error

 */
int rozofs_export_queue_ie_in_wr_block_list_from_fuse_ctx(void *param)
{
   fuse_ino_t ino;
   ientry_t *ie;
   RESTORE_FUSE_PARAM(param,ino);

    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        return -1;
    }
    rozofs_export_queue_ie_in_wr_block_list(ie);
    return 0;
}
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
 
void export_write_block_retry_cbk(void *this,void *param) 
{
   epgw_mattr_ret_t ret ;
   struct rpc_msg  rpc_reply;

   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_mattr_ret_t;
   errno = 0;
   int trc_idx;
   ientry_t *ie = 0;
   ino_t   ino;
    
   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_PARAM(param,ino);
   
   ie = get_ientry_by_inode(ino);


   rpc_reply.acpted_rply.ar_results.proc = NULL;
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
       ** queue the ientry on the write_block pending list for retrying later
       */
       rozofs_export_queue_ie_in_wr_block_list_from_fuse_ctx(param);
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
    bufsize = rozofs_tx_get_small_buffer_size();
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

    if (ret.status_gw.status == EP_FAILURE) {
        errno = ret.status_gw.ep_mattr_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    
    /*
    ** Update eid free quota
    */
    eid_set_free_quota(ret.free_quota);
            
    /*
    ** Update cache entry
    */
    if (ie) {
      /*
      ** update the attributes in the ientry
      */
      rozofs_ientry_update(ie,(struct inode_internal_t *)&ret.status_gw.ep_mattr_ret_t_u.attrs);  
      ie->file_extend_running = 0;
      /*
      ** check if some more write blocks are pending
      */
      rozofs_export_write_block_list_process();
    }
        
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
error:
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_ioctl,0/*ino*/,NULL,(errno==0)?0:1,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_ioctl);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    /*
    ** check if the ientry has to be deleted
    */
    if (ie) del_ientry(ie);
    
    return;
}

/**
*  metadata update on retry -> need to update the file size 
   THis occurs when the exportd does not respond to the write block because of a switch-over condition


 @param fuse_ctx_p: pointer to the fuse request context (must be preserved for the transaction duration
 @param ie : ientry for whicj a write block retry is attempted
 
 @retval 0 success
 @retval <>0 error

*/
int export_write_block_asynchrone_retry(void *fuse_ctx_p, ientry_t *ie) 
{
    epgw_write_block_arg_t arg;
    int    ret;        

    int trc_idx = rozofs_trc_req_io(srv_rozofs_ll_ioctl,(fuse_ino_t)ie,ie->fid, ie->attrs.attrs.size,0);
    SAVE_FUSE_PARAM(fuse_ctx_p,trc_idx);
    /*
    ** insert the site number in the argument
    */
    arg.hdr.gateway_rank = rozofs_get_site_number(); 
    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid = exportclt.eid;
    memcpy(arg.arg_gw.fid, ie->fid, sizeof (fid_t));
    arg.arg_gw.bid = 0;
    arg.arg_gw.nrb = 1;
    arg.arg_gw.length = 0; //buf_flush_len;
    arg.arg_gw.offset = ie->attrs.attrs.size; //buf_flush_offset;
    arg.arg_gw.geo_wr_start = 0;
    arg.arg_gw.geo_wr_end   = 0;
    /*
    ** now initiates the transaction towards the remote end
    */
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,ie->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_WRITE_BLOCK,(xdrproc_t) xdr_epgw_write_block_arg_t,(void *)&arg,
                              export_write_block_retry_cbk,fuse_ctx_p); 
    if (ret == 0) {
      /*
      ** indicates that the revelant file size will be found in the ientry
      */
      if (ie->file_extend_pending )       
      {
        ie->file_extend_pending = 0;
	ie->file_extend_size    = 0;
        ie->file_extend_running = 1;
     } 
    } 
    else
    {
      /*
      ** queue the ientry in the retry list of the wr_block. The insertion takes place
      ** where the exportd connectivity is lost during a switch-over
      */
      rozofs_export_queue_ie_in_wr_block_list(ie);
    }   
    return ret;  
}


/*
**__________________________________________________________________
*/
/**
  Process the pending write block list
 
  @param param : none
  
 */
void rozofs_export_write_block_list_process()
{
  rozofs_fuse_ctx_t  *ctx_p;
  uint32_t            buffer_count;
  fid_t               fake_fid;
  expgw_tx_routing_ctx_t routing_ctx; 
  list_t             * p;
  list_t             * n;
   void *buffer_p = NULL;
  ientry_t           *ie;
  fuse_ino_t         ino;
  int                ret;
  int                credit = 1;
 
  if (list_empty(&list_wr_block_head)) return;
  /*
  ** check if there is enought fuse context for processing pending requests
  */
  ctx_p = (rozofs_fuse_ctx_t*)rozofs_fuse_ctx_p;
  
  buffer_count = ruc_buf_getFreeBufferCount(ctx_p->fuseReqPoolRef);
  if (buffer_count < 30) return;
  /*
  ** Check the status of the lbg towards the exportd
  */
  if (expgw_get_export_routing_lbg_info(exportclt.eid,fake_fid,&routing_ctx) != 0) {
     return;
  }
  if (north_lbg_get_state(routing_ctx.lbg_id[0]) != NORTH_LBG_UP) {       
     return;
  }
  /*
  ** loop on the pending wr_block requests
  */
  list_for_each_forward_safe(p,n,&list_wr_block_head) {
    
    ie = (ientry_t *) list_entry(p, ientry_t, list_wr_block);
    /*
    ** allocate a fuse context
    */
    buffer_p = rozofs_fuse_alloc_saved_context();
    if (buffer_p == NULL)
    {
      return;
    }
    ino = ie->inode;
    SAVE_FUSE_PARAM(buffer_p,ino);
    /*
    ** remove the ientry from the list prior attempting to send the write block towards
    ** the exportd: this is required since the ie can be requeued during the attempt
    */
    rozofs_export_dequeue_ie_from_wr_block_list(ie);
    ret = export_write_block_asynchrone_retry(buffer_p,ie);
    if (ret != 0) return;
    /*
    ** check if it remains some credits
    */
    credit--;
    if (credit == 0) return;
  }
}

/**
**____________________________________________________________
      Write Block Retry end
**____________________________________________________________
*/ 

/**
*  metadata update -> need to update the file size

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param parent : inode parent provided by rozofsmount
 @param name : name to search in the parent directory
 
 @retval none
*/
int export_write_block_asynchrone(void *fuse_ctx_p, file_t *file_p, sys_recv_pf_t recv_cbk) 
{
    epgw_write_block_arg_t arg;
    int    ret;        
    uint64_t buf_flush_offset ;
    uint32_t buf_flush_len ;
    ientry_t *ie;
    
    RESTORE_FUSE_PARAM(fuse_ctx_p,buf_flush_offset);
    RESTORE_FUSE_PARAM(fuse_ctx_p,buf_flush_len);

    /*
    ** Invalidate cache entry
    */ 
    ie = file_p->ie;
    if (ie) {
      ie->timestamp = 0;
      /*
      ** dequeue the ientry from the write block pending list
      */
      rozofs_export_dequeue_ie_from_wr_block_list(ie);

    }  


    /*
    ** adjust the size of the attributes of the local file
    */
    if ((buf_flush_offset + buf_flush_len) > ie->attrs.attrs.size)
    {
      ie->file_extend_size   += ((buf_flush_offset + buf_flush_len) - ie->attrs.attrs.size);    
      ie->attrs.attrs.size          = buf_flush_offset + buf_flush_len;
      ie->file_extend_pending = 1;
    }
    int trc_idx = rozofs_trc_req_io(srv_rozofs_ll_ioctl,(fuse_ino_t)file_p,file_p->fid, ie->attrs.attrs.size,0);
    SAVE_FUSE_PARAM(fuse_ctx_p,trc_idx);
    /*
    ** insert the site number in the argument
    */
    arg.hdr.gateway_rank = rozofs_get_site_number(); 
//    severe("FDL site_number %d",arg.hdr.gateway_rank);
    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid = exportclt.eid;
    memcpy(arg.arg_gw.fid, file_p->fid, sizeof (fid_t));
    arg.arg_gw.bid = 0;
    arg.arg_gw.nrb = 1;
    arg.arg_gw.length = 0; //buf_flush_len;
    arg.arg_gw.offset = ie->attrs.attrs.size; //buf_flush_offset;
    
    /*
    ** PREVIOUSLY
    **____________
    ** - exportd did use the received (offset+length) as the new file size
    ** - rozofsmount used to set file size in offset and length to 0
    **
    ** NOW
    **____
    ** - exportd still uses the received (offset+length) as the new file size
    **   and if length is 1 records the write error occurence in the meta-data
    ** - rozofsmount still sets file size in offset and set length to 0 when no error
    **   and set file size minus one in offset and set length to 1 in case of error
    **
    ** This way any set of old/new rozofsmount/exportd are compatible.
    */
    if (file_p->wr_error) {
      arg.arg_gw.length = 1; /* Raise bit 1 of length filed to report write error */
      arg.arg_gw.offset--;
    }    
    
    arg.arg_gw.geo_wr_start = file_p->off_wr_start;
    arg.arg_gw.geo_wr_end = file_p->off_wr_end;
    /*
    ** now initiates the transaction towards the remote end
    */
    
#if 1
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,file_p->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_WRITE_BLOCK,(xdrproc_t) xdr_epgw_write_block_arg_t,(void *)&arg,
                              recv_cbk,fuse_ctx_p); 
#else
    ret = rozofs_export_send_common(&exportclt,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_WRITE_BLOCK,(xdrproc_t) xdr_epgw_write_block_arg_t,(void *)&arg,
                              recv_cbk,fuse_ctx_p); 
#endif
    if (ret == 0) {
      rozofs_geo_write_reset(file_p);
      ie->timestamp_wr_block = rozofs_get_ticker_us();
      /*
      ** indicates that the revelant file size will be found in the ientry
      */
      if (ie->file_extend_pending )       
      {
        ie->file_extend_pending = 0;
	ie->file_extend_size    = 0;
        ie->file_extend_running = 1;
     } 
    } 
    else
    {
      /*
      ** queue the ientry in the retry list of the wr_block. The insertion takes place
      ** where the exportd connectivity is lost during a switch-over
      */
      rozofs_export_queue_ie_in_wr_block_list(ie);
    }   
    return ret;  
}

/**
*  metadata update -> need to update the file size

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param parent : inode parent provided by rozofsmount
 @param name : name to search in the parent directory
 
 @retval none
*/

void export_write_block_nb(void *fuse_ctx_p, file_t *file_p) 
{
    int    ret;        
    void *buffer_p = fuse_ctx_p;
    int trc_idx;
    uint64_t buf_flush_offset ;
    uint32_t buf_flush_len ;
    ientry_t *ie;

    RESTORE_FUSE_PARAM(fuse_ctx_p,buf_flush_offset);
    RESTORE_FUSE_PARAM(fuse_ctx_p,buf_flush_len);
    
    ie = file_p->ie;
    /*
    ** adjust the size of the attributes of the local file
    */
    if ((buf_flush_offset + buf_flush_len) > ie->attrs.attrs.size)
    {
      ie->file_extend_size   += ((buf_flush_offset + buf_flush_len) - ie->attrs.attrs.size);        
      ie->attrs.attrs.size          = buf_flush_offset + buf_flush_len;
      ie->file_extend_pending = 1;
    }
    /*
    ** check if we need to push the new size on metadata server
    */
    if (ie->file_extend_pending == 1)
    {
       if ((rozofs_get_ticker_us()-ie->timestamp_wr_block) > rozofs_tmr_get(TMR_WR_BLOCK)*1000)
       {
         file_p->write_block_req = 1;
	 file_p->write_block_pending = 1;
       }
    
    }
    /*
    ** When mtime is locked, do not send any write block to prevent mtime modification
    */
    if (ie->mtime_locked) {
      file_p->write_block_req     = 0;
      file_p->write_block_pending = 0;
      goto out;
    }
    
    /*
    ** check if the update is requested
    */
    if (file_p->write_block_req == 0) 
    {
      /*
      ** assert the pending flag in order to trigger the writeblock on flush and release
      */
      file_p->write_block_pending = 1;
      goto out;
    }
    /*
    ** there is a writeblock request check if there is a pending request
    ** without pending request nothing takes place
    */
    if (file_p->write_block_pending ==0) 
    {
      file_p->write_block_req = 0;
      goto out;
    }
    /*
    ** clear the flag and update the metadata server
    */
    START_PROFILING_NB(buffer_p,rozofs_ll_ioctl);
    ret = export_write_block_asynchrone(fuse_ctx_p, file_p, export_write_block_cbk);
    if (ret < 0) goto error; 
    file_p->write_block_pending = 0;
    file_p->write_block_req = 0;
    return;
error:
    /*
    ** release the buffer if has been allocated
    */
    RESTORE_FUSE_PARAM(fuse_ctx_p,trc_idx);
    rozofs_trc_rsp(srv_rozofs_ll_ioctl,(fuse_ino_t)file_p,file_p->fid,(errno==0)?0:1,trc_idx);
    STOP_PROFILING_NB(fuse_ctx_p,rozofs_ll_ioctl);
out:
    if (fuse_ctx_p != NULL) rozofs_fuse_release_saved_context(fuse_ctx_p);
    return;
}
/**
*  metadata update -> need to update the file size

   Do not update file size in ientry since the last write is in error
   but send the write_block message to the export for the already written size on disk

   @param fuse_ctx_p: pointer to the fuse request context
   @param file_p    : open file context
 
   @retval none
*/
void export_write_block_error_nb(void *fuse_ctx_p, file_t *file_p) 
{
    int    ret;        
    void *buffer_p = fuse_ctx_p;
    int trc_idx;

    /*
    ** there is a writeblock request check if there is a pending request
    ** without pending request nothing takes place
    */
    if (file_p->write_block_pending ==0) 
    {
      file_p->write_block_req = 0;
      goto out;
    }
    /*
    ** clear the flag and update the metadata server
    */
    START_PROFILING_NB(buffer_p,rozofs_ll_ioctl);
    ret = export_write_block_asynchrone(fuse_ctx_p, file_p, export_write_block_cbk);
    if (ret < 0) goto error; 
    file_p->write_block_pending = 0;
    file_p->write_block_req = 0;
    return;
error:
    /*
    ** release the buffer if has been allocated
    */
    RESTORE_FUSE_PARAM(fuse_ctx_p,trc_idx);
    rozofs_trc_rsp(srv_rozofs_ll_ioctl,(fuse_ino_t)file_p,file_p->fid,(errno==0)?0:1,trc_idx);
    STOP_PROFILING_NB(fuse_ctx_p,rozofs_ll_ioctl);
out:
    if (fuse_ctx_p != NULL) rozofs_fuse_release_saved_context(fuse_ctx_p);
    return;
}


/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
 
void export_write_block_cbk(void *this,void *param) 
{
   epgw_mattr_ret_t ret ;
   struct rpc_msg  rpc_reply;

   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_mattr_ret_t;
   rozofs_fuse_save_ctx_t *fuse_ctx_p;
   errno = 0;
   int trc_idx;
   ientry_t *ie = 0;
    
   GET_FUSE_CTX_P(fuse_ctx_p,param);    
   RESTORE_FUSE_PARAM(param,trc_idx);

   rpc_reply.acpted_rply.ar_results.proc = NULL;
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
       ** queue the ientry on the write_block pending list for retrying later
       */
       rozofs_export_queue_ie_in_wr_block_list_from_fuse_ctx(param);
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
    bufsize = rozofs_tx_get_small_buffer_size();
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
    /*
    **  This gateway do not support the required eid 
    */    
    if (ret.status_gw.status == EP_FAILURE_EID_NOT_SUPPORTED) {    

        /*
        ** Do not try to select this server again for the eid
        ** but directly send to the exportd
        */
        expgw_routing_expgw_for_eid(&fuse_ctx_p->expgw_routing_ctx, ret.hdr.eid, EXPGW_DOES_NOT_SUPPORT_EID);       

        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    

        /* 
        ** Attempt to re-send the request to the exportd and wait being
        ** called back again. One will use the same buffer, just changing
        ** the xid.
        */
        status = rozofs_expgateway_resend_routing_common(rozofs_tx_ctx_p, NULL,param); 
        if (status == 0)
        {
          /*
          ** do not forget to release the received buffer
          */
          ruc_buf_freeBuffer(recv_buf);
          recv_buf = NULL;
          return;
        }           
        /*
        ** Not able to resend the request
        */
        errno = EPROTO; /* What else ? */
        goto error;
         
    }



    if (ret.status_gw.status == EP_FAILURE) {
        errno = ret.status_gw.ep_mattr_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    
    /*
    ** Update eid free quota
    */
    eid_set_free_quota(ret.free_quota);
    
        
    /*
    ** Update cache entry
    */
    ie = get_ientry_by_fid((unsigned char*) ret.status_gw.ep_mattr_ret_t_u.attrs.fid);
    if (ie) {
      /*
      ** update the attributes in the ientry
      */
      rozofs_ientry_update(ie,(struct inode_internal_t *)&ret.status_gw.ep_mattr_ret_t_u.attrs);  
      ie->file_extend_running = 0;
      /*
      ** check if some more write blocks are pending
      */
      rozofs_export_write_block_list_process();
    }
        
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
//out:

error:
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_ioctl,0/*ino*/,NULL,(errno==0)?0:1,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_ioctl);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    
    return;
}
