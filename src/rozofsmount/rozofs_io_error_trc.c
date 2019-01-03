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
 
#include <pthread.h>
#include <inttypes.h>
#include <assert.h>
#include <semaphore.h>
#include <mntent.h>
#include <sys/resource.h>
#include <rozofs/rozofs.h>
#include "rozofs_io_error_trc.h"
#include <rozofs/common/log.h>
#include <rozofs/common/common_config.h>
#include <rozofs/core/uma_dbg_api.h>


rozofs_io_data_t *rozofs_ioerr_context_p = NULL; /**< I/O error context */
/*
**__________________________________________
*/
/*  Debugging
*/
static char * show_ioerr_wr_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"ioerr_wr enable      : enable logging of I/O write errors\n");
  pChar += sprintf(pChar,"ioerr_wr disable     : disable logging of I/O write errors\n");
  pChar += sprintf(pChar,"ioerr_wr show [file] : show the I/O errors in either the current buffer or from file \n");
  pChar += sprintf(pChar,"ioerr_wr             : show current status\n");
  return pChar; 
}

#define ROZOFS_MAX_IOERR_ENTRY2SCAN 512
/*__________________________________________________________________________
*/
/**
*   Display of the content of the fuse trace buffer

    @param pChar : pointer to the result buffer
    @retval none
*/
void show_ioerr_wr_buffer_file(uint32_t tcpRef, void *bufRef,rozofs_io_data_t *ctx_p)
{
   char str[37];
   char date[128];
   char * pOut = uma_dbg_get_buffer();
   
   int count,i;
   int nb_entries;
   int ret;
   int readcount;
   char * pChar;
   char * pHead;
   int    max_size;
   void * nextBuff = NULL;
   rozofs_io_err_entry_t *entry_p;
   char pathname[1024];
   struct stat statbuf; 
   int fd=-1;
   rozofs_io_err_entry_t *entry_top_p;
     
   /*
   ** allocate a working buffer
   */
   entry_top_p = malloc(sizeof(rozofs_io_err_entry_t)*ROZOFS_MAX_IOERR_ENTRY2SCAN);
   if (entry_top_p == NULL)
   {
     sprintf(pOut,"cannot allocate memory %u: %s \n",(unsigned int) sizeof(rozofs_io_err_entry_t)*ROZOFS_MAX_IOERR_ENTRY2SCAN,strerror(errno));
     uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
     goto outerr;      
   }
   /*
   ** open the file
   */
   rozofs_iowr_build_pathname(pathname,rozofs_ioerr_context_p->instance);
   if ((fd = open(pathname, O_RDONLY| O_NOATIME, S_IRWXU)) < 0) {
     sprintf(pOut,"cannot open %s: %s \n",pathname,strerror(errno));
     uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
     goto outerr;

   }
   /*
   ** Get the file size
   */
   ret = fstat(fd, &statbuf);    
   if (ret < 0)
   {
     sprintf(pOut,"cannot stat %s: %s \n",pathname,strerror(errno));
     uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
     goto outerr;      
   }
   nb_entries = statbuf.st_size/sizeof(rozofs_io_err_entry_t);
   if (nb_entries == 0)
   {
     sprintf(pOut,"file %s is empty \n",pathname);
     uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
     goto outerr;
   }
   /* 
   ** Retrieve the buffer payload 
   */
   if ((pHead = (char *)ruc_buf_getPayload(bufRef)) == NULL) {
     severe( "ruc_buf_getPayload(%p)", bufRef );
     /* Let's tell the caller fsm that the message is sent */
     goto outerr;;
   }  
   max_size = ruc_buf_getMaxPayloadLen(bufRef)-1024;
 
   /*
   ** Set the command recell string
   */
   pChar = uma_dbg_cmd_recall((UMA_MSGHEADER_S *)pHead);   
   pChar+=sprintf(pChar,"nb entries   : %u\n",(unsigned int)nb_entries);
   pChar +=sprintf(pChar,"|                      FID             |     offset     |  length  |             date           |    error_code                            |\n");   
   pChar +=sprintf(pChar,"+--------------------------------------+----------------+----------+----------------------------+------------------------------------------+\n");
   
   while(nb_entries !=0)
   {
      /*
      ** read data in the fake buffer
      */
      if (nb_entries <=  ROZOFS_MAX_IOERR_ENTRY2SCAN) readcount = nb_entries;
      else readcount = ROZOFS_MAX_IOERR_ENTRY2SCAN;
      
      nb_entries -=readcount;
      
      ret = read(fd,entry_top_p,sizeof(rozofs_io_err_entry_t)*readcount);
      if (ret < 0)
      {
        count = 0;
      }
      else
      {
        count = ret/sizeof(rozofs_io_err_entry_t);
      }
      /*
      ** set the pointer to the first entry
      */
      entry_p = entry_top_p;   
      /*
      ** loop on the buffer
      */    
      for (i = 0; i < count; i++,entry_p++)
      {

	  /*
	  ** Flush current buffer when almost full
	  */
	  if ((pChar - pHead) >= max_size) {
            *pChar = 0;

            /*
            ** Allocate a next buffer
            */
            nextBuff = uma_dbg_get_new_buffer(tcpRef);
            if (nextBuff==NULL) {
              pChar += sprintf(pChar,"\n\n!!! Buffer depletion !!!\n");
              break;
            }
            /*
            ** Send this buffer
            */
            uma_dbg_send_buffer(tcpRef, bufRef, pChar-pHead, FALSE);         
            /*
            ** Allocate a new one
            */
            bufRef = nextBuff;
            if ((pHead = (char *)ruc_buf_getPayload(bufRef)) == NULL) {
              severe( "ruc_buf_getPayload(%p)", bufRef );
              /* Let's tell the caller fsm that the message is sent */
              goto outerr;
            }  
            max_size = ruc_buf_getMaxPayloadLen(bufRef)-1024;
            pChar = pHead + sizeof(UMA_MSGHEADER_S);
            *pChar = 0;          
	  }
	  /*
	  ** put code here
	  */ 
	  rozofs_uuid_unparse(entry_p->fid, str);
	  ctime_r((const time_t *)&rozofs_ioerr_context_p->last_write_time,date);
	  {
             int len;
	     len = strlen(date);
	      if (len > 0) date[len-1] = 0;
	  }
	  pChar +=sprintf(pChar,"| %s | %14llu | %8u |  %s  | %40s |\n",str,(long long unsigned int)entry_p->off,entry_p->len,date,strerror(entry_p->error)); 

      }
   }
   /*
   ** Send last buffer
   */
   uma_dbg_send_buffer(tcpRef, bufRef, pChar-pHead, TRUE);         

   
   
outerr:
   if (entry_top_p != NULL) free(entry_top_p);
   if (fd != -1) close(fd);

}


/*__________________________________________________________________________
*/
/**
*   Display of the content of the fuse trace buffer

    @param pChar : pointer to the result buffer
    @retval none
*/
void show_ioerr_wr_buffer(uint32_t tcpRef, void *bufRef,rozofs_io_data_t *ctx_p)
{
   char str[37];
   char date[128];
   int count,i;

   char * pChar;
   char * pHead;
   int    max_size;
   void * nextBuff = NULL;
   rozofs_io_err_entry_t *entry_p;
   
   /* 
   ** Retrieve the buffer payload 
   */
   if ((pHead = (char *)ruc_buf_getPayload(bufRef)) == NULL) {
     severe( "ruc_buf_getPayload(%p)", bufRef );
     /* Let's tell the caller fsm that the message is sent */
     return;
   }  
   max_size = ruc_buf_getMaxPayloadLen(bufRef)-1024;
   /*
   ** set the pointer to the first entry
   */
   entry_p = ctx_p->io_err_buf_p;     
   /*
   ** Set the command recell string
   */
   pChar = uma_dbg_cmd_recall((UMA_MSGHEADER_S *)pHead);   
   pChar+=sprintf(pChar,"nb entries   : %u\n",(unsigned int)ctx_p->cur_idx);
   pChar +=sprintf(pChar,"|                      FID             |     offset     |  length  |             date           |    error_code                            |\n");   
   pChar +=sprintf(pChar,"+--------------------------------------+----------------+----------+----------------------------+------------------------------------------+\n");
   count = ctx_p->cur_idx;

   
   for (i = 0; i < count; i++,entry_p++)
   {
    
       /*
       ** Flush current buffer when almost full
       */
       if ((pChar - pHead) >= max_size) {
         *pChar = 0;
         
         /*
         ** Allocate a next buffer
         */
         nextBuff = uma_dbg_get_new_buffer(tcpRef);
         if (nextBuff==NULL) {
           pChar += sprintf(pChar,"\n\n!!! Buffer depletion !!!\n");
           break;
         }
         /*
         ** Send this buffer
         */
         uma_dbg_send_buffer(tcpRef, bufRef, pChar-pHead, FALSE);         
         /*
         ** Allocate a new one
         */
         bufRef = nextBuff;
         if ((pHead = (char *)ruc_buf_getPayload(bufRef)) == NULL) {
           severe( "ruc_buf_getPayload(%p)", bufRef );
           /* Let's tell the caller fsm that the message is sent */
           return;
         }  
         max_size = ruc_buf_getMaxPayloadLen(bufRef)-1024;
         pChar = pHead + sizeof(UMA_MSGHEADER_S);
         *pChar = 0;          
       }
       /*
       ** put code here
       */ 
       rozofs_uuid_unparse(entry_p->fid, str);
       ctime_r((const time_t *)&rozofs_ioerr_context_p->last_write_time,date);
       {
          int len;
	  len = strlen(date);
	   if (len > 0) date[len-1] = 0;
       }
       pChar +=sprintf(pChar,"| %s | %14llu | %8u |  %s  | %40s |\n",str,(long long unsigned int)entry_p->off,entry_p->len,date,strerror(entry_p->error)); 
      
   }
   /*
   ** Send last buffer
   */
   uma_dbg_send_buffer(tcpRef, bufRef, pChar-pHead, TRUE);         
   return;
}
/*
**__________________________________________
*/
/**
*   IO ERROR Write main debugging entrypoint

*/

void show_ioerr_wr(char * argv[], uint32_t tcpRef, void *bufRef)
{
  char bufall[128];
  char * pChar = uma_dbg_get_buffer();
  char pathname[1024];
  struct stat statbuf;
  int ret;

  if (argv[1] !=NULL)
  {
    if (strcmp(argv[1],"enable")==0)  
    {
      rozofs_ioerr_context_p->valid = 1;
      sprintf(pChar,"I/O error write logging is now enabled");
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;
    }
    if (strcmp(argv[1],"disable")==0)  
    {
      rozofs_ioerr_context_p->valid = 0;
      sprintf(pChar,"I/O error write logging is now enabled");
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;
    }
    if (strcmp(argv[1],"show")==0)  
    {
        if (argv[2] !=NULL)
	{
	  if (strcmp(argv[2],"file")==0)  
	  show_ioerr_wr_buffer_file(tcpRef,bufRef,rozofs_ioerr_context_p);
	  return;	
	}
	else
	{
	  show_ioerr_wr_buffer(tcpRef,bufRef,rozofs_ioerr_context_p);
	  return;	
	}
      if (rozofs_ioerr_context_p->cur_idx == 0)
      {
	sprintf(pChar,"No write error(s) registered\n");
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer()); 
	return;
      } 
      show_ioerr_wr_buffer(tcpRef,bufRef,rozofs_ioerr_context_p);
      return;
    }
    pChar = show_ioerr_wr_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;
  }
  else
  {
    pChar += sprintf(pChar,"status        : %s\n",rozofs_ioerr_context_p->valid==0?"Disabled":"Enabled");
    pChar += sprintf(pChar,"nb entries    : %u\n",rozofs_ioerr_context_p->nb_entries);
    pChar += sprintf(pChar,"cur index     : %u\n",rozofs_ioerr_context_p->cur_idx);
    ctime_r((const time_t *)&rozofs_ioerr_context_p->last_write_time,bufall);
    pChar += sprintf(pChar,"last write    : %s\n",bufall);
    rozofs_iowr_build_pathname(pathname,rozofs_ioerr_context_p->instance);
    pChar += sprintf(pChar,"path          : %s\n",pathname);
    ret = stat((const char *)pathname, &statbuf);    
    if (ret < 0)
    {
      pChar += sprintf(pChar,"file entries  : %s\n",strerror(errno));
    }
    else
    {
      pChar += sprintf(pChar,"file entries  : %u\n",(unsigned int)(statbuf.st_size/sizeof(rozofs_io_err_entry_t)));
    }
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;
  }

}
/*
**__________________________________________
*/
void *rozofs_ioerr_write_thread(void *arg) {
 
 struct timespec ts = {CONF_IOERR_THREAD_TIMESPEC, 0};
 pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
 int minutes_counter = 0;
 int flush = 0;
 char bufname[1024];
 int fd = -1;
 ssize_t count;
 

  rozofs_io_data_t * ctx_p = (rozofs_io_data_t*)arg;
  rozofs_iowr_build_pathname(bufname,ctx_p->instance);

  uma_dbg_thread_add_self("I/O_Err_th");
  
  while(1)
  {
      nanosleep(&ts, NULL); 
      minutes_counter++;
      if (minutes_counter > CONF_IOERR_THREAD_DEADLINE_COUNT)
      {
         minutes_counter = 0;
	 if (ctx_p->cur_idx != 0) flush = 1;  
      }
      /*
      ** Check if the buffer is almost full
      */
      if (ctx_p->cur_idx > (ROZOFS_IO_ERROR_MAX_ENTRY - 32))
      {
        flush = 1;
      }
      /*
      ** Check if the buffer must be flushed on disk
      */
      if (flush)
      {
        flush = 0;
        if ((fd = open(bufname, O_WRONLY | O_CREAT | O_NOATIME| O_APPEND, S_IRWXU)) < 0) {
	  severe("cannot open %s: %s ",bufname,strerror(errno));
	  continue;
	
	}
	if ((errno = pthread_rwlock_wrlock
              (&rozofs_ioerr_context_p->io_lock)) != 0) {
               severe("pthread_rwlock_wrlock failed (invalidate the I/O error logging): %s", strerror(errno));
	       close(fd);
	       fd= -1;
               rozofs_ioerr_context_p->valid = 0;
	       return 0;
	}	
        /*
	** Write on disk
	*/
	count = write(fd,ctx_p->io_err_buf_p,(ctx_p->cur_idx)*sizeof(rozofs_io_err_entry_t));
	if (count != (ctx_p->cur_idx)*sizeof(rozofs_io_err_entry_t))
	{	
	  warning("write error on %s: %s ",bufname,strerror(errno));	
	}
	/*
	** reinit on the fisrt index
	*/
	ctx_p->cur_idx = 0;

	/*
	** log the last time write
	*/
	ctx_p->last_write_time = time(NULL);
	if ((errno = pthread_rwlock_unlock
              (&rozofs_ioerr_context_p->io_lock)) != 0) {
              severe("pthread_rwlock_unlock failed (invalidate the I/O error logging): %s", strerror(errno));
               rozofs_ioerr_context_p->valid = 0;
	       close(fd);
	       fd= -1;	       
	       return 0;
	}  

      }  
  }
}
/*
**__________________________________________
*/
/**
  Init of the I/O write error tracing
  
  @param instance: rozofsmount instance
  
  @retval 0 on success
  @retval -1 on error (see errno for details)
  
*/

int rozofs_iowr_err_init(int instance)
{
   
   rozofs_ioerr_context_p = malloc(sizeof(rozofs_io_data_t));
   if (rozofs_ioerr_context_p == NULL)
   {
     errno = ENOMEM;
     return -1;
   }
   memset(rozofs_ioerr_context_p,0,sizeof(rozofs_io_data_t));
   /*
   ** allocate the buffer for error logging
   */
   rozofs_ioerr_context_p->io_err_buf_p = malloc(sizeof(rozofs_io_err_entry_t)*ROZOFS_IO_ERROR_MAX_ENTRY);
   if (rozofs_ioerr_context_p->io_err_buf_p == NULL)
   {
     errno = ENOMEM;
     return -1;   
   }
   rozofs_ioerr_context_p->nb_entries = ROZOFS_IO_ERROR_MAX_ENTRY;
   /*
   ** init of the mutex
   */
   if ((errno = pthread_rwlock_init(&rozofs_ioerr_context_p->io_lock, NULL)) != 0) {
     return -1;
   }
   rozofs_ioerr_context_p->last_write_time = time(NULL);
   /*
   ** Create the thread that writes the I/O buffer on disk
   */
   if ((errno = pthread_create(&rozofs_ioerr_context_p->ioerr_thread, NULL,
        rozofs_ioerr_write_thread, rozofs_ioerr_context_p)) != 0) {
    severe("can't create rozofs_ioerr_write_thread: %s", strerror(errno));
    return -1;
   }
   rozofs_ioerr_context_p->valid = 1;
   uma_dbg_addTopic("ioerr_wr",show_ioerr_wr);
   return 0;
}
   
   
/*
**__________________________________________
*/
/**
   Log a write error in the internal buffer
   
   @param fid: file of the file
   @param off: offset in the file
   @param len: expected length to write
   @param error : error code returned
   
   @retval none
*/
void rozofs_iowr_err_log(fid_t fid,off_t off, uint32_t len,int error)
{
  rozofs_io_err_entry_t *entry_p;

  if (rozofs_ioerr_context_p == NULL) return;
  if (rozofs_ioerr_context_p->valid == 0) return;
  
  if ((errno = pthread_rwlock_wrlock
        (&rozofs_ioerr_context_p->io_lock)) != 0) {
        severe("pthread_rwlock_wrlock failed (invalidate the I/O error logging): %s", strerror(errno));
         rozofs_ioerr_context_p->valid = 0;
	 return ;
  }
  entry_p = rozofs_ioerr_context_p->io_err_buf_p;
  entry_p +=rozofs_ioerr_context_p->cur_idx;
  memcpy(entry_p->fid,fid,sizeof(fid_t));
  entry_p->off = off;
  entry_p->len = len;
  entry_p->time = time(NULL);
  entry_p->error = error;
  rozofs_ioerr_context_p->cur_idx++;
  if (rozofs_ioerr_context_p->cur_idx >= ROZOFS_IO_ERROR_MAX_ENTRY) 
  {
    rozofs_ioerr_context_p->cur_idx = ROZOFS_IO_ERROR_MAX_ENTRY-1;
  }
  
  if ((errno = pthread_rwlock_unlock
        (&rozofs_ioerr_context_p->io_lock)) != 0) {
        severe("pthread_rwlock_unlock failed (invalidate the I/O error logging): %s", strerror(errno));
         rozofs_ioerr_context_p->valid = 0;
	 return ;
  }  
}


/*
**__________________________________________
*/
/**
   enable write error logging
  
   
   @retval none
*/
void rozofs_iowr_err_enable()
{
  if (rozofs_ioerr_context_p == NULL) return;
  rozofs_ioerr_context_p->valid =1;

}

/*
**__________________________________________
*/
/**
   disable write error logging
  
   
   @retval none
*/
void rozofs_iowr_err_disable()
{
  if (rozofs_ioerr_context_p == NULL) return;
  rozofs_ioerr_context_p->valid =0;
}
