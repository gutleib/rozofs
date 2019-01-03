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
 
#ifndef ROZOFS_IO_ERROR_TRC_H
#define ROZOFS_IO_ERROR_TRC_H

#include <pthread.h>
#include <inttypes.h>
#include <assert.h>
#include <semaphore.h>
#include <mntent.h>
#include <sys/resource.h>
#include <rozofs/rozofs.h>


#define ROZOFS_IO_ERROR_MAX_ENTRY (256*1024)/sizeof(rozofs_io_err_entry_t)
#define CONF_IOERR_THREAD_TIMESPEC (60)
#define CONF_IOERR_THREAD_DEADLINE_COUNT (5)



typedef struct _rozofs_io_data_t
{
   int valid;    /**< when asserted it indicates that rozofsmount logs the I/O write error */
   rozofs_io_err_entry_t *io_err_buf_p;
   pthread_rwlock_t io_lock;  /**< mutex on the buffer */
   uint32_t cur_idx;          /**< current index in the buffer */
   uint32_t nb_entries;       /**< nb entries in the buffer    */
   int      instance;         /**< rozofsmount instance        */
   uint64_t last_write_time;  /**< last write time             */ 
   pthread_t ioerr_thread;    /**< PID of the thread that logs error on disk */
} rozofs_io_data_t;   





/*
**__________________________________________
*/
/**
  Init of the I/O write error tracing
  
  @param instance: rozofsmount instance
  
  @retval 0 on success
  @retval -1 on error (see errno for details)
  
*/

int rozofs_iowr_err_init(int instance);
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
void rozofs_iowr_err_log(fid_t fid,off_t off, uint32_t len,int error);
/*
**__________________________________________
*/
/**
   enable write error logging
  
   
   @retval none
*/
void rozofs_iowr_err_enable();
/*
**__________________________________________
*/
/**
   disable write error logging
  
   
   @retval none
*/
void rozofs_iowr_err_disable();

/**
*  Build the filename of the io error write log file

   @param instance: rozofsmount instance
   @param buffer: pointer to the returned buffer
   
   @retval buffer: pointer to the buffer that contains the pathname
*/
static inline char *rozofs_iowr_build_pathname(char *buffer,int instance)
{
    sprintf(buffer,"/var/log/rozofsmount_%d_ioerr_wr.log",instance);
    return buffer;
}
#endif
