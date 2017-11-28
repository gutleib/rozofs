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

#define _XOPEN_SOURCE 500
#define STORAGE_C

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <inttypes.h>
#include <glob.h>
#include <fnmatch.h>
#include <dirent.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <mntent.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/list.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/rozofs_string.h>
#include "storio_cache.h"
#include "storio_bufcache.h"
#include "storio_device_mapping.h"
#include "storio_device_mapping.h"
#include "storio_crc32.h"


int      re_enumration_required=0; 
time_t   storio_last_enumeration_date;


/*_______________________________________________________________________
* REPAIT STATISTICS
*
*/
typedef struct storio_repair_stat_t {
  uint64_t   file_error;
  uint64_t   nb_requests;
  uint64_t   nb_blocks_requested;  
  uint64_t   nb_blocks_attempted;
  uint64_t   nb_blocks_success;
} storio_repair_stat_t;
storio_repair_stat_t storio_repair_stat = { 0 };
/*_______________________________________________________________________
*  man
*
*/
void storio_repair_stat_man(char * pChar) {
  pChar += rozofs_string_append(pChar,"usage:\nprofiler reset       : reset statistics\nprofiler             : display statistics\n");  
}
/*
**____________________________________________________
** Display counters 
**
*/
char * storio_repair_stat_show(char * pChar) {  

  pChar += rozofs_string_append(pChar, "{ \"repair\" : {\n");
  pChar += rozofs_string_append(pChar, "    \"file errors\" : ");
  pChar += rozofs_u64_append(pChar, storio_repair_stat.file_error);
  pChar += rozofs_string_append(pChar, ",\n      \"requests\"  : ");    
  pChar += rozofs_u64_append(pChar, storio_repair_stat.nb_requests);
  pChar += rozofs_string_append(pChar, ",\n      \"block requested\" : ");    
  pChar += rozofs_u64_append(pChar, storio_repair_stat.nb_blocks_requested);
  pChar += rozofs_string_append(pChar, ",\n      \"block attempted\" : ");    
  pChar += rozofs_u64_append(pChar, storio_repair_stat.nb_blocks_attempted);
  pChar += rozofs_string_append(pChar, ",\n      \"block success\" : ");    
  pChar += rozofs_u64_append(pChar, storio_repair_stat.nb_blocks_success);
  pChar += rozofs_string_append(pChar, "\n  }\n}\n");  
  
  return pChar;
  
}
/*_______________________________________________________________________
*  cli
*
*/
void storio_repair_stat_cli(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();

  if (argv[1] != NULL) {

    /*
    ** Reset counter
    */
    if (strcasecmp(argv[1],"reset")==0) {
      pChar = storio_repair_stat_show(pChar);
      memset(&storio_repair_stat,0, sizeof(storio_repair_stat));     
    }

    /*
    ** Help
    */      
    else {
      storio_repair_stat_man(pChar);  
    }	 
  }  
  else {
    pChar = storio_repair_stat_show(pChar);    
  } 
  
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}

/*
=================== STORIO LOG SERVICE ====================================

This service logs in a small buffer the first encountered errors.
The logg cn be displayed through rozodiag
The log can be reset through rozodiag

===========================================================================
*/
int storio_error_log_initialized = 0;

/*
** A device access error record
*/
typedef struct _storio_device_error_log_record_t {
  fid_t            fid;
  uint16_t         nb_blocks;
  uint8_t          chunk;
  uint8_t          device;
  uint32_t         line;
  uint32_t         bid;
  uint32_t         error;
  uint64_t         ts;
  char             string[16];
} storio_device_error_log_record_t;

/*
** Dimensionning factor of the log
*/
#define STORIO_DEVICE_ERROR_LOG_MAX_RECORD      256

typedef struct _storio_device_error_log {
  pthread_rwlock_t                  lock; 
  uint32_t                          next_record;
  storio_device_error_log_record_t  record[STORIO_DEVICE_ERROR_LOG_MAX_RECORD];
} storio_device_error_log_t;

storio_device_error_log_t storio_device_error_log;


/*_______________________________________________________________________
* Write an error in the next log entry
*
* @param fid        The FID tha has encountered an error
* @param line       The line where the error is called
* @param device     The device on which the error occured
* @param chunk      The chunk of the FID on which the   error occured
* @param bid        The block where the error occured
* @param nb_blocks  The block of the chunk where the error occured
* @param error      The errno
* @param string     A string
*
*/
void storio_device_error_log_new(fid_t fid, int line, uint8_t device, uint8_t chunk, uint32_t bid, uint16_t nb_blocks, uint32_t error, char * string) {
  uint32_t                          record_nb;
  storio_device_error_log_record_t *p ;
  
  /*
  ** Service must have been initialized
  */ 
  if (storio_error_log_initialized==0) return;
   
  /*
  ** No space left in the log
  */
  if (storio_device_error_log.next_record >= STORIO_DEVICE_ERROR_LOG_MAX_RECORD) return;

  /*
  ** Take the lock
  */
  if (pthread_rwlock_wrlock(&storio_device_error_log.lock) != 0) {
    return;
  }
  
  /*
  ** Get a record number
  */ 
  record_nb = storio_device_error_log.next_record;
  storio_device_error_log.next_record++;
  
  /*
  ** release the lock
  */
  pthread_rwlock_unlock(&storio_device_error_log.lock);  

  /*
  ** Check  the record number again
  */
  if (record_nb >= STORIO_DEVICE_ERROR_LOG_MAX_RECORD) return;
  
  p = &storio_device_error_log.record[record_nb];
  memcpy(p->fid, fid,sizeof(fid_t));
  p->nb_blocks   = nb_blocks;
  p->line        = line;
  p->device      = device;
  p->chunk       = chunk;
  p->bid         = bid;
  p->error       = error;
  p->ts          = time(NULL);
  if (string == NULL) {
    p->string[0] = 0;
  }
  else {
    int i;
    for (i=0; i<15; i++,string++) {
      p->string[i] = *string;
      if (*string == 0) break;
    }
    p->string[i] = 0;
  }
}
/*_______________________________________________________________________
* Reset the error log
*/
void storio_device_error_log_reset() {
   
  /*
  ** Take the lock
  */
  if (pthread_rwlock_wrlock(&storio_device_error_log.lock) != 0) {
    return;
  }
  
  /*
  ** Reset the log
  */ 
  storio_device_error_log.next_record = 0;
  memset(&storio_device_error_log.record[0],0,sizeof(storio_device_error_log_record_t)*STORIO_DEVICE_ERROR_LOG_MAX_RECORD);

  /*
  ** release the lock
  */
  pthread_rwlock_unlock(&storio_device_error_log.lock);  
}

/*_______________________________________________________________________
* Display the error log
*/
void storio_device_error_log_man (char * pChar) {
  pChar += rozofs_string_append(pChar,"log         : Display a internal log of encountered errors.\n");
  pChar += rozofs_string_append(pChar,"For each error logged is displayed:\n"); 
  pChar += rozofs_string_append(pChar," - the FID,\n");
  pChar += rozofs_string_append(pChar," - the line in storage.c where the error was detected,\n");
  pChar += rozofs_string_append(pChar," - the device,\n");
  pChar += rozofs_string_append(pChar," - the chunk,\n");
  pChar += rozofs_string_append(pChar," - the block identifier,\n");
  pChar += rozofs_string_append(pChar," - the number of blocks of the request,\n");
  pChar += rozofs_string_append(pChar," - the date\n");
  pChar += rozofs_string_append(pChar,"\nlog reset : reset the log buffer\n");  
}
/*_______________________________________________________________________
* Display the error log
*/
void storio_device_error_log_display (char * argv[], uint32_t tcpRef, void *bufRef) {
  char                             * pChar = uma_dbg_get_buffer();
  int                                idx;
  int                                nb_record = storio_device_error_log.next_record;
  storio_device_error_log_record_t * p = storio_device_error_log.record;
  struct tm                          ts;
  char                             * line_sep = "+-----+--------------------------------------+-------+-----+-----+-----------+-----------+-------------------+-----------------+--------------------------------+\n";
  
    
  pChar += rozofs_string_append(pChar,"nb log  : ");
  pChar += rozofs_u32_append(pChar,nb_record);
  *pChar++ = '/';
  pChar += rozofs_u32_append(pChar,STORIO_DEVICE_ERROR_LOG_MAX_RECORD);
  pChar += rozofs_eol(pChar);   

  if (nb_record == 0) {
    uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());
    return;
  } 

  pChar += rozofs_string_append(pChar,"git ref : ");
  pChar += rozofs_string_append(pChar,ROZO_GIT_REF);
  pChar += rozofs_eol(pChar);

  pChar += rozofs_string_append(pChar, line_sep);
  pChar += rozofs_string_append(pChar,"|  #  |                FID                   | line  | dev | chk |  block id |  nb block |   time stamp      |      action     |          error                 |\n");
  pChar += rozofs_string_append(pChar, line_sep);

  for (idx=0; idx < nb_record; idx++,p++) {
    *pChar++ = '|';
    pChar += rozofs_u32_padded_append(pChar, 4, rozofs_right_alignment, idx);   
    pChar += rozofs_string_append(pChar," | ");
    rozofs_uuid_unparse(p->fid, pChar);
    pChar += 36;
    *pChar++ = ' '; *pChar++ = '|';      
    pChar += rozofs_u32_padded_append(pChar, 6, rozofs_right_alignment, p->line);
    *pChar++ = ' '; *pChar++ = '|';  
    pChar += rozofs_u32_padded_append(pChar, 4, rozofs_right_alignment, p->device); 
    *pChar++ = ' '; *pChar++ = '|';
    pChar += rozofs_u32_padded_append(pChar, 4, rozofs_right_alignment, p->chunk); 
    *pChar++ = ' '; *pChar++ = '|';
    if (p->bid==-1) {
      pChar += rozofs_string_append(pChar, "           |");    
    }
    else {
      pChar += rozofs_u32_padded_append(pChar, 10, rozofs_right_alignment, p->bid);
      *pChar++ = ' '; *pChar++ = '|'; 
    }
    
    pChar += rozofs_u32_padded_append(pChar, 10, rozofs_right_alignment, p->nb_blocks);
    *pChar++ = ' '; *pChar++ = '|'; *pChar++ = ' ';
    
    time_t t = p->ts;
    ts = *localtime(&t);
    pChar += rozofs_u32_append(pChar, ts.tm_year-100);
    *pChar++ = '/';
    pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, ts.tm_mon+1);
    *pChar++ = '/';
    pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, ts.tm_mday);
    *pChar++ = ' ';
    pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, ts.tm_hour);
    *pChar++ = ':';
    pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, ts.tm_min);
    *pChar++ = ':';
    pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, ts.tm_sec);
    *pChar++ = ' '; *pChar++ = '|';*pChar++ = ' ';
    
    pChar += rozofs_string_padded_append(pChar, 16,rozofs_left_alignment,p->string);
    *pChar++ = '|';*pChar++ = ' ';
    
    pChar += rozofs_string_padded_append(pChar, 31, rozofs_left_alignment, strerror(p->error));
    *pChar++ = '|'; *pChar++ = ' ';

    pChar += rozofs_eol(pChar);
    

  } 
  pChar += rozofs_string_append(pChar, line_sep);
  
  
  /*
  ** Reset log when requested
  */
  if ((argv[1] != NULL) && (strcmp(argv[1],"reset")==0)) {
    storio_device_error_log_reset();
    pChar += rozofs_string_append(pChar, "Reset Done\n");
  }

  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());

}
/*_______________________________________________________________________
* Initialize the storio error log service
*
*/
void storio_device_error_log_init(void) {
 
  if (storio_error_log_initialized) return ;
  
  memset((char*)&storio_device_error_log,0,sizeof(storio_device_error_log_t));

  if (pthread_rwlock_init(&storio_device_error_log.lock, NULL) != 0) {
    severe("pthread_rwlock_init %s",strerror(errno));
  }
  
  uma_dbg_addTopicAndMan("log", storio_device_error_log_display,storio_device_error_log_man,0);
  
  storio_error_log_initialized = 1;
}


#define storio_fid_error(fid,dev,chunk,bid,nb_blocks,string) storio_device_error_log_new(fid, __LINE__, dev, chunk, bid, nb_blocks, errno,string)
#define storio_hdr_error(fid,dev,string)                     storio_device_error_log_new(fid, __LINE__, dev, 0, -1, -1, errno,string) 

/*
=================== END OF STORIO LOG SERVICE ====================================
*/




static inline void storage_get_projection_size(uint8_t spare, 
                                               sid_t sid, 
					       uint8_t layout, 
					       uint32_t bsize,
					       sid_t * dist_set,
					       uint16_t * msg,
				    	       uint16_t * disk) { 
  int prj_id;
  int idx;
  int safe;
  int forward;
  char mylog[128];
  char * p = mylog;
    
  /* Size of a block in a message received from the client */  
  *msg = rozofs_get_max_psize_in_msg(layout,bsize);
  
  /*
  ** On a spare storage, we store the projections as received.
  ** That is one block takes the maximum projection block size.
  */
  if (spare) {
    *disk = *msg;
    return;
  }
    
  /*
  ** On a non spare storage, we store the projections on its exact size.
  */
  
  forward = rozofs_get_rozofs_forward(layout);
  safe    = rozofs_get_rozofs_safe(layout);

  /* Retrieve the current sid in the given distribution */
  for (prj_id=0; prj_id < safe; prj_id++) {
    if (sid == dist_set[prj_id]) break;
  }
  
  /* The sid is within the forward 1rst sids : this is what we expected */
  if (prj_id < forward) {
    *disk = rozofs_get_psizes_on_disk(layout,bsize,prj_id);
    return;
  }	  

  /* This is abnormal. The sid is not supposed to be spare */
  p += rozofs_string_append(p, " safe ");
  p += rozofs_u32_append(p,safe);
  for (idx=0; idx < safe; idx++) {
    *p++ = '/';
    p += rozofs_u32_append(p,dist_set[idx]);    
  }    
  p += rozofs_string_append(p, " storage_get_projection_size spare ");
  p += rozofs_u32_append(p,spare);
  p += rozofs_string_append(p, " sid ");
  p += rozofs_u32_append(p,sid);

  if (prj_id < safe) {
    /* spare should have been set to 1 !? */
    severe("%s",mylog);
    *disk = *msg;
    return;
  }
  
  /* !!! sid not in the distribution !!!! */
  fatal("%s",mylog);	
}  
static inline void storage_get_projid_size(uint8_t spare, 
                                           uint8_t prj_id, 
					   uint8_t layout,
					   uint32_t bsize,
					   uint16_t * msg,
				    	   uint16_t * disk) { 

  *msg = rozofs_get_max_psize_in_msg(layout,bsize);
  
  /*
  ** On a spare storage, we store the projections as received.
  ** That is one block takes the maximum projection block size.
  */
  if (spare) {
    *disk = *msg;
    return;
  }
    
  /*
  ** On a non spare storage, we store the projections on its exact size.
  */
  *disk = rozofs_get_psizes_on_disk(layout,bsize,prj_id);		
} 





/*_________________________________________________________________________
**  Write a header file on disk and eventually truncate it when the file is longer
**  on disk
**
**  @param st           The logical storage context
**  @param dev          The device number to write on (for trace only)
**  @param path         The full path of the device
**  @param hdr          The header file content to write
**  
**  @retval 0 on sucess. -1 on failure
**_________________________________________________________________________
*/
int storage_write_header_file(storage_t * st, int dev, char * path, rozofs_stor_bins_file_hdr_t * hdr) {
  size_t                    nb_write;
  int                       fd;
  char                      my_path[FILENAME_MAX];
  char                     *pChar;
  struct stat               buf;
  int                       size2write;
     
  /*
  ** Create directory when needed */
  if (storage_create_dir(path) < 0) {   
    storio_hdr_error(hdr->fid,dev,"create dir");
    storage_error_on_device(st,dev);
    return -1;
  }      
      
  strcpy(my_path,path); // Not to modify input path
  pChar = my_path;
  pChar += strlen(my_path);
  rozofs_uuid_unparse_no_recycle(hdr->fid, pChar);

  size2write = rozofs_st_get_header_file_size(hdr);
  if (size2write < 0) {
    severe("rozofs_st_get_header_file_size version %d",hdr->version);
    return -1;
  }  
 
  
  // Open bins file
  fd = open(my_path, ROZOFS_ST_BINS_FILE_FLAG, ROZOFS_ST_BINS_FILE_MODE);
  if (fd < 0) {	
    storio_hdr_error(hdr->fid, dev,"open hdr write");
    storage_error_on_device(st,dev);    
    return -1;
  }      

  
  // Write the header for this bins file
  nb_write = pwrite(fd, hdr, size2write, 0);
  if (nb_write != size2write) {
    storio_hdr_error(hdr->fid, dev,"write hdr");
    storage_error_on_device(st,dev);  
    close(fd);
    return -1;
  }
  
  // Truncate the file to the right size when it is too long
  if (fstat(fd, &buf) == 0) {
    if (buf.st_size > size2write) {
      if (ftruncate(fd,size2write)<0) {};
    }
  }
  close(fd);
  return 0;
}  
/*
 ** Write a header/mapper file on every device
    This function writes the header file of the given FID on every
    device where it should reside on this storage.    
    
  @param st    : storage we are looking on
  @param fid   : fid whose hader file we are looking for
  @param spare : whether this storage is spare for this FID
  @param hdr   : the content to be written in header file
  
  @retval The number of header file that have been written successfuly
  
 */
int storage_write_all_header_files(storage_t * st, fid_t fid, uint8_t spare, rozofs_stor_bins_file_hdr_t * hdr) {
  int                       dev;
  int                       hdrDevice;
  int                       storage_slice;
  char                      path[FILENAME_MAX];
  int                       result=0;
  
  storage_slice = rozofs_storage_fid_slice(fid);
  
  /*
  ** Compute CRC32
  */
  uint32_t crc32 = fid2crc32((uint32_t *)fid);  
  storio_gen_header_crc32(hdr,crc32);

  for (dev=0; dev < st->mapper_redundancy ; dev++) {

    hdrDevice = storage_mapper_device(fid,dev,st->mapper_modulo);
    storage_build_hdr_path(path, st->root, hdrDevice, spare, storage_slice);
                 
    if (storage_write_header_file(st,hdrDevice,path, hdr) == 0) {    
      //dbg("Header written on storage %d/%d device %d", st->cid, st->sid, hdrDevice);
      result++;
    }
  }  
  return result;
} 
   
/*
** API to be called when an error occurs on a device
 *
 * @param st: the storage to be initialized.
 * @param device_nb: device number
 *
 */
int storage_error_on_device(storage_t * st, uint8_t device_nb) {

  if ((st == NULL) || (device_nb >= STORAGE_MAX_DEVICE_NB)) return 0;     
    
  int active = st->device_errors.active;
    
  // Since several threads can call this API at the same time
  // some count may be lost...
  st->device_errors.errors[active][device_nb]++;
  return st->device_errors.errors[active][device_nb];
}
/**
*   truncate to 0 a file that has been just recycled

  @param st    : storage we are looking on
  @param device    : list of the devices per chunk
  @param storage_slice    : directory number depending on fid
  @param fid   : fid whose hader file we are looking for
  @param spare : whether this storage is spare for this FID
  @param hdr   :  the read header file
  
  @retval  0 on success
  @retval  -1 on error
  
*/  
int storage_truncate_recycle(storage_t * st, int storage_slice,uint8_t spare, fid_t fid,rozofs_stor_bins_file_hdr_t *file_hdr) {
    int status = -1;
    char path[FILENAME_MAX];
    int fd = -1;
    int open_flags;
    int chunk;
    int result;
    int chunk_idx;
    int dev;


    open_flags = ROZOFS_ST_BINS_FILE_FLAG;     
    // Build the chunk file name for chunk 0
    chunk = 0;
    dev   = rozofs_st_header_get_chunk(file_hdr, chunk);
    
    /*
    ** A valid device is given as input, so use it
    */
    if ((dev != ROZOFS_EOF_CHUNK)&&(dev != ROZOFS_EMPTY_CHUNK)&&(dev != ROZOFS_UNKNOWN_CHUNK)) {
      /*
      ** Build the chunk file name using the valid device id given in the device array
      */
      storage_build_chunk_full_path(path, st->root, dev, spare, storage_slice, fid, chunk);

      // Open bins file
      fd = open(path, open_flags, ROZOFS_ST_BINS_FILE_MODE);
      if (fd < 0) {
          storio_fid_error(fid, dev, chunk, 0, 0,"open truncate"); 		        
          storage_error_on_device(st,dev);  				    
          severe("open failed (%s) : %s", path, strerror(errno));
          goto out;
      }
      
      /* 
      ** truncate the file to the required size
      */      
      if ((spare) || (common_config.recycle_truncate_blocks==0)) {
        if (ftruncate(fd, 0)){}
	goto next_chunks;
      }

      /*
      ** Find out the projection identifier for this sid
      */
      uint8_t  prj_id  = 0;
      uint8_t  forward = rozofs_get_rozofs_forward(file_hdr->layout);
      uint16_t rozofs_msg_psize=0, rozofs_disk_psize=0;

      for (prj_id=0; prj_id< forward; prj_id++) {
        if (file_hdr->distrib[prj_id] == file_hdr->sid) break;
      }

      /*
      ** Retrieve the projection size on disk
      */      
      storage_get_projid_size(spare, prj_id, file_hdr->layout, file_hdr->bsize,
                              &rozofs_msg_psize, &rozofs_disk_psize);
      /*
      ** compute the truncate size
      */
      uint64_t truncate_size = common_config.recycle_truncate_blocks;
      truncate_size *= rozofs_disk_psize;

      /*
      ** compare the truncate size and the file size
      */
      struct stat buf;
      if (fstat(fd, &buf)<0) {     
        if (ftruncate(fd, 0)){}
	goto next_chunks;	  
      }
      
      if (buf.st_size > truncate_size) {
        if (ftruncate(fd, truncate_size)){}            	  
      }
    }
    
next_chunks:    
    /*
    ** Remove the extra chunks
    */
    for (chunk_idx=(chunk+1); chunk_idx<file_hdr->nbChunks; chunk_idx++) {

      dev = rozofs_st_header_get_chunk(file_hdr, chunk_idx);

      if (dev == ROZOFS_EOF_CHUNK) {
        break;
      }
      
      if (dev == ROZOFS_EMPTY_CHUNK) {
        rozofs_st_header_set_chunk(file_hdr, chunk_idx, ROZOFS_EOF_CHUNK);
        continue;
      }
      
      storage_rm_data_chunk(st, dev, fid, spare, chunk_idx,1/*errlog*/);
      rozofs_st_header_set_chunk(file_hdr, chunk_idx, ROZOFS_EOF_CHUNK);
    }    
    /*
    ** Update number of chunks within the file header
    */
    file_hdr->nbChunks = rozofs_st_header_roundup_chunk_number(chunk+1);  
    
    /* 
    ** Rewrite file header on disk
    */   
    result = storage_write_all_header_files(st, fid, spare, file_hdr);        
    /*
    ** Failure on every write operation
    */ 
    if (result == 0) goto out;       
    status = 0;
out:

    if (fd != -1) close(fd);
    
    return status;
}
/*________________________________________________________________________
**  Read a header file
**
**  @param path        File path
**  @param cid         cluster id
**  @param sid         logical storage identifier
**  @param fid         FID
**  @param hdr         Where to store the read header
**
**  @retval An error string or NULL on success
**________________________________________________________________________
*/
char * rozofs_st_header_read(char * path, cid_t cid, sid_t sid, fid_t fid, rozofs_stor_bins_file_hdr_t * hdr) {
  int                 fd;
  int                 nb_read;
  
  /*
  ** Open hdr file
  */
  fd = open(path, ROZOFS_ST_NO_CREATE_FILE_FLAG, ROZOFS_ST_BINS_FILE_MODE);
  if (fd < 0) {
    return "open hdr read";       
  }

  nb_read = pread(fd, hdr, sizeof(rozofs_stor_bins_file_hdr_vall_t), 0); 
  close(fd);

  if (nb_read < 0) {
    return "read hdr";       
  }    

  errno = ENODATA; 
  if (nb_read <= 1) {
    return "read hdr";       
  }

  /*
  ** File can not be read completly 
  */    
  if (hdr->version == 0) {
    if (nb_read < sizeof(rozofs_stor_bins_file_hdr_v0_t)) {
      return "hdr0 size";       
    }
  }  
  else if (hdr->version == 1) {
    if (nb_read < sizeof(rozofs_stor_bins_file_hdr_v1_t)) {
      return "hdr1 size";       
    }
  }
  else if (hdr->version == 2) {
    if ((hdr->nbChunks<8) || (hdr->nbChunks>ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) || (nb_read < rozofs_st_get_header_file_size(hdr))) {
      return "hdr2 size";       
    }
    /*
    ** Fullfill v2 chunk 2 device array with EOF marks
    */
    memset(&hdr->devFromChunk[hdr->nbChunks], ROZOFS_EOF_CHUNK, ROZOFS_STORAGE_MAX_CHUNK_PER_FILE-hdr->nbChunks);
  }
  else {
    return "hdr vers";         
  } 

  /*
  ** check CRC32
  */
  uint32_t crc32 = fid2crc32((uint32_t *)fid);
  if (storio_check_header_crc32(hdr, crc32) != 0) {
    errno = 0;
    return "crc32 hdr";     
  } 

  /*
  ** Transform v0 header to v1 header
  */
  if (hdr->version == 0) {
    rozofs_stor_bins_file_hdr_vall_t * vall = (rozofs_stor_bins_file_hdr_vall_t*) hdr;
    vall->v1.cid     = cid;
    vall->v1.sid     = sid;
    vall->v1.version = 1;
  }
  /*
  ** Transform v1 header to v2 header
  */
  if (hdr->version == 1) {
    rozofs_stor_bins_file_hdr_t v2; 
    rozofs_st_header_from_v1_to_v2((rozofs_stor_bins_file_hdr_v1_t *)hdr, &v2);
    int v2size = rozofs_st_get_header_file_size(&v2);        
    memcpy(hdr,&v2,v2size);
  }

  /*
  ** check the recycle case : not the same recycling value
  */
  if (memcmp(hdr->fid,fid,sizeof(fid_t)) != 0) {
    return "recycle";   
  }  
  return NULL;
}
/*
 ** Read a header/mapper file
    This function looks for a header file of the given FID on every
    device when it should reside on this storage.

  @param st    : storage we are looking on
  @param fid   : fid whose hader file we are looking for
  @param spare : whether this storage is spare for this FID
  @param hdr   : where to return the read header file
  @param update_recycle : whether the header file is to be updated when recycling occurs
  
  @retval  STORAGE_READ_HDR_ERRORS     on failure
  @retval  STORAGE_READ_HDR_NOT_FOUND  when header file does not exist
  @retval  STORAGE_READ_HDR_OK         when header file has been read
  @retval  STORAGE_READ_HDR_OTHER_RECYCLING_COUNTER         when header file has been read
  
*/
typedef struct _rozofs_storage_dev_info_t {
  uint64_t     time;
  int          absoluteIdx;
  int          result;
} rozofs_storage_dev_info_t;

STORAGE_READ_HDR_RESULT_E storage_read_header_file(storage_t                   * st, 
                                                   fid_t                         fid, 
						   uint8_t                       spare, 
						   rozofs_stor_bins_file_hdr_t * hdr, 
						   int                           update_recycle) {
  int  fidRelativeIdx;
  int  absoluteIdx;
  char path[FILENAME_MAX];
  int  storage_slice;
  int       nb_devices=0;
  struct stat buf;
  int       idx;
  int       ret;
  uint64_t  to_repair=0;
  char     *pChar;
  int                           nbMapper=0;
  int                           orderedIdx[STORAGE_MAX_DEVICE_NB];
  rozofs_storage_dev_info_t     devinfo[STORAGE_MAX_DEVICE_NB];
  rozofs_storage_dev_info_t   * pdevinfo;
  char                        * error;
  
  memset(orderedIdx,0,sizeof(orderedIdx));
  memset(devinfo,0,sizeof(devinfo));

  /*
  ** Compute storage slice from FID
  */
  storage_slice = rozofs_storage_fid_slice(fid);    
 
  nbMapper = st->mapper_redundancy;
   
  /*
  ** Search for the last updated file.
  ** It may occur that a file can not be written any more although 
  ** it can still be read, so we better read the latest file writen
  ** on disk to be sure to get the correct information.
  ** So let's list all the redundant header files in the modification 
  ** date order.
  */
  pdevinfo = devinfo;
  for (fidRelativeIdx=0; fidRelativeIdx < nbMapper ; fidRelativeIdx++,pdevinfo++) {

    /*
    ** Header file path
    */
    absoluteIdx           = storage_mapper_device(fid,fidRelativeIdx,st->mapper_modulo);
    pdevinfo->absoluteIdx = absoluteIdx;
    pChar = storage_build_hdr_path(path, st->root, absoluteIdx, spare, storage_slice);
	            
    /*
    ** Check that this directory already exists, otherwise it will be create
    */
    if (storage_create_dir(path) < 0) {
      storio_hdr_error(fid, absoluteIdx,"create dir");   
      storage_error_on_device(st,absoluteIdx);
      continue;
    }   
        
    /* 
    ** Fullfill the path with the name of the mapping file
    */
    rozofs_uuid_unparse_no_recycle(fid, pChar);

    /*
    ** Get the file attributes
    */
    ret = stat(path,&buf);
    if (ret < 0) {
      /*
      ** File is missing. 
      ** Should be rewritten
      */
      if (errno == ENOENT) {
        to_repair |= (1ULL<<fidRelativeIdx);
      }
      pdevinfo->result = errno;
      continue;
    }
    
    /*
    ** File is not completly written. 
    ** Should be rewritten
    */
    if (buf.st_size < (sizeof(rozofs_stor_bins_file_hdr_v2_t)-ROZOFS_STORAGE_MAX_CHUNK_PER_FILE)) {
      to_repair |= (1ULL<<fidRelativeIdx);
      pdevinfo->result = ENODATA;
      continue;
    }
        
    /*
    ** One more correct device
    */
    pdevinfo->time = buf.st_mtime;
    nb_devices++;
  }
  
  /*
  ** Header files do not exist
  */
  if (nb_devices == 0) {
    pdevinfo = devinfo;
    for (fidRelativeIdx=0; fidRelativeIdx < nbMapper ; fidRelativeIdx++,pdevinfo++) {
      if ((pdevinfo->result == ENOENT) || (pdevinfo->result == ENODATA)) {
        return STORAGE_READ_HDR_NOT_FOUND;  
      } 
    } 
    /*
    ** All devices have problems
    */
    return STORAGE_READ_HDR_ERRORS; 
  }
 

  /*
  ** Some header file is missing but not all
  */
  if (nb_devices != nbMapper) {
    pdevinfo = devinfo;
    for (fidRelativeIdx=0; fidRelativeIdx < nbMapper ; fidRelativeIdx++,pdevinfo++) {
      errno = pdevinfo->result;
      if (errno != 0) {

        absoluteIdx = pdevinfo->absoluteIdx; 

        /*
        ** Log error in memory log
        */
        storio_hdr_error(fid, absoluteIdx,"stat hdr"); 
        /*
        ** Consider the device is in fault
        */
        if ((errno!=ENODATA)&&(errno!=ENOENT)) {
          storage_error_on_device(st,absoluteIdx);        
        }  
      }
    }
  }
  
  /*
  ** Sort the devices in the modification date order
  */
  nb_devices = 0;
  pdevinfo = devinfo;
  for (fidRelativeIdx=0; fidRelativeIdx < nbMapper ; fidRelativeIdx++,pdevinfo++) {
    int new;
    int old;
    
    if (pdevinfo->time == 0) continue; 
    
    for (idx=0; idx<nb_devices; idx++) {
      if (devinfo[orderedIdx[idx]].time > pdevinfo->time) continue;
      break;
    }
    nb_devices++;

    new = fidRelativeIdx;
    for (; idx < nb_devices; idx++) {    
      old = orderedIdx[idx];
      orderedIdx[idx] = new;
      new = old; 
    } 
  }    

  /*
  ** Loop on reading the mapper files in the modification date order
  ** until reading a correct one.
  */
  for (idx=0; idx < nb_devices ; idx++) {

    /*
    ** Header file name
    */
    fidRelativeIdx = orderedIdx[idx];
    absoluteIdx    = devinfo[fidRelativeIdx].absoluteIdx;
    storage_build_hdr_file_path(path, st->root, absoluteIdx, spare, storage_slice, fid);

    error = rozofs_st_header_read(path, st->cid, st->sid, fid, hdr);    
    if (error == NULL) break;
    
    if (strcmp(error,"recycle")==0) {
      /*
      ** need to update the value of the fid in hdr
      */
      if (update_recycle) {
        memcpy(hdr->fid,fid,sizeof(fid_t));
        storage_truncate_recycle(st,storage_slice,spare,fid,hdr);
        return STORAGE_READ_HDR_OK;
      }
      /*
      ** This not the same FID, so the file we are looking for does not exist
      */
      return STORAGE_READ_HDR_OTHER_RECYCLING_COUNTER;	    
    }

    if (strcmp(error,"crc32 hdr")==0) {
       __atomic_fetch_add(&st->crc_error,1,__ATOMIC_SEQ_CST);
    }

    to_repair |= (1ULL<<fidRelativeIdx);
    storio_hdr_error(fid, absoluteIdx, error);       
    storage_error_on_device(st, absoluteIdx);
  }  
  
  /*       
  ** All devices have problems
  */
  if (idx == nb_devices) {
    return STORAGE_READ_HDR_ERRORS;
  }
      
  /*
  ** Header file has been read successfully. 
  ** Check whether some header files need to be repaired
  */
  if (to_repair!=0) {
    for (fidRelativeIdx=0; fidRelativeIdx < nbMapper; fidRelativeIdx++) {
      if (to_repair & (1ULL<<fidRelativeIdx)) {
	/*
	** Rewrite corrupted header file
	*/
        absoluteIdx = devinfo[fidRelativeIdx].absoluteIdx;
	storage_build_hdr_path(path, st->root, absoluteIdx, spare, storage_slice);
        if (storage_write_header_file(st, absoluteIdx, path, hdr) == 0) {
          /*
          ** Log reparation in memory log
          */
          errno = 0;
          storio_hdr_error(fid, absoluteIdx,"hdr repaired");            
        }
      }
    }
  }
  return STORAGE_READ_HDR_OK;	
}
/*
 ** Find the name of the chunk file.
    
  @param st        : storage we are looking on
  @param fidCtx    : the FID mapping context
  @param dev       : the device number from the fidCtx
  @param chunk     : chunk number to write
  @param fid       : FID of the file to write
  @param layout    : layout to use
  @param dist_set  : the file sid distribution set
  @param spare     : whether this storage is spare for this FID
  @param path      : The returned absolute path
  @param version   : current header file format version

  @retval A value within storage_dev_map_distribution_write_ret_e
  @retval MAP_OK      the path is written in path
  @retval MAP_COPY2CACHE when an error occur that prevent the correct operation
  
 */
typedef enum {

  /* An error occur that prevent access to the file */
  MAP_FAILURE=0, 
  
  /* The path is written in path */
  MAP_OK,
       
  /* The path is written in path. But FID cache has to be updated after file is written
     from information in file header */  
  MAP_COPY2CACHE 
} storage_dev_map_distribution_write_ret_e;

	  
static inline storage_dev_map_distribution_write_ret_e
    storage_dev_map_distribution_write(  storage_t * st, 
                                         storio_device_mapping_t * fidCtx,
                                         uint8_t                 * dev,
					 uint8_t chunk,
					 uint32_t bsize, 
					 fid_t fid, 
					 uint8_t layout,
                                	 sid_t * dist_set, 
					 uint8_t spare, 
					 char *path, 
					 int version,
					 rozofs_stor_bins_file_hdr_t * file_hdr) {
    int                         result;
    STORAGE_READ_HDR_RESULT_E   read_hdr_res;
    int                         storage_slice = rozofs_storage_fid_slice(fid); 
    

    /*
    ** A valid device is given as input, so use it
    */
    if ((*dev != ROZOFS_EOF_CHUNK)&&(*dev != ROZOFS_EMPTY_CHUNK)&&(*dev != ROZOFS_UNKNOWN_CHUNK)) {
      /*
      ** Build the chunk file name using the valid device id given in the device array
      */
      storage_build_chunk_full_path(path, st->root, *dev, spare, storage_slice, fid, chunk);
      return MAP_OK;  
    }   

    /*
    ** When no device id is given as input, let's read the header file 
    */    
    read_hdr_res = storage_read_header_file(
            st,       // cid/sid context
            fid,      // FID we are looking for
	    spare,    // Whether the storage is spare for this FID
	    file_hdr,// Returned header file content
            1 );      // Update header file when not the same recycling value

      
    /*
    ** Error accessing all the devices
    */
    if (read_hdr_res == STORAGE_READ_HDR_ERRORS) {
      return MAP_FAILURE;
    }
    
    /*
    ** Header file has been read
    */
    if (read_hdr_res == STORAGE_READ_HDR_OK) {
       
      *dev = rozofs_st_header_get_chunk(file_hdr,chunk);
      
      /*
      ** A device is already allocated for this chunk.
      */
      if ((*dev != ROZOFS_EOF_CHUNK)&&(*dev != ROZOFS_EMPTY_CHUNK)) {
      
	 /*
	 ** Build the chunk file name using the device id read from the header file
	 */
	 storage_build_chunk_full_path(path, st->root, *dev, spare, storage_slice, fid, chunk);
	 
         /*
	 ** Update input device array from the read header file
	 */
         storio_store_to_ctx(fidCtx, file_hdr->nbChunks, file_hdr->devFromChunk);	 
	 return MAP_OK;
      }   
         
    }    
      
    /*
    ** Header file does not exist. This is a brand new file
    */    
    if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) {
     /*
      ** Prepare file header
      */
      rozofs_st_header_init(file_hdr);
      memcpy(file_hdr->distrib, dist_set, ROZOFS_ST_HEADER_MAX_DISTRIB * sizeof (sid_t));
      file_hdr->layout = layout;
      file_hdr->bsize  = bsize;
      file_hdr->cid = st->cid;
      file_hdr->sid = st->sid;
      memcpy(file_hdr->fid, fid,sizeof(fid_t)); 
    }
    
      
    /*
    ** Allocate a device for this newly written chunk
    */
    *dev = storio_device_mapping_allocate_device(st);
    rozofs_st_header_set_chunk(file_hdr,chunk,*dev);     
        
    /*
    ** (re)Write the header files on disk 
    */
    result = storage_write_all_header_files(st, fid, spare, file_hdr);        
    /*
    ** Failure on every write operation
    */ 
    if (result == 0) {
      /*
      ** Header file was not existing, so let's remove it from every
      ** device. The inode may have been created although the file
      ** data can not be written
      */
      if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) {
        storage_dev_map_distribution_remove(st, fid, spare);
      }
      return MAP_FAILURE;
    }       
    

    /*
    ** Build the chunk file name using the newly allocated device id
    */
    storage_build_chunk_full_path(path, st->root, *dev, spare, storage_slice, fid, chunk);
    
    /*
    ** Update input device array from header file
    */   
    return MAP_COPY2CACHE;            
}
/*
** 
** Create RozoFS a storage subdirecty along with its slices
**
** @param pDir   The directory name (whith an ending '/')
**  
*/
void rozofs_storage_device_one_subdir_create(char * pDir) {
  char          * pChar;
  int             slice;

  /*
  ** Create directory when needed
  */
  if (access(pDir, F_OK) != 0) {
    if (storage_create_dir(pDir) < 0) {
      severe("%s creation %s",pDir, strerror(errno));
    }
  }
  
  /*
  ** Create slice directories when needed
  */
  pChar = pDir;
  pChar += strlen(pDir);
  rozofs_u32_append(pChar,common_config.storio_slice_number);
  if (access(pDir, F_OK) != 0) {  
    for (slice = 0; slice < (common_config.storio_slice_number); slice++) {
      rozofs_u32_append(pChar,slice);
      if (storage_create_dir(pDir) < 0) {
	severe("%s creation %s",pDir, strerror(errno));
      }	    
    }
  }
}    
/*
** 
** Create RozoFS storage subdirectories on a device
**
** @param root   storage root path
** @param dev    device number
**  
*/
void rozofs_storage_device_subdir_create(char * root, int dev) {
  char   path[FILENAME_MAX];
  char * pChar;

  pChar = path;
  pChar += rozofs_string_append(pChar,root);
  *pChar++ = '/';
  pChar += rozofs_u32_append(pChar,dev);

  /*
  ** Build 2nd level directories
  */
  rozofs_string_append(pChar,"/hdr_0/");
  rozofs_storage_device_one_subdir_create(path);

  rozofs_string_append(pChar,"/hdr_1/");
  rozofs_storage_device_one_subdir_create(path);

  rozofs_string_append(pChar,"/bins_0/");
  rozofs_storage_device_one_subdir_create(path);

  rozofs_string_append(pChar,"/bins_1/");
  rozofs_storage_device_one_subdir_create(path);	
}

/*
** Create sub directories structure of a storage node
**  
** @param st    The storage context
*/
int storage_subdirectories_create(storage_t *st) {
  int status = -1;
  char path[FILENAME_MAX];
  struct stat s;
  int dev;
  char * pChar, * pChar2;


  // sanity checks
  if (stat(st->root, &s) != 0) {
      severe("can not stat %s",st->root);
      goto out;
  }

  if (!S_ISDIR(s.st_mode)) {
      errno = ENOTDIR;
      goto out;
  }		

  for (dev=0; dev < st->device_number; dev++) {		


      // sanity checks
      pChar = path;
      pChar += rozofs_string_append(pChar,st->root);
      *pChar++ = '/';
      pChar += rozofs_u32_append(pChar,dev);

      if (stat(path, &s) != 0) {
	  continue;
      }

      if (!S_ISDIR(s.st_mode)) {
          severe("Not a directory %s",path);
          errno = ENOTDIR;
	  continue;
      }

      /*
      ** Check whether a X file is present. This means that the device is not
      ** mounted, so no need to create subdirectories.
      */
      pChar2 = pChar;
      pChar2 += rozofs_string_append(pChar2,"/X");
      if (access(path, F_OK) == 0) {
          // device not mounted
	  continue;
      }

      /*
      ** Build 2nd level directories
      */
      rozofs_storage_device_subdir_create(st->root,dev);
  }

  status = 0;
out:
  return status;
}

/*
** 
** Initialize the storage context and create the subdirectory structure 
** if not yet done
**  
*/
int storage_initialize(storage_t *st, 
                       cid_t cid, 
		       sid_t sid, 
		       const char *root, 
                       uint32_t device_number, 
		       uint32_t mapper_modulo, 
		       uint32_t mapper_redundancy,
                       const char *spare_mark) {
    int status = -1;
    int dev;

    DEBUG_FUNCTION;

    storio_device_error_log_init();

    if (!realpath(root, st->root))
        goto out;
	
    if (mapper_modulo > device_number) {
      severe("mapper_modulo is %d > device_number %d",mapper_modulo,device_number)
      goto out;
    }	
		
    if (mapper_redundancy > mapper_modulo) {
      severe("mapper_redundancy is %d > mapper_modulo %d",mapper_redundancy,mapper_modulo)
      goto out;
    }	
    
    st->mapper_modulo     = mapper_modulo;
    st->device_number     = device_number; 
    st->mapper_redundancy = mapper_redundancy;
    st->share             = NULL;
    st->next_device       = 0;
    
    if (spare_mark == NULL) {
      /*
      ** Spare disks have an empty mark file "rozofs_spare"
      */
      st->spare_mark = NULL;
    }
    else {
      /*
      ** Spare disks have the mark file "rozofs_spare" containing string <spare_mark>"
      */
      st->spare_mark = strdup(spare_mark);
    }
      
    
    st->device_free.active = 0;
    for (dev=0; dev<STORAGE_MAX_DEVICE_NB; dev++) {
      st->device_free.blocks[0][dev] = 20000;
      st->device_free.blocks[1][dev] = 20000;
    }

    /*
    ** Initialize device status
    */
    for (dev=0; dev<device_number; dev++) {
      st->device_ctx[dev].status = storage_device_status_init;
      st->device_ctx[dev].failure = 0;
    }

    memset(&st->device_errors , 0,sizeof(st->device_errors));        
	    
    st->sid = sid;
    st->cid = cid;

    storage_subdirectories_create(st);

    status = 0;
out:
    return status;
}

void storage_release(storage_t * st) {

    DEBUG_FUNCTION;

    st->sid = 0;
    st->cid = 0;
    st->root[0] = 0;

}

uint64_t buf_ts_storage_write[STORIO_CACHE_BCOUNT];


int storage_relocate_chunk(storage_t * st, storio_device_mapping_t * fidCtx,fid_t fid, uint8_t spare, 
                           uint8_t chunk, uint8_t * old_device) {
    STORAGE_READ_HDR_RESULT_E      read_hdr_res;  
    rozofs_stor_bins_file_hdr_t    file_hdr;
    int                            result;

    /*
    ** Let's read the header file 
    */    
    read_hdr_res = storage_read_header_file(
            st,       // cid/sid context
            fid,      // FID we are looking for
	    spare,    // Whether the storage is spare for this FID
	    &file_hdr,// Returned header file content
            1 );      // Update header file when not the same recycling value   

    /*
    ** Error accessing all the devices
    */
    if (read_hdr_res == STORAGE_READ_HDR_ERRORS) {
      severe("storage_relocate_chunk");
      return -1;
    }

    /*
    ** Header file does not exist! This is a brand new file
    */    
    if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) { 
      storio_free_dev_mapping(fidCtx);
      *old_device = ROZOFS_EMPTY_CHUNK;
      return 0;
    }

    /*
    ** Header file has been read
    */
    
    /* Save the previous chunk location and then release it */
    *old_device = rozofs_st_header_get_chunk(&file_hdr,chunk);
    
    /* Last chunk ? */
    if (chunk == (ROZOFS_STORAGE_MAX_CHUNK_PER_FILE-1)) {
      rozofs_st_header_set_chunk(&file_hdr, chunk, ROZOFS_EOF_CHUNK);
    }
    /* End of file ? */
    else if (rozofs_st_header_get_chunk(&file_hdr, chunk+1) == ROZOFS_EOF_CHUNK) {
      int idx;
      rozofs_st_header_set_chunk(&file_hdr, chunk, ROZOFS_EOF_CHUNK);
      idx = chunk-1;
      /* Previous empty chunk is now end of file */
      while (idx>=0) {
        if (rozofs_st_header_get_chunk(&file_hdr,idx) != ROZOFS_EMPTY_CHUNK) break;
        rozofs_st_header_set_chunk(&file_hdr,idx, ROZOFS_EOF_CHUNK);
	    idx--;
      }
    }
    /* Inside the file */
    else {
      rozofs_st_header_set_chunk(&file_hdr,chunk,ROZOFS_EMPTY_CHUNK);
    }  
    storio_store_to_ctx(fidCtx, file_hdr.nbChunks, file_hdr.devFromChunk);

    /* 
    ** Rewrite file header on disk
    */   
    result = storage_write_all_header_files(st, fid, spare, &file_hdr);        
    /*
    ** Failure on every write operation
    */ 
    if (result == 0) return -1;   
    return 0;
}
int storage_rm_data_chunk(storage_t * st, uint8_t device, fid_t fid, uint8_t spare, uint8_t chunk, int errlog) {
  char path[FILENAME_MAX];
  int  ret;

  uint32_t storage_slice = rozofs_storage_fid_slice(fid);
  storage_build_chunk_full_path(path, st->root, device, spare, storage_slice, fid, chunk);

  ret = unlink(path);   
  if ((ret < 0) && (errno != ENOENT) && (errlog)) {
    severe("storage_rm_data_chunk(%s) %s", path, strerror(errno));
  }
  return ret;  
} 
int storage_restore_chunk(storage_t * st, storio_device_mapping_t * fidCtx, fid_t fid, uint8_t spare, 
                           uint8_t chunk, uint8_t old_device) {
    STORAGE_READ_HDR_RESULT_E      read_hdr_res;  
    rozofs_stor_bins_file_hdr_t    file_hdr;
    int                            result;  
    int                            idx;     
   
    /*
    ** Let's read the header file 
    */    
    read_hdr_res = storage_read_header_file(
            st,       // cid/sid context
            fid,      // FID we are looking for
	    spare,    // Whether the storage is spare for this FID
	    &file_hdr,// Returned header file content
            0 );      // Do not update header file when not the same recycling value
   
    /*
    ** Error accessing all the devices
    */
    if (read_hdr_res == STORAGE_READ_HDR_ERRORS) {
      severe("storage_relocate_chunk");
      return -1;
    }

    /*
    ** Header file does not exist! This is a brand new file
    */    
    if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) { 
      storio_free_dev_mapping(fidCtx);
      return 0;
    }

    /*
    ** Header file has been read. 
    */
    
    if (read_hdr_res == STORAGE_READ_HDR_OTHER_RECYCLING_COUNTER) {
      /*
      ** This is not the same recycle counter that the one we were rebuilding
      ** better not modify any thing.
      */
      return 0;
    }
       
    
    /*
    ** Remove new data file which rebuild has failed 
    */
    int dev = rozofs_st_header_get_chunk(&file_hdr,chunk);
    storage_rm_data_chunk(st, dev , fid, spare, chunk,0/* No errlog*/);        
    
    /*
    ** Restore device in header file
    */
    rozofs_st_header_set_chunk(&file_hdr, chunk, old_device);
    
    if (old_device==ROZOFS_EOF_CHUNK) {
      /* not the last chunk */
      if ((chunk != (ROZOFS_STORAGE_MAX_CHUNK_PER_FILE-1))
      &&  (rozofs_st_header_get_chunk(&file_hdr,chunk+1) != ROZOFS_EOF_CHUNK)) {
         rozofs_st_header_set_chunk(&file_hdr,chunk,ROZOFS_EMPTY_CHUNK);
      }
    }
    else if (old_device==ROZOFS_EMPTY_CHUNK) {  
      /* Last chunk */
      if ((chunk == (ROZOFS_STORAGE_MAX_CHUNK_PER_FILE-1))
      ||  (rozofs_st_header_get_chunk(&file_hdr,chunk+1) == ROZOFS_EOF_CHUNK)) {
        rozofs_st_header_set_chunk(&file_hdr,chunk,ROZOFS_EOF_CHUNK);
      } 
    }
    if (rozofs_st_header_get_chunk(&file_hdr,chunk) == ROZOFS_EOF_CHUNK) {
      idx = chunk-1;
      /* Previous empty chunk is now end of file */
      while (idx>=0) {
        if (rozofs_st_header_get_chunk(&file_hdr,idx) != ROZOFS_EMPTY_CHUNK) break;
        rozofs_st_header_set_chunk(&file_hdr,idx,ROZOFS_EOF_CHUNK);
	    idx--;
      }      
    }    
    
    for (idx=0; idx < ROZOFS_STORAGE_MAX_CHUNK_PER_FILE; idx++) {
      if (rozofs_st_header_get_chunk(&file_hdr,idx) == ROZOFS_EOF_CHUNK) {
        break;
      }
    }
    file_hdr.nbChunks = rozofs_st_header_roundup_chunk_number(idx);    
    storio_store_to_ctx(fidCtx, file_hdr.nbChunks, file_hdr.devFromChunk);

    /* 
    ** Rewrite file header on disk
    */   
    result = storage_write_all_header_files(st, fid, spare, &file_hdr);        
    /*
    ** Failure on every write operation
    */ 
    if (result == 0) return -1;   
    return 0;
}
int storage_write_chunk(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, uint8_t chunk, bid_t bid, uint32_t nb_proj, uint8_t version,
        uint64_t *file_size, const bin_t * bins, int * is_fid_faulty) {
    int status = -1;
    char path[FILENAME_MAX];
    int fd = -1;
    size_t nb_write = 0;
    size_t length_to_write = 0;
    off_t bins_file_offset = 0;
    uint16_t rozofs_msg_psize;
    uint16_t rozofs_disk_psize;
    struct stat sb;
    int open_flags;
    int    device_id_is_given;
    rozofs_stor_bins_file_hdr_t file_hdr;
    storage_dev_map_distribution_write_ret_e map_result = MAP_FAILURE;
    uint8_t   dev = storio_get_dev(fidCtx, chunk);

    // No specific fault on this FID detected
    *is_fid_faulty = 0; 

    dbg("%d/%d Write chunk %d : ", st->cid, st->sid, chunk);
   
  
    // If the device id is given as input, that proves that the file
    // has been existing with that name on this device sometimes ago. 
open:  
    if ((dev != ROZOFS_EOF_CHUNK)&&(dev != ROZOFS_EMPTY_CHUNK)&&(dev != ROZOFS_UNKNOWN_CHUNK)) {
      device_id_is_given = 1;
      open_flags = ROZOFS_ST_NO_CREATE_FILE_FLAG;
    }
    // The file location is not known. It may not exist and should be created 
    else {
      device_id_is_given = 0;
      open_flags = ROZOFS_ST_BINS_FILE_FLAG;
    }        
 
    // Build the chunk file name 
    map_result = storage_dev_map_distribution_write(st, fidCtx, &dev, chunk, bsize, 
                                        	    fid, layout, dist_set, 
						    spare, path, 0, &file_hdr);
    if (map_result == MAP_FAILURE) {
      goto out;      
    }  

    // Open bins file
    fd = open(path, open_flags, ROZOFS_ST_BINS_FILE_MODE);
    if (fd < 0) {
    
        // Something definitively wrong on device
        if (errno != ENOENT) {
          storio_fid_error(fid, dev, chunk, bid, nb_proj,"open write");	
	  storage_error_on_device(st,dev); 
	  goto out;
	}
	
        // If device id was not given as input, the file path has been deduced from 
	// the header files or should have been allocated. This is a definitive error !!!
	if (device_id_is_given == 0) {
          storio_fid_error(fid, dev, chunk, bid, nb_proj,"open write");
	  storage_error_on_device(st,dev); 
	  goto out;
	}
	
	// The device id was given as input so the file did exist some day,
	// but the file may have been deleted without storio being aware of it.
	// Let's try to find the file again without using the given device id.
	dev = ROZOFS_EOF_CHUNK;
	goto open;    
    }

    
    /*
    ** Retrieve the projection size in the message
    ** and the projection size on disk 
    */
    storage_get_projection_size(spare, st->sid, layout, bsize, dist_set,
                                &rozofs_msg_psize, &rozofs_disk_psize); 
	       
    // Compute the offset and length to write
    
    bins_file_offset = bid * rozofs_disk_psize;
    length_to_write  = nb_proj * rozofs_disk_psize;

    //dbg("write %s bid %d nb %d",path,bid,nb_proj);

    uint32_t crc32 = fid2crc32((uint32_t *)fid) + bid;

    /*
    ** Writting the projection as received directly on disk
    */
    if (rozofs_msg_psize == rozofs_disk_psize) {
    
      /*
      ** generate the crc32c for each projection block
      */
      storio_gen_crc32((char*)bins,nb_proj,rozofs_disk_psize,crc32);

      errno = 0;
      nb_write = pwrite(fd, bins, length_to_write, bins_file_offset);
    }

    /*
    ** Writing the projections on a different size on disk
    */
    else {
      struct iovec       vector[ROZOFS_MAX_BLOCK_PER_MSG*2]; 
      int                i;
      char *             pMsg;
      
      if (nb_proj > (ROZOFS_MAX_BLOCK_PER_MSG*2)) {  
        severe("storage_write more blocks than possible %d vs max %d",
	        nb_proj,ROZOFS_MAX_BLOCK_PER_MSG*2);
        errno = ESPIPE;	
        goto out;
      }
      pMsg  = (char *) bins;
      for (i=0; i< nb_proj; i++) {
        vector[i].iov_base = pMsg;
        vector[i].iov_len  = rozofs_disk_psize;
	pMsg += rozofs_msg_psize;
      }
      
      /*
      ** generate the crc32c for each projection block
      */
      
      storio_gen_crc32_vect(vector,nb_proj,rozofs_disk_psize,crc32);
      
      errno = 0;      
      nb_write = pwritev(fd, vector, nb_proj, bins_file_offset);      
    } 

    if (nb_write != length_to_write) {
	
        if (errno==0) errno = ENOSPC;
	storio_fid_error(fid, dev, chunk, bid, nb_proj,"write");
        
	/*
	** Only few bytes written since no space left on device 
	*/
        if ((errno==0)||(errno==ENOSPC)) {
	  errno = ENOSPC;
	  goto out;
        }
	storage_error_on_device(st,dev);
	// A fault probably localized to this FID is detected   
	*is_fid_faulty = 1;  
        severe("pwrite(%s) size %llu expecting %llu offset %llu : %s",
	        path, (unsigned long long)nb_write,
	        (unsigned long long)length_to_write, 
		(unsigned long long)bins_file_offset, 
		strerror(errno));
        goto out;
    }
    /**
    * insert in the fid cache the written section
    */
//    storage_build_ts_table_from_prj_header((char*)bins,nb_proj,rozofs_max_psize,buf_ts_storage_write);
//    storio_cache_insert(fid,bid,nb_proj,buf_ts_storage_write,0);
    

    // Write is successful
    status = nb_proj * rozofs_msg_psize;

out:
    if (fd != -1) {
      // Stat file for return the size of bins file after the write operation
      if (fstat(fd, &sb) == -1) {
        severe("fstat failed: %s", strerror(errno));
        *file_size = 0;
      }
      else {      
        *file_size = sb.st_blocks;
      }
      close(fd);
    }

    /*
    ** Update device array in FID cache from header file
    */    
    if (map_result == MAP_COPY2CACHE) {
      storio_store_to_ctx(fidCtx, file_hdr.nbChunks, file_hdr.devFromChunk);    
    }
        
    return status;
}
char * storage_write_repair3_chunk(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, uint8_t chunk, bid_t bid, uint32_t nb_proj, sp_b2rep_t * blk2repair, uint8_t version,
        uint64_t *file_size, const bin_t * bins, int * is_fid_faulty) {
    char path[FILENAME_MAX];
    int fd = -1;
    size_t nb_write = 0;
    size_t nb_read = 0;
    off_t bins_file_offset = 0;
    uint16_t rozofs_msg_psize;
    uint16_t rozofs_disk_psize;
    struct stat sb;
    rozofs_stor_bins_file_hdr_t file_hdr;
    storage_dev_map_distribution_write_ret_e map_result = MAP_FAILURE;
    uint8_t   dev;

    char *data_p = NULL;
    int block_idx;
    int i;
    char myblock[4096];
    rozofs_stor_bins_hdr_t  * bins_hdr = (rozofs_stor_bins_hdr_t*)myblock;
    
 
    // No specific fault on this FID detected
    *is_fid_faulty = 0; 

    dbg("%d/%d repair chunk %d : ", st->cid, st->sid, chunk);
    dev = storio_get_dev(fidCtx, chunk); 
        
    /*
    ** This is a repair, so the blocks to repair have been read and the CRC32 
    ** was incorrect, so the chunk must exist.
    */
    if ((dev == ROZOFS_EOF_CHUNK)||(dev == ROZOFS_EMPTY_CHUNK)||(dev == ROZOFS_UNKNOWN_CHUNK)) {
      errno = EADDRNOTAVAIL;
      storio_repair_stat.file_error++;
      goto out;
    }   
 
    // Build the chunk file name
    map_result = storage_dev_map_distribution_write(st, fidCtx, &dev, chunk, bsize, 
                                        	    fid, layout, dist_set, 
						    spare, path, 0, &file_hdr);
    if (map_result == MAP_FAILURE) {
      storio_repair_stat.file_error++;
      goto out;      
    }  
        
    // Open bins file
    fd = open(path, ROZOFS_ST_NO_CREATE_FILE_FLAG, ROZOFS_ST_BINS_FILE_MODE);
    if (fd < 0) {
      storio_fid_error(fid, dev, chunk, bid, nb_proj,"open repair"); 		
      storage_error_on_device(st,dev); 
      storio_repair_stat.file_error++; 
     goto out;
    }

    /*
    ** Retrieve the projection size in the message
    ** and the projection size on disk 
    */
    storage_get_projection_size(spare, st->sid, layout, bsize, dist_set,
                                &rozofs_msg_psize, &rozofs_disk_psize); 
	       
    data_p = (char *)bins;
    
    
    /*
    ** Initializae CRC32 base 
    */ 
    uint32_t crc32 = fid2crc32((uint32_t *)fid);
    storio_repair_stat.nb_requests++;
        
    /*
    ** Loop on every block to repair within this projection chunk
    */   
    for (i=0; i < nb_proj; i++,data_p += rozofs_msg_psize) {

       /*
       ** Get relative block index from bid
       */
       block_idx = blk2repair[i].relative_bid + bid;
       storio_repair_stat.nb_blocks_requested++;
                       
       /*
       ** Read the block 
       */
       bins_file_offset = block_idx * rozofs_disk_psize;
       nb_read = pread(fd, myblock, rozofs_disk_psize, bins_file_offset);
       if (nb_read < sizeof(rozofs_stor_bins_hdr_t)) continue;       
    
       {
         uint64_t error_counter;
         uint64_t error_bitmask;
         storio_check_crc32(myblock,1, rozofs_disk_psize,
		             &error_counter, crc32+block_idx, &error_bitmask);
       }    
 	  
       /*
       ** Check the read timestamp against the given one
       ** Do not modify the file when it has been rewritten
       */
       if (memcmp(blk2repair[i].hdr, bins_hdr, sizeof(rozofs_stor_bins_hdr_t)) != 0) {
         continue; 
       }
       /* 
       ** Let's write the corrected block since the timestamp has not changed
       */
       storio_repair_stat.nb_blocks_attempted++;
       
       /* 
       ** generate the crc32c
       */
       storio_gen_crc32((char*)data_p,1,rozofs_disk_psize,crc32+block_idx);

                
       /*
       ** Re-write the block
       */  
       nb_write = pwrite(fd, data_p, rozofs_disk_psize, bins_file_offset);               
       if (nb_write == rozofs_disk_psize) {
         /*
         ** Trace CRC32 repair in sgorio log
         */
         storio_fid_error(fid, dev, chunk, block_idx, 1,"crc32 repaired");         
         storio_repair_stat.nb_blocks_success++;
         continue;	     
       }       

       /*
       ** Only few bytes written since no space left on device 
       */
       if ((errno==0)||(errno==ENOSPC)) {
         errno = ENOSPC;
       }
       else {
         severe("pwrite failed: %s", strerror(errno));
       } 

       /*
       ** Trace error 
       */
       storio_fid_error(fid, dev, chunk, block_idx, 1,"write repair");
       storage_error_on_device(st,dev); 

    }    

out:
    if (fd != -1) {
      // Stat file for return the size of bins file after the write operation
      if (fstat(fd, &sb) == -1) {
        severe("fstat failed: %s", strerror(errno));
        *file_size = 0;
      }
      else {      
        *file_size = sb.st_blocks;
      }
      close(fd);
    }

    return data_p;
}

uint64_t buf_ts_storage_before_read[STORIO_CACHE_BCOUNT];
uint64_t buf_ts_storage_after_read[STORIO_CACHE_BCOUNT];
uint64_t buf_ts_storcli_read[STORIO_CACHE_BCOUNT];
char storage_bufall[4096];
uint8_t storage_read_optim[4096];

int storage_read_chunk(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, uint8_t chunk, bid_t bid, uint32_t nb_proj,
        bin_t * bins, size_t * len_read, uint64_t *file_size,int * is_fid_faulty) {

    int status = -1;
    char path[FILENAME_MAX];
    int fd = -1;
    size_t nb_read = 0;
    size_t length_to_read = 0;
    off_t bins_file_offset = 0;
    uint16_t rozofs_msg_psize;
    uint16_t rozofs_disk_psize;
    int    device_id_is_given = 1;
    int                       storage_slice;
    struct iovec vector[ROZOFS_MAX_BLOCK_PER_MSG*2];
    uint64_t    crc32_errors[3]; 
    uint8_t     dev;
    int result;
    
    dbg("%d/%d Read chunk %d : ", st->cid, st->sid, chunk);

    // No specific fault on this FID detected
    *is_fid_faulty = 0;  
    path[0]=0;
    dev = storio_get_dev(fidCtx, chunk); 
    
    /*
    ** When device array is not given, one has to read the header file on disk
    */
    if (dev == ROZOFS_UNKNOWN_CHUNK) {
      device_id_is_given = 0;    
    }

    /*
    ** Retrieve the projection size in the message 
    ** and the projection size on disk
    */
    storage_get_projection_size(spare, st->sid, layout, bsize, dist_set,
                                &rozofs_msg_psize, &rozofs_disk_psize); 


retry:

    /*
    ** Let's read the header file from disk
    */
    if (!device_id_is_given) {
      rozofs_stor_bins_file_hdr_t file_hdr;
      STORAGE_READ_HDR_RESULT_E read_hdr_res;  
      
      read_hdr_res = storage_read_header_file(
              st,       // cid/sid context
              fid,      // FID we are looking for
	      spare,    // Whether the storage is spare for this FID
	      &file_hdr,// Returned header file content
              0 );      // do not update header file when not the same recycling value


      /*
      ** Header files are unreadable
      */
      if (read_hdr_res == STORAGE_READ_HDR_ERRORS) {
	*is_fid_faulty = 1; 
	errno = EIO;
	goto out;
      }
      
      /*
      ** Header files does not exist
      */      
      if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) {
        errno = ENOENT;
        goto out;  
      } 
      
      /*
      ** Header files has not the requested recycling value.
      ** The requested file does not exist.
      */      
      if (read_hdr_res == STORAGE_READ_HDR_OTHER_RECYCLING_COUNTER) {
        /*
        ** Update the FID context in order to make it fit with the disk content
        ** Copy recycling counter value as well as chunk distribution
        */
        fidCtx->recycle_cpt = rozofs_get_recycle_from_fid(file_hdr.fid);
        storio_store_to_ctx(fidCtx, file_hdr.nbChunks, file_hdr.devFromChunk);
        errno = ENOENT;
        goto out;  
      } 
      
      /*
      ** Update recycle counter in FID context when relevant
      */
      if (fidCtx->recycle_cpt != rozofs_get_recycle_from_fid(file_hdr.fid)) {
        fidCtx->recycle_cpt = rozofs_get_recycle_from_fid(file_hdr.fid); 
      } 

      /* 
      ** The header file has been read
      */
      storio_store_to_ctx(fidCtx, file_hdr.nbChunks, file_hdr.devFromChunk);
      dev = storio_get_dev(fidCtx, chunk); 
    } 
    
         
    /*
    ** We are trying to read after the end of file
    */				     
    if (dev == ROZOFS_EOF_CHUNK) {
      *len_read = 0;
      status = nb_proj * rozofs_msg_psize;
      goto out;
    }

    /*
    ** We are trying to read inside a whole. Return 0 on the requested size
    */      
    if(dev == ROZOFS_EMPTY_CHUNK) {
      *len_read = nb_proj * rozofs_msg_psize;
      memset(bins,0,* len_read);
      status = *len_read;
      goto out;
    }
    
  
    storage_slice = rozofs_storage_fid_slice(fid);
    storage_build_chunk_full_path(path, st->root, dev, spare, storage_slice, fid,chunk);

    // Open bins file
    fd = open(path, ROZOFS_ST_NO_CREATE_FILE_FLAG, ROZOFS_ST_BINS_FILE_MODE_RO);
    if (fd < 0) {
    
        // Something definitively wrong on device
        if (errno != ENOENT) {
          storio_fid_error(fid, dev, chunk, bid, nb_proj,"open read"); 		
	  storage_error_on_device(st,dev); 
	  goto out;
	}
	
        // If device id was not given as input, the file path has been deduced from 
	// the header files and so should exist. This is an error !!!
	if (device_id_is_given == 0) {
          storio_fid_error(fid, dev, chunk, bid, nb_proj,"open read"); 			  
	  errno = EIO; // Data file is missing !!!
	  *is_fid_faulty = 1;
	  storage_error_on_device(st,dev); 
	  goto out;
	}
	
	// The device id was given as input so the file did exist some day,
	// but the file may have been deleted without storio being aware of it.
	// Let's try to find the file again without using the given device id.
	device_id_is_given = 0;
	goto retry ;
    }	

	       
    // Compute the offset and length to write
    
    bins_file_offset = bid * rozofs_disk_psize;
    length_to_read   = nb_proj * rozofs_disk_psize;

    //dbg("read %s bid %d nb %d",path,bid,nb_proj);
  
    /*
    ** Reading the projection directly as they will be sent in message
    */
    if (rozofs_msg_psize == rozofs_disk_psize) {    
      // Read nb_proj * (projection + header)
      nb_read = pread(fd, bins, length_to_read, bins_file_offset);       
    }
    /*
    ** Projections are smaller on disk than in message
    */
    else {
      int          i;
      char *       pMsg;
      
      if (nb_proj > ROZOFS_MAX_BLOCK_PER_MSG*2) {  
        severe("storage_read more blocks than possible %d vs max %d",
	        nb_proj,ROZOFS_MAX_BLOCK_PER_MSG*2);
        errno = ESPIPE;			
        goto out;
      }
      pMsg  = (char *) bins;
      for (i=0; i< nb_proj; i++) {
        vector[i].iov_base = pMsg;
        vector[i].iov_len  = rozofs_disk_psize;
	pMsg += rozofs_msg_psize;
      }
      nb_read = preadv(fd, vector, nb_proj, bins_file_offset);      
    } 
    
    // Check error
    if (nb_read == -1) {
        storio_fid_error(fid, dev, chunk, bid, nb_proj,"read"); 			
        severe("pread failed: %s", strerror(errno));
	storage_error_on_device(st,dev);  
	// A fault probably localized to this FID is detected   
	*is_fid_faulty = 1;   		
        goto out;
    }


    /* 
    ** The length read must be a multiple of the block size.
    ** When this is not the case, it means that the last block has not been
    ** written correctly on disk and is so incorrect.
    ** Let's generate a CRC32 error to trigger a block repair
    */
    if ((nb_read % rozofs_disk_psize) != 0) {
        char fid_str[37];
        rozofs_uuid_unparse(fid, fid_str);
        warning("storage_read (FID: %s layout %d bsize %d chunk %d bid %d): read inconsistent length %d not modulo of %d",
	       fid_str,layout,bsize,chunk, (int) bid,(int)nb_read,rozofs_disk_psize);
        if ((nb_read % rozofs_disk_psize) >= sizeof(rozofs_stor_bins_file_hdr_t)) {      
	  nb_read = (nb_read / rozofs_disk_psize);
	  nb_read += 1;
	  nb_read *= rozofs_disk_psize;
        }
        else {
	  nb_read = (nb_read / rozofs_disk_psize);
	  nb_read *= rozofs_disk_psize;          
        }  
    }

    int nb_proj_effective;
    nb_proj_effective = nb_read /rozofs_disk_psize ;

    /*
    ** check the crc32c for each projection block
    */
    uint32_t crc32 = fid2crc32((uint32_t *)fid)+bid;
    memset(crc32_errors,0,sizeof(crc32_errors));
    
    if (rozofs_msg_psize == rozofs_disk_psize) {        
      result = storio_check_crc32((char*)bins,
                        	  nb_proj_effective,
                		  rozofs_disk_psize,
				  &st->crc_error,
				  crc32,
				  crc32_errors);
    }
    else {
      result = storio_check_crc32_vect(vector,
                        	       nb_proj_effective,
                		       rozofs_disk_psize,
				       &st->crc_error,
				       crc32,
				       crc32_errors);      
    }
    if (result!=0) { 
      int i;
      errno = 0;
      for (i = 0; i < nb_proj_effective ; i++) {
        if (crc32_errors[i/64] & (1ULL<<(i%64))) {
          storio_fid_error(fid, dev, chunk, bid+i, 1,"crc32"); 		     
          result--;
          if(result==0) break;
        }
      }  
    }	  

    // Update the length read
    *len_read = (nb_read/rozofs_disk_psize)*rozofs_msg_psize;

    *file_size = 0;

    // Read is successful
    status = nb_proj * rozofs_msg_psize;

out:
    if (fd != -1) close(fd);
    return status;
}
    
int storage_resize(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, bin_t * bins, uint32_t * nb_blocks, uint32_t * last_block_size, int * is_fid_faulty) {

    int status = -1;
    char path[FILENAME_MAX];
    int fd = -1;
    size_t nb_read = 0;
    size_t length_to_read = 0;
    off_t bins_file_offset = 0;
    uint16_t rozofs_msg_psize;
    uint16_t rozofs_disk_psize;
    int                       storage_slice;
    uint64_t    crc32_errors[3]; 
    uint8_t     dev;
    int result;
    int chunk;
    struct stat buf; 
    rozofs_stor_bins_hdr_t * hdr = (rozofs_stor_bins_hdr_t *) bins;
    
    dbg("%d/%d Resize", st->cid, st->sid);

    // No specific fault on this FID detected
    *is_fid_faulty = 0;  
    path[0]=0;
    *nb_blocks = 0;
    *last_block_size = 0;

    /*
    ** Retrieve the projection size in the message 
    ** and the projection size on disk
    */
    storage_get_projection_size(spare, st->sid, layout, bsize, dist_set,
                                &rozofs_msg_psize, &rozofs_disk_psize); 
    
    dev = storio_get_dev(fidCtx,0);
    
    /*
    ** When device array is not given, one has to read the header file on disk
    */
    if (dev == ROZOFS_UNKNOWN_CHUNK) {
      rozofs_stor_bins_file_hdr_t file_hdr;
      STORAGE_READ_HDR_RESULT_E read_hdr_res;  
      
      read_hdr_res = storage_read_header_file(
              st,       // cid/sid context
              fid,      // FID we are looking for
	      spare,    // Whether the storage is spare for this FID
	      &file_hdr,// Returned header file content
              0 );      // do not update header file when not the same recycling value


      /*
      ** Header files are unreadable
      */
      if (read_hdr_res == STORAGE_READ_HDR_ERRORS) {
	*is_fid_faulty = 1; 
	errno = EIO;
	goto out;
      }
      
      /*
      ** Header files does not exist
      */      
      if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) {
        errno = ENOENT;
        goto out;  
      } 
      
      /*
      ** Header files has not the requested recycling value.
      ** The requested file does not exist.
      */      
      if (read_hdr_res == STORAGE_READ_HDR_OTHER_RECYCLING_COUNTER) {
        /*
        ** Update the FID context in order to make it fit with the disk content
        ** Copy recycling counter value as well as chunk distribution
        */
        fidCtx->recycle_cpt = rozofs_get_recycle_from_fid(file_hdr.fid);
        storio_store_to_ctx(fidCtx,file_hdr.nbChunks, file_hdr.devFromChunk);
        errno = ENOENT;
        goto out;  
      } 
      
      /*
      ** Update recycle counter in FID context when relevant
      */
      if (fidCtx->recycle_cpt != rozofs_get_recycle_from_fid(file_hdr.fid)) {
        fidCtx->recycle_cpt = rozofs_get_recycle_from_fid(file_hdr.fid); 
      } 

      /* 
      ** The header file has been read
      */
      storio_store_to_ctx(fidCtx, file_hdr.nbChunks, file_hdr.devFromChunk);
    }
    
    /* 
    ** Get the last chunk number
    */
    chunk = storio_get_last_chunk(fidCtx,&dev);
    if (chunk < 0) {
      errno = ENOENT;
      goto out;  
    } 
    dev = storio_get_dev(fidCtx,chunk);
     
    /*
    ** Build the chunk full path
    */
    storage_slice = rozofs_storage_fid_slice(fid);
    storage_build_chunk_full_path(path, st->root, dev, spare, storage_slice, fid,chunk);

    // Open bins file
    fd = open(path, ROZOFS_ST_NO_CREATE_FILE_FLAG, ROZOFS_ST_BINS_FILE_MODE_RO);
    if (fd < 0) {
    
        // Something definitively wrong on device
        if (errno != ENOENT) {
          storio_fid_error(fid, dev, chunk, 0, 0,"open resize"); 		
	  storage_error_on_device(st,dev); 
	  goto out;
	}
	
        // If device id was not given as input, the file path has been deduced from 
	// the header files and so should exist. This is an error !!!
        storio_fid_error(fid, dev, chunk, 0, 0,"open resize"); 			  
	errno = EIO; // Data file is missing !!!
	*is_fid_faulty = 1;
	storage_error_on_device(st,dev); 
	goto out;
    }	

	       
    // Compute the offset and length to write
    if (fstat(fd, &buf)<0) {
       storio_fid_error(fid, dev, chunk, 0, 0,"open resize"); 			  
       errno = EIO; // Data file is missing !!!
       *is_fid_faulty = 1;
       storage_error_on_device(st,dev);
       goto out;        
    }


    *nb_blocks = buf.st_size/rozofs_disk_psize;
    if ((*nb_blocks == 0)&&(chunk==0)) {
        errno = ENOENT;
        goto out;  
    }   
    *nb_blocks = (*nb_blocks) - 1;
    
    bins_file_offset = (*nb_blocks) * rozofs_disk_psize;
    length_to_read   = rozofs_disk_psize;

    //dbg("read %s bid %d nb %d",path,bid,nb_proj);
    
    /*
    ** Reading the projection directly as they will be sent in message
    */
    nb_read = pread(fd, bins, length_to_read, bins_file_offset);       
    
    // Check error
    if (nb_read == -1) {
        storio_fid_error(fid, dev, chunk, *nb_blocks, 1,"resize"); 			
        severe("pread failed: %s", strerror(errno));
	storage_error_on_device(st,dev);  
	// A fault probably localized to this FID is detected   
	*is_fid_faulty = 1;   		
        goto out;
    }

    // Check the length read
    if (nb_read != length_to_read) {
        char fid_str[37];
        rozofs_uuid_unparse(fid, fid_str);
        severe("storage_read failed (FID: %s layout %d bsize %d chunk %d bid %d): read inconsistent length %d not modulo of %d",
	       fid_str,layout,bsize,chunk, (int) (*nb_blocks),(int)nb_read,rozofs_disk_psize);
	goto out;
    }

    /*
    ** check the crc32c for each projection block
    */
    uint32_t crc32 = fid2crc32((uint32_t *)fid)+(*nb_blocks);
    memset(crc32_errors,0,sizeof(crc32_errors));
           
    result = storio_check_crc32((char*)bins,
                        	1,
                		rozofs_disk_psize,
				&st->crc_error,
				crc32,
				crc32_errors);
    if (result!=0) { 
      storio_fid_error(fid, dev, chunk, (*nb_blocks), result,"read crc32"); 		     
      if (result>1) storage_error_on_device(st,dev); 
       errno = EIO; // Data file is missing !!!
       *is_fid_faulty = 1;
       storage_error_on_device(st,dev);
       goto out;       
    }	  

    if (hdr->s.timestamp == 0) {
      *last_block_size = ROZOFS_BSIZE_BYTES(bsize);
    }
    else if (hdr->s.effective_length<=ROZOFS_BSIZE_BYTES(bsize)) {
      *last_block_size = hdr->s.effective_length;      
    }
    
    /*
    ** Add number of bloxk of the previous chunks
    */
    *nb_blocks += (chunk*ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(bsize));
    status = 0;

out:
    if (fd != -1) close(fd);
    return status;
}


int storage_truncate(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, tid_t proj_id,bid_t input_bid,uint8_t version,uint16_t last_seg,uint64_t last_timestamp,
	u_int length_to_write, char * data, int * is_fid_faulty) {
    int status = -1;
    char path[FILENAME_MAX];
    int fd = -1;
    off_t bins_file_offset = 0;
    uint16_t rozofs_msg_psize;
    uint16_t rozofs_disk_psize;
    bid_t bid_truncate;
    size_t nb_write = 0;
    int block_per_chunk         = ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(bsize);
    int chunk                   = input_bid/block_per_chunk;
    int result;
    bid_t bid = input_bid - (chunk * block_per_chunk);
    STORAGE_READ_HDR_RESULT_E read_hdr_res;
    int chunk_idx;
    rozofs_stor_bins_file_hdr_t file_hdr;
    int                         rewrite_file_hdr = 0;
    storage_dev_map_distribution_write_ret_e map_result = MAP_FAILURE;
    uint8_t   dev;

    if (chunk>=ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) { 
      errno = EFBIG;
      return -1;
    } 
    dev = storio_get_dev(fidCtx,chunk);
    
    // No specific fault on this FID detected
    *is_fid_faulty = 0;  

    /*
    ** Prepare a v2 file header
    */
    rozofs_st_header_init(&file_hdr);
    memcpy(file_hdr.distrib, dist_set, ROZOFS_ST_HEADER_MAX_DISTRIB * sizeof (sid_t));
    file_hdr.layout  = layout;
    file_hdr.bsize   = bsize;
    file_hdr.cid     = st->cid;
    file_hdr.sid     = st->sid;	
    memcpy(file_hdr.fid, fid, sizeof(fid_t)); 


    /*
    ** Valid FID context. Do not re read disk but use FID ctx information.
    */
    if (dev != ROZOFS_UNKNOWN_CHUNK) {
    
      /*
      ** FID context contains valid distribution. Use it.
      */
      storio_read_from_ctx(fidCtx, &file_hdr.nbChunks, file_hdr.devFromChunk);
      
      /*
      ** Process to the recycling when needed
      */
      if (fidCtx->recycle_cpt != rozofs_get_recycle_from_fid(fid)) {
	/*
	** Update FID context
	*/
        fidCtx->recycle_cpt = rozofs_get_recycle_from_fid(fid);
	/*
	** Update disk
	*/	
	int storage_slice = rozofs_storage_fid_slice(fid);
	storage_truncate_recycle(st,storage_slice,spare,fid,&file_hdr);
      }   
   
    }
    else {    
      /*
      ** FID context do not contain valid distribution. Read it from disk.
      */    
      read_hdr_res = storage_read_header_file(
              st,       // cid/sid context
              fid,      // FID we are looking for
	      spare,    // Whether the storage is spare for this FID
	      &file_hdr,// Returned header file content
              1 );      // Update header file when not the same recycling value

      /*
      ** File is unreadable
      */
      if (read_hdr_res == STORAGE_READ_HDR_ERRORS) {
	return -1;
      }

      /*
      ** The file has to be created
      */    
      if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) {
	rewrite_file_hdr = 1; // Header files will have to be written back to disk
      }
      
      /*
      ** Update FID context, to make it match disk as well as requested FID.
      */
      fidCtx->recycle_cpt = rozofs_get_recycle_from_fid(fid);      
    }  
    
    /*
    ** Let's process to the truncate at the requested size 
    ** using information read from disk
    */  
    dev = rozofs_st_header_get_chunk(&file_hdr, chunk);

    /*
    ** We may allocate a device for the current truncated chunk
    */ 
    if ((dev == ROZOFS_EOF_CHUNK)||(dev == ROZOFS_EMPTY_CHUNK)) {
      rewrite_file_hdr = 1;// Header files will have to be re-written to disk   
      /*
      ** Aloocate a device
      */ 
      dev = storio_device_mapping_allocate_device(st);
      /*
      ** Store the devic e in the file header
      */
      rozofs_st_header_set_chunk(&file_hdr, chunk, dev);
    }
    
    // Build the chunk file name
    map_result = storage_dev_map_distribution_write(st, fidCtx, &dev, chunk, bsize, 
                                        	    fid, layout, dist_set, 
						    spare, path, 0, &file_hdr);
    if (map_result == MAP_FAILURE) {
      goto out;      
    }   

    // Open bins file
    fd = open(path, ROZOFS_ST_BINS_FILE_FLAG, ROZOFS_ST_BINS_FILE_MODE);
    if (fd < 0) {	
      storio_fid_error(fid, dev, chunk, bid, last_seg,"open truncate"); 		        
      storage_error_on_device(st,dev);  				    
      severe("open failed (%s) : %s", path, strerror(errno));
      goto out;
    }


    /*
    ** Retrieve the projection size in the message 
    ** and the projection size on disk
    */
    storage_get_projid_size(spare, proj_id, layout, bsize,
                            &rozofs_msg_psize, &rozofs_disk_psize); 
	       
    // Compute the offset from the truncate
    bid_truncate = bid;
    if (last_seg!= 0) bid_truncate+=1;
    bins_file_offset = bid_truncate * rozofs_disk_psize;
    status = ftruncate(fd, bins_file_offset);
    if (status < 0) goto out;
    
    /*
    ** When the truncate occurs in the middle of a block, it is either
    ** a shortening of the block or a an extension of the file.
    ** When extending the file only the header of the block is written 
    ** to reflect the new size. 
    ** In case of a shortening the whole block to write is given in the
    ** request
    */
    if (last_seg!= 0) {
	
      bins_file_offset = bid * rozofs_disk_psize;

      /*
      ** Rewrite the whole given data block 
      */
      if (length_to_write!= 0)
      {

        length_to_write = rozofs_disk_psize;
	
	/*
	** generate the crc32c for each projection block
	*/
	uint32_t crc32 = fid2crc32((uint32_t *)fid)+bid;
	storio_gen_crc32(data,1,rozofs_disk_psize,crc32);	
	
	nb_write = pwrite(fd, data, length_to_write, bins_file_offset);
	if (nb_write != length_to_write) {
            status = -1;
            storio_fid_error(fid, dev, chunk, bid, last_seg,"write truncate"); 		    
            severe("pwrite failed on last segment: %s", strerror(errno));
	    storage_error_on_device(st,dev); 
	    // A fault probably localized to this FID is detected   
	    *is_fid_faulty = 1;  	     				    
            goto out;
	}
      
      }
      else {
      
        // Write the block header
        rozofs_stor_bins_hdr_t bins_hdr;  
	bins_hdr.s.timestamp        = last_timestamp;
	bins_hdr.s.effective_length = last_seg;
	bins_hdr.s.projection_id    = proj_id;
	bins_hdr.s.version          = version;
	bins_hdr.s.filler           = 0; // Empty data : no CRC32

	nb_write = pwrite(fd, &bins_hdr, sizeof(bins_hdr), bins_file_offset);
	if (nb_write != sizeof(bins_hdr)) {
            storio_fid_error(fid, dev, chunk, bid, last_seg,"write hdr truncate"); 	
            severe("pwrite failed on last segment header : %s", strerror(errno));
	    storage_error_on_device(st,dev); 
	    // A fault probably localized to this FID is detected   
	    *is_fid_faulty = 1;  	     				    
            goto out;
        }   
        
        // Write the block footer
	bins_file_offset += (sizeof(rozofs_stor_bins_hdr_t) 
	        + rozofs_get_psizes(layout,bsize,proj_id) * sizeof (bin_t));
	nb_write = pwrite(fd, &last_timestamp, sizeof(last_timestamp), bins_file_offset);
	if (nb_write != sizeof(last_timestamp)) {
            storio_fid_error(fid, dev, chunk, bid, last_seg,"write foot truncate"); 	
            severe("pwrite failed on last segment footer : %s", strerror(errno));
	    storage_error_on_device(st,dev);  				    
	    // A fault probably localized to this FID is detected   
	    *is_fid_faulty = 1;  
            goto out;
        }   	  
      }
    } 
    

    /*
    ** Remove the extra chunks
    */
    file_hdr.nbChunks = rozofs_st_header_roundup_chunk_number(chunk+1);
    for (chunk_idx=(chunk+1); chunk_idx<ROZOFS_STORAGE_MAX_CHUNK_PER_FILE; chunk_idx++) {

      dev = rozofs_st_header_get_chunk(&file_hdr, chunk_idx);
      if (dev == ROZOFS_EOF_CHUNK) {
        continue;
      }

      rewrite_file_hdr = 1;                  
      if (dev != ROZOFS_EMPTY_CHUNK) {
        /*
        ** Remove the chunk
        */
        storage_rm_data_chunk(st, dev, fid, spare, chunk_idx,1/*errlog*/);
      }
      /*
      ** Update the header file
      */
      rozofs_st_header_set_chunk(&file_hdr,chunk_idx,ROZOFS_EOF_CHUNK);
    } 
    
    /* 
    ** Rewrite file header on disk
    */   
    if (rewrite_file_hdr) {
      dbg("%s","truncate rewrite file header");
      result = storage_write_all_header_files(st, fid, spare, &file_hdr);        
      /*
      ** Failure on every write operation
      */ 
      if (result == 0) goto out;
    }
       
    /*
    ** Update device array in FID cache from header file
    */ 
    storio_store_to_ctx(fidCtx,file_hdr.nbChunks, file_hdr.devFromChunk);
    status = 0;
    
out:

    if (fd != -1) close(fd);
    return status;
}

int storage_rm_chunk(storage_t * st, storio_device_mapping_t * fidCtx, 
                     uint8_t layout, uint8_t bsize, uint8_t spare, 
		     sid_t * dist_set, fid_t fid, 
		     uint8_t chunk, int * is_fid_faulty) {
    rozofs_stor_bins_file_hdr_t file_hdr;
    uint8_t                     dev;

    dbg("%d/%d rm chunk %d : ", st->cid, st->sid, chunk);

    // No specific fault on this FID detected
    *is_fid_faulty = 0;  
    
    /*
    ** When device array is not given, one has to read the header file on disk
    */
    if (storio_get_dev(fidCtx,0) == ROZOFS_UNKNOWN_CHUNK) {
      STORAGE_READ_HDR_RESULT_E read_hdr_res;  
      
      read_hdr_res = storage_read_header_file(
              st,       // cid/sid context
              fid,      // FID we are looking for
	      spare,    // Whether the storage is spare for this FID
	      &file_hdr,// Returned header file content
              0 );      // do not update header file when not the same recycling value

      /*
      ** Header files are unreadable
      */
      if (read_hdr_res == STORAGE_READ_HDR_ERRORS) {
	*is_fid_faulty = 1;
	errno = EIO; 
	return -1;
      }
      
      /*
      ** Header files do not exist
      */      
      if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) {
        return 0;  
      } 

      /* 
      ** The header file has been read
      */
      storio_store_to_ctx(fidCtx, file_hdr.nbChunks, file_hdr.devFromChunk);
   } 
    else {
      /*
      ** Prepare a v2 file header
      */
      rozofs_st_header_init(&file_hdr);
      memcpy(file_hdr.distrib, dist_set, ROZOFS_ST_HEADER_MAX_DISTRIB * sizeof (sid_t));
      file_hdr.layout  = layout;
      file_hdr.bsize   = bsize;
      file_hdr.cid     = st->cid;
      file_hdr.sid     = st->sid;	
      memcpy(file_hdr.fid, fid, sizeof(fid_t)); 
      storio_read_from_ctx(fidCtx, &file_hdr.nbChunks, file_hdr.devFromChunk);      
    }
             
    dev = storio_get_dev(fidCtx,chunk);
         
    /*
    ** We are trying to read after the end of file
    */				     
    if (dev == ROZOFS_EOF_CHUNK) {
      return 0;
    }

    /*
    ** This chunk is a whole
    */      
    if(dev == ROZOFS_EMPTY_CHUNK) {
      return 0;
    }
    
    /*
    ** Remove data chunk
    */
    storage_rm_data_chunk(st, dev, fid, spare, chunk, 0 /* No errlog*/) ;
    
    // Last chunk
    if ((chunk+1) >= ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) {
      rozofs_st_header_set_chunk(&file_hdr, chunk, ROZOFS_EOF_CHUNK);
    }
    // Next chunk is end of file
    else if (storio_get_dev(fidCtx,chunk+1) == ROZOFS_EOF_CHUNK) {  
      rozofs_st_header_set_chunk(&file_hdr, chunk, ROZOFS_EOF_CHUNK);
    }
    // Next chunk is not end of file
    else {
      rozofs_st_header_set_chunk(&file_hdr, chunk, ROZOFS_EMPTY_CHUNK);
    }
    
    /*
    ** Chunk is now EOF. Are the previous chunks empty ?
    */ 
    while (rozofs_st_header_get_chunk(&file_hdr,chunk) == ROZOFS_EOF_CHUNK) {
      /*
      ** The file is totaly empty
      */
      if (chunk == 0) {
        storage_dev_map_distribution_remove(st, fid, spare);
	    return 0;
      }
      
      chunk--;
      if (rozofs_st_header_get_chunk(&file_hdr,chunk) == ROZOFS_EMPTY_CHUNK) {
        rozofs_st_header_set_chunk(&file_hdr, chunk, ROZOFS_EOF_CHUNK);
      }
    }
    
    /*
    ** Re-write distibution
    */
    storio_store_to_ctx(fidCtx, file_hdr.nbChunks, file_hdr.devFromChunk);
    storage_write_all_header_files(st, fid, spare, &file_hdr);        
    return 0;
}
int storage_rm_file(storage_t * st, fid_t fid) {
    uint8_t spare = 0;
    STORAGE_READ_HDR_RESULT_E read_hdr_res;
    int chunk;
    rozofs_stor_bins_file_hdr_t file_hdr;


    // For spare and no spare
    for (spare = 0; spare < 2; spare++) {

      /*
      ** When no device id is given as input, let's read the header file 
      */      
      read_hdr_res = storage_read_header_file(
              st,       // cid/sid context
              fid,      // FID we are looking for
	      spare,    // Whether the storage is spare for this FID
	      &file_hdr,// Returned header file content
              0 );      // Do not update header file when not the same recycling value

      /*
      ** File does not exist or is unreadable
      */
      if ((read_hdr_res != STORAGE_READ_HDR_OK)&&(read_hdr_res != STORAGE_READ_HDR_OTHER_RECYCLING_COUNTER)) {
	continue;
      }
      
      for (chunk=0; chunk<ROZOFS_STORAGE_MAX_CHUNK_PER_FILE; chunk++) {
      
        int dev = rozofs_st_header_get_chunk(&file_hdr,chunk);

        if (dev == ROZOFS_EOF_CHUNK) {
	       break;
        }
	
	    if (dev == ROZOFS_EMPTY_CHUNK) {
          continue;
        }

        /*
        ** Remove data chunk
        */
        storage_rm_data_chunk(st, dev, fid, spare, chunk, 0 /* No errlog*/);
      }

      // It's not possible for one storage to store one bins file
      // in directories spare and no spare.
      storage_dev_map_distribution_remove(st, fid, spare);
      return 0;
               
    }
    return 0;
} 
void storage_rm_best_effort(storage_t * st, fid_t fid, uint8_t spare) {
  char path[FILENAME_MAX];
  char FID_string[64];
  int  dev;
  uint32_t storage_slice;
  DIR           * dp = NULL;
  struct dirent * pep;  
  int             dirfd;
    
  /*
  ** 1rst remove the header files
  */
  storage_dev_map_distribution_remove(st, fid, spare);

  storage_slice = rozofs_storage_fid_slice(fid);
  rozofs_uuid_unparse_no_recycle(fid, FID_string);

  info("storage_rm_best_effort cid %d sid %d slice %d spare %d FID %s", 
        st->cid, st->sid, storage_slice, spare, FID_string);

  /*
  ** Now remove the chunks
  */
  for (dev=0; dev < st->mapper_redundancy ; dev++) {

    /*
    ** Get the slice path
    */
    storage_build_slice_path(path, st->root, dev, spare, storage_slice);

    dirfd = open(path,O_RDONLY);
    if (dirfd< 0) {
      warning("open(%s) %s",path, strerror(errno));
      continue; 
    }
		
    dp = opendir(path);
    if (dp) {
	
      // Readdir the slice content
      while ((pep = readdir(dp)) != NULL) {

        // end of directory
        if (pep == NULL) break;

        // Check whether this is the expected file
        if (strncmp(pep->d_name,FID_string,36) != 0) continue;
            
        unlinkat(dirfd,pep->d_name,0);
        info("best effort %s%s", path, pep->d_name);
      }
    }
    closedir(dp);
    close(dirfd);
  }    
}
/*_____________________________________________________________________________
** Compute the size of a file as well as the number of allocated sectors
** from  stat() on every projection file.
**
** @param st                  The logical storage context
** @param fid                 The FID of the target file
** @param spare               Whether this storage is spare for this file
** @param nb_chunk            returned number of created chunk
** @param file_size_in_blocks returned nfile size in blocks
** @param allocated_sectors   returned allocated sectors of projections
**
** @retval 0 on success. -1 on error (errno is set)
**_____________________________________________________________________________
*/
int storage_size_file(storage_t * st, fid_t fid, uint8_t spare, uint32_t * nb_chunk, 
                      uint64_t * file_size_in_blocks, uint64_t * allocated_sectors) {
  STORAGE_READ_HDR_RESULT_E   read_hdr_res;
  int                         chunk;
  rozofs_stor_bins_file_hdr_t file_hdr;
  int32_t                     storage_slice;
  char                        path[FILENAME_MAX];
  uint8_t                     dev;
  struct stat                 buf;
  uint16_t                    rozofs_msg_psize;
  uint16_t                    rozofs_disk_psize;
  
  
  *file_size_in_blocks = 0;
  *allocated_sectors   = 0;
  *nb_chunk            = 0;

  /*
  ** Let's read the header file 
  */      
  read_hdr_res = storage_read_header_file(
          st,       // cid/sid context
          fid,      // FID we are looking for
	  spare,    // Whether the storage is spare for this FID
	  &file_hdr,// Returned header file content
          0 );      // Update header file when not the same recycling value


  if (read_hdr_res == STORAGE_READ_HDR_ERRORS) {
    /*
    ** No header file is correct....
    */
    errno = EIO;
    return -1;
  }
  
  if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) {
    /*
    ** File does nor exist
    */
    errno = ENOENT;
    return -1;
  }
  
  /*
  ** Compute the storage slice from the FID
  */
  storage_slice = rozofs_storage_fid_slice(fid);    

  /*
  ** Retrieve the projection size in the message 
  ** and the projection size on disk
  */
  storage_get_projection_size(spare, st->sid, file_hdr.layout, file_hdr.bsize, file_hdr.distrib,
                              &rozofs_msg_psize, &rozofs_disk_psize); 

  /*
  ** stat every chunk
  */
  for (chunk=0; chunk<ROZOFS_STORAGE_MAX_CHUNK_PER_FILE; chunk++) {

    dev = rozofs_st_header_get_chunk(&file_hdr,chunk);

    /*
    ** No more chunk
    */
    if (dev == ROZOFS_EOF_CHUNK) {
      break;
    }

    /*
    ** This chunk is empty
    */
    if (dev == ROZOFS_EMPTY_CHUNK) {
      continue;
    }

    /*
    ** stat this chunk
    */
    storage_build_chunk_full_path(path, st->root, dev, spare, storage_slice, fid, chunk);
    if (stat(path,&buf) == 0) {
      *file_size_in_blocks = buf.st_size;
      *allocated_sectors  += buf.st_blocks;
      *nb_chunk           += 1;
    }
  }
  
  if (chunk) {
    *file_size_in_blocks /= rozofs_disk_psize;
    *file_size_in_blocks += ((chunk-1)*ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(file_hdr.bsize));
  }
  
  //info("%d chunks %llu blocks %llu sectors",*nb_chunk,*file_size_in_blocks, *allocated_sectors)
  
  return 0;               
} 

int storage_rm2_file(storage_t * st, fid_t fid, uint8_t spare) {
 STORAGE_READ_HDR_RESULT_E read_hdr_res;
 int chunk;
 rozofs_stor_bins_file_hdr_t file_hdr;
 int dev;
 
  /*
  ** Let's read the header file 
  */      
  read_hdr_res = storage_read_header_file(
          st,       // cid/sid context
          fid,      // FID we are looking for
	  spare,    // Whether the storage is spare for this FID
	  &file_hdr,// Returned header file content
          0 );      // Update header file when not the same recycling value


  if (read_hdr_res == STORAGE_READ_HDR_ERRORS) {
    /*
    ** No header file is correct
	** Let's remove the header files.
	** We should try to remove the chunks if any...
	*/
	storage_rm_best_effort(st, fid, spare);
    return 0;
  }
  
  if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) {
    /*
    ** File already deleted
    */
    return  0;
  }
    
  /*
  ** Delete every chunk
  */
  for (chunk=0; chunk<ROZOFS_STORAGE_MAX_CHUNK_PER_FILE; chunk++) {

    dev = rozofs_st_header_get_chunk(&file_hdr,chunk);

    if (dev == ROZOFS_EOF_CHUNK) {
      break;
    }

    if (dev == ROZOFS_EMPTY_CHUNK) {
      continue;
    }

    /*
    ** Remove data chunk
    */
    storage_rm_data_chunk(st, dev, fid, spare, chunk, 0 /* No errlog*/);
  }

  storage_dev_map_distribution_remove(st, fid, spare);
  return 0;               
} 

bins_file_rebuild_t ** storage_list_bins_file(storage_t * st, sid_t sid, uint8_t device_id, 
                                              uint8_t spare, uint16_t slice, uint64_t * cookie,
        				      bins_file_rebuild_t ** children, uint8_t * eof,
        				      uint64_t * current_files_nb) {
    int i = 0;
    char path[FILENAME_MAX];
    DIR *dp = NULL;
    struct dirent *ep = NULL;
    bins_file_rebuild_t **iterator = children;
    rozofs_stor_bins_file_hdr_t file_hdr;
    int                         sid_idx;
    int                         safe;

    DEBUG_FUNCTION;
        
    /*
    ** Build the directory path
    */
    storage_build_hdr_path(path, st->root, device_id, spare, slice);
    
     
    // Open directory
    if (!(dp = opendir(path)))
        goto out;

    // Step to the cookie index
    if (*cookie != 0) {
      seekdir(dp, *cookie);
    }

    // Readdir first time
    ep = readdir(dp);


    // The current nb. of bins files in the list
    i = *current_files_nb;

    // Readdir the next entries
    while (ep && i < MAX_REBUILD_ENTRIES) {
        fid_t   fid;
    
        if ((strcmp(ep->d_name,".") != 0) && (strcmp(ep->d_name,"..") != 0)) {      
            char * error;
            
            // Read the file
            storage_build_hdr_path(path, st->root, device_id, spare, slice);
            strcat(path,ep->d_name);

            rozofs_uuid_parse(ep->d_name,fid);
            error = rozofs_st_header_read(path, st->cid, st->sid, fid, &file_hdr);         
            if (error != NULL) {
               // Readdir for next entry
               ep = readdir(dp);	       
	       continue;
            }

	    // Check the requested sid is in the distribution
	    safe = rozofs_get_rozofs_safe(file_hdr.layout);
	    for (sid_idx=0; sid_idx<safe; sid_idx++) {
	      if (file_hdr.distrib[sid_idx] == sid) break;
	    }
	    if (sid_idx == safe) {
               // Readdir for next entry
               ep = readdir(dp);	       
	       continue;
            }	    

            // Alloc a new bins_file_rebuild_t
            *iterator = xmalloc(sizeof (bins_file_rebuild_t)); // XXX FREE ?
            // Copy FID
            //rozofs_uuid_parse(ep->d_name, (*iterator)->fid);
            memcpy((*iterator)->fid,file_hdr.fid,sizeof(fid_t));
            // Copy current dist_set
            int size2copy = sizeof(file_hdr.distrib);
            memcpy((*iterator)->dist_set_current, file_hdr.distrib,size2copy);
            memset(&(*iterator)->dist_set_current[size2copy], 0, sizeof ((*iterator)->dist_set_current)-size2copy);
            // Copy layout
            (*iterator)->layout = file_hdr.layout;
            (*iterator)->bsize = file_hdr.bsize;

            // Go to next entry
            iterator = &(*iterator)->next;

            // Increment the current nb. of bins files in the list
            i++;
        }
	
        // Readdir for next entry
        if (i < MAX_REBUILD_ENTRIES) {
          ep = readdir(dp);
        }  
    }

    // Update current nb. of bins files in the list
    *current_files_nb = i;

    if (ep) {
        // It's not the EOF
        *eof = 0;
	// Save where we are
        *cookie = telldir(dp);
    } else {
        *eof = 1;
    }

    // Close directory
    if (closedir(dp) == -1)
        goto out;

    *iterator = NULL;
out:
    return iterator;
}

int storage_list_bins_files_to_rebuild(storage_t * st, sid_t sid, uint8_t * device_id,
        uint8_t * spare, uint16_t * slice, uint64_t * cookie,
        bins_file_rebuild_t ** children, uint8_t * eof) {

    int status = -1;
    uint8_t spare_it = 0;
    uint64_t current_files_nb = 0;
    bins_file_rebuild_t **iterator = NULL;
    uint8_t device_it = 0;
    uint16_t slice_it = 0;

    DEBUG_FUNCTION;

    // Use iterator
    iterator = children;

    device_it = *device_id;
    spare_it  = *spare;
    slice_it  = *slice;
    
    // Loop on all the devices
    for (; device_it < st->device_number;device_it++,spare_it=0) {

        // For spare and no spare
        for (; spare_it < 2; spare_it++,slice_it=0) {
	
            // For slice
            for (; slice_it < (common_config.storio_slice_number); slice_it++) {

        	// Build path directory for this layout and this spare type
        	char path[FILENAME_MAX];
        	storage_build_hdr_path(path, st->root, device_it, spare_it, slice_it);

        	// Go to this directory
        	if (chdir(path) != 0)
                    continue;

                // List the bins files for this specific directory
                if ((iterator = storage_list_bins_file(st, sid, device_it, spare_it, slice_it, 
		                                       cookie, iterator, eof,
                                                       &current_files_nb)) == NULL) {
                    severe("storage_list_bins_file failed: %s\n",
                            strerror(errno));
                    continue;
                }
		

                // Check if EOF
                if (0 == *eof) {
                    status = 0;
		            *device_id = device_it;
                    *spare = spare_it;
                    *slice = slice_it;
                    goto out;
                } else {
                    *cookie = 0;
                }
            }
	    }    
    }
    *eof = 1;
    status = 0;

out:

    return status;
}
/*
** ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**
**                Device enumeration and mounting
**
** ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/






/*
** Description of a RozoFS dedicated device
*/
typedef struct _storage_enumerated_device_t {
  cid_t     cid;          // CID this device is dedicated to
  sid_t     sid;          // SID this device is dedicated to
  uint8_t   dev;          // Device number within the SID
  char      name[32];     // Device name
  time_t    date;         // Date of the mark file that gave cid/sid/device
  
  int       mounted:1;    // Is it mounted 
  int       ext4:1;       // Is it ext4 (else xfs)
  int       spare:1;      // Is it a spare drive (cid/sid/device are meaningless)
  char *    spare_mark;   // String written in spare mark file in case of a spare device
} storage_enumerated_device_t;

#define STORAGE_MAX_DEV_PER_NODE 255

storage_enumerated_device_t * storage_enumerated_device_tbl[STORAGE_MAX_DEV_PER_NODE]={0};
int                           storage_enumerated_device_nb=0;

/*
 *_______________________________________________________________________
 *
 * Free an enumerated device context
 *
 * @param pDev       Enumerated device context address
 */
void storage_enumerated_device_free(storage_enumerated_device_t * pDev) {

  if (pDev == NULL) return;
  
  if (pDev->spare_mark != NULL) {
    xfree(pDev->spare_mark);
    pDev->spare_mark = NULL;
  }
  
  xfree (pDev);
} 
/*
 *_______________________________________________________________________
 *
 * Try to find out a RozoFS mark file in a given directory
 *
 * @param dir       directory to search a mark file in
 * @param pDev      device context to update information from mark file
 *
 *
 * @retval 0 on success -1 when no mark file found
 */
 #define        MAX_MARK_LEN    65
int storage_check_device_mark_file(char * dir, storage_enumerated_device_t * pDev) {
  DIR           * dp = NULL;
  int             ret;
  struct dirent * pep; 
  struct stat     buf;
  char            path[FILENAME_MAX];  
  char          * pChar;
  int             cid, sid, dev;
  int             status = -1;
  int             i;
  char            mark[MAX_MARK_LEN+1];
  int             size;
  
  /*
  ** Check the directory is accessible
  */
  if (access(dir,F_OK)!=0) {
    return -1;
  }
  
  /*
  ** Look for a spare mark file in the directory
  */
  pChar = path;
  pChar += rozofs_string_append(pChar,dir);
  *pChar = '/';
  pChar ++;
  pChar += rozofs_string_append(pChar,STORAGE_DEVICE_SPARE_MARK);    
  if (stat(path,&buf) == 0) {
  
    pDev->spare = 1;
    pDev->date  = buf.st_mtime;
    pDev->cid   = 0;
    pDev->sid   = 0;
    pDev->dev   = 0;
    
    /*
    ** There is one spare mark file.
    ** Check its content
    */
    pDev->spare_mark = NULL;

    /*
    ** Empty mark file
    */
    if (buf.st_size == 0) return 0;
    
    /*
    ** Mark file too big
    */
    if (buf.st_size > MAX_MARK_LEN) return -1;
          
    /*
    ** Open file
    */
    int fd = open(path, ROZOFS_ST_NO_CREATE_FILE_FLAG);
    if (fd < 0) return -1;
    
    /*
    ** Read mark file
    */  
    size = pread(fd, mark, MAX_MARK_LEN, 0);
    close(fd);
      
    /*
    ** Remove carriage return
    */  
    for (i=0; i<size; i++) {
      if (mark[i] == 0) break;
      if (mark[i] == '\n') break;
    } 
    mark[i] = 0;
    pDev->spare_mark = xmalloc(i+1);
    strcpy(pDev->spare_mark, mark);
    return 0;
  }    

  /*
  ** Open the directory to read its content
  */
  if (!(dp = opendir(dir))) {
    severe("opendir(%s) %s",dir,strerror(errno));
    return -1;
  }

  /*
  ** Look for some RozoFS mark file
  */
  while ((pep = readdir(dp)) != NULL) {

    /*
    ** end of directory
    */
    if (pep == NULL) break;

    /*
    ** Check whether this is a mark file
    */
    ret = rozofs_scan_mark_file(pep->d_name, &cid, &sid, &dev);
    if (ret < 0) continue;
    
    /*
    ** Mark file found
    */
    pChar = path;
    pChar += rozofs_string_append(pChar,dir);
    *pChar = '/';
    pChar ++;
    pChar += rozofs_string_append(pChar,pep->d_name);    
   
    /*
    ** Get mark file modification date
    */
    if (stat(path, &buf)<0) continue;
    
    pDev->date  = buf.st_mtime;
    pDev->cid   = cid;
    pDev->sid   = sid;
    pDev->dev   = dev;
    pDev->spare = 0;
    status      = 0;    
    break;
  }  

  /*
  ** Close directory
  */ 
  if (dp) {
    closedir(dp);
    dp = NULL;
  }   
  return status;
} 
/*
 *_______________________________________________________________________
 *
 * Sort the enumerated devices in the following order:
 * - 1rst the lowest cid,
 * - then the lowest sid 
 * - then the lowest device
 * - then the latest mark files
 *
 */
void storage_sort_enumerated_devices() {
  int                           idx1, idx2;
  storage_enumerated_device_t * pDev1;
  storage_enumerated_device_t * pDev2;
  int                           swap;
  
  /*
  ** Order the table
  ** Lower CID, then lower sid, then lower device
  ** then latest date
  */
  
  for (idx1=0; idx1<storage_enumerated_device_nb; idx1++) {
  
    pDev1 = storage_enumerated_device_tbl[idx1];
  
    for (idx2=idx1+1; idx2< storage_enumerated_device_nb; idx2++) {
    
      pDev2 = storage_enumerated_device_tbl[idx2];
      
      swap = 0;
      
      while (1) {
	/* 
	** Compare CID
	*/     	 
        if (pDev2->cid > pDev1->cid) break;
	if (pDev2->cid < pDev1->cid) {
	  swap = 1;
	  break;
	}
	/* 
	** Same CID. Compare SID
	*/     	 
        if (pDev2->sid > pDev1->sid) break;;
        if (pDev2->sid < pDev1->sid) {
	  swap = 1;
	  break;
	}
	/* 
	** Same CID/SID. Compare device
	*/     	 	     	 	
        if (pDev2->dev > pDev1->dev) break;
        if (pDev2->dev < pDev1->dev) {
	  swap = 1;
	  break;
	}     
	/* 
	** Same CID/SID/device. Compare mark file date
	*/     	 	     	 	
        if (pDev2->date > pDev1->date) {
	  swap = 1;
	  break;
	} 		 	
        break;
      }
      
      if (swap) {	
        storage_enumerated_device_tbl[idx1] = pDev2;
        storage_enumerated_device_tbl[idx2] = pDev1;
        pDev1 = pDev2;
      }	
    }
  }
} 
/*
 *_______________________________________________________________________
 *
 * Free the device list
 */
void storage_reset_enumerated_device_tbl() {
  int                           idx;
  storage_enumerated_device_t * pDev;
  
  for (idx=0; idx<storage_enumerated_device_nb; idx++) {
    pDev = storage_enumerated_device_tbl[idx];
    storage_enumerated_device_free(pDev); 
    storage_enumerated_device_tbl[idx] = NULL;
  }
  storage_enumerated_device_nb = 0;
} 
/*
 *_______________________________________________________________________
 *
 * Enumerate every RozoFS device 
 *
 * @param workDir   A directory to use to temporary mount the available 
 *                  devices on it in order to check their content.
 * @param unmount   Whether the devices should all be umounted during the procedure
 */
 
#define CONT  { free(line); line = NULL; continue; }

int storage_enumerate_devices(char * workDir, int unmount) {
  char            cmd[512];
  char            fdevice[128];
  char          * line;
  FILE          * fp=NULL;
  size_t          len;
  char            FStype[16];
  char          * pMount;
  storage_t     * st;
  char          * pt, * pt2;
  int             ret;
  storage_enumerated_device_t * pDev = NULL;  


  storio_last_enumeration_date = time(NULL);

  /*
  ** Reset enumerated device table
  */
  storage_reset_enumerated_device_tbl();
  
  /*
  ** Create the working directory to mount the devices on
  */
  if (access(workDir,F_OK)!=0) {
    if (rozofs_mkpath(workDir,S_IRUSR | S_IWUSR | S_IXUSR)!=0) {
      severe("rozofs_mkpath(%s) %s", workDir, strerror(errno));
      return storage_enumerated_device_nb;
    }
  }      
  

  /*
  ** Unmount the working directory, just in case
  */
  if (umount2(workDir,MNT_FORCE)==-1) {}    
      
  /*
  ** Build the list of block devices available on the system
  */  
  pt = fdevice;
  pt += rozofs_string_append(pt,workDir);
  pt += rozofs_string_append(pt,".dev");
  
  pt = cmd;
  pt += rozofs_string_append(pt,"lsblk -nro KNAME,FSTYPE,MOUNTPOINT | awk '{print $1\":\"$2\":\"$3;}' > ");
  pt += rozofs_string_append(pt,fdevice);
  if (system(cmd)==0) {}
  
  /*
  ** Open result file
  */
  fp = fopen(fdevice,"r");
  if (fp == NULL) {
    severe("fopen(%s) %s", fdevice, strerror(errno)); 
    return storage_enumerated_device_nb;   
  }
  
  /*
  ** Loop on devices to check whether they are
  ** dedicated to some RozoFS device usage
  */
  line = NULL;
  while (getline(&line, &len, fp) != -1) {

    /*
    ** Unmount the working directory 
    */
    if (umount2(workDir,MNT_FORCE)==-1) {}
        
    /*
    ** Get device name from the result file
    */
    pt  = line;
    while ((*pt!=0)&&(*pt!=':')) pt++;
    if (*pt == 0) {
      CONT;
    }
    *pt = 0;
    
    /*
    ** Allocate a device information structure when no available
    */
    if (pDev == NULL) {
      pDev = xmalloc(sizeof(storage_enumerated_device_t));
      if (pDev == NULL) {
	severe("Out of memory");
	CONT;
      }
    }
    else {
      /*
      ** Free spare mark string if any was allocated
      */
      if (pDev->spare_mark) {
        xfree(pDev->spare_mark);
        pDev->spare_mark = NULL;
      }
    }
    memset(pDev,0,sizeof(storage_enumerated_device_t));

    /* 
    ** Recopy device name 
    */
    sprintf(pDev->name,"/dev/%s",line);


    pt++;
    pt2 = pt; // Save starting of FS type string    
 
    /*
    ** Get FS type from the result file
    */    
    while ((*pt!=0)&&(*pt!=':')) pt++;
    if (*pt == 0) {
      // Bad line !!!
      CONT;
    }   
    *pt = 0;


    /* 
    ** Recopy the FS type
    */
    strcpy(FStype,pt2);
    pDev->ext4 = 0;
    if (strcmp(FStype,"ext4")==0) {
      pDev->ext4 = 1;
    }
    else if (strcmp(FStype,"xfs")!=0){
      CONT;
    }  
      
    /*
    ** Get the mountpoint name from the result file
    */
    pt++;
    pMount = pt;
    while ((*pt!=0)&&(*pt!='\n')&&(*pt!=':')) pt++;    
    *pt = 0;

    /*
    ** If file system is mounted
    */
    if (*pMount != 0) {
    
      pDev->mounted = 1;
    
      /*
      ** Read the mark file
      */
      if (storage_check_device_mark_file(pMount, pDev) < 0) {
        CONT;
      }
      
      /*
      ** Spare device
      */
      if (pDev->spare) {
      
        /*
        ** Spare device should not be mounted 
        */
        if (umount2(pMount,MNT_FORCE)==0) {
	  pDev->mounted = 0;
	}
        
        /*
        ** Check if someone cares about this spare file in this module        
        */
        st = NULL;
        while ((st = storaged_next(st)) != NULL) {
          /*
          ** This storage requires a mark in the spare file
          */
          if (st->spare_mark != NULL) {
            /*
            ** There is none in this spare file
            */
            if (pDev->spare_mark == NULL) continue;
            /*
            ** There is one but not the expected one
            */
            if (strcmp(st->spare_mark,pDev->spare_mark)!= 0) continue;
            /*
            ** This spare device is interresting for this logical storage
            */
            break;
          }
          /*
          ** This storage requires no mark in the spare file (empty file)
          */
          if (pDev->spare_mark == NULL) break;
        }
	/*
	** Record device in enumeration table when relevent
	*/
        if (st != NULL) {
  	  storage_enumerated_device_tbl[storage_enumerated_device_nb++] = pDev;
	  pDev = NULL;
        }  
        CONT;
      }	   
      
      /*
      ** Check CID/SID
      */
      st = storaged_lookup(pDev->cid, pDev->sid);
      if (st == NULL) {
        // Not mine
        CONT;
      }
      
      /*
      ** Umount it when requested
      */
      if (unmount) {
        if (umount2(pMount,MNT_FORCE)==0) {
	  pDev->mounted = 0;
	}
	/*
	** Record device 
	*/
	storage_enumerated_device_tbl[storage_enumerated_device_nb++] = pDev;
	pDev = NULL;
	CONT;
      }
      
      /*
      ** Check it is mounted at the right place 
      */	          	
      pt = cmd;
      pt += rozofs_string_append(pt,st->root);
      *pt++ = '/';
      pt += rozofs_u32_append(pt,pDev->dev);
      if (strcmp(cmd,pMount)!=0) {
        if (umount2(pMount,MNT_FORCE)==0) {
	  pDev->mounted = 0;
	}
      }	
      storage_enumerated_device_tbl[storage_enumerated_device_nb++] = pDev;
      pDev = NULL;     	
      CONT;       
    }
    /*
    ** If file system is not mounted
    */
                    
    /*
    ** Mount the file system on the working directory
    */
    ret = mount(pDev->name, workDir,  FStype, 
		MS_NOATIME | MS_NODIRATIME | MS_SILENT, 
		common_config.device_automount_option);
    if (ret != 0) {
      severe("mount(%s,%s,%s) %s",
              pDev->name,workDir,common_config.device_automount_option,
	      strerror(errno));
      CONT;
    }
    /*
    ** Read the mark file
    */
    ret = storage_check_device_mark_file(workDir, pDev);
  
    /*
    ** unmount directory to remount it at the convenient place
    */
    if (umount2(workDir,MNT_FORCE)==-1) {}
    
    if (ret < 0) {
      CONT;
    }

    /*
    ** Spare device
    */
    if (pDev->spare) {
      
      /*
      ** Check if someone cares about this spare file in this module
      */
      st = NULL;
      while ((st = storaged_next(st)) != NULL) {
        /*
        ** This storage requires a mark in the spare file
        */
        if (st->spare_mark != NULL) {
          /*
          ** There is none in this spare file
          */
          if (pDev->spare_mark == NULL) continue;
          /*
          ** There is one but not the expected one
          */
          if (strcmp(st->spare_mark,pDev->spare_mark)!= 0) continue;
          /*
          ** This spare device is interresting for this logical storage
          */
          break;
        }
        /*
        ** This storage requires no mark in the spare file (empty file)
        */
        if (pDev->spare_mark == NULL) break;
      }
      /*
      ** Record device in enumeration table when relevent
      */
      if (st != NULL) {
        storage_enumerated_device_tbl[storage_enumerated_device_nb++] = pDev;
        pDev = NULL;
      }  
      CONT;
    }	   
      

    /*
    ** Check CID/SID
    */
    st = storaged_lookup(pDev->cid, pDev->sid);
    if (st == NULL) {
      // Not mine
      CONT;
    }
    
    /*
    ** Record device 
    */
    storage_enumerated_device_tbl[storage_enumerated_device_nb++] = pDev;
    pDev = NULL;
    CONT;
  }
  /*
  ** Free allocated structure that has not been used
  */ 
  if (pDev != NULL) {
    storage_enumerated_device_free(pDev);
    pDev = NULL;
  }
  /*
  ** Close device file list
  */   
  if (fp) {
    fclose(fp);
    fp = NULL;
  }  
  unlink(fdevice);

  /*
  ** Unmount the directory
  */
  if (umount2(workDir,MNT_FORCE)==-1) {}
  
  /*
  ** Remove working directory
  */
  if (rmdir(workDir)==-1) {}
  
  /*
  ** Order the table
  */
  storage_sort_enumerated_devices();
  
  return storage_enumerated_device_nb;
  
}
/**
 *_______________________________________________________________________
 * Get the mount path a device is mounted on
 * 
 * @param dev the device
 *
 * @return: Null when not found, else the mount point path
 */
char * rozofs_get_device_mountpath(const char * dev) {
  struct mntent  mnt_buff;
  char           buff[512];
  struct mntent* mnt_entry = NULL;
  FILE* mnt_file_stream = NULL;

  mnt_file_stream = setmntent("/proc/mounts", "r");
  if (mnt_file_stream == NULL) {
    severe("setmntent failed for file /proc/mounts %s \n", strerror(errno));
    return NULL;
  }

  while ((mnt_entry = getmntent_r(mnt_file_stream,&mnt_buff,buff,512))) {
    if (strcmp(dev, mnt_entry->mnt_fsname) == 0){
      char *  result = xstrdup(mnt_entry->mnt_dir);
      endmntent(mnt_file_stream);
      return result;
    }
  }

  endmntent(mnt_file_stream);
  return NULL;
}
/**
 *_______________________________________________________________________
 * Check a given mount point is actually mounted
 * 
 * @param mount_path The mount point to check
 *
 * @return: 1 when mounted else 0
 */
int rozofs_check_mountpath(const char * mntpoint) {
  struct mntent  mnt_buff;
  char           buff[512];
  struct mntent* mnt_entry = NULL;
  FILE* mnt_file_stream = NULL;
  char mountpoint_path[PATH_MAX];


  if (!realpath(mntpoint, mountpoint_path)) {
    severe("bad mount point %s: %s\n", mntpoint, strerror(errno));
    return 0;
  }

  mnt_file_stream = setmntent("/proc/mounts", "r");
  if (mnt_file_stream == NULL) {
    severe("setmntent failed for file /proc/mounts %s \n", strerror(errno));
    return 0;
  }

  while ((mnt_entry = getmntent_r(mnt_file_stream,&mnt_buff,buff,512))) {
    if (strcmp(mntpoint, mnt_entry->mnt_dir) == 0){
      endmntent(mnt_file_stream);
      return 1;
    }
  }
  endmntent(mnt_file_stream);


  return 0;
}
/*
 *_______________________________________________________________________
 *
 * Display enumerated devices
 */
void storage_show_enumerated_devices_man(char * pt) {
  pt += rozofs_string_append(pt, "usage :\nenumeration\n    Displays RozoFS enumerated devices.\n");
  pt += rozofs_string_append(pt, "enumeration again\n    Forces re-enumeration (storio only).\n");
}
/*
 *_______________________________________________________________________
 *
 * Display enumerated devices
 */
void storage_show_enumerated_devices(char * argv[], uint32_t tcpRef, void *bufRef) {
  char                   * pChar = uma_dbg_get_buffer();
  int                      idx1;
  storage_enumerated_device_t * pDev1;
  int                      first=1;
  
  if ((argv[1] != NULL) && (strcasecmp(argv[1],"again")==0)) {
    re_enumration_required = 1;
    pChar += rozofs_string_append(pChar,"re enumeration is to come\n");
    uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());     
    return;
  }

  pChar += rozofs_string_append(pChar,"{\n");
  pChar += rozofs_string_append(pChar,"  \"enumeration date\" : \"");  
  pChar +=strftime(pChar, 20, "%Y-%m-%d %H:%M:%S", localtime(&storio_last_enumeration_date));
  pChar += rozofs_string_append(pChar,"\",\n"); 
      
  
  pChar += rozofs_string_append(pChar,"  \"enumerated devices\" : [\n");
  
  for (idx1=0; idx1<STORAGE_MAX_DEV_PER_NODE; idx1++) {
  
    pDev1 = storage_enumerated_device_tbl[idx1];
    if (pDev1 == NULL) continue;
    
    if (first) {
      first = 0;
    }
    else {
      pChar += rozofs_string_append(pChar,",\n");
    }    
  
    
     
    pChar += rozofs_string_append(pChar,"    { \"name\" : \"");
    pChar += rozofs_string_append(pChar,pDev1->name);
    pChar += rozofs_string_append(pChar,"\", \"date\" : \"");
    pChar +=strftime(pChar, 20, "%Y-%m-%d %H:%M:%S", localtime(&pDev1->date));    
    
    if (pDev1->ext4) {
      pChar += rozofs_string_append(pChar,"\", \"fs\" : \"ext4\",\n");
    }
    else {
      pChar += rozofs_string_append(pChar,"\", \"fs\" : \"xfs\",\n");
    }    
    pChar += rozofs_string_append(pChar, "      \"role\" : \"");
    if (pDev1->spare) {
      pChar += rozofs_string_append(pChar, "spare\"");
      pChar += rozofs_string_append(pChar, ", \"mark\" : \"");
      if (pDev1->spare_mark) {
        pChar += rozofs_string_append(pChar, pDev1->spare_mark);
      }
      pChar += rozofs_string_append(pChar, "\"");
    }
    else {
      pChar += rozofs_u32_append(pChar,pDev1->cid);
      pChar += rozofs_string_append(pChar, "/");
      pChar += rozofs_u32_append(pChar,pDev1->sid);
      pChar += rozofs_string_append(pChar, "/");
      pChar += rozofs_u32_append(pChar,pDev1->dev);   
      pChar += rozofs_string_append(pChar, "\"");
    }    
    if (pDev1->mounted) {
      char * mnt;
      pChar += rozofs_string_append(pChar,", \"mountpath\" : \"");
      mnt = rozofs_get_device_mountpath(pDev1->name);
      pChar += rozofs_string_append(pChar,mnt);
      if (mnt) xfree(mnt);
      pChar += rozofs_string_append(pChar,"\"");
    }
    else {
      pChar += rozofs_string_append(pChar,", \"mountpath\" : \"Not mounted\"");
    }
    pChar += rozofs_string_append(pChar,"\n    }");
  }
  pChar += rozofs_string_append(pChar,"\n  ],\n");
  
  pChar += rozofs_string_append(pChar,"  \"device self healing\" : {\n");
  pChar += rozofs_string_append(pChar,"    \"exportd\" \t: \"");
  pChar += rozofs_string_append(pChar,common_config.export_hosts);
  pChar += rozofs_string_append(pChar,"\",\n    \"mode\" \t: \"");
  pChar += rozofs_string_append(pChar,common_config.device_selfhealing_mode);
  pChar += rozofs_string_append(pChar,"\",\n    \"delay\" \t: ");
  pChar += rozofs_u32_append(pChar,common_config.device_selfhealing_delay);
  pChar += rozofs_string_append(pChar,"\n  }\n}\n");
  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer()); 
} 
/*
 *_______________________________________________________________________
 *
 * Try to mount one device at the convenient path
 *
 * @param pDev   The device descriptor to mount
 * 
 * @retval 0 on success, -1 on failure
 */
int storage_mount_one_device(storage_enumerated_device_t * pDev) {
  char                     cmd[512];
  char                   * pt;        
  char                   * pt2;        
  storage_t              * st;
  int                      ret;
  int                      fd;
   
  /*
  ** Lookup for the storage context
  */    
  st = storaged_lookup(pDev->cid, pDev->sid);
  if (st == NULL) return -1; // not mine

  /*
  ** Get the storage mount root path 
  */
  pt = cmd;
  pt += rozofs_string_append(pt,st->root);
  
  /*
  ** Create root directory if no yet done
  */
  if (storage_create_dir(cmd)<0) {
    return -1;
  }
  
  /*
  ** Get the device directory mount path
  */
  *pt++ = '/';
  pt += rozofs_u32_append(pt,pDev->dev);
  pt += rozofs_string_append(pt,"/");
  
  /* 
  ** Device directory does not exist 
  */
  if ((access(cmd, F_OK) != 0)&&(errno==ENOENT)) {
    /*
    ** Create device directory
    */
    if (storage_create_dir(cmd)<0) {
      return -1;
    } 
    /*
    ** Put X file
    */
    char * pt2= pt;
    pt2 += rozofs_string_append(pt2,"X");    
    fd = creat(cmd,0755);
    if (fd < 0) {
      severe("creat(%s,0755) %s", cmd, strerror(errno));   
    }    
    else {
      close(fd);   
    }
    *pt = 0;
  }
    
  /*
  ** Umount this directory, just in case
  */
  if (umount2(cmd,MNT_FORCE)==-1) {}   
  
  /*
  ** Mount the device at this place
  */
  if (pDev->ext4) {
    ret = mount(pDev->name, cmd, "ext4", 
		MS_NOATIME | MS_NODIRATIME | MS_SILENT,
		common_config.device_automount_option);
  }
  else {
    ret = mount(pDev->name, cmd, "xfs", 
		MS_NOATIME | MS_NODIRATIME | MS_SILENT,
		common_config.device_automount_option);
  }		  
  if (ret != 0) {
    severe("mount(\"%s\",\"%s\",\"%s\") %s",
            pDev->name,cmd,common_config.device_automount_option, 
	    strerror(errno));
    return -1;
  }
  
  /*
  ** Check whether this is a replacement by a spare
  */
  if (pDev->spare) {
          
    /* 
    ** Create the CID/SID/DEV mark file
    */
    pt2  = pt;
    pt2 += sprintf(pt2,STORAGE_DEVICE_MARK_FMT, pDev->cid, pDev->sid, pDev->dev);    
    fd = creat(cmd,0755);
    if (fd < 0) {
      severe("creat(%s,0755) %s", cmd, strerror(errno));
      /*
      ** Umount this directory
      */
      if (umount2(cmd,MNT_FORCE)==-1) {}   
      return -1;     
    }    
    close(fd);

    /* 
    ** Create the rebuild required mark file
    */
    pt2  = pt;
    pt2 += rozofs_string_append(pt2,STORAGE_DEVICE_REBUILD_REQUIRED_MARK);
    fd = creat(cmd,0755);
    if (fd < 0) {
      severe("creat(%s,0755) %s", cmd, strerror(errno));   
    }    
    else {
      close(fd);    
    }
    
    /*
    ** Remove spare mark
    */
    rozofs_string_append(pt,STORAGE_DEVICE_SPARE_MARK);
    unlink(cmd);
    
    /*
    ** This is no more a spare device
    */
    pDev->spare = 0;
    * pt = 0;
  }
  
  /*
  ** Device is mounted now
  */
  pDev->mounted = 1;
  /*
  ** Force rereading major & minor of the mounted device
  */
  st->device_ctx[pDev->dev].major = 0;
  st->device_ctx[pDev->dev].minor = 0;  
  info("%s mounted on %s",pDev->name,cmd);    
  return 0;
}  

/*
 *_______________________________________________________________________
 *
 * Try to find out a spare device to repair a failed device
 *
 * @param st   The storage context
 * @param dev  The device number to replace
 * 
 * @retval 0 on success, -1 when no spare found
 */
int storage_replace_with_spare(storage_t * st, int dev) {
  int                           idx;
  storage_enumerated_device_t * pDev;
 
  /*
  ** Look for a spare device
  */
  for (idx=0; idx<storage_enumerated_device_nb; idx++) {
  
    pDev = storage_enumerated_device_tbl[idx];
    if (pDev == NULL)   continue;
    if (pDev->spare==0) continue;
    
    
    if (st->spare_mark == 0) {
      /*
      ** Looking for an empty spare mark file
      */
      if (pDev->spare_mark != NULL) continue; 
    }
    else {
      /*
      ** Looking for a given string in the spare mark file
      */
      if (pDev->spare_mark == NULL) continue;
      if (strcmp(pDev->spare_mark,st->spare_mark) != 0) continue;  
    }

    /*
    ** This is either the empty spare mark file we were expecting
    ** or a spare mark file containing the string we were expecting.
    ** This spare device can be used by this cid/sid.
    */
    
    
    /*
    ** Mount this spare disk instead
    */
    pDev->cid   = st->cid;
    pDev->sid   = st->sid;
    pDev->dev   = dev;
    
    if (storage_mount_one_device(pDev) < 0) {
      pDev->cid   = 0;
      pDev->sid   = 0;
      pDev->dev   = 0;
      continue;
    }
       
    return 0;
  }  
  return -1;
}     
/*
 *_______________________________________________________________________
 *
 *  Try to mount every enumerated device at the rigth place
 *
 */
int storage_mount_all_enumerated_devices() {
  int                           idx;
  storage_enumerated_device_t * pDev=NULL;
  int                           last_cid=0;
  int                           last_sid=0;
  int                           last_dev=0;
  int                           count = 0;
 
  /*
  ** Loop on every enumerated RozoFS devices
  */
  for (idx=0; idx<storage_enumerated_device_nb; idx++) {
  
    pDev = storage_enumerated_device_tbl[idx];
    
    /*
    ** Do not mount spare device
    */
    if (pDev->spare) continue;
    
    /*
    ** Check whether previous device has the same CID/SID/DEV in 
    ** which case the previous one has the latest mark file and 
    ** so was a spare that has replaced this one.
    */
    if ((last_cid == pDev->cid)
    &&  (last_sid == pDev->sid)
    &&  (last_dev == pDev->dev)) {
      continue;
    }
    
    last_cid = pDev->cid;
    last_sid = pDev->sid;
    last_dev = pDev->dev;
    
    /*
    ** Already mounted
    */
    if (pDev->mounted) continue;

    /*
    ** Mount it
    */
    if (storage_mount_one_device(pDev)==0) {
      count++;
    }  
  }
  return count;
}     
/*
 *_______________________________________________________________________
 *
 * Try to mount the devices on the convenient path
 *
 * @param workDir   A directory to use to temporary mount the available 
 *                  devices on in order to check their content.
 * @param storaged  Whether this is the storaged or storio process    
 */
int storage_enumerated_devices_registered = 0;
int storage_process_automount_devices(char * workDir, int storaged) {

  /*
  ** Automount should be configured
  */
  if (!common_config.device_automount) return 0;  

  /*
  ** Register feature to debug if not yet done
  */
  if (storage_enumerated_devices_registered==0) {
    storage_enumerated_devices_registered = 1;
    uma_dbg_addTopicAndMan("enumeration", storage_show_enumerated_devices, storage_show_enumerated_devices_man, 0);
  }
  
  /*
  ** Enumerate the available RozoFS devices.
  ** Storaged will umount every device, while storio will let
  ** the correctly mounted devices in place.
  */     
  storage_enumerate_devices(workDir, storaged) ;
  
  /*
  ** Mount the devices that have to
  */
  return storage_mount_all_enumerated_devices();  
}
/*
 *_______________________________________________________________________
 *
 * Try to mount the devices on the convenient path
 *
 * @param workDir   A directory to use to temporary mount the available 
 *                  devices on in order to check their content.
 * @param count     Returns the number of devices that have been mounted
 */
void storaged_do_automount_devices(char * workDir, int * count) {
  *count = storage_process_automount_devices(workDir,1);
}
void storio_do_automount_devices(char * workDir, int * count) {
  *count = storage_process_automount_devices(workDir,0);
}
