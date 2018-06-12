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

#ifndef _STORAGE_H
#define _STORAGE_H

#include <stdint.h>
#include <limits.h>
#include <uuid/uuid.h>
#include <sys/param.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mount.h>

#include <rozofs/rozofs.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/common/list.h>
#include <rozofs/common/htable.h>
#include <rozofs/common/common_config.h>
#include <rozofs/core/rozofs_string.h>
#include <rozofs/core/rozofs_share_memory.h>
#include <rozofs/rpc/sproto.h>

#include "storio_device_mapping.h"
#include "storage_header.h"


/*
** The mark that should be present on a RozoFS device
*/
// Device requires a rebuild or is being rebuilt
#define STORAGE_DEVICE_REBUILD_REQUIRED_MARK "PREPARING_DISK"
// Device is a spare disk
#define STORAGE_DEVICE_SPARE_MARK            "rozofs_spare"
// Device is dedicated to a given device of a given CID/SID
#define STORAGE_DEVICE_MARK_BEGIN            "storage_c"
#define STORAGE_DEVICE_MARK_END              "%d_s%d_%d"
#define STORAGE_DEVICE_MARK_FMT              STORAGE_DEVICE_MARK_BEGIN STORAGE_DEVICE_MARK_END
// Search which device is encoded inside mark file name
static inline int rozofs_scan_mark_file(char * name, int * cid, int * sid, int * dev) { 
  if (strncmp(name,STORAGE_DEVICE_MARK_BEGIN,strlen(STORAGE_DEVICE_MARK_BEGIN)) != 0) return -1;
  name += strlen(STORAGE_DEVICE_MARK_BEGIN);
  int ret = sscanf(name,STORAGE_DEVICE_MARK_END, cid,  sid, dev);
  if (ret != 3) return -1;
  return 0;
}
#define ROZOFS_MAX_DISK_THREADS  64

/* Storage config to be configured in cfg file */
#define STORAGE_MAX_DEVICE_NB   64
#define STORAGE_NB_DEVICE       6
#define STORAGE_NB_MAPPER       4
#define STORAGE_NB_MAPPER_RED   2


/** Maximum size in bytes for the header of file bins */
#define ROZOFS_ST_BINS_FILE_HDR_SIZE 8192

/** Default open flags to use for oprn without creation */
#define ROZOFS_ST_NO_CREATE_FILE_FLAG O_RDWR | O_NOATIME

/** Default open flags to use for open bins files */
#define ROZOFS_ST_BINS_FILE_FLAG O_RDWR | O_CREAT | O_NOATIME

/** Default mode to use for open bins files */
#define ROZOFS_ST_BINS_FILE_MODE S_IFREG | S_IRUSR | S_IWUSR
#define ROZOFS_ST_BINS_FILE_MODE_RW S_IFREG | S_IRUSR | S_IWUSR
#define ROZOFS_ST_BINS_FILE_MODE_RO S_IFREG | S_IRUSR

/** Default mode to use for create subdirectories */
#define ROZOFS_ST_DIR_MODE S_IRUSR | S_IWUSR | S_IXUSR

#define MAX_REBUILD_ENTRIES 186

#define STORIO_PID_FILE "storio"
#define TIME_BETWEEN_2_RB_ATTEMPS 60

#if 1
#define dbg(fmt,...)
#else
#define dbg(fmt,...) info(fmt,__VA_ARGS__)
#endif

/*
** Initialize a CRC32 from a FID
*/
static inline uint32_t fid2crc32(uint32_t * fid) {
    uint32_t *fid_p;
    rozofs_inode_t fake_inode;
    memcpy(&fake_inode,fid,sizeof(fake_inode));
    rozofs_reset_recycle_on_fid(&fake_inode);
//    fake_inode.s.recycle_cpt =0;
    fid_p = (uint32_t*)&fake_inode;
    return (fid_p[0] ^ fid_p[1] ^ fid_p[2] ^ fid_p [3]);
}

/*
** Structure used to monitor device errors
*/
typedef struct _storage_device_errors_t {
  int      active;  // active set of blocks
  uint32_t total[STORAGE_MAX_DEVICE_NB];
  uint32_t errors[2][STORAGE_MAX_DEVICE_NB];
} storage_device_errors_t;


/*
** Structure used to help allocating a device for a new file
*/
typedef struct _storage_device_free_blocks_t {
  int      active;  // active set of blocks
  uint64_t blocks[2][STORAGE_MAX_DEVICE_NB];
} storage_device_free_blocks_t;



typedef enum _storage_device_status_e {
  storage_device_status_none=0,
  storage_device_status_init,
  storage_device_status_is,
  storage_device_status_rebuild,
  storage_device_status_reloc,
  storage_device_status_resec,
  storage_device_status_failed,
  storage_device_status_oos
} storage_device_status_e;
#include "storage_device_status_e2String.h"


typedef enum _storage_device_diagnostic_e {
  DEV_DIAG_OK,
  DEV_DIAG_FAILED_FS,
  DEV_DIAG_READONLY_FS,  
  DEV_DIAG_UNMOUNTED,
  DEV_DIAG_INODE_DEPLETION,
  DEV_DIAG_BLOCK_DEPLETION,
  DEV_DIAG_INVERTED_DISK,
  DEV_DIAG_REBUILD_REQUIRED,
} storage_device_diagnostic_e;

static inline char * storage_device_diagnostic2String(storage_device_diagnostic_e diagnostic) { 
  switch (diagnostic) {
    case DEV_DIAG_OK : return "OK";
    case DEV_DIAG_FAILED_FS: return "FAILED FS";
    case DEV_DIAG_READONLY_FS: return "READONLY FS";  
    case DEV_DIAG_UNMOUNTED: return "UNMOUNTED";
    case DEV_DIAG_INODE_DEPLETION: return "INODE DEPLETION";
    case DEV_DIAG_BLOCK_DEPLETION: return "BLOCK DEPLETION";
    case DEV_DIAG_INVERTED_DISK: return "INVERTED DISK";
    case DEV_DIAG_REBUILD_REQUIRED: return "REBUILDING";
    default: return "??";
  }  
}  

/*
** Structure of the data in share memory beteen storaged and storio
*/

/*
** Substructure per device of a storio
*/
typedef struct _storage_device_info_t {
  int                        major;
  int                        minor;
  char                       devName[8];
  uint32_t                   usage;
  uint32_t                   rdNb;
  uint32_t                   rdUs;
  uint32_t                   wrNb;
  uint32_t                   wrUs;  
  uint32_t                   lastActivityDelay;
  storage_device_status_e    status;
  storage_device_diagnostic_e diagnostic;
  uint64_t                   free;
  uint64_t                   size;    
} storage_device_info_t;

/*
** The structure
*/
typedef struct _storage_share_t {
  int                    monitoring_period; /* Monitoring periodicity in sec */
  /* An array of context for each device handled by the storio.
  ** actually more that one... */   
  storage_device_info_t  dev[1];
} storage_share_t;




#define STORAGE_DEVICE_NO_ACTION      0
#define STORAGE_DEVICE_RESET_ERRORS   1
#define STORAGE_DEVICE_REINIT         2
typedef struct _storage_device_ctx_t {
  storage_device_status_e     status;
  uint64_t                    failure;
  storage_device_diagnostic_e diagnostic;
  uint8_t                     action;
  int                         major;
  int                         minor;
  char                        devName[8];
  int                         usage;
  uint64_t                    ticks;
  uint32_t                    rdDelta;
  uint64_t                    rdCount;  
  uint64_t                    rdTicks;
  uint32_t                    rdAvgUs;  
  uint32_t                    wrDelta;
  uint64_t                    wrCount;  
  uint64_t                    wrTicks;  
  uint32_t                    wrAvgUs;
  uint64_t                    monitor_run;
  uint64_t                    monitor_no_activity;
  uint64_t                    last_activity_time;
} storage_device_ctx_t;


/** Directory used to store bins files for a specific storage ID*/
typedef struct storage {
    sid_t sid; ///< unique id of this storage for one cluster
    cid_t cid; //< unique id of cluster that owns this storage
    char root[FILENAME_MAX]; ///< absolute path.
    uint32_t next_device;    ///< for strict round robin allocation
    /*
    ** String to search for inside spare mark file when looking for a spare device
    ** When null : look for empty "rozofs_spare" file
    ** else      : look for "rozofs_spare" file containing string <spare-mark>"
    */    
    char * spare_mark; 
   uint64_t  crc_error;   ///> CRC32C error counter
    uint32_t mapper_modulo; // Last device number that contains the fid to device mapping
    uint32_t device_number; // Number of devices to receive the data for this sid
    uint32_t mapper_redundancy; // Mapping file redundancy level
    storage_device_free_blocks_t device_free;    // available blocks on devices
    storage_device_errors_t      device_errors;  // To monitor errors on device
    storage_device_ctx_t         device_ctx[STORAGE_MAX_DEVICE_NB]; 
    storage_share_t            * share; // share memory between storaged and storio          
} storage_t;

/*
 *_______________________________________________________________________
 *
 * Umount some path
 *
 * @param path       Path to unmount
 */
static inline int storage_umount(char * path) {
  int flag = MNT_DETACH;
  int i;
    
  if (path == NULL) return -1;
  
  /*
  ** 3 attempts to unmount the given path
  */
  for (i=0; i< 3; i++) {

    errno = 0;
   
    /*
    ** Request for unmount
    */
    if (umount2(path, flag)==-1) {}
    
    /*
    ** Hurra. This is no more a mount point
    */
    if ((errno == ENOENT)||(errno == EINVAL)) return 0;

    /*
    ** Next time try a force umount
    */
    flag = MNT_FORCE;
    usleep(1);
  }
  return -1;      
} 
/*_____________________________________________________________
** Array of storages
*/
extern storage_t storaged_storages[];
extern uint16_t  storaged_nrstorages;
/*_____________________________________________________________
** Retrieve the storage context from its CID and SID
** @param cid    The clster identifier
** @param sid    The storage identifier within this cluster
** 
** @retval the storage context or NULL when it does not exist
*/
static inline storage_t *storaged_lookup(cid_t cid, sid_t sid) {
    storage_t *st = 0;
    DEBUG_FUNCTION;

    st = storaged_storages;
    do {
        if ((st->cid == cid) && (st->sid == sid))
            goto out;
    } while (st++ != storaged_storages + storaged_nrstorages);
    errno = EINVAL;
    st = 0;
out:
    return st;
}
/*_____________________________________________________________
** Retrieve the next storage context 
** @param st     The current storage context
** 
** @retval the next storage context or NULL when no more 
*/
static inline storage_t *storaged_next(storage_t * st) {
    DEBUG_FUNCTION;

    if (storaged_nrstorages == 0) return NULL;
    if (st == NULL) return storaged_storages;

    st++;
    if (st < storaged_storages + storaged_nrstorages) return st;
    return NULL;
}



typedef struct bins_file_rebuild {
    fid_t fid;
    uint8_t layout; ///< layout used for this file.
    uint8_t bsize; ///< Block size as defined in ROZOFS_BSIZE_E
    sid_t dist_set_current[ROZOFS_SAFE_MAX]; ///< currents sids of storage nodes target for this file.
    struct bins_file_rebuild *next;
} bins_file_rebuild_t;


 
//  
typedef enum rozofs_rbs_error_e {
  rozofs_rbs_error_none=0,
  rozofs_rbs_error_no_such_cluster,
  rozofs_rbs_error_not_enough_storages_up,
  rozofs_rbs_error_file_deleted,
  rozofs_rbs_error_file_to_much_running_rebuild,
  rozofs_rbs_error_rebuild_start_failed,
  rozofs_rbs_error_read_error,
  rozofs_rbs_error_transform_error,
  rozofs_rbs_error_rebuild_broken,
  rozofs_rbs_error_write_failed,
  rozofs_rbs_error_not_enough_projection_read,
  rozofs_rbs_error_no_spare_target_available,
   
  rozofs_rbs_error_unknown,
} ROZOFS_RBS_ERROR_E;
#include "ROZOFS_RBS_ERROR_E2String.h"


/** Get the file enty size from the layout
 */
static inline int rbs_entry_size_from_layout(uint8_t layout) {
  uint8_t safe = rozofs_get_rozofs_safe(layout);
  int size = sizeof(rozofs_rebuild_entry_file_t) - ROZOFS_SAFE_MAX + safe;
  return size;
}   

/** Retrieve the share memory address for a storage 
 *
 * @param st: the storage context
 */
static inline storage_share_t * storage_get_share(storage_t * st) {

  if (st->share == NULL) {   
    st->share = rozofs_share_memory_resolve_from_name(st->root);
  }
  return st->share;  
}

/**
 *  Get the next storage 
 *
 *  @param st: NULL for getfirst, else getnext
 *  @return : the first or the next storage or NULL
 */
storage_t *storaged_next(storage_t * st);


/** API to be called when an error occurs on a device
 *
 * @param st: the storage to be initialized.
 * @param device_nb: device number
 *
 */
int storage_error_on_device(storage_t * st, uint8_t device_nb);


static inline char * trace_device(char * pChar, storio_device_mapping_t * p) {
  int       idx;
  int       chunk;
  uint8_t * pDev;
       
  /*
  ** Distribution is unkown yet
  */
  if (p->device_unknown){
    *pChar++ = '?';
    *pChar = 0;        
    return pChar;
  }
  
  /*
  ** Distribution is known and is stored in a small device list
  */  
  if (p->small_device_array) {
    chunk = ROZOFS_MAX_SMALL_ARRAY_CHUNK;
    pDev  = p->device.small;
  }
  /*
  ** Distribution is known and is stored in a big device list
  */  
  else {
    chunk = ROZOFS_STORAGE_MAX_CHUNK_PER_FILE-1;
    pDev  = p->device.ptr;
  }

  for (idx=0; idx<=chunk;idx++) {
    uint8_t dev = pDev[idx];
    if (dev == ROZOFS_UNKNOWN_CHUNK) {
      *pChar++ = '?';
      break; 
    }   
    if (dev == ROZOFS_EOF_CHUNK) break;

    if (dev == ROZOFS_EMPTY_CHUNK) {
      *pChar++ = 'E';
      *pChar++ = '/';          
    }  
    else {
      pChar += rozofs_u32_append(pChar,dev);
      *pChar++ = '/';          
    }
  }
  *pChar = 0; 
  return pChar;
} 

/*
**___________________________________________________________
** Get a FID value and display it as a string. An end of string
** is inserted at the end of the string.
**
** @param fid     The FID value
** @param pChar   Where to write the ASCII translation
**
** @retval The end of string
*/
static inline void rozofs_uuid_unparse_no_recycle(uuid_t fid, char * pChar) {
  uint8_t * pFid;
  rozofs_inode_t fake_inode;
  
  memcpy(&fake_inode,fid,sizeof(fid_t));
  rozofs_reset_recycle_on_fid(&fake_inode);
//  fake_inode.s.recycle_cpt = 0;
  pFid = (uint8_t *)&fake_inode;
  
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);   
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  
  *pChar++ = '-';
  
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
   
  *pChar++ = '-';
  
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  
  *pChar++ = '-';
  
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  
  *pChar++ = '-';
  
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);   
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);   

  *pChar = 0;  
}
/*
** Build a hdr path on storage disk
**
** @param path       where to write the path
** @param root_path  The cid/sid root path
** @param device     The device to access
** @param spare      Whether this is a path to a spare file
** @param slice      the storage slice number
*/
static inline char * storage_build_hdr_path(char * path, 
                               char * root_path, 
			       uint32_t device, 
			       uint8_t spare, 
			       int     slice) {	          
   path += rozofs_string_append(path,root_path);
   *path++ = '/';
   path += rozofs_u32_append(path,device);
   if (spare) {
     path += rozofs_string_append(path,"/hdr_1/");
   } 
   else { 
     path += rozofs_string_append(path,"/hdr_0/");
   }  
   path += rozofs_u32_append(path,slice);
   *path++ = '/';
   *path = 0;
   return path;
} 
/*
** Build a hdr file path on storage disk
**
** @param path       where to write the path
** @param root_path  The cid/sid root path
** @param device     The device to access
** @param spare      Whether this is a path to a spare file
** @param slice      the storage slice number
** @param fid        The file FID
*/
static inline char * storage_build_hdr_file_path(char * path, 
                               char * root_path, 
			       uint32_t device, 
			       uint8_t spare, 
			       int     slice,
			       fid_t fid) {	          
   path += rozofs_string_append(path,root_path);
   *path++ = '/';
   path += rozofs_u32_append(path,device);
   if (spare) {
     path += rozofs_string_append(path,"/hdr_1/");
   } 
   else { 
     path += rozofs_string_append(path,"/hdr_0/");
   }  
   path += rozofs_u32_append(path,slice);
   *path++ = '/';
   rozofs_uuid_unparse_no_recycle(fid, path);
   path += 36;     
   *path = 0;
   return path;
}    
/*
** Build a bins path on storage disk
**
** @param path       where to write the path
** @param root_path  The cid/sid root path
** @param device     The device to access
** @param spare      Whether this is a path to a spare file
** @param slice      the storage slice number
*/
static inline char * storage_build_bins_path(char * path, 
                               char * root_path, 
			       uint32_t device, 
			       uint8_t spare, 
			       int     slice) {
   path += rozofs_string_append(path,root_path);
   *path++ = '/';
   path += rozofs_u32_append(path,device);
   if (spare) {
     path += rozofs_string_append(path,"/bins_1/");
   } 
   else { 
     path += rozofs_string_append(path,"/bins_0/");
   }  
   path += rozofs_u32_append(path,slice);
   *path++ = '/';
   *path = 0;
   return path;
} 
/*
** Build a chunk file name path on storage disk
**
** @param path       where to write the path
** @param fid        The file fid
** @param chunk      The chunk number
*/
static inline char * storage_build_chunk_path(char * path, 
                        		      fid_t fid, 
					      uint8_t chunk) {	     
    path += strlen(path);
    rozofs_uuid_unparse_no_recycle(fid, path);
    path += 36;    
    *path++ ='-';    
    if (chunk< 10) {
      *path++ = '0';
      *path++ = '0';
    }     
    else if (chunk<100) {
      *path++ = '0';
    }	
    path += rozofs_u32_append(path, chunk);    
    return path;
}     
   
/*
** Build a chunk path on storage disk
**
** @param path       where to write the path
** @param root_path  The cid/sid root path
** @param device     The device to access
** @param spare      Whether this is a path to a spare file
** @param slice      the storage slice number
*/
static inline char * storage_build_slice_path(char * path, 
                               char * root_path, 
			       uint32_t device, 
			       uint8_t spare, 
			       int     slice) {
   path += rozofs_string_append(path,root_path);
   *path++ = '/';
   path += rozofs_u32_append(path,device);
   if (spare) {
     path += rozofs_string_append(path,"/bins_1/");
   } 
   else { 
     path += rozofs_string_append(path,"/bins_0/");
   }  
   path += rozofs_u32_append(path,slice);
   *path++ = '/';
   *path = 0; 
   return path;
}	   
   
/*
** Build a chunk path on storage disk
**
** @param path       where to write the path
** @param root_path  The cid/sid root path
** @param device     The device to access
** @param spare      Whether this is a path to a spare file
** @param slice      the storage slice number
*/
static inline char * storage_build_chunk_full_path(char * path, 
                               char * root_path, 
			       uint32_t device, 
			       uint8_t spare, 
			       int     slice,
			       fid_t   fid, 
			       uint8_t chunk) {
   path += rozofs_string_append(path,root_path);
   *path++ = '/';
   path += rozofs_u32_append(path,device);
   if (spare) {
     path += rozofs_string_append(path,"/bins_1/");
   } 
   else { 
     path += rozofs_string_append(path,"/bins_0/");
   }  
   path += rozofs_u32_append(path,slice);
   *path++ = '/';
   *path = 0;
    rozofs_uuid_unparse_no_recycle(fid, path);
    path += 36;    
    *path++ ='-';
    if (chunk< 10) {
      *path++ = '0';
      *path++ = '0';
    }     
    else if (chunk<100) {
      *path++ = '0';
    }	
    path += rozofs_u32_append(path, chunk);      
    return path;  			       
			       
}			       
/** Remove a chunk of data without modifying the header file 
 *
 * @param st: the storage where the data file resides
 * @param device: device where the data file resides
 * @param fid: the fid of the file 
 * @param spare: wheteher this is a spare file
 * @param chunk: The chunk number that has to be removed
 * @param errlog: whether an log is to be send on error
 */
int storage_rm_data_chunk(storage_t * st, uint8_t device, fid_t fid, uint8_t spare, uint8_t chunk, int errlog) ;

/** Restore a chunk of data as it was before the relocation attempt 
 *
 * @param st: the storage where the data file resides
 * @param fidCtx: FID mapping context
 * @param fid: the fid of the file 
 * @param spare: wheteher this is a spare file
 * @param chunk: The chunk number that has to be removed
 * @param old_device: previous device to be restored
 */
int storage_restore_chunk(storage_t * st, storio_device_mapping_t * fidCtx, fid_t fid, uint8_t spare, 
                           uint8_t chunk, uint8_t old_device);
/** Compute the 
 *
 * @param fid: FID of the file
 * @param modulo: number of mapper devices
 * @param rank: rank of the mapper device within (0..modulo)
 *
 * @return: the device number to hold the mapper file of this FID/nb
 */
static inline int storage_mapper_device(fid_t fid, int rank, int modulo) {
  uint32_t        h = 2166136261;
  unsigned char * d = (unsigned char *) fid;
  int             i;

  /*
  ** hash on fid
  */
  for (i=0; i<sizeof(fid_t); i++,d++) {
    h = (h * 16777619)^ *d;
  }
  return (h+rank) % modulo;
} 
/*
 ** Write a header/mapper file on a device

  @param path : pointer to the bdirectory where to write the header file
  @param hdr : header to write in the file
  
  @retval 0 on sucess. -1 on failure
  
 */
int storage_write_header_file(storage_t * st,int dev, char * path, rozofs_stor_bins_file_hdr_t * hdr);
/*
 ** Write all header/mapper files on a storage

  @param path : pointer to the bdirectory where to write the header file
  @param hdr : header to write in the file
  
  @retval The number of header file that have been written successfuly
  
 */
int storage_write_all_header_files(storage_t * st, fid_t fid, uint8_t spare, rozofs_stor_bins_file_hdr_t * hdr);
 

				    
char *storage_dev_map_distribution_read(  storage_t * st, 
					  uint8_t * device_id,
				           uint8_t chunk,					  
					  fid_t fid, 
					  uint8_t spare, 
					  char *path);	
/** Add the fid bins file to a given path
 *
 * @param fid: unique file id.
 * @param path: the directory path.
 *
 * @return: the directory path
 */
char *storage_map_projection(fid_t fid, char *path);

/** Initialize a storage
 *
 * @param st: the storage to be initialized.
 * @param cid: unique id of cluster that owns this storage.
 * @param sid: the unique id for this storage.
 * @param root: the absolute path.
 * @param device_number: number of device for data storage
 * @param mapper_modulo: number of device for device mapping
 * @param mapper_redundancy: number of mapping device
 * @param mapper_redundancy: number of mapping device
 * @param spare_mark: to tell the exact mark file of spare device
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int storage_initialize(storage_t *st, cid_t cid, sid_t sid, const char *root,
                       uint32_t device_number, uint32_t mapper_modulo, uint32_t mapper_redundancy,
                       const char *spare_mark);

/** Release a storage
 *
 * @param st: the storage to be released.
 */
void storage_release(storage_t * st);

/** Write nb_proj projections
 *
 * @param st: the storage to use.
 * @param fidCtx: FID context that contains the array of devices allocated for the 128 chunks
 * @param layout: layout used for store this file.
 * @param bsize: Block size from enum ROZOFS_BSIZE_E
 * @param dist_set: storages nodes used for store this file.
 * @param spare: indicator on the status of the projection.
 * @param fid: unique file id.
 * @param bid: first block idx (offset).
 * @param nb_proj: nb of projections to write.
 * @param version: version of rozofs used by the client. (not used yet)
 * @param *file_size: size of file after the write operation.
 * @param *bins: bins to store.
 * @param *is_fid_faulty: returns whether a fault is localized in the file
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */

int storage_write_chunk(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, uint8_t chunk, bid_t bid, uint32_t nb_proj, uint8_t version,
        uint64_t *file_size, const bin_t * bins, int * is_fid_faulty);
int storage_write_chunk_empty(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, uint8_t chunk, bid_t bid, uint32_t nb_proj, uint8_t version,
        uint64_t *file_size, int * is_fid_faulty);	 
static inline int storage_write(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, bid_t input_bid, uint32_t input_nb_proj, uint8_t version,
        uint64_t *file_size, const bin_t * bins, int * is_fid_faulty) {
    int ret1,ret2;
    bid_t      bid;
    uint32_t   nb_proj;
    char     * pBins;
    
    dbg("write bid %d nb %d",input_bid,input_nb_proj);
    int block_per_chunk         = ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(bsize);
    int chunk                   = input_bid/block_per_chunk;

    
    if (chunk>=ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) { 
      errno = EFBIG;
      return -1;
    }  
    
    bid = input_bid - (chunk * block_per_chunk);
           
    if ((bid+input_nb_proj) <= block_per_chunk){ 
      /*
      ** Every block can be written in one time in the same chunk
      */
      ret1 = storage_write_chunk(st, fidCtx, layout, bsize, dist_set,
        			 spare, fid, chunk, bid, input_nb_proj, version,
        			 file_size, bins, is_fid_faulty);
      if (ret1 == -1) {
        dbg("storage_write_chunk errno %s",strerror(errno));			     
      }
      return ret1;	 
    }  

    /* 
    ** We have to write two chunks
    */ 
    
    if ((chunk+1)>=ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) { 
      errno = EFBIG;
      return -1;
    }        
    
    // 1rst chunk
    nb_proj = block_per_chunk-bid;
    pBins = (char *) bins;    
    ret1 = storage_write_chunk(st, fidCtx, layout, bsize, dist_set,
        		      spare, fid, chunk, bid, nb_proj, version,
        		      file_size, (bin_t*)pBins, is_fid_faulty); 
    if (ret1 == -1) {
      dbg("storage_write_chunk errno %s",strerror(errno));
      return -1;			     
    }
	    
      
    // 2nd chunk
    chunk++;         
    bid     = 0;
    nb_proj = input_nb_proj - nb_proj;
    pBins += ret1;  
    ret2 = storage_write_chunk(st, fidCtx, layout, bsize, dist_set,
        		      spare, fid, chunk, bid, nb_proj, version,
        		      file_size, (bin_t*)pBins, is_fid_faulty); 
    if (ret2 == -1) {
      dbg("storage_write_chunk errno %s",strerror(errno));
      return -1;			     
    }
    
    return ret2+ret1;
}
static inline int storage_write_empty(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, bid_t input_bid, uint32_t input_nb_proj, uint8_t version,
        uint64_t *file_size, int * is_fid_faulty) {
    int ret1,ret2;
    bid_t      bid;
    uint32_t   nb_proj;
    
    dbg("write bid %d nb %d",input_bid,input_nb_proj);
    int block_per_chunk         = ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(bsize);
    int chunk                   = input_bid/block_per_chunk;

    
    if (chunk>=ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) { 
      errno = EFBIG;
      return -1;
    }  
    
    bid = input_bid - (chunk * block_per_chunk);
           
    if ((bid+input_nb_proj) <= block_per_chunk){ 
      /*
      ** Every block can be written in one time in the same chunk
      */
      ret1 = storage_write_chunk_empty(st, fidCtx, layout, bsize, dist_set,
        			 spare, fid, chunk, bid, input_nb_proj, version,
        			 file_size, is_fid_faulty);
      if (ret1 == -1) {
        dbg("storage_write_chunk errno %s",strerror(errno));			     
      }
      return ret1;	 
    }  

    /* 
    ** We have to write two chunks
    */ 
    
    if ((chunk+1)>=ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) { 
      errno = EFBIG;
      return -1;
    }        
    
    // 1rst chunk
    nb_proj = block_per_chunk-bid;
    ret1 = storage_write_chunk_empty(st, fidCtx, layout, bsize, dist_set,
        		      spare, fid, chunk, bid, nb_proj, version,
        		      file_size, is_fid_faulty); 
    if (ret1 == -1) {
      dbg("storage_write_chunk errno %s",strerror(errno));
      return -1;			     
    }
	    
      
    // 2nd chunk
    chunk++;         
    bid     = 0;
    nb_proj = input_nb_proj - nb_proj;

    ret2 = storage_write_chunk_empty(st, fidCtx, layout, bsize, dist_set,
        		      spare, fid, chunk, bid, nb_proj, version,
        		      file_size, is_fid_faulty); 
    if (ret2 == -1) {
      dbg("storage_write_chunk errno %s",strerror(errno));
      return -1;			     
    }
    
    return ret2+ret1;
}
/** Write nb_proj projections
 *
 * @param st: the storage to use.
 * @param device: Array of device allocated for the 128 chunks
 * @param layout: layout used for store this file.
 * @param bsize: Block size from enum ROZOFS_BSIZE_E
 * @param dist_set: storages nodes used for store this file.
 * @param spare: indicator on the status of the projection.
 * @param fid: unique file id.
 * @param bid: first block idx (offset).
 * @param nb_proj: nb of projections to write.
 * @param version: version of rozofs used by the client. (not used yet)
 * @param *file_size: size of file after the write operation.
 * @param *bins: bins to store.
 * @param *is_fid_faulty: returns whether a fault is localized in the file
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */

#if 1
#define repairdbg(fmt,...)
#else
#define repairdbg(fmt,...) info(fmt,__VA_ARGS__)
#endif
char * storage_write_repair3_chunk(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, uint8_t chunk, bid_t bid, uint32_t nb_proj, sp_b2rep_t * blk2repair, uint8_t version,
        uint64_t *file_size, const bin_t * bins, int * is_fid_faulty);

static inline int storage_write_repair3(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, bid_t input_bid, uint32_t input_nb_proj, sp_b2rep_t * blk2repair, uint8_t version,
        uint64_t *file_size, const bin_t * bins, int * is_fid_faulty) {
    bid_t      bid;
    char   * pBins;    
    int      block_per_chunk         = ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(bsize);
    int      chunk                   = input_bid/block_per_chunk;
    int      last_relative_block_idx_in_1rst_chunk;
    int      first_block2repair_in_2nd_chunk;
    int      idx;

    
    if (chunk>=ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) { 
      errno = EFBIG;
      return -1;
    }  
    
    bid = input_bid - (chunk * block_per_chunk);
         
    repairdbg ("repair3 input_bid %llu = chunk/block %d/%d  %d blocks to rebuild. %d block per chunk.",
	          (long long unsigned int)input_bid,chunk,(int)bid, input_nb_proj,block_per_chunk); 
    /*
    ** Find out the last block number to repair
    */
    if (input_nb_proj==0) {
      severe("storage_write_repair3 with no block");
      return -1;
    }
    if (input_nb_proj>ROZOFS_MAX_REPAIR_BLOCKS) {
      severe("storage_write_repair3 too much blocks %d > %d",input_nb_proj,ROZOFS_MAX_REPAIR_BLOCKS);
      return -1;
    }  
    if (blk2repair[input_nb_proj-1].relative_bid > (3*64)) {
      severe("storage_write_repair3 blk2repair[%d].relative_bid = %d (192)",input_nb_proj-1,blk2repair[input_nb_proj-1].relative_bid);
      return -1;
    }  

    
    
    /*
    ** Compute the relative index of the last block of the chunk
    */
    last_relative_block_idx_in_1rst_chunk =  block_per_chunk - 1 - bid;     
	   
    if (last_relative_block_idx_in_1rst_chunk >= blk2repair[input_nb_proj-1].relative_bid) { 
      repairdbg("all in one chunk %d",chunk);
    
      /*
      ** Every block can be written in one time in the same chunk
      */
      pBins = storage_write_repair3_chunk(st, fidCtx, layout, bsize, dist_set,
        			 spare, fid, chunk, bid, input_nb_proj, &blk2repair[0], version,
        			 file_size, bins, is_fid_faulty);
      if (pBins == NULL) {
        dbg("write errno %s",strerror(errno));	
        return -1;		     
      }
      return 0;	 
    }  

    /* 
    ** We have to repair two chunks
    */     
    
    if ((chunk+1)>=ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) { 
      errno = EFBIG;
      return -1;
    }        

    /*
    ** Find out the last block in the blocks to repair within the 1rst chunk
    */
    for (first_block2repair_in_2nd_chunk=0; first_block2repair_in_2nd_chunk < input_nb_proj; first_block2repair_in_2nd_chunk++) {
      if (blk2repair[first_block2repair_in_2nd_chunk].relative_bid > last_relative_block_idx_in_1rst_chunk) break; 
    }
     
    repairdbg("1rst chunk : bid %d repair %d blocks",(int)bid, first_block2repair_in_2nd_chunk);
    

    pBins = (char *) bins; 
        
    if (first_block2repair_in_2nd_chunk) {
      pBins = storage_write_repair3_chunk(st, fidCtx, layout, bsize, dist_set,
        			   spare, fid, chunk, bid, first_block2repair_in_2nd_chunk, &blk2repair[0], version,
        			   file_size, (bin_t*)pBins, is_fid_faulty);    
      if (pBins == NULL) {
	dbg("write errno %s",strerror(errno));
	return -1;			     
      }
    }
	    
      
    // 2nd chunk
    chunk++;         
    bid = 0;

    /*
    ** Find out the last block in the blocks to repair within the 1rst chunk
    */
    for (idx = first_block2repair_in_2nd_chunk;idx < input_nb_proj; idx++) {
      blk2repair[idx].relative_bid -= (last_relative_block_idx_in_1rst_chunk+1); 
    }
     
    
    repairdbg("2nd chunk : bid %d repair %d blocks",(int)bid, input_nb_proj-first_block2repair_in_2nd_chunk);
    
    
    pBins = storage_write_repair3_chunk(st, fidCtx, layout, bsize, dist_set,
        			 spare, fid, chunk, bid, input_nb_proj-first_block2repair_in_2nd_chunk, &blk2repair[first_block2repair_in_2nd_chunk], version,
        			 file_size, (bin_t*)pBins, is_fid_faulty);    
    if (pBins == NULL) {
      dbg("write errno %s",strerror(errno));
      return -1;			     
    }
    
    return 0;
}

/** Read nb_proj projections
 *
 * @param st: the storage to use.
 * @param fidCtx: FID context that contains the array of devices allocated for the 128 chunks
 * @param layout: layout used by this file.
 * @param bsize: Block size from enum ROZOFS_BSIZE_E
 * @param dist_set: storages nodes used for store this file.
 * @param spare: indicator on the status of the projection.
 * @param fid: unique file id.
 * @param bid: first block idx (offset).
 * @param nb_proj: nb of projections to read.
 * @param *bins: bins to store.
 * @param *len_read: the length read.
 * @param *file_size: size of file after the read operation.
 * @param *is_fid_faulty: returns whether a fault is localized in the file
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int storage_read_chunk(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, uint8_t chunk, bid_t bid, uint32_t nb_proj,
        bin_t * bins, size_t * len_read, uint64_t *file_size,int * is_fid_faulty) ;
static inline int storage_read(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, bid_t input_bid, uint32_t input_nb_proj,
        bin_t * bins, size_t * len_read, uint64_t *file_size,int * is_fid_faulty) {
    bid_t    bid;
    uint32_t nb_proj;
    char * pBins = (char *) bins;
    size_t len_read1,len_read2;
    int    ret1,ret2;

    dbg("read bid %d nb %d",input_bid,input_nb_proj);

    *len_read = len_read1 = len_read2 = 0;       
    int block_per_chunk         = ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(bsize);
    int chunk                   = input_bid/block_per_chunk;
    
    if (chunk>=ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) {  
      return 0;
    }        
    
    bid = input_bid - (chunk * block_per_chunk);
    if ((bid+input_nb_proj) <= block_per_chunk){ 
      /*
      ** All the blocks can be read in one time in the same chunk
      */
      ret1 = storage_read_chunk(st, fidCtx, layout, bsize, dist_set,
        			spare, fid, chunk, bid, input_nb_proj,
        			bins, len_read, file_size,is_fid_faulty);
      if (ret1 == -1) {
        dbg("read error len %d errno %s",*len_read,strerror(errno));			     
        return -1;
      }
      /*
      ** When this chunk is not the last one and we have read less than requested
      ** one has to pad with 0 the missing data (whole in file)
      */
      if (*len_read < ret1) {
        chunk++;
        if (chunk<ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) {          
          if (storio_get_dev(fidCtx, chunk) != ROZOFS_EOF_CHUNK) {
	    pBins += *len_read;
	    memset(pBins,0,ret1-*len_read);
	    *len_read = ret1;
	  }  
        }
      }	      
      dbg("read success len %d",*len_read);			           	
      return 0;				  
    }  


    /* 
    ** We have to read from two chunks
    */   
    
    /*
    *_____ 1rst chunk
    */
    nb_proj = (block_per_chunk-bid);

    ret1 = storage_read_chunk(st, fidCtx, layout, bsize, dist_set,
        		      spare, fid, chunk, bid, nb_proj,
        		      (bin_t*) pBins, &len_read1, file_size,is_fid_faulty);
    if (ret1 == -1) {
      dbg("read error len %d errno %s",*len_read,strerror(errno));			     		    
      return -1;
    }

    /*
    ** Is there a next chunk ?
    */
    chunk++;
    if (chunk>=ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) {
      *len_read = len_read1;
      dbg("read success len %d",*len_read);			           	
      return 0;	
    }
    
    if (storio_get_dev(fidCtx, chunk) == ROZOFS_EOF_CHUNK) {
      *len_read = len_read1;
      dbg("read success len %d",*len_read);			           	
      return 0;	
    }
    
        
    /*
    ** When this chunk is not the last one and we have read less than requested
    ** one has to pad with 0 the missing data (whole in file)
    */
    if (len_read1 < ret1) {
      pBins += len_read1;
      memset(pBins,0,ret1-len_read1);
      len_read1 = ret1;
    }
          
    /*
    *_____ 2nd chunk
    */
    bid = 0;
    nb_proj = input_nb_proj - nb_proj;  
    pBins = (char *) bins;
    pBins += ret1;  
    ret2 = storage_read_chunk(st, fidCtx, layout, bsize, dist_set,
        		      spare, fid, chunk, bid, nb_proj,
        		      (bin_t*) pBins, &len_read2, file_size,is_fid_faulty);
    if (ret2 == -1) {
      dbg("read error len %d errno %s",*len_read,strerror(errno));			     		    
      return -1;
    }     

    /*
    ** Is there a next chunk ?
    */
    chunk++;
    if (chunk>=ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) {
      *len_read = len_read1 + len_read2;
      dbg("read success len %d",*len_read);			           	
      return 0;	
    }
    
    if (storio_get_dev(fidCtx, chunk) == ROZOFS_EOF_CHUNK) {
      *len_read = len_read1 + len_read2;
      dbg("read success len %d",*len_read);			           	
      return 0;	
    }
            
    /*
    ** When this chunk is not the last one and we have read less than requested
    ** one has to pad with 0 the missing data (whole in file)
    */
    if (len_read2 < ret2) {
      pBins += len_read2;
      memset(pBins,0,ret2-len_read2);
      len_read2 = ret2;
    }    
    *len_read = len_read1 + len_read2;
    dbg("read success len %d",*len_read);			           	
    return 0;

}
/** Relocate a chunk on a new device in a process of rebuild. 
 *  This just consist in changing in the file distribution the 
 *  chunk to empty, but not removing the data in order to be 
 *  able to restore it later when the rebuild fails...
 * 
 * @param st: the storage to use.
 * @param fidCtx: FID mapping context
 * @param fid: unique file id.
 * @param spare: indicator on the status of the projection.
 * @param chunk: the chunk that is to be rebuilt with relocate
 * @param old_device: to return the old device value 
 * 
 * @return: 0 on success -1 otherwise (errno is set)
 */
int storage_relocate_chunk(storage_t * st, storio_device_mapping_t * fidCtx,fid_t fid, uint8_t spare, 
                           uint8_t chunk, uint8_t * old_device);
			   
/** Truncate a bins file (not used yet)
 *
 * @param st: the storage to use.
 * @param fidCtx: FID context that contains the array of devices allocated for the 128 chunks
 * @param layout: layout used by this file.
 * @param bsize: Block size from enum ROZOFS_BSIZE_E
 * @param dist_set: storages nodes used for store this file.
 * @param spare: indicator on the status of the projection.
 * @param fid: unique file id.
 * @param proj_id: the projection id.
 * @param bid: first block idx (offset).
 * @param last_seg: length of the last segment if not modulo prj. size
 * @param last_timestamp: timestamp to associate with the last_seg
 * @param len: the len to writen in the last segment
 * @param data: the data of the last segment to write
 * @param is_fid_faulty: returns whether a fault is localized in the file
 
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int storage_truncate(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, tid_t proj_id,bid_t bid,uint8_t version,
         uint16_t last_seg,uint64_t last_timestamp,u_int len, char * data,int * is_fid_faulty);

/** Remove a bins file
 *
 * @param st: the storage to use.
 * @param fid: unique file id.
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int storage_rm_file(storage_t * st, fid_t fid);

/** Remove a bins file
 *
 * @param st: the storage to use.
 * @param fid: unique file id.
 * @param spare: whether the storage is spare or not for this FID
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int storage_rm2_file(storage_t * st, fid_t fid, uint8_t spare);

/** Stat a storage
 *
 * @param st: the storage to use.
 * @param sstat: structure to use for store stats about this storage.
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int storage_stat(storage_t * st, sstat_t * sstat);


int storage_list_bins_files_to_rebuild(storage_t * st, sid_t sid,  uint8_t * device_id,
        uint8_t * spare, uint16_t * slice, uint64_t * cookie,
        bins_file_rebuild_t ** children, uint8_t * eof);



/*
 ** Create a directory if it does not yet exist
  @param path : path toward the directory
  
  @retval 0 on success
  
 */
static inline int storage_create_dir(char *path) {

  /* Directory exist */
  if (access(path, F_OK) == 0) return 0;
  
  /* Unhandled error on directory access */
  if (errno != ENOENT) return -1;
  
  /* The directory doesn't exist, let's create it */
  if (mkdir(path, ROZOFS_ST_DIR_MODE) == 0) return 0;
  
  /* Someone else has just created the directory */ 
  if (errno == EEXIST) return 0;	
  	

  /* Unhandled error on directory creation */
  return -1;
}  
/** Remove header files from disk
 *
 * @param st: the storage to use.
 * @param fid: FID of the file
 * @param dist_set: distribution set of the file
 * @param spare: whether this is a spare sid
*
 * @return: 0 on success -1 otherwise (errno is set)
 */	
void static inline storage_dev_map_distribution_remove(storage_t * st, fid_t fid, uint8_t spare) {
    char                      path[FILENAME_MAX];
    int                       dev;
    int                       hdrDevice;

    DEBUG_FUNCTION;
 
    /*
    ** Compute storage slice from FID
    */
    int storage_slice = rozofs_storage_fid_slice(fid);

   /*
   ** Loop on the reduncant devices that should hold a copy of the mapping file
   */
   for (dev=0; dev < st->mapper_redundancy ; dev++) {

       hdrDevice = storage_mapper_device(fid,dev,st->mapper_modulo);	
       storage_build_hdr_file_path(path, st->root, hdrDevice, spare, storage_slice, fid);

       // Check that the file exists
       if (access(path, F_OK) == -1) continue;

       // The file exist, let's remove it
       unlink(path);
   }
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
typedef enum { 
  STORAGE_READ_HDR_OK,
  STORAGE_READ_HDR_NOT_FOUND,
  STORAGE_READ_HDR_ERRORS,
  STORAGE_READ_HDR_OTHER_RECYCLING_COUNTER
} STORAGE_READ_HDR_RESULT_E;


STORAGE_READ_HDR_RESULT_E storage_read_header_file(storage_t                   * st, 
                                                   fid_t                         fid, 
						   uint8_t                       spare, 
						   rozofs_stor_bins_file_hdr_t * hdr, 
						   int                           update_recycle);
						   
						   
						   

int storage_rm_chunk(storage_t * st, storio_device_mapping_t * fidCtx, 
                     uint8_t layout, uint8_t bsize, uint8_t spare, 
		     sid_t * dist_set, fid_t fid, 
		     uint8_t chunk, int * is_fid_faulty);

/*
 ** Name the storio pid file for the rozolauncher

  @param pidfile           : the name of the storio pid file
  @param storaged_hostname : hostname
  @param instance          : cstorio instance
  
 */
static inline void storio_pid_file(char * pidfile, char * storaged_hostname, int instance) {

  pidfile += rozofs_string_append(pidfile,"/var/run/launcher_storio_");
  if (storaged_hostname) {
    pidfile += rozofs_string_append(pidfile,storaged_hostname);
    *pidfile++ = '_';
  }
  pidfile += rozofs_u32_append(pidfile,instance);
  pidfile += rozofs_string_append(pidfile, ".pid");

}
/*
 ** Name the storaged spare restorer pid file for the rozolauncher

  @param pidfile           : the name of the pid file
  @param storaged_hostname : hostname
  
 */
static inline void storaged_spare_restorer_pid_file(char * pidfile, char * storaged_hostname) {

  pidfile += rozofs_string_append(pidfile,"/var/run/launcher_storaged_spare_restorer");
  if (storaged_hostname) {
    *pidfile++ = '_';
    pidfile += rozofs_string_append(pidfile,storaged_hostname);
  }
  pidfile += rozofs_string_append(pidfile, ".pid");

}

int storage_resize(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, bin_t * bins, uint32_t * nb_blocks, uint32_t * last_block_size, int * is_fid_faulty);
/*
** Create sub directories structure of a storage node
**  
** @param st    The storage context
*/
int storage_subdirectories_create(storage_t *st);
/*
 *_______________________________________________________________________
 *
 *  Try to mount the devices on the convenient path
 *
 * @param workDir   A directory to use to temporary mount the available 
 *                  devices on in order to check their content.
 * @param count     Returns the number of devices that have been mounted
 */
void storaged_do_automount_devices(char * workDir, int * count);
void storio_do_automount_devices(char * workDir, int * count);
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
int storage_replace_with_spare(storage_t * st, int dev);
/*
** Reset memory log off encountered errors
*/
void storio_device_error_log_reset();


uint32_t storio_device_mapping_allocate_device(storage_t * st, uint8_t layout, sid_t * distrib);
/*
**____________________________________________________
**
** Allocate a device for a new chunk of a given file.
** If the storage has #devices, the 1rst chunk within 
** a slice of #devices is allocated following the 
** round robin rule. The next chunks are allocated in
** order to spread the chunks on every available devices.
**
** @param fidCtx    : the FID mapping context 
** @param chunk     : the chunk number to allocate
** @param st        : storage context
** @param layout    : layout to be used for this file
** @param distrib   : File projection distribution within the cluster
*/
uint32_t storio_device_mapping_new_chunk(uint16_t                  chunk,
                                         storio_device_mapping_t * fidCtx,
                                         storage_t               * st, 
                                         uint8_t                   layout, 
                                         sid_t                   * distrib) ;
/*
** 
** Create RozoFS storage subdirectories on a device
**
** @param root   storage root path
** @param dev    device number
**  
*/
void rozofs_storage_device_subdir_create(char * root, int dev);

/**
 *_______________________________________________________________________
 * Get the mount path a device is mounted on
 * 
 * @param dev the device
 *
 * @return: Null when not found, else the mount point path
 */
char * rozofs_get_device_mountpath(const char * dev) ;
/**
 *_______________________________________________________________________
 * Check a given mount point is actually mounted
 * 
 * @param mount_path The mount point to check
 *
 * @return: 1 when mounted else 0
 */
int rozofs_check_mountpath(const char * mntpoint);
/*_____________________________________________________________________________
** Compute the size of a file as well as the number of allocated sectors
** from  stat() on every projection file.
**
** @param st            The logical storage context
** @param fid           The FID of the target file
** @param spare         Whether this storage is spare for this file
** @param nb_chunk      returned number of chunk
** @param size_B        returned size of projections
** @param allocated_KB  returned allocated sectors of projections
**
** @retval 0 on success. -1 on error (errno is set)
**_____________________________________________________________________________
*/
int storage_size_file(storage_t * st, fid_t fid, uint8_t spare, uint32_t * nb_chunk, uint64_t * size_B, uint64_t * allocated_KB);
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
char * rozofs_st_header_read(char * path, cid_t cid, sid_t sid, fid_t fid, rozofs_stor_bins_file_hdr_t * hdr) ;
#endif

