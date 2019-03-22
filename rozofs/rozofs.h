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

#ifndef _ROZOFS_H
#define _ROZOFS_H

#include <stdint.h>
#include <uuid/uuid.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <rozofs/common/log.h>
#include <sys/mman.h>
#include <unistd.h>

#include <config.h>
#include <rozofs/common/common_config.h>
#include <rozofs/core/rozofs_string.h>

#define ROZOFS_RUNDIR                "/var/run/rozofs/"
#define ROZOFS_RUNDIR_RBS            ROZOFS_RUNDIR"rbs/"
#define ROZOFS_RUNDIR_RBS_SPARE      ROZOFS_RUNDIR_RBS"spare/"
#define ROZOFS_RUNDIR_RBS_REBUILD    ROZOFS_RUNDIR_RBS"rebuild/"
#define ROZOFS_RUNDIR_CORE           ROZOFS_RUNDIR"core/"
#define ROZOFS_RUNDIR_PID            ROZOFS_RUNDIR"pid/"
#define ROZOFS_DIR_TRASH                 "@rozofs-trash@"

#define ROZOFS_KPI_ROOT_PATH "/var/run/rozofs_kpi"
//#include <rozofs/common/log.h>
/*
** Get ticker
*/
static __inline__ unsigned long long rdtsc(void)
{
  unsigned hi,lo;
  __asm__ volatile("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo)| (((unsigned long long)hi)<<32);

}

/*
** To associate with STORCLI_READ for read multiple
*/
#define ROZOFS_SHMEM_READ_PAYLOAD_OFF 4096 /**< need to be aligned on page boundary for performance purpose */
#define ROZOFS_SHMEM_READ_MAX_CMD 4
typedef struct _rozofs_shmem_cmd_read_t
{
    uint32_t xid;                /**< XID of the transaction (IN)   */
    uint32_t received_len;       /**< number of bytes read (OUT) */
    uint32_t offset_in_buffer;   /**< offset of the data in the shared buffer (IN) */
    uint32_t filler;             /**< for future usage          */
    uint64_t inode;              /**< reference of the inode for the case of the direct write in page cache (by-pass shared read/write buffer). */
    uint64_t f_offset;           /**< offset within the target file     */
} rozofs_shmem_cmd_read_t;
    

typedef struct _rozofs_shared_buf_rd_hdr_t
{
   rozofs_shmem_cmd_read_t cmd[ROZOFS_SHMEM_READ_MAX_CMD]; 
} rozofs_shared_buf_rd_hdr_t;

/*
** To associate with STORCLI_WRITE for write multiple
*/

#define ROZOFS_SHMEM_WRITE_PAYLOAD_OFF 128 /**< need to start on a 128 bits boundady */
#define ROZOFS_SHMEM_WRITE_MAX_CMD 4
typedef struct _rozofs_shmem_cmd_write_t
{
    uint32_t xid;                /**< XID of the transaction (IN)   */
    uint32_t write_len;       /**< number of bytes to write (IN) */
    uint32_t offset_in_buffer;   /**< offset of the data in the shared buffer (IN) */
} rozofs_shmem_cmd_write_t;
    

typedef struct _rozofs_shared_buf_wr_hdr_t
{
   rozofs_shmem_cmd_write_t cmd[ROZOFS_SHMEM_WRITE_MAX_CMD]; 
} rozofs_shared_buf_wr_hdr_t;


/*
**_____________________________________________________________________________
** 
** Cluster administrative status
**
*/
typedef enum _rozofs_cluster_admin_status_e {
  rozofs_cluster_admin_status_undefined = 0,
  rozofs_cluster_admin_status_in_service,
  rozofs_cluster_admin_status_frozen
} rozofs_cluster_admin_status_e;
#include <rozofs/rozofs_cluster_admin_status_e2String.h>


/*
**_____________________________________________________________________________
** File distribution rules
**
*/
typedef enum _rozofs_file_distribution_rule_e {
  rozofs_file_distribution_size_balancing,
  rozofs_file_distribution_round_robin_write_1,
  rozofs_file_distribution_round_robin_write_2,
  rozofs_file_distribution_round_robin_write_3,  
  rozofs_file_distribution_round_robin_read,  
  rozofs_file_distribution_wsid_write,  
  rozofs_file_distribution_wsid_read,  
  rozofs_file_distribution_wfsz_write,  
  rozofs_file_distribution_wfsz_read,  
  rozofs_file_distribution_max
} rozofs_file_distribution_rule_e;

#include <rozofs/rozofs_file_distribution_rule_e2String.h>
/*
**_____________________________________________________________________________
** List of existing cluster distribution rule 
*/
typedef enum _rozofs_cluster_distribution_rule_e {
  //  Cluster size balancing 
  rozofs_cluster_distribution_rule_size_balancing,
  //  Cluster weighted round robin 
  rozofs_cluster_distribution_rule_weighted_round_robin,
} rozofs_cluster_distribution_rule_e;
#include <rozofs/rozofs_cluster_distribution_rule_e2String.h>

/*
**_____________________________________________________________________________
** Get the cluster distribution rule from the file distribution rule
**
** @param rule    The file distribution rule
**
** retval the cluster distribution rule deduced from the file distribution rule
**_____________________________________________________________________________
*/
static inline rozofs_cluster_distribution_rule_e rozofs_get_cluster_distribution_rule(rozofs_file_distribution_rule_e rule) {
  switch(rule) {
    
    /*
    ** Cluster size balancing
    */
    case rozofs_file_distribution_size_balancing: 
      return rozofs_cluster_distribution_rule_size_balancing;

    /*
    ** Cluster weighted round robin
    */   
    case rozofs_file_distribution_round_robin_write_1: 
    case rozofs_file_distribution_round_robin_write_2:
    case rozofs_file_distribution_round_robin_write_3:
    case rozofs_file_distribution_round_robin_read: 
    case rozofs_file_distribution_wsid_write:
    case rozofs_file_distribution_wsid_read: 
    case rozofs_file_distribution_wfsz_write:  
    case rozofs_file_distribution_wfsz_read:  
      return rozofs_cluster_distribution_rule_weighted_round_robin;

    default:
      severe("Unexpected distribution rule %d. Apply sb",rule);
      break;     
  }
  return rozofs_cluster_distribution_rule_size_balancing;
}
/*
**_____________________________________________________________________________
** List of existing device distribution rule 
*/
typedef enum _rozofs_device_distribution_rule_e {
  //  Device size balancing 
  rozofs_device_distribution_rule_size_balancing,
  //  Device write round robin 
  rozofs_device_distribution_rule_write,
  //  Device read round robin 
  rozofs_device_distribution_rule_read,
} rozofs_device_distribution_rule_e;
#include <rozofs/rozofs_device_distribution_rule_e2String.h>
/*
**_____________________________________________________________________________
** Get the device distribution rule from the file distribution rule
**
** @param rule    The file distribution rule
**
** retval the device distribution rule deduced from the file distribution rule
**_____________________________________________________________________________
*/
static inline rozofs_device_distribution_rule_e rozofs_get_device_distribution_rule(rozofs_file_distribution_rule_e rule) {
  switch(rule) {

    /*  
    ** Device size balancing 
    */
    case rozofs_file_distribution_size_balancing: 
      return rozofs_device_distribution_rule_size_balancing;

    /*  
    ** Device write round robin
    */    
    case rozofs_file_distribution_round_robin_write_1: 
    case rozofs_file_distribution_round_robin_write_2:
    case rozofs_file_distribution_round_robin_write_3: 
    case rozofs_file_distribution_wsid_write:
    case rozofs_file_distribution_wfsz_write:  
      return rozofs_device_distribution_rule_write;      


    /*  
    ** Device read round robin
    */    
    case rozofs_file_distribution_round_robin_read: 
    case rozofs_file_distribution_wsid_read: 
    case rozofs_file_distribution_wfsz_read:  
      return rozofs_device_distribution_rule_read;
      
    default:
      severe("Unexpected distribution rule %d. Apply sb",rule);
      break;   
  }
  return rozofs_device_distribution_rule_size_balancing;
}






#ifndef GETMICROLONG
#define GETMICROLONG(usec) {\
  struct timeval tv;\
  gettimeofday(&tv,(struct timezone *)0);\
  usec = MICROLONG(tv);\
}    
#endif



/*
**
** Bitmap of export options returned on the mount inside the msite field
**
*/
#define ROZOFS_EXPORT_MSITE_BIT              (1<<0)
#define ROZOFS_EXPORT_THIN_PROVISIONNING_BIT (1<<1)



/*
** ___________________________________________________________________________________
**
**       SOME USEFULL MACROS FOR BITMAPS BUILD UPON ARRAY OF 64 BIT UNSIGNED
** ___________________________________________________________________________________
**
*/

#define ROZOFS_BITMAP64_BIT(bitNumber)            ( 1ULL<<((bitNumber)%64) )
#define ROZOFS_BITMAP64_IDX(bitNumber)            ( (bitNumber)/64 )
 
#define ROZOFS_BITMAP64_SET(bitNumber, bitmap)    ( bitmap[ROZOFS_BITMAP64_IDX(bitNumber)] |=  ROZOFS_BITMAP64_BIT(bitNumber) ) 
#define ROZOFS_BITMAP64_RESET(bitNumber, bitmap)  ( bitmap[ROZOFS_BITMAP64_IDX(bitNumber)] &=  ~ROZOFS_BITMAP64_BIT(bitNumber) )
#define ROZOFS_BITMAP64_TEST0(bitNumber, bitmap)  ( (bitmap[ROZOFS_BITMAP64_IDX(bitNumber)] & ROZOFS_BITMAP64_BIT(bitNumber)) == 0 )
#define ROZOFS_BITMAP64_TEST1(bitNumber, bitmap)  ( (bitmap[ROZOFS_BITMAP64_IDX(bitNumber)] & ROZOFS_BITMAP64_BIT(bitNumber)) != 0 )

#define ROZOFS_BITMAP64_NB_UINT64(nb)             ( ROZOFS_BITMAP64_IDX(nb-1) + 1 )

#define ROZOFS_BITMAP64_ALL_RESET(bitmap) memset(bitmap, 0, sizeof(bitmap))
#define ROZOFS_BITMAP64_ALL_SET(bitmap)   memset(bitmap, 0xFF, sizeof(bitmap))

static inline int ROZOFS_BITMAP64_TEST_ALL1_FUNC(uint64_t * bitmap, int sz64)  {
  while(--sz64>=0) { 
    if (bitmap[sz64] != 0xFFFFFFFF) return 0; 
  }
  return 1;
}   
#define ROZOFS_BITMAP64_TEST_ALL1(bitmap)  (ROZOFS_BITMAP64_TEST_ALL1_FUNC(bitmap,sizeof(bitmap)/sizeof(uint64_t)))

static inline int ROZOFS_BITMAP64_TEST_ALL0_FUNC(uint64_t * bitmap, int sz64)  {
  while(--sz64>=0) {
    if (bitmap[sz64] != 0) return 0;
  }  
  return 1;
} 
#define ROZOFS_BITMAP64_TEST_ALL0(bitmap)  (ROZOFS_BITMAP64_TEST_ALL0_FUNC(bitmap,sizeof(bitmap)/sizeof(uint64_t)))

static inline int ROZOFS_BITMAP64_NB_BIT1_FUNC(uint8_t * p8, int sz8)  {
  int count=0;
  uint8_t   mask;
  int       i;

  while(--sz8>=0) {
    if (*p8 != 0) {
      mask=1;  
      for (i=0; i<8; i++,mask=mask<<1) {
        if (mask & *p8) {
          count++;
        }
      }
    }  
    p8++;
  }
  return count;
}
#define ROZOFS_BITMAP64_NB_BIT1(bitmap)  ROZOFS_BITMAP64_NB_BIT1_FUNC((uint8_t *)bitmap, sizeof(bitmap))

static inline int ROZOFS_BITMAP64_NB_BIT0_FUNC(uint8_t * p8, int sz8)  {
  int count=0;
  uint8_t   mask;
  int       i;
    
  while(--sz8>=0) {
    if (*p8 != 0xFF) {
      mask=1;  
      for (i=0; i<8; i++,mask=mask<<1) {
        if ((mask &*p8) == 0) count++;
      }
    }
    p8++;
  }
  return count;  
}
#define ROZOFS_BITMAP64_NB_BIT0(bitmap)  ROZOFS_BITMAP64_NB_BIT0_FUNC((uint8_t *)bitmap, sizeof(bitmap))

/*___________________________________________________________________________________
*
* Run a command and exit
*
* @param cmd  The command to run
*
*/
static inline void rozofs_run_until_exit(char * cmd) {
  int    idx;
  char * pChar = cmd;
  char * argv[64];
    
  while (*pChar == ' ') pChar++;    

  idx   = 0;
  while ( idx < 62 ) {
    argv[idx++] = pChar;
    while ((*pChar != ' ') && (*pChar != 0)) pChar++;
    if (*pChar == 0) break;
    *pChar = 0;
    pChar++;
    while (*pChar == ' ') pChar++;    
  }  
  argv[idx] = NULL;

  execvp(argv[0],&argv[0]);
  int error = errno;
  exit(error); 
}










/**
* Ports definition of RozoFS
*/

#include "rozofs_service_ports.h"

#define P_COUNT     0
#define P_ELAPSE    1
#define P_BYTES     2

#define MICROLONG(time) ((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)


#define ROZOFS_UUID_SIZE 16
/* Instead of using an array of unsigned char for store the UUID, we use an
 * array of uint32_t in the RPC protocol to use less space (see XDR). */
#define ROZOFS_UUID_SIZE_RPC (ROZOFS_UUID_SIZE/sizeof(uint32_t))
#define ROZOFS_UUID_SIZE_NET ROZOFS_UUID_SIZE_RPC
#define ROZOFS_HOSTNAME_MAX 128
/*
** The block size is dependant on the eid
**
** NOTE: THE EFFECTIVE LENGTH IN THE BLOCK HEADER ON DISK IS 16 BITS
**       LONG. THE MAXIMUM BLOCK SIZE IS SO (64K-1)
*/
typedef enum _ROZOFS_BSIZE_E {
  ROZOFS_BSIZE_4K,
  ROZOFS_BSIZE_8K,
  ROZOFS_BSIZE_16K,
  ROZOFS_BSIZE_32K,
} ROZOFS_BSIZE_E ;
#define ROZOFS_BSIZE_MIN        ROZOFS_BSIZE_4K
#define ROZOFS_BSIZE_MAX        ROZOFS_BSIZE_32K
#define ROZOFS_BSIZE_NB         (ROZOFS_BSIZE_MAX+1)
#define ROZOFS_BSIZE_BYTES(val) ((4*1024)<<val)
// Maximum number of block per message 
#define ROZOFS_DEFAULT_KERNEL_BUF_SZ (512*1024)
#define ROZOFS_MAX_FILE_BUF_SZ_READ (512*1024)
#define ROZOFS_MAX_BLOCK_PER_MSG ((ROZOFS_MAX_FILE_BUF_SZ_READ/4096)+1)

// The number of 64bit words necessary for a bitmap of blocks of a message
#define ROZOFS_BLOCK_BITMAP_NB_UINT64 (ROZOFS_BITMAP64_NB_UINT64(ROZOFS_MAX_BLOCK_PER_MSG))

#define ROZOFS_MAX_FILE_BUF_SZ (256*1024)
#define ROZOFS_SAFE_MAX 36
#define ROZOFS_SAFE_MAX_STORCLI 16
#define ROZOFS_SAFE_MAX_RPC  (ROZOFS_SAFE_MAX/sizeof(uint32_t))
/* Instead of using an array of sid_t for store the dist_set, we use an
 * array of uint32_t in the RPC protocol to use less space (see XDR). */
#define ROZOFS_SAFE_MAX_NET ROZOFS_SAFE_MAX_RPC
#define ROZOFS_DIR_SIZE 4096
#define ROZOFS_PATH_MAX 1024
#define ROZOFS_XATTR_NAME_MAX 255
#define ROZOFS_XATTR_VALUE_MAX 65536
#define ROZOFS_XATTR_LIST_MAX 65536
#define ROZOFS_FILENAME_MAX (255*2)

/* Value for rpc buffer size used for sproto */
#define ROZOFS_RPC_STORAGE_BUFFER_SIZE (1024*300) 

#define ROZOFS_CLUSTERS_MAX 255 /**< FDL : limit for cluster */
/* Value max for a SID */
#define SID_MAX 255
/* Value min for a SID */
#define SID_MIN 1
/* Nb. max of storage node for one volume */
#define STORAGE_NODES_MAX 64
/* Nb. max of storaged ports on the same storage node */
#define STORAGE_NODE_PORTS_MAX 32
/* Nb. max of storages (couple cid:sid) on the same storage node */
#define STORAGES_MAX_BY_STORAGE_NODE 128
/* First TCP port used */
#define STORAGE_PORT_NUM_BEGIN 40000

#define ROZOFS_INODE_SZ  512  /**< rozofs inode size (memory and disk)*/
#define ROZOFS_NAME_INODE 128 /**< max size for object name in RPC message */
#define ROZOFS_NAME_INODE_RPC (ROZOFS_NAME_INODE/sizeof(uint32_t))
#define ROZOFS_XATTR_BLOCK_SZ 4096 /**< rozofs xattr block size */

#define MAX_DIR_ENTRIES 50
#define ROZOFS_MD5_SIZE 22
#define ROZOFS_MD5_NONE "0000000000000000000000"

#if GEO_REPLICATION 
#define ROZOFS_GEOREP_MAX_SITE 2 /**< max sites supported for geo-replication   */
#else
#define ROZOFS_GEOREP_MAX_SITE 1 /**< max sites supported for geo-replication   */
#endif

#define EXPGW_EID_MAX_IDX 1024 /**< max number of eid  */
#define EXPGW_EXPGW_MAX_IDX 32 /**< max number of export gateway per exportd */
#define EXPGW_EXPORTD_MAX_IDX 64 /**< max number of exportd */

#define EXPORT_SLICE_PROCESS_NB 8 /**< number of processes for the slices */


/*
** Exports default path
*/
#define EXPORTS_ROOT  "/srv/rozofs/exports"


/* Value max for an Exportd Gateway */
#define GWID_MAX 32
/* Value min for a Exportd Gateway */
#define GWID_MIN 1

#define SHAREMEM_PER_FSMOUNT_POWER2 1
#define SHAREMEM_PER_FSMOUNT (1<<SHAREMEM_PER_FSMOUNT_POWER2)
#define SHAREMEM_IDX_READ 0
#define SHAREMEM_IDX_WRITE 1

/*
** Projection files on storages are split in chunks. Each chunk is allocated 
** a specific device on the storage.
*/
#define ROZOFS_STORAGE_FILE_MAX_SIZE_POWER2      43 // 8 TiB
#define ROZOFS_STORAGE_FILE_MAX_SIZE             (1ULL<<ROZOFS_STORAGE_FILE_MAX_SIZE_POWER2)
#define ROZOFS_STORAGE_MAX_CHUNK_PER_FILE        128
#define ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(bsize) (ROZOFS_STORAGE_FILE_MAX_SIZE/ROZOFS_STORAGE_MAX_CHUNK_PER_FILE/ROZOFS_BSIZE_BYTES(bsize))
#define ROZOFS_STORAGE_GET_CHUNK_NB(offset)     ((offset)/(ROZOFS_STORAGE_FILE_MAX_SIZE/ROZOFS_STORAGE_MAX_CHUNK_PER_FILE))

/* 
** Old constant redefined on new constant
*/
#define ROZOFS_FILESIZE_MAX ROZOFS_STORAGE_FILE_MAX_SIZE
/*
**   Socket controller configuration: define the max nuumber of context per process
*/
#define ROZO_SOCKCTRL_CTX_EXPORTD_M      2048
#define ROZO_SOCKCTRL_CTX_EXPORTD_S      2048
#define ROZO_SOCKCTRL_CTX_ROZOFSMOUNT     128
#define ROZO_SOCKCTRL_CTX_STORCLI        1024
#define ROZO_SOCKCTRL_CTX_STORAGED       (3*1024)
#define ROZO_SOCKCTRL_CTX_STORIO         (3*1024)

#if GEO_REPLICATION 
#define ROZO_SOCKCTRL_CTX_GEOMGR         (32)
#define ROZO_SOCKCTRL_CTX_GEOCLI         (128)
#else
#define ROZO_SOCKCTRL_CTX_GEOMGR         (0)
#define ROZO_SOCKCTRL_CTX_GEOCLI         (0)
#endif

/*
**   AF_UNIX configuration: define the max nuumber of context per process
*/
#define ROZO_AFUNIX_CTX_EXPORTD_M      1024
#define ROZO_AFUNIX_CTX_EXPORTD_S      2048
#define ROZO_AFUNIX_CTX_ROZOFSMOUNT     64
#define ROZO_AFUNIX_CTX_STORCLI        1024
#define ROZO_AFUNIX_CTX_STORAGED       1024
#define ROZO_AFUNIX_CTX_STORIO         (3*1024)
#if GEO_REPLICATION 
#define ROZO_AFUNIX_CTX_GEOMGR         (32)
#define ROZO_AFUNIX_CTX_GEOCLI         (32)
#else
#define ROZO_AFUNIX_CTX_GEOMGR         (0)
#define ROZO_AFUNIX_CTX_GEOCLI         (0)
#endif
/**
* cluster state
*/
typedef enum {
  CID_DEPENDENCY_ST = 0,
  CID_UP_ST,
  CID_DOWNGRADED_ST,
  CID_DOWN_ST,
  CID_MAX_ST
} cid_state_e;

typedef enum {
    LAYOUT_2_3_4, LAYOUT_4_6_8, LAYOUT_8_12_16,
    LAYOUT_MAX
} rozofs_layout_t;

typedef uint8_t tid_t; /**< projection id */
typedef uint64_t bid_t; /**< block id */
typedef uuid_t fid_t; /**< file id */
typedef uint8_t sid_t; /**< storage id */
typedef uint16_t cid_t; /**< cluster id */
typedef uint16_t vid_t; /**< volume id */
typedef uint32_t eid_t; /**< export id */

/**
*  type of the exportd attributes
*/
typedef enum
{
   ROZOFS_EXTATTR = 0, /**< extended attributes */
   ROZOFS_TRASH,   /**< pending trash */
   ROZOFS_REG,  /**< regular file & symbolic links */
   ROZOFS_DIR,     /**< directory    */
   ROZOFS_SLNK,    /**< name of symbolic link */
   ROZOFS_DIR_FID,     /**< directory rferenced by its fid  */
   ROZOFS_RECYCLE,     /**< recycle directory  */
   ROZOFS_REG_S_MOVER,     /**< FID of a regular file under the control of the file mover: source       */
   ROZOFS_REG_D_MOVER,     /**< FID of a regular file under the control of the file mover: destination  */

   ROZOFS_MAXATTR
} export_attr_type_e;

static inline char * export_attr_type2String(export_attr_type_e val) {
  switch(val) {
    case ROZOFS_EXTATTR: return "EXTATTR";
    case ROZOFS_TRASH: return "TRASH";
    case ROZOFS_REG: return "REG";
    case ROZOFS_DIR: return "DIR";
    case ROZOFS_SLNK: return "SLNK";
    case ROZOFS_DIR_FID: return "DIR_FID";
    case ROZOFS_RECYCLE: return "RECYCLE";
    case ROZOFS_REG_S_MOVER: return "REG_SRC_MOVER";
    case ROZOFS_REG_D_MOVER: return "REG_DST_MOVER";
    default:
      return "?";
  }
}        
typedef union
{
   uint64_t fid[2];   /**<   */
   struct {
     uint64_t  vers:4;        /**< fid version */
     uint64_t  mover_idx:8;   /**< fid index: needed by mover feature: rozo_rebalancing */
     uint64_t  fid_high:33;   /**< highest part of the fid: not used */
     uint64_t  recycle_cpt:2;   /**< recycle counter */
     uint64_t  opcode:4;      /**< opcode used for metadata log */
     uint64_t  exp_id:3;      /**< exportd identifier: must remain unchanged for a given server */
     uint64_t  eid:10;        /**< export identifier */     
     uint64_t  usr_id:8;     /**< usr defined value-> for exportd;it is the slice   */
     uint64_t  file_id:40;    /**< bitmap file index within the slice                */
     uint64_t  idx:11;     /**< inode relative to the bitmap file index           */
     uint64_t  key:4;     /**< inode relative to the bitmap file index           */
     uint64_t  del:1;     /**< asserted to 1 when the i-node has a pending deletion      */
   } s;
   struct {
     uint64_t  vers:4;        /**< fid version */
     uint64_t  mover_idx:8;   /**< fid index: needed by mover feature: rozo_rebalancing */
     uint64_t  fid_high:33;   /**< highest part of the fid: not used */
     uint64_t  recycle_cpt:2;   /**< recycle counter */
     uint64_t  opcode:4;      /**< opcode used for metadata log */
     uint64_t  exp_id:3;      /**< exportd identifier: must remain unchanged for a given server */
     uint64_t  eid:10;        /**< export identifier */     
     uint64_t  usr_id:8;     /**< usr defined value-> for exportd;it is the slice   */
     uint64_t  file_id:40;    /**< bitmap file index within the slice                */
     uint64_t  idx:11;     /**< inode relative to the bitmap file index           */
     uint64_t  key:4;     /**< inode relative to the bitmap file index           */
     uint64_t  del:1;     /**< asserted to 1 when the i-node has a pending deletion      */
   } meta;
} rozofs_inode_t;

/*
** Structures used for direct read based on inode opening thanks IOCTL of the fusectl
*/

#define ROZOFS_FUSECTL_IOCTL_INODE 8
#define ROZOFS_FUSECTL_IOCTL_MOUNT 9

typedef enum {
  OPE_OPEN = 0,
  OPE_MMAP,
  OPE_UNMAP,
  OPE_CLOSE,
  OPE_MAX
} rozofs_mmap_ope_e;
/*
** ** RozoFS-inodeFile
** Structure used to open/mmap,unmap and close a file that has been opened by its inode
*/
typedef struct _ioctl_direct_read_t
{
   uint64_t ino;                  /**< IN: inode to open  */
   rozofs_mmap_ope_e      ope;   /**< IN : operation to execute */
   int status;                   /**< OUT: status of the operation */
   void *file;                   /**< OUT/IN : allocated file      */
   void *raddr;                  /**< OUT: pointer to address to be used by user land process */
   unsigned long size;           /**< IN: buffer size (mmap)   */
   unsigned long pgoff;          /**< IN: offset in the file (mmap)*/
} ioctl_direct_read_t;

typedef struct _ioctl_rozofs_mountpath_t
{
   char name[1024];              /**< IN: rozofs mountpath  */
   int status;                   /**< OUT: status : O (OK) < 0 (NOK) */
} ioctl_rozofs_mountpath_t;




/*
**__________________________________________
*/
/*
** I/O write error log entry structure
*/
typedef struct _rozofs_io_err_entry_t
{
  fid_t fid;       /**< FID of the file                */
  uint64_t time;   /**< time at which the error occur  */
  off_t    off;    /**< offset in file                 */
  uint32_t len;    /**< length to write                */
  int      error;  /**< returned error code (errno)    */
} rozofs_io_err_entry_t;

/*
**__________________________________________
** Extract the recycle counter from the FID
** @param fid : the FID
** @retval the recycle counter value
*/
static inline int rozofs_get_recycle_from_fid(void * fid) {
  rozofs_inode_t * inode = (rozofs_inode_t *) fid; 
  return inode->s.recycle_cpt;
}

/*
**__________________________________________
** Extract the dif from the FID
** @param fid : the FID
** @retval the recycle counter value
*/
static inline eid_t rozofs_get_eid_from_fid(void * fid) {
  rozofs_inode_t * inode = (rozofs_inode_t *) fid; 
  return inode->s.eid;
}
/*
**__________________________________________
** Set the recycle counter in the FID
** @param fid : the FID
** @param value: the value to set
** 
*/
static inline void rozofs_set_recycle_on_fid(void * fid, int value) {
  rozofs_inode_t * inode = (rozofs_inode_t *) fid; 
  inode->s.recycle_cpt = value;
}
/*
**__________________________________________________________________
*/
/**
*
    check the delete pending bit of an inode
    
    @param fid: inode reference
    
    @retval 1 when the i-node has the delete pending bit asserted
    @retval 0 when the i-node has not the delete pending bit asserted
*/
static inline int rozofs_inode_is_del_pending(fid_t fid)
{
   rozofs_inode_t *fake_inode = (rozofs_inode_t*)fid;
   if(fake_inode->s.del!=0) return 1;
   return 0;
}
/*
**__________________________________________________________________
*/
/**
*
    set trash key
    
    @param fid: inode reference
    
*/
static inline void rozofs_inode_set_trash(fid_t fid)
{
   rozofs_inode_t *fake_inode = (rozofs_inode_t*)fid;
   fake_inode->s.key = ROZOFS_TRASH;
}

static inline void rozofs_inode_set_dir(fid_t fid)
{
   rozofs_inode_t *fake_inode = (rozofs_inode_t*)fid;
   fake_inode->s.key = ROZOFS_DIR;
}
/*
**__________________________________________________________________
*/
/**
*
    check if the inode designates the trash directory
    
    @param fid: inode reference
    
    @retval 1 trash diectory
    @retval 0 not trash directory
*/
static inline int rozofs_inode_is_trash(fid_t fid)
{
   rozofs_inode_t *fake_inode = (rozofs_inode_t*)fid;
   if(fake_inode->s.key == ROZOFS_TRASH) return 1;
   return 0;
}
/*
**__________________________________________
** reset the recycle counter in the FID
** @param fid : the FID
** @retval the recycle counter value
*/
static inline void rozofs_reset_recycle_on_fid(void * fid) {
  rozofs_set_recycle_on_fid(fid,0);
}
/*
**__________________________________________
** reset the recycle counter in the FID
** @param fid : the FID
** @retval the recycle counter value
*/
static inline void rozofs_inc_recycle_on_fid(void * fid) {
  rozofs_inode_t * inode = (rozofs_inode_t *) fid; 
  inode->s.recycle_cpt++;
}

// storage stat

typedef struct sstat {
    uint64_t size;
    uint64_t free;
} sstat_t;


/**
 *  Header structure for one projection
 */
typedef union {
    uint64_t u64[2];

    struct {
        uint64_t timestamp : 64; ///<  time stamp.
        uint64_t effective_length : 16; ///<  effective length of the rebuilt block size: MAX is 64K.
        uint64_t projection_id : 8; ///<  index of the projection -> needed to find out angles/sizes: MAX is 255.
        uint64_t version : 8; ///<  version of rozofs. (not used yet)
        uint64_t filler : 32; ///<  for future usage.
    } s;
} rozofs_stor_bins_hdr_t;

typedef struct {
        uint64_t timestamp : 64; ///<  time stamp.
} rozofs_stor_bins_footer_t;



typedef struct child {
    char *name;
    fid_t fid;
    struct child *next;
} child_t;

#include "common/transform.h"
/**
* data structure related to optimized Mojette Usage
*/
typedef struct _projection_opt_t {
    bin_t *bins;
    int    projection_id;;
} projection_opt_t; 

typedef struct _trans_lk_table_t
{
   int min;
   int max;
   void *data[];
} trans_lk_table_t;

typedef void (*inverse_prog)(pxl_t *support,projection_opt_t * projections);
/**
* structure used to define the encoding rule of the lookup table
*/
typedef struct encode_t
{
  int nb_bits;   /**< number of bits that must be taken for the index */
  int nb_proj_per_grp;  /**< number of projections taken a each level */
  int nb_level;         /**< number of levels */
} encode_t;
/**
*  end of Mojette Optimized data structure
*/

/**
 *  By default the system uses 256 slices with 4096 subslices per slice
 */
#define MAX_SLICE_BIT 8
#define MAX_SLICE_NB (1<<MAX_SLICE_BIT)
#define MAX_SUBSLICE_BIT 12
#define MAX_SUBSLICE_NB (1<<MAX_SUBSLICE_BIT)
/*
 **__________________________________________________________________
 */
static inline void mstor_get_slice_and_subslice(fid_t fid, uint32_t *slice, uint32_t *subslice) {
    //uint32_t hash = 0;
    //uint8_t *c = 0;

     rozofs_inode_t *rozo_inode_p = (rozofs_inode_t*)fid;
     *subslice = 0;
     *slice = rozo_inode_p->s.usr_id & ((1 << MAX_SLICE_BIT) - 1);;
#if 0    
    for (c = fid; c != fid + 8; c++)
        hash = *c + (hash << 6) + (hash << 16) - hash;

    *slice = hash & ((1 << MAX_SLICE_BIT) - 1);
    hash = hash >> MAX_SLICE_BIT;
    *subslice = hash & ((1 << MAX_SUBSLICE_BIT) - 1);
#endif
}

/*
**__________________________________________________________________
*/
/**
*  Get the slice number from the upper part of the unique file id (fid)

  @param fid : unique file identifier
  
  @retval : slice value
*/
static inline void exp_trck_get_slice(fid_t fid, uint32_t *slice) {
    uint32_t hash = 0;
    uint8_t *c = 0;

    int i;
    rozofs_inode_t *rozo_inode_p = (rozofs_inode_t*)fid;
    c = (uint8_t*)&rozo_inode_p->fid[1];
    
    for (i= 0; i < sizeof(uint64_t); c++,i++)
        hash = *c + (hash << 6) + (hash << 16) - hash;

    *slice = hash & ((1 << MAX_SLICE_BIT) - 1);
    hash = hash >> MAX_SLICE_BIT;
}

/**
*  check if the slice is local to the export process
  
   @param slice : slice number
   
   @retval 1 : the slice is local
   @retval 0: the slice is not local
*/
static inline int exp_trck_is_local_slice(uint32_t slice)
{
   return 1;

}


/**
*  Generate a fake FID for rozoFS
  
   @param fid : pointer to the fid
   @param export_id : reference of the export host
   
   
   @retval 1 : the slice is local
   @retval 0: the slice is not local
*/
#define ROZOFS_FID_VERSION_0 0
static inline void rozofs_uuid_generate(fid_t fid,uint8_t export_id)
{
  rozofs_inode_t *fake_inode;

  fake_inode = (rozofs_inode_t*)fid;
  fake_inode->fid[0] = 0;
  fake_inode->fid[1] = 0;
  fake_inode->s.vers = ROZOFS_FID_VERSION_0;
  fake_inode->s.exp_id = export_id;
  
}
/**
*  Compute the STORIO slice of a given FID
  
   @param fid : pointer to the fid   
   
   @retval   The storio slice number
*/
static inline unsigned int rozofs_storage_fid_slice(void * fid) {
  rozofs_inode_t *fake_inode = (rozofs_inode_t *) fid;
  
  /*
  ** The storio slice number is built from the export slice and the
  ** lower bits of the tracking file number
  */
  uint32_t        val = fake_inode->s.usr_id + (fake_inode->s.file_id<<8);
  
  /*
  ** The number of slices within the storio is configurable thanks
  ** to the common configuration file (field storio_slice_number).
  */  
  return val % (common_config.storio_slice_number);
} 
/*
**__________________________________________________________________
** Format a string with a FID and parse some inforamtion within the FID
** for debug usage. To be used in log traces.
*/
static inline char * fid2string(fid_t fid , char * string) {
  
  char * p = string;
  
  p += rozofs_fid_append(p,fid);

  *p++ = ' ';
  
  rozofs_inode_t * fake_inode_p =  (rozofs_inode_t *) fid;
  
  p += rozofs_string_append(p,"vers=");
  p += rozofs_u64_append(p,fake_inode_p->s.vers);
  p += rozofs_string_append(p," mover_idx=");
  p += rozofs_u64_append(p,fake_inode_p->s.mover_idx);
  p += rozofs_string_append(p," fid_high=");
  p += rozofs_u64_append(p,fake_inode_p->s.fid_high);
  p += rozofs_string_append(p," recycle=");
  p += rozofs_u64_append(p,fake_inode_p->s.recycle_cpt);  
  p += rozofs_string_append(p," opcode=");
  p += rozofs_u64_append(p,fake_inode_p->s.opcode);
  p += rozofs_string_append(p," exp_id=");
  p += rozofs_u64_append(p,fake_inode_p->s.exp_id);
  p += rozofs_string_append(p," eid=");
  p += rozofs_u64_append(p,fake_inode_p->s.eid);
  p += rozofs_string_append(p," usr_id=");
  p += rozofs_u64_append(p,fake_inode_p->s.usr_id);
  p += rozofs_string_append(p," file_id=");
  p += rozofs_u64_append(p,fake_inode_p->s.file_id);
  p += rozofs_string_append(p," idx=");
  p += rozofs_u64_append(p,fake_inode_p->s.idx);
  p += rozofs_string_append(p," key=");
  p += rozofs_u64_append(p,fake_inode_p->s.key);
  p += rozofs_string_append(p," (");
  p += rozofs_string_append(p,export_attr_type2String(fake_inode_p->s.key));
  p += rozofs_string_append(p,")");          
  p += rozofs_string_append(p," storage slice=");
  p += rozofs_u64_append(p,rozofs_storage_fid_slice(fid));
  p += rozofs_string_append(p,"/");
  p += rozofs_u64_append(p,common_config.storio_slice_number);  
    
  *p = 0;
  return p;
}

// Maximum number of parallel threads that will run a rebuild
#define MAXIMUM_PARALLEL_REBUILD_PER_SID 64
/*
**__________________________________________________________________
*/
/**
*   Build the storage fid of a "mover" file

    @param fid: exportd file (either "mover" or "primary")
    @param index: index of the fid (mover or primary)
    
    @retval modified fid
*/
static inline void rozofs_build_storage_fid (fid_t fid,uint8_t index)
{
    rozofs_inode_t *fake_inode = (rozofs_inode_t*)fid;
    /*
    ** indicates a regular file with a given index (mover) and clear the delete bit from fid
    */
    fake_inode->s.key = ROZOFS_REG;
    fake_inode->s.mover_idx = index;
    fake_inode->s.del = 0;
}
/*
**__________________________________________________________________
** Structure of an entry of a rebuild job file
*/
typedef struct _rozofs_rebuild_entry_file_t {
    fid_t     fid;         //< unique file identifier associated with the file
    uint32_t  block_start; //< Starting block to rebuild from 
    uint32_t  block_end;   //< Last block to rebuild
    uint8_t   layout:4;    //< layout
    uint8_t   bsize:2;     //< Block size 0=4K / 1=8K / 2=16K / 3=32K    
    uint8_t   todo:1;      //< 1 when rebuild not yet done
    uint8_t   error;       //< error code. See ROZOFS_RBS_ERROR_E
    /*
    ** Warning : this field has to be the last since only the 
    ** significant bits of the distribution are written on disk
    */
    sid_t     dist_set_current[ROZOFS_SAFE_MAX]; ///< currents sids of storage nodes
} rozofs_rebuild_entry_file_t; 
/*
**___________________________________________________________________
**
** JSON formating macros
**
**___________________________________________________________________
*/

#define JSON_write_offset {\
  int json_idx;\
  for (json_idx=0; json_idx<json_offset; json_idx++) {\
    *pJSON++ = ' ';\
    *pJSON++ = ' ';\
    *pJSON++ = ' ';\
    *pJSON++ = ' ';\
  } \
}

#define JSON_begin {*pJSON++ = '{';JSON_eol;json_offset++;}
#define JSON_end {JSON_remove_coma;json_offset--; JSON_write_offset; *pJSON++ = '}';JSON_eol;}

#define JSON_separator { *pJSON++ = '\t'; *pJSON++ = ':'; *pJSON++ = ' ';}
#define JSON_make_name(name) { *pJSON++ = '"'; pJSON += rozofs_string_append(pJSON, name); *pJSON++ = '"';}
#define JSON_name(name) { JSON_write_offset; JSON_make_name(name); JSON_separator;}

#define JSON_eol {pJSON += rozofs_eol(pJSON); }
#define JSON_coma_eol { *pJSON++ = ',';  JSON_eol; }
#define JSON_remove_coma { pJSON = pJSON-2; JSON_eol; }

#define JSON_open_obj(name) { JSON_name(name); JSON_begin; }
#define JSON_close_obj { JSON_remove_coma; json_offset--; JSON_write_offset; *pJSON++ = '}'; JSON_coma_eol;}

#define JSON_open_array(name) { JSON_name(name);  *pJSON++ = '['; JSON_eol; json_offset++; }
#define JSON_close_array { JSON_remove_coma; json_offset--; JSON_write_offset; *pJSON++ = ']'; JSON_coma_eol; }

#define JSON_string(name,value) { JSON_name(name);JSON_make_name(value);JSON_coma_eol;}

#define JSON_string_element(value) {JSON_write_offset; JSON_make_name(value); JSON_coma_eol; }

#define JSON_new_element { JSON_write_offset; *pJSON++ = '{'; JSON_eol; json_offset++; }
#define JSON_end_element { JSON_remove_coma; json_offset--; JSON_write_offset; *pJSON++ = '}'; JSON_coma_eol; }
#define JSON_i32(name, value) { JSON_name(name); pJSON += rozofs_i32_append(pJSON, value);JSON_coma_eol; }
#define JSON_u32( name, value) {JSON_name(name); pJSON += rozofs_u32_append(pJSON, value);JSON_coma_eol; }

#define JSON_2u32(name1, value1, name2, value2) { \
  JSON_name(name1); pJSON += rozofs_u32_append(pJSON, value1);\
  *pJSON++ = ','; *pJSON++ = ' ';\
  JSON_make_name(name2); JSON_separator; pJSON += rozofs_u32_append(pJSON, value2);\
  JSON_coma_eol;}
 
#define JSON_u64(name, value) { JSON_name(name); pJSON += rozofs_u64_append(pJSON, value);JSON_coma_eol;}


/*
**________________________________________________________________________
**
** Some declaration for file rebuild
*/
typedef enum _rbs_file_type_e {
  rbs_file_type_nominal=0,
  rbs_file_type_spare,
  rbs_file_type_all  
} rbs_file_type_e;

static inline char * rbs_file_type2string(rbs_file_type_e ftype) {
  switch (ftype) {
    case rbs_file_type_nominal: return "nominal";
    case rbs_file_type_spare:   return "spare";
    case rbs_file_type_all:     return "all";
  }
  return "?";
}  

/*
*______________________________________________________________________
* Create a directory, recursively creating all the directories on the path 
* when they do not exist
*
* @param directory_path   The directory path
* @param mode             The rights
*
* retval 0 on success -1 else
*/
static inline int rozofs_kpi_mkpath(char * directory_path) {
  char* p;
  int  isZero=1;
  int  status = -1;
    
  p = directory_path;
  p++; 
  while (*p!=0) {
  
    while((*p!='/')&&(*p!=0)) p++;
    
    if (*p==0) {
      isZero = 1;
    }  
    else {
      isZero = 0;      
      *p = 0;
    }
    
    if (access(directory_path, F_OK) != 0) {
      if (mkdir(directory_path, 0744) != 0) {
        if (errno != EEXIST)
	{
	  severe("mkdir(%s) %s", directory_path, strerror(errno));
          goto out;
	}
      }      
    }
    
    if (isZero==0) {
      *p ='/';
      p++;
    }       
  }
  status = 0;
  
out:
  if (isZero==0) *p ='/';
  return status;
}

static inline void *rozofs_kpi_map(char *path,char *name,int size,void *init_buf)
{
  int fd;
  struct stat sb;
  void *p;
  char pathname[1024];
  int ret;
  
  strcpy(pathname,path);  
  ret = rozofs_kpi_mkpath(pathname);
  if (ret < 0) return NULL;
  
  sprintf(pathname,"%s/%s",path,name);  
  
  fd = open (pathname, O_RDWR| O_CREAT, 0644);
  if (fd == 1) {
          severe ("open failure for %s : %s",path,strerror(errno));
          return NULL;
  }

 if (fstat (fd, &sb) == -1) {
   severe ("fstat failure for %s : %s",path,strerror(errno));
   close(fd);
   return NULL;
 }
 if (ftruncate (fd, size) == -1) {
   severe ("ftruncate failure for %s : %s",path,strerror(errno));
   close(fd);
   return NULL;
 }
 p = mmap (0, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
 if (p == MAP_FAILED) {
         severe ("map failure for %s : %s",path,strerror(errno));
         return NULL;
 }
 if (init_buf != NULL)
 {
   memcpy(p,init_buf,size);
 }
 else
 {
   memset(p,0,size);
 }
 return p;
}
/*
*______________________________________________________________________
* Create a directory, recursively creating all the directories on the path 
* when they do not exist
*
* @param path2create      The directory path to create
* @param mode             The rights
*
* retval 0 on success -1 else
*/
static inline int rozofs_mkpath(char * path2create, mode_t mode) {
  char* p;
  int  isZero=1;
  int  status = -1;
  char  directory_path[ROZOFS_PATH_MAX+1];
  
  strcpy(directory_path,path2create);
    
  p = directory_path;
  p++; 
  while (*p!=0) {
  
    while((*p!='/')&&(*p!=0)) p++;
    
    if (*p==0) {
      isZero = 1;
    }  
    else {
      isZero = 0;      
      *p = 0;
    }
    
    if (access(directory_path, F_OK) != 0) {
      if (mkdir(directory_path, mode) != 0) {
        if (errno != EEXIST) {
	  severe("mkdir(%s) %s", directory_path, strerror(errno));
          goto out;
        }  
      }      
    }
    
    if (isZero==0) {
      *p ='/';
      p++;
    }       
  }
  status = 0;
  
out:
  if (isZero==0) *p ='/';
  return status;
}


#endif
