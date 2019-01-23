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

#ifndef _MATTR_H
#define _MATTR_H

#include <rozofs/rozofs.h>
#include <sys/stat.h>

/*
** Constant for file resizing
*/
#define ROZOFS_RESIZEA 0x20524553495A4541LL
#define ROZOFS_RESIZEM 0x20524553495A454DLL


/** API meta attributes functions.
 */

/** all we need to know about a managed file.
 *
 * attributes fid, cid, sids are rozofs data storage information.
 * others are a used the same way struct stat(2)
 */
 #define ROZOFS_HAS_XATTRIBUTES 0x80000000
 
static inline void rozofs_set_xattr_flag(uint32_t *mode) 
{*mode &= (~ROZOFS_HAS_XATTRIBUTES);} 

static inline void  rozofs_clear_xattr_flag(uint32_t *mode) {*mode |= (ROZOFS_HAS_XATTRIBUTES);}

static inline int rozofs_has_xattr(uint32_t mode)
 {
    return (mode&ROZOFS_HAS_XATTRIBUTES)?0:1; 
 }
/**
**__________________________________________
*  Directory sids[0] remapping
**__________________________________________
*/
/*
** SIDS[0] remapping
*/
typedef union 
{
  uint8_t byte;
  struct _internal {
   uint8_t filler:3;       /**< for future usage */
   uint8_t trash:2;       /**< when asserted it indicates that the directory has a trash            */
   uint8_t root_trash:1;  /**< when asserted it indicates that the directory is a root for trash: make @rozofs-trash@ display in readdir */
   uint8_t backup:2;      /**< when asserted it indicates that the directory is candidate for backup */  
  } s;
} rozofs_dir0_sids_t;

#define ROZOFS_DIR_BACKUP_RECURSIVE 2      /**< when asserted it indicates that the backup state must be propagated to children */
#define ROZOFS_DIR_TRASH_RECURSIVE 2      /**< when asserted it indicates that the trash state must be propagated to children */

/**
*  extra metadata associated with a directory
*/
#define ROZOFS_DIR_VERSION_1 1
typedef union
{
  sid_t sids[ROZOFS_SAFE_MAX];
  struct {
    rozofs_dir0_sids_t  sid0; /**< see structure above      */
    uint8_t  version;      /**< i-node version       */
    uint8_t  layout;       /**< bit 7:(0:no layout/1: layout defined); bit 6-0: layout value */
    uint8_t  filler1[5];   /**< 40 bits for future usage   */
    uint64_t update_time;  /**< current time of last change of a directory's child */
    uint64_t nb_bytes;     /**< number of bytes in that directory */
    uint16_t profile_id;   /**< profile associated with the directory    */
    uint8_t filler2[8];    /**< for future usage */ 
  } s;            
} ext_dir_mattr_t;
/*
**__________________________________________________________________
*/
/**
*  Check if directory has a trash

   @param sids_p: pointer to the byte that contains the trash bit
   
   @retval 1: trash is active
   @retval 0: no trash
*/
static inline int rozofs_has_trash(uint8_t *sids_p)
{
  rozofs_dir0_sids_t *p = (rozofs_dir0_sids_t*)sids_p;
  if (p->s.trash) return 1;
  else return 0;
}

/*
**__________________________________________________________________
*/
/**
*  set trash flag on directory

   @param sids_p: pointer to the byte that contains the trash bit
   
   @retval none
*/
static inline void rozofs_set_trash_sid0(uint8_t *sids_p)
{
  rozofs_dir0_sids_t *p = (rozofs_dir0_sids_t*)sids_p;
  p->s.trash = 1;
}

/*
**__________________________________________________________________
*/
/**
*  Check if directory has is a root trash

   @param sids_p: pointer to the byte that contains the root  trash bit
   
   @retval 1: trash is active
   @retval 0: no trash
*/
static inline int rozofs_has_root_trash(uint8_t *sids_p)
{
  rozofs_dir0_sids_t *p = (rozofs_dir0_sids_t*)sids_p;
  if (p->s.root_trash) return 1;
  else return 0;
}
/*
** SIDS[1..34] are reserved for future usage
*/
 
/**
*  inode Generic structure
*/ 
 typedef struct mattr {
    fid_t fid;                      /**< unique file id */
    sid_t sids[ROZOFS_SAFE_MAX];    /**< sid of storage nodes target (regular file only)*/
    cid_t cid;                      /**< cluster id 0 for non regular files */
    uint32_t mode;                  /**< see stat(2) */
    uint32_t uid;                   /**< see stat(2) */
    uint32_t gid;                   /**< see stat(2) */
    uint16_t nlink;                 /**< see stat(2) */
    uint64_t ctime;                 /**< see stat(2) */
    uint64_t atime;                 /**< see stat(2) */
    uint64_t mtime;                 /**< see stat(2) */
    uint64_t size;                  /**< see stat(2) */
    uint32_t children;              /**< number of children (excluding . and ..) */
} mattr_t;

/**
*  the following structure is mapped on the children field of the mattr_t
*  to address the case of the file mover
*/
typedef union
{
  uint32_t u32;
  struct {
  uint8_t vid_fast;     /**< index of the fast volume: 0-> not significant  */
  uint8_t filler2;
  uint8_t mover_idx;   /**< index of the mover FID for storage usage   */
  uint8_t primary_idx; /**< index of the normal FID for storage usage  */
  } fid_st_idx;
} rozofs_mover_children_t;


/**
*  the following structure is mapped on the hpc_reserved field of the mattr_t
*  to address the case of the file mover
*/
typedef union
{
  uint64_t u64;
  struct {
  uint64_t nb_deleted_files; /**< number of deleted files within the directory  */
  } dir;
  struct {
  uint16_t share_id; /**< Index of the share or project: follow the parent */
  uint16_t filler;   /**< for future usage   */
  uint32_t nb_blocks_thin; /**< number of 4KB blocks of a file  */
  } reg;
} rozofs_hpc_reserved_t;


/**
*  structure used for the case of the file mover: only for regular file type:
   That structure is intended to be mapped on the sids[] field of the mattr_t structure
*/
typedef union
{
   sid_t sids[ROZOFS_SAFE_MAX];   
   struct {
   sid_t primary_sids[ROZOFS_SAFE_MAX_STORCLI];
   cid_t mover_cid;
   sid_t mover_sids[ROZOFS_SAFE_MAX_STORCLI];
   } dist_t;
} rozofs_mover_sids_t;


#define ROZOFS_OBJ_NAME_MAX 60
#define ROZOFS_OBJ_MAX_SUFFIX 16
/**
*  structure used for tracking the location of the fid and name of the object
*/
typedef struct _mdirent_fid_name_info_t
{
    uint16_t coll:1;   /**< asserted to 1 if coll_idx is significant  */
    uint16_t root_idx:15;   /**< index of the root file  */
    uint16_t coll_idx;   /**< index of the collision file */
    uint16_t chunk_idx:12; 
    uint16_t nb_chunk :4;
} mdirent_fid_name_info_t;

#define ROZOFS_FNAME_TYPE_DIRECT 0
#define ROZOFS_FNAME_TYPE_INDIRECT 1
typedef struct _inode_fname_t
{
   uint16_t name_type:1;
   uint16_t len:15;
   uint16_t hash_suffix;
   union
   {
     char name[ROZOFS_OBJ_NAME_MAX]; /**< direct case   */
     struct
     {
       mdirent_fid_name_info_t name_dentry;
       char suffix[ROZOFS_OBJ_MAX_SUFFIX];     
     } s;
   };
 } rozofs_inode_fname_t;


/*___________________________________
** Bits mask of bit field 1 in ext_mattr_t
*/
/* Wether locks on this file must be kept on export switchover */
#define ROZOFS_BITFIELD1_PERSISTENT_FLOCK    1
/* Whether this file has had a write error which could lead to file corruption */
#define ROZOFS_BITFIELD1_WRITE_ERROR         2

/*___________________________________
** Case of the multiple files
*/

#define ROZOFS_STRIPING_UNIT_BASE (1024*256)
#define ROZOFS_HYBRID_UNIT_BASE (1024*512)
#define ROZOFS_MAX_STRIPING_FACTOR  7
#define ROZOFS_MAX_STRIPING_UNIT_POWEROF2 7
//#define ROZOFS_STRIPING_MAX_HYBRID_BYTES ((1024*1024)*2)
//#define ROZOFS_STRIPING_DEF_HYBRID_BYTES ((1024*1024))
typedef union
{
  uint8_t byte;
  struct  {
  uint8_t master:1; /**< assert to one for the master inode, 0 for the slaves */
  uint8_t filler:7; 
  } common;
  struct  {
  uint8_t master:1; /**< assert to one for the master inode, 0 for the slaves */
  uint8_t striping_unit:3; /**<0: 512KB, 1:1024KB, 2: 2038KB, 3:4096KB  */
  uint8_t striping_factor:3; /**< striping factor is given in power of 2: 0:1; 1:2; 2:4.... */
  uint8_t inherit:1;  /**< for directory usage only: when assert the child directory inherits of the parent striping configuration */

  } master;
  struct {
  uint8_t master:1; /**< assert to one for the master inode, 0 for the slaves */
  uint8_t slave:1;  /**< Always set to 1 to differentiate from non striped files */
  uint8_t file_idx:6; /**<index of the inode in the multiple file  */
  } slave;
} rozofs_multiple_desc_t;


typedef union
{
  uint8_t byte;

  struct  {
  uint8_t no_hybrid:1; /**<assert to 1 when hybrid mode MUST not be used: for directory usage only. Not significant for regular file, use hybrid field of  rozofs_multiple_desc_t instead */
  uint8_t hybrid_sz:7; /**<number of 512 KB unit used for the hybrid mode : when it is 0 rozofs use the striping unit defined in rozofs_multiple_desc*/
  } s;
} rozofs_hybrid_desc_t;

/*___________________________________
**   RozoFS inode structure
*/
typedef union
{
   char inode_buf[512];
   struct inode_internal_t {
     mattr_t attrs;      /**< standard attributes       */
     uint64_t cr8time;   /**< creation time          */
     fid_t   pfid;   /**< parent fid                */
     uint32_t hash1;   /**< parent/name hash1  */
     uint32_t hash2;   /**< parent/name hash2  */
     uint8_t i_extra_isize;  /**< array reserved for extended attributes */
     uint8_t bitfield1;  /**< reserve fot future use */
     rozofs_multiple_desc_t multi_desc;  /**< used for rozofs multiple file see rozofs_multiple_desc_t */
     rozofs_hybrid_desc_t hybrid_desc;  /**<indicates in hybrid mode is in used and the max size of the hybrid section */
     uint8_t i_state;     /**< inode state               */
     uint8_t filler4;  /**< reserve fot future use */
     uint8_t filler5;  /**< reserved for future use */
     uint8_t filler6;  /**<reserve for future use */
     uint64_t i_file_acl;  /**< extended inode */
     uint64_t i_link_name;  /**< symlink block */
     rozofs_hpc_reserved_t hpc_reserved;  /**< reserved for hpc */
     rozofs_inode_fname_t fname;  /**< reference of the name within the dentry file */
   } s;

} ext_mattr_t;

#define ROZOFS_I_EXTRA_ISIZE (sizeof(struct inode_internal_t))

#define ROZOFS_I_EXTRA_ISIZE_BIS (sizeof(ext_mattr_t) -sizeof(struct inode_internal_t))


/** initialize mattr_t
 *
 * fid is not initialized
 * cid is set to UINT16_MAX (serve to detect unset value)
 * sids is filled with 0
 *
 * @param mattr: the mattr to initialize.
 */
void mattr_initialize(mattr_t *mattr);

/** initialize mattr_t
 *
 * fid is not initialized
 * cid is set to UINT16_MAX (serve to detect unset value)
 * sids is filled with 0
 *
 * @param mattr: the mattr to release.
 */
void mattr_release(mattr_t *mattr);
/*
**__________________________________________________________________
*/
/**
* store the file name in the inode
  The way the name is stored depends on the size of
  the filename: when the name is less than 62 bytes
  it is directly stored in the inode
  
  @param inode_fname_p: pointer to the array used for storing object name
  @param name: name of the object
  @param dentry_fname_info_p :pointer to the array corresponding to the fname in dentry
*/
void exp_store_fname_in_inode(rozofs_inode_fname_t *inode_fname_p,
                              char *name,
			      mdirent_fid_name_info_t *dentry_fname_info_p);

/*
**__________________________________________________________________
*/
/**
* store the directory name in the inode
  The way the name is stored depends on the size of
  the filename: when the name is less than 62 bytes
  it is directly stored in the inode
  
  @param inode_fname_p: pointer to the array used for storing object name
  @param name: name of the object
  @param dentry_fname_info_p :pointer to the array corresponding to the fname in dentry
*/
void exp_store_dname_in_inode(rozofs_inode_fname_t *inode_fname_p,
                              char *name,
			      mdirent_fid_name_info_t *dentry_fname_info_p);


#define ROZOFS_PRIMARY_FID 0
#define ROZOFS_MOVER_FID  1
/*
**__________________________________________________________________
*/
/**
*   Build the FID associated with a storage

    @param attr_p: pointer to the attributes of the i-node
    @param fid: output storage fid
    @param type: fid type (either ROZOFS_PRIMARY_FID or ROZOFS_MOVER_FID)
    
    @retval 0 on success (fid contains the storage fid value according to type)
    @retval < 0 error (see errno for details)
*/    
static inline int rozofs_build_storage_fid_from_attr(mattr_t *attr_p,fid_t fid,int type)
{
   rozofs_mover_children_t mover_idx; 
   
   mover_idx.u32 = attr_p->children;
   /*
   ** check the mode of the i-node
   */
   if (!S_ISREG(attr_p->mode))
   {
     errno = EINVAL;
     return -1;
   }
   if ((type != ROZOFS_PRIMARY_FID) && (type != ROZOFS_MOVER_FID))
   {
     errno = EINVAL;
     return -1;      
   }
   memcpy(fid,attr_p->fid,sizeof(fid_t));
   if (ROZOFS_PRIMARY_FID == type)
   {
     rozofs_build_storage_fid(fid,mover_idx.fid_st_idx.primary_idx);   
   }
   else
   {
     if (mover_idx.fid_st_idx.primary_idx == mover_idx.fid_st_idx.mover_idx)
     {
        errno = EINVAL;
	return -1;
     }
     rozofs_build_storage_fid(fid,mover_idx.fid_st_idx.mover_idx);   
   }
   return 0;
}
/*
**__________________________________________________________________
*/
/**
*   Fill up the information needed by storio in order to read/write a file

    @param attrs_p: pointer to the inode attributes
    @param fid_storage: pointer to the array where the fid of the file on storage is returned
    
    @retval 1 on success
    @retval 0 otherwise
*/
static inline int rozofs_is_storage_fid_valid(mattr_t *attrs_p,fid_t fid_storage)
{
  rozofs_inode_t         *inode_p;
  uint8_t                 mover_idx_fid;
  rozofs_mover_children_t mover_idx;
    
  inode_p = (rozofs_inode_t*)attrs_p->fid; 
  /*
  ** get the current mover idx of the fid: it might designate either the primary or mover temporary file
  */
  mover_idx_fid = inode_p->s.mover_idx;
  mover_idx.u32 = attrs_p->children;
  
  if (mover_idx.fid_st_idx.primary_idx == mover_idx_fid) return 1;
  if (mover_idx.fid_st_idx.mover_idx == mover_idx_fid) return 1;
  return 0;
}

/*
**__________________________________________________________________
*/
/**
*   Fill up the information needed by storio in order to read/write a file

    @param ie: pointer to the inode entry that contains file information
    @param cid: pointer to the array where the cluster_id is returned
    @param sids_p: pointer to the array where storage id are returned
    @param fid_storage: pointer to the array where the fid of the file on storage is returned
    @param key: key for building the storage fid: ROZOFS_MOVER_FID or ROZOFS_PRIMARY_FID
    
    @retval 0 on success
    @retval < 0 error (see errno for details)
*/
static inline int rozofs_fill_storage_info_from_mattr(mattr_t *attrs_p,cid_t *cid,uint8_t *sids_p,fid_t fid_storage,int key)
{
  int ret;
  rozofs_mover_sids_t *dist_mv_p;
    
  ret = rozofs_build_storage_fid_from_attr(attrs_p,fid_storage,key);
  if (ret < 0) return ret;
  /*
  ** get the cluster and the list of the sid
  */
  if (key == ROZOFS_MOVER_FID)
  {
    dist_mv_p = (rozofs_mover_sids_t*)attrs_p->sids;
    *cid = dist_mv_p->dist_t.mover_cid;
    memcpy(sids_p,dist_mv_p->dist_t.mover_sids,ROZOFS_SAFE_MAX_STORCLI);
  }
  else
  {
    *cid = attrs_p->cid;
    memcpy(sids_p,attrs_p->sids,ROZOFS_SAFE_MAX_STORCLI);  
  }
  return 0;
}

/*
**__________________________________________________________________

    M U L T I P L E   F I L E  S E R V I C E S
**__________________________________________________________________
*/    
typedef struct _rozofs_slave_inode_t 
{
    uint64_t size;                  /**< see stat(2) */
    sid_t sids[ROZOFS_SAFE_MAX];    /**< sid of storage nodes target (regular file only)*/
    uint32_t children;              /**< number of children (excluding . and ..) */
    cid_t cid;

} rozofs_slave_inode_t;


typedef struct _rozofs_multi_vect_t
{
  uint64_t off;    /**< offset in the slave file    */
  uint64_t len;    /**< size in bytes to transfer   */
  uint32_t byte_offset_in_shared_buf;   /**< offset where data must be copy in/out in the rozofsmount shared buffer */
  uint8_t  file_idx;  /**< index of the slave file   */
} rozofs_multi_vect_t;  

typedef struct _rozofs_iov_multi_t
{
   int nb_vectors;   /** number of vectors */
   rozofs_multi_vect_t vectors[ ROZOFS_MAX_STRIPING_FACTOR+1];
} rozofs_iov_multi_t;



/*
**__________________________________________________________________
*/
/** That function returns the striping size of the file
    That information is found in one attributes of the main inode
    
    @param ie: pointer to the inode information
    
    @retval > 0: value of the striping size in byte
    @retval < 0 : error (see errno for details)
 */
static inline int rozofs_get_striping_size(rozofs_multiple_desc_t *p)
{
  if (p->common.master == 0) return -1;
  return ROZOFS_STRIPING_UNIT_BASE << (p->master.striping_unit);
}


/*
**__________________________________________________________________
*/
/** That function returns the hybrid size of the file
    That information is found in one attributes of the main inode
    
    @param ie: pointer to the inode information
    
    @retval > 0: value of the striping size in byte
    @retval < 0 : error (see errno for details)
 */
static inline int rozofs_get_hybrid_size(rozofs_multiple_desc_t *p,rozofs_hybrid_desc_t *q)
{
  if (p->common.master == 0) return 0;
  /*
  ** Check if hybrid mode is defined for that file
  */
  if (q->s.no_hybrid == 1) return 0;
  /*
  ** Check if the number of blocks have been defined for the hybrid array, otherwise use the 
  ** striping unit
  */
  if (q->s.hybrid_sz == 0) {
    return ROZOFS_STRIPING_UNIT_BASE << (p->master.striping_unit);
  }
  return(q->s.hybrid_sz*ROZOFS_HYBRID_UNIT_BASE);
}

/*
**__________________________________________________________________
*/
/**
  Get the striping factor: that value indicates the number of secondary inode that
  are associated with the primary inode
  
  The FID of the secondary inodes are in the same file index as the primary inode*
  the first secondary inode is found has the next index in sequence in the file index
  that contains the primary inode

    @param ie: pointer to the inode information
    
    @retval > 0: value of the striping size in byte
    @retval < 0 : error (see errno for details)
*/

static inline int rozofs_get_striping_factor(rozofs_multiple_desc_t *p)
{
  if (p->common.master == 0) return -1;
  return p->master.striping_factor+1;

}

/*
**__________________________________________________________________
*/
static inline void rozofs_print_multi_vector(rozofs_iov_multi_t *vector_p,char *pbuf)
{
   int i;
   rozofs_multi_vect_t *p;   


   p = &vector_p->vectors[0];
   pbuf +=sprintf(pbuf,"+---------------+---------------+----------+--------+\n");
   pbuf +=sprintf(pbuf,"|     offset    |     length    | buf. off | f_idx  |\n");
   pbuf +=sprintf(pbuf,"+---------------+---------------+----------+--------+\n");
   for (i = 0; i < vector_p->nb_vectors; i++,p++)   
   {
     pbuf +=sprintf(pbuf,"|  %12.12llu | %12.12llu  | %8.8u |   %2.2u   |\n",
                   (long long unsigned int)p->off,(long long unsigned int)p->len,p->byte_offset_in_shared_buf,p->file_idx);
   }
   pbuf +=sprintf(pbuf,"+---------------+---------------+----------+--------+\n");
}

/*
**__________________________________________________________________
*/
/**
   Build the vector for a multi file access
   
   @param off: file offset (starts on a 4KB boundary)
   @param len: length in byte to read or write
   @param vector_p : pointer to the vector that will contains the result
   @param striping_unit: striping unit in bytes 
   @param striping_factor:max  number of slave files
   @param alignment: alignment in bytes on the first block (needed to be 128 aligned for Mojette
   

   @retval 0 on success
   @retval < 0 on error (see errno for details)
*/
static inline int rozofs_build_multiple_offset_vector(uint64_t off, uint64_t len,rozofs_iov_multi_t *vector_p,uint32_t striping_unit_bytes, uint32_t striping_factor,uint32_t alignment)
{
   int i = 0;
   rozofs_multi_vect_t *p;
   uint64_t block_number;
   uint64_t offset_in_block;
   uint64_t file_idx;
   uint32_t byte_offset_in_shared_buf = alignment;
   
   vector_p->nb_vectors = 0;
   p = &vector_p->vectors[0];
   
   /*
   ** Get the number of entries to create: it depends on the striping_size 
   */
   while (len != 0)
   {
     block_number = off/striping_unit_bytes;
     offset_in_block = off%striping_unit_bytes;
     file_idx = block_number%striping_factor;
     p->off = (block_number/striping_factor)*striping_unit_bytes + offset_in_block;
     p->file_idx = file_idx+1;
     p->byte_offset_in_shared_buf = byte_offset_in_shared_buf;
     {
       if ((offset_in_block +len) > striping_unit_bytes)
       {
	  p->len = striping_unit_bytes - offset_in_block;
	  len -= p->len;
	  off +=p->len;
       }
       else
       {
	  p->len = len;
	  len = 0;
       }
       byte_offset_in_shared_buf +=p->len;
       p++;
       i++;
     }
   }
   vector_p->nb_vectors = i;
   return 0;  
}   

/*
**__________________________________________________________________
*/
/**
   Build the vector for a multi file access
   
   @param off: file offset (starts on a 4KB boundary)
   @param len: length in byte to read or write
   @param vector_p : pointer to the vector that will contains the result
   @param striping_unit: striping unit in bytes 
   @param striping_factor:max  number of slave files
   @param alignment: alignment in bytes on the first block (needed to be 128 aligned for Mojette)
   @param hybrid_size: size of the hybrid section

   @retval 0 on success
   @retval < 0 on error (see errno for details)
*/
static inline int rozofs_build_multiple_offset_vector_hybrid(uint64_t off, uint64_t len,rozofs_iov_multi_t *vector_p,uint32_t striping_unit_bytes, uint32_t striping_factor,uint32_t alignment,uint32_t hybrid_size_bytes)
{
   int i = 0;
   rozofs_multi_vect_t *p;
   uint64_t block_number;
   uint64_t offset_in_block;
   uint64_t file_idx;
   uint32_t byte_offset_in_shared_buf = alignment;
   
   
   vector_p->nb_vectors = 0;
   p = &vector_p->vectors[0];

   
   if (off < hybrid_size_bytes)
   {
        offset_in_block = off%hybrid_size_bytes;
        p->file_idx = 0;
	p->off = off;	
	p->byte_offset_in_shared_buf = byte_offset_in_shared_buf;   
	if ((offset_in_block +len) > hybrid_size_bytes)
	{
	   p->len = hybrid_size_bytes - offset_in_block;
	   len -= p->len;
	   off +=p->len;
	}
	else
	{
	   p->len = len;
	   len = 0;
	}
	off = off -  hybrid_size_bytes + striping_unit_bytes;
        byte_offset_in_shared_buf +=p->len;
	p++;
	i++;      
   }
   else
   {
	off = off -  hybrid_size_bytes + striping_unit_bytes;   
   }
   
   /*
   ** Get the number of entries to create: it depends on the striping_size 
   */
   while (len != 0)
   {
     block_number = off/striping_unit_bytes;
     offset_in_block = off%striping_unit_bytes;
     if (block_number == 0)
     {
        p->file_idx = 0;
	p->off = off;	
	p->byte_offset_in_shared_buf = byte_offset_in_shared_buf;
	
	if ((offset_in_block +len) > striping_unit_bytes)
	{
	   p->len = striping_unit_bytes - offset_in_block;
	   len -= p->len;
	   off +=p->len;
	}
	else
	{
	   p->len = len;
	   len = 0;
	}
     }
     else
     {
       block_number -=1; // (off-striping_unit_bytes)/striping_unit_bytes;
       file_idx = block_number%(striping_factor);
       file_idx +=1;
       p->off = (block_number/(striping_factor))*striping_unit_bytes + offset_in_block;
       p->file_idx = file_idx;
       p->byte_offset_in_shared_buf = byte_offset_in_shared_buf;
       {
	 if ((offset_in_block +len) > striping_unit_bytes)
	 {
	    p->len = striping_unit_bytes - offset_in_block;
	    len -= p->len;
	    off +=p->len;
	 }
	 else
	 {
	    p->len = len;
	    len = 0;
	 }
       }
     }
     byte_offset_in_shared_buf +=p->len;
     p++;
     i++;
   }
   vector_p->nb_vectors = i;
   return 0;  
}  


/*
**__________________________________________________________________
*/
/**
   Build the vector size for a multi file access in non hybrid mode
   
   @param len: total file size 
   @param vector_p : pointer to the vector that will contains the result
   @param striping_unit: striping unit in bytes 
   @param striping_factor:max  number of slave files
   

   @retval 0 on success
   @retval < 0 on error (see errno for details)
*/
static int rozofs_build_multiple_size_vector(uint64_t len,rozofs_iov_multi_t *vector_p,uint32_t striping_unit_bytes, uint32_t striping_factor)
{
   int i = 0;
   rozofs_multi_vect_t *p;
   uint64_t nb_blocks;
   uint64_t remainder = 0;
   
   vector_p->nb_vectors = 0;
   p = &vector_p->vectors[0];
   
   /*
   ** In no hybrid mode the master inode is empty
   */
   p->len      = 0;
   p->file_idx = 0;
   p->off      = 0;
   p++;

   /*
   ** compute the number of blocks per slave files
   */
   nb_blocks = len/(striping_unit_bytes*striping_factor);
   for (i = 0 ; i < striping_factor; i++,p++)
   {
     p->len = nb_blocks*striping_unit_bytes;
     p->file_idx = i+1;
     p->off = 0;
   } 
   remainder = len -  nb_blocks*striping_unit_bytes*striping_factor;
   
   p = &vector_p->vectors[1];
   while (remainder!= 0)
   {
     if (striping_unit_bytes > remainder )
     {
       p->len +=remainder;
       remainder = 0;
     }
     else
     {
       p->len +=striping_unit_bytes;
       remainder -= striping_unit_bytes;
       p++;
     }    
   }   
   vector_p->nb_vectors = striping_factor+2;
   return 0;  
}   

/*
**__________________________________________________________________
*/
/**
   Build the vector size for a multi file access in  hybrid mode
   
   @param len: total file size 
   @param vector_p : pointer to the vector that will contains the result
   @param striping_unit: striping unit in bytes 
   @param striping_factor:max  number of slave files
   @param hybrid_size_bytes: size of the hybrid section
   

   @retval 0 on success
   @retval < 0 on error (see errno for details)
*/
static int rozofs_build_multiple_size_vector_hybrid(uint64_t len,rozofs_iov_multi_t *vector_p,uint32_t striping_unit_bytes, uint32_t striping_factor,uint32_t hybrid_size_bytes)
{
   int i = 0;
   rozofs_multi_vect_t *p;
   uint64_t nb_blocks;
   uint64_t remainder = 0;
   
   vector_p->nb_vectors = 0;
   p = &vector_p->vectors[0];
   if (len <= hybrid_size_bytes)
   {
     p->len = len;
     p->file_idx = 0;
     p->off = 0;      
   }
   else
   {
     p->len = hybrid_size_bytes;
     p->file_idx = 0;
     p->off = 0;          
   }
   if (len <= hybrid_size_bytes) len = 0;
   else len = len -hybrid_size_bytes;
   /*
   ** compute the number of blocks per slave files
   */
   nb_blocks = len/(striping_unit_bytes*striping_factor);
   p = &vector_p->vectors[1];
   for (i = 0 ; i < striping_factor; i++,p++)
   {
     p->len = nb_blocks*striping_unit_bytes;
     p->file_idx = i+1;
     p->off = 0;
   } 
   remainder = len -  nb_blocks*striping_unit_bytes*striping_factor;
   
   p = &vector_p->vectors[1];
   while (remainder!= 0)
   {
     if (striping_unit_bytes > remainder )
     {
       p->len +=remainder;
       remainder = 0;
     }
     else
     {
       p->len +=striping_unit_bytes;
       remainder -= striping_unit_bytes;
       p++;
     }    
   }   
   vector_p->nb_vectors = striping_factor+2;
   return 0;  
}   

/*
**__________________________________________________________________
*/
/*
** Get the size of the different slave files including the hybrid case

   @param inode_p: pointer to the inode context
   @param vector_p: pointer to the vector that will contain the result
   
   note: file_idx 0 is reserved from the hybrid file (distributation of the master inode
 
  @retval 0 on success
  @retval -1 on error  
*/
static inline int rozofs_get_multiple_file_sizes(ext_mattr_t *inode_p,rozofs_iov_multi_t *vector_p)
{
   uint32_t striping_unit_bytes;
   uint32_t striping_factor;
   
   striping_unit_bytes = rozofs_get_striping_size(&inode_p->s.multi_desc);
   striping_factor = rozofs_get_striping_factor(&inode_p->s.multi_desc);   

    /*
    **___________________________________________________________________
    **         build the  write vector
    **  The return vector indicates how many commands must be generated)
    **____________________________________________________________________
    */
    
    if (inode_p->s.multi_desc.byte == 0) {
      vector_p->vectors[0].len = inode_p->s.attrs.size;
      vector_p->vectors[0].file_idx = 0;
      vector_p->vectors[0].off = 0; 
      vector_p->nb_vectors = 1; 
      return 0;   
    }
    
    if (inode_p->s.hybrid_desc.s.no_hybrid==1)
    {
      rozofs_build_multiple_size_vector(inode_p->s.attrs.size,vector_p,striping_unit_bytes,striping_factor);
    }
    else
    {
      uint32_t hybrid_size;
      hybrid_size = rozofs_get_hybrid_size(&inode_p->s.multi_desc,&inode_p->s.hybrid_desc);
      rozofs_build_multiple_size_vector_hybrid(inode_p->s.attrs.size,vector_p,striping_unit_bytes,striping_factor,hybrid_size);    
    }
    return 0;

}
#endif
