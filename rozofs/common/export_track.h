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
#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <uuid/uuid.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <malloc.h>
#if 0
#define severe printf
#define fatal printf
#include "rozofs.h"
#else
#include <rozofs/rozofs.h>
#include "log.h"
#endif

#ifndef EXPORT_TRACK_CREATE_H
#define EXPORT_TRACK_CREATE_H

#define EXP_TRCK_MAIN_FILENAME "track_main"
extern int open_count;
extern int close_count;

/**
*  structure of the main tracking file
*/
typedef struct _exp_trck_header_t 
{
  uint64_t first_idx;  /**< first file index within a tracking directory */
  uint64_t last_idx; /**< last file index within a tracking directory    */

} exp_trck_header_t;

#if 0
/**
*  structure of the tracking index within the file
*/
typedef union _exp_trck_idx_t
{
   uint16_t u16;
   struct 
   {
     uint16_t idx:11;  /**< index within the file payload  */
     uint16_t key:5;   /**< user defined key               */
   } s;
} exp_trck_idx_t;
#endif
/**
*  tracking file header structure
*/
#define GET_FILE_OFFSET(index) (sizeof(uint64_t)+sizeof(uint16_t)*index);
#define EXP_TRCK_MAX_INODE_PER_FILE ((4096 - sizeof(uint64_t))/2)
typedef struct _exp_trck_file_header_t
{
     uint64_t  creation_time;    /**< creation time of the tracking file             */
     uint16_t  inode_idx_table[EXP_TRCK_MAX_INODE_PER_FILE];  /**< table that contains the relative inode idx within the file     */
} exp_trck_file_header_t;


#define EXP_TRCK_MAX_RECURSE 4
/**
* memory tracking context used in memory
*/
#define EXP_TRCK_MAIN_REPLICA_COUNT 4  /**< number of replica used for a main tracking file  */
typedef struct _exp_trck_header_memory_t
{
     uint8_t user_id;        /**< user identifier: typical the number of the directory        */
     char    *root_path;     /**< root path of the tracking system for the user               */
     int fd[EXP_TRCK_MAIN_REPLICA_COUNT];   /**< file descriptor of the main tracking file    */
     uint64_t cur_main_idx;    /**< current starting index for lookup */
     exp_trck_header_t entry;  /**< tracking context entry */
     int index_available;      /**< 1: current tracking file is available                     */
     int cur_tracking_file_fd;   /**< file descriptor associated with the current tracking file */
     uint16_t max_attributes_sz;  /**< max size of the attributes :512,1024,1024+512 etc.. */
     uint16_t cur_idx;           /**< current inode index within the tracking file              */
     exp_trck_file_header_t *tracking_file_hdr_p;  /**< pointer to the tracking file header     */
} exp_trck_header_memory_t;


#define EXP_TRCK_MAX_USER_ID 256
/**
*  top header associated with a given object type
*/
typedef struct _exp_trck_top_header_t
{  
   char name[256];    /**< name of the table   */
   uint16_t max_attributes_sz;  /**< max size of the attributes :512,1024,1024+512 etc.. */
   char root_path[1024];  
   int create_flag;    /**< assert to 1 when tracking main file has to be created */
   exp_trck_header_memory_t *entry_p[EXP_TRCK_MAX_USER_ID];
   void *trck_inode_p;  /**< memory structure used for inode tracking */
} exp_trck_top_header_t;

extern int64_t exp_trk_malloc_size; 
/*
**__________________________________________________________________
*/
/**
*    P U B L I C   A  P I
*/
/*
**__________________________________________________________________
*/
/**
* create the top header of a given inode table

   @param name : name of the table
   @param max_attributes_sz : max attribute size
   @param root_path : root pathname
   @param create_flag: indicates if the main tracking files have to be created

   @retval <> NULL : pointer to the allocated structure
   @retval NULL: out of memory
*/
exp_trck_top_header_t *exp_trck_top_allocate(char *name,char *root_path,uint16_t max_attributes_sz,int create_flag);

/**
*  Get the current allocated size
*/
static inline int64_t exp_trk_get_memory()
{
  return exp_trk_malloc_size;

}
/*
**__________________________________________________________________
*/
/**
*  add a user id entry in the top tracking table

    @param top_hdr_p : pointer to the top table
    @param user_id : user id to insert
    
    @retval 0 on success
    @retval <0 on error see errno for detail
*/
int exp_trck_top_add_user_id(exp_trck_top_header_t *top_hdr_p,int user_id);

/*
**__________________________________________________________________
*/
/**
* release the top header of a given inode table

   @param top_hdr_p : pointer to the top header of an inode table

   @retval 0 on success
   @retval < 0 on error (see errno for details)
*/
int exp_trck_top_release(exp_trck_top_header_t *top_hdr_p);
/*
**__________________________________________________________________
*/
/**
*    allocate an inode within a given space
    
    @param top_hdr_p: pointer to the top table
    @param inode: address of the inode
    @param key: key associated with the inode (opaque to the inode allocator
    @param slice: slice to which the inode will belong
    
    @retval 0 on success
    @retval -1 on error
    
*/
int exp_metadata_allocate_inode(exp_trck_top_header_t *top_hdr_p,rozofs_inode_t *inode,uint16_t key,uint8_t slice);
/*
**__________________________________________________________________
*/
/**
*
    release an inode within a given space
    
    @param top_hdr_p: pointer to the top table
    @param inode: address of the inode
    
    @retval 0 on success
    @retval -1 on error
    
*/
int exp_metadata_release_inode(exp_trck_top_header_t *top_hdr_p,rozofs_inode_t *inode);
/*
**__________________________________________________________________
*/
/**
*
    write attributes associated with an inode within a given space
    
    @param top_hdr_p: pointer to the top table
    @param inode: address of the inode
    @param attr_p: pointer to the attribute array
    @param attr_sz: size of the attributes
    @param sync: whether to force sync on disk of attributes
    
    @retval 0 on success
    @retval -1 on error    
*/
int exp_metadata_write_attributes(exp_trck_top_header_t *top_hdr_p,rozofs_inode_t *inode,void *attr_p,int attr_sz, int sync);
/*
**__________________________________________________________________
*/
/**
*
    read attributes associated with an inode within a given space
    
    @param top_hdr_p: pointer to the top table
    @param inode: address of the inode
    @param attr_p: pointer to the attribute array
    @param attr_sz: size of the attributes
    
    @retval 0 on success
    @retval -1 on error    
*/
int exp_metadata_read_attributes(exp_trck_top_header_t *top_hdr_p,rozofs_inode_t *inode,void *attr_p,int attr_sz);
/*
**__________________________________________________________________
*/
/**
*
    get the file header of a tracking file within a given user_id directory
    
    @param top_hdr_p: pointer to the top table
    @param user_id: reference of the directory
    @param file_id : reference of the tracking file
    @param buf_p: size of the attributes
    @param nb_entries_p: pointer where service will return the number iof entries (might be NULL)
    
    @retval 0 on success
    @retval -1 on error    
*/
int exp_metadata_get_tracking_file_header(exp_trck_top_header_t *top_hdr_p,
                                          int user_id,uint64_t file_id,exp_trck_file_header_t *buf_p,int *nb_entries_p);
					  
					  
/*
**__________________________________________________________________
*/
/**
*  get the number of file within the tracking file

   @param track_hdr_p : pointer to the tracking header
   
   @retval : number of active files
*/
uint64_t exp_metadata_get_tracking_file_count(exp_trck_file_header_t *track_hdr_p);

/*
**__________________________________________________________________
*/
/**
*
   Init of the tracking file search context
   
   @param main_trck_p : pointer to the main tracking context of a user_id
   
   @retval 0
   @retval == NULL error (see errno for details)
   
*/
int exp_metadata_trck_lookup_ctx_init(exp_trck_header_memory_t  *main_trck_p);
/*
**__________________________________________________________________
*/
/**
*    get the  header of a main tracking file within a given user_id directory : read from memory context
    
    @param top_hdr_p: pointer to the top table
    @param user_id: reference of the directory
    @param buf_p: pointer when data of the main tracking fila are returned
    
    @retval 0 on success
    @retval -1 on error
    
*/
int exp_metadata_get_main_tracking_file_header(exp_trck_top_header_t *top_hdr_p,
                                               int user_id,exp_trck_header_t *buf_p);


/*
**__________________________________________________________________
*/
/**
*    update the  header of a main tracking file within a given user_id directory : 
       read from disk and update the context in memory
    
    @param top_hdr_p: pointer to the top table
    @param user_id: reference of the directory
    @param buf_p: pointer when data of the main tracking fila are returned
    
    @retval 0 on success
    @retval -1 on error
    
*/
int exp_metadata_update_main_tracking_file_header(exp_trck_top_header_t *top_hdr_p,
                                               int user_id,exp_trck_header_t *buf_p);


/*
**__________________________________________________________________
*/
/**
*  increment the last idx of  the main tracking file
  
   @param root_path : root pathname of the tracking main file
   @param user_id : index of the directory within the root path 
   @param offset: main tracking file write offset
   @param size: size to write
   @data_p: pointer to the data array to write on disk
   
   @retval 0 on success
   @retval -1 on error (see errno for details

*/
int exp_trck_write_main_tracking_file(char * root_path,uint8_t user_id,off_t offset,size_t size,void *data_p);					       

/*
**__________________________________________________________________
*/
/**
*
    write attributes associated with an inode within a given space
    
    @param top_hdr_p: pointer to the top table
    @param inode: address of the inode
    @param attr_p: pointer to the attribute array
    @param attr_sz: size of the attributes
    @param sync: whether to force sync on disk of attributes

    
    @retval 0 on success
    @retval -1 on error
    
*/
int exp_metadata_create_attributes_burst(exp_trck_top_header_t *top_hdr_p,rozofs_inode_t *inode,void *attr_p,int attr_sz, int sync);

/*
**__________________________________________________________________
*/
/**
*
    get the stats of a tracking file within a given user_id directory
    
    @param top_hdr_p: pointer to the top table
    @param user_id: reference of the directory
    @param file_id : reference of the tracking file
    @param buf_p: pointer to the stat buffer
    
    @retval 0 on success
    @retval -1 on error    
*/
int exp_metadata_get_tracking_file_stat(exp_trck_top_header_t *top_hdr_p,
                                        int user_id,uint64_t file_id,struct stat *buf_p);

/*
**__________________________________________________________________
*/
/**
*
    Assert the delete pending bit of an inode
    
    @param fid: inode reference
    
    @retval none
*/
static inline void exp_metadata_inode_del_assert(fid_t fid)
{
   rozofs_inode_t *fake_inode = (rozofs_inode_t*)fid;
   fake_inode->s.del=1;
}

/*
**__________________________________________________________________
*/
/**
*
    De-assert the delete pending bit of an inode
    
    @param fid: inode reference
    
    @retval none
*/
static inline void exp_metadata_inode_del_deassert(fid_t fid)
{
   rozofs_inode_t *fake_inode = (rozofs_inode_t*)fid;
   fake_inode->s.del=0;
}
/*
**__________________________________________________________________
*/
/**
*
    Asset the delete pending bit of an inode
    
    @param fid: inode reference
    
    @retval 1 when the i-node has the delete pending bit asserted
    @retval 0 when the i-node has not the delete pending bit asserted
*/
static inline int exp_metadata_inode_is_del_pending(fid_t fid)
{
   rozofs_inode_t *fake_inode = (rozofs_inode_t*)fid;
   if((fake_inode->s.del!=0) || (fake_inode->s.key == ROZOFS_TRASH)) return 1;
   return 0;
}

#define EXP_TRK_FREE(p)  exp_trk_free((uint64_t*)p,__LINE__);
#define EXP_TRK_MALLOC(length)  exp_trk_malloc((int)length,__LINE__);
static inline void exp_trk_free(uint64_t *p, int line) {
    uint64_t size;
    p -= 1;
    size = *p;
    exp_trk_malloc_size -= size;
    free(p);
}
static inline void *exp_trk_malloc(int size, int line) {
    uint64_t *p;

    p = memalign(32,size + 8);
    if (p == NULL )
    {
        printf("Out of memory at line %d\n", line);
	return NULL;
    }
    exp_trk_malloc_size += (uint64_t) size;
    *p = size;
    return p + 1;
}



#endif
