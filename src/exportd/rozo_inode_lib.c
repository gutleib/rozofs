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
#include <sys/param.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <uuid/uuid.h>
#include <errno.h>
#include <string.h>
#include <rozofs/common/export_track.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/rozofs.h>
#include <src/exportd/exp_cache.h>
#include <getopt.h>
#include <sys/time.h>
#include "export.h"
#include "mdirent.h"
#include "rozo_inode_lib.h"

#define DIRENT_ROOT_FILE_IDX_SHIFT 12
#define DIRENT_ROOT_FILE_IDX_MASK ((1<<DIRENT_ROOT_FILE_IDX_SHIFT)-1)
#define DIRENT_ROOT_FILE_IDX_MAX   (1 << DIRENT_ROOT_FILE_IDX_SHIFT)

/**
* Globals
*/
int fdl_debug = 0;
int verbose_mode = 1;
uint64_t rozo_lib_current_file_id = 0;  /**< current file in use for inode tracking            */
int      rozo_lib_current_user_id = 0;  /**< current slice directory in use for inode tracking */
int      rozo_lib_current_inode_idx = 0; /**< current inode index in current file             */

int      rozo_lib_stop_var        = 0;        /**< assert to one for stopping the inode reading      */
/*
** prototypes
*/
mdirents_cache_entry_t * dirent_get_root_entry_from_cache(fid_t fid, int root_idx);
int dirent_remove_root_entry_from_cache(fid_t fid, int root_idx);
int export_dir_load_root_idx_bitmap(export_t *e,fid_t fid,lv2_entry_t *lvl2);
/**
*  structures
*/
typedef struct _buf_work_t
{
   exp_trck_top_header_t *inode_metadata_p;
   check_inode_dirent_pf_t inode_check_cbk;
   char pathname[1024];
   off_t offset;
   int fd;    /**< file descriptor  */
   char *buf;  /**< pointer to the beginning of buffer  */
   int len;    /**< buffer length  */
   int cur_len; /**< current length */
   int read_idx;;
   int len_disk;
   char *bufdisk;
} buf_work_t;

/**
*  Global variables
*/
uint64_t number_of_file_transistions = 0;
uint64_t file_match_count = 0;
exp_trck_file_header_t tracking_buffer;
buf_work_t *buf_reg_save = NULL;
buf_work_t *buf_dir_save = NULL;
buf_work_t *buf_dir_read_save = NULL;
int count_threshold = 200000;

extern export_tracking_table_t * export_tracking_table[];
/*
**_________________________________________________________________

      WORKING BUFFER SECTION
**_________________________________________________________________
*/
/**
**______________________________________________________________
*/

/*
**_________________________________________________________________
*/
/**
*  
  allocate a working buffer
  
  @param pathname : pathname of the file
  @param inode_metadata_p: pointer to the export metadata top level structure
  @param inode_check_cbk : inode check procedure (NULL if not is required
  
  @retval <> NULL: pointer to the allocated working buffer
*/
buf_work_t *wbuf_create(char *pathname,int size,exp_trck_top_header_t *inode_metadata_p,check_inode_dirent_pf_t inode_check_cbk)
{
   buf_work_t *buf_p = malloc(sizeof(buf_work_t));
   if (buf_p == NULL) return buf_p;
   
   memset(buf_p,0,sizeof(buf_work_t));
   buf_p->fd=-1;
   buf_p->buf = malloc(size);
   if (buf_p->buf == NULL)
   {
      free(buf_p);
      return NULL;
   }
   buf_p->len = size;
   buf_p->cur_len = 0;
   /*
   ** open the file in read/write mode
   */
   buf_p->fd = open(pathname, O_RDWR | O_CREAT, 0640);
   if (buf_p->fd == -1)
   {
     free(buf_p->buf);
      free(buf_p);
      return NULL;
   }
   buf_p->inode_metadata_p = inode_metadata_p;
   buf_p->inode_check_cbk = inode_check_cbk;
   return buf_p;     
}
    
/**
**______________________________________________________________
*/
#define IDLE_ST 0
#define TRACK_ST 1
/*
**_________________________________________________________________
*/
/**
*   private API that check if the buffer has to be flush on disk

    the entry is a TLV type:
     length: length of the entry excluding the length field ( 2 bytes)
     inode : rozofs_inode structure (16 bytes)
     name : object name ending by \0.

   @param buf_p : pointer to the working buffer structure

   @retval >= 0 success
   @retval < 0 failure
*/   
int wbuf_write_check(buf_work_t *buf_p)
{
  int cur_len;
  rozofs_inode_t *inode_p;
  uint16_t *p16;
  uint16_t length;
  uint64_t file_id = 0;
  file_id =~file_id;
  int usr_id = -1;
  int count = 0;
  int state = IDLE_ST;
  char *buf_start=NULL;
  int i;
  int ret;
  ext_mattr_t  ext_attr;
  
  buf_p->len_disk = 0; 
  if (buf_p->bufdisk == NULL)
  {
    buf_p->bufdisk = malloc(buf_p->len);   
  }
  if (buf_p->bufdisk == NULL)
  {
     severe("Out of memory");
     printf("Out of memory!!\n");
     exit(-1);
  }
   /*
   ** go through the entries stored in memory and
   ** check if data must be saved disk
   */
   /*
   ** step 1: get the file_id associated with the fid
   **         all the entries share the same slice since they
   **         belong to the same directory
   */
   cur_len = 0;
   while(cur_len < buf_p->cur_len)
   {
      /*
      ** get the length
      */
      p16 = (uint16_t*)&buf_p->buf[cur_len];
      length = *p16;
      cur_len +=sizeof(uint16_t);
      /*
      ** get the fid
      */
      inode_p = (rozofs_inode_t*)&buf_p->buf[cur_len];
      cur_len+=length;

      switch (state)
      {
	case TRACK_ST:
	  /*
	  ** check if there is a change in the file_id and usr_id
	  */
	  if ((file_id == inode_p->s.file_id) && (usr_id == inode_p->s.usr_id))
	  {
	     count+=1;
	     continue;
	  }
	  /*
	  ** there is a change in the inode file: go through the entries
	  ** that have already been tracked
	  */
	  if (count < count_threshold)
	  {
	     /*
	     ** read attributes per attributes to minimize unecessary reads
	     */
	     int size = 0;
	     int length2;
             rozofs_inode_t *inode2_p;
	     char *name_p;
	     for (i = 0; i < count; i++)
	     {
	       p16 = (uint16_t*)&buf_start[size];
	       length2 = *p16;
	       size +=sizeof(uint16_t);
	       /*
	       ** get the fid
	       */
	       inode2_p = (rozofs_inode_t*)&buf_start[size];
	       /*
	       ** pointer to the filename
	       */
	       name_p = &buf_start[size+sizeof(fid_t)];
	       size +=length2;
	       
	       ret = exp_metadata_read_attributes(buf_p->inode_metadata_p,inode2_p,&ext_attr,sizeof(ext_attr));
	       if (ret < 0)
	       {
	          printf("cannot get attributes for file %s \n",name_p);
		  continue;
	       }
	       /*
	       ** check the ctime and mtime of the attributes
	       */
#warning no callback fct yet defined
//	       ret = (*buf_p->inode_check_cbk)(&ext_attr);
	       if (ret == 1)
	       {
		 /*
		 ** push the entry in the output buffer to save on disk
		 */
   	        // name_p[length2-sizeof(fid_t)-1] = 0xa;
		 file_match_count++;
//       	         if ((file_match_count % 10000) == 0) printf("file_match_count %llu\n",file_match_count);
		 memcpy(&buf_p->bufdisk[buf_p->len_disk],name_p,length2-sizeof(fid_t));
		 buf_p->len_disk +=length2-sizeof(fid_t);
	       }	   
	     }
	  }
	  else
	  {
	     /*
	     ** here we have a large number of inodes, so it makes sense to load-up
	     ** all the attributes file in memory
	     */
	  }  
	  /*
	  ** that's is finish for that file_id/usr_id, so let's go back to IDLE state
	  */
        case IDLE_ST:
	  /*
	  ** get the file_id and user_id from inode
	  */
	  number_of_file_transistions++;
	  usr_id  = inode_p->s.usr_id;
	  file_id = inode_p->s.file_id;
	  count=1;
	  buf_start = &buf_p->buf[cur_len-sizeof(uint16_t)-length];
	  state = TRACK_ST;
	  break;
	          			
      }      
    }
    /*
    ** OK we went through all the entries, so we can write to disk
    ** but before we need to check if there is some pending inodes for
    ** which to control has not yet been done
    */
    if (TRACK_ST == state)
    {
      if (count < count_threshold)
      {
	 /*
	 ** read attributes per attributes to minimize unecessary reads
	 */
	 char *name_p;
	 int size = 0;
	 for (i = 0; i < count; i++)
	 {
	   p16 = (uint16_t*)&buf_start[size];
	   length = *p16;
	   size +=sizeof(uint16_t);
	   /*
	   ** get the fid
	   */
	   inode_p = (rozofs_inode_t*)&buf_start[size];
	   name_p = &buf_start[size+sizeof(fid_t)];
	   size+=length;
	   
	   ret = exp_metadata_read_attributes(buf_p->inode_metadata_p,inode_p,&ext_attr,sizeof(ext_attr));
	   if (ret < 0)
	   {
	      printf("cannot get attributes for file %s \n",name_p);
	      continue;
	   }
	   /*
	   ** check the ctime and mtime of the attributes
	   */
#warning no callback fct yet defined
//	   ret = (*buf_p->inode_check_cbk)(&ext_attr);
	   if (ret == 1)
	   {
	     /*
	     ** push the entry in the output buffer to save on disk
	     */
	    // name_p[length-sizeof(fid_t)-1] = 0xa;
             file_match_count++;
//	     if ((file_match_count % 10000) == 0) printf("file_match_count %llu\n",file_match_count);
	     memcpy(&buf_p->bufdisk[buf_p->len_disk],name_p,length-sizeof(fid_t));
	     buf_p->len_disk +=length-sizeof(fid_t);
	   }	   
	 }
      }
      else
      {
	 /*
	 ** here we have a large number of inodes, so it makes sense to load-up
	 ** all the attributes file in memory
	 */
      }      
    }
    /*
    ** write to disk
    */
    if (buf_p->len_disk == 0) return 0;
    ssize_t size_wr = pwrite(buf_p->fd,buf_p->bufdisk,buf_p->len_disk,buf_p->offset);
    if (size_wr != buf_p->len_disk) {
      severe("exit");
      exit(0);
    }  
    buf_p->offset +=buf_p->len_disk;
    buf_p->cur_len = 0;  
    buf_p->len_disk = 0; 
    return 0;
}

/**
**______________________________________________________________
*/
/**
    push the input parameter the working buffer
    
    When there is no callback associated with the buffer, the
    data are flushed on disk. The name of the file is the one
    saved in the working buffer context. That information is
    provided at working buffer allocation time.
    
    The write check function is called only when data that is
    in memory has to be flushed on disk. 
    
    @param buf_p: pointer to the working buffer
    @param name : object name
    @param fid : rozofs inode
    
    @retval 0 on success
    @retval < 0 on error
*/
int wbuf_write(buf_work_t *buf_p,char *name,fid_t fid)
{
  uint16_t *p16;
  uint8_t  *p8;
  ssize_t size;
  /*
  ** Get the length of the string
  */
  int len = strlen(name);
  len+=1;
  len +=sizeof(fid_t);
  if ((len+buf_p->cur_len+sizeof(uint16_t)) > buf_p->len)
  {
    if (buf_p->inode_check_cbk == NULL)
    {
       /*
       ** push data on disk
       */

       size = pwrite(buf_p->fd,buf_p->buf,buf_p->cur_len,buf_p->offset);
       if (size != buf_p->cur_len) {
         severe("exit");    
         exit(0);
       }  
     }
     else
     {
       wbuf_write_check(buf_p);
     }
     buf_p->offset +=buf_p->cur_len;
     buf_p->cur_len = 0;
  }
  p16 = (uint16_t*)&buf_p->buf[buf_p->cur_len];
  *p16 = len;
  p8 = (uint8_t*)&buf_p->buf[buf_p->cur_len];
  p8+=2;
  memcpy(p8,fid,sizeof(fid_t));
  p8+=sizeof(fid_t);
  strcpy((char*)p8,name);
  buf_p->cur_len +=len+sizeof(uint16_t);
  
  return 0;
}

/**
**______________________________________________________________
*/
/*
** read data from a wroking buffer. The data provided
   are the object name and its inode.
   It is assumed that the file associated with the wroking buffer
   has been previously opened
   
   @param buf_p : pointer to the working context
   @param name : pointer to the array where object name is returned
   @param fid : pointer to the array where the inode is returned
   
   @retval 0 on success
   @retval -1 on EOF
*/
#warning need to set errno to address the case of EOF
int wbuf_read(buf_work_t *buf_p,char *name,fid_t fid)
{
  uint16_t *p16;
  uint8_t  *p8;
  ssize_t size;
  int len;
  /*
  ** Get the length of the string
  */
  if ((buf_p->cur_len == 0)|| ((buf_p->cur_len != 0)&&(buf_p->cur_len == buf_p->read_idx)))
  {
     /*
     ** attempt to read from disk
     */
     size = pread(buf_p->fd,buf_p->buf,buf_p->len,buf_p->offset);
     if (size < 0)
     {
       /*
       ** error while reading
       */
       severe("exit");
       exit(0);
     }
     if (size == 0)
     {
       /*
       ** eof is encountered
       */
       return -1;
     }
     buf_p->cur_len  = size;
     buf_p->read_idx = 0;
     buf_p->offset += size;
  }
  /*
  ** read the data from the buffer
  */
  p16 = (uint16_t*) &buf_p->buf[buf_p->read_idx];
  len = *p16;
  p16+=1;
  p8 = (uint8_t*)p16;
  memcpy(fid,p8,sizeof(fid_t));
  p8 +=sizeof(fid_t);
  strcpy(name,(char*)p8);
  buf_p->read_idx += len+sizeof(uint16_t); 
  
  return 0;
}

/**
**______________________________________________________________
*/  
/**
   That service is an unconditional flush of the data residing
   in memory towards the disk.
   
   If there is a callback associated with the working buffer
   The write on disk wil depends on the return value of the 
   check callback.
   
   @param buf_p: pointer to the working buffer
   
   @retval 0 on success
   @retval < 0 on error
    
*/
int wbuf_flush(buf_work_t *buf_p)
{
  ssize_t size;
  if (buf_p->fd == -1) return -1;
  {
    if (buf_p->inode_check_cbk == NULL)
    {
       /*
       ** push data on disk
       */
       size = pwrite(buf_p->fd,buf_p->buf,buf_p->cur_len,buf_p->offset);
       if (size != buf_p->cur_len) {
         severe("exit");    
         exit(0);
       }  
  //     printf("FDL flush size %u \n",size);
     }
     else
     {
       wbuf_write_check(buf_p);
     }
     buf_p->offset +=buf_p->cur_len;
     buf_p->cur_len = 0;
  }  
  return 0;
}

/**
**______________________________________________________________
*/  
/**
     API for closing the file associated with a write buffer
     
     If there is some pending data in the memory buffer, the 
     data are flushed on disk prior closing the associated file
     
     @param buf_p:  pointer to the working buffer context
     
     @retval 0 success
     @retval < 0 error
     
*/
int wbuf_close(buf_work_t *buf_p)
{
  if (buf_p->fd == -1) return -1;
  if (buf_p->cur_len != 0) wbuf_flush(buf_p);
  close(buf_p->fd);
  buf_p->fd = -1;
  return 0;
}
/**
**______________________________________________________________
*/  
/**
*    Release a working buffer

     If the working buffer is not empty, the working buffer
     is flushed and associated file is closed prior releasing
     the memory used by the working buffer
     
     @param buf_p:  pointer to the working buffer context
     
     @retval 0 success
     @retval < 0 error
*/
int wbuf_release(buf_work_t *buf_p)
{
  if (buf_p->cur_len != 0) wbuf_flush(buf_p);
  if (buf_p->fd == -1) return -1;
  close(buf_p->fd);
  if (buf_p->buf != NULL) free(buf_p->buf);
  free(buf_p);
  return 0;
}

/*
**_________________________________________________________________

      DENTRY SECTION
**_________________________________________________________________
*/

/**
*  child structure used by listdir
*/
typedef struct _child {
    char *name;  /**< name of the object */
    int   mode;   /**< file mode */
    fid_t fid;    /**< inode of the object */
    struct _child *next;
} child_mode_t;

/**
* local data to the section
*/
lv2_entry_t fake_level2_entry;
lv2_cache_t cache;
export_t *fake_export_p = NULL;

/**
**______________________________________________________________
*/  
/*
   release the buffer that contains the dirent file bitmap
*/
void root_bitmap_release()
{
   if (fake_level2_entry.dirent_root_idx_p != NULL)
   {
     free(fake_level2_entry.dirent_root_idx_p);
     fake_level2_entry.dirent_root_idx_p = NULL;
   }
}

/**
**______________________________________________________________
*/  
/**
 *   Compute the hash values for the name and fid: called from listdir

 @param key1 : pointer to a string 
 @param key2 : pointer to a fid (16 bytes)
 @param hash2 : pointer to the second hash value that is returned
 @param len : len of the key 1(trailing \0 must not be included)

 @retval primary hash value
 */
static inline uint32_t filename_uuid_hash_fnv_with_len(uint32_t h, void *key1, void *key2, uint32_t *hash2, int len) {

    unsigned char *d = (unsigned char *) key1;
    int i = 0;

    if (h == 0) h = 2166136261U;
    /*
     ** hash on name
     */
    d = key1;
    for (i = 0; i <len ; d++, i++) {
        h = (h * 16777619)^ *d;

    }

    *hash2 = h;
    /*
     ** hash on fid
     */
    d = (unsigned char *) key2;
    for (d = key2; d != key2 + 16; d++) {
        h = (h * 16777619)^ *d;

    }
    return h;
}

/*
 **______________________________________________________________________________
 */

/**
 * API for get a mdirentry in one parent directory
 *
 * @param mdir: pointer to the mdirent structure for directory specific attributes
 * @param name: (key) pointer to the name to search
 * @param *fid: pointer to the unique identifier for this mdirentry
 * @param *type: pointer to the type for this mdirentry
 *
 * @retval  0 on success
 * @retval -1 on failure
 */
typedef union _dirent_list_cookie_t {
    uint64_t val64;

    struct {
        uint64_t index_level : 1; /**< 0: root file, 1 : collision file          */
        uint64_t root_idx : 12; /**< currenr root file index                   */
        uint64_t coll_idx : 11; /**< index of the next collision file to test  */
        uint64_t hash_entry_idx : 10; /**< index of the next bitmap entry to test    */
        uint64_t filler : 30; /**< for future usage                         */

    } s;
} dirent_list_cookie_t;

int list_mdirentries_internal(void *root_idx_bitmap_p,int dir_fd, fid_t fid_parent, child_mode_t ** children, uint64_t *cookie, uint8_t * eof) {
    int root_idx = 0;
    int cached = 0;
    child_mode_t ** iterator;
    dirent_list_cookie_t dirent_cookie;
    mdirents_header_new_t dirent_hdr;
    mdirents_cache_entry_t *root_entry_p = NULL;
    mdirents_cache_entry_t *cache_entry_p;
    mdirents_hash_entry_t *hash_entry_p = NULL;
    mdirent_sector0_not_aligned_t *sect0_p;
    int hash_entry_idx = 0;
    int index_level = 0;
    int read_file = 0;
    int coll_idx;
    int loop_cnt = 0;
    int next_coll_idx = 0;
    int bit_idx;
    int chunk_u8_idx;
    uint8_t *coll_bitmap_p;
    int next_hash_entry_idx;
    int root_idx_bit;

   /*
   ** set the pointer to the root idx bitmap
   */
   dirent_set_root_idx_bitmap_ptr(root_idx_bitmap_p);

    dirent_readdir_stats_call_count++;
    /*
     ** load up the cookie to figure out where to start the read
     */
    dirent_cookie.val64 = *cookie;
    iterator = children;

    /*
     ** set the different parameter
     */
    root_idx = dirent_cookie.s.root_idx;
    hash_entry_idx = dirent_cookie.s.hash_entry_idx;
    index_level = dirent_cookie.s.index_level;
    coll_idx = dirent_cookie.s.coll_idx;
    *eof = 0;
    /*
     **___________________________________________________________
     **  loop through the potential root file index
     **  We exit fro the while loop once one has been found or if
     **  the last dirent root file index has been reached
     **___________________________________________________________
     */
    while (read_file < MAX_DIR_ENTRIES) {
        while (root_idx < DIRENT_ROOT_FILE_IDX_MAX) {

	   /*
	   ** Allow a priori to read and write on the root cache entry
	   ** In case of some error while reading the dirent files from 
	   ** disk, the rigths may be downgraded to read only.
	   */
	   DIRENT_ROOT_SET_READ_WRITE();
	   
	   /*
	   ** check if the bit is asserted for the root_idx
	   */
	   root_idx_bit = dirent_check_root_idx_bit(root_idx);
	   if (root_idx_bit == 1)
	   {
              /*
               ** attempt to get the dirent root file from the cache
               */
              root_entry_p = (mdirents_cache_entry_t*)dirent_get_root_entry_from_cache(fid_parent, (int)root_idx);
              if (root_entry_p == NULL) {
                  /*
                   ** dirent file is not in the cache need to read it from disk
                   */
                  dirent_hdr.type = MDIRENT_CACHE_FILE_TYPE;
                  dirent_hdr.level_index = 0;
                  dirent_hdr.dirent_idx[0] = root_idx;
                  dirent_hdr.dirent_idx[1] = 0;
                  root_entry_p = read_mdirents_file(dir_fd, &dirent_hdr,fid_parent);
              } else {
                  /*
                   ** found one, so process its content
                   */
                  cached = 1;
                  break;
              }
              /*
               ** ok now it depends if the entry exist on not
               */
              if (root_entry_p != NULL) {
                  break;
              }
	    }
            /*
             ** That root file does not exist-> need to check the next root_idx
             */
            root_idx++;
        }
        /*
         **_____________________________________________
         ** Either there is an entry or there is nothing
         **_____________________________________________
         */
        if (root_entry_p == NULL) {
            /*
             ** we are done
             */
            *eof = 1;
            break;
        }
        /*
         ** There is a valid entry, but before doing the job, check if the entry has
         **  been extract from the cache or read from disk. If entry has been read
         ** from disk, we attempt to insert it in the cache.
         */
#if 0
        if (cached == 0) {
            /**
             * fill up the key associated with the file
             */
            memcpy(root_entry_p->key.dir_fid, fid_parent, sizeof (fid_t));
            root_entry_p->key.dirent_root_idx = root_idx;
            /*
             ** attempt to insert it in the cache if not in cache
             */
            if (dirent_put_root_entry_to_cache(fid_parent, root_idx, root_entry_p) == 0) {
                /*
                 ** indicates that entry is present in the cache
                 */
                cached = 1;
            }
        }
#endif
        /*
         **___________________________________________________________________________
         ** OK, now start the real job where we start scanning the collision file and
         ** the allocated hash entries. Here there is no readon to follow the link list
         ** of the bucket, checking the bitmap of the hash entries is enough.
         ** There is the same approach for the case of the collision file
         **___________________________________________________________________________
         */
        /*
         ** Need to get the pointer to the hash entry bitmap to figure out which
         ** entries are allocated
         */
        sect0_p = DIRENT_VIRT_TO_PHY_OFF(root_entry_p, sect0_p);
        if (sect0_p == (mdirent_sector0_not_aligned_t*) NULL) {
            DIRENT_SEVERE("list_mdirentries sector 0 ptr does not exist( line %d\n)", __LINE__);
	    if (cached == 1) 
	    {
	      /*
	      ** the content of the dirent file (root+ collision) is in the cache
	      ** need to remove from cache if read only is asserted
	      */
	      if (DIRENT_ROOT_IS_READ_ONLY())
	      {
        	dirent_remove_root_entry_from_cache(fid_parent, root_idx);
		dirent_cache_release_entry(root_entry_p);
	      }
	      cached = 0;
	    }
            root_idx++;
            root_entry_p = NULL;
            continue;
        }
        coll_bitmap_p = (uint8_t*) & sect0_p->coll_bitmap;
        cache_entry_p = root_entry_p;

get_next_collidx:
        if (index_level != 0) {
            /*
             ** case of the collision file, so need to go through the bitmap of the
             ** dirent root file
             */
            cache_entry_p = NULL;
            while (coll_idx < MDIRENTS_MAX_COLLS_IDX) {
                chunk_u8_idx = coll_idx / 8;
                bit_idx = coll_idx % 8;
                /*
                 ** there is no collision dirent entry or the collision dirent entry exist and is not full
                 */
                if ((coll_bitmap_p[chunk_u8_idx] & (1 << bit_idx)) != 0) {
                    /*
                     ** That entry is free, need to find out the next entry that is busy (0: busy, 1:free)
                     */
                    if (coll_idx % 8 == 0) {
                        next_coll_idx = check_bytes_val(coll_bitmap_p, coll_idx, MDIRENTS_MAX_COLLS_IDX, &loop_cnt, 1);
                        if (next_coll_idx < 0) break;
                        /*
                         ** next  chunk
                         */
                        if (next_coll_idx == coll_idx) coll_idx++;
                        else coll_idx = next_coll_idx;
                        continue;
                    }
                    /*
                     ** next chunk
                     */
                    hash_entry_idx = 0;
                    coll_idx++;
                    continue;
                }
                /*
                 ** one collision idx has been found
                 ** need to get the entry associated with the collision index
                 */
                cache_entry_p = dirent_cache_get_collision_ptr(root_entry_p, coll_idx);
                if (cache_entry_p == NULL) {
                    /*
                     ** something is rotten in the cache since the pointer to the collision dirent cache
                     ** does not exist
                     */
                    DIRENT_SEVERE("list_mdirentries not collisiob file %d\n", coll_idx);
                    /*
                     ** OK, do not break the analysis, skip that collision entry and try the next if any
                     */
                    hash_entry_idx = 0;
                    coll_idx++;
                    continue;
                }
                break;
            }
        }
        /*
         ** OK either we have one dirent entry or nothing: for the nothing case we go to
         ** the next root_idx
         */
        if (cache_entry_p == NULL) {
            /*
             ** check the next root index
             */
            coll_idx = 0;
            hash_entry_idx = 0;
            index_level = 0;
	    if (cached == 1) 
	    {
	      /*
	      ** the content of the dirent file (root+ collision) is in the cache
	      ** need to remove from cache if read only is asserted
	      */
	      if (DIRENT_ROOT_IS_READ_ONLY())
	      {
        	dirent_remove_root_entry_from_cache(fid_parent, root_idx);
		dirent_cache_release_entry(root_entry_p);
	      }
	      cached = 0;
	    }
	    else
	    {
	       dirent_cache_release_entry(root_entry_p);
	    
	    }
            root_entry_p = NULL;
            root_idx++;
            continue;
        }
        sect0_p = DIRENT_VIRT_TO_PHY_OFF(cache_entry_p, sect0_p);
        if (sect0_p == (mdirent_sector0_not_aligned_t*) NULL) {
            DIRENT_SEVERE("list_mdirentries sector 0 ptr does not exist( line %d\n)", __LINE__);
            /*
             ** do break the walktrhough, try either the next root entry and collision entry
             */
            cache_entry_p = NULL;
            hash_entry_idx = 0;
            if (index_level == 0) {
                coll_idx = 0;
                index_level = 1;
                goto get_next_collidx;
            }
            coll_idx++;
            goto get_next_collidx;
        }
        /*
         ** Get the pointer to the hash entry bitmap
         */
        while ((hash_entry_idx < MDIRENTS_ENTRIES_COUNT) && (read_file < MAX_DIR_ENTRIES)) {
            next_hash_entry_idx = DIRENT_CACHE_GETNEXT_ALLOCATED_HASH_ENTRY_IDX(&sect0_p->hash_bitmap, hash_entry_idx);
            if (next_hash_entry_idx < 0) {
                /*
                 ** all the entry of that dirent cache entry have been scanned, need to check the next collision file if
                 ** any
                 */
                cache_entry_p = NULL;
                hash_entry_idx = 0;
                /*
                 ** check the next
                 */
                if (index_level == 0) {
                    coll_idx = 0;
                    index_level = 1;
                    goto get_next_collidx;
                }
                coll_idx++;
                goto get_next_collidx;
            }
            hash_entry_idx = next_hash_entry_idx;
            /*
             ** need to get the hash entry context and then the pointer to the name entry. The hash entry context is
             ** needed since it contains the reference of the starting chunk of the name entry
             */
            hash_entry_p = (mdirents_hash_entry_t*) DIRENT_CACHE_GET_HASH_ENTRY_PTR(cache_entry_p, hash_entry_idx);
            if (hash_entry_p == NULL) {
                /*
                 ** something wrong!! (either the index is out of range and the memory array has been released
                 */
                DIRENT_SEVERE("list_mdirentries pointer does not exist at %d\n", __LINE__);
                /*
                 ** ok, let check the next hash_entry
                 */
                hash_entry_idx++;
                continue;
            }
            /*
             ** OK, now, get the pointer to the name array
             */
            mdirents_name_entry_t *name_entry_p;
            name_entry_p = (mdirents_name_entry_t*) dirent_get_entry_name_ptr(dir_fd, cache_entry_p, hash_entry_p->chunk_idx, DIRENT_CHUNK_NO_ALLOC);
            if (name_entry_p == (mdirents_name_entry_t*) NULL) {
                /*
                 ** something wrong that must not occur
                 */
                severe("list_mdirentries: pointer does not exist");
                /*
                 ** ok, let check the next hash_entry
                 */
                hash_entry_idx++;
                continue;
            }

            /**
             *  that's songs good, copy the content of the name in the result buffer
             */
            /*
	    ** check the length of the filename, if the length is 0, it indicates that we read a truncate dirent file
	    ** so we skip that entry and goes to the next one
	    */
	    if (name_entry_p->len == 0)
	    {
	       char fidstr[37];
	       uuid_unparse(fid_parent, fidstr);

                severe("empty name entry in directory %s at hash_idx %d in file d_%d collision idx %d chunk_idx %d",
		        fidstr,hash_entry_idx,root_idx,coll_idx,hash_entry_p->chunk_idx);
                hash_entry_idx++;
                continue;	    
	    
	    }
            {
	      uint32_t hash1;
	      uint32_t hash2;
	      int computed_root_idx;
	      int len = name_entry_p->len;
	      
	      
              hash1 = filename_uuid_hash_fnv_with_len(0, name_entry_p->name, fid_parent, &hash2, len);
              /*
              ** attempt to get the root dirent file from cache: check if there is match with the root idx
	      ** the hash of the hash_entry-> no match: skip the entry	     
              */
              computed_root_idx = hash1 & DIRENT_ROOT_FILE_IDX_MASK;	      
              if ((computed_root_idx != root_idx) ||(hash_entry_p->hash != (hash2 &DIRENT_ENTRY_HASH_MASK)))
	      {
                hash_entry_idx++;
                continue;	   	      	      	      
	      }
            }

            *iterator = xmalloc(sizeof (child_mode_t));
            memset(*iterator, 0, sizeof (child_mode_t));
            memcpy((*iterator)->fid, name_entry_p->fid, sizeof (fid_t));
            (*iterator)->name = strndup(name_entry_p->name, name_entry_p->len);
	    (*iterator)->mode = name_entry_p->type;

            // Go to next entry
            iterator = &(*iterator)->next;
            /*
             ** increment the number of file and try to get the next one
             */
            hash_entry_idx++;
            read_file++;
            dirent_readdir_stats_file_count++;

        }
        /*
         ** Check if the amount of file has been read
         */
        if (read_file >= MAX_DIR_ENTRIES) {
            /*
             ** We are done
             */
            break;
        }
        /*
         ** No the end and there still some room in the output buffer
         ** Here we need to check the next collision entry if any
         */
        cache_entry_p = NULL;
        hash_entry_idx = 0;
        /*
         ** check the next
         */
        if (index_level == 0) {
            coll_idx = 0;
            index_level = 1;
            goto get_next_collidx;
        }
        coll_idx++;
        goto get_next_collidx;
    }
    /*
     ** done
     */
    /*
     ** set the different parameter
     */
    dirent_cookie.s.root_idx = root_idx;
    dirent_cookie.s.hash_entry_idx = hash_entry_idx;
    dirent_cookie.s.index_level = index_level;
    dirent_cookie.s.coll_idx = coll_idx;
    *cookie = dirent_cookie.val64;

    /*
     ** check the cache status to figure out if root entry need to be released
     */
    if (cached == 1) 
    {
      /*
      ** the content of the dirent file (root+ collision) is in the cache
      ** need to remove from cache if read only is asserted
      */
      if (DIRENT_ROOT_IS_READ_ONLY())
      {
        dirent_remove_root_entry_from_cache(fid_parent, root_idx);
	dirent_cache_release_entry(root_entry_p);
      }
      return 0;
    }
    if (root_entry_p != NULL) {
        int ret = 0;
        ret = dirent_cache_release_entry(root_entry_p);
        if (ret < 0) {
            DIRENT_SEVERE(" get_mdirentry failed to release cache entry\n");
        }
    }
    return 0;
}

/*
**______________________________________________________________________________
*/
/** read a directory (Private API)
 *
 * @param e: the export managing the file
 * @param fid: the id of the directory
 * @param children: pointer to pointer where the first children we will stored
 * @param cookie: index mdirentries where we must begin to list the mdirentries
 * @param eof: pointer that indicates if we list all the entries or not
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int export_readdir_internal(export_t * e, fid_t fid, uint64_t * cookie,
        child_mode_t ** children, uint8_t * eof) {
    int status = -1;
    lv2_entry_t *parent = NULL;
    int fdp = -1;
    char buf_fid[64];

    parent = &fake_level2_entry;
    *children = NULL;
    /*
    ** load the root_idx bitmap of the old parent
    */
    root_bitmap_release();
    export_dir_load_root_idx_bitmap(e,fid,parent);
    

    // List directory
    fdp = export_open_parent_directory(e,fid);
    if (fdp == -1) 
    {
       /*
       ** cannot open parent directory for fid
       */
       uuid_unparse(fid,buf_fid);
       printf("(%d)Cannot open directory for fid:%s\n",__LINE__,buf_fid);
       severe("EXIT Cannot open directory for fid:%s",buf_fid);
       exit(0);
      goto out;
    }
    if (list_mdirentries_internal(parent->dirent_root_idx_p,fdp, fid, children, cookie, eof) != 0) {
        goto out;
    }


    status = 0;
out:

    if (fdp != -1) close(fdp);

    return status;
}

/*
**______________________________________________________________________________
*/

/**
*   Public API: scan all the directories from fid provided as input arguments

   @param : pointer to the exportd on which the scan takes place
   @param fid_dir : inode of the directory from which the scan must start
   @param file_count: pointer to an array where the service returns the number of files
   @param root_path: root path of the exportd

*/
int scan_all_directories(export_t *fake_export_p,fid_t fid_dir,uint64_t *file_count_p,char *root_path )
{   
   child_mode_t *children;
   child_mode_t *iterator= NULL;
   child_mode_t *iterator_cur=NULL;
   uint64_t cookie = 0;
   dirent_list_cookie_t last_cookie,returned_cookie;
   uint8_t eof = 0;
   uint64_t file_count = 0;
   last_cookie.val64 = 0;
   returned_cookie.val64= 0;
//   char buf_uuid[64];
   char pathname[8192];

     
   while(eof==0)
   {  
     export_readdir_internal(fake_export_p,fid_dir,&cookie,&children,&eof);    
     iterator = children;
     returned_cookie.val64 = cookie;
     returned_cookie.s.hash_entry_idx= 0;
     returned_cookie.s.filler= 0;

     if (returned_cookie.val64  != last_cookie.val64)
     {
//      printf("idx_lvl %d root_idx %d col_idx %d\n",returned_cookie.s.index_level,returned_cookie.s.root_idx,returned_cookie.s.coll_idx);
     }
     last_cookie.val64 = returned_cookie.val64 ;
     while (iterator != NULL) {

     #if 0  
      memcpy(attrs.fid, iterator->fid, sizeof (fid_t));
      inode_p = (rozofs_inode_t*) attrs.fid;
      inode = inode_p->fid[1];
     #endif
      // Add this directory entry to the buffer
      if (iterator->name!= NULL) 
      { 
	if (S_ISDIR(iterator->mode))
	{
	  /*
	  ** append the directory to the tracking directory file
	  */
	  if ((strcmp(".",iterator->name) == 0 )|| (strcmp("..",iterator->name)==0)) 
	  {
	    iterator_cur = iterator;
	    iterator = iterator->next;	
            if (iterator_cur->name != NULL) free(iterator_cur->name);  
	    free(iterator_cur);
	    continue;
	  }
//          uuid_unparse(iterator->fid,buf_uuid);
//	  printf("%s %s\n",buf_uuid,iterator->name);
          strcpy(pathname,root_path);
	  strcat(pathname,iterator->name);	
          wbuf_write(buf_dir_save,pathname,iterator->fid);	  
	}
	else
	{
//          uuid_unparse(iterator->fid,buf_uuid);
//	  printf("%s %s\n",buf_uuid,iterator->name);
          strcpy(pathname,root_path);
	  strcat(pathname,iterator->name);		
          wbuf_write(buf_reg_save,pathname,iterator->fid);
	  file_count+=1;
	  

	}
      }
      iterator_cur = iterator;
      iterator = iterator->next;
      if (iterator_cur->name != NULL) free(iterator_cur->name);  
      free(iterator_cur);
    }
  }
  *file_count_p = file_count;
//  free(children);
  return 0;
}

/*
**______________________________________________________________________________
*/
/**
*  Public scan an exportd with a criteria

   @param intf_p: pointer to the request
   
*/
int export_scan_from_dir(scan_export_intf_t *intf_p)
{
  struct perf start, stop;  /* statistics */
  fid_t fid_dir;
  intf_p->status = -1;
  int ret;
  uint64_t file_count=0;
  uint64_t total_file_count = 0;
  char buf_pathname[8192];
  char buf_fid[64];
  
  /*
  ** init of the layout variables
  */
  rozofs_layout_initialize();
  /*
  ** allocate a fake export
  */
  fake_export_p = malloc(sizeof(export_t));
  if (fake_export_p == NULL)
  {
     printf("error on fake export allocation: %s\n",strerror(errno));
     intf_p->error = errno; 
     intf_p->line = __LINE__;
     return -1;
  }
  memset(fake_export_p,0,sizeof(export_t));
  /*
  ** init of dirent stuff
  */
  //dirent_cache_level0_initialize();
  //dirent_wbcache_init();
  dirent_wbcache_disable();
  /*
  ** fill up the export structure
  */
  if (!realpath(intf_p->root_path, fake_export_p->root))
  {
      printf("realpath failure for %s : %s",intf_p->root_path,strerror(errno));
     intf_p->error = errno; 
     intf_p->line = __LINE__; 
     return -1;
  }
  memset(&fake_level2_entry,0,sizeof(fake_level2_entry));
  /*
  ** create the tracking context
  */
  fake_export_p->trk_tb_p = exp_create_attributes_tracking_context(fake_export_p->eid,intf_p->root_path,0);
  if (fake_export_p->trk_tb_p == NULL)
  {
     printf("error on tracking context allocation: %s\n",strerror(errno));
     intf_p->error = errno; 
     intf_p->line = __LINE__; 
     return -1;  
  }
  perf_start(&start);
  int directory_count = 0; 
  buf_dir_save = wbuf_create(intf_p->dir_pathname,1024*1024,fake_export_p->trk_tb_p->tracking_table[ROZOFS_DIR],intf_p->dir_cbk);
  if (buf_dir_save == NULL)
  {
     printf("error on wbuf_create: %s\n",strerror(errno));
     intf_p->error = errno; 
     intf_p->line = __LINE__; 
     return -1;      
  }
  buf_reg_save = wbuf_create(intf_p->reg_pathname,1024*1024*16,fake_export_p->trk_tb_p->tracking_table[ROZOFS_REG],intf_p->reg_cbk);
  if (buf_reg_save == NULL)
  {
     printf("error on wbuf_create: %s\n",strerror(errno));
     intf_p->error = errno; 
     intf_p->line = __LINE__; 
     return -1;      
  }  
  buf_dir_read_save = wbuf_create(intf_p->dir_pathname,1024*1024,fake_export_p->trk_tb_p->tracking_table[ROZOFS_DIR],NULL);
  if (buf_dir_read_save == NULL)
  {
     printf("error on wbuf_create: %s\n",strerror(errno));
     intf_p->error = errno; 
     intf_p->line = __LINE__; 
     return -1;      
  }  
  ret = scan_all_directories(fake_export_p,intf_p->fid_start,&file_count,"./");
  if (ret < 0)
  {
     printf("error on wbuf_create: %s\n",strerror(errno));
     intf_p->error = errno; 
     intf_p->line = __LINE__; 
     return -1;      
  }
  /*
  ** update the total count of file matching the requested criteria
  */
  total_file_count +=file_count;
  wbuf_flush(buf_dir_save);
  /*
  ** go through the directories until we reach the end
  */
  memcpy(fid_dir,intf_p->fid_start,sizeof(fid_t));
  while(wbuf_read(buf_dir_read_save,buf_pathname,fid_dir)!=-1)
  { 
      uuid_unparse(fid_dir,buf_fid);
      directory_count++;
     strcat(buf_pathname,"/");
     ret = scan_all_directories(fake_export_p,fid_dir,&file_count,buf_pathname);
     if (ret < 0)
     {
	printf("error on wbuf_create: %s\n",strerror(errno));
	intf_p->error = errno; 
	intf_p->line = __LINE__; 
	return -1;      
     }
     total_file_count +=file_count;
     wbuf_flush(buf_dir_save);
  }
   wbuf_flush(buf_reg_save);

  perf_stop(&stop);
  printf("Read directory performances ");
  perf_print(stop,start,(unsigned long long)(total_file_count));
  printf("number of matching files %llu\n",(long long unsigned int)file_match_count);
  
  intf_p->status = 0;
  intf_p->directory_count = directory_count;
  intf_p->file_count      = total_file_count; 
  return 0; 

}

/**
**_______________________________________________________________________________________

   INODE OPERATIONS
**_______________________________________________________________________________________
*/
#if 0 // FDL
enum comparison_type
{
  COMP_GT,
  COMP_LT,
  COMP_EQ
};
enum xval 
  {
    XVAL_ATIME, XVAL_BIRTHTIME, XVAL_CTIME, XVAL_MTIME, XVAL_TIME
  };

typedef struct _rzcheck_time_val
{
  int window;
  enum xval            xval; 
  enum comparison_type kind;
  struct timespec      ts;
}rzcheck_time_val;

#  define STAT_TIMESPEC(st, st_xtim) ((st)->st_xtim)
/* Return *ST's status change time.  */
static inline struct timespec
get_stat_ctime (struct stat const *st)
{
  return STAT_TIMESPEC (st, st_ctim);
}
/*
**________________________________________________
*/
static int
compare_ts(struct timespec ts1,
	   struct timespec ts2)
{
  if ((ts1.tv_sec == ts2.tv_sec) &&
      (ts1.tv_nsec == ts2.tv_nsec))
    {
      return 0;
    }
  else
    {
      double diff = ts_difference(ts1, ts2);
      return diff < 0.0 ? -1 : +1;
    }
}
/*
**________________________________________________
*/
/* pred_timewindow
 *
 * Returns true if THE_TIME is
 * COMP_GT: after the specified time
 * COMP_LT: before the specified time
 * COMP_EQ: after the specified time but by not more than WINDOW seconds.
 
 @param timespec ts
 */
static bool
rozofs_pred_timewindow(struct timespec ts, rzcheck_time_val *pred_ptr)
{
  switch (pred_ptr->args.reftime.kind)
    {
    case COMP_GT:
      return compare_ts(ts, pred_ptr->args.reftime.ts) > 0;

    case COMP_LT:
      return compare_ts(ts, pred_ptr->args.reftime.ts) < 0;

    case COMP_EQ:
      {
	/* consider "find . -mtime 0".
	 *
	 * Here, the origin is exactly 86400 seconds before the start
	 * of the program (since -daystart was not specified).   This
	 * function will be called with window=86400 and
	 * pred_ptr->args.reftime.ts as the origin.  Hence a file
	 * created the instant the program starts will show a time
	 * difference (value of delta) of 86400.   Similarly, a file
	 * created exactly 24h ago would be the newest file which was
	 * _not_ created today.   So, if delta is 0.0, the file
	 * was not created today.  If the delta is 86400, the file
	 * was created this instant.
	 */
	double delta = ts_difference(ts, pred_ptr->args.reftime.ts);
	return (delta > 0.0 && delta <= pred_ptr->window);
      }
    }
  assert (0);
  abort ();
}
#endif
/*
 *_______________________________________________________________________
*/ 
/**
*  Get the name of the file from the chunk associated with a direntry file
*/
static char bufchunk[2048];

int get_name_from_direntchunk(fid_t fid_child, char *root_path,fid_t fid,mdirent_fid_name_info_t *p,char *bufname)
{
    char str[37];
    char path[1024];
    char dentry_fname[64];
    char *pdata = dentry_fname;   
    uint32_t slice;
    uint32_t subslice;
    int fd = -1;
    int flag = O_RDONLY;
    off_t offset;
    ssize_t size;
    int status = -1;
    mdirents_name_entry_t *buf;
    
    pdata += sprintf(pdata, "d_%d",p->root_idx);
    if (p->coll) pdata += sprintf(pdata, "_%d",p->coll_idx);
	  
    mstor_get_slice_and_subslice(fid, &slice, &subslice);
    uuid_unparse(fid, str);
    sprintf(path, "%s/%d/%s/%s", root_path, slice, str,dentry_fname);

    if ((fd = open(path, flag, S_IRWXU)) == -1)
    {
        //printf("Cannot open the file %s: %s\n",path,strerror(errno));
        goto out;
    }
    /*
    ** now get the chunk
    */
    offset = DIRENT_HASH_NAME_BASE_SECTOR*MDIRENT_SECTOR_SIZE
    + MDIRENTS_NAME_CHUNK_SZ*(p->chunk_idx);
    size = MDIRENTS_NAME_CHUNK_SZ*p->nb_chunk;
    if (pread(fd,bufchunk,size,offset)!=size)
    {
       //printf("error while reading %s:%s",path,strerror(errno));
       goto out;
    } 
        
    buf = (mdirents_name_entry_t*)bufchunk;
    /*
    ** Check that the chunk is still allocated to this FID
    */
    if (memcmp(fid_child, buf->fid, sizeof (fid_t))!=0) {
      goto out; 
    }


    memcpy(bufname,buf->name,buf->len);
    bufname[buf->len]=0;
    
    status = 0;   
out:
    if(fd !=-1) close(fd);
    return status;
    
}

#if 0 // useless see rozofs_string.h
static inline void rozofs_high4bits2Char(uint8_t hexa, char * pChar) {
  uint8_t high = hexa >> 4;
  if (high < 10) *pChar++ = high + '0';
  else           *pChar++ = (high-10) + 'a';
}
static inline void rozofs_low4bits2Char(uint8_t hexa, char * pChar) {
  uint8_t low = hexa & 0x0F;
  if (low < 10) *pChar++ = low + '0';
  else          *pChar++ = (low-10) + 'a';     
}
static inline void rozofs_u8_2_char(uint8_t hexa, char * pChar) {
  rozofs_high4bits2Char(hexa,&pChar[0]);
  rozofs_low4bits2Char(hexa,&pChar[1]);
}
static inline void rozofs_fid2string(uuid_t fid, char * pChar) {
  uint8_t * pFid = (uint8_t *) fid;
  
  rozofs_u8_2_char(pFid[0],&pChar[0]);
  rozofs_u8_2_char(pFid[1],&pChar[2]);    
  rozofs_u8_2_char(pFid[2],&pChar[4]);  
  rozofs_u8_2_char(pFid[3],&pChar[6]);  
  pChar[8] = '-';
  rozofs_u8_2_char(pFid[4],&pChar[9]);
  rozofs_u8_2_char(pFid[5],&pChar[11]); 
  pChar[13] = '-';
  rozofs_u8_2_char(pFid[6],&pChar[14]);
  rozofs_u8_2_char(pFid[7],&pChar[16]); 
  pChar[18] = '-';
  rozofs_u8_2_char(pFid[8],&pChar[19]);
  rozofs_u8_2_char(pFid[9],&pChar[21]); 
  pChar[23] = '-';    
  rozofs_u8_2_char(pFid[10],&pChar[24]);
  rozofs_u8_2_char(pFid[11],&pChar[26]);    
  rozofs_u8_2_char(pFid[12],&pChar[28]);  
  rozofs_u8_2_char(pFid[13],&pChar[30]);  
  rozofs_u8_2_char(pFid[14],&pChar[32]);
  rozofs_u8_2_char(pFid[15],&pChar[34]); 
  pChar[36] = 0;   
}
#endif
/*
**_______________________________________________________________________________
*/
/**
*  Get the name of the object

   @param fid_child: FID of the file which name is loooked up
   @param fname: pointer to the description of the object name
   @param bufout : output buffer;
   @param pfid :parent fid
   
   @retval pointer to the output buffer
*/
char *_rozolib_get_fname(fid_t fid_child, export_t *e,char *bufout,rozofs_inode_fname_t *fname,fid_t pfid)
{
   if (fname->name_type == ROZOFS_FNAME_TYPE_DIRECT)
   {
      memcpy(bufout,fname->name,fname->len);
      bufout[fname->len] = 0;
      return bufout;
   }
   /*
   ** get it from dentry chunk
   */
   get_name_from_direntchunk(fid_child, e->root,pfid,&fname->s.name_dentry,bufout);
   return bufout;
}
char *rozolib_get_fname(fid_t child_fid, void *e,char *bufout,void *fname,fid_t pfid)
{
  return _rozolib_get_fname(child_fid, (export_t *)e,bufout,(rozofs_inode_fname_t*)fname,pfid);
}
/*
 *_______________________________________________________________________
 */
 /**
*   That function is intended to return the relative path to an object:
    @rozofs_uuid@<FID_parent>/<child_name>
    example:
    @rozofs_uuid@1b4e28ba-2fa1-11d2-883f-0016d3cca427
    
    @param exportd: pointer to exportd data structure
    @param inode_p: pointer to the inode
    @param buf : pointer to the output buffer
    @param lenmax: max length of the output buffer
*/
char *rozolib_get_relative_path(void *exportd,void *inode_p,char *buf,int lenmax)
{
   char name[1024];
   lv2_entry_t *plv2;  
   char *pbuf = buf;
   char buf_fid[64];
   ext_mattr_t *inode_attr_p = inode_p;
   rozofs_inode_t *inode_val_p;
   
   
   export_t *e= exportd;
   
   inode_val_p = (rozofs_inode_t*)inode_attr_p->s.pfid;
   if ((inode_val_p->fid[0]==0) && (inode_val_p->fid[1]==0))
   {
      pbuf += sprintf(pbuf,"./"); 
   } 
   else
   {
     uuid_unparse(inode_attr_p->s.pfid,buf_fid);
     pbuf += sprintf(pbuf,"./@rozofs_uuid@%s/",buf_fid); 
   } 
   if (exp_metadata_inode_is_del_pending(inode_attr_p->s.attrs.fid))
   {
      /*
      ** get the attributes of the parent
      */
      if (!(plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,&cache, inode_attr_p->s.pfid))) {
	buf[0] = 0;
	return buf;
      } 
      if (!exp_metadata_inode_is_del_pending(plv2->attributes.s.attrs.fid))
      {
        pbuf += sprintf(pbuf,"./%s/",ROZOFS_DIR_TRASH);      
      }
   } 
   /*
   ** get the object name
   */
   name[0] = 0;     
   rozolib_get_fname(inode_attr_p->s.attrs.fid,e,name,&inode_attr_p->s.fname,inode_attr_p->s.pfid);
   if (name[0]== 0)
   {
     uuid_unparse(inode_attr_p->s.attrs.fid,buf_fid);
     pbuf += sprintf(pbuf,"@rozofs_uuid@%s",buf_fid);    
   
   }
   else
   {
     pbuf += sprintf(pbuf,"%s",name);       
   }
   return buf;
}


/*
**__________________________________________________________________
*/
/**
*
    load all the attributes of a tracking file in memory
    
    @param top_hdr_p: pointer to the top table
    @param usr_id: slice of the tracking file
    @param attr_p: pointer to the array where data must be loaded
    @param attr_sz: size of the array

    
    @retval 0 on success
    @retval -1 on error
    
*/
int exp_metadata_read_all_attributes(exp_trck_top_header_t *top_hdr_p,int usr_id,uint64_t file_id,void *attr_p,int attr_sz)
{
   exp_trck_header_memory_t  *main_trck_p;
   char pathname[1024];
   struct stat stats;
   int ret;
   int fd;
   size_t read_size;
   ssize_t returned_size;
   
   
   main_trck_p = top_hdr_p->entry_p[usr_id];
   if (main_trck_p == NULL)
   {
      severe("user_id %d does not exist\n",usr_id);
      errno = ENOENT;
      return -1;
   }
   /*
   ** build the pathname of the tracking file
   */
   sprintf(pathname,"%s/%d/trk_%llu",top_hdr_p->root_path,usr_id,(long long unsigned int)file_id);
   /*
   ** open the file
   */
   if ((fd = open(pathname, O_RDWR , 0640)) < 0)  
   {
     severe("cannot open %s: %s\n",pathname,strerror(errno));
     return -1;
   } 
   /*
   ** get the size of the file
   */
   ret = fstat(fd,&stats);
   if (ret < 0)
   {
     severe("fstat failure for %s:%s\n",pathname,strerror(errno));
     close(fd);
     return -1;
   }
   if (stats.st_size <  sizeof(exp_trck_file_header_t))
   {
      severe("%s file too small %d/%d\n",pathname,sizeof(stats.st_size),sizeof(exp_trck_file_header_t));
      /*
      ** the file is corrupted
      */
      close(fd);    
      return -1; 
   } 
   /*
   ** check it the file is 0K
   */
   read_size =  stats.st_size - sizeof(exp_trck_file_header_t);
   if ((read_size % main_trck_p->max_attributes_sz) != 0)
   {
      severe("metadata file corrupted %s\n",pathname);
   }
   off_t attr_offset = sizeof(exp_trck_file_header_t);
   returned_size = pread(fd,attr_p,read_size, attr_offset);  
   if ( returned_size < 0)
   {
      severe("error while reading %s : %s\n",pathname,strerror(errno));
      close(fd);
      return -1;
   }
   close(fd);
   return 0;
}
/*
**__________________________________________________________________
*/
/**
*
    read attributes associated with an inode from a memory array
    
    @param buffer: buffer where the content of the tracking file has been loaded
    @param index: index of the inode in the array
    @param attr_p: pointer to the attribute array (returned)
    @param attr_sz: size of the attributes

    
    @retval 0 on success
    @retval -1 on error (see errno for details
    
*/
int exp_trck_read_attributes_from_buffer(char *buffer,int index,void *attr_p,int attr_sz)
{
      
   /*
   ** get the real index of the inode within the tracking file
   */
   int attr_offset = index*attr_sz;
   memcpy(attr_p,&buffer[attr_offset],attr_sz);

   return 0; 
}

static inline void stat_to_mattr(struct stat *st, mattr_t * attr) 
{
     attr->mode = st->st_mode;
     attr->size = st->st_size;
     attr->atime = st->st_atime;
     attr->mtime = st->st_mtime;
     attr->ctime = st->st_ctime;
     attr->uid = st->st_uid;
     attr->gid = st->st_gid;
}
/*
**_______________________________________________________________________________
*/
/**
   @param mode: set to 0 to clear the verbose mode
*/
void rz_set_verbose_mode(int mode)
{
    verbose_mode = mode;
}
/*
**_______________________________________________________________________________
*/
/**
*  scan of the inode of a given type:
   
   @param export: pointer to the export context
   @param type: type of the inode to search for
   @param read : assert to one if inode attributes must be read
   @param callback_fct : optional callback function, NULL if none
   @param fd : file descriptor if output must be flushed in a file, -1 otherwise
   @param callback_trk_fct : optional callback function associated with the tracking file, NULL if none
   
   @retval
*/
int rz_scan_all_inodes_from_context(void *export,int type,int read,check_inode_pf_t callback_fct,void *param,
                       check_inode_pf_t callback_trk_fct,void *param_trk,scan_index_context_t *index_ctx_p)
{
   int ret=0;
   export_t *e;
   int user_id;
   int match;
   uint64_t count = 0;
   uint64_t match_count = 0;
   uint64_t file_id;
   rozofs_inode_t inode;
   int i;
   ext_mattr_t  ext_attr;
   exp_trck_top_header_t *inode_metadata_p; 
   struct perf start, stop;  /* statistics */
   int   file_count = 0;
   uint8_t *metadata_buf_p = NULL;
   struct stat stat;
   int start_with_context = 0;
   
   rozo_lib_stop_var = 0;
   e = export;
   
    inode_metadata_p = e->trk_tb_p->tracking_table[type];
    if (index_ctx_p != NULL)
    {
       rozo_lib_current_user_id = index_ctx_p->user_id;
       rozo_lib_current_file_id = index_ctx_p->file_id;
       rozo_lib_current_inode_idx = index_ctx_p->inode_idx;
       start_with_context = 1;
    
    }
    /*
    ** allocate memory to store the metadata
    */
    int alloc_size = inode_metadata_p->max_attributes_sz*EXP_TRCK_MAX_INODE_PER_FILE;
    metadata_buf_p = malloc(alloc_size);
    if (metadata_buf_p == NULL)
    {
      severe("Out of memory");
      printf("Out of memory: cannot allocate %d\n",alloc_size);
      exit(-1);
    }
    perf_start(&start);
   
   /*
   ** go through  all the slices of the export
   */
   for (user_id = rozo_lib_current_user_id; user_id < EXP_TRCK_MAX_USER_ID; user_id++,rozo_lib_current_user_id++)
   {
     inode.s.usr_id = user_id;
     if (start_with_context)
     {
       file_id = inode_metadata_p->entry_p[user_id]->entry.first_idx;
       if (file_id >= rozo_lib_current_file_id) rozo_lib_current_file_id = file_id;
       else file_id = rozo_lib_current_file_id;
       start_with_context = 0;
        
     }
     else
     {
       file_id = inode_metadata_p->entry_p[user_id]->entry.first_idx;
       rozo_lib_current_file_id = file_id;
       rozo_lib_current_inode_idx = 0;
     }
     /*
     ** get the information related to the main tracking file: that file contains the
     ** first and last attribute file indexes
     */     
     for (;file_id <= inode_metadata_p->entry_p[user_id]->entry.last_idx;file_id++,rozo_lib_current_file_id++)
     {

//         printf("user_id %d file_id %d \n",user_id,file_id);
         file_count+=1;

	 ret = exp_metadata_get_tracking_file_header(inode_metadata_p,user_id,file_id,&tracking_buffer,NULL);
	 if (ret < 0)
	 {
	   if (errno != ENOENT)
	   {
              severe("EXIT error while reading metadata header %s",strerror(errno));
              printf("error while reading metadata header %s\n",strerror(errno));
	      exit(-1);
	   }
	   continue;
	 }
         
	 if (callback_trk_fct)
	 {
	    /*
	    ** get the stat information of the tracking file
	    */
	    exp_metadata_get_tracking_file_stat(inode_metadata_p,user_id,file_id,&stat);
	    stat_to_mattr(&stat,&ext_attr.s.attrs);
            /*
            ** get creation time from tracking file header
            */
            ext_attr.s.cr8time = tracking_buffer.creation_time;
	    match = (*callback_trk_fct)(e,&ext_attr,param_trk);
	    if (match == 0) 
	    {
	      continue;
	    }
	 }

         /*
	 ** load the content of the tracking file in memory
	 */
	 ret = exp_metadata_read_all_attributes(inode_metadata_p,user_id,file_id,metadata_buf_p,0);
	 if (ret < 0)
	 {
            printf("error while reading metadata file %s\n",strerror(errno));
	    continue;	   
	 }
         /*
	 ** update the number of objects
	 */
	 count +=exp_metadata_get_tracking_file_count(&tracking_buffer);
	 inode.s.file_id = file_id;
	 if (read)
	 {
	   for (i = rozo_lib_current_inode_idx; i < EXP_TRCK_MAX_INODE_PER_FILE; i++)
	   {
              inode.s.idx = i;
	      inode.s.key = type;
	      rozo_lib_current_inode_idx = i+1;

	      if (tracking_buffer.inode_idx_table[i] == 0xffff) continue;
              if (tracking_buffer.inode_idx_table[i] > i) {
		printf("error in tracking file slice %d trck_%d for index %d : %d\n",
                        (int)inode.s.usr_id, (int)inode.s.file_id, (int)inode.s.idx,
                        (int)tracking_buffer.inode_idx_table[i]);
                continue;        
              }
	      ret = exp_trck_read_attributes_from_buffer((char*)metadata_buf_p,tracking_buffer.inode_idx_table[i],&ext_attr,sizeof(ext_attr));
	      if (ret < 0)
	      {
		printf("error while reading attributes %d:%llu:%d\n",inode.s.usr_id,
	        	(long long unsigned int)inode.s.file_id,inode.s.idx);
	      }
#if 0
	      sprintf(parent_name,".");
              if (uuid_compare(e->rfid,ext_attr.s.pfid)!= 0)
	      {
	        uuid_unparse(ext_attr.s.pfid,bufout);
	        sprintf(parent_name,"./@rozofs_uuid@%s",bufout);
	      }
#endif
//	      printf("usr_id %3.3d %8.8llu  %s/%s\n",user_id,ext_attr.s.attrs.size,parent_name,ext_attr.s.name);
//	      printf("usr_id:%3.3d file_id:%8.8llu idx:%4.4d key:%d \n",user_id,file_id,i,type);
//              memset(fid,0,sizeof(fid_t));
//	      buf64 = (uint64_t*)fid;
//	      buf64[1] = inode.fid[1];
//	      uuid_unparse(ext_attr.s.attrs.fid,bufout);
//	      printf("inode @rozofs@%s\n",bufout);
//	      uuid_unparse(fid,bufout);
//	      printf("inode @rozofs@%s\n",bufout);
              /*
	      ** check if the fid has been recycled
	      */
	      if (ext_attr.s.cr8time == 0) continue;
              match = 0;
              if (callback_fct != NULL) 
	      {
	         match = (*callback_fct)(e,&ext_attr,param);
	         if (match) 
		 {
		   match_count++;
		  // sprintf(bufall,"%s/%s\n",parent_name,get_fname(e,child_name,&ext_attr.s.fname,ext_attr.s.pfid));
                  // printf("%s",bufall);
		 }
		 /*
		 ** Check if you should stop the scanning of the inode
		 */
		 if (rozo_lib_stop_var) goto out;
	      }
	      else
	      {
	         match = 1;
	      }
	   }	   
	   rozo_lib_current_inode_idx = 0;
	 }
     }   
   }
//   printf("type %d\n",type);
out:
   if (verbose_mode)
   {
      perf_stop(&stop);
      perf_print(stop,start,(unsigned long long)(file_count));
      perf_print(stop,start,(unsigned long long)(count));

      printf("match_count/count %llu/%llu\n",(long long unsigned int)match_count,(long long unsigned int)count);

   } 
  /*
   ** release the metadata buffer
   */
   if (metadata_buf_p != NULL) free(metadata_buf_p);
   return ret;

}
/*
**_______________________________________________________________________________
*/
/**
*  scan of the inode of a given type:
   
   @param export: pointer to the export context
   @param type: type of the inode to search for
   @param read : assert to one if inode attributes must be read
   @param callback_fct : optional callback function, NULL if none
   @param fd : file descriptor if output must be flushed in a file, -1 otherwise
   @param callback_trk_fct : optional callback function associated with the tracking file, NULL if none
   
   @retval
*/
int rz_scan_all_inodes(void *export,int type,int read,check_inode_pf_t callback_fct,void *param,
                       check_inode_pf_t callback_trk_fct,void *param_trk)
{

   rozo_lib_current_file_id = 0;
   rozo_lib_current_user_id = 0;
   rozo_lib_current_inode_idx = 0;
   return  rz_scan_all_inodes_from_context(export,type,read,callback_fct,param,
                       callback_trk_fct,param_trk,NULL);

}	
/*
**_______________________________________________________________________________
*/
/**
*  API to get the pathname of the objet: @rozofs_uuid@<FID_parent>/<child_name>

   @param export : pointer to the export structure
   @param inode_attr_p : pointer to the inode attribute
   @param buf: output buffer
   
   @retval buf: pointer to the beginning of the outbuffer
*/
static char buf_uuid[]="./@rozofs_uuid@";
static int buf_uuid_len = -1;
static int buf_dir_len = -1;
char *rozo_get_parent_child_path(void *exportd,void *inode_p,char *buf)
{

   int offset;
   ext_mattr_t *inode_attr_p = inode_p;
   
   export_t *e= exportd;
   
   buf[0] = 0;
   
    if (memcmp(e->rfid,inode_attr_p->s.pfid,sizeof(fid_t))!= 0)
    {
      if (buf_uuid_len == -1) buf_uuid_len =strlen(buf_uuid);
      memcpy(buf,buf_uuid,buf_uuid_len);
      rozofs_uuid_unparse(inode_attr_p->s.pfid,&buf[buf_uuid_len]);
      if (buf_dir_len == -1) buf_dir_len = strlen(buf);
      offset = buf_dir_len;
    }
    else
    {
      buf[0]='.';
      buf[1]=0; 
      offset= 1;   
    
    }
      buf[offset]='/';
      rozolib_get_fname(inode_attr_p->s.attrs.fid,e,&buf[offset+1],&inode_attr_p->s.fname,inode_attr_p->s.pfid);
    return buf;
}
	       
 /*
 **___________________________________________________________________________
 **
      INIT
 **___________________________________________________________________________
*/  
/** constants of the export */
typedef struct export_const {
    char version[20]; ///< rozofs version
    fid_t rfid; ///< root id
} export_const_t;

/**
   rozo inode lib initialization

    @param  : pointer to the root path of the exportd under analysis
    
    @retval <> NULL export reference to call for any operation
    @retval == NULL error
*/
void *rz_inode_lib_init(char *root_path)
{
   export_const_t export_const_file;
   char const_path[1024];
   int fd=-1;
   char root_export_host_id[PATH_MAX];

  /*
  ** init of the layout variables
  */
  rozofs_layout_initialize();
  /*
  ** allocate a fake export
  */
  fake_export_p = malloc(sizeof(export_t));
  if (fake_export_p == NULL)
  {
     printf("error on fake export allocation: %s\n",strerror(errno));
     return NULL;  
  }
  memset(fake_export_p,0,sizeof(export_t));
  /*
  ** init of dirent stuff
  */
//  dirent_cache_level0_initialize();
//  dirent_wbcache_init();
  dirent_wbcache_disable();
  /*
  ** fill up the export structure
  */
  if (!realpath(root_path, fake_export_p->root))
  {
      printf("realpath failure for %s : %s",root_path,strerror(errno));
      return NULL;
  }
  /*
  ** read the file that contains the version and the root fid
  */
  sprintf(const_path, "%s/%s", root_path, CONST_FNAME);
  if ((fd = open(const_path, O_RDWR, S_IRWXU)) < 1) {
      severe("open failure for %s : %s",const_path,strerror(errno));
      return NULL;
  }

  if (read(fd, &export_const_file, sizeof (export_const_t)) != sizeof (export_const_t)) {
      close(fd);
      printf("error while reading %s :%s",const_path,strerror(errno));
      return NULL;
  }
  /*
  ** copy the root fid
  */
  memcpy(fake_export_p->rfid,export_const_file.rfid,sizeof(fid_t));
  close(fd);
  
  memset(&fake_level2_entry,0,sizeof(fake_level2_entry));
  /*
  ** create the tracking context
  */
  sprintf(root_export_host_id,"%s/host%d",root_path,rozofs_get_export_host_id());
#warning Workaround for scaning several exports 
  if (export_tracking_table[0]!= NULL) {
    free(export_tracking_table[0]);
    export_tracking_table[0] = NULL;
  }
  
  fake_export_p->trk_tb_p = exp_create_attributes_tracking_context(fake_export_p->eid,root_export_host_id,0);
  if (fake_export_p->trk_tb_p == NULL)
  {
     printf("error on tracking context allocation: %s\n",strerror(errno));
     return NULL;
  }
  return fake_export_p;

}

/*
**__________________________________________________
*/
/**
   Get the rozoFS attributes in the struct stat form
   
   @param inode_attr_p: pointer to the rozoFS attributes
   @param st : pointer to the struct stat array
   
   @retval 0 on success
   @retval < 0 on error
*
*/
int rozofs_fstat(void *inode_p,struct stat *st)
{
    mattr_t * attr;
    uint32_t bsize = 4096;    
    ext_mattr_t *inode_attr_p = inode_p;
    memset(st, 0, sizeof (struct stat));
    
    attr = &inode_attr_p->s.attrs;

    st->st_mode = attr->mode;
    st->st_nlink = attr->nlink;
    st->st_size = attr->size;
    st->st_ctime = attr->ctime;
    st->st_atime = attr->atime;
    st->st_mtime = attr->mtime;
    st->st_blksize = ROZOFS_BSIZE_BYTES(bsize);
    st->st_blocks = ((attr->size + 512 - 1) / 512);
    st->st_dev = 0;
    st->st_uid = attr->uid;
    st->st_gid = attr->gid;
    return 0;
}

/**
*  Get the file distribution (cid and sids)

   @param inode_p : pointer to the inode
   @param p: file distribution
   
   @retval none
*/
void rozofs_get_file_distribution(void *inode_p,rozofs_file_distribution_t *p)
{
   int i;
   ext_mattr_t *inode_attr_p = inode_p;
   
   p->cid = inode_attr_p->s.attrs.cid;
   for (i = 0; i < ROZOFS_SAFE_MAX; i++)
   {
     p->sids[i] = inode_attr_p->s.attrs.sids[i];
   }
}


/*
**__________________________________________________
*/
/**
   Release the memory allocated to deal with an exportd
   
   @param none
   
   @retval 0 on success
   @retval < 0 on error
*
*/
int rozo_lib_export_release()
{
   int i,k;   
   export_tracking_table_t *trk_tb_p;
   exp_trck_top_header_t *tracking_table_p;
   exp_trck_header_memory_t *entry_p;
   
   if (fake_export_p == NULL) return 0;
   trk_tb_p = fake_export_p->trk_tb_p;
   if ( fake_export_p->trk_tb_p == NULL)return 0;
   
   
   for (i=0; i < ROZOFS_MAXATTR; i++)
   {
      tracking_table_p = trk_tb_p->tracking_table[i];
      if (tracking_table_p == NULL) continue;
      trk_tb_p->tracking_table[i] = NULL;
      for (k= 0; k < EXP_TRCK_MAX_USER_ID;k++)
      {
        entry_p = tracking_table_p->entry_p[k];
        if (entry_p == NULL) continue;
	if (entry_p->tracking_file_hdr_p != NULL) EXP_TRK_FREE(entry_p->tracking_file_hdr_p);
	EXP_TRK_FREE(entry_p);
      }
      if (tracking_table_p->trck_inode_p != NULL) free(tracking_table_p->trck_inode_p);
      EXP_TRK_FREE(tracking_table_p);
   }

   /*
   ** free the fake exportd
   */
   free(fake_export_p);
   fake_export_p = NULL;
   /*
   ** free the tracking table
   */   
   if (export_tracking_table[0]!= NULL) {
    free(export_tracking_table[0]);
    export_tracking_table[0] = NULL;
  }
   return 0;	
}
