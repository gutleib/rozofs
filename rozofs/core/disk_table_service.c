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
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <malloc.h>
#include "disk_table_service.h"



/*
**__________________________________________________________________
*/
/**
*  build a fullpathname

   @param ctx_p : needed to extract the root path and the basename path
   @param fullpath: pointer to the buffer that contains the full pathname
   @param name : pointer to the name to append, NULL if there is no name to append
   @param file_idx : index of the file to append to the ful pathname, -1 if there is nothing to append
   
   @retval pointer to the full pathname
*/
static inline char  *disk_tb_build_fullpathname(disk_table_header_t *ctx_p,char *fullpath,int file_idx,char *name)
{
   char *buf = fullpath;
   buf +=sprintf(buf,"%s/%s",ctx_p->root_path,ctx_p->basename);
   if (name !=NULL)
   {
     buf +=sprintf(buf,"_%s",name);   
   }
   if (file_idx != -1)
   {
     buf +=sprintf(buf,"_%d",file_idx);      
   }
   return fullpath;
}

/*
 **__________________________________________________________________
 */
/**
* service to check if the bitmap for file_idx must be loaded

    @param ctx_p : pointer to the table context

  
  @retval 0 on success
  @retval < 0 on error
*/

int disk_tb_load_bitmap_file(disk_table_header_t *ctx_p)
{
   int fd = -1;
   disk_tb_bitmap_file_t *bitmap_p;
   char fullpath[1024];

   disk_tb_build_fullpathname(ctx_p,fullpath,-1,"btmap");
   /*
   ** allocate the memory
   */
   ctx_p->file_btmap_p = memalign(32,sizeof(disk_tb_bitmap_file_t)+ctx_p->bitmap_size-1);
   if (ctx_p->file_btmap_p == NULL) {
     severe("out of memory");
     return -1;
   }
   bitmap_p = (disk_tb_bitmap_file_t*)ctx_p->file_btmap_p;
   /*
   ** Clear bitmap before reading from disk
   */
   memset(bitmap_p->bitmap,0,ctx_p->bitmap_size);
   bitmap_p->dirty = 0;
   /*
   ** read the bitmap from disk
   */    
   if ((fd = open(fullpath, O_RDONLY | O_NOATIME, S_IRWXU)) < 0) 
   {
     if (errno == ENOENT)
     {
       /*
       ** do not complain, just clear the buffer
       */
       warning("File %s does not yet exist", fullpath);        
       return 0;     
     }
     severe("open(%s) %s", fullpath, strerror(errno));        
     return -1;     
   }
   
   ssize_t len = pread(fd,bitmap_p->bitmap,ctx_p->bitmap_size,0);
   if (len != ctx_p->bitmap_size) {
     warning("File %s has size %u instead of %u", fullpath, (int)len, ctx_p->bitmap_size);
   }  
   /*
   ** close the file
   */
   close(fd);
   return 0;
}
/*
**__________________________________________________________________
*/
/**
*   update the file_idx bitmap in memory

   @param ctx_p: pointer to the level2 cache entry
   @param file_idx : file index to update
   @param set : assert to 1 when the file_idx is new/ 0 for removing
   

*/
void disk_tb_file_bitmap_update(disk_table_header_t *ctx_p,int file_idx,int set)
{
    uint32_t byte_idx;
    int bit_idx ;
    disk_tb_bitmap_file_t *bitmap_p;
    
    if (ctx_p == NULL) return;
    
    bitmap_p = (disk_tb_bitmap_file_t*)ctx_p->file_btmap_p;
    
    if (file_idx >DISK_MAX_IDX_FOR_FILE_ID(ctx_p)) return;
    
    byte_idx = file_idx/8;
    bit_idx =  file_idx%8;
    if (set)
    {
       if (bitmap_p->bitmap[byte_idx] & (1<<bit_idx)) return;
       bitmap_p->bitmap[byte_idx] |= 1<<bit_idx;    
    }
    else
    {
       if ((bitmap_p->bitmap[byte_idx] & (1<<bit_idx))==0) return;
       bitmap_p->bitmap[byte_idx] &=~(1<<bit_idx);        
    }
    bitmap_p->dirty = 1;
}
/*
**__________________________________________________________________
*/
/**
*   check the presence of a file_idx  in the bitmap 

   @param ctx_p: pointer to the level2 cache entry
   @param file_idx : root index to update

  @retval 1 asserted
  @retval 0 not set   

*/
int disk_tb_check_file_idx_bit(disk_table_header_t *ctx_p,int file_idx)
{
    uint32_t byte_idx;
    int bit_idx ;
    disk_tb_bitmap_file_t *bitmap_p;
    
    
    bitmap_p = (disk_tb_bitmap_file_t*)ctx_p->file_btmap_p;
    if (file_idx >DISK_MAX_IDX_FOR_FILE_ID(ctx_p)) return 1;
    
    byte_idx = file_idx/8;
    bit_idx =  file_idx%8;

    if (bitmap_p->bitmap[byte_idx] & (1<<bit_idx)) 
    {
      return 1;
    }
    return 0;
}
/*
**__________________________________________________________________
*/
/**
* service to flush on disk the file_idx bitmap if it is dirty

  @param bitmap_p : pointer to the file_idx bitmap
  @param fid : file id of the directory
  @param e:   pointer to the export structure
  
  @retval 0 on success
  @retval < 0 on error
*/

int disk_tb_bitmap_flush(disk_table_header_t *ctx_p)
{
   int fd = -1;
   char fullpath[1024];
   disk_tb_bitmap_file_t *bitmap_p;
    
    
   bitmap_p = (disk_tb_bitmap_file_t*)ctx_p->file_btmap_p;

   if (bitmap_p->dirty == 0) return 0;
   /*
   ** bitmap has changed :write the bitmap on disk
   */    
   disk_tb_build_fullpathname(ctx_p,fullpath,-1,"btmap");

   if ((fd = open(fullpath, O_WRONLY | O_CREAT | O_NOATIME, S_IRWXU)) < 0) {
        goto error;
   }
   ssize_t len = pwrite(fd,bitmap_p->bitmap,ctx_p->bitmap_size,0);
   if (len != ctx_p->bitmap_size) goto error;
   /*
   ** clear the dirty bit
   */
   bitmap_p->dirty = 0;
   /*
   ** close the file
   */
   close(fd);
   return 0;
   
error:
   if (fd != -1) close(fd);
   return -1;
}

/*
**__________________________________________________________________
*/
/*
**  read one entry of the table

    @param ctx_p : pointer to the table context
    @param entry_id : index of the entry to search
    @param data_p : pointer to the data buffer that will contain the data associated with entry_id
    
    @retval > 0 : length of the data
    @retal  = 0 : no data, the entry does not exist
    @retval < 0 : error, see errno for details
    
*/
int disk_tb_read_entry(disk_table_header_t *ctx_p,uint32_t entry_id,void *data_p)
{
    int entry_file_idx;
    int idx_in_file;
    int ret;
    int fd = -1;
    off_t off;
    ssize_t count;    
    char fullpath[1024];
    uint16_t val16;
    
    /*
    ** get the index of the object within the table
    */
    entry_file_idx = entry_id / DISK_TABLE_MAX_ENTRY_PER_FILE;
    idx_in_file = entry_id % DISK_TABLE_MAX_ENTRY_PER_FILE;
    if (entry_file_idx >= DISK_MAX_IDX_FOR_FILE_ID(ctx_p))
    {
       /*
       ** entry is out of range
       */
       errno = ERANGE;
       return -1;
    }
    /*
    ** Check the presence of the file
    */
    disk_tb_build_fullpathname(ctx_p,fullpath,entry_file_idx,NULL);
    ret = disk_tb_check_file_idx_bit(ctx_p,entry_file_idx);
    if (ret == 0)
    {
       /*
       ** file does not exist
       */
       return 0;
    }
    /*
    ** let's open the file and read the file header
    */
    if ((fd = open(fullpath, O_RDWR , 0640)) < 0)  
    {
       if (errno == ENOENT)
       {
          /*
	  ** update the bitmap
	  */
	  disk_tb_file_bitmap_update(ctx_p,entry_file_idx,0);
	  return 0;
       }
       return -1;
    }
    /*
    ** let's read the header of the file to find out the exact index of the object
    ** within the file
    */
    off = idx_in_file*sizeof(uint16_t);
    count = pread(fd,&val16,sizeof(uint16_t),off);
    if (count != sizeof(uint16_t))
    {
      severe("fail to read table file %s:bad size (%d) expect %d\n",fullpath,(int)count,(int)sizeof(uint16_t));
      errno = EIO;
      close(fd);
      return -1;
    }   
    /*
    ** ok, check the real index of the entry within the file
    */
    if ((val16 == 0xffff) || (val16 & DISK_TB_EMPTY_ENTRY)) 
    {
       /*
       ** no data available
       */
       close(fd);
       return 0;
    }
    /*
    ** no read the data
    */
    off = val16;
    off = off*ctx_p->entry_sz+DISK_TB_HEADER_SZ;
    count = pread(fd,data_p,ctx_p->entry_sz,off);
    if (count != ctx_p->entry_sz)
    {
      severe("fail to read table file %s:bad size (%d) expect %d\n",fullpath,(int)count,(int)ctx_p->entry_sz);
      errno = EIO;
      close(fd);
      return -1;
    }
    close(fd);
    return count;       
}

/*
**__________________________________________________________________
*/
/*
**  write one entry of the table

    @param ctx_p : pointer to the table context
    @param entry_id : index of the entry to search
    @param data_p : pointer to the data buffer that contains the data associated with entry_id
    
    @retval > 0 : length of the data
    @retal  = 0 : no data, the entry does not exist
    @retval < 0 : error, see errno for details
    
*/
int disk_tb_write_entry(disk_table_header_t *ctx_p,uint32_t entry_id,void *data_p)
{
    int entry_file_idx;
    int idx_in_file;
    int ret;
    int fd = -1;
    off_t off;
    ssize_t count;    
    char fullpath[1024];
    uint16_t val16;
    int flags = O_RDWR;
    
    /*
    ** get the index of the object within the table
    */
    entry_file_idx = entry_id / DISK_TABLE_MAX_ENTRY_PER_FILE;
    idx_in_file = entry_id % DISK_TABLE_MAX_ENTRY_PER_FILE;
    if (entry_file_idx >= DISK_MAX_IDX_FOR_FILE_ID(ctx_p))
    {
       /*
       ** entry is out of range
       */
       errno = ERANGE;
       return -1;
    }
    /*
    ** Check the presence of the file
    */
    disk_tb_build_fullpathname(ctx_p,fullpath,entry_file_idx,NULL);
recreate:
    ret = disk_tb_check_file_idx_bit(ctx_p,entry_file_idx);
    if (ret == 0)
    {
       /*
       ** file does not exist
       */
       flags |= O_CREAT;
       if ((fd = open(fullpath,flags, 0640)) < 0)  
       {
	  return -1;
       }
       /**
       *   write an empty bitmap
       */
       uint16_t *bufall = memalign(32,DISK_TB_HEADER_SZ);
       if (bufall == NULL)
       {
          errno = ENOMEM;
	  severe("Out of memory while writing entry for table %s\n",ctx_p->basename);
	  return -1;
       }
       memset(bufall,0xff,DISK_TB_HEADER_SZ);
       /*
       ** allocate the first entry
       */
       bufall[idx_in_file] = 0;
       count = pwrite(fd,bufall,DISK_TB_HEADER_SZ,0);
       if (count != DISK_TB_HEADER_SZ)
       {
         severe("fail to write table file %s:bad size (%d) expect %d\n",fullpath,(int)count,(int)DISK_TB_HEADER_SZ);
	 close(fd);
	 free(bufall);
	 errno = EIO;
	 return -1;
       } 
       free(bufall);  
       /*
       ** now write the data at the relative idx in the file
       */   
       count = pwrite(fd,data_p,ctx_p->entry_sz,DISK_TB_HEADER_SZ);
       if (count != ctx_p->entry_sz)
       {
         severe("fail to write table file entry %s:bad size (%d) expect %d\n",fullpath,(int)count,(int)ctx_p->entry_sz);
	 close(fd);
	 errno = EIO;
	 return -1;
       } 
       close(fd);
       /*
       ** flush the bitmap if there is a change
       */
       disk_tb_file_bitmap_update(ctx_p,entry_file_idx,1);
       disk_tb_bitmap_flush(ctx_p);
       return count;
    }
    /*
    ** let's open the file and read the file header
    */
    if ((fd = open(fullpath,flags, 0640)) < 0)  
    {
       if (errno == ENOENT)
       {
          /*
	  ** update the bitmap
	  */
	  disk_tb_file_bitmap_update(ctx_p,entry_file_idx,0);
	  severe("disk table %s:bitmap mismatch for file_id %d\n",fullpath,entry_file_idx);
	  /*
	  ** let's attempt to re-create the file
	  */
	  goto recreate;
       }
       return -1;
    }
    /*
    ** let's read the header of the file to find out the exact index of the object
    ** within the file
    */
    off = idx_in_file*sizeof(uint16_t);
    count = pread(fd,&val16,sizeof(uint16_t),off);
    if (count != sizeof(uint16_t))
    {
      severe("fail to read table file %s:bad size (%d) expect %d\n",fullpath,(int)count,(int)sizeof(uint16_t));
      errno = EIO;
      close(fd);
      return -1;
    }   
    /*
    ** check if the entry has already been allocated
    */
    if (val16 == 0xffff)
    {
       /*
       ** the entry is empty so append the entry at the end of the file
       */
       struct stat stats;
       ret = fstat(fd,&stats);
       if (ret < 0)
       {
	 severe("fstat failure for %s:%s\n",fullpath,strerror(errno));
	 close(fd);
	 return -1;
       }	  
       if (stats.st_size <  DISK_TB_HEADER_SZ)
       {
         severe("bad disk table file size %s : %d",fullpath,(int)stats.st_size);
	 close(fd);
	 return -1;
       }
       uint64_t my_size = stats.st_size - DISK_TB_HEADER_SZ;
       int last_entry = my_size/ctx_p->entry_sz;
       if (my_size%ctx_p->entry_sz)
       {
          severe("bad disk table file size %s: %d modulo %d  ",fullpath,(int)stats.st_size, (int)my_size%ctx_p->entry_sz);
	  close(fd);
	  return -1;              
       }
       /*
       ** check if the entry is in the right range
       */
       if (last_entry >= DISK_TABLE_MAX_ENTRY_PER_FILE)
       {
          severe("bad disk table file %s size %d: %d index out of range  ",fullpath,(int)stats.st_size, last_entry);
	  close(fd);
	  return -1;                    
       }
       /*
       ** update the entry in the header
       */
       val16 = (uint16_t)last_entry;
       off = idx_in_file*sizeof(uint16_t);
       count = pwrite(fd,&val16,sizeof(uint16_t),off);
       if (count != sizeof(uint16_t))
       {
         severe("fail to write table file %s:bad size (%d) expect %d\n",fullpath,(int)count,(int)sizeof(uint16_t));
	 close(fd);
	 errno = EIO;
	 return -1;
       } 
       /*
       ** write the data on disk
       */
       off = last_entry;
       off = off*ctx_p->entry_sz+DISK_TB_HEADER_SZ;       
       count = pwrite(fd,data_p,ctx_p->entry_sz,off);
       if (count != ctx_p->entry_sz)
       {
	 severe("fail to write table file %s:bad size (%d) expect %d\n",fullpath,(int)count,(int)ctx_p->entry_sz);
	 errno = EIO;
	 close(fd);
	 return -1;
       }       
       close(fd);
       return count;
    }
    /*
    ** check if the entry has been release
    */
    if (val16 & DISK_TB_EMPTY_ENTRY)
    {
       /*
       ** need to re-allocate the entry by clearing the empty bit
       */
       val16 &=(~ DISK_TB_EMPTY_ENTRY);
       count = pwrite(fd,&val16,sizeof(uint16_t),off);
       if (count != DISK_TB_HEADER_SZ)
       {
         severe("fail to write table file %s:bad size (%d) expect %d\n",fullpath,(int)count,(int)sizeof(uint16_t));
	 close(fd);
	 errno = EIO;
	 return -1;
       }     
    }
    /*
    ** no read the data
    */
    off = val16;
    off = off*ctx_p->entry_sz+DISK_TB_HEADER_SZ;
    count = pwrite(fd,data_p,ctx_p->entry_sz,off);
    if (count != ctx_p->entry_sz)
    {
      severe("fail to write table file %s:bad size (%d) expect %d\n",fullpath,(int)count,(int)ctx_p->entry_sz);
      errno = EIO;
      close(fd);
      return -1;
    }
    close(fd);

    return count;       
}

/*
**__________________________________________________________________
*/
/**
*  release a disk table context
  @param ctx_p: pointer to the context to release
  
  @retval none
*/
void disk_tb_ctx_release(disk_table_header_t *ctx_p)
{


   if (ctx_p->basename != NULL) free(ctx_p->basename);
   if (ctx_p->root_path != NULL) free(ctx_p->root_path);   
   if (ctx_p->file_btmap_p != NULL) free(ctx_p->file_btmap_p);   
   
   free( ctx_p);
}

/*
**__________________________________________________________________
*/
/**
*  allocate a disk table context
  @param root: root_path
  @param name: name of the table
  @param entry_sz : size of a record
  @param bitmap_sz_powerof2 : bitmap file size in power of 2
  
  @retval <> NULL pointer to the allocated context
  @retval == NULL error (see errno for details)
*/
disk_table_header_t *disk_tb_ctx_allocate(char *root_path,char *name,int entry_sz,int bitmap_sz_powerof2)
{

   disk_table_header_t *ctx_p = NULL;
   
   ctx_p = memalign(32,sizeof(disk_table_header_t));
   if (ctx_p == NULL)
   {
      /*
      ** out of memory
      */
      return NULL;
   }
   memset(ctx_p,0,sizeof(disk_table_header_t));
   ctx_p->basename = strdup(name);
   if (ctx_p->basename == NULL) goto error;

   ctx_p->root_path = strdup(root_path);
   if (ctx_p->root_path == NULL) goto error; 
   
   ctx_p->entry_sz = entry_sz;  
   ctx_p->bitmap_size = 1<<bitmap_sz_powerof2;
   
   if (disk_tb_load_bitmap_file(ctx_p) < 0) goto error;
   
   return ctx_p;
   
error:
   disk_tb_ctx_release(ctx_p);
   return NULL;

}

/*
**__________________________________________________________________
*/
/*
**  read one entry of the table

    @param ctx_p : pointer to the table context
    @param entry_id : index of the entry to search
    @param data_p : pointer to the data buffer that will contain the data associated with entry_id
    
    @retval > 0 : length of the data
    @retal  = 0 : no data, the entry does not exist
    @retval < 0 : error, see errno for details
    
*/
int disk_tb_get_next_file_entry(disk_table_header_t *ctx_p,uint32_t *entry_file_idx_next_p)
{
    int entry_file_idx;
    int fd = -1;
    char fullpath[1024];
    int ret;
    
    entry_file_idx = *entry_file_idx_next_p;
    
    /*
    ** get the index of the object within the table
    */
    while(entry_file_idx < DISK_MAX_IDX_FOR_FILE_ID(ctx_p))
    {    
      /*
      ** Check the presence of the file
      */
      ret = disk_tb_check_file_idx_bit(ctx_p,entry_file_idx);
      if (ret == 0)
      {
	 /*
	 ** file does not exist: check next 
	 */
	 entry_file_idx++;
	 continue;
      }
      /*
      ** let's open the file and read the file header
      */
      disk_tb_build_fullpathname(ctx_p,fullpath,entry_file_idx,NULL);
      if ((fd = open(fullpath, O_RDWR , 0640)) < 0)  
      {
	 if (errno == ENOENT)
	 {
            /*
	    ** update the bitmap
	    */
	    disk_tb_file_bitmap_update(ctx_p,entry_file_idx,0);
            entry_file_idx++;

	    continue;
	 }
	 entry_file_idx++;
	 continue;
      }
      *entry_file_idx_next_p = entry_file_idx+1;
      return entry_file_idx;    
    }
    return -1;
}

/*
**__________________________________________________________________
*/    
/**
*   Open the table file given as input argument and return the current records
    within the file
    
    @param ctx_p : pointer to the table context
    @param file_idx : index of the file to open
    @param fd_p : pointer to the file descriptor
    
    @retval < 0 error
    @retval > 0 number of records in the file (fd_p contains the reference of the file descriptor
    @retval = 0 no records (file is closed)
*/   
int  disk_tb_get_nb_records(disk_table_header_t *ctx_p,int entry_file_idx,int *fd_p)
{
    char fullpath[1024];
    int fd;
    int ret;
    
    *fd_p = -1;

      disk_tb_build_fullpathname(ctx_p,fullpath,entry_file_idx,NULL);
      if ((fd = open(fullpath, O_RDWR , 0640)) < 0)  
      {
	 if (errno == ENOENT)
	 {
            /*
	    ** update the bitmap
	    */
	    disk_tb_file_bitmap_update(ctx_p,entry_file_idx,0);
	    return -1;
	 }
	 return -1;
      }
      /*
      ** get the file stats
      */
       struct stat stats;
       ret = fstat(fd,&stats);
       if (ret < 0)
       {
	 close(fd);
	 return -1;
       }	  
       if (stats.st_size <  DISK_TB_HEADER_SZ)
       {
	 close(fd);
	 return -1;
       }
       uint64_t my_size = stats.st_size - DISK_TB_HEADER_SZ;
       if (my_size%ctx_p->entry_sz)
       {
	  close(fd);
	  return -1;              
       }
       if (my_size/ctx_p->entry_sz !=0)
       {
          *fd_p = fd;
       }
       else close(fd);
       return (my_size/ctx_p->entry_sz);         
}    

/*
**__________________________________________________________________
*/    
/**
*   Open the table file given as input argument and return the current records
    within the file
    
    @param ctx_p : pointer to the table context
    @param file_idx : index of the file to open
    @param fd_p : pointer to the file descriptor
    
    @retval < 0 error (see errno for details)
    @retval > 0 size of the read data
    @retval = 0 end of file
*/    
int  disk_tb_get_next_record(disk_table_header_t *ctx_p,int record_id,int fd,void *data_p)
{
    off_t off;
    ssize_t count;    
    /*
    ** no read the data
    */
    off = record_id;
    off = off*ctx_p->entry_sz+DISK_TB_HEADER_SZ;
    count = pread(fd,data_p,ctx_p->entry_sz,off);
    if (count == 0) return 0;
    if (count != ctx_p->entry_sz)
    {
      errno = EIO;
      return -1;
    }
    return count;       
}

