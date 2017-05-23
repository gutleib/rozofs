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
#include "export_track.h"
#include <malloc.h>
#include "export_track_change.h"


//static char pathname[1024];
int64_t exp_trk_malloc_size;
int open_count;
int close_count;
#define CLOSE_CONTROL(val) /*{ if(close_count >= open_count) {printf("bad_close %d\n",val);} else {close_count++;}};*/


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
int exp_trck_write_main_tracking_file(char * root_path,uint8_t user_id,off_t offset,size_t size,void *data_p)
{
   ssize_t count;
   int success_count = 0;
   int i;
   int fd[EXP_TRCK_MAIN_REPLICA_COUNT];
   char pathname[1024];
   
   for (i = 0; i < EXP_TRCK_MAIN_REPLICA_COUNT; i++) fd[i] = -1;

   for (i= 0; i < EXP_TRCK_MAIN_REPLICA_COUNT; i++)
   {
     sprintf(pathname,"%s/%d/%s_%d",root_path,user_id,EXP_TRCK_MAIN_FILENAME,i);
     open_count++;
     if ((fd[i] = open(pathname, O_RDWR , 0640)) < 0)  
     {
       severe("cannot open %s:%s\n",pathname,strerror(errno));
      continue;
     }
     success_count++;       
   } 
   if (success_count == 0)
   {
      severe("open fails for all files\n");
      return -1;   
   }
   /*
   ** now write the content of the main tracking file: stop on the first success
   */
   success_count = 0;
   for (i= 0; i < EXP_TRCK_MAIN_REPLICA_COUNT; i++)
   {
     if (fd[i] != -1)
     {
       count = pwrite(fd[i],data_p,size,offset);
       if (count != size)
       {
	 severe("write failure for %s:%s\n",pathname,strerror(errno));
	 close(fd[i]);
	 fd[i] = -1;
	 continue;
       }
       close(fd[i]);
       fd[i] = -1;
       success_count++;
     }        
   }
   if (success_count == 0)
   {
     severe("write fails for all files\n");
     return -1;
   }
   return 0;       
}

/*
**__________________________________________________________________
*/
/**
*  open or create the main tracking file
  
   @param root_path : root pathname of the tracking main file
   @param user_id : index of the directory within the root path 
   @param offset: main tracking file write offset
   @param size: size to write
   @data_p: pointer to the data array to write on disk
   @param create_flag : assert to 1 if the main tracking file must be created
      
   @retval 0 on success
   @retval -1 on error (see errno for details

*/
int exp_trck_open_and_read_main_tracking_file(char * root_path,uint8_t user_id,void *data_p,int create_flag)
{
   ssize_t count;
   int success_count;
   int i;
   int create_request;
//   int repair_count;
   char pathname[1024];
   
   memset(data_p,0,sizeof(exp_trck_header_t));

   int fd[EXP_TRCK_MAIN_REPLICA_COUNT];
   
   for (i = 0; i < EXP_TRCK_MAIN_REPLICA_COUNT; i++) fd[i] = -1;
   
   for (i= 0; i < EXP_TRCK_MAIN_REPLICA_COUNT; i++)
   {
     sprintf(pathname,"%s/%d/%s_%d",root_path,user_id,EXP_TRCK_MAIN_FILENAME,i);
     if (access(pathname, F_OK) == -1) 
     {
      if (errno != ENOENT) 
      {     
	fd[i] = -2; 
      }
      continue;
     }
     fd[i] = 0;     
   }    
   /*
   ** check the presence of the main tracking file
   */
   create_request = 1;
//   repair_count = 0;
   for (i= 0; i < EXP_TRCK_MAIN_REPLICA_COUNT; i++)
   {
     if (fd[i] == 0) 
     {
        create_request = 0;
     }
   }
   if ((create_request) &&(create_flag == 0)) return 0;
//#warning take into account the repair case
   success_count = 0;
   if (create_request)
   {
     /*
     ** clear the content of the main tracking file before writing it to disk
     */
     memset(data_p,0,sizeof(exp_trck_header_t));
     for (i= 0; i < EXP_TRCK_MAIN_REPLICA_COUNT; i++)
     {
       sprintf(pathname,"%s/%d/%s_%d",root_path,user_id,EXP_TRCK_MAIN_FILENAME,i);
       open_count++;
       if ((fd[i] = open(pathname, O_RDWR| O_CREAT , 0640)) < 0)  
       {
	 severe("cannot create %s:%s\n",pathname,strerror(errno));
	continue;
       }
       count = pwrite(fd[i],data_p,sizeof(exp_trck_header_t),0);
       if (count != sizeof(exp_trck_header_t))
       {
	 severe("read failure for %s:%s\n",pathname,strerror(errno));
	 close(fd[i]);
	 fd[i] = -1;
	 continue;
       }
       success_count++;
     }
   }
   else
   {   
     for (i= 0; i < EXP_TRCK_MAIN_REPLICA_COUNT; i++)
     {
       sprintf(pathname,"%s/%d/%s_%d",root_path,user_id,EXP_TRCK_MAIN_FILENAME,i);
       open_count++;
       if ((fd[i] = open(pathname, O_RDWR , 0640)) < 0)  
       {
	 severe("cannot open %s:%s\n",pathname,strerror(errno));
	continue;
       }
       success_count++;       
     } 
   }  
   if (success_count == 0)
   {
      severe("open fails for all files\n");
      return -1;
   
   }
   /*
   ** now read the content of the main tracking file: stop on the first success
   */
   success_count = 0;
   for (i= 0; i < EXP_TRCK_MAIN_REPLICA_COUNT; i++)
   {
     if (fd[i] != -1)
     {
       count = pread(fd[i],data_p,sizeof(exp_trck_header_t),0);
       if (count != sizeof(exp_trck_header_t))
       {
	 severe("read failure for %s (size %d):%s\n",pathname,(int)count,strerror(errno));
	 close(fd[i]);
	 fd[i] = -1;
	 continue;
       }
       success_count = 1;
       close(fd[i]);
       fd[i] = -1;

       break;
     }        
   }
   if (success_count == 0)
   {
     severe("read fails for all files\n");
     return -1;
   }
   for (i= 0; i < EXP_TRCK_MAIN_REPLICA_COUNT; i++)
   {
     if (fd[i] != -1)
     {
       close(fd[i]);
       fd[i] = -1;     
     }
   }
   return 0;       
}

/*
**__________________________________________________________________
*/
/**
*  increment the last idx of  the main tracking file
  
   @param main_trck_p : pointer to the memory structure of the main tracking file
   
   @retval 0 on success
   @retval -1 on error (see errno for details

*/
int exp_trck_increment_main_tracking_file(exp_trck_header_memory_t *main_trck_p)
{

   main_trck_p->entry.last_idx++;
   return exp_trck_write_main_tracking_file(main_trck_p->root_path,main_trck_p->user_id,
                                       sizeof(uint64_t),sizeof(uint64_t),&main_trck_p->entry.last_idx);
      
}

/*
**__________________________________________________________________
*/
/**
*   open or create a tracking file (private api)

   That service is called with the fd associated with the current tracking file is -1

      @param main_trck_p : pointer to the main tracking file header in memory
      
      @retval 0 on success
      @retval < 0 on error see errno for details
*/     
int exp_trck_open_tracking_file(exp_trck_header_memory_t *main_trck_p, int *reloop)
{
   int ret;
   ssize_t count ;
  struct stat buf;
  int i;
  char pathname[1024];
  
  
   if (main_trck_p->index_available)
   {
      errno = EBADF;
      return -1;
   }
   /*
   ** check the presence of the memory buffer used for storing the header
   ** allocate one if none
   */
   if (main_trck_p->tracking_file_hdr_p == NULL)
   {
     main_trck_p->tracking_file_hdr_p = EXP_TRK_MALLOC(sizeof(exp_trck_file_header_t));
     if (main_trck_p->tracking_file_hdr_p == NULL)
     {
        errno = ENOMEM;
	return -1;
     }
     memset(main_trck_p->tracking_file_hdr_p,0xff,sizeof(exp_trck_file_header_t));        
   }
   /*
   ** build the pathname of the tracking file
   */
   sprintf(pathname,"%s/%d/trk_%llu",main_trck_p->root_path,
                               main_trck_p->user_id,
			       (long long unsigned int)main_trck_p->entry.last_idx);
   
  /*
  ** open the tracking file
  */
  if (access(pathname, F_OK) == -1) 
  {
    if (errno == ENOENT) 
    {
      /*
      ** create the tracking file
      */
      open_count++;
      if ((main_trck_p->cur_tracking_file_fd = open(pathname, O_RDWR| O_CREAT  , 0640)) < 0)  
      {
        severe("fail to open tracking file %s:%s\n",pathname,strerror(errno));
	return -1;
      } 
      /**
      *  fill up all the entry assuming that all the entries have already been allocated
      */
      for (i = 0; i < EXP_TRCK_MAX_INODE_PER_FILE ; i++)
      {
        main_trck_p->tracking_file_hdr_p->inode_idx_table[i] = i;
      }      
      /**
      * get the creation time of the file
      */             
      fstat(main_trck_p->cur_tracking_file_fd,&buf);
      main_trck_p->tracking_file_hdr_p->creation_time = (uint64_t)buf.st_atime;
      count = pwrite(main_trck_p->cur_tracking_file_fd,
                     main_trck_p->tracking_file_hdr_p,
		     sizeof(exp_trck_file_header_t),0);
      if (count != sizeof(exp_trck_file_header_t))
      {
        severe("fail to write tracking file %s:bad size (%d) expect %d\n",pathname,(int)count,(int)sizeof(exp_trck_file_header_t));
	close(main_trck_p->cur_tracking_file_fd);
	main_trck_p->cur_tracking_file_fd = -1;
	errno = EIO;
	return -1;
      }
      close(main_trck_p->cur_tracking_file_fd);
      main_trck_p->cur_tracking_file_fd = -1;
      /*
      ** clear all the entries and put the creation time
      */
      memset(main_trck_p->tracking_file_hdr_p,0xff,sizeof(exp_trck_file_header_t)); 
      main_trck_p->tracking_file_hdr_p->creation_time = (uint64_t)buf.st_atime;
      /*
      ** update the next entry to allocated
      */
      main_trck_p->index_available = 1;
      main_trck_p->cur_idx         = 0;
      return 0;
    }
    severe("error of access %s: %s\n",pathname,strerror(errno));
    return -1;
  }
  /*
  ** the file exist so open it and read its header in order to find out where is the 
  ** next index to allocated
  */
  open_count++;
  if ((main_trck_p->cur_tracking_file_fd = open(pathname, O_RDWR , 0640)) < 0)  
  {
    severe("cannot open %s: %s\n",pathname,strerror(errno));
    return -1;
  } 
  /*
  ** get the size of the file to figure out how many entries have been allocated
  */
  ret = fstat(main_trck_p->cur_tracking_file_fd,&buf);
  if (ret < 0)
  {
    /*
    ** fatal error, cannot read inode information of the tracking file
    */
    severe("fstat error on %s: %s\n",pathname,strerror(errno));
    close(main_trck_p->cur_tracking_file_fd);
    main_trck_p->cur_tracking_file_fd = -1;    
    return -1;  
  }
  /**
  * check how many entries have been filled in the tracking file
  */
  int nb_entries;
  if (buf.st_size <  sizeof(exp_trck_file_header_t))
  {
     /*
     ** the file is corrupted
     */
     nb_entries = 0;     
  }
  else
  {
    nb_entries = (buf.st_size - sizeof(exp_trck_file_header_t))/main_trck_p->max_attributes_sz;
    if ((buf.st_size - sizeof(exp_trck_file_header_t))%main_trck_p->max_attributes_sz) nb_entries++; 
  }

  /*
  ** read the header
  */
  count = pread(main_trck_p->cur_tracking_file_fd,
                main_trck_p->tracking_file_hdr_p,
		sizeof(exp_trck_file_header_t),0);
  if (count != sizeof(exp_trck_file_header_t))
  {
    close(main_trck_p->cur_tracking_file_fd);
    main_trck_p->cur_tracking_file_fd = -1;
    if (count == 0)
    {
       /*
       ** the file is empty, so delete it
       */
       unlink(pathname);       
    }
    severe("cannot read %s: %s\n",pathname,strerror(errno));
    return -1;
  } 
  close(main_trck_p->cur_tracking_file_fd);
  main_trck_p->cur_tracking_file_fd = -1;
  /*
  ** search for the first index to allocate
  */
  int empty_found = 0;
  int found_idx = 0;
  /*
  ** do not search for the empty inode since we might have hole in the
  ** inode file. Since we always increment the index of the entry 
  ** without checking the availability we might face the situation
  ** where 2 files share the same inode value
  */
#if 0 
  for (i = 0; i < nb_entries ; i++)
  {
     if (main_trck_p->tracking_file_hdr_p->inode_idx_table[i] == 0xffff)
     {
        if (empty_found == 0) 
	{
	  empty_found = 1;
	  found_idx = i;
	}
	continue;
     }
     empty_found = 0;
  }
#endif
  if ((empty_found == 0) && ( EXP_TRCK_MAX_INODE_PER_FILE == nb_entries))
  {
//     severe("full %d %d %s\n",i,__LINE__,pathname);

     if (main_trck_p->cur_tracking_file_fd != -1)
     {
       close(main_trck_p->cur_tracking_file_fd);
       main_trck_p->cur_tracking_file_fd = -1;
     }
     /*
     ** quite strange, the file is full, so skip it 
     ** and create a new one
     */
     main_trck_p->cur_idx = EXP_TRCK_MAX_INODE_PER_FILE;
     ret = exp_trck_increment_main_tracking_file(main_trck_p);
     if (ret < 0) 
     {
       severe("cannot increment main tracking file\n");
       return -1;
     }
     (*reloop)++;
     if (*reloop > EXP_TRCK_MAX_RECURSE)
     {
        errno = EINVAL;
	severe("too many recursion %s\n",pathname);
	return -1;     
     }
     return exp_trck_open_tracking_file(main_trck_p,reloop);
  }
  /*
  ** update the next entry to allocated
  */
  if (empty_found) main_trck_p->cur_idx = found_idx;
  else  main_trck_p->cur_idx = nb_entries;
  main_trck_p->index_available = 1;
  return 0; 
  
}

/*
**__________________________________________________________________
*/
/**
*  Check if a tracking file is full (private api)

   When the tracking file is full the main tracking file is updated, the 
   current tracking file is closed and a new tracking file is created
   
   @param main_trck_p : pointer to the main tracking file header in memory

  @retval 0 on success
  @retval < 0 on error (see errno for details)
*/
int  exp_trck_check_tracking_file_full(exp_trck_header_memory_t *main_trck_p)
{
   int ret;
   
   if (main_trck_p->cur_idx < EXP_TRCK_MAX_INODE_PER_FILE) return 0;
   /**
   *  reinit the indexes for the next tracking file
   */
   main_trck_p->cur_idx = 0;  
   main_trck_p->index_available = 0;
   /*
   ** update the tracking main tracking file
   */
   ret = exp_trck_increment_main_tracking_file(main_trck_p);
   if (ret < 0) return -1;
 
   return ret;
}

/*
**__________________________________________________________________
*/
/**
*   Get the relative index associated with a inode

   @param fd : file descriptor of the attribut file
   @param root_path : pointer to the root path 
   @param inode : inode of the object

   
   @retval >=0: index of the inode in the attributes file
   @retval < 0 : error (see errno for details
   
*/    
int exp_trck_get_relative_inode_idx(int fd,char *root_path ,rozofs_inode_t *inode)
{
  off_t off;
  ssize_t count;
  rozofs_inode_t fake_idx; 
  uint16_t val16;
 
  
  off = GET_FILE_OFFSET(inode->s.idx);
  count = pread(fd,&val16,sizeof(uint16_t),off);
  if (count != sizeof(uint16_t))
  {
    severe("fail to read tracking file %s:bad size (%d) expect %d\n",root_path,(int)count,(int)sizeof(uint16_t));
    errno = EIO;
    return -1;
  }
  if (val16 == 0xffff)
  {
     errno = ENOENT;
     return -1;
  }
  /*
  ** Check the read index is not corrupted
  */
  if (val16 > inode->s.idx) {
    severe("error in tracking file %s slice %d trck_%d for index %d : %d",
           root_path, (int)inode->s.usr_id, (int)inode->s.file_id, (int)inode->s.idx, val16);  
    errno = ENOENT;
    return -1;       
  }
  fake_idx.s.idx = val16;
  return fake_idx.s.idx;
}

#if 0
/*
**__________________________________________________________________
*/
/**
*  service to get the attribute associated with an inode

   @param main_trck_p : pointer to the main tracking file header in memory
   @param attr_p:  pointer to the attributes
   @param attr_sz: size of the attributes
   @param inode : inode of the object

*/
int exp_trck_get_entry(exp_trck_header_memory_t *main_trck_p,void *attr_p,int attr_sz,rozofs_inode_t inode)
{
   int ret;
   /**
   * check the attributes length
   */
   if (attr_sz > main_trck_p->max_attributes_sz)
   {
     /*
     ** length of the attributes is out of range
     */
     return -1;
   }
   /*
   ** get the real index of the inode within the tracking file
   */
   int real_idx = exp_trck_get_relative_inode_idx(main_trck_p->root_path,inode);
   if (real_idx < 0)
   {
     return -1;
   }
   ret = exp_trck_get_attributes(main_trck_p,attr_p,attr_sz,inode,real_idx);
   return ret;

}
#endif


/*
**__________________________________________________________________
*/
/**
*   release a memory entry associated with a main tracking file

   @param p : context to release
*/
void exp_trck_release_header_memory(exp_trck_header_memory_t *p)
{
   int i;
   
   if (p->tracking_file_hdr_p != NULL) EXP_TRK_FREE(p->tracking_file_hdr_p);
   if ( p->cur_tracking_file_fd != -1) close (p->cur_tracking_file_fd);
   for (i= 0; i < EXP_TRCK_MAIN_REPLICA_COUNT; i++)
   {
     if (p->fd[i] != -1) {
       close(p->fd[i]);
     } 
   }
   EXP_TRK_FREE(p);
}

/*
**__________________________________________________________________
*/
/**
*  allocate a memory entry for a tracking main file associated with a given user_id

    @param top_hdr_p : pointer to the top table
    @param user_id : user id to insert
    @param create_flag : assert to 1 in th emain tracking file must be created
*/
exp_trck_header_memory_t *exp_trck_allocate_header_memory(exp_trck_top_header_t *top_hdr_p,int user_id,int create_flag)
{
   int i;
   exp_trck_header_memory_t *header_memory_p;
   char pathname[1024];
   int ret;
   
   header_memory_p = EXP_TRK_MALLOC(sizeof(exp_trck_header_memory_t));
   if (header_memory_p == NULL)
   {
      severe("Out of memory\n");
      return NULL;
   }
   memset(header_memory_p,0,sizeof(exp_trck_header_memory_t));
   header_memory_p->user_id = user_id;
   header_memory_p->root_path = top_hdr_p->root_path;
   header_memory_p->max_attributes_sz = top_hdr_p->max_attributes_sz;
   header_memory_p->cur_tracking_file_fd = -1;
   for (i= 0; i < EXP_TRCK_MAIN_REPLICA_COUNT; i++)
   {
     header_memory_p->fd[i] = -1;   
   }
   /*
   ** check the existence of the user_id directory
   */
   sprintf(pathname,"%s/%d",header_memory_p->root_path,header_memory_p->user_id);
   if (access(pathname, F_OK) == -1) 
   {
    if (errno == ENOENT) 
    {
      /*
      ** create the directory
      */
      if (mkdir(pathname, S_IRUSR | S_IWUSR | S_IXUSR) != 0)
      goto error; 
     }
     else
     {
       severe("cannot access to %s: %s\n",pathname,strerror(errno));
       goto error;
     }
   }
   /*
   ** read the main tracking file
   */
   ret = exp_trck_open_and_read_main_tracking_file(header_memory_p->root_path,
                                                   header_memory_p->user_id,&header_memory_p->entry,create_flag);
   if (ret < 0) goto error;
   
   return header_memory_p;
   
error:
    exp_trck_release_header_memory(header_memory_p);
    return NULL;

}


/*
**__________________________________________________________________
*/
/**
*
    allocate an inode within a given space
    
    @param top_hdr_p: pointer to the top table
    @param inode: address of the inode
    
    @retval 0 on success
    @retval -1 on error
    
*/
int exp_trck_main_allocate_inode(exp_trck_header_memory_t  *main_trck_p,rozofs_inode_t *inode)
{
   int reloop = 0;
   int ret;

   /**
   *  check the presence of the tracking file
   */
   if (main_trck_p->index_available == 0)
   {
     /*
     ** the file is not opened, so attempt to open the file reference by the last index of the 
     ** of the main tracking file
     */
     ret = exp_trck_open_tracking_file(main_trck_p,&reloop);
     if (ret < 0)
     {
       /**
       * cannot open/create the tracking file,face an issue while reading it or runs out of memory
       */
       return -1;      
     }
   }   
   /*
   ** allocate an entry within the current tracking file
   */
   inode->s.file_id = main_trck_p->entry.last_idx;

   inode->s.idx = main_trck_p->cur_idx;
   main_trck_p->cur_idx++;

   /*
   ** check if the tracking file is full:
   ** when the tracking file is full, the main trk file is updated, the
   ** current tracking file is closed and a new tracking file is created
   ** starting at cur_idx 0
   */
   ret = exp_trck_check_tracking_file_full(main_trck_p);
   return ret;   
}
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
int exp_metadata_allocate_inode(exp_trck_top_header_t *top_hdr_p,rozofs_inode_t *inode,uint16_t key,uint8_t slice)
{

   exp_trck_header_memory_t  *main_trck_p;
    /*
    ** use the slice to get the right entry
    */
    main_trck_p = top_hdr_p->entry_p[slice];
    if (main_trck_p == NULL)
    {
       severe("slice %d does not exist\n",slice);
       errno = ENOENT;
       return -1;
    }
    inode->s.usr_id = slice;
    inode->s.file_id = 0;
    inode->s.idx = 0;
    inode->s.key = key;
    return exp_trck_main_allocate_inode(main_trck_p,inode);
}

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
int exp_metadata_release_inode(exp_trck_top_header_t *top_hdr_p,rozofs_inode_t *inode)
{
   exp_trck_header_memory_t  *main_trck_p;
   ssize_t count;
   int fd= -1;
   off_t off;
   uint16_t  val_reset = 0xffff;  
   char pathname[1024]; 
   
    main_trck_p = top_hdr_p->entry_p[inode->s.usr_id];    
    sprintf(pathname,"%s/%d/trk_%llu",main_trck_p->root_path,inode->s.usr_id,(long long unsigned int)inode->s.file_id);

    if (main_trck_p == NULL)
    {
       severe("slice %d does not exist\n",inode->s.usr_id);
       errno = ENOENT;
       return -1;
    }
#if 0 // useless
    /*
    * check if the inode is in the current tracking file. In that case
    * we need update its header in the current main_trck structure
    */
    if (main_trck_p->entry.last_idx ==inode->s.file_id)
    {
      main_trck_p->tracking_file_hdr_p->inode_idx_table[inode->s.idx]=0xffff;     
      if ((fd = open(pathname, O_RDWR , 0640)) < 0)  
      {
	severe(" open failure :%s: %s\n",pathname,strerror(errno));
	return -1;
      } 
      off = GET_FILE_OFFSET(inode->s.idx);
      count = pwrite(fd,&val_reset,sizeof(uint16_t),off);
      if (count != sizeof(uint16_t))
      {
        severe("fail to write tracking file %s:bad size (%d) expect %d\n",pathname,(int)count,(int)sizeof(uint16_t));
	close(fd);
	main_trck_p->cur_tracking_file_fd = -1;
	errno = EIO;
	return -1;
      }      
      close(fd);
      return 0;
    }
#endif
    /*
    ** read the header of the tracking file referenced by the inode, clear the entry and re-write the header
    */
    open_count++;

    if ((fd = open(pathname, O_RDWR , 0640)) < 0)  
    {
     severe(" open failure :%s:%s\n",pathname,strerror(errno));
      return -1;
    } 
    /**
    * clear the entry on disk
    */
    off = GET_FILE_OFFSET(inode->s.idx);
    count = pwrite(fd,&val_reset,sizeof(uint16_t),off);
    if (count != sizeof(uint16_t))
    {
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
    read or write attributes associated with an inode within a given space (friend API)
    
    @param root_path: root_pathname
    @param inode: address of the inode
    @param attr_p: pointer to the attribute array
    @param attr_sz: size of the attributes
    @param max_attr_sz: size of the attributes on disk (must be greater or equal to attr_sz)
    @param read: assert to 1 for reading
    @param sync: whether to force sync on disk of attributes

    
    @retval 0 on success
    @retval -1 on error (see errno for details
    
*/
int fdl_access_count=10;
int exp_trck_rw_attributes(char *root_path,rozofs_inode_t *inode,void *attr_p,int attr_sz,int max_attr_sz,int read, int sync)
{
   int fd = -1;
   ssize_t count;
   char pathname[1024];
   
#if 0
#warning do not write
     if (fdl_access_count == 0 ) return 0;
     fdl_access_count--;

#endif   
   if (attr_sz > max_attr_sz)
   {
      errno = EFBIG;
      return -1;
   }
    /*
    ** build the pathname of the tracking file
    */
    sprintf(pathname,"%s/%d/trk_%llu",root_path,inode->s.usr_id,(long long unsigned int)inode->s.file_id);
   /*
   ** the file exist so open it and read its header in order to find out where is the 
   ** next index to allocated
   */
     open_count++;

   if ((fd = open(pathname, O_RDWR , 0640)) < 0)  
   {
     severe("cannot open %s: %s\n",pathname,strerror(errno));     
     return -1;
   } 
   /*
   ** get the real index of the inode within the tracking file
   */
   int real_idx = exp_trck_get_relative_inode_idx(fd,root_path,inode);
   if (real_idx < 0)
   {
     CLOSE_CONTROL(__LINE__);
     close(fd);   
     return -1;
   }
   /*
   ** write the attributes on disk: 
   */
   off_t attr_offset = real_idx*max_attr_sz+sizeof(exp_trck_file_header_t);
   if (read) count = pread(fd,attr_p,attr_sz, attr_offset);
   else {
     count = pwrite(fd,attr_p,attr_sz, attr_offset);
     /*
     ** sync data on disk immeditely if requested 
     ** and allowed
     */
     if ((sync) && (!common_config.disable_sync_attributes)){
       fdatasync(fd);
     } 
   }	         
   if (count != attr_sz)
   {
     CLOSE_CONTROL(__LINE__);
     if (fd != -1) close(fd);
     return -1;
   } 
   CLOSE_CONTROL(__LINE__);
   if (fd != -1) close(fd);
   return 0; 
}

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
int exp_metadata_write_attributes(exp_trck_top_header_t *top_hdr_p,rozofs_inode_t *inode,void *attr_p,int attr_sz, int sync)
{
   exp_trck_header_memory_t  *main_trck_p;

   main_trck_p = top_hdr_p->entry_p[inode->s.usr_id];
   if (main_trck_p == NULL)
   {
      severe("user_id %d does not exist\n",inode->s.usr_id);
      errno = ENOENT;
      return -1;
   }
   if (attr_sz > main_trck_p->max_attributes_sz)
   {
      errno = EFBIG;
      return -1;
   }
   /*
   ** take care of the tracking
   */
//   expt_set_bit(top_hdr_p->trck_inode_p,inode->s.usr_id,inode->s.file_id);
   
   return exp_trck_rw_attributes(main_trck_p->root_path,inode,attr_p,attr_sz,main_trck_p->max_attributes_sz,0,sync);
}


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
int exp_metadata_create_attributes_burst(exp_trck_top_header_t *top_hdr_p,rozofs_inode_t *inode,void *attr_p,int attr_sz, int sync)
{
   exp_trck_header_memory_t  *main_trck_p;
   int fd = -1;
   ssize_t count;
   char pathname[1024];
   
   main_trck_p = top_hdr_p->entry_p[inode->s.usr_id];
   if (main_trck_p == NULL)
   {
      severe("user_id %d does not exist\n",inode->s.usr_id);
      errno = ENOENT;
      return -1;
   }
   /*
   ** take care of the tracking
   */
//   expt_set_bit(top_hdr_p->trck_inode_p,inode->s.usr_id,inode->s.file_id);
    /*
    ** build the pathname of the tracking file
    */
    sprintf(pathname,"%s/%d/trk_%llu",main_trck_p->root_path,inode->s.usr_id,(long long unsigned int)inode->s.file_id);
   /*
   ** the file exist so open it and read its header in order to find out where is the 
   ** next index to allocated
   */
     open_count++;

   if ((fd = open(pathname, O_RDWR , 0640)) < 0)  
   {
     severe("cannot open %s: %s\n",pathname,strerror(errno));
     return -1;
   } 
   /*
   ** write the attributes on disk: 
   */
   off_t attr_offset = inode->s.idx*main_trck_p->max_attributes_sz+sizeof(exp_trck_file_header_t);
   count = pwrite(fd,attr_p,attr_sz, attr_offset);		         
   if (count != attr_sz)
   {
     CLOSE_CONTROL(__LINE__);
     if (fd != -1) close(fd);
     return -1;
   } 
   CLOSE_CONTROL(__LINE__);
   if (fd != -1) close(fd);
   return 0; 







   
   return exp_trck_rw_attributes(main_trck_p->root_path,inode,attr_p,attr_sz,main_trck_p->max_attributes_sz,0,sync);
}

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
int exp_metadata_read_attributes(exp_trck_top_header_t *top_hdr_p,rozofs_inode_t *inode,void *attr_p,int attr_sz)
{
   exp_trck_header_memory_t  *main_trck_p;

   main_trck_p = top_hdr_p->entry_p[inode->s.usr_id];
   if (main_trck_p == NULL)
   {
      severe("user_id %d does not exist\n",inode->s.usr_id);
      errno = ENOENT;
      return -1;
   }
   if (attr_sz > main_trck_p->max_attributes_sz)
   {
      errno = EFBIG;
      return -1;
   }
   return exp_trck_rw_attributes(main_trck_p->root_path,inode,attr_p,attr_sz,main_trck_p->max_attributes_sz,1,0/* No sync*/);
}

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
int exp_metadata_trck_lookup_ctx_init(exp_trck_header_memory_t  *main_trck_p)
{

   main_trck_p->cur_main_idx = main_trck_p->entry.first_idx;
   return 0;
}  
/*
**__________________________________________________________________
*/
/**
*
    get the file header of a tracking file within a given user_id directory (internal api)
    
    @param main_trck_p: pointer to the main tracking context
    @param user_id: reference of the directory
    @param file_id : reference of the tracking file
    @param buf_p: size of the attributes
    @param nb_entries_p: pointer where service will return the number iof entries (might be NULL)
    
    @retval 0 on success
    @retval -1 on error   
*/
int exp_trck_get_tracking_file_header(exp_trck_header_memory_t  *main_trck_p,uint64_t file_id,exp_trck_file_header_t *buf_p,int *nb_entries_p)
{
   int fd = -1;
   int ret;
   struct stat stats;
   int i;
   char pathname[1024];

   if (nb_entries_p!= NULL) *nb_entries_p = 0;
    /*
    ** build the pathname of the tracking file
    */
    sprintf(pathname,"%s/%d/trk_%llu",main_trck_p->root_path,main_trck_p->user_id,(long long unsigned int)file_id);

   /*
   ** the file exist so open it and read its header in order to find out where is the 
   ** next index to allocated
   */
     open_count++;

   if ((fd = open(pathname, O_RDWR , 0640)) < 0)  
   {
     if (errno != ENOENT) severe("open failure for %s:%s\n",pathname,strerror(errno));
     return -1;
   } 
   /*
   ** write the attributes on disk: 
   */
   ssize_t count = pread(fd,
                          buf_p,sizeof(exp_trck_file_header_t),
		          0);
   if (count != sizeof(exp_trck_file_header_t))
   {
     severe("read failure for %s:%s (len %d/%d)\n",pathname,strerror(errno),(int)count,(int)sizeof(exp_trck_file_header_t));
     CLOSE_CONTROL(__LINE__);close(fd);
     return -1;
   } 
   ret = fstat(fd,&stats);
   if (ret < 0)
   {
     severe("fstat failure for %s:%s\n",pathname,strerror(errno));
     CLOSE_CONTROL(__LINE__);close(fd);
     return -1;
   }
   CLOSE_CONTROL(__LINE__);close(fd);
   int nb_entries;
   if (stats.st_size <  sizeof(exp_trck_file_header_t))
   {
      /*
      ** the file is corrupted
      */
      nb_entries = 0;     
   }
   else
   {
     nb_entries = (stats.st_size - sizeof(exp_trck_file_header_t))/main_trck_p->max_attributes_sz;
     if ((stats.st_size - sizeof(exp_trck_file_header_t))%main_trck_p->max_attributes_sz) nb_entries++; 
   }
   for (i = nb_entries; i < EXP_TRCK_MAX_INODE_PER_FILE; i++)
   {
     buf_p->inode_idx_table[i] = 0xffff;
   }
   if (nb_entries_p!= NULL) *nb_entries_p = nb_entries;
   return 0; 
}

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
                                               int user_id,exp_trck_header_t *buf_p)
{
   exp_trck_header_memory_t  *main_trck_p;
   
   memset(buf_p,0,sizeof(exp_trck_header_t));

   main_trck_p = top_hdr_p->entry_p[user_id];
   if (main_trck_p == NULL)
   {
      severe("user_id %d does not exist\n",user_id);
      errno = ENOENT;
      return -1;
   }
   memcpy(buf_p,&main_trck_p->entry,sizeof(exp_trck_header_t));
   return 0;
}

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
                                               int user_id,exp_trck_header_t *buf_p)
{
   exp_trck_header_memory_t  *main_trck_p;
   int ret;
   
   memset(buf_p,0,sizeof(exp_trck_header_t));

   main_trck_p = top_hdr_p->entry_p[user_id];
   if (main_trck_p == NULL)
   {
      severe("user_id %d does not exist\n",user_id);
      errno = ENOENT;
      return -1;
   }
   ret = exp_trck_open_and_read_main_tracking_file(main_trck_p->root_path,
                                                main_trck_p->user_id,&main_trck_p->entry,top_hdr_p->create_flag);
   if (ret < 0) return -1;
   memcpy(buf_p,&main_trck_p->entry,sizeof(exp_trck_header_t));
   return 0;
}

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
    @retval -1 on error  (see errno for details)  
*/
int exp_metadata_get_tracking_file_header(exp_trck_top_header_t *top_hdr_p,
                                          int user_id,uint64_t file_id,exp_trck_file_header_t *buf_p,int *nb_entries_p)
{
   exp_trck_header_memory_t  *main_trck_p;

   main_trck_p = top_hdr_p->entry_p[user_id];
   if (main_trck_p == NULL)
   {
      severe("user_id %d does not exist\n",user_id);
      errno = ENOENT;
      return -1;
   }
   return exp_trck_get_tracking_file_header(main_trck_p,file_id,buf_p,nb_entries_p);
}



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
                                        int user_id,uint64_t file_id,struct stat *buf_p)
{
   exp_trck_header_memory_t  *main_trck_p;
   char pathname[1024];
   int ret;

   main_trck_p = top_hdr_p->entry_p[user_id];
   if (main_trck_p == NULL)
   {
      severe("user_id %d does not exist\n",user_id);
      errno = ENOENT;
      return -1;
   }
   sprintf(pathname,"%s/%d/trk_%llu",main_trck_p->root_path,main_trck_p->user_id,(long long unsigned int)file_id);
   ret = lstat(pathname,buf_p);
   if (ret < 0)
   {
      return -1;
   }
   return 0;
}

/*
**__________________________________________________________________
*/
/**
*  get the number of file within the tracking file

   @param track_hdr_p : pointer to the tracking header
   
   @retval : number of active files
*/
uint64_t exp_metadata_get_tracking_file_count(exp_trck_file_header_t *track_hdr_p)
{

    int i;
    uint64_t count = 0;
    for (i = 0; i < EXP_TRCK_MAX_INODE_PER_FILE; i++)
    {
      if (track_hdr_p->inode_idx_table[i] != 0xffff) count++;    
    }
    return count;
}

/*
**__________________________________________________________________
*/
/**
* release the top header of a given inode table

   @param top_hdr_p : pointer to the top header of an inode table

   @retval 0 on success
   @retval < 0 on error (see errno for details)
*/
int exp_trck_top_release(exp_trck_top_header_t *top_hdr_p)
{
   int loop;
   if (top_hdr_p == NULL)
   {
     return -1;
   }
   for (loop = 0; loop < EXP_TRCK_MAX_USER_ID; loop++)
   {
      if (top_hdr_p->entry_p[loop] == NULL) continue;
      /*
      ** release the memory resources
      */
      exp_trck_release_header_memory(top_hdr_p->entry_p[loop]);
   }
   EXP_TRK_FREE(top_hdr_p);   
   return 0; 
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
int exp_trck_top_add_user_id(exp_trck_top_header_t *top_hdr_p,int user_id)
{
   if (user_id >= EXP_TRCK_MAX_USER_ID)
   {
     errno = EINVAL;
     return -1;
   }
   if (top_hdr_p->entry_p[user_id] != NULL)
   {
     errno = EEXIST;
     return -1;
   }
   top_hdr_p->entry_p[user_id] = exp_trck_allocate_header_memory(top_hdr_p,user_id,top_hdr_p->create_flag);
   if (top_hdr_p->entry_p[user_id] == NULL)
   {
      return -1;
   }
   return 0;
}
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
exp_trck_top_header_t *exp_trck_top_allocate(char *name,char *root_path,uint16_t max_attributes_sz,int create_flag)
{
   exp_trck_top_header_t *top_hdr_p;
   
   top_hdr_p = EXP_TRK_MALLOC(sizeof(exp_trck_top_header_t));
   if (top_hdr_p == NULL)
   {
     return NULL;
   }
   /*
   ** init of the context
   */
   memset(top_hdr_p,0,sizeof(exp_trck_top_header_t));
   strcpy(top_hdr_p->name,name);
   strcpy(top_hdr_p->root_path,root_path);
   top_hdr_p->max_attributes_sz = max_attributes_sz;
   top_hdr_p->create_flag = create_flag;
   
#if 0
   if (create_flag)
   {
     /*
     ** allocare a context for inode type tracking
     */
     sprintf(full_path,"%s/%s",root_path,name);
     top_hdr_p->trck_inode_p = expt_alloc_context(root_path);
   }
#endif
   return top_hdr_p; 
}

