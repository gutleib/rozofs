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
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <unistd.h>
#include <libintl.h>
#include <libconfig.h>
#include <getopt.h>
#include <inttypes.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/vfs.h>
#include <malloc.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdarg.h>
#include <mntent.h>
#include <sys/mount.h>
#include <attr/xattr.h>

#include <rozofs/rozofs_srv.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include <rozofs/common/common_config.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/rozofs_core_files.h>

#include "rozofs_mover.h"


/*
** Working buffer for read/write
*/
#define ROZOFS_MOVER_BUFFER_SIZE_MB 4
#define ROZOFS_MOVER_1MB            (1024*1024)
#define ROZOFS_MOVER_BUFFER_SIZE   (ROZOFS_MOVER_BUFFER_SIZE_MB*ROZOFS_MOVER_1MB)
static char buf[ROZOFS_MOVER_BUFFER_SIZE];


#define MAX_XATTR_SIZE 4096
static char rozofs_mover_xattr_buff[MAX_XATTR_SIZE];
static char rozofs_mover_rozofs_xattr_buff[MAX_XATTR_SIZE];

typedef struct _rozofs_mover_stat_t {
  uint64_t         submited;
  uint64_t         not_mounted; 
  uint64_t         updated; 
  uint64_t         xattr;
  uint64_t         error;
  uint64_t         success;
  uint64_t         bytes;
} rozofs_mover_stat_t;

static rozofs_mover_stat_t stats;


static int          src=-1;
static int          dst=-1;  
static char         tmp_fname[256]={0};
static char         mount_path[128]={0};
static char         src_fname[128];


/*-----------------------------------------------------------------------------
**
** Check whether a mount path is actually mounted by reading /proc/mounts
** 
** @param   mnt_path    Mount path
**
**----------------------------------------------------------------------------
*/
int rozofs_mover_is_mounted (char * mnt_path) {
  FILE          * mtab = NULL;
  struct mntent * part = NULL;
  int             is_mounted = 0;

  /*
  ** Read file
  */
  mtab = setmntent("/proc/mounts", "r");
  if (mtab == NULL) return 0;
  
  /*
  ** Loop on entries to find out the mount path we are looking for
  */
  while ( ( part = getmntent ( mtab) ) != NULL) {
    if (!part->mnt_fsname) continue;
    if (strcmp(part->mnt_fsname,"rozofs")!=0) continue;
    if (!part->mnt_dir) continue;
    if (strcmp(part->mnt_dir, mnt_path) == 0) {
      is_mounted = 1;
      break;
    }
  }

  endmntent ( mtab);
  return is_mounted;
}
/*-----------------------------------------------------------------------------
**
** Remove a rozofsmount mount point
** 
** @param   mnt_path    Mount path
**
**----------------------------------------------------------------------------
*/
void rozofs_mover_remove_mount_point(char * mnt_path) {

  /*
  ** No mount path
  */
  if (mnt_path[0] == 0) return;

  /*
  ** When mount point is mounted, unmount it  
  */
  if (rozofs_mover_is_mounted(mnt_path)) {

    /*
    ** Unmount it
    */
    if (umount(mnt_path)==-1) {}  
  
    /*
    ** When mount point is still mounted, force unmount 
    */
    if (rozofs_mover_is_mounted(mnt_path)) {
      /*
      ** Unmount it
      */
      if (umount2(mnt_path,MNT_FORCE)==-1) {}  
    }
  }
  
  /*
  ** Remove directory
  */    
  rmdir(mnt_path);
}
/*-----------------------------------------------------------------------------
**
**  Get a free mount point instance by scanning the running rozofsmount instances
**
**----------------------------------------------------------------------------
*/
int rozofs_get_free_rozofsmount_intance(void) {
  char     fname[128];
  char     cmd[256];
  int      fd;
  uint64_t mask = 1; // Do not take instance 0
  int      size;
  int      instance;
  char   * pChar;

  sprintf(fname, "/tmp/rozofs.%d",getpid());

  /*
  ** Check rozofsmount instances in use
  */
  sprintf(cmd,"ps -fC rozofsmount > %s",fname);
  if (system(cmd)==0) {}

  /*
  ** Read result file and then remove it
  */
  fd = open(fname,O_RDONLY);
  if (fd < 0) {
    severe("open(%s) %s",fname, strerror(errno));
    return 0;
  }
  
  size = pread(fd,buf,ROZOFS_MOVER_BUFFER_SIZE,0);
  if (size < 0) {
    severe("pread(%s) %s",fname, strerror(errno));
    close(fd);
    return 0;
  }
  close(fd);
  buf[size] = 0;
  unlink(fname);
  
  /*
  ** Scan for instance in the buffer
  */
  pChar = buf;
  while (*pChar != 0) {

    pChar = strstr(pChar,"instance=");
    if (pChar == NULL) break;
    
    int ret = sscanf(pChar+9,"%d",&instance);
    if (ret != 1) {
      severe("Bad instance option \"%s\"", pChar);
      continue;
    } 
    pChar += 9; 
    mask |= (1ULL<<instance);  
  }

  /*
  ** Find a free instance starting at instance 10
  */
  for (instance=6; instance<64; instance=instance+2) {
    if ((mask & (1ULL<<instance))==0) return instance;
  }
  /*
  ** No free instance
  */
  return 0;
}
/*-----------------------------------------------------------------------------
**
** Create a emporary rozofsmount mount point for RozoFS rebalancing
**
** @param   export_hosts  names or addresses of the exports
** @param   export_path   export path (eid) to mount toward
** @param   mnt_path      Mount path to use for mounting
** @param   mnt_instance  rozofsmount instance
**
**----------------------------------------------------------------------------
*/
int rozofs_mover_create_mount_point(char * export_hosts, char * export_path, char * mnt_path,int mnt_instance) {
  char     cmd[512];
  char   * pChar = cmd; 
  int      i;

  /*
  ** Check whether this path exist
  */
  if (access(mnt_path,W_OK) == 0) {
    /*
    ** The path already exist !!! Unmount just in case
    */
    rozofs_mover_remove_mount_point(mnt_path);   
  }


  /*
  ** Create the temporary mount path
  */
  if (mkdir(mnt_path,S_IRUSR | S_IWUSR | S_IXUSR)<0) {
    severe("mkdir(%s) %s", mnt_path, strerror(errno));
    return -1;
  }  
  
  /*
  ** Prepare the rozofsmount command 
  */
  pChar += sprintf(pChar,"rozofsmount -H %s -E %s %s", export_hosts, export_path, mnt_path);
  pChar += sprintf(pChar," -o rozofsexporttimeout=60,rozofsstoragetimeout=40,rozofsstorclitimeout=50,rozofsnbstorcli=1,auto_unmount,noReadFaultTolerant");
  pChar += sprintf(pChar,",instance=%d",mnt_instance);

  /*
  ** Start the rozofsmount instance
  */
  if (system(cmd)==0) {}
  
  
  /*
  ** Wait for the mount point to be actually mounted
  */
  for (i=0; i < 20; i++) {
    sleep(1);
    if (rozofs_mover_is_mounted(mnt_path)) break; 
  }  
  if (i==20) {
    severe("mount %s failed", mnt_path);
    return -1;
  }
  
  /*
  ** Change local directory to the mount point
  */
  if (chdir(mnt_path)!= 0) {
    severe("chdir(%s) %s",mnt_path,strerror(errno));
    if (chdir("/")!= 0) {}
    return -1;
  } 
  return 0; 
  
}
/*-----------------------------------------------------------------------------
**
** Get time in micro seconds
**
** @param from  when set compute the delta in micro seconds from this time to now
**              when zero just return current time in usec
**----------------------------------------------------------------------------
*/
uint64_t rozofs_mover_get_us(uint64_t from) {
  struct timeval     timeDay;
  uint64_t           us;
  
  /*
  ** Get current time in us
  */
  gettimeofday(&timeDay,(struct timezone *)0); 
  us = ((unsigned long long)timeDay.tv_sec * 1000000 + timeDay.tv_usec);

  /*
  ** When from is given compute the delta
  */
  if (from) {
    us -= from;
  }  
  
  return us;	
}
/*-----------------------------------------------------------------------------
**
** Build the destination temporary file name
**
** @param result     Where to format the file name
** 
**----------------------------------------------------------------------------
*/
void rozofs_mover_tmp_fname(char * result) {
  int slice;
  char * pChar;
#define SEARCH_TAG   "usr_id="
  
  /*
  ** Get the slice of the source file
  */
  pChar = strstr(rozofs_mover_rozofs_xattr_buff,SEARCH_TAG);
  if (pChar == NULL) {
    severe("Can not find %s in user.rozofs xattr %s",SEARCH_TAG,rozofs_mover_rozofs_xattr_buff);
  }
  else {
    pChar += strlen(SEARCH_TAG);
    sscanf(pChar,"%d",&slice);
  }
  slice = slice % MAX_SLICE_NB;
  
  /*
  ** Create a temporary file on the given slice
  */
  sprintf(result,"./.rozofs/@rozofs_slice@%d",slice); 
}
/*-----------------------------------------------------------------------------
**
** Copy xattribute from one file to an other
**
** @param srcName           Source file name
** @param dstName           Destination file name
**
** @retval The number of extended attributes moved or -1 in case of error
**
**----------------------------------------------------------------------------
*/
int rozofs_mover_xattr(char * srcName, char * dstName) {
  ssize_t       size;
  char *pAttr = buf;
  char *pEnd  = buf;
  int           nb;

  /*
  ** Get the list of extended attributes of the source file
  */
  size = listxattr(srcName,buf,ROZOFS_MOVER_BUFFER_SIZE);
  if (size < 0) {
    severe("listxattr(%s) %s\n", srcName, strerror(errno));
    /*
    ** Let's do as if no extended attribute is set
    */
    return 0;
  } 
  
  /*
  ** No extended attribute
  */
  if (size == 0) {
    return 0;
  }

  pEnd = buf + size; /* after last extended attribute */
  nb   = 0;
  /*
  ** While end of buffer not reached
  */
  while (pAttr < pEnd) {
  
    /*
    ** Read next extended attribute
    */
    size = getxattr(srcName,pAttr,rozofs_mover_xattr_buff,MAX_XATTR_SIZE);    
    if (size == -1) {
      severe("getxattr(%s) on file %s %s\n", pAttr, srcName, strerror(errno));
      return -1;
    }        

    nb++;
        
    /*
    ** Re-write it
    */    
    if (setxattr(dstName, pAttr, rozofs_mover_xattr_buff, size, XATTR_CREATE)<0) {
      severe("setxattr(%s) on file %s %s\n", pAttr, dstName, strerror(errno));
      return -1;
    } 
    
    /*
    ** Next extended attribute
    */
    pAttr += (strlen(pAttr)+1);
  }
  return nb;    
}
/*-----------------------------------------------------------------------------
**
** Move one file
**
** @param job               Description of the file to move
** @param throughput        Throughput limitation or zero when no limit
**
**----------------------------------------------------------------------------
*/
int rozofs_do_move_one_file(rozofs_mover_job_t * job, int throughput) {
  struct  stat statbuf1;
  struct  stat statbuf2;
  char       * pChar;
  int          i;
  off_t        offset=0;
  uint64_t     loop_delay_us = 0;
  uint64_t     total_delay_us=0;
  int64_t      sleep_time_us;
  uint64_t     begin;
  int          nbxattr;
  int          size;
  
  tmp_fname[0] = 0;
  
  /*
  ** Starting time
  */
  begin = rozofs_mover_get_us(0);
    
  /*
  ** Check file name exist
  */
  if (access(job->name,R_OK) != 0) {
    severe("Can not access file \"%s\"",job->name);
    goto generic_error;
  }
  
  /*
  ** Open source file for reading
  */
  src = open(job->name,O_RDONLY);
  if (src < 0) {
    severe("open(%s) %s",job->name,strerror(errno));
    goto generic_error;
  }
  
  /*
  ** Get the file modification time
  */
  if (fstat(src,&statbuf1)< 0) {  
    severe("fstat(%s) %s",job->name,strerror(errno));
    goto generic_error;
  }
  
  
  /*
  ** Create working dir .rozofs under root path
  */
  if (access("./.rozofs",W_OK) != 0) {
    if (mkdir("./.rozofs",S_IRUSR | S_IWUSR | S_IXUSR)<0) {
      if (errno!=EEXIST) {
        severe("mkdir(./.rozofs) %s",strerror(errno));
        goto generic_error;
      }
    }
  }  
  
  /*
  ** Get rozofs extended attribute of the source file
  */
  size = fgetxattr(src,"user.rozofs",rozofs_mover_rozofs_xattr_buff,MAX_XATTR_SIZE);  
  if (size <= 0) {
    severe("fgetxattr(%s) on file %s %s\n", "user.rozofs", job->name, strerror(errno));
    goto generic_error;
  } 
  rozofs_mover_rozofs_xattr_buff[size] = 0;  
   
  /*
  ** Remove file that could have the same name as our destination file
  */ 
  rozofs_mover_tmp_fname(tmp_fname);
  unlink(tmp_fname);
  
  /*
  ** Create the brand new file
  */
  if (mknod(tmp_fname,S_IFREG|0766,0)< 0) {  
    severe("mknod(%s) %s",tmp_fname,strerror(errno));
    goto generic_error;
  }

  /*
  ** Set the new distribution 
  */
  pChar = buf;
  pChar += sprintf(pChar,"%d",job->cid);
  i=0;
  while ((i<ROZOFS_SAFE_MAX)&&(job->sid[i]!=0)) {
    pChar += sprintf(pChar," %d", job->sid[i]);
    i++;
  }
  if (setxattr(tmp_fname, "user.rozofs", buf, strlen(buf),0)<0) {
    if (errno==EINVAL) {
      severe("invalid distibution %s:%s",tmp_fname,buf);
      goto generic_error;   
    }
    
    severe("fsetxattr(%s) %s",buf,strerror(errno));   
    goto generic_error;
  }
  
  /*
  ** Open destination file
  */
  dst = open(tmp_fname,O_RDWR,0766);
  if (dst < 0) {
    severe("open(%s) %s",tmp_fname,strerror(errno));
    goto generic_error;    
  }   

  /*
  ** When troughput limitation is set, compute allowed time for
  ** one read write loop
  */
  if (throughput>0) {
    /*
    ** Compute loop_delay_us in us for copying the buffer of size ROZOFS_MOVER_BUFFER_SIZE_MB
    */
    loop_delay_us = 1000000 * ROZOFS_MOVER_BUFFER_SIZE_MB/throughput;
  }   

  /*
  ** Copy the file
  */
  while (1) {
    int size;

    /*
    ** Read a whole buffer
    */      
    size = pread(src, buf, ROZOFS_MOVER_BUFFER_SIZE, offset);
    if (size < 0) {
      severe("pread(%s) %s",job->name,strerror(errno)); 
      goto generic_error;         
    }
    
    /*
    ** End of file
    */      
    if (size == 0) break;
    
    /*
    ** Write the data
    */      
    if (pwrite(dst, buf, size, offset)<0) {
      severe("pwrite(%d) %s",size,strerror(errno));     
      goto generic_error;       
    }
    
    offset += size;

    /*
    ** When throughput limitation is set adapdt the speed accordingly
    ** by sleeping for a while
    */
    if (throughput>0) {
      if (size == ROZOFS_MOVER_BUFFER_SIZE) {
        total_delay_us += loop_delay_us;
      }
      else {
      	total_delay_us += (loop_delay_us*size/ROZOFS_MOVER_BUFFER_SIZE);
      }	
      sleep_time_us   = total_delay_us - rozofs_mover_get_us(begin);
      if (sleep_time_us >= 10000) {
        usleep(sleep_time_us);
      }
    }       
  }

  close(src);
  src = -1;
  close(dst);
  dst = -1;

  /*
  ** Get the file modification time
  */
  if (stat(job->name,&statbuf2)< 0) {  
    severe("fstat(%s) %s",job->name,strerror(errno));  
    goto generic_error;
  }
  
  /*
  ** File has been modified while copying it
  */
  if (statbuf2.st_mtime != statbuf1.st_mtime) {
    severe("file %s has been updated during copy",job->name);
    stats.updated++;
    goto specific_error;
  }    
  
  /*
  ** Copy extended attributes
  */
  nbxattr = rozofs_mover_xattr(job->name,tmp_fname);
  if (nbxattr<0) {
    stats.xattr++;
    goto specific_error;
  }
  
  /*
  ** Get the file modification time
  */
  if (stat(job->name,&statbuf2)< 0) {  
    severe("fstat(%s) %s",job->name,strerror(errno));  
    goto generic_error;
  }
  
  /*
  ** File has been modified while copying it
  */
  if (statbuf2.st_mtime != statbuf1.st_mtime) {
    severe("file %s has been updated during copy",job->name);
    stats.updated++;
    goto specific_error;
  }    
    
  /*
  ** Replace original with the copy
  */
  if(rename(tmp_fname,job->name)<0) {
    severe("rename %s %s",job->name,strerror(errno));
    goto generic_error;
  }  
  
  stats.success++;
  stats.bytes += offset;
    
  /*
  ** Done
  */
  total_delay_us = rozofs_mover_get_us(begin);
  if (offset > ROZOFS_MOVER_1MB) {
    info("%s %lluMB relocated with %d xattr in %d.%6.6d seconds\n", job->name, 
                      (long long unsigned int) offset/ROZOFS_MOVER_1MB,
		      nbxattr,
                      (int)total_delay_us/1000000, (int)total_delay_us%1000000); 
  }
  else {
    info("%s %lluKB relocated with %d xattr in %d.%6.6d seconds\n",job->name, 
                       (long long unsigned int) offset,
		       nbxattr,
                       (int) total_delay_us/1024, (int)total_delay_us%1000000); 
  }
  return 0;
  
generic_error:
  stats.error++;
  
specific_error:  

  /*
  ** Close opened files
  */

  if (src > 0) {    
    close(src);
    src = -1;
  }

  if (dst > 0) {
    close(dst);
    dst = -1;
  } 
  
  /*
  ** Remove the eventualy created temporary file
  */ 
  if (tmp_fname[0] != 0) {
    unlink(tmp_fname);   
  } 
  tmp_fname[0] = 0;

  
  return -1;
}  
/*-----------------------------------------------------------------------------
**
** Move a list a file to a new location for rebalancing purpose
**
** @param exportd_hosts     exportd host name or addresses (from configuration file)
** @param export_path       the export path to mount
** @param throughput        throughput litation in MB. 0 is no limitation.
** @param jobs              list of files along with their destination
**
**----------------------------------------------------------------------------
*/
int rozofs_do_move_one_export(char * exportd_hosts, char * export_path, int throughput, list_t * jobs) {
  int                  instance;
  list_t             * p;
  list_t             * n;
  rozofs_mover_job_t * job;

  if (list_empty(jobs)) {
     return 0;
  }
  
  /*
  ** Find out a free rozofsmount instance
  */
  instance = rozofs_get_free_rozofsmount_intance();
  sprintf(mount_path,"/tmp/rozofs_mover_pid%d_instance%d", getpid(), instance);    

  /*
  ** Mount it 
  */
  rozofs_mover_create_mount_point(exportd_hosts , export_path, mount_path, instance);

  
  /*
  ** Loop on the file to move
  */
  list_for_each_forward_safe(p,n,jobs) {
    
    job = (rozofs_mover_job_t *) list_entry(p, rozofs_mover_job_t, list);
    stats.submited++;

    /*
    ** Process the job
    */
    if (rozofs_mover_is_mounted(mount_path)) {
      rozofs_do_move_one_file(job,throughput); 
    }
    else {
      stats.not_mounted++;    
    }
   
    /*
    ** Free this job
    */
    list_remove(&job->list);
    free(job->name);
    free(job);
  }

  /*
  ** Get out of the mountpoint before removing it
  */
  if (chdir("/")!= 0) {}

  /*
  ** Unmount the temporary mount path 
  */
  rozofs_mover_remove_mount_point(mount_path);
  /*
  ** Clear mount path name
  */
  mount_path[0] = 0;
  return 0;
}


/*-----------------------------------------------------------------------------
**
** Move one file in fid mode
**
** @param job               Description of the file to move
** @param throughput        Throughput limitation or zero when no limit
**
**----------------------------------------------------------------------------
*/
int rozofs_do_move_one_file_fid_mode(rozofs_mover_job_t * job, int throughput) {
  char       * pChar;
  int          i;
  off_t        offset=0;
  uint64_t     loop_delay_us = 0;
  uint64_t     total_delay_us=0;
  int64_t      sleep_time_us;
  uint64_t     begin;
  
  tmp_fname[0] = 0;
  char dst_fname[128];
  char buf_fid[64];
  
  rozofs_inode_t *inode_p = (rozofs_inode_t*)job->name;

  inode_p->s.key = ROZOFS_REG_S_MOVER;
  rozofs_uuid_unparse((unsigned char *)job->name,buf_fid);
  sprintf(src_fname,"@rozofs_uuid@%s",buf_fid);

  inode_p->s.key = ROZOFS_REG_D_MOVER;
  rozofs_uuid_unparse((unsigned char *)job->name,buf_fid);
  sprintf(dst_fname,"@rozofs_uuid@%s",buf_fid);
  /*
  ** Starting time
  */
  begin = rozofs_mover_get_us(0);    
  /*
  ** Check file name exist
  */
  if (access(src_fname,R_OK) != 0) {
    severe("Can not access file \"%s\"",src_fname);
    goto generic_error;
  }  
  /*
  ** Open source file for reading
  */
  src = open(src_fname,O_RDONLY);
  if (src < 0) {
    severe("open(%s) %s",src_fname,strerror(errno));
    goto generic_error;
  }

  /*
  ** Set the new distribution 
  */
  pChar = buf;
  pChar += sprintf(pChar,"mover_allocate = %d",job->cid);
  i=0;
  while ((i<ROZOFS_SAFE_MAX)&&(job->sid[i]!=0)) {
    pChar += sprintf(pChar," %d", job->sid[i]);
    i++;
  }
  if (setxattr(dst_fname, "user.rozofs", buf, strlen(buf),0)<0) {
    if (errno==EINVAL) {
      severe("invalid distibution %s:%s",dst_fname,buf);
      goto generic_error;   
    }
    
    severe("fsetxattr(%s) %s",dst_fname,strerror(errno));   
    goto generic_error;
  }
  
  /*
  ** Open destination file
  */
  dst = open(dst_fname,O_RDWR,0766);
  if (dst < 0) {
    severe("open(%s) %s",dst_fname,strerror(errno));
    goto abort;    
  }   
  /*
  ** When troughput limitation is set, compute allowed time for
  ** one read write loop
  */
  if (throughput>0) {
    /*
    ** Compute loop_delay_us in us for copying the buffer of size ROZOFS_MOVER_BUFFER_SIZE_MB
    */
    loop_delay_us = 1000000 * ROZOFS_MOVER_BUFFER_SIZE_MB/throughput;
  }   

  /*
  ** Copy the file
  */
  while (1) {
    int size;

    /*
    ** Read a whole buffer
    */      
    size = pread(src, buf, ROZOFS_MOVER_BUFFER_SIZE, offset);
    if (size < 0) {
      severe("pread(%s) %s",src_fname,strerror(errno)); 
      goto abort;         
    }
    
    /*
    ** End of file
    */      
    if (size == 0) break;
    
    /*
    ** Write the data
    */      
    if (pwrite(dst, buf, size, offset)<0) {
      severe("pwrite(%s,%d) %s",dst_fname,size,strerror(errno));     
      goto abort;       
    }
    
    offset += size;

    /*
    ** When throughput limitation is set adapdt the speed accordingly
    ** by sleeping for a while
    */
    if (throughput>0) {
      if (size == ROZOFS_MOVER_BUFFER_SIZE) {
        total_delay_us += loop_delay_us;
      }
      else {
      	total_delay_us += (loop_delay_us*size/ROZOFS_MOVER_BUFFER_SIZE);
      }	
      sleep_time_us   = total_delay_us - rozofs_mover_get_us(begin);
      if (sleep_time_us >= 10000) {
        usleep(sleep_time_us);
      }
    }       
  }

  close(src);
  src = -1;
  close(dst);
  dst = -1;

  pChar = buf;
  pChar += sprintf(pChar,"mover_validate = 0");
  if (setxattr(dst_fname, "user.rozofs", buf, strlen(buf),0)<0) {
    if (errno == EACCES)
    {
       stats.updated++;
       goto specific_error;
    }
    severe("fsetxattr(%s,%s) %s",dst_fname,buf,strerror(errno));   
    goto generic_error;
  }
  stats.success++;
  stats.bytes += offset;
    
  /*
  ** Done
  */
  return 0;

abort:
  if (setxattr(dst_fname, "user.rozofs", "mover_invalidate = 0", strlen("mover_invalidate"),0)<0) {
    severe("fsetxattr(%s,mover_invalidate) %s",dst_fname,strerror(errno));   
  }
  
generic_error:
  stats.error++;
  
specific_error:  

  /*
  ** Close opened files
  */

  if (src > 0) {    
    close(src);
    src = -1;
  }

  if (dst > 0) {
    close(dst);
    dst = -1;
  } 
  return -1;
}

/*-----------------------------------------------------------------------------
**
** Move a list a file to a new location for rebalancing purpose
**
** @param exportd_hosts     exportd host name or addresses (from configuration file)
** @param export_path       the export path to mount
** @param throughput        throughput litation in MB. 0 is no limitation.
** @param jobs              list of files along with their destination
**
**----------------------------------------------------------------------------
*/
int rozofs_do_move_one_export_fid_mode(char * exportd_hosts, char * export_path, int throughput, list_t * jobs) {
  int                  instance;
  list_t             * p;
  list_t             * n;
  rozofs_mover_job_t * job;

  if (list_empty(jobs)) {
     return 0;
  }
  
  /*
  ** Find out a free rozofsmount instance
  */
  instance = rozofs_get_free_rozofsmount_intance();
  sprintf(mount_path,"/tmp/rozofs_mover_pid%d_instance%d", getpid(), instance);    

  /*
  ** Mount it 
  */
  rozofs_mover_create_mount_point(exportd_hosts , export_path, mount_path, instance);

  
  /*
  ** Loop on the file to move
  */
  list_for_each_forward_safe(p,n,jobs) {
    
    job = (rozofs_mover_job_t *) list_entry(p, rozofs_mover_job_t, list);
    stats.submited++;

    /*
    ** Process the job
    */
    if (rozofs_mover_is_mounted(mount_path)) {
      rozofs_do_move_one_file_fid_mode(job,throughput); 
    }
    else {
      stats.not_mounted++;    
    }
   
    /*
    ** Free this job
    */
    list_remove(&job->list);
    free(job->name);
    free(job);
  }

  /*
  ** Get out of the mountpoint before removing it
  */
  if (chdir("/")!= 0) {}

  /*
  ** Unmount the temporary mount path 
  */
  rozofs_mover_remove_mount_point(mount_path);
  /*
  ** Clear mount path name
  */
  mount_path[0] = 0;
  return 0;
}
/*-----------------------------------------------------------------------------
**
** Man function
**
**----------------------------------------------------------------------------
*/
void rozofs_mover_man(char * pChar) {
  pChar += rozofs_string_append(pChar,"mover       : display mover statistics\n");  
  pChar += rozofs_string_append(pChar,"mover reset : reset mover statistics\n");    
}  
/*-----------------------------------------------------------------------------
**
** Statistics formating
**
**----------------------------------------------------------------------------
*/
void rozofs_mover_print_stat(char * pChar) {
  pChar += sprintf(pChar,"{ \"mover\" : \n");
  pChar += sprintf(pChar,"   {\n");
  pChar += sprintf(pChar,"     \"submited files\" : %llu,\n",(unsigned long long)stats.submited);
  pChar += sprintf(pChar,"     \"mount error\"    : %llu,\n",(unsigned long long)stats.not_mounted);
  pChar += sprintf(pChar,"     \"updated files\"  : %llu,\n",(unsigned long long)stats.updated);
  pChar += sprintf(pChar,"     \"xattr copy\"     : %llu,\n",(unsigned long long)stats.xattr);
  pChar += sprintf(pChar,"     \"other error\"    : %llu,\n",(unsigned long long)stats.error);
  pChar += sprintf(pChar,"     \"success\"        : %llu,\n",(unsigned long long)stats.success);
  pChar += sprintf(pChar,"     \"bytes moved\"    : %llu\n",(unsigned long long)stats.bytes);
  pChar += sprintf(pChar,"     \"last moved\"     : %s\n",src_fname);
  pChar += sprintf(pChar,"   }\n}\n");
}  
/*-----------------------------------------------------------------------------
**
** Diagnostic function
**
**----------------------------------------------------------------------------
*/
void rozofs_mover_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
  char           *pChar=uma_dbg_get_buffer();
 
  if (argv[1]!=0) {
    if (strcmp(argv[1],"reset")==0) {
      memset(&stats,0,sizeof(stats));
    }
  }
  rozofs_mover_print_stat(pChar);
  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());
}
/*-----------------------------------------------------------------------------
**
** Function called on exception in order to cleaup the mess
**
**----------------------------------------------------------------------------
*/
static void on_crash(int sig) {

  /*
  ** Close opened files
  */

  if (src > 0) {    
    close(src);
    src = -1;
  }

  if (dst > 0) {
    close(dst);
    dst = -1;
  } 
  
  /*
  ** Remove the eventualy created temporary file
  */ 
  if (tmp_fname[0] != 0) {
    unlink(tmp_fname);   
  } 
  tmp_fname[0] = 0;
  
  
  /*
  ** Remove move point
  */
  rozofs_mover_remove_mount_point(mount_path);
} 
/*-----------------------------------------------------------------------------
**
** Service initialize
**
**----------------------------------------------------------------------------
*/
int rozofs_mover_init() {

  /*
  ** Initialize global variables
  */
  memset(&stats,0, sizeof(stats));
  memset(mount_path,0,sizeof(mount_path));
  memset(tmp_fname,0,sizeof(tmp_fname));
    
  /*
  ** Add debug function
  */  
  uma_dbg_addTopicAndMan("mover", rozofs_mover_debug, rozofs_mover_man, 0);  
  
  /*
  ** Add crash call back
  */
  rozofs_attach_crash_cbk(on_crash);
  
  return 0;
}
