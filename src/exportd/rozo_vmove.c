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
#include <getopt.h>
#include <attr/xattr.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/mattr.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/rozofs_service_ports.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/rozofs_core_files.h>
#include <rozofs/common/common_config.h>
#include "export.h"
#include "rozo_inode_lib.h"
#include "exp_cache.h"
#include "volume.h"
#include "rozofs_mover.h"
#include "export_volume_stat.h"

#define ROZOFS_VMOVE_UNDEFINED  0
#define ROZOFS_VMOVE_FAST       1
#define ROZOFS_VMOVE_SLOW       2


int                destination = ROZOFS_VMOVE_UNDEFINED;
int                cluster[ROZOFS_CLUSTERS_MAX] = {ROZOFS_VMOVE_UNDEFINED};
list_t             cluster_distributor;
econfig_t          exportd_config;
export_config_t  * econfig = NULL;

uint64_t  scanned_match   = 0;
uint64_t  scanned_hybrid  = 0;
uint64_t  scanned_already = 0;
uint64_t  scanned_to_move = 0;
uint64_t  scanned_error   = 0;
uint64_t  scanned_directories  = 0;


int rozofs_no_site_file = 0;

list_t jobs;  
list_t subdirectories;

void *rozofs_export_p = NULL;
/*
** The name of the utility to display on help
*/
char * utility_name=NULL;
int export_local_site_number = 0;

char printfBuffer[4096*4];

/*
** Whether recursive mode is requested
*/
int recursive = 0;

typedef struct _rozo_vmove_directory_t {
  list_t  list;
  fid_t   fid;
} rozo_vmove_directory_t;

rozo_vmove_directory_t * currentDir = NULL;


typedef struct _rozo_vmove_subdirectory_t {
  fid_t       fid;
  uint32_t    subdirs;
  uint32_t    files;
} rozo_vmove_subdirectory_t;  

typedef struct _rozo_vmove_subdirectories_t {
  uint32_t                  nb;
  uint32_t                  count;
  list_t                    list;
  rozo_vmove_subdirectory_t dir[1];
} rozo_vmove_subdirectories_t;  

rozo_vmove_subdirectories_t * pNextLevel;
rozo_vmove_subdirectories_t * pPrevLevel;


fid_t                       * pFidTable;
uint32_t                      nbFid;


export_vol_cluster_stat2_t  cluster_stats;
scan_index_context_t        scan_context;
uint64_t                    rozofs_vmove_size2move;
int                         whole_eid = 0;

/*
**_______________________________________________________________________
**
** Read a directory attributes from SSD
**
** @param e           Export context
** @param fid         Directory FID to get attributes for
** @param ext_mattr   Where to return the read attributes
**  
** @retval 0 no match
** @retval 1 when match
**_______________________________________________________________________
*/
int rozo_vmove_read_dir_attribute(export_t *e, fid_t fid, ext_mattr_t * ext_mattr) {
  exp_trck_header_memory_t  *main_trck_p;
  char pathname[1024];
  struct stat stats;
  int ret;
  int fd;
  ssize_t returned_size;
  rozofs_inode_t   * ino_p  = (rozofs_inode_t *) fid;  

  main_trck_p = e->trk_tb_p->tracking_table[ROZOFS_DIR]->entry_p[ino_p->s.usr_id];
  if (main_trck_p == NULL) {
     severe("user_id %d does not exist\n",ino_p->s.usr_id);
     errno = ENOENT;
     return -1;
  }
  /*
  ** build the pathname of the tracking file
  */
  sprintf(pathname,"%s/%d/trk_%llu",e->trk_tb_p->tracking_table[ROZOFS_DIR]->root_path,ino_p->s.usr_id,(long long unsigned int)ino_p->s.file_id);
  /*
  ** open the inode file
  */
  if ((fd = open(pathname, O_RDWR , 0640)) < 0) {
    severe("cannot open %s: %s\n",pathname,strerror(errno));
    return -1;
  } 
  /*
  ** get the size of the file
  */
  ret = fstat(fd,&stats);
  if (ret < 0) {
    severe("fstat failure for %s:%s\n",pathname,strerror(errno));
    close(fd);
    return -1;
  }
  /*
  ** Check the file contains some inodes
  */
  if (stats.st_size <  sizeof(exp_trck_file_header_t)) {
     severe("%s file too small %d/%d\n",pathname,(int)sizeof(stats.st_size),(int)sizeof(exp_trck_file_header_t));
     close(fd);    
     return -1; 
  } 
  /*
  ** Read at the offset of the input directory
  */
  off_t attr_offset = sizeof(exp_trck_file_header_t) + (ino_p->s.idx * sizeof(ext_mattr_t));
  returned_size = pread(fd,ext_mattr,sizeof(ext_mattr_t), attr_offset);  
  if ( returned_size != sizeof(ext_mattr_t)) {
     severe("error while reading %s : %s\n",pathname,strerror(errno));
     close(fd);
     return -1;
  }
  close(fd);
  return 0;
}
/*
**_______________________________________________________________________
**
** Read the attributes of a directory from SSD to get its number of
** files and sub-directories
**
** @param e           Export context
** @param fid         Directory FID to get sub-directories and files for
** @param nbsubdir    returned number of sub-directories
** @param nbfile      returned number of files
**  
** @retval 0 no match
** @retval 1 when match
**_______________________________________________________________________
*/
int rozofs_get_subdir_nb(export_t * e, fid_t fid, uint32_t * nbsubdir, uint32_t * nbfile) {
  ext_mattr_t        ext_mattr;
  int                ret;
  uint32_t           local_subdir;
  

  
  /*
  ** Lookup directory entry 
  */ 
  ret = rozo_vmove_read_dir_attribute(e,fid,&ext_mattr);
  if (ret < 0) {
    char fidstr[40];
    rozofs_fid_append(fidstr,fid);
    severe("rozo_vmove_read_dir_attribute(%s) %s\n",fidstr,strerror(errno));
    return -1;	   
  } 
    
  local_subdir = ext_mattr.s.attrs.nlink - 2;
  (*nbsubdir) = local_subdir;
  (*nbfile)   = (ext_mattr.s.attrs.children - local_subdir);   
  
#if 0  
  {
    char fidstr[40];
    rozofs_fid_append(fidstr,fid);
    info("%s %s subdir %d files %d",
         (ext_mattr.s.fname.name_type==ROZOFS_FNAME_TYPE_DIRECT)?ext_mattr.s.fname.name:"?",
         fidstr, local_subdir,(ext_mattr.s.attrs.children - local_subdir));
  }
#endif
  
  if (ext_mattr.s.attrs.children - local_subdir) {
    nbFid++;
  }  
  return 0;
} 
/*
**_______________________________________________________________________
**
** RozoFS specific function for visiting directories. We have made the list of 
** subdirectories at a given level (in pPrevLevel) and we are lokking at the
** next level of directory, i.2 their sub-directories. The found subdirectories
** are recorded in pNextLevel. 
**
** @param exportd       pointer to exporthd data structure
** @param inode_attr_p  pointer to the inode data
** @param p             always NULL
**  
** @retval 0 no match
** @retval 1 when match
**_______________________________________________________________________
*/
int rozofs_visit_directory(void *exportd,void *inode_attr_p,void *p) {
  ext_mattr_t            * inode_p = inode_attr_p;
  int                      idx;

  /*
  ** Check this directory contains something
  */     
  if (inode_p->s.attrs.children == 0) return 0; 

  /*
  ** Compare the parent FID of the directory to the FIDs in the
  ** pPrevLevel context. This is the list of directory FID of the upper level
  */
  for (idx=0; idx<pPrevLevel->count; idx++) {
    if (memcmp(inode_p->s.pfid,pPrevLevel->dir[idx].fid,sizeof(fid_t)) != 0) {
      continue;
    }  
    /*
    ** Found the parent FID in the list of FID of the upper level.
    ** This directory is so one of the subdirectories we are lokking for
    */
    memcpy(pNextLevel->dir[pNextLevel->count].fid,inode_p->s.attrs.fid,sizeof(fid_t));
    pNextLevel->count++;
#if 0  
    {
      char fidstr[40];
      char parentstr[40];
      rozofs_fid_append(fidstr,inode_p->s.attrs.fid);
      rozofs_fid_append(parentstr,pPrevLevel->dir[idx].fid);
      info("%s %s found under %s",
           (inode_p->s.fname.name_type==ROZOFS_FNAME_TYPE_DIRECT)?inode_p->s.fname.name:"?",
           fidstr, parentstr);
    }
#endif

    /*
    ** Whe the number of FID that we are lokking for is reached,
    ** no need to go on scanning
    */
    if (pNextLevel->count == pNextLevel->nb) rozo_lib_stop_scanning();
    return 1;
  }  
  return 0;
} 
/*
**_______________________________________________________________________
**
** Insert FID in pFidTable which is a sorted list of directory FID that
** we will scan for files in. 
**
** @param fid    The fid to insert
** @param count  Count of FID already installed in the table
**_______________________________________________________________________
*/ 
void rozo_vmove_insert_fid(fid_t fid, uint32_t count) {
  int idx1,idx2;
  int ret;
  
  /*
  ** Compare input FID to beginning of the FID table until finding
  ** where it should be inserted.
  */
  for (idx1=0; idx1<count; idx1++) {
    ret = memcmp(pFidTable[idx1], fid, sizeof(fid_t));
    if (ret < 0) break;
  }
  /*
  ** Shift every FID 1 slot down from the insertion slot
  */ 
  for (idx2=count-1; idx2>=idx1; idx2--) {  
    memcpy(pFidTable[idx2+1],pFidTable[idx2],sizeof(fid_t));
  }
  /*
  ** Insert the input FID at its rank
  */
  memcpy(pFidTable[idx1],fid,sizeof(fid_t));
}
/*
**_______________________________________________________________________
**
** Build a sorted table of the subdirectoris under a given directory
** where we should scan for file that need to be moved.
** We proceed per level. pPrevLevel contains the list of sub-directories
** of a given level, and we will scan for the sun directories.
**
** @param e           Export context
** @param fid         Directory FID to get sub-directories list
** @param recursive   Whether one should proceed recursively
**  
** @retval 0 no match
** @retval 1 when match
**_______________________________________________________________________
*/
int rozofs_build_directory_list(export_t * e, fid_t fid, int recursive) {
  uint32_t                      totalfile;
  uint32_t                      totaldir;
  uint32_t                      localdir;
  int                           idx;
  int                           size;
  list_t                        dir_list_by_level; 
  int                           count;
  
  list_init(&dir_list_by_level);
    
  /*
  ** Upper level only contains the input directory
  */ 
  pPrevLevel = malloc(sizeof(rozo_vmove_subdirectories_t));
  memset(pPrevLevel,0, sizeof(rozo_vmove_subdirectories_t));
  pPrevLevel->nb    = 1;
  pPrevLevel->count = 1;
  memcpy(pPrevLevel->dir[0].fid,fid,sizeof(fid_t));
  list_init(&pPrevLevel->list);
  list_push_back(&dir_list_by_level, &pPrevLevel->list);

  totalfile = 0;
  totaldir  = 1;
  nbFid = 0;
    
  /*
  ** Loop each time one step lower
  */  
  while(1) {

    /*
    ** Read attributes of each subdirectory in this level to find out 
    ** how many subdirectories and files it contain
    */
    localdir = 0;
    for (idx=0; idx<pPrevLevel->count; idx++) {    
      rozofs_get_subdir_nb(e ,pPrevLevel->dir[idx].fid, &pPrevLevel->dir[idx].subdirs, &pPrevLevel->dir[idx].files);
      totalfile += pPrevLevel->dir[idx].files;
      localdir  += pPrevLevel->dir[idx].subdirs;
    }
 
    /*
    ** No more sub directories
    */
    if (localdir == 0) break;

    totaldir += localdir;
    
    /*
    ** Not recursive case 
    */
    if (!recursive) break;
      
    /*
    ** Allocate the structure for the next level now that we now how much subdirectories exist
    */
    size = sizeof(rozo_vmove_subdirectories_t) + ((localdir-1)*sizeof(rozo_vmove_subdirectory_t));
    pNextLevel = malloc(size);
    memset(pNextLevel,0,size);
    pNextLevel->nb    = localdir;
    pNextLevel->count = 0;
    list_init(&pNextLevel->list);
    list_push_back(&dir_list_by_level, &pNextLevel->list);

    /*
    ** Scan directories and fill the structure withh the directoris having a parent FID
    ** in the upper level
    */
    rz_scan_all_inodes(e,ROZOFS_DIR,1,rozofs_visit_directory,NULL,NULL,NULL);  
      
    /*
    ** Next level
    */    
    pPrevLevel = pNextLevel;
  }
  
  /*
  ** Allocate a table of FID to store every sub directory FID 
  */
  scanned_directories = nbFid;
  pFidTable = malloc(sizeof(fid_t)*nbFid);
  count = 0;
  
  /*
  ** Process per level
  */
  while (!list_empty(&dir_list_by_level)) {
  
    pPrevLevel = list_first_entry(&dir_list_by_level, rozo_vmove_subdirectories_t, list);
    list_remove(&pPrevLevel->list);
   
    for (idx=0; idx<pPrevLevel->count; idx++) {
      /*
      ** Do not need to add directories without file
      */
      if (pPrevLevel->dir[idx].files == 0) continue;
      rozo_vmove_insert_fid(pPrevLevel->dir[idx].fid,count);
      count++;
    }  
    free(pPrevLevel);
  }
  return 0;
} 
/*
**_______________________________________________________________________
**
** Distribute a file upon cids and sids in a round robin maner
**
** @param job   A job context to store the new distribution in
**  
** @retval 0 no match
** @retval 1 when match
**_______________________________________________________________________
*/
int rozofs_vmove_distribute_round_robin(rozofs_mover_job_t * job) {
  cluster_t             * pCluster;

  /*
  ** Get 1rst cluster in the list
  */
  pCluster  = list_first_entry(&cluster_distributor, cluster_t, list);
  
  /*
  ** Round robin sid distribution
  */
  if (do_cluster_distribute_strict_round_robin(econfig->layout,0, pCluster, &job->sid[0], 0)< 0) {
    return -1;
  }    
  job->cid = pCluster->cid;
  
  /*
  ** Put cluster at the end of the list
  */
  list_remove(&pCluster->list);
  list_push_back(&cluster_distributor, &pCluster->list);                              
  return 0;
}
/*
**_______________________________________________________________________
**
** Compare free size of 2 clusters
**_______________________________________________________________________
*/
static int cluster_compare_capacity(list_t *l1, list_t *l2) {
    cluster_t *e1 = list_entry(l1, cluster_t, list);
    cluster_t *e2 = list_entry(l2, cluster_t, list);
    return e1->free < e2->free;
} 
/*
**_______________________________________________________________________
**
** Compare free size of 2 clusters
**_______________________________________________________________________
*/
static int volume_storage_compare(list_t * l1, list_t *l2) {
  volume_storage_t *e1 = list_entry(l1, volume_storage_t, list);
  volume_storage_t *e2 = list_entry(l2, volume_storage_t, list);

  // online server takes priority
  if ((!e1->status && e2->status) || (e1->status && !e2->status)) {
      return (e2->status - e1->status);
  }
  return e1->stat.free <= e2->stat.free;
}
/*
**_______________________________________________________________________
**
** Distribute a file upon cids and sids in a round robin maner
**
** @param job   A job context to store the new distribution in
**  
** @retval 0 no match
** @retval 1 when match
**_______________________________________________________________________
*/
int rozofs_vmove_distribute_size_balancing(rozofs_mover_job_t * job) {  
  cluster_t             * pCluster;
  
  /*
  ** Get 1rst cluster in the list
  */
  pCluster  = list_first_entry(&cluster_distributor, cluster_t, list);
  
  /*
  ** Size balancing allocation
  */
  if (do_cluster_distribute_size_balancing(econfig->layout,0, pCluster, &job->sid[0], job->size, 0)< 0) {
    return -1;
  }    
  job->cid = pCluster->cid;

  list_sort(&cluster_distributor, cluster_compare_capacity);
  return 0;
} 
/*
**_______________________________________________________________________
**
** Distribute a file upon cids and sids
**
** @param job   A job context to store the new distribution in
**  
** @retval 0 no match
** @retval 1 when match
**_______________________________________________________________________
*/
static int rozofs_vmove_distribute(rozofs_mover_job_t * job) {
  /*
  ** Choose round robin for speed when fast volume destination 
  */
  if (destination==ROZOFS_VMOVE_FAST) {
    return rozofs_vmove_distribute_round_robin(job);
  }  
  /*
  ** Choose size balancing for equalizing slow volume 
  */
  return rozofs_vmove_distribute_size_balancing(job);
} 
/*
**_______________________________________________________________________
**
** RozoFS specific function for visiting a file. We have to check whether
** this is an hybrid file under one of the directory listed in pFidTable
** that needs its hybrid chunk to be moved
**
** @param exportd       pointer to exporthd data structure
** @param inode_attr_p  pointer to the inode data
** @param p             always NULL
**  
** @retval 0 no match
** @retval 1 when match
**_______________________________________________________________________
*/
int rozofs_visit(void *exportd,void *inode_attr_p,void *p) {
  ext_mattr_t        * inode_p = inode_attr_p;
  rozofs_mover_job_t * job;
  int                  ret;
  int                  idx;
  rozofs_iov_multi_t   vector;

  /*
  ** Simple file case
  */
  if (inode_p->s.multi_desc.byte == 0) {
    return 0;  
  }  
  
  /*
  ** Multiple file case. 
  */

  /*
  ** The master inode is used only in hybrid mode
  */
  if (inode_p->s.hybrid_desc.s.no_hybrid == 1) {
    return 0;
  }
    
  scanned_hybrid++;

  /*
  ** Check the directory it is in
  */
  if (whole_eid == 0) {
    for (idx=0; idx< nbFid; idx++) {  
      ret = memcmp(pFidTable[idx], inode_p->s.pfid, sizeof(fid_t));
      if (ret == 0) break;
      if (ret < 0) return 0;    
    }  
    if (idx == nbFid) return 0;
  }

  /*
  ** It is under the tree 
  */
  scanned_match++;
  
  /*
  ** Check whether it needs to be moved
  */
  if (cluster[inode_p->s.attrs.cid] == destination) {
    /*
    ** Already on the requested volume
    */
    scanned_already++;
    return 0;
  }
  
  /*
  ** Unexpected cluster identifier
  */
  if (cluster[inode_p->s.attrs.cid] == ROZOFS_VMOVE_UNDEFINED) {
    char fidstr[40];
    rozofs_fid_append(fidstr,inode_p->s.attrs.fid);
    severe("Found cid %d for FID %s",inode_p->s.attrs.cid, fidstr);
    scanned_error++;    
    return 0;
  }  

  scanned_to_move++;
  
  /*
  ** Allocate a job for the mover
  */
  job = malloc(sizeof(rozofs_mover_job_t));
  memset(job,0,sizeof(rozofs_mover_job_t));

  /*
  ** Get size of the hybrid chunk
  */
  vector.vectors[0].len = 0;
  rozofs_get_multiple_file_sizes(inode_p, &vector);
  job->size = vector.vectors[0].len;
  
  /*
  ** San until all credit is consumed
  */
  if (job->size > rozofs_vmove_size2move) {
    rozo_lib_stop_scanning();
    rozofs_vmove_size2move = 0;
  }
  else {
    rozofs_vmove_size2move -= job->size;
  }
    
  /*
  ** Get a new distribution
  */
  int retval = rozofs_vmove_distribute(job);
  if (retval < 0) {
    scanned_to_move--;
    scanned_error++;
    char fidstr[40];
    rozofs_fid_append(fidstr,inode_p->s.attrs.fid);
    warning("rozofs_vmove_distribute %s\n",fidstr);
    free(job); 
    return 0;   
  }
  
  job->name = malloc(sizeof(fid_t));
  memcpy(job->name,inode_p->s.attrs.fid,sizeof(fid_t));
  list_push_back(&jobs,&job->list);
  return 1;
}  
/*
**_______________________________________________________________________
**
** Retrieve an export configuration
**
** @param eid    Export identifier to retrieve configuration for
**    
** @retval the export configuration or NULL
**_______________________________________________________________________
*/
export_config_t * rozofs_get_eid_configuration(int eid) {
  list_t          * e=NULL;
  export_config_t * econfig;
  
  /*
  ** Loop on export described in the configuration to find out 
  ** the input eid
  */
  list_for_each_forward(e, &exportd_config.exports) {
    econfig = list_entry(e, export_config_t, list);
    if (econfig->eid == eid) return econfig;
  }
  return NULL;   
}
/*
**__________________________________________________________________
** Load in memory the file that corresponds to a volume/cluster
**
** @param vid            volume identifier
** @param cid            cluster identifier
**  
** @retval 0 success
** @retval -1 error
**__________________________________________________________________  
*/
int rozofs_vmove_load_cluster_stats(int vid,int cid){
  char                        path[FILENAME_MAX];
  int                         fd_v=-1;
  ssize_t                     size;
  int                         ret;
  struct stat                 stat_buf_before;
  struct stat                 stat_buf_after;
  int                         i;
  
  /*
  ** Initialize cluster_stats structure
  */
  memset(&cluster_stats,0,sizeof(export_vol_cluster_stat2_t));
  
  sprintf(path, DAEMON_PID_DIRECTORY"exportd/volume_%d_cluster_%d.bin", vid, cid);
  for (i = 0; i < 10; i++) {
  
    if (i!=0) sleep(1);
    if (fd_v!=-1) {
       close(fd_v);
       fd_v=-1;
    }
    
    if ((fd_v = open(path, O_RDONLY, S_IRWXU | S_IROTH)) < 0) {
      severe("open failure %s : %s\n",path,strerror(errno));
      continue;
    } 
    
    ret = fstat(fd_v,&stat_buf_before);
    if (ret < 0) continue;
    
    size = pread(fd_v,&cluster_stats,sizeof(export_vol_cluster_stat2_t),0);
    if ( size < 0) {
      severe("read failure %s : %s\n",path,strerror(errno));
      continue;
    }
    /*
    ** Check the file size 
    */
    if (size != sizeof(export_vol_cluster_stat2_t)) continue;

    /*
    ** re-check the mtime
    */
    ret = fstat(fd_v,&stat_buf_after);
    if (ret < 0) continue;
    if  (stat_buf_after.st_mtime != stat_buf_before.st_mtime) continue; 
             close(fd_v);
    return 0;
  }     
  errno=EINVAL;     
  return -1;
}
/*
**_______________________________________________________________________
**
** Get cid/sid free and total space from cluster stats on disk
**
** @param vid_fast    Fast volume vid
** @param vid_slow    Slow volume vid
**_______________________________________________________________________
*/
int rozofs_vmove_set_disk_space(int vid,int cid,volume_storage_t * pStorage){
  int idx;
  
  pStorage->stat.free = 0;
  pStorage->stat.size = 0; 
  pStorage->status = 0;

  /*
  ** Read cluster stats if not yet done
  */
  if (cluster_stats.cluster_id != cid) {
    if (rozofs_vmove_load_cluster_stats(vid,cid) < 0) {
      severe("Can not read vid %d cid %d statistics %s",vid,cid,strerror(errno));
      return -1;
    }
  }
  for (idx=0; idx<cluster_stats.nb_sid; idx++) {
    if (cluster_stats.sid_tab[idx].sid == pStorage->sid) {
      pStorage->stat.free = cluster_stats.sid_tab[idx].free_size_bytes;
      pStorage->stat.size = cluster_stats.sid_tab[idx].total_size_bytes;
      pStorage->status = 1;
      return 0;    
    }
  }
  severe("cid/sid %d/%d not found",cid,pStorage->sid);  
  return -1;  
} 
/* 
**_______________________________________________________________________
**
** Upgrade size of storaged for further size balancing
**
** @param vid_slow    Slow volume vid
**_______________________________________________________________________
*/
void rozofs_upgrade_storage_size(vid_t vid_slow) {
  cluster_t             * pCluster;
  volume_storage_t      * pStorage;
  list_t                * c, * s;

  if (destination != ROZOFS_VMOVE_SLOW) return;
  
  /*
  ** Force rereading the cluster statistics
  */
  cluster_stats.cluster_id = 0;
 
  /*
  ** Loop on clusters of this volume
  */
  list_for_each_forward(c, &cluster_distributor) {
        
    pCluster = list_entry(c, cluster_t, list);
    pCluster->size = 0;
    pCluster->free = 0;
     
    list_for_each_forward(s, &pCluster->storages[0]) {
      
      pStorage = list_entry(s, volume_storage_t, list);
      rozofs_vmove_set_disk_space(vid_slow, pCluster->cid, pStorage);
      pCluster->size += pStorage->stat.size;
      pCluster->free += pStorage->stat.free;
    }
    list_sort(&pCluster->storages[0], volume_storage_compare);     
  }
  list_sort(&cluster_distributor, cluster_compare_capacity);
}   
/*
**_______________________________________________________________________
**
** Build the list of cluster of volume fast and slow
**
** @param vid_fast    Fast volume vid
** @param vid_slow    Slow volume vid
**_______________________________________________________________________
*/
void rozofs_build_cluster_list(vid_t fid_fast, vid_t vid_slow) {
  list_t                * v=NULL;
  volume_config_t       * vconfig;
  list_t                * c=NULL;  
  cluster_config_t      * cconfig;
  list_t                * s=NULL;
  storage_node_config_t * sconfig;
  int                     val;

  cluster_t             * pCluster;
  volume_storage_t      * pStorage;
  
  /*
  ** Reset memory area to read cluster statistics from disk
  */
  memset(&cluster_stats,0,sizeof(cluster_stats));
  
  /*
  ** Loop on configured volumes to find out the fast as well as the slow volumes 
  */
  list_for_each_forward(v, &exportd_config.volumes) {
  
    vconfig = list_entry(v, volume_config_t, list);
    if ((vconfig->vid == fid_fast)||(vconfig->vid == vid_slow)) {
      
      
      if (vconfig->vid == fid_fast) val = ROZOFS_VMOVE_FAST;
      else                          val = ROZOFS_VMOVE_SLOW;
      
      /*
      ** Loop on clusters of this volume
      */
      list_for_each_forward(c, &vconfig->clusters) {
      
        cconfig = list_entry(c, cluster_config_t, list);
        cluster[cconfig->cid] = val;
        
        /*
        ** For the destination volume, setup a structure for distributing the files
        */
        if (destination == val) {
        
          pCluster = malloc(sizeof(cluster_t));
          memset(pCluster,0,sizeof(cluster_t));
          pCluster->cid = cconfig->cid;
          list_init(&pCluster->list);
          list_init(&pCluster->storages[0]);
          
          /*
          ** Loop on storages of this cluster
          */
          list_for_each_forward(s, &cconfig->storages[0]) {
            sconfig = list_entry(s, storage_node_config_t, list);
            pStorage =  malloc(sizeof(volume_storage_t));
            memset(pStorage,0,sizeof(volume_storage_t));   
            volume_storage_initialize(pStorage, 
	                              sconfig->sid, 
			              sconfig->host,
				      sconfig->host_rank,
				      sconfig->siteNum);  
            list_push_back(&pCluster->storages[0], &pStorage->list);                                          
          }
          list_push_back(&cluster_distributor, &pCluster->list);                              
        }
      }  
    }
  }
}
/*
**_______________________________________________________________________
** Display error and usage
**_______________________________________________________________________
*/
static void usage(char * fmt, ...) {
  va_list   args;
  char      error_buffer[512];

  /*
  ** Display optionnal error message if any
  */
  if (fmt) {
    va_start(args,fmt);
    vsprintf(error_buffer, fmt, args);
    va_end(args);   
    printf("\n"ROZOFS_COLOR_BOLD""ROZOFS_COLOR_RED"!!!  %s !!! "ROZOFS_COLOR_NONE"\n",error_buffer);
    exit(EXIT_FAILURE);      
  }
  
  printf("RozoFS hybrid volume mover - %s\n", VERSION);    
  printf("Move hybrid chunks of hybrid files betwwen slow and fast volumes.\n");
  printf("Usage: "ROZOFS_COLOR_BOLD"rozo_vmove {--fast|--slow} {--fid <directory FID>|--name <directory name>|--eid <eid>} [--recursive] [--throughput <MiB>]"ROZOFS_COLOR_NONE"\n");
  printf("The direction of the move is defined by one of the following mandatory parameter:\n");
  printf(ROZOFS_COLOR_BOLD"\t-f, --fast"ROZOFS_COLOR_NONE"\t\t\tMove hybrid chunks from slow volume to fast volume.\n");
  printf(ROZOFS_COLOR_BOLD"\t-s, --slow"ROZOFS_COLOR_NONE"\t\t\tMove hybrid chunks from fast volume to slow volume.\n");
  printf("The files to move are defined as follow:\n");
  printf(ROZOFS_COLOR_BOLD"\t-n, --name <directory name>"ROZOFS_COLOR_NONE"\tRoot directory name on which the move applies.\n");  
  printf(ROZOFS_COLOR_BOLD"\t-F, --fid <directory FID>"ROZOFS_COLOR_NONE"\tRoot directory FID on which the move applies.\n");  
  printf(ROZOFS_COLOR_BOLD"\t-e, --eid <eid>"ROZOFS_COLOR_NONE"\t\t\tWhole eid must be moved (--recursive is implicit).\n");  
  printf(ROZOFS_COLOR_BOLD"\t-r, --recursive"ROZOFS_COLOR_NONE"\t\t\tApply vmove recursively on each sub-directory under the root directory.\n");
  printf("Miscellaneous options:\n");
  printf(ROZOFS_COLOR_BOLD"\t-t, --throughput"ROZOFS_COLOR_NONE"\t\tThroughput limitation of the mover.\n");
  printf(ROZOFS_COLOR_BOLD"\t-h, --help"ROZOFS_COLOR_NONE"\t\t\tprint this message.\n");
  printf(ROZOFS_COLOR_BOLD"\t-c, --config <filename>"ROZOFS_COLOR_NONE"\t\tExportd configuration file name (when different from %s)\n",EXPORTD_DEFAULT_CONFIG);  
  printf("\n");
  exit(EXIT_SUCCESS); 
}
/*
**_______________________________________________________________________
**
** Get FID of a directory name
**
** @param dname    Directory name to resolve
** @param fid      Where to store the resolved FID
**_______________________________________________________________________
*/
void rozofs_vmove_get_fid_from_name(char * dname, fid_t fid) {
  char    * p;
  ssize_t   size;

  /*
  ** Read Rozofs extended attribute
  */
  size = getxattr(dname,"user.rozofs",printfBuffer,sizeof(printfBuffer));    
  if (size == -1) {
    usage("%s is not a RozoFS directory",dname);
  }    
  
  if (strstr(printfBuffer,"MODE    : DIRECTORY") == NULL) {
    usage("%s is not a directory",dname);
  }    
    
  p = strstr(printfBuffer,"FID     :");
  if (p == NULL) {
    usage("Bad rozofs extended attribute %s",dname);
  }    
  p += strlen("FID     : ");
  
  if (rozofs_uuid_parse(p,fid)<0) {
    usage("Unexpected FID");
  }    
}
/*
**_______________________________________________________________________
** MAIN
**_______________________________________________________________________
*/
int main(int argc, char *argv[]) {
  int                c;
  char             * configFileName = EXPORTD_DEFAULT_CONFIG;
  char             * directory = NULL;
  int                eid;
  rozofs_inode_t   * ino_p;
  long long unsigned int throughput = 0;
  fid_t              parent;
  
  /*
  ** Change local directory to "/"
  */
  if (chdir("/")!= 0) {}

  list_init(&cluster_distributor);
  
  /*
  ** Check user is root
  */
  if (getuid() != 0) {
    usage("You need to be root");
  }      

  /*
  ** read common config file
  */
  common_config_read(NULL);    

  /*
  ** Get utility name and record it for syslog
  */
  utility_name = basename(argv[0]);   
  uma_dbg_record_syslog_name(utility_name);

  /*
  ** Set a signal handler
  */
  rozofs_signals_declare(utility_name, common_config.nb_core_file); 

  rozofs_mover_init();        

  static struct option long_options[] = {
      {"help", no_argument, 0, 'h'},
      {"fast", no_argument, 0, 'f'},
      {"slow", no_argument, 0, 's'},
      {"config", required_argument, 0, 'c'},
      {"throughput", required_argument, 0, 't'},
      {"name", required_argument, 0, 'n'},
      {"fid", required_argument, 0, 'F'},
      {"eid", required_argument, 0, 'e'},
      {"recursive", no_argument, 0, 'r'},
      {0, 0, 0, 0}
  };


  while (1) {

    int option_index = -1;
    c = getopt_long(argc, argv, "rhfsc:n:F:e:t:", long_options, &option_index);

    if (c == -1)
        break;

    switch (c) {

      case 'h':
        usage(NULL);
        break;

      case 'f':
        if (destination == ROZOFS_VMOVE_SLOW) {
          usage ("--fast and --slow are incompatible");
        }
        destination = ROZOFS_VMOVE_FAST;
        break;

      case 's':
        if (destination == ROZOFS_VMOVE_FAST) {
          usage ("--fast and --slow are incompatible");
        }
        destination = ROZOFS_VMOVE_SLOW;
        break;

      case 'c':
        configFileName = optarg;
        break;

      case 'F':
        directory = optarg;
        if (rozofs_uuid_parse(optarg,parent) != 0) {
          usage("Bad FID format \"%s\"",optarg);
        }  
        break;
        
      case 'n':
        directory = optarg;
        rozofs_vmove_get_fid_from_name(optarg,parent);
        break;

      case 'e':
        directory = optarg;
        if (sscanf(optarg,"%d", &whole_eid)!= 1) {
          usage("Bad eid number %s", optarg);
        }  
        break;

      case 'r':
        recursive = 1; 
        break;
        
      case 't':
        if (sscanf(optarg,"%llu",&throughput) != 1) {
          usage("Bad throughput format \"%s\"",optarg);
        }  
        break;
                
      default:
        usage("Unexpected argument \"%s\"",c);
        break;
    }
  }
  
  if (directory == NULL) {
    usage("Missing mandatory --dir option");
  }
  if (destination == ROZOFS_VMOVE_UNDEFINED) {  
    usage("Missing either --fast or --slow option");
  }

  /*
  ** Read the configuration file
  */
  if (econfig_initialize(&exportd_config) != 0) {
    severe("EXIT : can't initialize exportd config %s.",strerror(errno));
    exit(EXIT_FAILURE);  
  }    
  if (econfig_read(&exportd_config, configFileName) != 0) {
    severe("EXIT : failed to parse configuration file %s %s.",configFileName,strerror(errno));
    exit(EXIT_FAILURE);  
  }   

  /*
  ** Extract eid from directory 
  */
  if (whole_eid == 0) {
    ino_p = (rozofs_inode_t*) parent;
    eid = ino_p->s.eid;
  }
  else {
    eid = whole_eid;
  }  
  
  /*
  ** Find out this edi in the configuration
  */
  econfig = rozofs_get_eid_configuration(eid); 
  if (econfig == NULL) {
    usage("eid %d not found in configuration",eid);
  }
  if (econfig->vid_fast == 0xFFFF) {
    usage("eid %d only uses volume %d and no fast volume",eid,econfig->vid);
  }
  /*
  ** Build list of cluster of each volumes
  */
  rozofs_build_cluster_list(econfig->vid_fast, econfig->vid);

  /*
  ** init of the lv2 cache
  */
  lv2_cache_initialize(&cache);
  rz_set_verbose_mode(0);
  rozofs_export_p = rz_inode_lib_init(econfig->root);
  if (rozofs_export_p == NULL) {
    severe("EXIT : RozoFS: error while reading %s",econfig->root);
    exit(EXIT_FAILURE);  
  }

  /*
  ** start scanning from beginning 
  */
  list_init(&jobs);
  list_init(&subdirectories);
  
  /*
  ** Build the list of diredtory to scan for files
  */
  if (whole_eid == 0) {
    rozofs_build_directory_list(rozofs_export_p,parent,recursive);
  }
     
  rozo_lib_reset_index_context(&scan_context);
  while (1) {  

    /*
    ** Upgrade the size of the storage when moving toward slow volume
    ** in order to process size balancing
    */
    rozofs_upgrade_storage_size(econfig->vid);

    /*
    ** Move up to 1G per round
    */
    rozofs_vmove_size2move =  1024 * 1024 * 1024;
    
    /*
    ** Scan all files in the directory
    */
    rz_scan_all_inodes_from_context(rozofs_export_p,ROZOFS_REG,1,rozofs_visit,NULL,NULL,NULL,&scan_context);  
    rozo_lib_save_index_context(&scan_context);
    /*
    ** No more file to move 
    */    
    if (list_empty(&jobs)) break;
    
    /*
    ** Move the inut files
    */
    rozofs_do_move_one_export_fid_mode("localhost", econfig->root, throughput, &jobs); 
    rozofs_mover_print_stat(printfBuffer);
    printf("%s",printfBuffer);       

  }  

  printf("{ \"vmove\" : {\n");
  printf("     \"directory\"      : \"%s\",\n", directory);
  printf("     \"eid\"            : %d,\n", eid);
  printf("     \"vid fast\"       : %d,\n", econfig->vid_fast);
  printf("     \"vid slow\"       : %d,\n", econfig->vid);
  printf("     \"destination\"    : \"%s\",\n", (destination==ROZOFS_VMOVE_FAST)?"fast":"slow");  
  printf("     \"throughput MB\"  : %llu,\n", throughput);  
  printf("     \"directories\"    : %llu,\n",(long long unsigned int)scanned_directories);
  printf("     \"hybridFiles\"    : %llu,\n",(long long unsigned int)scanned_hybrid);
  printf("     \"match\"          : %llu,\n",(long long unsigned int)scanned_match);
  printf("     \"alreadyInPlace\" : %llu,\n",(long long unsigned int)scanned_already);
  printf("     \"errors\"         : %llu,\n",(long long unsigned int)scanned_error);
  printf("     \"toMove\"         : %llu,\n",(long long unsigned int)scanned_to_move);
  printf("}}\n");
  exit(EXIT_SUCCESS);  
  return 0;
}
