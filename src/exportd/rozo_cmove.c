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


typedef struct _rozo_cmove_ctx_t
{
  char *configFileName;              /**< export configuration file                */
  int rebalance_frequency;           /**< rebalance polling frequency              */
  int volume_id;                     /**< volume for which rebalancing is applied  */
  uint16_t debug_port;               /**< TCP port used for rozodiag               */
  uint16_t instance;                 /**< instance of the rebalancing process      */
  int number_of_eid_in_volume;       /**< number of eids in the volume             */
  int current_eid_idx;               /**< current export index                     */
  int max_scanned;                   /**< max file scanned before a move           */
  uint64_t min_filesize_config;      /**< min file size in Bytes                   */
  uint64_t max_filesize_config;      /**< max file size in Bytes                   */
  uint64_t max_move_size_config;     /**< max move size                            */
  int continue_on_balanced_state;    /**< assert to one if the process should continue while reaching balanced state */
  int verbose;                        /**< assert to one for verbose mode */
  long long unsigned int throughput;  /**< file throughput in MBytes/s    */
  /*
  ** statistics 
  */
  int64_t scanned_file_cpt;
  int64_t current_scanned_file_cpt;
  int64_t scanned_error;
  int    nb_threads;
} rozo_cmove_ctx_t;


#define ROZOFS_CMOVE_MAX_SCANNED_DEFAULT  1000

#define ROZOFS_VMOVE_UNDEFINED  0
#define ROZOFS_CMOVE_FROZEN       1


int                destination = ROZOFS_VMOVE_UNDEFINED;
int                cluster[ROZOFS_CLUSTERS_MAX] = {ROZOFS_VMOVE_UNDEFINED};
list_t             cluster_distributor;
export_config_t  * econfig = NULL;
export_config_t * volume_export_table[EXPGW_EID_MAX_IDX];   /**< table of eids associated with a volume  */
char *            mount_path[EXPGW_EID_MAX_IDX];

uint64_t  scanned_match_path   = 0;
uint64_t  scanned_hybrid  = 0;
uint64_t  scanned_already = 0;
uint64_t  scanned_to_move = 0;
uint64_t  scanned_error   = 0;
uint64_t  scanned_directories = 0;
uint64_t  scanned_over_sized  = 0;
uint64_t  scanned_under_sized = 0;
rozo_cmove_ctx_t  rozo_cmove_ctx;     /**< Cluster Move context */

long long unsigned int  size_over  = 0;
long long unsigned int  size_under  = 0xFFFFFFFFFFFFFFFF;
uint8_t   volume_layout;
char printfBuffer[4096*4];

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
int                         vid2move = 0;
int scanned_current_count = 0;
int all_export_scanned_count = 0;


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
int rozofs_cmove_distribute_round_robin(rozofs_mover_job_t * job) {
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
    /*
    ** Put non in service clusters at the end of the list
    */
    if (e1->adminStatus != rozofs_cluster_admin_status_in_service) return 1;
    if (e2->adminStatus != rozofs_cluster_admin_status_in_service) return 0;

    return e1->free < e2->free;
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
int rozofs_cmove_distribute_size_balancing(rozofs_mover_job_t * job) {  
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
static int rozofs_cmove_distribute(rozofs_mover_job_t * job) {
  /*
  ** Always in round robin mode
  */
  return rozofs_cmove_distribute_round_robin(job);
} 


/*
**_______________________________________________________________________
**
** RozoFS specific function for visiting a file. We have to check whether
** this is an hybrid file under one of the directory listed in pFidTable
** that needs its hybrid stride to be moved
**
** @param exportd       pointer to exporthd data structure
** @param inode_attr_p  pointer to the inode data
** @param p             always NULL
**  
** @retval 0 no match
** @retval 1 when match
**_______________________________________________________________________
*/
int rozofs_visit_single(void *exportd,void *inode_attr_p,void *p) {

  ext_mattr_t        * inode_p = inode_attr_p;
  rozofs_mover_job_t * job;


  if (cluster[inode_p->s.attrs.cid] != ROZOFS_CMOVE_FROZEN) {
    /*
    ** No a frozen cluster, so ignore it
    */
    return 0;
  }
  rozo_cmove_ctx.current_scanned_file_cpt++;   
  
  /*
  ** Allocate a job for the mover
  */
  job = malloc(sizeof(rozofs_mover_job_t));
  memset(job,0,sizeof(rozofs_mover_job_t));
  job->size = inode_p->s.attrs.size;


  /*
  ** Get a new distribution
  */
  int retval = rozofs_cmove_distribute(job);
  if (retval < 0) {
    rozo_cmove_ctx.scanned_error++;
    char fidstr[40];
    rozofs_fid_append(fidstr,inode_p->s.attrs.fid);
    warning("rozofs_cmove_distribute %s\n",fidstr);
    free(job); 
    return 0;   
  }
  
  job->name = malloc(sizeof(fid_t));
  memcpy(job->name,inode_p->s.attrs.fid,sizeof(fid_t));
  
  scanned_current_count++;  
  list_push_back(&jobs,&job->list);

  if (scanned_current_count > rozo_cmove_ctx.max_scanned)
  {  
    rozo_lib_stop_scanning();
  } 

  return 1;
  
}

typedef enum _INODE_TYPE_E {
  INODE_TYPE_SINGLE,
  INODE_TYPE_HYBRID_MASTER,
  INODE_TYPE_HYBRID_SLAVE,
  INODE_TYPE_SLAVE,
} INODE_TYPE_E;
/*
**_______________________________________________________________________
**
** RozoFS specific function for visiting a sub-file. 
** 
** @param inode_type     The type of visited inode
** @param master_p       pointer to the master inode where information such as 
**                       mode, dates are relevent
** @param slave_p        pointer to the sub-file (may be the master inode)
**                       where cid/sid are relevent
**  
** @retval 0 no match
** @retval 1 when match
**_______________________________________________________________________
*/
int rozofs_do_visit(INODE_TYPE_E inode_type,ext_mattr_t *master_p,ext_mattr_t * slave_p, uint64_t size) {

  rozofs_mover_job_t * job;


  if (cluster[slave_p->s.attrs.cid] != ROZOFS_CMOVE_FROZEN) {
    /*
    ** Not a frozen cluster, so ignore it
    */
    return 0;
  }
  rozo_cmove_ctx.current_scanned_file_cpt++;   
  
  /*
  ** Allocate a job for the mover
  */
  job = malloc(sizeof(rozofs_mover_job_t));
  memset(job,0,sizeof(rozofs_mover_job_t));
  job->size = size;


  /*
  ** Get a new distribution
  */
  int retval = rozofs_cmove_distribute(job);
  if (retval < 0) {
    rozo_cmove_ctx.scanned_error++;
    char fidstr[40];
    rozofs_fid_append(fidstr,slave_p->s.attrs.fid);
    warning("rozofs_cmove_distribute %s\n",fidstr);
    free(job); 
    return 0;   
  }
  
  job->name = malloc(sizeof(fid_t));
  memcpy(job->name,slave_p->s.attrs.fid,sizeof(fid_t));
  
  scanned_current_count++;  
  list_push_back(&jobs,&job->list);

  if (scanned_current_count > rozo_cmove_ctx.max_scanned)
  {  
    rozo_lib_stop_scanning();
  } 

  return 1;
}
/*
**_______________________________________________________________________
*/
/**
*   RozoFS specific function for visiting

   @param inode_attr_p: pointer to the inode data
   @param exportd : pointer to exporthd data structure
   @param p: always NULL
   
   @retval 0 no match
   @retval 1 match
*/

int rozofs_visit(void *exportd,void *inode_attr_p,void *p) {
  ext_mattr_t *inode_p = inode_attr_p;
  ext_mattr_t *slave_p = inode_p;  
  int          idx;
  int          nb_slave;
  int          match = 0;
  rozofs_iov_multi_t vector; 

  /*
  ** Only move regular files
  */
  if (!S_ISREG(inode_p->s.attrs.mode)) {
    return 0;
  }  

  /*
  ** get the size of each section
  */
  rozofs_get_multiple_file_sizes(inode_p,&vector);
   
  /*
  ** Is this file hybrid or striped...
  */
  if (inode_p->s.multi_desc.byte == 0) {
    /*
    ** Regular file without striping
    ** inode_attr_p and slave_p are the same
    */
    match += rozofs_do_visit(INODE_TYPE_SINGLE,inode_attr_p,slave_p, vector.vectors[0].len);
  }  
  else if (inode_p->s.multi_desc.common.master == 1) {
    /*
    ** When not in hybrid mode 1st inode has no distribution
    */
    if (inode_p->s.hybrid_desc.s.no_hybrid== 0) {
      match += rozofs_do_visit(INODE_TYPE_HYBRID_MASTER,inode_attr_p,slave_p, vector.vectors[0].len);
    }  
    /*
    ** Check every slave
    */
    slave_p ++;
    nb_slave = rozofs_get_striping_factor(&inode_p->s.multi_desc);
    for (idx=0; idx<nb_slave; idx++,slave_p++) {
      if (inode_p->s.hybrid_desc.s.no_hybrid==0) {
         match += rozofs_do_visit(INODE_TYPE_HYBRID_SLAVE,inode_attr_p,slave_p, vector.vectors[idx+1].len);
      }  
      else{
         match += rozofs_do_visit(INODE_TYPE_SLAVE,inode_attr_p,slave_p,vector.vectors[idx+1].len);
      }  
    }  
  }
  if(match) return 1;
  return 0;
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
** Build the list of cluster of the volume:
**   It registers the in service cluster to be able to allocate new set of sid
**   and it keepe track of the frozen cluster that in for the cluster move the source.
**
** @param vid2move    volume vid


  @retval 0 on success
  @retval -1 on error (see errno for details)
**_______________________________________________________________________
*/
int rozofs_build_cluster_list(vid_t vid2move) {
  list_t                * v=NULL;
  volume_config_t       * vconfig;
  list_t                * c=NULL;  
  cluster_config_t      * cconfig;
  list_t                * s=NULL;
  storage_node_config_t * sconfig;

  cluster_t             * pCluster;
  volume_storage_t      * pStorage;
  int                     ret = -1;
  
  errno = ENOENT;
  /*
  ** Reset memory area to read cluster statistics from disk
  */
  memset(&cluster_stats,0,sizeof(cluster_stats));
  
  /*
  ** Loop on configured volumes to find out the fast as well as the slow volumes 
  */
  list_for_each_forward(v, &exportd_config.volumes) {
  
    vconfig = list_entry(v, volume_config_t, list);
    if (vconfig->vid != vid2move) continue;
    
    /*
    ** Save the default layout of the volume
    */
    volume_layout = vconfig->layout;

    /*
    ** Loop on clusters of this volume
    */
    list_for_each_forward(c, &vconfig->clusters) {

      cconfig = list_entry(c, cluster_config_t, list);        
      /*
      ** Mark the frozen clusters since they are the source of the cluster move
      */
      if (cconfig->adminStatus == rozofs_cluster_admin_status_frozen)
      {
	cluster[cconfig->cid] = ROZOFS_CMOVE_FROZEN;
	ret = 0;	
	continue;
      }
      /*
      ** We keep only the clusters that are in service
      */
      if (cconfig->adminStatus != rozofs_cluster_admin_status_in_service) continue;        

      pCluster = malloc(sizeof(cluster_t));
      memset(pCluster,0,sizeof(cluster_t));
      pCluster->cid         = cconfig->cid;
      pCluster->adminStatus = cconfig->adminStatus;
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
  
  return ret;
}


/*
**_______________________________________________________________________
*/
/**
*   Build the list of the eid that are associated with the volume
   
    @param vid
    
    @retval number of eid
*/

int build_eid_table_associated_with_volume(vid_t vid)
{
  list_t          * e=NULL;
  export_config_t * econfig;
  int index=0;
  
  list_for_each_forward(e, &exportd_config.exports) {

    econfig = list_entry(e, export_config_t, list);
    
    if ((econfig->vid == vid) || (econfig->vid_fast == vid))
    {
       volume_export_table[index] =  econfig;
       if (econfig->layout == 0xff)  econfig->layout =volume_layout;
       index++;
    } 
  }
  return index;   
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
  
  printf("RozoFS Cluster mover - %s\n", VERSION);    
  printf("Move files from frozen Clusters within a Volume\n");
  printf("Usage: "ROZOFS_COLOR_BOLD"rozo_cmove --vid <vid> [--threads <count>] [--throughput <MiB>] [--mount <path> ... --mount <path>]"ROZOFS_COLOR_NONE"\n");

  printf(ROZOFS_COLOR_BOLD"\t-v, --vid <vid>"ROZOFS_COLOR_NONE"\t\t\tVolume to which the Cluster move applies\n");  
  printf("Miscellaneous options:\n");
  printf(ROZOFS_COLOR_BOLD"\t-t, --throughput"ROZOFS_COLOR_NONE"\t\tThroughput limitation of the mover.\n");
  printf(ROZOFS_COLOR_BOLD"\t-m, --mount"ROZOFS_COLOR_NONE"\t\t\tA mountpoint to use for one eid\n");
  printf(ROZOFS_COLOR_BOLD"\t-T, --threads"ROZOFS_COLOR_NONE"\t\t\tThe number of threads of the file mover (default %u)\n",ROZOFS_MAX_MOVER_THREADS);
  printf(ROZOFS_COLOR_BOLD"\t-h, --help"ROZOFS_COLOR_NONE"\t\t\tprint this message.\n");
  printf(ROZOFS_COLOR_BOLD"\t-c, --config <filename>"ROZOFS_COLOR_NONE"\t\tExportd configuration file name (when different from %s)\n",EXPORTD_DEFAULT_CONFIG);  
  printf("\n");
  exit(EXIT_SUCCESS); 
}
/*
**_______________________________________________________________________
**
** Get eid valie from mount point path
**
** @param mountpath     Mount point path name
**
** @retval -1 in case of error / eid value on success
**_______________________________________________________________________
*/
int rozofs_vmove_get_eid_from_path(char * mountpath) {
  int    ret;
  char   xattrValue[1024];
  char * p=xattrValue;
  int    eid;
  
  ret = getxattr(mountpath,"user.rozofs.export",xattrValue,sizeof(xattrValue));
  if (ret <= 0) {
    return -1;
  } 
  if (ret >= sizeof(xattrValue)) {
    ret = sizeof(xattrValue)-1;
  }
  xattrValue[ret] = 0;
  
  while ((*p!=0) && (*p!= ' ')) p++;
  if (*p == 0) return -1;
  
   ret = sscanf(p, "%d",&eid);
   if (ret != 1) return -1;
   
   return eid;
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
  int                ret;
  int                start;
  char              *root_path=NULL; 
  uint32_t           eid; 
  char               mover_path[1024];
  
  memset(&rozo_cmove_ctx,0,sizeof(rozo_cmove_ctx));
  rozo_cmove_ctx.nb_threads = ROZOFS_MAX_MOVER_THREADS;
  memset(mount_path,0,sizeof(mount_path));
  
  rozo_cmove_ctx.max_scanned = ROZOFS_CMOVE_MAX_SCANNED_DEFAULT;
  /*
  ** Change local directory to "/"
  */
  if (chdir("/")!= 0) {}

  list_init(&cluster_distributor);
  
  
  sprintf(mover_path,"%s/mover",ROZOFS_KPI_ROOT_PATH);
  ret = rozofs_kpi_mkpath(mover_path);
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

 

  static struct option long_options[] = {
      {"help", no_argument, 0, 'h'},
      {"config", required_argument, 0, 'c'},
      {"throughput", required_argument, 0, 't'},
      {"vid", required_argument, 0, 'v'},      
      {"mount", required_argument, 0, 'm'},      
      {"threads", required_argument, 0, 'T'},
      
      {0, 0, 0, 0}
  };


  while (1) {

    int option_index = -1;
    c = getopt_long(argc, argv, "hc:t:v:m:T:", long_options, &option_index);

    if (c == -1)
        break;

    switch (c) {

      case 'h':
        usage(NULL);
        break;

      case 'c':
        configFileName = optarg;
        break;
       

      case 'v':
        if (sscanf(optarg,"%d", &vid2move)!= 1) {
          usage("Bad vid number %s", optarg);
        }  
        break;
        
      case 'm':
        /*
        ** Find out eid from the given mount path
        */
        eid = rozofs_vmove_get_eid_from_path(optarg);
        if (eid == -1) {
          usage("Bad RozoFS mount point \"%s\"",optarg);
        }     
        if (mount_path[eid] != NULL) {
          usage("2 RozoFS mount points for eid %d : \"%s and \"%s\"",eid,optarg,mount_path[eid]);
        }     
	mount_path[eid] = optarg;	 
        break;
      case 't':
        if (sscanf(optarg,"%llu",&rozo_cmove_ctx.throughput) != 1) {
          usage("Bad throughput format \"%s\"",optarg);
        }  
        break;        

      case 'T':
        if (sscanf(optarg,"%d",&rozo_cmove_ctx.nb_threads) != 1) {
          usage("Bad threads parameter \"%s\"",optarg);
        } 
	if (rozo_cmove_ctx.nb_threads <= 0)
	{
          usage("Bad threads parameter \"%s\"",optarg);	
	} 
	if (rozo_cmove_ctx.nb_threads > ROZOFS_MAX_MOVER_THREADS)
	{
          rozo_cmove_ctx.nb_threads = ROZOFS_MAX_MOVER_THREADS;	
	} 
        break; 
      default:
        usage("Unexpected argument");
        break;
    }
  }
  
  if (vid2move == 0) {
    usage("Volume identifier (vid) is missing");  
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
  ** Build list of the frozen clusters
  */
  ret = rozofs_build_cluster_list(vid2move);
  if (ret != 0){
    info("EXIT : No frozen Cluster on VID %d",vid2move);
    printf("No frozen Cluster on VID %d\n",vid2move);
    exit(EXIT_FAILURE);    
  }

  /**
  * check if the volume is defined in the configuration file
  */  
  rozo_cmove_ctx.number_of_eid_in_volume = build_eid_table_associated_with_volume(vid2move); 
  if ( rozo_cmove_ctx.number_of_eid_in_volume <=0)
  {
       severe("EXIT : there is no eid associated with volume identified (%d) in the exportd configuration file (%s)",vid2move,rozo_cmove_ctx.configFileName);
       exit(EXIT_FAILURE);  
  }
  rozo_cmove_ctx.current_eid_idx = -1;

  rozofs_mover_init_th(mover_path,rozo_cmove_ctx.throughput,rozo_cmove_ctx.nb_threads);       

  /*
  ** init of the lv2 cache
  */
  lv2_cache_initialize(&cache);
  rz_set_verbose_mode(0);

  /*
  ** start scanning from beginning 
  */
  list_init(&jobs);

   /*
  ** Main loop
  */
   for(;;)
   {


      if (start == 0) sleep(rozo_cmove_ctx.rebalance_frequency);
      start = 0;
      
      /*
      ** need to get the inode from the exportd associated with the volume
      */
      if (rozo_cmove_ctx.current_eid_idx < 0)
      {
         /*
	 ** get the path towards the first eid
	 */
	 rozo_cmove_ctx.current_eid_idx = 0;
         econfig = volume_export_table[rozo_cmove_ctx.current_eid_idx];
	 root_path = econfig->root;
	 if (root_path == NULL)
	 {
	   severe("EXIT : eid %d does not exist",econfig->eid);
	   exit(EXIT_FAILURE); 
	 } 	    
	 rozofs_export_p = rz_inode_lib_init(root_path);
	 if (rozofs_export_p == NULL)
	 {
	   severe("EXIT : RozoFS: error while reading %s",root_path);
	   exit(EXIT_FAILURE);  
	 }        
	 /*
	 ** start scanning from beginning 
	 */
	 rozo_lib_save_index_context(&scan_context);	       
      }

      scanned_current_count = 0;
      
      list_init(&jobs);
      rz_scan_all_inodes_from_context(rozofs_export_p,ROZOFS_REG,1,rozofs_visit,NULL,NULL,NULL,&scan_context);

      /*
      ** launch the jobs
      */
      if (scanned_current_count !=0)
      {
        if (rozo_cmove_ctx.verbose) info("%d file to move\n",scanned_current_count);
        all_export_scanned_count +=scanned_current_count;
	rozofs_do_move_one_export_fid_mode_multithreaded_mounted("localhost", 
                              econfig->root, 
			      rozo_cmove_ctx.throughput, 
			      &jobs,
			      mount_path[econfig->eid]); 
      }
      
      if (rozo_lib_is_scanning_stopped()== 0)
      {
	
	/*
	** attempt to go to the next exportd if any
	*/
	rozo_lib_export_release();
	rozo_cmove_ctx.current_eid_idx++;
	if (rozo_cmove_ctx.current_eid_idx >= rozo_cmove_ctx.number_of_eid_in_volume)
	{
	  rozo_cmove_ctx.scanned_file_cpt = rozo_cmove_ctx.current_scanned_file_cpt;
	  rozo_cmove_ctx.current_scanned_file_cpt = 0;
	  if (all_export_scanned_count == 0)
	  {
	    if (rozo_cmove_ctx.verbose) info("Empty Scanning!\n");
	    goto end;
	  }
	  else
	  {
	    all_export_scanned_count = 0;
	  }	   
	  rozo_cmove_ctx.current_eid_idx = 0;
          rozo_lib_reset_index_context(&scan_context);

	}
        econfig = volume_export_table[rozo_cmove_ctx.current_eid_idx];
	root_path = econfig->root;
	if (root_path == NULL)
	{
	  severe("EXIT : eid %d does not exist", econfig->eid);
	  exit(EXIT_FAILURE); 
	} 	    
	rozofs_export_p = rz_inode_lib_init(root_path);
	if (rozofs_export_p == NULL)
	{
	  severe("EXIT : RozoFS: error while reading %s",root_path);
	  exit(EXIT_FAILURE);  
	}
	/*
	** start scanning from beginning 
	*/
	rozo_lib_reset_index_context(&scan_context);	
	if (rozo_cmove_ctx.verbose) {
          info("scan export %d from the beginning\n",econfig->eid);
        }
      }
      else
      {
        rozo_lib_save_index_context(&scan_context);
	if (rozo_cmove_ctx.verbose) 
	{
	   info("user_id %d file_id %llu inode_idx %d\n",scan_context.user_id,( long long unsigned int)scan_context.file_id,scan_context.inode_idx);
        }
      }

   }



end:
    rozofs_mover_print_stat(printfBuffer);
    printf("%s",printfBuffer); 

  exit(EXIT_SUCCESS);  
  return 0;
}
