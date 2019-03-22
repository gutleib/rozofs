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
#include "export_volume_stat.h"
#include "rozo_balance.h"
#include "rozofs_mover.h"
#include "rebalance_config.h"
#define RZ_FILE_128K  (1024*128)
#define RZ_FILE_1M  (1024*1024)

#define TRACEOUT(fmt, ...) { \
printf(fmt, ##__VA_ARGS__);\
}


typedef enum _rs_file_sz_e
{
   FILE_128K_E = 0,
   FILE_1M_E,
   FILE_10M_E,
   FILE_100M_E,
   FILE_1G_E,
   FILE_10G_E,
   FILE_100G_E,
   FILE_1T_E,
   FILE_SUP_1T_E,
   FILE_MAX_T_E
} rs_file_sz_e;
  

typedef struct _rz_sids_stats_t
{
    uint64_t nb_files;  
    uint64_t byte_size;
    uint64_t tab_size[FILE_MAX_T_E];
} rz_sids_stats_t;



typedef struct _rz_cids_stats_t
{
   rz_sids_stats_t sid_tab[SID_MAX];
} rz_cids_stats_t;

lv2_cache_t cache;   /**< pseudo level 2 cache */

typedef enum 
{
   CLUSTER_OFF = 0,
   CLUSTER_IN_RANGE,
   CLUSTER_OVERLOADED,
   CLUSTER_UNDERLOADED,
   CLUSTER_MAX,
} cluster_state_e;

typedef struct _rozo_cluster_sid_t
{
   int cluster_idx;
   int score;
   int sid_idx_tab[SID_MAX];
   int64_t data2mov[SID_MAX]; /**< data to move in bytes */
} rozo_cluster_sid_t;
   
#define DAEMON_PID_DIRECTORY "/var/run/"
export_vol_stat_t *export_rebalance_vol_stat_p = NULL;   /**< table that contains the informations relative to a cluster within a volume  */
export_vol_cluster_stat2_t *export_rebalance_cluster_stat_p[ROZOFS_CLUSTERS_MAX];   /**< cluster table: contains the information relative to sid */
cluster_t                  *export_rebalance_cluster_alloc_p[ROZOFS_CLUSTERS_MAX];  /**< cluster table: contains the information relative to sid */
export_vol_cluster_stat_t cluster_average;     /**< information relative to the clusters within a volume */
export_vol_cluster_stat_t cluster_sid_average; /**< information relative to the storages within a cluster */

rozo_cluster_sid_t *sid2idx_table_p[ROZOFS_CLUSTERS_MAX];   /**< that table is indexed by the sid value    */

scan_index_context_t scan_context;   /**< current indexes for inode tracking                             */

list_t pList_volume_cluster;         /**< ordered list of the cluster within a volume                    */
int rebalance_trigger_score;         /**< score relative to the rozo_balancing_ctx.rebalance_threshold_trigger              */
    
int rozofs_no_site_file = 0;
export_config_t * volume_export_table[EXPGW_EID_MAX_IDX];   /**< table of eids associated with a volume  */
export_config_t * current_export = NULL;

rz_cids_stats_t *cids_tab_p[ROZOFS_CLUSTERS_MAX];
time_t mtime_cluster_table[ROZOFS_CLUSTERS_MAX];   /**< mtime value of the cluster file */
time_t mtime_volume;                               /**< mtime value of the volume file */

int rozo_balance_non_blocking_thread_started = 0;  /**< flag that indicates that the non-blocking thread is started  */
rozo_balancing_ctx_t rozo_balancing_ctx;
int rozo_rebalance_do_cluster_distribute_size_balancing(uint8_t layout,int site_idx,  sid_t *sids, uint8_t multi_site,uint64_t size,cid_t *cid);
static pthread_t rebalance_thread=0;
char *debug_buffer = NULL;

pthread_rwlock_t cluster_stats_lock;
pthread_rwlock_t cluster_balance_compute_lock;

list_t jobs;  
void *rozofs_export_p = NULL;
/*
** The name of the utility to display on help
*/
char * utility_name=NULL;

time_t rebalance_config_file_mtime = 0;
/*
**_______________________________________________________________________
*/
/** Find out the export root path from its eid reading the configuration file
*   
    @param  vid : volume identifier
    
    @retval 0 found
    @retval <0 not found
*/
cluster_config_t * get_cluster_config(cid_t cid) {
  list_t           * v;
  volume_config_t  * vconfig;
  list_t           * c;
  cluster_config_t * cconfig;

  list_for_each_forward(v, &exportd_config.volumes) {

    vconfig = list_entry(v, volume_config_t, list);

    list_for_each_forward(c, &vconfig->clusters) {

      cconfig = list_entry(c, cluster_config_t, list);
      if (cconfig->cid == cid) return cconfig;
    }    
  }
  return NULL;
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
static int mkpath(char * directory_path, mode_t mode) {
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
      if (mkdir(directory_path, mode) != 0) {
	severe("mkdir(%s) %s", directory_path, strerror(errno));
        goto out;
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
/*
*_______________________________________________________________________________
*/
void cluster_initialize(cluster_t *cluster, cid_t cid, uint64_t size,
        uint64_t free,
        rozofs_cluster_admin_status_e adminStatus) {
    int i;
    cluster->cid = cid;
    cluster->adminStatus = adminStatus;
    cluster->size = size;
    cluster->free = free;
    for (i = 0; i < ROZOFS_GEOREP_MAX_SITE;i++) list_init(&cluster->storages[i]);
}
/*
*_______________________________________________________________________________
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
*_______________________________________________________________________________
*/
/**
*  Check if the mtime of the file matches the range defined by the user

   @param mtime: mtime of the file to move
   
   @retval 0 match
   @retval < 0 no match 
*/

int rozo_bal_check_mtime(int64_t mtime)
{
  int64_t curtime;
  int64_t newer_time;
  int64_t older_time;
   /*
   ** check if a time range has been configured
   */
   newer_time = rozo_balancing_ctx.newer_time_sec_config;
   older_time = rozo_balancing_ctx.older_time_sec_config;
   if ((newer_time == -1) && (older_time==-1))
   {
     rozo_balancing_ctx.time_inrange_cpt++;
     return 0;
   }
   curtime=time(NULL);
   /*
   ** Check if the file is not too old to be moved
   */
   if (newer_time != -1)
   {
      if (mtime < (curtime - newer_time))
      {
        rozo_balancing_ctx.time_older_cpt++;
        return -1;   
      }
   }
   /*
   ** check if the file is old enough to be moved
   */
   if (older_time != -1)
   {
      if (mtime > (curtime - older_time))
      {
        rozo_balancing_ctx.time_newer_cpt++;
        return -1;   
      }
   }
   rozo_balancing_ctx.time_inrange_cpt++;
   return 0;
}
/*
*_______________________________________________________________________________
*/
/**
*   Compute the score of a file

   @param cid: cluster allocated for the file
   @param sid_p: list of the sid
   @param forward: number of sid used by the forward process
   @param size:file size
   
   @retval score value
*/
int rozo_bal_compute_file_score(int cid,sid_t *sid_p,int forward,uint64_t size)
{
  int sid_idx;
  int i;
  int score;
  export_vol_sid_in_cluster_t *q;
  export_vol_cluster_stat2_t *cluster_p;
  rozo_cluster_sid_t *p = sid2idx_table_p[cid];
  int estimated_prj_size = 0;
  
  cluster_p = export_rebalance_cluster_stat_p[p->cluster_idx];
  if (p->score < 0) return p->score;
  score = p->score;
  estimated_prj_size = size/(forward-1);
  
  for (i = 0; i < forward;i++)
  {
     sid_idx = p->sid_idx_tab[sid_p[i]];
     q = &cluster_p->sid_tab[sid_idx];
     switch (q->rebalance_state)
     {
        case CLUSTER_OFF:
	case CLUSTER_IN_RANGE:
	  break;
	case CLUSTER_OVERLOADED:
	  if ((p->data2mov[sid_idx] - estimated_prj_size) < 0)  score-=2;
	  else score+=1;
	  break;
	case CLUSTER_UNDERLOADED:
	  score-=2;
	  break;
     }  
   }
   return score;
}

/*
*_______________________________________________________________________________
*/
/**
*   Compute the score of a file

   @param cid: cluster allocated for the file
   @param sid_p: list of the sid
   @param forward: number of sid used by the forward process
   @param size:file size
   
   @retval none
*/
void rozo_bal_adjust_sid_score(int cid,sid_t *sid_p,int forward,uint64_t size)
{
  int sid_idx;
  int i;
  export_vol_sid_in_cluster_t *q;
  export_vol_cluster_stat2_t *cluster_p;
  rozo_cluster_sid_t *p = sid2idx_table_p[cid];
  int estimated_prj_size = 0;
  
  cluster_p = export_rebalance_cluster_stat_p[p->cluster_idx];
  estimated_prj_size = size/(forward-1);
  
  for (i = 0; i < forward;i++)
  {
     sid_idx = p->sid_idx_tab[sid_p[i]];
     q = &cluster_p->sid_tab[sid_idx];
     switch (q->rebalance_state)
     {
        case CLUSTER_OFF:
	case CLUSTER_IN_RANGE:
	  break;
	case CLUSTER_OVERLOADED:
	  p->data2mov[sid_idx] -=estimated_prj_size;
	  if (p->data2mov[sid_idx] < 0) q->rebalance_state = CLUSTER_UNDERLOADED; 
	  break;
	case CLUSTER_UNDERLOADED:
	  break;
     }  
   }
}
/*;
*_______________________________________________________________________________
*/
void volume_storage_initialize_balance(volume_storage_t * vs, sid_t sid,
                               const char *hostname, uint8_t host_rank, uint8_t siteNum,
			       uint64_t size,uint64_t free,uint8_t state) {

    vs->sid = sid;
    strncpy(vs->host, hostname, ROZOFS_HOSTNAME_MAX);
    vs->host_rank = host_rank;
    vs->siteNum = siteNum;
    vs->stat.free = free;
    vs->stat.size = size;
    vs->status = state;
    vs->inverseCounter = 0; // Nb selection in the 1rst inverse SID
    vs->forwardCounter = 0; // Nb selection in the 1rst forward SID
    vs->spareCounter   = 0; // Nb selection as a spare SID

    list_init(&vs->list);
}
/**
*_______________________________________________________________________________
*/
/**
*     Build the cluster structure needed for file allocation

      @param clusterbalance_p: pointer to the binary representation of the cluster structure used for rebalancing
      
      @retval <> NULL: pointer to the cluster structure used for sid allocation
*/
cluster_t * allocate_cluster_in_memory(export_vol_cluster_stat2_t *clusterbalance_p)
{

    export_vol_sid_in_cluster_t *sid_p;
    int sid;
    int i;
    int host_rank=0;
    int found = 0;
    int k;
    cluster_config_t * cconfig;
    rozofs_cluster_admin_status_e  adminStatus;
      
    /*
    ** need to compute the nb_host for the cluster and the nb_rank of each storage
    */
    clusterbalance_p->nb_host = 0;
    for (sid=0;sid < clusterbalance_p->nb_sid;sid++)  
    {
        found = 0;
        for (k=0;k < sid; k++)
	{
	   if (strcmp((char*)clusterbalance_p->sid_tab[sid].hostname,(char*)clusterbalance_p->sid_tab[k].hostname)==0)
	   {
	     clusterbalance_p->sid_tab[sid].host_rank = clusterbalance_p->sid_tab[k].host_rank; 
	     found = 1;
	     break;
	   }	
	}
	if (found == 0) clusterbalance_p->sid_tab[sid].host_rank = host_rank++;
    } 
    clusterbalance_p->nb_host =  host_rank;  
    
    // Memory allocation for this cluster
    cluster_t *cluster = (cluster_t *) xmalloc(sizeof (cluster_t));
    
    /*
    ** Retrieve administrative status from configuration
    */
    cconfig = get_cluster_config(clusterbalance_p->cluster_id);
    adminStatus = rozofs_cluster_admin_status_undefined;
    if (cconfig) {
      adminStatus = cconfig->adminStatus;
    }

    cluster_initialize(cluster, clusterbalance_p->cluster_id, clusterbalance_p->total_size_bytes,clusterbalance_p->free_size_bytes, adminStatus);
    for (i = 0; i <ROZOFS_GEOREP_MAX_SITE; i++) 
    {
	  cluster->nb_host[i] = clusterbalance_p->nb_host;
      for (sid=0;sid < clusterbalance_p->nb_sid;sid++)  
      {
          volume_storage_t *vs = (volume_storage_t *) xmalloc(sizeof (volume_storage_t));
	  sid_p = &clusterbalance_p->sid_tab[sid];
          volume_storage_initialize_balance(vs, sid_p->sid, (const char*)sid_p->hostname, sid_p->host_rank,0, /*sid_p->siteNum*/
		                    sid_p->total_size_bytes,sid_p->free_size_bytes,sid_p->state);
	  list_push_front((&cluster->storages[i]), &vs->list);  

	}
    }
    /*
    ** Re-order the SIDs
    */
    list_t           *pList = &cluster->storages[0];
    list_sort(pList, volume_storage_compare);
    return cluster;
}
/**
*_______________________________________________________________________________
*/
/**
*     release the cluster structure used for file allocation

      @param clusterbalance_p: pointer to the binary representation of the cluster structure used for rebalancing
      
      @retval <> NULL: pointer to the cluster structure used for sid allocation
*/
void release_cluster_in_memory(cluster_t *cluster)	
{
    list_t *p, *q;
    int i;
    for (i = 0; i < ROZOFS_GEOREP_MAX_SITE;i++) {
      list_for_each_forward_safe(p, q, (&cluster->storages[i])) {
          volume_storage_t *entry = list_entry(p, volume_storage_t, list);
          list_remove(p);
          free(entry);
      }
    }
}
	
/*
**_______________________________________________________________________
*/
char *rozo_display_one_sid(rz_sids_stats_t *sid_p,int i,char *pbuf)
{
  char buffer[128];
  int k;
  pbuf +=sprintf(pbuf," %3.3d | %12llu  | %s |",i,(long long unsigned int)sid_p->nb_files,
                                                         display_size((long long unsigned int)sid_p->byte_size,buffer));
  for (k = 0; k < FILE_MAX_T_E; k ++)
  {
    pbuf +=sprintf(pbuf," %10llu |",(long long unsigned int)sid_p->tab_size[k]);
  }
  pbuf +=sprintf(pbuf,"\n");
  return pbuf;
 
}
/*
**_______________________________________________________________________
*/
char *rozo_display_one_cluster(rz_cids_stats_t *cid_p,int i,char *pChar)
{
   int sid;
   rz_sids_stats_t *sid_p;
   pChar += sprintf(pChar,"Cluster %d:\n",i);
   pChar += sprintf(pChar," sid |   bins files  |   total size  |    0-128K  |   128K-1M  |    1-10M   |   10-100M  |   100-1000M|     1-10G  |    10-100G |   100-1000G|      > 1TB |\n");
   pChar += sprintf(pChar," ----+---------------+---------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+\n");
   for (sid=0; sid < SID_MAX; sid++)
   {
      sid_p = &cid_p->sid_tab[sid];
      if (sid_p->nb_files == 0) continue;
      pChar = rozo_display_one_sid(sid_p,sid,pChar);
   }
   pChar += sprintf(pChar," ----+---------------+---------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+\n");
   pChar += sprintf(pChar,"\n");
   return pChar;
}

/*
**_______________________________________________________________________
*/
char *rozo_display_all_cluster_with_buf(char *pChar)
{
   int cid;
   
   pthread_rwlock_rdlock(&cluster_stats_lock);
   
   for (cid=0; cid < ROZOFS_CLUSTERS_MAX; cid++)
   {
      if (cids_tab_p[cid] == NULL) continue;
      pChar = rozo_display_one_cluster(cids_tab_p[cid],cid,pChar);
   }
   
   pthread_rwlock_unlock(&cluster_stats_lock);
   pChar += sprintf(pChar,"\n");
   return pChar;
}

void rozo_display_all_cluster()
{
  return;
  rozo_display_all_cluster_with_buf(debug_buffer);
  printf("%s",debug_buffer);
}
/*
**_______________________________________________________________________
*/
/**
*  API to get the pathname of the objet: @rozofs_uuid@<FID_parent>/<child_name>

   @param export : pointer to the export structure
   @param inode_attr_p : pointer to the inode attribute
   @param buf: output buffer
   
   @retval buf: pointer to the beginning of the outbuffer
*/
char *rozo_get_full_path(void *exportd,void *inode_p,char *buf,int lenmax)
{
   lv2_entry_t *plv2;
   char name[1024];
   char *pbuf = buf;
   int name_len=0;
   int first=1;
   ext_mattr_t *inode_attr_p = inode_p;
   rozofs_inode_t *inode_val_p;
   
   pbuf +=lenmax;
   
   export_t *e= exportd;
   
   inode_val_p = (rozofs_inode_t*)inode_attr_p->s.pfid;
   if ((inode_val_p->fid[0]==0) && (inode_val_p->fid[1]==0))
   {
      pbuf-=2;
      pbuf[0]='.';   
      pbuf[1]=0;      
   } 
   
   buf[0] = 0;
   first = 1;
   while(1)
   {
      /*
      ** get the name of the directory
      */
      name[0]=0;
      rozolib_get_fname(inode_attr_p->s.attrs.fid,e,name,&inode_attr_p->s.fname,inode_attr_p->s.pfid);
      name_len = strlen(name);
      if (name_len == 0) break;
      if (first == 1) {
	name_len+=1;
	first=0;
      }
      pbuf -=name_len;
      memcpy(pbuf,name,name_len);
      pbuf--;
      *pbuf='/';

      if (memcmp(e->rfid,inode_attr_p->s.pfid,sizeof(fid_t))== 0)
      {
	 /*
	 ** this the root
	 */
	 pbuf--;
	 *pbuf='.';
	 return pbuf;
      }
      /*
      ** get the attributes of the parent
      */
      if (!(plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,&cache, inode_attr_p->s.pfid))) {
	break;
      }  
      inode_attr_p=  &plv2->attributes;
    }

    return pbuf;
}

/*
**__________________________________________________________
*/
/*
** Load up in memory the volume statistics

  @param vid : Volume identifier
  
  @retval O on success
  @retval -1 on error
*/
int load_volume_stats(int vid)
{
   char path[FILENAME_MAX];
   int fd_v=-1;
   int ret;
   ssize_t size;
   struct stat stat_buf_before;
   struct stat stat_buf_after;
   int i;
      
   if (export_rebalance_vol_stat_p == NULL)
   {
      export_rebalance_vol_stat_p = malloc(sizeof(export_vol_stat_t));
      if (export_rebalance_vol_stat_p == NULL)
      {
         return -1;
      }
      memset(export_rebalance_vol_stat_p,0,sizeof(export_vol_stat_t));
   }
//   memset(export_rebalance_vol_stat_p,0,sizeof(export_vol_stat_t));
   sprintf(path, "%s%s%d.bin", DAEMON_PID_DIRECTORY, "exportd/volume_", vid);
   for (i = 0; i < 4; i++)
   {
     if (i!=0) sleep(1);
     if (fd_v!=-1) 
     {
        close(fd_v);
	fd_v=-1;
     }
     if ((fd_v = open(path, O_RDONLY, S_IRWXU | S_IROTH)) < 0) {
	 severe("open failure %s : %s\n",path,strerror(errno));
	 continue;
     } 
     ret = fstat(fd_v,&stat_buf_before);
     if (ret < 0)
     {
       continue;
     } 
     if (mtime_volume == stat_buf_before.st_mtime) 
     {
       close(fd_v);
       errno=EAGAIN;
       return -1;
     }       
     size = pread(fd_v,(void *)export_rebalance_vol_stat_p,sizeof(export_vol_stat_t),0);
     if ( size < 0)
     {
	severe("read failure %s : %s\n",path,strerror(errno));
	continue;
     }
     /*
     ** Check the file size 
     */
     if (size != sizeof(export_vol_stat_t))
     {
       /*
       ** the file is not full
       */
	continue;
     } 
     /*
     ** re-check the mtime
     */
     ret = fstat(fd_v,&stat_buf_after);
     if (ret < 0)
     {
       continue;
     } 
     if  (stat_buf_after.st_mtime != stat_buf_before.st_mtime)
     {
       continue;      
     }
     close(fd_v);
     mtime_volume = stat_buf_after.st_mtime; 
     return 0;
   }       
   return -1;
}
/*
**__________________________________________________________________
*/
/**
*   Load in memory the file that corresponds to a volume/cluster

  @param vid: volume identifier
  @param cid: cluster identifier
  @param cluster_idx: cluster index
  
  @retval 0 success
  @retval -1 error
  
*/
int load_cluster_stats(int vid,int cid,int cluster_idx)
{
   char path[FILENAME_MAX];
   int fd_v=-1;
   ssize_t size;
   int ret;
   struct stat stat_buf_before;
   struct stat stat_buf_after;
   int i;
   
   if (export_rebalance_cluster_stat_p[cluster_idx] == NULL)
   {
      export_rebalance_cluster_stat_p[cluster_idx] = malloc(sizeof(export_vol_cluster_stat2_t));
      if (export_rebalance_cluster_stat_p[cluster_idx] == NULL)
      {
         return -1;
      }
   }
   sprintf(path, "%s%s%d_cluster_%d.bin", DAEMON_PID_DIRECTORY, "exportd/volume_", vid,cid);
   for (i = 0; i < 4; i++)
   {

     memset(export_rebalance_cluster_stat_p[cluster_idx],0,sizeof(export_vol_cluster_stat2_t));

     if (i!=0) sleep(1);
     if (fd_v!=-1) 
     {
        close(fd_v);
	fd_v=-1;
     }
     if ((fd_v = open(path, O_RDONLY, S_IRWXU | S_IROTH)) < 0) {
	 severe("open failure %s : %s\n",path,strerror(errno));
       continue;
     } 
     ret = fstat(fd_v,&stat_buf_before);
     if (ret < 0)
     {
       continue;
     } 
     if (mtime_cluster_table[cluster_idx] == stat_buf_before.st_mtime) 
     {
       close(fd_v);
       errno=EAGAIN;
       return -1;
     }  
     size = pread(fd_v,( void *)export_rebalance_cluster_stat_p[cluster_idx],sizeof(export_vol_cluster_stat2_t),0);
     if ( size < 0)
     {
	severe("read failure %s : %s\n",path,strerror(errno));
	continue;
     }
     /*
     ** Check the file size 
     */
     if (size != sizeof(export_vol_cluster_stat2_t))
     {
       /*
       ** the file is not full
       */
	continue;
     } 
     /*
     ** re-check the mtime
     */
     ret = fstat(fd_v,&stat_buf_after);
     if (ret < 0)
     {
       continue;
     } 
     if  (stat_buf_after.st_mtime != stat_buf_before.st_mtime)
     {
       continue;      
     }
     close(fd_v);
     mtime_cluster_table[cluster_idx] = stat_buf_after.st_mtime;
     return 0;
   }     
   errno=EINVAL;     
   return -1;
}
/*
**__________________________________________________________________
*/
int volume_stats_compute(export_vol_stat_t *p)
{
   int i;
   export_vol_cluster_stat_t *q;
   
   memset(&cluster_average,0,sizeof(export_vol_cluster_stat_t));
   
   q = &p->cluster_tab[0];
   for (i= 0; i < p->nb_cluster; i++,q++)
   {
     cluster_average.total_size_bytes +=q->total_size_bytes; 
     cluster_average.free_size_bytes  +=q->free_size_bytes;    
     q->rebalance_state =CLUSTER_IN_RANGE;
   }
   /*
   ** Compute the average free capacity of the volume
   */
   if (cluster_average.total_size_bytes == 0) return -1;
   cluster_average.free_percent = cluster_average.free_size_bytes*100/cluster_average.total_size_bytes;
   /*
   ** Check if there is some clusters that are out of range
   */
   q = &p->cluster_tab[0];
   for (i= 0; i < p->nb_cluster; i++,q++)
   {
     if ((q->free_percent < rozo_balancing_ctx.rebalance_threshold) || ( rozo_balancing_ctx.rebalance_threshold >= cluster_average.free_percent))
     {
       q->rebalance_state = CLUSTER_OVERLOADED;
       continue;
     }     
     if (q->free_percent < cluster_average.free_percent)
     {
        if (q->free_percent >= ( cluster_average.free_percent-rozo_balancing_ctx.rebalance_threshold)) continue;
	q->rebalance_state = CLUSTER_OVERLOADED;
	continue;
     } 
     if (q->free_percent >= ( cluster_average.free_percent+rozo_balancing_ctx.rebalance_threshold)) {
       q->rebalance_state = CLUSTER_UNDERLOADED;
     }

   }   
   
   if (p->nb_cluster < 2) return 0;
   return 0;
}  
/*
**__________________________________________________________________
*/
char *print_rebalance_state(int state)
{
   switch (state)
   {
      case CLUSTER_OVERLOADED: return  "\033[93m\033[40mOverloaded \033[0m";
      case CLUSTER_UNDERLOADED: return "\033[33m\033[40mUnderloaded\033[0m";
      case CLUSTER_IN_RANGE: return    "\033[92m\033[40mIn range   \033[0m";
      case CLUSTER_OFF: return         "\033[91m\033[40mOff        \033[0m";
      default: return                  "\033[91m\033[40mUnknown?   \033[0m";
    }
    return "Unkown?";
}
/*
**__________________________________________________________________
*/
char  *display_one_cluster_balancing_stats(export_vol_cluster_stat2_t *p,char *pChar)
{
   int i;
   export_vol_sid_in_cluster_t *q;
   rozo_cluster_sid_t *val2idx_p = sid2idx_table_p[p->cluster_id];
   char buffer[64];
   
   pChar +=sprintf(pChar,"    {  \"cid\" : %d, \"nb_host\" : %d,  \"nb_sid\" : %d, \"score\" : %d, \n       \"sids\" : [\n",p->cluster_id,p->nb_host,p->nb_sid,val2idx_p->score);
   q = &p->sid_tab[0];
   for (i= 0; i < p->nb_sid; i++,q++)
   {
     int64_t data2mov = 0;
     switch (q->rebalance_state)
     {
        case  CLUSTER_OVERLOADED:
	   data2mov = val2idx_p->data2mov[i];
	   break;
	case  CLUSTER_UNDERLOADED:
	   break;
	default:
           break;
      }
      if (i!=0) pChar += sprintf(pChar,",\n");
      pChar += sprintf(pChar,"        { \"sid\" : %3d, \"free%%\" : %2d, \"state\" : \"%s\",  \"size MB\" : %llu, \"data2move\" :  \"%s\" }",
                   q->sid,q->free_percent, 
		   print_rebalance_state(q->rebalance_state),
		   ( long long unsigned int)q->total_size_bytes/1000000,
                   display_size(data2mov,buffer));
   }
   pChar +=sprintf(pChar,"\n      ]\n    }");  
   return pChar;
}

/*
**__________________________________________________________________
*/
char  *display_all_cluster_balancing_stats(char *pChar)
{
   int cluster_idx;
   
   pthread_rwlock_wrlock(&cluster_balance_compute_lock);
   if (export_rebalance_vol_stat_p == NULL)
   {
     pChar +=sprintf(pChar,"Not yet ready\n");
     goto out;
   }

   pChar += sprintf(pChar, "{\n  \"average free space in volume\" :  %d,\n",cluster_average.free_percent);
   pChar += sprintf(pChar, "  \"clusters\" :   [\n");
   
   for (cluster_idx = 0; cluster_idx < export_rebalance_vol_stat_p->nb_cluster;cluster_idx++)
   {
     if (cluster_idx != 0) {
       pChar += sprintf(pChar, ",\n");
     }
     pChar = display_one_cluster_balancing_stats(export_rebalance_cluster_stat_p[cluster_idx],pChar);
   }
   pChar += sprintf(pChar, "\n  ]\n}\n");
   
out:   
   pthread_rwlock_unlock(&cluster_balance_compute_lock);
   return pChar;
}
/*
**__________________________________________________________________
*/
int cluster_stats_compute(export_vol_cluster_stat2_t *p,int cluster_idx)
{
   int i;
   export_vol_sid_in_cluster_t *q;
   rozo_cluster_sid_t *val2idx_p;
   int score_underloaded = 0;
   int score_overloaded= 0;
   
   memset(&cluster_sid_average,0,sizeof(export_vol_cluster_stat_t));
   /*
   ** Check if a table has already been allocated for that cluster
   */
   if (sid2idx_table_p[p->cluster_id] == NULL)
   {
      sid2idx_table_p[p->cluster_id]= malloc(sizeof(rozo_cluster_sid_t));
      memset(sid2idx_table_p[p->cluster_id],-1,sizeof(rozo_cluster_sid_t));
   }
   val2idx_p = sid2idx_table_p[p->cluster_id];
   val2idx_p->cluster_idx=cluster_idx;
   val2idx_p->score = 0;
   
   q = &p->sid_tab[0];
   for (i= 0; i < p->nb_sid; i++,q++)
   {
     cluster_sid_average.total_size_bytes +=q->total_size_bytes; 
     cluster_sid_average.free_size_bytes  +=q->free_size_bytes; 
     val2idx_p->sid_idx_tab[q->sid]=i; 
   }
   /*
   ** Compute the average free capacity of the volume
   */
   if (cluster_sid_average.total_size_bytes == 0) return -1;
   cluster_sid_average.free_percent = cluster_sid_average.free_size_bytes*100/cluster_sid_average.total_size_bytes;
   /*
   ** Check if there is some storages that under the rebalancing threshold trigger
   */
   q = &p->sid_tab[0];
   for (i= 0; i < p->nb_sid; i++,q++)
   {
     if (q->free_percent < rozo_balancing_ctx.rebalance_threshold_trigger) 
     {
        rebalance_trigger_score +=1;
     }
   } 

   /*
   ** Check if there is some storages that are out of range
   */
   q = &p->sid_tab[0];
   for (i= 0; i < p->nb_sid; i++,q++)
   {
     if (q->state == 0) 
     {
       q->rebalance_state = 0;
       continue;
     }
     if ((q->free_percent < rozo_balancing_ctx.rebalance_threshold) || ( rozo_balancing_ctx.rebalance_threshold >= cluster_average.free_percent))
     {
       score_overloaded +=1;
       q->rebalance_state = CLUSTER_OVERLOADED;
       val2idx_p->data2mov[i] = ((cluster_average.free_percent - q->free_percent)*q->total_size_bytes/100);
       continue;
     }     
     if (q->free_percent < cluster_average.free_percent)
     {
        if (q->free_percent >= ( cluster_average.free_percent-rozo_balancing_ctx.rebalance_threshold)) 
	{
          q->rebalance_state = CLUSTER_IN_RANGE;
	  continue;
	}
        score_overloaded +=1;
        val2idx_p->data2mov[i] = ((cluster_average.free_percent - q->free_percent)*q->total_size_bytes/100);
	q->rebalance_state = CLUSTER_OVERLOADED;
	continue;
     } 
     if (q->free_percent > ( cluster_average.free_percent+rozo_balancing_ctx.rebalance_threshold)) {
       q->rebalance_state = CLUSTER_UNDERLOADED;
        score_underloaded +=1;
	continue;
     }
     q->rebalance_state = CLUSTER_IN_RANGE;
     
   } 
   /*
   ** evaluate the score of the cluster
   */
   if (score_overloaded > 0) val2idx_p->score = score_overloaded;
   else
   {
     if (score_underloaded > 0) val2idx_p->score = 0- score_underloaded;
   }
   return 0;
} 
/*
**_______________________________________________________________________
*/
/**
*   Display Cluster score

    @param none 
    @retval 0: nothing
    @retval -1: need to recompute
    @retval 1 : need to move
*/
int rozo_bal_display_cluster_score()
{
   int cluster_idx;
   int cluster_id;
   rozo_cluster_sid_t *val2idx_p;

   
   rozo_balancing_ctx.max_cid_score = 0;
   rozo_balancing_ctx.min_cid_score = 0;
   for (cluster_idx = 0; cluster_idx < export_rebalance_vol_stat_p->nb_cluster;cluster_idx++)
   {
       cluster_id = export_rebalance_vol_stat_p->cluster_tab[cluster_idx].cluster_id;
       val2idx_p = sid2idx_table_p[cluster_id];
       if (val2idx_p->score > rozo_balancing_ctx.max_cid_score) rozo_balancing_ctx.max_cid_score = val2idx_p->score;
       if (val2idx_p->score < rozo_balancing_ctx.min_cid_score) rozo_balancing_ctx.min_cid_score = val2idx_p->score;
   }
   if (rozo_balancing_ctx.max_cid_score > 0)
   {
     rozo_balancing_ctx.rebalance_threshold =rozo_balancing_ctx.rebalance_threshold_config;
     return 1;
   }
   if (rozo_balancing_ctx.min_cid_score < 0)
   {
      /*
      ** shrink the threshold
      */
      if (rozo_balancing_ctx.rebalance_threshold == 0)
      {
         info("FDL----> threshold is 0 and min_score < 0!!!!\n");
	 return 0;
      }
      rozo_balancing_ctx.rebalance_threshold -= 5;
      if (rozo_balancing_ctx.rebalance_threshold < 0) rozo_balancing_ctx.rebalance_threshold = 0;
      return -1;

   }
   return 0;
}
/*
**_______________________________________________________________________
**
**   RozoFS specific function for visiting
**
** @param exportd :    pointer to exporthd data structure
** @param inode_p:     pointer to the master inode data
** @param slave_p:     pointer to the slave inode data
** @param chunk_len:   size of the slave chunk
** 
** @retval 0 no match
** @retval 1 match
*/
char bufall[1024];
char *bufout;
uint8_t rozofs_fwd ;
uint8_t rozofs_inv;
uint8_t rozofs_sf;

int blocksize= 4096;
int scanned_current_count = 0;
int all_export_scanned_count = 0;
int score_shrink;

void check_tracking_table(int,int);

int rozofs_do_visit(void *exportd, ext_mattr_t *inode_p, ext_mattr_t *slave_p, uint64_t chunk_len) {
    int i;
    rz_cids_stats_t  *cid_p;
    rz_sids_stats_t  *sid_p;
    cid_t             cid;


    /*
    ** Do not process symlink
    */
    if (!S_ISREG(inode_p->s.attrs.mode)) {
      return 0;
    }
    
    /*
    ** This chunk may not be in the balanced volume in case some export
    ** are configured with vid as well as vid_fast.
    */   
    cid = slave_p->s.attrs.cid;
    if (sid2idx_table_p[cid] == NULL) return 0;

    rozo_balancing_ctx.current_scanned_file_cpt++;   

    /*
    ** Get the cluster pointer
    */
    if (cids_tab_p[cid] == 0)
    {
      rz_cids_stats_t *temp_p;
      temp_p = malloc(sizeof(rz_cids_stats_t));
      if (temp_p == NULL)
      {
	 severe("EXIT : Error while allocating %u bytes: %s",(unsigned int)sizeof(rz_cids_stats_t),strerror(errno));
	 exit(-1);
      }
      memset(temp_p,0,sizeof(rz_cids_stats_t));
      cids_tab_p[cid] = temp_p;
    }
    cid_p = cids_tab_p[cid];
    uint64_t size;
    uint64_t size2 = chunk_len;
    size2 = size2/rozofs_inv;
    if (size2/blocksize == 0) size2 = blocksize;
    
    /*
    ** Compute the score of the file
    */
    int score = rozo_bal_compute_file_score(cid,slave_p->s.attrs.sids,rozofs_fwd,chunk_len);
    

    for (i = 0; i < rozofs_fwd; i++)
    {
       sid_p = &cid_p->sid_tab[slave_p->s.attrs.sids[i]];
       sid_p->nb_files++;
       sid_p->byte_size+=size2;
       while(1)
       {
	 if (chunk_len/RZ_FILE_128K == 0)
	 {
           sid_p->tab_size[FILE_128K_E]++;
	   break;
	 }
	 size = chunk_len;
	 size = size/RZ_FILE_1M;
	 if (size == 0)
	 {
           sid_p->tab_size[FILE_1M_E]++;
	   break;
	 }       
	 size = size/10;
	 if (size == 0)
	 {
           sid_p->tab_size[FILE_10M_E]++;
	   break;
	 } 
	 size = size/10;
	 if (size == 0)
	 {
           sid_p->tab_size[FILE_100M_E]++;
	   break;
	 } 
	 size = size/10;
	 if (size == 0)
	 {
           sid_p->tab_size[FILE_1G_E]++;
	   break;
	 } 
	 size = size/10;
	 if (size == 0)
	 {
           sid_p->tab_size[FILE_10G_E]++;
	   break;
	 } 
	 size = size/10;
	 if (size == 0)
	 {
           sid_p->tab_size[FILE_100G_E]++;
	   break;
	 } 
	 size = size/10;
	 if (size == 0)
	 {
           sid_p->tab_size[FILE_1T_E]++;
	   break;
	 } 
	 sid_p->tab_size[FILE_SUP_1T_E]++;
	 break;
       }
  }
  /*
  ** Check the file size is in range
  */
  if ((chunk_len < rozo_balancing_ctx.min_filesize_config)
  ||  (chunk_len > rozo_balancing_ctx.max_filesize_config)) {
    return 0;
  }
  
  /*
  ** Check if the mtime matches
  */
  if (rozo_bal_check_mtime(inode_p->s.attrs.mtime) < 0)
  {
     return 0;
  }
  
  uuid_unparse(slave_p->s.attrs.fid,bufall);
  bufout = bufall;
  
  if (score <= (rozo_balancing_ctx.max_cid_score - score_shrink)) {
    return 0;
  }  
    

  scanned_current_count++;  
  rozofs_mover_job_t * job;
  job = malloc(sizeof(rozofs_mover_job_t));
  memset(job,0,sizeof(rozofs_mover_job_t));
  job->size = chunk_len;
  int retval = rozo_rebalance_do_cluster_distribute_size_balancing(current_export->layout,0,job->sid,0,chunk_len,&job->cid);
  if (retval < 0)
  {
    scanned_current_count--;
    warning("cannot allocate a distribution for the file %s\n",bufout);
    free(job); 
    return 0;   
  }


  /*
  ** adjust the remain size to move
  */
  rozo_bal_adjust_sid_score(cid,slave_p->s.attrs.sids,rozofs_fwd,chunk_len);

  if (rozo_balancing_ctx.verbose)
  {
     char   msg[256];
     char * p=msg;
     info("%s size:%llu bytes score %d\n",bufout,(long long unsigned int)chunk_len,score);
     int z;
     p += sprintf(p,"CID :%d SID:",job->cid);
     for (z=0;z<4;z++)
     {
        p += sprintf(p,"%d ",job->sid[z]);
     }
     info("%s",msg);
  }
  
  job->name = malloc(sizeof(fid_t));
  memcpy(job->name,slave_p->s.attrs.fid,sizeof(fid_t));
  
  /*
  ** update the current size to move
  */
  rozo_balancing_ctx.cur_move_size += chunk_len;

  list_init(&job->list);
  list_push_back(&jobs,&job->list);    

  return 1;
}
/*
**_______________________________________________________________________
**
**   RozoFS specific function for visiting
**
** @param inode_attr_p: pointer to the inode data
** @param exportd : pointer to exporthd data structure
** @param p: always NULL
**  
**  @retval 0 no match
**  @retval 1 when match
*/
int rozofs_visit(void *exportd,void *inode_attr_p,void *p) {
  ext_mattr_t *inode_p = inode_attr_p;
  ext_mattr_t *slave_p;  
  int          idx;
  int          nb_slave;
  rozofs_iov_multi_t vector; 
   
  /*
  ** Simple file case
  */
  if (inode_p->s.multi_desc.byte == 0) {
    /*
    ** Regular file without striping nor hybrid mode
    */
    if (rozofs_do_visit(exportd, inode_p , inode_p, inode_p->s.attrs.size)) {
      goto success;
    }
    return 0;  
  }  
  
  /*
  ** Multiple file case. 
  */
    
  /*
  ** get the size of each chunk
  */
  rozofs_get_multiple_file_sizes(inode_p,&vector);

  /*
  ** The master inode is used only in hybrid mode
  */
  if (inode_p->s.hybrid_desc.s.no_hybrid == 0) {
    if (rozofs_do_visit(exportd, inode_p, inode_p, vector.vectors[0].len)) {
      /*
      ** Rebalance just one chunk at a time to avoid getting the same
      ** distribution on every chunk...
      */
      goto success;
    }  
  }  
  
  /*
  ** Check every slave inode.
  */
  slave_p = inode_p + 1;
  nb_slave = rozofs_get_striping_factor(&inode_p->s.multi_desc);
  for (idx=1; idx<=nb_slave; idx++,slave_p++) {
    if (rozofs_do_visit(exportd, inode_p, slave_p, vector.vectors[idx].len)) {
      /*
      ** Rebalance just one chunk at a time to avoid getting the same
      ** distribution on every chunk...
      */
      goto success;
    }  
  }
  
  /*
  ** No chunk selected for rebalancing
  */
  return 0;
  

success:  
  if ((scanned_current_count > rozo_balancing_ctx.max_scanned) 
  ||  (rozo_balancing_ctx.cur_move_size >=rozo_balancing_ctx.max_move_size_config))
  {  
    rozo_lib_stop_scanning();
  }  
  return 1;
}  
/**
*   ALlocate a set of SID based on the available space

   @param layout: RozoFS layout (needed to find out how many sid must be allocated )
   @param site_idx : always 0
   @param cluster
   @param sids : contains the set of the allocated sid
   @param multi_site: not used
   @param size: file size
   
   @retval -1 on error
*/
int rozo_rebalance_do_cluster_distribute_size_balancing(uint8_t layout,int site_idx, sid_t *sids, uint8_t multi_site,uint64_t size,cid_t *cid) 
{
  int        idx;
  uint64_t   sid_taken=0;
  uint64_t   taken_bit;  
  uint64_t   location_mask;
  uint64_t   location_bit;  
  uint8_t    ms_ok = 0;;
  int        nb_selected=0; 
  int        location_collision; 
  int        loop;
  volume_storage_t *selected[ROZOFS_SAFE_MAX];
  volume_storage_t *vs;
  list_t           *pList;
  list_t           *p;
  list_t           *pFirst;
  uint64_t          decrease_size = size;

  uint8_t rozofs_inverse=0; 
  uint8_t rozofs_forward=0;
  uint8_t rozofs_safe=0;
  
  cluster_t *cluster;
  
  pFirst = pList_volume_cluster.next;
  cluster = list_entry(pFirst, cluster_t, list);
  pList = &cluster->storages[0];
  
  *cid = cluster->cid;
  

  rozofs_get_rozofs_invers_forward_safe(layout,&rozofs_inverse,&rozofs_forward,&rozofs_safe);
  decrease_size = size/rozofs_inverse;
  /*
  ** Loop on the sid and take only one per node on each loop
  */    
  loop = 0;
  while (loop < 8) {
    loop++;

    idx                = -1;
    location_mask      = 0;
    location_collision = 0;

    list_for_each_forward(p, pList) {

      vs = list_entry(p, volume_storage_t, list);
      idx++;

      /* SID already selected */
      taken_bit = (1ULL<<idx);
      if ((sid_taken & taken_bit)!=0) {
        //info("idx%d/sid%d already taken", idx, vs->sid);
	    continue;
      }

      /* 
      ** In multi site location is the site number.
      ** else location is the host number within the cluter
      ** Is there one sid already allocated on this location ?
      */
      if (multi_site) location_bit = (1ULL<<vs->siteNum);
      else            location_bit = (1ULL<<vs->host_rank);
      if ((location_mask & location_bit)!=0) {
		//info("idx%d/sid%d location collision %x", idx, vs->sid, location_bit);
		location_collision++;	    
		continue;
      }

      /* Is there some available space on this server */
      if (vs->status != 0 && vs->stat.free != 0)
            ms_ok++;

      /*
      ** Take this guy
      */
      sid_taken     |= taken_bit;
      location_mask |= location_bit;
      selected[nb_selected++] = vs;

      //info("idx%d/sid%d is #%d selected with location bit %x with status %d", idx, vs->sid, nb_selected, location_bit, vs->status);

      /* Enough sid found */
      if (rozofs_safe==nb_selected) {
		if (ms_ok<rozofs_forward) return -1;
		//info("selection done");
		goto success;
      }	  
    }
    //info("end loop %d nb_selected %d location_collision %d", loop, nb_selected, location_collision);
    
    if ((nb_selected+location_collision) < rozofs_safe) return  -1;    
  }
  return -1;
  
success:

  
  /* 
  ** In weigthed round robin and in size equalizing decrease the estimated size 
  ** of the storages and re-order them in the cluster
  */
  idx = 0;

  while(idx < rozofs_safe) {
  
    vs = selected[idx];
    sids[idx] = vs->sid;
    vs->stat.free -= decrease_size;
    idx++;
  }
  /*
  ** Re-order the SIDs
  */
  list_sort(pList, volume_storage_compare);
    
  /*
  ** In case of size equalizing only, recompute the cluster estimated free size
  */  
  uint64_t  free = 0;

  list_for_each_forward(p, (&cluster->storages[site_idx])) {
  
    vs = list_entry(p, volume_storage_t, list);	    
    free += vs->stat.free;

  }  
  cluster->free = free; 
  /*
  ** re-order the cluster within the volume
  */
  list_sort(&pList_volume_cluster, cluster_compare_capacity);
  return 0;
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
       index++;
    } 
  }
  return index;   
}
/*
**_______________________________________________________________________
*/
void check_tracking_table(int idx_table,int idx_user_id)
{

   export_tracking_table_t *trk_tb_p;
   int k;
   exp_trck_top_header_t *tracking_table_p;
   exp_trck_header_memory_t *entry_p;
   export_t *fake_export_p = rozofs_export_p;
   
   if (fake_export_p == NULL) return ;
   trk_tb_p = fake_export_p->trk_tb_p;
   if ( fake_export_p->trk_tb_p == NULL)return ;
   
   
   tracking_table_p = trk_tb_p->tracking_table[idx_table];
   if (tracking_table_p == NULL) return;
   for (k= 0; k < EXP_TRCK_MAX_USER_ID;k++)
   {
   entry_p = tracking_table_p->entry_p[k];
   if (entry_p == NULL) continue;
   if (entry_p == (exp_trck_header_memory_t*)0x2) fatal("FDL bug");
   }
}
/*
*_______________________________________________________________________
*/
/**
*   print the result of the balancing in a buffer

  @param: pchar: pointer to the output buffer
  
  @retval none
*/
void print_resultat_buffer_success(char *p)
{
    char *pchar=p;
    
    pchar = show_conf_with_buf(pchar);
    pchar +=sprintf(pchar,"\n");
    pchar = display_all_cluster_balancing_stats(pchar);
    rozofs_mover_print_stat(pchar);

}
/*
*_______________________________________________________________________
*/
/**
*  Write the result file

  @retval 0 on success
  @retval -1 on error
*/  
int rozo_bal_write_result_file()
{
  char pathname[1024];
  FILE *fd;
  
  if (rozo_balancing_ctx.volume_id == -1)
  {
     /*
     ** nothing to write
     */
     return -1;
  }
  sprintf(pathname,"%s/result_vol_%d",REBALANCE_PATH,rozo_balancing_ctx.volume_id);
  if ((fd = fopen(pathname,"w")) == NULL)
  {
     return -1;
  }
  print_resultat_buffer_success(debug_buffer);
  fprintf(fd,"%s\n",debug_buffer);
  fclose(fd);
  return 0;

}
/*
*_______________________________________________________________________
*/
/**
*  Read configuration file and set rebalance context accordingly
*
*/  
void rozo_bal_read_configuration_file(void) {
  int          valid = 1;
  struct stat  stats;  	
  int          ret;
  uint64_t     val64;
  
  /*
  ** No configuration file
  */  
  if (rozo_balancing_ctx.rebalanceConfigFile == NULL) return;

  /*
  ** Read mtime and check if file has been modified
  */
  if (stat(rozo_balancing_ctx.rebalanceConfigFile,&stats) < 0) {
    severe("fstat(%s) %s",rozo_balancing_ctx.rebalanceConfigFile,strerror(errno));
    return;
  }
  if (stats.st_mtime == rebalance_config_file_mtime) return;
  rebalance_config_file_mtime = stats.st_mtime;
  
  /*
  ** Read configuration file
  */
  rebalance_config_read(rozo_balancing_ctx.rebalanceConfigFile);
  
  /*
  ** check the delays
  */
  if ((rebalance_config.newer>0) && (rebalance_config.older>0))  {
    if (rebalance_config.newer < rebalance_config.older) {
       severe("newer (%lld) delay must be greater than older delay (%lld)!\n",
              (long long int)rebalance_config.newer,
              (long long int)rebalance_config.older); 
       valid = 0;       
    }      
  }  

  rozo_balancing_ctx.rebalance_threshold_config  = rebalance_config.free_avg_tolerance;
  rozo_balancing_ctx.rebalance_threshold         = rozo_balancing_ctx.rebalance_threshold_config;  
  rozo_balancing_ctx.rebalance_threshold_trigger = rebalance_config.free_low_threshold;
  rozo_balancing_ctx.rebalance_frequency         = rebalance_config.frequency;
  rozo_balancing_ctx.max_scanned                 = rebalance_config.movecnt;
  rozo_balancing_ctx.throughput                  = rebalance_config.throughput;
  rozo_balancing_ctx.min_filesize_config         = rebalance_config.minfilesz;
  rozo_balancing_ctx.max_filesize_config         = rebalance_config.maxfilesz;
  /*
  ** Take newer and older when valid
  */
  if (valid) {
    rozo_balancing_ctx.older_time_sec_config       = rebalance_config.older;
    if (rozo_balancing_ctx.older_time_sec_config > 0) {
      rozo_balancing_ctx.older_time_sec_config *= 60;
    }
    else {
      rozo_balancing_ctx.older_time_sec_config = -1;
    }

    rozo_balancing_ctx.newer_time_sec_config = rebalance_config.newer;
    if (rozo_balancing_ctx.newer_time_sec_config > 0) {
      rozo_balancing_ctx.newer_time_sec_config *= 60;
    }      
    else {
      rozo_balancing_ctx.newer_time_sec_config = -1;
    }
  }  
  
  
  ret = get_size_value(rebalance_config.movesz,&val64);
  if (ret < 0) {
    severe("Bad value \"%s\" of movesz in configuration file", rebalance_config.movesz); 
    rozo_balancing_ctx.max_move_size_config = REBALANCE_MAX_MOVE_SIZE;     
  }
  else {
    rozo_balancing_ctx.max_move_size_config = val64;    
  } 
  
  
  if (strcmp(rebalance_config.mode,"fid") != 0)
  {

    severe("unsupported file_mode: %s\n",rebalance_config.mode);
  }
  rozo_balancing_ctx.file_mode = REBALANCE_MODE_FID;
     
  info("cfg file %s reread", rozo_balancing_ctx.rebalanceConfigFile);

}
/*
*_______________________________________________________________________
*/
static void usage() {
    char bufall[64];
    printf("RozoFS volume rebalancer - %s\n", VERSION);    
    printf("\nUsage: rozo_rebalance [OPTIONS]\n\n");
    printf("\t-h, --help\t\tprint this message.\n\n");
    printf("\t-v,--volume <vid>\t\tvolume identifier \n");
    printf("\t-t <threshold>\t\t\tthreshold in percentage \n");
    printf("\t-a <threshold>\t\t\tfree space threshold in percentage before triggering a rebalancing\n");
    printf("\nOptional parameters:\n");
    printf("\t-f,--frequency <seconds>\trebalance scan frequency in seconds (default: %u) \n",rozo_balancing_ctx.rebalance_frequency);
    printf("\t-c,--config <filename>\t\texportd configuration file name (when different from %s)\n",rozo_balancing_ctx.configFileName);
    printf("\t--olderm <minutes>\t\texclude files that are more recent than delay specified\n");
    printf("\t--older <days>\t\t\texclude files that are more recent than delay specified\n");
    printf("\t--newerm <minutes>\t\texclude files that are older than delay specified\n");
    printf("\t--newer <days>\t\t\texclude files that are older than delay specified\n");
    printf("\t--cont \t\t\t\tcontinue after reaching the balanced state\n");
#if 0
    /*
    ** Up to now only FID mode is supported
    */
    printf("\t--mode <rel|abs|fid>  \t\t\t\tuse relative, full path or fid while moving file (default is relative path)\n");
#endif
    printf("\t--minfilesz <size>[k|K|m|M|g|G] \t\tMinimum file size to move.(default:%llu)\n",(long long unsigned int)REBALANCE_MIN_FILE_SIZE);
    printf("\t--maxfilesz <size>[k|K|m|M|g|G] \t\tMaximum file size to move.(default:%llu)\n",(long long unsigned int)REBALANCE_MAX_FILE_SIZE);
    printf("\t--verbose \t\t\tset the rebalancing in verbose mode\n");
    printf("\t--movecnt <count> \t\tfile count threshold before triggering file move (default:%d)\n",REBALANCE_MAX_SCANNED);
    printf("\t--movesz <value>[k|K|m|M|g|G] \tcumulated file size threshold before triggering file move (default:%s)\n",
          display_size_not_aligned(REBALANCE_MAX_MOVE_SIZE,bufall));
    printf("\t--throughput <value> \t\tfile move througput in MBytes/s (default:%d MB/s)\n",REBALANCE_DEFAULT_THROUGPUT);
    printf("\t--cfg <fileName> \t\tThe rebalance configuration file name.\n");
    printf("\n");
};


/*
**_______________________________________________________________________
*/
static    int long_opt_cur;
int main(int argc, char *argv[]) {
    int c;
    int i;
    char *root_path=NULL;
    int ret;
    int cluster_idx;
    int loop_count = 0;
    int result;
    int cluster_error;
    char path[1024];
    int start= 1;
    uint64_t val64;

    /*
    ** Change local directory to "/"
    */
    if (chdir("/")!= 0) {}
    
    /*
    ** Check user is root
    */
    if (getuid() != 0) {
      printf("You need to be root !!!\n");
      exit(EXIT_FAILURE);
    }      


    debug_buffer = malloc(ROZO_BALANCE_DBG_BUF_SIZE);
    /*
    ** create the path toward the directory where result file is stored
    */
    strcpy(path,REBALANCE_PATH);
    ret = mkpath ((char*)path,S_IRUSR | S_IWUSR | S_IXUSR);
    if (ret < 0)
    {
       printf("Error while creating path towards result file (path:%s):%s",REBALANCE_PATH,strerror(errno));
        exit(EXIT_FAILURE);
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
    ** clear the mtime of the working files
    */
    memset(mtime_cluster_table,0,sizeof(time_t)*ROZOFS_CLUSTERS_MAX);   
    mtime_volume = 0;        
    /*
    ** Set a signal handler
    */
    rozofs_signals_declare(utility_name, common_config.nb_core_file); 
    /*
    ** init of the lock on cluster statistics
    */
    pthread_rwlock_init(&cluster_stats_lock,NULL);
    pthread_rwlock_init(&cluster_balance_compute_lock,NULL);
    
    rozofs_mover_init();
    
    
    memset(&rozo_balancing_ctx,0,sizeof(rozo_balancing_ctx_t));
    rozo_balancing_ctx.rebalance_frequency = REBALANCE_DEFAULT_FREQ_SEC;
    rozo_balancing_ctx.volume_id = -1;
    rozo_balancing_ctx.rebalance_threshold_config = -1;
    rozo_balancing_ctx.rebalance_threshold_trigger = 100;
    rozo_balancing_ctx.configFileName = EXPORTD_DEFAULT_CONFIG;   
    rozo_balancing_ctx.max_scanned = REBALANCE_MAX_SCANNED;
    rozo_balancing_ctx.min_filesize_config = REBALANCE_MIN_FILE_SIZE;
    rozo_balancing_ctx.max_filesize_config = REBALANCE_MAX_FILE_SIZE;
    rozo_balancing_ctx.max_move_size_config = REBALANCE_MAX_MOVE_SIZE;
    rozo_balancing_ctx.newer_time_sec_config = -1;         
    rozo_balancing_ctx.older_time_sec_config   = -1;  
    rozo_balancing_ctx.continue_on_balanced_state = 0;
    rozo_balancing_ctx.throughput = REBALANCE_DEFAULT_THROUGPUT;
    rozo_balancing_ctx.file_mode = REBALANCE_MODE_FID;
           
    static struct option long_options[] = {
        {"help", no_argument, 0, 'h'},
        {"path", required_argument, 0, 'p'},
        {"volume", required_argument, 0, 'v'},
        {"threshold", required_argument, 0, 't'},
        {"trigger", required_argument, 0, 'a'},
        {"frequency", required_argument, 0, 'f'},
        {"config", required_argument, 0, 'c'},
        {"olderm", required_argument, &long_opt_cur, 0},
        {"older", required_argument, &long_opt_cur, 1},
        {"newerm", required_argument, &long_opt_cur, 2},
        {"newer", required_argument, &long_opt_cur, 3},
        {"cont", no_argument, &long_opt_cur, 4},
        {"verbose", no_argument, &long_opt_cur, 5},
        {"movecnt", required_argument, &long_opt_cur, 6},
        {"movesz", required_argument, &long_opt_cur, 7},
        {"throughput", required_argument, &long_opt_cur, 8},
        {"mode", required_argument, &long_opt_cur, 9},
        {"cfg", required_argument, &long_opt_cur, 10},
        {"minfilesz", required_argument, &long_opt_cur, 11},
        {"maxfilesz", required_argument, &long_opt_cur, 12},

        {0, 0, 0, 0}
    };
    

    while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "hlrc:p:f:v:t:a:", long_options, &option_index);

      if (c == -1)
          break;

      switch (c) {
      
         case 0:
	   switch (long_options[option_index].val)
	   {
	      case 0:
        	if (sscanf(optarg,"%lld",(long long int *)&rozo_balancing_ctx.older_time_sec_config)!= 1) {
                  severe("Bad --olderm value: %s",optarg);	  
        	  usage();
        	  exit(EXIT_FAILURE);			  
        	}  
		rozo_balancing_ctx.older_time_sec_config *=60;
		break;
	      case 1:
        	if (sscanf(optarg,"%lld",(long long int *)&rozo_balancing_ctx.older_time_sec_config)!= 1) {
		  severe("Bad --older value: %s",optarg);	  
        	  usage();
        	  exit(EXIT_FAILURE);			  
        	}  
		rozo_balancing_ctx.older_time_sec_config *=(24*3600);
		break;
	      case 2:
        	if (sscanf(optarg,"%lld",(long long int *)&rozo_balancing_ctx.newer_time_sec_config)!= 1) {
		  severe("Bad --newerm value: %s",optarg);	  
        	  usage();
        	  exit(EXIT_FAILURE);			  
        	}  
		rozo_balancing_ctx.newer_time_sec_config *=60;
		break;	      
	      case 3:
        	if (sscanf(optarg,"%lld",(long long int *)&rozo_balancing_ctx.newer_time_sec_config)!= 1) {
		  severe("Bad --newerm value: %s",optarg);	  
        	  usage();
        	  exit(EXIT_FAILURE);			  
        	}  
		rozo_balancing_ctx.newer_time_sec_config *=(24*3600);
		break;	
	      case 4:
	        rozo_balancing_ctx.continue_on_balanced_state = 1;
		break;   
	      case 5:
	        rozo_balancing_ctx.verbose = 1;
		break;   
	      case 6:
        	if (sscanf(optarg,"%d",(int *)&rozo_balancing_ctx.max_scanned)!= 1) {
		  severe("--throughput: Bad value: %s",optarg);	  
        	  usage();
        	  exit(EXIT_FAILURE);			  
        	} 
		break;
	      case 7:
		ret = get_size_value(optarg,&val64);
		if (ret < 0) {
 		   severe("--movesz: Bad value: %s",optarg);	  
        	   usage();
        	   exit(EXIT_FAILURE);     
		}
		rozo_balancing_ctx.max_move_size_config = val64;

		break;
	      case 8:
        	if (sscanf(optarg,"%d",&rozo_balancing_ctx.throughput)!= 1) {
		  severe("Bad --throughput value: %s",optarg);	  
        	  usage();
        	  exit(EXIT_FAILURE);			  
        	}  
		break;
	      case 9:
                /*
                ** Up to now only FID mode is supported
                */
                break;

	      case 10:
                rozo_balancing_ctx.rebalanceConfigFile = optarg;
                /*
                ** Permanent balancer
                */
	        rozo_balancing_ctx.continue_on_balanced_state = 1;
		break;  
                              
	      case 11:
		ret = get_size_value(optarg,&val64);
		if (ret < 0) {
 		   severe("--minfilesz: Bad value: %s",optarg);	  
        	   usage();
        	   exit(EXIT_FAILURE);     
		}                
                rozo_balancing_ctx.min_filesize_config = val64;
		break;                  
                              
	      case 12:
		ret = get_size_value(optarg,&val64);
		if (ret < 0) {
 		   severe("--maxfilesz: Bad value: %s",optarg);	  
        	   usage();
        	   exit(EXIT_FAILURE);     
		}                
                rozo_balancing_ctx.max_filesize_config = val64;
		break;                  
                
	      default:
	      break;	   
	   }
	   break;

          case 'h':
              usage();
              exit(EXIT_SUCCESS);
              break;
          case 'p':
              root_path = optarg;
              break;
          case 'f':
            if (sscanf(optarg,"%d",&rozo_balancing_ctx.rebalance_frequency)!= 1) {
              severe("Bad -f value %s",optarg);	  
              usage();
              exit(EXIT_FAILURE);			  
            }  
	    break;
          case 'v':
            if (sscanf(optarg,"%d",&rozo_balancing_ctx.volume_id)!= 1) {
              severe("Bad -v value %s",optarg);	  
              usage();
              exit(EXIT_FAILURE);			  
            }              
	    break;
          case 't':
            if (sscanf(optarg,"%d",&rozo_balancing_ctx.rebalance_threshold_config)!= 1) {
              severe("Bad -t value %s",optarg);	  
              usage();
              exit(EXIT_FAILURE);			  
            }
	    if ( rozo_balancing_ctx.rebalance_threshold_config > 100) 
	    {
              severe("Out of range value %d (0..100)",rozo_balancing_ctx.rebalance_threshold_config);
              usage();
              exit(EXIT_FAILURE);	
	    } 
	    rozo_balancing_ctx.rebalance_threshold = rozo_balancing_ctx.rebalance_threshold_config;            
	    break;
          case 'a':
            if (sscanf(optarg,"%d",&rozo_balancing_ctx.rebalance_threshold_trigger)!= 1) {
              severe("Bad -t value %s",optarg);	  
              usage();
              exit(EXIT_FAILURE);			  
            }              
	    if ( rozo_balancing_ctx.rebalance_threshold_trigger > 100) 
	    {
              severe("Out of range value %d (0..100)",rozo_balancing_ctx.rebalance_threshold_trigger);
              usage();
              exit(EXIT_FAILURE);	
	    }       
	    break;
          case 'c':
              rozo_balancing_ctx.configFileName = optarg;
              break;	
          case '?':
              usage();
              exit(EXIT_SUCCESS);
              break;
          default:
              usage();
              exit(EXIT_FAILURE);
              break;
      }
  }

  if (rozo_balancing_ctx.volume_id == -1)
  {
       severe("EXIT : Volume identifier is missing");
       usage();
       exit(EXIT_FAILURE);  
  }     
  
  /*
  ** Case of the permanent mode with a configuration file.
  ** Read the given configuration file
  ** 
  */ 
  rozo_bal_read_configuration_file();
  
  
  if (rozo_balancing_ctx.rebalance_threshold == -1)
  {
       severe("EXIT : rebalance threshold is missing");
       usage();
       exit(EXIT_FAILURE);  
  } 
  /*
  ** check the delays
  */
  if ((rozo_balancing_ctx.newer_time_sec_config!= -1) && (rozo_balancing_ctx.older_time_sec_config!=-1)) 
  {
     if (rozo_balancing_ctx.newer_time_sec_config < rozo_balancing_ctx.older_time_sec_config)
     {
        severe("newer (%lld) delay must be greater than older delay (%lld)!",(long long int)rozo_balancing_ctx.newer_time_sec_config,
                                                                               (long long int)rozo_balancing_ctx.older_time_sec_config); 
        usage();
        exit(EXIT_FAILURE);
     }    
  
  }
  sprintf(path,"%sresult_vol_%d",REBALANCE_PATH,rozo_balancing_ctx.volume_id);
  printf("Result will be found in: %s\n",path);
  /*
  ** clear the cluster table
  */
  for (i= 0; i < ROZOFS_CLUSTERS_MAX;i++)
  {
     cids_tab_p[i] = NULL;  
  }
  /*
  ** Read the configuration file
  */
  if (econfig_initialize(&exportd_config) != 0) {
       severe("EXIT : can't initialize exportd config %s.",strerror(errno));
       exit(EXIT_FAILURE);  
  }    
  if (econfig_read(&exportd_config, rozo_balancing_ctx.configFileName) != 0) {
       severe("EXIT : failed to parse configuration file %s %s.",rozo_balancing_ctx.configFileName,strerror(errno));
       exit(EXIT_FAILURE);  
  }   
  /**
  * check if the volume is defined in the configuration file
  */  
  rozo_balancing_ctx.number_of_eid_in_volume = build_eid_table_associated_with_volume(rozo_balancing_ctx.volume_id); 
  if ( rozo_balancing_ctx.number_of_eid_in_volume <=0)
  {
       severe("EXIT : sthere is no eid associated with volume identified (%d) in the exportd configuration file (%s)",rozo_balancing_ctx.volume_id,rozo_balancing_ctx.configFileName);
       exit(EXIT_FAILURE);  
  }
  rozo_balancing_ctx.current_eid_idx = -1;
  /*
  ** init of the lv2 cache
  */
  lv2_cache_initialize(&cache);
  rz_set_verbose_mode(0);
  
  memset(export_rebalance_cluster_stat_p,0,sizeof(export_vol_cluster_stat2_t *)*ROZOFS_CLUSTERS_MAX);
  memset(sid2idx_table_p,0,sizeof(rozo_cluster_sid_t *)*ROZOFS_CLUSTERS_MAX);
  /*
  ** init of the debug port & instance
  */
  rozo_balancing_ctx.instance = 0;
  rozo_balancing_ctx.debug_port = rozofs_get_service_port_rebalancing_diag(rozo_balancing_ctx.volume_id);
  /*
  ** start the non-blocking thread, mainly used for rozodiag purpose
  */
  if ((errno = pthread_create(&rebalance_thread, NULL, (void*) rozo_rebalance_start_nb_blocking_th, &rozo_balancing_ctx)) != 0) {
        severe("can't create non blocking thread: %s", strerror(errno));
  }
  /*
  ** wait for end of init of the non blocking thread
  */
  loop_count = 0;
  while (rozo_balance_non_blocking_thread_started == 0)
  {
     sleep(1);
     loop_count++;
     if (loop_count > 5) fatal("Non Blocking thread does not answer");
  }  
  /*
  ** Main loop
  */
   for(;;)
   {
      /*
      ** Case of the permanent mode with a configuration file.
      ** Read the given configuration file if modified
      ** 
      */    
      rozo_bal_read_configuration_file();
   
      rebalance_trigger_score = 0;
      /*
      ** wait for some delay before looking at the volume statistics
      */
      rozo_bal_write_result_file();    

      if (start == 0) sleep(rozo_balancing_ctx.rebalance_frequency);
      start = 0;
      rozo_balancing_ctx.rebalance_threshold = rozo_balancing_ctx.rebalance_threshold_config;
      
      /*
      ** load up in memory the volume statistics
      */
      ret = load_volume_stats(rozo_balancing_ctx.volume_id);
      if (ret < 0) {
         if (errno == EAGAIN) rozo_balancing_ctx.eagain_volume++;
	 continue;
      }      
      
      rozo_balancing_ctx.cur_move_size = 0;
      /*
      ** load up the cluster statistics
      */
      pthread_rwlock_wrlock(&cluster_balance_compute_lock);
      cluster_error = 0;
      for (cluster_idx = 0; cluster_idx < export_rebalance_vol_stat_p->nb_cluster;cluster_idx++)
      {
	 ret =load_cluster_stats(rozo_balancing_ctx.volume_id,export_rebalance_vol_stat_p->cluster_tab[cluster_idx].cluster_id,cluster_idx);
	 if (ret < 0) 
	 {
	   if (errno == EAGAIN) rozo_balancing_ctx.eagain_cluster++;
	   cluster_error++;
	   break;
	 }
      }
      pthread_rwlock_unlock(&cluster_balance_compute_lock);
      if (cluster_error != 0) 
      {
        severe("Error while loading cluster statistics file %d",cluster_error);
	continue;
      }
      /*
      ** lock the data structure until the result becomes available
      */
reloop:
      /*
      ** Case of the permanent mode with a configuration file.
      ** Read the given configuration file if modified
      ** 
      */  
      rozo_bal_read_configuration_file();

      pthread_rwlock_wrlock(&cluster_balance_compute_lock);
      /*
      ** compute the total size
      */
      volume_stats_compute(export_rebalance_vol_stat_p);
      /*
      ** Load up the cluster statistics
      */
      for (cluster_idx = 0; cluster_idx < export_rebalance_vol_stat_p->nb_cluster;cluster_idx++)
      {
//	 load_cluster_stats(1,export_rebalance_vol_stat_p->cluster_tab[cluster_idx].cluster_id,cluster_idx);
	 cluster_stats_compute(export_rebalance_cluster_stat_p[cluster_idx],cluster_idx);
      }
      /*
      ** unlock the data
      */
      pthread_rwlock_unlock(&cluster_balance_compute_lock);
      /*
      ** check if the rebalance threshold trigger has been reached. If it is not the case
      ** wait for the next period
      */
      if (rebalance_trigger_score == 0)
      {
         printf("Rebalance trigger threshold not reached (%d)\n",rozo_balancing_ctx.rebalance_threshold_trigger);
	 if (rozo_balancing_ctx.continue_on_balanced_state) continue;
         info("EXIT : rebalance_trigger_score %d not reached",rozo_balancing_ctx.rebalance_threshold_trigger);
	 goto end;
      }

      /*
      ** display the cluster scores
      */
      rebalance_trigger_score = 0;
      result = rozo_bal_display_cluster_score();
      if (result < 0) goto reloop;
      /*
      ** check if the rebalance threshold trigger has been reached. If it is not the case
      ** wait for the next period
      */
      if (result == 0)
      {
         if (rozo_balancing_ctx.verbose) info("Nothing to re-balance min/max:%d/%d\n",rozo_balancing_ctx.max_cid_score,rozo_balancing_ctx.min_cid_score);
	 if (rozo_balancing_ctx.continue_on_balanced_state) continue;
         info("EXIT : rebalance state is reached");
	 goto end;
      }
      if (rozo_balancing_ctx.verbose) info(" current threshold : %d\n",rozo_balancing_ctx.rebalance_threshold);
      list_init(&pList_volume_cluster);
      for (cluster_idx = 0; cluster_idx < export_rebalance_vol_stat_p->nb_cluster;cluster_idx++)
      {
	 /*
	 ** build the cluster structure in memory for allocation purpose
	 */
	 export_rebalance_cluster_alloc_p[cluster_idx] = allocate_cluster_in_memory(export_rebalance_cluster_stat_p[cluster_idx]);
         list_push_back(&pList_volume_cluster,&export_rebalance_cluster_alloc_p[cluster_idx]->list);
      }
      /*
      ** need to get the inode from the exportd associated with the volume
      */
      if (rozo_balancing_ctx.current_eid_idx < 0)
      {
         /*
	 ** get the path towards the first eid
	 */
	 rozo_balancing_ctx.current_eid_idx = 0;
         current_export = volume_export_table[rozo_balancing_ctx.current_eid_idx];
	 root_path = current_export->root;
	 if (root_path == NULL)
	 {
	   severe("EXIT : eid %d does not exist",current_export->eid);
	   exit(EXIT_FAILURE); 
	 } 	    
	 rozofs_export_p = rz_inode_lib_init(root_path);
	 if (rozofs_export_p == NULL)
	 {
	   severe("EXIT : RozoFS: error while reading %s",root_path);
	   exit(EXIT_FAILURE);  
	 }
         if (rozofs_get_rozofs_invers_forward_safe(current_export->layout,&rozofs_inv,&rozofs_fwd,&rozofs_sf)<0)
         {
	   severe("EXIT : unexpected layout %d for eid %d",current_export->layout,current_export->eid);
	   exit(EXIT_FAILURE);  
         }           
         blocksize = ROZOFS_BSIZE_BYTES(current_export->bsize)/rozofs_inv;
	 /*
	 ** start scanning from beginning 
	 */
	 rozo_lib_save_index_context(&scan_context);	       
      }
      /*
      ** build the sorted list of cluster
      */
      list_sort(&pList_volume_cluster, cluster_compare_capacity);
      scanned_current_count = 0;
      
      list_init(&jobs);
      rz_scan_all_inodes_from_context(rozofs_export_p,ROZOFS_REG,1,rozofs_visit,NULL,NULL,NULL,&scan_context);

      /*
      ** launch the jobs
      */
      if (scanned_current_count !=0)
      {
        if (rozo_balancing_ctx.verbose) info("%d file to move\n",scanned_current_count);
        all_export_scanned_count +=scanned_current_count;
	rozofs_do_move_one_export_fid_mode("localhost", 
                              current_export->root, 
			      rozo_balancing_ctx.throughput, 
			      &jobs); 
      }
      
      if (rozo_lib_is_scanning_stopped()== 0)
      {
        int k;
	
	/*
	** attempt to go to the next exportd if any
	*/
	rozo_lib_export_release();
	rozo_balancing_ctx.current_eid_idx++;
	if (rozo_balancing_ctx.current_eid_idx >= rozo_balancing_ctx.number_of_eid_in_volume)
	{
	  rozo_balancing_ctx.scanned_file_cpt = rozo_balancing_ctx.current_scanned_file_cpt;
	  rozo_balancing_ctx.current_scanned_file_cpt = 0;
	  if (all_export_scanned_count == 0)
	  {
	    score_shrink = 1;
	    if (rozo_balancing_ctx.verbose) info("Empty Scanning!\n");
	  }
	  else
	  {
	    all_export_scanned_count = 0;
	    score_shrink = 0;
	  }	   
	  rozo_balancing_ctx.current_eid_idx = 0;
	 // rozo_display_all_cluster();
          rozo_lib_reset_index_context(&scan_context);
	  /*
	  ** Locking during the reinit
	  */
	  pthread_rwlock_wrlock(&cluster_stats_lock);
	  
	  for (k= 0; k < ROZOFS_CLUSTERS_MAX; k++)
	  {
	     if (cids_tab_p[k]!= NULL) 
	     {
	       free(cids_tab_p[k]);
	       cids_tab_p[k] = NULL;
	     }
	  }
	  pthread_rwlock_unlock(&cluster_stats_lock);

	}
        current_export = volume_export_table[rozo_balancing_ctx.current_eid_idx];
	root_path = current_export->root;
	if (root_path == NULL)
	{
	  severe("EXIT : eid %d does not exist", current_export->eid);
	  exit(EXIT_FAILURE); 
	} 	    
	rozofs_export_p = rz_inode_lib_init(root_path);
	if (rozofs_export_p == NULL)
	{
	  severe("EXIT : RozoFS: error while reading %s",root_path);
	  exit(EXIT_FAILURE);  
	}
        if (rozofs_get_rozofs_invers_forward_safe(current_export->layout,&rozofs_inv,&rozofs_fwd,&rozofs_sf)<0)
        {
	  severe("EXIT : unexpected layout %d for eid %d",current_export->layout,current_export->eid);
	  exit(EXIT_FAILURE);  
        }   
        blocksize = ROZOFS_BSIZE_BYTES(current_export->bsize)/rozofs_inv;        
	/*
	** start scanning from beginning 
	*/
	rozo_lib_reset_index_context(&scan_context);	
	if (rozo_balancing_ctx.verbose) {
          info("scan export %d from the beginning\n",current_export->eid);
        }
      }
      else
      {
        rozo_lib_save_index_context(&scan_context);
	if (rozo_balancing_ctx.verbose) 
	{
	   info("user_id %d file_id %llu inode_idx %d\n",scan_context.user_id,( long long unsigned int)scan_context.file_id,scan_context.inode_idx);
        }
      }
      /*
      ** release the memory allocated for cluster/sid allocation
      */
      for (cluster_idx = 0; cluster_idx < export_rebalance_vol_stat_p->nb_cluster;cluster_idx++)
      {
	 if (export_rebalance_cluster_alloc_p[cluster_idx]!= 0)
	 {
	   release_cluster_in_memory(export_rebalance_cluster_alloc_p[cluster_idx]);
	   export_rebalance_cluster_alloc_p[cluster_idx] = NULL;
	 }
      } 
   }

end:
    print_resultat_buffer_success(debug_buffer);
    printf("%s\n",debug_buffer);
    rozo_bal_write_result_file();    
//  rozo_display_all_cluster();

  exit(EXIT_SUCCESS);  
  return 0;
}
