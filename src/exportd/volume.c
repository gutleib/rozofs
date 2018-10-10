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

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <uuid/uuid.h>
#include <pthread.h>

#include <rozofs/rozofs.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/common/log.h>
#include <rozofs/common/list.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/common_config.h>
#include <rozofs/rpc/export_profiler.h>
#include <rozofs/rpc/epproto.h>
#include <rozofs/rpc/mclient.h>
#include <rozofs/core/uma_dbg_api.h>

#include "volume.h"
#include "export.h"

/*
** Cluster distribution table
*/
typedef struct _rozofs_cluster_distributor_t {
  uint64_t counter;
  int      max;
  int      value[1];
} rozofs_cluster_distributor_t;

/*
** Working structure needed to build the cluster distribution table
*/
typedef struct _cluster_desc_t {
  uint16_t cid;
  uint64_t weight;
} cluster_desc_t;


static int volume_storage_compare(list_t * l1, list_t *l2) {
    volume_storage_t *e1 = list_entry(l1, volume_storage_t, list);
    volume_storage_t *e2 = list_entry(l2, volume_storage_t, list);

    // online server takes priority
    if ((!e1->status && e2->status) || (e1->status && !e2->status)) {
        return (e2->status - e1->status);
    }
    return e1->stat.free <= e2->stat.free;
//  return e2->stat.free - e1->stat.free;
}

static int cluster_compare_capacity(list_t *l1, list_t *l2) {
    cluster_t *e1 = list_entry(l1, cluster_t, list);
    cluster_t *e2 = list_entry(l2, cluster_t, list);
    return e1->free < e2->free;
}

void volume_storage_initialize(volume_storage_t * vs, sid_t sid,
        const char *hostname, uint8_t host_rank, uint8_t siteNum) {
    DEBUG_FUNCTION;

    vs->sid = sid;
    strncpy(vs->host, hostname, ROZOFS_HOSTNAME_MAX);
    vs->host_rank = host_rank;
    vs->siteNum = siteNum;
    vs->stat.free = 0;
    vs->stat.size = 0;
    vs->status = 0;
    vs->inverseCounter = 0; // Nb selection in the 1rst inverse SID
    vs->forwardCounter = 0; // Nb selection in the 1rst forward SID
    vs->spareCounter   = 0; // Nb selection as a spare SID

    list_init(&vs->list);
}

void volume_storage_release(volume_storage_t *vs) {
    DEBUG_FUNCTION;
    return;
}

void cluster_initialize(cluster_t *cluster, cid_t cid, uint64_t size,
        uint64_t free) {
    DEBUG_FUNCTION;
    int i;
    cluster->cid = cid;
    cluster->size = size;
    cluster->free = free;
    for (i = 0; i < ROZOFS_GEOREP_MAX_SITE;i++) list_init(&cluster->storages[i]);
}

// assume volume_storage had been properly allocated

void cluster_release(cluster_t *cluster) {
    DEBUG_FUNCTION;
    list_t *p, *q;
    int i;
    for (i = 0; i < ROZOFS_GEOREP_MAX_SITE;i++) {
      list_for_each_forward_safe(p, q, (&cluster->storages[i])) {
          volume_storage_t *entry = list_entry(p, volume_storage_t, list);
          list_remove(p);
          volume_storage_release(entry);
          xfree(entry);
      }
    }
}

int volume_initialize(volume_t *volume, vid_t vid, uint8_t layout,uint8_t georep,uint8_t multi_site, char * rebalanceCfg) {
    int status = -1;
    DEBUG_FUNCTION;
    volume->vid = vid;
    volume->georep = georep;
    volume->multi_site = multi_site;
    volume->balanced = 0; // volume balance not yet called
    volume->full = 0;     // volume has enough free space
    volume->layout = layout;
    if (rebalanceCfg) {
      volume->rebalanceCfg = xstrdup(rebalanceCfg);
    }
    else {
      volume->rebalanceCfg = NULL;
    }
    list_init(&volume->clusters);
        
    volume->cluster_distibutor = NULL;
    volume->active_list = 0;
    list_init(&volume->cluster_distribute[0]);    
    list_init(&volume->cluster_distribute[1]);    

    if (pthread_rwlock_init(&volume->lock, NULL) != 0) {
        goto out;
    }
    status = 0;
out:
    return status;
}

void volume_release(volume_t *volume) {
    list_t *p, *q;
    DEBUG_FUNCTION;
    
    if (volume->rebalanceCfg) {
      xfree(volume->rebalanceCfg);
      volume->rebalanceCfg = NULL;
    }  
    
    /*
    ** Release cluster distribution table
    */
    if (volume->cluster_distibutor != NULL) {
      xfree(volume->cluster_distibutor);
      volume->cluster_distibutor = NULL;
    }  

    list_for_each_forward_safe(p, q, &volume->clusters) {
        cluster_t *entry = list_entry(p, cluster_t, list);
        list_remove(p);
        cluster_release(entry);
        xfree(entry);
    }
    list_for_each_forward_safe(p, q, &volume->cluster_distribute[0]) {
        cluster_t *entry = list_entry(p, cluster_t, list);
        list_remove(p);
        cluster_release(entry);
        xfree(entry);
    } 
    list_for_each_forward_safe(p, q, &volume->cluster_distribute[1]) {
        cluster_t *entry = list_entry(p, cluster_t, list);
        list_remove(p);
        cluster_release(entry);
        xfree(entry);
    }        
    if ((errno = pthread_rwlock_destroy(&volume->lock)) != 0) {
        severe("can't release volume lock: %s", strerror(errno));
    }
}

int volume_safe_copy(volume_t *to, volume_t *from) {
    list_t *p, *q;

    if ((errno = pthread_rwlock_rdlock(&from->lock)) != 0) {
        severe("can't lock volume: %u", from->vid);
        goto error;
    }

    if ((errno = pthread_rwlock_wrlock(&to->lock)) != 0) {
        severe("can't lock volume: %u", to->vid);
        goto error;
    }

    list_for_each_forward_safe(p, q, &to->clusters) {
        cluster_t *entry = list_entry(p, cluster_t, list);
        list_remove(p);
        cluster_release(entry);
        xfree(entry);
    }

    to->vid = from->vid;
    to->layout = from->layout;
    to->georep = from->georep;
    to->multi_site = from->multi_site;
    
    if (to->rebalanceCfg) {
      xfree(to->rebalanceCfg);
      to->rebalanceCfg = NULL;
    }
    
    if (from->rebalanceCfg) {
      to->rebalanceCfg = xstrdup(from->rebalanceCfg);
    }
    else {
      to->rebalanceCfg = NULL;
    }
    list_for_each_forward(p, &from->clusters) {
        cluster_t *to_cluster = xmalloc(sizeof (cluster_t));
        cluster_t *from_cluster = list_entry(p, cluster_t, list);
        cluster_initialize(to_cluster, from_cluster->cid, from_cluster->size,
                from_cluster->free);
	int i;
	for (i = 0; i < ROZOFS_GEOREP_MAX_SITE;i++) {
	  to_cluster->nb_host[i] = from_cluster->nb_host[i];
          list_for_each_forward(q, (&from_cluster->storages[i])) {
              volume_storage_t *from_storage = list_entry(q, volume_storage_t, list);
              volume_storage_t *to_storage = xmalloc(sizeof (volume_storage_t));
              volume_storage_initialize(to_storage, 
	                                from_storage->sid, 
					from_storage->host,
					from_storage->host_rank,
					from_storage->siteNum);
              to_storage->stat = from_storage->stat;
              to_storage->status = from_storage->status;
              list_push_back(&to_cluster->storages[i], &to_storage->list);
          }
	}
        list_push_back(&to->clusters, &to_cluster->list);
    }
    
     /*
     ** Release cluster distribution table
     */
    if (to->cluster_distibutor) {
      xfree(to->cluster_distibutor);
      to->cluster_distibutor = NULL;
    }
    /*
    ** Re copy the cluster distribution table
    */
    if (from->cluster_distibutor){
      rozofs_cluster_distributor_t * pDistributor = from->cluster_distibutor;
      int size = sizeof(rozofs_cluster_distributor_t) + (sizeof(int)*(pDistributor->max-1));
      to->cluster_distibutor = xmalloc(size);
      memcpy(to->cluster_distibutor,pDistributor, size);
    }

    if ((errno = pthread_rwlock_unlock(&from->lock)) != 0) {
        severe("can't unlock volume: %u", from->vid);
        goto error;
    }

    if ((errno = pthread_rwlock_unlock(&to->lock)) != 0) {
        severe("can't unlock volume: %u", to->vid);
        goto error;
    }

    return 0;
error:
    // Best effort to release locks
    pthread_rwlock_unlock(&from->lock);
    pthread_rwlock_unlock(&to->lock);
    return -1;

}
int volume_safe_from_list_copy(volume_t *to, list_t *from) {
    list_t *p, *q;

    if ((errno = pthread_rwlock_wrlock(&to->lock)) != 0) {
        severe("can't lock volume: %u %s", to->vid,strerror(errno));
        goto error;
    }

    list_for_each_forward_safe(p, q, &to->clusters) {
        cluster_t *entry = list_entry(p, cluster_t, list);
        list_remove(p);
        cluster_release(entry);
        xfree(entry);
    }

    list_for_each_forward(p, from) {
    
        cluster_t *to_cluster = xmalloc(sizeof (cluster_t));
        cluster_t *from_cluster = list_entry(p, cluster_t, list);
        cluster_initialize(to_cluster, from_cluster->cid, from_cluster->size,
                from_cluster->free);
	int i;
	for (i = 0; i < ROZOFS_GEOREP_MAX_SITE;i++) {
	  to_cluster->nb_host[i] = from_cluster->nb_host[i];
          list_for_each_forward(q, (&from_cluster->storages[i])) {
              volume_storage_t *from_storage = list_entry(q, volume_storage_t, list);
              volume_storage_t *to_storage = xmalloc(sizeof (volume_storage_t));
              volume_storage_initialize(to_storage, 
	                                from_storage->sid, 
					from_storage->host,
					from_storage->host_rank,
					from_storage->siteNum);
              to_storage->stat = from_storage->stat;
              to_storage->status = from_storage->status;
              list_push_back(&to_cluster->storages[i], &to_storage->list);
          }
	}
        list_push_back(&to->clusters, &to_cluster->list);
    }

    if ((errno = pthread_rwlock_unlock(&to->lock)) != 0) {
        severe("can't unlock volume: %u %s", to->vid,strerror(errno));
        goto error;
    }

    return 0;
error:
    // Best effort to release locks
    pthread_rwlock_unlock(&to->lock);
    return -1;

}
int volume_safe_to_list_copy(volume_t *from, list_t *to) {
    list_t *p, *q;

    if ((errno = pthread_rwlock_rdlock(&from->lock)) != 0) {
        severe("can't lock volume: %u %s", from->vid,strerror(errno));
        goto error;
    }

    list_for_each_forward_safe(p, q, to) {
        cluster_t *entry = list_entry(p, cluster_t, list);
        list_remove(p);
        cluster_release(entry);
        xfree(entry);
    }

    list_for_each_forward(p, &from->clusters) {
        cluster_t *to_cluster = xmalloc(sizeof (cluster_t));
        cluster_t *from_cluster = list_entry(p, cluster_t, list);
        cluster_initialize(to_cluster, from_cluster->cid, from_cluster->size,
                from_cluster->free);
	int i;
	for (i = 0; i < ROZOFS_GEOREP_MAX_SITE;i++) {
	  to_cluster->nb_host[i] = from_cluster->nb_host[i];
          list_for_each_forward(q, (&from_cluster->storages[i])) {
              volume_storage_t *from_storage = list_entry(q, volume_storage_t, list);
              volume_storage_t *to_storage = xmalloc(sizeof (volume_storage_t));
              volume_storage_initialize(to_storage, 
	                                from_storage->sid, 
					from_storage->host,
					from_storage->host_rank,
					from_storage->siteNum);
              to_storage->stat = from_storage->stat;
              to_storage->status = from_storage->status;
              list_push_back(&to_cluster->storages[i], &to_storage->list);
          }
	}
        list_push_back(to, &to_cluster->list);
    }

    if ((errno = pthread_rwlock_unlock(&from->lock)) != 0) {
        severe("can't unlock volume: %u %s", from->vid,strerror(errno));
        goto error;
    }

    return 0;
error:
    // Best effort to release locks
    pthread_rwlock_unlock(&from->lock);
    return -1;

}

/*
**________________________________________________________________________________________________
** Find out whether a given prime number is a common divisor for every cluster weight
** and divide when it is the case
**
** @param divisor    The prime number
** @param nbCluster  Number of cluster
** @param clusters   Cluster information
**________________________________________________________________________________________________
*/
void static inline rozo_apply_common_divisor(int divisor,int nbCluster, cluster_desc_t * clusters) {
  int idx;
  /*
  ** Divide as many times as possible
  */
  while (1) {
    /*
    ** Check whether divisor is a common divisor of all clusters
    */
    for (idx=0; idx < nbCluster; idx++) {
      if ((clusters[idx].weight % divisor)!=0) break;
    }
    /*
    ** Not a common divisor
    */    
    if (idx != nbCluster) return;
    /*
    ** Do divide and reloop
    */
    for (idx=0; idx < nbCluster; idx++) {
      clusters[idx].weight /= divisor;
    }
  }   
} 
/*
**________________________________________________________________________________________________
** Find out whether some prime number is a common divisor for every cluster
** and divide when it is the case
**
** @param nbCluster  Number of cluster
** @param clusters   Cluster information
**
**________________________________________________________________________________________________
*/
void static inline rozofs_apply_common_divisors(int nbCluster, cluster_desc_t * clusters) {
  rozo_apply_common_divisor(2,nbCluster,clusters);
  rozo_apply_common_divisor(3,nbCluster,clusters);
  rozo_apply_common_divisor(5,nbCluster,clusters);
  rozo_apply_common_divisor(7,nbCluster,clusters);
  rozo_apply_common_divisor(11,nbCluster,clusters);
  rozo_apply_common_divisor(13,nbCluster,clusters);
  rozo_apply_common_divisor(17,nbCluster,clusters);
} 
/*
**________________________________________________________________________________________________
** Trace distributor
**________________________________________________________________________________________________
*/ 
int volume_distrib_display(char * pChar, volume_t * volume) {
  char * pbuf = pChar;
  rozofs_cluster_distributor_t *  pDistributor = (rozofs_cluster_distributor_t*) volume->cluster_distibutor;
  int             idx;
  uint32_t        clusterOccurence[ROZOFS_CLUSTERS_MAX] = {0};
  int             first;

  if (volume == NULL) return 0;
  
  pbuf += sprintf(pbuf, "{ \"Distribution\" : {\n");
  pbuf += sprintf(pbuf, "     \"rule\"              : \"");
  pbuf += sprintf(pbuf, "%s\",\n",rozofs_file_distribution_rule_e2String(common_config.file_distribution_rule));
  pbuf += sprintf(pbuf, "     \"cluster rule\"      : \"");
  pbuf += sprintf(pbuf, "%s\",\n",rozofs_cluster_distribution_rule_e2String(rozofs_get_cluster_distribution_rule(common_config.file_distribution_rule)));
  pbuf += sprintf(pbuf, "     \"device rule\"       : \"");
  pbuf += sprintf(pbuf, "%s\"",rozofs_device_distribution_rule_e2String(rozofs_get_device_distribution_rule(common_config.file_distribution_rule)));

  if (pDistributor != NULL) {
    pbuf += sprintf(pbuf, ",\n     \"distributor size\"  : %d",pDistributor->max);
    pbuf += sprintf(pbuf, ",\n     \"distributor count\" : %llu",(unsigned long long int)pDistributor->counter);
    if (pDistributor->max != 0) {
      pbuf += sprintf(pbuf, ",\n     \"entries\" : [ %d", pDistributor->value[0]);
      clusterOccurence[pDistributor->value[0]-1]++;
      for (idx=1; idx<pDistributor->max; idx++) {
        pbuf += sprintf(pbuf,", %d", pDistributor->value[idx]);
        clusterOccurence[pDistributor->value[idx]-1]++;
      }       
      pbuf += sprintf(pbuf, "],\n");
      pbuf += sprintf(pbuf, "     \"weights\" : [");
      first = 1;
      for (idx=0; idx<ROZOFS_CLUSTERS_MAX; idx++) {
        if (clusterOccurence[idx] == 0) continue;
        if (first) {
          first = 0;
          pbuf += sprintf(pbuf, "\n");
        }  
        else pbuf += sprintf(pbuf, ",\n");
        pbuf += sprintf(pbuf,"                   { \"cid\" : %d, \"weigth\" : %d}", idx+1, clusterOccurence[idx]);
      }       
      if (first) pbuf += sprintf(pbuf, "]");
      else       pbuf += sprintf(pbuf, "\n                 ]");

    }
  }  
  pbuf += sprintf(pbuf, "\n}}\n");
  return pbuf - pChar;
}
/*
**________________________________________________________________________________________________
** Build a fair distribution sequence for a set of cluster, each cluster having ts specific weight
**
** @param nbCluster  Number of cluster
** @param clusters   Cluster information (cid, weight)
**
**________________________________________________________________________________________________
*/ 
rozofs_cluster_distributor_t * rozofs_build_cluster_distributor(int nbCluster, cluster_desc_t * clusters) {
  int   distributorSize;
  int   idx1;
  int   idx2;
  int * p;
  float frequency;
  float floatingRank;
  int   integerRank;
  int   oldintegerRank;
  int   steps;
  rozofs_cluster_distributor_t * pDistributor;
  
  /*
  ** Divide weight by common divisors
  */
  rozofs_apply_common_divisors(nbCluster,clusters);

  /*
  ** Order clusters in decreasing weight
  */
  for (idx1=1; idx1 < nbCluster; idx1++) {
    for (idx2=0; idx2 < idx1; idx2++) {
      if (clusters[idx2].weight < clusters[idx1].weight) {
        cluster_desc_t cluster;
        /*
        ** Swap idx1 and idx2
        */
        memcpy(&cluster,&clusters[idx2],sizeof(cluster));
        memcpy(&clusters[idx2],&clusters[idx1],sizeof(cluster));
        memcpy(&clusters[idx1],&cluster,sizeof(cluster));        
      }
    }
  }  
 
  /*
  ** Sum up the cluster weights
  */ 
  distributorSize  = 0;
  for (idx1=0; idx1 < nbCluster; idx1++) {
    distributorSize += clusters[idx1].weight;
  }  
  
  /*
  ** Allocate a distribution array
  */  
  int size = sizeof(rozofs_cluster_distributor_t) + (sizeof(int)*(distributorSize-1));
  pDistributor = (rozofs_cluster_distributor_t *) xmalloc(size);
  if (pDistributor == NULL) {
    return NULL;
  }
  memset(pDistributor, 0, size);
  pDistributor->counter = 0;
  pDistributor->max     = distributorSize;
  
  /*
  ** Install cluster id in distribution array in decreasing weight order
  */
  for (idx1=0;idx1 < nbCluster; idx1++) {

    /*
    ** Compute the floating frequency of this cluster in the free 
    ** slots of the distribution sequence
    */
    frequency = distributorSize;
    frequency /= clusters[idx1].weight;
    floatingRank = frequency;

    /*
    ** p is the starting of the cluster sequence
    */
    p = &pDistributor->value[0];
    oldintegerRank = 0;
    integerRank    = floatingRank;
    
    while(integerRank <= distributorSize) {
      /*
      ** Compute steps to go in the array to reach the next
      ** attributed slot to this cluster
      */
      steps = integerRank - oldintegerRank - 1;
      /*
      ** Jump <steps> free steps over the distribution
      */
      while(1) {
        /*
        ** Jump over already allocated slots
        */
        while (*p != 0) p++;
        /*
        ** This is the free slot to insert this cluster id
        */
        if (steps == 0) break;
        /*
        ** More steps to go
        */
        steps--;
        p++;
      } 
      /*
      ** Insert cluster id
      */   
      *p = clusters[idx1].cid;
      /*
      ** Add one floating frequency to the floating rank
      */
      floatingRank   += frequency; 
      oldintegerRank = integerRank;
      integerRank    = floatingRank;
    }

    /*
    ** Less free slots in the distributor
    */
    distributorSize -= clusters[idx1].weight;
  }
  
  return pDistributor;
}
/*
**________________________________________________________________________________________________
** Compute table of distribution betwen clusters in order to equally distribute upon every cluster
** with different numbers of SID
**
** retval The distribution table
**________________________________________________________________________________________________
*/
void * rozofs_cluster_distributor_create_wsid(list_t *cluster_list) {
  cluster_desc_t                 cluster_desc[ROZOFS_CLUSTERS_MAX];
  list_t                       * p;
  list_t                       * q;
  uint32_t                       nb_cluster = 0;
  cluster_desc_t               * pCluster;


  /*
  ** Prepare the list of cluster from the configuration
  */
  pCluster = &cluster_desc[0];
  list_for_each_forward(p, cluster_list) {

    /*
    ** Get cluster
    */
    cluster_t *volume_cluster = list_entry(p, cluster_t, list);

    /*
    ** Fill working structure
    */
    pCluster->weight = 0;    
    pCluster->cid    = volume_cluster->cid;
    
    /*
    ** The weight is the number of SID
    */
    list_for_each_forward(q, (&volume_cluster->storages[0])) {    
      pCluster->weight ++;
    }
    
    /*
    ** Next cluster
    */
    nb_cluster++;
    pCluster++;
  }
  
  /*
  ** Build the distribution sequence from this descriptor
  */
  return rozofs_build_cluster_distributor(nb_cluster,cluster_desc);
}
/*
**________________________________________________________________________________________________
** Compute table of distribution betwen clusters in order to equally distribute upon every cluster
** with different numbers of SID
**
** retval The distribution table
**________________________________________________________________________________________________
*/
void * rozofs_cluster_distributor_create_round_robin(list_t *cluster_list) {
  cluster_desc_t                 cluster_desc[ROZOFS_CLUSTERS_MAX];
  list_t                       * p;
  uint32_t                       nb_cluster = 0;
  cluster_desc_t               * pCluster;


  /*
  ** Prepare the list of cluster from the configuration
  */
  pCluster = &cluster_desc[0];
  list_for_each_forward(p, cluster_list) {

    /*
    ** Get cluster
    */
    cluster_t *volume_cluster = list_entry(p, cluster_t, list);

    /*
    ** Fill working structure
    */
    pCluster->weight = 1;    
    pCluster->cid    = volume_cluster->cid;
    
    /*
    ** Next cluster
    */
    nb_cluster++;
    pCluster++;
  }
  
  /*
  ** Build the distribution sequence from this descriptor
  */
  return rozofs_build_cluster_distributor(nb_cluster,cluster_desc);
}
/*
**________________________________________________________________________________________________
** Compute table of distribution betwen clusters in order to equally distribute upon every cluster
** with different size
**
** retval The distribution table
**________________________________________________________________________________________________
*/
#define ROZOFS_MAX_weight_FREE_SIZE 31
void * rozofs_cluster_distributor_create_wfsz(list_t *cluster_list) {
  cluster_desc_t                 cluster_desc[ROZOFS_CLUSTERS_MAX];
  list_t                       * p;
  uint32_t                       nb_cluster = 0;
  cluster_desc_t               * pCluster;
  uint64_t                       freeMax = 0;
  int                            i;

  /*
  ** Prepare the list of cluster from the configuration
  */
  pCluster = &cluster_desc[0];
  list_for_each_forward(p, cluster_list) {

    /*
    ** Get cluster
    */
    cluster_t *volume_cluster = list_entry(p, cluster_t, list);

    /*
    ** Fill workind structure
    */
    pCluster->cid   = volume_cluster->cid;
    
    /*
    ** Get the maximum free size value of a cluster
    */
    if (volume_cluster->free > freeMax) {
      freeMax = volume_cluster->free;
    } 
    /*
    ** The weight is build from the free cluster size. We want 
    ** ROZOFS_MAX_weight_FREE_SIZE to be the maximum weight of a cluster
    */
    pCluster->weight = volume_cluster->free * ROZOFS_MAX_weight_FREE_SIZE;
    
    /*
    ** Next cluster
    */
    nb_cluster++;
    pCluster++;
  }
  
  /*
  ** Well !!!
  */
  if (freeMax == 0) return NULL;
  freeMax++;
  
  /*
  ** Divide every weight by freeMax to get a maximum weight
  ** of ROZOFS_MAX_weight_FREE_SIZE
  */
  pCluster = &cluster_desc[0];
  for (i=0; i<nb_cluster; i++) {
    pCluster->weight = pCluster->weight / freeMax;
    if (pCluster->weight == 0) pCluster->weight = 1;
    pCluster++;    
  }
  
  /*
  ** Build the distribution sequence from this descriptor
  */
  return rozofs_build_cluster_distributor(nb_cluster,cluster_desc);
}
/*
**________________________________________________________________________________________________
** Call the cluster distributor creator depending on the configuration
**
** @retval The distribution table
**________________________________________________________________________________________________
*/
static inline void * rozofs_cluster_distributor_create(list_t *cluster_list) {
  void * ret = NULL;
  switch (common_config.file_distribution_rule) {

    /*
    ** Weight is given by the free size
    */
    case rozofs_file_distribution_wfsz_write:  
    case rozofs_file_distribution_wfsz_read: 
      ret = rozofs_cluster_distributor_create_wfsz(cluster_list);
      if (ret != NULL) return ret;
      warning("Can not allocate WFSZ distributor. Try WSID...");

    /*
    ** weight is given by the SID number
    */
    case rozofs_file_distribution_wsid_write:
    case rozofs_file_distribution_wsid_read: 
      ret = rozofs_cluster_distributor_create_wsid(cluster_list);
      return ret;

    /*
    ** Strict round robin
    */
    default:
      ret = rozofs_cluster_distributor_create_round_robin(cluster_list);
      return ret;
  }
  return NULL;
}        
/*
**________________________________________________________________________________________________
** Get next cluster in the pre-calculated distribution sequence
**
** retval The distribution table
**________________________________________________________________________________________________
*/
uint16_t rozofs_cluster_next(volume_t *volume) {
  rozofs_cluster_distributor_t * pDistributor;
  int                            idx;

  pDistributor = volume->cluster_distibutor;
  
  idx = pDistributor->counter++;
  idx %= pDistributor->max;
  return pDistributor->value[idx];
}  
/*
**________________________________________________________________________________________________
** Poll every sid of the volume to get the updated size of the whole volume, and eventualy 
** re-order the volume for further file allocation depending on the configured distribution
** rule
**
** @param volume     The volume to work on
**
**________________________________________________________________________________________________
*/
void volume_balance(volume_t *volume) {
    list_t *p, *q;
    list_t   * pList;
    list_t     cnx;
    int        new;
    rozofs_cluster_distributor_t * oldD=NULL;
    rozofs_cluster_distributor_t * newD;
    uint64_t   volume_free_space  = 0;
    uint64_t   volume_total_space = 0;
    
    rozofs_cluster_distribution_rule_e rule = rozofs_get_cluster_distribution_rule(common_config.file_distribution_rule);
        
    START_PROFILING_0(volume_balance);

    list_init(&cnx);    
    int local_site = export_get_local_site_number();
    
    /*
    ** Re-initialize the inactive cluster distribution list from the configured cluster list.
    */
    pList = &volume->cluster_distribute[1 - volume->active_list];
    if (volume_safe_to_list_copy(volume,pList) != 0) {
        severe("can't volume_safe_to_list_copy: %u %s", volume->vid,strerror(errno));
        goto out;
    }              

    /*
    ** Loop on this list to check the storage status and free storage
    */
    list_for_each_forward(p, pList) {
        cluster_t *cluster = list_entry(p, cluster_t, list);

        cluster->free = 0;
        cluster->size = 0;

        list_for_each_forward(q, (&cluster->storages[local_site])) {
            volume_storage_t *vs = list_entry(q, volume_storage_t, list);
	    
            mstorage_client_t * mclt;
	    
	    mclt = mstorage_client_get(&cnx, vs->host, cluster->cid, vs->sid);
            
            new = 0;
            
            if (mclt) {
              if (mstoraged_client_stat(mclt, cluster->cid, vs->sid, &vs->stat) == 0) {
                new = 1;
              }  
            }		    
	    
	    // Status has changed
	    if (vs->status != new) {
	      vs->status = new;
	      if (new == 0) {
                warning("storage host '%s' unreachable: %s", vs->host, strerror(errno));	        
	      }
	      else {
                info("storage host '%s' is now reachable", vs->host);	         
	      }
	    }

            // Update cluster stats
	    if (new) {
              cluster->free += vs->stat.free;
              cluster->size += vs->stat.size;
            }
        }
        
        volume_free_space  += cluster->free;
        volume_total_space += cluster->size;
    } 
    mstoraged_release_cnx(&cnx);

    /*
    ** Check whether the volume should be declared as full or not
    */
    if (common_config.minimum_free_size_percent == 0) {
      volume->full = 0;
    }
    else if (volume_total_space != 0) {
      volume_free_space *= 100;
      volume_free_space /= volume_total_space;
      if (volume_free_space <= common_config.minimum_free_size_percent) {
        volume->full = 1;
      }
      else {
        volume->full = 0;
      }  
    }
    
    /*
    ** case of the geo-replication
    */
    if (volume->georep)
    {
      list_for_each_forward(p, pList) {
          cluster_t *cluster = list_entry(p, cluster_t, list);

          list_for_each_forward(q, (&cluster->storages[1-local_site])) {
              volume_storage_t *vs = list_entry(q, volume_storage_t, list);

              mstorage_client_t * mclt;
	    
	      mclt = mstorage_client_get(&cnx, vs->host, cluster->cid, vs->sid);
            
              new = 0;
             
              if (mclt) {
                if (mstoraged_client_stat(mclt, cluster->cid, vs->sid, &vs->stat) == 0) {
                  new = 1;
                }  
              }		    
	      // Status has changed
	      if (vs->status != new) {
	        vs->status = new;
	        if (new == 0) {
                  warning("remote site storage host '%s' unreachable: %s", vs->host, strerror(errno));	        
	        }
	        else {
                  info("remote site storage host '%s' is now reachable", vs->host);	         
	        }
              }  
          }

      }
      mstoraged_release_cnx(&cnx);
    }  
    
      
    /* 
    ** Order the storages in the clusters, and then the cluster
    */
    if (rule == rozofs_cluster_distribution_rule_size_balancing) {
	
      list_for_each_forward(p, pList) {
	  
        cluster_t *cluster = list_entry(p, cluster_t, list);
		
        list_sort((&cluster->storages[local_site]), volume_storage_compare);

	if (volume->georep) {
    	  /*
    	  ** do it also for the remote site
    	  */
          list_sort((&cluster->storages[1-local_site]), volume_storage_compare);
    	}
      }
	  
	  
      list_sort(pList, cluster_compare_capacity);
    }

    /*
    ** Case of weighted round robin algorithm
    */ 
    
    oldD = NULL;
    while (rule == rozofs_cluster_distribution_rule_weighted_round_robin) {
    
      oldD = (rozofs_cluster_distributor_t *) volume->cluster_distibutor;
      
      /*
      ** Create 1rst distributor
      */
      if (oldD == NULL) {
        newD = rozofs_cluster_distributor_create(pList); 
        volume->cluster_distibutor = newD; 
        break;
      }
      
      /*
      ** Create a new distributor
      */      
      newD = rozofs_cluster_distributor_create(pList); 
      if (newD == NULL) {
        oldD = NULL; // No update
        break;          
      }
      
      /*
      ** Compare to old distributor
      */              
      if ((newD->max == oldD->max) && (memcmp(&newD->value[0], &oldD->value[0], newD->max * sizeof(int))==0)) {
        xfree(newD);
        newD = NULL;
        oldD = NULL;  // No update
        break;
      }
                    
      /*
      ** Switch distributors
      */
      newD->counter = oldD->counter;
      volume->cluster_distibutor = newD;
      break;  
    }
    
    // Copy the result back to our volume
    if (volume_safe_from_list_copy(volume,pList) != 0) {
        severe("can't volume_safe_from_list_copy: %u %s", volume->vid,strerror(errno));
        goto out;
    }


    /*
    ** Use this new list as the next cluster distribution list,
    ** --> exception: in stict round robin mode, keep the the distribution list as it is
    **     --> exception: on the 1rst call, the cluster distibution list has to be initialized
    */    
    if (volume->balanced == 0) goto swap;
    
    if (rule == rozofs_cluster_distribution_rule_weighted_round_robin) { 
       goto out;
    }
    
swap:
    volume->active_list = 1 - volume->active_list;
    volume->balanced = 1;    
out:
    /*
    ** Old distributor is to be freed : partir ayeeeeur....
    */
    if (oldD != NULL) {
      /*
      ** Let time to the main thread before freeing this distibutor
      */
      usleep(2000000);
      xfree(oldD);
      oldD = NULL;      
    }
    STOP_PROFILING_0(volume_balance);
}
/*
** Some usefull function to sort a list of storage node context
** depending on some criteria
** Either inverseCounter or forwardCounter or spareCounter
*/
int compareInverse(list_t * a, list_t * b) {
  volume_storage_t  * A = list_entry(a, volume_storage_t, list);
  volume_storage_t  * B = list_entry(b, volume_storage_t, list);

//  if (A->inverseCounter == B->inverseCounter) return (A->forwardCounter - B->forwardCounter);
  return (A->inverseCounter - B->inverseCounter);  
}
int compareForward(list_t * a, list_t * b) {
  volume_storage_t  * A = list_entry(a, volume_storage_t, list);
  volume_storage_t  * B = list_entry(b, volume_storage_t, list);

  return (A->forwardCounter - B->forwardCounter); 
}
int compareSpare(list_t * a, list_t * b) {
  volume_storage_t  * A = list_entry(a, volume_storage_t, list);
  volume_storage_t  * B = list_entry(b, volume_storage_t, list);
  
  return A->spareCounter - B->spareCounter;  
}
void do_reorderInverse(list_t * l) {
  list_sort(l, compareInverse);
}
void  do_reorderForward(list_t * l) {
  list_sort(l, compareForward);
}  
void  do_reorderSpare(list_t * l) {
  list_sort(l, compareSpare);
}

// what if a cluster is < rozofs safe
#define DISTTRACE(fmt,...)
static int do_cluster_distribute_strict_round_robin(uint8_t layout,int site_idx, cluster_t *cluster, sid_t *sids, uint8_t multi_site) {
  int        nb_selected=0; 
  int        location_collision = 0; 
  int        location_bit;
  int        loop;
  volume_storage_t *vs;
  list_t           *pList = &cluster->storages[site_idx];
  list_t           *p;
  int               sid;

  uint8_t rozofs_inverse=0; 
  uint8_t rozofs_forward=0;
  uint8_t rozofs_safe=0;
  uint64_t    selectedBitMap[4];
  uint64_t    locationBitMap[4];
  int         weight;
  

  rozofs_get_rozofs_invers_forward_safe(layout,&rozofs_inverse,&rozofs_forward,&rozofs_safe);
  
  ROZOFS_BITMAP64_ALL_RESET(selectedBitMap);
  ROZOFS_BITMAP64_ALL_RESET(locationBitMap);
  
  /* 
  ** Sort the storage list, to put the less used in the
  ** inverse sid of a distribution
  */
  do_reorderInverse(pList);
  
  /*
  ** Get the first inverse sid
  */

  location_collision = 0;

  loop = 0;
  while (loop < 4) {
    loop++;

    list_for_each_forward(p, pList) {

      vs = list_entry(p, volume_storage_t, list);
	  sid = vs->sid;

      /* SID already selected */
	  if (ROZOFS_BITMAP64_TEST1(sid,selectedBitMap)) {
        DISTTRACE("sid%d already taken", vs->sid);
	    continue;
      }

      /* 
      ** In multi site location is the site number.
      ** else location is the host number within the cluter
      ** Is there one sid already allocated on this location ?
      */
      if (multi_site) location_bit = vs->siteNum;
      else            location_bit = vs->host_rank;
      if (ROZOFS_BITMAP64_TEST1(location_bit, locationBitMap)) {
		DISTTRACE("sid%d location collision %x  weight %d", vs->sid, location_bit, vs->inverseCounter);
		location_collision++;	    
		continue;
      }

      /*
      ** Take this guy
      */
      ROZOFS_BITMAP64_SET(sid,selectedBitMap);
      ROZOFS_BITMAP64_SET(location_bit,locationBitMap);	  
      vs->inverseCounter++;
      vs->forwardCounter++;	
      sids[nb_selected++] = sid;

      DISTTRACE("sid%d is #%d selected with location bit %x weight %d", vs->sid, nb_selected, location_bit, vs->inverseCounter);

      /* Enough sid found */
      if (rozofs_inverse==nb_selected) {
		DISTTRACE("inverse done");
		goto forward;
      }
    }
    DISTTRACE("end loop %d nb_selected %d location_collision %d", loop, nb_selected, location_collision);
    
    if ((nb_selected+location_collision) < rozofs_inverse) return  -1;
    // Reset location condition before re looping
    ROZOFS_BITMAP64_ALL_RESET(locationBitMap);
    location_collision =0;
  }
  return -1;

forward:
  /* 
  ** Sort the storage list, to put the less used in the
  ** forward sid of a distribution
  */
  do_reorderForward(pList);
  
  /*
  ** Get the next forward sid
  */
  loop = 0;
  while (loop < 4) {
    loop++;

    list_for_each_forward(p, pList) {

      vs = list_entry(p, volume_storage_t, list);
	  sid = vs->sid;

      /* SID already selected */
	  if (ROZOFS_BITMAP64_TEST1(sid,selectedBitMap)) {
        DISTTRACE("isid%d already taken", vs->sid);
	    continue;
      }

      /* 
      ** In multi site location is the site number.
      ** else location is the host number within the cluter
      ** Is there one sid already allocated on this location ?
      */
      if (multi_site) location_bit = vs->siteNum;
      else            location_bit = vs->host_rank;
      if (ROZOFS_BITMAP64_TEST1(location_bit, locationBitMap)) {
		DISTTRACE("sid%d location collision %x weight %d", vs->sid, location_bit, vs->forwardCounter);
		location_collision++;	    
		continue;
      }

      /*
      ** Take this guy
      */
      ROZOFS_BITMAP64_SET(sid,selectedBitMap);
      ROZOFS_BITMAP64_SET(location_bit,locationBitMap);
      vs->forwardCounter++;	
      sids[nb_selected++] = sid;

      DISTTRACE("sid%d is #%d selected with location bit %x weight %d", vs->sid, nb_selected, location_bit, vs->forwardCounter);

      /* Enough sid found */
      if (rozofs_forward==nb_selected) {
		DISTTRACE("forward done");
		goto spare;
      }
    }
    DISTTRACE("end loop %d nb_selected %d location_collision %d", loop, nb_selected, location_collision);
    
    if ((nb_selected+location_collision) < rozofs_forward) return  -1;    
    // Reset location condition before re looping
    ROZOFS_BITMAP64_ALL_RESET(locationBitMap);
    location_collision =0;
  }
  return -1;
  
spare:    
  /* 
  ** Sort the storage list, to put the less used in the
  ** forward sid of a distribution
  */
  do_reorderSpare(pList);
  
  /*
  ** Get the next forward sid
  */
  loop = 0;
  /* 
  ** The probability to receive a spare projection depends on the rank in the list of spare
  ** so the weight given to a spare role depends on the rank
  ** in layout 1: 1rst spare has a weight of 2, 2nd spare a weight of 1
  ** in layout 2: 1rst spare has a weight of 4, 2nd spare a weight of 3...
  */
  weight = rozofs_safe-rozofs_forward; 
  while (loop < 4) {
    loop++;

    list_for_each_forward(p, pList) {

      vs = list_entry(p, volume_storage_t, list);
	  sid = vs->sid;

      /* SID already selected */
	  if (ROZOFS_BITMAP64_TEST1(sid,selectedBitMap)) {
        DISTTRACE("sid%d already taken", vs->sid);
	    continue;
      }
	  
      /* 
      ** In multi site location is the site number.
      ** else location is the host number within the cluter
      ** Is there one sid already allocated on this location ?
      */
      if (multi_site) location_bit = vs->siteNum;
      else            location_bit = vs->host_rank;
      if (ROZOFS_BITMAP64_TEST1(location_bit, locationBitMap)) {
		DISTTRACE("sid%d location collision %x weight %d", vs->sid, location_bit, vs->spareCounter);
		location_collision++;	    
		continue;
      }

      /*
      ** Take this guy
      */
      ROZOFS_BITMAP64_SET(sid,selectedBitMap);
      ROZOFS_BITMAP64_SET(location_bit,locationBitMap);
      vs->spareCounter += weight;
      if (weight > 1) {
        weight--;	// Next spare will have lower weight since less probability to receive a spare prj
      }  
      sids[nb_selected++] = sid;

      DISTTRACE("sid%d is #%d selected with location bit %x with status %d", vs->sid, nb_selected, location_bit, vs->spareCounter);

      /* Enough sid found */
      if (rozofs_safe==nb_selected) {
		DISTTRACE("spare done");
		return 0;
      }
    }
    
    if ((nb_selected+location_collision) < rozofs_safe) return  -1;    
    // Reset location condition before re looping
    ROZOFS_BITMAP64_ALL_RESET(locationBitMap);
    location_collision =0;
  }
  return -1;  
}

/*
**___________________________________________________________________________________________________
** Distribute files in a size balancing maner within a cluster
**
** @param layout        The layout to use
** @param site_idx      Local site number for geo replication (deprecated)
** @param cluster       Selected Cluster identifier (to be returned)
** @param sids          Selected sids withing the cluster (to be returned)
** @param multi_site    Whether multi site distribution has to be applied
**
** @retval 0 on success, -1 else (errno is set)
**___________________________________________________________________________________________________
*/
static int do_cluster_distribute_size_balancing(uint8_t layout,int site_idx, cluster_t *cluster, sid_t *sids, uint8_t multi_site) {
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
  list_t           *pList = &cluster->storages[site_idx];
  list_t           *p;
  uint64_t          decrease_size;

  uint8_t rozofs_inverse=0; 
  uint8_t rozofs_forward=0;
  uint8_t rozofs_safe=0;

  rozofs_get_rozofs_invers_forward_safe(layout,&rozofs_inverse,&rozofs_forward,&rozofs_safe);
  
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
  ** In weighted round robin and in size equalizing decrease the estimated size 
  ** of the storages and re-order them in the cluster
  */
  decrease_size = common_config.alloc_estimated_mb*(1024*1024);
  idx = 0;

  while(idx < rozofs_inverse) {
  
    vs = selected[idx];
    sids[idx] = vs->sid;
    if (decrease_size) {
      if (vs->stat.free > (256*decrease_size)) {
	vs->stat.free -= decrease_size;
      }
      else if (vs->stat.free > (64*decrease_size)) {
	vs->stat.free -= (decrease_size/2);      
      }
      else if (vs->stat.free > decrease_size) {
	vs->stat.free -= (decrease_size/8);
      }
      else {
	vs->stat.free /= 2;
      }
    }
    idx++;
  }
  
  decrease_size = decrease_size /2;
  while(idx < rozofs_forward) {
  
    vs = selected[idx];
    sids[idx] = vs->sid;

    if (decrease_size) {
      if (vs->stat.free > (256*decrease_size)) {
	vs->stat.free -= decrease_size;
      }
      else if (vs->stat.free > (64*decrease_size)) {
	vs->stat.free -= (decrease_size/2);      
      }
      else if (vs->stat.free > decrease_size) {
	vs->stat.free -= (decrease_size/8);
      }
      else {
	vs->stat.free /= 2;
      }
    }  
    idx++;
  } 

  decrease_size = decrease_size /16;   
  while(idx < rozofs_safe) {
  
    vs = selected[idx];
    sids[idx] = vs->sid;

    if (decrease_size) {
      if (vs->stat.free > (256*decrease_size)) {
	vs->stat.free -= decrease_size;
      }
      else if (vs->stat.free > (64*decrease_size)) {
	vs->stat.free -= (decrease_size/2);      
      }
      else if (vs->stat.free > decrease_size) {
	vs->stat.free -= (decrease_size/8);
      }
      else {
	vs->stat.free /= 2;
      }
    }  
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
  return 0;
}
/*
**___________________________________________________________________________________________________
** Distribute files in a size balancing maner
**
** @param layout        The layout to use
** @param volume        The volume to use
** @param site_number   Local site number for geo replication (deprecated)
** @param cid           Selected Cluster identifier (to be returned)
** @param sids          Selected sids withing the cluster (to be returned)
**
** @retval 0 on success, -1 else (errno is set)
**___________________________________________________________________________________________________
*/
static inline int volume_distribute_size_balancing(uint8_t layout, volume_t *volume,int site_number, cid_t *cid, sid_t *sids) {
  list_t     * p,* q;
  int          site_idx;
  list_t     * cluster_distribute;

  site_idx = export_get_local_site_number();


  if (volume->georep)
  {
    site_idx = site_number;
  }

  /*
  ** Get active cluster list
  ** This list has been prepared by the volume balance process
  ** Ths clusters are order in available size decreasing order
  ** and sids whithin the cluster too.
  */
  cluster_distribute = &volume->cluster_distribute[volume->active_list];

  list_for_each_forward(p, cluster_distribute) {

    cluster_t *next_cluster;
    cluster_t *cluster = list_entry(p, cluster_t, list);


    if (do_cluster_distribute_size_balancing(layout, site_idx, cluster, sids, volume->multi_site) == 0) {

      *cid = cluster->cid;

      /*
      ** In size equalizing, Re-order the clusters
      */	      
      while (1) {

	q = p->next;

	// This cluster is the last and so the smallest
	if (q == cluster_distribute) break;

	// Check against next cluster
	next_cluster = list_entry(q, cluster_t, list);
	if (cluster->free > next_cluster->free) break;

	// Next cluster has to be set before the current one		
	q->prev       = p->prev;
	q->prev->next = q;
	p->next       = q->next;
	p->next->prev = p;
	q->next       = p;
	p->prev       = q;
      }

      return 0;
    }
  }

  errno = ENOSPC;
  return -1;
}
/*
**___________________________________________________________________________________________________
** Distribute file in a weighted round robin
**
** @param layout        The layout to use
** @param volume        The volume to use
** @param site_number   Local site number for geo replication (deprecated)
** @param cid           Selected Cluster identifier (to be returned)
** @param sids          Selected sids withing the cluster (to be returned)
**
** @retval 0 on success, -1 else (errno is set)
**___________________________________________________________________________________________________
*/
static inline int volume_distribute_round_robin(uint8_t layout, volume_t *volume,int site_number, cid_t *cid, sid_t *sids) {
  list_t    * p;
  int         site_idx;
  list_t    * cluster_distribute;


  site_idx = export_get_local_site_number();

  if (volume->georep)
  {
    site_idx = site_number;
  }


  /*
  ** Get active cluster list
  ** This list has been prepared by the volume balance process
  ** The clusters are order in available size decreasing order
  */

  cluster_distribute = &volume->cluster_distribute[volume->active_list];

  list_for_each_forward(p, cluster_distribute) {

    cluster_t *cluster = list_entry(p, cluster_t, list);

    if (do_cluster_distribute_strict_round_robin(layout,site_idx, cluster, sids, volume->multi_site) == 0) {

      *cid = cluster->cid;

      /* 
      ** Put the cluster to the end of the list 
      */    
      list_remove(&cluster->list);
      list_push_back(cluster_distribute, &cluster->list);
      return 0;
    }
  }

  errno = ENOSPC;
  return -1;
}
/*
**___________________________________________________________________________________________________
** Distribute file in a size balancing maner
**
** @param layout        The layout to use
** @param volume        The volume to use
** @param site_number   Local site number for geo replication (deprecated)
** @param cid           Selected Cluster identifier (to be returned)
** @param sids          Selected sids withing the cluster (to be returned)
**
** @retval 0 on success, -1 else (errno is set)
**___________________________________________________________________________________________________
*/
static inline int volume_distribute_weighted_round_robin(uint8_t layout, volume_t *volume,int site_number, cid_t *cid, sid_t *sids) {
  list_t    * p;
  int         site_idx;
  list_t    * cluster_distribute;
  uint16_t    next_cid;
  int         loop;

  /*
  ** Check the distribution table is available
  */
  if (volume->cluster_distibutor == NULL) {
    /*
    ** No weighted distribution table. Go for strict round robin
    */ 
    return volume_distribute_round_robin(layout,volume, site_number, cid, sids);     
  }
  
  site_idx = export_get_local_site_number();

  if (volume->georep)
  {
    site_idx = site_number;
  }

  /*
  ** Try to follow the pre-calculated list of CID to allocate from.
  ** When allocation fails attempt the next CID in the list...
  ** ...and so on for 3 trials 
  */
  for (loop=0; loop<3; loop++) { 

    /*
    ** Get next cluster id to allocate from 
    */
    next_cid = rozofs_cluster_next(volume);

    /*
    ** Get active cluster list
    ** This list is never updated by the volume balance process
    */
    cluster_distribute = &volume->cluster_distribute[volume->active_list];

    list_for_each_forward(p, cluster_distribute) {

      cluster_t *cluster = list_entry(p, cluster_t, list);
      if (cluster->cid != next_cid) continue;

      if (do_cluster_distribute_strict_round_robin(layout,site_idx, cluster, sids,volume->multi_site) == 0) {

        *cid = cluster->cid;

	/* 
        ** Put the cluster to the end of the list
        ** which is the more probable next rank when it will be re-used 
        */    
	list_remove(&cluster->list);
	list_push_back(cluster_distribute, &cluster->list);
        return 0;
      }
      break;
    }
  }

  /*
  ** Get the first cluster that works
  */ 
  return volume_distribute_round_robin(layout,volume, site_number, cid, sids);   
}
/*
**___________________________________________________________________________________________________
** Call distribution algorithm for a new file that is being created
**
** @param layout        The layout to use
** @param volume        The volume to use
** @param site_number   Local site number for geo replication (deprecated)
** @param cid           Selected Cluster identifier (to be returned)
** @param sids          Selected sids withing the cluster (to be returned)
**
** @retval 0 on success, -1 else (errno is set)
**___________________________________________________________________________________________________
*/
int volume_distribute(uint8_t layout, volume_t *volume,int site_number, cid_t *cid, sid_t *sids) {
  int status = -1;
  
  START_PROFILING(volume_distribute);
  errno = 0;
    
  switch(rozofs_get_cluster_distribution_rule(common_config.file_distribution_rule)) {

    case rozofs_cluster_distribution_rule_size_balancing:
      status = volume_distribute_size_balancing(layout, volume, site_number, cid, sids) ;
      break;

    case rozofs_cluster_distribution_rule_weighted_round_robin:
      status = volume_distribute_weighted_round_robin(layout, volume, site_number, cid, sids) ;
      break;        

    default:
      severe("No such distribution algorithm %d",common_config.file_distribution_rule);
      status = volume_distribute_size_balancing(layout, volume, site_number, cid, sids) ;
      break;
  }	
  
  STOP_PROFILING(volume_distribute);
  return status; 
}
/*
**___________________________________________________________________________________________________
**___________________________________________________________________________________________________
*/
void volume_stat(volume_t *volume, volume_stat_t *stat) {
    list_t *p;
    DEBUG_FUNCTION;
    START_PROFILING_0(volume_stat);

    stat->bsize = 1024;
    stat->bfree = 0;
    stat->blocks = 0;
    uint8_t rozofs_forward = rozofs_get_rozofs_forward(volume->layout);
    uint8_t rozofs_inverse = rozofs_get_rozofs_inverse(volume->layout);

    if ((errno = pthread_rwlock_rdlock(&volume->lock)) != 0) {
        warning("can't lock volume %u.", volume->vid);
    }

    list_for_each_forward(p, &volume->clusters) {
        stat->bfree += list_entry(p, cluster_t, list)->free / stat->bsize;
        stat->blocks += list_entry(p, cluster_t, list)->size / stat->bsize;
    }

    if ((errno = pthread_rwlock_unlock(&volume->lock)) != 0) {
        warning("can't unlock volume %u.", volume->vid);
    }

    stat->bfree = (long double) stat->bfree / ((double) rozofs_forward /
            (double) rozofs_inverse);
    stat->blocks = (long double) stat->blocks / ((double) rozofs_forward /
            (double) rozofs_inverse);

    STOP_PROFILING_0(volume_stat);
}

int volume_distribution_check(volume_t *volume, int rozofs_safe, int cid, int *sids) {
    list_t * p;
    int xerrno = EINVAL;
    int nbMatch = 0;
    int idx;

    int local_site = export_get_local_site_number();

    if ((errno = pthread_rwlock_rdlock(&volume->lock)) != 0) {
        warning("can't lock volume %u.", volume->vid);
        goto out;
    }

    list_for_each_forward(p, &volume->clusters) {
        cluster_t *cluster = list_entry(p, cluster_t, list);

        if (cluster->cid == cid) {

            list_for_each_forward(p, (&cluster->storages[local_site])) {
                volume_storage_t *vs = list_entry(p, volume_storage_t, list);

                for (idx = 0; idx < rozofs_safe; idx++) {
                    if (sids[idx] == vs->sid) {
                        nbMatch++;
                        break;
                    }
                }

                if (nbMatch == rozofs_safe) {
                    xerrno = 0;
                    break;
                }
            }
            break;
        }
    }
    if ((errno = pthread_rwlock_unlock(&volume->lock)) != 0) {
        warning("can't unlock volume %u.", volume->vid);
        goto out;
    }
out:
    errno = xerrno;
    return errno == 0 ? 0 : -1;
}
