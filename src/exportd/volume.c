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

#include "volume.h"
#include "export.h"

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
    volume->layout = layout;
    if (rebalanceCfg) {
      volume->rebalanceCfg = xstrdup(rebalanceCfg);
    }
    else {
      volume->rebalanceCfg = NULL;
    }
    list_init(&volume->clusters);
    
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
uint8_t export_rotate_sid[ROZOFS_CLUSTERS_MAX] = {0};
  
void volume_balance(volume_t *volume) {
    list_t *p, *q;
    list_t   * pList;
    list_t     cnx;
    int        new;
    
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

    }
    mstoraged_release_cnx(&cnx);
    
    
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
    if ((common_config.file_distribution_rule == rozofs_file_distribution_size_balancing)
	||  (common_config.file_distribution_rule == rozofs_file_distribution_weigthed_round_robin)) {
	
	
      list_for_each_forward(p, pList) {
	  
        cluster_t *cluster = list_entry(p, cluster_t, list);
        export_rotate_sid[cluster->cid] = 0;
		
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
    if  (volume->balanced == 0) goto swap;
    switch(common_config.file_distribution_rule) {
      case rozofs_file_distribution_strict_round_robin_forward: 
      case rozofs_file_distribution_strict_round_robin_inverse: goto out;
    }
    
swap:
    volume->active_list = 1 - volume->active_list;
    volume->balanced = 1;    
out:
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
		DISTTRACE("sid%d location collision %x  weigth %d", vs->sid, location_bit, vs->inverseCounter);
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

      DISTTRACE("sid%d is #%d selected with location bit %x weigth %d", vs->sid, nb_selected, location_bit, vs->inverseCounter);

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
		DISTTRACE("sid%d location collision %x weigth %d", vs->sid, location_bit, vs->forwardCounter);
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

      DISTTRACE("sid%d is #%d selected with location bit %x weigth %d", vs->sid, nb_selected, location_bit, vs->forwardCounter);

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
  ** so the weigth given to a spare role depends on the rank
  ** in layout 1: 1rst spare has a weigth of 2, 2nd spare a weigth of 1
  ** in layout 2: 1rst spare has a weigth of 4, 2nd spare a weigth of 3...
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
		DISTTRACE("sid%d location collision %x weigth %d", vs->sid, location_bit, vs->spareCounter);
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
        weight--;	// Next spare will have lower weigth since less probability to receive a spare prj
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
static int do_cluster_distribute_weighted_round_robin(uint8_t layout,int site_idx, cluster_t *cluster, sid_t *sids, uint8_t multi_site) {
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
  
//  int modulo = export_rotate_sid[cluster->cid] % rozofs_forward;
//  export_rotate_sid[cluster->cid]++;

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
        //DISTTRACE("idx%d/sid%d already taken", idx, vs->sid);
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
  return 0;
}

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
  
//  int modulo = export_rotate_sid[cluster->cid] % rozofs_forward;
//  export_rotate_sid[cluster->cid]++;

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
static int do_cluster_distribute(uint8_t layout,int site_idx, cluster_t *cluster, sid_t *sids, uint8_t multi_site) {


  switch(common_config.file_distribution_rule) {
  
    case rozofs_file_distribution_size_balancing:
	  return do_cluster_distribute_size_balancing(layout, site_idx, cluster, sids, multi_site);
	  break;
	case rozofs_file_distribution_weigthed_round_robin:
	  return do_cluster_distribute_weighted_round_robin(layout, site_idx, cluster, sids, multi_site);
	  break;
    case rozofs_file_distribution_strict_round_robin_forward:
	case rozofs_file_distribution_strict_round_robin_inverse:
	  return do_cluster_distribute_strict_round_robin(layout, site_idx, cluster, sids, multi_site);	
      break;
  }	
  
  severe("No such distribution rule %d\n",common_config.file_distribution_rule);
  return -1;    
}
int volume_distribute(uint8_t layout, volume_t *volume,int site_number, cid_t *cid, sid_t *sids) {
    list_t *p,*q;
    int xerrno = ENOSPC;
    int site_idx;
    list_t * cluster_distribute;
    

    DEBUG_FUNCTION;
    START_PROFILING(volume_distribute);
    
    site_idx = export_get_local_site_number();

    
    if (volume->georep)
    {
      site_idx = site_number;
    }

    cluster_distribute = &volume->cluster_distribute[volume->active_list];
    
    list_for_each_forward(p, cluster_distribute) {
    
      cluster_t *next_cluster;
      cluster_t *cluster = list_entry(p, cluster_t, list);

      if (do_cluster_distribute(layout,site_idx, cluster, sids,volume->multi_site) == 0) {

        *cid = cluster->cid;
        xerrno = 0;

	/* In round robin mode put the cluster to the end of the list */    
	if (common_config.file_distribution_rule != rozofs_file_distribution_size_balancing){
	  list_remove(&cluster->list);
	  list_push_back(cluster_distribute, &cluster->list);
	  break;
	}

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

        break;
      }
    }
    
    STOP_PROFILING(volume_distribute);
    errno = xerrno;
    return errno == 0 ? 0 : -1;
}

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
