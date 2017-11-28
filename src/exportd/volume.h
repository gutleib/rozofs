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

#ifndef _VOLUME_H
#define _VOLUME_H

#include <stdint.h>
#include <limits.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/list.h>

/** status of a volume */
typedef struct volume_stat {
    uint16_t bsize; ///< the block size
    uint64_t bfree; ///< number of free blocks
    uint64_t blocks; ///< number of  blocks
} volume_stat_t;

/** a managed storage */
typedef struct volume_storage {
    sid_t sid; ///< storage identifier
    char host[ROZOFS_HOSTNAME_MAX]; ///< storage host name
    uint8_t host_rank;   /// host number within this cluster for quick comparison
    uint8_t siteNum;     /// Site number where this storage is located
    uint8_t status; ///< status (0 = off line)
    sstat_t stat; ///< storage stat
	
    list_t list; ///< used to chain storages
	
    // 3 counters to know how this storage has been used on different
    // file distribution to try to equalize distribution in strict
    // round robin mode
    uint64_t inverseCounter; // Nb selection in the 1rst inverse SID
    uint64_t forwardCounter; // Nb selection in the 1rst forward SID
    uint64_t spareCounter;   // Nb selection as a spare SID
} volume_storage_t;

/** initialize a volume storage
 *
 * @param vs: the volume storage to initialize
 * @param sid: identifier to set
 * @param hostname : hostname to set (memory is copied)
 */
void volume_storage_initialize(volume_storage_t * vs, sid_t sid,
        const char *hostname, uint8_t hostidx, uint8_t siteNum);

/** release a volume storage
 *
 * has no effect for now
 *
 * @param vs: the volume storage to release
 */
void volume_storage_release(volume_storage_t *vs);

/** a cluster of volume storages
 *
 * volume storages are gather in cluster of volume storage
 * each volume storage should be of the same capacity
 */
typedef struct cluster {
    cid_t cid; ///< cluster identifier
    uint64_t size; ///< cluster size
    uint64_t free; ///< free space on cluster
    list_t storages[ROZOFS_GEOREP_MAX_SITE]; ///< list of storages managed in the cluster
    uint8_t nb_host[ROZOFS_GEOREP_MAX_SITE]; ///< list of storages managed in the cluster
    list_t list; ///< used to chain cluster in a volume
} cluster_t;

/** initialize a cluster
 *
 * @param cluster: the cluster to initialize
 * @param cid: the id to set
 * @param size: the size to set
 * @param free: free space to set
 */
void cluster_initialize(cluster_t *cluster, cid_t cid, uint64_t size,
        uint64_t free);

/** release a cluster
 *
 * empty the list of volume storages and free each entry
 * (assuming entry were well allocated)
 *
 * @param cluster: the cluster to release
 */
void cluster_release(cluster_t *cluster);

/** a volume
 *
 * volume is the storage where data are stored
 * it is made of a pool of clusters and are shared across several exports
 */
typedef struct volume {
    vid_t vid; ///< volume identifier
    uint8_t balanced:1; // Whether volume balance has already been called
    uint8_t layout:6;
    uint8_t georep:1; /**< asserted to 1 when geo-replication is enabled */
    uint8_t multi_site:1; /**< asserted to 1 when geo-replication is enabled */ 
    char *  rebalanceCfg; /**< rebalancer configuration file for this volume */
    list_t clusters; ///< cluster(s) list
    pthread_rwlock_t lock; ///< lock to be used by export
    /*
    ** Updateed by volume_balance() and the used by cluster_distribute()
    */
    int    active_list;
    list_t cluster_distribute[2];  
} volume_t;

/** initialize a volume
 *
 * @param volume: pointer to the volume
 * @param vid: it' id
 * @param layout: layout defined for this volume
 * @param georep: asserted to 1 if geo-replication is supported for the volume
 * @param rebalanceCfg: rebalancer configuration file
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int volume_initialize(volume_t *volume, vid_t vid, uint8_t layout,uint8_t georep,uint8_t multi_site, char * rebalanceCfg);

/** release a volume
 *
 * empty the list of clusters and free each entry
 *
 * @param volume: the volume to release
 */
void volume_release(volume_t *volume);

/** copy a volume
 *
 * Taking care of lock to perform thread safe copy
 * the destination volume will be first reset and then filled
 * with source volume value
 *
 * @param to: destination volume
 * @param from: source volume
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int volume_safe_copy(volume_t *to, volume_t *from);

/** order volume storages to distributed data
 *
 * free capacity is the criterion of cluster sorting. the same is applied
 * inside cluster to sort volume_storages.
 *
 * @param volume: the volume to balance
 */
void volume_balance(volume_t *volume);

/** search the host name of a volume storage according to a sid
 *
 * @param volume: the volume to scan
 * @param sid: the searched sid
 * @param host: destination string where host name is copied
 *
 * @return: the host name (host param) or null if not found
 */
//char *lookup_volume_storage(volume_t *volume, sid_t sid, char *host);

/** gives the cluster and in this cluster the storages to used
 * for a new file.
 *
 * @param layout: the layout to use
 * @param volume: the volume to scan
   @param site_number: needed for geo-replication
 * @param cid: destination cid_t where cid is copied
 * @param host: destination sid_t pointer where sids are copied
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int volume_distribute(uint8_t layout, volume_t *volume,int site_number, cid_t *cid, sid_t *sids);

/** get status of a volume
 *
 * @param volume: the volume to scan
 * @param volume_stat: the volume_stat_t to fill
 */
void volume_stat(volume_t *volume, volume_stat_t *volume_stat);

/** Check that a given distribution is valid
 *
 * @param volume: the volume of the distribution
 * @param rozofs_safe: the number of sid in sids
 * @param cid: the cid within the volume
 * @param sids: the sids within the cluster
 */
int volume_distribution_check(volume_t *volume, int rozofs_safe, int cid, int *sids);
#endif
