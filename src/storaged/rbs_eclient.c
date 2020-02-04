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

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/list.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/rpc/rpcclt.h>
#include "rozofs/rpc/eproto.h"
#include "rozofs/core/rozofs_host_list.h"

#include "rbs.h"
#include "rbs_eclient.h"
/** Send a request to export server for get the list of member storages
 *  of cluster with a given cid and add this storage list to the list
 *  of clusters
 *
 * @param clt: RPC connection to export server
 * @param export_host: IP or hostname of export server
 * @param site: the site identifier
 * @param cid: the unique ID of cluster
 * @param cluster_entries: list of cluster(s)
 *
 * @return: NULL on error, valid export host name on success
 */
char * rbs_get_cluster2_list(rpcclt_t * clt, const char *export_host_list, int site, cid_t cid,
        list_t * cluster_entries, uint8_t * layout, uint16_t * vid) {
    epgw_cluster2_ret_t *ret = 0;
    epgw_cluster_arg_t arg;
    int i = 0;
    int export_idx;
    char * pHost = NULL;
    int retry;

    DEBUG_FUNCTION;

    struct timeval timeo;
    timeo.tv_sec  = 0; 
    timeo.tv_usec = 100000;

    clt->sock = -1;

    /*
    ** Parse host list
    */
    if (rozofs_host_list_parse(export_host_list,'/') == 0) {
        severe("rozofs_host_list_parse(%s)",export_host_list);
    }   
     
    
    for (retry=10; retry > 0; retry--) {
    
      for (export_idx=0; export_idx<ROZOFS_HOST_LIST_MAX_HOST; export_idx++) {

        // Free resources from previous loop
        if (ret) xdr_free((xdrproc_t) xdr_ep_cluster2_ret_t, (char *) ret);
	    rpcclt_release(clt);

	    pHost = rozofs_host_list_get_host(export_idx);
	    if (pHost == NULL) break;


	    // Initialize connection with exportd server
	    if (rpcclt_initialize
        	    (clt, pHost, EXPORT_PROGRAM, EXPORT_VERSION,
        	    ROZOFS_RPC_BUFFER_SIZE, ROZOFS_RPC_BUFFER_SIZE,
		    rozofs_get_service_port_export_master_eproto(), timeo) != 0)
        	continue;

	    // Send request
	    arg.hdr.gateway_rank = site;
	    arg.cid              = cid;

	    ret = ep_list_cluster2_1(&arg, clt->client);
	    if (ret == 0) {
        	errno = EPROTO;
        	continue;
	    }

	    if (ret->status_gw.status == EP_FAILURE) {
        	errno = ret->status_gw.ep_cluster2_ret_t_u.error;
        	continue;
	    }
 
        *layout = ret->status_gw.ep_cluster2_ret_t_u.cluster.layout;
        *vid    = ret->status_gw.ep_cluster2_ret_t_u.cluster.vid;

	    // Allocation for the new cluster entry
	    rb_cluster_t *cluster = (rb_cluster_t *) xmalloc(sizeof (rb_cluster_t));
	    cluster->cid = ret->status_gw.ep_cluster2_ret_t_u.cluster.cid;

	    // Init the list of storages for this cluster
	    list_init(&cluster->storages);

	    // For each storage member
	    for (i = 0; i < ret->status_gw.ep_cluster2_ret_t_u.cluster.storages_nb; i++) {

        	// Init storage
        	rb_stor_t *stor = (rb_stor_t *) xmalloc(sizeof (rb_stor_t));
        	memset(stor, 0, sizeof (rb_stor_t));
        	strncpy(stor->host, ret->status_gw.ep_cluster2_ret_t_u.cluster.storages[i].host,
                	ROZOFS_HOSTNAME_MAX);
        	stor->sid = ret->status_gw.ep_cluster2_ret_t_u.cluster.storages[i].sid;
        	stor->mclient.rpcclt.sock = -1;

        	// Add this storage to the list of storages for this cluster
                list_init(&stor->list);
        	list_push_back(&cluster->storages, &stor->list);
	    }
	    // Add this cluster to the list of clusters
            list_init(&cluster->list);
	    list_push_back(cluster_entries, &cluster->list);

            // Free resources from current loop
            if (ret) xdr_free((xdrproc_t) xdr_ep_cluster2_ret_t, (char *) ret);
	    rpcclt_release(clt);
            return pHost;
	}
	
	if (timeo.tv_usec == 100000) {
	  timeo.tv_usec = 500000; 
	}
	else {
	  timeo.tv_usec = 0;
	  timeo.tv_sec++;	
	}  
    }		
    return NULL;
}
/** Send a request to export server for get the list of member storages
 *  of cluster with a given cid and add this storage list to the list
 *  of clusters
 *
 * @param clt: RPC connection to export server
 * @param export_host: IP or hostname of export server
 * @param site: the site identifier
 * @param cid: the unique ID of cluster
 * @param cluster_entries: list of cluster(s)
 *
 * @return: NULL on error, valid export host name on success
 */
char * rbs_get_cluster_list(rpcclt_t * clt, const char *export_host_list, int site, cid_t cid,
        list_t * cluster_entries) {
    epgw_cluster_ret_t *ret = 0;
    epgw_cluster_arg_t arg;
    int i = 0;
    int export_idx;
    char * pHost = NULL;
    int retry;

    DEBUG_FUNCTION;

    struct timeval timeo;
    timeo.tv_sec  = 0; 
    timeo.tv_usec = 100000;

    clt->sock = -1;

    /*
    ** Parse host list
    */
    if (rozofs_host_list_parse(export_host_list,'/') == 0) {
        severe("rozofs_host_list_parse(%s)",export_host_list);
    }   
     
    
    for (retry=10; retry > 0; retry--) {
    
        for (export_idx=0; export_idx<ROZOFS_HOST_LIST_MAX_HOST; export_idx++) {

            // Free resources from previous loop
            if (ret) xdr_free((xdrproc_t) xdr_ep_cluster_ret_t, (char *) ret);
	    rpcclt_release(clt);

	    pHost = rozofs_host_list_get_host(export_idx);
	    if (pHost == NULL) break;


	    // Initialize connection with exportd server
	    if (rpcclt_initialize
        	    (clt, pHost, EXPORT_PROGRAM, EXPORT_VERSION,
        	    ROZOFS_RPC_BUFFER_SIZE, ROZOFS_RPC_BUFFER_SIZE,
		    rozofs_get_service_port_export_master_eproto(), timeo) != 0)
        	continue;

	    // Send request
	    arg.hdr.gateway_rank = site;
	    arg.cid              = cid;

	    ret = ep_list_cluster_1(&arg, clt->client);
	    if (ret == 0) {
        	errno = EPROTO;
        	continue;
	    }

	    if (ret->status_gw.status == EP_FAILURE) {
        	errno = ret->status_gw.ep_cluster_ret_t_u.error;
        	continue;
	    }

	    // Allocation for the new cluster entry
	    rb_cluster_t *cluster = (rb_cluster_t *) xmalloc(sizeof (rb_cluster_t));
	    cluster->cid = ret->status_gw.ep_cluster_ret_t_u.cluster.cid;

	    // Init the list of storages for this cluster
	    list_init(&cluster->storages);

	    // For each storage member
	    for (i = 0; i < ret->status_gw.ep_cluster_ret_t_u.cluster.storages_nb; i++) {

        	// Init storage
        	rb_stor_t *stor = (rb_stor_t *) xmalloc(sizeof (rb_stor_t));
        	memset(stor, 0, sizeof (rb_stor_t));
        	strncpy(stor->host, ret->status_gw.ep_cluster_ret_t_u.cluster.storages[i].host,
                	ROZOFS_HOSTNAME_MAX);
        	stor->sid = ret->status_gw.ep_cluster_ret_t_u.cluster.storages[i].sid;
        	stor->mclient.rpcclt.sock = -1;

        	// Add this storage to the list of storages for this cluster
                list_init(&stor->list);
        	list_push_back(&cluster->storages, &stor->list);
	    }

	    // Add this cluster to the list of clusters
            list_init(&cluster->list);
	    list_push_back(cluster_entries, &cluster->list);

            // Free resources from current loop
            if (ret) xdr_free((xdrproc_t) xdr_ep_cluster_ret_t, (char *) ret);
	    rpcclt_release(clt);
            return pHost;
	}
	
	if (timeo.tv_usec == 100000) {
	  timeo.tv_usec = 500000; 
	}
	else {
	  timeo.tv_usec = 0;
	  timeo.tv_sec++;	
	}  
    }		
    rpcclt_release(clt);
    return NULL;
}
/** Initialize a storage structure to reach a cid/sid by interogating
 *  the exportd as well as the storaged
 *
 * @param clt: RPC connection to export server
 * @param export_host_list: IP or hostname of export server
 * @param site: the site identifier
 * @param cid: the unique ID of cluster
 * @param sid: the storage identifier
 * @param stor: the storage structure to initialize
 *
 * @return: NULL on error, valid export host name on success
 */
int rbs_get_storage(rpcclt_t * clt, const char *export_host_list, int site, cid_t cid, sid_t sid, rb_stor_t * stor) {
    epgw_cluster_ret_t *ret = 0;
    epgw_cluster_arg_t arg;
    int i = 0;
    int export_idx;
    char * pHost = NULL;
    int retry;
    int status = -1;

    DEBUG_FUNCTION;

    struct timeval timeo;
    timeo.tv_sec  = 0; 
    timeo.tv_usec = 100000;

    clt->sock = -1;
    memset(stor, 0, sizeof (rb_stor_t));
    /*
    ** Parse host list
    */
    if (rozofs_host_list_parse(export_host_list,'/') == 0) {
        severe("rozofs_host_list_parse(%s)",export_host_list);
	return -1;
    }   
     
    
    for (retry=10; retry > 0; retry--) {
    
        for (export_idx=0; export_idx<ROZOFS_HOST_LIST_MAX_HOST; export_idx++) {

            // Free resources from previous loop
            if (ret) xdr_free((xdrproc_t) xdr_ep_cluster_ret_t, (char *) ret);
	    rpcclt_release(clt);

	    pHost = rozofs_host_list_get_host(export_idx);
	    if (pHost == NULL) break;


	    // Initialize connection with exportd server
	    if (rpcclt_initialize
        	    (clt, pHost, EXPORT_PROGRAM, EXPORT_VERSION,
        	    ROZOFS_RPC_BUFFER_SIZE, ROZOFS_RPC_BUFFER_SIZE,
		    rozofs_get_service_port_export_master_eproto(), timeo) != 0)
        	continue;

	    // Send request
	    arg.hdr.gateway_rank = site;
	    arg.cid              = cid;

	    ret = ep_list_cluster_1(&arg, clt->client);
	    if (ret == 0) {
        	errno = EPROTO;
        	continue;
	    }

	    if (ret->status_gw.status == EP_FAILURE) {
        	errno = ret->status_gw.ep_cluster_ret_t_u.error;
        	continue;
	    }

	    // For each storage member
	    for (i = 0; i < ret->status_gw.ep_cluster_ret_t_u.cluster.storages_nb; i++) {
	    
	        if (ret->status_gw.ep_cluster_ret_t_u.cluster.storages[i].sid != sid) continue;

        	// Init storage
        	strncpy(stor->host, ret->status_gw.ep_cluster_ret_t_u.cluster.storages[i].host,
                	ROZOFS_HOSTNAME_MAX);
        	stor->sid = ret->status_gw.ep_cluster_ret_t_u.cluster.storages[i].sid;
        	stor->mclient.rpcclt.sock = -1;
		
                // Get connections for this storage
                if (rbs_stor_cnt_initialize(stor,cid) != 0) {
                  severe("rbs_stor_cnt_initialize cid/sid %d/%d failed: %s",
                            cid, stor->sid, strerror(errno));
		  goto out;	    
                }                
		status = 0;
		goto out;
	    }
	}
	
	if (timeo.tv_usec == 100000) {
	  timeo.tv_usec = 500000; 
	}
	else {
	  timeo.tv_usec = 0;
	  timeo.tv_sec++;	
	}  
    }
    	
out:
    // Free resources from current loop
    if (ret) xdr_free((xdrproc_t) xdr_ep_cluster_ret_t, (char *) ret);
    rpcclt_release(clt);
    return status;
}
/* 
**__________________________________________________________________________
** Send a request to export server to get the attributes of a FID
** The request must be sent tothe slave export that manages this FID.
** 
** @param export_host_list: List of hostnames of the export servers
** @param fid             : FID whose attributes are requested 
** @param attr            : returned attributes
** @param bsize           : returned block size 
** @param layout          : returned layout  
**
** @retval: 0 on success -1 otherwise (errno is set)
**__________________________________________________________________________
*/
int rbs_get_fid_attr(const char *export_host_list, fid_t fid, ep_mattr_t * attr, uint32_t * bsize, uint8_t * layout) {
  int                 status = -1;
  epgw_mattr_ret_t  * ret = NULL;
  epgw_mfile_arg_t    arg;
  rozofs_inode_t    * inode = (rozofs_inode_t *) fid;
  struct timeval      timeo;
  int                 slave_idx;
  int                 export_idx;
  int                 retry;
  char              * pHost;  
  rpcclt_t            clt;
  
  memset(&clt,0,sizeof(rpcclt_t));
  clt.sock = -1;
  
  //info("rbs_get_fid_attr %s",export_host_list);
  
  /*
  ** Compute index of the slave export responsible for this FID
  */
  slave_idx = ((inode->s.eid - 1) % EXPORT_SLICE_PROCESS_NB) + 1;
  

  /*
  ** Parse host list
  */
  if (rozofs_host_list_parse(export_host_list,'/') == 0) {
    severe("rozofs_host_list_parse(%s)",export_host_list);
    return -1;
  }   

  /*
  ** Try 10 times
  */
  for (retry=10; retry > 0; retry--) {

    /*
    ** Loop on export names
    */
    for (export_idx=0; export_idx<ROZOFS_HOST_LIST_MAX_HOST; export_idx++) {

      /*
      ** Free resources from previous loop
      */
      if (ret) xdr_free((xdrproc_t) xdr_epgw_mattr_ret_t, (char *) ret);
      rpcclt_release(&clt);

      /*
      ** Connect time out is 100ms + 100ms per retry
      */ 
      timeo.tv_sec  = 0;
      timeo.tv_usec = 100000 + ((10-retry)*100000);

      /*
      ** Next export name if any
      */
      pHost = rozofs_host_list_get_host(export_idx);
      if (pHost == NULL) break;

      /*
      ** Initialize connection with exportd server
      */
      if (rpcclt_initialize(&clt, pHost, EXPORT_PROGRAM, EXPORT_VERSION,
                            ROZOFS_RPC_BUFFER_SIZE, ROZOFS_RPC_BUFFER_SIZE, 
	                    rozofs_get_service_port_export_slave_eproto(slave_idx), 
                            timeo) != 0) {
        continue;
      }    
      //info("rbs_get_fid_attr winner is %s slave %d",pHost,slave_idx);

      /*
      ** Prepare the request
      */
      arg.arg_gw.eid = inode->s.eid;
      memcpy(&arg.arg_gw.fid,fid, sizeof(fid_t));
      /*
      ** Make it a mover source FID
      */
      inode = (rozofs_inode_t*)arg.arg_gw.fid;
      inode->s.key = ROZOFS_REG_S_MOVER;

      /*
      ** Set request timeout
      */ 
      timeo.tv_sec  = 0;
      timeo.tv_usec = 100000 + ((10-retry)*20000);

      /*
      ** Send the request to the exportd
      */
      ret = ep_getattr_1(&arg, clt.client);
      if (ret == 0) {
        errno = EPROTO;
        goto out;
      }

      if (ret->status_gw.status == EP_FAILURE) {
        errno = ret->status_gw.ep_mattr_ret_t_u.error;
        goto out;
      }    
    
      *bsize  = ret->bsize;
      *layout = ret->layout;
      memcpy(attr, &ret->status_gw.ep_mattr_ret_t_u.attrs, sizeof(ep_mattr_t));

      /*
      ** Check the case of the mover (rozo_rebalancing)
      */
      if (rozofs_is_storage_fid_valid((mattr_t*)attr,fid) == 0) {
        errno = ENOENT;
        goto out;
      }
      
      status = 0;
      goto out;
    }
  }
      
out:
  /*
  ** Release connection
  */
  rpcclt_release(&clt);
  /*
  ** Free the response
  */
  if (ret) xdr_free((xdrproc_t) xdr_epgw_mattr_ret_t, (char *) ret);
  //info("rbs_get_fid_attr status %d",status);
  return status;
}
