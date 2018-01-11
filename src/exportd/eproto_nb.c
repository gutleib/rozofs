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
#include <unistd.h>
#include <limits.h>
#include <dirent.h>
#include <errno.h>
#include <sys/types.h>
#include <pthread.h>

#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/rpc/export_profiler.h>
#include <rozofs/common/profile.h>
#include <rozofs/rpc/epproto.h>
#include <rozofs/rpc/eproto.h>
#include "eproto_nb.h"
#include <rozofs/rpc/sproto.h>
#include <rozofs/core/af_unix_socket_generic.h>

#include "export.h"
#include "volume.h"
#include "exportd.h"
#include "rozofs_ip4_flt.h"

DECLARE_PROFILING(epp_profiler_t);



#define EXPORTS_SEND_REPLY(req_ctx_p) \
     req_ctx_p->xmitBuf  = req_ctx_p->recv_buf; \
    req_ctx_p->recv_buf = NULL; \
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); \
    rozorpc_srv_release_context(req_ctx_p);

#define EXPORTS_SEND_REPLY_WITH_RET(req_ctx_p,ret) \
     req_ctx_p->xmitBuf  = req_ctx_p->recv_buf; \
    req_ctx_p->recv_buf = NULL; \
    rozorpc_srv_forward_reply(req_ctx_p,(char*)ret); \
    rozorpc_srv_release_context(req_ctx_p);
 
#define  EXPORTS_SEND_REPLY_WITH_LEN(req_ctx_p,len) \
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf; \
    req_ctx_p->recv_buf = NULL; \
    rozorpc_srv_forward_reply_with_extra_len(req_ctx_p,(char*)&ret,len); \
    rozorpc_srv_release_context(req_ctx_p);
    
ep_cnf_storage_node_t exportd_storage_host_table[STORAGE_NODES_MAX]; /**< configuration for each storage */
epgw_conf_ret_t export_storage_conf; /**< preallocated area to build storage configuration message */

/*
 *_______________________________________________________________________
 */
/**
*  Get export id free block count in case a quota has been set
  
  @param none
  
  @retval 0 on success
  @retval -1 on error
 */
static inline uint64_t exportd_get_free_quota(export_t *exp) {
   
  uint64_t quota;
  
  if (exp->hquota == 0) return -1;

  export_fstat_t * estats = export_fstat_get_stat(exp->eid);    
  if (estats == NULL) return -1;
  
  quota = exp->hquota - estats->blocks;
  return quota; 
}
/*
**______________________________________________________________________________
*/
void ep_null_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static void *ret = NULL;
     EXPORTS_SEND_REPLY(req_ctx_p);
    return;
}
/*
**______________________________________________________________________________
*/
/**
*  pseudo periodic task for geo-replication polling
*  the goal is to check the geo-replication entries that
*  must be pushed on disk
*/
extern void geo_replication_poll();

void ep_geo_poll_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static void *ret = NULL;
    export_profiler_eid = 0;
    START_PROFILING(ep_geo_poll);
    geo_replication_poll();
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_geo_poll);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd configuration polling : check if the configuration of
    the remote is inline with the current one of the exportd.
*/

void ep_poll_conf_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p)
{
    static epgw_status_ret_t ret;
    ep_gateway_t *args = (ep_gateway_t *)pt; 
    START_PROFILING_0(ep_poll);

    if (args->hash_config == export_configuration_file_hash) 
    {   
      ret.status_gw.status = EP_SUCCESS;
    }
    else
    {
      ret.status_gw.status = EP_NOT_SYNCED;
    }
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING_0(ep_poll);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd: Get the configuration of the storaged for a given eid

  : returns to the rozofsmount the list of the
*   volume,clusters and storages
*/
void ep_conf_storage_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_conf_ret_t ret;
    epgw_conf_stor_arg_t * arg = (epgw_conf_stor_arg_t *)pt; 
    
    epgw_conf_ret_t *ret_cnf_p = &export_storage_conf;
    epgw_conf_ret_t *ret_out = NULL;
    list_t *p, *q, *r;
    eid_t *eid = NULL;
    export_t *exp;
    int i = 0;
    int stor_idx = 0;
    int exist = 0;
    ep_cnf_storage_node_t *storage_cnf_p;
    

    DEBUG_FUNCTION;

    // XXX exportd_lookup_id could return export_t *
    eid = exports_lookup_id(arg->path);	

    // XXX exportd_lookup_id could return export_t *
    if (eid) export_profiler_eid = *eid;
    else     export_profiler_eid = 0;
	
    START_PROFILING(ep_configuration);
    if (!eid) goto error;
        
    /*
    ** extract the site id from the request (gateway rank)
    */
    int requested_site = arg->hdr.gateway_rank;
    	
    exportd_reinit_storage_configuration_message();
#if 0
#warning fake xdrmem_create
    XDR               xdrs; 
    int total_len = -1 ;
    int size = 1024*64;
    char *pchar = malloc(size);
    xdrmem_create(&xdrs,(char*)pchar,size,XDR_ENCODE);    
#endif
    // XXX exportd_lookup_id could return export_t *
    if (!(eid = exports_lookup_id(arg->path)))
        goto error;
    if (!(exp = exports_lookup_export(*eid)))
        goto error;

    /* Get lock on config */
    if ((errno = pthread_rwlock_rdlock(&config_lock)) != 0) {
        goto error;
    }

    /* For each volume */
    list_for_each_forward(p, &exportd_config.volumes) {

        volume_config_t *vc = list_entry(p, volume_config_t, list);

        /* Get volume with this vid */
        if (vc->vid == exp->volume->vid) {
            /*
	    	** check if the geo-replication is supported fro the volume. If it is not
	    	** the case the requested_site is set to 0
	    	*/
	    	if (vc->georep == 0) requested_site = 0;
    		if (requested_site  >= ROZOFS_GEOREP_MAX_SITE)
    		{
    		  severe("site number is out of range (%d) max %d",requested_site,ROZOFS_GEOREP_MAX_SITE-1);
    		  errno = EINVAL;
    		  goto error;
    		}		
		
            stor_idx = 0;
            ret_cnf_p->status_gw.ep_conf_ret_t_u.export.storage_nodes.storage_nodes_len = 0;
            storage_cnf_p = &exportd_storage_host_table[stor_idx];
//            memset(ret_cnf_p->status_gw.ep_conf_ret_t_u.export.storage_nodes, 0, sizeof (ep_cnf_storage_node_t) * STORAGE_NODES_MAX);

            /* For each cluster */
            list_for_each_forward(q, &vc->clusters) {

                cluster_config_t *cc = list_entry(q, cluster_config_t, list);

                /* For each sid */
                list_for_each_forward(r, (&cc->storages[requested_site])) {

                    storage_node_config_t *s = list_entry(r, storage_node_config_t, list);

                    /* Verify that this hostname does not already exist
                     * in the list of physical storage nodes. */
                    ep_cnf_storage_node_t *storage_cmp_p = &exportd_storage_host_table[0];
                    for (i = 0; i < stor_idx; i++,storage_cmp_p++) {

                        if (strcmp(s->host, storage_cmp_p->host) == 0) {

                            /* This physical storage node exist
                             *  but we add this SID*/
                            uint8_t sids_nb = storage_cmp_p->sids_nb;
                            storage_cmp_p->sids[sids_nb] = s->sid;
                            storage_cmp_p->cids[sids_nb] = cc->cid;
                            storage_cmp_p->sids_nb++;
                            exist = 1;
                            break;
                        }
                    }

                    /* This physical storage node doesn't exist*/
                    if (exist == 0) {

                        /* Add this storage node to the list */
                        strncpy(storage_cnf_p->host, s->host, ROZOFS_HOSTNAME_MAX);
                        /* Add this sid */
                        storage_cnf_p->sids[0] = s->sid;
                        storage_cnf_p->cids[0] = cc->cid;
                        storage_cnf_p->sids_nb++;

                        /* Increments the nb. of physical storage nodes */
                        stor_idx++;
                        storage_cnf_p++;
                    }
                    exist = 0;
                }
            }
        }
    }
    ret_cnf_p->status_gw.ep_conf_ret_t_u.export.storage_nodes.storage_nodes_len = stor_idx;
    ret_cnf_p->status_gw.ep_conf_ret_t_u.export.eid = *eid;
    ret_cnf_p->status_gw.ep_conf_ret_t_u.export.hash_conf = export_configuration_file_hash;
    memcpy(ret_cnf_p->status_gw.ep_conf_ret_t_u.export.md5, exp->md5, ROZOFS_MD5_SIZE);
    ret_cnf_p->status_gw.ep_conf_ret_t_u.export.rl = exp->layout;
    memcpy(ret_cnf_p->status_gw.ep_conf_ret_t_u.export.rfid, exp->rfid, sizeof (fid_t));

    if ((errno = pthread_rwlock_unlock(&config_lock)) != 0) {
        goto error;
    }

    ret_cnf_p->status_gw.status = EP_SUCCESS;
    ret_out = ret_cnf_p;
    goto out;
error:
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_conf_ret_t_u.error = errno;
    ret_out = &ret;

out:
    EXPORTS_SEND_REPLY_WITH_RET(req_ctx_p,ret_out);
    STOP_PROFILING(ep_mount);
    return ;
}

static uint32_t local_expgw_eid_table[EXPGW_EID_MAX_IDX];
static ep_gw_host_conf_t local_expgw_host_table[EXPGW_EXPGW_MAX_IDX];

/*
**______________________________________________________________________________
*/
/**
 *  Message to provide a ROZOFS with the export gateway configuration: EP_CONF_EXPGW
*/
void ep_conf_expgw_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static ep_gw_gateway_configuration_ret_t ret;
    ep_path_t * arg = (ep_path_t *) pt; 
    static char exportd_hostname[ROZOFS_HOSTNAME_MAX];
    eid_t *eid = NULL;
    export_t *exp;

    //int err = 0;
    list_t *iterator;
    list_t *iterator_expgw;    
    ep_gateway_configuration_t *expgw_conf_p= &ret.status_gw.ep_gateway_configuration_ret_t_u.config;

    // XXX exportd_lookup_id could return export_t *
    eid = exports_lookup_id(*arg);	

    // XXX exportd_lookup_id could return export_t *
    if (eid) export_profiler_eid = *eid;
    else     export_profiler_eid = 0;
	
    START_PROFILING(ep_conf_gateway);

	ret.hdr.eid          = 0;      /* NS */
	ret.hdr.nb_gateways  = 0; /* NS */
	ret.hdr.gateway_rank = 0; /* NS */
	ret.hdr.hash_config  = 0; /* NS */
    

    // XXX exportd_lookup_id could return export_t *
    if (!(eid = exports_lookup_id(*arg)))
        goto error;
    if (!(exp = exports_lookup_export(*eid)))
        goto error;

    /* Get lock on config */
    if ((errno = pthread_rwlock_rdlock(&config_lock)) != 0) {
        goto error;
    }            
    expgw_conf_p->eid.eid_len = 0;
    expgw_conf_p->eid.eid_val = local_expgw_eid_table;
    expgw_conf_p->exportd_host = exportd_hostname;
    strcpy(expgw_conf_p->exportd_host,exportd_config.exportd_vip);
    expgw_conf_p->exportd_port = 0;
    expgw_conf_p->gateway_port = 0;
    expgw_conf_p->gateway_host.gateway_host_len = 0;  
    expgw_conf_p->gateway_host.gateway_host_val = local_expgw_host_table;  
  
    list_for_each_forward(iterator, &exportd_config.exports) 
    {
       export_config_t *entry = list_entry(iterator, export_config_t, list);
       local_expgw_eid_table[expgw_conf_p->eid.eid_len] = entry->eid;
       expgw_conf_p->eid.eid_len++;
    }
    /*
    ** unlock exportd config
    */
    if ((errno = pthread_rwlock_unlock(&config_lock)) != 0) 
    {
        severe("can unlock config_lock, potential dead lock.");
        goto error;
    }
    if (expgw_conf_p->eid.eid_len == 0)
    {
      severe("no eid in the exportd configuration !!");
      //err = EPROTO;
      goto error;
    }

    expgw_conf_p->hdr.export_id            = 0;
    expgw_conf_p->hdr.nb_gateways          = 0;
    expgw_conf_p->hdr.gateway_rank         = 0;
    expgw_conf_p->hdr.configuration_indice = export_configuration_file_hash;
    /*
    ** Lock Config.
    */
    if ((errno = pthread_rwlock_rdlock(&config_lock)) != 0) 
    {
        goto error;
    }       
    list_for_each_forward(iterator, &exportd_config.expgw) 
    {
        expgw_config_t *entry = list_entry(iterator, expgw_config_t, list);
        expgw_conf_p->hdr.export_id = entry->daemon_id;
        /*
        ** loop on the storage
        */
        
        list_for_each_forward(iterator_expgw, &entry->expgw_node) 
        {
          expgw_node_config_t *entry = list_entry(iterator_expgw, expgw_node_config_t, list);
          /*
          ** copy the hostname
          */
 
          strcpy((char*)local_expgw_host_table[expgw_conf_p->gateway_host.gateway_host_len].host, entry->host);
           expgw_conf_p->gateway_host.gateway_host_len++;
          expgw_conf_p->hdr.nb_gateways++;
        }
    }
    /*
    ** Unlock Config.
    */
    if ((errno = pthread_rwlock_unlock(&config_lock)) != 0) 
    {
        severe("can't unlock expgws, potential dead lock.");
        goto error;
    } 
    ret.status_gw.status = EP_SUCCESS;

    goto out;
error:
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_gateway_configuration_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_conf_gateway);
    return ;
}


/*
**______________________________________________________________________________
*/
/**
*   exportd mount file systems: returns to the rozofsmount the list of the
*   volume,clusters and storages
*/
void ep_mount_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_mount_ret_t ret;
    epgw_mount_arg_t * arg= (epgw_mount_arg_t *)pt; 
    list_t *p, *q, *r;
    eid_t *eid = NULL;
    export_t *exp;
    int i = 0;
    int stor_idx = 0;
    int exist = 0;
    int requested_site =0;        

	memset(&ret,0,sizeof(ret));

    DEBUG_FUNCTION;

    // XXX exportd_lookup_id could return export_t *
    eid = exports_lookup_id(arg->path);    
    if (eid) export_profiler_eid = *eid;	
    else     export_profiler_eid = 0; 
        
    START_PROFILING(ep_mount);
    if (!eid) goto error;
      
    requested_site = arg->hdr.gateway_rank;
    
    if (!(exp = exports_lookup_export(*eid)))
        goto error;

    /*
    ** Thin provisonning requires a MSIT mount
    */
    if (exp->thin) {
      errno = ENOTSUP;
      goto error;
    }
    
    /*
    ** Check whether this client is allowed
    */
    uint32_t ip = af_unix_get_remote_ip(req_ctx_p->socketRef);
    if (rozofs_ip4_filter_check(exp->filter_tree, ip)) {
      info("%u.%u.%u.%u mounting export %d on path %s : allowed",IP2FOURU8(ip), *eid, arg->path);
    }
    else {
      warning("%u.%u.%u.%u mounting export %d on path %s : forbiden !!!",IP2FOURU8(ip), *eid, arg->path);
      errno = EPERM;
      goto error;
    }
    
    
    /* Get lock on config */
    if ((errno = pthread_rwlock_rdlock(&config_lock)) != 0) {
        goto error;
    }

    /* For each volume */
    list_for_each_forward(p, &exportd_config.volumes) {

        volume_config_t *vc = list_entry(p, volume_config_t, list);

        /* Get volume with this vid */
        if (vc->vid == exp->volume->vid) {
		
		    /*
			** If volume is declared as multi site, it can only be mounted through a
			** mount_msite procedure to give back the storages site localization
			*/
			if (vc->multi_site) {
			    pthread_rwlock_unlock(&config_lock);
			    errno = ENOTSUP;
				goto error;
			}
					
            /*
	    	** check if the geo-replication is supported fro the volume. If it is not
	    	** the case the requested_site is set to 0
	    	*/
	    	if (vc->georep == 0) requested_site = 0;
    		if (requested_site  >= ROZOFS_GEOREP_MAX_SITE)
    		{
    		  severe("site number is out of range (%d) max %d",requested_site,ROZOFS_GEOREP_MAX_SITE-1);
    		  errno = EINVAL;
    		  goto error;
    		}
						
            stor_idx = 0;
            ret.status_gw.ep_mount_ret_t_u.export.storage_nodes_nb = 0;
            memset(ret.status_gw.ep_mount_ret_t_u.export.storage_nodes, 0, sizeof (ep_cnf_storage_node_t) * STORAGE_NODES_MAX);

            /* For each cluster */
            list_for_each_forward(q, &vc->clusters) {

                cluster_config_t *cc = list_entry(q, cluster_config_t, list);

                /* For each sid */
                list_for_each_forward(r, (&cc->storages[requested_site])) {

                    storage_node_config_t *s = list_entry(r, storage_node_config_t, list);

                    /* Verify that this hostname does not already exist
                     * in the list of physical storage nodes. */
                    for (i = 0; i < stor_idx; i++) {

                        if (strcmp(s->host, ret.status_gw.ep_mount_ret_t_u.export.storage_nodes[i].host) == 0) {

                            /* This physical storage node exist
                             *  but we add this SID*/
                            uint8_t sids_nb = ret.status_gw.ep_mount_ret_t_u.export.storage_nodes[i].sids_nb;
                            ret.status_gw.ep_mount_ret_t_u.export.storage_nodes[i].sids[sids_nb] = s->sid;
                            ret.status_gw.ep_mount_ret_t_u.export.storage_nodes[i].cids[sids_nb] = cc->cid;
                            ret.status_gw.ep_mount_ret_t_u.export.storage_nodes[i].sids_nb++;
                            exist = 1;
                            break;
                        }
                    }

                    /* This physical storage node doesn't exist*/
                    if (exist == 0) {

                        /* Add this storage node to the list */
                        strncpy(ret.status_gw.ep_mount_ret_t_u.export.storage_nodes[stor_idx].host, s->host, ROZOFS_HOSTNAME_MAX);
                        /* Add this sid */
                        ret.status_gw.ep_mount_ret_t_u.export.storage_nodes[stor_idx].sids[0] = s->sid;
                        ret.status_gw.ep_mount_ret_t_u.export.storage_nodes[stor_idx].cids[0] = cc->cid;
                        ret.status_gw.ep_mount_ret_t_u.export.storage_nodes[stor_idx].sids_nb++;

                        /* Increments the nb. of physical storage nodes */
                        stor_idx++;
                    }
                    exist = 0;
                }
            }
        }
    }

    ret.status_gw.ep_mount_ret_t_u.export.storage_nodes_nb = stor_idx;
    ret.status_gw.ep_mount_ret_t_u.export.eid = *eid;
    ret.status_gw.ep_mount_ret_t_u.export.hash_conf = export_configuration_file_hash;
    ret.status_gw.ep_mount_ret_t_u.export.bs = exp->bsize;

    int eid_slice = (ret.status_gw.ep_mount_ret_t_u.export.eid-1)%EXPORT_SLICE_PROCESS_NB + 1;
    uint32_t port = rozofs_get_service_port_export_slave_eproto(eid_slice);
    ret.status_gw.ep_mount_ret_t_u.export.listen_port = port;
    
    memcpy(ret.status_gw.ep_mount_ret_t_u.export.md5, exp->md5, ROZOFS_MD5_SIZE);
    ret.status_gw.ep_mount_ret_t_u.export.rl = exp->layout;
    memcpy(ret.status_gw.ep_mount_ret_t_u.export.rfid, exp->rfid, sizeof (fid_t));

    if ((errno = pthread_rwlock_unlock(&config_lock)) != 0) {
        goto error;
    }

    ret.status_gw.status = EP_SUCCESS;

    goto out;
error:
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_mount_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_mount);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd mount file systems: returns to the rozofsmount the list of the
*   volume,clusters and storages
*/
void ep_mount_msite_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_mount_msite_ret_t ret;
    epgw_mount_arg_t * arg= (epgw_mount_arg_t *)pt; 
    list_t *p, *q, *r;
    eid_t *eid = NULL;
    export_t *exp;
    int i = 0;
    int stor_idx = 0;
    int exist = 0;
    int requested_site =0;        

    DEBUG_FUNCTION;

	memset(&ret,0,sizeof(ret));
	
    // XXX exportd_lookup_id could return export_t *
    eid = exports_lookup_id(arg->path);    
    if (eid) export_profiler_eid = *eid;	
    else     export_profiler_eid = 0; 
        
    START_PROFILING(ep_mount);
    if (!eid) goto error;

    requested_site = arg->hdr.gateway_rank;
    
    if (!(exp = exports_lookup_export(*eid)))
        goto error;
    
    ret.status_gw.ep_mount_msite_ret_t_u.export.msite = 0;
    
    /*
    ** Tell whether thin provisionning is configured
    */
    if (exp->thin) {
      ret.status_gw.ep_mount_msite_ret_t_u.export.msite |= ROZOFS_EXPORT_THIN_PROVISIONNING_BIT;
    }
    
         
    /*
    ** Check whether this client is allowed
    */
    uint32_t ip = af_unix_get_remote_ip(req_ctx_p->socketRef);
    if (rozofs_ip4_filter_check(exp->filter_tree, ip)) {
      info("%u.%u.%u.%u mounting export %d on path %s : allowed",IP2FOURU8(ip), *eid, arg->path);
    }
    else {
      warning("%u.%u.%u.%u mounting export %d on path %s : forbiden !!!",IP2FOURU8(ip), *eid, arg->path);
      errno = EPERM;
      goto error;
    }
    
    /* Get lock on config */
    if ((errno = pthread_rwlock_rdlock(&config_lock)) != 0) {
        goto error;
    }
    /*
    ** init of the working variables: it has been moved here because one export might be associated with 2 volumes
    */
    stor_idx = 0;
    ret.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes_nb = 0;
    memset(ret.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes, 0, sizeof (ret.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes));
    /* For each volume */
    list_for_each_forward(p, &exportd_config.volumes) {

        volume_config_t *vc = list_entry(p, volume_config_t, list);

        /* Get volume with this vid */
        if ((vc->vid == exp->volume->vid) || ((exp->volume_fast != NULL) && (vc->vid == exp->volume_fast->vid))) {
		
		    /*
			** Volume is declared as multi site
			*/
			if (vc->multi_site) {
			   ret.status_gw.ep_mount_msite_ret_t_u.export.msite |= ROZOFS_EXPORT_MSITE_BIT; 
			}	
							
            /*
	    	** check if the geo-replication is supported fro the volume. If it is not
	    	** the case the requested_site is set to 0
	    	*/
	    	if (vc->georep == 0) requested_site = 0;
    		if (requested_site  >= ROZOFS_GEOREP_MAX_SITE)
    		{
    		  severe("site number is out of range (%d) max %d",requested_site,ROZOFS_GEOREP_MAX_SITE-1);
    		  errno = EINVAL;
    		  goto error;
    		}			
#if 0			
            stor_idx = 0;
            ret.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes_nb = 0;
            memset(ret.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes, 0, sizeof (ret.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes));
#endif
            /* For each cluster */
            list_for_each_forward(q, &vc->clusters) {

                cluster_config_t *cc = list_entry(q, cluster_config_t, list);

                /* For each sid */
                list_for_each_forward(r, (&cc->storages[requested_site])) {

                    storage_node_config_t *s = list_entry(r, storage_node_config_t, list);

                    /* Verify that this hostname does not already exist
                     * in the list of physical storage nodes. */
                    for (i = 0; i < stor_idx; i++) {

                        if (strcmp(s->host, ret.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes[i].host) == 0) {

                            /* This physical storage node exist
                             *  but we add this SID*/
                            uint8_t sids_nb = ret.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes[i].sids_nb;
                            ret.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes[i].sids[sids_nb] = s->sid;
                            ret.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes[i].cids[sids_nb] = cc->cid;
                            ret.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes[i].sids_nb++;
                            exist = 1;
                            break;
                        }
                    }

                    /* This physical storage node doesn't exist*/
                    if (exist == 0) {

                        /* Add this storage node to the list */
                        strncpy(ret.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes[stor_idx].host, s->host, ROZOFS_HOSTNAME_MAX);
                        /* Add this sid */
                        ret.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes[stor_idx].site    = s->siteNum;						
                        ret.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes[stor_idx].sids[0] = s->sid;
                        ret.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes[stor_idx].cids[0] = cc->cid;
                        ret.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes[stor_idx].sids_nb++;

                        /* Increments the nb. of physical storage nodes */
                        stor_idx++;
                    }
                    exist = 0;
                }
            }
        }
    }

    ret.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes_nb = stor_idx;
    ret.status_gw.ep_mount_msite_ret_t_u.export.eid = *eid;
    ret.status_gw.ep_mount_msite_ret_t_u.export.hash_conf = export_configuration_file_hash;
    ret.status_gw.ep_mount_msite_ret_t_u.export.bs = exp->bsize;

    int eid_slice = (ret.status_gw.ep_mount_msite_ret_t_u.export.eid-1)%EXPORT_SLICE_PROCESS_NB + 1;
    uint32_t port = rozofs_get_service_port_export_slave_eproto(eid_slice);
    ret.status_gw.ep_mount_msite_ret_t_u.export.listen_port = port;
    
    memcpy(ret.status_gw.ep_mount_msite_ret_t_u.export.md5, exp->md5, ROZOFS_MD5_SIZE);
    ret.status_gw.ep_mount_msite_ret_t_u.export.rl = exp->layout;
    memcpy(ret.status_gw.ep_mount_msite_ret_t_u.export.rfid, exp->rfid, sizeof (fid_t));

    if ((errno = pthread_rwlock_unlock(&config_lock)) != 0) {
        goto error;
    }

    ret.status_gw.status = EP_SUCCESS;
    
    goto out;
error:
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_mount_msite_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_mount);
    return ;
}

/*
**______________________________________________________________________________
*/
/**
*   exportd list of clusters of exportd
*/
void ep_list_cluster_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_cluster_ret_t ret;
    epgw_cluster_arg_t * arg = (epgw_cluster_arg_t*) pt;
    list_t *p, *q, *r;
    uint8_t stor_idx = 0;

    int requested_site = arg->hdr.gateway_rank;
    
    DEBUG_FUNCTION;

    ret.status_gw.status = EP_FAILURE;

    // Get lock on config
    if ((errno = pthread_rwlock_rdlock(&config_lock)) != 0) {
        ret.status_gw.ep_cluster_ret_t_u.error = errno;
        goto out;
    }

    // For each volume

    list_for_each_forward(p, &exportd_config.volumes) {

        volume_config_t *vc = list_entry(p, volume_config_t, list);

        ret.status_gw.ep_cluster_ret_t_u.cluster.storages_nb = 0;
        memset(ret.status_gw.ep_cluster_ret_t_u.cluster.storages, 0, sizeof (ep_storage_t) * SID_MAX);

        // For each cluster

        list_for_each_forward(q, &vc->clusters) {

            cluster_config_t *cc = list_entry(q, cluster_config_t, list);

            // Check if it's a the good cluster
            if (cc->cid == arg->cid) {

                // Copy cid
                ret.status_gw.ep_cluster_ret_t_u.cluster.cid = cc->cid;

                // For each storage 

                list_for_each_forward(r, (&cc->storages[requested_site])) {

                    storage_node_config_t *s = list_entry(r, storage_node_config_t, list);

                    // Add the storage to response
                    strncpy(ret.status_gw.ep_cluster_ret_t_u.cluster.storages[stor_idx].host, s->host, ROZOFS_HOSTNAME_MAX);
                    ret.status_gw.ep_cluster_ret_t_u.cluster.storages[stor_idx].sid = s->sid;
                    stor_idx++;
                }
                // OK -> answered
		
		if (stor_idx == 0) {
		  // This site has no such cluster
                  ret.status_gw.ep_cluster_ret_t_u.error = ENODEV;		  
		  goto unlock;
		}
                ret.status_gw.ep_cluster_ret_t_u.cluster.storages_nb = stor_idx;
                ret.status_gw.status = EP_SUCCESS;
                goto unlock;
            }
        }
    }   
    // cid not found
    ret.status_gw.ep_cluster_ret_t_u.error = EINVAL;

unlock:
    if ((errno = pthread_rwlock_unlock(&config_lock)) != 0) {
        ret.status_gw.ep_cluster_ret_t_u.error = errno;
        goto out;
    }
out:
    EXPORTS_SEND_REPLY(req_ctx_p);

    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd list of clusters of exportd
*/
void ep_list_cluster2_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_cluster2_ret_t ret;
    epgw_cluster_arg_t * arg = (epgw_cluster_arg_t*) pt;
    list_t *p, *q, *r;
    uint8_t stor_idx = 0;

    int requested_site = arg->hdr.gateway_rank;
    
    DEBUG_FUNCTION;

    ret.status_gw.status = EP_FAILURE;

    // Get lock on config
    if ((errno = pthread_rwlock_rdlock(&config_lock)) != 0) {
        ret.status_gw.ep_cluster2_ret_t_u.error = errno;
        goto out;
    }

    // For each volume

    list_for_each_forward(p, &exportd_config.volumes) {

        volume_config_t *vc = list_entry(p, volume_config_t, list);

        ret.status_gw.ep_cluster2_ret_t_u.cluster.vid    = vc->vid;
        ret.status_gw.ep_cluster2_ret_t_u.cluster.layout = vc->layout;
        ret.status_gw.ep_cluster2_ret_t_u.cluster.storages_nb = 0;
        memset(ret.status_gw.ep_cluster2_ret_t_u.cluster.storages, 0, sizeof (ep_storage_t) * SID_MAX);

        // For each cluster

        list_for_each_forward(q, &vc->clusters) {

            cluster_config_t *cc = list_entry(q, cluster_config_t, list);

            // Check if it's a the good cluster
            if (cc->cid == arg->cid) {

                // Copy cid
                ret.status_gw.ep_cluster2_ret_t_u.cluster.cid = cc->cid;

                // For each storage 

                list_for_each_forward(r, (&cc->storages[requested_site])) {

                    storage_node_config_t *s = list_entry(r, storage_node_config_t, list);

                    // Add the storage to response
                    strncpy(ret.status_gw.ep_cluster2_ret_t_u.cluster.storages[stor_idx].host, s->host, ROZOFS_HOSTNAME_MAX);
                    ret.status_gw.ep_cluster2_ret_t_u.cluster.storages[stor_idx].sid = s->sid;
                    stor_idx++;
                }
                // OK -> answered
		
		if (stor_idx == 0) {
		  // This site has no such cluster
                  ret.status_gw.ep_cluster2_ret_t_u.error = ENODEV;		  
		  goto unlock;
		}
                ret.status_gw.ep_cluster2_ret_t_u.cluster.storages_nb = stor_idx;
                ret.status_gw.status = EP_SUCCESS;
                goto unlock;
            }
        }
    }   
    // cid not found
    ret.status_gw.ep_cluster2_ret_t_u.error = EINVAL;

unlock:
    if ((errno = pthread_rwlock_unlock(&config_lock)) != 0) {
        ret.status_gw.ep_cluster2_ret_t_u.error = errno;
        goto out;
    }
out:
    EXPORTS_SEND_REPLY(req_ctx_p);

    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd unmount
*/
/* Will do something one day !! */
void ep_umount_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_status_ret_t ret;
//    uint32_t * arg = (uint32_t *)pt; 
    DEBUG_FUNCTION;
    START_PROFILING(ep_umount);

    ret.status_gw.status = EP_SUCCESS;

    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_umount);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd file system statfs
*/
void ep_statfs_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_statfs_ret_t ret;
    uint32_t * arg = (uint32_t*)pt; 
    export_t *exp;
    DEBUG_FUNCTION;
    // Set profiler export index
    export_profiler_eid = * arg;
    
    START_PROFILING(ep_statfs);

    if (!(exp = exports_lookup_export((eid_t) * arg)))
        goto error;
    if (export_stat(exp, & ret.status_gw.ep_statfs_ret_t_u.stat) != 0)
        goto error;
    ret.status_gw.status = EP_SUCCESS;
    goto out;
error:
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_statfs_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_statfs);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd lookup fid,name
    @param args : fid parent and name of the object
    
    @retval: EP_SUCCESS :attributes of the parent and child (fid,name)
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_lookup_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_mattr_ret_t ret;
    epgw_lookup_arg_t * arg = (epgw_lookup_arg_t*)pt; 
    export_t *exp;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;

    START_PROFILING(ep_lookup);

    ret.parent_attr.status = EP_EMPTY;

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;
    if (export_lookup
            (exp, (unsigned char *) arg->arg_gw.parent, arg->arg_gw.name,
            (mattr_t *) & ret.status_gw.ep_mattr_ret_t_u.attrs,
            (mattr_t *) & ret.parent_attr.ep_mattr_ret_t_u.attrs) != 0)
        goto error;
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status   = EP_SUCCESS;
    ret.parent_attr.status = EP_SUCCESS;
    ret.free_quota = exportd_get_free_quota(exp);
    goto out;
error:
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_mattr_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_lookup);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd get attributes

    @param args : fid of the object 
    
    @retval: EP_SUCCESS :attributes of the object
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_getattr_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_mattr_ret_t ret;
    epgw_mfile_arg_t * arg = (epgw_mfile_arg_t*)pt; 
    export_t *exp;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;

   START_PROFILING(ep_getattr);

    ret.parent_attr.status = EP_EMPTY;

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;
    if (export_getattr
            (exp, (unsigned char *) arg->arg_gw.fid,
            (mattr_t *) & ret.status_gw.ep_mattr_ret_t_u.attrs,
	    (mattr_t *) & ret.parent_attr.ep_mattr_ret_t_u.attrs) != 0)
        goto error;
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status = EP_SUCCESS;
    ret.parent_attr.status = EP_SUCCESS;
    ret.free_quota = exportd_get_free_quota(exp);
    ret.bsize = exp->bsize;
    ret.layout = exp->layout;
    goto out;
error:
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_mattr_ret_t_u.error = errno;
out:
   EXPORTS_SEND_REPLY(req_ctx_p);
   STOP_PROFILING(ep_getattr);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd set attributes

    @param args : fid of the object and attributes to set
    
    @retval: EP_SUCCESS :attributes of the object
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_setattr_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_mattr_ret_t ret;
    epgw_setattr_arg_t * arg = (epgw_setattr_arg_t*)pt; 
    export_t *exp;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;

    START_PROFILING(ep_setattr);

    ret.parent_attr.status = EP_EMPTY;

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;
    if (export_setattr(exp, (unsigned char *) arg->arg_gw.attrs.fid,
            (mattr_t *) & arg->arg_gw.attrs, arg->arg_gw.to_set) != 0)
        goto error;
    if (export_getattr(exp, (unsigned char *) arg->arg_gw.attrs.fid,
            (mattr_t *) & ret.status_gw.ep_mattr_ret_t_u.attrs,
	    (mattr_t *) & ret.parent_attr.ep_mattr_ret_t_u.attrs) != 0)
        goto error;
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status = EP_SUCCESS;
    ret.parent_attr.status = EP_SUCCESS;
    ret.free_quota = exportd_get_free_quota(exp);
    goto out;
error:
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_mattr_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_setattr);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd read link

    @param args : fid of the object 
    
    @retval: EP_SUCCESS :content of the link (max is ROZOFS_PATH_MAX)
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_readlink_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_readlink_ret_t ret;
    epgw_mfile_arg_t * arg = (epgw_mfile_arg_t*)pt;
    export_t *exp;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;

    START_PROFILING(ep_readlink);


    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;

    if (ret.status_gw.ep_readlink_ret_t_u.link == NULL)
    {
      ret.status_gw.ep_readlink_ret_t_u.link = xmalloc(ROZOFS_PATH_MAX);
    }

    if (export_readlink(exp, (unsigned char *) arg->arg_gw.fid,
            ret.status_gw.ep_readlink_ret_t_u.link) != 0)
        goto error;

    ret.status_gw.status = EP_SUCCESS;
    goto out;
error:
    if (ret.status_gw.ep_readlink_ret_t_u.link != NULL)
        free(ret.status_gw.ep_readlink_ret_t_u.link);
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_readlink_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_readlink);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd link

    @param args : fid of the parent and object name and the target inode (fid)  
    
    @retval: EP_SUCCESS :attributes of the parent and of the object
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/

void ep_link_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_mattr_ret_t ret;
    epgw_link_arg_t * arg =(epgw_link_arg_t*)pt; 
    export_t *exp;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;

    START_PROFILING(ep_link);
    
    ret.parent_attr.status = EP_EMPTY;

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;
    if (export_link(exp, (unsigned char *) arg->arg_gw.inode,
            (unsigned char *) arg->arg_gw.newparent, arg->arg_gw.newname,
            (mattr_t *) & ret.status_gw.ep_mattr_ret_t_u.attrs,
            (mattr_t *) & ret.parent_attr.ep_mattr_ret_t_u.attrs) != 0)
        goto error;
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status   = EP_SUCCESS;
    ret.parent_attr.status = EP_SUCCESS;
    ret.free_quota = exportd_get_free_quota(exp);
    goto out;
error:
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_mattr_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_link);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd mknod (create a new regular file)

    @param args : fid of the parent and object name 
    
    @retval: EP_SUCCESS :attributes of the parent and of the object
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_mknod_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_mattr_ret_t ret;
    epgw_mknod_arg_t * arg = (epgw_mknod_arg_t*)pt; 
    export_t *exp;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;

    START_PROFILING(ep_mknod);

    ret.parent_attr.status = EP_EMPTY;

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;
    if (export_mknod(
            exp,arg->hdr.gateway_rank, 
            (unsigned char *) arg->arg_gw.parent, arg->arg_gw.name, arg->arg_gw.uid, arg->arg_gw.gid,
            arg->arg_gw.mode, (mattr_t *) & ret.status_gw.ep_mattr_ret_t_u.attrs,
            (mattr_t *) & ret.parent_attr.ep_mattr_ret_t_u.attrs) != 0)
        goto error;
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.parent_attr.status = EP_SUCCESS;
    ret.status_gw.status = EP_SUCCESS;
    ret.free_quota = exportd_get_free_quota(exp);
    goto out;
error:
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_mattr_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_mknod);
    return ;
}

/*
**______________________________________________________________________________
*/
/**
*   exportd mkdir (create a new directory)

    @param args : fid of the parent and object name 
    
    @retval: EP_SUCCESS :attributes of the parent and of the object
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_mkdir_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_mattr_ret_t ret;
    epgw_mkdir_arg_t * arg = (epgw_mkdir_arg_t*)pt; 
    export_t *exp;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;

    START_PROFILING(ep_mkdir);

    ret.parent_attr.status = EP_EMPTY;

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;
    if (export_mkdir
            (exp, (unsigned char *) arg->arg_gw.parent, arg->arg_gw.name, arg->arg_gw.uid, arg->arg_gw.gid,
            arg->arg_gw.mode, (mattr_t *) & ret.status_gw.ep_mattr_ret_t_u.attrs,
            (mattr_t *) & ret.parent_attr.ep_mattr_ret_t_u.attrs) != 0)
        goto error;
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.parent_attr.status = EP_SUCCESS;
    ret.status_gw.status = EP_SUCCESS;
    ret.free_quota = exportd_get_free_quota(exp);
    goto out;
error:
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_mattr_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_mkdir);
    return ;
}

/*
**______________________________________________________________________________
*/
/**
*   exportd unlink: delete a regular,symlink or hardlink object

    @param args : fid of the parent and object name 
    
    @retval: EP_SUCCESS :attributes of the parent and fid of the deleted object (contains 0 if not deleted)
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_unlink_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_fid_ret_t ret;
    epgw_unlink_arg_t * arg =(epgw_unlink_arg_t*)pt; 
    export_t *exp;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;

    START_PROFILING(ep_unlink);

    ret.parent_attr.status = EP_EMPTY;

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;
    if (export_unlink(exp, (unsigned char *) arg->arg_gw.pfid, arg->arg_gw.name,
            (unsigned char *) ret.status_gw.ep_fid_ret_t_u.fid,
            (mattr_t *) &ret.parent_attr.ep_mattr_ret_t_u.attrs) != 0)
        goto error;

    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status = EP_SUCCESS;
    ret.parent_attr.status = EP_SUCCESS;
    goto out;
error:
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_fid_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_unlink);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd rmdir: delete a directory

    @param args : fid of the parent and directory  name 
    
    @retval: EP_SUCCESS :attributes of the parent and fid of the deleted directory 
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_rmdir_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_fid_ret_t ret;
    epgw_rmdir_arg_t * arg=(epgw_rmdir_arg_t*)pt; 
    export_t *exp;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;

    START_PROFILING(ep_rmdir);

    ret.parent_attr.status = EP_EMPTY;

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;
    if (export_rmdir(exp, (unsigned char *) arg->arg_gw.pfid, arg->arg_gw.name,
            (unsigned char *) ret.status_gw.ep_fid_ret_t_u.fid,
            (mattr_t *) &ret.parent_attr.ep_mattr_ret_t_u.attrs) != 0)
        goto error;

    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status = EP_SUCCESS;
    ret.parent_attr.status = EP_SUCCESS;
    goto out;
error:
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_fid_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_rmdir);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd symlink: create a symbolic link

    @param args : fid of the parent and symlink name  and target link (name)
    
    @retval: EP_SUCCESS :attributes of the parent and  of the created symlink
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_symlink_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_mattr_ret_t ret;
    epgw_symlink_arg_t * arg = (epgw_symlink_arg_t*)pt; 
    export_t *exp;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;

    START_PROFILING(ep_symlink);

    ret.parent_attr.status = EP_EMPTY;

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;

    if (export_symlink(exp, arg->arg_gw.link, (unsigned char *) arg->arg_gw.parent, arg->arg_gw.name,
            (mattr_t *) & ret.status_gw.ep_mattr_ret_t_u.attrs,
            (mattr_t *) & ret.parent_attr.ep_mattr_ret_t_u.attrs,
	    getuid(),getgid()) != 0)
        goto error;

    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.parent_attr.status = EP_SUCCESS;
    ret.status_gw.status = EP_SUCCESS;
    ret.free_quota = exportd_get_free_quota(exp);
    goto out;
error:
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_mattr_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_symlink);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd symlink: create a symbolic link

    @param args : fid of the parent and symlink name  and target link (name)
    
    @retval: EP_SUCCESS :attributes of the parent and  of the created symlink
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_symlink2_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_mattr_ret_t ret;
    epgw_symlink2_arg_t * arg = (epgw_symlink2_arg_t*)pt; 
    export_t *exp;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;

    START_PROFILING(ep_symlink);

    ret.parent_attr.status = EP_EMPTY;

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;

    if (export_symlink(exp, arg->arg_gw.link, (unsigned char *) arg->arg_gw.parent, arg->arg_gw.name,
            (mattr_t *) & ret.status_gw.ep_mattr_ret_t_u.attrs,
            (mattr_t *) & ret.parent_attr.ep_mattr_ret_t_u.attrs,
	    arg->arg_gw.uid, arg->arg_gw.gid) != 0)
        goto error;

    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.parent_attr.status = EP_SUCCESS;
    ret.status_gw.status = EP_SUCCESS;
    ret.free_quota = exportd_get_free_quota(exp);
    goto out;
error:
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_mattr_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_symlink);
    return ;
}

/*
**______________________________________________________________________________
*/
/**
*   exportd rename: rename a file or a directory

    @param args : old fid parent/name  and new fid parent/name
    
    @retval: EP_SUCCESS :to be completed
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/

void ep_rename_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_rename_ret_t ret;
    epgw_rename_arg_t * arg = (epgw_rename_arg_t*)pt; 
    export_t *exp;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;

    START_PROFILING(ep_rename);
    
    ret.parent_attr.status = EP_EMPTY;

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;
    if (export_rename(exp, (unsigned char *) arg->arg_gw.pfid, arg->arg_gw.name,
            (unsigned char *) arg->arg_gw.npfid, arg->arg_gw.newname,
            (unsigned char *) ret.status_gw.ep_fid_ret_t_u.fid,
	    (mattr_t *) &ret.child_attr.ep_mattr_ret_t_u.attrs) != 0)
        goto error;

    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status = EP_SUCCESS;
    goto out;
error:
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_fid_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_rename);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd readdir: list the content of a directory

    @param args : fid of the directory
    
    @retval: EP_SUCCESS :set of name and fid associated with the directory and cookie for next readdir
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_readdir_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_readdir_ret_t ret;
    epgw_readdir_arg_t * arg = (epgw_readdir_arg_t*)pt;
    export_t *exp;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;

    START_PROFILING(ep_readdir);

    xdr_free((xdrproc_t) xdr_epgw_readdir_ret_t, (char *) &ret);

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;

    if (export_readdir(exp, (unsigned char *) arg->arg_gw.fid, &arg->arg_gw.cookie,
            (child_t **) & ret.status_gw.ep_readdir_ret_t_u.reply.children,
            (uint8_t *) & ret.status_gw.ep_readdir_ret_t_u.reply.eof) != 0)
        goto error;

    ret.status_gw.ep_readdir_ret_t_u.reply.cookie = arg->arg_gw.cookie;

    ret.status_gw.status = EP_SUCCESS;
    goto out;
error:
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_readdir_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_readdir);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd readdir: list the content of a directory

    @param args : fid of the directory
    
    @retval: EP_SUCCESS :set of name and fid associated with the directory and cookie for next readdir
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_readdir2_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_readdir2_nodata_ret_t ret;
    epgw_readdir_arg_t * arg = (epgw_readdir_arg_t*)pt;
    export_t *exp;
    int len= 0;
    char *pbuf = NULL;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;

    START_PROFILING(ep_readdir);

    ret.status_gw.ep_readdir2_nodata_ret_t_u.reply.len = 0;
    ret.status_gw.ep_readdir2_nodata_ret_t_u.reply.eof = 0;

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;

   /*
   ** generate a fake RPC reply
   */
   {
      pbuf = ruc_buf_getPayload(req_ctx_p->recv_buf);
      int position;
      position =  sizeof(uint32_t); /* length header of the rpc message */
      position += rozofs_rpc_get_min_rpc_reply_hdr_len();
      position += (4*sizeof(uint32_t));   /* eid+nb_gateways+gateway_rank+hash_config field */
      position += (4*sizeof(uint32_t));   /* eof + cookie */
      position += sizeof(uint32_t);   /* length of the bins len field */
      pbuf +=position;      
   }
    len =export_readdir2(exp, (unsigned char *) arg->arg_gw.fid, &arg->arg_gw.cookie,
        	(char*) pbuf,
        	(uint8_t *) & ret.status_gw.ep_readdir2_nodata_ret_t_u.reply.eof);
    if (len < 0)
        goto error;

    ret.status_gw.ep_readdir2_nodata_ret_t_u.reply.cookie = arg->arg_gw.cookie;
    ret.status_gw.ep_readdir2_nodata_ret_t_u.reply.len = len;

    ret.status_gw.status = EP_SUCCESS;
    goto out;
error:
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_readdir2_nodata_ret_t_u.error = errno;
    len = 0;
out:
    EXPORTS_SEND_REPLY_WITH_LEN(req_ctx_p,len);
    STOP_PROFILING(ep_readdir);
    return ;
}


/*
**______________________________________________________________________________
*/
/**
*   exportd write_block: update the size and date of a file

    @param args : fid of the file, offset and length written
    
    @retval: EP_SUCCESS :attributes of the updated file
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_write_block_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_mattr_ret_t ret;
    epgw_write_block_arg_t * arg = (epgw_write_block_arg_t*)pt;
    export_t *exp;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;

    START_PROFILING_IO(ep_write_block, arg->arg_gw.length);

    ret.parent_attr.status = EP_EMPTY;

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;
    if (export_write_block(exp,(unsigned char *) arg->arg_gw.fid, 
                           arg->arg_gw.bid, arg->arg_gw.nrb, 
			   arg->arg_gw.dist,
                           arg->arg_gw.offset, 
			   arg->arg_gw.length,
			   arg->hdr.gateway_rank,
			   arg->arg_gw.geo_wr_start,
			   arg->arg_gw.geo_wr_end,
                           (mattr_t *) & ret.status_gw.ep_mattr_ret_t_u.attrs) < 0)
        goto error;
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status   = EP_SUCCESS;
    ret.free_quota = exportd_get_free_quota(exp);
    goto out;
error:
    ret.hdr.eid = arg->arg_gw.eid ;  
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_mattr_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_write_block);
    return ;
}

/*
**______________________________________________________________________________
*/
/**
*   exportd setxattr: set extended attribute

    @param args : fid of the object, extended attribute name and value
    
    @retval: EP_SUCCESS : no specific returned parameter
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_setxattr_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_setxattr_ret_t ret;
    epgw_setxattr_arg_t * arg = (epgw_setxattr_arg_t*)pt; 
    export_t *exp;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;


    START_PROFILING(ep_setxattr);
    
    if ((ret.symlink.status == EP_SUCCESS) 
    &&  (ret.symlink.epgw_setxattr_symlink_t_u.info.target.target_val != NULL)) {
      free(ret.symlink.epgw_setxattr_symlink_t_u.info.target.target_val);
    }  
    ret.symlink.status = EP_EMPTY;
    ret.symlink.epgw_setxattr_symlink_t_u.info.target.target_len = 0;
    ret.symlink.epgw_setxattr_symlink_t_u.info.target.target_val = NULL;    

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;

    if (export_setxattr(exp, (unsigned char *) arg->arg_gw.fid, arg->arg_gw.name,
            arg->arg_gw.value.value_val, arg->arg_gw.value.value_len, arg->arg_gw.flags,
	    &ret.symlink) != 0) {
        goto error;
    }

    ret.status_gw.status = EP_SUCCESS;
    goto out;
error:
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_status_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_setxattr);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd getxattr: get extended attributes

    @param args : fid of the object, extended attribute name 
    
    @retval: EP_SUCCESS : extended attribute value
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_getxattr_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_getxattr_ret_t ret;
    epgw_getxattr_arg_t * arg = (epgw_getxattr_arg_t*)pt; 
    export_t *exp;
    ssize_t size = -1;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;


    START_PROFILING(ep_getxattr);

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;

    if (ret.status_gw.ep_getxattr_ret_t_u.value.value_val == NULL)
    {
      ret.status_gw.ep_getxattr_ret_t_u.value.value_val = xmalloc(ROZOFS_XATTR_VALUE_MAX);
    }
    ret.status_gw.ep_getxattr_ret_t_u.value.value_len = 0;
    if ((size = export_getxattr(exp, (unsigned char *) arg->arg_gw.fid, arg->arg_gw.name,
            ret.status_gw.ep_getxattr_ret_t_u.value.value_val, arg->arg_gw.size)) == -1) {
        goto error;
    }

    ret.status_gw.ep_getxattr_ret_t_u.value.value_len = size;

    ret.status_gw.status = EP_SUCCESS;
    goto out;
error:
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_getxattr_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_getxattr);
    return ;
}

/*
**______________________________________________________________________________
*/
/**
*   exportd getxattr: get extended attributes in raw mode

    @param args : fid of the object, extended attribute name 
    
    @retval: EP_SUCCESS : extended attribute value
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/

void ep_getxattr_raw_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_getxattr_raw_ret_t ret;
    epgw_getxattr_arg_t * arg = (epgw_getxattr_arg_t*)pt; 
    export_t *exp;
    ssize_t size = -1;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;
    /*
    ** Check if the buffers have been allocated: check only the first one
    */
    
   if (ret.status_gw.ep_getxattr_raw_ret_t_u.raw.inode_xattr.inode_xattr_val == NULL)
   {
 
      ret.status_gw.ep_getxattr_raw_ret_t_u.raw.inode_xattr.inode_xattr_val = xmalloc(4096);
      ret.status_gw.ep_getxattr_raw_ret_t_u.raw.inode_xattr_block.inode_xattr_block_val = xmalloc(4096);
   }


    START_PROFILING(ep_getxattr);
    
    ret.status_gw.ep_getxattr_raw_ret_t_u.raw.inode_xattr.inode_xattr_len = 0;
    ret.status_gw.ep_getxattr_raw_ret_t_u.raw.inode_xattr_block.inode_xattr_block_len = 0;


    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;


    if ((size = export_getxattr_raw(exp, (unsigned char *) arg->arg_gw.fid, arg->arg_gw.name,
            NULL, arg->arg_gw.size,&ret
	    )) == -1) {
        goto error;
    }
    ret.status_gw.status = EP_SUCCESS;
    goto out;
error:
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_getxattr_raw_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_getxattr);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd removexattr: remove extended attribute

    @param args : fid of the object, extended attribute name 
    
    @retval: EP_SUCCESS : no returned value
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_removexattr_1_svc_nb( void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_status_ret_t ret;
    epgw_removexattr_arg_t * arg = (epgw_removexattr_arg_t*)pt;
    export_t *exp;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;


    START_PROFILING(ep_removexattr);

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;

    if (export_removexattr(exp, (unsigned char *) arg->arg_gw.fid, arg->arg_gw.name) != 0) {
        goto error;
    }

    ret.status_gw.status = EP_SUCCESS;
    goto out;
error:
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_status_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_removexattr);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd listxattr: list the extended attributes associated with an object (file,directory)

    @param args : fid of the object 
    
    @retval: EP_SUCCESS : list of the object extended attributes (name and value)
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_listxattr_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_listxattr_ret_t ret;
    export_t *exp;
    epgw_listxattr_arg_t * arg = (epgw_listxattr_arg_t*)pt; 
    ssize_t size = -1;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;


    START_PROFILING(ep_listxattr);

    xdr_free((xdrproc_t) xdr_epgw_listxattr_ret_t, (char *) &ret);

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;

    // Allocate memory
    ret.status_gw.ep_listxattr_ret_t_u.list.list_val =
            (char *) xmalloc(arg->arg_gw.size * sizeof (char));

    if ((size = export_listxattr(exp, (unsigned char *) arg->arg_gw.fid,
            ret.status_gw.ep_listxattr_ret_t_u.list.list_val, arg->arg_gw.size)) == -1) {
        goto error;
    }

    ret.status_gw.ep_listxattr_ret_t_u.list.list_len = size;

    ret.status_gw.status = EP_SUCCESS;
    goto out;
error:
    if (ret.status_gw.ep_listxattr_ret_t_u.list.list_val != NULL)
        free(ret.status_gw.ep_listxattr_ret_t_u.list.list_val);
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_listxattr_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_listxattr);
    return ;
}

/*
**______________________________________________________________________________
*/
/**
*   exportd set file lock 

    @param args 
    
    @retval: EP_SUCCESS : no returned value
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
/*
**______________________________________________________________________________
*/
/**
*   exportd set file lock 

    @param args 
    
    @retval: EP_SUCCESS : no returned value
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_set_file_lock_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_lock_ret_t ret;
    int    res;
    export_t *exp;
    epgw_lock_arg_t * arg = (epgw_lock_arg_t*)pt; 
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;


    START_PROFILING(ep_set_file_lock);

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;

    arg->arg_gw.client_info.socketRef = req_ctx_p->socketRef;
    res = export_set_file_lock(exp, (unsigned char *) arg->arg_gw.fid, &arg->arg_gw.lock, &ret.gw_status.ep_lock_ret_t_u.lock, &arg->arg_gw.client_info);
    if(res == 0) {
        ret.gw_status.status = EP_SUCCESS;
	memcpy(&ret.gw_status.ep_lock_ret_t_u.lock,&arg->arg_gw.lock, sizeof(ep_lock_t));
        goto out;
    }
    if (errno == EWOULDBLOCK) {
       ret.gw_status.status = EP_EAGAIN;
       goto out;	
    } 
error:    
    ret.gw_status.status = EP_FAILURE;
    ret.gw_status.ep_lock_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_set_file_lock);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd reset file lock 

    @param args 
    
    @retval: EP_SUCCESS : no returned value
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_clear_client_file_lock_1_svc_nb( void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_status_ret_t ret;
    export_t *exp;
    epgw_lock_arg_t * arg = pt;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;


    START_PROFILING(ep_clearclient_flock);

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;

    arg->arg_gw.client_info.socketRef = req_ctx_p->socketRef;
    if (export_clear_client_file_lock(exp, &arg->arg_gw.lock, &arg->arg_gw.client_info) != 0) {
        goto error;
    }

    ret.status_gw.status = EP_SUCCESS;
    goto out;
error:
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_status_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_clearclient_flock);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd reset all file locks of an owner 

    @param args 
    
    @retval: EP_SUCCESS : no returned value
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_clear_owner_file_lock_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_status_ret_t ret;
    export_t *exp;
    epgw_lock_arg_t * arg = pt; 
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;


    START_PROFILING(ep_clearowner_flock);

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;

    if (export_clear_owner_file_lock(exp, (unsigned char *) arg->arg_gw.fid, &arg->arg_gw.lock) != 0) {
        goto error;
    }

    ret.status_gw.status = EP_SUCCESS;
    goto out;
error:
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_status_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_clearowner_flock);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd set file lock 

    @param args 
    
    @retval: EP_SUCCESS : no returned value
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_get_file_lock_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_lock_ret_t ret;
    epgw_lock_arg_t * arg = pt;
    int    res;
    export_t *exp;
    DEBUG_FUNCTION;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;

    START_PROFILING(ep_get_file_lock);

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;

    res = export_get_file_lock(exp, (unsigned char *) arg->arg_gw.fid, &arg->arg_gw.lock, &ret.gw_status.ep_lock_ret_t_u.lock);
    if(res == 0) {
        ret.gw_status.status = EP_SUCCESS;
	memcpy(&ret.gw_status.ep_lock_ret_t_u.lock,&arg->arg_gw.lock, sizeof(ep_lock_t));
        goto out;
    }
    if (errno == EWOULDBLOCK) {
       ret.gw_status.status = EP_EAGAIN;
       goto out;	
    } 
error:
    ret.gw_status.status = EP_FAILURE;
    ret.gw_status.ep_lock_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_get_file_lock);
    return ;
}
/*
**______________________________________________________________________________
*/
/**
*   exportd poll file lock 

    @param args 
    
    @retval: EP_SUCCESS : no returned value
    @retval: EP_FAILURE :error code associated with the operation (errno)
*/
void ep_poll_file_lock_1_svc_nb( void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static epgw_status_ret_t ret;
    epgw_lock_arg_t * arg = pt; 
    export_t *exp;

    // Set profiler export index
    export_profiler_eid = arg->arg_gw.eid;

    START_PROFILING(ep_poll_file_lock);

    if (!(exp = exports_lookup_export(arg->arg_gw.eid)))
        goto error;
	
    arg->arg_gw.client_info.socketRef = req_ctx_p->socketRef;
    if (export_poll_file_lock(exp, &arg->arg_gw.lock, &arg->arg_gw.client_info) != 0) {
        goto error;
    }

    ret.status_gw.status = EP_SUCCESS;
    goto out;
error:
    ret.status_gw.status = EP_FAILURE;
    ret.status_gw.ep_status_ret_t_u.error = errno;
out:
    EXPORTS_SEND_REPLY(req_ctx_p);
    STOP_PROFILING(ep_poll_file_lock);
    return ;
}
