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


#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <libconfig.h>
#include <unistd.h>
#include <inttypes.h>
#include <config.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include "rozofs_ip4_flt.h"
#include "export.h"
#include "econfig.h"

#define ELAYOUT	    "layout"
#define EBSIZE	    "bsize"
#define EVIP	    "exportd_vip"
#define EVOLUMES    "volumes"
#define EVID        "vid"
#define EVID_FAST   "vid_fast"
#define ECIDS       "cids"
#define ECID        "cid"
#define ESTORAGES   "storages"
#define ESIDS       "sids"
#define ESID	    "sid"
#define EHOST	    "host"
#define ESITE       "site" // Optionnal For multi site deployment w/o eplication
#define EEXPORTS    "exports"
#define EEID        "eid"
#define EROOT       "root"
#define ENAME       "name"
#define EMD5        "md5"
#define ESQUOTA     "squota"
#define EHQUOTA     "hquota"
#define EHQUOTA_FAST     "hquota_fast"
#define SUFFIX_FILE     "suffix_file"
#define EGEOREP     "georep"
#define ESITE0     "site0"
#define ESITE1     "site1"
#define EREBALANCE "rebalance"

#define EFILTERS    "filters"
#define EFILTER     "filter"
#define ERULE       "rule"
#define EALLOW      "allow"
#define EFORBID     "forbid"
#define EIP4SUBNET  "ip4subnet"
#define ESUBNETS    "subnets"
#define ETHIN       "thin-provisioning"
#define ENODEID     "nodeid"
#define EFLOCKP     "flockp"

/*
** constant for exportd gateways
*/
#define EXPORTDID     "export_gateways"
#define EDAEMONID     "daemon_id"
#define EGWIDS     "gwids"
#define EGWID     "gwid"


econfig_t   exportd_config;
econfig_t   exportd_reloaded_config;
econfig_t * exportd_config_to_show = &exportd_config;


int storage_node_config_initialize(storage_node_config_t *s, uint8_t sid,
        const char *host, uint8_t siteNum) {
    int status = -1;

    DEBUG_FUNCTION;

    if (sid > SID_MAX || sid < SID_MIN) {
        fatal("The SID value must be between %u and %u", SID_MIN, SID_MAX);
        goto out;
    }
    if (siteNum == 0) {
        fatal("Invalid site number 0");
        goto out;    
    }
    memset(s,0,sizeof(storage_node_config_t));
    s->sid = sid;
    strncpy(s->host, host, ROZOFS_HOSTNAME_MAX);
    s->siteNum = siteNum;
    list_init(&s->list);

    status = 0;
out:
    return status;
}

void storage_node_config_release(storage_node_config_t *s) {
    return;
}

int cluster_config_initialize(cluster_config_t *c, cid_t cid) {
    DEBUG_FUNCTION;
    int i;
    memset(c,0,sizeof(cluster_config_t));
    c->cid = cid;
    for (i = 0; i <ROZOFS_GEOREP_MAX_SITE; i++) list_init(&c->storages[i]);
    list_init(&c->list);
    return 0;
}

void cluster_config_release(cluster_config_t *c) {
    list_t *p, *q;
    DEBUG_FUNCTION;
    int i;

    for (i = 0; i <ROZOFS_GEOREP_MAX_SITE; i++) 
    {
      list_for_each_forward_safe(p, q, (&c->storages[i])) {
          storage_node_config_t *entry = list_entry(p, storage_node_config_t,
                  list);
          storage_node_config_release(entry);
          list_remove(p);
          xfree(entry);
      }
    }
}

int volume_config_initialize(volume_config_t *v, vid_t vid, uint8_t layout,uint8_t georep) {
    DEBUG_FUNCTION;

    v->vid = vid;
    v->layout = layout;
    v->georep = georep;
    v->rebalance_cfg = NULL;
    list_init(&v->clusters);
    list_init(&v->list);
    return 0;
}

void volume_config_release(volume_config_t *v) {
    list_t *p, *q;
    DEBUG_FUNCTION;

    list_for_each_forward_safe(p, q, &v->clusters) {
        cluster_config_t *entry = list_entry(p, cluster_config_t, list);
        cluster_config_release(entry);
        if (v->rebalance_cfg) xfree(v->rebalance_cfg);
        v->rebalance_cfg = NULL;
        list_remove(p);
        xfree(entry);
    }
}


/**< exportd expgw */
int expgw_node_config_initialize(expgw_node_config_t *s, uint8_t gwid,
        const char *host) {
    int status = -1;

    DEBUG_FUNCTION;

    if (gwid > GWID_MAX || gwid < GWID_MIN) {
        fatal("The Exportd Gateway Id value must be between %u and %u", GWID_MIN, GWID_MAX);
        goto out;
    }

    s->gwid = gwid;
    strcpy(s->host, host);
    list_init(&s->list);

    status = 0;
out:
    return status;
}
/**< exportd expgw */
void expgw_node_config_release(expgw_node_config_t *s) {
    return;
}

/**< exportd expgw */
int expgw_config_initialize(expgw_config_t *v, int daemon_id) {
    DEBUG_FUNCTION;

    v->daemon_id = daemon_id;
    list_init(&v->expgw_node);
    list_init(&v->list);
    return 0;
}
/**< exportd expgw */
void expgw_config_release(expgw_config_t *c) {
    list_t *p, *q;
    DEBUG_FUNCTION;

    list_for_each_forward_safe(p, q, &c->expgw_node) {
        expgw_node_config_t *entry = list_entry(p, expgw_node_config_t,
                list);
        expgw_node_config_release(entry);
        list_remove(p);
        xfree(entry);
    }
}




int export_config_initialize(export_config_t *e, eid_t eid, vid_t vid, uint8_t layout, uint32_t bsize,
        const char *root, const char * name, const char *md5, uint64_t squota, uint64_t hquota, 
	const char *filter_name, int thin,vid_t vid_fast, uint64_t hquota_fast,int suffix_file, 
        int flockp) {
    DEBUG_FUNCTION;

    e->eid = eid;
    e->vid = vid;
    e->vid_fast = vid_fast;
    e->layout = layout;
    e->bsize = bsize;
    strncpy(e->root, root, FILENAME_MAX);
    strncpy(e->name, name, FILENAME_MAX);
    strncpy(e->md5, md5, MD5_LEN);
    e->squota = squota;
    e->hquota = hquota;    
    e->hquota_fast = hquota_fast;
    e->suffix_file_idx = suffix_file;
    if (thin) {
      e->thin = 1;
    }
    else {
      e->thin = 0;
    }
    if (flockp) {   
      e->flockp = 1;
    }
    else {
      e->flockp = 0;
    }  
    if (filter_name == NULL) {
      e->filter_name = NULL;
    }  
    else {  
      e->filter_name = xstrdup(filter_name);
    }  
    list_init(&e->list);
    return 0;
}

void export_config_release(export_config_t *s) {
    if (s->filter_name) {
      xfree(s->filter_name);
      s->filter_name = NULL;
    }  
    return;
}

void filter_config_release(filter_config_t *s) {
    xfree(s->name);
    s->name = NULL;
    rozofs_ip4_filter_tree_release(s->filter_tree);
    s->filter_tree = NULL; 
    return;
}
int econfig_initialize(econfig_t *ec) {
    DEBUG_FUNCTION;

    list_init(&ec->volumes);
    list_init(&ec->exports);
    list_init(&ec->expgw);
    list_init(&ec->filters);    
    return 0;
}
static void econfig_number_storages_per_cluster(volume_config_t *config) {
  list_t *l1, *l2, *l3;
  int host_rank;
  int site = 0;
  cluster_config_t *c;
  storage_node_config_t *s1, *s2;
  
  /*
  ** For each site
  */
  for (site = 0; site <ROZOFS_GEOREP_MAX_SITE; site++) {

    /*
    ** Loop on cluster
    */
    list_for_each_forward(l1, &config->clusters) {
    
      c = list_entry(l1, cluster_config_t, list);
      host_rank = 0;
      
      /*
      ** Loop on storages of this cluster & site
      */
      list_for_each_forward(l2, (&c->storages[site])) {
      
        s1 = list_entry(l2, storage_node_config_t, list);

        /*
	** Check against the previous storages in the cluster
	*/         
        list_for_each_forward(l3, (&c->storages[site])) {
	
          s2 = list_entry(l3, storage_node_config_t, list);
	  
	  if (s2 == s1) {
	    /*
	    *** Previous storages have a different host name
	    */
	    s1->host_rank = host_rank++;
	    break;
	  }  
	  
	  /*
	  ** Same host for s2 & s1
	  */
	  if (strcmp(s2->host,s1->host)==0) {
	    s1->host_rank = s2->host_rank;
	    break;
	  }
	}
      }
      c->nb_host[site] = host_rank;	  
    }
  }
}
void econfig_release(econfig_t *config) {
    list_t *p, *q;
    DEBUG_FUNCTION;

    list_for_each_forward_safe(p, q, &config->volumes) {
        volume_config_t *entry = list_entry(p, volume_config_t, list);
        volume_config_release(entry);
        list_remove(p);
        xfree(entry);
    }

    list_for_each_forward_safe(p, q, &config->exports) {
        export_config_t *entry = list_entry(p, export_config_t, list);
        export_config_release(entry);
        list_remove(p);
        xfree(entry);
    }
    list_for_each_forward_safe(p, q, &config->expgw) {
        expgw_config_t *entry = list_entry(p, expgw_config_t, list);
        expgw_config_release(entry);
        list_remove(p);
        xfree(entry);
    }
    list_for_each_forward_safe(p, q, &config->filters) {
        filter_config_t *entry = list_entry(p, filter_config_t, list);
        filter_config_release(entry);
        list_remove(p);
        xfree(entry);
    }    
}

static int load_volumes_conf(econfig_t *ec, struct config_t *config, int elayout) {
    int status = -1, v, c, s;
    struct config_setting_t *volumes_set = NULL;
    int    multi_site = -1;
    const char *rebalance_cfg;
    
    DEBUG_FUNCTION;

    // Get settings for volumes (list of volumes)
    if ((volumes_set = config_lookup(config, EVOLUMES)) == NULL) {
        errno = ENOKEY;
        severe("can't lookup volumes setting.");
        goto out;
    }

    // For each volume
    for (v = 0; v < config_setting_length(volumes_set); v++) {

        // Check version of libconfig
#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR >= 4)) \
               || (LIBCONFIG_VER_MAJOR > 1))
        int vid,vlayout; // Volume identifier
	int vgeorep;
#else
        long int vid,vlayout; // Volume identifier
	int vgeorep;
#endif
        struct config_setting_t *vol_set = NULL; // Settings for one volume
        /* Settings of list of clusters for one volume */
        struct config_setting_t *clu_list_set = NULL;
        volume_config_t *vconfig = NULL;
		
        // Get settings for the volume config
        if ((vol_set = config_setting_get_elem(volumes_set, v)) == NULL) {
            errno = ENOKEY;
            severe("can't get volume setting %d.", v);
            goto out;
        }

        // Lookup vid for this volume
        if (config_setting_lookup_int(vol_set, EVID, &vid) == CONFIG_FALSE) {
            errno = ENOKEY;
            severe("can't lookup vid setting for volume idx: %d.", v);
            goto out;
        }
        /*
        ** Looking for a rebalane configuration file 
        ** for automatic rebalance launching
        */
        rebalance_cfg = NULL;
        if (config_setting_lookup_string(vol_set, EREBALANCE, &rebalance_cfg) == CONFIG_FALSE) {
          rebalance_cfg = NULL;
        }
        
        // Check whether a layout is specified for this volume in the configuration file
	// or take the layout from the export default value
        if (config_setting_lookup_int(vol_set, ELAYOUT, &vlayout) == CONFIG_FALSE) {
	  /* 
	  ** No specific layout given for this volume. Get the export default layout.
	  */
          vlayout = elayout;	
        }

        // Check whether geo-replication is specified for this volume in the
        // configuration file, default value is none
        if (config_setting_lookup_bool(vol_set, EGEOREP,
                &vgeorep) == CONFIG_FALSE) {
            // No specific layout given for this volume.
            // Get the export default geo-replication.
            vgeorep = 0;
        }
	if (vgeorep!= 0)
	{
	  /*
	  ** the site file MUST exist
	  */
	  if (rozofs_no_site_file)
	  {
	    severe("RozoFS site file is missing (%s/rozofs_site)",ROZOFS_CONFIG_DIR);
	    goto out;
	  }
	}
        // Allocate new volume_config
        vconfig = (volume_config_t *) xmalloc(sizeof (volume_config_t));
        if (volume_config_initialize(vconfig, (vid_t) vid, (uint8_t) vlayout,(uint8_t)vgeorep) != 0) {
            severe("can't initialize volume.");
            goto out;
        }

        vconfig->multi_site = 0;
        multi_site = -1; // To determine whether multi site or not
        
        if (rebalance_cfg!=NULL) {
          vconfig->rebalance_cfg = xstrdup(rebalance_cfg);
        } 

        // Get settings for clusters for this volume
        if ((clu_list_set = config_setting_get_member(vol_set, ECIDS)) == NULL) {
            errno = ENOKEY;
            severe("can't get cids setting for volume idx: %d.", v);
            goto out;
        }

        // For each cluster of this volume
        for (c = 0; c < config_setting_length(clu_list_set); c++) {

            // Check version of libconfig
#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR >= 4)) \
               || (LIBCONFIG_VER_MAJOR > 1))
            int cid;
#else
            long int cid;
#endif
            struct config_setting_t *stor_set;
            struct config_setting_t *clu_set;
            cluster_config_t *cconfig;

            // Get settings for this cluster
            if ((clu_set = config_setting_get_elem(clu_list_set, c)) == NULL) {
                errno = ENOKEY;
                severe("can't get cluster setting for volume idx: %d cluster idx: %d.", v, c);
                goto out;
            }

            // Lookup cid for this cluster
            if (config_setting_lookup_int(clu_set, ECID, &cid) == CONFIG_FALSE) {
                errno = ENOKEY;
                severe("can't lookup cid for volume idx: %d cluster idx: %d.", v, c);
                goto out;
            }

            // Allocate a new cluster_config
            cconfig = (cluster_config_t *) xmalloc(sizeof (cluster_config_t));
            if (cluster_config_initialize(cconfig, (cid_t) cid) != 0) {
                severe("can't initialize cluster config.");
            }

            // Get settings for sids for this cluster
            if ((stor_set = config_setting_get_member(clu_set, ESIDS)) == NULL) {
                errno = ENOKEY;
                severe("can't get sids for volume idx: %d cluster idx: %d.", v, c);
                goto out;
            }

            for (s = 0; s < config_setting_length(stor_set); s++) {

                struct config_setting_t *mstor_set = NULL;
                // Check version of libconfig
#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR >= 4)) \
               || (LIBCONFIG_VER_MAJOR > 1))
                int sid,siteNum;
#else
                long int sid,siteNum;
#endif
                const char *host;
                storage_node_config_t *snconfig = NULL;

                // Get settings for the storage_config
                if ((mstor_set = config_setting_get_elem(stor_set, s)) == NULL) {
                    errno = ENOKEY;
                    severe("can't get storage config for volume idx: %d\
                             , cluster idx: %d, storage idx: %d.", v, c, s);
                    goto out;
                }

                // Lookup sid for this storage
                if (config_setting_lookup_int(mstor_set, ESID, &sid) == CONFIG_FALSE) {
                    errno = ENOKEY;
                    severe("can't get sid for volume idx: %d\
                             , cluster idx: %d,  storage idx: %d.", v, c, s);
                    goto out;
                }
		
                // Check the multi site case
                siteNum = -1; // value when no site number defined
                if (config_setting_lookup_int(mstor_set, ESITE, &siteNum) == CONFIG_FALSE) {
		
                  // 1rst sid has no site information 
                  // This is so not a multi site configuration
                  if (multi_site == -1) {
                    multi_site = 0;
                  }
		   
                  // Every sid must be configured without site information
                  if (multi_site == 1) {
                     severe("cid %d sid %d has not site number, while other have.",
			                 (int) cid, (int) sid);
                      goto out;
		          }   
                }
                else {
		
                  // 1rst sid has site information 
                  // This is so a multi site configuration
                  if (multi_site == -1) {
                    multi_site = 1;
                    vconfig->multi_site = 1;
							     
                    // Geo replication must not be configured as well
                    if (vgeorep) {
                      errno = EINVAL;
                      severe("cid %d sid %d has a site number, while geo replication is configured",
			                (int) cid, (int) sid);
                      goto out;		     
                    }
                  }
		   
                  // Every sid must be configured with site information
                  if (multi_site == 0) {
                     severe("cid %d sid %d has a site number, while other have not.",
			                 (int) cid, (int) sid);
                      goto out;
                  }   		     
                }		
		
                if (vgeorep == 0) {
                  // Lookup hostname for this storage
                  if (config_setting_lookup_string(mstor_set, EHOST, &host) == CONFIG_FALSE) {
                      errno = ENOKEY;
                      severe("can't get host for volume idx: %d\
                               , cluster idx: %d,  storage idx: %d.", v, c, s);
                      goto out;
                  }
                  // Check length of storage hostname
                  if (strlen(host) > ROZOFS_HOSTNAME_MAX) {
                      errno = ENAMETOOLONG;
                      severe("Storage hostname length (volume idx: %d\
                               , cluster idx: %d, storage idx: %d) must be lower\
                           than %d.", v, c, s, ROZOFS_HOSTNAME_MAX);
                      goto out;
                  }

                  // Allocate a new storage_config
                  snconfig = (storage_node_config_t *) xmalloc(sizeof (storage_node_config_t));
                  if (storage_node_config_initialize(snconfig, (uint8_t) sid, host, siteNum) != 0) {
                      severe("can't initialize storage node config.");

                  }

                  // Add it to the cluster.
                  list_push_back((&cconfig->storages[0]), &snconfig->list);
		}
		else
		{

                  // Lookup hostname for this storage
                  if (config_setting_lookup_string(mstor_set, ESITE0, &host) == CONFIG_FALSE) {
                      errno = ENOKEY;
                      severe("can't get host for volume idx: %d\
                               , cluster idx: %d,  storage idx: %d.", v, c, s);
                      goto out;
                  }
                  // Check length of storage hostname
                  if (strlen(host) > ROZOFS_HOSTNAME_MAX) {
                      errno = ENAMETOOLONG;
                      severe("Storage hostname length (volume idx: %d\
                               , cluster idx: %d, storage idx: %d) must be lower\
                           than %d.", v, c, s, ROZOFS_HOSTNAME_MAX);
                      goto out;
                  }

                  // Allocate a new storage_config
                  snconfig = (storage_node_config_t *) xmalloc(sizeof (storage_node_config_t));
                  if (storage_node_config_initialize(snconfig, (uint8_t) sid, host, -1) != 0) {
                      severe("can't initialize storage node config.");

                  }

                  // Add it to the cluster.
                  list_push_back((&cconfig->storages[0]), &snconfig->list);
                  // Lookup hostname for this storage
                  if (config_setting_lookup_string(mstor_set, ESITE1, &host) == CONFIG_FALSE) {
                      errno = ENOKEY;
                      severe("can't get host for volume idx: %d\
                               , cluster idx: %d,  storage idx: %d.", v, c, s);
                      goto out;
                  }
                  // Check length of storage hostname
                  if (strlen(host) > ROZOFS_HOSTNAME_MAX) {
                      errno = ENAMETOOLONG;
                      severe("Storage hostname length (volume idx: %d\
                               , cluster idx: %d, storage idx: %d) must be lower\
                           than %d.", v, c, s, ROZOFS_HOSTNAME_MAX);
                      goto out;
                  }

                  // Allocate a new storage_config
                  snconfig = (storage_node_config_t *) xmalloc(sizeof (storage_node_config_t));
                  if (storage_node_config_initialize(snconfig, (uint8_t) sid, host, -1) != 0) {
                      severe("can't initialize storage node config.");

                  }

                  // Add it to the cluster.
                  list_push_back((&cconfig->storages[1]), &snconfig->list);
		}
				

            }

            // Add the cluster to the volume
            list_push_back(&vconfig->clusters, &cconfig->list);

        } // End add cluster
		
		// Number the storages in each cluster of the volume
		// For later distribution upon SID
		econfig_number_storages_per_cluster(vconfig);

        // Add this volume to the list of volumes
        list_push_back(&ec->volumes, &vconfig->list);
    } // End add volume

    status = 0;
out:
    return status;
}

static int load_expgw_conf(econfig_t *ec, struct config_t *config) {
    int status = -1, v, s;
    struct config_setting_t *master_expgw_set = NULL;

    DEBUG_FUNCTION;

    // Get settings for expgw (list of expgw)
    if ((master_expgw_set = config_lookup(config, EXPORTDID)) == NULL) {
//        errno = ENOKEY;
//        severe("can't lookup expgw setting.");
        status = 0;
        goto out;
    }

    // For each expgw
    for (v = 0; v < config_setting_length(master_expgw_set); v++) {

        // Check version of libconfig
#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR >= 4)) \
               || (LIBCONFIG_VER_MAJOR > 1))
        int daemon_id; // Replica identifier
#else
        long int daemon_id; // Replica identifier
#endif
        struct config_setting_t *expgw_set = NULL; // Settings for one volume
        struct config_setting_t *mexpgw_set = NULL; // Settings for one volume
        /* Settings of list of clusters for one volume */
        expgw_config_t *rconfig = NULL;

        // Get settings for the volume config
        if ((expgw_set = config_setting_get_elem(master_expgw_set, v)) == NULL) {
            errno = ENOKEY;
            severe("can't get expgw setting %d.", v);
            goto out;
        }

        // Lookup daemon_id for this volume
        if (config_setting_lookup_int(expgw_set, EDAEMONID, &daemon_id) == CONFIG_FALSE) {
            errno = ENOKEY;
            severe("can't lookup daemon_id setting for volume idx: %d.", v);
            goto out;
        }

        // Allocate new expgw_config
        rconfig = (expgw_config_t *) xmalloc(sizeof (expgw_config_t));
        if (expgw_config_initialize(rconfig, (int) daemon_id) != 0) {
            severe("can't initialize expgw.");
            goto out;
        }

            // Get settings for gwids for this exportd expgw
            if ((mexpgw_set = config_setting_get_member(expgw_set, EGWIDS)) == NULL) {
                errno = ENOKEY;
                severe("can't get gwids for daemon_id idx: %d ", v);
                goto out;
            }

            for (s = 0; s < config_setting_length(mexpgw_set); s++) {

                struct config_setting_t *node_expgw_set = NULL;
                // Check version of libconfig
#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR >= 4)) \
               || (LIBCONFIG_VER_MAJOR > 1))
                int gwid;
#else
                long int gwid;
#endif
                const char *host;
                expgw_node_config_t *snconfig = NULL;

                // Get settings for the expgw_node_config_t
                if ((node_expgw_set = config_setting_get_elem(mexpgw_set, s)) == NULL) {
                    errno = ENOKEY;
                    severe("can't get expgw node config for export expgw: %d\
                             , expgw node idx: %d.", v, s);
                    goto out;
                }

                // Lookup sid for this storage
                if (config_setting_lookup_int(node_expgw_set, EGWID, &gwid) == CONFIG_FALSE) {
                    errno = ENOKEY;
                    severe("can't get gwid for export expgw: %d\
                               , expgw node idx: %d.", v, s);
                    goto out;
                }

                // Lookup hostname for this storage
                if (config_setting_lookup_string(node_expgw_set, EHOST, &host) == CONFIG_FALSE) {
                    errno = ENOKEY;
                    severe("can't get host for export expgw: %d\
                               , expgw node idx: %d.", v, s);
                    goto out;
                }

                // Allocate a new expgw_node_config
                snconfig = (expgw_node_config_t *) xmalloc(sizeof (expgw_node_config_t));
                if (expgw_node_config_initialize(snconfig, (uint8_t) gwid, host) != 0) {
                    severe("can't initialize storage node config.");
                }

            // Add the expgw node to the exportd expgw
            list_push_back(&rconfig->expgw_node, &snconfig->list);

        } // End add cluster

        // Add this volume to the list of expgw
        list_push_back(&ec->expgw, &rconfig->list);
    } // End add volume

    status = 0;
out:
    return status;
}
static int load_filters_conf(econfig_t *ec, struct config_t *config) {
    int status = -1, i, s;
    struct config_setting_t *filter_set = NULL;
    rozofs_ip4_subnet_t * filter_tree;
    filter_config_t     * efilter
    DEBUG_FUNCTION;

    // Get the filters settings
    if ((filter_set = config_lookup(config, EFILTERS)) == NULL) {
        return 0;
    }

    // For each filter
    for (i = 0; i < config_setting_length(filter_set); i++) {
        struct config_setting_t * filter_setting = NULL;
        const char              * filter_name;
        const char              * filter_rule;
        int                       rule;
        struct config_setting_t * subnet_set = NULL;
        
        if ((filter_setting = config_setting_get_elem(filter_set, i)) == NULL) {
            errno = ENOKEY;
            severe("can't get filter idx: %d.", i);
            goto out;
        }

        if (config_setting_lookup_string(filter_setting, EFILTER, &filter_name) == CONFIG_FALSE) {
            errno = ENOKEY;
            severe("can't look up filter name for filter idx: %d", i);
            goto out;
        }

        if (config_setting_lookup_string(filter_setting, ERULE, &filter_rule) == CONFIG_FALSE) {
          rule = ROZOFS_IP4_FORBID;
        }
        else {
          if      (strcasecmp(filter_rule,EALLOW)==0)  rule = ROZOFS_IP4_ALLOW;
          else if (strcasecmp(filter_rule,EFORBID)==0) rule = ROZOFS_IP4_FORBID;
          else {
            errno = ENOKEY;
            severe("Unexpected rule \"%s\" for filter %s", filter_rule, filter_name);
            goto out;      
          }      
        }
        
        // Get subnets in the filter
        if ((subnet_set = config_setting_get_member(filter_setting, ESUBNETS)) == NULL) {
          severe("can't lookup subnets in filter %s", filter_name);
          continue;
        }
        
        /*
        ** Create the filter tree
        */
        filter_tree = rozofs_ip4_filter_tree_allocate(rule,config_setting_length(subnet_set));

        // For each subnet 
        for (s = 0; s < config_setting_length(subnet_set); s++) {
            struct config_setting_t * subnet_setting;
            const char              * subnet_name;
            uint32_t                  scanip[4];
            uint32_t                  ip;
            uint32_t                  subnetLen;
            int                       ret;
            int                       idx;

            // Get settings for this subnet
            if ((subnet_setting = config_setting_get_elem(subnet_set, s)) == NULL) {
                errno = ENOKEY;
                severe("can't get subnet setting %d for filter %s", s, filter_name);
                rozofs_ip4_filter_tree_release(filter_tree);
                goto out;
            }

            if (config_setting_lookup_string(subnet_setting, ERULE, &filter_rule) == CONFIG_FALSE) {
                errno = ENOKEY;
                severe("can't get subnet rule for subnet %d for filter %s", s, filter_name);
                goto out;
            }
            if      (strcasecmp(filter_rule,EALLOW)==0)  rule = ROZOFS_IP4_ALLOW;
            else if (strcasecmp(filter_rule,EFORBID)==0) rule = ROZOFS_IP4_FORBID;
            else {
              errno = ENOKEY;
              severe("Unexpected rule \"%s\" for subnet %d for filter %s", filter_rule, s, filter_name);
              goto out;      
            }      
        
            // Lookup IPv4 subnet
            if (config_setting_lookup_string(subnet_setting, EIP4SUBNET, &subnet_name) == CONFIG_FALSE) {
                errno = ENOKEY;
                severe("can't look up IPv4 subnet in subnet %d of filter %s", s, filter_name);
                rozofs_ip4_filter_tree_release(filter_tree);
                goto out;
            }
            ret = sscanf(subnet_name,"%u.%u.%u.%u/%u", &scanip[0], &scanip[1], &scanip[2], &scanip[3],&subnetLen);
            if (ret != 5) {
              severe("Expecting an IPv4 subnet and got \"%s\" in subnet %d of filter %s", subnet_name, s, filter_name);
              rozofs_ip4_filter_tree_release(filter_tree);
              goto out;
            }
            for (idx=0; idx<4; idx++) {
              if ((scanip[idx]<0)||(scanip[idx]>255)) {
                severe("Bad IPv4 subnet \"%s\" in subnet %d of filter %s", subnet_name, s, filter_name);
                rozofs_ip4_filter_tree_release(filter_tree);
                goto out;
              }
            }
            ip = (scanip[0]<<24) + (scanip[1]<<16) + (scanip[2]<<8) + scanip[3];
            if (subnetLen>32) {
              severe("Bad IPv4 subnet len \"%s\" in subnet %d of filter %s", subnet_name, s, filter_name);
              rozofs_ip4_filter_tree_release(filter_tree);
              goto out;
            }
            
            if (rozofs_ip4_filter_add_subnet(filter_tree, ip, subnetLen, rule)<0) {
              severe("Can not add subnet %s of filter %s", subnet_name, filter_name);
              rozofs_ip4_filter_tree_release(filter_tree);
              goto out;
            }            

        } // End subnet loop      

        efilter = xmalloc(sizeof (filter_config_t));
        efilter->name        = xstrdup(filter_name);
        efilter->filter_tree = filter_tree;
        list_init(&efilter->list);
        list_push_back(&ec->filters, &efilter->list);

    }
    status = 0;
out:
    return status;
}


static int strquota_to_nbblocks(const char *str, uint64_t *blocks, ROZOFS_BSIZE_E bsize) {
    int status = -1;
    char *unit;
    uint64_t value;

    errno = 0;
    value = strtoull(str, &unit, 10);
    if ((errno == ERANGE && (value == LONG_MAX || value == LONG_MIN))
            || (errno != 0 && value == 0)) {
        goto out;
    }

    // no digit, no quota
    if (unit == str) {
        *blocks = 0;
        status = 0;
        goto out;
    }

    switch (*unit) {
        case 'K':
            *blocks = (1024ULL * value) / ROZOFS_BSIZE_BYTES(bsize);
            break;
        case 'M':
            *blocks = (1024ULL * 1024ULL * value) / ROZOFS_BSIZE_BYTES(bsize);
            break;
        case 'G':
            *blocks = (1024ULL * 1024ULL * 1024ULL * value) / ROZOFS_BSIZE_BYTES(bsize);
            break;
        case 'T':
            *blocks = (1024ULL * 1024ULL * 1024ULL * 1024ULL * value) / ROZOFS_BSIZE_BYTES(bsize);
            break;
        case 'P':
            *blocks = (1024ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL * value) / ROZOFS_BSIZE_BYTES(bsize);
            break;
        default: // no unit -> nb blocks
            *blocks = value;
            break;
    }

    status = 0;

out:
    return status;
}

static int load_exports_conf(econfig_t *ec, struct config_t *config) {
    int status = -1, i;
    struct config_setting_t *export_set = NULL;
    char   dafault_root_path[FILENAME_MAX];

    /*
    ** Prior to read the export configuration, we need
    ** to parse the filter configuration that will be used 
    ** by the exports
    */
    if (load_filters_conf(ec, config) != 0) {
        severe("can't load filters config.");
        goto out;
    }

    DEBUG_FUNCTION;

    // Get the exports settings
    if ((export_set = config_lookup(config, EEXPORTS)) == NULL) {
        errno = ENOKEY;
        severe("can't lookup exports setting.");
        goto out;
    }

    // For each export
    for (i = 0; i < config_setting_length(export_set); i++) {
        struct config_setting_t *mfs_setting = NULL;
        const char *root;
        const char *name;
        const char *md5;
        int thin;
        int flockp;
        // Check version of libconfig
#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR >= 4)) \
               || (LIBCONFIG_VER_MAJOR > 1))
        int eid; // Export identifier
        int vid; // Volume identifier
        int vid_fast; // Volume identifier
	int bsize; // Block size
	int layout; // Export layout
	int suffix_file;
#else
        long int eid; // Export identifier
        long int vid; // Volume identifier
        long int vid_fast; // Volume identifier
        long int bsize; // Block size
	long int layout; // Export layout
	long int suffix_file;
#endif
        const char *str;
        uint64_t squota;
        uint64_t hquota;
        uint64_t hquota_fast;
	
        export_config_t *econfig = NULL;
        const char * filter_name;
        
        if ((mfs_setting = config_setting_get_elem(export_set, i)) == NULL) {
            errno = ENOKEY;
            severe("can't get export idx: %d.", i);
            goto out;
        }

        if (config_setting_lookup_int(mfs_setting, EEID, &eid) == CONFIG_FALSE) {
            errno = ENOKEY;
            severe("can't look up eid for export idx: %d", i);
            goto out;
        }

        // Default block size is 4K
	    bsize = ROZOFS_BSIZE_4K;
	
        if (config_setting_lookup_int(mfs_setting, EBSIZE, &bsize) != CONFIG_FALSE) {
	
	      // bsize is given as an integer
	      if ((bsize < ROZOFS_BSIZE_MIN) || (bsize > ROZOFS_BSIZE_MAX)) {
            errno = EINVAL;
            severe("Block size must be within [%d:%d]", ROZOFS_BSIZE_MIN,ROZOFS_BSIZE_MAX);
            goto out;
          }
	    }
	    else {
	
	      // bsize is not int but may be a string
	      if (config_setting_lookup_string(mfs_setting, EBSIZE, &str) != CONFIG_FALSE) {

	    	// bsize is given as a string
	    	if      (strcasecmp(str,"4K")==0)  bsize = ROZOFS_BSIZE_4K;
	    	else if (strcasecmp(str,"8K")==0)  bsize = ROZOFS_BSIZE_8K;
	    	else if (strcasecmp(str,"16K")==0) bsize = ROZOFS_BSIZE_16K;
	    	else if (strcasecmp(str,"32K")==0) bsize = ROZOFS_BSIZE_32K;
	    	else {
            	  errno = EINVAL;
            	  severe("Bad block size %s", str);
            	  goto out;             
	    	}
		  }     
        }

	
        /*
        ** Search for root path
        */
        if (config_setting_lookup_string(mfs_setting, EROOT, &root) != CONFIG_FALSE) {
          // Check root path length
          if (strlen(root) >= FILENAME_MAX) {
              errno = ENAMETOOLONG;
              severe("root path length for export idx: %d must be lower than %d.",
                      i, FILENAME_MAX);
              goto out;
          }
        }
        /* 
        ** Use default root path name
        */
        else {
          sprintf(dafault_root_path,"%s/export_%d", EXPORTS_ROOT, (int) eid);
          root = dafault_root_path;
        }        

        /*
        ** Search for a name
        */        
        if (config_setting_lookup_string(mfs_setting, ENAME, &name) != CONFIG_FALSE) {
          // Check root path length
          if (strlen(name) >= FILENAME_MAX) {
              errno = ENAMETOOLONG;
              severe("export name length for export idx: %d must be lower than %d.",
                      i, FILENAME_MAX);
              goto out;
          }
        }
        /*
        ** Use oot path as default path name, which is the initial RozoFS behavior
        */        
        else {
          name = root;
        }          

        md5 = "";
        if (config_setting_lookup_string(mfs_setting, EMD5, &md5) != CONFIG_FALSE) {
	  warning("MD5 parameter is ignored");
	  md5 = "";
#if 0	  
          // Check md5 length
          if (strlen(md5) > MD5_LEN) {
              errno = EINVAL;
              severe("md5 crypt length for export idx: %d must be lower than %d.",
                      i, MD5_LEN);
              goto out;
          }
#endif	  
        }

        squota = 0;		
        if (config_setting_lookup_string(mfs_setting, ESQUOTA, &str) != CONFIG_FALSE) {
          if (strquota_to_nbblocks(str, &squota, bsize) != 0) {
              severe("%s: can't convert to quota)", str);
              goto out;
          }
        }
		
		hquota = 0;
        if (config_setting_lookup_string(mfs_setting, EHQUOTA, &str) != CONFIG_FALSE) {
          if (strquota_to_nbblocks(str, &hquota, bsize) != 0) {
              severe("%s: can't convert to quota)", str);
              goto out;
          }
        }
		
		hquota_fast = 0;
        if (config_setting_lookup_string(mfs_setting, EHQUOTA_FAST, &str) != CONFIG_FALSE) {
          if (strquota_to_nbblocks(str, &hquota_fast, bsize) != 0) {
              severe("%s: can't convert to quota)", str);
              goto out;
          }
        }
	suffix_file = -1;
        if (config_setting_lookup_int(mfs_setting, SUFFIX_FILE, &suffix_file) != CONFIG_FALSE) {

        }
		
        // Lookup volume identifier
        if (config_setting_lookup_int(mfs_setting, EVID, &vid) == CONFIG_FALSE) {
            errno = ENOKEY;
            severe("can't look up vid for export idx: %d", i);
            goto out;
        }
        // Lookup fast volume identifier
        if (config_setting_lookup_int(mfs_setting, EVID_FAST, &vid_fast) == CONFIG_FALSE) {
            vid_fast = -1;
        }

        // Check for thin provisionning
        thin = 0;
        if (config_setting_lookup_bool(mfs_setting, ETHIN, &thin)== FALSE) {
          thin = 0;
        }  

        /*
        ** Are persistent file locks configured
        */
        flockp = 0;
        if (config_setting_lookup_bool(mfs_setting, EFLOCKP, &flockp) == FALSE) {
          flockp = 0;
        }  
		
        // Lookup export layout if any
        if (config_setting_lookup_int(mfs_setting, ELAYOUT, &layout) == CONFIG_FALSE) {
	  layout = -1;           
        }
        
        /*
        IPv4 filtering
        */
        if (config_setting_lookup_string(mfs_setting, EFILTER, &filter_name) == CONFIG_FALSE) {
          filter_name = NULL;
        }
        else {
          /*
          ** Check filter exists 
          */
          rozofs_ip4_subnet_t * tree = rozofs_ip4_flt_get_tree(ec,filter_name);
          if (tree == NULL) {
            fatal("No such IPv4 filter \"%s\" defined for export %d.",filter_name, (int)eid);
          }  
        }
	
        econfig = xmalloc(sizeof (export_config_t));
        if (export_config_initialize(econfig, (eid_t) eid, (vid_t) vid, layout, bsize, root, name,
                md5, squota, hquota, filter_name, thin, (vid_t) vid_fast,hquota_fast,suffix_file,
                flockp) != 0) {
            severe("can't initialize export config.");
        }

        // Initialize export

        // Add this export to the list of exports
        list_push_back(&ec->exports, &econfig->list);

    }
    status = 0;
out:
    return status;
}


int load_exports_conf_api(econfig_t *ec, struct config_t *config) {
   return load_exports_conf(ec,config);
}
/*
**_________________________________________________________________________
**
** Replace old configuration by the new configuration
**
** @param old     The old configuration
** @param new     The new configuration
**
*/
void econfig_move(econfig_t *old, econfig_t *new) {
  
  /*
  ** Release old configuration
  */
  econfig_release(old);

  /*
  ** Recopy the new configuration
  */
  memcpy(old,new,sizeof(econfig_t));
  
  /*
  ** Reinitialize the list broken by memcpy and then move the lists 
  ** from the new configuration to the old configuration
  */
  list_init(&old->volumes);
  list_move(&old->volumes, &new->volumes);

  list_init(&old->exports);
  list_move(&old->exports, &new->exports);

  list_init(&old->expgw);
  list_move(&old->expgw, &new->expgw);
  
  list_init(&old->filters);      
  list_move(&old->filters, &new->filters);
  
//  info ("MOVED");
//  econfig_info(old);
  
} 
/* 
**_________________________________________________________________________
**
** Compare an old configuration to a new oneto see whether some new clusters 
** and/or SIDs appears in the new conf. These new CID/SID must not be used
** immediatly for distributing the new files because the STORCLI do not yet know 
** about them. We have to wait for 2 minutes, that every storcli has reloaded the 
** new configuration before using these new CID/SID.
**
** @param old     The old configuration
** @param new     The new configuration
**
** @retval 1 when new objects exist that requires delay
**         0 when configuration can be changed immediatly
*/
int econfig_does_new_config_requires_delay(econfig_t *old, econfig_t *new) {
  volume_config_t       * vnew, * vold;
  list_t                * vn,   * vo;
  cluster_config_t      * cold, * cnew;
  list_t                * co,   * cn;  
  storage_node_config_t * sold, * snew;
  list_t                * so,   * sn;  
  int                     new_cid;
  int                     new_sid;
  int                     site_num;
  
  /*
  ** Loop on new volumes 
  */
  list_for_each_forward(vn, &new->volumes) {
  
    vnew = (volume_config_t *) list_entry(vn, volume_config_t, list);
    /*
    ** Find out this volume in the old configuration
    */
    list_for_each_forward(vo, &old->volumes) {  

      vold = (volume_config_t *) list_entry(vo, volume_config_t, list);
      if (vold->vid != vnew->vid) continue;
      
      /*
      ** Loop on the new clusters of this new volume
      */
      list_for_each_forward(cn, &vnew->clusters) {
  
        cnew = (cluster_config_t *) list_entry(cn, cluster_config_t, list);
        new_cid = 1;

        /*
        ** Find out this cluster in the old configuration
        */
        list_for_each_forward(co,&vold->clusters) {

          cold = (cluster_config_t *) list_entry(co, cluster_config_t, list);
          if (cold->cid != cnew->cid) continue;
           
          new_cid = 0;
           
          /*
          ** Loop on the new SID of this new cluster of this new volume
          */
          for (site_num = 0; site_num<ROZOFS_GEOREP_MAX_SITE; site_num++) {
          
            list_for_each_forward(sn, &cnew->storages[site_num]) {

              snew = (storage_node_config_t *) list_entry(sn, storage_node_config_t, list);
              new_sid = 1;

              /*
              ** Find out this SID in the old configuration
              */
              list_for_each_forward(so,&cold->storages[site_num]) {

                sold = (storage_node_config_t *) list_entry(so, storage_node_config_t, list);
                if (sold->sid != snew->sid) continue;
                
                new_sid = 0;
                break;
              }
              /*
              ** There is a new SID. We have to wait for the STORCLI to update its configuration
              */
              if (new_sid) return 1;
              //info("%s SID %d/%d",new_sid?"New":"Old",cnew->cid, snew->sid);                                   
            }
          } 
        }        
        /*
        ** There is a new CID. We have to wait for the STORCLI to update its configuration
        */
        if (new_cid) return 1;
        //info("%s cluster %d",new_cid?"New":"Old",cnew->cid);
      }
    }
  }      
  /*
  ** No new cluster no new SID. Configuration can be changed immediatly
  */ 
  return 0;
}
int econfig_read(econfig_t *config, const char *fname) {
    int status = -1;
    //const char *host;
    config_t cfg;
    // Check version of libconfig
#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR >= 4)) \
               || (LIBCONFIG_VER_MAJOR > 1))
    int layout;
    int nodeid;    
#else

    long int layout;
    long int nodeid;
#endif

    DEBUG_FUNCTION;

    config_init(&cfg);

    if (config_read_file(&cfg, fname) == CONFIG_FALSE) {
        errno = EIO;
        severe("can't read %s: %s (line %d).", fname, config_error_text(&cfg),
                config_error_line(&cfg));
        goto out;
    }

    
    if (!config_lookup_int(&cfg, ELAYOUT, &layout)) {
        errno = ENOKEY;
        severe("can't lookup layout setting.");
        goto out;
    }
    config->layout = (uint8_t) layout;

    /*
    ** Is there a defined numa node id to pin the export on
    ** when num aware is requested in rozofs.conf
    */
    config->nodeid = -1;
    if (config_lookup_int(&cfg, ENODEID, &nodeid)) {
       config->nodeid = (uint8_t) nodeid;
    }

#if 0 // not needed since exportgateway code is inactive
    if (!config_lookup_string(&cfg, EVIP, &host)) {
        errno = ENOKEY;
        severe("can't lookup exportd vip setting.");
        goto out;
    }

    // Check length of export hostname
    if (strlen(host) > ROZOFS_HOSTNAME_MAX) {
        errno = ENAMETOOLONG;
        severe(" hostname length  must be lower\
             than %d.", ROZOFS_HOSTNAME_MAX);
        goto out;
    }
    strncpy(config->exportd_vip, host, ROZOFS_HOSTNAME_MAX);
#endif
    
    if (load_volumes_conf(config, &cfg, layout) != 0) {
        severe("can't load volume config.");
        goto out;
    }
    /**< exportd replca -> load the configuration */
    if (load_expgw_conf(config, &cfg) != 0) {
        severe("can't load export expgw config.");
        goto out;
    }
    
    if (load_exports_conf(config, &cfg) != 0) {
        severe("can't load exports config.");
        goto out;
    }

    status = 0;
out:
    config_destroy(&cfg);
    return status;
}

static int econfig_validate_storages(cluster_config_t *config) {
    int status = -1;
    list_t *p, *q;
    DEBUG_FUNCTION;
    int i;
    for (i = 0; i <ROZOFS_GEOREP_MAX_SITE; i++) {
      list_for_each_forward(p, (&config->storages[i])) {
          storage_node_config_t *e1 = list_entry(p, storage_node_config_t, list);

          list_for_each_forward(q, (&config->storages[i])) {
              storage_node_config_t *e2 = list_entry(q, storage_node_config_t, list);
              if (e1 == e2)
                  continue;
              if (e1->sid == e2->sid) {
                  severe("duplicated sid: %d", e1->sid);
                  errno = EINVAL;
                  goto out;
              }
          }
      }
    }

    status = 0;
out:
    return status;
}

/**< expgw exportd */
static int econfig_validate_expgw_node(expgw_config_t *config) {
    int status = -1;
    list_t *p, *q;
    DEBUG_FUNCTION;

    list_for_each_forward(p, &config->expgw_node) {
        expgw_node_config_t *e1 = list_entry(p, expgw_node_config_t, list);

        list_for_each_forward(q, &config->expgw_node) {
            expgw_node_config_t *e2 = list_entry(q, expgw_node_config_t, list);
            if (e1 == e2)
                continue;
            if (e1->gwid == e2->gwid) {
                severe("duplicated gwid: %d", e1->gwid);
                errno = EINVAL;
                goto out;
            }
        }
    }

    status = 0;
out:
    return status;
}
static int econfig_validate_clusters(volume_config_t *config) {
    int status = -1;
    list_t *p, *q;
    DEBUG_FUNCTION;

    list_for_each_forward(p, &config->clusters) {
        cluster_config_t *e1 = list_entry(p, cluster_config_t, list);

        list_for_each_forward(q, &config->clusters) {
            cluster_config_t *e2 = list_entry(q, cluster_config_t, list);
            if (e1 == e2)
                continue;
            if (e1->cid == e2->cid) {
                severe("duplicated cid: %d", e1->cid);
                errno = EINVAL;
                goto out;
            }
        }
        if (econfig_validate_storages(e1) != 0) {
            severe("invalid storage.");
            goto out;
        }
    }

    status = 0;
out:
    return status;
}
/** Checks if the nb. of storages is valid
 *
 * @param config: volume configuration
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
static int econfig_validate_volume_clusters_vs_layout(volume_config_t *config, uint8_t layout) {
  list_t  *q;
  int      size;
  int      safe;

  safe = rozofs_get_rozofs_safe(layout);

  // For each cluster
  list_for_each_forward(q, &config->clusters) {

    cluster_config_t *c = list_entry(q, cluster_config_t, list);

    // Check if the nb. of storages in this cluster is sufficient
    // for this layout
    size = list_size(&c->storages[0]);
    if (size < safe) {
      severe("not enough storages (%d) in cluster %d to use layout %d.",
              size, c->cid, layout);
      errno = EINVAL;
      return -1;
    }

    // Geo replication case
    if (config->georep == 0) continue;


    // Check if the nb. of storages in this cluster is sufficient
    // for this layout
    size = list_size(&c->storages[1]);
    if (size < safe) {
      severe("not enough storages (%d) in cluster %d of site 1 to use layout %d.",
              size, c->cid, layout);
      errno = EINVAL;
      return -1;
    }       
    
  }
  return 0;
}

/** Checks if the nb. of storages is valid
 *
 * @param config: volume configuration
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
static int econfig_validate_storage_nb(volume_config_t *config) {
    int status = -1;
    list_t *q, *r;
    int curr_stor_idx = 0;
    int j = 0;
    int i = 0;
    int exist = 0;

    DEBUG_FUNCTION;

    // Internal structure for store storage node

    typedef struct stor_node_check {
        char host[ROZOFS_HOSTNAME_MAX];
        uint8_t sids_nb;
    } stor_node_check_t;

    stor_node_check_t stor_nodes[STORAGE_NODES_MAX];
    memset(stor_nodes, 0, STORAGE_NODES_MAX * sizeof (stor_node_check_t));

    // Check if the nb. of storages in the clusters is sufficient
    // for the required volume layout
    if (econfig_validate_volume_clusters_vs_layout(config,config->layout) != 0) {
      severe("Volume %d has layout %d",config->vid,config->layout);
      return -1;
    }
    for (j = 0; j <ROZOFS_GEOREP_MAX_SITE; j++) 
    {

     // For each cluster

     list_for_each_forward(q, &config->clusters) {
         cluster_config_t *c = list_entry(q, cluster_config_t, list);


         // For each storage

         list_for_each_forward(r, (&c->storages[j])) {
             storage_node_config_t *s = list_entry(r, storage_node_config_t,
                     list);

             exist = 0;

             // Check if the storage hostname already exists
             for (i = 0; i < curr_stor_idx; i++) {

                 if (strcmp(s->host, stor_nodes[i].host) == 0) {
                     // This physical storage node exist
                     stor_nodes[i].sids_nb++;
                     // Check nb. of storages with this hostname
                     if (stor_nodes[i].sids_nb > STORAGES_MAX_BY_STORAGE_NODE) {
                         severe("Too many storages with the hostname=%s"
                                 " in volume with vid=%u (number max is %d)",
                                 s->host, config->vid,
                                 STORAGES_MAX_BY_STORAGE_NODE);
                         errno = EINVAL;
                         goto out;
                     }
                     exist = 1;
                     break;
                 }
             }

             // This physical storage node doesn't exist
             if (exist == 0) {

                 if ((curr_stor_idx + 1) > STORAGE_NODES_MAX) {
                     severe("Too many storages in volume with vid=%u"
                             " (number max is %d)",
                             config->vid, STORAGE_NODES_MAX);
                     errno = EINVAL;
                     goto out;
                 }
                 // Add this storage node
                 strncpy(stor_nodes[curr_stor_idx].host, s->host,
                         ROZOFS_HOSTNAME_MAX);
                 stor_nodes[curr_stor_idx].sids_nb++;
                 // Increments the nb. of physical storage nodes
                 curr_stor_idx++;
             }
         }
     }
    }
    status = 0;
out:
    return status;
}
/*< expgw exportd  */
static int econfig_validate_expgw(econfig_t *config) {
    int status = -1;
    list_t *p, *q;
    DEBUG_FUNCTION;

    list_for_each_forward(p, &config->expgw) {
        expgw_config_t *e1 = list_entry(p, expgw_config_t, list);

        list_for_each_forward(q, &config->expgw) {
            expgw_config_t *e2 = list_entry(q, expgw_config_t, list);
            if (e1 == e2)
                continue;
            if (e1->daemon_id == e2->daemon_id) {
                severe("duplicated daemon_id: %d", e1->daemon_id);
                errno = EINVAL;
                goto out;
            }
        }
        if (econfig_validate_expgw_node(e1) != 0) {
            severe("invalid expgw node.");
            goto out;
        }
    }

    status = 0;
out:
    return status;
}

static int econfig_validate_volumes(econfig_t *config) {
    int status = -1;
    list_t *p, *q;
    DEBUG_FUNCTION;

    list_for_each_forward(p, &config->volumes) {
        volume_config_t *e1 = list_entry(p, volume_config_t, list);

        if (e1->layout >= LAYOUT_MAX) {
            severe("unknown layout: %d.", e1->layout);
            errno = EINVAL;
            goto out;
        }

        list_for_each_forward(q, &config->volumes) {
            volume_config_t *e2 = list_entry(q, volume_config_t, list);
            if (e1 == e2)
                continue;
            if (e1->vid == e2->vid) {
                severe("duplicated vid: %d", e1->vid);
                errno = EINVAL;
                goto out;
            }
        }

        if (econfig_validate_storage_nb(e1)) {
            goto out;
        }

        if (econfig_validate_clusters(e1) != 0) {
            severe("invalid cluster.");
            goto out;
        }

    }

    status = 0;
out:
    return status;
}

static int econfig_validate_exports(econfig_t *config) {
    int status = -1;
    list_t *p, *q, *r;
    int found = 0;
    volume_config_t *e3
    DEBUG_FUNCTION;

    list_for_each_forward(p, &config->exports) {
        export_config_t *e1 = list_entry(p, export_config_t, list);

        list_for_each_forward(q, &config->exports) {
            export_config_t *e2 = list_entry(q, export_config_t, list);
            if (e1 == e2)
                continue;
            if (e1->eid == e2->eid) {
                severe("duplicated eid: %d", e1->eid);
                errno = EINVAL;
                goto out;
            }
            if (strcmp(e1->root, e2->root) == 0) {
                severe("duplicated root: %s", e1->root);
                errno = EINVAL;
                goto out;
            }
            if (strcmp(e1->name, e2->name) == 0) {
                severe("duplicated name: %s", e1->name);
                errno = EINVAL;
                goto out;
            }            
        }
        found = 0;

        list_for_each_forward(r, &config->volumes) {
            e3 = list_entry(r, volume_config_t, list);
            if (e1->vid == e3->vid) {
                found = 1;
                break;
            }
        }
        if (found != 1) {
            severe("invalid vid for eid: %d", e1->eid);
            errno = EINVAL;
            goto out;
        }
        if (access(e1->root, F_OK) != 0) {
            severe("can't access %s: %s.", e1->root, strerror(errno));
            goto out;
        }
	// When layout is not set in the config file, get the volume layout
	if (e1->layout>LAYOUT_MAX) e1->layout = e3->layout;
	else {
	  // one should check that enough SID are defined in each cluster
	  // to support this layout
          if (econfig_validate_volume_clusters_vs_layout(e3,e1->layout) != 0) {
            severe("Export %d has layout %d on volume %d",e1->eid,e1->layout, e1->vid);
            goto out;
          }
	}
    }

    status = 0;
out:
    return status;
}

int econfig_validate(econfig_t *config) {
    int status = -1;
    DEBUG_FUNCTION;

    if (config->layout < LAYOUT_2_3_4 || config->layout > LAYOUT_8_12_16) {
        severe("unknown layout: %d.", config->layout);
        errno = EINVAL;
        goto out;
    }

    if (econfig_validate_volumes(config) != 0) {
        severe("invalid volume.");
        goto out;
    }

    if (econfig_validate_expgw(config) != 0) {
        severe("invalid expgw.");
        goto out;
    }
    if (econfig_validate_exports(config) != 0) {
        severe("invalid export.");
        goto out;
    }

    status = 0;
out:
    return status;
}

// check whenever we can load to coming from from without breaking
// exportd consistency

int econfig_check_consistency(econfig_t *from, econfig_t *to) {
    DEBUG_FUNCTION;

    if (from->layout != to->layout) {
        severe("inconsistent layout %d vs %d.", from->layout, to->layout);
        return -1;
    }

    //TODO
    return 0;
}

int econfig_print(econfig_t *config) {
    list_t *p;
    int i;
    printf("layout: %d\n", config->layout);
    printf("volume: \n");

    list_for_each_forward(p, &config->volumes) {
        list_t *q;
        volume_config_t *vconfig = list_entry(p, volume_config_t, list);
        printf("vid: %d\n", vconfig->vid);
        printf("layout: %d\n", vconfig->layout);

        list_for_each_forward(q, &vconfig->clusters) {
            list_t *r;
            cluster_config_t *cconfig = list_entry(q, cluster_config_t, list);
            printf("cid: %d\n", cconfig->cid);
            for (i = 0; i <ROZOFS_GEOREP_MAX_SITE; i++) {
              list_for_each_forward(r, (&cconfig->storages[i])) {
                  storage_node_config_t *sconfig = list_entry(r, storage_node_config_t, list);
                  printf("sid: %d\n", sconfig->sid);
                  printf("host: %s\n", sconfig->host);
              }
	    }
        }
    }

    list_for_each_forward(p, &config->exports) {
        export_config_t *e = list_entry(p, export_config_t, list);
        printf("eid: %d, vid:%d,  root: %s, squota:%"PRIu64", hquota: %"PRIu64"\n", e->eid, e->vid, e->root, e->squota, e->hquota);
    }
    return 0;
}
