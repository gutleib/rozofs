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

#ifndef _ECONFIG_H
#define _ECONFIG_H

#include <stdio.h>
#include <limits.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/list.h>
#include <libconfig.h>

#include "rozofs_ip4_flt.h"

#define MD5_LEN  22

typedef struct storage_node_config {
    sid_t sid;
    char host[ROZOFS_HOSTNAME_MAX];
    uint8_t siteNum; 
    uint8_t host_rank;
    list_t list;
} storage_node_config_t;

typedef struct cluster_config {
    cid_t cid;
    list_t storages[ROZOFS_GEOREP_MAX_SITE];
    uint8_t nb_host[ROZOFS_GEOREP_MAX_SITE];
    uint8_t nb_sites;
    list_t list;
} cluster_config_t;

typedef struct volume_config {
    vid_t vid;
    uint8_t layout;    
    uint8_t georep; 
    uint8_t multi_site;   
    list_t clusters;
    char  * rebalance_cfg; // Rebalance configuation file if any
    list_t list;
} volume_config_t;

typedef struct export_config {
    eid_t eid;
    vid_t vid;
    uint8_t layout;
    ROZOFS_BSIZE_E  bsize;
    uint8_t         thin:1;    /* Thin provisionning */
    uint8_t         flockp:1;  /* persistent file locks */
    char root[FILENAME_MAX];
    char name[FILENAME_MAX];
    char md5[MD5_LEN];
    uint64_t squota;
    uint64_t hquota;
    char *   filter_name;  
    list_t list;
} export_config_t;



/**< exportd expgw */

typedef struct expgw_node_config {
    int gwid;
    char host[ROZOFS_HOSTNAME_MAX];
    list_t list;
} expgw_node_config_t;


typedef struct expgw_config {
    int daemon_id;
    list_t expgw_node;
    list_t list;
} expgw_config_t;


typedef struct econfig {
    uint8_t layout; ///< layout used for this exportd
    char   exportd_vip[ROZOFS_HOSTNAME_MAX]; ///< virtual IP address of the exportd
    list_t volumes;
    list_t exports;
    list_t expgw;   /*< exportd gateways */
    list_t filters;
} econfig_t;

typedef struct filter_config {
    char                * name;
    rozofs_ip4_subnet_t * filter_tree;
    list_t                list;
} filter_config_t;


extern econfig_t   exportd_config;
extern econfig_t   exportd_reloaded_config;
extern econfig_t * exportd_config_to_show;

int econfig_initialize(econfig_t *config);

void econfig_release(econfig_t *config);

int econfig_read(econfig_t *config, const char *fname);

int econfig_validate(econfig_t *config);

int econfig_check_consistency(econfig_t *from, econfig_t *to);

int econfig_print(econfig_t *config);

int load_exports_conf_api(econfig_t *ec, struct config_t *config);

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
int econfig_does_new_config_requires_delay(econfig_t *old, econfig_t *new);
/*
**_________________________________________________________________________
**
** Replace old configuration by the new configuration
**
** @param old     The old configuration
** @param new     The new configuration
**
*/
void econfig_move(econfig_t *old, econfig_t *new);

#endif
