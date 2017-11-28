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

#ifndef SCONFIG_H
#define SCONFIG_H

#include <stdio.h>
#include <limits.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/list.h>
#include <rozofs/rpc/mproto.h>
#include "storage.h"
typedef struct _sconfig_devices_t {
    int                     total; 
    int                     mapper;
    int                     redundancy;
} sconfig_devices_t;
 
typedef struct storage_config {
    sid_t sid;
    cid_t cid;
    char root[PATH_MAX];
    sconfig_devices_t       device;  
    /*
    ** String to search for inside spare mark files when looking for a spare device
    ** When null : look for empty "rozofs_spare" file
    ** else      : look for "rozofs_spare" file containing string <spare_mark>"
    */    
    char                  * spare_mark;  
    list_t list;
} storage_config_t;

   
typedef struct sconfig {
    int                     numa_node_id;
    int                     io_addr_nb; 
    struct mp_io_address_t  io_addr[STORAGE_NODE_PORTS_MAX];
    char                  * export_hosts;
    list_t storages;
} sconfig_t;

int sconfig_initialize(sconfig_t *config);

void sconfig_release(sconfig_t *config);

int sconfig_read(sconfig_t *config, const char *fname,int cid);

int sconfig_validate(sconfig_t *config);

extern sconfig_t storaged_config;

#endif
