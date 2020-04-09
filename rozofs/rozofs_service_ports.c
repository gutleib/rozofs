/*
 Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
 This file is part of Rozofs.

 Rozofs is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published
 by the Free Software Foundation; either version 2 of the License,
 or (at your option) any later version.

 Rozofs is distributed in the hope that it will be useful, but
 WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see
 <http://www.gnu.org/licenses/>.
 */


#include "rozofs_service_ports.h"

ROZOFS_SERVICE_PORT_RANGE_DESC_T rozofs_service_port_range[ROZOFS_SERVICE_PORT_MAX] = {
  [ROZOFS_SERVICE_PORT_EXPORT_DIAG] = {
    .defaultValue = 52000,
    .rangeSize    = NB_EXPORT_SLAVE+1,
    .name         = "rozofs_export_diag",
    .service      = "Export master and slave diagnostic",
  },
  
  [ROZOFS_SERVICE_PORT_EXPORT_EPROTO] = {
    .defaultValue = 53000,
    .rangeSize    = NB_EXPORT_SLAVE+1,
    .name         = "rozofs_export_eproto",
    .service      = "Export master and slave eproto",
  },
  
  [ROZOFS_SERVICE_PORT_EXPORT_GEO_REPLICA] = {
    .defaultValue = 53010,
    .rangeSize    = 1,
    .name         = "rozofs_export_geo_replica",
    .service      = "Export master geo-replication",
  },

  [ROZOFS_SERVICE_PORT_EXPORT_RCMD] = {
    .defaultValue = 53030,
    .rangeSize    = 1,
    .name         = "rozofs_export_rcmd",
    .service      = "Export remote command server",
  },

  [ROZOFS_SERVICE_PORT_EXPORT_RCMD_DIAG] = {
    .defaultValue = 53031,
    .rangeSize    = 1,
    .name         = "rozofs_export_rcmd_diag",
    .service      = "Export remote command server diagnostic",
  },
    
  [ROZOFS_SERVICE_PORT_REBALANCE_DIAG] = {
    .defaultValue = 53020,
    .rangeSize    = NB_REBALANCING+1,
    .name         = "rozofs_rebalance",
    .service      = "Rozofs re-balancing diagnostic",
  },
   
  [ROZOFS_SERVICE_PORT_MOUNT_DIAG] = {
    .defaultValue = 50003,
    .rangeSize    = NB_FSMOUNT*3,
    .name         = "rozofs_mount_diag",
    .service      = "rozofsmount & storcli diagnostic",
  },
  
  [ROZOFS_SERVICE_PORT_STORAGED_DIAG] = {
    .defaultValue = 50200,
    .rangeSize    = NB_STORIO+1,
    .name         = "rozofs_storaged_diag",
    .service      = "Storaged & storio diagnostic",
  }, 
  
  [ROZOFS_SERVICE_PORT_STORAGED_MPROTO] = {
    .defaultValue = 51000,
    .rangeSize    = 1,
    .name         = "rozofs_storaged_mproto",
    .service      = "Storaged mproto",
  }, 
  
  [ROZOFS_SERVICE_PORT_GEOMGR_DIAG] = {
    .defaultValue = 54000,
    .rangeSize    = NB_GEOCLI*3+1,
    .name         = "rozofs_geomgr_diag",
    .service      = "Geo-replication manager, clients & storcli diagnostic",
  },     
  
  [ROZOFS_SERVICE_PORT_STSPARE_DIAG] = {
    .defaultValue = 50100,
    .rangeSize    = 1,
    .name         = "rozofs_stspare_diag",
    .service      = "Storaged spare restorer diagnostic port",
  }, 
  [ROZOFS_SERVICE_PORT_ROZODIAG_SRV] = {
    .defaultValue = 50000,
    .rangeSize    = 2,
    .name         = "rozofs_diag_srv",
    .service      = "rozodiag server port",
  }, };
