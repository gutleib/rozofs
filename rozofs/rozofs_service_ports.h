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

#ifndef _ROZOFS_SERVICE_PORTS_H
#define _ROZOFS_SERVICE_PORTS_H

#include <stdint.h>
#include <netdb.h>
#include <stdio.h>

#define ROZOFS_DIAGNOSTIC_SRV_PORT   50000

/*
** List of RozoFS service ports
**
*/

#define NB_FSMOUNT   8
#define NB_STORIO  255
#define NB_GEOCLI   30
#define NB_EXPORT_SLAVE 8
#define NB_REBALANCING 4
#define NB_EXPBT 2

typedef enum rozofs_service_port_range_e {

  ROZOFS_SERVICE_PORT_EXPORT_DIAG,
  ROZOFS_SERVICE_PORT_EXPORT_EPROTO, 
  ROZOFS_SERVICE_PORT_EXPORT_GEO_REPLICA,
  ROZOFS_SERVICE_PORT_MOUNT_DIAG,
  ROZOFS_SERVICE_PORT_STORAGED_DIAG,
  ROZOFS_SERVICE_PORT_STORAGED_MPROTO,
  ROZOFS_SERVICE_PORT_GEOMGR_DIAG,
  ROZOFS_SERVICE_PORT_REBALANCE_DIAG,
  ROZOFS_SERVICE_PORT_STSPARE_DIAG,
  ROZOFS_SERVICE_PORT_EXPORT_RCMD,
  ROZOFS_SERVICE_PORT_EXPORT_RCMD_DIAG,
  ROZOFS_SERVICE_PORT_EXPBT,
  ROZOFS_SERVICE_PORT_EXPBT_DIAG,
  ROZOFS_SERVICE_PORT_ROZODIAG_SRV,
  
  ROZOFS_SERVICE_PORT_MAX
  
} ROZOFS_SERVICE_PORT_RANGE_E;

typedef struct rozofs_service_port_range_desc_t {
  uint16_t        defaultValue;   // Default port value when not defined in /etc/services
  uint16_t        rangeSize;      // Number of reserved ports
  char         *  name;           // Name in /etc/services
  char         *  service;        // What is the given service 
} ROZOFS_SERVICE_PORT_RANGE_DESC_T;


extern ROZOFS_SERVICE_PORT_RANGE_DESC_T rozofs_service_port_range[];

/*
**__________________________________________________________________________
*/
/**
   Get the port number reserved for a given service name 
   (having a look at /etc/services)
        
   @param serviceName The name of the service
   @param proto       The protocol of the service ("tcp","udp", NULL)
   @param defaultPort The default port value to return when this service
                      has not been found
		      
   retval: the service port

*/
static inline uint16_t rozofs_get_service_port(int service_nb){
  ROZOFS_SERVICE_PORT_RANGE_DESC_T *p;
  struct servent *servinfo;
  uint16_t        port;
  
  if (service_nb >= ROZOFS_SERVICE_PORT_MAX) return 0;
  p = &rozofs_service_port_range[service_nb];
  
  servinfo = getservbyname(p->name, NULL);
  if(!servinfo) {
     return p->defaultValue;
  }
  
  port = servinfo->s_port;// s_port is an int
  port = ntohs(port); 
  return port;
}  

/*
** Export master ports
*/
static inline uint16_t rozofs_get_service_port_export_master_diag(void) {
  return rozofs_get_service_port(ROZOFS_SERVICE_PORT_EXPORT_DIAG);
}
static inline uint16_t rozofs_get_service_port_export_master_eproto(void) {
  return rozofs_get_service_port(ROZOFS_SERVICE_PORT_EXPORT_EPROTO);
}
static inline uint16_t rozofs_get_service_port_export_master_geo_replica(void) {
  return rozofs_get_service_port(ROZOFS_SERVICE_PORT_EXPORT_GEO_REPLICA);
}
static inline uint16_t rozofs_get_service_port_export_rcmd(void) {
  return rozofs_get_service_port(ROZOFS_SERVICE_PORT_EXPORT_RCMD);
}
static inline uint16_t rozofs_get_service_port_export_rcmd_diag(void) {
  return rozofs_get_service_port(ROZOFS_SERVICE_PORT_EXPORT_RCMD_DIAG);
}
/*
** Export slave ports
*/
static inline uint16_t rozofs_get_service_port_export_slave_diag(int idx) {
  return rozofs_get_service_port_export_master_diag()+idx;
}
static inline uint16_t rozofs_get_service_port_export_slave_eproto(int idx) {
  return rozofs_get_service_port_export_master_eproto()+idx;
}  

/*
** Rozofsmount ports
*/
static inline uint16_t rozofs_get_service_port_fsmount_diag(int idx) {
  return rozofs_get_service_port(ROZOFS_SERVICE_PORT_MOUNT_DIAG) + 3 * idx;
}
static inline uint16_t rozofs_get_service_port_fsmount_storcli_diag(int m,int s) {
  return rozofs_get_service_port(ROZOFS_SERVICE_PORT_MOUNT_DIAG) + 3 * m + s;
}

/*
** Storaged ports
*/
static inline uint16_t rozofs_get_service_port_storaged_diag(void) {
  return rozofs_get_service_port(ROZOFS_SERVICE_PORT_STORAGED_DIAG);
}  
static inline uint16_t rozofs_get_service_port_storaged_mproto(void) {
  return rozofs_get_service_port(ROZOFS_SERVICE_PORT_STORAGED_MPROTO);
}
static inline uint16_t rozofs_get_service_port_stspare_diag(void) {
  return rozofs_get_service_port(ROZOFS_SERVICE_PORT_STSPARE_DIAG);
} 
/*
** Storio ports
*/
static inline uint16_t rozofs_get_service_port_storio_diag(int idx) {
  return rozofs_get_service_port_storaged_diag()+idx;
}
/*
** Geomanager ports
*/
static inline uint16_t rozofs_get_service_port_geomgr_diag(void) {
  return rozofs_get_service_port(ROZOFS_SERVICE_PORT_GEOMGR_DIAG);
} 
/*
** Geocli ports
*/
static inline uint16_t rozofs_get_service_port_geocli_diag(int idx) {
  return rozofs_get_service_port_geomgr_diag()+ 1 + (3*idx);
}
static inline uint16_t rozofs_get_service_port_geocli_storcli_diag(int idx,int s) {
  return rozofs_get_service_port_geomgr_diag()+ 1 + (3*idx) + s;
}

/*
** Re-balancing ports
*/
static inline uint16_t rozofs_get_service_port_rebalancing_diag(int idx) {
  return rozofs_get_service_port(ROZOFS_SERVICE_PORT_REBALANCE_DIAG)+idx;
}

/*
** expbt (file tracking reader) ports debug
*/
static inline uint16_t rozofs_get_service_port_expbt_diag(int idx) {
  return rozofs_get_service_port(ROZOFS_SERVICE_PORT_EXPBT_DIAG)+idx;
}

/*
** expbt (file tracking reader) ports serv vice
*/
static inline uint16_t rozofs_get_service_port_expbt(int idx) {
  return rozofs_get_service_port(ROZOFS_SERVICE_PORT_EXPBT)+idx;
}
/*
**__________________________________________________________________________
*/
/**
   Display a model for /proc/sys/net/ipv4/ip_local_reserved_ports

*/

static inline void show_ip_local_reserved_ports_man(char * pt) {
  pt += sprintf(pt,"Display the list of TCP listening ports that should be reserved for RozoFS.\n");
  pt += sprintf(pt,"1rst column : the base port number in use.\n");
  pt += sprintf(pt,"2nd  column : the number of ports used from the base port number.\n");
  pt += sprintf(pt,"3rd  column : the default base port number.\n");
  pt += sprintf(pt,"4th  column : the service name to use in /etc/services to change the base port number.\n");
  pt += sprintf(pt,"5th  column : the listening port role.\n");  
}
static inline int show_ip_local_reserved_ports(char * buf){
  char * pt = buf;
  uint16_t  port[ROZOFS_SERVICE_PORT_MAX];
  int       idx;
  ROZOFS_SERVICE_PORT_RANGE_DESC_T *p;
  
  pt += sprintf(pt,"_______._____._______.___________________________.____________________________________\n");
  pt += sprintf(pt," %5s | %3s | %5s | %25s | %s\n", "Value","Nb","Const", "/etc/services","Role");
  pt += sprintf(pt,"_______|_____|_______|___________________________|____________________________________\n");
  
  
  p = rozofs_service_port_range;
  for (idx=0; idx < ROZOFS_SERVICE_PORT_MAX; idx++,p++) {
    port[idx] = rozofs_get_service_port(idx);
    pt += sprintf(pt," %5d | %3d | %5d | %25s | %s\n", 
                  port[idx], p->rangeSize, p->defaultValue, p->name, p->service);
  }		 
  pt += sprintf(pt,"_______|_____|_______|___________________________|____________________________________\n");


  pt += sprintf(pt, "\necho net.ipv4.ip_local_reserved_ports=\"");
  p = rozofs_service_port_range;
  for (idx=0; idx < ROZOFS_SERVICE_PORT_MAX; idx++,p++) {
    pt += sprintf(pt, "%d-%d,", port[idx], port[idx]+p->rangeSize-1);
  }  
  pt--;// remove last ','
  pt += sprintf(pt, "\" >> /etc/sysctl.conf\n"); 
  
  pt += sprintf(pt, "\necho \"");
  p = rozofs_service_port_range;
  for (idx=0; idx < ROZOFS_SERVICE_PORT_MAX; idx++,p++) {
    pt += sprintf(pt, "%d-%d,", port[idx], port[idx]+p->rangeSize-1);
  }  
  pt--;// remove last ','
  pt += sprintf(pt, "\" > /proc/sys/net/ipv4/ip_local_reserved_ports\n");   
  return (pt-buf); 
}

#endif
