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


#ifndef _MCLIENT_H
#define _MCLIENT_H

#include <uuid/uuid.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include "rpcclt.h"
#include "mproto.h"

extern uint16_t mproto_service_port;
#define MAX_MCLIENT_NAME 8

typedef struct mclient {
    /*
    ** Host may contain several addresses initialy separated by '/'
    ** After parsing, '/' are replaced by 0 and name_idx contains 
    ** the list of indexes in host where the differents other names 
    ** start. Example:
    ** host is initialy "192.168.10.1/192.168.11.1/192.168.12.1\0"
    ** after parsing 
    ** host contains "192.168.10.1\0192.168.11.1\0192.168.12.1\0"
    ** nb_names is 3
    ** and name_idx is [0,13,26,...]
    */
    char    host[ROZOFS_HOSTNAME_MAX];
    uint8_t nb_names;
    uint8_t name_idx[MAX_MCLIENT_NAME]; 
    cid_t cid;
    sid_t sid;
    int status;
    rpcclt_t rpcclt;
} mclient_t;

/*_________________________________________________________________
** Initialize a mclient context. 
** The mclient context has already been allocated
** 
** @param  clt    The allocated mclient context
** @param  host   The host names ('/' separated list of addresses)
** @param  cid    The cluster identifier
** @param  sid    The storage identifier within the cluster
**
** @retval        The total number of names found in host (at least 1)
*/
static inline int mclient_new(mclient_t *clt, char * host, cid_t cid, sid_t sid) {
  char * pChar;

  // Reset RPC context
  init_rpcctl_ctx(&clt->rpcclt);

  strcpy(clt->host,host);
  clt->nb_names = 0;
  pChar         = clt->host;
  
  // Skip eventual '/' at the beginning
  while (*pChar == '/') pChar++;
  if (*pChar == 0) {
    severe("Bad mclient name %s",host);
    return 0;
  }
  
  // Register 1rst name
  clt->name_idx[clt->nb_names++] = (pChar - clt->host);
  
  // Loop on searching for extra names
  while (clt->nb_names < MAX_MCLIENT_NAME) {
  
    // end of string parsing
    if (*pChar == 0)   break;
    
    // name separator
    if (*pChar == '/') {
      *pChar = 0;             // Replace separator 
      pChar++;                // Next character should be next name starting
      if (*pChar == 0) break; // This was actually the end of the string !!!
      clt->name_idx[clt->nb_names++] = (pChar - clt->host); // Save starting index of name
    } 
    
    pChar++;
  } 
  
  clt->cid = cid;
  clt->sid = sid;
  return (clt->nb_names);
}

/*_________________________________________________________________
** try to connect a mclient
** 
** @param  clt      The mclient to connect
** @param  timeout  The connection timeout
**
** @retval          0 when connected / -1 when failed
*/
int mclient_connect(mclient_t *clt, struct timeval timeout);


static inline void mclient_release(mclient_t * clt) {
  if (clt && clt->rpcclt.client) rpcclt_release(&clt->rpcclt);
}

int mclient_ports(mclient_t * mclt,  mp_io_address_t * io_address_p);

/*
** New structure to have only one TCP connection toward each storage
** instead of one per SID
*/




/*
** Bitmpa of the SID for a given cluster
*/
typedef uint64_t mstorage_sid_bm[ROZOFS_BITMAP64_NB_UINT64(SID_MAX)];

typedef struct mstorage_client_t {
    /* To chain the storaged clients */
    list_t      list;
    /* Status of the storaged *. 0 unreachable / 1 reachable */
    int         status;
    /* host name comprising '/' */ 
    char        name[ROZOFS_HOSTNAME_MAX];
 
    /*
    ** Host may contain several addresses initialy separated by '/'
    ** After parsing, '/' are replaced by 0 and name_idx contains 
    ** the list of indexes in host where the differents other names 
    ** start. Example:
    ** host is initialy "192.168.10.1/192.168.11.1/192.168.12.1\0"
    ** after parsing 
    ** host contains "192.168.10.1\0192.168.11.1\0192.168.12.1\0"
    ** nb_names is 3
    ** and name_idx is [0,13,26,...]
    */
    char    host[ROZOFS_HOSTNAME_MAX];
    uint8_t nb_names;
    uint8_t name_idx[MAX_MCLIENT_NAME]; 
    
    /*
    ** Each cluster has a bitmap of sid 
    */
    mstorage_sid_bm  bm[ROZOFS_CLUSTERS_MAX];
    
    /*
    ** blocking RPC context
    */
    rpcclt_t rpcclt;
    
} mstorage_client_t;



/*_________________________________________________________________
** Get a mstorage client context to reach storaged at address host
** Either a context already exist in the list 'clients' in which case
** the found context is returned, 
** or no client already exsit toward that destination, and a new one is 
** created and added to the 'clients' list.
** In any case, the sid is set in the convenient cluster bitmap.  
** 
** @param  clients The list a already existing clients toward storaged
** @param  host    The host names ('/' separated list of addresses)
** @param  cid     The cluster identifier
** @param  sid     The storage identifier within the cluster
**
** @retval         The total number of names found in host (at least 1)
*/
static inline mstorage_client_t * mstorage_client_get(list_t * clients, 
                                                      char   * host, 
                                                      cid_t    cid, 
                                                      sid_t    sid) {
  char              * pChar;
  list_t            * p;
  mstorage_client_t * clt;
  struct timeval      timeout;
  int                 idx;

  /*
  ** Check the list of already created mstorage clients,
  ** wheter the required storage exists
  */
  list_for_each_forward(p, clients) {
  
    clt = list_entry(p, mstorage_client_t, list);

    if (strcmp(host, clt->name) != 0) continue;
    
    /*
    ** We have found our guy
    */
    ROZOFS_BITMAP64_SET(sid-1, clt->bm[cid-1]);
    return clt;
  }  

  /*
  ** We have not found the required host.
  ** Allocate one
  */
  clt = xmalloc(sizeof(mstorage_client_t));
  if (clt == NULL) return NULL;

  /* 
  ** Initialize it
  */
  memset(clt,0, sizeof(mstorage_client_t));
  init_rpcctl_ctx(&clt->rpcclt);
  list_init(&clt->list);
  
  /*
  ** Save name and parse host list
  */  
  strcpy(clt->name,host);
  strcpy(clt->host,host);
  clt->nb_names = 0;
  pChar         = clt->host;
  
  // Skip eventual '/' at the beginning
  while (*pChar == '/') pChar++;
  if (*pChar == 0) {
    severe("Bad mclient name %s",host);
    return 0;
  }
  
  // Register 1rst name
  clt->name_idx[clt->nb_names++] = (pChar - clt->host);
  
  // Loop on searching for extra names
  while (clt->nb_names < MAX_MCLIENT_NAME) {
  
    // end of string parsing
    if (*pChar == 0)   break;
    
    // name separator
    if (*pChar == '/') {
      *pChar = 0;             // Replace separator 
      pChar++;                // Next character should be next name starting
      if (*pChar == 0) break; // This was actually the end of the string !!!
      clt->name_idx[clt->nb_names++] = (pChar - clt->host); // Save starting index of name
    } 
    
    pChar++;
  } 
  
  ROZOFS_BITMAP64_SET(sid-1, clt->bm[cid-1]);
  
  /*
  ** Push the new storage in the list
  */
  list_push_back(clients, &clt->list);
  
  /*
  ** Let's connect the new storaged
  */
  timeout.tv_sec  = common_config.mproto_timeout;
  timeout.tv_usec = 0;
            
  if (mproto_service_port == 0) {
    /* Try to resolve the mproto port from /etc/services */    
    mproto_service_port = rozofs_get_service_port_storaged_mproto();
  }

  /*
  ** Loop on every host name
  */
  for (idx=0; idx < clt->nb_names; idx++) {
  
    pChar = &clt->host[clt->name_idx[idx]];
    if (rpcclt_initialize(&clt->rpcclt, pChar, MONITOR_PROGRAM,
            MONITOR_VERSION, ROZOFS_RPC_BUFFER_SIZE, ROZOFS_RPC_BUFFER_SIZE,
            mproto_service_port, timeout) == 0) {
      clt->status = 1;
      break;
    } 
  }    
  return clt;
}
/*
**____________________________________________________________________________
**
** Release a list of storaged connection
**
** @param cnx          The head of the list of mstorage clients 
**
** @retval 0 on success / -1 on error
*/
static inline void mstoraged_release_cnx(list_t *cnx) {
  mstorage_client_t * clt;
  list_t            *p, *q;

  list_for_each_forward_safe(p, q, cnx) {

    clt = list_entry(p, mstorage_client_t, list);

    list_remove(&clt->list);
    if (clt->rpcclt.client) rpcclt_release(&clt->rpcclt);
    xfree(clt);    
  }
}

/*
**____________________________________________________________________________
**
** Retrieve a storaged connection for a given logical storage (cid/sid) in the 
** given list of connections
**
** @param cnx          The head of the list of mstorage clients 
** @param cid          The cluster id of the logical storage
** @param sid          The storage id of the logical storage
**
** @retval 0 on success / -1 on error
*/
static inline mstorage_client_t * mstoraged_lookup_cnx(list_t * cnx, cid_t cid, sid_t sid) {
  list_t             * p;
  mstorage_client_t  * mclt;

  list_for_each_forward(p, cnx) {

    mclt = list_entry(p, mstorage_client_t, list);

    if (ROZOFS_BITMAP64_TEST1(sid-1,mclt->bm[cid-1])) {
      return mclt;
    }
  }
  errno = EINVAL;
  return NULL;
}

/*_________________________________________________________________
** Ask to a logical storage to remove a file 
** 
** @param  clt     The client toward the storaged
** @param  cid     The cluster identifier
** @param  sid     The storage identifier within the cluster
** @param  fid     The FID of the file to remove
**
** @retval         The total number of names found in host (at least 1)
*/
int mstoraged_client_remove2(mstorage_client_t * clt, cid_t cid, sid_t sid, fid_t fid,uint8_t spare);

/*_________________________________________________________________
** Ask to a logical storage its devices statistics
** 
** @param  clt     The client toward the storaged
** @param  cid     The cluster identifier
** @param  sid     The storage identifier within the cluster
** @param  st      The returned stats
**
** @retval         0 on success / -1 on failure
*/
int mstoraged_client_stat(mstorage_client_t * clt,  cid_t cid, sid_t sid, sstat_t * st);

/*_________________________________________________________________
** Ask to a logical storage to get the number of blocks of file 
  The purpose of that service is to address the case of the thin provisioning

  @param  clt     The client toward the storaged
  @param  cid     The cluster identifier
  @param  sid     The storage identifier within the cluster
  @param  fid     The FID of the file  
  @param  rsp_p : pointer to the array with the response is stored

  @retval         0 on sucess
  @retval          -1 on error (see errno for details)
*/
int mstoraged_client_get_file_size(mstorage_client_t * clt, cid_t cid, sid_t sid, fid_t fid,uint8_t spare, mp_size_rsp_t *rsp_p);

#endif
