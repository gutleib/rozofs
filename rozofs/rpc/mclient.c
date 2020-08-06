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

#include <string.h>
#include <errno.h>

#include <rozofs/common/log.h>
#include <rozofs/core/rozofs_ip_utilities.h>

#include "mproto.h"
#include "rpcclt.h"
#include "mclient.h"

uint16_t mproto_service_port = 0;

/*_________________________________________________________________
** try to connect a mclient
** 
** @param  clt      The mclient to connect
** @param  timeout  The connection timeout
**
** @retval          0 when connected / -1 when failed
*/
int mclient_connect(mclient_t *clt, struct timeval timeout) {
  int    idx;
  char * pHost;

  clt->status = 0;

  if (mproto_service_port == 0) {
    /* Try to resolve the mproto port from /etc/services */    
    mproto_service_port = rozofs_get_service_port_storaged_mproto();
  }

  for (idx=0; idx < clt->nb_names; idx++) {
  
    pHost = &clt->host[clt->name_idx[idx]];
    if (rpcclt_initialize(&clt->rpcclt, pHost, MONITOR_PROGRAM,
            MONITOR_VERSION, ROZOFS_RPC_BUFFER_SIZE, ROZOFS_RPC_BUFFER_SIZE,
            mproto_service_port, timeout) == 0) {
	clt->status = 1;
	return 0;
    } 
  }
  return -1;
}

int mclient_ports_and_storio_nb(mclient_t * mclt, mp_io_address_t * io_address_p, uint32_t * storio_nb) {
    int status = -1;
    mp_ports_ret_t *ret = 0;
    DEBUG_FUNCTION;
    
    if (!(mclt->rpcclt.client) || !(ret = mp_ports_1(NULL, mclt->rpcclt.client))) {
        errno = EPROTO;
        goto out;
    }
    if (ret->status != 0) {
        errno = ret->mp_ports_ret_t_u.error;
        goto out;
    }
    memcpy(io_address_p, &ret->mp_ports_ret_t_u.ports.io_addr, STORAGE_NODE_PORTS_MAX * sizeof (struct mp_io_address_t));
    
    *storio_nb = 0;
    if (ret->mp_ports_ret_t_u.ports.mode > 1) {
      *storio_nb = ret->mp_ports_ret_t_u.ports.mode;
    }  
    status = 0;
out:
    if (ret)
        xdr_free((xdrproc_t) xdr_mp_ports_ret_t, (char *) ret);
    return status;
}






/*
** Services used by exportd in order to process to the file removal
** and to the storage device availability request
*/






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
int mstoraged_client_remove2(mstorage_client_t * clt, cid_t cid, sid_t sid, fid_t fid,uint8_t spare) {
  int               status = -1;
  mp_status_ret_t * ret    = 0;
  mp_remove2_arg_t  args;

  args.cid   = cid;
  args.sid   = sid;
  args.spare = spare;
  memcpy(args.fid, fid, sizeof (fid_t));

  if (!(clt->rpcclt.client) || !(ret = mp_remove2_1(&args, clt->rpcclt.client))) {
    errno = EPROTO;
    goto out;
  }
  
  if (ret->status != 0) {
    errno = ret->mp_status_ret_t_u.error;
    goto out;
  }
  status = 0;
out:
  if (ret) {
    xdr_free((xdrproc_t) xdr_mp_status_ret_t, (char *) ret);
  }  
  return status;
}


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
int mstoraged_client_get_file_size(mstorage_client_t * clt, cid_t cid, sid_t sid, fid_t fid,uint8_t spare, mp_size_rsp_t *rsp_p) {
  int               status = -1;
  mp_size_ret_t * ret    = 0;
  mp_size_arg_t  args;

  args.cid   = cid;
  args.sid   = sid;
  args.spare = spare;
  memcpy(args.fid, fid, sizeof (fid_t));

  if (!(clt->rpcclt.client) || !(ret = mp_size_1(&args, clt->rpcclt.client))) {
    errno = EPROTO;
    goto out;
  }
  
  if (ret->status != 0) {
    errno = ret->mp_size_ret_t_u.error;
    goto out;
  }
  /*
  ** copy the received data
  */
  memcpy(rsp_p,&ret->mp_size_ret_t_u.rsp,sizeof(mp_size_rsp_t));
  status = 0;
out:
  if (ret) {
    xdr_free((xdrproc_t) xdr_mp_status_ret_t, (char *) ret);
  }  
  return status;
}
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
int mstoraged_client_stat(mstorage_client_t * clt,  cid_t cid, sid_t sid, sstat_t * st) {
  int             status = -1;
  mp_stat_ret_t * ret = 0;
  mp_stat_arg_t   args;

  args.cid = cid;
  args.sid = sid;

  if (!(clt->rpcclt.client) || !(ret = mp_stat_1(&args, clt->rpcclt.client))) {
    errno = EPROTO;
    goto out;
  }
  
  if (ret->status != 0) {
    errno = ret->mp_stat_ret_t_u.error;
    warning("mclient_stat cid:%d sid:%d RozoFS error %s", cid, sid, strerror(errno));	
    memset(st,0, sizeof(sstat_t));
    goto out;
  }
  
  memcpy(st, &ret->mp_stat_ret_t_u.sstat, sizeof (sstat_t));

  status = 0;
out:
  if (ret) {
    xdr_free((xdrproc_t) xdr_mp_stat_ret_t, (char *) ret);
  }  
  return status;
}
