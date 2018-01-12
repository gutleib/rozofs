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


#ifndef _SCLIENT_H
#define _SCLIENT_H

#include <uuid/uuid.h>

#include <rozofs/rozofs.h>

#include "rpcclt.h"
#include "sproto.h"

typedef struct sclient {
    char host[ROZOFS_HOSTNAME_MAX];
    //sid_t sid;
    uint32_t ipv4;
    uint32_t port;
    int status;
    rpcclt_t rpcclt;
} sclient_t;

int sclient_initialize(sclient_t * clt, struct timeval timeout);

void sclient_release(sclient_t * clt);

int sclient_write(sclient_t * clt, cid_t cid, sid_t sid, uint8_t layout,
        uint8_t spare, sid_t dist_set[ROZOFS_SAFE_MAX], fid_t fid,
        bid_t bid, uint32_t nb_proj, const bin_t * bins);

int sclient_read(sclient_t * clt, cid_t cid, sid_t sid, uint8_t layout,
        uint8_t spare, sid_t dist_set[ROZOFS_SAFE_MAX], fid_t fid,
        bid_t bid, uint32_t nb_proj, bin_t * bins);

int sclient_read_rbs(sclient_t * clt, cid_t cid, sid_t sid, uint8_t layout, uint32_t bsize, uint8_t spare,
        sid_t dist_set[ROZOFS_SAFE_MAX], fid_t fid, bid_t bid,
        uint32_t nb_proj, uint32_t * nb_proj_recv, bin_t * bins);
	
uint32_t sclient_rebuild_start_rbs(sclient_t * clt, cid_t cid, sid_t sid, fid_t fid, 
                                   sp_device_e device, uint8_t chunk, uint8_t spare,
				   uint64_t block_start, uint64_t block_stop);
int sclient_rebuild_stop_rbs(sclient_t * clt, cid_t cid, sid_t sid, fid_t fid, uint32_t ref, sp_status_t status);
int sclient_remove_rbs(sclient_t * clt, cid_t cid, sid_t sid, uint8_t layout, fid_t fid) ;
int sclient_remove_chunk_rbs(sclient_t * clt, cid_t cid, sid_t sid, uint8_t layout, uint8_t spare, uint32_t bsize,
                               sid_t dist_set[ROZOFS_SAFE_MAX], 
			      fid_t fid, int chunk, uint32_t ref);
int sclient_write_rbs(sclient_t * clt, cid_t cid, sid_t sid, uint8_t layout, uint32_t bsize,
        uint8_t spare, sid_t dist_set[ROZOFS_SAFE_MAX], fid_t fid, uint32_t bid,
        uint32_t nb_proj, const bin_t * bins, uint32_t rebuild_ref);
int sclient_clear_error_rbs(sclient_t * clt, cid_t cid, sid_t sid, uint8_t dev,uint8_t reinit);	

/*__________________________________________________________________________________
* Write empty blocks during a rebuild
*
* @param clt          The RPC client context that is being rebuilt
* @param cid          The cluster id of the file to rebuild
* @param sid          The sid on want to rebuild 
* @param layout       The layout of the file to rebuild
* @param bsize        The block size of the file to rebuild
* @param spare        Whether the target sid is spare for the file to rebuild
* @param dist_set     The distribution set of the file to rebuild 
* @param fid          The fid of the file to rebuild
* @param bid          Starting block index to write
* @param nb_proj      Number of empty blocks to write
* @param rebuild_ref  The storio rebuild reference for this FID
*
* @retval -1 on error. 0 on success
*/
int sclient_write_empty_rbs(sclient_t * clt, cid_t cid, sid_t sid, uint8_t layout, uint32_t bsize,
        uint8_t spare, sid_t dist_set[ROZOFS_SAFE_MAX], fid_t fid, uint32_t bid,
        uint32_t nb_proj, 
        uint32_t rebuild_ref);

#endif
