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
#ifndef STORIO_TRC_H
#define STORIO_TRC_H
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <stdint.h>

typedef enum _storio_trc_service_e {
  storio_trc_service_tcp_rd,
  storio_trc_service_local_rd,
  storio_trc_service_rdma_rd,
  storio_trc_service_tcp_wr,
  storio_trc_service_local_wr,
  storio_trc_service_rdma_wr,
  storio_trc_service_tcp_wr_empty,
  storio_trc_service_trunc,
  storio_trc_service_rb_start,
  storio_trc_service_rb_stop,
  storio_trc_service_remove,
  storio_trc_service_rm_chunk,
  storio_trc_service_repair,
  storio_trc_service_rdma_setup,
  storio_trc_service_local_setup,
} storio_trc_service_e;
#include "storio_trc_service_e2String.h"

/*__________________________________________________________________________
** Allocate a trace record for a request
**
** @param service  Service identifier 
** @param cid      CID
** @param sid      SID
** @param fid      FID to trace
** @param bid      Block number
** @param nb_proj  number of blocks
**
** retval 0 on success/-1 on error
*/
uint32_t storio_trc_req(int service,uint16_t cid, uint16_t sid, uuid_t fid,uint64_t bid, uint32_t nb_proj);

/*__________________________________________________________________________
** Allocate a trace record for a response
**
** @param service  Service identifier 
** @param status   1 OK / 0 error
** @param index    index of the corresponding request
**
** retval 0 on success/-1 on error
*/
void storio_trc_rsp(int service, int status, uint32_t size, uint32_t index);

/*__________________________________________________________________________
** Init of the trace service
**
** @param nbRecords  Number of records in the trace buffer 
**
** retval 0 on success/-1 on error
*/
int storio_trc_init() ;
#endif
