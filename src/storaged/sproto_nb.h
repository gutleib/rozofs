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

 
#ifndef SPROTO_NB_H
#define SPROTO_NB_H

#include <stdint.h>

#include <config.h>
#include <rozofs/rozofs.h>
#include <rozofs/core/rozofs_rpc_non_blocking_generic_srv.h>
#include <rozofs/core/ruc_buffer_api.h>
#include <rozofs/rpc/rozofs_rpc_util.h>
#include <rozofs/common/profile.h>
#include <rozofs/rpc/sproto.h>

void sp_null_1_svc_nb(void *args, rozorpc_srv_ctx_t *req_ctx_p) ;
void sp_write_1_svc_nb(void *args, rozorpc_srv_ctx_t *req_ctx_p) ;
void sp_write_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p);

void sp_read_1_svc_nb(void *args, rozorpc_srv_ctx_t *req_ctx_p) ;
void sp_read_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) ;

void sp_truncate_1_svc_nb(void *args,rozorpc_srv_ctx_t *req_ctx_p);
void sp_truncate_1_svc_disk_thread(void *args,rozorpc_srv_ctx_t *req_ctx_p);
void sp_remove_1_svc_disk_thread(void *args,rozorpc_srv_ctx_t *req_ctx_p);
void sp_rebuild_start_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void sp_rebuild_stop_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void sp_remove_chunk_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void sp_clear_error_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p);

void detailed_counters_init(void) ;
void update_write_detailed_counters(uint64_t delay);
void update_read_detailed_counters(uint64_t delay) ;
void sp_rebuild_stop_response(void * dev_map_p, rozorpc_srv_ctx_t *req_ctx_p);
void serialization_counters_init(void) ;
void sp_write_repair3_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p);

#endif
