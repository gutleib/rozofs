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

 
#ifndef EPROTO_NB_H
#define EPROTO_NB_H

#include <stdint.h>

#include <config.h>
#include <rozofs/rozofs.h>
#include <rozofs/core/rozofs_rpc_non_blocking_generic_srv.h>
#include <rozofs/core/ruc_buffer_api.h>
#include <rozofs/rpc/rozofs_rpc_util.h>
#include <rozofs/common/profile.h>
#include <rozofs/rpc/eproto.h>

void ep_null_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_geo_poll_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_poll_conf_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_conf_storage_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_conf_expgw_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_mount_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_mount_msite_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_list_cluster_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_list_cluster2_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_umount_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_statfs_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_lookup_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_getattr_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_setattr_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_readlink_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_link_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_mknod_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_mkdir_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_unlink_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_rmdir_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_symlink_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_symlink2_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_rename_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_readdir_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_readdir2_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_read_block_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_write_block_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_setxattr_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_getxattr_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_removexattr_1_svc_nb( void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_listxattr_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_set_file_lock_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_clear_client_file_lock_1_svc_nb( void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_clear_owner_file_lock_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_get_file_lock_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_poll_file_lock_1_svc_nb( void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_getxattr_raw_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p);
void ep_poll_owner_lock_1_svc_nb( void * pt, rozorpc_srv_ctx_t *req_ctx_p);

#endif
