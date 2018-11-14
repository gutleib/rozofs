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

 
#ifndef MPROTOSVC_H
#define MPROTOSVC_H

#include <stdint.h>

#include <config.h>
#include <rozofs/rozofs.h>
#include <rozofs/core/rozofs_rpc_non_blocking_generic_srv.h>
#include <rozofs/core/ruc_buffer_api.h>
#include <rozofs/rpc/rozofs_rpc_util.h>
#include <rozofs/common/profile.h>
#include <rozofs/rpc/mproto.h>

void mp_null_1_svc_nb(void * pt_req, 
                       rozorpc_srv_ctx_t  * rozorpc_srv_ctx_p,
                       void * pt_resp,
		       uint32_t cnx_id) ;
		       
void mp_stat_1_svc_nb(void * pt_req, 
                      rozorpc_srv_ctx_t *rozorpc_srv_ctx_p, 
		      void * pt_resp,
		       uint32_t cnx_id) ;
		      
void mp_remove_1_svc_nb(void * pt_req, 
                      rozorpc_srv_ctx_t *rozorpc_srv_ctx_p, 
		      void * pt_resp,
		       uint32_t cnx_id) ;
void mp_remove2_1_svc_nb(void * pt_req, 
                      rozorpc_srv_ctx_t *rozorpc_srv_ctx_p, 
		      void * pt_resp,
		       uint32_t cnx_id) ;
		      		      
void mp_ports_1_svc_nb(void * pt_req, 
                       rozorpc_srv_ctx_t *rozorpc_srv_ctx_p, 
		       void * pt_resp,
		       uint32_t cnx_id) ;
		       	      		      
void mp_list_bins_files_1_svc_nb(void * pt_req, 
                              rozorpc_srv_ctx_t *rozorpc_srv_ctx_p, 
	 	              void * pt_resp,
		       uint32_t cnx_id) ;

void mp_subthread_remove2(void * pt, rozorpc_srv_ctx_t *req_ctx_p);

void mp_subthread_remove(void * pt, rozorpc_srv_ctx_t *req_ctx_p);

void mp_subthread_list_bins_files(void * pt, rozorpc_srv_ctx_t *req_ctx_p);			      
void mp_subthread_size(void * pt, rozorpc_srv_ctx_t *req_ctx_p);			      
void mp_subthread_locate(void * pt, rozorpc_srv_ctx_t *req_ctx_p);			      

#endif
