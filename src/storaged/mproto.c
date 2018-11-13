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

#include <limits.h>
#include <errno.h>

#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/profile.h>
#include <rozofs/common/common_config.h>
#include <rozofs/rpc/spproto.h>
#include <rozofs/rpc/mproto.h>
#include <rozofs/core/rozofs_rpc_non_blocking_generic_srv.h>
#include <rozofs/core/rozofs_share_memory.h>

#include "storage.h"
#include "storaged.h"
#include "sconfig.h"
#include "storaged_sub_thread_intf.h"

#define MAX_STORAGED_CNX_TBL 128     
storage_t * st_per_cnx[MAX_STORAGED_CNX_TBL]={0};

static inline storage_t * get_storage(cid_t cid, sid_t sid, uint32_t cnx_id) {
  storage_t * st;

  /*
  ** Retrieve storage context used for this connection
  */
  if (cnx_id<MAX_STORAGED_CNX_TBL) {
    st = st_per_cnx[cnx_id];
    if ((st!=NULL) 
    &&  (st->cid == cid) 
    &&  (st->sid == sid)) return st;
  }
  
  /*
  ** Lookup for the storaage the request argument
  */
  st = storaged_lookup(cid, sid);
  
  /*
  ** Save the storage in the connection table
  */
  if (cnx_id<MAX_STORAGED_CNX_TBL) {
    st_per_cnx[cnx_id] = st;
  }
  
  return st;
}



DECLARE_PROFILING(spp_profiler_t);

void mp_null_1_svc_nb(void * pt_req, 
                       rozorpc_srv_ctx_t  * rozorpc_srv_ctx_p,
                       void * pt_resp, uint32_t cnx_id) { 
}

void mp_stat_1_svc_nb(void * pt_req, rozorpc_srv_ctx_t *rozorpc_srv_ctx_p,
        void * pt_resp, uint32_t cnx_id) {

    mp_stat_arg_t * args = (mp_stat_arg_t *) pt_req;
    mp_stat_ret_t * ret = (mp_stat_ret_t *) pt_resp;
    storage_t     * st = 0;
    uint64_t        ssize;
    uint64_t        sfree;
    int             device;
    storage_share_t *share;
    
    DEBUG_FUNCTION;

    START_PROFILING(stat);

    ret->status = MP_FAILURE;

    if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
        ret->mp_stat_ret_t_u.error = errno;
        goto out;
    }

    sfree = 0;
    ssize = 0;

    /*
    ** Resolve the share memory address
    */
    share = storage_get_share(st);    
    if (share == NULL) {
      ret->mp_stat_ret_t_u.error = ENOENT;
      goto out;
    }    
    

    for (device=0; device < st->device_number; device++) {
      sfree += share->dev[device].free;
      ssize += share->dev[device].size;
    }  
    
    ret->mp_stat_ret_t_u.sstat.size = ssize;
    ret->mp_stat_ret_t_u.sstat.free = sfree;

    ret->status = MP_SUCCESS;

out:
    STOP_PROFILING(stat);
}

void mp_remove_1_svc_nb(void * pt_req, 
                        rozorpc_srv_ctx_t *rozorpc_srv_ctx_p,
                        void * pt_resp, 
			uint32_t cnx_id) {

    mp_status_ret_t * ret = (mp_status_ret_t *) pt_resp;
    mp_remove_arg_t * args = (mp_remove_arg_t*) pt_req;
    storage_t *st = 0;
    
    DEBUG_FUNCTION;

    START_PROFILING(remove);

    ret->status = MP_FAILURE;

    if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
        ret->mp_status_ret_t_u.error = errno;
        goto out;
    }
    

    if (storage_rm_file(st, (unsigned char *) args->fid) != 0) {
        ret->mp_status_ret_t_u.error = errno;
        goto out;
    }

    ret->status = MP_SUCCESS;

    
out:
    STOP_PROFILING(remove);
}
void mp_remove2_1_svc_nb(void * pt_req, 
                         rozorpc_srv_ctx_t *rozorpc_srv_ctx_p,
			 void * pt_resp, 
			 uint32_t cnx_id) {

    mp_status_ret_t * ret = (mp_status_ret_t *) pt_resp;
    mp_remove2_arg_t * args = (mp_remove2_arg_t*) pt_req;
    storage_t *st = 0;

    DEBUG_FUNCTION;

    START_PROFILING(remove);

    ret->status = MP_FAILURE;

    if ((st = get_storage(args->cid, args->sid, cnx_id)) == 0) {
        ret->mp_status_ret_t_u.error = errno;
        goto out;
    }

    if (storage_rm2_file(st, (unsigned char *) args->fid, args->spare) != 0) {
        ret->mp_status_ret_t_u.error = errno;
        goto out;
    }

    ret->status = MP_SUCCESS;
    
     
out:
    STOP_PROFILING(remove);
}
void mp_subthread_remove2(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    mp_remove2_arg_t        * args = (mp_remove2_arg_t*) pt;
    storage_t               * st = 0;
    static    mp_status_ret_t ret;
    
    START_PROFILING(remove);
    
    /*
    ** Use received buffer for the response
    */
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;


    if ((st = get_storage(args->cid, args->sid, req_ctx_p->socketRef)) == 0) {
      goto error;
    }

    if (storaged_sub_thread_intf_send_req(MP_REMOVE2,req_ctx_p,st,tic)==0) { 
      return;
    }
    

error:    
    ret.status                  = MP_FAILURE;            
    ret.mp_status_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    
    STOP_PROFILING(remove);
}
void mp_subthread_remove(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    mp_remove_arg_t         * args = (mp_remove_arg_t*) pt;
    storage_t               * st = 0;
    static    mp_status_ret_t ret;
    
    START_PROFILING(remove);
    
    /*
    ** Use received buffer for the response
    */
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;


    if ((st = get_storage(args->cid, args->sid, req_ctx_p->socketRef)) == 0) {
      goto error;
    }

    if (storaged_sub_thread_intf_send_req(MP_REMOVE,req_ctx_p,st,tic)==0) { 
      return;
    }
    

error:    
    ret.status                  = MP_FAILURE;            
    ret.mp_status_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);

    STOP_PROFILING(remove);
}
void mp_subthread_list_bins_files(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    mp_list_bins_files_arg_t * args = (mp_list_bins_files_arg_t*) pt;
    storage_t                * st = 0;
    static    mp_status_ret_t ret;

    
    START_PROFILING(list_bins_files);
    
    /*
    ** Use received buffer for the response
    */
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;


    if ((st = get_storage(args->cid, args->sid, req_ctx_p->socketRef)) == 0) {
      goto error;
    }

    if (storaged_sub_thread_intf_send_req(MP_LIST_BINS_FILES,req_ctx_p,st,tic)==0) { 
      return;
    }
    

error:    
    ret.status                  = MP_FAILURE;            
    ret.mp_status_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    
    STOP_PROFILING(list_bins_files);
}
void mp_subthread_size(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    mp_size_arg_t           * args = (mp_size_arg_t*) pt;
    storage_t               * st = 0;
    static    mp_size_ret_t   ret;
    
    START_PROFILING(size);
    
    /*
    ** Use received buffer for the response
    */
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;


    if ((st = get_storage(args->cid, args->sid, req_ctx_p->socketRef)) == 0) {
      goto error;
    }

    if (storaged_sub_thread_intf_send_req(MP_SIZE,req_ctx_p,st,tic)==0) { 
      return;
    }
    

error:    
    ret.status                = MP_FAILURE;            
    ret.mp_size_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    
    STOP_PROFILING(size);
}
void mp_subthread_locate(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    mp_locate_arg_t           * args = (mp_locate_arg_t*) pt;
    storage_t                 * st = 0;
    static    mp_locate_ret_t   ret;
    
    START_PROFILING(locate);
    
    /*
    ** Use received buffer for the response
    */
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;


    if ((st = get_storage(args->cid, args->sid, req_ctx_p->socketRef)) == 0) {
      goto error;
    }

    if (storaged_sub_thread_intf_send_req(MP_LOCATE,req_ctx_p,st,tic)==0) { 
      return;
    }
    

error:    
    ret.status                  = MP_FAILURE;            
    ret.mp_locate_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    
    STOP_PROFILING(locate);
}
void mp_ports_1_svc_nb(void * pt_req, 
                       rozorpc_srv_ctx_t *rozorpc_srv_ctx_p,
                       void * pt_resp, 
		       uint32_t cnx_id) {

    mp_ports_ret_t * ret = (mp_ports_ret_t *) pt_resp;

    START_PROFILING(ports);

    ret->status = MP_SUCCESS;

    memcpy(&ret->mp_ports_ret_t_u.ports.io_addr, storaged_config.io_addr,
            sizeof(storaged_config.io_addr));

    ret->mp_ports_ret_t_u.ports.mode = MP_MULTIPLE;
    STOP_PROFILING(ports);
}

void mp_list_bins_files_1_svc_nb(void * pt_req,
                                 rozorpc_srv_ctx_t *rozorpc_srv_ctx_p, 
				 void * pt_resp, 
				 uint32_t cnx_id) {

    mp_list_bins_files_ret_t * ret = (mp_list_bins_files_ret_t *) pt_resp;
    mp_list_bins_files_arg_t * args = (mp_list_bins_files_arg_t*)  pt_req;

    storage_t *st = 0;

    ret->status = MP_FAILURE;

    START_PROFILING(list_bins_files);

    DEBUG_FUNCTION;

    if ((st = get_storage(args->cid, args->sid, cnx_id)) == 0) {
        ret->mp_list_bins_files_ret_t_u.error = errno;
        goto out;
    }

    // It's necessary
    memset(ret, 0, sizeof(mp_list_bins_files_ret_t));

    if (storage_list_bins_files_to_rebuild(st, args->rebuild_sid,
            &args->device,
            &args->spare,
	    &args->slice,
            &args->cookie,
            (bins_file_rebuild_t **)
            & ret->mp_list_bins_files_ret_t_u.reply.children,
            (uint8_t *) & ret->mp_list_bins_files_ret_t_u.reply.eof) != 0) {
      ret->mp_list_bins_files_ret_t_u.error = errno;
      goto out;
    }

    ret->mp_list_bins_files_ret_t_u.reply.cookie = args->cookie;
    ret->mp_list_bins_files_ret_t_u.reply.spare = args->spare;    
    ret->mp_list_bins_files_ret_t_u.reply.device = args->device;
    ret->mp_list_bins_files_ret_t_u.reply.slice = args->slice;

    ret->status = MP_SUCCESS;

out:
    STOP_PROFILING(list_bins_files);
}
