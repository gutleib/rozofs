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

 
#ifndef STORAGE_SERIALIZATION_H
#define STORAGE_SERIALIZATION_H

#include <limits.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <inttypes.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/common/profile.h>
#include <rozofs/rpc/spproto.h>
#include <rozofs/rpc/sproto.h>
#include <rozofs/core/uma_dbg_api.h>

#include "storage.h"
#include "storaged.h"
#include "sproto_nb.h"
#include "storio_disk_thread_intf.h"
#include "storio_device_mapping.h"


/*_______________________________________________________________________
* Initialize dserialization counters 
*/
void serialization_counters_init(void) ;
/*
**___________________________________________________________
*/
int storio_serialization_begin(storio_device_mapping_t * dev_map_p, rozorpc_srv_ctx_t *req_ctx_p) ;
/*
**___________________________________________________________
*/
void storio_serialization_end(storio_device_mapping_t * dev_map_p, rozorpc_srv_ctx_t *req_ctx_p) ;

/*
**_______________________________________________________
*/

/**
  That function is intended to be used by the main thread
  
  @param p: pointer the FID context that contains the requests lisk
  @param request: pointer to the request to append to the FID context

  @retval 0: FID context already active
  @retval 1: not to post a message to activate the FID context 
*/
int storio_insert_pending_request_list(storio_device_mapping_t *p,list_t *request);

/*
**_______________________________________________________
*/

/**
  That function is intended to be used by the diskthreads
  
  @param p: pointer the FID context that contains the requests lisk
  @param diskthread_list: pointer to the current disk thread list
  @param do_not_clear_running: if asserted serial_is_running is not cleared when both queues are empty

  @retval 1: no more request to process
  @retval 0: the list is not empty
*/
int storio_get_pending_request_list(storio_device_mapping_t *p,list_t *diskthread_list,int do_not_clear_running);

#endif
