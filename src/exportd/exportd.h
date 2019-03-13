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

#include <pthread.h>

#include <rozofs/rozofs.h>
#include <rozofs/rpc/eproto.h>

#include "econfig.h"
#include "export.h"

extern pthread_rwlock_t config_lock;

eid_t *exports_lookup_id(ep_path_t path);

export_t *exports_lookup_export(eid_t eid);

int exports_remove_bins();
extern void dirent_wbcache_flush_on_stop();
