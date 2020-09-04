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

#ifndef _STORAGE_ENUMERATION_H
#define _STORAGE_ENUMERATION_H

#include <stdint.h>
#include <limits.h>
#include <uuid/uuid.h>
#include <sys/param.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mount.h>

#include <rozofs/rozofs.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/common/list.h>
#include <rozofs/common/htable.h>
#include <rozofs/common/common_config.h>
#include <rozofs/core/rozofs_string.h>

#include "storage.h"
//#include <rozofs/core/rozofs_share_memory.h>
//#include <rozofs/rpc/sproto.h>





#define ROZOFS_LABEL_SIZE    20
#define ROZOFS_MODEL_SIZE    44

#define ROZOFS_SPARE_LABEL     "RozoSp_"
#define ROZOFS_REGULAR_LABEL   "Rozo_%u_%u_%u"

extern int re_enumration_required;

/*
** Description of a RozoFS dedicated device
*/
typedef struct _storage_device_name_t {
  uint8_t                         mounted:1; // Is it mounted 
  char                            name[32];  // Device name
  uint32_t                        H;
  uint32_t                        C;
  uint32_t                        T;
  uint32_t                        L;
  struct _storage_device_name_t * next;      // Next device name   
} storage_device_name_t;

typedef struct _storage_enumerated_device_t {
  cid_t                   cid;          // CID this device is dedicated to
  sid_t                   sid;          // SID this device is dedicated to
  uint8_t                 dev;          // Device number within the SID
  char                    label[ROZOFS_LABEL_SIZE];    // Label
  char                    model[ROZOFS_MODEL_SIZE];    // Label
  uuid_t                  uuid;         // File system UUID
  time_t                  date;         // Date of the mark file that gave cid/sid/device  
  uint32_t                ext4:1;       // Is it ext4 (else xfs)
  uint32_t                spare:1;      // Is it a spare drive (cid/sid/device are meaningless)
  storage_device_name_t * pName;
} storage_enumerated_device_t;

#endif

