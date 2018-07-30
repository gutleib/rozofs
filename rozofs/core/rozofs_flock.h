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

#ifndef _ROZOFS_FLOCK_H
#define _ROZOFS_FLOCK_H

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#include <rozofs/common/log.h>
#include <rozofs/rpc/eproto.h>
#include <rozofs/core/rozofs_fid_string.h>

/*
**______________________________________________________________________________
*/
/** Prepare lock information for tracing
 *
 * @param e: the export managing the file or directory.
 * @param fid: the id of the file or directory.
 * @param lock: the lock to set/remove
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */
static inline char * flock_request2string(char * buffer, fid_t fid, ep_lock_t * lock) {
  char * pChar = buffer;

  pChar = rozofs_fid2string(fid,pChar);
  switch(lock->mode) {
    case EP_LOCK_FREE:  pChar += sprintf(pChar," F:"); break;
    case EP_LOCK_READ:  pChar += sprintf(pChar," R:"); break;
    case EP_LOCK_WRITE: pChar += sprintf(pChar," W:"); break;
    default:            pChar += sprintf(pChar," %d:",lock->mode);
  } 
  pChar += sprintf(pChar,"%llx:%llx:%llx:%llx", 
                  (long long unsigned int)lock->client_ref, 
                  (long long unsigned int)lock->owner_ref,
                  (long long unsigned int)lock->user_range.offset_start,
                  (long long unsigned int)lock->user_range.offset_stop);
  return pChar;
}

#endif  
