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
#ifndef ROZOFS_FDSET_H
#define ROZOFS_FDSET_H

#include <rozofs/common/types.h>
#include <sys/select.h>



/*__________________________________________________________________________
**
** Redefine fd_set to handle 4096 sockets instead of 1024
**
*/
#define ROZO_FD_SETSIZE (1024*4)
#undef  FD_SETSIZE
#define FD_SETSIZE ROZO_FD_SETSIZE


#define ROZO_FDSET_LONG  (ROZO_FD_SETSIZE/__NFDBITS)
#undef  FDSET_LONG
#define FDSET_LONG ROZO_FDSET_LONG

typedef struct _rozo_fd_set {
  unsigned long fds_bits[ROZO_FDSET_LONG];
} rozo_fd_set;



/*__________________________________________________________________________
**
** Redefine FD_SET macro for 4096 sockets
**__________________________________________________________________________
*/
static __inline__ void ROZO_FD_SET(unsigned long fd, rozo_fd_set *fdsetp) {
  unsigned long _tmp = fd / __NFDBITS;
  unsigned long _rem = fd % __NFDBITS;
  fdsetp->fds_bits[_tmp] |= (1UL<<_rem);
}
#undef  FD_SET
#define FD_SET ROZO_FD_SET
/*__________________________________________________________________________
**
** Redefine FD_CLR macro for 4096 sockets
**__________________________________________________________________________
*/
static __inline__ void ROZO_FD_CLR(unsigned long fd, rozo_fd_set *fdsetp) {
  unsigned long _tmp = fd / __NFDBITS;
  unsigned long _rem = fd % __NFDBITS;
  fdsetp->fds_bits[_tmp] &= ~(1UL<<_rem);
}
#undef  FD_CLR 
#define FD_CLR ROZO_FD_CLR
/*__________________________________________________________________________
**
** Redefine FD_ISSET macro for 4096 sockets
**__________________________________________________________________________
*/
static __inline__ int ROZO_FD_ISSET(unsigned long fd, rozo_fd_set *fdsetp) { 
  unsigned long _tmp = fd / __NFDBITS;
  unsigned long _rem = fd % __NFDBITS;
  return (fdsetp->fds_bits[_tmp] & (1UL<<_rem)) != 0;
}
#undef  FD_ISSET 
#define FD_ISSET ROZO_FD_ISSET
/*__________________________________________________________________________
**
** Redefine FD_ZERO macro for 4096 sockets
**__________________________________________________________________________
*/
static __inline__ void ROZO_FD_ZERO(rozo_fd_set *fdsetp) {
  unsigned long * p = fdsetp->fds_bits;
  int             i;
  for (i=0; i<ROZO_FDSET_LONG; i++) *p++ = 0; 
}
#undef  FD_ZERO
#define FD_ZERO ROZO_FD_ZERO


#endif
