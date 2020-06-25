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
#ifndef _ROZOFS_BT_TRK_READER_H
#define _ROZOFS_BT_TRK_READER_H
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>          
#include <errno.h>  
#include <rozofs/rozofs.h>
#include <rozofs/common/htable.h>
#include <rozofs/common/list.h>
#include <rozofs/core/ruc_buffer_debug.h>
#include "rozofs_bt_inode.h"

#define ROZOFS_BT_TRK_CLI_READER__MAX_SOCK 8
#define ROZOFS_BT_MAX_CLI_READER_THREADS 1
#define ROZOFS_BT_TRK_CLI_READER_MAX_TRX 32
#define ROZOFS_BT_TRK_CLI_SMALL_XMIT_SIZE 1024
#define ROZOFS_BT_TRK_CLI_LARGE_XMIT_SIZE 1024

#define ROZOFS_BT_TRK_CLI_LARGE_RECV_SIZE ((1024*1024)+8192)
#define ROZOFS_BT_TRK_CLI_SMALL_RECV_SIZE ((1024)



#endif
