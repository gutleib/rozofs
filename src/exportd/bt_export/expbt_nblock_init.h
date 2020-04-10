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
 
#ifndef EXP_BT_NB_INIT_H
#define EXP_BT_NB_INIT_H
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h> 
#include <sys/socket.h>        
#include <errno.h>  
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/common_config.h>
#include <rozofs/core/ruc_buffer_api.h>
#include <rozofs/core/ruc_list.h>



typedef struct _expbt_start_conf_param_t
{
   uint16_t   debug_port;  /**< debug port for the storaged debug */
   uint16_t   instance;  /**< instance number of the storaged  */
   uint16_t   io_port;   /**< IO port for read/write : 0 for parent process */
   char       *export_path; /**< pathname of the NVMe where metadata data are stored */

} expbt_start_conf_param_t;

extern expbt_start_conf_param_t  expbt_conf_param;
/*
 *_______________________________________________________________________
 */

/**
 *  This function is the entry point for setting rozofs in non-blocking mode

   @param args->ch: reference of the fuse channnel
   @param args->se: reference of the fuse session
   @param args->max_transactions: max number of transactions that can be handled in parallel
   
   @retval -1 on error
   @retval : no retval -> only on fatal error

 */
int expbt_start_nb_th(void *args);

#endif
