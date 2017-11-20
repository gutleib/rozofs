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
 
 #ifndef EXPTHIN_PROV_API_H
 #define EXPTHIN_PROV_API_H
 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "export.h"

/*
**_________________________________________________
*/
/**
*  Init of the thin provisioning feature

   @param none
   
   @retval 0 on success
   @retval -1 on error
   
*/
int expthin_init();
/*
*_______________________________________________________________________
*/
/**
*  thin provisioning statistics 
*/
void show_thin_prov(char * argv[], uint32_t tcpRef, void *bufRef);

/*
**_______________________________________________________________________
**
** Start every required exportd thin provisioning thread
**
*/
void start_all_expthin_thread() ;
/*
**_______________________________________________________________________
**
** Stop every started  exportd thin provisioning thread
**
*/
void stop_all_expthin_thread();

/*
**__________________________________________________________________
*/
/*
** Check if the file has an entry in the thin provisioning table
 
   That function is intended to be called by lookup, getattr and write_block API
   
   @param lv2_entry_p: pointer to the level 2 cache entry
   @param e: pointer to the export configuration in memory
   @param write_block_flag: assert to one when the file is either extended or a write_block occurs
   @param nb_block_p : pointer to array where the difference in 4KB is reported when there is a change
   @param dir_p: pointer to the location used to indicate in the number of blocks must be added or substracted
   
   These 2 last fields are revelant only when there is a change in the number of blocks.
   
   @retval 0: nothing to write
   @retval 1: there is a change: inode must be written on disk to update the number of blocks
   
*/
int expthin_check_entry(export_t *e,lv2_entry_t *lv2_entry_p,int write_block_flag,uint32_t *nb_block_p,int *dir_p);
/*
**__________________________________________________________________
*/
/*
** remove an entry 
 
   That function is intended to be called when the level2 cache entry is removed (it could be
   because the file is deleted or because of LRU)
   That service is intended to be called for regular files only, useless for all other file types.
   
   @param lv2_entry_p: pointer to the level 2 cache entry

   @retval none
   
*/
int expthin_remove_entry(lv2_entry_t *lv2_entry_p);
 
 #endif
 
