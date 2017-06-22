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

#ifndef ROZOFS_RW_LOAD_BALANCING_H
#define ROZOFS_RW_LOAD_BALANCING_H

#include <rozofs/rpc/storcli_lbg_prototypes.h>

#define STCLBG_HASH_SIZE  48
/**
* hash table associated with the load balancing amoung the 
* available storcli associated with the rozofs client
*/
extern ruc_obj_desc_t stclbg_hash_table[];

extern unsigned int stclbg_next_idx; /**< index of the next storcli to use */


void show_stclbg(char * argv[], uint32_t tcpRef, void *bufRef);
/*
*________________________________________________________
*/
/**
  insert a storcli load balancing context in the hash
  table (stclbg_hash_table).
  

  @param p: pointer to the context to insert
  @param storcli_idx: index of the storcli that has been selected
  @param fid : fid of the file 
   
 
  @retval none
*/
void stclbg_hash_table_insert_ctx(rozofs_tx_rw_lbg_t *p, fid_t fid, int storcli_idx);

/*
*________________________________________________________
*/
/**
* That service is intended to be called prior
  sending a read/write or truncate request to a storcli
  The fid is the key to search.
  If there is already a request with the same fid in the
  hash table (stclbg_hash_table), then the function returns
  the reference of the storcli found in the entry that matches.
  
  Otherwise, the service gets the next storlci index to use 
  and then inserts an entry in the hash table.
  
  The entry is removed at the end of the transaction (success or error)
  
  @param fid: fid of the file
  
  @retval local index of the storcli to use
  
*/
int stclbg_storcli_idx_from_fid(fid_t fid);

/*
 **____________________________________________________
 *
 * Set the number of STORCLI
 *
 * @param nb number of expected STORCLI
 *
 * retval -1 in invalid number is given 0 else
 */
int stclbg_set_storcli_number (int nb);

/*
*________________________________________________________
*/
/**
*  Init service
*
  @param none
  @retval none
  
*/
void stclbg_init();



/*
 **____________________________________________________
 */
void rozofs_kill_one_storcli(int instance);
void rozofs_start_one_storcli(int instance);
int stclbg_get_storcli_number (void) ;

#endif
