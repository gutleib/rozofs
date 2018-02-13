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

#include <sys/param.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <uuid/uuid.h>
#include <errno.h>
#include <string.h>
 
#include <rozofs/common/xmalloc.h>
#include <rozofs/rpc/epproto.h>
#include <rozofs/rpc/export_profiler.h>
#include "cache.h"
#include <rozofs/core/af_unix_socket_generic.h>
#include "exportd.h"

/*
**___________________________FILE LOCK SERVICE_____________________________
*/

typedef struct _file_lock_stat_t {
  uint64_t    nb_file_lock;
  uint64_t    nb_client_file_lock;
  uint64_t    nb_lock_unlink;
  uint64_t    nb_lock_allocate;
  uint64_t    nb_remove_client;
  uint64_t    nb_add_client;
} file_lock_stat_t;

static file_lock_stat_t file_lock_stat;
/*
** List of the client owning a lock 
*/
static list_t  file_lock_client_list[EXPGW_EID_MAX_IDX];

/*
** Context of a client
*/
typedef struct _rozofs_file_lock_client_t {
  uint64_t         client_ref;         /**< reference of the client */
  ep_client_info_t info;            /**< client software version */
  uint64_t         last_poll_time;     /**< time stamp of the last poll received */
  uint64_t         nb_lock;            /**< Number of lock owned by this client */
  list_t           next_client;        /**< Link to the next client in the list of clients */
  list_t           file_lock_list;     /**< List of the lock owned by this client */
} rozofs_file_lock_client_t;

/*
*___________________________________________________________________
* Display file lock statistics
*___________________________________________________________________
*/
#define DISPLAY_LOCK_STAT(name) pChar += sprintf(pChar, "  %-20s = %llu\n", #name, (long long unsigned int) file_lock_stat.name); 
char * display_file_lock(char * pChar) {  
  pChar += sprintf(pChar,"\nFile lock statistics:\n");
  DISPLAY_LOCK_STAT(nb_file_lock);
  DISPLAY_LOCK_STAT(nb_client_file_lock);
  DISPLAY_LOCK_STAT(nb_lock_allocate);
  DISPLAY_LOCK_STAT(nb_lock_unlink);
  DISPLAY_LOCK_STAT(nb_add_client);  
  DISPLAY_LOCK_STAT(nb_remove_client);
  return pChar;
}

/*
*___________________________________________________________________
* Recompute the effective range of the lock from the user range
*___________________________________________________________________
*/
void compute_effective_lock_range(uint8_t bsize, struct ep_lock_t * lock) {  
  uint32_t bbytes = ROZOFS_BSIZE_BYTES(bsize);

  
  if (lock->user_range.size == EP_LOCK_TOTAL) {
    lock->effective_range.offset_start = 0;  
    lock->effective_range.offset_stop = 0;
   lock->effective_range.size = EP_LOCK_TOTAL;   
  }
    
  lock->effective_range.offset_start = lock->user_range.offset_start / bbytes;
  
  if (lock->user_range.size == EP_LOCK_TO_END) {
    lock->effective_range.offset_stop = 0;
    if (lock->effective_range.offset_start == 0) lock->effective_range.size = EP_LOCK_TOTAL;   
    else                                         lock->effective_range.size = EP_LOCK_TO_END;
    return;   
  }
  

  if (lock->effective_range.offset_stop % bbytes == 0) {
    lock->effective_range.offset_stop = lock->user_range.offset_stop / bbytes;
  }
  else {
    lock->effective_range.offset_stop = lock->user_range.offset_stop / bbytes + 1;  
  }   

  if (lock->effective_range.offset_start == 0) {
    lock->effective_range.size = EP_LOCK_FROM_START;
  }
  else {
    lock->effective_range.size = EP_LOCK_PARTIAL;  
  }  
}
/*
*___________________________________________________________________
* Check whether two locks are compatible in oreder to set a new one.
* We have to check the effective range and not the user range
*
* @param lock1   1rst lock
* @param lock2   2nd lock
* 
* @retval 1 when locks are compatible, 0 else
*___________________________________________________________________
*/
int are_file_locks_compatible(struct ep_lock_t * lock1, struct ep_lock_t * lock2) {
  int key;
  ep_lock_range_t * p1, * p2;  

  if ((lock1->mode == EP_LOCK_READ)&&(lock2->mode == EP_LOCK_READ)) return 1;

  p1 = &lock1->effective_range;
  p2 = &lock2->effective_range;

  /*
  ** If one of the 2 locks is a write, it must overlap with the other one
  */
  
  key = p1->size << 8 | p2->size;
  switch(key) {
  

      return -1;
    
    case (EP_LOCK_FROM_START<<8|EP_LOCK_TO_END): 
    case (EP_LOCK_FROM_START<<8|EP_LOCK_PARTIAL): 
    case (EP_LOCK_PARTIAL<<8|EP_LOCK_TO_END):
      if (p1->offset_stop <= p2->offset_start) return 1;
      return 0;  
      
    
    case (EP_LOCK_TO_END<<8|EP_LOCK_FROM_START): 
    case (EP_LOCK_TO_END<<8|EP_LOCK_PARTIAL):   
    case (EP_LOCK_PARTIAL<<8|EP_LOCK_FROM_START):
      if (p1->offset_start >= p2->offset_stop) return 1;
      return 0;

    case (EP_LOCK_PARTIAL<<8|EP_LOCK_PARTIAL):    
      if (p1->offset_start <= p2->offset_start) {
	if (p1->offset_stop <= p2->offset_start) return 1;
	return 0;
      }
      if (p2->offset_stop <= p1->offset_start) return 1;
      return 0;
             
                
    default:
    //case (EP_LOCK_TOTAL<<8|EP_LOCK_TOTAL):
    //case (EP_LOCK_TOTAL<<8|EP_LOCK_FROM_START):
    //case (EP_LOCK_TOTAL<<8|EP_LOCK_TO_END):   
    //case (EP_LOCK_TOTAL<<8|EP_LOCK_PARTIAL): 
           
    //case (EP_LOCK_FROM_START<<8|EP_LOCK_TOTAL):
    //case (EP_LOCK_FROM_START<<8|EP_LOCK_FROM_START):
    
    //case (EP_LOCK_TO_END<<8|EP_LOCK_TOTAL):
    //case (EP_LOCK_TO_END<<8|EP_LOCK_TO_END):
    
    //case (EP_LOCK_PARTIAL<<8|EP_LOCK_TOTAL):    
      return 0;   
  }   
}
/*
*___________________________________________________________________
* Check whether two locks are overlapping. This has to be check at user 
* level in order to merge the different requested locks into one.
*
* @param lock1   1rst lock
* @param lock2   2nd lock
*
* @retval 1 when locks overlap, 0 else
*___________________________________________________________________
*/
int are_file_locks_overlapping(struct ep_lock_t * lock1, struct ep_lock_t * lock2) {
  int key;
  ep_lock_range_t * p1, * p2;
    
  p1 = &lock1->user_range;
  p2 = &lock2->user_range;  
  
  key = p1->size << 8 | p2->size;
  switch(key) {

      return 1;
    
    case (EP_LOCK_FROM_START<<8|EP_LOCK_TO_END): 
    case (EP_LOCK_FROM_START<<8|EP_LOCK_PARTIAL): 
    case (EP_LOCK_PARTIAL<<8|EP_LOCK_TO_END):
      if (p1->offset_stop < p2->offset_start) return 0;
      return 1;

    case (EP_LOCK_TO_END<<8|EP_LOCK_FROM_START): 
    case (EP_LOCK_TO_END<<8|EP_LOCK_PARTIAL):   
    case (EP_LOCK_PARTIAL<<8|EP_LOCK_FROM_START):
      if (p1->offset_start > p2->offset_stop) return 0;
      return 1;        

    case (EP_LOCK_PARTIAL<<8|EP_LOCK_PARTIAL):    
      if (p1->offset_start <= p2->offset_start) {
	if (p1->offset_stop < p2->offset_start) return 0;
	return 1;
      }
      if (p2->offset_stop < p1->offset_start) return 0;
      return 1;
             
                
    default:
    //case (EP_LOCK_FROM_START<<8|EP_LOCK_FROM_START):
    //case (EP_LOCK_TO_END<<8|EP_LOCK_TO_END):  
    //case (EP_LOCK_TOTAL<<8|EP_LOCK_TOTAL):  
    //case (EP_LOCK_TOTAL<<8|EP_LOCK_TO_END):  
    //case (EP_LOCK_TOTAL<<8|EP_LOCK_FROM_START):  
    //case (EP_LOCK_TOTAL<<8|EP_LOCK_PARTIAL):  
    //case (EP_LOCK_TOTAL<<8|EP_LOCK_TOTAL):  
    //case (EP_LOCK_TO_END<<8|EP_LOCK_TOTAL):  
    //case (EP_LOCK_FROM_START<<8|EP_LOCK_TOTAL):  
    //case (EP_LOCK_PARTIAL<<8|EP_LOCK_TOTAL):           
      return 1;   
  }   
}
/*
*___________________________________________________________________
* Try to concatenate overlapping locks in lock1. This has to be done
* at user level in order to merge the different requested locks into one.
*
* @param bsize   The blok size as defined in ROZOFS_BSIZE_E
* @param lock1   1rst lock
* @param lock2   2nd lock
*
* @retval 1 when locks overlap, 0 else
*___________________________________________________________________
*/
#define max(a,b) (a>b?a:b)
#define min(a,b) (a>b?b:a)
int try_file_locks_concatenate(uint8_t bsize, struct ep_lock_t * lock1, struct ep_lock_t * lock2) {
  int key;
  ep_lock_range_t * p1, * p2;
  
  p1 = &lock1->user_range;
  p2 = &lock2->user_range;  
  
  key = p1->size << 8 | p2->size;
  switch(key) {
    
    case (EP_LOCK_FROM_START<<8|EP_LOCK_TO_END): 
      if (p1->offset_stop < p2->offset_start) return 0;
      p1->size = EP_LOCK_TOTAL;
      p1->offset_stop = p2->offset_stop;
      compute_effective_lock_range(bsize,lock1);
      return 1;      
      
    case (EP_LOCK_FROM_START<<8|EP_LOCK_PARTIAL): 
      if (p1->offset_stop < p2->offset_start) return 0;
      p1->offset_stop = max(p2->offset_stop,p1->offset_stop);
      compute_effective_lock_range(bsize,lock1);
      return 1;      
      
    case (EP_LOCK_PARTIAL<<8|EP_LOCK_TO_END):
      if (p1->offset_stop < p2->offset_start) return 0;
      p1->size = EP_LOCK_TO_END;
      p1->offset_start = min(p2->offset_start,p1->offset_start);      
      p1->offset_stop = p2->offset_stop;
      compute_effective_lock_range(bsize,lock1);
      return 1;      

    case (EP_LOCK_TO_END<<8|EP_LOCK_FROM_START): 
      if (p1->offset_start > p2->offset_stop) return 0;
      p1->size = EP_LOCK_TOTAL;
      p1->offset_start = p2->offset_start;
      compute_effective_lock_range(bsize,lock1);
      return 1;      

    case (EP_LOCK_TO_END<<8|EP_LOCK_PARTIAL): 
      if (p1->offset_start > p2->offset_stop) return 0;
      p1->offset_start = min(p2->offset_start,p1->offset_start);      
      compute_effective_lock_range(bsize,lock1);
      return 1;      
            
    case (EP_LOCK_PARTIAL<<8|EP_LOCK_FROM_START):
      if (p1->offset_start > p2->offset_stop) return 0;
      p1->size = EP_LOCK_FROM_START;
      p1->offset_stop = max(p2->offset_stop,p1->offset_stop);
      p1->offset_start = p2->offset_start;  
      compute_effective_lock_range(bsize,lock1);
      return 1;      

    case (EP_LOCK_PARTIAL<<8|EP_LOCK_PARTIAL):    
      if (p1->offset_start <= p2->offset_start) {
	if (p1->offset_stop < p2->offset_start) return 0;
        p1->offset_stop = max(p2->offset_stop,p1->offset_stop);  
        compute_effective_lock_range(bsize,lock1);
        return 1;      
      }
      if (p2->offset_stop < p1->offset_start) return 0;
      p1->offset_start = p2->offset_start;      
      p1->offset_stop = max(p2->offset_stop,p1->offset_stop);  
      compute_effective_lock_range(bsize,lock1);
      return 1;      
      
    case (EP_LOCK_FROM_START<<8|EP_LOCK_FROM_START):             
      p1->offset_stop = max(p2->offset_stop,p1->offset_stop);    
      compute_effective_lock_range(bsize,lock1);
      return 1;      
             
    case (EP_LOCK_TO_END<<8|EP_LOCK_TO_END):  
      p1->offset_start = min(p2->offset_start,p1->offset_start); 
      compute_effective_lock_range(bsize,lock1);
      return 1;      
      
    case (EP_LOCK_TOTAL<<8|EP_LOCK_TOTAL): 
    case (EP_LOCK_TOTAL<<8|EP_LOCK_TO_END): 
    case (EP_LOCK_TOTAL<<8|EP_LOCK_FROM_START):  
    case (EP_LOCK_TOTAL<<8|EP_LOCK_PARTIAL):    
      return 1;
             	              
    case (EP_LOCK_TO_END<<8|EP_LOCK_TOTAL):  
    case (EP_LOCK_FROM_START<<8|EP_LOCK_TOTAL):  
    case (EP_LOCK_PARTIAL<<8|EP_LOCK_TOTAL):  
      p1->size = EP_LOCK_TOTAL;
      p1->offset_start = p2->offset_start;      
      p1->offset_stop  = p2->offset_stop;                     
      compute_effective_lock_range(bsize,lock1);
      return 1;      
  }   
  return 0;
}
/*
*___________________________________________________________________
* Check whether two lock2 must :free or update lock1
*
* @param bsize       The blok size as defined in ROZOFS_BSIZE_E
* @param lock_free   The free lock operation
* @param lock_set    The set lock that must be checked
*
* @retval 1 when locks are compatible, 0 else
*___________________________________________________________________
*/
int must_file_lock_be_removed(uint32_t eid, uint8_t bsize, struct ep_lock_t * lock_free, struct ep_lock_t * lock_set, rozofs_file_lock_t ** new_lock_ctx,ep_client_info_t * info) {
  int       key;
  ep_lock_t new_lock;
  ep_lock_range_t * pfree, * plock;
    
  *new_lock_ctx = NULL;

  if (lock_free->client_ref != lock_set->client_ref) return 0;
  if (lock_free->owner_ref != lock_set->owner_ref)   return 0;  

  pfree = &lock_free->user_range;
  plock = &lock_set->user_range;  
    
  key = pfree->size << 8 | plock->size;
  switch(key) {  
    case (EP_LOCK_TOTAL<<8|EP_LOCK_TOTAL):
    case (EP_LOCK_TOTAL<<8|EP_LOCK_FROM_START):
    case (EP_LOCK_TOTAL<<8|EP_LOCK_TO_END):   
    case (EP_LOCK_TOTAL<<8|EP_LOCK_PARTIAL):
      return 1;

    //   FREE #_______...........# 
    //   LOCK #__________________#   
    //        #.......___________#
    case (EP_LOCK_FROM_START<<8|EP_LOCK_TOTAL):
      plock->offset_start = pfree->offset_stop; 
      plock->size = EP_LOCK_TO_END;
      compute_effective_lock_range(bsize,lock_set);
      return 0;

    //   FREE #_______...........#    FREE #_______...........# 
    //   LOCK #__________........#    LOCK #_____.............#   
    //        #.......___........#         #..................#
    case (EP_LOCK_FROM_START<<8|EP_LOCK_FROM_START):
      if (pfree->offset_stop >= plock->offset_stop) return 1;
      plock->offset_start = pfree->offset_stop; 
      plock->size = EP_LOCK_PARTIAL;  
      compute_effective_lock_range(bsize,lock_set);      
      return 0;    

    //   FREE #_______...........#    FREE #_______...........# 
    //   LOCK #....______________#    LOCK #........._________#  
    //        #.......___________#         #........._________#
    case (EP_LOCK_FROM_START<<8|EP_LOCK_TO_END): 
      if (pfree->offset_stop <= plock->offset_start) return 0;
      plock->offset_start = pfree->offset_stop; 
      compute_effective_lock_range(bsize,lock_set);
      return 0;
      
    //   FREE #_______...........#    FREE #_______...........#  FREE #_______...........# 
    //   LOCK #..__..............#    LOCK #....______........#  LOCK #.........______...#  
    //        #..................#         #.......___........#       #.........______...#
    case (EP_LOCK_FROM_START<<8|EP_LOCK_PARTIAL): 
      if (pfree->offset_stop <= plock->offset_start) return 0;
      if (plock->offset_stop <= pfree->offset_stop) return 1;
      plock->offset_start = pfree->offset_stop; 
      compute_effective_lock_range(bsize,lock_set);
      return 0;

    //   FREE #..........._______# 
    //   LOCK #__________________#   
    //        #___________.......#            
    case (EP_LOCK_TO_END<<8|EP_LOCK_TOTAL):  
      plock->offset_stop = pfree->offset_start; 
      plock->size = EP_LOCK_FROM_START;
      compute_effective_lock_range(bsize,lock_set);      
      return 0;    

    //   FREE #..........._______#    FREE #..........._______# 
    //   LOCK #_______________...#    LOCK #________..........#
    //        #___________.......#         #________..........#
    case (EP_LOCK_TO_END<<8|EP_LOCK_FROM_START): 
      if (pfree->offset_start >= plock->offset_stop) return 0;
      plock->offset_stop = pfree->offset_start; 
      compute_effective_lock_range(bsize,lock_set);      
      return 0;
      
    //   FREE #..........._______#    FREE #.......___________# 
    //   LOCK #....______________#    LOCK #........._________#  
    //        #...._______.......#         #..................#
    case (EP_LOCK_TO_END<<8|EP_LOCK_TO_END):
      if (pfree->offset_start <= plock->offset_start) return 1;
      plock->offset_stop = pfree->offset_start; 
      plock->size = EP_LOCK_PARTIAL;
      compute_effective_lock_range(bsize,lock_set);      
      return 0;
      
    //   FREE #..........._______#    FREE #.......___________#  FREE #.......___________# 
    //   LOCK #..__..............#    LOCK #....______........#  LOCK #.........______...#  
    //   LOCK #..__..............#    LOCK #....___...........#  LOCK #..................#  
    case (EP_LOCK_TO_END<<8|EP_LOCK_PARTIAL):   
      if (plock->offset_stop <= pfree->offset_start) return 0;
      if (plock->offset_start >= pfree->offset_start) return 1;
      plock->offset_stop = pfree->offset_start;
      compute_effective_lock_range(bsize,lock_set);      
      return 0;      

    //   FREE #.........____.....#    
    //   LOCK #__________________#   
    //        #_________...._____#   
    case (EP_LOCK_PARTIAL<<8|EP_LOCK_TOTAL):
      memcpy(&new_lock,lock_set, sizeof(new_lock));
      new_lock.user_range.size = EP_LOCK_FROM_START;
      new_lock.user_range.offset_stop = pfree->offset_start;
      compute_effective_lock_range(bsize,&new_lock);
      *new_lock_ctx = lv2_cache_allocate_file_lock(eid, &new_lock,info); 
      plock->offset_start = pfree->offset_stop; 
      plock->size = EP_LOCK_TO_END;
      compute_effective_lock_range(bsize,lock_set);    
      return 0;

      
    //   FREE #.........____.....#    FREE #..____............#    FREE #...._______.......# 
    //   LOCK #....______________#    LOCK #........._________#    LOCK #........._________#  
    //        #...._____...._____#         #........._________#         #..........._______#
    case (EP_LOCK_PARTIAL<<8|EP_LOCK_TO_END):
      if (pfree->offset_stop <= plock->offset_start) return 0;
      if (pfree->offset_start > plock->offset_start) {
	memcpy(&new_lock,lock_set, sizeof(new_lock));
	new_lock.user_range.size = EP_LOCK_PARTIAL;
	new_lock.user_range.offset_stop = pfree->offset_start;
        compute_effective_lock_range(bsize,&new_lock);
	*new_lock_ctx = lv2_cache_allocate_file_lock(eid, &new_lock,info); 
      }
      plock->offset_start = pfree->offset_stop;       
      compute_effective_lock_range(bsize,lock_set);    
      return 0;  

    //   FREE #...____...........#    FREE #........____......#    FREE #......_______.....# 
    //   LOCK #__________........#    LOCK #_____.............#    LOCK #__________........# 
    case (EP_LOCK_PARTIAL<<8|EP_LOCK_FROM_START):
      if (pfree->offset_start >= plock->offset_stop) return 0;
      if (plock->offset_stop <= pfree->offset_stop) {
        plock->offset_stop = pfree->offset_start;
        compute_effective_lock_range(bsize,lock_set);    	
	return 0;
      }
      memcpy(&new_lock,lock_set, sizeof(new_lock));
      new_lock.user_range.size = EP_LOCK_PARTIAL;
      new_lock.user_range.offset_start = pfree->offset_stop;
      compute_effective_lock_range(bsize,&new_lock);
      *new_lock_ctx = lv2_cache_allocate_file_lock(eid,&new_lock,info);
      plock->offset_stop = pfree->offset_start;
      compute_effective_lock_range(bsize,lock_set);    
      return 0;    

    //   FREE #.......___..#    FREE #..____.........#    FREE #..._____.....#  FREE #.....______..#
    //   LOCK #..__........#    LOCK #.........____..#    LOCK #.....______..#  LOCK #..._____.....#
    case (EP_LOCK_PARTIAL<<8|EP_LOCK_PARTIAL):    
      if (pfree->offset_start >= plock->offset_stop) return 0;
      if (plock->offset_start >= pfree->offset_stop) return 0;      
      if (pfree->offset_start <= plock->offset_start) {
        if (pfree->offset_stop >= plock->offset_stop) return 1;
	plock->offset_start = pfree->offset_stop;
        compute_effective_lock_range(bsize,lock_set);    
	return 0;
      }
      if (pfree->offset_stop >= plock->offset_stop) {
	plock->offset_stop = pfree->offset_start;
        compute_effective_lock_range(bsize,lock_set);    
	return 0;
      }      
      memcpy(&new_lock,lock_set, sizeof(new_lock));
      new_lock.user_range.offset_stop = pfree->offset_start;
      compute_effective_lock_range(bsize,&new_lock);
      *new_lock_ctx = lv2_cache_allocate_file_lock(eid,&new_lock,info);
      plock->offset_start = pfree->offset_stop;
      return 0;  
             
    default:
      return 0;   
  }   
}
/*
*___________________________________________________________________
* initialize the lock service
*
*___________________________________________________________________
*/
void file_lock_service_init(void) {
  int idx;
  
  memset(&file_lock_stat,0, sizeof(file_lock_stat));
  for (idx=0; idx<EXPGW_EID_MAX_IDX; idx++) {
    list_init(&file_lock_client_list[idx]);
  }  
}
/*
*___________________________________________________________________
* Unlink a lock. This consist in downgrading the global number of lock 
* and unlinking the lock from every list it is in
*
* @param lock The lock to unlink
*
*___________________________________________________________________
*/
static inline void file_lock_unlink(rozofs_file_lock_t * lock) {

  file_lock_stat.nb_lock_unlink++;
  file_lock_stat.nb_file_lock--;
  
  /* Unlink lock from the FID */  
  list_remove(&lock->next_fid_lock);
  /* Unlink lock from the client */  
  list_remove(&lock->next_client_lock);
  
}
/*
*___________________________________________________________________
* Display file lock clients
*___________________________________________________________________
*/ 
char * display_file_lock_clients(char * pChar) {  
  list_t                    * p;
  rozofs_file_lock_client_t * client;
  uint64_t                    now;
  uint32_t                    ipClient;
  char                        orig[32];
  int                         idx;
  
  now = time(0);
  
  for (idx=0; idx<EXPGW_EID_MAX_IDX; idx++) {

    if (list_empty(&file_lock_client_list[idx])) {
      continue;
    }  
  
    pChar += sprintf(pChar,"client polling time out : %d sec\n", FILE_LOCK_POLL_DELAY_MAX);
    pChar += sprintf(pChar, "+------------------+---------+-------+-------------------------+------------------------------\n");
    pChar += sprintf(pChar, "| client ref       | poll(s) | #lock | client diagnostic srv   | version\n");  
    pChar += sprintf(pChar, "+------------------+---------+-------+-------------------------+------------------------------\n");  
    /* Loop on the clients */
    list_for_each_forward(p, &file_lock_client_list[idx]) {

      client = list_entry(p, rozofs_file_lock_client_t, next_client);

      ipClient = af_unix_get_remote_ip(client->info.socketRef);

      sprintf(orig,"%u.%u.%u.%u:%u", 
             (ipClient>>24)&0xFF,
	     (ipClient>>16)&0xFF,
	     (ipClient>>8)&0xFF,
	     ipClient&0xFF,
	     client->info.diag_port);

      pChar += sprintf(pChar, "| %16llx | %7d | %5d | %-23s | %s\n",
                       (long long unsigned int)client->client_ref, 
		       (int) (now-client->last_poll_time),
		       (int)client->nb_lock,
		       orig,
		       client->info.vers);  
    } 
    pChar += sprintf(pChar, "+------------------+---------+-------+-------------------------+------------------------------\n");  
  }
  return pChar;
}
/*
*___________________________________________________________________
* Remove all the locks of a client and then remove the client 
*
* @param client_ref reference of the client to remove
*___________________________________________________________________
*/
static inline void file_lock_remove_one_client(rozofs_file_lock_client_t * client) {
  rozofs_file_lock_t        * lock;
         
  file_lock_stat.nb_remove_client++;
       
  /* loop on the locks */
  while (!list_empty(&client->file_lock_list)) {
    lock = list_first_entry(&client->file_lock_list,rozofs_file_lock_t, next_client_lock);
    file_lock_unlink(lock);
    xfree(lock);
  }
      
  /* No more lock on this client. Let's unlink this client */
  list_remove(&client->next_client);
  xfree(client);
  file_lock_stat.nb_client_file_lock--;
}
/*
*___________________________________________________________________
* Remove all the locks of a client and then remove the client 
*
* @param client_ref reference of the client to remove
*___________________________________________________________________
*/
void file_lock_remove_client(uint32_t eid, uint64_t client_ref) {
  list_t * p;
  rozofs_file_lock_client_t * client;
  
  /* Search the given client */
  list_for_each_forward(p, &file_lock_client_list[eid-1]) {
     
    client = list_entry(p, rozofs_file_lock_client_t, next_client);
 
    if (client->client_ref == client_ref) {
      file_lock_remove_one_client(client);
      return;
    }       
  } 
}
/*
*___________________________________________________________________
* To be called after a relaod in order to clean up deleted export
*
*___________________________________________________________________
*/
void file_lock_reload() {
  int        idx;
  export_t * e;
  rozofs_file_lock_client_t * client;

  /*
  ** Loop on every export identifier  
  */
  for (idx=0; idx<EXPGW_EID_MAX_IDX; idx++) {

    /*
    ** Only care about not empty client lists
    */
    if (list_empty(&file_lock_client_list[idx])) {
      continue;
    }
    
    /*
    ** Check whether the eid still exist 
    */
    e = exports_lookup_export(idx+1);
    if (e != NULL) {
      continue;
    }
      
    /*
    ** This export does not exist any more
    */
    while (!list_empty(&file_lock_client_list[idx])) {
      client = list_first_entry(&file_lock_client_list[idx],rozofs_file_lock_client_t, next_client);
      file_lock_remove_one_client(client);
    }
  } 
}
/*
*___________________________________________________________________
* put lock on a client
* eventualy create the client when it does not exist
*
* @param lock the lock to be set
*___________________________________________________________________
*/
rozofs_file_lock_client_t * file_lock_create_client(uint32_t eid, uint64_t ref, ep_client_info_t * info) {
  rozofs_file_lock_client_t * client;
 
  /*
  ** Allocate a client structure
  */  
  client = xmalloc(sizeof(rozofs_file_lock_client_t));
  if (client == NULL) return NULL;
  
  file_lock_stat.nb_add_client++;
  
  client->nb_lock        = 0;
  client->client_ref     = ref;
  memcpy(&client->info,info,sizeof(ep_client_info_t));
  client->last_poll_time = time(0);
 
  list_init(&client->next_client);
  list_init(&client->file_lock_list);
  
  /* Put the client in the list of clients */
  file_lock_stat.nb_client_file_lock++;
  list_push_front(&file_lock_client_list[eid-1],&client->next_client); 
  
  return client;
}
/*
*___________________________________________________________________
* Receive a poll request from a client
*
* @param client_ref reference of the client to remove
*___________________________________________________________________
*/
void file_lock_poll_client(uint32_t eid, uint64_t client_ref, ep_client_info_t * info) {
  list_t * p, * q;
  rozofs_file_lock_client_t * client;
  uint64_t                    now;
  int                         found=0;
  
  now = time(0);
  
  /* Search the given client */
  list_for_each_forward_safe(p, q, &file_lock_client_list[eid-1]) {
     
    client = list_entry(p, rozofs_file_lock_client_t, next_client);
 
    /*
    ** Update this client poll time
    */
    if (client->client_ref == client_ref) {
      client->last_poll_time = now;
      found = 1;
      continue;
    }   

    /*
    ** Check whether this client has to be removed
    */   
    if ((now - client->last_poll_time) > FILE_LOCK_POLL_DELAY_MAX) {
      /* This client has not been polling us for a long time */
      file_lock_remove_client(eid, client->client_ref);
    }
  } 
  
  
  if (found==0) {
    file_lock_create_client(eid, client_ref,info);
  }
}
/*
*___________________________________________________________________
* Remove all the locks of a FID
*
* @param client_ref reference of the client to remove
*___________________________________________________________________
*/
void file_lock_remove_fid_locks(list_t * lock_list) {
  rozofs_file_lock_t        * lock;  

  /* loop on the locks */
  while (!list_empty(lock_list)) {
    lock = list_first_entry(lock_list,rozofs_file_lock_t, next_fid_lock);
    file_lock_unlink(lock);
    xfree(lock);
  }
}

/*
*___________________________________________________________________
* put lock on a client
* eventualy create the client when it does not exist
*
* @param lock the lock to be set
*___________________________________________________________________
*/
void file_lock_add_lock_to_client(uint32_t eid, rozofs_file_lock_t * lock, ep_client_info_t * info) {
  list_t * p;
  rozofs_file_lock_client_t * client;
  
  /* Search the given client */
  list_for_each_forward(p, &file_lock_client_list[eid-1]) {
     
    client = list_entry(p, rozofs_file_lock_client_t, next_client);
 
    if (client->client_ref == lock->lock.client_ref) {
      goto add_lock;
    }       
  }  

  /*
  ** Client does not exist. Allocate a client structure
  */
  client = file_lock_create_client(eid, lock->lock.client_ref,info);
  if (client == NULL) return;

add_lock:

  /* Put the lock in the list of lock of this client */
  list_push_front(&client->file_lock_list,&lock->next_client_lock);
  client->nb_lock++;   
}
/*
*___________________________________________________________________
* Create a new lock
*
* @param lock the lock to be set
*
* retval the lock structure
*___________________________________________________________________
*/
rozofs_file_lock_t * lv2_cache_allocate_file_lock(uint32_t eid, ep_lock_t * lock, ep_client_info_t * info) {

  /*
  ** Allocate a lock structure
  */
  rozofs_file_lock_t * new_lock = xmalloc(sizeof(rozofs_file_lock_t));
  file_lock_stat.nb_file_lock++;
  file_lock_stat.nb_lock_allocate++;
  /* 
  ** Initialize the lock content
  */
  list_init(&new_lock->next_fid_lock);
  list_init(&new_lock->next_client_lock);
  memcpy(&new_lock->lock, lock, sizeof(ep_lock_t));

  /*
  ** Put the lock on the client 
  */
  file_lock_add_lock_to_client(eid, new_lock,info);
  
  return new_lock;
}
/*
*___________________________________________________________________
* Remove a lock
*
* @param lock the lock to be removed
*___________________________________________________________________
*/
void lv2_cache_free_file_lock(uint32_t eid, rozofs_file_lock_t * lock) {
  list_t * p;
  rozofs_file_lock_client_t * client;
   
  /*
  ** Check whether the client has still a lock set 
  */
  
  /* Search the given client */
  list_for_each_forward(p, &file_lock_client_list[eid-1]) {
     
    client = list_entry(p, rozofs_file_lock_client_t, next_client);
 
    if (client->client_ref == lock->lock.client_ref) {
      client->nb_lock--;
      file_lock_unlink(lock);
      break;
    }       
  }
  
  /*
  ** Free the lock
  */  
  xfree(lock);
}
/*
**___________________________END OF FILE LOCK SERVICE_____________________________
*/





char * lv2_cache_display(lv2_cache_t *cache, char * pChar) {

  int i;

  pChar += sprintf(pChar, "lv2 attributes cache : current/max %u/%u\n",cache->size, cache->max);
  pChar += sprintf(pChar, "hit %llu / miss %llu / lru_del %llu\n",
                   (long long unsigned int) cache->hit, 
		   (long long unsigned int)cache->miss,
		   (long long unsigned int)cache->lru_del);
  pChar += sprintf(pChar, "entry size %u - current size %u - maximum size %u\n", 
                   (unsigned int) sizeof(lv2_entry_t), 
		   (unsigned int)sizeof(lv2_entry_t)*cache->size, 
		   (unsigned int)sizeof(lv2_entry_t)*cache->max);
  for (i = 0; i < EXPORT_LV2_MAX_LOCK; i++)
  {
    pChar += sprintf(pChar, "hash%2.2d: %llu \n",i,
                     (long long unsigned int) cache->hash_stats[i]);  
  
  } 
  memset(cache->hash_stats,0,sizeof(uint64_t)*EXPORT_LV2_MAX_LOCK);
  return pChar;		   
}


