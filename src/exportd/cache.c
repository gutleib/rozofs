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
#include <rozofs/core/rozofs_flock.h>
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
  int              forget_locks;       /**< The client has forgotten every locks even persistent ones */
} rozofs_file_lock_client_t;


/*
*___________________________________________________________________
* Lock size to char

*___________________________________________________________________
*/
static inline int rozofs_flockrange2string(char * string, ep_lock_range_t * lock) {
  char * p = string;
  
  switch(lock->size) {
    case EP_LOCK_TOTAL: 
      p += sprintf(p,"0:0 ");
      break;
    case EP_LOCK_FROM_START: 
      p += sprintf(p,"0:%llx ",(long long unsigned int)lock->offset_stop);
      break;
    case EP_LOCK_TO_END: *p = 'E';
      p += sprintf(p,"%llx:0 ",(long long unsigned int)lock->offset_start);
      break;
    case EP_LOCK_PARTIAL: *p = 'P';
      p += sprintf(p,"%llx:%llx ",(long long unsigned int)lock->offset_start,(long long unsigned int)lock->offset_stop);
      break;
    default:
      break;
  }
  return p-string;
} 
/*
*___________________________________________________________________
* Format a string describing the lock set
*
* 
* @retval 1 when locks are compatible, 0 else
*___________________________________________________________________
*/
int rozofs_format_flockp_string(char * string,lv2_entry_t *lv2) {
  char               * pString = string;
  list_t             * p;
  rozofs_file_lock_t * lock_elt;
  uint64_t             prev_client=0;
  uint64_t             prev_owner=0;

  list_for_each_forward(p, &lv2->file_lock) {
    
      lock_elt = list_entry(p, rozofs_file_lock_t, next_fid_lock);
      
      /*
      ** Lock mode
      */
      if (lock_elt->lock.mode == EP_LOCK_READ) {
        pString += sprintf(pString,"R");
      }
      else if (lock_elt->lock.mode == EP_LOCK_WRITE) {
        pString += sprintf(pString,"W");
      }
      else {
        continue;
      }  
      if (prev_client != lock_elt->lock.client_ref) {
        pString += sprintf(pString,":%llx", (long long unsigned int)lock_elt->lock.client_ref);
      }
      else {
        pString += sprintf(pString,":0");
      }
         
      if (prev_owner  != lock_elt->lock.owner_ref) {
        pString += sprintf(pString,":%llx:", (long long unsigned int)lock_elt->lock.owner_ref);
      }
      else {
        pString += sprintf(pString,":0:");
      }
      prev_client = lock_elt->lock.client_ref;
      prev_owner  = lock_elt->lock.owner_ref;    
      pString += rozofs_flockrange2string(pString, &lock_elt->lock.user_range);
  }
  return (pString-string);  
} 
/*
*___________________________________________________________________
* Display file lock statistics
*___________________________________________________________________
*/
#define DISPLAY_LOCK_STAT(name) pChar += sprintf(pChar, "  \"%s\" \t: %llu,\n", #name, (long long unsigned int) file_lock_stat.name); 
#define DISPLAY_LOCK_LAST_STAT(name) pChar += sprintf(pChar, "  \"%s\" \t: %llu\n}}\n", #name, (long long unsigned int) file_lock_stat.name); 
char * display_file_lock(char * pChar) {  
  pChar += sprintf(pChar,"{ \"File lock statistics\" : {\n");
  DISPLAY_LOCK_STAT(nb_file_lock);
  DISPLAY_LOCK_STAT(nb_client_file_lock);
  DISPLAY_LOCK_STAT(nb_lock_allocate);
  DISPLAY_LOCK_STAT(nb_lock_unlink);
  DISPLAY_LOCK_STAT(nb_add_client);  
  DISPLAY_LOCK_LAST_STAT(nb_remove_client);
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
int must_file_lock_be_removed(lv2_entry_t * lv2, uint32_t eid, uint8_t bsize, struct ep_lock_t * lock_free, struct ep_lock_t * lock_set, rozofs_file_lock_t ** new_lock_ctx,ep_client_info_t * info) {
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
      *new_lock_ctx = lv2_cache_allocate_file_lock(lv2,eid, &new_lock,info); 
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
	*new_lock_ctx = lv2_cache_allocate_file_lock(lv2,eid, &new_lock,info); 
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
      *new_lock_ctx = lv2_cache_allocate_file_lock(lv2,eid,&new_lock,info);
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
      *new_lock_ctx = lv2_cache_allocate_file_lock(lv2,eid,&new_lock,info);
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
** Display all the locks of a client
** 
** @param pChar         Buffer to format output
** @param client        The client context
**
** Output the end of the formated buffer
*___________________________________________________________________
*/ 
char * display_file_lock_this_client(char * pChar, rozofs_file_lock_client_t * client) {
  rozofs_file_lock_t        * lock;
  int                         first=1;
  list_t                    * p, *q;
  time_t                      now = time(0);

  pChar += sprintf(pChar,"{\n  \"client reference\"  : \"%llx\",\n  \"#flocks\" : %llu,\n  \"locks\" : [", 
                   (long long unsigned int) client->client_ref,
                   (long long unsigned int)client->nb_lock);
   
  /* 
  ** Find out every locks from this client
  */
  list_for_each_forward_safe(p, q, &client->file_lock_list) {
    lock = list_entry(p, rozofs_file_lock_t, next_client_lock); 
    if (first) {
      first = 0;
    }  
    else {  
      pChar += sprintf(pChar, ","); 
    }  
    pChar += sprintf(pChar, "\n    { \"FID\" : \"");
    pChar += rozofs_fid_append(pChar,lock->lv2->attributes.s.attrs.fid);
    pChar += sprintf(pChar, "\", \"owner\" : \"%llx\", \"age\" : %llu, \"type\" : \"",
                     (long long unsigned int)lock->lock.owner_ref,
                     (long long unsigned int)(now-lock->last_poll_time));
    switch(lock->lock.mode) {
      case EP_LOCK_READ: pChar += sprintf(pChar, "READ"); break;
      case EP_LOCK_WRITE: pChar += sprintf(pChar, "WRITE"); break;
      default: pChar += sprintf(pChar, "%d", lock->lock.mode);
    }                                   
    pChar += sprintf(pChar, "\", \"start\" : %llu, \"stop\" : %llu}", 
                     (long long unsigned int)lock->lock.user_range.offset_start,
                     (long long unsigned int)lock->lock.user_range.offset_stop);
  }
  pChar += sprintf(pChar,"\n  ]\n}\n");
  return pChar;
}
/*
*___________________________________________________________________
** Find out a client in order to dsplay all its locks
** 
** @param pChar         Buffer to format output
** @param client_ref    Client reference to display locks from
**
** Output the end of the formated buffer
*___________________________________________________________________
*/ 
char * display_file_lock_client(char * pChar, uint64_t client_ref) {
  list_t                    * p;
  rozofs_file_lock_client_t * client;
  int                         idx;
  
  for (idx=0; idx<EXPGW_EID_MAX_IDX; idx++) {

    if (list_empty(&file_lock_client_list[idx])) {
      continue;
    }  

    /* Loop on the clients */
    list_for_each_forward(p, &file_lock_client_list[idx]) {

      client = list_entry(p, rozofs_file_lock_client_t, next_client);
      if (client->client_ref != client_ref) continue;

      return display_file_lock_this_client(pChar, client); 
    }   
  }  
  pChar += sprintf(pChar,"No such client reference %llx\n",
                  (long long unsigned int)client_ref);
  return pChar;
}
/*
*___________________________________________________________________
* Remove all the locks of a client and then remove the client 
*
* @param client_ref reference of the client to remove
*___________________________________________________________________
*/
static inline void file_lock_remove_one_client(eid_t eid, rozofs_file_lock_client_t * client) {
  rozofs_file_lock_t        * lock;
  lv2_entry_t               * lv2 ;         
  list_t                    * p, * q;
  export_t                  * e;
  
  file_lock_stat.nb_remove_client++;
       
  /* loop on the locks */
  while (!list_empty(&client->file_lock_list)) {

    lock = list_first_entry(&client->file_lock_list,rozofs_file_lock_t, next_client_lock);   
    lv2 = lock->lv2;

    /*
    ** Remove every lock from this cient on this file
    */
    list_for_each_forward_safe(p, q, &lv2->file_lock) {
    
      lock = list_first_entry(p,rozofs_file_lock_t, next_fid_lock);
      if (lock->lock.client_ref == client->client_ref) {
        file_lock_unlink(lock);
        xfree(lock);
      }
    } 
    /*
    ** Save persistent locks in rozofs specific extended attribute when configure
    */
    e = exports_lookup_export(eid);
    if (e) {
      rozofs_save_flocks_in_xattr(e, lv2);
    }      
  }
      
  /* No more lock on this client. Let's unlink this client */
  list_remove(&client->next_client);
  xfree(client);
  file_lock_stat.nb_client_file_lock--;
}
/*
*___________________________________________________________________
* Display file lock clients
*___________________________________________________________________
*/ 
char * display_file_lock_clients_json(char * pChar) {  
  list_t                    * p, * q;
  rozofs_file_lock_client_t * client;
  uint64_t                    now;
  uint32_t                    ipClient;
  int                         idx;
  int                         first = 1;
  
  now = time(0);

  pChar += sprintf(pChar,"{ \"flock clients\" : {\n");
  pChar += sprintf(pChar, "  \"timeout\" : %d,\n", FILE_LOCK_POLL_DELAY_MAX);
  pChar += sprintf(pChar, "  \"clients\" : [\n");
  
  for (idx=0; idx<EXPGW_EID_MAX_IDX; idx++) {

    if (list_empty(&file_lock_client_list[idx])) {
      continue;
    }  

    /* Loop on the clients */
    list_for_each_forward_safe(p, q, &file_lock_client_list[idx]) {

      client = list_entry(p, rozofs_file_lock_client_t, next_client);

      /*
      ** Remove old clients
      */
      if ((now-client->last_poll_time) > 600) {
        file_lock_remove_one_client(idx+1, client);
        continue;
      }
      
      ipClient = af_unix_get_remote_ip(client->info.socketRef);
      if (first) {
        first = 0;
      }
      else {
        pChar += sprintf(pChar,",\n"); 
      }

      pChar += sprintf(pChar, "    { \"ref\" : \"%16.16llx\", \"eid\" : %2u, \"address\" : \"%u.%u.%u.%u:%u\", \"last poll\" : %2d, \"flock#\" : %d,  \"version\" : \"%s\"}",
                       (long long unsigned int)client->client_ref, idx+1,  
                       (ipClient>>24)&0xFF, (ipClient>>16)&0xFF,(ipClient>>8)&0xFF, ipClient&0xFF, client->info.diag_port,
		       (int) (now-client->last_poll_time),
		       (int)client->nb_lock,
		       client->info.vers);  
    }   
  }
  pChar += sprintf(pChar,"\n  ]\n}}\n"); 
  return pChar;
}
/*
*___________________________________________________________________
* Display file lock clients
*___________________________________________________________________
*/ 
char * display_file_lock_clients(char * pChar) {  
  list_t                    * p, * q;
  rozofs_file_lock_client_t * client;
  uint64_t                    now;
  uint32_t                    ipClient;
  char                        orig[32];
  int                         idx;
  
  now = time(0);

  *pChar = 0;
    
  for (idx=0; idx<EXPGW_EID_MAX_IDX; idx++) {

    if (list_empty(&file_lock_client_list[idx])) {
      continue;
    }  
  
    pChar += sprintf(pChar,"client polling time out : %d sec\n", FILE_LOCK_POLL_DELAY_MAX);
    pChar += sprintf(pChar, "+------------------+---------+-------+-------------------------+------------------------------\n");
    pChar += sprintf(pChar, "| client ref       | poll(s) | #lock | client diagnostic srv   | version\n");  
    pChar += sprintf(pChar, "+------------------+---------+-------+-------------------------+------------------------------\n");  
    /* Loop on the clients */
    list_for_each_forward_safe(p, q, &file_lock_client_list[idx]) {

      client = list_entry(p, rozofs_file_lock_client_t, next_client);

      /*
      ** Remove old clients
      */
      if ((now-client->last_poll_time) > 600) {
        file_lock_remove_one_client(idx+1, client);
        continue;
      }

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
* Search client from its reference
*
* @param client_ref reference of the client to remove
*___________________________________________________________________
*/
rozofs_file_lock_client_t * file_lock_lookup_client(eid_t eid, uint64_t client_ref) {
  list_t * p;
  rozofs_file_lock_client_t * client = NULL;

  /* Search the given client */
  list_for_each_forward(p, &file_lock_client_list[eid-1]) {
     
    client = list_entry(p, rozofs_file_lock_client_t, next_client);
 
    if (client->client_ref == client_ref) {
      return client;
    }       
  }  
  return NULL;
}  
/*
*___________________________________________________________________
* Remove all the locks of a client and then remove the client 
*
* @param client_ref reference of the client to remove
*___________________________________________________________________
*/
void file_lock_remove_client(uint32_t eid, uint64_t client_ref) {
  rozofs_file_lock_client_t * client;
  
  /* Search the given client */
  client = file_lock_lookup_client(eid, client_ref);
  if (client == NULL) return;
  
  file_lock_remove_one_client(eid, client);
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
      file_lock_remove_one_client(idx+1,client);
    }
  } 
}
/*
*___________________________________________________________________
* Creat a  client
*
* @param eid        Export identifier
* @param ref        Client reference
* @param info       Some extra client information
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

  client->forget_locks = 0; 
  
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
  list_t * p, * q, *r, *s;
  rozofs_file_lock_client_t * client;
  uint64_t                    now;
  int                         found=0;
  rozofs_file_lock_t        * pLock;
  
  now = time(0);
  
  /* Search the given client */
  list_for_each_forward_safe(p, q, &file_lock_client_list[eid-1]) {
     
    client = list_entry(p, rozofs_file_lock_client_t, next_client);
 
    /*
    ** Update this client poll time
    */
    if (client->client_ref != client_ref) continue;
    
    client->last_poll_time = now;     

    /* 
    ** Find out every locks from this client
    */
    list_for_each_forward_safe(r, s, &client->file_lock_list) {
      pLock = list_entry(r, rozofs_file_lock_t, next_client_lock); 
      pLock->last_poll_time = now;
    }

    found = 1;
    continue;
  } 
  
  
  if (found==0) {
    file_lock_create_client(eid, client_ref,info);
  }
}
/*
*___________________________________________________________________
* Receive a poll request from a client
*
* @param eid            export identifier
* @param lv2            File information in cache
* @param client_ref     reference of the client/owner to remove
* @param info           client information
*___________________________________________________________________
*/
int file_lock_poll_owner(uint32_t eid, lv2_entry_t * lv2, ep_lock_t * lock, ep_client_info_t * info) {
  list_t * p, * q, *r, *s;
  rozofs_file_lock_client_t * client;
  uint64_t                    now;
  int                         found=0;
  uint64_t                    client_ref =  lock->client_ref;
  uint64_t                    owner_ref  =  lock->owner_ref;
  rozofs_file_lock_t        * pLock;
  int                         result = 0;                    
  now = time(NULL);

  /* 
  ** Search the given client 
  */
  list_for_each_forward_safe(p, q, &file_lock_client_list[eid-1]) {
     
    client = list_entry(p, rozofs_file_lock_client_t, next_client);

    /*
    ** Update this client poll time
    */
    if (client->client_ref == client_ref) {    
      client->last_poll_time = now;
      found = 1;
      /*
      ** Update client info when different. (reloaded locks from flash
      ** have no valid client information associated with them)
      */
      if (strcmp(client->info.vers, info->vers) != 0) {
        memcpy(&client->info,info, sizeof(ep_client_info_t));
      }
      if (owner_ref == 0) {
        break;;
      }

      /*
      ** Loop on the locks of this file
      */
      list_for_each_forward_safe(r, s, &lv2->file_lock) {
        pLock = list_entry(r, rozofs_file_lock_t, next_fid_lock); 
        if ((pLock->lock.client_ref == client_ref) &&  (pLock->lock.owner_ref == owner_ref)) {
          pLock->last_poll_time = now;
          result = 1;        
        }
      }
      continue;
    }   
  } 
  
  
  if (found==0) {
    client = file_lock_create_client(eid, client_ref,info);
  }
  return result;
}
/*
*___________________________________________________________________
* Receive a poll request from a client
*
* @param eid            export identifier
* @param client_ref     reference of the client to remove
* @param info           client information
* @param forget_locks   Client has just been restarted and has forgotten every lock
*___________________________________________________________________
*/
int file_lock_clear_client_file_lock(uint32_t eid, uint64_t client_ref, ep_client_info_t * info) {
  rozofs_file_lock_client_t * client;
  
  /*
  ** Remove this client and all its locks
  */
  file_lock_remove_client(eid,client_ref);

  /*
  ** Recreate this client
  */
  client = file_lock_create_client(eid, client_ref,info);
  client->forget_locks = 1;
  return 0;
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
  rozofs_file_lock_client_t * client;
  
  /* Search the given client */
  client = file_lock_lookup_client(eid, lock->lock.client_ref);

  /*
  ** Client does not exist. Allocate a client structure
  */
  if (client == NULL) {
    client = file_lock_create_client(eid, lock->lock.client_ref,info);
    if (client == NULL) return;
  }

  client->last_poll_time = time(0);

  /* Put the lock in the list of lock of this client */
  list_push_front(&client->file_lock_list,&lock->next_client_lock);
  client->nb_lock++;   
}
/*
*___________________________________________________________________
* Create a new lock
*
* @param lv2     lv2 entry of the file/directory 
* @param eid     export identifier
* @param lock    the lock to be added
* @param info    rozofsmount client information
*
* retval the lock structure
*___________________________________________________________________
*/
rozofs_file_lock_t * lv2_cache_allocate_file_lock(lv2_entry_t * lv2, uint32_t eid, ep_lock_t * lock, ep_client_info_t * info) {

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
  new_lock->lv2 = lv2;
  new_lock->last_poll_time = time(NULL);
  
  /*
  ** Put the lock on the client 
  */
  file_lock_add_lock_to_client(eid, new_lock,info);
  
  /*
  ** insert it on the lv2 entry
  */
  list_push_front(&lv2->file_lock,&new_lock->next_fid_lock);
  lv2->nb_locks++;
  
  return new_lock;
}
/*
*___________________________________________________________________
* Create a lock list from a string
* Some client may have forgotten their locks...
** In this case locks should be re-written back
* 
* @retval The number of forgotten locks
*___________________________________________________________________
*/
int rozofs_decode_flockp_string(lv2_entry_t * lv2, char * string) {
  char                 * pString = string;
  long long unsigned int prev_client=0;
  long long unsigned int prev_owner=0;
  long long unsigned int client=0;
  long long unsigned int owner=0;
  char                   c;
  long long unsigned int start=0;
  long long unsigned int stop=0;
  ep_lock_t              lock; 
  ep_client_info_t       info;
  char                   fidString[40];
  int                    read;
  rozofs_file_lock_client_t * pClient; 
  int                    nb_forgotten=0;
  char                   logmsg[128];

  rozofs_inode_t * fake_inode = (rozofs_inode_t*)lv2->attributes.s.attrs.fid;
    
  while (1) {
    
    read = sscanf(pString,"%c:%llx:%llx:%llx:%llx ", &c, &client, &owner, &start, &stop);
    if (read != 5) return nb_forgotten;
    
    switch(c) {
      case 'R':
        lock.mode = EP_LOCK_READ;
        break;
      case 'W':
        lock.mode = EP_LOCK_WRITE;
        break;       
      default:
        fid2string(lv2->attributes.s.attrs.fid,fidString);
        severe("Bad RozoFLOCKP string %s %s", fidString, string);
        return 1;
    }   
    
    if (client==0) {
      lock.client_ref = prev_client;
    }
    else {
      lock.client_ref = client;
    }
    prev_client = lock.client_ref;
    
    if (owner==0) {
      lock.owner_ref = prev_owner;
    }
    else {
      lock.owner_ref = owner;
    }
    prev_owner = lock.owner_ref;    
      
    if ((start==0)&&(stop==0)) { 
      lock.user_range.offset_start = start;
      lock.user_range.offset_stop  = stop;
      lock.user_range.size = EP_LOCK_TOTAL;      
    }
    else if (start==0) {
      lock.user_range.offset_start = start;
      lock.user_range.offset_stop  = stop;
      lock.user_range.size = EP_LOCK_FROM_START;      
    }
    else if (stop==0) {
      lock.user_range.offset_start = start;
      lock.user_range.offset_stop  = stop;
      lock.user_range.size = EP_LOCK_TO_END;      
    }
    else {
      lock.user_range.offset_start = start;
      lock.user_range.offset_stop  = stop;
      lock.user_range.size = EP_LOCK_PARTIAL;      
    }
    
    info.vers[0]   = '?';
    info.vers[1]   = 0;
    info.diag_port = 0;
    info.socketRef = -1;

    flock_request2string(logmsg,lv2->attributes.s.attrs.fid,&lock);

    pClient = file_lock_lookup_client(fake_inode->s.eid,prev_client);
    if ((pClient) && (pClient->forget_locks)) {
      /*
      ** this client has forgottent every locks. 
      ** Do not reload locks from disk
      */     
      nb_forgotten++;
      warning("not reloaded flock %s",logmsg);
    }
    else {
      /*
      ** Add the lock the the lv2 entry
      */
      lv2_cache_allocate_file_lock(lv2, fake_inode->s.eid, &lock, &info) ; 
      warning("reloaded flock %s",logmsg);
    }
    
    /*
    ** Next lock in the string
    */
    while ((*pString != ' ')&&(*pString != 0)) pString++;
    if (*pString ==0) break;
    pString++;
    if (*pString ==0) break;    
  }
  return nb_forgotten;  
} 
/*
*___________________________________________________________________
* Initialize the flock list from extended attributes when persistent
* file lock is enabled
* 
* @retval 1 when locks are compatible, 0 else
*___________________________________________________________________
*/

struct dentry{
  lv2_entry_t *d_inode;
  export_tracking_table_t *trk_tb_p;

};
ssize_t rozofs_getxattr(struct dentry *dentry, const char *name, void *buffer, size_t size);

void rozofs_reload_flockp(lv2_entry_t * lv2, export_tracking_table_t *trk_tb_p) {
  struct dentry entry;  
  char   buffer[1024];
  int    ret;
  int    nb_forgotten;
  rozofs_inode_t * fake_inode = (rozofs_inode_t*)lv2->attributes.s.attrs.fid;
  export_t                  * e;

  /*
  ** No file locks on directories
  */
  if (S_ISDIR(lv2->attributes.s.attrs.mode)) {
    return;
  }  
  
  /*
  ** The persistent file locks are saved in extended attributes
  ** so there must exist an extended attribute
  */
  if (!rozofs_has_xattr(lv2->attributes.s.attrs.mode)) {
    return;
  }
  
  /*
  ** persistent file locks have to be enabled either on file, export
  ** or RozoFS.
  */
  e = exports_lookup_export(fake_inode->s.eid);
  if (rozofs_are_persistent_file_locks_configured(e, lv2)==0) {
    return;
  }     
     
  /*
  ** Read the RozoFLOCKP extended attribute value
  */
  entry.d_inode  = lv2;
  entry.trk_tb_p = trk_tb_p;
  ret = rozofs_getxattr(&entry, ROZOFS_XATTR_FLOCKP, buffer, 1024);
  if (ret <= 0) {
    return;
  }
  buffer[ret] = 0;
  
  nb_forgotten = rozofs_decode_flockp_string(lv2, buffer);  
  if (nb_forgotten) {
    /*
    ** Some locks have been forgotten
    ** On must update it in extended attributes
    */

    if (e) {
      rozofs_save_flocks_in_xattr(e, lv2);
    }     
  }
}
/*
*___________________________________________________________________
* Remove a lock
*
* @param lock the lock to be removed
*___________________________________________________________________
*/
void lv2_cache_free_file_lock(uint32_t eid, rozofs_file_lock_t * lock) {
  rozofs_file_lock_client_t * client;
   
  /* Search the given client */
  client = file_lock_lookup_client(eid,  lock->lock.client_ref);  

  if (client) {
    client->nb_lock--;
  }
  
  /*
  ** Free the lock
  */  
  file_lock_unlink(lock);     
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


