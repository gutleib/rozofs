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
 <http://www.storage_fid_debuggnu.org/licenses/>.
 */
#define _XOPEN_SOURCE 500

#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h>
#include <uuid/uuid.h>
#include <sys/vfs.h> 
#include <pthread.h> 
#include <sys/wait.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/list.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/profile.h>
#include <rozofs/common/mattr.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/ruc_timer_api.h>
#include <rozofs/core/rozofs_string.h>

#include "storio_device_mapping.h"
#include "sconfig.h"
#include "storaged.h"
#include "storio_fid_cache.h"


extern sconfig_t storaged_config;

int STORIO_DEVICE_MAPPING_MAX_ENTRIES = 0;

char * storio_device_monitor_show(char * pChar);

storio_device_mapping_stat_t storio_device_mapping_stat = { };
STORIO_REBUILD_STAT_S        storio_rebuild_stat = {0};
/*
**______________________________________________________________________________

      Attributes LOOKUP SECTION
**______________________________________________________________________________
*/

#define NB_STORIO_FAULTY_FID_MAX 15

typedef struct _storio_disk_thread_file_desc_t {
  fid_t        fid;
  uint8_t      cid;
  uint8_t      sid;
} storio_disk_thread_file_desc_t;

typedef struct _storio_disk_thread_faulty_fid_t {
   uint32_t                         nb_faulty_fid_in_table;
   storio_disk_thread_file_desc_t   file[NB_STORIO_FAULTY_FID_MAX];
} storio_disk_thread_faulty_fid_t;

storio_disk_thread_faulty_fid_t storio_faulty_fid[ROZOFS_MAX_DISK_THREADS] = {  };
/*
**____________________________________________________
*/
/*
* Register the FID that has encountered an error
  
   @param threadNb the thread number
   @param cid      the faulty cid 
   @param sid      the faulty sid
   @param fid      the FID in fault   
*/
void storio_clear_faulty_fid() {
  memset(storio_faulty_fid,0,sizeof(storio_faulty_fid));
}
/*
**____________________________________________________
*/
/*
* Register the FID that has encountered an error
  
   @param threadNb the thread number
   @param cid      the faulty cid 
   @param sid      the faulty sid
   @param fid      the FID in fault   
*/
void storio_register_faulty_fid(int threadNb, uint8_t cid, uint8_t sid, fid_t fid) {
  storio_disk_thread_faulty_fid_t * p;
  int                               idx;
  storio_disk_thread_file_desc_t  * pf;
    
  if (threadNb >= ROZOFS_MAX_DISK_THREADS) return;
  
  p = &storio_faulty_fid[threadNb];
  
  // No space left to register this FID
  if (p->nb_faulty_fid_in_table >= NB_STORIO_FAULTY_FID_MAX) return;
  
  // Check this FID is not already registered in the table
  for (idx = 0; idx < p->nb_faulty_fid_in_table; idx++) {
    if (memcmp(p->file[idx].fid, fid, sizeof(fid_t))== 0) return;
  } 
  
  // Register this FID
  pf = &p->file[p->nb_faulty_fid_in_table];
  pf->cid    = cid;  
  pf->sid    = sid;
  memcpy(pf->fid, fid, sizeof(fid_t));
  p->nb_faulty_fid_in_table++;
  return;
}
/*
**______________________________________________________________________________
**
** Display one rebuild context
**
** @param pChar       Where to format the output string
** @param pRebuild    The rebuild context to display
**
** retval the end of the formated string
**______________________________________________________________________________
*/
char *  storage_rebuild_one_debug(char * pChar, STORIO_REBUILD_T * pRebuild) {
  pChar += rozofs_string_append(pChar, "        { \"FID\" : \"");
  pChar += rozofs_fid_append(pChar,pRebuild->fid);  
  pChar += rozofs_string_append(pChar, "\", \"ref\" : ");
  pChar += rozofs_u32_append(pChar,pRebuild->ref);
  pChar += rozofs_string_append(pChar, ", \"spare\" : ");
  pChar += rozofs_u32_append(pChar,pRebuild->spare);
  pChar += rozofs_string_append(pChar, ", \"chunk\" : ");
  pChar += rozofs_u32_append(pChar,pRebuild->chunk);
  pChar += rozofs_string_append(pChar, ", \"dev\" : ");
  pChar += rozofs_u32_append(pChar,pRebuild->old_device);
  pChar += rozofs_string_append(pChar, ", \"start\" : ");
  pChar += rozofs_u32_append(pChar,pRebuild->start_block);
  pChar += rozofs_string_append(pChar, ", \"stop\" : ");
  pChar += rozofs_u32_append(pChar,pRebuild->stop_block);
  pChar += rozofs_string_append(pChar, ", \"age\" : ");  
  pChar += rozofs_u64_append(pChar,(time(NULL)-pRebuild->rebuild_ts));
  pChar += rozofs_string_append(pChar, "}");
  return pChar;
}
/*
**______________________________________________________________________________
**
** Display one FID device mapping context
**
** @param pChar   Where to format the output string
** @param p       The FID device mapping context to display
**
** retval the end of the formated string
**______________________________________________________________________________
*/
char * storio_display_one_mapping_ctx(char * pChar, storio_device_mapping_t * p) {
  int                            count=0;
  uint8_t                        nb_rebuild;
  uint8_t                        storio_rebuild_ref;
  STORIO_REBUILD_T           *   pRebuild;
  

  pChar += rozofs_string_append(pChar,"    { \"index\" : ");
  pChar += rozofs_u32_append(pChar,p->index);    
  pChar += rozofs_string_append(pChar,", \"cid\" : ");
  pChar += rozofs_u32_append(pChar,p->key.cid);
  pChar += rozofs_string_append(pChar,", \"sid\" : ");
  pChar += rozofs_u32_append(pChar,p->key.sid);
  pChar += rozofs_string_append(pChar,", \"FID\" : \"");
  pChar += rozofs_fid_append(pChar,p->key.fid);
  pChar += rozofs_string_append(pChar,"\", \"running\" : ");
  if (p->serial_is_running) {    
    pChar += rozofs_string_append(pChar,"\"YES\",\n");
  }
  else {  
    pChar += rozofs_string_append(pChar,"\"NO\",\n");
  }
  
  /*
  ** Display the list of devices per chunk until the end of file
  */
  pChar += rozofs_string_append(pChar,"      \"devices\" : \"");
  pChar  = trace_device(pChar,p);
  pChar += rozofs_string_append(pChar,"\",\n");
  
  
  pChar += rozofs_string_append(pChar,"      \"rebuilds\" :  [");
    
  if (p->storio_rebuild_ref.u64 == 0xFFFFFFFFFFFFFFFF) {
    pChar += rozofs_string_append(pChar,"]\n    }"); 
    return pChar;
  }

  /*
  ** Add the rebuild contexts associated to this FID mapping context
  */
  count = 0;  
  for (nb_rebuild=0; nb_rebuild   <MAX_FID_PARALLEL_REBUILD; nb_rebuild++) {

    storio_rebuild_ref = p->storio_rebuild_ref.u8[nb_rebuild];
    if (storio_rebuild_ref == 0xFF) continue;
    pRebuild = storio_rebuild_ctx_retrieve(storio_rebuild_ref, NULL);
    if (pRebuild == NULL) continue;
    
    if (count==0) {
      pChar += rozofs_string_append(pChar,"\n");
    }
    else {
      pChar += rozofs_string_append(pChar,",\n");
    }
    count++;

    pChar = storage_rebuild_one_debug(pChar,pRebuild);
  }
  pChar += rozofs_string_append(pChar,"\n      ]\n    }"); 
  return pChar;
}
/*
**______________________________________________________________________________
**
** Man of FID rozodiag command
**
**______________________________________________________________________________
*/
void storage_fid_man(char * pChar) {
  pChar += rozofs_string_append_bold(pChar,"fid\n");
  pChar += rozofs_string_append     (pChar,"    Display some general information.\n");  
  pChar += rozofs_string_append_bold(pChar,"fid <FID>\n");  
  pChar += rozofs_string_append     (pChar,"    Display FID mapping context matching FID <FID>.\n");  
  pChar += rozofs_string_append_bold(pChar,"fid <index>\n");
  pChar += rozofs_string_append     (pChar,"    Display FID mapping context of index <index>.\n");  
  pChar += rozofs_string_append_bold(pChar,"fid last [<nb>]\n");
  pChar += rozofs_string_append     (pChar,"    Display last <nb> activated FID contexts. Default <nb> is 6.\n");  
}
/*
**______________________________________________________________________________
**
** Display some FID device mapping context
** argv may tell what c ontext is being requested
**
**______________________________________________________________________________
*/
void storage_fid_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
  char                         * pChar=uma_dbg_get_buffer();
  int                            ret;
  storio_device_mapping_key_t    key;
  storage_t                    * st;
  int                            count;
  storio_device_mapping_t      * p;  
 
  /*
  ** Display general information
  */
  if (argv[1] == NULL) {
    
    pChar += rozofs_string_append(pChar,"{ \"FID mapping statistics\" : {\n");

    pChar = display_cache_fid_stat(pChar);
    
    pChar += rozofs_string_append(pChar,",\n  \"FID ctx statistics\" : {\n    \"chunk size\"  : ");    
    pChar += rozofs_u64_append(pChar,ROZOFS_STORAGE_FILE_MAX_SIZE/ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);  
    pChar += rozofs_string_append(pChar,",\n    \"context size\" : ");
    pChar += rozofs_u32_append(pChar,sizeof(storio_device_mapping_t));
    pChar += rozofs_string_append(pChar,",\n    \"context number\" : ");    
    pChar += rozofs_u32_append(pChar,STORIO_DEVICE_MAPPING_MAX_ENTRIES);
    pChar += rozofs_string_append(pChar,",\n    \"total size\" : ");
    pChar += rozofs_u32_append(pChar,STORIO_DEVICE_MAPPING_MAX_ENTRIES * sizeof(storio_device_mapping_t));
    pChar += rozofs_string_append(pChar,",\n    \"allocation\"  : ");
    pChar += rozofs_u64_append(pChar,storio_device_mapping_stat.allocation);
    pChar += rozofs_string_append(pChar,",\n    \"release\" : ");
    pChar += rozofs_u64_append(pChar,storio_device_mapping_stat.release);
    pChar += rozofs_string_append(pChar,"\n  }\n}}\n");
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
    return;       
  }
  
  
  /*
  ** Display some FID mapping context
  ** depending on given arguments
  */  
     
  pChar += rozofs_string_append(pChar,"{ \"FID mapping\" : [ \n");


  /*
  ** Display the last used FID  mapping contexts
  */
  if (strcmp(argv[1],"last")==0) {
    /*
    ** Default is to display 6 contexts, but
    ** it is possible to request a different number
    */ 
    int max = 6;
    if (argv[2] != NULL) {
      sscanf(argv[2],"%d",&max);
    }
    /*
    ** Get first running FID context in the list
    */
    p = NULL;
    count = 0;

    p = list_last_entry(&storio_device_mapping_ctx_initialized_list,storio_device_mapping_t, link); 

    while ((p!= NULL) &&(count<max)) {
    
      if (count) {
        pChar += rozofs_string_append(pChar,",\n");
      }
      count++;
      pChar = storio_display_one_mapping_ctx(pChar,p); 
      
      p = list_last_entry(&p->link,storio_device_mapping_t, link); 
      if ((void*)p == (void*)&storio_device_mapping_ctx_initialized_list) {
         p = NULL; 
      }
    }
    goto out;
    return;          
  }

  /*
  ** scan argument for a FID
  */ 
  ret = rozofs_uuid_parse(argv[1],key.fid);
  if (ret != 0) {
    int idx;
    /*
    ** It is possible to request a context by its index
    */ 
    if (sscanf(argv[1],"%d",&idx) != 1) goto out;
    
    p = storio_device_mapping_ctx_retrieve(idx);     
    if (p==NULL) goto out;
      
    pChar = storio_display_one_mapping_ctx(pChar,p);        
    goto out;
  }

  /*
  ** It is possible to request a context by its FID
  */ 
  st = NULL;
  count = 0;
  while ((st = storaged_next(st)) != NULL) {
  
    key.cid = st->cid;
    key.sid = st->sid;

    int index = storio_fid_cache_search(storio_device_mapping_hash32bits_compute(&key),&key);
    if (index == -1) {
      continue;
    } 
    p = storio_device_mapping_ctx_retrieve(index);
    if (p == NULL) {
     continue;
    }    

    if (count) {
      pChar += rozofs_string_append(pChar,",\n");
    }
    count++;
    pChar = storio_display_one_mapping_ctx(pChar,p);    
  }  
  
out:  
  pChar += rozofs_string_append(pChar,"\n] }\n");  
  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());
  return;         
}

/*
** Reset error counter on every device
*/
void storage_device_error_reset(void) {
  storage_t   * st;
  int           idx;

  /*
  ** Loop on storage context and clear errors on every device 
  */
  st = NULL;
  while ((st = storaged_next(st)) != NULL) {
    for (idx=0; idx< st->device_number;idx++) {
      if (st->device_ctx[idx].action < STORAGE_DEVICE_RESET_ERRORS) {
	st->device_ctx[idx].action = STORAGE_DEVICE_RESET_ERRORS;
      }  
    }
  }
  
  /*
  ** By the way reset memory log
  */
  storio_device_error_log_reset();
}  
void storage_device_man(char * pChar) {
  pChar += rozofs_string_append(pChar,"device reset : clears error counters on every device.\n");  
  pChar += rozofs_string_append(pChar,"device       : display the storio devices status.\n");  
  pChar += rozofs_string_append(pChar,"It first displays some configuration information, and then displays for each device\n");  
  pChar += rozofs_string_append(pChar,"some specific informations.\n");  
}
void storage_device_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
  char                         * pChar=uma_dbg_get_buffer();
  int                            idx,threadNb; 
  int                            first;

  storage_t   * st;
  int           faulty_devices[STORAGE_MAX_DEVICE_NB];
  
  if ((argv[1] != NULL)&&(strcmp(argv[1],"reset")==0)) {
    storage_device_error_reset();
    uma_dbg_send(tcpRef,bufRef,TRUE,"Device error counters have been reset");  
    return;  
  } 

  st = NULL;
  while ((st = storaged_next(st)) != NULL) {
    int           dev;
    int           fault=0;
    storio_disk_thread_file_desc_t * pf;

    pChar += rozofs_string_append(pChar,"cid = ");
    pChar += rozofs_u32_append(pChar,st->cid);
    pChar += rozofs_string_append(pChar," sid = ");
    pChar += rozofs_u32_append(pChar,st->sid);

    pChar += rozofs_string_append(pChar,"\n    root              = ");
    pChar += rozofs_string_append(pChar,st->root);
    
    pChar += rozofs_string_append(pChar,"\n    mapper_modulo     = ");
    pChar += rozofs_u32_append(pChar,st->mapper_modulo);
    
    pChar += rozofs_string_append(pChar,"\n    mapper_redundancy = ");
    pChar += rozofs_u32_append(pChar,st->mapper_redundancy);
    		          
    pChar += rozofs_string_append(pChar,"\n    device_number     = ");
    pChar += rozofs_u32_append(pChar,st->device_number);
    pChar += rozofs_string_append(pChar,"\n    self-healing-mode  = ");
    pChar += rozofs_string_append(pChar, common_config.device_selfhealing_mode);
    pChar += rozofs_string_append(pChar,"\n    self-healing-delay = ");
    pChar += rozofs_u32_append(pChar,common_config.device_selfhealing_delay);
    pChar += rozofs_string_append(pChar,"\n    spare-mark         = \"");
    if (st->spare_mark != NULL) {   
      pChar += rozofs_string_append(pChar, st->spare_mark);
    }  
    pChar += rozofs_string_append(pChar, "\"\n"); 
      
    pChar += rozofs_string_append(pChar,"\n    device | status | failures |    blocks    |    errors    | diagnostic      |  monitor | inactive | last activity\n");
    pChar += rozofs_string_append(pChar,"    _______|________|__________|______________|______________|_________________|__________|__________|_______________\n");
	     
    for (dev = 0; dev < st->device_number; dev++) {
      storage_device_ctx_t * pDev = &st->device_ctx[dev];
      
      pChar += rozofs_string_append(pChar,"   ");
      pChar += rozofs_u32_padded_append(pChar, 7, rozofs_right_alignment, dev);
      pChar += rozofs_string_append(pChar," | ");
      pChar += rozofs_string_padded_append(pChar, 7, rozofs_left_alignment, storage_device_status2string(pDev->status));
      pChar += rozofs_string_append(pChar,"|");
      pChar += rozofs_u32_padded_append(pChar, 9, rozofs_right_alignment, pDev->failure);
      pChar += rozofs_string_append(pChar," |");
      pChar += rozofs_u64_padded_append(pChar, 13, rozofs_right_alignment, st->device_free.blocks[st->device_free.active][dev]);
      pChar += rozofs_string_append(pChar," |");
      pChar += rozofs_u64_padded_append(pChar, 13, rozofs_right_alignment, st->device_errors.total[dev]);
      pChar += rozofs_string_append(pChar," | ");
      pChar += rozofs_string_padded_append(pChar, 15, rozofs_left_alignment, storage_device_diagnostic2String(pDev->diagnostic));      
      pChar += rozofs_string_append(pChar," |");
      pChar += rozofs_u64_padded_append(pChar, 9, rozofs_right_alignment, pDev->monitor_run);      
      pChar += rozofs_string_append(pChar," |");
      pChar += rozofs_u64_padded_append(pChar, 9, rozofs_right_alignment, pDev->monitor_no_activity);      
      pChar += rozofs_string_append(pChar," | ");
      pChar += rozofs_u64_padded_append(pChar, 9, rozofs_right_alignment, time(NULL)-pDev->last_activity_time);      
       
      pChar += rozofs_eol(pChar);

      if (pDev->status != storage_device_status_is) {
	faulty_devices[fault++] = dev;
      }  			 
    }

    // Display faulty FID table
    first = 1;
    for (threadNb=0; threadNb < ROZOFS_MAX_DISK_THREADS; threadNb++) {
      for (idx=0; idx < storio_faulty_fid[threadNb].nb_faulty_fid_in_table; idx++) {
        if (first) {
	  pChar += rozofs_string_append(pChar,"Faulty FIDs:\n");
          first = 0;
        }
	pf = &storio_faulty_fid[threadNb].file[idx];

	// Check whether this FID has already been listed
	{
	  int already_listed = 0;
	  int prevThread,prevIdx;
          storio_disk_thread_file_desc_t * prevPf;	    
	  for (prevThread=0; prevThread<threadNb; prevThread++) {
            for (prevIdx=0; prevIdx < storio_faulty_fid[prevThread].nb_faulty_fid_in_table; prevIdx++) {
	      prevPf = &storio_faulty_fid[prevThread].file[prevIdx];
	      if ((pf->cid == prevPf->cid) && (pf->sid == prevPf->sid)
	      &&  (memcmp(pf->fid, prevPf->fid, sizeof(fid_t))==0)) {
		already_listed = 1;
		break; 
	      }
            }
	    if (already_listed) break; 	      
	  }
	  if (already_listed) continue;
	}

        pChar += rozofs_string_append(pChar,"    -s ");
	pChar += rozofs_u32_append(pChar, pf->cid);
        pChar += rozofs_string_append(pChar,"/");
	pChar += rozofs_u32_append(pChar, pf->sid);
        pChar += rozofs_string_append(pChar," -f ");	
	rozofs_uuid_unparse(pf->fid, pChar);
	pChar += 36;
	pChar += rozofs_eol(pChar);
      }
    } 

    if (fault == 0) continue;

    // There is some faults on some devices
    pChar += rozofs_string_append(pChar,"\n    !!! ");
    pChar += rozofs_u32_append(pChar,fault);
    pChar += rozofs_string_append(pChar," faulty devices cid=");
    pChar += rozofs_u32_append(pChar,st->cid);
    pChar += rozofs_string_append(pChar,"/sid=");
    pChar += rozofs_u32_append(pChar,st->sid);
    pChar += rozofs_string_append(pChar,"/devices=");
    pChar += rozofs_u32_append(pChar,faulty_devices[0]);
    for (dev = 1; dev < fault; dev++) {
      *pChar++ = ',';
      pChar += rozofs_u32_append(pChar,faulty_devices[dev]);  
    } 
    pChar += rozofs_eol(pChar);
    pChar += rozofs_string_append(pChar,"    !!! Check for errors in \"log show\" rozodiag topic\n");
  }  
  
  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());
  return;         
}

/*
**______________________________________________________________________________
*/
/**
* Debug 
  
*/
void storage_rebuild_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
  uint8_t                        storio_ref;
  STORIO_REBUILD_T             * pRebuild;
  char                         * pChar=uma_dbg_get_buffer();
  int                            nb=0;
  int                            doreset=0;

  if ((argv[1] != NULL) && (strcmp(argv[1],"reset")==0)) {
    doreset=1;     
  }

  pChar += rozofs_string_append(pChar,"allocated        : ");
  pChar += rozofs_u64_append(pChar,storio_rebuild_stat.allocated);
  pChar += rozofs_string_append(pChar,"\nstollen          : ");
  pChar += rozofs_u64_append(pChar,storio_rebuild_stat.stollen);
  pChar += rozofs_string_append(pChar,"\naborted          : ");
  pChar += rozofs_u64_append(pChar,storio_rebuild_stat.aborted);
  pChar += rozofs_string_append(pChar,"\nreleased         : ");
  pChar += rozofs_u64_append(pChar,storio_rebuild_stat.released);
  pChar += rozofs_string_append(pChar,"\nout of ctx       : ");
  pChar += rozofs_u64_append(pChar,storio_rebuild_stat.out_of_ctx);
  pChar += rozofs_string_append(pChar,"\nlookup hit       : ");
  pChar += rozofs_u64_append(pChar,storio_rebuild_stat.lookup_hit);
  pChar += rozofs_string_append(pChar,"\nlookup miss      : ");
  pChar += rozofs_u64_append(pChar,storio_rebuild_stat.lookup_miss);
  pChar += rozofs_string_append(pChar,"\nlookup bad index : ");
  pChar += rozofs_u64_append(pChar,storio_rebuild_stat.lookup_bad_index);
  
  pChar += rozofs_string_append(pChar,"\n{ \"rebuilds\" : ["); 
  
  for (storio_ref=0; storio_ref <MAX_STORIO_PARALLEL_REBUILD; storio_ref++) {

    pRebuild = storio_rebuild_ctx_retrieve(storio_ref, NULL);
    if (pRebuild->rebuild_ts == 0) continue;
    if (nb) {
      pChar += rozofs_string_append(pChar,",\n");
    }
    else{
      pChar += rozofs_string_append(pChar,"\n");
    } 
    pChar = storage_rebuild_one_debug(pChar,pRebuild);
    nb++;
  }
  pChar += rozofs_string_append(pChar,"\n] }"); 


  if (doreset) {
    memset(&storio_rebuild_stat,0,sizeof(storio_rebuild_stat));
    pChar += rozofs_string_append(pChar,"Reset done\n");
  }    
  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());
  return;         
}
/*
**____________________________________________________
*/
/*
  Allocate a device for a file
  
   @param st: storage context
*/
uint32_t storio_device_mapping_allocate_device(storage_t * st) {
  uint32_t      dev=0;
  uint64_t    * pBlocks;
  uint64_t      max = 0;
  uint64_t      val;
  uint64_t      choosen_device = 0;
  int           active;
  
  if (common_config.file_distribution_rule != rozofs_file_distribution_size_balancing) {
    int count = 0;
    
    while(count < st->device_number) {
    
      /*
      ** Get next device number
      */  
      dev = __atomic_fetch_add(&st->next_device,1,__ATOMIC_SEQ_CST) % st->device_number;

      /*
      ** Check that the device is usable
      */     
      if (st->device_ctx[dev].status <= storage_device_status_is) return dev;

      /*
      ** Bad device. Get the next one
      */
      count++;
    }
    /*
    ** Well !!!
    */
    return dev;
  }
  
  active = st->device_free.active;
  for (dev = 0; dev < st->device_number; dev++,pBlocks++) {
    
    val = st->device_free.blocks[active][dev];
    if (val > max) {
      max = val;
      choosen_device = dev;
    }
  }
  if (max > 256) max -= 256;
  st->device_free.blocks[active][choosen_device] = max;
  
  return choosen_device;
}
/*_____________________________________
** Parameter of the relocate thread
*/
typedef struct _storio_relocate_ctx_t {
  storage_t     * st;
  int             dev;
} storio_relocate_ctx_t;

/*
**______________________________________________________________________________
*/
/**
* attributes entry hash compute 

  @param key1 : pointer to the key associated with the entry from cache 
  @param key2 : pointer to array that contains the searching key
  
  @retval 0 on match
  @retval <> 0  no match  
*/

uint32_t storio_device_mapping_exact_match(void *key ,uint32_t index) {
  storio_device_mapping_t   * p;  
  
  p = storio_device_mapping_ctx_retrieve(index);
  if (p == NULL) return 0;
    
  if (memcmp(&p->key,key, sizeof(storio_device_mapping_key_t)) != 0) {
    return 0;
  }
  /*
  ** Match !!
  */
  return 1;
}
/*
**______________________________________________________________________________
*/
/**
* attributes entry hash compute 

  @param key1 : pointer to the key associated with the entry from cache 
  @param key2 : pointer to array that contains the searching key
  
  @retval 0 on match
  @retval <> 0  no match  
*/

uint32_t storio_device_mapping_delete_req(uint32_t index) {
  storio_device_mapping_t   * p;  
  
  /*
  ** Retrieve the context from the index
  */
  p = storio_device_mapping_ctx_retrieve(index);
  if (p == NULL) return 0;
    
  /*
  ** Release the context when inactive
  */  
  storio_device_mapping_release_entry(p);
  return 0;
}	   
/*
**______________________________________________________________________________
*/
/**
* creation of the FID cache
 That API is intented to be called during the initialization of the module
 
 The max number of entries is given the STORIO_DEVICE_MAPPING_MAX_ENTRIES constant
 and the size of the level 0 entry set is given by STORIO_DEVICE_MAPPING_LVL0_SZ_POWER_OF_2 constant
 
 retval 0 on success
 retval < 0 on error
*/
 
uint32_t storio_device_mapping_init()
{

  /*
  ** Initialize rebuild context distributor
  */
  storio_rebuild_ctx_distributor_init();
  
  /*
  ** Initialize the FID cache 
  */
  storio_fid_cache_init(storio_device_mapping_exact_match, storio_device_mapping_delete_req);

  /*
  ** Initialize dev mapping distributor
  */
//  storio_device_mapping_stat.consistency = 1;   
  storio_device_mapping_ctx_distributor_init(common_config.storio_fidctx_ctx);

  /*
  ** Start device monitor thread
  */
  storio_device_mapping_monitor_thread_start();
  
    
  /*
  ** Add a debug topic
  */
  uma_dbg_addTopicAndMan("device", storage_device_debug, storage_device_man, 0); 
  uma_dbg_addTopicAndMan("fid", storage_fid_debug,  storage_fid_man, 0); 
  uma_dbg_addTopic_option("rebuild", storage_rebuild_debug, UMA_DBG_OPTION_RESET); 
  return 0;
}
