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


extern int storio_read_parallel;
extern sconfig_t storaged_config;

int STORIO_DEVICE_MAPPING_MAX_ENTRIES = 0;

char * storio_device_monitor_show(char * pChar);

storio_device_mapping_stat_t storio_device_mapping_stat = { };
STORIO_REBUILD_STAT_S        storio_rebuild_stat = {0};
/*
**______________________________________________________________________________
**
**    List of FID having had a problem
**
**    storio_register_faulty_fid() is called to register a faulty FID
**    storio_clear_faulty_fid()    is used to clear the table
**______________________________________________________________________________
*/
pthread_rwlock_t     storio_faulty_fid_lock;

/*
** Record a maximum of 16 faulty FIDs
*/
#define NB_STORIO_FAULTY_FID_MAX 16
typedef struct _storio_disk_thread_file_desc_t {
  fid_t        fid;
  uint8_t      cid;
  uint8_t      sid;
} storio_disk_thread_file_desc_t;

typedef struct _storio_faulty_fid_t {
   uint32_t                         storio_faulty_fid_nb;
   storio_disk_thread_file_desc_t   file[NB_STORIO_FAULTY_FID_MAX];
} storio_faulty_fid_t;

static storio_faulty_fid_t storio_faulty_fid;
/*
**____________________________________________________
**
** Reset the Faulty FID table
**
*/
void storio_clear_faulty_fid() {
  pthread_rwlock_wrlock(&storio_faulty_fid_lock);
  storio_faulty_fid.storio_faulty_fid_nb = 0;
  pthread_rwlock_unlock(&storio_faulty_fid_lock);
}
/*
**____________________________________________________
**
** Initialize the table of faulty FID at start up
**
*/
void storio_faulty_fid_init() {
  pthread_rwlock_init(&storio_faulty_fid_lock,NULL);
  storio_clear_faulty_fid();
}
/*
**____________________________________________________
**
** Display the list of faulty FID
**
** @param pBuffer   buffer to write the faulty FID list in
** @param sid       SID to display faults for
**
** @retval    written size
*/
int storio_faulty_fid_display(char * pBuffer, int sid) {
  char                            * pChar = pBuffer;
  storio_disk_thread_file_desc_t  * pf;
  int                               idx;
  int                               max;
  int                               first = 1;
  
  if (storio_faulty_fid.storio_faulty_fid_nb == 0) return 0;
  
  max = storio_faulty_fid.storio_faulty_fid_nb;
  if (max >= NB_STORIO_FAULTY_FID_MAX) max = NB_STORIO_FAULTY_FID_MAX;
  
  pf = &storio_faulty_fid.file[0];
  for (idx=0; idx  < max; idx++,pf++) {
    if (pf->sid != sid) continue;
    if (first) {
      pChar += rozofs_string_append(pChar,"Faulty FIDs:\n");
      first = 0;
    }  
    pChar += rozofs_string_append(pChar,"    -s ");
    pChar += rozofs_u32_append(pChar, pf->cid);
    pChar += rozofs_string_append(pChar,"/");
    pChar += rozofs_u32_append(pChar, pf->sid);
    pChar += rozofs_string_append(pChar," -f ");	
    pChar += rozofs_fid_append(pChar, pf->fid);
    pChar += rozofs_eol(pChar);
  }
  
  return (pChar-pBuffer);
}   
/*
**____________________________________________________
**
** Register the FID that has encountered an error
**  
** @param NS       UNUSED
** @param cid      the faulty cid 
** @param sid      the faulty sid
** @param fid      the FID in fault   
*/
void storio_register_faulty_fid(int NS, uint8_t cid, uint8_t sid, fid_t fid) {
  int                               idx;
  storio_disk_thread_file_desc_t  * pf;
  int                               max;
  
  /*
  ** Table is full
  */  
  if (storio_faulty_fid.storio_faulty_fid_nb >= NB_STORIO_FAULTY_FID_MAX) return; 

  /*
  ** Check this file is not yet registered
  */
  max = storio_faulty_fid.storio_faulty_fid_nb;
  if (max >= NB_STORIO_FAULTY_FID_MAX) max = NB_STORIO_FAULTY_FID_MAX;

  pf = &storio_faulty_fid.file[0];
  for (idx=0; idx<max; idx++,pf++) {
    if ((pf->cid == cid) && (pf->sid == sid) && (memcmp(pf->fid, fid, sizeof(fid_t))== 0)) {
      return;
    }  
  }

  /*
  ** Get a free record number
  */  
  idx = -1;
  pthread_rwlock_wrlock(&storio_faulty_fid_lock);
  if (storio_faulty_fid.storio_faulty_fid_nb < NB_STORIO_FAULTY_FID_MAX) {
    idx = storio_faulty_fid.storio_faulty_fid_nb++;
  }  
  pthread_rwlock_unlock(&storio_faulty_fid_lock);

  /*
  ** No free record
  */
  if (idx == -1) return;

  
  /*
  ** Register this FID
  */  
  pf = &storio_faulty_fid.file[idx];
  pf->cid    = cid;  
  pf->sid    = sid;
  memcpy(pf->fid, fid, sizeof(fid_t));
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
  int                            idx;
  
  pChar += rozofs_string_append(pChar,"    { \"index\" : ");
  pChar += rozofs_u32_padded_append(pChar,6, rozofs_right_alignment, p->index);    
  pChar += rozofs_string_append(pChar,", \"cid\" : ");
  pChar += rozofs_u32_padded_append(pChar,2, rozofs_right_alignment, p->key.cid);
  pChar += rozofs_string_append(pChar,", \"sid\" : ");
  pChar += rozofs_u32_padded_append(pChar,2, rozofs_right_alignment,p->key.sid);
  pChar += rozofs_string_append(pChar,", \"FID\" : \"");
  pChar += rozofs_fid_append(pChar,p->key.fid);
  pChar += rozofs_string_append(pChar,"\", \"queues\" : { ");
  for (idx=0; idx<ROZOFS_FIDCTX_QUEUE_NB; idx++) {
    if (idx != 0) pChar += rozofs_string_append(pChar,", ");
    pChar += rozofs_u32_append(pChar,p->serial_is_running[idx]); 
  }  
  pChar += rozofs_string_append(pChar,"},\n");

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
    
    pChar += rozofs_string_append(pChar,",\n  \"FID ctx statistics\" : {\n    \"chunk size\"     : ");    
    pChar += rozofs_u64_append(pChar,ROZOFS_STORAGE_FILE_MAX_SIZE/ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);  
    pChar += rozofs_string_append(pChar,",\n    \"read // Q\"      : ");
    pChar += rozofs_u32_append(pChar,storio_read_parallel);
    pChar += rozofs_string_append(pChar,",\n    \"context size\"   : ");
    pChar += rozofs_u32_append(pChar,sizeof(storio_device_mapping_t));
    pChar += rozofs_string_append(pChar,",\n    \"context number\" : ");    
    pChar += rozofs_u32_append(pChar,STORIO_DEVICE_MAPPING_MAX_ENTRIES);
    pChar += rozofs_string_append(pChar,",\n    \"total size\"     : ");
    pChar += rozofs_u32_append(pChar,STORIO_DEVICE_MAPPING_MAX_ENTRIES * sizeof(storio_device_mapping_t));
    pChar += rozofs_string_append(pChar,",\n    \"allocation\"     : ");
    pChar += rozofs_u64_append(pChar,storio_device_mapping_stat.allocation);
    pChar += rozofs_string_append(pChar,",\n    \"release\"        : ");
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

  storage_t   * st;
  int           faulty_devices[STORAGE_MAX_DEVICE_NB];
  
  if ((argv[1] != NULL)&&(strcmp(argv[1],"reset")==0)) {
    storage_device_error_reset();
    uma_dbg_send(tcpRef,bufRef,TRUE,"Device error counters have been reset");  
    return;  
  }
   
  pChar += rozofs_string_append(pChar,"File distibution rule = ");
  pChar += rozofs_string_append(pChar, rozofs_file_distribution_rule_e2String(common_config.file_distribution_rule));
  pChar += rozofs_eol(pChar);
 
  st = NULL;
  while ((st = storaged_next(st)) != NULL) {
    int           dev;
    int           fault=0;

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
    pChar += rozofs_string_append(pChar, common_config_device_selfhealing_mode2String(common_config.device_selfhealing_mode));
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
      pChar += rozofs_string_padded_append(pChar, 7, rozofs_left_alignment, storage_device_status_e2String(pDev->status));
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
    pChar += storio_faulty_fid_display(pChar, st->sid);

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
/*____________________________________________________

  Allocate a device for a file in size balancing mode
  
   @param st: storage context
*/
static inline uint32_t storio_device_mapping_allocate_device_size_balancing(storage_t * st) {
  int           active;
  uint32_t      dev=0;
  uint64_t      val;
  uint32_t      choosen_device = 0;
  uint64_t      max = 0;
     
  active = st->device_free.active;
  for (dev = 0; dev < st->device_number; dev++) {
    
    val = st->device_free.blocks[active][dev];
    
    if (val > max) {
      max            = val;
      choosen_device = dev;
    }
  }
  if (max > 256) max -= 256;

  st->device_free.blocks[active][choosen_device] = max;
   
  
  return choosen_device;
} 
/*____________________________________________________

  Allocate a device for a file in strict round robin mode
  
   @param st: storage context
*/
static inline uint32_t storio_device_mapping_allocate_device_round_robin(storage_t * st) {
  uint32_t      dev=0;
  uint32_t      count=0;
     
  while(count < st->device_number) {    

    /*
    ** Get next device number
    */  
    dev = __atomic_fetch_add(&st->next_device,1,__ATOMIC_SEQ_CST) % st->device_number;

    /*
    ** Check that the device is usable
    */     
    if ((st->device_ctx[dev].status == storage_device_status_is)
    ||  (st->device_ctx[dev].status == storage_device_status_rebuild)) {
      break;
    }
    
    /*
    ** Bad device. Get the next one
    */
    count++;
  }

  return dev;
} 
/*
**____________________________________________________
**
** Allocate a device for a file following a read or write
** round robin algorithm
**  
** @param st        : storage context
** @param layout    : layout to be used for this file
** @param distrib   : File projection distribution within the cluster
**
** @retval The allocated device number on this storage
*/
uint32_t storio_device_mapping_allocate_device_rw_round_robin(storage_t * st, uint8_t layout, sid_t * distrib) {
  uint8_t       inverse;
  int           idx;

  rozofs_device_distribution_rule_e rule = rozofs_get_device_distribution_rule(common_config.file_distribution_rule);
  
  /*
  ** Read round robin
  ** When in the first inverse of the distribution use round robin distribution
  ** else try to equalize the devices
  */
  if (rule == rozofs_device_distribution_rule_read) {
    inverse = rozofs_get_rozofs_inverse(layout);
    for (idx=0; idx < inverse; idx++,distrib++) {
      if (st->sid == *distrib) break;
    }
    if (idx < inverse) {
      return storio_device_mapping_allocate_device_round_robin(st);
    }
    return storio_device_mapping_allocate_device_size_balancing (st);
  }
  
  /*
  ** Strict round robin
  */
  return storio_device_mapping_allocate_device_round_robin(st);
}

/*
**____________________________________________________
**
** Allocate a device for a new chunk of a given file.
**
** SIZE BALANCING:
** _______________
** In size balancing mode one allocates the device with 
** the biggest free size
**
** ROUND ROBIN READ OR WRITE:
** __________________________
** The goal is to spread chunks of big files on every
** devices. If the storio has #nbDevice devices, the 
** allocation policy considers the file in slices of
** #nbDevice continuous chunks.
**
** For the 1rst slice: chunks [0..(#nbDevice-1)]
** - the 1rst allocated device is allocated following 
**   the configured distibution rule. (either read round
**   robin or write round robin).
** - The next chunks in the same slice are allocated 
**   in order to spread the chunks on every available 
**   devices.
**
** For the further slices:
** - the 1rst allocated device is allocated in size 
**   balancing distibution rule. 
** - The next chunks in the same slice are allocated 
**   in order to spread the chunks on every available 
**   devices.
**
** @param fidCtx    : the FID mapping context 
** @param chunk     : the chunk number to allocate
** @param st        : storage context
** @param layout    : layout to be used for this file
** @param distrib   : File projection distribution within the cluster
**
** @retval The allocated device number on this storage
**
*/
uint32_t storio_device_mapping_new_chunk(uint16_t                  chunk,
                                         storio_device_mapping_t * fidCtx,
                                         storage_t               * st, 
                                         uint8_t                   layout, 
                                         sid_t                   * distrib) {
  uint8_t     chunkIdx;                                         
  uint8_t     devIdx;                                         
  uint64_t    bitmap   = 0;
  uint32_t    nbDevice = st->device_number;
  uint8_t     firstChunk;   
  uint8_t     device;

  /*
  ** Case of the mono device without header files. There is only chunk 0
  */
  if (st->mapper_modulo == 0) {
    if (chunk != 0) {
      severe("storio_device_mapping_new_chunk %d", chunk);
    }
    return 0;  
  }  

  /*
  ** size balancing
  */    
  if (common_config.file_distribution_rule == rozofs_file_distribution_size_balancing) {
    return storio_device_mapping_allocate_device_size_balancing (st);    
  }
    
  /*
  ** Loop on a slice of chunks of size nbDevice to find out
  ** which devices has already been allocated for a chunK
  ** within the slice. We try to spread the slice on all devices.
  */
  firstChunk = (chunk / nbDevice)*nbDevice;
  /* Build the bitmap of already used devices in this slice */
  for (chunkIdx=0; chunkIdx<nbDevice; chunkIdx++) {
    device = storio_get_dev(fidCtx, firstChunk+chunkIdx);
    if (device < 65) {
      bitmap |= (1<<device);
    } 
  }
    
  /*
  ** First chunk allocated in this slice.
  ** 1rst slice     : use configured distribution algorithm
  ** further slices : use size balancing distribution
  */
  if (bitmap == 0) {
    if (firstChunk == 0) { 
      return storio_device_mapping_allocate_device_rw_round_robin(st, layout, distrib);
    }
    return storio_device_mapping_allocate_device_size_balancing(st); 
  } 
  
  /*
  ** Some devices have already been choosen in this slice,
  ** Try to get a different one to spread the slice on all devices
  */
  
  /*
  ** Find the 1rst allocated device in the slice
  */
  for (devIdx=0; devIdx<nbDevice; devIdx++) {
    if (bitmap & (1<<devIdx)) break;
  }
  device = devIdx;
  
  /*
  ** Find the 1rst non allocated device after the current one
  */
  for (devIdx=0; devIdx<nbDevice; devIdx++) {
    /* Next device */  
    device = (device+1) % nbDevice; 
    if ( (bitmap & (1<< device)) == 0) {
      /* This device is not yet allocated in this slice */
      if ((st->device_ctx[device].status == storage_device_status_is)
      ||  (st->device_ctx[device].status == storage_device_status_rebuild)) {
        /* This device can be used */
        return device;   
      }
    }  
  }  
  
  /*
  ** No more usable device. Let's use size balancing distribution
  */
  return storio_device_mapping_allocate_device_size_balancing(st);
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
