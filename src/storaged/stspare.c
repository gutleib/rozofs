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

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <fcntl.h> 
#include <sys/un.h>             
#include <errno.h>  
#include <time.h>
#include <pthread.h> 
#include <dirent.h>
#include <getopt.h>

#include <rozofs/core/ruc_common.h>
#include <rozofs/core/ruc_sockCtl_api.h>
#include <rozofs/core/ruc_timer_api.h>
#include <rozofs/core/uma_tcp_main_api.h>
#include <rozofs/core/ruc_tcpServer_api.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/rozofs_socket_family.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rpc/mproto.h>
#include <rozofs/rpc/rozofs_rpc_util.h>
#include <rozofs/rpc/rpcclt.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/common/daemon.h>

#include "config.h"
#include "sconfig.h"
#include "storage.h"
#include "stspare_fid_cache.h"
#include "rbs.h"
#include "rbs_eclient.h"

sconfig_t     storaged_config;
static char   storaged_config_file[PATH_MAX] = STORAGED_DEFAULT_CONFIG;
static char   storaged_hostname_buffer[512];


storage_t storaged_storages[STORAGES_MAX_BY_STORAGE_NODE] = { { 0 } };
uint16_t  storaged_nrstorages = 0;

int       spare_restore_read_throughput=0;

/*
** Buffer for reading and analyzing spare files
*/
#define          BUFFER_SIZE_MB    1
#define          BUFFER_SIZE      (BUFFER_SIZE_MB*1024*1024)
static char      buffer[BUFFER_SIZE];

/*
** Statistics
*/
typedef struct _stspare_stat_t {
  time_t        seconds_before_next_run;
  uint64_t      number_of_run_done;
  uint64_t      rebuild_nominal_attempt;
  uint64_t      rebuild_nominal_success;
  uint64_t      rebuild_nominal_failure;
  uint64_t      rebuild_spare_attempt;
  uint64_t      rebuild_spare_success;
  uint64_t      rebuild_spare_failure;  
} stspare_stat_t;
stspare_stat_t stspare_stat={0};


#define       MAX_STORAGED_HOSTNAMES 64
char *        pHostArray[MAX_STORAGED_HOSTNAMES]={0};

#define STSPARE_PID_FILE "stspare"

/*
** Bit map  of available sid in each cluster
*/
uint64_t   cluster_bitmap[ROZOFS_CLUSTERS_MAX];


#warning need to get local site number
int        local_site = 0;
int        seed=0;
typedef struct _stspare_working_file_t {
  cid_t      cid;
  sid_t      sid;
  uint8_t    chunk;
  fid_t      fid;
  uint8_t    prj;
  uint64_t   start;
  uint64_t   stop;
  time_t     time;
} stspare_working_file_t; 

stspare_working_file_t stspare_current_working_file = {0}; 
  
/*
**____________________________________________________
**
** Fake Allocate of a device for a file, in order to link
** with storage.c
** 
** @param st    storage context
**
** @retval -1 always
**____________________________________________________
*/
uint32_t storio_device_mapping_allocate_device(storage_t * st) {
  severe("storaged spare restorer should not call storio_device_mapping_allocate_device");
  return -1;
}
/*
**____________________________________________________
**
** Parsing a / separated list of host
** The result is an array pointer pHostArray[]
** towards the given names or addresses
**
** @param host   A / separated list of host name or IP addresses    
**____________________________________________________
*/
void parse_host_name(char * host) {
  int    nb_names=0;
  char * pHost;
  char * pNext;

  if (host == NULL) return;
  
  strcpy(storaged_hostname_buffer,host);
  pHost = storaged_hostname_buffer;
  while (*pHost=='/') pHost++;
  
  while (*pHost != 0) {
  
    pHostArray[nb_names++] = pHost;
    
    pNext = pHost;
    
    while ((*pNext != 0) && (*pNext != '/')) pNext++;
    if (*pNext == '/') {
      *pNext = 0;
      pNext++;
    }  

    pHost = pNext;
  }
  pHostArray[nb_names++] = NULL;  
}
/*
**____________________________________________________
**
** Spare restorer statistics format
** 
**____________________________________________________
*/
#define  STSPARE_DEBUG_STRING(name,val) pChar += sprintf(pChar,"    \"%s\"\t: \"%s\",\n",#name,val);
#define  STSPARE_DEBUG_INT(name,val) pChar += sprintf(pChar,"    \"%s\"\t: %llu,\n",#name,(long long unsigned int)val);
#define  STSPARE_DEBUG_STAT(name) STSPARE_DEBUG_INT(name,stspare_stat.name);
void stspare_debug_stat(char * pChar) {

  /*
  ** Begin
  */
  pChar += sprintf(pChar, "{\n");

  /*
  ** Config data
  */
  pChar += sprintf(pChar, "  \"spare restore configuration\" : {\n");


  STSPARE_DEBUG_STRING(spare_restore_enabled,common_config.spare_restore_enable?"Yes":"No");
  STSPARE_DEBUG_STRING(export_host_name_list,common_config.export_hosts);
  STSPARE_DEBUG_INT(spare_restore_loop_delay,common_config.spare_restore_loop_delay);
  STSPARE_DEBUG_INT(read_throughput_MB,spare_restore_read_throughput);

  pChar -= 2;
  pChar += sprintf(pChar,"\n  },\n");
  
  /*
  ** Update statistics data 
  */
  pChar += sprintf(pChar, "  \"spare restore statistics\" : {\n");

  STSPARE_DEBUG_STAT(number_of_run_done);
  if (stspare_stat.seconds_before_next_run==0) {
    STSPARE_DEBUG_INT(seconds_before_next_run,0);
  }  
  else {
    STSPARE_DEBUG_INT(seconds_before_next_run,stspare_stat.seconds_before_next_run-time(NULL));
  }  
  STSPARE_DEBUG_STAT(rebuild_nominal_attempt);
  STSPARE_DEBUG_STAT(rebuild_nominal_success);
  STSPARE_DEBUG_STAT(rebuild_nominal_failure);
  STSPARE_DEBUG_STAT(rebuild_spare_attempt);
  STSPARE_DEBUG_STAT(rebuild_spare_success);  
  STSPARE_DEBUG_STAT(rebuild_spare_failure);  
  STSPARE_DEBUG_INT (pending_spare_file,stspare_fid_cache_stat.allocation-stspare_fid_cache_stat.release);
   
  pChar -= 2;
  pChar += sprintf(pChar,"\n  },\n");

  /*
  ** Current working file
  */
  pChar += sprintf(pChar, "  \"current file\" : {\n"); 
  if (stspare_current_working_file.cid!=0){ 
    pChar += sprintf(pChar,"    \"cluster id\"\t: %u,\n",stspare_current_working_file.cid);
    pChar += sprintf(pChar,"    \"storage id\"\t: %u,\n",stspare_current_working_file.sid);
    pChar += rozofs_string_append(pChar,"    \"fid\"\t\t: \"");
    pChar += rozofs_fid_append(pChar, stspare_current_working_file.fid);
    pChar += rozofs_string_append(pChar,"\",\n");
    pChar += sprintf(pChar,"    \"chunk number\"\t: %u,\n",stspare_current_working_file.chunk);
    pChar += sprintf(pChar,"    \"duration\"\t\t: %llu,\n",(unsigned long long int)(time(NULL)-stspare_current_working_file.time));
    
    if (stspare_current_working_file.prj==0xFF) {
      pChar += rozofs_string_append(pChar,"    \"projection\"\t: \"all\",\n");
    }
    else {
      pChar += sprintf(pChar,"    \"projection\"\t: %u,\n",stspare_current_working_file.prj);
    }  
    if ((stspare_current_working_file.start==0) && (stspare_current_working_file.stop==0xFFFFFFFF)) {
      pChar += rozofs_string_append(pChar,"    \"block interval\"\t: \"all\"\n");
    }  
    else {
      pChar += sprintf(pChar,"    \"block interval\"\t: \"[%llu;%llu]\"\n",
                       (unsigned long long int)stspare_current_working_file.start,
                       (unsigned long long int)stspare_current_working_file.stop);    
    }                       
  }
  pChar += sprintf(pChar,"  }\n");

  /*
  ** End
  */  
  pChar += sprintf(pChar,"}\n");  
}
/*
**______________________________________________________________________________
**
** Spare restorer statistics diagnostic functions
**____________________________________________________
*/
void stspare_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
  stspare_debug_stat(uma_dbg_get_buffer());
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());         
}
/*
**______________________________________________________________________________
**
** Spare restorer statistics diagnostic functions
**____________________________________________________
*/
void stspare_set_throughput(char * argv[], uint32_t tcpRef, void *bufRef) {
  int    val;
  char * p = uma_dbg_get_buffer();

  /*
  ** Display current throughput value
  */  
  if (argv[1] == 0) {
    goto display;
  }
  
  /*
  ** Read a new throughput value
  */
  if (sscanf(argv[1],"%d",&val)!=1) {
    p += rozofs_string_append(p,"Unexpected value for throughput \"");
    p += rozofs_string_append(p,argv[1]);      
    p += rozofs_string_append(p,"\"\n");
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());         
    return;
  }

  
  spare_restore_read_throughput = val;

display:
  p += rozofs_string_append(p,"{ \"read_throughput_MB\" : {\n");
  p += rozofs_string_append(p,"         \"current\"    : ");
  p += rozofs_u32_append(p,spare_restore_read_throughput);      
  p += rozofs_string_append(p,",\n         \"configured\" : ");
  p += rozofs_u32_append(p, common_config.spare_restore_read_throughput);      
  p += rozofs_string_append(p,"\n  }\n}\n");
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());         
  return;
        
}

/*
**____________________________________________________
**
** Reset the bitmap of the available sid in a cluster
**
**____________________________________________________
*/
static inline void stspare_reset_sid_bitmap() {
  memset(cluster_bitmap,0,sizeof(cluster_bitmap));
}
/*
**____________________________________________________
**
** Get the bitmap of the available sid in a cluster
**
** @param   cid       The cluster id we are interested in
**
** @retval  The bitmap of the available sid
**____________________________________________________
*/
uint64_t stspare_get_sid_bitmap(cid_t cid) {
  uint64_t     * p;
  rpcclt_t       rpcclt;
  list_t       * s;
  rb_cluster_t * cluster;
  rb_stor_t    * storage;
  list_t         cluster_entries;  

  /*
  ** Check whether this clustr bitmap has already been computed
  */
  p = &cluster_bitmap[cid];
  if (*p!=0) return *p;

  /*
  ** Initialize the list of clusters
  */
  list_init(&cluster_entries);
  
  /*
  ** Get list of storages of that cluster from the export
  */
  memset(&rpcclt,0,sizeof(rpcclt_t));
  if (rbs_get_cluster_list(&rpcclt, common_config.export_hosts, local_site, cid, &cluster_entries)==NULL) {
    rpcclt_release(&rpcclt);
    return 0;
  }
  
  /*
  ** Valid entry
  */
  *p |= 1;
  
  /*
  ** Check the available storages
  */
  cluster = list_first_entry(&cluster_entries, rb_cluster_t, list);
  if (cluster->cid != cid) return 0;
  
  /*
  ** Loop on configured storages 
  */
  list_for_each_forward(s, &cluster->storages) {
  
    storage = list_entry(s, rb_stor_t, list);
		
    /*
    ** Get connections for this storage
    */
    if (rbs_stor_cnt_initialize(storage,cid) != 0) {
      continue;	    
    }
    
    /*
    ** Valid storage
    */  
    *p |= (1L<<storage->sid);
  }
  /*
  ** Realease every mclient and sclient in the storage list of the cluster
  */    
  rbs_release_cluster_list(&cluster_entries) ;
       
  return *p; 
}
/*
**____________________________________________________
**
** Compute the last block to rebuild from start at this 
** speed in order to rebuild progressively the file 
** in slots of 192 secondes (3min12)
**
** @param speedMB  The input speed
** @param start    The rebuild starting block number
** @param stop     The last block to rebuild
**
** @retval  the last block to rebuild from start in 
**          a run
**____________________________________________________
*/
static inline uint64_t stspare_restore_compute_last_block(uint64_t speedMB, uint64_t start, uint64_t stop) {
  uint64_t nbBlocks;
  uint64_t blocks256sec;
  uint64_t blocks192sec;

  /*
  ** Speed limit
  */
  if (speedMB > 400) {
    speedMB = 400;
  }
  
  /*
  ** number of block to rebuild
  */   
  nbBlocks = stop - start + 1;

  /*
  ** number of blocks during 256 seconds at this speed
  */
  blocks256sec = (256*speedMB*1024*1024)/4096;

  /*
  ** More blocks to rebuild ?
  */
  if (nbBlocks > blocks256sec) {
    /*
    ** Let's rebuild 3 min 12
    */
    blocks192sec = (192*speedMB*1024*1024)/4096;
    stop = start + blocks192sec - 1;
  }
   
  return stop;
}
/*
**____________________________________________________
**
** One spare file with only holes restoring
**
** @param fidCtx       The FID cache context of the spare file
**
** @retval 0 when FID context is completly processed
**         1 else
**____________________________________________________
*/
int stspare_restore_hole(stspare_fid_cache_t * fidCtx, uint64_t sidBitMap, char * fidString) {
  int                            idx;
  char                           cmd[512];
  int                            ret;
  int                            result;
  uint8_t                        fwd;
   

  /*
  ** Get the forward number of projection for this layout
  */
  fwd = rozofs_get_rozofs_forward(fidCtx->data.layout);

  if (fidCtx->data.prj_bitmap == 0) {
    /*
    ** Abnormal case. Forget about this context,
    ** we will see later.
    */
    goto release;   
  }
  /*
  ** One must rebuild every projections since we do not know which sid is
  ** secured by this hole. Check every sid is up.
  */
  for (idx=1; idx<=fwd; idx++) {

    /*
    ** Check that every sid to rebuild is up
    */
    if ((sidBitMap & (1L<<fidCtx->data.dist[idx-1])) == 0) {
      return 1;
    }
  }

  stspare_current_working_file.cid   = fidCtx->data.key.cid;
  stspare_current_working_file.chunk = fidCtx->data.key.chunk;
  memcpy(stspare_current_working_file.fid,fidCtx->data.key.fid,sizeof(fid_t));

  stspare_current_working_file.start = fidCtx->data.prj[0].start;
  stspare_current_working_file.stop  = fidCtx->data.prj[0].stop;
  
  /*
  ** One must rebuild every projections
  */
  for (idx=1; idx<=fwd; idx++) {
    stspare_current_working_file.prj  = idx - 1;
    stspare_current_working_file.sid  = fidCtx->data.dist[idx-1];
    stspare_current_working_file.time = time(NULL); 
    stspare_stat.rebuild_nominal_attempt++;

    /*
    ** Request for rebuilding
    ** prj[0]   describes the holes
    ** prj[idx] describes projection id (idx-1) that should be written in sid dist[idx-1] 
    */
    sprintf(cmd,"storage_rebuild --nolog -t %d -s %d/%d -f %s --chunk %d --bstart %u --bstop %u -l 1 -fg -q",
            spare_restore_read_throughput,
	    fidCtx->data.key.cid, fidCtx->data.dist[idx-1],  fidString,
	    fidCtx->data.key.chunk, fidCtx->data.prj[0].start, fidCtx->data.prj[0].stop);	 		 
    ret    = system(cmd);
    result = WEXITSTATUS(ret);

    /*
    ** Update statistics
    */
    if (result==0) {
      stspare_stat.rebuild_nominal_success++;
    } 
    else {
      stspare_stat.rebuild_nominal_failure++;      
    }   
    usleep(100000);    
  } 

  stspare_current_working_file.prj  = -1;
  stspare_current_working_file.sid  = fidCtx->data.key.sid;
  stspare_current_working_file.time = time(NULL); 
  stspare_stat.rebuild_spare_attempt++;
  
  /*
  ** Then let's rebuild the spare file it self hopping it will disappear
  */	
  sprintf(cmd,"storage_rebuild --nolog  -t %d -s %d/%d -f %s --chunk %d --bstart %u --bstop %u -l 1 -fg -q",
          spare_restore_read_throughput,  
	  fidCtx->data.key.cid, fidCtx->data.key.sid,  fidString,
	  fidCtx->data.key.chunk, fidCtx->data.prj[0].start, fidCtx->data.prj[0].stop);
  ret    = system(cmd);
  result = WEXITSTATUS(ret);

  /*
  ** Update statistics
  */    
  if (result==0) {
    stspare_stat.rebuild_spare_success++;
  }     
  else {
    stspare_stat.rebuild_spare_failure++;      
  }   
  usleep(100000); 

  /*
  ** Everything that could be done has been done
  */
  
release:
  /*
  ** Release context
  */      
  stspare_fid_cache_release(fidCtx);   
  return 0;     
}
/*
**____________________________________________________
**
** One spare file restoring with projections in it
**
** @param fidCtx       The FID cache context of the spare file
**
** @retval 0 when FID context is completly processed
**         1 else
**____________________________________________________
*/
int stspare_restore_projections(stspare_fid_cache_t * fidCtx, uint64_t sidBitMap, char * fidString) {
  int                            idx;
  char                           cmd[512];
  int                            ret;
  int                            result;
  uint8_t                        fwd;
  uint64_t                       start,stop;

  /*
  ** Get the forward number of projection for this layout
  */
  fwd = rozofs_get_rozofs_forward(fidCtx->data.layout);


  stspare_current_working_file.cid   = fidCtx->data.key.cid;
  stspare_current_working_file.chunk = fidCtx->data.key.chunk;
  memcpy(stspare_current_working_file.fid,fidCtx->data.key.fid,sizeof(fid_t));
  /*
  ** Some projections are present in the spare file. Try to rebuild them
  ** on the sid that should normaly have them.
  */
  for (idx=1; idx<=fwd; idx++) {

    /*
    ** Rebuild each sid whoes projections are present in the spare file
    */
    if ((fidCtx->data.prj_bitmap & (1 << idx)) == 0) {
      continue;
    } 

    /*
    ** Check that this sid is up
    */
    if ((sidBitMap & (1L<<fidCtx->data.dist[idx-1])) == 0) {
      continue;
    }
    
    /*
    ** To update rozodiag "spare"
    */
    stspare_current_working_file.prj   = idx - 1;
    stspare_current_working_file.sid   = fidCtx->data.dist[idx-1];
    stspare_current_working_file.time = time(NULL); 
    stspare_current_working_file.start = fidCtx->data.prj[idx].start;
    stspare_current_working_file.stop  = fidCtx->data.prj[idx].stop;

    stspare_stat.rebuild_nominal_attempt++;

    /*
    ** Split the rebuild in slots of 3/4 minutes
    */
    while (1) {

      start = fidCtx->data.prj[idx].start;
      if (start > fidCtx->data.prj[idx].stop) {
        stspare_stat.rebuild_nominal_success++;      
        /* 
        ** This projection is done so clear the bit 
        */
	fidCtx->data.prj_bitmap &= ~(1 << idx);
	fidCtx->data.nb_prj --;  
        /*
        ** a small sleep between each rebuild
        */ 
        usleep(100000);
        /*
        ** Next projection to rebuild
        */
        break;              
      }

      /*
      ** Where to stop for a 3/4 minutes rebuild
      */
      stop = stspare_restore_compute_last_block(spare_restore_read_throughput,
                                        start, 
                                        fidCtx->data.prj[idx].stop);
      /*
      ** To update rozodiag "spare"
      */
      stspare_current_working_file.start = start;

      /*
      ** Request for rebuilding
      ** prj[0]   describes the holes
      ** prj[idx] describes projection id (idx-1) that should be written in sid dist[idx-1] 
      */
      sprintf(cmd,"storage_rebuild --nolog -t %d -s %d/%d -f %s --chunk %d --bstart %llu --bstop %llu -l 1 -fg -q",
               spare_restore_read_throughput,
	       fidCtx->data.key.cid, fidCtx->data.dist[idx-1],  fidString,
	       fidCtx->data.key.chunk, (unsigned long long int)start, (unsigned long long int)stop);
      ret    = system(cmd);
      result = WEXITSTATUS(ret);

      /*
      ** Failure
      */      
      if (result!=0) {
        stspare_stat.rebuild_nominal_failure++;
        /*
        ** a small sleep between each rebuild
        */ 
        usleep(100000);
        break;
      }

      /*
      ** Success
      */
      
      /* 
      ** Update starting point 
      */
      start = stop + 1;
      fidCtx->data.prj[idx].start = start;
    }
  }
  
  /*
  ** All projections have been rebuilt, just need to rebuild the spare file
  ** Set nb_proj to -1 not to miss with the case of only hole spare file.
  */
  if (fidCtx->data.nb_prj == 0) {
    fidCtx->data.nb_prj = 0xFF;
  }
  
  if (fidCtx->data.nb_prj != 0xFF) { 
    /*
    ** Not all projections have been rebuilt
    */   
    return 1;
  }
  
  /*
  ** Check that every sid of the distribution is up before rebuilding the spare file
  */
  for (idx=1; idx<=fwd; idx++) {

    if ((sidBitMap & (1L<<fidCtx->data.dist[idx-1])) == 0) {
      return 1;
    }
  }
  stspare_current_working_file.start = 0;
  stspare_current_working_file.stop  = -1;
  stspare_current_working_file.prj   = -1;
  stspare_current_working_file.sid   = fidCtx->data.key.sid;
  stspare_current_working_file.time  = time(NULL); 
  stspare_stat.rebuild_spare_attempt++;
      
  /*
  ** Since every thing has been rebuilt, rebuild the spare file it self hopping it will disappear
  */	
  sprintf(cmd,"storage_rebuild --nolog -t %d -s %d/%d -f %s --chunk %d --bstart %u --bstop %u -l 1 -fg -q",
           spare_restore_read_throughput,
	   fidCtx->data.key.cid, fidCtx->data.key.sid,  fidString,
	   fidCtx->data.key.chunk, 0, -1);
  ret    = system(cmd);
  result = WEXITSTATUS(ret);


  /*
  ** Update statistics
  */      
  if (result==0) {
    stspare_stat.rebuild_spare_success++;
  }     
  usleep(100000);

  /*
  ** Release context
  */      
  stspare_fid_cache_release(fidCtx);   
  return 0;     
}
/*
**____________________________________________________
**
** One spare file restoring
**
** @param fidCtx       The FID cache context of the spare file
**
** @retval 0 when FID context is completly processed
**         1 else
**____________________________________________________
*/
int stspare_restore_one_file(stspare_fid_cache_t * fidCtx) {
  char                           fidString[64];
  uint64_t                       sidBitMap;
  int                            ret;
   
  /*
  ** mtime is null which means that the spare file has to be read 
  ** and so is not ready to be processed
  */
  if (fidCtx->data.mtime == 0) return 1;  

  /*
  ** Get cluster bitmap that gives the list of available sid 
  ** in that cluster
  */
  sidBitMap = stspare_get_sid_bitmap(fidCtx->data.key.cid);

  rozofs_uuid_unparse(fidCtx->data.key.fid, fidString);
#if 0  
  info("%s chunk %d is to be restored on cid %d sid %d nb proj %d",
       fidString,
       fidCtx->data.key.chunk,
       fidCtx->data.key.cid,
       fidCtx->data.key.sid,
       fidCtx->data.nb_prj);
  for (idx=0; idx<STSPARE_SAFE_MAX; idx++) {
    if (fidCtx->data.prj_bitmap & (1<<idx)) {
      info("prj %d start %llu stop %llu", idx, 
	   (long long unsigned int)fidCtx->data.prj[idx].start,
	   (long long unsigned int)fidCtx->data.prj[idx].stop);
    }
  }
#endif  

  /*
  ** No projection is present in the spare file : it contains only holes
  */
  if (fidCtx->data.nb_prj == 0) {
    ret =  stspare_restore_hole(fidCtx, sidBitMap, fidString);
    stspare_current_working_file.cid = 0;
    return ret;
  }
    

  /*
  ** Some projections are present in the spare file. Try to rebuild them
  ** on the sid that should normaly have them.
  */
  ret = stspare_restore_projections(fidCtx, sidBitMap, fidString);
  stspare_current_working_file.cid = 0;
  return ret;
}
/*
**____________________________________________________
**
** Get time in micro seconds
**
** @param from  when set compute the delta in micro seconds from this time to now
**              when zero just return current time in usec
**____________________________________________________
*/
uint64_t stspare_get_us(uint64_t from) {
  struct timeval     timeDay;
  uint64_t           us;
  
  /*
  ** Get current time in us
  */
  gettimeofday(&timeDay,(struct timezone *)0); 
  us = ((unsigned long long)timeDay.tv_sec * 1000000 + timeDay.tv_usec);

  /*
  ** When from is given compute the delta
  */
  if (from) {
    us -= from;
  }  
  
  return us;	
}
/*
**____________________________________________________
**
** Spare file analyzing to know what should be rebuilt
**
** @param st       storage context
** @param fid      The FID of the spare file
** @param chunk    Chunk number of the spare FID
** @param pathname The file name
** @param now      Current time
**
**____________________________________________________
*/
stspare_fid_cache_t * stspare_scan_one_spare_file(
                              storage_t    * st,
                              fid_t          fid,
			      int            chunk,
			      char         * pathname,
			      time_t         now) {
			      
  int                           fd=-1;
  struct stat                   stats;  	
  int                           prj_size;
  int                           sizeRequested;
  int                           sizeRead;
  uint64_t                      offset;
  uint64_t                      nbBlocks;
  rozofs_stor_bins_hdr_t      * prj;  
  uint8_t                       fwd;
  int                           blk;
  int                           totalBlocks;
  int                           buffOff;
  stspare_fid_cache_t         * fidCtx = NULL;
  uint8_t                       prjId;
  uint64_t                      start;
  uint64_t                      loop_delay_us = 0;
  uint64_t                      total_delay_us=0;
  int64_t                       sleep_time_us;
  
  /*
  ** Lookup for the file in the FID cache
  */
  fidCtx = stspare_fid_cache_search(st->cid, st->sid, fid, chunk);

  /*
  ** Get the file modification time
  */
  if (stat(pathname,&stats) < 0) {
    /*
    ** File may have been deleted in the meantime
    */
    if (errno != ENOENT) {
      severe("fstat(%s) %s",pathname,strerror(errno));
    }
    /*
    ** Release context when one was allocated 
    */  
    goto release;
  }

  /*
  ** Only take care of the file when it is stable; not modified for a while
  */
  if (stats.st_mtime >= (now-(common_config.spare_restore_loop_delay*60))) {
    /*
    ** Release context when one was allocated 
    */  
    goto release;
  }     
  
  /*
  ** No FID context. Allocate one
  */
  if (fidCtx == NULL) {

    rozofs_stor_bins_file_hdr_t   file_hdr;
    STORAGE_READ_HDR_RESULT_E     read_hdr_res;

    /*
    ** Allocate a FID context
    */
    fidCtx = stspare_fid_cache_insert(st->cid, st->sid, fid, chunk);

    /*
    ** No more free context
    */
    if (fidCtx == NULL) goto release;

    /*
    ** Let's read the header files on disk
    */    
    read_hdr_res = storage_read_header_file(
            st,       // cid/sid context
            fid,      // FID we are looking for
	    1,        // Whether the storage is spare for this FID
	    &file_hdr, // Returned header file content
            0 );      // Update header file when not the same recycling value


    /*
    ** Error accessing all the devices  
    */
    if (read_hdr_res == STORAGE_READ_HDR_ERRORS) {
      /*
      ** Impossible to read the header files ????
      */
      severe("storage_read_header_file - %s - %s",pathname,strerror(errno));
      goto release;
    }

    if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) {
      /*
      ** Should we cleanup this file ???
      */
      warning("storage_read_header_file for %s not found",pathname);
      goto release;
    }
        
    /*
    ** Save interresting information
    */
    fidCtx->data.layout = file_hdr.layout;
    fidCtx->data.bsize  = file_hdr.bsize;
    int size2copy = sizeof(file_hdr.distrib);
    memcpy(fidCtx->data.dist,file_hdr.distrib,size2copy);
    memset(&fidCtx->data.dist[size2copy],0,sizeof(fidCtx->data.dist)-size2copy);
    fidCtx->data.mtime  = 0; /* This tells that the file not yet read */
  }        

  /*
  ** When mtime has not changed, no need to reread the file
  */
  if (fidCtx->data.mtime == stats.st_mtime) {
    /*
    ** Context is up to date
    */
    goto ok;
  }
  
  /*
  ** File has to be (re-)read
  */
  fidCtx->data.mtime = 0;  
  
  /*
  ** Open file
  */
  fd = open(pathname, O_RDONLY, S_IRWXU | S_IROTH);
  if (fd < 0) {
    /*
    ** File may have been deleted in the meantime
    */
    if (errno != ENOENT) {
      severe("open(%s) %s",pathname,strerror(errno));
    }
    goto release;
  }

  /*
  ** Let's read the file now
  */
  
  fidCtx->data.prj_bitmap = 0;
  fidCtx->data.nb_prj     = 0;
    
  /*
  ** Compute some info from what has been read in header file
  */     
  fwd = rozofs_get_rozofs_forward(fidCtx->data.layout);
  prj_size = rozofs_get_max_psize_in_msg(fidCtx->data.layout, fidCtx->data.bsize);
  
  /*
  ** Compute the exact number of blocks to read in a loop
  */
  sizeRequested = BUFFER_SIZE / prj_size;
  sizeRequested = sizeRequested * prj_size;

  /*
  ** Let's compute the time credit for each read loop
  */
  start = stspare_get_us(0);
  if (spare_restore_read_throughput == 0) {
    /*
    ** No throughput limitation
    */
    loop_delay_us = 0;
  }
  else {  
    loop_delay_us = (1000000 * BUFFER_SIZE_MB)/spare_restore_read_throughput;
  }
  
  offset        = 0;
  totalBlocks   = 0;
  while(1) {

    /*
    ** Read a full buffer  
    */        
    sizeRead = pread(fd,buffer,sizeRequested,offset);
    if (sizeRead < 0) {
      /*
      ** File may have been deleted in the meantime
      */
      if (errno != ENOENT) {
        severe("pread(%s,size=%d,offset=%d) %s",pathname,sizeRequested,(int)offset,strerror(errno));
      }
      goto release;
    }

    /*
    ** End of file
    */
    if (sizeRead < prj_size) break;

    /*
    ** Round to a number of projection
    */ 
    nbBlocks  = sizeRead / prj_size;
    sizeRead  = nbBlocks * prj_size;

    /*
    ** Loop on the read blocks
    */
    for (blk=0,buffOff=0; blk<nbBlocks; blk++,buffOff+=prj_size) {

      /*
      ** Projection header
      */
      prj = (rozofs_stor_bins_hdr_t *) &buffer[buffOff];

      /*
      ** In FID cahe context:
      ** prj[0]   describes the holes
      ** prj[idx] describes projection id (idx-1) that should be written in sid dist[idx-1] 
      */
      if (prj->s.timestamp == 0) {
        /* Just a hole in the file */
        prjId = 0;
      }
      else {
        /* projection id idx is storred in prj[idx+1] */
        prjId = prj->s.projection_id + 1;
      }
           
      if (prjId > fwd) {
	severe("%s block %d - Bad prj id %d",pathname,blk,prjId-1);
	continue;
      }
      
      /*
      ** Check projection id is not too big
      */
      if (prjId > STSPARE_SAFE_MAX) {
	severe("%s block %d - prj id %d >= STSPARE_SAFE_MAX(%d)",pathname,blk,prjId-1,STSPARE_SAFE_MAX);
	continue;
      }      

      /*
      ** Already encountered projection id. Update last block index
      */
      if (fidCtx->data.prj_bitmap & (1<<prjId)) {
        fidCtx->data.prj[prjId].stop = blk + totalBlocks;
      }
      /*
      ** New projection. Update first as well as last block index
      */
      else {
        if (prjId != 0) fidCtx->data.nb_prj++; /* An other projection but not a hole */
        fidCtx->data.prj_bitmap |= (1<<prjId); /* Set the projection bitmap */
        fidCtx->data.prj[prjId].start = blk + totalBlocks;
        fidCtx->data.prj[prjId].stop  = blk + totalBlocks;
      }
    }

    totalBlocks += nbBlocks;
    offset      += sizeRead;  
    
    /*
    ** Sleep between each loop for read throughput limitation 
    */
    total_delay_us += loop_delay_us;	
    sleep_time_us   = total_delay_us - stspare_get_us(start);
    if (sleep_time_us >= 10000) {
      usleep(sleep_time_us);
    }   
  }
  fidCtx->data.mtime = stats.st_mtime;
  goto ok;



release:
  /*
  ** release fid context on error
  */
  stspare_fid_cache_release(fidCtx);    
  fidCtx = NULL; 

  
ok:
  /*
  ** Close the file when open
  */
  if (fd != -1) {
    close(fd);
    fd = -1;  
  }
  return fidCtx;
}
/*
**____________________________________________________
**
** Try to restore the spare files pending in the list
**
**____________________________________________________
*/
void stspare_restore_pending_files() {
  stspare_fid_cache_t * fidCtx;
  ruc_obj_desc_t               * pnext;
   
  /*
  ** Loop on FID context
  */
  pnext = NULL; 

  while ((fidCtx = (stspare_fid_cache_t*) ruc_objGetNext(&stspare_fid_cache_running_list, &pnext)) != NULL)  {
    stspare_restore_one_file(fidCtx);
  }       
}
/*
**____________________________________________________
**
** Spare file scanning on disk
**
**____________________________________________________
*/
void stspare_scan_all_spare_files() {
  int             stidx;
  storage_t     * st;
  char            pathname[256];
  DIR *           sliceDir;
  int             slice;
  struct dirent * pep;  
  int             dev;
  fid_t           fid;
  int             chunk;
  time_t          now;
  stspare_fid_cache_t * fidCtx;
   
  st = storaged_storages;

  /*
  ** Loop on configured storages
  */    
  for (stidx=0; stidx < storaged_nrstorages; stidx++,st++) {

    /*
    ** Loop on devices
    */
    for (dev=0; dev < st->device_number; dev++) {

      /*
      ** Loop on every slice
      */
      for (slice=0; slice<common_config.storio_slice_number; slice++) {
      
        sprintf(pathname,"%s/%d/bins_1/%d",st->root, dev, slice);
        sliceDir = opendir(pathname);
        if (sliceDir == NULL) continue;

	now = time(NULL);

        /*
	** Loop on every file
	*/
        while ( (pep = readdir(sliceDir)) != NULL) {
	  char * pChar;
	  int    ret;
    	  /*
	  ** end of directory
	  */
    	  if (pep == NULL) break;
	  if (pep->d_name[0] == '.') continue;

	  pChar = pep->d_name;
	  ret = rozofs_uuid_parse(pChar,fid);
	  if (ret<0) {
	    //severe("rozofs_uuid_parse");
	    continue;
	  }
	  pChar += 38;
	  ret = sscanf(pChar,"%3d",&chunk);
          if (ret != 1) {
	    //severe("sscanf %d",ret);	    
	    continue;
	  }  

          /*
	  ** Let's scan this file if not yet done
	  */
          sprintf(pathname,"%s/%d/bins_1/%d/%s",st->root, dev, slice,pep->d_name);
	  fidCtx = stspare_scan_one_spare_file(st,fid,chunk,pathname,now);
	  if (fidCtx != NULL) {
	    /*
	    ** Try to restore the file
	    */
	    stspare_restore_one_file(fidCtx);
	  } 
	  
	  /*
	  ** When no more free context, stop scanning for the files
	  */
	  if (stspare_fid_cache_distributor_empty()) {
	    closedir(sliceDir);
	    return;
	  }   	  
        }
	closedir(sliceDir);
        /*
        ** Next slice
        */
        usleep(100000);  
      }
      
      /*
      ** Next device
      */
      sleep(2);        
    }
  }    
}
/*
**____________________________________________________
**
** Wait until export host name is defined
**
**____________________________________________________
*/
static void wait_until_ready_to_run() {
  int delay;
  
  /*
  ** Wait until export host name is defined in rozofs.conf
  */
  while (1) {

    /*
    ** read common config file
    */
    common_config_read(NULL);    
    
    /*
    ** Check export host name is defined
    */
    if ((strcmp(common_config.export_hosts,"")!=0)
    &&  (common_config.spare_restore_enable)) {
      break;
    }  
    
    /*
    ** Wait until it is defined
    */
    delay = 15*60;
    stspare_stat.seconds_before_next_run = time(NULL)+delay;
    sleep (delay);
  }
}  
/*
**____________________________________________________
**
** Spare file restoration thread 
**
**____________________________________________________
*/
void *stspare_thread(void *arg) {
     
  uma_dbg_thread_add_self("stspare");  
  
  /*
  ** 1rst delay not to start every process at the same time
  */
  int delay = random() % (60*common_config.spare_restore_loop_delay);
  stspare_stat.seconds_before_next_run = time(NULL)+delay;
  sleep(delay);
  
  while(1) {
       
    /*
    ** Re-read rozofs.conf and wait until export_hosts is defined
    */
    wait_until_ready_to_run();

    /*
    ** Reset SID bitmap on each loop
    */
    stspare_reset_sid_bitmap();
    
    /*
    ** Statistics 
    */
    stspare_stat.number_of_run_done++;
    stspare_stat.seconds_before_next_run = 0;
    
    /*
    ** Load throughput value from config
    */
    spare_restore_read_throughput = common_config.spare_restore_read_throughput;
     
    /*
    ** Loop on disk to find out the new spare files and
    ** try to restore them when possible
    */
    if (!stspare_fid_cache_distributor_empty()) {
      stspare_scan_all_spare_files();
    }   
    
    /*
    ** Try to rebuild the spare files pending in the list
    */
    stspare_restore_pending_files();    
       
    /*
    ** Sleep until next run
    */ 
    stspare_stat.seconds_before_next_run = time(NULL)+common_config.spare_restore_loop_delay*60;    
    sleep(common_config.spare_restore_loop_delay*60);    
  }
}
/*
**____________________________________________________
**
** Spare file restoration thread launching
**
**____________________________________________________
*/
void *stspare_thread_launch() {
  pthread_attr_t             attr;
  int                        err;
  pthread_t                  thread;

  /*
  ** Add a debug topic
  */
  uma_dbg_addTopic("spare", stspare_debug); 
  uma_dbg_addTopic("throughput", stspare_set_throughput); 

  /*
  ** Initialize FID cache
  */
  stspare_fid_cache_init(common_config.spare_restore_spare_ctx);
  
  err = pthread_attr_init(&attr);
  if (err != 0) {
    fatal("pthread_attr_init() %s",strerror(errno));
  }  

  err = pthread_create(&thread,&attr,stspare_thread,NULL);
  if (err != 0) {
    fatal("pthread_create() %s",strerror(errno));
  }  

  return 0;
}
/*
**____________________________________________________
**
** The famous ruc_init
**
** @param    debug_port    The rozodiag listening port
**____________________________________________________
*/
uint32_t ruc_init(uint16_t debug_port) {
    int ret = RUC_OK;


  uint32_t mx_tcp_client = 2;
  uint32_t mx_tcp_server = 8;
  uint32_t mx_tcp_server_cnx = 10;
  uint32_t local_ip = INADDR_ANY;
  uint32_t        mx_af_unix_ctx = ROZO_AFUNIX_CTX_STORAGED;

  /*
  ** init of the system ticker
  */
  rozofs_init_ticker();
  /*
  ** trace buffer initialization
  */
  ruc_traceBufInit();

  /*
  ** initialize the socket controller:
  **   for: NPS, Timer, Debug, etc...
  */
  ret = ruc_sockctl_init(ROZO_SOCKCTRL_CTX_STORAGED);
  if (ret != RUC_OK) {
    fatal( " socket controller init failed" );
  }

  /*
  **  Timer management init
  */
  ruc_timer_moduleInit(FALSE);

  while (1) {
    /*
     **--------------------------------------
     **  configure the number of TCP connection
     **  supported
     **--------------------------------------   
     **  
     */
    ret = uma_tcp_init(mx_tcp_client + mx_tcp_server + mx_tcp_server_cnx);
    if (ret != RUC_OK) {
      break;
    }

    /*
     **--------------------------------------
     **  configure the number of TCP server
     **  context supported
     **--------------------------------------   
     **  
     */
    ret = ruc_tcp_server_init(mx_tcp_server);
    if (ret != RUC_OK) {
      break;
    }
    /*
    **--------------------------------------
    **  configure the number of AF_UNIX/AF_INET
    **  context supported
    **--------------------------------------   
    **  
    */    
    ret = af_unix_module_init(mx_af_unix_ctx,
                              2,1024*1, // xmit(count,size)
                              2,1024*1 // recv(count,size)
                              );
    if (ret != RUC_OK) break;   
	    /*
     **--------------------------------------
     **   D E B U G   M O D U L E
     **--------------------------------------
     */

    if (pHostArray[0] != NULL) {
      int idx=0;
      while (pHostArray[idx] != NULL) {
	rozofs_host2ip(pHostArray[idx], &local_ip);
	uma_dbg_init(10, local_ip, debug_port);	    
	idx++;
      }  
    }
    else {
      local_ip = INADDR_ANY;
      uma_dbg_init(10, local_ip, debug_port);
    }  


    {
      char name[256];
      if (pHostArray[0] == 0) {
	sprintf(name, "stspare");
      }
      else {
        sprintf(name, "stspare %s", pHostArray[0]);
      }  
      uma_dbg_set_name(name);
    }

#if 0
    /*
    ** RPC SERVER MODULE INIT
    */
    ret = rozorpc_srv_module_init();
    if (ret != RUC_OK) break;  
#endif	

    break;
  }
  return ret;
}
/*
**____________________________________________________
**
** Initialize storages from configuration
**
**____________________________________________________
*/
static int storages_initialize() {
  list_t *p = NULL;
  int     first=1;

  /* Initialize rozofs constants (redundancy) */
  rozofs_layout_initialize();

  storaged_nrstorages = 0;

  /* For each storage on configuration file */
  list_for_each_forward(p, &storaged_config.storages) {
      storage_config_t *sc = list_entry(p, storage_config_t, list);
       
      if (first) {
        /*
	** Prepare the seed for the random sleep to try to desynchronize the threads
	*/
	srandom(sc->cid*10+sc->sid);
	first = 0;
      }
      
      /* Initialize the storage */
      if (storage_initialize(storaged_storages + storaged_nrstorages++,
              sc->cid, sc->sid, sc->root,
	      sc->device.total,
	      sc->device.mapper,
	      sc->device.redundancy,
              sc->spare_mark) != 0) {
          severe("can't initialize storage (cid:%d : sid:%d) with path %s",
                  sc->cid, sc->sid, sc->root);
          return -1;
      }
  }
  return 0;
}
/*
**____________________________________________________
**
** End of process
**
**____________________________________________________
*/
static void on_stop() {}
/*
**____________________________________________________
**
** Init of spare restorer
**
**____________________________________________________
*/
static void on_start() {
  int  debug_port;
  int  ret;

  /*
  ** read common config file
  */
  common_config_read(NULL);    

  /*
  ** Initialize the list of storage config
  */
  sconfig_initialize(&storaged_config);

  /*
  ** Read the configuration file
  */
  if (sconfig_read(&storaged_config, storaged_config_file,0) != 0) {
    fatal("Failed to parse storage configuration file: %s.\n",strerror(errno));
  }

  /*
  ** Check the configuration
  */
  if (sconfig_validate(&storaged_config) != 0) {
    fatal("Inconsistent storage configuration file: %s.\n",strerror(errno));
  }

  /*
  ** Initialization of the local storage configuration
  */
  if (storages_initialize() != 0) {
    fatal("can't initialize storaged: %s.", strerror(errno));
  }

  /* Try to get debug port from /etc/services */    
  debug_port = rozofs_get_service_port_stspare_diag();
  ret = ruc_init(debug_port);
  if (ret != RUC_OK) {
    /*
    ** fatal error
    */
    fatal("ruc_init() can't initialize storaged non blocking thread");
  }

  /*
  ** Start spare restorer thread
  */
  stspare_thread_launch();


  if (pHostArray[0] != NULL) {
    info("storaged spare restorer started (host: %s, dbg port: %d).",
            (pHostArray[0]==NULL)?"":pHostArray[0], debug_port);
  } else {
    info("storaged spare restorer started (dbg port: %d).", debug_port);
  }

  /*
   ** main loop
   */
  while (1) {
      ruc_sockCtrl_selectWait();
  }
  fatal("Exit from ruc_sockCtrl_selectWait()");
}
/*-----------------------------------------------------------------------------
**
**  Display usage
**
**----------------------------------------------------------------------------
*/
char * utility_name=NULL;
void usage(char * fmt, ...) {
  va_list   args;
  char      error_buffer[512];
  
  /*
  ** Display optionnal error message if any
  */
  if (fmt) {
    va_start(args,fmt);
    vsprintf(error_buffer, fmt, args);
    va_end(args);   
    severe("%s",error_buffer);
    printf("%s\n",error_buffer);
  }
  
  /*
  ** Display usage
  */
  printf("RozoFS Storaged spare restorer - %s\n", VERSION);
  printf("Usage: %s [OPTIONS]\n\n",utility_name);
  printf("   -h, --help\t\t\tprint this message.\n");
  printf("   -H, --host=storaged-host\tspecify the hostname to use for build pid name (default: none).\n");
  printf("   -c, --config=config-file\tspecify config file to use (default: %s).\n", STORAGED_DEFAULT_CONFIG);  
  
  if (fmt) exit(EXIT_FAILURE);
  exit(EXIT_SUCCESS); 
}
/*-----------------------------------------------------------------------------
**
**  M A I N
**
**----------------------------------------------------------------------------
*/
int main(int argc, char *argv[]) {
  int c;
  char pid_name[256];
  static struct option long_options[] = {
      { "help", no_argument, 0, 'h'},
      { "config", required_argument, 0, 'c'},
      { "host", required_argument, 0, 'H'},
      { 0, 0, 0, 0}
  };

  /*
  ** Change local directory to "/"
  */
  if (chdir("/")!= 0) {}

  uma_dbg_thread_add_self("Main");

  uma_dbg_record_syslog_name("stspare");

  // Init of the timer configuration
  rozofs_tmr_init_configuration();
  
  // Read common config
  common_config_read(NULL);    
  spare_restore_read_throughput = common_config.spare_restore_read_throughput;
  
  while (1) {

    int option_index = 0;
    c = getopt_long(argc, argv, "hmc:H:", long_options, &option_index);

    if (c == -1) break;

    switch (c) {
      case 'h':
        usage(NULL);
        break;
	
      case 'c':
        if (!realpath(optarg, storaged_config_file)) {
          severe("storaged failed: %s %s\n", optarg, strerror(errno));
          exit(EXIT_FAILURE);
        }
        break;
	
      case 'H':
	parse_host_name(optarg);
        break;
	
      case '?':
        usage(NULL);
        break;
	
      default:
        usage("Unexpected option \'%c\'",c);
        break;
    }
  }

  /*
  ** Prepare PID file name
  */
  char *pid_name_p = pid_name;
  if (pHostArray[0] != NULL) {
      char * pChar = pid_name_p;
      pChar += rozofs_string_append(pChar,STSPARE_PID_FILE);
      *pChar++ = '_'; 
      pChar += rozofs_string_append(pChar,pHostArray[0]);
      pChar += rozofs_string_append(pChar,".pid");	
  } else {
      char * pChar = pid_name_p;
      pChar += rozofs_string_append(pChar,STSPARE_PID_FILE);
      pChar += rozofs_string_append(pChar,".pid");	
  }

  /*
  ** Start without deamonizing
  */
  no_daemon_start("stspare", common_config.nb_core_file, 
                   pid_name, on_start, on_stop, NULL);

  closelog();
  exit(EXIT_FAILURE);
}
