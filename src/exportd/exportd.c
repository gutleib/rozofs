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
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <getopt.h>
#include <libconfig.h>
#include <limits.h>
#include <inttypes.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>
#include <rpc/pmap_clnt.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/common/daemon.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/common_config.h>
#include <rozofs/rpc/export_profiler.h>
#include <rozofs/common/profile.h>
#include <rozofs/rpc/eproto.h>
#include <rozofs/rpc/epproto.h>
#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/rpc/gwproto.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/rpc/export_profiler.h>
#include <rozofs/common/profile.h>
#include <rozofs/common/rozofs_site.h>
#include <rozofs/rpc/rpcclt.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/rozo_launcher.h>
#include <rozofs/core/rozofs_core_files.h>
#include <rozofs/core/af_unix_socket_generic.h>

#include <rozofs/core/rozofs_cpu.h>
#include "config.h"
#include "exportd.h"
#include "export.h"
#include "monitor.h"
#include "econfig.h"
#include "volume.h"
#include "export_expgw_conf.h"
#include "export_internal_channel.h"
#include "export_share.h"
#include "geo_profiler.h"
#define EXPORTD_PID_FILE "exportd.pid"
/* Maximum open file descriptor number for exportd daemon */
#define EXPORTD_MAX_OPEN_FILES 5000

extern econfig_t exportd_config;
pthread_rwlock_t config_lock;
export_reload_conf_status_t export_reload_conf_status;
int export_instance_id;    /**< instance id of the export  : 0 is the master   */
int export_master;         /**< assert to 1 for export Master                  */

lv2_cache_t cache;
int export_local_site_number = 0;
int rozofs_no_site_file;
typedef struct export_entry {
    export_t export;
    list_t list;
} export_entry_t;

static list_t exports;
static pthread_rwlock_t exports_lock;

typedef struct volume_entry {
    volume_t volume;
    list_t list;
} volume_entry_t;

static list_t volumes;
static pthread_rwlock_t volumes_lock;

static pthread_t bal_vol_thread=0;
static pthread_t monitor_thread=0;
static pthread_t exp_tracking_thread=0;
static pthread_t geo_poll_thread=0;

static char exportd_config_file[PATH_MAX] = EXPORTD_DEFAULT_CONFIG;

static SVCXPRT *exportd_svc = NULL;

extern void export_program_1(struct svc_req *rqstp, SVCXPRT * ctl_svc);

DEFINE_PROFILING(epp_profiler_t);


exportd_start_conf_param_t  expgwc_non_blocking_conf;  /**< configuration of the non blocking side */
exp_trk_th_stats_t  *exp_trk_th_stats_p = NULL; /**< tracking thread statistis  */
gw_configuration_t  expgw_conf;

uint32_t expgw_eid_table[EXPGW_EID_MAX_IDX];
gw_host_conf_t expgw_host_table[EXPGW_EXPGW_MAX_IDX];

uint32_t export_configuration_file_hash = 0;  /**< hash value of the configuration file */
export_one_profiler_t  * export_profiler[EXPGW_EID_MAX_IDX+1] = { 0 };
uint32_t export_profiler_eid;

/*
** rmbins thread context tabel
*/
rmbins_thread_t rmbins_thread[ROZO_NB_RMBINS_THREAD];

/*
 *_______________________________________________________________________
 */
/**
*   Get the configuration file of the exportd

    @param none
    
    @retval: pointer to the exportd configuration file (full path)
*/
char *export_get_config_file_path()
{
  return exportd_config_file;
}
/*
 *_______________________________________________________________________
 */
/**
*   export slave launcher pid file

   @param slaveid: slave identifier 
  
   @retval none
*/
static inline void export_slave_launcher_pid_file(char * pidfile, int slaveid) {
  sprintf(pidfile,ROZOFS_RUNDIR_PID"launcher_exportd_slave_%d.pid",slaveid);
}
/*
 *_______________________________________________________________________
 */
/**
*   kill of a slave exportd process

  @param instance: instance id of the exportd process
  
   @retval none
*/
void export_kill_one_export_slave(int instance) {
    int ret = -1;
    char pidfile[128];
    
    export_slave_launcher_pid_file(pidfile,instance);
    	  
    // Launch exportd slave
    ret = rozo_launcher_stop(pidfile);
    if (ret !=0) {
      severe("rozo_launcher_stop(%s) %s",pidfile, strerror(errno));
    }
}

/*
 *_______________________________________________________________________
 */
/**
*   start of a slave exportd process

  @param instance: instance id of the exportd process
  
   @retval none
*/
void export_start_one_export_slave(int instance) {
    char cmd[1024];
    uint16_t debug_port_value;
    char   pidfile[128];
    int ret = -1;
           
    char *cmd_p = &cmd[0];
    cmd_p += sprintf(cmd_p, "%s ", "exportd");
    cmd_p += sprintf(cmd_p, "-i %d ", instance);
    cmd_p += sprintf(cmd_p, "-s ");
    cmd_p += sprintf(cmd_p, "-c %s ", exportd_config_file);
    
    /* Try to get debug port from /etc/services */
    debug_port_value = rozofs_get_service_port_export_slave_diag(instance);

    cmd_p += sprintf(cmd_p, "-d %d ",debug_port_value );
          
    export_slave_launcher_pid_file(pidfile,instance);

    // Launch exportd slave
    ret = rozo_launcher_start(pidfile, cmd);
    if (ret !=0) {
      severe("rozo_launcher_start(%s,%s) %s",pidfile, cmd, strerror(errno));
      return;
    }
    
    info("start exportd slave (instance: %d, config: %s,"
            " profile port: %d).",
            instance,  exportd_config_file,
            debug_port_value);
}
/*
 *_______________________________________________________________________
 */
/**
*   rebalancer launcher pid file

   @param vid: volume identifier of the rebalancer
  
   @retval none
*/
static inline void export_rebalancer_pid_file(char * pidfile, int vid) {
  sprintf(pidfile,ROZOFS_RUNDIR_PID"launcher_rebalance_vol%d.pid",vid);
}
/*
 *_______________________________________________________________________
 */
/**
*   stop a rebalancer

   @param vid: volume identifier of the rebalancer
   @param cfg: rebalancer configuration file name
  
   @retval none
*/
void export_stop_one_rebalancer(int vid) {
  char   pidfile[256];
  int    ret = -1;

  export_rebalancer_pid_file(pidfile,vid);

  // Launch exportd slave
  ret = rozo_launcher_stop(pidfile);
  if (ret !=0) {
    severe("rozo_launcher_stop(%s) %s",pidfile, strerror(errno));
    return;
  }
}

/*
 *_______________________________________________________________________
 */
/**
*   start a rebalancer

   @param vid: volume identifier of the rebalancer
   @param cfg: rebalancer configuration file name
  
   @retval none
*/
void export_start_one_rebalancer(int vid, char * cfg) {
  char cmd[1024];
  char pidfile[256];
  int  ret = -1;

  char *cmd_p = &cmd[0];
  cmd_p += sprintf(cmd_p, "%s ", "rozo_rebalance --cont");
  cmd_p += sprintf(cmd_p, "--volume %d ", vid);
  cmd_p += sprintf(cmd_p, "--cfg %s ", cfg);
  cmd_p += sprintf(cmd_p, "--config %s ", exportd_config_file);

  export_rebalancer_pid_file(pidfile,vid);

  // Launch exportd slave
  ret = rozo_launcher_start(pidfile, cmd);
  if (ret !=0) {
    severe("rozo_launcher_start(%s,%s) %s",pidfile, cmd, strerror(errno));
    return;
  }

  info("start vid %d rebalancer (%s)", vid, cfg);
}
/*
 *_______________________________________________________________________
 */
/**
*   start every rebalancer for volumes configured in automatic mode
  
   @retval none
*/
void export_rebalancer(int start) {
  volume_entry_t  * pvol;
  struct timespec ts = {3, 0};

  for (;;) {
    list_t *p;

    if ((errno = pthread_rwlock_tryrdlock(&volumes_lock)) != 0) {
     warning("can lock volumes, balance_volume_thread deferred.");
     nanosleep(&ts, NULL);
     continue;
    }

    list_for_each_forward(p, &volumes) {
      pvol = (volume_entry_t * ) list_entry(p, volume_entry_t, list);
      if (pvol->volume.rebalanceCfg) {
        if (start) {
          export_start_one_rebalancer(pvol->volume.vid,pvol->volume.rebalanceCfg);
        }
        else {
          export_stop_one_rebalancer(pvol->volume.vid);
        }  
      }
    }

    if ((errno = pthread_rwlock_unlock(&volumes_lock)) != 0) {
      severe("can unlock volumes, potential dead lock.");
    }
    
    break;
  }
}
/*
 *_______________________________________________________________________
 */
/**
*   start a remove server for storage node to execute some commands
  
   @retval none
*/
void export_rcmd_server(void) {
  char   pidfile[128];
  int    ret = -1;

  sprintf(pidfile,ROZOFS_RUNDIR_PID"launcher_rcmd_server.pid");
  
  // Launch exportd slave
  ret = rozo_launcher_start(pidfile, "rozo_rcmd_server");
  if (ret !=0) {
    severe("rozo_launcher_start(%s,%s) %s",pidfile, "rozo_rcmd_server", strerror(errno));
    return;
  }

  info("start rcmd server");
}
/*
 *_______________________________________________________________________
 */
/**
*   start of a slave exportd process

  @param instance: instance id of the exportd process
  
   @retval none
*/
void export_reload_one_export_slave(int instance) {
    char cmd[1024];
    uint16_t debug_port_value;
    char   pidfile[128];
    int ret = -1;
           
    char *cmd_p = &cmd[0];
    cmd_p += sprintf(cmd_p, "%s ", "exportd");
    cmd_p += sprintf(cmd_p, "-i %d ", instance);
    cmd_p += sprintf(cmd_p, "-s ");
    cmd_p += sprintf(cmd_p, "-c %s ", exportd_config_file);
    
    /* Try to get debug port from /etc/services */
    debug_port_value = rozofs_get_service_port_export_slave_diag(instance);

    cmd_p += sprintf(cmd_p, "-d %d ",debug_port_value );
          
    export_slave_launcher_pid_file(pidfile,instance);
//    sprintf(pidfile,"/var/run/launcher_exportd_slave_%d.pid",instance);

    // Launch exportd slave
    ret = rozo_launcher_reload(pidfile);
    if (ret !=0) {
      severe("rozo_launcher_reload(%s,%s) %s",pidfile, cmd, strerror(errno));
      return;
    }
    
    info("reload exportd slave (instance: %d, config: %s,"
            " profile port: %d).",
            instance,  exportd_config_file,
            debug_port_value);
}


/*
 *_______________________________________________________________________
 */
 /**
 *  starting of all the slave exportd
 
 @param none
 retval none
*/
void export_kill_all_export_slave() {
    int i;

    for (i = 1; i <= EXPORT_SLICE_PROCESS_NB; i++) {
      export_kill_one_export_slave(i);
    }
}

/*
 *_______________________________________________________________________
 */
/**
*  slave exportd starts: that happens upon master exportd starting
*/

void export_start_export_slave() {
	int i;
        
    /*
    ** Kill every rozolauncher of slave export just in case
    */    
    if (system("for pid in `ps -ef | grep \"exportd -i\" | grep rozolauncher | grep -v grep | awk '{print $2}'`; do kill $pid; done")){};

    usleep(10000);
    
    /*
    ** Kill every exportd slave just in case
    */
    if (system("for pid in `ps -ef | grep \"exportd -i\" | grep -v rozolauncher | grep -v grep | awk '{print $2}'`; do kill $pid; done")){};

    for (i = 1; i <= EXPORT_SLICE_PROCESS_NB; i++) { 
      export_start_one_export_slave(i);
    }
}

/*
 *_______________________________________________________________________
 */
/**
*  slave exportd reload: that happens upon a change in the configuration
*/
void export_reload_all_export_slave() {
    int i;

    for (i = 1; i <= EXPORT_SLICE_PROCESS_NB; i++) {
      export_reload_one_export_slave(i);
    }
}


/*
 *_______________________________________________________________________
 */
/**
*  compute the hash of a configuration file

   @param : full pathname of the file
   @param[out]: pointer to the array where hash value is returned

   @retval 0 on success
   @retval -1 on error
*/
int hash_file_compute(char *path,uint32_t *hash_p)
{
  uint32_t hash=0;
  uint8_t c;

  FILE *fp = fopen( path,"r");
  if (fp == NULL)
  {
    return -1;
  }
  while (!feof(fp) && !ferror(fp))
  {
    c = fgetc(fp);
    hash = c + (hash << 6) + (hash << 16) - hash;
  }
  *hash_p = hash;
  fclose(fp);
  return 0;
}


/*
 *_______________________________________________________________________
 */

void expgw_init_configuration_message(char *exportd_hostname)
{
  gw_configuration_t *expgw_conf_p = &expgw_conf;
  expgw_conf_p->eid.eid_val = expgw_eid_table;
  expgw_conf_p->eid.eid_len = 0;
  expgw_conf_p->exportd_host = malloc(ROZOFS_HOSTNAME_MAX + 1);
  strcpy(expgw_conf_p->exportd_host, exportd_hostname);
  memset(expgw_conf_p->eid.eid_val, 0, sizeof(expgw_eid_table));
  expgw_conf_p->gateway_host.gateway_host_val = expgw_host_table;
  memset(expgw_conf_p->gateway_host.gateway_host_val, 0, sizeof(expgw_host_table));
  int i;
  for (i = 0; i < EXPGW_EXPGW_MAX_IDX; i++)
  {
    expgw_host_table[i].host = malloc( ROZOFS_HOSTNAME_MAX+1);
  }
  expgw_conf_p->gateway_host.gateway_host_len = 0;
}


/*
 *_______________________________________________________________________
 */
void expgw_reinit_configuration_message()
{
  gw_configuration_t  *expgw_conf_p = &expgw_conf;
  expgw_conf_p->eid.eid_val = expgw_eid_table;
  expgw_conf_p->eid.eid_len = 0;
  expgw_conf_p->gateway_host.gateway_host_val = expgw_host_table;
  expgw_conf_p->gateway_host.gateway_host_len = 0;
}

/*
 *_______________________________________________________________________
 */
int expgw_build_configuration_message(char * pchar, uint32_t size)
{
    list_t *iterator;
    list_t *iterator_expgw;
    DEBUG_FUNCTION;

    gw_configuration_t *expgw_conf_p = &expgw_conf;

    expgw_reinit_configuration_message();

    expgw_conf_p->eid.eid_len = 0;
    expgw_conf_p->exportd_port = 0;
    expgw_conf_p->gateway_port = 0;

    // lock the exportd configuration while searching for the eid handled by the export daemon
    if ((errno = pthread_rwlock_rdlock(&exports_lock)) != 0)
    {
        severe("can't lock exports.");
        return -1;
    }

    list_for_each_forward(iterator, &exports)
    {
       export_entry_t *entry = list_entry(iterator, export_entry_t, list);
       expgw_eid_table[expgw_conf_p->eid.eid_len] = entry->export.eid;
       expgw_conf_p->eid.eid_len++;
    }

    // unlock exportd config
    if ((errno = pthread_rwlock_unlock(&exports_lock)) != 0)
    {
        severe("can't unlock exports, potential dead lock.");
        return -1;
    }

/*
    if (expgw_conf_p->eid.eid_len == 0)
    {
      severe("no eid in the exportd configuration !!");
      return -1;
    }
*/
    /*
    ** now go through the exportd gateway configuration
    */
    if ((errno = pthread_rwlock_rdlock(&expgws_lock)) != 0)
    {
        severe("can't lock expgws.");
        return -1;
    }
    expgw_conf_p->hdr.export_id = 0;
    expgw_conf_p->hdr.nb_gateways = 0;
    expgw_conf_p->hdr.gateway_rank = 0;
    expgw_conf_p->hdr.configuration_indice = export_configuration_file_hash;

    list_for_each_forward(iterator, &expgws)
    {
        expgw_entry_t *entry = list_entry(iterator, expgw_entry_t, list);
        expgw_conf_p->hdr.export_id = entry->expgw.daemon_id;
        expgw_t *expgw = &entry->expgw;

        // loop on the storage
        list_for_each_forward(iterator_expgw, &expgw->expgw_storages) {
          expgw_storage_t *entry = list_entry(iterator_expgw, expgw_storage_t, list);
          // copy the hostname
          strcpy((char*)expgw_host_table[expgw_conf_p->gateway_host.gateway_host_len].host, entry->host);
          expgw_conf_p->gateway_host.gateway_host_len++;
          expgw_conf_p->hdr.nb_gateways++;
        }
    }

    if ((errno = pthread_rwlock_unlock(&expgws_lock)) != 0)
    {
        severe("can't unlock expgws, potential dead lock.");
        return -1;
    }

    XDR xdrs;
    int total_len = -1;

    xdrmem_create(&xdrs,(char*)pchar,size,XDR_ENCODE);

    if (xdr_gw_configuration_t(&xdrs,expgw_conf_p) == FALSE){
        severe("encoding error");
    }else{
        total_len = xdr_getpos(&xdrs) ;
    }
    return total_len;
}

static void *balance_volume_thread(void *v) {
    struct timespec ts = {3, 0};

    uma_dbg_thread_add_self("Volume balance");

    
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

    for (;;) {
        list_t *p;

        if ((errno = pthread_rwlock_tryrdlock(&volumes_lock)) != 0) {
            warning("can lock volumes, balance_volume_thread deferred.");
            nanosleep(&ts, NULL);
            continue;
        }

        list_for_each_forward(p, &volumes) {
            volume_balance(&list_entry(p, volume_entry_t, list)->volume);
        }

        if ((errno = pthread_rwlock_unlock(&volumes_lock)) != 0) {
            severe("can unlock volumes, potential dead lock.");
        }

        nanosleep(&ts, NULL);
		
        /* In round robin mode less frequent polling is needed */    
        if (common_config.file_distribution_rule == rozofs_file_distribution_size_balancing) {
          if (ts.tv_sec < 6) ts.tv_sec++;
        }  
        else if (common_config.file_distribution_rule == rozofs_file_distribution_weigthed_round_robin) {
    	  if (ts.tv_sec < 10) ts.tv_sec++;
        }  
        else {
          if (ts.tv_sec < 12) ts.tv_sec++;
        }
    }
    return 0;
}  
/*
 *_______________________________________________________________________
 */
/** Thread for remove bins files on storages for each exports
 */
static void *remove_bins_thread(void *v) {
  pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
  list_t          * iterator = NULL;
  int               export_idx = 0;
  char              thName[16];
  rmbins_thread_t * thCtx;
  uint64_t          before,after,credit;
  uint64_t          processed;

  thCtx = (rmbins_thread_t *) v;

  /*
  ** Record thread name
  */
  sprintf(thName, "Trash%d", thCtx->idx);    
  uma_dbg_thread_add_self(thName);

  // Pointer for store re-start bucket index for each exports
  uint16_t * bucket_idx = xmalloc(list_size(&exports) * sizeof (uint16_t));
  // Init. each index to 0
  memset(bucket_idx, 0, sizeof (uint16_t) * list_size(&exports));

  /*
  ** Thread time shifting.
  ** Thread 0 do not sleep.
  ** Thread 1 sleeps 2 sec. 
  ** Thread 2 sleeps 4 sec.
  ** ...
  */ 
  sleep(thCtx->idx * 2);

  for (;;) {


    if (thCtx->idx >= common_config.nb_trash_thread) {
      usleep(RM_BINS_PTHREAD_FREQUENCY_SEC*1000000*ROZO_NB_RMBINS_THREAD);
      continue;
    }  

    export_idx = 0;
    processed  = 0;

    /*
    ** One run time credit
    */
    credit = RM_BINS_PTHREAD_FREQUENCY_SEC*1000000*common_config.nb_trash_thread;

    /*
    ** Read ticker before loop
    */
    GETMICROLONG(before);

    list_for_each_forward(iterator, &exports) {
        export_entry_t *entry = list_entry(iterator, export_entry_t, list);

        // Remove bins file starting with specific bucket idx
        processed += export_rm_bins(&entry->export, &bucket_idx[export_idx], thCtx);
        export_idx++;
    }

    /*
    ** Read ticker after loop
    */
    GETMICROLONG(after); 
    thCtx->total_usec  += thCtx->last_usec;
    thCtx->total_count += thCtx->last_count;

    thCtx->last_usec  = after - before;
    thCtx->last_count = processed;

    thCtx->nb_run++; 

    /*
    ** Compte credit for a loop 
    */
    if (thCtx->last_usec < credit) {
      uint64_t delay;
      /*
      ** The loop did last less than the credit
      ** Wait a little...
      */ 
      delay = credit - thCtx->last_usec;
      usleep(delay);
    }  
  }
  return 0;
}
/*
**_______________________________________________________________________
**
** Start every required trash thread
**
*/
static void start_all_remove_bins_thread() {
  int idx;
  
  for (idx=0; idx < ROZO_NB_RMBINS_THREAD; idx++) {
  
    rmbins_thread[idx].idx  = idx;
    rmbins_thread[idx].thId = 0;
    
    if ((errno = pthread_create(&rmbins_thread[idx].thId, NULL, remove_bins_thread, &rmbins_thread[idx])) != 0) {
      fatal("can't create remove files thread %d %s", idx, strerror(errno));  
    }	
  }
}
/*
**_______________________________________________________________________
**
** Stop every started trash thread
**
*/
static void stop_all_remove_bins_thread() {
  int idx;
  
  for (idx=0; idx < ROZO_NB_RMBINS_THREAD; idx++) {
    if (rmbins_thread[idx].thId) {
      // Canceled the remove bins pthread before reload list of exports
      if ((errno = pthread_cancel(rmbins_thread[idx].thId)) != 0) {
        severe("can't canceled remove bins pthread %d : %s", idx, strerror(errno));
      }  
    }
    rmbins_thread[idx].thId = 0;
  }
}  
/*
**_______________________________________________________________________
**
**
*/
/**
*  tracking thread parameters and statistics
*/
uint64_t export_tracking_poll_stats[2];   /**< statistics  */
int      export_tracking_thread_period_count;   /**< current period in seconds  */
#define START_PROFILING_TH(the_probe)\
    uint64_t tic, toc;\
    struct timeval tv;\
    {\
        the_probe[P_COUNT]++;\
        gettimeofday(&tv,(struct timezone *)0);\
        tic = MICROLONG(tv);\
    }

#define STOP_PROFILING_TH(the_probe)\
    {\
        gettimeofday(&tv,(struct timezone *)0);\
        toc = MICROLONG(tv);\
        the_probe[P_ELAPSE] += (toc - tic);\
    }



static char * show_tracking_thread_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"trk_thread reset                : reset statistics\n");
  pChar += sprintf(pChar,"trk_thread period [ <period> ]  : change thread period(unit is minutes)\n");  
  return pChar; 
}
/*
**_______________________________________________________________
*/
char * show_stacking_thread_stats_display(char *pChar)
{
     /*
     ** display the statistics of the thread
     */
     exp_trk_th_stats_t *stats_p ;
     
     pChar += sprintf(pChar,"period     : %d minute(s) \n",export_tracking_thread_period_count);
     pChar += sprintf(pChar,"statistics :\n");
     pChar += sprintf(pChar," - allocated memory  :%llu MB (%llu Bytes)\n",(long long unsigned int)exp_trk_malloc_size/(1024*1024),
                                                    (long long unsigned int)exp_trk_malloc_size);
     stats_p = &exp_trk_th_stats_p[ROZOFS_REG];
     pChar += sprintf(pChar," - regular attr. :%llu/%llu\n",
                (unsigned long long int)stats_p->counter[TRK_TH_INODE_DEL_STATS],		
                (unsigned long long int)stats_p->counter[TRK_TH_INODE_TRUNC_STATS]);		
     stats_p = &exp_trk_th_stats_p[ROZOFS_DIR];
     pChar += sprintf(pChar," - directories   :%llu/%llu\n",
                (unsigned long long int)stats_p->counter[TRK_TH_INODE_DEL_STATS],		
                (unsigned long long int)stats_p->counter[TRK_TH_INODE_TRUNC_STATS]);	
     stats_p = &exp_trk_th_stats_p[ROZOFS_EXTATTR];
     pChar += sprintf(pChar," - extended attr.:%llu/%llu\n",
                (unsigned long long int)stats_p->counter[TRK_TH_INODE_DEL_STATS],		
                (unsigned long long int)stats_p->counter[TRK_TH_INODE_TRUNC_STATS]);	
     stats_p = &exp_trk_th_stats_p[ROZOFS_SLNK];
     pChar += sprintf(pChar," - symbolic links:%llu/%llu\n",
                (unsigned long long int)stats_p->counter[TRK_TH_INODE_DEL_STATS],		
                (unsigned long long int)stats_p->counter[TRK_TH_INODE_TRUNC_STATS]);	

     pChar += sprintf(pChar," - activation counter:%llu\n",
                (unsigned long long int)export_tracking_poll_stats[P_COUNT]);
     pChar += sprintf(pChar," - average time (us) :%llu\n",
                      (unsigned long long int)(export_tracking_poll_stats[P_COUNT]?
		      export_tracking_poll_stats[P_ELAPSE]/export_tracking_poll_stats[P_COUNT]:0));
     pChar += sprintf(pChar," - total time (us)   :%llu\n",  (unsigned long long int)export_tracking_poll_stats[P_ELAPSE]);
     return pChar;


}
/*
**_______________________________________________________________
*/
void show_tracking_thread(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int ret;
    int period;
    
    
    if (argv[1] == NULL) {
      show_stacking_thread_stats_display(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer()); 
      return;  	  
    }

    if (strcmp(argv[1],"reset")==0) {
      pChar = show_stacking_thread_stats_display(pChar);
      pChar +=sprintf(pChar,"\nStatistics have been cleared\n");
      export_tracking_poll_stats[0] = 0;
      export_tracking_poll_stats[1] = 0;
      memset(exp_trk_th_stats_p,0, sizeof(exp_trk_th_stats_t)*ROZOFS_MAXATTR);

      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());  
      return;   
    }
    if (strcmp(argv[1],"period")==0) {   
	if (argv[2] == NULL) {
	show_tracking_thread_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;  	  
      }
      ret = sscanf(argv[2], "%d", &period);
      if (ret != 1) {
	show_tracking_thread_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;   
      }
      /*
      ** change the period of the thread
      */
      if (period == 0)
      {
        uma_dbg_send(tcpRef, bufRef, TRUE, "value not supported\n");
        return;
      }
      
      export_tracking_thread_period_count = period;
      uma_dbg_send(tcpRef, bufRef, TRUE, "Done\n");   	  
      return;
    }
    show_tracking_thread_help(pChar);	
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());    
    return;
}

/*
 *_______________________________________________________________________
 */
/** Thread for remove bins files on storages for each exports
 */
static void *export_tracking_thread(void *v) {
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    list_t *iterator = NULL;
    int type = 0;
    int export_tracking_thread_period_current_count = 0;
    export_tracking_thread_period_count = 1;
    // Set the frequency of calls
    struct timespec ts = {TRACKING_PTHREAD_FREQUENCY_SEC, 0};

    uma_dbg_thread_add_self("Tracking");

    /*
    ** allocate memory for statistics
    */
    exp_trk_th_stats_p = malloc(sizeof(exp_trk_th_stats_t)*ROZOFS_MAXATTR);
    if (exp_trk_th_stats_p == NULL)
    {
       fatal("cannot allocate memory for tracking thread statistics");
    }
    memset(exp_trk_th_stats_p,0, sizeof(exp_trk_th_stats_t)*ROZOFS_MAXATTR);
    
    export_tracking_thread_period_count = TRACKING_PTHREAD_FREQUENCY_SEC;
    export_tracking_poll_stats[0] = 0;
    export_tracking_poll_stats[1] = 0;
    info("Tracking thread started for instance %d",export_instance_id);


    for (;;) {
        export_tracking_thread_period_current_count++;
	if (export_tracking_thread_period_current_count >= export_tracking_thread_period_count)
	{
          START_PROFILING_TH(export_tracking_poll_stats);
          list_for_each_forward(iterator, &exports) {
              export_entry_t *entry = list_entry(iterator, export_entry_t, list);
	      /*
	      ** do it for the eid that are controlled by the exportd instance, skips the others
	      */
	      if (exportd_is_eid_match_with_instance(entry->export.eid))
	      {
        	for (type = 0;type < ROZOFS_MAXATTR; type++)
		{
		  if ((type == ROZOFS_TRASH)||(type == ROZOFS_DIR_FID)||(type == ROZOFS_RECYCLE)||(type == ROZOFS_REG_S_MOVER) ||
		      (type == ROZOFS_REG_D_MOVER))continue;
        	  if (exp_trck_inode_release_poll(&entry->export, type) != 0) {
                      severe("export_tracking_thread failed (eid: %"PRIu32"): %s",
                              entry->export.eid, strerror(errno));
        	  }
		}
	      }
          }
	  STOP_PROFILING_TH(export_tracking_poll_stats);
	  export_tracking_thread_period_current_count = 0;
	}
        nanosleep(&ts, NULL);
    }
    return 0;
}

/*
 *_______________________________________________________________________
 ** Do one monitoring attempt on master exportd
 ** It updates gprofiler structure used for rozodiag 
 ** and writes volume information under /var/run/exportd/volume_<idx>
 */
static void do_monitor_master() {
  list_t *p;

  uint32_t nb_volumes = 0;

  if ((errno = pthread_rwlock_tryrdlock(&volumes_lock)) != 0) {
    warning("can't lock volumes, monitoring_thread deferred.");
    return;
  }

  list_for_each_forward(p, &volumes) {
    if (monitor_volume(&list_entry(p, volume_entry_t, list)->volume, nb_volumes) != 0) {
      severe("monitor thread failed: %s", strerror(errno));
    }
    nb_volumes++;
  }

  if ((errno = pthread_rwlock_unlock(&volumes_lock)) != 0) {
    severe("can't unlock volumes, potential dead lock.");
  }

  gprofiler->nb_volumes = nb_volumes;
}
/*
 *_______________________________________________________________________
 */
static void *monitoring_thread(void *v) {
  int ts = 1;

  uma_dbg_thread_add_self("Monitor");

  pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

  for (;;) {

    /*
    ** Sleep and increase delay up to 32 seconds
    */    
    sleep(ts);
    if (ts<32) {
      ts *= 2;
    }
    
    do_monitor_master();
  }
  return 0;
}
/*
 *_______________________________________________________________________
 ** Do one monitoring attempt on slave exportd
 ** It updates gprofiler structure used for rozodiag 
 ** and writes export information under /var/run/exportd/export_<idx>
 */
/*
 *_______________________________________________________________________
 */
static void do_monitor_slave() {
  list_t *p;
  uint32_t nb_volumes = 0;
  
  if ((errno = pthread_rwlock_tryrdlock(&volumes_lock)) != 0) {
    warning("can't lock volumes, monitoring_thread deferred.");
    return;
  }


  list_for_each_forward(p, &volumes) {
    if (monitor_volume_slave(&list_entry(p, volume_entry_t, list)->volume, nb_volumes) != 0) {
      severe("monitor thread failed: %s", strerror(errno));
    }
    nb_volumes++;
  }

  if ((errno = pthread_rwlock_unlock(&volumes_lock)) != 0) {
    severe("can't unlock volumes, potential dead lock.");
  }

  if ((errno = pthread_rwlock_tryrdlock(&exports_lock)) != 0) {
    warning("can't lock exports, monitoring_thread deferred.");
    return;
  }

  gprofiler->nb_volumes = nb_volumes;

  gprofiler->nb_exports = 0;

  list_for_each_forward(p, &exports) {
    if (monitor_export(&list_entry(p, export_entry_t, list)->export) != 0) {
      severe("monitor thread failed: %s", strerror(errno));
    }
    gprofiler->nb_exports++;
  }

  if ((errno = pthread_rwlock_unlock(&exports_lock)) != 0) {
    severe("can't unlock exports, potential dead lock.");
  }
}
/*
 *_______________________________________________________________________
 */
static void *monitoring_thread_slave(void *v) {
  int ts = 0;

  uma_dbg_thread_add_self("Monitor");

  pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

  for (;;) {

    /*
    ** Sleep and ijncrease delay up to 10 seconds
    */
    sleep(ts);
    if (ts<10) {
      ts++;
    }
    
    do_monitor_slave();      
  }
  return 0;
}
/*
**____________________________________________________________________________
*/
/**
*  Geo-replication polling 

   that function is the ticker of the geo-replication. its role
   is to check the buffer that must be flushed on disk and
   take care of the progression of the geo-replication indexes file
*/
void geo_replication_poll()
{
    list_t *iterator;
    export_t *e;
    int k;
    geo_rep_srv_ctx_t *ctx_p;
    
    if ((errno = pthread_rwlock_rdlock(&exports_lock)) != 0) {
        severe("can lock exports.");
        return ;
    }

    list_for_each_forward(iterator, &exports) 
    {
      export_entry_t *entry = list_entry(iterator, export_entry_t, list);
      e = &entry->export;
      /*
      ** check if the geo-replication is actve for that exportd: 
      ** it is indicated thanks a a flag in the attached volume
      */
      if (e->volume->georep != 0) 
      {
	for (k = 0; k < EXPORT_GEO_MAX_CTX; k++)
	{
	  ctx_p = e->geo_replication_tb[k];
	  if (ctx_p == NULL)
	  {
	     continue;
	  }
	  geo_replication_poll_one_exportd(ctx_p);
	}
      }
    }
    if ((errno = pthread_rwlock_unlock(&exports_lock)) != 0) {
        severe("can unlock exports, potential dead lock.");
        return ;
    }
}
/*
 *_______________________________________________________________________
 */
/**
*  Polling thread that control the geo-replication disk flush
 */
static void *georep_poll_thread(void *v) {
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    rpcclt_t export_cnx;
    struct timeval timeout_mproto;
    void *ret;

    uma_dbg_thread_add_self("Geo rep poll");

    // Set the frequency of calls
    struct timespec ts = {5, 0};
    /*
    ** initiate a local connection towards the exportd: use localhost as
    ** destination host
    */
    timeout_mproto.tv_sec = 10;
    timeout_mproto.tv_usec = 0;
    /*
    ** init of the rpc context before attempting to connect with the 
    ** exportd
    */
    init_rpcctl_ctx(&export_cnx);

    while(1)
    {
    /*
    ** OK now initiate the connection with the exportd
    */
    if (rpcclt_initialize
            (&export_cnx, "127.0.0.1", EXPORT_PROGRAM, EXPORT_VERSION,
            ROZOFS_RPC_BUFFER_SIZE, ROZOFS_RPC_BUFFER_SIZE,
	     rozofs_get_service_port_export_slave_eproto(export_instance_id),
            timeout_mproto) == 0) break;
     /*
     ** wait for a while and then re-attempt to re-connect
     */
     nanosleep(&ts, NULL);

    }
    for (;;) {
    
      ret = ep_geo_poll_1(NULL, export_cnx.client);
      if (ret == NULL) {
          errno = EPROTO;
	  severe("geo-replication polling error");
      }

    nanosleep(&ts, NULL);
    }
    return 0;
}
/*
 *_______________________________________________________________________
 */
eid_t *exports_lookup_id(ep_path_t path) {
    list_t *iterator;
    char export_path[PATH_MAX];
    DEBUG_FUNCTION;

    if (!realpath(path, export_path)) {
        return NULL;
    }

    if ((errno = pthread_rwlock_rdlock(&exports_lock)) != 0) {
        severe("can lock exports.");
        return NULL;
    }

    list_for_each_forward(iterator, &exports) {
        export_entry_t *entry = list_entry(iterator, export_entry_t, list);
        if (strcmp(entry->export.root, export_path) == 0) {
            if ((errno = pthread_rwlock_unlock(&exports_lock)) != 0) {
                severe("can unlock exports, potential dead lock.");
                return NULL;
            }
            return &entry->export.eid;
        }
    }

    if ((errno = pthread_rwlock_unlock(&exports_lock)) != 0) {
        severe("can unlock exports, potential dead lock.");
        return NULL;
    }
    errno = EINVAL;
    return NULL;
}
/*
 *_______________________________________________________________________
 */
export_t *exports_lookup_export(eid_t eid) {
    list_t *iterator;
    DEBUG_FUNCTION;

    if ((errno = pthread_rwlock_rdlock(&exports_lock)) != 0) {
        severe("can lock exports.");
        return NULL;
    }

    list_for_each_forward(iterator, &exports) {
        export_entry_t *entry = list_entry(iterator, export_entry_t, list);
        if (eid == entry->export.eid) {
            if ((errno = pthread_rwlock_unlock(&exports_lock)) != 0) {
                severe("can unlock exports, potential dead lock.");
                return NULL;
            }
            return &entry->export;
        }
    }

    if ((errno = pthread_rwlock_unlock(&exports_lock)) != 0) {
        severe("can unlock exports, potential dead lock.");
        return NULL;
    }
    errno = EINVAL;
    return NULL;
}
/*
 *_______________________________________________________________________
 */
volume_t *volumes_lookup_volume(vid_t vid) {
    list_t *iterator;
    DEBUG_FUNCTION;

    if ((errno = pthread_rwlock_rdlock(&volumes_lock)) != 0) {
        severe("can't lock volumes.");
        return NULL;
    }

    list_for_each_forward(iterator, &volumes) {
        volume_entry_t *entry = list_entry(iterator, volume_entry_t, list);
        if (vid == entry->volume.vid) {
            if ((errno = pthread_rwlock_unlock(&volumes_lock)) != 0) {
                severe("can't unlock volumes, potential dead lock.");
                return NULL;
            }
            return &entry->volume;
        }
    }

    if ((errno = pthread_rwlock_unlock(&volumes_lock)) != 0) {
        severe("can't unlock volumes, potential dead lock.");
        return NULL;
    }

    errno = EINVAL;
    return NULL;
}
/*
 *_______________________________________________________________________
 */
int exports_initialize() {
    list_init(&exports);
    if (pthread_rwlock_init(&exports_lock, NULL) != 0) {
        return -1;
    }
    return 0;
}
/*
 *_______________________________________________________________________
 */
int volumes_initialize() {
    list_init(&volumes);
    if (pthread_rwlock_init(&volumes_lock, NULL) != 0) {
        return -1;
    }
    return 0;
}
/*
 *_______________________________________________________________________
 */
void exports_release() {
    list_t *p, *q;

    list_for_each_forward_safe(p, q, &exports) {
        export_entry_t *entry = list_entry(p, export_entry_t, list);
//	export_profiler_free(entry->export.eid);
	geo_profiler_free(entry->export.eid);
        export_release(&entry->export);
        list_remove(p);
        free(entry);
    }

    if ((errno = pthread_rwlock_destroy(&exports_lock)) != 0) {
        severe("can't release exports lock: %s", strerror(errno));
    }
}
/*
 *_______________________________________________________________________
 */
void volumes_release() {
    list_t *p, *q;

    list_for_each_forward_safe(p, q, &volumes) {
        volume_entry_t *entry = list_entry(p, volume_entry_t, list);
        volume_release(&entry->volume);
        list_remove(p);
        free(entry);
    }
    if ((errno = pthread_rwlock_destroy(&volumes_lock)) != 0) {
        severe("can't release volumes lock: %s", strerror(errno));
    }
}
/*
 *_______________________________________________________________________
 */
static int load_volumes_conf() {
    list_t *p, *q, *r;
    int i;
	int flipflop=0;
    DEBUG_FUNCTION;
    
    // For each volume

    list_for_each_forward(p, &exportd_config.volumes) {
        volume_config_t *vconfig = list_entry(p, volume_config_t, list);
        volume_entry_t *ventry = 0;

        // Memory allocation for this volume
        ventry = (volume_entry_t *) xmalloc(sizeof (volume_entry_t));

        // Initialize the volume
        volume_initialize(&ventry->volume, vconfig->vid, vconfig->layout,vconfig->georep,vconfig->multi_site, vconfig->rebalance_cfg);

        // For each cluster of this volume

        list_for_each_forward(q, &vconfig->clusters) {
            cluster_config_t *cconfig = list_entry(q, cluster_config_t, list);

            // Memory allocation for this cluster
            cluster_t *cluster = (cluster_t *) xmalloc(sizeof (cluster_t));
            cluster_initialize(cluster, cconfig->cid, 0, 0);
			flipflop = cconfig->cid & 0x3;
            for (i = 0; i <ROZOFS_GEOREP_MAX_SITE; i++) {
	          cluster->nb_host[i] = cconfig->nb_host[i];
              list_for_each_forward(r, (&cconfig->storages[i])) {
                  storage_node_config_t *sconfig = list_entry(r, storage_node_config_t, list);
                  volume_storage_t *vs = (volume_storage_t *) xmalloc(sizeof (volume_storage_t));
                  volume_storage_initialize(vs, sconfig->sid, sconfig->host, sconfig->host_rank, sconfig->siteNum);
				  // Mix the order of the SID depending on the CID number 
				  // This may be usefull in strict rund robin to better distribute files
				  switch(flipflop) {
				    case 1: 
                         list_push_back((&cluster->storages[i]), &vs->list); 
                         break;
					case 2: 
                         list_push_front((&cluster->storages[i]), &vs->list);  
                         break;
					case 3: 
					    list_push_back((&cluster->storages[i]), &vs->list);
						flipflop=0;
						break;
					default:
					  	list_push_front((&cluster->storages[i]), &vs->list);
						flipflop=3;
					    break;
                  }   
              }
	        }
		
            // Add this cluster to the list of this volume
            list_push_back(&ventry->volume.clusters, &cluster->list);
        }
        // Add this volume to the list of volume
        list_push_back(&volumes, &ventry->list);
    }

    return 0;
}
/*
 *_______________________________________________________________________
 */
static int load_exports_conf() {
    int status = -1;
    list_t *p;
    DEBUG_FUNCTION;
    export_entry_t *entry;

    // For each export

    list_for_each_forward(p, &exportd_config.exports) {
        export_config_t *econfig = list_entry(p, export_config_t, list);
	/*
	** do it for eid if the process is master. For the slaves do it for
	** the eid that are in their scope only
	*/
	if (exportd_is_master()== 0) 
	{   
	  if (exportd_is_eid_match_with_instance(econfig->eid) ==0) continue;
	}
        entry = xmalloc(sizeof (export_entry_t));
        volume_t *volume;

        list_init(&entry->list);

        if (!(volume = volumes_lookup_volume(econfig->vid))) {
            severe("can't lookup volume for vid %d: %s\n",
                    econfig->vid, strerror(errno));
        }
	entry->export.trk_tb_p = NULL;
	entry->export.quota_p = NULL;
        if (export_is_valid(econfig->root) != 0) {
            // try to create it
	    entry->export.eid = econfig->eid;
            if (export_create(econfig->root,&entry->export,&cache) != 0) {
                severe("can't create export with path %s: %s\n",
                        econfig->root, strerror(errno));
                goto out;
            }
        }
	info("initializing export %d path %s",econfig->eid,econfig->root);

        // Initialize export
        if (export_initialize(&entry->export, volume, econfig->layout, econfig->bsize,
                &cache, econfig->eid, econfig->root, econfig->md5,
                econfig->squota, econfig->hquota, econfig->filter_name) != 0) {
            severe("can't initialize export with path %s: %s\n",
                    econfig->root, strerror(errno));
            goto out;
        }
	info("initializing export %d OK",econfig->eid);
     
       // Allocate default profiler structure
        export_profiler_allocate(econfig->eid);
        geo_profiler_allocate(econfig->eid);


        // Add this export to the list of exports
        list_push_back(&exports, &entry->list);
    }

    status = 0;
out:
    return status;
}
/*
 *_______________________________________________________________________
 */
static int exportd_initialize() {
    DEBUG_FUNCTION;

    if ((errno = pthread_rwlock_init(&config_lock, NULL)) != 0)
        fatal("can't initialize lock for config: %s", strerror(errno));

    // Initialize lv2 cache
    lv2_cache_initialize(&cache);

    // Initialize list of volume(s)
    if (volumes_initialize() != 0)
        fatal("can't initialize volume: %s", strerror(errno));

    // Initialize list of export gateway(s)
    if (expgw_root_initialize() != 0)
        fatal("can't initialize export gateways: %s", strerror(errno));

    // Initialize list of exports
    if (exports_initialize() != 0)
        fatal("can't initialize exports: %s", strerror(errno));

    // Initialize monitoring
    if (monitor_initialize() != 0)
        fatal("can't initialize monitoring: %s", strerror(errno));

    // Load rozofs parameters
    rozofs_layout_initialize();

    if (load_volumes_conf() != 0)
        fatal("can't load volume");

    if (load_export_expgws_conf() != 0)
        fatal("can't load export gateways");


    if (load_exports_conf() != 0)
        fatal("can't load exports");


    if (pthread_create(&bal_vol_thread, NULL, balance_volume_thread, NULL) !=
            0)
        fatal("can't create balancing thread %s", strerror(errno));

    if (expgwc_non_blocking_conf.slave != 0) {
      start_all_remove_bins_thread();     
    }
    	
    /*
    ** create the thread that control the release of the inode and blocks
    */
    if (pthread_create(&exp_tracking_thread, NULL, export_tracking_thread, NULL) != 0)
        fatal("can't create remove files thread %s", strerror(errno));
    if ( expgwc_non_blocking_conf.slave == 0) 
    {
      if (pthread_create(&monitor_thread, NULL, monitoring_thread, NULL) != 0)
          fatal("can't create monitoring thread %s", strerror(errno));
    }
    else {
    
      /*
      ** Needed to update gprofiler table that is used to respond to
      ** get xattribute rozofs_maxsize
      */ 
      if (pthread_create(&monitor_thread, NULL, monitoring_thread_slave, NULL) != 0)
          fatal("can't create monitoring thread %s", strerror(errno));      

      /*
      ** just needed by slave exportd
      */
      if (pthread_create(&geo_poll_thread, NULL, georep_poll_thread, NULL) != 0)
	  fatal("can't create geo-replication polling thread %s", strerror(errno));
    }


    return 0;
}

static void exportd_release() {

}

/*
**__________________________________________________________________
  SSD resources rozodiag man   
**__________________________________________________________________
*/
void show_metadata_device_usage(char *pChar) {
  pChar += rozofs_string_append           (pChar,"Checking metadata device resources\n");
  pChar += rozofs_string_append_underscore(pChar,"\nUsage:\n");
  pChar += rozofs_string_append_bold      (pChar,"\tmetadata  [<eid>]\n");
  pChar += rozofs_string_append           (pChar,"\tto display resources of device of export <eid>\n\tDefault export is 1\n");
  pChar += rozofs_string_append_underscore(pChar,"\nDisplay:\n");  
  pChar += rozofs_string_append_bold      (pChar,"\teid");
  pChar += rozofs_string_append           (pChar,"\t\texport identifier\n");  
  pChar += rozofs_string_append_bold      (pChar,"\tfull");
  pChar += rozofs_string_append           (pChar,"\t\twhether ENOSPC would be returned on mkdir/mknod\n");  
  pChar += rozofs_string_append_bold      (pChar,"\tfull counter");
  pChar += rozofs_string_append           (pChar,"\t# of occurences of ENOSPC responses to mknod/mkdir\n");  
  pChar += rozofs_string_append_bold      (pChar,"\tfstat errors");
  pChar += rozofs_string_append           (pChar,"\t# of occurences of fstat() errors\n");  
  pChar += rozofs_string_append_bold      (pChar,"\tinodes/sizeMB");
  pChar += rozofs_string_append           (pChar,"\tinformation about device inodes or size in MB\n");  
  pChar += rozofs_string_append_bold      (pChar,"\t . total");
  pChar += rozofs_string_append           (pChar,"\ttotal <number of inode/size in MB> of the device\n");  
  pChar += rozofs_string_append_bold      (pChar,"\t . free");
  pChar += rozofs_string_append           (pChar,"\t\tavailable <number of inode/size in MB> of the device\n");  
  pChar += rozofs_string_append_bold      (pChar,"\t . mini");
  pChar += rozofs_string_append           (pChar,"\t\tminimal allowed value to process mknod/mkdir\n");  
  pChar += rozofs_string_append_bold      (pChar,"\t . depletion");
  pChar += rozofs_string_append           (pChar,"\twhether an <inode/size> depletion is running\n"); 
}
/*
**__________________________________________________________________
  SSD resources rozodiag cli   
**__________________________________________________________________
*/
void show_metadata_device(char * argv[], uint32_t tcpRef, void *bufRef)  {
  char     * pChar = uma_dbg_get_buffer();
  uint32_t   eid;
  export_t * e;
  meta_resources_t * pRes;

  eid = 1;  
  if (argv[1] != NULL) {
    sscanf(argv[1],"%u",&eid);
  }
  
  e = exports_lookup_export(eid);
  if (e==NULL) {
    show_metadata_device_usage(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer()); 
    return;  	     
  }
  export_metadata_check_device(e);
  
  pRes = &e->space_left;
  
   
  pChar += sprintf(pChar,"{ \"meta-data\" : {\n");
  pChar += sprintf(pChar,"     \"eid\"             : %d,\n",eid);    
  pChar += sprintf(pChar,"     \"full\"            : \"%s\",\n",pRes->full?"YES":"NO");  
  pChar += sprintf(pChar,"     \"full counter\"    : %llu,\n",(unsigned long long int)pRes->full_counter);
  pChar += sprintf(pChar,"     \"fstat errors\"    : %llu,\n",(unsigned long long int)pRes->statfs_error);
  pChar += sprintf(pChar,"     \"inodes\" : {\n");
  pChar += sprintf(pChar,"        \"total\"     : %llu,\n",(unsigned long long int)pRes->inodes.total);
  pChar += sprintf(pChar,"        \"free\"      : %llu,\n",(unsigned long long int)pRes->inodes.free);
  pChar += sprintf(pChar,"        \"mini\"      : %llu,\n",(unsigned long long int)common_config.min_metadata_inodes);
  pChar += sprintf(pChar,"        \"depletion\" : \"%s\"\n",common_config.min_metadata_inodes>pRes->inodes.free ?"YES":"NO");
  pChar += sprintf(pChar,"     },\n     \"sizeMB\" : {\n");
  pChar += sprintf(pChar,"        \"total\"     : %llu,\n",(unsigned long long int)pRes->sizeMB.total);
  pChar += sprintf(pChar,"        \"free\"      : %llu,\n",(unsigned long long int)pRes->sizeMB.free);
  pChar += sprintf(pChar,"        \"mini\"      : %llu,\n",(unsigned long long int)common_config.min_metadata_MB);
  pChar += sprintf(pChar,"        \"depletion\" : \"%s\"\n",common_config.min_metadata_MB>pRes->sizeMB.free ?"YES":"NO");
  pChar += sprintf(pChar,"     }\n  }\n}\n"); 
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	     
}
/*
 *_______________________________________________________________________
 */
static void on_start() {
    int sock;
    int one = 1;
    struct rlimit rls;
    pthread_t thread;
    DEBUG_FUNCTION;
    int loop_count = 0;
        
    // Allocate default profiler structure
    export_profiler_allocate(0);
    geo_profiler_allocate(0);

    /*
    ** IPv4 filtering initialization
    */
    rozofs_ip4_ftl_init();


    uma_dbg_thread_add_self("Blocking");

    /**
    * start the non blocking thread
    */ 
    expgwc_non_blocking_thread_started = 0;
    export_non_blocking_thread_can_process_messages = 0;
    if ((errno = pthread_create(&thread, NULL, (void*) expgwc_start_nb_blocking_th, &expgwc_non_blocking_conf)) != 0) {
        severe("can't create non blocking thread: %s", strerror(errno));
    }
    
    /*
    ** wait for end of init of the non blocking thread
    */
    while (expgwc_non_blocking_thread_started == 0)
    {
       sleep(1);
       loop_count++;
       if (loop_count > 5) fatal("Non Blocking thread does not answer");
    }


    if (exportd_initialize() != 0) {
        fatal("can't initialize exportd.");
    }    
    
    /*
    ** Configuration has been processes and data structures have been set up
    ** so non blocking thread can now process safely incoming messages
    */
    export_non_blocking_thread_can_process_messages = 1;

     /*
     ** build the structure for sending out an exportd gateway configuration message
     */
#define MSG_SIZE  (32*1024)
     char * pChar = malloc(MSG_SIZE);
     int msg_sz;
     if (pChar == NULL)  {
       fatal("malloc %d", MSG_SIZE);
     }
    expgw_init_configuration_message(exportd_config.exportd_vip);
    if ( (msg_sz = expgw_build_configuration_message(pChar, MSG_SIZE) ) < 0)
    {
        fatal("can't build export gateway configuration message");
    }
    /*
    ** create the array for building export gateway configuration
    ** message for rozofsmount
    */
    ep_expgw_init_configuration_message(exportd_config.exportd_vip);

    /*
    ** create the array for storing storage configuration
    */
    if (exportd_init_storage_configuration_message()!=0)
    {
        fatal("can't allocate storage configuration array");

    }

    /*
    ** Send the exportd gateway configuration towards the non blocking thread
    */
    {
      int ret;
      ret = expgwc_internal_channel_send(EXPGWC_LOAD_CONF,msg_sz,pChar);
      if (ret < 0)
      {
         severe("EXPGWC_LOAD_CONF: %s",strerror(errno));
      }
    }
    free(pChar);
    
    if ( expgwc_non_blocking_conf.slave == 0)
    {

      // Metadata service
      sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

      // SET NONBLOCK
      int value = 1;
      int oldflags = fcntl(sock, F_GETFL, 0);
      // If reading the flags failed, return error indication now
      if (oldflags < 0) {
          fatal("can't initialize exportd.");
          return;
      }
      // Set just the flag we want to set
      if (value != 0) {
          oldflags |= O_NONBLOCK;
      } else {
          oldflags &= ~O_NONBLOCK;
      }
      // Store modified flag word in the descriptor
      fcntl(sock, F_SETFL, oldflags);

      setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (char *) &one, sizeof (int));
      setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char *) &one, sizeof (int));
  //    setsockopt(sock, SOL_TCP, TCP_DEFER_ACCEPT, (char *) &one, sizeof (int));


      // Change the value of the maximum file descriptor number
      // that can be opened by this process.
      rls.rlim_cur = EXPORTD_MAX_OPEN_FILES;
      rls.rlim_max = EXPORTD_MAX_OPEN_FILES;
      if (setrlimit(RLIMIT_NOFILE, &rls) < 0) {
          warning("Failed to change open files limit to %u", EXPORTD_MAX_OPEN_FILES);
      }

      // XXX Buffers sizes hard coded
      exportd_svc = svctcp_create(sock, ROZOFS_RPC_BUFFER_SIZE,
              ROZOFS_RPC_BUFFER_SIZE);
      if (exportd_svc == NULL) {
          fatal("can't create service %s", strerror(errno));
      }

      pmap_unset(EXPORT_PROGRAM, EXPORT_VERSION); // in case !

      if (!svc_register
              (exportd_svc, EXPORT_PROGRAM, EXPORT_VERSION, export_program_1,
                      IPPROTO_TCP)) {
          fatal("can't register EXPORT_PROGRAM service in rpcbind");
      }

      SET_PROBE_VALUE(uptime, time(0));
      strncpy((char *) gprofiler->vers, VERSION, 20);
      /*
      ** start all the slave exportds
      */
      info("starting slave exportd");
      export_start_export_slave();
      
      /*
      ** Start rebalancer when in automatic mode
      */
      export_rebalancer(1);
      
      /*
      ** Start remote command server that enables storage node to 
      ** remotly access the meta-data
      */
      export_rcmd_server();      

      info("running.");
      svc_run();
    }
    else
    {
      info("slave %d running.",expgwc_non_blocking_conf.instance);
      while(1)
      {
        /**
	 put a heath check here
	 */
	 sleep(60*5);
      }    
    }
}
/*
 *_______________________________________________________________________
 */
void remove_pid_file(int sig);
static void on_stop() {
    DEBUG_FUNCTION;
    if ( expgwc_non_blocking_conf.slave == 0)
    {
    
      svc_exit();

      svc_unregister(EXPORT_PROGRAM, EXPORT_VERSION);
      pmap_unset(EXPORT_PROGRAM, EXPORT_VERSION);
      if (exportd_svc) {
	  svc_destroy(exportd_svc);
	  exportd_svc = NULL;
      }
    }
    
    /*
    ** flush the dirent write back cache on disk
    */
    dirent_wbcache_flush_on_stop();
    
    
    exportd_release();
    closelog();
    
    if ( expgwc_non_blocking_conf.slave == 0)
    {
      
      remove_pid_file(0);      
    }    
}
/*
 *_______________________________________________________________________
 */
/**
*   reload of the configuration initiated from the non-blocking thread

  @param none
  
  @retval 0 on success
  @retval -1 on error
  
*/
int export_reload_nb()
{
   int status = -1;
    list_t *p, *q;

    // Reload the exportd_config structure

    if ((errno = pthread_rwlock_wrlock(&config_lock)) != 0) {
        severe("can't lock config: %s", strerror(errno));
        goto error;
    }

    econfig_release(&exportd_config);

    if (econfig_read(&exportd_config, exportd_config_file) != 0) {
        severe("failed to parse configuration file: %s.", strerror(errno));
        goto error;
    }

    // Reload the list of volumes

    if ((errno = pthread_rwlock_wrlock(&volumes_lock)) != 0) {
        severe("can't lock volumes: %s", strerror(errno));
        goto error;
    }

    list_for_each_forward_safe(p, q, &volumes) {
        volume_entry_t *entry = list_entry(p, volume_entry_t, list);
        volume_release(&entry->volume);
        list_remove(p);
        free(entry);
    }

    load_volumes_conf();

    list_for_each_forward(p, &volumes) {
      volume_balance(&list_entry(p, volume_entry_t, list)->volume);
    }
    
    // volumes lock should be released before loading exports config
    // since load_exports_conf calls volume_lookup_volume which
    // needs to acquire volumes lock
    if ((errno = pthread_rwlock_unlock(&volumes_lock)) != 0) {
        severe("can't unlock volumes: %s", strerror(errno));
        goto error;
    } 

    stop_all_remove_bins_thread();

    // Canceled the export tracking pthread before reload list of exports
    if ((errno = pthread_cancel(exp_tracking_thread)) != 0)
        severe("can't canceled export tracking pthread: %s", strerror(errno));

    // Acquire lock on export list
    if ((errno = pthread_rwlock_wrlock(&exports_lock)) != 0) {
        severe("can't lock exports: %s", strerror(errno));
        goto error;
    }

    // Release the list of exports

    list_for_each_forward_safe(p, q, &exports) {
        export_entry_t *entry = list_entry(p, export_entry_t, list);

        // Canceled the load trash pthread if neccesary before
        // reload list of exports
        if (entry->export.load_trash_thread) {
          pthread_cancel(entry->export.load_trash_thread);
        }
        export_release(&entry->export);
        list_remove(p);
        free(entry);
    }

    // Load new list of exports
    load_exports_conf();

    if ((errno = pthread_rwlock_unlock(&exports_lock)) != 0) {
        severe("can't unlock exports: %s", strerror(errno));
        goto error;
    }

    if ((errno = pthread_rwlock_unlock(&config_lock)) != 0) {
        severe("can't unlock config: %s", strerror(errno));
        goto error;
    }

    // XXX: An export may have been deleted while the rest of the files deleted.
    // These files will never be deleted.

    // Start pthread for remove bins file on slave thread only
    if (expgwc_non_blocking_conf.slave != 0) {
      start_all_remove_bins_thread();
    }

    if (pthread_create(&exp_tracking_thread, NULL, export_tracking_thread, NULL) != 0)
    {
        severe("can't create remove files thread %s", strerror(errno));
        goto error;
    }
    /*
    ** reload the export gateways configuration
    */
    // Acquire lock on export gateways list
    if ((errno = pthread_rwlock_wrlock(&expgws_lock)) != 0) {
        severe("can't lock export gateways: %s", strerror(errno));
        goto error;
    }
    list_for_each_forward_safe(p, q, &expgws) {
        expgw_entry_t *entry = list_entry(p, expgw_entry_t, list);
        expgw_release(&entry->expgw);
        list_remove(p);
        free(entry);
    }
    load_export_expgws_conf();

    //Release lock on export gateways list
    if ((errno = pthread_rwlock_unlock(&expgws_lock)) != 0) {
        severe("can't lock export gateways: %s", strerror(errno));
        goto error;
    }
    export_sharemem_increment_reload();
    status = 0;
    goto out;
error:
    pthread_rwlock_unlock(&exports_lock);
    pthread_rwlock_unlock(&volumes_lock);
    pthread_rwlock_unlock(&config_lock);
    pthread_rwlock_unlock(&expgws_lock);
    severe("reload failed.");
out:
    return status;
}

/*
 *_______________________________________________________________________
 */
/**
* entry point on a kill -1. It correspond to an external request
  for an exportd configuration reload
  
  Once the new configuration has been validated, the slave exportd are
  requested to reload the new configuration
*/

static void on_hup() {
    econfig_t new;

    info("hup signal received.");

    // Check if the new exportd configuration file is valid

    if (econfig_initialize(&new) != 0) {
        severe("can't initialize exportd config: %s.", strerror(errno));
        goto invalid_conf;
    }

    if (econfig_read(&new, exportd_config_file) != 0) {
        severe("failed to parse configuration file: %s.", strerror(errno));
        goto invalid_conf;
    }

    if (econfig_validate(&new) != 0) {
        severe("invalid configuration file: %s.", strerror(errno));
        goto invalid_conf;
    }
    /*
    ** compute the hash of the exportd configuration
    */
    if (hash_file_compute(exportd_config_file,&export_configuration_file_hash) != 0) {
        severe("error while computing hash value of the configuration file: %s.\n",
                strerror(errno));
        goto error;
    }

    econfig_release(&new);

    /*
    ** Stop every rebalancer
    */
    if (expgwc_non_blocking_conf.slave == 0)
    {    
      export_rebalancer(0);
    }
    
    /*
    ** the configuration is valid, so we reload the new configuration
    ** but for doing it we need to warn the non-blocking thread and then wait
    ** for the end of the reload
    */
    export_reload_conf_status.done = 0;
    export_reload_conf_status.status = -1;
    {
      int ret;
      ret = expgwc_internal_channel_send(EXPORT_LOAD_CONF,0,NULL);
      if (ret < 0)
      {
         severe("EXPORT_LOAD_CONF: %s",strerror(errno));
         goto error;
      }

    }
    /*
    ** now wait for the end of the configuration processing
    */
    int loop= 0;
    for (loop = 0; loop < 30; loop++)
    {
       sleep(1);
       if (export_reload_conf_status.done == 1) break;    
    }
    if (export_reload_conf_status.done == 0)
    {
       /*
       ** It think that we can put a fatal here
       */
       goto error;    
    }
    if (export_reload_conf_status.status < 0)
    {
       /*
       ** It think that we can put a fatal here
       */
       goto error;    
    }
    
    if (expgwc_non_blocking_conf.slave == 0)
    {
      /*
      ** reload the slave exportd
      */
      export_reload_all_export_slave();
      
      /*
      ** Start every rebalancer
      */
      export_rebalancer(1);
    }

    info("reloaded.");
    goto out;
error:
    severe("reload failed !!!");
    goto out;
invalid_conf:
    severe("reload failed: invalid configuration !!!");
out:
    econfig_release(&new);
    return;
    

}
/*
 *_______________________________________________________________________
 */
static void usage() {
    printf("Rozofs export daemon - %s\n", VERSION);
    printf("Usage: exportd [OPTIONS]\n\n");
    printf("\t-h, --help\tprint this message.\n");
    printf("\t-d,--debug <port>\t\texportd non blocking debug port(default: none) \n");
//    printf("\t-n,--hostname <name>\t\texportd host name(default: none) \n");
    printf("\t-i,--instance <value>\t\texportd instance id(default: 1) \n");
    printf("\t-c, --config\tconfiguration file to use (default: %s).\n",EXPORTD_DEFAULT_CONFIG);
    printf("\t-s, --slave\tslave exportd (default master.\n");
};
/*
 *_______________________________________________________________________
 */
int main(int argc, char *argv[]) {
    int c;
    int val;

    static struct option long_options[] = {
        {"help", no_argument, 0, 'h'},
        {"debug", required_argument, 0, 'd'},
        {"instance", required_argument, 0, 'i'},
        {"config", required_argument, 0, 'c'},
        {"slave", no_argument, 0, 's'},
        {0, 0, 0, 0}
    };
    
    ALLOC_PROFILING(epp_profiler_t);
    /*
    ** Change local directory to "/"
    */
    if (chdir("/")!=0) {}
    
    rozofs_mkpath(ROZOFS_RUNDIR_PID,0755);

    /* Try to get debug port from /etc/services */
    expgwc_non_blocking_conf.debug_port = rozofs_get_service_port_export_master_diag();
    expgwc_non_blocking_conf.instance = 0;
    expgwc_non_blocking_conf.slave    = 0;
    expgwc_non_blocking_conf.exportd_hostname = NULL;

    while (1) {

        int option_index = 0;
        c = getopt_long(argc, argv, "shc:i:d:", long_options, &option_index);

        if (c == -1)
            break;

        switch (c) {

            case 'h':
                usage();
                exit(EXIT_SUCCESS);
                break;
            case 'c':
                if (!realpath(optarg, exportd_config_file)) {
                    fprintf(stderr,
                            "exportd failed: configuration file: %s: %s\n",
                            optarg, strerror(errno));
                    exit(EXIT_FAILURE);
                }
                break;
             case 'd':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                expgwc_non_blocking_conf.debug_port = val;
                break;
             case 's':
                errno = 0;
                expgwc_non_blocking_conf.slave = 1;
                break;
             case 'i':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                expgwc_non_blocking_conf.instance = val;
                break;
            case '?':
                usage();
                exit(EXIT_SUCCESS);
                break;
            default:
                usage();
                exit(EXIT_FAILURE);
                break;
        }
    }
    
    /*
    ** Do not log each remote end disconnection
    */
    af_unix_socket_no_disconnect_log();
       
    /*
    ** read common config file
    */
    common_config_read(NULL);         
        
    /*
    ** set the instance id and the role of the exportd
    */
    exportd_set_export_instance_and_role(expgwc_non_blocking_conf.instance,(expgwc_non_blocking_conf.slave==0)?1:0);

    // Load RozoFS parameters
    rozofs_layout_initialize();

    if (econfig_initialize(&exportd_config) != 0) {
        fprintf(stderr, "can't initialize exportd config: %s.\n",
                strerror(errno));
        goto error;
    }
    if (econfig_read(&exportd_config, exportd_config_file) != 0) {
        fprintf(stderr, "failed to parse configuration file: %s.\n",
                strerror(errno));
        goto error;
    }
    if (econfig_validate(&exportd_config) != 0) {
        fprintf(stderr, "inconsistent configuration file: %s.\n",
                strerror(errno));
        goto error;
    }
    /*
    ** compute the hash of the exportd configuration
    */
    
    if (hash_file_compute(exportd_config_file,&export_configuration_file_hash) != 0) {
        fprintf(stderr, "error while computing hash value of the configuration file: %s.\n",
                strerror(errno));
        goto error;
    }
    if ( expgwc_non_blocking_conf.slave == 0)
    {
        uma_dbg_record_syslog_name("exportd");

        /*
         * Check if rpcbind service is running
         */
        struct sockaddr_in addr;
        struct pmaplist *plist = NULL;

        get_myaddress(&addr);
        plist = (struct pmaplist *) pmap_getmaps(&addr);
        if (plist == NULL) {
            fprintf(stderr,
                    "failed to contact rpcbind service (%s)."
                    " The rpcbind service must be started before.\n",
                    strerror(errno));
            severe(
                    "Cannot start exportd: "
                    "unable to contact rpcbind service (%s).\n",
                    strerror(errno));
            goto error;
        }

        daemon_start("exportd", common_config.nb_core_file, EXPORTD_PID_FILE,
                on_start, on_stop, on_hup);
    }
    else
    {
      char name[1024];
      char name2[1024];

      sprintf(name,"export_slave_%d",expgwc_non_blocking_conf.instance);
      uma_dbg_record_syslog_name(name);
      sprintf(name2,"%s.pid",name);
      no_daemon_start("export_slave",common_config.nb_core_file,name2, on_start, on_stop, on_hup);    
    }


    exit(0);
error:
    fprintf(stderr, "see log for more details.\n");
    exit(EXIT_FAILURE);
}
