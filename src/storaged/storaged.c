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
#include <pthread.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <sys/resource.h>
#include <unistd.h>
#include <libintl.h>
#include <sys/poll.h>
#include <rpc/rpc.h>
#include <rpc/pmap_clnt.h>
#include <libconfig.h>
#include <getopt.h>
#include <inttypes.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <netdb.h>
#include <dirent.h>
#include <sys/mount.h>

#include <rozofs/rozofs_srv.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include <rozofs/common/daemon.h>
#include <rozofs/common/profile.h>
#include <rozofs/common/common_config.h>
#include <rozofs/rpc/mproto.h>
#include <rozofs/rpc/sproto.h>
#include <rozofs/rpc/spproto.h>
#include <rozofs/core/rozofs_core_files.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/rozo_launcher.h>
#include <rozofs/core/rozofs_share_memory.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/core/rozofs_numa.h>

#include "storio_crc32.h"

#include "config.h"
#include "sconfig.h"
#include "storage.h"
#include "storaged.h"
#include "sconfig.h"
#include "storaged_nblock_init.h"

#define STORAGED_PID_FILE "storaged"

static char * exe_path_name=NULL;

char storaged_config_file[PATH_MAX] = STORAGED_DEFAULT_CONFIG;

sconfig_t storaged_config;

storage_t storaged_storages[STORAGES_MAX_BY_STORAGE_NODE] = { { 0 } };
uint16_t  storaged_nrstorages = 0;

static SVCXPRT *storaged_monitoring_svc = 0;

uint8_t storaged_nb_ports = 0;
uint8_t storaged_nb_io_processes = 0;

DEFINE_PROFILING(spp_profiler_t);


static int storaged_initialize() {
    int status = -1;
    list_t *p = NULL;
    DEBUG_FUNCTION;

    /* Initialize rozofs constants (redundancy) */
    rozofs_layout_initialize();

    storaged_nrstorages = 0;

    storaged_nb_io_processes = 1;
    
    storaged_nb_ports = storaged_config.io_addr_nb;

    /* For each storage on configuration file */
    list_for_each_forward(p, &storaged_config.storages) {
        storage_config_t *sc = list_entry(p, storage_config_t, list);
        /* Initialize the storage */
        if (storage_initialize(storaged_storages + storaged_nrstorages++,
                sc->cid, sc->sid, sc->root,
		sc->device.total,
		sc->device.mapper,
		sc->device.redundancy,
                sc->spare_mark) != 0) {
            severe("can't initialize storage (cid:%d : sid:%d) with path %s",
                    sc->cid, sc->sid, sc->root);
            goto out;
        }
    }

    status = 0;
out:
    return status;
}

/*
**____________________________________________________
*/
/*
  Allocate a device for a file
  
   @param st: storage context
*/
uint32_t storio_device_mapping_allocate_device(storage_t * st, uint8_t layout, sid_t * distrib) {
  severe("storio_device_mapping_allocate_device");
  return -1;
}
uint32_t storio_device_mapping_new_chunk(uint16_t                  chunk,
                                         storio_device_mapping_t * fidCtx,
                                         storage_t               * st, 
                                         uint8_t                   layout, 
                                         sid_t                   * distrib) {
  severe("storio_device_mapping_new_chunk");
  return -1;
}   

pid_t session_id=0;

static void on_stop() {
    DEBUG_FUNCTION;

    svc_exit();

    if (storaged_monitoring_svc) {
        svc_unregister(MONITOR_PROGRAM, MONITOR_VERSION);
        pmap_unset(MONITOR_PROGRAM, MONITOR_VERSION);
        svc_destroy(storaged_monitoring_svc);
        storaged_monitoring_svc = NULL;
    }
}
/*
**____________________________________________________
**
** Start Spare restorer process
**
*/
void storaged_start_spare_restorer_process() {
  char   cmd[1024];
  char   pidfile[128];
  char * p;
  int    ret;
  char IPString[20];

  p = cmd;
      
  /*
  ** Get storio executable from same directory as storaged 
  */
  if (exe_path_name) {
    p += rozofs_string_append(p,exe_path_name);
    *p++ ='/'; 
  }
  p += rozofs_string_append(p, "stspare -c ");
  p += rozofs_string_append(p,storaged_config_file);

  rozofs_ipv4_append(IPString,sconfig_get_this_IP(&storaged_config,0));      
  storaged_spare_restorer_pid_file(pidfile, IPString);
      
  // Launch process throufh rozo launcher
  ret = rozo_launcher_start(pidfile, cmd);
  if (ret !=0) {
    severe("rozo_launcher_start(%s,%s) %s",pidfile, cmd, strerror(errno));
  }
}
/*
**____________________________________________________
*/
/*
  Mount devices if automount is configured
*/
void storaged_automount_devices() {
  char                     * p;
  char                       rozofs_storaged_path[PATH_MAX];
  int                        count;
  
  if (!common_config.device_automount) return;

  p = rozofs_storaged_path;
  p += rozofs_string_append(p, common_config.device_automount_path);
  p += rozofs_string_append(p, "/storaged_");
  p += rozofs_ipv4_append(p, sconfig_get_this_IP(&storaged_config,0));

  /*
  ** Try to mount the devices
  */
  storaged_do_automount_devices(rozofs_storaged_path,&count);  
  
  
  /*
  ** Unmount the working directory 
  */
  storage_umount(rozofs_storaged_path);
        
}
/*
**____________________________________________________
** Thread to process periodic lsblk
**
*/
int rozofs_check_old_lsblk(void);
void storage_run_lsblk();
volatile int lsblk_ready = 0;
void *storaged_lsblk_thread(void *arg) {

  uma_dbg_thread_add_self("lsblk");

  /*
  ** Check for lsblk version 
  */
  rozofs_check_old_lsblk(); 
  
  /*
  ** 1rst lsblk run
  */
  storage_run_lsblk();     
  lsblk_ready = 1;     

  while (1) {
    sleep(5);
    storage_run_lsblk();     
  }  
}  
/*
**____________________________________________________
** Start a thread to process periodic lsblk
**
*/
void storaged_start_lsblk_thread() {
  pthread_attr_t             attr;
  pthread_t                  thrdId;
  int                        err;
  
  lsblk_ready = 0;
  
  err = pthread_attr_init(&attr);
  if (err != 0) {
     fatal("pthread_attr_init(storaged_start_lsblk_thread) %s",strerror(errno));
     return;
   }  

   err = pthread_create(&thrdId,&attr,storaged_lsblk_thread,NULL);
   if (err != 0) {
     fatal("pthread_create(storaged_start_lsblk_thread) %s", strerror(errno));
     return;
   }  
   
   sched_yield();
   
   while (lsblk_ready == 0) {
     usleep(100000);
   }
}


char storage_process_filename[NAME_MAX];

static void on_start() {
    char cmd[1024];
    char pidfile[128];
    char * p;
    storaged_start_conf_param_t conf;
    int ret = -1;
    list_t   *l;
    char IPString[20];
          
    DEBUG_FUNCTION;

    session_id = setsid();
    
    /*
    ** Do not log each remote end disconnection
    */
    af_unix_socket_no_disconnect_log();

    af_unix_socket_set_datagram_socket_len(common_config.storio_buf_cnt);
    storage_process_filename[0] = 0;

    /*
    ** Start thread that periodically processes a lsblk
    */
    storaged_start_lsblk_thread();

    // Initialization of the storage configuration
    if (storaged_initialize() != 0) {
        fatal("can't initialize storaged: %s.", strerror(errno));
        return;
    }

//    SET_PROBE_VALUE(nb_rb_processes, 0);

    SET_PROBE_VALUE(uptime, time(0));
    strncpy((char*) gprofiler->vers, VERSION, 20);
    SET_PROBE_VALUE(nb_io_processes, common_config.nb_disk_thread);
    
    // Create storio process(es)

    // Set monitoring values just for the master process
    //SET_PROBE_VALUE(io_process_ports[i],(uint16_t) storaged_storage_ports[i] + 1000);
    
    
    /*
    ** Create a share memory to get the device status for every SID
    */
    l = NULL;
    list_for_each_forward(l, &storaged_config.storages) {

      storage_config_t *sc = list_entry(l, storage_config_t, list);
      
      /*
      ** Allocate share memory to report device status
      */
      int size = sizeof(storage_share_t)+(STORAGE_MAX_DEVICE_NB-1)*sizeof(storage_device_info_t);
      void * p = rozofs_share_memory_allocate_from_name(sc->root, size);
      if (p) {
        memset(p,0,size);
      }
    }

    storaged_automount_devices();       
    
    conf.nb_storio = 0;
    /*
    ** Then start storio
    */
    {
      uint64_t  bitmask[4] = {0};
      uint8_t   cid,rank,bit; 
         
      /* For each storage on configuration file */
      l = NULL;
      list_for_each_forward(l, &storaged_config.storages) {
      
        storage_config_t *sc = list_entry(l, storage_config_t, list);
	cid = sc->cid;
	
        /* Is this storage already started */
	rank = (cid-1)/64;
	bit  = (cid-1)%64; 
	if (bitmask[rank] & (1ULL<<bit)) {
	  continue;
	}
	
	bitmask[rank] |= (1ULL<<bit);
	
	p = cmd;
	
	// Get storio executable from same directory as storaged 
	if (exe_path_name) {
          p += rozofs_string_append(p,exe_path_name);
	  *p++ ='/'; 
	}
      	p += rozofs_string_append(p, "storio -i ");
	p += rozofs_u32_append(p,cid);
	p += rozofs_string_append(p, " -c ");
	p += rozofs_string_append(p,storaged_config_file);	
      
        rozofs_ipv4_append(IPString,sconfig_get_this_IP(&storaged_config,0));
        storio_pid_file(pidfile, IPString, cid); 
	
        // Launch storio
	ret = rozo_launcher_start(pidfile, cmd);
	if (ret !=0) {
          severe("rozo_launcher_start(%s,%s) %s",pidfile, cmd, strerror(errno));
	}
        conf.nb_storio++;
      }
    }
    
    /*
    ** Start spare restorer process
    */
    storaged_start_spare_restorer_process();

    // Create the debug thread of the parent
    conf.instance_id = 0;
    /* Try to get debug port from /etc/services */    
    conf.debug_port = rozofs_get_service_port_storaged_diag();
    
    storaged_start_nb_th(&conf);
}
/*-----------------------------------------------------------------------------
**
**  Display usage
**
**----------------------------------------------------------------------------
*/
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
  printf("RozoFS storage daemon - %s\n", VERSION);
  printf("Usage: storaged [OPTIONS]\n\n");
  printf("   -h, --help\t\t\tprint this message.\n");
  printf("   -c, --config=config-file\tspecify config file to use (default: %s).\n",
          STORAGED_DEFAULT_CONFIG);
  printf("   -C, --check\tthe storaged just checks the configuration and returns an exit status (0 when OK).\n");  
  
  
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
    int  justCheck=0; /* Run storaged. Not just a configuration check. */
    char pid_name[256];
    static struct option long_options[] = {
        { "help", no_argument, 0, 'h'},
        { "config", required_argument, 0, 'c'},
        { "host", required_argument, 0, 'H'},
	{ "check", no_argument, 0, 'C'},
        { 0, 0, 0, 0}
    };

    ALLOC_PROFILING(spp_profiler_t);
    /*
    ** Change local directory to "/"
    */
    if (chdir("/")!= 0) {}

    uma_dbg_thread_add_self("Main");
    
    uma_dbg_record_syslog_name("storaged");

    // Init of the timer configuration
    rozofs_tmr_init_configuration();

    // Get the path name of the storaged executable
    if (strcmp("storaged",argv[0]) != 0) {
      exe_path_name = xstrdup(argv[0]);
      exe_path_name = dirname(exe_path_name);
    }
    else {
      exe_path_name = NULL;
    }  

    while (1) {

        int option_index = 0;
        c = getopt_long(argc, argv, "hmCc:H:", long_options, &option_index);

        if (c == -1)
            break;

        switch (c) {

            case 'h':
                usage(NULL);
                break;
            case 'C':
                justCheck = 1;
                break;				
            case 'c':
                if (!realpath(optarg, storaged_config_file)) {
                    severe("storaged failed: %s %s\n", optarg, strerror(errno));
                    exit(EXIT_FAILURE);
                }
                break;
            case 'H':
		// Deprecated
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
    ** read common config file
    */
    common_config_read(NULL);    

    // Initialize the list of storage config
    sconfig_initialize(&storaged_config);
    
    // Read the configuration file
    if (sconfig_read(&storaged_config, storaged_config_file,0) != 0) {
        severe("Failed to parse storage configuration file: %s.\n",strerror(errno));
        goto error;
    }
    // Check the configuration
    if (sconfig_validate(&storaged_config) != 0) {
        severe("Inconsistent storage configuration file: %s.\n",strerror(errno));
        goto error;
    }

    {
         char path[256];
         char * pChar = path;
         
	 pChar += rozofs_string_append(pChar,ROZOFS_KPI_ROOT_PATH);
         pChar += rozofs_string_append(pChar,"/storage/");        
         pChar += rozofs_ipv4_append(pChar,sconfig_get_this_IP(&storaged_config,0));
	 pChar += rozofs_string_append(pChar,"/storaged/");
         
	 ALLOC_KPI_FILE_PROFILING(path,"profiler",spp_profiler_t);    
    }
    /*
    ** If any startup script has to be called, call it now
    */
    if (common_config.storaged_start_script[0] != 0) {
      /*
      ** File has to be executable
      */
      if (access(common_config.storaged_start_script,X_OK)==0) {
        if (system(common_config.storaged_start_script)!=0) {}
	info("%s",common_config.storaged_start_script);
      }
      else {
        severe("%s is not executable - %s",common_config.storaged_start_script, strerror(errno));
      }
    }
    
    /*
    **  set the numa node for storaged
    */
    if (storaged_config.numa_node_id != -1) {       
        /*
        ** No node identifier set in storage.conf; Use IP address
        */
        rozofs_numa_allocate_node(storaged_config.numa_node_id,"storage.conf");
    }
    else {
      /*
      ** No node identifier set in storage.conf 
      */       
      rozofs_numa_allocate_node(sconfig_get_this_IP(&storaged_config,0),"host IP");
    }
    
    if (justCheck) {
      closelog();
      exit(EXIT_SUCCESS);      
    }

    {
         char * pChar = pid_name;
         
	 pChar += rozofs_string_append(pChar,STORAGED_PID_FILE);
         pChar += rozofs_string_append(pChar,"_");        
         pChar += rozofs_ipv4_append(pChar,sconfig_get_this_IP(&storaged_config,0));
	 pChar += rozofs_string_append(pChar,".pid");	
    }
    
    no_daemon_start("storaged", common_config.nb_core_file, pid_name, on_start,
            on_stop, NULL);

    exit(EXIT_SUCCESS);
error:
    closelog();
    exit(EXIT_FAILURE);
}
