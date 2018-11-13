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
#include <sys/vfs.h>
#include <dirent.h> 
#include <sys/wait.h>
#include <signal.h>
#include <stdarg.h>

#include <rozofs/rozofs_srv.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include <rozofs/common/rozofs_site.h>
#include <rozofs/common/common_config.h>
#include <rozofs/rpc/mproto.h>
#include <rozofs/rpc/sproto.h>
#include <rozofs/rpc/spproto.h>
#include <rozofs/rpc/eproto.h>
#include <rozofs/core/rozofs_core_files.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/core/rozofs_string.h>
#include <rozofs/core/rozofs_rcmd.h>
#include <stdarg.h>
 
#include "config.h"
#include "sconfig.h"
#include "storage.h"
#include "storaged.h"
#include "sconfig.h"
#include "rbs.h"
#include "storaged_nblock_init.h"
#include "rbs_sclient.h"
#include "rbs_eclient.h"
#include "storage_header.h"

char    * rbs_status_root = NULL;

sconfig_t   storaged_config;
char      * pExport_host = NULL;


storage_t storaged_storages[STORAGES_MAX_BY_STORAGE_NODE] = { { 0 } };
uint16_t  storaged_nrstorages = 0;

uint64_t        rb_fid_table_count=0;
uint32_t        previous_delay=0;

char            command[1024];
char          * status_given_file_name = NULL;
char          * status_given_file_path = NULL;
 

static char rebuild_status[128];
char myFormatedString[1024*64];
static rbs_storage_config_t storage_config;

static storage_t   ze_storage_to_rebuild;
static storage_t * storage_to_rebuild = &ze_storage_to_rebuild;

/* RPC client for exports server */
static rpcclt_t rpcclt_export;

/* List of cluster(s) */
static list_t cluster_entries;
uint32_t    run_loop=0;
  
int     nb_rbs_entry=0;
typedef struct rbs_monitor_s {
  uint8_t  cid;
  uint8_t  sid;
  rbs_file_type_e  ftype;
  int      list_building_sec;
  uint64_t nb_files;
  uint64_t done_files;
  uint64_t resecured;
  uint64_t deleted;  
  uint64_t written_spare;
  uint64_t written;
  uint64_t read_spare;
  uint64_t read;  
  char     status[64];
} RBS_MONITOR_S;
RBS_MONITOR_S rbs_monitor[STORAGES_MAX_BY_STORAGE_NODE*2];



typedef struct _rbs_devices_t {
    uint32_t                     total; 
    uint32_t                     mapper;
    uint32_t                     redundancy;
} rbs_devices_t;

typedef enum RBS_STATUS_e {
  RBS_STATUS_BUILD_JOB_LIST,
  RBS_STATUS_PROCESSING_LIST,
  RBS_STATUS_FAILED,
  RBS_STATUS_ERROR,
  RBS_STATUS_SUCCESS 
} RBS_STATUS_E;

/** Structure used to store configuration for each storage to rebuild */
typedef struct rbs_stor_config {
    char export_hostname[ROZOFS_HOSTNAME_MAX]; ///< export hostname or IP.
    cid_t cid; //< unique id of cluster that owns this storage.
    RBS_STATUS_E status;
    sid_t sid; ///< unique id of this storage for one cluster.
    rbs_file_type_e  ftype;    
    rbs_devices_t  device;    
    char root[PATH_MAX]; ///< absolute path.
} rbs_stor_config_t;

rbs_stor_config_t rbs_stor_configs[STORAGES_MAX_BY_STORAGE_NODE*2] ;


int rbs_index=0;
  
uint8_t storio_nb_threads = 0;
uint8_t storaged_nb_ports = 0;
uint8_t storaged_nb_io_processes = 0;

int current_file_index = 0;
char                rbs_monitor_file_path[ROZOFS_PATH_MAX]={0};
int quiet=0;
int sigusr_received=0;
int nolog = 0;
int verbose=0;

/* Empty and remove a directory
*
* @param dirname: Name of the directory to cleanup
*/
void clean_dir(char * name) {
  DIR           *dir;
  struct dirent *file;
  char           fname[256];
  struct stat    st;

#if 0
  #warning NO CLEAN DIR
  return;
#endif
  
  if (name==NULL) return;
    
  if (stat(name,&st)<0) {
    return;
  }
  
  if (!S_ISDIR(st.st_mode)) {
    if (unlink(name)<0) {
      severe("unlink(%s) %s",name,strerror(errno));
    }  
    return;
  }
      
    
  /*
  ** Open this directory
  */
  dir = opendir(name);
  if (dir == NULL) {
    severe("opendir(%s) %s", name, strerror(errno));
    return;
  } 	  
  /*
  ** Loop on distibution sub directories
  */
  while ((file = readdir(dir)) != NULL) {
  
    if (strcmp(file->d_name,".")==0)  continue;
    if (strcmp(file->d_name,"..")==0) continue;
    
    char * pChar = fname;
    pChar += rozofs_string_append(pChar,name);
    *pChar++ = '/';
    pChar += rozofs_string_append(pChar,file->d_name);
    
    clean_dir(fname);
  }
  closedir(dir); 
  rmdir(name);
  return;
}
/*________________________________________________
*
* Parameter structure
*/
typedef enum rbs_rebuild_type_e  {
  rbs_rebuild_type_fid,
  rbs_rebuild_type_device,
  rbs_rebuild_type_storage
} RBS_REBUILD_TYPE_E;

typedef struct _rbs_parameter_t {
  char     storaged_config_file[PATH_MAX];
  char     rbs_export_hostname[ROZOFS_HOSTNAME_MAX];
  int      rbs_device_number;
  RBS_REBUILD_TYPE_E type;
//  char   * storaged_hostname; deprecated
  int      cid;
  int      sid;
  fid_t    fid2rebuild;
  int      parallel;
  int      storaged_geosite;
  int      relocate;
  int      resecure;
  int      max_reloop;
  char   * output;
  int      clear;
  int      clearOnly;
  int      resume;
  int      pause;
  int      abort;
  int      list;
  int      rebuildRef;
  int      background;
  char   * simu;
  int      bstart; // 1rst block to rebuild when FID is given
  int      bstop;  // Last block to rebuild when FID is given
  int      chunk;  // Chunk to rebuild when FID is given 
  rbs_file_type_e filetype; // spare/nominal/all 
  int      throughput; // spare/nominal/all 
} rbs_parameter_t;

rbs_parameter_t parameter;
/*
**____________________________________________________
** Create or re-create the monitoring file for this rebuild process
*/

time_t loc_time;
char   initial_date[80];
static inline char * format_status_file(char * pJSON) {
  char   delay[32];
  char * pt;
  int    i;
  uint64_t nb_files=0;
  uint64_t done_files=0;
  uint64_t resecured=0;
  uint64_t deleted=0;
  uint64_t written=0;
  uint64_t written_spare=0;
  uint64_t read_spare=0;
  uint64_t read=0;
  uint32_t listing=0;
  int json_offset=0;   
  char     mystring[128];
  
  JSON_begin;
  
    JSON_open_array("comment");
      JSON_string_element("This file was generated by storage_rebuild(8).");
      JSON_string_element("All changes to this file will be lost.");  
    JSON_close_array;
    
    JSON_string("version",VERSION);
    JSON_string("git ref",ROZO_GIT_REF);
    JSON_u32("id.",parameter.rebuildRef);
    JSON_string("started",initial_date);
    JSON_string("command",command);
    JSON_u32("parallel",parameter.parallel);   
    JSON_u32("loop",run_loop);
    if (parameter.type == rbs_rebuild_type_fid) {
      JSON_string("mode","FID");
    }
    else if (parameter.type == rbs_rebuild_type_device) { 
      if (parameter.relocate) {
        JSON_string("mode","relocate device");      
      }
      else if (parameter.resecure) {
        JSON_string("mode","resecure device");      
      }      
      else {     
        JSON_string("mode","device");
      }	
    }
    else {
      if (parameter.cid == -1) {
        JSON_string("mode","node");     
      }
      else {
        if (parameter.resecure) {
          JSON_string("mode","storage resecure");        
        }
        else {
          JSON_string("mode","storage");
        }     
      }	 
    }
    
    uint32_t sec = time(NULL) - loc_time + previous_delay;
    if (sec == 0) sec = 1;
    uint32_t seconds= sec % 60; 
    uint32_t min=sec/60;
    uint32_t hour=min/60; 
    min = min % 60;
    pt = delay;
    pt += rozofs_u32_append(pt, hour);
    pt += rozofs_string_append(pt, ":");  
    pt += rozofs_u32_padded_append(pt, 2, rozofs_zero, min);
    pt += rozofs_string_append(pt, ":");
    pt += rozofs_u32_padded_append(pt, 2, rozofs_zero, seconds);  
    JSON_string("delay", delay);


    JSON_open_array("storages");
      for (i=0; i<nb_rbs_entry; i++) {

	JSON_new_element;
	  JSON_2u32("cid",rbs_monitor[i].cid, "sid",rbs_monitor[i].sid);	   
	  JSON_string("kind of file",rbs_file_type2string(rbs_monitor[i].ftype)); 
	  JSON_string("rebuild status", rbs_monitor[i].status);
	  JSON_u32("listing time", rbs_monitor[i].list_building_sec);
	  listing += rbs_monitor[i].list_building_sec;
	  JSON_u64("files to process", rbs_monitor[i].nb_files);
	  JSON_u64("files processed", rbs_monitor[i].done_files);

	  nb_files   += rbs_monitor[i].nb_files;
	  done_files += rbs_monitor[i].done_files;
          resecured  += rbs_monitor[i].resecured;
	  if (rbs_monitor[i].nb_files) {
	    JSON_u32("percent done", rbs_monitor[i].done_files*100/rbs_monitor[i].nb_files); 
	  }
	  else {
	    JSON_u32("percent done", 0); 
	  }

	  JSON_u64("files resecured", rbs_monitor[i].resecured);
	  JSON_u64("deleted files", rbs_monitor[i].deleted);
	  deleted += rbs_monitor[i].deleted;

	  JSON_open_obj("written");
	    JSON_u64("total bytes", (long long unsigned int)rbs_monitor[i].written);
	    written += rbs_monitor[i].written;
	    JSON_u64("nominal bytes", (long long unsigned int)rbs_monitor[i].written-rbs_monitor[i].written_spare);
	    JSON_u64("spare bytes", (long long unsigned int)rbs_monitor[i].written_spare);
	    written_spare += rbs_monitor[i].written_spare;
	  JSON_close_obj;

	  JSON_open_obj("read");
	    JSON_u64("total bytes", (long long unsigned int)rbs_monitor[i].read);
	    read += rbs_monitor[i].read;
	    JSON_u64("nominal bytes", (long long unsigned int)rbs_monitor[i].read-rbs_monitor[i].read_spare);
	    JSON_u64("spare bytes", (long long unsigned int)rbs_monitor[i].read_spare);
	    read_spare += rbs_monitor[i].read_spare; 
	  JSON_close_obj;
	JSON_end_element;  
      }


      JSON_new_element;
	JSON_2u32("cid",0, "sid",0);	
        JSON_string("kind of file",rbs_file_type2string(rbs_file_type_all));    
	JSON_string("rebuild status", rebuild_status);
	JSON_u32("listing time", listing);	
	JSON_u64("files to process", nb_files);
	JSON_u64("files processed", done_files);
	JSON_u64("files resecured", resecured);
	if (nb_files) {
	  JSON_u32("percent done", done_files*100/nb_files); 
	}
	else {
	  JSON_u32("percent done", 0); 
	}
	JSON_u64("deleted files", deleted);

	JSON_open_obj("written");
	  JSON_u64("total bytes", (long long unsigned int)written);
	  JSON_u64("nominal bytes", (long long unsigned int)written-written_spare);
	  JSON_u64("spare bytes", (long long unsigned int)written_spare);
	JSON_close_obj;

	JSON_open_obj("read");
	  JSON_u64("total bytes", (long long unsigned int)read);
	  JSON_u64("nominal bytes", (long long unsigned int)read-read_spare);
	  JSON_u64("spare bytes", (long long unsigned int)read_spare);
	JSON_close_obj;

      JSON_end_element;   

    JSON_close_array;

    JSON_open_array("tips");
       sprintf(mystring,"storage_rebuild -id %d -pause",parameter.rebuildRef);
       JSON_string_element(mystring);
       sprintf(mystring,"storage_rebuild -id %d -resume",parameter.rebuildRef);
       JSON_string_element(mystring);
       sprintf(mystring,"storage_rebuild -id %d -abort",parameter.rebuildRef);    
       JSON_string_element(mystring);       
       sprintf(mystring,"storage_rebuild -id %d -list",parameter.rebuildRef);
       JSON_string_element(mystring);       
    JSON_close_array;

  JSON_end;
  
  return pJSON; 
}

void static inline rbs_status_file_name() {
  struct tm date;

  char * pChar = rbs_monitor_file_path;
  if (*pChar != 0) return;

  loc_time=time(NULL);
  localtime_r(&loc_time,&date); 
  ctime_r(&loc_time,initial_date);
  int end =  strlen(initial_date);
  initial_date[end-1]=0;
      
  /*
  ** Get given absolute path
  */    
  if (status_given_file_path) {
    pChar += rozofs_string_append(pChar,status_given_file_path);
    return;
  }  

  /*
  ** Start with root rebuild path
  */
  pChar += rozofs_string_append(pChar,rbs_status_root);
  if (access(rbs_monitor_file_path,W_OK) == -1) {
    rozofs_mkpath(rbs_status_root, S_IRWXU | S_IROTH);
  }	

  /*
  ** Add given relative name
  */
  if (status_given_file_name != NULL) {
    pChar += rozofs_string_append(pChar, status_given_file_name);  
    return;
  }

  /*
  ** Build name from date
  */
  pChar += rozofs_u32_padded_append(pChar, 4, rozofs_right_alignment, date.tm_year+1900); 
  *pChar++ =':'; 
  pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, date.tm_mon+1);  
  *pChar++ =':';     
  pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, date.tm_mday);  
  *pChar++ ='_';     
  pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, date.tm_hour);  
  *pChar++ =':';     
  pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, date.tm_min);  
  *pChar++ =':';     
  pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, date.tm_sec);  
  *pChar++ ='_'; 

  if (parameter.type == rbs_rebuild_type_fid) {
    pChar += rozofs_fid_append(pChar,parameter.fid2rebuild); 
  }
  else {
    pChar += rozofs_u32_append(pChar, parameter.rebuildRef);     
  }   	       
}
/*________________________________________________
*
* Initialize parameter structure with default values
* @param   cnf Structure to initialize
*/
void rbs_conf_init(rbs_parameter_t * par) {
  fid_t fid_null={0};

  strcpy(par->storaged_config_file,STORAGED_DEFAULT_CONFIG);
  if (strcmp(common_config.export_hosts,"")==0) {
    par->rbs_export_hostname[0] = 0;
  }
  else {
    strcpy(par->rbs_export_hostname,common_config.export_hosts);
  }  
  par->rbs_device_number    = -1;
  par->type                 = rbs_rebuild_type_storage;
//  par->storaged_hostname    = NULL;
  par->cid                  = -1;
  par->sid                  = -1;
  memset(par->fid2rebuild,0,sizeof(fid_t));
  par->parallel             = common_config.device_self_healing_process;
  par->relocate             = 0;
  par->resecure             = 0;
  par->max_reloop           = common_config.default_rebuild_reloop;
  par->output               = NULL;
  par->clear                = 0;
  par->rebuildRef           = 0;
  par->resume               = 0;
  par->pause                = 0;  
  par->list                 = 0;
  par->background           = 1;
  par->simu                 = NULL;
  memcpy(par->fid2rebuild,fid_null,sizeof(fid_t));
  par->bstart               = 0;
  par->bstop                = -1;
  par->chunk                = -1;
  par->filetype             = rbs_file_type_all;
  par->throughput           = 0;
  
  
  par->storaged_geosite = rozofs_get_local_site();
  if (par->storaged_geosite == -1) {
    par->storaged_geosite = 0;
  }
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


    printf("\nStorage node rebuild - RozoFS %s\n", VERSION);
    printf("Usage: storage_rebuild [OPTIONS]\n\n");
    printf("   -h, --help                \tPrint this message.\n");
    printf("   -r, --rebuild <names>     \tlist of \'/\' separated host where exportd is running (optionnal. Check rozofs.conf)\n");
    printf("   -d, --device <device>     \tDevice number to rebuild.\n");
    printf("                             \tAll devices are rebuilt when omitted.\n");
    printf("   -s, --sid <cid/sid>       \tCluster and storage identifier to rebuild.\n");
    printf("                             \tAll <cid/sid> are rebuilt when omitted.\n");
    printf("   -f, --fid <FID>           \tSpecify one FID to rebuild. -s must also be set.\n");
    printf("       --bstart              \t1rst block to rebuild when FID is given\n");
    printf("       --bstop               \tlast block to rebuild when FID is given\n");
    printf("       --chunk               \tchunk to rebuild when FID is given\n");
    printf("   -p, --parallel=<val>      \tNumber of rebuild processes in parallel per cid/sid\n");
    printf("                             \t(default is %d, maximum is %d)\n",
           common_config.device_self_healing_process,MAXIMUM_PARALLEL_REBUILD_PER_SID);   
    printf("   -t, --throughput          \tThroughput limitation in MB/s per rebuild process in parallel.\n");    
    printf("       --spare               \tTo rebuild only spare files on node or sid rebuild.\n");
    printf("       --nominal             \tTo rebuild only nominal files on node or sid rebuild.\n");
    printf("   -g, --geosite             \tTo force site number in case of geo-replication\n");
    printf("   -R, --relocate            \tTo rebuild a device by relocating files\n");
    printf("   -S, --reSecure            \tTo resecure files of a failed device on their spare location\n");
    printf("   -l, --loop                \tNumber of reloop in case of error (default %d)\n",common_config.default_rebuild_reloop);
    printf("   -q, --quiet               \tDo not display messages\n");
    printf("   -C, --clear               \tClear the status of the device after it has been set OOS\n");
    printf("   -K, --clearOnly           \tJust clear the status of the device, but do not rebuild it\n");
    printf("   -o, --output=<file>       \tTo give the name of the rebuild status file (under %s)\n",ROZOFS_RUNDIR_RBS_REBUILD);
    printf("   -O, --OUTPUT=<filePath>   \tTo give the absolute file name of the rebuild status file\n");
    printf("   -id <id>                  \tIdentifier of a non completed rebuild.\n");
    printf("   -abort                    \tAbort a rebuild\n");
    printf("   -pause                    \tPause a rebuild\n");
    printf("   -resume                   \tResume a rebuild\n");
    printf("   -list                     \tDisplay a list of FID to rebuild\n");
    printf("   -rawlist                  \tDisplay a raw list of FID to rebuild\n");
    printf("   -fg                       \tTo force foreground execution\n");
    printf("   -bg                       \tTo force background execution\n");
    printf(" mainly for tests:\n");
//    printf("   -H, --host=storaged-host  \tSpecify the hostname to rebuild\n");
    printf("   -c, --config=config-file  \tSpecify config file to use\n");
    printf("                             \t(default: %s).\n",STORAGED_DEFAULT_CONFIG);

    printf("Rebuilding a whole storage node as fast as possible:\n");
    printf("storage_rebuild -p %d\n\n",MAXIMUM_PARALLEL_REBUILD_PER_SID);
    printf("Rebuilding every devices of sid 2 of cluster 1:\n");
    printf("storage_rebuild -s 1/2\n\n");
    printf("Rebuilding only device 3 of sid 2 of cluster 1:\n");
    printf("storage_rebuild -s 1/2 -d 3\n\n");
    printf("Rebuilding by resecuring files on spare storages, device 3 of sid 2 of cluster 1:\n");
    printf("storage_rebuild -s 1/2 -d 3 --resecure\n\n");
    printf("Rebuilding by relocating device 3 of sid 2 of cluster 1 on other devices:\n");
    printf("storage_rebuild -s 1/2 -d 3 --relocate\n\n");
    printf("Puting a device back in service when it is replaced after\n");
    printf("an automatic relocation (self healing)\n");
    printf("storage_rebuild -s 1/2 -d 3 --clear\n\n");    
    printf("Pause a running rebuild in order to resume it later\n");
    printf("storage_rebuild -id <id> -pause\n\n"); 
    printf("Resume a paused or failed rebuild\n");
    printf("storage_rebuild -id <id> -resume\n\n"); 
    printf("Check the list of remaining FID to rebuild\n");
    printf("storage_rebuild -id <id> -list\n\n");     
    printf("Abort definitively a running rebuild\n");
    printf("storage_rebuild -id <id> -abort\n\n");  
                     
    if (fmt) exit(EXIT_FAILURE);
    exit(EXIT_SUCCESS); 
}

/*________________________________________________
*
* Command parsing
*/
#define GET_PARAM(opt) {\
  idx++;\
  if (idx >= argc) usage("argument without value \"%s\".\n",#opt);\
  optarg = argv[idx];\
}
#define NEXT_ARG {\
  idx++;\
  if (idx >= argc) break;\
  optarg = argv[idx];\
}  
#define IS_ARG(x) (strcmp(optarg, #x)==0)  

#define GET_INT_PARAM(opt,val) {\
  GET_PARAM(opt)\
  ret = sscanf(optarg,"%u",&val);\
  if (ret != 1) {\
   usage("\"%s\" option has not int value \"%s\"\n",#opt,optarg);\
  }\
}      
 
void parse_command(int argc, char *argv[], rbs_parameter_t * par) {
  int    ret;
  int    idx;
  char * optarg;

  //if (argc < 2) usage("Only %d arguments.\n",argc); 
  
  idx = 0;
  while (idx < argc) {
  
    NEXT_ARG;
    //printf("optarg = %s\n",optarg);

    if (IS_ARG(-h) || IS_ARG(--help) || IS_ARG(?)) {
      usage(NULL);
    }
    
    if (IS_ARG(-c) || IS_ARG(--config)) {
      GET_PARAM(--config)
      if (!realpath(optarg, par->storaged_config_file)) {
	usage("No such configuration file %s.",optarg);
      }
      continue;
    }  

    if IS_ARG(-id) {
      GET_INT_PARAM(-id,par->rebuildRef);
      continue;
    }  	
        	
    if (IS_ARG(-t) || IS_ARG(--throughput)) {
      GET_INT_PARAM(-t,par->throughput);
      continue;
    }  	
       
    if IS_ARG(-resume) {
      par->resume = 1;
      continue;
    } 
    
    if IS_ARG(-pause) {
      par->pause = 1;
      continue;
    }
    
    if IS_ARG(-abort) {
      par->abort = 1;
      continue;
    }
             
    if IS_ARG(-list) {
      par->list = 1;     
      continue;
    } 
    
    if IS_ARG(-rawlist) {
      par->list = 2;     
      continue;
    }   
      
    if (IS_ARG(-R) || IS_ARG(--relocate)) {
      par->relocate = 1;     
      continue;
    } 
      
    if (IS_ARG(-S) || IS_ARG(--reSecure)) {
      par->resecure = 1;     
      continue;
    } 

    if (IS_ARG(-C) || IS_ARG(--clear)) {
      par->clear = 1;     
      continue;
    }  

    if (IS_ARG(--spare)) {
      if (par->filetype == rbs_file_type_nominal) {
	usage("--spare and --nominal options are incompatible.\n");
      }
      par->filetype = rbs_file_type_spare;     
      continue;
    }  

    if (IS_ARG(--nominal)) {
      if (par->filetype == rbs_file_type_spare) {
	usage("--spare and --nominal options are incompatible.\n");
      }
      par->filetype = rbs_file_type_nominal;     
      continue;
    }  
        
    if (IS_ARG(-K) || IS_ARG(--clearOnly)) {
      par->clear = 2;     
      continue;
    }  

    if (IS_ARG(-q) || IS_ARG(--quiet)) {
      quiet = 1;     
      continue;
    }  


    if (IS_ARG(--verbose)) {
      verbose = 1;     
      continue;
    }  
    
    if IS_ARG(--nolog) {
      nolog = 1;
      quiet = 1;     
      continue;
    }  

    if IS_ARG(-bg) { 
      par->background = 1;     
      continue;
    }  

    if IS_ARG(-fg) { 
      par->background = 0;     
      continue;
    }  
        
    if (IS_ARG(-r) || IS_ARG(--rebuild)) { 
      GET_PARAM(--rebuild)
      if (strncpy(par->rbs_export_hostname, optarg, ROZOFS_HOSTNAME_MAX) == NULL) {
        usage("Bad host name %s.", optarg);
      }
      continue;
    }  
	  
    if (IS_ARG(-s) || IS_ARG(--sid)) { 
      GET_PARAM(--sid)
      ret = sscanf(optarg,"%d/%d", &par->cid, &par->sid);
      if (ret != 2) {
	usage("-s option requires also cid/sid.\n");
      }	
      continue;
    }  
    
    if (IS_ARG(-f) || IS_ARG(--fid)) { 
      GET_PARAM(--fid)
      ret = rozofs_uuid_parse(optarg,par->fid2rebuild);
      if (ret != 0) {
	usage("Bad FID format %s.", optarg);
      }
      par->type = rbs_rebuild_type_fid; 
      par->rbs_device_number = -2; // To tell one FID to rebuild 
      continue;
    } 
	
    if (IS_ARG(--bstart)) { 
      GET_INT_PARAM(-l,par->bstart)
      continue;
    }
    
    if (IS_ARG(--bstop)) { 
      GET_INT_PARAM(-l,par->bstop)
      continue;
    }
    
    if (IS_ARG(--chunk)) { 
      GET_INT_PARAM(-l,par->chunk)
      continue;
    }
        	  
    if (IS_ARG(-o) || IS_ARG(--output)) { 
      GET_PARAM(--output)
      status_given_file_name = optarg;	  
      continue;
    } 	
    
    if (IS_ARG(-O) || IS_ARG(--OUTPUT)) { 
      GET_PARAM(--OUTPUT)
      status_given_file_path = optarg;	  
      continue;
    } 	
	  
	// --simu exportd configuration file for vbox
    if (IS_ARG(--simu)) { 
      GET_PARAM(--simu)
	  par->simu = optarg;	  	  
      continue;
    }
	
    if (IS_ARG(-l) || IS_ARG(--loop)) { 
      GET_INT_PARAM(-l,par->max_reloop)
      continue;
    }
	  									
    if (IS_ARG(-d) || IS_ARG(--device)) { 
      GET_INT_PARAM(-d,par->rbs_device_number)
      par->type = rbs_rebuild_type_device; 
      continue;
    }
	  			
    if (IS_ARG(-g) || IS_ARG(--geosite)) { 
      GET_INT_PARAM(-g,par->storaged_geosite)
      if ((par->storaged_geosite!=0)&&(par->storaged_geosite!=1)) { 
        usage("Site number must be within [0:1] instead of %s.", optarg);
      }
      continue;
    }

    if (IS_ARG(-p) || IS_ARG(--parallel)) { 
      GET_INT_PARAM(-p,par->parallel)
      if (par->parallel > MAXIMUM_PARALLEL_REBUILD_PER_SID) {
        REBUILD_MSG("--parallel value is too big %d. Assume maximum parallel value of %d\n", 
		       par->parallel, MAXIMUM_PARALLEL_REBUILD_PER_SID);
	par->parallel = MAXIMUM_PARALLEL_REBUILD_PER_SID;
      }
      continue;
    }
				
    if (IS_ARG(-H) || IS_ARG(--host)) { 
      GET_PARAM(--host)
      // deprecated
      //par->storaged_hostname = optarg;
      continue;
    }

    usage("Unexpected argument \"%s\".\n",optarg);
  }


  /*
  ** On rebuild resume the rebuild identifier must be provided
  */
  if ((par->resume)||(par->list)||(par->pause)||(par->abort)) {
    if (par->rebuildRef==0) {
      usage("-resume -pause -abort and -list/-rawlist options require a rebuild identifier.");
    }
  }
  else {
    if (par->rbs_export_hostname[0] == 0) {
        usage("Missing mandatory option --rebuild");    
    }     
    /*
    ** When neither resume nor ailed is given, the rebuild ref is the process pid
    */    
    par->rebuildRef = getpid();
  }
  /*
  ** Relocate & resecure re exclusive
  */
  if ((par->relocate) && (par->resecure)) {
      usage("--relocate and --reSecure are exclusive options.");
  }   
  /*
  ** When FID is given, eid and cid/sid is mandatory
  */ 
  if (par->type == rbs_rebuild_type_fid) {
    if ((par->cid==-1)&&(par->sid==-1)) {
      usage("--fid option requires --sid option too.");
    }
    par->parallel = 1;
    /* 
    ** When relocate is requested, rebuild the whole file and not only a part of it
    */
    if (par->relocate) {
      if (par->chunk != -1) {
        severe("With relocate --chunk --bstart and --bstop are ignored");
        par->chunk = -1;
      }
    }  
    /*
    ** When resecure is requested, rebuild the whole file and not only a part of it
    */
    if (par->resecure) {
      if (par->chunk != -1) {
        severe("With resecure --chunk --bstart and --bstop are ignored");
        par->chunk = -1;
      }
    }      
  }
  /*
  ** When relocate is set, cid/sid and device are mandatory 
  */
  if (par->relocate) {
    if ((par->cid==-1)&&(par->sid==-1)) {
      usage("--relocate option requires --sid option too.");
    }
    if (par->type != rbs_rebuild_type_device) {
      usage("--relocate option requires --device option too.");
    }
  }
  /*
  ** When resecure is set, cid/sid and device are mandatory 
  */
  if (par->resecure) {
    if ((par->cid==-1)&&(par->sid==-1)) {
      usage("--resecure option requires --sid option too.");
    }
    if (par->type != rbs_rebuild_type_device) {
      usage("--resecure option requires --device option too.");
    }
  }  
  /*
  ** Clear errors and reinitialize disk
  */
  if (par->clear) {

    /*
    ** When clear is set cid/sid must too
    */    
    if ((par->cid==-1)&&(par->sid==-1)) {
      usage("--clear option requires --sid option too.");
    }
    /*
    ** When clear is set device number must too
    */    
    if (par->rbs_device_number < 0) {
      usage("--clear option requires --device option too.");
    }    
  }
  
}
/*________________________________________________
*
* Search for -a or --resume in command
* and return the rebuild reference
*/
static rbs_parameter_t localPar;  
int is_command_resume(int argc, char *argv[]) {
  
  rbs_conf_init(&localPar);
  parse_command(argc,argv,&localPar);
  if (localPar.resume == 0) return 0;
  
  if (strcmp(localPar.storaged_config_file,STORAGED_DEFAULT_CONFIG)!=0) {
    usage("-resume and --config options are incompatibles.");
  }
  if ((localPar.cid!=-1)||(localPar.sid!=-1)) {
    usage("-resume and --sid options are incompatibles.");
  } 
  if (localPar.type == rbs_rebuild_type_device) {
    usage("-resume and --device options are incompatibles.");
  } 
  if (localPar.type == rbs_rebuild_type_fid) {
    usage("-resume and --fid options are incompatibles.");
  } 
  if (localPar.cid!=-1) {
    usage("-resume and --fid options are incompatibles.");
  }        
  if (localPar.relocate) {
    usage("-resume and --relocate options are incompatibles.");
  } 
  return localPar.rebuildRef;
}

/*__________________________________________________________________________
*/
#define RBS_MAX_REBUILD_MARK    512
int    rbs_nb_rebuild_mark = 0;
char * rbs_rebuild_mark[512];

/*__________________________________________________________________________
** Remove one rebuild mark file which name is stored at idx in 
** table rbs_rebuild_mark
*/
static inline void rbs_remove_rebuild_mark(int idx) {
  if (rbs_rebuild_mark[idx] != NULL) {
    unlink(rbs_rebuild_mark[idx]);
    xfree(rbs_rebuild_mark[idx]);
    rbs_rebuild_mark[idx] = NULL;
  }  
}
/*__________________________________________________________________________
** Remove every rebuild mark stored in table rbs_rebuild_mark
*/
void rbs_remove_rebuild_marks() {
  int idx;
      
  for (idx=0; idx<rbs_nb_rebuild_mark; idx++) {
    rbs_remove_rebuild_mark(idx);
  }
} 
/*__________________________________________________________________________
** Write a rebuild mark on a device and store the name of the rebuild mark 
** in table rbs_rebuild_mark
*/
void rbs_write_rebuild_mark(char * root, int dev) {
  char          path[FILENAME_MAX];
  char        * pChar = path;
  int           fd;
  int           idx;

  pChar += rozofs_string_append(pChar, root);
  pChar += rozofs_string_append(pChar, "/");
  pChar += rozofs_u32_append(pChar, dev); 
  pChar += rozofs_string_append(pChar, "/");
  pChar += rozofs_string_append(pChar, STORAGE_DEVICE_REBUILD_REQUIRED_MARK);  
    
  /*
  ** Check whether this rebuild mark already exist
  */
  for (idx=0; idx<rbs_nb_rebuild_mark; idx++) {
    if (strcmp(path, rbs_rebuild_mark[idx])==0) {
      /*
      ** This device has already been marked
      */
      return;
    }  
  }
  
  /*
  ** Store this mark name in the table
  */
  rbs_rebuild_mark[rbs_nb_rebuild_mark] = xstrdup(path);
  rbs_nb_rebuild_mark++;
    
  /*
  ** Write the mark on the device
  */
  fd = creat(path,0777);
  if (fd<0) return;
  close(fd);
  return;
}

/*__________________________________________________________________________
** Write a rebuild mark on every device that is beeing rebuilt
*/
void rbs_write_rebuild_marks() {
  int idx;
  int dev;

  if (parameter.type == rbs_rebuild_type_fid) {
    /*
    ** Just one FID rebuild, no device rebuild => no rebuild mark
    */
    return;
  }
    
  /* 
  ** Only on disk to rebuild. Put the mark on the disk
  */
  if (parameter.type == rbs_rebuild_type_device) {
    rbs_write_rebuild_mark(rbs_stor_configs[0].root, parameter.rbs_device_number);
    return;
  }
  
  /*
  ** More to rebuild. Put a mark on each device targeted bu the rebuild
  */
  for (idx=0; idx<nb_rbs_entry; idx++) {
    /*
    ** For each CID/SID mark every device
    */
    for (dev=0; dev < rbs_stor_configs[idx].device.total; dev++) {
      rbs_write_rebuild_mark(rbs_stor_configs[idx].root,dev);
    }  
  }
  return;
}

/*____________________________________________________
   Rebuild monitoring
*/

#define RBS_MAX_MONITOR_PATH 128
typedef struct rbs_monitor_file_list_s {    
  char          name[RBS_MAX_MONITOR_PATH];  
  uint64_t      mtime;
} RBS_MONITOR_FILE_LIST_S;

#define RBS_MONITOR_MAX_FILES   128
static RBS_MONITOR_FILE_LIST_S rbs_monitor_file_list[RBS_MONITOR_MAX_FILES];
/*
**____________________________________________________
** Purge excedent files
*/
void rbs_monitor_purge(void) {
  struct dirent * dirItem;
  struct stat     statBuf;
  DIR           * dir;
  uint32_t        nb,idx;
  uint32_t        older;
  char file_path[FILENAME_MAX];
  char * pChar;

  pChar = file_path;
  pChar += rozofs_string_append(pChar,rbs_status_root);
  
  /* Open core file directory */ 
  dir=opendir(file_path);
  if (dir==NULL) return;

  nb = 0;

  while ((dirItem=readdir(dir))!= NULL) {
    
    /* Skip . and .. */ 
    if (dirItem->d_name[0] == '.') continue;

    rozofs_string_append(pChar,dirItem->d_name); 
    
    if (strlen(file_path) >= RBS_MAX_MONITOR_PATH) {
      /* Too big : can not store it, so delete it */
      unlink(file_path);
      continue;
    }

    /* Get file date */ 
    if (stat(file_path,&statBuf) < 0) {   
      severe("rbs_monitor_purge : stat(%s) %s",file_path,strerror(errno));
      unlink(file_path);
      continue;	           
    }
      
    /* Maximum number of file not yet reached. Just register this one */
    if (nb < RBS_MONITOR_MAX_FILES) {
      rbs_monitor_file_list[nb].mtime = statBuf.st_mtime;
      strcpy(rbs_monitor_file_list[nb].name,file_path);      
      nb ++;
      continue;
    }

    /* Maximum number of file is reached. Remove the older */     

    /* Find older in already registered list */ 
    older = 0;
    for (idx=1; idx < RBS_MONITOR_MAX_FILES; idx ++) {
      if (rbs_monitor_file_list[idx].mtime < rbs_monitor_file_list[older].mtime) older = idx;
    }

    /* 
    ** If older in list is older than the last one read, 
    ** the last one read replaces the older in the array and the older is removed
    */
    if (rbs_monitor_file_list[older].mtime < (uint32_t)statBuf.st_mtime) {
      unlink(rbs_monitor_file_list[older].name);	
      rbs_monitor_file_list[older].mtime = statBuf.st_mtime;
      strcpy(rbs_monitor_file_list[older].name, file_path);
      continue;
    }
    /*
    ** Else the last read is removed 
    */
    unlink(file_path);
  }
  closedir(dir);  
}
/*
**____________________________________________________
** Purge excedent /tmp/rbs.xxx directories
*/
void rbs_tmp_purge(void) {
  struct dirent * dirItem;
  struct stat     statBuf;
  DIR           * dir;
  char            file_path[FILENAME_MAX];
  time_t          lastWeeks;
  
  /* Open /tmp file directory */ 
  dir=opendir("/tmp");
  if (dir==NULL) return;
  
  lastWeeks = time(NULL) - (14 * 24 * 3600);
  
  while ((dirItem=readdir(dir))!= NULL) {
    
    /* Skip . and .. */ 
    if (dirItem->d_name[0] == '.') continue;
    if (strncmp(dirItem->d_name,"rbs.",4)!= 0) continue;

    sprintf(file_path, "/tmp/%s", dirItem->d_name);
    
    /* Get file date */ 
    if (stat(file_path,&statBuf) < 0) {   
      severe("rbs_tmp_purge : stat(%s) %s",file_path,strerror(errno));
      clean_dir(file_path);
      continue;	           
    }

    if (statBuf.st_mtime < lastWeeks) { 
      clean_dir(file_path);
    }
  }
  closedir(dir);  
}

char * get_rebuild_status_file_name_to_use() {
  return rbs_monitor_file_path;
}

void rbs_monitor_file_update(void) {
    int    fd = -1;
    char * pEnd;
    char * path = get_rebuild_status_file_name_to_use();
    
    if ((fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IROTH)) < 0) {
        severe("can't open %s", path);
        return;
    }
    
    pEnd = format_status_file(myFormatedString);
    if (pwrite(fd,myFormatedString,pEnd-myFormatedString,0)<=0) {
      severe("pwrite(%s) %s",path, strerror(errno));
    }
    close(fd);
}
void rbs_monitor_update(char * global, char * local) {  

  if (global) {
    rozofs_string_append(rebuild_status, global);
  }
  
  if (local) {
    rozofs_string_append(rbs_monitor[rbs_index].status, local);       
  }
 
  rbs_monitor_file_update();
}

int rbs_monitor_display() {
  char cmdString[256];
  char * pChar = cmdString;
  
  if (quiet) return 0;

  if (rbs_monitor_file_path[0] == 0) return 0;

  pChar += rozofs_string_append(pChar,"cat ");
  pChar += rozofs_string_append(pChar,rbs_monitor_file_path);
  return system(cmdString);
}

/*
**____________________________________________________

  Save delay
*/
void save_consummed_delay(void) {
  char fname[1024];
  int  fd = -1;
    
  sprintf(fname,"%s/%s",get_rebuild_directory_name(parameter.rebuildRef),"delay");
  
  uint32_t delay = time(NULL) - loc_time + previous_delay;
  /*
  ** Save command 
  */
  if ((fd = open(fname, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IROTH)) < 0) {
    severe("can't open %s %s", fname,strerror(errno));
    return;
  }
  
  if (write(fd,&delay,sizeof(delay))<=0) {
    severe("write(%s) %s",fname,strerror(errno));
  }
  close(fd);
  //info("PID %s written",fname);
}
/*
**____________________________________________________

  Save delay
*/
uint32_t read_previous_delay() {
  char      fname[1024];
  int       fd = -1;
    
  sprintf(fname,"%s/%s",get_rebuild_directory_name(parameter.rebuildRef),"delay");
  
  /*
  ** Save command 
  */
  if ((fd = open(fname, O_RDONLY , S_IRWXU | S_IROTH)) < 0) {
    severe("can't open %s %s", fname,strerror(errno));
    return 0; 
  }
  
  if (read(fd,&previous_delay,sizeof(previous_delay))<=0) {
    severe("write(%s) %s",fname,strerror(errno));
    close(fd);
	return -1;
  }
  
  close(fd);
  return 0;
}
/*
**____________________________________________________

  Save command in command file
*/
void save_pid() {
  char fname[1024];
  int  fd = -1;
  pid_t pid = getpid();
    
  sprintf(fname,"%s/%s",get_rebuild_directory_name(parameter.rebuildRef),"pid");
  
  /*
  ** Save command 
  */
  if ((fd = open(fname, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IROTH)) < 0) {
    severe("can't open %s %s", fname,strerror(errno));
    return;
  }
  
  if (write(fd,&pid,sizeof(pid))<=0) {
    severe("write(%s) %s",fname,strerror(errno));
  }
  close(fd);
  //info("PID %s written",fname);
}
/*
**____________________________________________________

  Save command in command file
*/
void forget_pid() {
  char fname[1024];
    
  sprintf(fname,"%s/%s",get_rebuild_directory_name(parameter.rebuildRef),"pid");
  unlink(fname);
}
/*
**____________________________________________________

  Save command in command file
*/
pid_t read_pid() {
  char fname[1024];
  int  fd = -1;
  pid_t pid= 0;
    
  sprintf(fname,"%s/%s",get_rebuild_directory_name(parameter.rebuildRef),"pid");
  
  /*
  ** Save command 
  */
  if ((fd = open(fname, O_RDONLY, S_IRWXU | S_IROTH)) < 0) {
    if (errno != ENOENT) severe("can't open %s %s", fname,strerror(errno));
  }
  else {   
    if (read(fd,&pid,sizeof(pid))<=0) {
      severe("read(%s) %s",fname,strerror(errno));
    } 
    close(fd);
  }  
  return pid;
}
/*
**____________________________________________________

  Crash callback
*/
static void on_crash(int sig) {
    // Remove pid file
    forget_pid();
    closelog();
}    
/*
**--------------------FID hash table
*/

/*
** FID hash table to prevent registering 2 times
**   the same FID for rebuilding
*/
#define FID_TABLE_HASH_SIZE  (16*1024)

#define FID_MAX_ENTRY      31
typedef struct _rb_fid_entries_t {
    int                        count;
    int                        padding;
    struct _rb_fid_entries_t * next;   
    fid_t                      fid[FID_MAX_ENTRY];
    uint8_t                    chunk[FID_MAX_ENTRY];
} rb_fid_entries_t;

rb_fid_entries_t ** rb_fid_table=NULL;


/*
**
*/
static inline unsigned int fid_hash(void *key) {
    uint32_t hash = 0;
    uint8_t *c;
    for (c = key; c != key + 16; c++)
        hash = *c + (hash << 6) + (hash << 16) - hash;
    return hash % FID_TABLE_HASH_SIZE;
}
static inline void rb_hash_table_initialize() {
  int size;
  
  size = sizeof(void *)*FID_TABLE_HASH_SIZE;
  rb_fid_table = malloc(size);
  memset(rb_fid_table,0,size);
  rb_fid_table_count = 0;
}

int rb_hash_table_search(fid_t fid) {
  int      i;
  unsigned int idx = fid_hash(fid);
  rb_fid_entries_t * p;
  fid_t            * pF;
  
  p = rb_fid_table[idx];
  
  while (p != NULL) {
    pF = &p->fid[0];
    for (i=0; i < p->count; i++,pF++) {
      if (memcmp(fid, pF, sizeof (fid_t)) == 0) return 1;
    }
    p = p->next;
  }
  return 0;
}
int rb_hash_table_search_chunk(fid_t fid,int chunk) {
  int      i;
  unsigned int idx = fid_hash(fid);
  rb_fid_entries_t * p;
  fid_t            * pF;
  uint8_t          * pC;
  
  p = rb_fid_table[idx];
  
  while (p != NULL) {
    pF = &p->fid[0];
    pC = &p->chunk[0];
    
    for (i=0; i < p->count; i++,pF++,pC++) {
      if (*pC != chunk) continue;
      if (memcmp(fid, pF, sizeof (fid_t)) == 0) return 1;
    }
    p = p->next;
  }
  return 0;
}
rb_fid_entries_t * rb_hash_table_new(unsigned int idx) {
  rb_fid_entries_t * p;
    
  p = (rb_fid_entries_t*) malloc(sizeof(rb_fid_entries_t));
  p->count = 0;
  p->next = rb_fid_table[idx];
  rb_fid_table[idx] = p;
  
  return p;
}
rb_fid_entries_t * rb_hash_table_get(unsigned int idx) {
  rb_fid_entries_t * p;
    
  p = rb_fid_table[idx];
  if (p == NULL)                 p = rb_hash_table_new(idx);
  if (p->count == FID_MAX_ENTRY) p = rb_hash_table_new(idx);  
  return p;
}
void rb_hash_table_insert(fid_t fid) {
  unsigned int idx = fid_hash(fid);
  rb_fid_entries_t * p;
  
  p = rb_hash_table_get(idx);
  memcpy(p->fid[p->count],fid,sizeof(fid_t));
  p->count++;
  rb_fid_table_count++;
}
void rb_hash_table_insert_chunk(fid_t fid, int chunk) {
  unsigned int idx = fid_hash(fid);
  rb_fid_entries_t * p;
  
  p = rb_hash_table_get(idx);
  memcpy(p->fid[p->count],fid,sizeof(fid_t));
  p->chunk[p->count] = chunk;
  p->count++;
  rb_fid_table_count++;
}
void rb_hash_table_delete() {
  int idx;
  rb_fid_entries_t * p, * pNext;
  
  if (rb_fid_table == NULL) return;
  
  for (idx = 0; idx < FID_TABLE_HASH_SIZE; idx++) {
    
    p = rb_fid_table[idx];
    while (p != NULL) {
      pNext = p->next;
      free(p);
      p = pNext;
    }
  }
  
  free(rb_fid_table);
  rb_fid_table = NULL;
} 



/** Retrieves the list of bins files to rebuild from a storage
 *
 * @param rb_stor: storage contacted.
 * @param cid: unique id of cluster that owns this storage.
 * @param sid: the unique id for the storage to rebuild.
 *
 * @return: the number of file to rebuild.  -1 otherwise (errno is set)
 */
uint64_t rbs_get_rb_entry_list_one_storage(rb_stor_t *rb_stor, cid_t cid, sid_t sid, int *cfgfd, int failed) {
    uint16_t slice = 0;
    uint8_t spare = 0;
    uint8_t device = 0;
    uint64_t cookie = 0;
    uint8_t eof = 0;
    sid_t dist_set[ROZOFS_SAFE_MAX];
    bins_file_rebuild_t * children = NULL;
    bins_file_rebuild_t * iterator = NULL;
    bins_file_rebuild_t * free_it = NULL;
    int            ret;
    rozofs_rebuild_entry_file_t file_entry;
    int      entry_size;
    uint64_t count=0;
    
    DEBUG_FUNCTION;

  
    memset(dist_set, 0, sizeof (sid_t) * ROZOFS_SAFE_MAX);

    // While the end of the list is not reached
    while (eof == 0) {

        // Send a request to storage to get the list of bins file(s)
        if (rbs_get_rb_entry_list(&rb_stor->mclient, cid, rb_stor->sid, sid,
                &device, &spare, &slice, &cookie, &children, &eof) != 0) {
            severe("rbs_get_rb_entry_list failed: %s\n", strerror(errno));
            return -1;;
        }

        iterator = children;

        // For each entry 
        while (iterator != NULL) {

	  /*
	  ** Check that not too much storages are failed for this layout
	  */
	  failed = 0;
	  switch(iterator->layout) {

	    case LAYOUT_2_3_4:
	      if (failed>1) {
		severe("%d failed storages on LAYOUT_2_3_4",failed);
		return -1;
	      }
	      break;
	    case LAYOUT_4_6_8:
	      if (failed>2) {
		severe("%d failed storages on LAYOUT_4_6_8",failed);
		return -1;
	      }
	      break;	
	    case LAYOUT_8_12_16:
	      if (failed>2) {
		severe("%d failed storages on LAYOUT_8_12_16",failed);
		return -1;
	      }
	      break;	
	    default:	         	 		 	 
	      severe("Unexpected layout %d",iterator->layout);
	      return -1;
	  }


          // Verify if this entry is already present in list
	  if (rb_hash_table_search(iterator->fid) == 0) { 
		
	  entry_size = rbs_entry_size_from_layout(iterator->layout);

          rb_hash_table_insert(iterator->fid);

	  memcpy(file_entry.fid,iterator->fid, sizeof (fid_t));
	  file_entry.bsize       = iterator->bsize;
          file_entry.todo        = 1;    
	  file_entry.block_start = 0;  
	  file_entry.block_end   = -1;
	  file_entry.layout      = iterator->layout;
          file_entry.error       = rozofs_rbs_error_none;
          memcpy(file_entry.dist_set_current, iterator->dist_set_current, sizeof (sid_t) * ROZOFS_SAFE_MAX);	    

#if 0
  {
    char fid_string[128];
    int i;
    rozofs_uuid_unparse(file_entry.fid,fid_string);  
    printf("record FID %s bsize %d from %llu to %llu dist %d",
          fid_string,file_entry.bsize,
         (long long unsigned int) file_entry.block_start, 
	 (long long unsigned int) file_entry.block_end,
	 file_entry.dist_set_current[0]);
   for (i=1;i<4;i++) printf("-%d", file_entry.dist_set_current[i]);
   printf("\n");
  }  
#endif
          ret = write(cfgfd[current_file_index],&file_entry,entry_size); 
	  if (ret != entry_size) {
	    severe("can not write file cid%d sid%d %d %s",cid,sid,current_file_index,strerror(errno));
	  }	    
	  current_file_index++;
	  count++;
	  if (current_file_index >= parameter.parallel) current_file_index = 0; 		

        }
        free_it = iterator;
        iterator = iterator->next;
        free(free_it);
      }
    }

    return count;
}








int rbs_initialize(cid_t cid, sid_t sid, const char *storage_root, 
                   uint32_t dev, uint32_t dev_mapper, uint32_t dev_red) {
    int status = -1;
    DEBUG_FUNCTION;

    /*
    ** Only initialize the storage in case 
    ** of a local rebuild (device or node).
    ** In case of FID rebuild, the rebuild can occur remotly.
    */
    if (parameter.type != rbs_rebuild_type_fid) {
      // Initialize the storage to rebuild 
      if (storage_initialize(storage_to_rebuild, cid, sid, storage_root,
		  dev,
		  dev_mapper,
		  dev_red,
                  NULL/*don't care about spare files */) != 0)
          goto out;
    }

    status = 0;
out:
    return status;
}

static int storaged_initialize() {
    int status = -1;
    list_t *p = NULL;
    DEBUG_FUNCTION;

   storaged_nrstorages = 0;

    storaged_nb_io_processes = 1;
    
    storio_nb_threads = common_config.nb_disk_thread;

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
int rbs_sanity_check(cid_t cid, sid_t sid, const char *root, uint32_t dev, uint32_t dev_mapper, uint32_t dev_red) {

    int status = -1;

    DEBUG_FUNCTION;

    // Try to initialize the storage to rebuild
    if (rbs_initialize(cid, sid, root, dev, dev_mapper, dev_red) != 0) {
        // Probably a path problem
        REBUILD_FAILED("Can't initialize rebuild storage (cid:%u; sid:%u;"
                " path:%s): %s\n", cid, sid, root, strerror(errno));
        goto out;
    }

    status = 0;

out:
    return status;
}
int rbs_sanity_cid_sid_check(cid_t cid, sid_t sid) {

    int status = -1;

    DEBUG_FUNCTION;

    
    // Initialize the list of cluster(s)
    list_init(&cluster_entries);
    
    // Try to get the list of storages for this cluster ID
    pExport_host = rbs_get_cluster_list(&rpcclt_export, parameter.rbs_export_hostname, 
                                        parameter.storaged_geosite, cid, &cluster_entries);
    if (pExport_host == NULL) {	    
        REBUILD_FAILED("Can't get list of storages from export for cluster %u",cid);
	severe("rbs_get_cluster_list(export=\"%s\",cid=%u) %s", parameter.rbs_export_hostname, cid, strerror(errno));
        goto out;
    }

    // Check the list of cluster
    if (rbs_check_cluster_list(&cluster_entries, cid, sid) != 0) {
        REBUILD_FAILED("No such storage %u/%u\n", cid, sid);
        goto out;
    }

    status = 0;

out:
    // Free cluster(s) list
    rbs_release_cluster_list(&cluster_entries);

    return status;
}
/** Check each storage to rebuild
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int rbs_write_count_file(int cid, int sid, rbs_file_type_e ftype, uint64_t count) {
  char            filename[FILENAME_MAX];
  char          * pChar;
  char          * dir;
  int             fd;

  dir = get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid,ftype);

  /*
  ** Write the count file
  */
  pChar = filename;
  pChar += rozofs_string_append(pChar,dir);
  pChar += rozofs_string_append(pChar,"/count");
  fd = open(filename, O_WRONLY | O_APPEND | O_CREAT | O_TRUNC,0755);
  if (fd<0) {
    severe("open(%s) %s\n",filename,strerror(errno));
    return -1;
  }	
  if (write(fd,&count, sizeof(uint64_t)) != sizeof(uint64_t)) {
    severe("write(%d,%s) %s\n",fd,filename,strerror(errno));
    close(fd);
    return -1;			  
  }
  close(fd);
  //info("%s cid/sid %d/%d : %llu files",rbs_file_type2string(ftype), cid,sid,(long long unsigned int)count);
  return 0;  
}
/** Check each storage to rebuild
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
static int rbs_check() {
    list_t *p = NULL;
    int status = -1;
    DEBUG_FUNCTION;

    // For each storage present on configuration file

    list_for_each_forward(p, &storaged_config.storages) {
        storage_config_t *sc = list_entry(p, storage_config_t, list);

        // Sanity check for rebuild this storage
        if (rbs_sanity_check( 
	        sc->cid, sc->sid, sc->root,
		sc->device.total,sc->device.mapper,sc->device.redundancy) != 0)
            goto out;  
    }
    status = 0;
out:
    return status;
}

int rbs_storio_reinit(cid_t cid, sid_t sid, uint8_t dev, uint8_t reinit) {
    int status = -1;
    int ret;
    rb_stor_t stor;

    ret = rbs_get_storage(&rpcclt_export, parameter.rbs_export_hostname, parameter.storaged_geosite, cid, sid, &stor) ;
    if (ret == 0) {
      status = sclient_clear_error_rbs(&stor.sclients[0], cid, sid, dev, reinit);
    } 
    return status;
}
/*__________________________________________________________________________
** Build list of FID to rebuild from export
**
** Use export remote command server to 
** - rozofs_rcmd_rebuild_list() 
**   build the list of file to rebuild on export node calling 
**   rozo_rbsList utility remotly,
** - rozofs_rcmd_getfile() 
**   transfer back the generated lists
** - rozofs_rcmd_rebuild_list_clear() 
**   cleanup local directory on export node
**
** @retval 0 on success. -1 on error
**==========================================================================*/
int rbs_build_job_list_from_export() {
  int       socketId = -1;
  char      command[512];
  char      local[256];
  char      remote[256];
  int       idx;
  int       res;
  int       para;
  char    * pChar;

  int delay = time(NULL);
      
  // Initialize the list of cluster(s)
  list_init(&cluster_entries);
  
  /*
  ** All local directories have already been created previously;
  ** Write storage.conf that will be used by rebuilders in it.
  */
  for (idx=0; idx<nb_rbs_entry; idx++) {
    uint8_t   vlayout;
    uint16_t  vid;
    
    // Get the list of storages for this cluster ID
    pExport_host = rbs_get_cluster2_list(&rpcclt_export, parameter.rbs_export_hostname, 
                                         parameter.storaged_geosite, 
					 rbs_stor_configs[idx].cid, 
					 &cluster_entries,
					 &vlayout,
					 &vid);
    if (pExport_host == NULL) {					
      severe("rbs_get_cluster2_list failed exportd %s cid %d %s", 
		      parameter.rbs_export_hostname, rbs_stor_configs[idx].cid, strerror(errno));
      REBUILD_FAILED("Can not connect to export.");  
      return -1;
    }  

    // Initialize the storage to rebuild
    if (rbs_initialize(rbs_stor_configs[idx].cid, 
	               rbs_stor_configs[idx].sid, 
		       rbs_stor_configs[idx].root, 
		       rbs_stor_configs[idx].device.total, 
		       rbs_stor_configs[idx].device.mapper, 
		       rbs_stor_configs[idx].device.redundancy) != 0) {
      severe("can't init. storage to rebuild (cid:%u;sid:%u;path:%s)",
                rbs_stor_configs[idx].cid, rbs_stor_configs[idx].sid, rbs_stor_configs[idx].root);
      REBUILD_FAILED("Bad cid/sid config %d/%d.",rbs_stor_configs[idx].cid,rbs_stor_configs[idx].sid);  
      return -1;
    }
	
    strcpy(storage_config.export_hostname,parameter.rbs_export_hostname);
//    strcpy(storage_config.config_file,parameter.storaged_config_file);
    storage_config.site   = parameter.storaged_geosite;
//    storage_config.device = parameter.rbs_device_number;
    storage_config.ftype  = rbs_stor_configs[idx].ftype;
    storage_config.cid    = rbs_stor_configs[idx].cid;
    storage_config.sid    = rbs_stor_configs[idx].sid;    
    rbs_stor_configs[idx].status = RBS_STATUS_PROCESSING_LIST;
    
    /*
    ** Write storage configuration file 
    */
    rbs_write_storage_config_file(parameter.rebuildRef, &storage_config);
    
    // Free cluster(s) list
    rbs_release_cluster_list(&cluster_entries);
  }
  
  
  /*
  ** Connect to the remote command server on exportd node
  */
  socketId = rozofs_rcmd_connect_to_server(pExport_host);
  if (socketId == -1) {
    REBUILD_FAILED("Can not connect to export %s.",pExport_host);  
    return -1;
  }

  /*
  ** Prepare parameters for rozo_rbsList command
  */
  pChar = command;
  pChar += sprintf(pChar, 
                   "-p %d -r %d -E %s ",
                   parameter.parallel, 
                   parameter.rebuildRef, 
                   common_config.export_temporary_dir);
                   
  /*
  ** Rebuild limited to spare
  */                 
  if (parameter.filetype == rbs_file_type_spare) {
    pChar += sprintf(pChar,"--spare ");
  }
  /*
  ** Rebuild limited to nominal
  */                 
  else if (parameter.filetype == rbs_file_type_nominal) {
    pChar += sprintf(pChar,"--nominal ");
  }      
                   
  /* 
  ** In case of local test simulation, provide export.conf full path
  */
  if (parameter.simu != NULL) {
    pChar += sprintf(pChar, "-c %s ",parameter.simu);
  }
  
  /*
  ** Add list of cid:sid
  */
  pChar += sprintf(pChar,"-i %d:%d", 
                   (int) rbs_stor_configs[0].cid, 
		   (int) rbs_stor_configs[0].sid);
    
  for (idx=1; idx<nb_rbs_entry; idx++) {
    /*
    ** Only nominal or only spare. One entry per cid:sid => push every entry
    */
    if (parameter.filetype != rbs_file_type_all) {
      pChar += sprintf(pChar, ",%d:%d",rbs_stor_configs[idx].cid, rbs_stor_configs[idx].sid);
      continue;
    }
    /*
    ** rebuild nominal as well as spare files. 2 entries per cid:sid so push only 
    ** the cid:sid when encountering nominal entries
    */
    if (rbs_stor_configs[idx].ftype == rbs_file_type_nominal){
      pChar += sprintf(pChar, ",%d:%d",rbs_stor_configs[idx].cid, rbs_stor_configs[idx].sid);        
    }	
  }
  
  /*
  ** Run the remote command
  */
  res = rozofs_rcmd_rebuild_list(socketId,command);
  if (res != rozofs_rcmd_status_success) {
    REBUILD_FAILED("Can not make rebuild job list %s", rozofs_rcmd_status_e2String(res));  
    goto out;
  }    
  
  /*
  ** Get result files
  */
  for (idx=0; idx<nb_rbs_entry; idx++) {

    /*
    ** count file
    */
    sprintf(local,"%s/rbs.%d/cid%d_sid%d_%d/count",
            common_config.storage_temporary_dir,
            parameter.rebuildRef, 
            rbs_stor_configs[idx].cid, 
            rbs_stor_configs[idx].sid, 
            rbs_stor_configs[idx].ftype);

    sprintf(remote,"%s/rebuild.%d/cid%d_sid%d_%d/count",
            common_config.export_temporary_dir,
            parameter.rebuildRef, 
            rbs_stor_configs[idx].cid, 
            rbs_stor_configs[idx].sid, 
            rbs_stor_configs[idx].ftype);
    
    res = rozofs_rcmd_getfile(socketId, remote, local);
    if (res != rozofs_rcmd_status_success) {
      REBUILD_FAILED("Can not retrieve file %s -> %s. %s", remote, local, rozofs_rcmd_status_e2String(res));  
      goto out;
    }        
      
    /*
    ** Get jobs
    */  
    for (para=0; para<parameter.parallel; para++) {
    
      sprintf(local,"%s/rbs.%d/cid%d_sid%d_%d/job%d",
              common_config.storage_temporary_dir,
              parameter.rebuildRef, 
              rbs_stor_configs[idx].cid, 
              rbs_stor_configs[idx].sid, 
              rbs_stor_configs[idx].ftype,
              para);

      sprintf(remote,"%s/rebuild.%d/cid%d_sid%d_%d/job%d",
              common_config.export_temporary_dir,
              parameter.rebuildRef, 
              rbs_stor_configs[idx].cid, 
              rbs_stor_configs[idx].sid, 
              rbs_stor_configs[idx].ftype,
              para);

      res = rozofs_rcmd_getfile(socketId,remote, local);
      if (res != rozofs_rcmd_status_success) {
        REBUILD_FAILED("Can not retrieve file %s -> %s. %s", remote, local, rozofs_rcmd_status_e2String(res));  
        goto out;
      }        
    }
  }  

out:
  /*
  ** Cleanup temporary directory under export node
  */
  rozofs_rcmd_rebuild_list_clear(socketId,common_config.export_temporary_dir, parameter.rebuildRef);  

  /*
  ** Disconnect from server
  */
  rozofs_rcmd_disconnect_from_server(socketId);  

  /*
  ** Get deuration
  */
  delay = time(NULL) - delay;
  
  
  if (res != rozofs_rcmd_status_success) {
    return -1;
  }  
      
  for (idx=0; idx<nb_rbs_entry; idx++) {
  
    rbs_monitor[idx].list_building_sec = delay/nb_rbs_entry;  
    rbs_monitor[idx].nb_files = rbs_read_file_count(parameter.rebuildRef,
	                                            rbs_stor_configs[idx].cid,
						    rbs_stor_configs[idx].sid,
						    rbs_stor_configs[idx].ftype);					
  }
  
  return 0;  
}
/** Build a list with just one FID
 *
 * @param cid: unique id of cluster that owns this storage.
 * @param sid: the unique id for the storage to rebuild.
 * @param fid2rebuild: the FID to rebuild
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
static int rbs_build_one_fid_list(cid_t cid, sid_t sid, uint8_t layout, uint8_t bsize, uint8_t * dist) {
  int            fd; 
  rozofs_rebuild_entry_file_t file_entry;
  char         * dir;
  char           filename[FILENAME_MAX];
  int            ret;
  int            i;
  /*
  ** Create FID list file files
  */
  dir = get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid,rbs_file_type_all);

  char * pChar = filename;
  pChar += rozofs_string_append(pChar,dir);
  pChar += rozofs_string_append(pChar,"/job0");
      
  fd = open(filename,O_CREAT | O_TRUNC | O_WRONLY, 0640);
  if (fd == -1) {
    severe("Can not open file %s %s", filename, strerror(errno));
    return -1;
  }

  memcpy(file_entry.fid, parameter.fid2rebuild, sizeof (fid_t));
  file_entry.bsize       = bsize;  
  file_entry.todo        = 1;      
  /*
  ** Where to start rebuild from 
  */    
  if (parameter.chunk == -1) {
    file_entry.block_start = 0;
  }
  else {
    file_entry.block_start =  parameter.bstart;
    file_entry.block_start += (parameter.chunk * ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(bsize));
  }
  
  /*
  ** Where to stop rebuild
  */  
  if (parameter.chunk == -1) {
    file_entry.block_end   = -1;  
  }
  else {  
    file_entry.block_end   = (parameter.chunk * ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(bsize));
    if (parameter.bstop == -1) {
      file_entry.block_end += (ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(bsize))-1;
    }
    else {
      file_entry.block_end += parameter.bstop;
    }   
  }
  
  file_entry.error       = rozofs_rbs_error_none;
  file_entry.layout      = layout;
  
  for(i=0; i<ROZOFS_SAFE_MAX; i++) {
    file_entry.dist_set_current[i] = dist[i];
  }    

  int entry_size = rbs_entry_size_from_layout(layout);
  
  ret = write(fd,&file_entry,entry_size); 
  if (ret != entry_size) {
    severe("can not write file cid%d sid%d %s",cid,sid,strerror(errno));
    return -1;
  }  
  
  close(fd);
 
  /*
  ** Write the count file
  */
  rbs_write_count_file(cid,sid,rbs_file_type_all,1);  
  
  return 0;   
}
/** During rebuild update periodicaly the stats
 *
 */
void periodic_stat_update(int * fd) {
  RBS_MONITOR_S         stat;
  int i;
  ROZOFS_RBS_COUNTERS_T counters;
    
  memcpy(&stat,&rbs_monitor[rbs_index], sizeof(stat));  
  stat.done_files      = 0;
  stat.resecured       = 0;
  stat.deleted         = 0;    
  stat.written         = 0;
  stat.written_spare   = 0;
  stat.read            = 0;
  stat.read_spare      = 0;

  for (i=0; i< parameter.parallel; i++) {
    if (pread(fd[i], &counters, sizeof(counters), 0) == sizeof(counters)) {
      stat.done_files      += counters.done_files;
      stat.resecured       += counters.resecured;
      stat.deleted         += counters.deleted;
      stat.written         += counters.written;
      stat.written_spare   += counters.written_spare;	
      stat.read            += counters.read;
      stat.read_spare      += counters.read_spare;
    }
  }

  memcpy(&rbs_monitor[rbs_index],&stat,sizeof(stat));
  rbs_monitor_file_update();
}
  
/** Rebuild list just produced 
 *
 */
int rbs_do_list_rebuild(int cid, int sid, rbs_file_type_e ftype) {
  char         * dirName;
  char           cmd[FILENAME_MAX];
  int            status;
  int            failure;
  int            success;
  int            fd[128];
  char           fname[128];
  struct timespec timeout;
  sigset_t        mask;
  sigset_t        orig_mask; 
  pid_t           pid;
  int             instance;
  struct stat     buf;
  int             idx;
  char          * argv[32];
  
  sigemptyset (&mask);
  sigaddset (&mask, SIGCHLD); 
  if (sigprocmask(SIG_BLOCK, &mask, &orig_mask) < 0) {
    severe("sigprocmask %s", strerror(errno));
    return 1;
  } 
   
  /*
  ** Start one rebuild process par rebuild file
  */
  dirName = get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid,ftype);  
  
  failure = 0;
  success = 0;
  	  
  /*
  ** Loop on distibution sub directories
  */
  for (instance=0; instance<parameter.parallel; instance++) {

    char * pChar = fname;
    pChar += rozofs_string_append(pChar,dirName);
    pChar += rozofs_string_append(pChar,"/stat");
    pChar += rozofs_u32_append(pChar,instance);
    
    fd[instance] = open(fname, O_RDONLY|O_CREAT, 0755);
    if (fd[instance]<0) {
      severe("open(%s) %s",fname, strerror(errno));
    }  


    // Start a process for a job file
    pChar = fname;
    pChar += rozofs_string_append(pChar,dirName);
    pChar += rozofs_string_append(pChar,"/job");
    pChar += rozofs_u32_append(pChar,instance);
    
	// Look up for the job file
    if (stat(fname, &buf)<0) {
      // No such job list any more
      if (errno == ENOENT) {
	success++;
	continue;
      }
      // Other errors
      severe("stat(%s) %s",fname, strerror(errno));	  
      failure++;
      continue;	  
    }
	
    // File exist but is empty
    if (buf.st_size == 0) {
      unlink(fname);
      success++;
      continue;	  
    }

    pid = fork();  
    if (pid == 0) {    
      pChar = cmd;
//      pChar += rozofs_string_append(pChar,"valgrind --leak-check=full --track-origins=yes --log-file=/root/valgrind storage_list_rebuilder -c ");
      pChar += rozofs_string_append(pChar,"storage_list_rebuilder -c ");
      pChar += rozofs_u32_append(pChar,cid);
      pChar += rozofs_string_append(pChar," -s ");
      pChar += rozofs_u32_append(pChar,sid);
      pChar += rozofs_string_append(pChar," -r ");
      pChar += rozofs_u32_append(pChar,parameter.rebuildRef);
      pChar += rozofs_string_append(pChar," -i ");
      pChar += rozofs_u32_append(pChar,instance);
      pChar += rozofs_string_append(pChar," -f ");
      pChar += rozofs_string_append(pChar,rbs_file_type2string(ftype));
      if (parameter.resecure) {
	pChar += rozofs_string_append(pChar," --reSecure");      
      }
      if (parameter.relocate) {
	pChar += rozofs_string_append(pChar," --relocate");            
      }
      if (parameter.throughput) {
        pChar += rozofs_string_append(pChar," -t ");
        pChar += rozofs_u32_append(pChar,parameter.throughput);
      }
      if (nolog) {
	pChar += rozofs_string_append(pChar," --nolog");
      }
      else if (quiet) {
	pChar += rozofs_string_append(pChar," --quiet");
      }

      if (verbose) {
	pChar += rozofs_string_append(pChar," --verbose");
      }
      
      pChar = cmd;
      idx   = 0;
      while ( idx < 31 ) {
	argv[idx++] = pChar;
	while ((*pChar != ' ') && (*pChar != 0)) pChar++;
	if (*pChar == 0) break;
	*pChar = 0;
	pChar++;
      }  
      argv[idx] = NULL;

      execvp(argv[0],&argv[0]);
      int error = errno;
      exit(error); 
    }
  }


  periodic_stat_update(fd);
  
  while (parameter.parallel > (failure+success)) {
    int                   ret;

    timeout.tv_sec  = 20;
    timeout.tv_nsec = 0;
    
    ret = sigtimedwait(&mask, NULL, &timeout);
    if (ret < 0) {
      if (errno != EAGAIN) continue;
    }  

     
    /* Check for rebuild sub processes status */    
    while ((pid = waitpid(-1,&status,WNOHANG)) > 0) {
	
      status = WEXITSTATUS(status);

      if (status != 0) failure++;
      else             success++;
	  
      periodic_stat_update(fd);
    }
     
	 
    periodic_stat_update(fd);
    
    // Rebuild is paused. Forward signal to every child
    if (sigusr_received) {
       kill(0,SIGUSR1);
    }
  }
  
  
  for (instance=0; instance<parameter.parallel; instance++) {
    if (fd[instance]>0) close(fd[instance]);
  }


  if (failure != 0) {
    if (!nolog) {
      if (sigusr_received) {
        info("%d list rebuild processes paused upon %d",failure,parameter.parallel);
      }
      else {
        info("%d list rebuild processes failed upon %d",failure,parameter.parallel);
      } 
    }   
    return -1;
  }
  return 0;
}
/** Update monitoring file while listing localy files to rebuild
 *
 * @param idx: the rbs_entry index
 * @param count: count of file
 *
 */
static void rbs_update_file_count(int idx) {
  rbs_monitor[idx].list_building_sec = time(NULL) - loc_time;
  rbs_monitor[idx].nb_files          = rb_fid_table_count;      
  rbs_monitor_file_update();
}
 
/** Retrieves the list of bins files to rebuild from the available disks
 *
 * @param monitorIdx: the monitor netry index
 * @param cluster_entries: list of cluster(s).
 * @param cid: unique id of cluster that owns this storage.
 * @param sid: the unique id for the storage to rebuild.
 * @param device: the missing device identifier 
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
static int rbs_build_device_missing_list_one_cluster(int   monitorIdx,
                                                     cid_t cid, 
						     sid_t sid,
						     int device_to_rebuild,
                                                     int spare_it) {
  char           dir_path[FILENAME_MAX];						     
  char           slicepath[FILENAME_MAX];						     
  char           filepath[FILENAME_MAX];						     
  int            device_it;
  DIR           *dir1;
  struct dirent *file;
  rozofs_stor_bins_file_hdr_t file_hdr; 
  rozofs_rebuild_entry_file_t file_entry;
  int            idx;
  char         * dir;
  char           filename[FILENAME_MAX];
  int            cfgfd[MAXIMUM_PARALLEL_REBUILD_PER_SID];
  int            ret;
  int            slice;
  uint8_t        chunk;  
  int            entry_size=0;
  char         * pChar;
  /*
  ** Create FID list file files
  */
  dir = get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid,spare_it);

  for (idx=0; idx < parameter.parallel; idx++) {

    pChar = filename;
    pChar += rozofs_string_append(pChar,dir);
    pChar += rozofs_string_append(pChar,"/job");
    pChar += rozofs_u32_append(pChar,idx);

    cfgfd[idx] = open(filename,O_CREAT | O_TRUNC | O_WRONLY, 0755);
    if (cfgfd[idx] == -1) {
      severe("Can not open file %s %s", filename, strerror(errno));
      return 0;
    }
  }   

  // Loop on all the devices
  for (device_it = 0; device_it < storage_to_rebuild->device_number;device_it++) {

    // Do not read the disk to rebuild
    if (device_it == device_to_rebuild) continue;

  // For spare and no spare
  //for (spare_it = 0; spare_it < 2; spare_it++) {

    // Build path directory for this layout and this spare type        	
    char * pChar = dir_path;
    pChar += rozofs_string_append(pChar,storage_to_rebuild->root);
    *pChar++ = '/';
    pChar += rozofs_u32_append(pChar,device_it); 
    pChar += rozofs_string_append(pChar,"/hdr_");
    pChar += rozofs_u32_append(pChar,spare_it); 


    // Check that this directory already exists, otherwise it will be create
    if (access(dir_path, F_OK) == -1) continue;

    for (slice=0; slice < (common_config.storio_slice_number); slice++) {
    
      storage_build_hdr_path(slicepath, storage_to_rebuild->root, device_it, spare_it, slice);

      // Open this directory
      dir1 = opendir(slicepath);
      if (dir1 == NULL) continue;


      // Loop on header files in slice directory
      while ((file = readdir(dir1)) != NULL) {
        int i;
        char * error;
        fid_t   fid;

	if (file->d_name[0] == '.') continue;

        // Read the file
	pChar = filepath;
	pChar += rozofs_string_append(pChar,slicepath);
	*pChar++ = '/';
	pChar += rozofs_string_append(pChar,file->d_name);
        rozofs_uuid_parse(file->d_name, fid);

        error = rozofs_st_header_read(filepath, cid, sid, fid, &file_hdr);         
        if (error != NULL) {
           continue;
        }

	// When not in a relocation case, rewrite the file header on this device if it should
	if ((!parameter.relocate)&&(!parameter.resecure)) {
          for (i=0; i < storage_to_rebuild->mapper_redundancy; i++) {
	        int dev;

            dev = storage_mapper_device(file_hdr.fid,i,storage_to_rebuild->mapper_modulo);

 	    if (dev == device_to_rebuild) {
	      // Let's re-write the header file  	      
              storage_build_hdr_path(filepath, storage_to_rebuild->root, device_to_rebuild, spare_it, slice);
              ret = storage_write_header_file(NULL,dev,filepath,&file_hdr);
	      if (ret != 0) {
	        severe("storage_write_header_file(%s) %s",filepath,strerror(errno))
	      }	
	      break;
	    } 
	  } 
	}

        // Check whether this file has some chunk of data on the device to rebuild
	for (chunk=0; chunk<ROZOFS_STORAGE_MAX_CHUNK_PER_FILE; chunk++) {
            int dev;
            dev = rozofs_st_header_get_chunk(&file_hdr,chunk);

	    if (dev == ROZOFS_EOF_CHUNK)  break;

            if (dev != device_to_rebuild) continue;

            /*
	    ** This file has a chunk on the device to rebuild
	    ** Check whether this FID is already set in the list
	    */
	    if (rb_hash_table_search_chunk(file_hdr.fid,chunk) == 0) {
	      rb_hash_table_insert_chunk(file_hdr.fid,chunk);	
	    }
	    else {
	      continue;
	    }	      

	    entry_size = rbs_entry_size_from_layout(file_hdr.layout);

	    memcpy(file_entry.fid,file_hdr.fid, sizeof (fid_t));
	    file_entry.bsize       = file_hdr.bsize;
            file_entry.todo        = 1;     
	    file_entry.block_start = chunk * ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(file_hdr.bsize);  
	    file_entry.block_end   = file_entry.block_start + ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(file_hdr.bsize) -1;  
            file_entry.error       = rozofs_rbs_error_none;
	    file_entry.layout      = file_hdr.layout;
            
            int size2copy = sizeof(file_hdr.distrib);
            memcpy(file_entry.dist_set_current, file_hdr.distrib, size2copy);	    
            memset(&file_entry.dist_set_current[size2copy], 0,  sizeof(file_entry.dist_set_current)-size2copy);	    

            ret = write(cfgfd[current_file_index],&file_entry,entry_size); 
	    if (ret != entry_size) {
	      severe("can not write file cid%d sid%d %d %s",cid,sid,current_file_index,strerror(errno));
	    }
	    current_file_index++;
	    if (current_file_index >= parameter.parallel) current_file_index = 0; 
	}

      } // End of loop in one slice 
      closedir(dir1);
      
      rbs_update_file_count(monitorIdx); 
    } // End of slices
  } 

  for (idx=0; idx < parameter.parallel; idx++) {
    close(cfgfd[idx]);
    cfgfd[idx] = -1;
  } 

  /*
  ** Write the count file
  */
  rbs_write_count_file(cid,sid,spare_it,rb_fid_table_count); 
    
  return 0;   
}

static int time_start = 0;
   
int rbs_build_job_list_local(int idx) {
    rbs_stor_config_t *stor_confs = &rbs_stor_configs[idx];
    int status = -1;
    int failed,available;
    uint8_t   cid = stor_confs->cid;
    uint8_t   sid = stor_confs->sid;

    DEBUG_FUNCTION;

    time_start = time(NULL);

    rb_hash_table_initialize();

    // Initialize the storage to rebuild
    if (rbs_initialize(cid, sid, stor_confs->root, 
                       stor_confs->device.total, stor_confs->device.mapper, stor_confs->device.redundancy) != 0) {
        severe("can't init. storage to rebuild (cid:%u;sid:%u;path:%s)",
                cid, sid, stor_confs->root);
        goto out;
    }
    strcpy(storage_config.export_hostname,parameter.rbs_export_hostname);
//    strcpy(storage_config.config_file,parameter.storaged_config_file);
    storage_config.site = parameter.storaged_geosite;
//    storage_config.device = parameter.rbs_device_number;
    storage_config.ftype  = stor_confs->ftype;    
    storage_config.cid    = cid;
    storage_config.sid    = sid;
    rbs_write_storage_config_file(parameter.rebuildRef, &storage_config);

    // Get the list of storages for this cluster ID
    pExport_host = rbs_get_cluster_list(&rpcclt_export, parameter.rbs_export_hostname, 
                                        parameter.storaged_geosite, cid, &cluster_entries);
    if (pExport_host == NULL) {					
        severe("rbs_get_cluster_list failed (cid: %u) : %s", cid, strerror(errno));
        goto out;
    }

    // Check the list of cluster
    if (rbs_check_cluster_list(&cluster_entries, cid, sid) != 0)
        goto out;

    // Get connections for this given cluster
    rbs_init_cluster_cnts(&cluster_entries, cid, sid,&failed,&available);

    // Build the list from the available data on local disk
    if (rbs_build_device_missing_list_one_cluster(idx, cid, sid, parameter.rbs_device_number, stor_confs->ftype) != 0) {
        goto out;
    }		    		
    
    
    // No file to rebuild
    if (rb_fid_table_count==0) {
      REBUILD_MSG("No file to rebuild.");
      rbs_empty_dir (get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid,rbs_file_type_all));
      unlink(get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid,rbs_file_type_all));
    }
    else { 
      REBUILD_MSG("%llu files to rebuild by %d processes",
           (unsigned long long int)rb_fid_table_count,parameter.parallel);
    }	   
     
    status = 0;
    rbs_monitor[idx].list_building_sec = time(NULL) - time_start;
    rbs_monitor[idx].nb_files          = rb_fid_table_count;    
out:    
    rb_hash_table_delete();    
    // Free cluster(s) list
    rbs_release_cluster_list(&cluster_entries);
    return status;
}
int rbs_build_job_lists_one_fid(rbs_stor_config_t *stor_confs) {
    int status = -1;
    int failed,available;
    uint8_t   cid = stor_confs->cid;
    uint8_t   sid = stor_confs->sid;

    DEBUG_FUNCTION;

    int time_start = time(NULL);

    rb_hash_table_initialize();

    // Initialize the storage to rebuild
    if (rbs_initialize(cid, sid, stor_confs->root, 
                       stor_confs->device.total, stor_confs->device.mapper, stor_confs->device.redundancy) != 0) {
        severe("can't init. storage to rebuild (cid:%u;sid:%u;path:%s)",
                cid, sid, stor_confs->root);
        goto out;
    }
    strcpy(storage_config.export_hostname,parameter.rbs_export_hostname);
//    strcpy(storage_config.config_file,parameter.storaged_config_file);
    storage_config.site = parameter.storaged_geosite;
//    storage_config.device = parameter.rbs_device_number;
    storage_config.ftype  = stor_confs->ftype;    
    storage_config.cid    = cid;
    storage_config.sid    = sid;
    rbs_write_storage_config_file(parameter.rebuildRef, &storage_config);

    // Get the list of storages for this cluster ID
    pExport_host = rbs_get_cluster_list(&rpcclt_export, parameter.rbs_export_hostname, 
                                        parameter.storaged_geosite, cid, &cluster_entries);
    if (pExport_host == NULL) {					
        severe("rbs_get_cluster_list failed (cid: %u) : %s", cid, strerror(errno));
        goto out;
    }

    // Check the list of cluster
    if (rbs_check_cluster_list(&cluster_entries, cid, sid) != 0)
        goto out;

    // Get connections for this given cluster
    rbs_init_cluster_cnts(&cluster_entries, cid, sid,&failed,&available);

    // One FID to rebuild
    {
      uint32_t   bsize;
      uint8_t    layout; 
      ep_mattr_t attr;
      
      // Resolve this FID thanks to the exportd
      if (rbs_get_fid_attr(&rpcclt_export, pExport_host, parameter.fid2rebuild, &attr, &bsize, &layout) != 0)
      {
        if (errno == ENOENT) {
	  status = -2;
	  REBUILD_FAILED("Unknown FID");
	}
	else {
	  REBUILD_FAILED("Can not get attributes from export \"%s\" %s",pExport_host,strerror(errno));
	}
	goto out;
      }
      
      if (rbs_build_one_fid_list(cid, sid, layout, bsize, (uint8_t*) attr.sids) != 0)
        goto out;
      rb_fid_table_count = 1;	
      parameter.parallel = 1;
    }
    
    REBUILD_MSG("%llu files to rebuild by %d processes",
           (unsigned long long int)rb_fid_table_count,parameter.parallel);	   
     
    status = 0;
    rbs_monitor[rbs_index].list_building_sec = time(NULL) - time_start;
out:    
    rb_hash_table_delete();    
    rbs_monitor[rbs_index].nb_files = rb_fid_table_count;
    // Free cluster(s) list
    rbs_release_cluster_list(&cluster_entries);
    return status;
}
/*
** Display the list of remaining FID in a given rebuild job list
** given by its file name
*/
int zecount;
void storaged_rebuild_list_read(char * fid_list) {
  int        fd = -1;
  uint64_t   offset;
  rozofs_rebuild_entry_file_t   file_entry;
  char fidString[40];
      
  fd = open(fid_list,O_RDONLY);
  if (fd < 0) {
      printf("Can not open file %s %s",fid_list,strerror(errno));
      goto error;
  }
  
  offset = 0;

  while (pread(fd,&file_entry,sizeof(rozofs_rebuild_entry_file_t),offset)>0) {
  
    offset += rbs_entry_size_from_layout(file_entry.layout);;    
    
    /* Next file to rebuild */ 
    if (file_entry.todo) {
      rozofs_uuid_unparse(file_entry.fid,fidString);
      if (parameter.list == 1) {
        if (zecount) {
          printf(",\n");
	}
	zecount++;
	printf("    { \"FID\" : \"%s\", \"error\" : \"%s\" }",fidString, ROZOFS_RBS_ERROR_E2String(file_entry.error));
      }
      else {
        printf("%s\n",fidString);
      }	
    }
  }


error: 
  if (fd != -1) close(fd);   
}

/** Display the list of remaining FIDs
 *
 */
void rbs_list_remaining_fid(void) {
  char         * dirName;
  DIR           *dir0;
  DIR           *dir1;
  struct dirent *file0;
  struct dirent *file1;
  char           fname[512];
     
  /*
  ** Get rebuild job directory
  */
  dirName = get_rebuild_directory_name(parameter.rebuildRef);
  
  /*
  ** Open this directory
  */
  dir0 = opendir(dirName);
  if (dir0 == NULL) {
    if (errno==ENOENT) {
      REBUILD_FAILED("Rebuild identifier %d does not exist any more.",parameter.rebuildRef);
    }
    else {
      severe("opendir(%s) %s", dirName, strerror(errno));
    }  
    return;
  } 

  if (parameter.list == 1) {
    printf("{ \"remaining FID\" : [\n");
    zecount = 0;    
  }
    
  /*
  ** Loop on distibution sub directories
  */
  while ((file0 = readdir(dir0)) != NULL) {
  
    if (strncmp(file0->d_name,"cid",3)!=0)  continue; 
    
    /*
    ** For each subdirectory : ie cid/sid
    */
    char * pChar = fname;
    pChar += rozofs_string_append(pChar,dirName);
    *pChar++ = '/';
    pChar += rozofs_string_append(pChar,file0->d_name);    
    dir1 = opendir(fname);
    if (dir1 == NULL) {
      severe("opendir(%s) %s", fname, strerror(errno));
      continue;
    }

    /*
    ** Read the storage configuration file
    */
    if (rbs_read_storage_config_file(fname, &storage_config) == NULL) {
      severe("rbs_read_storage_config_file(%s) %s", fname, strerror(errno));
      continue;
    }
	
    	
    /*
    ** Loop on distibution sub directories
    */
    while ((file1 = readdir(dir1)) != NULL) {

      if (strncmp(file1->d_name,"job",3)!=0)  continue;
      
      /*
      ** For each file of each subdirectory
      */
      char * pChar = fname;
      pChar += rozofs_string_append(pChar,dirName);
      *pChar++ = '/';
      pChar += rozofs_string_append(pChar,file0->d_name);    
      *pChar++ = '/';
      pChar += rozofs_string_append(pChar,file1->d_name);          
      storaged_rebuild_list_read(fname);
    }
    
    closedir(dir1);  	
  }
  if (parameter.list == 1) {
    printf("\n  ],\n  \"count\" : %d\n}\n",zecount);
  }  
  closedir(dir0);
}
/** Stop a running rebuild
 *
 */
pid_t rbs_get_running_pid() {
  pid_t pid;
  char  fname[512];
  int   fd;  
    
  pid = read_pid();
  if (pid == 0) { 
    return 0;
  }
      
  sprintf(fname,"/proc/%d/cmdline",pid);
  
  /*
  ** Save command 
  */
  if ((fd = open(fname, O_RDONLY, S_IRWXU | S_IROTH)) < 0) {
    if (errno != ENOENT) severe("can't open %s %s", fname,strerror(errno));
    return 0;    
  }

  if (read(fd,fname,sizeof(fname))<=0) {
    severe("read(%s) %s",fname,strerror(errno));
    return 0;
  }
  close(fd);
  
  if (strstr(fname, "storage_rebuild")==NULL) {
    return 0;
  }

  return pid;
}
/** Pause a running rebuild
 *
 */
void rbs_rebuild_pause() {
  pid_t pid;

  pid = rbs_get_running_pid();
  if (pid == 0) {
    REBUILD_MSG("Rebuild %d is not running",parameter.rebuildRef); 
    return;
  }

  kill(pid,SIGUSR1);

  REBUILD_MSG("Rebuild %d will be paused within few minutes",parameter.rebuildRef); 
  printf("Resume this rebuild   : storage_rebuild -id %d -resume\n", parameter.rebuildRef); 

  return;
}
/** Abort a running rebuild
 *
 */
void rbs_rebuild_abort() {
  pid_t pid;

  pid = rbs_get_running_pid();
  if (pid != 0) {
    kill(pid,SIGINT);
    sleep(1);
  }
  clean_dir(get_rebuild_directory_name(parameter.rebuildRef));

  REBUILD_MSG("Rebuild %d will be aborted within few minutes",parameter.rebuildRef); 
  return;
}
/** Starts a thread for rebuild given storage(s)
 *
 * @param nb: Number of entries.
 * @param v: table of storages configurations to rebuild.
 */
static inline int rebuild_storage_thread(rbs_stor_config_t *stor_confs) {
  int    result;
  int    delay=1;
  int    cid, sid;
  rbs_file_type_e ftype;


  while (run_loop < parameter.max_reloop) {

    run_loop++;

    /*
    ** When relooping, let some time for things to get repaired magicaly
    */
    if (run_loop != 1) {

      rbs_monitor_display();  
      REBUILD_MSG("Rebuild failed ! Attempt #%u/%u will start in %d minutes", run_loop, parameter.max_reloop, delay);      
          	
      sleep(delay * 60);       
      if (delay < 60) delay = 2 *delay;
      if (delay > 60) delay = 60;
    }

    /*
    ** Let's process the clusters one after the other
    */
    for (rbs_index = 0; rbs_index < nb_rbs_entry; rbs_index++) {

      cid = stor_confs[rbs_index].cid;
      sid = stor_confs[rbs_index].sid;
      ftype = stor_confs[rbs_index].ftype;

      /* 
      ** Depending on the rebuild status
      */
      switch(stor_confs[rbs_index].status) {

	/*
	** The list of rebuilding jobs is not yet done for this sid
	*/
	case RBS_STATUS_BUILD_JOB_LIST:

	/*
	** Try or retry to rebuild the job list
	*/ 
	case RBS_STATUS_PROCESSING_LIST:
        case RBS_STATUS_FAILED:
          rbs_monitor_update("running","running");

	  if (rbs_monitor[rbs_index].nb_files == 0) {
            REBUILD_MSG("cid %d sid %d %s.  No file to rebuild.", cid, sid, rbs_file_type2string(ftype));
            rbs_monitor_update("running","No file to rebuild.");
            stor_confs[rbs_index].status = RBS_STATUS_SUCCESS;
            /*
            ** Remove cid/sid directory 
            */
            //clean_dir(get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid,ftype));  
            /*
            ** Clear errors
            */
            if (parameter.type != rbs_rebuild_type_fid) {
              if (parameter.type == rbs_rebuild_type_device) {
                rbs_storio_reinit(cid, sid, parameter.rbs_device_number, 0);
              }   
              else {
	        rbs_storio_reinit(cid, sid, 0xFF, 0); 
	      }  
            } 
	    continue;
	  }		  
	  stor_confs[rbs_index].status = RBS_STATUS_PROCESSING_LIST;	      	    
	  result = rbs_do_list_rebuild(cid, sid, ftype);

	  if (sigusr_received) {
	    /* Rebuild is interupted */
            goto paused;
	    
	  }		      
	  
	  /*
	  ** Success 
	  */
          if (result == 0) {
            rbs_monitor_update("running","success");
	    REBUILD_MSG("cid %d sid %d %s. Rebuild success.", cid, sid,rbs_file_type2string(ftype));  

	    stor_confs[rbs_index].status = RBS_STATUS_SUCCESS;
	    /*
	    ** Remove cid/sid directory 
	    */
	    //clean_dir(get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid,ftype));  
	    /*
	    ** Clear errors
	    */
            if (parameter.type != rbs_rebuild_type_fid) {            
	      if (parameter.type == rbs_rebuild_type_device) {
	        rbs_storio_reinit(cid, sid, parameter.rbs_device_number, 0);
	      }   
	      else {
	        rbs_storio_reinit(cid, sid, 0xFF, 0); 
	      } 
            }  
	    continue;
	  }
	  
	  /*
	  ** Failure
	  */
          if (result == -1) {
            rbs_monitor_update("running","failed");
	    REBUILD_MSG("cid %d sid %d %s. Rebuild failed.", cid, sid, rbs_file_type2string(ftype)); 	    
	    stor_confs[rbs_index].status = RBS_STATUS_FAILED;
	    continue;	      
	  }


	  /*
	  ** Error
	  */
          rbs_monitor_update("running","error");
	  REBUILD_MSG("cid %d sid %d %s. Rebuild error.", cid, sid, rbs_file_type2string(ftype)); 
	  
          stor_confs[rbs_index].status = RBS_STATUS_ERROR;	    
	  continue;	      

	default:
	  continue;
      }
    }

    /*
    ** Check whether some reloop is to be done
    */
    for (rbs_index = 0; rbs_index < nb_rbs_entry; rbs_index++) {
      if ((stor_confs[rbs_index].status != RBS_STATUS_ERROR)
      &&  (stor_confs[rbs_index].status != RBS_STATUS_SUCCESS)) break;
    }
    if (rbs_index == nb_rbs_entry) {
      /*
      ** Everything is finished 
      */
      REBUILD_MSG("Rebuild %d completed.",parameter.rebuildRef);      
      rbs_monitor_update("completed",NULL);
      return 0;
    }
    sprintf(rebuild_status,"waiting %d min before reloop",delay);
    rbs_monitor_update(rebuild_status,NULL);
  }  	    

  REBUILD_MSG("Rebuild %d failed.",parameter.rebuildRef);      
  rbs_monitor_update("failed",NULL);		     
  return -1;
  
paused:
  REBUILD_MSG("Rebuild %d paused.",parameter.rebuildRef);
  rbs_monitor_update("paused",NULL);
  /*
  ** Save elpased delay in order 
  ** to reread it on resume
  */
  save_consummed_delay();   		     
  return -1;       
}

/*
**____________________________________________________
*/
/*
  Allocate a device for a file
  
   @param st: storage context
*/
uint32_t storio_device_mapping_allocate_device(storage_t * s, uint8_t layout, sid_t * distrib) {
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
/*
**____________________________________________________

  Save rebuild status file name
*/
void save_rebuild_status_file_name() {
  char fname[1024];
  int  fd = -1;
    
  sprintf(fname,"%s/%s",get_rebuild_directory_name(parameter.rebuildRef),"rbs_status");
  
  /*
  ** Save command 
  */
  if ((fd = open(fname, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IROTH)) < 0) {
    severe("can't open %s %s", fname,strerror(errno));
    return;
  }
  
  dprintf(fd,"%s",rbs_monitor_file_path);
  close(fd);
}
/*
**____________________________________________________

  Save rebuild status file name
*/
void read_rebuild_status_file_name() {
  char fname[1024];
  int  fd = -1;
    
  sprintf(fname,"%s/%s",get_rebuild_directory_name(parameter.rebuildRef),"rbs_status");
  
  /*
  ** Save command 
  */
  if ((fd = open(fname, O_RDONLY , S_IRWXU | S_IROTH)) < 0) {
    severe("can't open %s %s", fname,strerror(errno));
    return;
  }
  
  if (pread(fd, &rbs_monitor_file_path, sizeof(rbs_monitor_file_path), 0)<0) {
    severe("can't read %s %s", fname, strerror(errno));
  }  
  
  close(fd);
}
/*
**____________________________________________________

  Save command in command file
*/
void save_command() {
  char fname[1024];
  int  fd = -1;
    
  sprintf(fname,"%s/%s",get_rebuild_directory_name(parameter.rebuildRef),"command");
  
  /*
  ** Save command 
  */
  if ((fd = open(fname, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IROTH)) < 0) {
    severe("can't open %s %s", fname,strerror(errno));
    return;
  }
  
  dprintf(fd,"%s",command);
  close(fd);
}
/*
**____________________________________________________

  read command in command file
*/
char * read_command(int rebuildRef, char * cmd) {
  char fname[1024];
  int  fd = -1;
    
  sprintf(fname,"%s/%s",get_rebuild_directory_name(rebuildRef),"command");
  
  /*
  ** Save command 
  */
  if ((fd = open(fname, O_RDONLY , S_IRWXU | S_IROTH)) < 0) {
    severe("can't open %s %s", fname,strerror(errno));
    return NULL;
  }
  
  if (pread(fd, cmd, 1024, 0)<0) {
    severe("can't read %s %s", fname, strerror(errno));
    close(fd);
    return NULL;
  }
  close(fd);
  return cmd;
}
/*
**____________________________________________________

  read command in command file
*/
char saved_command[1024];
int preload_command(int rebuildRef, rbs_parameter_t * par) {
  char * pChar;
  char *argv[32];
  int   argc;
    
  if (read_command(rebuildRef,saved_command) == NULL) {
    return -1;
  }

  //info("preload %s",saved_command);
    
  argc       = 0;
  pChar      = saved_command;
  
  while (*pChar != 0) {
  
    while ((*pChar == ' ')||(*pChar == '\t')) *pChar++ = 0;
    if (*pChar == 0) break;

    argv[argc++] = pChar;
    pChar++;
        
    while ((*pChar != 0)&&(*pChar != ' ')&&(*pChar != '\t')) pChar++;
    if (*pChar == 0) break;    

  }  

  parse_command(argc, argv, par); 
  return 0;
}
/*________________________________________________________________
** Prepare the cid/sid list to process in rbs_stor_configs array
**/
static int prepare_list_of_storage_to_rebuild() {
  rbs_file_type_e ftype;
  int             ret;
  char          * dir;
  int             idx;
  int             nb;
  list_t        * p = NULL;

  /*
  ** Reset array of storage to processs
  */
  nb_rbs_entry = 0;
  memset(&rbs_stor_configs, 0,sizeof(rbs_stor_configs));

  /*
  ** One FID rebuild
  */
  if (parameter.type == rbs_rebuild_type_fid) {
    ftype = rbs_file_type_all;
    strncpy(rbs_stor_configs[nb_rbs_entry].export_hostname, parameter.rbs_export_hostname,
    ROZOFS_HOSTNAME_MAX);
    rbs_stor_configs[nb_rbs_entry].cid = parameter.cid;
    rbs_stor_configs[nb_rbs_entry].sid = parameter.sid;
    rbs_stor_configs[nb_rbs_entry].ftype = ftype;
    rbs_stor_configs[nb_rbs_entry].device.total      = 1;
    rbs_stor_configs[nb_rbs_entry].device.mapper     = 1;
    rbs_stor_configs[nb_rbs_entry].device.redundancy = 1;
    rbs_stor_configs[nb_rbs_entry].status            = RBS_STATUS_BUILD_JOB_LIST;

    strcpy(rbs_stor_configs[nb_rbs_entry].root, "/");

    rbs_monitor[nb_rbs_entry].cid        = parameter.cid;
    rbs_monitor[nb_rbs_entry].sid        = parameter.sid;
    rbs_monitor[nb_rbs_entry].nb_files   = 0;	
    rbs_monitor[nb_rbs_entry].done_files = 0;
    rbs_monitor[nb_rbs_entry].resecured  = 0;
    rbs_monitor[nb_rbs_entry].deleted    = 0;	
    rbs_monitor[nb_rbs_entry].ftype      = ftype;
    strcpy(rbs_monitor[nb_rbs_entry].status,"to do");
    rbs_monitor[nb_rbs_entry].list_building_sec = 0;	
    nb_rbs_entry++;

    // Create a temporary directory to receive the job list files 
    dir = get_rebuild_sid_directory_name(parameter.rebuildRef,parameter.cid,parameter.sid,ftype);
    ret = mkdir(dir,ROZOFS_ST_BINS_FILE_MODE);
    if ((ret != 0)&&(errno!=EEXIST)) {
      severe("mkdir(%s) %s", dir, strerror(errno));
      REBUILD_FAILED("Can not reate directory %s %s.",dir,strerror(errno));  
      return -1;
    }
    
    rbs_build_job_lists_one_fid(&rbs_stor_configs[0]);
    return 0;
  }	      

  /*
  ** Other rebuild type. Get cid/sid from the configuration file
  */
  list_for_each_forward(p, &storaged_config.storages) {

    storage_config_t *sc = list_entry(p, storage_config_t, list);

    /*
    ** If a specific sid is to be rebuilt, skip the other
    */
    if ((parameter.cid!=-1)&&(parameter.sid!=-1)) {
      if ((parameter.cid != sc->cid) || (parameter.sid != sc->sid)) continue; 
    }
    /*
    ** Rebuild one device. Check the device number
    */
    if (parameter.type == rbs_rebuild_type_device) {
    
      /*
      ** Bad device number
      */
      if(parameter.rbs_device_number >= sc->device.total) {
        REBUILD_FAILED("No such device number %d.",parameter.rbs_device_number);  
        return -1;  
      }
      /*
      ** Only one device? This is equivalent to a SID rebuild
      */	
      if (sc->device.total == 1){
        parameter.type = rbs_rebuild_type_storage;
      }
    }
      
    /* 
    ** Only recored nominal file type. Spare file type will be added after
    ** since nominal files have to be rebuilt before spare ones.
    */
    if (parameter.filetype == rbs_file_type_spare)
      ftype = rbs_file_type_spare;
    else  
      ftype = rbs_file_type_nominal;
    strncpy(rbs_stor_configs[nb_rbs_entry].export_hostname, parameter.rbs_export_hostname,
    ROZOFS_HOSTNAME_MAX);
    rbs_stor_configs[nb_rbs_entry].cid = sc->cid;
    rbs_stor_configs[nb_rbs_entry].sid = sc->sid;
    rbs_stor_configs[nb_rbs_entry].ftype = ftype;
    rbs_stor_configs[nb_rbs_entry].device.total      = sc->device.total;
    rbs_stor_configs[nb_rbs_entry].device.mapper     = sc->device.mapper;
    rbs_stor_configs[nb_rbs_entry].device.redundancy = sc->device.redundancy;
    rbs_stor_configs[nb_rbs_entry].status            = RBS_STATUS_BUILD_JOB_LIST;

    strncpy(rbs_stor_configs[nb_rbs_entry].root, sc->root, PATH_MAX);

    rbs_monitor[nb_rbs_entry].cid        = sc->cid;
    rbs_monitor[nb_rbs_entry].sid        = sc->sid;
    rbs_monitor[nb_rbs_entry].nb_files   = 0;	
    rbs_monitor[nb_rbs_entry].done_files = 0;
    rbs_monitor[nb_rbs_entry].resecured  = 0;
    rbs_monitor[nb_rbs_entry].deleted    = 0;	
    rbs_monitor[nb_rbs_entry].ftype      = ftype;
    strcpy(rbs_monitor[nb_rbs_entry].status,"to do");
    rbs_monitor[nb_rbs_entry].list_building_sec = 0;	
    nb_rbs_entry++;

    // Create a temporary directory to receive the list files 
    dir = get_rebuild_sid_directory_name(parameter.rebuildRef,sc->cid,sc->sid,ftype);
    ret = mkdir(dir,ROZOFS_ST_BINS_FILE_MODE);
    if ((ret != 0)&&(errno!=EEXIST)) {
      severe("mkdir(%s) %s", dir, strerror(errno));
      REBUILD_FAILED("Can not reate directory %s %s.",dir,strerror(errno));  
      return -1;
    }  
  }

  if (nb_rbs_entry==0) {
    REBUILD_FAILED("No such cid/sid %d/%d.", parameter.cid, parameter.sid);
    return -1;           
  }

  /*
  ** Add the spare file type to rebuild in rbs_stor_configs table
  ** Since only nominal file type have been registered.
  ** (Nominal projections have to be rebuild prior to spare projections)
  */
  if (parameter.filetype == rbs_file_type_all) {
    nb = nb_rbs_entry;
    for (idx=0; idx<nb; idx++) {

      // Copy the configuration for the storage to rebuild
      ftype = rbs_file_type_spare; 
      memcpy(&rbs_stor_configs[nb+idx],&rbs_stor_configs[idx], sizeof(rbs_stor_configs[0]));
      rbs_stor_configs[nb+idx].ftype     = ftype;

      memcpy(&rbs_monitor[nb+idx], &rbs_monitor[idx], sizeof(rbs_monitor[0]));
      rbs_monitor[nb+idx].ftype  = ftype;
      nb_rbs_entry++;

      // Create a temporary directory to receive the list files 
      dir = get_rebuild_sid_directory_name(parameter.rebuildRef,
	                                          rbs_stor_configs[idx].cid,
						  rbs_stor_configs[idx].sid,
						  ftype);

      ret = mkdir(dir,ROZOFS_ST_BINS_FILE_MODE);
      if ((ret != 0)&&(errno!=EEXIST)) {
	severe("mkdir(%s) %s", dir, strerror(errno));
        REBUILD_FAILED("Can not reate directory %s %s.",dir,strerror(errno));  
        return -1;  
      }
    }         
  }

  rbs_monitor_update("initiated",NULL);
  
  /*
  ** When rebuilding one device, the list is done localy from the other disks
  ** else the liste is done on th export node.
  */
  if (parameter.type == rbs_rebuild_type_device) {
    for (idx=0; idx< nb_rbs_entry; idx++) {
      rbs_build_job_list_local(idx); 
    } 
    return 0;   
  }
  
  /*
  ** Ask the export for the list of jobs
  */
  return rbs_build_job_list_from_export(); 
}

/*________________________________________________________________
** Start one rebuild process for each storage to rebuild
*/
static inline int rbs_rebuild_process() {
    int ret;
    int status = -1;

    /*
    ** Create a temporary directory to receive the list files
    */  
    clean_dir(get_rebuild_directory_name(parameter.rebuildRef));
    ret = mkdir(get_rebuild_directory_name(parameter.rebuildRef),ROZOFS_ST_BINS_FILE_MODE);
    if (ret != 0) {
      severe("Can not create directory %s : %s",get_rebuild_directory_name(parameter.rebuildRef), strerror(errno));
      goto out;
    }  
    
    /*
    ** Save command in file "command" of the temporary directory
    */
    save_command();

    /*
    ** Save rebuild status file name for later pause/resume
    */
    save_rebuild_status_file_name();       
        
    /*
    ** Save pid
    */
    save_pid();

    /*
    ** Write marks on disk for the case of one disk rebuild.
    ** In case of more than one disk rebuild, this call will have 
    ** no effect
    */
    rbs_write_rebuild_marks();
    
    /*
    ** Build the array of cid/sid to rebuild in rbs_stor_configs[]
    ** and initialize monitoring statistics.
    */
    if (prepare_list_of_storage_to_rebuild()<0) {
      goto out;
    }  

    /*
    ** Write marks on disk for the case several disk to rebuild
    ** since now the list of jobs has been done
    */
    rbs_write_rebuild_marks();
    
    /*
    ** Process to the rebuild
    */    
    run_loop = 0;
    status = rebuild_storage_thread(rbs_stor_configs);
    rbs_monitor_display();  

    /*
    ** Clean pid file
    */
    forget_pid();

    /*
    ** Remove temporary directory
    */
    if (status==0) {
      clean_dir(get_rebuild_directory_name(parameter.rebuildRef));
    } 
    
out:
    /*
    ** Purge excedent old rebuild result files
    */
    rbs_monitor_purge();
    rbs_tmp_purge();
    return status;
}
/** Start one rebuild process for each storage to rebuild
 */
static inline int rbs_rebuild_resume() {
  char             * dirName;
  DIR              * dir0;
  struct dirent    * file0;
  DIR              * dir1;
  int                cid,sid;
  int                ret;
  int                status = -1;
  storage_t        * sc=NULL;
  char               fname[1024];
  rbs_file_type_e    ftype,readftype;
  
  memset(&rbs_stor_configs, 0,sizeof(rbs_stor_configs));
     
  /*
  ** Start one rebuild process par rebuild file
  */
  dirName = get_rebuild_directory_name(parameter.rebuildRef);
   
  
  for (ftype=rbs_file_type_nominal; ftype <= rbs_file_type_spare; ftype++) {
    /*
    ** Open this directory
    */
    dir0 = opendir(dirName);
    if (dir0 == NULL) {
      if (errno==ENOENT) {
	REBUILD_FAILED("Rebuild identifier %d does not exist any more.",parameter.rebuildRef);
      }
      else {
	severe("opendir(%s) %s", dirName, strerror(errno));
      }  
      return status;
    } 	  
    /*
    ** Loop on distibution sub directories
    */
    while ((file0 = readdir(dir0)) != NULL) {

      if (strcmp(file0->d_name,".")==0)  continue;
      if (strcmp(file0->d_name,"..")==0) continue;    

      /*
      ** Scan cid/sid
      */
      ret = sscanf(file0->d_name, "cid%d_sid%d_%u",&cid, &sid, &readftype);
      if (ret != 3) {
	//severe("Unexpected directory name %s/%s", dirName, file0->d_name);
	continue;
      }
      if (readftype != ftype) continue;

      // For each storage in configuration file
      if (parameter.type == rbs_rebuild_type_fid) {
        /*
	** FID rebuild. Do not check the local storage configuration
	** Rebuild can be processed remotly.
	** Simulate a storage sconfig !
	*/
        sc = storage_to_rebuild;
	sc->cid = cid;
	sc->sid = sid;
	sc->device_number     = 1;
	sc->mapper_modulo     = 1;
	sc->mapper_redundancy = 1;
	strcpy(sc->root,"/");
        
      }
      else {
      	sc = storaged_lookup(cid,sid);
	if (sc == NULL) {
	  severe("Unexpected cid/sid %d/%d", cid, sid);
	  continue;
	}
      }	


      // Copy the configuration for the storage to rebuild
      strncpy(rbs_stor_configs[nb_rbs_entry].export_hostname, parameter.rbs_export_hostname, ROZOFS_HOSTNAME_MAX);
      rbs_stor_configs[nb_rbs_entry].cid   = cid;
      rbs_stor_configs[nb_rbs_entry].sid   = sid;
      rbs_stor_configs[nb_rbs_entry].ftype = ftype;

      rbs_stor_configs[nb_rbs_entry].device.total      = sc->device_number;
      rbs_stor_configs[nb_rbs_entry].device.mapper     = sc->mapper_modulo;
      rbs_stor_configs[nb_rbs_entry].device.redundancy = sc->mapper_redundancy;
      rbs_stor_configs[nb_rbs_entry].status            = RBS_STATUS_FAILED;
      strncpy(rbs_stor_configs[nb_rbs_entry].root, sc->root, PATH_MAX);

      rbs_monitor[nb_rbs_entry].cid        = cid;
      rbs_monitor[nb_rbs_entry].sid        = sid;
      rbs_monitor[nb_rbs_entry].ftype      = ftype;    
      rbs_monitor[nb_rbs_entry].nb_files   = rbs_read_file_count(parameter.rebuildRef,cid,sid,ftype); 
      rbs_monitor[nb_rbs_entry].done_files = 0;
      rbs_monitor[nb_rbs_entry].resecured  = 0;
      rbs_monitor[nb_rbs_entry].deleted    = 0;	
      strcpy(rbs_monitor[nb_rbs_entry].status,"to do");

      sprintf(fname,"%s/%s",get_rebuild_directory_name(parameter.rebuildRef),file0->d_name);
      dir1 = opendir(fname);
      if (dir1 == NULL) {
	severe("opendir(%s) %s", fname, strerror(errno));
	nb_rbs_entry++;
	continue;
      }

      /*
      ** Read the storage configuration file
      */
      if (rbs_read_storage_config_file(fname, &storage_config) == NULL) {
	severe("rbs_read_storage_config_file(%s) %s", fname, strerror(errno));
	continue;
      }

      if (rbs_monitor[nb_rbs_entry].nb_files == 0) {
	/* No file to rebuild => the list is to be built */
	rbs_stor_configs[nb_rbs_entry].status = RBS_STATUS_BUILD_JOB_LIST;
      }    
      nb_rbs_entry++; 
      closedir(dir1);
    }
    closedir(dir0);
  }
    
  /*
  ** Save pid
  */
  save_pid();
    
  // Read previously elapsed delay 
  read_previous_delay();   

  /*
  ** Put a mark on the devices to rebuild
  */
  rbs_write_rebuild_marks();
	
  /*
  ** Process to the rebuild
  */   
  status = rebuild_storage_thread(rbs_stor_configs);
  rbs_monitor_display();  
  forget_pid();
  
  /*
  ** Remove temporary directory
  */
  if (status!=-1) {
    clean_dir(get_rebuild_directory_name(parameter.rebuildRef));
  } 

  /*
  ** Purge excedent old rebuild result files
  */
  rbs_monitor_purge();
  return status;
}

static void storaged_release() {
    DEBUG_FUNCTION;
    int i;
    list_t *p, *q;
    
    if (storaged_nrstorages==0) return;

    for (i = 0; i < storaged_nrstorages; i++) {
        storage_release(&storaged_storages[i]);
    }
    storaged_nrstorages = 0;

    // Free config

    list_for_each_forward_safe(p, q, &storaged_config.storages) {

        storage_config_t *s = list_entry(p, storage_config_t, list);
        free(s);
    }
}
     
/*-----------------------------------------------------------------------------
**
**  Stop handler
**
**----------------------------------------------------------------------------
*/
static void on_stop() {
    DEBUG_FUNCTION;   
    
    storaged_release();
    closelog();
    // Kill all sub-processes
    if (sigusr_received) {
      kill(-getpid(),SIGTERM);  
    }  
}
  
/*-----------------------------------------------------------------------------
**
**  SIGUSR1 receiving handler
**
**----------------------------------------------------------------------------
*/
void rbs_cath_sigusr(int sig){
  sigusr_received = 1;
  signal(SIGUSR1, rbs_cath_sigusr);  
}
/*-----------------------------------------------------------------------------
**
**  M A I N
**
**----------------------------------------------------------------------------
*/
int main(int argc, char *argv[]) {
    int ret;
    int status = -1;
    
    memset(&rpcclt_export,0,sizeof(rpcclt_export));
    
    /*
    ** Change local directory to "/"
    */
    if (chdir("/")!= 0) {}

    rozofs_layout_initialize();

    /*
    ** read common config file
    */
    common_config_read(NULL);         

    uma_dbg_record_syslog_name("RBS");
    
    // Init of the timer configuration
    rozofs_tmr_init_configuration();

    /*
    ** Check whether this is a resume command
    */
    int rebuildRef = is_command_resume(argc, argv);

    /*
    ** Read parameters
    */
    rbs_conf_init(&parameter);
    if (rebuildRef != 0) {
      /* 
      ** When resume command, pre-initialize the parameters with the 
      ** ones of the original command
      */
      if (preload_command(rebuildRef, &parameter) == -1) {
        usage("No such rebuild id %d\n",rebuildRef);	 
      } 
    }
    /*
    ** Parse command parameters
    */
    parse_command(argc, argv, &parameter);

    /*
    ** Process listing of failed FID
    */
    if (parameter.list) {
      rbs_list_remaining_fid();
      exit(EXIT_SUCCESS);
    }

    /*
    ** Process listing of failed FID
    */
    if (parameter.pause) {
      quiet = 0;
      rbs_rebuild_pause();
      exit(EXIT_SUCCESS);
    }    
    /*
    ** Process listing of failed FID
    */
    if (parameter.abort) {
      quiet = 0;
      rbs_rebuild_abort();
      exit(EXIT_SUCCESS);
    }   
        
    if (parameter.resume) {
      /*
      ** Check the rebuild is not running
      */
      if (rbs_get_running_pid()!=0) {
        REBUILD_MSG("Rebuild %d is already running\n",parameter.rebuildRef);
        exit(EXIT_FAILURE);
      }   
      /*
      ** Read saved command for the monitoring file
      */
      read_command(parameter.rebuildRef,command);
    } 
    
    /*
    ** Initalize the command for the monitoring file 
    */
    else {
      char * p = command;
      int i;
      
      for (i=0; i< argc; i++) {
        p += rozofs_string_append(p, argv[i]);
	*p++ = ' ';
      }
      *p = 0;
      /*
      ** Do not log in the case of FID rebuild
      */
      if (!nolog) {	 
        info("%s",command);
      }  
    }      

    /*
    ** SIGUSR handler
    */
    signal(SIGUSR1, rbs_cath_sigusr);  

    
    /*
    ** Declare the signal handler that may generate core files,
    ** and attach a crash callback 
    */
    setsid(); // Start a new session
    rozofs_signals_declare("storage_rebuild", common_config.nb_core_file);
    rozofs_attach_crash_cbk(on_crash);
        
    /*
    ** Clear errors and reinitialize disk
    */
    if (parameter.clear) {
      
      if (rbs_storio_reinit(parameter.cid, parameter.sid, parameter.rbs_device_number, 1)!=0) {
        REBUILD_MSG("Can't reset error on cid %d sid %d .",parameter.cid,parameter.sid);
        exit(EXIT_FAILURE);  
      }
      
      REBUILD_MSG("cid %d sid %d device %d re-initialization",parameter.cid,parameter.sid,parameter.rbs_device_number);
      if (parameter.clear==2) {
        exit(EXIT_SUCCESS);
      }
    }
       

    /*
    ** When only one FID rebuild may be done remotly
    ** while other rebuilds are local and require configuration checking.
    */
    rbs_status_root = ROZOFS_RUNDIR_RBS_SPARE;
    
    if (parameter.type != rbs_rebuild_type_fid) {
    
      rbs_status_root = ROZOFS_RUNDIR_RBS_REBUILD;    
    
      // Initialize the list of storage config
      if (sconfig_initialize(&storaged_config) != 0) {
          quiet = 0;
          REBUILD_FAILED("Can't initialize storaged config.");
          goto error;
      }
      // Read the configuration file
      if (parameter.cid == -1) {
	ret = sconfig_read(&storaged_config, parameter.storaged_config_file,0);
      }
      else {
	ret = sconfig_read(&storaged_config, parameter.storaged_config_file,parameter.cid);    
      }  
      if (ret < 0) {
          quiet = 0;
          REBUILD_FAILED("Failed to parse storage configuration file %s.",parameter.storaged_config_file);
          goto error;
      }
      // Check the configuration
      if (sconfig_validate(&storaged_config) != 0) {
          quiet = 0;
          REBUILD_FAILED("Inconsistent storage configuration file %s.", parameter.storaged_config_file);
          goto error;
      }
      // Check rebuild storage configuration if necessary
      if (rbs_check() != 0) goto error;
   

      // Initialization of the storage configuration
      if (storaged_initialize() != 0) {
          quiet = 0;
          REBUILD_FAILED("Can't initialize storaged.");
          goto error;
      }

      // Check the requested cid/sid exist
      if (parameter.cid!=-1) {
	if (storaged_lookup(parameter.cid,parameter.sid)== NULL) {
          quiet = 0;
          REBUILD_FAILED("No such cid/sid %d/%d.",parameter.cid,parameter.sid);
          goto error;
	}
      }
    }  



    /*
    ** When cid/sid is given check the existence of the logical storage
    */
    if ((parameter.cid!=-1) && (parameter.sid!=-1)) {        
      if (rbs_sanity_cid_sid_check (parameter.cid, parameter.sid) < 0) {
        goto error;        
      }
    }
    
    /*
    ** Initialize the rebuild status file name
    */
    rbs_status_file_name();
    
    /*
    ** In case of resume retrieve the former rebuild status file name 
    */
    if (parameter.resume) {
      /*
      ** Read the saved rebuild file name
      */
      read_rebuild_status_file_name();
    }  

    /*
    ** Do not log in the case of FID rebuild
    */
    if (!nolog) {
      syslog(EINFO,"Rebuild %d monitoring file is %s\n", parameter.rebuildRef, rbs_monitor_file_path); 
    }
    
    if (!quiet) {

      printf("Check rebuild status : watch -d -n 20 cat %s\n", rbs_monitor_file_path); 
      printf("Abort this rebuild   : storage_rebuild -id %d -abort\n", parameter.rebuildRef); 
      printf("Pause this rebuild   : storage_rebuild -id %d -pause\n", parameter.rebuildRef); 
    }

    /*
    ** Daemonize
    */
    if (parameter.background) {
      if (daemon(0, 0)==-1) {
        severe("daemon %s",strerror(errno));        
      }
      quiet = 1;
    }
    
    /*
    ** Resume rebuild storage
    */   
    if (parameter.resume) {
      
      status = rbs_rebuild_resume();
    }
    /*
    ** Start a new rebuild
    */
    else {       
      
      status = rbs_rebuild_process();
    }
    
    /*
    ** Remove the rebuild marks from the devices
    */
    rbs_remove_rebuild_marks();

    on_stop(); 
     
    if (status == 0) {
      exit(EXIT_SUCCESS);
    }
    
    exit(EXIT_FAILURE);
    
error:
    /*
    ** Remove the rebuild marks from the devices
    */
    rbs_remove_rebuild_marks();

    REBUILD_MSG("Can't start storage_rebuild. See logs for more details.");
    exit(EXIT_FAILURE);
}
