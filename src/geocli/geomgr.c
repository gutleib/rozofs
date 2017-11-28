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

#include <inttypes.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/resource.h>
#include <getopt.h>
#include <stdio.h>
#include <signal.h>
#include <time.h>

#include <rozofs/rozofs.h>
#include <rozofs/core/ruc_common.h>
#include <rozofs/core/uma_tcp_main_api.h>
#include <rozofs/core/ruc_tcpServer_api.h>
#include <rozofs/core/rozofs_core_files.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/rozo_launcher.h>
#include <rozofs/common/log.h>
#include <rozofs/common/daemon.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/common_config.h>

#include "geomgr.h"
#include "geomgr_config.h"

#define GEOMGR_DEFAULT_TIMER    60

geomgr_input_param_t geomgr_input_param;

typedef enum _geomgr_export_status_e {
  geomgr_export_status_free,
  geomgr_export_status_off,
  geomgr_export_status_suspended,
  geomgr_export_status_running,   
} geomgr_export_status_e;

#define GEOMGR_MAX_EXPORTS 64
typedef struct _geomgr_export_t {
  int                             active;
  geomgr_export_status_e     status;
  char                            path[GEO_STRING_MAX];
  char                            host[GEO_STRING_MAX];
  int                             site;
  int                             instance;
  int                             valid;  
  geomgr_config_calendar_t * calendar;                          
} geomgr_export_t;

geomgr_export_t   exports[GEOMGR_MAX_EXPORTS];

/*__________________________________________________________________________
*/
char * geomgr_export_status2String (geomgr_export_status_e x) {
  switch(x) {
    case geomgr_export_status_free: return("free");
    case geomgr_export_status_off: return("off");
    case geomgr_export_status_suspended: return ("suspended");
    case geomgr_export_status_running: return ("running");
    default: return "unknown";
  }
}
/*__________________________________________________________________________
*/
void geomgr_unvalidate_all_exports() {
  int i;
  geomgr_export_t  * p = exports;
  
  for (i=0; i<GEOMGR_MAX_EXPORTS; i++,p++) {
    p->valid = 0;
  }
}
/*__________________________________________________________________________
*/
int geomgr_find_export_from_names(char * exportd, char * path, int site, int instance) {
  int i;
  geomgr_export_t  * p = exports;
  
  for (i=0; i<GEOMGR_MAX_EXPORTS; i++,p++) {
    if (p->status == geomgr_export_status_free) continue;
    if (p->site!=site)                               continue;
    if (p->instance!= instance)                      continue;
    if (strcmp(p->host,exportd)!=0)                  continue;
    if (strcmp(p->path,path)!=0)                     continue;
    p->valid = 1;
    return i;
  }
  return -1;
}
/*__________________________________________________________________________
*/
int geomgr_allocate_export(char * exportd, char * path, int site, int instance) {
  int i;
  geomgr_export_t  * p = exports;
  
  for (i=0; i<GEOMGR_MAX_EXPORTS; i++,p++) {
    if (p->status != geomgr_export_status_free) continue;
    p->status = geomgr_export_status_off;
    p->site = site;
    strcpy(p->host,exportd);
    strcpy(p->path,path);
    p->instance = instance;
    p->valid = 1;
    return i;
  }
  return -1;
}
/*__________________________________________________________________________
*/
void geomgr_free_export(int i) {
  if (i<0) return;
  if (i>GEOMGR_MAX_EXPORTS) return;
  exports[i].status = geomgr_export_status_free;
}
/*__________________________________________________________________________
*/
static void usage() {
    printf("Rozofs geomgr - %s\n", VERSION);
    printf("Usage: geomgr [OPTIONS]\n\n");
    printf("\t-h, --help\t\tprint this message.\n");
    printf("\t-D, diag_port=N\t\tdefine the rozodiag port (default is 54000).\n");
    printf("\t-c, --config\t\tconfiguration file to use (default: %s).\n",GEOMGR_DEFAULT_CONFIG);
    printf("\t-t, --timer\t\ttimer in second to reread configuration file (default: %d).\n",GEOMGR_DEFAULT_TIMER);

}
/*__________________________________________________________________________
*/
char geomgr_calString[64];
char * geomgr_calendar2string(geomgr_config_calendar_t * calendar) {
  int i;
  char * pt = geomgr_calString;
  int    val;

  /*
  ** No calendar condition. Start it !
  */
  if (calendar == NULL) {
    pt += sprintf(pt,"--");
    return geomgr_calString;
  }
  
  /*
  ** Some calendar condition. Check whether it should run.
  */
  for (i=0; i<calendar->nb_entries; i++) {
  
    val = calendar->entries[i].start;
    if (val>=(60*24)) val -= (60*24);
    pt += sprintf(pt,"%2.2dH%2.2d/",val/60,val%60);
    
    val = calendar->entries[i].stop;
    if (val>=(60*24)) val -= (60*24);
    pt += sprintf(pt,"%2.2dH%2.2d ",val/60,val%60);
  }
  
  /*
  ** Some calendar condition. Check whether it should run.
  */
  return geomgr_calString;

}
/*__________________________________________________________________________
*/
int geomgr_when_should_client_run(geomgr_config_calendar_t * calendar, int now) {
  int i;
  int start;
  int next=-1;

  /*
  ** No calendar condition. Start it !
  */
  if (calendar == NULL) return -1;

  /*
  ** Some calendar condition. Check whether it should run.
  */
  for (i=0; i<calendar->nb_entries; i++) {
  
    start = calendar->entries[i].start;
    if (start>=(60*24)) start -= (60*24);

    if (now < start) {
      if (next == -1)        next = start;
      else if (next > start) next = start;
    } 
  }
  if (next != -1) return next;
  
  /*
  ** Some calendar condition. Check whether it should run.
  */
  for (i=0; i<calendar->nb_entries; i++) {
  
    start = calendar->entries[i].start;
    if (start>=(60*24)) start -= (60*24);

    if (next == -1)        next = start;
    else if (next > start) next = start;
  }  
  return next;	
}
/*__________________________________________________________________________
*/
char GEOMGR_SEPARATOR[256];
char GEOMGR_FORMAT[256];
char GEOMGR_HEADER[256];
void geomgr_format_prepare(int host_len, int path_len) { 
  int i;
  char * pSep = GEOMGR_SEPARATOR;
  char * pForm = GEOMGR_FORMAT;
  char * pHead = GEOMGR_HEADER;

  pForm += sprintf(pForm,"| #| %%-7s |site| %%-%ds | %%-%ds | %%s\n", 
                   host_len,path_len);  

  pHead += sprintf(pHead,GEOMGR_FORMAT, "status","export","path","calendar");
  
  pForm = GEOMGR_FORMAT;
  pForm += sprintf(pForm,"|%%2d| %%-7s | %%2d | %%-%ds | %%-%ds | %%s\n",
                   host_len,path_len);  

  pSep += sprintf(pSep,"+--+---------+----+");
  host_len += 2;
  for(i=0; i< host_len; i++) *pSep++ = '-';    
  *pSep++ = '+';
  path_len += 2;
  for(i=0; i< path_len; i++) *pSep++ = '-';    
  sprintf(pSep,"+--- - -  -   -\n");
  
} 
void show_clients(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
  geomgr_export_t  * pe = exports;
  int idx;
  char * pStatus;
  char next_time_string[32];
  int next_time;

  time_t t = time(NULL);
  struct tm tm,*ptm;
  ptm = localtime_r(&t,&tm);  
  int now = ptm->tm_hour*60+ ptm->tm_min;
  int host_len=0;
  int path_len=0;
  int len;

  for (idx=0; idx<GEOMGR_MAX_EXPORTS; idx++,pe++) {
  
    if (pe->status == geomgr_export_status_free) continue;
  
    len = strlen(pe->host);
    if (len > host_len) host_len = len;
    len = strlen(pe->path);
    if (len > path_len) path_len = len;
  }
  
  geomgr_format_prepare(host_len,path_len);

  pChar += sprintf(pChar,"%s",GEOMGR_SEPARATOR);
  pChar += sprintf(pChar,"%s",GEOMGR_HEADER);
  pChar += sprintf(pChar,"%s",GEOMGR_SEPARATOR);
  
  pe = exports;
  for (idx=0; idx<GEOMGR_MAX_EXPORTS; idx++,pe++) {
  
    if (pe->status == geomgr_export_status_free) continue;
  
    pStatus = geomgr_export_status2String(pe->status); 
    
    /*
    ** When suspended, display next time it will run
    */
    if (pe->status == geomgr_export_status_suspended) {
      next_time = geomgr_when_should_client_run(pe->calendar,now);
      if (next_time != -1) {
        sprintf(next_time_string,"%2.2d:%2.2d", next_time/60, next_time%60);
	pStatus = next_time_string;
      } 
    }
    
    pChar += sprintf(pChar,GEOMGR_FORMAT, 
                     idx,pStatus, pe->site, pe->host, pe->path, geomgr_calendar2string(pe->calendar));
  }
  pChar += sprintf(pChar,"%s",GEOMGR_SEPARATOR);
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}

/*__________________________________________________________________________
*/
#define DISPLAY_UINT32_CONFIG(field)   pChar += sprintf(pChar,"%-25s = %u\n",#field, geomgr_input_param.field); 
#define DISPLAY_INT32_CONFIG(field)   pChar += sprintf(pChar,"%-25s = %d\n",#field, geomgr_input_param.field); 
#define DISPLAY_STRING_CONFIG(field) \
  if (geomgr_input_param.field == NULL) pChar += sprintf(pChar,"%-25s = NULL\n",#field);\
  else                                pChar += sprintf(pChar,"%-25s = %s\n",#field,geomgr_input_param.field); 
  
void show_start_config(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();

  DISPLAY_UINT32_CONFIG(dbg_port);
  DISPLAY_STRING_CONFIG(cfg);  
  DISPLAY_UINT32_CONFIG(timer);
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}  


/*__________________________________________________________________________
*/
void geomgr_start_client(int idx) {
  geomgr_export_t  * pe = &exports[idx];
  char cmd[256];
  char pid_file[64];
  int ret;
  
  /*
  ** prepare command line
  */
  sprintf(cmd,"geocli -M geocli_%d_ -H %s -E %s -G %d -i %d",
         idx, pe->host, pe->path, pe->site, idx);
  sprintf(pid_file,"/var/run/launcher_geocli_%d.pid",idx);
  
  ret = rozo_launcher_start(pid_file,cmd);
  if (ret < 0) {
    severe("rozo_launcher_start(%s,%s) %s",pid_file, cmd, strerror(errno));
    return;
  }
  pe->status = geomgr_export_status_running;
}
/*__________________________________________________________________________
*/
void geomgr_stop_client(int idx) {
  geomgr_export_t  * pe = &exports[idx];
  char pid_file[64];
  int ret;
  
  sprintf(pid_file,"/var/run/launcher_geocli_%d.pid",idx);  
  ret = rozo_launcher_stop(pid_file); 
  if (ret <0) {
    severe("rozo_launcher_stop(%s) %s",pid_file, strerror(errno));
  }
  pe->status = geomgr_export_status_suspended;
}
/*__________________________________________________________________________
*/
int geomgr_should_client_run(geomgr_config_calendar_t * calendar, int now) {
  int i;
  int start, stop;
  /*
  ** No calendar condition. Start it !
  */
  if (calendar == NULL) return 1;


  /*
  ** Some calendar condition. Check whether it should run.
  */
  for (i=0; i<calendar->nb_entries; i++) {
  
    start = calendar->entries[i].start;
    if (start>=(60*24)) start -= (60*24);
    stop = calendar->entries[i].stop;

    if (start < stop) {
      if ((start <= now) && (now < stop)) {
	return 1;
      }
    }
    else {
      if (now >= start) return 1;
      if (now < stop)   return 1;
    }  
  }
  return 0;	
}

/*
**____________________________________________________
*/
/*
  Periodic timer expiration
*/
static void geomgr_ticker(void * param) {
  geomgr_config_t * config;
  list_t *p, *q;
  int idx;
  int                    index;
  geomgr_export_t  * pe = exports;
  int now;
  geomgr_config_calendar_t * calendar;
  int should_run;
  int instance;
   
  config = geomgr_config_read(geomgr_input_param.cfg);
  if (config == NULL) return;

  geomgr_unvalidate_all_exports();
  
  for (idx=0; idx < config->nb_exportd; idx++) {
  
    /*
    ** Loop on configured geo clients 
    */
    list_for_each_forward_safe(p, q, &config->exportd[idx].export) {
      geomgr_config_export_t *entry = list_entry(p, geomgr_config_export_t, list);
      
      /*
      ** Lookup for this client in our data base
      */
      for (instance=0; instance < entry->nb; instance++) {
	index = geomgr_find_export_from_names(config->exportd[idx].host,entry->path,entry->site,instance);
	if (index == -1) {
          /*
	  ** Allocate a new client context
	  */
          index = geomgr_allocate_export(config->exportd[idx].host,entry->path,entry->site, instance);
	  /* Out of memory */
	  if (index == -1) continue;
	} 


	/*
	** Update calendar
	*/

	pe = &exports[index];
	/* No calendar configured */
	if (entry->calendar == NULL) {
          if (pe->calendar != NULL) {
            free(pe->calendar);
	    pe->calendar = NULL;
	  }  	
	} 
	/* Calendar is configured */
	else {
	  /* Calendar differs from config ? */
	  if ( (pe->calendar == NULL)
	  ||   (entry->calendar->nb_entries != pe->calendar->nb_entries)
	  ||   (memcmp(entry->calendar->entries,pe->calendar->entries,entry->calendar->nb_entries*sizeof(geomgr_config_calendar_entry_t))!=0)) {
	    if (pe->calendar != NULL) {
              free(pe->calendar);
	      pe->calendar = NULL;
	    }  
	    int size = sizeof(geomgr_config_calendar_t)+((entry->calendar->nb_entries-1)*sizeof(geomgr_config_calendar_entry_t));
	    pe->calendar = xmalloc(size);
	    memcpy(pe->calendar,entry->calendar,size);
	  }  
        }


	pe->active = entry->active;    
      }              
    }
  }
  

  time_t t = time(NULL);
  struct tm tm,*ptm;
  ptm = localtime_r(&t,&tm);  
  now = ptm->tm_hour*60+ ptm->tm_min;
  
  
  pe = exports;
  for (idx=0; idx<GEOMGR_MAX_EXPORTS; idx++,pe++) {
    
    if (pe->status == geomgr_export_status_free) continue;
  
    /*
    ** This client has disappeared
    */
    if (pe->valid == 0) {
      /*
      ** Stop it
      */
      geomgr_stop_client(idx);
      /*
      ** Free it 
      */
      geomgr_free_export(idx);
      continue;
    }
    
    /*
    ** This client is off
    */
    if (pe->active == 0) {
      if (pe->status == geomgr_export_status_running) {
        geomgr_stop_client(idx);
      }
      pe->status = geomgr_export_status_off;
      continue;      
    }
    
    /*
    ** This client was off
    */
    if (pe->status == geomgr_export_status_off) {
      pe->status = geomgr_export_status_suspended;
    }
    
    calendar = pe->calendar;
    should_run = geomgr_should_client_run(calendar,now);
    
    /*
    ** This client is not running. Shouldn't it ?
    */
    if (pe->status == geomgr_export_status_suspended) {
    
      if (should_run) {
	geomgr_start_client(idx);
      }
      continue;      
    }  
    
    /*
    ** This client is running. Should it be stopped ?
    */
    else if (pe->status == geomgr_export_status_running) {
    
      if (!should_run) {
	geomgr_stop_client(idx);
      }
      continue;      
    }
  }  
  
  
  geomgr_config_release(config);
  return;
}
/*
**____________________________________________________
*/
/**
*  start the periodic timer associated with the geo-replication

   @param period : frequency in ms
   
   @retval 0 on success
   @retval -1 on error (see errno for details)
*/

int geomgr_timer(int period)
{

  struct timer_cell * geo_cli_periodic_timer;

  geo_cli_periodic_timer = ruc_timer_alloc(0,0);
  if (geo_cli_periodic_timer == NULL) {
    severe("cannot allocate timer cell");
    errno = EPROTO;
    return -1;
  }
  ruc_periodic_timer_start (geo_cli_periodic_timer, 
                            period,
 	                    geomgr_ticker,
 			    NULL);
  return 0;

}
/*
 *_______________________________________________________________________
 */
uint32_t ruc_init(uint32_t test, uint16_t debug_port) {
    int ret;


    uint32_t mx_tcp_client = 10;
    uint32_t mx_tcp_server = 10;
    uint32_t mx_tcp_server_cnx = 10;
    uint32_t mx_af_unix_ctx = ROZO_AFUNIX_CTX_GEOMGR;

    /*
     ** trace buffer initialization
     */
    ruc_traceBufInit();

    /*
     ** initialize the socket controller:
     **   4 connections per Relc and 32
     **   for: NPS, Timer, Debug, etc...
     */
    ret = ruc_sockctl_init(ROZO_SOCKCTRL_CTX_GEOMGR);
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
        if (ret != RUC_OK) break;

        /*
         **--------------------------------------
         **  configure the number of TCP server
         **  context supported
         **--------------------------------------   
         **  
         */
        ret = ruc_tcp_server_init(mx_tcp_server);
        if (ret != RUC_OK) break;

        /*
         **--------------------------------------
         **  configure the number of TCP client
         **  context supported
         **--------------------------------------   
         **  
         */
        //     ret = ruc_tcp_clientinit(mx_tcp_client);
        //     if (ret != RUC_OK) break;   


        /*
         **--------------------------------------
         **  configure the number of AF_UNIX
         **  context supported
         **--------------------------------------   
         **  
         */
        ret = af_unix_module_init(mx_af_unix_ctx,
                16, 1024, // xmit(count,size)
                16, 1024 // recv(count,size)
                );
        if (ret != RUC_OK) break;
	   
        break;

    }



    /*
     ** internal debug init
     */
    //ruc_debug_init();


    /*
     **--------------------------------------
     **   D E B U G   M O D U L E
     **--------------------------------------
     */
    uma_dbg_init(10, INADDR_ANY, debug_port);

    return ret;
}
/*
 *_______________________________________________________________________
 */
static void on_start() {
  int ret;

  /*
  ** Create a session id under which every sub process will run
  */
  setsid();

  // Change AF_UNIX datagram socket length
  af_unix_socket_set_datagram_socket_len(128);

  uma_dbg_record_syslog_name("geomgr");

  /*
   ** Register these topics before start the rozofs_stat_start that will
   ** register other topic. Topic registration is not safe in multi-thread
   ** case
   */
  uma_dbg_addTopic("start_config", show_start_config);
  uma_dbg_addTopic("clients", show_clients);

  ret = ruc_init(FALSE, geomgr_input_param.dbg_port);
  if (ret != RUC_OK) fatal("ruc_init");;


  {
      char name[32];
      sprintf(name, "geomgr");
      uma_dbg_set_name(name);
  }


  ret = geomgr_timer(geomgr_input_param.timer*1000);
  if (ret < 0) fatal("geomgr_timer");

  /*
   ** main loop
   */
  while (1) {
      ruc_sockCtrl_selectWait();
  }
  fatal( "main() code is rotten" );
}
/*
 *_______________________________________________________________________
 */
static void on_stop() {
}
/*
 *_______________________________________________________________________
 */
int main(int argc, char *argv[]) {
  int c;
  int val;
  struct rlimit core_limit;
  static struct option long_options[] = {
      { "help", no_argument, 0, 'h'},
      { "dbg", required_argument, 0, 'D'},
      {"config", required_argument, 0, 'c'},
      {"timer", required_argument, 0, 't'},      
      { 0, 0, 0, 0}
  };

  /*
  ** Change local directory to "/"
  */
  if (chdir("/")!= 0) {}

  //memset(&geomgr_conf, 0, sizeof (geomgr_conf));

  geomgr_input_param.dbg_port = rozofs_get_service_port_geomgr_diag();   
  geomgr_input_param.cfg      = GEOMGR_DEFAULT_CONFIG;
  geomgr_input_param.timer    = 60;


  while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "hD:c:t:", long_options, &option_index);

      if (c == -1)
          break;

      switch (c) {
          case 'h':
              usage();
              exit(EXIT_SUCCESS);

          case 'D':
              errno = 0;
              val = (int) strtol(optarg, (char **) NULL, 10);
              if (errno != 0) {
                  strerror(errno);
                  usage();
                  exit(EXIT_FAILURE);
              }
              geomgr_input_param.dbg_port = val;
              break;
	      
	  case 't':    		
              errno = 0;
              val = (int) strtol(optarg, (char **) NULL, 10);
              if (errno != 0) {
                  strerror(errno);
                  usage();
                  exit(EXIT_FAILURE);
              }
	      if (val<=0) {
	        printf("Bad timer value %s", optarg);
		exit(EXIT_FAILURE); 
	      }
              geomgr_input_param.timer = val;
              break;     
	           
	  case 'c':
              geomgr_input_param.cfg = optarg;
              break;
	      
          case '?':
              usage();
              exit(EXIT_SUCCESS);
              break;
	      
          default:
	      printf("unknown option %c\n",c);
              usage();
              exit(EXIT_FAILURE);
              break;
      }
  }       

  /*
  ** read common config file
  */
  common_config_read(NULL);    
  

  // Change the value of maximum size of core file
  core_limit.rlim_cur = RLIM_INFINITY;
  core_limit.rlim_max = RLIM_INFINITY;
  if (setrlimit(RLIMIT_CORE, &core_limit) < 0) {
      warning("Failed to change maximum size of core file: %s",
              strerror(errno));
  }

  no_daemon_start("geomgr", common_config.nb_core_file, "geomgr.pid", 
                on_start, on_stop, NULL);
  exit(0);
}

