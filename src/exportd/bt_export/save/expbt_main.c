
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

#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <errno.h>
#include <sched.h>
#include <getopt.h>
#include <libconfig.h>
#include <limits.h>
#include <inttypes.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/profile.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/rozofs_rpc_non_blocking_generic.h>
#include <rozofs/core/rozofs_numa.h>
#include <rozofs/core/rozofs_core_files.h>
#include "config.h"
#include "../econfig.h"
#include "expbt_global.h"
#include "expbt_north_intf.h"
#include <rozofs/rpc/expbt_protocol.h>
#include "expbt_trk_thread_intf.h"
#include "expbt_nblock_init.h"
#include <rozofs/common/expbt_inode_file_tracking.h>

#define EXPBT_MAX_EXPORTS 1024

typedef struct _expbt_export_eid_path_t
{
   int eid;    /**< export identifier */
   char *path; /**< path for the export */
} expbt_export_eid_path_t;


static char exportd_config_file[PATH_MAX] = EXPORTD_DEFAULT_CONFIG;
int expbt_curtb_idx = 0;
expbt_export_eid_path_t *expbt_export_eid_path_tb[2];
expbt_export_eid_path_t *expbt_export_eid_path_cur_tb;

int                      rozofs_no_site_file = 0;
uint32_t export_configuration_file_hash = 0;  /**< hash value of the configuration file */
expbt_start_conf_param_t  expbt_conf_param;



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
**____________________________________________________________________________
*/

/**
*  Get the pathname of the export according to the export identifier

   @param eid: export identifier
   
   @retval <> NULL: pointer to the export pathname
   @retval NULL (no path, see errno for details )
*/

char *expbt_get_export_path(uint32_t eid)
{
  if (expbt_export_eid_path_cur_tb == NULL)
  {
    errno = ENOENT;
    return NULL;
  }
  if (eid >= EXPBT_MAX_EXPORTS) 
  {
    errno = ERANGE;
    return NULL;
  }
  if (expbt_export_eid_path_cur_tb[eid].path == NULL)
  {
    errno = ENOENT;
    return NULL;
  }
  return expbt_export_eid_path_cur_tb[eid].path;


}

/*
**____________________________________________________________________________
*/

int expbt_build_export_table(expbt_export_eid_path_t *table_p)
{
  list_t          * e=NULL;
  export_config_t * econfig;
  int index=0;
  int i;
  int status;
  
  /*
  ** clean up the table before
  */
  for (i = 0; i < EXPBT_MAX_EXPORTS;i++)
  {
     table_p[i].eid = 0;
     if (table_p[i].path!= NULL) 
     {
       free(table_p[i].path);
       table_p[i].path = NULL;
     }
  }
  list_for_each_forward(e, &exportd_config.exports) {

    econfig = list_entry(e, export_config_t, list);
    table_p[econfig->eid].path = strdup(econfig->root);
     table_p[econfig->eid].eid = econfig->eid;
     /*
     ** Init of the shared memory used for tracking changes for directories and files
     */
     info("tracking file inode shared memory init (eid:%d)",econfig->eid);
     status = expb_open_shared_memory(econfig->eid,ROZOFS_REG,0);
     if (status < 0)
     {
       severe("%s shared memory for Regular file failure (eid:%d):%s","Opening of",econfig->eid,strerror(errno));
     }
     info("tracking directory inode shared memory init (eid:%d)",econfig->eid);
     status = expb_open_shared_memory(econfig->eid,ROZOFS_DIR,0);
     if (status < 0)
     {
       severe("%s shared memory for Directory failure (eid:%d):%s","Opening of",econfig->eid,strerror(errno));
     } 

     index++;
  }
  return index;   
}

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
static void usage() {
    printf("Rozofs export tracking file reader daemon - %s\n", VERSION);
    printf("Usage: rozo_expbtd [OPTIONS]\n\n");
    printf("\t-h, --help\tprint this message.\n");
    printf("\t-d,--debug <port>\t\rozo_expbtd blocking debug port(default: none) \n");
    printf("\t-p,--port <value>\t\t service port \n");
    printf("\t-i,--instance <value>\t\texportd instance id(default: 1) \n");
    printf("\t-c, --config\tconfiguration file to use (default: %s).\n",EXPORTD_DEFAULT_CONFIG);
};
/*
 *_______________________________________________________________________
 */
int main(int argc, char *argv[]) {
    int c;
    int val;
    int i;

    static struct option long_options[] = {
        {"help", no_argument, 0, 'h'},
        {"debug", required_argument, 0, 'd'},
        {"instance", required_argument, 0, 'i'},
        {"config", required_argument, 0, 'c'},
        {"port", required_argument, 0, 'p'},
        {0, 0, 0, 0}
    };
    
//    ALLOC_PROFILING(epp_profiler_t);

    for (i = 0; i < 2; i++)
    {
       expbt_export_eid_path_tb[i] = malloc(sizeof(expbt_export_eid_path_t)*EXPBT_MAX_EXPORTS);
      memset(expbt_export_eid_path_tb[i],0,sizeof(expbt_export_eid_path_t)*EXPBT_MAX_EXPORTS);       
    
    }
    expbt_curtb_idx = 0;
    expbt_export_eid_path_cur_tb= expbt_export_eid_path_tb[expbt_curtb_idx];

    
    /*
    ** Change local directory to "/"
    */
    if (chdir("/")!=0) {}
    
    rozofs_mkpath(ROZOFS_RUNDIR_PID,0755);

    /* Try to get debug port from /etc/services */
    expbt_conf_param.debug_port = 0;
    expbt_conf_param.instance = 0;
    expbt_conf_param.io_port  = 0;

    while (1) {

        int option_index = 0;
        c = getopt_long(argc, argv, "shc:i:d:p:", long_options, &option_index);

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
                            "rozo_expbtd failed: configuration file: %s: %s\n",
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
                expbt_conf_param.debug_port = val;
                break;
             case 'p':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                expbt_conf_param.io_port = val;
		break;
             case 'i':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                expbt_conf_param.instance = val;
		if (val > 1) 
		{ 
		  printf("instance is out of range (max: 2)\n");
                  usage();
                  exit(EXIT_FAILURE);		  
		}
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
    
    rozofs_get_service_port_expbt_diag(expbt_conf_param.instance);
    expbt_conf_param.debug_port = rozofs_get_service_port_expbt_diag(expbt_conf_param.instance);
    expbt_conf_param.io_port = rozofs_get_service_port_expbt(expbt_conf_param.instance);
    /*
    ** Do not log each remote end disconnection
    */
    af_unix_socket_no_disconnect_log();
       
    /*
    ** read common config file
    */
    common_config_read(NULL);         
        

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
    expbt_build_export_table(expbt_export_eid_path_tb[(expbt_curtb_idx+1)&0x1]);
    expbt_curtb_idx++;
    expbt_export_eid_path_cur_tb = expbt_export_eid_path_tb[(expbt_curtb_idx)&0x1];
    
    
    /*
    ** Is a numa node given in the configuration
    */
    if (exportd_config.nodeid != -1) {
        /*
        ** Use the node id of the export.conf 
        */
        rozofs_numa_allocate_node(exportd_config.nodeid ,"export.conf");
    }
    
    
    /*
    ** compute the hash of the exportd configuration
    */
    
    if (hash_file_compute(exportd_config_file,&export_configuration_file_hash) != 0) {
        fprintf(stderr, "error while computing hash value of the configuration file: %s.\n",
                strerror(errno));
        goto error;
    }

    {
      char name[1024];

      sprintf(name,"expbt_%d",expbt_conf_param.instance);
      uma_dbg_record_syslog_name(name);
      /*
      ** Set a signal handler
      */
      rozofs_signals_declare(name, common_config.nb_core_file); 

    }
    /*
    ** Jump in the main loop
    */
    expbt_start_nb_th(&expbt_conf_param);
    
    exit(0);
error:
    fprintf(stderr, "see log for more details.\n");
    exit(EXIT_FAILURE);
}

