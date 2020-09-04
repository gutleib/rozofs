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
#include <rozofs/common/profile.h>
#include <rozofs/common/common_config.h>
#include <rozofs/rpc/mproto.h>
#include <rozofs/rpc/sproto.h>
#include <rozofs/rpc/spproto.h>
#include <rozofs/core/rozofs_core_files.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/uma_dbg_api.h>


#include "config.h"
#include "sconfig.h"
#include "storage.h"
#include "storaged.h"
#include "sconfig.h"
#include "storaged_nblock_init.h"

int rozofs_check_old_lsblk(void);
void storage_run_lsblk();

static char storaged_config_file[PATH_MAX] = STORAGED_DEFAULT_CONFIG;

sconfig_t storaged_config;

storage_t storaged_storages[STORAGES_MAX_BY_STORAGE_NODE] = { { 0 } };
uint16_t  storaged_nrstorages = 0;

uint8_t storaged_nb_ports = 0;
uint8_t storaged_nb_io_processes = 0;
int     expected_devices = 0;

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
		sc->device.redundancy) != 0) {
            severe("can't initialize storage (cid:%d : sid:%d) with path %s",
                    sc->cid, sc->sid, sc->root);
            goto out;
        }
        expected_devices += sc->device.total;
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
  fatal("storio_device_mapping_allocate_device");
  return -1;
}
uint32_t storio_device_mapping_new_chunk(uint16_t                  chunk,
                                         storio_device_mapping_t * fidCtx,
                                         storage_t               * st, 
                                         uint8_t                   layout, 
                                         sid_t                   * distrib) {
  fatal("storio_device_mapping_new_chunk");
  return -1;
}   


/*
**____________________________________________________
*/
/*
  Mount devices if automount is configured
*/
static void mount_devices() {
    int      count;

    /*
    ** Enumerate the available RozoFS devices.
    ** Storaged will umount every device, while storio will let
    ** the correctly mounted devices in place.
    */     
    storage_enumerate_devices("/tmp/storage_utility", 0) ;

    /*
    ** Mount the devices that have to
    */
    count = storage_mount_all_enumerated_devices();  

    /*
    ** Unmount the working directory 
    */
    storage_umount("/tmp/storage_utility");     

    if (count < expected_devices) {
      severe("%d expected devices and only %d found !!!", expected_devices, count);
      exit(EXIT_FAILURE);
    }   
    else if (count > expected_devices) {
      severe("More device mounted (%d) than expected (%d) !!!", count, expected_devices);
    }
    else {
      info("%d mounted devices", count);    
    } 
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
    printf("%s\n",error_buffer);
  }
  
  /*
  ** Display usage
  */
  printf("RozoFS storage mount - %s\n", VERSION);
  printf("  This utility :\n");
  printf("  - checks the storage configuration file syntax,\n");
  printf("  - creates the required mount directories (with X) for every device,\n");
  printf("  - mounts every available devices at its place,\n");
  printf("  - initializes the device subdirectories.\n");
  printf("  All these actions would else be done by the storaged at startup.\n");
  printf("\nUsage: storage_utility [OPTIONS]\n");
  printf("   -h, --help\t\t\tprint this message.\n");
  printf("   -c, --config=config-file\tspecify config file to use (default: %s).\n",
          STORAGED_DEFAULT_CONFIG);  
  
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
    static struct option long_options[] = {
        { "help", no_argument, 0, 'h'},
        { "config", required_argument, 0, 'c'},
        { 0, 0, 0, 0}
    };

    /*
    ** Change local directory to "/"
    */
    if (chdir("/")!= 0) {}

    while (1) {

        int option_index = 0;
        c = getopt_long(argc, argv, "hc:", long_options, &option_index);

        if (c == -1)
            break;

        switch (c) {

            case 'h':
                usage(NULL);
                break;
            case 'c':
                if (!realpath(optarg, storaged_config_file)) {
                    severe("storaged failed: %s %s", optarg, strerror(errno));
                    exit(EXIT_FAILURE);
                }
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
        severe("Failed to parse storage configuration file %s (%s).",storaged_config_file,strerror(errno));
        exit(EXIT_FAILURE);
    }
    // Check the configuration
    if (sconfig_validate(&storaged_config) != 0) {
        severe("Inconsistent storage configuration file: %s.",strerror(errno));
        exit(EXIT_FAILURE);
    }
    info("Configuration file %s is correct.", storaged_config_file);

    if (!common_config.device_automount) {
      info("Automount option is not set");
      exit(EXIT_SUCCESS);      
    }  

    // Initialization of the storage configuration
    if (storaged_initialize() != 0) {
        severe("can't initialize storaged: %s.", strerror(errno));
        exit(EXIT_FAILURE);
    }
    
    /*
    ** Check for lsblk version 
    */
    rozofs_check_old_lsblk(); 

    /*
    ** 1rst lsblk run
    */
    storage_run_lsblk(); 
           
    mount_devices();

    exit(EXIT_SUCCESS);
}
