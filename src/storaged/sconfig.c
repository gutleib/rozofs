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


#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <libconfig.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/log.h>
#include <rozofs/core/rozofs_host2ip.h>

#include "sconfig.h"







/* Settings names for storage configuration file */
#define SSTORAGES   "storages"
#define SSID	    "sid"
#define SCID	    "cid"
#define SROOT	    "root"
#define SIOLISTEN   "listen"
#define SIOADDR     "addr"
#define SIOPORT     "port"
#define SNODEID     "nodeid"

#define SSPARE_MARK     "spare-mark"
#define SDEV_TOTAL      "device-total"
#define SDEV_MAPPER     "device-mapper"
#define SDEV_RED        "device-redundancy"

int storage_config_initialize(storage_config_t *s, cid_t cid, sid_t sid,
        const char *root, int dev, int dev_mapper, int dev_red, const char * spare_mark) {
    DEBUG_FUNCTION;

    s->sid = sid;
    s->cid = cid;
    strncpy(s->root, root, PATH_MAX);
    s->device.total      = dev;
    s->device.mapper     = dev_mapper; 
    s->device.redundancy = dev_red;
    if (spare_mark == NULL) {
      /*
      ** Spare device have an empty "rozofs_spare" file
      */
      s->spare_mark = NULL;
    }
    else {
      /*
      ** Spare device have a "rozofs_spare" file with <spare_mark> string in it
      */      
      s->spare_mark = xstrdup(spare_mark);
    }
    list_init(&s->list);
    return 0;
}

void storage_config_release(storage_config_t *s) {
    if (s->spare_mark != NULL) {
      xfree(s->spare_mark);
      s->spare_mark = NULL;
    }  
    return;
}

int sconfig_initialize(sconfig_t *sc) {
    DEBUG_FUNCTION;
    memset(sc, 0, sizeof (sconfig_t));
    list_init(&sc->storages);
    return 0;
}

void sconfig_release(sconfig_t *config) {
    list_t *p, *q;
    DEBUG_FUNCTION;

    list_for_each_forward_safe(p, q, &config->storages) {
        storage_config_t *entry = list_entry(p, storage_config_t, list);
        storage_config_release(entry);
        list_remove(p);
        free(entry);
    }
}

int sconfig_read(sconfig_t *config, const char *fname, int cluster_id) {
    int status = -1;
    config_t cfg;
    struct config_setting_t *stor_settings = 0;
    struct config_setting_t *ioaddr_settings = 0;
    int i = 0;
//    const char              *char_value = NULL;    
#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR >= 4)) \
               || (LIBCONFIG_VER_MAJOR > 1))
    int port;
    int devices, mapper, redundancy, nodeid;
#else
    long int port;
    long int devices, mapper, redundancy, nodeid;
#endif      
    DEBUG_FUNCTION;

    config_init(&cfg);

    if (config_read_file(&cfg, fname) == CONFIG_FALSE) {
        errno = EIO;
        severe("can't read %s: %s (line %d).", fname, config_error_text(&cfg),
                config_error_line(&cfg));
        goto out;
    }
    
    /*
    ** Check for node identifier for NUMA
    */
    if (config_lookup_int(&cfg, SNODEID, &nodeid) == CONFIG_FALSE) {
        config->numa_node_id = -1;
    }    
    else {
        config->numa_node_id = nodeid;
    }    


    /*
    ** Check whether self-healing is configured 
    */
    config->export_hosts = NULL;

    if (!(ioaddr_settings = config_lookup(&cfg, SIOLISTEN))) {
        errno = ENOKEY;
        severe("can't fetch listen settings.");
        goto out;
    }

    config->io_addr_nb = config_setting_length(ioaddr_settings);

    if (config->io_addr_nb > STORAGE_NODE_PORTS_MAX) {
        errno = EINVAL;
        severe("too many IO listen addresses defined. %d while max is %d.",
                config->io_addr_nb, STORAGE_NODE_PORTS_MAX);
        goto out;
    }

    if (config->io_addr_nb == 0) {
        errno = EINVAL;
        severe("no IO listen address defined.");
        goto out;
    }

    for (i = 0; i < config->io_addr_nb; i++) {
        struct config_setting_t * io_addr = NULL;
        const char * io_addr_str = NULL;

        if (!(io_addr = config_setting_get_elem(ioaddr_settings, i))) {
            errno = ENOKEY;
            severe("can't fetch IO listen address(es) settings %d.", i);
            goto out;
        }

        if (config_setting_lookup_string(io_addr, SIOADDR, &io_addr_str)
                == CONFIG_FALSE) {
            errno = ENOKEY;
            severe("can't lookup address in IO listen address %d.", i);
            goto out;
        }

        // Check if the io address is specified by a single * character
        // if * is specified storio will listen on any of the interfaces
        if (strcmp(io_addr_str, "*") == 0) {
            config->io_addr[i].ipv4 = INADDR_ANY;
        } else {
            if (rozofs_host2ip((char*) io_addr_str, &config->io_addr[i].ipv4)
                    < 0) {
                severe("bad address \"%s\" in IO listen address %d",
                        io_addr_str, i);
                goto out;
            }
        }

        if (config_setting_lookup_int(io_addr, SIOPORT, &port)
                == CONFIG_FALSE) {
            errno = ENOKEY;
            severe("can't lookup port in IO address %d.", i);
            goto out;
        }

        config->io_addr[i].port = port;
    }

    if (!(stor_settings = config_lookup(&cfg, SSTORAGES))) {
        errno = ENOKEY;
        severe("can't fetch storages settings.");
        goto out;
    }

    for (i = 0; i < config_setting_length(stor_settings); i++) {
        storage_config_t *new = 0;
        struct config_setting_t *ms = 0;

        // Check version of libconfig
#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR >= 4)) \
               || (LIBCONFIG_VER_MAJOR > 1))
        int sid;
        int cid;
#else
        long int sid;
        long int cid;
#endif
        const char *root = 0;
        const char *spare_mark = NULL;
        
	char       rootPath[PATH_MAX];

        if (!(ms = config_setting_get_elem(stor_settings, i))) {
            errno = ENOKEY;
            severe("can't fetch storage.");
            goto out;
        }

        if (config_setting_lookup_int(ms, SSID, &sid) == CONFIG_FALSE) {
            errno = ENOKEY;
            severe("can't lookup sid for storage %d.", i);
            goto out;
        }

        if (config_setting_lookup_int(ms, SCID, &cid) == CONFIG_FALSE) {
            errno = ENOKEY;
            severe("can't lookup cid for storage %d.", i);
            goto out;
        }
	
        /*
	** Only keep the clusters we take care of
	*/
	if ((cluster_id!=0)&&(cluster_id!=cid)) continue;

	/*
	** Device configuration
	*/
	if (!config_setting_lookup_int(ms, SDEV_TOTAL, &devices)) {
            errno = ENOKEY;
            severe("can't fetch total device number.");
            goto out;
	}
	if (devices > STORAGE_MAX_DEVICE_NB) {
            errno = EINVAL;
            severe("Device number exceed %d for storage %d.", STORAGE_MAX_DEVICE_NB, i);
            goto out;
        }


	if (!config_setting_lookup_int(ms, SDEV_MAPPER, &mapper)) {
	  mapper = devices;
	}

	if (!config_setting_lookup_int(ms, SDEV_RED, &redundancy)) {
	  redundancy = 2;
	}

	
	/*
	** When automount is configured, do not get the root path from 
	** storage.conf but deduce it from the common_config,
	** and create the directories to mount devices on.
	*/
	if (common_config.device_automount) {
	
	  char * p = rootPath;
	  p += rozofs_string_append(p, common_config.device_automount_path);
	  *p++ = '/';
	  p += rozofs_string_append(p, "storage_");
	  p += rozofs_u32_append(p, cid);
	  p += rozofs_string_append(p, "_");	
	  p += rozofs_u32_append(p, sid);
	  
	  root = rootPath;
	  	  
	  /*
	  ** Create directory if it does not yet exist
	  */
	  if (access(root, F_OK) != 0) {	  
	    rozofs_mkpath ((char*)root,S_IRUSR | S_IWUSR | S_IXUSR);
	  }  

	  /*
	  ** Create every device directory
	  */
	  int idx;
	  for (idx=0; idx<devices; idx++) {
	    char devicePath[PATH_MAX];
	    p = devicePath;
	    p += rozofs_string_append(p,rootPath);
  	    *p++ = '/';
	    p += rozofs_u32_append(p,idx);
	    if (access(devicePath, F_OK) != 0) {
	      /*
	      ** Create directory
	      */
	      if (mkdir (devicePath,S_IRUSR | S_IWUSR | S_IXUSR) == -1) {
		severe("mkdir(%s) %s", devicePath, strerror(errno));
	      }	 
	      /*
	      ** Create X empty file while the device is not yet mounted
	      */
	      p += rozofs_string_append(p,"/X");
	      int fd = open(devicePath, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IXUSR);
	      if (fd <= 0) {
		severe("open(%s) %s", devicePath, strerror(errno));	      
	      }  
	      else {
	        close(fd);
	      }
	    }
	  }
	}
	else {

          if (config_setting_lookup_string(ms, SROOT, &root) == CONFIG_FALSE) {
              errno = ENOKEY;
              severe("can't lookup root path for storage %d.", i);
              goto out;
          }
        }
	
        // Check root path length
        if (strlen(root) > PATH_MAX) {
            errno = ENAMETOOLONG;
            severe("root path for storage %d must be lower than %d.", i,
                    PATH_MAX);
            goto out;
        }

        
        /*
        ** What string should be set in rozofs_spare mark files of spare disk for
        ** this volume
        */  
        spare_mark = NULL;
        if (config_setting_lookup_string(ms, SSPARE_MARK, &spare_mark) == CONFIG_FALSE) {
          spare_mark = NULL;
        }

        new = xmalloc(sizeof (storage_config_t));
        if (storage_config_initialize(new, (cid_t) cid, (sid_t) sid,
                root, devices, mapper, redundancy,spare_mark) != 0) {
            if (new)
                free(new);
            goto out;
        }
        list_push_back(&config->storages, &new->list);
    }

    status = 0;
out:
    config_destroy(&cfg);
    return status;
}
/*____________________________________________________
**
** Check listening IP addresses are configured 
** It loops for 10 minutes until the IP address is configured.
** After 10 minutes loop it failes
*/
int storaged_wait_ip_address_is_configured(uint32_t ipAddr) {
  int warning_count = 0;

  /*
  ** INADDR_ANY is a perfect address
  */
  if (ipAddr == INADDR_ANY) return 1;
  
  /*
  ** Wait until the IP address is configured
  */
  while (1) {
  
    if (is_this_ipV4_configured(ipAddr)) {
      if (warning_count != 0) {
        warning("%u.%u.%u.%u addresses is configured now",(ipAddr>>24)&0xFF,(ipAddr>>16)&0xFF,(ipAddr>>8)&0xFF,ipAddr&0xFF);
      }
      return 1;
    }

    /* 
    ** Raise a warning every minute 
    */  
    if (warning_count > 600) {
      severe("%u.%u.%u.%u addresses is NOT configured !!!",(ipAddr>>24)&0xFF,(ipAddr>>16)&0xFF,(ipAddr>>8)&0xFF,ipAddr&0xFF);
      return 0;
    }
    if ((warning_count%20)==0) {	
      warning("%u.%u.%u.%u addresses is not yet configured",(ipAddr>>24)&0xFF,(ipAddr>>16)&0xFF,(ipAddr>>8)&0xFF,ipAddr&0xFF);
    }	
    warning_count++;
    sleep(1);
  }
}
/*____________________________________________________
**
** Validate storag.conf configuration
**
** @param config The configuration read from the file in internal
**               RozoFS structure.
*/  
int sconfig_validate(sconfig_t *config) {
    int status = -1;
    int i = -1;
    int j = -1;
    list_t *p;
    int storages_nb = 0;
    uint32_t ip = 0;
    DEBUG_FUNCTION;

    // Check if IO addresses are duplicated
    for (i = 0; i < config->io_addr_nb; i++) {

        if ((config->io_addr[i].ipv4 == INADDR_ANY) &&
                (config->io_addr_nb != 1)) {
            severe("only one IO listen address can be configured if '*'"
                    " character is specified");
            errno = EINVAL;
            goto out;
        }

        /*
        ** Check that the given listening IP address is configured
        */
        if (!storaged_wait_ip_address_is_configured(config->io_addr[i].ipv4)) {
          errno = EADDRNOTAVAIL;
          goto out;
        }
        
        for (j = i + 1; j < config->io_addr_nb; j++) {

            if ((config->io_addr[i].ipv4 == config->io_addr[j].ipv4)
                    && (config->io_addr[i].port == config->io_addr[j].port)) {

                ip = config->io_addr[i].ipv4;
                severe("duplicated IO listen address (addr: %u.%u.%u.%u ;"
                        " port: %"PRIu32")",
                        ip >> 24, (ip >> 16)&0xFF, (ip >> 8)&0xFF, ip & 0xFF,
                        config->io_addr[i].port);
                errno = EINVAL;
                goto out;
            }
        }
    }

    list_for_each_forward(p, &config->storages) {
        list_t *q;
        storage_config_t *e1 = list_entry(p, storage_config_t, list);
        if (access(e1->root, F_OK) != 0) {
            severe("invalid root for storage (cid: %u ; sid: %u) %s: %s.",
                    e1->cid, e1->sid, e1->root, strerror(errno));
            errno = EINVAL;
            goto out;
        }
	
	if (e1->device.total < e1->device.mapper) {
            severe("device total is %d and mapper is %d", 
	           e1->device.total, e1->device.mapper);
            errno = EINVAL;
            goto out;
	}

	if (e1->device.redundancy > e1->device.mapper) {
            severe("device redundancy is %d and mapper is %d", 
	           e1->device.redundancy, e1->device.mapper);
            errno = EINVAL;
            goto out;
	}
	
        list_for_each_forward(q, &config->storages) {
            storage_config_t *e2 = list_entry(q, storage_config_t, list);
            if (e1 == e2)
                continue;
            if ((e1->sid == e2->sid) && (e1->cid == e2->cid)) {
                severe("duplicated couple (cid: %u ; sid: %u)", e1->cid,
                        e1->sid);
                errno = EINVAL;
                goto out;
            }

            if (strcmp(e1->root, e2->root) == 0) {
                severe("duplicated root: %s", e1->root);
                errno = EINVAL;
                goto out;
            }
        }

        // Compute the nb. of storage(s) for this storage node
        storages_nb++;
    }

    // Check the nb. of storage(s) for this storage node
    if (storages_nb > STORAGES_MAX_BY_STORAGE_NODE) {
        severe("too many number of storages for this storage node: %d"
                " storages register (maximum is %d)",
                storages_nb, STORAGES_MAX_BY_STORAGE_NODE);
        errno = EINVAL;
        goto out;
    }

    status = 0;
out:
    return status;
}
