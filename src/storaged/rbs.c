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
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <limits.h>
#include <dirent.h> 
#include <sys/wait.h>

#include <rozofs/common/list.h>
#include <rozofs/common/htable.h>
#include <rozofs/rozofs.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/rpc/rpcclt.h>
#include <rozofs/rpc/mclient.h>
#include <rozofs/common/profile.h>
#include <rozofs/rpc/spproto.h>
#include <rozofs/rpc/eproto.h>
#include <rozofs/core/rozofs_host2ip.h>
#include <rozofs/common/common_config.h>

#include "sconfig.h"
#include "storage.h"
#include "rbs_sclient.h"
#include "rbs_eclient.h"
#include "rbs.h"
#include "storage.h"

DECLARE_PROFILING(spp_profiler_t);




/** Get name of temporary rebuild directory
 *
 */
char rebuild_directory_name[FILENAME_MAX];
char * get_rebuild_directory_name(int rebuildRef) {
  char * pChar = rebuild_directory_name;
  pChar += rozofs_string_append(pChar,common_config.storage_temporary_dir);
  pChar += rozofs_string_append(pChar,"/rbs.");
  pChar += rozofs_u32_append(pChar,rebuildRef);  
  return rebuild_directory_name;
}
char rebuild_sid_directory_name[FILENAME_MAX];
char * get_rebuild_sid_directory_name(int rebuildRef, int cid, int sid, rbs_file_type_e ftype) {
  char * pChar = rebuild_sid_directory_name;
  pChar += rozofs_string_append(pChar,common_config.storage_temporary_dir);
  pChar += rozofs_string_append(pChar,"/rbs.");
  pChar += rozofs_u32_append(pChar,rebuildRef);  
  pChar += rozofs_string_append(pChar,"/cid");
  pChar += rozofs_u32_append(pChar,cid);  
  pChar += rozofs_string_append(pChar,"_sid");
  pChar += rozofs_u32_append(pChar,sid); 
  pChar += rozofs_string_append(pChar,"_");  
  pChar += rozofs_u32_append(pChar,ftype);   
  return rebuild_sid_directory_name;
}

/** Initialize a storage to rebuild
 *
 * @param cid: unique id of cluster that owns this storage.
 * @param sid: the unique id for the storage to rebuild.
 * @param storage_root: the absolute path where rebuild bins file(s) 
 * will be store.
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */

/** Initialize connections (via mproto and sproto) to one storage
 *
 * @param rb_stor: storage to connect.
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int rbs_stor_cnt_initialize(rb_stor_t * rb_stor, int cid) {
    int status = -1;
    int i = 0;
    mp_io_address_t io_address[STORAGE_NODE_PORTS_MAX];
    int    storio_id;

    // Copy hostname for this storage
    mclient_new(&rb_stor->mclient, rb_stor->host, 0, 0);
    memset(io_address, 0, STORAGE_NODE_PORTS_MAX * sizeof (mp_io_address_t));
    rb_stor->sclients_nb = 0;
 
    struct timeval timeo;
    timeo.tv_sec = RBS_TIMEOUT_MPROTO_REQUESTS;
    timeo.tv_usec = 0;

    // Initialize connection with this storage (by mproto)
    if (mclient_connect(&rb_stor->mclient, timeo) != 0) {
//        severe("failed to join storage (host: %s), %s.",
//                rb_stor->host, strerror(errno));
        goto out;
    } else {
        // Send request to get TCP ports for this storage
        if (mclient_ports_and_storio_nb(&rb_stor->mclient, io_address, &rb_stor->storio_nb) != 0) {
            severe("Warning: failed to get ports for storage (host: %s)."
                    , rb_stor->host);
            goto out;
        }
    }
    
    if (rb_stor->storio_nb == 0) {
      storio_id = cid;
    }
    else {
      storio_id = ((cid-1)%rb_stor->storio_nb) + 1;
    }

    // Initialize each TCP ports connection with this storage (by sproto)
    for (i = 0; i < STORAGE_NODE_PORTS_MAX; i++) {
        if (io_address[i].port != 0) {

            struct timeval timeo;
            timeo.tv_sec = RBS_TIMEOUT_SPROTO_REQUESTS;
            timeo.tv_usec = 0;

            uint32_t ip = io_address[i].ipv4;
 
            if (ip == INADDR_ANY) {
                // Copy storage hostname and IP
                strcpy(rb_stor->sclients[i].host, rb_stor->host);
                rozofs_host2ip(rb_stor->host, &ip);
            } else {
	        rozofs_ipv4_append(rb_stor->sclients[i].host, ip);
            }

            rb_stor->sclients[i].ipv4 = ip;	    
            rb_stor->sclients[i].port = io_address[i].port+storio_id;	     
            rb_stor->sclients[i].status = 0;
            rb_stor->sclients[i].rpcclt.sock = -1;

            if (sclient_initialize(&rb_stor->sclients[i], timeo) != 0) {
                severe("failed to join storage (host: %s, port: %u), %s.",
                        rb_stor->host, rb_stor->sclients[i].port,
                        strerror(errno));
                goto out;
            }
            rb_stor->sclients_nb++;
        }
    }

    status = 0;
out:
    return status;
}


/** Check if the storage is present on cluster list
 *
 * @param cluster_entries: list of cluster(s).
 * @param cid: unique id of cluster that owns this storage.
 * @param sid: the unique id for the storage to rebuild.
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int rbs_check_cluster_list(list_t * cluster_entries, cid_t cid, sid_t sid) {
    list_t *p, *q;

    list_for_each_forward(p, cluster_entries) {

        rb_cluster_t *clu = list_entry(p, rb_cluster_t, list);

        if (clu->cid == cid) {

            list_for_each_forward(q, &clu->storages) {

                rb_stor_t *stor = list_entry(q, rb_stor_t, list);

                // Check if the sid to rebuild exist in the list
                if (stor->sid == sid)
                    return 0;
            }
        }
    }
    errno = EINVAL;
    return -1;
}

/** Init connections for storage members of a given cluster but not for the 
 *  storage with sid=sid
 *
 * @param cluster_entries: list of cluster(s).
 * @param cid: unique id of cluster that owns this storage.
 * @param sid: the unique id for the storage to rebuild.
 * @param failed: number of failed server comprising the rebuilt server
 * @param available: number of available server
 *
 */
void rbs_init_cluster_cnts(list_t * cluster_entries, 
                          cid_t cid,
                          sid_t sid,
			  int * failed,
			  int * available) {
    list_t *p, *q;

    *failed = 0;
    *available = 0;
    
    list_for_each_forward(p, cluster_entries) {

        rb_cluster_t *clu = list_entry(p, rb_cluster_t, list);

        if (clu->cid == cid) {

            list_for_each_forward(q, &clu->storages) {

                rb_stor_t *rb_stor = list_entry(q, rb_stor_t, list);
		
                // Get connections for this storage
                if (rbs_stor_cnt_initialize(rb_stor,cid) != 0) {
                  severe("rbs_stor_cnt_initialize cid/sid %d/%d failed: %s",
                            cid, rb_stor->sid, strerror(errno));
		  (*failed)++;
		  continue;	    
                }
		
		if (rb_stor->sid == sid) {
		  (*failed)++; // Storage to rebuild is counted as failed
		}   
                else {
		  (*available)++;
		}  
            }
        }
    }
}
/** Release the list of cluster(s)
 *
 * @param cluster_entries: list of cluster(s).
 */
void rbs_release_cluster_list(list_t * cluster_entries) {
    list_t *p, *q, *r, *s;
    int i = 0;

    list_for_each_forward_safe(p, q, cluster_entries) {

        rb_cluster_t *clu = list_entry(p, rb_cluster_t, list);

        list_for_each_forward_safe(r, s, &clu->storages) {

            rb_stor_t *rb_stor = list_entry(r, rb_stor_t, list);

            // Remove and free storage
            mclient_release(&rb_stor->mclient);

            for (i = 0; i < rb_stor->sclients_nb; i++)
                sclient_release(&rb_stor->sclients[i]);

            list_remove(&rb_stor->list);
            xfree(rb_stor);

        }

        // Remove and free cluster
        list_remove(&clu->list);
        xfree(clu);
    }
}




/** Remove empty rebuild list files 
 *
 */
int rbs_empty_dir(char * dirname) {
  char           fileName[FILENAME_MAX];
  DIR           *dir;
  struct dirent *file;
    
  /*
  ** Open this directory
  */
  dir = opendir(dirname);
  if (dir == NULL) {
    if (errno == ENOENT) return 0;
    severe("opendir(%s) %s", dirname, strerror(errno));
    return -1;
  } 	  
  /*
  ** Loop on distibution sub directories
  */
  while ((file = readdir(dir)) != NULL) {  
    if (strcmp(file->d_name,".")==0)  continue;
    if (strcmp(file->d_name,"..")==0) continue;
    char * pChar = fileName;
    pChar += rozofs_string_append(pChar,dirname);
    pChar += rozofs_string_append(pChar,"/");
    pChar += rozofs_string_append(pChar,file->d_name);
    unlink(fileName);
  }
  
  closedir(dir);
  return 0;
} 



