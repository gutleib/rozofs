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
#include <stdarg.h>
#include <sys/types.h>
#include <attr/xattr.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/common/daemon.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/common_config.h>
#include <rozofs/rpc/export_profiler.h>
#include <rozofs/common/profile.h>
#include <rozofs/rpc/eproto.h>
#include <rozofs/rpc/mproto.h>
#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/rpc/gwproto.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/rpc/export_profiler.h>
#include <rozofs/common/profile.h>
#include <rozofs/common/rozofs_site.h>
#include <rozofs/rpc/rpcclt.h>
#include <rozofs/rpc/eproto.h>

#include "config.h"
#include "exportd.h"
#include "export.h"
#include "monitor.h"
#include "econfig.h"
#include "volume.h"
#include "export_expgw_conf.h"

int rozofs_no_site_file = 0;
char     * fidString = NULL;
uint64_t total_sectors = 0;


#define BUFFER_SIZE 4096
char   value[BUFFER_SIZE];
char * pChar;

/*_________________________________________________________________
** Ask to a logical storage to get the number of blocks of file 
  The purpose of that service is to address the case of the thin provisioning

  @param  clt     The client toward the storaged
  @param  cid     The cluster identifier
  @param  sid     The storage identifier within the cluster
  @param  fid     The FID of the file  
  @param  rsp_p : pointer to the array with the response is stored

  @retval         0 on sucess
  @retval          -1 on error (see errno for details)
*/
int locate_request(char * host, rpcclt_t * rpcClient, cid_t cid, sid_t sid, fid_t fid, int addComma) {
  int              retVal = 0;
  mp_locate_ret_t * ret    = 0;
  mp_locate_arg_t   args;
  mp_file_t      * pFile;
  int              idx;
  int              first;
  char             dateString[64];
  
  args.cid   = cid;
  args.sid   = sid;
  args.spare = 0;
  memcpy(args.fid, fid, sizeof (fid_t));

  if (!(rpcClient->client) || !(ret = mp_locate_1(&args, rpcClient->client))) {
    errno = EPROTO;
    goto out;
  }
  
  if (ret->status != 0) {
    errno = ret->mp_locate_ret_t_u.error;
    goto out;
  }
  /*
  ** Print the received information
  */
  first = 1;

  if ((ret->mp_locate_ret_t_u.rsp.hdrs.mp_files_t_len != 0) 
  ||  (ret->mp_locate_ret_t_u.rsp.chunks.mp_files_t_len!= 0)) {
    retVal = 1;
    
    if (addComma) printf(",");
    
    switch (addComma%6) {
      case 0:
        printf(ROZOFS_COLOR_CYAN);
        break;
      case 1:  
       printf(ROZOFS_COLOR_YELLOW);
       break;
     case 2:
       printf(ROZOFS_COLOR_BLUE);
       break;
     case 3:
       printf(ROZOFS_COLOR_RED);
       break;          
     case 4:
       printf(ROZOFS_COLOR_GREEN);
       break;  
     default:
       printf(ROZOFS_COLOR_PURPLE);
       break;
       
    }    
    if (ret->mp_locate_ret_t_u.rsp.hdrs.mp_files_t_len != 0) {
      pFile = ret->mp_locate_ret_t_u.rsp.hdrs.mp_files_t_val;
      for (idx=0; idx < ret->mp_locate_ret_t_u.rsp.hdrs.mp_files_t_len; idx++,pFile++) {  
        rozofs_time2string(dateString, pFile->modDate);
        if (first) first = 0;
        else printf(",");       
        printf ("\n    { \"host\": \"%s\", \"size\": %11llu, \"sectors\": %6llu, \"date\": \"%s\",", 
                host,
                (unsigned long long)pFile->sizeBytes, 
                (unsigned long long)pFile->sectors, 
                dateString);
        printf ("\n      \"name\": \"%s\"}", pFile->file_name);
        total_sectors += pFile->sectors;       
      }       
    }
    printf(ROZOFS_COLOR_BOLD);
    if (ret->mp_locate_ret_t_u.rsp.chunks.mp_files_t_len != 0) {
      pFile = ret->mp_locate_ret_t_u.rsp.chunks.mp_files_t_val;
      for (idx=0; idx < ret->mp_locate_ret_t_u.rsp.chunks.mp_files_t_len; idx++,pFile++) { 
        rozofs_time2string(dateString, pFile->modDate);
        if (first) first = 0;
        else printf(",");       
        printf ("\n    { \"host\": \"%s\", \"size\": %11llu, \"sectors\": %6llu, \"date\": \"%s\",", 
                host,
                (unsigned long long)pFile->sizeBytes, 
                (unsigned long long)pFile->sectors,                 
                dateString);
        printf ("\n      \"name\": \"%s\"}", pFile->file_name);                
        total_sectors += pFile->sectors;                       
      }       
    } 
    printf(ROZOFS_COLOR_NONE);
  }
    
out:
  if (ret) {
    xdr_free((xdrproc_t) xdr_mp_status_ret_t, (char *) ret);
  } 
  return retVal; 
}
/*
 *_______________________________________________________________________
 */
void locate_cluster(cluster_config_t * cconfig, fid_t fid) {
  int                     count = 0; 
  rpcclt_t                rpcClient;
  storage_node_config_t * sconfig = NULL;
  list_t                * p;
  char                    *pStart, *pChar;
  int                     next;
  int                     success;
  struct timeval          timeout;  
  char                    hostString[1024]; 
  memset(&rpcClient, 0, sizeof(rpcClient));
  rpcClient.sock = -1;
  
  /*
  ** Let's connect the new storaged
  */
  timeout.tv_sec  = 10;
  timeout.tv_usec = 0;
  
  uint16_t mproto_service_port = rozofs_get_service_port_storaged_mproto();
  
  list_for_each_forward(p, (&cconfig->storages[0])) {
    
    sconfig = list_entry(p, storage_node_config_t, list);  
    
    success = 0;
    
    strcpy(hostString, sconfig->host);
    pChar = hostString;
    next  = 1;
    
    while (next) {
    
      next   = 0;
      pStart = pChar;
      
      while ((*pChar != 0)||(*pChar =! '/')) pChar++;
      if (*pChar == '/') next = 1;
      *pChar = 0;
      pChar ++;
      
      if (rpcclt_initialize(&rpcClient, pStart, MONITOR_PROGRAM,
                        MONITOR_VERSION, ROZOFS_RPC_BUFFER_SIZE, ROZOFS_RPC_BUFFER_SIZE,
                        mproto_service_port, timeout) == 0) {
	success++;
      }
    }
    
    if (success) {   
      count += locate_request(sconfig->host,&rpcClient,cconfig->cid, sconfig->sid, fid, count);
    } 
    
    rpcclt_release(& rpcClient);
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
    severe("%s",error_buffer);
    printf("\n%s !!!\n\n",error_buffer);
  }
  printf("Rozofs fid locate utility - %s\n", VERSION);
  printf("Usage: rozo_locate_fid [OPTIONS] <MANDATORY>\n\n");
  printf ("  MANDATORY: {--cid <cid> --fid <fid>|--name <name>}\n");
  printf("\t    -c, --cid\tcluster identifier to search fid in.\n");
  printf("                \tcid 0 to search for fid in all clusters\n");    
  printf("\t    -f, --fid\tfid to search for.\n");    
  printf("\t    -n, --name\tFile name to search for.\n");    
  printf ("  OPTIONS:\n");
  printf("\t    -h, --help  \tprint this message.\n");
  printf("\t    -k, --config\tconfiguration file to use (default: %s).\n",EXPORTD_DEFAULT_CONFIG);
  exit(EXIT_FAILURE);
}
/*
 *_______________________________________________________________________
 */
int main(int argc, char *argv[]) {
    char exportd_config_file[256];
    int c;
    char     * fname = NULL;
    uint32_t   cid = -1;        
    fid_t      fid;
    int        eid;
    list_t   * p;
    export_config_t  *  econfig = NULL;
    volume_config_t  *  vconfig = NULL;  
    cluster_config_t *  cconfig = NULL;   
    int size;
    
    
    static struct option long_options[] = {
        {"help", no_argument, 0, 'h'},
        {"config", required_argument, 0, 'k'},
        {"fid", required_argument, 0, 'f'},
        {"cid", required_argument, 0, 'c'},
        {"name", required_argument, 0, 'n'},
        {0, 0, 0, 0}
    };
    
    strcpy(exportd_config_file,EXPORTD_DEFAULT_CONFIG);

    while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "hc:k:f:n:", long_options, &option_index);

      if (c == -1) break;

      switch (c) {

        case 'h':
          usage(NULL);
          exit(EXIT_SUCCESS);
          break;
          
        case 'k':
          if (!realpath(optarg, exportd_config_file)) {
              usage("configuration file: %s: %s", optarg, strerror(errno));
          }
          break;
          
        case 'c':
          if (sscanf(optarg, "%u", &cid) != 1) {
            usage("Bad CID value %s",optarg);
          }  
          break;
            
        case 'f':
           fidString = optarg;
           if (rozofs_uuid_parse(fidString, fid)) {
             usage("Bad FID value %s",fidString);
           }  
           break; 
           
        case 'n':
           fname = optarg;
           size = getxattr(fname,"user.rozofs",value,BUFFER_SIZE);
           if (size <= 0) {
             usage("%s is not a RozoFS file",fname);
           }
           pChar = strstr(value,"CLUSTER : ");
           if (pChar == NULL) {
             usage("Bad rozofs xattr CLUSTER %s",value);
           }
           if (sscanf(pChar,"CLUSTER : %d",&cid)!=1) {
             usage("Bad rozofs xattr CLUSTER value %s",value);
           }           
           pChar = strstr(value,"FID_SP  : ");
           if (pChar == NULL) {
             usage("Bad rozofs xattr FID_SP %s",value);
           }
           fidString = pChar;
           fidString += strlen("FID_SP  : "); 
           fidString[36] = 0;
           if (rozofs_uuid_parse(fidString, fid)) {
             usage("Bad FID value %s",fidString);
           }             
           break;                            
                           
        case '?':
          usage(NULL);
          break;
            
        default:
          usage("Unexpected parameter");
          break;
      }
    }
    
    /*
    ** Change local directory to "/"
    */
    if (chdir("/")!=0) {}
        
    if (cid == -1) {
      usage("Missing CID value");
    }  
    
    if (fidString == NULL) {
      usage("Missing FID value");
    }  

    if (econfig_initialize(&exportd_config) != 0) {
        usage( "can't initialize exportd config: %s.",
                strerror(errno));
    }
    if (econfig_read(&exportd_config, exportd_config_file) != 0) {
        usage( "failed to parse configuration file: %s.",
                strerror(errno));
    }
    if (econfig_validate(&exportd_config) != 0) {
        usage( "inconsistent configuration file: %s.",
                strerror(errno));
    }

    eid = rozofs_get_eid_from_fid(fid);

    // For each export

    list_for_each_forward(p, &exportd_config.exports) {
    
        econfig = list_entry(p, export_config_t, list);
        if (econfig->eid == eid) break;
        econfig = NULL;
    }
    if (econfig == NULL) {
      usage( "No such eid %d.", eid);
    }


    list_for_each_forward(p, &exportd_config.volumes) {
        
        vconfig = list_entry(p, volume_config_t, list);
        if (vconfig->vid == econfig->vid) break;
        vconfig = NULL;
    }
    if (vconfig == NULL) {
      usage( "No such vid %d.", econfig->vid);
    }
    
    
    printf ("\n{ \"fid\" : \"%s\",\n", fidString);
    printf ("  \"files\" : [");

    list_for_each_forward(p, &vconfig->clusters) {
    
        cconfig = list_entry(p, cluster_config_t, list);
        if ((cid == 0) || (cconfig->cid == cid)) {
          locate_cluster(cconfig, fid);    
        }
    }

   printf("  ],\n  \"total sectors\" : %llu\n}\n", (long long unsigned int)total_sectors);  
   exit(EXIT_SUCCESS);   
}
