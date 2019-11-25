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
#include <stdarg.h>
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
#include <sys/types.h>
#include <attr/xattr.h>



#include <rozofs/rozofs.h>
#include <rozofs/core/rozofs_rcmd.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/core/rozofs_host_list.h>

#define BUFFER_SIZE 4096
char   value[BUFFER_SIZE];
char   fidString[BUFFER_SIZE];
char  *pChar;
char  *nocolor = "";

#define FNAME "rozofs_locate_prjections"
/*-----------------------------------------------------------------------------
**
**  Exit function
**
**----------------------------------------------------------------------------
*/
void finish(int code) {
  
  exit(code);  
}
/*-----------------------------------------------------------------------------
**
**  Display error and exit
**
**----------------------------------------------------------------------------
*/
void fatal_exit(int res, char * fmt, ...) {
  va_list   args;
  char      error_buffer[512];
  
  /*
  ** Display optionnal error message if any
  */
  if (fmt) {
    va_start(args,fmt);
    vsprintf(error_buffer, fmt, args);
    va_end(args);   
    printf("%s !!!\n",error_buffer);
  }
  finish(res); 
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
  printf("Rozofs file projection location utility - %s\n", VERSION);
  printf("Usage: rozo_locate_projections [OPTIONS] <MANDATORY>\n\n");
  printf ("  MANDATORY: \n");
  printf("\t    -n, --name\tFile name to locate pieces for.\n");    
  printf ("  OPTIONS:\n");
  printf("\t    -C, --nocolor\tDo not colorize output.\n");
  printf("\t    -h, --help  \tprint this message.\n");
  printf("\t    -k, --config\tconfiguration file to use (default: %s).\n",EXPORTD_DEFAULT_CONFIG);
  exit(EXIT_FAILURE);
}
/*
 *_______________________________________________________________________
 */
int main(int argc, char *argv[]) {
  char exportd_config_file[256] = {0};
  int        c;
  char     * fname = NULL;
  uint32_t   cid = -1;        
  fid_t      fid;
  int        size;
  char * pMetaAddr      = NULL;
  char *pHost;
  int  export_idx;
  int  socketId = -1;
  char remote_fname[128];
  char local_fname[128];
  char param[2048];  
  
  int   res = rozofs_rcmd_status_success;
  char *token;
  

  static struct option long_options[] = {
      {"help", no_argument, 0, 'h'},
      {"config", required_argument, 0, 'k'},
      {"nocolor", no_argument, 0, 'C'},
      {"name", required_argument, 0, 'n'},
      {0, 0, 0, 0}
      };

  while (1) {

    int option_index = 0;
    c = getopt_long(argc, argv, "hCk:n:", long_options, &option_index);

    if (c == -1) break;

    switch (c) {

      case 'h':
        usage(NULL);
        exit(EXIT_SUCCESS);
        break;

      case 'k':
        strcpy(exportd_config_file,optarg);
        break;

      case 'C':
        nocolor = "--nocolor"; 
        break;

      case 'n':
         fname = optarg;
         size = getxattr(fname,"user.rozofs",value,BUFFER_SIZE);
         if (size <= 0) {
           usage("%s is not a RozoFS file",fname);
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

  if (fname == NULL) {
    usage("Missing file name");
  }
    
  /*
  ** Change local directory to "/"
  */
  if (chdir("/")!=0) {}

  uma_dbg_record_syslog_name("LocateProj");

  rozofs_layout_initialize();

  /*
  ** Get export address from config
  */
  if (pMetaAddr == NULL) {

    /*
    ** read common config file
    */
    common_config_read(NULL);         

    /*
    ** Parse host list
    */
    pMetaAddr = common_config.export_hosts;
  }  
    
  /*
  ** Parse export host list 
  */  
  if (rozofs_host_list_parse(pMetaAddr,'/') == 0) {
    fatal_exit(rozofs_rcmd_status_no_connection, "Bad export host address %s",pMetaAddr);   
  }  


  // Init of the timer configuration
  rozofs_tmr_init_configuration();

  /*
  ** Loop on configured exportd and connect to the remote command server
  */
  pHost = NULL;
  for (export_idx=0; export_idx<ROZOFS_HOST_LIST_MAX_HOST; export_idx++) {

    pHost = rozofs_host_list_get_host(export_idx);
    if (pHost == NULL) break;
      
    socketId = rozofs_rcmd_connect_to_server(pHost);
    if (socketId == -1) continue;  
    
    break;
  }
  
  if (socketId == -1) {
    fatal_exit(rozofs_rcmd_status_no_connection,"Can not connect to remote command server");
  }  

  /*
  ** Prepare local and remote file names to exchanges results 
  ** between local node and export node
  */
  sprintf(remote_fname,"/tmp/%s.%d",FNAME,getpid());
  sprintf(local_fname,"/tmp/%s.loc.%d",FNAME,getpid());

  /*
  ** Prepare rcmd command
  */ 
  pChar = param;
  if (exportd_config_file[0] != 0) {
    pChar += sprintf(pChar," -k %s ", exportd_config_file);
  }
  pChar += sprintf(pChar," %s ", nocolor);
  
  cid = 0;
  fidString[0] = 0;

  token = strtok(value,"\n");
  while ((token = strtok(NULL,"\n")) != 0) {
         
     /*
     ** Scan cluster id
     */
     if (strncmp(token,"CLUSTER",strlen("CLUSTER")) == 0) {
     
       token += strlen("CLUSTER");
       
       if (sscanf(token," : %d", &cid) != 1) {
         printf("Error parsing CLUSTER id in xattr %s",strerror(errno));
         exit(1);
       }
       continue;
     }  

     /*
     ** Scan FID
     */     
     if (strncmp(token,"FID_SP",strlen("FID_SP")) == 0) {
     
       if (cid == 0) continue;
       
       token += strlen("FID_SP");
       
       if (sscanf(token," : %s", fidString) != 1) {
         printf("Error parsing FID_SP in xattr %s",strerror(errno));
         exit(1);
       }
       if (rozofs_uuid_parse(fidString, fid)) {
         printf("Bad FID value %s",fidString);
         exit(1);  
       } 
       
       pChar += sprintf(pChar, "-c %d -f %s ", cid, fidString);    
       
       /*
       ** Reset and reloop
       */
       cid = 0;
       fidString[0] = 0;      
       continue;
     }        
  } 
  
  /*
  ** Finalize the command
  */
  pChar += sprintf(pChar," > %s", remote_fname);    

  printf("\n{\n  \"fname\" : \"%s\",\n", fname);          
  fflush(stdout);
  
  /*
  ** Send the command
  */
  res = rozofs_rcmd_locate_projections(socketId, param);
  if (res != rozofs_rcmd_status_success) {
    fatal_exit(res,"Error rozofs_rcmd_locate_projections %s", rozofs_rcmd_status_e2String(res));
  } 

  /*
  ** Get the response
  */
  res = rozofs_rcmd_getrmfile(socketId, remote_fname, local_fname, 1);
  if (res != rozofs_rcmd_status_success) {
    fatal_exit(res,"Error rozofs_rcmd_getrmfile %s", rozofs_rcmd_status_e2String(res));
  }   

  sprintf(param,"cat %s", local_fname);    
  if (system(param)) {};
  unlink(local_fname);     
  
  rozofs_rcmd_disconnect_from_server(socketId);
  finish(EXIT_SUCCESS);
  return EXIT_SUCCESS;
}
