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



#include <rozofs/rozofs.h>
#include <rozofs/core/rozofs_rcmd.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/core/rozofs_host_list.h>

char   tmp_input_file[128];
char   tmp_output_file[128];

#define FNAME "rozofs_fid2path"
/*-----------------------------------------------------------------------------
**
**  Exit function
**
**----------------------------------------------------------------------------
*/
void finish(code) {

  /*
  ** Remove temporary input file if any
  */
  if (tmp_input_file[0] != 0) {
    unlink(tmp_input_file);
  }
  
  /*
  ** Display and remove temporary output file 
  */
  if (tmp_output_file[0] != 0) {

    char cmd[128];
    sprintf(cmd,"cat %s",tmp_output_file);
    if (system(cmd)==0) {}

    unlink(tmp_output_file);
  }  
  
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
    printf("%s !!!\n",error_buffer);
  }
  
  /*
  ** Display usage
  */
  printf("\nUsage: rozo_storage_fid2pathname -i <input_filename -o output_filename [-c export_cfg_file]\n\n");
  printf("Options:\n");
  printf("\t-h,--help:     print this message.\n");
  printf("\t-i,--input:    fid of the objet or input filename \n");
  printf("\t               that contains a list of one fid per line\n");
  printf("\t-o,--output:   output filename for full path translation (optional) \n\n");
  printf("\t-c,--config:   exportd configuration file name (when different from %s)\n\n",EXPORTD_DEFAULT_CONFIG);
  
  finish(EXIT_FAILURE); 
}

/*
 *_______________________________________________________________________
 */
int main(int argc, char *argv[]) {
  int c;
  char * input_path     = NULL;
  char * output_path    = NULL;
  char * configFileName = NULL;
  
  FILE *fd_in = NULL;
  int  socketId = -1;
  int  export_idx;
  char *pHost;
  int   res = rozofs_rcmd_status_success;
  char remote_fname[128];
  char param[256];
  fid_t  fid;

  static struct option long_options[] = {
      {"help", no_argument, 0, 'h'},
      {"input", required_argument, 0, 'i'},
      {"output", required_argument, 0, 'o'},
      {"config", required_argument, 0, 'c'},	
      {0, 0, 0, 0}
  };

  /*
  ** Change local directory to "/"
  */
  if (chdir("/")!= 0) {}

  tmp_input_file[0] = 0;    
  tmp_output_file[0] = 0;    
  
  while (1) {

    int option_index = 0;
    c = getopt_long(argc, argv, "hlrc:i:o:c:r", long_options, &option_index);

    if (c == -1)
        break;

    switch (c) {

      case 'h':
        usage(NULL);
        break;
      case 'i':
        input_path = optarg;
        break;
      case 'o':
        output_path = optarg;
        break;
      case 'c':
        configFileName = optarg;
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
  ** input path
  */
  
  /*
  ** It is mandatory
  */
  if (input_path == NULL) {
    usage("Missing -i parameter");
  }
  
  /*
  ** One only one FID is given, write it to a temporary file
  */
  if (rozofs_uuid_parse(input_path, fid) == 0) {
    char cmd[128];
    sprintf(tmp_input_file,"/tmp/rozofs_fid2path_input.%d",getpid());
    sprintf(cmd, "echo %s > %s", input_path, tmp_input_file);
    if (system(cmd)==0) {}
    input_path = tmp_input_file;
  }
  /*
  **  check the presence of the input file
  */    
  if ((fd_in = fopen(input_path,"r")) == NULL) {
    usage("can not open file: %s: %s\n",input_path,strerror(errno));
  }         
  fclose(fd_in); 


  /*
  ** ouput path
  */

  /*
  ** No ouput file given. Create a temporary output file name
  */
  if (output_path == NULL) {
    char cmd[128];
    sprintf(tmp_output_file,"/tmp/rozofs_fid2path_output.%d",getpid());
    sprintf(cmd, "echo > %s", tmp_output_file);
    if (system(cmd)==0) {}    
    output_path = tmp_output_file;
  }

  rozofs_layout_initialize();

  /*
  ** read common config file
  */
  common_config_read(NULL);         

  uma_dbg_record_syslog_name("StFid2Path");

  // Init of the timer configuration
  rozofs_tmr_init_configuration();

  /*
  ** Parse host list
  */
  if (rozofs_host_list_parse(common_config.export_hosts,'/') == 0) {
    fatal_exit(rozofs_rcmd_status_no_connection, "export_hosts missing in %s",ROZOFS_CONFIG_DIR"/rozofs.conf");
  }   
  
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
  ** Transfer local file
  */
  sprintf(remote_fname,"%s.%d",FNAME,getpid());
  res = rozofs_rcmd_puttmpfile(socketId,input_path, remote_fname);
  if (res != rozofs_rcmd_status_success) {
    fatal_exit(res,"Error rozofs_rcmd_puttmpfile %s",rozofs_rcmd_status2String(res));
  } 

  
  /*
  ** Execute fid2pathname
  */ 
  if (configFileName != NULL) {
    sprintf(param,"-i /tmp/%s -o /tmp/%s.out -c %s", remote_fname, remote_fname, configFileName);    
  }
  else {
    sprintf(param,"-i /tmp/%s -o /tmp/%s.out", remote_fname, remote_fname);
  }  
  res = rozofs_rcmd_fid2path(socketId, param);
  if (res != rozofs_rcmd_status_success) {
    fatal_exit(res,"Error rozofs_rcmd_fid2path %s", rozofs_rcmd_status2String(res));
  } 
  
  sprintf(remote_fname,"/tmp/%s.%d.out",FNAME,getpid());
  
  res = rozofs_rcmd_getrmfile(socketId, remote_fname, output_path, 1);
  if (res != rozofs_rcmd_status_success) {
    fatal_exit(res,"Error rozofs_rcmd_getrmfile %s", rozofs_rcmd_status2String(res));
  }   
    
  rozofs_rcmd_disconnect_from_server(socketId);
  finish(EXIT_SUCCESS);
  return EXIT_SUCCESS;
}
