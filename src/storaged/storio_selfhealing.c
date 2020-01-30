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
#define _XOPEN_SOURCE 500

#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/sysmacros.h>
#include <getopt.h>
#include <inttypes.h>
 
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>

#define STORIO_REBUILD_BASE_CMD "storage_rebuild -fg "

/*
**___________________________________________________________
** M A I N
**___________________________________________________________
**
*/
int main(int argc, char *argv[]) {
  int c;
  static struct option long_options[] = {
      { "args", required_argument, 0, 'a'},
      { 0, 0, 0, 0}
  };
  char     * args_file  = NULL;
  char       rebuild_command[512];
  char     * pChar;
  int        fd = -1;
  int        nb;
  pid_t      pid;
  int        status;

  /*
  ** Change local directory to "/"
  */
  if (chdir("/")!= 0) {}
  
  openlog("storio_selfhealing", LOG_PID, LOG_DAEMON); 
 
  while (1) {

    int option_index = 0;
    c = getopt_long(argc, argv, "a:", long_options, &option_index);

    if (c == -1) break;

    switch (c) {

      case 'a':
        args_file = optarg;
        break;
        
      default:
        severe("Unexpected parameter \'%c\'",c);
        exit(1);
        break;
    }
  }    

  if (args_file==NULL) {
    severe("Missing args file mandatory parameter");
    exit(1);
  }    


  /*
  ** Wait the rebuild argument file to be written
  */
  while(1) {

    /*
    ** Close and remove the command file of the previous run
    */
    if (fd>0) {
      close(fd);
      fd = -1;
    }  
    unlink(args_file);

    /*
    ** Wait on the rebuild argument file to be created
    */   
    while (1) {
      if (access(args_file, F_OK) == 0) break;        
      sleep(60); 
    }

    /*
    ** Read rebuild parameters
    */
    pChar = rebuild_command;
    pChar += sprintf(pChar, STORIO_REBUILD_BASE_CMD);

    fd = open(args_file, O_RDONLY);
    if (fd < 0) {
      severe("open(%s) %s", args_file, strerror(errno));
      continue;
    }   

    nb = read(fd, pChar, sizeof(rebuild_command)-strlen(STORIO_REBUILD_BASE_CMD));
    if (nb <= 0) {
      severe("read(%s) %s", args_file ,strerror(errno));
      continue;
    } 
    pChar[nb] = 0;

    info("self healing start");     
    status = system(rebuild_command);
    info("self healing stop");     
  }  
}    

