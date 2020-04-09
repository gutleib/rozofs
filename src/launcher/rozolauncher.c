/*
  Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
  This file is part of rozo.

  rozo is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published
  by the Free Software Foundation, version 2.

  rozo is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see
  <http://www.gnu.org/licenses/>.
 */

#include <inttypes.h>
#include <assert.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/wait.h>
#include <stdarg.h>

 
//#define LAUNCHER_TRACE 1
#ifdef LAUNCHER_TRACE
#include <rozofs/common/log.h>
#endif


/*
 *_______________________________________________________________________
 */

#define MIN_CHILD_RESTART_DELAY_SEC 10

pid_t    child_pid=0;
int      rozolauncher_signal_raised=0;
char   * pid_file_name=NULL;
 

/*
 *_______________________________________________________________________
 * When receiving a signal this handler kills its child
 * and then deletes the pid file.
 */
void rozolauncher_catch_signal(int sig){

  if  (rozolauncher_signal_raised != 0) return;
  rozolauncher_signal_raised++;

  if (child_pid) kill(child_pid,SIGTERM);
  if (pid_file_name) {
    unlink(pid_file_name);
    free(pid_file_name);
    pid_file_name = NULL;
  }  
  
#ifdef LAUNCHER_TRACE  
  closelog();
#endif

  /* Adios */
  signal (sig,SIG_DFL);
  raise (sig);
}
/*
 *_______________________________________________________________________
 * When receiving a hangup this handler propagates it to its child
 */
void rozolauncher_catch_hup(int sig){
  
  /*
  ** Propagate hangup to the child
  */
  if (child_pid) kill(child_pid,SIGHUP);
  
  signal(SIGHUP,rozolauncher_catch_hup);
}
/*
 *_______________________________________________________________________
 */
void rozolauncher_catch_sigpipe(int s){
  signal(SIGPIPE,rozolauncher_catch_sigpipe);
}
/*
 *_______________________________________________________________________
  *
  * Read pid file when it exist and return back the pid
  *
  * @param pid_file The name of the pid file
  *
  * @retval pid or 0 
 */
pid_t rozolauncher_read_pid_file(char * pid_file) {
  pid_t pid = 0;
  int   fd  = -1;
  char  process_id_string[64];
  int   ret;   

  /*
  ** Check input 
  */
  if ((pid_file == NULL) || (pid_file[0] == 0)) {
    goto out;
  }   
  
  /*
  ** Open file
  */  
  fd = open(pid_file,O_RDONLY, 0640);
  if (fd < 0) {
    goto out;
  }
    
  /*
  ** Read file
  */  
  ret = pread(fd,&process_id_string, 64,0);
  if (ret < 0) {
    goto out;
  }
  
  /*
  ** Scan for pid
  */
  ret = sscanf(process_id_string,"%d",&pid);
  if (ret != 1) {
    goto out;
  }
  
out:

  /*
  ** Cmose file when open
  */
  if (fd >=0) {
    close(fd);
    fd = 0;
  }
  
  return pid;
}
/*
 *_______________________________________________________________________
  *
  * Send a signal to a rozolauncher
  *
  * @param pid_file The name of the pid file
  * @signal         The signal to send
 */
int rozolauncher_send_signal(char * pid_file, int signal) {
  pid_t   pid;

  pid = rozolauncher_read_pid_file(pid_file);
  if (pid == 0) return -1;

#ifdef LAUNCHER_TRACE
  info("send signal %d to process %d %s",signal,pid,pid_file);
#endif 
  
  kill(pid,signal);
  return 0;
}
/*
 *_______________________________________________________________________
  *
  * Try to read the process id in the gieven pid file and sends it a SIGTERM
  *
  * @param pid_file The name of the pid file
  * @timer          Time in ms to wait for the process to dy
 */
int rozolauncher_stop(char * pid_file, int timer) {
  struct timespec ts;

  if (rozolauncher_send_signal(pid_file,SIGTERM) < 0) {
    return -1;
  }  
  
  while (timer) {
  
    /*
    ** pid file has been removed
    */      
    if (access(pid_file,R_OK)!=0) return 0;
    
    /*
    ** pid file not yet removed. 
    ** Let's wait...
    */
    timer--;
    ts.tv_sec  = 0;
    ts.tv_nsec = 1000000;
    nanosleep(&ts,NULL);      
  }  

#ifdef LAUNCHER_TRACE
  info("File %s not yet removed !!!",pid_file);
#endif   

  return 0;
}

/*
 *_______________________________________________________________________
  *
  * Try to read the process id in the given pid file and send it a SIGTERM
  *
  * @param pid_file The name of the pid file
 */
int rozolauncher_reload(char * pid_file) {
  return rozolauncher_send_signal(pid_file,SIGHUP);
}
/*
 *_______________________________________________________________________
 * 
 * Saves the current process pid in the file whos name is given in input
 *
 * @param pid_file The name of the file to save the pid in
 *
 */
void rozolauncher_write_pid_file(char * pid_file) {
  int   fd;
  char  process_id_string[64];
  
  fd = open(pid_file,O_RDWR|O_CREAT|O_TRUNC, 0640);
  if (fd < 0) {

#ifdef LAUNCHER_TRACE
    severe("open(%s) %s",pid_file,strerror(errno));
#endif   
    return;
  }
  sprintf(process_id_string,"%d",getpid());  
  if (pwrite(fd,&process_id_string, strlen(process_id_string),0)>0) {
    pid_file_name = strdup(pid_file);
  }
  close(fd);  
}
/*
*______________________________________________________________________
* Create a directory, recursively creating all the directories on the path 
* when they do not exist
*
* @param path2create      The directory path to create
* @param mode             The rights
*
* retval 0 on success -1 else
*/
static inline int rozofs_launcher_mkpath(char * path2create, mode_t mode) {
  char* p;
  int  isZero=1;
  int  status = -1;
  char  directory_path[1024+1];
  
  strcpy(directory_path,path2create);
    
  p = directory_path;
  p++; 
  while (*p!=0) {
  
    while((*p!='/')&&(*p!=0)) p++;
    
    if (*p==0) {
      isZero = 1;
    }  
    else {
      isZero = 0;      
      *p = 0;
    }
    
    if (access(directory_path, F_OK) != 0) {
      if (mkdir(directory_path, mode) != 0) {
        if (errno != EEXIST) {
          goto out;
        }  
      }      
    }
    
    if (isZero==0) {
      *p ='/';
      p++;
    }       
  }
  status = 0;
  
out:
  if (isZero==0) *p ='/';
  return status;
}
/*
 *_______________________________________________________________________
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
#ifdef LAUNCHER_TRACE
    severe("%s",error_buffer);
#endif   
    printf("%s\n",error_buffer);
  }
  
  
  fprintf(stderr, "Usage: \n");
  fprintf(stderr, "  rozolauncher start <pid file> <command line>\n");
  fprintf(stderr, "     This command launches a process that runs the command defined\n");
  fprintf(stderr, "     in <command line> and relaunches it when it fails. The process\n");  
  fprintf(stderr, "     saves its pid in <pid file>.\n");
  fprintf(stderr, "  rozolauncher daemon <pid file> <command line>\n");
  fprintf(stderr, "     Identical to start but daemonize.\n");
  fprintf(stderr, "  rozolauncher stop <pid file>\n");
  fprintf(stderr, "     This command kill the process whose pid is in <pid file>\n");
  fprintf(stderr, "  rozolauncher reload <pid file>\n");
  fprintf(stderr, "     This command reload the process whose pid is in <pid file>\n");

  exit(-1);
}
/*
 *_______________________________________________________________________
** argv[0] is rozolauncher
** argv[1] is either start, daemon, stop or reload
** argv[2] pid file name
** argv[3...  the command line to run in case of a start or daemon command
 */
int main(int argc, char *argv[]) {
  time_t   last_start = 0;
  int      daemonize;
  pid_t    pid;

#ifdef LAUNCHER_TRACE
  openlog("launcher", LOG_PID, LOG_DAEMON);
  info("%s %s",argv[1],argv[2]);
#endif 

  /*
  ** Change local directory to "/"
  */
  if (chdir("/")!=0) {}

  /*
  ** Check the number of arguments
  */
  if (argc < 3) usage("rozolauncher requires at least 3 arguments but only %d are provided",argc);

  rozofs_launcher_mkpath("/var/run/rozofs/pid/",0755);

  /*
  ** Stop
  */
  if (strcmp(argv[1],"stop")==0) {
    return rozolauncher_stop(argv[2],50);
  }
  /*
  ** Reload
  */
  if (strcmp(argv[1],"reload")==0) {
    return rozolauncher_reload(argv[2]);
  }

  /*
  ** Start/daemon
  */
  daemonize = 0;
  if (strcmp(argv[1],"start")!=0){
  
    if (strcmp(argv[1],"daemon")==0){
      daemonize = 1;
    }  
    else {
      usage("rozolauncher 1rst argument \"%s\" should be within <start|stop|reload|daemon>",argv[1]); 
    }  
  }
     
  /*
  ** Check the number of arguments
  */
  if (argc < 4) { 
    usage("rozolauncher <start|daemon> requires at least 4 arguments but only %d are provided",argc);
  }  
  
  /*
  ** Kill previous process if any
  */
  rozolauncher_stop(argv[2],100);
 

  char *daemon_name;
  char exe_path[1024];
  strcpy(exe_path, argv[3]);
  daemon_name = basename(exe_path);

  /*
  ** daemonize storaged
  */
  if ( strcmp(daemon_name, "storaged") == 0) {
    daemonize = 1;
  }
  
  /*
  ** Let's daemonize 
  */
  if (daemonize) {
    #ifdef LAUNCHER_TRACE
        info("daemonize launcher");
    #endif
    if (daemon(0, 0) != 0) {
      #ifdef LAUNCHER_TRACE
            severe("daemon failed %s", strerror(errno));
      #endif
      return -1;
    }
  }

  
  /*
  ** Write pid file
  */
  rozolauncher_write_pid_file(argv[2]);
  
  
  
  /* 
  ** Declare a signal handler 
  */
  signal (SIGQUIT, rozolauncher_catch_signal);
  signal (SIGILL,  rozolauncher_catch_signal);
  signal (SIGBUS,  rozolauncher_catch_signal);
  signal (SIGSEGV, rozolauncher_catch_signal);
  signal (SIGFPE,  rozolauncher_catch_signal);
  signal (SIGSYS,  rozolauncher_catch_signal);
  signal (SIGXCPU, rozolauncher_catch_signal);  
  signal (SIGXFSZ, rozolauncher_catch_signal);
  signal (SIGABRT, rozolauncher_catch_signal);
  signal (SIGINT,  rozolauncher_catch_signal);
  signal (SIGTERM, rozolauncher_catch_signal);
  
  /* Ignored signals */  
  signal(SIGCHLD, SIG_IGN);
  signal(SIGTSTP, SIG_IGN);
  signal(SIGTTOU, SIG_IGN);
  signal(SIGTTIN, SIG_IGN);
  signal(SIGWINCH, SIG_IGN);

  /* 
   * redirect SIGPIPE signal to avoid the end of the 
   * process when a TCP connexion is down
   */   
  signal (SIGPIPE,rozolauncher_catch_sigpipe);
  
  /*
  ** Add a handler for hangup  
  */
  signal (SIGHUP,rozolauncher_catch_hup);
  
  /*
  ** Never ending loop
  */
  while(1) {
  
    /*
    ** Check we do not loop on restarting too fast
    */
    time_t now = time(NULL);
    if ((now-last_start) < MIN_CHILD_RESTART_DELAY_SEC) {
      sleep(MIN_CHILD_RESTART_DELAY_SEC-(now-last_start));
      last_start = time(NULL);
    }
    else last_start = now;
    
    
    /*
    ** Create a child process
    */
    child_pid = fork();


    /*
    ** Child process executes the command
    */
    if (child_pid == 0) {
      pid_file_name = NULL;
      execvp(argv[3],&argv[3]);
      exit(0); 
    }
#ifdef LAUNCHER_TRACE
    info("%s %s PID is %d",argv[1],argv[2],child_pid);
#endif 
    /*
    ** Father process waits for the child to fail
    ** to relaunch it
    */
    waitpid(child_pid,NULL,0);
    
    /**
    ** Check the content of the pid file
    */
    pid = rozolauncher_read_pid_file(argv[2]);
    if (pid != getpid()) {
#ifdef LAUNCHER_TRACE
      info("%s %s pid %d is no more the active one",argv[1],argv[2],getpid());
#endif 
      exit(-1);      
    }
  }
}

