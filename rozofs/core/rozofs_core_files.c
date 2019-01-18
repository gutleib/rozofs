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
 
/*__________________________________________________________________________
  Signal handler
  ==========================================================================*/
#define ROZOFS_CORE_C

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <signal.h>
#include <errno.h>
#include <sys/resource.h>
#include <rozofs/rozofs.h>
#include <rozofs/core/rozofs_core_files.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/common/common_config.h>

volatile sig_atomic_t     rozofs_fatal_error_processing = 0;
char                      rozofs_core_file_path[256] = {0};           
int                       rozofs_max_core_files         = 0;

rozofs_attach_crash_cbk_t rozofs_crash_cbk[]= {
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
};
#define ROZFS_MAX_CRASH_CBK (sizeof(rozofs_crash_cbk)/sizeof(rozofs_attach_crash_cbk_t))

int rozofs_crash_cbk_nb   = 0;

rozofs_attach_crash_cbk_t rozofs_hgup_cbk[]= {
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
};
#define ROZFS_MAX_HGUP_CBK (sizeof(rozofs_hgup_cbk)/sizeof(rozofs_attach_crash_cbk_t))
int rozofs_hgup_cbk_nb = 0;
/*__________________________________________________________________________
  Attach a callback to be called when crashing
  ==========================================================================
  PARAMETERS: 
  - entryPoint : the callback
  RETURN: none
  ==========================================================================*/
void rozofs_attach_crash_cbk(rozofs_attach_crash_cbk_t entryPoint) {

  if (rozofs_crash_cbk_nb >= ROZFS_MAX_CRASH_CBK) {
    warning("Alread %d crash cbk registered", rozofs_crash_cbk_nb);
    return;
  }

  if (entryPoint == NULL) return;
  
  rozofs_crash_cbk[rozofs_crash_cbk_nb] = entryPoint;
  rozofs_crash_cbk_nb++;
}
/*__________________________________________________________________________
  Attach a callback to be called on reload
  ==========================================================================
  PARAMETERS: 
  - entryPoint : the callback
  RETURN: none
  ==========================================================================*/
void rozofs_attach_hgup_cbk(rozofs_attach_crash_cbk_t entryPoint) {

  if (rozofs_hgup_cbk_nb >= ROZFS_MAX_HGUP_CBK) {
    warning("Alread %d hgup cbk registered", rozofs_hgup_cbk_nb);
    return;
  }
  
  if (entryPoint == NULL) return;
  
  rozofs_hgup_cbk[rozofs_hgup_cbk_nb] = entryPoint;
  rozofs_hgup_cbk_nb++;
}
/*__________________________________________________________________________
  Just ignore the given signal 
  ==========================================================================
  PARAMETERS: 
  - sig : the signal number to be ignored
  RETURN: none
  ==========================================================================*/
void rozofs_set_handler_signal_ignore(int sig);
void rozofs_signal_ignore(int sig){
  rozofs_set_handler_signal_ignore(sig);
}
/*__________________________________________________________________________
  Set signal handler for ignoring signal
  ==========================================================================
  PARAMETERS: 
  - sig     : the signal received 
  RETURN: none
  ==========================================================================*/
void rozofs_set_handler_signal_ignore(int sig){
  struct sigaction action;

  /* Set up the structure to specify the action. */
  action.sa_handler = rozofs_signal_ignore;
  sigemptyset (&action.sa_mask);
  action.sa_flags = SA_RESTART;
  sigaction (sig, &action, NULL); 
}
/*__________________________________________________________________________
  This function translate a LINUX signal into a readable string
  ==========================================================================
  PARAMETERS: 
  . sig : the signal value
  RETURN: a character string giving the signal name
  ==========================================================================*/
static char sig_num[32];
char * rozofs_signal(int sig) {
  switch(sig) {
  case SIGHUP    : return "SIGHUP";   /* Hangup (POSIX)                                      */
  case SIGINT    : return "SIGINT";   /* Terminal interrupt (ANSI)                           */
  case SIGQUIT   : return "SIGQUIT";  /* Terminal quit (POSIX)                               */
  case SIGILL    : return "SIGILL";   /* Illegal instruction (ANSI)                          */
  case SIGTRAP   : return "SIGTRAP";  /* Trace trap (POSIX)                                  */
  case SIGABRT   : return "SIGABRT";  /* IOT Trap (4.2 BSD)                                  */
  case SIGBUS    : return "SIGBUS";   /* BUS error (4.2 BSD)                                 */
  case SIGFPE    : return "SIGFPE";   /* Floating point exception (ANSI)                     */
  case SIGKILL   : return "SIGKILL";  /* Kill(can't be caught or ignored) (POSIX)            */
  case SIGUSR1   : return "SIGUSR1";  /* User defined signal 1 (POSIX)                       */
  case SIGSEGV   : return "SIGSEGV";  /* Invalid memory segment access (ANSI)                */
  case SIGUSR2   : return "SIGUSR2";  /* User defined signal 2 (POSIX)                       */
  case SIGPIPE   : return "SIGPIPE";  /* Write on a pipe with no reader, Broken pipe (POSIX) */
  case SIGALRM   : return "SIGALRM";  /* Alarm clock (POSIX)                                 */
  case SIGTERM   : return "SIGTERM";  /* Termination (ANSI)                                  */
  case SIGSTKFLT : return "SIGSTKFLT";/* Stack fault                                         */
  case SIGCHLD   : return "SIGCHLD";  /* Child process has stopped or exited, changed (POSIX)*/
  case SIGCONT   : return "SIGCONT";  /* Continue executing, if stopped (POSIX)              */
  case SIGSTOP   : return "SIGSTOP";  /* top executing(can't be caught or ignored) (POSIX)   */
  case SIGTSTP   : return "SIGTSTP";  /* Terminal stop signal (POSIX)                        */
  case SIGTTIN   : return "SIGTTIN";  /* Background process trying to read, from TTY (POSIX) */
  case SIGTTOU   : return "SIGTTOU";  /* Background process trying to write, to TTY (POSIX)  */
  case SIGURG    : return "SIGURG";   /* Urgent condition on socket (4.2 BSD)                */
  case SIGXCPU   : return "SIGXCPU";  /* CPU limit exceeded (4.2 BSD)                        */
  case SIGXFSZ   : return "SIGXFSZ";  /* File size limit exceeded (4.2 BSD)                  */
  case SIGVTALRM : return "SIGVTALRM";/* Virtual alarm clock (4.2 BSD)                       */
  case SIGPROF   : return "SIGPROF";  /* Profiling alarm clock (4.2 BSD)                     */
  case SIGWINCH  : return "SIGWINCH"; /* Window size change (4.3 BSD, Sun)                   */
  case SIGIO     : return "SIGIO";    /* I/O now possible (4.2 BSD)                          */
  case SIGPWR    : return "SIGPWR";   /* Power failure restart (System V)                    */
  case SIGSYS    : return "SIGSYS";
  default        : 
    rozofs_i32_append(sig_num,sig);
    return sig_num;      
  }
}  
/*__________________________________________________________________________
  Only keep the last core dumps
  ==========================================================================
  PARAMETERS: none
  RETURN: none
  ==========================================================================*/
typedef struct rozofs_my_file_s {    
  char          name[256];  
  uint32_t      ctime;
} ROZOFS_MY_FILE_S;

#define ROZOFS_MAX_CORE   16
static ROZOFS_MY_FILE_S directories[ROZOFS_MAX_CORE];
static char buff [1024];

void rozofs_clean_core(char * coredir) {
  struct dirent * dirItem;
  struct dirent * subdirItem;
  struct stat     traceStat;
  DIR           * dir;
  DIR           * subdir;
  uint32_t        nb,idx;
  uint32_t        younger;

  if (rozofs_mkpath(coredir,0744) < 0) return;
   
  /* 
  ** Open core file directory 
  */ 
  dir=opendir(coredir);
  if (dir==NULL) return;

  nb = 0;

  while ((dirItem=readdir(dir))!= NULL) {
    
    /* 
    ** Skip . and .. 
    */ 
    if (dirItem->d_name[0] == '.') continue;

    sprintf(buff,"%s/%s/", coredir, dirItem->d_name); 
    
    /* 
    ** Open core sub-directory 
    */ 
    subdir=opendir(buff);
    if (subdir==NULL) continue;

    while ((subdirItem=readdir(subdir))!= NULL) {

      /* 
      ** Skip . and .. 
      */ 
      if (subdirItem->d_name[0] == '.') continue;

      sprintf(buff,"%s/%s/%s", coredir, dirItem->d_name, subdirItem->d_name); 
    
      /*
      ** No core file
      */
      if (rozofs_max_core_files == 0) {
        unlink(buff);	 
        continue;
      }     

      /* Get file date */ 
      if (stat(buff,&traceStat) < 0) {   
        severe("rozofs_clean_core : stat(%s) %s",buff,strerror(errno));
        unlink(buff);	           
      }
      
      /* Maximum number of file not yet reached. Just register this one */
      if (nb < rozofs_max_core_files) {
        directories[nb].ctime = traceStat.st_ctime;
        snprintf(directories[nb].name, 256, "%s", buff);
        nb ++;
        continue;
      }

      /* Maximum number of file is reached. Remove the older */     

      /* Find younger in already registered list */ 
      younger = 0;
      for (idx=1; idx < rozofs_max_core_files; idx ++) {
        if (directories[idx].ctime > directories[younger].ctime) younger = idx;
      }

      /* 
      ** If younger in list is younger than the last one read, 
      ** the last one read replaces the younger in the array and the older is removed
      */
      if (directories[younger].ctime > (uint32_t)traceStat.st_ctime) {
        unlink(directories[younger].name);	
        directories[younger].ctime = traceStat.st_ctime;
        strcpy(directories[younger].name, buff);
        continue;
      }
      /*
      ** Else the last read is removed 
      */
      unlink(buff);
    }
    closedir(subdir);  
  }
  closedir(dir);
}
/*__________________________________________________________________________
  Let's die
  ==========================================================================
  PARAMETERS: 
  - sig : the signal received 
  RETURN: none
  ==========================================================================*/
void rozofs_die(int sig){
    
  /* 
  ** Set current directory to the core directory 
  */
  if (rozofs_core_file_path[0]) {
    if (chdir(rozofs_core_file_path) == -1){}
  }
  
  /*
  ** Reset default handler and re-raise the signal
  */
  signal (sig,SIG_DFL);
  raise (sig);
}  
/*__________________________________________________________________________
  generic handler for fatal errors
  ==========================================================================
  PARAMETERS: 
  - sig : the signal received 
  RETURN: none
  ==========================================================================*/
void rozofs_catch_error(int sig){
  int   idx;
  pid_t pid = getpid();
  char * sigString = rozofs_signal(sig);  
  /*
  ** Already processing a signal.
  ** We may have crashed within execution of a crash call back !
  ** Let's die immediatly
  */
  if  (rozofs_fatal_error_processing != 0) {

    /*
    ** Ignore SIGTERM now
    */
    if (sig == SIGTERM) return;
    
    /*
    ** Just crash on any other call back
    */
    rozofs_die(rozofs_fatal_error_processing);
  }  
  rozofs_fatal_error_processing = sig;
  
  /*
  ** Session leaders must kill the whole session
  */
  if (pid == getsid(0)) {  
    /*
    ** Request termination to every sub process
    */
    kill(-pid,SIGTERM);  
  }

  /* 
  ** Call the registered crash call backs 
  */
  for (idx = 0; idx <rozofs_crash_cbk_nb; idx++) {
    rozofs_crash_cbk[idx](sig);
  }

  /*
  ** Log signal if not yet in syslog
  */
  if (!rozofs_in_syslog) {
    syslog(LOG_INFO, sigString,NULL); 
  }

  /* 
  ** Adios crual world !
  */
  rozofs_die (sig);
}
/*__________________________________________________________________________
  Set signal handler for fatal errors
  ==========================================================================
  PARAMETERS: 
  - sig     : the signal received 
  RETURN: none
  ==========================================================================*/
void rozofs_set_handler_catch_error(int sig){
  struct sigaction action;

  /* Set up the structure to specify the action. */
  action.sa_handler = rozofs_catch_error;
  sigemptyset (&action.sa_mask);
  action.sa_flags = SA_RESTART;
  sigaction (sig, &action, NULL); 
}
/*__________________________________________________________________________
  generic handler for hangup signal
  ==========================================================================
  PARAMETERS: 
  - sig : the signal received 
  RETURN: none
  ==========================================================================*/
void rozofs_set_handler_hangup(int sig);
void rozofs_catch_hangup(int sig){
  int idx;
 
  /* Write the information in the trace file */
  info("Receive reload signal");

  /* Call the relaod call back */
  for (idx = 0; idx <rozofs_hgup_cbk_nb; idx++) {
    rozofs_hgup_cbk[idx](sig);
  }  
  
  rozofs_set_handler_hangup (sig);
}
/*__________________________________________________________________________
  Set signal handler for hangup signal
  ==========================================================================
  PARAMETERS: 
  - sig     : the signal received 
  RETURN: none
  ==========================================================================*/
void rozofs_set_handler_hangup(int sig){
  struct sigaction action;

  /* Set up the structure to specify the action. */
  action.sa_handler = rozofs_catch_hangup;
  sigemptyset (&action.sa_mask);
  action.sa_flags = SA_RESTART;
  sigaction (sig, &action, NULL); 
}

/*__________________________________________________________________________
  Declare a list of signals and their handler
  ==========================================================================
  @param application       the application name. This will be the directory
                           name where its core files will e saved
  @param max_core_files    maximum number of core files to keep in this
                           directory
  ==========================================================================*/
void rozofs_signals_declare(char * application, int max_core_files) {
  char * pChar;
  
  if (max_core_files > ROZOFS_MAX_CORE) rozofs_max_core_files = ROZOFS_MAX_CORE;
  else                                  rozofs_max_core_files = max_core_files;
  
  rozofs_core_file_path[0] = 0;
  if (application == NULL) return;
 
  pChar = rozofs_core_file_path;
  if ((common_config.core_file_directory==NULL) || (common_config.core_file_directory[0]==0)) {
    pChar += sprintf(pChar,"%s",ROZOFS_RUNDIR_CORE);
  }
  else {
    pChar += sprintf(pChar,"%s",common_config.core_file_directory);
  }  
  rozofs_clean_core(rozofs_core_file_path);

  pChar += sprintf(pChar,"/%s",application);
  rozofs_mkpath(rozofs_core_file_path,0744);  

  uma_dbg_declare_core_dir(rozofs_core_file_path);
  
  /*
  ** Set no limit for the core file size
  */
  struct rlimit core_limit;
  core_limit.rlim_cur = RLIM_INFINITY;
  core_limit.rlim_max = RLIM_INFINITY;   
  setrlimit(RLIMIT_CORE, &core_limit);
 
  
  /* Declare the fatal errors handler */
 
  rozofs_set_handler_catch_error (SIGQUIT);
  rozofs_set_handler_catch_error (SIGILL);
  rozofs_set_handler_catch_error (SIGBUS);
  rozofs_set_handler_catch_error (SIGSEGV);
  rozofs_set_handler_catch_error (SIGFPE);
  rozofs_set_handler_catch_error (SIGSYS);
  rozofs_set_handler_catch_error (SIGXCPU);  
  rozofs_set_handler_catch_error (SIGXFSZ);
  rozofs_set_handler_catch_error (SIGABRT);
  rozofs_set_handler_catch_error (SIGINT);
  rozofs_set_handler_catch_error (SIGTERM);
  
  /* Declare the reload handler */
  
  rozofs_set_handler_hangup (SIGHUP);

  /* Ignored signals */
  
  rozofs_set_handler_signal_ignore(SIGTSTP);
  rozofs_set_handler_signal_ignore(SIGTTOU);
  rozofs_set_handler_signal_ignore(SIGTTIN);
  rozofs_set_handler_signal_ignore(SIGWINCH);

  /* 
   * redirect SIGPIPE signal to avoid the end of the 
   * process when a TCP connexion is down
   */   
  rozofs_set_handler_signal_ignore(SIGPIPE);
  
  /*
  ** SIG child
  */
  rozofs_set_handler_signal_ignore(SIGCHLD);
}
