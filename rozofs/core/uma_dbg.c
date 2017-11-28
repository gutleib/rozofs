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
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <errno.h>
#include <stdarg.h>
#include <unistd.h>
#include <time.h>
#include <ctype.h>
#include <sys/syscall.h>
#include <pthread.h>

#include "ruc_common.h"
#include "ruc_list.h"
#include "ruc_sockCtl_api.h"
#include "uma_tcp_main_api.h"
#include "ruc_tcpServer_api.h"
#include "uma_dbg_api.h"
#include "config.h"
#include "../rozofs_service_ports.h"


char rozofs_github_reference[256];

uint32_t   uma_dbg_initialized=FALSE;
char     * uma_gdb_system_name=NULL;
static time_t uptime=0;

/*
** List of threads
*/
typedef struct _uma_dbg_threads_t {
  pthread_t    tid;
  char       * name;
} uma_dbg_threads_t;

#define UMA_DBG_MAX_THREAD  64
uma_dbg_threads_t uma_dbg_thread_table[UMA_DBG_MAX_THREAD];
int               uma_dbg_thread_table_initialized=0;
pthread_mutex_t   uma_dbg_thread_mutex = PTHREAD_MUTEX_INITIALIZER;



char uma_dbg_core_file_path[128] = {0};

char uma_dbg_syslog_name[128] = {0};

typedef struct uma_dbg_topic_s {
  char                     * name;
  uint16_t                   option;
  uint8_t                    len;  
  uint8_t                    unic;
  uma_dbg_topic_function_t   funct;
  uma_dbg_manual_function_t  man;  
} UMA_DBG_TOPIC_S;

#define UMA_DBG_MAX_TOPIC 128
UMA_DBG_TOPIC_S uma_dbg_topic[UMA_DBG_MAX_TOPIC] = {};
uint32_t        uma_dbg_nb_topic = 0;
uint32_t          uma_dbg_topic_initialized=FALSE;

#define            MAX_ARG   64

uma_dbg_catcher_function_t	uma_dbg_catcher = uma_dbg_catcher_DFT;

typedef struct uma_dbg_session_s {
  ruc_obj_desc_t            link;
  void 		            * ref;
  uint32_t                    ipAddr;
  uint16_t                    port;
  uint32_t                    tcpCnxRef;
//64BITS  uint32_t                    recvPool;
  void                      *recvPool;
  char                     *argv[MAX_ARG];
  char                      argvBuffer[2050];
  char                      last_valid_command[2000];
} UMA_DBG_SESSION_S;

UMA_DBG_SESSION_S *uma_dbg_freeList = (UMA_DBG_SESSION_S*)NULL;
UMA_DBG_SESSION_S *uma_dbg_activeList = (UMA_DBG_SESSION_S*)NULL;

#define UMA_DBG_MAX_CMD_LEN (1024*4)
char rcvCmdBuffer[UMA_DBG_MAX_CMD_LEN+1];

char uma_dbg_temporary_buffer[UMA_DBG_MAX_SEND_SIZE];

void uma_dbg_listTopic(uint32_t tcpCnxRef, void *bufRef, char * topic);
uint32_t uma_dbg_do_not_send = 0;
int      old;
/*__________________________________________________________________________
 */
/**
*  Format an ASCII dump
* @param mem   memory area to dump
* @param len   size to dump mem on
* @param p     where to output the dump
*
*  return the address of the end of the dump 
*/
/*__________________________________________________________________________
 */ 
#define HEXDUMP_COLS 16
char * uma_dbg_hexdump(void *ptr, unsigned int len, char * p)
{
        unsigned int i, j;
	char * mem =(char *) ptr;
        
        for(i = 0; i < len + ((len % HEXDUMP_COLS) ? (HEXDUMP_COLS - len % HEXDUMP_COLS) : 0); i++)
        {
                /* print offset */
                if(i % HEXDUMP_COLS == 0)
                {
			p += rozofs_x64_append(p,(uint64_t)(mem+i));			
                        *p++ = ' ';
                }
		
                /* print hex data */
                if(i < len)
                {
			p += rozofs_x8_append(p, 0xFF & ((char*)mem)[i]);			
		       *p++ = ' ';
                }
                else /* end of block, just aligning for ASCII dump */
                {
                        *p++ = ' '; *p++ = ' ';*p++ = ' ';
                }
                
                /* print ASCII dump */
                if(i % HEXDUMP_COLS == (HEXDUMP_COLS - 1))
                {
                        for(j = i - (HEXDUMP_COLS - 1); j <= i; j++)
                        {
                                if(j >= len) /* end of block, not really printing */
                                {
                                        *p++ = ' ';
                                }
                                else if(isprint(((char*)mem)[j])) /* printable char */
                                {
				        *p++ = 0xFF & ((char*)mem)[j];  
                                }
                                else /* other char */
                                {
                                        *p++ = '.';
                                }
                        }
                        *p++ = '\n';
                }
        }
	*p = 0;
	return p;
}

/*__________________________________________________________________________
*  Add a thread in the thread table
** @param tid    The thread identifier
** @param name   The function of the tread
*/
void uma_dbg_thread_add_self(char * name) {
  int  i;
  int  tid = syscall(SYS_gettid);
  
  uma_dbg_threads_t * th = &uma_dbg_thread_table[0];

  pthread_mutex_lock(&uma_dbg_thread_mutex);
  
  if (uma_dbg_thread_table_initialized==0) {
    memset(uma_dbg_thread_table,0,sizeof(uma_dbg_thread_table));
    uma_dbg_thread_table_initialized = 1;
  }
  
  for (i=0; i < UMA_DBG_MAX_THREAD; i++,th++) {
    if (th->tid == 0) {
      th->tid  = tid;
      break;
    }  
  }  
  
  pthread_mutex_unlock(&uma_dbg_thread_mutex);

  if (i != UMA_DBG_MAX_THREAD) {
    th->name = strdup(name);
  }  
}
/*__________________________________________________________________________
*  Remove a thread from the thread table
*/
void uma_dbg_thread_remove_self(void) {
  int  i;
  int  tid = syscall(SYS_gettid);
  
  uma_dbg_threads_t * th = &uma_dbg_thread_table[0];

  pthread_mutex_lock(&uma_dbg_thread_mutex);
  
  for (i=0; i < UMA_DBG_MAX_THREAD; i++,th++) {
    if (th->tid == tid) {
      th->tid = 0;
      if (th->name) {
        free(th->name);
        th->name = NULL;
      }	
      break;
    }  
  }  
  pthread_mutex_unlock(&uma_dbg_thread_mutex);
}
/*__________________________________________________________________________
**  Get the thread name of a thread
** @param tid    The thread identifier
** @retval the name of the thread or NULL
*/
char * uma_dbg_thread_get_name(pthread_t tid){
  int i;
  
  for (i=0; i< UMA_DBG_MAX_THREAD; i++) {
    if (tid == uma_dbg_thread_table[i].tid) return uma_dbg_thread_table[i].name;
  }
  return NULL;
}  
/*__________________________________________________________________________
**
**  Display whether some syslog exists
*/
#define UMA_DBG_DEFAULT_SYSLOG_LINES 40
void show_uma_dbg_syslog_man(char * pChar) {
  pChar += rozofs_string_append           (pChar,"Check for syslog of this module family of a given severity in\n");
  pChar += rozofs_string_append_bold      (pChar,"/var/log/syslog");
  pChar += rozofs_string_append           (pChar," or ");
  pChar += rozofs_string_append_bold      (pChar,"/var/log/messages.\n"); 
  pChar += rozofs_string_append_underscore(pChar,"\nUsage:\n");   
  pChar += rozofs_string_append_bold      (pChar,"\tsyslog [fatal|severe|warning|info] [nblines]\n");
  pChar += rozofs_string_append_bold      (pChar,"\t\tfatal");
  pChar += rozofs_string_append           (pChar,"\tdisplays only fatal logs.\n");
  pChar += rozofs_string_append_bold      (pChar,"\t\tsevere");
  pChar += rozofs_string_append           (pChar,"\tdisplays fatal & severe logs.\n");
  pChar += rozofs_string_append_bold      (pChar,"\t\twarning");
  pChar += rozofs_string_append           (pChar,"\tdisplays fatal & severe & warning logs.\n");
  pChar += rozofs_string_append_bold      (pChar,"\t\tinfo");
  pChar += rozofs_string_append           (pChar,"\tdisplays all logs.\n");
  pChar += rozofs_string_append           (pChar,"\t\tDefault severity is ");
  pChar += rozofs_string_append_bold      (pChar,"severe.\n\n");
  pChar += rozofs_string_append_bold      (pChar,"\t\tnbLines");
  pChar += rozofs_string_append           (pChar," is the number of line to be displayed (default is ");
  pChar += rozofs_u32_append              (pChar,  UMA_DBG_DEFAULT_SYSLOG_LINES);  
  pChar += rozofs_string_append           (pChar,").\n");
   
}
/*__________________________________________________________________________
**
**  Scan argument looking for number of lines
*/
int show_uma_dbg_syslog_scan_lines(char * argv) {
  uint32_t  lines=UMA_DBG_DEFAULT_SYSLOG_LINES;
  /*
  ** When no argument get default value
  */  
  if (argv == NULL) return UMA_DBG_DEFAULT_SYSLOG_LINES;
  
  sscanf(argv,"%u",&lines);
  /* Limit the number of lines to 100 */
  if (lines>100) lines = 100;
  return lines;
}
/*__________________________________________________________________________
**
**  Syslog diagnostic
*/  
void show_uma_dbg_syslog(char * argv[], uint32_t tcpRef, void *bufRef) {
  int       len;
  char      *p = uma_dbg_get_buffer();
  uint32_t  lines=UMA_DBG_DEFAULT_SYSLOG_LINES;

  p += rozofs_string_append(p,"grep \'");
  p += rozofs_string_append(p,uma_dbg_syslog_name);

  /* 
  ** Check the requested level given as 1rst argument
  */  
  if (argv[1] == NULL) {
    p += rozofs_string_append(p,"\\[[0-9]*\\]: .*\\(fatal\\|severe\\):\' ");
  }
  else {
    if ((strcmp(argv[1],"info")) == 0) {
      p += rozofs_string_append(p,"\\[[0-9]*\\]: .*\\(fatal\\|severe\\|warning\\|info\\):\' ");
    }
    else if ((strcmp(argv[1],"warning")) == 0) {
      p += rozofs_string_append(p,"\\[[0-9]*\\]: .*\\(fatal\\|severe\\|warning\\):\' ");
    }  \
    else if ((strcmp(argv[1],"severe")) == 0) {
      p += rozofs_string_append(p,"\\[[0-9]*\\]: .*\\(fatal\\|severe\\):\' ");
    }
    else if ((strcmp(argv[1],"fatal")) == 0) {
      p += rozofs_string_append(p,"\\[[0-9]*\\]: .*fatal:\' ");
    }
    else {
      /*
      ** Default is severe. argv[1] shoudl be a number of line
      */
      p += rozofs_string_append(p,"\\[[0-9]*\\]: .*\\(fatal\\|severe\\):\' ");
      lines = show_uma_dbg_syslog_scan_lines(argv[1]);      
    } 
    
    /*
    ** When argv[2] is set, it should be the number of lines to display 
    */  
    if (argv[2] != NULL) {
      lines = show_uma_dbg_syslog_scan_lines(argv[2]);
    }
  }  
  
  if (access("/var/log/syslog",R_OK)==0) {
    p += rozofs_string_append(p,"/var/log/syslog | tail -");
  }
  else if (access("/var/log/messages",R_OK)==0) {
    p += rozofs_string_append(p,"/var/log/messages | tail -");
  }
  else  {
    rozofs_string_append(uma_dbg_get_buffer(),"No log file /var/log/syslog or /var/log/messages\n");
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
    return; 
  }
  
  p += rozofs_u32_append(p,lines);

  len = uma_dbg_run_system_cmd(uma_dbg_get_buffer(), uma_dbg_get_buffer(), uma_dbg_get_buffer_len());
  if (len == 0)  uma_dbg_send(tcpRef, bufRef, TRUE, "No such log\n");    
  else           uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
  return ;
}
/*__________________________________________________________________________
*  Record syslog name
*
* @param name The syslog name
*/
void uma_dbg_record_syslog_name(char * name) {
  strcpy(uma_dbg_syslog_name,name);
  openlog(uma_dbg_syslog_name, LOG_PID, LOG_DAEMON);
  uma_dbg_addTopicAndMan("syslog",show_uma_dbg_syslog, show_uma_dbg_syslog_man, 0);  
}
/*__________________________________________________________________________
 */
/**
*  Display whether some core files exist
*/
void show_uma_dbg_core_files_man(char * pChar) {
  pChar += rozofs_string_append           (pChar,"Check for the presence of core files under ");
  pChar += rozofs_string_append_bold      (pChar,uma_dbg_core_file_path);
  pChar += rozofs_string_append           (pChar,"\n");

}
void show_uma_dbg_core_files(char * argv[], uint32_t tcpRef, void *bufRef) {
  int    len;
  char  *p;
  
  p = uma_dbg_get_buffer();
  p += rozofs_string_append(p,"ls -1 -h --full-time ");
  p += rozofs_string_append(p,uma_dbg_core_file_path);
  p += rozofs_string_append(p,"/* 2>/dev/null");

  len = uma_dbg_run_system_cmd(uma_dbg_get_buffer(), uma_dbg_get_buffer(), uma_dbg_get_buffer_len());
  if (len == 0)  uma_dbg_send(tcpRef, bufRef, TRUE, "None\n");    
  else           uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
  return ;
}
/*__________________________________________________________________________
 */
/**
*  Declare the path where to serach for core files
*/
void uma_dbg_declare_core_dir(char * path) {
  strcpy(uma_dbg_core_file_path,path);
  uma_dbg_addTopicAndMan("core", show_uma_dbg_core_files, show_uma_dbg_core_files_man,0);  
}
/*__________________________________________________________________________
 */
/**
*  Run a system command and return the result 
*/
int uma_dbg_run_system_cmd(char * cmd, char *result, int len) {
  pid_t  pid;
  char   fileName[32];
  int    fd;
  int ret = -1;
  char * p;
  
  pid = getpid();
  p = fileName;
  p += rozofs_string_append(p,"/tmp/rozo.");
  p += rozofs_u32_append(p,pid);
  
  p = cmd;
  p += strlen(cmd);
  *p++ = ' '; *p++ = '>'; *p++ = ' ';
  p += rozofs_string_append(p,fileName);
  
  ret = system(cmd);
  if (-1 == ret) {
    DEBUG("%s returns -1",cmd);
  }
  
  fd = open(fileName, O_RDONLY);
  if (fd < 0) {
    unlink(fileName);
    return 0;    
  }
  
  len = read(fd,result, len-1);
  result[len] = 0;
  
  close(fd);
  unlink(fileName);  
  return len;
} 

/*__________________________________________________________________________
 */
/**
*  Display the system name if any has been set thanks to uma_dbg_set_name()
*/
void uma_dbg_system_cmd(char * argv[], uint32_t tcpRef, void *bufRef) {
  char * cmd;
  int    len;

  if(argv[1] == NULL) {  
    uma_dbg_listTopic(tcpRef, bufRef, NULL);
    return; 
  }
  
  cmd = rcvCmdBuffer;
  while (*cmd != 's') cmd++;
  cmd += 7;

  len = uma_dbg_run_system_cmd(cmd, uma_dbg_get_buffer(), uma_dbg_get_buffer_len());
  if (len == 0)  uma_dbg_send(tcpRef, bufRef, TRUE, "No response\n");    
  else           uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
  return ;
} 
/*__________________________________________________________________________
 */
/**
*  Display the system name if any has been set thanks to uma_dbg_set_name()
*/
void uma_dbg_system_ps_man(char * pChar) {
  pChar += rozofs_string_append           (pChar,"Display the list of threads of this process.\n");
  pChar += rozofs_string_append           (pChar,"For each thread is displayed\n");
  pChar += rozofs_string_append           (pChar,"- its name when one has been given,\n");
  pChar += rozofs_string_append           (pChar,"- its core number,\n");
  pChar += rozofs_string_append           (pChar,"- its \% of CPU usage,\n");
  pChar += rozofs_string_append           (pChar,"- its total CPU consumption.\n");

}
void uma_dbg_system_ps(char * argv[], uint32_t tcpRef, void *bufRef) {
  int    len;
  pid_t  pid;
  char  *p;
  int   tid;
  
  pid = getpid();
  
  p = uma_dbg_get_buffer();
  p += rozofs_string_append(p,"ps -p ");
  p += rozofs_u32_append(p, pid);
  p += rozofs_string_append(p," -m -olwp:20,psr,pcpu,cputime,pmem,vsz,args");

  len = uma_dbg_run_system_cmd(uma_dbg_get_buffer(), uma_dbg_get_buffer(), uma_dbg_get_buffer_len());
  
  if (len == 0)  return uma_dbg_send(tcpRef, bufRef, TRUE, "No response\n");  
    
  /*
  ** Add thread identifier list 
  */
  p = uma_dbg_get_buffer();
  // Title line
  while((*p!=0)&&(*p!='\n')) p++;
  if (*p==0) goto out;
  p++;

  // General line
  while((*p!=0)&&(*p!='\n')) p++;  
  if (*p==0) goto out;
  p++;
    
  // scan for tid
  while (*p!=0) {
  
    int ret = sscanf(p,"%d",&tid);
    if (ret == 1) {
    
      char * name = uma_dbg_thread_get_name(tid);
      if (name != NULL) {
        len = strlen(name);
	memcpy(p,name,len);
      }   
    }
    while((*p!=0)&&(*p!='\n')) p++;  
    if (*p==0) break;
    p++;
  }      

out:  
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
  return ;
} 
/*__________________________________________________________________________
 */
/**
*  Display the system name if any has been set thanks to uma_dbg_set_name()
*/
void uma_dbg_show_uptime_man(char * pChar) {
  pChar += rozofs_string_append           (pChar,"Display the elapsed time since this module start up.\n");
}
void uma_dbg_show_uptime(char * argv[], uint32_t tcpRef, void *bufRef) {
    time_t elapse;
    int days, hours, mins, secs;
    char   * pChar;

    // Compute uptime for storaged process
    elapse = (int) (time(0) - uptime);
    days = (int) (elapse / 86400);
    hours = (int) ((elapse / 3600) - (days * 24));
    mins = (int) ((elapse / 60) - (days * 1440) - (hours * 60));
    secs = (int) (elapse % 60);
    
    pChar = uma_dbg_get_buffer();
    pChar += rozofs_string_append(pChar,"uptime = ");
    pChar += rozofs_u32_append(pChar,days);
    pChar += rozofs_string_append(pChar," days, ");
    pChar += rozofs_u32_padded_append(pChar,2,rozofs_zero,hours);
    *pChar++ = ':';
    pChar += rozofs_u32_padded_append(pChar,2,rozofs_zero,mins);
    *pChar++ = ':';
    pChar += rozofs_u32_padded_append(pChar,2,rozofs_zero,secs);
    pChar += rozofs_eol(pChar);
        
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}
/*__________________________________________________________________________
 */
/**
*  Display the ports that should be reserved
*/
void uma_dbg_reserved_ports_man(char * pt) {
  show_ip_local_reserved_ports_man(pt);
}
void uma_dbg_reserved_ports(char * argv[], uint32_t tcpRef, void *bufRef) {
  char * pt = uma_dbg_get_buffer();
  char cmd[512];
        
  pt += show_ip_local_reserved_ports(pt);

  *pt++ = '\n';
  strcpy(cmd,"grep ip_local_reserved_ports /etc/sysctl.conf");
  pt += rozofs_string_append(pt,cmd);
  *pt++ = '\n';      
  pt += uma_dbg_run_system_cmd(cmd, pt, 1024);
  *pt++ = '\n';      
  
  strcpy(cmd,"cat /proc/sys/net/ipv4/ip_local_reserved_ports");
  pt += rozofs_string_append(pt,cmd);
  *pt++ = '\n';   
  pt += uma_dbg_run_system_cmd(cmd, pt, 1024);
  pt += rozofs_eol(pt); 
  
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}
/*__________________________________________________________________________
 */
/**
*  Retrieve a topic context from its name or a part of its name
*/
int uma_dbg_retrieve_topic(char * topic) {
  int              length;
  int              topicNum;
  int              found=0;
  int              idx=-1;
  
  /* Search exact match in the topic list the one requested */
  if (topic==NULL) return -1;
  length = strlen(topic);
  if (length==0) return -1;  
  
  for (topicNum=0; topicNum <uma_dbg_nb_topic; topicNum++) {
    if (uma_dbg_topic[topicNum].len == length) {        
      int order = strcasecmp(topic,uma_dbg_topic[topicNum].name);
      if (order == 0) return topicNum;
      if (order < 0) break;  
    }
  }

  /* Search match on first characters */
  for (topicNum=0; topicNum <uma_dbg_nb_topic; topicNum++) {
    if (uma_dbg_topic[topicNum].option & UMA_DBG_OPTION_HIDE) continue;
    if (uma_dbg_topic[topicNum].len > length) {
      int order = strncasecmp(topic,uma_dbg_topic[topicNum].name, length);
      if (order < 0) break; 	
	  if (order == 0) {
		found++;
		idx = topicNum;
		 /* Several matches Display possibilities */
		if (found > 1) return -2;
	  }  
    }
  }
  
  if (found) return idx;
    
  return -1;    		
}	 
/*__________________________________________________________________________
 */
/**
*  Display the system name if any has been set thanks to uma_dbg_set_name()
*/
void uma_dbg_manual_man(char * pChar) {  
  pChar += rozofs_string_append           (pChar,"Used to display rozodiag commands manual when one has been written.\n");
}
void uma_dbg_manual(char * argv[], uint32_t tcpRef, void *bufRef) {  
  char * pt = uma_dbg_get_buffer();
  int    idx;

  if (argv[1] == NULL) {
    pt += rozofs_string_append(pt, "Missing topic name\n");
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
    return;    
  }

  idx = uma_dbg_retrieve_topic(argv[1]);
  if (idx == -1) {
    pt += rozofs_string_append(pt, "Unknown topic\n");
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
    return;    

  }  
  if (idx == -2) {
    uma_dbg_listTopic(tcpRef, bufRef, argv[1]); 
    return;
	
  }  
  
  if (uma_dbg_topic[idx].man == NULL) {   
    pt += rozofs_string_append(pt, "No manual for ");
    pt += rozofs_string_append(pt, uma_dbg_topic[idx].name);
    pt += rozofs_string_append(pt, "\n");
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	return;
  }	
  
  pt += rozofs_string_append_underscore(pt,"\nmanual : ");
  pt += rozofs_string_append_underscore(pt,uma_dbg_topic[idx].name);
  pt += rozofs_string_append(pt,"\n\n");
  uma_dbg_topic[idx].man(pt);
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}
/*__________________________________________________________________________
 */
/**
*  Display the system name if any has been set thanks to uma_dbg_set_name()
*/
void uma_dbg_show_name_man(char * pChar) {  
  pChar += rozofs_string_append           (pChar,"Display the target system identifier.\n");    
}
void uma_dbg_show_name(char * argv[], uint32_t tcpRef, void *bufRef) {  
  char * pt = uma_dbg_get_buffer();
      
  if (uma_gdb_system_name == NULL) {
    uma_dbg_send(tcpRef, bufRef, TRUE, "system : NO NAME\n");
  }  
  else {  
    pt += rozofs_string_append(pt,"system : ");
    pt += rozofs_string_append(pt,uma_gdb_system_name);
    pt += rozofs_eol(pt);
    
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
  }
}
/*__________________________________________________________________________
 */
/**
*  Display the version of the library 
*/
void uma_dbg_show_version_man(char * pChar) {  
  pChar += rozofs_string_append           (pChar,"Display the software release version.\n");
}  
void uma_dbg_show_version(char * argv[], uint32_t tcpRef, void *bufRef) {  
  char * pt = uma_dbg_get_buffer();
  pt += rozofs_string_append(pt,"version : ");
  pt += rozofs_string_append(pt,VERSION);
  pt += rozofs_eol(pt);
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}
/**
*  Display the version of the library 
*/
void uma_dbg_show_git_ref_man(char * pChar) {  
  pChar += rozofs_string_append           (pChar,"Display the module complete build reference including the git tag.\n");
} 
void uma_dbg_show_git_ref(char * argv[], uint32_t tcpRef, void *bufRef) {  
  char * pt = uma_dbg_get_buffer();
  pt += rozofs_string_append(pt,"git : ");
  pt += rozofs_string_append(pt,rozofs_github_reference);
  pt += rozofs_eol(pt);
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}
/*__________________________________________________________________________
 */
/**
*  Reset every resetable command
*/
void uma_dbg_counters_reset_man(char * pChar) {
  int topicNum;
  UMA_DBG_TOPIC_S *   p = uma_dbg_topic;
  
  pChar += rozofs_string_append           (pChar,"CLI used to reset a bunch of counters in a run.\n");
  pChar += rozofs_string_append           (pChar,"Running ");
  pChar += rozofs_string_append_bold      (pChar,"counter reset");
  pChar += rozofs_string_append           (pChar," is equivallent to run:\n");
  
  for (topicNum=0; topicNum <uma_dbg_nb_topic; topicNum++,p++) {
    if (p->option & UMA_DBG_OPTION_RESET) {
      pChar += rozofs_string_append_bold  (pChar," - ");
      pChar += rozofs_string_append_bold  (pChar,p->name);
      pChar += rozofs_string_append_bold  (pChar," reset\n");
    }  
  }
}
void uma_dbg_counters_reset(char * argv[], uint32_t tcpRef, void *bufRef) {
  int topicNum;
  UMA_DBG_TOPIC_S * p;
  char mybuffer[1024];
  char *pChar = mybuffer;
  
  if ((argv[1] == NULL)||(strcasecmp(argv[1],"reset")!=0)) {  
    uma_dbg_send(tcpRef, bufRef, TRUE, "counters requires \"reset\" as parameter\n");
    return; 
  }
  
  /*
  ** To prevent called function to send back a response
  */ 
  uma_dbg_do_not_send = 1;
  
  p = uma_dbg_topic;
  for (topicNum=0; topicNum <uma_dbg_nb_topic; topicNum++,p++) {
    if (p->option & UMA_DBG_OPTION_RESET) {
      pChar += rozofs_string_append(pChar,p->name);
      pChar += rozofs_string_append(pChar," reset\n");
      p->funct(argv,tcpRef,bufRef);
    }
  }  

  uma_dbg_do_not_send = 0;

  uma_dbg_send(tcpRef, bufRef, TRUE, mybuffer);
} 
/*-----------------------------------------------------------------------------
**
**  #SYNOPSIS
**   Default catcher function
**
**  IN:
**   OUT :
**
**----------------------------------------------------------------------------
*/
//64BITS uint32_t uma_dbg_catcher_DFT(uint32 tcpRef, uint32 bufRef)
uint32_t uma_dbg_catcher_DFT(uint32_t tcpRef, void *bufRef)
{
	return FALSE;
}

/*-----------------------------------------------------------------------------
**
**  #SYNOPSIS
**   Change the catcher function
**
**  IN:
**   OUT :
**
**----------------------------------------------------------------------------
*/
void uma_dbg_setCatcher(uma_dbg_catcher_function_t funct)
{
	uma_dbg_catcher = funct;
}

/*-----------------------------------------------------------------------------
**
**  #SYNOPSIS
**   Send a message
**
**  IN:
**   OUT :
**
**----------------------------------------------------------------------------
*/
void uma_dbg_send_format(uint32_t tcpCnxRef, void  *bufRef, uint8_t end, char *fmt, ... ) {
  va_list         vaList;
  UMA_MSGHEADER_S *pHead;
  char            *pChar;
  uint32_t           len;

  /* 
  ** May be in a specific process such as counter reset
  ** and so do not send any thing
  */
  if (uma_dbg_do_not_send) return;
  
  /* Retrieve the buffer payload */
  if ((pHead = (UMA_MSGHEADER_S *)ruc_buf_getPayload(bufRef)) == NULL) {
    severe( "ruc_buf_getPayload(%p)", bufRef );
    /* Let's tell the caller fsm that the message is sent */
    return;
  }
  pChar = (char*) (pHead+1);
  
  pChar += rozofs_string_append(pChar,"____[");
  pChar += rozofs_string_append(pChar,uma_gdb_system_name);
  pChar += rozofs_string_append(pChar,"]__[");  
  pChar += rozofs_string_append(pChar,rcvCmdBuffer);
  pChar += rozofs_string_append(pChar,"]____\n");  
  
  len = pChar - (char*)pHead;

  /* Format the string */
  va_start(vaList,fmt);
  len += vsprintf(pChar, fmt, vaList)+1;
  va_end(vaList);

  if (len > UMA_DBG_MAX_SEND_SIZE)
  {
    severe("debug response exceeds buffer length %u/%u",len,(int)UMA_DBG_MAX_SEND_SIZE);
  }

  pHead->len = htonl(len-sizeof(UMA_MSGHEADER_S));
  pHead->end = end;

  ruc_buf_setPayloadLen(bufRef,len);
  uma_tcp_sendSocket(tcpCnxRef,bufRef,0);
}

/*-----------------------------------------------------------------------------
**
**  #SYNOPSIS
**   Free a debug session
**
**  IN:
**   OUT :
**
**----------------------------------------------------------------------------
*/
void uma_dbg_free(UMA_DBG_SESSION_S * pObj) {
  /* Free the TCP connection context */
  if (pObj->tcpCnxRef != -1) uma_tcp_deleteReq(pObj->tcpCnxRef);
  /* remove the context from the active list */
  ruc_objRemove((ruc_obj_desc_t*)pObj);
  /* Set the context in the free list */
  ruc_objInsertTail((ruc_obj_desc_t*)uma_dbg_freeList,(ruc_obj_desc_t*)pObj);
}
/*-----------------------------------------------------------------------------
**
**  #SYNOPSIS
**   Find a debug session context from its IP address and TCP port
**
**  IN:
**       - IP addr : searched logical IP address
**       - TCP port
**   OUT :
**      !=-1 : object reference
**     == -1 error
**
**----------------------------------------------------------------------------
*/

UMA_DBG_SESSION_S *uma_dbg_findFromAddrAndPort(uint32_t ipAddr, uint16_t port) {
  ruc_obj_desc_t    * pnext;
  UMA_DBG_SESSION_S * p;

  pnext = (ruc_obj_desc_t*)NULL;
  while ((p = (UMA_DBG_SESSION_S*)ruc_objGetNext(&uma_dbg_activeList->link, &pnext))
	 !=(UMA_DBG_SESSION_S*)NULL) {

    if ((p->ipAddr == ipAddr) && (p->port == port)) return p;

  }
  /* not found */
  return (UMA_DBG_SESSION_S *) NULL;
}
/*-----------------------------------------------------------------------------
**
**  #SYNOPSIS
**   Find a debug session context from its IP address and TCP port
**
**  IN:
**       - IP addr : searched logical IP address
**       - TCP port
**   OUT :
**      !=-1 : object reference
**     == -1 error
**
**----------------------------------------------------------------------------
*/

UMA_DBG_SESSION_S *uma_dbg_findFromRef(void * ref) {
  ruc_obj_desc_t    * pnext;
  UMA_DBG_SESSION_S * p;

  pnext = (ruc_obj_desc_t*)NULL;
  while ((p = (UMA_DBG_SESSION_S*)ruc_objGetNext(&uma_dbg_activeList->link, &pnext))
	 !=(UMA_DBG_SESSION_S*)NULL) {

    if (p->ref == ref) return p;

  }
  /* not found */
  return (UMA_DBG_SESSION_S *) NULL;
}
/*-----------------------------------------------------------------------------
**
**  #SYNOPSIS
**   Find a debug session context from its connection ref
**
**  IN:
**       - ref : TCP connection reference
**       - TCP port
**   OUT :
**      !=-1 : object reference
**     == -1 error
**
**----------------------------------------------------------------------------
*/

UMA_DBG_SESSION_S *uma_dbg_findFromCnxRef(uint32_t ref) {
  ruc_obj_desc_t    * pnext;
  UMA_DBG_SESSION_S * p;

  pnext = (ruc_obj_desc_t*)NULL;
  while ((p = (UMA_DBG_SESSION_S*)ruc_objGetNext(&uma_dbg_activeList->link, &pnext))
	 !=(UMA_DBG_SESSION_S*)NULL) {

    if (p->tcpCnxRef == ref) return p;

  }
  /* not found */
  return (UMA_DBG_SESSION_S *) NULL;
}
/*
**--------------------------------------------------------------------------
**  #SYNOPSIS
**  called by any SWBB that wants to add a topic on the debug interface

**   IN:
**       topic : a string representing the topic
**       allBack : the function to be called when a request comes in
**                 for this topic
**   OUT : none
**
**
**--------------------------------------------------------------------------
*/
void uma_dbg_insert_topic(int idx, char * topic, uint16_t option, uint16_t length, uma_dbg_topic_function_t funct, uma_dbg_manual_function_t man) {
  /* Register the topic */
  uma_dbg_topic[idx].name         = topic;
  uma_dbg_topic[idx].len          = length;
  uma_dbg_topic[idx].unic         = length;
  uma_dbg_topic[idx].funct        = funct;
  uma_dbg_topic[idx].man          = man;
  uma_dbg_topic[idx].option       = option;
}  
/*
**--------------------------------------------------------------------------
**  #SYNOPSIS
**  called by any SWBB that wants to add a topic on the debug interface

**   IN:
**       topic : a string representing the topic
**       allBack : the function to be called when a request comes in
**                 for this topic
**       option : a bit mask of options
**   OUT : none
**
**
**--------------------------------------------------------------------------
*/
int uma_dbg_differentiator(char * st1, char * st2) {
  int res = 1;
    
  while(*st1) {		
    if ((*st1==0) || (*st2==0)) return res;
    if (tolower(*st1) != tolower(*st2)) return res;
	res++;
	st1++;
	st2++;
  }
  return res;
} 
void uma_dbg_addTopicAndMan(char                     * topic, 
                            uma_dbg_topic_function_t   funct,
							uma_dbg_manual_function_t  man,
							uint16_t                   option) {
  int    idx,idx2;
  uint16_t length;
  char * my_topic = NULL;
  int    unic;

  /*
  ** Get the lock for multhread environment
  */
  pthread_mutex_lock(&uma_dbg_thread_mutex);

  if (uma_dbg_topic_initialized == FALSE) {
    /* Reset the topic table */
    for (idx=0; idx <UMA_DBG_MAX_TOPIC; idx++) {
      uma_dbg_topic[idx].len   = 0;
      uma_dbg_topic[idx].name  = NULL;
      uma_dbg_topic[idx].funct = NULL;
      uma_dbg_topic[idx].man   = NULL;	  
    }
    uma_dbg_topic_initialized = TRUE;
    uma_dbg_nb_topic = 0;
  }

  /* Get the size of the topic */
  length = strlen(topic);
  if (length == 0) {
    severe( "Bad topic length %d", length );
    goto out;
  }
  
  /* Check a place is left */
  if (uma_dbg_nb_topic == UMA_DBG_MAX_TOPIC) {
    severe( "Too much topic %d. Can not insert %s", UMA_DBG_MAX_TOPIC, topic );
    goto out;  
  }

  /* copy the topic */
  my_topic = malloc(length + 1) ;
  if (my_topic == NULL) {
    severe( "Out of memory. Can not insert %s",topic );    
    goto out;
  }
  strcpy(my_topic, topic);

  /* Find a free entry in the topic table */
  for (idx=0; idx <uma_dbg_nb_topic; idx++) {
    int order;   
    
    order = strcasecmp(topic,uma_dbg_topic[idx].name);
    
    /* check the current entry has got a different key word than
       the one we are to add */
    if (order == 0) {
      severe( "Trying to add topic %s that already exist", topic );
      free(my_topic);
      goto out;
    }
    
    /* Insert here */
    if (order < 0) break;
  }
  
  for (idx2 = uma_dbg_nb_topic-1; idx2 >= idx; idx2--) {
     uma_dbg_insert_topic(idx2+1,uma_dbg_topic[idx2].name,uma_dbg_topic[idx2].option,uma_dbg_topic[idx2].len, uma_dbg_topic[idx2].funct, uma_dbg_topic[idx2].man);
  }
  uma_dbg_insert_topic(idx,my_topic,option,length, funct, man);
  uma_dbg_nb_topic++;
  
  if (uma_dbg_nb_topic < 3) goto out; 
      
  idx  = 0; 
  unic = 1;
  while (idx <uma_dbg_nb_topic-1) {
    int next;
	uma_dbg_topic[idx].unic = unic;
	
    next = idx+1;	
	while (uma_dbg_topic[next].option & UMA_DBG_OPTION_HIDE) next++;

    unic = uma_dbg_differentiator(uma_dbg_topic[idx].name,uma_dbg_topic[next].name);
	if (unic>uma_dbg_topic[idx].unic) uma_dbg_topic[idx].unic = unic;
	if (uma_dbg_topic[idx].unic>uma_dbg_topic[idx].len) uma_dbg_topic[idx].unic = uma_dbg_topic[idx].len;
	idx = next;
  }
  uma_dbg_topic[idx].unic = unic;
  
out:
  /*
  ** Release the lock for multhread environment
  */
  pthread_mutex_unlock(&uma_dbg_thread_mutex);  	  

}
/*-----------------------------------------------------------------------------
**
**  #SYNOPSIS
**   Send a message
**
**  IN:
**   OUT :
**
**----------------------------------------------------------------------------
*/
//64BITS void uma_dbg_listTopic(uint32_t tcpCnxRef, uint32 bufRef, char * topic)
int uma_dbg_display_new_topic(int topicNum, char * pt) {
  char *p = pt;
  int             nbc;             

  if (uma_dbg_topic[topicNum].man == NULL) {
    p += rozofs_string_append(p,"   ");
  }	
  else {	    
    p += rozofs_string_append(p,"m  ");
  }	

  for (nbc=0; nbc<uma_dbg_topic[topicNum].unic; nbc++) {
	*p++ = uma_dbg_topic[topicNum].name[nbc];
  }

  if (uma_dbg_topic[topicNum].len>uma_dbg_topic[topicNum].unic) {
	*p++='(';

	for ( ;nbc<uma_dbg_topic[topicNum].len; nbc++) {
	  *p++ = uma_dbg_topic[topicNum].name[nbc];
	}
	*p++=')';
  }	
  p += rozofs_eol(p);
  return p-pt;
}
int uma_dbg_display_old_topic(int topicNum, char * pt) {
  char *p = pt;

  p += rozofs_string_append(p,"   ");
  p += rozofs_string_append(p,uma_dbg_topic[topicNum].name); 
  p += rozofs_eol(p);
  return p-pt;
}
void uma_dbg_listTopic(uint32_t tcpCnxRef, void *bufRef, char * topic) {
  UMA_MSGHEADER_S *pHead;
  char            *p;
  uint32_t           idx,topicNum;
  int             len=0;  

  /* Retrieve the buffer payload */
  if ((pHead = (UMA_MSGHEADER_S *)ruc_buf_getPayload(bufRef)) == NULL) {
    severe( "ruc_buf_getPayload(%p)", bufRef );
    return;
  }

  p = (char*) (pHead+1);
  idx = 0;
  /* Format the string */
  if (topic) {
    idx += rozofs_string_append(&p[idx], "No such topic \"");
    idx += rozofs_string_append(&p[idx],topic);
    idx += rozofs_string_append(&p[idx],"\" !!!\n\n");
    len = strlen(topic);                
  }

  /* Build the list of topic */
  if (len == 0) idx += rozofs_string_append(&p[idx], "List of available topics :\n");
  else {
    idx += rozofs_string_append(&p[idx], "List of ");
    idx += rozofs_string_append(&p[idx], topic);
    idx += rozofs_string_append(&p[idx], "... topics:\n");
  }
  for (topicNum=0; topicNum <uma_dbg_nb_topic; topicNum++) {
  
    if (uma_dbg_topic[topicNum].option & UMA_DBG_OPTION_HIDE) continue;

    if ((len == 0)||(strncasecmp(topic,uma_dbg_topic[topicNum].name, len) == 0)) {
	  if (old) idx += uma_dbg_display_old_topic(topicNum, &p[idx]);
	  else     idx += uma_dbg_display_new_topic(topicNum, &p[idx]);
    }
  }
  if (len == 0) idx += rozofs_string_append(&p[idx],"  exit / quit / q\n");

  idx ++;

  pHead->len = htonl(idx);
  pHead->end = TRUE;

  ruc_buf_setPayloadLen(bufRef,idx + sizeof(UMA_MSGHEADER_S));
  uma_tcp_sendSocket(tcpCnxRef,bufRef,0);
}
/*
**--------------------------------------------------------------------------
**  #SYNOPSIS
**   callback used by the TCP connection receiver FSM
**   when a message has been fully received on the
**   TCP connection.
**
**   When the application has finsihed to process the message, it must
**   release it
**
**   IN:
**       user reference provide at TCP connection creation time
**       reference of the TCP objet on which the buffer has been allocated
**       reference of the buffer that contains the message
**
**   OUT : none
**
**
**--------------------------------------------------------------------------
*/
//64BITS void uma_dbg_receive_CBK(uint32_t userRef,uint32 tcpCnxRef,uint32 bufRef)
void uma_dbg_receive_CBK(void *opaque,uint32_t tcpCnxRef,void *bufRef) {
  int              topicNum;
  char           * pBuf, * pArg;
  uint32_t           argc;
  UMA_MSGHEADER_S *pHead;
  UMA_DBG_SESSION_S * p;
  int                 replay=0;

  /*
  ** clear the received command buffer content
  */
  rcvCmdBuffer[0] = 0;

  /* Retrieve the session context from the referecne */

  if ((p = uma_dbg_findFromRef(opaque)) == NULL) {
    uma_dbg_send(tcpCnxRef,bufRef,TRUE,"Internal error");
    return;
  }

  /* Retrieve the buffer payload */
  if ((pHead = (UMA_MSGHEADER_S *)ruc_buf_getPayload(bufRef)) == NULL) {
    severe( "ruc_buf_getPayload(%p)", bufRef );
    return;
  }

  /*
  ** Call an optional catcher in order to redirect the message.
  ** This must be done after having checked the validity of the buffer,
  ** in order to avoid confusion : a bad bufRef means disconnection.
  */
  if(uma_dbg_catcher(tcpCnxRef, bufRef) )
  {
    return;
  }


  /* Scan the command line */
  argc = 0;
  pBuf = (char*)(pHead+1);
  
  /* Is it a replay request i.e "!!" */
  if ((pBuf[0] =='!') && (pBuf[1] =='!')) {
    /* let's get as input the last saved command */
    pBuf = p->last_valid_command;
    replay = 1;
  }
  
  if (*pBuf == 0) {
    old=1;
    uma_dbg_listTopic(tcpCnxRef, bufRef, NULL);
	return;
  }
  old=0;
  
  /*
  ** save the current received command
  */
  memcpy(rcvCmdBuffer,pBuf,UMA_DBG_MAX_CMD_LEN);
  rcvCmdBuffer[UMA_DBG_MAX_CMD_LEN] = 0;
  pArg = p->argvBuffer;
  while (1) {
    /* Skip blanks */
//  (before FDL)  while ((*pBuf == ' ') || (*pBuf == '\t')) *pBuf++;
  while ((*pBuf == ' ') || (*pBuf == '\t')) pBuf++;
    if (*pBuf == 0) break; /* end of command line */
    p->argv[argc] = pArg;     /* beginning of a parameter */
    argc++;
    /* recopy the parameter */
    while ((*pBuf != ' ') && (*pBuf != '\t') && (*pBuf != 0)) *pArg++ = *pBuf++;
    *pArg++ = 0; /* End the parameter with 0 */
    if (*pBuf == 0) break;/* end of command line */
  }

  /* Set to NULL parameter number not filled in the command line */
  for (topicNum=argc; topicNum < MAX_ARG; topicNum++) p->argv[topicNum] = NULL;

  /* No topic */
  if (argc == 0) {
    uma_dbg_listTopic(tcpCnxRef, bufRef, NULL);
    return;
  }

  /* Search exact match in the topic list the one requested */
  topicNum = uma_dbg_retrieve_topic(p->argv[0]);
  if (topicNum == -1) {
    uma_dbg_listTopic(tcpCnxRef, bufRef, NULL); 
    return;
  }  
  if (topicNum == -2) {
    uma_dbg_listTopic(tcpCnxRef, bufRef, p->argv[0]); 
    return;
  }    
  /* We have found one command */

  /* Save this existing command for later replay */
  if (replay == 0) {
    strcpy(p->last_valid_command,(char*)(pHead+1));
  } 
  
  uma_dbg_topic[topicNum].funct(p->argv,tcpCnxRef,bufRef);
}
/*
**--------------------------------------------------------------------------
**  #SYNOPSIS
**   Read and execute a rozodiag command file if it exist
**
**
**   IN:
**       The command file name to execute
**
**   OUT : none
**
**
**--------------------------------------------------------------------------
*/
void uma_dbg_process_command_file(char * command_file_name) {
  char              * pBuf, * pArg;
  size_t              length;
  uint32_t            argc;
  UMA_MSGHEADER_S   * pHead;
  uint32_t            idx;
  UMA_DBG_SESSION_S * p;
  FILE              * fd = NULL;
  void              * bufRef = NULL;

  uma_dbg_do_not_send = 1;

  /*
  ** Try to open the given command file
  */
  fd = fopen(command_file_name,"r");
  if (fd == NULL) {
    goto out;
  }
  
  /*
  ** Get the first context in the distributor
  */
  p = (UMA_DBG_SESSION_S*)ruc_objGetFirst((ruc_obj_desc_t*)uma_dbg_freeList);
  if (p == (UMA_DBG_SESSION_S*)NULL) {
    severe("Out of context");
    goto out;
  }
  
  /* 
  ** Initialize the session context 
  */
  p->ipAddr    = ntohl(0x7F000001);
  p->port      = 0;
  p->tcpCnxRef = (uint32_t) -1;  
  p->last_valid_command[0] = 0;
  
  /*
  ** Get a buffer
  */
  bufRef = ruc_buf_getBuffer(p->recvPool);
  if (bufRef == NULL) {
    severe("can not allocate buffer");
    goto out;
  }
  
  /*
  ** Get pointer to the payload
  */
  if ((pHead = (UMA_MSGHEADER_S *)ruc_buf_getPayload(bufRef)) == NULL) {
    severe( "ruc_buf_getPayload(%p)", bufRef );
    goto out;
  }  


  while (1){

    /*
    ** Read a command from  the file
    */
    length = ruc_buf_getMaxPayloadLen(bufRef)-sizeof(UMA_MSGHEADER_S *);
    pBuf = (char*)(pHead+1);
    length = getline(&pBuf, &length, fd);
    if (length == -1) goto out;
      
    if (length == 0) continue;
      
    /*
    ** Remove '\n' and set the payload length
    */  
    if (pBuf[length-1] == '\n') {
      length--;
      pBuf[length] = 0;
    } 
    ruc_buf_setPayloadLen(bufRef, length); 

//    info("Line read \"%s\"",pBuf);

    /*
    ** save the current received command
    */
    memcpy(rcvCmdBuffer,pBuf,length+1);


    /* Scan the command line */
    argc = 0;

    pArg = p->argvBuffer;
    argc = 0;
    while (1) {
      /* Skip blanks */
      while ((*pBuf == ' ') || (*pBuf == '\t')) pBuf++; 
      if (*pBuf == 0) break; /* end of command line */
      p->argv[argc] = pArg;     /* beginning of a parameter */
      argc++;
      /* recopy the parameter */
      while ((*pBuf != ' ') && (*pBuf != '\t') && (*pBuf != 0)) *pArg++ = *pBuf++;
      *pArg++ = 0; /* End the parameter with 0 */
      if (*pBuf == 0) break;/* end of command line */
    }

    /* Empty line */
    if (argc == 0) continue;
    
    /* Comment line */
    if (*(p->argv[0]) == '#') continue;

    /* Set to NULL parameter number not filled in the command line */
    for (idx=argc; idx < MAX_ARG; idx++) p->argv[idx] = NULL;


    /* Search exact match in the topic list the one requested */
	idx = uma_dbg_retrieve_topic(p->argv[0]);
    if (idx < 0) {
      severe("No such rozodiag command \"%s\" in %s",p->argv[0],command_file_name);
      continue;
    }
  
    /*
    ** Run the command
    */
    uma_dbg_topic[idx].funct(p->argv,-1,bufRef);
  }
  
  
  
out:
  /*
  ** Reset do not send indicator
  */
  uma_dbg_do_not_send = 0;
  
  /*
  ** Close command file
  */
  if (fd != NULL) {
    fclose(fd);
  } 
  /*
  ** Release buffer
  */
  if (bufRef != NULL) {
    ruc_buf_freeBuffer(bufRef);
  }
}
/*
**-------------------------------------------------------
  void upc_nse_ip_disc_uph_ctl_CBK(uint32_t nsei,uint32 tcpCnxRef)
**-------------------------------------------------------
**  #SYNOPSIS
**   That function allocates all the necessary
**   resources for UPPS TCP connections management
**
**   IN:
**       refObj : reference of the NSE context
**       tcpCnxRef : reference of the tcpConnection
**
**
**   OUT :
**        none
**
**-------------------------------------------------------
*/
//64BITS void uma_dbg_disc_CBK(uint32_t refObj,uint32 tcpCnxRef) {
void uma_dbg_disc_CBK(void *opaque,uint32_t tcpCnxRef) {
  UMA_DBG_SESSION_S * pObj;

  if ((pObj = uma_dbg_findFromRef(opaque)) == NULL) {
    return;
  }

  uma_dbg_catcher(tcpCnxRef, NULL);
  uma_dbg_free(pObj);
}
/*
**-------------------------------------------------------
**  #SYNOPSIS
**   called when a debug session open is requested
**
**   IN:
**
**   OUT :
**
**
**-------------------------------------------------------
*/
uint32_t uma_dbg_accept_CBK(uint32_t userRef,int socketId,struct sockaddr * sockAddr) {
  uint32_t              ipAddr;
  uint16_t              port;
  UMA_DBG_SESSION_S * pObj;
  uma_tcp_create_t    conf;
  struct  sockaddr_in vSckAddr;
  int                 vSckAddrLen=14;
  char                name[32];
  char              * pChar;

  uma_tcp_create_t *pconf = &conf;

  /* Get the source IP address and port */
  if((getpeername(socketId, (struct sockaddr *)&vSckAddr,(socklen_t*) &vSckAddrLen)) == -1){
    return RUC_NOK;
  }
  ipAddr = (uint32_t) ntohl((uint32_t)(/*(struct sockaddr *)*/vSckAddr.sin_addr.s_addr));
  port   = ntohs((uint16_t)(vSckAddr.sin_port));
  
  pChar = name;
  pChar += rozofs_string_append(pChar,"A:DIAG/");
  pChar += rozofs_ipv4_port_append(pChar,ipAddr,port);  

  /* Search for a debug session with this IP address and port */
  if ((pObj = uma_dbg_findFromAddrAndPort(ipAddr,port)) != NULL) {

    /* Session already exist. Just update it */
    if (uma_tcp_updateTcpConnection(pObj->tcpCnxRef,socketId,name) != RUC_OK) {
      uma_dbg_free(pObj);
      return RUC_NOK;
    }
    return RUC_OK;
  }

  /* Find a free debug session context */
  pObj = (UMA_DBG_SESSION_S*)ruc_objGetFirst((ruc_obj_desc_t*)uma_dbg_freeList);
  if (pObj == (UMA_DBG_SESSION_S*)NULL) {
//    INFO8 "No more free debug session" EINFO;
    return RUC_NOK;
  }

  /* Initialize the session context */
  pObj->ipAddr    = ipAddr;
  pObj->port      = port;
  pObj->tcpCnxRef = (uint32_t) -1;
  
  pObj->last_valid_command[0] = 0;

  /* Allocate a TCP connection descriptor */
  pconf->headerSize       = sizeof(UMA_MSGHEADER_S);
  pconf->msgLenOffset     = 0;
  pconf->msgLenSize       = sizeof(uint32_t);
  pconf->bufSize          = 2048*4;
  pconf->userRcvCallBack  = uma_dbg_receive_CBK;
  pconf->userDiscCallBack = uma_dbg_disc_CBK;
  pconf->userRef          =  pObj->ref;
  pconf->socketRef        = socketId;
  pconf->xmitPool         = NULL; /* use the default XMIT pool ref */
  pconf->recvPool         = pObj->recvPool; /* Use a big buffer pool */
  pconf->userRcvReadyCallBack = (ruc_pf_sock_t)NULL ;

  if ((pObj->tcpCnxRef = uma_tcp_create_rcvRdy_bufPool(pconf)) == (uint32_t)-1) {
    severe( "uma_tcp_create" );
    return RUC_NOK;
  }

  /* Tune the socket and connect with the socket controller */
  if (uma_tcp_createTcpConnection(pObj->tcpCnxRef,name) != RUC_OK) {
    severe( "uma_tcp_createTcpConnection" );
    uma_dbg_free(pObj);
    return RUC_NOK;
  }

  /* Remove the debud session context from the free list */
  ruc_objRemove((ruc_obj_desc_t*)pObj);
  /* Set the context in the active list */
  ruc_objInsertTail((ruc_obj_desc_t*)uma_dbg_activeList,(ruc_obj_desc_t*)pObj);

  return RUC_OK;
}
/*
**-------------------------------------------------------
**  #SYNOPSIS
**   creation of the TCP server connection for debug sessions
**   IN:    void
**   OUT :  void
**-------------------------------------------------------
*/
void uma_dbg_init(uint32_t nbElements,uint32_t ipAddr, uint16_t serverPort) {
  ruc_tcp_server_connect_t  inputArgs;
  UMA_DBG_SESSION_S         *p;
  ruc_obj_desc_t            *pnext ;
  void                      *idx;
  uint32_t                    tcpCnxServer;
  char                     * pChar;
  
  /*
  ** Save version reference in global variable
  */
  sprintf(rozofs_github_reference,"%s %s", VERSION, ROZO_GIT_REF);

  /* Create a TCP connection server */
  inputArgs.userRef    = 0;
  inputArgs.tcpPort    = serverPort /* UMA_DBG_SERVER_PORT */ ;
  inputArgs.priority   = 1;
  inputArgs.ipAddr     = ipAddr;
  inputArgs.accept_CBK = uma_dbg_accept_CBK;
  
  pChar = (char *) &inputArgs.cnxName[0];
  pChar += rozofs_string_append(pChar,"L:DIAG/");
  pChar += rozofs_ipv4_port_append (pChar,ipAddr,serverPort);  

  if ((tcpCnxServer = ruc_tcp_server_connect(&inputArgs)) == (uint32_t)-1) {
    severe("ruc_tcp_server_connect" );
  }
  
  /* Service already initialized */
  if (uma_dbg_initialized) return;

  uma_dbg_initialized = TRUE;
  
  uptime = time(0);

  /* Create a distributor of debug sessions */
  uma_dbg_freeList = (UMA_DBG_SESSION_S*)ruc_listCreate(nbElements,sizeof(UMA_DBG_SESSION_S));
  if (uma_dbg_freeList == (UMA_DBG_SESSION_S*)NULL) {
    severe( "ruc_listCreate(%d,%d)", nbElements,(int)sizeof(UMA_DBG_SESSION_S) );
    return;
  }

  /* Loop on initializing the distributor entries */
  pnext = NULL;
  idx = 0;
  while (( p = (UMA_DBG_SESSION_S*) ruc_objGetNext(&uma_dbg_freeList->link, &pnext)) != NULL) {
    p->ref       = (void *) idx++;
    p->ipAddr    = (uint32_t)-1;
    p->port      = (uint16_t)-1;
    p->tcpCnxRef = (uint32_t)-1;
    p->recvPool  = ruc_buf_poolCreate(2,UMA_DBG_MAX_SEND_SIZE);
  }

  /* Initialize the active list */
  uma_dbg_activeList = (UMA_DBG_SESSION_S*) malloc(sizeof(UMA_DBG_SESSION_S));
  if (uma_dbg_activeList == ((UMA_DBG_SESSION_S*)NULL)) {
    severe( "uma_dbg_activeList = malloc(%d)", (int)sizeof(UMA_DBG_SESSION_S) );
    return;
  }
  ruc_listHdrInit(&uma_dbg_activeList->link);
  
  uma_dbg_addTopicAndMan("who", uma_dbg_show_name, uma_dbg_show_name_man, 0);
  uma_dbg_addTopicAndMan("uptime", uma_dbg_show_uptime,uma_dbg_show_uptime_man,0);
  uma_dbg_addTopicAndMan("version", uma_dbg_show_version,uma_dbg_show_version_man,0);
  uma_dbg_addTopicAndMan("git",uma_dbg_show_git_ref, uma_dbg_show_git_ref_man, 0);
  uma_dbg_addTopic_option("system", uma_dbg_system_cmd, UMA_DBG_OPTION_HIDE); 
  uma_dbg_addTopicAndMan("ps", uma_dbg_system_ps,uma_dbg_system_ps_man,0);
  uma_dbg_addTopicAndMan("reserved_ports", uma_dbg_reserved_ports, uma_dbg_reserved_ports_man, 0);
  uma_dbg_addTopicAndMan("counters", uma_dbg_counters_reset, uma_dbg_counters_reset_man, 0);
  uma_dbg_addTopicAndMan("manual", uma_dbg_manual, uma_dbg_manual_man, 0);
}
/*
**-------------------------------------------------------
**  #SYNOPSIS
**   Give the system name to be display 
**   IN:    system_name
**   OUT :  void
**-------------------------------------------------------
*/  
void uma_dbg_set_name( char * system_name) {

  if (uma_gdb_system_name != NULL) {
    severe( "uma_dbg_set_name(%s) although name %s already set", system_name, uma_gdb_system_name );
    return;
  }  
  
  uma_gdb_system_name = malloc(strlen(system_name)+1);
  if (uma_gdb_system_name == NULL) {
    severe( "uma_dbg_set_name out of memory" );
    return;
  }
  
  strcpy(uma_gdb_system_name,system_name);
}
