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
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <syslog.h>
#include <libgen.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <pthread.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/common_config.h>
#include <rozofs/core/ruc_common.h>
#include <rozofs/core/ruc_sockCtl_api.h>
#include <rozofs/core/ruc_timer_api.h>
#include <rozofs/core/uma_tcp_main_api.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/rozofs_rpc_non_blocking_generic_srv.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/ruc_tcpServer_api.h>
#include <rozofs/core/ruc_tcp_client_api.h>
#include <rozofs/core/uma_well_known_ports_api.h>
#include <rozofs/core/rozofs_rcmd.h>
#include <rozofs/core/rozofs_core_files.h>
#include <rozofs/rozofs_service_ports.h>

#define RCMD_NAME "rcmd"

/*
** Number of sub threads to run commands
*/
#define              ROZFS_RCMD_MAX_SUB_THREADS   32

/*
** A 64 bytes reserve for some memory blocks
*/
#define              ROZOFS_RCMD_RESERVE           64


/*
** Thread context
*/

// status
typedef enum _rozofs_rcmd_thread_status_e {
  rozofs_rcmd_thread_status_free,
  rozofs_rcmd_thread_status_running
} rozofs_rcmd_thread_status_e;
// structure
typedef struct _rozofs_rcmd_thread_ctx_t {
  int                         idx;
  rozofs_rcmd_thread_status_e status;
  int                         socket;
  pthread_t                   threadId;
  uint32_t                    ip;
  uint16_t                    port;
} rozofs_rcmd_thread_ctx_t;

/*
** Table of thread context
*/
rozofs_rcmd_thread_ctx_t rozofs_rcmd_thread_ctx_tbl[ROZFS_RCMD_MAX_SUB_THREADS];


/*
** rozofs_rcmd_statistics
*/
typedef struct _rozofs_rcmd_statistics_t {
  uint64_t    active_sessions;
  uint64_t    total_sessions;
  uint64_t    accept_error;
  uint64_t    too_much_client;
  uint64_t    sub_thread_create;
} rozofs_rcmd_statistics_t;
rozofs_rcmd_statistics_t rozofs_rcmd_stats = {0};

/*
** rozofs_rcmd_profiler
*/
typedef struct _rozofs_rcmd_profiler_one_req_t {
  uint64_t    count;
  uint64_t    error;
} rozofs_rcmd_profiler_one_req_t;

rozofs_rcmd_profiler_one_req_t rozofs_rcmd_profiler[rozofs_rcmd_ope_max];

/*
** Error counters
*/
uint64_t rozofs_rcmd_error[rozofs_rcmd_status_max];

/*
** Macro for syslog embedding client IP address and port
*/
#define RCMD_LOG(which,fmt, ...) which("[%u.%u.%u.%u:%u] " fmt,(p->ip>>24)&0xFF,(p->ip>>16)&0xFF,(p->ip>>8)&0xFF,p->ip&0xFF,p->port,##__VA_ARGS__)


/*__________________________________________________________________________
** 
** Profiler
**
**==========================================================================*/
void show_profiler(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
  int   ope;
  int   reset = 0;

  if (argv[1] != NULL) {
    if (strcmp(argv[1],"reset")==0) {
      reset = 1;
    }
  }  

  pChar += rozofs_string_append(pChar, "\n{ \"profiler\" : [\n");
  ope = rozofs_rcmd_ope_unknown;
  while(ope < rozofs_rcmd_ope_max) {
    if (ope!=rozofs_rcmd_ope_unknown) {
      pChar += rozofs_string_append(pChar, ",\n");
    }
    pChar += rozofs_string_append(pChar, "      { \"ope\" : \"");
    pChar += rozofs_string_padded_append(pChar, 18, rozofs_left_alignment, rozofs_rcmd_ope2String(ope));
    pChar += rozofs_string_append(pChar, "\", \"count\" : ");
    pChar += rozofs_u64_append(pChar, rozofs_rcmd_profiler[ope].count);
    pChar += rozofs_string_append(pChar, "\t, \"errors\" : ");    
    pChar += rozofs_u64_append(pChar, rozofs_rcmd_profiler[ope].error);
    pChar += rozofs_string_append(pChar, "}");    
    ope++;
  }
  pChar += rozofs_string_append(pChar, "\n  ]\n}\n");
  
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  

  if (reset) {
    memset(rozofs_rcmd_profiler,0, sizeof(rozofs_rcmd_profiler));
  }    
}    

/*__________________________________________________________________________
** 
** Statistics
**
**==========================================================================*/
#define RCMD_SHOW_STAT(stat) {\
  pChar += rozofs_string_append(pChar, "\n      \"");\
  pChar += rozofs_string_append(pChar, #stat);\
  pChar += rozofs_string_append(pChar, "\" : ");\
  pChar += rozofs_u64_append(pChar, rozofs_rcmd_stats.stat);\
}
#define RCMD_SHOW_STAT_NEXT(stat) {\
  pChar += rozofs_string_append(pChar, ",");\
  RCMD_SHOW_STAT(stat);\
}

void show_statistics(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
  int   reset = 0;
  int   status;
  
  if (argv[1] != NULL) {
    if (strcmp(argv[1],"reset")==0) {
      reset = 1;
    }
  }  

  pChar += rozofs_string_append(pChar, "\n{ \"statistics\" : {");
  RCMD_SHOW_STAT(total_sessions);
  RCMD_SHOW_STAT(active_sessions);
  RCMD_SHOW_STAT_NEXT(accept_error);
  RCMD_SHOW_STAT_NEXT(too_much_client);
  pChar += rozofs_string_append(pChar, "\n  },\n");
  
  pChar += rozofs_string_append(pChar, "  \"errors\" : [\n");
  status = rozofs_rcmd_status_cmd+1;
  while(status <= rozofs_rcmd_status_max) {
    if (status!=rozofs_rcmd_status_cmd+1) {
      pChar += rozofs_string_append(pChar, ",\n");
    }
    pChar += rozofs_string_append(pChar, "      { \"status\" : \"");
    pChar += rozofs_string_padded_append(pChar, 28, rozofs_left_alignment, rozofs_rcmd_status2String(status));
    pChar += rozofs_string_append(pChar, "\", \"count\" : ");
    pChar += rozofs_u64_append(pChar, rozofs_rcmd_error[status]);
    pChar += rozofs_string_append(pChar, "}");    
    status++;
  }
  pChar += rozofs_string_append(pChar, "\n  ]\n}\n");

  
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  

  if (reset) {
    memset(&rozofs_rcmd_stats,0, sizeof(rozofs_rcmd_stats));
    memset(rozofs_rcmd_error,0, sizeof(rozofs_rcmd_error));    
  }  
}    
/*__________________________________________________________________________
** Find a free thread context
**
** @param socket      The TCP socket to save in the allocated context
** @param clientaddr  The client address and port
**
** @retval the allocated context or null
**==========================================================================*/
rozofs_rcmd_thread_ctx_t * rozofs_rcmd_allocate_thread_cxt(int                  socket,
                                                           struct sockaddr_in * clientaddr){
  int                          idx;
  rozofs_rcmd_thread_ctx_t   * p=rozofs_rcmd_thread_ctx_tbl;       
  
  for (idx=0; idx< ROZFS_RCMD_MAX_SUB_THREADS; idx++,p++) {
    if (p->status == rozofs_rcmd_thread_status_free) {
      p->status = rozofs_rcmd_thread_status_running;
      if (p->socket != -1) {
        rozofs_rcmd_disconnect_from_server(p->socket);
        p->socket = -1;
      }
      p->socket = socket;
      p->ip     = (uint32_t) ntohl((uint32_t)(clientaddr->sin_addr.s_addr));
      p->port   = ntohs((uint16_t)(clientaddr->sin_port));
      return p;
    }
  }     
  return NULL;
}
/*__________________________________________________________________________
** Initialize the thread context table
**==========================================================================*/
void rozofs_rcmd_init_thread_tbl(){
  int                          idx;
  rozofs_rcmd_thread_ctx_t   * p=rozofs_rcmd_thread_ctx_tbl;       
  
  for (idx=0; idx< ROZFS_RCMD_MAX_SUB_THREADS; idx++,p++) {
    p->status   = rozofs_rcmd_thread_status_free;
    p->socket   = -1;
    p->idx      = idx;
    p->threadId = 0;
  }     
}
/*__________________________________________________________________________
** Release a thread context entry
**
** @param p    The thread context to release
**
**==========================================================================*/
void rozofs_rcmd_release_thread_ctx(rozofs_rcmd_thread_ctx_t * p){
  p->status   = rozofs_rcmd_thread_status_free;
  p->threadId = 0;
  if (p->socket != -1) {
    rozofs_rcmd_disconnect_from_server(p->socket);
    p->socket = -1;
  }   
}
/*__________________________________________________________________________
** Set REUSE_ADDR to the socket
**
** @param socket   The TCP socket to tune
**==========================================================================*/
static inline void rozofs_rcmd_reuseaddr(int socket){
  int optval = 1;
  setsockopt(socket, SOL_SOCKET, SO_REUSEADDR, (const void *)&optval , sizeof(int));
}
/*__________________________________________________________________________
** Modify socket blocking mode blocking socket
**
** @param socket     The TCP socket to tune
** @param blocking   Whether to set it blocking or not
**
** @retval 0 on success. -1 on error
**==========================================================================*/
static inline int rozofs_rcmd_set_blocking(int socket, int blocking){
  int fileflags;

  /*
  ** Read socket synchronous mode 
  */
  fileflags = fcntl(socket,F_GETFL,0);
  if (fileflags==-1) {
    severe("fcntl(F_GETFL) %s",strerror(errno));
    return -1;
  }
  
  if (blocking) {
    fileflags &= ~O_NDELAY;
  }
  else {  
    fileflags |= O_NDELAY;
  } 
  
  if((fcntl(socket,F_SETFL,fileflags))==-1) {
    severe("fcntl(F_SETFL,%d) %s",blocking, strerror(errno));
    return -1;
  }
  return 0;
}  
/*__________________________________________________________________________
** Initialize a rcmd response structure
**
** @param   response  the rcmd header structure to initialize
** @param   ope   the command code
**
**==========================================================================*/
static inline void rozofs_rcmd_init_response(rozofs_rcmd_hdr_t * response, 
                                             rozofs_rcmd_ope_e   ope) {
  response->magic   = ROZOFS_RCMD_MAGIC_NB;
  response->ope     = ope;
  response->status  = rozofs_rcmd_status_success;
  response->more    = 0;
  response->size    = 0;
  response->padding = 0;  
}
/*__________________________________________________________________________
** Send a message on TCP connection in blocking mode
**
** @param p       the thread context
** @param pBuf    pointeur to the data to send
** @param size    size of the data to send
**
** @retval the size sent on success. -1 on error
**==========================================================================*/
int rozofs_rcmd_blocking_send(rozofs_rcmd_thread_ctx_t * p, char * pBuf, int size) {
  ssize_t           sent; 
   
  sent = send(p->socket, pBuf, size, 0);
  if (sent < 0) {
    return -1;
  } 
  
  if (sent != size) {
    RCMD_LOG(warning,"send %d/%d %s",(int)sent,size,strerror(errno)); /* an error accured */
    return -1;
  }
  return sent;
}  
/*__________________________________________________________________________
** Read some data from a TCP connection in blocking mode
**
** @param p       the thread context
** @param pBuf    pointeur to the data to receive
** @param size    size of the data to receive
**
** @retval the size received on success. -1 on error
**==========================================================================*/
rozofs_rcmd_status_e rozofs_rcmd_blocking_read(rozofs_rcmd_thread_ctx_t * p, char * pBuf, int size) {
  int nbread;
  
  nbread = read(p->socket, pBuf, size);
  if (nbread < 0) {
    RCMD_LOG(severe,"read %s", strerror(errno));
    return rozofs_rcmd_status_server_error;
  }
    
  if (nbread == 0) {
    RCMD_LOG(info,"remote disconnection");
    return rozofs_rcmd_status_remote_disconnection;
  }

  return rozofs_rcmd_status_success;      
}
/*__________________________________________________________________________
** Process a getfile service. ie transfer the file content tto the requester
**
** @param p       the thread context
** @param hdr     header structure of the request
** @param fname   Local name of the file
**
** @retval status of the request (rozofs_rcmd_status_e)
**==========================================================================*/
rozofs_rcmd_status_e rozofs_rcmd_process_getfile(
                                     rozofs_rcmd_thread_ctx_t * p, 
                                     rozofs_rcmd_hdr_t        * hdr, 
                                     char                     * fname) {
  rozofs_rcmd_hdr_t   rsp;
  int                 fd = -1;
  char              * data = NULL;
  
  /*
  ** Initialize a response structure for the received command
  */
  rozofs_rcmd_init_response(&rsp, hdr->ope);
  
  /*
  ** A file name must be present in the extra data
  */
  if (hdr->size<=1) {
    rsp.status = rozofs_rcmd_status_missing_param;
    rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t));    
    goto out;
  } 
   
  /*
  ** Log the request
  */ 
  RCMD_LOG(info,"getfile %s",fname);
  
  /*
  ** Check the file exists
  */
  if (access(fname,R_OK)!=0) {
    rsp.status = rozofs_rcmd_status_no_such_file;
    rsp.size   = hdr->size;
    rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t));    
    rozofs_rcmd_blocking_send(p,(char *)fname,rsp.size);    
    goto out;
  }   

  /*
  ** Open the file for reading
  */
  fd = open(fname,O_RDONLY);
  if (fd < 0) {
    rsp.status = rozofs_rcmd_status_open_failure;
    rsp.size   = hdr->size;
    rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t));    
    rozofs_rcmd_blocking_send(p,(char *)fname,rsp.size);    
    goto out;
  } 

  /*
  ** Allocate a buffer for data trasnfer
  */
  data = xmalloc(ROZOFS_RCMD_MAX_PARAM_SIZE);
  if (data == NULL) {
    rsp.status = rozofs_rcmd_status_out_of_memory;
    rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t));  
    goto out;
  }

  /*
  ** Read and send the data
  */
  while (1) {

    /*
    ** Read the file context sequentialy
    */
    rsp.size = read(fd, data, ROZOFS_RCMD_MAX_PARAM_SIZE);
    if (rsp.size<0) {
      /*
      ** Read failed
      */
      rsp.status = rozofs_rcmd_status_read_failure;
      rsp.more   = 0;
      rsp.size   = 0;            
      rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t));
      break;
    }

    rsp.status = rozofs_rcmd_status_success;
    if (rsp.size==ROZOFS_RCMD_MAX_PARAM_SIZE) {
      rsp.more = 1; // more data are to come
    }
    else {  
      rsp.more = 0; // last packet sent
    } 
    /*
    ** Send the header
    */
    rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t)); 
    /*
    ** Send the data if any
    */
    if (rsp.size) {
      rozofs_rcmd_blocking_send(p,data,rsp.size);
    }
    /*
    ** no more to give. exit
    */
    if (!rsp.more) break;               
  }   
   
out:
 
  /*
  ** Rlease file descriptor
  */
  if (fd != -1) {
    close(fd);
    fd = -1;
  } 
   
  /*
  ** Release buffer
  */
  if (data != NULL) {
    xfree(data);
    data = NULL;
  } 
  return rsp.status;
}
/*__________________________________________________________________________
** Process a rebuild_list service. ie call rbsList to build list of FID to rebuild
**
** @param p             the thread context
** @param hdr           header structure of the request
** @param parameters    The received parameters
**
** @retval status of the request (rozofs_rcmd_status_e)
**==========================================================================*/
rozofs_rcmd_status_e rozofs_rcmd_process_rebuild_list(
                                     rozofs_rcmd_thread_ctx_t * p, 
                                     rozofs_rcmd_hdr_t        * hdr, 
                                     char                     * parameters) {
  rozofs_rcmd_hdr_t     rsp;
  char                * pChar;  
  int                   cmdLen;
  int                   res;

  /*
  ** Initialize a response structure for the received command
  */
  rozofs_rcmd_init_response(&rsp, hdr->ope);

  /*
  ** Parameters are mandatory
  */
  if (hdr->size == 0) {
    rsp.status = rozofs_rcmd_status_missing_param;
    rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t));    
    goto out;
  }
  /*
  ** Build the command line using space left at the beginning of the buffer
  */
  cmdLen = strlen("rozo_rbsList");
  pChar = parameters - cmdLen - 1;
  strcpy(pChar,"rozo_rbsList");
  pChar[cmdLen] = ' ';
  RCMD_LOG(info,"%s",pChar);

  /*
  ** Execute the command
  */
  res = system(pChar);
  if (res==0) { 
    rsp.status = rozofs_rcmd_status_success;   
  }
  else {
    rsp.status = rozofs_rcmd_status_failed;  
  }
  /*
  ** Send response
  */
  rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t));


out: 

  return rsp.status;
}
/*__________________________________________________________________________
** Process a rebuild_list_clear service. ie Cleanup the temporary directory
**
** @param p             the thread context
** @param hdr           header structure of the request
** @param parameters    The received parameters
**
** @retval status of the request (rozofs_rcmd_status_e)
**==========================================================================*/
rozofs_rcmd_status_e rozofs_rcmd_process_rebuild_list_clear(
                                 rozofs_rcmd_thread_ctx_t * p, 
                                 rozofs_rcmd_hdr_t        * hdr, 
                                 char                     * parameters) {
  rozofs_rcmd_hdr_t     rsp;
  int                   res;
  int                   rebuildRef;
  char                  buffer[2048];
  char                * pChar;
  int                   cmdLen;

  /*
  ** Initialize a response structure for the received command
  */  
  rozofs_rcmd_init_response(&rsp, hdr->ope);

  /*
  ** Parameters are mandatory
  */
  if (hdr->size == 0) {
    rsp.status = rozofs_rcmd_status_missing_param;
    rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t));    
    goto out;
  }
  
  if (hdr->size > (2000+ROZOFS_RCMD_RESERVE)) {
    rsp.status = rozofs_rcmd_status_message_too_big;
    rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t));    
    goto out;
  }
  
  /*
  ** scan parameter
  */
  res = sscanf(parameters,"-r %d -E %s", &rebuildRef, &buffer[ROZOFS_RCMD_RESERVE]);
  if (res != 2) {
    rsp.status = rozofs_rcmd_status_missing_param;
    rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t));    
    goto out;
  }
  
  /*
  ** Add rebuild ref at the end
  */
  pChar = &buffer[ROZOFS_RCMD_RESERVE];
  pChar += strlen(pChar);
  pChar += sprintf(pChar,"/rebuild.%d",rebuildRef);
  
  /*
  ** Add command at the beginning
  */
  pChar = &buffer[ROZOFS_RCMD_RESERVE];
  cmdLen = strlen("rm -rf");
  pChar -= (cmdLen + 1);
  strcpy(pChar,"rm -rf");
  pChar[cmdLen] = ' ';
  RCMD_LOG(info,"%s",pChar);

    
  /*
  ** Execute the command
  */
  res = system(pChar);
  if (res==0) { 
    rsp.status = rozofs_rcmd_status_success;   
  }
  else {
    rsp.status = rozofs_rcmd_status_failed;  
  }
  /*
  ** Send response
  */
  rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t));


out: 

  return rsp.status;
}
/*__________________________________________________________________________
** Execute rcmd subprocess code.
** Read incoming requests and process them until disconnection
**
**==========================================================================*/
void * run_rcmd_sub_process(rozofs_rcmd_thread_ctx_t * p) {
  char                * data = NULL;
  rozofs_rcmd_hdr_t     hdr;
  rozofs_rcmd_hdr_t     rsp;
  rozofs_rcmd_status_e  res;
  rozofs_rcmd_ope_e     ope;

  while (1) {
  
    /*
    ** Read a request from the connection
    */    
    rsp.status = rozofs_rcmd_blocking_read(p, (char *)&hdr, sizeof(hdr));
    if (rsp.status != rozofs_rcmd_status_success) {
      goto out;
    }

    /*
    ** Prepare the response
    */
    rozofs_rcmd_init_response(&rsp, hdr.ope);

    /*
    ** Check some values
    */

    if (hdr.magic != ROZOFS_RCMD_MAGIC_NB) {
      rsp.status = rozofs_rcmd_status_protocol_error;
      rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t));
      goto out;
    }

    if (hdr.status !=  rozofs_rcmd_status_cmd) {
      rsp.status = rozofs_rcmd_status_protocol_error;
      rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t));    
      goto out;
    }  

    if (hdr.size > ROZOFS_RCMD_MAX_PARAM_SIZE) {
      rsp.status = rozofs_rcmd_status_message_too_big;
      rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t));              
      goto out;
    }

    /*
    ** Read command parameters if any
    */ 
    if (hdr.size) {
      data = malloc(hdr.size+ROZOFS_RCMD_RESERVE+1);
      if (data==NULL) {
        rsp.status = rozofs_rcmd_status_out_of_memory;
        rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t));    
        goto out;
      }
      rsp.status = rozofs_rcmd_blocking_read(p, &data[ROZOFS_RCMD_RESERVE], hdr.size);
      if (rsp.status != rozofs_rcmd_status_success) {
        rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t));    
        goto out;
      }  
      /*
      ** Add a 0 at the end, just in case
      */
      data[hdr.size+ROZOFS_RCMD_RESERVE] = 0;
    }

    /*
    ** Dispatch depending on the request code
    */
    ope = hdr.ope;
    switch(ope) {

      /*
      ** Request to build the list of rebuild jobs
      */
      case rozofs_rcmd_ope_rebuild_list :
        rozofs_rcmd_profiler[ope].count++;      
        res = rozofs_rcmd_process_rebuild_list(p, &hdr, &data[ROZOFS_RCMD_RESERVE]);
        break;

      /*
      ** Request to clear rebuild lists 
      */
      case rozofs_rcmd_ope_rebuild_list_clear :
        rozofs_rcmd_profiler[ope].count++;
        res = rozofs_rcmd_process_rebuild_list_clear(p, &hdr, &data[ROZOFS_RCMD_RESERVE]);
        break;

      /*
      ** Transfer a file from server to client
      */
      case rozofs_rcmd_ope_getfile :
        rozofs_rcmd_profiler[ope].count++;      
        res = rozofs_rcmd_process_getfile(p, &hdr, &data[ROZOFS_RCMD_RESERVE]);      
        break;

      /*
      ** Unexpected command
      */      
      default:
        ope = rozofs_rcmd_ope_unknown;
        rozofs_rcmd_profiler[ope].count++;      
        rsp.status = rozofs_rcmd_status_unexpected_command;
        res = rozofs_rcmd_status_unexpected_command;
        rozofs_rcmd_blocking_send(p,(char *)&rsp,sizeof(rozofs_rcmd_hdr_t));            
    }
     
    /*
    ** Free extra parameters 
    */
    if (data) {
      free(data);
      data = NULL;
    } 
         
    /*
    ** Log bad status
    */ 
    if (res != rozofs_rcmd_status_success) {
      RCMD_LOG(info,"%s : %s",
               rozofs_rcmd_ope2String(ope),
               rozofs_rcmd_status2String(res));
      rozofs_rcmd_profiler[ope].error++; 
      if (res > rozofs_rcmd_status_max) {
        res = rozofs_rcmd_status_max;
      }    
    }
    rozofs_rcmd_error[res]++;         
  }   
  
out:  
  rozofs_rcmd_error[rsp.status]++;
      
  if (data) {
    free(data);
    data = NULL;
  }  
  return NULL;
} 
/*__________________________________________________________________________
  Child process
  ==========================================================================*/
void * rozofs_rcmd_child(void * arg) {
  rozofs_rcmd_thread_ctx_t * p = (rozofs_rcmd_thread_ctx_t *) arg;

  rozofs_rcmd_stats.total_sessions++;
  __atomic_fetch_add(&rozofs_rcmd_stats.active_sessions,1,__ATOMIC_SEQ_CST);
  
  p->threadId = getpid();

  run_rcmd_sub_process(p);    

  rozofs_rcmd_release_thread_ctx(p);

  __atomic_fetch_sub(&rozofs_rcmd_stats.active_sessions,1,__ATOMIC_SEQ_CST);

  pthread_exit(0);
  return NULL;
}
/*__________________________________________________________________________
  Tune TCP socket
  ==========================================================================*/
uint32_t tune_client_socket(rozofs_rcmd_thread_ctx_t * p) {
  int YES   = 1;
  int IDLE  = 2;
  int INTVL = 2;
  int COUNT = 3;

  /*
  ** active keepalive on the new connection
  */
  setsockopt (p->socket,SOL_SOCKET,SO_KEEPALIVE,&YES,sizeof(int));
  setsockopt (p->socket,IPPROTO_TCP,TCP_KEEPIDLE,&IDLE,sizeof(int));
  setsockopt (p->socket,IPPROTO_TCP,TCP_KEEPINTVL,&INTVL,sizeof(int));
  setsockopt (p->socket,IPPROTO_TCP,TCP_KEEPCNT,&COUNT,sizeof(int));

  rozofs_rcmd_reuseaddr(p->socket);
  
  return 0;
}
/*__________________________________________________________________________
** Start a thread
**==========================================================================*/
int rozofs_cmd_start_thread(void *(*start_routine) (void *), void * arg) {
  pthread_attr_t             attr;
  pthread_t                  thid;

  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_DETACHED);

  if (pthread_create(&thid,&attr,start_routine,arg)!= 0) {
    severe("pthread_create %s", strerror(errno));
    return -1;           
  }
  
  pthread_attr_destroy(&attr);  
  return 0;
}
/*__________________________________________________________________________
  Remote command server main thread listening to incoming TCP connection
  ==========================================================================*/
void * rozofs_rcmd_server(void * unused) {
  int listenSocket; /* listen socket */
  int clientSocket; /* client socket */
  int portno;       /* port to listen on */
  unsigned int clientlen;    /* byte size of client's address */
  struct sockaddr_in serveraddr; /* server's addr */
  struct sockaddr_in clientaddr; /* client addr */
  int accept_error=0;
  rozofs_rcmd_thread_ctx_t * pThread;

  uma_dbg_thread_add_self("rcmd_srv");

  pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
  
  /*
  ** Initialize thread context table
  */
  rozofs_rcmd_init_thread_tbl();
  
  /*
  ** Retrieve RCMD service port
  */
  portno = rozofs_get_service_port_export_rcmd();

  /* 
  ** Create the listening TCP socket 
  */
  listenSocket = socket(AF_INET, SOCK_STREAM, 0);
  if (listenSocket < 0) { 
    severe("socket %s",strerror(errno));
    fatal("socket");
  }
  
  /*
  ** Tune socket
  */
  rozofs_rcmd_reuseaddr(listenSocket);  
  

  /* 
  ** bind the socket to the port 
  */
  bzero((char *) &serveraddr, sizeof(serveraddr));
  serveraddr.sin_family = AF_INET;
  serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
  serveraddr.sin_port = htons((unsigned short)portno);
  if (bind(listenSocket, (struct sockaddr *) &serveraddr, sizeof(serveraddr)) < 0) { 
    severe("bind %s",strerror(errno));
    fatal("bind");
  }

  /* 
  ** listen: make this socket ready to accept connection requests 
  */
  if (listen(listenSocket, 5) < 0) {/* allow 5 requests to queue up */ 
    severe("listen %s",strerror(errno));
    fatal("listen");
  }
  
  /*
  ** Wait for incoming connections
  */
  while (1) {
  
    clientlen = sizeof(clientaddr);
    
    /* 
    ** accept: wait for a connection request 
    */
    clientSocket = accept(listenSocket, (struct sockaddr *) &clientaddr, &clientlen);
    if (clientSocket < 0) {
      accept_error++;
      if (accept_error==8) {
        fatal("accept");
      } 
      rozofs_rcmd_stats.accept_error++;
      continue; 
    }    
    accept_error = 0;  

    /*
    ** Control number of running sub threades
    */
    pThread = rozofs_rcmd_allocate_thread_cxt(clientSocket,&clientaddr);
    if (pThread == NULL) {
      rozofs_rcmd_stats.too_much_client++;
      /*
      ** Release the socket
      */
      close(clientSocket);
      continue;
    }

    /*
    ** Tune socket
    */
    tune_client_socket(pThread);      
    
    /*
    ** Start a sub thread
    */
    if (rozofs_cmd_start_thread(rozofs_rcmd_child,pThread) != 0) {
      /*
      ** Release the thread context
      */
      rozofs_rcmd_release_thread_ctx(pThread);
      continue;       
    }  
  }
  return NULL;
}
/*__________________________________________________________________________
  The faous ruc_init
  ==========================================================================*/
#define fdl_debug_loop(line) 
uint32_t ruc_init() {
  int ret = RUC_OK;
  uint32_t mx_tcp_client = 2;
  uint32_t mx_tcp_server = 8;
  uint32_t mx_tcp_server_cnx = 10;
  uint32_t mx_af_unix_ctx = 20;
  uint16_t dbgPort=0;


  //#warning TCP configuration ressources is hardcoded!!
  /*
   ** init of the system ticker
   */
  rozofs_init_ticker();
  /*
   ** trace buffer initialization
   */
  ruc_traceBufInit();

  /*
   ** initialize the socket controller:
   **   for: NPS, Timer, Debug, etc...
   */
  // warning set the number of contexts for socketCtrl to 1024
  ret = ruc_sockctl_init(mx_af_unix_ctx);
  if (ret != RUC_OK) {
      fdl_debug_loop(__LINE__);
      fatal( " socket controller init failed" );
  }

  /*
   **  Timer management init
   */
  ruc_timer_moduleInit(FALSE);

  while (1) {
      /*
       **--------------------------------------
       **  configure the number of TCP connection
       **  supported
       **--------------------------------------   
       **  
       */
      ret = uma_tcp_init(mx_tcp_client + mx_tcp_server + mx_tcp_server_cnx);
      if (ret != RUC_OK) {
          fdl_debug_loop(__LINE__);
          break;
      }

      /*
       **--------------------------------------
       **  configure the number of TCP server
       **  context supported
       **--------------------------------------   
       **  
       */
      ret = ruc_tcp_server_init(mx_tcp_server);
      if (ret != RUC_OK) {
          fdl_debug_loop(__LINE__);
          break;
      }


      /*
      **--------------------------------------
      **  configure the number of AF_UNIX/AF_INET
      **  context supported
      **--------------------------------------   
      **  
      */    
      ret = af_unix_module_init(mx_af_unix_ctx,
                                2,1024*1, // xmit(count,size)
                                2,1024*1 // recv(count,size)
                                );
      if (ret != RUC_OK) break;   
      /*
       **--------------------------------------
       **   D E B U G   M O D U L E
       **--------------------------------------
       */       
      dbgPort = rozofs_get_service_port_export_rcmd_diag();
      uma_dbg_init(10, INADDR_ANY, dbgPort);
      uma_dbg_set_name(RCMD_NAME);

      /*
      ** RPC SERVER MODULE INIT
      */
      ret = rozorpc_srv_module_init_ctx_only(1);
      if (ret != RUC_OK) break; 


      break;
  }

  memset(rozofs_rcmd_profiler,0, sizeof(rozofs_rcmd_profiler));
  uma_dbg_addTopic_option("profiler", show_profiler,UMA_DBG_OPTION_RESET);

  memset(rozofs_rcmd_error,0, sizeof(rozofs_rcmd_error));
  memset(&rozofs_rcmd_stats,0, sizeof(rozofs_rcmd_stats));
  uma_dbg_addTopic_option("statistics", show_statistics,UMA_DBG_OPTION_RESET);


  return ret;
}
/*__________________________________________________________________________
  MAIN thread lisening to incoming TCP connection
  ==========================================================================*/
int main(int argc, char *argv[]) {
  
  /*
  ** Change local directory to "/"
  */
  if (chdir("/")!=0) {}

  /*
  ** read common config file
  */
  common_config_read(NULL);    

  /*
  ** Get utility name and record it for syslog
  */
  uma_dbg_record_syslog_name(RCMD_NAME);    
  uma_dbg_thread_add_self("Main");
      
  /*
  ** Set a signal handler
  */
  rozofs_signals_declare(RCMD_NAME, common_config.nb_core_file); 

  /*
  ** RUC init
  */
  if (ruc_init() != RUC_OK) {
    fatal("ruc_init");
  }
  
  /*
  ** Start server listening thread
  */
  if (rozofs_cmd_start_thread(rozofs_rcmd_server,NULL) != 0) {
    fatal("ruc_init");
  }

  /*
   ** main loop
   */
  while (1) {
    ruc_sockCtrl_selectWait();
  }
  fatal("Exit from ruc_sockCtrl_selectWait()");
  fdl_debug_loop(__LINE__);
}
