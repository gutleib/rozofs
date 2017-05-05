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

#ifndef _ROZOFS_RCMD_H
#define _ROZOFS_RCMD_H

#include <rozofs/common/log.h>
#include <rozofs/core/rozofs_fdset.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/rozofs_service_ports.h>

/*
** Maximum data size after header
*/
#define ROZOFS_RCMD_MAX_PARAM_SIZE  (64*1024)

/*
** Magic number in the 8 first bytes of the message
*/
#define ROZOFS_RCMD_MAGIC_NB 0x52454D4F54434D44ULL

/*
** Status field 
*/
typedef enum _rozofs_rcmd_status_e {
  rozofs_rcmd_status_cmd,
  rozofs_rcmd_status_success,
  rozofs_rcmd_status_failed,
  rozofs_rcmd_status_unexpected_command,
  rozofs_rcmd_status_no_connection,
  rozofs_rcmd_status_remote_disconnection,
  rozofs_rcmd_status_protocol_error,
  rozofs_rcmd_status_missing_param,  
  rozofs_rcmd_status_message_too_big,
  rozofs_rcmd_status_out_of_memory,
  rozofs_rcmd_status_server_error,
  rozofs_rcmd_status_local_error,
  rozofs_rcmd_status_no_such_file,
  rozofs_rcmd_status_open_failure,
  rozofs_rcmd_status_read_failure,
  
  rozofs_rcmd_status_max
} rozofs_rcmd_status_e;

/*__________________________________________________________________________
** Return a string from a status value
**
** @param   status   a status value
**
** @retval  a string of the status field
**
**==========================================================================*/
static inline char *  rozofs_rcmd_status2String(rozofs_rcmd_status_e status) {
  switch(status) {
    case rozofs_rcmd_status_cmd: return "cmd";
    case rozofs_rcmd_status_success: return "success";
    case rozofs_rcmd_status_failed : return "failed";
    case rozofs_rcmd_status_unexpected_command: return "unexpected command";
    case rozofs_rcmd_status_no_connection: return "no connection";
    case rozofs_rcmd_status_remote_disconnection: return "remote disconnection";
    case rozofs_rcmd_status_protocol_error: return "protocol error";
    case rozofs_rcmd_status_missing_param: return "missing parameter";
    case rozofs_rcmd_status_message_too_big: return "message too big";
    case rozofs_rcmd_status_out_of_memory: return "out of memory";
    case rozofs_rcmd_status_server_error: return "server error";
    case rozofs_rcmd_status_local_error: return "local error";
    case rozofs_rcmd_status_no_such_file: return"no such file";
    case rozofs_rcmd_status_open_failure: return "open failure";
    case rozofs_rcmd_status_read_failure: return "read failure";
    case rozofs_rcmd_status_max: return "status max";
  }
  return "??";  
}

/*
** List of operations
*/
typedef enum _rozofs_rcmd_ope_e {

  // Unknown operation
  rozofs_rcmd_ope_unknown,

  // Build list of files for a rebuild from meta data library
  rozofs_rcmd_ope_rebuild_list,
  // remove directory previously used for rebuild_list
  rozofs_rcmd_ope_rebuild_list_clear,
  // Get a remote file content
  rozofs_rcmd_ope_getfile, 
  
  rozofs_rcmd_ope_max
} rozofs_rcmd_ope_e;
/*__________________________________________________________________________
** Return a string from an operation value
**
** @param   ope   an operation value
**
** @retval  a string of the operation field
**
**==========================================================================*/
static inline char *  rozofs_rcmd_ope2String(rozofs_rcmd_ope_e ope) {
  switch(ope) {
    case rozofs_rcmd_ope_unknown: return "unknown";
    case rozofs_rcmd_ope_rebuild_list: return "rebuild list";
    case rozofs_rcmd_ope_rebuild_list_clear: return "rebuild list clear";
    case rozofs_rcmd_ope_getfile : return "getfile";
    case rozofs_rcmd_ope_max: return "ope max";
  }
  return "??";  
}

/*
** Remote commande/response header
*/
typedef struct _rozofs_rcmd_hdr_t {
  uint64_t                 magic; // ROZOFS_RCMD_MAGIC_NB
  rozofs_rcmd_ope_e        ope;   // code of operation 
  rozofs_rcmd_status_e     status;// status of the response   
  uint32_t                 size;  // size of extra data 
  uint32_t                 more:1;// Whether more data are to come
  uint32_t                 padding:31;     
} rozofs_rcmd_hdr_t;

/*__________________________________________________________________________
** Initialize a command header structure
**
** @param   command   the command headerstructure to initialize
** @param   ope       the operation code
**
**==========================================================================*/
static inline void rozofs_rcmd_init_command(rozofs_rcmd_hdr_t * command, 
                                            rozofs_rcmd_ope_e   ope) {
  command->magic   = ROZOFS_RCMD_MAGIC_NB;
  command->ope     = ope;
  command->status  = rozofs_rcmd_status_cmd;
  command->more    = 0;
  command->size    = 0;
  command->padding = 0;  
}
/*__________________________________________________________________________
** Send a command to the server
**
** @param   socketId   TCP socket
** @param   command    formatted command header
** @param   data       optionnal extra parameters
**
** @retval rozofs_rcmd_status_success on success. rozofs_rcmd_status_no_connection else
**==========================================================================*/
static inline int rozofs_rcmd_send_command(int                   socketId,  
                                           rozofs_rcmd_hdr_t   * command,
                                           char                * data) {
  uint32_t            sent;
   
  /*
  ** Send the header
  */   
  sent = send(socketId, command, sizeof(rozofs_rcmd_hdr_t), 0);
  if (sent != sizeof(rozofs_rcmd_hdr_t)) {
    warning("send hdr (%d/%ld) %s",sent, sizeof(rozofs_rcmd_hdr_t), strerror(errno));
    return rozofs_rcmd_status_no_connection;
  }
  /*
  ** Send the extra data 
  */
  if (command->size) {
    sent = send(socketId, data, command->size, 0);
    if (sent != command->size) {
      warning("send data (%d/%d) %s",sent, command->size, strerror(errno));
      return rozofs_rcmd_status_no_connection;
    }
  }  
  return rozofs_rcmd_status_success;
} 
/*__________________________________________________________________________
** Read some data from the remote server
**
** @param   socketId   TCP socket
** @param   pBuf       reception buffer
** @param   size       size to read
** @param   tmo        timeout value (0 is no timeout)
**
  ==========================================================================*/
static inline int rozofs_rcmd_server_read(int    socketId, 
                                          char * pBuf, 
                                          int    size, 
                                          int    tmo) {
  rozo_fd_set    fdset;
  struct timeval timeout;
  int            nb;
  int            toread;
  int            nbread;

  toread = size;

  ROZO_FD_ZERO(&fdset);
  ROZO_FD_SET(socketId, &fdset);

  while(toread) {
  
    /*
    ** When timeout is given, do a select
    */
    if (tmo) {
      timeout.tv_sec  = tmo;
      timeout.tv_usec = 0;
      nb = select(socketId + 1, (fd_set*)&fdset, NULL, NULL, &timeout);
      
      if(nb == -1) {
        severe("select %s",strerror(errno)); /* an error accured */
        return -1;
      }

      if(nb == 0) {
        warning("timeout. expecting %d received only %d",size,size-toread); /* a timeout occured */
        return -1;
      }
    }    
    
    /*
    ** Read the data
    */
    nbread = read(socketId, pBuf, toread);
    if (nbread < 0) {
      severe("read %s", strerror(errno));
      return -1;
    }
    
    if (nbread == 0) {
      info("remote disconnection");
      return -1;
    }
        
    pBuf += nbread;
    toread -= nbread;
  }
  return size;      
} 
/*__________________________________________________________________________
** Read a response from the server 
**
** @param   socketId   TCP socket
** @param   ope        Expected operation code
** @param   response   received response header
** @param   data       received extra data
** @param   tmo        timeout
**
** @retval execution status from  rozofs_rcmd_status_e
**==========================================================================*/
static inline rozofs_rcmd_status_e rozofs_rcmd_read_response(
                                   int                  socketId, 
                                   int                  ope, 
                                   rozofs_rcmd_hdr_t *  response, 
                                   char              ** data,
                                   int                  tmo) {

  /*
  ** Read the response header
  */     
  if (rozofs_rcmd_server_read(socketId, (char*)response, sizeof(rozofs_rcmd_hdr_t), tmo) < -1) {
    return rozofs_rcmd_status_no_connection;
  } 
  /*
  ** Check some fields consistency
  */
  if (response->magic != ROZOFS_RCMD_MAGIC_NB) {
    severe("No magic word");
    return rozofs_rcmd_status_protocol_error;
  }
  
  if (response->ope != ope) {
    severe("Receive code %d expecting code %d",response->ope,ope);
    return rozofs_rcmd_status_protocol_error;
  }
  
  /*
  ** No extra data
  */
  if (response->size == 0) {
    return response->status;
  }  

  /*
  ** Too much extra data
  */
  if (response->size > ROZOFS_RCMD_MAX_PARAM_SIZE) {
    severe("Too big response %d/%d",response->size,ROZOFS_RCMD_MAX_PARAM_SIZE);
    return rozofs_rcmd_status_message_too_big;
  }
        
  /*
  ** Need to allocate a buffer for data
  */      
  if (*data == NULL) {  
    *data = malloc(response->size); 
    if (*data == NULL) {
      return rozofs_rcmd_status_out_of_memory;
    }
  }
     
  /*
  ** Read extra data
  */       
  if (rozofs_rcmd_server_read(socketId, *data, response->size, tmo) < -1) {
    return rozofs_rcmd_status_no_connection;  
  } 
  return response->status;
}
/*__________________________________________________________________________
** Get a file from remote server
**
** @param   socketId   TCP socket
** @param   from       remote file name
** @param   to         local file name
**
**==========================================================================*/
int rozofs_rcmd_getfile(int socketId, char * from, char * to) {
  uint32_t            size; 
  rozofs_rcmd_hdr_t   command;
  int                 fd=1;
  int                 res;
  char              * data = NULL;
  uint64_t            fsize=0;
     
  /*
  ** Initialize the command header
  */ 
  rozofs_rcmd_init_command(&command,rozofs_rcmd_ope_getfile);
  command.size = strlen(from)+1;
  
  res = rozofs_rcmd_send_command(socketId, &command, from);
  if (res != rozofs_rcmd_status_success) {
    goto failure;
  }
  
  /*
  ** Wait for the first response
  */
  res = rozofs_rcmd_read_response(socketId, rozofs_rcmd_ope_getfile, &command, &data, 30);
  if (res != rozofs_rcmd_status_success) {
    goto failure;
  }
    
  /*
  ** Open local destination file
  */  
  fd = open(to, O_WRONLY | O_CREAT | O_TRUNC,0777);
  if (fd < 0) {
    res = rozofs_rcmd_status_local_error;
    goto failure;     
  } 
 
  while (1) {
  
    /*
    ** Write the received data to the file
    */
    if (command.size != 0) {
      size = write(fd, data, command.size);
      if (size != command.size) {
        res = rozofs_rcmd_status_local_error;
        goto failure;
      }
      fsize += size;
    }
    
    /*
    ** No more data to come
    */
    if (!command.more) {
      if (data != NULL) {
        free(data);
        data = NULL;
      }      
      close(fd);
      return rozofs_rcmd_status_success;     
    }
    
    /*
    ** More data
    */   
    res = rozofs_rcmd_read_response(socketId, rozofs_rcmd_ope_getfile, &command, &data, 30);
    if (res != rozofs_rcmd_status_success) {
      goto failure;
    }
  }
  
failure:
  if (data != NULL) {
    free(data);
    data = NULL;
  }
      
  if (fd!=-1) {
    close(fd);
    fd=-1;
  }
    
  unlink(to);
  return res;
}
/*__________________________________________________________________________
  Request remote server to create rebuild jobs
  ==========================================================================*/
int rozofs_rcmd_rebuild_list(int socketId, char * parameters) {
  int                   res;
  rozofs_rcmd_hdr_t   command;
  char                * data = NULL;

  rozofs_rcmd_init_command(&command,rozofs_rcmd_ope_rebuild_list);
  command.size = strlen(parameters)+1;
  
  res = rozofs_rcmd_send_command(socketId,&command,parameters);
  if (res != rozofs_rcmd_status_success) {
    return res;
  }  

  res = rozofs_rcmd_read_response(socketId, rozofs_rcmd_ope_rebuild_list, &command, &data, 0);
  if (data != NULL) {
    free(data);
    data = NULL;
  }  
  return res;
}
/*__________________________________________________________________________
  Request remote server to cleanup temporary rebuild dir
  ==========================================================================*/
int rozofs_rcmd_rebuild_list_clear(int socketId, char * dirExport, int rebuildRef) {
  int                   res;
  rozofs_rcmd_hdr_t   command;
  char                  data[1024];
  char                * pData;

  rozofs_rcmd_init_command(&command,rozofs_rcmd_ope_rebuild_list_clear);
  
  sprintf(data,"-r %d -E %s",rebuildRef,dirExport);
  command.size = strlen(data)+1;
  
  res = rozofs_rcmd_send_command(socketId,&command,data);
  if (res != rozofs_rcmd_status_success) {
    return res;
  }  
  
  pData = NULL;
  res = rozofs_rcmd_read_response(socketId, rozofs_rcmd_ope_rebuild_list_clear, &command, &pData, 20);
  if (pData != NULL) {
    free(pData);
    pData = NULL;
  }  
  return res;
}
/*__________________________________________________________________________
** Connect to the remote command server
**
** @param   host       Remote command server host
**
** retval the connected TCP socket   
**
**==========================================================================*/
static inline int rozofs_rcmd_connect_to_server(char * host) {
  int                 socketId;  
  struct  sockaddr_in vSckAddr = {0};
  int                 sockSndSize  = 32*ROZOFS_RCMD_MAX_PARAM_SIZE;
  int                 sockRcvdSize = 32*ROZOFS_RCMD_MAX_PARAM_SIZE;
  uint16_t            serverPort;


  /*
  ** Retrieve RCMD service port
  */
  serverPort = rozofs_get_service_port_export_rcmd();
  
  /*
  ** Get host IP address 
  */
  if (rozofs_host2ip_netw(host, &vSckAddr.sin_addr.s_addr) != 0) {
    severe("rozofs_host2ip failed for host : %s, %s", host,strerror(errno));
    return -1;
  }
  vSckAddr.sin_family = AF_INET;
  vSckAddr.sin_port   = htons(serverPort);
    
  /*
  ** now create the socket for TCP
  */
  if ((socketId = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    severe("socket %s",strerror(errno));
    return -1;
  }  
  
  /* 
  ** change sizeof the buffer of socket for sending
  */
  if (setsockopt (socketId,SOL_SOCKET,SO_SNDBUF,(char*)&sockSndSize,sizeof(int)) == -1)  {
    severe("setsockopt SO_SNDBUF %s",strerror(errno));
    close(socketId);
    return -1;
  }
  /* 
  ** change sizeof the buffer of socket for receiving
  */  
  if (setsockopt (socketId,SOL_SOCKET,SO_RCVBUF,(char*)&sockRcvdSize,sizeof(int)) == -1)  {
    severe("setsockopt SO_RCVBUF %s",strerror(errno));
    close(socketId);
    return -1;
  }
  

  /* Connect */
  if (connect(socketId,(struct sockaddr *)&vSckAddr,sizeof(struct sockaddr_in)) == -1) {
    severe("connect %s", strerror(errno));
    return -1;
  }
  return socketId;
}
/*__________________________________________________________________________
** Disconnect from server
**
** @param   socketId   TCP socket
**  
**==========================================================================*/
static inline void rozofs_rcmd_disconnect_from_server(int  socketId) {
  shutdown(socketId,SHUT_RDWR);     
  close(socketId);
}
#endif
