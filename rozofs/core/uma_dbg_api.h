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
#ifndef UMA_DBG_API_H
#define UMA_DBG_API_H

#include <stdio.h>
#include <arpa/inet.h>

#include <rozofs/common/types.h>
#include <rozofs/common/log.h>
#include <rozofs/core/rozofs_string.h>
#include <rozofs/core/uma_tcp_main_api.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/af_inet_stream_api.h>

#include "uma_dbg_msgHeader.h"
#include "ruc_common.h"
#include "rozofs_string.h"
#include "ruc_buffer_api.h"

#include "ruc_buffer_debug.h"

#define   UMA_DBG_OPTION_HIDE           (1<<0)
#define   UMA_DBG_OPTION_RESET          (1<<1)

/*
** Max length of the user payload when answering to a debug command
*/
#define UMA_DBG_MAX_SEND_SIZE (1024*384)
extern char uma_dbg_temporary_buffer[];
extern uint32_t uma_dbg_do_not_send;
extern char   * uma_gdb_system_name;
extern char     rcvCmdBuffer[];
extern UMA_MSGHEADER_S uma_dbg_lastSendHeader;


static inline void uma_dbg_send(uint32_t tcpCnxRef, void  *bufRef, uint8_t end, char *string);


/*__________________________________________________________________________
 */
/**
*  Display bytes with correct unit 
*  @param value         Value in bytes to display
*  @param value_string  String where to format the value
*/
static inline int uma_dbg_byte2String(uint64_t value, char * value_string) {
  uint64_t   modulo=0;
  char     * pt = value_string;
  
  if (value<1000) {
    pt += rozofs_u64_append(pt,value);
    pt += rozofs_string_append(pt," Bytes");
    return (pt-value_string);  		    
  }
  
  if (value<1000000) {
  
    if (value>99000) {
      pt += rozofs_u64_append(pt,value/1000);
      pt += rozofs_string_append(pt," KB");
      return (pt-value_string);    		    
    }
    
    modulo = (value % 1000) / 100;
    pt += rozofs_u64_append(pt,value/1000);
    *pt++ = '.';
    pt += rozofs_u32_padded_append(pt,1,rozofs_zero,modulo);
    pt += rozofs_string_append(pt," KB");
    return (pt-value_string); 
  }
  
  if (value<1000000000) {
  
    if (value>99000000) {
      pt += rozofs_u64_append(pt,value/1000000);
      pt += rozofs_string_append(pt," MB");
      return (pt-value_string);    		    
    }
    
    modulo = (value % 1000000) / 100000;
    pt += rozofs_u64_append(pt,value/1000000);
    *pt++ = '.';
    pt += rozofs_u32_padded_append(pt,1,rozofs_zero,modulo);
    pt += rozofs_string_append(pt," MB");
    return (pt-value_string);     
  }
    
  if (value<1000000000000) {
  
    if (value>99000000000) {
      pt += rozofs_u64_append(pt,value/1000000000);
      pt += rozofs_string_append(pt," GB");
      return (pt-value_string);    		    
    }
    
    modulo = (value % 1000000000) / 100000000;
    pt += rozofs_u64_append(pt,value/1000000000);
    *pt++ = '.';
    pt += rozofs_u32_padded_append(pt,1,rozofs_zero,modulo);
    pt += rozofs_string_append(pt," GB");
    return (pt-value_string);     
  }  
  
  if (value>99000000000000) {
    pt += rozofs_u64_append(pt,value/1000000000000);
    pt += rozofs_string_append(pt," PB");
    return (pt-value_string);     
  }  

  modulo = (value % 1000000000000) / 100000000000;
  pt += rozofs_u64_append(pt,value/1000000000000);
  *pt++ = '.';
  pt += rozofs_u32_padded_append(pt,1,rozofs_zero,modulo);      
  pt += rozofs_string_append(pt," PB");
  return (pt-value_string);   
}
/*__________________________________________________________________________
*  Add a thread in the thread table
** @param tid    The thread identifier
** @param name   The function of the tread
*/
void uma_dbg_thread_add_self(char * name);
/*__________________________________________________________________________
*  Remove a thread from the thread table
*/
void uma_dbg_thread_remove_self(void) ;
/*__________________________________________________________________________
*  Get the thread name of a thread
** @param tid    The thread identifier
** @retval the name of the thread or NULL
*/
char * uma_dbg_thread_get_name(pthread_t tid);

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
char * uma_dbg_hexdump(void *mem, unsigned int len, char * p);
/*__________________________________________________________________________
 */
/**
**  @param tcpCnxRef   TCP connection reference
**
*  Return an xmit buffer to be used for TCP sending
*/
void * uma_dbg_get_new_buffer(uint32_t tcpCnxRef); 
/*__________________________________________________________________________
 */
/**
*  Return a temporary buffer where one can format a response
*/
static inline char * uma_dbg_get_buffer() {return uma_dbg_temporary_buffer;}
/*__________________________________________________________________________
 */
/**
*  Return the size of the temporary buffer where one can format a response
*/
static inline int uma_dbg_get_buffer_len() {return UMA_DBG_MAX_SEND_SIZE;}
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
void uma_dbg_process_command_file(char * command_file_name);
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
static inline char *  uma_dbg_cmd_recall(UMA_MSGHEADER_S * pHead) {  
  char            *pChar;

  pChar = (char*) (pHead+1);
  
  pChar += rozofs_string_append(pChar,"____[");
  pChar += rozofs_string_append(pChar,uma_gdb_system_name);
  pChar += rozofs_string_append(pChar,"]__[");  
  pChar += rozofs_string_append(pChar,rcvCmdBuffer);
  pChar += rozofs_string_append(pChar,"]____\n");  
  * pChar = 0; 
  return pChar;
}
/*-----------------------------------------------------------------------------
**
**   Send back a diagnostic response
**
**  @param tcpCnxRef   TCP connection reference
**  @param bufRef      reference of the received buffer that will be used to respond
**  @param end         whether this is the last buffer of the response 
**  @param string      A pre-formated string ontaining the reponse 
**
**----------------------------------------------------------------------------
*/
static inline void uma_dbg_send_buffer(uint32_t tcpCnxRef, void  *bufRef, int len, int end) {
  UMA_MSGHEADER_S *pHead;

  len++;
  
  /*
  ** Check the message is not too long
  */
  if ((len >= ruc_buf_getMaxPayloadLen(bufRef))) {
    char * tmp = uma_dbg_get_buffer();
    sprintf(tmp,"!!! rozodiag response exceeds buffer length %u/%u !!!\n",len,ruc_buf_getMaxPayloadLen(bufRef));
    severe("%s",tmp);
    uma_dbg_send(tcpCnxRef, bufRef, 1, tmp);
    return;
  }
  
  /* 
  ** Retrieve the buffer payload 
  */
  if ((pHead = (UMA_MSGHEADER_S *)ruc_buf_getPayload(bufRef)) == NULL) {
    severe( "ruc_buf_getPayload(%p)", bufRef );
    /* Let's tell the caller fsm that the message is sent */
    return;
  }
  
  /*
  ** Save this header, to initialize further header of the same response
  */
  if (!end) {
    memcpy(&uma_dbg_lastSendHeader,pHead,sizeof(UMA_MSGHEADER_S));
  }
  
  /*
  ** Set the length in the message header
  */
  pHead->len = htonl(len-sizeof(UMA_MSGHEADER_S));
  pHead->end = end;
    
  ruc_buf_setPayloadLen(bufRef,len);
  af_unix_generic_send_stream_with_idx(tcpCnxRef,bufRef);
}
/*-----------------------------------------------------------------------------
**
**   Send back a diagnostic response
**
**  @param tcpCnxRef   TCP connection reference
**  @param bufRef      reference of the received buffer that will be used to respond
**  @param end         whether this is the last buffer of the response 
**  @param string      A pre-formated string ontaining the reponse 
**
**----------------------------------------------------------------------------
*/
static inline void uma_dbg_send(uint32_t tcpCnxRef, void  *bufRef, uint8_t end, char *string) {
  char            * pHead;
  char            * pChar;

  /* 
  ** May be in a specific process such as counter reset
  ** and so do not send any thing
  */
  if (uma_dbg_do_not_send) return;

  /* 
  ** Retrieve the buffer payload 
  */
  if ((pHead = (char *)ruc_buf_getPayload(bufRef)) == NULL) {
    severe( "ruc_buf_getPayload(%p)", bufRef );
    /* Let's tell the caller fsm that the message is sent */
    return;
  }  
  
  /*
  ** Set the command recell string
  */
  pChar = uma_dbg_cmd_recall((UMA_MSGHEADER_S *)pHead);
  
  /*
  ** Add the response
  */
  pChar += rozofs_string_append(pChar,string);
  *pChar = 0;
  
  /*
  ** Send everything
  */
  uma_dbg_send_buffer(tcpCnxRef, bufRef, pChar-pHead, end);
}
/*-----------------------------------------------------------------------------
**
**   Send back a diagnostic response and dis connect
**
**  @param tcpCnxRef   TCP connection reference
**  @param bufRef      reference of the received buffer that will be used to respond
**  @param string      A pre-formated string ontaining the reponse 
**
**----------------------------------------------------------------------------
*/
static inline void uma_dbg_disconnect(uint32_t tcpCnxRef, void  *bufRef, char *string) {

  /*
  ** Send error message
  */
  uma_dbg_send(tcpCnxRef, bufRef, 1, string);

  /*
  ** Log the error
  */
  warning("%s",string);
  
  /*
  ** Disconnect the client
  */
  af_unix_delete_socket(tcpCnxRef); 
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
static inline void uma_dbg_send_format(uint32_t tcpCnxRef, void  *bufRef, uint8_t end, char *fmt, ... ) {
  va_list         vaList;
  char            *pHead;
  char            *pChar;

  /* 
  ** May be in a specific process such as counter reset
  ** and so do not send any thing
  */
  if (uma_dbg_do_not_send) return;
  
  /* Retrieve the buffer payload */
  if ((pHead = (char *)ruc_buf_getPayload(bufRef)) == NULL) {
    severe( "ruc_buf_getPayload(%p)", bufRef );
    /* Let's tell the caller fsm that the message is sent */
    return;
  }
  /*
  ** Set the command recell string
  */
  pChar = uma_dbg_cmd_recall((UMA_MSGHEADER_S *)pHead);
  
  /* 
  ** Format the string 
  */
  va_start(vaList,fmt);
  pChar += vsprintf(pChar, fmt, vaList)+1;
  va_end(vaList);

  /*
  ** Send everything
  */
  uma_dbg_send_buffer(tcpCnxRef, bufRef, pChar-pHead, end);
}
/*
   The function uma_dbg_addTopic enables to declare a new topic to
   the debug module. You have to give :

   1) topic :
   The topic is a string that has to be unic among the list of all
   topics knwon by the debug module. If you give a topic name that
   already exist, the debug module process an ERRLOG. So change
   the topic names.

   2) funct :
   A function with a specific prototype that will be called when
   the debug module receives a command beginning with the
   corresponding topic name.


   For instance : you declare a topic named "capicone"
   uma_dbg_addTopic("capicone", capicone_debug_entry);

   When on debug session is entered the following command
   line
     capicone     234 0x56    lulu         0o0
   the function capicone_debug_entry is called with

   - argv[] :
     argv[0] = "capicone"
     argv[1] = "234"
     argv[2] = "0x56"
     argv[3] = "lulu"
     argv[4] = "0o0"
     argv[5]...argv[40] = NULL

   - tcpRef : the reference of the debug session that the command
              comes from

   - bufRef : a reference a buffer in which  Y O U   H A V E  to
     put the response (note that the buffer is 2048 bytes length)

   To send a response, you are given an API uma_dbg_send which
   prototype is a bit like printf :
   - tcpCnxRef : is the reference of the debug session
   - bufRef : is the buffer for the response
   - end : is true when your response is complete, and false
           when a extra response buffer is to be sent. It is
	   up to you to get an other buffer.
   Examples :
   If you are happy
     uma_dbg_send(tcpCnxRef,bufRef,TRUE,"I am happy");
   or if you think that argv[1 has a bad value
     uma_dbg_send(tcpCnxRef,bufRef,TRUE,"2nd parameter has a bad value \"%s\"", argv[1]);
   or
     uma_dbg_send(tcpCnxRef,bufRef,TRUE,"IP addres is %u.%u.%u.%u",
                  ip>>24 & 0xFF, ip>>16 & 0xFF, ip>>8 & 0xFF, ip & 0xFF);

 */
 //64BITS typedef void (*uma_dbg_topic_function_t)(char * argv[], uint32_t tcpRef, uint32 bufRef);
typedef void (*uma_dbg_topic_function_t)(char * argv[], uint32_t tcpRef, void *bufRef);
typedef void (*uma_dbg_manual_function_t)(char * pt);


/*
** Adding a topic
*/
void uma_dbg_addTopicAndMan(char * topic, uma_dbg_topic_function_t funct, uma_dbg_manual_function_t man, uint16_t option);
// For compatibility with older interface
#define uma_dbg_addTopic_option(topic, funct, opt) uma_dbg_addTopicAndMan(topic, funct, NULL, opt);
#define uma_dbg_addTopic(topic, funct) uma_dbg_addTopicAndMan(topic, funct, NULL, 0);


void uma_dbg_hide_topic(char * topic);
void uma_dbg_init(uint32_t nbElements, uint32_t ipAddr, uint16_t serverPort, char * target) ;
void uma_dbg_init_no_system(uint32_t nbElements, uint32_t ipAddr, uint16_t serverPort, char * target) ;
//64BITS void uma_dbg_send(uint32_t tcpCnxRef, uint32 bufRef, uint8_t end, char *fmt, ... );
void uma_dbg_send_format(uint32_t tcpCnxRef, void *bufRef, uint8_t end, char *fmt, ... ); 
void uma_dbg_set_name( char * system_name) ;

//64BITS typedef uint32_t (*uma_dbg_catcher_function_t)(uint32 tcpRef, uint32 bufRef);
typedef uint32_t (*uma_dbg_catcher_function_t)(uint32_t tcpRef, void *bufRef);

//64BITS uint32_t uma_dbg_catcher_DFT(uint32 tcpRef, uint32 bufRef);
uint32_t uma_dbg_catcher_DFT(uint32_t tcpRef, void *bufRef);
void uma_dbg_setCatcher(uma_dbg_catcher_function_t funct);
/*__________________________________________________________________________
 */
/**
*  Run a system command and return the result 
*/
int uma_dbg_run_system_cmd(char * cmd, char *result, int len);

/*__________________________________________________________________________
 */
/**
*  Declare the path where to serach for core files
*/
void uma_dbg_declare_core_dir(char * path);
/*__________________________________________________________________________
*  Record syslog name
*
* @param name The syslog name
*/
void uma_dbg_record_syslog_name(char * name);
#endif
