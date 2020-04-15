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
 
#ifndef EXP_BT_PROTOCOL_H
#define EXP_BT_PROTOCOL_H

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h> 
#include <sys/socket.h>        
#include <errno.h>  
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/common_config.h>
#include <rozofs/core/ruc_buffer_api.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/common/export_track.h>

typedef enum {
  EXP_BT_NULL = 0,    /**< polling message */
  EXP_BT_TRK_READ,    /**< tracking file read */
  EXP_BT_TRK_CHECK,   /**< tracking file check to detect tracking file changes */      
  EXP_BT_DIRENT_LOAD,   /**< load a set of dirent file on a client */      
  EXP_BT_MAX
} expbt_opcode_e;


typedef struct _expbt_msg_hdr_t 
{
  uint32_t len;    /**< length of the message including the header */
  uint32_t xid;    /**< message identifier                         */
  uint32_t opcode; /**< opcode of the message                      */
  uint32_t dir;    /**< 0: request, 1: response                    */
} expbt_msg_hdr_t;

/*
**_________________________________________
    Tracking file read
**_________________________________________
*/      
typedef struct _expbt_trk_read_req_t
{
   uint32_t eid;    /**< export identifier */
   uint32_t type;    /**< type of the tracking file: either dir or regular only */
   uint32_t usr_id;  /**< slice number   */
   uint64_t file_id; /**< tracking file number */
} expbt_trk_read_req_t;


/**
*
*  Structure of the response message on the line

    +------------------------+
    |   len                  |  the message size includes the header and the commands.
    +------------------------+
    |     xid                |
    +------------------------+
    |     opcode             |
    +------------------------+
    |     dir = 1            |
    +------------------------+
    |  status = trk_len      |
    +------------------------+
    | errcode = 0            |
    +------------------------+
    | data[0]                |  Tracking file content.
    +------------------------+	           
           .....
    +------------------------+
    |  data[trk_len-1]       |
    +------------------------+	     

note: the tracking file content does not exist when status is -1
*/

typedef struct _expbt_trk_read_rsp_t
{
   int status;    /**< status of the operation: >0 success, it contains the length of the tracking file, -1 error */
   int errcode;  /**< errno value   */
   uint32_t change_count;  /**< number of time that the tracking file has been updated   */
   uint32_t alignment;
   uint64_t mtime; /**< tracking file mtime      */
} expbt_trk_read_rsp_t;



typedef struct _expbt_trk_main_rsp_t
{
   int status;    /**< status of the operation: >0 success, it contains the length of the tracking file, -1 error */
   int errcode;  /**< errno value   */
} expbt_trk_main_rsp_t;

/*
**_________________________________________
    Tracking file check
**_________________________________________
*/      


#define EXP_BT_TRK_CHECK_MAX_CMD 8  /**< max number of file tracking check in one message */

typedef struct _expbt_trk_check_req_entry_t
{
   uint32_t eid;    /**< export identifier */
   uint32_t type;    /**< type of the tracking file: either dir or regular only */
   uint32_t change_count;  /**< number of time that the tracking file has been updated   */
   uint32_t usr_id;  /**< slice number   */
   uint64_t file_id; /**< tracking file number */
   uint64_t mtime;   /**< mtime of the file    */
} expbt_trk_check_req_entry_t;


typedef struct _expbt_trk_check_req_t
{
   uint32_t nb_commands;    /**< nb of tracking file to check */
   uint32_t alignment;

} expbt_trk_check_req_t;


typedef struct _expbt_trk_check_rsp_entry_t
{
   uint32_t type;    /**< type of the tracking file: either dir or regular only */
   uint32_t usr_id;  /**< slice number   */
   uint64_t file_id; /**< tracking file number */
   int      status;   /**< status: 0 : no change, 1: mtime change, -1 error    */
   int      errcode;
} expbt_trk_check_rsp_entry_t;




typedef struct _expbt_trk_check_rsp_t
{
   uint32_t nb_responses;    /**< nb of tracking file to check */
   uint32_t alignment;

} expbt_trk_check_rsp_t;

/*
**_________________________________________
    dirent file load
**_________________________________________
*/
#define EXPBT_MAX_EXPORT_PATH 256
typedef struct _expbt_dirent_load_req_t
{
   uint32_t eid;    /**< export identifier  (needed since it is the 64 bits of the rozofs inode */
   uint32_t ipaddr;               /**< IP@ of the client  */
   uint64_t inode;            /**< inode of the directory for which the dirent files must be loaded */
   char client_export_root_path[EXPBT_MAX_EXPORT_PATH];  /**< root path on the client side                                     */    
} expbt_dirent_load_req_t;


typedef struct _expbt_dirent_load_rsp_t
{
   int status;    /**< status of the operation: >0 success, it contains the length of the tracking file, -1 error */
   int errcode;  /**< errno value   */
} expbt_dirent_load_rsp_t;

/*
**_________________________________________
   union structure for expbt
**_________________________________________
*/

typedef struct _expbt_msg_t
{
   expbt_msg_hdr_t  hdr;
   union 
   {
      struct 
      {
        expbt_trk_read_req_t read_trk;
      
      } read_trk_rq;
      struct 
      {
        expbt_trk_check_req_t cmd;
	expbt_trk_check_req_entry_t entry[EXP_BT_TRK_CHECK_MAX_CMD];
      
      } check_rq;
      struct
      {
        expbt_trk_main_rsp_t  global_rsp;
        expbt_trk_check_rsp_t rsp;
	expbt_trk_check_rsp_entry_t entry[EXP_BT_TRK_CHECK_MAX_CMD];
      } check_rsp;
      struct
      {
        expbt_trk_main_rsp_t  global_rsp;
      } min_rsp;
      struct
      {
        expbt_dirent_load_req_t req;
      } dirent_rq;      
      struct
      {
        expbt_dirent_load_rsp_t rsp;
      } dirent_rsp;      
    };
} expbt_msg_t;

#define EXP_BT_MAX_READ_RSP (sizeof(expbt_msg_hdr_t)+sizeof(expbt_trk_read_rsp_t)+(EXP_TRCK_MAX_INODE_PER_FILE)*512+4096)

#endif

