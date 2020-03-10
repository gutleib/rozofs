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
#ifndef _ROZOFS_BT_API_H
#define _ROZOFS_BT_API_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <fcntl.h> 
#include <sys/un.h>             
#include <errno.h>  
/**
*
*  Structure of the message on the line

    +------------------------+
    |   msg_size             |  the message size includes the header and the commands.
    +------------------------+
    |     opcode             |
    +------------------------+
    |     nb_commands        |
    +------------------------+
    |  caller                |
    +------------------------+
           .....
    +------------------------+
    |  cmd 1                 |
    +------------------------+	           
           .....
    +------------------------+
    |  cmd n                 |
    +------------------------+	     
*/
typedef enum {
  ROZO_BATCH_INIT = 0,
  ROZO_BATCH_READ,
  ROZO_BATCH_WRITE,
  ROZO_BATCH_GETATTR,
  ROZO_BATCH_MEMREG,
  ROZO_BATCH_MEMUNREG,
  ROZO_BATCH_MEMADDR_REG,
  ROZO_BATCH_MEMCREATE,
      
  ROZO_BATCH_MAX
} rozofs_batch_opcode_e;

#define ROZOFS_MAX_BATCH_CMD 32

typedef struct _rozo_batch_hdr_t
{
   uint32_t msg_sz;  /**< message size */
   uint16_t opcode;
   uint16_t nb_commands;
   void *caller;     /**< opaque reference provided by the caller */
   uint64_t private;
} rozo_batch_hdr_t;
/*
**  paylaod of a ROZOFS_BATCH_READ or ROZOFS_BATCH_WRITE
*/
typedef struct _rozo_io_cmd_t
{
    void *data;    /* io callback */
    uint16_t prio;
    uint64_t inode;  /**< rozofs lower inode part */
    uint64_t offset; /**< offset in file          */
    uint32_t length; /**< length to read or write */
    uint32_t rozofs_shm_ref; /**< reference of the share memory within rozofs */
    uint8_t     *buf;           /**< pointer to the buffer on the caller side */
} rozo_io_cmd_t;


/*
**  paylaod of a ROZOFS_BATCH_GETATTR
*/
typedef struct _rozo_attr_cmd_t
{
    void *data;    /* io callback */
    uint16_t prio;
    uint64_t inode;  /**< rozofs lower inode part */
    uint64_t offset; /**< offset in file          */
    uint32_t length; /**< length to read or write */
    uint32_t rozofs_shm_ref; /**< reference of the share memory within rozofs */
    uint8_t     *buf;           /**< pointer to the buffer on the caller side */
} rozo_attr_cmd_t;

//1234567890123456789012345678901234567890
/*
**  paylaod of a ROZOFS_BATCH_MEMREG
*/
#define ROZOFS_MAX_SHARED_NAME 32
typedef struct _rozo_memreg_cmd_t
{
    void *data;    /* io callback */
    char name[ROZOFS_MAX_SHARED_NAME];
    uint32_t rozofs_shm_ref; /**< reference of the share memory within rozofs */
    uint64_t length;         /**< length to read or write */
    void     *buf;           /**< pointer to the shared memory the caller side */
} rozo_memreg_cmd_t;

#define ROZOFS_BT_MAX_PATHNAME 1024
typedef struct _rozo_init_cmd_t
{
    char export_path[ROZOFS_BT_MAX_PATHNAME];
    int eid; /**< export identifier */
} rozo_init_cmd_t;

/*
** response entry
*/
typedef struct _rozo_batch_res_t
{
  void *data;      /**< opaque data of the caller */
  int   status;    /**< global status: 0 OK, -1 error */
  uint64_t   size; /**< size of status is O or errno value */
} rozo_io_res_t;

typedef union
{
  uint32_t u32;
  struct
  {
    uint16_t alloc_idx;
    uint16_t ctx_idx;
  } s;
} rozofs_mem_key;

#define ROZOFS_SHARED_MEMORY_REG_MAX 128
typedef struct _rozofs_memreg_t
{
   char *name;                 /**< name of the shared memory */      
   rozofs_mem_key rozofs_key;  /**< rozofs share memory key */
   size_t length;        /**< shared memory length   */
   uint32_t lock;        /**< lock count */
   int      owner;       /**< owner of the shared memory : it is the reference of the socket */
   int      fd;          /**< file descriptor  */
   int      fd_share;    /**< file decriptor of the shared memory */
   uint8_t     *addr;
   uint8_t     *remote_addr;  /**< address of the owner */
} rozofs_memreg_t;  
/*
** structure used by the sharemem between rozofsmount & storcli to keep track of the user share memory registration
*/
#define ROZOFS_BT_MAX_SHM_NAME 64
#define ROZOFS_BT_MAX_STORCLI 32 /**< max number of storcli */

typedef struct _rozofs_stc_memreg_t
{
   char name[ROZOFS_BT_MAX_SHM_NAME];   /**< name of the shared memory */      
   rozofs_mem_key rozofs_key;           /**< rozofs share memory key */
   size_t length;        /**< shared memory length   */
   uint8_t     *remote_addr;            /**< address of the owner: application */
   int          delete_req;             /**< assert to 1 when rozofsmount wants to delete a share memory that it has created */
   uint8_t      storcli_ack[ROZOFS_BT_MAX_STORCLI];            /**< assert to 1 by the storcli once the memory has been deleted                     */             
   uint8_t      storcli_done[ROZOFS_BT_MAX_STORCLI];            /**< assert to 1 by the storcli once the memory has been mapped internally          */             
} rozofs_stc_memreg_t; 

rozofs_memreg_t *rozofs_shm_tb;
rozofs_memreg_t *rozofs_bt_stc_mem_p;  /**< pointer to the shared memory that has been created by rozofsmount to handle user shared memory */
int rozofs_shm_count;

typedef struct _rozofs_bt_rsp_buf 
{
  rozo_batch_hdr_t  hdr;
  rozo_io_res_t     res[ROZOFS_MAX_BATCH_CMD];
} rozofs_bt_rsp_buf;



/*
**_________________________________________________________________________________________________
*/
/**
   get a shared memory context
   
   @param key: name of the shared memory

   
   @retval <>NULL : pointer to the share memory context
   @retval NULL: not found (see errno for details)
   
*/
rozofs_memreg_t *rozofs_shm_lookup(uint32_t key);

#endif
