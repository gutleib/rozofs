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
#include <sys/types.h> 
#include <sys/socket.h>
#include <fcntl.h> 
#include <sys/un.h>             
#include <errno.h>  
#include <pthread.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/core/ruc_buffer_api.h>
#include <arpa/inet.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/ruc_buffer_debug.h>
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/core/rozofs_socket_family.h>
#include "expbt_global.h"
#include "expbt_north_intf.h"
#include <rozofs/rpc/expbt_protocol.h>
#include "expbt_trk_thread_intf.h"
#include "expbt_nblock_init.h"
#include <rozofs/common/expbt_inode_file_tracking.h>

DECLARE_PROFILING(expbt_profiler_t);
/*
**_________________________________________________________________
*/
/**
*    Local file tracking read: only when rozofsmount and export resides on the same node

     @param eid: export identifier
     @param type: either ROZOFS_REG or ROZOFS_DIR
     @param usr_id: slice identifier
     @param file_id: index of the file
     @param mtime: mtime value of the tracking file (returned value)
     @param change_count: number of time that file has been written (returned value)
     
     @retval 0 on success
     @retval < 0 on error
*/
int expbt_read_inode_tracking_file(uint32_t eid,uint16_t type,uint16_t usr_id,uint64_t file_id,uint64_t *mtime,uint32_t *change_count,char *buf_p)
{

   int fd;
   char filename[1024];
   struct stat statbuf;
   ssize_t ret;
   char *export_path;
   rozofs_inode_t inode;
   
   *change_count = 0;
   
   /*
   ** get the export_path
   */
   export_path = expbt_get_export_path(eid);
   if (export_path == NULL)
   {
     return -1;
   }
   
   while (1)
   {
     if (type == ROZOFS_REG)
     {
       sprintf(filename,"%s/host0/reg_attr/%d/trk_%llu",export_path,usr_id,(long long unsigned int)file_id);
       break;
     }
     if (type ==ROZOFS_DIR)
     {
       sprintf(filename,"%s/host0/dir_attr/%d/trk_%llu",export_path,usr_id,(long long unsigned int)file_id);
       break;
     }
     errno = ENOTSUP;
     return -1;
   }
   fd = open(filename,O_RDONLY);
   if (fd < 0) return -1;
   ret = fstat(fd,&statbuf);
   if (ret < 0)
   {
     close(fd);
     return -1;
   } 
   
   if (buf_p == NULL)
   {
     /*
     ** it was just a mtime check
     */
     *mtime = statbuf.st_mtime;
     close(fd);
     return 0;
   }
   /*
   ** Attempt to read the file
   */ 
   ret = pread( fd, buf_p, statbuf.st_size, 0);
   if (ret != statbuf.st_size)
   {
     /*
     ** we might need to check the length since it might be possible that one inode has been added or remove
     */
     close(fd);
     return -1;        
   }
   *mtime = statbuf.st_mtime;
   close(fd);
   /*
   ** get the change count for the tracking file
   */
   inode.s.eid = eid;
   inode.s.usr_id = usr_id;
   inode.s.file_id = file_id;
   inode.s.key = type;
   /*
   ** do care about the index
   */
   inode.s.idx = 0;      
   *change_count = expbt_track_inode_get_counter(&inode,1);
   return statbuf.st_size;
}


/*
**_________________________________________________________________
*/ 
/**
  Post a request for tracking file check towards the command ring of the tracking threads
  
  @param arg: pointer to the header of the request
  @param  rozorpc_srv_ctx_p : pointer to the RPC context that contains the received buffer and the command to execute
  
  @retval none
*/
void expbt_trk_check_post2thread(void *arg, rozorpc_srv_ctx_t *rozorpc_srv_ctx_p)
{

  EXPBT_START_PROFILING(file_check);  
  /*
  ** put the message in the ring buffer
  */
  af_unix_trk_pending_req_count++;
  rozofs_queue_put(&expbt_trk_cmd_ring,rozorpc_srv_ctx_p);
}
/*
**_________________________________________________________________
*/
/**
   check the content of a tracking file: it tracks the mtime of the tracking file
   That function is called from a tracking file reading thread
   It reads the tracking file found in the request received from the client and format the response ready to send back to the main thread
   
   @param rozorpc_srv_ctx_p: RPC context associated with the request
   
   @retval none
*/
void expb_trk_check_in_thread(rozorpc_srv_ctx_t *rozorpc_srv_ctx_p)
{

   int ret;
   expbt_msg_t *rsp_msg_p;
   expbt_msg_t *req_p;
   expbt_trk_check_req_entry_t *entry_rq_p;
   expbt_trk_check_rsp_entry_t *entry_rsp_p;
   uint64_t mtime;
   int i;
   uint32_t returned_size;
   uint32_t change_count;
   
   
   rsp_msg_p = (expbt_msg_t*)  ruc_buf_getPayload(rozorpc_srv_ctx_p->recv_buf);

   
   req_p = (expbt_msg_t*)ruc_buf_getPayload(rozorpc_srv_ctx_p->decoded_arg); 
   if (req_p->check_rq.cmd.nb_commands > EXP_BT_TRK_CHECK_MAX_CMD) 
   {
     errno = ERANGE;
     goto error;
   }
   
   entry_rq_p  = &req_p->check_rq.entry[0];
   entry_rsp_p = &rsp_msg_p->check_rsp.entry[0];
   rsp_msg_p->check_rsp.rsp.nb_responses = req_p->check_rq.cmd.nb_commands;
   
   for (i = 0; i < req_p->check_rq.cmd.nb_commands; i++,entry_rq_p++,entry_rsp_p++)
   {
     entry_rsp_p->type = entry_rq_p->type;
     entry_rsp_p->usr_id = entry_rq_p->usr_id;
     entry_rsp_p->file_id = entry_rq_p->file_id;
            
     ret = expbt_read_inode_tracking_file(entry_rq_p->eid,(uint16_t)entry_rq_p->type,(uint16_t)entry_rq_p->usr_id,entry_rq_p->file_id,&mtime,&change_count,NULL);
     if (ret < 0)
     {
      entry_rsp_p->status = -1;
      entry_rsp_p->errcode = errno;  
      continue;     
     }
     entry_rsp_p->errcode = 0;
//     info("FDL mtime (local/remote): %llu/%llu change_count: %d/%d",(long long unsigned int) mtime ,(long long unsigned int) entry_rq_p->mtime,(int)change_count,(int) entry_rq_p->change_count);
     if (mtime ==  entry_rq_p->mtime) entry_rsp_p->status = 0;
     else entry_rsp_p->status = 1;
   }
   /*
   ** udpate the header of the response and the global status
   */
   memcpy(&rsp_msg_p->hdr,&req_p->hdr,sizeof(expbt_msg_hdr_t));   
   rsp_msg_p->check_rsp.global_rsp.status = 0;
   rsp_msg_p->check_rsp.global_rsp.errcode = 0;
   rsp_msg_p->hdr.dir = 1;
   rsp_msg_p->hdr.len = 0;   
   returned_size = sizeof(expbt_msg_hdr_t)+sizeof(expbt_trk_main_rsp_t)+sizeof(expbt_trk_check_rsp_t) + rsp_msg_p->check_rsp.rsp.nb_responses*sizeof(expbt_trk_check_rsp_entry_t);   
   rsp_msg_p->hdr.len = returned_size-sizeof(uint32_t);   

   /*
   ** send back the context towards the main thread by using the af_unix socket
   */
out:
   /*
   ** send back the context towards the main thread by using the af_unix socket
   */
   ruc_buf_setPayloadLen(rozorpc_srv_ctx_p->recv_buf,rsp_msg_p->hdr.len+sizeof(uint32_t));     
   expbt_send2mainthread(rozorpc_srv_ctx_p);
   return;
   
error:
   memcpy(&rsp_msg_p->hdr,&req_p->hdr,sizeof(expbt_msg_hdr_t));   
   rsp_msg_p->min_rsp.global_rsp.status = -1;
   rsp_msg_p->min_rsp.global_rsp.errcode = errno;   
   rsp_msg_p->hdr.dir = 1;
   rsp_msg_p->hdr.len = 0;
   returned_size = sizeof(expbt_msg_hdr_t)+sizeof(expbt_trk_main_rsp_t)+sizeof(expbt_trk_check_rsp_t) -sizeof(uint32_t); 
   goto out;  
}   


/*
**_________________________________________________________________
*/ 
/**
  Post a request for tracking file read towards the command ring of the tracking threads
  
  @param arg: pointer to the header of the request
  @param  rozorpc_srv_ctx_p : pointer to the RPC context that contains the received buffer and the command to execute
  
  @retval none
*/
void expbt_trk_read_post2thread(void *arg, rozorpc_srv_ctx_t *rozorpc_srv_ctx_p)
{
  
  EXPBT_START_PROFILING(file_read);
  /*
  ** put the message in the ring buffer
  */
  af_unix_trk_pending_req_count++;
  rozofs_queue_put(&expbt_trk_cmd_ring,rozorpc_srv_ctx_p);
}

/*
**_________________________________________________________________
*/
/**
   Read the content of a tracking file
   That function is called from a tracking file reading thread
   It reads the tracking file found in the request received from the client and format the response ready to send back to the main thread
   
   @param rozorpc_srv_ctx_p: RPC context associated with the request
   
   @retval none
*/
void expb_trk_read_in_thread(rozorpc_srv_ctx_t *rozorpc_srv_ctx_p)
{

   expbt_msg_hdr_t   *hdr_p,*hdr_rsp_p;
   expbt_trk_read_req_t *cmd_p;
   expbt_trk_read_rsp_t *rsp_p;
   char *data_p;
   int ret;
   uint32_t returned_size;
   
   hdr_rsp_p =  (expbt_msg_hdr_t*)ruc_buf_getPayload(rozorpc_srv_ctx_p->recv_buf);
   rsp_p     = (expbt_trk_read_rsp_t*) (hdr_rsp_p +1);
   data_p    = (char *) (rsp_p+1);
   
   hdr_p = (expbt_msg_hdr_t*)ruc_buf_getPayload(rozorpc_srv_ctx_p->decoded_arg);   
   cmd_p = (expbt_trk_read_req_t*)(hdr_p+1);
   
   ret = expbt_read_inode_tracking_file(cmd_p->eid,(uint16_t)cmd_p->type,(uint16_t)cmd_p->usr_id,cmd_p->file_id,&rsp_p->mtime,&rsp_p->change_count,data_p);
   if (ret >= 0)
   {
//     info("FDL expbt cache mtime %llu",(long long unsigned int)rsp_p->mtime);
     memcpy(hdr_rsp_p,hdr_p,sizeof(expbt_msg_hdr_t));
     hdr_rsp_p->dir = 1;
     rsp_p->status = ret;
     rsp_p->errcode = 0;
     hdr_rsp_p->len = 0;
     returned_size = sizeof(expbt_msg_hdr_t)+sizeof(expbt_trk_read_rsp_t)+ret;
     hdr_rsp_p->len = returned_size-sizeof(uint32_t);
     ruc_buf_setPayloadLen(rozorpc_srv_ctx_p->recv_buf,hdr_rsp_p->len+sizeof(uint32_t));   
   }
   else
   {
     memcpy(hdr_rsp_p,hdr_p,sizeof(expbt_msg_hdr_t));
     hdr_rsp_p->dir = 1;
     hdr_rsp_p->len = 0;
     rsp_p->status = -1;
     rsp_p->errcode = errno;
     returned_size = sizeof(expbt_msg_hdr_t)+sizeof(expbt_trk_read_rsp_t);     
     hdr_rsp_p->len = returned_size-sizeof(uint32_t);
     ruc_buf_setPayloadLen(rozorpc_srv_ctx_p->recv_buf,hdr_rsp_p->len+sizeof(uint32_t));      
   }
   /*
   ** send back the context towards the main thread by using the af_unix socket
   */
   expbt_send2mainthread(rozorpc_srv_ctx_p);
   
}   


/*
**_________________________________________________________________
*/ 
/**
  Post a request for dirent files loading to the command ring of the tracking threads
  
  @param arg: pointer to the header of the request
  @param  rozorpc_srv_ctx_p : pointer to the RPC context that contains the received buffer and the command to execute
  
  @retval none
*/
void expbt_dirent_load_post2thread(void *arg, rozorpc_srv_ctx_t *rozorpc_srv_ctx_p)
{
  
  EXPBT_START_PROFILING(load_dentry);
  /*
  ** put the message in the ring buffer
  */
  af_unix_trk_pending_req_count++;
  rozofs_queue_put(&expbt_trk_cmd_ring,rozorpc_srv_ctx_p);
}


/*
**_________________________________________________________________
*/ 


/*
**__________________________________________________________________
*/
/** build a full path based on export root and fid of the lv2 file
 *
 * lv2 is the second level of files or directories in storage of metadata
 * they are acceded thru mreg or mdir API according to their type.
 *
 * @param root_path: root path of the exportd
 * @param fid: the fid we are looking for
 * @param path: the path to fill in
 */

static inline int export_lv2_resolve_path_internal(char *root_path, fid_t fid, char *path) {
    uint32_t slice;
    uint32_t subslice;
    char str[37];
    fid_t fid_save;
   
    /*
    ** clear the delete pending bit for path resolution
    */
    memcpy(fid_save,fid,sizeof(fid_t));
    exp_metadata_inode_del_deassert(fid_save);
    /*
     ** extract the slice and subsclie from the fid
     */
    mstor_get_slice_and_subslice(fid_save, &slice, &subslice);
    /*
     ** convert the fid in ascii
     */
    rozofs_uuid_unparse(fid_save, str);
    sprintf(path, "%s/%d/%s", root_path, slice, str);
    return 0;

    return -1;
}

/*
**_______________________________________________________________________________________
*/

int expbt_rsync_for_load_dirent(int eid, uint64_t inode,uint32_t ipaddr,char *remote_path)
{

  struct sockaddr_in sa;
  char str[INET_ADDRSTRLEN];
  char *src_path1;
  char buffer_cmd[1024];  
  rozofs_inode_t fake_inode;
  char src_path[512];
  char dst_path[512];
  int ret;
  
  src_path1 = expbt_get_export_path(eid);
  if (src_path1 == NULL)
  {
    errno = ENOENT;
    return -1;
  }
  fake_inode.fid[0] = 0;
  fake_inode.fid[1] = inode;  
  fake_inode.s.eid = eid;
  sa.sin_addr.s_addr = (unsigned long)ipaddr;
  
  export_lv2_resolve_path_internal(expbt_get_export_path(eid),(uint8_t *)(&fake_inode.fid[0]),src_path);
  export_lv2_resolve_path_internal(remote_path,(uint8_t *)(&fake_inode.fid[0]),dst_path);
  



  // now get it back and print it
  inet_ntop(AF_INET, &(sa.sin_addr), str, INET_ADDRSTRLEN);
  sprintf(buffer_cmd,"rsync -d -lptgoD -P -q --delete-before  --update %s/ --rsync-path=\"mkdir -p %s/ && rsync\" root@%s:%s/",src_path,dst_path,str,dst_path);

  info("FDL rsync_cmd : %s",buffer_cmd);
  ret = system(buffer_cmd);
  return ret;


}

/*
**_________________________________________________________________
*/
/**
   Read the content of a tracking file
   That function is called from a tracking file reading thread
   It reads the tracking file found in the request received from the client and format the response ready to send back to the main thread
   
   @param rozorpc_srv_ctx_p: RPC context associated with the request
   
   @retval none
*/
void expbt_load_dentry_in_thread(rozorpc_srv_ctx_t *rozorpc_srv_ctx_p)
{

   expbt_msg_hdr_t   *hdr_p,*hdr_rsp_p;
   expbt_dirent_load_req_t *cmd_p;
   expbt_dirent_load_rsp_t *rsp_p;
   int ret;
   uint32_t returned_size;

   
   hdr_rsp_p =  (expbt_msg_hdr_t*)ruc_buf_getPayload(rozorpc_srv_ctx_p->recv_buf);
   rsp_p     = (expbt_dirent_load_rsp_t*) (hdr_rsp_p +1);
   
   hdr_p = (expbt_msg_hdr_t*)ruc_buf_getPayload(rozorpc_srv_ctx_p->decoded_arg);   
   cmd_p = (expbt_dirent_load_req_t*)(hdr_p+1);
   
   ret = expbt_rsync_for_load_dirent(cmd_p->eid,cmd_p->inode,cmd_p->ipaddr,cmd_p->client_export_root_path);
   if (ret >= 0)
   {
     memcpy(hdr_rsp_p,hdr_p,sizeof(expbt_msg_hdr_t));
     hdr_rsp_p->dir = 1;
     rsp_p->status = ret;
     rsp_p->errcode = 0;
     hdr_rsp_p->len = 0;
     returned_size = sizeof(expbt_msg_hdr_t)+sizeof(expbt_dirent_load_rsp_t);
     hdr_rsp_p->len = returned_size-sizeof(uint32_t);
     ruc_buf_setPayloadLen(rozorpc_srv_ctx_p->recv_buf,hdr_rsp_p->len+sizeof(uint32_t));   
   }
   else
   {
     memcpy(hdr_rsp_p,hdr_p,sizeof(expbt_msg_hdr_t));
     hdr_rsp_p->dir = 1;
     hdr_rsp_p->len = 0;
     rsp_p->status = -1;
     rsp_p->errcode = errno;
     returned_size = sizeof(expbt_msg_hdr_t)+sizeof(expbt_dirent_load_rsp_t);     
     hdr_rsp_p->len = returned_size-sizeof(uint32_t);
     ruc_buf_setPayloadLen(rozorpc_srv_ctx_p->recv_buf,hdr_rsp_p->len+sizeof(uint32_t));      
   }
   /*
   ** send back the context towards the main thread by using the af_unix socket
   */
   expbt_send2mainthread(rozorpc_srv_ctx_p);
   
}   
