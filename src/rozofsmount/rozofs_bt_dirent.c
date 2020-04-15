
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
#include <stdint.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <fcntl.h> 
#include <sys/un.h>             
#include <errno.h> 
#include "rozofs_bt_inode.h"
#include <sys/stat.h>
#include <unistd.h>
#include <rozofs/rozofs.h>
#include "rozofs_bt_api.h"
#include "rozofs_bt_proto.h"
#include <rozofs/common/log.h>
#include <rozofs/core/ruc_buffer_api.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/core/ruc_buffer_debug.h>
#include <rozofs/common/export_track.h>
#include <rozofs/common/mattr.h>
#include <rozofs/core/rozofs_queue.h>
#include <rozofs/core/rozofs_queue_pri.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include "rozofsmount.h"
#include "../exportd/mdirent.h"
#include "rozofs_bt_dirent.h"


#define ROZOFS_BT_DIRENT_QUEUE_LENGTH 1024



typedef struct _rozofs_bt_dirent_priv_ctx_t
{
   uint32_t ipaddr; /**< IP@ of the client  */
   int      eid;    /**< export identifier  */
   char *local_export_path;
   void *buffer_pool;  /**< ruc buffer pool reference */
} rozofs_bt_dirent_priv_ctx_t;


typedef struct _rozofs_bt_inode_pending_t {
   list_t               list; /**< link list of the pending requests asking for the loading of the directory or file attributes */
   uint64_t             inode; /**< inode for which we are waiting data */
   rozofs_bt_track_key_t file_key; /**< file key of the inode: used fro the case of the regular file only */
} rozofs_bt_inode_pending_t;
/*
** Global data
*/

rozofs_bt_thread_ctx_t dirent_thread_ctx;  /**< dirent file load thread context */
int dirent_thread_ready = 0;
rozofs_queue_prio_t dirent_queue;  /**< dirent file load queue (CMD & RSP) */
void *rozofs_bt_dirent_buffer_pool_p = NULL;
int rozofs_bt_dirent_mtime_valid = 1;   /**< use the mtime of the directory for root bitmap mtime when asserted to 1; when its 0, uses the update time (utime) */

rozofs_bt_track_key_t *rozofs_pending_inode_tab_p = NULL;
int rozofs_bt_nb_buffers = -1;
list_t pending_inode_head; /**< head of the pending inode list */
rozofs_bt_dirent_stats *dirent_stats_p = NULL;


/**
**_______________________________________________________________________________________
*/
/**
*  attempt to get the inode attribute

   @param inode: inode value
   @param tracking_ret_p: pointer to the tracking entry (might be NULL)
   @param dirent_valid: pointer to an array where the service indicates if dirent are valid (0: invalid, 1: valid) (for directory only)
      
   @retval <> NULL: the inode attributes are valid, tracking cache entry available if provided
   @retval NULL: inode attributes not available (see errno for detail
   
   EAGAIN: the loading of the tracking file (and dirent file) are in progress
   ENOTSUP: not supported
   ENOENT: the entry does not exist
   other: 
*/
ext_mattr_t *rozofs_bt_load_dirent_from_main_thread(uint64_t inode,rozofs_bt_tracking_cache_t **tracking_p,int *dirent_valid)
{

   ext_mattr_t * ext_attr_p;
   dentry_btmap_t *bt_map_p;
   rozofs_inode_t  rozofs_inode; 
   ext_dir_mattr_t *ext_dir_mattr_p;
   time_t cur_time;
   uint64_t time2check;
   
    *dirent_valid = 0;
   if (dirent_thread_ready == 0) {
     errno = ENOTSUP;
     return NULL;
   }
   /*
   ** normalize the inode value
   */
   inode = rozofs_bt_inode_normalize(inode);
   /*
   ** check if the inode can be found in the tracking file cache
   */
   ext_attr_p = rozofs_bt_lookup_inode_internal_with_tracking_entry(inode,tracking_p);
   if (ext_attr_p == NULL)
   {
     ROZOFS_BT_DIRENT_STATS_INO(lookup_inode_miss,inode);
     if (errno == EAGAIN)
     {
       rozofs_queue_put_prio(&dirent_queue,(void*)inode,PRIO_CMD);
       errno = EAGAIN;
       return NULL;
     }
     /*
     ** use the errno asserted by rozofs_bt_lookup_inode_internal()
     */
     return NULL;     
   }
   /*
   ** the inode context has been found
   */
   ROZOFS_BT_DIRENT_STATS_INO(lookup_inode_hit,inode);
   /*
   ** check if the root bitmap has been loaded in the cache
   ** if it is not the case, we force it. This is only true for the case of directory
   */
   rozofs_inode.fid[0] = 0;
   rozofs_inode.fid[1] = inode;
   if (rozofs_inode.s.key != ROZOFS_DIR) {
     errno = 0;
     return ext_attr_p;
   }
   /*
   ** it is directory
   */
   cur_time = time(NULL);
   ext_dir_mattr_p = (ext_dir_mattr_t*)ext_attr_p->s.attrs.sids;
   rozofs_inode.s.eid = rozofs_bt_dirent_eid;
   bt_map_p = get_btmap_entry_by_fid_external((unsigned char *) &rozofs_inode.fid[0]);
   if (bt_map_p == NULL)
   {
     ROZOFS_BT_DIRENT_STATS_DIR(lookup_bitmap_miss);
     if ((ext_dir_mattr_p->s.update_time + ROZOFS_BT_DIRENT_GUARD_DELAY) < cur_time)
     {
       rozofs_queue_put_prio(&dirent_queue,(void*)inode,PRIO_CMD);
     }
     /*
     ** directory change is too recent to trigger the reload of the dirent files
     */
     errno = 0;    
     return ext_attr_p;       
   }
   /*
   ** OK, we have it, check if the mtime of the bitmap matches the update_time of the directory
   */
   ROZOFS_BT_DIRENT_STATS_DIR(lookup_bitmap_hit);
   time2check = (rozofs_bt_dirent_mtime_valid!=0)?ext_attr_p->s.attrs.mtime:ext_dir_mattr_p->s.update_time;
   if (bt_map_p->mtime != time2check)
   {
     ROZOFS_BT_DIRENT_STATS_DIR(lookup_dir_bad_mtime);
     if ((ext_dir_mattr_p->s.update_time + ROZOFS_BT_DIRENT_GUARD_DELAY) < cur_time)
     {
       rozofs_queue_put_prio(&dirent_queue,(void*)inode,PRIO_CMD);
     }
     /*
     ** directory change is too recent to trigger the reload of the dirent files
     */    
     errno = 0;    
     return ext_attr_p;   
   } 
   /*
   ** dirent and attributes are valid
   */
   *dirent_valid = 1;
   errno = 0;
   return ext_attr_p;    

}


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
**______________________________________________________________________
*/


int rozofs_bt_check_mdirent_validity(rozofs_bt_thread_ctx_t * thread_ctx_p, ext_mattr_t * ext_attr_p)
{
  
  rozofs_bt_dirent_priv_ctx_t *priv_p;  
  char src_path[512];
  struct stat statbuf;
  int ret;
  dentry_btmap_t *btmap_p;
  rozofs_inode_t *rozofs_inode_p;
  ext_dir_mattr_t *ext_dir_mattr_p = (ext_dir_mattr_t*)ext_attr_p->s.attrs.sids;  
  uint64_t time2check;
  
  time2check = (rozofs_bt_dirent_mtime_valid!=0)?ext_attr_p->s.attrs.mtime:ext_dir_mattr_p->s.update_time;
  
//  struct utimbuf  time;
   rozofs_inode_p =(rozofs_inode_t*) ext_attr_p->s.attrs.fid;
   priv_p = thread_ctx_p->private;
   
   export_lv2_resolve_path_internal(priv_p->local_export_path,ext_attr_p->s.attrs.fid,src_path);
   
   ret = stat(src_path,&statbuf);
   if (ret < 0)
   {
     return ret;
   }
   /*
   ** check the mtime
   */
   if (statbuf.st_mtime != time2check)
   {
      FDL_INFO("FDL mtime does not match %llu/%llu",(unsigned long long int)statbuf.st_mtime,(unsigned long long int)time2check);
      return -1;
   }
   /*
   ** check if the information in the root bitmap cache is valid
   ** if the cache entry is empty or the mtime is different we should reload it
   */
   btmap_p = get_btmap_entry_by_fid_external(ext_attr_p->s.attrs.fid);
   if (btmap_p == NULL)
   {
     /*
     ** the file is OK, but not in the cache
     */
     ret =  rozofs_bt_load_dirent_root_bitmap_file(rozofs_inode_p->fid[1],time2check);
     if (ret < 0)
     {
       warning ("FDL error while loading dirent root bitmap:%s",strerror(errno));
     }
     return ret;
   }
   if (btmap_p->mtime != time2check)
   {
     return -1;
   }   
   FDL_INFO("FDL mtime match for %s",src_path);
   return 0;
}


/*
**______________________________________________________________________
*/


int rozofs_bt_mdirent_set_dir_mtime(rozofs_bt_thread_ctx_t * thread_ctx_p,ext_mattr_t * ext_attr_p,uint64_t time2check)
{
  
   rozofs_bt_dirent_priv_ctx_t *priv_p;  
   char src_path[512];
   int ret;  
   struct utimbuf  new_time;

   priv_p = thread_ctx_p->private;
   
   new_time.actime = time(NULL);
   new_time.modtime = time2check;
   
   export_lv2_resolve_path_internal(priv_p->local_export_path,ext_attr_p->s.attrs.fid,src_path);
   FDL_INFO("FDL change mtime of %s to %llu\n",src_path,(unsigned long long int)new_time.modtime);
   ret = utime(src_path,&new_time);
   if (ret < 0)
   {
     FDL_INFO("FDL error on %s: %s\n",src_path,strerror(errno));
     return ret;
   }
   return 0;
}
/*
 *_______________________________________________________________________
 */
/**
*  That function is called when a message is received on the PRIO_CMD level of the queue

  @param thread_ctx_p: thread context
  @param inode: inode of the directory for which we want to load the dirent files on the local server
  @param buf: reference of a ruc_buffer or NULL (it is not null when it is called from the procedure that processes the response of the tracking file loading)

  @retval none  
*/

int rozofs_bt_trigger_read_trk_file_for_directory_from_dirent_thread(rozofs_bt_thread_ctx_t * thread_ctx_p,uint64_t inode);

void rozofs_bt_dirent_load_request(rozofs_bt_thread_ctx_t * thread_ctx_p,uint64_t inode,void *buf)
{
   rozofs_bt_dirent_priv_ctx_t *priv_p;   
   void *ruc_buffer;
   expbt_msgint_full_t *msg_th_p;
   uint64_t message;
   int ret;
   ext_mattr_t * ext_attr_p;
   rozofs_inode_t *fake_inode_p;
   rozofs_bt_inode_pending_t *rozofs_bt_inode_pending_p;
   list_t * p;   
   rozofs_bt_track_key_t file_key;
   time_t cur_time;
   ext_dir_mattr_t *ext_dir_mattr_p = NULL;  
   
   ruc_buffer = buf;
   /*
   ** normalize the inode
   */
   inode = rozofs_bt_inode_normalize(inode);
   /*
   ** get the key of the inode
   */
   file_key = rozofs_bt_inode_get_filekey_from_inode(inode);
   
   if (buf == NULL) {
      ROZOFS_BT_DIRENT_STATS_INO(get_inode_attempts,inode);
      /*
      ** normalize the inode value
      */
      if (rozofs_bt_dirent_load_debug) 
      {
	 char fid_str[64];
	 rozofs_inode_t rozofs_inode;

	 rozofs_inode.fid[0] = 0;
	 rozofs_inode.fid[1] = inode;
	 rozofs_inode.s.eid = rozofs_bt_dirent_eid;
	 rozofs_uuid_unparse((unsigned char*) &rozofs_inode.fid[0],fid_str);      
	 info("inode %llx - @rozofs_uuid@%s\n",(unsigned long long int) inode,fid_str);
      }
   }
   /*
   ** check if there is pending request for the same inode: if it is the case, we stop here
   */
   list_for_each_forward(p, &pending_inode_head) 
   {
     rozofs_bt_inode_pending_p = list_entry(p, rozofs_bt_inode_pending_t, list);
     if ((rozofs_bt_inode_pending_p->file_key.s.key == ROZOFS_REG) && (file_key.s.key == ROZOFS_REG))
     {
        if (rozofs_bt_inode_pending_p->file_key.u64 != file_key.u64) continue;
        /*
	** there is already of request for that inode
	*/
	ROZOFS_BT_DIRENT_STATS_INO(get_inode_duplicated,inode);
	if (ruc_buffer != NULL) ruc_buf_freeBuffer(ruc_buffer); /* that should not occur */
	return;		
     
     }
     if (rozofs_bt_inode_pending_p->inode == inode)
     {
        /*
	** there is already of request for that inode
	*/
	ROZOFS_BT_DIRENT_STATS_INO(get_inode_duplicated,inode);
	if (ruc_buffer != NULL) ruc_buf_freeBuffer(ruc_buffer); /* that should not occur */
	return;	
     }
   }	
   priv_p = thread_ctx_p->private;
   /*
   ** Check if we can the inode context
   */
   /*
   ** check if the inode can be found in the tracking file cache
   */
   ext_attr_p = rozofs_bt_lookup_inode_internal( inode);
   if (ext_attr_p == NULL)
   {
      if (errno == EAGAIN) goto request_file_tracking_load;
      /*
      ** nothing more to do here : either ENOENT or other error
      */
      return;
   }
   /**
   * stop here for regular file
   */
   fake_inode_p= (rozofs_inode_t*) ext_attr_p->s.attrs.fid;
   if (fake_inode_p->s.key != ROZOFS_DIR) 
   {
     if (ruc_buffer != NULL) ruc_buf_freeBuffer(ruc_buffer); 
     return;     
   }
   /*
   ** it is a directory: check the presence and the mtime of the mdirent directory
   */
   ROZOFS_BT_DIRENT_STATS_DIR(check_dirent_root_valid);
   if (rozofs_bt_check_mdirent_validity(thread_ctx_p,ext_attr_p) == 0) 
   {
     if (ruc_buffer != NULL) ruc_buf_freeBuffer(ruc_buffer);
     ROZOFS_BT_DIRENT_STATS_DIR(check_dirent_root_valid_ok);
     return;
   }
   /*
   ** Allocate ressources for loading the dirent file if not provided
   */  
   ROZOFS_BT_DIRENT_STATS_DIR(check_dirent_root_valid_nok);
   /*
   ** if it too early for dirent loading then return
   */
   cur_time = time(NULL);
   ext_dir_mattr_p = (ext_dir_mattr_t*)ext_attr_p->s.attrs.sids;
   /*
   ** check if it is the good time for loading the dirent files
   */
   if ((ext_dir_mattr_p->s.update_time+ROZOFS_BT_DIRENT_GUARD_DELAY) > cur_time)
   {
      ROZOFS_BT_DIRENT_STATS_DIR(check_dirent_too_early);
     if (ruc_buffer != NULL) ruc_buf_freeBuffer(ruc_buffer);
     return;      
   }
   /*
   ** OK, let's try to load the dirent files
   */
   if (ruc_buffer == NULL)
   { 
     ruc_buffer = ruc_buf_getBuffer(priv_p->buffer_pool); 
     if (ruc_buffer == NULL) 
     {
       fatal("dirent buffer command pool is empty");
       return ;
     }
   }
   ROZOFS_BT_DIRENT_STATS_DIR(get_dirent_file_attempts);
   msg_th_p = (expbt_msgint_full_t*)ruc_buf_getPayload(ruc_buffer);
   rozofs_bt_inode_pending_p = (rozofs_bt_inode_pending_t*)(msg_th_p+1);
   /*
   ** insert the inode in the pending list
   */
   list_init(&rozofs_bt_inode_pending_p->list);
   rozofs_bt_inode_pending_p->inode = inode;
   rozofs_bt_inode_pending_p->file_key = rozofs_bt_inode_get_filekey_from_inode(inode);
   list_push_back(&pending_inode_head,&rozofs_bt_inode_pending_p->list);
   /*
   ** fill up the request
   */
   msg_th_p->hdr.xid = 0;
   msg_th_p->hdr.opcode = EXP_BT_DIRENT_LOAD;
   msg_th_p->hdr.queue_prio = PRIO_RSP;
   msg_th_p->hdr.rozofs_queue = &dirent_queue;
   msg_th_p->hdr.user_ctx = ruc_buffer;
         
   msg_th_p->req.dirent_rq.req.eid =  priv_p->eid;
   msg_th_p->req.dirent_rq.req.inode = inode;
   /*
   ** set the mtime & utime at the time of the dirent load request
   */
   msg_th_p->req.dirent_rq.mtime = ext_attr_p->s.attrs.mtime;
   msg_th_p->req.dirent_rq.utime = ext_dir_mattr_p->s.update_time;
   msg_th_p->req.dirent_rq.req.ipaddr = priv_p->ipaddr;
   
   strcpy(msg_th_p->req.dirent_rq.req.client_export_root_path,priv_p->local_export_path);
   
   message = (uint64_t) msg_th_p;

   ret = sendto(thread_ctx_p->sendSocket,&message, sizeof(message),0,(struct sockaddr*)&rozofs_bt_cli_reader_socket_name[0],sizeof(rozofs_bt_cli_reader_socket_name[0]));
   if (ret <= 0) {
      fatal("error while submitting a request to the tracking file client reader sendto socket %d (%s) %s",thread_ctx_p->sendSocket, rozofs_bt_cli_reader_socket_name[0].sun_path, strerror(errno));
      exit(0);  
   } 
   return;

request_file_tracking_load:
   /*
   ** load the tracking file
   */
   if (ruc_buffer == NULL) rozofs_bt_trigger_read_trk_file_for_directory_from_dirent_thread(thread_ctx_p,inode);  
   else
   {
     /*
     ** that situation should not occur
     */
     ruc_buf_freeBuffer(ruc_buffer);
   }
   return;
}


/**
**_______________________________________________________________________________________
*/
/**
*   the sock_id is an AF_UNIX socket used in non connected node (thread_ctx_p->sendSocket)
*/
int rozofs_bt_trigger_read_trk_file_for_directory_from_dirent_thread(rozofs_bt_thread_ctx_t * thread_ctx_p,uint64_t inode)
{

   rozofs_bt_dirent_priv_ctx_t *priv_p;   
   void *ruc_buffer;
   expbt_msgint_full_t *msg_th_p;
   rozofs_bt_track_key_t inode_key;
   rozofs_bt_inode_pending_t *rozofs_bt_inode_pending_p;   
   uint64_t message;

   priv_p = thread_ctx_p->private;
   /*
   ** normalize the inode value
   */
   inode = rozofs_bt_inode_normalize(inode);
   
   ruc_buffer = ruc_buf_getBuffer(priv_p->buffer_pool); 
   if (ruc_buffer == NULL)
   {
     fatal("dirent buffer command pool is empty");
     return -1;
   }
   ROZOFS_BT_DIRENT_STATS_INO(get_tracking_file_attempts,inode);
   msg_th_p = (expbt_msgint_full_t*)ruc_buf_getPayload(ruc_buffer);
   rozofs_bt_inode_pending_p = (rozofs_bt_inode_pending_t*)(msg_th_p+1);
   /*
   ** insert the inode in the pending list
   */
   list_init(&rozofs_bt_inode_pending_p->list);
   rozofs_bt_inode_pending_p->inode = inode;
   rozofs_bt_inode_pending_p->file_key = rozofs_bt_inode_get_filekey_from_inode(inode);
   list_push_back(&pending_inode_head,&rozofs_bt_inode_pending_p->list);

   
   msg_th_p = (expbt_msgint_full_t*)ruc_buf_getPayload(ruc_buffer);
   
   msg_th_p->hdr.xid = 0;
   msg_th_p->hdr.opcode = ROZO_BATCH_TRK_FILE_READ;
   msg_th_p->hdr.queue_prio = PRIO_RSP;
   msg_th_p->hdr.rozofs_queue = &dirent_queue;
   msg_th_p->hdr.user_ctx = ruc_buffer;
   
   inode_key = rozofs_bt_inode_get_filekey_from_inode(inode);
   
   msg_th_p->req.read_trk_rq.inode = inode;
   msg_th_p->req.read_trk_rq.read_trk.eid =  priv_p->eid;
   msg_th_p->req.read_trk_rq.read_trk.type = inode_key.s.key;
   msg_th_p->req.read_trk_rq.read_trk.file_id = inode_key.s.file_id;
   msg_th_p->req.read_trk_rq.read_trk.usr_id = inode_key.s.usr_id;
   
   message = (uint64_t) msg_th_p;
   rozofs_queue_put_prio(&rozofs_bt_ino_queue_req,(void*)message,PRIO_CMDQ);
   return 0;
}   


/*
 *_______________________________________________________________________
 */
/**
*  That function is called when a message is received on the PRIO_RSP level of the queue

  @param thread_ctx_p: thread context
  @param msg_th_p: pointer to the message that contains the command and the response

  @retval none  
*/
void rozofs_bt_dirent_load_response(rozofs_bt_thread_ctx_t * thread_ctx_p,expbt_msgint_full_t *msg_th_p)
{

   void *ruc_buffer;
   ext_mattr_t * ext_attr_p;
   int ret;
   rozofs_bt_inode_pending_t *rozofs_bt_inode_pending_p;    
   uint64_t time2check;   

   /*
   ** remove the inode from the pending list
   */
   rozofs_bt_inode_pending_p = (rozofs_bt_inode_pending_t*)(msg_th_p+1);
   list_remove(&rozofs_bt_inode_pending_p->list);
   
   /*
   ** extract the ruc buffer reference from the message header
   */
   ruc_buffer = msg_th_p->hdr.user_ctx;
   /*
   ** Get the status of the load dirent files operation
   */
   if (msg_th_p->rsp.dirent_rsp.rsp.status == 0)
   {
      FDL_INFO("dirent file successfully loaded for inode %llx of eid %d", (unsigned long long int) msg_th_p->req.dirent_rq.req.inode, 
                                                                       msg_th_p->req.dirent_rq.req.eid);   

      ROZOFS_BT_DIRENT_STATS_DIR(get_dirent_file_success);
      ext_attr_p = rozofs_bt_lookup_inode_internal( msg_th_p->req.dirent_rq.req.inode);
      if (ext_attr_p != NULL)
      {
         /*
	 ** set the time to put in the btmap  entry : either mtime or utime
	 */
         time2check = (rozofs_bt_dirent_mtime_valid!=0)?msg_th_p->req.dirent_rq.mtime:msg_th_p->req.dirent_rq.utime;
         rozofs_bt_mdirent_set_dir_mtime(thread_ctx_p,ext_attr_p,time2check);	
	 /*
	 ** load the bitmap of the dirent directory
	 */
	 ret = rozofs_bt_load_dirent_root_bitmap_file(msg_th_p->req.dirent_rq.req.inode,time2check);
	 if (ret < 0)
	 {
           warning("error while loading bitmap for directory inode %llx:%s",(unsigned long long int) msg_th_p->req.dirent_rq.req.inode,strerror(errno));
	 }	
      }						       
								      
   }
   else
   {
      ROZOFS_BT_DIRENT_STATS_DIR(get_dirent_file_failure);
      FDL_INFO("dirent file loading error for inode %llx of eid %d: %s", (unsigned long long int) msg_th_p->req.dirent_rq.req.inode, 
                                                                       msg_th_p->req.dirent_rq.req.eid,strerror(msg_th_p->rsp.dirent_rsp.rsp.errcode));   
   }
   /*
   ** release the ruc buffer
   */
   ruc_buf_freeBuffer(ruc_buffer);


}



/*
 *_______________________________________________________________________
 */
/**
*  That function is called when a message is received on the PRIO_RSP level of the queue

  @param thread_ctx_p: thread context
  @param msg_th_p: pointer to the message that contains the command and the response

  @retval none  
*/
void rozofs_bt_read_tracking_file_response(rozofs_bt_thread_ctx_t * thread_ctx_p,expbt_msgint_full_t *msg_th_p)
{

   void *ruc_buffer;
   rozofs_inode_t fake_inode;
   rozofs_bt_inode_pending_t *rozofs_bt_inode_pending_p;

   /*
   ** remove the inode from the pending list
   */
   rozofs_bt_inode_pending_p = (rozofs_bt_inode_pending_t*)(msg_th_p+1);
   list_remove(&rozofs_bt_inode_pending_p->list);   
   /*
   ** extract the ruc buffer reference from the message header
   */
   ruc_buffer = msg_th_p->hdr.user_ctx;
   /*
   ** Get the status of the load dirent files operation
   */
   if (msg_th_p->rsp.read_trk_rsp.rsp.status == 0)
   {
      FDL_INFO("tracking file successfully loaded for inode %llx of eid %d", (unsigned long long int) msg_th_p->req.read_trk_rq.inode, 
                                                                       msg_th_p->req.read_trk_rq.read_trk.eid);
      /*
      ** check if the inode is a directory, if it is the case, we load the dirent files
      ** otherwise it is the case of the regular file and we can stop here
      */
      ROZOFS_BT_DIRENT_STATS_INO(get_tracking_file_success,msg_th_p->req.read_trk_rq.inode);
      fake_inode.fid[1]=  msg_th_p->req.read_trk_rq.inode;
      if (fake_inode.s.key == ROZOFS_DIR) 
      {
         /*
	 ** it is a directory, so we ask for dirent file loading
	 */
         return rozofs_bt_dirent_load_request(thread_ctx_p, msg_th_p->req.read_trk_rq.inode,ruc_buffer);
      }
   }
   else
   {
      ROZOFS_BT_DIRENT_STATS_INO(get_tracking_file_failure,msg_th_p->req.read_trk_rq.inode);

      warning("tracking file loading error for inode %llx of eid %d: %s", (unsigned long long int) msg_th_p->req.read_trk_rq.inode, 
                                                                       msg_th_p->req.read_trk_rq.read_trk.eid,strerror(msg_th_p->rsp.read_trk_rsp.rsp.errcode));   
   }
   /*
   ** release the ruc buffer
   */
   ruc_buf_freeBuffer(ruc_buffer);


}
/*
 *_______________________________________________________________________
 */
static void *rozofs_bt_dirent_thread(void *v) {

   rozofs_bt_thread_ctx_t * thread_ctx_p = (rozofs_bt_thread_ctx_t*)v; 
   rozofs_bt_dirent_priv_ctx_t *priv_p;
   uint8_t mask;
   void *msg;
   int prio;
   expbt_msgint_full_t *msg_th_p;
   
   priv_p = thread_ctx_p->private;   
   uma_dbg_thread_add_self("bt_dirent");
   dirent_thread_ready = 1;
   
   while(1)
   {
     mask = 1<< PRIO_RSP; 
     if (ruc_buf_getFreeBufferCount(priv_p->buffer_pool) != 0) mask = mask | (1 << PRIO_CMD);

     msg = rozofs_queue_get_prio_with_mask(&dirent_queue,&prio,mask);
     switch (prio)
     {
       case PRIO_CMD:	    
	 FDL_INFO("load dirent files for inode %llu\n",(unsigned long long int)msg);
	 rozofs_bt_dirent_load_request(thread_ctx_p,(uint64_t)msg,NULL); 
	 break; 
       case PRIO_RSP:
         msg_th_p = (expbt_msgint_full_t*)msg;
	 switch (msg_th_p->hdr.opcode)
	 {
           case EXP_BT_DIRENT_LOAD:
            rozofs_bt_dirent_load_response(thread_ctx_p,msg); 
	    break;

           case ROZO_BATCH_TRK_FILE_READ:
           rozofs_bt_read_tracking_file_response(thread_ctx_p,msg); 
	    break; 

	   default:
	    fatal("unexpected  opcode:%d (prio: %d\n",(int)msg_th_p->hdr.opcode,prio);
	     }
	 break;  
       default:
	fatal("unexpected  PRIO:%d\n",prio);
      }
   
   }

}
/*
 *_______________________________________________________________________
 */
/**
*  Init of the dirent file loader for a client

   @param  nb_buffer: number of buffer allocated for command submit
   @param ipaddr: IP@ of the client
   @param local_export_path: local root path on the client when dirent file will be stored
   @param eid: exportd identifier of the client

   @retval 0 on success
   @retval -1 on error
*/   
int rozofs_bt_dirent_init(int nb_buffer,char *host/*,char *local_export_path*/,int eid)
{
   int ret;
   pthread_attr_t             attr;  
   struct timespec ts = {0, ((1000)*30)};
   int i;
   rozofs_bt_dirent_priv_ctx_t *priv_p;
   uint32_t ipaddr;
   char pathname[1024];
     pthread_t                    thrdId;
   
   sprintf(pathname,"%s/backup/",conf.export);
   /*
   ** load the path for dirent usage
   */
   dirent_export_root_path[eid] = strdup(pathname);
   rozofs_bt_dirent_hash_init(eid,pathname);
   
   
   rozofs_bt_nb_buffers = nb_buffer;
   /*
   ** allocate the statistics buffer
   */
   dirent_stats_p = malloc(sizeof(*dirent_stats_p));
   if (dirent_stats_p == NULL)
   {
     severe("Out of memory");
     return -1;
   }
   memset( dirent_stats_p,0,sizeof(*dirent_stats_p)); 
   /*
   ** Allocate the table to track the pending file tracking/dirent
   */
   rozofs_pending_inode_tab_p = (rozofs_bt_track_key_t*)malloc(sizeof(uint64_t)*rozofs_bt_nb_buffers);
   if (rozofs_pending_inode_tab_p == NULL)
   {
     severe("Out of memory");
     return -1;
   }
   memset(rozofs_pending_inode_tab_p,0,sizeof(uint64_t)*rozofs_bt_nb_buffers);
   
   if (rozofs_host2ip_netw(host, &ipaddr) != 0) 
   {
      severe("rozofs_host2ip failed for host : %s, %s", host,strerror(errno));
      return -1;
   } 
//   ipaddr = ntohl(ipaddr);
     
   rozofs_bt_dirent_buffer_pool_p  = ruc_buf_poolCreate(nb_buffer,sizeof(expbt_msgint_full_t)+sizeof(rozofs_bt_inode_pending_t)+sizeof(uint64_t));
   if (rozofs_bt_dirent_buffer_pool_p == NULL)
   {
     fatal("Cannot create the buffer pull for dirent file loading (nb buffers %d)",nb_buffer);
     return -1;
   }
    /*
    ** register the pool
    */
    ruc_buffer_debug_register_pool("dirent_load",rozofs_bt_dirent_buffer_pool_p);   
    /*
    ** Create the queue priority for command & response
    */
   ret = rozofs_queue_init_prio(&dirent_queue,ROZOFS_BT_DIRENT_QUEUE_LENGTH,MAX_PRIO);    
   if (ret < 0)
   {
     fatal("Cannot create the dirent queue: %s",strerror(errno));
     return -1;
   }
   /*
   ** init of the head the list that maintains the inode for which the main thread has requested the inode attributes
   */
   list_init(&pending_inode_head);
   
   memset(&dirent_thread_ctx,0,sizeof(rozofs_bt_thread_ctx_t));
   dirent_thread_ctx.sendSocket = -1;
   dirent_thread_ctx.private = malloc(sizeof(rozofs_bt_dirent_priv_ctx_t));
   if (dirent_thread_ctx.private == NULL)
   {
     fatal("Cannot allocate memory: %s",strerror(errno));
     return -1;
   } 
   priv_p = (rozofs_bt_dirent_priv_ctx_t*)dirent_thread_ctx.private;
   priv_p->buffer_pool = rozofs_bt_dirent_buffer_pool_p;
   priv_p->ipaddr = ipaddr;
   priv_p->local_export_path = strdup(pathname);
   priv_p->eid = eid;
   
   dirent_thread_ctx.sendSocket = socket(AF_UNIX,SOCK_DGRAM,0);
   if ( dirent_thread_ctx.sendSocket < 0) {
      fatal("rozofs_bt_dirent_thread fail to create socket: %s", strerror(errno));
      return -1;   
   } 
   ret = pthread_attr_init(&attr);
   if (ret != 0) {
     fatal("rozofs_bt_dirent_thread pthread_attr_init %s",strerror(errno));
     return -1;
   }  
   /*
   ** now create the thread and start it
   */
   ret = pthread_create(&thrdId,&attr,rozofs_bt_dirent_thread,&dirent_thread_ctx);
   if (ret != 0) {
     fatal("rozofs_bt_dirent_thread pthread_create() %s", strerror(errno));
     return -1;
   }    
   for (i=0;i < 16;i++) {
       nanosleep(&ts, NULL);
       if (dirent_thread_ready)
       {
         return 0;  
       }   
   }    
   fatal("rozofs_bt_dirent_thread cannot start");
   return -1;   
}   
   
