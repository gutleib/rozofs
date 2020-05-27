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

#include <rozofs/rpc/eproto.h>

#include "rozofs_fuse_api.h"
#include "rozofs_bt_dirent.h"
#include <rozofs/core/rozofs_string.h>

DECLARE_PROFILING(mpp_profiler_t);

/**
*  decoding structure for object filename
*/
typedef struct mattr_obj {
    fid_t fid;                      /**< unique file id */
    cid_t cid;                      /**< cluster id 0 for non regular files */
    sid_t sids[ROZOFS_SAFE_MAX];    /**< sid of storage nodes target (regular file only)*/
    uint64_t size;                  /**< see stat(2) */
} mattr_obj_t;

int rozofs_get_safe(int layout)
{
  switch (layout)
  {
    case 0: return 4;
    case 1: return 8;
    case 2: return 16;
  }
  return -1;
}
ruc_obj_desc_t  rozofs_lookup_queue[ROZOFS_MAX_LKUP_QUEUE];  /**< pending list of the lookup */

/**
 * hashing function used to find a dentry in the cache
 */
static inline uint32_t dentry_hash(fuse_ino_t parent,char *name) {
    uint32_t       hash = 0;
    uint8_t       *c;
    int            i;
    /*
    ** hash on the fid
    */
    c = (uint8_t *) &parent;
    for (i = 0; i < sizeof(fuse_ino_t); c++,i++)
        hash = *c + (hash << 6) + (hash << 16) - hash;
    /*
    ** hash on the name
    */
    c = (uint8_t *) name;
    i = 0;
    while (*c !=0)
    {
        hash = *c + (hash << 6) + (hash << 16) - hash; 
	c++;
	i++;
	if (i == 16) break;
    }
    return hash;
}
/*
**___________________________________________________________________________________ 
*/ 
/**
*   Search if there is already a pending lookup for the same object

    @param buffer: rozofs fuse context
    @param parent: inode parent
    @param name: name to look for
    
    @retval 1 : found
    @retval 0: not found
*/
int rozofs_lookup_insert_queue(void *buffer,fuse_ino_t parent, const char *name,fuse_req_t req,int trc_idx,int lookup_flags)
{
   ruc_obj_desc_t   * phead;
   ruc_obj_desc_t   * elt;
   ruc_obj_desc_t   * pnext;
   rozofs_fuse_save_ctx_t *fuse_save_ctx_p;  
   uint32_t hash; 
   /*
    ** scan the pending lookup request searching for the same request (ino+name)
    */
   hash = dentry_hash(parent,(char*)name);
   phead = &rozofs_lookup_queue[hash%ROZOFS_MAX_LKUP_QUEUE];   
   pnext = (ruc_obj_desc_t*)NULL;
   while ((elt = ruc_objGetNext(phead, &pnext)) != NULL) 
   {
      /*
      ** Check if the inode and the name are the same
      */
      fuse_save_ctx_p = (rozofs_fuse_save_ctx_t*)ruc_buf_getPayload(elt); 
      if (fuse_save_ctx_p->parent != parent) continue;
      if (strcmp(name,fuse_save_ctx_p->name) !=0) continue;
      /*
      ** it is the same request: so queue it on the current one if there is enough room
      */
      if (fuse_save_ctx_p->lkup_cpt < ROZOFS_MAX_PENDING_LKUP)
      {
         fuse_save_ctx_p->lookup_tb[fuse_save_ctx_p->lkup_cpt].req = req;
         fuse_save_ctx_p->lookup_tb[fuse_save_ctx_p->lkup_cpt].trc_idx = trc_idx;
         fuse_save_ctx_p->lookup_tb[fuse_save_ctx_p->lkup_cpt].flags = lookup_flags;
	 fuse_save_ctx_p->lkup_cpt++;
	 return 1;
      }
    }
    /*
    ** this a new request: insert it in the global pending list
    */
    ruc_objInsertTail(phead,(ruc_obj_desc_t*)buffer);   
    return 0;
}
/*
**___________________________________________________________________________________ 
*/ 
/**
* that service provides the parsing of a filename for object mode access
   in order to bypass the metadata server (exportd)
   the filename structure is the following:
   
   @rozofs@<eid>-<cid>-<layout>-<distribution><fid>-<size>
   
   the structure of the distribution is :(the number of values depends on the layout
   <sid0>-<sid1>-...
   the structure of the fid is (as example):
     5102b7e5-8f44-4d0c-2500-000000000010
     
   @param name : filename to parse
   @param attr_p : pointer to the structure used for storing the attributes
   
   @retval 0 on success
   @retval -1 on error (see errno for details)

*/

int rozofs_parse_object_name(char *name,mattr_obj_t *attr_p)
{
  int ret;
  int layout;
  int eid;
  int cid;
  int sid;
  int i;
  int nb_sid;
  uint64_t size;
  char *pnext;
  char *cur = name;
  int len;
  
  memset(attr_p,0,sizeof(mattr_obj_t));
  len = strlen(name);

  while(1)
  {
    /*
    ** get the eid
    */
    errno = 0;
    eid = strtoul(name,&pnext,10);
    if (eid == ULONG_MAX) break;
    if (errno != 0) break;
    if (*pnext !='-') 
    {
      errno = EINVAL;
      break;
    }
    /*
    ** get the cid
    */
    errno = 0;
    cur = pnext+1;
    cid = strtoul(cur,&pnext,10);
    if (errno != 0) 
    {
       break;
    }
    if (*pnext !='-') 
    {
      errno = EINVAL;
      break;
    }
    attr_p->cid = cid;
    /*
    ** get the layout
    */
    errno = 0;
    cur = pnext+1;
    layout = strtoul(cur,&pnext,10);
    if (errno != 0) 
    {
       break;
    }
    if (*pnext !='-') 
    {
      errno = EINVAL;
      break;
    }
    /*
    ** get the distribution: remenber that the number
    ** of sid depends on the layout
    */
    nb_sid = rozofs_get_safe(layout);
    if (nb_sid < 0)
    {
      errno = EINVAL;
      break;
    }
    for (i = 0; i < nb_sid; i++)
    {
      errno = 0;
      cur = pnext+1;
      sid = strtoull(cur,&pnext,10);
      if (errno != 0) 
      {
	 break;
      }
      if (*pnext !='-') 
      {
	errno = EINVAL;
	break;
      } 
      attr_p->sids[i] = (uint8_t)sid;        
    }
    if (errno!= 0) break;
    /*
    ** get the fid
    */
    cur = pnext+1;
    if (pnext[37] != '-')
    {
      errno = EINVAL;
      break;    
    }
    pnext[37]=0;
    ret = rozofs_uuid_parse(cur,attr_p->fid);
    if (ret < 0)
    {
      errno = EINVAL;
    } 
    pnext+=37;
    /*
    ** get the size
    */
    errno = 0;
    cur = pnext+1;
    size = strtoull(cur,&pnext,10);
    if (errno != 0) 
    {
       break;
    }
    attr_p->size = size;
    break;
  }
  if (errno!=0) 
  {
    return -1;
  }
  if ((pnext-name) != len)
  {
     errno = EINVAL;
    return -1;
  }  
  return 0;

}
/*
**__________________________________________________________________________________________________
*/
/**
   Check if the ientry of a directory is still valid

   @param parent_fid: fid of the directory
   @param pie: directory ientry
 
   @retval 0 on success
   @retval -1 on lookup fail
*/

int rozofs_bt_local_check_parent_attributes(fid_t parent_fid,ientry_t *pie)
{
   rozofs_inode_t *rozofs_inode_p;
   ext_mattr_t * ext_attr_parent_p;
   ext_dir_mattr_t *stats_attr_p;
   uint64_t update_time;
   int dirent_valid;
   rozofs_bt_tracking_cache_t *tracking_ret_p = NULL;
   uint64_t inode;
  
   rozofs_inode_p = (rozofs_inode_t*)parent_fid;
   inode = rozofs_inode_p->fid[1];

   ext_attr_parent_p = rozofs_bt_load_dirent_from_main_thread(inode,&tracking_ret_p,&dirent_valid);
   if (ext_attr_parent_p == NULL)
   {
     /*
     ** clear the timestamp of the ientry in order to force an access towards the exportd:
     ** !!WARNING!! not a good idea since a getattr can permit to update that information
     ** so we should not touch the timestamp: the only thing that will happen is to provide a shorter cache time to the kernel
     */
     //pie->timestamp = 0;
     return -1;
   }
   stats_attr_p = (ext_dir_mattr_t *)&ext_attr_parent_p->s.attrs.sids[0];
   update_time = rozofs_get_parent_update_time_from_ie(pie);
   
   if (update_time == stats_attr_p->s.update_time)
   {
     /*
     ** all is fine: copy the current timestamp of tracking file
     */
     pie->timestamp = tracking_ret_p->timestamp;
     return 0;
   }
   /*
   ** we need to update the parent attributes in the ientry cache
   ** among the update, the cache time is also asserted according to the mtime of the regular file, we should do the same for the directories
   */
   rozofs_ientry_update(pie,(struct inode_internal_t*)ext_attr_parent_p);      
   /*
   ** the timestamp of the directory is update with the current time in the rozofs_ientry_update(), it will be better if we update
   ** it with the timestamp of the tracking file
   */
   pie->timestamp = tracking_ret_p->timestamp;
   return 0;  
}

/*
**__________________________________________________________________________________________________
*/
/**
   get the current time stamp of the tracking cache file: use for the case when attributes are valid and the inode is provided by fuse kernel

   @param parent_fid: fid of file
   @param pie: ientry 
 
  @retval none
*/

void rozofs_bt_local_set_regular_file_timestamp(fid_t fid,ientry_t *pie)
{
   rozofs_inode_t *rozofs_inode_p;
   ext_mattr_t * ext_attr_p;
   rozofs_bt_tracking_cache_t *tracking_ret_p = NULL;
   int dirent_valid;
   uint64_t inode;
     
   rozofs_inode_p = (rozofs_inode_t*) fid;
   if (rozofs_inode_p->s.key != ROZOFS_REG) return;
   inode = rozofs_inode_p->fid[1];
   
   ext_attr_p = rozofs_bt_load_dirent_from_main_thread(inode,&tracking_ret_p,&dirent_valid);
   if (ext_attr_p == NULL)
   {
     return ;
   }
   /*
   ** copy the timestamp of the tracking cache entry in the ientry
   */
   if (tracking_ret_p->errcode != 0) return;
   pie->timestamp = tracking_ret_p->timestamp;
        
}
/*
**__________________________________________________________________________________________________
*/
/*
**
   Get the slave inodes if any: only fro regular files

   @param tracking_ret_p: pointer to the tracking file cache entry
   @param rozofs_inode_p: pointer to the master rozofs inode
   @param ie: inode ientry pointer
   
   @retval 0 on success
   @retval < 0 on error (see errno for details)

*/
int rozofs_bt_get_slave_inode(rozofs_bt_tracking_cache_t *tracking_ret_p,rozofs_inode_t *rozofs_inode_p,ientry_t *ie)
{
   int attr_sz;
   rozofs_slave_inode_t *slave_inode_p;
   int cur_slave = 0;
   ext_mattr_t * ext_attr_p;
   
   if (rozofs_inode_p->s.key != ROZOFS_REG)
   {
     errno = ENOTSUP;
     warning("attempt to get slave inode for an inode that is not associated with a regular file");
     return -1;
   }
   if (ie->attrs.multi_desc.common.master == 0)
   {
     /*
     ** it not the case of the multifile, so exit
     */
     return 0;
   }
   attr_sz = sizeof(rozofs_slave_inode_t)*(ie->attrs.multi_desc.master.striping_factor+1);
   /*
   ** allocate the array if not yet done:
   ** The entry can be be there becasue it is an update of an inode
   */
   if (ie->slave_inode_p == NULL)
   {
     ie->slave_inode_p = malloc(attr_sz);
     if (ie->slave_inode_p== NULL)
     {
       severe("Out of memory while allocating memory for slave inodes (%d)",attr_sz);
       errno = ENOMEM;
       return -1;
     }
   }
   /*
   ** now loop on the slave inodes
   */
   slave_inode_p = ie->slave_inode_p;
   rozofs_inode_p->s.idx += 1;
   for (cur_slave = 0; cur_slave < ie->attrs.multi_desc.master.striping_factor+1;cur_slave++,slave_inode_p++)
   {
     ext_attr_p = rozofs_bt_get_inode_ptr_from_image(tracking_ret_p,rozofs_inode_p->fid[1]);
     rozofs_inode_p->s.idx += 1;

     if (ext_attr_p == NULL)
     {
       severe("slave inode index %d not found",cur_slave);
       errno = EPROTO;
       return -1;
     }
     /*
     ** copy the part of the slave inode in the ientry
     */
     memcpy(slave_inode_p->sids,ext_attr_p->s.attrs.sids,sizeof(sid_t)*ROZOFS_SAFE_MAX);
     slave_inode_p->cid = ext_attr_p->s.attrs.cid;
     slave_inode_p->size = ext_attr_p->s.attrs.size;
     slave_inode_p->children = ext_attr_p->s.attrs.children;  
   }
   
   errno = 0;
   return 0;
}

/*
**__________________________________________________________________________________________________
*/
/**

   @param eid: export identifier
   @param parent_fid: fid of the parent directory
   @param name: name to search
   @param param: fuse context
   @param pie: parent ientry
 
   @retval 0 on success
   @retval -1 on lookup fail
*/

int rozofs_bt_local_lookup_attempt(uint32_t eid,fid_t parent_fid,char *name,void *param,ientry_t *pie) 
{

   rozofs_inode_t *rozofs_inode_p;   
   ext_mattr_t * ext_attr_parent_p;
   ext_mattr_t * ext_attr_child_p;  
   fid_t child_fid; 
   struct stat stbuf;
   fuse_req_t req; 
   int trc_idx;
   int ret;
   int dirent_valid;
   uint64_t inode;

   struct fuse_entry_param fep;
   ientry_t *child_ie = NULL;
   rozofs_inode_t *fake_id_p;
   rozofs_bt_tracking_cache_t *tracking_ret_p = NULL;

   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,trc_idx);
   
   /*
   ** we do not care about @rozofs_uuid@xxxx-xxxxx-xxxx : this request is always sent to the export
   */
  if ((strncmp(name,"@rozofs_uuid@",13) == 0) ||(strncmp(name,ROZOFS_DIR_TRASH,strlen(ROZOFS_DIR_TRASH)) == 0))
  {
    errno = EAGAIN;
    return -1;
  }   
   rozofs_inode_p = (rozofs_inode_t*)parent_fid;
   inode = rozofs_inode_p->fid[1];
   ext_attr_parent_p = rozofs_bt_load_dirent_from_main_thread(inode,NULL,&dirent_valid);
   /*
   ** we stop here if either the parent attributes are not available or the dirent file are not up to date
   */
   if ((ext_attr_parent_p == NULL) || (dirent_valid == 0))
   {
     return -1;
   }
   /*
   ** now attempt to get the inode for the required name
   */
   ret = rozofs_bt_get_mdirentry(parent_fid,name,child_fid);
   if (ret < 0)
   {
     if (errno == EAGAIN) return -1;
     if (errno == ENOENT) goto enoent;
     return -1;
   }   
   /*
   ** We have found the inode, attempt to get the attributes now
   */
   rozofs_inode_p = (rozofs_inode_t*)child_fid;
   inode = rozofs_inode_p->fid[1];
   ext_attr_child_p = rozofs_bt_load_dirent_from_main_thread(inode,&tracking_ret_p,&dirent_valid);
   if (ext_attr_child_p == NULL)
   {
     return -1;
   }
   /*
   ** we have both attributes
   */
   if (!(child_ie = get_ientry_by_fid(ext_attr_child_p->s.attrs.fid))) {
       child_ie = alloc_ientry(ext_attr_child_p->s.attrs.fid);
   } 
   /*
   ** among the update, the cache time is also asserted according to the mtime of the regular file, we should do the same for the directories
   */
   rozofs_ientry_update(child_ie,(struct inode_internal_t*)ext_attr_child_p);     
   /*
   ** need to take care of the slave inode here: only for the case of the regular file
   */
   if (S_ISREG(ext_attr_child_p->s.attrs.mode))
   {
      ret = rozofs_bt_get_slave_inode(tracking_ret_p,rozofs_inode_p,child_ie);
   }   
    memset(&fep, 0, sizeof (fep));
    mattr_to_stat((struct inode_internal_t*)ext_attr_child_p, &stbuf,exportclt.bsize);
    stbuf.st_ino = child_ie->inode;
    /*
    ** check the case of the directory
    */
    if ((S_ISDIR(ext_attr_child_p->s.attrs.mode)) &&(strncmp(name,"@rozofs_uuid@",13) == 0))
    {
        rozofs_inode_t fake_id;
		
	fake_id.fid[1]= child_ie->inode;
	fake_id.s.key = ROZOFS_DIR_FID;
        fep.ino = fake_id.fid[1];  
    }
    else
    {
      fep.ino = child_ie->inode;
    }
    stbuf.st_size = child_ie->attrs.attrs.size;
    fake_id_p = (rozofs_inode_t *)ext_attr_child_p->s.attrs.fid;
    if (fake_id_p->s.del)
    {
      fep.attr_timeout  = 0;
      fep.entry_timeout = 0;
    }
    else
    {
      fep.attr_timeout = rozofs_get_linux_caching_time_second(child_ie);
      fep.entry_timeout = rozofs_tmr_get_entry(rozofs_is_directory_inode(child_ie->inode));    
    }
    memcpy(&fep.attr, &stbuf, sizeof (struct stat));
    child_ie->nlookup++;

    rozofs_inode_t * finode = (rozofs_inode_t *) child_ie->attrs.attrs.fid;
    fep.generation = finode->fid[0]; 
    /*
    ** set the parent update time in the ientry of the child inode
    */
    child_ie->parent_update_time = rozofs_get_parent_update_time_from_ie(pie);
    
    rz_fuse_reply_entry(req, &fep);  
    errno = 0;
    goto out;   

enoent:
   /*
   ** Case of non existent entry. 
   ** Tell FUSE to keep responding ENOENT for this name for a few seconds
   */
   memset(&fep, 0, sizeof (fep));
   fep.ino = 0;
   fep.attr_timeout  = rozofs_tmr_get_enoent();
   fep.entry_timeout = rozofs_tmr_get_enoent();
   rz_fuse_reply_entry(req, &fep);
   errno = ENOENT;

out:

   rozofs_trc_rsp_attr(srv_rozofs_ll_lookup,0xfacebeef,(child_ie==NULL)?NULL:child_ie->attrs.attrs.fid,0,(child_ie==NULL)?-1:child_ie->attrs.attrs.size,trc_idx);   
   return 0;


}       

/*
**__________________________________________________________________________________________________
*/
/**
*  metadata Lookup

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param parent : inode parent provided by rozofsmount
 @param name : name to search in the parent directory
 
 @retval none
*/
void rozofs_ll_lookup_cbk(void *this,void *param);


void rozofs_ll_lookup_nb(fuse_req_t req, fuse_ino_t parent, const char *name) 
{
    ientry_t *ie = 0;
    ientry_t *nie = 0;
    epgw_lookup_arg_t arg;
    int    ret;        
    void *buffer_p = NULL;
    int trc_idx;
    mattr_obj_t mattr_obj;
    struct fuse_entry_param fep;
    struct stat stbuf;
    int allocated = 0;
    int len_name;
    fuse_ino_t child = 0;   
//    int  local_lookup_success = 0;
    uint32_t lookup_flags=0;
    int extra_length = 0;
    fuse_ino_t ino = 0;
    int trace_flag = 0;
    int fast_reconnect = 0;
//    uint64_t parent_update_time;

    /*
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_LOOKUP], 1);
    
    extra_length = rozofs_check_extra_inode_in_lookup((char*)name, &len_name);
    if (extra_length !=0)
    {
       uint8_t *pdata_p;
       uint32_t *lookup_flags_p = (uint32_t*)&name[len_name+1];
       lookup_flags =*lookup_flags_p;
       if (extra_length > 4)
       {
         pdata_p = (uint8_t*)&name[len_name+1];
	 pdata_p+=sizeof(uint32_t);
	 fuse_ino_t *inode_p = (fuse_ino_t*)pdata_p;
	 child = *inode_p;
	 ino = child;
       }
    }
    trace_flag = lookup_flags;
    if (ino != 0) {trace_flag |=(1<<31);}
    trc_idx = rozofs_trc_req_name_flags(srv_rozofs_ll_lookup,parent,(char*)name,(int)(trace_flag));
    /*
    ** allocate a context for saving the fuse parameters
    */
    buffer_p = rozofs_fuse_alloc_saved_context();
    if (buffer_p == NULL)
    {
      severe("out of fuse saved context");
      errno = ENOMEM;
      goto error;
    }
    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,parent);
    SAVE_FUSE_STRING(buffer_p,name);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);
    SAVE_FUSE_PARAM(buffer_p,ino);
    SAVE_FUSE_PARAM(buffer_p,lookup_flags);
    

    DEBUG("lookup (%lu,%s)\n", (unsigned long int) parent, name);

    START_PROFILING_NB(buffer_p,rozofs_ll_lookup);
    len_name=strlen(name);
    if (len_name > ROZOFS_FILENAME_MAX) {
        errno = ENAMETOOLONG;
        goto error;
    }
    /*
    ** Check the case of NFS that attempt to revalidate an inode that has been flushed from the cache
    */
    if ((strcmp(name,"..")== 0) || (strcmp(name,".")==0))
    {
       fid_t lookup_fid;
       rozofs_inode_t *inode_p = (rozofs_inode_t*)lookup_fid;
       inode_p->fid[0]=0;
       inode_p->s.eid= exportclt.eid;
       inode_p->fid[1]=parent;
              
       arg.arg_gw.eid = exportclt.eid;
       memcpy(arg.arg_gw.parent,lookup_fid, sizeof (uuid_t));
       arg.arg_gw.name = (char*)name;    
       /*
       ** Queue the request and attempt to check if there is already the same
       ** request queued
       */
       if (rozofs_lookup_insert_queue(buffer_p,parent,name,req,trc_idx,lookup_flags)== 1)
       {
	 /*
	 ** There is already a pending request, so nothing to send to the export
	 */
	 gprofiler->rozofs_ll_lookup_agg[P_COUNT]++;
	 rozofs_fuse_release_saved_context(buffer_p);
	 return;
       }

       
       ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,lookup_fid,EXPORT_PROGRAM, EXPORT_VERSION,
                        	 EP_LOOKUP,(xdrproc_t) xdr_epgw_lookup_arg_t,(void *)&arg,
                        	 rozofs_ll_lookup_cbk,buffer_p); 
       if (ret < 0) goto error;
       /*
       ** no error just waiting for the answer
       */
       return;    
    }
    if (!(ie = get_ientry_by_inode(parent))) {
        errno = ENOENT;
        goto error;
    }
    /*
    ** check for direct access
    */
    if (strncmp(name,"@rozofs@",8) == 0)
    {
      ret = rozofs_parse_object_name((char*)(name+8),&mattr_obj);
      if (ret == 0)
      {
	/*
	** successful parsing-> attempt to create a fake ientry
	*/
	//errno = ENOENT;
	goto lookup_objectmode;
      }     
    }    
    /*
    ** Queue the request and attempt to check if there is already the same
    ** request queued
    */
    if (rozofs_lookup_insert_queue(buffer_p,parent,name,req,trc_idx,lookup_flags)== 1)
    {
      /*
      ** There is already a pending request, so nothing to send to the export
      */
      gprofiler->rozofs_ll_lookup_agg[P_COUNT]++;
      rozofs_fuse_release_saved_context(buffer_p);
      return;
    }
    /*
    ** check if the lookup is a lookup revalidate. In such a case, the VFS provides*
    ** the inode. So if the i-node attributes are fine, we do not worry
    */
    if ((child != 0) /*&& ((lookup_flags & 0x100) == 0)*/)
    {

      nie = get_ientry_by_inode(child);
      if (nie != NULL)
      {
        /*
	** in batch mode: attempt to update the directory ientry if there is a change in the tracking file of the directory
	*/
        if (conf.batch) rozofs_bt_local_check_parent_attributes(ie->fid,ie);
	/*
	** !!WARNING!!: ignore the batch mode for the check
	*/
	if (rozofs_is_attribute_valid_with_parent(nie,(conf.batch == 0)?ie:ie)) {
	  /*
	  ** check if parent and child are either deleted/deleted or active/active
	  */
	  int trash_state = 0;
	  if (rozofs_inode_is_del_pending(ie->attrs.attrs.fid)) trash_state = 1;
	  if (rozofs_inode_is_del_pending(nie->attrs.attrs.fid)) trash_state |= 1<<1;
	  switch (trash_state)
	  {
	     case 2:
	       if (rozofs_inode_is_trash(nie->attrs.attrs.fid) == 0)
	       {
		 errno = ENOENT;
		 goto error;
	       }	         
	       mattr_to_stat(&nie->attrs, &stbuf,exportclt.bsize);
	       stbuf.st_ino = child;
	       /*
	       ** set nlinks to reflect . & ..
	       */
	       stbuf.st_nlink =2;
	       goto success;
	       break;

	     case 0:
	     case 3:
	      mattr_to_stat(&nie->attrs, &stbuf,exportclt.bsize);
	      stbuf.st_ino = child;
	      goto success; 
	      break;
	     
	      
	    default: 
	     errno = ENOENT;
	     goto error;
	     break;  	  	  
	  }	    
	}                
      }
    }
    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid = exportclt.eid;
    memcpy(arg.arg_gw.parent,ie->fid, sizeof (uuid_t));
    arg.arg_gw.name = (char*)name;    
    /*
    ** now initiates the transaction towards the remote end
    */
    
    
    /*
    ** In case the EXPORT LBG is down ans we know this ientry, let's respond to
    ** the requester with the current available information
    */
    if ((common_config.client_fast_reconnect) && (child != 0)) {
      expgw_tx_routing_ctx_t routing_ctx; 
      
      if (expgw_get_export_routing_lbg_info(arg.arg_gw.eid,ie->fid,&routing_ctx) != 0) {
         goto error;
      }
      if (north_lbg_get_state(routing_ctx.lbg_id[0]) != NORTH_LBG_UP) {
	  if (!(nie = get_ientry_by_inode(child))) {
              errno = ENOENT;
              goto error;
	  }
	  mattr_to_stat(&nie->attrs, &stbuf,exportclt.bsize);
	  stbuf.st_ino = child;
          fast_reconnect = 1;
	  goto success;        
      }      
    }          
    if (conf.batch )
    {
      /*
      ** attempt to use the local dirent cache for searching the inode
      */
      rozofs_bt_lookup_local_attempt++;
      ret = rozofs_bt_lookup_req_from_main_thread(arg.arg_gw.eid, ie->fid,buffer_p);
      if (ret == 0)
      {
	/*
	** OK, we attempt to get the inode from the local dirent cache
	*/
	return;
      }
    }
    /*
    ** there is an error: either the dirent file are not present on the local client or other error
    */

    rozofs_bt_lookup_local_reject_from_main++;

    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,ie->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_LOOKUP,(xdrproc_t) xdr_epgw_lookup_arg_t,(void *)&arg,
                              rozofs_ll_lookup_cbk,buffer_p); 
    
    if (ret < 0) {
      /*
      ** In case of fast reconnect mode let's respond with the previously knows 
      ** parameters instead of failing
      */
      if (common_config.client_fast_reconnect) {
        if (child != 0) {
	  if (!(nie = get_ientry_by_inode(child))) {
              errno = ENOENT;
              goto error;
	  }
	  mattr_to_stat(&nie->attrs, &stbuf,exportclt.bsize);
	  stbuf.st_ino = child;
          fast_reconnect = 1;
	  goto success;
        }
      }
      goto error;
    }
    /*
    ** no error just waiting for the answer
    */

    return;

error:
    fuse_reply_err(req, errno);
    /*
    ** remove the context from the lookup queue
    */
    if (buffer_p != NULL) ruc_objRemove(buffer_p);
    /*
    ** release the buffer if has been allocated
    */
    rozofs_trc_rsp(srv_rozofs_ll_lookup,parent,NULL,1,trc_idx);
out:
    STOP_PROFILING_NB(buffer_p,rozofs_ll_lookup);
    if (buffer_p != NULL) rozofs_fuse_release_saved_context(buffer_p);

    return;
    /**
    * case of the object mode
    */
lookup_objectmode:
    if (!(nie = get_ientry_by_fid(mattr_obj.fid))) {
        nie = alloc_ientry(mattr_obj.fid);
        nie->attrs.attrs.mtime = 0;
	allocated=1;
    } 
    /**
    *  update the timestamp in the ientry context
    */
    rozofs_update_timestamp(nie);
    if (allocated)
    {
      /*
      ** update the attributes in the ientry
      */
      memcpy(nie->attrs.attrs.fid, mattr_obj.fid, sizeof(fid_t));
      nie->attrs.attrs.cid = mattr_obj.cid;
      memcpy(nie->attrs.attrs.sids, mattr_obj.sids, sizeof(sid_t)*ROZOFS_SAFE_MAX);
      nie->attrs.attrs.size = mattr_obj.size;
      nie->attrs.attrs.nlink = 1;
      nie->attrs.attrs.mode = S_IFREG | S_IRWXU | S_IRWXG | S_IRWXO ;
      nie->attrs.attrs.uid = 0;
      nie->attrs.attrs.gid = 0;
      nie->nlookup   = 0;
    }   
    mattr_to_stat(&nie->attrs, &stbuf,exportclt.bsize);
    stbuf.st_ino = nie->inode;

//    info("FDL %d mode %d  uid %d gid %d",allocated,nie->attrs.attrs.mode,nie->attrs.attrs.uid,nie->attrs.attrs.gid);
success:
    /*
    ** update the timestamp of the ientry (batch mode only)
    ** it applies only on regular files
    */
    if (conf.batch) rozofs_bt_local_set_regular_file_timestamp(nie->attrs.attrs.fid,nie);

    memset(&fep, 0, sizeof (fep));
    fep.ino =stbuf.st_ino;  
    if (fast_reconnect) {
      fep.attr_timeout = rozofs_get_linux_fast_reconnect_caching_time_second();
    }
    else {
      fep.attr_timeout = rozofs_get_linux_caching_time_second(nie);
    }  
    fep.entry_timeout = rozofs_tmr_get_entry(rozofs_is_directory_inode(nie->inode));
    memcpy(&fep.attr, &stbuf, sizeof (struct stat));
    nie->nlookup++;

    rozofs_inode_t * finode = (rozofs_inode_t *) nie->attrs.attrs.fid;
    fep.generation = finode->fid[0];    
    
    rz_fuse_reply_entry(req, &fep);
    errno = 0;

    rozofs_trc_rsp_attr(srv_rozofs_ll_lookup,0xfaceface,(nie==NULL)?NULL:nie->attrs.attrs.fid,0,(nie==NULL)?-1:nie->attrs.attrs.size,trc_idx);
    goto out;
}
/*
**__________________________________________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */

void rozofs_ll_lookup_cbk(void *this,void *param) 
{
   struct fuse_entry_param fep;
   ientry_t *nie = 0;
   struct stat stbuf;
   fuse_req_t req; 
   epgw_mattr_ret_no_data_t ret ;
   struct rpc_msg  rpc_reply;
   char *name;
   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   struct inode_internal_t  attrs;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_mattr_ret_no_data_t;
   rozofs_fuse_save_ctx_t *fuse_ctx_p;
   int trc_idx;
   errno = 0;
   ientry_t *pie = 0;
   struct inode_internal_t  pattrs;
   int errcode=0;
   fuse_ino_t ino = 0;
   rozofs_inode_t *fake_id_p;
   int            fast_reconnect = 0;
   
   GET_FUSE_CTX_P(fuse_ctx_p,param);  
   /*
   ** dequeue the buffer from the pending list
   */
   ruc_objRemove(param);  
    
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_PARAM(param,ino);
   RESTORE_FUSE_STRUCT_PTR(param,name);
    /*
    ** get the pointer to the transaction context:
    ** it is required to get the information related to the receive buffer
    */
    rozofs_tx_ctx_t      *rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;     
    /*    
    ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
    */
    status = rozofs_tx_get_status(this);
    if (status < 0)
    {
       /*
       ** something wrong happened
       */
       errno = rozofs_tx_get_errno(this); 
       /*
       ** In case of fast reconnect mode let's respond with the previously knows 
       ** parameters instead of failing
       */
       if ((common_config.client_fast_reconnect)&&(errno==ETIME)) {
         if (ino != 0) {
	   if (!(nie = get_ientry_by_inode(ino))) {
               errno = ENOENT;
               goto error;
	   }
           memcpy(&attrs, &nie->attrs, sizeof (struct inode_internal_t));
	   errno = EAGAIN;
           fast_reconnect = 1;	   
	   goto success;
         }
       }        
       goto error; 
    }
    /*
    ** get the pointer to the receive buffer payload
    */
    recv_buf = rozofs_tx_get_recvBuf(this);
    if (recv_buf == NULL)
    {
       /*
       ** something wrong happened
       */
       errno = EFAULT;  
       goto error;         
    }
    payload  = (uint8_t*) ruc_buf_getPayload(recv_buf);
    payload += sizeof(uint32_t); /* skip length*/
    /*
    ** OK now decode the received message
    */
    bufsize = rozofs_tx_get_small_buffer_size();
    bufsize -= sizeof(uint32_t); /* skip length*/
    xdrmem_create(&xdrs,(char*)payload,bufsize,XDR_DECODE);
    /*
    ** decode the rpc part
    */
    if (rozofs_xdr_replymsg(&xdrs,&rpc_reply) != TRUE)
    {
     TX_STATS(ROZOFS_TX_DECODING_ERROR);
     errno = EPROTO;
     goto error;
    }
    /*
    ** ok now call the procedure to encode the message
    */
    memset(&ret,0, sizeof(ret));                    
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       xdr_free((xdrproc_t) decode_proc, (char *) &ret);
       goto error;
    }   
    
    /*
    **  This gateway do not support the required eid 
    */    
    if (ret.status_gw.status == EP_FAILURE_EID_NOT_SUPPORTED) {    

        /*
        ** Do not try to select this server again for the eid
        ** but directly send to the exportd
        */
        expgw_routing_expgw_for_eid(&fuse_ctx_p->expgw_routing_ctx, ret.hdr.eid, EXPGW_DOES_NOT_SUPPORT_EID);       

        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    

        /* 
        ** Attempt to re-send the request to the exportd and wait being
        ** called back again. One will use the same buffer, just changing
        ** the xid.
        */
        status = rozofs_expgateway_resend_routing_common(rozofs_tx_ctx_p, NULL,param); 
        if (status == 0)
        {
          /*
          ** do not forget to release the received buffer
          */
          ruc_buf_freeBuffer(recv_buf);
          recv_buf = NULL;
          return;
        }           
        /*
        ** Not able to resend the request
        */
        errno = EPROTO; /* What else ? */
        goto error;
         
    }
        
    if (ret.status_gw.status == EP_FAILURE) {
        errno = ret.status_gw.ep_mattr_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);   
	
	/*
	** Case of non existent entry. 
	** Tell FUSE to keep responding ENOENT for this name for a few seconds
	*/
	if (errno == ENOENT) {
	  memset(&fep, 0, sizeof (fep));
	  errcode = errno;
	  fep.ino = 0;
	  fep.attr_timeout  = rozofs_tmr_get_enoent();
	  fep.entry_timeout = rozofs_tmr_get_enoent();
	  rz_fuse_reply_entry(req, &fep);
	  /*
	  ** OK now let's check if there was some other lookup request for the same
	  ** object
	  */
	  {
	    int trc_idx,i;      
	    for (i = 0; i < fuse_ctx_p->lkup_cpt;i++)
	    {
	       /*
	       ** Check if the inode and the name are the same
	       */
               rz_fuse_reply_entry(fuse_ctx_p->lookup_tb[i].req, &fep);
	       trc_idx = fuse_ctx_p->lookup_tb[i].trc_idx;
	       errno=errcode;
               rozofs_trc_rsp_attr(srv_rozofs_ll_lookup,0xdeadbeef,(nie==NULL)?NULL:nie->attrs.attrs.fid,status,(nie==NULL)?-1:nie->attrs.attrs.size,trc_idx);
	    }        
	  }
	  goto out;	
	}
        goto error;
    }
            
    /*
    ** Update eid free quota
    */
    eid_set_free_quota(ret.free_quota);
    
    memcpy(&attrs, &ret.status_gw.ep_mattr_ret_t_u.attrs, sizeof (struct inode_internal_t));
    /*
    ** get the parent attributes
    */
    memcpy(&pattrs, &ret.parent_attr.ep_mattr_ret_t_u.attrs, sizeof (struct inode_internal_t));
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
 
    if (!(nie = get_ientry_by_fid(attrs.attrs.fid))) {
        nie = alloc_ientry(attrs.attrs.fid);
    }     
    /*
    ** update the attributes in the ientry
    */
    rozofs_ientry_update(nie,&attrs);  
    if (ret.slave_ino_len !=0)
    {
      /*
      ** copy the slave inode information in the ientry of the master inode
      */
      int position;
      position = XDR_GETPOS(&xdrs); 
      rozofs_ientry_slave_inode_write(nie,ret.slave_ino_len,payload+position);
    }
    /*
    ** get the parent attributes
    */
    pie = get_ientry_by_fid(pattrs.attrs.fid);
    if (pie != NULL)
    {
      memcpy(&pie->attrs,&pattrs, sizeof (struct inode_internal_t));
      /**
      *  update the timestamp in the ientry context
      */
      rozofs_update_timestamp(pie);
      ientry_update_parent(nie,pie->fid);
    }   

success:    
    memset(&fep, 0, sizeof (fep));
    mattr_to_stat(&attrs, &stbuf,exportclt.bsize);
    stbuf.st_ino = nie->inode;
    /*
    ** check the case of the directory
    */
    if ((S_ISDIR(attrs.attrs.mode)) &&(strncmp(name,"@rozofs_uuid@",13) == 0))
    {
        rozofs_inode_t fake_id;
		
	fake_id.fid[1]= nie->inode;
	fake_id.s.key = ROZOFS_DIR_FID;
        fep.ino = fake_id.fid[1];  
    }
    else
    {
      fep.ino = nie->inode;
    }
    stbuf.st_size = nie->attrs.attrs.size;
    fake_id_p = (rozofs_inode_t *) attrs.attrs.fid;
    if (fake_id_p->s.del)
    {
      fep.attr_timeout  = 0;
      fep.entry_timeout = 0;
    }
    else
    {
      if (fast_reconnect) {
        fep.attr_timeout = rozofs_get_linux_fast_reconnect_caching_time_second();
      }
      else {  
        fep.attr_timeout = rozofs_get_linux_caching_time_second(nie);
      }  
      fep.entry_timeout = rozofs_tmr_get_entry(rozofs_is_directory_inode(nie->inode));    
    }
    memcpy(&fep.attr, &stbuf, sizeof (struct stat));
    nie->nlookup++;

    rozofs_inode_t * finode = (rozofs_inode_t *) nie->attrs.attrs.fid;
    fep.generation = finode->fid[0]; 
    /*
    ** update the ientry of the object with the update time of the parent directory
    ** this occur only if the parent attributes are returned
    */
    if (pie != NULL)
    {
      nie->parent_update_time = rozofs_get_parent_update_time_from_ie(pie);
    }    
    rz_fuse_reply_entry(req, &fep);
    /*
    ** OK now let's check if there was some other lookup request for the same
    ** object
    */

    {
      int trc_idx,i;      
      for (i = 0; i < fuse_ctx_p->lkup_cpt;i++)
      {
	 /*
	 ** Check if the inode and the name are the same
	 */
         rz_fuse_reply_entry(fuse_ctx_p->lookup_tb[i].req, &fep);
         nie->nlookup++;
	 trc_idx = fuse_ctx_p->lookup_tb[i].trc_idx;
         rozofs_trc_rsp_attr(srv_rozofs_ll_lookup,0xdeadbeef,(nie==NULL)?NULL:nie->attrs.attrs.fid,status,(nie==NULL)?-1:nie->attrs.attrs.size,trc_idx);
      }        
    }
    goto out;
error:
    errcode = errno;
    fuse_reply_err(req, errno);
    /*
    ** OK now let's check if there was some other lookup request for the same
    ** object
    */
    {
      int trc_idx,i;      
      for (i = 0; i < fuse_ctx_p->lkup_cpt;i++)
      {
	 /*
	 ** Check if the inode and the name are the same
	 */
         fuse_reply_err(fuse_ctx_p->lookup_tb[i].req,errcode);
	 trc_idx = fuse_ctx_p->lookup_tb[i].trc_idx;
	 errno = errcode;
         rozofs_trc_rsp_attr(srv_rozofs_ll_lookup,0xdeadbeef,(nie==NULL)?NULL:nie->attrs.attrs.fid,status,(nie==NULL)?-1:nie->attrs.attrs.size,trc_idx);
      }        
    }
out:
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp_attr(srv_rozofs_ll_lookup,(nie==NULL)?0:nie->inode,(nie==NULL)?NULL:nie->attrs.attrs.fid,status,(nie==NULL)?-1:nie->attrs.attrs.size,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_lookup);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    
    return;
}
