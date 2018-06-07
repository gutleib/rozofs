#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <uuid/uuid.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <getopt.h>
#include <pcre.h>
#include <stdarg.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/mattr.h>
#include "export.h"
#include "rozo_inode_lib.h"
#include "exp_cache.h"
#include "mdirent.h"

int rozofs_no_site_file = 0;
econfig_t exportd_config;
char * configFileName = EXPORTD_DEFAULT_CONFIG;
lv2_cache_t            cache;
mdirents_name_entry_t  bufferName;

typedef enum _scan_criterie_e {
  SCAN_CRITERIA_NONE=0,
  SCAN_CRITERIA_CR8,
  SCAN_CRITERIA_MOD,
  SCAN_CRITERIA_SIZE,  
  SCAN_CRITERIA_UID,    
  SCAN_CRITERIA_GID,  
  SCAN_CRITERIA_CID,   
  SCAN_CRITERIA_SID,   
  SCAN_CRITERIA_PROJECT,      
  SCAN_CRITERIA_NLINK, 
  SCAN_CRITERIA_CHILDREN, 
  SCAN_CRITERIA_PFID,
  SCAN_CRITERIA_FNAME,       
  SCAN_CRITERIA_UPDATE     /**< directory update time */      
} SCAN_CRITERIA_E;

SCAN_CRITERIA_E scan_criteria = SCAN_CRITERIA_NONE;

/*
** Privileges
*/
int Ux  = -1;
int Ur  = -1;
int Uw  = -1;
int Gx  = -1;
int Gr  = -1;
int Gw  = -1;
int Ox  = -1;
int Or  = -1;
int Ow  = -1;
/*
** Modification time 
*/
uint64_t    mod_lower  = -1;
uint64_t    mod_bigger = -1;
uint64_t    mod_equal  = -1;
uint64_t    mod_diff   = -1;

/*
** creation time 
*/
uint64_t    cr8_lower  = -1;
uint64_t    cr8_bigger = -1;
uint64_t    cr8_equal  = -1;
uint64_t    cr8_diff  = -1;

/*
** directory update time 
*/
uint64_t    update_lower  = -1;
uint64_t    update_bigger = -1;
uint64_t    update_equal  = -1;
uint64_t    update_diff  = -1;

/*
** Size
*/
uint64_t    size_lower  = -1;
uint64_t    size_bigger = -1;
uint64_t    size_equal  = -1;
uint64_t    size_diff  = -1;

/*
** UID
*/
uint64_t    uid_equal  = -1;
uint64_t    uid_diff   = -1;

/*
** GID
*/
uint64_t    gid_equal  = -1;
uint64_t    gid_diff  = -1;

/*
** Project
*/
uint64_t    project_equal  = -1;
uint64_t    project_diff   = -1;

/*
** CID
*/
uint64_t    cid_equal  = -1;
uint64_t    cid_diff   = -1;

/*
** SID
*/
uint64_t    sid_equal  = -1;
uint64_t    sid_diff   = -1;

/*
** NLINK
*/
uint64_t    nlink_lower  = -1;
uint64_t    nlink_bigger = -1;
uint64_t    nlink_equal  = -1;
uint64_t    nlink_diff  = -1;

/*
** Children
*/
uint64_t    children_lower  = -1;
uint64_t    children_bigger = -1;
uint64_t    children_equal  = -1;
uint64_t    children_diff  = -1;

/*
** PFID 
*/
fid_t       fid_null   = {0};
fid_t       pfid_equal = {0};

/*
** FNAME
*/
pcre      * pRegex = NULL;
char      * fname_equal = NULL;
char      * fname_bigger = NULL;

int         search_dir=0;

/*
** xatrr or not
*/
int         has_xattr=-1;
/*
** slink ? regular files ?
*/
int         exclude_symlink=1;
int         exclude_regular=0;

/*
** Trash or not trash
*/
int         exclude_trash=0;
int         only_trash=0;

/*
** Whether to scan all tracking files or only those whose
** creation and modification time match the research date
** criteria
*/
int scan_all_tracking_files = 0; // Only those matching research criteria

/*
**__________________________________________________________________
**
** Read the name from the inode
  
  @param rootPath : export root path
  @param buffer   : where to copy the name back
  @param len      : name length
*/
char * exp_read_fname_from_inode(char        * rootPath,
                              ext_mattr_t    * inode_p,
                              int            * len)
{
  char             pathname[ROZOFS_PATH_MAX+1];
  char           * p = pathname;
  rozofs_inode_t * fake_inode;
  int              fd;
  off_t            offset;
  size_t           size;
  mdirents_name_entry_t * pentry;

  
  if (inode_p->s.fname.name_type == ROZOFS_FNAME_TYPE_DIRECT) {
    * len = inode_p->s.fname.len;
    inode_p->s.fname.name[*len] = 0;
    return inode_p->s.fname.name;
  }
  
  pentry = &bufferName;

  /*
  ** Name is too big and is so in the direntry
  */

  size = inode_p->s.fname.s.name_dentry.nb_chunk * MDIRENTS_NAME_CHUNK_SZ;
  offset = DIRENT_HASH_NAME_BASE_SECTOR * MDIRENT_SECTOR_SIZE
         + inode_p->s.fname.s.name_dentry.chunk_idx * MDIRENTS_NAME_CHUNK_SZ;

  /*
  ** Start with the export root path
  */   
  p += rozofs_string_append(p,rootPath);
  p += rozofs_string_append(p, "/");

  /*
  ** Add the parent slice
  */
  fake_inode = (rozofs_inode_t *) inode_p->s.pfid;
  p += rozofs_u32_append(p, fake_inode->s.usr_id);
  p += rozofs_string_append(p, "/");

  /*
  ** Add the parent FID
  */
  p += rozofs_fid_append(p, inode_p->s.pfid);
  p += rozofs_string_append(p, "/d_");

  /*
  ** Add the root idx
  */
  p += rozofs_u32_append(p, inode_p->s.fname.s.name_dentry.root_idx);

  /*
  ** Add the collision idx
  */
  if (inode_p->s.fname.s.name_dentry.coll) {
    p += rozofs_string_append(p, "_");
    p += rozofs_u32_append(p, inode_p->s.fname.s.name_dentry.coll_idx);
  }   

  /*
  ** Open the file
  */
  fd = open(pathname,O_RDONLY);
  if (fd < 0) {
    return NULL;
  }
  
  /*
  ** Read the file
  */
  int ret = pread(fd, &bufferName, size, offset);
  close(fd);
  
  if (ret != size) {
    return NULL;
  }
  * len = pentry->len;
  pentry->name[*len] = 0;
  return pentry->name;
}
/* 
**__________________________________________________________________
**
** Check whether names matches regex
  
  @param rootPath : export root path
  @param inode_p  : the inode to check
  @param name     : the name to check
*/
int exp_check_regex(char        * rootPath,
                    ext_mattr_t * inode_p,
                    pcre        * fname_equal)
{
  char             pathname[ROZOFS_PATH_MAX+1];
  char           * p = pathname;
  rozofs_inode_t * fake_inode;
  int              fd;
  off_t            offset;
  size_t           size;
  mdirents_name_entry_t * pentry;
  
  /*
  ** Short names are stored in inode
  */
  if (inode_p->s.fname.name_type == ROZOFS_FNAME_TYPE_DIRECT) {
    /*
    ** Compare the names
    */
    if (pcre_exec (fname_equal, NULL, inode_p->s.fname.name, inode_p->s.fname.len, 0, 0, NULL, 0) == 0) {
      return 1;
    }  
    return 0;
  }
  
  /*
  ** When name length is bigger than ROZOFS_OBJ_NAME_MAX
  ** indirect mode is used
  */
  pentry = &bufferName;

  /*
  ** Name is too big and is so in the direntry
  */

  size = inode_p->s.fname.s.name_dentry.nb_chunk * MDIRENTS_NAME_CHUNK_SZ;
  
  offset = DIRENT_HASH_NAME_BASE_SECTOR * MDIRENT_SECTOR_SIZE
         + inode_p->s.fname.s.name_dentry.chunk_idx * MDIRENTS_NAME_CHUNK_SZ;

  /*
  ** Start with the export root path
  */   
  p += rozofs_string_append(p,rootPath);
  p += rozofs_string_append(p, "/");

  /*
  ** Add the parent slice
  */
  fake_inode = (rozofs_inode_t *) inode_p->s.pfid;
  p += rozofs_u32_append(p, fake_inode->s.usr_id);
  p += rozofs_string_append(p, "/");

  /*
  ** Add the parent FID
  */
  p += rozofs_fid_append(p, inode_p->s.pfid);
  p += rozofs_string_append(p, "/d_");

  /*
  ** Add the root idx
  */
  p += rozofs_u32_append(p, inode_p->s.fname.s.name_dentry.root_idx);

  /*
  ** Add the collision idx
  */
  if (inode_p->s.fname.s.name_dentry.coll) {
    p += rozofs_string_append(p, "_");
    p += rozofs_u32_append(p, inode_p->s.fname.s.name_dentry.coll_idx);
  }   

  /*
  ** Open the file
  */
  fd = open(pathname,O_RDONLY);
  if (fd < 0) {
    return 0;
  }
  
  /*
  ** Read the file
  */
  int ret = pread(fd, &bufferName, size, offset);
  close(fd);
  
  if (ret != size) {
    return 0;
  }
  /*
  ** Compare the names
  */
  if (pcre_exec (fname_equal, NULL, pentry->name, pentry->len, 0, 0, NULL, 0) == 0) {
    return 1;
  }  
  return 0;  
}      
/*
**__________________________________________________________________
**
** Check whether names are equal 
  
  @param rootPath : export root path
  @param inode_p  : the inode to check
  @param name     : the name to check
*/
int exp_are_name_equal(char        * rootPath,
                       ext_mattr_t * inode_p,
                       char        * name)
{
  char             pathname[ROZOFS_PATH_MAX+1];
  char           * p = pathname;
  rozofs_inode_t * fake_inode;
  int              fd;
  off_t            offset;
  size_t           size;
  mdirents_name_entry_t * pentry;
  int              len;

  len = strlen(name);
  
  /*
  ** Short names are stored in inode
  */
  if (len < ROZOFS_OBJ_NAME_MAX) {

    if (inode_p->s.fname.name_type != ROZOFS_FNAME_TYPE_DIRECT) {
      /* 
      ** This guy has a long name
      */
      return 0;
    }
    if (inode_p->s.fname.len != len) {
      /*
      ** Not the same length
      */
      return 0;
    }    
    /*
    ** Compare the names
    */
    if (strcmp(inode_p->s.fname.name, name)==0) {
      return 1;
    }  
    return 0;
  }
  
  /*
  ** When name length is bigger than ROZOFS_OBJ_NAME_MAX
  ** indirect mode is used
  */
  pentry = &bufferName;

  /*
  ** Name is too big and is so in the direntry
  */

  size = inode_p->s.fname.s.name_dentry.nb_chunk * MDIRENTS_NAME_CHUNK_SZ;
  if ((size-sizeof(fid_t)) < len) {
    return 0;
  }  
  
  offset = DIRENT_HASH_NAME_BASE_SECTOR * MDIRENT_SECTOR_SIZE
         + inode_p->s.fname.s.name_dentry.chunk_idx * MDIRENTS_NAME_CHUNK_SZ;

  /*
  ** Start with the export root path
  */   
  p += rozofs_string_append(p,rootPath);
  p += rozofs_string_append(p, "/");

  /*
  ** Add the parent slice
  */
  fake_inode = (rozofs_inode_t *) inode_p->s.pfid;
  p += rozofs_u32_append(p, fake_inode->s.usr_id);
  p += rozofs_string_append(p, "/");

  /*
  ** Add the parent FID
  */
  p += rozofs_fid_append(p, inode_p->s.pfid);
  p += rozofs_string_append(p, "/d_");

  /*
  ** Add the root idx
  */
  p += rozofs_u32_append(p, inode_p->s.fname.s.name_dentry.root_idx);

  /*
  ** Add the collision idx
  */
  if (inode_p->s.fname.s.name_dentry.coll) {
    p += rozofs_string_append(p, "_");
    p += rozofs_u32_append(p, inode_p->s.fname.s.name_dentry.coll_idx);
  }   

  /*
  ** Open the file
  */
  fd = open(pathname,O_RDONLY);
  if (fd < 0) {
    return 0;
  }
  
  /*
  ** Read the file
  */
  int ret = pread(fd, &bufferName, size, offset);
  close(fd);
  
  if (ret != size) {
    return 0;
  }
  if (len != pentry->len) {
    return 0;
  }  
  pentry->name[len] = 0;
  /*
  ** Compare the names
  */
  if (strcmp(pentry->name, name)==0) {
    return 1;
  }  
  return 0;
}    
/*
**_______________________________________________________________________
*/
/**
*  API to get the pathname of the objet: @rozofs_uuid@<FID_parent>/<child_name>

   @param export : pointer to the export structure
   @param inode_attr_p : pointer to the inode attribute
   @param buf: output buffer
   
   @retval buf: pointer to the beginning of the outbuffer
*/
char *rozo_get_full_path(void *exportd,void *inode_p,char *buf,int lenmax)
{
   lv2_entry_t *plv2;
   char name[1024];
   char *pbuf = buf;
   int name_len=0;
   int first=1;
   ext_mattr_t *inode_attr_p = inode_p;
   rozofs_inode_t *inode_val_p;
   
   pbuf +=lenmax;
   
   export_t *e= exportd;
   
   inode_val_p = (rozofs_inode_t*)inode_attr_p->s.pfid;
   if ((inode_val_p->fid[0]==0) && (inode_val_p->fid[1]==0))
   {
      pbuf-=2;
      pbuf[0]='.';   
      pbuf[1]=0;      
   } 
   
   buf[0] = 0;
   first = 1;
   while(1)
   {
      /*
      ** get the name of the directory
      */
      name[0]=0;
      rozolib_get_fname(inode_attr_p->s.attrs.fid,e,name,&inode_attr_p->s.fname,inode_attr_p->s.pfid);
      name_len = strlen(name);
      if (name_len == 0) break;
      if (first == 1) {
	name_len+=1;
	first=0;
      }
      pbuf -=name_len;
      memcpy(pbuf,name,name_len);
      pbuf--;
      *pbuf='/';
      if (memcmp(e->rfid,inode_attr_p->s.pfid,sizeof(fid_t))== 0)
      {
	 /*
	 ** this the root
	 */
	 pbuf--;
	 *pbuf='.';
	 return pbuf;
      }
      /*
      ** get the attributes of the parent
      */
      if (!(plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,&cache, inode_attr_p->s.pfid))) {
	break;
      }  
      inode_attr_p=  &plv2->attributes;
    }

    return pbuf;
}

/*
**_______________________________________________________________________
*/
/**
*   RozoFS specific function for visiting

   @param inode_attr_p: pointer to the inode data
   @param exportd : pointer to exporthd data structure
   @param p: always NULL
   
   @retval 0 no match
   @retval 1 match
*/
int rozofs_visit(void *exportd,void *inode_attr_p,void *p)
{
  ext_mattr_t *inode_p = inode_attr_p;
  char         fullName[ROZOFS_PATH_MAX];
  char        *pChar;
  int          nameLen;
  char       * pName;
  export_t   * e = exportd;
  
  if (search_dir==0) {
    /*
    ** Exclude symlink
    */
    if ((exclude_symlink)&&(S_ISLNK(inode_p->s.attrs.mode))) {
      return 0;
    }
    /*
    ** Exclude regular files
    */
    if ((exclude_regular)&&(S_ISREG(inode_p->s.attrs.mode))) {
      return 0;
    }     
  }   
  else {
    /*
    ** Only process directories
    */   
    if (!S_ISDIR(inode_p->s.attrs.mode)) {
      return 0;
    }       
  }

  /*
  ** Only trash
  */
  if ((only_trash)&&(!exp_metadata_inode_is_del_pending(inode_p->s.attrs.fid))) {
    return 0;
  }

  /*
  ** Exclude trash
  */
  if ((exclude_trash)&&(exp_metadata_inode_is_del_pending(inode_p->s.attrs.fid))) {
    return 0;
  }

  /*
  ** PFID must match equal_pfid
  */
  if (memcmp(pfid_equal,fid_null,sizeof(fid_t)) != 0) {
    if (memcmp(pfid_equal,inode_p->s.pfid,sizeof(fid_t)) != 0) {
      return 0;
    }  
  }

  /*
  ** Name must match fname_equal
  */
  if (fname_equal) {
    if (!exp_are_name_equal(e->root,inode_p,fname_equal)) {
      return 0;
    }  
  }
  
  /*
  ** Name must match regex
  */
  if (pRegex) {
    if (!exp_check_regex(e->root,inode_p,pRegex)) {
      return 0;
    }  
  }  
  /*
  ** Name must include fname_bigger
  */
  if (fname_bigger) {
    pName = exp_read_fname_from_inode(e->root,inode_p,&nameLen);
    if (pName==NULL) {
      return 0;
    }  
    if (nameLen < strlen(fname_bigger)) {
      return 0;
    }
    if (strstr(pName, fname_bigger)==NULL) {
      return 0;
    }  
  }
  
  /*
  ** Must have a creation time bigger than cr8_bigger
  */ 
  if (cr8_bigger != -1) {
    if (inode_p->s.cr8time < cr8_bigger) {
      return 0;
    }
  }  

  /*
  ** Must have a creation time lower than cr8_lower
  */    
  if (cr8_lower != -1) {
    if (inode_p->s.cr8time > cr8_lower) {
      return 0;
    }
  }  

  /*
  ** Must have a creation time equal to cr8_equal
  */    
  if (cr8_equal != -1) {
    if (inode_p->s.cr8time != cr8_equal) {
      return 0;
    }
  } 
  
  /*
  ** Must have a creation time different from cr8_equal
  */    
  if (cr8_diff != -1) {
    if (inode_p->s.cr8time == cr8_diff) {
      return 0;
    }
  }
   
  /*
  ** Must have a modification time bigger than mod_bigger
  */ 
  if (mod_bigger != -1) {
    if (inode_p->s.attrs.mtime < mod_bigger) {
      return 0;
    }
  }  

  /*
  ** Must have a modification time lower than mod_lower
  */    
  if (mod_lower != -1) {
    if (inode_p->s.attrs.mtime > mod_lower) {
      return 0;
    }
  }     

  /*
  ** Must have a modification time equal to mod_equal
  */    
  if (mod_equal != -1) {
    if (inode_p->s.attrs.mtime != mod_equal) {
      return 0;
    }
  } 
  
  /*
  ** Must have a modification time different from mod_diff
  */    
  if (mod_diff != -1) {
    if (inode_p->s.attrs.mtime == mod_diff) {
      return 0;
    }
  }

  /* 
  ** Privileges
  */
  
  /* User privileges */
  if (Ux != -1) {
    if (inode_p->s.attrs.mode & S_IXUSR) {
      if (!Ux) {
        return 0; 
      }
    }
    else {
      if (Ux) {
        return 0; 
      } 
    }
  }
  if (Uw != -1) {
    if (inode_p->s.attrs.mode & S_IWUSR) {
      if (!Uw) {
        return 0; 
      }
    }
    else {
      if (Uw) {
        return 0; 
      } 
    }
  }   
  if (Ur != -1) {
    if (inode_p->s.attrs.mode & S_IRUSR) {
      if (!Ur) {
        return 0; 
      }
    }
    else {
      if (Ur) {
        return 0; 
      } 
    }
  } 
  /* Group privileges */
  if (Gx != -1) {
    if (inode_p->s.attrs.mode & S_IXGRP) {
      if (!Gx) {
        return 0; 
      }
    }
    else {
      if (Gx) {
        return 0; 
      } 
    }
  }
  if (Gw != -1) {
    if (inode_p->s.attrs.mode & S_IWGRP) {
      if (!Gw) {
        return 0; 
      }
    }
    else {
      if (Gw) {
        return 0; 
      } 
    }
  }   
  if (Gr != -1) {
    if (inode_p->s.attrs.mode & S_IRGRP) {
      if (!Gr) {
        return 0; 
      }
    }
    else {
      if (Gr) {
        return 0; 
      } 
    }
  }       
  /* Others privileges */
  if (Ox != -1) {
    if (inode_p->s.attrs.mode & S_IXOTH) {
      if (!Ox) {
        return 0; 
      }
    }
    else {
      if (Ox) {
        return 0; 
      } 
    }
  }
  if (Ow != -1) {
    if (inode_p->s.attrs.mode & S_IWOTH) {
      if (!Ow) {
        return 0; 
      }
    }
    else {
      if (Ow) {
        return 0; 
      } 
    }
  }   
  if (Or != -1) {
    if (inode_p->s.attrs.mode & S_IROTH) {
      if (!Or) {
        return 0; 
      }
    }
    else {
      if (Or) {
        return 0; 
      } 
    }
  }        
    
  if (S_ISDIR(inode_p->s.attrs.mode)) 
  {
    ext_dir_mattr_t *stats_attr_p;
    stats_attr_p = (ext_dir_mattr_t *)&inode_p->s.attrs.sids[0];
    
    if (stats_attr_p->s.version >=  ROZOFS_DIR_VERSION_1)
    {
    
      /*
      ** Must have a modification time bigger than update_bigger
      */ 
      if (update_bigger != -1) {
	if (stats_attr_p->s.update_time < update_bigger) {
	  return 0;
	}
      }  

      /*
      ** Must have a modification time lower than update_lower
      */    
      if (update_lower != -1) {
	if (stats_attr_p->s.update_time > update_lower) {
	  return 0;
	}
      }     

      /*
      ** Must have a modification time equal to update_equal
      */    
      if (update_equal != -1) {
	if (stats_attr_p->s.update_time != update_equal) {
	  return 0;
	}
      } 

      /*
      ** Must have a modification time different from update_diff
      */    
      if (update_diff != -1) {
	if (stats_attr_p->s.update_time == update_diff) {
	  return 0;
	}
      }
    }
  }     
  /*
  ** Must have a size bigger than size_bigger
  */ 
  if (size_bigger != -1) {
    if (inode_p->s.attrs.size < size_bigger) {
      return 0;
    }
  }  

  /*
  ** Must have a size lower than size_lower
  */    
  if (size_lower != -1) {
    if (inode_p->s.attrs.size > size_lower) {
      return 0;
    }
  }     

  /*
  ** Must have a size equal to size_equal
  */    
  if (size_equal != -1) {
    if (inode_p->s.attrs.size != size_equal) {
      return 0;
    }
  }   

  /*
  ** Must have a size time different from size_diff
  */    
  if (size_diff != -1) {
    if (inode_p->s.attrs.size == size_diff) {
      return 0;
    }
  }
     
  
  /*
  ** Must have an uid equal to size_equal
  */    
  if (uid_equal != -1) {
    if (inode_p->s.attrs.uid != uid_equal) {
      return 0;
    }
  }         

  /*
  ** Must have an uid different from uid_diff
  */    
  if (uid_diff != -1) {
    if (inode_p->s.attrs.uid == uid_diff) {
      return 0;
    }
  }
  
  /*
  ** Must have an gid equal to size_equal
  */    
  if (gid_equal != -1) {
    if (inode_p->s.attrs.gid != gid_equal) {
      return 0;
    }
  }

  /*
  ** Must have an gid different from gid_diff
  */    
  if (gid_diff != -1) {
    if (inode_p->s.attrs.gid == gid_diff) {
      return 0;
    }
  }
  
  /*
  ** For regular files only
  */    
  if (S_ISREG(inode_p->s.attrs.mode)) {
    /*
    ** Must have a cid equal to cid_equal
    */    
    if (cid_equal != -1) {
      if (inode_p->s.attrs.cid != cid_equal) {
        return 0;
      }
    }

    /*
    ** Must have an cid different from cid_diff
    */    
    if (cid_diff != -1) {
      if (inode_p->s.attrs.cid == cid_diff) {
        return 0;
      }
    }
    
    /*
    ** Must have a sid equal to sid_equal
    */    
    if (sid_equal != -1) {
      int   sid_idx;
      sid_t sid;
      for (sid_idx=0; sid_idx<ROZOFS_SAFE_MAX_STORCLI; sid_idx++) {
        sid = inode_p->s.attrs.sids[sid_idx];
        if ((sid == 0) || (sid_equal == sid)) break;
      }
      if (sid_equal != sid) {
        return 0;
      }
    }
    
    /*
    ** Must not have a sid equal to sid_diff
    */    
    if (sid_diff != -1) {
      int   sid_idx;
      sid_t sid;
      for (sid_idx=0; sid_idx<ROZOFS_SAFE_MAX_STORCLI; sid_idx++) {
        sid = inode_p->s.attrs.sids[sid_idx];
        if ((sid == 0) || (sid_diff == sid)) break;
      }
      if (sid_diff == sid) {
        return 0;
      }
    }
    
    
  }
    
  /*
  ** Must have a nlink bigger than nlink_bigger
  */ 
  if (nlink_bigger != -1) {
    if (inode_p->s.attrs.nlink < nlink_bigger) {
      return 0;
    }
  }  

  /*
  ** Must have a nlink lower than nlink_lower
  */    
  if (nlink_lower != -1) {
    if (inode_p->s.attrs.nlink > nlink_lower) {
      return 0;
    }
  }  

  /*
  ** Must have a nlink equal to nlink_equal
  */    
  if (nlink_equal != -1) {
    if (inode_p->s.attrs.nlink != nlink_equal) {
      return 0;
    }
  } 
  
  /*
  ** Must have a nlink different from nlink_diff
  */    
  if (nlink_diff != -1) {
    if (inode_p->s.attrs.nlink == nlink_diff) {
      return 0;
    }
  }

  /*
  ** For directory only
  */    
  if (S_ISREG(inode_p->s.attrs.mode)) {

    /*
    ** Must have children bigger than children_bigger
    */ 
    if (children_bigger != -1) {
      if (inode_p->s.attrs.children < children_bigger) {
        return 0;
      }
    }  

    /*
    ** Must have a children lower than children_lower
    */    
    if (children_lower != -1) {
      if (inode_p->s.attrs.children > children_lower) {
        return 0;
      }
    }  

    /*
    ** Must have a children equal to children_equal
    */    
    if (children_equal != -1) {
      if (inode_p->s.attrs.children != children_equal) {
        return 0;
      }
    } 

    /*
    ** Must have a children different from children_diff
    */    
    if (children_diff != -1) {
      if (inode_p->s.attrs.children == children_diff) {
        return 0;
      }
    }
  }

  /*
  ** Project equals
  */
  if (project_equal != -1) {
    if (S_ISDIR(inode_p->s.attrs.mode)) {    
      if (inode_p->s.attrs.cid != project_equal) {
        return 0;
      }
    }
    else if (S_ISREG(inode_p->s.attrs.mode)) {
      if (inode_p->s.hpc_reserved.reg.share_id != project_equal) {
        return 0;
      }
    }
  }
  
  /*
  ** Project differs
  */
  if (project_diff != -1) {
    if (S_ISDIR(inode_p->s.attrs.mode)) {    
      if (inode_p->s.attrs.cid == project_diff) {
        return 0;
      }
    }
    else if (S_ISREG(inode_p->s.attrs.mode)) {
      if (inode_p->s.hpc_reserved.reg.share_id == project_diff) {
        return 0;
      }
    }
  }  
   
  /*
  ** Must have or not xattributes 
  */    
  if (has_xattr != -1) {    
    if (rozofs_has_xattr(inode_p->s.attrs.mode)) {
      if (has_xattr == 0) {
        return 0;
      }
    }
    else {
      if (has_xattr == 1) {
        return 0;
      }
    }        
  }
             
  /*
  ** This inode is valid
  */
  if (exp_metadata_inode_is_del_pending(inode_p->s.attrs.fid))
  {
    pChar = rozolib_get_relative_path(exportd,inode_attr_p, fullName,sizeof(fullName)); 
  }
  else
  {
    pChar = rozo_get_full_path(exportd,inode_attr_p, fullName,sizeof(fullName)); 
  } 

  if (pChar) {
    printf("%s\n",pChar);
  }  
  return 1;
}

/*
 *_______________________________________________________________________
 */
static void usage(char * fmt, ...) {
  va_list   args;
  char      error_buffer[512];

  /*
  ** Display optionnal error message if any
  */
  if (fmt) {
    va_start(args,fmt);
    vsprintf(error_buffer, fmt, args);
    va_end(args);   
    printf("\n\033[1m!!!  %s !!!\033[0m\n",error_buffer);
  }

  printf("\n\033[1mRozoFS File system scanning utility - %s\033[0m\n", VERSION);
  printf("This RozoFS utility enables to scan for files or (exclusive) directories in a RozoFS file system\naccording to one or several criteria and conditions.\n");
  printf("\n\033[4mUsage:\033[0m\n\t\033[1mrozo_scan [EXPORT] [OPTIONS] { <CRITERIA> } { <FIELD> <CONDITIONS> } \033[0m\n\n");
  printf("\n\033[1mEXPORT:\033[0m\n");
  printf("\tCan be omitted when current path is the RozoFS mountpoint one want to scan.\n");
  printf("\tElse the target export must be given:\n");
  printf("\tEither\t\033[1m-e,--eid <eid> [-k <cfg file>]\033[0m\t\texport identifier and optionally its configuration file.\n");
  printf("\tor\t\033[1m-p,--path <export_root_path>\033[0m\t\texport root path.\n");
  printf("\n\033[1mOPTIONS:\033[0m\n");
  printf("\t\033[1m-v,--verbose\033[0m\t\tDisplay some execution statistics.\n");
  printf("\t\033[1m-h,--help\033[0m\t\tprint this message along with examples and exit.\n");
  printf("\t\033[1m-a,--all\033[0m\t\tForce scanning all tracking files and not only those matching scan time criteria.\n");
  printf("\t\t\t\tThis is usefull for files imported with tools such as rsync, since their creation\n");
  printf("\t\t\t\tor modification dates are not related to their importation date under RozoFS.\n");
  printf("\n\033[1mCRITERIA:\033[0m\n");
  printf("\t\033[1m-x,--xattr\033[0m\t\tfile/directory must have extended attribute.\n");
  printf("\t\033[1m-X,--noxattr\033[0m\t\tfile/directory must not have extended attribute.\n");    
  printf("\t\033[1m-d,--dir\033[0m\t\tscan directories only. Without this options only files are scanned.\n");
  printf("\t\033[1m-S,--slink\033[0m\t\tinclude symbolink links in the scan. Default is not to scan symbolic links.\n");
  printf("\t\033[1m-R,--noreg\033[0m\t\texclude regular files from the scan.\n");
  printf("\t\033[1m-t,--trash\033[0m\t\tonly trashed files or directories.\n");
  printf("\t\033[1m-T,--notrash\033[0m\t\texclude trashed files and directories.\n");
  printf("\t\033[1m--U<x|w|r>\033[0m\t\tUser has <executable|write|read> priviledge.\n");
  printf("\t\033[1m--Un<x|w|r>\033[0m\t\tUser has not <executable|write|read> priviledge.\n");
  printf("\t\033[1m--G<x|w|r>\033[0m\t\tGroup has <executable|write|read> priviledge.\n");
  printf("\t\033[1m--Gn<x|w|r>\033[0m\t\tGroup has not <executable|write|read> priviledge.\n");
  printf("\t\033[1m--O<x|w|r>\033[0m\t\tOthers have <executable|write|read> priviledge.\n");
  printf("\t\033[1m--On<x|w|r>\033[0m\t\tOthers have not <executable|write|read> priviledge.\n");
  printf("\n\033[1mFIELD:\033[0m\n");
  printf("\t\033[1m-c,--cr8\033[0m\t\tcreation date.\n");
  printf("\t\033[1m-m,--mod\033[0m\t\tmodification date.\n"); 
  printf("\t\033[1m-r,--update\033[0m\t\tdirectory update date.\n"); 
  printf("\t\033[1m-s,--size\033[0m\t\tfile size.\n"); 
  printf("\t\033[1m-g,--gid\033[0m\t\tgroup identifier (1).\n"); 
  printf("\t\033[1m-u,--uid\033[0m\t\tuser identifier (1).\n"); 
  printf("\t\033[1m-C,--cid\033[0m\t\tcluster identifier (1).\n"); 
  printf("\t\033[1m-z,--sid\033[0m\t\tSID identifier (1).\n"); 
  printf("\t\033[1m-P,--project\033[0m\t\tproject identifier (1).\n"); 
  printf("\t\033[1m-l,--link\033[0m\t\tnumber of links.\n"); 
  printf("\t\033[1m-b,--children\033[0m\t\tnumber of children.\n"); 
  printf("\t\033[1m-f,--pfid\033[0m\t\tParent FID (2).\n");
  printf("\t\033[1m-n,--name\033[0m\t\tfile/directory name (3).\n");
  printf("(1) only --eq or --ne conditions are supported.\n");
  printf("(2) only --eq condition is supported.\n");
  printf("(3) only --eq, --ge or --regex conditions are supported.\n");
  printf("\n\033[1mCONDITIONS:\033[0m\n");              
  printf("\t\033[1m--lt <val>\033[0m\t\tField must be lower than <val>.\n");
  printf("\t\033[1m--le <val>\033[0m\t\tField must be lower or equal than <val>.\n");
  printf("\t\033[1m--gt <val>\033[0m\t\tField must be greater than <val>.\n");
  printf("\t\033[1m--ge <val>\033[0m\t\tField must be greater or equal than <val>.\n");
  printf("\t\t\t\tFor --name search files whoes name contains <val>.\n");
  printf("\t\033[1m--regex <regexfile>\033[0m\tOnly valid for --name.\n");
  printf("\t\t\t\t<regexfile> is the name of the text file containing the Perl regex to match.\n");
  printf("\t\033[1m--eq <val>\033[0m\t\tField must be equal to <val>.\n");
  printf("\t\033[1m--ne <val>\033[0m\t\tField must not be equal to <val>.\n");
  printf("\nDates must be expressed as:\n");
  printf(" - YYYY-MM-DD\n - \"YYYY-MM-DD HH\"\n - \"YYYY-MM-DD HH:MM\"\n - \"YYYY-MM-DD HH:MM:SS\"\n");
  
  if (fmt == NULL) {
    printf("\n\033[4mExamples:\033[0m\n");
    printf("Searching files with a size comprised between 76000 and 76100 and having extended attributes.\n");
    printf("  \033[1mrozo_scan --xattr --size --ge 76000 --le 76100\033[0m\n");
    printf("Searching files with a modification date in february 2017 but created before 2017.\n");
    printf("  \033[1mrozo_scan --mod --ge \"2017-02-01\" --lt \"2017-03-01\" --cr8 --lt \"2017-01-01\"\033[0m\n");
    printf("Searching files created by user 4501 on 2015 January the 10th in the afternoon.\n");
    printf("  \033[1mrozo_scan --uid --eq 4501 --cr8 --ge \"2015-01-10 12:00\" --le \"2015-01-11\"\033[0m\n");
    printf("Searching files owned by group 4321 in directory with FID 00000000-0000-4000-1800-000000000018.\n");
    printf("  \033[1mrozo_scan --gid --eq 4321 --pfid --eq 00000000-0000-4000-1800-000000000018\033[0m\n");
    printf("Searching files whoes name constains captainNemo.\n");
    printf("  \033[1mrozo_scan --name --ge captainNemo\033[0m\n");
    printf("Searching directories whoes name starts by a \'Z\', ends with \".DIR\" and constains at least one decimal number.\n");
    printf("  \033[1mrozo_scan --dir --name --regex /tmp/regex\033[0m\n");
    printf("  With /tmp/regex containing regex string \033[1m^Z.*\\d.*\\.DIR$\033[0m\n");    
    printf("Searching directories with more than 100K entries.\n");
    printf("  \033[1mrozo_scan --dir --children --ge 100000\033[0m\n");
    printf("Searching all symbolic links.\n");
    printf("  \033[1mrozo_scan --slink --noreg\033[0m\n");
    printf("Searching files in project #31 owned by user 2345.\n");
    printf("  \033[1mrozo_scan --project --eq 31 --uid --eq 2345\033[0m\n");
    printf("Searching files in cluster 2 having a potential projection on sid 7.\n");
    printf("  \033[1mrozo_scan --cid --eq 2 --sid --eq 7\033[0m\n");
    printf("Searching non writable files being executable by its group but not by the others.\n");
    printf("  \033[1mrozo_scan --Unw --Gx --Onx\033[0m\n");
  }
  exit(EXIT_FAILURE);     
};
/*
**_______________________________________________________________________
**
** Make a time in seconds from the epoch from a given date
**
** @param year  : the year number YYYY
** @param month : the month number in the year
** @param day   : the day number in the month 
** @param hour  : the hour number in the day 
** @param minute: the minute number in the hour 
** @param sec   : the second number in the minute 
**   
** @retval -1 when bad value are given
** @retval the date in seconds since the epoch
*/
static inline time_t rozofs_date_in_seconds(int year, int month, int day, int hour, int minute, int sec) {
  struct tm mytime = {0};
  time_t    t;
  
  mytime.tm_isdst = -1; // system should determine daylight saving time 
 
  if (year < 1900) return -1;
  mytime.tm_year = year - 1900;
  
  if (month > 12) return -1;
  mytime.tm_mon = month -1;
  
  if (day > 31) return -1;
  mytime.tm_mday = day;
  
  if (hour > 24) return -1;
  mytime.tm_hour = hour;
  
  if (minute > 60) return -1;
  mytime.tm_min = minute;
  
  if (sec > 60) return -1;
  mytime.tm_sec = sec;  
  t = mktime(&mytime); 
//  printf("%d-%2d-%2d %2d:%2d:%2d -> %d", year,month,day,hour,minute,sec,t);
  return t;
}
/*
**_______________________________________________________________________
**
**  Scan a string containing a date and compute the date in sec from the epoch
**
** @param date  : the string to scan
**   
** @retval -1 when bad string is given
** @retval the date in seconds since the epoch
*/
static inline time_t rozofs_date2time(char * date) {
  int ret;
  int year=0;
  int month=0;
  int day=0;
  int hour=0;
  int minute=0;
  int sec=0;
  
  ret = sscanf(date,"%d-%d-%d %d:%d:%d",&year,&month,&day,&hour,&minute,&sec);
  if (ret == 6) {
    return rozofs_date_in_seconds(year,month,day,hour,minute,sec);
  }    
  if (ret == 5) {
    return rozofs_date_in_seconds(year,month,day,hour,minute,0);
  }    
  if (ret == 4) {
    return rozofs_date_in_seconds(year,month,day,hour,0,0);
  }    
  if (ret == 3) {
    return rozofs_date_in_seconds(year,month,day,0,0,0);
  }
  return -1;    
  
}
/*
**_______________________________________________________________________
**
**  Scan a string containing an unsigned long integer on 64 bits
**
** @param str  : the string to scan
**   
** @retval -1 when bad string is given
** @retval the unsigned long integer value
*/
static inline uint64_t rozofs_scan_u64(char * str) {
  uint64_t val;
  int      ret;
  
  ret = sscanf(str,"%llu",(long long unsigned int *)&val);
  if (ret != 1) {
    return -1;
  }    
  return val;    
}
/*
**_______________________________________________________________________
*/
/** Find out the export root path from its eid reading the configuration file
*   
    @param  eid : eport identifier
    
    @retval -the root path or null when no such eid
*/
char * get_export_root_path(uint8_t eid) {
  list_t          * e;
  export_config_t * econfig;

  list_for_each_forward(e, &exportd_config.exports) {

    econfig = list_entry(e, export_config_t, list);
    if (econfig->eid == eid) return econfig->root;   
  }
  return NULL;
}
#if 1
#define dbg(fmt,...)
#define dbgsuccess(big,small) 
#define dbgfailed(small,big)  
#else
#define dbg(fmt,...) printf(fmt,__VA_ARGS__)
#define dbgsuccess(big,small) printf("  Success "#big" %llu later or equal "#small" %llu\n",(long long unsigned int)big, (long long unsigned int)small)
#define dbgfailed(small,big)  printf("  Failed "#small" %llu later "#big" %llu\n",(long long unsigned int)small, (long long unsigned int)big)
#endif

#define docheck(big,small) \
  if (small>big) { \
    dbgfailed(small,big);\
    return 0;\
  }\
  else {\
    dbgsuccess(big,small);\
  }  

/*
**_______________________________________________________________________
** Check whether the tracking file c an match the given date criteria
**   
**  @param  eid : eport identifier
**    
**  @retval 0 = do not read this file / 1 = read this file
*/
int rozofs_check_trk_file_date (void *export,void *inode,void *param) {
  ext_mattr_t * inode_p = inode;

  dbg("- rozofs_check_trk_file_date cr8 %llu modif %llu\n", (long long unsigned int)inode_p->s.cr8time,(long long unsigned int)inode_p->s.attrs.mtime);

  /*
  ** Must have a creation time bigger than cr8_bigger
  */ 
  if (cr8_bigger != -1) {
    /*
    ** The tracking file must have been modified at this timer or later
    */
    docheck(inode_p->s.attrs.mtime,cr8_bigger);
  }  

  /*
  ** Must have a creation time lower than cr8_lower
  */    
  if (cr8_lower != -1) {
    /*
    ** The tracking file must have been created at this time or before
    */
    docheck(cr8_lower,inode_p->s.cr8time);
  }  

  /*
  ** Must have a creation time equal to cr8_equal
  */    
  if (cr8_equal != -1) {
    /*
    ** The tracking file must have been created at this time or before
    */
    docheck(cr8_equal,inode_p->s.cr8time) ;
    /*
    ** The tracking file must have been modified at this timer or later
    */
    docheck(inode_p->s.attrs.mtime,cr8_equal);
  } 
  
   
  /*
  ** Must have a modification time bigger than mod_bigger
  */ 
  if (mod_bigger != -1) {
    /*
    ** The tracking file must have been modified at this timer or later
    */
    docheck(inode_p->s.attrs.mtime, mod_bigger);
  }  

  /*
  ** Must have a modification time lower than mod_lower
  */    
  if (mod_lower != -1) {
    /*
    ** The tracking file must have been created at this time or before
    */
    docheck(mod_lower,inode_p->s.cr8time);
  }     

  /*
  ** Must have a modification time equal to mod_equal
  */    
  if (mod_equal != -1) {
    /*
    ** The tracking file must have been created at this time or before
    */
    docheck(mod_equal,inode_p->s.cr8time);
    /*
    ** The tracking file must have been modified at this timer or later
    */
    docheck(inode_p->s.attrs.mtime,mod_equal);
  } 
  return 1; 
}
/*
**_______________________________________________________________________
**
**  M A I N
*/

#define NEW_CRITERIA(criteria){\
              if (expect_comparator) {\
                usage("Expecting --lt, --le, --gt, --ge or --ne. Got --%s", long_options[option_index].name);\
              }\
              expect_comparator = 1; \
              scan_criteria = criteria;\
              crit = c;\
}

int main(int argc, char *argv[]) {
    int   c;
    void *rozofs_export_p;
    char *root_path=NULL;
    int   eid = -1;
    int   verbose = 0;
    char  crit=0;
    char *comp;
    int   expect_comparator = 0;
    int   date_criteria_is_set = 0;
    check_inode_pf_t date_criteria_cbk;
    char  regex[1024];
     
    static struct option long_options[] = {
        {"help", no_argument, 0, 'h'},
        {"path", required_argument, 0, 'p'},
        {"eid", required_argument, 0, 'e'},
        {"config", required_argument, 0, 'k'},
        {"verbose", no_argument, 0, 'v'},
        {"cr8", no_argument, 0, 'c'},
        {"mod", no_argument, 0, 'm'},
        {"size", no_argument, 0, 's'},
        {"uid", no_argument, 0, 'u'},
        {"gid", no_argument, 0, 'g'},        
        {"cid", no_argument, 0, 'C'},        
        {"sid", no_argument, 0, 'z'},        
        {"project", no_argument, 0, 'P'},        
        {"link", no_argument, 0, 'l'},        
        {"children", no_argument, 0, 'b'},        
        {"pfid", no_argument, 0, 'f'},        
        {"name", no_argument, 0, 'n'},        
        {"xattr", no_argument, 0, 'x'}, 
        {"noxattr", no_argument, 0, 'X'},  
        {"slink", no_argument, 0, 'S'},  
        {"noreg", no_argument, 0, 'R'},  
        {"lt", required_argument, 0, '<'},
        {"le", required_argument, 0, '-'},
        {"gt", required_argument, 0, '>'},
        {"ge", required_argument, 0, '+'},
        {"eq", required_argument, 0, '='},
        {"regex", required_argument, 0, '*'},
        {"ne", required_argument, 0, '!'},
        {"dir", no_argument, 0, 'd'},
        {"all", no_argument, 0, 'a'},
        {"trash", no_argument, 0, 't'},
        {"notrash", no_argument, 0, 'T'},
        {"update", no_argument, 0, 'r'},
        {"Ux", no_argument, 0, 1},
        {"Unx", no_argument, 0, 2},
        {"Ur", no_argument, 0, 3},
        {"Unr", no_argument, 0, 4},
        {"Uw", no_argument, 0, 5},
        {"Unw", no_argument, 0, 6},
        {"Gx", no_argument, 0, 7},
        {"Gnx", no_argument, 0, 8},
        {"Gr", no_argument, 0, 9},
        {"Gnr", no_argument, 0, 10},
        {"Gw", no_argument, 0, 11},
        {"Gnw", no_argument, 0, 12},
        {"Ox", no_argument, 0, 13},
        {"Onx", no_argument, 0, 14},
        {"Or", no_argument, 0, 15},
        {"Onr", no_argument, 0, 16},
        {"Ow", no_argument, 0, 17},
        {"Onw", no_argument, 0, 18},

        {0, 0, 0, 0}
    };

    if (argc < 2)  usage(NULL);    

    for (c=0; c<argc; c++) {
      if (strcmp(argv[c],"-ge")==0) {
        usage("Argument %d is %s. Don't you mean --ge ?",c,argv[c]);
      }
      if (strcmp(argv[c],"-le")==0) {
        usage("Argument %d is %s. Don't you mean --le ?",c,argv[c]);
      }
      if (strcmp(argv[c],"-gt")==0) {
        usage("Argument %d is %s. Don't you mean --gt ?",c,argv[c]);
      }
      if (strcmp(argv[c],"-lt")==0) {
        usage("Argument %d is %s. Don't you mean --lt ?",c,argv[c]);
      }
      if (strcmp(argv[c],"-eq")==0) {
        usage("Argument %d is %s. Don't you mean --eq ?",c,argv[c]);
      }
      if (strcmp(argv[c],"-ne")==0) {
        usage("Argument %d is %s. Don't you mean --ne ?",c,argv[c]);
      }
    } 
  
    while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "p:<:-:>:+:=:!:e:k:*:hvcmsguCPlxXdbfnSRartTz", long_options, &option_index);

      if (c == -1)
          break;

      switch (c) {

          case 'h':
              usage(NULL);
              break;
          case 'a':
              scan_all_tracking_files = 1; // scan all tracking files
              break;
          case 'p':
              root_path = optarg;
              break;
          case 'k':
              configFileName = optarg;
              break;			  	                        
          case 'e':
              eid = rozofs_scan_u64(optarg);
              if (eid==-1) {
                usage("Bad format for --eid \"%s\"",optarg);     
              }
              break;    
          case 'v':
              verbose = 1;
              break;
          case  1:  Ux = 1; break;
          case  2:  Ux = 0; break;
          case  3:  Ur = 1; break;
          case  4:  Ur = 0; break;
          case  5:  Uw = 1; break;
          case  6:  Uw = 0; break;
          case  7:  Gx = 1; break;
          case  8:  Gx = 0; break;
          case  9:  Gr = 1; break;
          case 10:  Gr = 0; break;
          case 11:  Gw = 1; break;
          case 12:  Gw = 0; break;
          case 13:  Ox = 1; break;
          case 14:  Ox = 0; break;
          case 15:  Or = 1; break;
          case 16:  Or = 0; break;
          case 17:  Ow = 1; break;
          case 18:  Ow = 0; break;

          case 'c':
              NEW_CRITERIA(SCAN_CRITERIA_CR8);
              date_criteria_is_set = 1;
              break;
          case 'm':
              NEW_CRITERIA(SCAN_CRITERIA_MOD);
              date_criteria_is_set = 1;
              break;
          case 'r':
              NEW_CRITERIA(SCAN_CRITERIA_UPDATE);
              date_criteria_is_set = 1;
              break;
          case 's':
              NEW_CRITERIA(SCAN_CRITERIA_SIZE);
              break;
          case 'g':
              NEW_CRITERIA(SCAN_CRITERIA_GID);
              break;
          case 'u':
              NEW_CRITERIA(SCAN_CRITERIA_UID);
              break;                  
          case 'C':
              NEW_CRITERIA(SCAN_CRITERIA_CID);
              break;    
          case 'z':
              NEW_CRITERIA(SCAN_CRITERIA_SID);
              break;    
          case 'P':
              NEW_CRITERIA(SCAN_CRITERIA_PROJECT);
              break;                                    
          case 'l':
              NEW_CRITERIA(SCAN_CRITERIA_NLINK);
              break;                
          case 'b':
              NEW_CRITERIA(SCAN_CRITERIA_CHILDREN);
              break;                
          case 'f':
              NEW_CRITERIA(SCAN_CRITERIA_PFID);
              break;                
          case 'n':
              NEW_CRITERIA(SCAN_CRITERIA_FNAME);
              break;                
          case 'x':   
              has_xattr = 1;
              break;   
          case 'X':
              has_xattr = 0;
              break;                     
          case 'S':
              exclude_symlink = 0;
              break;                     
          case 'R':
              exclude_regular = 1;
              if (only_trash) {
                usage("only trash (-t) and exclude trash (-T) are incompatible");     
              }
              break;                     
          case 't':
              only_trash = 1;
              if (exclude_trash) {
                usage("only trash (-t) and exclude trash (-T) are incompatible");     
              }
              break;                     
          case 'T':
              exclude_trash = 1;
              break;                     
          /*
          ** Lower or equal
          */              
          case '-':
              expect_comparator = 0;
              comp = "--le";
              switch (scan_criteria) {
              
                case SCAN_CRITERIA_CR8:
                  cr8_lower = rozofs_date2time(optarg);
                  if (cr8_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break; 
                               
                case SCAN_CRITERIA_MOD:
                  mod_lower = rozofs_date2time(optarg);
                  if (mod_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break; 

                case SCAN_CRITERIA_UPDATE:
                  update_lower = rozofs_date2time(optarg);
                  if (update_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break; 
                    
                case SCAN_CRITERIA_SIZE:
                  size_lower = rozofs_scan_u64(optarg);
                  if (size_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break;  
                                   
                case SCAN_CRITERIA_NLINK:
                  nlink_lower = rozofs_scan_u64(optarg);
                  if (nlink_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break; 
                                   
                case SCAN_CRITERIA_CHILDREN:
                  children_lower = rozofs_scan_u64(optarg);
                  if (children_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break; 
                                                                    
                case SCAN_CRITERIA_GID:          
                case SCAN_CRITERIA_UID:
                case SCAN_CRITERIA_CID:
                case SCAN_CRITERIA_SID:
                case SCAN_CRITERIA_PFID:
                case SCAN_CRITERIA_FNAME:
                case SCAN_CRITERIA_PROJECT:
                  usage("No %s comparison for -%c",comp,crit);     
                  break;
                  
                default:
                  usage("No criteria defined prior to %s",comp);     
              }
              break;
          /*
          ** Lower strictly
          */              
          case '<':
              expect_comparator = 0;
              comp = "--lt";
              switch (scan_criteria) {
              
                case SCAN_CRITERIA_CR8:
                  cr8_lower = rozofs_date2time(optarg);
                  if (cr8_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  cr8_lower--;   
                  break;   
                             
                case SCAN_CRITERIA_MOD:
                  mod_lower = rozofs_date2time(optarg);
                  if (mod_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }
                  mod_lower--;    
                  break;  

                case SCAN_CRITERIA_UPDATE:
                  update_lower = rozofs_date2time(optarg);
                  if (update_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }
                  update_lower--;    
                  break;  
                  
                case SCAN_CRITERIA_SIZE:
                  size_lower = rozofs_scan_u64(optarg);
                  if (size_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  if (size_lower==0) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  size_lower--;   
                  break;     
                  
                case SCAN_CRITERIA_NLINK:
                  nlink_lower = rozofs_scan_u64(optarg);
                  if (nlink_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  if (nlink_lower==0) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  nlink_lower--;   
                  break;  
                                   
                case SCAN_CRITERIA_CHILDREN:
                  children_lower = rozofs_scan_u64(optarg);
                  if (children_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }   
                  if (children_lower==0) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  children_lower--;   
                  break;  

                                      
                case SCAN_CRITERIA_GID:          
                case SCAN_CRITERIA_UID:
                case SCAN_CRITERIA_PFID:
                case SCAN_CRITERIA_FNAME:
                  usage("No %s comparison for -%c",comp,crit);     
                  break; 
                                                
                default:
                  usage("No criteria defined prior to %s",comp);     
              }
              break;
          /*
          ** Greater or equal
          */  
          case '+':
              expect_comparator = 0;          
              comp = "--ge";         
              switch (scan_criteria) {
              
                case SCAN_CRITERIA_CR8:
                  cr8_bigger = rozofs_date2time(optarg);
                  if (cr8_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break;  
                              
                case SCAN_CRITERIA_MOD:
                  mod_bigger = rozofs_date2time(optarg);
                  if (mod_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break; 

                case SCAN_CRITERIA_UPDATE:
                  update_bigger = rozofs_date2time(optarg);
                  if (update_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break;
                  
                case SCAN_CRITERIA_SIZE:
                  size_bigger = rozofs_scan_u64(optarg);
                  if (size_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  break; 
                   
                case SCAN_CRITERIA_NLINK:
                  nlink_bigger = rozofs_scan_u64(optarg);
                  if (nlink_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  break;
                   
                case SCAN_CRITERIA_CHILDREN:
                  children_bigger = rozofs_scan_u64(optarg);
                  if (children_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  break;
                  
                case SCAN_CRITERIA_FNAME:
                  fname_bigger = optarg;
                  break;
                     
                case SCAN_CRITERIA_GID:          
                case SCAN_CRITERIA_UID:
                case SCAN_CRITERIA_CID:                
                case SCAN_CRITERIA_SID:                
                case SCAN_CRITERIA_PFID:
                case SCAN_CRITERIA_PROJECT:
                  usage("No %s comparison for -%c",comp,crit);      
                  break;                               
                                       
                default:
                  usage("No criteria defined prior to %s",comp);     
              }
              break;
          /*
          ** Greater strictly
          */                 
          case '>':
              expect_comparator = 0;          
              comp = "--gt";         
              switch (scan_criteria) {
              
                case SCAN_CRITERIA_CR8:
                  cr8_bigger = rozofs_date2time(optarg);
                  if (cr8_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }  
                  cr8_bigger++; 
                  break;        
                        
                case SCAN_CRITERIA_MOD:
                  mod_bigger = rozofs_date2time(optarg);
                  if (mod_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  mod_bigger++;  
                  break;

                case SCAN_CRITERIA_UPDATE:
                  update_bigger = rozofs_date2time(optarg);
                  if (update_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  update_bigger++;  
                  break;                  
                case SCAN_CRITERIA_SIZE:
                  size_bigger = rozofs_scan_u64(optarg);
                  if (size_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  size_bigger++;
                  break;  
                  
                case SCAN_CRITERIA_NLINK:
                  nlink_bigger = rozofs_scan_u64(optarg);
                  if (nlink_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  nlink_bigger++;
                  break;  
                  
                case SCAN_CRITERIA_CHILDREN:
                  children_bigger = rozofs_scan_u64(optarg);
                  if (children_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  children_bigger++;
                  break;  
                  
                case SCAN_CRITERIA_GID:          
                case SCAN_CRITERIA_UID:
                case SCAN_CRITERIA_CID:                
                case SCAN_CRITERIA_SID:                
                case SCAN_CRITERIA_PFID:
                case SCAN_CRITERIA_FNAME:
                case SCAN_CRITERIA_PROJECT:
                  usage("No %s comparison for -%c",comp,crit);     
                  break;                               
                                      
                default:
                  usage("No criteria defined prior to %s",comp);     
              }
              break; 
          /*
          ** Equality
          */    
          case '=':
              expect_comparator = 0;          
              comp = "--eq";        
              switch (scan_criteria) {
              
                case SCAN_CRITERIA_CR8:
                  cr8_equal = rozofs_date2time(optarg);
                  if (cr8_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  break;    
                            
                case SCAN_CRITERIA_MOD:
                  mod_equal = rozofs_date2time(optarg);
                  if (mod_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }                   
                  break;  
                  
                case SCAN_CRITERIA_UPDATE:
                  update_equal = rozofs_date2time(optarg);
                  if (update_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }                   
                  break;  
                case SCAN_CRITERIA_SIZE:
                  size_equal = rozofs_scan_u64(optarg);
                  if (size_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }
                  break; 
                  
                case SCAN_CRITERIA_GID:
                  gid_equal = rozofs_scan_u64(optarg);
                  if (gid_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }   
                  break;
                  
                case SCAN_CRITERIA_NLINK:
                  nlink_equal = rozofs_scan_u64(optarg);
                  if (nlink_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }   
                  break;
                                                                      
                case SCAN_CRITERIA_CHILDREN:
                  children_equal = rozofs_scan_u64(optarg);
                  if (children_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }   
                  break;
                                                                      
                case SCAN_CRITERIA_UID:
                  uid_equal = rozofs_scan_u64(optarg);
                  if (uid_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }   
                  break;   
                  
                case SCAN_CRITERIA_CID:
                  cid_equal = rozofs_scan_u64(optarg);
                  if (cid_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }   
                  break;
                  
                case SCAN_CRITERIA_SID:
                  sid_equal = rozofs_scan_u64(optarg);
                  if (sid_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }   
                  break;                  
                  
                case SCAN_CRITERIA_PROJECT:
                  project_equal = rozofs_scan_u64(optarg);
                  if (project_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }   
                  break;
                  
                case SCAN_CRITERIA_PFID:
                  if (rozofs_uuid_parse(optarg, pfid_equal)!=0) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }   
                  break;                                                                         
                  
                case SCAN_CRITERIA_FNAME:
                  fname_equal = optarg;
                  break;                                                                         
                                                                         
                                                                         
                default:
                  usage("No criteria defined prior to %s",comp);     
              }
              break; 
          /*
          ** Regex
          */    
          case '*':
              expect_comparator = 0;          
              comp = "--regex";        
              switch (scan_criteria) {
              
                case SCAN_CRITERIA_FNAME:
                {  
                  FILE*       f;
                  const char *pcreErrorStr;     
                  int         pcreErrorOffset;
                  int         index;
                  
                  /*
                  ** Open regex file
                  */
                  f = fopen(optarg, "r");
                  if (f == NULL) {
                    usage("Can not open file %s (%s)", optarg, strerror(errno));                 
                  } 
                  /*
                  ** Read regex
                  */
                  if (fread(regex, sizeof(regex), 1, f) != 0) {       
                    fclose(f);                           
                    usage("Can not read file %s (%s)", optarg, strerror(errno));
                  } 
                  fclose(f);
                  /*
                  ** Compile the regex
                  */
                  index = 0;
                  while (regex[index] != 0) {
                    if (regex[index] == '\n') {
                      regex[index] = 0;
                      break;
                    }
                    index++;
                  }    
                  pRegex = pcre_compile(regex, 0, &pcreErrorStr, &pcreErrorOffset, NULL);
                  if(pRegex == NULL) {
                    usage("Bad regex \"%s\" at offset %d : %s", regex, pcreErrorOffset, pcreErrorStr);  
                  }
                }  
                break; 
                                                                         
                default:
                  usage("No criteria defined prior to %s",comp);     
              }
              break;  

          /*
          ** Different
          */    
          case '!':
              expect_comparator = 0;          
              comp = "--ne";        
              switch (scan_criteria) {
              
                case SCAN_CRITERIA_CR8:
                  cr8_diff = rozofs_date2time(optarg);
                  if (cr8_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  break; 
                               
                case SCAN_CRITERIA_MOD:
                  mod_diff = rozofs_date2time(optarg);
                  if (mod_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }                   
                  break; 

                case SCAN_CRITERIA_UPDATE:
                  update_diff = rozofs_date2time(optarg);
                  if (update_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }                   
                  break; 
                   
                case SCAN_CRITERIA_SIZE:
                  size_diff = rozofs_scan_u64(optarg);
                  if (size_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }
                  break; 
                   
                case SCAN_CRITERIA_NLINK:
                  nlink_diff = rozofs_scan_u64(optarg);
                  if (nlink_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }
                  break; 
                   
                case SCAN_CRITERIA_CHILDREN:
                  children_diff = rozofs_scan_u64(optarg);
                  if (children_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }
                  break; 
                  
                case SCAN_CRITERIA_GID:
                  gid_diff = rozofs_scan_u64(optarg);
                  if (gid_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }   
                  break; 
                                  
                case SCAN_CRITERIA_UID:
                  uid_diff = rozofs_scan_u64(optarg);
                  if (uid_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }   
                  break; 
                    
                case SCAN_CRITERIA_CID:
                  cid_diff = rozofs_scan_u64(optarg);
                  if (cid_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }   
                  break;
                    
                case SCAN_CRITERIA_SID:
                  sid_diff = rozofs_scan_u64(optarg);
                  if (sid_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }   
                  break;
                    
                case SCAN_CRITERIA_PROJECT:
                  project_diff = rozofs_scan_u64(optarg);
                  if (project_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }   
                  break;
                  
                case SCAN_CRITERIA_PFID:
                case SCAN_CRITERIA_FNAME:
                  usage("No %s comparison for -%c",comp,crit);      
                  break;                                                   
                                                                         
                default:
                  usage("No criteria defined prior to %s",comp);     
              }
              break;                
          case 'd':
              search_dir = 1;
              break;                               
          case '?':
          default:
              if (optopt)  usage("Unexpected argument \"-%c\"", optopt);
              else         usage("Unexpected argument \"%s\"", argv[optind-1]);
              break;
      }
  }
  
  if (expect_comparator) {
    usage("Expecting --lt, --le, --gt, --ge or --ne.");
  }                     

  /*
  ** Search for the given eid in configuration file
  ** in case one is given as input
  */
  if (eid!=-1) {
    /*
    ** Read configuration file
    */
    if (econfig_initialize(&exportd_config) != 0) {
      usage("can't initialize exportd config %s",strerror(errno));
    }    
    if (econfig_read(&exportd_config, configFileName) != 0) {
      usage("failed to parse configuration file %s %s",configFileName,strerror(errno));
    }              	 
    /*
    ** Find the export root path
    */
    root_path = get_export_root_path(eid);
    if (root_path==NULL) {
      usage("eid %d is not configured",eid);       
    }
  }

  if (root_path == NULL) 
  {
    usage("Missing root_path(-p) or export identifier (-e)");
  }

  /*
  ** init of the RozoFS data structure on export
  ** in order to permit the scanning of the exportd
  */
  rozofs_export_p = rz_inode_lib_init(root_path);
  if (rozofs_export_p == NULL)
  {
    usage("RozoFS: error while reading %s",root_path);
  }
  /*
  ** init of the lv2 cache
  */
  lv2_cache_initialize(&cache);
  rz_set_verbose_mode(verbose);
  
  /*
  ** Use call back to reject a whole attribute file when date criteria is set
  */
  date_criteria_cbk = NULL;
  if (!scan_all_tracking_files && date_criteria_is_set) {
    date_criteria_cbk = rozofs_check_trk_file_date;
  }

  
  if (search_dir) {
    rz_scan_all_inodes(rozofs_export_p,ROZOFS_DIR,1,rozofs_visit,NULL,date_criteria_cbk,NULL);
  }
  else {
    rz_scan_all_inodes(rozofs_export_p,ROZOFS_REG,1,rozofs_visit,NULL,date_criteria_cbk,NULL);
  }
  exit(EXIT_SUCCESS);  
  return 0;
}
