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

uint64_t nb_scanned_entries = 0;
uint64_t nb_matched_entries = 0;
uint64_t max_display = -1;

typedef enum _scan_criterie_e {
  SCAN_CRITERIA_NONE=0,
  SCAN_CRITERIA_HCR8,
  SCAN_CRITERIA_SCR8,
  SCAN_CRITERIA_HMOD,
  SCAN_CRITERIA_SMOD,
  SCAN_CRITERIA_HCTIME,
  SCAN_CRITERIA_SCTIME,
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
  SCAN_CRITERIA_HUPDATE,     /**< directory update time */      
  SCAN_CRITERIA_SUPDATE,     /**< directory update time */      
  SCAN_CRITERIA_DELETED, 
} SCAN_CRITERIA_E;

SCAN_CRITERIA_E scan_criteria = SCAN_CRITERIA_NONE;
/*
** Ouput format
*/
int entry_per_line = 1;
int cur_entry_per_line = 0;
/*
** Name output format
*/
typedef enum _name_format_e {
  name_format_full = 0,
  name_format_relative,
  name_format_fid,
} name_format_e;
name_format_e name_format = name_format_full;

int display_size = 0;
int display_children = 0;
int display_deleted = 0;
int display_nlink = 0;
int display_project = 0;
int display_uid = 0;
int display_gid = 0;
int display_cr8 = 0;
int display_mod = 0;
int display_ctime = 0;
int display_update = 0;
int display_atime = 0;
int display_priv = 0;
int display_distrib = 0;
int display_id = 0;
int display_xattr = 0;
int display_all = 0;
int display_json = 0;
int first_entry = 1;
#define DO_DISPLAY           1
#define DO_HUMAN_DISPLAY     2

#define IF_DISPLAY(x)  if ((x!=0) || (display_all!=0))
#define IF_DISPLAY_HUMAN(x) if ((x==DO_HUMAN_DISPLAY) || (display_all==DO_HUMAN_DISPLAY))

char separator[128] = {0};

#define NEW_FIELD(field) {\
  if (display_json) {\
    if (strlen(#field) < 6) printf(",\n       \""#field"\" \t\t: ");\
    else                    printf(",\n       \""#field"\" \t: ");\
  }\
  else {\
    if (separator[0]) printf(" %s "#field"=",separator);\
    else printf(" "#field"=");\
  } \
}
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
** change time 
*/
uint64_t    ctime_lower  = -1;
uint64_t    ctime_bigger = -1;
uint64_t    ctime_equal  = -1;
uint64_t    ctime_diff   = -1;
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
** Delcount
*/
uint64_t    deleted_lower  = -1;
uint64_t    deleted_bigger = -1;
uint64_t    deleted_equal  = -1;
uint64_t    deleted_diff  = -1;


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
** Scan junk files
*/
int         only_junk=0;
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
  int              nameLen;
  char           * pName;
  
  /*
  ** Short names are stored in inode
  ** When name length is bigger than ROZOFS_OBJ_NAME_MAX
  ** indirect mode is used
  */
  pName = exp_read_fname_from_inode(rootPath,inode_p,&nameLen);    

  /*
  ** Compare the names
  */
  if (pcre_exec (fname_equal, NULL, pName, nameLen, 0, 0, NULL, 0) == 0) {
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
  int              len;
  int              nameLen;
  char           * pName;
  
  len = strlen(name);

  /*
  ** When name length is bigger than ROZOFS_OBJ_NAME_MAX
  ** indirect mode is used
  */
  pName = exp_read_fname_from_inode(rootPath,inode_p,&nameLen);
  
  if (nameLen != len) {
    /*
    ** Not the same length
    */
    return 0;
  }    
  /*
  ** Compare the names
  */
  if (strcmp(pName, name)==0) {
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
char *rozo_get_path(void *exportd,void *inode_p,char *buf,int lenmax, int relative)
{
   lv2_entry_t *plv2;
   char name[1024];
   char *pbuf = buf;
   int name_len=0;
   int first=1;
   ext_mattr_t *inode_attr_p = inode_p;
   rozofs_inode_t *inode_val_p;
   int was_in_trash=0;
   
   
   export_t *e= exportd;

   /*
   ** This is the root.
   */        
   if (memcmp(e->rfid,inode_attr_p->s.attrs.fid,sizeof(fid_t))== 0)
   {         
      pbuf[0] = '.';
      pbuf[1] = 0;
      return pbuf;
   }
   
   inode_val_p = (rozofs_inode_t*)inode_attr_p->s.pfid;
   if ((inode_val_p->fid[0]==0) && (inode_val_p->fid[1]==0))
   if (memcmp(e->rfid,inode_attr_p->s.attrs.fid,sizeof(fid_t))== 0)
   {         
      pbuf[0] = '.';
      pbuf[1] = 0;
      return pbuf;
   }

   pbuf +=lenmax;
   
   buf[0] = 0;
   first = 1;
   while(1)
   {
      /*
      ** get the name of the current inode
      */
      name[0]=0;
      rozolib_get_fname(inode_attr_p->s.attrs.fid,e,name,&inode_attr_p->s.fname,inode_attr_p->s.pfid);
      if (name[0]== 0) {
        /*
        ** Long name and parent FID may not exist any more. Put FID only
        */
        pbuf = buf;
        pbuf += sprintf(pbuf,"./@rozofs_uuid@");
        rozofs_fid_append(pbuf,inode_attr_p->s.attrs.fid);
        return buf;   
      }
      
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

      /*
      ** Memorize whether the current FID is in trash or not ?
      */
      if (exp_metadata_inode_is_del_pending(inode_attr_p->s.attrs.fid)) {
        was_in_trash = 1;
      }  
      else {
        was_in_trash = 0;
      } 
            
      /*
      ** Parent is the root. This is the end of the loop
      */        
      if (memcmp(e->rfid,inode_attr_p->s.pfid,sizeof(fid_t))== 0)
      {
         /*
         ** So far FID were trash FIDs...
         ** Add the trash mark.
         */
         if (was_in_trash) {
           pbuf -= strlen("/@rozofs-trash@");
           memcpy(pbuf,"/@rozofs-trash@",strlen("/@rozofs-trash@"));          
         }            
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
        /*
        ** Parent FID does not exist any more. Put FID only
        */
        pbuf = buf;
        pbuf += sprintf(pbuf,"./@rozofs_uuid@");
        rozofs_fid_append(pbuf,inode_attr_p->s.attrs.fid);
        return buf;   
      }      
      inode_attr_p=  &plv2->attributes;
      /*
      ** So far FID were trash FIDs...
      */
      if (was_in_trash) {
        /*
        ** .. but now this parent is not in trash.
        ** Introduce the trash mark in the name
        */
        if (!exp_metadata_inode_is_del_pending(inode_attr_p->s.attrs.fid)) {
          pbuf -= strlen("/@rozofs-trash@");
          memcpy(pbuf,"/@rozofs-trash@",strlen("/@rozofs-trash@")); 
        }         
      }
      
      /*
      ** In relative mode just add the parent fid
      */
      if (relative) {
	 pbuf -= strlen("./@rozofs_uuid@");
         pbuf -= 36;
         memcpy(pbuf,"./@rozofs_uuid@",strlen("./@rozofs_uuid@")); 
         rozofs_fid_append(pbuf+strlen("./@rozofs_uuid@"),inode_attr_p->s.attrs.fid);
         pbuf[strlen("./@rozofs_uuid@")+36] = '/';
	 return pbuf;        
      }        
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

  nb_scanned_entries++;
  
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
  ** Must have a change time bigger than ctime_bigger
  */ 
  if (ctime_bigger != -1) {
    if (inode_p->s.attrs.ctime < ctime_bigger) {
      return 0;
    }
  }  

  /*
  ** Must have a change time lower than ctime_lower
  */    
  if (ctime_lower != -1) {
    if (inode_p->s.attrs.ctime > ctime_lower) {
      return 0;
    }
  }     

  /*
  ** Must have a change time equal to ctime_equal
  */    
  if (ctime_equal != -1) {
    if (inode_p->s.attrs.ctime != ctime_equal) {
      return 0;
    }
  } 
  
  /*
  ** Must have a change time different from ctime_diff
  */    
  if (ctime_diff != -1) {
    if (inode_p->s.attrs.ctime == ctime_diff) {
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
    ext_dir_mattr_t * stats_attr_p = (ext_dir_mattr_t *)&inode_p->s.attrs.sids[0];
    
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
    /*
    ** Must have a size bigger than size_bigger
    */ 
    if (size_bigger != -1) {
      if (stats_attr_p->s.nb_bytes < size_bigger) {
        return 0;
      }
    }  

    /*
    ** Must have a size lower than size_lower
    */    
    if (size_lower != -1) {
      if (stats_attr_p->s.nb_bytes  > size_lower) {
        return 0;
      }
    }     

    /*
    ** Must have a size equal to size_equal
    */    
    if (size_equal != -1) {
      if (stats_attr_p->s.nb_bytes  != size_equal) {
        return 0;
      }
    }   

    /*
    ** Must have a size time different from size_diff
    */    
    if (size_diff != -1) {
      if (stats_attr_p->s.nb_bytes  == size_diff) {
        return 0;
      }
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
    
  }

  /*
  ** For directory only
  */    
  if (S_ISDIR(inode_p->s.attrs.mode)) {
    
    uint64_t deleted = inode_p->s.hpc_reserved.dir.nb_deleted_files;

    /*
    ** Must have delete count bigger than deleted_bigger
    */ 
    if (deleted_bigger != -1) {
      if (deleted < deleted_bigger) {
        return 0;
      }
    }  

    /*
    ** Must have a delete count lower than deleted_lower
    */    
    if (deleted_lower != -1) {
      if (deleted > deleted_lower) {
        return 0;
      }
    }  

    /*
    ** Must have a delete count equal to deleted_equal
    */    
    if (deleted_equal != -1) {
      if (deleted != deleted_equal) {
        return 0;
      }
    } 

    /*
    ** Must have a delete count different from deleted_diff
    */    
    if (deleted_diff != -1) {
      if (deleted == deleted_diff) {
        return 0;
      }
    }
  
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
  nb_matched_entries++;
    
  switch(name_format) {

    case name_format_fid:
      pChar = fullName;    
      pChar += sprintf(pChar,"./@rozofs_uuid@");
      rozofs_fid_append(pChar,inode_p->s.attrs.fid);
      pChar = fullName;    
      break;

    case name_format_relative:
      pChar = rozo_get_path(exportd,inode_attr_p, fullName,sizeof(fullName),1);
      break;
      pChar = fullName;    
      pChar += sprintf(pChar,"./@rozofs_uuid@");
      pChar += rozofs_fid_append(pChar,inode_p->s.pfid);
      if (exp_metadata_inode_is_del_pending(inode_p->s.attrs.fid)) {
        pChar += sprintf(pChar,"/@rozofs-trash@");  
      }
      pName = exp_read_fname_from_inode(e->root,inode_p,&nameLen);        
      pChar += sprintf(pChar,"/%s",pName);        
      pChar  = fullName;    
      break;

    default:        
      pChar = rozo_get_path(exportd,inode_attr_p, fullName,sizeof(fullName),0);
  }     

  /*
  ** Put entry_per_line entries per line
  */
  if (pChar==NULL) {
    return 0;
  }  

  
  if (display_json) {
    if (first_entry) {
      first_entry = 0;
      printf("\n    {  \"name\" : \"%s\"",pChar);
    }
    else {
      printf(",\n    {  \"name\" : \"%s\"",pChar);
    }   
  }
  else {  
    printf("%s",pChar);
  }

  /*
  ** User id
  */  
  IF_DISPLAY(display_uid) {
    NEW_FIELD(uid); 
    printf("%d", inode_p->s.attrs.uid);
  }

  /*
  ** Group id
  */  
  IF_DISPLAY(display_gid) {
    NEW_FIELD(gid);   
    printf("%d", inode_p->s.attrs.gid);
  }
  
  /*
  ** Linuxx privileges
  */  
  IF_DISPLAY(display_priv) {
    NEW_FIELD(priv);  
    if (display_json) {
      printf("\"%4.4o\"", inode_p->s.attrs.mode & (S_IRWXU|S_IRWXG|S_IRWXO));
    }
    else {
      printf("%4.4o", inode_p->s.attrs.mode & (S_IRWXU|S_IRWXG|S_IRWXO));
    }  
  }
  
  /*
  ** Presence of extended attributes
  */
  IF_DISPLAY(display_xattr) {
    NEW_FIELD(xattr);  
    if (rozofs_has_xattr(inode_p->s.attrs.mode)) {
      printf("\"YES\"");
    }
    else {
      printf("\"NO\"");
    }
  }

  /*
  ** Directory
  */
  if (S_ISDIR(inode_p->s.attrs.mode)) {
    ext_dir_mattr_t *ext_dir_mattr_p = (ext_dir_mattr_t*)inode_p->s.attrs.sids; 

    IF_DISPLAY(display_size) {
      NEW_FIELD(size); 
      printf("%llu",(long long unsigned)ext_dir_mattr_p->s.nb_bytes);        
    }
    IF_DISPLAY(display_children) {       
      NEW_FIELD(children); 
      printf("%llu",(long long unsigned)inode_p->s.attrs.children);                
    }
    IF_DISPLAY(display_deleted) {       
      uint64_t deleted = inode_p->s.hpc_reserved.dir.nb_deleted_files;
      NEW_FIELD(deleted); 
      printf("%llu",(long long unsigned)deleted);                
    }
    IF_DISPLAY(display_project) {
      NEW_FIELD(project);       
      printf("%d", inode_p->s.attrs.cid);
    }
    IF_DISPLAY(display_update) {
      ext_dir_mattr_t * stats_attr_p = (ext_dir_mattr_t *)&inode_p->s.attrs.sids[0];
      if (stats_attr_p->s.version >=  ROZOFS_DIR_VERSION_1) {
        IF_DISPLAY_HUMAN(display_update) {
          char buftime[512];
          NEW_FIELD(hupdate);
          rozofs_time2string(buftime,stats_attr_p->s.update_time);            
          printf("\"%s\"", buftime);
        }
        else {
          NEW_FIELD(supdate);
          printf("%llu",(long long unsigned int)stats_attr_p->s.update_time);  
        }  
      }
    }
  }

  /*
  ** Regular file
  */
  else {  
    IF_DISPLAY(display_size) {
      NEW_FIELD(size);       
      printf("%llu",(long long unsigned)inode_p->s.attrs.size);        
    }  
    IF_DISPLAY(display_nlink) {       
      NEW_FIELD(nlink); 
      printf("%llu",(long long unsigned)inode_p->s.attrs.nlink);                
    }
    IF_DISPLAY(display_project) {
      NEW_FIELD(project);       
      printf("%d", inode_p->s.hpc_reserved.reg.share_id);
    }
    IF_DISPLAY(display_distrib) {
      NEW_FIELD(cid); 
      printf("%u",inode_p->s.attrs.cid);
      NEW_FIELD(sid); 
        
      printf("[%u", inode_p->s.attrs.sids[0]);
      int sid_idx;
      for (sid_idx=1; sid_idx<ROZOFS_SAFE_MAX_STORCLI; sid_idx++) {
        if (inode_p->s.attrs.sids[sid_idx] == 0) break;
        printf(",%u",inode_p->s.attrs.sids[sid_idx]);
      }
      printf("]");
    }     
  } 
  
  IF_DISPLAY(display_id) {
    char fidString[40];
    NEW_FIELD(fid);  
    rozofs_fid_append(fidString,inode_p->s.attrs.fid);
    printf("\"%s\"", fidString);       
  }     
   
  IF_DISPLAY(display_cr8) {
    IF_DISPLAY_HUMAN(display_cr8) {
      char buftime[512];
      rozofs_time2string(buftime,inode_p->s.cr8time);  
      NEW_FIELD(hcr8); 
      printf("\"%s\"", buftime); 
    }  
    else {
      NEW_FIELD(scr8);
      printf("%llu",(long long unsigned int)inode_p->s.cr8time);  
    }  
  }
  
  IF_DISPLAY(display_mod) {
    IF_DISPLAY_HUMAN(display_mod) {
      char buftime[512];
      rozofs_time2string(buftime,inode_p->s.attrs.mtime);  
      NEW_FIELD(hmod); 
      printf("\"%s\"", buftime);
    }        
    else {
      NEW_FIELD(smod);
      printf("%llu",(long long unsigned int)inode_p->s.attrs.mtime);  
    }  
  }
  
  IF_DISPLAY(display_ctime) {
    IF_DISPLAY_HUMAN(display_ctime) {
      char buftime[512];
      rozofs_time2string(buftime,inode_p->s.attrs.ctime);  
      NEW_FIELD(hctime); 
      printf("\"%s\"", buftime);
    }        
    else {
      NEW_FIELD(sctime);
      printf("%llu",(long long unsigned int)inode_p->s.attrs.ctime);  
    }  
  }   

  IF_DISPLAY(display_atime) {
    IF_DISPLAY_HUMAN(display_atime) {
      char buftime[512];
      rozofs_time2string(buftime,inode_p->s.attrs.atime);  
      NEW_FIELD(hatime); 
      printf("\"%s\"", buftime);
    }        
    else {
      NEW_FIELD(satime);
      printf("%llu",(long long unsigned int)inode_p->s.attrs.atime);  
    }  
  }   

  if (display_json) {
    printf("  }");    
  }
  else {   
    cur_entry_per_line++;
    if (cur_entry_per_line >= entry_per_line) {
      cur_entry_per_line = 0;
      printf("\n");
    }
    else {
      printf("  %s ", separator);
    } 
  } 
  
  if (nb_matched_entries >= max_display) {
    rozo_lib_stop_var = 1;
  }    
  return 1;
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
int rozofs_visit_junk(void *exportd,void *inode_attr_p,void *p)
{
  rmfentry_disk_t *rmentry = inode_attr_p;
//  export_t   * e = exportd;
  char         fullName[64];

  nb_scanned_entries++;

  /*
  ** Must have a size bigger than size_bigger
  */ 
  if (size_bigger != -1) {
    if (rmentry->size < size_bigger) {
      return 0;
    }
  }  

  /*
  ** Must have a size lower than size_lower
  */    
  if (size_lower != -1) {
    if (rmentry->size > size_lower) {
      return 0;
    }
  }     

  /*
  ** Must have a size equal to size_equal
  */    
  if (size_equal != -1) {
    if (rmentry->size != size_equal) {
      return 0;
    }
  }   

  /*
  ** Must have a size time different from size_diff
  */    
  if (size_diff != -1) {
    if (rmentry->size == size_diff) {
      return 0;
    }
  }

  /*
  ** Must have a cid equal to cid_equal
  */    
  if (cid_equal != -1) {
    if (rmentry->cid != cid_equal) {
      return 0;
    }
  }

  /*
  ** Must have an cid different from cid_diff
  */    
  if (cid_diff != -1) {
    if (rmentry->cid == cid_diff) {
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
      sid = rmentry->current_dist_set[sid_idx];
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
      sid = rmentry->current_dist_set[sid_idx];
      if ((sid == 0) || (sid_diff == sid)) break;
    }
    if (sid_diff == sid) {
      return 0;
    }
  }
             
  /*
  ** This inode is valid
  */
  nb_matched_entries++;
   
  rozofs_fid_append(fullName,rmentry->trash_inode);

  
  if (display_json) {
    if (first_entry) {
      first_entry = 0;
      printf("\n    {  \"name\" : \"%s\"",fullName);
    }
    else {
      printf(",\n    {  \"name\" : \"%s\"",fullName);
    }   
  }
  else {  
    printf("%s",fullName);
  }

  IF_DISPLAY(display_size) {
    NEW_FIELD(size);       
    printf("%llu",(long long unsigned)rmentry->size);        
  }  

  IF_DISPLAY(display_distrib) {
    NEW_FIELD(cid); 
    printf("%u",rmentry->cid);
    NEW_FIELD(sid); 

    printf("[%u", rmentry->current_dist_set[0]);
    int sid_idx;
    for (sid_idx=1; sid_idx<ROZOFS_SAFE_MAX_STORCLI; sid_idx++) {
      printf(",%u",rmentry->current_dist_set[sid_idx]);
    }
    printf("]");
  }     
  
  IF_DISPLAY(display_id) {
    NEW_FIELD(fid);  
    rozofs_fid_append(fullName,rmentry->fid);
    printf("\"%s\"", fullName);       
  }        

  if (display_json) {
    printf("  }");    
  }
  else {   
    cur_entry_per_line++;
    if (cur_entry_per_line >= entry_per_line) {
      cur_entry_per_line = 0;
      printf("\n");
    }
    else {
      printf("  %s ", separator);
    } 
  } 
  
  if (nb_matched_entries >= max_display) {
    rozo_lib_stop_var = 1;
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
  printf("\n\033[4mUsage:\033[0m\n\t\033[1mrozo_scan [FILESYSTEM] [OPTIONS] { <CRITERIA> } { <FIELD> <CONDITIONS> } [OUTPUT]\033[0m\n\n");
  printf("\n\033[1mFILESYSTEM:\033[0m\n");
  printf("\tThe FILESYSTEM can be omitted when current path is a RozoFS mountpoint on the file system one want to scan.\n");
  printf("\tElse the targeted RozoFS file system must be provided by eid or by path:\n");
  printf("\tEither\t\033[1m-e,--eid <eid> [-k <cfg file>]\033[0m\t\texport identifier and optionally its configuration file.\n");
  printf("\tor    \t\033[1m-p,--path <export_root_path>\033[0m\t\texport root path.\n");
  printf("\n\033[1mOPTIONS:\033[0m\n");
  printf("\t\033[1m-v,--verbose\033[0m\t\tDisplay some execution statistics.\n");
  printf("\t\033[1m-h,--help\033[0m\t\tprint this message along with examples and exit.\n");
  printf("\t\033[1m-a,--all\033[0m\t\tForce scanning all tracking files and not only those matching scan time criteria.\n");
  printf("\t\t\t\tThis is usefull for files imported with tools such as rsync, since their creation\n");
  printf("\t\t\t\tor modification dates are not related to their importation date under RozoFS.\n");
  printf("\n\033[1mCRITERIA:\033[0m\n");
  printf("\t\033[1m-x,--xattr\033[0m\t\tfile/directory must have extended attribute.\n");
  printf("\t\033[1m-X,--noxattr\033[0m\t\tfile/directory must not have extended attribute.\n");    
  printf("\t\033[1m-d,--dir\033[0m\t\tscan directories only. Without this option only files are scanned.\n");
  printf("\t\033[1m-S,--slink\033[0m\t\tinclude symbolink links in the scan. Default is not to scan symbolic links.\n");
  printf("\t\033[1m-R,--noreg\033[0m\t\texclude regular files from the scan.\n");
  printf("\t\033[1m-t,--trash\033[0m\t\tonly trashed files or directories.\n");
  printf("\t\033[1m-T,--notrash\033[0m\t\texclude trashed files and directories.\n");
  printf("\t\033[1m-j,--junk\033[0m\t\tScan junk files waiting for deletion process.\n");
  printf("\t\033[1m--U<x|w|r>\033[0m\t\tUser has <executable|write|read> priviledge.\n");
  printf("\t\033[1m--Un<x|w|r>\033[0m\t\tUser has not <executable|write|read> priviledge.\n");
  printf("\t\033[1m--G<x|w|r>\033[0m\t\tGroup has <executable|write|read> priviledge.\n");
  printf("\t\033[1m--Gn<x|w|r>\033[0m\t\tGroup has not <executable|write|read> priviledge.\n");
  printf("\t\033[1m--O<x|w|r>\033[0m\t\tOthers have <executable|write|read> priviledge.\n");
  printf("\t\033[1m--On<x|w|r>\033[0m\t\tOthers have not <executable|write|read> priviledge.\n");
  printf("\n\033[1mFIELD:\033[0m\n");
  printf("\t\033[1m-c,--hcr8\033[0m\t\tcreation date in human readable format.\n");
  printf("\t\033[1m-A,--scr8\033[0m\t\tcreation date in seconds.\n");
  printf("\t\033[1m-m,--hmod\033[0m\t\tmodification date in human readable format.\n"); 
  printf("\t\033[1m-y,--smod\033[0m\t\tmodification date in seconds..\n"); 
  printf("\t\033[1m-M,--hctime\033[0m\t\tchange date in human readable format.\n"); 
  printf("\t\033[1m-Y,--sctime\033[0m\t\tchange date in seconds.\n"); 
  printf("\t\033[1m-r,--hupdate\033[0m\t\tdirectory update date in human readable format (directory only).\n"); 
  printf("\t\033[1m-Z,--supdate\033[0m\t\tdirectory update date in seconds (directory only).\n"); 
  printf("\t\033[1m-s,--size\033[0m\t\tfile/directory size.\n"); 
  printf("\t\033[1m-g,--gid\033[0m\t\tgroup identifier (1).\n"); 
  printf("\t\033[1m-u,--uid\033[0m\t\tuser identifier (1).\n"); 
  printf("\t\033[1m-C,--cid\033[0m\t\tcluster identifier (file only) (1).\n"); 
  printf("\t\033[1m-z,--sid\033[0m\t\tSID identifier (file only) (1).\n"); 
  printf("\t\033[1m-P,--project\033[0m\t\tproject identifier (1).\n"); 
  printf("\t\033[1m-l,--link\033[0m\t\tnumber of links (file only).\n"); 
  printf("\t\033[1m-b,--children\033[0m\t\tnumber of children (directory only).\n"); 
  printf("\t\033[1m-D,--deleted\033[0m\t\tnumber of deleted inode in the trash (directory only).\n"); 
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
  printf("\nDates must be expressed in one of the following format:\n");
  printf(" - YYYY-MM-DD\n - YYYY-MM-DD-HH\n - YYYY-MM-DD-HH:MM\n - YYYY-MM-DD-HH:MM:SS\n");
  printf("\n\033[1mOUTPUT:\033[0m\n");              
  printf("\t\033[1m-o,--out <f1,f2...>\033[0m\tDescribes requested output fields.\n");
  printf("\t\t\t\tDefault is to have one file/directory path per line.\n");
  printf("\t\033[1mline<val>\033[0m\t\tDisplay <val> file/directory per line.\n");
  printf("\t\033[1m<full|rel|fid>\033[0m\t\tDisplay <full names|relative names|fid>.\n");
  printf("\t\033[1msize\033[0m\t\t\tdisplay file/directory size.\n");
  printf("\t\033[1mproject\033[0m\t\t\tdisplay project identifier.\n");
  printf("\t\033[1mchildren\033[0m\t\tdisplay directory number of children.\n");
  printf("\t\033[1mdeleted\033[0m\t\t\tdisplay directory number of deleted children.\n");
  printf("\t\033[1mnlink\033[0m\t\t\tdisplay file number of link.\n");
  printf("\t\033[1muid\033[0m\t\t\tdisplay uid.\n");
  printf("\t\033[1mgid\033[0m\t\t\tdisplay gid.\n");
  printf("\t\033[1mscr8|hcr8\033[0m\t\tdisplay creation time in seconds or human readable date.\n");
  printf("\t\033[1msmod|hmod\033[0m\t\tdisplay modification time in seconds or human readable date.\n");
  printf("\t\033[1msctime|hctime\033[0m\t\tdisplay change time in seconds or human readable date.\n");
  printf("\t\033[1msupdate|hupdate\033[0m\t\tdisplay update directory time in seconds or human readable date.\n");
  printf("\t\033[1msatime|hatime\033[0m\t\tdisplay access time in seconds or human readable date.\n");
  printf("\t\033[1mpriv\033[0m\t\t \tdisplay Linux privileges.\n");
  printf("\t\033[1mxattr\033[0m\t\t \tdisplay extended attributes.\n");
  printf("\t\033[1mdistrib\033[0m\t\t\tdisplay RozoFS distribution.\n");
  printf("\t\033[1mid\033[0m\t\t\tdisplay RozoFS FID.\n");
  printf("\t\033[1malls|allh\033[0m\t\tdisplay every field (time in seconds or human readable date).\n");
  printf("\t\033[1msep=<string>\033[0m\t\tdefines a field separator without ' '.\n");
  printf("\t\033[1mjson\033[0m\t\t\toutput is in json format.\n");
  printf("\t\033[1mcount<val>\033[0m\t\tStop after displaying the <val> first found entries.\n");
  
  if (fmt == NULL) {
    printf("\n\033[4mExamples:\033[0m\n");
    printf("Searching files with a size comprised between 76000 and 76100 and having extended attributes.\n");
    printf("  \033[1mrozo_scan --xattr --size --ge 76000 --le 76100 --out size\033[0m\n");
    printf("Searching files with a modification date in february 2017 but created before 2017.\n");
    printf("  \033[1mrozo_scan --hmod --ge 2017-02-01 --lt 2017-03-01 --hcr8 --lt 2017-01-01 -o hcr8,hmod,uid,sep=#\033[0m\n");
    printf("Searching files created by user 4501 on 2015 January the 10th in the afternoon.\n");
    printf("  \033[1mrozo_scan --uid --eq 4501 --hcr8 --ge 2015-01-10-12:00 --le 2015-01-11 -o hcr8,hmod,uid,gid\033[0m\n");
    printf("Searching files owned by group 4321 in directory with FID 00000000-0000-4000-1800-000000000018.\n");
    printf("  \033[1mrozo_scan --gid --eq 4321 --pfid --eq 00000000-0000-4000-1800-000000000018\033[0m\n");
    printf("Searching files whoes name constains captainNemo.\n");
    printf("  \033[1mrozo_scan --name --ge captainNemo --out all\033[0m\n");
    printf("Searching directories whoes name starts by a \'Z\', ends with \".DIR\" and constains at least one decimal number.\n");
    printf("  \033[1mrozo_scan --dir --name --regex /tmp/regex\033[0m\n");
    printf("  With /tmp/regex containing regex string \033[1m^Z.*\\d.*\\.DIR$\033[0m\n");    
    printf("Searching directories with more than 100K entries.\n");
    printf("  \033[1mrozo_scan --dir --children --ge 100000 --out size,children\033[0m\n");
    printf("Searching all symbolic links.\n");
    printf("  \033[1mrozo_scan --slink --noreg --out allh\033[0m\n");
    printf("Searching files in project #31 owned by user 2345.\n");
    printf("  \033[1mrozo_scan --project --eq 31 --uid --eq 2345\033[0m\n");
    printf("Searching files in cluster 2 having a potential projection on sid 7.\n");
    printf("  \033[1mrozo_scan --cid --eq 2 --sid --eq 7 -o distrib\033[0m\n");
    printf("Searching non writable files being executable by its group but not by the others.\n");
    printf("  \033[1mrozo_scan --Unw --Gx --Onx -o priv,gid,uid\033[0m\n");
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
  
  ret = sscanf(date,"%d-%d-%d-%d:%d:%d",&year,&month,&day,&hour,&minute,&sec);
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
   
  /*
  ** Must have a modification time bigger than ctime_bigger
  */ 
  if (ctime_bigger != -1) {
    /*
    ** The tracking file must have been modified at this timer or later
    */
    docheck(inode_p->s.attrs.ctime, ctime_bigger);
  }  

  /*
  ** Must have a modification time lower than mod_lower
  */    
  if (ctime_lower != -1) {
    /*
    ** The tracking file must have been created at this time or before
    */
    docheck(ctime_lower,inode_p->s.cr8time);
  }     

  /*
  ** Must have a modification time equal to mod_equal
  */    
  if (ctime_equal != -1) {
    /*
    ** The tracking file must have been created at this time or before
    */
    docheck(ctime_equal,inode_p->s.cr8time);
    /*
    ** The tracking file must have been modified at this timer or later
    */
    docheck(inode_p->s.attrs.mtime,ctime_equal);
  }   
  return 1; 
}
/*
**_______________________________________________________________________
** Check whether the tracking file c an match the given date criteria
**   
**  @param  eid : eport identifier
**    
**  @retval 0 = do not read this file / 1 = read this file
*/
#define NEXT(p) { \
  while((*p!=',')&&(*p!=0)&&(*p!=' ')) p++;\
  while (*p==',') {\
    p++;\
  }  \
  if ((*p==0)||(*p==' ')) {\
    return 0;\
  } \
  continue;\
}     
int rozofs_parse_output_format(char * fmt) {
  char * p = fmt;
  
  while (1) {
  
    if (strncmp(p, "line", 4)==0) {
      p+=4;
      if (*p=='=') p++;
      if (*p==0) {
        usage("Bad output format for \"line\"\" : no line value.");           
      }
      if (sscanf(p,"%d",&entry_per_line)!=1) {
        usage("Bad output format for \"line\" : integer value required");     
      } 
      if ((entry_per_line<=0)||(entry_per_line>50)) {
        usage("Bad output format for \"line\" : value must be within [1..50] ");     
      }
      NEXT(p);
    }
    
    if (strncmp(p, "count", 5)==0) {
      p+=5;
      if (*p=='=') p++;
      if (*p==0) {
        usage("Bad output format for \"count\"\" : no count value.");           
      }
      if (sscanf(p,"%llu",(long long unsigned int*)&max_display)!=1) {
        usage("Bad output format for \"count\" : integer value required");     
      } 
      NEXT(p);
    }
    
    if (strncmp(p, "fid", 3)==0) {
      name_format = name_format_fid;
      NEXT(p);
    }    
    if (strncmp(p, "full", 3)==0) {
      name_format = name_format_full;
      NEXT(p);
    }  
    if (strncmp(p, "rel", 3)==0) {
      name_format = name_format_relative;
      NEXT(p);
    }  
    
    if (strncmp(p, "size", 2)==0) {
      display_size = DO_DISPLAY;
      NEXT(p);
    }      
    
    if (strncmp(p, "children", 2)==0) {
      display_children = DO_DISPLAY;
      NEXT(p);
    }      
    
    if (strncmp(p, "deleted", 3)==0) {
      display_deleted = DO_DISPLAY;
      NEXT(p);
    }      

    if (strncmp(p, "nlink", 5)==0) {
      display_nlink = DO_DISPLAY;
      NEXT(p);
    }      

    if (strncmp(p, "project", 4)==0) {
      display_project = DO_DISPLAY;
      NEXT(p);
    }      

    if (strncmp(p, "uid", 1)==0) {
      display_uid = DO_DISPLAY;
      NEXT(p);
    }      

    if (strncmp(p, "gid", 1)==0) {
      display_gid = DO_DISPLAY;
      NEXT(p);
    }   
    
    if (strncmp(p, "scr8", 3)==0) {
      display_cr8 = DO_DISPLAY;
      NEXT(p);
    } 
    if (strncmp(p, "hcr8", 3)==0) {
      display_cr8 = DO_HUMAN_DISPLAY;
      NEXT(p);
    } 

    if (strncmp(p, "smod", 3)==0) {
      display_mod = DO_DISPLAY;
      NEXT(p);
    }              

    if (strncmp(p, "hmod", 3)==0) {
      display_mod = DO_HUMAN_DISPLAY;
      NEXT(p);
    }              

    if (strncmp(p, "sctime", 3)==0) {
      display_ctime = DO_DISPLAY;
      NEXT(p);
    }              

    if (strncmp(p, "hctime", 3)==0) {
      display_ctime = DO_HUMAN_DISPLAY;
      NEXT(p);
    }              

    if (strncmp(p, "satime", 3)==0) {
      display_atime = DO_DISPLAY;
      NEXT(p);
    }              

    if (strncmp(p, "hatime", 3)==0) {
      display_atime = DO_HUMAN_DISPLAY;
      NEXT(p);
    }              

    if (strncmp(p, "supdate", 3)==0) {
      display_update = DO_DISPLAY;
      NEXT(p);
    }              

    if (strncmp(p, "hupdate", 3)==0) {
      display_update = DO_HUMAN_DISPLAY;
      NEXT(p);
    }              

 
    if (strncmp(p, "priv", 2)==0) {
      display_priv = DO_DISPLAY;
      NEXT(p);
    }      
      
    if (strncmp(p, "xattr", 3)==0) {
      display_xattr = DO_DISPLAY;
      NEXT(p);
    }     
               
    if (strncmp(p, "distrib", 4)==0) {
      display_distrib = DO_DISPLAY;
      NEXT(p);
    }       
                    
    if (strncmp(p, "id", 2)==0) {
      display_id = DO_DISPLAY;
      NEXT(p);
    }                       

    if (strncmp(p, "sep=", 4)==0) {
      p+=4;
      char * pSep = separator;
      while((*p!= ' ') && (*p!= 0) &&(*p!=',')) {
        *pSep++ = *p++;
      }
      NEXT(p);
    } 
    if (strncmp(p, "allh", 4)==0) {
      display_all = DO_HUMAN_DISPLAY;
      NEXT(p);
    }      
    if (strncmp(p, "alls", 4)==0) {
      display_all = DO_DISPLAY;
      NEXT(p);
    }    
    if (strncmp(p, "all", 2)==0) {
      display_all = DO_HUMAN_DISPLAY;
      NEXT(p);
    }               
    if (strncmp(p, "json", 2)==0) {
      display_json = DO_DISPLAY;
      NEXT(p);
    }         
       
    { 
      int i;
      char msg[1024];   
      char * pmsg = msg;
      pmsg += sprintf(pmsg,"Unexpected output format\n");
      pmsg += sprintf(pmsg,"     %s\n",fmt);

      i = p-fmt+5;
      while (i) {
      *pmsg++ = ' ';
       i--;
      }
      *pmsg++ = '^';
      *pmsg++ = '\n'; 
      *pmsg++ = 0; 
      usage(msg); 
    }

  }  
  return 0;
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
    long long usecs;
    struct timeval start;
    struct timeval stop;
        
    static struct option long_options[] = {
        {"help", no_argument, 0, 'h'},
        {"path", required_argument, 0, 'p'},
        {"eid", required_argument, 0, 'e'},
        {"config", required_argument, 0, 'k'},
        {"verbose", no_argument, 0, 'v'},
        {"cr8", no_argument, 0, 'c'},
        {"hcr8", no_argument, 0, 'c'},
        {"scr8", no_argument, 0, 'A'},
        {"mod", no_argument, 0, 'm'},
        {"hmod", no_argument, 0, 'm'},
        {"smod", no_argument, 0, 'y'},
        {"ctime", no_argument, 0, 'M'},
        {"hctime", no_argument, 0, 'M'},
        {"sctime", no_argument, 0, 'Y'},
        {"size", no_argument, 0, 's'},
        {"uid", no_argument, 0, 'u'},
        {"gid", no_argument, 0, 'g'},        
        {"cid", no_argument, 0, 'C'},        
        {"sid", no_argument, 0, 'z'},        
        {"project", no_argument, 0, 'P'},        
        {"link", no_argument, 0, 'l'},        
        {"children", no_argument, 0, 'b'},        
        {"deleted", no_argument, 0, 'D'},        
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
        {"hupdate", no_argument, 0, 'r'},
        {"supdate", no_argument, 0, 'Z'},
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
        {"out", required_argument, 0, 'o'},
        {"junk", required_argument, 0, 'j'},

        {0, 0, 0, 0}
    };

    gettimeofday(&start,(struct timezone *)0);

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
      c = getopt_long(argc, argv, "<:-:>:+:=:!:*:abcde:fghjk:lmno:p:rtsuvxyzACDMPRSTXY", long_options, &option_index);

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
          
          case 'o':
            if (optarg==0) {
              usage("No output format defined");     
            }
            if (rozofs_parse_output_format(optarg)!=0) {
              usage("Bad output format \"%s\"",optarg);     
            }
            break;

          case 'c':
              NEW_CRITERIA(SCAN_CRITERIA_HCR8);
              date_criteria_is_set = 1;
              break;
          case 'A':
              NEW_CRITERIA(SCAN_CRITERIA_SCR8);
              date_criteria_is_set = 1;
              break;
          case 'm':
              NEW_CRITERIA(SCAN_CRITERIA_HMOD);
              date_criteria_is_set = 1;
              break;
          case 'y':
              NEW_CRITERIA(SCAN_CRITERIA_SMOD);
              date_criteria_is_set = 1;
              break;
          case 'M':
              NEW_CRITERIA(SCAN_CRITERIA_HCTIME);
              date_criteria_is_set = 1;
              break;
          case 'Y':
              NEW_CRITERIA(SCAN_CRITERIA_SCTIME);
              date_criteria_is_set = 1;
              break;
          case 'r':
              NEW_CRITERIA(SCAN_CRITERIA_HUPDATE);
              date_criteria_is_set = 1;
              break;
          case 'Z':
              NEW_CRITERIA(SCAN_CRITERIA_SUPDATE);
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
          case 'D':
              NEW_CRITERIA(SCAN_CRITERIA_DELETED);
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
          case 'j':
              only_junk = 1;
              if (only_trash) {
                usage("only trash (-t) and junk files (-j) are incompatible");     
              }
              if (search_dir) {
                usage("Directory (-d) and junk files (-j) are incompatible");     
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
              
                case SCAN_CRITERIA_HCR8:
                  cr8_lower = rozofs_date2time(optarg);
                  if (cr8_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break; 
                  
                case SCAN_CRITERIA_SCR8:
                  cr8_lower = rozofs_scan_u64(optarg);
                  if (cr8_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break; 
                               
                case SCAN_CRITERIA_HMOD:
                  mod_lower = rozofs_date2time(optarg);
                  if (mod_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break; 
                               
                case SCAN_CRITERIA_SMOD:
                  mod_lower = rozofs_scan_u64(optarg);
                  if (mod_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break; 

                case SCAN_CRITERIA_HCTIME:
                  ctime_lower = rozofs_date2time(optarg);
                  if (ctime_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break; 

                case SCAN_CRITERIA_SCTIME:
                  ctime_lower = rozofs_scan_u64(optarg);
                  if (ctime_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break; 

                case SCAN_CRITERIA_HUPDATE:
                  update_lower = rozofs_date2time(optarg);
                  if (update_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break;                     

                case SCAN_CRITERIA_SUPDATE:
                  update_lower = rozofs_scan_u64(optarg);
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
                                   
                case SCAN_CRITERIA_DELETED:
                  deleted_lower = rozofs_scan_u64(optarg);
                  if (deleted_lower==-1) {
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
              
                case SCAN_CRITERIA_HCR8:
                  cr8_lower = rozofs_date2time(optarg);
                  if (cr8_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  cr8_lower--;   
                  break;   
                  
                case SCAN_CRITERIA_SCR8:
                  cr8_lower = rozofs_scan_u64(optarg);
                  if (cr8_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  cr8_lower--;   
                  break;                                
                             
                case SCAN_CRITERIA_HMOD:
                  mod_lower = rozofs_date2time(optarg);
                  if (mod_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }
                  mod_lower--;    
                  break;  
                             
                case SCAN_CRITERIA_SMOD:
                  mod_lower = rozofs_scan_u64(optarg);
                  if (mod_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }
                  mod_lower--;    
                  break;  
                  
                case SCAN_CRITERIA_HCTIME:
                  ctime_lower = rozofs_date2time(optarg);
                  if (ctime_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }
                  ctime_lower--;    
                  break;  
                  
                case SCAN_CRITERIA_SCTIME:
                  ctime_lower = rozofs_scan_u64(optarg);
                  if (ctime_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }
                  ctime_lower--;    
                  break;  

                case SCAN_CRITERIA_HUPDATE:
                  update_lower = rozofs_date2time(optarg);
                  if (update_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }
                  update_lower--;    
                  break;  
                  
                case SCAN_CRITERIA_SUPDATE:
                  update_lower = rozofs_scan_u64(optarg);
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
                                   
                case SCAN_CRITERIA_DELETED:
                  deleted_lower = rozofs_scan_u64(optarg);
                  if (deleted_lower==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }   
                  if (deleted_lower==0) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  deleted_lower--;   
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
              
                case SCAN_CRITERIA_HCR8:
                  cr8_bigger = rozofs_date2time(optarg);
                  if (cr8_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break;  
                  
                case SCAN_CRITERIA_SCR8:
                  cr8_bigger = rozofs_scan_u64(optarg);
                  if (cr8_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break;  
                               
                case SCAN_CRITERIA_HMOD:
                  mod_bigger = rozofs_date2time(optarg);
                  if (mod_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break; 
                               
                case SCAN_CRITERIA_SMOD:
                  mod_bigger = rozofs_scan_u64(optarg);
                  if (mod_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break; 

                case SCAN_CRITERIA_HCTIME:
                  ctime_bigger = rozofs_date2time(optarg);
                  if (ctime_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break; 
                  
                case SCAN_CRITERIA_SCTIME:
                  ctime_bigger = rozofs_scan_u64(optarg);
                  if (ctime_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break; 
                   
                case SCAN_CRITERIA_HUPDATE:
                  update_bigger = rozofs_date2time(optarg);
                  if (update_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }    
                  break;
                  
                case SCAN_CRITERIA_SUPDATE:
                  update_bigger = rozofs_scan_u64(optarg);
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

                case SCAN_CRITERIA_DELETED:
                  deleted_bigger = rozofs_scan_u64(optarg);
                  if (deleted_bigger==-1) {
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
              
                case SCAN_CRITERIA_HCR8:
                  cr8_bigger = rozofs_date2time(optarg);
                  if (cr8_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }  
                  cr8_bigger++; 
                  break;        
              
                case SCAN_CRITERIA_SCR8:
                  cr8_bigger = rozofs_scan_u64(optarg);
                  if (cr8_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }  
                  cr8_bigger++; 
                  break;        
                        
                case SCAN_CRITERIA_HMOD:
                  mod_bigger = rozofs_date2time(optarg);
                  if (mod_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  mod_bigger++;  
                  break;
                  
                case SCAN_CRITERIA_SMOD:
                  mod_bigger = rozofs_scan_u64(optarg);
                  if (mod_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  mod_bigger++;  
                  break;

                case SCAN_CRITERIA_HCTIME:
                  ctime_bigger = rozofs_date2time(optarg);
                  if (ctime_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  ctime_bigger++;  
                  break;

                case SCAN_CRITERIA_SCTIME:
                  ctime_bigger = rozofs_scan_u64(optarg);
                  if (ctime_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  ctime_bigger++;  
                  break;

                 case SCAN_CRITERIA_HUPDATE:
                  update_bigger = rozofs_date2time(optarg);
                  if (update_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  update_bigger++;  
                  break;                  

                 case SCAN_CRITERIA_SUPDATE:
                  update_bigger = rozofs_scan_u64(optarg);
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
                  
                case SCAN_CRITERIA_DELETED:
                  deleted_bigger = rozofs_scan_u64(optarg);
                  if (deleted_bigger==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  deleted_bigger++;
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
              
                case SCAN_CRITERIA_HCR8:
                  cr8_equal = rozofs_date2time(optarg);
                  if (cr8_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  break;    
                  
                case SCAN_CRITERIA_SCR8:
                  cr8_equal =  rozofs_scan_u64(optarg);
                  if (cr8_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  break;    
                            
                case SCAN_CRITERIA_HMOD:
                  mod_equal = rozofs_date2time(optarg);
                  if (mod_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }                   
                  break;  
                            
                case SCAN_CRITERIA_SMOD:
                  mod_equal =  rozofs_scan_u64(optarg);
                  if (mod_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }                   
                  break;  
                  
                case SCAN_CRITERIA_HCTIME:
                  ctime_equal = rozofs_date2time(optarg);
                  if (ctime_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }                   
                  break;  
                  
                case SCAN_CRITERIA_SCTIME:
                  ctime_equal =  rozofs_scan_u64(optarg);
                  if (ctime_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }                   
                  break;  
                   
                case SCAN_CRITERIA_HUPDATE:
                  update_equal = rozofs_date2time(optarg);
                  if (update_equal==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }                   
                  break;  

                case SCAN_CRITERIA_SUPDATE:
                  update_equal =  rozofs_scan_u64(optarg);
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
                  
                case SCAN_CRITERIA_DELETED:
                  deleted_equal = rozofs_scan_u64(optarg);
                  if (deleted_equal==-1) {
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
              
                case SCAN_CRITERIA_HCR8:
                  cr8_diff = rozofs_date2time(optarg);
                  if (cr8_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  break; 
                  
                case SCAN_CRITERIA_SCR8:
                  cr8_diff =  rozofs_scan_u64(optarg);
                  if (cr8_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  } 
                  break; 
                               
                case SCAN_CRITERIA_HMOD:
                  mod_diff = rozofs_date2time(optarg);
                  if (mod_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }                   
                  break; 
                               
                case SCAN_CRITERIA_SMOD:
                  mod_diff =  rozofs_scan_u64(optarg);
                  if (mod_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }                   
                  break; 
                  
                case SCAN_CRITERIA_HCTIME:
                  ctime_diff = rozofs_date2time(optarg);
                  if (ctime_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }                   
                  break; 
                  
                case SCAN_CRITERIA_SCTIME:
                  ctime_diff =  rozofs_scan_u64(optarg);
                  if (ctime_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }                   
                  break; 

                case SCAN_CRITERIA_HUPDATE:
                  update_diff = rozofs_date2time(optarg);
                  if (update_diff==-1) {
                    usage("Bad format for -%c %s \"%s\"",crit,comp,optarg);     
                  }                   
                  break; 

                case SCAN_CRITERIA_SUPDATE:
                  update_diff =  rozofs_scan_u64(optarg);
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
                   
                case SCAN_CRITERIA_DELETED:
                  deleted_diff = rozofs_scan_u64(optarg);
                  if (deleted_diff==-1) {
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

  if (display_json) {
    int i;
    printf("{ \"command\" : \"rozo_scan");
    for (i=1; i<argc; i++) printf(" %s",argv[i]);
    printf("\",\n  \"results\" : [");
  }  
  
  if (search_dir) {
    rz_scan_all_inodes(rozofs_export_p,ROZOFS_DIR,1,rozofs_visit,NULL,date_criteria_cbk,NULL);
  }
  else if (only_junk) {
    rz_scan_all_inodes(rozofs_export_p,ROZOFS_TRASH,1,rozofs_visit_junk,NULL,date_criteria_cbk,NULL);    
  }
  else {
    rz_scan_all_inodes(rozofs_export_p,ROZOFS_REG,1,rozofs_visit,NULL,date_criteria_cbk,NULL);
  }
  if (display_json) {
    printf("\n  ],\n");
    printf("  \"scanned entries\" : %llu,\n", (long long unsigned int)nb_scanned_entries);
    printf("  \"matched entries\" : %llu,\n", (long long unsigned int)nb_matched_entries);

    gettimeofday(&stop,(struct timezone *)0); 
    usecs   = stop.tv_sec  * 1000000 + stop.tv_usec;
    usecs  -= (start.tv_sec  * 1000000 + start.tv_usec);
    printf("  \"micro seconds\"   : %llu\n}\n", usecs);    
  }  
  /*
  ** Current ouput line is not yet finished.
  ** add a \n
  */
  if (cur_entry_per_line != 0) {
    printf("\n");
  }  
  
  exit(EXIT_SUCCESS);  
  return 0;
}
