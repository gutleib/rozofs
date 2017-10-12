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
  SCAN_CRITERIA_NLINK, 
  SCAN_CRITERIA_CHILDREN, 
  SCAN_CRITERIA_PFID,
  SCAN_CRITERIA_FNAME       
} SCAN_CRITERIA_E;

SCAN_CRITERIA_E scan_criteria = SCAN_CRITERIA_NONE;

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
      get_fname(e,name,&inode_attr_p->s.fname,inode_attr_p->s.pfid);
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
static void usage() {
    printf("\n\033[1mRozoFS File system scanning utility - %s\033[0m\n", VERSION);
    printf("This RozoFS utility enables to scan for files or directories in a RozoFS file system\naccording to one or several criteria.\n");
    printf("\n\033[4mUsage:\033[0m\n\t\033[1mrozo_scan_by_criteria <MANDATORY> [OPTIONS] { <CRITERIA> } { <FIELD> <CONDITIONS> } \033[0m\n\n");
    printf("\n\033[1mMANDATORY:\033[0m\n");
    printf("\t\033[1m-p,--path <export_root_path>\033[0m\t\texportd root path.\n");
    printf("or\t\033[1m-e,--eid <eid> [-k <cfg file>]\033[0m\t\texport identifier.\n");
    printf("\n\033[1mOPTIONS:\033[0m\n");
    printf("\t\033[1m-v,--verbose\033[0m\t\tDisplay some execution statistics.\n");
    printf("\t\033[1m-h,--help\033[0m\t\tprint this message and exit.\n");
    printf("\n\033[1mCRITERIA:\033[0m\n");
    printf("\t\033[1m-x,--xattr\033[0m\t\twith xattribute.\n");
    printf("\t\033[1m-X,--noxattr\033[0m\t\twithout xattribute.\n");    
    printf("\t\033[1m-d,--dir\033[0m\t\tis a directory.\n");
    printf("\t\033[1m-S,--slink\033[0m\t\tinclude symbolink links.\n");
    printf("\t\033[1m-R,--noreg\033[0m\t\texclude regular files.\n");
    printf("\n\033[1mFIELD:\033[0m\n");
    printf("\t\033[1m-c,--cr8\033[0m\t\tcreation date.\n");
    printf("\t\033[1m-m,--mod\033[0m\t\tmodification date.\n"); 
    printf("\t\033[1m-s,--size\033[0m\t\tfile size.\n"); 
    printf("\t\033[1m-g,--gid\033[0m\t\tgroup identifier (1).\n"); 
    printf("\t\033[1m-u,--uid\033[0m\t\tuser identifier (1).\n"); 
    printf("\t\033[1m-C,--cid\033[0m\t\tcluster identifier (1).\n"); 
    printf("\t\033[1m-l,--link\033[0m\t\tnumber of link.\n"); 
    printf("\t\033[1m-b,--children\033[0m\t\tnumber of children.\n"); 
    printf("\t\033[1m-f,--pfid\033[0m\t\tParent FID (2).\n");
    printf("\t\033[1m-n,--name\033[0m\t\tfile name (3).\n");
    printf("(1) only --eq or --ne conditions are supported.\n");
    printf("(2) only --eq condition is supported.\n");
    printf("(3) only --eq or --ge conditions are supported.\n");
    printf("\n\033[1mCONDITIONS:\033[0m\n");              
    printf("\t\033[1m--lt <val>\033[0m\t\tField must be lower than <val>.\n");
    printf("\t\033[1m--le <val>\033[0m\t\tField must be lower or equal than <val>.\n");
    printf("\t\033[1m--gt <val>\033[0m\t\tField must be greater than <val>.\n");
    printf("\t\033[1m--ge <val>\033[0m\t\tField must be greater or equal than <val>.\n");
    printf("\t\t\t\tFor --name search files whoes name contains <val>.\n");
    printf("\t\033[1m--eq <val>\033[0m\t\tField must be equal to <val>.\n");
    printf("\t\033[1m--ne <val>\033[0m\t\tField must not be equal to <val>.\n");
    printf("\nDates must be expressed as:\n");
    printf(" - YYYY-MM-DD\n - \"YYYY-MM-DD HH\"\n - \"YYYY-MM-DD HH:MM\"\n - \"YYYY-MM-DD HH:MM:SS\"\n");
    printf("\n\033[4mExamples:\033[0m\n");
    printf("Searching files with a size comprised between 76000 and 76100 and having extended attributes.\n");
    printf("  \033[1mrozo_scan_by_criteria --eid 1 --xattr --size --ge 76000 --le 76100\033[0m\n");
    printf("Searching files with a modification date in february 2017 but created before 2017.\n");
    printf("  \033[1mrozo_scan_by_criteria --eid 1 --mod --ge \"2017-02-01\" --lt \"2017-03-01\" --cr8 --lt \"2017-01-01\"\033[0m\n");
    printf("Searching files created by user 4501 on 2015 January the 10th in the afternoon.\n");
    printf("  \033[1mrozo_scan_by_criteria --eid 1 --uid --eq 4501 --cr8 --ge \"2015-01-10 12:00\" --le \"2015-01-11\"\033[0m\n");
    printf("Searching files owned by group 4321 in directory 00000000-0000-4000-1800-000000000018.\n");
    printf("  \033[1mrozo_scan_by_criteria --eid 1 --gid --eq 4321 --pfid --eq 00000000-0000-4000-1800-000000000018\033[0m\n");
    printf("Searching files whoes name constains captainNemo.\n");
    printf("  \033[1mrozo_scan_by_criteria --eid 1 --name --ge captainNemo\033[0m\n");
    printf("Searching directories with more than 100K entries.\n");
    printf("  \033[1mrozo_scan_by_criteria --eid 1 --dir --children --ge 100000\033[0m\n");
    printf("Searching all symbolic links.\n");
    printf("  \033[1mrozo_scan_by_criteria --eid 1 --slink --noreg\033[0m\n");
     
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
/*
**_______________________________________________________________________
**
**  M A I N
*/

#define NEW_CRITERIA(criteria){\
              if (expect_comparator) {\
                printf("Expecting --lt, --le, --gt, --ge or --ne. Got --%s\n", long_options[option_index].name);\
                exit(EXIT_FAILURE);\
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
        {"ne", required_argument, 0, '!'},
        {"dir", no_argument, 0, 'd'},
        {0, 0, 0, 0}
    };
    

    for (c=0; c<argc; c++) {
      if (strcmp(argv[c],"-ge")==0) {
        printf("Argument %d is %s. Don't you mean --ge ?\n",c,argv[c]);
        exit(EXIT_FAILURE);
      }
      if (strcmp(argv[c],"-le")==0) {
        printf("Argument %d is %s. Don't you mean --le ?\n",c,argv[c]);
        exit(EXIT_FAILURE);
      }
      if (strcmp(argv[c],"-gt")==0) {
        printf("Argument %d is %s. Don't you mean --gt ?\n",c,argv[c]);
        exit(EXIT_FAILURE);
      }
      if (strcmp(argv[c],"-lt")==0) {
        printf("Argument %d is %s. Don't you mean --lt ?\n",c,argv[c]);
        exit(EXIT_FAILURE);
      }
      if (strcmp(argv[c],"-eq")==0) {
        printf("Argument %d is %s. Don't you mean --eq ?\n",c,argv[c]);
        exit(EXIT_FAILURE);
      }
      if (strcmp(argv[c],"-ne")==0) {
        printf("Argument %d is %s. Don't you mean --ne ?\n",c,argv[c]);
        exit(EXIT_FAILURE);
      }
    } 
  
    while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "p:<:-:>:+:=:!:e:k:hvcmsguClxXdbfnSR", long_options, &option_index);

      if (c == -1)
          break;

      switch (c) {

          case 'h':
              usage();
              exit(EXIT_SUCCESS);
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
                printf("\nBad format for --eid \"%s\"\n",optarg);     
                usage();
                exit(EXIT_FAILURE);
              }
              break;    
          case 'v':
              verbose = 1;
              break;
          case 'c':
              NEW_CRITERIA(SCAN_CRITERIA_CR8);
              break;
          case 'm':
              NEW_CRITERIA(SCAN_CRITERIA_MOD);
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
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }    
                  break; 
                               
                case SCAN_CRITERIA_MOD:
                  mod_lower = rozofs_date2time(optarg);
                  if (mod_lower==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }    
                  break; 
                    
                case SCAN_CRITERIA_SIZE:
                  size_lower = rozofs_scan_u64(optarg);
                  if (size_lower==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }    
                  break;  
                                   
                case SCAN_CRITERIA_NLINK:
                  nlink_lower = rozofs_scan_u64(optarg);
                  if (nlink_lower==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }    
                  break; 
                                   
                case SCAN_CRITERIA_CHILDREN:
                  children_lower = rozofs_scan_u64(optarg);
                  if (children_lower==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }    
                  break; 
                                                                    
                case SCAN_CRITERIA_GID:          
                case SCAN_CRITERIA_UID:
                case SCAN_CRITERIA_CID:
                case SCAN_CRITERIA_PFID:
                case SCAN_CRITERIA_FNAME:
                  printf("\nNo %s comparison for -%c\n",comp,crit);     
                  usage();
                  exit(EXIT_FAILURE);  
                  break;
                  
                default:
                  printf("\nNo criteria defined prior to %s\n",comp);     
                  usage();
                  exit(EXIT_FAILURE);
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
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  } 
                  cr8_lower--;   
                  break;   
                             
                case SCAN_CRITERIA_MOD:
                  mod_lower = rozofs_date2time(optarg);
                  if (mod_lower==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }
                  mod_lower--;    
                  break;  
                  
                case SCAN_CRITERIA_SIZE:
                  size_lower = rozofs_scan_u64(optarg);
                  if (size_lower==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  } 
                  if (size_lower==0) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  } 
                  size_lower--;   
                  break;     
                  
                case SCAN_CRITERIA_NLINK:
                  nlink_lower = rozofs_scan_u64(optarg);
                  if (nlink_lower==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  } 
                  if (nlink_lower==0) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  } 
                  nlink_lower--;   
                  break;  
                                   
                case SCAN_CRITERIA_CHILDREN:
                  children_lower = rozofs_scan_u64(optarg);
                  if (children_lower==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }   
                  if (children_lower==0) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  } 
                  children_lower--;   
                  break;  

                                      
                case SCAN_CRITERIA_GID:          
                case SCAN_CRITERIA_UID:
                case SCAN_CRITERIA_PFID:
                case SCAN_CRITERIA_FNAME:
                  printf("\nNo %s comparison for -%c\n",comp,crit);     
                  usage();
                  exit(EXIT_FAILURE);  
                  break; 
                                                
                default:
                  printf("\nNo criteria defined prior to %s\n",comp);     
                  usage();
                  exit(EXIT_FAILURE);
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
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }    
                  break;  
                              
                case SCAN_CRITERIA_MOD:
                  mod_bigger = rozofs_date2time(optarg);
                  if (mod_bigger==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }    
                  break; 
                  
                case SCAN_CRITERIA_SIZE:
                  size_bigger = rozofs_scan_u64(optarg);
                  if (size_bigger==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  } 
                  break; 
                   
                case SCAN_CRITERIA_NLINK:
                  nlink_bigger = rozofs_scan_u64(optarg);
                  if (nlink_bigger==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  } 
                  break;
                   
                case SCAN_CRITERIA_CHILDREN:
                  children_bigger = rozofs_scan_u64(optarg);
                  if (children_bigger==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  } 
                  break;
                  
                case SCAN_CRITERIA_FNAME:
                  fname_bigger = optarg;
                  break;
                     
                case SCAN_CRITERIA_GID:          
                case SCAN_CRITERIA_UID:
                case SCAN_CRITERIA_CID:                
                case SCAN_CRITERIA_PFID:
                  printf("\nNo %s comparison for -%c\n",comp,crit);     
                  usage();
                  exit(EXIT_FAILURE);  
                  break;                               
                                       
                default:
                  printf("\nNo criteria defined prior to %s\n",comp);     
                  usage();
                  exit(EXIT_FAILURE);
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
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }  
                  cr8_bigger++; 
                  break;        
                        
                case SCAN_CRITERIA_MOD:
                  mod_bigger = rozofs_date2time(optarg);
                  if (mod_bigger==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  } 
                  mod_bigger++;  
                  break;
                  
                case SCAN_CRITERIA_SIZE:
                  size_bigger = rozofs_scan_u64(optarg);
                  if (size_bigger==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  } 
                  size_bigger++;
                  break;  
                  
                case SCAN_CRITERIA_NLINK:
                  nlink_bigger = rozofs_scan_u64(optarg);
                  if (nlink_bigger==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  } 
                  nlink_bigger++;
                  break;  
                  
                case SCAN_CRITERIA_CHILDREN:
                  children_bigger = rozofs_scan_u64(optarg);
                  if (children_bigger==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  } 
                  children_bigger++;
                  break;  
                  
                case SCAN_CRITERIA_GID:          
                case SCAN_CRITERIA_UID:
                case SCAN_CRITERIA_CID:                
                case SCAN_CRITERIA_PFID:
                case SCAN_CRITERIA_FNAME:
                  printf("\nNo %s comparison for -%c\n",comp,crit);     
                  usage();
                  exit(EXIT_FAILURE);  
                  break;                               
                                      
                default:
                  printf("\nNo criteria defined prior to %s\n",comp);     
                  usage();
                  exit(EXIT_FAILURE);
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
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  } 
                  break;    
                            
                case SCAN_CRITERIA_MOD:
                  mod_equal = rozofs_date2time(optarg);
                  if (mod_equal==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }                   
                  break;  
                  
                case SCAN_CRITERIA_SIZE:
                  size_equal = rozofs_scan_u64(optarg);
                  if (size_equal==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }
                  break; 
                  
                case SCAN_CRITERIA_GID:
                  gid_equal = rozofs_scan_u64(optarg);
                  if (gid_equal==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }   
                  break;
                  
                case SCAN_CRITERIA_NLINK:
                  nlink_equal = rozofs_scan_u64(optarg);
                  if (nlink_equal==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }   
                  break;
                                                                      
                case SCAN_CRITERIA_CHILDREN:
                  children_equal = rozofs_scan_u64(optarg);
                  if (children_equal==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }   
                  break;
                                                                      
                case SCAN_CRITERIA_UID:
                  uid_equal = rozofs_scan_u64(optarg);
                  if (uid_equal==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }   
                  break;   
                  
                case SCAN_CRITERIA_CID:
                  cid_equal = rozofs_scan_u64(optarg);
                  if (cid_equal==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }   
                  break;
                  
                case SCAN_CRITERIA_PFID:
                  if (rozofs_uuid_parse(optarg, pfid_equal)!=0) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }   
                  break;                                                                         
                  
                case SCAN_CRITERIA_FNAME:
                  fname_equal = optarg;
                  break;                                                                         
                                                                         
                                                                         
                default:
                  printf("\nNo criteria defined prior to %s\n",comp);     
                  usage();
                  exit(EXIT_FAILURE);
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
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  } 
                  break; 
                               
                case SCAN_CRITERIA_MOD:
                  mod_diff = rozofs_date2time(optarg);
                  if (mod_diff==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }                   
                  break; 
                   
                case SCAN_CRITERIA_SIZE:
                  size_diff = rozofs_scan_u64(optarg);
                  if (size_diff==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }
                  break; 
                   
                case SCAN_CRITERIA_NLINK:
                  nlink_diff = rozofs_scan_u64(optarg);
                  if (nlink_diff==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }
                  break; 
                   
                case SCAN_CRITERIA_CHILDREN:
                  children_diff = rozofs_scan_u64(optarg);
                  if (children_diff==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }
                  break; 
                  
                case SCAN_CRITERIA_GID:
                  gid_diff = rozofs_scan_u64(optarg);
                  if (gid_diff==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }   
                  break; 
                                  
                case SCAN_CRITERIA_UID:
                  uid_diff = rozofs_scan_u64(optarg);
                  if (uid_diff==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }   
                  break; 
                    
                case SCAN_CRITERIA_CID:
                  cid_diff = rozofs_scan_u64(optarg);
                  if (cid_diff==-1) {
                    printf("\nBad format for -%c %s \"%s\"\n",crit,comp,optarg);     
                    usage();
                    exit(EXIT_FAILURE);
                  }   
                  break;
                  
                case SCAN_CRITERIA_PFID:
                case SCAN_CRITERIA_FNAME:
                  printf("\nNo %s comparison for -%c\n",comp,crit);     
                  usage();
                  exit(EXIT_FAILURE);  
                  break;                                                   
                                                                         
                default:
                  printf("\nNo criteria defined prior to %s\n",comp);     
                  usage();
                  exit(EXIT_FAILURE);
              }
              break;                
          case 'd':
              search_dir = 1;
              break;                               
          case '?':
          default:
              if (optopt)  printf("\nUnexpected argument \"-%c\"\n", optopt);
              else         printf("\nUnexpected argument \"%s\"\n", argv[optind-1]);
              usage();
              exit(EXIT_FAILURE);
              break;
      }
  }
  
  if (expect_comparator) {
    printf("Expecting --lt, --le, --gt, --ge or --ne.\n");
    exit(EXIT_FAILURE);
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
      printf("can't initialize exportd config %s.\n",strerror(errno));
      exit(EXIT_FAILURE);  
    }    
    if (econfig_read(&exportd_config, configFileName) != 0) {
      printf("failed to parse configuration file %s %s.\n",configFileName,strerror(errno));
      exit(EXIT_FAILURE);  
    }              	 
    /*
    ** Find the export root path
    */
    root_path = get_export_root_path(eid);
    if (root_path==NULL) {
      printf("eid %d is not configured\n",eid);       
      exit(EXIT_FAILURE);
    }
  }

  if (root_path == NULL) 
  {
       usage();
       exit(EXIT_FAILURE);  
  }

  printf("\n\n");
  /*
  ** init of the RozoFS data structure on export
  ** in order to permit the scanning of the exportd
  */
  rozofs_export_p = rz_inode_lib_init(root_path);
  if (rozofs_export_p == NULL)
  {
    printf("RozoFS: error while reading %s\n",root_path);
    exit(EXIT_FAILURE);  
  }
  /*
  ** init of the lv2 cache
  */
  lv2_cache_initialize(&cache);
  rz_set_verbose_mode(verbose);
  if (search_dir) {
    rz_scan_all_inodes(rozofs_export_p,ROZOFS_DIR,1,rozofs_visit,NULL,NULL,NULL);
  }
  else {
    rz_scan_all_inodes(rozofs_export_p,ROZOFS_REG,1,rozofs_visit,NULL,NULL,NULL);
  }
  
  exit(EXIT_SUCCESS);  
  return 0;
}
