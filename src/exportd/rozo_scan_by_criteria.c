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


/*
**---------------------------------------------------------------
**
** L O N G    O P T I O N S
**
**---------------------------------------------------------------
*/
/*
** Define an integer value for a long input string option
** to be used in enum INT_VALUE_FOR_STRING_ENUM
*/
#define DEFINE_INT_FOR_INPUT_STRING(s) INT_VALUE_FOR_STRING_ ## s ,

/*
** Declare a string to translate to an integer by the parser
** To be used in long_options[]
*/
#define INPUT_STRING_NO_ARG_TO_INT(s)   {#s, no_argument,       0, INT_VALUE_FOR_STRING_ ## s},
#define INPUT_STRING_WITH_ARG_TO_INT(s) {#s, required_argument, 0, INT_VALUE_FOR_STRING_ ## s},


/*
** String to integer value
*/
typedef enum _INT_VALUE_FOR_STRING_ENUM {   
  INT_VALUE_FOR_STRING_START=1000,
  
  DEFINE_INT_FOR_INPUT_STRING(config)
  DEFINE_INT_FOR_INPUT_STRING(eid)
  DEFINE_INT_FOR_INPUT_STRING(out)
  DEFINE_INT_FOR_INPUT_STRING(path)
  DEFINE_INT_FOR_INPUT_STRING(Ux)
  DEFINE_INT_FOR_INPUT_STRING(Unx)
  DEFINE_INT_FOR_INPUT_STRING(Ur)
  DEFINE_INT_FOR_INPUT_STRING(Unr)
  DEFINE_INT_FOR_INPUT_STRING(Uw)
  DEFINE_INT_FOR_INPUT_STRING(Unw)
  DEFINE_INT_FOR_INPUT_STRING(Gx)
  DEFINE_INT_FOR_INPUT_STRING(Gnx)
  DEFINE_INT_FOR_INPUT_STRING(Gr)
  DEFINE_INT_FOR_INPUT_STRING(Gnr)
  DEFINE_INT_FOR_INPUT_STRING(Gw)
  DEFINE_INT_FOR_INPUT_STRING(Gnw)
  DEFINE_INT_FOR_INPUT_STRING(Ox)
  DEFINE_INT_FOR_INPUT_STRING(Onx)
  DEFINE_INT_FOR_INPUT_STRING(Or)
  DEFINE_INT_FOR_INPUT_STRING(Onr)
  DEFINE_INT_FOR_INPUT_STRING(Ow)
  DEFINE_INT_FOR_INPUT_STRING(Onw)    
  DEFINE_INT_FOR_INPUT_STRING(wrerror)
  DEFINE_INT_FOR_INPUT_STRING(size)
  DEFINE_INT_FOR_INPUT_STRING(junk)
  DEFINE_INT_FOR_INPUT_STRING(sctime)
  DEFINE_INT_FOR_INPUT_STRING(hctime)
  DEFINE_INT_FOR_INPUT_STRING(ctime)
  DEFINE_INT_FOR_INPUT_STRING(smod)
  DEFINE_INT_FOR_INPUT_STRING(hmod)
  DEFINE_INT_FOR_INPUT_STRING(mod)
  DEFINE_INT_FOR_INPUT_STRING(scr8)
  DEFINE_INT_FOR_INPUT_STRING(hcr8)
  DEFINE_INT_FOR_INPUT_STRING(cr8)
  DEFINE_INT_FOR_INPUT_STRING(uid)
  DEFINE_INT_FOR_INPUT_STRING(gid)
  DEFINE_INT_FOR_INPUT_STRING(cid)
  DEFINE_INT_FOR_INPUT_STRING(sid)
  DEFINE_INT_FOR_INPUT_STRING(project)
  DEFINE_INT_FOR_INPUT_STRING(link)
  DEFINE_INT_FOR_INPUT_STRING(children)
  DEFINE_INT_FOR_INPUT_STRING(deleted)
  DEFINE_INT_FOR_INPUT_STRING(pfid)
  DEFINE_INT_FOR_INPUT_STRING(name)
  DEFINE_INT_FOR_INPUT_STRING(xattr)
  DEFINE_INT_FOR_INPUT_STRING(noxattr)
  DEFINE_INT_FOR_INPUT_STRING(slink)
  DEFINE_INT_FOR_INPUT_STRING(noreg)
  DEFINE_INT_FOR_INPUT_STRING(lt)
  DEFINE_INT_FOR_INPUT_STRING(le)
  DEFINE_INT_FOR_INPUT_STRING(eq)
  DEFINE_INT_FOR_INPUT_STRING(ne)
  DEFINE_INT_FOR_INPUT_STRING(ge)
  DEFINE_INT_FOR_INPUT_STRING(gt)
  DEFINE_INT_FOR_INPUT_STRING(regex)
  DEFINE_INT_FOR_INPUT_STRING(dir)
  DEFINE_INT_FOR_INPUT_STRING(all)
  DEFINE_INT_FOR_INPUT_STRING(trash)
  DEFINE_INT_FOR_INPUT_STRING(notrash)
  DEFINE_INT_FOR_INPUT_STRING(help)
  DEFINE_INT_FOR_INPUT_STRING(update)
  DEFINE_INT_FOR_INPUT_STRING(hupdate)
  DEFINE_INT_FOR_INPUT_STRING(supdate)
  
  DEFINE_INT_FOR_INPUT_STRING(MAX)
} INT_VALUE_FOR_STRING_ENUM;              


/*
** Declare long string option to integer translation
*/
static struct option long_options[] = {

    INPUT_STRING_WITH_ARG_TO_INT(path)
    INPUT_STRING_WITH_ARG_TO_INT(eid)
    INPUT_STRING_WITH_ARG_TO_INT(config)
    INPUT_STRING_WITH_ARG_TO_INT(out)
    INPUT_STRING_NO_ARG_TO_INT(help)
    INPUT_STRING_NO_ARG_TO_INT(cr8)
    INPUT_STRING_NO_ARG_TO_INT(hcr8)
    INPUT_STRING_NO_ARG_TO_INT(scr8)
    INPUT_STRING_NO_ARG_TO_INT(mod)
    INPUT_STRING_NO_ARG_TO_INT(hmod)
    INPUT_STRING_NO_ARG_TO_INT(smod)
    INPUT_STRING_NO_ARG_TO_INT(ctime)
    INPUT_STRING_NO_ARG_TO_INT(hctime)
    INPUT_STRING_NO_ARG_TO_INT(sctime)
    INPUT_STRING_NO_ARG_TO_INT(size)
    INPUT_STRING_NO_ARG_TO_INT(uid)
    INPUT_STRING_NO_ARG_TO_INT(gid)       
    INPUT_STRING_NO_ARG_TO_INT(cid)        
    INPUT_STRING_NO_ARG_TO_INT(sid)      
    INPUT_STRING_NO_ARG_TO_INT(project)       
    INPUT_STRING_NO_ARG_TO_INT(link)       
    INPUT_STRING_NO_ARG_TO_INT(children)      
    INPUT_STRING_NO_ARG_TO_INT(deleted)
    INPUT_STRING_NO_ARG_TO_INT(pfid)  
    INPUT_STRING_NO_ARG_TO_INT(name)        
    INPUT_STRING_NO_ARG_TO_INT(xattr)
    INPUT_STRING_NO_ARG_TO_INT(noxattr)  
    INPUT_STRING_NO_ARG_TO_INT(slink)
    INPUT_STRING_NO_ARG_TO_INT(noreg)  
    INPUT_STRING_WITH_ARG_TO_INT(lt)
    INPUT_STRING_WITH_ARG_TO_INT(le)
    INPUT_STRING_WITH_ARG_TO_INT(gt)
    INPUT_STRING_WITH_ARG_TO_INT(ge)
    INPUT_STRING_WITH_ARG_TO_INT(eq)
    INPUT_STRING_WITH_ARG_TO_INT(regex)
    INPUT_STRING_WITH_ARG_TO_INT(ne)
    INPUT_STRING_NO_ARG_TO_INT(dir)
    INPUT_STRING_NO_ARG_TO_INT(all)
    INPUT_STRING_NO_ARG_TO_INT(trash)
    INPUT_STRING_NO_ARG_TO_INT(notrash)
    INPUT_STRING_NO_ARG_TO_INT(update)
    INPUT_STRING_NO_ARG_TO_INT(hupdate)
    INPUT_STRING_NO_ARG_TO_INT(supdate)
    INPUT_STRING_NO_ARG_TO_INT(junk)
    INPUT_STRING_NO_ARG_TO_INT(wrerror)
    INPUT_STRING_NO_ARG_TO_INT(Ux)
    INPUT_STRING_NO_ARG_TO_INT(Unx)
    INPUT_STRING_NO_ARG_TO_INT(Ur)
    INPUT_STRING_NO_ARG_TO_INT(Unr)
    INPUT_STRING_NO_ARG_TO_INT(Uw)
    INPUT_STRING_NO_ARG_TO_INT(Unw)
    INPUT_STRING_NO_ARG_TO_INT(Gx)
    INPUT_STRING_NO_ARG_TO_INT(Gnx)
    INPUT_STRING_NO_ARG_TO_INT(Gr)
    INPUT_STRING_NO_ARG_TO_INT(Gnr)
    INPUT_STRING_NO_ARG_TO_INT(Gw)
    INPUT_STRING_NO_ARG_TO_INT(Gnw)
    INPUT_STRING_NO_ARG_TO_INT(Ox)
    INPUT_STRING_NO_ARG_TO_INT(Onx)
    INPUT_STRING_NO_ARG_TO_INT(Or)
    INPUT_STRING_NO_ARG_TO_INT(Onr)
    INPUT_STRING_NO_ARG_TO_INT(Ow)
    INPUT_STRING_NO_ARG_TO_INT(Onw)

    {0, 0, 0, 0}
};


export_config_t   * export_config = NULL;

INT_VALUE_FOR_STRING_ENUM scan_criteria = INT_VALUE_FOR_STRING_START;

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
int display_trash_cfg = 0;
int display_all = 0;
int display_json = 0;
int display_error = 0;
int display_striping = 0;
int first_entry = 1;
#define DO_DISPLAY           1
#define DO_HUMAN_DISPLAY     2

#define IF_DISPLAY(x)  if ((x!=0) || (display_all!=0))
#define IF_DISPLAY_HUMAN(x) if ((x==DO_HUMAN_DISPLAY) || (display_all==DO_HUMAN_DISPLAY))

char separator[128] = {0};

int first_array_element = 1;

#define NEW_NAME(field) {\
  if (display_json) {\
    pDisplay += rozofs_string_append(pDisplay,", \""#field"\" : ");\
  }\
  else {\
    pDisplay += rozofs_string_append(pDisplay," "#field"=");\
  }\
}  

#define NEW_QUOTED_NAME(field) {\
  if (display_json) {\
    pDisplay += rozofs_string_append(pDisplay,", \""#field"\" : \"");\
  }\
  else {\
    pDisplay += rozofs_string_append(pDisplay," "#field"=\"");\
  }\
}  
#define FIRST_QUOTED_NAME(field) {\
  if (display_json) {\
    pDisplay += rozofs_string_append(pDisplay,"\""#field"\" : \"");\
  }\
  else {\
    pDisplay += rozofs_string_append(pDisplay," "#field"=\"");\
  }\
}  
#define NEW_FIELD(field) {\
  if (display_json) {\
    if (strlen(#field) < 6) {\
      pDisplay += rozofs_string_append(pDisplay,",\n       \""#field"\" \t\t: ");\
    }\
    else {\
      pDisplay += rozofs_string_append(pDisplay,",\n       \""#field"\" \t: ");\
    }\
  }\
  else {\
    if (separator[0]) {\
      pDisplay += rozofs_string_append(pDisplay," ");\
      pDisplay += rozofs_string_append(pDisplay,separator);\
    }\
    pDisplay += rozofs_string_append(pDisplay," "#field"=");\
  } \
}
#define NEW_QUOTED_FIELD(field) {\
  if (display_json) {\
    if (strlen(#field) < 6) {\
      pDisplay += rozofs_string_append(pDisplay,",\n       \""#field"\" \t\t: \"");\
    }\
    else {\
      pDisplay += rozofs_string_append(pDisplay,",\n       \""#field"\" \t: \"");\
    }\
  }\
  else {\
    if (separator[0]) {\
      pDisplay += rozofs_string_append(pDisplay," ");\
      pDisplay += rozofs_string_append(pDisplay,separator);\
    }\
    pDisplay += rozofs_string_append(pDisplay," "#field"=\"");\
  } \
}
#define START_SUBARRAY(field) {\
  first_array_element = 1;\
  if (display_json) {\
    if (strlen(#field) < 6) {\
      pDisplay += rozofs_string_append(pDisplay,",\n       \""#field"\" \t\t: [ ");\
    }\
    else {\
      pDisplay += rozofs_string_append(pDisplay,",\n       \""#field"\" \t: [ ");\
    }\
  }\
}  
#define SUBARRAY_START_ELEMENT()  {\
  if (display_json) {\
    if (first_array_element) {\
      pDisplay += rozofs_string_append(pDisplay,"\n            {");\
      first_array_element = 0;\
    }\
    else {\
      pDisplay += rozofs_string_append(pDisplay,",\n            {");\
    }\
  }\
}
#define SUBARRAY_STOP_ELEMENT()  {\
  if (display_json) {\
    pDisplay += rozofs_string_append(pDisplay,"}");\
  }\
}

#define STOP_SUBARRAY() {\
  if (display_json) {\
    pDisplay += rozofs_string_append(pDisplay,"\n       ]");\
  }\
}


char   display_buffer[4096*4];
char * pDisplay;


#define LONG_VALUE_UNDEF  -1LLU



/*
** Privileges
*/
uint64_t Ux  = LONG_VALUE_UNDEF;
uint64_t Ur  = LONG_VALUE_UNDEF;
uint64_t Uw  = LONG_VALUE_UNDEF;
uint64_t Gx  = LONG_VALUE_UNDEF;
uint64_t Gr  = LONG_VALUE_UNDEF;
uint64_t Gw  = LONG_VALUE_UNDEF;
uint64_t Ox  = LONG_VALUE_UNDEF;
uint64_t Or  = LONG_VALUE_UNDEF;
uint64_t Ow  = LONG_VALUE_UNDEF;


/*
** Modification time 
*/
uint64_t    mod_lower  = LONG_VALUE_UNDEF;
uint64_t    mod_bigger = LONG_VALUE_UNDEF;
uint64_t    mod_equal  = LONG_VALUE_UNDEF;
uint64_t    mod_diff   = LONG_VALUE_UNDEF;
/*
** change time 
*/
uint64_t    ctime_lower  = LONG_VALUE_UNDEF;
uint64_t    ctime_bigger = LONG_VALUE_UNDEF;
uint64_t    ctime_equal  = LONG_VALUE_UNDEF;
uint64_t    ctime_diff   = LONG_VALUE_UNDEF;
/*
** creation time 
*/
uint64_t    cr8_lower  = LONG_VALUE_UNDEF;
uint64_t    cr8_bigger = LONG_VALUE_UNDEF;
uint64_t    cr8_equal  = LONG_VALUE_UNDEF;
uint64_t    cr8_diff  = LONG_VALUE_UNDEF;

/*
** directory update time 
*/
uint64_t    update_lower  = LONG_VALUE_UNDEF;
uint64_t    update_bigger = LONG_VALUE_UNDEF;
uint64_t    update_equal  = LONG_VALUE_UNDEF;
uint64_t    update_diff  = LONG_VALUE_UNDEF;

/*
** Size
*/
uint64_t    size_lower  = LONG_VALUE_UNDEF;
uint64_t    size_bigger = LONG_VALUE_UNDEF;
uint64_t    size_equal  = LONG_VALUE_UNDEF;
uint64_t    size_diff  = LONG_VALUE_UNDEF;

/*
** UID
*/
uint64_t    uid_equal  = LONG_VALUE_UNDEF;
uint64_t    uid_diff   = LONG_VALUE_UNDEF;

/*
** GID
*/
uint64_t    gid_equal  = LONG_VALUE_UNDEF;
uint64_t    gid_diff  = LONG_VALUE_UNDEF;

/*
** Project
*/
uint64_t    project_equal  = LONG_VALUE_UNDEF;
uint64_t    project_diff   = LONG_VALUE_UNDEF;

/*
** CID
*/
uint64_t    cid_equal  = LONG_VALUE_UNDEF;
uint64_t    cid_diff   = LONG_VALUE_UNDEF;

/*
** SID
*/
uint64_t    sid_equal  = LONG_VALUE_UNDEF;
uint64_t    sid_diff   = LONG_VALUE_UNDEF;

/*
** NLINK
*/
uint64_t    nlink_lower  = LONG_VALUE_UNDEF;
uint64_t    nlink_bigger = LONG_VALUE_UNDEF;
uint64_t    nlink_equal  = LONG_VALUE_UNDEF;
uint64_t    nlink_diff  = LONG_VALUE_UNDEF;

/*
** Children
*/
uint64_t    children_lower  = LONG_VALUE_UNDEF;
uint64_t    children_bigger = LONG_VALUE_UNDEF;
uint64_t    children_equal  = LONG_VALUE_UNDEF;
uint64_t    children_diff  = LONG_VALUE_UNDEF;

/*
** Delcount
*/
uint64_t    deleted_lower  = LONG_VALUE_UNDEF;
uint64_t    deleted_bigger = LONG_VALUE_UNDEF;
uint64_t    deleted_equal  = LONG_VALUE_UNDEF;
uint64_t    deleted_diff  = LONG_VALUE_UNDEF;


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
uint64_t     has_xattr=LONG_VALUE_UNDEF;
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
** Scan files with write errors
*/
int         only_wrerror=0;
/*
** Whether to scan all tracking files or only those whose
** creation and modification time match the research date
** criteria
*/
int scan_all_tracking_files = 0; // Only those matching research criteria



int highlight;
#define HIGHLIGHT(x) if (x) {highlight = 1; pDisplay += rozofs_string_append(pDisplay,ROZOFS_COLOR_CYAN);}else{highlight = 0;}
#define NORMAL()     if(highlight) {pDisplay += rozofs_string_append(pDisplay,ROZOFS_COLOR_NONE);}
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
*   Check the presence of a sid in a list of sid 

   @param sids    : the list of sids
   @param sid     : the sid to search for
   
   @retval 0 sid not in sid list
   @retval 1 sid in sid list 
*/
int rozofs_check_sid_in_list(sid_t * sids, sid_t sid) {
  int   sid_idx;
  
  for (sid_idx=0; sid_idx<ROZOFS_SAFE_MAX_STORCLI; sid_idx++,sids++) {
    if (*sids == 0) return 0;
    if (*sids == sid) return 1;
  }
  return 0;
}    
/*
**_______________________________________________________________________
*/
/**
*   Check the presence of a cdi and sid 

   @param inode_p    : the pointer to the inode
   @param cid        : cid to match
   @param sid        : sid to match
   
   @retval 0 sid not in sid list
   @retval 1 sid in sid list 
*/
int rozofs_check_cid_and_sid(ext_mattr_t *inode_p, cid_t cid_equal, sid_t sid_equal) {

  if (cid_equal != inode_p->s.attrs.cid) {
    return 0;
  }  
  /*
  ** Master inode matches
  */
  if (rozofs_check_sid_in_list(inode_p->s.attrs.sids, sid_equal)) {
    /*
    ** Master inode matches cid & sid
    */
    return 1;
  }

  return 0;
}  
/*
**_______________________________________________________________________
*/
/**
*   Check the presence of a cid but not sid 

   @param inode_p    : the pointer to the inode
   @param cid_equal  : cid to match
   @param sid_diff   : sid not to match
   
   @retval 0 sid not in sid list
   @retval 1 sid in sid list 
*/
int rozofs_check_cid_and_not_sid(ext_mattr_t *inode_p, cid_t cid_equal, sid_t sid_diff) {
  int           cid_match = 0;
  
  if (inode_p->s.attrs.cid == cid_equal) {
    /*
    ** Master inode matches
    */
    cid_match = 1;
    if (rozofs_check_sid_in_list(inode_p->s.attrs.sids, sid_diff)) {
      /*
      ** Master inode matches cid & sid
      */
      return 0;
    }
  }   

  return cid_match;
}  

/*
**_______________________________________________________________________
*/
/**
*   Check the presence of a cid 

   @param inode_p    : the pointer to the inode
   @param cid        : cid to match
   
   @retval 0 cid is not present
   @retval 1 cid is present
*/
int rozofs_check_cid(ext_mattr_t *inode_p, cid_t cid_equal) {
  
  if (inode_p->s.attrs.cid == cid_equal) {
    return 1;
  }   
  
  return 0;
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
  ** Only files with write errors
  */
  if ((only_wrerror)&&(!rozofs_is_wrerror((lv2_entry_t*)inode_p))) {
    return 0;
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
  if (cr8_bigger != LONG_VALUE_UNDEF) {
    if (inode_p->s.cr8time < cr8_bigger) {
      return 0;
    }
  }  

  /*
  ** Must have a creation time lower than cr8_lower
  */    
  if (cr8_lower != LONG_VALUE_UNDEF) {
    if (inode_p->s.cr8time > cr8_lower) {
      return 0;
    }
  }  

  /*
  ** Must have a creation time equal to cr8_equal
  */    
  if (cr8_equal != LONG_VALUE_UNDEF) {
    if (inode_p->s.cr8time != cr8_equal) {
      return 0;
    }
  } 
  
  /*
  ** Must have a creation time different from cr8_equal
  */    
  if (cr8_diff != LONG_VALUE_UNDEF) {
    if (inode_p->s.cr8time == cr8_diff) {
      return 0;
    }
  }
   
  /*
  ** Must have a modification time bigger than mod_bigger
  */ 
  if (mod_bigger != LONG_VALUE_UNDEF) {
    if (inode_p->s.attrs.mtime < mod_bigger) {
      return 0;
    }
  }  

  /*
  ** Must have a modification time lower than mod_lower
  */    
  if (mod_lower != LONG_VALUE_UNDEF) {
    if (inode_p->s.attrs.mtime > mod_lower) {
      return 0;
    }
  }     

  /*
  ** Must have a modification time equal to mod_equal
  */    
  if (mod_equal != LONG_VALUE_UNDEF) {
    if (inode_p->s.attrs.mtime != mod_equal) {
      return 0;
    }
  } 
  
  /*
  ** Must have a modification time different from mod_diff
  */    
  if (mod_diff != LONG_VALUE_UNDEF) {
    if (inode_p->s.attrs.mtime == mod_diff) {
      return 0;
    }
  }
   
  /*
  ** Must have a change time bigger than ctime_bigger
  */ 
  if (ctime_bigger != LONG_VALUE_UNDEF) {
    if (inode_p->s.attrs.ctime < ctime_bigger) {
      return 0;
    }
  }  

  /*
  ** Must have a change time lower than ctime_lower
  */    
  if (ctime_lower != LONG_VALUE_UNDEF) {
    if (inode_p->s.attrs.ctime > ctime_lower) {
      return 0;
    }
  }     

  /*
  ** Must have a change time equal to ctime_equal
  */    
  if (ctime_equal != LONG_VALUE_UNDEF) {
    if (inode_p->s.attrs.ctime != ctime_equal) {
      return 0;
    }
  } 
  
  /*
  ** Must have a change time different from ctime_diff
  */    
  if (ctime_diff != LONG_VALUE_UNDEF) {
    if (inode_p->s.attrs.ctime == ctime_diff) {
      return 0;
    }
  }
  /* 
  ** Privileges
  */
  
  /* User privileges */
  if (Ux != LONG_VALUE_UNDEF) {
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
  if (Uw != LONG_VALUE_UNDEF) {
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
  if (Ur != LONG_VALUE_UNDEF) {
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
  if (Gx != LONG_VALUE_UNDEF) {
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
  if (Gw != LONG_VALUE_UNDEF) {
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
  if (Gr != LONG_VALUE_UNDEF) {
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
  if (Ox != LONG_VALUE_UNDEF) {
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
  if (Ow != LONG_VALUE_UNDEF) {
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
  if (Or != LONG_VALUE_UNDEF) {
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
      if (update_bigger != LONG_VALUE_UNDEF) {
	if (stats_attr_p->s.update_time < update_bigger) {
	  return 0;
	}
      }  

      /*
      ** Must have a modification time lower than update_lower
      */    
      if (update_lower != LONG_VALUE_UNDEF) {
	if (stats_attr_p->s.update_time > update_lower) {
	  return 0;
	}
      }     

      /*
      ** Must have a modification time equal to update_equal
      */    
      if (update_equal != LONG_VALUE_UNDEF) {
	if (stats_attr_p->s.update_time != update_equal) {
	  return 0;
	}
      } 

      /*
      ** Must have a modification time different from update_diff
      */    
      if (update_diff != LONG_VALUE_UNDEF) {
	if (stats_attr_p->s.update_time == update_diff) {
	  return 0;
	}
      }
    }
    /*
    ** Must have a size bigger than size_bigger
    */ 
    if (size_bigger != LONG_VALUE_UNDEF) {
      if (stats_attr_p->s.nb_bytes < size_bigger) {
        return 0;
      }
    }  

    /*
    ** Must have a size lower than size_lower
    */    
    if (size_lower != LONG_VALUE_UNDEF) {
      if (stats_attr_p->s.nb_bytes  > size_lower) {
        return 0;
      }
    }     

    /*
    ** Must have a size equal to size_equal
    */    
    if (size_equal != LONG_VALUE_UNDEF) {
      if (stats_attr_p->s.nb_bytes  != size_equal) {
        return 0;
      }
    }   

    /*
    ** Must have a size time different from size_diff
    */    
    if (size_diff != LONG_VALUE_UNDEF) {
      if (stats_attr_p->s.nb_bytes  == size_diff) {
        return 0;
      }
    }
  }     
  
  /*
  ** Must have an uid equal to size_equal
  */    
  if (uid_equal != LONG_VALUE_UNDEF) {
    if (inode_p->s.attrs.uid != uid_equal) {
      return 0;
    }
  }         

  /*
  ** Must have an uid different from uid_diff
  */    
  if (uid_diff != LONG_VALUE_UNDEF) {
    if (inode_p->s.attrs.uid == uid_diff) {
      return 0;
    }
  }
  
  /*
  ** Must have an gid equal to size_equal
  */    
  if (gid_equal != LONG_VALUE_UNDEF) {
    if (inode_p->s.attrs.gid != gid_equal) {
      return 0;
    }
  }

  /*
  ** Must have an gid different from gid_diff
  */    
  if (gid_diff != LONG_VALUE_UNDEF) {
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
    if (size_bigger != LONG_VALUE_UNDEF) {
      if (inode_p->s.attrs.size < size_bigger) {
        return 0;
      }
    }  

    /*
    ** Must have a size lower than size_lower
    */    
    if (size_lower != LONG_VALUE_UNDEF) {
      if (inode_p->s.attrs.size > size_lower) {
        return 0;
      }
    }     

    /*
    ** Must have a size equal to size_equal
    */    
    if (size_equal != LONG_VALUE_UNDEF) {
      if (inode_p->s.attrs.size != size_equal) {
        return 0;
      }
    }   

    /*
    ** Must have a size time different from size_diff
    */    
    if (size_diff != LONG_VALUE_UNDEF) {
      if (inode_p->s.attrs.size == size_diff) {
        return 0;
      }
    }
       
    /*
    ** Must have a cid equal to cid_equal
    */    
    while (cid_equal != LONG_VALUE_UNDEF) {
    
      /*
      ** Check the cid and sid are present in one distribution
      */
      if (sid_equal != LONG_VALUE_UNDEF) {        
        if (rozofs_check_cid_and_sid(inode_p,cid_equal,sid_equal) == 0) {
          return 0;
        }
        break;  
      } 
      
      /*
      ** Check the cid is present but not the sid
      */      
      if (sid_diff != LONG_VALUE_UNDEF) {        
        if (rozofs_check_cid_and_not_sid(inode_p,cid_equal,sid_diff) == 0) {
          return 0;
        }
        break;  
      }  
      
      /*
      ** Just check the CID presence 
      */  
      if (rozofs_check_cid(inode_p,cid_equal) == 0) {
        return 0;
      }   
      break;
    }

    /*
    ** Must have an cid different from cid_diff
    */    
    while (cid_diff != LONG_VALUE_UNDEF) {    
      
      /*
      ** Check the cid and sid ar not present
      */      
      if (sid_diff != LONG_VALUE_UNDEF) {        
        if (rozofs_check_cid_and_sid(inode_p,cid_diff,sid_diff)) {
          return 0;
        }
        break;  
      }
      
      /*
      ** Just check the CID presence 
      */  
      if (rozofs_check_cid(inode_p,cid_diff)) {
        return 0;
      }   
      break;
    }

    
    /*
    ** Must have a nlink bigger than nlink_bigger
    */ 
    if (nlink_bigger != LONG_VALUE_UNDEF) {
      if (inode_p->s.attrs.nlink < nlink_bigger) {
        return 0;
      }
    }  

    /*
    ** Must have a nlink lower than nlink_lower
    */    
    if (nlink_lower != LONG_VALUE_UNDEF) {
      if (inode_p->s.attrs.nlink > nlink_lower) {
        return 0;
      }
    }  

    /*
    ** Must have a nlink equal to nlink_equal
    */    
    if (nlink_equal != LONG_VALUE_UNDEF) {
      if (inode_p->s.attrs.nlink != nlink_equal) {
        return 0;
      }
    } 

    /*
    ** Must have a nlink different from nlink_diff
    */    
    if (nlink_diff != LONG_VALUE_UNDEF) {
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
    if (deleted_bigger != LONG_VALUE_UNDEF) {
      if (deleted < deleted_bigger) {
        return 0;
      }
    }  

    /*
    ** Must have a delete count lower than deleted_lower
    */    
    if (deleted_lower != LONG_VALUE_UNDEF) {
      if (deleted > deleted_lower) {
        return 0;
      }
    }  

    /*
    ** Must have a delete count equal to deleted_equal
    */    
    if (deleted_equal != LONG_VALUE_UNDEF) {
      if (deleted != deleted_equal) {
        return 0;
      }
    } 

    /*
    ** Must have a delete count different from deleted_diff
    */    
    if (deleted_diff != LONG_VALUE_UNDEF) {
      if (deleted == deleted_diff) {
        return 0;
      }
    }
  
    /*
    ** Must have children bigger than children_bigger
    */ 
    if (children_bigger != LONG_VALUE_UNDEF) {
      if (inode_p->s.attrs.children < children_bigger) {
        return 0;
      }
    }  

    /*
    ** Must have a children lower than children_lower
    */    
    if (children_lower != LONG_VALUE_UNDEF) {
      if (inode_p->s.attrs.children > children_lower) {
        return 0;
      }
    }  

    /*
    ** Must have a children equal to children_equal
    */    
    if (children_equal != LONG_VALUE_UNDEF) {
      if (inode_p->s.attrs.children != children_equal) {
        return 0;
      }
    } 

    /*
    ** Must have a children different from children_diff
    */    
    if (children_diff != LONG_VALUE_UNDEF) {
      if (inode_p->s.attrs.children == children_diff) {
        return 0;
      }
    }  
    
  }

  /*
  ** Project equals
  */
  if (project_equal != LONG_VALUE_UNDEF) {
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
  if (project_diff != LONG_VALUE_UNDEF) {
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
  if (has_xattr != LONG_VALUE_UNDEF) {    
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

    default:        
      pChar = rozo_get_path(exportd,inode_attr_p, fullName,sizeof(fullName),0);
  }     

  /*
  ** Put entry_per_line entries per line
  */
  if (pChar==NULL) {
    return 0;
  }  

  pDisplay  = display_buffer;
  *pDisplay = 0;
  
  if (display_json) {
    if (first_entry) {
      first_entry = 0;
    }
    else {
      pDisplay += rozofs_string_append(pDisplay,",");
    }   
    pDisplay += rozofs_string_append(pDisplay,"\n    {  \"name\" : \"");
    pDisplay += rozofs_string_append(pDisplay,pChar);
    pDisplay += rozofs_string_append(pDisplay,"\"");
  }
  else {  
    pDisplay += rozofs_string_append(pDisplay,pChar);
  }

  IF_DISPLAY(display_id) {
    NEW_QUOTED_FIELD(fid);  
    pDisplay += rozofs_fid_append(pDisplay,inode_p->s.attrs.fid);
    pDisplay += rozofs_string_append(pDisplay,"\"");   
    NEW_QUOTED_FIELD(pfid);  
    pDisplay += rozofs_fid_append(pDisplay,inode_p->s.pfid);
    pDisplay += rozofs_string_append(pDisplay,"\"");   
  }   
    
  /*
  ** User id
  */  
  IF_DISPLAY(display_uid) {
    NEW_FIELD(uid); 
    pDisplay += rozofs_u32_append(pDisplay,inode_p->s.attrs.uid);
  }

  /*
  ** Group id
  */  
  IF_DISPLAY(display_gid) {
    NEW_FIELD(gid);   
    pDisplay += rozofs_u32_append(pDisplay,inode_p->s.attrs.gid);
  }
  
  /*
  ** Linuxx privileges
  */  
  IF_DISPLAY(display_priv) {
    NEW_FIELD(priv);  
    if (display_json) {
      pDisplay += rozofs_string_append(pDisplay,"\"");
      pDisplay += rozofs_mode2String(pDisplay, inode_p->s.attrs.mode & (S_IRWXU|S_IRWXG|S_IRWXO));
      pDisplay += rozofs_string_append(pDisplay,"\"");
    }
    else {
      pDisplay += rozofs_u32_append(pDisplay, inode_p->s.attrs.mode & (S_IRWXU|S_IRWXG|S_IRWXO));
    }  
  }
  
  /*
  ** Presence of extended attributes
  */
  IF_DISPLAY(display_xattr) {
    NEW_FIELD(xattr);  
    if (rozofs_has_xattr(inode_p->s.attrs.mode)) {
      pDisplay += rozofs_string_append(pDisplay,"\"YES\"");
    }
    else {
      pDisplay += rozofs_string_append(pDisplay,"\"NO\"");
    }
  }

  /*
  ** Directory
  */
  if (S_ISDIR(inode_p->s.attrs.mode)) {
    ext_dir_mattr_t *ext_dir_mattr_p = (ext_dir_mattr_t*)inode_p->s.attrs.sids; 

    IF_DISPLAY(display_size) {
      NEW_FIELD(size); 
      pDisplay += rozofs_u64_append(pDisplay,ext_dir_mattr_p->s.nb_bytes);        
    }
    IF_DISPLAY(display_children) {       
      NEW_FIELD(children); 
      pDisplay += rozofs_u64_append(pDisplay,inode_p->s.attrs.children);                
    }
    IF_DISPLAY(display_deleted) {       
      uint64_t deleted = inode_p->s.hpc_reserved.dir.nb_deleted_files;
      NEW_FIELD(deleted); 
      pDisplay += rozofs_u64_append(pDisplay,deleted);                
    }
    IF_DISPLAY(display_project) {
      NEW_FIELD(project);       
      pDisplay += rozofs_u32_append(pDisplay, inode_p->s.attrs.cid);
    }
    IF_DISPLAY(display_trash_cfg) {
      uint8_t trash = ((rozofs_dir0_sids_t*)&inode_p->s.attrs.sids[0])->s.trash;
      NEW_FIELD(trash);
      if (trash) {
        if (trash == ROZOFS_DIR_TRASH_RECURSIVE) {
          pDisplay += rozofs_string_append(pDisplay,"\"RECURSIVE\"");
        }
        else {
          pDisplay += rozofs_string_append(pDisplay,"\"ENABLED\"");
        }   
      }  
      else {
        pDisplay += rozofs_string_append(pDisplay,"\"DISABLED\"");      
      }
    }    
    IF_DISPLAY(display_update) {
      ext_dir_mattr_t * stats_attr_p = (ext_dir_mattr_t *)&inode_p->s.attrs.sids[0];
      if (stats_attr_p->s.version >=  ROZOFS_DIR_VERSION_1) {
        IF_DISPLAY_HUMAN(display_update) {
          NEW_QUOTED_FIELD(hupdate);
          pDisplay += rozofs_time2string(pDisplay,stats_attr_p->s.update_time);            
          pDisplay += rozofs_string_append(pDisplay,"\"");   
        }
        else {
          NEW_FIELD(supdate);
          pDisplay += rozofs_u64_append(pDisplay,stats_attr_p->s.update_time);  
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
      pDisplay += rozofs_u64_append(pDisplay,inode_p->s.attrs.size);        
    }  
    IF_DISPLAY(display_nlink) {       
      NEW_FIELD(nlink); 
      pDisplay += rozofs_u64_append(pDisplay,inode_p->s.attrs.nlink);                
    }
    IF_DISPLAY(display_project) {
      NEW_FIELD(project);       
      pDisplay += rozofs_u64_append(pDisplay,inode_p->s.hpc_reserved.reg.share_id);
    }
    IF_DISPLAY(display_error) {
      NEW_FIELD(wrerror);       
      if (rozofs_is_wrerror((lv2_entry_t*)inode_p)) {
        pDisplay += rozofs_string_append(pDisplay,"\"YES\"");
      }
      else {
        pDisplay += rozofs_string_append(pDisplay,"\"NO\"");
      }
    } 
              
    IF_DISPLAY(display_distrib) {

      START_SUBARRAY(distribution);
      /*
      ** Display distribution 
      */
        SUBARRAY_START_ELEMENT();
        FIRST_QUOTED_NAME(storage_fid);
        pDisplay += rozofs_fid_append(pDisplay,inode_p->s.attrs.fid);
        pDisplay += rozofs_string_append(pDisplay,"\"");   

        HIGHLIGHT (cid_equal == inode_p->s.attrs.cid) ;  
        NEW_NAME(cid)
        pDisplay += rozofs_u32_append(pDisplay,inode_p->s.attrs.cid);
        NORMAL();

        NEW_NAME(sid)
        pDisplay += rozofs_string_append(pDisplay,"[");
        int sid_idx;
        for (sid_idx=0; sid_idx<ROZOFS_SAFE_MAX_STORCLI; sid_idx++) {
          if (inode_p->s.attrs.sids[sid_idx] == 0) break;
          if (sid_idx != 0) pDisplay += rozofs_string_append(pDisplay,",");
          HIGHLIGHT ((cid_equal == inode_p->s.attrs.cid)&&(sid_equal == inode_p->s.attrs.sids[sid_idx])) 
          pDisplay += rozofs_u32_append(pDisplay,inode_p->s.attrs.sids[sid_idx]);
          NORMAL();
        }
        pDisplay += rozofs_string_append(pDisplay,"]");
        SUBARRAY_STOP_ELEMENT();   
        STOP_SUBARRAY();
    }     
  } 
  
  IF_DISPLAY(display_cr8) {
    IF_DISPLAY_HUMAN(display_cr8) {
      NEW_FIELD(hcr8); 
      if (display_json) {
        pDisplay += rozofs_string_append(pDisplay,"\"");
        pDisplay += rozofs_time2string(pDisplay,inode_p->s.cr8time);  
        pDisplay += rozofs_string_append(pDisplay,"\"");
      }
      else {
        pDisplay += rozofs_time2string(pDisplay,inode_p->s.cr8time);        
      }  
    }  
    else {
      NEW_FIELD(scr8);
      pDisplay += rozofs_u64_append(pDisplay,inode_p->s.cr8time);  
    }  
  }
  
  IF_DISPLAY(display_mod) {
    IF_DISPLAY_HUMAN(display_mod) {
      NEW_FIELD(hmod); 
      if (display_json) {
        pDisplay += rozofs_string_append(pDisplay,"\"");
        pDisplay += rozofs_time2string(pDisplay,inode_p->s.attrs.mtime);  
        pDisplay += rozofs_string_append(pDisplay,"\"");
      }
      else {
        pDisplay += rozofs_time2string(pDisplay,inode_p->s.attrs.mtime);        
      }        
    }        
    else {
      NEW_FIELD(smod);
      pDisplay += rozofs_u64_append(pDisplay,inode_p->s.attrs.mtime);  
    }  
  }

  IF_DISPLAY(display_ctime) {
    IF_DISPLAY_HUMAN(display_ctime) {
      NEW_FIELD(hctime); 
      if (display_json) {
        pDisplay += rozofs_string_append(pDisplay,"\"");
        pDisplay += rozofs_time2string(pDisplay,inode_p->s.attrs.ctime);  
        pDisplay += rozofs_string_append(pDisplay,"\"");
      }
      else {
        pDisplay += rozofs_time2string(pDisplay,inode_p->s.attrs.ctime);        
      }        
    }        
    else {
      NEW_FIELD(sctime);
      pDisplay += rozofs_u64_append(pDisplay,inode_p->s.attrs.ctime);  
    }  
  }  

  IF_DISPLAY(display_atime) {
    IF_DISPLAY_HUMAN(display_atime) {
      NEW_FIELD(hatime); 
      if (display_json) {
        pDisplay += rozofs_string_append(pDisplay,"\"");
        pDisplay += rozofs_time2string(pDisplay,inode_p->s.attrs.atime);  
        pDisplay += rozofs_string_append(pDisplay,"\"");
      }
      else {
        pDisplay += rozofs_time2string(pDisplay,inode_p->s.attrs.atime);        
      }        
    }        
    else {
      NEW_FIELD(satime);
      pDisplay += rozofs_u64_append(pDisplay,inode_p->s.attrs.atime);  
    }  
  }  

  if (display_json) {
    pDisplay += rozofs_string_append(pDisplay,"  }");    
  }
  else {   
    cur_entry_per_line++;
    if (cur_entry_per_line >= entry_per_line) {
      cur_entry_per_line = 0;
      pDisplay += rozofs_string_append(pDisplay,"\n");
    }
    else {
      pDisplay += rozofs_string_append(pDisplay," ");
      pDisplay += rozofs_string_append(pDisplay,separator);
    } 
  } 
  
  printf("%s",display_buffer);
  
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

  nb_scanned_entries++;

  /*
  ** Must have a size bigger than size_bigger
  */ 
  if (size_bigger != LONG_VALUE_UNDEF) {
    if (rmentry->size < size_bigger) {
      return 0;
    }
  }  

  /*
  ** Must have a size lower than size_lower
  */    
  if (size_lower != LONG_VALUE_UNDEF) {
    if (rmentry->size > size_lower) {
      return 0;
    }
  }     

  /*
  ** Must have a size equal to size_equal
  */    
  if (size_equal != LONG_VALUE_UNDEF) {
    if (rmentry->size != size_equal) {
      return 0;
    }
  }   

  /*
  ** Must have a size time different from size_diff
  */    
  if (size_diff != LONG_VALUE_UNDEF) {
    if (rmentry->size == size_diff) {
      return 0;
    }
  }

  /*
  ** Must have a cid equal to cid_equal
  */    
  if (cid_equal != LONG_VALUE_UNDEF) {
    if (rmentry->cid != cid_equal) {
      return 0;
    }

    /*
    ** Must have a sid equal to sid_equal
    */
    if (sid_equal != LONG_VALUE_UNDEF) {
      if (!rozofs_check_sid_in_list(rmentry->current_dist_set, sid_equal)) {
        return 0;
      }
    }
    /*
    ** Must not have a sid equal to sid_diff
    */        
    if (sid_diff != LONG_VALUE_UNDEF) {
      if (rozofs_check_sid_in_list(rmentry->current_dist_set, sid_diff)) {
        return 0;
      }
    }        
  }

  /*
  ** Must have an cid different from cid_diff
  */    
  if (cid_diff != LONG_VALUE_UNDEF) {
  
    if (rmentry->cid == cid_diff) {
    
      /*
      ** Must not have a have cid equal to cid_diff and sid equal to sid_diff
      */        
      if (sid_diff != LONG_VALUE_UNDEF) {
        if (rozofs_check_sid_in_list(rmentry->current_dist_set, sid_diff)) {
          return 0;
        }
      }
      /*
      ** Must not have cid equal to cid_diff 
      */
      else {
        return 0;
      }
    }     
  }
  
             
  /*
  ** This inode is valid
  */
  nb_matched_entries++;
  pDisplay = display_buffer;

  
  if (display_json) {
    if (first_entry) {
      first_entry = 0;
    }
    else {
      pDisplay += rozofs_string_append(pDisplay,",");
    }   
    pDisplay += rozofs_string_append(pDisplay,"\n    {  \"name\" : \"");
    pDisplay += rozofs_fid_append(pDisplay,rmentry->trash_inode);
    pDisplay += rozofs_string_append(pDisplay,"\"");
  }
  else {  
    pDisplay += rozofs_fid_append(pDisplay,rmentry->trash_inode);
  }

  IF_DISPLAY(display_size) {
    NEW_FIELD(size);       
    pDisplay += rozofs_u64_append(pDisplay,rmentry->size);        
  }  

  IF_DISPLAY(display_distrib) {
    NEW_FIELD(cid); 
    pDisplay += rozofs_u32_append(pDisplay,rmentry->cid);
    NEW_FIELD(sid); 

    pDisplay += rozofs_string_append(pDisplay,"[");
    int sid_idx;
    for (sid_idx=0; sid_idx<ROZOFS_SAFE_MAX_STORCLI; sid_idx++) {
      if (rmentry->current_dist_set[sid_idx] == 0) break;
      if (sid_idx != 0) pDisplay += rozofs_string_append(pDisplay,",");
      pDisplay += rozofs_u32_append(pDisplay,rmentry->current_dist_set[sid_idx]);
    }          
    pDisplay += rozofs_string_append(pDisplay,"]");
  }     
  
  IF_DISPLAY(display_id) {
    NEW_QUOTED_FIELD(fid);  
    pDisplay += rozofs_fid_append(pDisplay,rmentry->fid);    
    pDisplay += rozofs_string_append(pDisplay,"\"");   
  }        

  if (display_json) {
     pDisplay += rozofs_string_append(pDisplay,"  }");    
  }
  else {   
    cur_entry_per_line++;
    if (cur_entry_per_line >= entry_per_line) {
      cur_entry_per_line = 0;
       pDisplay += rozofs_string_append(pDisplay,"\n");
    }
    else {
      pDisplay += rozofs_string_append(pDisplay," ");
      pDisplay += rozofs_string_append(pDisplay,separator);
    } 
  } 
  
  if (nb_matched_entries >= max_display) {
    rozo_lib_stop_var = 1;
  }    
  printf("%s",display_buffer);
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
    printf("\n"ROZOFS_COLOR_BOLD""ROZOFS_COLOR_RED"!!!  %s !!! "ROZOFS_COLOR_NONE"\n",error_buffer);
    exit(EXIT_FAILURE);      
  }

  printf("\n"ROZOFS_COLOR_BOLD"RozoFS File system scanning utility - %s"ROZOFS_COLOR_NONE"\n", VERSION);
  printf("This RozoFS utility enables to scan files or (exclusive) directories in a RozoFS file system\naccording to one or several criteria and conditions.\n");
  printf("\n\033[4mUsage:\033[0m\n\t\033[1mrozo_scan [FILESYSTEM] [OPTIONS] { <CRITERIA> } { <FIELD> <CONDITIONS> } [OUTPUT]\033[0m\n\n");
  printf("\n\033[1mFILESYSTEM:\033[0m\n");
  printf("\tThe FILESYSTEM can be omitted when current path is a RozoFS mountpoint on the file system one want to scan.\n");
  printf("\tElse the targeted RozoFS file system must be provided by eid:\n");
  printf("\t\033[1m-e,--eid <eid> [--config <cfg file>]\033[0m\t\texport identifier and optionally its configuration file.\n");
  printf("\n\033[1mOPTIONS:\033[0m\n");
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
  printf("\t\033[1m-w,--wrerror\033[0m\t\tScan files having encountered a detected write error.\n");
  printf("\t\033[1m--U<x|w|r>\033[0m\t\tUser has <executable|write|read> priviledge.\n");
  printf("\t\033[1m--Un<x|w|r>\033[0m\t\tUser has not <executable|write|read> priviledge.\n");
  printf("\t\033[1m--G<x|w|r>\033[0m\t\tGroup has <executable|write|read> priviledge.\n");
  printf("\t\033[1m--Gn<x|w|r>\033[0m\t\tGroup has not <executable|write|read> priviledge.\n");
  printf("\t\033[1m--O<x|w|r>\033[0m\t\tOthers have <executable|write|read> priviledge.\n");
  printf("\t\033[1m--On<x|w|r>\033[0m\t\tOthers have not <executable|write|read> priviledge.\n");
  printf("\n\033[1mFIELD:\033[0m\n");
  printf("\t\033[1m-n,--name\033[0m\t\tfile/directory name (3).\n");
  printf("\t\033[1m-P,--project\033[0m\t\tproject identifier (1).\n"); 
  printf("\t\033[1m-s,--size\033[0m\t\tfile/directory size.\n"); 
  printf("\t\033[1m-g,--gid\033[0m\t\tgroup identifier (1).\n"); 
  printf("\t\033[1m-u,--uid\033[0m\t\tuser identifier (1).\n"); 
  printf("\t\033[1m-c,--cid\033[0m\t\tcluster identifier (file only) (1).\n"); 
  printf("\t\033[1m-z,--sid\033[0m\t\tSID identifier (file only) (1).\n"); 
  printf("\t\033[1m--hcr8\033[0m\t\t\tcreation date in human readable format.\n");
  printf("\t\033[1m--scr8\033[0m\t\t\tcreation date in seconds.\n");
  printf("\t\033[1m--hmod\033[0m\t\t\tmodification date in human readable format.\n"); 
  printf("\t\033[1m--smod\033[0m\t\t\tmodification date in seconds..\n"); 
  printf("\t\033[1m--hctime\033[0m\t\tchange date in human readable format.\n"); 
  printf("\t\033[1m--sctime\033[0m\t\tchange date in seconds.\n"); 
  printf("\t\033[1m--hupdate\033[0m\t\tdirectory update date in human readable format (directory only).\n"); 
  printf("\t\033[1m--supdate\033[0m\t\tdirectory update date in seconds (directory only).\n"); 
  printf("\t\033[1m--link\033[0m\t\t\tnumber of links (file only).\n"); 
  printf("\t\033[1m--children\033[0m\t\tnumber of children (directory only).\n"); 
  printf("\t\033[1m--deleted\033[0m\t\tnumber of deleted inode in the trash (directory only).\n"); 
  printf("\t\033[1m--pfid\033[0m\t\t\tParent FID (2).\n");
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
  printf("\nFile size can be expressed in K, M, G, T or P units : 8, 9M, 10T\n");
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
  printf("\t\033[1mtrash\033[0m\t\t\tdisplay directory trash configuration.\n");
  printf("\t\033[1mid\033[0m\t\t\tdisplay RozoFS FID.\n");
  printf("\t\033[1merror\033[0m\t\t\tdisplay file write error detected.\n");
  printf("\t\033[1mstrip\033[0m\t\t\tdisplay file stripping information.\n");
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
  exit(EXIT_SUCCESS);     
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
    return LONG_VALUE_UNDEF;
  }    
  return val;    
}
/*
**__________________________________________________________________
**
** Read the name from the inode
  
  @param rootPath : export root path
  @param buffer   : where to copy the name back
  @param len      : name length
*/
static inline uint64_t rozofs_scan_size_string(char * sizeString) {
  uint64_t   value;
  char     * pUnits = sizeString;
   
  value =  rozofs_scan_u64(sizeString);
  if (value == -1) return -1;
  
  while ( (*pUnits >= 0x30) && (*pUnits <= 0x39)) pUnits++;
  if (*pUnits == 0) return value;
  if (*pUnits == 'K') return 1024UL*value;
  if (*pUnits == 'M') return 1024UL*1024UL*value;
  if (*pUnits == 'G') return 1024UL*1024UL*1024UL*value;
  if (*pUnits == 'T') return 1024UL*1024UL*1024UL*1024UL*value;
  if (*pUnits == 'P') return 1024UL*1024UL*1024UL*1024UL*1024UL*value;
  return value;
}
   
/*
**_______________________________________________________________________
*/
/** Find out the export root path from its eid reading the configuration file
*   
    @param  eid : eport identifier
    
    @retval -the root path or null when no such eid
*/
export_config_t * get_export_config(uint8_t eid) {
  list_t          * e;
  export_config_t * econfig;

  list_for_each_forward(e, &exportd_config.exports) {

    econfig = list_entry(e, export_config_t, list);
    if (econfig->eid == eid) return econfig;   
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
  if (cr8_bigger != LONG_VALUE_UNDEF) {
    /*
    ** The tracking file must have been modified at this timer or later
    */
    docheck(inode_p->s.attrs.mtime,cr8_bigger);
  }  

  /*
  ** Must have a creation time lower than cr8_lower
  */    
  if (cr8_lower != LONG_VALUE_UNDEF) {
    /*
    ** The tracking file must have been created at this time or before
    */
    docheck(cr8_lower,inode_p->s.cr8time);
  }  

  /*
  ** Must have a creation time equal to cr8_equal
  */    
  if (cr8_equal != LONG_VALUE_UNDEF) {
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
  if (mod_bigger != LONG_VALUE_UNDEF) {
    /*
    ** The tracking file must have been modified at this timer or later
    */
    docheck(inode_p->s.attrs.mtime, mod_bigger);
  }  

  /*
  ** Must have a modification time lower than mod_lower
  */    
  if (mod_lower != LONG_VALUE_UNDEF) {
    /*
    ** The tracking file must have been created at this time or before
    */
    docheck(mod_lower,inode_p->s.cr8time);
  }     

  /*
  ** Must have a modification time equal to mod_equal
  */    
  if (mod_equal != LONG_VALUE_UNDEF) {
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
  if (ctime_bigger != LONG_VALUE_UNDEF) {
    /*
    ** The tracking file must have been modified at this timer or later
    */
    docheck(inode_p->s.attrs.ctime, ctime_bigger);
  }  

  /*
  ** Must have a modification time lower than mod_lower
  */    
  if (ctime_lower != LONG_VALUE_UNDEF) {
    /*
    ** The tracking file must have been created at this time or before
    */
    docheck(ctime_lower,inode_p->s.cr8time);
  }     

  /*
  ** Must have a modification time equal to mod_equal
  */    
  if (ctime_equal != LONG_VALUE_UNDEF) {
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
      
    if (strncmp(p, "strip", 3)==0) {
      display_striping = DO_DISPLAY;
      NEXT(p);
    }     
               
    if (strncmp(p, "distrib", 4)==0) {
      display_distrib = DO_DISPLAY;
      NEXT(p);
    }       
    
    if (strncmp(p, "error", 4)==0) {
      display_error = DO_DISPLAY;
      NEXT(p);
    }           
    
    if (strncmp(p, "trash", 5)==0) {
      display_trash_cfg = DO_DISPLAY;
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

#define EXPECTING_COMPARATOR_CHECK \
        if (expect_comparator) {\
          if (option_index!=-1) {\
            if (criteria_string)  usage("Expecting --lt, --le, --gt, --ge or --ne for %s. Got --%s", criteria_string, long_options[option_index].name);\
            else                  usage("Expecting --lt, --le, --gt, --ge or --ne for %s. Got -%c", criteria_char, long_options[option_index].name);\
          }\
          else {\
            if (criteria_string)  usage("Expecting --lt, --le, --gt, --ge or --ne for %s. Got --%s", criteria_string, c);\
            else                  usage("Expecting --lt, --le, --gt, --ge or --ne for %s. Got -%c", criteria_char, c);\
          }\
        }
              
#define NEW_COMPARISON_CHECKS(criteria){\
        EXPECTING_COMPARATOR_CHECK\
        expect_comparator = 1; \
        scan_criteria = criteria;\
        if (option_index!=-1) {\
          criteria_string = (char *)long_options[option_index].name;\
        }\
        else {\
          criteria_char = c;\
          criteria_string = NULL;\
        }\
}
#define NEW_OPTION_CHECKS(){\
        EXPECTING_COMPARATOR_CHECK\
}
#define BAD_FORMAT \
  if (criteria_string) usage("Bad format for --%s %s \"%s\"",criteria_string,comp,optarg);\
  else                 usage("Bad format for -%c %s \"%s\"",criteria_char,comp,optarg);
 
#define SCAN_U64(x) \
  x = rozofs_scan_u64(optarg);\
  if (x==LONG_VALUE_UNDEF) {\
    BAD_FORMAT;\
  } 
#define SCAN_DATE(x) \
  x = rozofs_date2time(optarg);\
  if (x==LONG_VALUE_UNDEF) {\
    BAD_FORMAT;\
  }   
#define SCAN_SIZE(x) \
  x = rozofs_scan_size_string(optarg);\
  if (x==LONG_VALUE_UNDEF) {\
    BAD_FORMAT;\
  } 

int main(int argc, char *argv[]) {
    int   c;
    void *rozofs_export_p;
    char *root_path=NULL;
    uint64_t   eid = LONG_VALUE_UNDEF;
    char *comp = " ";
    int   expect_comparator = 0;
    char *criteria_string = NULL;
    char  criteria_char = ' ';
    int   date_criteria_is_set = 0;
    check_inode_pf_t date_criteria_cbk;
    char  regex[1024];
    long long usecs;
    struct timeval start;
    struct timeval stop;
        

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

      int option_index = -1;
      c = getopt_long(argc, argv, "acde:ghjno:p:tsuwxAPRSTXZ", long_options, &option_index);

      if (c == -1)
          break;

      switch (c) {

          case 'h':
          case INT_VALUE_FOR_STRING_help:
              usage(NULL);
              break;
          case 'a':
          case INT_VALUE_FOR_STRING_all:
              NEW_OPTION_CHECKS();
              scan_all_tracking_files = 1; // scan all tracking files
              break;
          case INT_VALUE_FOR_STRING_config:
              NEW_OPTION_CHECKS();
              configFileName = optarg;
              break;			  	                        
          case 'e':
          case INT_VALUE_FOR_STRING_eid:
              NEW_OPTION_CHECKS();
              SCAN_U64(eid)
              break;                  
          case INT_VALUE_FOR_STRING_Ux:   
            NEW_OPTION_CHECKS();
            if (Ux == 0) {
              usage("--Ux and --Unx are incompatible");
            }  
            Ux = 1; 
            break;
          case INT_VALUE_FOR_STRING_Unx:  
            NEW_OPTION_CHECKS();
            if (Ux == 1) {
              usage("--Ux and --Unx are incompatible");
            }  
            Ux = 0; 
            break;
          case INT_VALUE_FOR_STRING_Ur:   
            NEW_OPTION_CHECKS();
            if (Ur == 0) {
              usage("--Ur and --Unr are incompatible");
            }  
            Ur = 1; 
            break;
          case INT_VALUE_FOR_STRING_Unr:  
            NEW_OPTION_CHECKS();
            if (Ur == 1) {
              usage("--Ur and --Unr are incompatible");
            }  
            Ur = 0; 
            break;
          case INT_VALUE_FOR_STRING_Uw:   
            NEW_OPTION_CHECKS();
            if (Uw == 0) {
              usage("--Uw and --Unw are incompatible");
            }             
            Uw = 1; 
            break;
          case INT_VALUE_FOR_STRING_Unw: 
            NEW_OPTION_CHECKS();
            if (Uw == 1) {
              usage("--Uw and --Unw are incompatible");
            }             
            Uw = 0; 
            break;
          case INT_VALUE_FOR_STRING_Gx:  
            NEW_OPTION_CHECKS();
            if (Gx == 0) {
              usage("--Gx and --Gnx are incompatible");
            }              
            Gx = 1;
            break;
          case INT_VALUE_FOR_STRING_Gnx: 
            NEW_OPTION_CHECKS();
            if (Gx == 1) {
              usage("--Gx and --Gnx are incompatible");
            }              
            Gx = 0; 
            break;
          case INT_VALUE_FOR_STRING_Gr: 
            NEW_OPTION_CHECKS();
            if (Gr == 0) {
              usage("--Gr and --Gnr are incompatible");
            }                          
            Gr = 1; 
            break;
          case INT_VALUE_FOR_STRING_Gnr:
            NEW_OPTION_CHECKS();
            if (Gr == 1) {
              usage("--Gr and --Gnr are incompatible");
            }             
            Gr = 0; 
            break;
          case INT_VALUE_FOR_STRING_Gw: 
            NEW_OPTION_CHECKS();
            if (Gw == 0) {
              usage("--Gw and --Gnw are incompatible");
            }             
            Gw = 1; 
            break;
          case INT_VALUE_FOR_STRING_Gnw:
            NEW_OPTION_CHECKS();
            if (Gw == 1) {
              usage("--Gw and --Gnw are incompatible");
            }             
            Gw = 0; 
            break;
          case INT_VALUE_FOR_STRING_Ox: 
            NEW_OPTION_CHECKS();
            if (Ox == 0) {
              usage("--Ox and --Onx are incompatible");
            }             
            Ox = 1;
            break;
          case INT_VALUE_FOR_STRING_Onx:
            NEW_OPTION_CHECKS();
            if (Ox == 1) {
              usage("--Ox and --Onx are incompatible");
            }              
            Ox = 0; 
            break;
          case INT_VALUE_FOR_STRING_Or: 
            NEW_OPTION_CHECKS();
            if (Or == 0) {
              usage("--Or and --Onr are incompatible");
            }              
            Or = 1; 
            break;
          case INT_VALUE_FOR_STRING_Onr: 
            NEW_OPTION_CHECKS();
            if (Or == 1) {
              usage("--Or and --Onr are incompatible");
            }              
            Or = 0;
            break;
          case INT_VALUE_FOR_STRING_Ow: 
            NEW_OPTION_CHECKS();
            if (Ow == 0) {
              usage("--Ow and --Onw are incompatible");
            }              
            Ow = 1; 
            break;
          case INT_VALUE_FOR_STRING_Onw: 
            NEW_OPTION_CHECKS();
            if (Ow == 1) {
              usage("--Ow and --Onw are incompatible");
            }              
            Ow = 0; 
            break;                           
          case 'o':
          case INT_VALUE_FOR_STRING_out:
            NEW_OPTION_CHECKS();
            if (optarg==0) {
              usage("No output format defined");     
            }
            if (rozofs_parse_output_format(optarg)!=0) {
              usage("Bad output format \"%s\"",optarg);     
            }
            break;
          case INT_VALUE_FOR_STRING_hcr8:          
          case INT_VALUE_FOR_STRING_cr8:          
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_hcr8);
              date_criteria_is_set = 1;
              break;
          case INT_VALUE_FOR_STRING_scr8:                    
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_scr8);
              date_criteria_is_set = 1;
              break;
          case INT_VALUE_FOR_STRING_mod:          
          case INT_VALUE_FOR_STRING_hmod:          
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_hmod);
              date_criteria_is_set = 1;
              break;
          case INT_VALUE_FOR_STRING_smod:                    
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_smod);
              date_criteria_is_set = 1;
              break;
          case INT_VALUE_FOR_STRING_hctime:
          case INT_VALUE_FOR_STRING_ctime:
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_hctime);
              date_criteria_is_set = 1;
              break;
          case INT_VALUE_FOR_STRING_sctime:
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_sctime);
              date_criteria_is_set = 1;
              break;
          case INT_VALUE_FOR_STRING_hupdate:
          case INT_VALUE_FOR_STRING_update:          
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_hupdate);
              date_criteria_is_set = 1;
              break;
          case INT_VALUE_FOR_STRING_supdate:
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_supdate);
              date_criteria_is_set = 1;
              break;
           case 's':
           case INT_VALUE_FOR_STRING_size:
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_size);
              break;
          case 'g':
          case INT_VALUE_FOR_STRING_gid:
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_gid);
              break;
          case 'u':
          case INT_VALUE_FOR_STRING_uid:
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_uid);
              break;                  
          case 'c':
          case INT_VALUE_FOR_STRING_cid:
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_cid);
              break;    
          case 'z':
          case INT_VALUE_FOR_STRING_sid:          
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_sid);
              break;    
          case 'P':
          case INT_VALUE_FOR_STRING_project:
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_project);
              break;                                    
          case INT_VALUE_FOR_STRING_link:          
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_link);
              break;                
          case INT_VALUE_FOR_STRING_children:          
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_children);
              break;                
          case INT_VALUE_FOR_STRING_deleted:
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_deleted);
              break;                
          case INT_VALUE_FOR_STRING_pfid:
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_pfid);
              break;                
          case 'n':
          case INT_VALUE_FOR_STRING_name:
              NEW_COMPARISON_CHECKS(INT_VALUE_FOR_STRING_name);
              break;                
          case 'x':   
          case INT_VALUE_FOR_STRING_xattr:
              NEW_OPTION_CHECKS();
              has_xattr = 1;
              break;   
          case 'X':
          case INT_VALUE_FOR_STRING_noxattr:
              NEW_OPTION_CHECKS();
              has_xattr = 0;
              break;                     
          case 'S':
          case INT_VALUE_FOR_STRING_slink:          
              NEW_OPTION_CHECKS();
              exclude_symlink = 0;
              break;                     
          case 'R':
          case INT_VALUE_FOR_STRING_noreg:          
              NEW_OPTION_CHECKS();
              exclude_regular = 1;
              if (only_trash) {
                usage("only trash (-t) and exclude trash (-T) are incompatible");     
              }
              break;                     
          case 't':
          case INT_VALUE_FOR_STRING_trash:
              NEW_OPTION_CHECKS();
              only_trash = 1;
              if (exclude_trash) {
                usage("only trash (-t) and exclude trash (-T) are incompatible");     
              }
              break;                     
          case 'j':
          case INT_VALUE_FOR_STRING_junk:
              NEW_OPTION_CHECKS();
              only_junk = 1;
              if (only_trash) {
                usage("only trash (-t) and junk files (-j) are incompatible");     
              }
              if (search_dir) {
                usage("Directory (-d) and junk files (-j) are incompatible");     
              }
              break;  
          case 'w':
          case INT_VALUE_FOR_STRING_wrerror:
              NEW_OPTION_CHECKS();
              only_wrerror = 1;
              if (search_dir) {
                usage("Directory (-d) and write errror files (-w) are incompatible");     
              }
              break;                                   
           case 'T':
           case INT_VALUE_FOR_STRING_notrash:
              NEW_OPTION_CHECKS();
              exclude_trash = 1;
              break;                     
          /*
          ** Lower or equal
          */              
          case INT_VALUE_FOR_STRING_le:  
              if (expect_comparator == 0) {
                 usage("Got unexpected --le");
              }           
              expect_comparator = 0;
              comp = "--le";
              switch (scan_criteria) {              
                case INT_VALUE_FOR_STRING_hcr8:
                  SCAN_DATE(cr8_lower)   
                  break;                 
                case INT_VALUE_FOR_STRING_scr8:
                  SCAN_U64(cr8_lower)    
                  break;                                
                case INT_VALUE_FOR_STRING_hmod:
                  SCAN_U64(mod_lower)    
                  break;                                
                case INT_VALUE_FOR_STRING_smod:
                  SCAN_U64(mod_lower)    
                  break; 
                case INT_VALUE_FOR_STRING_hctime:
                  SCAN_DATE(ctime_lower)    
                  break; 
                case INT_VALUE_FOR_STRING_sctime:
                  SCAN_U64(ctime_lower)    
                  break; 
                case INT_VALUE_FOR_STRING_hupdate:
                  SCAN_DATE(update_lower)    
                  break;                     
                case INT_VALUE_FOR_STRING_supdate:
                  SCAN_U64(update_lower)    
                  break;                     
                case INT_VALUE_FOR_STRING_size:
                  SCAN_SIZE(size_lower)    
                  break;                    
                case INT_VALUE_FOR_STRING_link:
                  SCAN_U64(nlink_lower)    
                  break;                                    
                case INT_VALUE_FOR_STRING_children:
                  SCAN_U64(children_lower)    
                  break;                                   
                case INT_VALUE_FOR_STRING_deleted:
                  SCAN_U64(deleted_lower)    
                  break;                                                                     
                case INT_VALUE_FOR_STRING_gid:          
                case INT_VALUE_FOR_STRING_uid:
                case INT_VALUE_FOR_STRING_cid:
                case INT_VALUE_FOR_STRING_sid:
                case INT_VALUE_FOR_STRING_pfid:
                case INT_VALUE_FOR_STRING_name:
                case INT_VALUE_FOR_STRING_project:
                  if (criteria_string) usage("No %s comparison for --%s",comp,criteria_string);  
                  else                 usage("No %s comparison for -c",comp,criteria_char);    
                  break;
                  
                default:
                  usage("No criteria defined prior to %s",comp);     
              }
              break;
          /*
          ** Lower strictly
          */              
          case INT_VALUE_FOR_STRING_lt:
              if (expect_comparator == 0) {
                 usage("Got unexpected --lt");
              }           
              expect_comparator = 0;
              comp = "--lt";
              switch (scan_criteria) {              
                case INT_VALUE_FOR_STRING_hcr8:
                  SCAN_DATE(cr8_lower) 
                  cr8_lower--;   
                  break;                     
                case INT_VALUE_FOR_STRING_scr8:
                  SCAN_U64(cr8_lower) 
                  cr8_lower--;   
                  break;                                                             
                case INT_VALUE_FOR_STRING_hmod:
                  SCAN_DATE(mod_lower)
                  mod_lower--;    
                  break;                               
                case INT_VALUE_FOR_STRING_smod:
                  SCAN_U64(mod_lower)
                  mod_lower--;    
                  break;                    
                case INT_VALUE_FOR_STRING_hctime:
                  SCAN_DATE(ctime_lower)
                  ctime_lower--;    
                  break;                    
                case INT_VALUE_FOR_STRING_sctime:
                  SCAN_U64(ctime_lower)
                  ctime_lower--;    
                  break;  
                case INT_VALUE_FOR_STRING_hupdate:
                  SCAN_DATE(update_lower)
                  update_lower--;    
                  break;                    
                case INT_VALUE_FOR_STRING_supdate:
                  SCAN_U64(update_lower)
                  update_lower--;    
                  break;                    
                case INT_VALUE_FOR_STRING_size:
                  SCAN_SIZE(size_lower) 
                  if (size_lower==0) {
                    BAD_FORMAT;     
                  } 
                  size_lower--;   
                  break;                                    
                case INT_VALUE_FOR_STRING_link:
                  SCAN_U64(nlink_lower) 
                  if (nlink_lower==0) {
                    BAD_FORMAT;     
                  } 
                  nlink_lower--;   
                  break;                                     
                case INT_VALUE_FOR_STRING_children:
                  SCAN_U64(children_lower)   
                  if (children_lower==0) {
                    BAD_FORMAT;     
                  } 
                  children_lower--;   
                  break;                                     
                case INT_VALUE_FOR_STRING_deleted:
                  SCAN_U64(deleted_lower)   
                  if (deleted_lower==0) {
                    BAD_FORMAT;     
                  } 
                  deleted_lower--;   
                  break;  
                                      
                case INT_VALUE_FOR_STRING_gid:          
                case INT_VALUE_FOR_STRING_uid:
                case INT_VALUE_FOR_STRING_pfid:
                case INT_VALUE_FOR_STRING_name:
                  if (criteria_string) usage("No %s comparison for --%s",comp,criteria_string);  
                  else                 usage("No %s comparison for -c",comp,criteria_char);    
                  break; 
                                                
                default:
                  usage("No criteria defined prior to %s",comp);     
              }
              break;
          /*
          ** Greater or equal
          */  
          case INT_VALUE_FOR_STRING_ge:
              if (expect_comparator == 0) {
                 usage("Got unexpected --ge");
              }           
              expect_comparator = 0;          
              comp = "--ge";         
              switch (scan_criteria) {              
                case INT_VALUE_FOR_STRING_hcr8:
                  SCAN_DATE(cr8_bigger)    
                  break;                    
                case INT_VALUE_FOR_STRING_scr8:
                  SCAN_U64(cr8_bigger)    
                  break;                                 
                case INT_VALUE_FOR_STRING_hmod:
                  SCAN_DATE(mod_bigger)    
                  break;                                
                case INT_VALUE_FOR_STRING_smod:
                  SCAN_U64(mod_bigger)    
                  break; 
                case INT_VALUE_FOR_STRING_hctime:
                  SCAN_DATE(ctime_bigger)    
                  break;                   
                case INT_VALUE_FOR_STRING_sctime:
                  SCAN_U64(ctime_bigger)    
                  break;                    
                case INT_VALUE_FOR_STRING_hupdate:
                  SCAN_DATE(update_bigger)    
                  break;                  
                case INT_VALUE_FOR_STRING_supdate:
                  SCAN_U64(update_bigger)    
                  break;                                   
                case INT_VALUE_FOR_STRING_size:
                  SCAN_SIZE(size_bigger) 
                  break; 
                case INT_VALUE_FOR_STRING_link:
                  SCAN_U64(nlink_bigger) 
                  break;                   
                case INT_VALUE_FOR_STRING_children:
                  SCAN_U64(children_bigger) 
                  break;
                case INT_VALUE_FOR_STRING_deleted:
                  SCAN_U64(deleted_bigger) 
                  break;                  
                case INT_VALUE_FOR_STRING_name:
                  fname_bigger = optarg;
                  break;
                     
                case INT_VALUE_FOR_STRING_gid:          
                case INT_VALUE_FOR_STRING_uid:
                case INT_VALUE_FOR_STRING_cid:                
                case INT_VALUE_FOR_STRING_sid:                
                case INT_VALUE_FOR_STRING_pfid:
                case INT_VALUE_FOR_STRING_project:
                  if (criteria_string) usage("No %s comparison for --%s",comp,criteria_string);  
                  else                 usage("No %s comparison for -c",comp,criteria_char);    
                  break;                               
                                       
                default:
                  usage("No criteria defined prior to %s",comp);     
              }
              break;
          /*
          ** Greater strictly
          */         
          case INT_VALUE_FOR_STRING_gt:                  
              if (expect_comparator == 0) {
                 usage("Got unexpected --gt");
              }           
              expect_comparator = 0;          
              comp = "--gt";         
              switch (scan_criteria) {              
                case INT_VALUE_FOR_STRING_hcr8:
                  SCAN_DATE(cr8_bigger)  
                  cr8_bigger++; 
                  break;                      
                case INT_VALUE_FOR_STRING_scr8:
                  SCAN_U64(cr8_bigger)  
                  cr8_bigger++; 
                  break;                                
                case INT_VALUE_FOR_STRING_hmod:
                  SCAN_DATE(mod_bigger) 
                  mod_bigger++;  
                  break;                  
                case INT_VALUE_FOR_STRING_smod:
                  SCAN_U64(mod_bigger) 
                  mod_bigger++;  
                  break;
                case INT_VALUE_FOR_STRING_hctime:
                  SCAN_DATE(ctime_bigger) 
                  ctime_bigger++;  
                  break;
                case INT_VALUE_FOR_STRING_sctime:
                  SCAN_U64(ctime_bigger) 
                  ctime_bigger++;  
                  break;
                 case INT_VALUE_FOR_STRING_hupdate:
                  SCAN_DATE(update_bigger) 
                  update_bigger++;  
                  break;                  
                 case INT_VALUE_FOR_STRING_supdate:
                  SCAN_U64(update_bigger) 
                  update_bigger++;  
                  break;                  
                case INT_VALUE_FOR_STRING_size:
                  SCAN_SIZE(size_bigger) 
                  size_bigger++;
                  break;  
                case INT_VALUE_FOR_STRING_link:
                  SCAN_U64(nlink_bigger) 
                  nlink_bigger++;
                  break;                    
                case INT_VALUE_FOR_STRING_children:
                  SCAN_U64(children_bigger) 
                  children_bigger++;
                  break;                    
                case INT_VALUE_FOR_STRING_deleted:
                  SCAN_U64(deleted_bigger) 
                  deleted_bigger++;
                  break;  
                  
                case INT_VALUE_FOR_STRING_gid:          
                case INT_VALUE_FOR_STRING_uid:
                case INT_VALUE_FOR_STRING_cid:                
                case INT_VALUE_FOR_STRING_sid:                
                case INT_VALUE_FOR_STRING_pfid:
                case INT_VALUE_FOR_STRING_name:
                case INT_VALUE_FOR_STRING_project:
                  if (criteria_string) usage("No %s comparison for --%s",comp,criteria_string);  
                  else                 usage("No %s comparison for -c",comp,criteria_char);    
                  break;                               
                                      
                default:
                  usage("No criteria defined prior to %s",comp);     
              }
              break; 
          /*
          ** Equality
          */    
          case INT_VALUE_FOR_STRING_eq:
              if (expect_comparator == 0) {
                 usage("Got unexpected --eq");
              }           
              expect_comparator = 0;          
              comp = "--eq";        
              switch (scan_criteria) {              
                case INT_VALUE_FOR_STRING_hcr8:
                  SCAN_DATE(cr8_equal) 
                  break;                      
                case INT_VALUE_FOR_STRING_scr8:
                  SCAN_U64(cr8_equal) 
                  break;                                
                case INT_VALUE_FOR_STRING_hmod:
                  SCAN_DATE(mod_equal)                   
                  break;                              
                case INT_VALUE_FOR_STRING_smod:
                  SCAN_U64(mod_equal)                   
                  break;                    
                case INT_VALUE_FOR_STRING_hctime:
                  SCAN_DATE(ctime_equal)                   
                  break;                    
                case INT_VALUE_FOR_STRING_sctime:
                  SCAN_U64(ctime_equal)                   
                  break;                     
                case INT_VALUE_FOR_STRING_hupdate:
                  SCAN_DATE(update_equal)                   
                  break;  
                case INT_VALUE_FOR_STRING_supdate:
                  SCAN_U64(update_equal)                   
                  break;  
                case INT_VALUE_FOR_STRING_size:
                  SCAN_SIZE(size_equal)
                  break; 
                case INT_VALUE_FOR_STRING_gid:
                  SCAN_U64(gid_equal)   
                  break;                  
                case INT_VALUE_FOR_STRING_link:
                  SCAN_U64(nlink_equal)   
                  break;                                                                      
                case INT_VALUE_FOR_STRING_children:
                  SCAN_U64(children_equal)   
                  break;                  
                case INT_VALUE_FOR_STRING_deleted:
                  SCAN_U64(deleted_equal)   
                  break;                                                                       
                case INT_VALUE_FOR_STRING_uid:
                  SCAN_U64(uid_equal)   
                  break;                     
                case INT_VALUE_FOR_STRING_cid:
                  SCAN_U64(cid_equal)   
                  break;                  
                case INT_VALUE_FOR_STRING_sid:
                  SCAN_U64(sid_equal)   
                  break;                                    
                case INT_VALUE_FOR_STRING_project:
                  SCAN_U64(project_equal)   
                  break;                  
                case INT_VALUE_FOR_STRING_pfid:
                  if (rozofs_uuid_parse(optarg, pfid_equal)!=0) {
                    BAD_FORMAT;     
                  }   
                  break;                                                                         
                  
                case INT_VALUE_FOR_STRING_name:
                  fname_equal = optarg;
                  break;                                                                         
                                                                         
                                                                         
                default:
                  usage("No criteria defined prior to %s",comp);     
              }
              break; 
          /*
          ** Regex
          */    
          case INT_VALUE_FOR_STRING_regex:          
              if (expect_comparator == 0) {
                 usage("Got unexpected --regex");
              }           
              expect_comparator = 0;          
              comp = "--regex";        
              switch (scan_criteria) {
              
                case INT_VALUE_FOR_STRING_name:
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
          case INT_VALUE_FOR_STRING_ne:
             if (expect_comparator == 0) {
                usage("Got unexpected --ne");
             }           
             expect_comparator = 0;          
              comp = "--ne";        
              switch (scan_criteria) {              
                case INT_VALUE_FOR_STRING_hcr8:
                  SCAN_DATE(cr8_diff) 
                  break;                   
                case INT_VALUE_FOR_STRING_scr8:
                  SCAN_U64(cr8_diff) 
                  break;                                
                case INT_VALUE_FOR_STRING_hmod:
                  SCAN_DATE(mod_diff)                   
                  break;                                
                case INT_VALUE_FOR_STRING_smod:
                  SCAN_U64(mod_diff)                   
                  break;                   
                case INT_VALUE_FOR_STRING_hctime:
                  SCAN_DATE(ctime_diff)                   
                  break;                   
                case INT_VALUE_FOR_STRING_sctime:
                  SCAN_U64(ctime_diff)                   
                  break; 
                case INT_VALUE_FOR_STRING_hupdate:
                  SCAN_DATE(update_diff)                   
                  break; 
                case INT_VALUE_FOR_STRING_supdate:
                  SCAN_U64(update_diff)                   
                  break;                    
                case INT_VALUE_FOR_STRING_size:
                  SCAN_SIZE(size_diff)
                  break; 
                case INT_VALUE_FOR_STRING_link:
                  SCAN_U64(nlink_diff)
                  break;                    
                case INT_VALUE_FOR_STRING_children:
                  SCAN_U64(children_diff)
                  break;                    
                case INT_VALUE_FOR_STRING_deleted:
                  SCAN_U64(deleted_diff)
                  break;                   
                case INT_VALUE_FOR_STRING_gid:
                  SCAN_U64(gid_diff)   
                  break;                                   
                case INT_VALUE_FOR_STRING_uid:
                  SCAN_U64(uid_diff)   
                  break;                     
                case INT_VALUE_FOR_STRING_cid:
                  SCAN_U64(cid_diff)   
                  break;                    
                case INT_VALUE_FOR_STRING_sid:
                  SCAN_U64(sid_diff)   
                  break;                    
                case INT_VALUE_FOR_STRING_project:
                  SCAN_U64(project_diff)   
                  break;
                                    
                case INT_VALUE_FOR_STRING_pfid:
                case INT_VALUE_FOR_STRING_name:
                  if (criteria_string) usage("No %s comparison for --%s",comp,criteria_string);  
                  else                 usage("No %s comparison for -c",comp,criteria_char);    
                  break;                                                   
                                                                         
                default:
                  usage("No criteria defined prior to %s",comp);     
              }
              break;                
          case 'd':
          case INT_VALUE_FOR_STRING_dir:
              NEW_OPTION_CHECKS();
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
  if (eid!=LONG_VALUE_UNDEF) {
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
    export_config = get_export_config(eid);
    if (export_config==NULL) {
      usage("eid %d is not configured",eid);       
    }
    root_path = export_config->root;
  }

  if (root_path == NULL) 
  {
    usage("Missing export identifier (-e)");
  }
  
  /*
  ** sid_equal can only be set with cid equal
  */
  if ((sid_equal != LONG_VALUE_UNDEF) && (cid_equal==LONG_VALUE_UNDEF)) {
    usage("--sid --eq can only be set along with --cid --eq to find out files having this cid and sid in its distribution.");
  }
  /*
  ** sid_diff can only be set with cid_equal or cid_diff
  */
  if ((sid_diff != LONG_VALUE_UNDEF) && (cid_equal==LONG_VALUE_UNDEF) && (cid_diff==LONG_VALUE_UNDEF)) {
    usage("--sid --ne can only be set along with\n   --cid --eq to find out files having this cid but not this sid in its distribution\nor --cid --ne to find out files not having this cid and sid in its distribution.");
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
  rz_set_verbose_mode(0);
  
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
