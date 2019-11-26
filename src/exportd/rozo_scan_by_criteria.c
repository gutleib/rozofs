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
#include <rozofs/rozofs_srv.h>
#include "export.h"
#include "rozo_inode_lib.h"
#include "exp_cache.h"
#include "mdirent.h"
#include "xattr_main.h"
#include <rozofs/common/acl.h>
#include <rozofs/common/posix_acl_attr.h>

#define LONG_VALUE_UNDEF  -1LLU

int                      rozofs_no_site_file = 0;
lv2_cache_t              cache;
mdirents_name_entry_t    bufferName;
export_config_t        * export_config = NULL;
int                      layout=LAYOUT_4_6_8;
uint8_t                  rozofs_inverse,rozofs_forward,rozofs_safe;

#define ROZOFS_SCAN_XATTR_BUFFER_SIZE 8192
char rozofs_scan_xattr_list_buffer[ROZOFS_SCAN_XATTR_BUFFER_SIZE];
char rozofs_scan_xattr_value_buffer[ROZOFS_SCAN_XATTR_BUFFER_SIZE];


#define ROZO_SCAN_MAX_XATTR      128
int     rozo_scan_xattr_nb  = 0;
char *  rozo_scan_xattr_table[ROZO_SCAN_MAX_XATTR];
/*
** lv2 structure to read extended attribute of an inode
*/
lv2_entry_t      xattr_lv2;
struct dentry    xattr_entry;

fid_t            this_fid = {0};
int              just_this_fid = 0;


/*____________________________________________________________
** Global variables for argument populated while parsing command
*/
char                   * configFileName = EXPORTD_DEFAULT_CONFIG;  
uint64_t                 eid            = LONG_VALUE_UNDEF;

/*
**_____________________________________________
** Verbose mode
** 1: print argument parsing
** 2: print argument parsing and scan execution
*/
int verbose = 0;
#define VERBOSE(fmt, ...)  {if (verbose) printf(fmt, ##__VA_ARGS__);}
#define VVERBOSE(fmt, ...) {if (verbose>1) printf(fmt, ##__VA_ARGS__);}


/*
**______________________________________________________________________________
** Input arguments are parsed one by one. But one argument may contain several
** rozo_scan keywords and values without ' '. One has to memorize during the
** argument parsing:
** - which is the crrently parsed argument.
** - whether this argument is completly parsed or not.
** - which is the current offset of the next part to parse in the current argument
*/
static int rozofs_scan_current_arg_idx = 0;
static int rozofs_scan_parse_same_arg = 0;
static int rozofs_scan_current_arg_char2skip = 0;  


/*__________________________________________________________________
**
** enumeration of input command keywords
**_____________________________________________________________________________________
** -- W A R N I N G -- W A R N I N G -- W A R N I N G -- W A R N I N G -- W A R N I N G
**
** In case of any change within rozofs_scan_keyw_e please run the following command
**
**     ../../tools/enum2String.py -n rozofs_scan_keyw_e -f rozo_scan_by_criteria.c -c 17
**
** under src/export directory to regenerate rozofs_scan_keyw_e2String.h accordingly
**
** -- W A R N I N G -- W A R N I N G -- W A R N I N G -- W A R N I N G -- W A R N I N G
**_____________________________________________________________________________________

*/
typedef enum _rozofs_scan_keyw_e {
  
  
  rozofs_scan_keyw_criteria_min=0,
  
  rozofs_scan_keyw_criteria_has_write_error,
  rozofs_scan_keyw_criteria_is_in_trash,
  rozofs_scan_keyw_criteria_not_in_trash,
  rozofs_scan_keyw_criteria_has_xattr,
  rozofs_scan_keyw_criteria_has_no_xattr,
  rozofs_scan_keyw_criteria_priv_user_x,
  rozofs_scan_keyw_criteria_priv_user_not_x,
  rozofs_scan_keyw_criteria_priv_user_w,
  rozofs_scan_keyw_criteria_priv_user_not_w,
  rozofs_scan_keyw_criteria_priv_user_r,
  rozofs_scan_keyw_criteria_priv_user_not_r,
  rozofs_scan_keyw_criteria_priv_group_x,
  rozofs_scan_keyw_criteria_priv_group_not_x,
  rozofs_scan_keyw_criteria_priv_group_w,
  rozofs_scan_keyw_criteria_priv_group_not_w,
  rozofs_scan_keyw_criteria_priv_group_r,
  rozofs_scan_keyw_criteria_priv_group_not_r,
  rozofs_scan_keyw_criteria_priv_other_x,
  rozofs_scan_keyw_criteria_priv_other_not_x,
  rozofs_scan_keyw_criteria_priv_other_w,
  rozofs_scan_keyw_criteria_priv_other_not_w,
  rozofs_scan_keyw_criteria_priv_other_r,
  rozofs_scan_keyw_criteria_priv_other_not_r,
  rozofs_scan_keyw_criteria_is_hybrid,
  rozofs_scan_keyw_criteria_is_not_hybrid,
  rozofs_scan_keyw_criteria_is_aging,
  rozofs_scan_keyw_criteria_is_not_aging,
  
  rozofs_scan_keyw_criteria_max,


  
  rozofs_scan_keyw_field_min,  
  
  rozofs_scan_keyw_field_fid,
  rozofs_scan_keyw_field_pfid,
  rozofs_scan_keyw_field_fname,
  rozofs_scan_keyw_field_cr8time,
  rozofs_scan_keyw_field_mtime,  
  rozofs_scan_keyw_field_ctime,  
  rozofs_scan_keyw_field_atime,  
  rozofs_scan_keyw_field_update_time,
  rozofs_scan_keyw_field_size,
  rozofs_scan_keyw_field_uid,
  rozofs_scan_keyw_field_gid,
  rozofs_scan_keyw_field_slave,
  rozofs_scan_keyw_field_nlink,
  rozofs_scan_keyw_field_deleted,
  rozofs_scan_keyw_field_children,
  rozofs_scan_keyw_field_project,
  rozofs_scan_keyw_field_parent,  
  rozofs_scan_keyw_field_cid,  
  rozofs_scan_keyw_field_sid,  
  rozofs_scan_keyw_field_sidrange,  
  rozofs_scan_keyw_field_xname,  
  
  rozofs_scan_keyw_field_max,


  
  rozofs_scan_keyw_comparator_min,
  
  rozofs_scan_keyw_comparator_lt,  
  rozofs_scan_keyw_comparator_le,
  rozofs_scan_keyw_comparator_eq,
  rozofs_scan_keyw_comparator_ge,
  rozofs_scan_keyw_comparator_gt,
  rozofs_scan_keyw_comparator_ne,
  rozofs_scan_keyw_comparator_regex,

  rozofs_scan_keyw_comparator_max,


  
  rozofs_scan_keyw_separator_open,  // ( { [
  rozofs_scan_keyw_separator_close, // ) ] }

  
  rozofs_scan_keyw_operator_or,    // or
  rozofs_scan_keyw_operator_and,   // and

  
  rozofs_scan_keyw_option_skipdate,  
  rozofs_scan_keyw_option_help,
  rozofs_scan_keyw_option_verbose,
  rozofs_scan_keyw_option_vverbose,


  rozofs_scan_keyw_scope_junk,
  rozofs_scan_keyw_scope_dir,
  rozofs_scan_keyw_scope_slink,


  rozofs_scan_keyw_argument_output,
  rozofs_scan_keyw_argument_eid,
  rozofs_scan_keyw_argument_config,

  
  rozofs_scan_keyw_input_end,  
  rozofs_scan_keyw_input_error

  
} rozofs_scan_keyw_e;
/*
**_____________________________________________________________________________________
** -- W A R N I N G -- W A R N I N G -- W A R N I N G -- W A R N I N G -- W A R N I N G
**
** In case of any change within rozofs_scan_keyw_e please run the following command
**
**     ../../tools/enum2String.py -n rozofs_scan_keyw_e -f rozo_scan_by_criteria.c -c 17
**
** under src/export directory to regenerate rozofs_scan_keyw_e2String.h accordingly
**
** -- W A R N I N G -- W A R N I N G -- W A R N I N G -- W A R N I N G -- W A R N I N G
**_____________________________________________________________________________________
*/
#include "rozofs_scan_keyw_e2String.h"



/*________________________
** Scan scope variable
*/
typedef enum _rozofs_scan_scope_e {
  rozofs_scan_scope_regular_file,
  rozofs_scan_scope_symbolic_link,
  rozofs_scan_scope_directory,
  rozofs_scan_scope_junk_file
} rozofs_scan_scope_e;
#include "rozofs_scan_scope_e2String.h"
/*
** Default scan scope is regular file
*/
rozofs_scan_scope_e rozofs_scan_scope = rozofs_scan_scope_regular_file; 



/*___________________________
** Global statistics
*/
uint64_t nb_scanned_entries = 0;
uint64_t nb_scanned_entries_in_scope = 0;
uint64_t nb_matched_entries = 0;
uint64_t sum_file_size = 0;
uint64_t sum_file_blocks = 0;
uint64_t sum_sub_dir               = 0;
uint64_t sum_sub_files             = 0;
uint64_t nb_checked_tracking_files = 0;
uint64_t nb_skipped_tracking_files = 0;


/*_________________________________
** Output variables.
*/
uint64_t max_display = -1;
int      entry_per_line = 1;
int      cur_entry_per_line = 0;
/*
** Name output format
*/
typedef enum _name_format_e {
  name_format_full = 0,
  name_format_relative,
  name_format_fid,
  name_format_none,
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
int display_mtime = 0;
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
int display_stat = 0;
int first_entry = 1;
#define DO_DISPLAY           1
#define DO_HUMAN_DISPLAY     2

#define IF_DISPLAY(x)  if ((x!=0) || (display_all!=0))
#define IF_DISPLAY_HUMAN(x) if ((x==DO_HUMAN_DISPLAY) || (display_all==DO_HUMAN_DISPLAY))

char separator[128] = {0};

int first_array_element = 1;

#define SUBARRAY_NEW_LINE()  {\
  if (display_json) {\
    pDisplay += rozofs_string_append(pDisplay,",\n             ");\
  }\
}

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
#define NEW_QUOTED_NAME_NEW_LINE(field) {\
  if (display_json) {\
    SUBARRAY_NEW_LINE();\
    pDisplay += rozofs_string_append(pDisplay," \""#field"\" : \"");\
  }\
}  
#define FIRST_QUOTED_NAME(field) {\
  if (display_json) {\
    pDisplay += rozofs_string_append(pDisplay," \""#field"\" : \"");\
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


/*
** Hybrid or not hybrid
*/
#define HYBRID        1
#define NOHYBRID      0
uint64_t hybrid = LONG_VALUE_UNDEF;


/*
** Whether to scan all tracking files or only those whose
** creation and modification time match the research date
** criteria
*/
int skip_tracking_file = 0; // Every tracking file must be scanned

int highlight;
#define HIGHLIGHT(x) if (x) {highlight = 1; pDisplay += rozofs_string_append(pDisplay,ROZOFS_COLOR_CYAN);}else{highlight = 0;}
#define NORMAL()     if(highlight) {pDisplay += rozofs_string_append(pDisplay,ROZOFS_COLOR_NONE);}


typedef enum _rozofs_scan_value_type_e {
  rozofs_scan_value_type_u64,
  rozofs_scan_value_type_2u32,
  rozofs_scan_value_type_2u32_range,
  rozofs_scan_value_type_string,
  rozofs_scan_value_type_fid,
  rozofs_scan_value_type_regex,
} rozofs_scan_value_type_e;

typedef struct _rozofs_range_t {
  uint64_t min;
  uint64_t max;
} rozofs_range_t;

typedef union _rozofs_scan_field_value_u {
  uint32_t     u32[2];
  uint64_t     u64;
  char       * string;
  fid_t        fid;
  pcre       * regex;
} rozofs_scan_field_value_u;

typedef enum _rozofs_scan_type_e {  
  rozofs_scan_type_criteria,
  rozofs_scan_type_field,
  rozofs_scan_type_node
} rozofs_scan_type_e;


typedef struct _rozofs_scan_leaf_param_t {
  rozofs_scan_keyw_e                name;
  rozofs_scan_keyw_e                comp;
  rozofs_scan_value_type_e          valType;
  rozofs_range_t                    range;
  rozofs_scan_field_value_u         value;
} rozofs_scan_leaf_param_t;

typedef enum _rozofs_scan_node_ope_e {
  rozofs_scan_node_ope_and,
  rozofs_scan_node_ope_or 
} rozofs_scan_node_ope_e;

#define ROZOFS_SCAN_MAX_NEXT_NODE   64
typedef struct _rozofs_scan_node_param_t {
  int                              ope;
  int                              nbNext;
  struct _rozofs_scan_node_t     * next[ROZOFS_SCAN_MAX_NEXT_NODE];
  struct _rozofs_scan_node_t     * prev;
  int                              nb;
} rozofs_scan_node_param_t;

typedef struct _rozofs_scan_node_t {
  rozofs_scan_type_e               type;
  union {
    rozofs_scan_node_param_t       n;
    rozofs_scan_leaf_param_t       l;
  };
} rozofs_scan_node_t;



int                  rozofs_scan_node_counter = 0;

/*
** The upper node. The one that will be evaluate
*/
rozofs_scan_node_t * upNode  = NULL;

static inline int rozo_scan_has_extended_attr(ext_mattr_t * inode_p) {
  if ((inode_p->s.i_state == 0)&&(inode_p->s.i_file_acl == 0)) return 0;
  return 1;
}
/*
**_____________________________________________________
** Prototype of criteria and field evaluation functions
*/
typedef int (* rozofs_scan_eval_one_field_fct) (
               export_t                  * e, 
               void                      * entry, 
               rozofs_scan_keyw_e          field_name,
               rozofs_scan_field_value_u * field_value, 
               rozofs_range_t            * field_range,
               rozofs_scan_keyw_e          comp);

typedef int (* rozofs_scan_eval_one_criteria_fct) (
               void               * e, 
               void               * entry, 
               rozofs_scan_keyw_e   criteria_to_match);     
      
/*
**_____________________________________________________
** Variables to contain the evaluation function addresss
** depending on the scope
*/             
rozofs_scan_eval_one_field_fct        rozofs_scan_eval_one_field    = NULL;
rozofs_scan_eval_one_criteria_fct     rozofs_scan_eval_one_criteria = NULL;


/*
**______________________________________________________________
** Stack to push and pop nodes when a bracket is opened or closed
*/
#define               MAX_NODE_STACK      1024
rozofs_scan_node_t  * nodeStack[MAX_NODE_STACK];
int                   rozofs_scan_node_stack_count = 0;


/*
**______________________________________________________________
** List of leaves describing a date constraint
** They could enable to fasten the search by by-passing some tracking files
*/
rozofs_scan_node_t  * dateField[MAX_NODE_STACK];
int                   rozofs_scan_date_field_count = 0;
/*
**_______________________________________________________________________
** Push a node in the stack
**
** @node         The address of the node to push 
**_______________________________________________________________________
*/
int rozofs_scan_push_node (rozofs_scan_node_t * node) {

  if (rozofs_scan_node_stack_count>=MAX_NODE_STACK) {
    printf("PUSH to a full stack %d/%d\n",rozofs_scan_node_stack_count,MAX_NODE_STACK);
    return -1;
  }
     
  nodeStack[rozofs_scan_node_stack_count] = node;
  rozofs_scan_node_stack_count++;
  return rozofs_scan_node_stack_count;
}
/*
**_______________________________________________________________________
** Pop a node from the stack
**
** @node         The address of the node to push 
**_______________________________________________________________________
*/
rozofs_scan_node_t * rozofs_scan_pop_node () {
  
  if (rozofs_scan_node_stack_count<=0) {
    printf("POP from an empty stack %d\n",rozofs_scan_node_stack_count);
    return NULL;
  }
  rozofs_scan_node_stack_count--;   
  return nodeStack[rozofs_scan_node_stack_count];
}  
                 
/*
**_______________________________________________________________________
** Display syntax and examples
** Eventually display an error message
** and exit
**_______________________________________________________________________
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
  printf("This RozoFS utility displays some attributes of one file/directory known by its FID,\nor scans for files or (exclusive) directories matching several conditions in a RozoFS file system.\nIt by-passes the POSIX interface, and directly accesses the meta-data.\n");
  printf("\n\033[1m\033[4mUSAGE:\033[0m\033[1m\trozo_scan fid=<FID> [OUTPUT]\n");
  printf("\tor\n");
  printf("\trozo_scan [SCOPE] [FILESYSTEM] [OPTIONS] [CONDITION] [OUTPUT]\033[0m\n\n");
  printf("\033[1m\033[4mSCOPE:\033[0m\033[1m\t{dir|slink|junk}\033[0m\n");
  printf("\tWhen SCOPE is omitted only the regular files are scanned.\n");
  printf("\t\033[1md, dir\033[0m\t\tScan directories only.\n");
  printf("\t\033[1mS, slink\033[0m\tScan symbolic links only.\n");
  printf("\t\033[1mj, junk\033[0m\t\tScan junk files currently in the process of data deletion. All meta-data are\n");
  printf("\t\t\talready deleted except fid, size, cid, sid.\n");
  printf("\033[1m\033[4mFILESYSTEM:\033[0m\n");
  printf("\tThe \033[1mFILESYSTEM\033[0m can be omitted when the current path is the RozoFS mountpoint to scan.\n");
  printf("\tElse the targeted RozoFS file system must be provided thanks to its eid value.\n");
  printf("\t\033[1me, eid <eid#> [config <cfg file name>]\033[0m   Export identifier and optionally its configuration file.\n");
  printf("\033[1m\033[4mOPTIONS:\033[0m\n");
  printf("\t\033[1mh, help\033[0m\t\tPrint this message along with examples and exit.\n");
  printf("\t\033[1mv, verbose\033[0m\tPrint input parsing information.\n");
  printf("\t\033[1mvv, vverbose\033[0m\tPrint input parsing information and scanning execution.\n");
  printf("\t\033[1mskipdate\033[0m\tSkip tracking files which creation/modification date tells they can not contain files/directory matching\n");
  printf("\t\t\tthe input scan date criteria. This speeds the scan, but unfortunatly does not work for files imported with tools\n");
  printf("\t\t\tsuch as rsync, since their creation/modification dates are not related to their importation date under RozoFS.\n");
  printf("\t\t\tThey may be sometimes no correlation between the tracking file dates and the file creation/modification dates.\n");
  printf("\033[1m\033[4mCONDITION:\033[0m\n");
  printf("\tThe \033[1mCONDITION\033[0m is a set of elementary statements combined together thanks to and/or operators,\n");
  printf("\tthat files/directories have to match . Brackets should be used to unambiguously express the \033[1mCONDITION\033[0m.\n");
  printf("\t\033[1m[, {, \\(\033[0m\tOpen brackets.\n");
  printf("\t\033[1m], }, \\)\033[0m\tClose brackets.\n");
  printf("\t\033[1mand\033[0m\t\tAnd operator.\n");
  printf("\t\033[1mor\033[0m\t\tOr operator.\n");
  printf("\tAn elementary statement is either a \033[1mCRITERIA\033[0m or a \033[1mFIELD\033[0m comparison the file/directory has to match\n");
  printf("\033[1m  \033[4mCRITERIA:\033[0m\n");
  printf("\t\033[1mxattr\033[0m\t\tFile/directory must have extended attribute.\n");
  printf("\t\033[1mnoxattr\033[0m\t\tFile/directory must not have extended attribute.\n");    
  printf("\t\033[1mt, trash\033[0m\tFile/directory must be in the trash (deleted but not yet junked).\n");
  printf("\t\033[1mT, notrash\033[0m\tFile/directory must not be in the trash.\n");
  printf("\t\033[1mw, wrerror\033[0m\tFile has registered a write error.\n");
  printf("\t\033[1mUx, Uw, Ur\033[0m\tFile/directory owner user has executable, write, read priviledge.\n");
  printf("\t\033[1mUnx, Unw, Unr\033[0m\tFile/directory owner user has NOT executable, write, read priviledge.\n");
  printf("\t\033[1mGx, Gw, Gr\033[0m\tFile/directory owner group has executable, write, read priviledge.\n");
  printf("\t\033[1mGnx, Gnw, Gnr\033[0m\tFile/directory owner group has NOT executable, write, read priviledge.\n");
  printf("\t\033[1mOx, Ow, Or\033[0m\tOther users have executable, write, read priviledge.\n");
  printf("\t\033[1mOnx, Onw, Onr\033[0m\tOther users have NOT executable, write, read priviledge.\n");
  printf("\t\033[1mhybrid\033[0m\t\tFile/directory is hybrid.\n");
  printf("\t\033[1mnohybrid\033[0m\tFile/directory is NOT hybrid.\n");
  printf("\033[1m  \033[4mFIELD:\033[0m\033[1m <FIELDNAME> <COMPARATOR> <VALUE>\033[0m\n");
  printf("\033[1m    \033[4mVALUE:\033[0m\n");              
  printf("\t\033[1m1 unsigned 64 bit\033[0m It can be expressed in 1000 or 1024 units: 8 [8], 1k [1000], 4K [4096], 9M [9663676416]\n");
  printf("\t\033[1m2 unsigned 32 bit\033[0m In the format <cid#>/<sid#>.\n");
  printf("\t\033[1mcharacter string\033[0m  A file/directory name, a regex, ...\n");
  printf("\t\033[1mfid\033[0m\t\t  In UUID format.\n");
  printf("\t\033[1mdate\033[0m\t\t  Dates must be expressed in one of the following format:\n");
  printf("\t\t\t   - YYYY-MM-DD\n\t\t\t   - YYYY-MM-DD-HH\n\t\t\t   - YYYY-MM-DD-HH:MM\n\t\t\t   - YYYY-MM-DD-HH:MM:SS\n");
  printf("\t\t\t or just in seconds from the EPOC.\n");
  printf("\033[1m    \033[4mCOMPARATOR:\033[0m\n");              
  printf("\t\033[1mlt, \\<\033[0m\t\t<FIELDNAME> must be lower than <VALUE>.\n");
  printf("\t\033[1mle, \\<=\033[0m\t\t<FIELDNAME> must be lower or equal to <VALUE>.\n");
  printf("\t\033[1meq, =\033[0m\t\t<FIELDNAME> must be equal to <VALUE>.\n");
  printf("\t\033[1mge, \\>=\033[0m\t\t<FIELDNAME> must be bigger or equal to <VALUE>.\n");
  printf("\t\033[1mgt, \\>\033[0m\t\t<FIELDNAME> must be bigger than <VALUE>.\n");
  printf("\t\033[1mne, !=\033[0m\t\t<FIELDNAME> must be different from <VALUE>.\n");
  printf("\t\033[1mregex\033[0m\t\t<FIELDNAME> must match PCRE regex <VALUE>. Regex MUST be quoted and a space MUST be set after the last quote.\n");
  printf("\033[1m    \033[4mFIELDNAME:\033[0m\033[1m supporting \"=\" or \"!=\" comparators with unsigned 64 bit VALUE\033[0m\n");
  printf("\t\033[1mproject\033[0m\t\tFile/directory project identifier.\n"); 
  printf("\t\033[1mg, gid\033[0m\t\tFile/directory group identifier.\n"); 
  printf("\t\033[1mu, uid\033[0m\t\tFile/directory user identifier.\n"); 
  printf("\t\033[1mcid\033[0m\t\tFile cluster identifier of any sub-file.\n"); 
  printf("\033[1m    \033[4mFIELDNAME:\033[0m\033[1m supporting \"=\" or \"!=\" comparators with 2 unsigned 32 bit VALUE\033[0m\n");
  printf("\t\033[1msid\033[0m\t\tLogical storage identifiers of any sub-file (<cid#>/<sid#>).\n"); 
  printf("\t\033[1msidrange[x:y]\033[0m\tLogical storages identifiers within rank [x..y] of the distribution of any sub-file.\n"); 
  printf("\033[1m    \033[4mFIELDNAME:\033[0m\033[1m supporting \"=\", \"\\>=\" or regex comparators with character string VALUE\033[0m\n");
  printf("\t\033[1mn, name\033[0m\t\tFile/directory name.\n");
  printf("\t\033[1mparent\033[0m\t\tFile/directory parent name.\n");
  printf("\t\033[1xname\033[0m\t\textended attribute name.\n");
  printf("\033[1m    \033[4mFIELDNAME:\033[0m\033[1m supporting  \"\\<\", \"\\<=\", \"=\", \"\\>\", \"\\>=\" or \"!=\" comparators.\033[0m\n");
  printf("\033[1m      \033[4mwith unsigned VALUE\033[0m\n");
  printf("\t\033[1mlink\033[0m\t\tFile number of links.\n"); 
  printf("\t\033[1ms, size\033[0m\t\tFile/directory size.\n"); 
  printf("\t\033[1mchildren\033[0m\tDirectory number of children.\n"); 
  printf("\t\033[1mdeleted\033[0m\t\tDirectory number of deleted inode in the trash.\n"); 
  printf("\t\033[1mslave\033[0m\t\tFile/directory number of slave inodes in multifile mode.\n");
  printf("\033[1m      \033[4mwith date VALUE\033[0m\n");
  printf("\t\033[1mcr8\033[0m\t\tFile/directory creation date.\n");
  printf("\t\033[1mmtime\033[0m\t\tFile/directory modification date.\n"); 
  printf("\t\033[1mctime\033[0m\t\tFile/directory change date.\n"); 
  printf("\t\033[1matime\033[0m\t\tFile/directory access date.\n"); 
  printf("\t\033[1mupdate\033[0m\t\tDirectory update date.\n"); 
  printf("\033[1m    \033[4mFIELDNAME:\033[0m\033[1m supporting  only \"=\" comparator.\033[0m\n");
  printf("\t\033[1mpfid\033[0m\t\tParent FID (value is a UUID).\n");
  printf("\033[1m\033[4mOUTPUT:\033[0m\n");              
  printf("\t\033[1mout <f1,f2...>\033[0m\tDescribes requested output fields with ',' and no ' '.\n");
  printf("\t\t\t\tDefault is to have one file/directory path per line.\n");
  printf("\t\033[1mline<val>\033[0m\t\tDisplay <val> file/directory per line.\n");
  printf("\t\033[1mjson\033[0m\t\t\toutput is in json format.\n");
  printf("\t\033[1malls|allh\033[0m\t\tdisplay every field except extended attributes (\033[1mxattr\033[0m option).\n");
  printf("\t\t\t\t\tdates are expressed in \033[7ms\033[0meconds or \033[7mh\033[0muman readable date.\n");
  printf("\t\033[1m<full|rel|fid>\033[0m\t\tDisplay <full names|relative names|fid>.\n");
  printf("\t\033[1msize\033[0m\t\t\tdisplay file/directory size.\n");
  printf("\t\033[1mproject\033[0m\t\t\tdisplay project identifier.\n");
  printf("\t\033[1mchildren\033[0m\t\tdisplay directory number of children.\n");
  printf("\t\033[1mdeleted\033[0m\t\t\tdisplay directory number of deleted children.\n");
  printf("\t\033[1mnlink\033[0m\t\t\tdisplay file number of link.\n");
  printf("\t\033[1muid\033[0m\t\t\tdisplay uid.\n");
  printf("\t\033[1mgid\033[0m\t\t\tdisplay gid.\n");
  printf("\t\033[1mscr8|hcr8\033[0m\t\tdisplay creation time in \033[7ms\033[0meconds or \033[7mh\033[0muman readable date.\n");
  printf("\t\033[1msmtime|hmtime\033[0m\t\tdisplay modification time in \033[7ms\033[0meconds or \033[7mh\033[0muman readable date.\n");
  printf("\t\033[1msctime|hctime\033[0m\t\tdisplay change time in \033[7ms\033[0meconds or \033[7mh\033[0muman readable date.\n");
  printf("\t\033[1msupdate|hupdate\033[0m\t\tdisplay update directory time in \033[7ms\033[0meconds or \033[7mh\033[0muman readable date.\n");
  printf("\t\033[1msatime|hatime\033[0m\t\tdisplay access time in \033[7ms\033[0meconds or \033[7mh\033[0muman readable date.\n");
  printf("\t\033[1mpriv\033[0m\t\t \tdisplay Linux privileges.\n");
  printf("\t\033[1mxattr\033[0m\t\t \tdisplay extended attributes names and values.\n");
  printf("\t\t\t\t\t\033[1mall\033[0m or \033[1mallh\033[0m do not display extended attributes.\n");
  printf("\t\033[1mdistrib\033[0m\t\t\tdisplay RozoFS distribution.\n");
  printf("\t\033[1mtrash\033[0m\t\t\tdisplay directory trash configuration.\n");
  printf("\t\033[1mid\033[0m\t\t\tdisplay RozoFS FID.\n");
  printf("\t\033[1merror\033[0m\t\t\tdisplay file write error detected.\n");
  printf("\t\033[1mstrip\033[0m\t\t\tdisplay file striping information.\n");
  printf("\t\033[1msep=<string>\033[0m\t\tdefines a field separator without ' '.\n");
  printf("\t\033[1mcount<val>\033[0m\t\tStop after displaying the <val> first found entries.\n");
  printf("\t\033[1mnone\033[0m\t\t\tJust display the count of file/dir but no file/dir information.\n");
  printf("\t\033[1mstat\033[0m\t\t\tDisplay scanning statistic.\n");
  printf("\n\033[4mNOTE:\033[0m\tCharacters \')\', \'(\', \'>\' and \'<\' must be escaped using \'\\' to avoid Linux interpretation.\n");
  printf("\tRegex MUST be quoted and a space MUST be set after the last quote.\n");
  printf("\t1k is 1000 while 1K is 1024.\n");
  
  if (fmt == NULL) {
    printf("\n\033[4mExamples:\033[0m\n");
    printf("Display every attributes of file with FID 00000000-0000-4000-1800-000000000310.\n");
    printf("  \033[1mrozo_scan fid=00000000-0000-4000-1800-000000000310 out json,all,xattr\033[0m\n");
    printf("Searching for files having extended attributes with a size comprised between 76000 and 76100 or equal to 78M .\n");
    printf("  \033[1mrozo_scan [xattr and [[size\\>=76k and size\\<=76100] or size=78M]] out size,xattr\033[0m\n");
    printf("Searching for files having system extended attributes.\n");
    printf("  \033[1mrozo_scan xname ge system. out json,xattr\033[0m\n");
    printf("Searching for files with a modification date in february 2017 but created before 2017.\n");
    printf("  \033[1mrozo_scan mtime ge 2017-02-01 and mtime lt 2017-03-01 and cr8 lt 2017-01-01 out hcr8,hmtime,uid,sep=#\033[0m\n");
    printf("Searching for files created by user 4501 or goup 1023 on 2015 January the 10th in the afternoon.\n");
    printf("  \033[1mrozo_scan {{uid=4501 or gid=1023} and cr8\\< 2015-01-10-12:00 and cr8\\>=2015-01-11} out hcr8,hmtime,uid,gid\033[0m\n");
    printf("Searching for files owned by group 4321 in directory with FID 00000000-0000-4000-1800-000000000018.\n");
    printf("  \033[1mrozo_scan gid=4321 and pfid=00000000-0000-4000-1800-000000000018 out json,all\033[0m\n");
    printf("Searching for files whoes name constains captainNemo.\n");
    printf("  \033[1mrozo_scan name ge captainNemo out json,all\033[0m\n");
    printf("Searching for directories whoes name starts by \"Dir_\", ends with \".DIR\", and contains at least one decimal number.\n");
    printf("  \033[1mrozo_scan dir name regex \"^Dir_.*\\d+.*\\.DIR$\"\033[0m\n");    
    printf("Searching for directories with more than 10000 entries.\n");
    printf("  \033[1mrozo_scan dir children\\>=10k out json,size,children\033[0m\n");
    printf("Searching for all symbolic links.\n");
    printf("  \033[1mrozo_scan slink out json,all\033[0m\n");
    printf("Searching for files in project #31 owned by user 2345 off size 120K or 240K.\n");
    printf("  \033[1mrozo_scan project=31 and uid=2345 and[size=120K or size=240K]\033[0m\n");
    printf("Searching for files in cluster 2 not using sid 7 in their distribution.\n");
    printf("  \033[1mrozo_scan cid=2 and sid!=2/7 out distrib\033[0m\n");
    printf("Searching for files using sid 3/5 as spare.\n");
    printf("  \033[1mrozo_scan sidrange[6:7]=3/5 out json,all\033[0m\n");
    printf("Searching for non writable files being executable by its group but not by the others.\n");
    printf("  \033[1mrozo_scan Unw Gx Onx out priv,gid,uid\033[0m\n");
    printf("Searching for multifiles not hybrid files having more than 4 sub files.\n");
    printf("  \033[1mrozo_scan nohybrid slave gt 4 out json,all\033[0m\n");
    printf("Searching for sub-directories under a some directories where any change occured after a given date.\n");
    printf("  \033[1mrozo_scan dir \\(parent\\>=./joe/ or parent\\>=./jeff/ \\) and update\\>=2019-06-18\033[0m\n");
    printf("Searching for sub-directories under a some directories that have been moved/renamed after a given date.\n");
    printf("  \033[1mrozo_scan dir parent ge ./project/BenHur atime ge 0030-04-07-12:00 cr8 lt 0030-04-07-12:00\033[0m\n");
    printf("Searching for .o object files under one directory.\n");
    printf("  \033[1mrozo_scan dir parent ge ./proj2/compil name ge .o out json,all\033[0m\n");
  }
  exit(EXIT_SUCCESS);     
} 
/*
**___________________________________________________________________
** Show wherethe error is in the parsed command line
**
** @param argc   Number of input argument
** @param argv   Array of arguments
**
**___________________________________________________________________
*/
void rozofs_scan_display_error(int argc, char *argv[]) {
  int    i,j;                                
  printf("\nrozo_scan");                       
  for (i=1; i<argc; i++) {                   
    if (rozofs_scan_current_arg_idx!=i) {    
      printf(" %s", argv[i]);                
      continue;                              
    }                                        
    printf(" ");                             
    for (j=0; j<rozofs_scan_current_arg_char2skip; j++) {
      printf("%c",argv[i][j]);               
    }                                        
    printf("\033[91m\033[40m\033[1m");       
    for (; j<strlen(argv[i]); j++) {         
      printf("%c",argv[i][j]);               
    }                                        
  }                                          
  printf("\033[0m\n");                       
}
#define rozofs_show_error(fmt, ...) {rozofs_scan_display_error(argc,argv); usage(fmt,##__VA_ARGS__); } 

/* 
**__________________________________________________________________
** Display the whole tree behind a given node in recursive mode
**
**   @param node             The node to display 
**   @param level            The level of recursion
**__________________________________________________________________
*/
void rozofs_scan_display_tree(rozofs_scan_node_t * node, int level) {
  char                 blanks[80];
  char               * pChar;
  char                 fidString[40];
  int                  idx;
    
  if (node == NULL) {
    printf("OK");
    return;
  }
  
  /*
  ** shift the display to the right depending on the level of recursion
  */
  pChar = blanks;
  *pChar = 0;
  for (idx=0; idx<level; idx++) {
    *pChar++ = '|';
    *pChar++ = ' ';
    *pChar++ = ' ';
    *pChar++ = ' ';
    *pChar++ = ' ';
    *pChar = 0;  
  }   
  
  switch(node->type) {
  
    /*
    ** Just a criteria name
    */
    case rozofs_scan_type_criteria:
      printf("[%s]\n", rozofs_scan_keyw_e2String(node->l.name));
      break;
      
    /*
    ** Field name + a comparator + a value to compare to
    */      
    case rozofs_scan_type_field:
      switch(node->l.valType) {
        case rozofs_scan_value_type_u64:    
          printf("[%s]--[%s]--[%llu]\n",
                  rozofs_scan_keyw_e2String(node->l.name),
                  rozofs_scan_keyw_e2String(node->l.comp),
                  (long long unsigned int)node->l.value.u64); 
          break;
        case rozofs_scan_value_type_2u32:
          printf("[%s]--[%s]--[%u/%u]\n",
                  rozofs_scan_keyw_e2String(node->l.name),
                  rozofs_scan_keyw_e2String(node->l.comp),
                  node->l.value.u32[0],node->l.value.u32[1]); 
          break;        
        case rozofs_scan_value_type_2u32_range:
          printf("[%s[%llu:%llu]]--[%s]--[%u/%u]\n",
                  rozofs_scan_keyw_e2String(node->l.name),
                  (long long unsigned int)node->l.range.min, 
                  (long long unsigned int)node->l.range.max, 
                  rozofs_scan_keyw_e2String(node->l.comp),
                  node->l.value.u32[0],
                  node->l.value.u32[1]); 
          break;        
        case rozofs_scan_value_type_string: 
          printf("[%s]--[%s]--[%s]\n",
                  rozofs_scan_keyw_e2String(node->l.name),
                  rozofs_scan_keyw_e2String(node->l.comp),
                  node->l.value.string); 
          break;
        case rozofs_scan_value_type_fid:    
          rozofs_fid_append(fidString,node->l.value.fid);
          printf("[%s]--[%s]--[%s]\n",
                  rozofs_scan_keyw_e2String(node->l.name),
                  rozofs_scan_keyw_e2String(node->l.comp),
                  fidString); 
          break;
        case rozofs_scan_value_type_regex:  
          printf("[%s]--[%s]--[REGEX]\n",
                  rozofs_scan_keyw_e2String(node->l.name),
                  rozofs_scan_keyw_e2String(node->l.comp)); 
          break;      
        default:                            
          printf("[%s]--[%s]-- ? Bad valType %d?\n", 
                  rozofs_scan_keyw_e2String(node->l.name),
                  rozofs_scan_keyw_e2String(node->l.comp),          
                  node->l.valType);
          break;
      }           
      break;

    /*
    ** sub node 
    */            
    case rozofs_scan_type_node:      
      /*
      ** Increment the recursion level for the next calls
      */       
      level++;
      
      //printf("\n%s ___\n",blanks);
      printf("\n");
      for (idx=0; idx< node->n.nbNext; idx++) {
        if (idx) printf("%s+-%3s--",blanks,(node->n.ope == rozofs_scan_node_ope_and)?"AND":"OR-");          
        else     printf("%s+-%3s--",blanks,(node->n.ope == rozofs_scan_node_ope_and)?"AND":"OR-");                 
        rozofs_scan_display_tree((rozofs_scan_node_t *) node->n.next[idx],level);
      }  
      //printf("%s|___\n",blanks);
      break;
      
    default:  
      printf("? Bad type %d?", node->type);
      return;
  }  
} 
/*
**_______________________________________________________________________
** Add a new sub node to a node
**
** @param node       The node to add a sub node on
** @param subnode    The sub node to add to the node
** 
** @retval the sub node address
**_______________________________________________________________________
*/
rozofs_scan_node_t * rozofs_scan_add_node (rozofs_scan_node_t * node,
                                           rozofs_scan_node_t * subnode) {
  /*
  ** No next node
  */                                         
  if (subnode== NULL) return node;                                         
                 
  /*
  ** Add the node if the maximum sub node number is not exceeded
  */                                         
  subnode->n.prev = node;
  if (node->n.nbNext < ROZOFS_SCAN_MAX_NEXT_NODE) {
    node->n.next[node->n.nbNext] = subnode;
    node->n.nbNext++;
    return subnode;  
  }                         

  /*
  ** Dispay the tree and exit
  */
  rozofs_scan_display_tree(upNode, 0);
  usage("rozofs_scan_add_node on full node [%d]\n",node->n.nb);
  return NULL;   
}
/*
**_______________________________________________________________________
** Create a new node taking as 1rst sub-node the input leaf which
** should be either a criteria or a field comparison.
**
** @param leaf   The leaf to create a node with
**
** @retval the created node
**_______________________________________________________________________
*/
rozofs_scan_node_t * rozofs_scan_new_node (rozofs_scan_node_t * leaf) {
  rozofs_scan_node_t * node; 
  
  node = malloc(sizeof(rozofs_scan_node_t));
  memset(node, 0, sizeof(rozofs_scan_node_t));

  node->type    = rozofs_scan_type_node;
  /*
  ** Default operation is AND 
  */
  node->n.ope     = rozofs_scan_node_ope_and;
  /*
  ** Number the nodes
  */
  node->n.nb      = rozofs_scan_node_counter++;
  /*
  ** New node is empty up to now
  */
  node->n.nbNext  = 0;
  /*
  ** Add the given leaf
  */
  rozofs_scan_add_node(node,leaf);
  return node;  
}
/*
**_______________________________________________________________________
** Set OR operation
** This operation is only valid when no more than 1 next node
** is defined in the node. After that the operation can not change.
**
** @param node     The node to modify the operation in
**_______________________________________________________________________
*/
void rozofs_scan_set_ope_or (rozofs_scan_node_t * node) {

  if (node->n.nbNext<2) {
    node->n.ope = rozofs_scan_node_ope_or;
    return;
  }  

  if (node->n.ope != rozofs_scan_node_ope_or) {
    rozofs_scan_display_tree(upNode, 0);    
    usage("Badly placed brackets");
  }  
}
/*
**_______________________________________________________________________
** Set AND operation
** And is the default operation.It can not be changed once more 
** that 1 next node is defined in the node. 
**
** @param node     The node to modify the operation in
**_______________________________________________________________________
*/
void rozofs_scan_set_ope_and (rozofs_scan_node_t * node) {

  if (node->n.nbNext<2) {
    node->n.ope = rozofs_scan_node_ope_and;
    return;
  }  

  if (node->n.ope != rozofs_scan_node_ope_and) {
    rozofs_scan_display_tree(upNode, 0);    
    usage("Badly placed brackets");
  }
}
/*
**_______________________________________________________________________
** Create a new node holding a new criteria as 1rst node
**
** @param  criteria     New criteria
**
** @retval the address of the new created node
**_______________________________________________________________________
*/
rozofs_scan_node_t * rozofs_scan_new_criteria(rozofs_scan_keyw_e criteria) {
  rozofs_scan_node_t * leaf;
  
  leaf = malloc(sizeof(rozofs_scan_node_t));
  memset(leaf, 0, sizeof(rozofs_scan_node_t));

  leaf->type   = rozofs_scan_type_criteria;
  leaf->l.name = criteria;
  
  return rozofs_scan_new_node(leaf);
}
/*
**_______________________________________________________________________
** Create a new node holding a new fid field comparison 
**
** @param name      New field name
** @param fid       fid to compare to
** @param comp      comparator
**
** @retval the address of the new created node
_______________________________________________________________________
*/
rozofs_scan_node_t * rozofs_scan_new_fid_field(rozofs_scan_keyw_e              name, 
                                               fid_t                           fid,
                                               rozofs_scan_keyw_e              comp) {
  rozofs_scan_node_t * leaf;
  
  leaf = malloc(sizeof(rozofs_scan_node_t));
  memset(leaf, 0, sizeof(rozofs_scan_node_t));

  leaf->type         = rozofs_scan_type_field;
  leaf->l.name       = name;
  leaf->l.valType    = rozofs_scan_value_type_fid;   
  memcpy(leaf->l.value.fid, fid, sizeof(fid_t));
  leaf->l.comp       = comp;
  return rozofs_scan_new_node(leaf);
}
/*
**_______________________________________________________________________
** Create a new node holding a new u64 field
**
** @param name      New field name
** @param value     64 bit value
** @param comp      comparator
**
** @retval the address of the new created node
**_______________________________________________________________________
*/
rozofs_scan_node_t * rozofs_scan_new_u64_field(rozofs_scan_keyw_e              name, 
                                               uint64_t                        value,
                                               rozofs_scan_keyw_e              comp) {
  rozofs_scan_node_t * leaf;
  
  leaf = malloc(sizeof(rozofs_scan_node_t));
  memset(leaf, 0, sizeof(rozofs_scan_node_t));

  leaf->type         = rozofs_scan_type_field;
  leaf->l.name       = name;
  leaf->l.valType    = rozofs_scan_value_type_u64;   
  leaf->l.value.u64  = value;
  leaf->l.comp       = comp;
  return rozofs_scan_new_node(leaf);
}
/*
**_______________________________________________________________________
** Create a new node holding a new 2 x u32 field
**
** @param name      New field name
** @param comp      comparator
** @param val0      1rst 32 bit value
**
** @retval the address of the new created node
**_______________________________________________________________________
*/
rozofs_scan_node_t * rozofs_scan_new_2u32_field(rozofs_scan_keyw_e              name, 
                                                uint32_t                        val0,
                                                uint32_t                        val1,
                                                rozofs_scan_keyw_e              comp) {
  rozofs_scan_node_t * leaf;
  
  leaf = malloc(sizeof(rozofs_scan_node_t));
  memset(leaf, 0, sizeof(rozofs_scan_node_t));

  leaf->type           = rozofs_scan_type_field;
  leaf->l.name         = name;
  leaf->l.valType      = rozofs_scan_value_type_2u32;   
  leaf->l.value.u32[0] = val0;
  leaf->l.value.u32[1] = val1;
  leaf->l.comp         = comp;
  return rozofs_scan_new_node(leaf);
}
/*
**_______________________________________________________________________
** Create a new node holding a new 2 x u32 field
**
** @param name      New field name
** @param val0      1rst 32 bit value
** @param val1      2nd 32 bit value
** @param comp      comparator
** @param min       min range
** @param max       max range
**
** @retval the address of the new created node
**_______________________________________________________________________
*/
rozofs_scan_node_t * rozofs_scan_new_2u32_range_field(rozofs_scan_keyw_e              name, 
                                                      uint32_t                        val0,
                                                      uint32_t                        val1,
                                                      rozofs_scan_keyw_e              comp,
                                                      uint64_t                        min,
                                                      uint64_t                        max) {
  rozofs_scan_node_t * leaf;
  
  leaf = malloc(sizeof(rozofs_scan_node_t));
  memset(leaf, 0, sizeof(rozofs_scan_node_t));

  leaf->type           = rozofs_scan_type_field;
  leaf->l.name         = name;
  leaf->l.valType      = rozofs_scan_value_type_2u32_range;   
  leaf->l.value.u32[0] = val0;
  leaf->l.value.u32[1] = val1;
  leaf->l.range.min    = min;
  leaf->l.range.max    = max;  
  leaf->l.comp         = comp;
  return rozofs_scan_new_node(leaf);
}
/*
**_______________________________________________________________________
** Create a new node holding a new string field
**
** @param name      New field name
** @param value     string value
** @param comp      comparator
**
** @retval the address of the new created node
**_______________________________________________________________________
*/
rozofs_scan_node_t * rozofs_scan_new_string_field(rozofs_scan_keyw_e   name, 
                                                  char               * value,
                                                  rozofs_scan_keyw_e   comp) {
  rozofs_scan_node_t * leaf;
  
  leaf = malloc(sizeof(rozofs_scan_node_t));
  memset(leaf, 0, sizeof(rozofs_scan_node_t));

  leaf->type           = rozofs_scan_type_field;
  leaf->l.name         = name;
  leaf->l.valType      = rozofs_scan_value_type_string;   
  leaf->l.value.string = strdup(value);
  leaf->l.comp         = comp;
  return rozofs_scan_new_node(leaf);
}
/*
**_______________________________________________________________________
** Create a new node holding a new regex
**
** @param name      New field name
** @param regex     compiled regex
**
** @retval the address of the new created node
**_______________________________________________________________________
*/
rozofs_scan_node_t * rozofs_scan_new_regex_field(rozofs_scan_keyw_e     name, 
                                                 pcre                 * regex) {
  rozofs_scan_node_t * leaf; 
  
  leaf = malloc(sizeof(rozofs_scan_node_t));
  memset(leaf, 0, sizeof(rozofs_scan_node_t));

  leaf->type           = rozofs_scan_type_field;
  leaf->l.name         = name;
  leaf->l.valType      = rozofs_scan_value_type_regex;   
  leaf->l.value.regex  = regex;
  leaf->l.comp         = rozofs_scan_keyw_comparator_regex;
  return rozofs_scan_new_node(leaf);
}
/*
**_______________________________________________________________________
*/
/**
*   Return striping information for a directory

   @param inode_p         pointer to the inode of the directory
   @param ishybrid        whether the directory is hybrid
   @param hybridSize      The size of the hybrid chunk
   @param slaveNb         Number of slave inodes
   @param slaveSize       The size of a slave strip
   @param isaging         whether the directory is aging
      
   @retval 0 no match
   @retval 1 match
*/
void get_directory_striping_info(ext_mattr_t  * inode_p,
                                  int          * ishybrid,  
                                  uint32_t     * hybridSize,
                                  uint32_t     * slaveNb,
                                  uint32_t     * slaveSize,
                                  int          * isaging) {

  /*
  ** Get default export config
  */ 
  *isaging    = 0;
  *slaveNb    = export_config->stripping.factor+1;
  *slaveSize  =  ROZOFS_STRIPING_UNIT_BASE << export_config->stripping.unit;
  *ishybrid   = 0;
  *hybridSize = 0;


  if ((inode_p->s.bitfield1 & ROZOFS_BITFIELD1_AGING) != 0) {
    *isaging = 1;
  }
                               
  /*
  ** The directory has its own configuration
  */                                
  if (inode_p->s.multi_desc.byte != 0) {

    /*
    ** Number of slave and strip size is configured
    */
    if (inode_p->s.multi_desc.master.striping_factor == 0) {
      *slaveNb    = 0; 
      *slaveSize  = 0; 
    }    
    else {
      *slaveNb    = rozofs_get_striping_factor(&inode_p->s.multi_desc); 
      *slaveSize  = rozofs_get_striping_size(&inode_p->s.multi_desc); 
    }
    
    /*
    ** Not hybrid
    */
    if (inode_p->s.hybrid_desc.s.no_hybrid != 0) {
      *ishybrid   = 0;
      *hybridSize = 0;
      return;  
    }    
    
    /*
    ** Hybrid mode
    */
    *ishybrid   = 1;      
    *hybridSize = rozofs_get_hybrid_size(&inode_p->s.multi_desc,&inode_p->s.hybrid_desc);
    return;
  }
  
  /*
  ** nothing defined at directory level so get the information from the export conf
  */       

  if ((inode_p->s.bitfield1 & ROZOFS_BITFIELD1_AGING) != 0) {
    *isaging = 1;
    return;
  }

  switch (export_config->fast_mode) {
    case rozofs_econfig_fast_aging:
      *isaging = 1;
      break;
    case rozofs_econfig_fast_hybrid:
      *ishybrid   = 1;
      *hybridSize = *slaveSize;
      break;
  }
  return;
}  
/*
**_______________________________________________________________________
** API to get the pathname of the objet
**
** @param export           : pointer to the export structure
** @param inode_attr_p     : pointer to the inode attribute
** @param buf              : output buffer
** @param lenmax           : max length of the output buffer
** @param relative         : whether relative mode is requested
** 
** @retval buf: pointer to the beginning of the formated name
**_______________________________________________________________________
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
      pbuf[1] = '/';
      pbuf[2] = 0;
      return pbuf;
   }
   
   inode_val_p = (rozofs_inode_t*)inode_attr_p->s.pfid;
   if ((inode_val_p->fid[0]==0) && (inode_val_p->fid[1]==0))      
   {         
      pbuf[0] = '.';
      pbuf[1] = '/';
      pbuf[2] = 0;
      return pbuf;
   }

   pbuf +=lenmax;
   
   buf[0] = 0;
   first = 1;
   
   if (S_ISDIR(inode_attr_p->s.attrs.mode)) {
     pbuf--;
     *pbuf = 0;
     pbuf--;
     *pbuf ='/';
     first = 0;
   }   
   
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
**__________________________________________________________________
** Get the value of extended attributes of an inode from its name
** rozo_scan_read_xattribute_list() must have been already called
** so global variable xattr_lv2 and xattr_dentry are already populated 
** The value of the extended attribute is stored in buffer
** rozofs_scan_xattr_value_buffer
** 
** @param pXname : Name of the extended attribute
** 
** @retval the size of the extended attribute value in rozofs_scan_xattr_value_buffer
**__________________________________________________________________
*/
int rozo_scan_read_xattribute_value(char * pXname) {
  /*
  ** rozo_scan_read_xattribute_list() must have been already called
  ** so global variable xattr_lv2 and xattr_dentry are already populated 
  */
  return rozofs_getxattr(&xattr_entry, pXname, rozofs_scan_xattr_value_buffer, ROZOFS_SCAN_XATTR_BUFFER_SIZE);
}
/*
**__________________________________________________________________
** Get the list of extended attributes from the inode
** The function gets the list of extended attributes in buffer
** rozofs_scan_xattr_list_buffer, sets rozo_scan_xattr_nb to 
** the number of extended attributes, and populates table
** rozo_scan_xattr_table[] with the pointers to the extended 
** attributes names within buffer rozofs_scan_xattr_list_buffer
**  
** @param inode_attr_p: pointer to the inode data
** @param exportd : pointer to exportd data structure
*
** @retval the number of extended attributes of the inode
**__________________________________________________________________
*/
int rozo_scan_read_xattribute_list(export_t * e, void * inode_attr_p) {
  int              length;
  char *           pt;

  /*
  ** if attibutes have already been read, do not read again
  */
  if (rozo_scan_xattr_nb >= 0) return rozo_scan_xattr_nb;
  rozo_scan_xattr_nb = 0;
  
  /*
  ** Recopy the the attributes in the xattr_lv2 global variable 
  */
  memcpy(&xattr_lv2,inode_attr_p,sizeof(ext_mattr_t));
  xattr_lv2.extended_attr_p = NULL;

  /*
  ** Prepare interface to call list xattr
  */
  xattr_entry.d_inode  = &xattr_lv2;
  xattr_entry.trk_tb_p = e->trk_tb_p;

  length = rozofs_listxattr(&xattr_entry, rozofs_scan_xattr_list_buffer, ROZOFS_SCAN_XATTR_BUFFER_SIZE);
  
  /*
  ** Loop on xattribute names in the buffer
  */
  pt = rozofs_scan_xattr_list_buffer;
  while (length > 0) {
  
    rozo_scan_xattr_table[rozo_scan_xattr_nb] = pt;
    rozo_scan_xattr_nb++;
    
    length -= (strlen(pt)+1);
    pt += (strlen(pt)+1);
  }
  return rozo_scan_xattr_nb;   
}
/*
**__________________________________________________________________
** Read the name from the inode
**  
** @param rootPath : export root path
** @param buffer   : where to copy the name back
** @param len      : returned name length
**
** @retval the file/directory name or NULL
**__________________________________________________________________
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
**_______________________________________________________________________
**  Check the presence of a sid in a list of sid 
**
**   @param sids    : the list of sids
**   @param sid     : the sid to search for
**  
**  @retval 0 sid not in sid list
**  @retval 1 sid in sid list 
**_______________________________________________________________________
*/
int rozofs_check_sid_in_list(sid_t * sids, sid_t sid, uint64_t from, uint64_t to) {

  /*
  ** From too big => no match
  */
  if (from >= rozofs_safe) return 0; 
  /*
  ** To too big => set it to its maximum value
  */
  if (to >= rozofs_safe) to = rozofs_safe-1;
  
  sids += from;    
  while (from<=to) {
    if (*sids == 0)   return 0;
    if (*sids == sid) return 1;
    from++;
    sids++;
  }
  return 0;
}    
/*
**_______________________________________________________________________ rozofs_get_forward
** Check the presence of a cid and sid and an inode
**
**   @param inode_p    : the pointer to the inode
**   @param cid_equal  : cid to match
**   @param sid_equal  : sid to match
**   
**   @retval 0 sid not in sid list
**   @retval 1 sid in sid list 
**_______________________________________________________________________
*/
int rozofs_check_cid_and_sid(ext_mattr_t *inode_p, cid_t cid_equal, sid_t sid_equal, uint64_t from, uint64_t to) {
  int           idx; 
  int           nbSlaves; 
  ext_mattr_t * slave_p;;
  
  /*
  ** hybrid as well as non multifile mode have a distribution in 1rst inde
  */
  if ((inode_p->s.multi_desc.byte == 0)||(inode_p->s.hybrid_desc.s.no_hybrid == 0)) { 
    if (inode_p->s.attrs.cid == cid_equal) {
      /*
      ** Master inode matches
      */
      if (rozofs_check_sid_in_list(inode_p->s.attrs.sids, sid_equal, from, to)) {
        /*
        ** Master inode matches cid & sid
        */
        return 1;
      }
    }   
  } 
  
  /*
  ** Slave inodes for multifile mode
  */ 
  
  if (inode_p->s.multi_desc.byte == 0) {
    /*
    ** No other distribution
    */
    return 0;
  } 
   
  nbSlaves = rozofs_get_striping_factor(&inode_p->s.multi_desc); 
  for (idx=1; idx <= nbSlaves;  idx++) {

    slave_p = inode_p + idx;
    if (slave_p->s.attrs.cid == cid_equal) {
      /*
      ** Slave inode matches
      */   
      if (rozofs_check_sid_in_list(slave_p->s.attrs.sids, sid_equal, from, to)) {
        /*
        ** Slave inode matches cid & sid
        */
        return 1;
      }
    }
  }
  return 0;
}  
/*
**_______________________________________________________________________
**   Check the presence of a cid in one of the sub files
**
**   @param inode_p    : the pointer to the inode
**   @param cid_equal  : cid to match
**   
**   @retval 0 cid is not present
**   @retval 1 cid is present
**_______________________________________________________________________
*/
int rozofs_check_cid(ext_mattr_t *inode_p, cid_t cid_equal) {
  int           idx; 
  int           nbSlaves; 
  ext_mattr_t * slave_p;;
  
  /*
  ** hybrid as well as non multifile mode have a distribution in 1rst inde
  */
  if ((inode_p->s.multi_desc.byte == 0)||(inode_p->s.hybrid_desc.s.no_hybrid == 0)) { 
    if (inode_p->s.attrs.cid == cid_equal) {
      /*
      ** Master inode matches
      */
      return 1;
    }   
  } 
  
  /*
  ** Slave inodes for multifile mode
  */ 
  
  if (inode_p->s.multi_desc.byte == 0) {
    /*
    ** No other distribution
    */
    return 0;
  } 
   
  nbSlaves = rozofs_get_striping_factor(&inode_p->s.multi_desc); 
  for (idx=1; idx <= nbSlaves;  idx++) {

    slave_p = inode_p + idx;
    if (slave_p->s.attrs.cid == cid_equal) {
      /*
      ** Slave inode matches
      */   
      return 1;
    }
  }
  return 0;
}  
/*
**_______________________________________________________________________
** Check input rmfentry against a given criteria
**
**   @param e                   pointer to exportd data structure
**   @param entry               pointer to the data of the rmfentry to check
**   @param criteria_to_match   Criteria to check the rmfentry against  
**
** @retaval 1 on success / 0 on failure
**_______________________________________________________________________
*/
int rozofs_scan_eval_one_criteria_rmfentry(
                   void               * e, 
                   void               * entry, 
                   rozofs_scan_keyw_e   criteria_to_match) {

  /*
  ** Absolutly no criteria is valid for a rmfentry
  */
  severe("Unexpected rmfentry criteria %d",criteria_to_match);
  return 1;             
}
/*
**_______________________________________________________________________
** Check input rmfentry against a given field and comparator
**
** @param e                   pointer to exportd data structure
** @param entry               pointer to the data  of the rmfentry check
** @param field_name          Name of the field to compare 
** @param field_value         Value to compare the field to  
** @param comp                comparator to apply  
**
** @retaval 1 on success / 0 on failure
**_______________________________________________________________________
*/

int rozofs_scan_eval_one_field_rmfentry(
                  export_t                  * e, 
                  void                      * entry, 
                  rozofs_scan_keyw_e          field_name,
                  rozofs_scan_field_value_u * field_value, 
                  rozofs_range_t            * field_range,
                  rozofs_scan_keyw_e          comp) {
                  
  rmfentry_disk_t * rmentry = entry;              
  
  switch(field_name) {
        
    /*______________________________________
    ** Size
    */
    case rozofs_scan_keyw_field_size:
      switch(comp) {
        case rozofs_scan_keyw_comparator_lt:
          if (rmentry->size < field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_le:
          if (rmentry->size <= field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_eq:
          if (rmentry->size == field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ge:
          if (rmentry->size >= field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_gt:
          if (rmentry->size > field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ne:
          if (rmentry->size != field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        default:
          severe("Unexpected comparator for field update_time %d",comp);
          return 1;
          break;                 
      }
      return 1;
      break;                      
          
    /*______________________________________
    ** cid
    */
    case rozofs_scan_keyw_field_cid:
      switch(comp) {
        case rozofs_scan_keyw_comparator_eq:
          if (rmentry->cid == field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ne:
          if (rmentry->cid == field_value->u64) {
            return 0;
          }        
          return 1;
          break;          
          
        default:
          severe("Unexpected comparator for field project %d",comp);
          return 1;
          break;                 
      }
      return 1;
      break;   

    /*______________________________________
    ** sid
    */
    case rozofs_scan_keyw_field_sid:
    case rozofs_scan_keyw_field_sidrange:
      switch(comp) {
        case rozofs_scan_keyw_comparator_eq:
          if (rmentry->cid == field_value->u32[0]) {
            if (rozofs_check_sid_in_list(rmentry->initial_dist_set, field_value->u32[1], field_range->min, field_range->max)) {       
              return 1;
            }
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ne:
          if (rmentry->cid == field_value->u32[0]) {
            if (rozofs_check_sid_in_list(rmentry->initial_dist_set, field_value->u32[1], field_range->min, field_range->max)) {       
              return 0;
            }
          }          
          return 1;
          break;          
          
        default:
          severe("Unexpected comparator for field project %d",comp);
          return 1;
          break;                 
      }
      return 1;
      break;  
            
    default:
     severe("Unexpected rmfentry field %d",field_name);
     return 1;    
  }
  return 1;
}
/*
**_______________________________________________________________________
** Check input inode against a given criteria
**
**   @param e                   pointer to exportd data structure
**   @param entry               pointer to the data of the inode to check
**   @param criteria_to_match   Criteria to check the inode against  
**
** @retaval 1 on success / 0 on failure
**_______________________________________________________________________
*/
int rozofs_scan_eval_one_criteria_inode(
                   void               * e, 
                   void               * entry, 
                   rozofs_scan_keyw_e   criteria_to_match) {
                   
  ext_mattr_t        * inode_p = (ext_mattr_t *) entry;  
  int                  result = 1;              
  int                  ishybrid;
  int                  isaging;
  uint32_t             hybridSize;
  uint32_t             slaveNb = -1;
  uint32_t             slaveSize;
                  
  switch(criteria_to_match) {
            
    /*
    ** Must have a write error
    */  
    case rozofs_scan_keyw_criteria_has_write_error: 
      if (rozofs_is_wrerror((lv2_entry_t*)inode_p)) break;
      result = 0;
      break;   
                    
    /*
    ** Must be in trash
    */  
    case rozofs_scan_keyw_criteria_is_in_trash: 
      if (exp_metadata_inode_is_del_pending(inode_p->s.attrs.fid)) break;
      result = 0;
      break;     
         
    /*
    ** Must not be in trash
    */  
    case rozofs_scan_keyw_criteria_not_in_trash: 
      if (!exp_metadata_inode_is_del_pending(inode_p->s.attrs.fid)) break;
      result = 0;
      break;        
               
    /*
    ** Must have extended attributes
    */  
    case rozofs_scan_keyw_criteria_has_xattr: 
      if (rozo_scan_has_extended_attr(inode_p)) break;
      result = 0;
      break;        
      
    /*
    ** Must have no extended attributes
    */  
    case rozofs_scan_keyw_criteria_has_no_xattr: 
      /*
      ** When no more xattribute is set, clear the xattr flag in the mode field
      */
      if (!rozo_scan_has_extended_attr(inode_p)) break;
      result = 0;
      break;        
            
    /*
    ** User must have x privileges
    */  
    case rozofs_scan_keyw_criteria_priv_user_x: 
      if (inode_p->s.attrs.mode & S_IXUSR) break;
      result = 0;
      break;        

    /*
    ** User must not have x privileges
    */  
    case rozofs_scan_keyw_criteria_priv_user_not_x: 
      if (!(inode_p->s.attrs.mode & S_IXUSR)) break;
      result = 0;
      break;        
      
    /*
    ** User must have w privileges
    */  
    case rozofs_scan_keyw_criteria_priv_user_w: 
      if (inode_p->s.attrs.mode & S_IWUSR) break;
      result = 0;
      break;        

    /*
    ** User must not have w privileges
    */  
    case rozofs_scan_keyw_criteria_priv_user_not_w: 
      if (!(inode_p->s.attrs.mode & S_IWUSR)) break;
      result = 0;
      break;        
      
    /*
    ** User must have r privileges
    */  
    case rozofs_scan_keyw_criteria_priv_user_r: 
      if (inode_p->s.attrs.mode & S_IRUSR) break;
      result = 0;
      break;        

    /*
    ** User must not have r privileges
    */  
    case rozofs_scan_keyw_criteria_priv_user_not_r: 
      if (!(inode_p->s.attrs.mode & S_IRUSR)) break;
      result = 0;
      break;        
            
    /*
    ** Group must have x privileges
    */  
    case rozofs_scan_keyw_criteria_priv_group_x: 
      if (inode_p->s.attrs.mode & S_IXGRP) break;
      result = 0;
      break;        

    /*
    ** Group must not have x privileges
    */  
    case rozofs_scan_keyw_criteria_priv_group_not_x: 
      if (!(inode_p->s.attrs.mode & S_IXGRP)) break;
      result = 0;
      break;        
      
    /*
    ** Group must have w privileges
    */  
    case rozofs_scan_keyw_criteria_priv_group_w: 
      if (inode_p->s.attrs.mode & S_IWGRP) break;
      result = 0;
      break;        

    /*
    ** Group must not have w privileges
    */  
    case rozofs_scan_keyw_criteria_priv_group_not_w: 
      if (!(inode_p->s.attrs.mode & S_IWGRP)) break;
      result = 0;
      break;        
      
    /*
    ** Group must have r privileges
    */  
    case rozofs_scan_keyw_criteria_priv_group_r: 
      if (inode_p->s.attrs.mode & S_IRGRP) break;
      result = 0;
      break;        

    /*
    ** Group must not have r privileges
    */  
    case rozofs_scan_keyw_criteria_priv_group_not_r: 
      if (!(inode_p->s.attrs.mode & S_IRGRP)) break;
      result = 0;
      break;            
            
    /*
    ** Other must have x privileges
    */  
    case rozofs_scan_keyw_criteria_priv_other_x: 
      if (inode_p->s.attrs.mode & S_IXOTH) break;
      result = 0;
      break;        

    /*
    ** Other must not have x privileges
    */  
    case rozofs_scan_keyw_criteria_priv_other_not_x: 
      if (!(inode_p->s.attrs.mode & S_IXOTH)) break;
      result = 0;
      break;        
      
    /*
    ** Other must have w privileges
    */  
    case rozofs_scan_keyw_criteria_priv_other_w: 
      if (inode_p->s.attrs.mode & S_IWOTH) break;
      result = 0;
      break;        

    /*
    ** Other must not have w privileges
    */  
    case rozofs_scan_keyw_criteria_priv_other_not_w: 
      if (!(inode_p->s.attrs.mode & S_IWOTH)) break;
      result = 0;
      break;        
      
    /*
    ** Other must have r privileges
    */  
    case rozofs_scan_keyw_criteria_priv_other_r: 
      if (inode_p->s.attrs.mode & S_IROTH) break;
      result = 0;
      break;        

    /*
    ** Other must not have r privileges
    */  
    case rozofs_scan_keyw_criteria_priv_other_not_r: 
      if (!(inode_p->s.attrs.mode & S_IROTH)) break;
      result = 0;
      break;            
      
    /*
    ** Must be an hybrid file
    */
    case rozofs_scan_keyw_criteria_is_hybrid:
      /*
      ** Case of the directory
      */
      if (S_ISDIR(inode_p->s.attrs.mode)) {
        get_directory_striping_info(inode_p, &ishybrid, &hybridSize, &slaveNb, &slaveSize, &isaging);
        if (ishybrid) {
          break;
        }
        result = 0;
        break;  
      }  
      /*
      ** Regular file
      */
      if (inode_p->s.multi_desc.byte == 0) {
        /*
        ** not a multifile 
        */
        result = 0;
        break;
      }  
      if (inode_p->s.hybrid_desc.s.no_hybrid == 0) { 
        break;
      }
      result = 0;
      break;
      
    /*
    ** Must not be an hybrid file
    */
    case rozofs_scan_keyw_criteria_is_not_hybrid:
      /*
      ** Case of the directory
      */
      if (S_ISDIR(inode_p->s.attrs.mode)) {
        get_directory_striping_info(inode_p, &ishybrid, &hybridSize, &slaveNb, &slaveSize, &isaging);
        if (ishybrid) {
          result = 0;
          break;
        }
        break;  
      }  
      /*
      ** Regular file
      */
      if (inode_p->s.multi_desc.byte == 0) {
        /*
        ** not a multifile 
        */
        break;
      }  
      if (inode_p->s.hybrid_desc.s.no_hybrid) { 
        break;
      }
      result = 0;
      break;
      
    /*
    ** Must be an aging file
    */
    case rozofs_scan_keyw_criteria_is_aging:
      /*
      ** Case of the directory
      */
      if (S_ISDIR(inode_p->s.attrs.mode)) {
        get_directory_striping_info(inode_p, &ishybrid, &hybridSize, &slaveNb, &slaveSize, &isaging);
        if (isaging) {
          break;
        }
        result = 0;
        break;  
      } 
      /*
      ** Regular file
      */       
      if ((inode_p->s.bitfield1 & ROZOFS_BITFIELD1_AGING) != 0) {
        /*
        ** is aging
        */
        break;
      }  
      result = 0;
      break;
      
    /*
    ** Must not be an aging file
    */
    case rozofs_scan_keyw_criteria_is_not_aging:
      /*
      ** Case of the directory
      */
      if (S_ISDIR(inode_p->s.attrs.mode)) {
        get_directory_striping_info(inode_p, &ishybrid, &hybridSize, &slaveNb, &slaveSize, &isaging);
        if (isaging) {
          result = 0;
          break;
        }
        break;  
      } 
      /*
      ** Regular file
      */
      if ((inode_p->s.bitfield1 & ROZOFS_BITFIELD1_AGING) == 0) {
        /*
        ** no aging 
        */
        break;
      }  
      result = 0;
      break;


   
   default:
     severe("Unexpected inode criteria %d",criteria_to_match);
     break;
  } 
  VVERBOSE("   %s %s\n", rozofs_scan_keyw_e2String(criteria_to_match), result?"OK":"Failed");   
  return result;     
}
/*
**_______________________________________________________________________
** Check input criteria is a valid check to process for a directory
**
**   @param NS1                   NS
**   @param NS2                   NS
**   @param criteria_to_match     Criteria to check the inode against  
**
** @retaval 1 on success / 0 on failure
**_______________________________________________________________________
*/
int rozofs_scan_validate_one_criteria_directory(
                   void               * NS1, 
                   void               * NS2, 
                   rozofs_scan_keyw_e   criteria_to_match) {
                                                       
  switch(criteria_to_match) {
            
    case rozofs_scan_keyw_criteria_is_in_trash: 
    case rozofs_scan_keyw_criteria_not_in_trash: 
    case rozofs_scan_keyw_criteria_has_xattr: 
    case rozofs_scan_keyw_criteria_has_no_xattr: 
    case rozofs_scan_keyw_criteria_priv_user_x: 
    case rozofs_scan_keyw_criteria_priv_user_not_x: 
    case rozofs_scan_keyw_criteria_priv_user_w: 
    case rozofs_scan_keyw_criteria_priv_user_not_w: 
    case rozofs_scan_keyw_criteria_priv_user_r: 
    case rozofs_scan_keyw_criteria_priv_user_not_r: 
    case rozofs_scan_keyw_criteria_priv_group_x: 
    case rozofs_scan_keyw_criteria_priv_group_not_x: 
    case rozofs_scan_keyw_criteria_priv_group_w: 
    case rozofs_scan_keyw_criteria_priv_group_not_w: 
    case rozofs_scan_keyw_criteria_priv_group_r: 
    case rozofs_scan_keyw_criteria_priv_group_not_r: 
    case rozofs_scan_keyw_criteria_priv_other_x: 
    case rozofs_scan_keyw_criteria_priv_other_not_x: 
    case rozofs_scan_keyw_criteria_priv_other_w: 
    case rozofs_scan_keyw_criteria_priv_other_not_w: 
    case rozofs_scan_keyw_criteria_priv_other_r: 
    case rozofs_scan_keyw_criteria_priv_other_not_r: 
    case rozofs_scan_keyw_criteria_is_hybrid:
    case rozofs_scan_keyw_criteria_is_not_hybrid:
    case rozofs_scan_keyw_criteria_is_aging:
    case rozofs_scan_keyw_criteria_is_not_aging:
      return 1;
      break;
      
    default:
      break;

  } 
  usage("%s is not valid for directories",rozofs_scan_keyw_e2String(criteria_to_match));  
  return 0;          
}
/*
**_______________________________________________________________________
** Check input criteria is a valid check to process for a regular file
**
**   @param NS1                   NS
**   @param NS2                   NS
**   @param criteria_to_match     Criteria to check the inode against  
**
** @retaval 1 on success / 0 on failure
**_______________________________________________________________________
*/
int rozofs_scan_validate_one_criteria_regular(
                   void               * NS1, 
                   void               * NS2, 
                   rozofs_scan_keyw_e   criteria_to_match) {
                                     
  switch(criteria_to_match) {
            
    case rozofs_scan_keyw_criteria_has_write_error: 
    case rozofs_scan_keyw_criteria_is_in_trash: 
    case rozofs_scan_keyw_criteria_not_in_trash: 
    case rozofs_scan_keyw_criteria_has_xattr: 
    case rozofs_scan_keyw_criteria_has_no_xattr: 
    case rozofs_scan_keyw_criteria_priv_user_x: 
    case rozofs_scan_keyw_criteria_priv_user_not_x: 
    case rozofs_scan_keyw_criteria_priv_user_w: 
    case rozofs_scan_keyw_criteria_priv_user_not_w: 
    case rozofs_scan_keyw_criteria_priv_user_r: 
    case rozofs_scan_keyw_criteria_priv_user_not_r: 
    case rozofs_scan_keyw_criteria_priv_group_x: 
    case rozofs_scan_keyw_criteria_priv_group_not_x: 
    case rozofs_scan_keyw_criteria_priv_group_w: 
    case rozofs_scan_keyw_criteria_priv_group_not_w: 
    case rozofs_scan_keyw_criteria_priv_group_r: 
    case rozofs_scan_keyw_criteria_priv_group_not_r: 
    case rozofs_scan_keyw_criteria_priv_other_x: 
    case rozofs_scan_keyw_criteria_priv_other_not_x: 
    case rozofs_scan_keyw_criteria_priv_other_w: 
    case rozofs_scan_keyw_criteria_priv_other_not_w: 
    case rozofs_scan_keyw_criteria_priv_other_r: 
    case rozofs_scan_keyw_criteria_priv_other_not_r: 
    case rozofs_scan_keyw_criteria_is_hybrid:
    case rozofs_scan_keyw_criteria_is_not_hybrid:
    case rozofs_scan_keyw_criteria_is_aging:
    case rozofs_scan_keyw_criteria_is_not_aging:
      return 1;
      break;
   
   default:
     break;
  } 
  usage("%s is not valid for regular files",rozofs_scan_keyw_e2String(criteria_to_match)); 
  return 0;           
}
/*
**_______________________________________________________________________
** Check input criteria is a valid check toprocess for a symbolic link
**
**   @param NS1                   NS
**   @param NS2                   NS
**   @param criteria_to_match     Criteria to check the inode against  
**
** @retaval 1 on success / 0 on failure
**_______________________________________________________________________
*/
int rozofs_scan_validate_one_criteria_slink(
                   void               * NS1, 
                   void               * NS2, 
                   rozofs_scan_keyw_e   criteria_to_match) {
                                     
  switch(criteria_to_match) {
            
    case rozofs_scan_keyw_criteria_priv_user_x: 
    case rozofs_scan_keyw_criteria_priv_user_not_x: 
    case rozofs_scan_keyw_criteria_priv_user_w: 
    case rozofs_scan_keyw_criteria_priv_user_not_w: 
    case rozofs_scan_keyw_criteria_priv_user_r: 
    case rozofs_scan_keyw_criteria_priv_user_not_r: 
    case rozofs_scan_keyw_criteria_priv_group_x: 
    case rozofs_scan_keyw_criteria_priv_group_not_x: 
    case rozofs_scan_keyw_criteria_priv_group_w: 
    case rozofs_scan_keyw_criteria_priv_group_not_w: 
    case rozofs_scan_keyw_criteria_priv_group_r: 
    case rozofs_scan_keyw_criteria_priv_group_not_r: 
    case rozofs_scan_keyw_criteria_priv_other_x: 
    case rozofs_scan_keyw_criteria_priv_other_not_x: 
    case rozofs_scan_keyw_criteria_priv_other_w: 
    case rozofs_scan_keyw_criteria_priv_other_not_w: 
    case rozofs_scan_keyw_criteria_priv_other_r: 
    case rozofs_scan_keyw_criteria_priv_other_not_r: 
      return 1;
      break;
   
   default:
     break;
  } 
  usage("%s is not valid for symbolic links",rozofs_scan_keyw_e2String(criteria_to_match));            
  return 0;           
}
/*
**_______________________________________________________________________
** Check input criteria is a valid check to process for a regular file
**
**   @param NS1                   NS
**   @param NS2                   NS
**   @param criteria_to_match     Criteria to check the inode against  
**
** @retaval 1 on success / 0 on failure
**_______________________________________________________________________
*/
int rozofs_scan_validate_one_criteria_junk(
                   void               * NS1, 
                   void               * NS2, 
                   rozofs_scan_keyw_e   criteria_to_match) {
  usage("%s is not valid for junk files",rozofs_scan_keyw_e2String(criteria_to_match));            
  return 0;           
}
/*
**_______________________________________________________________________
** Check input inode against a given field and comparator
**
** @param e                   pointer to exportd data structure
** @param inode_attr_p        pointer to the data of the inode check
** @param field_name          Name of the field to compare 
** @param field_value         Value to compare the field to  
** @param comp                comparator to apply  
**
** @retaval 1 on success / 0 on failure
**_______________________________________________________________________
*/
#define VVERBOSE_U64(X)  {if (verbose>1) printf("  %s(%llu) %s %llu\n", rozofs_scan_keyw_e2String(field_name), (long long unsigned int)X, rozofs_scan_keyw_e2String(comp), (long long unsigned int)field_value->u64);}
#define VVERBOSE_CHAR(X) {if (verbose>1) printf("  %s(\"%s\") %s \"%s\"\n", rozofs_scan_keyw_e2String(field_name), X, rozofs_scan_keyw_e2String(comp), field_value->string);}
#define VVERBOSE_REGEX(X) {if (verbose>1) printf("  %s(\"%s\") %s\n", rozofs_scan_keyw_e2String(field_name), X, rozofs_scan_keyw_e2String(comp));}
int rozofs_scan_eval_one_field_inode(
                  export_t                  * e, 
                  void                      * entry, 
                  rozofs_scan_keyw_e          field_name,
                  rozofs_scan_field_value_u * field_value, 
                  rozofs_range_t            * field_range,
                  rozofs_scan_keyw_e          comp) {
                  
  ext_mattr_t      * inode_p = entry;               
  int                nameLen;
  char             * pName;
  ext_dir_mattr_t  * stats_attr_p;
  int                nbSlaves;
  int                project;
  int                len;
  char               fullName[ROZOFS_PATH_MAX];
  int                trailer_slash = 0; 
  char             * pt; 
  int                xattr_idx;
  
  switch(field_name) {

    /*______________________________________
    ** Parent FID
    ** Only equality is checked
    */
    case rozofs_scan_keyw_field_pfid:
      if (comp == rozofs_scan_keyw_comparator_eq) {
        if (memcmp(field_value->fid,inode_p->s.pfid,sizeof(fid_t)) != 0) {
          return 0;
        }
        return 1;          
      }
      severe("Unexpected comparator for field pfid %d",comp);
      return 1;
      break;
      
    /*______________________________________
    ** File/directory name
    ** eq, ge, regex
    */
   case rozofs_scan_keyw_field_fname:
     pName = exp_read_fname_from_inode(e->root,inode_p,&nameLen);
     if (pName==NULL) {
       return 0;
     }  
     switch(comp) {
       /*
       ** Equality
       */
       case rozofs_scan_keyw_comparator_eq:  
         VVERBOSE_CHAR(pName);
         /*
         ** Compare the names
         */
         if (strcmp(pName, field_value->string)==0) {
           return 1;
         }  
         return 0;
         break;
          
       /*
       ** ge : Name must contain the given string
       */
       case rozofs_scan_keyw_comparator_ge:   
         VVERBOSE_CHAR(pName);
         if (nameLen < strlen(field_value->string)) {
           return 0;
         }
         if (strstr(pName, field_value->string)==NULL) {
           return 0;
         }  
         return 1;
         break;  
         
       case rozofs_scan_keyw_comparator_regex:
         VVERBOSE_REGEX(pName);         
         /*
         ** Check the regex 
         */
         if (pcre_exec (field_value->regex, NULL, pName, nameLen, 0, 0, NULL, 0) == 0) {
           return 1;
         }
         return 0;             
         
       default:  
        severe("Unexpected comparator for field fname %d",comp);
        return 1;
        break;                 
     }
     return 1;
     break;      
     
    /*______________________________________
    ** xattribute name
    ** eq, ge, regex
    */
   case rozofs_scan_keyw_field_xname:
     /*
     ** Get xattribute list
     */ 
     rozo_scan_read_xattribute_list(e,inode_p);

     /*
     ** Loop on extended attributes
     */
     for (xattr_idx=0; xattr_idx< rozo_scan_xattr_nb; xattr_idx++) {          

       pName = rozo_scan_xattr_table[xattr_idx];
       if (pName==NULL) continue;

       switch(comp) {
         /*
         ** Equality
         */
         case rozofs_scan_keyw_comparator_eq:  
           VVERBOSE_CHAR(pName);
           /*
           ** Compare the names
           */
           if (strcmp(pName, field_value->string)==0) {
             return 1;
           }  
           continue;
           break;

         /*
         ** ge : Name must contain the given string
         */
         case rozofs_scan_keyw_comparator_ge:   
           VVERBOSE_CHAR(pName);
           if (strlen(pName) < strlen(field_value->string)) {
             continue;
           }
           if (strstr(pName, field_value->string)==NULL) {
             continue;
           }  
           return 1;
           break;  

         case rozofs_scan_keyw_comparator_regex:
           VVERBOSE_REGEX(pName);         
           /*
           ** Check the regex 
           */
           if (pcre_exec (field_value->regex, NULL, pName, strlen(pName), 0, 0, NULL, 0) == 0) {
             return 1;
           }
           continue;             

         default:  
          severe("Unexpected comparator for field xname %d",comp);
          continue;
          break;                 
       }
     }  
     return 0;
     break;      
           
    /*______________________________________
    ** Creation time
    */
    case rozofs_scan_keyw_field_cr8time:
      VVERBOSE_U64(inode_p->s.cr8time);               
      switch(comp) {
        case rozofs_scan_keyw_comparator_lt:
          if (inode_p->s.cr8time < field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_le:
          if (inode_p->s.cr8time <= field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_eq:
          if (inode_p->s.cr8time == field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ge:
          if (inode_p->s.cr8time >= field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_gt:
          if (inode_p->s.cr8time > field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ne:
          if (inode_p->s.cr8time != field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        default:
          severe("Unexpected comparator for field cr8time %d",comp);
          return 1;
          break;                 
      }
      return 1;
      break;
      
    /*______________________________________
    ** Modification time
    */
    case rozofs_scan_keyw_field_mtime:
      VVERBOSE_U64(inode_p->s.attrs.mtime);
      switch(comp) {
        case rozofs_scan_keyw_comparator_lt:
          if (inode_p->s.attrs.mtime < field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_le:
          if (inode_p->s.attrs.mtime <= field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_eq:
          if (inode_p->s.attrs.mtime == field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ge:
          if (inode_p->s.attrs.mtime >= field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_gt:
          if (inode_p->s.attrs.mtime > field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ne:
          if (inode_p->s.attrs.mtime != field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        default:
          severe("Unexpected comparator for field mtime %d",comp);
          return 1;
          break;                 
      }
      return 1;
      break;      

    /*______________________________________
    ** Change time
    */
    case rozofs_scan_keyw_field_ctime:
      VVERBOSE_U64(inode_p->s.attrs.ctime);
      switch(comp) {
        case rozofs_scan_keyw_comparator_lt:
          if (inode_p->s.attrs.ctime < field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_le:
          if (inode_p->s.attrs.ctime <= field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_eq:
          if (inode_p->s.attrs.ctime == field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ge:
          if (inode_p->s.attrs.ctime >= field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_gt:
          if (inode_p->s.attrs.ctime > field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ne:
          if (inode_p->s.attrs.ctime != field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        default:
          severe("Unexpected comparator for field ctime %d",comp);
          return 1;
          break;                 
      }
      return 1;
      break;

    /*______________________________________
    ** Access time
    */
    case rozofs_scan_keyw_field_atime:
      VVERBOSE_U64(inode_p->s.attrs.atime);
      switch(comp) {
        case rozofs_scan_keyw_comparator_lt:
          if (inode_p->s.attrs.atime < field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_le:
          if (inode_p->s.attrs.atime <= field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_eq:
          if (inode_p->s.attrs.atime == field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ge:
          if (inode_p->s.attrs.atime >= field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_gt:
          if (inode_p->s.attrs.atime > field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ne:
          if (inode_p->s.attrs.atime != field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        default:
          severe("Unexpected comparator for field atime %d",comp);
          return 1;
          break;                 
      }
      return 1;
      break;

    /*______________________________________
    ** Directory update time
    */
    case rozofs_scan_keyw_field_update_time:
      if (!S_ISDIR(inode_p->s.attrs.mode)) {
        severe("updatetime is only valid for directories");
        return 1;
      }  
      stats_attr_p = (ext_dir_mattr_t *)&inode_p->s.attrs.sids[0];
      if (stats_attr_p->s.version <  ROZOFS_DIR_VERSION_1) {
        return 1;
      }
      VVERBOSE_U64(stats_attr_p->s.update_time);               
      switch(comp) {
        case rozofs_scan_keyw_comparator_lt:
          if (stats_attr_p->s.update_time < field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_le:
          if (stats_attr_p->s.update_time <= field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_eq:
          if (stats_attr_p->s.update_time == field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ge:
          if (stats_attr_p->s.update_time >= field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_gt:
          if (stats_attr_p->s.update_time > field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ne:
          if (stats_attr_p->s.update_time != field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        default:
          severe("Unexpected comparator for field update_time %d",comp);
          return 1;
          break;                 
      }
      return 1;
      break;      
        
    /*______________________________________
    ** Size
    */
    case rozofs_scan_keyw_field_size:
      /*
      ** Directory case
      */
      if (S_ISDIR(inode_p->s.attrs.mode)) {
        stats_attr_p = (ext_dir_mattr_t *)&inode_p->s.attrs.sids[0];
        VVERBOSE_U64(stats_attr_p->s.nb_bytes);
        switch(comp) {
          case rozofs_scan_keyw_comparator_lt:
            if (stats_attr_p->s.nb_bytes < field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          case rozofs_scan_keyw_comparator_le:
            if (stats_attr_p->s.nb_bytes <= field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          case rozofs_scan_keyw_comparator_eq:
            if (stats_attr_p->s.nb_bytes == field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          case rozofs_scan_keyw_comparator_ge:
            if (stats_attr_p->s.nb_bytes >= field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          case rozofs_scan_keyw_comparator_gt:
            if (stats_attr_p->s.nb_bytes > field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          case rozofs_scan_keyw_comparator_ne:
            if (stats_attr_p->s.nb_bytes != field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          default:
            severe("Unexpected comparator for field directory size %d",comp);
            return 1;
            break; 
        }                   
      }
      else {
        /*
        ** File case
        */
        VVERBOSE_U64(inode_p->s.attrs.size);
        switch(comp) {
          case rozofs_scan_keyw_comparator_lt:
            if (inode_p->s.attrs.size < field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          case rozofs_scan_keyw_comparator_le:
            if (inode_p->s.attrs.size <= field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          case rozofs_scan_keyw_comparator_eq:
            if (inode_p->s.attrs.size == field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          case rozofs_scan_keyw_comparator_ge:
            if (inode_p->s.attrs.size >= field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          case rozofs_scan_keyw_comparator_gt:
            if (inode_p->s.attrs.size > field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          case rozofs_scan_keyw_comparator_ne:
            if (inode_p->s.attrs.size != field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          default:
            severe("Unexpected comparator for field update_time %d",comp);
            return 1;
            break;                 
        }
        return 1;
        break;        
      }
      return 1;
      break;           
        
    /*______________________________________
    ** UID
    */
    case rozofs_scan_keyw_field_uid:
      VVERBOSE_U64(inode_p->s.attrs.uid);
      switch(comp) {
        case rozofs_scan_keyw_comparator_eq:
          if (inode_p->s.attrs.uid != field_value->u64) {
            return 0;
          }        
          return 1;
          break;
        case rozofs_scan_keyw_comparator_ne:
          if (inode_p->s.attrs.uid == field_value->u64) {
            return 0;
          }        
          return 1;
          break;
        default:
          severe("Unexpected comparator for field uid %d",comp);
          return 1;
          break;                 
      }
      return 1;
      break;   
      
    /*______________________________________
    ** GID
    */
    case rozofs_scan_keyw_field_gid:
      VVERBOSE_U64(inode_p->s.attrs.gid);
      switch(comp) {
        case rozofs_scan_keyw_comparator_eq:
          if (inode_p->s.attrs.gid != field_value->u64) {
            return 0;
          }        
          return 1;
          break;
        case rozofs_scan_keyw_comparator_ne:
          if (inode_p->s.attrs.gid == field_value->u64) {
            return 0;
          }        
          return 1;
          break;
        default:
          severe("Unexpected comparator for field gid %d",comp);
          return 1;
          break;                 
      }
      return 1;
      break;   

    /*______________________________________
    ** Slave number
    */
    case rozofs_scan_keyw_field_slave:
      if (S_ISREG(inode_p->s.attrs.mode)) {
        if (inode_p->s.multi_desc.byte == 0) {
          nbSlaves = 1;
        }     
        else {  
          nbSlaves = rozofs_get_striping_factor(&inode_p->s.multi_desc); 
        }
        VVERBOSE_U64(nbSlaves);  
        switch(comp) {
          case rozofs_scan_keyw_comparator_lt:
            if (nbSlaves < field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          case rozofs_scan_keyw_comparator_le:
            if (nbSlaves <= field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          case rozofs_scan_keyw_comparator_eq:
            if (nbSlaves == field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          case rozofs_scan_keyw_comparator_ge:
            if (nbSlaves >= field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          case rozofs_scan_keyw_comparator_gt:
            if (nbSlaves > field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          case rozofs_scan_keyw_comparator_ne:
            if (nbSlaves != field_value->u64) {
              return 1;
            }          
            return 0;
            break;
          default:
            severe("Unexpected comparator for field nb slave %d",comp);
            return 1;
            break;                 
        }
      }
      return 1;
      break;   

    /*______________________________________
    ** nlink
    */
    case rozofs_scan_keyw_field_nlink:
      if (!S_ISREG(inode_p->s.attrs.mode)) {
        return 1;
        break;
      } 
      VVERBOSE_U64(inode_p->s.attrs.nlink); 
      switch(comp) {
        case rozofs_scan_keyw_comparator_lt:
          if (inode_p->s.attrs.nlink < field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_le:
          if (inode_p->s.attrs.nlink <= field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_eq:
          if (inode_p->s.attrs.nlink == field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ge:
          if (inode_p->s.attrs.nlink >= field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_gt:
          if (inode_p->s.attrs.nlink > field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ne:
          if (inode_p->s.attrs.nlink != field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        default:
          severe("Unexpected comparator for field nlink %d",comp);
          return 1;
          break;                 
      }
      return 1;
      break;      
                               
    /*______________________________________
    ** Deleted files
    */
    case rozofs_scan_keyw_field_deleted:
      if (!S_ISDIR(inode_p->s.attrs.mode)) {
        return 1;
        break;
      } 
      VVERBOSE_U64(inode_p->s.hpc_reserved.dir.nb_deleted_files); 
      switch(comp) {
        case rozofs_scan_keyw_comparator_lt:
          if (inode_p->s.hpc_reserved.dir.nb_deleted_files < field_value->u64) {
            return 1;
          }          
          return 0;
          break;
         case rozofs_scan_keyw_comparator_le:
          if (inode_p->s.hpc_reserved.dir.nb_deleted_files <= field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_eq:
          if (inode_p->s.hpc_reserved.dir.nb_deleted_files == field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ge:
          if (inode_p->s.hpc_reserved.dir.nb_deleted_files >= field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_gt:
          if (inode_p->s.hpc_reserved.dir.nb_deleted_files > field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ne:
          if (inode_p->s.hpc_reserved.dir.nb_deleted_files != field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        default:
          severe("Unexpected comparator for field deleted %d",comp);
          return 1;
          break;                 
      }
      return 1;
      break;             
          
    /*______________________________________
    ** Children
    */
    case rozofs_scan_keyw_field_children:
      if (!S_ISDIR(inode_p->s.attrs.mode)) {
        return 1;
        break;
      }  
      VVERBOSE_U64(inode_p->s.attrs.children);
      switch(comp) {
        case rozofs_scan_keyw_comparator_lt:
          if (inode_p->s.attrs.children < field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_le:
          if (inode_p->s.attrs.children <= field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_eq:
          if (inode_p->s.attrs.children == field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ge:
          if (inode_p->s.attrs.children >= field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_gt:
          if (inode_p->s.attrs.children > field_value->u64) {
            return 1;
          }          
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ne:
          if (inode_p->s.attrs.children != field_value->u64) {
            return 1;
          }          
          return 0;
          break;
          
          
        default:
          severe("Unexpected comparator for field children %d",comp);
          return 1;
          break;                 
      }
      return 1;
      break;    

          
    /*______________________________________
    ** Project
    */
    case rozofs_scan_keyw_field_project:
      if (S_ISDIR(inode_p->s.attrs.mode)) {    
        project = inode_p->s.attrs.cid;
      }
      else if (S_ISREG(inode_p->s.attrs.mode)) {
        project = inode_p->s.hpc_reserved.reg.share_id;
      }
      VVERBOSE_U64(project);
      switch(comp) {
        case rozofs_scan_keyw_comparator_eq:
          if (project == field_value->u64) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ne:
          if (project != field_value->u64) {
            return 1;
          }        
          return 0;
          break;          
          
        default:
          severe("Unexpected comparator for field project %d",comp);
          return 1;
          break;                 
      }
      return 1;
      break;   
          
    /*______________________________________
    ** cid
    */
    case rozofs_scan_keyw_field_cid:
      /*
      ** Only for regular files
      */
      if (!S_ISREG(inode_p->s.attrs.mode)) {
        return 1;   
      }
      switch(comp) {
        case rozofs_scan_keyw_comparator_eq:
          if (rozofs_check_cid(inode_p, field_value->u64)) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ne:
          if (rozofs_check_cid(inode_p, field_value->u64)) {
            return 0;
          }        
          return 1;
          break;          
          
        default:
          severe("Unexpected comparator for field project %d",comp);
          return 1;
          break;                 
      }
      return 1;
      break;   

    /*______________________________________
    ** sid
    */
    case rozofs_scan_keyw_field_sid:
    case rozofs_scan_keyw_field_sidrange:
      /*
      ** Only for regular files
      */
      if (!S_ISREG(inode_p->s.attrs.mode)) {
        return 1;   
      }
      switch(comp) {
        case rozofs_scan_keyw_comparator_eq:
          if (rozofs_check_cid_and_sid(inode_p, field_value->u32[0], field_value->u32[1], field_range->min, field_range->max)) {
            return 1;
          }        
          return 0;
          break;
        case rozofs_scan_keyw_comparator_ne:
          if (rozofs_check_cid_and_sid(inode_p, field_value->u32[0], field_value->u32[1], field_range->min, field_range->max)) {
            return 0;
          }        
          return 1;
          break;          
          
        default:
          severe("Unexpected comparator for field project %d",comp);
          return 1;
          break;                 
      }
      return 1;
      break;  
             
    /*______________________________________
    ** parent  
    */
    case rozofs_scan_keyw_field_parent:
      pName =  rozo_get_path(e,inode_p, fullName,sizeof(fullName),0);
      nameLen = strlen(pName);
      len = strlen(field_value->string);
      VVERBOSE_CHAR(pName);
      switch(comp) {
        case rozofs_scan_keyw_comparator_eq:
          /*
          ** Check whether given string has a trailer '/'
          */    
          if (field_value->string[len-1] == '/') {
            trailer_slash = 1;
          } 
          else {
            trailer_slash = 0;
          }    
          /*
          ** Point to the end of the name
          */
          pt = pName;
          pt += nameLen-1;
          /*
          ** Rewind until previous / 
          */
          if (S_ISREG(inode_p->s.attrs.mode)) {
            /*
            ** Rewind until previous / to keep only parent directory name
            */
            while ((pt>pName) && (*pt!='/')) {
              pt--;
            }
            if (*pt == '/') {
              if (trailer_slash==1) {
                pt++; /* Keep trailer slash */
              } 
              *pt = 0;
            }      
            if (strcmp(field_value->string,pName) != 0) return 0;
            return 1;
          }
          
          if (S_ISDIR(inode_p->s.attrs.mode)) {
            /*
            ** Check whether we must keep or not last '/' for comparison
            */
            if (trailer_slash==0) {
              /*
              ** Remove trailing / from name
              */
              if (*pt == '/') *pt = 0;
            }
            /*
            ** Is it the directory itself ?
            */
            if (strcmp(field_value->string,pName) == 0) return 1;
            /*
            ** Rewind until previous / to keep only parent directory name
            */
            *pt = 0; /* eventualy remove the latests '/' */
            pt--;
            while ((pt>pName) && (*pt!='/')) {
              pt--;
            }
            if (trailer_slash==0) {
              /*
              ** Remove trailing / from name
              */
              if (*pt == '/') *pt = 0;
            }
            else {
               pt ++;
               *pt = 0;
            }   
            if (strcmp(field_value->string,pName) == 0) return 1;
            return 0;
          }     
          return 1;
          break;
          
        case rozofs_scan_keyw_comparator_ge:
          len = strlen(field_value->string);
          if (nameLen < len) return 0;
          if (strncmp(field_value->string,pName, len) != 0) return 0;
          return 1;
          break;          
          
        case rozofs_scan_keyw_comparator_regex:
          /*
          ** Compare the names
          */
          if (pcre_exec (field_value->regex, NULL, pName, nameLen, 0, 0, NULL, 0) != 0) {
            return 0;
          }  
          return 1;
          break;
          
        default:
          severe("Unexpected comparator for field project %d",comp);
          return 1;
          break;                 
      }
      return 1;
      break;    
      
            
    default:
     severe("Unexpected inode field %d",field_name);
     return 1;    
  }
  return 1;
}
/*
**_______________________________________________________________________
** Check the input test is valid for regular files
**
** @param e                   N.S
** @param inode_attr_p        N.S
** @param field_name          Name of the field to compare 
** @param field_value         Value to compare the field to  
** @param comp                comparator to apply  
**
** @retaval 1 on success / 0 on failure
**_______________________________________________________________________
*/
int rozofs_scan_validate_one_field_regular(
                  export_t                  * NS1, 
                  void                      * NS2, 
                  rozofs_scan_keyw_e          field_name,
                  rozofs_scan_field_value_u * field_value, 
                  rozofs_range_t            * field_range,
                  rozofs_scan_keyw_e          comp) { 
  switch(field_name) {

    /*______________________________________
    ** eq
    */
    case rozofs_scan_keyw_field_pfid:
      if (comp == rozofs_scan_keyw_comparator_eq) {
        return 1;     
      }
      break;
      
    /*______________________________________
    ** eq, ge, regex
    */
    case rozofs_scan_keyw_field_fname:
    case rozofs_scan_keyw_field_parent:
    case rozofs_scan_keyw_field_xname:
      switch(comp) {
        case rozofs_scan_keyw_comparator_eq:  
        case rozofs_scan_keyw_comparator_ge:   
        case rozofs_scan_keyw_comparator_regex:
          return 1;             
          break;
        default:  
         break;                 
      }
      break;      
     
    /*______________________________________
    ** lt, le, eq, gt, ge, ne
    */      
    case rozofs_scan_keyw_field_cr8time:
    case rozofs_scan_keyw_field_mtime:
    case rozofs_scan_keyw_field_ctime:
    case rozofs_scan_keyw_field_atime:
    case rozofs_scan_keyw_field_size:
    case rozofs_scan_keyw_field_slave:      
    case rozofs_scan_keyw_field_nlink:
      switch(comp) {
        case rozofs_scan_keyw_comparator_lt:
        case rozofs_scan_keyw_comparator_le:
        case rozofs_scan_keyw_comparator_eq:
        case rozofs_scan_keyw_comparator_ge:
        case rozofs_scan_keyw_comparator_gt:
        case rozofs_scan_keyw_comparator_ne:
          return 1;
        default:  
          break;                 
      }
      break;        
     
    /*______________________________________
    ** eq, ne
    */      
    case rozofs_scan_keyw_field_uid:
    case rozofs_scan_keyw_field_gid:
    case rozofs_scan_keyw_field_project:
    case rozofs_scan_keyw_field_cid:
    case rozofs_scan_keyw_field_sid:
    case rozofs_scan_keyw_field_sidrange:
      switch(comp) {
        case rozofs_scan_keyw_comparator_eq:
        case rozofs_scan_keyw_comparator_ne:
          return 1;
          break;
        default:
          break;                 
      }
      break;       
             
    default:
      break;    
  }
  usage("%s and %s is not a valid comparison for regular files",rozofs_scan_keyw_e2String(field_name),rozofs_scan_keyw_e2String(comp));
  return 0;           
}
/*
**_______________________________________________________________________
** Check the input test is valid for symbolic links
**
** @param e                   N.S
** @param inode_attr_p        N.S
** @param field_name          Name of the field to compare 
** @param field_value         Value to compare the field to  
** @param comp                comparator to apply  
**
** @retaval 1 on success / 0 on failure
**_______________________________________________________________________
*/
int rozofs_scan_validate_one_field_slink(
                  export_t                  * NS1, 
                  void                      * NS2, 
                  rozofs_scan_keyw_e          field_name,
                  rozofs_scan_field_value_u * field_value,
                  rozofs_range_t            * field_range, 
                  rozofs_scan_keyw_e          comp) { 
  switch(field_name) {

    /*______________________________________
    ** eq
    */
    case rozofs_scan_keyw_field_pfid:
      if (comp == rozofs_scan_keyw_comparator_eq) {
        return 1;     
      }
      break;
      
    /*______________________________________
    ** eq, ge, regex
    */
    case rozofs_scan_keyw_field_fname:
    case rozofs_scan_keyw_field_parent:
      switch(comp) {
        case rozofs_scan_keyw_comparator_eq:  
        case rozofs_scan_keyw_comparator_ge:   
        case rozofs_scan_keyw_comparator_regex:
          return 1;             
          break;
        default:  
         break;                 
      }
      break;      
     
    /*______________________________________
    ** lt, le, eq, gt, ge, ne
    */      
    case rozofs_scan_keyw_field_size:
    case rozofs_scan_keyw_field_cr8time:
    case rozofs_scan_keyw_field_mtime:
    case rozofs_scan_keyw_field_ctime:
    case rozofs_scan_keyw_field_atime:
      switch(comp) {
        case rozofs_scan_keyw_comparator_lt:
        case rozofs_scan_keyw_comparator_le:
        case rozofs_scan_keyw_comparator_eq:
        case rozofs_scan_keyw_comparator_ge:
        case rozofs_scan_keyw_comparator_gt:
        case rozofs_scan_keyw_comparator_ne:
          return 1;
        default:  
          break;                 
      }
      break;        
     
    /*______________________________________
    ** eq, ne
    */      
    case rozofs_scan_keyw_field_uid:
    case rozofs_scan_keyw_field_gid:
    case rozofs_scan_keyw_field_project:
      switch(comp) {
        case rozofs_scan_keyw_comparator_eq:
        case rozofs_scan_keyw_comparator_ne:
          return 1;
          break;
        default:
          break;                 
      }
      break;       
             
    default:
      break;    
  }
  usage("%s and %s is not a valid comparison for symbolic links",rozofs_scan_keyw_e2String(field_name),rozofs_scan_keyw_e2String(comp));
  return 0;           
}
/*
**_______________________________________________________________________
** Check the input test is valid for junk files
**
** @param e                   N.S
** @param inode_attr_p        N.S
** @param field_name          Name of the field to compare 
** @param field_value         Value to compare the field to  
** @param comp                comparator to apply  
**
** @retval 1 on success / 0 on failure
**_______________________________________________________________________
*/
int rozofs_scan_validate_one_field_junk(
                  export_t                  * NS1, 
                  void                      * NS2, 
                  rozofs_scan_keyw_e          field_name,
                  rozofs_scan_field_value_u * field_value, 
                  rozofs_range_t            * field_range,
                  rozofs_scan_keyw_e          comp) { 
  switch(field_name) {

    /*______________________________________
    ** eq
    */
    case rozofs_scan_keyw_field_pfid:
      if (comp == rozofs_scan_keyw_comparator_eq) {
        return 1;     
      }
      break;
           
    /*______________________________________
    ** lt, le, eq, gt, ge, ne
    */      
    case rozofs_scan_keyw_field_size:
      switch(comp) {
        case rozofs_scan_keyw_comparator_lt:
        case rozofs_scan_keyw_comparator_le:
        case rozofs_scan_keyw_comparator_eq:
        case rozofs_scan_keyw_comparator_ge:
        case rozofs_scan_keyw_comparator_gt:
        case rozofs_scan_keyw_comparator_ne:
          return 1;
        default:  
          break;                 
      }
      break;        
     
    /*______________________________________
    ** eq, ne
    */      
    case rozofs_scan_keyw_field_cid:
    case rozofs_scan_keyw_field_sid:
    case rozofs_scan_keyw_field_sidrange:
      switch(comp) {
        case rozofs_scan_keyw_comparator_eq:
        case rozofs_scan_keyw_comparator_ne:
          return 1;
          break;
        default:
          break;                 
      }
      break;       
             
    default:
      break;    
  }
  usage("%s and %s is not a valid comparison for junk files",rozofs_scan_keyw_e2String(field_name),rozofs_scan_keyw_e2String(comp));
  return 0;           
}
/*
**_______________________________________________________________________
** Check the input test is valid for directories
**
** @param e                   N.S
** @param inode_attr_p        N.S
** @param field_name          Name of the field to compare 
** @param field_value         Value to compare the field to  
** @param comp                comparator to apply  
**
** @retaval 1 on success / 0 on failure
**_______________________________________________________________________
*/
int rozofs_scan_validate_one_field_directory(
                  export_t                  * NS1, 
                  void                      * NS2, 
                  rozofs_scan_keyw_e          field_name,
                  rozofs_scan_field_value_u * field_value, 
                  rozofs_range_t            * NS3,
                  rozofs_scan_keyw_e          comp) {
  
  switch(field_name) {

    /*______________________________________
    ** eq
    */
    case rozofs_scan_keyw_field_pfid:
      if (comp == rozofs_scan_keyw_comparator_eq) {
        return 1;     
      }
      break;
      
    /*______________________________________
    ** eq, ge, regex
    */
    case rozofs_scan_keyw_field_fname:
    case rozofs_scan_keyw_field_parent:
    case rozofs_scan_keyw_field_xname:
      switch(comp) {
        case rozofs_scan_keyw_comparator_eq:  
        case rozofs_scan_keyw_comparator_ge:   
        case rozofs_scan_keyw_comparator_regex:
          return 1;             
          break;
        default:  
         break;                 
      }
      break;      
     
    /*______________________________________
    ** lt, le, eq, gt, ge, ne
    */      
    case rozofs_scan_keyw_field_cr8time:
    case rozofs_scan_keyw_field_mtime:
    case rozofs_scan_keyw_field_ctime:
    case rozofs_scan_keyw_field_atime:
    case rozofs_scan_keyw_field_update_time:
    case rozofs_scan_keyw_field_size:
    case rozofs_scan_keyw_field_slave:      
    case rozofs_scan_keyw_field_deleted:
    case rozofs_scan_keyw_field_children:
      switch(comp) {
        case rozofs_scan_keyw_comparator_lt:
        case rozofs_scan_keyw_comparator_le:
        case rozofs_scan_keyw_comparator_eq:
        case rozofs_scan_keyw_comparator_ge:
        case rozofs_scan_keyw_comparator_gt:
        case rozofs_scan_keyw_comparator_ne:
          return 1;
        default:  
          break;                 
      }
      break;        
     
    /*______________________________________
    ** eq, ne
    */      
    case rozofs_scan_keyw_field_uid:
    case rozofs_scan_keyw_field_gid:
    case rozofs_scan_keyw_field_project:
      switch(comp) {
        case rozofs_scan_keyw_comparator_eq:
        case rozofs_scan_keyw_comparator_ne:
          return 1;
          break;
        default:
          break;                 
      }
      break;       
             
    default:
      break;    
  }
  usage("%s and %s is not a valid comparison for directories",rozofs_scan_keyw_e2String(field_name),rozofs_scan_keyw_e2String(comp));
  return 0;           
}
/* 
**__________________________________________________________________
** Evaluate an entry against a condition defined by a node
**
**   @param e                   pointer to exportd data structure
**   @param entry               pointer to the data to check
**   @param node                node describing the condition
**
** @retaval 1 on success / 0 on failure
**__________________________________________________________________
*/
int rozofs_scan_eval_node(export_t                  * e, 
                          void                      * entry,
                          rozofs_scan_node_t        * node) {
  int result;
  int idx;

  if (node==NULL) return 1;
  
  switch(node->type) {
    case rozofs_scan_type_criteria:
      return result = (*rozofs_scan_eval_one_criteria)(e, entry,  node->l.name);
      break;  

    case rozofs_scan_type_field:
      return (*rozofs_scan_eval_one_field)(e, entry,  node->l.name, &node->l.value, &node->l.range, node->l.comp);
      break; 
        
    default:
      break;   
  }
    
  /*
  ** Evaluate every subnode 
  */       
  for (idx=0; idx<node->n.nbNext; idx++) {   
           
    result = rozofs_scan_eval_node(e, entry,  node->n.next[idx]);

    /*
    ** And operator. All sub nodes have to be TRUE for the node to be TRUE
    */
    if (node->n.ope == rozofs_scan_node_ope_and) {
      if (!result) {
        VVERBOSE(" node %d subnode %d/%d : Failed => AND FAILED\n",node->n.nb, idx,node->n.nbNext);
        return 0;
      }  
      VVERBOSE(" node %d subnode %d/%d : OK => AND next node\n",node->n.nb,idx,node->n.nbNext);
      continue;    
    }
    
    /*
    ** Or operator. One TRUE sub node is enough to make the node TRUE
    */ 
    if (result) {
      VVERBOSE(" node %d subnode %d/%d : OK => OR OK\n",node->n.nb,idx,node->n.nbNext);
      return 1;
    }  
    VVERBOSE(" node %d subnode %d/%d : FAILED => OR next node\n",node->n.nb,idx,node->n.nbNext);    
  }    
  return result;
}    
/* 
**__________________________________________________________________
** Validate that the given condition is meaningfull for the scope
** of the search
**
**   @param node                node describing the condition
**
** @retaval 1 on success / 0 on failure
**__________________________________________________________________
*/
int rozofs_scan_validate_node(rozofs_scan_node_t * node) {
  int result;
  int idx;
    
  if (node==NULL) return 1;
  
  switch(node->type) {
    case rozofs_scan_type_criteria:
      return (*rozofs_scan_eval_one_criteria)(NULL, NULL,  node->l.name);
      break;  

    case rozofs_scan_type_field:
      return (*rozofs_scan_eval_one_field)(NULL, NULL,  node->l.name, NULL, NULL, node->l.comp);
      break; 
         
    default:
      break;   
  }  

  /*
  ** Validate every node in the subtree 
  */       
  for (idx=0; idx<node->n.nbNext; idx++) {   
    result = rozofs_scan_validate_node(node->n.next[idx]);
    if (result == 0) return 0;
  }
  return 1;
} 
/* 
**__________________________________________________________________
** Simplify the tree as much as possible 
**
**   @param node                node describing the condition
**__________________________________________________________________
*/
void rozofs_scan_simplify_tree(rozofs_scan_node_t * node) {
  int                   idx, idx2;
  rozofs_scan_node_t  * next;
  int                   count;
    
  if (node==NULL) return;
  if (node->type != rozofs_scan_type_node) return;
  
  if (node->n.nbNext == 1) {
    next = node->n.next[0];
    memcpy(node,next,sizeof(rozofs_scan_node_t));
    free(next);        
    return rozofs_scan_simplify_tree(node);
  }

  for (idx=0; idx<node->n.nbNext; idx++) {
    rozofs_scan_simplify_tree(node->n.next[idx]);
  }  
  
  count = node->n.nbNext;
  for (idx=0; idx<count; idx++) {
    next = node->n.next[idx];
    if ((next->type != rozofs_scan_type_node)||(next->n.ope != node->n.ope)) {
      continue;
    }
    node->n.next[idx] = next->n.next[0];
    for (idx2=1; idx2<next->n.nbNext; idx2++) {
      node->n.next[node->n.nbNext] = next->n.next[idx2];
      node->n.nbNext++;
    } 
    free(next);         
  } 
} 
/*
**_______________________________________________________________________
**
**  Decode acl xattribute
**
** @param acl_p          pointer to the acl extended attribute value
** @param pChar          pointer to the buffer where to write the decoded string
**    
** @retval Number of bytes added to the buffer
**_______________________________________________________________________
*/                
int rozo_scan_decode_acl(char * pChar, struct posix_acl * acl_p) {
  struct posix_acl_entry * acl_e;
  int                      idx;
  char                   * pDisplay = pChar;
  
  acl_e = acl_p->a_entries;
  for (idx=0; idx<acl_p->a_count; idx++,acl_e++) {

    switch(acl_e->e_tag) {
      case ACL_USER_OBJ:
        pDisplay += rozofs_string_append(pDisplay,"user:");
        break;
      case ACL_GROUP_OBJ:
        pDisplay += rozofs_string_append(pDisplay,"group:");
        break;                         
      case ACL_MASK:
        pDisplay += rozofs_string_append(pDisplay,"mask:");
        break;                         
      case ACL_OTHER:
        pDisplay += rozofs_string_append(pDisplay,"other:");
        break;                         
      case ACL_USER:
        pDisplay += rozofs_string_append(pDisplay,"user:");
        pDisplay += rozofs_u32_append(pDisplay,acl_e->e_id);
        break;                         
      case ACL_GROUP:
        pDisplay += rozofs_string_append(pDisplay,"group:");
        pDisplay += rozofs_u32_append(pDisplay,acl_e->e_id);
        break;                         
      default:
        pDisplay += rozofs_u32_append(pDisplay,acl_e->e_tag);
	pDisplay += rozofs_string_append(pDisplay,"?:");
        pDisplay += rozofs_u32_append(pDisplay,acl_e->e_id);
    }

    if (acl_e->e_perm & ACL_READ) {
      pDisplay += rozofs_string_append(pDisplay,":r");
    }
    else {
      pDisplay += rozofs_string_append(pDisplay,":-");
    }  

    if (acl_e->e_perm & ACL_WRITE) {
      pDisplay += rozofs_string_append(pDisplay,"w");
    }
    else {
      pDisplay += rozofs_string_append(pDisplay,"-");
    }    

    if (acl_e->e_perm & ACL_EXECUTE) {
      pDisplay += rozofs_string_append(pDisplay,"x ");
    }
    else {
      pDisplay += rozofs_string_append(pDisplay,"- ");
    }  
  }
  return (pDisplay-pChar);
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
  export_t   * e = exportd;
  ext_mattr_t *slave_p;;
  int          ishybrid;
  int          isaging;
  uint32_t     hybridSize;
  uint32_t     slaveNb = -1;
  uint32_t     slaveSize;
  char         fidString[40];      
  int          result=0;
  
  nb_scanned_entries++;
 
  /*
  ** Only process REG, DIR and SLINK
  */
  if ((!S_ISREG(inode_p->s.attrs.mode))
  &&  (!S_ISDIR(inode_p->s.attrs.mode))
  &&  (!S_ISLNK(inode_p->s.attrs.mode))) {
    return 0;
  }  
   
  /*
  ** Check for symbolic link scope
  */
  if (S_ISLNK(inode_p->s.attrs.mode)) {
    if (just_this_fid) {
      rozofs_scan_scope = rozofs_scan_scope_symbolic_link;
    }
    if (rozofs_scan_scope != rozofs_scan_scope_symbolic_link) {
      return 0;
    }  
  }
  else {
    if (rozofs_scan_scope == rozofs_scan_scope_symbolic_link) {
      return 0;   
    }   
  }
  
  /*
  ** Reset inode xattribute number on each inode
  */
  rozo_scan_xattr_nb = -1;
  
  nb_scanned_entries_in_scope++;
     
  rozofs_fid_append(fidString,inode_p->s.attrs.fid);
  VVERBOSE("\nVISIT: %s\n",fidString);
  if (just_this_fid) {
    rozofs_inode_t * inode1 = (rozofs_inode_t *) this_fid;
    rozofs_inode_t * inode2 = (rozofs_inode_t *) inode_p->s.attrs.fid;

    result = 1;
  }
  else {
    result = rozofs_scan_eval_node (e, inode_attr_p, upNode);
  }  
  VVERBOSE("VISIT: %s %s\n",fidString, result?"OK":"Failed");
  if (!result) return 0;
   
  /*
  ** This inode is valid
  */
  nb_matched_entries++;
  if (S_ISREG(inode_p->s.attrs.mode)) { 
    sum_file_size    += inode_p->s.attrs.size;
    sum_file_blocks += ((inode_p->s.attrs.size + ROZOFS_BSIZE_BYTES(e->bsize) - 1)/ROZOFS_BSIZE_BYTES(e->bsize));
  }
  if (S_ISDIR(inode_p->s.attrs.mode)) { 
    ext_dir_mattr_t * stats_attr_p;
    stats_attr_p = (ext_dir_mattr_t *)&inode_p->s.attrs.sids[0];
    sum_file_size    += stats_attr_p->s.nb_bytes;
    sum_sub_dir      += (inode_p->s.attrs.nlink-2);
    sum_sub_files    += (inode_p->s.attrs.children+2-inode_p->s.attrs.nlink);
  }    
  switch(name_format) {

    case name_format_fid:
      pChar = fullName;    
      pChar += sprintf(pChar,"./@rozofs_uuid@%s",fidString);
      pChar = fullName;    
      break;

    case name_format_relative:
      pChar = rozo_get_path(exportd,inode_attr_p, fullName,sizeof(fullName),1);
      break;

    case name_format_none:
      return 1;
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
  ** Linux privileges
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
    if (rozo_scan_has_extended_attr(inode_p)) {
      pDisplay += rozofs_string_append(pDisplay,"\"YES\"");
      if (display_xattr) {
        int    xattr_idx;
        int    xattr_length;
        char * pXname;

        START_SUBARRAY(xattr_list);
         
        /*
        ** Get xattribute list
        */ 
        rozo_scan_read_xattribute_list(e,inode_attr_p);
        
        /*
        ** Loop on extended attributes
        */
        for (xattr_idx=0; xattr_idx< rozo_scan_xattr_nb; xattr_idx++) {

          SUBARRAY_START_ELEMENT();
          FIRST_QUOTED_NAME(xattr_name);
          
          pXname = rozo_scan_xattr_table[xattr_idx];

          pDisplay += rozofs_string_append(pDisplay,pXname);
          pDisplay += rozofs_string_append(pDisplay,"\"");   
    
          xattr_length = rozo_scan_read_xattribute_value(pXname);
          
          if (xattr_length > 0) {
            NEW_QUOTED_NAME_NEW_LINE(xattr_value);
            if (rozofs_is_printable(rozofs_scan_xattr_value_buffer,xattr_length)){
              rozofs_scan_xattr_value_buffer[xattr_length] = 0;
              pDisplay += rozofs_string_append(pDisplay,rozofs_scan_xattr_value_buffer);
              pDisplay += rozofs_string_append(pDisplay,"\"");                 
            }
            else {
              pDisplay += rozofs_hexa_append(pDisplay,rozofs_scan_xattr_value_buffer,xattr_length); 
              pDisplay += rozofs_string_append(pDisplay,"\"");                 

              /*
              ** Decode ACL
              */ 
              if ((strcmp(pXname,POSIX_ACL_XATTR_ACCESS)==0) || (strcmp(pXname,POSIX_ACL_XATTR_DEFAULT)==0)) {
                struct posix_acl       * acl_p;
                acl_p = posix_acl_from_xattr(rozofs_scan_xattr_value_buffer, xattr_length);
                if (acl_p != NULL) {
                  NEW_QUOTED_NAME_NEW_LINE(acl);
                  pDisplay += rozo_scan_decode_acl(pDisplay,acl_p);
                  pDisplay += rozofs_string_append(pDisplay,"\"");                               
                }
              }
            }   
          }
          
          SUBARRAY_STOP_ELEMENT();
        }
        STOP_SUBARRAY();

      }
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

    IF_DISPLAY(display_striping) {
      
      get_directory_striping_info(inode_p, &ishybrid, &hybridSize, &slaveNb, &slaveSize, &isaging);
      
      NEW_FIELD(hybdrid); 
      if (ishybrid) {
        pDisplay += rozofs_string_append(pDisplay,"\"YES\"");
        NEW_FIELD(hybdrid_size); 
        pDisplay += rozofs_u64_append(pDisplay,hybridSize);
      }
      else {
        pDisplay += rozofs_string_append(pDisplay,"\"NO\"");
      }
      NEW_FIELD(aging); 
      if (isaging) {
        pDisplay += rozofs_string_append(pDisplay,"\"YES\"");
      }
      else {
        pDisplay += rozofs_string_append(pDisplay,"\"NO\"");
      }
      
      NEW_FIELD(slaves); 
      pDisplay += rozofs_u32_append(pDisplay,slaveNb);   
      NEW_FIELD(stripe_size);       
      pDisplay += rozofs_u32_append(pDisplay,slaveSize); 
    }        
  }

  /*
  ** Regular file
  */
  else if (S_ISREG(inode_p->s.attrs.mode)) {
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
       
    IF_DISPLAY(display_striping) {
      if (inode_p->s.multi_desc.byte != 0){
        int strip_factor;
        int stripe_size;
        strip_factor = rozofs_get_striping_factor(&inode_p->s.multi_desc); 
        stripe_size   = rozofs_get_striping_size(&inode_p->s.multi_desc); 
        NEW_FIELD(hybdrid); 
        if (inode_p->s.hybrid_desc.s.no_hybrid == 0) {
          pDisplay += rozofs_string_append(pDisplay,"\"YES\"");
          NEW_FIELD(hybdrid_size); 
          pDisplay += rozofs_u64_append(pDisplay,rozofs_get_hybrid_size(&inode_p->s.multi_desc,&inode_p->s.hybrid_desc));
        }
        else {
          pDisplay += rozofs_string_append(pDisplay,"\"NO\"");
        }
        NEW_FIELD(aging); 
        if (inode_p->s.bitfield1 & ROZOFS_BITFIELD1_AGING) {
          pDisplay += rozofs_string_append(pDisplay,"\"YES\"");
        }
        else {
          pDisplay += rozofs_string_append(pDisplay,"\"NO\"");
        }
        NEW_FIELD(slaves); 
        pDisplay += rozofs_u32_append(pDisplay,strip_factor);   
        NEW_FIELD(stripe_size);       
        pDisplay += rozofs_u32_append(pDisplay,stripe_size); 
      }   
      else {
        NEW_FIELD(hybdrid);       
        pDisplay += rozofs_string_append(pDisplay,"\"NO\"");
        NEW_FIELD(slaves); 
        pDisplay += rozofs_u32_append(pDisplay,0);   
      } 
    }    
    
    
    IF_DISPLAY(display_distrib) {

      START_SUBARRAY(distribution);
      /*
      ** Display distribution in inode for hybrid as well as non multifile mode
      */
      if ((inode_p->s.multi_desc.byte == 0)||(inode_p->s.hybrid_desc.s.no_hybrid == 0)){
        slave_p = inode_p;
        SUBARRAY_START_ELEMENT();
        FIRST_QUOTED_NAME(storage_fid);
        pDisplay += rozofs_fid_append(pDisplay,slave_p->s.attrs.fid);
        pDisplay += rozofs_string_append(pDisplay,"\"");   
        NEW_NAME(cid)
        pDisplay += rozofs_u32_append(pDisplay,slave_p->s.attrs.cid);
        NEW_NAME(sid)
        pDisplay += rozofs_string_append(pDisplay,"[");
        int sid_idx;
        for (sid_idx=0; sid_idx<ROZOFS_SAFE_MAX_STORCLI; sid_idx++) {
          if (slave_p->s.attrs.sids[sid_idx] == 0) break;
          if (sid_idx != 0) pDisplay += rozofs_string_append(pDisplay,",");
          pDisplay += rozofs_u32_append(pDisplay,slave_p->s.attrs.sids[sid_idx]);
        }
        pDisplay += rozofs_string_append(pDisplay,"]");
        SUBARRAY_STOP_ELEMENT();   
      } 
      /*
      ** Display distribution of slave inodes for multifile mode
      */ 
      if (inode_p->s.multi_desc.byte != 0) {
        int idx; 
        int nbSlaves = rozofs_get_striping_factor(&inode_p->s.multi_desc); 
        for (idx=1; idx <= nbSlaves;  idx++) {
          slave_p = inode_p + idx;
          SUBARRAY_START_ELEMENT();
          FIRST_QUOTED_NAME(storage_fid)
          pDisplay += rozofs_fid_append(pDisplay,slave_p->s.attrs.fid);
          pDisplay += rozofs_string_append(pDisplay,"\"");   
           
          NEW_NAME(cid)
          pDisplay += rozofs_u32_append(pDisplay,slave_p->s.attrs.cid);

          NEW_NAME(sid)
          pDisplay += rozofs_string_append(pDisplay,"[");
          int sid_idx;
          for (sid_idx=0; sid_idx<ROZOFS_SAFE_MAX_STORCLI; sid_idx++) {
            if (slave_p->s.attrs.sids[sid_idx] == 0) break;
            if (sid_idx != 0) pDisplay += rozofs_string_append(pDisplay,",");
            pDisplay += rozofs_u32_append(pDisplay,slave_p->s.attrs.sids[sid_idx]);
          }
          pDisplay += rozofs_string_append(pDisplay,"]");
          SUBARRAY_STOP_ELEMENT();   
        }  
      }
      STOP_SUBARRAY();
    }     
  } 
  /*
  ** Symbolic link
  */
  else if (S_ISLNK(inode_p->s.attrs.mode)) {
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
    IF_DISPLAY((display_distrib)&&(inode_p->s.i_link_name)) {
       rozofs_inode_t slinkFid;
       fid_t          fid;
       slinkFid.fid[1] = inode_p->s.i_link_name;
       slinkFid.fid[0] = 0;
       memcpy(fid,&slinkFid.fid[0],sizeof(fid_t));
       NEW_QUOTED_FIELD(lfid);  
       pDisplay += rozofs_fid_append(pDisplay,inode_p->s.attrs.fid);
       pDisplay += rozofs_string_append(pDisplay,"\"");   
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
  
  IF_DISPLAY(display_mtime) {
    IF_DISPLAY_HUMAN(display_mtime) {
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
  
  if ((just_this_fid) || (nb_matched_entries >= max_display)) {
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
  rmfentry_disk_t * rmentry = inode_attr_p;
  char              fidString[40]; 
  int               result;     
  export_t        * e = exportd;

  nb_scanned_entries++;
  nb_scanned_entries_in_scope++;

  rozofs_fid_append(fidString,rmentry->trash_inode);
  VVERBOSE("\nVISIT: %s\n",fidString);
  result = rozofs_scan_eval_node (e, inode_attr_p, upNode);
  VVERBOSE("VISIT: %s %s\n",fidString, result?"OK":"Failed");
  if (!result) return 0;
             
  /*
  ** This rmfentry is valid
  */
  nb_matched_entries++;
  if (name_format != name_format_none) {
    
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

    printf("%s",display_buffer);
  }
  
  if (nb_matched_entries >= max_display) {
    rozo_lib_stop_var = 1;
  }    

  return 1;
}

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
**  Scan a string containing an unsigned long integer on 64 bits
**
** @param str  : the string to scan
**   
** @retval -1 when bad string is given
** @retval the unsigned long integer value
*/
static inline uint64_t rozofs_scan_parse_u64(char * str) {
  uint64_t val;
  int      ret;
  int      charCount;
  
  ret = sscanf(str,"%llu%n",(long long unsigned int *)&val, &charCount);
  if (ret != 1) {
    return LONG_VALUE_UNDEF;
  }  
    
  str += charCount;
  rozofs_scan_current_arg_char2skip += charCount;
  
  if (*str!=0) rozofs_scan_parse_same_arg = 1;
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
volume_config_t * get_volume_config(uint8_t vid) {
  list_t          * v;
  volume_config_t * vconfig;

  list_for_each_forward(v, &exportd_config.volumes) {

    vconfig = list_entry(v, volume_config_t, list);
    if (vconfig->vid == vid) return vconfig;   
  }
  return NULL;
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
  volume_config_t * vconfig;
  
  list_for_each_forward(e, &exportd_config.exports) {

    econfig = list_entry(e, export_config_t, list);
    if (econfig->eid == eid) {
      /*
      ** When striping factor is not initialize at export level
      ** Get it at volume level
      */
      if (econfig->stripping.factor == 255) {
        vconfig = get_volume_config(econfig->vid);
        if (vconfig != NULL) {
          memcpy(&econfig->stripping,&vconfig->stripping, sizeof(vconfig->stripping));
        }
      }
      return econfig;   
    }  
  }
  return NULL;
}  

/*
** Take some seconds of tolerance because of eventual write back 
*/
#define ROZOFS_SCAN_TRK_CHECK_TOLERANCE     4
/*
**_______________________________________________________________________
** Check whether the tracking file can match the given date criteria
**   
**  @param  e         export context
**  @param  inode     Tracking file inode
**  @param  param     eport context
**    
**  @retval 0 = do not read this file / 1 = read this file
*/
int rozofs_check_trk_file_date (void *export,void *inode,void *param) {
  int                  idx;
  rozofs_scan_node_t * leaf;    
  ext_mattr_t        * inode_p = inode;
  uint64_t             date2Check;
  
  nb_checked_tracking_files++;
  
  /*
  ** When it is not requested to optimize tracking file walking thanks to date criteria
  ** just return the file has to be processed 
  */
  if (skip_tracking_file == 0) return 1;

  /*
  ** If some date comparisons are determinant, some tracking files may be skipped
  */
  for (idx=0; idx<rozofs_scan_date_field_count; idx++) {
  
    leaf = dateField[idx];       
    if (leaf==NULL) continue;
    
    date2Check = leaf->l.value.u64;
  
    switch(leaf->l.name) {
    
      case rozofs_scan_keyw_field_cr8time:
      case rozofs_scan_keyw_field_mtime:
      case rozofs_scan_keyw_field_ctime:
        switch(leaf->l.comp) {
        
          case rozofs_scan_keyw_comparator_lt:
          case rozofs_scan_keyw_comparator_le:
            /*                      Trk cr8                   Trk mtime
            **          ------------+-------------------------+----------------------> Time
            ** date2Check   <-*     |         <-*             |    <-*
            ** Trk         NO WAY   |         OK              |      OK    
            **
            ** The file has been created/modified before date2Check. 
            ** => its tracking file must have been created before this date.
            */
            if (date2Check < (inode_p->s.cr8time-ROZOFS_SCAN_TRK_CHECK_TOLERANCE)) { 
              /*
              ** The tracking file wa created after the inode
              */     
              nb_skipped_tracking_files++;      
              return 0;
            }      
            continue;
           
          case rozofs_scan_keyw_comparator_eq:
            /*                      Trk cr8                   Trk mtime
            **          ------------+-------------------------+----------------------> Time
            ** date2Check    *      |         *               |    *
            ** Trk         NO WAY   |         OK              |    NO WAY     
            **
            ** The file has been created/modified at date2Check. 
            ** => its tracking file must have a last modification date after this date.
            **    and a cration date before this date.
            */
            if (date2Check > (inode_p->s.attrs.mtime+ROZOFS_SCAN_TRK_CHECK_TOLERANCE)) { 
              /*
              ** The tracking file last modification is older that the inode creation date
              */     
              nb_skipped_tracking_files++;      
              return 0;
            }
            if (date2Check < (inode_p->s.cr8time-ROZOFS_SCAN_TRK_CHECK_TOLERANCE)) { 
              /*
              ** The tracking file last modification is older that the inode creation date
              */     
              nb_skipped_tracking_files++;      
              return 0;
            }            
            continue;            
          
          case rozofs_scan_keyw_comparator_ge:      
          case rozofs_scan_keyw_comparator_gt:
            /*                      Trk cr8                   Trk mtime
            **          ------------+-------------------------+----------------------> Time
            ** date2Check    *->    |         *->             |    *->
            ** Trk           OK     |         OK              |    NO WAY     
            **
            ** The file has been created after date2Check. 
            ** => its tracking file must have a last modification date before this date.
            */
            if (date2Check > (inode_p->s.attrs.mtime+ROZOFS_SCAN_TRK_CHECK_TOLERANCE)) { 
              /*
              ** The tracking file last modification is older that the inode creation date
              */     
              nb_skipped_tracking_files++;      
              return 0;
            }
            continue;
            
          default:
            severe("Unexpected time comparator %s %s", 
                    rozofs_scan_keyw_e2String(leaf->l.name),
                    rozofs_scan_keyw_e2String(leaf->l.comp));
               
            return 1;  

        }
        break;
 
      default:
        break;
    }
  }  
  /*
  ** All date controls are OK for this tracking file
  */
  return 1; 
}
/*
**_______________________________________________________________________
** Parse requested output format
**   
**  @param  fmt : The output format
**    
**  @retval 0 = do not read this file / 1 = read this file
*/
#define slip(X) { p+=(X);rozofs_scan_current_arg_char2skip+=(X);} 

#define NEXT(p) { \
  while((*p!=',')&&(*p!=0)&&(*p!=' ')) slip(1);\
  while (*p==',') {\
    slip(1);\
  }  \
  if ((*p==0)||(*p==' ')) {\
    return 0;\
  } \
  continue;\
}     
int rozofs_parse_output_format(char * fmt, int argc, char * argv[]) {
  char * p = fmt;
  
  while (1) {
  
    if (strncmp(p, "line", 4)==0) {
      slip(4);
      if (*p=='=') slip(1);
      if (*p==0) {
        rozofs_show_error("Bad output format for \"line\"\" : no line value.");           
      }
      if (sscanf(p,"%d",&entry_per_line)!=1) {
        rozofs_show_error("Bad output format for \"line\" : integer value required");     
      } 
      if ((entry_per_line<=0)||(entry_per_line>50)) {
        rozofs_show_error("Bad output format for \"line\" : value must be within [1..50] ");     
      }
      NEXT(p);
    }
    
    if (strncmp(p, "count", 5)==0) {
      slip(5);
      if (*p=='=') slip(1);
      if (*p==0) {
        rozofs_show_error("Bad output format for \"count\"\" : no count value.");           
      }
      if (sscanf(p,"%llu",(long long unsigned int*)&max_display)!=1) {
        rozofs_show_error("Bad output format for \"count\" : integer value required");     
      } 
      NEXT(p);
    }
    if (strncmp(p, "none", 4)==0) {
      name_format = name_format_none;
      /*
      ** Always display stat when node is set
      */
      display_stat = DO_DISPLAY;
      NEXT(p);
    }   
    if (strncmp(p, "stat", 4)==0) {
      display_stat = DO_DISPLAY;
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
      display_mtime = DO_DISPLAY;
      NEXT(p);
    }              

    if (strncmp(p, "smtime", 3)==0) {
      display_mtime = DO_DISPLAY;
      NEXT(p);
    }              

    if (strncmp(p, "hmod", 3)==0) {
      display_mtime = DO_HUMAN_DISPLAY;
      NEXT(p);
    }              

    if (strncmp(p, "hmtime", 3)==0) {
      display_mtime = DO_HUMAN_DISPLAY;
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
      slip(4);
      char * pSep = separator;
      while((*p!= ' ') && (*p!= 0) &&(*p!=',')) {
        *pSep++ = *p++; rozofs_scan_current_arg_char2skip++;
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
      /*
      ** Always display stat in JSON
      */
      display_stat = DO_DISPLAY;
      NEXT(p);
    }         
    
    rozofs_show_error("Unexpected output format.");           

  }  
  return 0;
}
/* 
**__________________________________________________________________
** Returns next string to parse if any
** Input arguments are parsed one by one. But one argument may contain several
** rozo_scan keywords and values without ' '. One has to memorize during the
** argument parsing:
** - which is the crrently parsed argument.
** - whether this argument is completly parsed or not.
** - which is the current offset of the next part to parse in the current argument

** @param argc     Number of input arguments
** @param argv[]   Array of arguments
**
** @retval pointer to the next part to parse or NULL
**__________________________________________________________________
*/ 
char * rozofs_scan_get_next_input_pointer(int argc, char *argv[]) {
  char * pChar;
  
  /*
  ** The same agument has to be parsed since it still
  ** contains information
  */
  if (rozofs_scan_parse_same_arg) {
    /*
    ** A priori the whole argument will be parsed 
    ** and next time we will step to the next argument
    */
    rozofs_scan_parse_same_arg = 0;
    /*
    ** Get the current argument
    */
    pChar = argv[rozofs_scan_current_arg_idx];
    /*
    ** Skip already processed characters in this argument
    */
    pChar += rozofs_scan_current_arg_char2skip;
    return pChar;
  }
  
  /*
  ** Step to the next argument
  */
  rozofs_scan_current_arg_idx++;
  /*
  ** No more argument
  */
  if (rozofs_scan_current_arg_idx == argc) {
    return NULL;
  }  
  /*
  ** Reset characters to skip within this new argument
  */
  rozofs_scan_parse_same_arg        = 0;
  rozofs_scan_current_arg_char2skip = 0;  
  return argv[rozofs_scan_current_arg_idx]; 
}
/* 
**__________________________________________________________________
** This macro compares the current input argument pointer (named pt) against 
** a given string (named str). In case of success it moves the pointer
** to the argument forward of the size of the string, and returns
** code as a result
**
** @param str   The string to match
** @param code  The code to return 
**__________________________________________________________________
*/ 
#define rozofs_scan_check_against(str,code) if (strncmp(pt,str,strlen(str))==0) rozofs_scan_ret_arg_len(strlen(str),argLen,code);   
/* 
**__________________________________________________________________
** In case the current input argument length (argLen) is different from
** the length processed within it (readLen), the same argument must be 
** parsed again next time,  but we have to skip readLen characters forward 
** in the argument.
** The value "code" must be returned.
**
** @param readLen The size processed in the argument
** @param argLen  The argument len
** @param code    The code to return 
**__________________________________________________________________
*/ 
#define rozofs_scan_ret_arg_len(readLen,argLen,code) {if ((argLen)!=(readLen)) {rozofs_scan_parse_same_arg = 1;rozofs_scan_current_arg_char2skip += (int)(readLen);} return (code);}
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
  int argLen = strlen(date);
  int charCount;
  char * pChar = date; 

  ret = sscanf(pChar,"%d-%d-%d%n",&year,&month,&day,&charCount);
  if (ret != 3) {
    return LONG_VALUE_UNDEF;
  }
  pChar += charCount;
  
  ret = sscanf(pChar,"-%d%n",&hour,&charCount);
  if (ret!=1) {
    rozofs_scan_ret_arg_len(pChar-date,argLen, rozofs_date_in_seconds(year,month,day,0,0,0));
  }
  pChar += charCount;

  ret = sscanf(pChar,":%d%n",&minute,&charCount);
  if (ret!=1) {
    rozofs_scan_ret_arg_len(pChar-date,argLen, rozofs_date_in_seconds(year,month,day,hour,0,0));
  }
  pChar += charCount;

  ret = sscanf(pChar,":%d%n",&sec,&charCount);
  if (ret!=1) {
    rozofs_scan_ret_arg_len(pChar-date,argLen, rozofs_date_in_seconds(year,month,day,hour,minute,0));
  }
  pChar += charCount;

  rozofs_scan_ret_arg_len(pChar-date,argLen, rozofs_date_in_seconds(year,month,day,hour,minute,sec));    
}
/* 
**__________________________________________________________________
** Decode the value in argument. 
** The whole argument may not be consumed during the call.
** In this case we have to remeber that the same argument has
** to be processed again, and we have to skip the aleady processed
** characters
**
** @param argument  Pointer the string to parse
** 
** @retval the identification of the read keyword
*/ 
rozofs_scan_keyw_e rozofs_scan_decode_argument(char * argument) {
  char     * pt = argument;
  int        argLen;
  
  /*
  ** Skip - and -- that can still be used for compatibility with
  ** the old rozo_scan syntax
  */
  while (*pt == ' ') { pt++; rozofs_scan_current_arg_char2skip++;}
  if (*pt == '-') { pt++; rozofs_scan_current_arg_char2skip++;}
  if (*pt == '-') { pt++; rozofs_scan_current_arg_char2skip++;}
  argLen = strlen(pt);
  if (argLen == 0) return rozofs_scan_keyw_input_error;
    
  switch(pt[0]) {

    /*
    ** comparator: !=    old[--ne]
    */  
    case '!' : 
      if (argLen == 1) return rozofs_scan_keyw_input_error;
      if (pt[1] == '=') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_comparator_ne);
      return rozofs_scan_keyw_input_error;
      break;

    /*
    ** separator: ([{    open brackets
    */    
    case '[' : 
    case '(' : 
    case '{' : 
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_separator_open);
      break;

    /*
    ** separator: )]}    close brackets
    */          
    case ']' : 
    case ')' : 
    case '}' : 
       rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_separator_close);
      break;

    /*
    ** comparator: <     old[--lt]
    ** comparator: <=    old[--le]
    */  
    case '<' : 
      if (argLen == 1) return rozofs_scan_keyw_comparator_lt;
      if (pt[1] == '=') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_comparator_le);  
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_comparator_lt);
      break;

    /*
    ** comparator: =, ==     old[--eq]
    ** comparator: =!        old[--ne]
    ** comparator: =>        old[--ge]
    ** comparator: =<        old[--le]
    */  
    case '=' : 
      if (argLen == 1) return rozofs_scan_keyw_comparator_eq;
      if (pt[1] == '=') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_comparator_eq);
      if (pt[1] == '!') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_comparator_ne);
      if (pt[1] == '>') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_comparator_ge);
      if (pt[1] == '<') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_comparator_le);       
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_comparator_eq); 
      break;

    /*
    ** comparator: >         old[--gt]
    ** comparator: >=        old[--ge]
    */
    case '>' : 
      if (argLen == 1) return rozofs_scan_keyw_comparator_gt;
      if (pt[1] == '=') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_comparator_ge);
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_comparator_gt);
      break;
      
    /*
    ** criteria: Gx,Gw,Gr    old[--G<x|w|r>]
    ** criteria: Gnx,Gnw,Gnr old[--Gn<x|w|r]
    */  
    case 'G' : 
      if (argLen == 1) return rozofs_scan_keyw_input_error;
      if (pt[1] == 'n') {
        if (argLen < 3) return rozofs_scan_keyw_input_error;
        if (pt[2] == 'x') rozofs_scan_ret_arg_len(3,argLen,rozofs_scan_keyw_criteria_priv_group_not_x);
        if (pt[2] == 'w') rozofs_scan_ret_arg_len(3,argLen,rozofs_scan_keyw_criteria_priv_group_not_w);
        if (pt[2] == 'r') rozofs_scan_ret_arg_len(3,argLen,rozofs_scan_keyw_criteria_priv_group_not_r);
        return rozofs_scan_keyw_input_error;
      } 
      if (pt[1] == 'x') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_criteria_priv_group_x);
      if (pt[1] == 'w') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_criteria_priv_group_w);
      if (pt[1] == 'r') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_criteria_priv_group_r);
      return rozofs_scan_keyw_input_error;
      break;

    /*
    ** criteria: Ox,Ow,Or    old[--O<x|w|r>]
    ** criteria: Onx,Onw,Onr old[--On<x|w|r]
    */  
    case 'O' : 
      if (argLen == 1) return rozofs_scan_keyw_input_error;
      if (pt[1] == 'n') {
        if (argLen < 3) return rozofs_scan_keyw_input_error;
        if (pt[2] == 'x') rozofs_scan_ret_arg_len(3,argLen,rozofs_scan_keyw_criteria_priv_other_not_x);
        if (pt[2] == 'w') rozofs_scan_ret_arg_len(3,argLen,rozofs_scan_keyw_criteria_priv_other_not_w);
        if (pt[2] == 'r') rozofs_scan_ret_arg_len(3,argLen,rozofs_scan_keyw_criteria_priv_other_not_r);
        return rozofs_scan_keyw_input_error;
      } 
      if (pt[1] == 'x') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_criteria_priv_other_x);
      if (pt[1] == 'w') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_criteria_priv_other_w);
      if (pt[1] == 'r') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_criteria_priv_other_r);
      return rozofs_scan_keyw_input_error;
      break;

    /*
    ** field: P    old[-P,--project]
    */  
    case 'P' : 
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_field_project);
      break;
       
    /*  
    ** scope:  S             old[-S,--slink]
    */
    case 'S':
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_scope_slink);
      break;
      
    /*  
    ** criteria:  not(rash)           old[-T,--notrash]
    */
    case 'T':
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_criteria_not_in_trash);
      break;
      
    /*
    ** criteria: Ux,Uw,Ur    old[--U<x|w|r>]
    ** criteria: Unx,Unw,Unr old[--Un<x|w|r]
    */  
    case 'U' : 
      if (argLen == 1) return rozofs_scan_keyw_input_error;
      if (pt[1] == 'n') {
        if (argLen < 3) return rozofs_scan_keyw_input_error;
        if (pt[2] == 'x') rozofs_scan_ret_arg_len(3,argLen,rozofs_scan_keyw_criteria_priv_user_not_x);
        if (pt[2] == 'w') rozofs_scan_ret_arg_len(3,argLen,rozofs_scan_keyw_criteria_priv_user_not_w);
        if (pt[2] == 'r') rozofs_scan_ret_arg_len(3,argLen,rozofs_scan_keyw_criteria_priv_user_not_r);
        return rozofs_scan_keyw_input_error;
      } 
      if (pt[1] == 'x') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_criteria_priv_user_x);
      if (pt[1] == 'w') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_criteria_priv_user_w);
      if (pt[1] == 'r') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_criteria_priv_user_r);
      return rozofs_scan_keyw_input_error;
      break;

    /*  
    ** criteria:  X           old[-X,--noxattr]
    */
    case 'X':
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_criteria_has_no_xattr);
      break;
     
    /*
    ** field:    atime          old[--hatime] old[--satime] old[--atime]
    ** criteria: aging          
    ** operator: and
    */  
    case 'a' : 
      rozofs_scan_check_against("and",rozofs_scan_keyw_operator_and);
      rozofs_scan_check_against("atime",rozofs_scan_keyw_field_atime);  
      rozofs_scan_check_against("aging",rozofs_scan_keyw_criteria_is_aging);  
      return rozofs_scan_keyw_input_error;
      break;

    case 'b' : 
      return rozofs_scan_keyw_input_error;
      break;
      
    /*
    ** field:    c, cid          old[-c, --cid]
    ** field:    children        old[--children]
    ** field:    cr8             old[--hcr8] old[--scr8] old[--cr8]
    ** field:    ctime           old[--hctime] old[--sctime] old[--ctime]
    ** argument: config          old[--config]
    */  
    case 'c' :
      if (argLen == 1) return rozofs_scan_keyw_field_cid;
      rozofs_scan_check_against("cid",rozofs_scan_keyw_field_cid);   
      rozofs_scan_check_against("children",rozofs_scan_keyw_field_children);   
      rozofs_scan_check_against("cr8",rozofs_scan_keyw_field_cr8time);   
      rozofs_scan_check_against("config",rozofs_scan_keyw_argument_config);   
      rozofs_scan_check_against("ctime",rozofs_scan_keyw_field_ctime);   
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_field_cid);
      break;
      
    /*
    ** option: d, dir        old[-d, --dir]
    ** field:  deleted       old[--deleted]
    */  
    case 'd' :
      if (argLen == 1) return rozofs_scan_keyw_scope_dir;
      rozofs_scan_check_against("dir",rozofs_scan_keyw_scope_dir);
      rozofs_scan_check_against("deleted",rozofs_scan_keyw_field_deleted);   
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_scope_dir);
      break;

    /*
    ** comparator: eq        old[--eq]
    ** argument:   e,eid     old[-e,--eid]
    */  
    case 'e' :      
      if (argLen == 1) return rozofs_scan_keyw_argument_eid;
      if (pt[1] == 'q') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_comparator_eq);        
      rozofs_scan_check_against("eid",rozofs_scan_keyw_argument_eid);
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_argument_eid);
      break;

    case 'f' :
      rozofs_scan_check_against("fid",rozofs_scan_keyw_field_fid);     
      return rozofs_scan_keyw_input_error;

    /*
    ** comparator:  ge             old[--ge]
    ** comparator:  gt             old[--gt]
    ** field:       g, gid         old[-g, --gid]
    */
    case 'g' : 
      if (argLen == 1) return rozofs_scan_keyw_field_gid;
      if (pt[1] == 'e') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_comparator_ge);
      if (pt[1] == 't') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_comparator_gt);
      rozofs_scan_check_against("gid",rozofs_scan_keyw_field_gid);   
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_field_gid);
      break;
      
    /*
    ** option:   h, help        old[-h, --help]
    ** criteria: hybrid         old[--hybrid]
    ** field:    hcr8           old[--hcr8]
    ** field:    hctime         old[--hctime]
    ** field:    hmod           old[--hmod]
    ** field:    hatime         old[--hatime]
    ** field:    hupdate        old[--hupdate]
    */
    case 'h' :
      if (argLen == 1) return rozofs_scan_keyw_option_help;
      rozofs_scan_check_against("hybrid",rozofs_scan_keyw_criteria_is_hybrid);   
      rozofs_scan_check_against("hcr8",rozofs_scan_keyw_field_cr8time); 
      rozofs_scan_check_against("hctime",rozofs_scan_keyw_field_ctime); 
      rozofs_scan_check_against("hmod",rozofs_scan_keyw_field_mtime);   
      rozofs_scan_check_against("hatime",rozofs_scan_keyw_field_atime);  
      rozofs_scan_check_against("hupdate",rozofs_scan_keyw_field_update_time);   
      rozofs_scan_check_against("help",rozofs_scan_keyw_option_help);
      return rozofs_scan_keyw_input_error;
      break;

    case 'i' :
      return rozofs_scan_keyw_input_error;
      break;
      
    /*
    ** option:  j,junk           old[-j, --junk]
    */      
    case 'j' :
      rozofs_scan_check_against("junk",rozofs_scan_keyw_scope_junk);
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_scope_junk);
      break;
      
    case 'k' :
      return rozofs_scan_keyw_input_error;
      break;
      
    /*
    ** comparator:  le             old[--le]
    ** comparator:  lt             old[--lt]
    ** field:       link           old[--link]
    */
    case 'l' :
      if (argLen == 1) return rozofs_scan_keyw_input_error;
      if (pt[1] == 'e') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_comparator_le);
      if (pt[1] == 't') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_comparator_lt);
      rozofs_scan_check_against("link",rozofs_scan_keyw_field_nlink);
      return rozofs_scan_keyw_input_error;
      break;
      
    /*  
    ** field:    mod         old[--hmod] old[--smod] old[--mode]
    ** field:    mtime       old[--hmod] old[--smod] old[--mode]
    */
    case 'm' :
      rozofs_scan_check_against("mod",rozofs_scan_keyw_field_mtime);
      rozofs_scan_check_against("mtime",rozofs_scan_keyw_field_mtime);
      return rozofs_scan_keyw_input_error;
      break;
      
    /*
    ** field:     n, name            old[-n, --name]
    ** criteria:  noxattr            old[-X,--noxattr]
    ** criteria:  notrash            old[-T,--notrash]
    ** criteria:  nohybrid           old[--nohybrid]
    ** criteria:  noaging            
    */
    case 'n' :
      if (argLen == 1) return rozofs_scan_keyw_field_fname; 
      rozofs_scan_check_against("name",rozofs_scan_keyw_field_fname);
      rozofs_scan_check_against("noxattr",rozofs_scan_keyw_criteria_has_no_xattr);
      rozofs_scan_check_against("notrash",rozofs_scan_keyw_criteria_not_in_trash);
      rozofs_scan_check_against("nohybrid",rozofs_scan_keyw_criteria_is_not_hybrid);
      rozofs_scan_check_against("noaging",rozofs_scan_keyw_criteria_is_not_aging);  
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_field_fname);;
      break;

    /*
    ** argument:   o, output      old[-o,--output]
    ** operator:   or
    */
    case 'o' : 
      if (argLen == 1) return rozofs_scan_keyw_argument_output;
      if (pt[1]=='r') rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_operator_or);         
      rozofs_scan_check_against("out",rozofs_scan_keyw_argument_output);        
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_argument_output);
      break;

    /*
    ** field:    project      old[-P,--project]
    ** field:    pfid         old[--pfid]
    ** field:    parent       old[--under]
    */
    case 'p' : 
      rozofs_scan_check_against("project",rozofs_scan_keyw_field_project);
      rozofs_scan_check_against("pfid",rozofs_scan_keyw_field_pfid);   
      rozofs_scan_check_against("parent",rozofs_scan_keyw_field_parent);   
      return rozofs_scan_keyw_input_error;
      break;

    case 'q' : 
      return rozofs_scan_keyw_input_error;
      break;
      
    /*
    ** comparator:  regex      old[--regex]
    */
    case 'r' : 
      rozofs_scan_check_against("regex",rozofs_scan_keyw_comparator_regex);
      return rozofs_scan_keyw_input_error;
      break;

    /*
    ** field:     s, size    old[-s,--size]
    ** field:     satime     old[--satime]
    ** field:     sid        old[-z,--sid]
    ** field:     scr8       old[--scr8]
    ** field:     sctime     old[--sctime]
    ** field:     slave      old[--slave]
    ** field:     smod       old[--smod]
    ** field:     supdate    old[--supdate]
    ** scope:     slink      old[-S,--slink]
    ** scope:     slink      old[-S,--slink]
    ** option:    skipdate   
    */
    case 's' : 
      if (argLen == 1) return rozofs_scan_keyw_field_size;
      rozofs_scan_check_against("size",rozofs_scan_keyw_field_size);
      rozofs_scan_check_against("sidrange",rozofs_scan_keyw_field_sidrange);
      rozofs_scan_check_against("sid",rozofs_scan_keyw_field_sid);
      rozofs_scan_check_against("slink",rozofs_scan_keyw_scope_slink);
      rozofs_scan_check_against("scr8",rozofs_scan_keyw_field_cr8time);
      rozofs_scan_check_against("sctime",rozofs_scan_keyw_field_ctime);
      rozofs_scan_check_against("slave",rozofs_scan_keyw_field_slave);
      rozofs_scan_check_against("smod",rozofs_scan_keyw_field_mtime);
      rozofs_scan_check_against("satime",rozofs_scan_keyw_field_atime);
      rozofs_scan_check_against("supdate",rozofs_scan_keyw_field_update_time);
      rozofs_scan_check_against("skipdate",rozofs_scan_keyw_option_skipdate);
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_field_size);
      break;
      
    /*
    ** field:     t, trash     old[-t,--trash]
    */
    case 't' :
      rozofs_scan_check_against("trash",rozofs_scan_keyw_criteria_is_in_trash);
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_criteria_is_in_trash); 
      break;
      
    /*
    ** field:    under     old[--under]
    ** field     u,uid     old[-u,--uid]
    ** field:    update    old[--hupdate] old[--supdate] old[--update]
    */
    case 'u' :
      rozofs_scan_check_against("uid",rozofs_scan_keyw_field_uid);
      rozofs_scan_check_against("under",rozofs_scan_keyw_field_parent);
      rozofs_scan_check_against("update",rozofs_scan_keyw_field_update_time);
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_field_uid); 
      break;

      
    /*
    ** option:   v
    ** option:   vv
    */      
    case 'v' :
      if (argLen == 1) return rozofs_scan_keyw_option_verbose;
      if (pt[1]=='v') {
        if (argLen == 2) return rozofs_scan_keyw_option_vverbose;
        rozofs_scan_ret_arg_len(2,argLen,rozofs_scan_keyw_option_vverbose); 
      }  
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_option_verbose); 
      break;
      
    /*
    ** criteria:  w,werror     old[-w,--wrerror]
    */      
    case 'w' :
      rozofs_scan_check_against("wrerror",rozofs_scan_keyw_criteria_has_write_error);
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_criteria_has_write_error);
      break;
      
    /*
    ** criteria:  x,xattr     old[-x,--xattr]
    */      
    case 'x' :
      rozofs_scan_check_against("xattr",rozofs_scan_keyw_criteria_has_xattr);
      rozofs_scan_check_against("xname",rozofs_scan_keyw_field_xname);
      rozofs_scan_ret_arg_len(1,argLen,rozofs_scan_keyw_criteria_has_xattr);
      break;

    case 'y' :
      return rozofs_scan_keyw_input_error;
      break;

    case 'z' :
      return rozofs_scan_keyw_input_error;
      break;
      

    default:
     return rozofs_scan_keyw_input_error; 
  }  
  return rozofs_scan_keyw_input_error;    
}
/*
**__________________________________________________________________
** Decode a size in a string
** The size can be expressed in K, M, G, T or P units : 8, 9M, 10T
**
** @param sizeString   The string to decode  
**
** @retval The decoded value on success. LONG_VALUE_UNDEF on error
** Decode a size in a string
**__________________________________________________________________
*/
static inline uint64_t rozofs_scan_parse_size_string(char * sizeString) {
  uint64_t   value;
  int        argLen  = strlen(sizeString); 
  char     * pUnits = sizeString;
  int        ret;
  int        charCount;
   
  /*  
  ** Read 64 bit unsigned integer value
  */ 
  ret = sscanf(sizeString,"%llu%n",(long long unsigned int *)&value,&charCount);
  if (ret != 1) {
    return LONG_VALUE_UNDEF;
  } 
    
  /*
  ** Point to the end of the numbers where units could be set
  */  
  pUnits += charCount;
  
  /*
  ** End of the string whithout units
  */
  if (*pUnits==0) return value;
  
  /*
  ** Multiply by units
  */
  switch (*pUnits) {
    case 'K': pUnits++; value *= 1024UL; break;
    case 'M': pUnits++; value *= (1024UL*1024UL); break;
    case 'G': pUnits++; value *= (1024UL*1024UL*1024UL); break;
    case 'T': pUnits++; value *= (1024UL*1024UL*1024UL*1024UL); break;
    case 'P': pUnits++; value *= (1024UL*1024UL*1024UL*1024UL*1024UL); break;
    case 'k': pUnits++; value *= 1000UL; break;
    case 'm': pUnits++; value *= (1000UL*1000UL); break;
    case 'g': pUnits++; value *= (1000UL*1000UL*1000UL); break;
    case 't': pUnits++; value *= (1000UL*1000UL*1000UL*1000UL); break;
    case 'p': pUnits++; value *= (1000UL*1000UL*1000UL*1000UL*1000UL); break;
    case ']': 
    case '[': 
    case ')': 
    case '(': 
    case '}': 
    case '{': 
    case '/': 
    case ':': 
      break;
    default: 
      rozofs_scan_ret_arg_len((pUnits-sizeString),argLen,LONG_VALUE_UNDEF);
      break;  
  }  
  
  rozofs_scan_ret_arg_len((pUnits-sizeString),argLen,value);
}
/*
**___________________________________________________________________
** Scan a fid value on 36 characters
** 12345678-1234-1234-1234-123456789012
**
** @param string   The string containig the fid value
**
** @retval    The decoded fid on 16 bytes
**___________________________________________________________________
*/
int rozofs_scan_parse_fid(char * pArg, fid_t fid) {  
  int argLen = strlen(pArg); 
    
  /*
  ** Call common RozoFS function for decoding the FID
  */  
  if (rozofs_uuid_parse(pArg,fid)!=0) {
    /*
    ** This is not a correct FID
    */
    return -1;                  
  } 
  /*
  ** Skip to the end of the FID in case some extra input
  ** information is present
  */
  rozofs_scan_ret_arg_len(36,argLen,0);
}
/*
**___________________________________________________________________
** Scan a time string either in seconds or in the format  
** YYYY-MM-DD or YYYY-MM-DD-HH or YYYY-MM-DD-HH:MM or YYYY-MM-DD-HH:MM:SS
**
** @param string   The string containing the date value
**
** @retval    The date in seconds
**___________________________________________________________________
*/
uint64_t rozofs_scan_parse_time(char * string) {  
  uint64_t seconds = LONG_VALUE_UNDEF;
  
  /*
  ** Interpret the date as in human readable format
  */
  seconds = rozofs_date2time(string);
  if (seconds == LONG_VALUE_UNDEF) {
    /*
    ** Format is not human readable. Read seconds
    */
    seconds = rozofs_scan_parse_u64(string);
  }
  return seconds;
}
/*
**___________________________________________________________________
** Scan a string containing a name. 
** The string has to stop on closing brackets.
** If input string is "fileName]or[uid=0]" the returned name must be
** "fileName" and the next rgument to parse must be "]or[uid=0]"
**
** @param string   The string containing the name
**
** @retval    The name
**___________________________________________________________________
*/
char * rozofs_scan_parse_name(char * string) {  
  char * pChar;
  char * name;
  
  /*
  ** Duplicate the string since we may have to add a 0 at the end
  ** that would suppress information from the input string
  */
  name = strdup(string);
  pChar = name;
  
  /*
  ** Check for closing brackets
  */
  while((*pChar!='}')&&(*pChar!=')')&&(*pChar!=']')
      &&(*pChar!='{')&&(*pChar!='[')&&(*pChar!='(')
      &&(*pChar!=0)) pChar++;
  
  /*
  ** Stop the name at the closig bracket
  */
  if (*pChar!=0) {  
    *pChar = 0;
    rozofs_scan_current_arg_char2skip += strlen(name);
    rozofs_scan_parse_same_arg = 1;
  }
  return name; 
}
/*
**___________________________________________________________________
** Compile a regex
**
** @param fname   The file name containing the regex to compile
**
** @retval The address of the compiled regex
*/
pcre * rozofs_scan_compile_regex_string(char * pt) {  
  const char *pcreErrorStr;     
  int         pcreErrorOffset;
  pcre      * pRegex;  

  pRegex = pcre_compile(pt, 0, &pcreErrorStr, &pcreErrorOffset, NULL);
  if(pRegex == NULL) {
    usage("Bad regex \"%s\" at offset %d : %s", pt, pcreErrorOffset, pcreErrorStr);  
  }
  return pRegex;
} 
/* 
**__________________________________________________________________
** Check whether the input date comparison enables to skip some
** tracking files. We will evaluate the condition setting true to
** every criteria and false to this date comaprison. If result is false
** the date comparison enables to skip some tracking files 
**
** @param node                node describing the condition
** @param dateComparison      date comaprison leaf
**
** @retaval 1 on success / 0 on failure
**__________________________________________________________________
*/
int rozofs_scan_is_date_comparison_determinant(rozofs_scan_node_t * node,
                                               rozofs_scan_node_t * dateComparison) {
  int result;
  int idx;
    
  if (node==NULL) return 1;
  
  switch(node->type) {
    case rozofs_scan_type_criteria:
      return 1;
      break;  

    case rozofs_scan_type_field:
      if (node == dateComparison) return 0;
      return 1;
      break; 
         
    default:
      break;   
  }  

  /*
  ** Evaluate every subnode 
  */       
  for (idx=0; idx<node->n.nbNext; idx++) {   
           
    result = rozofs_scan_is_date_comparison_determinant(node->n.next[idx],dateComparison);

    /*
    ** And operator. All sub nodes have to be TRUE for the node to be TRUE
    */
    if (node->n.ope == rozofs_scan_node_ope_and) {
      if (!result) {
        return 0;
      }  
      continue;    
    }
    
    /*
    ** Or operator. One TRUE sub node is enough to make the node TRUE
    */ 
    if (result) {
      return 1;
    }  
  }    
  return result;
}    
/* 
**__________________________________________________________________
** Check whether the input date comparison enables to skip some
** tracking files. We will evaluate the condition setting true to
** every criteria and false to this date comaprison. If result is false
** the date comparison enables to skip some tracking files 
**
** @param node                node describing the condition
** @param dateComparison      date comaprison leaf
**
** @retaval 1 on success / 0 on failure
**__________________________________________________________________
*/
void rozofs_scan_lookup_date_field(rozofs_scan_node_t * node) {
  int                  idx;
    
  if (node==NULL) return;  
  
  /*
  ** Loop on subnodes of nodes
  */      
  if (node->type == rozofs_scan_type_node) {
    for (idx=0; idx<node->n.nbNext; idx++) {          
      rozofs_scan_lookup_date_field(node->n.next[idx]);
    }
    return;
  }
  
  /*
  ** Check field
  */
  if (node->type == rozofs_scan_type_field) {
    switch(node->l.name) {  
      case rozofs_scan_keyw_field_cr8time:
      case rozofs_scan_keyw_field_mtime: 
      case rozofs_scan_keyw_field_ctime: 
        switch(node->l.comp) {
          case rozofs_scan_keyw_comparator_lt:
          case rozofs_scan_keyw_comparator_le:
          case rozofs_scan_keyw_comparator_eq:
          case rozofs_scan_keyw_comparator_ge:
          case rozofs_scan_keyw_comparator_gt:
            dateField[rozofs_scan_date_field_count] = node;
            rozofs_scan_date_field_count++;
            break;          
          default:
            break;
        }
        break;
      default:
        break;
    }     
  }
  return;
}     
/*
**___________________________________________________________________
** Parse input parameters
**
** @param argc   Number of input argument
** @param argv   Array of arguments
**
**___________________________________________________________________
*/
void rozofs_scan_parse_command(int argc, char *argv[]) {
  rozofs_scan_node_t * newNode = NULL;
  rozofs_scan_node_t * oldNode = NULL;
  rozofs_scan_keyw_e   name;
  rozofs_scan_keyw_e   comp;
  fid_t                fid;
  char               * pFname;
  uint64_t             val64;
  uint64_t             val64bis;
  int                  deep=0;
  char               * pArg;
  pcre               * pRegex = NULL;
  int                  determinant;
  uint64_t             min_sid,max_sid;
  
  /*
  ** No parameter is help needed
  */
  if (argc < 2)  usage(NULL); 
  
  /*
  ** Create upper node of the tree
  */
  oldNode = rozofs_scan_new_node(NULL);   
  upNode  = oldNode;

  /*
  ** Loop on the input key words
  */
  while ((pArg = rozofs_scan_get_next_input_pointer(argc,argv)) != NULL) {
    
    name = rozofs_scan_decode_argument(pArg);
    if (name==rozofs_scan_keyw_input_error) {
      rozofs_show_error("Unexpected argument %s",pArg);              
    }  

    if (name==rozofs_scan_keyw_option_verbose) {
      verbose = 1;
    } 
    if (name==rozofs_scan_keyw_option_vverbose) {
      verbose = 2;
    }               
    VERBOSE("\n %s ", rozofs_scan_keyw_e2String (name));  

    if ((name==rozofs_scan_keyw_option_verbose)||(name==rozofs_scan_keyw_option_vverbose)) {
      continue;
    }  
     
    if ((name > rozofs_scan_keyw_criteria_min) && (name < rozofs_scan_keyw_criteria_max)) {
      /*
      ** No criteria must be set when only one FID is requested
      */
      if (just_this_fid) {
        rozofs_show_error("No criteria expected after \"fid\" field comparator.");              
      }

      newNode = rozofs_scan_new_criteria(name);
      oldNode = rozofs_scan_add_node(oldNode,newNode);
      continue;
    }  
    
    if ((name > rozofs_scan_keyw_field_min) && (name < rozofs_scan_keyw_field_max)) {
      min_sid = 0;
      max_sid = LONG_VALUE_UNDEF;

      /*
      ** No field comparator must be set when only one FID is requested
      */
      if (just_this_fid) {
        rozofs_show_error("No field comparator expected after \"fid\" field comparator.");              
      }
             
      /*
      ** Field comparison requires a comparator...
      */
      pArg = rozofs_scan_get_next_input_pointer(argc,argv);
      if (pArg == NULL) {
        rozofs_show_error("comparator expected after %s",rozofs_scan_keyw_e2String (name));              
      }      
      comp = rozofs_scan_decode_argument(pArg);
      
      /*
      ** sidrange[x:y] <COMPARATOR> <cid#>/<sid#>
      */
      if (name == rozofs_scan_keyw_field_sidrange) {
        if (comp != rozofs_scan_keyw_separator_open) {
          rozofs_show_error("Open bracket expected after %s. Got %s instead",rozofs_scan_keyw_e2String (name), pArg);      
        } 
      }        
      /*
      ** <FIELD> <COMPARATOR> <VALUE>
      */
      else {
        VERBOSE("%s ", rozofs_scan_keyw_e2String (comp));
        if ((rozofs_scan_keyw_comparator_min>=comp)||(comp>=rozofs_scan_keyw_comparator_max)) {
          rozofs_show_error("comparator expected after %s. Got %s instead",rozofs_scan_keyw_e2String (name), pArg);      
        } 
        /*
        ** ... and value
        */
        pArg = rozofs_scan_get_next_input_pointer(argc,argv);
        if (pArg == NULL) {
          rozofs_show_error("%s %s expect a value",rozofs_scan_keyw_e2String (name), rozofs_scan_keyw_e2String(comp));              
        }
      }     
       
      switch(name) {

        case rozofs_scan_keyw_field_fid:
          if (comp!=rozofs_scan_keyw_comparator_eq) {
            rozofs_show_error("%s expects only comparator eq but got %s instead",rozofs_scan_keyw_e2String (name), rozofs_scan_keyw_e2String(comp));                  
          }
          if (rozofs_scan_parse_fid(pArg,this_fid)!=0) {
            rozofs_show_error("%s expects a fid value but got %s",rozofs_scan_keyw_e2String (name), pArg);                  
          }  
          if (verbose){
            char  fidString[40];
            rozofs_fid_append(fidString,this_fid);
            printf("%s ", fidString);
          }
          just_this_fid = 1;
          break;
          
      
        case rozofs_scan_keyw_field_pfid:
          if (comp!=rozofs_scan_keyw_comparator_eq) {
            rozofs_show_error("%s expects only comparator eq but got %s instead",rozofs_scan_keyw_e2String (name), rozofs_scan_keyw_e2String(comp));                  
          }
          if (rozofs_scan_parse_fid(pArg,fid)!=0) {
            rozofs_show_error("%s expects a fid value but got %s",rozofs_scan_keyw_e2String (name), pArg);                  
          }  
          if (verbose){
            char  fidString[40];
            rozofs_fid_append(fidString,fid);
            printf("%s ", fidString);
          }
          newNode = rozofs_scan_new_fid_field(name,fid,comp);    
          oldNode = rozofs_scan_add_node(oldNode,newNode);
          break;
          
        case rozofs_scan_keyw_field_fname:
        case rozofs_scan_keyw_field_parent:
        case rozofs_scan_keyw_field_xname:
          switch(comp) {
            case rozofs_scan_keyw_comparator_eq:
            case rozofs_scan_keyw_comparator_ge:
              pFname = rozofs_scan_parse_name(pArg);
              newNode = rozofs_scan_new_string_field(name,pFname,comp);  
              oldNode = rozofs_scan_add_node(oldNode,newNode);                      
              VERBOSE("%s",pFname);
              break;
            case rozofs_scan_keyw_comparator_regex:
              pRegex = rozofs_scan_compile_regex_string(pArg);
              newNode = rozofs_scan_new_regex_field(name,pRegex);  
              oldNode = rozofs_scan_add_node(oldNode,newNode);
              VERBOSE("%s",pArg);
              break;
            default:
              rozofs_show_error("%s does not support %s",rozofs_scan_keyw_e2String (name), rozofs_scan_keyw_e2String(comp));                                
          }
          break;
          
        case rozofs_scan_keyw_field_cr8time:
        case rozofs_scan_keyw_field_mtime:
        case rozofs_scan_keyw_field_ctime:
        case rozofs_scan_keyw_field_atime:
        case rozofs_scan_keyw_field_update_time:
          if (comp == rozofs_scan_keyw_comparator_regex) {
            rozofs_show_error("%s does not support %s",rozofs_scan_keyw_e2String (name), rozofs_scan_keyw_e2String(comp));                                
          }
          val64 = rozofs_scan_parse_time(pArg);
          if (val64 == LONG_VALUE_UNDEF) {
            rozofs_show_error("Bad date value %s",pArg);                                
          } 
          VERBOSE("%llu",(long long unsigned int)val64);        
          newNode = rozofs_scan_new_u64_field(name,val64,comp);         
          oldNode = rozofs_scan_add_node(oldNode,newNode);                    
          break;
          
        case rozofs_scan_keyw_field_size:
        case rozofs_scan_keyw_field_slave:
        case rozofs_scan_keyw_field_nlink:
        case rozofs_scan_keyw_field_deleted:
        case rozofs_scan_keyw_field_children:        
          if (comp == rozofs_scan_keyw_comparator_regex) {
            rozofs_show_error("%s does not support %s",rozofs_scan_keyw_e2String (name), rozofs_scan_keyw_e2String(comp));                                
          }
          val64 = rozofs_scan_parse_size_string(pArg);
          if (val64==LONG_VALUE_UNDEF) {
            rozofs_show_error("Bad value \"%s\" for %s %s",pArg,rozofs_scan_keyw_e2String (name), rozofs_scan_keyw_e2String(comp));                                            
          }
          VERBOSE("%llu",(long long unsigned int)val64);                  
          newNode = rozofs_scan_new_u64_field(name,val64,comp);         
          oldNode = rozofs_scan_add_node(oldNode,newNode);                    
          break;

        case rozofs_scan_keyw_field_uid:
        case rozofs_scan_keyw_field_gid:
        case rozofs_scan_keyw_field_project:
        case rozofs_scan_keyw_field_cid:
          if ((comp != rozofs_scan_keyw_comparator_eq)&&(comp != rozofs_scan_keyw_comparator_ne)) {
            rozofs_show_error("%s does not support %s",rozofs_scan_keyw_e2String (name), rozofs_scan_keyw_e2String(comp));                                
          }
          val64 = rozofs_scan_parse_size_string(pArg);
          if (val64==LONG_VALUE_UNDEF) {
            rozofs_show_error("Bad value \"%s\" for %s %s",pArg,rozofs_scan_keyw_e2String (name), rozofs_scan_keyw_e2String(comp));                                            
          }
          VERBOSE("%llu",(long long unsigned int)val64); 
          newNode = rozofs_scan_new_u64_field(name,val64,comp);                
          oldNode = rozofs_scan_add_node(oldNode,newNode);         
          break;
          
        case rozofs_scan_keyw_field_sidrange: /* <cid#>/<sid#> */
          if (comp != rozofs_scan_keyw_separator_open) {
            rozofs_show_error("%s requires %s and got %s",rozofs_scan_keyw_e2String (name), rozofs_scan_keyw_e2String(rozofs_scan_keyw_separator_open), rozofs_scan_keyw_e2String(comp));                                
          }
          /*
          ** Here comes the 1rst value of the range
          */
          pArg = rozofs_scan_get_next_input_pointer(argc,argv);
          min_sid = rozofs_scan_parse_size_string(pArg);
          if (min_sid==LONG_VALUE_UNDEF) {
            rozofs_show_error("Bad sid range value(1rst) \"%s\" for %s",pArg,rozofs_scan_keyw_e2String (name));                                            
          }
          /*
          ** Next should come a ':'
          */
          pArg = rozofs_scan_get_next_input_pointer(argc,argv);
          if ((pArg == NULL)||(*pArg != ':')) {
            rozofs_show_error("Expecting \':\' in %s value definition",rozofs_scan_keyw_e2String (name));              
          }
          rozofs_scan_current_arg_char2skip++;
          pArg++;
          /*
          ** Here comes the 2nd value of the range
          */
          max_sid = rozofs_scan_parse_size_string(pArg);
          if (max_sid==LONG_VALUE_UNDEF) {
            rozofs_show_error("Bad sid range value(2nd) \"%s\" for %s",pArg,rozofs_scan_keyw_e2String (name));                                            
          }
          if (min_sid > max_sid) {
            rozofs_show_error("1rst value of sid range(%d) must be lower or equal to 2nd value.",min_sid,pArg);                                            
          } 
          /*
          ** Then a close bracket
          */
          pArg = rozofs_scan_get_next_input_pointer(argc,argv);
          comp = rozofs_scan_decode_argument(pArg);
          if (comp != rozofs_scan_keyw_separator_close) {
            rozofs_show_error("Close bracket is expexted for %s. Got %s instead",rozofs_scan_keyw_e2String (name), pArg);      
          } 
          /*
          ** Field comparison requires a comparator...
          */
          pArg = rozofs_scan_get_next_input_pointer(argc,argv);
          if (pArg == NULL) {
            rozofs_show_error("comparator expected after %s",rozofs_scan_keyw_e2String (name));              
          }
          comp = rozofs_scan_decode_argument(pArg);
          VERBOSE("[%llu:%llu] %s ",(long long unsigned int)min_sid,(long long unsigned int)max_sid,rozofs_scan_keyw_e2String(comp));
          /*
          ** Get the value
          */
          pArg = rozofs_scan_get_next_input_pointer(argc,argv);
          if (pArg == NULL) {
            rozofs_show_error("Value expected after %s",rozofs_scan_keyw_e2String (name));              
          }

          /*
          ** ....
          */
        case rozofs_scan_keyw_field_sid: /* <cid#>/<sid#> */
          if ((comp != rozofs_scan_keyw_comparator_eq)&&(comp != rozofs_scan_keyw_comparator_ne)) {
            rozofs_show_error("%s does not support %s",rozofs_scan_keyw_e2String (name), rozofs_scan_keyw_e2String(comp));                                
          }
          /*
          ** Here comes the CID value
          */
          val64 = rozofs_scan_parse_size_string(pArg);
          if (val64==LONG_VALUE_UNDEF) {
            rozofs_show_error("Bad CID value \"%s\" for %s %s",pArg,rozofs_scan_keyw_e2String (name), rozofs_scan_keyw_e2String(comp));                                            
          }
          /*
          ** Next should come a '/'
          */
          pArg = rozofs_scan_get_next_input_pointer(argc,argv);
          if ((pArg == NULL)||(*pArg != '/')) {
            rozofs_show_error("Expecting \'/\' in %s value definition",rozofs_scan_keyw_e2String (name));              
          }
          rozofs_scan_current_arg_char2skip++;
          pArg++;
          /*
          ** Here comes the SID value
          */
          val64bis = rozofs_scan_parse_size_string(pArg);
          if (val64bis==LONG_VALUE_UNDEF) {
            rozofs_show_error("Bad SID value \"%s\" for %s %s",pArg,rozofs_scan_keyw_e2String (name), rozofs_scan_keyw_e2String(comp));                                            
          }          
          VERBOSE("%llu/%llu",(long long unsigned int)val64, (long long unsigned int)val64bis); 
          newNode = rozofs_scan_new_2u32_range_field(name,val64,val64bis,comp, min_sid, max_sid);                
          oldNode = rozofs_scan_add_node(oldNode,newNode);         
          break;          
        default:
         rozofs_show_error("Unexpected %s",rozofs_scan_keyw_e2String (name));       
      }
      continue;
    }
    
    /*
    ** Other arguments
    */
    switch(name) {
    
      /*
      ** Open bracket
      */
      case rozofs_scan_keyw_separator_open:
        /*
        ** No brackets must be set when only one FID is requested
        */
        if (just_this_fid) {
          rozofs_show_error("No bracket expected after \"fid\" field comparator.");              
        }
        /*
        ** Push old node in the stack
        */
        if (rozofs_scan_push_node(oldNode)<0) {
          rozofs_scan_display_tree(upNode, 0);    
          usage("invalid condition");
        } 
        /*
        ** Create a new node and link it to the old node
        */
        newNode = rozofs_scan_new_node(NULL);
        oldNode = rozofs_scan_add_node(oldNode,newNode);       
        deep++;
        VERBOSE("(%d)",deep);        
        break;
    
      /*
      ** Close bracket
      */
      case rozofs_scan_keyw_separator_close: 
        /*
        ** No brackets must be set when only one FID is requested
        */
        if (just_this_fid) {
          rozofs_show_error("No bracket expected after \"fid\" field comparator.");              
        }
        VERBOSE("(%d)",deep);        
        if (deep==0) {
          rozofs_show_error("unbalanced parantheses");                
        } 
        deep--;
        /*
        ** Restore old node from the stack
        */
        oldNode = rozofs_scan_pop_node(); 
        if (oldNode == NULL) {
          rozofs_show_error("error poping node");            
        }      
        break;
        
      case rozofs_scan_keyw_operator_or:
        /*
        ** No or must be set when only one FID is requested
        */
        if (just_this_fid) {
          rozofs_show_error("No \"or\" expected after \"fid\" field comparator.");              
        }
        if (oldNode) {
          rozofs_scan_set_ope_or(oldNode);
        }  
        else {
          rozofs_show_error("unexpected or operation");                        
        }          
        break;
        
      case rozofs_scan_keyw_operator_and:
        /*
        ** No and must be set when only one FID is requested
        */
        if (just_this_fid) {
          rozofs_show_error("No \"and\" expected after \"fid\" field comparator.");              
        }
        if (oldNode) {
          rozofs_scan_set_ope_and(oldNode);
        }  
        else {
          rozofs_show_error("unexpected and operation");                        
        }          
        break;
        
      case rozofs_scan_keyw_option_skipdate:
        skip_tracking_file = 1;
        break;    
        
      case rozofs_scan_keyw_scope_junk:
        if ((rozofs_scan_scope != rozofs_scan_scope_regular_file)&&(rozofs_scan_scope != rozofs_scan_scope_junk_file)) { 
          rozofs_show_error("dir, junk and slink are incompatible options");
        }
        rozofs_scan_scope = rozofs_scan_scope_junk_file;
        break;
      
      case rozofs_scan_keyw_scope_dir:
        if ((rozofs_scan_scope != rozofs_scan_scope_regular_file)&&(rozofs_scan_scope != rozofs_scan_scope_directory)) {     
          rozofs_show_error("dir, junk and slink are incompatible options");
        }
        rozofs_scan_scope = rozofs_scan_scope_directory;
        break;
      
      case rozofs_scan_keyw_scope_slink:
        if ((rozofs_scan_scope != rozofs_scan_scope_regular_file)&&(rozofs_scan_scope != rozofs_scan_scope_symbolic_link)) {     
          rozofs_show_error("dir, junk and slink are incompatible options");
        }
        rozofs_scan_scope = rozofs_scan_scope_symbolic_link;
        break;
        
      case rozofs_scan_keyw_argument_eid:
        pArg = rozofs_scan_get_next_input_pointer(argc,argv);
        if (pArg == NULL) {
          rozofs_show_error("%s expect a value",rozofs_scan_keyw_e2String (name));              
        }      
        eid = rozofs_scan_parse_u64(pArg);
        if (eid==LONG_VALUE_UNDEF) {
          rozofs_show_error("Bad value \"%s\" for %s",pArg,rozofs_scan_keyw_e2String (name));                                            
        }
        VERBOSE("%llu",(long long unsigned int)eid);        
        break;
        
      case rozofs_scan_keyw_argument_config:
        configFileName = rozofs_scan_get_next_input_pointer(argc,argv);
        if (pArg == NULL) {
          rozofs_show_error("%s expect a value",rozofs_scan_keyw_e2String (name));              
        }      
        VERBOSE("%s",configFileName);        
        break;          
      
      case rozofs_scan_keyw_option_help:
        usage(NULL);
        break;
        
      case rozofs_scan_keyw_argument_output:     
        pArg = rozofs_scan_get_next_input_pointer(argc,argv);
        if (pArg == NULL) {
          rozofs_show_error("argument %s expect a value",pArg);              
        }
        if (rozofs_parse_output_format(pArg,argc,argv)!=0) {
          rozofs_show_error("Bad output format \"%s\"",pArg);     
        } 
        VERBOSE("%s",pArg);
        break;       
             
      default:
        rozofs_show_error("Unexpected argument \"%s\"",pArg);     
    }
    continue;
  }
  VERBOSE("\n");
  
  if (deep) {
    rozofs_show_error("Unbalanced parantheses");
  } 

  /*
  ** Simplify the tree
  */
  rozofs_scan_simplify_tree(upNode);
  
  if (verbose) {
    printf("\n");
    rozofs_scan_display_tree(upNode, 0);
    printf("\n");    
  }  
  
  /*
  ** If it is requested to skip tracking files not matching input date condition,
  ** check if actually some date condition exist
  */
  if (skip_tracking_file) {
    VERBOSE("\n Check for tracking file skipping thanks to some date criteria\n");
    /*
    ** Build array of date conditions
    */
    rozofs_scan_lookup_date_field(upNode);
    /*
    ** Check whether the date condition is determinant by setting every condition
    ** to true and this one to false. If result is false, the condition can be 
    ** used to skip tracking files.
    */
    determinant = 0;
    if (rozofs_scan_date_field_count) {
      int                  idx;
      rozofs_scan_node_t * leaf;

      /*
      ** Loop on the date comparisons set
      */
      for (idx=0; idx<rozofs_scan_date_field_count; idx++) {
        leaf = dateField[idx];
        VERBOSE("   <%s> \t<%s> \t<%llu> \t", 
                 rozofs_scan_keyw_e2String(leaf->l.name), 
                 rozofs_scan_keyw_e2String(leaf->l.comp),
                 (long long unsigned int)leaf->l.value.u64);

        if (rozofs_scan_is_date_comparison_determinant(upNode, leaf)==0) {
          /*
          ** This date comparison is determinant
          */
          VERBOSE(" is DETERMINANT\n");
          determinant++;
        }
        else {
          /*
          ** This date field is not determinant. Clear it from the table
          */
          VERBOSE(" is NOT determinant\n");
          dateField[rozofs_scan_date_field_count] = NULL;
        }
      }  
    }  
    VERBOSE("\n %d determinant date comparison(s)\n\n", determinant);
    skip_tracking_file = determinant;
  }
}
/*
**_______________________________________________________________________
**
**  M A I N
**_______________________________________________________________________
*/
int main(int argc, char *argv[]) {
  void                * rozofs_export_p;
  char                * root_path=NULL;
  long long             usecs;
  struct timeval        start;
  struct timeval        stop;
  rozofs_inode_t      * fake_inode_p;
  
  rozofs_layout_initialize();       

  /*
  ** Get starting date tomeasure performances
  */  
  gettimeofday(&start,(struct timezone *)0);

  /*
  ** Parse input parameters, populate some global variables from this input,
  ** and build a tree of the conditions the files/directories have to fullfill.
  */
  rozofs_scan_parse_command(argc,argv); 
  
  /*
  ** Case only one FID is requested
  */
   if (just_this_fid) {
    fake_inode_p = (rozofs_inode_t *) this_fid;
    eid = fake_inode_p->s.eid;
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
    layout = export_config->layout;
    rozofs_get_rozofs_invers_forward_safe(layout, &rozofs_inverse, &rozofs_forward, &rozofs_safe);
    root_path = export_config->root;        
  }
  if (root_path == NULL)  {
    usage("Missing export identifier (eid)");
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
  ** Beginning of the display
  */
  if (display_json) {
    int i;
    printf("{ \"command\" : \"rozo_scan");
    for (i=1; i<argc; i++) printf(" %s",argv[i]);
    printf("\",\n  \"results\" : [");
  }  
  
  /*
  ** Depending on the scope of the scan
  */
  if (just_this_fid) {
    scan_index_context_t   index;

    index.file_id   = fake_inode_p->s.file_id;
    index.user_id   = fake_inode_p->s.usr_id;
    index.inode_idx = fake_inode_p->s.idx;
    
    switch(fake_inode_p->s.key) {
      case ROZOFS_REG_S_MOVER:
      case ROZOFS_REG_D_MOVER:
      case ROZOFS_REG:   
        rozofs_scan_scope = rozofs_scan_scope_regular_file;
        rz_scan_all_inodes_from_context(rozofs_export_p,ROZOFS_REG,1,rozofs_visit,NULL,NULL,NULL,&index);
        break;
      case ROZOFS_DIR: 
        rozofs_scan_scope = rozofs_scan_scope_directory;
        rz_scan_all_inodes_from_context(rozofs_export_p,ROZOFS_DIR,1,rozofs_visit,NULL,NULL,NULL,&index);
        break;
    default:
      usage("Bad FID value");
    }            
  }
  
  else switch (rozofs_scan_scope) {
  
    case rozofs_scan_scope_directory:  
      /*
      ** Check that the condition is meaningfull  for directories
      */
      rozofs_scan_eval_one_field    = rozofs_scan_validate_one_field_directory;
      rozofs_scan_eval_one_criteria = rozofs_scan_validate_one_criteria_directory;
      rozofs_scan_validate_node(upNode);
      /*
      ** Execute the walk and evaluate the inodes
      */
      rozofs_scan_eval_one_field    = rozofs_scan_eval_one_field_inode;
      rozofs_scan_eval_one_criteria = rozofs_scan_eval_one_criteria_inode;
      rz_scan_all_inodes(rozofs_export_p,ROZOFS_DIR,1,rozofs_visit,NULL,rozofs_check_trk_file_date,NULL);
      break;
      
    case rozofs_scan_scope_junk_file:     
      /*
      ** Check that the condition is meaningfull  for directories
      */
      rozofs_scan_eval_one_field    = rozofs_scan_validate_one_field_junk;
      rozofs_scan_eval_one_criteria = rozofs_scan_validate_one_criteria_junk;
      rozofs_scan_validate_node(upNode);
      /*
      ** Execute the walk and evaluate the inodes
      */
      rozofs_scan_eval_one_field    = rozofs_scan_eval_one_field_rmfentry;
      rozofs_scan_eval_one_criteria = rozofs_scan_eval_one_criteria_rmfentry;;
      rz_scan_all_inodes(rozofs_export_p,ROZOFS_TRASH,1,rozofs_visit_junk,NULL,NULL,NULL);  
      break;
      
    case rozofs_scan_scope_regular_file:     
      /*
      ** Check that the condition is meaningfull  for directories
      */
      rozofs_scan_eval_one_field    = rozofs_scan_validate_one_field_regular;
      rozofs_scan_eval_one_criteria = rozofs_scan_validate_one_criteria_regular;
      rozofs_scan_validate_node(upNode);
      /*
      ** Execute the walk and evaluate the inodes
      */
      rozofs_scan_eval_one_field    = rozofs_scan_eval_one_field_inode;
      rozofs_scan_eval_one_criteria = rozofs_scan_eval_one_criteria_inode;
      rz_scan_all_inodes(rozofs_export_p,ROZOFS_REG,1,rozofs_visit,NULL,rozofs_check_trk_file_date,NULL);
      break;

    case rozofs_scan_scope_symbolic_link:     
      /*
      ** Check that the condition is meaningfull  for directories
      */
      rozofs_scan_eval_one_field    = rozofs_scan_validate_one_field_slink;
      rozofs_scan_eval_one_criteria = rozofs_scan_validate_one_criteria_slink;
      rozofs_scan_validate_node(upNode);
      /*
      ** Execute the walk and evaluate the inodes
      */
      rozofs_scan_eval_one_field    = rozofs_scan_eval_one_field_inode;
      rozofs_scan_eval_one_criteria = rozofs_scan_eval_one_criteria_inode;
      rz_scan_all_inodes(rozofs_export_p,ROZOFS_REG,1,rozofs_visit,NULL,rozofs_check_trk_file_date,NULL);
      break;
  }
   
  /*
  ** Finish display by search statistics
  */
  gettimeofday(&stop,(struct timezone *)0); 

  if (display_json) {
    printf("\n  ],\n");
    printf("  \"scope\"            : \"%s\",\n", rozofs_scan_scope_e2String(rozofs_scan_scope));
    printf("  \"scanned entries\"  : %llu,\n", (long long unsigned int)nb_scanned_entries);
    printf("  \"in scope\"         : %llu,\n", (long long unsigned int)nb_scanned_entries_in_scope);
    printf("  \"matched entries\"  : %llu,\n", (long long unsigned int)nb_matched_entries);
    if (rozofs_scan_scope == rozofs_scan_scope_regular_file) { 
      printf("  \"sum file size\"    : %llu,\n", (long long unsigned int)sum_file_size);
      printf("  \"sum file blocks\"  : %llu,\n", (long long unsigned int)sum_file_blocks);
    }
    if (rozofs_scan_scope == rozofs_scan_scope_directory) { 
      printf("  \"sum sub dirs\"     : %llu,\n", (long long unsigned int)sum_sub_dir);
      printf("  \"sum sub files\"    : %llu,\n", (long long unsigned int)sum_sub_files);
      printf("  \"sum file size\"    : %llu,\n", (long long unsigned int)sum_file_size);
    }    
    printf("  \"Trk date checked\" : %llu,\n", (long long unsigned int)nb_checked_tracking_files);
    printf("  \"Trk skipped\"      : %llu,\n", (long long unsigned int)nb_skipped_tracking_files);
    gettimeofday(&stop,(struct timezone *)0); 
    usecs   = stop.tv_sec  * 1000000 + stop.tv_usec;
    usecs  -= (start.tv_sec  * 1000000 + start.tv_usec);
    printf("  \"micro seconds\"    : %llu\n}\n", usecs);    
  }
  /*
  ** When not in json format but output is none, the count of matched entries is required
  */
  else if (display_stat == DO_DISPLAY) {
    printf("scanned entries  : %llu\n", (long long unsigned int)nb_scanned_entries);
    printf("in scope         : %llu\n", (long long unsigned int)nb_scanned_entries_in_scope);
    printf("matched entries  : %llu\n", (long long unsigned int)nb_matched_entries);
    if (rozofs_scan_scope == rozofs_scan_scope_regular_file) { 
      printf("sum file size    : %llu,\n", (long long unsigned int)sum_file_size);
      printf("sum file blocks  : %llu,\n", (long long unsigned int)sum_file_blocks);
    }
    if (rozofs_scan_scope == rozofs_scan_scope_directory) { 
      printf("sum sub dirs     : %llu,\n", (long long unsigned int)sum_sub_dir);
      printf("sum sub files    : %llu,\n", (long long unsigned int)sum_sub_files);
      printf("sum file size    : %llu,\n", (long long unsigned int)sum_file_size);
    }       
    printf("Trk date checked : %llu\n", (long long unsigned int)nb_checked_tracking_files);
    printf("Trk skipped      : %llu\n", (long long unsigned int)nb_skipped_tracking_files);
    usecs   = stop.tv_sec  * 1000000 + stop.tv_usec;
    usecs  -= (start.tv_sec  * 1000000 + start.tv_usec);
    printf("micro seconds    : %llu\n", usecs);    
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
