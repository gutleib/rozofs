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
#ifndef ROZOFS_DU_H
#define ROZOFS_DU_H
#include <stdio.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/list.h>
#include <inttypes.h>
#include <assert.h>
#include <rozofs/common/htable.h>


typedef struct _rozofs_dir_layout_t
{
   list_t                        child_head;  /**< pointer to the children directories     */
   list_t                        link;        /**< linked list of the children directories */
   fid_t   fid;                                 /**< directory fid                           */
   hash_entry_t                  he;
   uint64_t                      nb_bytes;
   uint32_t                      nb_files;
   uint32_t                      nb_dir;
   uint32_t                      opaque;
} rozofs_dir_layout_t;


typedef struct _big_obj_t
{
   fid_t fid;   /**< object FID  */
   uint64_t count;   /**< either max number of file or size   */
} big_obj_t;

#define ROZODU_MAX_OBJ 1024

extern int  rozodu_big_dir_count_table_sz;     
extern big_obj_t  rozodu_big_dir_count_table[ROZODU_MAX_OBJ+1];
extern int  rozodu_big_dir_size_table_sz;     
extern big_obj_t  rozodu_big_dir_size_table[ROZODU_MAX_OBJ+1];
extern int  rozodu_big_file_table_sz;     
extern big_obj_t  rozodu_big_file_size_table[ROZODU_MAX_OBJ+1];

extern list_t  dir_head;
extern list_t  dir_orphan_head;
extern htable_t htable_directory;
extern int rozodu_verbose;
extern int format_bytes;

#define ROZODU_CACHE_BUCKETS (1024*128)
typedef enum {
  ROZOPRJ_DIR_CNT = 0, 
  ROZOPRJ_DIR_SZ,
  ROZOPRJ_FILE_SZ,
  ROZORPJ_MAX
} rozofs_prj_tb_e;

#define ROZODU_MAX_PROJECTS 2048 
extern  int rozofs_project_count;  /**< current number of projects  */
extern  list_t rozofs_project_head;

typedef struct _rozofs_du_project_t
{
  list_t  list;                          /**< linked list of the projects */
  fid_t   fid;                           /**< root fid of the project     */
  uint32_t  tag;                         /**< local tag                   */
  char *name;                            /**< name of the project         */
  int count[ROZORPJ_MAX];                /**< current  count table count  */
  big_obj_t  *big_table_p[ROZORPJ_MAX];  /**<big tables                   */
  uint32_t nb_dir;                       /**< number of directories       */
  uint32_t nb_files;                     /**< number of files             */
  uint64_t total_bytes;;                 /**< table bytes                 */
  char *outpath;                         /**< output path                 */
} rozofs_du_project_t;



typedef struct _rozodu_parsed_entry_t
{
   int line;
   fid_t fid;
   char *projectname;
} rozodu_parsed_entry_t;


extern rozofs_du_project_t *rozofs_project_table[];
/*
**____________________________________________________
*/
/**
*  Search for a child file of a given parent

  @param parent_p : pointer to the parent context
  @param fid      : fid to search
  
  @retval 0 on success
  @retval -1 on error
*/
int rozodu_dir_search_pfid_fid(fid_t pfid,fid_t fid);

int  rozodu_init();

/*
**_______________________________________________________________________
*/
/**
   @param exportd : pointer to the exportd context
   @param check_fid: assert to one if the fid need to be check
   @param fid: fid when check_fid is asserted
*/
void rozodu_check(void *exportd,int check_fid,fid_t fid);
/*
**_______________________________________________________________________
*/
/**
   Apply the tag on the directories entries when all the arborescence 
   has been built   
   
   @param exportd : pointer to the exportd context
   
   @retval 0 success
   @retval -1 error (see errno for details)
*/
int rozodu_check_projects(void *exportd);


/*
**_______________________________________________________________________
*/
/**
   Insert the object in sorted table, When the table is full, the last entry is ejected
   
   @param table_p : current table
   @param count: pointer to the current number of entries in the table
   @param key: value used for sorting
   @param fid : inode if the object either a directory or file
*/
void rozodu_insert_sorted(big_obj_t *table_p,int *count,uint64_t key ,fid_t fid);

char *rozo_get_full_path(void *exportd,void *inode_p,char *buf,int lenmax);
char  *display_size(long long unsigned int number,char *buffer);

/*
**_______________________________________________________________________
*/
/**
  Check if the du operates in project mode
  
  @param none
  
  @retval 0 no
  @retval 1 yes
*/
int rozofs_is_project_enabled();

/*
**_______________________________________________________________________
*/
/**
  Init of the data used for managaning projects
  
  @param none
  
  @retval 0 on sucess
  @retval < 0 error (see errno for details)
*/
int rozofs_project_init();
/*
**_______________________________________________________________________
*/
/**
  Scan the input filename to figure out is some projects are defined
  
  @param input_path: either a FID or the filename that contains the FID of the projects
  @param eid: export identifier
  @param output_path: output pathname or NULL
  
  @retval 0 on success
  @retval -1 on error
*/
int rozofs_project_scan(char *input_path,int eid,char *output_path);

/*
**_______________________________________________________________________
*/
/**
   Apply the tag on the directories entries when all the arborescence 
   has been built   
   
   @param exportd : pointer to the exportd context
   
   @retval 0 success
   @retval -1 error (see errno for details)
*/
int rozodu_tag_apply_on_projects(void *exportd);
/*
**_______________________________________________________________________
*/
/**
*   Parse the input file that contains the relation between the projects and the fid

   @param filename: filename of the input file
   @param maxentries : not used
   @param entry_p : returned pointer to the beginning of the table
   
   @retval > 0 : number of projects
   @retval < 0 : error (see errno for details
*/
int rozodu_parse_file(char *filename,int max_entries,rozodu_parsed_entry_t **entry_p);


void rozodu_display_sorted_table(char *label,void *exportd,big_obj_t *table_p,int count,int type);

/**
***___________________________________________________________________________
        BUFFER OUT MANAGMENT
***___________________________________________________________________________
*/
#define ROZODU_BUF_SZ_OUT (1024*(512+16))
#define ROZODU_BUF_FLUSH_SZ_OUT (1024*512)
typedef struct  _rozodu_wrctx_t
{
    char *buf;       /**< pointer to the beginning of the buffer   */
    int   size;      /**< size of the buffer                       */
    int   cur_idx;   /**< current idx in the buffer                */
    FILE  *fd_out;   /**< out file descriptor                      */
    int    error;
} rozodu_wrctx_t;

extern rozodu_wrctx_t rozodu_outctx;
/*
**_______________________________________________________________________
*/
static inline int rozodu_bufout_create(rozodu_wrctx_t *p)
{
   p->buf = malloc(ROZODU_BUF_SZ_OUT);
   if (p->buf == NULL) return -1;
   p->size = ROZODU_BUF_SZ_OUT;
   p->cur_idx = 0;
   p->fd_out = NULL;
   p->error = 0;
   return 0;
}

/*
**_______________________________________________________________________
*/
static inline int rozodu_bufout_reinit(rozodu_wrctx_t *p,FILE *fd_out)
{
   p->cur_idx = 0;
   p->fd_out = fd_out;
   return 0;
}
/*
**_______________________________________________________________________
*/
static inline int rozodu_bufout_release(rozodu_wrctx_t *p)
{
   if (p->buf!= 0) free( p->buf);
   p->buf= NULL;
   p->size = 0;
   p->cur_idx = 0;
   return 0;
}

/*
**_______________________________________________________________________
*/
static inline int rozodu_bufout_flush(rozodu_wrctx_t *p)
{
   int ret;
   
   if (p->buf== 0) return 0;
   if (p->fd_out == NULL) return 0;
   if (p->cur_idx != 0)
   ret = fwrite(rozodu_outctx.buf,p->cur_idx,1,rozodu_outctx.fd_out);
   if (ret < 0)
   {
      if (p->error == 0 ) p->error = errno;
   }
   p->cur_idx = 0;
   if (p->error != 0) 
   {
     errno = p->error;
     return -1;
   }
   return 0;
}



#define ROZODU_PRINT(fmt, ...) \
{ \
  if (rozodu_outctx.fd_out==NULL) printf(fmt, ##__VA_ARGS__);\
  else \
  { \
    char *buf; \
    buf = &rozodu_outctx.buf[rozodu_outctx.cur_idx]; \
    rozodu_outctx.cur_idx += sprintf(buf,fmt, ##__VA_ARGS__); \
    if ( rozodu_outctx.cur_idx >= ROZODU_BUF_FLUSH_SZ_OUT) \
    { \
       int ret; \
       ret =fwrite(rozodu_outctx.buf,ROZODU_BUF_FLUSH_SZ_OUT,1,rozodu_outctx.fd_out); \
       if (ret < 0)\
       { \
         if (rozodu_outctx.error ==0) rozodu_outctx.error = errno; \
       } \
       if (rozodu_outctx.cur_idx > ROZODU_BUF_FLUSH_SZ_OUT) \
       { \
          memcpy(rozodu_outctx.buf,&rozodu_outctx.buf[ROZODU_BUF_FLUSH_SZ_OUT],rozodu_outctx.cur_idx-ROZODU_BUF_FLUSH_SZ_OUT); \
       } \
       rozodu_outctx.cur_idx = rozodu_outctx.cur_idx-ROZODU_BUF_FLUSH_SZ_OUT; \
    } \
  } \
} 



#endif
