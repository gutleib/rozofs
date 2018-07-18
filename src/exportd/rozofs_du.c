
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
#include <rozofs/rozofs.h>
#include <rozofs/common/list.h>
#include <inttypes.h>
#include <assert.h>
#include <rozofs/common/htable.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/mattr.h>
#include <rozofs/core/rozofs_fid_string.h>
#include "export.h"
#include "rozo_inode_lib.h"
#include "exp_cache.h"
#include "rozofs_du.h"


htable_t htable_directory;
list_t  dir_head;
list_t  dir_orphan_head;
uint64_t hash_inode_collisions_count;
uint64_t hash_inode_max_collisions;
uint64_t hash_inode_cur_collisions;
fid_t fid_null;
int root_found = 0;
uint64_t memory_allocated = 0;
int  rozofs_project_count;  /**< current number of projects  */
list_t rozofs_project_head;
rozofs_du_project_t *rozofs_project_table[ROZODU_MAX_PROJECTS];


static inline int fid_cmp(void *key1, void *key2) {

      int ret;
      ret =  memcmp(key1, key2, sizeof (fid_t));
      if (ret != 0) {
          hash_inode_collisions_count++;
	  hash_inode_cur_collisions++;
	  return ret;
      }
      if (hash_inode_max_collisions < hash_inode_cur_collisions) hash_inode_max_collisions = hash_inode_cur_collisions;
      return ret;

}

static inline unsigned int fid_hash(void *key) {
    uint32_t hash = 0;
    uint8_t *c;
    for (c = key; c != key + 16; c++)
        hash = *c + (hash << 6) + (hash << 16) - hash;
    return hash;
}
/*
**____________________________________________________
*/
/** initialize an  dentry cache
 *
 * @param cache: the cache to initialize
 */
void rozodu_cache_initialize() {

    htable_initialize(&htable_directory, ROZODU_CACHE_BUCKETS, fid_hash, fid_cmp);
}
/*
**____________________________________________________
*/
/**
   Create an entry for a directory FID and insert it in the hash table
   
   @param fid: fid to insert
   
   @retval <> NULL : pointer to the entry
   @retval ==NULL: out of memory
*/
rozofs_dir_layout_t *rozodu_alloc_entry(fid_t fid)
{

   rozofs_dir_layout_t  *p = malloc(sizeof(rozofs_dir_layout_t));
   if (p == NULL) return NULL;
   
   memory_allocated+=sizeof(rozofs_dir_layout_t);
   memset(p,0,sizeof(rozofs_dir_layout_t));
   /*
   ** insert the entry in the hash table
   */
   memcpy(p->fid,fid,sizeof(fid_t));
   p->he.key   = p->fid;
   p->he.value = p;
   list_init(&p->child_head);
   list_init(&p->link);
   htable_put_entry(&htable_directory, &p->he);
   return p;
}
  
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


int rozodu_dir_search_pfid_fid(fid_t pfid,fid_t fid)
{

  rozofs_dir_layout_t  *child_p;
  rozofs_dir_layout_t  *parent_p;
  /*
  ** search for the parent fid entry
  */
  parent_p = htable_get(&htable_directory, pfid);
  if (parent_p == NULL)
  {
    /*
    ** need to create an entry
    */
    parent_p = rozodu_alloc_entry(pfid);
    if (parent_p == NULL)
    {
       return -1;
    }
    /*
    ** insert the entry in the orphan list
    */
    list_push_front(&dir_orphan_head,&parent_p->link);        
  }
  /*
  ** search for the child: it might possible that the child already exists
  */
  child_p = htable_get(&htable_directory, fid);
  if (child_p == NULL)
  {
    /*
    ** create an entry for the child
    */
    child_p = rozodu_alloc_entry(fid);
    if (child_p == NULL)
    {
       return -1;
    }
  }
  /*
  ** update the count of children directories on the parent
  */
  list_remove(&child_p->link);
  parent_p->nb_dir+=1;
  /*
  ** linked the child directory with the parent (push front)
  */
  list_push_front(&parent_p->child_head,&child_p->link);  
  if (root_found==0)
  {
     if (memcmp(pfid, fid_null, sizeof (fid_t)) == 0)
     {
        //printf("Root inode found\n");
        root_found=1;
	list_remove(&parent_p->link);
        list_push_front(&dir_head,&parent_p->link);   	
     }
  }
  return 0;
}
    
    
/*
**____________________________________________________
*/

int  rozodu_init()
{
  memset(fid_null,0,sizeof(fid_t));

  rozodu_cache_initialize();
   list_init(&dir_head);
   list_init(&dir_orphan_head);
  hash_inode_collisions_count=0;
  hash_inode_max_collisions=0;
  hash_inode_cur_collisions=0;
  return 0;
  
}

char buffer_all[64];
int  stack_level = 0;
int  stack_level_max = 0;
char bufpath[1024];
char bufisze[64];

/*
**_______________________________________________________________________
*/
/**
 Dump a directory entry
  
  @param export: pointer to exportd context
  @param entry_p: directory entry
  @param prj_p: pointer to the project , might be NULL if there is no project
  
  @retval none
*/
void rozodu_dump_dir_entry(void *exportd,rozofs_dir_layout_t *entry_p,rozofs_du_project_t *prj_p)
{
    list_t * p=NULL;
    rozofs_dir_layout_t *dir_child_p;
    
    stack_level++;
    
//    if (rozodu_verbose)
    {
       /*
       ** skip empty directories
       */
//       if (entry_p->nb_files !=0)
       {
	 lv2_entry_t *plv2;
	 ext_mattr_t *inode_attr_p;
	 char *pChar;
	 export_t *e= exportd;    

	 plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,&cache, entry_p->fid);
	 if (plv2 != NULL)
	 {
	   inode_attr_p=  &plv2->attributes;
	   pChar = rozo_get_full_path(exportd,inode_attr_p, bufpath,sizeof(bufpath));
	   if (pChar) 
	   {
	     if(rozodu_dironly)
	     {
	       ROZODU_PRINT("%s\n",pChar);
	     }
	     else
	     {
               if (format_bytes )
	       {
        	 ROZODU_PRINT("%-64s : (%u) %llu\n",pChar,entry_p->nb_files,(unsigned long long int)entry_p->nb_bytes);
	       }
	       else 
	       {
		 ROZODU_PRINT("%-64s : (%u) %s\n",pChar,entry_p->nb_files,display_size(entry_p->nb_bytes,bufisze));
	       }
	     }
	   } 
	 } 
       }    
    }
    
//    rozofs_uuid_unparse(entry_p->fid,buffer_all);
//    printf("FID : %s count:%u file %u bytes : %llu\n",buffer_all,entry_p->nb_dir,entry_p->nb_files,entry_p->nb_bytes);    

   if (entry_p->nb_files != 0)
   {
      /*
      ** attempt to insert the directory in the big dir sorted tables
      */
      rozodu_insert_sorted(rozodu_big_dir_count_table,&rozodu_big_dir_count_table_sz,(uint64_t)entry_p->nb_files,entry_p->fid);
      rozodu_insert_sorted(rozodu_big_dir_size_table,&rozodu_big_dir_size_table_sz,entry_p->nb_bytes,entry_p->fid);
      if (prj_p!=NULL)
      {
	rozodu_insert_sorted( prj_p->big_table_p[ROZOPRJ_DIR_CNT],&prj_p->count[ROZOPRJ_DIR_CNT],(uint64_t)entry_p->nb_files,entry_p->fid);
	rozodu_insert_sorted(prj_p->big_table_p[ROZOPRJ_DIR_SZ],&prj_p->count[ROZOPRJ_DIR_SZ],entry_p->nb_bytes,entry_p->fid);
      }
    }
    list_for_each_forward(p, &entry_p->child_head) 
    {
      dir_child_p = list_entry(p, rozofs_dir_layout_t, link);
      rozodu_dump_dir_entry(exportd,dir_child_p,prj_p);
    
    }
    if (stack_level > stack_level_max) stack_level_max = stack_level;
    stack_level--;
}
/*
**_______________________________________________________________________
*/
/**
  Init of the data used for managaning projects
  
  @param none
  
  @retval 0 on sucess
  @retval < 0 error (see errno for details)
*/
int rozofs_project_init()
{

  rozofs_project_count = 0;
  list_init(&rozofs_project_head);
  memset(rozofs_project_table,0,sizeof(rozofs_du_project_t *)*ROZODU_MAX_PROJECTS);
  return 0;
}

/*
**_______________________________________________________________________
*/
/**
  Check if the du operates in project mode
  
  @param none
  
  @retval 0 no
  @retval 1 yes
*/
int rozofs_is_project_enabled()
{

  if (list_empty(&rozofs_project_head)==1) return 0;
  return 1;
}

/*
**_______________________________________________________________________
*/
/**
  allocate a data structure for storing a project
  
  @param name : project label
  @param fid: fid associated with the project (it should be a FID of a directory
  
  @retval 0 on sucess
  @retval < 0 error (see errno for details)
*/
int rozofs_project_allocate(char *name,fid_t fid,char *output_path)
{

  rozofs_du_project_t *prj_p;
  int i;
  
  if (rozofs_project_count >= (ROZODU_MAX_PROJECTS-1))
  {
    printf("Too Many projects (max is %d\n",ROZODU_MAX_PROJECTS-1);
    return -1;
  }
  prj_p = malloc(sizeof(rozofs_du_project_t));
  if (prj_p == NULL)
  {
     return -1;
  }
  
  memset(prj_p,0,sizeof(rozofs_du_project_t));
  /*
  ** allocate memory for storing big dir/file count or sze
  */
  for (i=0; i < ROZORPJ_MAX;i++)
  {
     prj_p->big_table_p[i]=malloc((ROZODU_MAX_OBJ+1)*sizeof(big_obj_t));
     if(prj_p->big_table_p[i]==NULL)
     {
       return -1;
     } 
  }
  /*
  ** allocate the tag for the project
  */
  rozofs_project_count++;
  prj_p->tag = rozofs_project_count;
  /*
  ** init of the list
  */
  list_init(&prj_p->list);
  /*
  ** insert the project in the list
  */
  list_push_back(&rozofs_project_head,&prj_p->list); 
  /*
  ** insert the project name and the fid
  */
  prj_p->name = strdup(name);
  memcpy(prj_p->fid,fid,sizeof(fid_t));
  prj_p->outpath = output_path;
  /*
  ** insert in the table
  */
  rozofs_project_table[rozofs_project_count]= prj_p;

  return 0;
}
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
int rozofs_project_scan(char *input_path,int eid,char *output_path)
{
  rozofs_inode_t *inode_val_p;
  fid_t fid;
  int ret;
  rozodu_parsed_entry_t *entry_parse_p;
  int nb_prj = 0;
  int i;

  if (input_path == NULL) return 0;
  /*
  ** check for unique fid
  */
  if (rozofs_uuid_parse(input_path, fid)<0) 
  {
    
    /*
    **  check the presence of the input file
    */  
    
    nb_prj = rozodu_parse_file(input_path,0,&entry_parse_p);
    if (nb_prj < 0)
    {
       /*
       ** error while parsing the input file
       */
       return -1;
    }
    /*
    ** OK, the input file has been parsed, go through the entries and creat them
    */
    for (i = 0; i < nb_prj; i++,entry_parse_p++)
    {

      /*
      ** check if the FID matches the export identifier
      */
      inode_val_p = (rozofs_inode_t *) entry_parse_p->fid;
      if (eid != inode_val_p->s.eid)
      {
	printf("project %s at the line %d: EID mismatch (reference->%d inode eid->%d)\n",entry_parse_p->projectname,entry_parse_p->line,eid,inode_val_p->s.eid);
	errno = EINVAL;
	return -1;    
      }    
      /*
      ** insert the projection in the project list
      */
      if (rozodu_verbose) {
         char bufout[64];
	 uuid_unparse(entry_parse_p->fid,bufout);
	 printf("Inserting %s with fid %s \n",entry_parse_p->projectname,bufout);
      
      }
      ret = rozofs_project_allocate(entry_parse_p->projectname,entry_parse_p->fid,output_path);
      if (ret < 0)
      {
	printf("error while inserting %s in project list\n",entry_parse_p->projectname);
	return -1;    
      }          
    }
  }
  else
  {
     /*
     ** check if the FID matches the export identifier
     */
     inode_val_p = (rozofs_inode_t *) fid;
     if (eid != inode_val_p->s.eid)
     {
       printf("EID mismatch (reference->%d inode eid->%d\n",eid,inode_val_p->s.eid);
       return -1;    
     }
     /*
     ** insert the projection in the project list
     */
     ret = rozofs_project_allocate(input_path,fid,output_path);
     if (ret < 0)
     {
       printf("error while inserting %s in project list\n",input_path);
       return -1;    
     }  
  }
  return 0;
}
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

rozodu_parsed_entry_t rozodu_prj_tb[ROZODU_MAX_PROJECTS];


int rozodu_parse_file(char *filename,int max_entries,rozodu_parsed_entry_t **entry_p)
{
  FILE *fd_in;
  char *lineptr = NULL;
  int retsize;
  int nb_line = 0;
  size_t linesize = 0;
  char *token;
  rozodu_parsed_entry_t *cur_entry_p;
  int entry_count = 0;
  
  
  memset(rozodu_prj_tb,0,sizeof(rozodu_prj_tb));
  
  if ((fd_in = fopen(filename,"r")) == NULL)
  {
    return -1;
  }
  /*
  ** read the line
  */
  
  while ((retsize = getline(&lineptr,&linesize,fd_in)) >= 0)
  {
     cur_entry_p = &rozodu_prj_tb[entry_count];
     nb_line++;
     cur_entry_p->line = nb_line;
     /**
     *  extract the project and the fid from the line
     */
     token =strtok(lineptr," ;=\n");
     if (token == NULL)
     {
       goto next;    
     }
     if (*token =='#') goto next;
     
     cur_entry_p->projectname = strdup(token);
     /*
     ** get the fid
     */
     token = strtok(NULL," ;=\n");
     if (token == NULL)
     {
       printf("error at line %d while parsing file %s (%d)\n",nb_line,filename,__LINE__); 
       goto error;   
     }  
     if (uuid_parse(token,cur_entry_p->fid) != 0)
     {
       printf("error at line %d (bad fid) while parsing file %s (%d)\n",nb_line,filename,__LINE__);  
       goto error;        
     
     }
     entry_count++;

next:
     free(lineptr);
     lineptr= NULL;
     if (entry_count == ROZODU_MAX_PROJECTS)
     {
       printf("too many projects entries (max is %u\n",ROZODU_MAX_PROJECTS);  
       goto error;       
     
     }
  }
  fclose(fd_in);
  if (entry_p != NULL) *entry_p = rozodu_prj_tb;
  return entry_count;


error:
  fclose(fd_in);
  return -1;    
}

/*
**_______________________________________________________________________
*/
/**
   Tag an entry with the input tag
   
   @param entry_p: entry to tag
   @param tag: tag associated with the project
*/
void rozodu_tag_dir_entry(rozofs_dir_layout_t *entry_p,uint32_t tag)
{
    list_t * p=NULL;
    rozofs_dir_layout_t *dir_child_p;
    
    entry_p->opaque = tag;  
    /*
    ** attempt to insert the directory in the big dir sorted tables
    */
    list_for_each_forward(p, &entry_p->child_head) 
    {
      dir_child_p = list_entry(p, rozofs_dir_layout_t, link);
      rozodu_tag_dir_entry(dir_child_p,tag);    
    }
}
/*
**_______________________________________________________________________
*/
/**
   Tag a path relative to a project, the tag is the local project index
   allocated at the project creation time
   
   @param exportd : pointer to the exportd context
   @param fid: fid when check_fid is asserted
   @param tag: project tag
   
   @retval 0 success
   @retval -1 error (see errno for details)
*/
int rozodu_tag_path(void *exportd,fid_t fid,uint32_t tag)
{

  list_t * p;
  p = NULL;
  list_t *head_p=NULL;
  rozofs_dir_layout_t *dir_p;
  char buffer[64];
  rozofs_dir_layout_t *entry_p = NULL;

  /*
  ** search for the fid if present in the input parameters
  */
  entry_p = htable_get(&htable_directory, fid); 
  if (entry_p == NULL)
  {
    /*
    ** search relative root directory is not found
    */
    rozofs_uuid_unparse(fid,buffer);
    printf("directory with local reference @rozofs_uuid@%s is not found\n",buffer);   
    errno = ENOENT;
    return -1; 
  }
  /*
  ** tag the beginning
  */
  entry_p->opaque = tag;
  head_p = &entry_p->child_head;

  list_for_each_forward(p, head_p) 
  {      
    dir_p = list_entry(p, rozofs_dir_layout_t, link);
    rozodu_tag_dir_entry(dir_p,tag);
  }
  return 0;
}

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
int rozodu_tag_apply_on_projects(void *exportd)
{
  list_t * p;
  p = NULL;
  int ret=0;
  rozofs_du_project_t *prj_p;
  
  list_for_each_forward(p, &rozofs_project_head) 
  {      
    prj_p = list_entry(p, rozofs_du_project_t, list);
    if (rozodu_verbose) {
       char bufout[64];
       uuid_unparse(prj_p->fid,bufout);
       printf("Inserting %s with fid %s \n",prj_p->name,bufout);

    }
    ret = rozodu_tag_path(exportd,prj_p->fid,prj_p->tag);
    if (ret == -1) break;
  }
  return ret;
}
  
/*
**_______________________________________________________________________
*/
/**
   @param exportd : pointer to the exportd context
   @param check_fid: assert to one if the fid need to be check
   @param fid: fid when check_fid is asserted
*/
void rozodu_check(void *exportd,int check_fid,fid_t fid)
{

  list_t * p;
  p = NULL;
  list_t *head_p=NULL;
  rozofs_dir_layout_t *dir_p;
  char buffer[64];
  rozofs_dir_layout_t *entry_p = NULL;
  
  if (rozodu_verbose)
  {
    printf("Head is %s\n",(list_empty(&dir_head)==1)?"empty":"not empty");
    printf("Orphan is %s\n",(list_empty(&dir_orphan_head)==1)?"empty":"not empty");
    printf("\n");
  }
  if (check_fid)
  {  
    /*
    ** search for the fid if present in the input parameters
    */
    entry_p = htable_get(&htable_directory, fid); 
    if (entry_p == NULL)
    {
      /*
      ** search relative root directory is not found
      */
      rozofs_uuid_unparse(fid,buffer);
      printf("directory with local reference @rozofs_uuid@%s is not found\n",buffer);   
      return; 
    }
    head_p = &entry_p->child_head;
    if (rozodu_verbose)
    {
       /*
       ** skip empty directories
       */
       /*if (entry_p->nb_files !=0)*/
       {
	 lv2_entry_t *plv2;
	 ext_mattr_t *inode_attr_p;
	 char *pChar;
	 export_t *e= exportd;    

	 plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,&cache, entry_p->fid);
	 if (plv2 != NULL)
	 {
	   inode_attr_p=  &plv2->attributes;
	   pChar = rozo_get_full_path(exportd,inode_attr_p, bufpath,sizeof(bufpath));
	   if (pChar) 
	   {
             if (format_bytes )
	     {
               printf("%-64s : (%u) %llu\n",pChar,entry_p->nb_files,(unsigned long long int)entry_p->nb_bytes);
	     }
	     else
	     {
	       printf("%-64s : (%u) %s\n",pChar,entry_p->nb_files,display_size(entry_p->nb_bytes,bufisze));
	     }
	   }
	 } 
       }    
    }
  }
  else
  {
    head_p = &dir_head;
  }
  list_for_each_forward(p, head_p) 
  {      
    dir_p = list_entry(p, rozofs_dir_layout_t, link);
    rozodu_dump_dir_entry(exportd,dir_p,NULL);
  }
  if (rozodu_verbose)
  {
      printf("stack_level %d\n",stack_level_max);
      printf("Memory allocated : %llu\n",(unsigned long long int)memory_allocated);
  }
}


/*
**_______________________________________________________________________
*/
/**
   @param exportd : pointer to the exportd context
   @param fid: fid when check_fid is asserted
   @param prj_p: pointer to the project context
*/
void rozodu_check_one_project(void *exportd,fid_t fid,rozofs_du_project_t *prj_p)
{
  list_t * p;
  p = NULL;
  FILE *fd_out = NULL;
  list_t *head_p=NULL;
  char pathname[1024];
  char buffer[64];
  rozofs_dir_layout_t *entry_p = NULL;
  rozofs_dir_layout_t *dir_p;
  errno = 0;
  /*
  ** open the output file if the path is defined
  */
  if (prj_p->outpath != NULL)
  {
    sprintf(pathname,"%s/%s",prj_p->outpath,prj_p->name);
    if ((fd_out = fopen(pathname,"w")) == NULL)
    {
      printf("Error while attempting to open %s (%s) for project %s-> skip it\n",pathname,strerror(errno),prj_p->name);
      return ;
    }    
    rozodu_bufout_reinit(&rozodu_outctx,fd_out);
  }
  if (prj_p->name != NULL) ROZODU_PRINT("\t\t-- %s --\n",prj_p->name);
  /*
  ** search for the fid if present in the input parameters
  */
  entry_p = htable_get(&htable_directory, fid); 
  if (entry_p == NULL)
  {
    /*
    ** search relative root directory is not found
    */
    rozofs_uuid_unparse(fid,buffer);
    printf("directory with local reference @rozofs_uuid@%s is not found\n",buffer);   
    ROZODU_PRINT("directory with local reference @rozofs_uuid@%s is not found\n",buffer); 
    goto out; 
  }
  head_p = &entry_p->child_head;
  if (rozodu_verbose)
  {
     /*
     ** skip empty directories
     */
     /*if (entry_p->nb_files !=0)*/
     {
       lv2_entry_t *plv2;
       ext_mattr_t *inode_attr_p;
       char *pChar;
       export_t *e= exportd;    

       plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,&cache, entry_p->fid);
       if (plv2 != NULL)
       {
	 inode_attr_p=  &plv2->attributes;
	 pChar = rozo_get_full_path(exportd,inode_attr_p, bufpath,sizeof(bufpath));
	 if (pChar) 
	 {
           if (format_bytes )
	   {
             ROZODU_PRINT("%-64s : (%u) %llu\n",pChar,entry_p->nb_files,(unsigned long long int)entry_p->nb_bytes);
	   }
	   else
	   {
	     ROZODU_PRINT("%-64s : (%u) %s\n",pChar,entry_p->nb_files,display_size(entry_p->nb_bytes,bufisze));
	   }
	 }
       } 
     }    
  }
    
  {
    rozodu_insert_sorted(rozodu_big_dir_count_table,&rozodu_big_dir_count_table_sz,(uint64_t)entry_p->nb_files,entry_p->fid);
    rozodu_insert_sorted(rozodu_big_dir_size_table,&rozodu_big_dir_size_table_sz,entry_p->nb_bytes,entry_p->fid);
    rozodu_insert_sorted( prj_p->big_table_p[ROZOPRJ_DIR_CNT],&prj_p->count[ROZOPRJ_DIR_CNT],(uint64_t)entry_p->nb_files,entry_p->fid);
    rozodu_insert_sorted(prj_p->big_table_p[ROZOPRJ_DIR_SZ],&prj_p->count[ROZOPRJ_DIR_SZ],entry_p->nb_bytes,entry_p->fid);
  }

  list_for_each_forward(p, head_p) 
  {      
    dir_p = list_entry(p, rozofs_dir_layout_t, link);
    rozodu_dump_dir_entry(exportd,dir_p,prj_p);
  }
  if (rozodu_dironly == 0)
  {
    rozodu_display_sorted_table("Big Directory count table",exportd,prj_p->big_table_p[ROZOPRJ_DIR_CNT],prj_p->count[ROZOPRJ_DIR_CNT],0);
    rozodu_display_sorted_table("Big Directory size table",exportd,prj_p->big_table_p[ROZOPRJ_DIR_SZ],prj_p->count[ROZOPRJ_DIR_SZ],1);
    rozodu_display_sorted_table("Big file size table",exportd,prj_p->big_table_p[ROZOPRJ_FILE_SZ],prj_p->count[ROZOPRJ_FILE_SZ],1);
  }
out:
  if (fd_out!=NULL)
  {

    rozodu_bufout_flush(&rozodu_outctx);
    fclose(fd_out);   
    printf("%s has been generated for project %s: status (%s)\n",pathname,prj_p->name,strerror(errno));
  }
}
  
  
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
int rozodu_check_projects(void *exportd)
{
  list_t * p;
  p = NULL;
  int ret=0;
  rozofs_du_project_t *prj_p;
  
  list_for_each_forward(p, &rozofs_project_head) 
  {      
    prj_p = list_entry(p, rozofs_du_project_t, list);
    rozodu_check_one_project(exportd,prj_p->fid,prj_p);

  }
  return ret;
}
  
  

