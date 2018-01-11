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
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <rozofs/common/log.h>
#include <errno.h>
#include <rozofs/common/types.h>
#include <rozofs/core/uma_dbg_api.h>
#include "config.h"
#include "rozofs_suffix.h"


rozof_suffix_tb_t *rozofs_suffix_default_table_p = NULL;
rozofs_suffix_htable_t *rozofs_htable_tab_p[ROZOFS_MAX_SUFFIX_FILE];
rozofs_suffix_file_t *rozofs_file_tab_p[ROZOFS_MAX_SUFFIX_FILE];
/*
**__________________________________________________________________
*/
/**
*   Create a suffix table 

    @param nb_entries: number of entries in the suffix table
    
    @retval <> NULL: pointer to the suffix table
    @retval NULL: out of memory
*/
rozof_suffix_tb_t *rozofs_suffix_tb_create(int nb_entries)
{
   rozof_suffix_tb_t *suffix_tb_p = malloc(sizeof(rozof_suffix_tb_t));
   if (suffix_tb_p == NULL) return NULL;
   suffix_tb_p->nb_entries = nb_entries;
   suffix_tb_p->cur_entries=0;
   suffix_tb_p->table_p = malloc(sizeof(rozofs_suffix_entry_t)*nb_entries);
   if (suffix_tb_p->table_p == NULL)
   {
     free(suffix_tb_p);
     return NULL;
   }
   memset(suffix_tb_p->table_p,0,sizeof(rozofs_suffix_entry_t)*nb_entries);
   return suffix_tb_p;
}
/*
**__________________________________________________________________
*/
/**
*   Delete a suffix table 

    @param suffix_tb_p:  pointer to the suffix table
    
    @retval None

*/
void rozofs_suffix_tb_del(rozof_suffix_tb_t *suffix_tb_p)
{
   free(suffix_tb_p->table_p);
   free(suffix_tb_p); 
}

/*
**__________________________________________________________________
*/
/**
*  search for a suffix in a suffix table 

    @param suffix_tb_p:  pointer to the suffix table
    @param suffix2search: pointer to the suffix to search
    @param len2search: lenbgth of the suffix to search for
    
    @retval 1 found
    @retval -1 not found

*/
int rozofs_suffix_search(rozof_suffix_tb_t *suffix_tb_p,char *suffix2search,int len2search)
{

   int i;
   int k;
   rozofs_suffix_entry_t *entry_p;
   entry_p = suffix_tb_p->table_p;
   
   for (i=0; i < suffix_tb_p->cur_entries;i++,entry_p++)
   {
      if (len2search != entry_p->len) continue;
      for (k = 0; k < len2search; k++)
      {
         if (suffix2search[k]!=entry_p->suffix[k]) break;
      }
      if (k == len2search) return 1;
   }
   return -1;  
}

/*
**__________________________________________________________________
*/
/**
*   insert a suffix in a suffix table 

    @param suffix_tb_p:  pointer to the suffix table
    @param suffix_p: pointer to the suffix to insert (NULL termninated string)
    
    @retval 1 success
    @retval -1 error

*/
int rozofs_suffix_tb_insert(rozof_suffix_tb_t *suffix_tb_p,char *suffix_p)
{
   int len;
   int ret;
   rozofs_suffix_entry_t *entry_p;
   /*
   ** check if the table is full
   */
   if (suffix_tb_p->cur_entries >= suffix_tb_p->nb_entries) 
   {
      errno = ENOMEM;
      return -1;
   }
   len = strlen(suffix_p);
   if (len > ROZOFS_MAX_SUFFIX)
   {
      errno = ERANGE;
      return -1;
   }
   /*
   ** search if the suffix already exists
   */
   ret = rozofs_suffix_search(suffix_tb_p,suffix_p,len);
   if (ret >= 0)
   {
      errno = EEXIST;
      return -1;
   }
   entry_p = &suffix_tb_p->table_p[suffix_tb_p->cur_entries];
   entry_p->len = len;
   memcpy(entry_p->suffix,suffix_p,len);
   suffix_tb_p->cur_entries++;
   return 1;  
}




/*
**__________________________________________________________________
*/
int export_file_create_check_fast(char *name,rozof_suffix_tb_t *suffix_tb_p)
{
    int i;
    int ret;
    char *pbuf;
    int len = strlen(name);
    
    pbuf = name+len-1;
    for (i = 0; i < len; i++)
    {
      if (*pbuf == '.') break;
      pbuf--;
    }
    if ((i==0) || (i==len)) return -1;
    pbuf++;
    ret = rozofs_suffix_search(suffix_tb_p,pbuf,i);
    return ret;
}



/*
**__________________________________________________________________
*/
/**
*   Create a hash table associated with a given suffix file index
  
   @param file_idx
   
   @retval <> NULL pointer to the htable
   @retval NULL cannot allocate hashtable (see errno for details)
*/
rozofs_suffix_htable_t *rozofs_suffix_htable_create(int file_idx)
{
   rozofs_suffix_htable_t *tab_p;
   
   if (file_idx >= ROZOFS_MAX_SUFFIX_FILE)
   {
      errno = ERANGE;
      return NULL;
   } 
   tab_p = malloc(sizeof(rozofs_suffix_htable_t));
   if (tab_p == NULL)
   {
     errno = ENOMEM;
     return NULL;
   }
   memset(tab_p,0,sizeof(rozofs_suffix_htable_t));
   tab_p->file_idx = file_idx;
   htable_initialize(&tab_p->htable, ROZOFS_SUFFIX_HSIZE, rozofs_suffix_hash, rozofs_suffix_cmp);
   return tab_p;
}

/*
**__________________________________________________________________
*/
/**
*   insert a suffix in a suffix hash table 

    @param htable_p:  pointer to the suffix hash table
    @param suffix_p: pointer to the suffix to insert (NULL termninated string)
    
    @retval 1 success
    @retval -1 error

*/
int rozofs_suffix_htable_insert(rozofs_suffix_htable_t *htable_p,char *suffix_p)
{
   int len;

   rozofs_suffix_entry_t *entry_p;
   rozofs_suffix_entry_t *entry_ret_p;
   
   entry_p = malloc(sizeof(rozofs_suffix_entry_t));
   if (entry_p == NULL)
   {
     errno = ENOMEM;
     return -1;
   }
   memset(entry_p,0,sizeof(rozofs_suffix_entry_t));
   len = strlen(suffix_p);
   if (len > ROZOFS_MAX_SUFFIX)
   {
      errno = ERANGE;
      return -1;
   }

   entry_p->len = len;
   memcpy(entry_p->suffix,suffix_p,len);
   entry_p->suffix_p = &entry_p->suffix[0];
   /*
   ** search if the suffix already exists
   */
   entry_ret_p  = htable_get(&htable_p->htable,entry_p);
   if (entry_ret_p != NULL)
   {
     /*
     ** the entry already exists
     */
     errno = EEXIST;
     return -1;
   }
   /*
   ** insert the entry in the hash table
   */
   htable_p->nb_entries++;
   entry_p->he.key   = entry_p;
   entry_p->he.value = entry_p;
   entry_p->sufx_htable_p = htable_p;
   htable_put_entry(&htable_p->htable, &entry_p->he);
   return 1;  
}


/*
**__________________________________________________________________
*/
/**
*  search for a suffix in a suffix hash table 

    @param htable_p:  pointer to the suffix hash table
    @param suffix2search: pointer to the suffix to search
    @param len2search: lenbgth of the suffix to search for
    
    @retval 1 found
    @retval -1 not found

*/
int rozofs_suffix_htable_search(rozofs_suffix_htable_t *htable_p,char *suffix2search,int len2search)
{

 
   rozofs_suffix_entry_t entry;
   rozofs_suffix_entry_t *entry_ret_p;
   
   entry.len = len2search;
   entry.suffix_p = suffix2search;
   htable_p->nb_access++;
   htable_p->cur_collisions_count = 0;
   entry_ret_p  = htable_get(&htable_p->htable,&entry);
   if (entry_ret_p == NULL)
   {
     /*
     ** the does not exist
     */
     return -1;
   }
   return 1;  
}

/*
**__________________________________________________________________
*/
/**
*   hash table deletion 

    @param htable_p:  pointer to the suffix hash table
    
    @retval 1 success
    @retval -1 error

*/
int rozofs_suffix_htable_delete(rozofs_suffix_htable_t *htable_p)
{
#warning rozofs_suffix_htable_delete: not yet implemented
  warning("Not Yet implemented");
  return 0;
}

/*
**__________________________________________________________________
*/
/**
*   Search if the suffix of the file could be candidate for fast volume

  @param file_idx : index of the file that contains the suffix that has been associated with the exportd
  @param name: file name
  
  @retval
  @retval
*/
int export_file_create_check_fast_htable(char *name,int file_idx)
{
    int i;
    int ret;
    char *pbuf;
    int len = strlen(name);
    rozofs_suffix_htable_t *htable_p;
    
    /*
    ** check if there is an hash table entry for that file index
    ** if none: put the file on the regular volume
    */
    if (file_idx >= ROZOFS_MAX_SUFFIX_FILE)
    {
      return -1;
    }
    /*
    ** check if there is an entry
    */
    htable_p = rozofs_htable_tab_p[file_idx];
    if (htable_p == NULL)
    {
      /*
      ** That file index does not exist
      */
      return -1;
    }
       
    pbuf = name+len-1;
    for (i = 0; i < len; i++)
    {
      if (*pbuf == '.') break;
      pbuf--;
    }
    if ((i==0) || (i==len)) return -1;
    pbuf++;
    ret = rozofs_suffix_htable_search(htable_p,pbuf,i);
    return ret;
}


/**
**___________________________________________________________________________________________

      SUFFIX FILE MANAGEMENT PARSING & Periodic Thread
**___________________________________________________________________________________________
*/

/*_________________________________________________________________
 * Builds a string from an integer value supposed to be within
 * the enumerated list rozofs_suffix_state_e
 *
 * @param x : value from rozofs_suffix_state_e to translate into a string
 *
 * The input value is translated into a string deduced from the enum
 * definition. When the input value do not fit any of the predefined
 * values, "??" is returned
 *
 * @return A char pointer to the constant string or "??"
 *_________________________________________________________________*/ 
static inline char * rozofs_suffix_state_e2String (const rozofs_suffix_state_e x) {
  switch(x) {
    case ROZOFS_SUF_IDLE                         : return("IDLE");
    case ROZOFS_SUF_PARSING_IN_PRG               : return("PARSING IN PRG");
    case ROZOFS_SUF_PARSED                       : return("PARSED");
    case ROZOFS_SUF_PARSED_ERROR                 : return("PARSED ERROR");
    case ROZOFS_SUF_MAX                          : return("MAX");
    /* Unexpected value */
    default: return "??";
  }
}
/*_________________________________________________________________
*/
void rozofs_show_suffix_files(char * argv[], uint32_t tcpRef, void *bufRef)
{
    char *pChar = uma_dbg_get_buffer();
    rozofs_suffix_file_t *file_ctx_p;
    int file_idx;
    int empty = 1;
    for (file_idx = 0; file_idx < ROZOFS_MAX_SUFFIX_FILE ; file_idx++)
    {
       file_ctx_p = rozofs_file_tab_p[file_idx];
       if (file_ctx_p == NULL) continue;
       empty = 0;
       pChar += sprintf(pChar,"%s/suffix_file_%d.txt \n",ROZOFS_CONFIG_DIR,file_ctx_p->idx);
       pChar += sprintf(pChar," - State       : %s\n", rozofs_suffix_state_e2String(file_ctx_p->state));
       if (file_ctx_p->errmsg[0] != 0)
         pChar += sprintf(pChar," - ErrMsg      : %s\n", file_ctx_p->errmsg);
       pChar += sprintf(pChar," - Nb Suffixes : %d\n", file_ctx_p->nb_suffix);
;
     } 
     if (empty)
     {
        sprintf(pChar," no suffix file in use\n");
     }
     uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());     
}


/*_________________________________________________________________
*/
void rozofs_show_suffix_htable(char * argv[], uint32_t tcpRef, void *bufRef)
{
    char *pChar = uma_dbg_get_buffer();
    rozofs_suffix_htable_t *htable_p;
    int file_idx;
    int empty = 1;
    for (file_idx = 0; file_idx < ROZOFS_MAX_SUFFIX_FILE ; file_idx++)
    {
       htable_p = rozofs_htable_tab_p[file_idx];
       if (htable_p == NULL) continue;
       empty = 0;
       pChar += sprintf(pChar,"hash table of %s/suffix_file_%d.txt \n",ROZOFS_CONFIG_DIR,htable_p->file_idx);
       pChar += sprintf(pChar," - Total entries      : %d\n", htable_p->nb_entries);
       pChar += sprintf(pChar," - Collisions counter : %llu\n", (unsigned long long int)htable_p->collisions_count);
       pChar += sprintf(pChar," - Max collisions     : %llu\n", (unsigned long long int)htable_p->max_collisions_count);
       pChar += sprintf(pChar," - Total access count : %llu\n", (unsigned long long int)htable_p->nb_access);

     } 
     if (empty)
     {
        sprintf(pChar," no hash table in use\n");
     }
     uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());     
}


/*
**___________________________________________________________________________
*/
/**
*   Create an entry for a suffix file that need to be parsed

    @param file_idx : index of the file that need to be parsed
    
    @retval 0 on success
    @retval -1 on error (see errno for details)
*/
int rozofs_suffix_file_parse_req(int file_idx)
{
   rozofs_suffix_file_t *entry_p;
   /*
   ** check if the file is in range
   */
   if (file_idx >=  ROZOFS_MAX_SUFFIX_FILE)
   {
     errno = ERANGE;
     warning("Suffix file index is out of range (%d)",file_idx);
     return -1;
   }
   /*
   ** check if the entry exists and creates one if there is no entry
   */
   entry_p = rozofs_file_tab_p[file_idx];
   if (entry_p != NULL) return 0;
   
   entry_p=malloc(sizeof(rozofs_suffix_file_t));
   if (entry_p == NULL)
   {
      warning("Out of memory while creating data for suffix file %d",file_idx);
      errno = ENOMEM;
      return -1;
   }
   memset(entry_p,0,sizeof(rozofs_suffix_file_t));
   entry_p->idx = file_idx;
   /*
   ** save the context
   */
   rozofs_file_tab_p[file_idx] = entry_p;
   return 0;
}
/*
**___________________________________________________________________________
*/
/**
*  Parsing of a suffix file

   @param file_ctx_p : pointer to the context used for file parsing      


*/      
int export_suffix_parse_file(rozofs_suffix_file_t *file_ctx_p)
{
  FILE *fd_in;
  char *lineptr = NULL;
  int retsize;
  int nb_line = 0;
  size_t linesize = 0;
  char *token;
  char filename[1024];
  int ret;
  rozofs_suffix_htable_t *suf_htable_p = NULL;
  rozofs_suffix_htable_t *old_suf_htable_p = NULL;
  struct stat statbuf;
  
  sprintf(filename,"%s/suffix_file_%d.txt",ROZOFS_CONFIG_DIR,file_ctx_p->idx);
  file_ctx_p->state     = ROZOFS_SUF_PARSING_IN_PRG;
  file_ctx_p->errcode   = 0;
  file_ctx_p->errmsg[0] = 0;
  file_ctx_p->nb_suffix = 0;
  
    
  if ((fd_in = fopen(filename,"r")) == NULL)
  {
    file_ctx_p->errcode = errno;
    return -1;
  }
  /*
  ** allocate memory for the hash table
  */
  suf_htable_p =rozofs_suffix_htable_create(file_ctx_p->idx);
  if (suf_htable_p == NULL)
  {
    file_ctx_p->state = ROZOFS_SUF_PARSED_ERROR;
    file_ctx_p->errcode = errno;
    fclose(fd_in);
    return -1;  
  
  }
  /*
  ** read the line
  */
  
  while ((retsize = getline(&lineptr,&linesize,fd_in)) >= 0)
  {
     nb_line++;
     /**
     *  extract the project and the fid from the line
     */
     token =strtok(lineptr," ;=\n");
     if (token == NULL)
     {
       goto next;    
     }
     if (*token =='#') goto next;
     /*
     ** insert the suffix in the hash table
     */
     ret = rozofs_suffix_htable_insert(suf_htable_p,token);
     if (ret < 0)
     {
        /*
	** if the error code is different from EEXIST: stop the parsing
	*/
	if (errno != EEXIST)
	{
	   file_ctx_p->errcode = errno;
	   sprintf(file_ctx_p->errmsg,"Error while inserting token %s (line %d) in hash table (%s)",token,nb_line,strerror(errno));
	   goto error;
	
	}
     }
     else
     {
       file_ctx_p->nb_suffix++;
     }
     for (;;)
     {
       /*
       ** get the next token
       */
       token = strtok(NULL," ;=\n");
       if (token == NULL)
       {
          break;
       }
       if (*token =='#') break;
       /*
       ** insert the suffix in the hash table
       */
       ret = rozofs_suffix_htable_insert(suf_htable_p,token);
       if (ret < 0)
       {
          /*
	  ** if the error code is different from EEXIST: stop the parsing
	  */
	  if (errno != EEXIST)
	  {
	     file_ctx_p->errcode = errno;
	     sprintf(file_ctx_p->errmsg,"Error while inserting token %s (line %d) in hash table (%s)",token,nb_line,strerror(errno));
	     goto error;

	  }
       }
       else
       {
	 file_ctx_p->nb_suffix++;
       }
     }

next:
     free(lineptr);
     lineptr= NULL;

  }
  fclose(fd_in);
  /*
  ** get the mtime of the file
  */
  ret = stat(filename,&statbuf);
  if (ret >=0)
  {
    file_ctx_p->mtime = statbuf.st_mtime; 
  }
  /*
  ** swap the hash table
  */
  old_suf_htable_p = rozofs_htable_tab_p[file_ctx_p->idx];
  rozofs_htable_tab_p[file_ctx_p->idx] = suf_htable_p;
  if (old_suf_htable_p !=NULL)
  {
     /*
     ** just wait for a while and them delete the hash table
     */
     sleep(1);
     rozofs_suffix_htable_delete(old_suf_htable_p);
  }
  
  file_ctx_p->state = ROZOFS_SUF_PARSED;
  
  return file_ctx_p->nb_suffix;

error:
  file_ctx_p->state = ROZOFS_SUF_PARSED_ERROR;
  if (suf_htable_p != NULL) rozofs_suffix_htable_delete(suf_htable_p);
  fclose(fd_in);
  return -1;    
}



/**
*  RozoFS suffix periodic thread

   The purpose of that thread is to  check if there is some files to parse
   
*/
void rozofs_suffix_periodic_thread (void *v)
{

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    int loop_first = 1;
    int file_idx;
    rozofs_suffix_file_t *file_ctx_p;
    char filename[1024];
    struct stat statbuf;
    int ret;

    /*
    ** Record thread name
    */
    uma_dbg_thread_add_self("Suffix_th");
    struct timespec ts = {ROZOFS_SUFFIX_PTHREAD_FREQUENCY_SEC, 0};
    
    for(;;)
    {
      if (loop_first == 1) ts.tv_sec = 1;
      else ts.tv_sec = ROZOFS_SUFFIX_PTHREAD_FREQUENCY_SEC;
      nanosleep(&ts,NULL);      
      /*
      ** Go through the table suffix file to find out the files that needs parsing
      */
      for (file_idx = 0; file_idx < ROZOFS_MAX_SUFFIX_FILE ; file_idx++)
      {
         file_ctx_p = rozofs_file_tab_p[file_idx];
	 if (file_ctx_p == NULL) continue;
	 sprintf(filename,"%s/suffix_file_%d.txt",ROZOFS_CONFIG_DIR,file_ctx_p->idx);
	 ret = stat(filename,&statbuf);
	 if (ret < 0)
	 {
	   file_ctx_p->errcode = errno;
	   file_ctx_p->state = ROZOFS_SUF_PARSED_ERROR;
	   sprintf(file_ctx_p->errmsg,"cannot stat %s  error:%s",filename,strerror(errno));
	   continue;	  
	 }
	 if ( file_ctx_p->mtime == statbuf.st_mtime) continue;
	 /*
	 ** attempt to parse the file and create the associated hash table
	 */ 	 
         export_suffix_parse_file(file_ctx_p);
       }
    }
}



/**
**___________________________________________________________________________________________

      SUFFIX FILE MANAGEMENT INIT
**___________________________________________________________________________________________
*/

/*
**__________________________________________________________________
*/
/**
*   init of the default suffix table 

    
    @retval 1 success
    @retval -1 error

*/

static pthread_t suffix_thread=0;         /**< pid of periodic parsing suffix thread       */
int rozofs_suffix_tb_init()
{
  int ret=0;
  
   memset(rozofs_htable_tab_p,0,sizeof(htable_t *)*ROZOFS_MAX_SUFFIX_FILE);
   memset(rozofs_file_tab_p,0,sizeof(rozofs_suffix_file_t *)*ROZOFS_MAX_SUFFIX_FILE);
   /*
   ** create the peirodic thread
   */
   if ((errno = pthread_create(&suffix_thread, NULL, (void*) rozofs_suffix_periodic_thread, NULL)) != 0) {
        severe("can't create suffix periodic thread: %s", strerror(errno));
  }
   
   /*
   ** add the debug topic
   */
   uma_dbg_addTopic("show_suffix_file",rozofs_show_suffix_files);
   uma_dbg_addTopic("show_suffix_htable",rozofs_show_suffix_htable);
   
   return ret;  
} 
