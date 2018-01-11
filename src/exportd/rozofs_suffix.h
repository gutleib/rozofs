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
#ifndef ROZOFS_SUFFIX_H
#define ROZOFS_SUFFIX_H
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <errno.h>
#include <rozofs/common/htable.h>

#define ROZOFS_MAX_SUFFIX_FILE 32
#define ROZOFS_SUFFIX_HSIZE 256
#define ROZOFS_MAX_SUFFIX 15 
#define ROZOFS_MAX_DEFAULT_SUFFIX 64

#define ROZOFS_SUFFIX_PTHREAD_FREQUENCY_SEC 10
typedef struct _rozofs_suffix_entry_t
{
   hash_entry_t he;
   void    *sufx_htable_p;   /**< pointer to the hash table structure  */
   char    *suffix_p;        /**< pointer to the suffix array          */
   uint8_t len;  /**< suffix length   */
   char    suffix[ROZOFS_MAX_SUFFIX];  /**< suffix within NULL termination */
} rozofs_suffix_entry_t;

typedef struct _rozof_suffix_tb_t
{
   int nb_entries;  /**< number of entries in the suffix table */
   int cur_entries; /**< number of entries in use */
   rozofs_suffix_entry_t *table_p;  /**< pointer to the suffix table */
} rozof_suffix_tb_t;


typedef struct _rozofs_suffix_htable_t
{
    int file_idx;  /**< suffix file index   */
    int nb_entries;  /**< number of entries in the hashtable */
    uint64_t collisions_count;  /**< number of collisions */
    uint64_t cur_collisions_count; /**< current number of collisions */
    uint64_t max_collisions_count; /**< max number of collisions     */
    uint64_t nb_access;            /**< total number of access (get) */
    htable_t htable;               /**< hash table */
} rozofs_suffix_htable_t;    

/*
** structure used by suffix thread to detect if same suffix files must be parsed
*/
typedef enum {
   ROZOFS_SUF_IDLE = 0,
   ROZOFS_SUF_PARSING_IN_PRG,
   ROZOFS_SUF_PARSED,
   ROZOFS_SUF_PARSED_ERROR,
   ROZOFS_SUF_MAX
} rozofs_suffix_state_e;

typedef struct _rozofs_suffix_file_t
{
   int idx;                      /**< index of the suffix file */
   int nb_suffix;                /**< number of suffix in the file */
   rozofs_suffix_state_e state;  /**< cutrent state of parsing */
   int errcode;                  /**< 0 when there is no error, otherwise contains the errno  */
   char errmsg[256];             /**< pointer to the error message */
   uint64_t mtime;               /**< file mtime  */
} rozofs_suffix_file_t;


extern rozof_suffix_tb_t *rozofs_suffix_default_table_p ;
extern rozofs_suffix_htable_t *rozofs_htable_tab_p[];
extern rozofs_suffix_file_t *rozofs_file_tab_p[];

/*
**__________________________________________________________________
*/
/**
*   Create a suffix table 

    @param nb_entries: number of entries in the suffix table
    
    @retval <> NULL: pointer to the suffix table
    @retval NULL: out of memory
*/
rozof_suffix_tb_t *rozofs_suffix_tb_create(int nb_entries);
/*
**__________________________________________________________________
*/
/**
*   Delete a suffix table 

    @param suffix_tb_p:  pointer to the suffix table
    
    @retval None

*/
void rozofs_suffix_tb_del(rozof_suffix_tb_t *suffix_tb_p);
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
int rozofs_suffix_search(rozof_suffix_tb_t *suffix_tb_p,char *suffix2search,int len2search);
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
int rozofs_suffix_tb_insert(rozof_suffix_tb_t *suffix_tb_p,char *suffix_p);

/*
**__________________________________________________________________
*/
int export_file_create_check_fast(char *name,rozof_suffix_tb_t *suffix_tb_p);

/*
**__________________________________________________________________
*/
/**
*   init of the default suffix table 

    
    @retval 1 success
    @retval -1 error

*/
int rozofs_suffix_tb_init();

/*
**__________________________________________________________________
*/
static inline uint32_t rozofs_suffix_hash( void *key1) {

    rozofs_suffix_entry_t *entry_p;
    entry_p = (rozofs_suffix_entry_t*)key1;
    unsigned char *d = (unsigned char *) entry_p->suffix_p;
    int i = 0;
    int h;

     h = 2166136261U;
    /*
     ** hash on name
     */

    for (i = 0; i <entry_p->len ; d++, i++) {
        h = (h * 16777619)^ *d;
    }
    return (uint32_t) h;
}

/*
**__________________________________________________________________
*/
/*
**  Compare the content of the hash table entry with the entry to search

  @param v1: current entry of the hashtable
  @param v2: entry to search

  @retval 1: no match
  @retval 0: match
*/
static inline int rozofs_suffix_cmp(void *v1, void *v2) {
      int ret;
      rozofs_suffix_entry_t *key1 = (rozofs_suffix_entry_t*)v1;
      rozofs_suffix_entry_t *key2 = (rozofs_suffix_entry_t*)v2;
      rozofs_suffix_htable_t *htable_p = (rozofs_suffix_htable_t*) key1->sufx_htable_p;
      
      if (key1->len != key2->len)
      {
          htable_p->collisions_count++;
	  htable_p->cur_collisions_count++; 
	  return 1;    
      }
      
      ret =  memcmp(key1->suffix_p, key2->suffix_p, key1->len);
      if (ret != 0) {
          htable_p->collisions_count++;
	  htable_p->cur_collisions_count++; 
	  return ret;
      }
      if (htable_p->max_collisions_count < htable_p->cur_collisions_count) htable_p->max_collisions_count = htable_p->cur_collisions_count;
      return ret;
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
int rozofs_suffix_file_parse_req(int file_idx);

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
int export_file_create_check_fast_htable(char *name,int file_idx);
#endif
