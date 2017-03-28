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

#ifndef ROZO_TRASHD_H
#define  ROZO_TRASHD_H
#include <unistd.h>
#include <inttypes.h>

#define TRASH_PATH "/var/run/rozo_trashd/"
#define TRASH_DEFAULT_FREQ_SEC 12
#define ROZO_BALANCE_DBG_BUF_SIZE (1024*384)
#define TRASH_MAX_SCANNED 100
#define TRASH_MIN_FILE_SIZE (10*1024*1024)
#define TRASH_MAX_MOVE_SIZE (1000*1024*1024)
#define TRASH_DEFAULT_THROUGPUT 10
#define TRASH_DEFAULT_OLDER (7*24*3600)

#define ROZOFS_TRASH_STOPPED 0
#define ROZOFS_TRASH_RUNNING 1
#define ROZOFS_TRASH_WAITING 2

typedef struct _rozo_trash_ctx_t
{
  int state;                       /**< current state of the trash process                             */
  char *configFileName;            /**< export configuration file                                      */
  int frequency;                   /**< rebalance polling frequency                                    */
  int export_id;                   /**< exportd for which rebalancing is applied                       */
  int64_t older_time_sec_config;   /**< max time of file to accept a move                              */
  uint16_t debug_port;             /**< TCP port used for rozodiag                                     */
  uint16_t instance;               /**< instance of the rebalancing process                            */
  int continue_on_trash_state;     /**< assert to one if the process should continue while reaching balanced state */
  int verbose;                     /**< assert to one for verbose mode                                 */
  int deletion_rate;               /**< max deletion rate in messages/sec                              */
  int scan_rate;                   /**< max inode rate in messages/sec                                 */
  /*
  ** counter used by the periodic thread 
  */
  int deletion_rate_th;               /**< current count                              */
  int scan_rate_th;                   /**< current count                              */
  /*
  ** statistics 
  */
  int64_t current_scanned_file_cpt;        /**< number of scanned inodes */
  int64_t current_scanned_file_cpt_active; /**< number of active inodes */
  int64_t current_scanned_file_cpt_deleted; /**< number of deleted inodes */
  int64_t current_scanned_file_deleted_total_bytes; /**< total bytes deleted */
  int64_t current_scanned_file_fail_cpt;    /**< number of deletion failures */

  int64_t current_scanned_dir_cpt;        /**< number of scanned inodes */
  int64_t current_scanned_dir_cpt_active; /**< number of active inodes */
  int64_t current_scanned_dir_cpt_deleted; /**< number of deleted inodes */
  int64_t current_scanned_dir_fail_cpt;    /**< number of deletion failures */

  int64_t time_older_cpt;
  int64_t scanned_file_cpt;
  char   * trashConfigFile;
} rozo_trash_ctx_t;

extern int rozo_trash_non_blocking_thread_started;
extern rozo_trash_ctx_t rozo_trash_ctx;


/**
*  Prototypes
*/
int rozo_trash_start_nb_blocking_th(void *args);
char *show_conf_with_buf(char * buf);
/*
**_______________________________________________________________________
*/
#define SUFFIX(var) sprintf(suffix,"%s",var);
static inline char  *display_size(long long unsigned int number,char *buffer)
{
    double tmp = number;
    char suffix[64];
        SUFFIX(" B ");

        if (tmp >= 1024) { tmp = tmp / 1024;  SUFFIX( " KB"); }
        if (tmp >= 1024) { tmp = tmp / 1024;  SUFFIX( " MB"); }
        if (tmp >= 1024) { tmp = tmp / 1024;  SUFFIX( " GB"); }
        if (tmp >= 1024) { tmp = tmp / 1024;  SUFFIX( " TB"); }
        if (tmp >= 1024) { tmp = tmp / 1024;  SUFFIX( " PB"); }
    sprintf(buffer,"%10.2f%s", tmp,suffix);
    return buffer;
}
static inline char  *display_size_not_aligned(long long unsigned int number,char *buffer)
{
    double tmp = number;
    char suffix[64];
        SUFFIX(" B ");

        if (tmp >= 1024) { tmp = tmp / 1024;  SUFFIX( " K"); }
        if (tmp >= 1024) { tmp = tmp / 1024;  SUFFIX( " M"); }
        if (tmp >= 1024) { tmp = tmp / 1024;  SUFFIX( " G"); }
        if (tmp >= 1024) { tmp = tmp / 1024;  SUFFIX( " T"); }
        if (tmp >= 1024) { tmp = tmp / 1024;  SUFFIX( " P"); }
    sprintf(buffer,"%.2f%s", tmp,suffix);
    return buffer;
}

/*
**_______________________________________________________________________
*/
/**
*  Get a size 
  supported formats:
   1/1b/1B (bytes)
   1k/1K (Kilobytes)
   1m/1M (Megabytes)  
   1g/1G (Gigabytes)  
   
   @param str: string that contains the value to convert
   @param value: pointer to the converted value
   
   @retval 0 on success
   @retval -1 on error (see errno for details)
*/
int get_size_value(char *str,uint64_t *value);
#endif
