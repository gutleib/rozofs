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

#ifndef ROZO_BALANCE_H
#define  ROZO_BALANCE_H
#include <unistd.h>
#include <inttypes.h>

#define REBALANCE_PATH "/var/run/rozo_rebalance/"
#define REBALANCE_DEFAULT_FREQ_SEC 30
#define ROZO_BALANCE_DBG_BUF_SIZE (1024*384)
#define REBALANCE_MAX_SCANNED 10
#define REBALANCE_MIN_FILE_SIZE (10*1024*1024)
#define REBALANCE_MAX_MOVE_SIZE (1000*1024*1024)
#define REBALANCE_DEFAULT_THROUGPUT 10

typedef enum _rebalance_file_mode_e
{
  REBALANCE_MODE_REL= 0, /**< use relative path: @rozofs_uuid@<parent_fid>/filename  */
  REBALANCE_MODE_ABS, /**< use full pathname  */
  REBALANCE_MODE_FID, /**< usefid mode: @rozofs_uuid@<file_fid>  */
  REBALANCE_MODE_MAX
} rebalance_file_mode_e;


typedef struct _rozo_balancing_ctx_t
{
  char *configFileName;         /**< export configuration file                */
  int rebalance_frequency;   /**< rebalance polling frequency              */
  int volume_id;            /**< volume for which rebalancing is applied   */
  int rebalance_threshold_config;  /**< rebalance threshold in percentage         */
  int rebalance_threshold;  /**< current rebalance threshold in percentage         */
  int rebalance_threshold_trigger; /**< percentage of free space before triggering a disk rebalancing  */
  int64_t newer_time_sec_config;                    /**< min time of file to accept a move                              */
  int64_t older_time_sec_config;                    /**< max time of file to accept a move                              */
  int max_cid_score;
  int min_cid_score;
  uint16_t debug_port;    /**< TCP port used for rozodiag                 */
  uint16_t instance;     /**< instance of the rebalancing process         */
  int number_of_eid_in_volume;   /**< number of eids in the volume        */
  int current_eid_idx;           /**< current export index                */
  int max_scanned;           /**< max file scanned before a move          */
  uint64_t filesize_config;           /**< min file size in Bytes         */
  uint64_t max_move_size_config;     /**< max move size                   */
  int continue_on_balanced_state;    /**< assert to one if the process should continue while reaching balanced state */
  int verbose;                       /**< assert to one for verbose mode */
  int throughput;                    /**< file throughput in MBytes/s */
  int file_mode;                     /**< either relative/absolute or fid node (see rebalance_file_mode_e) */
  /*
  ** statistics 
  */
  int eagain_volume;
  int eagain_cluster;
  int64_t time_older_cpt;
  int64_t time_newer_cpt;
  int64_t time_inrange_cpt;
  int64_t scanned_file_cpt;
  int64_t current_scanned_file_cpt;
  uint64_t cur_move_size;            /**< current size to move           */
  char   * rebalanceConfigFile;
} rozo_balancing_ctx_t;

extern int rozo_balance_non_blocking_thread_started;
extern rozo_balancing_ctx_t rozo_balancing_ctx;


/**
*  Prototypes
*/
int rozo_rebalance_start_nb_blocking_th(void *args);
char *rozo_display_all_cluster_with_buf(char *pChar);
char  *display_all_cluster_balancing_stats(char *pChar);
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
