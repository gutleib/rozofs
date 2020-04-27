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
#ifndef ROZOFS_MOVER_H
#define ROZOFS_MOVER_H
#include <rozofs/common/list.h>

#define ROZOFS_MAX_MOVER_THREADS 60

typedef enum
{
   MOVER_SUCCESS_E = 0,
   MOVER_UPDATED_E,
   MOVER_ERROR_E
} mover_status_e;
/*
** Description of a file to move
*/
typedef struct _rozofs_mover_job_t {
  cid_t       cid;                      /* Destination cluster */
  sid_t       sid[ROZOFS_SAFE_MAX];     /* Destination storages */
  char      * name;                     /* File name */
  uint64_t    size;                     /* Size of the file to move */
  list_t      list;                     /* For job chaining */
  mover_status_e    status;             /* 0 if OK and -1 if error */
  int         error;                    /* error code */
  time_t      validation_time;          /* Time to validate a move */
} rozofs_mover_job_t;
/*-----------------------------------------------------------------------------
**
** Statistics formating
**
**----------------------------------------------------------------------------
*/
void rozofs_mover_print_stat(char * pChar);

/*-----------------------------------------------------------------------------
**
** Service initialize
**
**----------------------------------------------------------------------------
*/
int rozofs_mover_init();

/*-----------------------------------------------------------------------------
**
** Service initialize in multithreaded mode
@param path: path to the profiler array in /var/run/rozofs_kpi/....
**
**----------------------------------------------------------------------------
*/
int rozofs_mover_init_th(char *path,int throughput,int nb_threads);

/*-----------------------------------------------------------------------------
**
** Move a list a file to a new location for rebalancing purpose
**
** @param exportd_hosts     exportd host name or addresses (from configuration file)
** @param export_path       the export path to mount
** @param throughput        throughput litation in MB. 0 is no limitation.
** @param jobs              list of files along with their destination
**
**----------------------------------------------------------------------------
*/
int rozofs_do_move_one_export(char * exportd_hosts, char * export_path, int throughput, list_t * jobs);
/*-----------------------------------------------------------------------------
**
** Request for a throughput value change
**
** @param throughput        Requested throughput limitation or zero when no limit
**
**----------------------------------------------------------------------------
*/
void rozofs_mover_throughput_update_request(uint64_t throughput) ;

/*-----------------------------------------------------------------------------
**
** Move a list a file to a new location for rebalancing purpose
**
** @param exportd_hosts     exportd host name or addresses (from configuration file)
** @param export_path       the export path to mount
** @param throughput        throughput litation in MB. 0 is no limitation.
** @param jobs              list of files along with their destination
**
**----------------------------------------------------------------------------
*/
int rozofs_do_move_one_export_fid_mode(char * exportd_hosts, char * export_path, int throughput, list_t * jobs) ;

/*-----------------------------------------------------------------------------
**
** Move a list a file to a new location for rebalancing purpose
**
** @param exportd_hosts     exportd host name or addresses (from configuration file)
** @param export_path       the export path to mount
** @param throughput        throughput litation in MB. 0 is no limitation.
** @param jobs              list of files along with their destination
**
**----------------------------------------------------------------------------
*/


int rozofs_do_move_one_export_fid_mode_multithreaded_mounted(char * exportd_hosts, char * export_path, int throughput, list_t * jobs,char *mount_path_in);

#endif
