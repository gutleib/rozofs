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
#include <stdlib.h>
#include <rozofs/rpc/spproto.h>
#include <rozofs/common/log.h>
#include <rozofs/common/list.h>
#include "rozofsstorage_netdata_cfg.h"
#include <pthread.h>

typedef struct _storage_ctx_t {
  list_t         list;
  list_t         storio_list;
  uint32_t       ip;   /*<< IP. */
  int            cid;  /*<< CID */
  int            fd;       /*<< Profiler file fd */
  spp_profiler_t profiler;
  spp_profiler_t old_profiler;
} storage_ctx_t;

uint64_t              storage_period_micro = 0;
list_t                storaged_list;
uint64_t              next_run = 0;
uint64_t              delay_between_run = 0;
uint64_t              last_run = 0;
uint64_t              cfg_mtime;
uint32_t              storage_count = 0;


#define PRINT_CHART(X) {\
  fputs(X,stdout);\
  if (rozofsstorage_netdata_cfg.debug) info("%s",X);\
}  

/*________________________________________________________________
** Create all storage charts
**
*_________________________________________________________________
*/
#define CREATE_ONE_CHART_COUNT(X,storio) {\
  list_t                          * p;\
  storage_ctx_t                   * storagedCtx;\
  list_t                          * q;\
  storage_ctx_t                   * storioCtx;\
  char                              line[8*1024];\
  char                            * pChar = line;\
\
  if ((rozofsstorage_netdata_cfg.display_count) && (rozofsstorage_netdata_cfg.display_count_##X)) {\
    list_for_each_forward(p, &storaged_list) {\
      storagedCtx = list_entry(p, storage_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"CHART storage.");\
      pChar += rozofs_ipv4_append(pChar,storagedCtx->ip);\
      pChar += rozofs_string_append(pChar,"."#X"_count '' 'number of calls to "#X" api' 'calls' ");\
      pChar += rozofs_ipv4_append(pChar,storagedCtx->ip);\
      pChar += rozofs_string_append(pChar," '' stacked 100 1 rozofsstorage_netdata.plugin\n");\
      if (storio) {\
        list_for_each_forward(q, &storagedCtx->storio_list) {\
          storioCtx = list_entry(q, storage_ctx_t, list);\
          pChar += rozofs_string_append(pChar,"DIMENSION storio");\
          pChar += rozofs_u32_append(pChar,storioCtx->cid);\
          pChar += rozofs_string_append(pChar," '' incremental\n");\
        }\
      }\
      else {\
        pChar += rozofs_string_append(pChar,"DIMENSION storaged");\
        pChar += rozofs_string_append(pChar," '' incremental\n");\
      }\
    }\
    PRINT_CHART(line);\
  }\
}
#define CREATE_ONE_CHART_DURATION(X,storio) {\
  list_t                          * p;\
  storage_ctx_t                   * storagedCtx;\
  list_t                          * q;\
  storage_ctx_t                   * storioCtx;\
  char                              line[8*1024];\
  char                            * pChar = line;\
\
  if ((rozofsstorage_netdata_cfg.display_duration) && (rozofsstorage_netdata_cfg.display_duration_##X)) {\
    list_for_each_forward(p, &storaged_list) {\
      storagedCtx = list_entry(p, storage_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"CHART storage.");\
      pChar += rozofs_ipv4_append(pChar,storagedCtx->ip);\
      pChar += rozofs_string_append(pChar,"."#X"_duration '' 'duration of a call to "#X" api' 'microseconds' ");\
      pChar += rozofs_ipv4_append(pChar,storagedCtx->ip);\
      pChar += rozofs_string_append(pChar," '' area 100 1 rozofsstorage_netdata.plugin\n");\
      if (storio) {\
        list_for_each_forward(q, &storagedCtx->storio_list) {\
          storioCtx = list_entry(q, storage_ctx_t, list);\
          pChar += rozofs_string_append(pChar,"DIMENSION storio");\
          pChar += rozofs_u32_append(pChar,storioCtx->cid);\
          pChar += rozofs_string_append(pChar," '' absolute\n");\
        }\
      }\
      else {\
        pChar += rozofs_string_append(pChar,"DIMENSION storaged");\
        pChar += rozofs_string_append(pChar," '' absolute\n");\
      }\
    }\
    PRINT_CHART(line);\
  }\
}
#define CREATE_ONE_CHART_THROUGHPUT(X,storio) {\
  list_t                          * p;\
  storage_ctx_t                   * storagedCtx;\
  list_t                          * q;\
  storage_ctx_t                   * storioCtx;\
  char                              line[8*1024];\
  char                            * pChar = line;\
\
  if ((rozofsstorage_netdata_cfg.display_bytes) && (rozofsstorage_netdata_cfg.display_bytes_##X)) {\
    list_for_each_forward(p, &storaged_list) {\
      storagedCtx = list_entry(p, storage_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"CHART storage.");\
      pChar += rozofs_ipv4_append(pChar,storagedCtx->ip);\
      pChar += rozofs_string_append(pChar,"."#X"_throughput '' '"#X" throughput' 'MB/s' ");\
      pChar += rozofs_ipv4_append(pChar,storagedCtx->ip);\
      pChar += rozofs_string_append(pChar," '' stacked 100 1 rozofsstorage_netdata.plugin\n");\
      if (storio) {\
        list_for_each_forward(q, &storagedCtx->storio_list) {\
          storioCtx = list_entry(q, storage_ctx_t, list);\
          pChar += rozofs_string_append(pChar,"DIMENSION storio");\
          pChar += rozofs_u32_append(pChar,storioCtx->cid);\
          pChar += rozofs_string_append(pChar," '' incremental\n");\
        }\
      }\
      else {\
        pChar += rozofs_string_append(pChar,"DIMENSION storaged");\
        pChar += rozofs_string_append(pChar," '' incremental\n");\
      }\
    }\
    PRINT_CHART(line);\
  }\
}
#define CREATE_2_CHARTS(X,storio) {\
  CREATE_ONE_CHART_COUNT(X,storio)\
  CREATE_ONE_CHART_DURATION(X,storio)\
}   
#define CREATE_3_CHARTS(X,storio) {\
  CREATE_ONE_CHART_COUNT(X,storio)\
  CREATE_ONE_CHART_DURATION(X,storio)\
  CREATE_ONE_CHART_THROUGHPUT(X,storio)\
}  
void create_storage_charts() { 
  if (storage_count==0) return;
  /*
  ** Storio charts
  */
  CREATE_3_CHARTS(read,1)
  CREATE_3_CHARTS(write,1)
  CREATE_2_CHARTS(truncate,1)
  CREATE_2_CHARTS(write_empty,1)
  CREATE_ONE_CHART_COUNT(rebuild_start,1)
  /*
  ** Storaged charts
  */
  CREATE_2_CHARTS(stat,0)
  CREATE_2_CHARTS(remove,0)
  
}
/*________________________________________________________________
** Update all storage charts
**
*_________________________________________________________________
*/
#define UPDATE_ONE_CHART_COUNT(X,storio) {\
  list_t                          * p;\
  storage_ctx_t                   * storagedCtx;\
  list_t                          * q;\
  storage_ctx_t                   * storioCtx;\
  char                              line[8*1024];\
  char                            * pChar = line;\
\
  if ((rozofsstorage_netdata_cfg.display_count) && (rozofsstorage_netdata_cfg.display_count_##X)) {\
    list_for_each_forward(p, &storaged_list) {\
      storagedCtx = list_entry(p, storage_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"BEGIN storage.");\
      pChar += rozofs_ipv4_append(pChar,storagedCtx->ip);\
      pChar += rozofs_string_append(pChar,"."#X"_count ");\
      pChar += rozofs_u64_append(pChar, storage_period_micro);\
      if (storio) {\
        list_for_each_forward(q, &storagedCtx->storio_list) {\
          storioCtx = list_entry(q, storage_ctx_t, list);\
          pChar += rozofs_string_append(pChar,"\nSET storio");\
          pChar += rozofs_u32_append(pChar,storioCtx->cid);\
          pChar += rozofs_string_append(pChar," = ");\
          pChar += rozofs_u64_append(pChar, storioCtx->profiler.X[P_COUNT]);\
        }\
      }\
      else {\
        pChar += rozofs_string_append(pChar,"\nSET storaged");\
        pChar += rozofs_string_append(pChar," = ");\
        pChar += rozofs_u64_append(pChar, storagedCtx->profiler.X[P_COUNT]);\
      }\
      pChar += rozofs_string_append(pChar,"\nEND\n");\
    }\
    PRINT_CHART(line);\
  }\
}
#define UPDATE_ONE_CHART_DURATION(X,storio) {\
  list_t                          * p;\
  storage_ctx_t                   * storagedCtx;\
  list_t                          * q;\
  storage_ctx_t                   * storioCtx;\
  char                              line[8*1024];\
  char                            * pChar = line;\
\
  if ((rozofsstorage_netdata_cfg.display_duration) && (rozofsstorage_netdata_cfg.display_duration_##X)) {\
    list_for_each_forward(p, &storaged_list) {\
      storagedCtx = list_entry(p, storage_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"BEGIN storage.");\
      pChar += rozofs_ipv4_append(pChar,storagedCtx->ip);\
      pChar += rozofs_string_append(pChar,"."#X"_duration ");\
      pChar += rozofs_u64_append(pChar, storage_period_micro);\
      if (storio) {\
        list_for_each_forward(q, &storagedCtx->storio_list) {\
          storioCtx = list_entry(q, storage_ctx_t, list);\
          pChar += rozofs_string_append(pChar,"\nSET storio");\
          pChar += rozofs_u32_append(pChar,storioCtx->cid);\
          pChar += rozofs_string_append(pChar," = ");\
          if (storioCtx->profiler.X[P_COUNT] == storioCtx->old_profiler.X[P_COUNT]) {\
            pChar += rozofs_string_append(pChar,"0");\
          }\
          else {\
            pChar += rozofs_u64_append(pChar,(storioCtx->profiler.X[P_ELAPSE] - storioCtx->old_profiler.X[P_ELAPSE])/(storioCtx->profiler.X[P_COUNT] - storioCtx->old_profiler.X[P_COUNT]));\
          }\
        }\
      }\
      else {\
        pChar += rozofs_string_append(pChar,"\nSET storaged");\
        pChar += rozofs_string_append(pChar," = ");\
        if (storagedCtx->profiler.X[P_COUNT] == storagedCtx->old_profiler.X[P_COUNT]) {\
          pChar += rozofs_string_append(pChar,"0");\
        }\
        else {\
          pChar += rozofs_u64_append(pChar,(storagedCtx->profiler.X[P_ELAPSE] - storagedCtx->old_profiler.X[P_ELAPSE])/(storagedCtx->profiler.X[P_COUNT] - storagedCtx->old_profiler.X[P_COUNT]));\
        }\
      }\
      pChar += rozofs_string_append(pChar,"\nEND\n");\
    }\
    PRINT_CHART(line);\
  }\
}
#define UPDATE_ONE_CHART_TROUGHPUT(X) {\
  list_t                          * p;\
  storage_ctx_t                   * storagedCtx;\
  list_t                          * q;\
  storage_ctx_t                   * storioCtx;\
  char                              line[8*1024];\
  char                            * pChar = line;\
\
  if ((rozofsstorage_netdata_cfg.display_bytes) && (rozofsstorage_netdata_cfg.display_bytes_##X)) {\
    list_for_each_forward(p, &storaged_list) {\
      storagedCtx = list_entry(p, storage_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"BEGIN storage.");\
      pChar += rozofs_ipv4_append(pChar,storagedCtx->ip);\
      pChar += rozofs_string_append(pChar,"."#X"_throughput ");\
      pChar += rozofs_u64_append(pChar, storage_period_micro);\
      list_for_each_forward(q, &storagedCtx->storio_list) {\
        storioCtx = list_entry(q, storage_ctx_t, list);\
        pChar += rozofs_string_append(pChar,"\nSET storio");\
        pChar += rozofs_u32_append(pChar,storioCtx->cid);\
        pChar += rozofs_string_append(pChar," = ");\
        pChar += rozofs_u64_append(pChar,storioCtx->profiler.X[P_BYTES]/1000000);\
      }\
      pChar += rozofs_string_append(pChar,"\nEND\n");\
    }\
    PRINT_CHART(line);\
  }\
}
#define UPDATE_2_CHARTS(X,storio) {\
  UPDATE_ONE_CHART_COUNT(X,storio)\
  UPDATE_ONE_CHART_DURATION(X,storio)\
}
#define UPDATE_3_CHARTS(X) {\
  UPDATE_ONE_CHART_COUNT(X,1)\
  UPDATE_ONE_CHART_DURATION(X,1)\
  UPDATE_ONE_CHART_TROUGHPUT(X)\
}
void update_storage_charts() { 
  if (storage_count==0) return;
  /*
  ** Storio charts
  */
  UPDATE_3_CHARTS(read)
  UPDATE_3_CHARTS(write)
  UPDATE_2_CHARTS(truncate,1)
  UPDATE_2_CHARTS(write_empty,1)
  UPDATE_ONE_CHART_COUNT(rebuild_start,1)
  /*
  ** Storaged charts
  */
  UPDATE_2_CHARTS(stat,0)
  UPDATE_2_CHARTS(remove,0)
  fflush(stdout);
}
/*________________________________________________________________
** Allocate a storage context
**
** @param storaged         storaged context
** @param IP               storage IP address
** @param cid              cid (-1 for storaged)
** @param fd               file descriptor of the profile file
**
** @retval storage context address
*_________________________________________________________________
*/
storage_ctx_t * allocate_storage_context(storage_ctx_t * storaged, uint32_t ip, int cid, int fd) {
  storage_ctx_t * ctx;
  
  ctx = malloc(sizeof(storage_ctx_t));
  memset(ctx,0,sizeof(storage_ctx_t));
  
  list_init(&ctx->list);
  list_init(&ctx->storio_list);
  if (cid == -1) {
    /*
    ** This is a storaged. Put it in the storaged list
    */
    list_push_back(&storaged_list,&ctx->list);
  }  
  else {
    /*
    ** This is a storio. Put it in the list of storio of its storaged
    */
    list_push_back(&storaged->storio_list,&ctx->list);
  } 
  ctx->ip  = ip;
  ctx->cid = cid;
  ctx->fd  = fd;
  return ctx;
}
/*________________________________________________________________
** Find out a storage context from its IP and CID
**
** @param IP               storage IP address
** @param cid              cid
**
** @retval storage context address or null
*_________________________________________________________________
*/
storage_ctx_t * find_storage_context(uint32_t ip, int cid) {
  list_t                      * p;
  storage_ctx_t               * storaged;
  list_t                      * q;
  storage_ctx_t               * storio;

  list_for_each_forward(p, &storaged_list) {
    storaged = list_entry(p, storage_ctx_t, list);
    if (storaged->ip != ip) continue;
    if( cid == -1) return storaged;
    /*
    ** Loop on the storio
    */
    list_for_each_forward(q, &storaged->storio_list) {
      storio = list_entry(q, storage_ctx_t, list);
      if (storio->cid == cid) return storio;
    }    
    return NULL;
  }
  return NULL;  
}
/*________________________________________________________________
** Read profiler file 
**
** @param       ctx         Rozofsmout context
*_________________________________________________________________
*/
void read_profiler_file(storage_ctx_t * ctx) {
  /*
  ** Save old values
  */
  memcpy(&ctx->old_profiler,&ctx->profiler, sizeof(ctx->profiler));
  /*
  ** Read new values
  */
  pread(ctx->fd, &ctx->profiler, sizeof(spp_profiler_t), 0); 
}
/*________________________________________________________________
** re-read profiler files 
**
** @param       ctx         Rozofsmout context
*_________________________________________________________________
*/
void read_all_profiler_files() {
  list_t                          * p;
  storage_ctx_t               * ctx;

  list_for_each_forward(p, &storaged_list) {
    ctx = list_entry(p, storage_ctx_t, list);
    read_profiler_file(ctx);
  }
}
/*________________________________________________________________
** Get time in micro seconds
**
*_________________________________________________________________
*/
static inline uint64_t get_time_microseconds() {
  struct timeval     timeDay;  
  
  gettimeofday(&timeDay,(struct timezone *)0); 
  return ((uint64_t)timeDay.tv_sec * 1000000 + timeDay.tv_usec);
}  
/*________________________________________________________________
** Wait before running a new loop
**
*_________________________________________________________________
*/
void wait_time_to_run() {
  uint64_t   now_micro;

  if (next_run==0) next_run = get_time_microseconds();
  next_run += storage_period_micro;  

  now_micro = get_time_microseconds();
  while ( now_micro < next_run ) {
    usleep(next_run - now_micro);
    now_micro = get_time_microseconds();
  }  
  if (last_run == 0) {
    delay_between_run = storage_period_micro;
  }
  else {
    delay_between_run =  now_micro - last_run;
  }   
  last_run = now_micro; 
}
/*________________________________________________________________
** Build the list of storage instances
**
** @retval number of new storage instances found
*_________________________________________________________________
*/
int build_storage_context_list() {
  char                 kpi_base_path[256];
  char                 kpi_storage_path[1024];
  char                 kpi_profiler_path[1024];
  char              *  pChar = kpi_base_path;
  DIR               *  d1;
  DIR               *  d2;
  struct dirent     *  dir1;
  struct dirent     *  dir2;
  int                  cid;
  storage_ctx_t     *  storio;  
  storage_ctx_t     *  storaged;  
  int                  fd;
  int                  new = 0;  
  uint32_t             ip;

  storage_count = 0;
 
  pChar += rozofs_string_append(pChar, ROZOFS_KPI_ROOT_PATH);
  pChar += rozofs_string_append(pChar, "/storage/");
  
  d1 = opendir(kpi_base_path);
  if (d1==NULL) {
    severe("open(%s) %s",kpi_base_path,strerror(errno));
    return -1;
  }
  
  
  while ((dir1 = readdir(d1)) != NULL) { 
  
    if (dir1->d_name[0] == '.') continue;
    
    {
      int ip1,ip2,ip3,ip4;
      if (sscanf(dir1->d_name,"%u.%u.%u.%u", &ip1, &ip2, &ip3, &ip4) != 4) {
        continue;
      }
      ip = ((ip1&0xFF)<<24) | ((ip2&0xFF)<<16)  | ((ip3&0xFF)<<8) | (ip4&0xFF);
    }

    sprintf(kpi_storage_path, "%s%s", kpi_base_path, dir1->d_name);

    /*
    ** Open storaged profiler file
    */
    storaged = find_storage_context(ip,-1);
    if (storaged) {
      storage_count++;
    }  
    else {  
      sprintf(kpi_profiler_path, "%s/storaged/profiler", kpi_storage_path);
      if ((fd = open(kpi_profiler_path, O_RDONLY, S_IRWXU)) < 0) {
        severe("open(%s) %s", kpi_profiler_path, strerror(errno));
        continue;
      }
      storaged = allocate_storage_context(NULL,ip,-1,fd);
      read_profiler_file(storaged);
      new++;
      storage_count++;   
    }

    /*
    ** Loop on storio
    */

    d2 = opendir(kpi_storage_path);
    if (d2==NULL) {
      severe("open(%s) %s",kpi_storage_path,strerror(errno));
      continue;
    }
    while ((dir2 = readdir(d2)) != NULL) { 
  
      cid = -1;
      if (sscanf(dir2->d_name, "storio_%d", &cid) != 1) {
        continue;
      }

      storio = find_storage_context(ip,cid);
      if (storio) {
        /*
        ** already in list 
        */
        storage_count++;
        continue;
      }

      /*
      ** New context
      */  

      /*
      ** Open profiler file
      */
      sprintf(kpi_profiler_path, "%s/%s/profiler", kpi_storage_path, dir2->d_name);
      if ((fd = open(kpi_profiler_path, O_RDONLY, S_IRWXU)) < 0) {
        severe("open(%s) %s", kpi_profiler_path, strerror(errno));
        continue;
      }

      /*
      ** Allocate a context
      */
      storio = allocate_storage_context(storaged,ip,cid,fd);
      read_profiler_file(storio);
      new++;
      storage_count++;
    }  
    closedir(d2);
  }  
  closedir(d1);
  return new;
}
/*________________________________________________________________
** Check for any change 
**
*_________________________________________________________________
*/
void check_for_changes() {
  uint64_t   new_cfg_mtime;
  int        new;
  
  /*
  ** Recheck the list of storage
  */
  new = build_storage_context_list();
  
  /*
  ** Reread configuration file modification time
  */
  new_cfg_mtime = rozofsstorage_netdata_cfg_get_mtime(NULL); 
  if (new_cfg_mtime != cfg_mtime) {
    /*
    ** Re-read the configuration file
    */
    rozofsstorage_netdata_cfg_read(NULL);
  } 
   
  /*
  ** If some new storage appear
  ** or new config parameters, re-print the charts
  */
  if ((new) || (new_cfg_mtime != cfg_mtime)) {
    create_storage_charts();
    cfg_mtime = new_cfg_mtime;
  }  
}

struct sched_param my_priority;
int my_policy=-1;
/*________________________________________________________________
** Main
**
** Only one parameter : the polling frequency
**
*_________________________________________________________________
*/
int main(int argc, char *argv[]) {
  uint64_t      loop = 0;
  
  /*
  ** Check input parameter
  */
  if (argc < 2) {
    printf("Missing polling frequency.\n");
    PRINT_CHART("DISABLE");
    exit(1);
  }
  if (sscanf(argv[1], "%llu", (long long unsigned int *)&storage_period_micro) != 1) {
    printf("Missing polling frequency.\n");
    PRINT_CHART("DISABLE");
    exit(1);
  }
  storage_period_micro *= 1000000;
  
  /*
  ** Initialize the list of context
  */
  list_init(&storaged_list);
  
  /*
  ** Read configuration file
  */
  {
    char cfg_file[128];
    sprintf(cfg_file,"%s/rozofsstorage_netdata_cfg.conf",ROZOFS_CONFIG_DIR);
    if (access(cfg_file,R_OK) !=0 ) {
      /**
      * Create empty configuration file
      */
      int fd = open(cfg_file, O_RDWR|O_CREAT, 0755); 
      close(fd);
    }
    /*
    ** Record configuration file modification time to check later
    ** whether it has been updated
    */
    cfg_mtime = rozofsstorage_netdata_cfg_get_mtime(NULL); 
    /*
    ** Read the configuration file
    */
    rozofsstorage_netdata_cfg_read(NULL);
  }
  
  /*
  ** Build the list of storage instances
  */
  build_storage_context_list();
  
  /*
  ** Create charts
  */
  create_storage_charts();

  {
    int ret= 0;

    pthread_getschedparam(pthread_self(),&my_policy,&my_priority);
    my_priority.sched_priority= 98;
    my_policy = SCHED_RR;
    ret = pthread_setschedparam(pthread_self(),my_policy,&my_priority);
    if (ret < 0) 
    {
      severe("error on sched_setscheduler: %s",strerror(errno));	
    }
  }  
    
  while (1) {
  
    loop++;
    
    /*
    ** Check every minute whether something has changed
    */
    if ((loop % 60)==0)  {
      check_for_changes();
    }
    
    /*
    ** Wait until next run
    */
    wait_time_to_run();
    
    /*
    ** Re-read profiler files
    */
    read_all_profiler_files();
    
    /*
    ** Update every chart
    */
    update_storage_charts();
  } 
}  
