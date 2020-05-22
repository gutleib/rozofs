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
#include <rozofs/rpc/mpproto.h>
#include <rozofs/common/log.h>
#include <rozofs/common/list.h>
#include "rozofsmount_netdata_cfg.h"


typedef struct _rozofsmount_ctx_t {
  list_t         list;
  int            instance; /*<< Rozofsmount instance. -1 for sall sum pseudo instance */
  int            fd;       /*<< Profiler file fd */
  mpp_profiler_t profiler;
  mpp_profiler_t old_profiler;
} rozofsmount_ctx_t;


rozofsmount_ctx_t   * ctx_all = NULL;
uint64_t              rozofsmout_period_micro = 0;
list_t                rozofsmount_list;
uint64_t              next_run = 0;


#define NEWLINE(fmt, ...) {\
  char string[128];\
  sprintf(string,fmt, ##__VA_ARGS__); \
  if (rozofsmount_netdata_cfg.debug) info("%s",string);\
  printf("%s\n",string);\
}  
/*________________________________________________________________
** Build an instacne string
**
** @param ctx         rozofsmount context
** @param stringIndex Where to format the string
*_________________________________________________________________
*/
static inline void build_string_from_index(rozofsmount_ctx_t   * ctx, char * stringIndex) {
  if (ctx->instance == -1) {
    strcpy(stringIndex, "all");
  }
  else {
    sprintf(stringIndex, "mount%d", ctx->instance);
  }
}
/*________________________________________________________________
** Create all rozofsmount charts
**
*_________________________________________________________________
*/
#define CREATE_ONE_CHART_COUNT(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  char                              stringIndex[16];\
\
  if ((rozofsmount_netdata_cfg.display_count) && (rozofsmount_netdata_cfg.display_count_##X)) {\
    NEWLINE("CHART rozofsmount."#X"_count '' 'number of calls to "#X" api' 'calls' "#X" '' line 10 1");\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      build_string_from_index(ctx, stringIndex);\
      NEWLINE("DIMENSION %s '' incremental",stringIndex);\
    }\
  }\
}
#define CREATE_ONE_CHART_DURATION(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  char                              stringIndex[16];\
\
  if ((rozofsmount_netdata_cfg.display_duration) && (rozofsmount_netdata_cfg.display_duration_##X)) {\
    NEWLINE("CHART rozofsmount."#X"_duration '' 'duration of a call to "#X" api' 'ms' "#X" '' line 10 1");\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      build_string_from_index(ctx, stringIndex);\
      NEWLINE("DIMENSION %s '' absolute",stringIndex);\
    }\
  }\
}
#define CREATE_ONE_CHART_THROUGHPUT(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  char                              stringIndex[16];\
\
  if ((rozofsmount_netdata_cfg.display_bytes) && (rozofsmount_netdata_cfg.display_bytes_##X)) {\
    NEWLINE("CHART rozofsmount."#X"_throughput '' '"#X" throughput' 'MB/s' "#X" '' line 10 1");\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      build_string_from_index(ctx, stringIndex);\
      NEWLINE("DIMENSION %s '' incremental",stringIndex);\
    }\
  }\
}
#define CREATE_2_CHARTS(X) {\
  CREATE_ONE_CHART_COUNT(X)\
  CREATE_ONE_CHART_DURATION(X)\
}   
#define CREATE_3_CHARTS(X) {\
  CREATE_ONE_CHART_COUNT(X)\
  CREATE_ONE_CHART_DURATION(X)\
  CREATE_ONE_CHART_THROUGHPUT(X)\
}  
void create_rozofsmount_charts() { \
  CREATE_2_CHARTS(lookup);
  CREATE_2_CHARTS(lookup_agg);
  CREATE_2_CHARTS(forget);
  CREATE_2_CHARTS(getattr);
  CREATE_2_CHARTS(setattr);
  CREATE_2_CHARTS(readlink);
  CREATE_2_CHARTS(mknod);
  CREATE_2_CHARTS(mkdir);
  CREATE_2_CHARTS(unlink);
  CREATE_2_CHARTS(rmdir);
  CREATE_2_CHARTS(symlink);
  CREATE_2_CHARTS(rename);
  CREATE_2_CHARTS(open);
  CREATE_2_CHARTS(link);
  CREATE_3_CHARTS(read);
  CREATE_3_CHARTS(write);
  CREATE_2_CHARTS(flush);
  CREATE_2_CHARTS(release);
  CREATE_2_CHARTS(opendir);
  CREATE_2_CHARTS(readdir);
  CREATE_2_CHARTS(releasedir);
  CREATE_2_CHARTS(fsyncdir);
  CREATE_2_CHARTS(statfs);
  CREATE_2_CHARTS(setxattr);
  CREATE_2_CHARTS(getxattr);
  CREATE_2_CHARTS(listxattr);
  CREATE_2_CHARTS(removexattr);
  CREATE_2_CHARTS(access);
  CREATE_2_CHARTS(create);
  CREATE_2_CHARTS(getlk);
  CREATE_2_CHARTS(setlk);
  CREATE_2_CHARTS(setlk_int);
  CREATE_2_CHARTS(ioctl);
  CREATE_2_CHARTS(clearlkowner); 
}
/*________________________________________________________________
** Update all rozofsmount charts
**
*_________________________________________________________________
*/
#define UPDATE_ONE_CHART_COUNT(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  char                              stringIndex[16];\
\
  if ((rozofsmount_netdata_cfg.display_count) && (rozofsmount_netdata_cfg.display_count_##X)) {\
    NEWLINE("BEGIN rozofsmount."#X"_count %llu",(long long unsigned int)rozofsmout_period_micro);\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      build_string_from_index(ctx, stringIndex);\
      NEWLINE("SET %s = %llu",stringIndex,(long long unsigned int)ctx->profiler.rozofs_ll_##X[0]);\
    }\
    NEWLINE("END");\
  }\
}
#define UPDATE_ONE_CHART_DURATION(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  char                              stringIndex[16];\
\
  if ((rozofsmount_netdata_cfg.display_count) && (rozofsmount_netdata_cfg.display_count_##X)) {\
    NEWLINE("BEGIN rozofsmount."#X"_duration %llu",(long long unsigned int)rozofsmout_period_micro);\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      build_string_from_index(ctx, stringIndex);\
      if (ctx->profiler.rozofs_ll_##X[0] == ctx->old_profiler.rozofs_ll_##X[0]) {\
        NEWLINE("SET %s = 0",stringIndex);\
      }\
      else {\
        NEWLINE("SET %s = %llu",stringIndex, (long long unsigned int)(ctx->profiler.rozofs_ll_##X[1] - ctx->old_profiler.rozofs_ll_##X[1])/(ctx->profiler.rozofs_ll_##X[0] - ctx->old_profiler.rozofs_ll_##X[0]));\
      }\
    }\
    NEWLINE("END");\
  }\
}
#define UPDATE_ONE_CHART_TROUGHPUT(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  char                              stringIndex[16];\
\
  if ((rozofsmount_netdata_cfg.display_count) && (rozofsmount_netdata_cfg.display_count_##X)) {\
    NEWLINE("BEGIN rozofsmount."#X"_throughput %llu",(long long unsigned int)rozofsmout_period_micro);\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      build_string_from_index(ctx, stringIndex);\
      NEWLINE("SET %s = %llu",stringIndex, (long long unsigned int)ctx->profiler.rozofs_ll_##X[2]);\
    }\
    NEWLINE("END");\
  }\
}
#define UPDATE_2_CHARTS(X) {\
  UPDATE_ONE_CHART_COUNT(X)\
  UPDATE_ONE_CHART_DURATION(X)\
}
#define UPDATE_3_CHARTS(X) {\
  UPDATE_ONE_CHART_COUNT(X)\
  UPDATE_ONE_CHART_DURATION(X)\
  UPDATE_ONE_CHART_TROUGHPUT(X)\
}
void update_rozofsmount_charts() { 
  UPDATE_2_CHARTS(lookup);
  UPDATE_2_CHARTS(lookup_agg);
  UPDATE_2_CHARTS(forget);
  UPDATE_2_CHARTS(getattr);
  UPDATE_2_CHARTS(setattr);
  UPDATE_2_CHARTS(readlink);
  UPDATE_2_CHARTS(mknod);
  UPDATE_2_CHARTS(mkdir);
  UPDATE_2_CHARTS(unlink);
  UPDATE_2_CHARTS(rmdir);
  UPDATE_2_CHARTS(symlink);
  UPDATE_2_CHARTS(rename);
  UPDATE_2_CHARTS(open);
  UPDATE_2_CHARTS(link);
  UPDATE_3_CHARTS(read);
  UPDATE_3_CHARTS(write);
  UPDATE_2_CHARTS(flush);
  UPDATE_2_CHARTS(release);
  UPDATE_2_CHARTS(opendir);
  UPDATE_2_CHARTS(readdir);
  UPDATE_2_CHARTS(releasedir);
  UPDATE_2_CHARTS(fsyncdir);
  UPDATE_2_CHARTS(statfs);
  UPDATE_2_CHARTS(setxattr);
  UPDATE_2_CHARTS(getxattr);
  UPDATE_2_CHARTS(listxattr);
  UPDATE_2_CHARTS(removexattr);
  UPDATE_2_CHARTS(access);
  UPDATE_2_CHARTS(create);
  UPDATE_2_CHARTS(getlk);
  UPDATE_2_CHARTS(setlk);
  UPDATE_2_CHARTS(setlk_int);
  UPDATE_2_CHARTS(ioctl);
  UPDATE_2_CHARTS(clearlkowner); 
}
/*________________________________________________________________
** Allocate a rozofsmount context
**
** @param instance         Rozofsmout instance
** @param fd               filed descriptor of the profile file
**
** @retval rozofsmount context address
*_________________________________________________________________
*/
rozofsmount_ctx_t * allocate_rozofsmount_context(int instance, int fd) {
  rozofsmount_ctx_t * ctx;
  
  ctx = malloc(sizeof(rozofsmount_ctx_t));
  memset(ctx,0,sizeof(rozofsmount_ctx_t));
  
  list_init(&ctx->list);
  list_push_back(&rozofsmount_list,&ctx->list);
  
  ctx->instance = instance;
  ctx->fd       = fd;
  return ctx;
}
/*________________________________________________________________
** Summ all profiler into the all context
**
*_________________________________________________________________
*/
#define SUM_UP_2(X) {\
  ctx_all->profiler.rozofs_ll_##X[0] += ctx->profiler.rozofs_ll_##X[0];\
  ctx_all->profiler.rozofs_ll_##X[1] += ctx->profiler.rozofs_ll_##X[1];\
}
#define SUM_UP_3(X) {\
  ctx_all->profiler.rozofs_ll_##X[0] += ctx->profiler.rozofs_ll_##X[0];\
  ctx_all->profiler.rozofs_ll_##X[1] += ctx->profiler.rozofs_ll_##X[1];\
  ctx_all->profiler.rozofs_ll_##X[2] += ctx->profiler.rozofs_ll_##X[2];\
}
void sum_up_all_profiler() {
  list_t                          * p;
  rozofsmount_ctx_t               * ctx;

  /*
  ** Save old values
  */
  memcpy(&ctx_all->old_profiler,&ctx_all->profiler, sizeof(ctx->profiler));

  /*
  ** Reset new values
  */
  memset(&ctx_all->profiler, 0, sizeof(ctx_all->profiler)); 

  list_for_each_forward(p, &rozofsmount_list) {
    ctx = list_entry(p, rozofsmount_ctx_t, list);
    if (ctx->instance != -1) {
      SUM_UP_2(lookup);
      SUM_UP_2(lookup_agg);
      SUM_UP_2(forget);
      SUM_UP_2(getattr);
      SUM_UP_2(setattr);
      SUM_UP_2(readlink);
      SUM_UP_2(mknod);
      SUM_UP_2(mkdir);
      SUM_UP_2(unlink);
      SUM_UP_2(rmdir);
      SUM_UP_2(symlink);
      SUM_UP_2(rename);
      SUM_UP_2(open);
      SUM_UP_2(link);
      SUM_UP_3(read);
      SUM_UP_3(write);
      SUM_UP_2(flush);
      SUM_UP_2(release);
      SUM_UP_2(opendir);
      SUM_UP_2(readdir);
      SUM_UP_2(releasedir);
      SUM_UP_2(fsyncdir);
      SUM_UP_2(statfs);
      SUM_UP_2(setxattr);
      SUM_UP_2(getxattr);
      SUM_UP_2(listxattr);
      SUM_UP_2(removexattr);
      SUM_UP_2(access);
      SUM_UP_2(create);
      SUM_UP_2(getlk);
      SUM_UP_2(setlk);
      SUM_UP_2(setlk_int);
      SUM_UP_2(ioctl);
      SUM_UP_2(clearlkowner); 
    }  
  }
}
/*________________________________________________________________
** Read profiler file 
**
** @param       ctx         Rozofsmout context
*_________________________________________________________________
*/
void read_profiler_file(rozofsmount_ctx_t * ctx) {
  /*
  ** Save old values
  */
  memcpy(&ctx->old_profiler,&ctx->profiler, sizeof(ctx->profiler));
  /*
  ** Read new values
  */
  pread(ctx->fd, &ctx->profiler, sizeof(mpp_profiler_t), 0); 
}
/*________________________________________________________________
** re-read profiler files 
**
** @param       ctx         Rozofsmout context
*_________________________________________________________________
*/
void read_all_profiler_files() {
  list_t                          * p;
  rozofsmount_ctx_t               * ctx;

  list_for_each_forward(p, &rozofsmount_list) {
    ctx = list_entry(p, rozofsmount_ctx_t, list);
    if (ctx->instance != -1) {
      read_profiler_file(ctx);
    }  
  }
  sum_up_all_profiler();
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
  next_run += rozofsmout_period_micro;  

  now_micro = get_time_microseconds();
  while ( now_micro < next_run ) {
    usleep(next_run - now_micro);
    now_micro = get_time_microseconds();
  }     
}
/*________________________________________________________________
** Build the list of rozofsmount instances
**
*_________________________________________________________________
*/
int build_rofsmount_context_list() {
  char                 mount_kpi_base_path[256];
  char                 mount_kpi_path[1024];
  char              *  pChar = mount_kpi_base_path;
  DIR               *  d;
  struct dirent     *  dir;
  int                  instance;
  rozofsmount_ctx_t *  ctx;  
  int                  fd;
  int                  count = 0;
  
  /*
  ** Initialize the list of context
  */
  list_init(&rozofsmount_list);

  /*
  ** Create the all context
  */
  ctx_all = allocate_rozofsmount_context(-1,-1);
  

  pChar += rozofs_string_append(pChar, ROZOFS_KPI_ROOT_PATH);
  pChar += rozofs_string_append(pChar, "/mount/");
  
  d = opendir(mount_kpi_base_path);
  if (d==NULL) {
    severe("open(%s) %s",mount_kpi_base_path,strerror(errno));
    return -1;
  }
  
  
  while ((dir = readdir(d)) != NULL) { 
  
    instance = -1;
    if (sscanf(dir->d_name, "inst_%d", &instance) != 1) {
      continue;
    }
    
    /*
    ** Open profile file
    */
    sprintf(mount_kpi_path, "%sinst_%d%s", mount_kpi_base_path, instance, "/mount/profiler");
    if ((fd = open(mount_kpi_path, O_RDONLY, S_IRWXU)) < 0) {
      severe("open(%s) %s", mount_kpi_path, strerror(errno));
      continue;
    }
    
    /*
    ** Allocate a context
    */
    ctx = allocate_rozofsmount_context(instance,fd);
    read_profiler_file(ctx);
    count++;
  }
  if (count == 0) return -1;
  
  sum_up_all_profiler();
  return 0;
}
/*________________________________________________________________
** Main
**
** Only one parameter : the polling frequency
**
*_________________________________________________________________
*/
int main(int argc, char *argv[]) {
  
  /*
  ** Check input parameter
  */
  if (argc < 2) {
    printf("Missing polling frequency.\n");
    NEWLINE("DISABLE");
    exit(1);
  }
  if (sscanf(argv[1], "%llu", (long long unsigned int *)&rozofsmout_period_micro) != 1) {
    printf("Missing polling frequency.\n");
    NEWLINE("DISABLE");
    exit(1);
  }
  rozofsmout_period_micro *= 1000000;
  
  /*
  ** Read configuration file
  */
  {
    char cfg_file[128];
    sprintf(cfg_file,"%s/rozofsmount_netdata_cfg.conf",ROZOFS_CONFIG_DIR);
    if (access(cfg_file,R_OK) !=0 ) {
      /**
      * Create empty configuration file
      */
      int fd = open(cfg_file, O_RDWR|O_CREAT, 0755); 
      close(fd);
    }
    rozofsmount_netdata_cfg_read(NULL);
  }
  /*
  ** Build the list of rozofsmount instances
  */
  if (build_rofsmount_context_list() < 0) {
    severe("No rozofsmount found");
    NEWLINE("DISABLE");
    exit(1);
  }  
  
  /*
  ** Create charts
  */
  create_rozofsmount_charts();
  
  while (1) {
    
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
    update_rozofsmount_charts();
  } 
}  
