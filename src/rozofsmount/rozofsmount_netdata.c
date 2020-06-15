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
#include <rozofs/rpc/stcpproto.h>
#include <rozofs/common/log.h>
#include <rozofs/common/list.h>
#include "rozofsmount_netdata_cfg.h"
#include <pthread.h>

typedef struct _rozofsmount_ctx_t {
  list_t         list;
  list_t         storcli_list;
  int            instance; /*<< Rozofsmount instance. */
  int            fd;       /*<< Profiler file fd */
  mpp_profiler_t profiler;
  mpp_profiler_t old_profiler;
} rozofsmount_ctx_t;

typedef struct _storcli_ctx_t {
  list_t         list;
  int            instance; /*<< Rozofsmount instance. */
  int            fd;       /*<< Profiler file fd */
  stcpp_profiler_t profiler;
  stcpp_profiler_t old_profiler;
} storcli_ctx_t;


uint64_t              rozofsmout_period_micro = 0;
list_t                rozofsmount_list;
uint64_t              next_run = 0;
uint64_t              delay_between_run = 0;
uint64_t              last_run = 0;
uint64_t              cfg_mtime;
uint32_t              rozofsmount_count = 0;

#define NEWLINE(fmt, ...) {\
  char string[128];\
  sprintf(string,fmt, ##__VA_ARGS__); \
  if (rozofsmount_netdata_cfg.debug) info("%s",string);\
  printf("%s\n",string);\
}  

/*________________________________________________________________
** Create all rozofsmount charts
**
*_________________________________________________________________
*/
#define MOUNT_CREATE_ONE_CHART_COUNT(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  char                              line[4096];\
  char                            * pChar = line;\
\
  if ((rozofsmount_netdata_cfg.display_count) && (rozofsmount_netdata_cfg.display_mount_count_##X)) {\
    pChar += rozofs_string_append(pChar,"CHART rozofsmount."#X"_count '' 'Number of calls to "#X" API of every rozofsmount instance' 'calls' "#X" '' stacked 100 1 rozofsmount_netdata.plugin\n");\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"DIMENSION mount");\
      pChar += rozofs_u32_append(pChar,ctx->instance);\
      pChar += rozofs_string_append(pChar," '' incremental\n");\
    }\
    fputs(line,stdout);\
  }\
}
#define STORCLI_CREATE_ONE_CHART_COUNT(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  list_t                          * q;\
  storcli_ctx_t                   * storcliCtx;\
  char                              line[4096];\
  char                            * pChar = line;\
\
  if ((rozofsmount_netdata_cfg.display_count) && (rozofsmount_netdata_cfg.display_storcli_count_##X)) {\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"CHART rozofsmount.");\
      pChar += rozofs_u32_append(pChar,ctx->instance);\
      pChar += rozofs_string_append(pChar,"."#X"_count '' 'Number of calls to "#X" API of each storcli instance of mount");\
      pChar += rozofs_u32_append(pChar,ctx->instance);\
      pChar += rozofs_string_append(pChar,"' 'calls' "#X" '' stacked 100 1 rozofsmount_netdata.plugin\n");\
      list_for_each_forward(q, &ctx->storcli_list) {\
        storcliCtx = list_entry(q, storcli_ctx_t, list);\
        pChar += rozofs_string_append(pChar,"DIMENSION storcli");\
        pChar += rozofs_u32_append(pChar,storcliCtx->instance);\
        pChar += rozofs_string_append(pChar," '' incremental\n");\
      }\
    }\
    fputs(line,stdout);\
  }\
}
#define MOUNT_CREATE_ONE_CHART_DURATION(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  char                              line[4096];\
  char                            * pChar = line;\
\
  if ((rozofsmount_netdata_cfg.display_duration) && (rozofsmount_netdata_cfg.display_mount_duration_##X)) {\
    pChar += rozofs_string_append(pChar,"CHART rozofsmount."#X"_duration '' 'Duration of a call to "#X" API of every rozofsmount instance' 'microseconds' "#X" '' area 100 1 rozofsmount_netdata.plugin\n");\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"DIMENSION mount");\
      pChar += rozofs_u32_append(pChar,ctx->instance);\
      pChar += rozofs_string_append(pChar," '' absolute\n");\
    }\
    fputs(line,stdout);\
  }\
}
#define STORCLI_CREATE_ONE_CHART_DURATION(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  list_t                          * q;\
  storcli_ctx_t                   * storcliCtx;\
  char                              line[4096];\
  char                            * pChar = line;\
\
  if ((rozofsmount_netdata_cfg.display_duration) && (rozofsmount_netdata_cfg.display_storcli_duration_##X)) {\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"CHART rozofsmount.");\
      pChar += rozofs_u32_append(pChar,ctx->instance);\
      pChar += rozofs_string_append(pChar,"."#X"_duration '' 'duration of a call to "#X" API of each storcli instance of mount");\
      pChar += rozofs_u32_append(pChar,ctx->instance);\
      pChar += rozofs_string_append(pChar,"' 'microseconds' "#X" '' area 100 1 rozofsmount_netdata.plugin\n");\
      list_for_each_forward(q, &ctx->storcli_list) {\
        storcliCtx = list_entry(q, storcli_ctx_t, list);\
        pChar += rozofs_string_append(pChar,"DIMENSION storcli");\
        pChar += rozofs_u32_append(pChar,storcliCtx->instance);\
        pChar += rozofs_string_append(pChar," '' absolute\n");\
      }\
    }\
    fputs(line,stdout);\
  }\
}
#define MOUNT_CREATE_ONE_CHART_THROUGHPUT(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  char                              line[4096];\
  char                            * pChar = line;\
\
  if ((rozofsmount_netdata_cfg.display_bytes) && (rozofsmount_netdata_cfg.display_mount_bytes_##X)) {\
    pChar += rozofs_string_append(pChar,"CHART rozofsmount."#X"_throughput '' '"#X" throughput of every rozofsmount instance' 'MB/s' "#X" '' stacked 100 1 rozofsmount_netdata.plugin\n");\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"DIMENSION mount");\
      pChar += rozofs_u32_append(pChar,ctx->instance);\
      pChar += rozofs_string_append(pChar," '' incremental\n");\
    }\
    fputs(line,stdout);\
  }\
}
#define STORCLI_CREATE_ONE_CHART_THROUGHPUT(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  list_t                          * q;\
  storcli_ctx_t                   * storcliCtx;\
  char                              line[4096];\
  char                            * pChar = line;\
\
  if ((rozofsmount_netdata_cfg.display_bytes) && (rozofsmount_netdata_cfg.display_storcli_bytes_##X)) {\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"CHART rozofsmount.");\
      pChar += rozofs_u32_append(pChar,ctx->instance);\
      pChar += rozofs_string_append(pChar,"."#X"_throughput '' '"#X" throughput of each storcli instance of mount");\
      pChar += rozofs_u32_append(pChar,ctx->instance);\
      pChar += rozofs_string_append(pChar,"' 'MB/s' "#X" '' stacked 100 1 rozofsmount_netdata.plugin\n");\
      list_for_each_forward(q, &ctx->storcli_list) {\
        storcliCtx = list_entry(q, storcli_ctx_t, list);\
        pChar += rozofs_string_append(pChar,"DIMENSION storcli");\
        pChar += rozofs_u32_append(pChar,storcliCtx->instance);\
        pChar += rozofs_string_append(pChar," '' incremental\n");\
      }\
    }\
    fputs(line,stdout);\
  }\
}
#define MOUNT_CREATE_2_CHARTS(X) {\
  MOUNT_CREATE_ONE_CHART_COUNT(X)\
  MOUNT_CREATE_ONE_CHART_DURATION(X)\
}   
#define MOUNT_CREATE_3_CHARTS(X) {\
  MOUNT_CREATE_ONE_CHART_COUNT(X)\
  MOUNT_CREATE_ONE_CHART_DURATION(X)\
  MOUNT_CREATE_ONE_CHART_THROUGHPUT(X)\
}  
#define STORCLI_CREATE_2_CHARTS(X) {\
  STORCLI_CREATE_ONE_CHART_COUNT(X)\
  STORCLI_CREATE_ONE_CHART_DURATION(X)\
} 
#define STORCLI_CREATE_3_CHARTS(X) {\
  STORCLI_CREATE_ONE_CHART_COUNT(X)\
  STORCLI_CREATE_ONE_CHART_DURATION(X)\
  STORCLI_CREATE_ONE_CHART_THROUGHPUT(X)\
}   
void create_rozofsmount_charts() { 
  if (rozofsmount_count==0) return;
  MOUNT_CREATE_2_CHARTS(lookup);
  MOUNT_CREATE_2_CHARTS(lookup_agg);
  MOUNT_CREATE_2_CHARTS(forget);
  MOUNT_CREATE_2_CHARTS(getattr);
  MOUNT_CREATE_2_CHARTS(setattr);
  MOUNT_CREATE_2_CHARTS(readlink);
  MOUNT_CREATE_2_CHARTS(mknod);
  MOUNT_CREATE_2_CHARTS(mkdir);
  MOUNT_CREATE_2_CHARTS(unlink);
  MOUNT_CREATE_2_CHARTS(rmdir);
  MOUNT_CREATE_2_CHARTS(symlink);
  MOUNT_CREATE_2_CHARTS(rename);
  MOUNT_CREATE_2_CHARTS(open);
  MOUNT_CREATE_2_CHARTS(link);
  MOUNT_CREATE_3_CHARTS(read);
  MOUNT_CREATE_3_CHARTS(write);
  MOUNT_CREATE_2_CHARTS(flush);
  MOUNT_CREATE_2_CHARTS(release);
  MOUNT_CREATE_2_CHARTS(opendir);
  MOUNT_CREATE_2_CHARTS(readdir);
  MOUNT_CREATE_2_CHARTS(releasedir);
  MOUNT_CREATE_2_CHARTS(fsyncdir);
  MOUNT_CREATE_2_CHARTS(statfs);
  MOUNT_CREATE_2_CHARTS(setxattr);
  MOUNT_CREATE_2_CHARTS(getxattr);
  MOUNT_CREATE_2_CHARTS(listxattr);
  MOUNT_CREATE_2_CHARTS(removexattr);
  MOUNT_CREATE_2_CHARTS(access);
  MOUNT_CREATE_2_CHARTS(create);
  MOUNT_CREATE_2_CHARTS(getlk);
  MOUNT_CREATE_2_CHARTS(setlk);
  MOUNT_CREATE_2_CHARTS(setlk_int);
  MOUNT_CREATE_2_CHARTS(ioctl);
  MOUNT_CREATE_2_CHARTS(clearlkowner); 
  
  STORCLI_CREATE_3_CHARTS(read);
  STORCLI_CREATE_3_CHARTS(write);
}
/*________________________________________________________________
** Update all rozofsmount charts
**
*_________________________________________________________________
*/
#define MOUNT_UPDATE_ONE_CHART_COUNT(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  char                              line[4096];\
  char                            * pChar = line;\
\
  if ((rozofsmount_netdata_cfg.display_count) && (rozofsmount_netdata_cfg.display_mount_count_##X)) {\
    pChar += rozofs_string_append(pChar,"BEGIN rozofsmount."#X"_count ");\
    pChar += rozofs_u64_append(pChar, rozofsmout_period_micro);\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"\nSET mount");\
      pChar += rozofs_u32_append(pChar, ctx->instance);\
      pChar += rozofs_string_append(pChar," = ");\
      pChar += rozofs_u64_append(pChar, ctx->profiler.rozofs_ll_##X[P_COUNT]);\
    }\
    pChar += rozofs_string_append(pChar,"\nEND\n");\
    fputs(line,stdout);\
  }\
}
#define STORCLI_UPDATE_ONE_CHART_COUNT(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  list_t                          * q;\
  storcli_ctx_t                   * storcliCtx;\
  char                              line[4096];\
  char                            * pChar = line;\
\
  if ((rozofsmount_netdata_cfg.display_count) && (rozofsmount_netdata_cfg.display_storcli_count_##X)) {\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"BEGIN rozofsmount.");\
      pChar += rozofs_u32_append(pChar,ctx->instance);\
      pChar += rozofs_string_append(pChar,"."#X"_count ");\
      pChar += rozofs_u64_append(pChar, rozofsmout_period_micro);\
      list_for_each_forward(q, &ctx->storcli_list) {\
        storcliCtx = list_entry(q, storcli_ctx_t, list);\
        pChar += rozofs_string_append(pChar,"\nSET storcli");\
        pChar += rozofs_u32_append(pChar, storcliCtx->instance);\
        pChar += rozofs_string_append(pChar," = ");\
        pChar += rozofs_u64_append(pChar, storcliCtx->profiler.X[P_COUNT]);\
      }\
      pChar += rozofs_string_append(pChar,"\nEND\n");\
    }\
    fputs(line,stdout);\
  }\
}
#define MOUNT_UPDATE_ONE_CHART_DURATION(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  char                              line[4096];\
  char                            * pChar = line;\
\
  if ((rozofsmount_netdata_cfg.display_duration) && (rozofsmount_netdata_cfg.display_mount_duration_##X)) {\
    pChar += rozofs_string_append(pChar,"BEGIN rozofsmount."#X"_duration ");\
    pChar += rozofs_u64_append(pChar, rozofsmout_period_micro);\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"\nSET mount");\
      pChar += rozofs_u32_append(pChar, ctx->instance);\
      if (ctx->profiler.rozofs_ll_##X[P_COUNT] == ctx->old_profiler.rozofs_ll_##X[P_COUNT]) {\
        pChar += rozofs_string_append(pChar," = 0");\
      }\
      else {\
        pChar += rozofs_string_append(pChar," = ");\
        pChar += rozofs_u64_append(pChar,(ctx->profiler.rozofs_ll_##X[P_ELAPSE] - ctx->old_profiler.rozofs_ll_##X[P_ELAPSE])/(ctx->profiler.rozofs_ll_##X[P_COUNT] - ctx->old_profiler.rozofs_ll_##X[P_COUNT]));\
      }\
    }\
    pChar += rozofs_string_append(pChar,"\nEND\n");\
    fputs(line,stdout);\
  }\
}
#define STORCLI_UPDATE_ONE_CHART_DURATION(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  list_t                          * q;\
  storcli_ctx_t                   * storcliCtx;\
  char                              line[4096];\
  char                            * pChar = line;\
\
  if ((rozofsmount_netdata_cfg.display_duration) && (rozofsmount_netdata_cfg.display_storcli_duration_##X)) {\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"BEGIN rozofsmount.");\
      pChar += rozofs_u32_append(pChar,ctx->instance);\
      pChar += rozofs_string_append(pChar,"."#X"_duration ");\
      pChar += rozofs_u64_append(pChar, rozofsmout_period_micro);\
      list_for_each_forward(q, &ctx->storcli_list) {\
        storcliCtx = list_entry(q, storcli_ctx_t, list);\
        pChar += rozofs_string_append(pChar,"\nSET storcli");\
        pChar += rozofs_u32_append(pChar, storcliCtx->instance);\
        pChar += rozofs_string_append(pChar," = ");\
        if (storcliCtx->profiler.X[P_COUNT] == storcliCtx->old_profiler.X[P_COUNT]) {\
          pChar += rozofs_string_append(pChar,"0");\
        }\
        else {\
          pChar += rozofs_u64_append(pChar,(storcliCtx->profiler.X[P_ELAPSE] - storcliCtx->old_profiler.X[P_ELAPSE])/(storcliCtx->profiler.X[P_COUNT] - storcliCtx->old_profiler.X[P_COUNT]));\
        }\
      }\
      pChar += rozofs_string_append(pChar,"\nEND\n");\
    }\
    fputs(line,stdout);\
  }\
}
#define MOUNT_UPDATE_ONE_CHART_TROUGHPUT(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  char                              line[4096];\
  char                            * pChar = line;\
\
  if ((rozofsmount_netdata_cfg.display_bytes) && (rozofsmount_netdata_cfg.display_mount_bytes_##X)) {\
    pChar += rozofs_string_append(pChar,"BEGIN rozofsmount."#X"_throughput ");\
    pChar += rozofs_u64_append(pChar, rozofsmout_period_micro);\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"\nSET mount");\
      pChar += rozofs_u32_append(pChar, ctx->instance);\
      pChar += rozofs_string_append(pChar," = ");\
      pChar += rozofs_u64_append(pChar,ctx->profiler.rozofs_ll_##X[P_BYTES]/1000000);\
    }\
    pChar += rozofs_string_append(pChar,"\nEND\n");\
    fputs(line,stdout);\
  }\
}
#define STORCLI_UPDATE_ONE_CHART_TROUGHPUT(X) {\
  list_t                          * p;\
  rozofsmount_ctx_t               * ctx;\
  list_t                          * q;\
  storcli_ctx_t                   * storcliCtx;\
  char                              line[4096];\
  char                            * pChar = line;\
\
  if ((rozofsmount_netdata_cfg.display_bytes) && (rozofsmount_netdata_cfg.display_storcli_bytes_##X)) {\
    list_for_each_forward(p, &rozofsmount_list) {\
      ctx = list_entry(p, rozofsmount_ctx_t, list);\
      pChar += rozofs_string_append(pChar,"BEGIN rozofsmount.");\
      pChar += rozofs_u32_append(pChar,ctx->instance);\
      pChar += rozofs_string_append(pChar,"."#X"_throughput ");\
      pChar += rozofs_u64_append(pChar, rozofsmout_period_micro);\
      list_for_each_forward(q, &ctx->storcli_list) {\
        storcliCtx = list_entry(q, storcli_ctx_t, list);\
        pChar += rozofs_string_append(pChar,"\nSET storcli");\
        pChar += rozofs_u32_append(pChar, storcliCtx->instance);\
        pChar += rozofs_string_append(pChar," = ");\
        pChar += rozofs_u64_append(pChar,storcliCtx->profiler.X[P_BYTES]/1000000);\
      }\
      pChar += rozofs_string_append(pChar,"\nEND\n");\
    }\
    fputs(line,stdout);\
  }\
}
#define MOUNT_UPDATE_2_CHARTS(X) {\
  MOUNT_UPDATE_ONE_CHART_COUNT(X)\
  MOUNT_UPDATE_ONE_CHART_DURATION(X)\
}
#define MOUNT_UPDATE_3_CHARTS(X) {\
  MOUNT_UPDATE_ONE_CHART_COUNT(X)\
  MOUNT_UPDATE_ONE_CHART_DURATION(X)\
  MOUNT_UPDATE_ONE_CHART_TROUGHPUT(X)\
}
#define STORCLI_UPDATE_2_CHARTS(X) {\
  STORCLI_UPDATE_ONE_CHART_COUNT(X)\
  STORCLI_UPDATE_ONE_CHART_DURATION(X)\
}
#define STORCLI_UPDATE_3_CHARTS(X) {\
  STORCLI_UPDATE_ONE_CHART_COUNT(X)\
  STORCLI_UPDATE_ONE_CHART_DURATION(X)\
  STORCLI_UPDATE_ONE_CHART_TROUGHPUT(X)\
}
void update_rozofsmount_charts() { 
  if (rozofsmount_count==0) return;
  MOUNT_UPDATE_2_CHARTS(lookup);
  MOUNT_UPDATE_2_CHARTS(lookup_agg);
  MOUNT_UPDATE_2_CHARTS(forget);
  MOUNT_UPDATE_2_CHARTS(getattr);
  MOUNT_UPDATE_2_CHARTS(setattr);
  MOUNT_UPDATE_2_CHARTS(readlink);
  MOUNT_UPDATE_2_CHARTS(mknod);
  MOUNT_UPDATE_2_CHARTS(mkdir);
  MOUNT_UPDATE_2_CHARTS(unlink);
  MOUNT_UPDATE_2_CHARTS(rmdir);
  MOUNT_UPDATE_2_CHARTS(symlink);
  MOUNT_UPDATE_2_CHARTS(rename);
  MOUNT_UPDATE_2_CHARTS(open);
  MOUNT_UPDATE_2_CHARTS(link);
  MOUNT_UPDATE_3_CHARTS(read);
  MOUNT_UPDATE_3_CHARTS(write);
  MOUNT_UPDATE_2_CHARTS(flush);
  MOUNT_UPDATE_2_CHARTS(release);
  MOUNT_UPDATE_2_CHARTS(opendir);
  MOUNT_UPDATE_2_CHARTS(readdir);
  MOUNT_UPDATE_2_CHARTS(releasedir);
  MOUNT_UPDATE_2_CHARTS(fsyncdir);
  MOUNT_UPDATE_2_CHARTS(statfs);
  MOUNT_UPDATE_2_CHARTS(setxattr);
  MOUNT_UPDATE_2_CHARTS(getxattr);
  MOUNT_UPDATE_2_CHARTS(listxattr);
  MOUNT_UPDATE_2_CHARTS(removexattr);
  MOUNT_UPDATE_2_CHARTS(access);
  MOUNT_UPDATE_2_CHARTS(create);
  MOUNT_UPDATE_2_CHARTS(getlk);
  MOUNT_UPDATE_2_CHARTS(setlk);
  MOUNT_UPDATE_2_CHARTS(setlk_int);
  MOUNT_UPDATE_2_CHARTS(ioctl);
  MOUNT_UPDATE_2_CHARTS(clearlkowner); 
  
  STORCLI_UPDATE_3_CHARTS(read);
  STORCLI_UPDATE_3_CHARTS(write)
  fflush(stdout);
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
  list_init(&ctx->storcli_list);
  
  list_init(&ctx->list);
  list_push_back(&rozofsmount_list,&ctx->list);
  
  ctx->instance = instance;
  ctx->fd       = fd;
  return ctx;
}
/*________________________________________________________________
** Allocate a storcli context
**
** @param ctx              Rozofsmout context
** @param instance         Rozofsmout instance
** @param fd               file descriptor of the profiler file
**
** @retval rozofsmount context address
*_________________________________________________________________
*/
storcli_ctx_t * allocate_storcli_context(rozofsmount_ctx_t * ctx, int instance, int fd) {
  storcli_ctx_t   * storliCtx;  
  
  storliCtx = malloc(sizeof(storcli_ctx_t));
  memset(storliCtx,0,sizeof(storcli_ctx_t));
  list_init(&storliCtx->list);
  
  list_push_back(&ctx->storcli_list,&storliCtx->list);
  
  storliCtx->instance = instance;
  storliCtx->fd       = fd;
  return storliCtx;
}
/*________________________________________________________________
** Find out a rozofsmount context from its instance
**
** @param instance         Rozofsmout instance
**
** @retval rozofsmount context address or null
*_________________________________________________________________
*/
rozofsmount_ctx_t * find_rozofsmount_context(int instance) {
  list_t                          * p;
  rozofsmount_ctx_t               * ctx;

  list_for_each_forward(p, &rozofsmount_list) {
    ctx = list_entry(p, rozofsmount_ctx_t, list);
    if (ctx->instance == instance) return ctx;
  }
  return NULL;  
}
/*________________________________________________________________
** Find out a storcli context from its instance
**
** @param ctx         Rozofsmout context
** @param stc         Storcli instance
**
** @retval storcli context address or null
*_________________________________________________________________
*/
storcli_ctx_t * find_storcli_context(rozofsmount_ctx_t * ctx, int stc) {
  list_t                          * p;
  storcli_ctx_t                   * storliCtx;

  list_for_each_forward(p, &ctx->storcli_list) {
    storliCtx = list_entry(p, storcli_ctx_t, list);
    if (storliCtx->instance == stc) return storliCtx;
  }
  return NULL;  
}
/*________________________________________________________________
** Read rozofsmount profiler file 
**
** @param       ctx         Rozofsmout context
*_________________________________________________________________
*/
void read_rozofsmount_profiler_file(rozofsmount_ctx_t * ctx) {
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
** Read storcli profiler file 
**
** @param       ctx         Storcli context
*_________________________________________________________________
*/
void read_storcli_profiler_file(storcli_ctx_t * ctx) {
  /*
  ** Save old values
  */
  memcpy(&ctx->old_profiler,&ctx->profiler, sizeof(ctx->profiler));
  /*
  ** Read new values
  */
  pread(ctx->fd, &ctx->profiler, sizeof(stcpp_profiler_t), 0); 
}
/*________________________________________________________________
** re-read profiler files 
**
** @param       ctx         Rozofsmout context
*_________________________________________________________________
*/
void read_all_profiler_files() {
  list_t                          * p;
  list_t                          * q;
  rozofsmount_ctx_t               * ctx;
  storcli_ctx_t                   * storcliCtx;

  list_for_each_forward(p, &rozofsmount_list) {
    ctx = list_entry(p, rozofsmount_ctx_t, list);
    read_rozofsmount_profiler_file(ctx);
    list_for_each_forward(q, &ctx->storcli_list) {
      storcliCtx = list_entry(q, storcli_ctx_t, list);
      read_storcli_profiler_file(storcliCtx);
    }       
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
  next_run += rozofsmout_period_micro;  

  now_micro = get_time_microseconds();
  while ( now_micro < next_run ) {
    usleep(next_run - now_micro);
    now_micro = get_time_microseconds();
  }  
  if (last_run == 0) {
    delay_between_run = rozofsmout_period_micro;
  }
  else {
    delay_between_run =  now_micro - last_run;
  }   
  last_run = now_micro; 
}
/*________________________________________________________________
** Build the list of rozofsmount instances
**
** @retval number of new rozofsmount instance found
*_________________________________________________________________
*/
int build_rofsmount_context_list() {
  char                 mount_kpi_base_path[256];
  char                 mount_kpi_path[1024];
  char                 mount_dir[1024];
  char              *  pChar = mount_kpi_base_path;
  DIR               *  d;
  DIR               *  d1;
  struct dirent     *  dir1;
  struct dirent     *  dir;
  int                  instance;
  int                  stc;
  rozofsmount_ctx_t *  ctx;  
  storcli_ctx_t     *  storcliCtx;  
  int                  fd;
  int                  new = 0;  

  rozofsmount_count = 0;
 
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

    sprintf(mount_dir, "%sinst_%d/", mount_kpi_base_path, instance);
    
    ctx = find_rozofsmount_context(instance);
    if (ctx) {
      /*
      ** already in list 
      */
      rozofsmount_count++;
    }
    else {

      /*
      ** Open profile file
      */
      sprintf(mount_kpi_path, "%s/%s", mount_dir, "mount/profiler");
      if ((fd = open(mount_kpi_path, O_RDONLY, S_IRWXU)) < 0) {
        severe("open(%s) %s", mount_kpi_path, strerror(errno));
        continue;
      }

      /*
      ** Allocate a context
      */
      ctx = allocate_rozofsmount_context(instance,fd);
      read_rozofsmount_profiler_file(ctx);
      new++;
      rozofsmount_count++;
    }  
    
    
    d1 = opendir(mount_dir);
    if (d1==NULL) {
      severe("open(%s) %s",mount_dir,strerror(errno));
      return -1;
    }
    
    while ((dir1 = readdir(d1)) != NULL) { 
  
      if (sscanf(dir1->d_name, "storcli_%d", &stc) != 1) {
        continue;
      }

      storcliCtx = find_storcli_context(ctx,stc);
      if (storcliCtx) {
        /*
        ** already in list 
        */
        rozofsmount_count++;
        continue;
      }
      /*
      ** Open profiler file
      */
      sprintf(mount_kpi_path, "%s%s/profiler", mount_dir, dir1->d_name);
      if ((fd = open(mount_kpi_path, O_RDONLY, S_IRWXU)) < 0) {
        severe("open(%s) %s", mount_kpi_path, strerror(errno));
        continue;
      }

      /*
      ** Allocate a context
      */
      storcliCtx = allocate_storcli_context(ctx,stc,fd);
      read_storcli_profiler_file(storcliCtx);
      new++;
      rozofsmount_count++;            
    } 
    closedir(d1);   
  }  
  
  closedir(d);
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
  ** Recheck the list of rozofsmount
  */
  new = build_rofsmount_context_list();
  
  /*
  ** Reread configuration file modification time
  */
  new_cfg_mtime = rozofsmount_netdata_cfg_get_mtime(NULL); 
  if (new_cfg_mtime != cfg_mtime) {
    /*
    ** Re-read the configuration file
    */
    rozofsmount_netdata_cfg_read(NULL);
  } 
   
  /*
  ** If some new rozofsmount appear
  ** or new config parameters, re-print the charts
  */
  if ((new) || (new_cfg_mtime != cfg_mtime)) {
    create_rozofsmount_charts();
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
  ** Initialize the list of context
  */
  list_init(&rozofsmount_list);
  
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
    /*
    ** Record configuration file modification time to check later
    ** whether it has been updated
    */
    cfg_mtime = rozofsmount_netdata_cfg_get_mtime(NULL); 
    /*
    ** Read the configuration file
    */
    rozofsmount_netdata_cfg_read(NULL);
  }
  
  /*
  ** Build the list of rozofsmount instances
  */
  build_rofsmount_context_list();
  
  /*
  ** Create charts
  */
  create_rozofsmount_charts();

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
    update_rozofsmount_charts();
  } 
}  
