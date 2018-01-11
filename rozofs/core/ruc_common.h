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

#ifndef RUC_COMMON_H
#define RUC_COMMON_H

#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include <sys/time.h>

extern struct timeval     Global_timeDay;
extern unsigned long long Global_timeBefore, Global_timeAfter;
#if 0
#define MICROLONGL(time) ((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)


#define STARTTIME      gettimeofday(&Global_timeDay,(struct timezone *)0);Global_timeBefore = MICROLONGL(Global_timeDay);

#define STOPTIME       gettimeofday(&Global_timeDay,(struct timezone *)0);  Global_timeAfter = MICROLONGL(Global_timeDay);\
                       printf("file:%s:%d  CPU time (us): %d\n",__FILE__,__LINE__,(int)(Global_timeAfter - Global_timeBefore));
#else


#define STARTTIME

#define STOPTIME

#endif

#define RUC_OK 0
#define RUC_NOK -1

/*
**  Object Types :
**
** An object reference is built as follows:
**
**   31-----------24+23--------------------0
**      Type        |     Index            |
**   +--------------+----------------------+
*/

#define RUC_OBJ_SHIFT_OBJ_TYPE 24
#define RUC_OBJ_MASK_OBJ_IDX   0xffffff

#define RUC_PDP_MASK_OBJ_IDX     0x7fffff
#define RUC_PDP_TEID_TYPE_GGSN   0x800000

#define RUC_ALLOCATED  1
#define RUC_FREE       2
/*
** known object types
*/
#define UMA_TCP_CTX_TYPE  0x41
//#define RUC_RELC_CTX_TYPE 0x42
//#define UMA_CELL_CTX_TYPE  0x43
#define RUC_TCP_SERVER_CTX_TYPE 0x44
//#define UMA_MS_REGISTERED_TYPE 0x45


/*
** constants for socketpair
*/
#define RUC_SOC_SEND  0
#define RUC_SOC_RECV  1

/*
** common error code for socket xmit
*/
#define RUC_WOULDBLOCK 1  /* congested */
#define RUC_DISC 2        /* socket error or disconnection */
#define RUC_PARTIAL 3        /* partial read on socket*/

/*
**   AGING constants
*/
#define UMA_AGING_IDLE  0
#define UMA_AGING_RUN   1
#define UMA_AGING_AGED  2


typedef void (*ruc_pf_2uint32_t)(uint32_t par1, uint32_t par2);
/*
**____________________________________________________________________________
*/
/**
* api for reading the cycles counter
*/

static inline unsigned long long ruc_rdtsc(void)
{
  unsigned hi,lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo)| (((unsigned long long)hi)<<32);

}

#endif
