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

#ifndef _STORAGE_DEVICE_KPI_H
#define _STORAGE_DEVICE_KPI_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage.h"

/*
** List of type of KPI
*/
typedef enum _storage_device_kpi_e {
  storage_device_kpi_read,
  storage_device_kpi_write,
  
  storage_device_kpi_max  
} storage_device_kpi_e;
#include "storage_device_kpi_e2String.h"

/*
** Number of devices
*/
extern int storage_device_kpi_nb_devices;
/*
** Number of threads that keep KPI
*/  
extern int storage_device_kpi_nb_thread;



/*
** Structure to hold performance for one device and one thread
*/
typedef struct _storage_device_one_kpi_t {
  uint64_t      count;
  uint64_t      size;
  uint64_t      time;
} storage_device_one_kpi_t;


/*
** An array of kpi for one device and one thread
*/
typedef storage_device_one_kpi_t  storage_device_all_kpi_t[storage_device_kpi_max];

/*
** One device KPI structure
*/
typedef struct storage_device_kpi_t {
  /*
  ** Device identifier
  */
  cid_t                     cid;
  cid_t                     sid;
  uint8_t                   dev;
  /*
  ** kpi is an array indexe dby thread number, so each thread can increment
  ** counters without any lock. Each ellement of the array is a 
  ** storage_device_all_kpi_t structure
  */
  storage_device_all_kpi_t * kpi; 
} storage_device_kpi_t;  
 
 
extern storage_device_kpi_t   storage_device_kpi[];
 

/*_____________________________________________________________
** Allocate memory for KPI
** 
** @param name   Device name
**
** @retval reference of the KPI records
**_____________________________________________________________
*/
int storage_device_kpi_allocate(cid_t cid, sid_t sid, uint8_t dev);

/*_____________________________________________________________
** Update KPI information
**
** @param kpiRef    Rerefence of the KPI records
** @param thread    The thread index to update KPI
** @param kpi       The type of KPI within storage_device_kpi_e
** @param size      The size of the operation
** @param time      The time consumed by this operation 
** 
**_____________________________________________________________
*/
static inline void storage_device_kpi_update(int kpiRef, int thread, storage_device_kpi_e kpi, uint64_t size, uint64_t time) {
  storage_device_one_kpi_t * one_kpi_p;

  /*
  ** Service must be initialized
  */
  if (thread >= storage_device_kpi_nb_thread) return;

  /*
  ** Check the KPI reference
  */
  kpiRef--;
  if ((kpiRef < 0)||(kpiRef>=storage_device_kpi_nb_devices)) return;

  /*
  ** Update KPI 
  */  
  one_kpi_p = &storage_device_kpi[kpiRef].kpi[thread][kpi];
  one_kpi_p->count++;
  one_kpi_p->size += size;
  one_kpi_p->time += time;
}
/*_____________________________________________________________
** Initialize KPI service
**
** @param nbThreads    Number of threads that do KPI
**_____________________________________________________________
*/
void storage_device_kpi_init(int nbThreads) ;

#endif

