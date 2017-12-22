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

#define _XOPEN_SOURCE 500


#include "storage_device_kpi.h"
#include <rozofs/core/uma_dbg_api.h>

/*
** Number of threads that keep KPI
*/  
int storage_device_kpi_nb_thread = 0;
/*
** Number of devices
*/
int storage_device_kpi_nb_devices = 0;


storage_device_kpi_t   storage_device_kpi[STORAGE_MAX_DEVICE_NB];



/*_____________________________________________________________
** Allocate memory for KPI of a device
** 
** @param cid   Cluster identifier
** @param sid   storage idetifier within the cluster 
** @param dev   device within the storage
**
** @retval reference of the KPI records
**_____________________________________________________________
*/
int storage_device_kpi_allocate(cid_t cid, sid_t sid, uint8_t dev) {
  int size;

  /*
  ** Service must be initialized
  */
  if (storage_device_kpi_nb_thread == 0) return 0;
  
  /*
  ** There is a maximum device number per logical storage
  */
  if (storage_device_kpi_nb_devices >= STORAGE_MAX_DEVICE_NB) {
    severe("Too much allocated devices");
    return 0;
  }  
  
  /*
  ** Store device identifier
  */
  storage_device_kpi[storage_device_kpi_nb_devices].cid = cid;
  storage_device_kpi[storage_device_kpi_nb_devices].sid = sid;
  storage_device_kpi[storage_device_kpi_nb_devices].dev = dev;
  
  /*
  ** Allocate memory for kpi
  */
  size = storage_device_kpi_nb_thread*sizeof(storage_device_all_kpi_t);
  storage_device_kpi[storage_device_kpi_nb_devices].kpi = malloc(size);
  memset(storage_device_kpi[storage_device_kpi_nb_devices].kpi,0,size);
  
  storage_device_kpi_nb_devices++;
  return storage_device_kpi_nb_devices;
}

/*_____________________________________________________________
** Reset KPI information
**
** @param kpiRef    Rerefence of the KPI records
** 
**_____________________________________________________________
*/
static inline void storage_device_kpi_reset(int kpiRef) {

  /*
  ** Service must be initialized
  */
  if (storage_device_kpi_nb_thread == 0) return;

  /*
  ** Check the KPI reference
  */
  kpiRef--;
  if ((kpiRef < 0)||(kpiRef>=storage_device_kpi_nb_devices)) return;
  
  if (storage_device_kpi[kpiRef].kpi == NULL) return;
  /*
  ** Reset KPI
  */
  memset(storage_device_kpi[kpiRef].kpi, 0, storage_device_kpi_nb_thread*sizeof(storage_device_all_kpi_t));
}
/*_______________________________________________________________________
* man kpi_dev CLI
*/
void storage_device_kpi_man (char * pChar) {
  pChar += rozofs_string_append_underscore(pChar,"\nUsage:\n");
  pChar += rozofs_string_append_bold(pChar,"\tkpi_device");
  pChar += rozofs_string_append     (pChar,"\t\tdisplays per device KPI.\n");
  pChar += rozofs_string_append_bold(pChar,"\tkpi_device reset");
  pChar += rozofs_string_append     (pChar,"\tdisplays per device KPI and then reset them.\n");
  pChar += rozofs_string_append     (pChar,"\nThe last KPIs displayed for cid=0, sid=0, dev=0,\n");
  pChar += rozofs_string_append     (pChar,"are just the sum of the KPI of every devices of this storio.\n");

}
/*_______________________________________________________________________
* Display the device KPIs
*/
void storage_device_kpi_display (char * argv[], uint32_t tcpRef, void *bufRef) {
  char                             * pChar = uma_dbg_get_buffer();
  int                                dev;
  storage_device_one_kpi_t         * kpi_p;
  storage_device_all_kpi_t           result; 
  storage_device_all_kpi_t           total; 
  int                                thread;
  storage_device_kpi_e               kpi;
  int                                reset = 0;

  /*
  ** Reset may be required
  */
  if (argv[1] != NULL) {
    if (strcasecmp(argv[1], "reset") == 0) {
      reset = 1;
    }
  } 

  memset(total,0,sizeof(total));
  pChar += rozofs_string_append(pChar, "\n");   

  if (storage_device_kpi_nb_devices!= 0) {

    /*
    ** Loop on configured devices 
    */
    pChar += rozofs_string_append(pChar, "{ \"device kpi\" : [");   
    for (dev=0; dev<storage_device_kpi_nb_devices; dev++) {

      kpi_p = (storage_device_one_kpi_t *) storage_device_kpi[dev].kpi;
      if (kpi_p == NULL) continue;

      memset(result,0,sizeof(result));

      /*
      ** Summ kpi of each thread
      */
      for (thread=0; thread<storage_device_kpi_nb_thread; thread++) {  
        for (kpi=0; kpi<storage_device_kpi_max; kpi++) {   
          result[kpi].count += kpi_p->count;
          result[kpi].size  += kpi_p->size;
          result[kpi].time  += kpi_p->time;
          kpi_p++;
        } 
      }
      
      /*
      ** Clearup the kpi when requested
      */
      if (reset) {
        storage_device_kpi_reset(dev+1);
      }      

      /*
      ** Keep a total for all devices
      */
      for (kpi=0; kpi<storage_device_kpi_max; kpi++) {   
        total[kpi].count += result[kpi].count;
        total[kpi].size  += result[kpi].size;
        total[kpi].time  += result[kpi].time;
      }

      /*
      ** Display the device KPI
      */       
      if (dev==0) {
        pChar += rozofs_string_append(pChar, "\n  { \"cid\" : ");  
      }
      else {
        pChar += rozofs_string_append(pChar, ",\n  { \"cid\" : ");  
      }
      pChar += rozofs_u32_append(pChar, storage_device_kpi[dev].cid);  
      pChar += rozofs_string_append(pChar, ", \"sid\" : ");  
      pChar += rozofs_u32_append(pChar, storage_device_kpi[dev].sid);  
      pChar += rozofs_string_append(pChar, ", \"dev\" : ");  
      pChar += rozofs_u32_append(pChar, storage_device_kpi[dev].dev);  
      pChar += rozofs_string_append(pChar, ",\n    \"kpi\" : [");  

      for (kpi=0; kpi<storage_device_kpi_max; kpi++) {   
        if (kpi==0) {
           pChar += rozofs_string_append(pChar, "\n      { ");
        }
        else {
           pChar += rozofs_string_append(pChar, ",\n      { ");
        }    
        pChar += rozofs_string_append(pChar, " \"ope\" : \"");
        pChar += rozofs_string_append(pChar,storage_device_kpi_e2String(kpi));
        pChar += rozofs_string_append(pChar, "\", \"count\" : ");
        pChar += rozofs_u64_append(pChar, (unsigned long long)result[kpi].count);  
        pChar += rozofs_string_append(pChar, ", \"size\" : ");
        pChar += rozofs_u64_append(pChar, (unsigned long long)result[kpi].size);  
        pChar += rozofs_string_append(pChar, ", \"time\" : ");
        pChar += rozofs_u64_append(pChar, (unsigned long long)result[kpi].time);  
        pChar += rozofs_string_append(pChar, ", \"avg time\" : ");
        pChar += rozofs_u64_append(pChar, result[kpi].count ? (unsigned long long)(result[kpi].time/result[kpi].count) : 0);  
        pChar += rozofs_string_append(pChar, ", \"avg size\" : ");
        pChar += rozofs_u64_append(pChar, result[kpi].count ? (unsigned long long)(result[kpi].size/result[kpi].count) : 0);  
        pChar += rozofs_string_append(pChar, ", \"MB/s\" : ");
        pChar += rozofs_u64_append(pChar, result[kpi].time ? (unsigned long long)(result[kpi].size/result[kpi].time) : 0);  
        pChar += rozofs_string_append(pChar, "}");    
      }
      pChar += rozofs_string_append(pChar, "\n  ]}");
    }

    /*
    ** Display the sum of all devices
    */
    pChar += rozofs_string_append(pChar, ",\n\n  { \"cid\" : 0, \"sid\" : 0, \"dev\" : 0, ");  
    pChar += rozofs_string_append(pChar, "\n    \"kpi\" : [");  

    for (kpi=0; kpi<storage_device_kpi_max; kpi++) {   
      pChar += rozofs_string_append(pChar, ",\n      { ");
      pChar += rozofs_string_append(pChar, " \"ope\" : \"");
      pChar += rozofs_string_append(pChar,storage_device_kpi_e2String(kpi));
      pChar += rozofs_string_append(pChar, "\", \"count\" : ");
      pChar += rozofs_u64_append(pChar, (unsigned long long)total[kpi].count);  
      pChar += rozofs_string_append(pChar, ", \"size\" : ");
      pChar += rozofs_u64_append(pChar, (unsigned long long)total[kpi].size);  
      pChar += rozofs_string_append(pChar, ", \"time\" : ");
      pChar += rozofs_u64_append(pChar, (unsigned long long)total[kpi].time);  
      pChar += rozofs_string_append(pChar, ", \"avg time\" : ");
      pChar += rozofs_u64_append(pChar, total[kpi].count ? (unsigned long long)(total[kpi].time/total[kpi].count) : 0);  
      pChar += rozofs_string_append(pChar, ", \"avg size\" : ");
      pChar += rozofs_u64_append(pChar, total[kpi].count ? (unsigned long long)(total[kpi].size/total[kpi].count) : 0);  
      pChar += rozofs_string_append(pChar, ", \"MB/s\" : ");
      pChar += rozofs_u64_append(pChar, total[kpi].time ? (unsigned long long)(total[kpi].size/total[kpi].time) : 0);  
      pChar += rozofs_string_append(pChar, "}");    
    }
    pChar += rozofs_string_append(pChar, "\n  ]}");  
    pChar += rozofs_string_append(pChar, "\n]}\n");   
  }
  
  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());  
}
/*_____________________________________________________________
** Initialize KPI service
**
** @param nbThreads    Number of threads that do KPI
**_____________________________________________________________
*/
void storage_device_kpi_init(int nbThreads) {
  storage_device_kpi_nb_thread = nbThreads;
  memset(storage_device_kpi,0,sizeof(storage_device_kpi));
  uma_dbg_addTopicAndMan("kpi_device", storage_device_kpi_display,storage_device_kpi_man,UMA_DBG_OPTION_RESET);
  
}
