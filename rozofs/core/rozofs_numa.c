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
#include <numa.h>
#include "rozofs_numa.h"
#include <rozofs/common/common_config.h>
#include <rozofs/common/log.h>
#include <rozofs/core/uma_dbg_api.h>

static    int bit = -1;
static    int dbg_recorded = 0;
static    char * numa_criteria = NULL;

void show_numa(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
  int available;
  
  pChar += sprintf(pChar,"{ \"numa\" : {\n");
  pChar += sprintf(pChar,"    \"aware\"     : \"%s\",\n",
                   common_config.numa_aware?"True":"False");
  available = numa_available();
  pChar += sprintf(pChar,"    \"available\" : ");

  if (available<0) {
    pChar += sprintf(pChar,"\"False\"\n");
  }
  else {
    pChar += sprintf(pChar,"\"True\",\n");
    pChar += sprintf(pChar,"    \"nodes    \" : %d,\n",
                     numa_num_configured_nodes());
    pChar += sprintf(pChar,"    \"criteria \" : \"%s\",\n",
                     numa_criteria);
    pChar += sprintf(pChar,"    \"node     \" : %d\n",
                     bit);
  }
  pChar += sprintf(pChar,"   }\n}");
  
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
} 
/**
*  case of NUMA: allocate the running node according to the
*  instance

   @param instance: instance number of the process
   @param criteria: the criteria that leaded to the instance choice
*/
void rozofs_numa_allocate_node(int instance, char * criteria)
{
   int configured_nodes;
   int available;

   numa_criteria = criteria;
   
   if (dbg_recorded==0) {
     dbg_recorded = 1;
     uma_dbg_addTopic("numa", show_numa);
   }   
   
   if (!common_config.numa_aware) {
     info("rozofs_numa_allocate_node(%d): aware not configured", instance);
     return;
   }  
   
   available = numa_available();
   if (available < 0)
   {
     /*
     ** numa not available
     */
     info("rozofs_numa_allocate_node(%d): numa not available", instance);
     return;
   }  

   configured_nodes = numa_num_configured_nodes();   
   bit = instance%configured_nodes;
   numa_run_on_node(bit); 
   /*
   ** set the preferred memory
   */
   numa_set_preferred(bit);
   info("rozofs_numa_allocate_node(%d): set on node %d", instance,bit);
}
