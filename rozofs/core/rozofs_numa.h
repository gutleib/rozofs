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
 #ifndef ROZOFS_NUMA_H
 #define ROZOFS_NUMA_H
#include <inttypes.h>
#include <unistd.h>
#include <numa.h>
#include <numaif.h>

typedef struct _rozofs_numa_mem_t {
   int mode; /** 0 or MPOL_F_STATIC_NODES,MPOL_F_RELATIVE_NODES */
   unsigned long nodemask;
   unsigned long maxnode;
   unsigned flags; /** MPOL_DEFAULT,MPOL_BIND,MPOL_INTERLEAVE,MPOL_PREFERRED,MPOL_LOCAL */
} rozofs_numa_mem_t;
/**
*  case of NUMA: allocate the running node according to the
*  instance

   @param instance: instance number of the process
   @param criteria: the criteria that leaded to the instance choice
*/
void rozofs_numa_allocate_node(int instance, char * criteria);
/**
*  case of NUMA: allocate the running node according to the
*  instance

   @param instance: instance number of the process
   @param excluded_node: numa node to exclude (can be -1 when not significant)
*/
void rozofs_numa_run_on_node(uint32_t instance, int excluded_node);

#endif
