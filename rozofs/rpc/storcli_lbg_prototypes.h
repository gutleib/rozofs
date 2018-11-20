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
 
 #ifndef STORCLI_LBG_PROTOTYPES_H
 #define STORCLI_LBG_PROTOTYPES_H
 
 #include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <errno.h>
#include <string.h>
#include <rpc/pmap_clnt.h>
#include <unistd.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/rpc/eclient.h>
#include "rpcclt.h"
#include <rozofs/core/af_unix_socket_generic.h>
 
#define STORCLI_PER_FSMOUNT_POWER2 5
#define STORCLI_PER_FSMOUNT (1<<STORCLI_PER_FSMOUNT_POWER2) 
  
 /*
**__________________________________________________________________________
*/
/**
*  Init of the load balancing group associated with the exportd
   (used by rozofsmount only)
   
   @param exportclt: data structure that describes the remote exportd
   @param prog: export program name
   @param vers: exportd program version
   @param port_num: tcp port of the exportd (0 for dynamic port )
   @param supervision_callback: supervision callback (NULL if none)
   
   @retval 0 on success
   @retval < 0 on error (see errno for details)
*/
int export_lbg_initialize(exportclt_t *exportclt ,unsigned long prog,
        		  unsigned long vers,uint32_t port_num,
			  af_stream_poll_CBK_t supervision_callback);
        
int storcli_get_storcli_idx_from_fid(fid_t fid);
       
//int storaged_lbg_initialize(mstorage_t *s);
int storcli_lbg_initialize(exportclt_t *exportclt ,char *owner,uint16_t rozofsmount_instance,int first_instance,int nb_instances);
int storcli_lbg_get_lbg_from_fid(fid_t fid);
int storcli_lbg_get_load_balancing_reference(int idx);
 #endif
