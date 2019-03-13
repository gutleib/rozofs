
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>

#include <rozofs/core/uma_dbg_api.h>

#include "econfig.h"
#include "rozofs_ip4_flt.h"


/*
**_______________________________________________________________________
**
** Get a filter tree from its name
**
** @param    name   The name of the tree
**
** @retval   the tree or NULL
*/
rozofs_ip4_subnet_t * rozofs_ip4_flt_get_tree(void * cfg, const char * name) {
  list_t *p, *q;    
  econfig_t * e = (econfig_t*) cfg;
  
  if (name == NULL) return NULL;
  
  list_for_each_forward_safe(p, q, &e->filters) {
  
    filter_config_t *entry = list_entry(p, filter_config_t, list);
  
    if (strcasecmp(entry->name,name)==0) {
      return  entry->filter_tree;
    }  
  }    
  return NULL;  
}  

/*
**_______________________________________________________________________
**
** Display IP4 filter man
*/
void rozofs_ip4_flt_man(char * pt) {
  pt += rozofs_string_append(pt, "IPv4 filters.\n");
  pt += rozofs_string_append(pt, "   ip4flt              display every filter\n");
  pt += rozofs_string_append(pt, "   ip4flt <flt>        display filter named <flt>\n");
  pt += rozofs_string_append(pt, "   ip4flt <flt> <ip>   checks whether <ip> is allowed by filter <flt>\n");
}
/*
**__________________________________________
** Subnet to sting translation
**
** @param    subnet   The subnet to display
**
** @retval   the end of the formated subnet and rule
*/
static inline char * rozofs_ip4_filter_subnet2string(char * pChar, rozofs_ip4_subnet_t * subnet, int level) { 
  int                   idx;

  for (idx=0; idx<level;idx++) {
    *pChar++ =' ';
    *pChar++ =' ';
    *pChar++ =' ';
  }  
  pChar += sprintf(pChar, "{ \"ip4subnet\":\"%u.%u.%u.%u/%d\", ", IP2FOURU8(subnet->ip), subnet->len);
  pChar += sprintf(pChar, "\"rule\":\"%s\" },\n",rozofs_ip4_filter_rule2string(subnet->allow));  
  return pChar;
}
/*
**_______________________________________________________________________
**
** Display IP4 filter subtree
**
** @param    pChar   Where to format the subtree
** @param    tree    The tree to display
** @param    level   Subtree level
**
** @retval   the end of the formated subnet and rule
*/
char * rozofs_ip4_filter_subtree2string(char * pChar, rozofs_ip4_subnet_t * subtree, int level) {
  rozofs_ip4_subnet_t * pcur;
  list_t              * p;
  list_t              * n;
  
  pChar = rozofs_ip4_filter_subnet2string(pChar, subtree, level);
  
  /*
  ** Loop on sons
  */
  level++;
  list_for_each_forward_safe(p, n, &subtree->sons) {
    /*
    ** Get next subnet
    */
    pcur = list_entry(p, rozofs_ip4_subnet_t, brothers);
    pChar = rozofs_ip4_filter_subtree2string(pChar, pcur, level);
  }
  return pChar;
}
/*
**_______________________________________________________________________
**
** Display IP4 filter tree
**
** @param    tree   The tree to display
**
** @retval   the end of the formated string
*/
char * rozofs_ip4_filter_tree2string(char * pChar, rozofs_ip4_subnet_t * tree) {
  pChar += sprintf(pChar, "      \"subnets\" : [\n"); 
  pChar =  rozofs_ip4_filter_subtree2string(pChar, tree, 3);
  pChar-=2;
  pChar += sprintf(pChar, "\n      ]\n"); 
  return pChar;  
}
/*
**_______________________________________________________________________
**
** IP4 filter diagnostic
*/
void rozofs_ip4_flt_diag(char * argv[], uint32_t tcpRef, void *bufRef) {
  int         ret;
  int         idx;
  uint32_t    scanip[4];
  uint32_t    ip;
  char      * pChar = uma_dbg_get_buffer();
  rozofs_ip4_subnet_t * tree;

  /*
  ** Display every filter tree
  */
  if (argv[1] == NULL) {
    pChar += sprintf(pChar, "{ \"filters\": [\n");  
    list_t *p, *q;    
    list_for_each_forward_safe(p, q, &exportd_config.filters) {
      filter_config_t *entry = list_entry(p, filter_config_t, list);
      pChar += sprintf(pChar, "    {\n");
      pChar += sprintf(pChar, "      \"filter\"  : \"%s\",\n",entry->name);
      pChar = rozofs_ip4_filter_tree2string(pChar, entry->filter_tree);
      pChar += sprintf(pChar, "    },\n");    
    }      
    pChar-=2;
    pChar += sprintf(pChar, "\n  ]\n}\n");  
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;	 
  }  

  /*
  ** Retrieve the given filter tree
  */  
  tree = rozofs_ip4_flt_get_tree(exportd_config_to_show,argv[1]);
  if (tree == NULL) {
    pChar += sprintf(pChar, "No such tree");   
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;	 
  }  

  /*
  ** Display the given filter tree
  */  
  if (argv[2] == NULL) {
    pChar += sprintf(pChar, "    {\n");
    pChar += sprintf(pChar, "      \"filter\"  : \"%s\",\n",argv[1]);
    pChar = rozofs_ip4_filter_tree2string(pChar, tree);
    pChar += sprintf(pChar, "    }\n");    
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;	    
  }
  
  /*
  ** Parse the given IP address
  */    
  ret = sscanf(argv[2],"%u.%u.%u.%u", &scanip[0], &scanip[1], &scanip[2], &scanip[3]);
  if (ret != 4) {
    uma_dbg_send_format(tcpRef,bufRef, 1, "Expecting an IPv4 address and got \"%s\"", argv[2]); 
    return;	    
  }
  for (idx=0; idx<4; idx++) {
    if ((scanip[idx]<0)||(scanip[idx]>255)) {
      uma_dbg_send_format(tcpRef,bufRef, 1, "Bad IPv4 address \"%s\"", argv[2]); 
      return;	    
    }
  }
  ip = (scanip[0]<<24) + (scanip[1]<<16) + (scanip[2]<<8) + scanip[3];
  ret = rozofs_ip4_filter_check(tree,  ip);
  pChar += sprintf(pChar, "%s %s %u.%u.%u.%u\n", argv[1], rozofs_ip4_filter_rule2string(ret), IP2FOURU8(ip)); 
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
  return;	 
}
/*
**_______________________________________________________________________
**
** Register debug function
*/
void rozofs_ip4_ftl_init() {
  uma_dbg_addTopicAndMan("ip4flt", rozofs_ip4_flt_diag, rozofs_ip4_flt_man, 0);
}

//int rozofs_ip4_check_allowed(uint32_t ip) {
//  return rozofs_ip4_filter_check(ptree,  ip);
//}
