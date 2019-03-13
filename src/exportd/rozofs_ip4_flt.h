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

#ifndef _ROZOFS_IP4_FLT_H
#define _ROZOFS_IP4_FLT_H


#ifdef __cplusplus
extern "C" {
#endif /*__cplusplus*/


#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <limits.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/list.h>
#include <rozofs/common/xmalloc.h>

#define ROZOFS_IP4_FORBID     0
#define ROZOFS_IP4_ALLOW      1


#define IP2FOURU8(ip) (ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF
#define IP4MASK(len) (0xFFFFFFFF<<(32-len))

/*
** IPv4 filtering subtree
*/
typedef struct _rozofs_ip4_subnet_t {
  /*
  ** Subnet description
  */
  uint32_t                        ip;           /* Subnet IP address */ 
  uint8_t                         len;          /* Subnet mask */
  uint8_t                         allow  ;      /* Whether subnet is allowed or forbiden */
  /*
  ** Only on upper treee for subnet allocation
  */  
  uint8_t                         size;         /* Size of the upper tree for subnet allocation */
  uint8_t                         next;         /* Next subnet to allocate */
  /*
  ** To reach other brother or son subnets
  */
  list_t                          sons;         /* List of sons */  
  list_t                          brothers;     /* List of brothers */
} rozofs_ip4_subnet_t;

/*
**__________________________________________
** Rule to string translation
**
** @param    rule   The rule to translate
**
** @retval   the string of the rule
*/
static inline char * rozofs_ip4_filter_rule2string(int rule) {  
  if (rule) return "allow";
  return "forbid";
}

/*
**__________________________________________
** Initialize a subnet
**
** @param    p      The subnet context to initialize
** @param    IP     IP address of the subnet
** @param    len    len of the subnet (32..0)
** @param    allow  wher=ther the subnet is allowed of forbiden
**
*/
static inline void rozofs_ip4_filter_reset(rozofs_ip4_subnet_t * p, uint32_t ip, uint32_t len, int allow) {  
  p->ip  = ip;
  p->len = len;
  if (allow) {
    p->allow = ROZOFS_IP4_ALLOW;
  }
  else {
    p->allow = ROZOFS_IP4_FORBID;
  }

  p->size = 0;
  p->next = 0;
    
  list_init(&p->sons);
  list_init(&p->brothers);  
}
/*
**__________________________________________
** Compare 2 subnets
**
** @param    subnet1    1rst subnet
** @param    subnet2    2nd subnet
**
** @retval   1  when subnet 1 is father of subnet 2
** @retval   2  when subnet 2 is father of subnet 1
** @retval   0  when subnet 1 and subnet 2 are disjoint
** @retval   -1 when subnet 1 and subnet 2 are equal
*/
static inline int rozofs_subnet_compare(rozofs_ip4_subnet_t * subnet1, rozofs_ip4_subnet_t * subnet2) {

  if (subnet1->len == subnet2->len) {
    if ((subnet1->ip&IP4MASK(subnet1->len))==(subnet2->ip&IP4MASK(subnet1->len))) {
      return -1;
    } 
    return 0;
  }
  
  if (subnet1->len < subnet2->len) {
    if ((subnet1->ip&IP4MASK(subnet1->len))==(subnet2->ip&IP4MASK(subnet1->len))) {
      //info("%u.%u.%u.%u/%d is in %u.%u.%u.%u/%d", IP2FOURU8(subnet2->ip), subnet2->len, IP2FOURU8(subnet1->ip), subnet1->len); 
      return 1;
    }
    //info("%u.%u.%u.%u/%d and %u.%u.%u.%u/%d are disjoint", IP2FOURU8(subnet2->ip), subnet2->len, IP2FOURU8(subnet1->ip), subnet1->len);  
    return 0;
  }    
  
  if ((subnet2->ip&IP4MASK(subnet2->len))==(subnet1->ip&IP4MASK(subnet2->len))) {
    //info("%u.%u.%u.%u/%d is in %u.%u.%u.%u/%d", IP2FOURU8(subnet1->ip), subnet1->len, IP2FOURU8(subnet2->ip), subnet2->len); 
    return 2;
  } 
  //info("%u.%u.%u.%u/%d and %u.%u.%u.%u/%d are disjoint", IP2FOURU8(subnet2->ip), subnet2->len, IP2FOURU8(subnet1->ip), subnet1->len);  
  return 0;  
}
/*
**__________________________________________
** Insert a new subnet in an existing tree
**
** @param    tree   Tree to insert the subnet in
** @param    new    The subnet to insert in the tree
**
** @retval   0 on success, -1 else
*/
static inline int rozofs_ip4_filter_insert(rozofs_ip4_subnet_t * tree, rozofs_ip4_subnet_t * new) {
  rozofs_ip4_subnet_t * pcur;
  list_t              * p;
  list_t              * n;
 
  /*
  ** 1rst subnet inserted in the tree
  */
  if (list_empty(&tree->sons)) {
    //info("%u.%u.%u.%u/%d 1rst son of %u.%u.%u.%u/%d", IP2FOURU8(new->ip), new->len, IP2FOURU8(tree->ip), tree->len);    
    list_push_back(&tree->sons,&new->brothers);
    return 0;
  }
  
  /*
  ** Loop on sons
  */
  list_for_each_forward_safe(p, n, &tree->sons) {
    /*
    ** Get next subnet
    */
    pcur = list_entry(p, rozofs_ip4_subnet_t, brothers);
    /*
    ** Compare with new subnet
    */
    switch(rozofs_subnet_compare(pcur,new)) {
    
      case 0:
	break;
	
      case 1:
	return rozofs_ip4_filter_insert(pcur,new);
	
      case 2:
        list_remove(&pcur->brothers);
	rozofs_ip4_filter_insert(new, pcur);	
	break;
        
      case -1:
        severe("same subnet %u.%u.%u.%u/%d 2 times", IP2FOURU8(new->ip), new->len);
	return -1;
	break;
	
      default:
        break;
    }	
  }

  //info("%u.%u.%u.%u/%d new son of %u.%u.%u.%u/%d", IP2FOURU8(new->ip), new->len, IP2FOURU8(tree->ip), tree->len);    
  list_push_back(&tree->sons,&new->brothers);
  return 0;
}
/*
**__________________________________________
** Check the longuest prefix match subnet 
** and return the associated rule
**
** @param    tree   The starting subtree to search in
** @param    new    The address to look for
**
** @retval   ROZOFS_IP4_FORBID/ROZOFS_IP4_ALLOW
*/
static inline  int rozofs_ip4_filter_check_internal(rozofs_ip4_subnet_t * tree, rozofs_ip4_subnet_t * new) {
  rozofs_ip4_subnet_t * pcur;
  list_t              * p;
  list_t              * n;
 
  /*
  ** Loop on sons
  */
  list_for_each_forward_safe(p, n, &tree->sons) {
    /*
    ** Get next subnet
    */
    pcur = list_entry(p, rozofs_ip4_subnet_t, brothers);
    /*
    ** Compare with new subnet
    */
    switch(rozofs_subnet_compare(pcur,new)) {
    
      case 0:
	break;
	
      case 1:
      case -1:           
	return rozofs_ip4_filter_check_internal(pcur,new);
	
      case 2:
      default:
        break;
    }	
  }
  return tree->allow;
}
/*
**__________________________________________
** Check whether an IP is allowed for a given tree
**
** @param    tree   The tree to check the address against
** @param    IP     IP address to check
**
** @retval   ROZOFS_IP4_FORBID/ROZOFS_IP4_ALLOW
*/
static inline int rozofs_ip4_filter_check(rozofs_ip4_subnet_t * tree, uint32_t ip) { 
  rozofs_ip4_subnet_t new;
  
  /*
  ** When no filter every IP is valid
  */
  if (tree == NULL) return ROZOFS_IP4_ALLOW;

  /*
  ** Initialize a subnet structure
  */
  rozofs_ip4_filter_reset(&new,ip,32,0);
  /*
  ** Check the subnet against the tree
  */
  return rozofs_ip4_filter_check_internal(tree,&new);
}
/*
**__________________________________________
** Allocate a new IPv4 filtering tree
**
** @param    allow      default rule (allow/forbid)
** @param    nb_subnet  Number of subnet in the tree
**
** @retval   the allocated tree
*/
static inline rozofs_ip4_subnet_t * rozofs_ip4_filter_tree_allocate(int allow,int nb_subnet) {
  rozofs_ip4_subnet_t * tree;
  int                   size;
  
  size = sizeof(rozofs_ip4_subnet_t)*(nb_subnet+1);
  tree = xmalloc(size);
  if (tree == NULL) {
    severe("out of memory %d", size);
    return NULL;
  }
  
  /*
  ** Initialize subnet entry
  */
  rozofs_ip4_filter_reset(tree, 0, 0, allow);  
  tree->size = nb_subnet;
  tree->next = 0;

  return tree;
}
/*
**__________________________________________
** Release an IPv4 filtering tree
**
** @param    tree   Tree to release
**
*/
static inline void rozofs_ip4_filter_tree_release(rozofs_ip4_subnet_t * tree) {
  if (tree) {
    xfree(tree);
  }  
}
/*
**__________________________________________
** Add a new subnet in an IPv4 filtering tree
**
** @param    tree   Tree to insert the subnet in
** @param    IP     IP address of the subnet
** @param    len    len of the subnet [32..1]
** @param    allow  subnet associated rule ROZOFS_IP4_FORBID/ROZOFS_IP4_ALLOW

**
** @retval   0 when subnet insertion succeeds, -1 else
*/
static inline int rozofs_ip4_filter_add_subnet(rozofs_ip4_subnet_t * tree, uint32_t ip, int len, int allow) {
  rozofs_ip4_subnet_t * new;

  /*
  ** Check the tree is not already full
  */    
  if (tree->size <= 0) {
    severe("Out of subnet %u.%u.%u.%u/%d rule %d", IP2FOURU8(ip), len, allow);
    return -1;
  }
  
  /*
  ** Check the subnet length
  */    
  if ((len < 0)||(len>32)) {
    severe("Bad subnet len %u.%u.%u.%u/%d rule %d", IP2FOURU8(ip), len, allow);
    return -1;    
  }   
  
  /* 
  ** Get next free subnet in the tree
  */
  tree->next++;
  new = tree + tree->next;
  tree->size--;

  /*
  ** Initialize subnet entry
  */
  rozofs_ip4_filter_reset(new, ip, len, allow); 

  /*
  ** Insert subnet in the tree
  */
  return rozofs_ip4_filter_insert(tree, new);
}

/*
**_______________________________________________________________________
**
** Get a tree from its name
**
** @param    name   The name of the tree
**
** @retval   the tree or NULL
 */
rozofs_ip4_subnet_t * rozofs_ip4_flt_get_tree(void * e,const char * name) ;
/*
**_______________________________________________________________________
**
** Register debug function
*/
void rozofs_ip4_ftl_init();

#ifdef __cplusplus
}
#endif /*__cplusplus */

#endif
