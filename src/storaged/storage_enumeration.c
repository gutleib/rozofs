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
#define STORAGE_ENUMERATION_C

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <inttypes.h>
#include <glob.h>
#include <fnmatch.h>
#include <dirent.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <mntent.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/list.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/rozofs_string.h>
#include <rozofs/rpc/mproto.h>
#include "storio_cache.h"
#include "storio_device_mapping.h"
#include "storio_device_mapping.h"
#include "storage_enumeration.h"

int      re_enumration_required=0; 
time_t   storio_last_enumeration_date;


#define STORAGE_MAX_DEV_PER_NODE 255

storage_enumerated_device_t * storage_enumerated_device_tbl[STORAGE_MAX_DEV_PER_NODE]={0};
int                           storage_enumerated_device_nb=0;

int oldlsblk = 1;
/*
 *_______________________________________________________________________
 *
 * Set a label on a device
 *
 * @param pDev       Enumerated device context address
 */
 void rozofs_set_label(storage_enumerated_device_t * pDev) {
   char label[ROZOFS_LABEL_SIZE];
   char cmd[256];
   
   if (pDev->spare) {
     char * pt = label;
     pt += sprintf(label, "%s", ROZOFS_REGULAR_LABEL); 
     if (pDev->spare_mark) {
       pt += sprintf(label, "%s",pDev->spare_mark);  
     } 
   }
   else {
     sprintf(label, ROZOFS_REGULAR_LABEL ,pDev->cid, pDev->sid, pDev->dev);  
   }
     
   if (strcmp(pDev->label,label) == 0) return;
   
   sprintf(cmd, "e2label %s %s", pDev->pName->name, label);
   if (system(cmd)==0) {
     strcpy(pDev->label, label);
   }
}   
/*
 *_______________________________________________________________________
 *
 * Lookup for a device from its UUID
 *
 * @param UUID      Device UUID
 *
 * @retval the device or NULL
 */
storage_enumerated_device_t * rozofs_get_device_from_uuid(uuid_t uuid) {
  storage_enumerated_device_t * pDev;
  int                           idx;
  
  /*
  ** Check whether input UUID is NULL
  */
  for (idx=0; idx<sizeof(uuid_t); idx++) {
    if (uuid[idx] != 0) break;
  }
  if (idx == sizeof(uuid_t)) return NULL;  
  
  /*
  ** Loop on existing devices searching for such UUID
  */
  for (idx=0; idx<storage_enumerated_device_nb; idx++) {    
    pDev = storage_enumerated_device_tbl[idx];
    if (memcmp(uuid,pDev->uuid,sizeof(uuid_t)) == 0) return pDev;
  } 
  return NULL; 
}
/*
 *_______________________________________________________________________
 *
 *  Analyze label of a device
 *
 * @param pDev       Enumerated device context address
 * 
 * @retval    0 when this is a valid RozoFS label , -1 else
 *            spare, cid, sid and dev field are update in pDev
 */
int rozofs_analyze_label(storage_enumerated_device_t * pDev) {
  uint32_t    cid;
  uint32_t    sid;
  uint32_t    dev;
  storage_t * st;
  int         length;  
  int         unknow_label;

  /*
  ** Initialized device ctx fileds 
  */
  pDev->spare = 0;
  pDev->date  = 0;
  pDev->cid   = 0;
  pDev->sid   = 0;
  pDev->dev   = 0;   
  if (pDev->spare_mark) {
    xfree(pDev->spare_mark);
    pDev->spare_mark = NULL;
  } 
    
  if (common_config.mandatory_device_label) {  
    /*
    ** When label is mandatory, no label return -1
    */ 
    unknow_label = -1;
  }
   else {
   /*
    ** When label is not mandatory, no label return 0
    */ 
    unknow_label = 0;
  }
      
  /*
  ** Check for RozoFS regular device label
  */
  if (sscanf(pDev->label, ROZOFS_REGULAR_LABEL, &cid, &sid, &dev) == 3) {

    /*
    ** This is a RozoFS regular device label. 
    ** Check whether CID/SID matches with this storage configuration.
    */
    st = storaged_lookup(cid, sid);
    if (st == NULL) {
      /*
      ** Not my device
      */
      return -1;
    }     

    /*
    ** This is our device
    */
    pDev->cid   = cid;
    pDev->sid   = sid;
    pDev->dev   = dev;
    return 0;
  } 

  /*
  ** Check for spare device label
  */
  if (strncmp(pDev->label,ROZOFS_SPARE_LABEL, strlen(ROZOFS_SPARE_LABEL)) != 0) {
    /*
    ** This is not a RozoFS device
    */
    return unknow_label;
  }     

  /*
  ** This is a spare device.
  */
  pDev->spare = 1; 

  /*
  ** Check for spare mark at the end of the label "RozoFS_spare.<spare mark>"
  */
  length = strlen(pDev->label);
  if (length > strlen(ROZOFS_SPARE_LABEL)) {    
    /*
    ** Read the spare mark
    */
    length -= strlen(ROZOFS_SPARE_LABEL); 
    length++;    
    pDev->spare_mark = xmalloc(length);
    strcpy(pDev->spare_mark, &pDev->label[strlen(ROZOFS_SPARE_LABEL)]);       
  }

  /*
  ** Does the mark fit with the configuration
  */
  st = NULL;
  while ((st = storaged_next(st)) != NULL) {
    /*
    ** This storage requires a mark in the spare file
    */
    if (st->spare_mark != NULL) {
      /*
      ** There is none in this spare file
      */
      if (pDev->spare_mark == NULL) continue;
      /*
      ** There is one but not the expected one
      */
      if (strcmp(st->spare_mark,pDev->spare_mark)!= 0) continue;
      /*
      ** This spare device is usable by this logical storage
      */
      return 0;
    }
    /*
    ** This storage requires no spare mark
    */
    if (pDev->spare_mark == NULL) {
      return 0;
    }
  }     
  /*
  ** The mark file do not fit with this storage configuration
  */
  return -1;
}   
/*
 *_______________________________________________________________________
 *
 * Cleanup an enumerated device
 *
 * @param pDev       Enumerated device context address
 */
static inline void storage_enumerated_device_cleanup(storage_enumerated_device_t * pDev) {
  storage_device_name_t * pName;
  storage_device_name_t * pNext;
  
  /*
  ** Check pointer is not NULL
  */
  if (pDev == NULL) return;

  /*
  ** Free spare mark if any
  */  
  if (pDev->spare_mark != NULL) {
    xfree(pDev->spare_mark);
    pDev->spare_mark = NULL;
  }

  /*
  ** Get 1rst name
  */    
  pName = pDev->pName; 
  pDev->pName = NULL;
  
  /*
  ** Free every name
  */      
  while (pName!= NULL) {
    pNext = pName->next;
    xfree(pName);
    pName = pNext;
  } 
} 
/*
 *_______________________________________________________________________
 *
 * Free an enumerated device context
 *
 * @param pDev       Enumerated device context address
 */
void storage_enumerated_device_free(storage_enumerated_device_t * pDev) {
  /*
  ** Cleanup the device
  */
  storage_enumerated_device_cleanup(pDev);
  /*
  ** Then free it
  */   
  xfree (pDev);
} 
/*
 *_______________________________________________________________________
 *
 * Try to find out a RozoFS mark file in a given directory
 *
 * @param dir       directory to search a mark file in
 * @param pDev      device context to update information from mark file
 *
 *
 * @retval 0 on success -1 when no mark file found
 */
 #define        MAX_MARK_LEN    65
int storage_check_device_mark_file(char * dir, storage_enumerated_device_t * pDev) {
  DIR           * dp = NULL;
  int             ret;
  struct dirent * pep; 
  struct stat     buf;
  char            path[FILENAME_MAX];  
  char          * pChar;
  int             cid, sid, dev;
  int             status = -1;
  int             i;
  char            mark[MAX_MARK_LEN+1];
  int             size;
  
  /*
  ** Check the directory is accessible
  */
  if (access(dir,F_OK)!=0) {
    return -1;
  }
  
  /*
  ** Look for a spare mark file in the directory
  */
  pChar = path;
  pChar += rozofs_string_append(pChar,dir);
  *pChar = '/';
  pChar ++;
  pChar += rozofs_string_append(pChar,STORAGE_DEVICE_SPARE_MARK);    
  if (stat(path,&buf) == 0) {
  
    pDev->spare = 1;
    pDev->date  = buf.st_mtime;
    pDev->cid   = 0;
    pDev->sid   = 0;
    pDev->dev   = 0;
    
    /*
    ** There is one spare mark file.
    ** Check its content
    */
    pDev->spare_mark = NULL;

    /*
    ** Empty mark file
    */
    if (buf.st_size == 0) return 0;
    
    /*
    ** Mark file too big
    */
    if (buf.st_size > MAX_MARK_LEN) return -1;
          
    /*
    ** Open file
    */
    int fd = open(path, ROZOFS_ST_NO_CREATE_FILE_FLAG);
    if (fd < 0) return -1;
    
    /*
    ** Read mark file
    */  
    size = pread(fd, mark, MAX_MARK_LEN, 0);
    close(fd);
      
    /*
    ** Remove carriage return
    */  
    for (i=0; i<size; i++) {
      if (mark[i] == 0) break;
      if (mark[i] == '\n') break;
    } 
    mark[i] = 0;
    pDev->spare_mark = xmalloc(i+1);
    strcpy(pDev->spare_mark, mark);
    return 0;
  }    

  /*
  ** Open the directory to read its content
  */
  if (!(dp = opendir(dir))) {
    severe("opendir(%s) %s",dir,strerror(errno));
    return -1;
  }

  /*
  ** Look for some RozoFS mark file
  */
  while ((pep = readdir(dp)) != NULL) {

    /*
    ** end of directory
    */
    if (pep == NULL) break;

    /*
    ** Check whether this is a mark file
    */
    ret = rozofs_scan_mark_file(pep->d_name, &cid, &sid, &dev);
    if (ret < 0) continue;
    
    /*
    ** Mark file found
    */
    pChar = path;
    pChar += rozofs_string_append(pChar,dir);
    *pChar = '/';
    pChar ++;
    pChar += rozofs_string_append(pChar,pep->d_name);    
   
    /*
    ** Get mark file modification date
    */
    if (stat(path, &buf)<0) continue;
    
    pDev->date  = buf.st_mtime;
    pDev->cid   = cid;
    pDev->sid   = sid;
    pDev->dev   = dev;
    pDev->spare = 0;
    status      = 0;    
    break;
  }  

  /*
  ** Close directory
  */ 
  if (dp) {
    closedir(dp);
    dp = NULL;
  }   
  return status;
} 
/*
 *_______________________________________________________________________
 *
 * Sort the enumerated devices in the following order:
 * - 1rst the lowest cid,
 * - then the lowest sid 
 * - then the lowest device
 * - then the latest mark files
 *
 */
void storage_sort_enumerated_devices() {
  int                           idx1, idx2;
  storage_enumerated_device_t * pDev1;
  storage_enumerated_device_t * pDev2;
  int                           swap;
  
  /*
  ** Order the table
  ** Lower CID, then lower sid, then lower device
  ** then latest date
  */
  
  for (idx1=0; idx1<storage_enumerated_device_nb; idx1++) {
  
    pDev1 = storage_enumerated_device_tbl[idx1];
  
    for (idx2=idx1+1; idx2< storage_enumerated_device_nb; idx2++) {
    
      pDev2 = storage_enumerated_device_tbl[idx2];
      
      swap = 0;
      
      while (1) {
	/* 
	** Compare CID
	*/     	 
        if (pDev2->cid > pDev1->cid) break;
	if (pDev2->cid < pDev1->cid) {
	  swap = 1;
	  break;
	}
	/* 
	** Same CID. Compare SID
	*/     	 
        if (pDev2->sid > pDev1->sid) break;;
        if (pDev2->sid < pDev1->sid) {
	  swap = 1;
	  break;
	}
	/* 
	** Same CID/SID. Compare device
	*/     	 	     	 	
        if (pDev2->dev > pDev1->dev) break;
        if (pDev2->dev < pDev1->dev) {
	  swap = 1;
	  break;
	}     
	/* 
	** Same CID/SID/device. Compare mark file date
	*/     	 	     	 	
        if (pDev2->date > pDev1->date) {
	  swap = 1;
	  break;
	} 		 	
        break;
      }
      
      if (swap) {	
        storage_enumerated_device_tbl[idx1] = pDev2;
        storage_enumerated_device_tbl[idx2] = pDev1;
        pDev1 = pDev2;
      }	
    }
  }
} 
/*
 *_______________________________________________________________________
 *
 * Free the device list
 */
void storage_reset_enumerated_device_tbl() {
  int                           idx;
  storage_enumerated_device_t * pDev;
  
  for (idx=0; idx<storage_enumerated_device_nb; idx++) {
    pDev = storage_enumerated_device_tbl[idx];
    storage_enumerated_device_free(pDev); 
    storage_enumerated_device_tbl[idx] = NULL;
  }
  storage_enumerated_device_nb = 0;
} 
/*
 *_______________________________________________________________________
 *
 * Allocate a new device name structure for a RozoFS device descriptor
 *
 * @pDev      RozoFS device descriptor
 * @mpath     Whether this is a multipath device name
 *
 * @retval The allocated structure or NULL

 */
storage_device_name_t * storage_allocate_new_device_name(storage_enumerated_device_t * pDev, int mpath) {
  storage_device_name_t * pDevName;
  storage_device_name_t * pNextName;

  /*
  ** Allocate a new name sub structure
  */
  pDevName = xmalloc(sizeof(storage_device_name_t));  
  memset(pDevName, 0, sizeof(storage_device_name_t));

  /*
  ** First name
  */
  if (pDev->pName == NULL) {
    pDev->pName = pDevName;
    return pDevName;
  }

  /*
  ** Multipath device is put at the head of the list
  */
  if (mpath) {
    pDevName->next = pDev->pName;
    pDev->pName    = pDevName;
  }  
  /*
  ** Other devices are put at the end of the list
  */
  else {
    pNextName = pDev->pName;
    while(pNextName->next != NULL) pNextName = pNextName->next;
    pNextName->next = pDevName;
  }  
  return pDevName;   
} 
/*__________________________________________________________
** Put the lsblk file name in a given buffer
**__________________________________________________________
*/
static inline char * storage_append_lsblk_filename(char * pt) {
  pt += rozofs_string_append(pt, common_config.device_automount_path);
  pt += rozofs_string_append(pt, "/lsblk");  
  return pt;  
}
/*__________________________________________________________
** Run a lsblk command and put result in file 
** <common_config.device_automount_path>/lsblk
** This function should be called periodically be a thread
** of the storaged. The storio just read the result file 
** without calling lsblk.
**__________________________________________________________
*/
void storage_run_lsblk() {
  char            cmd[512];
  char         *  pt;
    
  /*
  ** Create the working directory to mount the devices on
  */
  if (access(common_config.device_automount_path,F_OK)!=0) {
    if (rozofs_mkpath(common_config.device_automount_path,S_IRUSR | S_IWUSR | S_IXUSR)!=0) {
      severe("rozofs_mkpath(%s) %s", common_config.device_automount_path, strerror(errno));
      return;
    }
  }        
  
  pt = cmd;
  if (oldlsblk) {
    pt += rozofs_string_append(pt,"lsblk -Pno KNAME,FSTYPE,MOUNTPOINT,LABEL,MODEL,UUID,TYPE  | sort -u | awk -F '\"' '{print $2\"#\"$4\"#\"$6\"#\"$8\"#\"$10\"# #\"$12\"#\"$14\"#\";}' > /tmp/lsblk.");
  }
  else {
    pt += rozofs_string_append(pt,"lsblk -Pno KNAME,FSTYPE,MOUNTPOINT,LABEL,MODEL,HCTL,UUID,TYPE -x KNAME | sort -u | awk -F '\"' '{print $2\"#\"$4\"#\"$6\"#\"$8\"#\"$10\"#\"$12\"#\"$14\"#\"$16\"#\";}' > /tmp/lsblk.");
  }  
  pt += rozofs_u32_append(pt,getpid());
  if (system(cmd)==0) {}
  
  /*
  ** Rename the file
  */
  pt = cmd;
  pt += rozofs_string_append(pt, "mv /tmp/lsblk.");
  pt += rozofs_u32_append(pt,getpid());
  pt += rozofs_string_append(pt, " ");
  pt  = storage_append_lsblk_filename(pt);
  
  if (system(cmd)==0) {}
}
/*
 *_______________________________________________________________________
 *
 * Parse a line of the result file of lsblk
 *
 * @param line      Line to parse
 * @param pName     Parsed device name
 * @param pFS       Parsed File system type when device is formated
 * @param pMount    Parsed mount point path when device is mounted
 * @param pLabel    Parsed label when one has been set on the device
 * @param pModel    Parsed disk model
 * @param pModel    Parsed disk HCTL
 * @param uuid      Parsed file system UUID when device is fomated
 * @param mpath     Whether it is a multipath device
 *
 * @retval 0 when pasing is sccessfull, -1 else    
 */
int storage_parse_lsblk_line(char * line,  
                             char ** pName,
                             char ** pFS,
                             char ** pMount,
                             char ** pLabel,
                             char ** pModel,
                             char ** pHCTL,
                             uuid_t  uuid,
                             int   * mpath) {
  char * pt  = line;
  char * pUUID;
  
  *mpath = 0;
  
  /*
  ** 1rst comes the device name
  */
  *pName = pt;
  while ((*pt!=0)&&(*pt!='#')) pt++;
  if (*pt == 0) return -1;
  *pt = 0;
  
  /*
  ** Then FS type
  */
  pt++;
  *pFS = pt; 
  while ((*pt!=0)&&(*pt!='#')) pt++;
  if (*pt == 0) return -1;
  *pt = 0;

  /*
  ** Get the mountpoint name 
  */
  pt++;
  *pMount = pt;
  while ((*pt!=0)&&(*pt!='\n')&&(*pt!='#')) pt++;    
  if (*pt == 0) return -1;
  *pt = 0;

  /*
  ** Get the label
  */    
  pt++;
  *pLabel = pt;   
  while ((*pt!=0)&&(*pt!='#')) pt++;
  if (*pt == 0) return -1;
  *pt = 0;

  /*
  ** Get the model
  */    
  pt++;
  *pModel = pt;   
  while ((*pt!=0)&&(*pt!='#')) pt++;
  if (*pt == 0) return -1;
  *pt = 0;

  /*
  ** Get the HCTL
  */    
  pt++;
  *pHCTL = pt;   
  while ((*pt!=0)&&(*pt!='#')) pt++;
  if (*pt == 0) return -1;
  *pt = 0;

  /*
  ** Get the UUID
  */    
  pt++;
  pUUID = pt;   
  while ((*pt!=0)&&(*pt!='#')) pt++;
  if (*pt == 0) return -1;
  *pt = 0;
  if (rozofs_uuid_parse(pUUID, uuid) < 0) {
     memset(uuid,0,sizeof(uuid_t));
  }  
  /*
  ** Get the TYPE
  */    
  pt++;
  if (*pt == 0) return -1;
  if (strncmp(pt, "mpath",strlen("mpath")) == 0) {
    *mpath = 1;
  }    
  return 0;
}                             
/*
 *_______________________________________________________________________
 *
 * Enumerate every RozoFS device 
 *
 * @param workDir   A directory to use to temporary mount the available 
 *                  devices on it in order to check their content.
 * @param unmount   Whether the devices should all be umounted during the procedure
 */
 
#define CONT  { free(line); line = NULL; continue; }

int storage_enumerate_devices(char * workDir, int unmount) {
  char            cmd[512];
  char            fdevice[128];
  char          * line;
  FILE          * fp=NULL;
  size_t          len;
  storage_t     * st;
  char          * pt;
  int             ret;
  char          * pLabel;
  char          * pModel;
  char          * pName;
  char          * pFS;
  char          * pMount;
  char          * pHCTL;
  uuid_t          uuid;
  int             mpath; // Whether this is a multipath device or not
  storage_device_name_t * pDevName;
  struct stat     buf;

  
  storage_enumerated_device_t * pDev = NULL;  
  storage_enumerated_device_t * pExist = NULL;  

  /*
  ** Reset enumerated device table
  */
  storage_reset_enumerated_device_tbl();
  

  /*
  ** Create the working directory to mount the devices on
  */
  if (access(workDir,F_OK)!=0) {
    if (rozofs_mkpath(workDir,S_IRUSR | S_IWUSR | S_IXUSR)!=0) {
      severe("rozofs_mkpath(%s) %s", workDir, strerror(errno));
      return storage_enumerated_device_nb;
    }
  }
    
  /*
  ** Unmount the working directory, just in case
  */
  storage_umount(workDir);    
      
  /*
  ** Read lsblk file
  */  
  pt = fdevice;
  pt = storage_append_lsblk_filename(pt);
  
  /*
  ** Open result file
  */
  fp = fopen(fdevice,"r");
  if (fp == NULL) {
    severe("fopen(%s) %s", fdevice, strerror(errno)); 
    return storage_enumerated_device_nb;   
  }

  /*
  ** Get enumeration date
  */
  if (stat(fdevice,&buf) == 0) {
    storio_last_enumeration_date = buf.st_mtime;
  }  
  
  /*
  ** Loop on devices to check whether they are
  ** dedicated to some RozoFS device usage
  */
  line = NULL;
  while (getline(&line, &len, fp) != -1) {

    if (storage_parse_lsblk_line(line, &pName, &pFS, &pMount, &pLabel, &pModel, &pHCTL, uuid, &mpath) < 0) {
      CONT;
    }  
    
    /*
    ** Lookup for this UUID
    */
    pExist = rozofs_get_device_from_uuid(uuid);
    if (pExist != NULL) {
    
      /*
      ** Allocate new name structure
      */
      pDevName = storage_allocate_new_device_name(pExist,mpath);      
      /*
      ** Recopy the device name
      */
      sprintf(pDevName->name,"/dev/%s",pName);
      sscanf(pHCTL,"%u:%u:%u:%u",&pDevName->H, &pDevName->C, &pDevName->T, &pDevName->L);        
      
      if (*pMount == 0) {
        CONT;
      }

      pDevName->mounted = 1;

      if (pExist->spare) {
        CONT;
      }
        
      /*
      ** Check CID/SID
      */
      st = storaged_lookup(pExist->cid, pExist->sid);
      if (st == NULL) {
        // Not mine
        CONT;
      }
            
      /*
      ** Check it is mounted at the right place 
      */	          	
      pt = cmd;
      pt += rozofs_string_append(pt,st->root);
      *pt++ = '/';
      pt += rozofs_u32_append(pt,pExist->dev);
      if (strcmp(cmd,pMount)!=0) {
        if (storage_umount(pMount)==0) {
	  pDevName->mounted = 0;
	}
      }	
      CONT;      
    }
     

    /*
    ** Allocate a device information structure or use the laready allocated one
    */
    if (pDev == NULL) {
      pDev = xmalloc(sizeof(storage_enumerated_device_t));
      if (pDev == NULL) {
        severe("Out of memory");
        CONT;
      }
    }
    else {
      /*
      ** Cleanup already allocated structure and use it
      */
      storage_enumerated_device_cleanup(pDev); 
    }  
    memset(pDev,0,sizeof(storage_enumerated_device_t));

    /*
    ** Recopy the label
    */    
    if (strlen(pLabel) < ROZOFS_LABEL_SIZE) {
      strcpy(pDev->label, pLabel);
    }
    
    /*
    ** Check from label whether this device can not be ours
    */
    if (rozofs_analyze_label(pDev) == -1) {
      /*
      ** This device has not our label
      */
      CONT;                            
    }                       

    /*
    ** Recopy UUID
    */    
    memcpy(pDev->uuid, uuid, sizeof(uuid));

    /*
    ** Allocate a new name sub structure
    */
    pDevName = storage_allocate_new_device_name(pDev,mpath);      
    
    /* 
    ** Recopy device name 
    */
    sprintf(pDevName->name,"/dev/%s",pName);
    sscanf(pHCTL,"%u:%u:%u:%u",&pDevName->H, &pDevName->C, &pDevName->T, &pDevName->L);

    /* 
    ** Recopy the FS type
    */
    pDev->ext4 = 0;
    if (strcmp(pFS,"ext4")==0) {
      pDev->ext4 = 1;
    }
    else if (strcmp(pFS,"xfs")!=0){
      CONT;
    }      

    /*
    ** Recopy the model
    */    
    if (strlen(pModel) < ROZOFS_MODEL_SIZE) {
      strcpy(pDev->model, pModel);
    }

    /*
    ** If file system is mounted
    */
    if (*pMount != 0) {
    
      pDevName->mounted = 1;
    
      /*
      ** Read the mark file
      */
      if (storage_check_device_mark_file(pMount, pDev) < 0) {
        CONT;
      }
      
      /*
      ** Spare device
      */
      if (pDev->spare) {
      
        /*
        ** Spare device should not be mounted 
        */
        if (storage_umount(pMount)==0) {   
	  pDevName->mounted = 0;
	}       
        
        /*
        ** Check if someone cares about this spare file in this module        
        */
        st = NULL;
        while ((st = storaged_next(st)) != NULL) {
          /*
          ** This storage requires a mark in the spare file
          */
          if (st->spare_mark != NULL) {
            /*
            ** There is none in this spare file
            */
            if (pDev->spare_mark == NULL) continue;
            /*
            ** There is one but not the expected one
            */
            if (strcmp(st->spare_mark,pDev->spare_mark)!= 0) continue;
            /*
            ** This spare device is interresting for this logical storage
            */
            break;
          }
          /*
          ** This storage requires no mark in the spare file (empty file)
          */
          if (pDev->spare_mark == NULL) break;
        }
	/*
	** Record device in enumeration table when relevent
	*/
        if (st != NULL) {

          /*
          ** Write the correct label on it
          */
          rozofs_set_label(pDev);              
        
  	  storage_enumerated_device_tbl[storage_enumerated_device_nb++] = pDev;
	  pDev = NULL;
        }  
        CONT;
      }	   
      
      /*
      ** Check CID/SID
      */
      st = storaged_lookup(pDev->cid, pDev->sid);
      if (st == NULL) {
        // Not mine
        CONT;
      }
      
      /*
      ** Check it is mounted at the right place 
      */	          	
      pt = cmd;
      pt += rozofs_string_append(pt,st->root);
      *pt++ = '/';
      pt += rozofs_u32_append(pt,pDev->dev);
      if (strcmp(cmd,pMount)!=0) {
        if (storage_umount(pMount)==0) {
	  pDevName->mounted = 0;
	}
      }	
      
      /*
      ** Write the correct label on it
      */
      rozofs_set_label(pDev);      
      
      storage_enumerated_device_tbl[storage_enumerated_device_nb++] = pDev;
      pDev = NULL;     	
      CONT;       
    }
    /*
    ** If file system is not mounted
    */
                    
    /*
    ** Mount the file system on the working directory
    */
    ret = mount(pDevName->name, workDir,  pFS, 
		MS_NOATIME | MS_NODIRATIME | MS_SILENT, 
		common_config.device_automount_option);
    if (ret != 0) {
      if (errno == EBUSY) {
        CONT;
      }
      warning("mount(%s,%s,%s) %s",
              pDevName->name,workDir,common_config.device_automount_option,
	      strerror(errno));
      CONT;
    }
    
    /*
    ** Read the mark file
    */
    ret = storage_check_device_mark_file(workDir, pDev);
  
    /*
    ** unmount directory to remount it at the convenient place
    */
    storage_umount(workDir);
    
    if (ret < 0) {
      CONT;
    }

    /*
    ** Spare device
    */
    if (pDev->spare) {
      
      /*
      ** Check if someone cares about this spare file in this module
      */
      st = NULL;
      while ((st = storaged_next(st)) != NULL) {
        /*
        ** This storage requires a mark in the spare file
        */
        if (st->spare_mark != NULL) {
          /*
          ** There is none in this spare file
          */
          if (pDev->spare_mark == NULL) continue;
          /*
          ** There is one but not the expected one
          */
          if (strcmp(st->spare_mark,pDev->spare_mark)!= 0) continue;
          /*
          ** This spare device is interresting for this logical storage
          */
          break;
        }
        /*
        ** This storage requires no mark in the spare file (empty file)
        */
        if (pDev->spare_mark == NULL) break;
      }
      /*
      ** Record device in enumeration table when relevent
      */
      if (st != NULL) {
        storage_enumerated_device_tbl[storage_enumerated_device_nb++] = pDev;
        pDev = NULL;
      }  
      CONT;
    }	   
      

    /*
    ** Check CID/SID
    */
    st = storaged_lookup(pDev->cid, pDev->sid);
    if (st == NULL) {
      // Not mine
      CONT;
    }
    
    /*
    ** Record device 
    */
    storage_enumerated_device_tbl[storage_enumerated_device_nb++] = pDev;
    pDev = NULL;
    CONT;
  }
  /*
  ** Free allocated structure that has not been used
  */ 
  if (pDev != NULL) {
    storage_enumerated_device_free(pDev);
    pDev = NULL;
  }
  /*
  ** Close device file list
  */   
  if (fp) {
    fclose(fp);
    fp = NULL;
  }  

  /*
  ** Unmount the directory
  */
  storage_umount(workDir);
  
  /*
  ** Remove working directory
  */
  if (rmdir(workDir)==-1) {}
  
  /*
  ** Order the table
  */
  storage_sort_enumerated_devices();
  
  return storage_enumerated_device_nb;
  
}
/**
 *_______________________________________________________________________
 * Get the mount path a device is mounted on
 * 
 * @param dev the device
 *
 * @return: Null when not found, else the mount point path
 */
char * rozofs_get_device_mountpath(const char * dev) {
  struct mntent  mnt_buff;
  char           buff[512];
  struct mntent* mnt_entry = NULL;
  FILE* mnt_file_stream = NULL;

  mnt_file_stream = setmntent("/proc/mounts", "r");
  if (mnt_file_stream == NULL) {
    severe("setmntent failed for file /proc/mounts %s \n", strerror(errno));
    return NULL;
  }

  while ((mnt_entry = getmntent_r(mnt_file_stream,&mnt_buff,buff,512))) {
    if (strcmp(dev, mnt_entry->mnt_fsname) == 0){
      char *  result = xstrdup(mnt_entry->mnt_dir);
      endmntent(mnt_file_stream);
      return result;
    }
  }

  endmntent(mnt_file_stream);
  return NULL;
}
/**
 *_______________________________________________________________________
 * Check a given mount point is actually mounted
 * 
 * @param mount_path The mount point to check
 *
 * @return: 1 when mounted else 0
 */
int rozofs_check_mountpath(const char * mntpoint) {
  struct mntent  mnt_buff;
  char           buff[512];
  struct mntent* mnt_entry = NULL;
  FILE* mnt_file_stream = NULL;
  char mountpoint_path[PATH_MAX];


  if (!realpath(mntpoint, mountpoint_path)) {
    severe("bad mount point %s: %s\n", mntpoint, strerror(errno));
    return 0;
  }

  mnt_file_stream = setmntent("/proc/mounts", "r");
  if (mnt_file_stream == NULL) {
    severe("setmntent failed for file /proc/mounts %s \n", strerror(errno));
    return 0;
  }

  while ((mnt_entry = getmntent_r(mnt_file_stream,&mnt_buff,buff,512))) {
    if (strcmp(mntpoint, mnt_entry->mnt_dir) == 0){
      endmntent(mnt_file_stream);
      return 1;
    }
  }
  endmntent(mnt_file_stream);


  return 0;
}
/*
 *_______________________________________________________________________
 *
 * Display enumerated devices
 */
void storage_show_enumerated_devices_man(char * pt) {
  pt += rozofs_string_append(pt, "usage :\nenumeration\n    Displays RozoFS enumerated devices.\n");
  pt += rozofs_string_append(pt, "enumeration again\n    Forces re-enumeration (storio only).\n");
}
/*
 *_______________________________________________________________________
 *
 * Display enumerated devices
 */
void storage_show_enumerated_devices(char * argv[], uint32_t tcpRef, void *bufRef) {
  char                   * pChar = uma_dbg_get_buffer();
  int                      idx1;
  storage_enumerated_device_t * pDev1;
  int                      first=1;
  storage_device_name_t   * pName;
    
  if ((argv[1] != NULL) && (strcasecmp(argv[1],"again")==0)) {
    re_enumration_required = 1;
    pChar += rozofs_string_append(pChar,"re enumeration is to come\n");
    uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());     
    return;
  }

  pChar += rozofs_string_append(pChar,"{\n");
  pChar += rozofs_string_append(pChar,"  \"enumeration date\" : \"");  
  pChar +=strftime(pChar, 20, "%Y-%m-%d %H:%M:%S", localtime(&storio_last_enumeration_date));
  pChar += rozofs_string_append(pChar,"\",\n"); 
      
  
  pChar += rozofs_string_append(pChar,"  \"enumerated devices\" : [\n");
  
  for (idx1=0; idx1<STORAGE_MAX_DEV_PER_NODE; idx1++) {
  
    pDev1 = storage_enumerated_device_tbl[idx1];
    if (pDev1 == NULL) continue;
    
    if (first) {
      first = 0;
    }
    else {
      pChar += rozofs_string_append(pChar,",\n");
    }    
  
    
     
    pChar += rozofs_string_append(pChar,"    { \"uuid\" : \"");
    pChar += rozofs_fid_append(pChar,pDev1->uuid);    
    pChar += rozofs_string_append(pChar,"\", \"date\" : \"");
    pChar +=strftime(pChar, 20, "%Y-%m-%d %H:%M:%S", localtime(&pDev1->date));    
    
    if (pDev1->ext4) {
      pChar += rozofs_string_append(pChar,"\", \"fs\" : \"ext4\", \"model\" : \"");
    }
    else {
      pChar += rozofs_string_append(pChar,"\", \"fs\" : \"xfs\", \"model\" : \"");
    } 
    if (pDev1->model[0] != 0) {
      pChar += rozofs_string_append(pChar, pDev1->model);
    }     
        
    pChar += rozofs_string_append(pChar, "\",\n      \"label\" : \"");
    if (pDev1->label[0] != 0) {
      pChar += rozofs_string_append(pChar, pDev1->label);
    }     
           
    pChar += rozofs_string_append(pChar, "\", \"role\" : \"");
    if (pDev1->spare) {
      pChar += rozofs_string_append(pChar, "spare\"");      
      pChar += rozofs_string_append(pChar, ", \"mark\" : \"");
      if (pDev1->spare_mark) {
        pChar += rozofs_string_append(pChar, pDev1->spare_mark);
      }
    }
    else {
      pChar += rozofs_u32_append(pChar,pDev1->cid);
      pChar += rozofs_string_append(pChar, "/");
      pChar += rozofs_u32_append(pChar,pDev1->sid);
      pChar += rozofs_string_append(pChar, "/");
      pChar += rozofs_u32_append(pChar,pDev1->dev);   
      pChar += rozofs_string_append(pChar, "\"");
    }  
      
    pName = pDev1->pName;
    pChar += rozofs_string_append(pChar,",\n      \"names\" : [\n");
    while (pName) {
      pChar += rozofs_string_append(pChar,"        { \"name\" : \"");
      pChar += rozofs_string_append(pChar,pName->name);
      pChar += rozofs_string_append(pChar,"\", \"HCTL\" : \"");
      pChar += rozofs_u32_append(pChar,pName->H);   
      pChar += rozofs_string_append(pChar,":");
      pChar += rozofs_u32_append(pChar,pName->C);   
      pChar += rozofs_string_append(pChar,":");
      pChar += rozofs_u32_append(pChar,pName->T);   
      pChar += rozofs_string_append(pChar,":");
      pChar += rozofs_u32_append(pChar,pName->L);   
      
      if (pName->mounted) {
        pChar += rozofs_string_append(pChar,"\", \"mounted\" : \"YES\"}");
      }  
      else {
        pChar += rozofs_string_append(pChar,"\", \"mounted\" : \"NO\"}");
      }
      pName = pName->next;
      if (pName) pChar += rozofs_string_append(pChar,",\n");
      else       pChar += rozofs_string_append(pChar,"\n");
    }  
    pChar += rozofs_string_append(pChar,"      ]");    
    pChar += rozofs_string_append(pChar,"\n    }");
  }
  pChar += rozofs_string_append(pChar,"\n  ]\n}\n");

  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer()); 
} 
/*
 *_______________________________________________________________________
 *
 * Try to mount one device at the convenient path
 *
 * @param pDev   The RozoFS device descriptor to mount
 * @param pName  The device name to mount
 * 
 * @retval 0 on success, -1 on failure
 */
int storage_mount_one_device(storage_enumerated_device_t * pDev,
                             storage_device_name_t       * pName) {
  char                     cmd[512];
  char                   * pt;        
  char                   * pt2;        
  storage_t              * st;
  int                      ret;
  int                      fd;
   
  /*
  ** Lookup for the storage context
  */    
  st = storaged_lookup(pDev->cid, pDev->sid);
  if (st == NULL) return -1; // not mine

  /*
  ** Get the storage mount root path 
  */
  pt = cmd;
  pt += rozofs_string_append(pt,st->root);
  
  /*
  ** Create root directory if no yet done
  */
  if (storage_create_dir(cmd)<0) {
    return -1;
  }
  
  /*
  ** Get the device directory mount path
  */
  *pt++ = '/';
  pt += rozofs_u32_append(pt,pDev->dev);
  pt += rozofs_string_append(pt,"/");
  
  /* 
  ** Device directory does not exist 
  */
  if ((access(cmd, F_OK) != 0)&&(errno==ENOENT)) {
    /*
    ** Create device directory
    */
    if (storage_create_dir(cmd)<0) {
      return -1;
    } 
    /*
    ** Put X file
    */
    char * pt2= pt;
    pt2 += rozofs_string_append(pt2,"X");    
    fd = creat(cmd,0755);
    if (fd < 0) {
      severe("creat(%s,0755) %s", cmd, strerror(errno));   
    }    
    else {
      close(fd);   
    }
    *pt = 0;
  }
    
  /*
  ** Umount this directory, just in case
  */
  storage_umount(cmd);   
  
  /*
  ** Mount the device at this place
  */
  if (pDev->ext4) {
    ret = mount(pName->name, cmd, "ext4", 
		MS_NOATIME | MS_NODIRATIME | MS_SILENT,
		common_config.device_automount_option);
  }
  else {
    ret = mount(pName->name, cmd, "xfs", 
		MS_NOATIME | MS_NODIRATIME | MS_SILENT,
		common_config.device_automount_option);
  }		  
  if (ret != 0) {
    severe("mount(\"%s\",\"%s\",\"%s\") %s",
            pName->name,cmd,common_config.device_automount_option, 
	    strerror(errno));
    return -1;
  }
  
  /*
  ** Check whether this is a replacement by a spare
  */
  if (pDev->spare) {
          
    /* 
    ** Create the CID/SID/DEV mark file
    */
    pt2  = pt;
    pt2 += sprintf(pt2,STORAGE_DEVICE_MARK_FMT, pDev->cid, pDev->sid, pDev->dev);    
    fd = creat(cmd,0755);
    if (fd < 0) {
      severe("creat(%s,0755) %s", cmd, strerror(errno));
      /*
      ** Umount this directory
      */
      storage_umount(cmd); 
      return -1;     
    }    
    close(fd);

    /* 
    ** Create the rebuild required mark file
    */
    pt2  = pt;
    pt2 += rozofs_string_append(pt2,STORAGE_DEVICE_REBUILD_REQUIRED_MARK);
    fd = creat(cmd,0755);
    if (fd < 0) {
      severe("creat(%s,0755) %s", cmd, strerror(errno));   
    }    
    else {
      close(fd);    
    }
    
    /*
    ** Remove spare mark
    */
    rozofs_string_append(pt,STORAGE_DEVICE_SPARE_MARK);
    unlink(cmd);
    
    /*
    ** This is no more a spare device
    */
    pDev->spare = 0;
    * pt = 0;
  }

  /*
  ** Set the disk label
  */
  rozofs_set_label(pDev);
  
  /*
  ** Device is mounted now
  */
  pName->mounted = 1;
  /*
  ** Force rereading major & minor of the mounted device
  */
  st->device_ctx[pDev->dev].major = 0;
  st->device_ctx[pDev->dev].minor = 0;  
  info("%s mounted on %s",pName->name,cmd);    
  return 0;
}  

/*
 *_______________________________________________________________________
 *
 * Try to find out a spare device to repair a failed device
 *
 * @param st   The storage context
 * @param dev  The device number to replace
 * 
 * @retval 0 on success, -1 when no spare found
 */
int storage_replace_with_spare(storage_t * st, int dev) {
  int                           idx;
  storage_enumerated_device_t * pDev;
 
  /*
  ** Look for a spare device
  */
  for (idx=0; idx<storage_enumerated_device_nb; idx++) {
  
    pDev = storage_enumerated_device_tbl[idx];
    if (pDev == NULL)   continue;
    if (pDev->spare==0) continue;
    
    
    if (st->spare_mark == 0) {
      /*
      ** Looking for an empty spare mark file
      */
      if (pDev->spare_mark != NULL) continue; 
    }
    else {
      /*
      ** Looking for a given string in the spare mark file
      */
      if (pDev->spare_mark == NULL) continue;
      if (strcmp(pDev->spare_mark,st->spare_mark) != 0) continue;  
    }

    /*
    ** This is either the empty spare mark file we were expecting
    ** or a spare mark file containing the string we were expecting.
    ** This spare device can be used by this cid/sid.
    */
    
    
    /*
    ** Mount this spare disk instead
    */
    pDev->cid   = st->cid;
    pDev->sid   = st->sid;
    pDev->dev   = dev;
    
    if (storage_mount_one_device(pDev, pDev->pName) < 0) {
      pDev->cid   = 0;
      pDev->sid   = 0;
      pDev->dev   = 0;
      continue;
    }
       
    return 0;
  }  
  return -1;
}     
/*
 *_______________________________________________________________________
 *
 *  Try to mount every enumerated device at the rigth place
 *
 */
int storage_mount_all_enumerated_devices() {
  int                           idx;
  storage_enumerated_device_t * pDev=NULL;
  int                           last_cid=0;
  int                           last_sid=0;
  int                           last_dev=0;
  int                           count = 0;
  storage_t                   * st;
  storage_device_name_t       * pName;
   
  /*
  ** Loop on every enumerated RozoFS devices
  */
  for (idx=0; idx<storage_enumerated_device_nb; idx++) {
  
    pDev = storage_enumerated_device_tbl[idx];
    
    /*
    ** Do not mount spare device
    */
    if (pDev->spare) continue;

    /*
    ** Lookup for the storage context
    */    
    st = storaged_lookup(pDev->cid, pDev->sid);
    if (st == NULL) continue; // not mine
    
    /*
    ** Check whether previous device has the same CID/SID/DEV in 
    ** which case the previous one has the latest mark file and 
    ** so was a spare that has replaced this one.
    ** Note that the dev table is order in cid/sid/device/mask file date order
    */
    if ((last_cid == pDev->cid)
    &&  (last_sid == pDev->sid)
    &&  (last_dev == pDev->dev)) {
      continue;
    }
    
    last_cid = pDev->cid;
    last_sid = pDev->sid;
    last_dev = pDev->dev;

    /* 
    ** Look for the 1rst mounted device name 
    */
    pName = pDev->pName;
    while (pName) {
      if (pName->mounted) break;
      pName = pName->next;
    }

    /* 
    * This RozoFS device is already mounted 
    */
    if (pName) {
      count++;  
      continue;
    } 
           
    /*
    ** This RozoFS device is not yet mounted. Mount the 1rst path
    */
    if (storage_mount_one_device(pDev,pDev->pName)==0) {
      count++;
    }  
  }
  return count;
}     
/*
 *_______________________________________________________________________
 *
 * Check whether lsblk is old and do not support HCTL
 *
 * @retval 1 when old lsblk, 0 else 
 */
int rozofs_check_old_lsblk(void) {
  if (system("lsblk -o HCTL > /dev/null 2>&1 ")==0) {
    oldlsblk = 0;
  }
  else {
    oldlsblk = 1; 
  }   
  return oldlsblk;
}   
/*
 *_______________________________________________________________________
 *
 * Try to mount the devices on the convenient path
 *
 * @param workDir   A directory to use to temporary mount the available 
 *                  devices on in order to check their content.
 * @param storaged  Whether this is the storaged or storio process    
 */
int storage_enumerated_devices_registered = 0;
int storage_process_automount_devices(char * workDir, int storaged) {

  /*
  ** Automount should be configured
  */
  if (!common_config.device_automount) return 0;  

  /*
  ** Register feature to debug if not yet done
  */
  if (storage_enumerated_devices_registered==0) {
     
    storage_enumerated_devices_registered = 1;
    uma_dbg_addTopicAndMan("enumeration", storage_show_enumerated_devices, storage_show_enumerated_devices_man, 0);
     
  }
  
  /*
  ** Enumerate the available RozoFS devices.
  ** Storaged will umount every device, while storio will let
  ** the correctly mounted devices in place.
  */     
  storage_enumerate_devices(workDir, storaged) ;
  
  /*
  ** Mount the devices that have to
  */
  return storage_mount_all_enumerated_devices();  
}
/*
 *_______________________________________________________________________
 *
 * Try to mount the devices on the convenient path
 *
 * @param workDir   A directory to use to temporary mount the available 
 *                  devices on in order to check their content.
 * @param count     Returns the number of devices that have been mounted
 */
void storaged_do_automount_devices(char * workDir, int * count) {
  *count = storage_process_automount_devices(workDir,1);
}
void storio_do_automount_devices(char * workDir, int * count) {
  *count = storage_process_automount_devices(workDir,0);
}
