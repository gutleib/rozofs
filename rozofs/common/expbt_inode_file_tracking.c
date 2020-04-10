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
#include <stddef.h>
#include <stdint.h>
#include <errno.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>        /* For mode constants */
#include <fcntl.h>           /* For O_* constants */
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/expbt_inode_file_tracking.h>
#include <rozofs/common/log.h>
#include <rozofs/core/uma_dbg_api.h>

#if 0
/*
** TO BE REMOVED
*/
//#include <rozofs/rozofs.h>
#define severe printf
#define fatal printf
typedef enum
{
   ROZOFS_EXTATTR = 0, /**< extended attributes */
   ROZOFS_TRASH,   /**< pending trash */
   ROZOFS_REG,  /**< regular file & symbolic links */
   ROZOFS_DIR,     /**< directory    */
   ROZOFS_SLNK,    /**< name of symbolic link */
   ROZOFS_DIR_FID,     /**< directory rferenced by its fid  */
   ROZOFS_RECYCLE,     /**< recycle directory  */
   ROZOFS_REG_S_MOVER,     /**< FID of a regular file under the control of the file mover: source       */
   ROZOFS_REG_D_MOVER,     /**< FID of a regular file under the control of the file mover: destination  */

   ROZOFS_MAXATTR
} export_attr_type_e;
#define EXPGW_EID_MAX_IDX 1024

typedef union
{
   uint64_t fid[2];   /**<   */
   struct {
     uint64_t  vers:4;        /**< fid version */
     uint64_t  mover_idx:8;   /**< fid index: needed by mover feature: rozo_rebalancing */
     uint64_t  fid_high:33;   /**< highest part of the fid: not used */
     uint64_t  recycle_cpt:2;   /**< recycle counter */
     uint64_t  opcode:4;      /**< opcode used for metadata log */
     uint64_t  exp_id:3;      /**< exportd identifier: must remain unchanged for a given server */
     uint64_t  eid:10;        /**< export identifier */     
     uint64_t  usr_id:8;     /**< usr defined value-> for exportd;it is the slice   */
     uint64_t  file_id:40;    /**< bitmap file index within the slice                */
     uint64_t  idx:11;     /**< inode relative to the bitmap file index           */
     uint64_t  key:4;     /**< inode relative to the bitmap file index           */
     uint64_t  del:1;     /**< asserted to 1 when the i-node has a pending deletion      */
   } s;
   struct {
     uint64_t  vers:4;        /**< fid version */
     uint64_t  mover_idx:8;   /**< fid index: needed by mover feature: rozo_rebalancing */
     uint64_t  fid_high:33;   /**< highest part of the fid: not used */
     uint64_t  recycle_cpt:2;   /**< recycle counter */
     uint64_t  opcode:4;      /**< opcode used for metadata log */
     uint64_t  exp_id:3;      /**< exportd identifier: must remain unchanged for a given server */
     uint64_t  eid:10;        /**< export identifier */     
     uint64_t  usr_id:8;     /**< usr defined value-> for exportd;it is the slice   */
     uint64_t  file_id:40;    /**< bitmap file index within the slice                */
     uint64_t  idx:11;     /**< inode relative to the bitmap file index           */
     uint64_t  key:4;     /**< inode relative to the bitmap file index           */
     uint64_t  del:1;     /**< asserted to 1 when the i-node has a pending deletion      */
   } meta;
} rozofs_inode_t;
/*
** TO BE REMOVED
*/
#endif

rozo_trk_file_eid_shm_t *expbt_shared_mem_reg_attr_tb[EXPGW_EID_MAX_IDX];
rozo_trk_file_eid_shm_t *expbt_shared_mem_dir_attr_tb[EXPGW_EID_MAX_IDX];
int expbt_track_inode_init_done = 0;

/*
**______________________________________________________________________________
*/
int expbt_shared_mem_init()
{
   memset(expbt_shared_mem_reg_attr_tb,0,sizeof(expbt_shared_mem_reg_attr_tb));
   memset(expbt_shared_mem_dir_attr_tb,0,sizeof(expbt_shared_mem_dir_attr_tb));
   expbt_track_inode_init_done = 1;
   return 0;
}

/*
**______________________________________________________________________________
*/
/**
  check the existence of a shared memory used from file tracking changes
  
  @param eid: export identifier
  @param type:ROZOFS_REG or ROZOFS_DIR

  
  @retval 1 on success
  @retval 0 on error (see errno for details)
*/

int expb_is_shared_memory_entry_exist(int eid,int type)
{
  rozo_trk_file_eid_shm_t *p;
  
  if (expbt_track_inode_init_done ==0) {
     expbt_shared_mem_init();
     errno = ENOENT;
     return 0;
  }
  if (eid >= EXPGW_EID_MAX_IDX)
  {
     errno = ERANGE;
     return 0;
  }
  if ((type != ROZOFS_REG) && (type != ROZOFS_DIR))
  {
    errno = EINVAL;
    return 0;
  }  
  if (type == ROZOFS_REG)
  {    
    p = expbt_shared_mem_reg_attr_tb[eid];
  }
  else
  {
    p = expbt_shared_mem_dir_attr_tb[eid];
  }
  if (p == NULL) 
  {
    errno = ENOENT;
    return 0;
  }
  return 1;
}
/*
**______________________________________________________________________________
*/
/**
  Open a shared memory for tracking the file tracking changes
  
  @param eid: export identifier
  @param type:ROZOFS_REG or ROZOFS_DIR
  @param create: assert to 1 if shared memory must be created
  
  @retval 0 on success
  @retval -1 on error (see errno for details)
*/

int expb_open_shared_memory(int eid,int type,int create)
{
  char filename[64];
  int flags = O_RDWR;
  int ret;
  struct stat sb;   
  rozo_trk_file_eid_shm_t *p;
  int errcode;
  
  /*
  ** Init of the data structure if not yet done
  */
  if (expbt_track_inode_init_done ==0) expbt_shared_mem_init();
     
  if (eid >= EXPGW_EID_MAX_IDX)
  {
     errno = ERANGE;
     return -1;
  }
  if ((type != ROZOFS_REG) && (type != ROZOFS_DIR))
  {
    errno = EINVAL;
    return -1;
  }
  if (type == ROZOFS_REG)
  {    
    p = expbt_shared_mem_reg_attr_tb[eid];
  }
  else
  {
    p = expbt_shared_mem_dir_attr_tb[eid];
  }
  /*
  ** the entry exist so just exit 
  */
  if (p != NULL) return 0;
  /*
  ** allocate the entry
  */
  p = malloc(sizeof(rozo_trk_file_eid_shm_t));
  if (p == NULL)
  {
    /*
    ** Out of memory
    */
    errno = ENOMEM;
    return -1;
  }
  memset(p,0,sizeof(rozo_trk_file_eid_shm_t));
  p->eid = eid;
  p->type = type;
  p->length = sizeof(rozo_trk_file_t);
  /*
  ** now open the shared memory
  */
  if (type == ROZOFS_REG)
  {
    
    sprintf(filename,"/export_shm_reg_attr_%d",eid);
  }
  else
  {
    sprintf(filename,"/export_shm_dir_attr_%d",eid);    
  }
  /*
  ** allocate the entry
  */
  
  if (create)
  {
     ret =shm_unlink(filename);
     if (ret < 0)
     {
       if(errno != ENOENT)
       {
         severe("error of shared memory creation:%s:%s\n",filename,strerror(errno));
	 errcode = errno;
	 free(p);
	 errno = errcode;
	 return -1;
       }
     }
  }
   if (create) flags = flags | O_CREAT;
   ret = shm_open(filename,flags,0644);     
   if (ret < 0)
   {
     severe("error of shared memory creation:%s:%s\n",filename,strerror(errno));
     errcode = errno;
     free(p);
     errno = errcode;          
     return -1;
   }
   if (create)
   {
     if (ftruncate (ret,p->length) == -1) {
      severe ("ftruncate failure for %s : %s",filename,strerror(errno));
      errcode = errno;      
      close(ret);
      free(p);
      errno = errcode;
      return -1;
     } 
   }
   else
   {
     if (fstat (ret, &sb) == -1) {
       severe ("fstat failure for %s : %s",filename,strerror(errno));
       errcode = errno;
       close(ret);
       free(p);
       errno= errcode;
       return -1;
     }
     if (sb.st_size !=  p->length)
     {
       /*
       ** not the good size
       */
       severe("Bad shared memory size (%s): %d expected %d",filename,(int)sb.st_size,(int)p->length);
       close(ret);
       free(p);
       errno = EINVAL;
       return -1;
     }
   }
   p->addr = mmap (0, p->length, PROT_READ|PROT_WRITE, MAP_SHARED, ret, 0);
   if (p->addr == MAP_FAILED) {
           severe ("map failure for %s : %s",filename,strerror(errno));
	   close(ret);
	   free(p);
	   errno = EINVAL;
           return -1;
   }
   if (create)  memset(p->addr,0,p->length); 
   /*
   ** fill the entry with the context
   */ 
   if (type == ROZOFS_REG)
   {

     expbt_shared_mem_reg_attr_tb[eid]= p;
   }
   else
   {
     expbt_shared_mem_dir_attr_tb[eid]=p; 
   }    
   return 0;

}

/*
**______________________________________________________________________________
*/
/**
  Track the change that happen for an inode within its tracking file
  
  @param inode: pointer to the inode
  
  @retval none
*/
void expbt_track_inode_update(rozofs_inode_t *inode)
{

  int eid;
  int type;
  rozo_trk_file_eid_shm_t *p;
  uint32_t counter_idx;
  uint64_t file_id;
  uint32_t inode_idx;
  uint32_t file_id_idx;
  rozo_trk_file_t *count_p;
    
  eid = inode->s.eid;
  type =inode->s.key;
  file_id  = inode->s.file_id;
  inode_idx = inode->s.idx;


  
  if (expbt_track_inode_init_done ==0) return;
  
  if ((type != ROZOFS_REG) && (type != ROZOFS_DIR))
  {
    return;
  }
  if (type == ROZOFS_REG)
  {
    
    p = expbt_shared_mem_reg_attr_tb[eid];
    counter_idx = (uint32_t)(file_id %(ROZO_NB_ENTRIES_CHG_COUNT*ROZO_NB_TRK_FILE_ENTRIES));
    counter_idx = counter_idx + (inode->s.usr_id*ROZO_NB_ENTRIES_CHG_COUNT*ROZO_NB_TRK_FILE_ENTRIES);
  }
  else
  {
    p = expbt_shared_mem_dir_attr_tb[eid];
    counter_idx = inode_idx/ROZO_DIR_ATTR_NB_INODE_ENTRIES_PER_COUNTER;
    file_id_idx = file_id %(ROZO_NB_ENTRIES_CHG_COUNT*ROZO_NB_TRK_FILE_ENTRIES/ROZO_DIR_ATTR_NB_INODE_ENTRIES_PER_COUNTER);
    counter_idx = counter_idx+file_id_idx + (inode->s.usr_id*ROZO_NB_ENTRIES_CHG_COUNT*ROZO_NB_TRK_FILE_ENTRIES);    
  }
  if (p == NULL)
  {
    /*
    ** there is no entry for that eid
    */
    return;
  }
  count_p = (rozo_trk_file_t *)p->addr;
  if (count_p == NULL) return;
  
  count_p->change_count[counter_idx]++;
}
/*
**______________________________________________________________________________
*/
/**
  Track the change that happen for an inode within its tracking file
  
  @param inode: pointer to the inode
  @param all: when asserted does not care about the range that is applied on inode index (mainly for directory)
  
  @retval counter_value: current value of the counter (0 if it does not exits
*/

uint32_t expbt_track_inode_get_counter(rozofs_inode_t *inode,int all)
{

  int eid;
  int type;
  rozo_trk_file_eid_shm_t *p;
  uint32_t counter_idx;
  uint64_t file_id;
  uint32_t inode_idx;
  uint32_t file_id_idx;
  rozo_trk_file_t *count_p;
  uint32_t counter_value = 0;
  int i;
    
  eid = inode->s.eid;
  type =inode->s.key;
  file_id  = inode->s.file_id;
  inode_idx = inode->s.idx;


  if (expbt_track_inode_init_done ==0) return counter_value;
  
  if ((type != ROZOFS_REG) && (type != ROZOFS_DIR))
  {
    return counter_value;
  }
  if (type == ROZOFS_REG)
  {
    
    p = expbt_shared_mem_reg_attr_tb[eid];
    counter_idx = (uint32_t)(file_id %(ROZO_NB_ENTRIES_CHG_COUNT*ROZO_NB_TRK_FILE_ENTRIES));
    counter_idx = counter_idx + (inode->s.usr_id*ROZO_NB_ENTRIES_CHG_COUNT*ROZO_NB_TRK_FILE_ENTRIES);
  }
  else
  {
    p = expbt_shared_mem_dir_attr_tb[eid];
    if (all)
    {
      counter_idx = 0;
    }
    else
    {
      counter_idx = inode_idx/ROZO_DIR_ATTR_NB_INODE_ENTRIES_PER_COUNTER;    
    }
    file_id_idx = file_id %(ROZO_NB_ENTRIES_CHG_COUNT*ROZO_NB_TRK_FILE_ENTRIES/ROZO_DIR_ATTR_NB_INODE_ENTRIES_PER_COUNTER);
    counter_idx = counter_idx+file_id_idx + (inode->s.usr_id*ROZO_NB_ENTRIES_CHG_COUNT*ROZO_NB_TRK_FILE_ENTRIES);    
  }
  if (p == NULL)
  {
    /*
    ** there is no entry for that eid
    */
    return counter_value;
  }
  count_p = (rozo_trk_file_t *)p->addr;
  if (count_p == NULL) return counter_value;
  
  if (type == ROZOFS_REG)
  {  
    return count_p->change_count[counter_idx];
  }

  if (all == 0) return count_p->change_count[counter_idx];
  for (i = 0; i < ROZO_DIR_ATTR_NB_INODE_ENTRIES_PER_COUNTER; i++)
  {
    counter_value +=count_p->change_count[counter_idx+i];
  }
  return counter_value;
}

/*
**______________________________________________________________________________
*/
/**
  Track the change that happen for an inode within its tracking file
  
  @param inode: pointer to the inode
  
  @retval counter_value: current value of the counter (0 if it does not exits
*/

int expbt_track_inode_get_counter_for_slice(int eid,uint32_t slice,int type,uint32_t *counter_value_p,uint32_t *file_count_p)
{

  rozo_trk_file_eid_shm_t *p;
  uint32_t counter_idx;
  uint32_t counter_value = 0;
  uint32_t file_count = 0;  
  int prev_file_idx;
  int curr_file_idx;
  int divisor;
  int i;
  rozo_trk_file_t *count_p;
   
  *counter_value_p = 0;
  *file_count_p = 0;

  if (expbt_track_inode_init_done ==0) 
  {
    errno = ENOTSUP;
    return -1;
  }

  if (eid >= EXPGW_EID_MAX_IDX)
  {
     errno = EINVAL;
     return -1;
  }

  if (slice >= ROZO_SLICE_COUNT)
  {
     errno = EINVAL;
     return -1;
  }
  
  if (expbt_track_inode_init_done ==0) return counter_value;
  
  if ((type != ROZOFS_REG) && (type != ROZOFS_DIR))
  {
     errno = EINVAL;
     return -1;
  }
  if (type == ROZOFS_REG)
  {
    
    p = expbt_shared_mem_reg_attr_tb[eid];
    divisor = ROZO_REG_ATTR_NB_INODE_ENTRIES_PER_COUNTER;
  }
  else
  {
   divisor = ROZO_DIR_ATTR_NB_INODE_ENTRIES_PER_COUNTER;
   p = expbt_shared_mem_dir_attr_tb[eid];

  }

  count_p = (rozo_trk_file_t *)p->addr;
  if (count_p == NULL) {
     errno = ENOENT;
     return -1;
  }
  counter_idx = (ROZO_NB_ENTRIES_CHG_COUNT*ROZO_NB_TRK_FILE_ENTRIES)*slice;
  prev_file_idx = -1;
  for (i = 0; i < ROZO_NB_ENTRIES_CHG_COUNT*ROZO_NB_TRK_FILE_ENTRIES; i++)
  {
     curr_file_idx = i/divisor;
     if (count_p->change_count[i+counter_idx] != 0)
     {
        counter_value +=count_p->change_count[i+counter_idx];
	if (prev_file_idx != curr_file_idx) 
	{
	    prev_file_idx = curr_file_idx;
	    file_count++;
	}
     }
  
  }
  *file_count_p = file_count;
  *counter_value_p = counter_value;
  return 0;
}


/*
**______________________________________________________________________________
*/
void expbt_track_inode_display_for_slice_usage(char *err_msg_p,char *pChar)
{

  if (err_msg_p!= NULL)
  {
    pChar +=sprintf(pChar,"error: %s\n",err_msg_p);
  }
  pChar +=sprintf(pChar,"Usage:\n");
  pChar +=sprintf(pChar,"expbt_inode_track eid <eid> slice <slice|all> [dir|file|all]\n");  
  pChar +=sprintf(pChar,"\t- eid <eid>        : export identifier in the range of 1..1023\n");
  pChar +=sprintf(pChar,"\t- slice <slice_id> : either a slice identifier (range [0..255]) or all for all slices\n");
  return;
}

/*
**______________________________________________________________________________
*/
void expbt_track_inode_display_for_slice(char *argv[],char *pChar)
{
  int   eid;
  int i = 0;
  int all_slice = 0;
  int all_type = 0;
  int slice_id = 0;
  int type = 0;
  int cur_type;
  int first_slice,last_slice;
  uint32_t file_count;
  uint32_t counter_value;
  

  /* eid <eid> */
  if (argv[1] == NULL) {
      return expbt_track_inode_display_for_slice_usage(NULL,pChar);         
  }
  if (strcmp(argv[1],"eid")!=0) {
    return expbt_track_inode_display_for_slice_usage("eid keyword expected",pChar);         
  }
  if (argv[2] == NULL) {
      expbt_track_inode_display_for_slice_usage("eid value missing",pChar); 
      return;       
  }
  errno = 0;       
  eid = (int) strtol(argv[2], (char **) NULL, 10);   
  if (errno != 0) {
    return expbt_track_inode_display_for_slice_usage("bad value for eid",pChar);
  } 
 /* slice <all|slice_id> */
  if (argv[3] == NULL) {
      expbt_track_inode_display_for_slice_usage("eid value missing",pChar); 
      return;       
  }
  if (strcmp(argv[3],"slice")!=0) {
    return expbt_track_inode_display_for_slice_usage("slice keyword expected",pChar);         
  }
  if (argv[4] == NULL) {
      expbt_track_inode_display_for_slice_usage("slice parameter missing",pChar); 
      return;       
  }  
  if (strcmp(argv[4],"all")==0) {
    all_slice = 1;    
  }
  else
  {
    errno = 0;       
    slice_id = (int) strtol(argv[4], (char **) NULL, 10);   
    if (errno != 0) {
      return expbt_track_inode_display_for_slice_usage("bad value for slice",pChar);
    }
    if (slice_id >= ROZO_SLICE_COUNT)
    {
      return expbt_track_inode_display_for_slice_usage("slice id is out of range (max is 255)",pChar);    
    }
    
  }
 /* type <file|dir/all> */
  while(1)
  {
    if (argv[5] == NULL) {
      all_type = 1; 
      break;       
    }
    if (strcmp(argv[5],"dir")==0) {
      type = ROZOFS_DIR;
      break;         
    }
    if (strcmp(argv[5],"file")==0) {
      type = ROZOFS_REG;
      break;         
    }
    if (strcmp(argv[5],"all")==0) {
      all_type = 1;
      break;         
    }
    return expbt_track_inode_display_for_slice_usage("unsupported option for type",pChar);       
  }
  if (all_slice)
  {
   first_slice = 0;
   last_slice = ROZO_SLICE_COUNT;
  }
  else
  {
    first_slice = slice_id;
    last_slice = slice_id+1;
  }
  if (all_type)
  {
    cur_type=  ROZOFS_REG;
  }
  else
  {
    cur_type = type;
  } 
  pChar +=sprintf(pChar,"\n%s\n",(cur_type == ROZOFS_REG)?"inode regular files tracking files":"inode directories tracking files");
  pChar +=sprintf(pChar,"+---------+----------------+----------------------------+\n");
  pChar +=sprintf(pChar,"+  slice  + change counter +  number of tracking files  +\n");
  pChar +=sprintf(pChar,"+---------+----------------+----------------------------+\n");
  for (i=  first_slice; i < last_slice; i++)
  {
    file_count = 0;
    counter_value = 0;
    expbt_track_inode_get_counter_for_slice( eid,(uint32_t) i,cur_type, &counter_value,&file_count);
    if (counter_value == 0) continue;
       pChar +=sprintf(pChar,"|  %4.4d   |   %8.8d     |     %8.8d               |\n",(int)i,(int)counter_value,(int)file_count);    
      
  }
  pChar +=sprintf(pChar,"+---------+----------------+----------------------------+\n");
  if (all_type)
  {
    cur_type= ROZOFS_DIR;
    pChar +=sprintf(pChar,"\n%s\n",(cur_type == ROZOFS_REG)?"inode regular files tracking files":"inode directories tracking files");
    pChar +=sprintf(pChar,"+---------+----------------+----------------------------+\n");
    pChar +=sprintf(pChar,"+  slice  + change counter +  number of tracking files  +\n");
    pChar +=sprintf(pChar,"+---------+----------------+----------------------------+\n");
    for (i=  first_slice; i < last_slice; i++)
    {
      file_count = 0;
      counter_value = 0;
      expbt_track_inode_get_counter_for_slice( eid,(uint32_t) i,cur_type, &counter_value,&file_count);
      if (counter_value == 0) continue;
       pChar +=sprintf(pChar,"|  %4.4d   |   %8.8d     |     %8.8d               |\n",(int)i,(int)counter_value,(int)file_count);    

    }  
    pChar +=sprintf(pChar,"+---------+----------------+----------------------------+\n");

  }

}


/*
**______________________________________________________________________________
*/
void show_expbt_track_inode_display_for_slice(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();

    expbt_track_inode_display_for_slice(argv,pChar);

    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());  
} 
