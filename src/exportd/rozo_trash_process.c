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
#include <stdint.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <uuid/uuid.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <getopt.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/mattr.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/rozofs_service_ports.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/rozofs_core_files.h>
#include "export.h"
#include "rozo_inode_lib.h"
#include "exp_cache.h"
#include "rozo_trash_process.h"
#include "trash_process_config.h"
#include <rozofs/rpc/eclient.h>
#include <rozofs/rpc/rpcclt.h>


#define RZ_FILE_128K  (1024*128)
#define RZ_FILE_1M  (1024*1024)

#define TRACEOUT(fmt, ...) { \
printf(fmt, ##__VA_ARGS__);\
}

lv2_cache_t cache;   /**< pseudo level 2 cache */


   
#define DAEMON_PID_DIRECTORY "/var/run/"

typedef struct _trash_proc_conf {
    char *host;
    char *export;
    char *passwd;
    unsigned buf_size;
    unsigned min_read_size;
    unsigned max_retry;
    int site;
    int conf_site_file;

} trash_proc_conf_t;
trash_proc_conf_t conf;


scan_index_context_t scan_context;   /**< current indexes for inode tracking                             */

    
econfig_t exportd_config;            /**<exportd configuration */
int rozofs_no_site_file = 0;
int rozo_trash_non_blocking_thread_started = 0;  /**< flag that indicates that the non-blocking thread is started  */
rozo_trash_ctx_t rozo_trash_ctx;         /**< trash context of the process */
static pthread_t trash_thread=0;         /**< pid of the nin blocking thread uses for statistics       */
char *debug_buffer = NULL;
void *rozofs_export_p = NULL;           /**< pointer to the export tracking context */
exportclt_t exportclt;                  /**< structure associated to exportd, needed for communication */               
int rozo_trash_scan_start;             /**< when asserted it indicates that the scanning can restart */
int rozo_trash_result_print;             /**< when asserted it indicates that current result must be flush on disk */
/*
** The name of the utility to display on help
*/
char * utility_name=NULL;
uint64_t hour_cpt = 0;
void rozo_trash_read_configuration_file(void);
int rozo_trash_write_result_file();
char *show_trash_process_config_module_global(char*);

time_t trash_config_file_mtime = 0;

/*-----------------------------------------------------------------------------
**
** Get time in micro seconds
**
** @param from  when set compute the delta in micro seconds from this time to now
**              when zero just return current time in usec
**----------------------------------------------------------------------------
*/
uint64_t rozofs_mover_get_us(uint64_t from) {
  struct timeval     timeDay;
  uint64_t           us;
  
  /*
  ** Get current time in us
  */
  gettimeofday(&timeDay,(struct timezone *)0); 
  us = ((unsigned long long)timeDay.tv_sec * 1000000 + timeDay.tv_usec);

  /*
  ** When from is given compute the delta
  */
  if (from) {
    us -= from;
  }  
  
  return us;	
}

char *trash_state_print(int state)
{
   switch (state)
   {
     case ROZOFS_TRASH_STOPPED:
       return "Stopped";
     case ROZOFS_TRASH_RUNNING:
       return "Running";
     case ROZOFS_TRASH_WAITING:
       return "Waiting";
     default :
       return "Unk" ;  
   }
   return "Unk";
}
/*
 *_______________________________________________________________________
 */
char *show_conf_with_buf(char * buf)
{
     char *pChar = buf;
     char buffer2[128];
     int hours;
      rozo_trash_ctx_t *p = &rozo_trash_ctx;
     
     pChar +=sprintf(pChar,"export configuration file  :  %s\n",p->configFileName);
     pChar +=sprintf(pChar,"trashd configuration file  :  %s\n",(p->trashConfigFile==NULL)?"none":p->trashConfigFile);
     pChar +=sprintf(pChar,"verbose mode               :  %s\n",(p->verbose==0)?"Disabled":"Enabled");
     pChar +=sprintf(pChar,"exportd identifier         :  %d\n",p->export_id);
     pChar +=sprintf(pChar,"polling frequenccy (hrs)   :  %d\n",p->frequency);
     pChar +=sprintf(pChar,"delete files older than    :  %d days\n",(int)(p->older_time_sec_config/(3600*24)));
     pChar +=sprintf(pChar,"file deletion rate         :  %d del/s\n",p->deletion_rate);
     if (p->scan_rate < 0)
       pChar +=sprintf(pChar,"file scan     rate         :  none\n");
     else
       pChar +=sprintf(pChar,"file scan     rate         :  %d inode/s\n",p->scan_rate);
     pChar +=sprintf(pChar,"\n");
//     pChar +=sprintf(pChar,"older delay                :  ");
//     pChar = display_delay(pChar,p->older_time_sec_config);

     pChar +=sprintf(pChar,"process state              : %s ",trash_state_print(p->state));
     if  (p->state == ROZOFS_TRASH_WAITING)
     {
        if (hour_cpt >= rozo_trash_ctx.frequency)
	{
	  hours = 0;
	}
	else
	{
	  hours = rozo_trash_ctx.frequency - hour_cpt;	
	}
	pChar += sprintf(pChar,"( restart in %d hour(s))\n",hours);
     
     }
     else
     {
       pChar += sprintf(pChar,"\n");
     }
       
     pChar +=sprintf(pChar,"scanned/deleted files      : %lld/%lld\n",(long long  int)p->current_scanned_file_cpt,
                                                                           (long long  int)p->current_scanned_file_cpt_deleted);
									   
     pChar +=sprintf(pChar,"file deleletion failed     : %lld\n",(long long  int)p->current_scanned_file_fail_cpt);

     pChar +=sprintf(pChar,"total bytes deleted        : %s\n",display_size((long long unsigned int)p->current_scanned_file_deleted_total_bytes,buffer2));
     pChar +=sprintf(pChar,"scanned/deleted directories: %lld/%lld\n",(long long  int)p->current_scanned_dir_cpt,
                                                                           (long long  int)p->current_scanned_dir_cpt_deleted);
     pChar +=sprintf(pChar,"directory deleletion failed: %lld\n",(long long  int)p->current_scanned_dir_fail_cpt);
     return pChar;
}
/*
 *_______________________________________________________________________
 */
char *show_conf_with_buf_json(char * buf)
{
     char *pChar = buf;
     int hours;
      rozo_trash_ctx_t *p = &rozo_trash_ctx;
     pChar +=sprintf(pChar,"{\n");
     pChar +=sprintf(pChar,"\"export configuration file\"  :  \"%s\",\n",p->configFileName);
     pChar +=sprintf(pChar,"\"trashd configuration file\"  :  \"%s\",\n",(p->trashConfigFile==NULL)?"none":p->trashConfigFile);
     pChar +=sprintf(pChar,"\"verbose mode\"               :  \"%s\",\n",(p->verbose==0)?"Disabled":"Enabled");
     pChar +=sprintf(pChar,"\"exportd identifier\"         :  %d,\n",p->export_id);
     pChar +=sprintf(pChar,"\"polling frequenccy (hrs)\"   :  %d,\n",p->frequency);
     pChar +=sprintf(pChar,"\"nb days of retention\"       :  %d,\n",(int)(p->older_time_sec_config/(3600*24)));
     pChar +=sprintf(pChar,"\"file deletion rate\"         :  %d,\n",p->deletion_rate);
     if (p->scan_rate < 0)
       pChar +=sprintf(pChar,"\"file scan     rate\"         :  -1,\n");
     else
       pChar +=sprintf(pChar,"\"file scan     rate\"         :  %d ,\n",p->scan_rate);

     pChar +=sprintf(pChar,"\"process state\"              : \"%s\",\n ",trash_state_print(p->state));
     hours = 0;
     if  (p->state == ROZOFS_TRASH_WAITING)
     {
        if (hour_cpt >= rozo_trash_ctx.frequency)
	{
	  hours = 0;
	}
	else
	{
	  hours = rozo_trash_ctx.frequency - hour_cpt;	
	}     
     }
     pChar +=sprintf(pChar,"\"next run delay (hrs)\"       : %d,\n",hours);
       
     pChar +=sprintf(pChar,"\"scanned files\"              : %lld,\n",(long long  int)p->current_scanned_file_cpt);
     pChar +=sprintf(pChar,"\"deleted files\"              : %lld,\n",(long long  int)p->current_scanned_file_cpt_deleted);					   
     pChar +=sprintf(pChar,"\"file deleletion failed\"     : %lld,\n",(long long  int)p->current_scanned_file_fail_cpt);
     pChar +=sprintf(pChar,"\"total bytes deleted\"        : %lld,\n",(long long unsigned int)p->current_scanned_file_deleted_total_bytes);
     pChar +=sprintf(pChar,"\"scanned directories\"        : %lld,\n",(long long  int)p->current_scanned_dir_cpt);
     pChar +=sprintf(pChar,"\"deleted directories\"        : %lld,\n",(long long  int)p->current_scanned_dir_cpt_deleted);
     pChar +=sprintf(pChar,"\"directory deleletion failed\": %lld\n",(long long  int)p->current_scanned_dir_fail_cpt);
     pChar +=sprintf(pChar,"}\n");
     return pChar;
}




/*
 *_______________________________________________________________________
 */
/** writeback thread
 */
 #define DIRENT_WBCACHE_PTHREAD_FREQUENCY_SEC 1
static void *rozo_trash_periodic_thread(void *v) {

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    // Set the frequency of calls
    int hour_cpt_in_sec = 0;
    uint64_t hour_cpt = 0;
    
    struct timespec ts = {DIRENT_WBCACHE_PTHREAD_FREQUENCY_SEC, 0};
    int config_file_time_count = 0;
    for (;;) {

        nanosleep(&ts, NULL);
	config_file_time_count++;
	if (config_file_time_count == 60)
	{
	  config_file_time_count = 0;
	  rozo_trash_result_print = 1;
	  rozo_trash_read_configuration_file();
	}
	hour_cpt_in_sec++;
	if (hour_cpt_in_sec == 3600)
	{
	  hour_cpt_in_sec = 0;
	  hour_cpt +=1;
	  if (hour_cpt >= rozo_trash_ctx.frequency)
	  {
	     /*
	     ** assert the scanning flag
	     */
	     rozo_trash_scan_start = 1;	  
	     hour_cpt = 0;
	  }
	}
	rozo_trash_ctx.deletion_rate_th = 0;
	rozo_trash_ctx.scan_rate_th = 0;	
    }
    return 0;
}

/*
**________________________________________________________________________
*/
/*
**  Init of the connection towards the selected exportd

   @param : export: pathname of the corresponding exportd
   
   @retval 0 on success
   @retval -1 on error see error for detail
*/
int rozo_trash_export_initialize(char *export)
{
     struct timeval timeout_mproto;
     int retry_count;
     int rozofs_site_number = 0;  /**< site number for geo-replication */
     char host[]="localhost";

     memset(&conf, 0, sizeof (conf));
     conf.max_retry     = 4;
     conf.buf_size      = ROZOFS_BSIZE_BYTES(ROZOFS_BSIZE_MIN)/1024;;
     conf.min_read_size = ROZOFS_BSIZE_BYTES(ROZOFS_BSIZE_MIN)/1024;
     conf.passwd       =  strdup("none");
     conf.export       =  export;

     timeout_mproto.tv_sec = 1;//rozofs_tmr_get(TMR_EXPORT_PROGRAM);
     timeout_mproto.tv_usec = 0;

     memset(&exportclt,0,sizeof(exportclt_t ));
     init_rpcctl_ctx(&exportclt.rpcclt);

     for (retry_count = 15; retry_count > 0; retry_count--) {	
	/* Initiate the connection to the export and get information
	 * about exported filesystem */
	errno = 0;
	if (exportclt_initialize(
        	&exportclt,
        	host,
        	conf.export,
		rozofs_site_number,
        	conf.passwd,
        	conf.buf_size * 1024,
        	conf.min_read_size * 1024,
        	conf.max_retry,
        	timeout_mproto) == 0) break;
         if (errno == EPERM) {
           retry_count = 0;
           break;
         } 
         sleep(2);
	 timeout_mproto.tv_sec++;	
     }
    /*
    ** here should have the listening port of the requested exportd in the client structure
    */
    if (errno == 0) return 0;
    return -1;
}

/*
*______________________________________________________________________
* Create a directory, recursively creating all the directories on the path 
* when they do not exist
*
* @param directory_path   The directory path
* @param mode             The rights
*
* retval 0 on success -1 else
*/
static int mkpath(char * directory_path, mode_t mode) {
  char* p;
  int  isZero=1;
  int  status = -1;
    
  p = directory_path;
  p++; 
  while (*p!=0) {
  
    while((*p!='/')&&(*p!=0)) p++;
    
    if (*p==0) {
      isZero = 1;
    }  
    else {
      isZero = 0;      
      *p = 0;
    }
    
    if (access(directory_path, F_OK) != 0) {
      if (mkdir(directory_path, mode) != 0) {
	severe("mkdir(%s) %s", directory_path, strerror(errno));
        goto out;
      }      
    }
    
    if (isZero==0) {
      *p ='/';
      p++;
    }       
  }
  status = 0;
  
out:
  if (isZero==0) *p ='/';
  return status;
}


/*
*_______________________________________________________________________________
*/
/**
*  Check if the mtime of the file matches the range defined by the user

   @param atime: atime of the file to move
   
   @retval 0 match
   @retval < 0 no match 
*/

int rozo_trash_check_atime(int64_t atime)
{
  int64_t curtime;
  int64_t older_time;
  
  return 0;

   older_time = rozo_trash_ctx.older_time_sec_config;
   curtime=time(NULL);
   /*
   ** check if the file is old enough to be moved
   */
   if (atime > (curtime - older_time))
   {
     return -1;   
   }
   return 0;
}

/*
**_______________________________________________________________________
*/
/**
*  API to get the pathname of the objet: @rozofs_uuid@<FID_parent>/<child_name>

   @param export : pointer to the export structure
   @param inode_attr_p : pointer to the inode attribute
   @param buf: output buffer
   
   @retval buf: pointer to the beginning of the outbuffer
*/
char *rozo_get_full_path(void *exportd,void *inode_p,char *buf,int lenmax)
{
   lv2_entry_t *plv2;
   char name[1024];
   char *pbuf = buf;
   int name_len=0;
   int first=1;
   ext_mattr_t *inode_attr_p = inode_p;
   rozofs_inode_t *inode_val_p;
   
   pbuf +=lenmax;
   
   export_t *e= exportd;
   
   inode_val_p = (rozofs_inode_t*)inode_attr_p->s.pfid;
   if ((inode_val_p->fid[0]==0) && (inode_val_p->fid[1]==0))
   {
      pbuf-=2;
      pbuf[0]='.';   
      pbuf[1]=0;      
   } 
   
   buf[0] = 0;
   first = 1;
   while(1)
   {
      /*
      ** get the name of the directory
      */
      name[0]=0;
      get_fname(e,name,&inode_attr_p->s.fname,inode_attr_p->s.pfid);
      name_len = strlen(name);
      if (name_len == 0) break;
      if (first == 1) {
	name_len+=1;
	first=0;
      }
      pbuf -=name_len;
      memcpy(pbuf,name,name_len);
      pbuf--;
      *pbuf='/';

      if (memcmp(e->rfid,inode_attr_p->s.pfid,sizeof(fid_t))== 0)
      {
	 /*
	 ** this the root
	 */
	 pbuf--;
	 *pbuf='.';
	 return pbuf;
      }
      /*
      ** get the attributes of the parent
      */
      if (!(plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,&cache, inode_attr_p->s.pfid))) {
	break;
      }  
      inode_attr_p=  &plv2->attributes;
    }

    return pbuf;
}

/*
 *_______________________________________________________________________
 */
 /**
*   That function is intended to return the relative path to an object:
    @rozofs_uuid@<FID_parent>/<child_name>
    example:
    @rozofs_uuid@1b4e28ba-2fa1-11d2-883f-0016d3cca427
    
    @param exportd: pointer to exportd data structure
    @param inode_p: pointer to the inode
    @param buf : pointer to the output buffer
    @param lenmax: max length of the output buffer
*/
char *rozo_get_relative_path(void *exportd,void *inode_p,char *buf,int lenmax)
{
   char name[1024];
   char *pbuf = buf;
   char buf_fid[64];
   ext_mattr_t *inode_attr_p = inode_p;
   rozofs_inode_t *inode_val_p;
   
   
   export_t *e= exportd;
   
   inode_val_p = (rozofs_inode_t*)inode_attr_p->s.pfid;
   if ((inode_val_p->fid[0]==0) && (inode_val_p->fid[1]==0))
   {
      pbuf += sprintf(pbuf,"./"); 
   } 
   else
   {
     uuid_unparse(inode_attr_p->s.pfid,buf_fid);
     pbuf += sprintf(pbuf,"./@rozofs_uuid@%s/",buf_fid); 
   } 
   /*
   ** get the object name
   */
   name[0] = 0;     
   get_fname(e,name,&inode_attr_p->s.fname,inode_attr_p->s.pfid);
   if (name[0]== 0)
   {
     uuid_unparse(inode_attr_p->s.attrs.fid,buf_fid);
     pbuf += sprintf(pbuf,"@rozofs_uuid@%s",buf_fid);    
   
   }
   else
   {
     pbuf += sprintf(pbuf,"%s",name);       
   }
   return buf;
}

/*
 *_______________________________________________________________________
 */
 /**
*   Delete a file:

    
    @param exportd: pointer to exportd data structure
    @param inode_p: pointer to the inode

    @retval 0 on success
    @tetval < 0 error, see errno for details
*/
int rozo_trash_delete_file(void *exportd,void *inode_p)
{
   ext_mattr_t *inode_attr_p = inode_p;
   char name[1024];
   int status;
   fid_t fid_ret;
   export_t *e= exportd;
   /*
   ** get the object name
   */
   name[0] = 0;     
   get_fname(e,name,&inode_attr_p->s.fname,inode_attr_p->s.pfid);
   if (name[0]== 0)
   {
     errno = ENOENT;
     return -1;  
   
   }
   /*
   ** put the parent fid as a trash fid
   */
   exp_metadata_inode_del_assert(inode_attr_p->s.pfid);
   rozofs_inode_set_trash(inode_attr_p->s.pfid);
   /*
   ** attempt to delete the file
   */
   status = exportclt_unlink(&exportclt,inode_attr_p->s.pfid,name,&fid_ret);
   if (status < 0)
   {
      warning("Error while deleting file %s : %s",name,strerror(errno));
   }
   return status;
}

/*
 *_______________________________________________________________________
 */
 /**
*   Delete a directory:

    
    @param exportd: pointer to exportd data structure
    @param inode_p: pointer to the inode

    @retval 0 on success
    @tetval < 0 error, see errno for details
*/
int rozo_trash_delete_dir(void *exportd,void *inode_p)
{
   ext_mattr_t *inode_attr_p = inode_p;
   char name[1024];
   int status;
   fid_t fid_ret;
   export_t *e= exportd;
   
   if (rozo_trash_result_print)
   {
      rozo_trash_result_print = 0;   
   }
   /*
   ** get the object name
   */
   name[0] = 0;     
   get_fname(e,name,&inode_attr_p->s.fname,inode_attr_p->s.pfid);
   if (name[0]== 0)
   {
     errno = ENOENT;
     return -1;  
   
   }
   /*
   ** put the parent fid as a trash fid
   */
   exp_metadata_inode_del_assert(inode_attr_p->s.pfid);
   rozofs_inode_set_trash(inode_attr_p->s.pfid);
   /*
   ** attempt to delete the file
   */
   status = exportclt_rmdir(&exportclt,inode_attr_p->s.pfid,name,&fid_ret);
   if (status < 0)
   {
//      warning("Error while deleting directory %s : %s",name,strerror(errno));
   }
   return status;
}
/*
**_______________________________________________________________________
*/
/**
*   RozoFS specific function for visiting

   @param inode_attr_p: pointer to the inode data
   @param exportd : pointer to exporthd data structure
   @param p: always NULL
   
   @retval 0 no match
   @retval 1 match
*/
char bufall[1024];
char *bufout;
char rzofs_path_bid[]="rozofs";
int rozofs_fwd = -1;
int divider;
int blocksize= 4096;
int scanned_current_count = 0;
int all_export_scanned_count = 0;
int score_shrink;

void check_tracking_table(int,int);
/*
**_______________________________________________________________________
*/
int rozofs_visit_reg(void *exportd,void *inode_attr_p,void *p)
{
   int ret= 0;
   ext_mattr_t *inode_p = inode_attr_p;   
   int status;

   if (rozo_trash_result_print)
   {
      rozo_trash_result_print = 0; 
      rozo_trash_write_result_file();      
   }
   
   if (rozo_trash_ctx.scan_rate > 0) rozo_trash_ctx.scan_rate_th++;
   /*
   ** Do not process symlink
   */
   if (!S_ISREG(inode_p->s.attrs.mode)) {
     goto out;
   }   
   rozo_trash_ctx.current_scanned_file_cpt++;   
   if (!exp_metadata_inode_is_del_pending(inode_p->s.attrs.fid))
   {
      rozo_trash_ctx.current_scanned_file_cpt_active++;  
     goto out;
   }
   /*
   ** the file has been deleted
   */
   rozo_trash_ctx.current_scanned_file_cpt_deleted++; 

   /*
   ** Check if the atime matches
   */
   if (rozo_trash_check_atime(inode_p->s.attrs.atime) < 0)
   {
     goto out;
   }
   /*
   ** Delete the file
   */
   status = rozo_trash_delete_file(exportd,inode_attr_p);
   if (status < 0)
   {
     printf("REG FDL %s \n",strerror(errno));
     if (errno!= ENOENT) rozo_trash_ctx.current_scanned_file_fail_cpt++;
   }
   else
   {
     rozo_trash_ctx.current_scanned_file_deleted_total_bytes +=inode_p->s.attrs.size;    
   }
   rozo_trash_ctx.deletion_rate_th++;

out:
  /*
  ** check if we  are in the rate for inode scanning and trash rate
  */
  while ((rozo_trash_ctx.deletion_rate_th >= rozo_trash_ctx.deletion_rate) ||
     ((rozo_trash_ctx.scan_rate >=0) && (rozo_trash_ctx.scan_rate_th >= rozo_trash_ctx.scan_rate)))  
  {  
    usleep(20*1000);
  }
  
  return ret;
}

/*
**_______________________________________________________________________
*/
int rozofs_visit_dir(void *exportd,void *inode_attr_p,void *p)
{
   int ret= 0;
   ext_mattr_t *inode_p = inode_attr_p; 
   int status;  
   
   if (rozo_trash_ctx.scan_rate > 0) rozo_trash_ctx.scan_rate_th++;

   rozo_trash_ctx.current_scanned_dir_cpt++;   
   if (!exp_metadata_inode_is_del_pending(inode_p->s.attrs.fid))
   {
      rozo_trash_ctx.current_scanned_dir_cpt_active++;  
      rozo_trash_write_result_file();      
     goto out;
   }
   /*
   ** the file has been deleted
   */
   rozo_trash_ctx.current_scanned_dir_cpt_deleted++; 
   /*
   ** Check if the atime matches
   */
   if (rozo_trash_check_atime(inode_p->s.attrs.atime) < 0)
   {
     goto out;
   }
   /*
   ** Delete the file
   */
   status = rozo_trash_delete_dir(exportd,inode_attr_p);
   if (status < 0)
   {
     if (errno!= ENOENT) rozo_trash_ctx.current_scanned_dir_fail_cpt++;
   }
   rozo_trash_ctx.deletion_rate_th++;

out:
  /*
  ** check if we  are in the rate for inode scanning and trash rate
  */
  while ((rozo_trash_ctx.deletion_rate_th >= rozo_trash_ctx.deletion_rate) ||
     ((rozo_trash_ctx.scan_rate >=0) && (rozo_trash_ctx.scan_rate_th >= rozo_trash_ctx.scan_rate)))  
  {  
    usleep(20*1000);
  }
  
  return ret;
}
/*
**_______________________________________________________________________
*/
/** Find out the export root path from its eid reading the configuration file
*   
    @param  eid : export identifier
    
    @retval -the root path or null when no such eid
*/
char * get_export_exportd_root_path(uint8_t eid) {
  list_t          * e;
  export_config_t * econfig;

  list_for_each_forward(e, &exportd_config.exports) {

    econfig = list_entry(e, export_config_t, list);
    if (econfig->eid == eid) return econfig->root;   
  }
  return NULL;
}
/*
**_______________________________________________________________________
*/
void check_tracking_table(int idx_table,int idx_user_id)
{

   export_tracking_table_t *trk_tb_p;
   int k;
   exp_trck_top_header_t *tracking_table_p;
   exp_trck_header_memory_t *entry_p;
   export_t *fake_export_p = rozofs_export_p;
   
   if (fake_export_p == NULL) return ;
   trk_tb_p = fake_export_p->trk_tb_p;
   if ( fake_export_p->trk_tb_p == NULL)return ;
   
   
   tracking_table_p = trk_tb_p->tracking_table[idx_table];
   if (tracking_table_p == NULL) return;
   for (k= 0; k < EXP_TRCK_MAX_USER_ID;k++)
   {
   entry_p = tracking_table_p->entry_p[k];
   if (entry_p == NULL) continue;
   if (entry_p == (exp_trck_header_memory_t*)0x2) fatal("FDL bug");
   }
}
/*
*_______________________________________________________________________
*/
/**
*   print the result of the balancing in a buffer

  @param: pchar: pointer to the output buffer
  
  @retval none
*/
void print_resultat_buffer_success(char *p)
{
    char *pchar=p;
    
    pchar = show_conf_with_buf(pchar);
    pchar +=sprintf(pchar,"\n");

}

/*
*_______________________________________________________________________
*/
/**
*   print the result of the balancing in a buffer

  @param: pchar: pointer to the output buffer
  
  @retval none
*/
void print_resultat_buffer_success_json(char *p)
{
    char *pchar=p;
    
    pchar = show_conf_with_buf_json(pchar);
    pchar +=sprintf(pchar,"\n");

}
/*
*_______________________________________________________________________
*/
/**
*  Write the result file

  @retval 0 on success
  @retval -1 on error
*/  
int rozo_trash_write_result_file()
{
  char pathname[1024];
  FILE *fd;
  
  if (rozo_trash_ctx.export_id == -1)
  {
     /*
     ** nothing to write
     */
     return -1;
  }
  sprintf(pathname,"%s/result_trash_%d",TRASH_PATH,rozo_trash_ctx.export_id);
  if ((fd = fopen(pathname,"w")) == NULL)
  {
     return -1;
  }
  print_resultat_buffer_success(debug_buffer);
  fprintf(fd,"%s\n",debug_buffer);
  fclose(fd);
  fd = NULL;

  if (rozo_trash_ctx.export_id == -1)
  {
     /*
     ** nothing to write
     */
     return -1;
  }
  sprintf(pathname,"%s/result_trash_%d.json",TRASH_PATH,rozo_trash_ctx.export_id);
  if ((fd = fopen(pathname,"w")) == NULL)
  {
     return -1;
  }
  print_resultat_buffer_success_json(debug_buffer);
  fprintf(fd,"%s\n",debug_buffer);
  fclose(fd);  
  
  return 0;

}
/*
*_______________________________________________________________________
*/
/**
*  Read configuration file and set trash context accordingly
*
*/  
void rozo_trash_read_configuration_file(void) {
  struct stat  stats;  	
  
  /*
  ** No configuration file
  */  
  

  if (rozo_trash_ctx.trashConfigFile == NULL) return;
  /*
  ** Read mtime and check if file has been modified
  */
  if (stat(rozo_trash_ctx.trashConfigFile,&stats) < 0) {
    severe("fstat(%s) %s",rozo_trash_ctx.trashConfigFile,strerror(errno));
    return;
  }
  if (stats.st_mtime == trash_config_file_mtime) return;
  trash_config_file_mtime = stats.st_mtime;
  
  /*
  ** Read configuration file
  */
  trash_process_config_read(rozo_trash_ctx.trashConfigFile);

  

  rozo_trash_ctx.frequency               = trash_process_config.frequency;
  rozo_trash_ctx.scan_rate               = trash_process_config.scan_rate;
  rozo_trash_ctx.deletion_rate           = trash_process_config.deletion_rate;
  rozo_trash_ctx.older_time_sec_config   = trash_process_config.older*(24*3600);

  /*
  ** Take newer and older when valid
  */
  
  rozo_trash_write_result_file();
  info("cfg file %s reread", rozo_trash_ctx.trashConfigFile);

}
/*
*_______________________________________________________________________
*/
static void usage() {
    printf("\nUsage: rozo_trashd [OPTIONS]\n\n");
    printf("\t-h, --help\t\tprint this message.\n\n");
    printf("\t-e,--exportd <eid>\t\texport identifier \n");
    printf("\nOptional parameters:\n");
    printf("\t-c,--config <filename>\t\texportd configuration file name (when different from %s)\n",rozo_trash_ctx.configFileName);
    printf("\t--olderm <minutes>\t\texclude files that are more recent than delay specified\n");
    printf("\t--older <days>\t\t\texclude files that are more recent than delay specified\n");
    printf("\t--cont \t\t\t\tcontinue after reaching the trashd state\n");
    printf("\t--verbose \t\t\tset the rebalancing in verbose mode\n");
    printf("\t--rate <value> \t\tfile deletion rate msg/s (default:%d msg/s)\n",TRASH_DEFAULT_THROUGPUT);
    printf("\t--scan <value> \t\tmax inode scanning inode/s (default:no rate)\n");
    printf("\t--frequency <value> \t\ttrash period in hours (default:%d hours)\n",TRASH_DEFAULT_FREQ_SEC);
    printf("\t--cfg <fileName> \t\tThe trash configuration file name.\n");
    printf("\n");
    
    char buffer[4096];
    printf("Content of the default configuration file:\n");
    show_trash_process_config_module_global(buffer);
    printf("%s\n",buffer);
};


/*
**_______________________________________________________________________
*/
static    int long_opt_cur;
int main(int argc, char *argv[]) {
    int c;
    char *exportd_root_path=NULL;
    int ret;
    char path[1024];
    int scan_dir;

    
    debug_buffer = malloc(ROZO_BALANCE_DBG_BUF_SIZE);
    /*
    ** create the path toward the directory where result file is stored
    */
    strcpy(path,TRASH_PATH);
    ret = mkpath ((char*)path,S_IRUSR | S_IWUSR | S_IXUSR);
    if (ret < 0)
    {
       printf("Error while creating path towards result file (path:%s):%s\n",TRASH_PATH,strerror(errno));
        exit(EXIT_FAILURE);
    }

    /*
    ** Get utility name and record it for syslog
    */
    utility_name = basename(argv[0]);   
    uma_dbg_record_syslog_name(utility_name);       
    /*
    ** Set a signal handler
    */
    rozofs_signals_declare(utility_name, 1); 
    
    
    memset(&rozo_trash_ctx,0,sizeof(rozo_trash_ctx_t));
    rozo_trash_ctx.frequency         = TRASH_DEFAULT_FREQ_SEC;
    rozo_trash_ctx.export_id         = -1;
    rozo_trash_ctx.configFileName    = EXPORTD_DEFAULT_CONFIG;   
    rozo_trash_ctx.scan_rate         = -1;
    rozo_trash_ctx.older_time_sec_config   = TRASH_DEFAULT_OLDER;  
    rozo_trash_ctx.continue_on_trash_state = 0;
    rozo_trash_ctx.deletion_rate          = TRASH_MAX_SCANNED;
    rozo_trash_ctx.state                  = ROZOFS_TRASH_STOPPED;
    rozo_trash_ctx.trashConfigFile        = TRASHD_DEFAULT_CONFIG;
    static struct option long_options[] = {
        {"help", no_argument, 0, 'h'},
        {"path", required_argument, 0, 'p'},
        {"export", required_argument, 0, 'e'},
        {"config", required_argument, 0, 'c'},
        {"olderm", required_argument, &long_opt_cur, 0},
        {"older", required_argument, &long_opt_cur, 1},
        {"cont", no_argument, &long_opt_cur, 4},
        {"verbose", no_argument, &long_opt_cur, 5},
        {"frequency", no_argument, &long_opt_cur, 6},
        {"rate", required_argument, &long_opt_cur, 7},
        {"scan", required_argument, &long_opt_cur, 8},
        {"mode", required_argument, &long_opt_cur, 9},
        {"cfg", required_argument, &long_opt_cur, 10},

        {0, 0, 0, 0}
    };
    

    while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "hc:p:e:", long_options, &option_index);

      if (c == -1)
          break;

      switch (c) {
      
         case 0:
	   switch (long_options[option_index].val)
	   {
	      case 0:
        	if (sscanf(optarg,"%lld",(long long int *)&rozo_trash_ctx.older_time_sec_config)!= 1) {
                  severe("Bad --olderm value: %s\n",optarg);	  
        	  usage();
        	  exit(EXIT_FAILURE);			  
        	}  
		rozo_trash_ctx.older_time_sec_config *=60;
		break;
	      case 1:
        	if (sscanf(optarg,"%lld",(long long int *)&rozo_trash_ctx.older_time_sec_config)!= 1) {
		  severe("Bad --older value: %s\n",optarg);	  
        	  usage();
        	  exit(EXIT_FAILURE);			  
        	}  
		rozo_trash_ctx.older_time_sec_config *=(24*3600);
		break;

	      case 4:
	        rozo_trash_ctx.continue_on_trash_state = 1;
		break;   
	      case 5:
	        rozo_trash_ctx.verbose = 1;
		break;   
	      case 6:
        	if (sscanf(optarg,"%d",&rozo_trash_ctx.frequency)!= 1) {
		  severe("Bad --frequency value: %s\n",optarg);	  
        	  usage();
        	  exit(EXIT_FAILURE);			  
        	}  
		break;
	      case 7:
        	if (sscanf(optarg,"%d",&rozo_trash_ctx.deletion_rate)!= 1) {
		  severe("Bad --rate value: %s\n",optarg);	  
        	  usage();
        	  exit(EXIT_FAILURE);			  
        	}  
		break;
	      case 8:
        	if (sscanf(optarg,"%d",&rozo_trash_ctx.scan_rate)!= 1) {
		  severe("Bad --scan value: %s\n",optarg);	  
        	  usage();
        	  exit(EXIT_FAILURE);			  
        	}  
		break;

	      case 10:
                rozo_trash_ctx.trashConfigFile = optarg;
                /*
                ** Permanent trashr
                */
	        rozo_trash_ctx.continue_on_trash_state = 1;
		break;                
	      default:
	      break;	   
	   }
	   break;

          case 'h':
              usage();
              exit(EXIT_SUCCESS);
              break;
          case 'e':
            if (sscanf(optarg,"%d",&rozo_trash_ctx.export_id)!= 1) {
              severe("Bad -e value %s\n",optarg);	  
              usage();
              exit(EXIT_FAILURE);			  
            }              
	    break;
          case 'c':
              rozo_trash_ctx.configFileName = optarg;
              break;	
          case '?':
              usage();
              exit(EXIT_SUCCESS);
              break;
          default:
              usage();
              exit(EXIT_FAILURE);
              break;
      }
  }
  
  /*
  ** Case of the permanent mode with a configuration file.
  ** Read the given configuration file
  ** 
  */ 
  rozo_trash_read_configuration_file();
  if (rozo_trash_ctx.export_id == -1)
  {
     printf("export identifier is missing\n");
       usage();
       exit(EXIT_FAILURE);  
  }     

  
  sprintf(path,"%sresult_trash_%d",TRASH_PATH,rozo_trash_ctx.export_id);
  printf("Result will be found in: %s\n",path);
  /*
  ** Read the configuration file
  */
  if (econfig_initialize(&exportd_config) != 0) {
       fatal("can't initialize exportd config %s.\n",strerror(errno));
       exit(EXIT_FAILURE);  
  }    
  if (econfig_read(&exportd_config, rozo_trash_ctx.configFileName) != 0) {
       fatal("failed to parse configuration file %s %s.\n",rozo_trash_ctx.configFileName,strerror(errno));
       exit(EXIT_FAILURE);  
  }  
  /*
  ** Get the root path of the export
  */ 
  exportd_root_path = get_export_exportd_root_path( rozo_trash_ctx.export_id);
  if (exportd_root_path == NULL)
  {
    fatal("eid %d does not exist \n", rozo_trash_ctx.export_id);
    exit(EXIT_FAILURE); 
  }
  /*
  ** open the tcp connections towards the Master export to get the listening port of the selected exportd
  */
  if (rozo_trash_export_initialize(exportd_root_path) < 0)
  {
    fatal("Cannot connect to export %s: %s",exportd_root_path,strerror(errno));  
  } 	
  /*
  ** init of the lv2 cache
  */
  lv2_cache_initialize(&cache);
  rz_set_verbose_mode(0);
  /*
  ** init of the library
  */
  rozofs_export_p = rz_inode_lib_init(exportd_root_path);
  if (rozofs_export_p == NULL)
  {
    fatal("RozoFS: error while reading %s\n",exportd_root_path);
    exit(EXIT_FAILURE);  
  }
  /*
  ** start scanning from beginning 
  */
  rozo_lib_save_index_context(&scan_context);	 
  /*
  ** init of the debug port & instance
  */
  rozo_trash_ctx.instance = 0;
  rozo_trash_ctx.debug_port = rozofs_get_service_port_rebalancing_diag(rozo_trash_ctx.instance);
  /*
  ** start the non-blocking thread, mainly used for rozodiag purpose
  */

  if ((errno = pthread_create(&trash_thread, NULL, (void*) rozo_trash_periodic_thread, &rozo_trash_ctx)) != 0) {
        severe("can't create non blocking thread: %s", strerror(errno));
  }
#if 0
  /*
  ** wait for end of init of the non blocking thread
  */
  loop_count = 0;
  while (rozo_trash_non_blocking_thread_started == 0)
  {
     sleep(1);
     loop_count++;
     if (loop_count > 5) fatal("Non Blocking thread does not answer");
  }  
#endif
  /*
  ** Main loop
  */
   scan_dir = 0;
   rozo_trash_scan_start = 0;
   rozo_trash_ctx.state = ROZOFS_TRASH_RUNNING;
   for(;;)
   {
      /*
      ** Case of the permanent mode with a configuration file.
      ** Read the given configuration file if modified
      ** 
      */    
      rozo_trash_read_configuration_file();
      /*
      ** wait for some delay before looking at the volume statistics
      */
      rozo_trash_write_result_file();    

      /*
      ** Case of the permanent mode with a configuration file.
      ** Read the given configuration file if modified
      ** 
      */  
      rozo_trash_read_configuration_file();

      /*
      ** need to get the inode from the exportd 
      */
      scanned_current_count = 0;
      
      if (scan_dir == 0)
      {
	rz_scan_all_inodes_from_context(rozofs_export_p,ROZOFS_REG,1,rozofs_visit_reg,NULL,NULL,NULL,&scan_context);
	scan_dir = 1;
      }
      else
      {
        rz_scan_all_inodes_from_context(rozofs_export_p,ROZOFS_DIR,1,rozofs_visit_dir,NULL,NULL,NULL,&scan_context);
	scan_dir = 2;      
      }
      /*
      ** free the current and restart
      */
      rozo_lib_export_release();
      rozofs_export_p = rz_inode_lib_init(exportd_root_path);
      if (rozofs_export_p == NULL)
      {
	fatal("RozoFS: error while reading %s\n",exportd_root_path);
	exit(EXIT_FAILURE);  
      }
      /*
      ** start scanning from beginning 
      */
      rozo_lib_reset_index_context(&scan_context);	      
#if 0
      if (rozo_lib_is_scanning_stopped()== 0)
      {	
	/*
	** free the current and restart
	*/
	rozo_lib_export_release();
	rozofs_export_p = rz_inode_lib_init(exportd_root_path);
	if (rozofs_export_p == NULL)
	{
	  fatal("RozoFS: error while reading %s\n",exportd_root_path);
	  exit(EXIT_FAILURE);  
	}
	/*
	** start scanning from beginning 
	*/
	rozo_lib_reset_index_context(&scan_context);	
	if (rozo_trash_ctx.verbose) {
          info("scan export %d from the beginning\n",rozo_trash_ctx.export_id);
        }
      }
      else
      {
        rozo_lib_save_index_context(&scan_context);
	if (rozo_trash_ctx.verbose) 
	{
	   info("user_id %d file_id %llu inode_idx %d\n",scan_context.user_id,( long long unsigned int)scan_context.file_id,scan_context.inode_idx);
        }
      }
#endif
      if (scan_dir == 2)
      {
        if (rozo_trash_ctx.continue_on_trash_state==0) break;
	scan_dir = 0;
	/*
	** print the current results
	*/
	rozo_trash_ctx.state = ROZOFS_TRASH_WAITING;
        print_resultat_buffer_success(debug_buffer);
	rozo_trash_write_result_file();
	/*
	** Check if it is time for a next scan
	*/
	while (rozo_trash_scan_start==0)  
	{  
	  usleep(60*1000000);

	} 
	rozo_trash_scan_start = 0; 
      }    
   }

    rozo_trash_ctx.state = ROZOFS_TRASH_STOPPED;
    print_resultat_buffer_success(debug_buffer);
    printf("%s\n",debug_buffer);
    rozo_trash_write_result_file();    

  exit(EXIT_SUCCESS);  
  return 0;
}
