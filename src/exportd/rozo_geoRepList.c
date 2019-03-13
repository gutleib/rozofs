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
#include <sys/types.h>
#include <dirent.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/mattr.h>
#include <libconfig.h>
#include "export.h"
#include "rozo_inode_lib.h"
#include "exp_cache.h"
#include "econfig.h"
#include "rozofs/common/log.h"

int rozofs_no_site_file=0;


char      * pPath = NULL;
int         resultFileNum=0;
int         countInResultFile=0;
int         FDescr=-1;
char      * configFileName = EXPORTD_DEFAULT_CONFIG;
uint8_t     layout=-1;
lv2_cache_t cache;
int         debug = 0;
int         eid   = -1;
uint64_t    total=0;

#define TRACE if (debug) info


/*
**_______________________________________________________________________
*/
/**
*   Close previous file and create a new result file
   
   @retval -1 on error
   @retval the file descriptor
*/
int rozofs_create_new_result_file() {
  char       result_file[ROZOFS_FILENAME_MAX];
  uint64_t   cr8time;
   
  /*
  ** Close previous result file
  */
  if (FDescr!= -1) {
    close(FDescr);
    FDescr = -1;
  } 

  countInResultFile = 0;
    
  /*
  ** New result file number
  */
  resultFileNum++;

  /*
  ** Remove next+1 file index to make sure the next is the 
  ** last one of a continuous serie
  */
  sprintf(result_file,"%s/geoList.%d", pPath, resultFileNum+1);
  unlink(result_file);

  /*
  ** New result file name
  */
  sprintf(result_file,"%s/geoList.%d", pPath, resultFileNum);

  /*
  ** Open file
  */ 
  FDescr = open(result_file, O_CREAT | O_TRUNC | O_APPEND | O_WRONLY, 0755);
  if (FDescr==-1) {
    severe("open(%s) %s\n",result_file,strerror(errno));
    return -1; 	
  }  
  
  /*
  ** Write time stamp
  */
  cr8time = time(NULL);
  if (write(FDescr,&cr8time,sizeof(cr8time)) != sizeof(cr8time)) {
    severe("write(%d,time) %s",resultFileNum,strerror(errno));
    return -1;
  }  	  
  
  return FDescr;
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
int rozofs_visit(void *exportd,void *inode_attr_p,void *p) {
  geo_fid_entry_t       entry;
  ext_mattr_t         * inode_p = inode_attr_p;

  if (debug) {
     char name[ROZOFS_FILENAME_MAX];
     int i = 0;
     if (inode_p->s.fname.name_type == ROZOFS_FNAME_TYPE_DIRECT) {
       for (i=0;i<inode_p->s.fname.len;i++) name[i] = inode_p->s.fname.name[i];
     }
     name[i] = 0;	
     info("-> %s",name);
  } 
  
  /*
  ** Do not care about symbolic links
  */
  if (S_ISLNK(inode_p->s.attrs.mode)) return 0;

  /*
  ** Create file when not yet created
  */
  if (FDescr<0) rozofs_create_new_result_file();       
	
  /*
  ** Prepare record to write
  */	
  memcpy(entry.fid,inode_p->s.attrs.fid,sizeof(fid_t));
  entry.off_start   = 0;
  entry.off_end     = inode_p->s.attrs.size;
  entry.cid         = inode_p->s.attrs.cid;
  memcpy(entry.sids,inode_p->s.attrs.sids,ROZOFS_SAFE_MAX*sizeof(sid_t));
  entry.layout      = layout;
  
  /*
  ** Append in the file
  */
  if (write(FDescr,&entry,sizeof(entry))!= sizeof(entry)) {
    severe("write %s\n",strerror(errno));
    return 0;
  }
  
  /*
  ** Close and recreate a new file when file is bit enough
  */
  countInResultFile++;
  if (countInResultFile >= GEO_MAX_ENTRIES) {
    rozofs_create_new_result_file();
  }
  
  total++;  
  return 1;
}
/*
 *_______________________________________________________________________
 */
static void usage() {
    printf("Usage: rozo_geoRepList [OPTIONS]\n\n");
    printf("\t-h,--help                    print this message.\n\n");
    printf("\t-e,--export     <eid>        mandatory export id to replicate.\n");
    printf("\t-p,--path       <path>       mandatory path where to create the result files.\n");
    printf("\t-n,--nb         <nbEntries>  mandatory number of entries per file.\n");
    printf("\t-c,--config     <cfgFile>    optionnal configuration file name.\n");
    printf("\t-d,--debug                   display debugging information.\n");
};


/*
 *_______________________________________________________________________
 */
int main(int argc, char *argv[]) {
  int c;
  void * rozofs_export_p;
  int    status = EXIT_FAILURE;
  
  static struct option long_options[] = {
      {"help", no_argument, 0, 'h'},
      {"export", required_argument, 0, 'e'},
      {"path", required_argument, 0, 'p'},
      {"config", required_argument, 0, 'c'},
      {"debug", no_argument, 0, 'd'},
      {0, 0, 0, 0}
  };

  openlog("rozo_geoRepList", LOG_PID, LOG_DAEMON);

  while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "hde:c:p:n:", long_options, &option_index);

      if (c == -1)
          break;

      switch (c) {

        case 'h':
          usage();
          exit(EXIT_SUCCESS);
          break;
		  
        case 'c':
          configFileName = optarg;
          break;	
		  		  
        case 'e':
          if (sscanf(optarg,"%d",&eid)!= 1) {
	    severe("Bad -p value %s\n",optarg);	  
            usage();
            exit(EXIT_FAILURE);			  
	  }
          break;
	  		  		  
        case 'p':
          pPath = optarg;
          break;		  		  

        case 'd':
          debug = 1;
          break;
		  			  			  
        case '?':
	  severe("Unexpected option %c",c);	
          usage();
          exit(EXIT_SUCCESS);
          break;
		  
        default:
	  severe("Unexpected option %c",c);
          usage();
          exit(EXIT_FAILURE);
          break;
      }
  }

  if (eid == -1) {
       severe("eid value missing!!");
       usage();
       exit(EXIT_FAILURE);  
  }  
  if (pPath==NULL) {
       severe("path value missing!!");
       usage();
       exit(EXIT_FAILURE);      
  }
  
  /*
  ** Check configuration file 
  */
  if (access(configFileName,R_OK) != 0) {
    if (errno == ENOENT) {
       severe("No configuration file %s.\n",configFileName);
       exit(EXIT_FAILURE);  
    }
    severe("Bad configuration file %s %s",configFileName,strerror(errno));
    exit(EXIT_FAILURE); 
  }
  /*
  ** Read configuration file
  */
  if (econfig_initialize(&exportd_config) != 0) {
       severe("can't initialize exportd config %s.\n",strerror(errno));
       exit(EXIT_FAILURE);  
  }    
  if (econfig_read(&exportd_config, configFileName) != 0) {
        severe("failed to parse configuration file %s %s.\n",
            configFileName,strerror(errno));
       exit(EXIT_FAILURE);  
  }   
  if (econfig_validate(&exportd_config) != 0) {
       severe("inconsistent configuration file %s %s.\n",
            configFileName, strerror(errno));
       exit(EXIT_FAILURE);  
  } 
  
  /*
  ** Loop on export
  */
  export_config_t * econfig;
  list_t          * e;

  list_for_each_forward(e, &exportd_config.exports) {

    econfig = list_entry(e, export_config_t, list);
    if (econfig->eid != eid) continue;

    
    // Retrieve volume to get the layout
    layout = econfig->layout;
    if (layout==-1) {
      severe("Can not find volume %d referenced by eid %d\n",layout,eid);
      goto out;
    }
        
    // This export is the one we are looking for
    rozofs_export_p = rz_inode_lib_init(econfig->root);
    if (rozofs_export_p == NULL){
      severe("RozoFS: error while reading %s\n",econfig->root);
      goto out;  
    }

    /*
    ** init of the lv2 cache
    */
    lv2_cache_initialize(&cache);   
    rz_scan_all_inodes(rozofs_export_p,ROZOFS_REG,1,rozofs_visit,NULL,NULL,NULL);	
    
    status = EXIT_SUCCESS;
    goto out;
  }
  
  severe("No such eid %d\n",eid);

out:  

  if (FDescr>0) {
    close(FDescr);
    FDescr=-1;
  }  

  /*
  ** Check file size 
  */

  
  econfig_release(&exportd_config);
  if (status == 0) {
    info("SUCCESS : %llu FID in %d files",
         (long long unsigned int) total,
         resultFileNum);
  }
  else {
    warning("Failed");
  }	 
  exit(status); 
}
