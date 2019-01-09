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
#include <stdarg.h>

#include "export.h"
#include "rozo_inode_lib.h"
#include "exp_cache.h"
#include "econfig.h"
#include "rozofs/common/log.h"
#include "rozofs/rozofs_srv.h"

#define ALLRIGHTS 0777
static rbs_file_type_e   file_type = rbs_file_type_all;

int rozofs_no_site_file;

char * pDir = NULL;
char working_dir[ROZOFS_FILENAME_MAX];
  
econfig_t exportd_config;

int    parallel=0;
char * configFileName = EXPORTD_DEFAULT_CONFIG;
int    rebuildRef=0;

/*
** Information related to one job list
*/
typedef struct _job_list_info_t {
  // File descriptor of the job list
  int          fd;
  // Sum of the size of the files in the job list
  uint64_t     size;
} job_list_info_t;

/*
** Information related to one logical storage
** and one type of file within nominal/spare
*/
typedef struct _sid_one_info_t {  
  // Count of FID that match
  uint64_t     count;
  // Sum of the size of the files in all job list
  uint64_t     size;
  /*
  ** For each job list
  */
  job_list_info_t   job[MAXIMUM_PARALLEL_REBUILD_PER_SID];
} sid_one_info_t;
/*
** Information related to one logical storage
** i.e the nominal + the spare information
*/
typedef struct _sid_info_t {
  sid_one_info_t nominal;
  sid_one_info_t spare;
} sid_info_t;

/*
** Array of pointers toward logical storage information
** within a given cluster. It is index by the sid number minus 1.
*/
typedef sid_info_t * sid_tbl_t[SID_MAX];

/*
** Array index by the cluster id minus one, pointing to
** an array of pointer toward logical sorage information
*/
sid_tbl_t          * cid_tbl[ROZOFS_CLUSTERS_MAX] = {0};


lv2_cache_t  cache;
uint8_t rozofs_safe = -1;
uint8_t rozofs_forward = -1;
uint8_t layout = -1;
uint8_t bsize;

int     nb_requested_vid = 0;
uint8_t requested_vid[256] = {0};

int debug = 0;

uint64_t total=0;
int      entry_size;

#define TRACE if (debug) info


/*
**_______________________________________________________________________
*/
/**
*   Empty and remove a directory
*
* @param dirname: Name of the directory to cleanup
*/
void clean_dir(char * name) {
  DIR           *dir;
  struct dirent *file;
  char           fname[256];
  struct stat    st;

  
  if (name==NULL) return;
    
  if (stat(name,&st)<0) {
    return;
  }
  
  if (!S_ISDIR(st.st_mode)) {
    if (unlink(name)<0) {
      severe("unlink(%s) %s",name,strerror(errno));
    }  
    return;
  }
      
    
  /*
  ** Open this directory
  */
  dir = opendir(name);
  if (dir == NULL) {
    severe("opendir(%s) %s", name, strerror(errno));
    return;
  } 	  
  /*
  ** Loop on distibution sub directories
  */
  while ((file = readdir(dir)) != NULL) {
  
    if (strcmp(file->d_name,".")==0)  continue;
    if (strcmp(file->d_name,"..")==0) continue;
    
    char * pChar = fname;
    pChar += rozofs_string_append(pChar,name);
    *pChar++ = '/';
    pChar += rozofs_string_append(pChar,file->d_name);
    
    clean_dir(fname);
  }
  closedir(dir); 
  rmdir(name);
  return;
}
/*
**_______________________________________________________________________
**
** Choose the job file to write the next file to rebuild in.
** This must be the job file that has the less total size to rebuild
**
** @param  p    pointer to job list descriptor 
**
** @retval   The index to choose
**
*/
static int get_job_list_index(sid_one_info_t * p) {
  int      job;
  int      bestIdx=-1;
  uint64_t size=0xFFFFFFFFFFFFFFFF;
 
  
  for (job=0; job<parallel; job++) {
  
    if (p->job[job].size >= size) continue;
    
    bestIdx = job;
    size    = p->job[job].size;
  }
  
  return bestIdx;
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

int key_table[] = {ROZOFS_PRIMARY_FID,ROZOFS_MOVER_FID,-1};
static uint64_t master_size ;
static uint64_t strip_size ;
static uint64_t strip_factor ;

int rozofs_visit(void *exportd,void *inode_attr_p,void *p) {
  int i;
  ext_mattr_t *inode_p = inode_attr_p;
  int sid;
  int job;
  sid_tbl_t   * pCid;
  sid_info_t  * pSid;
  char          name[128];
  char        * pname = name;
  sid_one_info_t *pOne;
  int key_index = 0;
  int key;
  int ret;
  cid_t cid;
  sid_t sid_tab[ROZOFS_SAFE_MAX_STORCLI];
  uint64_t   rebuild_size;
  

  if (debug) {
     pname += rozofs_fid_append(pname, inode_p->s.attrs.fid);
  } 
  
  /*
  ** File of zero size need not to be rebuilt
  */
  if (inode_p->s.multi_desc.byte == 0) {
    /*
    ** Regular file without striping
    */
    rebuild_size = inode_p->s.attrs.size;
    master_size  = 0;
    strip_size   = 0;
    strip_factor = 1;
    pname += sprintf(pname," single");
  }  
  else if (inode_p->s.multi_desc.common.master == 1) {
    master_size  = inode_p->s.attrs.size;
    strip_factor = rozofs_get_striping_factor(&inode_p->s.multi_desc);
    strip_size   = rozofs_get_striping_size(&inode_p->s.multi_desc);
    /*
    ** When not in hybrid mode 1st inode has no distribution
    */
    if (!(inode_p->s.multi_desc.master.hybrid)) {
      return 0;
    }  
    /*
    ** In hybrid mode, this inode is located on fast volume but is only one strip long
    */
    if (master_size < strip_size) {
      rebuild_size = master_size;
    }
    else {
      rebuild_size = strip_size;      
    }      
    /*
    ** Left size to rebuild on this file
    */
    master_size -= rebuild_size;
    pname += sprintf(pname," hybrid");    
  }
  else {
    int slave_idx = inode_p->s.multi_desc.slave.file_idx;
    pname += sprintf(pname," slave %d",slave_idx);   
    if (master_size <= (slave_idx*strip_size)) {
      rebuild_size = 0;
    }
    else {  
      rebuild_size = master_size/strip_factor;
    }   
  }
  
    
  if (rebuild_size == 0) {
    TRACE("%s is empty and won't be rebuilt",name); 
    return 0;
  }

  /*
  ** Check whether this CID/SID is interresting
  */
  
  while ((key = key_table[key_index++])!= -1)
  {
    ret = rozofs_fill_storage_info_from_mattr(&inode_p->s.attrs,&cid,sid_tab,inode_p->s.attrs.fid,key);
    if (ret != 0) continue;
    cid--;
    pCid = cid_tbl[cid];
    if (pCid == NULL) continue;
    /*
    ** check for sid
    */
    for (i = 0; i < rozofs_safe; i++) {
      rozofs_rebuild_entry_file_t entry;
      int                         j;

      sid = sid_tab[i]-1;
      pSid = (*pCid)[sid];
      if (pSid == NULL) continue;

      memset(&entry,0,entry_size);
      memcpy(entry.fid,inode_p->s.attrs.fid,sizeof(fid_t));
      entry.todo      = 1;
      entry.block_end = -1;
      entry.layout    = layout;
      entry.bsize     = bsize;
      for (j=0; j<rozofs_safe;j++) {
	entry.dist_set_current[j] = sid_tab[j];
      }    

      if (i<rozofs_forward) {
	if (file_type == rbs_file_type_spare) {
          continue;
	}	 
	pOne = &pSid->nominal;
      }
      else {
	if (file_type == rbs_file_type_nominal) {
          continue;
	}	
	pOne = &pSid->spare;    
      }  

      /*
      ** Get Job list index to write in
      */
      job = get_job_list_index(pOne);	
      
      if (write(pOne->job[job].fd,&entry,entry_size) < entry_size) {
	severe("write(cid %d sid %d job %d) %s\n", cid+1, sid+1, job, strerror(errno));
      }
      else {
	TRACE("-> added size %llu to cid %d sid %d job %d : %s\n",(long long unsigned int)rebuild_size, cid+1,sid+1,job,name);
        pOne->job[job].size += rebuild_size;
      }  
      pOne->count++;
      pOne->size += rebuild_size;
      total++;
    }  
  }
  return 0;
}

/*
**_______________________________________________________________________
*/
/** Find out which volume hods a given cid/sid
*   
    @param cid
    @param sid
    
    @retval -1 on error
*/
int get_cid_sid_volume(cid_t cid, sid_t sid) {
  list_t * v, * c, * s;

  list_for_each_forward(v, &exportd_config.volumes) {

    volume_config_t * volume = list_entry(v, volume_config_t, list);

    list_for_each_forward(c, &volume->clusters) {
	
      cluster_config_t *cluster = list_entry(c, cluster_config_t, list);

      if (cluster->cid != cid) continue;

      list_for_each_forward(s, (&cluster->storages[0])) {
	  
        storage_node_config_t *storage = list_entry(s, storage_node_config_t, list);

        if (storage->sid == sid) return volume->vid;
      }
    }
  }
  return -1;
}
/*
**_______________________________________________________________________
**
**  Parse input cid/sid and prepare the working structure
**   
**  @param line: inout line to parse
**  @param p: pointer to the cid/sid list represenation in memory
**  @param buffer : pointer to the error buffer
**  
**  @retval 0  on success
**  @retval -1 on error
**
**        cid_tbl
**       +--------+
**       |        |
**       |  NULL  |
**       +--------+
**       |        |
**       |    o-------> 
**       +--------+
**          ...
**       +--------+       sid_tbl
**       |        |     +--------+
**       |     o------->|  NULL  |
**       +--------+     |        |
**       |        |     +--------+
**       |  NULL  |     |        |      sid_info_t
**       +--------+     |    o--------->+---------+ 
**          ...         +--------+      | nominal |
**                      |  NULL  |      | spare   |
**                      |        |      +---------+
**                      +--------+
**                         ...
*/
int parse_cidsid_list(char *line,int rebuildRef, int parallel)
{
  int ret;
  char * pch;
  int cid = -1;
  int sid = -1;
  int i;
  char fName[ROZOFS_FILENAME_MAX];
  sid_tbl_t      * pCid;
  sid_info_t     * pSid;
  int              vid;

  pch = line;
  while (*pch != 0) {
    /*
    ** convert to integer
    */
    ret = sscanf(pch, "%d:%d", &cid, &sid); 
    if (ret!=2) {
      severe("Bad conversion for cid:sid %s\n",pch);
      return -1;
    }    
    while ((cid > 0) && (cid <= ROZOFS_CLUSTERS_MAX) && 
	    (sid > 0) && (sid <= SID_MAX)) 
    {

      /*
      ** Find out the volume tha hold this cid/sid
      */
      vid = get_cid_sid_volume(cid,sid);
      if (vid < 0) {
	    severe("cid/sid %d/%d not found in configuration file\n", cid, sid);
	    break;
      }
      // Is this volume already recorded	
      for (i=0; i < nb_requested_vid; i++) {
	if (requested_vid[i] == vid) break;
      }
      if (i==nb_requested_vid) {
	// record it
	requested_vid[nb_requested_vid++] = vid;
      }

      cid--;
      sid--;

      pCid = cid_tbl[cid];
      if (pCid == NULL) {
    	pCid = xmalloc(sizeof(sid_tbl_t));
	memset(pCid,0,sizeof(sid_tbl_t));
	cid_tbl[cid] = pCid;
      }
	  
      pSid = (*pCid)[sid];
      if (pSid == NULL) {
	pSid = xmalloc(sizeof(sid_info_t));
	memset(pSid,0,sizeof(sid_info_t));
	(*pCid)[sid] = pSid;
      }

      if (file_type != rbs_file_type_spare) {
	sprintf(fName,"%scid%d_sid%d_%d",pDir,cid+1,sid+1,rbs_file_type_nominal);
	if (mkdir(fName,ALLRIGHTS)<0) {
          severe("mkdir(%s) %s\n",fName,strerror(errno));
	}  

	for (i=0; i< parallel; i++) {
	  sprintf(fName,"%scid%d_sid%d_%d/job%d",pDir,cid+1,sid+1,rbs_file_type_nominal,i);
	  pSid->nominal.job[i].fd = open(fName, O_CREAT | O_TRUNC | O_APPEND | O_WRONLY,ALLRIGHTS);
	  if (pSid->nominal.job[i].fd == -1) {
	    severe("open(%s) %s\n",fName,strerror(errno));
	  }
	  TRACE("%s opened\n",fName);
	} 
      }

      if (file_type != rbs_file_type_nominal) {      	 
	sprintf(fName,"%scid%d_sid%d_%d",pDir,cid+1,sid+1,rbs_file_type_spare);
	if (mkdir(fName,ALLRIGHTS)<0) {
          severe("mkdir(%s) %s\n",fName,strerror(errno));
	}  

	for (i=0; i< parallel; i++) {
	  sprintf(fName,"%scid%d_sid%d_%d/job%d",pDir,cid+1,sid+1,rbs_file_type_spare,i);
	  pSid->spare.job[i].fd = open(fName, O_CREAT | O_TRUNC | O_APPEND | O_WRONLY,ALLRIGHTS);
	  if (pSid->spare.job[i].fd == -1) {
	    severe("open(%s) %s\n",fName,strerror(errno));
	  }
	  TRACE("%s opened\n",fName);
	}
      }	  	    
      break;		
    }

    while((*pch!=',')&&(*pch!=0)) pch++;
    if(*pch==',') pch++;
  }
  return 0;   
}
/*
**_______________________________________________________________________
** Close every job file  
*/
void close_all() {
  int              cid, sid, job;
  sid_tbl_t      * pCid;
  sid_info_t     * pSid;
  char             fname[ROZOFS_FILENAME_MAX];
  int              fd;
  char             msg[512];
  char           * pChar = msg;
    
  for (cid=0; cid<ROZOFS_CLUSTERS_MAX; cid++) {
  
    pCid = cid_tbl[cid];
    if (pCid == NULL) continue;

    for (sid=0; sid<SID_MAX; sid++) {
	
      pSid = (*pCid)[sid];
      if (pSid == NULL) continue;

      for (job=0; job<parallel; job++) {
        if (file_type != rbs_file_type_spare) {
	  if (pSid->nominal.job[job].fd>0) {
	    close(pSid->nominal.job[job].fd);
	    pSid->nominal.job[job].fd = 0;
	  }
	}
        if (file_type != rbs_file_type_nominal) {      	 
	  if (pSid->spare.job[job].fd>0) {
	    close(pSid->spare.job[job].fd);
	    pSid->spare.job[job].fd = 0;
	  }	
	}  
      }
	
      pChar += sprintf(pChar,"[%d-%d",cid+1,sid+1);

      if (file_type != rbs_file_type_spare) {
	sprintf(fname,"%s/cid%d_sid%d_%d/count",pDir,cid+1,sid+1,rbs_file_type_nominal);
	fd = open(fname, O_WRONLY | O_APPEND | O_CREAT | O_TRUNC,ALLRIGHTS);
	if (fd<0) {
          severe("open(%s) %s\n",fname,strerror(errno));
	  continue;
	}	
	if (write(fd,&(pSid->nominal.count), sizeof(uint64_t)) != sizeof(uint64_t)) {
          severe("write(%d,%s) %s\n",fd,fname,strerror(errno));	  
	}
	if (write(fd,&(pSid->nominal.size), sizeof(uint64_t)) != sizeof(uint64_t)) {
          severe("write(%d,%s) %s\n",fd,fname,strerror(errno));	  
	}	
	close(fd);

	pChar += sprintf(pChar,"/%llu nom %lluB",
	      (unsigned long long)pSid->nominal.count,
	      (unsigned long long)pSid->nominal.size);
      }

      if (file_type != rbs_file_type_nominal) {    
	sprintf(fname,"%s/cid%d_sid%d_%d/count",pDir,cid+1,sid+1,rbs_file_type_spare);
	fd = open(fname, O_WRONLY | O_APPEND | O_CREAT | O_TRUNC,ALLRIGHTS);
	if (fd<0) {
          severe("open(%s) %s\n",fname,strerror(errno));
	  continue;
	}	
	if (write(fd,&(pSid->spare.count), sizeof(uint64_t)) != sizeof(uint64_t)) {
          severe("write(%d,%s) %s\n",fd,fname,strerror(errno));	  
	}
	if (write(fd,&(pSid->spare.size), sizeof(uint64_t)) != sizeof(uint64_t)) {
          severe("write(%d,%s) %s\n",fd,fname,strerror(errno));	  
	}
	close(fd);

	pChar += sprintf(pChar,"/%llu sp %lluB",
	      (unsigned long long)pSid->spare.count,
	      (unsigned long long)pSid->spare.size);
      }
      pChar += sprintf(pChar,"]");	      	   	
    }   
  }
  info("%s",msg);   
}
/*-----------------------------------------------------------------------------
**
**  Display usage
**
**----------------------------------------------------------------------------
*/
char * utility_name=NULL;
void usage(char * fmt, ...) {
  va_list   args;
  char      error_buffer[512];
  
  /*
  ** Display optionnal error message if any
  */
  if (fmt) {
    va_start(args,fmt);
    vsprintf(error_buffer, fmt, args);
    va_end(args);   
    severe("%s",error_buffer);
    printf("%s\n",error_buffer);
  }
  
  printf("RozoFS rebuild list maker utility - %s\n", VERSION);
  printf("Usage: %s [OPTIONS]\n\n",utility_name);
  printf("\t-h,--help                    print this message.\n\n");
  printf("\t-i,--input      <cid/sid>    mandatory list <cid>:<sid>,<sid>,<sid>.... \n");
  printf("\t-p,--parallel   <parallel>   mandatory rebuild parallelism number.\n");
  printf("\t-r,--rebuildRef <rebuildRef> mandatory rebuild reference\n");
  printf("\t-E,--expDir     <cfgFile>    optionnal result directory.\n");
  printf("\t-c,--config     <cfgFile>    optionnal configuration file name.\n");
  printf("\t-s,--spare                   only list spare files.\n");
  printf("\t-n,--nominal                 only list nominal files.\n");
  printf("\t-d,--debug                   display debugging information.\n");

  if (fmt) exit(EXIT_FAILURE);
  exit(EXIT_SUCCESS); 
};
/*-----------------------------------------------------------------------------
**
**  M A I N
**
**----------------------------------------------------------------------------
*/
int main(int argc, char *argv[]) {
  int c;
  void *rozofs_export_p;
  char *cidsid_p= NULL;
  int   i;
  int ret;

  static struct option long_options[] = {
      {"help", no_argument, 0, 'h'},
      {"input", required_argument, 0, 'i'},
      {"parallel", required_argument, 0, 'p'},
      {"rebuildRef", required_argument, 0, 'r'},
      {"expDir", required_argument, 0, 'E'},      
      {"config", required_argument, 0, 'c'},
      {"spare", no_argument, 0, 's'},
      {"nominal", no_argument, 0, 'n'},
      {"debug", no_argument, 0, 'd'},
      {0, 0, 0, 0}
  };

  openlog("rozo_rbsList", LOG_PID, LOG_DAEMON);

  /*
  ** Get utility name
  */
  utility_name = basename(argv[0]);   

  while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "hdv:i:p:r:c:E:sn", long_options, &option_index);

      if (c == -1)
          break;

      switch (c) {

        case 'h':
          usage(NULL);
          break;
		  
        case 'c':
          configFileName = optarg;
          break;	
		  
        case 'E':
          pDir = optarg;
          break;	
		  		  
        case 'p':
          if (sscanf(optarg,"%d",&parallel)!= 1) {
            usage("Bad -p value %s\n",optarg);
          }
          break;
		  		  
        case 'd':
          debug = 1;
          break;		  			  			  

        case 'r':
          if (sscanf(optarg,"%d",&rebuildRef)!= 1) {
            usage("Bad -r value %s\n",optarg);
          }
          break;
		  			  			  
        case 'i':
	  if (cidsid_p!= NULL) {
            usage("cid/sid list already defined : %s\n",cidsid_p);
	  }
          cidsid_p = optarg;
          break;
	  
        case 's':
	  if (file_type == rbs_file_type_nominal) {
            usage("-s or -n are incompatible");
	  }
          file_type = rbs_file_type_spare;
          break;
	  
        case 'n':
	  if (file_type == rbs_file_type_spare) {
            usage("-s or -n are incompatible");
	  }
          file_type = rbs_file_type_nominal;
          break;

        default:
          usage("Unexpected parameter \'%c\'",c);
      }
  }

  if (cidsid_p == NULL) 
  {
       usage("cid/sid list missing!!");
  }  
  if (rebuildRef == 0)
  {
       usage("rebuildRef missing!!");
  } 
  if (parallel == 0)
  {
       usage("parallel missing!!");
  }        
  /*
  ** Read configuration file
  */
  if (econfig_initialize(&exportd_config) != 0) {
       usage("can't initialize exportd config %s.\n",strerror(errno));
  }    
  if (econfig_read(&exportd_config, configFileName) != 0) {
        usage("failed to parse configuration file %s %s.\n",
            configFileName,strerror(errno));
  }   
  if (econfig_validate(&exportd_config) != 0) {
       usage("inconsistent configuration file %s %s.\n",
            configFileName, strerror(errno)); 
  } 
  rozofs_layout_initialize();
  
  /*
  ** Create a clean directory
  */
  if (pDir == NULL) {
    sprintf(working_dir,"/tmp/rebuild.%d/",rebuildRef);
    pDir = working_dir;
  }
  else {
    sprintf(working_dir,"%s/rebuild.%d/",pDir,rebuildRef);
    pDir = working_dir;
  }
  clean_dir(pDir);
  if (mkdir(pDir,ALLRIGHTS)<0) {
    usage("mkdir(%s) %s\n",pDir,strerror(errno));
  }  
  
  /*
  ** Parse cdi:sid list
  */
  ret = parse_cidsid_list(cidsid_p, rebuildRef, parallel);
  if (ret < 0) {
     usage("erreur while parsing cid/sid list\n");
  }
  /*
  ** Loop on export
  */
  export_config_t * econfig;
  list_t          * p;

  list_for_each_forward(p, &exportd_config.exports) {

    econfig = list_entry(p, export_config_t, list);
    layout = econfig->layout;

    TRACE("export %d layout %d in volume %d path %s\n", econfig->eid, layout, econfig->vid, econfig->root);
	
    // Check whether the volume of this export is interresting
    for (i=0; i < nb_requested_vid; i++) {
      if (requested_vid[i] == econfig->vid) break;
    }
    // No interest
    if (i==nb_requested_vid) continue;

    // Interresting
    rozofs_export_p = rz_inode_lib_init(econfig->root);
    if (rozofs_export_p == NULL) {
      severe("RozoFS: error while reading %s\n",econfig->root);
      continue;  
    }

    /*
    ** init of the lv2 cache
    */
    lv2_cache_initialize(&cache);   
    
    /*
    ** Compute entry size for this export
    */
    rozofs_safe = rozofs_get_rozofs_safe(layout);
    if (rozofs_safe == 0) {
      severe("eid %d has layout %d",econfig->eid, layout);
      continue;
    }
    rozofs_forward = rozofs_get_rozofs_forward(layout);
    if (rozofs_forward == 0) {
      severe("eid %d has layout %d",econfig->eid, layout);
      continue;
    }    
    
    bsize = econfig->bsize;
    
    entry_size =  sizeof(rozofs_rebuild_entry_file_t) - ROZOFS_SAFE_MAX + rozofs_safe;
    
    rz_scan_all_inodes(rozofs_export_p,ROZOFS_REG,1,rozofs_visit,NULL,NULL,NULL);
    free(rozofs_export_p);
    
    lv2_cache_release (&cache); 
      	
  }
  
  close_all();
  econfig_release(&exportd_config);
  exit(0);
}
