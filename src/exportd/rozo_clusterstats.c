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
#include <stdarg.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/mattr.h>
#include "rozofs/rozofs_srv.h"
#include "export.h"
#include "rozo_inode_lib.h"
#include "exp_cache.h"
#include "econfig.h"

int rozofs_no_site_file = 0;


/*
** Input values that define the ranges
*/
#define ROZOFS_MAX_RANGES   128
static long long unsigned rozofs_ranges[ROZOFS_MAX_RANGES] = {
                              128*1024ULL,
                              512*1024ULL,
                              1024ULL*1024ULL,
                              1024ULL*1024ULL*16,
                              1024ULL*1024ULL*128,
                              1024ULL*1024ULL*512,
                              1024ULL*1024ULL*1024ULL,
                              1024ULL*1024ULL*1024ULL*16,
                              1024ULL*1024ULL*1024ULL*128,                              
                              1024ULL*1024ULL*1024ULL*1024ULL};
static           int      nb_ranges = 10;

uint64_t total_files[ROZOFS_MAX_RANGES];
uint64_t total_size[ROZOFS_MAX_RANGES];

typedef struct _rz_sids_stats_t
{
    uint64_t nb_files;  
    uint64_t byte_size;
    uint64_t tab_size[ROZOFS_MAX_RANGES];
} rz_sids_stats_t;



typedef struct _rz_cids_stats_t
{
   rz_sids_stats_t sid_tab[SID_MAX];
} rz_cids_stats_t;

lv2_cache_t cache;

rz_cids_stats_t *cids_tab_p[ROZOFS_CLUSTERS_MAX];

char *          configFileName = EXPORTD_DEFAULT_CONFIG;
int             eid = -1;
rozofs_layout_t layout = -1;
uint8_t         rozofs_inv, rozofs_fwd, rozofs_safe;


/*
**_______________________________________________________________________
** Read configuration to find out which volule is hosting a given cluster
**
** @param cid    The cluster id ti search for
**
** @retval the vid or -1 in case of error
**_______________________________________________________________________
*/
vid_t rozo_get_vid_from_cid(int cid) {
  list_t            * v;
  volume_config_t   * volume_p;
  list_t            * c;
  cluster_config_t  * cluster_p;

  /*
  ** Find out the voulme hosting this cluster
  */
  list_for_each_forward(v, &exportd_config.volumes) {

    volume_p = list_entry(v, volume_config_t, list);    
    
    list_for_each_forward(c, &volume_p->clusters) {

      cluster_p = list_entry(c, cluster_config_t, list);    
      if (cluster_p->cid == cid) return volume_p->vid;
    }
  }    
  return -1;
} 
/*
**_______________________________________________________________________
*/
char * rozo_display_size(uint64_t size, char * str) {
  if (size >= (1024ULL*1024ULL*1024ULL*1024ULL)) {
    str += sprintf(str,"%4lluTB",size/(1024ULL*1024ULL*1024ULL*1024ULL));
    return str;
  }  
  if (size >= (1024ULL*1024ULL*1024ULL)) {
    str += sprintf(str,"%4lluGB",(unsigned long long)size/(1024ULL*1024ULL*1024ULL));
    return str;
  }
  if (size >= (1024ULL*1024ULL)) {
    str += sprintf(str,"%4lluMB",(unsigned long long)size/(1024ULL*1024ULL));
    return str;
  } 
  if (size >= (1024ULL)) {
    str += sprintf(str,"%4lluKB",(unsigned long long)size/1024ULL);
    return str;
  }     
  str += sprintf(str,"%6llu",(unsigned long long)size);
  return str;
}    
/*
**_______________________________________________________________________
*/
void rozo_display_one_cluster(rz_cids_stats_t *cid_p,int i)
{
   int sid;
   rz_sids_stats_t *sid_p;
   int    idx;
   char   sizeString[16];
   
   
   printf("\nCluster %d:\n",i);
   printf(" sid |   bins files  |   total size  |");
   for (idx=0; idx<nb_ranges;idx++) {
     rozo_display_size(rozofs_ranges[idx],sizeString);
     printf("  <=%6s  |",sizeString);
   }  
   printf("  > %6s  |",sizeString);
   
   printf("\n ----+---------------+---------------+");
   for (idx=0; idx<=nb_ranges;idx++) {
     printf("------------+");
   }  
   
   for (sid=0; sid < SID_MAX; sid++)
   {
      sid_p = &cid_p->sid_tab[sid];
      if (sid_p->nb_files == 0) continue;
      rozo_display_size(sid_p->byte_size,sizeString);
      printf("\n %3d | %12llu  | %12s  |",
              sid,(long long unsigned int)sid_p->nb_files,sizeString);
      for (idx=0; idx<=nb_ranges;idx++) {
        printf(" %10llu |",(long long unsigned int)sid_p->tab_size[idx]);
      } 
         
   }
   printf("\n ----+---------------+---------------+");
   for (idx=0; idx<=nb_ranges;idx++) {
     printf("------------+");
   }  
   printf("\n");
   
}

/*
**_______________________________________________________________________
*/
void rozo_display_all_cluster()
{
   int cid;
   int idx;
   char   sizeString[16];
   uint64_t  sum_file;
   uint64_t  sum_size;

   
   for (cid=0; cid < ROZOFS_CLUSTERS_MAX; cid++)
   {
      if (cids_tab_p[cid] == NULL) continue;
      rozo_display_one_cluster(cids_tab_p[cid],cid);
   }

   printf("\n   User files                        |");   
   for (idx=0; idx<nb_ranges;idx++) {
     rozo_display_size(rozofs_ranges[idx],sizeString);
     printf("  <=%6s  |",sizeString);
   }  
   printf("  > %6s  |   Total    |",sizeString);
 
   printf("\n ------------------------------------+");
   for (idx=0; idx<=nb_ranges+1;idx++) {
     printf("------------+");
   }     
   
   printf("\n   Number of files                   |");
   sum_file = 0;
   for (idx=0; idx<=nb_ranges;idx++) {
     sum_file +=  total_files[idx];
     printf(" %10llu |",(long long unsigned int)total_files[idx]);
   }    
   printf(" %10llu |",(long long unsigned int)sum_file);

   printf("\n   Number of file percent            |");
   for (idx=0; idx<=nb_ranges;idx++) {
     if (sum_file) {
       printf(" %8.1f %% |",((float)total_files[idx]*100)/sum_file);
     }
     else {
       printf(" %8.1f %% |",0.0);
     }  
   }       
   printf(" %8.1f %% |",100.0);

   printf("\n   Cumulated size                    |");
   sum_size = 0;
   for (idx=0; idx<=nb_ranges;idx++) {
     sum_size +=  total_size[idx];
     rozo_display_size(total_size[idx],sizeString);
     printf(" %10s |",sizeString);
   }    
   rozo_display_size(sum_size,sizeString);
   printf(" %10s |",sizeString);

   printf("\n   Cumulated size percent            |");
   for (idx=0; idx<=nb_ranges;idx++) {
     if (sum_size) {
       printf(" %8.1f %% |",((float)total_size[idx]*100)/sum_size);
     }
     else {
       printf(" %8.1f %% |",0.0);
     }  
   }       
   printf(" %8.1f %% |",100.0);

   printf("\n   Average   size                    |");
   for (idx=0; idx<=nb_ranges;idx++) {
     if (total_files[idx]) {
       rozo_display_size(total_size[idx]/total_files[idx],sizeString);
     }
     else {
       rozo_display_size(0,sizeString);
     }  
     printf(" %10s |",sizeString);
   }   
   if (sum_file) {
     rozo_display_size(sum_size/sum_file,sizeString);
   }
   else {
     rozo_display_size(0,sizeString);
   }     
   printf(" %10s |",sizeString);
   
   printf("\n ------------------------------------+");
   for (idx=0; idx<=nb_ranges+1;idx++) {
     printf("------------+");
   }   
   printf("\n");
}
/*
 *_______________________________________________________________________
 ** Get the counter number where the input size should be added
 ** @param size    The size to find the counter for
 */ 
static int get_counter_rank(uint64_t size) {
  int idx;
  
  for (idx=0; idx<nb_ranges; idx++) {
    if (size <= rozofs_ranges[idx] ) return idx;
  }
  return nb_ranges;
}  
/*
 *_______________________________________________________________________
 ** Insert a value in the ordered list of bondaries
 ** @param size    The size to find the counter for
 */ 
static void insert_in_range(uint64_t size) {
  int        idx;
  uint64_t   save;  
  for (idx=0; idx<nb_ranges; idx++) {
    if (size < rozofs_ranges[idx]) {
      save = rozofs_ranges[idx];
      rozofs_ranges[idx] = size;
      size = save;
    }  
  }
  rozofs_ranges[nb_ranges] = size;
  nb_ranges++;
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
      rozolib_get_fname(inode_attr_p->s.attrs.fid,e,name,&inode_attr_p->s.fname,inode_attr_p->s.pfid);
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
char rzofs_path_bid[]="rozofs";
int eblocksize = 0;
int blocksize= 4096;

int rozofs_do_visit(ext_mattr_t *inode_p, uint64_t file_size) {
   int ret= 0;
   int i;
   rz_cids_stats_t  *cid_p;
   rz_sids_stats_t  *sid_p;
   uint64_t size2;
   int rank;
   
    /*
    ** Get the cluster pointer
    */
    if (cids_tab_p[inode_p->s.attrs.cid] == 0)
    {
      cids_tab_p[inode_p->s.attrs.cid] = malloc(sizeof(rz_cids_stats_t));
      if (cids_tab_p[inode_p->s.attrs.cid] == NULL)
      {
	 printf("Error while allocating %u bytes: %s\n",(unsigned int)sizeof(rz_cids_stats_t),strerror(errno));
	 exit(-1);
      }
      memset(cids_tab_p[inode_p->s.attrs.cid],0,sizeof(rz_cids_stats_t));
    }
    cid_p = cids_tab_p[inode_p->s.attrs.cid];
    
    if (file_size == 0) {
      size2 = 0;
    }
    else {  
      size2 = file_size/rozofs_inv;
      if (size2/blocksize == 0) size2 = blocksize;
    }  

    rank = get_counter_rank(file_size);  

    for (i = 0; i < rozofs_fwd; i++)
    {
       sid_p = &cid_p->sid_tab[inode_p->s.attrs.sids[i]];
       sid_p->nb_files++;
       sid_p->byte_size+=size2;
       sid_p->tab_size[rank]++;
    }
  return ret;
}
int rozofs_visit(void *exportd,void *inode_attr_p,void *p) {
  ext_mattr_t *inode_p = (ext_mattr_t *)inode_attr_p;
  ext_mattr_t *slave_p = inode_p;  
  int          idx;
  int          nb_slave;
  int          match = 0;
  rozofs_iov_multi_t vector; 
  int rank;
  uint64_t size;

   if (!S_ISREG(inode_p->s.attrs.mode)) {
     return 0;
   }   
  /*
  ** get the size of each section
  */
  rozofs_get_multiple_file_sizes(inode_p,&vector);
   
  if (inode_p->s.multi_desc.byte == 0) {
    /*
    ** Regular file without striping
    */
    match += rozofs_do_visit(inode_p, vector.vectors[0].len);
  }  
  else if (inode_p->s.multi_desc.common.master == 1) {
    /*
    ** When not in hybrid mode 1st inode has no distribution
    */
    if (inode_p->s.hybrid_desc.s.no_hybrid== 0) {
      match += rozofs_do_visit(inode_p, vector.vectors[0].len);
    }  
    /*
    ** Check every slave
    */
    slave_p ++;
    nb_slave = rozofs_get_striping_factor(&inode_p->s.multi_desc);
    for (idx=0; idx<nb_slave; idx++,slave_p++) {
      match += rozofs_do_visit(slave_p, vector.vectors[idx+1].len);
    }  
  }

  size = inode_p->s.attrs.size;
  rank = get_counter_rank(size);
  total_files[rank] ++;       
  total_size[rank] += size; 
      
  if(match) return 1;
  return 0;
} 
/*
 *_______________________________________________________________________
 ** Parse a string that defines the ranges to sort the files
 ** @param range    The range string
 */ 
static int parse_ranges(char * range) {
  char     * pChar;
  char     * unitString;
  uint64_t   unit64;   
  long long unsigned int   value;
    
      
  if (range == NULL) return -1;
  
  /*
  ** Skip beginning spaces
  */
  while(*range == ' ') range++;
  
  nb_ranges = 0;
  memset(rozofs_ranges,0,sizeof(rozofs_ranges));
  
  /*
  ** Count the number of '-' in the string
  */
  pChar = range;
  while (*pChar!=0) {
  
    unitString = pChar;

    /*
    ** Skip numbers to get point to the units
    */
    while ((*unitString >= '0')&&(*unitString <= '9')) unitString++; 

    /*
    ** Check the unit string
    */
    if ((*unitString=='T')||(*unitString=='t')) {
      unit64 = (1024ULL*1024ULL*1024ULL*1024ULL);
      unitString++;
    }  
    else if ((*unitString=='G')||(*unitString=='g')) {
      unit64 = (1024ULL*1024ULL*1024ULL);
      unitString++;
    }  
    else if ((*unitString=='M')||(*unitString=='m')) {
      unit64 = (1024ULL*1024ULL);
      unitString++;
    }  
    else if ((*unitString=='K')||(*unitString=='k')) {
      unit64 = 1024ULL;
      unitString++;
    }  
    else if ((*unitString=='B')||(*unitString=='b')||(*unitString=='-')||(*unitString==0)) {
      unit64 = 1;
    }
    else {    
      printf("\nBad units in value #%d \"%s\"\n", nb_ranges+1, range);
      return -1;
    }
    
    /*
    ** Bytes may be specified
    */
    if ((*unitString=='B')||(*unitString=='b')) {
      unitString++;
    }  
    /*
    ** Then comes a separator or the end of the string
    */
    if ((*unitString!=0)&&(*unitString!='-')) {
      printf("\nBad units in value #%d \"%s\"\n", nb_ranges+1, range);
      return -1;
    }  
    
    /*
    ** Read the numbers
    */
    if (sscanf(pChar,"%llu",&value) != 1) {
      printf("\nBad value in value #%d \"%s\"\n", nb_ranges+1, range);
      return -1;
    }  
    /*
    ** Apply the units and insert the value in the range array
    */  
    value *= unit64;
    insert_in_range(value);
        
    if (*unitString == '-')  unitString++;
    pChar = unitString;
  }
    
  return nb_ranges;
   
}
/*
 *_______________________________________________________________________
 */
char * utility_name=NULL; 
static void usage(char * fmt, ...) {
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
  
  printf("RozoFS cluster statistics utility - %s\n", VERSION);
  printf("Usage: %s [OPTIONS] -e <eid>\n\n",utility_name);
  printf("Mandatory parameters:\n");
  printf("\t-e,--eid        <eid>        mandatory export identifier.\n");
  printf("Optionnal parameters:\n");  
  printf("\t-h, --help                   print this message.\n");
  printf("\t-c,--config     <cfgFile>    optionnal configuration file name.\n");
  printf("\t-v,--verbose                 Display some execution statistics\n");
  printf("\t-r,--ranges <bondaries>      To define the file size bondaries.\n");
  printf("\t                               examples -r 1Tb-1M-1g-1kB or -r 128K-256K-512K\n");
  printf("\t                                        or -r 1M-2M-3M-4M-5M-6M-7M-8M-9M-10M\n");


  if (fmt) exit(EXIT_FAILURE);
  exit(EXIT_SUCCESS); 
};




int main(int argc, char *argv[]) {
    int c;
    void *rozofs_export_p;
    int i;
    int verbose = 0;
    export_config_t * econfig;
    list_t          * p;
    
    static struct option long_options[] = {
        {"help", no_argument, 0, 'h'},
        {"eid", required_argument, 0, 'e'},
        {"config", required_argument, 0, 'c'},
        {"verbose", required_argument, 0, 'v'},
        {"ranges", required_argument, 0, 'r'},
        {0, 0, 0, 0}
    };
    
    /*
    ** Get utility name
    */
    utility_name = basename(argv[0]); 
       
     
    while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "hvc:e:r:", long_options, &option_index);

      if (c == -1)
          break;

      switch (c) {

          case 'h':
              usage(NULL);
              exit(EXIT_SUCCESS);
              break;
          case 'e':
              if (sscanf(optarg,"%d",&eid) != 1) {
                usage("Bad export identifier value \"%s\"",optarg);
              }  
              break;                        
          case 'r':
              if (parse_ranges(optarg) <= 0) {
                usage("Bad range \"%s\"",optarg);
              }  
              break;                        
          case 'c':
              configFileName = optarg;
              break;                        
          case 'v':
              verbose = 1;
              break;    
          case '?':
              usage(NULL);
              exit(EXIT_SUCCESS);
              break;
          default:
              usage(NULL);
              exit(EXIT_FAILURE);
              break;
      }
  }
  if (eid == -1) 
  {
       usage("Missing mandatory export identifier");
  }
  /*
  ** clear the cluster table
  */
  for (i= 0; i < ROZOFS_CLUSTERS_MAX;i++)
  {
     cids_tab_p[i] = NULL;  
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
  ** Loop on configured export
  */
  layout = -1;
  list_for_each_forward(p, &exportd_config.exports) {

    econfig = list_entry(p, export_config_t, list);
    
    if (econfig->eid != eid) continue;
    layout = econfig->layout;  
    rozofs_get_rozofs_invers_forward_safe(econfig->layout, &rozofs_inv, &rozofs_fwd, &rozofs_safe);
    blocksize = ROZOFS_BSIZE_BYTES(econfig->bsize)/rozofs_inv;
    break;
  }
  if (layout == -1) {  
    usage("No such eid %d configured",eid);
  }  
  
  /*
  ** init of the RozoFS data structure on export
  ** in order to permit the scanning of the exportd
  */
  rozofs_export_p = rz_inode_lib_init(econfig->root);
  if (rozofs_export_p == NULL)
  {
    usage("RozoFS: error while reading %s\n",econfig->root);
  }
  /*
  ** init of the lv2 cache
  */
  lv2_cache_initialize(&cache);
  rz_set_verbose_mode(verbose);
  rz_scan_all_inodes(rozofs_export_p,ROZOFS_REG,1,rozofs_visit,NULL,NULL,NULL);

  rozo_display_all_cluster();

  exit(EXIT_SUCCESS);  
  return 0;
}
