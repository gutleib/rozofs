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

#define RZ_FILE_128K  (1024*128)
#define RZ_FILE_1M  (1024*1024)

typedef enum _rs_file_sz_e
{
   FILE_128K_E = 0,
   FILE_1M_E,
   FILE_10M_E,
   FILE_100M_E,
   FILE_1G_E,
   FILE_10G_E,
   FILE_100G_E,
   FILE_1T_E,
   FILE_SUP_1T_E,
   FILE_MAX_T_E
} rs_file_sz_e;
  

typedef struct _rz_sids_stats_t
{
    uint64_t nb_files;  
    uint64_t byte_size;
    uint64_t tab_size[FILE_MAX_T_E];
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
*/
#define SUFFIX(var) sprintf(suffix,"%s",var);
char  *display_size(long long unsigned int number,char *buffer)
{
    double tmp = number;
    char suffix[64];
        SUFFIX(" B ");

        if (tmp >= 1024) { tmp = tmp / 1024;  SUFFIX( " KB"); }
        if (tmp >= 1024) { tmp = tmp / 1024;  SUFFIX( " MB"); }
        if (tmp >= 1024) { tmp = tmp / 1024;  SUFFIX( " GB"); }
        if (tmp >= 1024) { tmp = tmp / 1024;  SUFFIX( " TB"); }
        if (tmp >= 1024) { tmp = tmp / 1024;  SUFFIX( " PB"); }
    sprintf(buffer,"%10.2f%s", tmp,suffix);
    return buffer;
}
/*
**_______________________________________________________________________
*/
char *rozo_display_one_sid(rz_sids_stats_t *sid_p,int i,char *pbuf)
{
  char buffer[128];
  int k;
  pbuf +=sprintf(pbuf," %3.3d | %12llu  | %s |",i,(long long unsigned int)sid_p->nb_files,
                                                         display_size((long long unsigned int)sid_p->byte_size,buffer));
  for (k = 0; k < FILE_MAX_T_E; k ++)
  {
    pbuf +=sprintf(pbuf," %10llu |",(long long unsigned int)sid_p->tab_size[k]);
  }
  sprintf(pbuf,"\n");
  return pbuf;
 
}
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
void rozo_display_one_cluster(rz_cids_stats_t *cid_p,int i)
{
   char buffer[1024];
   int sid;
   rz_sids_stats_t *sid_p;
   
   printf("Volume %d / Cluster %d:\n",rozo_get_vid_from_cid(i),i);
   printf(" sid |   bins files  |   total size  |    0-128K  |   128K-1M  |    1-10M   |   10-100M  |   100-1000M|     1-10G  |    10-100G |   100-1000G|      > 1TB |\n");
   printf(" ----+---------------+---------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+\n");
   for (sid=0; sid < SID_MAX; sid++)
   {
      sid_p = &cid_p->sid_tab[sid];
      if (sid_p->nb_files == 0) continue;
      rozo_display_one_sid(sid_p,sid,buffer);
      printf("%s",buffer);   
   }
   printf(" ----+---------------+---------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+\n");
   printf("\n");
}

/*
**_______________________________________________________________________
*/
void rozo_display_all_cluster()
{
   int cid;
   
   for (cid=0; cid < ROZOFS_CLUSTERS_MAX; cid++)
   {
      if (cids_tab_p[cid] == NULL) continue;
      rozo_display_one_cluster(cids_tab_p[cid],cid);
   }

   printf("\n");
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
    uint64_t size;
    
    if (file_size == 0) {
      size2 = 0;
    }
    else {  
      size2 = file_size/rozofs_inv;
      if (size2/blocksize == 0) size2 = blocksize;
    }  
    
    for (i = 0; i < rozofs_fwd; i++)
    {
       sid_p = &cid_p->sid_tab[inode_p->s.attrs.sids[i]];
       sid_p->nb_files++;
       sid_p->byte_size+=size2;
       while(1)
       {
	 if (file_size/RZ_FILE_128K == 0)
	 {
           sid_p->tab_size[FILE_128K_E]++;
	   break;
	 }
	 size = file_size;
	 size = size/RZ_FILE_1M;
	 if (size == 0)
	 {
           sid_p->tab_size[FILE_1M_E]++;
	   break;
	 }       
	 size = size/10;
	 if (size == 0)
	 {
           sid_p->tab_size[FILE_10M_E]++;
	   break;
	 } 
	 size = size/10;
	 if (size == 0)
	 {
           sid_p->tab_size[FILE_100M_E]++;
	   break;
	 } 
	 size = size/10;
	 if (size == 0)
	 {
           sid_p->tab_size[FILE_1G_E]++;
	   break;
	 } 
	 size = size/10;
	 if (size == 0)
	 {
           sid_p->tab_size[FILE_10G_E]++;
	   break;
	 } 
	 size = size/10;
	 if (size == 0)
	 {
           sid_p->tab_size[FILE_100G_E]++;
	   break;
	 } 
	 size = size/10;
	 if (size == 0)
	 {
           sid_p->tab_size[FILE_1T_E]++;
	   break;
	 } 
	 sid_p->tab_size[FILE_SUP_1T_E]++;
	 break;
       }
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
  if(match) return 1;
  return 0;
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
        {0, 0, 0, 0}
    };
    
    /*
    ** Get utility name
    */
    utility_name = basename(argv[0]); 
       
     
    while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "hvc:e:", long_options, &option_index);

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
