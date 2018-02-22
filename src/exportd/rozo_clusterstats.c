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
#include "export.h"
#include "rozo_inode_lib.h"
#include "exp_cache.h"

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
*/
void rozo_display_one_cluster(rz_cids_stats_t *cid_p,int i)
{
   char buffer[1024];
   int sid;
   rz_sids_stats_t *sid_p;
   printf("Cluster %d:\n",i);
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
int rozofs_fwd = -1;
int divider;
int blocksize= 4096;
int rozofs_visit(void *exportd,void *inode_attr_p,void *p)
{
   int ret= 0;
   int i;
   ext_mattr_t *inode_p = inode_attr_p;
   rz_cids_stats_t  *cid_p;
   rz_sids_stats_t  *sid_p;
   
   /*
   ** Do not process symlink
   */
   if (!S_ISREG(inode_p->s.attrs.mode)) {
     return 0;
   }   

   if (rozofs_fwd < 0) 
   {
      /*
      ** compute the layout on the first file
      */
      rozofs_fwd = 0;
      for (i=0; i < ROZOFS_SAFE_MAX; i++,rozofs_fwd++)
      {
         if (inode_p->s.attrs.sids[i]==0) break;
      }
      switch (rozofs_fwd)
      {
         case 4:
	   rozofs_fwd -=1;
	   divider = 2;
	   break;
	 case 8:
	   rozofs_fwd -=2;
	   divider = 4;
	   break;
	 case 16:
	   rozofs_fwd -=4;
	   divider = 8;
	   break;
	 default:
	   exit(-1);
      }
      blocksize = blocksize/divider;
    }
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
    uint64_t size2 = inode_p->s.attrs.size;
    size2 = size2/divider;
    if (size2/blocksize == 0) size2 = blocksize;
    for (i = 0; i < rozofs_fwd; i++)
    {
       sid_p = &cid_p->sid_tab[inode_p->s.attrs.sids[i]];
       sid_p->nb_files++;
       sid_p->byte_size+=size2;
       while(1)
       {
	 if (inode_p->s.attrs.size/RZ_FILE_128K == 0)
	 {
           sid_p->tab_size[FILE_128K_E]++;
	   break;
	 }
	 size = inode_p->s.attrs.size;
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


/*
 *_______________________________________________________________________
 */
static void usage() {
    printf("Usage: ./rzsave [OPTIONS]\n\n");
    printf("\t-h, --help\tprint this message.\n");
    printf("\t-p,--path <export_root_path>\t\texportd root path \n");
    printf("\t-v,--verbose                \t\tDisplay some execution statistics\n");

};




int main(int argc, char *argv[]) {
    int c;
    void *rozofs_export_p;
    int i;
    char *root_path=NULL;
    int verbose = 0;
    
    static struct option long_options[] = {
        {"help", no_argument, 0, 'h'},
        {"path", required_argument, 0, 'p'},
        {"verbose", required_argument, 0, 'v'},
        {0, 0, 0, 0}
    };
    
  
    while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "hvlrc:p:", long_options, &option_index);

      if (c == -1)
          break;

      switch (c) {

          case 'h':
              usage();
              exit(EXIT_SUCCESS);
              break;
          case 'p':
              root_path = optarg;
              break;
          case 'v':
              verbose = 1;
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
  if (root_path == NULL) 
  {
       usage();
       exit(EXIT_FAILURE);  
  }
  /*
  ** clear the cluster table
  */
  for (i= 0; i < ROZOFS_CLUSTERS_MAX;i++)
  {
     cids_tab_p[i] = NULL;  
  }

  /*
  ** init of the RozoFS data structure on export
  ** in order to permit the scanning of the exportd
  */
  rozofs_export_p = rz_inode_lib_init(root_path);
  if (rozofs_export_p == NULL)
  {
    printf("RozoFS: error while reading %s\n",root_path);
    exit(EXIT_FAILURE);  
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
