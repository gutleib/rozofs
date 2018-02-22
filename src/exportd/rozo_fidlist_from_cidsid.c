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

int rozofs_no_site_file;

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
    uint64_t selected;
    uint64_t nb_files;  
    uint64_t byte_size;
    int  current_count;   /**< number of fid in the buffer */
    int  fd;              /**< file descriptor             */
    char *bufout;
    uint64_t tab_size[FILE_MAX_T_E];
} rz_sids_stats_t;



typedef struct _rz_cids_stats_t
{
   rz_sids_stats_t sid_tab[SID_MAX];
} rz_cids_stats_t;

typedef struct _rozo_cid_sid_list_t
{
    uint8_t nb_sid;
    uint8_t cid;
    uint8_t sid[SID_MAX];
} rozo_cid_sid_list_t;

lv2_cache_t cache;

rz_cids_stats_t *cids_tab_p[ROZOFS_CLUSTERS_MAX];

void rozo_flush_fid_distribution(rz_sids_stats_t *p);

char *rozo_build_storage_cid_sid_fname(char *root,int cid,int sid,char *bufout)
{
    char *pBuf= bufout;
    
    pBuf +=sprintf(pBuf,"%s/storage_%d_%d",root,cid,sid);
    return bufout;
}
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
      rozo_flush_fid_distribution(sid_p);
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
int uuid_string_length = 36;
void rozo_print_fid_distribution(ext_mattr_t *inode_p,rz_sids_stats_t *p)
{
   char buf_fid[64];
   int ret;
   
   uuid_unparse(inode_p->s.attrs.fid,buf_fid);
   buf_fid[36]='\n';
   if ((p->current_count + uuid_string_length +1) > 4096)
   {
      ret = write(p->fd,p->bufout,p->current_count);
      if (ret < 0)
      {
        printf("Error while writing sid file: %s\n",strerror(errno));
	exit(-1);
      }
      if (ret != p->current_count)
      {
        printf("Error while writing sid file: bad size %d (%d)\n",ret,p->current_count);
	exit(-1);      
      }
      p->current_count=0;
   }
   memcpy(&p->bufout[p->current_count],buf_fid,uuid_string_length+1);
   p->current_count +=uuid_string_length+1;   
//   printf("%s\n",buf_fid);

}

/*
**_______________________________________________________________________
*/
void rozo_flush_fid_distribution(rz_sids_stats_t *p)
{
   int ret;
   if (p->current_count!= 0)
   {
      ret = write(p->fd,p->bufout,p->current_count);
      if (ret < 0)
      {
        printf("Error while writing sid file: %s\n",strerror(errno));
	exit(-1);
      }
      if (ret != p->current_count)
      {
        printf("Error while writing sid file: bad size %d (%d)\n",ret,p->current_count);
	exit(-1);      
      }
      p->current_count=0;
   }
   free(p->bufout);
   close(p->fd);
   p->fd = -1;
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
    if (cids_tab_p[inode_p->s.attrs.cid] == 0) return ret;
    /*
    ** check the presence of the sid
    */
    cid_p = cids_tab_p[inode_p->s.attrs.cid];
    uint64_t size;
    uint64_t size2 = inode_p->s.attrs.size;
    size2 = size2/divider;
    if (size2/blocksize == 0) size2 = blocksize;
    for (i = 0; i < rozofs_fwd; i++)
    {
       sid_p = &cid_p->sid_tab[inode_p->s.attrs.sids[i]];
       if (sid_p->selected == 0) continue;
       rozo_print_fid_distribution(inode_p,sid_p);
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
**_______________________________________________________________________
*/
/**
*   
    @param line: inout line to parse
    @param p: pointer to the cid/sid list represenation in memory
    @param buffer : pointer to the error buffer
    
    @retval 0  on success
    @retval -1 on error
*/
int parse_cidsid_list(char *line,rozo_cid_sid_list_t *p,char *buffer)
{
   int ret;
  char * pch;
  int val=-1;
  int cid = -1;
  int count=0;
   
   memset(p,0,sizeof(rozo_cid_sid_list_t));
   pch = strtok (line,":,");
   while (pch != NULL)
   {
     /*
     ** convert to integer
     */
     ret = sscanf(pch, "%d", &val); 
     if (ret!=1)
     {
       sprintf(buffer,"Bad conversion for cid %d",cid);
       return -1;
     }    
     if (cid == -1) 
     {
       p->cid = val;
       cid = val;
     }
     else
     {
       p->sid[count] = val;
       count++;
       p->nb_sid = count;
     }
     pch = strtok (NULL, ":,");
   }
   return 0;   
}

/*
 *_______________________________________________________________________
 */
static void usage() {
    printf("Usage: ./rzsave [OPTIONS]\n\n");
    printf("\t-h, --help\tprint this message.\n");
    printf("\t-p,--path <export_root_path>\t\texportd root path \n");
    printf("\t-i,--input:    cid/sid list <cid>:<sid>,<sid>,<sid>.... \n");

};




int main(int argc, char *argv[]) {
    int c;
    void *rozofs_export_p;
    int i;
    char *root_path=NULL;
    char *cidsid_p= NULL;
    char buferr[1024];
    char pathname[4096];
    char root[1024];
    
    sprintf(root,".");
    
    rozo_cid_sid_list_t  cidsid_list;
    int ret;
    
    static struct option long_options[] = {
        {"help", no_argument, 0, 'h'},
        {"path", required_argument, 0, 'p'},
        {"input", required_argument, 0, 'i'},
        {0, 0, 0, 0}
    };
    
  
    while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "hlrc:p:i:", long_options, &option_index);

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
          case 'i':
	      if (cidsid_p!= NULL)
	      {
	        printf("cid/sid list already defined : %s\n",cidsid_p);
        	usage();
        	exit(EXIT_FAILURE);	        
	      
	      }
              cidsid_p = optarg;
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
  if (cidsid_p == NULL) 
  {
       printf("cid/sid list missing!!\n");
       usage();
       exit(EXIT_FAILURE);  
  }
  printf("parse \n");
  ret = parse_cidsid_list(cidsid_p,&cidsid_list,buferr);
  if (ret < 0)
  {
     printf("erreur while parsing cid/sid list: %s\n",buferr);
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
  ** Insert the sid in the cluster
  */
  if (cids_tab_p[cidsid_list.cid] !=0)
  {
     printf("erreur while parsing cid/sid list: cluster is already defined (%d)\n",cidsid_list.cid);
     usage();
     exit(EXIT_FAILURE);           
  }
  {
    cids_tab_p[cidsid_list.cid] = malloc(sizeof(rz_cids_stats_t));
    if (cids_tab_p[cidsid_list.cid] == NULL)
    {
       printf("Error while allocating %u bytes: %s\n",(unsigned int)sizeof(rz_cids_stats_t),strerror(errno));
       exit(-1);
    }
    memset(cids_tab_p[cidsid_list.cid],0,sizeof(rz_cids_stats_t));
  }
  /*
  ** now fill the list of the sid
  */
  for (i = 0; i < cidsid_list.nb_sid;i++)
  {
    cids_tab_p[cidsid_list.cid]->sid_tab[cidsid_list.sid[i]].selected = 1;  
    rozo_build_storage_cid_sid_fname(root,cidsid_list.cid,cidsid_list.sid[i],pathname);
    cids_tab_p[cidsid_list.cid]->sid_tab[cidsid_list.sid[i]].bufout = malloc(4096);
    if (cids_tab_p[cidsid_list.cid]->sid_tab[cidsid_list.sid[i]].bufout == NULL)
    {
       printf("Error while allocating %u bytes: %s\n",(unsigned int)sizeof(rz_cids_stats_t),strerror(errno));
       exit(-1);
    }
    cids_tab_p[cidsid_list.cid]->sid_tab[cidsid_list.sid[i]].current_count = 0;
    cids_tab_p[cidsid_list.cid]->sid_tab[cidsid_list.sid[i]].fd= open( pathname,O_TRUNC|O_CREAT|O_RDWR,S_IRWXU);
    if (cids_tab_p[cidsid_list.cid]->sid_tab[cidsid_list.sid[i]].fd< 0)
    {
       printf("failure while opening %s:%s\n",pathname,strerror(errno));
       exit(-1);    
    }
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
  
  rz_scan_all_inodes(rozofs_export_p,ROZOFS_REG,1,rozofs_visit,NULL,NULL,NULL);

  rozo_display_all_cluster();
  exit(EXIT_SUCCESS);
}
