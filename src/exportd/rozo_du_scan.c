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
#include "rozofs_du.h"
#include "econfig.h"

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




int  rozodu_big_dir_count_table_sz=0;     
big_obj_t  rozodu_big_dir_count_table[ROZODU_MAX_OBJ+1];
int  rozodu_big_dir_size_table_sz=0;     
big_obj_t  rozodu_big_dir_size_table[ROZODU_MAX_OBJ+1];
int  rozodu_big_file_table_sz=0;     
big_obj_t  rozodu_big_file_size_table[ROZODU_MAX_OBJ+1];


lv2_cache_t cache;
int rozodu_verbose = 0;
int format_byte = 1;
rozodu_wrctx_t rozodu_outctx;


rz_cids_stats_t *cids_tab_p[ROZOFS_CLUSTERS_MAX];
uint32_t nb_dirs = 0;
uint64_t nb_files = 0;
uint64_t total_bytes = 0;
int rozofs_no_site_file = 0;
char *rozo_get_full_path(void *exportd,void *inode_p,char *buf,int lenmax);
char  *display_size(long long unsigned int number,char *buffer);
econfig_t exportd_config;
int rozodu_eid;
int format_bytes=1;
char *rozodu_output_path = NULL;

/*
** Get the default config file name
*/
char * configFileName = EXPORTD_DEFAULT_CONFIG;

#define TRACEOUT(fmt, ...) { \
  if (fd_out==NULL) printf(fmt, ##__VA_ARGS__);\
  else              fprintf(fd_out,fmt, ##__VA_ARGS__);\
}
/*
**_______________________________________________________________________
*/
/**
   Insert the object in sorted table, When the table is full, the last entry is ejected
   
   @param table_p : current table
   @param count: pointer to the current number of entries in the table
   @param key: value used for sorting
   @param fid : inode if the object either a directory or file
*/
void rozodu_insert_sorted(big_obj_t *table_p,int *count,uint64_t key ,fid_t fid)
{
    int first = 0;
    int last = *count-1;
    int found= 0;
    int middle=0;
    int nb_entries;
    int k;

    if (key==0) return;
    while((first <= last) &&(found==0))
    {
       middle = (first+last)/2;
       if(table_p[middle].count == key) 
       {
         found = 1;
	 break;
       }
       if (table_p[middle].count < key) last=middle-1;
       else first = middle+1;
    }
     if ((table_p[middle].count > key) && (last >= 0)) middle++;
    nb_entries = *count;
    for (k=nb_entries-1;k >= middle;k--)
    {
       memcpy(&table_p[k+1],&table_p[k],sizeof(big_obj_t));    
    }
    if (middle >= ROZODU_MAX_OBJ) 
    {
       return;    
    }
    table_p[middle].count = key;
    memcpy(table_p[middle].fid,fid,sizeof(fid_t));
    *count+=1;
    if (*count > ROZODU_MAX_OBJ) *count = ROZODU_MAX_OBJ;

}
/*
**_______________________________________________________________________
*/
void rozodu_display_sorted_table(char *label,void *exportd,big_obj_t *table_p,int count,int type)
{
   char bufall[1024];
   char buffer2[64];
   int i;
   lv2_entry_t *plv2;
   ext_mattr_t *inode_attr_p;
   char *pChar;
   export_t *e= exportd;
               
   ROZODU_PRINT("%s\n",label);
   if (count == 0) 
   {
      ROZODU_PRINT("Table is empty\n");
      return;
   }
   for (i=0; i < count; i++)
   {
     //rozofs_uuid_unparse(table_p[i].fid,bufall);
     if (!(plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,&cache, table_p[i].fid))) {
       continue;
     }  
     inode_attr_p=  &plv2->attributes;
     pChar = rozo_get_full_path(exportd,inode_attr_p, bufall,sizeof(bufall));
     if (pChar)
     {   
       if (type == 0)
       {
         ROZODU_PRINT("%-64s : %llu\n",pChar,(unsigned long long int)table_p[i].count);
       }
       else
       { 
         if (format_bytes )
	 {
           ROZODU_PRINT("%-64s : %llu\n",pChar,(unsigned long long int)table_p[i].count);
	 }
	 else
	 {
	   ROZODU_PRINT("%-64s : %s\n",pChar,display_size(table_p[i].count,buffer2));
	 }
       }
     }
   
   }
   ROZODU_PRINT("\n\n");


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
uint64_t missing_entry= 0;
int rozofs_visit(void *exportd,void *inode_attr_p,void *p)
{
   int ret= 0;
   ext_mattr_t *inode_p = inode_attr_p;
   rozofs_dir_layout_t  *entry_p;
   rozofs_du_project_t *prj_p;
   /*
   ** Do not process symlink
   */
   if (!S_ISREG(inode_p->s.attrs.mode)) {
     return 0;
   }
   /*
   ** search for the directory entry
   */
   entry_p = htable_get(&htable_directory, inode_p->s.pfid);
   if (entry_p == NULL)
   {
     missing_entry++;
     return 0;
   
   }
   entry_p->nb_files+=1;
   entry_p->nb_bytes += inode_p->s.attrs.size;
   nb_files++;
   total_bytes += inode_p->s.attrs.size;
   /*
   ** insert in the big file sorted table
   */
   rozodu_insert_sorted(rozodu_big_file_size_table,&rozodu_big_file_table_sz,inode_p->s.attrs.size,inode_p->s.attrs.fid);
   /*
   ** Check if there is a tag on the directory entry
   */
   if ((entry_p->opaque != 0) && ( entry_p->opaque < ROZODU_MAX_PROJECTS))
   {
     prj_p = rozofs_project_table[entry_p->opaque];
     if (prj_p != NULL)
     {
       rozodu_insert_sorted(prj_p->big_table_p[ROZOPRJ_FILE_SZ],&prj_p->count[ROZOPRJ_FILE_SZ],inode_p->s.attrs.size,inode_p->s.attrs.fid);     
     }
   }


  return ret;
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
 int rozofs_visit_dir(void *exportd,void *inode_attr_p,void *p)
{
   int ret= 0;
   ext_mattr_t *inode_p = inode_attr_p;
   
   ret = rozodu_dir_search_pfid_fid(inode_p->s.pfid,inode_p->s.attrs.fid);
   nb_dirs++;

  return ret;
}

/*
**_______________________________________________________________________
**
**  Scan a string containing an unsigned long integer on 64 bits
**
** @param str  : the string to scan
**   
** @retval -1 when bad string is given
** @retval the unsigned long integer value
*/
static inline int64_t rozofs_scan_u64(char * str) {
  int64_t val=0;
  int      ret;
  
  ret = sscanf(str,"%llu",(long long unsigned int *)&val);
  if (ret != 1) {
    val=0;
    val--;
    return val;
  }    
  return (int64_t)val;    
}
/*
**_______________________________________________________________________
*/
/** Find out the export root path from its eid reading the configuration file
*   
    @param  eid : eport identifier
    
    @retval -the root path or null when no such eid
*/
char * get_export_root_path(uint8_t eid) {
  list_t          * e;
  export_config_t * econfig;

  list_for_each_forward(e, &exportd_config.exports) {

    econfig = list_entry(e, export_config_t, list);
    if (econfig->eid == eid) return econfig->root;   
  }
  return NULL;
}
/*
 *_______________________________________________________________________
 */
static void usage() {
    printf("Usage: ./rozo_du -e <eid> [-v] [-i <input_filename | fid>] [-c export_cfg_file] [-o output_path ]\n\n");
    printf("\t-h, --help\tprint this message.\n");
    printf("\t-e,--export <eid>\t\texportd identifier \n");
    printf("\t-i,--input:   fid of the objet or input filename \n");
    printf("\t-o,--output:   output path (optional, default is .) \n\n");
    printf("\t-c,--config:  exportd configuration file name (when different from %s)\n\n",configFileName);
    printf("\t-v,--verbose  Display some execution statistics\n");

};



int main(int argc, char *argv[]) {
    int c;
    int ret;
    void *rozofs_export_p;
    int i;
    char *root_path=NULL;
    char *input_path=NULL;   
    rozodu_verbose = 0;
//    FILE *fd_in = NULL;
    FILE *fd_out= NULL;
    rozodu_eid = -1;
    fid_t fid;
    int one_fid_only = 0;  
//    char fid_buf[64]; 
    char bufsize[64]; 

        
    static struct option long_options[] = {
        {"help", no_argument, 0, 'h'},
        {"input", required_argument, 0, 'i'},
        {"export", required_argument, 0, 'e'},
        {"verbose", required_argument, 0, 'v'},
        {"config", required_argument, 0, 'c'},	
        {"output", required_argument, 0, 'o'},	
        {0, 0, 0, 0}
    };
    
    rozodu_init();
    rozofs_project_init();
    ret = rozodu_bufout_create(&rozodu_outctx);
    if (ret < 0)
    {
       printf("Fatale error while creating output buffer\n");
       exit(EXIT_FAILURE);   
    }    
  
    while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "hvlrc:i:e:o:", long_options, &option_index);

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
          case 'o':
              rozodu_output_path = optarg;
              break;
          case 'v':
              rozodu_verbose = 1;
              break;    
          case 'e':
              rozodu_eid = (int)rozofs_scan_u64(optarg);
	      if (rozodu_eid < 0)
	      {
	         printf("bad eid value %s\n",optarg);
		 usage();
		 exit(EXIT_SUCCESS);
	      }
              break;  
         case 'i':
              input_path = optarg;
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
  ** check the presence of the mandatory parameters
  */
  if (rozodu_eid < 0)
  {
     printf("export identifier is missing (eid)\n");
     usage();
     exit(EXIT_FAILURE);     
  }   
#if 0
  if (input_path != NULL) 
  {
    /*
    ** check if the input is a FID or a filename
    */
    if (rozofs_uuid_parse(input_path, fid)<0) 
    {
      /*
      **  check the presence of the input file
      */    
      if ((fd_in = fopen(input_path,"r")) == NULL)
      {
	 TRACEOUT("not a valid file: %s: %s\n",input_path,strerror(errno));
	 usage();
	 exit(EXIT_FAILURE);      
      }          
    }
    else
    {
      one_fid_only = 1;
      strcpy(fid_buf,input_path);
    }
  }
#endif
  if (input_path != NULL)
  { 
     ret = rozofs_project_scan(input_path,rozodu_eid,rozodu_output_path);
     if (ret < 0)
     {
	 TRACEOUT("Error while scanning  %s: %s\n",input_path,(errno==0)?"  ":strerror(errno));
	 usage();
	 exit(EXIT_FAILURE);           
     
     }
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
       TRACEOUT("can't initialize exportd config %s.\n",strerror(errno));
       exit(EXIT_FAILURE);  
  }    
  if (econfig_read(&exportd_config, configFileName) != 0) {
       TRACEOUT("failed to parse configuration file %s %s.\n",configFileName,strerror(errno));
       exit(EXIT_FAILURE);  
  }
  /*
  ** Find the export root path
  */
  root_path = get_export_root_path(rozodu_eid);
  if (root_path==NULL) {
    TRACEOUT("eid %d is not configured\n",rozodu_eid);       
     usage();
     exit(EXIT_FAILURE);  	   	
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
  rz_set_verbose_mode(rozodu_verbose);
  rz_scan_all_inodes(rozofs_export_p,ROZOFS_DIR,1,rozofs_visit_dir,NULL,NULL,NULL);

  /*
  ** Check if the du is executed for projects
  */
  if (rozodu_verbose) printf("rozofs_project_count = %d status %d\n",rozofs_project_count,rozofs_is_project_enabled());
  if (rozofs_is_project_enabled())
  {
    if (rozodu_verbose) 
    {
       printf("Projetc tagging started\n");
    }
    if (rozodu_tag_apply_on_projects(rozofs_export_p)< 0)
    {
      printf("Error while tagging the directories\n");
      exit(EXIT_FAILURE);  
    }  
  }

//  rozo_display_all_cluster();

//  rozodu_check(rozofs_export_p);
  /*
  ** let's scan all the file inodes
  */
  rz_scan_all_inodes(rozofs_export_p,ROZOFS_REG,1,rozofs_visit,NULL,NULL,NULL);
  printf("nb Directories : %u\n",nb_dirs);
  printf("nb Files       : %llu\n",(unsigned long long int)nb_files);
  if (format_bytes)
    printf("total Bytes    :%llu\n",(unsigned long long int)total_bytes);
  else
    printf("total Bytes    :%s\n",display_size(total_bytes,bufsize));   
  printf("\n");
  
  if (rozofs_is_project_enabled()) rozodu_check_projects(rozofs_export_p);
  else 
  {
    printf("\nDirectories list:\n");
    rozodu_check(rozofs_export_p,one_fid_only,fid);
  }
  printf("\n");
  rozodu_bufout_reinit(&rozodu_outctx,NULL);
  rozodu_display_sorted_table("Big Directory count table",rozofs_export_p,rozodu_big_dir_count_table,rozodu_big_dir_count_table_sz,0);
  rozodu_display_sorted_table("Big Directory size table",rozofs_export_p,rozodu_big_dir_size_table,rozodu_big_dir_size_table_sz,1);
  rozodu_display_sorted_table("Big file size table",rozofs_export_p,rozodu_big_file_size_table,rozodu_big_file_table_sz,1);
  
  exit(EXIT_SUCCESS);  
  return 0;
}
