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
#include "econfig.h"

int rozofs_no_site_file = 0;
lv2_cache_t cache;
econfig_t exportd_config;
/*
** Get the default config file name
*/
char * configFileName = EXPORTD_DEFAULT_CONFIG;

/*
 *_______________________________________________________________________
 */
/**
*  API to get the full pathname of the objet

   @param export : pointer to the export structure
   @param inode_attr_p : pointer to the inode attribute
   @param buf: output buffer
   @param fidType: the type of the fid
   
   @retval buf: pointer to the beginning of the outbuffer
*/

char *rozo_get_full_path(void *exportd,void *inode_p,char *buf,int lenmax, char * fidType)
{
   lv2_entry_t *plv2;
   char name[1024];
   char *pbuf = buf;
   int name_len=0;
   int first=1;
   char buf_fid[64];

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
      rozolib_get_fname(inode_attr_p->s.attrs.fid,(void*)e,name,(void*)&inode_attr_p->s.fname,inode_attr_p->s.pfid);
      name_len = strlen(name);
      if (name_len == 0) 
      {
         /*
	 ** put the value of the i-node
	 */
	 uuid_unparse(inode_attr_p->s.attrs.fid,buf_fid);
	 printf(name,"@rozofs_uuid@%s",buf_fid); 	 
         name_len = strlen(name);      
      }
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
 /**
*   That function is intended to return the relative path to an object:
    @rozofs_uuid@<FID_parent>/<child_name>
    example:
    @rozofs_uuid@1b4e28ba-2fa1-11d2-883f-0016d3cca427
    
    @param exportd: pointer to exportd data structure
    @param inode_p: pointer to the inode
    @param buf : pointer to the output buffer
    @param lenmax: max length of the output buffer
    @param fidType: the type of the fid
*/
char *rozo_get_relative_path(void *exportd,void *inode_p,char *buf,int lenmax,char * fidType)
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
   rozolib_get_fname(inode_attr_p->s.attrs.fid,e,name,&inode_attr_p->s.fname,inode_attr_p->s.pfid);
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

#define TRACEOUT(fmt, ...) { \
  if (fd_out==NULL) printf(fmt, ##__VA_ARGS__);\
  else              fprintf(fd_out,fmt, ##__VA_ARGS__);\
}

#define TRACEFIDPATH(path) { \
     if (!first) {\
       TRACEOUT(",\n");\
     } \
     first = 0;\
     TRACEOUT("    {\"FID\" : \"@rozofs_uuid@%s\", \"eid\" : %d, \"type\" : \"%s\", \"path\" : \"%s\"}",fid_buf,current_eid,fidType,path);\
}
#define TRACEFIDERR(fmt, ...) { \
     if (!first) {\
       TRACEOUT(",\n");\
     } \
     first = 0;\
     TRACEOUT("    {\"FID\" : \"@rozofs_uuid@%s\", \"eid\" : %d, \"type\" : \"%s\", \"path\" : \"\", \"error\" : \"",fid_buf,current_eid,fidType);\
     TRACEOUT(fmt,##__VA_ARGS__);\
     TRACEOUT("\" }");\
}
          		 
/*
 *_______________________________________________________________________
 */
static void usage() {
    printf("\nUsage: rozo_fid2pathname [-r] -i <input_filename | fid> [-o output_filename] [-c export_cfg_file]\n\n");
    printf("Options:\n");
    printf("\t-h,--help:     print this message.\n");
    printf("\t-r,--relative: when asserted the output is generated with @rozofs_uuid@<FID_parent>/<child_name> format  \n");
    printf("\t-i,--input:    fid of the objet or input filename \n");
    printf("\t               that contains a list of fid in the 1b4e28ba-2fa1-11d2-883f-0016d3cca427 format \n");
    printf("\t-o,--output:   output filename for full path translation (optional) \n\n");
    printf("\t-c,--config:   exportd configuration file name (when different from %s)\n\n",configFileName);

};


eid_t     eid_done[EXPGW_EID_MAX_IDX] = {0};


int main(int argc, char *argv[]) {
    int c;
    void *rozofs_export_p;
    char *input_path=NULL;
    char *output_path=NULL;
    fid_t fid;
    int one_fid_only = 0;
    lv2_entry_t *plv2;
    int res;
    int count;
    FILE *fd_in = NULL;
    FILE *fd_out= NULL;
    char name[4096];
    char *pbuf;  
    char fid_buf[64]; 
    export_t *e = NULL;    
    int relative= 0;
    rozofs_inode_t *inode_val_p;
    char    * root_path;
    int       more;
    int       current_eid;
    int       first=1;
    char      fidType[128];
      
    static struct option long_options[] = {
        {"help", no_argument, 0, 'h'},
        {"input", required_argument, 0, 'i'},
        {"output", required_argument, 0, 'o'},
        {"relative", no_argument, 0, 'r'},
        {"config", required_argument, 0, 'c'},	
        {0, 0, 0, 0}
    };
    
  
    while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "hlrc:i:o:c:r", long_options, &option_index);

      if (c == -1)
          break;

      switch (c) {

          case 'h':
              usage();
              exit(EXIT_SUCCESS);
              break;
          case 'r':
              relative = 1;
              break;
         case 'i':
              input_path = optarg;
              break;
          case 'o':
              output_path = optarg;
              break;
          case 'c':
              configFileName = optarg;
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

  if (input_path == NULL) 
  {
       TRACEOUT("Neither an valid input file nor valid FID\n");
       usage();
       exit(EXIT_FAILURE);  
  }
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
  /*
  ** Check for ouput file
  */
  if (output_path != NULL)
  {
    if ((fd_out = fopen(output_path,"w")) == NULL)
    {
       TRACEOUT("fopen(%s) %s\n",input_path,strerror(errno));
       usage();
       exit(EXIT_FAILURE);      
    }       
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
  ** init of the RozoFS data structure on export
  ** in order to permit the scanning of the exportd
  */


  count = 0;  
  current_eid = 0;
  more = 0;
  rozofs_export_p = NULL;
  TRACEOUT("{ \"FID list\" : [\n");   
  while (1) {

     /*
     ** Get next FID
     */
     if (one_fid_only){
       /*
       ** Not to reloop
       */
       if (more) break;
       more = 1;
       res  = 0;
     }     
     else {
       res = fscanf(fd_in,"%s",fid_buf); 
     }
  
     /*
     ** End of file
     */
     if (res == EOF) {
       /*
       ** Current eid is done
       */
       eid_done[current_eid] = 1;
       /*
       ** Free previous export resources
       */
       if (rozofs_export_p != NULL) {
	 free(rozofs_export_p);
	 lv2_cache_release (&cache); 
	 rozofs_export_p = NULL;
       }
       /*
       ** Is there more eid to process ? 
       */
       if (more==0) break; // No. We are done.

       /*
       ** Rewind the file to the beginning
       */
       fclose(fd_in);
       if ((fd_in = fopen(input_path,"r")) < 0)
       {
	  TRACEFIDERR("re fopen(%s) %s",input_path,strerror(errno));
	  exit(EXIT_FAILURE);      
       }
       /*
       ** Read the first FID
       */
       res = fscanf(fd_in,"%s",fid_buf); 
       if (res < 0) {
	  TRACEFIDERR("Can not re-read file %s %s\n",input_path,strerror(errno)); 
	  break;         
       } 
       more        = 0;
       current_eid = 0;         
     }  
  
     /*
     ** Parse the fid
     */
     if (rozofs_uuid_parse(fid_buf, fid)<0) 
     {
       TRACEFIDERR("cannot be translated");       
       continue;
     }

     /*
     ** Extract the eid from the FID
     */       
     inode_val_p = (rozofs_inode_t *) fid;

     /*
     ** 1rst FID encountered
     */
     if (current_eid == 0) {
     
       current_eid = inode_val_p->s.eid;
       if (eid_done[current_eid] == 1) {
         /*
	 ** This eid has already been processed
	 */
	 current_eid = 0;
	 continue;
       }
       	 
       /*
       ** Find the export root path
       */
       root_path = get_export_root_path(current_eid);
       if (root_path==NULL) {
         TRACEFIDERR("eid %d is not configured",current_eid);       
	 current_eid = 0;
	 continue;	   	
       }

       rozofs_export_p = rz_inode_lib_init(root_path);
       e =  rozofs_export_p;  
       if (rozofs_export_p == NULL) {
         TRACEFIDERR("error while reading eid %d path %s",current_eid,root_path);       	 
	 exit(EXIT_FAILURE);  
       }
       /*
       ** init of the lv2 cache
       */
       lv2_cache_initialize(&cache);	 
     }

     /*
     ** This is not the current eid
     ** We will process the FID later if not already done
     */
     if (inode_val_p->s.eid != current_eid) {
       if (eid_done[inode_val_p->s.eid] == 0) {
         more = 1;
       }	 
       continue;
     }	  
       
     /*
     ** Get the attributes of the file
     */
     if (!(plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,&cache, fid))) {
       TRACEFIDERR("No such file or directory");
       continue;
     }
          
     switch(plv2->attributes.s.attrs.mode & S_IFMT) {
       case S_IFLNK:
         sprintf(fidType,"symlink");
         break;
       case S_IFDIR:
         sprintf(fidType,"directory");
         break;
       case S_IFREG:
         /*
         ** Case of multifile mode
         */
         if (plv2->attributes.s.multi_desc.byte == 0) {
           sprintf(fidType,"single file");
         }
         else {
            if (plv2->attributes.s.multi_desc.common.master == 1) {
              if (plv2->attributes.s.multi_desc.master.hybrid == 1) {
                sprintf(fidType,"master hybrid file");
              }
              else {
                sprintf(fidType,"master inode file");                
              }  
            }
            /*
            ** Slave inode case. get the master inode
            */
            else {
              inode_val_p->s.idx -= (plv2->attributes.s.multi_desc.slave.file_idx+1);
              sprintf(fidType,"slave inode %d", plv2->attributes.s.multi_desc.slave.file_idx+1);                
              if (!(plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,&cache, fid))) {
                TRACEFIDERR("No such file or directory");
                continue;
              }          
            }
         }       
         break;
       default:
         sprintf(fidType,"?? 0x%x ??",plv2->attributes.s.attrs.mode & S_IFMT);  
     }
         

     /*
     ** Get the full pathname
     */
     count++;
     if (relative)
       pbuf = rozo_get_relative_path(rozofs_export_p,&plv2->attributes,name,4096, fidType);
     else
       pbuf = rozo_get_full_path(rozofs_export_p,&plv2->attributes,name,4096,fidType);
     TRACEFIDPATH(pbuf);  	
  }
  TRACEOUT("\n  ]\n}\n");   
  if (fd_out) fclose(fd_out);
  if (fd_in) fclose(fd_in);
  
  return 0;
}
