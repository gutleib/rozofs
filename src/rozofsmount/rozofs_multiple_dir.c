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
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <getopt.h>
#include <stdlib.h>
#include <sys/types.h>
#include <attr/xattr.h>
#include <libgen.h>
#include <limits.h>
#include <rozofs/common/mattr.h>



/*
**_______________________________________________________________________
*/
/**
*  Get a size 
  supported formats:
   1/1b/1B (bytes)
   1k/1K (Kilobytes)
   1m/1M (Megabytes)  
   1g/1G (Gigabytes)  
   
   @param str: string that contains the value to convert
   @param value: pointer to the converted value
   
   @retval 0 on success
   @retval -1 on error (see errno for details)
*/
int get_size_value(char *str,uint64_t *value)
{
     uint64_t val64;
     int err = 0;
     char *s;
     
     s=str;

      val64 = strtoull(str, &str, 10);
      if (s == str || (val64 == ULONG_MAX && errno == ERANGE)) {
 	      err = 1;
      }
      if (err)
      {
        errno = EINVAL;
	return -1;
      }
      if (*str != 0)
      {
          while(1)
	  {
            if ((*str == 'k') || (*str == 'K'))
	    {
	       val64*=1024;
	       break;
            }
            if ((*str == 'm')|| (*str == 'M'))
	    {
	       val64*=(1024*1024);
	       break;
            }
	    err = 1;
	    break;
	  }
	  if ((str[1] == 'B') || (str[1] == 'b')) str++;	  
	  if (str[1] != 0) err=1;     
      }
      if (err )
      {
        errno=EINVAL;
	return -1;
      } 
      errno = 0;
      *value = val64;
      return 0;
 }

static void usage() {

printf ("\nRozoFS hybrid/striping configuration \n\n");
printf("  That tool enables to modify the hybrid/striping configuration at directory level.\n"); 
printf("  It has precedence over the striping configured at either volume or exportd level.\n\n"); 
printf("\nUsage: rozo_multifile [OPTIONS] path..\n\n");
printf (" Set configuration : rozo_multifile -s -m <striping_count>  [-u <striping-unit>] -f <0|1> [-z <hybrid_size>[k|K|m|M]] [-i] dir_1..dir_n\n");
printf (" Get configuration : rozo_multifile -g path_1..path_n\n");
printf("\nParameters:\n");
printf("  -s                    : To set the hybrid/striping configuration of a directory. The striping unit might be omitted if the <striping_count> is 1.\n");
printf("\n");
printf("  -m <striping_count>   : The <striping_count> is the number of RozoFS internal files on which the user file is striped.\n"); 
printf("                          It is applied at the creation time, and the value must be in the [1..8] range.\n");
printf("                          A <striping_count> of 1, indicates that there is no file striping.\n");
printf("\n");
printf(" -u <striping_unit>    : The value is given in power of 2 [0..7]. The default unit is 256KB. It permits to configure the maximum unit\n");
printf("                         size before using the next file according to the <striping_count> value. The value are:\n");
printf("                           - 0: 256KB\n");
printf("                           - 1: 512KB\n");
printf("                           - 2: 1MB\n");
printf("                           - 3: 2MB\n");
printf("                           - 4: 4MB\n");
printf("                           - 5: 8MB\n");
printf("                           - 6: 16MB\n");
printf("                           - 7: 32MB\n");
printf("                          The striping unit might be omitted when the <striping_count> is 1.\n");
printf("\n");
printf(" -f <0|1>              : that parameter indicates if the hybrid mode is enabled for the directory\n");
printf("                           - 0: disabled\n");
printf("                           - 1: enabled\n");
printf("                             When it is enabled, it might be possible to defined an <hybrid_size>. If that parameter is omitted, RozoFS uses\n");
printf("                           the size defined in the <striping_unit> for the hybrid section\n");
printf("\n");
printf(" -z <hybrid_size>      : When provided, that parameter gives the size of the hybrid section either in Bytes, KB or MB\n");
printf("                          The provided value is round up to the next %d KB boundary\n",ROZOFS_HYBRID_UNIT_BASE/1024);
printf("                          The maximun size of the hybrid section is  %d KB \n",(ROZOFS_HYBRID_UNIT_BASE/1024)*127);
printf("\n");
printf(" -i                    : When provided, it indicates that the child directory will inherit of the configuration of the parent directory\n");
printf(" -g                     : To get the current multifile configuration of either a directory or regular file\n");
printf(" -h                     : This message\n\n");

}


char *module_name;


/*
**__________________________________________________________________
*/
int set_multifile(char *dirpath,int striping_factor,int striping_unit,int redundancy,int hybrid,int hybrid_sz)
{
   char bufall[128];
   int ret;

   
   sprintf(bufall,"striping = %d,%d,%d,%d,%d",striping_factor,striping_unit,redundancy,hybrid,hybrid_sz);
   
   ret = setxattr(dirpath,"rozofs",bufall,strlen(bufall),0);
   if (ret < 0)
   {
      printf("Error while configuring multifile on directory %s: %s\n",dirpath,strerror(errno));
      return -1;
   }
   printf("configuring multifile on directory %s: %s\n",dirpath,strerror(errno)); 
   return ret;  
   
}

char bufall[4096];
/*
**__________________________________________________________________
*/
#define ROZOFS_MODE_UNSUPPORTED 0
#define ROZOFS_MODE_DIR 1
#define ROZOFS_MODE_REG 2

char bufout[1024];

int get_multifile(char *dirpath)
{

   int striping_factor = -1;
   int striping_unit =-1;
   int redundancy = -1;
   int hybrid = -1;
   int hybrid_sz = -1;
   int multi_f = -1;
   int ret;
   char *token;
   char *value_p;
   int i;  
   long long int factor_value;
   long long int unit_value;
   long long int hybrid_value;
   int striping = -1;
   int mode = -1;
   
   
   
   ret = getxattr(dirpath,"trusted.rozofs",bufall,4096);
   if (ret < 0)
   {
      printf("\n %s: %s\n",dirpath,strerror(errno));
      return -1;
   }
//   printf("getting multifile on directory %s: %s\n",dirpath,strerror(errno));   
//   printf("%s\n",bufall);

   token = strtok(bufall,"\n");
   i = 0;
   while (token != 0)
   {

     while(1)
     {
       if (mode == -1)
       {
	 ret = strncmp(token,"MODE    :",strlen("MODE    :"));
	 if (ret == 0) 
	 {
	   value_p = token+strlen("MODE    :");
           if (strcmp(value_p," DIRECTORY")== 0) {
             mode = ROZOFS_MODE_DIR;	
	     break;  		  
           }
           if (strcmp(value_p," REGULAR FILE")== 0) {
             mode = ROZOFS_MODE_REG;	
	     break;  		  
           }
	   mode = ROZOFS_MODE_UNSUPPORTED;
	   break;
	 }
       }       

       if (multi_f == -1)
       {
	 ret = strncmp(token,"MULTI_F : ",strlen("MULTI_F : "));
	 if (ret == 0) 
	 {
	   value_p = token+strlen("MULTI_F : ");
           if (strcmp(value_p,"Configured")== 0) {
             multi_f = 1;		  
           }
	   else multi_f = 0;
	   break;	   
	 }
       }    
       
       if (striping_factor == -1)
       {
	 ret = strncmp(token,"S_FACTOR:",strlen("S_FACTOR:"));
	 if (ret == 0) 
	 {
	   value_p = token+strlen("S_FACTOR:");
           if (sscanf(value_p,"%lld",(long long int *)&factor_value)!= 1) {
             printf("striping_factor conversion error: %s",value_p);	  		  
           }
	   else
	   {
	     striping_factor = factor_value-1;
	   }
	   break;
	 }
       }
       if (striping_unit == -1)
       {
	 ret = strncmp(token,"S_UNIT  :",strlen("S_UNIT  :"));
	 if (ret == 0) 
	 {
	   value_p = token+strlen("S_UNIT  :");
           if (sscanf(value_p,"%lld",(long long int *)&unit_value)!= 1) {
//             printf("striping_unit conversion error: %s",value_p);	  		  
           }
	   else
	   {
	     striping_unit = 0;
	     int count = unit_value/(1024*256);
	     while(count > 1)
	     {
	        striping_unit++;
		count = count/2;
             }
//	     printf("striping unit   :%d (%lld Bytes)\n",striping_unit,unit_value);
	   }
	   break;

	 }
       }

       if (hybrid_sz == -1)
       {
	 ret = strncmp(token,"S_HSIZE :",strlen("S_HSIZE :"));
	 if (ret == 0) 
	 {
	   value_p = token+strlen("S_HSIZE :");
           if (sscanf(value_p,"%lld",(long long int *)&hybrid_value)!= 1) {
//             printf("striping_unit conversion error: %s",value_p);	  		  
           }
	   else
	   {
	     hybrid_sz = hybrid_value/ROZOFS_HYBRID_UNIT_BASE;
//	     printf("hybrid_sz unit   :%d (%lld Bytes)\n",hybrid_sz,hybrid_value);
	   }
	   break;

	 }
       }

       if (hybrid == -1)
       {   
	 ret = strncmp(token,"S_HYBRID:",strlen("S_HYBRID:"));
	 if (ret == 0) 
	 {
	   value_p = token+strlen("S_HYBRID:");
	   if (strcmp(" Yes",value_p) == 0)
	   {
	     hybrid = 1;
	   }
	   else
	   {
	     hybrid = 0;
	   }  
//	   printf("hybrid: %s\n",hybrid==1?"Yes":"No");
	   break;	  
         } 
       }

       if (redundancy == -1)
       {   
	 ret = strncmp(token,"INHERIT :",strlen("INHERIT :"));
	 if (ret == 0) 
	 {
	   value_p = token+strlen("INHERIT :");
	   if (strcmp(" Yes",value_p) == 0)
	   {
	     redundancy = 1;
	   }
	   else
	   {
	     redundancy = 0;
	   }  
//	   printf("striping_inherit: %s\n",redundancy==1?"Yes":"No");
	   break;	  
         } 
       }
       if (striping == -1)
       {   

	 ret = strncmp(token,"STRIPING:",strlen("STRIPING:"));
	 if (ret == 0) 
	 {
	   striping = 0;
	   value_p = token+strlen("STRIPING:");
	   if (strcmp(" no striping forced",value_p) == 0)
	   {
	     striping_factor = 0;
	   }
//	   printf("STRIPING Found: %s\n",redundancy==1?"Yes":"No");
	   break;	  
         } 
       }
       break; 
     } 
     i++;
     token = strtok(NULL,"\n");
   } 
   /*
   **  printf out the result
   */
   if (mode == 0)
   {
      printf("\n %s: Unsupported mode\n",dirpath);
      return 0;
   }
   printf("\n %s:\n",dirpath);

   if (((striping_unit == -1) && (striping_factor == -1)) || (multi_f==0))
   {
      if (mode == ROZOFS_MODE_DIR)
      {
        printf("  - no custom striping for that directory (it follows either volume or export striping)\n");
	if (striping_factor == 0)
	{
          printf("  - no striping for that export\n");
	  return 0;	
	}
	printf("  - default striping factor :%d (%lld Files)\n",striping_factor,factor_value);  
	printf("  - default striping unit   :%d (%lld Bytes)\n",striping_unit,unit_value);	
	if (hybrid == 0)
	{
	  hybrid_sz = 0;
	  hybrid_value = 0;     
	}
	else
	{
	  if (hybrid_sz == 0)
	    hybrid_value = unit_value;        
	}
	printf("  - default hybrid size     :%lld Bytes\n",hybrid_value);
      }
      else
      {
        printf("  - no striping for that file\n");
      }
      return 0;
   }
   if (redundancy == -1) redundancy = 0;
   if (striping_factor == 0)
   {
      printf("  - no striping has been forced for that directory\n");
      printf("  - striping_inherit: %s\n",redundancy==1?"Yes":"No");
      if (mode == ROZOFS_MODE_DIR)
      {
	if (redundancy == 0)
	{
	  printf("    %s -s -m %d  %s\n",module_name,striping_factor+1,dirpath);
	}
	else
	{
	  printf("    %s -s -m %d -r %s\n",module_name,striping_factor+1,dirpath);   
	}
      }
      return 0;         
   }
   printf("  - striping count :%lld Files\n",factor_value);  
   printf("  - striping unit   :%d (%lld Bytes)\n",striping_unit,unit_value);
   if (hybrid == 0)
   {
     hybrid_sz = 0;
     hybrid_value = 0;     
   }
   else
   {
     if (hybrid_sz == 0)
       hybrid_value = unit_value;        
   }
   printf("  - hybrid size     :%lld Bytes\n",hybrid_value);
   if (mode == ROZOFS_MODE_DIR) printf("  - striping_inherit: %s\n",redundancy==1?"Yes":"No");
   if (mode == ROZOFS_MODE_DIR)
   {

     if (hybrid == 0)
     {
       printf("    %s -s -m %d -u %d -f 0 %s %s\n",module_name,striping_factor+1,striping_unit,(redundancy == 0)?" ":"-i",dirpath);
       return 0;
     }
     if (hybrid_sz == 0)
     {
       printf("    %s -s -m %d -u %d -f 1 %s %s\n",module_name,striping_factor+1,striping_unit,(redundancy == 0)?" ":"-i",dirpath);
       return 0;              
     }
     printf("    %s -s -m %d -u %d -f 1 -z %dK %s %s\n",module_name,striping_factor+1,striping_unit,(int)(hybrid_value/1024),(redundancy == 0)?" ":"-i",dirpath);
     return 0;

   }
   return 0;


}


/*
**__________________________________________________________________
*/
int main(int argc, char **argv) {
    int c;
    int i;

    static struct option long_options[] = {
        {"--help", no_argument, 0, 'h'},
        {"--set", no_argument, 0, 's'},
        {"--get", no_argument, 0, 'g'},
        {"--multi", required_argument, 0, 'm'},
        {"--unit", required_argument, 0, 'u'},
        {"--inherit", no_argument, 0, 'i'},
        {"--hybrid", required_argument, 0, 'f'},
        {"--hsize", required_argument, 0, 'z'},
	{0, 0, 0, 0}
    };
    int striping_factor = -1;
    int striping_unit = -1;
    int inherit = 0;
    int hybrid = -1;
    int hybrid_size = 0;
    int getvalue = 0;
    int setvalue = 0;
    int dir_count = 0;
    uint64_t hybrid_size_bytes;

    module_name = basename(argv[0]);
    while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "hsgm:u:if:z:", long_options, &option_index);

      if (c == -1)
          break;
      switch (c) {
          case 'h':
              usage();
              exit(EXIT_SUCCESS);
              break;      
          case 'm':
            if (sscanf(optarg,"%d",&striping_factor)!= 1) {
              printf("Bad striping factor: %s\n",optarg);	
              usage();
              exit(EXIT_FAILURE);			  
            }  
	    if ((striping_factor < 1) || (striping_factor > 8))
	    {
              printf("Out of range striping count: %s (must be in the [1..7] range)\n",optarg);	
              usage();
              exit(EXIT_FAILURE);			  
            } 
	    striping_factor--; 	      	                
	    break;
          case 'u':
            if (sscanf(optarg,"%d",&striping_unit)!= 1) {
              printf("Bad striping unit: %s\n",optarg);	
              usage();
              exit(EXIT_FAILURE);			  
            }  
	    if ((striping_unit < 0) || (striping_unit > 7))
	    {
              printf("Out of range striping unit: %s (must be in the [0..7] range)\n",optarg);	
              usage();
              exit(EXIT_FAILURE);			  
            }  	      	                
	    break;
          case 'i':
              inherit = 1;
              break;   

          case 'f':
            if (sscanf(optarg,"%d",&hybrid)!= 1) {
              printf("Bad value for hybrid mode (-f): %s\n",optarg);	
              usage();
              exit(EXIT_FAILURE);			  
            }  
	    if (hybrid > 1)
	    {
              printf("Out of range value for hybrid mode (-f): %s (must be either 0(disable) or 1(enable))\n",optarg);	
              usage();
              exit(EXIT_FAILURE);			  
            }  	      	                
	    break;

          case 'z':
	      if (get_size_value(optarg,&hybrid_size_bytes) < 0)
	      {
	         printf("Hybrid section size must be given either in KB (k or K) or MB (m or M)\n");
        	 usage();
        	 exit(EXIT_FAILURE);			  
              }  	      	                
              /*
	      ** translation the hybrid size section
	      */
	      hybrid_size = hybrid_size_bytes/ROZOFS_HYBRID_UNIT_BASE;
	      if (hybrid_size == 0) hybrid_size+=1;
	      if (hybrid_size > 127) hybrid_size = 127;
	      
              break;   
          case 'g':
              getvalue = 1;
              break;   
          case 's':
              setvalue = 1;
              break; 
      }
    }
    /*
    ** Check the parameters
    */
    if ((getvalue) && (setvalue))
    {
      printf("Error Set and Get are exclusive\n");
      usage();
      exit(EXIT_FAILURE);			  
    }
    if ( setvalue)
    {
       /*
       ** check striping factor and unit
       */
       if ( striping_factor == -1)
       {
         printf(" striping_count is missing\n");     
	 usage();
	 exit(EXIT_FAILURE);			  
       }
       /*
       ** no striping unit if the striping factor is 0
       */
       if (striping_factor == 0) 
       {
          striping_unit = 0;
	  hybrid = 0;
	  hybrid_size = 0;
       }
       if ( striping_unit == -1)
       {
         printf(" striping_unit is missing\n");     
	 usage();
	 exit(EXIT_FAILURE);			  
       }
       if ( hybrid == -1)
       {
         printf(" hybrid mode is missing\n");     
	 usage();
	 exit(EXIT_FAILURE);			  
       }
       if ( hybrid == 0) hybrid_size = 0;
    }
    dir_count = argc -optind;
    if (dir_count == 0)
    {
      printf("Directory is missing\n");
      usage();
      exit(EXIT_FAILURE);	      
    }
    if (setvalue)
    {
      for (i = 0;i < dir_count;i++)
	set_multifile(argv[optind+i],striping_factor,striping_unit,inherit,hybrid,hybrid_size);
    }
    else
    {
      for (i = 0;i < dir_count;i++)
	get_multifile(argv[optind+i]);
	printf("\n");
    
    }
    return 0;
}
