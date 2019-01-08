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

static void usage() {

printf ("RozoFS multifile configuration \n");
printf("\nUsage: rozo_multifile [OPTIONS] path..\n\n");
printf (" Set configuration : rozo_multifile -s -f <striping_factor>  [-u <striping-unit>] [-r] dir_1..dir_n\n");
printf (" Get configuration : rozo_multifile -g path_1..path_n\n");
printf("\nParameters:\n");
printf("  -s                   : To set the multiple configuration of a directory. The striping unit might be omitted if the striping factor is 0.\n");
printf("  -r <striping_factor> : The striping factor is given in power of 2 [0..4]. It defines the number of files that will be allocated\n");
printf("                         at the file creation time.\n");
printf("                         A striping factor of 0, indicates that there will not be not file striping. It has precedence over the\n"); 
printf("                         striping configured at either volume or exportd level.\n");
printf(" -u <striping_unit>    : The value is given in power of 2 [0..3]. The default unit is 256KB. It permits to configure the maximum unit\n");
printf("                         size before using the next file according to the striping factor. The value are:\n");
printf("                           - 0:256KB\n");
printf("                           - 1:512KB\n");
printf("                           - 2:1MB\n");
printf("                           - 3:2MB\n");
printf("                          The striping unit might be omitted when the striping_factor is 0.\n");
printf(" -r                     : When provided, it indicates that the child directory will inherit of the configuration of the parent directory\n");
printf(" -g                     : To get the current multifile configuration of either a directory or regular file\n");
printf(" -h                     : This message\n\n");

}


char *module_name;


/*
**__________________________________________________________________
*/
int set_multifile(char *dirpath,int striping_factor,int striping_unit,int redundancy)
{
   char bufall[128];
   int ret;

   
   sprintf(bufall,"striping = %d,%d,%d",striping_factor,striping_unit,redundancy);
   
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

int get_multifile(char *dirpath)
{

   int striping_factor = -1;
   int striping_unit =-1;
   int redundancy = -1;
   int ret;
   char *token;
   char *value_p;
   int i;  
   long long int factor_value;
   long long int unit_value;
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
	     striping_factor = 0;
	     int count = factor_value;
	     while(count > 1)
	     {
	        striping_factor++;
		count = count/2;
             }
//	     printf("striping factor :%d (%lld Files)\n",striping_factor,factor_value);
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
   if ((striping_unit == -1) && (striping_factor == -1))
   {
      if (mode == ROZOFS_MODE_DIR)
      {
        printf("  - no custom striping for that directory (it follows either volume or export striping)\n");
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
	  printf("    %s -s -f %d  %s\n",module_name,striping_factor,dirpath);
	}
	else
	{
	  printf("    %s -s -f %d -r %s\n",module_name,striping_factor,dirpath);   
	}
      }
      return 0;         
   }
   printf("  - striping factor :%d (%lld Files)\n",striping_factor,factor_value);  
   printf("  - striping unit   :%d (%lld Bytes)\n",striping_unit,unit_value);
   if (mode == ROZOFS_MODE_DIR) printf("  - striping_inherit: %s\n",redundancy==1?"Yes":"No");
   if (mode == ROZOFS_MODE_DIR)
   {
     if (redundancy == 0)
     {
       printf("    %s -s -f %d -u %d %s\n",module_name,striping_factor,striping_unit,dirpath);
     }
     else
     {
       printf("    %s -s -f %d -u %d -r %s\n",module_name,striping_factor,striping_unit,dirpath);   
     }
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
        {"help", no_argument, 0, 'h'},
        {"set", no_argument, 0, 's'},
        {"get", no_argument, 0, 'g'},
        {"--factor", required_argument, 0, 'f'},
        {"--unit", required_argument, 0, 'u'},
        {"--inherit", no_argument, 0, 'r'},
        {0, 0, 0, 0}
    };
    int striping_factor = -1;
    int striping_unit = -1;
    int inherit = 0;
    int getvalue = 0;
    int setvalue = 0;
    int dir_count = 0;

    module_name = basename(argv[0]);
    while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "hsgf:u:r", long_options, &option_index);

      if (c == -1)
          break;

      switch (c) {
          case 'h':
              usage();
              exit(EXIT_SUCCESS);
              break;      
          case 'f':
            if (sscanf(optarg,"%d",&striping_factor)!= 1) {
              printf("Bad striping factor: %s\n",optarg);	
              usage();
              exit(EXIT_FAILURE);			  
            }  
	    if ((striping_factor < 0) || (striping_factor > 4))
	    {
              printf("Out of range striping factor: %s (must be in the [0..4] range)\n",optarg);	
              usage();
              exit(EXIT_FAILURE);			  
            }  	      	                
	    break;
          case 'u':
            if (sscanf(optarg,"%d",&striping_unit)!= 1) {
              printf("Bad striping factor: %s\n",optarg);	
              usage();
              exit(EXIT_FAILURE);			  
            }  
	    if ((striping_unit < 0) || (striping_unit > 3))
	    {
              printf("Out of range striping unit: %s (must be in the [0..3] range)\n",optarg);	
              usage();
              exit(EXIT_FAILURE);			  
            }  	      	                
	    break;
          case 'r':
              inherit = 1;
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
         printf(" striping_factor is missing\n");     
	 usage();
	 exit(EXIT_FAILURE);			  
       }
       /*
       ** no striping unit of the striping factor is 0
       */
       if (striping_factor == 0) striping_unit = 0;
       if ( striping_unit == -1)
       {
         printf(" striping_unit is missing\n");     
	 usage();
	 exit(EXIT_FAILURE);			  
       }


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
	set_multifile(argv[optind+i],striping_factor,striping_unit,inherit);
    }
    else
    {
      for (i = 0;i < dir_count;i++)
	get_multifile(argv[optind+i]);
	printf("\n");
    
    }
    return 0;
}
