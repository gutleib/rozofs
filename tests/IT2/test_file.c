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
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <ctype.h>
#include <sys/wait.h>


typedef enum _action_e {
  ACTION_NONE,
  ACTION_CREATE,
  ACTION_DELETE,  
  ACTION_CHECK
} action_e;

action_e action  = ACTION_NONE;
int      nbfiles = -1;
char   * fullpath = NULL;
int      size = -1;
int      blocksize = 50000;


char    * refblock;
char    * readblock;

/*_____________________________________________________________________
*/
#define HEXDUMP_COLS 16
void hexdump(void *mem, unsigned int offset, unsigned int len)
{
        unsigned int i, j;
        
        for(i = 0; i < len + ((len % HEXDUMP_COLS) ? (HEXDUMP_COLS - len % HEXDUMP_COLS) : 0); i++)
        {
                /* print offset */
                if(i % HEXDUMP_COLS == 0)
                {
                        printf("0x%06x: ", i+offset);
                }
 
                /* print hex data */
                if(i < len)
                {
                        printf("%02x ", 0xFF & ((char*)mem)[i+offset]);
                }
                else /* end of block, just aligning for ASCII dump */
                {
                        printf("   ");
                }
                
                /* print ASCII dump */
                if(i % HEXDUMP_COLS == (HEXDUMP_COLS - 1))
                {
                        for(j = i - (HEXDUMP_COLS - 1); j <= i; j++)
                        {
                                if(j >= len) /* end of block, not really printing */
                                {
                                        putchar(' ');
                                }
                                else if(isprint(((char*)mem)[j+offset])) /* printable char */
                                {
                                        putchar(0xFF & ((char*)mem)[j+offset]);        
                                }
                                else /* other char */
                                {
                                        putchar('.');
                                }
                        }
                        putchar('\n');
                }
        }
}

/*_____________________________________________________________________
*/
static void usage() {
    printf("Parameters:\n");
    printf(" -fullpath <full path>                The full path\n");
    printf(" -nbfiles <NB> ]                      Number of files\n");
    printf(" -size <sz> ]                         size of the file\n");
    printf(" -blocksize <sz> ]                    Block size to use to write\n", blocksize);
    printf(" -action <create|check|delete>        What to do with these files\n");
    exit(-100);
}
/*_____________________________________________________________________
*/
static void read_parameters(argc, argv)
int argc;
char *argv[];
{
    unsigned int idx;
    int ret;
    int val;

    fullpath = NULL;

    idx = 1;
    while (idx < argc) {

        /* -fullpath <fullpath>  */
        if (strcmp(argv[idx], "-fullpath") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx-1]);
                usage();
            }
            fullpath = argv[idx];
            idx++;
            continue;
        }	
	
        /* -action <create|check|delete>  */
        if (strcmp(argv[idx], "-action") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx-1]);
                usage();
            }
            if      (strcmp(argv[idx],"create")==0) action = ACTION_CREATE;
	    else if (strcmp(argv[idx],"delete")==0) action = ACTION_DELETE;
	    else if (strcmp(argv[idx],"check")==0)  action = ACTION_CHECK;
	    else {
              printf("%s option but bad value \"%s\"!!!\n", argv[idx-1], argv[idx]);
	      usage();	      
	    }
            idx++;
            continue;
        }
					
	/* -nbfiles <NB> */
        if (strcmp(argv[idx], "-nbfiles") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx-1]);
                usage();
            }
            ret = sscanf(argv[idx], "%u", &val);
            if (ret != 1) {
                printf("%s option but bad value \"%s\"!!!\n", argv[idx-1], argv[idx]);
                usage();
            }
	    nbfiles = val;
            idx++;
            continue;
        }	
					
					
	/* -nbfiles <NB> */
        if (strcmp(argv[idx], "-size") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx-1]);
                usage();
            }
            ret = sscanf(argv[idx], "%u", &val);
            if (ret != 1) {
                printf("%s option but bad value \"%s\"!!!\n", argv[idx-1], argv[idx]);
                usage();
            }
	    size = val;
            idx++;
            continue;
        }
        	
	/* -nbfiles <NB> */
        if (strcmp(argv[idx], "-blocksize") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx-1]);
                usage();
            }
            ret = sscanf(argv[idx], "%u", &val);
            if (ret != 1) {
                printf("%s option but bad value \"%s\"!!!\n", argv[idx-1], argv[idx]);
                usage();
            }
	    blocksize = val;
            idx++;
            continue;
        }        		
        printf("Unexpected parameter %s\n", argv[idx]);
        usage();
    }
    
  if (fullpath == NULL) {
    printf("Missing -fullpath");
    usage();
  }
  if (nbfiles == -1) {
    printf("Missing -nbfiles");
    usage();
  }
  if (action == ACTION_NONE){
    printf("Missing -action");
    usage();
  } 
  if (size == -1){
    printf("Missing -size");
    usage();
  }  
  
  refblock = malloc(blocksize);   
}
/*_____________________________________________________________________
*/
void update_block(int f, int b) {
  char string[64];
  int len;
  
  len = sprintf(string,"\n--file %4d--block %8.8d--\n",f,b);
  memcpy(refblock,string,len);
  
}
/*_____________________________________________________________________
*/
void init_block() {
  int  idx;
  char car;

  car = '0';
  for (idx=0; idx<blocksize; idx++) {
    refblock[idx] = car;
    switch(car) {
      case '9': car = 'a'; break;
      case 'z': car = 'A'; break;
      case 'Z': car = '0'; break;  
      default:
      car++;
    }    
  }
}
/*_____________________________________________________________________
*/
char path_file_name[256];
char * getfilename(int idx) {
  sprintf(path_file_name,"%d", idx);
  return path_file_name;
}
/*_____________________________________________________________________
*/
int delete() {
  int idx;
  char * fname;
  
  for (idx=1; idx <= nbfiles; idx++) {

    fname = getfilename(idx);   
    unlink(fname);
  } 
  exit(0);       
}
/*_____________________________________________________________________
*/
int check() {
  int idx,loop;
  char * fname;
  int    fd=-1;
  int    ret;
  int    toread;  
  int    remaining;
  uint64_t offset;  
  
  readblock = malloc(blocksize);
  for (idx=1; idx <= nbfiles; idx++) {
    
    fname = getfilename(idx); 
  	
    fd = open(fname, O_RDONLY);
    if (fd < 0) {
      printf("CHECK open(%s) %s\n", fname, strerror(errno));
      exit(-1);
    }
    
    remaining = size;
    loop      = 0;
    offset    = 0;
    while (remaining) {
    
      loop++;
      update_block(idx,loop);
    
      if (remaining >= blocksize) toread = blocksize;
      else                        toread = remaining;

      ret = pread(fd, readblock, toread, offset); 
      if (ret < 0) {
        printf("CHECK pread(%s) size %d offset %d %s\n", fname, toread, offset, strerror(errno));          
        exit(-1);
      }

      update_block(idx,loop);
      
      if (memcmp(readblock,refblock,ret)!=0) {
	printf("CHECK memcmp(%s) bad content block %d\n", fname, loop);
	exit(-1);  
      } 
      
      offset    += ret;
      remaining -= ret;
    }  
    
    ret = close(fd);
    if (ret < 0) { 	    
      printf("CHECK close(%s) %s\n", fname, strerror(errno));
      exit(-1);
    } 
  }  
  exit(0);      
}
/*_____________________________________________________________________
*/
int create() {
  int idx,loop;
  char * fname;
  int    fd=-1;
  int    ret;
  int    remaining;
  int    towrite;
  uint64_t offset;
  

  for (idx=1; idx <= nbfiles; idx++) {
    
    fname = getfilename(idx); 

    fd = open(fname,O_CREAT | O_TRUNC | O_WRONLY, 0640);
    if (fd < 0) {
      printf("CREATE open(%s) %s",fname, strerror(errno));
      exit(-1);
    }  
    
    remaining = size;
    loop      = 0;
    offset    = 0;
    while (remaining) {
    
      loop++;
      update_block(idx,loop);
    
      if (remaining >= blocksize) towrite = blocksize;
      else                        towrite = remaining;
      
      ret = pwrite(fd, refblock, towrite, offset);
      if (ret != towrite) {
        printf("CREATE write(%s) size %d offset %d %s\n", fname, towrite, offset, strerror(errno));  
        exit(-1);
      }
      offset    += towrite;
      remaining -= towrite;
    }  
    
    ret = close(fd);
    if (ret < 0) { 	    
      printf("CREATE close(%s) %s\n", fname, strerror(errno));
      exit(-1);
    }    
  } 
  exit(0);  
}
/*_____________________________________________________________________
*/
int main(int argc, char **argv) {
    
  read_parameters(argc, argv);
  
  init_block();

  if (access(fullpath,W_OK) < 0) {
    mkdir(fullpath, 0640);
  }  
  chdir(fullpath);
 
  switch(action) {

    case ACTION_CREATE:  
      create();
      break;

    case ACTION_DELETE: 
      delete();
      break;

    case ACTION_CHECK:
      check();
      break;
      
    default:
      usage();
      exit(-1);  
  }
  
  exit(0);
}
