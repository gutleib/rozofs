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

#define DEFAULT_MOUNT "mnt1_1"

#define DEFAULT_NBFILE       30000

typedef enum _action_e {
  ACTION_NONE,
  ACTION_CREATE,
  ACTION_DELETE,  
  ACTION_CHECK
} action_e;
action_e action  = ACTION_NONE;
int      nbfiles = DEFAULT_NBFILE;
int      fNum    = -1;
char path[256];

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


static void usage() {
    printf("Parameters:\n");
    printf("[ -mount <mount> ]                     The mount point(default %s)\n", DEFAULT_MOUNT);
    printf("[ -nbfiles <NB> | -f <fileNumber>]     Number of files (default %d) or file number\n", DEFAULT_NBFILE);
    printf("[ -action <create|check|delete> ]      What to do with these files\n");
    exit(-100);
}

static void read_parameters(argc, argv)
int argc;
char *argv[];
{
    unsigned int idx;
    int ret;
    int val;
    char * mount = NULL;

    idx = 1;
    while (idx < argc) {

        /* -mount <mount>  */
        if (strcmp(argv[idx], "-mount") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx-1]);
                usage();
            }
            mount = argv[idx];
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
					
	/* -f <NB> */
        if (strcmp(argv[idx], "-f") == 0) {
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
	    fNum = val;
            idx++;
            continue;
        }				
        printf("Unexpected parameter %s\n", argv[idx]);
        usage();
    }
    
  if (mount == NULL) 
    sprintf(path,"%s/rebuild", DEFAULT_MOUNT);
  else      
    sprintf(path,"%s/rebuild", mount);
}
#define LOOP_NB  (257*3)
#define BLKSIZE (1024*4)
char    refblock[BLKSIZE];
char    readblock[BLKSIZE*LOOP_NB];
char    emptyBlock[BLKSIZE]={0};
void update_block(int b) {
  char string[64];
  int len;
  
  len = sprintf(string,"\n------------%8.8d--\n",b);
  memcpy(refblock,string,len);
  
}
void init_block() {
  int  idx;
  char car;

  car = '0';
  for (idx=0; idx<BLKSIZE; idx++) {
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
char path_file_name[256];
char * getfilename(int idx) {
  sprintf(path_file_name,"%d", idx);
  return path_file_name;
}
char * getemptyfilename(int idx) {
  sprintf(path_file_name,"empty%d", idx);
  return path_file_name;
}
int delete() {
  int idx;
  char * fname;

  for (idx=1; idx <= nbfiles; idx++) {
    fname = getfilename(idx); 
    unlink(fname);
  }        
}
int check() {
  int idx,loop;
  char * fname;
  int    fd=-1;
  int    ret;
  int    start,stop;
  
  if (fNum == -1) {
    start = 1;
    stop  = nbfiles;
  }
  else {
    start = fNum;
    stop  = fNum;
  }
  for (idx=start; idx <= stop; idx++) {

    fname = getfilename(idx); 
  	
    fd = open(fname, O_RDONLY);
    if (fd < 0) {
      printf("CHECK open(%s) %s\n", fname, strerror(errno));
      exit(-1);
    }

    ret = pread(fd, readblock, sizeof(readblock), 0);	
    if (ret < 0) {
      printf("CHECK pread(%s) %s\n", fname, strerror(errno));
      exit(-1);
    }
    
    ret = close(fd);
    if (ret < 0) { 	    
      printf("CHECK close(%s) %s\n", fname, strerror(errno));
    } 
      
    for (loop=0; loop < LOOP_NB; loop++) {
    
      update_block(loop);
      if ((loop % 10)==0) {
        if (memcmp(&readblock[loop*BLKSIZE],emptyBlock,BLKSIZE)!=0) {
	  printf("CHECK %s block %d is not empty.\n", fname, loop);
	  exit(-1);  
        }       
      }
      else if (memcmp(&readblock[loop*BLKSIZE],refblock,BLKSIZE)!=0) {
	printf("CHECK memcmp(%s) bad content block %d\n", fname, loop);
	exit(-1);  
      } 
    }     
  }  
  
  update_block(LOOP_NB*BLKSIZE);
  for (idx=1; idx< 16; idx++) {
    fname = getemptyfilename(idx); 
  	
    fd = open(fname, O_RDONLY);
    if (fd < 0) {
      printf("CHECK open(%s) %s\n", fname, strerror(errno));
      exit(-1);
    }

    ret = pread(fd, readblock, sizeof(readblock), 0);	
    if (ret < 0) {
      printf("CHECK pread(%s) %s\n", fname, strerror(errno));
      exit(-1);
    }
    for (loop=0; loop < LOOP_NB; loop++) {
    
      if (loop == LOOP_NB*BLKSIZE) {     
        if (memcmp(&readblock[loop*BLKSIZE],refblock,BLKSIZE)!=0) {
	  printf("CHECK memcmp(%s) bad content block %d\n", fname, loop);
	  exit(-1);  
        } 
      }
      else {
        if (memcmp(&readblock[loop*BLKSIZE],emptyBlock,BLKSIZE)!=0) {
	  printf("CHECK %s block %d is not empty.\n", fname, loop);
	  exit(-1);  
        }       
      }
    }         
  } 
}
int create() {
  int idx,loop;
  char * fname;
  int    fd=-1;
  int    ret;
  
  for (loop=0; loop < LOOP_NB; loop++) {
    for (idx=1; idx <= nbfiles; idx++) {

      fname = getfilename(idx); 
      update_block(loop);

      if (loop == 0) fd = open(fname, O_CREAT | O_TRUNC | O_WRONLY, 0640);
      else           fd = open(fname, O_WRONLY);
      if (fd < 0) {
	printf("CREATE open(%s) loop %d %s\n", fname,loop, strerror(errno));
	exit(-1);
      }
      if ((loop % 10)==0) {
        ret = pwrite(fd, emptyBlock, BLKSIZE, loop*BLKSIZE);      
      }
      else {
        ret = pwrite(fd, refblock, BLKSIZE, loop*BLKSIZE);
      }
      if (ret != BLKSIZE) {
	printf("CREATE write(%s) size %d offset %d %s\n", fname, BLKSIZE, loop, strerror(errno));
	exit(-1);
      }
      ret = close(fd);
      if (ret < 0) { 	    
	printf("CREATE close(%s) loop %d %s\n", fname,loop, strerror(errno));
	exit(-1);
      }
    }
  }

  update_block(LOOP_NB*BLKSIZE);
  for (idx=1; idx< 16; idx++) {
    fname = getemptyfilename(idx); 
    fd = open(fname, O_CREAT | O_TRUNC | O_WRONLY, 0640);
    if (fd < 0) {
      printf("CREATE open(%s) %s\n", fname, strerror(errno));
      exit(-1);
    }
    ret = pwrite(fd, refblock, BLKSIZE, LOOP_NB*BLKSIZE);
    if (ret != BLKSIZE) {
      printf("CREATE write(%s) size %d offset %d %s\n", fname, BLKSIZE, loop, strerror(errno));
      exit(-1);
    }
    close(fd);
  }
}  


int main(int argc, char **argv) {
    
  read_parameters(argc, argv);

  if (action == ACTION_NONE) {
    usage();
  }
  
  init_block();

  mkdir(path, 0640);
  chdir(path);
 
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
  }
   
  exit(0);
}
