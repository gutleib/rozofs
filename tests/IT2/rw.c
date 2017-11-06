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


#define DEFAULT_MNT         "mnt1_1"
#define DEFAULT_FILENAME    "this_is_the_default_rw_test_file_name"
#define DEFAULT_NB_PROCESS    20
#define DEFAULT_LOOP         200
#define DEFAULT_FILE_SIZE_MB   1

#define RANDOM_BLOCK_SIZE      (9*1024)
#define READ_BUFFER_SIZE       (RANDOM_BLOCK_SIZE+file_mb)

#define ERROR(...) printf("%d %s - ", __LINE__,filename); printf(__VA_ARGS__)


int shmid;
#define SHARE_MEM_NB 7538

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


char FILENAME[500];

int nbProcess       = DEFAULT_NB_PROCESS;
int loop=DEFAULT_LOOP;

typedef enum _CHECK_MODE_E {
  CHECK_MODE_TOTAL,
  CHECK_MODE_PARTIAL,
  CHECK_MODE_RANDOM
}  CHECK_MODE_E;
CHECK_MODE_E  ckeck_mode = CHECK_MODE_TOTAL;  

long long unsigned int file_mb=DEFAULT_FILE_SIZE_MB*1000000;

int closeBetween = 0;
int closeAfter = 0;
int * result;

static void usage() {
    printf("Parameters:\n");
    printf("[ -mount <mount> ]         The mount point (default %s)\n", DEFAULT_MNT);
    printf("[ -file <name> ]           file to do the test on (default %s)\n", DEFAULT_FILENAME);
    printf("[ -process <nb> ]          The test will be done by <nb> process simultaneously (default %d)\n", DEFAULT_NB_PROCESS);
    printf("[ -loop <nb> ]             <nb> test operations will be done (default %d)\n",DEFAULT_LOOP);
    printf("[ -fileSize <MB> ]         file size in MB (default %d)\n",DEFAULT_FILE_SIZE_MB);
    printf("[ -closeAfter ]            Close the file after each loop\n");
    printf("[ -closeBetween ]          Close the file between write and read\n");
    printf("[-total|-partial|-random]  Re-read and check all the file, only the written part or random parts\n");
    exit(-100);
}

static void read_parameters(argc, argv)
int argc;
char *argv[];
{
    unsigned int idx;
    int ret;
    int val;

    char * mnt = NULL;
    char * fname = NULL;
    

    idx = 1;
    while (idx < argc) {

        /* -file <name> */
        if (strcmp(argv[idx], "-file") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx-1]);
                usage();
            }
            fname = argv[idx];
            idx++;
            continue;
        }
        /* -mnt <name> */
        if (strcmp(argv[idx], "-mount") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx-1]);
                usage();
            }
            mnt = argv[idx];
            idx++;
            continue;
        }	
        /* -process <nb>  */
        if (strcmp(argv[idx], "-process") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx-1]);
                usage();
            }
            ret = sscanf(argv[idx], "%u", &nbProcess);
            if (ret != 1) {
                printf("%s option but bad value \"%s\"!!!\n", argv[idx-1], argv[idx]);
                usage();
            }
            idx++;
            continue;
        }
	
        /* -loop <nb>  */
        if (strcmp(argv[idx], "-loop") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx-1]);
                usage();
            }
            ret = sscanf(argv[idx], "%u", &loop);
            if (ret != 1) {
                printf("%s option but bad value \"%s\"!!!\n", argv[idx-1], argv[idx]);
                usage();
            }
            idx++;
            continue;
        }
		
        /* -closeBetween   */
        if (strcmp(argv[idx], "-closeBetween") == 0) {
            idx++;
            closeBetween = 1;
            continue;
        }	
        /* -closeAfter   */
        if (strcmp(argv[idx], "-closeAfter") == 0) {
            idx++;
            closeAfter = 1;
            continue;
        }	
		
        /* -partial   */
        if (strcmp(argv[idx], "-partial") == 0) {
            idx++;
            ckeck_mode = CHECK_MODE_PARTIAL;
            continue;
        }

        /* -total   */
        if (strcmp(argv[idx], "-total") == 0) {
            idx++;
            ckeck_mode = CHECK_MODE_TOTAL;
            continue;
        }

        /* -random   */
        if (strcmp(argv[idx], "-random") == 0) {
            idx++;
            ckeck_mode = CHECK_MODE_RANDOM;
            continue;
        }
					
	/* -fileSize <MB> */
        if (strcmp(argv[idx], "-fileSize") == 0) {
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
	    file_mb = val;
	    file_mb *= 1000000;
            idx++;
            continue;
        }	
			
        printf("Unexpected parameter %s\n", argv[idx]);
        usage();
    }
    
    
    char * p = FILENAME;
    if (mnt == NULL) {
      p += sprintf(p,"%s/",DEFAULT_MNT);
    }
    else {
      p += sprintf(p,"%s/",mnt);
    }
    if (fname == NULL) {
      p += sprintf(p,"%s",DEFAULT_FILENAME);
    }
    else {
      p += sprintf(p,"%s",fname);
    }    
    p += sprintf(p,".%d",getpid());    
}


int do_write_offset(int f, int offset, int blockSize, char *filename, char * pCompareBuff, char * pBlock) {
    ssize_t size;
   
    memcpy(&pCompareBuff[offset],pBlock,blockSize);
//   printf("PWRITE off [%x,%x] size %d\n",offset,offset+blockSize,blockSize);
    size = pwrite(f, pBlock, blockSize, offset);
    if (size != blockSize) {
        ERROR("pwrite size %d offset %d %s\n",blockSize, offset, strerror(errno));
        return -1;
    }
    return 0;

}
int do_total_read_and_check(int f, char *filename, char * pReadBuff, char * pCompareBuff) {
    ssize_t size;
    int idx2;
    size = pread(f, pReadBuff, READ_BUFFER_SIZE, 0);
    if (size <= 0) {
        ERROR("pread size %llu %s\n",READ_BUFFER_SIZE,strerror(errno));
        return -1;
    }
//    printf("PREAD size = %d (%x)\n",size,size);
    for (idx2 = 0; idx2 < size; idx2++) {
        if (pReadBuff[idx2] != pCompareBuff[idx2]) {
  	    int dump;	
            ERROR("offset %d = 0x%x contains %x instead of %x\n",idx2, idx2, pReadBuff[idx2],pCompareBuff[idx2]);
	    ERROR("offset %d of %d read at offset %d\n", (int)idx2,(int)size,0);
	    dump = size - idx2;
	    if (dump > 64) dump = 64;	            
	    hexdump(pReadBuff,idx2, dump);	    
	    ERROR("ref buf\n");
            hexdump(pCompareBuff,idx2, dump); 
            return -1;
        }
    }
    
    return 0;
}
int do_random_read_and_check(int f, char *filename,char * pReadBuff, char * pCompareBuff) {
    ssize_t      size;
    int          idx2;    
    unsigned int nbControl;
    unsigned int blockSize=0;
    unsigned int offset=0;    

    nbControl = loop;    
    while (nbControl--) {

      blockSize = (random()+blockSize) % RANDOM_BLOCK_SIZE;      
      if (blockSize == 0) blockSize = 1;      
      offset    = (random()+offset)    % (file_mb-blockSize); 

      memset(pReadBuff,0,blockSize);
      size = pread(f, pReadBuff, blockSize, offset);
      if (size < 0) {
        ERROR("pread offset %u size %u %s\n",offset, blockSize, strerror(errno));
        return -1;
      }      
      for (idx2 = 0; idx2 < blockSize; idx2++) {
          if (pReadBuff[idx2] != pCompareBuff[idx2+offset]) {
  	      int dump;
              ERROR("offset %d = 0x%x contains %x instead of %x\n", 
	            idx2+offset, idx2+offset, pReadBuff[idx2],pCompareBuff[idx2+offset]);
	      ERROR("offset %d of %d read at offset %d\n", (int)idx2,(int)size,offset);
	      dump = size - idx2;
	      if (dump > 64) dump = 64;		      
	      hexdump(pReadBuff,idx2, dump);	    
	      ERROR("ref buf\n");
              hexdump(pCompareBuff,idx2+offset, dump); 
              return -1;
          }
      }
    }
    return 0;
}
int do_partial_read_and_check(int f, char *filename, char * pReadBuff, char * pCompareBuff) {
    ssize_t size,len;
    int idx=0,idx2;
    int offsetStart;
    
    while (1) {
    
      /* Skip non written parts of the file */
      while ((idx<READ_BUFFER_SIZE) && (pCompareBuff[idx] == 0)) idx++;
      
      if (idx >= READ_BUFFER_SIZE) return 0;
      
      /* How much bytes to compare */
      offsetStart = idx;
      while ((idx<READ_BUFFER_SIZE) && (pCompareBuff[idx] != 0)) idx++; 
      
      len = idx-offsetStart;

      size = pread(f, pReadBuff, len, offsetStart);     
      if (size != len) {
        ERROR("pread size %lld %s\n",(long long) size, strerror(errno));
        return -1;
      }            
      
      /* Compare */
      for (idx2 = 0; idx2 < size; idx2++) {
        if (pReadBuff[idx2] != pCompareBuff[idx2+offsetStart]) {
	    int dump;
            ERROR("offset %d = 0x%x contains %x instead of %x\n", 
	             idx2+offsetStart, idx2+offsetStart,
		     pReadBuff[idx2],pCompareBuff[idx2+offsetStart]);
	    ERROR("offset %d of %d read at offset %d\n", (int)idx2,(int)size,offsetStart);
	    dump = size - idx2;
	    if (dump > 64) dump = 64;	 
            hexdump(pReadBuff,idx2, dump);	    
	    ERROR("ref buf\n");
            hexdump(pCompareBuff,idx2+offsetStart, dump); 
            return -1;
        }
      }
    }
 
    return 0;
}
int do_one_test(int * f, char * filename, int count, char * pReadBuff,char * pCompareBuff, char * pBlock) {
    unsigned int blockSize=0;
    unsigned int offset=0;
    unsigned int nbWrite;
    int          ret;


//    nbWrite = 1 + count % 3;
    nbWrite = 1 ;    
    while (nbWrite--) {

      blockSize = (random()+blockSize) % RANDOM_BLOCK_SIZE;      
      if (blockSize == 0) blockSize = 1;      
      offset    = (random()+offset)    % (file_mb-blockSize); 
      

      if (do_write_offset(*f, offset, blockSize, filename, pCompareBuff, pBlock) != 0) {
	ERROR("blocksize %6d  - offset %6d\n", blockSize, offset);
	close(*f);      
	return -1;
      }  
    }
        
    if (closeBetween) {
      ret = close(*f);
      if (ret != 0) {
          ERROR("close %s\n",strerror(errno));
          ERROR("last offset %d size %d block %d\n", offset,blockSize, offset/8096);
          return -1;
      }

      *f = open(filename, O_RDWR);
      if (*f < 0) {
          ERROR("re-open %s\n",strerror(errno));
          return -1;
      }
    }
    
    switch (ckeck_mode) {
      case CHECK_MODE_TOTAL:
	return do_total_read_and_check(*f,filename, pReadBuff, pCompareBuff);
	
      case CHECK_MODE_PARTIAL:	
	return do_partial_read_and_check(*f,filename, pReadBuff, pCompareBuff);
	
      case CHECK_MODE_RANDOM:	
	return do_random_read_and_check(*f,filename, pReadBuff, pCompareBuff);
	
      default:
        ERROR("unknown ckeck_mode %d\n",ckeck_mode);   	
        return -1;    
    }
}

int loop_test_process(int myProcId,char mySep) {
  int count=0; 
  char filename[500];
  char c;
  char * pChar;
  int    fd;
  char * pReadBuff;
  char * pCompareBuff;
  char * pBlock;
   
  pBlock = NULL;
  pBlock = malloc(RANDOM_BLOCK_SIZE + 1000);
  if (pBlock == NULL) {
      printf("Can not allocate %d bytes\n", RANDOM_BLOCK_SIZE+1);
      perror("malloc");
      return -1;
  }  
  
  pChar = pBlock;
  c = 'A'+ (myProcId%26);
  while ((pChar - pBlock)<RANDOM_BLOCK_SIZE) {
    pChar += sprintf(pChar,"%c%2.2d%c", c,myProcId,mySep);
    if  (c == 'Z') {
      c = 'a';
    }	
    else if (c == 'z') {
      c = 'A';
    }	
    else c++;    
  }
    
  // Prepare a working buffer to read from or write to the file
  pReadBuff = NULL;
  pReadBuff = malloc(READ_BUFFER_SIZE);
  if (pReadBuff == NULL) {
      printf("Can not allocate %llu bytes\n", READ_BUFFER_SIZE);
      perror("malloc");
      return -1;
  }
  memset(pReadBuff,0,READ_BUFFER_SIZE);

  // Buffer containing what the file should contain
  pCompareBuff = NULL;
  pCompareBuff = malloc(READ_BUFFER_SIZE);
  if (pCompareBuff == NULL) {
      printf("Can not allocate %llu bytes\n", READ_BUFFER_SIZE);
      perror("malloc");
      return -1;
  }
  memset(pCompareBuff,0,READ_BUFFER_SIZE);  
    
  sprintf(filename,"%s.%d",FILENAME, myProcId);
  if (unlink(filename) == -1) {
    if (errno != ENOENT) {
      printf("proc %3d - ERROR !!! unlink(%s) %s\n", myProcId, filename, strerror(errno));
      return -1;
    }
  }

  fd = open(&filename[0], O_RDWR | O_CREAT, 0640);
  if (fd == -1) {
      printf("proc %3d - open(%s) %s\n",myProcId, filename, strerror(errno));
      return -1;
  }
          
  while (1) {
    count++;    
    if  (do_one_test(&fd,filename,count, pReadBuff, pCompareBuff, pBlock) != 0) {
      ERROR("ERROR in loop %d\n", count);   
      close(fd);   
      return -1;
    } 
    
    if (loop==count) {
      if (close(fd)<0) {
        ERROR("ERROR in loop %d. close %s\n", count,strerror(errno)); 
	return -1;          
      }
      return 0;
    }
      
    if (closeAfter) {
      if (close(fd)<0) {
        ERROR("ERROR in loop %d. close() %s\n", count,strerror(errno));  
      }      
      fd = open(&filename[0], O_RDWR);
      if (fd == -1) {
	  ERROR("open %s\n",strerror(errno));
	  return -1;
      }             
    }  
  }
}  
void free_result(void) {
  struct shmid_ds   ds;
  shmctl(shmid,IPC_RMID,&ds); 
}
int * allocate_result(int size) {
  struct shmid_ds   ds;
  void            * p;
      
  /*
  ** Remove the block when it already exists 
  */
  shmid = shmget(SHARE_MEM_NB,1,0666);
  if (shmid >= 0) {
    shmctl(shmid,IPC_RMID,&ds);
  }
  
  /* 
  * Allocate a block 
  */
  shmid = shmget(SHARE_MEM_NB, size, IPC_CREAT | 0666);
  if (shmid < 0) {
    perror("shmget(IPC_CREAT)");
    return 0;
  }  

  /*
  * Map it on memory
  */  
  p = shmat(shmid,0,0);
  if (p == 0) {
    shmctl(shmid,IPC_RMID,&ds);  
       
  }
  memset(p,0,size);  
  return (int *) p;
}
int main(int argc, char **argv) {
  pid_t pid[2000];
  int proc;
  int ret;
  char Sep[6]={':','/','*','-','+','_'};
    
  read_parameters(argc, argv);

  if (nbProcess <= 0) {
    printf("Bad -process option %d\n",nbProcess);
    exit(-100);
  }

  result = allocate_result(4*nbProcess);
  if (result == NULL) {
    printf(" allocate_result error\n");
    exit(-100);
  }  
  for (proc=0; proc < nbProcess; proc++) {
  
     pid[proc] = fork();     
     if (pid[proc] == 0) {
       result[proc] = loop_test_process(proc,Sep[proc%6]);
       exit(0);
     }  
  }

  for (proc=0; proc < nbProcess; proc++) {
    waitpid(pid[proc],NULL,0);        
  }
  
  ret = 0;
  for (proc=0; proc < nbProcess; proc++) {
    if (result[proc] != 0) {
      ret++;
    }
  }
  free_result();
  if (ret != 0) printf("OK %d / FAILURE %d\n",nbProcess-ret, ret);
  exit(ret);
}
