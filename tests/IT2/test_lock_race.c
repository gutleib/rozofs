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
#include <sys/wait.h>
#include <time.h>



#define DEFAULT_NB_PROCESS    20
#define DEFAULT_LOOP         200

#define READ_BUFFER_SIZE       (32*1024)


int shmid;
#define SHARE_MEM_NB 7539

#define FILE_SIZE 5000

char FILENAME[500];
char * pReadBuff    = NULL;
char * pCompareBuff = NULL;
int nbProcess       = 2;
int myProcId;
int loop=DEFAULT_LOOP;
int * result;
int blocking=1;
int display=0;
int bsd=0;
int flockp=0;

typedef struct _record_t {
  uint32_t  owner;
  uint32_t  count;
} RECORD_T;  

static void usage() {
    printf("Parameters:\n");
    printf("-file <name>       file to do the test on\n" );
    printf("-nonBlocking       Lock in non blocking mode (default is blocking)\n");
    printf("-bsd               BSD mode lock (default is POSIX)\n");
    printf("-flockp            Set permanent lock\n");
    printf("-display           Display lock traces\n");
    printf("[ -process <nb> ]  The test will be done by <nb> process simultaneously (default %d)\n", DEFAULT_NB_PROCESS);
    printf("[ -loop <nb> ]     <nb> test operations will be done (default %d)\n",DEFAULT_LOOP);
    exit(-100);
}
void do_sleep_ms(int ms) {
    struct timespec req;
    req.tv_sec = 0;
    req.tv_nsec = ms*1000000;   
    nanosleep(&req, NULL);
}    
void do_sleep_sec(int sec) {
    struct timespec req;
    req.tv_sec = sec;
    req.tv_nsec = 0;   
    nanosleep(&req, NULL);
}    
static void read_parameters(argc, argv)
int argc;
char *argv[];
{
    unsigned int idx;
    int ret;

    FILENAME[0] = 0; 

    idx = 1;
    while (idx < argc) {

        /* -file <name> */
        if (strcmp(argv[idx], "-file") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx-1]);
                usage();
            }
            ret = sscanf(argv[idx], "%s", FILENAME);
            if (ret != 1) {
                printf("%s option but bad value \"%s\"!!!\n", argv[idx-1], argv[idx]);
                usage();
            }
            idx++;
            continue;
        }
	
	/* -display   */
        if (strcmp(argv[idx], "-display") == 0) {
            idx++;
            display = 1;
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
        /* -flockp   */
        if (strcmp(argv[idx], "-flockp") == 0) {
            idx++;
            flockp = 1;
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
			
        printf("Unexpected parameter %s\n", argv[idx]);
        usage();
    }
}
void set_flockp(char * name)  {
  char cmd[256];

  sprintf (cmd, "attr -s rozofs -V \" flockp = %d \" %s > /dev/null 2>&1", flockp, name);
  system(cmd);
}
int create_file(char * name)  {
  char c;
  int fd;
  RECORD_T record;
  
  unlink(name);
  
  fd = open(name, O_RDWR | O_CREAT, 0640);
  if (fd < 0) {
    printf("open(%s) %s\n",name,strerror(errno));
    return -1;
  }
  
  set_flockp(name);
  
  memset(&record,0, sizeof(record));
  
  pwrite(fd, &record, sizeof(record), 0);
  close(fd);
}
int display_flock(int ope, struct flock * flock,int retry) {
  char  msg[128];
  char *p = &msg[0];
  
  p += sprintf(p,"PROC %3d ", myProcId);
  switch(ope) {
    case F_GETLK: 
      p += sprintf(p,"GET      ");
      break;
    case F_SETLK: 
      p += sprintf(p,"SET      ");
      break; 
    case F_SETLKW: 
      p += sprintf(p,"SET WAIT ");
      break; 
    default:
      p += sprintf(p,"RESULT %3d    ",retry);
      break;
  }
                    
  switch(flock->l_type) {
    case F_RDLCK: p += sprintf(p,"READ   "); break;
    case F_WRLCK: p += sprintf(p,"WRITE  "); break; 
    case F_UNLCK: p += sprintf(p,"UNLOCK "); break; 
    default: p += sprintf(p,"type=%d ",flock->l_type);
  }
  
  switch(flock->l_whence) {
    case SEEK_SET: p += sprintf(p,"START"); break;
    case SEEK_CUR: p += sprintf(p,"CURRENT"); break; 
    case SEEK_END: p += sprintf(p,"END"); break; 
    default: p += sprintf(p,"whence=%d ",flock->l_whence);
  }
  p += sprintf(p,"%+d len %+d pid %p", flock->l_start,flock->l_len,flock->l_pid);
  printf("%s\n",msg);
}
#define RETRY 8000
int set_lock (int fd, int code , int ope, int start, int stop, int posRef) {
  int ret;
  int retry=RETRY;
  struct flock flock;
        
  flock.l_type   = ope;
  flock.l_whence = posRef;
  flock.l_pid    = getpid();
  switch(posRef) {

    case SEEK_END:
      flock.l_start  = start-FILE_SIZE;
      if (stop == 0) flock.l_len = 0;
      else           flock.l_len = stop-start;
      break;
      
    default:  
      flock.l_start  = start;
      if (stop == 0) flock.l_len = 0;
      else           flock.l_len = stop-start;
  }                
  if (display) display_flock(code,&flock,0);
  
  while (retry) {
    retry--;
    ret = fcntl(fd, code, &flock);
    if (ret == 0) break;
    if ((code == F_SETLKW)||(ope == F_UNLCK)||(errno != EAGAIN)) {
      printf("proc %3d - fnctl() errno %d %s\n", myProcId, errno, strerror(errno));
      return -1;
    }      
  }
  if (retry == 0) {
    printf("proc %3d - fnctl() max retry %d %s\n", myProcId, errno, strerror(errno));
    return -1;    
  }
  
  if (display) printf("PROC %3d %d retry\n", myProcId, RETRY-retry);

  return 0;
}


int loop_test_process() {
  int fd;
  int ret;
  int idx;
  RECORD_T record;
  time_t starting = time(NULL);

  fd = open(FILENAME, O_RDWR);
  if (fd < 0) {
    printf("proc %3d - open(%s) errno %d %s\n", myProcId, FILENAME,errno, strerror(errno));
    return -1;  
  }    
  if (myProcId != 0) sleep(2);

  while ( 1 ) {
  
    /*
    ** Get the lock
    */

    ret = set_lock(fd, F_SETLKW, F_WRLCK, 0, 8, SEEK_SET);    
    if (ret != 0) {
      printf("proc %3d - set_lock(F_SETLKW,%s) errno %d %s\n", myProcId, FILENAME,errno, strerror(errno));    
      close(fd);  
      return ret; 
    }
    
    //printf("proc %d : I have the lock\n",myProcId);
    /*
    ** Read the file content
    */
    pread(fd, &record, sizeof(record), 0);
    
    if (record.owner == myProcId) {

      record.count++;
      record.owner++;
      if (record.owner == nbProcess) record.owner = 0;

      pwrite(fd, &record, sizeof(record), 0);

      //printf("proc %d : new lock owner is %d/%d\n",myProcId, record.owner, record.count);    
    }

    ret = set_lock(fd, F_SETLKW, F_UNLCK, 0, 0, SEEK_SET);    
    if (ret != 0) {
      printf("proc %3d - set_lock(F_UNLCK,%s) errno %d %s\n", myProcId, FILENAME,errno, strerror(errno));    
      close(fd);  
      return ret;	 
    }      

    if (record.count >= loop) break;
     
    usleep(100000);
    
    if ((time(NULL) - starting) > 30) {
      printf("proc %3d - test too long %d/%d\n",myProcId, record.owner, record.count);    
      close(fd);        
      return -1;	   
    }
  }
        
  close(fd);     
  return 0;
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
  int fd;
  int c;
    
  read_parameters(argc, argv);
  if (FILENAME[0] == 0) {
    printf("-file is mandatory\n");
    exit(-100);
  }
  if (create_file(FILENAME)<0) exit(-1000);
  
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
       myProcId = proc;
       result[proc] = loop_test_process();
       exit(0);
     }  
  }

  for (proc=0; proc < nbProcess; proc++) {
    waitpid(pid[proc],NULL,0);        
  }
  
  ret = 0;
  for (proc=0; proc < nbProcess; proc++) {
    if (result[proc] != 0) {
      ret--;
    }
  }
  free_result();
  if (ret) printf("OK %d / FAILURE %d\n",nbProcess+ret, -ret);
  exit(ret);
}
