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
#include <sys/file.h>



#define DEFAULT_NB_PROCESS    20
#define DEFAULT_LOOP         200

#define READ_BUFFER_SIZE       (32*1024)


int shmid;
#define SHARE_MEM_NB 7539

#define FILE_SIZE 5000

char FILENAME[500];
char MYFILENAME[500];
char * pReadBuff    = NULL;
char * pCompareBuff = NULL;
int nbProcess       = DEFAULT_NB_PROCESS;
int myProcId;
int loop=DEFAULT_LOOP;
int * result;
int blocking=1;
int display=0;
int bsd=0;
int flockp=0;

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
    MYFILENAME[0] = 0; 

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
	
        /* -nonBlocking   */
        if (strcmp(argv[idx], "-nonBlocking") == 0) {
            idx++;
            blocking = 0;
            continue;
        }
	
        /* -bsd   */
        if (strcmp(argv[idx], "-bsd") == 0) {
            idx++;
            bsd = 1;
            continue;
        }
        	
        /* -flockp   */
        if (strcmp(argv[idx], "-flockp") == 0) {
            idx++;
            flockp = 1;
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
int create_file(char * name, int size)  {
  char c;
  int fd;
  
  unlink(name);
  
  fd = open(name, O_RDWR | O_CREAT, 0640);
  if (fd < 0) {
    printf("open(%s) %s\n",name,strerror(errno));
    return -1;
  }
  c = 0;
  pwrite(fd,&c, 1, size-1);  // Size the file
  
  set_flockp(name);
  
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

int lockAllUnlockByPieces (int ope, int posRef) {
  int ret;
  int fd;
  int code= blocking?F_SETLKW:F_SETLK;
     
  fd = open(MYFILENAME, O_RDWR);
  if (fd < 0) {
    printf("proc %3d - open(%s) errno %d %s\n", myProcId, MYFILENAME,errno, strerror(errno));
    ret -1;
    goto out;  
  }    
    
  ret = set_lock(fd, code , ope, 0, 0, posRef);    
  if (ret != 0) goto out; 
      
  ret = set_lock(fd, code , F_UNLCK, 1003, 0, posRef);       
  if (ret != 0) goto out; 
  
  ret = set_lock(fd, code , F_UNLCK, 1003, 0, posRef);       
  if (ret != 0) goto out; 

   ret = set_lock(fd, code , F_UNLCK, 1002, 0, posRef);       
  if (ret != 0) goto out; 

   ret = set_lock(fd, code , F_UNLCK, 1001, 1543, posRef);     
  if (ret != 0) goto out;  
    
  ret = set_lock(fd, code , F_UNLCK, 0, 497, posRef);      
  if (ret != 0) goto out; 
  
  ret = set_lock(fd, code , F_UNLCK, 0, 497, posRef);     
  if (ret != 0) goto out; 
    
  ret = set_lock(fd, code , F_UNLCK, 0, 498, posRef);    
  if (ret != 0) goto out; 
   
  ret = set_lock(fd, code , F_UNLCK, 143, 499, posRef);      
  if (ret != 0) goto out; 

  ret = set_lock(fd, code , F_UNLCK, 501, 999, posRef);       
  if (ret != 0) goto out; 
  
  ret = set_lock(fd, code , F_UNLCK, 100, 1999, posRef);        
  if (ret != 0) goto out; 

  ret = set_lock(fd, code , ope, 0, 0, posRef);     
  if (ret != 0) goto out; 
  
  ret = set_lock(fd, code , F_UNLCK, 0, 497, posRef);       
  if (ret != 0) goto out; 
  
  ret = set_lock(fd, code , F_UNLCK, 0, 497, posRef);    
  if (ret != 0) goto out; 
    
  ret = set_lock(fd, code , F_UNLCK, 0, 498, posRef);     
  if (ret != 0) goto out; 
   
  ret = set_lock(fd, code , F_UNLCK, 143, 499, posRef);      
  if (ret != 0)goto out; 

  ret = set_lock(fd, code , F_UNLCK, 1003, 0, posRef);    
  if (ret != 0) goto out; 
  
  ret = set_lock(fd, code , F_UNLCK, 1003, 0, posRef);     
  if (ret != 0) return ret;  

  ret = set_lock(fd, code , F_UNLCK, 1002, 0, posRef);     
  if (ret != 0) goto out;  

  ret = set_lock(fd, code , F_UNLCK, 1001, 1543, posRef);    
  if (ret != 0) goto out;  
 
  ret = set_lock(fd, code , F_UNLCK, 0, 0, posRef);    
  if (ret != 0) goto out; 

out:     
  close(fd);
  return ret;
}
int lockUnlock (int fd, int ope, int posRef) {
  int ret;
  int idx;
  int code=blocking?F_SETLKW:F_SETLK;

  for (idx=0; idx < 200; idx++) { 
   
    ret = set_lock(fd,code , ope, 0, 0, posRef);     
    if (ret != 0) return ret;

    ret = set_lock(fd, code , F_UNLCK, 1001, 0, posRef);      
    if (ret != 0) return ret;

    ret = set_lock(fd, code , F_UNLCK, 0, 1000, posRef); 
    if (ret != 0) return ret;
  }
  return ret;
}
typedef enum _flock_mode_e {
  FLOCK_MODE_READ,
  FLOCK_MODE_WRITE
} FLOCK_MODE_E;
int flockUnflock (int fd, FLOCK_MODE_E mode) {
  int ret;
  int opcode;
  int retry=RETRY;
    
  if (mode == FLOCK_MODE_WRITE) opcode = LOCK_EX;
  else                          opcode = LOCK_SH;
  if (!blocking) opcode |= LOCK_NB;
  

  while (retry) {
    retry--;
    ret = flock(fd, opcode);   
    if (ret == 0) break;
    if (blocking) {
      printf("proc %3d - flock() errno %d %s\n", myProcId, errno, strerror(errno));
      return -1;
    }      
  }
  if (retry == 0) {
    printf("proc %3d - flock() max retry %d %s\n", myProcId, errno, strerror(errno));
    return -1;    
  }

  opcode = LOCK_UN;
  if (!blocking) opcode |= LOCK_NB;
  ret = flock(fd, opcode);     
  if (ret != 0) {
    sprintf("flock(unclok) %s\n", strerror(errno));
  }
  return ret;
}
int lockUnlockByOffset (int fd, int ope, int posRef) {
  int ret;
  int idx;
  int myOffset = myProcId*10;
  int code = blocking?F_SETLKW:F_SETLK; 

  for (idx=0; idx < 20; idx++) {

    ret = set_lock(fd, code , ope, myOffset, myOffset+9, posRef);    
    if (ret != 0) return ret;

    ret = set_lock(fd, code , F_UNLCK, myOffset, myOffset+9, posRef);    
    if (ret != 0) return ret;

    ret = set_lock(fd, code , ope, myOffset, myOffset+9, posRef);    
    if (ret != 0) return ret;
    
    ret = set_lock(fd, code , F_UNLCK, 0, 0, posRef);       
    if (ret != 0) return ret;
    
    ret = set_lock(fd, code , ope, myOffset, myOffset+9, posRef);     
    if (ret != 0) return ret;    

    ret = set_lock(fd, code , F_UNLCK, 0, myOffset+9, posRef);       
    if (ret != 0) return ret;
    
    ret = set_lock(fd, code , ope, myOffset, myOffset+9, posRef);     
    if (ret != 0) return ret;    

    ret = set_lock(fd, code , F_UNLCK, myOffset, 0, posRef);    
    if (ret != 0) return ret;        
  }
  return ret;
}
int do_posix_test(int count) {
  int fd;
  int ret;
  int idx;
  int posRef;
     
  fd = open(MYFILENAME, O_RDWR);
  if (fd < 0) {
    printf("proc %3d - open(%s) errno %d %s\n", myProcId, MYFILENAME,errno, strerror(errno));
    return -1;  
  }    
      

  for (posRef=SEEK_SET; posRef <= SEEK_END; posRef++) {

    ret = set_lock(fd, F_GETLK, F_RDLCK,0,0, posRef);	    
    if (ret != 0) break;

    ret = set_lock(fd, F_GETLK, F_WRLCK,0,0, posRef);	 
    if (ret != 0) break;  
    
    ret = lockAllUnlockByPieces(F_RDLCK, posRef);    
    if (ret != 0) break;
    
    ret = lockAllUnlockByPieces(F_WRLCK, posRef);    	
    if (ret != 0) break;  

    ret = lockUnlockByOffset(fd, F_RDLCK, posRef);      
    if (ret != 0) break;    

    ret = lockUnlockByOffset(fd, F_WRLCK, posRef);    
    if (ret != 0) break;     

  }

  set_lock(fd, blocking?F_SETLKW:F_SETLK, F_WRLCK,0,0, SEEK_SET);       
  close(fd);
  return ret;
}
int do_bsd_test(int count) {
  int fd;
  int ret;
  int idx;

  fd = open(MYFILENAME, O_RDWR);
  if (fd < 0) {
    printf("proc %3d - open(%s) errno %d %s\n", myProcId, MYFILENAME,errno, strerror(errno));
    return -1;  
  }    

  for (idx=0; idx < 200; idx++) {
  
    ret = flockUnflock(fd, FLOCK_MODE_READ);	    
    if (ret != 0) break;
    
    ret = flockUnflock(fd, FLOCK_MODE_WRITE);	    
    if (ret != 0) break;

  }      
  close(fd);
  return ret;
}
int loop_test_process() {
  int count=0; 
  int ret=0;

  sprintf(MYFILENAME,"%s.%d", FILENAME, getpid());  
  if (create_file(MYFILENAME,FILE_SIZE)<0) return -1;
  
  while (count<loop) {   
    count++; 
    if (bsd) ret = do_bsd_test(count);
    else     ret = do_posix_test(count);
    if(ret != 0) break;
  }
  unlink(MYFILENAME);
  return ret;
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
  if (create_file(FILENAME,FILE_SIZE)<0) exit(-1000);
  
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
