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
#include <sys/stat.h>
#include <sys/wait.h>
#include <dirent.h>




#define DEFAULT_NB_PROCESS    20
#define DEFAULT_LOOP         200

int shmid;
#define SHARE_MEM_NB 7541

int nbProcess       = DEFAULT_NB_PROCESS;
int myProcId;
int loop=DEFAULT_LOOP;
int * result;
char mount[256];
static void usage() {
    printf("Parameters:\n");
    printf("-mount <mount point> ]  The mount point\n");
    printf("[ -process <nb> ]      The test will be done by <nb> process simultaneously (default %d)\n", DEFAULT_NB_PROCESS);
    printf("[ -loop <nb> ]        <nb> test operations will be done (default %d)\n",DEFAULT_LOOP);
    exit(-100);
}


char cmd[1024];

static void read_parameters(argc, argv)
int argc;
char *argv[];
{
    unsigned int idx;
    int ret;
    
    mount[0] = 0;

    idx = 1;
    while (idx < argc) {
	
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
        /* -mount <mount point>  */
        if (strcmp(argv[idx], "-mount") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx-1]);
                usage();
            }
            ret = sscanf(argv[idx], "%s", mount);
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

int check_rights(char * file, mode_t mode) {
  struct stat stats;
  int ret;
    
  ret = lstat(file,&stats);
  if (ret < 0) {
    printf("lstat(%s) %s\n",file,strerror(errno));
    return -1;
  }
  if ((stats.st_mode & 0x1FF) == mode) {
    return 0;
  }  
  
  printf("%s has mode %x while expecting %x\n",file,stats.st_mode,mode);
  sleep(1);
  ret = lstat(file,&stats);
  if (ret < 0) {
    printf("lstat(%s) %s\n",file,strerror(errno));
    return -1;
  }
  if ((stats.st_mode & 0x1FF) != mode) {
    printf("!!! %s has mode %x while expecting %x\n",file,stats.st_mode,mode);
    return -1;    
  }  
  return 0;
}

int do_one_test(char * d, char * f) {
  int ret = 0;
  int u,g,o;
  mode_t mode;
  
  mode= 0;
  for (u=0; u<8; u++) {
    for  (g=0; g<8; g++) {
      for  (o=0; o<8; o++) {
        mode = u<<6 | g << 3 | o; 
        ret = chmod(d,mode);
	if (ret < 0) {
	  printf("Can not set mode of %s to 0x%x %s\n",d, mode, strerror(errno));
	  return -1;
	}
	ret = check_rights(d,mode);
	if (ret < 0) return ret;  
      }
    }  
  }
  
  mode= 0;
  for (u=0; u<8; u++) {
    for  (g=0; g<8; g++) {
      for  (o=0; o<8; o++) {
        mode = u<<6 | g << 3 | o; 
        ret = chmod(f,mode);
	if (ret < 0) {
	  printf("Can not set mode of %s to 0x%x %s\n",f, mode, strerror(errno));
	  return -1;
	}
	ret = check_rights(f,mode);
	if (ret < 0) return ret;  
      }
    }  
  }
  
  return 0;
}
int loop_test_process() {
  char directoryName1[512];
  char fileName[512];
  pid_t pid = getpid();
  int ret;
  int count = 0;
 
  sprintf(directoryName1, "%s/d%u", mount,pid);
  ret = mkdir(directoryName1,0755);
  if (ret < 0) {
    printf("proc %3d - mkdir(%s) %s\n", myProcId, directoryName1,strerror(errno));  
    return -1;       
  }  
  
  sprintf(fileName,"%s/foil",directoryName1);
  sprintf(cmd,"echo QS > %s", fileName);
  system(cmd);
  
  while (1) {
  
    count ++;
       
    ret = do_one_test(directoryName1,fileName);   
    if (ret < 0) {
      printf("proc %3d - test failed in loop %d\n", myProcId, count);  
      return ret;
    }

    if (count == loop) {
      rmdir(directoryName1);    
      return 0;
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
    
  read_parameters(argc, argv);
  
  if (mount[0] == 0) {
    printf("-mount is mandatory\n");
    exit(-100);
  }
  
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
