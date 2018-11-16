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


#define FILE_BASE_NAME "This_is_a_pretty_long_base_file_name_in_order_to_trigger_readdir_not_just_in_one_exchange_with_the_export_but_in_several_runs_toward_the_export______This_should_make_this_test_a_better_test_"

#define DEFAULT_NB_PROCESS    20
#define DEFAULT_LOOP         16

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

void remove_file(int nb) {
  char file[1024];

  sprintf(file, "./%s%d", FILE_BASE_NAME, nb);
  unlink(file); 
}
void create_file(int nb) {
  char file[1024];

  sprintf(file, "./%s%d", FILE_BASE_NAME, nb);
  
  sprintf(cmd, "touch %s", file);
  system(cmd);  
}

#define NB_FILES 80

int read_directory(char * d,int nbFiles,int count) {
  DIR * dir;
  int   ret;
  struct dirent  * file;
  unsigned char exist[NB_FILES];
  unsigned char subdir;
  int number;
  int i;
  char * pt;
  
  memset(exist,0,sizeof(exist));
  subdir = 0;
  
  dir = opendir(".");
  if (dir == NULL) {
    printf("Can not open directory at level %d %s\n",count, strerror(errno));
    return -1;
  }

  ret = 0;
  while (1) {
  
    file = readdir(dir);
    if (file == NULL) break;

    if (strcmp(file->d_name,"..")==0) continue;
    if (strcmp(file->d_name,".")==0) continue;
    if (strcmp(file->d_name,d) == 0) {
      subdir = 1;
      continue;
    }
    
    if (strncmp(file->d_name, FILE_BASE_NAME, strlen(FILE_BASE_NAME)) != 0) {
      printf("%s found in directory level %d\n",file->d_name,count);
      ret = -1;
      break;       
    }     
        
    pt = file->d_name;
    pt += strlen(FILE_BASE_NAME);
    ret = sscanf(pt,"%d",&number);
    if (ret != 1) {
      printf("%s found in directory level %d\n",file->d_name,count);   
      ret = -1;
      break;                
    }
 
    if (number >= nbFiles) {
      printf("%s found in directory level %d\n",file->d_name,count);
      ret = -1;
      break;       
    } 
    
    if (exist[number] != 0) {
      printf("%s already found in directory level %d\n",file->d_name,count);
      ret = -1;
      break;       
    } 
    exist[number] = 1;
  }
  
  for (i=0; i< nbFiles; i++) {
    if (exist[i] != 1) {
      printf("file %d is missing in directory level %d\n",i,count);
      ret = -1;      
    }
  }
  for (i; i<NB_FILES;i++) {
    if (exist[i] != 0) {
      printf("file %d exist in directory level %d\n",i,count);
      ret = -1;      
    }
  }
  
  if (subdir == 0) {
    printf("subdir %s is missing in directory level %d\n",d,count);
    ret = -1;      
  }
  
  closedir(dir);
  return ret; 
}
int do_one_test(int count) {
  int ret = 0;
  int i;
  char d[32];
  
  sprintf(d,"d%u",count);
  ret = mkdir(d, 0755);
  if (ret < 0) {
    printf("proc %3d - ERROR in loop %d mkdir(%s) %s\n", myProcId, d,strerror(errno));  
    return -1;       
  } 

  ret = read_directory(d,0,count);  
  for (i=0; i< NB_FILES; i++) {
    create_file(i);
    ret = read_directory(d,i+1,count);
    if (ret < 0) return ret;
  }        
  
  ret = read_directory(d,NB_FILES,count);
  if (ret < 0) return ret;
  
  if (count > 0) {

    ret = chdir(d);
    if (ret < 0) {
      printf("proc %3d - ERROR in loop %d chdir(%s) %s\n", myProcId, d,strerror(errno));  
      return -1;       
    }
          
    ret = do_one_test(count-1);
    if (ret != 0) return ret;
    
    ret = chdir("..");
    if (ret < 0) {
      printf("proc %3d - ERROR in loop %d chdir(..) %s\n", myProcId, count,strerror(errno));  
      return -1;       
    }      
  }
  for (; i>=0; i--) {
    remove_file(i);      
    ret = read_directory(d,i,count);
    if (ret < 0) return ret;      
  }
  
  ret = read_directory(d,0,count);
  if (ret < 0) return ret;
  
  ret = rmdir(d);
  if (ret < 0) {
    printf("proc %3d - ERROR in loop %d rmdir(%s) %s\n", myProcId, d,strerror(errno));  
    return -1;       
  }    
  
  return 0;
}
int loop_test_process() {
  char directoryName[256];
  pid_t pid = getpid();
  int ret, res;
       
  
  sprintf(directoryName, "/%s/d%u", mount, pid);
  ret = mkdir(directoryName,0755);
  if (ret < 0) {
    printf("proc %3d - mkdir(%s) %s\n", myProcId, directoryName,strerror(errno));  
    return -1;       
  }
  ret = chdir(directoryName);
  if (ret < 0) {
    printf("proc %3d - chdir(%s) %s\n", myProcId, directoryName,strerror(errno));  
    return -1;       
  }  
          
  res = do_one_test(loop);   

  ret = chdir("/");
  if (ret < 0) {
    printf("proc %3d - chdir(%s) %s\n", myProcId, "/",strerror(errno));  
    return -1;       
  }    
  ret = rmdir(directoryName);
  if (ret < 0) {
    printf("proc %3d - rmdir(%s) %s\n", myProcId, directoryName,strerror(errno));  
    return -1;       
  }         
  return res;
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
