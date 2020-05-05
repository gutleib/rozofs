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




#define DEFAULT_NB_PROCESS    20
#define DEFAULT_LOOP         200

int shmid;
#define SHARE_MEM_NB 7541
#define NB_HARD_LINKS 16

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
int check_no_file(char * base, int nb, int line) {
  struct stat stats;
  int ret;
  char file[128];
  
  sprintf(file, "%s%d", base, nb);
  
  ret = lstat(file,&stats);
  if (ret >= 0) {
    printf("%d File %s should not exist\n",line,file);
    return -1;
  }
  if (errno != ENOENT) {
    printf("%d File %s expecting ENOENT %s\n",line,file, strerror(errno));
    return -1;
  }
  return 0;
}
int check_regular_file(char * base, int nb, int nlinks, int line) {
  struct stat stats;
  int ret;
  char file[128];
  int  error = 0;

  sprintf(file, "%s%d", base, nb);
reloop:    
  ret = lstat(file,&stats);
  if (ret < 0) {
    printf("%d lstat(%s) %s\n",line, file, strerror(errno));
    return -1;
  }
  if (!S_ISREG(stats.st_mode)) { 
    printf("%d lstat(%s) not a regular file\n",line,file);
    return -1;
  }  
  if (nlinks != stats.st_nlink) {
    if (error < 3) {
      error++;
      usleep(200000);
      goto reloop;
    }  
    printf("[%d] file %s has %d links while expecting %d\n",line,file,(int)stats.st_nlink,nlinks);
    return -1;
  }
  return 0;
}
int check_symlink_file(char * base, int nb, int line) {
  struct stat stats;
  int ret;
  char file[128];
  char target[128];
  char name[128];

  sprintf(file, "%s%d", base, nb);
    
  ret = lstat(file,&stats);
  if (ret < 0) {
    printf("%d lstat(%s) %s\n",line,file, strerror(errno));
    return -1;
  }
  if (!S_ISLNK(stats.st_mode)) { 
    printf("%d lstat(%s) not a symbolic link\n",line,file);
    return -1;
  }  
  ret = readlink(file,target,128);
  if (ret < 0) {
    printf("%d readlink(%s) %s\n",line,file, strerror(errno));
    return -1;
  }
  target[ret] = 0;
  sprintf(name, "%s%d", base, nb-NB_HARD_LINKS);
  if (strcmp(target,name) != 0) {
    printf("%d %s is a link to %s while expecting %s\n",line,file,target,name);
    return -1;    
  }
  return 0;
}
void remove_file(char * base, int nb) {
  char file[128];

  sprintf(file, "%s%d", base, nb);
  unlink(file); 
}
void create_file(char * base, int nb) {
  char file[128];

  sprintf(file, "%s%d", base, nb);
  
  sprintf(cmd, "echo HJKNKJNKNKhuezfqr > %s", file);
  system(cmd);  
}
void hard_link(char * base, int nb1,int nb2) {
  char f1[128];
  char f2[128];
  
  sprintf(f1, "%s%d", base, nb1); 
  sprintf(f2, "%s%d", base, nb2); 
  sprintf(cmd, "ln %s %s", f1, f2);
  system(cmd);  
}
void sym_link(char * base, int nb1,int nb2) {
  char f1[128];
  char f2[128];
  
  sprintf(f1, "%s%d", base, nb1); 
  sprintf(f2, "%s%d", base, nb2); 
  sprintf(cmd, "ln -s %s %s", f1, f2);
  system(cmd);  
}

int do_one_test(char * base, int count) {
  int ret = 0;
  int nbLink=0;
  int i,j;
  
  
  for (i=0; i<(2*NB_HARD_LINKS); i++) {
    remove_file(base,i);
  }
  for (i=0; i<(2*NB_HARD_LINKS); i++) {
    ret += check_no_file(base, i, __LINE__);
  }  
  if (ret != 0) goto error;

  /* File 0 is the base hardlink */
  create_file(base,0);
  nbLink ++;
  for (i=0; i<nbLink; i++) {                     
    ret += check_regular_file(base, i, nbLink, __LINE__);
  }
  for (; i<(2*NB_HARD_LINKS); i++)  {
    ret += check_no_file(base, i, __LINE__);
  }  
  if (ret != 0) goto error;
    
  /* Create hardink j from hardlink j-1*/  
  for (j=1; j <NB_HARD_LINKS; j++) {
    hard_link(base,j-1,j);
    nbLink ++;    
    for (i=0; i<nbLink; i++) {
      ret += check_regular_file(base, i, nbLink, __LINE__);
    }
    for (; i<(2*NB_HARD_LINKS); i++)  {
      ret += check_no_file(base, i, __LINE__);
    }  
    if (ret != 0) goto error;
  }

  /* Create symbolic link j on hardlink j */
  for (j=0; j <NB_HARD_LINKS; j++) {
    sym_link(base,j,j+NB_HARD_LINKS);
    for (i=0; i<NB_HARD_LINKS; i++) {
      ret += check_regular_file(base, i, nbLink, __LINE__);
    }  
    for (; i<=(NB_HARD_LINKS+j); i++) {
      ret += check_symlink_file(base, i, __LINE__);
    }  
    for (; i<(2*NB_HARD_LINKS); i++)  {
      ret += check_no_file(base, i, __LINE__);
    } 
    if (ret != 0) goto error;
  }
  
  /* remove hardlinks */
  for (j=0; j <NB_HARD_LINKS; j++) {
    remove_file(base,j);
    nbLink --;    
    for (i=0; i<=j; i++) {
      ret += check_no_file(base, i, __LINE__);
    }
    for (; i<NB_HARD_LINKS; i++) {
      ret += check_regular_file(base, i, nbLink, __LINE__);
    }  
    for (; i<(2*NB_HARD_LINKS); i++)  {    
      ret += check_symlink_file(base, i, __LINE__);
    }  
    if (ret != 0) goto error;
  }

  /* remove symbolic links */
  for (j=0; j <NB_HARD_LINKS; j++) {
    remove_file(base,j+NB_HARD_LINKS);
    nbLink --;    
    for (i=0; i<=(NB_HARD_LINKS+j); i++)  {
      ret += check_no_file(base, i, __LINE__);
    }
    for (; i<(2*NB_HARD_LINKS); i++)  {    
      ret += check_symlink_file(base, i, __LINE__);
    }  
    if (ret != 0) goto error;
  }  
  return 0;
  
error:
  //for (i=0; i<=NB_HARD_LINKS; i++) remove_file(base,i);
  return -1;
}
int loop_test_process() {
  int count=0;   
  char baseName[256];
  pid_t pid = getpid();
         
  sprintf(baseName, "%s/link_test.%u.", mount, pid);
            
  while (1) {
    count++;    
    if  (do_one_test(baseName,count) != 0) {
      printf("proc %3d - ERROR in loop %d\n", myProcId, count); 
      return -1;
    } 
    if (loop==count) {
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
