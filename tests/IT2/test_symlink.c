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
#include <attr/xattr.h>



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
  
  sprintf(file, "%s/%s%d", mount, base, nb);
  
  ret = lstat(file,&stats);
  if (ret >= 0) {
    printf("LINE %d File %s should not exist\n",line,file);
    return -1;
  }
  if (errno != ENOENT) {
    printf("LINE %d File %s expecting ENOENT %s\n",line,file, strerror(errno));
    return -1;
  }
  return 0;
}
int check_regular_file(char * base, int nb, int nlinks, int line) {
  struct stat stats;
  int ret;
  char file[128];

  sprintf(file, "%s/%s%d", mount, base, nb);
    
  ret = lstat(file,&stats);
  if (ret < 0) {
    printf("LINE %d lstat(%s) %s\n",line, file, strerror(errno));
    return -1;
  }
  if (!S_ISREG(stats.st_mode)) { 
    printf("LINE %d lstat(%s) not a regular file\n",line,file);
    return -1;
  }  
  if (nlinks != stats.st_nlink) {
    printf("LINE %d file %s has %d links while expecting %d\n",line,file,(int)stats.st_nlink,nlinks);
    return -1;
  }
  return 0;
}
int check_symlink_file(char * base, int target, int link, int line) {
  struct stat stats;
  int ret;
  char flink[128];
  char ftarget[128];
  char expected[128];

  sprintf(flink, "%s/%s%d", mount, base, link);
    
  ret = lstat(flink,&stats);
  if (ret < 0) {
    printf("LINE %d lstat(%s) %s\n",line,flink, strerror(errno));
    return -1;
  }
  if (!S_ISLNK(stats.st_mode)) { 
    printf("LINE %d lstat(%s) not a symbolic link\n",line,flink);
    return -1;
  }  
  ret = readlink(flink,ftarget,128);
  if (ret < 0) {
    printf("LINE %d readlink(%s) %s\n",line,flink, strerror(errno));
    return -1;
  }
  ftarget[ret] = 0;
  sprintf(expected, "%s/%s%d", mount, base, target);
  if (strcmp(ftarget,expected) != 0) {
    printf("LINE %d : %s is a link to %s while expecting %s\n",line, flink,ftarget,expected);
    return -1;    
  }
  return 0;
}
void remove_file(char * base, int nb) {
  char file[128];

  sprintf(file, "%s/%s%d", mount, base, nb);
  unlink(file); 
}
void create_file(char * base, int nb) {
  char file[128];

  sprintf(file, "%s/%s%d", mount, base, nb);
  
  sprintf(cmd, "echo %d > %s", nb, file);
  system(cmd);  
}
void sym_link(char * base, int target,int link) {
  char ftarget[128];
  char flink[128];
  
  sprintf(flink, "%s/%s%d", mount, base, link); 
  sprintf(ftarget, "%s/%s%d", mount, base, target); 
  symlink(ftarget,flink);
}
#define ROZOFS_RESYMLINK_XATTR "trusted.rozofs.symlink"
void sym_resymlink(char * base, int target,int link) {
  char ftarget[128];
  char flink[128];
  
  sprintf(flink, "%s/%s%d", mount, base, link); 
  sprintf(ftarget, "%s/%s%d", mount, base, target); 
  
  int ret = lsetxattr(flink, ROZOFS_RESYMLINK_XATTR, ftarget, strlen(ftarget), XATTR_CREATE);   
  if (ret < 0) {
    printf("sym_resymlink lsetxattr(%s,%s) %s\n",flink, ftarget, strerror(errno));
  } 
}
#define ROZOFS_REDIRSYMLINK_XATTR "user.rozofs.dirsymlink"
void sym_redirsymlink(char * base, int target,int link) {
  char ftarget[128];

  sprintf(ftarget, "%s%d %s/%s%d", base, link, mount, base, target); 
  
  int ret = setxattr(mount, ROZOFS_REDIRSYMLINK_XATTR, ftarget, strlen(ftarget), XATTR_CREATE);   
  if (ret < 0) {
    printf("sym_redirsymlink setxattr(%s,%s) %s\n",mount, ftarget, strerror(errno));
  } 
}
int do_one_test(char * base, int count) {
  int ret = 0;
  int nbLink=0;
  int i,idx;
  int nb[4] ={1,3,11,101};
  int link=99;
  int f;
  
  /* Create two files */
  for (i=0; i<4;i++) {
    create_file(base, nb[i]);
  }  
      
  /* create a symbolic link */
  idx = 0;
  f = nb[idx];
  sym_link(base,f,link); 
  check_symlink_file(base,f,link,__LINE__);

  for (i=0; i < 128; i++) {
    int prev = f;
    idx = (idx+1)%4;
    f = nb[idx];
    
    /* Check one can not change symlink via POSIX */
    sym_link(base,f,link); 
    if (check_symlink_file(base,prev,link,__LINE__)<0)
      return -1;
  
    /* Use rozofs extended attribute to change the target */
    if (getgid()==0) {
      sym_resymlink(base,f,link); 
      if (check_symlink_file(base,f,link,__LINE__)<0)
	return -1;
    }
    
    idx = (idx+1)%4;
    f = nb[idx];
      
    /* Use rozofs extended attribute to change the target */
    sym_redirsymlink(base,f,link); 
    if (check_symlink_file(base,f,link,__LINE__)<0)
      return -1;      
  
  }
  for (i=0; i<4;i++) {
    remove_file(base, nb[i]);
  }
  remove_file(base,link);  
  return 0;
}
int loop_test_process() {
  int count=0;   
  char baseName[256];
  pid_t pid = getpid();
         
  sprintf(baseName, "symlink_test.%u.", pid);
            
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
