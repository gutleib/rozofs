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
#include <ctype.h>


#define DEFAULT_NB_PROCESS    20
#define DEFAULT_LOOP         200
#define DEFAULT_FILE_SIZE_MB   1

#define ERROR(...) printf("%d proc %d %s - ", __LINE__,myProcId,fileName); printf(__VA_ARGS__)

int shmid;
#define SHARE_MEM_NB 7541

int nbProcess       = DEFAULT_NB_PROCESS;
int myProcId;
int loop=DEFAULT_LOOP;
int * result;
long long unsigned int file_mb=DEFAULT_FILE_SIZE_MB*1000000;
char mount[256];
static void usage() {
    printf("Parameters:\n");
    printf("-mount <mount point> ]  The mount point\n");
    printf("[ -fileSize <MB> ]     file size in MB (default %d)\n",DEFAULT_FILE_SIZE_MB);
    printf("[ -process <nb> ]      The test will be done by <nb> process simultaneously (default %d)\n", DEFAULT_NB_PROCESS);
    printf("[ -loop <nb> ]        <nb> test operations will be done (default %d)\n",DEFAULT_LOOP);
    exit(-100);
}
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


char cmd[1024];

static void read_parameters(argc, argv)
int argc;
char *argv[];
{
    unsigned int idx, val;
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
}

int check_size(char * fileName, size_t fsize, size_t writen) {
  struct stat stats;
  int ret;
  int i;
  char * buf=NULL;
  int f;
  int size;
  
    
  ret = lstat(fileName,&stats);
  if (ret < 0) {
    ERROR("lstat (size %d)%s\n",fsize,strerror(errno));   
    return -1;
  }
  if (stats.st_size != fsize) {
    ERROR("lstat -> size %d while expecting %d\n",stats.st_size,fsize);
    return -1;    
  }

  
  f = open(fileName, O_RDONLY);
  if (f == -1) {
    ERROR("open %s\n",strerror(errno));
    return -1;
  } 
  
  buf = malloc(fsize);
  if (buf == NULL) {
    ERROR("malloc %d %s\n", fsize, strerror(errno));
    return -1;
  }  
  
  size = pread(f, buf, fsize, 0);
  if (size != fsize) {
    ERROR("pread %d/%d %s\n",size, fsize, strerror(errno));
    return -1;
  } 
   
  for (i=0; i < writen; i++) {
    if (buf[i] != ((char)i)) {
      ERROR("offset %d contains %d instead of %d\n",i,buf[i],(char)i); 
      int from, len;
      if (i >= 64) from = i-64;
      else         from = 0;
      if ((from+128) <= fsize) len = 128;
      else                     len = fsize-from;      
      hexdump(buf,from,len); 
      close(f);
      free(buf);
      return -1;      
    }
  }
  for (; i < fsize; i++) {
    if (buf[i] != 0) {
      ERROR("offset %d contains %d instead of 0\n",i,buf[i]);       
      int from, len;
      if (i >= 64) from = i-64;
      else         from = 0;
      if ((from+128) <= fsize) len = 128;
      else                     len = fsize-from;     
      hexdump(buf,from,len);
      close(f);
      free(buf);
      return -1;          
    }
  }
  close(f);
  free(buf);
  return 0;
}

int do_one_test(char * fileName, int * fsize) {
  int ret = 0;
  size_t size;
  
  *fsize -= (random() % (*fsize/2)); 

  ret = truncate(fileName, *fsize);
  if (ret < 0) {
    ERROR("truncate(%d) %s\n",*fsize,strerror(errno));
    return -1;
  }  
  ret =  check_size(fileName,*fsize, *fsize);
  if (ret != 0) return ret;

  ret = truncate(fileName, *fsize+3743);
  if (ret < 0) {
    ERROR("truncate(%d) %s\n",*fsize+3743,strerror(errno));
    return -1;
  }  
  return check_size(fileName,*fsize+3743, *fsize);  
}

int loop_test_process() {
  char fileName[256];
  char path[256];
  pid_t pid = getpid();
  int ret;
  int f;
  int count = 0;
  int fsize=0;
  char * buf;
  int i;
   
  sprintf(fileName, "%s/f%u", mount,pid);
   
  f = open(fileName, O_RDWR | O_CREAT | O_TRUNC, 0640);
  if (f == -1) {
      ERROR("open %s",strerror(errno));
      return -1;
  }  

  buf = malloc(file_mb);
  for (i=0; i < file_mb; i++) buf[i] = i;
  
  fsize = pwrite(f, buf, file_mb, 0);
  if (fsize != file_mb) {
    ERROR("pwrite %s\n",strerror(errno));
    close(f);
    return -1;
  }  
  close(f);
  free(buf);
  sync();
  
  ret = check_size(fileName,fsize,fsize); 
  if (ret != 0) {
    ERROR("check_size %s\n",strerror(errno));
    return ret; 
  }

  count = 0;
  
  while (1) {
  
    count ++;
       
    ret = do_one_test(fileName,&fsize);   
    if (ret != 0) {
      ERROR("do_one_test loop %d %s\n",count,strerror(errno));
      return ret;
    }

    if (count == loop) {
      unlink(fileName);    
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
  return ret;
}
