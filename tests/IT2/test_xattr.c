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
#include <attr/xattr.h>
#include <sys/wait.h>




#define DEFAULT_NB_PROCESS    20
#define DEFAULT_LOOP         200

int shmid;
#define SHARE_MEM_NB 7540

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


#define BUFFER_SIZE (16*1024)
char buff[BUFFER_SIZE];
char value[BUFFER_SIZE];
char read_value[BUFFER_SIZE];
char name[BUFFER_SIZE];

int nbAttr = 7;

char * prefixes[]={"user","trusted","security"};
int nb_prefixes = (sizeof(prefixes)/sizeof(void*));

#define myAttributes(prefix,idx) sprintf(name,"%s._%s_%d",prefix,prefix,idx);
#define value_initial(prefix,idx) {myAttributes(prefix,idx); sprintf(value, "%s.%s.%s", file, name,"INITIAL");}
#define value_modified(prefix,idx) {myAttributes(prefix,idx); sprintf(value, "%s.%s.%s", file, name,"MODIFIED");}


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
int get_attribute(char * attr_name, int * prefix, int * idx) {
  int i,p;
  int len;
  
  /*
  ** Find prefix
  */
  for (p=0; p < nb_prefixes; p++) {
    len = strlen(prefixes[p]);
    if (strncmp(prefixes[p],attr_name,len) == 0) break;
  }
  
  if (p == nb_prefixes) {
    printf("No such prefix\n");
    return -1;
  }
  
  *prefix = p;
  for (i=0; i <  nbAttr; i++) {
    myAttributes(prefixes[p],i);
    if (strcmp(name,attr_name) == 0) {
      *idx = i;
      return 0;
    }  
  } 
  printf("No such attribute index\n");  
  return -1;
}
int list_xattr (char * file,int option, int exist, int symlink) {
  ssize_t size;
  char *pAttr=buff,*pEnd=buff;
  int nb;
  int idx;
  int prefix;
  int expected = 0;

  if (exist) {
    if (getgid()==0) {
      if (symlink) expected = nbAttr*(nb_prefixes-1);
      else         expected = nbAttr*nb_prefixes;
    }  
    else {
      if (symlink) expected = 0;
      else         expected = nbAttr;
    }  
  }
     
  if (symlink) {
    /* Apply on symbolic link and not the target */
    size = llistxattr(file,buff,BUFFER_SIZE);    
  }
  else {
    size = listxattr(file,buff,BUFFER_SIZE);
  }  
  
  if (size < 0)  {
    printf("listxattr(%s) %s\n", file, strerror(errno));
    return -1;
  } 
  
  if (!exist) {
    if (size != 0) {
      printf("Xattr does not exist but listxattr(%s) returns %d size\n", file,(int)size);
      return -1;
    }
    return 0;
  }
  
  if ((size == 0) && (expected!=0)){
    printf("Xattr exist but listxattr(%s) returns 0 size\n", file);
    return -1;
  }


  pEnd += size;
  nb = 0;
  while (pAttr < pEnd) {
  
    if (get_attribute(pAttr, &prefix, &idx)< 0) {
      printf("Unexpected attribute %s on file %s\n", pAttr, file);
      return -1;
    }  
    if (symlink) {
      if (prefix == 0) {
	printf("read attribute %s on file %s\n", pAttr, file);
	return -1;        
      }
      /* Apply on symbolic link and not the target */
      size = lgetxattr(file,pAttr,read_value,BUFFER_SIZE);   
    } 
    else {   
      size = getxattr(file,pAttr,read_value,BUFFER_SIZE);
    }  
    
    if (size == -1) {
      printf("getxattr(%s) on file %s %s\n", pAttr, file, strerror(errno));
      return -1;
    }        

    read_value[size] = 0;
    nb++;
    
    
    if (option == XATTR_CREATE) {
      value_initial(prefixes[prefix],idx);
    }
    else {
      value_modified(prefixes[prefix],idx);
    }   
    if (strcmp(read_value,value) != 0) {
      printf("read value %s while expecting %s for attr %s file %s\n", 
             read_value, value, pAttr, file);
      return -1;
    }           
    pAttr += (strlen(pAttr)+1);
  }

  if (nb != expected) {
      printf("Read %d attr while expecting %d\n", nb, expected);
      return -1;
  }  
 
  return 0;    
}
int set_attr (char * file, int option, int exist, int symlink) {
  int idx,res;
  int prefix;
  
  for (idx = 0 ; idx < nbAttr; idx++) {
    for (prefix=0; prefix<nb_prefixes; prefix++) {
  
      // skip trusted prefix when not root
      if (getgid()!= 0) {
        if ((strcmp(prefixes[prefix],"trusted")==0)
	||  (strcmp(prefixes[prefix],"security")==0))  {
	  continue;
        }  
      }
      
      if (option == XATTR_CREATE) {
	value_initial(prefixes[prefix],idx);
      }
      else {
	value_modified(prefixes[prefix],idx);
      }
      
      if (symlink) {
	/* Apply on symbolic link and not the target */
	res = lsetxattr(file, name, value, strlen(value),option);
      } 
      else {         
        res = setxattr(file, name, value, strlen(value),option);
      }
      
      /* set xattr on symlink not allowed in user mode */
      if ((symlink) && (prefix==0)) {
        if ((res >= 0)||(errno!=EPERM)) {
	  printf("lsetxattr(%s,%s) on file %s should be refused\n", name, value, file);
	  return -1;	  
	}
	continue;
      }
      
      if (option == XATTR_REPLACE) {
	if (res < 0) {
	  printf("REPLACE setxattr(%s,%s) on file %s %s\n", name, value, file, strerror(errno));
	  return -1;
	}
	continue;   
      }
      
      if (exist) {
	if ((res >= 0)||(errno!=EEXIST)) {
	  printf("CREATE & exist setxattr(%s,%s) on file %s %s\n", name, value, file, strerror(errno));
	  return -1;
	}
	continue;   
      }
      
      if (res < 0) {
	printf("CREATE & !exist setxattr(%s,%s) on file %s %s\n", name, value, file, strerror(errno));
	return -1;   
      }  
    }
  }  
  return 0;
}   
int remove_attr (char * file, int exist, int symlink) {
  int idx,res;
  int prefix;
  
  for (idx = 0 ; idx < nbAttr; idx++) {
    for (prefix=0; prefix<nb_prefixes;prefix++) {

      // skip trusted prefix when not root
      if (getgid()!= 0) {
        if ((strcmp(prefixes[prefix],"trusted")==0)
	||  (strcmp(prefixes[prefix],"security")==0))  {
	  continue;
        }  
      }    
      myAttributes(prefixes[prefix],idx);
      
      if (symlink) {
	/* Apply on symbolic link and not the target */
	res = lremovexattr(file, name);
      } 
      else {
        res = removexattr(file, name);
      }

      /* set lremovexattr on symlink not allowed in user mode */
      if ((symlink) && (prefix==0)) {
        if ((res >= 0)||(errno!=EPERM)) {
	  printf("lremovexattr(%s) on file %s should be refused\n", name, file);
	  return -1;	  
	}
	continue;
      }
            
      if (exist) {
	if (res < 0) {
	    printf("exist .remove_attr(%s) on file %s %s\n", name, file, strerror(errno));
	    return -1;
	}
	continue;
      }
      
      if (res>= 0) {
        printf("!exist .remove_attr(%s) on file %s\n", name, file);
        return -1; 
      }	         
    }
  }  
  return 0;
}   
#define MAX_XATTR_SIZE 3992
int do_check_max_xattr(char * file, int symlink) {
  int  ret = 0;
  char max_attr[MAX_XATTR_SIZE+1];
  int  i;
  int loop;
    
  for (i=0; i<MAX_XATTR_SIZE; i++) {
    max_attr[i] = ('A'+ (i%26));
  }
  max_attr[MAX_XATTR_SIZE] = 0;

  /*
  ** No xattr yet
  */
     
  if (symlink) {
    /* Apply on symbolic link and not the target */
    ret = llistxattr(file,buff,BUFFER_SIZE);    
  }
  else {
    ret = listxattr(file,buff,BUFFER_SIZE);
  }  
  
  if (ret != 0) {
    printf("LINE %d file %s %d %s\n",__LINE__,file,ret,strerror(errno));
    return 1;
  } 
  
  if (symlink) {
    /* Apply on symbolic link and not the target */
    ret = lsetxattr(file, "system.MAX", max_attr, MAX_XATTR_SIZE, XATTR_CREATE);
  } 
  else {         
    ret = setxattr(file, "user.MAX", max_attr, MAX_XATTR_SIZE, XATTR_CREATE);
  }
  if (ret != 0) {
    printf("LINE %d file %s %d %s\n",__LINE__,file,ret,strerror(errno));
    return 1;
  } 
  
  
  for (loop=0; loop<4; loop++) {
    if (symlink) {
      /* Apply on symbolic link and not the target */
      ret = lgetxattr(file,"system.MAX",max_attr,MAX_XATTR_SIZE);   
      if (ret <= 0) {
        printf("LINE %d file %s %d %s\n",__LINE__,file,ret,strerror(errno));
        return 1;
      } 
    } 
    else {   
      ret = getxattr(file,"user.MAX",max_attr,MAX_XATTR_SIZE);
      if (ret <= 0) {
        printf("LINE %d file %s %d %s\n",__LINE__,file,ret,strerror(errno));
        return 1;
      } 
    }  



    for (i=0; i<MAX_XATTR_SIZE; i++) {
      if (max_attr[i] != ('A'+ (i%26))){
        printf("LINE %d file %s %d\n",__LINE__,file,i);
        return i;    
      }
    }
  }

  if (symlink) {
    /* Apply on symbolic link and not the target */
    ret = lremovexattr(file, "system.MAX");
  } 
  else {
    ret = removexattr(file, "user.MAX");
  }  
  if (ret < 0) {
    printf("LINE %d file %s %d %s\n",__LINE__,file,ret,strerror(errno));
    return 1;
  }
  return 0;
}
int do_one_test(char * file, int count) {
  int ret = 0;
    
  ret += list_xattr(file,XATTR_CREATE, 0, 0 /* not on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  
  ret += set_attr (file,XATTR_CREATE, 0, 0 /* not on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  
  ret += list_xattr(file,XATTR_CREATE, 1, 0 /* not on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
    
  ret += set_attr (file,XATTR_CREATE, 1, 0 /* not on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  
  ret += list_xattr(file,XATTR_CREATE, 1, 0 /* not on symlink */);  
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  
  ret += set_attr (file,XATTR_REPLACE, 1, 0 /* not on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return;
  }
  
  ret += list_xattr(file,XATTR_REPLACE, 1, 0 /* not on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  
  ret += remove_attr (file, 1, 0 /* not on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  
  ret += list_xattr(file,XATTR_REPLACE, 0, 0 /* not on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  
  ret += remove_attr (file, 0, 0 /* not on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  
  ret += list_xattr(file,XATTR_REPLACE, 0, 0 /* not on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  return ret;
}
int do_one_test_symlink(char * file, int count) {
  int ret = 0;

  
  ret += list_xattr(file,XATTR_CREATE, 0, 1 /* apply on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  
  ret += set_attr (file,XATTR_CREATE, 0, 1 /* apply on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  
  ret += list_xattr(file,XATTR_CREATE, 1, 1 /* apply on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
    
  ret += set_attr (file,XATTR_CREATE, 1, 1 /* apply on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  
  ret += list_xattr(file,XATTR_CREATE, 1, 1 /* apply on symlink */);  
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  
  ret += set_attr (file,XATTR_REPLACE, 1, 1 /* apply on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  
  ret += list_xattr(file,XATTR_REPLACE, 1, 1 /* apply on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  
  ret += remove_attr (file, 1, 1 /* apply on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  
  ret += list_xattr(file,XATTR_REPLACE, 0, 1 /* apply on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  
  ret += remove_attr (file, 0, 1 /* apply on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  
  ret += list_xattr(file,XATTR_REPLACE, 0, 1 /* apply on symlink */);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,file);
    return -1;
  }
  return ret;
}
int loop_test_process() {
  int count=0;   
  char filename[256];
  char symlink[256];  
  char dirname[256];  
  char cmd[256];
  pid_t pid = getpid();
  int ret;
       
  
  pid = getpid();
  sprintf(filename, "%s/test_file_xattr.%u", mount, pid);
  sprintf(symlink, "%s/test_slink_xattr.%u", mount, pid);
  sprintf(dirname, "%s/test_dir_xattr.%u", mount, pid);
  
  unlink(filename);
  ret = mknod(filename,S_IFREG | 0700,0);
  if (ret < 0) {
    printf("proc %3d - mknod(%s) %s\n", myProcId, filename,strerror(errno));  
    return -1;       
  } 
  ret += do_check_max_xattr(filename,0);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,filename);
    return;
  }    

  rmdir(dirname);     
  ret = mkdir(dirname,0700); 
  if (ret < 0) {
    printf("proc %3d - mkdir(%s) %s\n", myProcId, dirname,strerror(errno));  
    return -1;       
  } 
  ret += do_check_max_xattr(dirname,0);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,dirname);
    return;
  }   
    
  unlink(symlink);
  sprintf(cmd, "ln -s %s %s", filename, symlink); 
  if (system(cmd) == -1) {
      return -1;
  }  
  ret += do_check_max_xattr(symlink,1);
  if (ret != 0) {
    printf("LINE %d file %s\n",__LINE__,symlink);
    return;
  }  
  
  while (1) {
    count++;    

    if  (do_one_test(filename,count) != 0) {
      printf("proc %3d - ERROR in loop %d %s\n", myProcId, count,filename); 
      return -1;
    } 
   
    if  (do_one_test(dirname,count) != 0) {
      printf("proc %3d - ERROR in loop %d %s\n", myProcId, count,dirname); 
      return -1;
    }         
   
    if  (do_one_test(symlink,count) != 0) {
      printf("proc %3d - ERROR in loop %d %s\n", myProcId, count,symlink); 
      return -1;
    }         
   
    if  (do_one_test_symlink(symlink,count) != 0) {
      printf("proc %3d - ERROR in loop %d %s\n", myProcId, count,symlink); 
      return -1;
    } 
        
    if (loop==count) {   
      unlink(symlink);
      unlink(filename);                 
      rmdir(dirname);
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
    printf("B-mount is mandatory\n");
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
