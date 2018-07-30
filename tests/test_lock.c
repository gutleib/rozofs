#include <sys/fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
    int      duration = 0;
  
set_lock(int fd,int type, long long unsigned int start, long long unsigned len) {
    struct flock fl = {};

    fl.l_type = type;
    fl.l_whence = SEEK_SET;
    fl.l_start = start;
    fl.l_len = len;
    
    switch(type) {
      case F_WRLCK:   
        printf("Request WRITE lock [%llu:%llu[\n", start, start+len);
        break;
      case F_RDLCK:
        printf("Request READ lock [%llu:%llu[\n", start, start+len);
        break;
      case F_UNLCK:
        printf("Release lock [%llu:%llu[\n", start, start+len);
        break;
    }
                     
    if (fcntl(fd, F_SETLKW, &fl) == -1) {  
      perror("fcntl");
      exit(1);
    }
    switch(type) {
      case F_WRLCK:   
      case F_RDLCK:
        printf("\033[92m__GOT_IT_FOR_%d_SECONDES__\033[0m\n", duration);
        break;
      case F_UNLCK:
        printf("Released\n\n\n\n\n\n");
        break;
    }
}
set_lock_start(int fd,int type, long long unsigned len) {
   set_lock(fd,type,0,len);
}
set_lock_size(int fd, int type, long long unsigned start) {
   set_lock(fd,type,start,0);
}
set_lock_total(int fd,int type) {
    set_lock_size(fd,type,0);
}
int main(int argc, char **argv) {
    unsigned long long begin = 0;
    unsigned long long  size   = 0;
    char   * fileName = NULL;
    int      idx;
  
    if (argc < 2) {
        fprintf(stderr, "usage: %s <file> [ -b <begin>] [-s <size>] [-d <duration>] \n", argv[0]);
        exit(1);
    }
    
    idx=1;
    while (idx < argc) {
      if (strcmp(argv[idx],"-b")==0) {
        idx++;
        if (idx == argc) {
          printf("%s option without value !!!", argv[idx-1]);
          exit(1);
        }
        if (sscanf(argv[idx],"%llu",&begin) != 1) {
          printf("Unexpected value for %s option \"%s\" !!!", argv[idx-1], argv[idx]);
          exit(1);
        }
        idx++;
        continue;  
      }
      if (strcmp(argv[idx],"-s")==0) {
        idx++;
        if (idx == argc) {
          printf("%s option without value !!!", argv[idx-1]);
          exit(1);
        }
        if (sscanf(argv[idx],"%llu",&size) != 1) {
          printf("Unexpected value for %s option \"%s\" !!!", argv[idx-1], argv[idx]);
          exit(1);
        }
        idx++;
        continue;  
      }
      if (strcmp(argv[idx],"-d")==0) {
        idx++;
        if (idx == argc) {
          printf("%s option without value !!!", argv[idx-1]);
          exit(1);
        }
        if (sscanf(argv[idx],"%d",&duration) != 1) {
          printf("Unexpected value for %s option \"%s\" !!!", argv[idx-1], argv[idx]);
          exit(1);
        }
        idx++;
        continue;  
      }
      if (fileName != NULL) {
        printf("2 file names \"%s\" \"%s\" !!!", fileName, argv[idx]);
        exit(1);
      }
      fileName = argv[idx];
      idx++;
    }      
      
    while (1) {
      int fd = open(fileName, O_RDWR);
      if (fd == -1) {
        perror("open");
        exit(1);
      }
      set_lock(fd,F_WRLCK,begin,size);
      if (duration) {
        sleep(duration);
      }
      else {
        while(1) sleep(60);
      }    
      set_lock(fd, F_UNLCK, begin,size);
      sleep(1);

    }   
//    set_lock_size(fd,F_WRLCK, 20000);
//    set_lock(fd,F_WRLCK, 50,10);


    
    exit(0);
}
