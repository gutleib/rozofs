#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
 
#define BLKSIZE 4096
char block[BLKSIZE];

void mmap_read(char * filename, int nbblocks) {
  int           fd;
  char        * addr;
  int           idx;
  int           ret;      

  fd = open(filename, O_RDONLY);
  if (fd == -1) {
    printf("open(%s,O_RDONLY) error %s\n",filename,strerror(errno));
    exit(EXIT_FAILURE);
  }
  
  addr = (char *) mmap(NULL, nbblocks*BLKSIZE , PROT_READ, MAP_PRIVATE, fd, 0);
  if (addr == MAP_FAILED) {
    printf("mmap(%s) read  %s\n", filename, strerror(errno));
    exit(EXIT_FAILURE);
  }
  
  for (idx=0; idx < BLKSIZE; idx++) {
    block[idx] = idx;
  }  
  
  ret = 0;
  for (idx=0; idx < nbblocks; idx++) {
    block[0] = idx;
    if (memcmp(&addr[idx*BLKSIZE], block, BLKSIZE)!=0) {
      printf("%s Block %d differs\n", filename, idx);
      ret ++;
    }
  }
  munmap(addr, nbblocks*BLKSIZE);
  close(fd);  
  if (ret)  exit(EXIT_FAILURE);
  exit(EXIT_SUCCESS); 
}
int mmap_write(char * filename, int nbblocks) {
  int           fd;
  char        * addr;
  int           idx;
  unlink(filename);
  
  fd = open(filename, O_RDWR|O_CREAT|O_TRUNC,0777);
  if (fd == -1) {
    printf("open(%s,O_WRONLY) error %s\n",filename,strerror(errno));
    exit(EXIT_FAILURE);
  }
  if (ftruncate(fd, nbblocks*BLKSIZE) < 0) {
    printf("ftruncate(%s) error %s\n",filename,strerror(errno));
    exit(EXIT_FAILURE);
  }

  addr = (char *) mmap(NULL, nbblocks*BLKSIZE , PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
  if (addr == MAP_FAILED) {
    printf("mmap(%s) write  %s\n", filename, strerror(errno));
    exit(EXIT_FAILURE);
  }

  for (idx=0; idx < BLKSIZE; idx++) {
    block[idx] = idx;
  }  
  for (idx=0; idx < nbblocks; idx++) {
    block[0] = idx;
    memcpy(&addr[idx*BLKSIZE], &block[0], BLKSIZE);
  }
  
  if (msync(addr, nbblocks*BLKSIZE, MS_SYNC) < 0) {
    printf("msync(%s)  %s\n", filename, strerror(errno));
    exit(EXIT_FAILURE);
  }
  munmap(addr, nbblocks*BLKSIZE);
  close(fd);
  exit(EXIT_SUCCESS); 
}    
int main(int argc, char *argv[]) {
  int nbblocks =0;
  
  if (argc < 4 ) {
    printf(" <filename> <read|write> <blocks>\n");
    exit(EXIT_FAILURE);
  }
  
  if (sscanf(argv[3],"%d",&nbblocks) != 1) {
    printf("Bad block number : <filename> <read|write> <blocks>\n");
    exit(EXIT_FAILURE);
  }
    
  if (strcmp(argv[2],"read") == 0) 
    mmap_read(argv[1], nbblocks);
  else if (strcmp(argv[2],"write") == 0) 
    mmap_write(argv[1], nbblocks); 
  printf("No such action %s\n", argv[2]);  
  exit(EXIT_FAILURE);
}
