#include <time.h>
#include <errno.h>
#include <sys/types.h>
#include <utime.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>

#define ROZOFS_RESIZEA 0x20524553495A4541LL
#define ROZOFS_RESIZEM 0x20524553495A454DLL
int resize(char * fname) {
  struct utimbuf times;
  int ret;
  
  times.actime  = ROZOFS_RESIZEA;
  times.modtime = ROZOFS_RESIZEM;
  ret = utime(fname, &times);
  if (ret < 0) {
    printf ("%s %s\n",fname,strerror(errno));
    return -1;
  }
  return 0;
}
main(int argc, char **argv) {
  resize(argv[1]);
}  
