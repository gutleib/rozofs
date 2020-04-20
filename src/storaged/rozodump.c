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
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <pthread.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <sys/resource.h>
#include <unistd.h>
#include <libintl.h>
#include <sys/poll.h>
#include <rpc/rpc.h>
#include <rpc/pmap_clnt.h>
#include <libconfig.h>
#include <getopt.h>
#include <inttypes.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/vfs.h>
#include <ctype.h>
#include <uuid/uuid.h>

#include <rozofs/rozofs_srv.h>
#include <rozofs/core/rozofs_string.h>
#include <rozofs/common/common_config.h>
#include <rozofs/rpc/eproto.h>
#include "storage.h"
#include "storio_crc32.h"
#include "rbs_sclient.h"
#define VERY_LAST (0x100000000000)

int firstBlock = 0;
char * filename[128] = {NULL};
int    fd[128] = {-1};

int    nb_file = 0;
int    block_number=-1;
int    bsize=0;
int    bbytes=-1;
int    dump_data=0;
uint64_t  first=0,last=VERY_LAST;
unsigned int prjid = -1;
int    display_blocks=0;
uint64_t   patched_date = 0;
int    silent_header = 0;
int    nocolor = 0;

#define HEXDUMP_COLS 16
void hexdump(void *mem, unsigned int offset, unsigned int len) {
  unsigned int i, j;

  for(i = 0; i < len + ((len % HEXDUMP_COLS) ? (HEXDUMP_COLS - len % HEXDUMP_COLS) : 0); i++) {
          /* print offset */
    if(i % HEXDUMP_COLS == 0) {
      printf("0x%06x: ", i+offset);
    }

    /* print hex data */
    if(i < len) {
      printf("%02x ", 0xFF & ((char*)mem)[i]);
    }
    else /* end of block, just aligning for ASCII dump */{
      printf("%s","   ");
    }

    /* print ASCII dump */
    if(i % HEXDUMP_COLS == (HEXDUMP_COLS - 1)) {
      for(j = i - (HEXDUMP_COLS - 1); j <= i; j++) {
        if(j >= len) /* end of block, not really printing */{
          printf("%c",' ');
        }
        else if(isprint(((char*)mem)[j])) /* printable char */{
	  printf("%c",0xFF & ((char*)mem)[j]);        
        }
        else /* other char */{
          printf("%c",'.');
        }
      }	
      printf("%c",'\n');
    }
  }
}

int rozofs_storage_get_device_number(char * root) {
  int  device;
  char path[256];
  
  for (device=0; device < 122; device++) {
    sprintf(path, "%s/%d", root, device);
    if (access(path, F_OK) == -1) return device; 
  }
  return 0;
}

int read_hdr_file(char * root, int devices, int slice, rozofs_stor_bins_file_hdr_vall_t * hdr, uuid_t uuid, int *spare) {
  char             path[256];
  int              dev;
  int              Zdev=-1;
  struct stat      st;
  uint64_t         ts=0;
  int              fd;
  uint8_t          safe;
  uint8_t          fwd;
  uint8_t          inv;
  char            *pChar;
  uint32_t        *pCrc;
  uint32_t         crcLen;
  int              i;
  sid_t         * pDist;
  uint8_t val;
  
  for (*spare=0; *spare<2; *spare+=1) {
  
    for (dev=0; dev < devices; dev++) {
    
      pChar = storage_build_hdr_path(path,root,dev, *spare, slice);
      rozofs_uuid_unparse(uuid, pChar);
      
      // Check that the file exists
      if (stat(path, &st) == -1) {
        continue;
      }

      // 1rst header file found
      if (Zdev == -1) {
        Zdev = dev;
	ts   = st.st_mtime;
        continue;
      }
      
      // An other header file found
      if (st.st_mtime > ts) {
        // More recently modified
        Zdev = dev;
	ts   = st.st_mtime;
        continue;	
      }	     
    }
    
    //Header file has been found. Read it 
    if (Zdev != -1) {
      pChar = storage_build_hdr_path(path,root,Zdev, *spare, slice);
      rozofs_uuid_unparse(uuid, pChar);
      // Open hdr file
      fd = open(path, ROZOFS_ST_NO_CREATE_FILE_FLAG, ROZOFS_ST_BINS_FILE_MODE);
      if (fd < 0) {
	printf("open(%s) %s",path, strerror(errno));
	return -1;
      }
      int nb_read = pread(fd, hdr, sizeof (*hdr), 0);
      if (nb_read < 0) {
        printf("pread(%s) %s",path, strerror(errno));
	return -1;  
      } 
      
      close(fd);  

      if (silent_header) return 0;

      printf("{ \"header file\" : {\n");
      printf("  \"path\"    : \"%s\",\n",path);
      printf("  \"version\" : %d,\n",hdr->v0.version);
      printf("  \"layout\"  : %d,\n",hdr->v0.layout);
      printf("  \"bsize\"   : %d,\n",hdr->v0.bsize);
      switch(hdr->v0.version) {
        case 0:
          rozofs_get_rozofs_invers_forward_safe(hdr->v0.layout, &inv, &fwd, &safe);
          pDist = hdr->v0.dist_set_current;
          break;
        case 1:  
          rozofs_get_rozofs_invers_forward_safe(hdr->v1.layout, &inv, &fwd, &safe);
          pDist = hdr->v1.dist_set_current;
          printf("  \"cid\"     : %d,\n", hdr->v1.cid);
          printf("  \"sid\"     : %d,\n", hdr->v1.sid);
          break;
        case 2:
          rozofs_get_rozofs_invers_forward_safe(hdr->v2.layout, &inv, &fwd, &safe);
          pDist = hdr->v2.distrib;
          printf("  \"cid\"     : %d,\n", hdr->v2.cid);
          printf("  \"sid\"     : %d,\n", hdr->v2.sid);
          memset(&hdr->v2.devFromChunk[hdr->v2.nbChunks], ROZOFS_EOF_CHUNK, ROZOFS_STORAGE_MAX_CHUNK_PER_FILE-hdr->v2.nbChunks);
          break;
        default:
          printf("Unknown header version\n");
          continue;
          break;     	 
      }    
      crcLen = rozofs_st_get_header_file_crc(hdr, &pCrc);
      
      printf("  \"distibution\" : {\n");      
      printf("      \"inverse\" : [");      
      printf(" %d", pDist[0]);
      for (i=1; i< inv; i++) {
        printf(", %d",pDist[i]);
      }
      printf("],\n      \"forward\" : [ %d",pDist[i]);      
      i++;
      for (  ; i< fwd; i++) {
        printf(", %d",pDist[i]);
      }
      printf("],\n      \"safe\"    : [ %d",pDist[i]);      
      i++;
      for (  ; i< safe; i++) {
        printf(", %d",pDist[i]);
      }          
      printf("]\n  },\n");
          

      i = 0;
      val = rozofs_st_header_get_chunk(hdr,i);
      printf("  \"devices\" : [ %d", val);
      i++;
      while(i<ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) {
        val = rozofs_st_header_get_chunk(hdr,i);
        if (val == ROZOFS_EOF_CHUNK) break;
        if (val == ROZOFS_EMPTY_CHUNK) printf(", E");
        else printf(", %d",val);
        i++;
      }
      printf("],\n");
            
      uint32_t save_crc32 = *pCrc;;
      printf("  \"crc32\"   : ");
      if (save_crc32 == 0) {
        printf("\"None\"\n");
      }
      else { 
        *pCrc = 0;
        uint32_t crc32;
        crc32 = fid2crc32((uint32_t *)uuid);   
        crc32 = crc32c(crc32,(char *) hdr, crcLen);
        *pCrc = save_crc32;

	if (save_crc32 != crc32) {
          printf("BAD %x. Rxpecting %x\"\n",save_crc32,crc32);
          continue;
	}	
        printf("\"OK\"\n");
      }
      printf("}}\n");
      return 0;  	
    }            
    // Check for spare
  }
  return -1;
}
char dateSting[128];
char * ts2string(uint64_t u64) {
  time_t   input = (u64/1000000);
  uint64_t micro = (u64%1000000);
  struct tm  ts;
  int len=0;
  
  if (u64==0) {
    sprintf(&dateSting[len],"Empty");
    return dateSting;    
  }

  ts = *localtime(&input);
  strftime(dateSting, sizeof(dateSting), "%a %Y-%m-%d %H:%M:%S:", &ts);
  len= strlen(dateSting);
  len += sprintf(&dateSting[len],"%6.6u",(unsigned int)micro);
  sprintf(&dateSting[len]," %llu",(unsigned long long int)u64);
  return dateSting;
}    
unsigned char * buffer;
void read_chunk_file(uuid_t fid, char * path, rozofs_stor_bins_file_hdr_vall_t * hdr, int spare, uint64_t firstBlock) {
  uint16_t rozofs_disk_psize;
  int      fd;
  rozofs_stor_bins_hdr_t    * pH;
  rozofs_stor_bins_footer_t * pF;
  int      nb_read;
  uint32_t bbytes = ROZOFS_BSIZE_BYTES(hdr->v0.bsize);
  char     crc32_string[64];
  uint64_t offset;
  uint64_t last_offset;
  char    *color;
  uint64_t bufferSize = 0;
  
  if (dump_data == 0) {
    printf ("+------------+------------------+------------+----+------+-------+--------------------------------------------\n");
    printf ("| %10s | %16s | %10s | %2s | %4s | %5s | %s\n", "block#","file offset", "prj offset", "pj", "size", "crc32", "date");
    printf ("+------------+------------------+------------+----+------+-------+--------------------------------------------\n");
  }

  // Open bins file
  fd = open(path, ROZOFS_ST_NO_CREATE_FILE_FLAG, ROZOFS_ST_BINS_FILE_MODE_RO);
  if (fd < 0) {
    printf("open(%s) %s\n",path,strerror(errno));
    return;	
  }

  /*
  ** Retrieve the projection size on disk
  */
  rozofs_disk_psize = rozofs_get_max_psize_in_msg(hdr->v0.layout,hdr->v0.bsize);
  if (spare==0) {
  
    /* Header version 1. Find the sid in  the distribution */
    if ((hdr->v0.version == 2)||(hdr->v0.version == ROZOFS_STORAGE_HDR_VERSION_MONODEV)) {
      int fwd = rozofs_get_rozofs_forward(hdr->v2.layout);
      int idx;
      for (idx=0; idx< fwd;idx++) {
	if (hdr->v2.distrib[idx] != hdr->v2.sid) continue;
	rozofs_disk_psize = rozofs_get_psizes_on_disk(hdr->v2.layout,hdr->v2.bsize,idx);
	break; 
      }
    }  
    else if (hdr->v0.version == 1) {
      int fwd = rozofs_get_rozofs_forward(hdr->v1.layout);
      int idx;
      for (idx=0; idx< fwd;idx++) {
	if (hdr->v1.dist_set_current[idx] != hdr->v1.sid) continue;
	rozofs_disk_psize = rozofs_get_psizes_on_disk(hdr->v1.layout,hdr->v1.bsize,idx);
	break; 
      }
    }  
    /* Projection id given as parameter */
    else if (prjid != -1) {
      rozofs_disk_psize = rozofs_get_psizes_on_disk(hdr->v0.layout,hdr->v0.bsize,prjid);
    }
    
    /*  Version 0 without projection given as parameter*/
    else {
      rozofs_stor_bins_hdr_t proj_hdr;
      // Read 1rst block
      nb_read = pread(fd, &proj_hdr, sizeof(rozofs_stor_bins_hdr_t), 0);
      if (nb_read<0) {
	printf("pread(%s) %s\n",path,strerror(errno));
	return;      
      }
      if (proj_hdr.s.timestamp == 0) {
	printf("Can not tell projection id\n");
	return;            
      }
      rozofs_disk_psize = rozofs_get_psizes_on_disk(hdr->v0.layout,hdr->v0.bsize,proj_hdr.s.projection_id);
    }
  }
  
  /*
  ** Do not read more than 64K blocks in a run.
  ** Try to read the exact requested number of block
  */
  bufferSize = (last + 1) - first; /* This is requested */
  if (bufferSize > (64*1024)) bufferSize = 64*1024; /* Requested was too big */
  bufferSize *= rozofs_disk_psize;
  buffer = malloc(bufferSize);

  /*
  ** Where to start reading from 
  */
  last_offset = (last + 1)*rozofs_disk_psize;
  if (first == 0) { 
    offset = 0;
  }
  else {
    if (first <= firstBlock) {
      offset = 0;
    }
    else {
      offset = (first-firstBlock)*rozofs_disk_psize;
    }
    last_offset -= (firstBlock*rozofs_disk_psize);
  }
  
  int idx;
  nb_read = 1;
  uint64_t bid;
  
  /*
  ** Reading blocks
  */  
  while (nb_read) {
    uint64_t to_read;
  
    // Read nb_proj * (projection + header)
    
    to_read = last_offset - offset; /* Left to read */
    if (to_read > bufferSize) to_read = bufferSize; /* Requested was to big */
    if (to_read == 0) {
      close(fd); free(buffer);
      return;         
    }
    //info("To read %llu",(long long unsigned int) to_read);   
    nb_read = pread(fd, buffer, to_read , offset);
    if (nb_read<0) {
      printf("pread(%s) %s\n",path,strerror(errno));
      close(fd); free(buffer);
      return;         
    }
    //info("Read %llu",(long long unsigned int) nb_read);   
    
    nb_read = (nb_read / rozofs_disk_psize);
    
    pH = (rozofs_stor_bins_hdr_t*) buffer;

    for (idx=0; idx<nb_read; idx++) {
    
      pH = (rozofs_stor_bins_hdr_t*)    &buffer[idx*rozofs_disk_psize];
      pF = (rozofs_stor_bins_footer_t*) &buffer[(idx+1)*rozofs_disk_psize];
      pF--;
 
      if ((spare) && (pH->s.timestamp != 0)) {
        uint64_t footer_offset = idx*rozofs_disk_psize;
        if ((pH->s.timestamp != 0) && (pH->s.effective_length!=0)){
          footer_offset += rozofs_get_psizes_on_disk(hdr->v2.layout,hdr->v2.bsize,pH->s.projection_id);
          pF = (rozofs_stor_bins_footer_t*) &buffer[footer_offset];
          pF--;
        } 
      }
            
      bid = (offset/rozofs_disk_psize)+idx+firstBlock;
      
      if (bid < first) continue;
      if (bid > last)  break;
     
      uint32_t save_crc32 = pH->s.filler;
      pH->s.filler = 0;
      uint32_t crc32=0;

      /*
      ** We are requested to patch the date in header as well as footer
      */
      if (patched_date!= 0) {
        /* CRC32 is set to 0 which means no CRC32 */
        save_crc32 = 0;
        pH->s.timestamp = patched_date;
        pF->timestamp   = patched_date;
        /* Re-write header */
        nb_read = pwrite(fd, pH, sizeof(rozofs_stor_bins_hdr_t), offset+((unsigned char*)pH-buffer));
        if (nb_read<sizeof(rozofs_stor_bins_hdr_t)) {
          printf("pwrite header %llu %s\n",(long long unsigned int)bid,strerror(errno));
          close(fd); free(buffer);
          return;         
        }        
        /* Re-write footer */
        nb_read = pwrite(fd, pF, sizeof(rozofs_stor_bins_footer_t), offset+((unsigned char*)pF-buffer));
        if (nb_read<sizeof(rozofs_stor_bins_footer_t)) {
          printf("pwrite footer %llu %s\n",(long long unsigned int)bid,strerror(errno));
          close(fd); free(buffer);
          return;         
        }                
      }
      
      if (save_crc32 == 0) {
        color = ROZOFS_COLOR_YELLOW;
        sprintf(crc32_string,"NONE");
      }
      else {
        crc32 = fid2crc32((uint32_t *)fid)+bid-firstBlock;
        crc32 = crc32c(crc32,(char *) pH, rozofs_disk_psize);
	if (crc32 != save_crc32) {
          color = ROZOFS_COLOR_RED;
          sprintf(crc32_string,"ERROR");
        }
	else {
          color = ROZOFS_COLOR_GREEN;
          sprintf(crc32_string,"OK");
        }  
      }
      pH->s.filler = save_crc32;
      	
      if (dump_data == 0) {
	printf ("| %10llu | %16llu | %10llu | %2d | ",
        	(long long unsigned int)bid,
        	(long long unsigned int)bbytes * bid,
        	(long long unsigned int)offset+(idx*rozofs_disk_psize),
                pH->s.projection_id); 
                
        if (pH->s.effective_length==0) {
          if (!nocolor) printf(ROZOFS_COLOR_YELLOW);
          printf ("%4d",pH->s.effective_length);
          if (!nocolor) printf(ROZOFS_COLOR_NONE);
        }       
        else {
          printf ("%4d",pH->s.effective_length);
        }       
        
        if (!nocolor) printf("%s",color);
	printf (" | %5s | ", crc32_string);
        if (!nocolor) printf(ROZOFS_COLOR_NONE);
                
	printf ("%s ", ts2string(pH->s.timestamp));
        if ((pH->s.timestamp!=0) && (pH->s.timestamp != pF->timestamp)){
          if (!nocolor) printf(ROZOFS_COLOR_RED);
 	  printf("%llu\n",(long long unsigned int)pF->timestamp);
          if (!nocolor) printf(ROZOFS_COLOR_NONE);
        }
        else {
 	  printf("\n");
        }         
      }		
      else {
	printf("_________________________________________________________________________________________\n");
	printf("Block# %llu / file offset %llu / projection offset %llu\n", 
        	(unsigned long long)bid, (unsigned long long)(bbytes * bid), (unsigned long long)(offset+(idx*rozofs_disk_psize)));
        if (nocolor) {
	  printf("prj id %d / length %d / CRC %s / time stamp %s", 
        	  pH->s.projection_id,pH->s.effective_length,
                  crc32_string, 
                  ts2string(pH->s.timestamp));
        }
        else {
	  printf("prj id %d / length %d / CRC %s %s %s / time stamp %s", 
        	  pH->s.projection_id,pH->s.effective_length,
                  color, crc32_string, ROZOFS_COLOR_NONE,
                  ts2string(pH->s.timestamp));
        }          
        if (pH->s.timestamp != pF->timestamp){        
          if (!nocolor) printf(ROZOFS_COLOR_RED);
          printf(" %llu",(long long unsigned int)pF->timestamp); 
          if (!nocolor) printf(ROZOFS_COLOR_NONE);
        }   	
	printf("\n_________________________________________________________________________________________\n");
	if ((pH->s.projection_id == 0)&&(pH->s.timestamp==0)) continue;
	hexdump(pH, (offset+(idx*rozofs_disk_psize)), rozofs_disk_psize);      	            
      }
    }
    offset += (nb_read*rozofs_disk_psize);
  }
  if (dump_data == 0) {
    printf ("+------------+------------------+------------+----+------+-------+--------------------------------------------\n");
  }
  close(fd); free(buffer);
  
}    
/*_________________________________________________________________________
**  Build speudo header file from information got from the exportd
**
**  @param hdr          The header file to initialize
**  @param pRoot        The storage root path
**  @param spare        Whether this storage is spare for this FID
**  
**  @retval 0 when OK. -1 in case of any error
**_________________________________________________________________________
*/
static inline int rozodump_build_header(rozofs_stor_bins_file_hdr_vall_t * hdr, char * pRoot, int * spare) {
  ep_mattr_t attr;
  uint32_t   bsize;
  uint8_t    layout;
  char     * pChar;
  uint32_t   sid;
  int        ret;
  uint8_t    safe;
  uint8_t    fwd;
  uint8_t    inv;
  int        idx;

  hdr->version = ROZOFS_STORAGE_HDR_VERSION_MONODEV; /* monodevice without header file */

  /*
  ** Get current sid from storage path
  */
  pChar = pRoot + strlen(pRoot);
  while(*pChar != '_') pChar--;
  pChar++;
  ret = sscanf(pChar, "%u", (unsigned int *) &sid);
  if (ret != 1) {
    printf("Can not get SID from root path %s !!!\n",pRoot);  
    return -1;    
  }

  /*
  ** Get distribution from exportd
  */
  ret = rbs_get_fid_attr(common_config.export_hosts, hdr->v2.fid, &attr, &bsize, &layout);
  if (ret != 0) {
    printf("Can not get FID from export %s !!!\n",strerror(errno));  
    return -1;    
  }
  rozofs_get_rozofs_invers_forward_safe(layout, &inv, &fwd, &safe);

  hdr->v2.bsize  = bsize;
  hdr->v2.layout = layout;
  hdr->v2.cid    = attr.cid;
  hdr->v2.sid    = sid;
  memcpy(hdr->v2.distrib, attr.sids, sizeof(hdr->v2.distrib));
  /*
  ** Find out whther this SID is spare
  */
  *spare = 1;
  for (idx = 0; idx < fwd; idx++) {
    if (sid == hdr->v2.distrib[idx]) {
      *spare = 0;
      break;
    }  
  }
  return 0;
}   
/*_________________________________________________________________________
**  usage
**_________________________________________________________________________
*/ 
char * utility_name=NULL;
char * input_file_name = NULL;
void usage() {

    printf("\nRozoFS projection file reader - %s\n", VERSION);
    printf("Usage: %s [OPTIONS] [PARAMETERS]\n",utility_name);
    printf("   PARAMETERS:\n");    
    printf("   -p <root path>       \tThe storage root path\n");
    printf("   -f <fid>             \tThe file FID\n");
    printf("   OPTIONS:\n");
    printf("   -h                   \tPrint this message.\n");
    printf("   -a                   \tGive the projection id in the case it can not be determine automatically.\n");    
    printf("   -d                   \tTo dump the data blocks and not only the block headers\n");
    printf("   -b                   \tDisplay all blocks.\n");
    printf("   -b <first>:<last>    \tTo display block numbers within <first> and <last>.\n"); 
    printf("   -b <first>:          \tTo display from block number <first> to the end.\n");
    printf("   -b :<last>           \tTo display from start to block number <last>.\n");  
    printf("   -b <block>           \tTo display only <block> block number.\n");   
    printf("   -D <date>            \tTo patch the date of the blocks given by -b option.\n");   
    printf("   -s                   \tDo not display header files.\n");   
    printf("   --nocolor            \tOutput without colorization.\n");   
    exit(-1);  
}


int main(int argc, char *argv[]) {
  int           idx=1;
  uuid_t        fid;
  char        * pFid=NULL;
  char        * pRoot=NULL;
  int           ret;
  int           slice;
  int           devices;
  rozofs_stor_bins_file_hdr_vall_t hdr;
  char          path[256];
  int           spare;
  bid_t         block_per_chunk;
  int           chunk;
  int           chunk_stop;
  int           monodev = 0;

  /*
  ** read common config file (to get number of slices)
  */
  common_config_read(NULL); 
  
  
  // Get utility name
  utility_name = basename(argv[0]); 
  
  if (argc < 3) usage(); 

  idx = 1;
  while (idx < argc) {

    /* -h */
    if (strcmp(argv[idx], "-h") == 0) usage();
    
    /* -d */
    if (strcmp(argv[idx], "-d") == 0) {
      idx++;
      dump_data = 1;
      continue;
    }
    
    /* -f */
    if (strcmp(argv[idx], "-f") == 0) {
      idx++;
      if (idx == argc) {
        printf("%s option set but missing value !!!\n", argv[idx-1]);
        usage();
      } 
      pFid = argv[idx];
      ret = rozofs_uuid_parse( pFid, fid);
      if (ret != 0) {
        printf("%s is not a FID !!!\n", pFid);
        usage();
      }  
      idx++;
      continue;    
    }

    /* -p */
    if (strcmp(argv[idx], "-p") == 0) {
      idx++;
      if (idx == argc) {
        printf("%s option set but missing value !!!\n", argv[idx-1]);
        usage();
      } 
      pRoot = argv[idx];
      struct statfs st;
      if (statfs(pRoot, &st) != 0) {
        printf("%s is not a directory !!!\n", pRoot);
        usage();
      }
      idx++;
      continue;    
    }
    
    /* -s */
    if (strcmp(argv[idx], "-s") == 0) {
      idx++;
      silent_header = 1;
      continue;    
    }
    
    /* --nocolor */
    if (strcmp(argv[idx], "--nocolor") == 0) {
      idx++;
      nocolor = 1;
      continue;    
    }
    
    /* -D <date>*/
    if (strcmp(argv[idx], "-D") == 0) {
      idx++;
      if (idx == argc) {
        printf("%s option set but missing value !!!\n", argv[idx-1]);
        usage();
      } 
      ret = sscanf(argv[idx], "%llu", (long long unsigned int *)&patched_date);
      if (ret != 1) {
        printf("Bad date value %s !!!\n",argv[idx]);
	usage();
      }
      idx++;
      continue;    
    }
    
    /* -a */
    if (strcmp(argv[idx], "-a") == 0) {
      idx++;
      if (idx == argc) {
        printf("%s option set but missing value !!!\n", argv[idx-1]);
        usage();
      } 
      ret = sscanf(argv[idx], "%u", &prjid);
      if (ret != 1) {
        printf("Bad projection identifier %s !!!\n",argv[idx]);
	usage();
      }
      idx++;
      continue;
    }
            
    /* -b */
    if (strcmp(argv[idx], "-b") == 0) {
      display_blocks = 1;
      idx++;
      if (idx == argc) {
        continue;
      } 
      
      ret = sscanf(argv[idx], "%llu:%llu", (long long unsigned int *)&first, (long long unsigned int *)&last);
      if (ret == 2) {
        if (first>last) {
	  printf("first block index %llu must be lower than last block index %llu !!!\n", (long long unsigned int)first, (long long unsigned int)last);
	  usage();
	}
        idx++;
	continue;
      }
      
      ret = sscanf(argv[idx], ":%llu", (long long unsigned int *)&last);
      if (ret == 1) {
        first = 0;
        idx++;
	continue;
      }
            
      ret = sscanf(argv[idx], "%llu:", (long long unsigned int *)&first);
      if (ret == 1) {
        last = VERY_LAST;
        idx++;
	continue;
      }
      ret = sscanf(argv[idx], "%llu", (long long unsigned int *)&first);
      if (ret == 1) {
        last = first;
        idx++;
	continue;
      }
   
      continue;
    }
    
    printf("No such option or parameter %s\n",argv[idx]);
    usage();
  }
  
  if ((pFid == NULL)||(pRoot == NULL)) {
    printf("Missing mandatory parameters !!!\n");
    usage();
  }

  rozofs_layout_initialize();
  
  /*
  ** Compute the FID slice
  */
  slice = rozofs_storage_fid_slice(fid);
  
  /*
  ** Get the number of devices
  */
  devices = rozofs_storage_get_device_number(pRoot);
  
  /*
  ** Find out a header file
  */
  if (read_hdr_file(pRoot,devices, slice, &hdr, fid, &spare)!= 0) {
    printf("No header file found for %s under %s !!!\n",pFid,pRoot);
    if (devices !=1 ) {
      return -1;
    }  
    /*
    ** This may be a mono device config without header file
    */
    monodev = 1;
  }
  
  if (monodev) {    
    block_per_chunk = ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(0) * ROZOFS_STORAGE_MAX_CHUNK_PER_FILE;
    chunk      = 0;
    chunk_stop = 1;
    /* 
    ** Build pseudo header file
    */
    memcpy(hdr.v2.fid,fid,sizeof(fid_t));
    if (rozodump_build_header(&hdr,pRoot,&spare) != 0) {
      exit(1);
    }  
      
  }
  else {
    block_per_chunk = ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(hdr.v0.bsize);
    if (first == 0) {
      chunk = 0;
    }
    else {
      chunk = first / block_per_chunk;
    }  


    if (last == -1) {
      chunk_stop = ROZOFS_STORAGE_MAX_CHUNK_PER_FILE+1;
    }
    else {
      chunk_stop = (last / block_per_chunk)+1;
    }  
  }
     
  while(chunk<chunk_stop) {
    int dev;
    
    dev = rozofs_st_header_get_chunk(&hdr, chunk);
      
    if (dev == ROZOFS_EOF_CHUNK) {
      printf ("\n============ CHUNK %d EOF   ================\n", chunk);      
      break;
    }
    if (dev == ROZOFS_EMPTY_CHUNK) {
      printf ("\n============ CHUNK %d EMPTY ================\n", chunk);
      chunk++;
      continue;
    }
    
    storage_build_chunk_full_path(path, pRoot, dev, spare, slice, fid, chunk);
    printf ("\n============ CHUNK %d ==  %s ================\n", chunk, path);
    
    if (display_blocks) {
      read_chunk_file(fid,path,&hdr,spare, chunk*block_per_chunk);
    }  
    chunk++;
  }
  return 0;
}
