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

#ifndef _STORAGE_HEADER_H
#define _STORAGE_HEADER_H

#include <stdint.h>
#include <limits.h>
#include <uuid/uuid.h>
#include <sys/param.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <rozofs/rozofs.h>

//__Specific values in the chunk to device array
// Used in the interface When the FID is not inserted in the cache and so the
// header file will have to be read from disk.
#define ROZOFS_UNKNOWN_CHUNK  255
// Used in the device per chunk array when no device has been allocated 
// because the chunk is after the end of file
#define ROZOFS_EOF_CHUNK      254
// Used in the device per chunk array when no device has been allocated 
// because it is included in a whole of the file
#define ROZOFS_EMPTY_CHUNK    253

/*_________________________________________________
** V0 header file
*/
typedef struct _rozofs_stor_bins_file_hdr_v0_t {
  uint8_t version; ///<  version of rozofs. (not used yet)
  uint8_t layout; ///< layout used for this file.
  uint8_t bsize;  ///< Block size as defined in enum ROZOFS_BSIZE_E
  fid_t   fid;
  sid_t   dist_set_current[ROZOFS_SAFE_MAX]; ///< currents sids of storage nodes target for this file.
  uint8_t devFromChunk[ROZOFS_STORAGE_MAX_CHUNK_PER_FILE]; // Device number that hold the chunk of projection
  uint32_t crc32; ///< CRC32 . Set to 0 by default when no CRC32 is computed
} rozofs_stor_bins_file_hdr_v0_t;






/*_________________________________________________
** V1 header file
** start with exactly the same fields as v0 at the same offset
** + new fileds at the end : cid & sid
*/
typedef struct _rozofs_stor_bins_file_hdr_v1_t {
  uint8_t  version; ///<  version of rozofs. (not used yet)
  uint8_t  layout; ///< layout used for this file.
  uint8_t  bsize;  ///< Block size as defined in enum ROZOFS_BSIZE_E
  fid_t    fid;
  sid_t    dist_set_current[ROZOFS_SAFE_MAX]; ///< currents sids of storage nodes target for this file.
  uint8_t  devFromChunk[ROZOFS_STORAGE_MAX_CHUNK_PER_FILE]; // Device number that hold the chunk of projection
  uint32_t crc32; ///< CRC32 . Set to 0 by default when no CRC32 is computed
  cid_t    cid;
  sid_t    sid;
} rozofs_stor_bins_file_hdr_v1_t;







/*_________________________________________________
** V2 header file
**
** dist_set_current is runcated to ROZOFS_ST_HEADER_MAX_DISTRIB
** bytes which is the maximum layout 2 distibution size
**
** These fields offset are chnaged
** - crc32
** - cid
** - sid 
**
** A new field nbChunks tells how many chunk with the array of ROZOFS_STORAGE_MAX_CHUNK_PER_FILE
** chunks are saved on disk. In can take the following values 8, 12, 16, 20, 24....
** device are more a 128 byte array, but a 8 byte array extensible by range of 4 bytes
**
** The check sum is computed on size sizeof(rozofs_stor_bins_file_hdr_v2_start_t) + hdr->h.nbChunks
*/

#define ROZOFS_ST_HEADER_MAX_DISTRIB       16

typedef struct _rozofs_stor_bins_file_hdr_v2_t {
  uint8_t     version; ///<  version of rozofs. (not used yet)
  uint8_t     layout; ///< layout used for this file.
  uint8_t     bsize;  ///< Block size as defined in enum ROZOFS_BSIZE_E 

  uint8_t     padding; // FREE    

  fid_t       fid;
  sid_t       distrib[ROZOFS_ST_HEADER_MAX_DISTRIB]; ///< currents sids of storage nodes target for this file.

  cid_t       cid;
  sid_t       sid;  
  uint8_t     nbChunks; // Number of devices actually written in the file after the crc32 field

  uint32_t    crc32; ///< CRC32 . Set to 0 by default when no CRC32 is computed  
  uint8_t     devFromChunk[ROZOFS_STORAGE_MAX_CHUNK_PER_FILE];            
} rozofs_stor_bins_file_hdr_v2_t;






/*_________________________________________________
** All version header file
** v0, v1 and v2 header are the same up to dist_set_current fi
*/
typedef union _rozofs_stor_bins_file_hdr_vall_t {
  uint8_t                             version;
  rozofs_stor_bins_file_hdr_v0_t      v0;  
  rozofs_stor_bins_file_hdr_v1_t      v1;  
  rozofs_stor_bins_file_hdr_v2_t      v2;
} rozofs_stor_bins_file_hdr_vall_t;

/*
** Current header version is 2
*/
typedef rozofs_stor_bins_file_hdr_v2_t rozofs_stor_bins_file_hdr_t;



/*________________________________________________________________________
**  Compute the number of chunk to store on disk
**
**  @param hdr        The header file
**  @param nbChunks   Number of chunks
**________________________________________________________________________
*/
static inline uint8_t rozofs_st_header_roundup_chunk_number(int nbChunks) {
  
  /*
  ** At least 8 devices shall be present in the device array
  ** and a multiple of 4 devices must be set
  */
  if (nbChunks <= 8) {
    return 8;
  }
  
  /*
  ** No more than ROZOFS_STORAGE_MAX_CHUNK_PER_FILE c hunks
  */
  if (nbChunks > ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) return ROZOFS_STORAGE_MAX_CHUNK_PER_FILE;
  
  /*
  ** Minimun is 8 chunk store. nb chunk store isthen increase in step of 4 bytes
  */
  return (((nbChunks-1)/4)+1)*4;
}
/*_________________________________________________________________________
**  Get the length on disk of a header/mapper file 
**
**  @param hdr          The header file
**  @param hdr          The pointer to the CRC32 within the header
**  
**  @retval The size when OK. -1 on failure
**_________________________________________________________________________
*/
static inline int rozofs_st_get_header_file_size(void * hdr) {
  rozofs_stor_bins_file_hdr_vall_t * vall = (rozofs_stor_bins_file_hdr_vall_t*) hdr;
  
  switch(vall->version) {
    case 0:
      return sizeof(vall->v0);
    case 1:
      return sizeof(vall->v1);      
    case 2:        
      return (sizeof(vall->v2) - ROZOFS_STORAGE_MAX_CHUNK_PER_FILE + vall->v2.nbChunks);
    default:
      break;
  }  
  return -1;
}  
/*_________________________________________________________________________
**  Get the length on disk of a header/mapper file 
**
**  @param hdr          The header file
**  @param pCrc         The pointer to the CRC32 within the header
**  
**  @retval The size when OK. -1 on failure
**_________________________________________________________________________
*/
static inline int rozofs_st_get_header_file_crc(void * hdr, uint32_t ** pCrc) {
  rozofs_stor_bins_file_hdr_vall_t * vall = (rozofs_stor_bins_file_hdr_vall_t*) hdr;
  
  switch(vall->version) {
    case 0:
      *pCrc = &vall->v0.crc32;
      return sizeof(vall->v0);
    case 1:
      *pCrc = &vall->v1.crc32; 
      return sizeof(vall->v1);      
    case 2:        
      *pCrc = &vall->v2.crc32;
      return (sizeof(vall->v2) - ROZOFS_STORAGE_MAX_CHUNK_PER_FILE + vall->v2.nbChunks);
    default:
      break;
  }  
  *pCrc = NULL;
  return -1;
}  
/*________________________________________________________________________
**  Translate a v1 header to a v2 format
**
**  @param v1    The original v1 header
**  @param v2    The destination v2 header
**________________________________________________________________________
*/
static inline void rozofs_st_header_from_v1_to_v2(rozofs_stor_bins_file_hdr_v1_t * v1,
                                                  rozofs_stor_bins_file_hdr_v2_t * v2) {
  int idx;
  uint8_t nbChunks;
    
  v2->version  = 2;
  v2->bsize    = v1->bsize;
  v2->layout   = v1->layout;
  v2->padding  = 0;
  v2->cid      = v1->cid;
  v2->sid      = v1->sid;     
  v2->crc32    = 0;
  memcpy(v2->fid, v1->fid, sizeof(fid_t));
  memcpy(v2->distrib, v1->dist_set_current,ROZOFS_ST_HEADER_MAX_DISTRIB);
  
  /*
  ** Copy devices until end of file chunk
  */
  for (idx=0; idx < ROZOFS_STORAGE_MAX_CHUNK_PER_FILE; idx++) { 
    v2->devFromChunk[idx] = v1->devFromChunk[idx];
    if (v2->devFromChunk[idx] == ROZOFS_EOF_CHUNK) {
      break;
    }
  }

  nbChunks = idx;
  
  /*
  ** Complete chunk with EOF marks
  */       
  for ( ; idx < ROZOFS_STORAGE_MAX_CHUNK_PER_FILE; idx ++) {
     v2->devFromChunk[idx] = ROZOFS_EOF_CHUNK;
  } 
  
  /*
  ** Set number of valid chunks
  */
  v2->nbChunks = rozofs_st_header_roundup_chunk_number(nbChunks);
}
/*________________________________________________________________________
**  Initialize a v2 header file
**
**  @param v1    The original v1 header
**  @param v2    The destination v2 header
**________________________________________________________________________
*/
static inline void rozofs_st_header_init(rozofs_stor_bins_file_hdr_t * hdr) {
  memset(hdr, 0, sizeof(rozofs_stor_bins_file_hdr_v2_t) - ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);  
  memset(hdr->devFromChunk, ROZOFS_EOF_CHUNK, ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);
  hdr->nbChunks = 8;
  hdr->version  = 2;
}
/*________________________________________________________________________
**  Get device of a chunk in the header file
**
**  @param hdr     Header file
**  @param chunk   Chunk number
**________________________________________________________________________
*/
static inline int rozofs_st_header_get_chunk(void * hdr, int chunk) {
  rozofs_stor_bins_file_hdr_vall_t * vall = hdr;
  
  if (chunk >= ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) return ROZOFS_EOF_CHUNK;
  
  if (vall->version == 2) {
    return vall->v2.devFromChunk[chunk];
  }
  
  return vall->v0.devFromChunk[chunk];
    
}
/*________________________________________________________________________
**  Set device of a chunk in the header file
**
**  @param hdr     Header file
**  @param chunk   Chunk number
**  @param dev     Device number to store
**________________________________________________________________________
*/
static inline void rozofs_st_header_set_chunk(rozofs_stor_bins_file_hdr_t * hdr, int chunk, uint8_t dev) {
  int idx;
  
  if (chunk >= ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) return;

  /*
  ** All header files should be encoded in v2 format
  */
  if ((hdr->version == 0)||(hdr->version == 1)) {
    severe("Bad header file version %d",hdr->version);
    return;
  } 
  
  if ((dev ==  ROZOFS_EOF_CHUNK) || (dev == ROZOFS_EOF_CHUNK)) {
    hdr->devFromChunk[chunk] = dev;
    return;
  }

  /*
  ** Previous chunk that were EOF must now be EMPTY
  */
  for (idx=0; idx < chunk; idx++) {
    if (hdr->devFromChunk[idx] == ROZOFS_EOF_CHUNK) {
      hdr->devFromChunk[idx] = ROZOFS_EMPTY_CHUNK;
    }
  }

  /*
  ** Store new chunk number
  */
  hdr->devFromChunk[chunk] = dev;

  /*
  ** Update number of chunk when needed
  */
  if (hdr->nbChunks < (chunk+1)) {
    hdr->nbChunks = rozofs_st_header_roundup_chunk_number(chunk+1);
  }
} 
#endif

