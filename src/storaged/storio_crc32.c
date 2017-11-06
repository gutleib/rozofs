
/* crc32c.c -- compute CRC-32C using the Intel crc32 instruction
 * Copyright (C) 2013 Mark Adler
 * Version 1.1  1 Aug 2013  Mark Adler
 */

/*
  This software is provided 'as-is', without any express or implied
  warranty.  In no event will the author be held liable for any damages
  arising from the use of this software.

  Permission is granted to anyone to use this software for any purpose,
  including commercial applications, and to alter it and redistribute it
  freely, subject to the following restrictions:

  1. The origin of this software must not be misrepresented; you must not
     claim that you wrote the original software. If you use this software
     in a product, an acknowledgment in the product documentation would be
     appreciated but is not required.
  2. Altered source versions must be plainly marked as such, and must not be
     misrepresented as being the original software.
  3. This notice may not be removed or altered from any source distribution.

  Mark Adler
  madler@alumni.caltech.edu
 */

/* Use hardware CRC instruction on Intel SSE 4.2 processors.  This computes a
   CRC-32C, *not* the CRC-32 used by Ethernet and zip, gzip, etc.  A software
   version is provided as a fall-back, as well as for speed comparisons. */

/* Version history:
   1.0  10 Feb 2013  First version
   1.1   1 Aug 2013  Correct comments on why three crc instructions in parallel
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <stdint.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/core/uma_dbg_api.h>
#include "storio_crc32.h"

/* CRC-32C (iSCSI) polynomial in reversed bit order. */
#define POLY 0x82f63b78


/*
** To count the number of block in fault on each read that has CRC32 errors
*/
#define  STORIO_MAX_CRC32_ERROR_PER_READ_COUNT 128
uint64_t storio_crc32_error_per_read[STORIO_MAX_CRC32_ERROR_PER_READ_COUNT] = {0};


/* Table for a quadword-at-a-time software crc. */
static pthread_once_t crc32c_once_sw = PTHREAD_ONCE_INIT;
static uint32_t crc32c_table[8][256];


/* Construct table for software CRC-32C calculation. */
static void crc32c_init_sw(void)
{
    uint32_t n, crc, k;

    for (n = 0; n < 256; n++) {
        crc = n;
        crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
        crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
        crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
        crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
        crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
        crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
        crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
        crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
        crc32c_table[0][n] = crc;
    }
    for (n = 0; n < 256; n++) {
        crc = crc32c_table[0][n];
        for (k = 1; k < 8; k++) {
            crc = crc32c_table[0][crc & 0xff] ^ (crc >> 8);
            crc32c_table[k][n] = crc;
        }
    }
}

/* Table-driven software version as a fall-back.  This is about 15 times slower
   than using the hardware instructions.  This assumes little-endian integers,
   as is the case on Intel processors that the assembler code here is for. */
static uint32_t crc32c_sw(uint32_t crci, const void *buf, size_t len)
{
    const unsigned char *next = buf;
    uint64_t crc;

    pthread_once(&crc32c_once_sw, crc32c_init_sw);
    crc = crci ^ 0xffffffff;
    while (len && ((uintptr_t)next & 7) != 0) {
        crc = crc32c_table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
        len--;
    }
    while (len >= 8) {
        crc ^= *(uint64_t *)next;
        crc = crc32c_table[7][crc & 0xff] ^
              crc32c_table[6][(crc >> 8) & 0xff] ^
              crc32c_table[5][(crc >> 16) & 0xff] ^
              crc32c_table[4][(crc >> 24) & 0xff] ^
              crc32c_table[3][(crc >> 32) & 0xff] ^
              crc32c_table[2][(crc >> 40) & 0xff] ^
              crc32c_table[1][(crc >> 48) & 0xff] ^
              crc32c_table[0][crc >> 56];
        next += 8;
        len -= 8;
    }
    while (len) {
        crc = crc32c_table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
        len--;
    }
    return (uint32_t)crc ^ 0xffffffff;
}

/* Multiply a matrix times a vector over the Galois field of two elements,
   GF(2).  Each element is a bit in an unsigned integer.  mat must have at
   least as many entries as the power of two for most significant one bit in
   vec. */
static inline uint32_t gf2_matrix_times(uint32_t *mat, uint32_t vec)
{
    uint32_t sum;

    sum = 0;
    while (vec) {
        if (vec & 1)
            sum ^= *mat;
        vec >>= 1;
        mat++;
    }
    return sum;
}

/* Multiply a matrix by itself over GF(2).  Both mat and square must have 32
   rows. */
static inline void gf2_matrix_square(uint32_t *square, uint32_t *mat)
{
    int n;

    for (n = 0; n < 32; n++)
        square[n] = gf2_matrix_times(mat, mat[n]);
}

/* Construct an operator to apply len zeros to a crc.  len must be a power of
   two.  If len is not a power of two, then the result is the same as for the
   largest power of two less than len.  The result for len == 0 is the same as
   for len == 1.  A version of this routine could be easily written for any
   len, but that is not needed for this application. */
static void crc32c_zeros_op(uint32_t *even, size_t len)
{
    int n;
    uint32_t row;
    uint32_t odd[32];       /* odd-power-of-two zeros operator */

    /* put operator for one zero bit in odd */
    odd[0] = POLY;              /* CRC-32C polynomial */
    row = 1;
    for (n = 1; n < 32; n++) {
        odd[n] = row;
        row <<= 1;
    }

    /* put operator for two zero bits in even */
    gf2_matrix_square(even, odd);

    /* put operator for four zero bits in odd */
    gf2_matrix_square(odd, even);

    /* first square will put the operator for one zero byte (eight zero bits),
       in even -- next square puts operator for two zero bytes in odd, and so
       on, until len has been rotated down to zero */
    do {
        gf2_matrix_square(even, odd);
        len >>= 1;
        if (len == 0)
            return;
        gf2_matrix_square(odd, even);
        len >>= 1;
    } while (len);

    /* answer ended up in odd -- copy to even */
    for (n = 0; n < 32; n++)
        even[n] = odd[n];
}

/* Take a length and build four lookup tables for applying the zeros operator
   for that length, byte-by-byte on the operand. */
static void crc32c_zeros(uint32_t zeros[][256], size_t len)
{
    uint32_t n;
    uint32_t op[32];
//    printf("FDL len %d\n",len);
    crc32c_zeros_op(op, len);
    for (n = 0; n < 256; n++) {
        zeros[0][n] = gf2_matrix_times(op, n);
        zeros[1][n] = gf2_matrix_times(op, n << 8);
        zeros[2][n] = gf2_matrix_times(op, n << 16);
        zeros[3][n] = gf2_matrix_times(op, n << 24);
    }
}

/* Apply the zeros operator table to crc. */
static inline uint32_t crc32c_shift(uint32_t zeros[][256], uint32_t crc)
{
    return zeros[0][crc & 0xff] ^ zeros[1][(crc >> 8) & 0xff] ^
           zeros[2][(crc >> 16) & 0xff] ^ zeros[3][crc >> 24];
}

/* Block sizes for three-way parallel crc computation.  LONG and SHORT must
   both be powers of two.  The associated string constants must be set
   accordingly, for use in constructing the assembler instructions. */
#define LONG 8192
#define LONGx1 "8192"
#define LONGx2 "16384"
#define SHORT 256
#define SHORTx1 "256"
#define SHORTx2 "512"

/* Tables for hardware crc that shift a crc by LONG and SHORT zeros. */
static pthread_once_t crc32c_once_hw = PTHREAD_ONCE_INIT;
static uint32_t crc32c_long[4][256];
static uint32_t crc32c_short[4][256];

/* Initialize tables for shifting crcs. */
static void crc32c_init_hw(void)
{
    crc32c_zeros(crc32c_long, LONG);
    crc32c_zeros(crc32c_short, SHORT);
}

/* Compute CRC-32C using the Intel hardware instruction. */
static uint32_t crc32c_hw(uint32_t crc, const void *buf, size_t len)
{
//        printf("FDL HW2\n");

    const unsigned char *next = buf;
    const unsigned char *end;
    uint64_t crc0, crc1, crc2;      /* need to be 64 bits for crc32q */
#if 0
    /* populate shift tables the first time through */
    pthread_once(&crc32c_once_hw, crc32c_init_hw);
#endif
    /* pre-process the crc */
    crc0 = crc ^ 0xffffffff;
#if 0
    /* compute the crc for up to seven leading bytes to bring the data pointer
       to an eight-byte boundary */
    while (len && ((uintptr_t)next & 7) != 0) {
        __asm__("crc32b\t" "(%1), %0"
                : "=r"(crc0)
                : "r"(next), "0"(crc0));
        next++;
        len--;
    }

    /* compute the crc on sets of LONG*3 bytes, executing three independent crc
       instructions, each on LONG bytes -- this is optimized for the Nehalem,
       Westmere, Sandy Bridge, and Ivy Bridge architectures, which have a
       throughput of one crc per cycle, but a latency of three cycles */
    while (len >= LONG*3) {
        crc1 = 0;
        crc2 = 0;
        end = next + LONG;
        do {
            __asm__("crc32q\t" "(%3), %0\n\t"
                    "crc32q\t" LONGx1 "(%3), %1\n\t"
                    "crc32q\t" LONGx2 "(%3), %2"
                    : "=r"(crc0), "=r"(crc1), "=r"(crc2)
                    : "r"(next), "0"(crc0), "1"(crc1), "2"(crc2));
            next += 8;
        } while (next < end);
        crc0 = crc32c_shift(crc32c_long, crc0) ^ crc1;
        crc0 = crc32c_shift(crc32c_long, crc0) ^ crc2;
        next += LONG*2;
        len -= LONG*3;
    }
#endif
    /* do the same thing, but now on SHORT*3 blocks for the remaining data less
       than a LONG*3 block */
    while (len >= SHORT*3) {
        crc1 = 0;
        crc2 = 0;
        end = next + SHORT;
        do {
            __asm__("crc32q\t" "(%3), %0\n\t"
                    "crc32q\t" SHORTx1 "(%3), %1\n\t"
                    "crc32q\t" SHORTx2 "(%3), %2"
                    : "=r"(crc0), "=r"(crc1), "=r"(crc2)
                    : "r"(next), "0"(crc0), "1"(crc1), "2"(crc2));
            next += 8;
        } while (next < end);
        crc0 = crc32c_shift(crc32c_short, crc0) ^ crc1;
        crc0 = crc32c_shift(crc32c_short, crc0) ^ crc2;
        next += SHORT*2;
        len -= SHORT*3;
    }

    /* compute the crc on the remaining eight-byte units less than a SHORT*3
       block */
    end = next + (len - (len & 7));
    while (next < end) {
        __asm__("crc32q\t" "(%1), %0"
                : "=r"(crc0)
                : "r"(next), "0"(crc0));
        next += 8;
    }
    len &= 7;

    /* compute the crc for up to seven trailing bytes */
    while (len) {
        __asm__("crc32b\t" "(%1), %0"
                : "=r"(crc0)
                : "r"(next), "0"(crc0));
        next++;
        len--;
    }
    /* return a post-processed crc */
    return (uint32_t)crc0 ^ 0xffffffff;
}

/*
**__________________________________________________________________
*/
int crc32c_hw_supported = 0;
int crc32c_generate_enable = 0;  /**< assert to 1 for CRC generation  */
int crc32c_check_enable = 0;  /**< assert to 1 for CRC generation  */
uint64_t storio_crc_error= 0;


/* Check for SSE 4.2.  SSE 4.2 was first supported in Nehalem processors
   introduced in November, 2008.  This does not check for the existence of the
   cpuid instruction itself, which was introduced on the 486SL in 1992, so this
   will fail on earlier x86 processors.  cpuid works on all Pentium and later
   processors. */
#define SSE42(have) \
    do { \
        uint32_t eax, ecx; \
        eax = 1; \
        __asm__("cpuid" \
                : "=c"(ecx) \
                : "a"(eax) \
                : "%ebx", "%edx"); \
        (have) = (ecx >> 20) & 1; \
    } while (0)

/* Compute a CRC-32C.  If the crc32 instruction is available, use the hardware
   version.  Otherwise, use the software version. */
uint32_t crc32c(uint32_t crc, const void *buf, size_t len)
{
    return (crc32c_hw_supported==1) ? crc32c_hw(crc, buf, len) : crc32c_sw(crc, buf, len);
}

/*
**__________________________________________________________________
*/
/*
**  Generate a CRC32 of the content of the header file.
    
    @param hdr: the header file structure
    @param initial_crc: Value to intializae the CRC to    

*/
void storio_gen_header_crc32(rozofs_stor_bins_file_hdr_t * hdr, uint32_t initial_crc)
{
   uint32_t crc;
 
   hdr->v0.crc32 = 0;
   
   if (crc32c_generate_enable == 0) return;

   crc = initial_crc;
   crc = crc32c(crc,(char *) hdr, sizeof(rozofs_stor_bins_file_hdr_t));
   if (crc == 0) crc = 1;
   hdr->v0.crc32 = crc;
}
/*
**__________________________________________________________________
*/
/*
**  Check the CRC32 on the content of the header file.

    @param hdr: the header file structure
    @param crc_errorcnt_p: pointer to the crc error counter of the storage (cid/sid).
    @param initial_crc: Value to intializae the CRC to    
    
    @retval 0 on success -1 on error
*/
int storio_check_header_crc32(rozofs_stor_bins_file_hdr_t * hdr, uint64_t *crc_error_cnt_p, uint32_t initial_crc)
{
   uint32_t crc;
   uint32_t cur_crc = hdr->v0.crc32;
   uint32_t crc_size;

#if CRC32_PERFORMANCE_CHECK
   uint64_t encode_cycles_start;
   uint64_t encode_cycles_stop;
#endif   

   if (crc32c_check_enable == 0) return 0;

   /*
   ** check if crc has been generated on write
   */
   if (cur_crc == 0) return 0;

#if CRC32_PERFORMANCE_CHECK
   encode_cycles_start = rdtsc();
#endif   

   /*
   **  compute the crc
   */
   hdr->v0.crc32 = 0;
   crc = initial_crc;
   if (hdr->v0.version == 0) {
     crc_size = sizeof(hdr->v0);
   }
   else {
     crc_size = sizeof(rozofs_stor_bins_file_hdr_t);
   }
   crc = crc32c(crc,(char *) hdr, crc_size);
   if (crc==0) crc = 1;      



#if CRC32_PERFORMANCE_CHECK
   encode_cycles_stop = rdtsc();
        severe("FDL(%d) encode cycles %llu length %d count %d\n",crc32c_hw_supported,
	       (unsigned long long int)(encode_cycles_stop - encode_cycles_start),prj_size,nb_proj);
#endif

   /*
   ** control with the one stored in the header
   */
   hdr->v0.crc32 = cur_crc; // Restore CRC32 in header
   if (cur_crc != crc) {
     __atomic_fetch_add(crc_error_cnt_p,1,__ATOMIC_SEQ_CST);   
     __atomic_fetch_add(&storio_crc_error,1,__ATOMIC_SEQ_CST);
     return -1;
   }
   return 0;  
}
/*
**__________________________________________________________________
*/
/*
**  Generate a CRC32 on each projection. THe cRC32 is stored in the
    filler field of each projection header.
    
    @param bin: pointer to the beginning of the set of projections
    @param nb_proj : number of projections
    @param prj_size: size of a projection including the prj header
    @param initial_crc: Value to intializae the CRC to    
*/
void storio_gen_crc32(char *bins,int nb_proj,uint16_t prj_size, uint32_t initial_crc)
{
   size_t crc_size = prj_size;
   uint32_t crc = 0;
   int i;
   char *buf=bins;
 
   if (crc32c_generate_enable == 0) return;

   for (i = 0; i < nb_proj ; i++)
   {
      crc = initial_crc + i;
      ((rozofs_stor_bins_hdr_t*)(buf))->s.filler = 0;
      if (crc32c_generate_enable) {
        crc = crc32c(crc,buf,crc_size);
        if (crc == 0) crc = 1;
        ((rozofs_stor_bins_hdr_t*)(buf))->s.filler = crc;
      }	
      buf+=prj_size;   
   }
}

/*
**__________________________________________________________________
*/
/*
**  Check the  CRC32 on each projection. THe cRC32 is stored in the
    filler field of each projection header.
    In case of error the projection id is se to 0xff in the header
    
    @param bin: pointer to the beginning of the set of projections
    @param nb_proj : number of projections
    @param prj_size: size of a projection including the prj header
    @param crc_errorcnt_p: pointer to the crc error counter of the storage (cid/sid).
    @param initial_crc: Value to intializae the CRC to    
    @param errors: a returned bitmask of the blocks in error   
    
    @retval the number of CRC32 error detected
*/
int storio_check_crc32(char     * bins,
                       int        nb_proj,
		       uint16_t   prj_size,
		       uint64_t * crc_error_cnt_p, 
		       uint32_t   initial_crc, 
		       uint64_t * errors)
{
   size_t crc_size = prj_size;
   uint32_t crc = 0;
   uint32_t cur_crc;
   int i;
   char *buf=bins;
   int result = 0;
#if 0
   uint64_t encode_cycles_start;
   uint64_t encode_cycles_stop;
#endif   
   if (crc32c_check_enable == 0) return 0;
#if 0
   encode_cycles_start = rdtsc();
#endif   
   for (i = 0; i < nb_proj ; i++)
   {
      /*
      ** check if crc has been generated on write
      */
      cur_crc = ((rozofs_stor_bins_hdr_t*)(buf))->s.filler;

      if (cur_crc == 0) continue;
      /*
      **  compute the crc
      */
      ((rozofs_stor_bins_hdr_t*)(buf))->s.filler = 0;
      crc = initial_crc + i;
      crc = crc32c(crc,buf,crc_size);
      if (crc==0) crc = 1;      

      /*
      ** Restore CRC in the read header
      */
      ((rozofs_stor_bins_hdr_t*)(buf))->s.filler = cur_crc;
      
      /*
      ** control with the one stored in the header
      */
      if (cur_crc != crc)
      {
        /*
	** data corruption
	*/
	((rozofs_stor_bins_hdr_t*)(buf))->s.projection_id = 0xff;
	/*
	** increment the global counter and the storage counter
	*/
	__atomic_fetch_add(&storio_crc_error,1,__ATOMIC_SEQ_CST);
	__atomic_fetch_add(crc_error_cnt_p,1,__ATOMIC_SEQ_CST);
	errors[i/64] |= (1ULL<<(i%64));
	result++;
      }
      buf+=prj_size;   
   }
#if 0
   encode_cycles_stop = rdtsc();
        severe("FDL(%d) encode cycles %llu length %d count %d\n",crc32c_hw_supported,
	       (unsigned long long int)(encode_cycles_stop - encode_cycles_start),prj_size,nb_proj);
#endif
  if (result) {
    if (result<STORIO_MAX_CRC32_ERROR_PER_READ_COUNT) {
      storio_crc32_error_per_read[result]++;
    }  
    else {
      storio_crc32_error_per_read[STORIO_MAX_CRC32_ERROR_PER_READ_COUNT-1]++;
    }
  }
  return result;
}

/*
**__________________________________________________________________
*/
/*
**  Generate a CRC32 on each projection. THe cRC32 is stored in the
    filler field of each projection header.
    
    @param vector: pointer to the beginning of the set of projections vector
    @param nb_proj : number of projections
    @param prj_size: size of a projection including the prj header
    @param initial_crc: Value to intializae the CRC to    
*/
void storio_gen_crc32_vect(struct iovec *vector,int nb_proj,uint16_t prj_size, uint32_t initial_crc)
{
   size_t crc_size = prj_size;
   uint32_t crc = 0;
   int i;
   char *buf;


   for (i = 0; i < nb_proj ; i++)
   {
      crc = initial_crc + i;
      buf = vector[i].iov_base;
      ((rozofs_stor_bins_hdr_t*)(buf))->s.filler = 0;
      if (crc32c_generate_enable) {
        crc = crc32c(crc,buf,crc_size);
        if (crc==0) crc = 1;
        ((rozofs_stor_bins_hdr_t*)(buf))->s.filler = crc;
      }	
   }
}

/*
**__________________________________________________________________
*/
/*
**  Check the  CRC32 on each projection. THe cRC32 is stored in the
    filler field of each projection header.
    In case of error the projection id is se to 0xff in the header
    
    @param vect: pointer to the beginning of the set of projections vector
    @param nb_proj : number of projections
    @param prj_size: size of a projection including the prj header
    @param crc_errorcnt_p: pointer to the crc error counter of the storage (cid/sid).
    @param initial_crc: Value to intializae the CRC to    
    @param errors: a returned bitmask of the blocks in error   
        
    @retval the number of CRC32 error detected
*/
int  storio_check_crc32_vect(struct iovec * vector,
                             int            nb_proj,
			     uint16_t       prj_size,
			     uint64_t     * crc_error_cnt_p, 
			     uint32_t       initial_crc, 
			     uint64_t     * errors)
{
   size_t crc_size = prj_size;
   uint32_t crc = 0;
   uint32_t cur_crc;
   int i;
   char *buf;
   int  result=0;

   if (crc32c_check_enable == 0) return 0;
      
   for (i = 0; i < nb_proj ; i++)
   {
      /*
      ** check if crc has been generated on write
      */
      buf = vector[i].iov_base;
      cur_crc = ((rozofs_stor_bins_hdr_t*)(buf))->s.filler;
      if (cur_crc == 0) continue;
      /*
      **  compute the crc
      */
      ((rozofs_stor_bins_hdr_t*)(buf))->s.filler = 0;
      crc = initial_crc + i;
      crc = crc32c(crc,buf,crc_size);
      if (crc==0) crc = 1;      
      /*
      ** Restore CRC in the read header
      */
      ((rozofs_stor_bins_hdr_t*)(buf))->s.filler = cur_crc;
      /*
      ** control with the one stored in the header
      */    
      if (cur_crc != crc)
      {
        /*
	** data corruption
	*/
	((rozofs_stor_bins_hdr_t*)(buf))->s.projection_id = 0xff;
	/*
	** increment the global counter and the storage counter
	*/
	__atomic_fetch_add(&storio_crc_error,1,__ATOMIC_SEQ_CST);
	__atomic_fetch_add(crc_error_cnt_p,1,__ATOMIC_SEQ_CST);
        errors[i/64] |= (1ULL<<(i%64));
	result++;	
      }
   }
   if (result) {
     if (result<STORIO_MAX_CRC32_ERROR_PER_READ_COUNT) {
       storio_crc32_error_per_read[result]++;
     }  
     else {
       storio_crc32_error_per_read[STORIO_MAX_CRC32_ERROR_PER_READ_COUNT-1]++;
     }
   }
   return result;
}
/*
**__________________________________________________________________
*/
void man_data_integrity(char * pChar) {  
  pChar += rozofs_string_append(pChar,"data_integrity display CRC32 error detection statistics.\n");
  pChar += rozofs_string_append(pChar,"  crc32c generation      Tells whether CRC32 is generated.\n");
  pChar += rozofs_string_append(pChar,"  crc32c control         Tells whether CRC32 is checked.\n");
  pChar += rozofs_string_append(pChar,"  crc32c computing mode  Tells the CRC32 computation mode.\n");
  pChar += rozofs_string_append(pChar,"  crc32c computing mode  Tells the CRC32 computation mode.\n");
  pChar += rozofs_string_append(pChar,"  crc32c error counter   Tells the number of CRC32 errors detected.\n");
}
/*
**__________________________________________________________________
*/
char * show_data_integrity_syntax(char * pChar)
{  
  pChar += rozofs_string_append(pChar,"syntax: data_integrity [reset]\n");
  return pChar;
}
/*
**__________________________________________________________________
*/
static void show_data_integrity(char * argv[], uint32_t tcpRef, void *bufRef)
{
    char *pChar = uma_dbg_get_buffer();
    
     if (argv[1] != NULL) {
     
       if (strcmp(argv[1],"reset")==0) {
         if (storio_crc_error) {
	   warning("CRC32 counter was %llu",(long long unsigned int) storio_crc_error);
	 }
         storio_crc_error = 0;
	 memset(storio_crc32_error_per_read,0,sizeof(storio_crc32_error_per_read));
	 pChar += rozofs_string_append(pChar,"error counter has been cleared\n");
         uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());	 
	 return;
       }

       pChar = show_data_integrity_syntax(pChar);
       uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
       return;      
     }
     
     pChar += rozofs_string_append(pChar,"Data integrity :\n");
     pChar += rozofs_string_append(pChar,"  crc32c generation      : ");
     if (crc32c_generate_enable==0) {
       pChar += rozofs_string_append(pChar,"DISABLED\n");
     }
     else {
       pChar += rozofs_string_append(pChar,"ENABLED\n");       
     }  
     pChar += rozofs_string_append(pChar,"  crc32c control         : ");
     if (crc32c_check_enable==0) {
       pChar += rozofs_string_append(pChar,"DISABLED\n");
     }
     else {
       pChar += rozofs_string_append(pChar,"ENABLED\n");       
     }       
     pChar += rozofs_string_append(pChar,"  crc32c computing mode  : ");
     if (crc32c_hw_supported==0) {
       pChar += rozofs_string_append(pChar,"SOFTWARE\n");
     }
     else {
       pChar += rozofs_string_append(pChar,"HARDWARE\n");       
     }       
     pChar += rozofs_string_append(pChar,"  crc32c error counter   : ");
     pChar += rozofs_u64_append(pChar, storio_crc_error);
     pChar += rozofs_eol(pChar);
     
     // Display the number 
     if (storio_crc_error) {
       int i;
       pChar += rozofs_string_append(pChar,"  Blocks | Times\n");
       uint64_t * p64 = storio_crc32_error_per_read;
       for (i=0; i < STORIO_MAX_CRC32_ERROR_PER_READ_COUNT; i++,p64++) {    
         if (*p64) {
	   pChar += rozofs_i32_padded_append(pChar, 8, rozofs_right_alignment, i);
	   pChar += rozofs_string_append(pChar," | ");
	   pChar += rozofs_u64_append(pChar, *p64);  
	   pChar += rozofs_eol(pChar);
	 }
       }  
     
     }
     pChar += rozofs_eol(pChar);
     
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());

}

/*
**__________________________________________________________________
*/
void crc32c_init(int generate_enable,int check_enable,int hw_forced)
{
    int sse42;


    SSE42(sse42);
    if (sse42== 1) crc32c_hw_supported = 1;
    if (hw_forced) crc32c_hw_supported = 1;
    pthread_once(&crc32c_once_hw, crc32c_init_hw);
    crc32c_check_enable = 0;
    crc32c_generate_enable = generate_enable;
    if ((check_enable) && (generate_enable))
    {
      crc32c_check_enable = check_enable;    
    }
    uma_dbg_addTopicAndMan("data_integrity", show_data_integrity, man_data_integrity, UMA_DBG_OPTION_RESET);
}
