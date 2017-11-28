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
 #ifndef STORIO_CRC32_H
 #define STORIO_CRC32_H
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <stdint.h>
#include "storage.h"

extern int crc32c_hw_spported ;
extern int crc32c_generate_enable;  /**< assert to 1 for CRC generation  */
extern int crc32c_check_enable;  /**< assert to 1 for CRC generation  */
extern uint64_t storio_crc_error;
 
 uint32_t crc32c(uint32_t crc, const void *buf, size_t len); 
 void crc32c_init(int generate_enable,int check_enable,int hw_forced);
 
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
void storio_gen_crc32(char *bins,int nb_proj,uint16_t prj_size, uint32_t initial_crc);
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
		       uint64_t * errors);
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
void storio_gen_crc32_vect(struct iovec *vector,int nb_proj,uint16_t prj_size, uint32_t initial_crc);

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
			     uint64_t     * errors);
/*
**__________________________________________________________________
*/
/*
**  Generate a CRC32 of the content of the header file.
    
    @param hdr: the header file structure
    @param initial_crc: Value to intializae the CRC to    

*/
void storio_gen_header_crc32(void * hdr, uint32_t initial_crc);
/*
**__________________________________________________________________
*/
/*
**  Check the CRC32 on the content of the header file.

    @param hdr: the header file structure
    @param initial_crc: Value to intializae the CRC to    
    
    @retval 0 on success -1 on error
*/
int storio_check_header_crc32(void * hdr, uint32_t initial_crc);
#endif
