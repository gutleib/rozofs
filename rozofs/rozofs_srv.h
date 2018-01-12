/*
 Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
 This file is part of Rozofs.

 Rozofs is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published
 by the Free Software Foundation; either version 2 of the License,
 or (at your option) any later version.

 Rozofs is distributed in the hope that it will be useful, but
 WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see
 <http://www.gnu.org/licenses/>.
 */

#ifndef ROZOFS_SRV_H
#define ROZOFS_SRV_H

#include <errno.h>
#include <stdio.h>

#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/rozofs_srv.h>
#include "rozofs.h"


typedef struct _rozofs_conf_psizes_t {
    float    redundancyCoeff;    /**< Redundacny coefficient for the given layout and block size (>1)*/
    float    redundancyCoeff_128;    /**< Redundacny coefficient for the given layout and block size (>1) for bins of 128 bits*/
    uint16_t rozofs_psizes_max;  /**< size of the larger projection                     */
    uint16_t rozofs_eff_psizes_max;  /**< size of the larger projection (optimized)     */
    uint16_t *rozofs_psizes;     /**< size in bins of each projection                   */
    uint16_t *rozofs_eff_psizes; /**< effective size in bins of each projection         */
    uint16_t *rozofs_128bits_psizes; /**< effective size in bins of each projection         */
    uint16_t rozofs_128bits_psizes_max; /**< effective size in bins of each projection         */
} rozofs_conf_psizes_t;  
/**
 * structure used to store the parameters relative to a given layout
 */
typedef struct _rozofs_conf_layout_t {
    uint8_t rozofs_safe; /**< max number of selectable storages       */
    uint8_t rozofs_forward; /**< number of projection to forward         */
    uint8_t rozofs_inverse; /**< number of projections needed to rebuild */
    angle_t *rozofs_angles; /**< p and q angle for each projection       */
    rozofs_conf_psizes_t sizes[ROZOFS_BSIZE_NB] ; /* Projection sizes */
} rozofs_conf_layout_t;

extern rozofs_conf_layout_t rozofs_conf_layout_table[];

/**
 * Initialize the layout table

 @param none
 @retval none
 */
void rozofs_layout_initialize();


/**
 * Release the layout table

 @param none
 @retval none
 */
void rozofs_layout_release();

/**
  Get the rozofs_inverse for a given layout
  
  @param layout : layout association with the file
  
  @retval rozofs_inverse associated with the layout
 */
static inline uint8_t rozofs_get_rozofs_inverse(uint8_t layout) {
    if (layout >= LAYOUT_MAX) return 0;
    return rozofs_conf_layout_table[layout].rozofs_inverse;

}

/**
  Get the rozofs_forward for a given layout
  
  @param layout : layout association with the file
  
  @retval rozofs_inverse associated with the layout
 */
static inline uint8_t rozofs_get_rozofs_forward(uint8_t layout) {
    if (layout >= LAYOUT_MAX) return 0;
    return rozofs_conf_layout_table[layout].rozofs_forward;

}

/**
  Get the rozofs_safe for a given layout
  
  @param layout : layout association with the file
  
  @retval rozofs_inverse associated with the layout
 */
static inline uint8_t rozofs_get_rozofs_safe(uint8_t layout) {
    if (layout >= LAYOUT_MAX) return 0;
    return rozofs_conf_layout_table[layout].rozofs_safe;

}

/**
  Get the rozofs_safe for a given layout
  
  @param layout : layout association with the file
  
  @retval rozofs_inverse associated with the layout
 */
static inline int rozofs_get_rozofs_invers_forward_safe(uint8_t layout, uint8_t * inv, uint8_t * fwd, uint8_t * safe) {
    *inv  = rozofs_conf_layout_table[layout].rozofs_inverse;
    *fwd  = rozofs_conf_layout_table[layout].rozofs_forward;
    *safe = rozofs_conf_layout_table[layout].rozofs_safe;
    if (layout >= LAYOUT_MAX) return -1;
    return 0;
}
/**
  Get the angle "p" for a given layout
  
  @param layout : layout association with the file
  @param projection_id : projection index in the layout
  
  @retval angle value
 */
static inline int rozofs_get_angles_p(uint8_t layout, uint8_t projection_id) {
    if (layout >= LAYOUT_MAX) return 0;
    if (projection_id >= rozofs_conf_layout_table[layout].rozofs_forward) return 0;    
    return rozofs_conf_layout_table[layout].rozofs_angles[projection_id].p;
}

/**
  Get the angle "q" for a given layout
  
  @param layout : layout association with the file
  @param projection_id : projection index in the layout
  
  @retval angle value
 */
static inline int rozofs_get_angles_q(uint8_t layout, uint8_t projection_id) {
    if (layout >= LAYOUT_MAX) return 0;
    if (projection_id >= rozofs_conf_layout_table[layout].rozofs_forward) return 0;    
    return rozofs_conf_layout_table[layout].rozofs_angles[projection_id].q;
}

/**
  Get the projection size for a given projection_id in the layout 
  
  @param layout : layout association with the file
  @param projection_id : projection index in the layout
  
  @retval projection size
 */ 
static inline int rozofs_get_psizes(uint8_t layout, uint32_t bsize, uint8_t projection_id) {
    if (layout >= LAYOUT_MAX) return 0;
    if (bsize > ROZOFS_BSIZE_MAX) return 0;
    if (projection_id >= rozofs_conf_layout_table[layout].rozofs_forward) return 0;
    return 2*rozofs_conf_layout_table[layout].sizes[bsize].rozofs_eff_psizes[projection_id];
}
/**
  Get the projection size for a given projection_id in the layout 
  
  @param layout : layout association with the file
  @param projection_id : projection index in the layout
  
  @retval projection size
 */ 
static inline int rozofs_get_legacy_psizes(uint8_t layout, uint32_t bsize, uint8_t projection_id) {
    if (layout >= LAYOUT_MAX) return 0;
    if (bsize > ROZOFS_BSIZE_MAX) return 0;
    if (projection_id >= rozofs_conf_layout_table[layout].rozofs_forward) return 0;
    return rozofs_conf_layout_table[layout].sizes[bsize].rozofs_psizes[projection_id];
}

/**
  Get the projection size for a given projection_id in the layout 
  
  @param layout : layout association with the file
  @param projection_id : projection index in the layout
  
  @retval projection size
 */ 
static inline int rozofs_get_128bits_psizes(uint8_t layout, uint32_t bsize, uint8_t projection_id) {
    if (layout >= LAYOUT_MAX) return 0;
    if (bsize > ROZOFS_BSIZE_MAX) return 0;
    if (projection_id >= rozofs_conf_layout_table[layout].rozofs_forward) return 0;
    return rozofs_conf_layout_table[layout].sizes[bsize].rozofs_eff_psizes[projection_id];
}
/**
  Get the projection size for a given projection_id in the layout 
  
  @param layout : layout association with the file
  @param projection_id : projection index in the layout
  
  @retval projection size
 */
static inline int rozofs_get_psizes_on_disk(uint8_t layout, uint32_t bsize, uint8_t projection_id) {
    int sz;
    
    if (layout >= LAYOUT_MAX) return 0;
    if (bsize > ROZOFS_BSIZE_MAX) return 0;
    if (projection_id >= rozofs_conf_layout_table[layout].rozofs_forward) return 0;
    sz = 2*rozofs_conf_layout_table[layout].sizes[bsize].rozofs_eff_psizes[projection_id] * sizeof (bin_t) 
          + sizeof (rozofs_stor_bins_hdr_t) 
          + sizeof(rozofs_stor_bins_footer_t);
    return sz;	  
}
/**
  Get the projection max size for a given  layout 
  
  @param layout : layout association with the file
  
  @retval projection size
 */
static inline int rozofs_get_max_psize(uint8_t layout, uint32_t bsize) {
    if (layout >= LAYOUT_MAX) return 0;
    if (bsize > ROZOFS_BSIZE_MAX) return 0;    
    return 2*rozofs_conf_layout_table[layout].sizes[bsize].rozofs_eff_psizes_max;
}

/**
  Get the projection max size for a given  layout 
  
  @param layout : layout association with the file
  
  @retval projection size
 */
static inline int rozofs_get_max_psize_128bits(uint8_t layout, uint32_t bsize) {
    if (layout >= LAYOUT_MAX) return 0;
    if (bsize > ROZOFS_BSIZE_MAX) return 0;    
    return rozofs_conf_layout_table[layout].sizes[bsize].rozofs_128bits_psizes_max;
}
/**
  Get the projection max size for a given  layout 
  
  @param layout : layout association with the file
  
  @retval projection size
 */
static inline int rozofs_get_max_psize_in_msg(uint8_t layout, uint32_t bsize) {
    int sz;
 
     if (layout >= LAYOUT_MAX) return 0;
    if (bsize > ROZOFS_BSIZE_MAX) return 0;    
    sz = 2*rozofs_conf_layout_table[layout].sizes[bsize].rozofs_eff_psizes_max * sizeof (bin_t) 
          + sizeof (rozofs_stor_bins_hdr_t) 
          + sizeof(rozofs_stor_bins_footer_t);
    if (sz%16) {	  
      sz += (16-(sz%16));
    }
    return sz;	  
}
/**
  Get the projection max size for a given  layout 
  
  @param layout : layout association with the file
  
  @retval projection size
 */
static inline int rozofs_get_max_psize_on_disk(uint8_t layout, uint32_t bsize) {
  return rozofs_get_max_psize_in_msg(layout, bsize);	  
}
/**
  Get the redundancy coefficient for a given  layout and block size 
  
  @param layout : layout association with the file
  
  @retval projection size
 */
static inline float rozofs_get_redundancy_coeff(uint8_t layout, uint32_t bsize) {
    if (layout >= LAYOUT_MAX) return 1;
    if (bsize > ROZOFS_BSIZE_MAX) return 1;    
    return rozofs_conf_layout_table[layout].sizes[bsize].redundancyCoeff;
}
/**
  Get the redundancy coefficinet for a given  layout and block size 
  
  @param layout : layout association with the file
  
  @retval projection size
 */
static inline char * rozofs_display_size(char * p, uint8_t layout, uint32_t bsize) {
  rozofs_conf_layout_t * pLayout;
  int                    prj_id;

  if (bsize > ROZOFS_BSIZE_MAX) { 
    p += sprintf(p,"Unknown block size value %d !!!\n",bsize);
    return p;
  }      


  switch (layout) {
    case LAYOUT_2_3_4:
	p += sprintf(p,"LAYOUT_2_3_4 ");
        break;
    case LAYOUT_4_6_8:
	p += sprintf(p,"LAYOUT_4_6_8 ");
        break;
    case LAYOUT_8_12_16:
	p += sprintf(p,"LAYOUT_8_12_16 ");
        break;
    default:
        p += sprintf(p,"Unknown layout value %d !!!\n",layout);
	return p;
  }
  
  p += sprintf(p,"/ block size %d ", ROZOFS_BSIZE_BYTES(bsize));
  
  
  pLayout = &rozofs_conf_layout_table[layout];
  p += sprintf(p,"/ redundancy factor %1.2f (%1.2f)\n",  
               pLayout->sizes[bsize].redundancyCoeff_128,
               pLayout->sizes[bsize].redundancyCoeff
	       );
  

  p += sprintf(p,"  prj |   p |   q | size in bytes\n");
  p += sprintf(p,"      |     |     | w/o header&footer\n");
  p += sprintf(p,"------+-----+-----+--------------\n");
  
  for (prj_id=0; prj_id < pLayout->rozofs_forward; prj_id++) {
    p += sprintf(p,"   %2d | %3d | %3d | %5d| %5d\n", 
                prj_id,
		pLayout->rozofs_angles[prj_id].p,
		pLayout->rozofs_angles[prj_id].q,
		pLayout->sizes[bsize].rozofs_eff_psizes[prj_id]*8*2,
		pLayout->sizes[bsize].rozofs_psizes[prj_id]*8);
  }
  return p;
}
/**
  Get the redundancy coefficinet for a given  layout and block size 
  
  @param layout : layout association with the file
  
  @retval projection size
 */
static inline char * rozofs_display_prjsize(char * p, uint8_t layout, uint32_t bsize) {
  rozofs_conf_layout_t * pLayout;
  int                    prj_id;

  if (bsize > ROZOFS_BSIZE_MAX) { 
    p += sprintf(p,"Unknown block size value %d !!!\n",bsize);
    return p;
  }      


  switch (layout) {
    case LAYOUT_2_3_4:
        p += sprintf(p,"{\n  \"layout\"     : 0,");
        p += sprintf(p,"\n  \"inverse\"    : 2,");
        p += sprintf(p,"\n  \"forward\"    : 3,");
        p += sprintf(p,"\n  \"safe\"       : 4,");
        break;
    case LAYOUT_4_6_8:
        p += sprintf(p,"{\n  \"layout\"     : 1,");
        p += sprintf(p,"\n  \"inverse\"    : 4,");
        p += sprintf(p,"\n  \"forward\"    : 6,");
        p += sprintf(p,"\n  \"safe\"       : 8,");
        
        break;
    case LAYOUT_8_12_16:
        p += sprintf(p,"{\n  \"layout\"     : 2,");
        p += sprintf(p,"\n  \"inverse\"    : 8,");
        p += sprintf(p,"\n  \"forward\"    : 12,");
        p += sprintf(p,"\n  \"safe\"       : 16,");
        break;
    default:
        p += sprintf(p,"Unknown layout value %d !!!\n",layout);
	return p;
  }
  

  pLayout = &rozofs_conf_layout_table[layout];
  
  p += sprintf(p,"\n  \"projections\": [\n");  
  for (prj_id=0; prj_id < pLayout->rozofs_forward; prj_id++) {
    if (prj_id != 0) p += sprintf(p,",\n");
    p += sprintf(p,"    { \"id\":%2d, \"p\":%3d, \"q\":%3d, \"disk size\":%5d }", 
                prj_id,
		pLayout->rozofs_angles[prj_id].p,
		pLayout->rozofs_angles[prj_id].q,
		rozofs_get_psizes_on_disk(layout, bsize, prj_id));
  }
  p += sprintf(p,"\n  ],");

  p += sprintf(p,"\n  \"block size\" : %d,", ROZOFS_BSIZE_BYTES(bsize));    
  p += sprintf(p,"\n  \"coefficient\": %1.2f\n}\n",  
               pLayout->sizes[bsize].redundancyCoeff_128);
  
  return p;
}
#endif
