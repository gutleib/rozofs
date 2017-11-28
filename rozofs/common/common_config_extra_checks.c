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

#include "config.h"
#include "common_config.h"
#include "log.h"

/*
**___________________________________________________________________
**
** Extra check to be called after common_config_read has been called
** in order to check consistency between different parameters
**
*/ 
void common_config_extra_checks() {  
  /*
  ** For self healing to be set, export host must be provided
  */
  if (strcasecmp(common_config.device_selfhealing_mode,"")!=0) {
    if (strcasecmp(common_config.export_hosts,"")==0) {
      severe("device_selfhealing_mode is \"%s\" while export_hosts is not defined -> set to \"\"",common_config.device_selfhealing_mode);
      common_config.device_selfhealing_mode[0] = 0;
    }
  }
}  
