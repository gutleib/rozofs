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


#ifndef ROZOFS_CORE_H
#define ROZOFS_CORE_H

typedef void (*rozofs_attach_crash_cbk_t) (int sig);

/*__________________________________________________________________________
  Declare a signal handler and what to do with core files
  ==========================================================================
  @param core_file_path    the path the core files should be generated
  @param max_core_files    maximum number of core files to keep in this
                           directory
  ==========================================================================*/
void rozofs_signals_declare(char * core_file_path, int max_core_files);

/*__________________________________________________________________________
  Attach a signal handler to be called on crash
  ==========================================================================
  PARAMETERS: 
  - entryPoint : the callback
  RETURN: none
  ==========================================================================*/
void rozofs_attach_crash_cbk(rozofs_attach_crash_cbk_t entryPoint) ;
/*__________________________________________________________________________
  Attach a callback to be called on reload
  ==========================================================================
  PARAMETERS: 
  - entryPoint : the callback
  RETURN: none
  ==========================================================================*/
void rozofs_attach_hgup_cbk(rozofs_attach_crash_cbk_t entryPoint) ;
#endif
