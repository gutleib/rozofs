# Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
# This file is part of Rozofs.
#
# Rozofs is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published
# by the Free Software Foundation, version 2.
#
# Rozofs is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see
# <http://www.gnu.org/licenses/>.

# - Find numa
# Find the native NUMA includes and library
#
#  PCRE_INCLUDE_DIR - where to find pcre.h, etc.
#  PCRE_LIBRARIES   - List of libraries when using PCRE.
#  PCRE_FOUND       - True if pcre found.

FIND_PATH(PCRE_INCLUDE_DIR pcre.h
  /usr/include
)

SET(PCRE_NAMES pcre)
FIND_LIBRARY(PCRE_LIBRARY
  NAMES ${PCRE_NAMES}
  PATHS /usr/lib /usr/local/lib
)

IF(PCRE_INCLUDE_DIR AND PCRE_LIBRARY)
  SET(PCRE_FOUND TRUE)
  SET(NUMA_LIBRARIES ${NUMA_LIBRARY} )
ELSE(PCRE_INCLUDE_DIR AND PCRE_LIBRARY)
  SET(PCRE_FOUND FALSE)
  SET(PCRE_LIBRARIES)
ENDIF(PCRE_INCLUDE_DIR AND PCRE_LIBRARY)

IF(NOT PCRE_FOUND)
   IF(PCRE_FIND_REQUIRED)
     MESSAGE(FATAL_ERROR "pcre library and headers required.")
   ENDIF(PCRE_FIND_REQUIRED)
ENDIF(NOT PCRE_FOUND)

MARK_AS_ADVANCED(
  PCRE_LIBRARY
  PCRE_INCLUDE_DIR
)
