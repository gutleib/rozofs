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
# Find the native InfiniBand includes and library
#
#  INFINIBAND_INCLUDE_DIR - where to find verbs.h, etc.
#  INFINIBAND_LIBRARIES   - List of libraries when using rdma.
#  INFINIBAND_FOUND       - True if infiniband found.

FIND_PATH(INFINIBAND_INCLUDE_DIR verbs.h
  /usr/include/infiniband
)

SET(INFINIBAND_NAMES ibverbs )
FIND_LIBRARY(INFINIBAND_LIBRARY
  NAMES ${INFINIBAND_NAMES}
  PATHS /usr/lib /usr/local/lib
)

IF(INFINIBAND_INCLUDE_DIR AND INFINIBAND_LIBRARY)
  SET(INFINIBAND_FOUND TRUE)
  SET(INFINIBAND_LIBRARIES ${INFINIBAND_LIBRARY} )
ELSE(INFINIBAND_INCLUDE_DIR AND INFINIBAND_LIBRARY)
  SET(INFINIBAND_FOUND FALSE)
  SET(INFINIBAND_LIBRARIES)
ENDIF(INFINIBAND_INCLUDE_DIR AND INFINIBAND_LIBRARY)

IF(NOT INFINIBAND_FOUND)
   IF(INFINIBAND_FIND_REQUIRED)
     MESSAGE(FATAL_ERROR "ibverbs library and headers required.")
   ELSE(INFINIBAND_FIND_REQUIRED)
     MESSAGE(WARNING "ibverbs library and/or headers missing.")   
   ENDIF(INFINIBAND_FIND_REQUIRED)
ENDIF(NOT INFINIBAND_FOUND)

MARK_AS_ADVANCED(
  INFINIBAND_LIBRARY
  INFINIBAND_INCLUDE_DIR
)
