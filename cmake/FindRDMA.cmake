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
# Find the native RDMA includes and library
#
#  RDMA_INCLUDE_DIR - where to find rdma_cma.h, etc.
#  RDMA_LIBRARIES   - List of libraries when using rdma.
#  RDMA_FOUND       - True if fuse found.

FIND_PATH(RDMA_INCLUDE_DIR rdma_cma.h
  /usr/include/FDL/rdma
)

SET(RDMA_NAMES rdmacm)
FIND_LIBRARY(RDMA_LIBRARY
  NAMES ${RDMA_NAMES}
  PATHS /usr/lib/ /usr/local/lib/
)

IF(RDMA_INCLUDE_DIR AND RDMA_LIBRARY)
  SET(RDMA_FOUND TRUE)
  SET(RDMA_LIBRARIES ${RDMA_LIBRARY} )
ELSE(RDMA_INCLUDE_DIR AND RDMA_LIBRARY)
  SET(RDMA_FOUND FALSE)
  SET(RDMA_LIBRARIES)
ENDIF(RDMA_INCLUDE_DIR AND RDMA_LIBRARY)

IF(NOT RDMA_FOUND)
   IF(RDMA_FIND_REQUIRED)
     MESSAGE(FATAL_ERROR "rdmacm library and headers required.")
   ELSE(RDMA_FIND_REQUIRED)
     MESSAGE(WARNING "rdmacm library and/or headers missing.")
   ENDIF(RDMA_FIND_REQUIRED)
ENDIF(NOT RDMA_FOUND)

MARK_AS_ADVANCED(
  RDMA_LIBRARY
  RDMA_INCLUDE_DIR
)
