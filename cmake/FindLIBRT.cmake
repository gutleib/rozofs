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

# - Find libtr (Posix realtime extension
# Find the native PTHREAD includes and library
#

#  RT_LIBRARIES   - List of libraries when using pthread.
#  RT_FOUND       - True if rt found.



SET(RT_NAMES rt)
FIND_LIBRARY(RT_LIBRARY
  NAMES ${RT_NAMES}
  PATHS /usr/lib /usr/local/lib
)

IF(RT_LIBRARY)
  SET(RT_FOUND TRUE)
  SET(RT_LIBRARIES ${RT_LIBRARY} )
ELSE(RT_LIBRARY)
  SET(RT_FOUND FALSE)
  SET(RT_LIBRARIES)
ENDIF(RT_LIBRARY)

IF(NOT RT_FOUND)
   IF(RT_FIND_REQUIRED)
     MESSAGE(FATAL_ERROR "rt library required.")
   ENDIF(RT_FIND_REQUIRED)
ENDIF(NOT RT_FOUND)

MARK_AS_ADVANCED(
  PTHREAD_LIBRARY
  PTHREAD_INCLUDE_DIR
)
