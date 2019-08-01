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


add_definitions(-DD_REENTRANT -D_FILE_OFFSET_BITS=64)
include_directories(${CMAKE_BINARY_DIR}) # for config.h
include_directories(${CMAKE_SOURCE_DIR}) # for librozofs headers
include_directories(${PTHREAD_INCLUDE_DIR})
link_directories(${CMAKE_BINARY_DIR}/rozofs)

add_executable(rozo_synchro_analyze_result
    rozo_synchro_analyze_result.c
)
target_link_libraries(rozo_synchro_analyze_result)

# Install
install(CODE "EXECUTE_PROCESS (COMMAND mkdir -p ${ROZOFS_CONFIG_DIR})" COMPONENT synchro)

install(TARGETS rozo_synchro_analyze_result
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT synchro
)

install(PROGRAMS rozo_synchro DESTINATION bin)
install(PROGRAMS rozo_synchro.sh DESTINATION bin)
