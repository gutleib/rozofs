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

include_directories(${CMAKE_BINARY_DIR}) # for config.h
include_directories(${CMAKE_SOURCE_DIR}) # for librozofs headers
include_directories(${READLINE_INCLUDE_DIR})
link_directories(${CMAKE_BINARY_DIR}/rozofs)

add_executable(rozodiag
    rozodiag.c
    ../../rozofs/rozofs_service_ports.c
    ../../rozofs/rozofs_service_ports.h    
    ../../rozofs/common/log.h 
    ../../rozofs/common/log.c    
)
target_link_libraries(rozodiag ${READLINE_LIBRARY})

# Install

install(TARGETS rozodiag
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT rozodiag
)
install(PROGRAMS rozo_status.py DESTINATION bin)
install(PROGRAMS rozo_node_status.py DESTINATION bin)
install(PROGRAMS rozo_dumpcnf DESTINATION bin)
install(PROGRAMS rozo_memdump DESTINATION bin)
install(PROGRAMS rozo_roce DESTINATION bin)
install(PROGRAMS rozo_nstat DESTINATION bin)
install(PROGRAMS rozo_core DESTINATION bin)
