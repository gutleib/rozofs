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


if(INFINIBAND_FOUND AND RDMA_FOUND AND NOT NO_RDMA)
  add_definitions(-DROZOFS_RDMA)
endif()

add_executable(storcli
     rozofs_storcli.h
    rozofs_storcli_transform.h
    rozofs_storcli_lbg_cnf_supervision.h
    storage_proto.h
    rozofs_storcli_rpc.h        
    storcli_main.h
    rozofs_storcli_send_new.c
    rozofs_storcli_lbg_cnf_supervision.c
    rozofs_storcli_transform_utility.c
    rozofs_storcli_transform.c
#    rozofs_storcli_transform_opt.c
    rozofs_storcli_mgt.c
    rozofs_storcli_write.c
    rozofs_storcli_nblock_init.c
    storcli_main.c
    rozofs_storcli_north_intf.c
    storcli_stub.c
#    rozofs_storcli_read.c
    rozofs_bt_storcli_read.c
    rozofs_storcli_resize.c
    rozofs_storcli_rebuild_prj.c
    rozofs_storcli_reload_storage_config.c
    rozofs_storcli_reload_storage_config.h
    storage_lbg.c
    rozofs_storcli_cnx_supervision.c
    rozofs_storcli_truncate.c        
    storcli_ring.c        
    storcli_ring.h        
    rozofs_storcli_mojette_thread.c
    rozofs_storcli_mojette_thread_intf.c 
    rozofs_storcli_mojette_thread_intf.h
    rozofs_storcli_delete.c
    rozofs_storcli_wr_repair.c
    rozofs_storcli_sharedmem.c
    rozofs_storcli_sharedmem.h
    standalone_client_send.c  
    standalone_client_send.h
   ../rozofsmount/rozofs_bt_memreg.c
   rozofs_bt_storcli.c
 )

if(INFINIBAND_FOUND AND RDMA_FOUND AND NOT NO_RDMA)
  message(STATUS "Compiling storcli with RDMA support")
  set(storcli_rmda_sources
  # RDMA section
      rdma_client_send.c
      ../../rozofs/rdma/rozofs_rdma.c
      ../../rozofs/rdma/rozofs_rdma.h
  )
  add_library(storcli_rmda STATIC ${storcli_rmda_sources})
  set_target_properties(storcli_rmda PROPERTIES COMPILE_FLAGS "-fPIC")
  target_link_libraries(storcli  storcli_rmda rozofs ${PTHREAD_LIBRARY} ${CRYPT_LIBRARY} ${CONFIG_LIBRARY} ${NUMA_LIBRARY} ${RDMA_LIBRARY} ${RT_LIBRARY} ${INFINIBAND_LIBRARY})
else(INFINIBAND_FOUND AND RDMA_FOUND AND NOT NO_RDMA)
  message(STATUS "Compiling storcli without RDMA support")
  target_link_libraries(storcli rozofs ${PTHREAD_LIBRARY} ${CRYPT_LIBRARY} ${CONFIG_LIBRARY} ${RT_LIBRARY} ${NUMA_LIBRARY})
endif(INFINIBAND_FOUND AND RDMA_FOUND AND NOT NO_RDMA)


# Install
install(TARGETS storcli
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT storcli
)
