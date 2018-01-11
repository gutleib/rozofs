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

add_executable(storaged
    mproto.c
    mprotosvc.h
    sconfig.h
    sconfig.c
    storage.h
    storage.c
    storage_device_kpi.c
    storage_device_kpi.h                
    storaged.h
    storaged.c
    storaged_nblock_init.h
    storaged_nblock_init.c
    storaged_north_intf.c
    storaged_north_intf.h
    storio_crc32.c
    storio_crc32.h
    storaged_sub_thread.c
    storaged_sub_thread_intf.c
    storaged_sub_thread_intf.h 
)
target_link_libraries(storaged rozofs ${PTHREAD_LIBRARY} ${UUID_LIBRARY} ${CONFIG_LIBRARY} ${NUMA_LIBRARY})

add_executable(rozo_storage_fid2pathname
    rozo_storage_fid2pathname.c
)
target_link_libraries(rozo_storage_fid2pathname rozofs ${PTHREAD_LIBRARY} ${UUID_LIBRARY} ${CONFIG_LIBRARY})

add_executable(stspare
    sconfig.h
    sconfig.c
    storage.h
    storage.c
    storage_device_kpi.c
    storage_device_kpi.h                  
    storio_crc32.c
    storio_crc32.h   
    storio_fid_cache.c
    stspare.c   
    stspare_fid_cache.c
    stspare_fid_cache.h
    rbs_eclient.c
    rbs.h    
    rbs.c  
)
target_link_libraries(stspare rozofs ${PTHREAD_LIBRARY} ${UUID_LIBRARY} ${CONFIG_LIBRARY} ${NUMA_LIBRARY})

add_executable(storage_rebuild
    sconfig.h
    sconfig.c
    storage.h
    storage.c
    storage_device_kpi.c
    storage_device_kpi.h       
    rbs.h
    rbs_transform.h
    rbs_transform.c
    rbs_sclient.h
    rbs_sclient.c
    rbs_eclient.h
    rbs_eclient.c
    rbs.c
    storaged.h
    storio_crc32.c
    storio_crc32.h    
    storage_rebuild.c
     
)
target_link_libraries(storage_rebuild rozofs ${PTHREAD_LIBRARY} ${UUID_LIBRARY} ${CONFIG_LIBRARY})

add_executable(storage_list_rebuilder
    storage.h
    rbs.h
    rbs_transform.h
    rbs_transform.c
    rbs_sclient.h
    rbs_sclient.c
    rbs_eclient.h
    rbs_eclient.c
    rbs.c
    storaged.h
    storage_list_rebuilder.c        
)

target_link_libraries(storage_list_rebuilder rozofs ${PTHREAD_LIBRARY} ${UUID_LIBRARY} ${CONFIG_LIBRARY})


add_executable(storio
    sconfig.h
    sconfig.c
    storage.h
    storage.c
    storage_device_kpi.c
    storage_device_kpi.h       
    sproto_nb.h
    sproto_nb.c
    sprotosvc_nb.c
    sprotosvc_nb.h
    storio_nblock_init.h
    storio_nblock_init.c
    storio_north_intf.c
    storio_north_intf.h
    storage_fd_cache.c
    storage_fd_cache.h
    storio.c
    storio_cache.h
    storio_cache.c
    storio_bufcache.c
    storio_bufcache.h
    storio_disk_thread_intf.h
    storio_disk_thread_intf.c
    storio_device_monitor.c    
    storio_disk_thread.c
    storio_device_mapping.c
    storio_device_mapping.h
    storio_serialization.h
    storio_serialization.c
    storio_fid_cache.c
    storio_fid_cache.h
    storio_crc32.c
    storio_crc32.h         
)

if(INFINIBAND_FOUND AND RDMA_FOUND AND NOT NO_RDMA)
  message(STATUS "Compiling storio with RDMA support")
  set(storio_rmda_sources
  # RDMA section
      ../../rozofs/rdma/rozofs_rdma.c
      ../../rozofs/rdma/rozofs_rdma.h
  )
  add_library(storio_rmda STATIC ${storio_rmda_sources})
  set_target_properties(storio_rmda PROPERTIES COMPILE_FLAGS "-fPIC")
  target_link_libraries(storio storio_rmda rozofs ${PTHREAD_LIBRARY} ${UUID_LIBRARY} ${CONFIG_LIBRARY} ${NUMA_LIBRARY} ${RDMA_LIBRARY} ${INFINIBAND_LIBRARY})
else(INFINIBAND_FOUND AND RDMA_FOUND AND NOT NO_RDMA)
  message(STATUS "Compiling storio without RDMA support")
  target_link_libraries(storio rozofs ${PTHREAD_LIBRARY} ${UUID_LIBRARY} ${CONFIG_LIBRARY} ${NUMA_LIBRARY})
endif(INFINIBAND_FOUND AND RDMA_FOUND AND NOT NO_RDMA)

add_executable(rozodump
    rozodump.c
    storage.h   
    storio_crc32.c
    storio_crc32.h
)
target_link_libraries(rozodump rozofs ${PTHREAD_LIBRARY} ${UUID_LIBRARY} ${CONFIG_LIBRARY})

# Install
install(CODE "EXECUTE_PROCESS (COMMAND mkdir -p ${ROZOFS_CONFIG_DIR})" COMPONENT storage)

install(TARGETS rozo_storage_fid2pathname
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT rozo_storage_fid2pathname
)
install(TARGETS stspare
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT stspare
)
install(TARGETS storaged
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT storage
)
install(TARGETS storage_rebuild
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT storage_rebuild
)
install(TARGETS storage_list_rebuilder
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT storage_list_rebuilder
)        
install(TARGETS storio
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT storio
)
install(TARGETS rozodump
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT rozodump
)

install(FILES storage.conf.sample DESTINATION ${ROZOFS_CONFIG_DIR} COMPONENT storage)
install(FILES storage.conf DESTINATION ${ROZOFS_CONFIG_DIR} COMPONENT storage)
