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
include_directories(${CMAKE_SOURCE_DIR}/src/storaged)
include_directories(${CMAKE_SOURCE_DIR}/src/exportd)

add_executable(test_list
    ${CMAKE_SOURCE_DIR}/rozofs/common/list.h
    test_list.c
)

add_executable(test_htable
    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.h
    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.c
    ${CMAKE_SOURCE_DIR}/rozofs/common/htable.h
    ${CMAKE_SOURCE_DIR}/rozofs/common/htable.c
    test_htable.c
)

add_executable(test_dist
    ${CMAKE_SOURCE_DIR}/rozofs/common/dist.h
    test_dist.c
)

add_executable(read_prj_blocks 
    read_prj_blocks.c
    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.h
    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.c
    ${CMAKE_SOURCE_DIR}/rozofs/rozofs_srv.h
    ${CMAKE_SOURCE_DIR}/rozofs/rozofs_srv.c
)
add_executable(test_truncate
    test_truncate.c
)

add_executable(test_write_all_offset
    test_write_all_offset.c
)

add_executable(test_transform
    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.h
    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.c
    ${CMAKE_SOURCE_DIR}/rozofs/common/transform.h
    ${CMAKE_SOURCE_DIR}/rozofs/common/transform.c
    test_transform.c
)

add_executable(transform_throughput
    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.h
    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.c
    ${CMAKE_SOURCE_DIR}/rozofs/common/transform.h
    ${CMAKE_SOURCE_DIR}/rozofs/common/transform.c
    transform_throughput.c
)

add_executable(transform_file
    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.h
    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.c
    ${CMAKE_SOURCE_DIR}/rozofs/common/transform.h
    ${CMAKE_SOURCE_DIR}/rozofs/common/transform.c
    transform_file.c
)

add_executable(rpc_throughput
    ${CMAKE_SOURCE_DIR}/rozofs/rpc/rpcclt.h
    ${CMAKE_SOURCE_DIR}/rozofs/rpc/rpcclt.c
    rpc_throughput.h
    rpc_throughputxdr.c
    rpc_throughputclt.c
    rpc_throughput_client.c
)

add_executable(rpc_throughput_server
    rpc_throughput.h
    rpc_throughputxdr.c
    rpc_throughput_server.c
    rpc_throughputsvc.c
)

#add_executable(test_volume
#    ${CMAKE_SOURCE_DIR}/rozofs/rozofs.h
#    ${CMAKE_SOURCE_DIR}/rozofs/rozofs_srv.h
#    ${CMAKE_SOURCE_DIR}/rozofs/rozofs_srv.c
#    ${CMAKE_SOURCE_DIR}/rozofs/rozofs_service_ports.c
#    ${CMAKE_SOURCE_DIR}/rozofs/rozofs_service_ports.h
#    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.h
#    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.c
#    ${CMAKE_SOURCE_DIR}/rozofs/rpc/rpcclt.h
#    ${CMAKE_SOURCE_DIR}/rozofs/rpc/rpcclt.c
#    ${CMAKE_SOURCE_DIR}/rozofs/rpc/mproto.h
#    ${CMAKE_SOURCE_DIR}/rozofs/rpc/mprotoxdr.c
#    ${CMAKE_SOURCE_DIR}/rozofs/rpc/mprotoclt.c
#    ${CMAKE_SOURCE_DIR}/rozofs/rpc/mclient.c
#    ${CMAKE_SOURCE_DIR}/src/exportd/volume.h
#    ${CMAKE_SOURCE_DIR}/src/exportd/volume.c
#    test_volume.c
#)
#target_link_libraries(test_volume ${PTHREAD_LIBRARY})


add_executable(test_sconfig
    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.h
    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.c
    ${CMAKE_SOURCE_DIR}/src/storaged/sconfig.h
    ${CMAKE_SOURCE_DIR}/src/storaged/sconfig.c
    test_sconfig.c
)
target_link_libraries(test_sconfig ${CONFIG_LIBRARY})

add_executable(test_econfig
    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.h
    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.c
    ${CMAKE_SOURCE_DIR}/rozofs/rozofs_srv.h
    ${CMAKE_SOURCE_DIR}/rozofs/rozofs_srv.c
    ${CMAKE_SOURCE_DIR}/src/exportd/econfig.h
    ${CMAKE_SOURCE_DIR}/src/exportd/econfig.c
    test_econfig.c
)
target_link_libraries(test_econfig ${CONFIG_LIBRARY})


#add_executable(test_mdirent
#    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.h
#    ${CMAKE_SOURCE_DIR}/rozofs/common/xmalloc.c
#    ${CMAKE_SOURCE_DIR}/src/exportd/dirent_journal.h
#    ${CMAKE_SOURCE_DIR}/src/exportd/mdirent.h
#    ${CMAKE_SOURCE_DIR}/src/exportd/dirent_file_repair.c
#    ${CMAKE_SOURCE_DIR}/src/exportd/writeback_cache.c
#    ${CMAKE_SOURCE_DIR}/src/exportd/dirent_file.c
#    ${CMAKE_SOURCE_DIR}/src/exportd/dirent_cache.c
#    ${CMAKE_SOURCE_DIR}/src/exportd/dirent_search.c
#    ${CMAKE_SOURCE_DIR}/src/exportd/dirent_insert.c
#    test_mdirent.c
#)
#target_link_libraries(test_mdirent ${UUID_LIBRARY})


#add_executable(export_tool
#       ${CMAKE_SOURCE_DIR}//src/exportd/cache.c
#       ${CMAKE_SOURCE_DIR}//src/exportd/exp_cache.c
#       ${CMAKE_SOURCE_DIR}//src/exportd/exp_cache.h
#    export_tool.c
#)
#target_link_libraries(export_tool rozofs ${CONFIG_LIBRARY} ${UUID_LIBRARY})



add_executable(test_statvfs
    test_statvfs.c
)
