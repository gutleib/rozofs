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
link_directories(${CMAKE_SOURCE_DIR}/libfuse/lib)

add_executable(rozofsmount
    file.h
    rozofsmount.c
    rozofs_fuse_api.h
    rozofs_fuse.c
    rozofs_fuse.h
    rozofs_tx_client_export.c
    rozofs_non_blocking_main.c
    rozofsmount.h
    rozofs_attr.c
    rozofs_lookup.c
    rozofs_mkdir.c
    rozofs_mknod.c
    rozofs_open.c
    rozofs_link.c
    rozofs_rmdir.c   
    rozofs_rename.c   
    rozofs_statfs.c   
    rozofs_create.c 
    rozofs_xattr.c
    rozofs_read.c
    rozofs_resize.c
    rozofs_write.c
    rozofs_readdir.c
    rozofs_sharedmem.c
    rozofs_sharedmem.h
    rozofs_reload_export_gateway_conf.c
    rozofs_reload_export_gateway_conf.h
    rozofs_export_gateway_conf_non_blocking.h
    rozofs_export_gateway_conf_non_blocking.c
    rozofs_modeblock_cache.c
    rozofs_modeblock_cache.h    
    rozofs_cache.h    
    rozofs_cache.c
    rozofs_rw_load_balancing.h
    rozofs_rw_load_balancing.c
    rozofs_file_lock.c
    rozofs_xattr_flt.h
    rozofs_xattr_flt.c
    rozofs_export_cnx_supervision.c
    rozofs_fuse_thread.c
    rozofs_fuse_wr_thread.c
    rozofs_fuse_thread_intf.c
    rozofs_fuse_thread_intf.h
    fuse_reply_thread.c
    rozofs_kpi.c
    rozofs_kpi.h
#    rozofs_acl.c
#    rozofs_acl.h
    
   xattr_acl.c
   xattr_main.c
   xattr_nocache.c
   xattr_security.c
   xattr_stub.c
   xattr_system.c
   xattr_trusted.c
   xattr_user.c
   rozofs_ext4.h
   xattr.h
   xattr_main.h
   rozofs_opendir.c
   rozofs_cachetrack.c
   rozofs_cachetrack.h
# Metadata services for standalone mode
#   ../exportd/rozofs_stdalone_export_thread_intf.c
#   ../exportd/rozofs_stdalone_export_threads.c
#  ../exportd/rozofs_stdalone_export_threads.h
#   ../exportd/rozofs_stdalone_eproto_nb.c
#   ../exportd/rozofs_stdalone_eproto_nb.h
#   ../exportd/rozofs_stdalone_eprotosvc_nb.c
#   ../exportd/rozofs_stdalone_eprotosvc_nb.h
   rozofs_io_error_trc.h
   rozofs_io_error_trc.c 
   
   rozofs_bt_init.c
   rozofs_bt_north_intf.c
   rozofs_bt_proto.h  
   rozofs_bt_thread_intf.c
   rozofs_bt_api.h 
   rozofs_bt_memreg.c 
   rozofs_bt_inode.c
   rozofs_bt_inode.h
   rozofs_bt_ioread.c
   rozofs_bt_dirent.c
   rozofs_bt_client_lbg.c
   rozofs_bt_client_trk_reader.c
   rozofs_bt_dirent_thread_intf.c
   rozofs_bt_dirent_cache.c
   ../exportd/dirent_file.c
   ../exportd/dirent_cache.c
   ../exportd/dirent_search.c
   ../exportd/dirent_insert.c
   ../exportd/dirent_file_repair.c
##   ../exportd/test_mdirent_stub.c
   ../exportd/dirent_writeback_cache.c
   ../exportd/writeback_cache.c
  rozofs_bt_dirent_stub.c
 rozofsmount_netdata_cfg.c
)
#pthread need by fuse
#target_link_libraries(rozofsmount rozofs ${PTHREAD_LIBRARY} ${FUSE_LIBRARY} ${UUID_LIBRARY} ${CRYPT_LIBRARY} ${CONFIG_LIBRARY} ${NUMA_LIBRARY})
target_link_libraries(rozofsmount rozofs fuse dl ${PTHREAD_LIBRARY}  ${UUID_LIBRARY} ${CRYPT_LIBRARY} ${CONFIG_LIBRARY} ${NUMA_LIBRARY} ${RT_LIBRARY})



add_executable(rozo_locate_projections
       rozo_locate_projections.c    
)
target_link_libraries(rozo_locate_projections rozofs ${UUID_LIBRARY} ${CONFIG_LIBRARY} ${PTHREAD_LIBRARY})

add_executable(rozo_mount_send_stats 
    rozo_mount_send_stats.c
)
target_link_libraries(rozo_mount_send_stats rozofs ${CONFIG_LIBRARY})

add_executable(rozofsmount_netdata.plugin
    rozofsmount_netdata.c
    rozofsmount_netdata_cfg.c
)
target_link_libraries(rozofsmount_netdata.plugin rozofs ${PTHREAD_LIBRARY} ${CONFIG_LIBRARY} ${RT_LIBRARY})


add_executable(rozo_multifile 
    rozofs_multiple_dir.c
)
target_link_libraries(rozo_multifile)

# Install
install(TARGETS rozofsmount
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT mount
)
# Install
install(TARGETS rozo_locate_projections
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT mount
)
# Install
install(TARGETS rozo_multifile
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT mount
)

# Install rozo_mount_send_stats
install(TARGETS rozo_mount_send_stats
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT rozo_mount_send_stats
)

    
# Create symbolic link /sbin/mount.rozofs -> /sbin/mount.fuse (mount.rozofs command)
install(CODE "MESSAGE(\"Create symbolic link /sbin/mount.rozofs -> /sbin/mount.fuse\")")
install(CODE "EXECUTE_PROCESS (COMMAND ln -sf /sbin/mount.fuse /sbin/mount.rozofs)" COMPONENT client)
install(PROGRAMS rozo_mount_clusterstats DESTINATION bin)
install(PROGRAMS rozo_mount_repquota DESTINATION bin)
install(PROGRAMS rozo_distribution_check DESTINATION bin)
install(PROGRAMS rozo_mount_cleanup DESTINATION bin)
install(PROGRAMS rozofsmount_wrapper DESTINATION bin)
install(PROGRAMS rozo_trash DESTINATION bin)
install(PROGRAMS rozo_scan DESTINATION bin)
install(PROGRAMS rozo_throughput DESTINATION bin)
install(PROGRAMS rozo_io DESTINATION bin)
install(PROGRAMS rozo_aging DESTINATION bin)
install(PROGRAMS rozo_move2slow DESTINATION bin)
install(PROGRAMS rozo_move2fast DESTINATION bin)
install(PROGRAMS rozo_file_consistency DESTINATION bin)
install(PROGRAMS rozo_private_broadcast DESTINATION bin)
