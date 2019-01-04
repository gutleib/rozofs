/*
  Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
  This file is part of Rozofs.

  Rozofs is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published
  by the Free Software Foundation, version 2.

  Rozofs is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see
  <http://www.gnu.org/licenses/>.
 */
 
 
#ifndef RZOFS_SOCKET_FAMILY_H
#define RZOFS_SOCKET_FAMILY_H

typedef enum _rozofs_socket_family_e
{
  ROZOFS_SOCK_FAMILY_TEST_NORTH = 0,
  ROZOFS_SOCK_FAMILY_TEST_SOUTH,
  ROZOFS_SOCK_FAMILY_STORAGE_NORTH,
  ROZOFS_SOCK_FAMILY_STORAGE_SOUTH,
  ROZOFS_SOCK_FAMILY_EXPORT_NORTH,
  ROZOFS_SOCK_FAMILY_EXPORT_SOUTH,
  ROZOFS_SOCK_FAMILY_MAX
} rozofs_socket_family_e;


/**
* basename name of the af unix sockets
*/
#define ROZOFS_SOCK_FAMILY_TEST_NORTH_SUNPATH  "/tmp/rozofs_test_north"
#define ROZOFS_SOCK_FAMILY_TEST_SOUTH_SUNPATH "/tmp/rozofs_test_south" 
#define ROZOFS_SOCK_FAMILY_STORCLI_NORTH_SUNPATH "/tmp/rozofs_storcli_north_eid_"
#define ROZOFS_SOCK_FAMILY_STORCLI_MOJETTE_NORTH_SUNPATH "/tmp/rozofs_stcmoj_north_eid_"
#define ROZOFS_SOCK_FAMILY_STORCLI_MOJETTE_SOUTH_SUNPATH "/tmp/rozofs_stcmoj_south_eid_"
#define ROZOFS_SOCK_FAMILY_EXPORT_NORTH_SUNPATH "/tmp/rozofs_export_north"
#define ROZOFS_SOCK_FAMILY_EXPORT_SOUTH "/tmp/rozofs_export_south"
#define ROZOFS_SOCK_FAMILY_DISK_SOUTH "/tmp/rozofs_disk_south"
#define ROZOFS_SOCK_FAMILY_DISK_RDMA_SOUTH "/tmp/rozofs_disk_rdma_south"
#define ROZOFS_SOCK_FAMILY_DISK_NORTH "/tmp/rozofs_disk_north"
#define ROZOFS_SOCK_FAMILY_QUOTA_NORTH_SUNPATH "/tmp/rozofs_quota"
#define ROZOFS_SOCK_FAMILY_FUSE_SOUTH "/tmp/rozofs_fuse_south"
#define ROZOFS_SOCK_FAMILY_FUSE_NORTH "/tmp/rozofs_fuse_north"

#define ROZOFS_SOCK_FAMILY_STORAGED "/tmp/rozofs_storaged"

#define ROZOFS_SOCK_FAMILY_STDALONE_EXP_SOUTH "/tmp/fake_export_south"
#define ROZOFS_SOCK_FAMILY_STDALONE_EXP_NORTH "/tmp/fake_export_north"
/**
* socket type
*/
typedef enum _rozofs_socket_family_type_e
{
  ROZOFS_SOCK_TYPE_NORTH = 0,
  ROZOFS_SOCK_TYPE_SOUTH,
   ROZOFS_SOCK_TYPE_MAX
} rozofs_socket_family_type_e;

/**
*
*/

typedef struct _rozofs_family_north_conf
{
  rozofs_socket_family_e family;  /**< family id  */
  int nb_instances;
} rozofs_family_north_conf;


typedef struct _rozofs_socket_any_family_conf
{
  rozofs_socket_family_e type;    /**< north or south  */
  rozofs_socket_family_e family;  /**< family id  */
  int first_instance;             /**< index of the first instance -> NS for south socket */  
  int nb_instances;               /**< number of instances */
} rozofs_socket_any_family_conf;

#endif
