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

/* Exportd profiling protocol
 */
%#include <rozofs/rozofs.h>


#define EPP_MAX_VOLUMES     16
#define EPP_MAX_STORAGES    2048
#define EPP_MAX_EXPORTS     2048

enum epp_status_t {
    EPP_SUCCESS = 0,
    EPP_FAILURE = 1
};

union epp_status_ret_t switch (epp_status_t status) {
    case EPP_FAILURE:   int error;
    default:            void;
};

struct epp_estat_t {
    uint32_t    eid;
    uint32_t    vid;
    uint32_t    bsize;
    uint64_t    blocks;
    uint64_t    bfree;
    uint64_t    files;
    uint64_t    ffree;
    char        path[ROZOFS_PATH_MAX];
    char        name[ROZOFS_PATH_MAX];
};

struct epp_sstat_t {
    uint16_t    cid;
    uint16_t    sid;
    char        host[ROZOFS_HOSTNAME_MAX];
    uint8_t     status;
    uint8_t     site;
    uint64_t    size;
    uint64_t    free;
};

struct epp_vstat_t {
    uint16_t    vid;
    uint16_t    georep;
    uint16_t    bsize;
    uint64_t    bfree;
    uint64_t    blocks;    
    uint32_t    nb_storages;
    epp_sstat_t sstats[EPP_MAX_STORAGES];
};

struct epp_profiler_t {
    uint64_t    uptime;
    uint64_t    now;
    uint8_t     vers[20];
    uint32_t    nb_volumes;
    epp_vstat_t vstats[EPP_MAX_VOLUMES];
    uint32_t    nb_exports;
    epp_estat_t estats[EPP_MAX_EXPORTS];
};

union epp_profiler_ret_t switch (epp_status_t status) {
    case EPP_SUCCESS:    epp_profiler_t profiler;
    case EPP_FAILURE:    int error;
    default:            void;
};

program EXPORTD_PROFILE_PROGRAM {
    version EXPORTD_PROFILE_VERSION {
        void
        EPP_NULL(void)          = 0;

        epp_profiler_ret_t
        EPP_GET_PROFILER(void)  = 1;

        epp_status_ret_t
        EPP_CLEAR(void)         = 2;

    }=1;
} = 0x20000005;
