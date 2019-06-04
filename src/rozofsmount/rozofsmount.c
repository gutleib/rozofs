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

#include <pthread.h>
#include <inttypes.h>
#include <assert.h>
#include <semaphore.h>
#include <mntent.h>
#include <sys/resource.h>

#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/core/rozofs_timer_conf_dbg.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/core/rozofs_host_list.h>
#include <rozofs/core/rozo_launcher.h>
#include <rozofs/core/rozofs_string.h>
#include <rozofs/core/rozofs_numa.h>
//#include <rozofs/core/malloc_tracking.h>
#include <rozofs/common/rozofs_site.h>
#include <rozofs/common/common_config.h>
#include <rozofs/core/rozofs_cpu.h>

#include "rozofs_fuse.h"
#include "rozofs_fuse_api.h"
#include "rozofsmount.h"
#include "rozofs_sharedmem.h"
#include "rozofs_modeblock_cache.h"
#include "rozofs_cache.h"
#include "rozofs_rw_load_balancing.h"
#include "rozofs_reload_export_gateway_conf.h"
#include "rozofs_xattr_flt.h"

#define hash_xor8(n)    (((n) ^ ((n)>>8) ^ ((n)>>16) ^ ((n)>>24)) & 0xff)
#define INODE_HSIZE (8192*32)
//#define PATH_HSIZE  (8192*8)

// Filesystem source (first field in /etc/mtab)
#define FSNAME "rozofs"

// Filesystem type (third field in /etc/mtab)
// If the kernel suppports it,
///etc/mtab and /proc/mounts will show the filesystem type as fuse.rozofs
#define FSTYPE "rozofs"

// File for list all mounted file systems
#define MOUNTED_FS_FILE_CHECK "/proc/mounts"

#define FUSE28_DEFAULT_OPTIONS "default_permissions,allow_other,fsname="FSNAME",subtype="FSTYPE",big_writes"""
#define FUSE28_DEFAULT_OPTIONS_WB "default_permissions,allow_other,fsname="FSNAME",subtype="FSTYPE",big_writes,atomic_o_trunc"""
#define FUSE27_DEFAULT_OPTIONS "default_permissions,allow_other,fsname="FSNAME",subtype="FSTYPE""""
#define FUSE27_DEFAULT_OPTIONS_WB "default_permissions,allow_other,,atomic_o_trunc,fsname="FSNAME",subtype="FSTYPE""""

#define CACHE_TIMEOUT 10.0
#define CONNECTION_THREAD_TIMESPEC  2




int rozofs_rotation_read_modulo = 0;
char *rozofs_mountpoint = NULL;
int rozofs_max_storcli_tx =0;  /**< depends on the number of storcli processes */
    
DEFINE_PROFILING(mpp_profiler_t) ;

sem_t *semForEver; /**< semaphore used for stopping rozofsmount: typically on umount */

uint16_t rozofsmount_diag_port=0;

int rozofs_mountpoint_check(const char * mntpoint);
/*
** Exportd id free byte count for quota management
*/
uint64_t eid_free_quota = -1; // -1 means no quota 
uint64_t hash_inode_collisions_count = 0;
uint64_t hash_inode_max_collisions = 0;
uint64_t hash_inode_cur_collisions;

/*
** Count the number of opened files
*/
uint64_t rozofs_opened_file;

uint64_t   rozofs_client_hash=0;

rozofs_fuse_profile_t fuse_kern_profile[]={
  {1024,64,48}, /* profile 0 */
  {1024,1024,1022}, /* profile 1 */
  {4096,1024,1022}, /* profile 2 : large readahead: WAN case with latency smaller than 50 ms */
  {8192,1024,1022}, /* profile 3 : large readahead: WAN case with latency greater than 50 ms */
};

#define  ROZOFS_DEFAULT_FUSE_PROFILE 1
#define ROZOFS_MAX_PROFILE (sizeof(fuse_kern_profile)/ sizeof(rozofs_fuse_profile_t))
/**
* fuse request/reponse trace parameters
*/

int rozofs_trc_wr_idx; /**< current trace index */
int rozofs_trc_buf_full; /**< trace buffer is full */
int rozofs_trc_last_idx; /**< last entry in the trace buffer */
int rozofs_trc_enabled = 0;  /**< assert to 1 when the trace is enable */
int rozofs_trc_index = 0;
rozofs_trace_t *rozofs_trc_buffer = NULL;  /**< pointer to the trace buffer */
int rozofs_xattr_disable = 0; /**< assert to one to disable xattr for the exported file system */
int rozofs_site_number;  /**< site number for geo-replication */


/**______________________________________________________________________________
*/
/**
*  get the current site number of the rozofsmount client

*/
void rozofs_clear_ientry_write_pending(file_t *f) {
  ientry_t *ie = f->ie;     
  if ((ie != NULL) && (ie->write_pending == f)) {
    ie->write_pending = NULL;
  }
}   
/**______________________________________________________________________________
*/
/**
*  Compute the client hash

  @param h: hostname string
  @param instance: instance id
  
  @retval hash value
*/
static inline uint64_t rozofs_client_hash_compute(char * hostname, int instance) {
    unsigned char *d = (unsigned char *) hostname;
    uint64_t h;

    h = 2166136261U;
    /*
     ** hash on hostname
     */
    for (; *d != 0; d++) {
      h = (h * 16777619)^ *d;
    }
    h = (h * 16777619)^ (instance&0xFF);
    return h;
}     
/*
 *________________________________________________________
 */

/*
 ** API to be called for stopping rozofsmount

 @param none
 @retval none
 */
void rozofs_exit() {
    if (semForEver != NULL) {
        sem_post(semForEver);
        for (;;) {
            info("rozofsmount exit required.");
            sleep(10);
        }
        fatal("semForEver is not initialized.");
    }
}

extern void rozofsmount_profile_program_1(struct svc_req *rqstp, SVCXPRT *ctl_svc);

static void usage() {
    fprintf(stderr, "ROZOFS options:\n");
    fprintf(stderr, "    -H EXPORT_HOST\t\tlist of \'/\' separated addresses (or dns names) where exportd daemon is running (default: rozofsexport) equivalent to '-o exporthost=EXPORT_HOST'\n");
    fprintf(stderr, "    -E EXPORT_NAME\t\tdefine name (or root path) of an export see exportd. Equivalent to '-o exportpath=EXPORT_NAME'\n");
    fprintf(stderr, "    -P EXPORT_PASSWD\t\tdefine passwd used for an export see exportd (default: none) equivalent to '-o exportpasswd=EXPORT_PASSWD'\n");
    fprintf(stderr, "    -o rozofsbufsize=N\t\tdefine size of I/O buffer in KiB (default: 256)\n");
    fprintf(stderr, "    -o rozofsminreadsize=N\tdefine minimum read size on disk in KiB (default: %u)\n", ROZOFS_BSIZE_BYTES(ROZOFS_BSIZE_MIN)/1024);
    fprintf(stderr, "    -o rozofsmaxwritepending=N\tdefine the number of write request(s) that can be sent for an open file from the rozofsmount toward the storcli asynchronously (default: 4)\n");
    fprintf(stderr, "    -o rozofsmaxretry=N\t\tdefine number of retries before I/O error is returned (default: 50)\n");
    fprintf(stderr, "    -o rozofsexporttimeout=N\tdefine timeout (s) for exportd requests (default: %ds)\n",
                            rozofs_tmr_get(TMR_EXPORT_PROGRAM));
    fprintf(stderr, "    -o rozofsstoragetimeout=N\tdefine timeout (s) for IO storaged requests (default: %ds)\n",
                            rozofs_tmr_get(TMR_STORAGE_PROGRAM));
    fprintf(stderr, "    -o rozofsstorclitimeout=N\tdefine timeout (s) for IO storcli requests (default: %ds)\n",
                            rozofs_tmr_get(TMR_STORCLI_PROGRAM));
    fprintf(stderr, "    -o rozofsattrtimeout=N\tdefine timeout (s) for which file/directory attributes are cached (default: %ds)\n",
                            rozofs_tmr_get(TMR_FUSE_ATTR_CACHE));
    fprintf(stderr, "    -o rozofsattrtimeoutms=N\tdefine timeout (ms) for which file/directory attributes are cached (default: %ds)\n",
                            rozofs_tmr_get(TMR_FUSE_ATTR_CACHE_MS));
    fprintf(stderr, "    -o rozofsattrdirtimeoutms=N\tdefine timeout (ms) for which directory attributes are cached (default: %ds)\n",
                            rozofs_tmr_get(TMR_FUSE_ATTR_DIR_CACHE_MS));
    fprintf(stderr, "    -o rozofsentrytimeout=N\tdefine timeout (s) for which name lookups will be cached (default: %ds)\n",
                            rozofs_tmr_get(TMR_FUSE_ENTRY_CACHE));
    fprintf(stderr, "    -o rozofsentrytimeoutms=N\tdefine timeout (ms) for which name lookups will be cached (default: %ds)\n",
                            rozofs_tmr_get(TMR_FUSE_ENTRY_CACHE_MS));
    fprintf(stderr, "    -o rozofsentrydirtimeoutms=N\tdefine timeout (ms) for which directory name lookups will be cached (default: %ds)\n",
                            rozofs_tmr_get(TMR_FUSE_ENTRY_DIR_CACHE_MS));
    fprintf(stderr, "    -o rozofssymlinktimeout=N\tdefine timeout (ms) for which symlink targets will be cached (default: %dms)\n",
                            rozofs_tmr_get(TMR_LINK_CACHE));
    fprintf(stderr, "    -o rozofsenoenttimeout=N\tdefine timeout (ms) for which non existent names will be cached (default: %dms)\n",
                            rozofs_tmr_get(TMR_FUSE_ENOENT_CACHE_MS));

    fprintf(stderr, "    -o debug_port=N\t\tdefine the base debug port for rozofsmount (default: none)\n");
    fprintf(stderr, "    -o instance=N\t\tdefine instance number within [0..127] (default: 0)\n");
    fprintf(stderr, "    -o rozofscachemode=N\tdefine the cache mode: 0: no cache, 1: direct_io, 2: keep_cache (default: 0)\n");
    fprintf(stderr, "    -o rozofsmode=N\t\tdefine the operating mode of rozofsmount: 0: filesystem, 1: block mode (default: 0)\n");
    fprintf(stderr, "    -o rozofsnbstorcli=N\tdefine the number of storcli process(es) to use (default: 1)\n");
    fprintf(stderr, "    -o rozofsshaper=N\t\tdefine the storcli shaper configuration (default: 1)\n");
    fprintf(stderr, "    -o rozofsrotate=N\t\tdefine the modulo on read distribution rotation (default: 0)\n");
    fprintf(stderr, "    -o posixlock\t\tDeprecated.\n");
    fprintf(stderr, "    -o bsdlock\t\t\tDeprecated.\n");
    fprintf(stderr, "    -o nolock\t\t\tTo dectivate BSD as well as POSIX locks.\n");
    fprintf(stderr, "    -o noXattr\t\t\tdisable support of extended attributes\n");
    fprintf(stderr, "    -o no0trunc\t\t\tdisable sending truncate to zero to storages\n");
    fprintf(stderr, "    -o onlyWriter\t\t\tthis client is the only writer of the file it writes\n");
    fprintf(stderr, "    -o site=N\t\t\tsite number for geo-replication purpose (default:0)\n");
    fprintf(stderr, "    -o mojThreadWrite=0|1\t\t\tdisable|enable Mojette threads use for write in storcli\n");
    fprintf(stderr, "    -o mojThreadRead=0|1\t\t\tdisable|enable Mojette threads use for read in storcli\n");
    fprintf(stderr, "    -o mojThreadThreshold=<bytes>\t\t\tset the byte threshold to use Mojette threads in storcli\n");
    fprintf(stderr, "    -o localPreference\t\t\tFavor local storage on read to save network bandwith in case of poor network connection\n");
    fprintf(stderr, "    -o noReadFaultTolerant\t\t\tGive back blocks with 0 on read for corrupted block instead of EIO\n");
    fprintf(stderr, "    -o xattrcache\t\t\tpermits to cache the extended attribute on client(same timer as attributes)\n");
    fprintf(stderr, "    -o asyncsetattr\t\t\toperates asynchronous mode for setattr operations)\n");
    fprintf(stderr, "    -o rozofssparestoragems=N\tdefine timeout for switching to a spare storaged for read/write requests (default: %d ms)\n",
                            rozofs_tmr_get(TMR_PRJ_READ_SPARE));
    fprintf(stderr, "    -o numanode=<#node>\tpin rozofsmount as well as its STORCLI on numa node <node#>\n");
    fprintf(stderr, "    -o wbcache\t\tactivate Linux writeback cache\n");
    fprintf(stderr, "    -o nb_writeThreads=N\tDefine the number of active write threads\n");
    fprintf(stderr, "    -o fuse_profile=N\tDefine the fuse profile to apply(default %u/max %u)\n",(unsigned int)ROZOFS_DEFAULT_FUSE_PROFILE,(unsigned int)(ROZOFS_MAX_PROFILE-1));

}

int rozofs_cache_mode  = 0;  /**< 0: no option on open/create, 1: direct_io; 2: keep_cache */
int rozofs_mode  = 0;  /**< 0:filesystem, 1: block mode */

enum {
    KEY_EXPORT_HOST,
    KEY_EXPORT_PATH,
    KEY_EXPORT_PASSWD,
    KEY_HELP,
    KEY_VERSION,
    KEY_DEBUG_PORT,
};

#define MYFS_OPT(t, p, v) { t, offsetof(struct rozofsmnt_conf, p), v }

static struct fuse_opt rozofs_opts[] = {
    MYFS_OPT("exporthost=%s", host, 0),
    MYFS_OPT("exportpath=%s", export, 0),
    MYFS_OPT("exportpasswd=%s", passwd, 0),
    MYFS_OPT("rozofsbufsize=%u", buf_size, 0),
    MYFS_OPT("rozofsminreadsize=%u", min_read_size, 0),
    MYFS_OPT("rozofsmaxwritepending=%u", max_write_pending, 0),
    MYFS_OPT("nb_writeThreads=%u", nb_writeThreads, 0),
    MYFS_OPT("rozofsnbstorcli=%u", nbstorcli, 0),    
    MYFS_OPT("rozofsmaxretry=%u", max_retry, 0),
    MYFS_OPT("rozofsexporttimeout=%u", export_timeout, 0),
    MYFS_OPT("rozofsstoragetimeout=%u", storage_timeout, 0),
    MYFS_OPT("rozofssparestoragems=%u", spare_timeout, 0),
    MYFS_OPT("rozofsstorclitimeout=%u", storcli_timeout, 0),
    MYFS_OPT("rozofsattrtimeout=%u", attr_timeout, 0),
    MYFS_OPT("rozofsattrtimeoutms=%u", attr_timeout_ms, 0),
    MYFS_OPT("rozofsattrdirtimeoutms=%u", attr_dir_timeout_ms, 0),
    MYFS_OPT("rozofsentrytimeout=%u", entry_timeout, 0),
    MYFS_OPT("rozofsentrytimeoutms=%u", entry_timeout_ms, 0),
    MYFS_OPT("rozofsentrydirtimeoutms=%u", entry_dir_timeout_ms, 0),
    MYFS_OPT("rozofsenoenttimeout=%u", enoent_timeout_ms, 0),
    
    MYFS_OPT("numanode=%u", numanode, 0),   
    MYFS_OPT("debug_port=%u", dbg_port, 0),
    MYFS_OPT("instance=%u", instance, 0),
    MYFS_OPT("rozofscachemode=%u", cache_mode, 0),
    MYFS_OPT("rozofsmode=%u", fs_mode, 0),
    MYFS_OPT("rozofsshaper=%u", shaper, 0),
    MYFS_OPT("rozofsrotate=%u", rotate, 0),
    MYFS_OPT("posixlock", posix_file_lock, 1),
    MYFS_OPT("bsdlock", bsd_file_lock, 1),
    MYFS_OPT("nolock", no_file_lock, 1),
    MYFS_OPT("grpquota", quota, 2),
    MYFS_OPT("noquota", quota, 0),
    MYFS_OPT("quota", quota, 3),
    MYFS_OPT("usrquota", quota, 1),
    MYFS_OPT("noXattr", noXattr, 1),
    MYFS_OPT("localPreference", localPreference, 1), 
    MYFS_OPT("noReadFaultTolerant", noReadFaultTolerant, 1), 
    MYFS_OPT("site=%u", site, 0),
    MYFS_OPT("mojThreadWrite=%u",mojThreadWrite,-1),
    MYFS_OPT("mojThreadRead=%u",mojThreadRead,-1),
    MYFS_OPT("mojThreadThreshold=%u",mojThreadThreshold,-1),
    MYFS_OPT("no0trunc", no0trunc, 1),
    MYFS_OPT("onlyWriter", onlyWriter, 1),
    MYFS_OPT("xattrcache", xattrcache, 0),
    MYFS_OPT("asyncsetattr", asyncsetattr,0),
    MYFS_OPT("wbcache", wbcache,1),
    MYFS_OPT("fuse_profile=%u", idx_fuse_profile,1),
   
    FUSE_OPT_KEY("-H ", KEY_EXPORT_HOST),
    FUSE_OPT_KEY("-E ", KEY_EXPORT_PATH),
    FUSE_OPT_KEY("-P ", KEY_EXPORT_PASSWD),

    FUSE_OPT_KEY("-V", KEY_VERSION),
    FUSE_OPT_KEY("--version", KEY_VERSION),
    FUSE_OPT_KEY("-h", KEY_HELP),
    FUSE_OPT_KEY("--help", KEY_HELP),
    FUSE_OPT_END
};

static int myfs_opt_proc(void *data, const char *arg, int key,
        struct fuse_args *outargs) {
    (void) data;
    switch (key) {
        case FUSE_OPT_KEY_OPT:
            return 1;
        case FUSE_OPT_KEY_NONOPT:
            return 1;
        case KEY_EXPORT_HOST:
            if (conf.host == NULL) {
                conf.host = strdup(arg + 2);
            }
            return 0;
        case KEY_EXPORT_PATH:
            if (conf.export == NULL) {
                conf.export = strdup(arg + 2);
            }
            return 0;
        case KEY_EXPORT_PASSWD:
            if (conf.passwd == NULL) {
                conf.passwd = strdup(arg + 2);
            }
            return 0;
        case KEY_HELP:
            fuse_opt_add_arg(outargs, "-h"); // PRINT FUSE HELP
            fuse_parse_cmdline(outargs, NULL, NULL, NULL);
            fuse_mount(NULL, outargs);
            usage(); // PRINT ROZOFS USAGE
            exit(1);
        case KEY_VERSION:
            fprintf(stderr, "rozofs version %s\n", VERSION);
            fuse_opt_add_arg(outargs, "--version"); // PRINT FUSE VERSION
            fuse_parse_cmdline(outargs, NULL, NULL, NULL);
            exit(0);
    }
    return 1;
}

/**< structure associated to exportd, needed for communication */
exportclt_t exportclt; 

list_t inode_entries;
list_t list_wr_block_head;  /**< pending write block list   */
int list_wr_block_count;   /**< statistics counter of wr_block that has been queued because of exportd loss of connectivity */
htable_t htable_inode;
//htable_t htable_fid;
uint64_t rozofs_ientries_count = 0;

static void rozofs_ll_init(void *userdata, struct fuse_conn_info *conn) {
    int *piped = (int *) userdata;
    char s;
    (void) conn;
    if (piped[1] >= 0) {
        s = 0;
        if (write(piped[1], &s, 1) != 1) {
            warning("rozofs_ll_init: pipe write error: %s", strerror(errno));
        }
        close(piped[1]);
    }
}

void rozofs_ll_forget(fuse_req_t req, fuse_ino_t ino, unsigned long nlookup) {
    ientry_t *ie;
    int trc_idx;

    /*
    ** Update the IO statistics
    */
    rozofs_thr_cnt_update(rozofs_thr_counter[ROZOFSMOUNT_COUNTER_OTHER], 1);

    START_PROFILING(rozofs_ll_forget);
    errno = 0;
    trc_idx = rozofs_trc_req_io(srv_rozofs_ll_forget,ino,NULL,nlookup,0);    


    DEBUG("forget :%lu, nlookup: %lu", ino, nlookup);
    if ((ie = get_ientry_by_inode(ino))) {
        assert(ie->nlookup >= nlookup);
        DEBUG("forget :%lu, ie lookup: %lu", ino, ie->nlookup);
        if ((ie->nlookup -= nlookup) == 0) {
            DEBUG("del entry for %lu", ino);
            del_ientry(ie);
	    ie = NULL;
        }
    }
    rozofs_trc_rsp_attr(srv_rozofs_ll_forget,ino,
                        (ie==NULL)?NULL:ie->attrs.attrs.fid,0,
			(ie==NULL)?0:ie->nlookup,trc_idx);
    STOP_PROFILING(rozofs_ll_forget);
    fuse_reply_none(req);
}


#define DISPLAY_UINT32_CONFIG(field) {\
  if (conf.field==-1) pChar += sprintf(pChar,"%-25s = UNDEFINED\n",#field);\
  else                pChar += sprintf(pChar,"%-25s = %u\n",#field, (unsigned int)conf.field);\
}

#define DISPLAY_INT32_CONFIG(field)   pChar += sprintf(pChar,"%-25s = %d\n",#field, conf.field); 
#define DISPLAY_STRING_CONFIG(field) \
  if (conf.field == NULL) pChar += sprintf(pChar,"%-25s = NULL\n",#field);\
  else                    pChar += sprintf(pChar,"%-25s = %s\n",#field,conf.field); 
  
void show_start_config(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
  
  DISPLAY_STRING_CONFIG(host);
  DISPLAY_STRING_CONFIG(export);
  DISPLAY_STRING_CONFIG(passwd);  
  DISPLAY_UINT32_CONFIG(buf_size);
  DISPLAY_UINT32_CONFIG(min_read_size);
  DISPLAY_UINT32_CONFIG(max_write_pending);
  DISPLAY_UINT32_CONFIG(nb_writeThreads);
  DISPLAY_UINT32_CONFIG(nbstorcli);
  DISPLAY_UINT32_CONFIG(max_retry);
  DISPLAY_UINT32_CONFIG(dbg_port);
  DISPLAY_UINT32_CONFIG(instance);  
  DISPLAY_UINT32_CONFIG(export_timeout);
  DISPLAY_UINT32_CONFIG(storcli_timeout);
  DISPLAY_UINT32_CONFIG(storage_timeout);
  DISPLAY_UINT32_CONFIG(spare_timeout);
  DISPLAY_UINT32_CONFIG(fs_mode); 
  DISPLAY_UINT32_CONFIG(cache_mode);  
  DISPLAY_UINT32_CONFIG(attr_timeout);
  DISPLAY_UINT32_CONFIG(attr_timeout_ms);
  DISPLAY_UINT32_CONFIG(entry_timeout);
  DISPLAY_UINT32_CONFIG(entry_timeout_ms);
  DISPLAY_UINT32_CONFIG(symlink_timeout);
  DISPLAY_UINT32_CONFIG(enoent_timeout_ms);
  DISPLAY_UINT32_CONFIG(numanode);

  DISPLAY_UINT32_CONFIG(shaper);  
  DISPLAY_UINT32_CONFIG(rotate);  
  DISPLAY_UINT32_CONFIG(no_file_lock);  
  DISPLAY_UINT32_CONFIG(noXattr); 
  DISPLAY_UINT32_CONFIG(no0trunc);
  DISPLAY_UINT32_CONFIG(onlyWriter);
  DISPLAY_INT32_CONFIG(site); 
  DISPLAY_INT32_CONFIG(conf_site_file); 
  DISPLAY_UINT32_CONFIG(running_site);  
  DISPLAY_UINT32_CONFIG(mojThreadWrite);  
  DISPLAY_UINT32_CONFIG(mojThreadRead);  
  DISPLAY_UINT32_CONFIG(mojThreadThreshold);  
  DISPLAY_UINT32_CONFIG(localPreference);
  DISPLAY_UINT32_CONFIG(noReadFaultTolerant);
  DISPLAY_UINT32_CONFIG(xattrcache);
  DISPLAY_UINT32_CONFIG(asyncsetattr);
  DISPLAY_UINT32_CONFIG(wbcache);  
  DISPLAY_UINT32_CONFIG(rozofs_bypass_size);
  DISPLAY_UINT32_CONFIG(idx_fuse_profile);

  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
} 
/*__________________________________________________________________________
*/  
void show_layout(char * argv[], uint32_t tcpRef, void *bufRef) {
  rozofs_display_size( uma_dbg_get_buffer(), exportclt.layout, exportclt.bsize);
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());         
} 
/*__________________________________________________________________________
*/  
void man_prj_size(char * pt) {
  pt += sprintf(pt,"This command shows the layout infomation as well as the effective\n");
  pt += sprintf(pt,"projection sizes on disk. Without argument the current layout as well\n");
  pt += sprintf(pt,"as block size is used. An other layout and/or block size can be given\n");
  pt += sprintf(pt,"  prjsize [l <layout>] [b <bsize>]\n");
  pt += sprintf(pt,"bsize is 0 for a 4096 block size or 1 for a 8192 block size.\n");
  
}
void show_prj_size(char * argv[], uint32_t tcpRef, void *bufRef) {
  int idx;
  int bsize;
  int layout;
  
  layout = exportclt.layout;
  bsize  = exportclt.bsize;


  idx = 1;
  while ((argv[idx] != NULL)&&(argv[idx+1] != NULL)) {
       
    if (strcmp(argv[idx],"b")==0) {
      sscanf(argv[idx+1],"%d",&bsize);
      idx += 2;
      continue;
    }
    if (strcmp(argv[idx],"l")==0) {
      sscanf(argv[idx+1],"%d",&layout);
      idx += 2;
      continue;
    }    
  }  
  
  rozofs_display_prjsize( uma_dbg_get_buffer(), layout, bsize);
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());         
} 
/*__________________________________________________________________________
*/  
static char * show_rotate_modulo_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"rotateModulo set <value> : set new rotate modulo value\n");
  pChar += sprintf(pChar,"rotateModulo             : display rotate modulo value\n");  
  return pChar; 
}
void show_rotate_modulo(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
  int   new_val;
   
   if (argv[1] !=NULL)
   {
    if (strcmp(argv[1],"set")==0) {
      errno = 0;       
      new_val = (int) strtol(argv[2], (char **) NULL, 10);   
      if (errno != 0) {
        pChar += sprintf(pChar, "bad value %s\n",argv[2]);
	pChar = show_rotate_modulo_help(pChar);
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;     
      } 
      rozofs_rotation_read_modulo = new_val;        
      uma_dbg_send_format(tcpRef, bufRef, TRUE, "New rotate modulo set to %d\n",rozofs_rotation_read_modulo);    
      return;     
   }
    /*
    ** Help
    */
    pChar = show_rotate_modulo_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;   
  }
  uma_dbg_send_format(tcpRef, bufRef, TRUE, "rotation modulo is %d\n",rozofs_rotation_read_modulo);    
  return;     
}   
/*__________________________________________________________________________
*/
void reset_lock_stat(void);
char * display_lock_stat(char * p);
/*__________________________________________________________________________
*/
static char * show_flock_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"flock reset       : reset statistics\n");
  pChar += sprintf(pChar,"flock             : display statistics\n");  
  return pChar; 
}
void show_flock(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
       
  if (argv[1] != NULL) {

    if (strcmp(argv[1],"reset")== 0) {
      char * p = display_lock_stat(uma_dbg_get_buffer());
      reset_lock_stat();
      p += sprintf(p,"Reset done\n");
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());          
      return;
    }

    pChar = show_flock_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;
  }
  display_lock_stat(uma_dbg_get_buffer());
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());      
}
  
/*__________________________________________________________________________
*/  
static char * show_ientry_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"ientry count         : display ientry count\n");
  pChar += sprintf(pChar,"ientry coll          : display ientry collisions\n");
  pChar += sprintf(pChar,"ientry fid <fid>     : display ientry by FID\n");  
  pChar += sprintf(pChar,"ientry inode <inode> : display ientry by inode\n");  
  pChar += sprintf(pChar,"ientry nb <nb> : display ientry number <nb> in list\n");  
  return pChar; 
}
void show_ientry(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
  fid_t fid;
  long long unsigned int   inode;
  ientry_t * ie = NULL;
  char fid_str[64];
  rozofs_inode_t * pInode;
  rozofs_mover_children_t  * mover_p;
  int i;
  
  if (argv[1] == NULL) {
      pChar = show_ientry_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;
  } 
  if (strcmp(argv[1],"coll")==0) {
      pChar += sprintf(pChar, "ientry collisions: %llu\n", (long long unsigned int) hash_inode_collisions_count);
      pChar += sprintf(pChar, "ientry max colls : %llu\n", (long long unsigned int) hash_inode_max_collisions);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer()); 
      hash_inode_collisions_count = 0;  
      hash_inode_max_collisions = 0;
      return;
  }
  if (strcmp(argv[1],"count")==0) {
      pChar += sprintf(pChar, "ientry counter: %llu\n", (long long unsigned int) rozofs_ientries_count);
      pChar += sprintf(pChar, "wr_blk retries: %d\n",  list_wr_block_count);
      pChar += sprintf(pChar, "large size: %u\n",  (unsigned int) ROZOFS_IENTRY_LARGE_SZ);
      pChar += sprintf(pChar, "ientry_size    : %u\n",  (unsigned int)sizeof(ientry_t));
      pChar += sprintf(pChar, "mattr_sz       : %u\n",  (unsigned int)sizeof(mattr_t));
      pChar += sprintf(pChar, "internal_ino_sz: %u\n",  (unsigned int)sizeof(struct inode_internal_t));
      pChar += sprintf(pChar, "lv2_entry_sz   : %u\n",  (unsigned int)sizeof(lv2_entry_t));
      {
        mattr_t  fake_inode;
	pChar += sprintf(pChar,"fid      =%u\n",(unsigned int)((void*)&fake_inode.fid - (void*)&fake_inode)); 
	pChar += sprintf(pChar,"sid      =%u\n",(unsigned int)( (void*)&fake_inode.sids[0] - (void*)&fake_inode)); 
	pChar += sprintf(pChar,"cid      =%u\n",(unsigned int)( (void*)&fake_inode.cid - (void*)&fake_inode)); 
	pChar += sprintf(pChar,"mode     =%u\n",(unsigned int)( (void*)&fake_inode.mode - (void*)&fake_inode));
	pChar += sprintf(pChar,"uid      =%u\n",(unsigned int)( (void*)&fake_inode.uid - (void*)&fake_inode));
	pChar += sprintf(pChar,"gid      =%u\n",(unsigned int)( (void*)&fake_inode.gid - (void*)&fake_inode));
	pChar += sprintf(pChar,"nlink    =%u\n",(unsigned int)( (void*)&fake_inode.nlink - (void*)&fake_inode));
	pChar += sprintf(pChar,"ctime    =%u\n",(unsigned int)( (void*)&fake_inode.ctime - (void*)&fake_inode)); 
	pChar += sprintf(pChar,"atime    =%u\n",(unsigned int)( (void*)&fake_inode.atime - (void*)&fake_inode));
	pChar += sprintf(pChar,"mtime    =%u\n",(unsigned int)( (void*)&fake_inode.mtime - (void*)&fake_inode)); 
	pChar += sprintf(pChar,"size     =%u\n",(unsigned int)( (void*)&fake_inode.size - (void*)&fake_inode));
	pChar += sprintf(pChar,"children =%u\n",(unsigned int)((void*)&fake_inode.children - (void*)&fake_inode));       
      
      }
      {
          ext_mattr_t fake_inode;
	  pChar += sprintf(pChar,"attrs          =%u\n",    (unsigned int)((void*) &fake_inode.s.cr8time - (void*) &fake_inode)); 
	  pChar += sprintf(pChar,"pfid           =%u\n",    (unsigned int)((void*) &fake_inode.s.pfid - (void*) &fake_inode)); 
	  pChar += sprintf(pChar,"hash1          =%u\n",    (unsigned int)( (void*) &fake_inode.s.hash1 - (void*) &fake_inode));
	  pChar += sprintf(pChar,"hash2          =%u\n",    (unsigned int)((void*) &fake_inode.s.hash2 - (void*) &fake_inode));
	  pChar += sprintf(pChar,"i_extra_isize  =%u\n",    (unsigned int)((void*) &fake_inode.s.i_extra_isize - (void*) &fake_inode));
	  pChar += sprintf(pChar,"bitfield1      =%u\n",    (unsigned int)((void*) &fake_inode.s.bitfield1 - (void*) &fake_inode));
	  pChar += sprintf(pChar,"filler2        =%u\n",    (unsigned int)((void*) &fake_inode.s.filler2 - (void*) &fake_inode)); 
	  pChar += sprintf(pChar,"filler3        =%u\n",    (unsigned int)((void*) &fake_inode.s.filler3 - (void*) &fake_inode));
	  pChar += sprintf(pChar,"filler4        =%u\n",    (unsigned int)((void*) &fake_inode.s.filler4 - (void*) &fake_inode)); 
	  pChar += sprintf(pChar,"filler5        =%u\n",    (unsigned int)((void*) &fake_inode.s.filler5 - (void*) &fake_inode)); 
	  pChar += sprintf(pChar,"filler6        =%u\n",    (unsigned int)((void*) &fake_inode.s.filler6 - (void*) &fake_inode)); 
	  pChar += sprintf(pChar,"i_file_acl     =%u\n",    (unsigned int)((void*) &fake_inode.s.i_file_acl - (void*) &fake_inode));
	  pChar += sprintf(pChar,"i_link_name    =%u\n",    (unsigned int)((void*) &fake_inode.s.i_link_name - (void*) &fake_inode)); 
	  pChar += sprintf(pChar,"i_link_name    =%u\n",    (unsigned int)((void*) &fake_inode.s.i_link_name - (void*) &fake_inode)); 
	  pChar += sprintf(pChar,"hpc_reserved   =%u\n",    (unsigned int)((void*) &fake_inode.s.hpc_reserved - (void*) &fake_inode)); 
	  pChar += sprintf(pChar,"fname          =%u\n",    (unsigned int)((void*) &fake_inode.s.fname - (void*) &fake_inode));       
      
	  pChar += sprintf(pChar,"fname_sz       =%u\n", (unsigned int)    sizeof(rozofs_inode_fname_t));       
      
      
      }
      
      

      
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;
  }
  
  if (argv[2] == NULL) {
      pChar = show_ientry_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;
  } 
  
  if (strcmp(argv[1],"fid")==0) {
      if (rozofs_uuid_parse(argv[2],fid)==-1) {
          pChar += sprintf(pChar, "this is not a valid FID\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;	        
      }
      ie = get_ientry_by_fid(fid);
      if (ie == NULL) {
          pChar += sprintf(pChar, "No ientry for this FID\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;        
      }
  }        
  else if (strcmp(argv[1],"inode")==0){
      int ret = sscanf(argv[2],"%llu",&inode);
      if (ret != 1) {
          pChar += sprintf(pChar, "this is not a valid inode\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;	        
      }
      ie = get_ientry_by_inode(inode);
      if (ie == NULL) {
          pChar += sprintf(pChar, "No ientry for this inode\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;        
      }      
  } 
  else if (strcmp(argv[1],"nb")==0){
      int ret = sscanf(argv[2],"%llu",&inode);
      if (ret != 1) {
          pChar += sprintf(pChar, "this is not a valid number\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;	        
      }
      if (inode>rozofs_ientries_count) {
          pChar += sprintf(pChar, "this is not a valid number\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;	        
      }
      list_t * p;
      ie = NULL;
      list_for_each_forward(p, &inode_entries) {
        inode--;
	if (inode==0) {
	  ie = list_entry(p, ientry_t, list);
	  break;
	}    
      }	
      if (ie == NULL) {
          pChar += sprintf(pChar, "No ientry for this number\n");
          uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
          return;        
      }      
  }   
  else {
      pChar += sprintf(pChar, "ientry counter: %llu\n", (long long unsigned int) rozofs_ientries_count);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;    
  }
  
  rozofs_uuid_unparse(ie->fid,fid_str);
  pInode = (rozofs_inode_t *)ie->fid;

  if (ie->attrs.fname.name_type == ROZOFS_FNAME_TYPE_DIRECT) {
    pChar += sprintf(pChar, "%-15s : ", "fname");
    for (i=0; i<ie->attrs.fname.len; i++) *pChar++ =  ie->attrs.fname.name[i];
    *pChar++ = '\n';
  }        
  pChar += sprintf(pChar, "%-15s : %llu\n", "inode", (long long unsigned int)ie->inode);
  pChar += sprintf(pChar, "%-15s : %s\n", "fid", fid_str);
  rozofs_uuid_unparse(ie->pfid,fid_str);  
  pChar += sprintf(pChar, "%-15s : %s\n", "pfid", fid_str);
  if (S_ISREG(ie->attrs.attrs.mode)) {
    pChar += sprintf(pChar, "%-15s : eid %d ./reg_attr/%d/trk_%llu index %d\n", "regular", 
                     pInode->s.eid,
		     pInode->s.usr_id,
		     (long long unsigned int)pInode->s.file_id,
		     pInode->s.idx);
    pChar += sprintf(pChar, "%-15s : %d\n", "attrs cid", ie->attrs.attrs.cid);
    pChar += sprintf(pChar, "%-15s : ", "attrs sid");
    {
      int i;
      pChar += sprintf(pChar, "%d", ie->attrs.attrs.sids[0]);
      for (i=1;i<ROZOFS_SAFE_MAX;i++) {
	 pChar += sprintf(pChar, "-%d", ie->attrs.attrs.sids[i]);
      }
      pChar += sprintf(pChar,"\n");
    }		     
    pChar += sprintf(pChar, "%-15s : %d\n", "nb blocks", ie->attrs.hpc_reserved.reg.nb_blocks_thin);    
    mover_p = (rozofs_mover_children_t *) &ie->attrs.attrs.children;
    pChar += sprintf(pChar, "%-15s : %d/%d\n", "mover/primary", mover_p->fid_st_idx.mover_idx, mover_p->fid_st_idx.primary_idx);          
  }
  else if (S_ISDIR(ie->attrs.attrs.mode)) {
    pChar += sprintf(pChar, "%-15s : eid %d ./dir_attr/%d/trk_%llu index %d\n", "directory", 
                     pInode->s.eid,
		     pInode->s.usr_id,
		     (long long unsigned int)pInode->s.file_id,
		     pInode->s.idx);    
    pChar += sprintf(pChar, "%-15s : %llu\n", "deleted files", (long long unsigned int)ie->attrs.hpc_reserved.dir.nb_deleted_files);    
    pChar += sprintf(pChar, "%-15s : %d\n", "attrs children", ie->attrs.attrs.children);  
  }
  else if (S_ISLNK(ie->attrs.attrs.mode)) {
    pChar += sprintf(pChar, "%-15s : eid %d ./reg_attr/%d/trk_%llu index %d\n", "symlink", 
                     pInode->s.eid,
		     pInode->s.usr_id,
		     (long long unsigned int)pInode->s.file_id,
		     pInode->s.idx);   
  }
  pChar += sprintf(pChar, "%-15s : %d\n", "nlookup", (int) ie->nlookup);
  pChar += sprintf(pChar, "%-15s : %llu\n", "consistency", (long long unsigned int) ie->read_consistency);
  pChar += sprintf(pChar, "%-15s : %llu\n", "timestamp", (long long unsigned int)ie->timestamp);
  pChar += sprintf(pChar, "%-15s : %s\n", "write_pending", ie->write_pending?"yes":"no"); 
  pChar += sprintf(pChar, "%-15s : %s\n", "extend_pending", ie->file_extend_pending?"yes":"no"); 
  pChar += sprintf(pChar, "%-15s : %lld\n", "pending size", (long long int)ie->file_extend_size); 
  pChar += sprintf(pChar, "%-15s : %s\n", "extend_running", ie->file_extend_running?"yes":"no"); 
  if (ie->symlink_target) {
    pChar += sprintf(pChar, "%-15s : %s\n", "symlink", ie->symlink_target);  
  }
  pChar += sprintf(pChar, "%-15s : ","cr8time");  
  pChar +=strftime(pChar, 20, "%Y-%m-%d %H:%M:%S", localtime((time_t*)&ie->attrs.cr8time));
  pChar += sprintf(pChar, "\n%-15s : ","attrs ctime");  
  pChar +=strftime(pChar, 20, "%Y-%m-%d %H:%M:%S", localtime((time_t*)&ie->attrs.attrs.ctime));
  pChar += sprintf(pChar, "\n%-15s : ", "attrs atime");
  pChar +=strftime(pChar, 20, "%Y-%m-%d %H:%M:%S", localtime((time_t*)&ie->attrs.attrs.atime));  
  pChar += sprintf(pChar, "\n%-15s : ", "attrs mtime");
  pChar +=strftime(pChar, 20, "%Y-%m-%d %H:%M:%S", localtime((time_t*)&ie->attrs.attrs.mtime));
  pChar += sprintf(pChar, "\n%-15s : %d\n", "attrs nlink", ie->attrs.attrs.nlink);  
  pChar += sprintf(pChar, "%-15s : 0x%x\n", "attrs mode", ie->attrs.attrs.mode);  
  pChar += sprintf(pChar, "%-15s : %d\n", "attrs gid", ie->attrs.attrs.gid);  
  pChar += sprintf(pChar, "%-15s : %d\n", "attrs uid", ie->attrs.attrs.uid);  
  pChar += sprintf(pChar, "%-15s : %llu\n", "attrs size", (long long unsigned int)ie->attrs.attrs.size);

  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
  return;
  
}  
/*__________________________________________________________________________
*/  
#define SHOW_PROFILER_PROBE(probe) pChar += sprintf(pChar," %-12s | %15"PRIu64" | %9"PRIu64" | %18"PRIu64" | %15s |\n",\
                    #probe,\
                    gprofiler->rozofs_ll_##probe[P_COUNT],\
                    gprofiler->rozofs_ll_##probe[P_COUNT]?gprofiler->rozofs_ll_##probe[P_ELAPSE]/gprofiler->rozofs_ll_##probe[P_COUNT]:0,\
                    gprofiler->rozofs_ll_##probe[P_ELAPSE]," " );

#define SHOW_PROFILER_PROBE_BYTE(probe) pChar += sprintf(pChar," %-12s | %15"PRIu64" | %9"PRIu64" | %18"PRIu64" | %15"PRIu64" |\n",\
                    #probe,\
                    gprofiler->rozofs_ll_##probe[P_COUNT],\
                    gprofiler->rozofs_ll_##probe[P_COUNT]?gprofiler->rozofs_ll_##probe[P_ELAPSE]/gprofiler->rozofs_ll_##probe[P_COUNT]:0,\
                    gprofiler->rozofs_ll_##probe[P_ELAPSE],\
                    gprofiler->rozofs_ll_##probe[P_BYTES]);


#define RESET_PROFILER_PROBE(probe) \
{ \
         gprofiler->rozofs_ll_##probe[P_COUNT] = 0;\
         gprofiler->rozofs_ll_##probe[P_ELAPSE] = 0; \
}

#define RESET_PROFILER_PROBE_BYTE(probe) \
{ \
   RESET_PROFILER_PROBE(probe);\
   gprofiler->rozofs_ll_##probe[P_BYTES] = 0; \
}
static char * show_profiler_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"profiler reset       : reset statistics\n");
  pChar += sprintf(pChar,"profiler             : display statistics\n");  
  return pChar; 
}
void show_profiler(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();

    time_t elapse;
    int days, hours, mins, secs;
    time_t  this_time = time(0);    
    
    elapse = (int) (this_time - gprofiler->uptime);
    days = (int) (elapse / 86400);
    hours = (int) ((elapse / 3600) - (days * 24));
    mins = (int) ((elapse / 60) - (days * 1440) - (hours * 60));
    secs = (int) (elapse % 60);


    pChar += sprintf(pChar, "GPROFILER version %s uptime =  %d days, %2.2d:%2.2d:%2.2d\n", gprofiler->vers,days, hours, mins, secs);
    pChar += sprintf(pChar, " - ientry counter: %llu\n", (long long unsigned int) rozofs_ientries_count);
    pChar += sprintf(pChar, " - opened file   : %llu\n", (long long unsigned int) rozofs_opened_file);
    pChar += sprintf(pChar, "   procedure  |     count       |  time(us) | cumulated time(us) |     bytes       |\n");
    pChar += sprintf(pChar, "--------------+-----------------+-----------+--------------------+-----------------+\n");
    SHOW_PROFILER_PROBE(lookup);
    SHOW_PROFILER_PROBE(lookup_agg);
    SHOW_PROFILER_PROBE(forget);
    SHOW_PROFILER_PROBE(getattr);
    SHOW_PROFILER_PROBE(setattr);
    SHOW_PROFILER_PROBE(readlink);
    SHOW_PROFILER_PROBE(mknod);
    SHOW_PROFILER_PROBE(mkdir);
    SHOW_PROFILER_PROBE(unlink);
    SHOW_PROFILER_PROBE(rmdir);
    SHOW_PROFILER_PROBE(symlink);
    SHOW_PROFILER_PROBE(rename);
    SHOW_PROFILER_PROBE(open);
    SHOW_PROFILER_PROBE(link);
    SHOW_PROFILER_PROBE_BYTE(read);
    SHOW_PROFILER_PROBE_BYTE(write);
    SHOW_PROFILER_PROBE(flush);
    SHOW_PROFILER_PROBE(release);
    SHOW_PROFILER_PROBE(opendir);
    SHOW_PROFILER_PROBE(readdir);
    SHOW_PROFILER_PROBE(releasedir);
    SHOW_PROFILER_PROBE(fsyncdir);
    SHOW_PROFILER_PROBE(statfs);
    SHOW_PROFILER_PROBE(setxattr);
    SHOW_PROFILER_PROBE(getxattr);
    SHOW_PROFILER_PROBE(listxattr);
    SHOW_PROFILER_PROBE(removexattr);
    SHOW_PROFILER_PROBE(access);
    SHOW_PROFILER_PROBE(create);
    SHOW_PROFILER_PROBE(getlk);
    SHOW_PROFILER_PROBE(setlk);
    SHOW_PROFILER_PROBE(setlk_int);
    SHOW_PROFILER_PROBE(clearlkowner);
    SHOW_PROFILER_PROBE(ioctl);
    
    if (argv[1] != NULL)
    {
      if (strcmp(argv[1],"reset")==0) {
	RESET_PROFILER_PROBE(lookup);
	RESET_PROFILER_PROBE(lookup_agg);
	RESET_PROFILER_PROBE(forget);
	RESET_PROFILER_PROBE(getattr);
	RESET_PROFILER_PROBE(setattr);
	RESET_PROFILER_PROBE(readlink);
	RESET_PROFILER_PROBE(mknod);
	RESET_PROFILER_PROBE(mkdir);
	RESET_PROFILER_PROBE(unlink);
	RESET_PROFILER_PROBE(rmdir);
	RESET_PROFILER_PROBE(symlink);
	RESET_PROFILER_PROBE(rename);
	RESET_PROFILER_PROBE(open);
	RESET_PROFILER_PROBE(link);
	RESET_PROFILER_PROBE_BYTE(read);
	RESET_PROFILER_PROBE_BYTE(write);
	RESET_PROFILER_PROBE(flush);
	RESET_PROFILER_PROBE(release);
	RESET_PROFILER_PROBE(opendir);
	RESET_PROFILER_PROBE(readdir);
	RESET_PROFILER_PROBE(releasedir);
	RESET_PROFILER_PROBE(fsyncdir);
	RESET_PROFILER_PROBE(statfs);
	RESET_PROFILER_PROBE(setxattr);
	RESET_PROFILER_PROBE(getxattr);
	RESET_PROFILER_PROBE(listxattr);
	RESET_PROFILER_PROBE(removexattr);
	RESET_PROFILER_PROBE(access);
	RESET_PROFILER_PROBE(create);
	RESET_PROFILER_PROBE(getlk);
	RESET_PROFILER_PROBE(setlk);
	RESET_PROFILER_PROBE(setlk_int);
	RESET_PROFILER_PROBE(clearlkowner);      
	RESET_PROFILER_PROBE(ioctl);
	pChar += sprintf(pChar,"Reset Done\n");  
	gprofiler->uptime = this_time;   
      }
      else {
	/*
	** Help
	*/
	pChar = show_profiler_help(pChar);
      }
    }    
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}
/*__________________________________________________________________________
*/
static char * rozofs_set_cache_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"cache_set {0|1|2}\n");
  pChar += sprintf(pChar,"   0: no option on open/create\n");
  pChar += sprintf(pChar,"   1: direct_io\n");
  pChar += sprintf(pChar,"   2: keep_cache\n");
  return pChar; 
}
void rozofs_set_cache(char * argv[], uint32_t tcpRef, void *bufRef) 
{
   char *pChar = uma_dbg_get_buffer();
   int cache_mode;
   
   if (argv[1] ==NULL)
   {
    pChar = rozofs_set_cache_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;
   }
   errno = 0;
   cache_mode = (int) strtol(argv[1], (char **) NULL, 10);   
   if (errno != 0) {
      pChar += sprintf(pChar, "bad value %s\n",argv[1]);
      pChar = rozofs_set_cache_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;     
   } 
   if (cache_mode > 2)
   {
      pChar += sprintf(pChar, "bad value %s\n",argv[1]);
      pChar = rozofs_set_cache_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;     
   } 
   rozofs_cache_mode = cache_mode;
   uma_dbg_send(tcpRef, bufRef, TRUE, "Success\n");
}

/*__________________________________________________________________________
*/

void rozofs_disable_xattr(char * argv[], uint32_t tcpRef, void *bufRef) 
{

   if ((argv[1] != NULL)&&(strcmp(argv[1],"disable")==0)) { 
     rozofs_xattr_disable = 1;
     uma_dbg_send(tcpRef, bufRef, TRUE, "Extended Attributes are now disabled\n");
     return;
   }  
   uma_dbg_send(tcpRef, bufRef, TRUE, "To disable extended attributes enter \"xattr disable\"\n");
   
}

/*__________________________________________________________________________
*/
static char * rozofs_set_fsmode_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"fs_mode set {fs|block}   : set FS mode\n");
  return pChar; 
}
void rozofs_set_fsmode(char * argv[], uint32_t tcpRef, void *bufRef) 
{
  char * pChar = uma_dbg_get_buffer();
   
   if (argv[1] ==NULL)
   {
    pChar = rozofs_set_fsmode_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;
   }
   if      (strcmp(argv[1],"fs")==0)    rozofs_mode = 0;
   else if (strcmp(argv[1],"block")==0) rozofs_mode = 1;
   else {
      pChar += sprintf(pChar, "bad value %s\n",argv[1]);
      pChar = rozofs_set_fsmode_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;       
   }
   uma_dbg_send(tcpRef, bufRef, TRUE, "Success\n");
}
/*__________________________________________________________________________
*/

void show_exp_routing_table(char * argv[], uint32_t tcpRef, void *bufRef) {

    char *pChar = uma_dbg_get_buffer();
    
    expgw_display_all_exportd_routing_table(pChar);

    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}

/*__________________________________________________________________________
*/
void show_eid_exportd_assoc(char * argv[], uint32_t tcpRef, void *bufRef) {

    char *pChar = uma_dbg_get_buffer();
    
    expgw_display_all_eid(pChar);

    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}
/*__________________________________________________________________________
*/
void show_mount(char * argv[], uint32_t tcpRef, void *bufRef) {

    char *pChar = uma_dbg_get_buffer();
    
    pChar += sprintf(pChar, "instance   : %d\n",conf.instance);
    pChar += sprintf(pChar, "mount      : %s\n",rozofs_mountpoint);
    pChar += sprintf(pChar, "export     : %s\n",conf.export);
    pChar += sprintf(pChar, "host       : %s\n",conf.host);
    pChar += sprintf(pChar, "eid        : %d\n",exportclt.eid);
    pChar += sprintf(pChar, "multi site : %s\n",rozofs_get_msite()?"Yes":"No");
    pChar += sprintf(pChar, "thin prov. : %s\n",rozofs_get_thin_provisioning()?"Yes":"No");
    pChar += sprintf(pChar, "local site : %d\n",rozofs_site_number);	
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}
/*__________________________________________________________________________
*/
static char * show_blockmode_cache_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"blockmode_cache reset       : reset statistics\n");
  pChar += sprintf(pChar,"blockmode_cache flush       : flush block mode cache\n");  
  pChar += sprintf(pChar,"blockmode_cache enable      : enable block mode cache\n");  
  pChar += sprintf(pChar,"blockmode_cache disable     : disable block mode cache\n");  
  pChar += sprintf(pChar,"blockmode_cache             : display statistics\n");  
  return pChar; 
}  
void show_blockmode_cache(char * argv[], uint32_t tcpRef, void *bufRef) {


    char *pChar = uma_dbg_get_buffer();
    
    if (argv[1] != NULL)
    {
        if (strcmp(argv[1],"reset")==0) {
	  rozofs_mbcache_stats_clear();
	  uma_dbg_send(tcpRef, bufRef, TRUE, "Reset Done\n");    
	  return;
	}
        if (strcmp(argv[1],"flush")==0) {
	  rozofs_gcache_flush();
	  rozofs_mbcache_stats_clear();
	  uma_dbg_send(tcpRef, bufRef, TRUE, "Flush Done\n");    
	  return;
        }
        if (strcmp(argv[1],"enable")==0) {
	  if (rozofs_mbcache_enable_flag != ROZOFS_MBCACHE_ENABLE)
	  {
            rozofs_mbcache_enable();
            rozofs_mbcache_stats_clear();
            uma_dbg_send(tcpRef, bufRef, TRUE, "Mode Block cache is now enabled\n");    
	  }
	  else
	  {
            uma_dbg_send(tcpRef, bufRef, TRUE, "Mode Block cache is already enabled\n");    
	  }
	}  
        if (strcmp(argv[1],"disable")==0) {
	  if (rozofs_mbcache_enable_flag != ROZOFS_MBCACHE_DISABLE)
	  {
            rozofs_mbcache_stats_clear();
            rozofs_mbcache_disable();
            uma_dbg_send(tcpRef, bufRef, TRUE, "Mode Block cache is now disabled\n");    
	  }
	  else
	  {
            uma_dbg_send(tcpRef, bufRef, TRUE, "Mode Block cache is already disabled\n");    
	  }
	  return;
        }
	pChar = show_blockmode_cache_help(pChar);
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	return;   	
    } 
    pChar +=sprintf(pChar,"cache state : %s\n", (rozofs_mbcache_enable_flag== ROZOFS_MBCACHE_ENABLE)?"Enabled":"Disabled"); 
    
    pChar = com_cache_show_cache_stats(pChar,rozofs_mbcache_cache_p,"Block mode cache");
    rozofs_mbcache_stats_display(pChar);

    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}
/*
**__________________________________________________________________________

    F U S E   T R A C E   S E R V I C E S
**__________________________________________________________________________
*/    

/*__________________________________________________________________________
*/
/**
*   enable the trace of the fuse service

    that trace can be enabled only if there is a valid allocated trace buffer
    
    @param none
    @retval none
*/
void rozofs_trace_enable()
{
   if (rozofs_trc_buffer!= NULL) rozofs_trc_enabled = 1;
}

/*__________________________________________________________________________
*/
/**
*   Reset the fuse trace buffer
   
    THat service consist in turning off the enable flag of the trace buffer
    
    @param none
    @retval none
*/
void rozofs_trace_reset()
{
   rozofs_trc_index = 0;
   rozofs_trc_wr_idx = 0;
   rozofs_trc_buf_full = 0;
}
/*__________________________________________________________________________
*/
/**
*   disable the trace of the fuse service
    
    @param none
    @retval none
*/
void rozofs_trace_disable()
{
   rozofs_trc_enabled = 0;

}
/*__________________________________________________________________________
*/
/**
*   init of the fuse trace service

    THe service allocates a default trace buffer and turn off the trace.
    @param none
    @retval 0 on success
    @retval -1 on error
*/
int rozofs_trace_init(int size)
{
   rozofs_trace_reset();  
   rozofs_trc_last_idx = size;
   rozofs_trc_buffer = malloc(sizeof(rozofs_trace_t)*size);
   if (rozofs_trc_buffer != NULL) 
   {  
     memset(rozofs_trc_buffer,0,sizeof(rozofs_trace_t)*size);
     return 0;
   }
   return -1;
}
/*__________________________________________________________________________
*/
/**
*   Show the status od the fuse trace service

    @param pChar : pointer to the result buffer
    @retval none
*/
void rozofs_trace_status(char *pChar)
{
   pChar+=sprintf(pChar,"trace status      :%s \n",(rozofs_trc_enabled==0)?"Disabled":"Enabled");
   pChar+=sprintf(pChar,"trace buffer size :%d entries \n",(rozofs_trc_buffer==NULL)?0:rozofs_trc_last_idx);
   if (rozofs_trc_enabled == 0) return;
   if ((rozofs_trc_wr_idx == 0) && (rozofs_trc_buf_full== 0)) 
   {
      pChar+=sprintf(pChar,"Buffer empty\n");
      return ;
   }
   if (rozofs_trc_buf_full!= 0) 
   {
      pChar+=sprintf(pChar,"Buffer full\n");
      return ;   
   }
   pChar+=sprintf(pChar,"Buffer contains %d entries\n",rozofs_trc_wr_idx);
}
/*__________________________________________________________________________
*/
/**
*   return the ascii pattern associated with the fuse service number

    @param srv : fuse service number
    @retval pChar : pointer to the result buffer
*/
char *trc_fuse_display_srv(int srv)
{
  switch (srv) {
	case srv_rozofs_ll_lookup:return "lookup";
        case srv_rozofs_ll_forget:return "forget";

	case srv_rozofs_ll_getattr:return "getattr";

	case srv_rozofs_ll_setattr:return "setattr";

	case srv_rozofs_ll_readlink:return "readlink";

	case srv_rozofs_ll_mknod:return "mknod";

	case srv_rozofs_ll_mkdir:return "mkdir";

	case srv_rozofs_ll_unlink:return "unlink";

	case srv_rozofs_ll_rmdir:return "rmdir";

	case srv_rozofs_ll_symlink:return "symlink";

	case srv_rozofs_ll_rename:return "rename";

	case srv_rozofs_ll_open:return "open";

	case srv_rozofs_ll_link:return "link";

	case srv_rozofs_ll_read:return "read";

	case srv_rozofs_ll_write:return "write";

	case srv_rozofs_ll_flush:return "flush";

	case srv_rozofs_ll_release:return "release";

	case srv_rozofs_ll_opendir:return "opendir";

	case srv_rozofs_ll_readdir:return "readdir";

	case srv_rozofs_ll_releasedir:return "releasedir";

	case srv_rozofs_ll_fsyncdir:return "fsyncdir";

	case srv_rozofs_ll_statfs:return "statfs";

	case srv_rozofs_ll_setxattr:return "setxattr";

	case srv_rozofs_ll_getxattr:return "getxattr";

	case srv_rozofs_ll_listxattr:return "listxattr";

	case srv_rozofs_ll_removexattr:return "removexattr";

	case srv_rozofs_ll_access:return "access";

	case srv_rozofs_ll_create:return "create";

	case srv_rozofs_ll_getlk:return "getlk";

	case srv_rozofs_ll_setlk:return "setlk";

	case srv_rozofs_ll_setlk_int:return "setlk_int";

	case srv_rozofs_ll_ioctl:return "wr_block";

	case srv_rozofs_ll_clearlkowner:return "clearlkowner";
	case srv_rozofs_ll_truncate:return "truncate";
	
	default: return "??unknown??";
    }
}

/*__________________________________________________________________________
*/
/**
*   Display of the content of the fuse trace buffer

    @param pChar : pointer to the result buffer
    @retval none
*/
void show_trc_fuse_buffer(uint32_t tcpRef, void *bufRef)
{
   char str[37];
   fid_t fake_fid;
   int start, count;
   uint64_t cur_ts;
   char * mode;
   char * pChar;
   char * pHead;
   int    max_size;
   void * nextBuff = NULL;
   
   /* 
   ** Retrieve the buffer payload 
   */
   if ((pHead = (char *)ruc_buf_getPayload(bufRef)) == NULL) {
     severe( "ruc_buf_getPayload(%p)", bufRef );
     /* Let's tell the caller fsm that the message is sent */
     return;
   }  
   max_size = ruc_buf_getMaxPayloadLen(bufRef)-1024;
  
   
   memset(fake_fid,0,sizeof(fid_t));
   rozofs_trace_t *p ;
   int i;
   if (rozofs_trc_buf_full) 
   {
     start = rozofs_trc_wr_idx;
     count = rozofs_trc_last_idx;
   }
   else  
   {
     start = 0;
     count = rozofs_trc_wr_idx;
   }
   p = &rozofs_trc_buffer[start];
   cur_ts = p->ts;

   /*
   ** Set the command recell string
   */
   pChar = uma_dbg_cmd_recall((UMA_MSGHEADER_S *)pHead);   
   pChar+=sprintf(pChar,"trace entry size : %lu Bytes\n",(long unsigned int)sizeof(rozofs_trace_t));
   pChar+=sprintf(pChar,"trace nb entry   : %lu\n",(long unsigned int)rozofs_trc_last_idx);
   pChar+=sprintf(pChar,"ino size         : %lu Bytes\n",(long unsigned int)sizeof(fuse_ino_t));

   
   for (i = 0; i < count; i++,start++)
   {
    
       /*
       ** Flush current buffer when almost full
       */
       if ((pChar - pHead) >= max_size) {
         *pChar = 0;
         
         /*
         ** Allocate a next buffer
         */
         nextBuff = uma_dbg_get_new_buffer(tcpRef);
         if (nextBuff==NULL) {
           pChar += sprintf(pChar,"\n\n!!! Buffer depletion !!!\n");
           break;
         }
         /*
         ** Send this buffer
         */
         uma_dbg_send_buffer(tcpRef, bufRef, pChar-pHead, FALSE);         
         /*
         ** Allocate a new one
         */
         bufRef = nextBuff;
         if ((pHead = (char *)ruc_buf_getPayload(bufRef)) == NULL) {
           severe( "ruc_buf_getPayload(%p)", bufRef );
           /* Let's tell the caller fsm that the message is sent */
           return;
         }  
         max_size = ruc_buf_getMaxPayloadLen(bufRef)-1024;
         pChar = pHead + sizeof(UMA_MSGHEADER_S);
         *pChar = 0;          
       }        

       if (start >= rozofs_trc_last_idx) start = 0;
        p = &rozofs_trc_buffer[start];
        if (p->hdr.s.fid == 1)
        {
	  rozofs_uuid_unparse(p->par.def.fid, str);
        } 
        else
	  rozofs_uuid_unparse(fake_fid, str); 
        if (p->hdr.s.req)
        {
          pChar+=sprintf(pChar,"[%12llu ]--> %-8s %4d %12.12llx ",
	           (unsigned long long int)(p->ts - cur_ts),trc_fuse_display_srv(p->hdr.s.service_id),p->hdr.s.index,
		   (unsigned long long int)p->ino);
          switch (p->hdr.s.trc_type)
	  {
	    default:
	    case rozofs_trc_type_attr:
	    case rozofs_trc_type_def:
              pChar+=sprintf(pChar,"%s\n",str);
	      break;
	    case rozofs_trc_type_def_flags:
              pChar+=sprintf(pChar,"%s (0%o)\n",str,p->flags);
	      break;
	    case rozofs_trc_type_io:
              pChar+=sprintf(pChar,"%s %8llu/%llu\n",str,(unsigned long long int)p->par.io.off,(unsigned long long int)p->par.io.size);
	      break;	
	    case rozofs_trc_type_name:
              pChar+=sprintf(pChar,"%s\n",p->par.name.name);
	      break;	
	    case rozofs_trc_type_name_flags:
              pChar+=sprintf(pChar,"%s (0x%x)\n",p->par.name.name, p->flags);
	      break;
	    case rozofs_trc_type_setattr:
	      pChar+=sprintf(pChar,"%s ", str);
	      if (p->par.setattr.to_set&FUSE_SET_ATTR_SIZE) {
	        pChar+=sprintf(pChar," SZ(%llu)", (long long unsigned int) p->par.setattr.attr.size);
	      }
	      if (p->par.setattr.to_set&FUSE_SET_ATTR_MODE) {
                pChar+=sprintf(pChar," MODE(%o)",p->par.setattr.attr.mode);
	      }
	      if (p->par.setattr.to_set&FUSE_SET_ATTR_UID) {
                pChar+=sprintf(pChar," UID(%d)", p->par.setattr.attr.uid);
	      }
	      if (p->par.setattr.to_set&FUSE_SET_ATTR_GID) {
                pChar+=sprintf(pChar," GID(%d)", p->par.setattr.attr.gid);
	      }	    	     
	      if (p->par.setattr.to_set&FUSE_SET_ATTR_ATIME) {
                pChar+=sprintf(pChar," ATIME");
	      }	    
	      if (p->par.setattr.to_set&FUSE_SET_ATTR_MTIME) {
                pChar+=sprintf(pChar," MTIME");
	      }
	      pChar+=sprintf(pChar,"\n");	
	      break;	
	    case rozofs_trc_type_flock:
	      switch(p->par.flock.mode) {
                case EP_LOCK_FREE:  mode = "free"; break;
                case EP_LOCK_WRITE: mode = "write"; break;
                case EP_LOCK_READ:  mode = "read"; break;	     
                default:            mode = "??"; 
              }	    
              pChar+=sprintf(pChar,"%s %s %llu-%llu %s \n", str, mode,
	                     (unsigned long long int)p->par.flock.start,
	                     (unsigned long long int)p->par.flock.len,
			     p->par.flock.block?"blocking":"pass");
	      break;	
	  }

        }
        else
        { 


          switch (p->hdr.s.trc_type)
	  {
	    default:
	    case rozofs_trc_type_io:
	    case rozofs_trc_type_def:
	    case rozofs_trc_type_setattr:
              pChar+=sprintf(pChar,"[%12llu ]<-- %-8s %4d %12.12llx %s %d:%s\n",
	                 (unsigned long long int)(p->ts - cur_ts),
		         trc_fuse_display_srv(p->hdr.s.service_id),
		         p->hdr.s.index,
		         (unsigned long long int)p->ino,
		         str,
		         p->errno_val,strerror(p->errno_val));  	  
	      break;
	    case rozofs_trc_type_name:
              pChar+=sprintf(pChar,"[%12llu ]<-- %-8s %4d %12.12llx %s\n",
	                 (unsigned long long int)(p->ts - cur_ts),
		         trc_fuse_display_srv(p->hdr.s.service_id),
		         p->hdr.s.index,
		         (unsigned long long int)p->ino,
		         (p->par.name.name[0] == 0)?strerror(p->errno_val):p->par.name.name); 
              break;		       
	    case rozofs_trc_type_attr:
              pChar+=sprintf(pChar,"[%12llu ]<-- %-8s %4d %12.12llx %s %d:%s %8llu\n",
	                 (unsigned long long int)(p->ts - cur_ts),
		         trc_fuse_display_srv(p->hdr.s.service_id),
		         p->hdr.s.index,
		         (unsigned long long int)p->ino,
		         str,
		         p->errno_val,strerror(p->errno_val),
		         (unsigned long long int)p->par.attr.size);
	      break;	
	  }


        }
        cur_ts = p->ts;   
   }
   /*
   ** Send last buffer
   */
   uma_dbg_send_buffer(tcpRef, bufRef, pChar-pHead, TRUE);         
   return;
}


static char * show_trc_fuse_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"trc_fuse reset         : reset trace buffer\n");
  pChar += sprintf(pChar,"trc_fuse enable        : enable trace mode\n");  
  pChar += sprintf(pChar,"trc_fuse disable       : disable trace mode\n");  
  pChar += sprintf(pChar,"trc_fuse status        : current status of the trace buffer\n");  
  pChar += sprintf(pChar,"trc_fuse count <count> : allocate a trace buffer with <count> entries\n");  
  pChar += sprintf(pChar,"trc_fuse               : display trace buffer\n");  
  return pChar; 
}  

void man_trc_fuse(char * pt) {
  pt = show_trc_fuse_help(pt);
  pt += sprintf(pt,"\nThe columns diplayed by trace trc_fuse must be read as follow:\n");
  pt += sprintf(pt,"1) delay between current row and previous row in CPU tick units.\n");
  pt += sprintf(pt,"   Check CPU frequency with \"frequency\" rozodiag CLI.\n");
  pt += sprintf(pt,"2) --> for incoming request.\n");
  pt += sprintf(pt,"   <-- for outgoing response.\n");
  pt += sprintf(pt,"3) Service name.\n");
  pt += sprintf(pt,"4) Request/response number. Enable to make the response match the request.\n");
  pt += sprintf(pt,"5) inode when available.\n");
  pt += sprintf(pt,"6) FID when available.\n");
  pt += sprintf(pt,"Optionnal and variable values:\n");
  pt += sprintf(pt,"- on responses:\n");
  pt += sprintf(pt,"  7) errno\n");
  pt += sprintf(pt,"  8) depending on the request\n");
  pt += sprintf(pt,"     - lookup,\n");
  pt += sprintf(pt,"       getattr,\n");  
  pt += sprintf(pt,"       setattr,\n");  
  pt += sprintf(pt,"       open    : the size of the object.\n");
  pt += sprintf(pt,"     - forget  : the remaining lookup count.\n");
  pt += sprintf(pt,"     - stafs   : number of available 1024K blocs.\n");
  pt += sprintf(pt,"     - readlink: the target name.\n");
}
void show_trc_fuse(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int   new_val;   
    int ret;
     
    if (argv[1] != NULL)
    {
        if (strcmp(argv[1],"reset")==0) {
	  rozofs_trace_reset();
	  uma_dbg_send(tcpRef, bufRef, TRUE, "Reset Done\n");    
	  return;
	}
        if (strcmp(argv[1],"enable")==0) {
	  if (rozofs_trc_enabled != 1)
	  {
            rozofs_trace_enable();
            rozofs_trace_reset();
            uma_dbg_send(tcpRef, bufRef, TRUE, "fuse trace is now enabled\n");    
	  }
	  else
	  {
            uma_dbg_send(tcpRef, bufRef, TRUE, "fuse trace is already enabled\n");    
	  }
	  return;
	}  
        if (strcmp(argv[1],"status")==0) {
	  rozofs_trace_status(pChar);
	  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	  return;
	}  
        if (strcmp(argv[1],"count")==0) {
	  errno = 0;
	  if (argv[2] == NULL)
	  {
            pChar += sprintf(pChar, "argument is missing\n");
	    pChar = show_trc_fuse_help(pChar);
	    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	    return;	  	  
	  }
          ret = sscanf(argv[2],"%d",&new_val);  
	  if (ret != 1) {
            pChar += sprintf(pChar, "bad value %s\n",argv[2]);
	    pChar = show_trc_fuse_help(pChar);
	    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	    return;
	  }
	  if (rozofs_trc_buffer != NULL) free(rozofs_trc_buffer);
	  if (rozofs_trace_init(new_val) < 0)
	  {
            uma_dbg_send(tcpRef, bufRef, TRUE, "cannot allocate a trace buffer with requested entries\n");
	    return;	       
	  }
          uma_dbg_send(tcpRef, bufRef, TRUE, "Done!!\n");
	  return;
	}  

        if (strcmp(argv[1],"disable")==0) {
	  if (rozofs_trc_enabled != ROZOFS_MBCACHE_DISABLE)
	  {
            rozofs_trace_disable();
            uma_dbg_send(tcpRef, bufRef, TRUE, "fuse trace is now disabled\n");    
	  }
	  else
	  {
            uma_dbg_send(tcpRef, bufRef, TRUE, "fuse trace is already disabled\n");    
	  }
	  return;
        }
	pChar = show_trc_fuse_help(pChar);
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	return;   	
    }
    show_trc_fuse_buffer(tcpRef,bufRef);
 }

struct fuse_lowlevel_ops rozofs_ll_operations = {
    .init = rozofs_ll_init,
    //.destroy = rozofs_ll_destroy,
    .lookup = rozofs_ll_lookup_nb, /** non blocking */
    .forget = rozofs_ll_forget, /** non blocking by construction */
    .getattr = rozofs_ll_getattr_nb, /** non blocking */
    .setattr = rozofs_ll_setattr_nb, /** non blocking */
    .readlink = rozofs_ll_readlink_nb, /** non blocking */
    .mknod = rozofs_ll_mknod_nb, /** non blocking */
    .mkdir = rozofs_ll_mkdir_nb, /** non blocking */
    .unlink = rozofs_ll_unlink_nb, /** non blocking */
    .rmdir = rozofs_ll_rmdir_nb, /** non blocking */
    .symlink = rozofs_ll_symlink_nb, /** non blocking */
    .rename = rozofs_ll_rename_nb, /** non blocking */
    .open = rozofs_ll_open_nb, /** non blocking */
    .link = rozofs_ll_link_nb, /** non blocking */
    .read = rozofs_ll_read_nb, /**non blocking */
    .write = rozofs_ll_write_nb, /**non blocking */
    .flush = rozofs_ll_flush_nb, /**non blocking */
    .release = rozofs_ll_release_nb, /**non blocking */
    .opendir = rozofs_ll_opendir_nb, /** non blocking by construction */
    .readdir = rozofs_ll_readdir_nb, /** non blocking */
    .releasedir = rozofs_ll_releasedir_nb, /** non blocking by construction */
    //.fsyncdir = rozofs_ll_fsyncdir, /** non blocking by construction */
    .statfs = rozofs_ll_statfs_nb, /** non blocking */
    .setxattr = rozofs_ll_setxattr_nb, /** non blocking */
    .getxattr = rozofs_ll_getxattr_nb, /** non blocking */
    .listxattr = rozofs_ll_listxattr_nb, /** non blocking */
    .removexattr = rozofs_ll_removexattr_nb, /** non blocking */
    //.access = rozofs_ll_access, /** non blocking by construction */
    .create = rozofs_ll_create_nb, /** non blocking */
    
    // POSIX lock to be activated thanks to -o posixlock option
    //.getlk = rozofs_ll_getlk_nb,
    //.setlk = rozofs_ll_setlk_nb,

    // BSD lock to be activated thanks to -o bsdlock option
    //.flock = rozofs_ll_flock_nb,

    //.bmap = rozofs_ll_bmap,
    //.ioctl = rozofs_ll_ioctl,
    //.poll = rozofs_ll_poll,
};

void rozofs_kill_one_storcli(int instance) {

    char pid_file[64];
    sprintf(pid_file,"/var/run/launcher_rozofsmount_%d_storcli_%d.pid", conf.instance, instance);
    rozo_launcher_stop(pid_file);
}

void rozofs_start_one_storcli(int instance) {
    char pid_file[128];
    char cmd[1024*2];
    uint16_t debug_port_value;
    char     debug_port_name[32];

    char *cmd_p = &cmd[0];
    cmd_p += sprintf(cmd_p, "%s ", "storcli");
    cmd_p += sprintf(cmd_p, "-i %d ", instance);
    cmd_p += sprintf(cmd_p, "-H %s ", conf.host);
    cmd_p += sprintf(cmd_p, "-o %s ", "rozofsmount");
    cmd_p += sprintf(cmd_p, "-E %s ", conf.export);
    cmd_p += sprintf(cmd_p, "-M %s ", rozofs_mountpoint);
    cmd_p += sprintf(cmd_p, "-g %d ", rozofs_site_number);
    
    if (conf.numanode != -1) {
      cmd_p += sprintf(cmd_p, "-n %d ", conf.numanode);      
    }
    
    /* Try to get debug port from /etc/services */
    debug_port_value = conf.dbg_port + instance;
    sprintf(debug_port_name,"rozo_storcli%d_%d_dbg",conf.instance,instance);
    debug_port_value = get_service_port(debug_port_name,NULL,debug_port_value);
          
    cmd_p += sprintf(cmd_p, "-D %d ", debug_port_value);
    cmd_p += sprintf(cmd_p, "-R %d ", conf.instance);
    cmd_p += sprintf(cmd_p, "--shaper %d ", conf.shaper);

    cmd_p += sprintf(cmd_p, "-s %d ", ROZOFS_TMR_GET(TMR_STORAGE_PROGRAM));
    cmd_p += sprintf(cmd_p, "-t %d ", ROZOFS_TMR_GET(TMR_PRJ_READ_SPARE));
    
    /*
    ** Storcli Mojette thread parameters
    */
    if (conf.mojThreadWrite!= -1) {
      cmd_p += sprintf(cmd_p, "-w %s ", (conf.mojThreadWrite==0)?"disable":"enable");
    }
    if (conf.mojThreadRead!= -1) {
      cmd_p += sprintf(cmd_p, "-r %s ", (conf.mojThreadRead==0)?"disable":"enable");
    }
    if (conf.mojThreadThreshold!= -1) {
      cmd_p += sprintf(cmd_p, "-m %d ", conf.mojThreadThreshold);
    }   
        
    /*
    ** check if there is a share mem key
    */
    if (rozofs_storcli_shared_mem[SHAREMEM_IDX_READ].key != 0)
    {
      cmd_p += sprintf(cmd_p, "-k %d ",rozofs_storcli_shared_mem[SHAREMEM_IDX_READ].key);       
      cmd_p += sprintf(cmd_p, "-l %d ",rozofs_storcli_shared_mem[SHAREMEM_IDX_READ].buf_sz);       
      cmd_p += sprintf(cmd_p, "-c %d ",rozofs_storcli_shared_mem[SHAREMEM_IDX_READ].buf_count);       
    }
    cmd_p += sprintf(cmd_p, "-L %d -B %d ",exportclt.layout, exportclt.bsize);       

    if (conf.localPreference) {
      cmd_p += sprintf(cmd_p, "-f ");
    } 
    
    if (conf.noReadFaultTolerant) {
      cmd_p += sprintf(cmd_p, "-F ");
    } 
    
    sprintf(pid_file,"/var/run/launcher_rozofsmount_%d_storcli_%d.pid", conf.instance, instance);
    rozo_launcher_start(pid_file,cmd);
    
    info("start storcli (instance: %d, export host: %s, export name: %s, mountpoint: %s,"
            " profile port: %d, rozofs instance: %d, storage timeout: %d).",
            instance, conf.host, conf.export, rozofs_mountpoint,
            debug_port_value, conf.instance,
            ROZOFS_TMR_GET(TMR_STORAGE_PROGRAM));

}
void rozofs_kill_storcli() {
    int i;

    for (i = 1; i <= STORCLI_PER_FSMOUNT; i++) {
      rozofs_kill_one_storcli(i);
    }
}
void rozofs_start_storcli() {
	int i;

	//rozofs_kill_storcli();

	i = stclbg_get_storcli_number();
	while (i) {
		rozofs_start_one_storcli(i);
		i--;
	}
}


int fuseloop(struct fuse_args *args, int fg) {
    int i = 0;
    int ret;
    int err=0;
    char *c;
    int piped[2];
    piped[0] = piped[1] = -1;
    char s;
    struct fuse_chan *ch;
    struct fuse_session *se;
    int retry_count;
    char ppfile[NAME_MAX];
    int ppfd = -1;
    int export_index=0;
    char * pHost;

    sprintf(ppfile,"rozofsmount:%d",conf.instance);
    uma_dbg_record_syslog_name(ppfile);

    uma_dbg_thread_add_self("Starter");

    struct timeval timeout_mproto;
    timeout_mproto.tv_sec = 1;//rozofs_tmr_get(TMR_EXPORT_PROGRAM);
    timeout_mproto.tv_usec = 0;

  
    if (rozofs_host_list_parse(conf.host,'/') == 0) {
      severe("rozofs_host_list_parse(%s)",conf.host);
    }

    init_rpcctl_ctx(&exportclt.rpcclt);

    for (retry_count = 15; retry_count > 0; retry_count--) {
    
        for (export_index=0; export_index < ROZOFS_HOST_LIST_MAX_HOST; export_index++) { 
	
	    pHost = rozofs_host_list_get_host(export_index);
	    if (pHost == NULL) break;
	    
            /* Initiate the connection to the export and get information
             * about exported filesystem */
            if (exportclt_initialize(
                    &exportclt,
                    pHost,
                    conf.export,
		    rozofs_site_number,
                    conf.passwd,
                    conf.buf_size * 1024,
                    conf.min_read_size * 1024,
                    conf.max_retry,
                    timeout_mproto) == 0) break;
        }
        if (errno == EPERM) {
          retry_count = 0;
          break;
        } 
        
	/* Connected to one of the given addresses */
	if (pHost != NULL) break;
	
        sleep(2);
	timeout_mproto.tv_sec++;
	
    }

    // Check the mountpoint after success connection with the export
    if (rozofs_mountpoint_check(rozofs_mountpoint) != 0) {
        return 1;
    }
    if (retry_count == 0) {

        fprintf(stderr,
                "rozofsmount failed for:\n" "export directory: %s\n"
                "export hostname: %s\n" "local mountpoint: %s\n" "error: %s\n"
                "See log for more information\n", conf.export, conf.host,
                rozofs_mountpoint, strerror(errno));
        return 1;
    }
    
    /* 
    ** Now that we know the block size of the export, let's check that minimum read size is a
    ** mutiple of the he block size 
    */
    uint32_t bkbytes = ROZOFS_BSIZE_BYTES(exportclt.bsize)/1024;
    if ((conf.min_read_size % bkbytes) != 0) {
      conf.min_read_size = ((conf.min_read_size / bkbytes)+1) * bkbytes;
    }        

#if 0
    This message has been post pone after the setup of the EXPORTD LBG (toward slave exportd)
    /*
    ** Send the file lock reset request to remove old locks
    */
    {
    	epgw_lock_arg_t     arg;

    	arg.arg_gw.eid             = exportclt.eid;
    	arg.arg_gw.lock.client_ref = rozofs_client_hash;

    	ep_clear_client_file_lock_1(&arg, exportclt.rpcclt.client);
    }
#endif
    /*
    ** init of the wr_block pending head of list
    */
    list_init(&list_wr_block_head);

    /* Initialize list and htables for inode_entries */
    list_init(&inode_entries);
    htable_initialize(&htable_inode, INODE_HSIZE, fuse_ino_hash, fuse_ino_cmp);
//    htable_initialize(&htable_fid, PATH_HSIZE, fid_hash, fid_cmp);

    /* Put the root inode entry*/
    ientry_t *root = xmalloc(sizeof (ientry_t));
    memset(root, 0, sizeof (ientry_t));
    memcpy(root->fid, exportclt.rfid, sizeof (fid_t));
    root->inode = ROOT_INODE;
    root->db.size = 0;
    root->db.eof = 0;
    root->db.p = NULL;
    root->nlookup = 1;
    put_ientry(root);

    info("mounting - export: %s from : %s on: %s", conf.export, conf.host,
            rozofs_mountpoint);

    if (fg == 0) {
        if (pipe(piped) < 0) {
            fprintf(stderr, "pipe error\n");
            return 1;
        }
        err = fork();
        if (err < 0) {
            fprintf(stderr, "fork error\n");
            return 1;
        } else if (err > 0) {
            // Parent process closes up output side of pipe
            close(piped[1]);
            err = read(piped[0], &s, 1);
            if (err == 0) {
                s = 1;
            }
            return s;
        }
        // Child process closes up input side of pipe
        close(piped[0]);
        s = 1;
    }
    if ((ch = fuse_mount(rozofs_mountpoint, args)) == NULL) {
        fprintf(stderr, "error in fuse_mount\n");
        if (piped[1] >= 0) {
            if (write(piped[1], &s, 1) != 1) {
                fprintf(stderr, "pipe write error\n");
            }
            close(piped[1]);
        }
        return 1;
    }
    /*
    ** Are BSD and POSIX lockdisabled
    */
    if (!conf.no_file_lock) {
      rozofs_ll_operations.getlk = rozofs_ll_getlk_nb;
      rozofs_ll_operations.setlk = rozofs_ll_setlk_nb;
      rozofs_ll_operations.flock = rozofs_ll_flock_nb;
    }

    se = fuse_lowlevel_new(args, &rozofs_ll_operations,
            sizeof (rozofs_ll_operations), (void *) piped);

    if (se == NULL) {
        fuse_unmount(rozofs_mountpoint, ch);
        fprintf(stderr, "error in fuse_lowlevel_new\n");
        usleep(100000); // time for print other error messages by FUSE
        if (piped[1] >= 0) {
            if (write(piped[1], &s, 1) != 1) {
                fprintf(stderr, "pipe write error\n");
            }
            close(piped[1]);
        }
        return 1;
    }

    fuse_session_add_chan(se, ch);

    if (fuse_set_signal_handlers(se) < 0) {
        fprintf(stderr, "error in fuse_set_signal_handlers\n");
        fuse_session_remove_chan(ch);
        fuse_session_destroy(se);
        fuse_unmount(rozofs_mountpoint, ch);
        if (piped[1] >= 0) {
            if (write(piped[1], &s, 1) != 1) {
                fprintf(stderr, "pipe write error\n");
            }
            close(piped[1]);
        }
        return 1;
    }

    if (fg == 0) {
        setsid();
        setpgid(0, getpid());
        if ((i = open("/dev/null", O_RDWR, 0)) != -1) {
            (void) dup2(i, STDIN_FILENO);
            (void) dup2(i, STDOUT_FILENO);
            (void) dup2(i, STDERR_FILENO);
            if (i > 2)
                close(i);
        }
    }

    /* Creates one debug thread */

    pthread_t thread;
    rozofs_fuse_conf_t rozofs_fuse_conf;

    semForEver = malloc(sizeof (sem_t)); /* semaphore for blocking the main thread doing nothing */
    //    int ret;

    if (sem_init(semForEver, 0, 0) == -1) {
        severe("main() sem_init() problem : %s", strerror(errno));
    }

    /*
     ** Register these topics before start the rozofs_stat_start that will
     ** register other topic. Topic registration is not safe in multi-thread
     ** case
     */
    uma_dbg_addTopic_option("stclbg", show_stclbg,UMA_DBG_OPTION_RESET);
    uma_dbg_addTopic_option("profiler", show_profiler,UMA_DBG_OPTION_RESET);
    uma_dbg_addTopic("exp_route", show_exp_routing_table);
    uma_dbg_addTopic("exp_eid", show_eid_exportd_assoc);
    uma_dbg_addTopic("mount",show_mount);
    uma_dbg_addTopic("cache_set", rozofs_set_cache);
    uma_dbg_addTopic("fsmode_set", rozofs_set_fsmode);
    uma_dbg_addTopic("shared_mem", rozofs_shared_mem_display);
    uma_dbg_addTopic("blockmode_cache", show_blockmode_cache);
    uma_dbg_addTopic_option("data_cache", rozofs_gcache_show_cache_stats,UMA_DBG_OPTION_RESET);
    uma_dbg_addTopic("start_config", show_start_config);
    uma_dbg_addTopic("rotateModulo", show_rotate_modulo);
    uma_dbg_addTopic_option("flock", show_flock,UMA_DBG_OPTION_RESET);
    uma_dbg_addTopic("ientry", show_ientry);
    rozofs_layout_initialize();    
    uma_dbg_addTopic("layout", show_layout);
    uma_dbg_addTopicAndMan("prjsize", show_prj_size, man_prj_size, 0);
    uma_dbg_addTopicAndMan("trc_fuse", show_trc_fuse,man_trc_fuse,UMA_DBG_OPTION_RESET);
    uma_dbg_addTopic("xattr_flt", show_xattr_flt);
    uma_dbg_addTopic("xattr", rozofs_disable_xattr);

    // Register rozodiag comman to get CPU frequency
    rozofs_get_cpu_frequency();
    
    /*
    ** Register malloc tracking diagnostic service
    */
    //malloc_tracking_register();    
    /*
    ** Disable extended attributes if required
    */
    if (conf.noXattr) {
      rozofs_xattr_disable = 1;
    }  
      
    /*
    ** clear write flush alignement stats
    */
    {
      int k;
      for (k= 0;k < 2;k++)
      {
        rozofs_aligned_write_start[k] = 0;
        rozofs_aligned_write_end[k] = 0;
      }
    }
    /*
    ** init of the trace buffer
    */
    rozofs_trace_init(ROZOFS_TRACE_BUF_SZ);

#if 0
    rozofs_trace_enable();
    rozofs_trace_init(200);
#endif

    /*
    ** init of the xattribute filter
    */
    rozofs_xattr_flt_filter_init();
    /*
    ** Initialize the number of write pending per fd
    ** and reset statistics
    */
    init_write_flush_stat(conf.max_write_pending);

    /*
    ** Initialize the number of active write threads
    */
    init_write_thread_active(conf.nb_writeThreads);

    /**
    * init of the mode block cache
    */
    ret = rozofs_mbcache_cache_init(ROZOFS_MBCACHE_DISABLE);
    if (ret < 0)
    {
      severe("Cannot create the mode block cache, revert to non-caching mode");
    }
    /**
    * init of the common cache array
    */
    ret = rozofs_gcache_pool_init();
    if (ret < 0)
    {
      severe("Cannot create the global cache, revert to non-caching mode");
    }
    /*
    ** declare timer debug functions
    */
    rozofs_timer_conf_dbg_init();
    /*
    ** Check if the base port of rozodiag has been provided, if there is no value, set it to default
    */
    if (conf.dbg_port == 0) 
    {
      conf.dbg_port = rozofs_get_service_port_fsmount_diag(conf.instance);    
    }    
    
   
    rozofs_fuse_conf.instance = (uint16_t) conf.instance;
    rozofs_fuse_conf.debug_port = conf.dbg_port;
    rozofsmount_diag_port = conf.dbg_port;

    rozofs_fuse_conf.se = se;
    rozofs_fuse_conf.ch = ch;
    rozofs_fuse_conf.exportclt = (void*) &exportclt;
    if (conf.nbstorcli > 1) {
     if ( conf.nbstorcli == 4) rozofs_max_storcli_tx = ROZOFSMOUNT_MAX_4_STORCLI_TX_PER_PROCESS*conf.nbstorcli;
     else rozofs_max_storcli_tx = ROZOFSMOUNT_MAX_DEFAULT_STORCLI_TX_PER_PROCESS*conf.nbstorcli;
    }
    else
    {
      rozofs_max_storcli_tx = ROZOFSMOUNT_MAX_DEFAULT_STORCLI_TX_STANDALONE;
    }
    rozofs_fuse_conf.max_transactions =ROZOFSMOUNT_MAX_EXPORT_TX + rozofs_max_storcli_tx;

    if ((errno = pthread_create(&thread, NULL, (void*) rozofs_stat_start, &rozofs_fuse_conf)) != 0) {
        severe("can't create debug thread: %s", strerror(errno));
        return err;
    }


    /* try to create a flag file with port number */
    sprintf(ppfile, "%s%s_%d%s", DAEMON_PID_DIRECTORY, "rozofsmount",conf.instance, rozofs_mountpoint);
    c = ppfile + strlen(DAEMON_PID_DIRECTORY);
    while (*c++) {
        if (*c == '/') *c = '.';
    }
    if ((ppfd = open(ppfile, O_RDWR | O_CREAT, 0640)) < 0) {
        severe("can't open profiling port file");
    } else {
        char str[10];
        sprintf(str, "%d\n", getpid());
		if ((write(ppfd, str, strlen(str))) != strlen(str)) {
			severe("can't write pid on flag file");
		}
        close(ppfd);
    }
    /*
    ** create the shared memory used by the storcli's
    */
    for (i = 0; i < SHAREMEM_PER_FSMOUNT; i++) {
       /*
       ** the size of the buffer is retrieved from the configuration. 1K is added for the management part of
       ** the RPC protocol. The key_instance of the shared memory is the concatenantion of the rozofsmount instance and
       ** storcli instance: (rozofsmount<<1 | storcli_instance) (assuming of max of 2 storclis per rozofsmount)
       */
       int key_instance = conf.instance<<SHAREMEM_PER_FSMOUNT_POWER2 | i;
       uint32_t buf_sz;
       if (SHAREMEM_IDX_READ == i) buf_sz = ROZOFS_MAX_FILE_BUF_SZ_READ;
       else buf_sz = ROZOFS_MAX_FILE_BUF_SZ_READ;
       ret = rozofs_create_shared_memory(key_instance,i,rozofs_max_storcli_tx,(buf_sz)+4096);
       if (ret < 0)
       {
         fatal("Cannot create the shared memory for storcli %d\n",i);
       }
    }   
    rozofs_shared_mem_init_done  = 1;  

    /*
    ** start the storcli processes
    */ 
    rozofs_start_storcli();

    for (;;) {
        int ret;
        ret = sem_wait(semForEver);
        if (ret != 0) {
            severe("sem_wait: %s", strerror(errno));
            continue;

        }
        break;
    }

    rozofs_kill_storcli();

    fuse_remove_signal_handlers(se);
    fuse_session_remove_chan(ch);
    fuse_session_destroy(se);
    fuse_unmount(rozofs_mountpoint, ch);
    exportclt_release(&exportclt);
    ientries_release();
    rozofs_layout_release();
    if (conf.export != NULL)
        free(conf.export);
    if (conf.host != NULL)
        free(conf.host);
    if (conf.passwd != NULL)
        free(conf.passwd);
    unlink(ppfile); // best effort

    return err ? 1 : 0;
}


/** Check that no other rozofsmount with the same instance is
 * running on the smae host
 *
 * @param instance: the instance number of this rozofsmount
 *
 * @retval: 0 when no other such instance is running
 */
int rozofs_check_instance(int instance) {
  char   cmd[256];
  char   fname[64];
  FILE * fp=NULL;
  int    ret;
  int    val=-1;
  int    status = 0;
  
  sprintf(fname,"/tmp/rozofsmount.%d.ps", getpid());
  
  /* Default instance number is 0. So get also the number of 
  ** rozofsmount started without instance number in its parameters */  
  if (instance==0) {
    sprintf(cmd,"ps -o cmd= -C rozofsmount | awk '!/instance/||/instance=0,/' | wc -l > %s",
            fname);  
  }
  else {
    sprintf(cmd,"ps -o cmd= -C rozofsmount | awk '/instance=%d,/' | wc -l > %s",
            instance, fname);
  }	  
  
  ret = system(cmd);
  if (ret < 0) goto out;	  
  
  fp = fopen(fname,"r");
  if (fp == NULL) goto out;
  
  ret = fscanf(fp, "%d", &val);
  if (ret != 1) goto out;
  
  if (val > 1) {
    status = -1;
  } 
   
out:
  if (fp) fclose(fp);   
  unlink(fname); 
  return status;
}

void rozofs_allocate_flush_buf(int size_kB);

/** Check if a given RozoFS mountpoint is already mounted
 *
 * @param *mntpoint: mountpoint to check
 *
 * @return: 0 on success -1 otherwise
 */
int rozofs_mountpoint_check(const char * mntpoint) {

    struct mntent* mnt_entry = NULL;
    FILE* mnt_file_stream = NULL;
    char mountpoint_path[PATH_MAX];

    if (!realpath(mntpoint, mountpoint_path)) {
        fprintf(stderr, "bad mount point %s: %s\n", mntpoint, strerror(errno));
        return -1;
    }

    mnt_file_stream = setmntent(MOUNTED_FS_FILE_CHECK, "r");
    if (mnt_file_stream == NULL) {
        fprintf(stderr, "setmntent failed for file "MOUNTED_FS_FILE_CHECK":"
        " %s \n", strerror(errno));
        return -1;
    }

    while ((mnt_entry = getmntent(mnt_file_stream))) {

        if ((strcmp(mountpoint_path, mnt_entry->mnt_dir) == 0)
                && (strcmp(FSNAME, mnt_entry->mnt_fsname) == 0)) {
            fprintf(stderr,
                    "according to '"MOUNTED_FS_FILE_CHECK"', %s is already a"
                    " active mountpoint for a Rozo file system\n",
                    mountpoint_path);
            endmntent(mnt_file_stream);
            return -1;
        }
    }

    endmntent(mnt_file_stream);

    return 0;
}

int main(int argc, char *argv[]) {
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    int fg = 0;
    int res;
    struct rlimit core_limit;
    
    memset(&exportclt,0,sizeof(exportclt_t ));

    ALLOC_PROFILING(mpp_profiler_t) ;
    /*
    ** Change local directory to "/"
    */
    if (chdir("/")!=0) {
    }

    /*
    ** Set PATH env. variable if it doesn't exist (useful for mounting a
    ** rozofs mountpoint thanks mount utility when execs are under /usr/local)
    */
    char * path = getenv("PATH");
    if (path == NULL) {
        path = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
        setenv("PATH", path, 0);
    }

    memset(&conf, 0, sizeof (conf));
    /*
    ** init of the shared memory data structure
    */
    rozofs_init_shared_memory();
    /*
    ** init of the timer configuration
    */
    rozofs_tmr_init_configuration();

    conf.max_retry = 50;
    conf.buf_size = 0;
    conf.min_read_size = ROZOFS_BSIZE_BYTES(ROZOFS_BSIZE_MIN)/1024;
    conf.max_write_pending = ROZOFS_BSIZE_BYTES(ROZOFS_BSIZE_MIN)/1024; /*  */ 
    conf.attr_timeout = -1;
    conf.entry_dir_timeout_ms= -1;
    conf.enoent_timeout_ms = -1;
    conf.numanode = -1;
    conf.attr_dir_timeout_ms= -1;
    conf.attr_timeout_ms = -1;
    conf.entry_timeout = -1;
    conf.entry_timeout_ms = -1;
    conf.symlink_timeout = -1; /* Get default value */
    conf.nbstorcli = 0;
    conf.shaper = 0; // Default traffic shaper value
    conf.rotate = 0;
    conf.posix_file_lock = 0; // No posix file lock until explicitly activated  man 2 fcntl)
    conf.bsd_file_lock = 0;   // No BSD file lock until explicitly activated    man 2 flock)
    conf.no_file_lock = 0;   // To disable locks 
    conf.noXattr = 0;   // By default extended attributes are supported
    conf.no0trunc = 0;  // By default truncate to zero are sent to exportd and storages
    conf.onlyWriter = 0;  // By default this client is not the only writer of the file it writes to
    conf.xattrcache = 0;  // By default this client does not cache extended attributes
    conf.asyncsetattr = 0;  // By default this client operates in synchronous mode for setattr
    conf.site = -1;
    conf.conf_site_file = -1; /* no site file  */
    conf.mojThreadWrite     = -1; // By default, do not modify the storli default
    conf.mojThreadRead      = -1; // By default, do not modify the storli default
    conf.mojThreadThreshold = -1; // By default, do not modify the storli default
    conf.localPreference = 0; // No local preference on read
    conf.noReadFaultTolerant = 0; // Give back blocks with 0 on read for corrupted block instead of EIO
    conf.wbcache = 0;
    conf.nb_writeThreads = 0;
    conf.idx_fuse_profile = ROZOFS_DEFAULT_FUSE_PROFILE;
    if (fuse_opt_parse(&args, &conf, rozofs_opts, myfs_opt_proc) < 0) {
        exit(1);
    }

    /*
    ** read common config file
    */
    common_config_read(NULL);  

    /*
    **  Rozofsmount instance must be lower than 128
    */
    if (conf.instance >= 128) {
        fprintf(stderr, "Rozofsmount instance must be lower than 128\n");  
        return 1;          
    }  

    /*
    **  set the numa node for rozofsmount and its storcli
    */
    if (conf.numanode==-1) {
      rozofs_numa_allocate_node(conf.instance,"instance");
    }
    else {
      rozofs_numa_allocate_node(conf.numanode,"parameter numanode");
    }  
    /*
    ** init of the site number for that rozofs client
    */
    conf.conf_site_file = rozofs_get_local_site();
    while (1)
    {
      if (conf.site != -1) 
      {
	    rozofs_site_number = conf.site;
	    break;
      }
      if (conf.conf_site_file < 0)
      {
	    rozofs_site_number = 0;
	    break;
      }
      rozofs_site_number = conf.conf_site_file;
      break;
    }
    conf.running_site = rozofs_site_number;
    

    if (conf.host == NULL) {
        conf.host = strdup("rozofsexport");
    }

    if (strlen(conf.host) >= ROZOFS_HOSTNAME_MAX) {
        fprintf(stderr,
                "The length of export host must be lower than %d\n",
                ROZOFS_HOSTNAME_MAX);
    }

    if (conf.export == NULL) {
        fprintf(stderr, "Missing -E option. Export name (or root path) is mandatory.\n");
        return 1;                
    }

    if (conf.passwd == NULL) {
        conf.passwd = strdup("none");
    }

    if (conf.buf_size == 0) {
        conf.buf_size = 256;
    }
    if (conf.buf_size < 128) {
        fprintf(stderr,
                "write cache size too low (%u KiB) - increased to 128 KiB\n",
                conf.buf_size);
        conf.buf_size = 128;
    }
    if (conf.buf_size > 256) {
        fprintf(stderr,
                "write cache size too big (%u KiB) - decreased to 256 KiB\n",
                conf.buf_size);
        conf.buf_size = 256;
    }
    
    if (conf.min_read_size == 0) {
        conf.min_read_size = 4;
    }
    if (conf.min_read_size > conf.buf_size) {
        conf.min_read_size = conf.buf_size;
    }


    if (conf.nbstorcli != 0) {
      if (stclbg_set_storcli_number(conf.nbstorcli) < 0) {
          fprintf(stderr,
                  "invalid rozofsnbstorcli parameter (%d) allowed range is [1..%d]\n",
                  conf.nbstorcli,STORCLI_PER_FSMOUNT);
      }
    }
    
    /* Initialize the rotation modulo on distribution for read request */
    rozofs_rotation_read_modulo = conf.rotate;
    
    /*
    ** Compute the identifier of the client from host and instance id 
    */
    {
        char hostName[256];
        hostName[0] = 0;
        gethostname(hostName, 256);
        rozofs_client_hash = rozofs_client_hash_compute(hostName, conf.instance);
    }
    /*
    ** allocate the common flush buffer
    */
    rozofs_allocate_flush_buf(conf.buf_size);

    // Set timeout for exportd requests
    if (conf.export_timeout != 0) {
        if (rozofs_tmr_configure(TMR_EXPORT_PROGRAM,conf.export_timeout)< 0)
        {
          fprintf(stderr,
                "timeout for exportd requests is out of range: revert to default setting");
        }
    }

    if (conf.storage_timeout != 0) {
        if (rozofs_tmr_configure(TMR_STORAGE_PROGRAM,conf.storage_timeout)< 0)
        {
          fprintf(stderr,
                "timeout for storaged requests is out of range: revert to default setting");
        }
    }
    
    if (conf.spare_timeout != 0) {
        if (rozofs_tmr_configure(TMR_PRJ_READ_SPARE,conf.spare_timeout)< 0)
        {
          fprintf(stderr,
                "timeout for spare requests is out of range: revert to default setting");
        }
    }    
    

    if (conf.storcli_timeout != 0) {
        if (rozofs_tmr_configure(TMR_STORCLI_PROGRAM,conf.storcli_timeout)< 0)
        {
          fprintf(stderr,
                "timeout for storcli requests is out of range: revert to default setting");
        }
    }
    if (conf.cache_mode > 2) {

          fprintf(stderr,
                "cache mode out of range: revert to default setting");
    }

    if (conf.fs_mode > 1) {

          fprintf(stderr,
                "rozofs mode out of range: revert to default setting");
          conf.fs_mode = 0;
    }
    rozofs_mode = conf.fs_mode;
    
    // attr_timeout & attr_timeout_ms are defined => ignore attr_timeout_ms
    if ((conf.attr_timeout != -1)&&(conf.attr_timeout_ms != -1)) { 
	fprintf(stderr,
            "attr_timeout as well as attr_timeout_ms are defined."
	      " Ignoring attr_timeout_ms.");
	conf.attr_timeout_ms = -1;
    }
    if (conf.attr_timeout != -1) {    
        if (rozofs_tmr_configure(TMR_FUSE_ATTR_CACHE,conf.attr_timeout) < 0)
	{
          fprintf(stderr,
        	"timeout for which file/directory attributes are cached is out"
                  " of range: revert to default setting");
	}
    }
    else if (conf.attr_timeout_ms != -1) {
        if (rozofs_tmr_configure(TMR_FUSE_ATTR_CACHE_MS,conf.attr_timeout_ms) < 0)	
	{
          fprintf(stderr,
        	"timeout for which file/directory attributes are cached is out"
                  " of range: revert to default setting");
	}
	else {
	  rozofs_tmr_configure(TMR_FUSE_ATTR_CACHE,0);
	}
    }

    // entry_timeout & entry_timeout_ms are defined => ignore entry_timeout_ms    
    if ((conf.entry_timeout != -1)&&(conf.entry_timeout_ms != -1)) { 
	fprintf(stderr,
            "entry_timeout as well as entry_timeout_ms are defined."
	      " Ignoring entry_timeout_ms.");
	conf.entry_timeout_ms = -1;
    }
    if (conf.entry_timeout != -1) {
        if (rozofs_tmr_configure(TMR_FUSE_ENTRY_CACHE,conf.entry_timeout) < 0)
        {
          fprintf(stderr,
                "timeout for which name lookups will be cached is out of range:"
                  " revert to default setting");
        }
    }
    else if (conf.entry_timeout_ms != -1) {
        if (rozofs_tmr_configure(TMR_FUSE_ENTRY_CACHE_MS,conf.entry_timeout_ms) < 0)	
	{
          fprintf(stderr,
                "timeout for which name lookups will be cached is out of range:"
                  " of range: revert to default setting");
	}
	else {
	  rozofs_tmr_configure(TMR_FUSE_ENTRY_CACHE,0);
	}
    }
        
    if (conf.symlink_timeout != -1) {
        if (rozofs_tmr_configure(TMR_LINK_CACHE,conf.symlink_timeout) < 0)
        {
          fprintf(stderr,
                "timeout for which symlink target will be cached is out of range:"
                  " revert to default setting");
        }
    }
    if (conf.entry_dir_timeout_ms ==-1)
    {
      rozofs_tmr_configure(TMR_FUSE_ENTRY_DIR_CACHE_MS,rozofs_tmr_get_entry_ms());    
    }
    else
    {
      rozofs_tmr_configure(TMR_FUSE_ENTRY_DIR_CACHE_MS,conf.entry_dir_timeout_ms);        
    }
    
    if (conf.enoent_timeout_ms ==-1)
    {
      rozofs_tmr_configure(TMR_FUSE_ENOENT_CACHE_MS,rozofs_tmr_get_enoent());    
    }
    else
    {
      rozofs_tmr_configure(TMR_FUSE_ENOENT_CACHE_MS,conf.enoent_timeout_ms);        
    }
    
    if (conf.attr_dir_timeout_ms ==-1)
    {
      rozofs_tmr_configure(TMR_FUSE_ATTR_DIR_CACHE_MS,rozofs_tmr_get_attr_ms());    
    }
    else
    {
      rozofs_tmr_configure(TMR_FUSE_ATTR_DIR_CACHE_MS,conf.attr_dir_timeout_ms);        
    }


        
    if (fuse_version() < 28) {
        if (conf.wbcache)
	{
          if (fuse_opt_add_arg(&args, "-o" FUSE27_DEFAULT_OPTIONS_WB) == -1) {
              fprintf(stderr, "fuse_opt_add_arg failed\n");
              return 1;
	  }
	}
	else
        {	
          if (fuse_opt_add_arg(&args, "-o" FUSE27_DEFAULT_OPTIONS) == -1) {
              fprintf(stderr, "fuse_opt_add_arg failed\n");
              return 1;
	  }
        }
    } else {
        if (conf.wbcache)
	{        
          if (fuse_opt_add_arg(&args, "-o" FUSE28_DEFAULT_OPTIONS_WB) == -1) {
              fprintf(stderr, "fuse_opt_add_arg failed\n");
              return 1;
          }
	}
	else
	{
          if (fuse_opt_add_arg(&args, "-o" FUSE28_DEFAULT_OPTIONS) == -1) {
              fprintf(stderr, "fuse_opt_add_arg failed\n");
              return 1;
          }		
	}
    } 
    /*
    ** set the readahead, congestion_threshold and max_background
    */
    {
      rozofs_fuse_profile_t *fuse_profile_p;
      char bufall[128];
      
      fuse_profile_p = &fuse_kern_profile[conf.idx_fuse_profile];      
      sprintf(bufall,"-omax_readahead=%u ",(unsigned int)fuse_profile_p->ra_pages*4096);
      if (fuse_opt_add_arg(&args,bufall) == -1) {
          fprintf(stderr, "fuse_opt_add_arg failed\n");
          return 1;
      }	
      sprintf(bufall,"-omax_background=%u ",(unsigned int)fuse_profile_p->max_background);
      if (fuse_opt_add_arg(&args,bufall) == -1) {
          fprintf(stderr, "fuse_opt_add_arg failed\n");
          return 1;
      }	

      sprintf(bufall,"-ocongestion_threshold=%u ",(unsigned int)fuse_profile_p->congestion_threshold);
      if (fuse_opt_add_arg(&args,bufall) == -1) {
          fprintf(stderr, "fuse_opt_add_arg failed\n");
          return 1;
      }	
    } 

    if (fuse_parse_cmdline(&args, &rozofs_mountpoint, NULL, &fg) == -1) {
        fprintf(stderr, "see: %s -h for help\n", argv[0]);
        return 1;
    }

    if (!rozofs_mountpoint) {
        fprintf(stderr, "no mount point\nsee: %s -h for help\n", argv[0]);
        return 1;
    }

    // Check the mountpoint
    if (rozofs_mountpoint_check(rozofs_mountpoint) != 0) {
        return 1;
    }
    
    /*
    ** Check whether such instance of rozofsmount is already running.
    ** Wait some seconds for the previous rozofsmount process to eventually stop
    */
    int delay=0;
#define ROZOFS_MOUNT_CHECK_RETRY_MICROSEC     (500000)  
#define ROZOFS_MOUNT_CHECK_TIMEOUT_MICROSEC  (7000000)  
    while (1) {
      /*
      ** Check whether the same instance of rozofsmount is running
      */
      if (rozofs_check_instance(conf.instance)==0) break; // No such running instance
            
      /*
      ** A rozofsmount is running with the same instance number
      ** Give it a ROZOFS_MOUNT_CHECK_TIMEOUT_MICROSEC delay to stop
      */
      if (delay>=ROZOFS_MOUNT_CHECK_TIMEOUT_MICROSEC) {
        fprintf(stderr, "Can not mount %s\n", rozofs_mountpoint);
        fprintf(stderr, "A RozoFS mount point is already mounted with the same instance number (%d)\n", conf.instance);
        return 1;  	
      }
      /* 
      ** Sleep for some microseconds
      */
      usleep(ROZOFS_MOUNT_CHECK_RETRY_MICROSEC);
      delay += ROZOFS_MOUNT_CHECK_RETRY_MICROSEC; 	
    }
    {
         char path[256];
	 
	 sprintf(path,"%s/mount/inst_%d/mount/",ROZOFS_KPI_ROOT_PATH,conf.instance);
	 ALLOC_KPI_FILE_PROFILING(path,"profiler",mpp_profiler_t);    
    }
    /*
    ** assert the cache mode
    */
    if (conf.fs_mode == 0) {

        if (conf.cache_mode > 2) {
            rozofs_cache_mode = 0;
        } else {
            rozofs_cache_mode = conf.cache_mode;
        }
    } else {

        if (conf.cache_mode > 2) {
            rozofs_cache_mode = 0;
        } else {
            rozofs_cache_mode = conf.cache_mode;
        }
    }
    
    // Change the value of maximum size of core file
    core_limit.rlim_cur = RLIM_INFINITY;
    core_limit.rlim_max = RLIM_INFINITY;
    if (setrlimit(RLIMIT_CORE, &core_limit) < 0) {
        warning("Failed to change maximum size of core file: %s",
                strerror(errno));
    }

    // Change AF_UNIX datagram socket length
    af_unix_socket_set_datagram_socket_len(128);
    
    /*
    ** Check whether fast reconnect is required
    */
    if (common_config.client_fast_reconnect) {
      /*
      ** Change some timer values
      */
      rozofs_tmr_configure(TMR_TCP_FIRST_RECONNECT,1);
      rozofs_tmr_configure(TMR_TCP_RECONNECT,1);
      rozofs_tmr_configure(TMR_RPC_NULL_PROC_TCP,1);
      rozofs_tmr_configure(TMR_RPC_NULL_PROC_LBG,1);   
    }
    /*
    ** update the file archiving timers
    */
    rozofs_tmr_configure(TMR_FUSE_ATTR_ARCH_CACHE,common_config.archive_file_attr_timeout);
    rozofs_tmr_configure(TMR_FUSE_ENTRY_ARCH_CACHE,common_config.archive_file_dentry_timeout);          

    gprofiler->uptime = time(0);
    res = fuseloop(&args, fg);

    fuse_opt_free_args(&args);
    return res;
}
