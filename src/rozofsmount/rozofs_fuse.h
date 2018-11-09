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
 
#ifndef ROZOFS_FUSE_H
#define ROZOFS_FUSE_H

#include <rozofs/core/expgw_common.h>

#include "rozofsmount.h"

void af_unix_fuse_scheduler_entry_point(uint64_t current_time);
#define ROZOFS_FUSE_CTX_MAX 64
extern ruc_obj_desc_t  rozofs_lookup_queue[];  /**< pending list of the lookup */


/**
*  RozoFS IOCTL 7
*   set the value for ra_pages, max_background & congestion_threshold
*/
#define ROZOFS_RA_PAGE 1
#define ROZOFS_MAX_BACKGROUND 2 
#define ROZOFS_CONGESTION_THRESHOLD 4
typedef struct _rozofs_cnx_param_t
{
   int read;  /**< assert to 1 for read and 0 for write */
   unsigned dev; /** read only */
   unsigned to_set;
   unsigned long ra_pages; /* read/write */
   unsigned max_background;  /* read/write */
   unsigned congestion_threshold;  /* read/write */
} rozofs_cnx_param_t;


typedef enum
{
   RZ_FUSE_WRITE_0 = 0,
   RZ_FUSE_WRITE_1,
   RZ_FUSE_WRITE_2,
   RZ_FUSE_WRITE_3,
   RZ_FUSE_WRITE_4,
   RZ_FUSE_WRITE_5,
   RZ_FUSE_WRITE_6,
   RZ_FUSE_WRITE_7,
   RZ_FUSE_WRITE_8,
   RZ_FUSE_WRITE_9,
   RZ_FUSE_WRITE_10,
   RZ_FUSE_WRITE_MAX
}  rozofs_write_merge_t;

typedef struct _rozofs_fuse_read_write_stats
{
    uint64_t   flush_buf_cpt;   /**< number of times the file descriptor buffer is flushed  */
    uint64_t   readahead_cpt;   /**< number of times readahead is called                    */
    uint64_t   read_req_cpt;    /**< number of times a read request is sent to storio       */
    uint64_t   read_fuse_cpt;    /**< number of times read request is received from fuse       */
    uint64_t   big_write_cpt;    /**< big write counter: greater or equal to 256K       */
    uint64_t   big_write_ioctl_cpt;    /**< big write counter: greater or equal to 256K  done with ioctl     */
    uint64_t   small_write_ioctl_cpt;    /**< small write counter: greater or equal to 256K  done with ioctl     */
}  rozofs_fuse_read_write_stats;

#define ROZOFS_PAGE_SZ  4096

#define ROZOFS_FUSE_NB_OF_BUSIZE_SECTION_MAX  ((512*1024)/ROZOFS_PAGE_SZ) /**< 64 sections of BUFSIZE  */
extern uint64_t rozofs_write_buf_section_table[];
extern uint64_t rozofs_read_buf_section_table[];
extern rozofs_fuse_read_write_stats  rozofs_fuse_read_write_stats_buf;
extern int fuse_sharemem_enable;

 /**
 * Must be the same as sys_recv_pf_t
 */
 typedef void (*fuse_end_tx_recv_pf_t)(void *tx_ctx,void *usr_param);

typedef struct _rozofs_fuse_conf_t
{
   uint16_t debug_port;   /**< port value to be used by rmonitor  */
   uint16_t instance;     /**< rozofsmount instance: needed when more than 1 rozofsmount run the same server and exports the same filesystem */
   void     *se;          /**< pointer to the session context     */
   void    *ch;           /**< pointer to the channel context     */
   void    *exportclt;           /**< pointer to the exportd conf     */
   int      max_transactions; /**< max number of simultaneous transactions */
} rozofs_fuse_conf_t;

#define ROZOFS_MAX_LKUP_QUEUE   16
#define ROZOFS_MAX_PENDING_LKUP 32
typedef struct _rozofs_fuse_lookup_entry_t
{
  fuse_req_t req;
  fuse_ino_t ino;
  int        trc_idx;
  int        flags;
} rozofs_fuse_lookup_entry_t;

/**
* fuse request context
*
*  Caution: need to double checked that the pointer are pointer
* either to the fuse message or a context allocated by fuse but not
* a pointer on a local variable
*/
typedef struct _rozofs_fuse_save_ctx_t
{
   ruc_obj_desc_t link;   /**< uwe to queue to context on the file_t structure */
   char  fct_name[64]  ;        /**< for trace purpose    */
   void  *buf_ref;        /**< pointer to the mabagement part of the buffer    */
   ruc_obj_desc_t link_req;   /**< use to queue the fuse request relative to the same object */
   fuse_req_t req;  /**< fuse request  */
   fuse_ino_t ino;  /**< fuse inode input argument  */
   fuse_ino_t parent;
   int  len;       /**< length of name (without \0) */
   int  lookup_flags;
   char *name;
   fuse_ino_t newparent;
   char *newname;
   struct fuse_file_info *fi;
   struct flock *flock;
   int    sleep;
   int    deferred_fuse_write_response;
   struct stat *stbuf;          /**< pointer to the setattr attributes */
   mode_t mode;
   off_t off;
   size_t size;
   int to_set;
   dev_t rdev;
   uint64_t time;
   void     *kernel_fuse_write_request;   /**< pointer to the fuse kernel request context for BIG WRITE with threads */
   dirbuf_t db;
   fuse_end_tx_recv_pf_t proc_end_tx_cbk;   /**< callback that must be call at end of transaction (mainly used by write/flush and close */ 
   uint64_t buf_flush_offset;               /**< offset of the first byte to flush    */
   uint32_t buf_flush_len;               /**< length of the data flush to disk    */
   uint32_t readahead;                   /**< assert to 1 for readahead case */
   void     *shared_buf_ref;             /**< reference of the shared buffer (used for STORCLI READ */
   int       trc_idx;                    /**< trace index */
   int       lkup_cpt;
   rozofs_fuse_lookup_entry_t lookup_tb[ROZOFS_MAX_PENDING_LKUP];
   /*
   ** Parameters specific to the exportd gateway management
   */
   expgw_tx_routing_ctx_t expgw_routing_ctx; 
   
 } rozofs_fuse_save_ctx_t;
 
 
  /**
* rozofs fuse data structure
*/
typedef struct _rozofs_fuse_ctx_t
{
   void *fuseReqPoolRef;     /**< reference of save fuse context pool     */
   uint32_t bufsize;          /**< size of the request buffer              */
   struct fuse_chan *ch;     /**< channel reference                       */
   struct fuse_session *se;  /**< fuse session                            */
   int    fd;                /**< /dev/fuse file descriptor               */
   void   *connectionId;     /**< socket controller reference             */
   int     congested;        /**< assert to 1 when the transmitter is congested  */
   char   *buf_fuse_req_p;   /**< fuse request buffer                      */
   uint32_t initBufCount;    /**< Number of buffer at initialization       */
   int     data_xon;         /**< assert to one when there is enough buffer on storcli side  */
   int     dir_attr_invalidate;   /**< assert to one for directory invalidate on mkdir/mknod/unlink/create and rmdir  */
   int     ioctl_supported;  /**< assert to 1 if ioctl is supported for xon/xoff */

} rozofs_fuse_ctx_t;
 
 
 extern rozofs_fuse_ctx_t  *rozofs_fuse_ctx_p ;  /**< pointer to the rozofs_fuse saved contexts   */

/**
* Prototypes
*/
/*
**__________________________________________________________________________
*/
/**
*  Init of the pseudo fuse thread 

  @param ch : initial channel
  @param se : initial session
  @param rozofs_fuse_buffer_count : number of request buffers  
  
  @retval 0 on success
  @retval -1 on error
*/
int rozofs_fuse_init(struct fuse_chan *ch,struct fuse_session *se,
                     int rozofs_fuse_buffer_count);

/*_______________________________________________________________________
*/
/**
*  This function is the entry point for setting rozofs in non-blocking mode

   @param args->ch: reference of the fuse channnel
   @param args->se: reference of the fuse session
   @param args->max_transactions: max number of transactions that can be handled in parallel
   
   @retval -1 on error
   @retval : no retval -> only on fatal error

*/
int rozofs_stat_start(void *args);

 /*
 *________________________________________________________
 */
 /*
 ** API to be called for stopping rozofsmount
 
  @param none
  @retval none
*/
 void rozofs_exit();


 struct rozofs_fuse_in_header {
	uint32_t	len;
	uint32_t	opcode;
	uint64_t	unique;
	uint64_t	nodeid;
	uint32_t	uid;
	uint32_t	gid;
	uint32_t	pid;
	uint32_t	padding;
};

#define ROZOFS_LOOKUP_OPEN   0x100
#define ROZOFS_LOOKUP_CREATE 0x200
#define ROZOFS_LOOKUP_EXCL   0x400
#define ROZOFS_LOOKUP_RENAME_TARGET 0x800

/**
* That function is intended to detect if the kernel has
  provided the inode reference of the child. When it is done
  the inode value follows the name of the object

*/  
static inline int rozofs_check_extra_inode_in_lookup(char *args,int *len)
{
   int local_len;
   struct rozofs_fuse_in_header  *p;
   /*
   ** get the length of the input string
   */
   local_len = strlen(args);
   *len = local_len;
   
   local_len +=1;
   p= (struct rozofs_fuse_in_header*)(args - sizeof(struct rozofs_fuse_in_header));
   local_len = (int)p->len - sizeof(struct rozofs_fuse_in_header) -local_len;
   return local_len;
}

/**
* That function is intended to detect if the kernel has
  provided the inode reference of the child. When it is done
  the inode value follows the name of the object

*/  
static inline int rozofs_check_extra_flags_in_lookup(char *args,int *len)
{
   int local_len;
   struct rozofs_fuse_in_header  *p;
   /*
   ** get the length of the input string
   */
   local_len = strlen(args);
   *len = local_len;
   
   local_len +=1;
   p= (struct rozofs_fuse_in_header*)(args - sizeof(struct rozofs_fuse_in_header));
   local_len = (int)p->len - sizeof(struct rozofs_fuse_in_header) -local_len;
   if (local_len >= sizeof(unsigned int)) return 1;
   return 0;
}

#endif
