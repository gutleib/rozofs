/*
 *
 *	Headerfile of quota interactions with system - filenames, fstab...
 *
 */

#ifndef GUARD_QUOTASYS_H
#define GUARD_QUOTASYS_H

#include <sys/types.h>
#include <stdint.h>
#include <linux/quota.h>
#include <syslog.h>
 #include <stdarg.h>
#include <libconfig.h>
#include "config.h"
#include "export.h"
#include "monitor.h"
#include "econfig.h"
#include <rozofs/rpc/export_profiler.h>

#define MAXNAMELEN 64		/* Maximal length of user/group name */
#define MAXTIMELEN 40		/* Maximal length of time string */
#define MAXNUMLEN 32		/* Maximal length of number */

/* Size of blocks in which are counted size limits in generic utility parts */
#define QUOTABLOCK_BITS 10
#define QUOTABLOCK_SIZE (1 << QUOTABLOCK_BITS)

/* Flags for formatting time */
#define TF_ROUND 0x1		/* Should be printed time rounded? */
#define qsize_t int64_t
#define qid_t int
#define _(x) 	(x)

/* Conversion routines from and to quota blocks */
#define qb2kb(x) ((x) << (QUOTABLOCK_BITS-10))
#define kb2qb(x) ((x) >> (QUOTABLOCK_BITS-10))
#define toqb(x) (((x) + QUOTABLOCK_SIZE - 1) >> QUOTABLOCK_BITS)
/*
 *	Exported functions
 */

/* Convert quota type to written form */
char *type2name(int);

/* Convert username to uid */
uid_t user2uid(char *, int flag, int *err);

/* Convert groupname to gid */
gid_t group2gid(char *, int flag, int *err);

/* Convert user/groupname to id */
int name2id(char *name, int qtype, int flag, int *err);

/* Convert uid to username */
int uid2user(uid_t, char *);

/* Convert gid to groupname */
int gid2group(gid_t, char *);

/* Convert id to user/group name */
int id2name(int id, int qtype, char *buf);

/* Possible default passwd handling */
#define PASSWD_FILES 0
#define PASSWD_DB 1
/* Parse /etc/nsswitch.conf and return type of default passwd handling */
int passwd_handling(void);

/* Convert quota format name to number */
int name2fmt(char *str);

/* Convert quota format number to name */
char *fmt2name(int fmt);

/* Convert utility to kernel format numbers */
int util2kernfmt(int fmt);

/* Convert time difference between given time and current time to printable form */
void difftime2str(time_t, char *);

/* Convert time to printable form */
void time2str(time_t, char *, int);

/* Convert number and units to time in seconds */
int str2timeunits(time_t, char *, time_t *);

/* Convert number in quota blocks to short printable form */
void space2str(qsize_t, char *, int);

/* Convert number to short printable form */
void number2str(unsigned long long, char *, int);

/* Return pointer to given mount option in mount option string */
char *str_hasmntopt(const char *optstring, const char *opt);

/* create an af_unix socket
*/
int quota_af_unix_sock_create(char *nameOfSocket,int size);
/**
*   delete an af_unix socket
*/
int quota_af_unix_sock_delete(char *nameOfSocket,int fd);

/*
**__________________________________________________________________
*/   
/*
** search for the eid of the export
  
    @param eid: eid to search
    
    @retval <> NULL pathname of the export
    @retval NULL eid not found
*/
char * econfig_get_export_path(econfig_t *config,int eid);
/*
**__________________________________________________________________
*/   
void *smalloc(size_t size);
void *srealloc(void *ptr, size_t size);
void sstrncpy(char *d, const char *s, size_t len);
void sstrncat(char *d, const char *s, size_t len);
char *sstrdup(const char *s);

/*
 * Convert number with unit to time in seconds
 */
int str2timeunits(time_t num, char *unit, time_t *res);
/*
 * Convert time to printable form
 */
void time2str(time_t seconds, char *buf, int flags);
/*
 * Convert time difference of seconds and current time
 */
void difftime2str(time_t seconds, char *buf);
/*
 *  Convert number to some nice short form for printing
 */
void number2str(unsigned long long num, char *buf, int format);
/*
 * Convert number in quota blocks to some nice short form for printing
 */
void space2str(int64_t space, char *buf, int format);

void errstr(char *fmtstr, ...);
void version(void);
void gettexton(void);
void die(int ret, char *fmtstr, ...);
int export_config_read(econfig_t *config, const char *fname);
int quota_wait_response(rozofs_qt_header_t *rsp_buf, int length,int fd,int transaction_id);
#endif /* GUARD_QUOTASYS_H */
