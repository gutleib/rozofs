/*
 *
 *	Set disk quota from command line 
 *
 *	Jan Kara <jack@suse.cz> - sponsored by SuSE CR
 */



#include <sys/types.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <time.h>
#include <ctype.h>

//#include "pot.h"
//#include "quotaops.h"
//#include "common.h"
//#include "quotasys.h"
#include <libconfig.h>
#include <src/exportd/rozofs_quota.h>
#include <src/exportd/rozofs_quota_api.h>
#include <src/exportd/rozofs_quota_intf.h>
#include <rozofs/core/disk_table_service.h>
#include <ctype.h>
#include "config.h"
#include "export.h"
#include "monitor.h"
#include "econfig.h"
#include "quotasys.h"
#include <rozofs/rpc/export_profiler.h>

#define FL_USER 1
#define FL_GROUP 2
#define FL_RPC 4
#define FL_ALL 8
#define FL_PROTO 16
#define FL_GRACE 32
#define FL_INDIVIDUAL_GRACE 64
#define FL_BATCH 128
#define FL_NUMNAMES 256
#define FL_NO_MIXED_PATHS 512
#define FL_CONTINUE_BATCH 1024
#define FL_SHARE 2048	/* Print share information */

/* Size of blocks in which are counted size limits in generic utility parts */
#define QUOTABLOCK_BITS 10
#define QUOTABLOCK_SIZE (1 << QUOTABLOCK_BITS)

/* Conversion routines from and to quota blocks */
#define qb2kb(x) ((x) << (QUOTABLOCK_BITS-10))
#define kb2qb(x) ((x) >> (QUOTABLOCK_BITS-10))
#define toqb(x) (((x) + QUOTABLOCK_SIZE - 1) >> QUOTABLOCK_BITS)

#define EXPORT_DEFAULT_PATH "/etc/rozofs/export.conf"

export_one_profiler_t * export_profiler[1];
uint32_t                export_profiler_eid;

struct util_dqblk {
	qsize_t dqb_ihardlimit;
	qsize_t dqb_isoftlimit;
	qsize_t dqb_curinodes;
	qsize_t dqb_bhardlimit;
	qsize_t dqb_bsoftlimit;
	qsize_t dqb_curspace;
	time_t dqb_btime;
	time_t dqb_itime;			/* Format specific dquot information */
};

static int flags;
static char **mnt;
char *progname;
static int mntcnt;
static qid_t protoid, id;
static struct util_dqblk toset;
static pid_t my_pid;
static int fd=-1;
struct  sockaddr_un sock_path;
int quota_transactionId=1;

char *confname=NULL;
int rozofs_no_site_file = 0;

/* Print usage information */
static void usage(void)
{

	char *ropt = "";
	errstr(_("Usage:\n\
  rozo_setquota [-u <user>|-g <group>|-s <project>]\n\
\t<block-softlimit> <block-hardlimit> <inode-softlimit> <inode-hardlimit> -a|<eid> [-f exportconf]...\n\
  rozo_setquota {-u <user>|-g <group>|-s <project>}  <-p protouser|protogroup>  -a|<eid> [-f exportconf]..\n\
  rozo_setquota {-u <user>|-g <group>|-s <project>} -b [-c] -a|<eid> [-f exportconf]...\n\
  rozo_setquota [-u|-g|-s] -t <blockgrace> <inodegrace> -a|<eid> [-f exportconf]...\n\
  rozo_setquota {-u <user>|-g <group>|-s <project>} -T <blockgrace> <inodegrace> -a|<eid> [-f exportconf]...\n\n\
\
  Block limits are KB units. \n\
-u, --user                 set limits for user\n\
-g, --group                set limits for group\n\
-s, --project              set limits for project\n\
-a, --all                  set limits for all filesystems\n\
    --always-resolve       always try to resolve name, even if is\n\
                           composed only of digits\n\
-f, --file=exportconf      specify the full pathname of the exportd configuration\n\
-p, --prototype=protoname  copy limits from user/group\n\
-b, --batch                read limits from standard input\n\
-c, --continue-batch       continue in input processing in case of an error\n"), ropt);
	fputs(_("-t, --edit-period          edit grace period\n\
-T, --edit-times           edit grace times for user/group\n\
-h, --help                 display this help text and exit\n\
-V, --version              display version information and exit\n\n"), stderr);
//	fprintf(stderr, _("Bugs to: %s\n"), MY_EMAIL);
	exit(1);
}

/* Convert string to number - print errstr message in case of failure */
static qsize_t parse_unum(char *str, char *msg)
{
	char *errch;
	qsize_t ret = strtoull(str, &errch, 0);

	if (*errch) {
		errstr(_("%s: %s\n"), msg, str);
		usage();
	}
	return ret;
}

/* Convert our flags to quota type */
static inline int flag2type(int flags)
{
	if (flags & FL_USER)
		return USRQUOTA;
	if (flags & FL_GROUP)
		return GRPQUOTA;
	if (flags & FL_SHARE)
		return SHRQUOTA;
	return -1;
}

/*
**__________________________________________________________________
*/
/*
 * Set grace time if needed
 */
 
void update_grace_times(sq_dqblk *q)
{
	time_t now;

	time(&now);
	if (q->rq_bsoftlimit && q->rq_curblocks > q->rq_bsoftlimit) {
		if (!q->rq_btimeleft)
			q->rq_btimeleft = now + ROZOFS_MAX_DQ_TIME ;
	}
	else
		q->rq_btimeleft = 0;
	if (q->rq_fsoftlimit && q->rq_curfiles > q->rq_fsoftlimit) {
		if (!q->rq_ftimeleft)
			q->rq_ftimeleft = now + ROZOFS_MAX_IQ_TIME;
	}
	else
		q->rq_ftimeleft = 0;
}

/* Parse options of setquota */
static void parse_options(int argcnt, char **argstr)
{
	int ret, otherargs;
	char *protoname = NULL;
	int flagtype;

	char *opts = "ghp:usVf:taTbc";
	struct option long_opts[] = {
		{ "user", 0, NULL, 'u' },
		{ "group", 0, NULL, 'g' },
		{ "project", 0, NULL, 's' },
		{ "prototype", 1, NULL, 'p' },
		{ "all", 0, NULL, 'a' },
		{ "always-resolve", 0, NULL, 256},
		{ "edit-period", 0, NULL, 't' },
		{ "edit-times", 0, NULL, 'T' },
		{ "batch", 0, NULL, 'b' },
		{ "continue", 0, NULL, 'c' },
		{ "file", 1, NULL, 'F' },
		{ "version", 0, NULL, 'V' },
		{ "help", 0, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	while ((ret = getopt_long(argcnt, argstr, opts, long_opts, NULL)) != -1) {
		switch (ret) {
		  case '?':
		  case 'h':
			  usage();
		  case 'g':
			  flags |= FL_GROUP;
			  break;
		  case 'u':
			  flags |= FL_USER;
			  break;
		  case 's':
			  flags |= FL_SHARE;
			  break;
		  case 'p':
			  flags |= FL_PROTO;
			  protoname = optarg;
			  break;
		  case 'r':
			  flags |= FL_RPC;
			  break;
		  case 'm':
			  flags |= FL_NO_MIXED_PATHS;
			  break;
		  case 'a':
			  flags |= FL_ALL;
			  break;
		  case 256:
			  flags |= FL_NUMNAMES;
			  break;
		  case 't':
			  flags |= FL_GRACE;
			  break;
		  case 'b':
			  flags |= FL_BATCH;
			  break;
		  case 'c':
			  flags |= FL_CONTINUE_BATCH;
			  break;
		  case 'T':
			  flags |= FL_INDIVIDUAL_GRACE;
			  break;
		  case 'f':
			  confname = optarg;
			  break;
		  case 'V':
			  version();
			  exit(0);
		}
	}

	flagtype = flags & (FL_USER | FL_GROUP | FL_SHARE);
	switch (flagtype)
	{
	  case 0:
	    flags |= FL_USER;
	    break;
	  case FL_USER:
	  case FL_GROUP:
	  case FL_SHARE:
	    break;
	  default:
	    errstr(_("Group and user or share quotas cannot be used together.\n"));
	    usage();
	    break;
	}
	if (flags & FL_PROTO && flags & FL_GRACE) {
		errstr(_("Prototype user has no sense when editing grace times.\n"));
		usage();
	}
	if (flags & FL_INDIVIDUAL_GRACE && flags & FL_GRACE) {
		errstr(_("Cannot set both individual and global grace time.\n"));
		usage();
	}
	if (flags & FL_BATCH && flags & (FL_GRACE | FL_INDIVIDUAL_GRACE)) {
		errstr(_("Batch mode cannot be used for setting grace times.\n"));
		usage();
	}
	if (flags & FL_BATCH && flags & FL_PROTO) {
		errstr(_("Batch mode and prototype user cannot be used together.\n"));
		usage();
	}
	if (flags & FL_RPC && (flags & (FL_GRACE | FL_INDIVIDUAL_GRACE))) {
		errstr(_("Cannot set grace times over RPC protocol.\n"));
		usage();
	}
	if (flags & FL_GRACE)
		otherargs = 2;
	else if (flags & FL_INDIVIDUAL_GRACE)
		otherargs = 3;
	else if (flags & FL_BATCH)
		otherargs = 0;
	else {
		otherargs = 1;
		if (!(flags & FL_PROTO))
			otherargs += 4;
	}
	if (optind + otherargs > argcnt) {
		errstr(_("Bad number of arguments.\n"));
		usage();
	}

	if (!(flags & (FL_GRACE | FL_BATCH))) {
		id = name2id(argstr[optind++], flag2type(flags), !!(flags & FL_NUMNAMES), NULL);
		if (!(flags & (FL_GRACE | FL_INDIVIDUAL_GRACE | FL_PROTO))) {
			toset.dqb_bsoftlimit = parse_unum(argstr[optind++], _("Bad block softlimit"));
			toset.dqb_bhardlimit = parse_unum(argstr[optind++], _("Bad block hardlimit"));
			toset.dqb_isoftlimit = parse_unum(argstr[optind++], _("Bad inode softlimit"));
			toset.dqb_ihardlimit = parse_unum(argstr[optind++], _("Bad inode hardlimit"));
		}
		else if (flags & FL_PROTO)
			protoid = name2id(protoname, flag2type(flags), !!(flags & FL_NUMNAMES), NULL);
	}
	if (flags & FL_GRACE) {
		toset.dqb_btime = parse_unum(argstr[optind++], _("Bad block grace time"));
		toset.dqb_itime = parse_unum(argstr[optind++], _("Bad inode grace time"));
	}
	else if (flags & FL_INDIVIDUAL_GRACE) {
		time_t now;

		time(&now);
		if (!strcmp(argstr[optind], _("unset"))) {
			toset.dqb_btime = 0;
			optind++;
		}
		else
			toset.dqb_btime = now + parse_unum(argstr[optind++], _("Bad block grace time"));
		if (!strcmp(argstr[optind], _("unset"))) {
			toset.dqb_itime = 0;
			optind++;
		}
		else
			toset.dqb_itime = now + parse_unum(argstr[optind++], _("Bad inode grace time"));
	}
	if (!(flags & FL_ALL)) {
		mntcnt = argcnt - optind;
		mnt = argstr + optind;
		if (!mntcnt) {
			errstr(_("Mountpoint not specified.\n"));
			usage();
		}
	}
}

/*
**__________________________________________________________________
*/
/*
** read quota
*/
static int read_quota(int eid,rozofs_getquota_rsp_t *msg_rsp)
{

	rozofs_getquota_req_t msg_req;

	memset(&msg_req,0,sizeof(rozofs_getquota_req_t));
	int ret = 0;

	msg_req.eid      = eid;
	msg_req.gqa_id   = id;
	msg_req.gqa_type = flag2type(flags);
	msg_req.hdr.opcode = ROZOFS_QUOTA_GETQUOTA;
	msg_req.hdr.length = sizeof(rozofs_setquota_req_t);
	msg_req.hdr.pid = my_pid;
	ret = rozofs_qt_thread_intf_send((rozofs_qt_header_t*)&msg_req,eid,fd);
	if (ret != 0) return ret;
	/*
	** wait the response
	*/
	ret = quota_wait_response((rozofs_qt_header_t*)msg_rsp,sizeof(rozofs_getquota_rsp_t),fd,msg_req.hdr.xid);
	if (ret < 0)
	{
	  errstr("wait response failure:%s\n",strerror(errno));
	  return ret;
	}
	/*
	** check the status in the response message
	*/
	ret = msg_rsp->status ;
	if (ret != 0)
	{
	  errstr("set quota failure:%s\n",strerror(msg_rsp->errcode));
	  return ret;	
	
	}
	return ret;

}
/*
**__________________________________________________________________
*/
static int setlimits(int eid,int grace)
{
	rozofs_setquota_req_t msg_req;
	rozofs_setquota_rsp_t msg_rsp;
	rozofs_getquota_rsp_t msg_get_rsp;
	
	memset(&msg_req,0,sizeof(rozofs_setquota_req_t));
	int ret = 0;
	/*
	** read the current quota
	*/
	ret = read_quota(eid,&msg_get_rsp);
	if (ret < 0) return ret;
	
	msg_req.sqa_dqblk.rq_curblocks = msg_get_rsp.quota_data.rq_curblocks;
	msg_req.sqa_dqblk.rq_curfiles  = msg_get_rsp.quota_data.rq_curfiles;
	if (grace == 0)
	{
	  msg_req.sqa_dqblk.rq_btimeleft = msg_get_rsp.quota_data.rq_btimeleft;
	  msg_req.sqa_dqblk.rq_ftimeleft = msg_get_rsp.quota_data.rq_ftimeleft;
	}
	else
	{
	  msg_req.sqa_dqblk.rq_btimeleft = toset.dqb_btime;
	  msg_req.sqa_dqblk.rq_ftimeleft = toset.dqb_itime;
	}

	if (grace == 0)
	{
	  msg_req.sqa_dqblk.rq_bsoftlimit = toset.dqb_bsoftlimit;
	  msg_req.sqa_dqblk.rq_bhardlimit = toset.dqb_bhardlimit;
	  msg_req.sqa_dqblk.rq_fsoftlimit = toset.dqb_isoftlimit;
	  msg_req.sqa_dqblk.rq_fhardlimit = toset.dqb_ihardlimit;
	}
	else
	{
	  msg_req.sqa_dqblk.rq_bsoftlimit = msg_get_rsp.quota_data.rq_bsoftlimit;
	  msg_req.sqa_dqblk.rq_bhardlimit = msg_get_rsp.quota_data.rq_bhardlimit;
	  msg_req.sqa_dqblk.rq_fsoftlimit = msg_get_rsp.quota_data.rq_fsoftlimit;
	  msg_req.sqa_dqblk.rq_fhardlimit = msg_get_rsp.quota_data.rq_fhardlimit;		
	}
#if 0
	/*
	** update grace time if needed
	*/
	update_grace_times(&msg_req.sqa_dqblk);
#endif
	msg_req.eid = eid;
	msg_req.sqa_id = id;
	msg_req.sqa_type = flag2type(flags);
	msg_req.sqa_qcmd = QIF_LIMITS;
	if (grace != 0) msg_req.sqa_qcmd = msg_req.sqa_qcmd | QIF_TIMES;
	msg_req.hdr.opcode = ROZOFS_QUOTA_SETQUOTA;
	msg_req.hdr.length = sizeof(rozofs_setquota_req_t);
	msg_req.hdr.pid = my_pid;
	
	ret = rozofs_qt_thread_intf_send((rozofs_qt_header_t*)&msg_req,eid,fd);
	if (ret != 0) return ret;
	/*
	** wait the response
	*/
	ret = quota_wait_response((rozofs_qt_header_t*)&msg_rsp,sizeof(rozofs_setquota_rsp_t),fd,msg_req.hdr.xid);
	if (ret < 0)
	{
	  errstr("wait response failure:%s\n",strerror(errno));
	  return ret;
	}
	/*
	** check the status in the response message
	*/
	ret = msg_rsp.status ;
	if (ret != 0)
	{
	  errstr("set quota failure:%s\n",strerror(msg_rsp.errcode));
	  return ret;	
	
	}
	return ret;
}
#define MAXLINELEN 65536
/*
**__________________________________________________________________
*/
/* Read & parse one batch entry */
static int read_entry(qid_t *id, qsize_t *isoftlimit, qsize_t *ihardlimit, qsize_t *bsoftlimit, qsize_t *bhardlimit)
{
	static int line = 0;
	char name[MAXNAMELEN+1];
	char linebuf[MAXLINELEN], *chptr;
	unsigned long is, ih, bs, bh;
	int ret;

	while (1) {
		line++;
		if (!fgets(linebuf, sizeof(linebuf), stdin))
			return -1;
		if (linebuf[strlen(linebuf)-1] != '\n')
			die(1, _("Line %d too long.\n"), line);
		/* Comment? */
		if (linebuf[0] == '#')
			continue;
		/* Blank line? */
		chptr = linebuf;
		while (isblank(*chptr))
			chptr++;
		if (*chptr == '\n')
			continue;
		ret = sscanf(chptr, "%s %lu %lu %lu %lu", name, &bs, &bh, &is, &ih);
		if (ret != 5) {
			errstr(_("Cannot parse input line %d.\n"), line);
			if (!(flags & FL_CONTINUE_BATCH))
				die(1, _("Exitting.\n"));
			errstr(_("Skipping line.\n"));
			continue;
		}
		*id = name2id(name, flag2type(flags), !!(flags & FL_NUMNAMES), &ret);
		if (ret) {
			errstr(_("Unable to resolve name '%s' on line %d.\n"), name, line);
			if (!(flags & FL_CONTINUE_BATCH))
				die(1, _("Exitting.\n"));
			errstr(_("Skipping line.\n"));
			continue;
		}
		break;
	}
	*isoftlimit = is;
	*ihardlimit = ih;
	*bsoftlimit = bs;
	*bhardlimit = bh;
	return 0;
}
/*
**__________________________________________________________________
*/
/* Set user limits in batch mode */
static int batch_setlimits(int eid)
{
	rozofs_setquota_req_t msg_req;
	rozofs_setquota_rsp_t msg_rsp;
	
	memset(&msg_req,0,sizeof(rozofs_setquota_req_t));
	qsize_t bhardlimit, bsoftlimit, ihardlimit, isoftlimit;
	qid_t id;
	int ret = 0;

	while (!read_entry(&id, &isoftlimit, &ihardlimit, &bsoftlimit, &bhardlimit)) {
	        
	    memset(&msg_req,0,sizeof(rozofs_setquota_req_t));
	    ret = 0;

	   msg_req.sqa_dqblk.rq_bsoftlimit = bsoftlimit;
	   msg_req.sqa_dqblk.rq_bhardlimit = bhardlimit;
	   msg_req.sqa_dqblk.rq_fsoftlimit = isoftlimit;
	   msg_req.sqa_dqblk.rq_fhardlimit = ihardlimit;
	   //update_grace_times(q);
	   msg_req.eid = eid;
	   msg_req.sqa_id = id;
	   msg_req.sqa_type = flag2type(flags);
	   msg_req.sqa_qcmd = QIF_LIMITS;
	   msg_req.hdr.opcode = ROZOFS_QUOTA_SETQUOTA;
	   msg_req.hdr.length = sizeof(rozofs_setquota_req_t);
	   msg_req.hdr.pid = my_pid;

	   ret = rozofs_qt_thread_intf_send((rozofs_qt_header_t*)&msg_req,eid,fd);
	   if (ret != 0) return ret;
	   /*
	   ** wait the response
	   */
	   ret = quota_wait_response((rozofs_qt_header_t*)&msg_rsp,sizeof(rozofs_setquota_rsp_t),fd,msg_req.hdr.xid);
	   if (ret < 0)
	   {
	     errstr("wait response failure:%s\n",strerror(errno));
	     return ret;
	   }
	   /*
	   ** check the status in the response message
	   */
	   ret = msg_rsp.status ;
	   if (ret != 0)
	   {
	     errstr("set quota failure:%s\n",strerror(msg_rsp.errcode));
	     return ret;	

	   }
	}
	return ret;
}

/*
**__________________________________________________________________
*/
int setgraces(int eid)
{

     rozofs_setgrace_req_t msg_req;
     rozofs_setgrace_rsp_t msg_rsp;
     int ret = 0;

     memset(&msg_req,0,sizeof(rozofs_setgrace_req_t));

     msg_req.sqa_dqblk.rq_btimeleft = toset.dqb_btime;
     msg_req.sqa_dqblk.rq_ftimeleft = toset.dqb_itime;
     msg_req.eid = eid;
     msg_req.sqa_id = -1;
     msg_req.sqa_type = flag2type(flags);
     msg_req.sqa_qcmd = QIF_TIMES;
     msg_req.hdr.opcode = ROZOFS_QUOTA_SETGRACE;
     msg_req.hdr.length = sizeof(rozofs_setquota_req_t);
     msg_req.hdr.pid = my_pid;

     ret = rozofs_qt_thread_intf_send((rozofs_qt_header_t*)&msg_req,eid,fd);
     if (ret != 0) return ret;
     /*
     ** wait the response
     */
     ret = quota_wait_response((rozofs_qt_header_t*)&msg_rsp,sizeof(rozofs_setquota_rsp_t),fd,msg_req.hdr.xid);
     if (ret < 0)
     {
       errstr("wait response failure:%s\n",strerror(errno));
       return ret;
     }
     /*
     ** check the status in the response message
     */
     ret = msg_rsp.status ;
     if (ret != 0)
     {
       errstr("set grace time failure:%s\n",strerror(msg_rsp.errcode));
       return ret;	

     }
     return ret;
}
/*
**__________________________________________________________________
*/
int main(int argc, char **argv)
{
	int ret=0;
	int i;
	int eid;
	char *errch;

        confname = strdup(EXPORT_DEFAULT_PATH);
	gettexton();
	progname = basename(argv[0]);

	parse_options(argc, argv);
#if 0

	if (flags & FL_ALL)
		handles = create_handle_list(0, NULL, flag2type(flags), fmt,
			(flags & FL_NO_MIXED_PATHS) ? 0 : IOI_NFS_MIXED_PATHS,
			(flags & FL_RPC) ? 0 : MS_LOCALONLY);
	else
		handles = create_handle_list(mntcnt, mnt, flag2type(flags), fmt,
			(flags & FL_NO_MIXED_PATHS) ? 0 : IOI_NFS_MIXED_PATHS,
			(flags & FL_RPC) ? 0 : MS_LOCALONLY);

#endif

        /*
	** create the af_unix socket
	*/
	my_pid = getpid();
	rozofs_qt_set_socket_name_with_pid_of_requester(&sock_path,(int)my_pid);
	fd = af_unix_sock_create(sock_path.sun_path, (300*1024));
	if (fd < 0)
	{
	   errstr(_("Cannot create AF_UNIX socket.\n"));
	   exit(1);
	}

	for (i = 0; i <mntcnt; i++)
	{
	   errch = NULL;
	   eid = strtoul(mnt[i], &errch, 0);
           if (!*errch)		/* Is name number - we got directly gid? */
	   if (errch != NULL) 
	   {
	      if (*errch != 0)
	      {
	        continue;
	      }
	   }

	   if (flags & FL_GRACE)
	   {
		   ret = setgraces(eid);
	   }
#if 1
	   else if (flags & FL_INDIVIDUAL_GRACE)
		   ret = setlimits(eid,1);
#endif
	   else 

	   if (flags & FL_BATCH)
		   ret = batch_setlimits(eid);
	   else

		   ret = setlimits(eid,0);
        }
	/*
	** close the af_unix socket
	*/
	af_unix_sock_delete(sock_path.sun_path,fd);
	return ret ? 1 : 0;
}
