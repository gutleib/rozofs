/*
 * Copyright (c) 1980, 1990 Regents of the University of California. All
 * rights reserved.
 * 
 * This code is derived from software contributed to Berkeley by Robert Elz at
 * The University of Melbourne.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met: 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer. 2.
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution. 3. All advertising
 * materials mentioning features or use of this software must display the
 * following acknowledgement: This product includes software developed by the
 * University of California, Berkeley and its contributors. 4. Neither the
 * name of the University nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific
 * prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "config.h"

/*
 * Turn quota on/off for a filesystem.
 */
#include <stdio.h>
#include <errno.h>
#include <getopt.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <linux/quota.h>
 #include <stdarg.h>
#include <libconfig.h>
#include <src/exportd/rozofs_quota.h>
#include <src/exportd/rozofs_quota_api.h>
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
#define FL_VERBOSE 4
#define FL_ALL 8
#define FL_STAT 16
#define FL_OFF 32
#define FL_SHARE 64	/* Print share information */

#define EXPORT_DEFAULT_PATH "/etc/rozofs/export.conf"
#define STATEFLAG_ON		0x01
#define STATEFLAG_OFF		0x02
#define STATEFLAG_ALL		0x04

static int flags;
char *progname;
static char **mnt;
static int mntcnt;
static char *xarg = NULL;
char *confname=NULL;
int rozofs_no_site_file = 0;
static int fd=-1;
struct  sockaddr_un sock_path;
uint32_t export_profiler_eid = 0;
export_one_profiler_t * export_profiler[1];
int quota_transactionId=1;


static void usage(void)
{
	errstr(_("Usage:\n\t%s [-gusvp] [-e exportconf] -a\n\
\t%s [-gusvp] [-e exportconf] filesys_id ...\n\n\
-a, --all                %s\n\
-f, --off                turn quotas off\n\
-u, --user               operate on user quotas\n\
-g, --group              operate on group quotas\n\
-s, --project            operate on project quotas\n\
-p, --print-state        print whether quotas are on or off\n\
-e, --exportconf=path       pathname of the export configuration\n\
-v, --verbose            print more messages\n\
-h, --help               display this help text and exit\n\
-V, --version            display version information and exit\n"),
 progname, progname,
 strcmp(progname, "rozo_quotaon") ? _("turn quotas off for all RozoFS filesystems") :
			       _("turn quotas on for all RozoFS filesystems"));
	exit(1);
}

static void parse_options(int argcnt, char **argstr)
{
	int c;
	struct option long_opts[] = {
		{ "all", 0, NULL, 'a' },
		{ "off", 0, NULL, 'f' },
		{ "verbose", 0, NULL, 'v' },
		{ "user", 0, NULL, 'u' },
		{ "group", 0, NULL, 'g' },
		{ "project", 0, NULL, 's' },
		{ "print-state", 0, NULL, 'p' },
		{ "xfs-command", 1, NULL, 'x' },
		{ "exportconf", 1, NULL, 'e' },
		{ "version", 0, NULL, 'V' },
		{ "help", 0, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	while ((c = getopt_long(argcnt, argstr, "afvsugpx:Ve:h", long_opts, NULL)) != -1) {
		switch (c) {
		  case 'a':
			  flags |= FL_ALL;
			  break;
		  case 'f':
			  flags |= FL_OFF;
			  break;
		  case 'g':
			  flags |= FL_GROUP;
			  break;
		  case 'u':
			  flags |= FL_USER;
			  break;
		  case 's':
			  flags |= FL_SHARE;
			  break;
		  case 'v':
			  flags |= FL_VERBOSE;
			  break;
		  case 'x':
			  xarg = optarg;
			  break;
		  case 'p':
			  flags |= FL_STAT;
			  break;
		  case 'e':
			  confname = optarg;
			  break;
		  case 'V':
			  version();
			  exit(0);
		  case 'h':
		  default:
			  usage();
		}
	}
	if ((flags & FL_ALL && optind != argcnt) || (!(flags & FL_ALL) && optind == argcnt)) {
		fputs("Bad number of arguments.\n", stderr);
		usage();
	}
	if (!(flags & (FL_USER | FL_GROUP|FL_SHARE)))
		flags |= FL_USER | FL_GROUP | FL_SHARE;
	if (!(flags & FL_ALL)) {
		mnt = argstr + optind;
		mntcnt = argcnt - optind;
	}
}

int pinfo(char *fmt, ...)
{
	va_list arg;
	int ret;

	if (!(flags & FL_VERBOSE))
		return 0;
	va_start(arg, fmt);
	ret = vprintf(fmt, arg);
	va_end(arg);
	return ret;
}

/*
 *	For both VFS quota formats, need to pass in the quota file;
 *	for XFS quota manager, pass on the -x command line option.
 */
static int newstate(rozofs_qt_export_t *quota_ctx_p, int type,int eid)
{
	int sflags, ret = 0;
	int curstate = 0;
	pid_t my_pid;
	rozofs_setquota_state_req_t msg_req;
	rozofs_setquota_state_rsp_t msg_rsp;
	int cmd = flags & FL_OFF ? ROZOFS_QUOTA_OFF:ROZOFS_QUOTA_ON;

	sflags = flags & FL_OFF ? STATEFLAG_OFF : STATEFLAG_ON;
	if (flags & FL_ALL)
		sflags |= STATEFLAG_ALL;
	
	/*
	** check the current state of the quota
	*/
	curstate = quota_ctx_p->quota_super[type].enable;
	if (curstate)
	{
	   if ( sflags == STATEFLAG_ON)
	   {
		errstr(_("%s quota is already on for eid #%d.\n"),type2name(type),eid);
		return 1;	   
	   }
	}
	else
	{
	   if ( sflags == STATEFLAG_OFF)
	   {
		errstr(_("%s quota is already off for eid #%d.\n"),type2name(type),eid);
		return 1;	   
	   }		
	}
        /*
	** change the state on disk
	*/
	memset(&msg_req,0,sizeof(rozofs_setquota_state_req_t));
	ret = 0;
	my_pid = getpid();
	msg_req.eid      = eid;
	msg_req.sqa_type = type;
	msg_req.cmd = cmd;
	msg_req.hdr.opcode = ROZOFS_QUOTA_SET;
	msg_req.hdr.length = sizeof(rozofs_setquota_state_req_t);
	msg_req.hdr.pid = my_pid;
	ret = rozofs_qt_thread_intf_send((rozofs_qt_header_t*)&msg_req,eid,fd);
	if (ret != 0) return ret;
	/*
	** wait the response
	*/
	ret = quota_wait_response((rozofs_qt_header_t*)&msg_rsp,sizeof(rozofs_setquota_state_rsp_t),fd,msg_req.hdr.xid);
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

/* Print state of quota (on/off) */
static int print_state(rozofs_qt_export_t *quota_ctx_p, int type,int eid)
{
	int on = 0;
        on = quota_ctx_p->quota_super[type].enable;

	printf("%s quota on %s (eid #%d) is %s\n", type2name(type), quota_ctx_p->root_path,eid,
	  on ? "on" : "off");
	
	return on;
}

int main(int argc, char **argv)
{
	int errs = 0;
	char *errch=NULL;
	int i;
        rozofs_qt_export_t *quota_ctx_p = NULL;
	char *pathname;
	int eid;
	int ret;
	
        confname = strdup(EXPORT_DEFAULT_PATH);
	gettexton();

	progname = basename(argv[0]);
	if (strcmp(progname, "rozo_quotaoff") == 0)
		flags |= FL_OFF;
	else if (strcmp(progname, "rozo_quotaon") != 0)
		die(1, _("Name must be rozo_quotaon or rozo_quotaoff not %s\n"), progname);

	parse_options(argc, argv);
	/*
	** read the configuration file of the exports
	*/
	ret = export_config_read(&exportd_config,confname);
	if (ret < 0)
	{
	   printf("Error on reading exportd configuration: %s -> %s\n",confname,strerror(errno));
	   exit(0);
	}
	/*
	** init of the data strcuture needed by quota manager
	*/
	rozofs_qt_init();
        /*
	** create the af_unix socket
	*/
	pid_t my_pid = getpid();
	rozofs_qt_set_socket_name_with_pid_of_requester(&sock_path,(int)my_pid);
	fd = af_unix_sock_create(sock_path.sun_path, (300*1024));
	if (fd < 0)
	{
	   errstr(_("Cannot create AF_UNIX socket.\n"));
	   exit(1);
	}
	/*
	**  check if this must done for all the exports
	*/
	if (flags & FL_ALL)
	{
	   list_t *p;
	   export_config_t *e1= NULL;

	   list_for_each_forward(p, &exportd_config.exports) {
               e1 = list_entry(p, export_config_t, list);
               if (access(e1->root, F_OK) != 0) {
        	   severe("can't access %s: %s.", e1->root, strerror(errno));
        	   continue;
               }
	       pathname = e1->root;
	       quota_ctx_p = rozofs_qt_alloc_context(e1->eid,pathname,0);
	       if (quota_ctx_p == NULL)
	       {
		  printf("fail to create quota data for exportd %d\n",e1->eid);
		  goto error;
	       }
	       if (!(flags & FL_STAT)) {
		       if (flags & FL_GROUP)
			       errs += newstate(quota_ctx_p, GRPQUOTA, e1->eid);
		       if (flags & FL_USER)
			       errs += newstate(quota_ctx_p, USRQUOTA, e1->eid);
		       if (flags & FL_SHARE)
			       errs += newstate(quota_ctx_p, SHRQUOTA, e1->eid);
	       }
	       else {
		       if (flags & FL_GROUP)
			       errs += print_state(quota_ctx_p, GRPQUOTA,e1->eid);
		       if (flags & FL_USER)
			       errs += print_state(quota_ctx_p, USRQUOTA,e1->eid);
		       if (flags & FL_SHARE)
			       errs += print_state(quota_ctx_p, SHRQUOTA,e1->eid);
	       }

	   } 
	   goto out;  	
	}
	/*
	** case of a subset of the exports
	*/
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
	   pathname = econfig_get_export_path(&exportd_config,eid);
	   if (pathname == NULL)
	   {
	      printf("export %d does not exist\n",eid);
	      continue;	   
	   }
	  
	  quota_ctx_p = rozofs_qt_alloc_context(eid,pathname,0);
	  if (quota_ctx_p == NULL)
	  {
	     printf("fail to create quota data for exportd %d\n",eid);
	     goto error;
	  }
	  if (!(flags & FL_STAT)) {
		  if (flags & FL_GROUP)
			  errs += newstate(quota_ctx_p, GRPQUOTA, eid);
		  if (flags & FL_USER)
			  errs += newstate(quota_ctx_p, USRQUOTA, eid);
		  if (flags & FL_SHARE)
			  errs += newstate(quota_ctx_p, SHRQUOTA, eid);
	  }
	  else {
		  if (flags & FL_GROUP)
			  errs += print_state(quota_ctx_p, GRPQUOTA,eid);
		  if (flags & FL_USER)
			  errs += print_state(quota_ctx_p, USRQUOTA,eid);
		  if (flags & FL_SHARE)
			  errs += print_state(quota_ctx_p, SHRQUOTA,eid);
	  }

	}
	/*
	** close the af_unix socket
	*/
out:
	af_unix_sock_delete(sock_path.sun_path,fd);
	return errs;

error:
      /*
      ** close the af_unix socket
      */
      af_unix_sock_delete(sock_path.sun_path,fd);
      exit(-1);

}

