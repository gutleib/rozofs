/*
 *
 *	Interactions of quota with system - filenames, fstab and so on...
 *
 *	Jan Kara <jack@suse.cz> - sponsored by SuSE CR
 */
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <limits.h>
#include <pwd.h>
#include <grp.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <ctype.h>
#include <paths.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdarg.h>
#include <sys/socket.h>
#include <sys/un.h>
#include "rozofs_quota_api.h"
#include "rozofs_quota.h"
#include <linux/quota.h>
#include "quotasys.h"

static int enable_syslog=0;
extern char *progname;
static char extensions[MAXQUOTAS + 2][20] = INITQFNAMES;


/*
 *	Convert type of quota to written representation
 */
char *type2name(int type)
{
	return extensions[type];
}

void use_syslog(void)
{
	openlog(progname,0,LOG_DAEMON);
	enable_syslog=1;
}

static void do_syslog(int level, const char *format, va_list args)
{
	char buf[1024];
	int i, j;
	
	vsnprintf(buf,sizeof(buf),format,args);
	/* This while removes newlines from the log, so that
	 * syslog() will be called once for every line */
	for (i = 0; buf[i]; i = j) {
		for (j = i; buf[j] && buf[j] != '\n'; j++);
		if (buf[j] == '\n')
			buf[j++] = '\0';
		syslog(level, "%s", buf + i);
	}
}

void die(int ret, char *fmtstr, ...)
{
	va_list args;

	va_start(args, fmtstr);
	if (enable_syslog) {
		do_syslog(LOG_CRIT, fmtstr, args);
		syslog(LOG_CRIT, "Exiting with status %d", ret);
	} else {
		fprintf(stderr, "%s: ", progname);
		vfprintf(stderr, fmtstr, args);
	}
	va_end(args);
	exit(ret);
}

void errstr(char *fmtstr, ...)
{
	va_list args;

	va_start(args, fmtstr);
	if (enable_syslog)
		do_syslog(LOG_ERR, fmtstr, args);
	else {
		fprintf(stderr, "%s: ", progname);
		vfprintf(stderr, fmtstr, args);
	}
	va_end(args);
}

void *smalloc(size_t size)
{
	void *ret = malloc(size);

	if (!ret) {
		fputs("Not enough memory.\n", stderr);
		exit(3);
	}
	return ret;
}

void *srealloc(void *ptr, size_t size)
{
	void *ret = realloc(ptr, size);

	if (!ret) {
		fputs("Not enough memory.\n", stderr);
		exit(3);
	}
	return ret;
}

void sstrncpy(char *d, const char *s, size_t len)
{
	strncpy(d, s, len);
	d[len - 1] = 0;
}

void sstrncat(char *d, const char *s, size_t len)
{
	strncat(d, s, len);
	d[len - 1] = 0;
}

char *sstrdup(const char *s)
{
	char *r = strdup(s);

	if (!r) {
		puts("Not enough memory.");
		exit(3);
	}
	return r;
}

void version(void)
{
  printf("%s\n", VERSION);
}


/*
 *	Convert name to uid
 */
uid_t user2uid(char *name, int flag, int *err)
{
	struct passwd *entry;
	uid_t ret;
	char *errch;

	if (err)
		*err = 0;
	if (!flag) {
		ret = strtoul(name, &errch, 0);
		if (!*errch)		/* Is name number - we got directly uid? */
			return ret;
	}
	if (!(entry = getpwnam(name))) {
		if (!err) {
			errstr(_("user %s does not exist.\n"), name);
			exit(1);
		}
		else {
			*err = -1;
			return 0;
		}
	}
	return entry->pw_uid;
}

/*
 *	Convert group name to gid
 */
gid_t group2gid(char *name, int flag, int *err)
{
	struct group *entry;
	gid_t ret;
	char *errch;

	if (err)
		*err = 0;
	if (!flag) {
		ret = strtoul(name, &errch, 0);
		if (!*errch)		/* Is name number - we got directly gid? */
			return ret;
	}
	if (!(entry = getgrnam(name))) {
		if (!err) {
			errstr(_("group %s does not exist.\n"), name);
			exit(1);
		}
		else {
			*err = -1;
			return 0;
		}
	}
	return entry->gr_gid;
}

/*
 *	Convert name to id
 */
int name2id(char *name, int qtype, int flag, int *err)
{
	if (qtype == USRQUOTA)
		return user2uid(name, flag, err);
	else
		return group2gid(name, flag, err);
}

/*
 *	Convert uid to name
 */
int uid2user(uid_t id, char *buf)
{
	struct passwd *entry;

	if (!(entry = getpwuid(id))) {
		snprintf(buf, MAXNAMELEN, "#%u", (uint) id);
		return 1;
	}
	else
		sstrncpy(buf, entry->pw_name, MAXNAMELEN);
	return 0;
}

/*
 *	Convert gid to name
 */
int gid2group(gid_t id, char *buf)
{
	struct group *entry;

	if (!(entry = getgrgid(id))) {
		snprintf(buf, MAXNAMELEN, "#%u", (uint) id);
		return 1;
	}
	else
		sstrncpy(buf, entry->gr_name, MAXNAMELEN);
	return 0;
}

/*
 *	Convert id to user/groupname
 */
int id2name(int id, int qtype, char *buf)
{
	if (qtype == USRQUOTA)
		return uid2user(id, buf);
	else
		return gid2group(id, buf);
}

/*
 *	Parse /etc/nsswitch.conf and return type of default passwd handling
 */
int passwd_handling(void)
{
	FILE *f;
	char buf[1024], *colpos, *spcpos;
	int ret = PASSWD_FILES;

	if (!(f = fopen("/etc/nsswitch.conf", "r")))
		return PASSWD_FILES;	/* Can't open nsswitch.conf - fallback on compatible mode */
	while (fgets(buf, sizeof(buf), f)) {
		if (strncmp(buf, "passwd:", 7))	/* Not passwd entry? */
			continue;
		for (colpos = buf+7; isspace(*colpos); colpos++);
		if (!*colpos)	/* Not found any type of handling? */
			break;
		for (spcpos = colpos; !isspace(*spcpos) && *spcpos; spcpos++);
		*spcpos = 0;
		if (!strcmp(colpos, "db") || !strcmp(colpos, "nis") || !strcmp(colpos, "nis+"))
			ret = PASSWD_DB;
		break;
	}
	fclose(f);
	return ret;
}


/*************************************************************************
 * if you want to turn off gettext without changing sources edit pot.h 
 *************************************************************************/

void gettexton(void)
{
#ifdef USE_GETTEXT
	setlocale(LC_ALL, "");
	bindtextdomain("quota", "/usr/share/locale");
	textdomain("quota");
#endif
}
/*
**__________________________________________________________________________
*/
/**
*   Create an af_unix socket
*/
int quota_af_unix_sock_create(char *nameOfSocket,int size)
{
  int ret;
  int fd=-1;
  struct sockaddr_un addr;
  int fdsize;
  int optionsize=sizeof(fdsize);

  /*
  ** create a datagram socket
  */
  fd=socket(PF_UNIX,SOCK_DGRAM,0);
  if(fd<0)
  {
    return -1;
  }
  /*
  ** remove fd if it already exists
  */
  ret=unlink(nameOfSocket);
  /*
  ** named the socket reception side
  */
  addr.sun_family= AF_UNIX;
  strcpy(addr.sun_path,nameOfSocket);
  ret=bind(fd,(struct sockaddr*)&addr,sizeof(addr));
  if(ret<0)
  {
    return -1;
  }
  /*
  ** change the length for the send buffer, nothing to do for receive buf
  ** since it is out of the scope of the AF_SOCKET
  */
  ret= getsockopt(fd,SOL_SOCKET,SO_SNDBUF,(char*)&fdsize,(socklen_t*)&optionsize);
  if(ret<0)
  {
    return -1;
  }
  /*
  ** update the size, always the double of the input
  */
  fdsize=2*size;

  /*
  ** set a new size for emission and
  ** reception socket's buffer
  */
  ret=setsockopt(fd,SOL_SOCKET,SO_SNDBUF,(char*)&fdsize,sizeof(int));
  if(ret<0)
  {
    return -1;
  }

  return(fd);
}


/*
**__________________________________________________________________________
*/
/**
*   delete an af_unix socket
*/
int quota_af_unix_sock_delete(char *nameOfSocket,int fd)
{
  /*
  ** close the socket
  */
  if (fd != -1) close(fd);

  /*
  ** remove the file
  */
  unlink(nameOfSocket);
  return 0;
}

/*
** wait response service
*/
/**
*  wait the response associated with the request

  @param rsp_buf : pointer to the response buffer
  @param buf_len : length of the buffer
  @param  fd:      file descriptor
  
  
  @retval 0 on success
  @retval -1 on error (see erno for details)
  
*/
int quota_wait_response(rozofs_qt_header_t *rsp_buf, int length,int fd,int transaction_id)
{
   fd_set  readfds;
   int nb;
   struct timeval time;
   int  bytesRcvd;
   
   while (1)
   {
     time.tv_sec = 30;
     time.tv_usec = 0;

     FD_ZERO(&readfds);     
     FD_SET(fd,&readfds);

     nb = select(fd+1,&readfds,NULL,NULL,&time);
     if (nb == 0)
     {
	/*
	** case of the time out
	*/
	errno = ETIME;
	return -1;   
     }
     /*
     ** attempt to receive the message
     */
     bytesRcvd = recvfrom(fd,
		       rsp_buf,length, 
		       0,(struct sockaddr *)NULL,NULL);
     if (bytesRcvd == -1) {
       return -1;
     }
     /*
     ** check the transaction id
     */
     if (rsp_buf->xid == transaction_id)
     {
	return 0;
     }
   }
   return -1;
}

/*
**__________________________________________________________________
*/   
/**
*  Read the export configuration and extract all the exported filesystem

  @param fname: full pathname of the exportd configuration file
  @param config: pointer to an array that will contain the parsed configuration
  
  @retval 0 on success
  @retval -1 on error
*/
int export_config_read(econfig_t *config, const char *fname)
{
    int status = -1;    
    econfig_initialize(config);
    
    if (econfig_read(config, fname) != 0) {
        errno = EIO;
        severe("can't read %s ", fname);
        goto out;
    }
    status = 0;
out:
    return status;
}
/*
**__________________________________________________________________
*/   
/*
** search for the eid of the export
  
    @param eid: eid to search
    
    @retval <> NULL pathname of the export
    @retval NULL eid not found
*/
char * econfig_get_export_path(econfig_t *config,int eid) {
    list_t *p;
    int found = 0;
    export_config_t *e1= NULL;

    list_for_each_forward(p, &config->exports) {
        e1 = list_entry(p, export_config_t, list);
        if (e1->eid != eid)
            continue;
	found = 1;
	break;
    }
    if (found)
    {
        if (access(e1->root, F_OK) != 0) {
            severe("can't access %s: %s.", e1->root, strerror(errno));
            goto out;
        }
    }
    return e1->root;
out:
    return NULL;
}

/*
**__________________________________________________________________
*/

/*
 * Convert number in quota blocks to some nice short form for printing
 */
void space2str(int64_t space, char *buf, int format)
{
	int i;
	char suffix[8] = " MGT";

	space = qb2kb(space);
	if (format) {
		for (i = 3; i > 0; i--)
			if (space >= (1LL << (QUOTABLOCK_BITS*i))*100) {
				sprintf(buf, "%llu%c", (unsigned long long)(space+(1 << (QUOTABLOCK_BITS*i))-1) >> (QUOTABLOCK_BITS*i), suffix[i]);
				return;
			}
		sprintf(buf, "%lluK", (unsigned long long)space);
		return;
	}
	sprintf(buf, "%llu", (unsigned long long)space);
}
/*
 *  Convert number to some nice short form for printing
 */
void number2str(unsigned long long num, char *buf, int format)
{
	int i;
	unsigned long long div;
	char suffix[8] = " kmgt";

	if (format)
		for (i = 4, div = 1000000000000LL; i > 0; i--, div /= 1000)
			if (num >= 100*div) {
				sprintf(buf, "%llu%c", (num+div-1) / div, suffix[i]);
				return;
			}
	sprintf(buf, "%llu", num);
}
/*
 * Convert time difference of seconds and current time
 */
void difftime2str(time_t seconds, char *buf)
{
	time_t now;

	buf[0] = 0;
	if (!seconds)
		return;
	time(&now);
	if (seconds <= now) {
		strcpy(buf, "none");
		return;
	}
	time2str(seconds - now, buf, TF_ROUND);
}

/*
 * Convert time to printable form
 */
void time2str(time_t seconds, char *buf, int flags)
{
	uint minutes, hours, days;

	if (flags & TF_ROUND) {
		minutes = (seconds + 30) / 60;	/* Rounding */
		hours = minutes / 60;
		minutes %= 60;
		days = hours / 24;
		hours %= 24;
		if (days >= 2)
			snprintf(buf, MAXTIMELEN, "%ddays", days);
		else
			snprintf(buf, MAXTIMELEN, "%02d:%02d", hours + days * 24, minutes);
	}
	else {
		minutes = seconds / 60;
		seconds %= 60;
		hours = minutes / 60;
		minutes %= 60;
		days = hours / 24;
		hours %= 24;
		if (seconds || (!minutes && !hours && !days))
			snprintf(buf, MAXTIMELEN, "%useconds", (uint)(seconds+minutes*60+hours*3600+days*3600*24));
		else if (minutes)
			snprintf(buf, MAXTIMELEN, "%uminutes", (uint)(minutes+hours*60+days*60*24));
		else if (hours)
			snprintf(buf, MAXTIMELEN, "%uhours", (uint)(hours+days*24));
		else
			snprintf(buf, MAXTIMELEN, "%udays", days);
	}
}
/*
 * Convert number with unit to time in seconds
 */
int str2timeunits(time_t num, char *unit, time_t *res)
{
	if (!strcmp(unit, _("second")) || !strcmp(unit, _("seconds")))
		*res = num;
	else if (!strcmp(unit, _("minute")) || !strcmp(unit, _("minutes")))
		*res = num * 60;
	else if (!strcmp(unit, _("hour")) || !strcmp(unit, _("hours")))
		*res = num * 60 * 60;
	else if (!strcmp(unit, _("day")) || !strcmp(unit, _("days")))
		*res = num * 24 * 60 * 60;
	else
		return -1;
	return 0;
}


