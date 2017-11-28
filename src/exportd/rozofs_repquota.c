/*
 *
 *	Utility for reporting quotas
 *
 *	Based on old repquota.
 *	Jan Kara <jack@suse.cz> - Sponsored by SuSE CZ
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <pwd.h>
#include <grp.h>
#include <getopt.h>
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
#include <rozofs/rpc/export_profiler.h>

#define PRINTNAMELEN 9	/* Number of characters to be reserved for name on screen */
#define MAX_CACHE_DQUOTS 1024	/* Number of dquots in cache */
/* Possible default passwd handling */
#define PASSWD_FILES 0
#define PASSWD_DB 1

#define EXPORT_DEFAULT_PATH "/etc/rozofs/export.conf"


#define MAXNAMELEN 64		/* Maximal length of user/group name */
#define MAXTIMELEN 40		/* Maximal length of time string */
#define MAXNUMLEN 32		/* Maximal length of number */

#define FL_USER 1
#define FL_GROUP 2
#define FL_VERBOSE 4
#define FL_ALL 8		/* Dump quota files on all filesystems */
#define FL_TRUNCNAMES 16	/* Truncate names to fit into the screen */
#define FL_SHORTNUMS 32	/* Try to print space in appropriate units */
#define FL_NONAME 64	/* Don't translate ids to names */
#define FL_NOCACHE 128	/* Don't cache dquots before resolving */
#define FL_NOAUTOFS 256	/* Ignore autofs mountpoints */
#define FL_RAWGRACE 512	/* Print grace times in seconds since epoch */
#define FL_SHARE 1024	/* Print share information */
/* Flags for formatting time */
#define TF_ROUND 0x1		/* Should be printed time rounded? */

/* Size of blocks in which are counted size limits in generic utility parts */
#define QUOTABLOCK_BITS 10
#define QUOTABLOCK_SIZE (1 << QUOTABLOCK_BITS)

/* Conversion routines from and to quota blocks */
#define qb2kb(x) ((x) << (QUOTABLOCK_BITS-10))
#define kb2qb(x) ((x) >> (QUOTABLOCK_BITS-10))
#define toqb(x) (((x) + QUOTABLOCK_SIZE - 1) >> QUOTABLOCK_BITS)

static int flags;
static char **mnt;
static int mntcnt;
char *progname;

static int enable_syslog=0;
char *confname=NULL;
econfig_t exportd_config;
int rozofs_no_site_file = 0;
static char extensions[MAXQUOTAS + 2][20] = INITQFNAMES;

export_one_profiler_t * export_profiler[1];
uint32_t                export_profiler_eid;
int                     searched_user_id;

/*
 *	Convert type of quota to written representation
 */
char *type2name(int type)
{
	return extensions[type];
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

void version(void)
{
//	printf(_("Quota utilities version %s.\n"), PACKAGE_VERSION);
//	printf(_("Compiled with:%s\n"), COMPILE_OPTS);
//	printf(_("Bugs to %s\n"), MY_EMAIL);
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


static void usage(void)
{
	errstr("Utility for reporting quotas for one or more eid.\nUsage:\n%s [-vugs]  [-c|C] [-t|n] [-f exportconf] [-i user_id] eid\n\n\
-v, --verbose               display also users/groups without any usage\n\
-u, --user                  display information about users\n\
-g, --group                 display information about groups\n\
-s, --share                 display information about shares\n\
-H, --human-readable        show numbers in human friendly units (MB, GB, ...)\n\
-t, --truncate-names        truncate names to 9 characters\n\
-p, --raw-grace             print grace time in seconds since epoch\n\
-n, --no-names              do not translate uid/gid to name\n\
-f, --exportconf=path       pathname of the export configuration\n\
-c, --batch-translation     translate big number of ids at once\n\
-C, --no-batch-translation  translate ids one by one\n\
-i, --id                    user identifier number\n\
-h, --help                  display this help message and exit\n\
-V, --version               display version information and exit\n\n", progname);
	exit(1);
}

static void parse_options(int argcnt, char **argstr)
{
	int ret;
	int cache_specified = 0;
	struct option long_opts[] = {
		{ "version", 0, NULL, 'V' },
		{ "all", 0, NULL, 'a' },
		{ "verbose", 0, NULL, 'v' },
		{ "user", 0, NULL, 'u' },
		{ "group", 0, NULL, 'g' },
		{ "share", 0, NULL, 's' },
		{ "help", 0, NULL, 'h' },
		{ "truncate-names", 0, NULL, 't' },
		{ "raw-grace", 0, NULL, 'p' },
		{ "human-readable", 0, NULL, 'H' },
		{ "no-names", 0, NULL, 'n' },
		{ "cache", 0, NULL, 'c' },
		{ "no-cache", 0, NULL, 'C' },
		{ "exportconf", 0, NULL, 'f' },
		{ "id", 0, NULL, 'i' },
		{ NULL, 0, NULL, 0 }
	};

        searched_user_id  = 0;
	
	while ((ret = getopt_long(argcnt, argstr, "VavugshtHpncCf:i:", long_opts, NULL)) != -1) {
		switch (ret) {
			case '?':
			case 'h':
				usage();
			case 'V':
				version();
				exit(0);
			case 'u':
				flags |= FL_USER;
				break;
			case 'g':
				flags |= FL_GROUP;
				break;
			case 's':
				flags |= FL_SHARE;
				break;
			case 'v':
				flags |= FL_VERBOSE;
				break;
			case 'a':
				flags |= FL_ALL;
				break;
			case 't':
				flags |= FL_TRUNCNAMES;
				break;
			case 'p':
				flags |= FL_RAWGRACE;
				break;
			case 'H':
				flags |= FL_SHORTNUMS;
				break;
			case 'C':
				flags |= FL_NOCACHE;
				cache_specified = 1;
				break;
			case 'c':
				cache_specified = 1;
				break;
			case 'f':
				confname = optarg;
				break;
			case 'i':
				if (sscanf(optarg,"%u",&searched_user_id)!= 1)
				{
				  usage();
				  exit(0);
				}
				break;
			case 'n':
				flags |= FL_NONAME;
				break;

		}
	}

	if ((flags & FL_ALL && optind != argcnt) || (!(flags & FL_ALL) && optind == argcnt)) {
		fputs("Bad number of arguments.\n", stderr);
		usage();
	}
	if (flags & FL_NONAME && flags & FL_TRUNCNAMES) {
		fputs("Specified both -n and -t but only one of them can be used.\n", stderr);
		exit(1);
	}
	
	if (!(flags & (FL_USER | FL_GROUP)))
		flags |= FL_USER;
	if (!(flags & FL_ALL)) {
		mnt = argstr + optind;
		mntcnt = argcnt - optind;
	}
	if (!cache_specified && !(flags & FL_NONAME) && passwd_handling() == PASSWD_DB)
		flags |= FL_NOCACHE;
}

/* Are we over soft or hard limit? */
static char overlim(int64_t usage, int64_t softlim, int64_t hardlim)
{
	if ((usage > softlim && softlim) || (usage > hardlim && hardlim))
		return '+';
	return '-';
}

/* Print one quota entry */
static void print(rozofs_dquot_t *dquot, char *name)
{
	char pname[MAXNAMELEN];
	char time[MAXTIMELEN];
	char numbuf[3][MAXNUMLEN];
	
	rozo_mem_dqblk *entry = &dquot->quota;

	if (!entry->dqb_curspace && !entry->dqb_curinodes && !(flags & FL_VERBOSE))
		return;
	sstrncpy(pname, name, sizeof(pname));
	if (flags & FL_TRUNCNAMES)
		pname[PRINTNAMELEN] = 0;
	if (entry->dqb_bsoftlimit && toqb(entry->dqb_curspace) >= entry->dqb_bsoftlimit)
		if (flags & FL_RAWGRACE)
			sprintf(time, "%llu", (unsigned long long)entry->dqb_btime);
		else
			difftime2str(entry->dqb_btime, time);
	else
		if (flags & FL_RAWGRACE)
			strcpy(time, "0");
		else
			time[0] = 0;
	space2str(toqb(entry->dqb_curspace), numbuf[0], flags & FL_SHORTNUMS);
	space2str(entry->dqb_bsoftlimit, numbuf[1], flags & FL_SHORTNUMS);
	space2str(entry->dqb_bhardlimit, numbuf[2], flags & FL_SHORTNUMS);
	printf("%-*s %c%c %10s %10s %10s %10s", PRINTNAMELEN, pname,
	       overlim(qb2kb(toqb(entry->dqb_curspace)), qb2kb(entry->dqb_bsoftlimit), qb2kb(entry->dqb_bhardlimit)),
	       overlim(entry->dqb_curinodes, entry->dqb_isoftlimit, entry->dqb_ihardlimit),
	       numbuf[0], numbuf[1], numbuf[2], time);
	if (entry->dqb_isoftlimit && entry->dqb_curinodes >= entry->dqb_isoftlimit)
		if (flags & FL_RAWGRACE)
			sprintf(time, "%llu", (unsigned long long)entry->dqb_itime);
		else
			difftime2str(entry->dqb_itime, time);
	else
		if (flags & FL_RAWGRACE)
			strcpy(time, "0");
		else
			time[0] = 0;
	number2str(entry->dqb_curinodes, numbuf[0], flags & FL_SHORTNUMS);
	number2str(entry->dqb_isoftlimit, numbuf[1], flags & FL_SHORTNUMS);
	number2str(entry->dqb_ihardlimit, numbuf[2], flags & FL_SHORTNUMS);
	printf(" %10s %8s %8s %6s\n", numbuf[0], numbuf[1], numbuf[2], time);
}



/**
*    header associated with a quota of a given export identifier

     @param type: type of the quota
     @param eid : export identifier
     @param path: pathname of the export identifier
     
*/
static char *quotatypes[] = INITQFNAMES;

static void report_it(int type,int eid,char *path)
{
//	char bgbuf[MAXTIMELEN], igbuf[MAXTIMELEN];
	char *spacehdr;

	if (flags & FL_SHORTNUMS)
		spacehdr = "Space";
	else
		spacehdr = "Block";
	printf("\n*** Report for %s quotas on eid %d: %s\n", type2name(type),eid, path);
#if 0
	time2str(h->qh_info.dqi_bgrace, bgbuf, TF_ROUND);
	time2str(h->qh_info.dqi_igrace, igbuf, TF_ROUND);
	printf(_("Block grace time: %s; Inode grace time: %s\n"), bgbuf, igbuf);
#endif

	printf("                         %s limits                               File limits\n", spacehdr);
	printf("%-9s          used       soft       hard      grace       used     soft     hard  grace\n", quotatypes[type]);
	printf("---------------------------------------------------------------------------------------------\n");

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
    config_t cfg;
    config_init(&cfg);
    
    econfig_initialize(config);

    if (config_read_file(&cfg, fname) == CONFIG_FALSE) {
        errno = EIO;
        severe("can't read %s : %s.", fname, config_error_text(&cfg));
        goto out;
    }

    if (load_exports_conf_api(config, &cfg) != 0) {
        severe("can't load exports config.");
        goto out;
    }

    status = 0;
out:
    config_destroy(&cfg);
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
/**

    Dump quota information relative to a given eid for a given type (USER or GROUP)
    
    @param : ctx_p : pointer to the disk table associated with the type
    @param  type: type of the quota
    
*/   
void dump_quota(disk_table_header_t *ctx_p,int type,int eid,char *path)
{

   int file_idx_next= 0;
   int file_idx = 0;
   int fd = -1;
   int nb_records;
   int record;
   int count;
   rozofs_dquot_t data;
   struct group *grp_p;
   struct passwd *usr_p;  
  char name[256];
  
  report_it(type,eid,path);
      
   while((file_idx = disk_tb_get_next_file_entry(ctx_p,(uint32_t*)&file_idx_next)) >= 0)
   {
      /*
      ** we get one file, so now get the entries
      */
      while((nb_records = disk_tb_get_nb_records(ctx_p,file_idx,&fd)) > 0)
      {
         /*
	 ** read the file records
	 */
	 for (record = 0; record < nb_records; record++)
	 {
	    count = disk_tb_get_next_record(ctx_p,record,fd,&data);
	    if (count != ctx_p->entry_sz)
	    {
	      break;
	    }
	    if ((searched_user_id !=0) && (data.key.s.qid != searched_user_id)) continue;
	    if (type == USRQUOTA)
	    {
	        if (flags & FL_NONAME)
		{
		  sprintf(name, "#%u",data.key.s.qid); 
		}
		else
		{	        
		  usr_p= getpwuid(data.key.s.qid);
		  if (usr_p != NULL)
		  {
		    strcpy(name,usr_p->pw_name);
		  }
		  else
		  {
		    sprintf(name, "#%u",data.key.s.qid); 
		  }
		}	    	    
	    }
	    else
	    {
	        if (flags & FL_NONAME)
		{
		  sprintf(name, "#%u",data.key.s.qid); 
		}	        
		else
		{
		  grp_p= getgrgid(data.key.s.qid);
		  if (grp_p != NULL)
		  {
		    strcpy(name,grp_p->gr_name);
		  }
		  else
		  {
		    sprintf(name, "#%u",data.key.s.qid); 
		  }
		}
	    
	    }
	    print(&data,name);	 
	 }
	 close(fd);
	 break;      
      }   
   }
}
/*
**__________________________________________________________________
*/   
int main(int argc, char **argv)
{
	gettexton();
	progname = basename(argv[0]);
	int eid;
	int i;
	char *errch=NULL;
	int ret;
        rozofs_qt_export_t *quota_ctx_p = NULL;
	char *pathname;
	
        confname = strdup(EXPORT_DEFAULT_PATH);
	
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
	  
	  quota_ctx_p = rozofs_qt_alloc_context(eid,pathname,1);
	  if (quota_ctx_p == NULL)
	  {
	     printf("fail to create quota data for exportd %d\n",eid);
	     exit(-1);
	  }

	  if (flags & FL_USER)
		  dump_quota(quota_ctx_p->quota_inode[USRQUOTA],USRQUOTA,eid,pathname);

	  if (flags & FL_GROUP)
		  dump_quota(quota_ctx_p->quota_inode[GRPQUOTA],GRPQUOTA,eid,pathname);

          
	  if (flags & FL_SHARE)
	  {
	      flags |=FL_NONAME;
		  dump_quota(quota_ctx_p->quota_inode[SHRQUOTA],SHRQUOTA,eid,pathname);
	  }
	}

	return 0;
}
