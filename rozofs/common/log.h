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

#ifndef _LOG_H
#define	_LOG_H

#include <syslog.h>
#include <libgen.h>

#define EDEBUG      0
#define EINFO       1
#define EWARNING    2
#define ESEVERE     3
#define EFATAL      4

#ifdef __GNUC__
static const char *messages[] __attribute__ ((unused)) =
        {"debug", "info", "warning", "severe", "fatal"};
#else
static const char *messages[] =
    { "debug", "info", "warning", "severe", "fatal" };
#endif

static const int priorities[] = {
	LOG_DEBUG, LOG_INFO, LOG_WARNING, LOG_ERR, LOG_ERR
};


#ifdef SYSLOG2PRINT
/*
** When compiled with this flag, syslog are transformed in printf
*/
#define logmsg(level, fmt, ...) {\
    printf("[%7s] %s - %d - %s: " fmt "\n", messages[level], basename(__FILE__), __LINE__, messages[level], ##__VA_ARGS__); \
}
#else
#define logmsg(level, fmt, ...) {\
    syslog(priorities[level], "%s - %d - %s: " fmt, basename(__FILE__), __LINE__, messages[level], ##__VA_ARGS__); \
}
#endif          

#define info(fmt, ...) logmsg(EINFO, fmt, ##__VA_ARGS__)
#define warning(fmt, ...) logmsg(EWARNING, fmt, ##__VA_ARGS__)
#define severe(fmt, ...) logmsg(ESEVERE, fmt, ##__VA_ARGS__)
#define fatal(fmt, ...) {logmsg(EFATAL, fmt, ##__VA_ARGS__); abort();}

#ifndef NDEBUG
#define DEBUG(fmt, ...) logmsg(EDEBUG, fmt, ##__VA_ARGS__)
#else
#define DEBUG(fmt, ...)
#endif


#ifndef NDEBUGFUNCTION
#define DEBUG_FUNCTION DEBUG("%s", __FUNCTION__)
#else
#define DEBUG_FUNCTION
#endif

#endif
