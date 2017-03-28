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

#include "config.h"
#include "rebalance_config.h"
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>


static char rebalance_config_file_name[256] = {0};
static int  rebalance_config_file_is_read=0;
rebalance_config_t rebalance_config;

void show_rebalance_config(char * argv[], uint32_t tcpRef, void *bufRef);
void rebalance_config_read(char * fname) ;


static int isDefaultValue;
#define REBALANCE_CONFIG_SHOW_NAME(val) {\
  if (isDefaultValue) {\
    pChar += rozofs_string_append(pChar,"// ");\
  } else {\
    pChar += rozofs_string_append(pChar,"   ");\
  }\
  pChar += rozofs_string_padded_append(pChar, 50, rozofs_left_alignment, #val);\
  pChar += rozofs_string_append(pChar, " = ");\
}

#define  REBALANCE_CONFIG_SHOW_NEXT \
  pChar += rozofs_eol(pChar);\
  pChar += rozofs_eol(pChar);

#define  REBALANCE_CONFIG_SHOW_END \
  *pChar++ = ';';\
  REBALANCE_CONFIG_SHOW_NEXT

#define  REBALANCE_CONFIG_SHOW_END_OPT(opt) \
  pChar += rozofs_string_append(pChar,"; \t// ");\
  pChar += rozofs_string_append(pChar,opt);\
  REBALANCE_CONFIG_SHOW_NEXT

#define REBALANCE_CONFIG_IS_DEFAULT_BOOL(val,def) \
  isDefaultValue = 0;\
  if (((rebalance_config.val)&&(strcmp(#def,"True")==0)) \
  ||  ((!rebalance_config.val)&&(strcmp(#def,"False")==0))) \
    isDefaultValue = 1;

#define REBALANCE_CONFIG_SHOW_BOOL(val,def)  {\
  REBALANCE_CONFIG_SHOW_NAME(val)\
  if (rebalance_config.val) pChar += rozofs_string_append(pChar, "True");\
  else        pChar += rozofs_string_append(pChar, "False");\
  REBALANCE_CONFIG_SHOW_END\
}

#define REBALANCE_CONFIG_IS_DEFAULT_STRING(val,def) \
  isDefaultValue = 0; \
  if (strcmp(rebalance_config.val,def)==0) isDefaultValue = 1;

#define REBALANCE_CONFIG_SHOW_STRING(val,def)  {\
  REBALANCE_CONFIG_SHOW_NAME(val)\
  *pChar++ = '\"';\
  if (rebalance_config.val!=NULL) pChar += rozofs_string_append(pChar, rebalance_config.val);\
  *pChar++ = '\"';\
  REBALANCE_CONFIG_SHOW_END\
}

#define REBALANCE_CONFIG_IS_DEFAULT_INT(val,def) \
  isDefaultValue = 0; \
  if (rebalance_config.val == def) isDefaultValue = 1;

#define REBALANCE_CONFIG_SHOW_INT(val,def)  {\
  REBALANCE_CONFIG_SHOW_NAME(val)\
  pChar += rozofs_i32_append(pChar, rebalance_config.val);\
  REBALANCE_CONFIG_SHOW_END\
}

#define REBALANCE_CONFIG_IS_DEFAULT_INT_OPT(val,def)  REBALANCE_CONFIG_IS_DEFAULT_INT(val,def)
#define REBALANCE_CONFIG_SHOW_INT_OPT(val,def,opt)  {\
  REBALANCE_CONFIG_SHOW_NAME(val)\
  pChar += rozofs_i32_append(pChar, rebalance_config.val);\
  REBALANCE_CONFIG_SHOW_END_OPT(opt)\
}

#define REBALANCE_CONFIG_IS_DEFAULT_LONG(val,def)  REBALANCE_CONFIG_IS_DEFAULT_INT(val,def)
#define REBALANCE_CONFIG_SHOW_LONG(val,def)  {\
  REBALANCE_CONFIG_SHOW_NAME(val)\
  pChar += rozofs_i64_append(pChar, rebalance_config.val);\
  REBALANCE_CONFIG_SHOW_END\
}

#define REBALANCE_CONFIG_IS_DEFAULT_LONG_OPT(val,def)  REBALANCE_CONFIG_IS_DEFAULT_INT(val,def)
#define REBALANCE_CONFIG_SHOW_LONG_OPT(val,def,opt)  {\
  REBALANCE_CONFIG_SHOW_NAME(val)\
  pChar += rozofs_i64_append(pChar, rebalance_config.val);\
  REBALANCE_CONFIG_SHOW_END_OPT(opt)\
}

static int  boolval;
#define REBALANCE_CONFIG_READ_BOOL(val,def)  {\
  if (strcmp(#def,"True")==0) {\
    rebalance_config.val = 1;\
  } else {\
    rebalance_config.val = 0;\
  }\
  if (config_lookup_bool(&cfg, #val, &boolval)) { \
    rebalance_config.val = boolval;\
  }\
}

#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR >= 4)) \
             || (LIBCONFIG_VER_MAJOR > 1))
static int               intval;
#else
static long int          intval;
#endif

#define REBALANCE_CONFIG_READ_INT_MINMAX(val,def,mini,maxi)  {\
  rebalance_config.val = def;\
  if (config_lookup_int(&cfg, #val, &intval)) { \
    if (intval<mini) {\
      rebalance_config.val = mini;\
    }\
    else if (intval>maxi) { \
      rebalance_config.val = maxi;\
    }\
    else {\
      rebalance_config.val = intval;\
    }\
  }\
}

#define REBALANCE_CONFIG_READ_INT(val,def) {\
  rebalance_config.val = def;\
  if (config_lookup_int(&cfg, #val, &intval)) { \
    rebalance_config.val = intval;\
  }\
}

#define REBALANCE_CONFIG_READ_LONG(val,def) {\
static long long         longval;\
  rebalance_config.val = def;\
  if (config_lookup_int64(&cfg, #val, &longval)) { \
    rebalance_config.val = longval;\
  }\
}


#define REBALANCE_CONFIG_READ_LONG_MINMAX(val,def,mini,maxi)  {\
static long long         longval;\
  rebalance_config.val = def;\
  if (config_lookup_int64(&cfg, #val, &longval)) { \
    if (longval<mini) {\
      rebalance_config.val = mini;\
    }\
    else if (longval>maxi) { \
      rebalance_config.val = maxi;\
    }\
    else {\
      rebalance_config.val = longval;\
    }\
  }\
}

static const char * charval;
#define REBALANCE_CONFIG_READ_STRING(val,def)  {\
  if (rebalance_config.val) free(rebalance_config.val);\
  if (config_lookup_string(&cfg, #val, &charval)) {\
    rebalance_config.val = strdup(charval);\
  } else {\
    rebalance_config.val = strdup(def);\
  }\
}


#include "rebalance_config_read_show.h"
void rebalance_config_extra_checks(void);

void show_rebalance_config(char * argv[], uint32_t tcpRef, void *bufRef) {
  rebalance_config_generated_show(argv,tcpRef,bufRef);
}

void rebalance_config_read(char * fname) {
  rebalance_config_generated_read(fname);
}
