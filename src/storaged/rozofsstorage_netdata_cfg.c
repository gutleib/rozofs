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
#include "rozofsstorage_netdata_cfg.h"
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>


static char rozofsstorage_netdata_cfg_file_name[256] = {0};
static int  rozofsstorage_netdata_cfg_file_is_read=0;
rozofsstorage_netdata_cfg_t rozofsstorage_netdata_cfg;

void show_rozofsstorage_netdata_cfg(char * argv[], uint32_t tcpRef, void *bufRef);
void rozofsstorage_netdata_cfg_read(char * fname) ;

char   myBigBuffer[1024*1024];
static int isDefaultValue;
#define ROZOFSSTORAGE_NETDATA_CFG_SHOW_NAME(val,def) {\
  if (isDefaultValue) {\
    pChar += rozofs_string_append(pChar,"// ");\
    pChar += rozofs_string_padded_append(pChar, 50, rozofs_left_alignment, #val);\
    pChar += rozofs_string_append(pChar, " = ");\
  } else {\
    pChar += rozofs_string_append(pChar,"// default is ");\
    pChar += rozofs_string_append(pChar, #def);\
    pChar += rozofs_eol(pChar);\
    pChar += rozofs_string_append(pChar,"   ");\
    pChar += rozofs_string_padded_append(pChar, 50, rozofs_left_alignment, #val);\
    pChar += rozofs_string_append(pChar, " = ");\
  }\
}

#define  ROZOFSSTORAGE_NETDATA_CFG_SHOW_NEXT \
  pChar += rozofs_eol(pChar);\
  pChar += rozofs_eol(pChar);

#define  ROZOFSSTORAGE_NETDATA_CFG_SHOW_END \
  *pChar++ = ';';\
  ROZOFSSTORAGE_NETDATA_CFG_SHOW_NEXT

#define  ROZOFSSTORAGE_NETDATA_CFG_SHOW_END_OPT(opt) \
  pChar += rozofs_string_append(pChar,"; \t// ");\
  pChar += rozofs_string_append(pChar,opt);\
  ROZOFSSTORAGE_NETDATA_CFG_SHOW_NEXT

#define ROZOFSSTORAGE_NETDATA_CFG_IS_DEFAULT_BOOL(val,def) \
  isDefaultValue = 0;\
  if (((rozofsstorage_netdata_cfg.val)&&(strcmp(#def,"True")==0)) \
  ||  ((!rozofsstorage_netdata_cfg.val)&&(strcmp(#def,"False")==0))) \
    isDefaultValue = 1;

#define ROZOFSSTORAGE_NETDATA_CFG_SHOW_BOOL(val,def)  {\
  ROZOFSSTORAGE_NETDATA_CFG_SHOW_NAME(val,def)\
  if (rozofsstorage_netdata_cfg.val) pChar += rozofs_string_append(pChar, "True");\
  else        pChar += rozofs_string_append(pChar, "False");\
  ROZOFSSTORAGE_NETDATA_CFG_SHOW_END\
}

#define ROZOFSSTORAGE_NETDATA_CFG_IS_DEFAULT_STRING(val,def) \
  isDefaultValue = 0; \
  if (strcmp(rozofsstorage_netdata_cfg.val,def)==0) isDefaultValue = 1;

#define ROZOFSSTORAGE_NETDATA_CFG_SHOW_STRING(val,def)  {\
  ROZOFSSTORAGE_NETDATA_CFG_SHOW_NAME(val,def)\
  *pChar++ = '\"';\
  if (rozofsstorage_netdata_cfg.val!=NULL) pChar += rozofs_string_append(pChar, rozofsstorage_netdata_cfg.val);\
  *pChar++ = '\"';\
  ROZOFSSTORAGE_NETDATA_CFG_SHOW_END\
}

#define ROZOFSSTORAGE_NETDATA_CFG_IS_DEFAULT_ENUM(val,def) \
  isDefaultValue = 0; \
  if (rozofsstorage_netdata_cfg.val == string2rozofsstorage_netdata_cfg_ ## val (def)) isDefaultValue = 1;

#define ROZOFSSTORAGE_NETDATA_CFG_SHOW_ENUM(val,def,opt)  {\
  ROZOFSSTORAGE_NETDATA_CFG_SHOW_NAME(val,def)\
  *pChar++ = '\"';\
  pChar += rozofs_string_append(pChar, rozofsstorage_netdata_cfg_ ## val ## 2String(rozofsstorage_netdata_cfg.val));\
  *pChar++ = '\"';\
  ROZOFSSTORAGE_NETDATA_CFG_SHOW_END_OPT(opt)\
}

#define ROZOFSSTORAGE_NETDATA_CFG_IS_DEFAULT_INT(val,def) \
  isDefaultValue = 0; \
  if (rozofsstorage_netdata_cfg.val == def) isDefaultValue = 1;

#define ROZOFSSTORAGE_NETDATA_CFG_SHOW_INT(val,def)  {\
  ROZOFSSTORAGE_NETDATA_CFG_SHOW_NAME(val,def)\
  pChar += rozofs_i32_append(pChar, rozofsstorage_netdata_cfg.val);\
  ROZOFSSTORAGE_NETDATA_CFG_SHOW_END\
}

#define ROZOFSSTORAGE_NETDATA_CFG_IS_DEFAULT_INT_OPT(val,def)  ROZOFSSTORAGE_NETDATA_CFG_IS_DEFAULT_INT(val,def)
#define ROZOFSSTORAGE_NETDATA_CFG_SHOW_INT_OPT(val,def,opt)  {\
  ROZOFSSTORAGE_NETDATA_CFG_SHOW_NAME(val,def)\
  pChar += rozofs_i32_append(pChar, rozofsstorage_netdata_cfg.val);\
  ROZOFSSTORAGE_NETDATA_CFG_SHOW_END_OPT(opt)\
}

#define ROZOFSSTORAGE_NETDATA_CFG_IS_DEFAULT_LONG(val,def)  ROZOFSSTORAGE_NETDATA_CFG_IS_DEFAULT_INT(val,def)
#define ROZOFSSTORAGE_NETDATA_CFG_SHOW_LONG(val,def)  {\
  ROZOFSSTORAGE_NETDATA_CFG_SHOW_NAME(val,def)\
  pChar += rozofs_i64_append(pChar, rozofsstorage_netdata_cfg.val);\
  ROZOFSSTORAGE_NETDATA_CFG_SHOW_END\
}

#define ROZOFSSTORAGE_NETDATA_CFG_IS_DEFAULT_LONG_OPT(val,def)  ROZOFSSTORAGE_NETDATA_CFG_IS_DEFAULT_INT(val,def)
#define ROZOFSSTORAGE_NETDATA_CFG_SHOW_LONG_OPT(val,def,opt)  {\
  ROZOFSSTORAGE_NETDATA_CFG_SHOW_NAME(val,def)\
  pChar += rozofs_i64_append(pChar, rozofsstorage_netdata_cfg.val);\
  ROZOFSSTORAGE_NETDATA_CFG_SHOW_END_OPT(opt)\
}

#define ROZOFSSTORAGE_NETDATA_CFG_READ_BOOL(val,def)  {\
  int  boolval;\
  if (strcmp(#def,"True")==0) {\
    rozofsstorage_netdata_cfg.val = 1;\
  } else {\
    rozofsstorage_netdata_cfg.val = 0;\
  }\
  if (config_lookup_bool(&cfg, #val, &boolval) == CONFIG_TRUE) { \
    rozofsstorage_netdata_cfg.val = boolval;\
  }\
}
#define ROZOFSSTORAGE_NETDATA_CFG_SET_BOOL(val,def)  {\
  if (strcmp(def,"True")==0) {\
    rozofsstorage_netdata_cfg.val = 1;\
    pChar += rozofs_string_append(pChar,#val);\
    pChar += rozofs_string_append(pChar," set to value ");\
    pChar += rozofs_string_append(pChar,def);\
    pChar += rozofs_eol(pChar);\
    return 0;\
  }\
  if (strcmp(def,"False")==0) {\
    rozofsstorage_netdata_cfg.val = 0;\
    pChar += rozofs_string_append(pChar,#val);\
    pChar += rozofs_string_append(pChar," set to value ");\
    pChar += rozofs_string_append(pChar,def);\
    pChar += rozofs_eol(pChar);\
    return 0;\
  }\
  pChar += rozofs_string_append_error(pChar,"True or False value expected.\n" );\
  return -1;\
}

#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR >= 4)) \
             || (LIBCONFIG_VER_MAJOR > 1))
static int               intval;
#else
static long int          intval;
#endif

#define ROZOFSSTORAGE_NETDATA_CFG_READ_INT_MINMAX(val,def,mini,maxi)  {\
  rozofsstorage_netdata_cfg.val = def;\
  if (config_lookup_int(&cfg, #val, &intval) == CONFIG_TRUE) { \
    if (intval<mini) {\
      rozofsstorage_netdata_cfg.val = mini;\
    }\
    else if (intval>maxi) { \
      rozofsstorage_netdata_cfg.val = maxi;\
    }\
    else {\
      rozofsstorage_netdata_cfg.val = intval;\
    }\
  }\
}

#define ROZOFSSTORAGE_NETDATA_CFG_SET_INT_MINMAX(val,def,mini,maxi)  {\
  int valint;\
  if (sscanf(def,"%d",&valint) != 1) {\
    pChar += rozofs_string_append_error(pChar,"integer value expected.\n");\
    return -1;\
  }\
  if (valint<mini) {\
    pChar += rozofs_string_append_error(pChar,"value lower than minimum.\n");\
    return -1;\
  }\
  if (valint>maxi) { \
    pChar += rozofs_string_append_error(pChar,"value bigger than maximum.\n");\
    return -1;\
  }\
  pChar += rozofs_string_append(pChar,#val);\
  pChar += rozofs_string_append(pChar," set to value ");\
  pChar += rozofs_string_append(pChar,def);\
  pChar += rozofs_eol(pChar);\
  rozofsstorage_netdata_cfg.val = valint;\
  return 0;\
}

#define ROZOFSSTORAGE_NETDATA_CFG_READ_INT(val,def) {\
  rozofsstorage_netdata_cfg.val = def;\
  if (config_lookup_int(&cfg, #val, &intval) == CONFIG_TRUE) { \
    rozofsstorage_netdata_cfg.val = intval;\
  }\
}

#define ROZOFSSTORAGE_NETDATA_CFG_SET_INT(val,def)  {\
  int valint;\
  if (sscanf(def,"%d",&valint) != 1) {\
    pChar += rozofs_string_append_error(pChar,"integer value expected.\n");\
    return -1;\
  }\
  pChar += rozofs_string_append(pChar, #val);\
  pChar += rozofs_string_append(pChar," set to value ");\
  pChar += rozofs_string_append(pChar,def);\
  pChar += rozofs_eol(pChar);\
  rozofsstorage_netdata_cfg.val = valint;\
  return 0;\
}

#define ROZOFSSTORAGE_NETDATA_CFG_READ_LONG(val,def) {\
  long long         longval;\
  rozofsstorage_netdata_cfg.val = def;\
  if (config_lookup_int64(&cfg, #val, &longval) == CONFIG_TRUE) { \
    rozofsstorage_netdata_cfg.val = longval;\
  }\
}

#define ROZOFSSTORAGE_NETDATA_CFG_SET_LONG(val,def) {\
  long long         longval;\
  if (sscanf(def,"%lld",&longval) != 1) {\
    pChar += rozofs_string_append_error(pChar,"long long integer value expected.\n");\
    return -1;\
  }\
  pChar += rozofs_string_append(pChar,#val);\
  pChar += rozofs_string_append(pChar," set to value ");\
  pChar += rozofs_string_append(pChar,def);\
  pChar += rozofs_eol(pChar);\
  rozofsstorage_netdata_cfg.val = longval;\
  return 0;\
}


#define ROZOFSSTORAGE_NETDATA_CFG_READ_LONG_MINMAX(val,def,mini,maxi)  {\
  long long         longval;\
  rozofsstorage_netdata_cfg.val = def;\
  if (config_lookup_int64(&cfg, #val, &longval) == CONFIG_TRUE) { \
    if (longval<mini) {\
      rozofsstorage_netdata_cfg.val = mini;\
    }\
    else if (longval>maxi) { \
      rozofsstorage_netdata_cfg.val = maxi;\
    }\
    else {\
      rozofsstorage_netdata_cfg.val = longval;\
    }\
  }\
}


#define ROZOFSSTORAGE_NETDATA_CFG_SET_LONG_MINMAX(val,def,mini,maxi)  {\
  long long         longval;\
  if (sscanf(def,"%lld",&longval) != 1) {\
    pChar += rozofs_string_append_error(pChar,"long long integer value expected.\n");\
    return -1;\
  }\
  if (longval<mini) {\
    pChar += rozofs_string_append_error(pChar,"value lower than minimum.\n");\
    return -1;\
  }\
  if (longval>maxi) { \
    pChar += rozofs_string_append_error(pChar,"value bigger than maximum.\n");\
    return -1;\
  }\
  pChar += rozofs_string_append(pChar,#val);\
  pChar += rozofs_string_append(pChar," set to value ");\
  pChar += rozofs_string_append(pChar,def);\
  pChar += rozofs_eol(pChar);\
  rozofsstorage_netdata_cfg.val = longval;\
  return 0;\
}
#define ROZOFSSTORAGE_NETDATA_CFG_READ_STRING(val,def)  {\
  const char * charval;\
  if (rozofsstorage_netdata_cfg.val) free(rozofsstorage_netdata_cfg.val);\
  if (config_lookup_string(&cfg, #val, &charval) == CONFIG_TRUE) {\
    rozofsstorage_netdata_cfg.val = strdup(charval);\
  } else {\
    rozofsstorage_netdata_cfg.val = strdup(def);\
  }\
}

#define ROZOFSSTORAGE_NETDATA_CFG_SET_STRING(val,def)  {\
  if (rozofsstorage_netdata_cfg.val) free(rozofsstorage_netdata_cfg.val);\
  rozofsstorage_netdata_cfg.val = strdup(def);\
  pChar += rozofs_string_append(pChar,#val);\
  pChar += rozofs_string_append(pChar," set to value ");\
  pChar += rozofs_string_append(pChar,def);\
  pChar += rozofs_eol(pChar);\
  return 0;\
}


#include "rozofsstorage_netdata_cfg_read_show.h"
void rozofsstorage_netdata_cfg_extra_checks(void);

void show_rozofsstorage_netdata_cfg(char * argv[], uint32_t tcpRef, void *bufRef) {
  rozofsstorage_netdata_cfg_generated_show(argv,tcpRef,bufRef);
}

void rozofsstorage_netdata_cfg_read(char * fname) {
  rozofsstorage_netdata_cfg_generated_read(fname);
}
