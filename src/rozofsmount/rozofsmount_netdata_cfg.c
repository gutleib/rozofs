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
#include "rozofsmount_netdata_cfg.h"
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>


static char rozofsmount_netdata_cfg_file_name[256] = {0};
static int  rozofsmount_netdata_cfg_file_is_read=0;
rozofsmount_netdata_cfg_t rozofsmount_netdata_cfg;

void show_rozofsmount_netdata_cfg(char * argv[], uint32_t tcpRef, void *bufRef);
void rozofsmount_netdata_cfg_read(char * fname) ;

char   myBigBuffer[1024*1024];
static int isDefaultValue;
#define ROZOFSMOUNT_NETDATA_CFG_SHOW_NAME(val,def) {\
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

#define  ROZOFSMOUNT_NETDATA_CFG_SHOW_NEXT \
  pChar += rozofs_eol(pChar);\
  pChar += rozofs_eol(pChar);

#define  ROZOFSMOUNT_NETDATA_CFG_SHOW_END \
  *pChar++ = ';';\
  ROZOFSMOUNT_NETDATA_CFG_SHOW_NEXT

#define  ROZOFSMOUNT_NETDATA_CFG_SHOW_END_OPT(opt) \
  pChar += rozofs_string_append(pChar,"; \t// ");\
  pChar += rozofs_string_append(pChar,opt);\
  ROZOFSMOUNT_NETDATA_CFG_SHOW_NEXT

#define ROZOFSMOUNT_NETDATA_CFG_IS_DEFAULT_BOOL(val,def) \
  isDefaultValue = 0;\
  if (((rozofsmount_netdata_cfg.val)&&(strcmp(#def,"True")==0)) \
  ||  ((!rozofsmount_netdata_cfg.val)&&(strcmp(#def,"False")==0))) \
    isDefaultValue = 1;

#define ROZOFSMOUNT_NETDATA_CFG_SHOW_BOOL(val,def)  {\
  ROZOFSMOUNT_NETDATA_CFG_SHOW_NAME(val,def)\
  if (rozofsmount_netdata_cfg.val) pChar += rozofs_string_append(pChar, "True");\
  else        pChar += rozofs_string_append(pChar, "False");\
  ROZOFSMOUNT_NETDATA_CFG_SHOW_END\
}

#define ROZOFSMOUNT_NETDATA_CFG_IS_DEFAULT_STRING(val,def) \
  isDefaultValue = 0; \
  if (strcmp(rozofsmount_netdata_cfg.val,def)==0) isDefaultValue = 1;

#define ROZOFSMOUNT_NETDATA_CFG_SHOW_STRING(val,def)  {\
  ROZOFSMOUNT_NETDATA_CFG_SHOW_NAME(val,def)\
  *pChar++ = '\"';\
  if (rozofsmount_netdata_cfg.val!=NULL) pChar += rozofs_string_append(pChar, rozofsmount_netdata_cfg.val);\
  *pChar++ = '\"';\
  ROZOFSMOUNT_NETDATA_CFG_SHOW_END\
}

#define ROZOFSMOUNT_NETDATA_CFG_IS_DEFAULT_ENUM(val,def) \
  isDefaultValue = 0; \
  if (rozofsmount_netdata_cfg.val == string2rozofsmount_netdata_cfg_ ## val (def)) isDefaultValue = 1;

#define ROZOFSMOUNT_NETDATA_CFG_SHOW_ENUM(val,def,opt)  {\
  ROZOFSMOUNT_NETDATA_CFG_SHOW_NAME(val,def)\
  *pChar++ = '\"';\
  pChar += rozofs_string_append(pChar, rozofsmount_netdata_cfg_ ## val ## 2String(rozofsmount_netdata_cfg.val));\
  *pChar++ = '\"';\
  ROZOFSMOUNT_NETDATA_CFG_SHOW_END_OPT(opt)\
}

#define ROZOFSMOUNT_NETDATA_CFG_IS_DEFAULT_INT(val,def) \
  isDefaultValue = 0; \
  if (rozofsmount_netdata_cfg.val == def) isDefaultValue = 1;

#define ROZOFSMOUNT_NETDATA_CFG_SHOW_INT(val,def)  {\
  ROZOFSMOUNT_NETDATA_CFG_SHOW_NAME(val,def)\
  pChar += rozofs_i32_append(pChar, rozofsmount_netdata_cfg.val);\
  ROZOFSMOUNT_NETDATA_CFG_SHOW_END\
}

#define ROZOFSMOUNT_NETDATA_CFG_IS_DEFAULT_INT_OPT(val,def)  ROZOFSMOUNT_NETDATA_CFG_IS_DEFAULT_INT(val,def)
#define ROZOFSMOUNT_NETDATA_CFG_SHOW_INT_OPT(val,def,opt)  {\
  ROZOFSMOUNT_NETDATA_CFG_SHOW_NAME(val,def)\
  pChar += rozofs_i32_append(pChar, rozofsmount_netdata_cfg.val);\
  ROZOFSMOUNT_NETDATA_CFG_SHOW_END_OPT(opt)\
}

#define ROZOFSMOUNT_NETDATA_CFG_IS_DEFAULT_LONG(val,def)  ROZOFSMOUNT_NETDATA_CFG_IS_DEFAULT_INT(val,def)
#define ROZOFSMOUNT_NETDATA_CFG_SHOW_LONG(val,def)  {\
  ROZOFSMOUNT_NETDATA_CFG_SHOW_NAME(val,def)\
  pChar += rozofs_i64_append(pChar, rozofsmount_netdata_cfg.val);\
  ROZOFSMOUNT_NETDATA_CFG_SHOW_END\
}

#define ROZOFSMOUNT_NETDATA_CFG_IS_DEFAULT_LONG_OPT(val,def)  ROZOFSMOUNT_NETDATA_CFG_IS_DEFAULT_INT(val,def)
#define ROZOFSMOUNT_NETDATA_CFG_SHOW_LONG_OPT(val,def,opt)  {\
  ROZOFSMOUNT_NETDATA_CFG_SHOW_NAME(val,def)\
  pChar += rozofs_i64_append(pChar, rozofsmount_netdata_cfg.val);\
  ROZOFSMOUNT_NETDATA_CFG_SHOW_END_OPT(opt)\
}

#define ROZOFSMOUNT_NETDATA_CFG_READ_BOOL(val,def)  {\
  int  boolval;\
  if (strcmp(#def,"True")==0) {\
    rozofsmount_netdata_cfg.val = 1;\
  } else {\
    rozofsmount_netdata_cfg.val = 0;\
  }\
  if (config_lookup_bool(&cfg, #val, &boolval) == CONFIG_TRUE) { \
    rozofsmount_netdata_cfg.val = boolval;\
  }\
}
#define ROZOFSMOUNT_NETDATA_CFG_SET_BOOL(val,def)  {\
  if (strcmp(def,"True")==0) {\
    rozofsmount_netdata_cfg.val = 1;\
    pChar += rozofs_string_append(pChar,#val);\
    pChar += rozofs_string_append(pChar," set to value ");\
    pChar += rozofs_string_append(pChar,def);\
    pChar += rozofs_eol(pChar);\
    return 0;\
  }\
  if (strcmp(def,"False")==0) {\
    rozofsmount_netdata_cfg.val = 0;\
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

#define ROZOFSMOUNT_NETDATA_CFG_READ_INT_MINMAX(val,def,mini,maxi)  {\
  rozofsmount_netdata_cfg.val = def;\
  if (config_lookup_int(&cfg, #val, &intval) == CONFIG_TRUE) { \
    if (intval<mini) {\
      rozofsmount_netdata_cfg.val = mini;\
    }\
    else if (intval>maxi) { \
      rozofsmount_netdata_cfg.val = maxi;\
    }\
    else {\
      rozofsmount_netdata_cfg.val = intval;\
    }\
  }\
}

#define ROZOFSMOUNT_NETDATA_CFG_SET_INT_MINMAX(val,def,mini,maxi)  {\
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
  rozofsmount_netdata_cfg.val = valint;\
  return 0;\
}

#define ROZOFSMOUNT_NETDATA_CFG_READ_INT(val,def) {\
  rozofsmount_netdata_cfg.val = def;\
  if (config_lookup_int(&cfg, #val, &intval) == CONFIG_TRUE) { \
    rozofsmount_netdata_cfg.val = intval;\
  }\
}

#define ROZOFSMOUNT_NETDATA_CFG_SET_INT(val,def)  {\
  int valint;\
  if (sscanf(def,"%d",&valint) != 1) {\
    pChar += rozofs_string_append_error(pChar,"integer value expected.\n");\
    return -1;\
  }\
  pChar += rozofs_string_append(pChar, #val);\
  pChar += rozofs_string_append(pChar," set to value ");\
  pChar += rozofs_string_append(pChar,def);\
  pChar += rozofs_eol(pChar);\
  rozofsmount_netdata_cfg.val = valint;\
  return 0;\
}

#define ROZOFSMOUNT_NETDATA_CFG_READ_LONG(val,def) {\
  long long         longval;\
  rozofsmount_netdata_cfg.val = def;\
  if (config_lookup_int64(&cfg, #val, &longval) == CONFIG_TRUE) { \
    rozofsmount_netdata_cfg.val = longval;\
  }\
}

#define ROZOFSMOUNT_NETDATA_CFG_SET_LONG(val,def) {\
  long long         longval;\
  if (sscanf(def,"%lld",&longval) != 1) {\
    pChar += rozofs_string_append_error(pChar,"long long integer value expected.\n");\
    return -1;\
  }\
  pChar += rozofs_string_append(pChar,#val);\
  pChar += rozofs_string_append(pChar," set to value ");\
  pChar += rozofs_string_append(pChar,def);\
  pChar += rozofs_eol(pChar);\
  rozofsmount_netdata_cfg.val = longval;\
  return 0;\
}


#define ROZOFSMOUNT_NETDATA_CFG_READ_LONG_MINMAX(val,def,mini,maxi)  {\
  long long         longval;\
  rozofsmount_netdata_cfg.val = def;\
  if (config_lookup_int64(&cfg, #val, &longval) == CONFIG_TRUE) { \
    if (longval<mini) {\
      rozofsmount_netdata_cfg.val = mini;\
    }\
    else if (longval>maxi) { \
      rozofsmount_netdata_cfg.val = maxi;\
    }\
    else {\
      rozofsmount_netdata_cfg.val = longval;\
    }\
  }\
}


#define ROZOFSMOUNT_NETDATA_CFG_SET_LONG_MINMAX(val,def,mini,maxi)  {\
  long long         longval;\
  if (sscanf(def,"%lld",&longval) != 1) {\
    pChar += rozofs_string_append_error(pChar,"long long integer value expected.\n"));\
    return -1;\
  }\
  if (longval<mini) {\
    pChar += rozofs_string_append_error(pChar,"value lower than minimum.\n"));\
    return -1;\
  }\
  if (longval>maxi) { \
    pChar += rozofs_string_append_error(pChar,"value bigger than maximum.\n"));\
    return -1;\
  }\
  pChar += rozofs_string_append(pChar,#val);\
  pChar += rozofs_string_append(pChar," set to value ");\
  pChar += rozofs_string_append(pChar,def);\
  pChar += rozofs_eol(pChar);\
  rozofsmount_netdata_cfg.val = longval;\
  return 0;\
}
#define ROZOFSMOUNT_NETDATA_CFG_READ_STRING(val,def)  {\
  const char * charval;\
  if (rozofsmount_netdata_cfg.val) free(rozofsmount_netdata_cfg.val);\
  if (config_lookup_string(&cfg, #val, &charval) == CONFIG_TRUE) {\
    rozofsmount_netdata_cfg.val = strdup(charval);\
  } else {\
    rozofsmount_netdata_cfg.val = strdup(def);\
  }\
}

#define ROZOFSMOUNT_NETDATA_CFG_SET_STRING(val,def)  {\
  if (rozofsmount_netdata_cfg.val) free(rozofsmount_netdata_cfg.val);\
  rozofsmount_netdata_cfg.val = strdup(def);\
  pChar += rozofs_string_append(pChar,#val);\
  pChar += rozofs_string_append(pChar," set to value ");\
  pChar += rozofs_string_append(pChar,def);\
  pChar += rozofs_eol(pChar);\
  return 0;\
}


#include "rozofsmount_netdata_cfg_read_show.h"
void rozofsmount_netdata_cfg_extra_checks(void);

void show_rozofsmount_netdata_cfg(char * argv[], uint32_t tcpRef, void *bufRef) {
  rozofsmount_netdata_cfg_generated_show(argv,tcpRef,bufRef);
}

void rozofsmount_netdata_cfg_read(char * fname) {
  rozofsmount_netdata_cfg_generated_read(fname);
}
