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
#include "common_config.h"
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>


static char common_config_file_name[256] = {0};
static int  common_config_file_is_read=0;
common_config_t common_config;

void show_common_config(char * argv[], uint32_t tcpRef, void *bufRef);
void common_config_read(char * fname) ;

char   myBigBuffer[1024*1024];
static int isDefaultValue;
#define COMMON_CONFIG_SHOW_NAME(val,def) {\
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

#define  COMMON_CONFIG_SHOW_NEXT \
  pChar += rozofs_eol(pChar);\
  pChar += rozofs_eol(pChar);

#define  COMMON_CONFIG_SHOW_END \
  *pChar++ = ';';\
  COMMON_CONFIG_SHOW_NEXT

#define  COMMON_CONFIG_SHOW_END_OPT(opt) \
  pChar += rozofs_string_append(pChar,"; \t// ");\
  pChar += rozofs_string_append(pChar,opt);\
  COMMON_CONFIG_SHOW_NEXT

#define COMMON_CONFIG_IS_DEFAULT_BOOL(val,def) \
  isDefaultValue = 0;\
  if (((common_config.val)&&(strcmp(#def,"True")==0)) \
  ||  ((!common_config.val)&&(strcmp(#def,"False")==0))) \
    isDefaultValue = 1;

#define COMMON_CONFIG_SHOW_BOOL(val,def)  {\
  COMMON_CONFIG_SHOW_NAME(val,def)\
  if (common_config.val) pChar += rozofs_string_append(pChar, "True");\
  else        pChar += rozofs_string_append(pChar, "False");\
  COMMON_CONFIG_SHOW_END\
}

#define COMMON_CONFIG_IS_DEFAULT_STRING(val,def) \
  isDefaultValue = 0; \
  if (strcmp(common_config.val,def)==0) isDefaultValue = 1;

#define COMMON_CONFIG_SHOW_STRING(val,def)  {\
  COMMON_CONFIG_SHOW_NAME(val,def)\
  *pChar++ = '\"';\
  if (common_config.val!=NULL) pChar += rozofs_string_append(pChar, common_config.val);\
  *pChar++ = '\"';\
  COMMON_CONFIG_SHOW_END\
}

#define COMMON_CONFIG_IS_DEFAULT_ENUM(val,def) \
  isDefaultValue = 0; \
  if (common_config.val == string2common_config_ ## val (def)) isDefaultValue = 1;

#define COMMON_CONFIG_SHOW_ENUM(val,def,opt)  {\
  COMMON_CONFIG_SHOW_NAME(val,def)\
  *pChar++ = '\"';\
  pChar += rozofs_string_append(pChar, common_config_ ## val ## 2String(common_config.val));\
  *pChar++ = '\"';\
  COMMON_CONFIG_SHOW_END_OPT(opt)\
}

#define COMMON_CONFIG_IS_DEFAULT_INT(val,def) \
  isDefaultValue = 0; \
  if (common_config.val == def) isDefaultValue = 1;

#define COMMON_CONFIG_SHOW_INT(val,def)  {\
  COMMON_CONFIG_SHOW_NAME(val,def)\
  pChar += rozofs_i32_append(pChar, common_config.val);\
  COMMON_CONFIG_SHOW_END\
}

#define COMMON_CONFIG_IS_DEFAULT_INT_OPT(val,def)  COMMON_CONFIG_IS_DEFAULT_INT(val,def)
#define COMMON_CONFIG_SHOW_INT_OPT(val,def,opt)  {\
  COMMON_CONFIG_SHOW_NAME(val,def)\
  pChar += rozofs_i32_append(pChar, common_config.val);\
  COMMON_CONFIG_SHOW_END_OPT(opt)\
}

#define COMMON_CONFIG_IS_DEFAULT_LONG(val,def)  COMMON_CONFIG_IS_DEFAULT_INT(val,def)
#define COMMON_CONFIG_SHOW_LONG(val,def)  {\
  COMMON_CONFIG_SHOW_NAME(val,def)\
  pChar += rozofs_i64_append(pChar, common_config.val);\
  COMMON_CONFIG_SHOW_END\
}

#define COMMON_CONFIG_IS_DEFAULT_LONG_OPT(val,def)  COMMON_CONFIG_IS_DEFAULT_INT(val,def)
#define COMMON_CONFIG_SHOW_LONG_OPT(val,def,opt)  {\
  COMMON_CONFIG_SHOW_NAME(val,def)\
  pChar += rozofs_i64_append(pChar, common_config.val);\
  COMMON_CONFIG_SHOW_END_OPT(opt)\
}

#define COMMON_CONFIG_READ_BOOL(val,def)  {\
  int  boolval;\
  if (strcmp(#def,"True")==0) {\
    common_config.val = 1;\
  } else {\
    common_config.val = 0;\
  }\
  if (config_lookup_bool(&cfg, #val, &boolval) == CONFIG_TRUE) { \
    common_config.val = boolval;\
  }\
}
#define COMMON_CONFIG_SET_BOOL(val,def)  {\
  if (strcmp(def,"True")==0) {\
    common_config.val = 1;\
    pChar += rozofs_string_append(pChar,#val);\
    pChar += rozofs_string_append(pChar," set to value ");\
    pChar += rozofs_string_append(pChar,def);\
    pChar += rozofs_eol(pChar);\
    return 0;\
  }\
  if (strcmp(def,"False")==0) {\
    common_config.val = 0;\
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

#define COMMON_CONFIG_READ_INT_MINMAX(val,def,mini,maxi)  {\
  common_config.val = def;\
  if (config_lookup_int(&cfg, #val, &intval) == CONFIG_TRUE) { \
    if (intval<mini) {\
      common_config.val = mini;\
    }\
    else if (intval>maxi) { \
      common_config.val = maxi;\
    }\
    else {\
      common_config.val = intval;\
    }\
  }\
}

#define COMMON_CONFIG_SET_INT_MINMAX(val,def,mini,maxi)  {\
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
  common_config.val = valint;\
  return 0;\
}

#define COMMON_CONFIG_READ_INT(val,def) {\
  common_config.val = def;\
  if (config_lookup_int(&cfg, #val, &intval) == CONFIG_TRUE) { \
    common_config.val = intval;\
  }\
}

#define COMMON_CONFIG_SET_INT(val,def)  {\
  int valint;\
  if (sscanf(def,"%d",&valint) != 1) {\
    pChar += rozofs_string_append_error(pChar,"integer value expected.\n");\
    return -1;\
  }\
  pChar += rozofs_string_append(pChar, #val);\
  pChar += rozofs_string_append(pChar," set to value ");\
  pChar += rozofs_string_append(pChar,def);\
  pChar += rozofs_eol(pChar);\
  common_config.val = valint;\
  return 0;\
}

#define COMMON_CONFIG_READ_LONG(val,def) {\
  long long         longval;\
  common_config.val = def;\
  if (config_lookup_int64(&cfg, #val, &longval) == CONFIG_TRUE) { \
    common_config.val = longval;\
  }\
}

#define COMMON_CONFIG_SET_LONG(val,def) {\
  long long         longval;\
  if (sscanf(def,"%lld",&longval) != 1) {\
    pChar += rozofs_string_append_error(pChar,"long long integer value expected.\n");\
    return -1;\
  }\
  pChar += rozofs_string_append(pChar,#val);\
  pChar += rozofs_string_append(pChar," set to value ");\
  pChar += rozofs_string_append(pChar,def);\
  pChar += rozofs_eol(pChar);\
  common_config.val = longval;\
  return 0;\
}


#define COMMON_CONFIG_READ_LONG_MINMAX(val,def,mini,maxi)  {\
  long long         longval;\
  common_config.val = def;\
  if (config_lookup_int64(&cfg, #val, &longval) == CONFIG_TRUE) { \
    if (longval<mini) {\
      common_config.val = mini;\
    }\
    else if (longval>maxi) { \
      common_config.val = maxi;\
    }\
    else {\
      common_config.val = longval;\
    }\
  }\
}


#define COMMON_CONFIG_SET_LONG_MINMAX(val,def,mini,maxi)  {\
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
  common_config.val = longval;\
  return 0;\
}
#define COMMON_CONFIG_READ_STRING(val,def)  {\
  const char * charval;\
  if (common_config.val) free(common_config.val);\
  if (config_lookup_string(&cfg, #val, &charval) == CONFIG_TRUE) {\
    common_config.val = strdup(charval);\
  } else {\
    common_config.val = strdup(def);\
  }\
}

#define COMMON_CONFIG_SET_STRING(val,def)  {\
  if (common_config.val) free(common_config.val);\
  common_config.val = strdup(def);\
  pChar += rozofs_string_append(pChar,#val);\
  pChar += rozofs_string_append(pChar," set to value ");\
  pChar += rozofs_string_append(pChar,def);\
  pChar += rozofs_eol(pChar);\
  return 0;\
}


#include "common_config_read_show.h"
void common_config_extra_checks(void);

void show_common_config(char * argv[], uint32_t tcpRef, void *bufRef) {
  common_config_generated_show(argv,tcpRef,bufRef);
}

void common_config_read(char * fname) {
  common_config_generated_read(fname);

  /*
  ** Add some consistency checks
  */
  common_config_extra_checks();
}
