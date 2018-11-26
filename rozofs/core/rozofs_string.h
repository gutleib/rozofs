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
 
#ifndef ROZOFS_STRING_H
#define ROZOFS_STRING_H

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <uuid/uuid.h>
#include <sys/stat.h>

#ifdef __cplusplus
extern "C" {
#endif /*__cplusplus*/

typedef enum _rozofs_alignment_e {
 rozofs_left_alignment,
 rozofs_right_alignment,
 rozofs_zero,
} rozofs_alignment_e;



#define ROZOFS_COLOR_CYAN       "\033[96m"
#define ROZOFS_COLOR_YELLOW     "\033[93m"
#define ROZOFS_COLOR_BLUE       "\033[94m"
#define ROZOFS_COLOR_GREEN      "\033[92m"
#define ROZOFS_COLOR_PURPLE     "\033[95m"
#define ROZOFS_COLOR_RED        "\033[91m"
#define ROZOFS_COLOR_WHITE      "\033[97m"

#define ROZOFS_COLOR_BOLD       "\033[1m"
#define ROZOFS_COLOR_REVERSE    "\033[7m"
#define ROZOFS_COLOR_UNDERSCORE "\033[4m"
#define ROZOFS_COLOR_NONE       "\033[0m"

/*
 *_______________________________________________________________________
 *  Build a string from a date given by time() API
 *
 *  @param pChar : where to store the formated date
 *  
 *  @retval the formated size
 *_______________________________________________________________________
 */
static inline int rozofs_time2string(char * pChar, time_t loc_time) {
  struct tm date;

  localtime_r(&loc_time,&date);
  return sprintf(pChar, 
                 "%4d-%2.2d-%2.2d-%2.2d:%2.2d:%2.2d", 
                 date.tm_year+1900, 
                 date.tm_mon+1, 
                 date.tm_mday,
                 date.tm_hour,
                 date.tm_min,
                 date.tm_sec);
}      
/*
 *_______________________________________________________________________
 *  Add current time to string
 *
 *  @param pChar : where to store the formated date
 *  
 *  @retval the formated size
 *_______________________________________________________________________
 */
static inline int rozofs_add_timestring(char * pChar) {
  return rozofs_time2string(pChar,time(NULL));
} 







/*
** ===================== FID ==================================
*/





/*
**___________________________________________________________
** Parse a 2 character string representing an hexadecimal
** value.
**
** @param pChar   The starting of the 2 characters
** @param hexa    Where to return the hexadecimal value
**
** @retval The next place to parse in the string or NULL
**         when the 2 characters do not represent an hexadecimal 
**         value
*/
static inline char * rozofs_2char2uint8(char * pChar, uint8_t * hexa) {
  uint8_t val;
  
  if ((*pChar >= '0')&&(*pChar <= '9')) {
    val = *pChar++ - '0';
  }  
  else if ((*pChar >= 'a')&&(*pChar <= 'f')) {
    val = *pChar++ - 'a' + 10; 
  }
  else if ((*pChar >= 'A')&&(*pChar <= 'F')) {
    val = *pChar++ - 'A' + 10; 
  }
  else {
    return NULL;
  }
  
  val = val << 4;
  
  if ((*pChar >= '0')&&(*pChar <= '9')) {
    val += *pChar++ - '0';
  }  
  else if ((*pChar >= 'a')&&(*pChar <= 'f')) {
    val += *pChar++ - 'a' + 10; 
  }
  else if ((*pChar >= 'A')&&(*pChar <= 'F')) {
    val += *pChar++ - 'A' +10; 
  }
  else {
    return NULL;
  }  
  *hexa = val;
  return pChar;
}
/*
**___________________________________________________________
** Parse a string representing an FID
**
** @param pChar   The string containing the FID
** @param fid     The parsed FID
**
** @retval The next place to parse in the string after the FID 
**         or NULL when the string do not represent an FID
*/
static inline int rozofs_uuid_parse(char * pChar, uuid_t fid) {
  uint8_t *pFid = (uint8_t*) fid;

  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return -1;
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return -1;
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return -1;
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return -1;
     
  if (*pChar++ != '-') return -1;  
  
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return -1;
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return -1;
  
  if (*pChar++ != '-') return -1;  

  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return -1;
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return -1;
  
  if (*pChar++ != '-') return -1;  

  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return -1;
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return -1;
  
  if (*pChar++ != '-') return -1;  
  
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return -1;  
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return -1;  
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return -1;  
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return -1;  
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return -1;  
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return -1;  
   
  return 0;
}

/*
**___________________________________________________________
** Get the a byte value and translate into 2 ASCII chars
**
** @param hexa    The byte value to display
** @param pChar   Where to write the ASCII translation
**
*/
static inline char * rozofs_u8_2_char(uint8_t hexa, char * pChar) {
  uint8_t high = hexa >> 4;
  if (high < 10) *pChar++ = high + '0';
  else           *pChar++ = (high-10) + 'a';
  
  hexa = hexa & 0x0F;
  if (hexa < 10) *pChar++ = hexa + '0';
  else           *pChar++ = (hexa-10) + 'a';
  
  return pChar;
}
/*
**___________________________________________________________
** Get a FID value and display it as a string. An end of string
** is inserted at the end of the string.
**
** @param fid     The FID value
** @param pChar   Where to write the ASCII translation
**
** @retval The end of string
*/
static inline void rozofs_uuid_unparse(uuid_t fid, char * pChar) {
  uint8_t * pFid = (uint8_t *) fid;
  
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);   
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  
  *pChar++ = '-';
  
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
   
  *pChar++ = '-';
  
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  
  *pChar++ = '-';
  
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  
  *pChar++ = '-';
  
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);   
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);   

  *pChar = 0;  
}
static inline int rozofs_fid_append(char * pChar, uuid_t fid) {
  rozofs_uuid_unparse(fid,pChar);
  return 36;
}  















/*
** ===================== STRINGS ==================================
*/



















/*
**___________________________________________________________
** Append a string and add a 0 at the end
**
**    sprintf(pChar,"%s",new_string) 
** -> rozofs_string_append(pChar, new_string)
**
** @param pChar       The string that is being built
** @param new_string  The string to append. Must have an ending 0
**
** @retval the size added to the built string
*/
static inline int rozofs_string_append(char * pChar, char * new_string) {
  int len=0;
  if (new_string == NULL) {
    *pChar = 0;
    return 0;
  }
  while ( (*pChar++ = *new_string++) != 0) len ++;
  return len;
}
/*
**___________________________________________________________
** Set display with default effects
**
** @param pChar       The string that is being built
**
** @retval the size added to the built string
*/
static inline int rozofs_string_set_default(char * pChar) {
  return rozofs_string_append(pChar,ROZOFS_COLOR_NONE);
}
/*
**___________________________________________________________
** Append an inversed string and add a 0 at the end
**
**
** @param pChar       The string that is being built
** @param new_string  The string to append. Must have an ending 0
**
** @retval the size added to the built string
*/
static inline int rozofs_string_append_effect(char * pChar, char * new_string, char * effect) {
  int len = 0;
  len += rozofs_string_append(pChar,effect);
  len += rozofs_string_append(&pChar[len],new_string);
  len += rozofs_string_set_default(&pChar[len]);
  return len;
}
/*
**___________________________________________________________
** Append an inversed string and add a 0 at the end
**
**
** @param pChar       The string that is being built
** @param new_string  The string to append. Must have an ending 0
**
** @retval the size added to the built string
*/
static inline int rozofs_string_append_error(char * pChar, char * new_string) {
  int len = 0;
  len += rozofs_string_append(pChar,ROZOFS_COLOR_BOLD ROZOFS_COLOR_RED);
  len += rozofs_string_append(&pChar[len],new_string);
  len += rozofs_string_set_default(&pChar[len]);
  return len;
}
/*
**___________________________________________________________
** Set display in underscore
**
** @param pChar       The string that is being built
**
** @retval the size added to the built string
*/
static inline int rozofs_string_set_underscore(char * pChar) {
  return rozofs_string_append(pChar,ROZOFS_COLOR_UNDERSCORE);
}
/*
**___________________________________________________________
** Set display in bold
**
** @param pChar       The string that is being built
**
** @retval the size added to the built string
*/
static inline int rozofs_string_set_bold(char * pChar) {
  return rozofs_string_append(pChar,ROZOFS_COLOR_BOLD);
}
/*
**___________________________________________________________
** Set display with inverse effect
**
** @param pChar       The string that is being built
**
** @retval the size added to the built string
*/
static inline int rozofs_string_set_inverse(char * pChar) {
  return rozofs_string_append(pChar,ROZOFS_COLOR_REVERSE);
}
/*
**___________________________________________________________
** Append a bold string and add a 0 at the end
**
**
** @param pChar       The string that is being built
** @param new_string  The string to append. Must have an ending 0
**
** @retval the size added to the built string
*/
static inline int rozofs_string_append_bold(char * pChar, char * new_string) {
  int len = 0;
  len += rozofs_string_set_bold(&pChar[len]);
  len += rozofs_string_append(&pChar[len],new_string);
  len += rozofs_string_set_default(&pChar[len]);
  return len;
}
/*
**___________________________________________________________
** Append an underscored string and add a 0 at the end
**
**
** @param pChar       The string that is being built
** @param new_string  The string to append. Must have an ending 0
**
** @retval the size added to the built string
*/
static inline int rozofs_string_append_underscore(char * pChar, char * new_string) {
  int len = 0;
  len += rozofs_string_set_underscore(&pChar[len]);
  len += rozofs_string_append(&pChar[len],new_string);
  len += rozofs_string_set_default(&pChar[len]);
  return len;
}
/*
**___________________________________________________________
** Append an inversed string and add a 0 at the end
**
**
** @param pChar       The string that is being built
** @param new_string  The string to append. Must have an ending 0
**
** @retval the size added to the built string
*/
static inline int rozofs_string_append_inverse(char * pChar, char * new_string) {
  int len = 0;
  len += rozofs_string_set_inverse(&pChar[len]);
  len += rozofs_string_append(&pChar[len],new_string);
  len += rozofs_string_set_default(&pChar[len]);
  return len;
}
/*
**___________________________________________________________
** Append a string, padd with ' ' on a given size
** and add a 0 at the end
**
**     sprintf(pChar,"%-12s",new_string) 
**  -> rozofs_string_padded_append(pChar, 12, rozofs_right_alignment,new_string)
**
**     sprintf(pChar,"%16s",new_string)
**  -> rozofs_string_padded_append(pChar, 16, rozofs_left_alignment,new_string)
**
** @param pChar       The string that is being built
** @param size        The total size to write to the built string
** @param alignment   Left/right alignment
** @param new_string  The string to append. Must have an ending 0
**
** @retval the size added to the string
*/
static inline int rozofs_string_padded_append(char * pChar, int size, rozofs_alignment_e alignment, char * new_string) {
  int i;
  int len;
  
  if (new_string == NULL) {
    len = 0;
  }
  else {
    len = strlen(new_string);
  }
  
  /*
  ** new_string is too big => truncate it
  */
  if (len > size) {
    strncpy(pChar,new_string,size);
    pChar[size] = 0;
    return size;
  }
  /*
  ** Put left alignment
  */
  if (alignment == rozofs_right_alignment) {
    for (i=0; i< (size-len) ; i++) {
      *pChar++ = ' ';
    }
  }
  /*
  ** Append string
  */
  for (i=0; i< len ; i++) {
    *pChar++ = *new_string++;
  } 
  /*
  ** Put right alignment
  */  
  if (alignment == rozofs_left_alignment) {
    for (i=0; i< (size-len) ; i++) {
      *pChar++ = ' ';
    }
  }   
  /*
  ** Add the end of string
  */
  *pChar = 0;
  return size;
}
/*
**___________________________________________________________
** Append a string, padd with ' ' on a given size
** and add a 0 at the end
**
**     sprintf(pChar,"%-12s",new_string) 
**  -> rozofs_string_padded_append(pChar, 12, rozofs_right_alignment,new_string)
**
**     sprintf(pChar,"%16s",new_string)
**  -> rozofs_string_padded_append(pChar, 16, rozofs_left_alignment,new_string)
**
** @param pChar       The string that is being built
** @param size        The total size to write to the built string
** @param alignment   Left/right alignment
** @param new_string  The string to append. Must have an ending 0
**
** @retval the size added to the string
*/
static inline int rozofs_string_padded_append_bold(char * pChar, int size, rozofs_alignment_e alignment, char * new_string) {
  int len = 0;
  len += rozofs_string_set_bold(&pChar[len]);
  len += rozofs_string_padded_append(&pChar[len],size,alignment,new_string);
  len += rozofs_string_set_default(&pChar[len]);
  return len;
}  
/*
**___________________________________________________________
** Append an end of line and a 0 at the end
**
**    sprintf(pChar,"\n",) 
** -> rozofs_eol(pChar)
**
** @param pChar       The string that is being built
**
** @retval the size added to the built string
*/
static inline int rozofs_eol(char * pChar) {
  *pChar++ = '\n';
  *pChar   = 0;
  return 1;
}


/*
**___________________________________________________________
** Draw a line
** 
** @param column_len      array of column length
**
** @retval the size added to the string
*/
static inline char * rozofs_line(char * pChar, uint8_t * column_len) {
  int len;
  *pChar++ = '+';
  
  len = *column_len++;
  while(len) {
    
    while(len) {
      *pChar++ = '-';
      len--;
    }  
    *pChar++ = '+';
    
    len = *column_len++;
  }
  *pChar++ = '\n';
  *pChar = 0;
  return pChar;
}








/*
** ===================== INTERNAL FUNCT FOR 64bits VALUES  ======================
*/














/*
**___________________________________________________________
** For internal use
** Append a 64 bit number in decimal representation 
** with eventually a minus sign. Add a 0 at the end.
**
** @param pChar       The string that is being built
** @param sign        Whether a sign must be added (0/1)
** @param val         The 64bits unsigned (so positive)value
**
** @retval the size added to the string
*/
static inline int rozofs_64_append(char * pChar, int sign, uint64_t val) {
  int len=0;
  uint8_t  values[32];
  int i;
  
  /*
  ** Decompose the value in 10 base
  */
  len = 0;
  while (val) {
    values[len] = val % 10;
    val = val / 10;    
    len++;
  }
  
  /*
  ** Given value was 0
  */
  if (len == 0) {
    values[0] = 0;
    len = 1;
  }  

  /*
  ** Put sign if required
  */
  if (sign) *pChar++ = '-';
  
  /*
  ** Write the value
  */
  for (i=len-1; i>=0; i--) {
    *pChar++ = values[i]+ '0';
  } 
  /*
  ** Add the end of string
  */   
  *pChar = 0;
  return len+sign;
}
/*
**___________________________________________________________
** For internal use
** Append a 64 bits unsigned number with eventually a minus sign
** in decimal representation on a fixed size to a string. padd with
** ' ' and add a 0 at the end.
**
** @param pChar       The string that is being built
** @param size        The size to add to the string
** @param alignment   Left/right alignment
** @param sign        Whether a sign must be added
** @param val         The 64bits unsigned value
**
** @retval the size added to the strings
*/
static inline int rozofs_64_padded_append(char * pChar, int size, rozofs_alignment_e alignment, int sign, uint64_t val) {
  int len=0;
  uint8_t  values[32];
  int i;
  
  /*
  ** Decompose the value in 10 base
  */  
  len = 0;
  while (val) {
    values[len] = val % 10;
    val = val / 10;    
    len++;
  }
  /*
  ** Given value was 0
  */  
  if (len == 0) {
    values[0] = 0;
    len = 1;
  }  

  len += sign;
  
  /*
  ** Put left alignment
  */
  if (alignment == rozofs_right_alignment) {
    for (i=0; i< (size-len) ; i++) {
      *pChar++ = ' ';
    }
  } 
   
  /*
  ** Put sign if required
  */
  if (sign) *pChar++ = '-';
  
  /*
  ** 0 left padding
  */
  if (alignment == rozofs_zero) {
    for (i=0; i< (size-len) ; i++) {
      *pChar++ = '0';
    }
  }  

  /*
  ** Write the value
  */
  for (i=len-1-sign; i>=0; i--) {
    *pChar++ = values[i]+ '0';
  }
  if (len>size) {
    *pChar = 0;
    return len;
  }
  /*
  ** Put right alignment
  */
  if (alignment == rozofs_left_alignment) {
    for (i=0; i< (size-len) ; i++) {
      *pChar++ = ' ';
    }
  }     
  /*
  ** Add the end of string
  */   
  *pChar = 0;
  return size;
} 



















/*
** ===================== 32 bits FORMATING  ======================
*/


















/*
**___________________________________________________________
** Append an unsigned 32 bit number in decimal representation
** to a string. Add a 0 at the end.
**
**     sprintf(pChar,"%u",val) 
**  -> rozofs_u32_append(pChar, val)
**
** @param pChar       The string that is being built
** @param val         The 32bits unsigned value
**
** @retval the size added to the string
*/
static inline int rozofs_u32_append(char * pChar, uint32_t val) {
  return rozofs_64_append(pChar, 0, val);
}
/*
**___________________________________________________________
** Append a signed 32 bit number in decimal representation
** to a string. Add a 0 at the end.
**
**     sprintf(pChar,"%d",val) 
**  -> rozofs_i32_append(pChar, val)
**
** @param pChar       The string that is being built
** @param val         The 32bits signed value
**
** @retval the size added to the string
*/
static inline int rozofs_i32_append(char * pChar, int32_t val) {
  if (val<0) return rozofs_64_append(pChar, 1, -val);
  return rozofs_64_append(pChar, 0, val);
}
/*
**___________________________________________________________
** Append an unsigned 32 bit number in hexadecimal representation
** to a string. Add a 0 at the end.
**
**     sprintf(pChar,"%8.8x",val) 
**  -> rozofs_x32_append(pChar, val)
**
** @param pChar       The string that is being built
** @param val         The 32bits unsigned value
**
** @retval the size added to the string
*/
static inline int rozofs_x32_append(char * pChar, uint32_t val) {
  int i;
  uint32_t v;

  for (i=0; i<8; i++) {
    v = val & 0xF;
    val = val >> 4;
    if (v<10) pChar[7-i] = v + '0';
    else      pChar[7-i] = v + 'a' - 10;
  }
  /*
  ** Add the end of string
  */      
  pChar[8] = 0;
  return 8;
} 
/*
**___________________________________________________________
** Append a 32 bits signed number in decimal representation 
** on a fixed size to a string. padd with ' ' and add a 0 at 
** the end.
**
** sprintf(pChar,"%9d",val) 
**  -> rozofs_i32_append(pChar, 9, rozofs_left_alignment, val)
**
** sprintf(pChar,"%-10d",val) 
**  -> rozofs_i32_append(pChar, 10, rozofs_right_alignment, val)
**
** @param pChar       The string that is being built
** @param size        The size to add to the string
** @param alignment   Left/right alignment
** @param val         The 32bits signed value
**
** @retval the size added to the string
*/
static inline int rozofs_i32_padded_append(char * pChar, int size, rozofs_alignment_e alignment, int32_t val) {
  if (val < 0) return rozofs_64_padded_append(pChar, size, alignment, 1, -val);
  return rozofs_64_padded_append(pChar, size, alignment, 0, val);
} 
/*
**___________________________________________________________
** Append a 32 bits unsigned number in decimal representation 
** on a fixed size to a string. padd with ' ' and add a 0 at 
** the end.
**
** sprintf(pChar,"%10u",val) 
**  -> rozofs_u32_append(pChar, 10, rozofs_left_alignment, val)
**
** sprintf(pChar,"%-12u",val) 
**  -> rozofs_u32_append(pChar, 12, rozofs_right_alignment, val)
**
** @param pChar       The string that is being built
** @param size        The size to add to the string
** @param alignment   Left/right alignment
** @param val         The 32bits unsigned value
**
** @retval the size added to the string
*/
static inline int rozofs_u32_padded_append(char * pChar, int size, rozofs_alignment_e alignment, uint32_t val) {
  return rozofs_64_padded_append(pChar, size, alignment, 0, val);
} 





















/*
** ===================== 64 bits FORMATING  ======================
*/












/*
**___________________________________________________________
** Append an unsigned 64 bit number in decimal representation
** to a string. Add a 0 at the end.
**
**     sprintf(pChar,"%llu",val) 
**  -> rozofs_u64_append(pChar, val)
**
** @param pChar       The string that is being built
** @param val         The 64bits unsigned value
**
** @retval the size added to the string
*/
static inline int rozofs_u64_append(char * pChar, uint64_t val) {
  return rozofs_64_append(pChar, 0, val);
}
/*
**___________________________________________________________
** Append a signed 64 bit number in decimal representation
** to a string. Add a 0 at the end.
**
** sprintf(pChar,"%lld",val) 
**  -> rozofs_i64_append(pChar, val)
**
** @param pChar       The string that is being built
** @param val         The 32bits signed value
**
** @retval the size added to the string
*/
static inline int rozofs_i64_append(char * pChar, int64_t val) {
  if (val<0) return rozofs_64_append(pChar, 1, -val);
  return rozofs_64_append(pChar, 0, val);
}
/*
**___________________________________________________________
** Append an unsigned 64 bit number in hexadecimal representation
** to a string. Add a 0 at the end.
**
** sprintf(pChar,"%16.16llx",val) 
**  -> rozofs_x64_append(pChar, val)
**
** @param pChar       The string that is being built
** @param val         The 64bits unsigned value
**
** @retval the size added to the string
*/
static inline int rozofs_x64_append(char * pChar, uint64_t val) {
  int i;
  uint32_t v;

  for (i=0; i<16; i++) {
    v = val & 0xF;
    val = val >> 4;
    if (v<10) pChar[15-i] = v + '0';
    else      pChar[15-i] = v + 'a' - 10;
  }
  /*
  ** Add the end of string
  */      
  pChar[16] = 0;
  return 16;
} 
/*
**___________________________________________________________
** Append a 64 bits signed number in decimal representation 
** on a fixed size to a string. padd with ' ' and add a 0 at 
** the end.
**
** sprintf(pChar,"%7lld",val) 
**  -> rozofs_x32_append(pChar, 7, rozofs_left_alignment, val)
**
** sprintf(pChar,"%-10lld",val) 
**  -> rozofs_x32_append(pChar, 10, rozofs_right_alignment, val)
**
** @param pChar       The string that is being built
** @param size        The size to add to the string
** @param alignment   Left/right alignment
** @param val         The 64bits signed value
**
** @retval the size added to the string
*/
static inline int rozofs_i64_padded_append(char * pChar, int size, rozofs_alignment_e alignment, int64_t val) {
  if (val < 0) return rozofs_64_padded_append(pChar, size, alignment, 1, -val);
  return rozofs_64_padded_append(pChar, size, alignment, 0, val);
} 
/*
**___________________________________________________________
** Append a 64 bits unsigned number in decimal representation 
** on a fixed size to a string. padd with ' ' and add a 0 at 
** the end.
**
** sprintf(pChar,"%20llu",val) 
**  -> rozofs_u64_append(pChar, 20, rozofs_left_alignment, val)
**
** sprintf(pChar,"%-6llu",val) 
**  -> rozofs_u64_append(pChar, 6, rozofs_right_alignment, val)
**
** @param pChar       The string that is being built
** @param size        The size to add to the string
** @param alignment   Left/right alignment
** @param val         The 64bits unsigned value
**
** @retval the size added to the string
*/
static inline int rozofs_u64_padded_append(char * pChar, int size, rozofs_alignment_e alignment, uint64_t val) {
  return rozofs_64_padded_append(pChar, size, alignment, 0, val);
} 
























/*
** ===================== 6IPv4 FORMATING  ======================
*/















/*
**___________________________________________________________
** Append an IPv4 address to a string and add a 0 at the end
**
**
** @param pChar       The string that is being built
** @param ip          The IP address
**
** @retval the size added to the built string
*/
static inline int rozofs_ipv4_append(char * pChar, uint32_t ip) {
  char * p = pChar; 
  
  p += rozofs_u32_append(p,(ip>>24)&0xFF);
  *p++ = '.';
  p += rozofs_u32_append(p,(ip>>16)&0xFF);  
  *p++ = '.';
  p += rozofs_u32_append(p,(ip>>8)&0xFF);  
  *p++ = '.';
  p += rozofs_u32_append(p,ip&0xFF);  
  *p = 0;

  return (p-pChar);
}
/*
**___________________________________________________________
** Append an IPv4:port address to a string and add a 0 at the end
**
**
** @param pChar       The string that is being built
** @param ip          The IP address
**
** @retval the size added to the built string
*/
static inline int rozofs_ipv4_port_append(char * pChar, uint32_t ip, uint16_t port) {
  char * p = pChar; 
  
  p += rozofs_u32_append(p,(ip>>24)&0xFF);
  *p++ = '.';
  p += rozofs_u32_append(p,(ip>>16)&0xFF);  
  *p++ = '.';
  p += rozofs_u32_append(p,(ip>>8)&0xFF);  
  *p++ = '.';
  p += rozofs_u32_append(p,ip&0xFF);  
  *p++ = ':';
  p += rozofs_u32_append(p,port);

  return (p-pChar);
}

























/*
** ===================== MISCELLANEOUS FORMATING  ======================
*/






/*
**___________________________________________________________
** Append an unsigned 64 bit number in hexadecimal representation
** to a string. Add a 0 at the end.
**
** sprintf(pChar,"%16.16llx",val) 
**  -> rozofs_x64_append(pChar, val)
**
** @param pChar       The string that is being built
** @param val         The 64bits unsigned value
**
** @retval the size added to the string
*/
static inline int rozofs_x8_append(char * pChar, uint8_t val) {
  uint32_t high,low;

  low  = val & 0xF;
  high = (val >> 4) & 0xF;
  
  if (high<10) *pChar++ = high + '0';
  else         *pChar++ = high + 'a' - 10;

  if (low<10)  *pChar++ = low  + '0';
  else         *pChar++ = low  + 'a' - 10;
  
  /*
  ** Add the end of string
  */      
  *pChar = 0;
  return 2;
} 








/*__________________________________________________________________________
 */
/**
*  Display bytes with correct unit 
*  @param value         Value in bytes to display
*  @param value_string  String where to format the value
*/
#define DIX  10ULL
#define CENT 100ULL
#define KILO 1000ULL
#define MEGA (KILO*KILO)
#define GIGA (KILO*MEGA)
#define TERA (KILO*GIGA)
#define PETA (KILO*TERA)
static inline int rozofs_bytes_padded_append(char * value_string, int size, uint64_t value) {
  uint64_t   modulo=0;
  char     * pt = value_string;
  int        sz;  
  
  if (value<KILO) {
    pt += rozofs_u64_padded_append(pt,4, rozofs_right_alignment, value);
    pt += rozofs_string_append(pt,"  B");
    goto out;  		    
  }
  
  if (value<MEGA) {
  
    if (value>=(CENT*KILO)) {
      pt += rozofs_u64_padded_append(pt,4,rozofs_right_alignment,value/KILO);
      pt += rozofs_string_append(pt," KB");
      goto out;    		    
    }
    
    if (value>=(DIX*KILO)) {    
      modulo = (value % KILO) / CENT;
      pt += rozofs_u64_padded_append(pt,2,rozofs_right_alignment,value/KILO);
      *pt++ = '.';
      pt += rozofs_u32_padded_append(pt,1,rozofs_zero,modulo);
      pt += rozofs_string_append(pt," KB");
      goto out;
    }  
    
    modulo = (value % KILO) / DIX;
    pt += rozofs_u64_padded_append(pt,1,rozofs_right_alignment,value/KILO);
    *pt++ = '.';
    pt += rozofs_u32_padded_append(pt,2,rozofs_zero,modulo);
    pt += rozofs_string_append(pt," KB");
    goto out;    
     
  }
  
  if (value<GIGA) {
  
    if (value>=(CENT*MEGA)) {
      pt += rozofs_u64_padded_append(pt,4,rozofs_right_alignment,value/MEGA);
      pt += rozofs_string_append(pt," MB");
      goto out;    		    
    }
    
    if (value>=(DIX*MEGA)) {    
      modulo = (value % MEGA) / (CENT*KILO);
      pt += rozofs_u64_padded_append(pt,2,rozofs_right_alignment,value/MEGA);
      *pt++ = '.';
      pt += rozofs_u32_padded_append(pt,1,rozofs_zero,modulo);
      pt += rozofs_string_append(pt," MB");
      goto out;
    }  
    
    modulo = (value % MEGA) / (DIX*KILO);
    pt += rozofs_u64_padded_append(pt,1,rozofs_right_alignment,value/MEGA);
    *pt++ = '.';
    pt += rozofs_u32_padded_append(pt,2,rozofs_zero,modulo);
    pt += rozofs_string_append(pt," MB");
    goto out;    
     
  } 
   
  if (value<TERA) {
  
    if (value>=(CENT*GIGA)) {
      pt += rozofs_u64_padded_append(pt,4,rozofs_right_alignment,value/GIGA);
      pt += rozofs_string_append(pt," GB");
      goto out;    		    
    }
    
    if (value>=(DIX*GIGA)) {    
      modulo = (value % GIGA) / (CENT*MEGA);
      pt += rozofs_u64_padded_append(pt,2,rozofs_right_alignment,value/GIGA);
      *pt++ = '.';
      pt += rozofs_u32_padded_append(pt,1,rozofs_zero,modulo);
      pt += rozofs_string_append(pt," GB");
      goto out;
    }  
    
    modulo = (value % GIGA) / (DIX*MEGA);
    pt += rozofs_u64_padded_append(pt,1,rozofs_right_alignment,value/GIGA);
    *pt++ = '.';
    pt += rozofs_u32_padded_append(pt,2,rozofs_zero,modulo);
    pt += rozofs_string_append(pt," GB");
    goto out;    
     
  }   
  
  if (value<PETA) {
  
    if (value>=(CENT*TERA)) {
      pt += rozofs_u64_padded_append(pt,4,rozofs_right_alignment,value/TERA);
      pt += rozofs_string_append(pt," TB");
      goto out;    		    
    }
    
    if (value>=(DIX*TERA)) {    
      modulo = (value % TERA) / (CENT*GIGA);
      pt += rozofs_u64_padded_append(pt,2,rozofs_right_alignment,value/TERA);
      *pt++ = '.';
      pt += rozofs_u32_padded_append(pt,1,rozofs_zero,modulo);
      pt += rozofs_string_append(pt," TB");
      goto out;
    }  
    
    modulo = (value % TERA) / (DIX*GIGA);
    pt += rozofs_u64_padded_append(pt,1,rozofs_right_alignment,value/TERA);
    *pt++ = '.';
    pt += rozofs_u32_padded_append(pt,2,rozofs_zero,modulo);
    pt += rozofs_string_append(pt," TB");
    goto out;    
     
  }  
    
  if (value>=(CENT*PETA)) {
    pt += rozofs_u64_padded_append(pt,4,rozofs_right_alignment,value/PETA);
    pt += rozofs_string_append(pt," PB");
    goto out; 		    
  }
  if (value>=(DIX*PETA)) {    
    modulo = (value % PETA) / (CENT*TERA);
    pt += rozofs_u64_padded_append(pt,2,rozofs_right_alignment,value/PETA);
    *pt++ = '.';
    pt += rozofs_u32_padded_append(pt,1,rozofs_zero,modulo);
    pt += rozofs_string_append(pt," PB");
    goto out;
  }     

  modulo = (value % PETA) / (DIX*TERA);
  pt += rozofs_u64_padded_append(pt,1,rozofs_right_alignment,value/PETA);
  *pt++ = '.';
  pt += rozofs_u32_padded_append(pt,2,rozofs_zero,modulo);
  pt += rozofs_string_append(pt," PB");
  goto out; 	  
  
  
out:
  sz = pt-value_string; 
  while(sz < size) {
    *pt++ = ' ';
    sz++;
  }
  return size;  
  
}
static inline int rozofs_count_padded_append(char * value_string, int size, uint64_t value) {
  uint64_t   modulo=0;
  char     * pt = value_string;
  int        sz;  
  
  if (value<KILO) {
    pt += rozofs_u64_padded_append(pt,4, rozofs_right_alignment, value);
    pt += rozofs_string_append(pt,"  ");
    goto out;  		    
  }
  
  if (value<MEGA) {
  
    if (value>=(CENT*KILO)) {
      pt += rozofs_u64_padded_append(pt,4,rozofs_right_alignment,value/KILO);
      pt += rozofs_string_append(pt," K");
      goto out;    		    
    }
    
    if (value>=(DIX*KILO)) {    
      modulo = (value % KILO) / CENT;
      pt += rozofs_u64_padded_append(pt,2,rozofs_right_alignment,value/KILO);
      *pt++ = '.';
      pt += rozofs_u32_padded_append(pt,1,rozofs_zero,modulo);
      pt += rozofs_string_append(pt," K");
      goto out;
    }  
    
    modulo = (value % KILO) / DIX;
    pt += rozofs_u64_padded_append(pt,1,rozofs_right_alignment,value/KILO);
    *pt++ = '.';
    pt += rozofs_u32_padded_append(pt,2,rozofs_zero,modulo);
    pt += rozofs_string_append(pt," K");
    goto out;    
     
  }
  
  if (value<GIGA) {
  
    if (value>=(CENT*MEGA)) {
      pt += rozofs_u64_padded_append(pt,4,rozofs_right_alignment,value/MEGA);
      pt += rozofs_string_append(pt," M");
      goto out;    		    
    }
    
    if (value>=(DIX*MEGA)) {    
      modulo = (value % MEGA) / (CENT*KILO);
      pt += rozofs_u64_padded_append(pt,2,rozofs_right_alignment,value/MEGA);
      *pt++ = '.';
      pt += rozofs_u32_padded_append(pt,1,rozofs_zero,modulo);
      pt += rozofs_string_append(pt," M");
      goto out;
    }  
    
    modulo = (value % MEGA) / (DIX*KILO);
    pt += rozofs_u64_padded_append(pt,1,rozofs_right_alignment,value/MEGA);
    *pt++ = '.';
    pt += rozofs_u32_padded_append(pt,2,rozofs_zero,modulo);
    pt += rozofs_string_append(pt," M");
    goto out;    
     
  } 
   
  if (value<TERA) {
  
    if (value>=(CENT*GIGA)) {
      pt += rozofs_u64_padded_append(pt,4,rozofs_right_alignment,value/GIGA);
      pt += rozofs_string_append(pt," G");
      goto out;    		    
    }
    
    if (value>=(DIX*GIGA)) {    
      modulo = (value % GIGA) / (CENT*MEGA);
      pt += rozofs_u64_padded_append(pt,2,rozofs_right_alignment,value/GIGA);
      *pt++ = '.';
      pt += rozofs_u32_padded_append(pt,1,rozofs_zero,modulo);
      pt += rozofs_string_append(pt," G");
      goto out;
    }  
    
    modulo = (value % GIGA) / (DIX*MEGA);
    pt += rozofs_u64_padded_append(pt,1,rozofs_right_alignment,value/GIGA);
    *pt++ = '.';
    pt += rozofs_u32_padded_append(pt,2,rozofs_zero,modulo);
    pt += rozofs_string_append(pt," G");
    goto out;    
     
  }   
  
  if (value<PETA) {
  
    if (value>=(CENT*TERA)) {
      pt += rozofs_u64_padded_append(pt,4,rozofs_right_alignment,value/TERA);
      pt += rozofs_string_append(pt," T");
      goto out;    		    
    }
    
    if (value>=(DIX*TERA)) {    
      modulo = (value % TERA) / (CENT*GIGA);
      pt += rozofs_u64_padded_append(pt,2,rozofs_right_alignment,value/TERA);
      *pt++ = '.';
      pt += rozofs_u32_padded_append(pt,1,rozofs_zero,modulo);
      pt += rozofs_string_append(pt," T");
      goto out;
    }  
    
    modulo = (value % TERA) / (DIX*GIGA);
    pt += rozofs_u64_padded_append(pt,1,rozofs_right_alignment,value/TERA);
    *pt++ = '.';
    pt += rozofs_u32_padded_append(pt,2,rozofs_zero,modulo);
    pt += rozofs_string_append(pt," T");
    goto out;    
     
  }  
    
  if (value>=(CENT*PETA)) {
    pt += rozofs_u64_padded_append(pt,4,rozofs_right_alignment,value/PETA);
    pt += rozofs_string_append(pt," P");
    goto out; 		    
  }
  if (value>=(DIX*PETA)) {    
    modulo = (value % PETA) / (CENT*TERA);
    pt += rozofs_u64_padded_append(pt,2,rozofs_right_alignment,value/PETA);
    *pt++ = '.';
    pt += rozofs_u32_padded_append(pt,1,rozofs_zero,modulo);
    pt += rozofs_string_append(pt," P");
    goto out;
  }     

  modulo = (value % PETA) / (DIX*TERA);
  pt += rozofs_u64_padded_append(pt,1,rozofs_right_alignment,value/PETA);
  *pt++ = '.';
  pt += rozofs_u32_padded_append(pt,2,rozofs_zero,modulo);
  pt += rozofs_string_append(pt," P");
  goto out; 	  
  
  
out:
  sz = pt-value_string; 
  while(sz < size) {
    *pt++ = ' ';
    sz++;
  }
  return size;  
  
}


/*
** ===================== FS MODE ==================================
*/


#define rozofs_add_mode_right(right, letter)\
  if ((mode & right) == right) *pChar = letter;\
  else                         *pChar = '-';\
  pChar++;
  
static inline int rozofs_mode2String(char * buffer, int mode) {
  char * pChar = buffer;
  
  // Put octal value
  pChar += sprintf(pChar, "0%o ",mode); 
  
  // Translate type to string
  if (S_ISLNK(mode))  pChar += rozofs_string_append(pChar,"LINK ");
  if (S_ISREG(mode))  pChar += rozofs_string_append(pChar,"REG ");
  if (S_ISDIR(mode))  pChar += rozofs_string_append(pChar,"DIR ");
  if (S_ISCHR(mode))  pChar += rozofs_string_append(pChar,"CHR ");
  if (S_ISBLK(mode))  pChar += rozofs_string_append(pChar,"BLK ");
  if (S_ISFIFO(mode)) pChar += rozofs_string_append(pChar,"FIFO ");
  if (S_ISSOCK(mode)) pChar += rozofs_string_append(pChar,"SOCK ");

  // Display rigths
  
  rozofs_add_mode_right(S_IRUSR,'r');
  rozofs_add_mode_right(S_IWUSR,'w');
  rozofs_add_mode_right(S_IXUSR,'x');

  rozofs_add_mode_right(S_IRGRP,'r');
  rozofs_add_mode_right(S_IRGRP,'w');
  rozofs_add_mode_right(S_IXGRP,'x');

  rozofs_add_mode_right(S_IROTH,'r');
  rozofs_add_mode_right(S_IWOTH,'w');
  rozofs_add_mode_right(S_IXOTH,'x');
  
  return (pChar-buffer);
}
#ifdef __cplusplus
}
#endif /*__cplusplus */

#endif

