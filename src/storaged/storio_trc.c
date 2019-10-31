#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include <sys/time.h>
#include <uuid/uuid.h>
#include <stdio.h>

#include <rozofs/core/uma_dbg_api.h>
#include "storio_trc.h"

/*
** Common header to every trace record
*/
typedef struct _storio_trc_hdr_t {
  uint64_t  ts;             /**< Time stamp */
  uint32_t  req:1 ;         /**< assert to one if it is request , 0 for the response */
  uint32_t  padding:9;
  uint32_t  service_id:6;   /**< service identifier */
  uint32_t  index:16 ;      /**< index of the request in the trace buffer */
} storio_trc_hdr_t;

#define STORIO_TRC_INDEX_MAX (0xFFFF)

typedef struct _storio_trc_req_par_t {
  uuid_t    fid;            /**< FID */
  uint64_t  bid   ;         /**< offset */
  uint32_t  nb_proj;        /**< size */
  uint16_t  cid;
  uint16_t  sid;  
} storio_trc_req_par_t;

typedef struct _storio_trc_rsp_par_t {
  uint32_t   status:1;
  uint32_t   errno_val:31;
  uint32_t   size;
} storio_trc_rsp_par_t;

typedef struct _storio_trc_t {
  storio_trc_hdr_t     hdr; /**< service identifier  */
  union {
    storio_trc_req_par_t  req;
    storio_trc_rsp_par_t  rsp;
  };  
} storio_trc_t;  


int storio_trc_wr_idx; /**< current trace index */
int storio_trc_buf_full; /**< trace buffer is full */
int storio_trc_last_idx; /**< last entry in the trace buffer */
int storio_trc_enabled = 0;  /**< assert to 1 when the trace is enable */
uint32_t      storio_trc_index = 0;
storio_trc_t *storio_trc_buffer = NULL;  /**< pointer to the trace buffer */

#define MAX_LATEST_TS    256


/*__________________________________________________________________________
** Enable the trace if buffer is allocated
**
** @retval the status of the trace 
*/
int storio_trc_enable() {
  if (storio_trc_buffer!= NULL) storio_trc_enabled = 1;
  return storio_trc_enabled;
}
/*__________________________________________________________________________
** Reset the trace buffer
** Reset trace indexes
**    
** @param none
** @retval none
*/
void storio_trc_reset() {
  storio_trc_index = 0;
  storio_trc_wr_idx = 0;
  storio_trc_buf_full = 0;
}
/*__________________________________________________________________________
** Disable the trace
**
** @param none
**
** @retval none
*/
void storio_trc_disable() {
  storio_trc_enabled = 0;
}
/*__________________________________________________________________________
** Show the status of the trace service
**
** @param pChar : pointer to the result buffer
**
** @retval none
*/
void storio_trc_status(char *pChar) {
  pChar += sprintf(pChar,"{ \"trace\" {\n");
  pChar += sprintf(pChar,"   \"status\"  : \"%s\",\n",(storio_trc_enabled==0)?"Disabled":"Enabled");
  pChar += sprintf(pChar,"   \"size\"    : %d,\n",(storio_trc_buffer==NULL)?0:storio_trc_last_idx);
  pChar += sprintf(pChar,"   \"records\" : %d\n}}",storio_trc_wr_idx);
}
/*__________________________________________________________________________
** Init of the trace service
** It allocates a default trace buffer and turn off the trace.
**
** @param nbRecords  Number of records in the trace buffer 
**
** retval 0 on success/-1 on error
*/
int storio_trc_allocate(int nbRecords) {

 if (storio_trc_buffer != NULL) free(storio_trc_buffer);

  storio_trc_reset();  
  
  storio_trc_last_idx = nbRecords;
  storio_trc_buffer = malloc(sizeof(storio_trc_t)*nbRecords);
  if (storio_trc_buffer != NULL) {  
    memset(storio_trc_buffer,0,sizeof(storio_trc_t)*nbRecords);
    return 0;
  }
  return -1;
}
/*__________________________________________________________________________
** Display of the content of the trace buffer
**
** @param pChar : pointer to the result buffer
**
** @retval none
*/
void show_trc_buffer(uint32_t tcpRef, void *bufRef) {
  int start, count;
  uint64_t cur_ts;
  char * pChar;
  char * pHead;
  int    max_size;
  void * nextBuff = NULL;
  storio_trc_t *p ;
  int i;
  uint32_t lastest_req[MAX_LATEST_TS];
  int      lastest_curr_idx = 0;
  int      lastest_nb_records = 0;
  uint32_t lastest_index = 0;
  
  
    
  if (storio_trc_enabled != 1) {
    return uma_dbg_send(tcpRef, bufRef, TRUE, "Trace is not enabled\n");    
  }

  /* 
  ** Retrieve the buffer payload 
  */
  if ((pHead = (char *)ruc_buf_getPayload(bufRef)) == NULL) {
    severe( "ruc_buf_getPayload(%p)", bufRef );
    /* Let's tell the caller fsm that the message is sent */
    return;
  }  
  max_size = ruc_buf_getMaxPayloadLen(bufRef)-1024;
  
  /*
  ** Full buffer. From storio_trc_wr_idx for storio_trc_last_idx
  */
  if (storio_trc_buf_full){
    start = storio_trc_wr_idx;
    count = storio_trc_last_idx;
  }
  /*
  ** Buffer not full. From 0 to storio_trc_wr_idx
  */
  else  {
    start = 0;
    count = storio_trc_wr_idx;
  }
  
  /*
  ** Get trace buffer oldest time stamp as reference
  */
  p = &storio_trc_buffer[start];
  cur_ts = p->hdr.ts;

  /*
  ** Set the command recell string
  */
  pChar = uma_dbg_cmd_recall((UMA_MSGHEADER_S *)pHead);   
  pChar+=sprintf(pChar,"trace entry size : %lu Bytes\n",(long unsigned int)sizeof(storio_trc_t));
  pChar+=sprintf(pChar,"trace nb entry   : %lu\n",(long unsigned int)storio_trc_last_idx);

  for (i = 0; i < count; i++,start++){

    /*
    ** Flush current buffer when almost full
    */
    if ((pChar - pHead) >= max_size) {
      *pChar = 0;

      /*
      ** Allocate a next buffer
      */
      nextBuff = uma_dbg_get_new_buffer(tcpRef);
      if (nextBuff==NULL) {
        pChar += sprintf(pChar,"\n\n!!! Buffer depletion !!!\n");
        break;
      }
      /*
      ** Send this buffer
      */
      uma_dbg_send_buffer(tcpRef, bufRef, pChar-pHead, FALSE);         
      /*
      ** Use the new allocated buffer
      */
      bufRef = nextBuff;
      if ((pHead = (char *)ruc_buf_getPayload(bufRef)) == NULL) {
        severe( "ruc_buf_getPayload(%p)", bufRef );
        /* Let's tell the caller fsm that the message is sent */
        return;
      }  
      max_size = ruc_buf_getMaxPayloadLen(bufRef)-1024;
      pChar = pHead + sizeof(UMA_MSGHEADER_S);
      *pChar = 0;          
    }            

    /*
    ** Trace buffer is circular
    */
    if (start >= storio_trc_last_idx) start = 0;
    
    /*
    ** Get current trace
    */
    p = &storio_trc_buffer[start];
    
    *pChar++ = '[';
    pChar += rozofs_u64_padded_append(pChar, 12, rozofs_right_alignment, (unsigned long long int)(p->hdr.ts - cur_ts));
    if (p->hdr.req) {
      pChar += rozofs_string_append(pChar," ]--> ");
      
      /*
      ** Record the request in the lastest_req table in order to retrieve 
      ** when receibing a response the corresponding request from the response index
      */
      
      /*
      ** lastest_req table is not yet full
      */
      if (lastest_nb_records<MAX_LATEST_TS) {
        /* 1rst record : get current record index as oldest index in lastest_req table */
        if (lastest_nb_records==0) lastest_index = p->hdr.index;
        lastest_nb_records++; /* One more record in lastest_req table */
        /*
        ** record current trace buffer index in lastest_req table 
        */
        lastest_req[lastest_curr_idx] = start;
        lastest_curr_idx++;    
      }
      /*
      ** lastest_req table is full
      */
      else{
        /*
        ** Each request reaplace the oldest one in lastest_req table 
        ** and request number are monotonically incremented.
        ** So new latest index is incremented by one for each request
        */
        lastest_index++;
        /*
        ** Index wrap at value STORIO_TRC_INDEX_MAX
        */
        if (lastest_index>STORIO_TRC_INDEX_MAX) lastest_index = 0;
        /*
        ** The lastest_req table wraps too
        */        
        if (lastest_curr_idx>=MAX_LATEST_TS) lastest_curr_idx = 0;
        /*
        ** record current trace buffer index in lastest_req table 
        */
        lastest_req[lastest_curr_idx] = start;
        lastest_curr_idx++;       
      }
    }  
    else {
      pChar += rozofs_string_append(pChar," ]<-- ");
    }  
    pChar += rozofs_string_padded_append(pChar,8,rozofs_right_alignment, storio_trc_service_e2String(p->hdr.service_id));
     *pChar++ = ' ';
    pChar += rozofs_u32_padded_append(pChar, 5, rozofs_right_alignment, p->hdr.index); 
     *pChar++ = ' ';

    if (p->hdr.req ) {
      pChar += rozofs_u32_padded_append(pChar, 2, rozofs_right_alignment, p->req.cid);
     *pChar++ = '/';
      pChar += rozofs_u32_padded_append(pChar, 2, rozofs_right_alignment, p->req.sid);
     *pChar++ = '/';
      pChar += rozofs_fid_append(pChar, p->req.fid);
       *pChar++ = ' ';
      pChar += rozofs_u64_padded_append(pChar, 8, rozofs_right_alignment, (unsigned long long int)p->req.bid);
       *pChar++ = '/';
      pChar += rozofs_u64_padded_append(pChar, 3, rozofs_right_alignment, p->req.nb_proj); 
       *pChar++ = ' ';
    }   
    else {
      /*
      ** Retrieve the request matching this esponse from its index
      */
      int delta = -1;
      int idx;
      storio_trc_t *pReq = NULL;
      /*
      ** Compute the delta between the index of the response and the oldest 
      ** stored index in lastest_req table.
      */
      if ((lastest_index <= p->hdr.index) && (p->hdr.index < (lastest_index + lastest_nb_records))) {
        delta = p->hdr.index - lastest_index;
      }
      else {
        /* index may be wrapping */
        if ((lastest_index + lastest_nb_records) > STORIO_TRC_INDEX_MAX) {
          if ((lastest_index > p->hdr.index) && (p->hdr.index < (lastest_index + lastest_nb_records - (STORIO_TRC_INDEX_MAX+1)))) {
            delta = STORIO_TRC_INDEX_MAX + 1 + p->hdr.index - lastest_index;;
          }
        }     
      }
      
      if (delta != -1) {
        if (lastest_nb_records<MAX_LATEST_TS) idx = delta;
        else idx = (lastest_curr_idx + delta) % MAX_LATEST_TS;
        if (idx < MAX_LATEST_TS) {
          idx = lastest_req[idx];     
          pReq = &storio_trc_buffer[idx];
        }
      }
      
      if ((pReq)&&(pReq->hdr.index == p->hdr.index)) {
        pChar += rozofs_u32_padded_append(pChar, 2, rozofs_right_alignment, pReq->req.cid);
        *pChar++ = '/';
        pChar += rozofs_u32_padded_append(pChar, 2, rozofs_right_alignment, pReq->req.sid);
        *pChar++ = '/';
        pChar += rozofs_fid_append(pChar, pReq->req.fid);
         *pChar++ = ' ';
         *pChar++ = '+' ; 
        pChar += rozofs_u64_padded_append(pChar, 7, rozofs_right_alignment, (unsigned long long int)(p->hdr.ts - pReq->hdr.ts));   
      } 
      else {
        pChar += rozofs_string_append(pChar, "  /  /                                     +???????");
      }                            
      
      *pChar++ = ' ';
      pChar += rozofs_u32_padded_append(pChar, 6, rozofs_right_alignment, p->rsp.size);
      *pChar++ = ' ';
      pChar += rozofs_string_append(pChar,strerror(p->rsp.errno_val));  
    }
    pChar += rozofs_string_append(pChar,"\n");

    /*
    ** Update current time
    */
    cur_ts = p->hdr.ts;   
  }
  /*
  ** Send last buffer
  */
  uma_dbg_send_buffer(tcpRef, bufRef, pChar-pHead, TRUE);         
  return;
}
/*__________________________________________________________________________
*/
static char * storio_trc_diag_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"trc reset         : reset trace buffer\n");
  pChar += sprintf(pChar,"trc enable        : enable trace mode\n");  
  pChar += sprintf(pChar,"trc disable       : disable trace mode\n");  
  pChar += sprintf(pChar,"trc status        : current status of the trace buffer\n");  
  pChar += sprintf(pChar,"trc count <count> : allocate a trace buffer with <count> entries\n");  
  pChar += sprintf(pChar,"trc               : display trace buffer\n");  
  return pChar; 
}  
/*__________________________________________________________________________
** Rozodiag call to display the trace buffer content
**
** @retval none
*/
void storio_trc_diag(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
  int   new_val;   
  int ret;

  if (argv[1] != NULL) {
    
    /*
    ** Reset the buffer
    */
    if (strcmp(argv[1],"reset")==0) {
      storio_trc_reset();
      return uma_dbg_send(tcpRef, bufRef, TRUE, "Reset Done\n");    
    }
    
    /*
    ** Enable the trace
    */    
    if (strcmp(argv[1],"enable")==0) {
      if (storio_trc_enabled != 1) {
        if (storio_trc_enable()) {
          storio_trc_reset();
          return uma_dbg_send(tcpRef, bufRef, TRUE, "Trace is now enabled\n");    
        }
        return uma_dbg_send(tcpRef, bufRef, TRUE, ROZOFS_COLOR_RED "Trace count not defined\n"ROZOFS_COLOR_NONE);    
      }
      return uma_dbg_send(tcpRef, bufRef, TRUE, "Trace is already enabled\n");    
    }  
    
    /*
    ** Disable the trace
    */    
    if (strcmp(argv[1],"disable")==0) {
      if (storio_trc_enabled == 1) {
        storio_trc_disable();
        return uma_dbg_send(tcpRef, bufRef, TRUE, "Trace is now disabled\n");    
      }
      return uma_dbg_send(tcpRef, bufRef, TRUE, "Trace is already disabled\n");    
    }
    
    /*
    ** Display the trace buffer status
    */
    if (strcmp(argv[1],"status")==0) {
      storio_trc_status(pChar);
      return uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
    } 
    
    /*
    ** Set the number of records in the trace
    */ 
    if (strcmp(argv[1],"count")==0) {
      errno = 0;
      if (argv[2] == NULL){
        pChar += sprintf(pChar, ROZOFS_COLOR_RED "argument is missing\n"ROZOFS_COLOR_NONE);
	pChar = storio_trc_diag_help(pChar);
	return uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
      }
      ret = sscanf(argv[2],"%d",&new_val);  
      if (ret != 1) {
        pChar += sprintf(pChar, ROZOFS_COLOR_RED"bad value %s\n"ROZOFS_COLOR_NONE,argv[2]);
	pChar = storio_trc_diag_help(pChar);
	return uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
      }
      if (new_val < 200) new_val = 200;
      if (storio_trc_allocate(new_val) < 0) {
        return uma_dbg_send(tcpRef, bufRef, TRUE, ROZOFS_COLOR_RED"cannot allocate a trace buffer with requested entries\n"ROZOFS_COLOR_NONE);
      }
      return uma_dbg_send(tcpRef, bufRef, TRUE, "Done!!\n");
    }  

    pChar = storio_trc_diag_help(pChar);
    return uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
  }
  
  /*
  ** Display the trace buffer content
  */
  show_trc_buffer(tcpRef,bufRef);
}
/*__________________________________________________________________________
** Allocate a trace record for a request
**
** @param service  Service identifier 
** @param cid      CID
** @param sid      SID
** @param fid      FID to trace
** @param bid      Block number
** @param nb_proj  number of blocks
**
** retval 0 on success/-1 on error
*/
uint32_t storio_trc_req(int service,uint16_t cid, uint16_t sid, uuid_t fid,uint64_t bid, uint32_t nb_proj) {
  storio_trc_t *p;

  if (storio_trc_enabled == 0) return 0;

  p = &storio_trc_buffer[storio_trc_wr_idx];

  p->hdr.ts         = ruc_rdtsc();
  p->hdr.service_id = service;
  p->hdr.req        = 1;
  p->hdr.index      = storio_trc_index++;

  if (fid!=NULL) {
    memcpy(p->req.fid,fid,sizeof(uuid_t));
  }
  else {
    memset(p->req.fid,0,sizeof(uuid_t));   
  }  
  p->req.bid        = bid;
  p->req.nb_proj    = nb_proj;
  p->req.cid        = cid;
  p->req.sid        = sid;

  storio_trc_wr_idx++;
  if (storio_trc_wr_idx >= storio_trc_last_idx) {
    storio_trc_wr_idx   = 0;
    storio_trc_buf_full = 1;
  }
  return p->hdr.index;
}
/*__________________________________________________________________________
** Allocate a trace record for a response
**
** @param service  Service identifier 
** @param status   1 OK / 0 error
** @param index    index of the corresponding request
**
** retval 0 on success/-1 on error
*/
void storio_trc_rsp(int service, int status, uint32_t size, uint32_t index) {
  storio_trc_t *p;

  if (storio_trc_enabled == 0) return;

  p = &storio_trc_buffer[storio_trc_wr_idx];

  p->hdr.ts         = ruc_rdtsc();
  p->hdr.service_id = service;
  p->hdr.req        = 0;
  p->hdr.index      = index;

  if (status==0) p->rsp.status = 1;
  else           p->rsp.status = 0;
  p->rsp.errno_val = status;
  p->rsp.size = size;
     
  storio_trc_wr_idx++;
  if (storio_trc_wr_idx >= storio_trc_last_idx) {
    storio_trc_wr_idx   = 0;
    storio_trc_buf_full = 1;
  }
}
/*__________________________________________________________________________
** Init of the trace service
**
** @param nbRecords  Number of records in the trace buffer 
**
** retval 0 on success/-1 on error
*/
int storio_trc_init() {
  storio_trc_buffer = NULL;
  storio_trc_reset();  
  uma_dbg_addTopicAndMan("trc", storio_trc_diag,NULL,0);
  return 0;
}
