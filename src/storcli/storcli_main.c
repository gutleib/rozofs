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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <sys/resource.h>
#include <unistd.h>
#include <libintl.h>
#include <rpc/rpc.h>
#include <rpc/pmap_clnt.h>
#include <getopt.h>
#include <time.h>

#include <rozofs/rozofs_srv.h>
#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include <rozofs/common/profile.h>
#include <rozofs/common/common_config.h>
#include <rozofs/rpc/mproto.h>
#include <rozofs/rpc/sproto.h>
#include <rozofs/rpc/spproto.h>
#include <rozofs/rpc/mclient.h>
#include <rozofs/rpc/eclient.h>
#include <rozofs/rpc/stcpproto.h>
#include <rozofs/rpc/storcli_lbg_prototypes.h>
#include <rozofs/core/north_lbg_api.h>
#include <rozofs/core/rozofs_timer_conf_dbg.h>
#include <rozofs/core/rozofs_core_files.h>
#include <rozofs/core/rozofs_host2ip.h>
#include <rozofs/core/ruc_traffic_shaping.h>
#include <rozofs/core/rozofs_host_list.h>
#include <rozofs/core/rozofs_numa.h>

#include "rozofs_storcli_lbg_cnf_supervision.h"
#include "rozofs_storcli.h"
#include "storcli_main.h"
#include "rozofs_storcli_reload_storage_config.h"
#include "rozofs_storcli_mojette_thread_intf.h"

#define STORCLI_PID_FILE "storcli.pid"

int rozofs_storcli_non_blocking_init(uint16_t dbg_port, uint16_t rozofsmount_instance);

DEFINE_PROFILING(stcpp_profiler_t);


/*
** reference of the shared memory opened by rozofsmount
*/
storcli_shared_t storcli_rozofsmount_shared_mem[SHAREMEM_PER_FSMOUNT];



/*
** KPI for Mojette transform
*/
 storcli_kpi_t storcli_kpi_transform_forward;
 storcli_kpi_t storcli_kpi_transform_inverse;

int storcli_site_number = 0;

/* 
**____________________________________________________
** REPAIR man & CLI
**
*/
void storcli_man_repair(char * pChar) ;
void storcli_cli_repair(char * argv[], uint32_t tcpRef, void *bufRef) ;
/*__________________________________________________________________________
 */
/**
 *  Global and local datas
 */
 

storcli_conf conf;

/*__________________________________________________________________________
 */
/**
* get the owner of the storcli

  @retval : pointer to the owner
*/
char *storcli_get_owner()
{
  if (conf.owner == NULL) return "no_owner";
  return conf.owner;
}



exportclt_t exportclt; /**< structure associated to exportd, needed for communication */
/**
*  service for computing all the cluster state
*/
void rozofs_storcli_cid_compute_cid_state();

uint32_t *rozofs_storcli_cid_table[ROZOFS_CLUSTERS_MAX];
uint32_t rozofs_storcli_cid_state_table[ROZOFS_CLUSTERS_MAX];
uint32_t storcli_vid_state = CID_DEPENDENCY_ST;

storcli_lbg_cnx_supervision_t storcli_lbg_cnx_supervision_tab[STORCLI_MAX_LBG];

#define DISPLAY_UINT32_CONFIG(field)   pChar += sprintf(pChar,"%-25s = %u\n",#field, conf.field); 
#define DISPLAY_STRING_CONFIG(field) \
  if (conf.field == NULL) pChar += sprintf(pChar,"%-25s = NULL\n",#field);\
  else                    pChar += sprintf(pChar,"%-25s = %s\n",#field,conf.field); 
  
void show_start_config(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
  
  DISPLAY_STRING_CONFIG(host);
  DISPLAY_STRING_CONFIG(export);
  DISPLAY_STRING_CONFIG(passwd);  
  DISPLAY_STRING_CONFIG(mount);  
  DISPLAY_STRING_CONFIG(owner);  
  DISPLAY_UINT32_CONFIG(module_index);
  DISPLAY_UINT32_CONFIG(layout);
  DISPLAY_UINT32_CONFIG(bsize);
  DISPLAY_UINT32_CONFIG(buf_size);
  DISPLAY_UINT32_CONFIG(max_retry);
  DISPLAY_UINT32_CONFIG(dbg_port);
  DISPLAY_UINT32_CONFIG(rozofsmount_instance);
  DISPLAY_UINT32_CONFIG(shaper);
  DISPLAY_UINT32_CONFIG(mojThreadWrite);
  DISPLAY_UINT32_CONFIG(mojThreadRead);    
  DISPLAY_UINT32_CONFIG(mojThreadThreshold);   
  DISPLAY_UINT32_CONFIG(site);
  DISPLAY_UINT32_CONFIG(localPreference);  
  DISPLAY_UINT32_CONFIG(noReadFaultTolerant);    
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}    


#define RESET_PROFILER_PROBE(probe) \
{ \
         gprofiler->probe[P_COUNT] = 0;\
         gprofiler->probe[P_ELAPSE] = 0; \
}

#define RESET_PROFILER_PROBE_BYTE(probe) \
{ \
   RESET_PROFILER_PROBE(probe);\
   gprofiler->probe[P_BYTES] = 0; \
}

#define SHOW_PROFILER_PROBE_COUNT(probe) {\
  if (gprofiler->probe[P_COUNT]) {\
    pChar += sprintf(pChar," %-18s | %15"PRIu64"  | %9s  | %18s  | %15s |\n",\
                     #probe,gprofiler->probe[P_COUNT]," "," "," ");\
  }\
}


#define SHOW_PROFILER_PROBE_BYTE(probe) {\
  if (gprofiler->probe[P_COUNT]) {\
    pChar += sprintf(pChar," %-18s | %15"PRIu64"  | %9"PRIu64"  | %18"PRIu64"  | %15"PRIu64" |\n",\
		     #probe,\
		     gprofiler->probe[P_COUNT],\
		     gprofiler->probe[P_COUNT]?gprofiler->probe[P_ELAPSE]/gprofiler->probe[P_COUNT]:0,\
		     gprofiler->probe[P_ELAPSE],\
                     gprofiler->probe[P_BYTES]);\
  }\
}

#define SHOW_PROFILER_KPI_BYTE(probe,kpi_buf) {\
  if (kpi_buf.count) {\
    pChar += sprintf(pChar," %-18s | %15"PRIu64"  | %9"PRIu64"  | %18"PRIu64"  | %15"PRIu64" |\n",\
		     #probe,\
		     kpi_buf.count,\
		     kpi_buf.count?kpi_buf.elapsed_time/kpi_buf.count:0,\
		     kpi_buf.elapsed_time,\
                     kpi_buf.bytes_count);\
  }\
}                    

#define RESET_PROFILER_KPI_BYTE(probe,kpi_buf) \
{ \
					kpi_buf.count = 0; \
					kpi_buf.elapsed_time = 0;\
                    kpi_buf.bytes_count = 0; \
}

static char * show_profiler_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"profiler reset       : display and then reset statistics\n");
  pChar += sprintf(pChar,"profiler             : display statistics\n");  
  return pChar; 
}
void show_profiler(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    time_t elapse;
    int days, hours, mins, secs;
    time_t  this_time = time(0);


    // Compute uptime for storaged process
    elapse = (int) (this_time - gprofiler->uptime);
    days = (int) (elapse / 86400);
    hours = (int) ((elapse / 3600) - (days * 24));
    mins = (int) ((elapse / 60) - (days * 1440) - (hours * 60));
    secs = (int) (elapse % 60);



    pChar += sprintf(pChar, "GPROFILER version %s uptime =  %d days, %2.2d:%2.2d:%2.2d\n", gprofiler->vers,days, hours, mins, secs);
    pChar += sprintf(pChar, "   procedure        |     count        |  time(us)  | cumulated time(us)  |     bytes       |\n");
    pChar += sprintf(pChar, "--------------------+------------------+------------+---------------------+-----------------+\n");

//    SHOW_PROFILER_PROBE_BYTE(read_req);
    SHOW_PROFILER_PROBE_BYTE(read);
    SHOW_PROFILER_KPI_BYTE(Mojette Inv,storcli_kpi_transform_inverse);
    SHOW_PROFILER_PROBE_COUNT(read_sid_miss);	   
    SHOW_PROFILER_PROBE_BYTE(read_prj);
    SHOW_PROFILER_PROBE_COUNT(read_prj_enoent);	
    SHOW_PROFILER_PROBE_COUNT(read_prj_err);
    SHOW_PROFILER_PROBE_COUNT(read_prj_tmo);
    SHOW_PROFILER_PROBE_COUNT(read_blk_footer);
    SHOW_PROFILER_PROBE_COUNT(read_blk_crc);
    SHOW_PROFILER_PROBE_COUNT(read_blk_prjid);
    SHOW_PROFILER_PROBE_COUNT(read_blk_corrupted);
    SHOW_PROFILER_PROBE_BYTE(write)
    SHOW_PROFILER_KPI_BYTE(Mojette Fwd,storcli_kpi_transform_forward);
    SHOW_PROFILER_PROBE_COUNT(write_sid_miss)    
    SHOW_PROFILER_PROBE_BYTE(write_prj);
    SHOW_PROFILER_PROBE_COUNT(write_prj_tmo);
    SHOW_PROFILER_PROBE_COUNT(write_prj_err);
    SHOW_PROFILER_PROBE_COUNT(write_prj_nospace);
    SHOW_PROFILER_PROBE_COUNT(write_prj_sid_err);
    SHOW_PROFILER_PROBE_BYTE(truncate);
    SHOW_PROFILER_PROBE_COUNT(truncate_sid_miss);
    SHOW_PROFILER_PROBE_BYTE(truncate_prj);
    SHOW_PROFILER_PROBE_COUNT(truncate_prj_tmo);
    SHOW_PROFILER_PROBE_COUNT(truncate_prj_err);
    SHOW_PROFILER_PROBE_BYTE(repair)
    SHOW_PROFILER_PROBE_BYTE(repair_prj);
    SHOW_PROFILER_PROBE_COUNT(repair_prj_tmo);
    SHOW_PROFILER_PROBE_COUNT(repair_prj_err);
    SHOW_PROFILER_PROBE_BYTE(delete);
    SHOW_PROFILER_PROBE_BYTE(delete_prj);
    SHOW_PROFILER_PROBE_COUNT(delete_prj_tmo);
    SHOW_PROFILER_PROBE_COUNT(delete_prj_err);
    
    SHOW_PROFILER_PROBE_COUNT(resize);
    SHOW_PROFILER_PROBE_COUNT(resize_prj);
    SHOW_PROFILER_PROBE_COUNT(resize_prj_err);
    if (argv[1] != NULL)
    {
      if (strcmp(argv[1],"reset")==0) {
	RESET_PROFILER_PROBE_BYTE(read);
	RESET_PROFILER_KPI_BYTE(Mojette Inv,storcli_kpi_transform_inverse);
	RESET_PROFILER_PROBE(read_sid_miss);	
	RESET_PROFILER_PROBE_BYTE(read_prj);
	RESET_PROFILER_PROBE(read_prj_enoent);	
	RESET_PROFILER_PROBE(read_prj_err);
	RESET_PROFILER_PROBE(read_prj_tmo);
	RESET_PROFILER_PROBE(read_blk_footer);
	RESET_PROFILER_PROBE(read_blk_crc);
	RESET_PROFILER_PROBE(read_blk_prjid);
        RESET_PROFILER_PROBE(read_blk_corrupted);	
	RESET_PROFILER_PROBE_BYTE(write)
	RESET_PROFILER_KPI_BYTE(Mojette Fwd,storcli_kpi_transform_forward);;
	RESET_PROFILER_PROBE(write_sid_miss);		
	RESET_PROFILER_PROBE_BYTE(write_prj);
	RESET_PROFILER_PROBE(write_prj_tmo);
	RESET_PROFILER_PROBE(write_prj_err);    
	RESET_PROFILER_PROBE(write_prj_nospace);    
	RESET_PROFILER_PROBE(write_prj_sid_err);    
	RESET_PROFILER_PROBE(truncate);
	RESET_PROFILER_PROBE(truncate_sid_miss);		
	RESET_PROFILER_PROBE_BYTE(truncate_prj);
	RESET_PROFILER_PROBE(truncate_prj_tmo);
	RESET_PROFILER_PROBE(truncate_prj_err);  
        RESET_PROFILER_PROBE_BYTE(repair)
	RESET_PROFILER_PROBE_BYTE(repair_prj);
	RESET_PROFILER_PROBE(repair_prj_tmo);
	RESET_PROFILER_PROBE(repair_prj_err);    
	RESET_PROFILER_PROBE(delete);
	RESET_PROFILER_PROBE_BYTE(delete_prj);
	RESET_PROFILER_PROBE(delete_prj_tmo);
	RESET_PROFILER_PROBE(delete_prj_err);  
    
	RESET_PROFILER_PROBE(resize);
	RESET_PROFILER_PROBE(resize_prj);
	RESET_PROFILER_PROBE(resize_prj_err);
	
	pChar += sprintf(pChar,"Reset Done\n");  
	gprofiler->uptime = this_time;  
      }
      else {
	/*
	** Help
	*/
	pChar = show_profiler_help(pChar);  
      }	 
    }   
     
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}
/*
**____________________________________________________
** Effective behaviour when reading corrupted blocks
**
*/
int noReadFaultTolerant=0;
storcli_corrupted_fid_ctx storcli_fid_corrupted = { 0 };
/* 
**____________________________________________________
** Corrupted CLI man
**
*/
void man_corrupted(char * pChar) {
  pChar += rozofs_string_append(pChar,"corrupted          : display block corruption counter and configuration.\n");
  pChar += rozofs_string_append(pChar,"corrupted reset    : reset block corruption counter.\n");
  pChar += rozofs_string_append(pChar,"corrupted tolerant : be tolerant to corrupted blocks.\n");  
  pChar += rozofs_string_append(pChar,"corrupted eio      : return EIO on corrupted block read.\n");   
}
/*
**____________________________________________________
** Display counters and configuration
**
*/
char * display_corrupted(char * pChar) {
  uint        idx;
  uint8_t *   fid;
  storcli_one_corrupted_fid_ctx * pCtx;
  int        first=1;
  
  
  /*
  ** Display counters and configuration
  */
  pChar += rozofs_string_append(pChar, "{\n   \"corrupted blocks\" : {\n      \"configuration\" : {\n         \"startup\" : ");
  pChar += rozofs_string_append(pChar, (conf.noReadFaultTolerant==0)?"\"Tolerant\"":"\"EIO\"");
  pChar += rozofs_string_append(pChar, ",\n         \"running\" : ");
  pChar += rozofs_string_append(pChar, (noReadFaultTolerant==0)?"\"Tolerant\"":"\"EIO\"");
  pChar += rozofs_string_append(pChar, "\n      },\n      \"corruption count\" : ");
  pChar += rozofs_u64_append(pChar, gprofiler->read_blk_corrupted[P_COUNT]);
  pChar += rozofs_string_append(pChar, ",\n");

  /*
  ** Display log of corrupted FID list
  */
  pChar += rozofs_string_append(pChar, "      \"corrupted FID\" : [\n");  
  pCtx = storcli_fid_corrupted.ctx;
  for (idx=0; idx<STORCLI_MAX_CORRUPTED_FID_NB; idx++,pCtx++) {
    if (pCtx->count == 0) continue;
    if (first) first = 0;
    else       pChar += rozofs_string_append(pChar, ",\n"); 
    fid = (uint8_t  *)pCtx->fid;
    pChar += rozofs_string_append(pChar, "         {\"FID\" : \"@rozofs_uuid@");  
    rozofs_uuid_unparse((uint8_t*)fid, pChar);
    pChar += 36;
    pChar += rozofs_string_append(pChar, "\", \"count\" : ");  
    pChar += rozofs_u64_append(pChar, pCtx->count);
    pChar += rozofs_string_append(pChar, "}");  
  }
  pChar += rozofs_string_append(pChar, "\n      ]\n   }\n}\n");  

  return pChar;
}
/*
**____________________________________________________
** Corrupted block CLI
**
*/
void show_corrupted(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();

  if (argv[1] != NULL) {

    /*
    ** Reset counter
    */
    if (strcasecmp(argv[1],"reset")==0) {
      pChar = display_corrupted(pChar);
      RESET_PROFILER_PROBE(read_blk_corrupted);	   
      memset(&storcli_fid_corrupted,0, sizeof(storcli_fid_corrupted));     
      pChar += rozofs_string_append(pChar, "\nBlock corruption counter reset.\n");	
    }

    /*
    ** Be tolerant
    */
    else if (strcasecmp(argv[1],"tolerant")==0) {
      noReadFaultTolerant = 0;
      pChar = display_corrupted(pChar);	
    }	

    /*
    ** Don't be tolerant
    */      
    else if (strcasecmp(argv[1],"eio")==0) {
      noReadFaultTolerant = 1;
      pChar = display_corrupted(pChar);	
    }

    /*
    ** Help
    */      
    else {
      man_corrupted(pChar);  
    }	 
  }  
  else {
    pChar = display_corrupted(pChar);    
  } 
  
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}
/* 
**____________________________________________________
** RW error buffer CLI man
**
*/
void man_rwerror(char * pChar) {
  pChar += rozofs_string_append(pChar,"rwerror          : display r/w error buffer.\n");
  pChar += rozofs_string_append(pChar,"rwerror reset    : reset r/w error buffer.\n");
  
  pChar += rozofs_string_append(pChar,"\nThe error buffer contains a list of error records.\n");
  pChar += rozofs_string_append(pChar,"Each error record contains the following information:\n");
  pChar += rozofs_string_append(pChar," time  : the date when the error occured.\n");
  pChar += rozofs_string_append(pChar," error : the errno associated to the error.\n");
  pChar += rozofs_string_append(pChar," ope   : the operation in error (READ/WRITE/TRUNCATE)\n");
  pChar += rozofs_string_append(pChar," line  : the line in the source code where the error was detected.\n");
  pChar += rozofs_string_append(pChar," FID   : an identifier of the file on which the error occured.\n");
  pChar += rozofs_string_append(pChar," bid   : the block number of the request.\n");
  pChar += rozofs_string_append(pChar," size  : the size in block of the request.\n");
  pChar += rozofs_string_append(pChar," lbg   : the status of the LBG toward the file distribution;\n");
  pChar += rozofs_string_append(pChar,"         One character per sid giving the LBG status.\n");
  pChar += rozofs_string_append(pChar,"         'U'=Up 'D'=Down 'S'=Shutting down '-'=Dependandcy\n");
  pChar += rozofs_string_append(pChar," prj   : the projection context. Each projection is displayed on 2 characters.\n");
  pChar += rozofs_string_append(pChar,"         1rst: storage index in the lbg status field.\n");
  pChar += rozofs_string_append(pChar,"         2nd : 'D'=Done 'N'=No file 'P'=in progress 'E'=Error 'I'=Idle.\n");
}

char * storcli_ope2string(int ope) {       
  switch(ope) {
    case  STORCLI_READ:     return "READ";
    case  STORCLI_WRITE:    return "WRITE";
    case  STORCLI_TRUNCATE: return "TRUNCATE";
  }
  return "?";
}   
/*
**____________________________________________________
** Display counters and configuration
**
*/
char * display_rwerror(char * pChar) {
  uint        idx;
  uint8_t *   fid;
  int        first=1;
  storcli_rw_error_record_t * pCtx;
  struct tm  ts;

  pChar += rozofs_string_append(pChar, "{\"rw errors\" : [\n");
  
  /*
  ** Display log of corrupted FID list
  */
  pCtx = &storcli_rw_error.record[0];
  for (idx=0; idx<ROZOFS_STORCLI_ERROR_RECORD_NB; idx++,pCtx++) {
    if (pCtx->ts == 0) continue;
    fid = (uint8_t  *)pCtx->fid;
    
    if (!first) pChar += rozofs_string_append(pChar, ",\n");  
    first = 0;   
    pChar += rozofs_string_append(pChar, "   {  \"time\" : \"");    
    ts = *localtime(&pCtx->ts);
    pChar += strftime(pChar, 100, "%Y-%m-%d %H:%M:%S\",", &ts);
    pChar += rozofs_string_append(pChar, " \"error\" : \"");
    pChar += rozofs_string_append(pChar, strerror(pCtx->error));   


    pChar += rozofs_string_append(pChar, "\",\n      \"FID\" : \"@rozofs_uuid@");
    rozofs_uuid_unparse((uint8_t*)fid, pChar);
    pChar += 36;
    
    pChar += rozofs_string_append(pChar, "\",\n      \"ope\" : \"");
    pChar += rozofs_string_append(pChar, storcli_ope2string(pCtx->opcode)); 
    pChar += rozofs_string_append(pChar, "\", \"line\" : ");
    pChar += rozofs_i32_append(pChar, pCtx->line);
    pChar += rozofs_string_append(pChar, ", \"bid\" : ");  
    pChar += rozofs_u64_append(pChar, pCtx->offset);
    pChar += rozofs_string_append(pChar, ", \"size\" : ");  
    pChar += rozofs_i32_append(pChar, pCtx->size);
    pChar += rozofs_string_append(pChar, ",\n      \"prj\" : \"");  
    pChar += rozofs_string_append(pChar, pCtx->prj);  
    pChar += rozofs_string_append(pChar, "\", \"lbg\" : \"");  
    pChar += rozofs_string_append(pChar, pCtx->lbg);  
    pChar += rozofs_string_append(pChar, "\"\n   }");  
  }
  pChar += rozofs_string_append(pChar, "\n   ]\n}\n");

  return pChar;
}
/*
**____________________________________________________
** Corrupted block CLI
**
*/
void show_rwerror(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();

  if (argv[1] != NULL) {

    /*
    ** Reset counter
    */
    if (strcasecmp(argv[1],"reset")==0) {
      pChar = display_rwerror(pChar);
      memset(&storcli_rw_error,0,sizeof(storcli_rw_error));	   
      pChar += rozofs_string_append(pChar, "\nrwerror buffer reset.\n");	
    }

    /*
    ** Help
    */      
    else {
      man_rwerror(pChar);  
    }	 
  }  
  else {
    pChar = display_rwerror(pChar);    
  } 
  
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}
static char bufall[1024];

/*__________________________________________________________________________
 */

static char *show_storlci_display_configuration_state(char *buffer,int state)
{
    char *pchar = buffer;
   switch (state)
   {
      default:
      case STORCLI_CONF_UNKNOWN:
        sprintf(pchar,"UNKNOWN   ");
        break;
   
      case STORCLI_CONF_NOT_SYNCED:
        sprintf(pchar,"NOT_SYNCED");
        break;   
      case STORCLI_CONF_SYNCED:
        sprintf(pchar,"SYNCED    ");
        break;   
   
   }
   return buffer;
}


/**________________________________________________________________________
*/
/**
*  Display of the state of the current configuration of the exportd

 */

static char *show_storcli_display_poll_state(char *buffer,int state)
{
    char *pchar = buffer;
   switch (state)
   {
      default:
      case STORCLI_POLL_IDLE:
        sprintf(pchar,"IDLE  ");
        break;
   
      case STORCLI_POLL_IN_PRG:
        sprintf(pchar,"IN_PRG");
        break;   
      case STORCLI_POLL_ERR:
        sprintf(pchar,"ERROR ");
        break;   
   
   }
   return buffer;
}
/*
**________________________________________________________________________
*/
/**
*
*/
void show_storcli_configuration(char * argv[], uint32_t tcpRef, void *bufRef) 
{
    char *pchar = uma_dbg_get_buffer();
   storcli_conf_ctx_t *p = &storcli_conf_ctx ;
   rpcclt_t *client_p;
   
   client_p = &exportclt.rpcclt;
   
   while(1)
   {   

     pchar += sprintf(pchar,"root path    :%s  (eid:%d)\n",exportclt.root,exportclt.eid);
     pchar += sprintf(pchar,"exportd host : %s\n",exportclt.host); 
     pchar += sprintf(pchar,"hash config  : 0x%x\n",exportd_configuration_file_hash); 

pchar += sprintf(pchar,"     hostname        |  socket  | state  | cnf. status |  poll (attps/ok/nok) | conf send (attps/ok/nok)\n");
pchar += sprintf(pchar,"---------------------+----------+--------+-------------+----------------------+--------------------------\n");
     while(1)
     {
       pchar += sprintf(pchar,"%20s |",exportclt.host);
       if ( client_p->sock == -1)
       {
         pchar += sprintf(pchar,"  ???     |");
       }
       else
       {
         pchar += sprintf(pchar,"  %3d     |",client_p->sock);
       
       }
       pchar += sprintf(pchar,"  %s  |",(client_p->sock !=-1)?"UP  ":"DOWN");
       pchar += sprintf(pchar," %s  |",show_storlci_display_configuration_state(bufall,p->conf_state));
       pchar += sprintf(pchar," %6.6llu/%6.6llu/%6.6llu |",
                (long long unsigned int)p->stats.poll_counter[STORCLI_STATS_ATTEMPT],
                (long long unsigned int)p->stats.poll_counter[STORCLI_STATS_SUCCESS],
               (long long unsigned int) p->stats.poll_counter[STORCLI_STATS_FAILURE]);

       pchar += sprintf(pchar," %6.6llu/%6.6llu/%6.6llu\n",
                (long long unsigned int)p->stats.conf_counter[STORCLI_STATS_ATTEMPT],
                (long long unsigned int)p->stats.conf_counter[STORCLI_STATS_SUCCESS],
               (long long unsigned int) p->stats.conf_counter[STORCLI_STATS_FAILURE]);  

       break;
     }
     break;
  } 
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}

/*
**________________________________________________________________________
*/
char *display_mstorage(mstorage_t *s,char *buffer)
{

  int i;
  int lbg_id;
  uint8_t cid,sid;
  uint32_t *sid_lbg_id_p;

  for (i = 0; i< s->sids_nb; i++)
  {
     cid = s->cids[i];
     sid = s->sids[i];
     buffer += sprintf(buffer," %3.3d  |  %2.2d  |",cid,sid);
     buffer += sprintf(buffer," %-26s |",s->host);
     
     sid_lbg_id_p = rozofs_storcli_cid_table[cid-1];
     if (sid_lbg_id_p == NULL) {
       lbg_id = -1;
     }
     else {       
       lbg_id = sid_lbg_id_p[sid-1];
     }
     
     if ( lbg_id == -1)
     {
       buffer += sprintf(buffer,"  ???     |");
     }
     else
     {
       buffer += sprintf(buffer,"  %3d     |",lbg_id);       
     }
     buffer += sprintf(buffer,"  %s  |",north_lbg_display_lbg_state(bufall,lbg_id));         
     buffer += sprintf(buffer,"  %s      |",north_lbg_is_available(lbg_id)==1 ?"UP  ":"DOWN");        
     buffer += sprintf(buffer," %3s |",storcli_lbg_cnx_supervision_tab[lbg_id].state==STORCLI_LBG_RUNNING ?"YES":"NO");        
     buffer += sprintf(buffer," %5d |",storcli_lbg_cnx_supervision_tab[lbg_id].tmo_counter); 
     buffer += sprintf(buffer," %5d |",storcli_lbg_cnx_supervision_tab[lbg_id].poll_counter); 
     buffer += sprintf(buffer," %2d |",STORCLI_LBG_SP_NULL_INTERVAL);         
     buffer += sprintf(buffer,"  %s      |",show_storcli_display_poll_state(bufall,storcli_lbg_cnx_supervision_tab[lbg_id].poll_state)); 
	 buffer += sprintf(buffer," %4d |\n",s->site);         
  }
  return buffer;
}
/*
**________________________________________________________________________
*/

void show_cnf_storaged(char * argv[], uint32_t tcpRef, void *bufRef) {
   list_t *iterator = NULL;
   char *pchar = uma_dbg_get_buffer();       
   int i;
   int lbg_id;
   int cid,sid;
   uint32_t *sid_lbg_id_p;
   int first = 1;

   pchar += sprintf(pchar,"{\"configuration status\" : [\n");
    
   /* Search if the node has already been created  */
   list_for_each_forward(iterator, &exportclt.storages) {
     mstorage_t *s = list_entry(iterator, mstorage_t, list);

     for (i = 0; i< s->sids_nb; i++) {
	cid = s->cids[i];
	sid = s->sids[i];

	sid_lbg_id_p = rozofs_storcli_cid_table[cid-1];
	if (sid_lbg_id_p == NULL) {
	  lbg_id = -1;
	}
	else {       
	  lbg_id = sid_lbg_id_p[sid-1];
	}
	if (!first) {
	  pchar += sprintf(pchar,",\n");
	}
	first = 0;
	pchar += sprintf(pchar,"   {\"cid\" : %2d, \"sid\" : %2d, \"host\" : \"%s\", \"lbg\" : %3d,",cid,sid,s->host,lbg_id);
	pchar += sprintf(pchar," \"status\" : \"");
	pchar += mstorage_cnf_status2string(pchar,s->cnf_status);
	pchar += sprintf(pchar,"\", \"count\" : %d, \"errno\" : \"%s\"}",s->cnf_count, strerror(s->error));
     }     
   }
   pchar += sprintf(pchar,"\n  ]\n}\n");

   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());      
}      

/**
*  Display the configuration et operationbal status of the storaged


*/
void show_storage_configuration(char * argv[], uint32_t tcpRef, void *bufRef) 
{
    char *pchar = uma_dbg_get_buffer();
	
   pchar +=sprintf(pchar,"multi site : %s\n",rozofs_get_msite()?"Yes":"No");
   pchar +=sprintf(pchar,"local site : %d\n",conf.site);
   

   pchar +=sprintf(pchar,"\n cid  |  sid |         hostname           |  lbg_id  | state  | Path state | Sel | tmo   | Poll. |Per.|  poll state  | site |\n");
   pchar +=sprintf(pchar,"------+------+----------------------------+----------+--------+------------+-----+-------+-------+----+--------------+------+\n");

   list_t *iterator = NULL;
   /* Search if the node has already been created  */
   list_for_each_forward(iterator, &exportclt.storages) 
   {
     mstorage_t *s = list_entry(iterator, mstorage_t, list);

     /*
     ** entry is found 
     ** update the cid and sid part only. changing the number of
     ** ports of the mstorage is not yet supported.
     */
     pchar=display_mstorage(s,pchar);
   }
   uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());      
}      

/*__________________________________________________________________________
*/
/**
* display the configuration of the shared memory provided by rozofsmount
*/
void storcli_shared_mem(char * argv[], uint32_t tcpRef, void *bufRef)
{
    char *pChar = uma_dbg_get_buffer();

    pChar += sprintf(pChar, " active |     key   |  size   | cnt  |    address     |\n");
    pChar += sprintf(pChar, "--------+-----------+---------+------+----------------+\n");
    int i;
    for (i = 0; i < SHAREMEM_PER_FSMOUNT; i++)
    {
      pChar +=sprintf(pChar," %4s | %8.8d |  %6.6d | %4.4d | %p |\n",(storcli_rozofsmount_shared_mem[i].active==1)?"  YES ":"  NO  ",
                      storcli_rozofsmount_shared_mem[i].key,
                      storcli_rozofsmount_shared_mem[i].buf_sz,
                      storcli_rozofsmount_shared_mem[i].buf_count,
                      storcli_rozofsmount_shared_mem[i].data_p);
    }                  
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());

}

char *storcli_display_cid_state(uint8_t state)
{
   switch (state)
   {
     case CID_DEPENDENCY_ST:
       return "DEPENDENCY";
     case CID_UP_ST:
       return "UP";   
     case CID_DOWNGRADED_ST:
       return "DOWNGRADED"; 
     case CID_DOWN_ST:
       return "DOWN"; 
     default:
       return "Unknown??";  
   }
}
/*__________________________________________________________________________
*/
/**
* display state of the clusters
*/
void show_cid_state(char * argv[], uint32_t tcpRef, void *bufRef)
{
    char *pChar = uma_dbg_get_buffer();
    
    rozofs_storcli_cid_compute_cid_state();

    pChar += sprintf(pChar, " cid    |     state    |\n");
    pChar += sprintf(pChar, "--------+--------------+\n");
    int i;
    for (i = 0; i < ROZOFS_CLUSTERS_MAX; i++)
    {
      if (rozofs_storcli_cid_table[i] == NULL) continue;
      pChar +=sprintf(pChar," %6d | %12s |\n",i+1,storcli_display_cid_state(rozofs_storcli_cid_state_table[i]));
    }                  
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());

}


/*__________________________________________________________________________
*/
/**
* display state of the clusters
*/
void show_vid_state(char * argv[], uint32_t tcpRef, void *bufRef)
{
    char *pChar = uma_dbg_get_buffer();
    
    rozofs_storcli_cid_compute_cid_state();

    pChar += sprintf(pChar, " volume state: %s \n",storcli_display_cid_state(storcli_vid_state));
                 
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}

/*__________________________________________________________________________
 */
/**
 * init of the cid state table. That table contains the state of the CID
    CID_DEPENDENCY 
    CID_DOWN
    CID_DOWNGRADED
    CID_UP
 */

void rozofs_storcli_cid_table_state_init() {
    memset(rozofs_storcli_cid_state_table, CID_DEPENDENCY_ST, ROZOFS_CLUSTERS_MAX * sizeof (uint8_t));
}
/*
**__________________________________________________________________________
*/
/**
* compute the state of a cid

  @param cid
  
  @retval none
north_lbg_get_state
*/
void rozofs_storcli_cid_compute_one_cid_state(int cid)
{ 
  int sid;
  uint32_t *sid_lbg_id_p;
  uint8_t cid_state = CID_UP_ST;
  uint8_t down_count = 0;
  uint8_t up_count = 0;
  int lbg_id;
  
  if (rozofs_storcli_cid_table[cid] == NULL)
  {
    rozofs_storcli_cid_state_table[cid]= CID_DEPENDENCY_ST;
    return;
  }
  sid_lbg_id_p = rozofs_storcli_cid_table[cid];
  for (sid = 0; sid < (SID_MAX + 1); sid++)
  {
     lbg_id = sid_lbg_id_p[sid];
     if (lbg_id == (uint32_t)-1) continue;
     if (north_lbg_get_state(lbg_id)==NORTH_LBG_UP)
     {
       up_count++;
     }
     else
     {
       down_count++; cid_state = CID_DOWNGRADED_ST;
     }     
  }
  if ((down_count != 0) && (up_count != 0)) cid_state = CID_DOWNGRADED_ST;
  if (up_count == 0) cid_state = CID_DOWN_ST;
  rozofs_storcli_cid_state_table[cid]= cid_state;
}

/*__________________________________________________________________________
 */

void rozofs_storcli_cid_compute_cid_state() 
{
    int cid;
    int vid_state = CID_UP_ST;
    int cid_up = 0;
    int cid_down = 0;
   for (cid = 0; cid < ROZOFS_CLUSTERS_MAX; cid++)
   {
     rozofs_storcli_cid_compute_one_cid_state(cid);
     switch(rozofs_storcli_cid_state_table[cid])
     {
       case CID_DEPENDENCY_ST:
	 break;
       case CID_UP_ST:
       case CID_DOWNGRADED_ST:
	 cid_up++; 
	 break;
       case CID_DOWN_ST:
	 cid_down++; 
	 break;
       default:
       break;  
     }
   }
   if ((cid_up != 0) && (cid_down!= 0)) vid_state = CID_DOWNGRADED_ST;
   if (cid_up == 0) vid_state = CID_DOWN_ST;
   storcli_vid_state = vid_state;
}
/*__________________________________________________________________________
 */

/**
 * init of the cid table. That table contains pointers to
 * a sid/lbg_id association table, the primary key being the sid index
 */

void rozofs_storcli_cid_table_init() {
    memset(rozofs_storcli_cid_table, 0, ROZOFS_CLUSTERS_MAX * sizeof (uint32_t *));
}

/*__________________________________________________________________________
 */

/**
 *  insert an entry in the  rozofs_storcli_cid_table table

   @param cid : cluster index
   @param sid : storage index
   @param lbg_id : load balancing group index
   
   @retval 0 on success
   @retval < 0 on error
 */
int rozofs_storcli_cid_table_insert(cid_t cid, sid_t sid, uint32_t lbg_id) {
    uint32_t *sid_lbg_id_p;

    if (cid >= ROZOFS_CLUSTERS_MAX) {
        /*
         ** out of range
         */
        return -1;
    }

    sid_lbg_id_p = rozofs_storcli_cid_table[cid - 1];
    if (sid_lbg_id_p == NULL) {
        /*
         ** allocate the cid/lbg_id association table if it does not
         ** exist
         */
        sid_lbg_id_p = xmalloc(sizeof (uint32_t)*(SID_MAX + 1));
        memset(sid_lbg_id_p, -1, sizeof (uint32_t)*(SID_MAX + 1));
        rozofs_storcli_cid_table[cid - 1] = sid_lbg_id_p;
    }
    sid_lbg_id_p[sid - 1] = lbg_id;
        /*
    ** clear the tmo supervision structure assocated with the lbg
    */
    storcli_lbg_cnx_sup_clear_tmo(lbg_id);
    
    return 0;

}

/*__________________________________________________________________________
 */

/** Send a request to a storage node for get the list of TCP ports this storage
 *
 * @param storage: the storage node
 *
 * @return 0 on success otherwise -1
 */
static int get_storage_ports(mstorage_t *s) {
    int status = -1;
    int i = 0;
    mclient_t mclt;

    mp_io_address_t io_address[STORAGE_NODE_PORTS_MAX];
    memset(io_address, 0, STORAGE_NODE_PORTS_MAX * sizeof (mp_io_address_t));

    mclient_new(&mclt, s->host, 0, 0);

    struct timeval timeo;
    timeo.tv_sec = common_config.mproto_timeout;
    timeo.tv_usec = 0;


    /* Initialize connection with storage (by mproto) */
    if (mclient_connect(&mclt, timeo) != 0) {
        s->error = errno;
        DEBUG("Warning: failed to join storage (host: %s), %s.\n",
                s->host, strerror(errno));
        goto out;
    } 
    
    
    /* Send request to get storage TCP ports */
    if (mclient_ports(&mclt, &s->single_storio, io_address) != 0) {
        s->error = errno;
        DEBUG("Warning: failed to get ports for storage (host: %s).\n",
                s->host);
	/* Release mclient*/
	mclient_release(&mclt);
        goto out;
    }
    
    /* Release mclient*/
    mclient_release(&mclt);
    
    /* Copy each TCP ports */
    for (i = 0; i < STORAGE_NODE_PORTS_MAX; i++) {

      if (io_address[i].port != 0) {
	      uint32_t ip = io_address[i].ipv4;

	if (ip == INADDR_ANY) {
		// Copy storage hostnane and IP
		strcpy(s->sclients[i].host, s->host);
		rozofs_host2ip(s->host, &ip);
	} else {
		sprintf(s->sclients[i].host, "%u.%u.%u.%u", ip >> 24,
				(ip >> 16) & 0xFF, (ip >> 8) & 0xFF, ip & 0xFF);
	}
	s->sclients[i].ipv4 = ip;
	s->sclients[i].port = io_address[i].port;
	s->sclients[i].status = 0;
	s->sclients_nb++;
      }
    }

    status = 0;
out:
    return status;
}

/*__________________________________________________________________________
 */
/** Thread : Check if the connections for one storage node are active or not
 *
 * @param storage: the storage node
 */
#define CONNECTION_THREAD_TIMESPEC  2

void *connect_storage(void *v) {
	mstorage_t *mstorage = (mstorage_t*) v;
	int configuration_done = 0;
	
	uma_dbg_thread_add_self("Connect");


	struct timespec ts = { CONNECTION_THREAD_TIMESPEC, 0 };
	mstorage->cnf_status = mstorage_cnf_idle;
        mstorage->cnf_count  = 0;
	mstorage->error      = 0;
	
	if (mstorage->sclients_nb != 0) {
		configuration_done = 1;
		ts.tv_sec = CONNECTION_THREAD_TIMESPEC * 20;
		mstorage->cnf_status = mstorage_cnf_already_done;
	}

	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

	for (;;) {
		if (configuration_done == 0) {
		
			mstorage->cnf_count++;

			/* We don't have the ports for this storage node */
			if (mstorage->sclients_nb == 0) {
			        mstorage->cnf_status = mstorage_cnf_get_ports;
				/* Get ports for this storage node */
				get_storage_ports(mstorage);
				if (mstorage->sclients_nb != 0) {
				  mstorage->cnf_status = mstorage_cnf_add_lbg;
				  mstorage->cnf_count  = 0;
				  mstorage->error = 0;
				}  
			}
			if (mstorage->sclients_nb != 0) {
				/*
				 ** configure the load balancing group is not yet done:
				 ** here we have to address the race competition case since
				 ** that thread runs in parallel with the socket controller, so
				 ** we cannot configure the load balancing group from that thread
				 ** we just can assert a flag to indicate that the configuration
				 ** data of the lbg are available.
				 */
				mstorage->cnf_status = mstorage_cnf_add_lbg;
				if (storcli_sup_send_lbg_port_configuration((void *) mstorage) == 0) {				  
				  configuration_done = 1;
				  mstorage->cnf_status = mstorage_cnf_done;
				  ts.tv_sec = CONNECTION_THREAD_TIMESPEC * 20;
				}  
			}
		}

		nanosleep(&ts, NULL);
	}
	return 0;
}
/*__________________________________________________________________________
 */

/**
 *  API to start the connect_storage thread
 *
 *   The goal of that thread is to retrieve the port configuration for the mstorage
   that were not accessible at the time storcli has started.
   That function is intended to be called for the following cases:
     - after the retrieving of the storage configuration from the exportd
     - after the reload of the configuration from the exportd
    
 @param none
 
 @retval none
 */
void rozofs_storcli_start_connect_storage_thread() {
    list_t *p = NULL;

    list_for_each_forward(p, &exportclt.storages) {

        mstorage_t *storage = list_entry(p, mstorage_t, list);
        if (storage->thread_started == 1) {
            /*
             ** thread has already been started
             */
            continue;
        }
        pthread_t thread;

        if ((errno = pthread_create(&thread, NULL, connect_storage, storage)) != 0) {
            severe("can't create connection thread: %s", strerror(errno));
        }
        storage->thread_started = 1;
    }
}
/*__________________________________________________________________________
 */

/**
 *  Configure all LBG for a storage node

  @param none
  
  @retval 0 on success
  @retval -1 on error
 */
int rozofs_storcli_setup_all_lbg_of_storage(mstorage_t *s) {
  int sid;
  int cid;  
  int i,idx;
  int ret;
  int hostlen;

  if (s->sclients_nb == 0) return 0;

  /*
  ** In single storio mode, there is only one LBG for all cids of this storage
  */
  if (s->single_storio) {

    /*
    ** allocate the load balancing group for the mstorage
    */
    if (s->lbg_id[0] == -1) {
    
      s->lbg_id[0] = north_lbg_create_no_conf();
      if (s->lbg_id[0] < 0) {
	severe(" out of lbg contexts");
	return -1;
      }	

      /*
      ** proceed with storage configuration if the number of port is different from 0
      */
      ret = storaged_lbg_initialize(s,0);
      if (ret < 0) {
	severe("storaged_lbg_initialize");
        return -1;
      }
    }
    
    /*
    ** init of the cid/sid<-->lbg_id association table
    */
    for (i = 0; i < s->sids_nb; i++) {
      rozofs_storcli_cid_table_insert(s->cids[i], s->sids[i], s->lbg_id[0]);
    }
    
    return 0;
  }

  /*
  ** In multiple storio, there is one LBG per cluster on the storage node.
  ** The destination port is the base port number + the cid value
  */
  for (idx = 0; idx < s->sids_nb; idx++) {

    cid = s->cids[idx];
    sid = s->sids[idx];
    
    /*
    ** LBG already configured for this cluster
    */
    if (s->lbg_id[cid-1] == -1) {

      /*
      ** allocate the load balancing group for the mstorage
      */
      s->lbg_id[cid-1] = north_lbg_create_no_conf();
      if (s->lbg_id[cid-1] < 0) {
	severe(" out of lbg contexts %d",cid);
	return -1;
      }

      /* Add the cid number to the service port */
      for (i = 0; i < STORAGE_NODE_PORTS_MAX; i++) {
	if (s->sclients[i].port != 0) {
	  s->sclients[i].port += cid;
	}
      }
      
      /*
      ** Add cluster number to the LBG name
      */
      hostlen = strlen(s->host);
      sprintf(&s->host[hostlen],"_c%d",cid);

      /*
      ** proceed with storage configuration if the number of port is different from 0
      */
      ret = storaged_lbg_initialize(s,cid-1);

      /*
      ** Restore host name
      */
      s->host[hostlen] = 0;

      /* Restore the base service port number */
      for (i = 0; i < STORAGE_NODE_PORTS_MAX; i++) {
	if (s->sclients[i].port != 0) {
	  s->sclients[i].port -= cid;
	}
      }
      
      /* Initialization failed */	
      if (ret < 0) {
	severe("storaged_lbg_initialize %d",cid);      
        return -1;
      }
    }
    
    /*
    ** init of the cid/sid<-->lbg_id association table
    */
    rozofs_storcli_cid_table_insert(cid, sid, s->lbg_id[cid-1]);
  } 
  
  return 0;
}	
/*__________________________________________________________________________
 */

/**
 *  Get the exportd configuration:
  The goal of that procedure is to get the list of the mstorages
  that are associated with the exportd that is referenced as input
  argument of the storage client process
  
  that API uses the parameters stored in the conf structure

  @param none
  
  @retval 0 on success
  @retval -1 on error
 */
extern int  transform_libinit(void);
int rozofs_storcli_get_export_config(storcli_conf *conf) {
    int i = 0;
    int ret;
    list_t *iterator = NULL;
    int export_index=0;
    char * pHost;
	  
    /* Initialize rozofs */
//    rozofs_layout_initialize();

    struct timeval timeout_exportd;
    timeout_exportd.tv_sec  = 0; //ROZOFS_TMR_GET(TMR_EXPORT_PROGRAM);
    timeout_exportd.tv_usec = 200000;
    
    init_rpcctl_ctx(&exportclt.rpcclt);  
    
  
    if (rozofs_host_list_parse(conf->host,'/') == 0) {
      severe("rozofs_host_list_parse(%s)",conf->host);
    }      

    /* Initiate the connection to the export and get information
     * about exported filesystem */
    int retry=15;
    while (retry>0) {
    
      for (export_index=0; export_index < ROZOFS_HOST_LIST_MAX_HOST; export_index++) { 
      
        pHost = rozofs_host_list_get_host(export_index);
	if (pHost == NULL) break;
	
	if (exportclt_initialize(
        	&exportclt,
        	pHost,
        	conf->export,
		conf->site,
        	conf->passwd,
        	conf->buf_size * 1024,
        	conf->buf_size * 1024,
        	conf->max_retry,
        	timeout_exportd) == 0) {
	    break;
	}	  
      }
      if (pHost != NULL) break;
      
      if (timeout_exportd.tv_usec == 200000) {
        timeout_exportd.tv_usec = 500000;
      }
      else {
        timeout_exportd.tv_usec = 0;
        timeout_exportd.tv_sec++; 
      }	     
      retry--; 
    }
    if (pHost == NULL) {
          fprintf(stderr,
                  "storcli failed for:\n" "export directory: %s\n"
                  "export hostname: %s\n" "error: %s\n"
                  "See log for more information\n", conf->export, conf->host,
                  strerror(errno));
          return -1;    
    }	  

    /* Initiate the connection to each storage node (with mproto),
     *  get the list of ports and
     *  establish a connection with each storage socket (with sproto) */
    list_for_each_forward(iterator, &exportclt.storages) {

        mstorage_t *s = list_entry(iterator, mstorage_t, list);

        mclient_t mclt;	
	mclient_new(&mclt, s->host, 0, 0);

        mp_io_address_t io_address[STORAGE_NODE_PORTS_MAX];
        memset(io_address, 0, STORAGE_NODE_PORTS_MAX * sizeof (mp_io_address_t));


        struct timeval timeout_mproto;
        timeout_mproto.tv_sec = ROZOFS_TMR_GET(TMR_MONITOR_PROGRAM);
        timeout_mproto.tv_usec = 0;

        /* Initialize connection with storage (by mproto) */
        if (mclient_connect(&mclt, timeout_mproto) != 0) {
            fprintf(stderr, "Warning: failed to join storage (host: %s), %s.\n",
                    s->host, strerror(errno));
            continue;		    
        }

        /* Send request to get storage TCP ports */
        if (mclient_ports(&mclt, &s->single_storio, io_address) != 0) {
            fprintf(stderr,
                    "Warning: failed to get ports for storage (host: %s).\n"
                    , s->host);
        }
	
        /* Release mclient*/
        mclient_release(&mclt);	

        /* Configuration not received */
        if (io_address[0].port == 0) continue;

        /* Initialize each TCP ports connection with this storage node
         *  (by sproto) */
	for (i = 0; i < STORAGE_NODE_PORTS_MAX; i++) {
	  if (io_address[i].port != 0) {
	    uint32_t ip = io_address[i].ipv4;

	    if (ip == INADDR_ANY) {
	      // Copy storage hostnane and IP
	      strcpy(s->sclients[i].host, s->host);
	      rozofs_host2ip(s->host, &ip);
	    } else {
	      sprintf(s->sclients[i].host, "%u.%u.%u.%u", ip >> 24,
			      (ip >> 16) & 0xFF, (ip >> 8) & 0xFF, ip & 0xFF);
	    }
	    s->sclients[i].ipv4 = ip;
	    s->sclients[i].port = io_address[i].port;
	    s->sclients[i].status = 0;
	    s->sclients_nb++;
	  }
	}

        ret = rozofs_storcli_setup_all_lbg_of_storage(s);
	if (ret != 0) {
	  goto fatal;
	}  

    }
    
    /*
    ** Release connection and change the export timeout according to the configuration
    */
    rpcclt_release(&exportclt.rpcclt);
    exportclt.timeout.tv_usec = 0;
    exportclt.timeout.tv_sec  = 60;

    /*
    ** start the exportd configuration polling thread
    */
    rozofs_storcli_start_exportd_config_supervision_thread(&exportclt);
    return 0;


fatal:
    return -1;
}







void usage() {
    printf("Rozofs storage client daemon - %s\n", VERSION);
    printf("Usage: storcli -i <instance> [OPTIONS]\n\n");
    printf("\t-h, --help\tprint this message.\n");
    printf("\t-H,--host EXPORT_HOST\t\tdefine address (or dns name) where exportd daemon is running (default: rozofsexport) \n");
    printf("\t-E,--path EXPORT_PATH\t\tdefine path of an export see exportd (default: /srv/rozofs/exports/export)\n");
    printf("\t-M,--mount MOUNT_POINT\t\tmount point\n");
    printf("\t-P,--pwd EXPORT_PASSWD\t\tdefine passwd used for an export see exportd (default: none) \n");
    printf("\t-D,--dbg DEBUG_PORT\t\tdebug port (default: none) \n");
    printf("\t-R,--rozo_instance ROZO_INSTANCE\t\trozofsmount instance number \n");
    printf("\t-i,--instance index\t\t unique index of the module instance related to export \n");
    printf("\t-s,--storagetmr \t\t define timeout (s) for IO storaged requests (default: 3)\n");
    printf("\t-t,--sparetmrms\t\t define timeout (ms) for switching to a spare storage\n");
    printf("\t-S,--shaper VALUE\t\tShaper initial value (default 1)\n");
    printf("\t-g,--geosite <0|1>\t\tSite number for geo-replication case (default 0)\n");
    printf("\t-o,--owner <string>\t\tstorcli owner name(default: rozofsmount)\n");
    printf("\t-r,--mojThreadRead <enable|disable>\t\tWhether the read mojette threads are enabled\n");
    printf("\t-w,--mojThreadWrite <enable|disable>\t\tWhether the read mojette threads are enabled\n");
    printf("\t-m,--mojThreadThreshold value\t\tThe number of bytes from which the storage threads are called\n");
    printf("\t-L,--layout <0|1|2>\t\tredundancy level supported by the process\n");
    printf("\t-B,--bsize <0|1|2>\t\tfile system block size (0:4K/1:8K/2:16K\n");
    printf("\t-f,--localPreference\t\tfavor local storage on read to save network bandwith in case of poor network connection\n");
    printf("\t-F,--noReadFaultTolerant\t\tReturn EIO on block corruption detection.\n");

}

/**
*  Signal catching
*/

static void storlci_handle_signal(int sig)
{

}



int main(int argc, char *argv[]) {
    int c;
    int ret;
    int val;
    static struct option long_options[] = {
        { "help", no_argument, 0, 'h'},
        { "host", required_argument, 0, 'H'},
        { "path", required_argument, 0, 'E'},
        { "pwd", required_argument, 0, 'P'},
        { "dbg", required_argument, 0, 'D'},
        { "shaper", required_argument, 0, 'S'},
        { "mount", required_argument, 0, 'M'},
        { "instance", required_argument, 0, 'i'},
        { "rozo_instance", required_argument, 0, 'R'},
        { "storagetmr", required_argument, 0, 's'},
        { "sparetmrms", required_argument, 0, 't'},
        { "mojThreadRead", required_argument, 0, 'r'},
        { "mojThreadWrite", required_argument, 0, 'w'},
        { "mojThreadThreshold", required_argument, 0, 'm'},
        { "layout", required_argument, 0, 'L'},
        { "bsize", required_argument, 0, 'B'},
        { "geosite", required_argument, 0, 'g'},
        { "owner", required_argument, 0, 'o'},
        { "localPreference", required_argument, 0, 'f'},
        { "noReadFaultTolerant", required_argument, 0, 'F'},
        { 0, 0, 0, 0}
    };

    ALLOC_PROFILING(stcpp_profiler_t);

    /*
    ** Change local directory to "/"
    */
    if (chdir("/")!=0) {}

    uma_dbg_thread_add_self("Main");
    /*
    ** Make the syslog available for the process
    */
    uma_dbg_record_syslog_name("storcli");

    /*
    ** init of the timer configuration
    */
    rozofs_tmr_init_configuration();
    /*
    ** init of the transform library
    */
    transform_libinit();
    
    /*
    ** init of the shared memory reference
    */
    {
      int k;
      
      for (k= 0; k < SHAREMEM_PER_FSMOUNT;k++)
      {
	storcli_rozofsmount_shared_mem[k].key = 0;
	storcli_rozofsmount_shared_mem[k].error = errno;
	storcli_rozofsmount_shared_mem[k].buf_sz = 0; 
	storcli_rozofsmount_shared_mem[k].buf_count = 0; 
	storcli_rozofsmount_shared_mem[k].active = 0; 
	storcli_rozofsmount_shared_mem[k].data_p = NULL;
      } 
    }   
    
    conf.host = NULL;
    conf.passwd = NULL;
    conf.export = NULL;
    conf.mount = NULL;
    conf.module_index = -1;
    conf.layout = -1;
    conf.bsize = -1;
    conf.buf_size = 256;
    conf.max_retry = 3;
    conf.dbg_port = 0;
    conf.rozofsmount_instance = 0;
    conf.sharedmem_key = 0;
    conf.shaper = 1; // Default value for traffic shaping 
    conf.site = 0;
    conf.owner=NULL;

    conf.mojThreadWrite      = -1;
    conf.mojThreadRead       = -1;    
    conf.mojThreadThreshold  = -1;
    conf.localPreference     = 0;  
    conf.noReadFaultTolerant = 0;
    
    while (1) {

        int option_index = 0;
        c = getopt_long(argc, argv, "hH:E:P:i:D:M:R:s:t:k:c:l:S:g:o:r:w:m:L:B:Ff", long_options, &option_index);

        if (c == -1)
            break;

        switch (c) {
            case 'h':
                usage();
                exit(EXIT_SUCCESS);
                break;
            case 'H':
                conf.host = strdup(optarg);
                break;
            case 'o':
                conf.owner = strdup(optarg);
                break;
            case 'E':
                conf.export = strdup(optarg);
                break;
            case 'P':
                conf.passwd = strdup(optarg);
                break;
            case 'M':
                conf.mount = strdup(optarg);
                break;
	    case 'f':
	        conf.localPreference = 1;
		break;
	    case 'F':
	        conf.noReadFaultTolerant = 1;
		break;
            case 'i':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                conf.module_index = val;
                break;
             case 'L':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
		if ((val < 0) || (val >= LAYOUT_MAX))
		{
		    severe("out of range layout %d",val);
                    usage();
                    exit(EXIT_FAILURE);				
		}
                conf.layout = val;
                break;
             case 'B':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
		if ((val < 0) || (val >= 2))
		{
		    severe("out of range blocksize %d",val);
                    usage();
                    exit(EXIT_FAILURE);				
		}
                conf.bsize = val;
                break;
            case 'g':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                conf.site = val;
                break;
            case 'D':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                conf.dbg_port = val;
                break;	
            case 'R':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                conf.rozofsmount_instance = val;
                break;

            case 'S':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                conf.shaper = val;
                break;            
	   case 's':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                rozofs_tmr_configure(TMR_STORAGE_PROGRAM,val);
                break;
	   case 't':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                rozofs_tmr_configure(TMR_PRJ_READ_SPARE,val);
                break;

            case 'k':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                storcli_rozofsmount_shared_mem[SHAREMEM_IDX_READ].key = (key_t) val;
                storcli_rozofsmount_shared_mem[SHAREMEM_IDX_WRITE].key = (key_t) (val+1);
                break;
            case 'c':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                storcli_rozofsmount_shared_mem[SHAREMEM_IDX_READ].buf_count = (key_t) val;
                storcli_rozofsmount_shared_mem[SHAREMEM_IDX_WRITE].buf_count = (key_t) val;
                break;
            case 'l':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                storcli_rozofsmount_shared_mem[SHAREMEM_IDX_READ].buf_sz = (key_t) (val);
                storcli_rozofsmount_shared_mem[SHAREMEM_IDX_WRITE].buf_sz = (key_t) (val);
                break;
		
            case 'r':
                errno = 0;
                if      (strcasecmp(optarg,"enable")==0) {
		  conf.mojThreadRead = 1;
		  rozofs_stcmoj_thread_enable_read(1);
		}  
		else if (strcasecmp(optarg,"disable")==0) {
		  conf.mojThreadRead = 0;		
		  rozofs_stcmoj_thread_enable_read(0);
		}
		else {  
                    usage();
                    exit(EXIT_FAILURE);
                }
                break;
		
            case 'w':
                errno = 0;
                if      (strcasecmp(optarg,"enable")==0) {
		  conf.mojThreadWrite = 1;		
		  rozofs_stcmoj_thread_enable_write(1);
		}  
		else if (strcasecmp(optarg,"disable")==0) {
		  conf.mojThreadWrite = 0;		
		  rozofs_stcmoj_thread_enable_write(0);
		}
		else {  
                    usage();
                    exit(EXIT_FAILURE);
                }
                break;	
		
            case 'm':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
		conf.mojThreadThreshold = val;
                rozofs_stcmoj_thread_set_threshold(val);
                break;
					
            case '?':
                usage();
                exit(EXIT_SUCCESS);
                break;
            default:
                usage();
                exit(EXIT_FAILURE);
                break;
        }
    }
    storcli_site_number = conf.site;
    noReadFaultTolerant = conf.noReadFaultTolerant;
    
    /*
     ** Check the parameters
     */
    if (conf.module_index == -1) {
        printf("instance number is mandatory!\n");
        usage();
        exit(EXIT_FAILURE);
    }
    if (conf.layout == -1) {
        printf("layout is mandatory!\n");
        usage();
        exit(EXIT_FAILURE);
    }
    if (conf.bsize == -1) {
        printf("block size is mandatory!\n");
        usage();
        exit(EXIT_FAILURE);
    }
    if (conf.host == NULL) {
        conf.host = strdup("rozofsexport");
    }

    if (conf.export == NULL) {
        conf.export = strdup("/srv/rozofs/exports/export");
    }

    if (conf.passwd == NULL) {
        conf.passwd = strdup("none");
    }
    
    {
         char path[256];
	 
	 sprintf(path,"%s/mount/inst_%d/storcli_%d/",ROZOFS_KPI_ROOT_PATH,conf.rozofsmount_instance,conf.module_index);
	 ALLOC_KPI_FILE_PROFILING(path,"profiler",stcpp_profiler_t);    
    }    
    
    /*
    ** read common config file
    */
    common_config_read(NULL);    
    
    /*
    **  set the numa node for rozofsmount and its storcli
    */
    rozofs_numa_allocate_node(conf.rozofsmount_instance);
    

    rozofs_signals_declare("storcli",common_config.nb_core_file);
    rozofs_attach_crash_cbk(storlci_handle_signal);
    
    rozofs_storcli_cid_table_init();
    rozofs_storcli_cid_table_state_init();
    storcli_lbg_cnx_sup_init();

    gprofiler->uptime = time(0);
    
    /*
    ** Kill the eventual storcli with same instance that main be locked
    */
    {
      char cmd[256];
      pid_t pid = getpid();
      
      sprintf(cmd,"ps -o pid,cmd -C storcli | grep rozofsmount | grep \" -i %d \"  | grep \" -R %d \" > /tmp/stc1.%d",
             conf.module_index, conf.rozofsmount_instance, pid); 
      if (system(cmd)){};
      
      sprintf(cmd,"awk \'{if ($1!=pid) print $1; }\' pid=%d /tmp/stc1.%d >  /tmp/stc2.%d", pid, pid, pid); 
      if (system(cmd)){};
      
      sprintf(cmd,"for p in `cat /tmp/stc2.%d`; do kill -9 $p; done", pid); 
      if (system(cmd)){};
      
      sprintf(cmd,"rm -f /tmp/stc1.%d; rm -f /tmp/stc2.%d", pid, pid); 
      if (system(cmd)){};
         
    }
    
    
    /*
    ** check if the rozofsmount has provided a shared memory
    */
    if (storcli_rozofsmount_shared_mem[SHAREMEM_IDX_READ].key != 0)
    {
      if ((storcli_rozofsmount_shared_mem[SHAREMEM_IDX_READ].buf_count != 0)&& (storcli_rozofsmount_shared_mem[SHAREMEM_IDX_READ].buf_sz != 0))
      {
         /*
         ** init of the shared memory
         */
	 int k;
	 for (k = 0; k < SHAREMEM_PER_FSMOUNT; k++)
	 {
           while(1)
           {
             int shmid;
             if ((shmid = shmget(storcli_rozofsmount_shared_mem[k].key, 
                        	storcli_rozofsmount_shared_mem[k].buf_count*storcli_rozofsmount_shared_mem[k].buf_sz, 
                        	0666)) < 0) 
             {
               severe("error on shmget %x : %s",storcli_rozofsmount_shared_mem[k].key,strerror(errno));
               storcli_rozofsmount_shared_mem[k].error = errno;
               storcli_rozofsmount_shared_mem[k].active = 0;
               break;
             }
             /*
             * Now we attach the segment to our data space.
             */
             if ((storcli_rozofsmount_shared_mem[k].data_p = shmat(shmid, NULL, 0)) == (char *) -1)
             {
               severe("error on shmat %d : %s",storcli_rozofsmount_shared_mem[k].key,strerror(errno));
               storcli_rozofsmount_shared_mem[k].error = errno;
               storcli_rozofsmount_shared_mem[k].active = 0;
               break;        
             }
             storcli_rozofsmount_shared_mem[k].active = 1;
             break;
           }
	 }
      }
    }

    /*
    ** clear KPI counters
    */
    memset(&storcli_kpi_transform_forward,0,sizeof(  storcli_kpi_transform_forward));
    memset(&storcli_kpi_transform_inverse,0,sizeof(  storcli_kpi_transform_inverse));
 
    
    /*
     ** init of the non blocking part
     */
    ret = rozofs_storcli_non_blocking_init(conf.dbg_port, conf.rozofsmount_instance);
    if (ret < 0) {
        fprintf(stderr, "Fatal error while initializing non blocking entity\n");
        goto error;
    }
    {
        char name[64];
	if (conf.owner == NULL)
	{
          sprintf(name, "storcli %d of rozofsmount %d", conf.module_index, conf.rozofsmount_instance);
	}
	else
	{
          sprintf(name, "storcli %d of %s %d", conf.module_index,conf.owner, conf.rozofsmount_instance);	
	}
        uma_dbg_set_name(name);
    }
    /**
    * init of the scheduler ring
    */
    ret  = stc_rng_init();
    if (ret < 0) {
        fprintf(stderr, "Fatal error while initializing scheduler ring\n");
        goto error;
    }

    /*
    ** Initialize the disk thread interface and start the disk threads
    */	
    ret = rozofs_stcmoj_thread_intf_create(conf.host, conf.rozofsmount_instance,conf.module_index,
                                           ROZOFS_MAX_DISK_THREADS,ROZOFS_MAX_DISK_THREADS ) ;
    if (ret < 0) {
      fatal("Mojette_disk_thread_intf_create");
      return -1;
    }
    /*
     ** Get the configuration from the export
     */
    ret = rozofs_storcli_get_export_config(&conf);
    if (ret < 0) {
        fprintf(stderr, "Fatal error on rozofs_storcli_get_export_config()\n");
        goto error;
    }
    /*
    ** Declare the debug entry to get the currrent configuration of the storcli
    */
    uma_dbg_addTopic("config_status", show_storcli_configuration);
    /*
    ** Declare the debug entry to get the currrent configuration of the storcli
    */
    uma_dbg_addTopic("storaged_status", show_storage_configuration);
    uma_dbg_addTopic("cnf_storaged", show_cnf_storaged);
    /*
    ** shared memory with rozofsmount
    */
    uma_dbg_addTopic("shared_mem", storcli_shared_mem);
    uma_dbg_addTopic("cid_state", show_cid_state);
    uma_dbg_addTopic("vid_state", show_vid_state);
    
    /*
     ** Init of the north interface (read/write request processing)
     */
    //#warning buffer size and count must not be hardcoded
    ret = rozofs_storcli_north_interface_init(
            exportclt.eid, conf.rozofsmount_instance, conf.module_index,
            STORCLI_NORTH_LBG_BUF_RECV_CNT, STORCLI_NORTH_LBG_BUF_RECV_SZ);
    if (ret < 0) {
        fprintf(stderr, "Fatal error on rozofs_storcli_get_export_config()\n");
        goto error;
    }
    rozofs_storcli_start_connect_storage_thread();
    /*
     ** add the topic for the local profiler
     */
    uma_dbg_addTopic_option("profiler", show_profiler,UMA_DBG_OPTION_RESET);
    /*
     ** add the topic for the local profiler
     */
    uma_dbg_addTopicAndMan("corrupted", show_corrupted, man_corrupted, 0);
    uma_dbg_addTopicAndMan("rwerror", show_rwerror, man_rwerror, 0);

    
    uma_dbg_addTopicAndMan("repair", storcli_cli_repair, storcli_man_repair, UMA_DBG_OPTION_RESET);
   /*
     ** add the topic to display the storcli configuration
     */    
    uma_dbg_addTopic("start_config", show_start_config);
    /*
    ** declare timer debug functions
    */
    rozofs_timer_conf_dbg_init();
    /**
    * init of the traffic shaper
    */
    trshape_module_init(conf.shaper);    
  /*
  **  change the priority of the main thread
  */
    {
      struct sched_param my_priority;
      int policy=-1;
      int ret= 0;

      pthread_getschedparam(pthread_self(),&policy,&my_priority);
          info("storio main thread Scheduling policy   = %s\n",
                    (policy == SCHED_OTHER) ? "SCHED_OTHER" :
                    (policy == SCHED_FIFO)  ? "SCHED_FIFO" :
                    (policy == SCHED_RR)    ? "SCHED_RR" :
                    "???");
 #if 1
      my_priority.sched_priority= 97;
      policy = SCHED_FIFO;
      ret = pthread_setschedparam(pthread_self(),policy,&my_priority);
      if (ret < 0) 
      {
	severe("error on sched_setscheduler: %s",strerror(errno));	
      }
      pthread_getschedparam(pthread_self(),&policy,&my_priority);
          DEBUG("RozoFS thread Scheduling policy (prio %d)  = %s\n",my_priority.sched_priority,
                    (policy == SCHED_OTHER) ? "SCHED_OTHER" :
                    (policy == SCHED_FIFO)  ? "SCHED_FIFO" :
                    (policy == SCHED_RR)    ? "SCHED_RR" :
                    "???");
 #endif        
    }     
    /*
     ** main loop
     */
    info("storcli started (instance: %d, rozofs instance: %d, mountpoint: %s).",
            conf.module_index, conf.rozofsmount_instance, conf.mount);

    while (1) {
        ruc_sockCtrl_selectWait();
    }

error:
    fprintf(stderr, "see log for more details.\n");
    exit(EXIT_FAILURE);
}
