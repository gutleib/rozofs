/*
  Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
  This file is part of Rozofs.

  Rozofs is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published
  by the Free Software Foundation, version 2.

  Rozofs is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for morozofs_fuse_initre details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see
  <http://www.gnu.org/licenses/>.
 */
#include <rozofs/core/ruc_common.h>
#include <rozofs/core/uma_tcp_main_api.h>
#include <rozofs/core/ruc_tcpServer_api.h>
#include <rozofs/core/rozofs_core_files.h>
#include <rozofs/rpc/storcli_lbg_prototypes.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/common/common_config.h>
#include <pthread.h>

#include "rozofs_export_gateway_conf_non_blocking.h"
#include "rozofs_fuse.h"
#include "rozofs_fuse_thread_intf.h"
#include "rozofs_kpi.h"
#include "rozofs_cachetrack.h"
#include "rozofs_io_error_trc.h"
#include "rozofs_bt_proto.h"

// For trace purpose
struct timeval Global_timeDay;
unsigned long long Global_timeBefore, Global_timeAfter;
static rozofs_fuse_conf_t *args_p;


pthread_t heartbeat_thrdId;
rozofs_thr_cnts_t *rozofs_thr_counter[ROZOFSMOUNT_COUNTER_MAX];

int module_test_id = 0;

void rozofs_flock_service_init(void) ;
extern uint64_t   rozofs_client_hash;
/**
*  Init of the module that deals with the export gateways

  At the start-up we just create the entry for the Master Export
  
  @param host : hostname or IP address of the exportd Master
  @param eid : exportid associated with the rozofsmount
  @param export_listening_port : listening port of the export for this eid
  
  @retval RUC_OK on success
  @retval RUC_NOK on failure
*/
int rozofs_expgateway_init(char *host,int eid,uint16_t export_listening_port, af_stream_poll_CBK_t supervision_callback)
{
    int ret;
    
    expgw_export_tableInit();

    /*
    ** init of the AF_UNIX channel used for receiving export gateway configuration changes
    */
    ret = rozofs_exp_moduleInit();
    if (ret != RUC_OK) return ret;

//#warning only 1 gateway: localhost1
#define exportd port is hardcoded : 53000  et slice process port = 8
    ret = expgw_export_add_eid(eid,   // exportd id
                               eid,   // eid
                               host,  // hostname of the Master exportd
#if 0
                               53000,  // port
                               EXPORT_SLICE_PROCESS_NB,  // nb Gateway
#else
//#warning multi-slice is temporary removed
                               export_listening_port,  // port
                               0,  // nb Gateway
#endif			       
                               0,   // gateway rank: not significant for an rozofsmount
                               supervision_callback); 
    if (ret < 0) {
        fatal("Fatal error on expgw_export_add_eid()");
        goto error;
    }
//#warning multi-slice is temporary removed
#if 0
    for (i = 0; i < EXPORT_SLICE_PROCESS_NB; i++)
    {
    
      ret =  expgw_add_export_gateway(eid, host, 53000+i+1,(uint16_t) i);
      if (ret < 0) {
          fatal("Fatal error on expgw_export_add_eid()");
          goto error;
      }    
    }
#endif

    return RUC_OK;
error: 
   return RUC_NOK;

}

uint32_t ruc_init(uint32_t test, uint16_t debug_port,uint16_t export_listening_port) {
    int ret;

#warning: STORCLI_PER_FSMOUNT need to be use to compute the number of LBG
    uint32_t mx_tcp_client = 10;
    uint32_t mx_tcp_server = 10;
    uint32_t mx_tcp_server_cnx = 10;
    uint32_t mx_af_unix_ctx = ROZO_AFUNIX_CTX_ROZOFSMOUNT;
    uint32_t mx_lbg_north_ctx = 48;

    //#warning TCP configuration ressources is hardcoded!!
    /*
     ** trace buffer initialization
     */
    ruc_traceBufInit();
#if 0
    /*
     ** Not needed since there is already done
     ** by libUtil
     */

    /* catch the sigpipe signal for socket 
     ** connections with RELC(s) in this way when a RELC
     ** connection breaks an errno is set on a recv or send 
     **  socket primitive 
     */
    sigAction.sa_flags = SA_RESTART;
    sigAction.sa_handler = SIG_IGN; /* Mask SIGPIPE */
    if (sigaction(SIGPIPE, &sigAction, NULL) < 0) {
        exit(0);
    }
    sigAction.sa_flags = SA_RESTART;
    sigAction.sa_handler = hand; /*  */
    if (sigaction(SIGUSR1, &sigAction, NULL) < 0) {
        exit(0);
    }
#endif



    /*
     ** initialize the socket controller:
     **   4 connections per Relc and 32
     **   for: NPS, Timer, Debug, etc...
     */
    //#warning set the number of contexts for socketCtrl to 100
    ret = ruc_sockctl_init(ROZO_SOCKCTRL_CTX_ROZOFSMOUNT);
    if (ret != RUC_OK) {
        fatal( " socket controller init failed" );
    }

    /*
     **  Timer management init
     */
    ruc_timer_moduleInit(FALSE);

    while (1) {
        /*
         **--------------------------------------
         **  configure the number of TCP connection
         **  supported
         **--------------------------------------   
         **  
         */
        ret = uma_tcp_init(mx_tcp_client + mx_tcp_server + mx_tcp_server_cnx);
        if (ret != RUC_OK) break;

        /*
         **--------------------------------------
         **  configure the number of TCP server
         **  context supported
         **--------------------------------------   
         **  
         */
        ret = ruc_tcp_server_init(mx_tcp_server);
        if (ret != RUC_OK) break;

        /*
         **--------------------------------------
         **  configure the number of TCP client
         **  context supported
         **--------------------------------------   
         **  
         */
        //     ret = ruc_tcp_clientinit(mx_tcp_client);
        //     if (ret != RUC_OK) break;   


        /*
         **--------------------------------------
         **  configure the number of AF_UNIX
         **  context supported
         **--------------------------------------   
         **  
         */
        ret = af_unix_module_init(mx_af_unix_ctx,
                32, 1024, // xmit(count,size)
                32, 1024 // recv(count,size)
                );
        if (ret != RUC_OK) break;

        /*
         ** init of th emodule that handles the load balancing group
         ** there is one load balancing group for the exportd
         ** and one load balancing group per storaged
         */
#if 1
        ret = north_lbg_module_init(mx_lbg_north_ctx);
        if (ret != RUC_OK) break;

        ret = rozofs_tx_module_init(args_p->max_transactions*2, // fuse trx + internal trx
                args_p->max_transactions*2, 1024*4, // xmit small [count,size]
                args_p->max_transactions, (1024 * 2), // xmit large [count,size]
                args_p->max_transactions, 1024, // recv small [count,size]
                args_p->max_transactions, (1024 * 64)); // recv large [count,size];  

        if (ret != RUC_OK) break;
#endif    

        exportclt_t *exportclt = args_p->exportclt;
        ret = rozofs_expgateway_init( exportclt->host,(int)exportclt->eid, export_listening_port,
	                              (af_stream_poll_CBK_t) rozofs_export_lbg_cnx_polling);
        if (ret != RUC_OK) break;
        break;

    }



    /*
     ** internal debug init
     */
    //ruc_debug_init();


    /*
     **--------------------------------------
     **   D E B U G   M O D U L E
     **--------------------------------------
     */
    {
        char name[32];
        sprintf(name, "rozofsmount %d", args_p->instance);
        uma_dbg_set_name(name);

        sprintf(name, "mount:%d", args_p->instance);
        uma_dbg_init(10, INADDR_ANY, debug_port, name);
    }    

    /*
     ** init of the stats module
     */
    //     rozofs_stats_init();
    rozofs_flock_service_init();

    return ret;
}
/*_______________________________________________________________________
* throughput manual
*
* @param pChar    Where to format the ouput
*/
#define PCHAR_STRING_BLD(x) pChar += rozofs_string_append_bold(pChar,x)
#define PCHAR_STRING(x)     pChar += rozofs_string_append(pChar,x)
static inline void man_throughput (char * pChar) {
  PCHAR_STRING    ("Display rozofsmount throughput history.\n");
  PCHAR_STRING_BLD(" throughput [read|write] [col <#col>] [avg] [s|m|h|a] [persec]\n");
  PCHAR_STRING_BLD("    read         ");
  PCHAR_STRING    (" only display read counters.\n");
  PCHAR_STRING_BLD("    write        ");
  PCHAR_STRING    (" only display write counters.\n");
  PCHAR_STRING    ("      when neither read nor write are set all counters are displayed.\n");
  PCHAR_STRING_BLD("    [col <#col>] ");
  PCHAR_STRING    (" request the display history on ");
  PCHAR_STRING_BLD("<#col>");
  PCHAR_STRING    (" columns [1..15].\n");
  PCHAR_STRING_BLD("    [avg]        ");
  PCHAR_STRING    (" display an average at the end of each column.\n");    
  PCHAR_STRING_BLD("    s            ");
  PCHAR_STRING    (" display last 60 seconds history (default)\n");    
  PCHAR_STRING_BLD("    m            ");
  PCHAR_STRING    (" display last 60 minutes history\n");  
  PCHAR_STRING_BLD("    h            ");
  PCHAR_STRING    (" display last 60 hours   history\n");  
  PCHAR_STRING_BLD("    a            ");
  PCHAR_STRING    (" display hour, minute and second history\n");  
  PCHAR_STRING_BLD("    persec       ");
  PCHAR_STRING    (" display throughput per second. Default is to have throughput per display step.\n");  
}
/*_______________________________________________________________________
* Display throughput counters
*
* @param pChar    Where to format the ouput
*/
void display_throughput (char * argv[], uint32_t tcpRef, void *bufRef) {
  char * pChar = uma_dbg_get_buffer();
  int ret,val,what=0;
  rozofs_thr_unit_e unit = rozofs_thr_unit_second;

  rozofs_thr_set_column(6);
  
  int i=1;
  while (argv[i] != NULL) {
  
    if (strcasecmp(argv[i],"col")==0) {
      i++;
      if (argv[i] == NULL) {
	pChar += rozofs_string_append(pChar,"\nthroughput col <#col>\n");   
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;      
      } 
      ret = sscanf(argv[i],"%d",&val);
      if (ret != 1) {
	pChar += rozofs_string_append(pChar,"\nthroughput col <#col>\n");   
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;      
      } 
      i++;
      rozofs_thr_set_column(val);
      continue;
    }  
    
    if (strcasecmp(argv[i],"avg")==0) {
      i++;
      rozofs_thr_set_average();
      continue;
    }  
    
    if (strcasecmp(argv[i],"read")==0) { 
      i++;
      what |= 1;
      continue;
    }  
    
    if (strcasecmp(argv[i],"write")==0) {      
      i++;
      what |= 2;
      continue;
    }
    
    if (strcasecmp(argv[i],"persec")==0) {      
      i++;
      rozofs_thr_display_throughput_per_sec();
      continue;
    }

    if (strcasecmp(argv[i],"s")==0) {       
      i++;
      unit = rozofs_thr_unit_second;
      continue;
    }  

    if (strcasecmp(argv[i],"m")==0) {       
      i++;
      unit = rozofs_thr_unit_minute;
      continue;
    }      
    
    if (strcasecmp(argv[i],"h")==0) {       
      i++;
      unit = rozofs_thr_unit_hour;
      continue;
    }  

    if (strcasecmp(argv[i],"a")==0) {       
      i++;
      unit = rozofs_thr_unit_all;
      continue;
    }  
        
    pChar += rozofs_string_append(pChar,"\nunexpected parameter ");
    pChar += rozofs_string_append(pChar,argv[i]); 
    pChar += rozofs_eol(pChar); 
    man_throughput(pChar);   
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;    
  } 
       
  switch (what) {
    case 1:
      pChar = rozofs_thr_display_bitmask(pChar, rozofs_thr_counter, 1<<ROZOFSMOUNT_COUNTER_READ_THR, unit);
      //pChar = rozofs_thr_display_unit(pChar, &rozofs_thr_counter[ROZOFSMOUNT_COUNTER_READ_THR],1, unit);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;
    case 2:
      pChar = rozofs_thr_display_bitmask(pChar, rozofs_thr_counter, 1<<ROZOFSMOUNT_COUNTER_WRITE_THR, unit);
//      pChar = rozofs_thr_display_unit(pChar, &rozofs_thr_counter[ROZOFSMOUNT_COUNTER_WRITE_THR],1, unit);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;
    default:
      pChar = rozofs_thr_display_bitmask(pChar, rozofs_thr_counter, (1<<ROZOFSMOUNT_COUNTER_READ_THR) | (1<<ROZOFSMOUNT_COUNTER_WRITE_THR), unit);
//      pChar = rozofs_thr_display_unit(pChar, rozofs_thr_counter, 2, unit);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer()); 
      return; 
  }       
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());              
}
/*_______________________________________________________________________
* Display throughput counters
*
* @param pChar    Where to format the ouput
*/
static inline void man_io (char * pChar) {
  PCHAR_STRING    ("Display rozofsmount IO history.\n");
  PCHAR_STRING_BLD(" io [with|exclude] [col <#col>] [avg] [s|m|h|a] [persec]\n");
  PCHAR_STRING_BLD("    with    = [r|w|l|c|d|a|x|o]\n");
  PCHAR_STRING_BLD("    exclude = [nr|nw|nl|nc|nd|na|nx|no]\n");
  PCHAR_STRING    ("      . r(nr) to (not to) display read bytes/IO/latency\n");
  PCHAR_STRING    ("      . w(nw) to (not to) display write bytes/IO/latency\n");
  PCHAR_STRING    ("      . l(nl) to (not to) display lookup count\n");
  PCHAR_STRING    ("      . c(nc) to (not to) display file/directory creation count\n");
  PCHAR_STRING    ("      . d(nd) to (not to) display file/directory deletion count\n");
  PCHAR_STRING    ("      . a(na) to (not to) display get attribute count\n");
  PCHAR_STRING    ("      . x(nx) to (not to) display extended attribute operation count\n");
  PCHAR_STRING    ("      . f(nf) to (not to) display file lock operation count\n"); 
  PCHAR_STRING    ("      . D(nD) to (not to) display open/read/release directory count\n"); 
  PCHAR_STRING    ("      . L(nL) to (not to) display link, slink, getlink count\n"); 
  PCHAR_STRING    ("      . o(no) to (not to) display other metadate count :\n");
  PCHAR_STRING    ("          setattr, forget, open, rename and flush. \n"); 
  PCHAR_STRING    ("    Default is to display all available counters\n");
  PCHAR_STRING_BLD("    [col <#col>] ");
  PCHAR_STRING    (" request the display history on ");
  PCHAR_STRING_BLD("<#col>");
  PCHAR_STRING    (" columns [1..15].\n");
  PCHAR_STRING_BLD("    [avg]        ");
  PCHAR_STRING    (" display an average at the end of each column.\n");    
  PCHAR_STRING_BLD("    s            ");
  PCHAR_STRING    (" display last 60 seconds history (default)\n");    
  PCHAR_STRING_BLD("    m            ");
  PCHAR_STRING    (" display last 60 minutes history\n");  
  PCHAR_STRING_BLD("    h            ");
  PCHAR_STRING    (" display last 60 hours   history\n");  
  PCHAR_STRING_BLD("    a            ");
  PCHAR_STRING    (" display hour, minute and second history\n");  
  PCHAR_STRING_BLD("    persec       ");
  PCHAR_STRING    (" display throughput per second. Default is to have throughput per display step.\n");  
}
/*_______________________________________________________________________
* Display throughput counters
*
* @param pChar    Where to format the ouput
*/
void display_io (char * argv[], uint32_t tcpRef, void *bufRef) {
  char * pChar = uma_dbg_get_buffer();
  int ret,val;
  int requested, excluded;
  rozofs_thr_unit_e unit = rozofs_thr_unit_second;

  rozofs_thr_set_column(2);

  int i=1;
  
  requested = 0;
  excluded  = 0;
  
  while (argv[i] != NULL) {
  
    if (strcasecmp(argv[i],"c")==0) {
      requested |= ((1<<ROZOFSMOUNT_COUNTER_DCR8)+(1<<ROZOFSMOUNT_COUNTER_FCR8));
      i++;
      continue;
    }
    if (strcasecmp(argv[i],"nc")==0) {
      excluded |= ((1<<ROZOFSMOUNT_COUNTER_DCR8)+(1<<ROZOFSMOUNT_COUNTER_FCR8));
      i++;
      continue;
    }    
    if (strcasecmp(argv[i],"d")==0) {
      requested |= ((1<<ROZOFSMOUNT_COUNTER_DDEL)+(1<<ROZOFSMOUNT_COUNTER_FDEL));
      i++;
      continue;  
    }
    if (strcasecmp(argv[i],"nd")==0) {
      excluded |= ((1<<ROZOFSMOUNT_COUNTER_DDEL)+(1<<ROZOFSMOUNT_COUNTER_FDEL));
      i++;
      continue;  
    }    
    if (strcasecmp(argv[i],"l")==0) {
      requested |= (1<<ROZOFSMOUNT_COUNTER_LOOKUP);
      i++;
      continue;
    }  
    if (strcasecmp(argv[i],"nl")==0) {
      excluded |= (1<<ROZOFSMOUNT_COUNTER_LOOKUP);
      i++;
      continue;
    }            
    if (strcasecmp(argv[i],"a")==0) {
      requested |= (1<<ROZOFSMOUNT_COUNTER_GETATTR);
      i++;
      continue;
    }  
    if (strcasecmp(argv[i],"na")==0) {
      excluded |= (1<<ROZOFSMOUNT_COUNTER_GETATTR);
      i++;
      continue;
    }      
    if (strcasecmp(argv[i],"x")==0) {
      requested |= (1<<ROZOFSMOUNT_COUNTER_XATTR);
      i++;
      continue;
    }    
    if (strcasecmp(argv[i],"nx")==0) {
      excluded |= (1<<ROZOFSMOUNT_COUNTER_XATTR);
      i++;
      continue;
    }          
    if (strcasecmp(argv[i],"f")==0) {
      requested |= (1<<ROZOFSMOUNT_COUNTER_FLOCK);
      i++;
      continue;
    }      
    if (strcasecmp(argv[i],"nf")==0) {
      excluded |= (1<<ROZOFSMOUNT_COUNTER_FLOCK);
      i++;
      continue;
    }   
    if (strcasecmp(argv[i],"D")==0) {
      requested |= (1<<ROZOFSMOUNT_COUNTER_LS);
      i++;
      continue;
    }      
    if (strcasecmp(argv[i],"nD")==0) {
      excluded |= (1<<ROZOFSMOUNT_COUNTER_LS);
      i++;
      continue;
    }   
    if (strcasecmp(argv[i],"L")==0) {
      requested |= (1<<ROZOFSMOUNT_COUNTER_LINK);
      i++;
      continue;
    }      
    if (strcasecmp(argv[i],"nL")==0) {
      excluded |= (1<<ROZOFSMOUNT_COUNTER_LINK);
      i++;
      continue;
    }   
    if (strcasecmp(argv[i],"o")==0) {
      requested |= (1<<ROZOFSMOUNT_COUNTER_OTHER);
      i++;
      continue;
    }      
    if (strcasecmp(argv[i],"no")==0) {
      excluded |= (1<<ROZOFSMOUNT_COUNTER_OTHER);
      i++;
      continue;
    }   
    if (strcasecmp(argv[i],"r")==0) {
      requested |= ((1<<ROZOFSMOUNT_COUNTER_READ_IO)|(1<<ROZOFSMOUNT_COUNTER_READ_THR)|(1<<ROZOFSMOUNT_COUNTER_READ_LATENCY));
      i++;
      continue;
    } 
    if (strcasecmp(argv[i],"nr")==0) {
      excluded |= ((1<<ROZOFSMOUNT_COUNTER_READ_IO)|(1<<ROZOFSMOUNT_COUNTER_READ_THR)|(1<<ROZOFSMOUNT_COUNTER_READ_LATENCY));
      i++;
      continue;
    }     
    if (strcasecmp(argv[i],"w")==0) {
      requested |= ((1<<ROZOFSMOUNT_COUNTER_WRITE_IO)|(1<<ROZOFSMOUNT_COUNTER_WRITE_THR)|(1<<ROZOFSMOUNT_COUNTER_WRITE_LATENCY));
      i++;
      continue;
    }     
    if (strcasecmp(argv[i],"nw")==0) {
      excluded |= ((1<<ROZOFSMOUNT_COUNTER_WRITE_IO)|(1<<ROZOFSMOUNT_COUNTER_WRITE_THR)|(1<<ROZOFSMOUNT_COUNTER_WRITE_LATENCY));
      i++;
      continue;
    }      
    if (strcasecmp(argv[i],"col")==0) {
      i++;
      if (argv[i] == NULL) {
	pChar += rozofs_string_append(pChar,"\nthroughput col <#col>\n");   
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;      
      } 
      ret = sscanf(argv[i],"%d",&val);
      if (ret != 1) {
	pChar += rozofs_string_append(pChar,"\nthroughput col <#col>\n");   
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;      
      } 
      i++;
      rozofs_thr_set_column(val);
      continue;
    }  
    
    if (strcasecmp(argv[i],"avg")==0) {
      i++;
      rozofs_thr_set_average();
      continue;
    }  
    
    
    if (strcasecmp(argv[i],"persec")==0) {      
      i++;
      rozofs_thr_display_throughput_per_sec();
      continue;
    }

    if (strcasecmp(argv[i],"s")==0) {       
      i++;
      unit = rozofs_thr_unit_second;
      continue;
    }  

    if (strcasecmp(argv[i],"m")==0) {       
      i++;
      unit = rozofs_thr_unit_minute;
      continue;
    }      
    
    if (strcasecmp(argv[i],"h")==0) {       
      i++;
      unit = rozofs_thr_unit_hour;
      continue;
    }  

    if (strcasecmp(argv[i],"a")==0) {       
      i++;
      unit = rozofs_thr_unit_all;
      continue;
    }  
        
    pChar += rozofs_string_append(pChar,"\nunexpected parameter ");
    pChar += rozofs_string_append(pChar,argv[i]); 
    pChar += rozofs_eol(pChar); 
    man_io(pChar);   
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;    
  } 
       
  if (requested==0) {
    for (i=0; i< ROZOFSMOUNT_COUNTER_MAX; i++) {
      requested |= (1<<i);
    }  
    requested ^= excluded;
    /*
    ** ROZOFSMOUNT_COUNTER_WRITE_LATENCY_COUNT is needed to compute ROZOFSMOUNT_COUNTER_WRITE_LATENCY 
    ** but must not be displayed 
    */
    requested ^= (1<<ROZOFSMOUNT_COUNTER_WRITE_LATENCY_COUNT) ; 
  }
  
  pChar = rozofs_thr_display_bitmask(pChar, rozofs_thr_counter, requested, unit);
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
             
}
/*_______________________________________________________________________
* Initialize the thoughput measurement service
*
*/
void rozofs_throughput_counter_init(void) {
  rozofs_thr_cnts_t * counters[2];
    
  /*
  ** allocate memory for bandwidth computation
  */
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_READ_THR]= rozofs_thr_cnts_allocate(NULL, "Read B");
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_WRITE_THR]= rozofs_thr_cnts_allocate(NULL, "Write B");
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_READ_IO]= rozofs_thr_cnts_allocate(NULL, "Rd I/O");
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_WRITE_IO]= rozofs_thr_cnts_allocate(NULL, "Wr I/O");
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_READ_LATENCY]= rozofs_thr_average_allocate(NULL, "Rd us", ROZOFSMOUNT_COUNTER_READ_IO);
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_WRITE_LATENCY]= rozofs_thr_average_allocate(NULL, "Wr us", ROZOFSMOUNT_COUNTER_WRITE_LATENCY_COUNT);
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_WRITE_LATENCY_COUNT]= rozofs_thr_cnts_allocate(NULL, "XXXXXX");  
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_FCR8]= rozofs_thr_cnts_allocate(NULL, "F.CR8");
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_FDEL]= rozofs_thr_cnts_allocate(NULL, "F.DEL");
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_DCR8]= rozofs_thr_cnts_allocate(NULL, "D.CR8");
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_DDEL]= rozofs_thr_cnts_allocate(NULL, "D.DEL");
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_LS]= rozofs_thr_cnts_allocate(NULL, "LS");
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_LOOKUP]= rozofs_thr_cnts_allocate(NULL, "LOOKUP");
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_GETATTR]= rozofs_thr_cnts_allocate(NULL, "GETATTR");
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_XATTR]= rozofs_thr_cnts_allocate(NULL, "XATTR");
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_FLOCK]= rozofs_thr_cnts_allocate(NULL, "FLOCK");
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_LINK]= rozofs_thr_cnts_allocate(NULL, "LINK");
  rozofs_thr_counter[ROZOFSMOUNT_COUNTER_OTHER]= rozofs_thr_cnts_allocate(NULL, "OTHER");

  
  /*
  ** CRead and write must not exceed a threshold
  */
  counters[0] = rozofs_thr_counter[ROZOFSMOUNT_COUNTER_READ_THR];
  counters[1] = rozofs_thr_counter[ROZOFSMOUNT_COUNTER_WRITE_THR];  
  rozofs_thr_cnt_create_threshold(ROZOFSMOUNT_RWBW_THRESHOLD_IDX, conf.rwMaxBw*1000000, 2, counters);
  
  
  /*
  ** Register the diagnostic function
  */
  uma_dbg_addTopicAndMan("throughput", display_throughput,man_throughput,0); 
  uma_dbg_addTopicAndMan("io", display_io,man_io,0); 

}

/*
 *_______________________________________________________________________
 */

/**
 *  This function is the entry point for setting rozofs in non-blocking mode

   @param args->ch: reference of the fuse channnel
   @param args->se: reference of the fuse session
   @param args->max_transactions: max number of transactions that can be handled in parallel
   
   @retval -1 on error
   @retval : no retval -> only on fatal error

 */
int rozofs_stat_start(void *args) {

    uma_dbg_thread_add_self("Main");

    int ret; 
    //sem_t semForEver;    /* semaphore for blocking the main thread doing nothing */
    args_p = args;
    exportclt_t *exportclt_p = (exportclt_t*)args_p->exportclt;
    uint16_t debug_port = args_p->debug_port;
    uint16_t export_listening_port = (uint16_t)exportclt_p->listen_port;
    
    info("exportd listening port %d",export_listening_port);
    /*
    ** allocate memory for bandwidth computation
    */
    rozofs_throughput_counter_init();
   
    ret = ruc_init(FALSE, debug_port,export_listening_port);
    if (ret != RUC_OK) {
        /*
         ** fatal error
         */
        return -1;
    }

    
    /*
    ** Send the file lock reset request to remove old locks
    ** (NB This is not actually a low level fuse API....)
    */
    rozofs_ll_clear_client_file_lock(exportclt.eid,rozofs_client_hash);

    /*
     ** init of the fuse part
     */
    ret = rozofs_fuse_init(args_p->ch, args_p->se, args_p->max_transactions);
    if (ret != RUC_OK) {
        /*
         ** fatal error
         */
        return -1;
    }
    
#if 0
    NO MORE LBG MASTER. USE ONLY LBG EXPORTD.
    /*
     ** Perform the init with exportd--> setup of the TCP connection associated with the load balancing group
     */
    uint16_t export_nb_port = rozofs_get_service_port_export_master_eproto(); 
    if (export_lbg_initialize((exportclt_t*) args_p->exportclt, EXPORT_PROGRAM, EXPORT_VERSION, export_nb_port,
                               (af_stream_poll_CBK_t) rozofs_export_lbg_cnx_polling) != 0) {
        severe("Cannot setup the load balancing group towards Exportd");
    }
#endif    
    
    //#warning storcli instances are hardcoded
    if (storcli_lbg_initialize((exportclt_t*) args_p->exportclt,"rozofsmount", args_p->instance, 1, 2) != 0) {
        severe("Cannot setup the load balancing group towards StorCli");
    }
    
    rozofs_signals_declare("rozofsmount",  common_config.nb_core_file);
    

  /*
  **  change the priority of the main thread
  */
#if 1
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
      my_priority.sched_priority= 98;
      policy = SCHED_RR;
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
#endif  
   /*
   ** start the file KPI service
   */
   rzkpi_file_service_init();
   /*
   ** File caching init service
   */
   rzcachetrack_file_service_init();
    /*
    ** create the fuse threads
    */
    info("FDL RozoFs Instance %d",args_p->instance);
    ret = rozofs_fuse_thread_intf_create("localhost",args_p->instance,ROZOFS_MAX_FUSE_THREADS);
    if (ret < 0)
    {
       fatal("Cannot create fuse threads");
    }
    fuse_reply_thread_init();
    uma_dbg_addTopic("fuse_reply_thread", show_fuse_reply_thread);  
    /*
    ** init of the write error logging
    */    
    rozofs_iowr_err_init(args_p->instance);  
    
    ret = rozofs_shm_init( args_p->instance,1);
    if (ret < 0)
    {
       fatal("error while creating shared memory ressources");
    }  
    if (conf.batch)
    {
      /*
      ** init of the batch part of rozofsmount: this will depend on a mount parameter 
      */
      ret = rozofs_bt_init(args_p->instance,exportclt_p->eid,args_p->max_transactions);
      if (ret < 0)
      {
	 fatal("error while creating batch ressources");
      }
    }
    /*
    **  Indicates that the main thread is ready
    */
    rozofsmount_main_ready = 1;
        
    /*
     ** main loop
     */
    while (1) {
        ruc_sockCtrl_selectWait();
    }
    fatal( "main() code is rotten" );
}
