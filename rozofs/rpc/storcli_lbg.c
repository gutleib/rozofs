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



#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <errno.h>
#include <string.h>
#include <rpc/pmap_clnt.h>
#include <unistd.h>

#include <rozofs/core/rozofs_socket_family.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/core/ruc_common.h>
#include <rozofs/core/ruc_sockCtl_api.h>
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/core/north_lbg_api.h>
#include <rozofs/rpc/eclient.h>
#include "rpcclt.h"
#include "storcli_lbg_prototypes.h"

int storcli_lbg_id[STORCLI_PER_FSMOUNT];  /**< reference of the load balancing group to join storcli processes */


/**
* API to get the reference of the load balancing group associated with storcli processes
*
  @retval < 0 : the load balancing group does not exist
  @retval >= 0 : reference of the load balancing group
*/

int storcli_lbg_get_load_balancing_reference(int idx)
{
  if (idx >= STORCLI_PER_FSMOUNT) return -1;
  return storcli_lbg_id[idx];
}
int storcli_lbg_get_lbg_from_fid(fid_t fid)
{
  int idx = fid[0] & (STORCLI_PER_FSMOUNT - 1);
  return storcli_lbg_get_load_balancing_reference(idx);
}

int storcli_get_storcli_idx_from_fid(fid_t fid)
{
  int idx = fid[0] & (STORCLI_PER_FSMOUNT - 1);
  return idx;
}
 /**
 *  socket configuration for the family
 */
static af_unix_socket_conf_t  af_unix_storcli_conf =
{
  1,  //           family: identifier of the socket family    */
  0,         /**< instance number within the family   */
  sizeof(uint32_t),  /* headerSize  -> size of the header to read                 */
  0,                 /* msgLenOffset->  offset where the message length fits      */
  sizeof(uint32_t),  /* msgLenSize  -> size of the message length field in bytes  */
  
  (1024*256), /*  bufSize        -> length of buffer (xmit and received)        */
  (300*1024), /*  so_sendbufsize -> length of buffer (xmit and received)        */
  rozofs_tx_userRcvAllocBufCallBack, /*  userRcvAllocBufCallBack -> user callback for buffer allocation             */
  rozofs_tx_recv_rpc_cbk,            /*  userRcvCallBack         -> callback provided by the connection owner block */
  rozofs_tx_xmit_abort_rpc_cbk,      /*  userDiscCallBack        ->callBack for TCP disconnection detection         */
  NULL,                              /* userConnectCallBack     -> callback for client connection only              */
  NULL,  //    userXmitDoneCallBack; /**< optional call that must be set when the application when to be warned when packet has been sent */
  NULL,  //    userRcvReadyCallBack; /* NULL for default callback                    */
  NULL,  //    userXmitReadyCallBack; /* NULL for default callback                    */
  NULL,  //    userXmitEventCallBack; /* NULL for default callback                    */
  rozofs_tx_get_rpc_msg_len_cbk,        /* userHdrAnalyzerCallBack ->NULL by default, function that analyse the received header that returns the payload  length  */
  ROZOFS_RPC_SRV,       /* recv_srv_type ---> service type for reception : ROZOFS_RPC_SRV or ROZOFS_GENERIC_SRV  */
  0,       /*   rpc_recv_max_sz ----> max rpc reception buffer size : required for ROZOFS_RPC_SRV only */
  NULL,  //    *userRef;           /* user reference that must be recalled in the callbacks */
  NULL,  //    *xmitPool; /* user pool reference or -1 */
  NULL   //    *recvPool; /* user pool reference or -1 */
};

int storcli_lbg_initialize(exportclt_t *exportclt,char *owner,uint16_t rozofsmount_instance ,int first_instance,int nb_instances) 
{
    int status = 0;
    int lbg_size;
    int lbg_idx;
    
    DEBUG_FUNCTION;    
    char sunpath[AF_UNIX_SOCKET_NAME_SIZE];
    
    memset(storcli_lbg_id,-1, sizeof(storcli_lbg_id));
    
    for (lbg_idx=0; lbg_idx < STORCLI_PER_FSMOUNT; lbg_idx++) {
    
      sprintf(sunpath,"%s%s_%d.%d_lbg%d",ROZOFS_SOCK_FAMILY_STORCLI_NORTH_SUNPATH,owner,
                       exportclt->eid,rozofsmount_instance,first_instance+lbg_idx);


       /*
      ** store the IP address and port in the list of the endpoint
      */
      //lbg_size = nb_instances;
     lbg_size = 1;
    
     af_unix_storcli_conf.recv_srv_type = ROZOFS_RPC_SRV;
     af_unix_storcli_conf.rpc_recv_max_sz = rozofs_large_tx_recv_size;
     storcli_lbg_id[lbg_idx] = north_lbg_create_af_unix("STORCLI",sunpath,0, /* not significant */
                                                 first_instance,
                                                 lbg_size,&af_unix_storcli_conf);
     
     if (storcli_lbg_id[lbg_idx] >= 0)
     {
       /*
       ** Configure the LBG to keep message until the LBG comes back up in case of a disconnection
       ** and the LBG is down
       */
       north_lbg_rechain_when_lbg_gets_down(storcli_lbg_id[lbg_idx]);
     }
     else {     
       status = -1;
       severe("Cannot create Load Balancing Group %d for Storcli", lbg_idx);
     }
   }    
   return  status;
}     

