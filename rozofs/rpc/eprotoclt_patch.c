//
// #include "eprotoclt_patch.c" 
// epgw_mount_msite_ret_t *ep_mount_msite_1(epgw_mount_arg_t *argp, CLIENT *clnt){return ep_mount_msite_patch_1(argp, clnt);}
//
epgw_mount_msite_ret_t *
ep_mount_msite_patch_1(epgw_mount_arg_t *argp, CLIENT *clnt)
{
	static epgw_mount_msite_ret_t clnt_res;
        int    ret;
        int    i;

	memset((char *)&clnt_res, 0, sizeof(clnt_res));
	ret = clnt_call (clnt, EP_MOUNT_MSITE,
		(xdrproc_t) xdr_epgw_mount_arg_t, (caddr_t) argp,
		(xdrproc_t) xdr_epgw_mount_msite_ret_t, (caddr_t) &clnt_res,
		TIMEOUT);
        if (ret == 2) {
	  static epgw_mount_msite_ret_patch_t clnt_res_patch;
          memset((char *)&clnt_res_patch, 0, sizeof(clnt_res_patch));
	  ret = clnt_call (clnt, EP_MOUNT_MSITE,
		  (xdrproc_t) xdr_epgw_mount_arg_t, (caddr_t) argp,
		  (xdrproc_t) xdr_epgw_mount_msite_ret_patch_t, (caddr_t) &clnt_res_patch,
		  TIMEOUT);
          if (ret != RPC_SUCCESS) {
		return (NULL);
	  }  
          memcpy(&clnt_res, &clnt_res_patch, sizeof(clnt_res_patch));    
          for (i=0; i<clnt_res_patch.status_gw.ep_mount_msite_ret_patch_t_u.export.storage_nodes_nb;i++) {
          
            memcpy(&clnt_res.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes[i],
                   &clnt_res_patch.status_gw.ep_mount_msite_ret_patch_t_u.export.storage_nodes[i],
                   sizeof(clnt_res_patch.status_gw.ep_mount_msite_ret_patch_t_u.export.storage_nodes[i]));                     
            memcpy(&clnt_res.status_gw.ep_mount_msite_ret_t_u.export.storage_nodes[i].cids[0], 
                   &clnt_res_patch.status_gw.ep_mount_msite_ret_patch_t_u.export.storage_nodes[i].cids[0], 
                   sizeof(clnt_res_patch.status_gw.ep_mount_msite_ret_patch_t_u.export.storage_nodes[i].cids));
          }
        }
        
        if (ret != RPC_SUCCESS) {
		return (NULL);
	}
	return (&clnt_res);
}
