
/*
 *_______________________________________________________________________
 */
/**
*    export tracking file reader launcher pid file

   @param vid: volume identifier of the rebalancer
  
   @retval none
*/
static inline void export_expbt_pid_file(char * pidfile, int instance) {
  sprintf(pidfile,ROZOFS_RUNDIR_PID"launcher_expbt_inst_%d.pid",instance);
}
/*
 *_______________________________________________________________________
 */
/**
*   start an export tracking file reader

   @param vid: volume identifier of the rebalancer
   @param cfg: rebalancer configuration file name
  
   @retval none
*/
void export_start_one_expbt(int instance) {
  char cmd[1024];
  char pidfile[256];
  int  ret = -1;

  char *cmd_p = &cmd[0];
  cmd_p += sprintf(cmd_p, "%s ", "rozo_expbtd ");
  cmd_p += sprintf(cmd_p, "-i %d ", instance);
  cmd_p += sprintf(cmd_p, "-c %s ", exportd_config_file);

  export_expbt_pid_file(pidfile,instance);

  // Launch exportd slave
  ret = rozo_launcher_start(pidfile, cmd);
  if (ret !=0) {
    severe("rozo_launcher_start(%s,%s) %s",pidfile, cmd, strerror(errno));
    return;
  }

  info("start rozo_expbt:%d",instance);
}
