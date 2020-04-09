#!/bin/bash

#  Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
#  This file is part of Rozofs.
#  Rozofs is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published
#  by the Free Software Foundation, version 2.
#  Rozofs is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  General Public License for more details.

#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see
#  <http://www.gnu.org/licenses/>.

#
# setup.sh will generates a full working rozofs locally
# it's a useful tool for testing and debugging purposes. 
#
. env.sh 2> /dev/null

BS4K=0
BS8K=1
BS16K=2
BS32K=3
REBUILD_LOOP=2
    
COREDIR="/var/run/rozofs/core"
LOCAL_SIMU="${WORKING_DIR}/simulation"
# $1 is the site number
create_site() {
ROZOFS_SITE_PATH=/usr/local/etc/rozofs/

   mkdir -p $ROZOFS_SITE_PATH
   echo $1 > $ROZOFS_SITE_PATH/rozofs_site
}
# hid must be set
# Output storage conf file name
resolve_storage_conf_file() {
  STORAGE_CONF=${LOCAL_CONF}'storage_'${hid}'.conf' 
} 
# Input cid & hid must be set 
resolve_storage_path() {
  storage_path=${LOCAL_STORAGES_ROOT}_c${cid}_h${hid}
}  
# Input host number
# output gid, hid, cid
resolve_host_storage() {
    
  # Geo replication case
  hid=$1
  if [ $hid -gt $STORAGES_PER_SITE ]
  then
    gid=1
  else
    gid=0
  fi 
  vid=$(( ((hid-1)/STORAGES_BY_CLUSTER) + 1 ))  
  resolve_storage_conf_file  
}
# Input local cid number and host number
resolve_local_cid_hid() {
  localcid=$1
  hid=$2
 
  if [ $hid -gt $STORAGES_PER_SITE ]
  then
    gid=1
    basehost=$((hid-STORAGES_PER_SITE))
  else
    gid=0
    basehost=$hid
  fi 
  vid=$(( ((basehost-1)/STORAGES_BY_CLUSTER) + 1 ))
  cid=$(( ((vid-1)*NB_CLUSTERS_BY_VOLUME)+localcid ))
  sid=$(( (basehost-1)%STORAGES_BY_CLUSTER + 1 ))

  resolve_storage_path       
}
resolve_cid_hid() {
  cid=$1
  hid=$2
 
  if [ $hid -gt $STORAGES_PER_SITE ]
  then
    gid=1
    basehost=$((hid-STORAGES_PER_SITE))
  else
    gid=0
    basehost=$hid
  fi 
  vid=$(( ((basehost-1)/STORAGES_BY_CLUSTER) + 1 ))
  sid=$(( (basehost-1)%STORAGES_BY_CLUSTER + 1 ))
  
  resolve_storage_path  
  resolve_storage_conf_file       
}

process_killer () { 

  if ls /var/run/$1* > /dev/null 2>&1
  then
    for pid in `cat /var/run/$1* `
    do
      case "$pid" in
        0);;
        *) kill $pid  > /dev/null 2>&1 ;;
      esac	    
    done
  else
    return  
  fi

  #sleep 2
      
  if ls /var/run/$1* > /dev/null 2>&1
  then   
    for pid in `cat /var/run/$1* `
    do
      case "$pid" in
        0);;
	*) kill -9 $pid > /dev/null 2>&1 ;; 
      esac	
    done
  fi  
}   
build ()
{
    if [ ! -e "${LOCAL_SOURCE_DIR}" ]
    then
        echo "Unable to build RozoFS (${LOCAL_SOURCE_DIR} not exist)"
    fi

    if [ -e "${LOCAL_BUILD_DIR}" ]
    then
        rm -rf ${LOCAL_BUILD_DIR}
    fi

    mkdir ${LOCAL_BUILD_DIR}

    cd ${LOCAL_BUILD_DIR}
    rm -f ${LOCAL_SOURCE_DIR}/CMakeCache.txt
    cmake -G "Unix Makefiles" -DROZOFS_BIN_DIR=${ROZOFS_BIN_DIR} -DROZOFS_SHELL_DIR=${ROZOFS_SHELL_DIR}  -DDAEMON_PID_DIRECTORY=${BUILD_DIR} -DCMAKE_BUILD_TYPE=${LOCAL_CMAKE_BUILD_TYPE} ${LOCAL_SOURCE_DIR}
    make
    cd ..
    cp -r ${LOCAL_SOURCE_DIR}/tests/fs_ops/pjd-fstest/tests ${LOCAL_PJDTESTS}
}


rebuild ()
{
    if [ ! -e "${LOCAL_SOURCE_DIR}" ]
    then
        echo "Unable to build RozoFS (${LOCAL_SOURCE_DIR} not exist)"
    fi

    cd ${LOCAL_BUILD_DIR}
    make
    cd ..
    cp -r ${LOCAL_SOURCE_DIR}/tests/fs_ops/pjd-fstest/tests ${LOCAL_PJDTESTS}
}
create_device_files() {

  case "$DEVICE_SIZE_MB" in
    "") return;;
  esac

  for ((gid=0;  gid < ${GEOREP}; gid++)) 
  do
    for vid in $(seq ${NB_VOLUMES}); 
    do
      for sid in $(seq ${STORAGES_BY_CLUSTER}); 
      do
      
        #hid=$(( sid + ((vid-1)*STORAGES_BY_CLUSTER) + (gid*STORAGES_PER_SITE)  )) 
	   
	for idx in $(seq ${NB_CLUSTERS_BY_VOLUME}); do

	  cid=$(( idx + ((vid-1)*NB_CLUSTERS_BY_VOLUME) ))
	  dir="${LOCAL_SIMU}/site$((GEOREP-1))/cid${cid}/sid${sid}"	  
	  mkdir -p ${dir}
	  
	  for dev in $(seq ${NB_DEVICE_PER_SID})
	  do
	    if [ ! -f ${dir}/dev$((dev-1)) ];
	    then
	    
	      dd if=/dev/zero of=${dir}/dev$((dev-1)) bs=1MB count=$DEVICE_SIZE_MB
	      
	      for loop in {1..128}
	      do
	        losetup /dev/loop${loop} ${dir}/dev$((dev-1)) 2>/dev/null
		case "$?" in
		  0) break
		esac
	      done
	      
	      case "$loop" in
	        128) echo "Can not find /dev/loop for ${dir}/dev$((dev-1))"; break;;
	        *) {		  
	          mkfs.ext4 /dev/loop${loop}
	        };;
	      esac	
	      echo "/dev/loop${loop} ${dir}/dev$((dev-1))"
	    fi  
	  done
        done	  
      done
    done  
  done 
}
gen_storage_conf ()
{

    create_device_files
    if [ ! -e "$LOCAL_CONF" ]
    then
	mkdir -p $LOCAL_CONF
    fi

    for ((gid=0;  gid < ${GEOREP}; gid++)) 
    do
      for vid in $(seq ${NB_VOLUMES}); do
        for sid in $(seq ${STORAGES_BY_CLUSTER}); do
	
          hid=$(( sid + ((vid-1)*STORAGES_BY_CLUSTER) + (gid*STORAGES_PER_SITE)  )) 
          resolve_storage_conf_file
       
	  if [ -e "$STORAGE_CONF" ]
	  then
              rm -rf $STORAGE_CONF
	  fi

	  touch $STORAGE_CONF

	  echo "#${NAME_LABEL}" >> $STORAGE_CONF
	  echo "#${DATE_LABEL}" >> $STORAGE_CONF
	  	    	  
	  case "$DEV_SELF_HEALING" in
	    "") ;;
	    *) {
	      printf "self-healing = $DEV_SELF_HEALING;\n" >> $STORAGE_CONF
	      printf "export-hosts = \"${EXPORT_HOST}\";\n" >> $STORAGE_CONF
	    }  
	  esac 
	  
	  printf "listen = ( \n" >> $STORAGE_CONF
	  printf "  {addr = \"192.168.10.$hid\"; port = 41000;}" >> $STORAGE_CONF

	  # Test for special character "*"
	  #printf "  {addr = \"*\"; port = 4100$sid;}" >> $STORAGE_CONF

	  for idx in $(seq 2 1 ${NB_PORTS_PER_STORAGE_HOST}); do
              printf " ,\n  {addr = \"192.168.$((idx+9)).$hid\"; port = 41000;}"
	  done >>  $STORAGE_CONF

	  printf "\n);\n" >>  $STORAGE_CONF
	  echo 'storages = (' >> $STORAGE_CONF
	  
	  for idx in $(seq ${NB_CLUSTERS_BY_VOLUME}); do
	  
	    cid=$(( idx + ((vid-1)*NB_CLUSTERS_BY_VOLUME) ))
	    
	    resolve_storage_path
	    printf "  {cid = $cid; sid = $sid; root =\"${storage_path}\"; device-total = $NB_DEVICE_PER_SID; device-mapper = $NB_DEVICE_MAPPER_PER_SID; device-redundancy = $NB_DEVICE_MAPPER_RED_PER_SID;}"
	    if [ $idx -ne $NB_CLUSTERS_BY_VOLUME ];
	    then
	      printf ",\n"
	    else
	      printf "\n"
	    fi
	  done  >> $STORAGE_CONF
	  echo ');' >> $STORAGE_CONF
	done
      done
    done
}

reset_one_storio() 
{
    case "$1" in
      "") usage;;
    esac


    hid=$1	       
    # Resolve STORAGE_CONF as well as gid, hid, cid, sid   
    resolve_storage_conf_file $hid

    #echo "Start storage cid: $cid sid: $sid"
    for pid in `ps -ef | grep "storio" | grep -v rozolauncher | grep " -c $STORAGE_CONF" | grep " -i $2" | awk '{ print $2 }'`
    do
      kill -9 $pid
    done  
    #sleep 1
}
gen_export_conf ()
{

    ROZOFS_LAYOUT=$1

    FILE=${LOCAL_CONF}'export.conf'

    if [ ! -e "$LOCAL_CONF" ]
    then
        mkdir -p $LOCAL_CONF
    fi

    if [ -e "$FILE" ]
    then
        rm -rf $FILE
    fi

    touch $FILE
    echo "#${NAME_LABEL}" >> $FILE
    echo "#${DATE_LABEL}" >> $FILE
#    echo "layout = ${ROZOFS_LAYOUT} ;" >> $FILE
    echo "layout = 2 ;" >> $FILE
    echo 'volumes =' >> $FILE
    echo '      (' >> $FILE


    for v in $(seq ${NB_VOLUMES}); do

        echo '        {' >> $FILE
        echo "            vid = $v;" >> $FILE
	echo "            layout = $ROZOFS_LAYOUT;" >> $FILE
	
	if [ "$GEOREP" -ne 1 ];
	then
	  echo "            georep = True;" >> $FILE	  
	fi
        echo '            cids= ' >> $FILE
        echo '            (' >> $FILE
 
        for c in $(seq ${NB_CLUSTERS_BY_VOLUME}); do

            let idx_cluster=(${v}-1)*${NB_CLUSTERS_BY_VOLUME}+${c}
            let idx_host=(${v}-1)*${STORAGES_BY_CLUSTER}

            echo '                   {' >> $FILE
            echo "                       cid = $idx_cluster;" >> $FILE
            echo '                       sids =' >> $FILE
            echo '                       (' >> $FILE

            for sid in $(seq ${STORAGES_BY_CLUSTER}); do
	    
		idx_host=$((idx_host+1))                   

	       	if [ "$GEOREP" -ne 1 ];
		then
		    idx_geo=$((idx_host+STORAGES_PER_SITE))
                    if [[ ${sid} == ${STORAGES_BY_CLUSTER} ]]
                    then
                	echo "                           {sid = ${sid}; site0 = \"${LOCAL_STORAGE_NAME_BASE}${idx_host}\";site1 = \"${LOCAL_STORAGE_NAME_BASE}${idx_geo}\";}" >> $FILE
                    else
                	echo "                           {sid = ${sid}; site0 = \"${LOCAL_STORAGE_NAME_BASE}${idx_host}\";site1 = \"${LOCAL_STORAGE_NAME_BASE}${idx_geo}\";}," >> $FILE
                    fi
		else
                    if [[ ${sid} == ${STORAGES_BY_CLUSTER} ]]
                    then
                        echo "                           {sid = ${sid}; host = \"${LOCAL_STORAGE_NAME_BASE}${idx_host}\";}" >> $FILE
                    else
                        echo "                           {sid = ${sid}; host = \"${LOCAL_STORAGE_NAME_BASE}${idx_host}\";}," >> $FILE
                    fi		  
		fi    
            done;

            echo '                       );' >> $FILE
            if [[ ${c} == ${NB_CLUSTERS_BY_VOLUME} ]]
            then
                echo '                   }' >> $FILE
            else
                echo '                   },' >> $FILE
            fi
        done;
    echo '            );' >> $FILE
    if [[ ${v} == ${NB_VOLUMES} ]]
    then
        echo '        }' >> $FILE
    else
        echo '        },' >> $FILE
    fi
    done;
    echo '    )' >> $FILE
    echo ';' >> $FILE
    
    echo 'exports = (' >> $FILE

    eid=0
    for vid in $(seq ${NB_VOLUMES}); do        
      for k in $(seq ${NB_EXPORTS}); do
          eid=$((eid+1))
          if [[ ${k} == ${NB_EXPORTS} && ${vid} ==  ${NB_VOLUMES} ]]
          then
              echo "   {eid = $eid; bsize = ${EXPORT_BSIZE[k-1]};root = \"${LOCAL_EXPORTS_ROOT}_$eid\"; md5=\"${4}\"; squota=\"$SQUOTA\"; hquota=\"$HQUOTA\"; vid=${vid};}" >> $FILE
          else
              echo "   {eid = $eid; bsize = ${EXPORT_BSIZE[k-1]};root = \"${LOCAL_EXPORTS_ROOT}_$eid\"; md5=\"${4}\"; squota=\"$SQUOTA\"; hquota=\"$HQUOTA\"; vid=${vid};}," >> $FILE
          fi
      done;
    done  
    echo ');' >> $FILE
}
geomgr_modify ()
{
    FILE=${LOCAL_CONF}'geomgr.conf'


    # No personalized geomgr configuration file up to now
    if [ ! -f ${WORKING_DIR}/geomgr.conf ];
    then
    
      if [ ! -f $FILE ];
      then
        echo "$FILE does not exist  ! You should re-start rozo"
	exit
      fi	
      cp $FILE ${WORKING_DIR}/geomgr.conf
    fi  
    
    nedit ${WORKING_DIR}/geomgr.conf
    cp ${WORKING_DIR}/geomgr.conf $FILE
}
geomgr_delete ()
{   
    FILE=${LOCAL_CONF}'geomgr.conf'


    # No personalized geomgr configuration file up to now
    if [ -f ${WORKING_DIR}/geomgr.conf ];
    then
      rm ${WORKING_DIR}/geomgr.conf
    fi  
    
    gen_geomgr_conf
}
gen_geomgr_conf ()
{
    if [ "$GEOREP" -eq 1 ];
    then
      return
    fi  

    FILE=${LOCAL_CONF}'geomgr.conf'

    if [ ! -e "$LOCAL_CONF" ]
    then
        mkdir -p $LOCAL_CONF
    fi

    if [ -e "$FILE" ]
    then
        rm -rf $FILE
    fi
    
    # When there is a saved geomgr configuration
    # use it
    if [ -f ${WORKING_DIR}/geomgr.conf ];
    then
      echo "Use for geo-replication ${WORKING_DIR}/geomgr.conf"
      cp ${WORKING_DIR}/geomgr.conf $FILE
      return
    fi  

    echo "Generate $FILE"
    
    touch $FILE
    echo "#${NAME_LABEL}" >> $FILE
    echo "#${DATE_LABEL}" >> $FILE
    if [ "$GEOREP" -ne 1 ];
    then
      echo "active = True ;" >> $FILE
    else
      echo "active = False ;" >> $FILE
    fi 
    echo "export-daemons = (" >> $FILE
    echo "   {" >> $FILE
    echo "	active = True;" >> $FILE
    echo "	host   = \"${EXPORT_HOST}\";" >> $FILE
    echo "	exports="   >> $FILE
    echo "	("   >> $FILE
    for k in $(seq ${NB_EXPORTS}); do
      echo "          {" >> $FILE
      echo "               active = True;"  >> $FILE
      echo "               path   = \"${LOCAL_EXPORTS_ROOT}_$k\";" >> $FILE
      echo "               site   = 1;" >> $FILE
      echo "               nb     = 1;" >> $FILE
      echo "          }," >> $FILE
      echo "          {" >> $FILE
      echo "               active = True;"  >> $FILE
      echo "               path   = \"${LOCAL_EXPORTS_ROOT}_$k\";" >> $FILE
      echo "               site   = 0;" >> $FILE
      echo "               nb     = 1;" >> $FILE
      echo "               calendar =" >> $FILE
      echo "		   (" >> $FILE
      echo "		     { start=\"8:00\"; stop=\"12:15\";  },">> $FILE
      echo "		     { start=\"14:15\"; stop=\"17:30\"; }">> $FILE
      echo "		   );" >> $FILE
      if [[ ${k} == ${NB_EXPORTS} ]]
      then
        echo "          }" >> $FILE            
      else
        echo "          }," >> $FILE            
      fi
    done;
    echo "	);"   >> $FILE
    echo "   }" >> $FILE
    echo ');' >> $FILE
}
rebuild_storage_fid() 
{
    hid=$1
    # Resolve STORAGE_CONF as well as gid, hid, cid, sid   
    resolve_host_storage $hid
    shift 1

    for localcid in $(seq $NB_CLUSTERS_BY_VOLUME)
    do

      # Resolve the storage path from cid&hid 
      resolve_local_cid_hid $localcid $hid
      if [ ! -d ${storage_path} ];
      then
	echo "${storage_path} does not exist !!!" 
	continue        	  
      fi
      cmd="${LOCAL_BINARY_DIR}/storaged/${LOCAL_STORAGE_REBUILD} -c $STORAGE_CONF -H ${LOCAL_STORAGE_NAME_BASE}$hid -r ${EXPORT_HOST} -l $REBUILD_LOOP $*"
      echo $cmd
      $cmd
    done
}
# Input 
# 1 : host number
# 2 : cid
# 3 : device num or all
# 4&5 : -g site
rebuild_storage_device() 
{
  hid=$1
  cid=$2
  case "$cid" in
    "") usage;;
  esac  
  
  case "$3" in
    "")    dev="";;
    "all") dev="";;
    *)     dev="-d $3";;
  esac  
  d=$3
    
  shift 3
    
  resolve_cid_hid $cid $hid

  if [ ! -d ${storage_path} ];
  then
    echo "${storage_path} does not exist !!!" 
    return        	  
  fi

  create_storage_dev $hid $cid $d
  
  cmd="${LOCAL_BINARY_DIR}/storaged/${LOCAL_STORAGE_REBUILD} -c $STORAGE_CONF -H ${LOCAL_STORAGE_NAME_BASE}$hid -r ${EXPORT_HOST} -l $REBUILD_LOOP --sid $cid/$sid $dev $*"
  echo $cmd
  $cmd  
  exit $?
}
# Input 
# 1 : host number
# 2 : cid
# 3 : device num or all
# 4&5 : -g site
clear_storage_device() 
{
  hid=$1
  cid=$2
  case "$cid" in
    "") usage;;
  esac  
  
  case "$3" in
    "")    dev="";;
    "all") dev="";;
    *)     dev="-d $3";;
  esac  
  shift 3  
    
  resolve_cid_hid $cid $hid

  if [ ! -d ${storage_path} ];
  then
    echo "${storage_path} does not exist !!!" 
    return        	  
  fi
  
  cmd="${LOCAL_BINARY_DIR}/storaged/${LOCAL_STORAGE_REBUILD} -c $STORAGE_CONF -H ${LOCAL_STORAGE_NAME_BASE}$hid -r ${EXPORT_HOST}  --clear --sid $cid/$sid $dev $*"
  echo $cmd
  $cmd  
  exit $?
}
# Input 
# 1 : host number
# 2 : cid
# 3 : device num or all
# 4&5 : -g site
relocate_storage_device() 
{
  hid=$1
  cid=$2
  
  case "$3" in
    "")    usage;;
    "all") usage;;
    *)     dev=$3;;
  esac  
  
  shift 3

  resolve_cid_hid $cid $hid

  if [ ! -d ${storage_path} ];
  then
    echo "${storage_path} does not exist !!!" 
    return        	  
  fi

  # delete device and wait 5 seconds for the storio to take it into account
  delete_storage_device $hid $cid $dev
  sleep 5
  
  cmd="${LOCAL_BINARY_DIR}/storaged/${LOCAL_STORAGE_REBUILD} -c $STORAGE_CONF -H ${LOCAL_STORAGE_NAME_BASE}$hid -r ${EXPORT_HOST} -l $REBUILD_LOOP --sid $cid/$sid -d $dev -R $*"
  echo $cmd
  $cmd
  exit $?  
}
# Input 
# 1 : host number
# 2&3 : -g site
rebuild_storage() 
{
  resolve_host_storage $1
  shift 1
    
  for cid in $(seq ${NB_CLUSTERS_BY_VOLUME})
  do
      cid=$((cid + ((vid-1)*NB_CLUSTERS_BY_VOLUME) ))
      resolve_storage_path
      create_storage_dev $hid $cid	  
  done
  
  cmd="${LOCAL_BINARY_DIR}/storaged/${LOCAL_STORAGE_REBUILD} -c $STORAGE_CONF -H ${LOCAL_STORAGE_NAME_BASE}$hid -r ${EXPORT_HOST} -l $REBUILD_LOOP $*"
  echo $cmd
  $cmd
  exit $?
}
mount_storage_dev() {

  case "$DEVICE_SIZE_MB" in
    "") return;;
  esac
	
  hid=$1
  cid=$2
  dev=$3  
    
  resolve_cid_hid $cid $hid
  
  dir="${LOCAL_SIMU}/site${gid}/cid${cid}/sid${sid}/dev${dev}"
  	  
  for loop in {1..128}
  do
  
    res=`losetup /dev/loop${loop} | grep "$dir"`
    
    case "$res" in
      "");;
      *) {
        echo "${storage_path}/$3 -> /dev/loop${loop} -> ${dir}"
        mount -t ext4 /dev/loop${loop} ${storage_path}/$3
	return
      };;
    esac
    
  done
  
  echo "No /dev/loop for $dir"
}
# Input 
# 1 : host number
# 2 : cid
# 3 : device num or all or ""
delete_storage_device() 
{
  hid=$1
  cid=$2
  dev=$3

  case "$dev" in
    "")   begin=0  ; end=$((NB_DEVICE_PER_SID-1));;
    all)  begin=0  ; end=$((NB_DEVICE_PER_SID-1));;
    *)    begin=$dev ; end=$dev;;
  esac
    
  resolve_cid_hid $cid $hid

  if [ ! -d ${storage_path} ];
  then
    echo "${storage_path} does not exist !!!" 
    return        	  
  fi

    
  for device in $(seq $begin $end)
  do
    umount ${storage_path}/$device 2>/dev/null
    \rm -rf ${storage_path}/$device 
  done   
}
# Input 
# 1 : host number
delete_storage() 
{
  resolve_host_storage $1

  for cid in $(seq ${NB_CLUSTERS_BY_VOLUME})
  do
      cid=$((cid + ((vid-1)*NB_CLUSTERS_BY_VOLUME) ))
      delete_storage_device $1 $cid
      resolve_storage_path
      \rm -rf $storage_path/*
      echo "\rm -rf $storage_path/"	  
  done
}
# Input 

# Input 
# 1 : directory name
# 2 : device num or all or ""
create_storage_dev() {	
	    
  hid=$1
  cid=$2
  case "$cid" in
    "") usage;;
  esac  
    
  resolve_cid_hid $cid $hid

    case "$3" in
      "")  begin=0  ; end=$((NB_DEVICE_PER_SID-1));;
      all) begin=0  ; end=$((NB_DEVICE_PER_SID-1));;
      *)   begin=$3 ; end=$3;;
    esac
    
    for device in $(seq $begin $end)
    do
      if [ ! -d ${storage_path}/${device} ];
      then
        mkdir ${storage_path}/${device} > /dev/null 2>&1
        case "$DEVICE_SIZE_MB" in
          "") ;;
	  *)  echo "" > ${storage_path}/${device}/X
        esac
      fi 
      
      mount_storage_dev $hid $cid $device
    done  
}
# 1 : host number
# 2 : cid
# 3 : device num or all or ""
create_storage_device()  {
  hid=$1
  cid=$2
  case "$cid" in
    "") usage;;
  esac  
    
  resolve_cid_hid $cid $hid

  if [ ! -d ${storage_path} ];
  then
    echo "${storage_path} does not exist !!!" 
    return        	  
  fi

  create_storage_dev $hid $cid $3
}
stop_one_storage () {
   case $1 in
     "all") stop_storaged; return;;
   esac
   rozolauncher stop /var/run/launcher_storaged_${LOCAL_STORAGE_NAME_BASE}$1.pid   
}   
start_one_storage() 
{
    case $1 in
	"all") start_storaged ${STORAGES_BY_CLUSTER} 0; return;;
    esac


    hid=$1	       
    # Resolve STORAGE_CONF as well as gid, hid, cid, sid   
    resolve_storage_conf_file

    #echo "Start storage cid: $cid sid: $sid"
    rozolauncher start /var/run/launcher_storaged_${LOCAL_STORAGE_NAME_BASE}$hid.pid  ${LOCAL_BINARY_DIR}/storaged/${LOCAL_STORAGE_DAEMON} -c $STORAGE_CONF -H ${LOCAL_STORAGE_NAME_BASE}$hid &
    #sleep 1
}
reset_one_storage () {
  stop_one_storage $1
  #sleep 1
  start_one_storage $1
}
# $1 = STORAGES_BY_CLUSTER
start_storaged ()
{
    STORAGES_BY_CLUSTER=$1
    
    echo "------------------------------------------------------"
    echo "Start ${LOCAL_STORAGE_DAEMON}"
    for hid in $(seq ${STORAGES_TOTAL}); do
        start_one_storage $hid 		  
    done	
}

stop_storaged()
{
   echo "------------------------------------------------------"
   echo "Stopping the storaged"
    
   for hid in $(seq ${STORAGES_TOTAL}); do
       stop_one_storage $hid
   done    
}
reload_storaged ()
{
    echo "------------------------------------------------------"
    echo "Reload ${LOCAL_STORAGE_DAEMON}"
    kill -1 `ps ax | grep ${LOCAL_STORAGE_DAEMON} | grep -v grep | awk '{print $1}'`
}
create_storages ()
{

    for ((gid=0;  gid < ${GEOREP}; gid++)) 
    do
      for vid in $(seq ${NB_VOLUMES}); do
        for sid in $(seq ${STORAGES_BY_CLUSTER}); do
          hid=$(( sid + ((vid-1)*STORAGES_BY_CLUSTER) + (gid*STORAGES_PER_SITE)  )) 
          resolve_storage_conf_file
	  	       
	  if [ ! -e "$STORAGE_CONF" ]
	  then
              echo "Missing configuration file $STORAGE_CONF"
	  fi
	  
	  for cid in $(seq ${NB_CLUSTERS_BY_VOLUME})
	  do

            cid=$((cid + ((vid-1)*NB_CLUSTERS_BY_VOLUME) ))
	    resolve_storage_path
            mkdir -p $storage_path
	    create_storage_dev $hid $cid	
          done
	  

	done
      done
    done	
}

deploy_clients_local ()
{
    echo "------------------------------------------------------"
    if [ ! -e "${LOCAL_CONF}${LOCAL_EXPORT_CONF_FILE}" ]
    then
        echo "Unable to mount RozoFS (configuration file doesn't exist)"
	return
    fi	

    EXPORT_IN_CONF=`grep eid ${LOCAL_CONF}${LOCAL_EXPORT_CONF_FILE} | wc -l`
    fs_instance=0
    geo_instance=0

    for eid in $(seq ${EXPORT_IN_CONF}); do
        
        for idx_client in $(seq ${ROZOFSMOUNT_CLIENT_NB_BY_EXPORT_FS}); do
	
	    for geo_site in $(seq ${GEOREP}); do
	    
        	mount_point=${LOCAL_MNT_ROOT}${eid}_${idx_client}_g$((geo_site-1))
        	mountpoint -q ${mount_point}


        	if [ "$?" -ne 0 ]
        	then

                    echo "__Mount RozoFS ${LOCAL_EXPORTS_NAME_PREFIX}_${eid} on ${mount_point}"

                    if [ ! -e "${mount_point}" ]
                    then
                	mkdir -p ${mount_point}
                    fi

                    option=" -o rozofsexporttimeout=24 -o rozofsstoragetimeout=4 -o rozofsstorclitimeout=11"
                    option="$option -o rozofsbufsize=$WRITE_FILE_BUFFERING_SIZE -o rozofsminreadsize=$READ_FILE_MINIMUM_SIZE" 
                    option="$option -o rozofsnbstorcli=$NB_STORCLI"
                    option="$option -o rozofsshaper=$SHAPER"
                    option="$option -o posixlock"
                    option="$option -o bsdlock"
                    option="$option -o rozofsrotate=3"		
		    option="$option -o site=$((geo_site-1))"	    
                    option="$option -o instance=$fs_instance"
                    fs_instance=$((fs_instance+1))
		    
                    cmd="${LOCAL_BINARY_DIR}/rozofsmount/${LOCAL_ROZOFS_CLIENT} -H ${EXPORT_HOST} -E ${LOCAL_EXPORTS_ROOT}_${eid} ${mount_point} ${option}"
                    echo $cmd
		    $cmd
        	else
                    echo "Unable to mount RozoFS (${mount_point} already mounted)"
        	fi
            done;
        done;
    done;
    
    # Start geocli manager
    cmd="rozolauncher start /var/run/launcher_geomgr.pid geomgr -c ${LOCAL_CONF}geomgr.conf -t 5"
    echo "__$cmd"
    $cmd &      

}
rozofsmount_kill_best_effort()
{
    #echo "------------------------------------------------------"
    echo "Killing rozofsmount and storcli in best effort mode"
    process_killer rozofsmount
}
geocli_kill_best_effort()
{
    echo "------------------------------------------------------"
    echo "Killing geomgr"
    rozolauncher stop /var/run/launcher_geomgr.pid
}
undeploy_clients_local ()
{
    geocli_kill_best_effort
    
    echo "------------------------------------------------------"
    if [ ! -e "${LOCAL_CONF}${LOCAL_EXPORT_CONF_FILE}" ]
        then
        echo "Unable to umount RozoFS (configuration file doesn't exist)"
	return
    fi	

    EXPORT_IN_CONF=`grep eid ${LOCAL_CONF}${LOCAL_EXPORT_CONF_FILE} | wc -l`

    for eid in $(seq ${EXPORT_IN_CONF}); do
        for idx_client in $(seq ${ROZOFSMOUNT_CLIENT_NB_BY_EXPORT_FS}); do

            mount_point0=${LOCAL_MNT_ROOT}${eid}_${idx_client}_g0
            mount_point1=${LOCAL_MNT_ROOT}${eid}_${idx_client}_g1

            echo "__Umount RozoFS ${LOCAL_MNT_PREFIX}${eid} instance ${idx_client}"

            umount $mount_point0
	    case $? in
	      0) ;;
	      *) umount -l $mount_point0;;
	    esac  
            rm -rf $mount_point0

            umount $mount_point1
	    case $? in
	      0) ;;
	      *) umount -l $mount_point1;;
	    esac  
            rm -rf $mount_point1		

        done

    done

    sleep 0.4
    rozofsmount_kill_best_effort
    
}
start_diag_srv ()
{
    echo "------------------------------------------------------"
    echo "Start diagnostic server"
    rozolauncher start /var/run/rozofs/pid/launcher_rozodiag.pid rozo_diag_srv &
}
stop_diag_srv ()
{
    echo "------------------------------------------------------"
    echo "Killing diagnostic server"
    rozolauncher stop /var/run/rozofs/pid/launcher_rozodiag.pid
}

start_exportd ()
{
    echo "------------------------------------------------------"
    echo "Start ${LOCAL_EXPORT_DAEMON}"
    ${LOCAL_BINARY_DIR}/exportd/${LOCAL_EXPORT_DAEMON} -c ${LOCAL_CONF}${LOCAL_EXPORT_CONF_FILE}
    
#    sleep 1
#    cmd=`ps -ef | grep exportd | grep export.conf | awk '{print $8" -pid "$2 }'`
#    ddd $cmd &
}

stop_exportd ()
{
    echo "------------------------------------------------------"
    echo "Killing exportd"
    process_killer exportd.pid 
}

reset_exportd () {
    stop_exportd
    start_exportd 
}

reload_exportd ()
{
    echo "------------------------------------------------------"
    PID=`ps ax | grep ${LOCAL_EXPORT_DAEMON} | grep -v grep | awk '{print $1}'`
    if [ "$PID" != "" ]
    then
        echo "Reload ${LOCAL_EXPORT_DAEMON} (PID: ${PID})"
        kill -1 $PID
    else
        echo "Unable to reload ${LOCAL_EXPORT_DAEMON} (not running)"
    fi
}

# $1 -> Nb. of exports
create_exports ()
{
    if [ ! -e "${LOCAL_CONF}${LOCAL_EXPORT_CONF_FILE}" ]
    then
        echo "Unable to create export directories (configuration file doesn't exist)"
	return
    fi
    
    EXPORT_IN_CONF=`grep eid ${LOCAL_CONF}${LOCAL_EXPORT_CONF_FILE} | wc -l`

    for eid in $(seq ${EXPORT_IN_CONF}); do
        if [ -e "${LOCAL_EXPORTS_ROOT}_${eid}" ]
        then
            rm -rf ${LOCAL_EXPORTS_ROOT}_${eid}/*
        else
            mkdir -p ${LOCAL_EXPORTS_ROOT}_${eid}
        fi
    done;
}

# $1 -> Nb. of exports
remove_exports ()
{
    if [ ! -e "${LOCAL_CONF}${LOCAL_EXPORT_CONF_FILE}" ]
    then
        echo "Unable to remove export directories (configuration file doesn't exist)"
	return
    fi
    
    EXPORT_IN_CONF=`grep eid ${LOCAL_CONF}${LOCAL_EXPORT_CONF_FILE} | wc -l`

    for eid in $(seq ${EXPORT_IN_CONF}); do

        if [ -e "${LOCAL_EXPORTS_ROOT}_${eid}" ]
        then
            rm -rf ${LOCAL_EXPORTS_ROOT}_${eid}
        fi
    done;
}

remove_config_files ()
{
    echo "------------------------------------------------------"
    echo "Remove configuration files"
    rm -rf $LOCAL_CONF
}

remove_all ()
{
    echo "------------------------------------------------------"
    echo "Remove configuration files, storage and exports directories"
    rm -rf $LOCAL_CONF
    # Export meta data
    rm -rf $LOCAL_EXPORTS_ROOT*      

    # Storage data
    case "$DEVICE_SIZE_MB" in
      "") {
           rm -rf $LOCAL_STORAGES_ROOT*
      };;  
      *) {
           umount ${LOCAL_STORAGES_ROOT}_*/*
           rm -rf ${LOCAL_STORAGES_ROOT}_*
	      
	   for loop in {1..128}
	   do
	     res=`losetup /dev/loop${loop} 2>/dev/null | grep ${LOCAL_SIMU}`
	     case "$res" in
	       "");;
	       *) losetup -d /dev/loop${loop};; 
	     esac
	   done
	   
           rm -rf ${LOCAL_SIMU}
      };;
    esac   
}

remove_build ()
{
    echo "------------------------------------------------------"
    echo "Remove build directory"
    rm -rf $LOCAL_BUILD_DIR
}

do_start_all_processes() {
     start_diag_srv     
     start_storaged ${STORAGES_BY_CLUSTER}
     #start_expgw
     start_exportd 1
     #sleep 5
     deploy_clients_local
     show_map     
}


do_pause() {
    undeploy_clients_local
    stop_storaged
    stop_exportd
    stop_diag_srv
    #stop_expgw
}

do_stop()
{
    do_pause
    remove_all
    #sleep 1
}

clean_all ()
{
    do_stop
    remove_build
}

get_bin_complete_name () {
  case "$1" in
  storaged|storio) bin=${LOCAL_BINARY_DIR}/storaged/$1;;
  export*)   bin=${LOCAL_BINARY_DIR}/exportd/exportd;;
  geomgr)     bin=${LOCAL_BINARY_DIR}/geocli/geomgr;;
  *)               bin=${LOCAL_BINARY_DIR}/$1/$1;;
  esac
}
do_listCore() {
  if [ -d $COREDIR ];
  then
    
    cd $COREDIR
    for dir in `ls `
    do
    
      get_bin_complete_name $dir

      for file in `ls $dir`
      do
        res=`ls -lh $dir/$file | awk '{print $5" "$6" "$7" "$8" "$9}'`
        if [ $dir/$file -nt $bin ];
	then
	  echo "(NEW) $res"
	else
	  echo "(OLD) $res"
	fi
      done
    done
    
  fi    
} 
do_removeCore() {
  shift 1
  if [ ! -d $COREDIR ];
  then
    return
  fi  
  cd $COREDIR
  
  case "$1" in
    all) {
      for dir in `ls `
      do
        for file in `ls $dir`
        do
          unlink $dir/$file
        done
      done
      return
    };;
  esac
    
  while [ ! -z "$1" ];
  do
    unlink $1
    shift 1
  done  
}   
do_debugCore () {
  name=`echo $1 | awk -F'/' '{ print $1}'`
  
  get_bin_complete_name $name
  ddd $bin -core $COREDIR/$1 &
}
do_core () 
{
  shift 1
  
  case "$1" in
  "")       do_listCore;;
  "remove") do_removeCore $*;;
  *)        do_debugCore $1;;
  esac      
}
do_cou() {
  shift 1
  while [ ! -z $1 ];
  do
    do_one_cou $1
    shift 1
  done
}
do_one_cou () 
{
  COUFILE=/tmp/.cou.$$

  
  case "$1" in
  "")       usage;;
  esac      


  if [ ! -e $1 ];
  then
    printf "%20s  does not exist !!!\n" $1
    return 
  fi

  attr -R -g rozofs $1 > $COUFILE
  if [ ! -s $COUFILE ];
  then
    printf "%20s is not a RozoFS object !!!\n" $1
    return 
  fi
  
  printf "\n ___________ $1 ___________\n"
  cat $COUFILE
  
  mode=`awk '{if ($1=="MODE") printf $3; }' $COUFILE`
 #  BSIZE=`awk '{if ($1=="BSIZE") printf $3; }' $COUFILE`
  
  fid=`awk '{if ($1=="FID") printf $3; }' $COUFILE`
  slice=`awk '{if ($1=="SLICE") printf $3; }' $COUFILE`
#  lay=`awk '{if ($1=="LAYOUT") printf $3; }' $COUFILE`
  dist=`awk '{if ($1=="STORAGE") printf $3; }' $COUFILE`
  cid=`awk '{if ($1=="CLUSTER") printf $3; }' $COUFILE`
  SID_LIST=`echo $dist | awk -F'-' '{ for (i=1;i<=NF;i++) print " "$i; }'`
  eid=`awk '{if ($1=="EID") printf $3; }' $COUFILE`
  vid=`awk '{if ($1=="VID") printf $3; }' $COUFILE`
  
  rm -f $COUFILE

  case $mode in
    DIRECTORY|SYMBOLIC) return;;
  esac

  if [ "$GEOREP" -ne 1 ];
  then
  printf "__________________Site 0\n"  
  fi
  
  # Header and bins files
  for sid in $SID_LIST
  do
    sid=`expr $sid + 0`
    hid=$(( ((sid-1)%STORAGES_BY_CLUSTER) + 1 + ((vid-1)*STORAGES_BY_CLUSTER) ))
    resolve_storage_path
    doSpace="Yes"
    for file in `find $storage_path -name "${fid}*"`
    do
      case $doSpace in
       Yes) printf "\n"; doSpace="No";;
      esac	
      size=`ls -l $file  | awk '{ printf $5 }'`
      printf "%10s %s\n" $size $file
    done
  done
  
  if [ "$GEOREP" -eq 1 ];
  then
    return
  fi
    
  printf "\n__________________Site 1\n"  
  for sid in $SID_LIST
  do
    sid=`expr $sid + $STORAGES_PER_SITE`
    hid=$(( ((sid-1)%STORAGES_BY_CLUSTER) + 1 + ((vid-1)*STORAGES_BY_CLUSTER) + STORAGES_PER_SITE))
    resolve_storage_path
    doSpace="Yes"
    for file in `find $storage_path -name "${fid}*"`
    do
      case $doSpace in
       Yes) printf "\n"; doSpace="No";;
      esac	
      size=`ls -l $file  | awk '{ printf $5 }'`
      printf "%10s %s\n" $size $file
    done
  done
   
}
do_monitor_cfg () 
{
  # Create monitor configuration file
  sid=0  
  for v in $(seq ${NB_VOLUMES}); 
  do
    echo "VOLUME ${EXPORT_HOST} $v"
    host=$((v-1))
    host=$((host*STORAGES_BY_CLUSTER))

    for j in $(seq ${STORAGES_BY_CLUSTER}); 
    do
      host=$((host+1))
      echo "STORAGE localhost$host"
    done
  done

  mount_instance=0
  EXPORT_IN_CONF=`grep eid ${LOCAL_CONF}${LOCAL_EXPORT_CONF_FILE} | wc -l`

  for eid in $(seq ${EXPORT_IN_CONF}); 
  do
    for idx_client in $(seq ${ROZOFSMOUNT_CLIENT_NB_BY_EXPORT_FS}); 
    do
      echo "FSMOUNT localhost $mount_instance"
      mount_instance=$((mount_instance+1))
    done
  done       
}
do_monitor () 
{
  case "$1" in
    "") delay="-t 10s";;
    *)  delay="-t $2";;
  esac  
  do_monitor_cfg > ${WORKING_DIR}/monitor.cfg
  ${WORKING_DIR}/monitor.py $delay -c ${WORKING_DIR}/monitor.cfg
  exit 0
}  
check_build ()
{

    if [ ! -e "${LOCAL_BINARY_DIR}/exportd/${LOCAL_EXPORT_DAEMON}" ]
    then
        echo "Daemons are not build !!! use $0 build"
        echo "${LOCAL_BINARY_DIR}/exportd/${LOCAL_EXPORT_DAEMON}"
        exit 0;
    fi

}

pjd_test()
{

    if [ ! -e "${LOCAL_CONF}${LOCAL_EXPORT_CONF_FILE}" ]
    then
        echo "Unable to run pjd tests (configuration file doesn't exist)"
    else
        EXPORT_IN_CONF=`grep eid ${LOCAL_CONF}${LOCAL_EXPORT_CONF_FILE} | wc -l`
        EXPORT_LAYOUT=`grep layout ${LOCAL_CONF}${LOCAL_EXPORT_CONF_FILE} | grep -v grep | cut -c 10`

        for eid in $(seq ${EXPORT_IN_CONF}); do
            echo "------------------------------------------------------"
            mountpoint -q ${LOCAL_MNT_ROOT}${eid}
            if [ "$?" -eq 0 ]
            then
                echo "Run pjd tests on ${LOCAL_MNT_PREFIX}${eid} with layout $EXPORT_LAYOUT"
                echo "------------------------------------------------------"

                cd ${LOCAL_MNT_ROOT}${eid}
                prove -r ${LOCAL_PJDTESTS}
                cd ..

            else
                echo "Unable to run pjd tests (${LOCAL_MNT_PREFIX}${eid} is not mounted)"
            fi
        done;
    fi
}

fileop_test(){

    if [ ! -e "${LOCAL_CONF}${LOCAL_EXPORT_CONF_FILE}" ]
    then
        echo "Unable to run pjd tests (configuration file doesn't exist)"
    else
        LOWER_LMT=1
        UPPER_LMT=4
        INCREMENT=1
        FILE_SIZE=2M

        EXPORT_IN_CONF=`grep eid ${LOCAL_CONF}${LOCAL_EXPORT_CONF_FILE} | wc -l`
        EXPORT_LAYOUT=`grep layout ${LOCAL_CONF}${LOCAL_EXPORT_CONF_FILE} | grep -v grep | cut -c 10`

        for eid in $(seq ${EXPORT_IN_CONF}); do
                echo "------------------------------------------------------"
                mountpoint -q ${LOCAL_MNT_ROOT}${eid}
                if [ "$?" -eq 0 ]
                then
                    echo "Run fileop test on ${LOCAL_MNT_PREFIX}${eid} with layout $EXPORT_LAYOUT"
                    echo "------------------------------------------------------"
                    ${FSOP_BINARY} -l ${LOWER_LMT} -u ${UPPER_LMT} -i ${INCREMENT} -e -s ${FILE_SIZE} -d ${LOCAL_MNT_ROOT}${eid}
                else
                    echo "Unable to run fileop test (${LOCAL_MNT_PREFIX}${eid} is not mounted)"
                fi
        done;
    fi
}
map_separator_line() {
  printf "______"
  for sid in $(seq ${STORAGES_BY_CLUSTER})
  do
    printf ${1}"_____"
  done
  printf "\n"   
}
show_map() {
  eid=0
  for vid in $(seq ${NB_VOLUMES})
  do
    printf "\n"
    map_separator_line "_"
    printf "  Volume %d\n" $vid
    map_separator_line "_"

    for k in $(seq ${NB_EXPORTS}); do
      eid=$((eid+1))
      bsize=${EXPORT_BSIZE[k-1]}
      for idx_client in $(seq ${ROZOFSMOUNT_CLIENT_NB_BY_EXPORT_FS})
      do
	for geo_site in $(seq ${GEOREP})
	do
          mount_point=${LOCAL_MNT_ROOT}${eid}_${idx_client}_g$((geo_site-1))
	  printf "%-20s eid %d %dK site %d\n" $mount_point ${eid} $(( (bsize+1)*4)) $((geo_site-1)) 
        done
      done 
    done
    map_separator_line "_"
         
    printf "site0 "
    for sid in $(seq ${STORAGES_BY_CLUSTER})
    do
       hid=$(( ((vid-1)*STORAGES_BY_CLUSTER) + sid ))
       printf "| h%-2d "  $hid
    done
    printf "\n"
    if [ $GEOREP -ne 1 ];
    then    
      printf "site1 "
      for sid in $(seq ${STORAGES_BY_CLUSTER})
      do
	 hid=$(( ((vid-1)*STORAGES_BY_CLUSTER) + STORAGES_PER_SITE + sid ))
	 printf "| h%-2d "  $hid
      done
      printf "\n"      
    fi

    map_separator_line "|" 
    
    for loccid in $(seq ${NB_CLUSTERS_BY_VOLUME})
    do
       cid=$(( ((vid-1)*NB_CLUSTERS_BY_VOLUME) + loccid ))
       printf " c%-2d  " $cid       
       for sid in $(seq ${STORAGES_BY_CLUSTER})
       do
	 printf "| s%-2d " $sid
       done
       printf "\n" 
    done
    
    map_separator_line "|"     
  done

}
usage ()
{
    case $1 in
      "");;
      *) {
        echo "!!! $1 !!!"
      };;
    esac
    
    echo >&2 "Usage:"
    echo >&2 "$0 site <0|1>"
    echo >&2 "$0 start <layout>"
    echo >&2 "$0 stop"
    echo >&2 "$0 pause"
    echo >&2 "$0 resume"
    echo >&2 "$0 storage <hid>|all stop|start|reset"
    echo >&2 "$0 storage <hid>|all device-delete|device-create <cid> <device>|all"  
    echo >&2 "$0 storage <hid>|all device-rebuild|device-relocate|device-clear <cid> <device>|all"      
    echo >&2 "$0 storage <hid> delete|rebuild"
    echo >&2 "$0 storage <hid> fid-rebuild -s <cid>/<sid> -f <fid>"
    echo >&2 "$0 export stop|start|reset"
    echo >&2 "$0 fsmount stop|start|reset"
    echo >&2 "$0 geomgr modify|delete"    
    echo >&2 "$0 cou <fileName>"    
    echo >&2 "$0 core [remove] <coredir>/<corefile>"
    echo >&2 "$0 process [ <filter> ]"
    echo >&2 "$0 monitor"
    echo >&2 "$0 reload"
    echo >&2 "$0 build"
    echo >&2 "$0 rebuild"
    echo >&2 "$0 clean"
    echo >&2 "$0 pjd_test"
    echo >&2 "$0 fileop_test"
    echo >&2 "$0 mount"
    echo >&2 "$0 umount"
    exit 0
}


display_process() {
  local header=$2
  local proc=$1
  local next=$3
  local last=0
  local idx=0
  local LIST=""
  
  LIST=`ps -ef | awk '{ if ($3==proc) print $2 ; }' proc=$proc`
  last=0
  for pid in $LIST
  do 
    last=$((last+1))
  done   
  Details=`ps -ef | awk '{ if ($2==proc) print $8" "$9" "$10" "$11" "$12" "$13" "$14" "$15" "$16" "$17; }' proc=$proc`  

  printf "%5d %s|__%s\n" $proc "$header" "$Details"
  
  idx=0
  for pid in `ps -ef | awk '{ if ($3==proc) print $2 ; }' proc=$proc`
  do 
    idx=$((idx+1))
    if [ $idx -eq $last ];
    then
      display_process $pid "${header}$next" "   "
    else
      display_process $pid "${header}$next" "|  "
    fi
  done  
}	
show_process () {

  sleep 0.4
  
  tst_dir=`pwd | awk -F'/' '{ print $NF }'`
  case "$1" in
    "") LIST=`ps -ef | grep "/$tst_dir" | awk '{ if ($3==1) print $2;}'`;;
    *)  LIST=`ps -ef | grep "/$tst_dir" | grep "$1" | awk '{ if ($3==1) print $2;}'`;;
  esac

  for proc in $LIST
  do
    echo "_______________________________________________________________________________________"
    display_process $proc "" "   "
  done
}
main ()
{
    [ $# -lt 1 ] && usage

    #____________________________________________________
    # C O N F I G U R A T I O N
    #____________________________________________________    
    # Read either personal configuration file (setup.cfg)
    # or default configuration file (setup.default.cfg
    if [ -f setup.cfg ];
    then
      . setup.cfg  
    else
      if [ -f setup.default.cfg ];
      then
        . setup.default.cfg
      else
        echo "!!!"
        echo "!!! Neither setup.cfg nor setup.default.cfg exist !!!"
	echo "!!!"
	exit 1
      fi
    fi
        

    # Modify path to reach every generated executable
    for dir in ${LOCAL_BUILD_DIR}/src/*
    do
      if [ -d $dir ];
      then
        export PATH=$PATH:$dir
      fi
    done  

    
    # Storage per cluster depends on layout
    case "$ROZOFS_LAYOUT" in
      0) {
	STORAGES_BY_CLUSTER=4
      };;
      1) {        
	STORAGES_BY_CLUSTER=8
      };;   
      2) {
	STORAGES_BY_CLUSTER=16
      };;
      *) {
	echo >&2 "Rozofs layout must be equal to 0, 1 or 2."
	exit 1
      };
    esac      


    STORAGES_PER_SITE=$((NB_VOLUMES*STORAGES_BY_CLUSTER))
    SID_PER_SITE=$((STORAGES_PER_SITE*NB_CLUSTERS_BY_VOLUME))
    STORAGES_TOTAL=$((STORAGES_PER_SITE*GEOREP))
    SID_TOTAL=$((SID_PER_SITE*GEOREP))
    
    #READ_FILE_MINIMUM_SIZE=8
    READ_FILE_MINIMUM_SIZE=$WRITE_FILE_BUFFERING_SIZE
    
    EXPORT_HOST="${LOCAL_EXPORT_NAME_BASE}/192.168.36.15"

    ulimit -c unlimited
    ${WORKING_DIR}/conf_local_addr.sh set $STORAGES_TOTAL lo > /dev/null 2>&1 

    if [ "$1" == "site" ]
    then    
        [ $# -lt 2 ] && usage
        create_site $2   
	
    elif [ "$1" == "start" ]
    then
        check_build
        do_stop

        gen_storage_conf 
        gen_export_conf ${ROZOFS_LAYOUT} ${STORAGES_BY_CLUSTER}
        gen_geomgr_conf
	
        create_storages
        create_exports

        do_start_all_processes

    elif [ "$1" == "stop" ]
    then
           do_stop
           show_process
	   
    elif [ "$1" == "core" ]
    then
           do_core $*	
    elif [ "$1" == "cou" ]
    then
           do_cou $*	      
    elif [ "$1" == "map" ]
    then
           show_map 	
    elif [ "$1" == "pause" ]
    then
           do_pause
	   show_process
	   
    elif [ "$1" == "resume" ]
    then
           do_start_all_processes

    elif [ "$1" == "reload" ]
    then

        undeploy_clients_local

        reload_storaged
        reload_exportd

        deploy_clients_local

    elif [ "$1" == "pjd_test" ]
    then
        check_build
        pjd_test
    elif [ "$1" == "fileop_test" ]
    then
        check_build
        fileop_test

    elif [ "$1" == "mount" ]
    then
        check_build
        deploy_clients_local

    elif [ "$1" == "umount" ]
    then
        check_build
        undeploy_clients_local

    elif [ "$1" == "build" ]
    then
        build

    elif [ "$1" == "rebuild" ]
    then
        rebuild
    elif [ "$1" == "export" ]
    then
      case "$2" in 
        stop)       stop_exportd;;
	start)      start_exportd;;
	reset)      reset_exportd;;	
        *)          usage;;
      esac
    elif [ "$1" == "fsmount" ]
    then
      case "$2" in 
        stop)       undeploy_clients_local;;
	start)      deploy_clients_local;;
	reset)      undeploy_clients_local;deploy_clients_local;;	
        *)          usage;;
      esac      
    elif [ "$1" == "geomgr" ]
    then
      case "$2" in 
        modify)     geomgr_modify ;;
	delete)     geomgr_delete;;	
        *)          usage;;
      esac      
    elif [ "$1" == "storage" ]
    then 
    
      stid=$2
      action=$3
      shift 3

      case "$action" in 
        # storage hid stop
        stop)            stop_one_storage ${stid};;
	# storage hid start
	start)           start_one_storage ${stid};;
	# storage hid device-relocate cid device -g site
	device-relocate) relocate_storage_device ${stid} $*;;	
	# storage hid device-rebuild cid device -g site
	device-rebuild)  rebuild_storage_device ${stid} $*;;
	# storage hid device-rebuild cid device -g site
	device-clear)    clear_storage_device ${stid} $*;;	
	# storage hid device-delete cid device
	device-delete)   delete_storage_device ${stid} $*;; 
	device-create)   create_storage_device ${stid} $*;; 
	# storage hid delete
	delete)          delete_storage ${stid};;	
	# storage hid rebuild -g site
	rebuild)         rebuild_storage ${stid} $*;;
        fid-rebuild)     rebuild_storage_fid ${stid} $*;;
	reset)           reset_one_storage ${stid};;
        *)               usage;;
      esac
    elif [ "$1" == "storio" ]
    then  	
      case "$3" in 
        # storio hid reset [instance]
        reset)           reset_one_storio $2 $4;;
        *)               usage;;
      esac      
    elif [ "$1" == "process" ]
    then 
       show_process $2
    elif [ "$1" == "monitor" ]
    then 
       do_monitor $2       
    elif [ "$1" == "clean" ]
    then
        clean_all
	show_process
    else
        usage
    fi
    exit 0
}

main $@
