#!/bin/bash
# Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
# This file is part of Rozofs.
#
# Rozofs is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published
# by the Free Software Foundation, version 2.
#
# Rozofs is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see
# <http://www.gnu.org/licenses/>.

#set -x

PING="ping -q -W 2 -c1 "
#_________________________________________________
convertsecs() {
 ((h=${1}/3600))
 ((m=(${1}%3600)/60))
 ((s=${1}%60))
 printf "%02d:%02d:%02d" $h $m $s
}
#_________________________________________________
# Read a configuration file element
#_________________________________________________
read_cfg() {
  if [ "$1" == "" ]
  then
    echo "read_cfg without parameter"
    exit 1
  fi  
  val=`awk '{if ($1==element) print $3;}' element=$1 ${CFG}`
  if [ "$val" == "" -a "$2" != "" ];
  then
    echo $2
  else
    echo ${val}
  fi     
}
#_________________________________________________
# Check whether every input directory has been processed
# succesfully. Output the failed or not processed directories
#
# Example of the log.parallel
#23	rozofs-demo-05	1527580422.155	     0.348	0	705	0	0	/mnt/private/rsync/eid1/process_synchro.sh ./test_rsync/dir3
#24	rozofs-demo-05	1527580422.523	     0.345	0	705	0	0	/mnt/private/rsync/eid1/process_synchro.sh ./test_rsync/dir4
#25	rozofs-demo-05	1527580422.889	     0.351	0	716	255	0	/mnt/private/rsync/eid1/process_synchro.sh ./dir1/dir2
#_________________________________________________
build_result_file() {

  DELAY=`convertsecs ${SECONDS}`
  
  printf "      , \"LAST SYNCHRONIZATION\"\t: {\n"                                              >  ${TMPRESULTS}
  printf "          \"DURATION\"\t: \"%s\",\n"  ${DELAY}                                        >> ${TMPRESULTS} 
  
  if [ -f ${RSYNCLOG}.update ]
  then
    printf "          \"DIRECTORY LEVEL RSYNC\" : {\n" 
    printf "              \"SUCCESS\" \t: %s,\n" $((total_success)) 
    printf "              \"FAILED\"\t\t: %s,\n"  $((total_failed)) 
    printf "              \"REMAINING\"\t: %s,\n" $((total_remaining)) 
    printf "              \"TOTAL\"\t\t: %s,\n"    $((total_success+total_failed+total_remaining))
    #
    # File stats
    #
    awk  '
    BEGIN {
      cr=0;
      del=0;
      snd=0;
      rcv=0;
    } 
    { 
       if (($1=="Number")&&($2=="of")&&($3=="created")&&($4=="files:")) {cr=cr+$5;} 
       if (($1=="Number")&&($2=="of")&&($3=="deleted")&&($4="files:")) {del=del+$5;} 
       if (($1=="Total")&&($2=="bytes")&&($3=="sent:")) {snd=snd+$4;} 
       if (($1=="Total")&&($2=="bytes")&&($3=="received:")) {rcv=rcv+$4;} 
    }
    END {
      printf("              \"CREATED INODES\"\t: %s,\n",cr);
      printf("              \"DELETED INODES\"\t: %s,\n",del);
      printf("              \"BYTES SENT\" \t: %s,\n",snd);
      printf("              \"BYTES RECV\" \t: %s\n",rcv);
    } ' ${RSYNCLOG}.update                                                                        
    printf "          },\n"                                                                  
  fi >>  ${TMPRESULTS} 

  if [ -f ${RSYNCLOG}.recursive  ]
  then
    printf "          \"DIRECTORY RECURSIVE RSYNC\" : {\n"                                                      
    printf "              \"SUCCESS\" \t: %s,\n" $((total_success_recursive))   
    printf "              \"FAILED\"\t\t: %s,\n"  $((total_failed_recursive))   
    printf "              \"REMAINING\"\t: %s,\n" $((total_remaining_recursive))   
    printf "              \"TOTAL\"\t\t: %s,\n"    $((total_success_recursive+total_failed_recursive+total_remaining_recursive)) 
    #
    # File stats
    #
    awk  '
    BEGIN {
      cr=0;
      del=0;
      snd=0;
      rcv=0;
    } 
    { 
       if (($1=="Number")&&($2=="of")&&($3=="created")&&($4=="files:")) {cr=cr+$5;} 
       if (($1=="Number")&&($2=="of")&&($3=="deleted")&&($4="files:")) {del=del+$5;} 
       if (($1=="Total")&&($2=="bytes")&&($3=="sent:")) {snd=snd+$4;} 
       if (($1=="Total")&&($2=="bytes")&&($3=="received:")) {rcv=rcv+$4;} 
    }
    END {
      printf("              \"CREATED INODES\"\t: %s,\n",cr);
      printf("              \"DELETED INODES\"\t: %s,\n",del);
      printf("              \"SENT BYTES\" \t: %s,\n",snd);
      printf("              \"RECV BYTES\" \t: %s\n",rcv);
    } ' ${RSYNCLOG}.recursive  
    printf "          }\n" 
  fi >> ${TMPRESULTS}
    
  printf "      }\n"  >> ${TMPRESULTS}

  cp ${TMPRESULTS} ${RESULTS}
}
#_________________________________________________
# Print rsync script
#_________________________________________________
display_rsync_file_recursive() {
  echo "#!/bin/bash"
  echo "status=0"
  echo "while [ ! -z \$1 ];"
  echo "do "
  
  # p preserve permission
  # l copy link as link
  # t preserve modification time
  # g preserve group
  # o preserve owner
  # D preserve device
  # A preserve ACL
  # X preserve extended attributes
  # E preserve executability
  # --sparse keep holes in file during synchro 
  myrsync="rsync $1 -e 'ssh -o StrictHostKeyChecking=no' --sockopts=SO_SNDBUF=1048576,SO_RCVBUF=1048576"
  if [ ${BW} != "0" ];
  then
    myrsync="${myrsync} --bwlimit $((BW*1024))"
  fi  
      
  echo "  if ! ${myrsync} --stats --sparse -d -rlptgoDAXE --delete-before --update ${MNT}/\$1/ --rsync-path=\"mkdir -p ${BASE_DST}/\$1 && ${myrsync} \" root@${TARGET}:${BASE_DST}/\$1"
  echo "  then"
  echo "    status=\$((status+1))"
  echo "  fi"
  echo "  shift 1"
  echo "done" 
  echo "exit \${status}"
}
#_________________________________________________
# Build rsync script
#_________________________________________________
build_rsync_file_recursive() {
  display_rsync_file_recursive -q > $1.sh
  chmod +x $1.sh
  display_rsync_file_recursive -vv > $1.verbose.sh
  chmod +x $1.verbose.sh  
}
#_________________________________________________
# Print rsync script
#_________________________________________________
display_rsync_file() {
  echo "#!/bin/bash"
  echo "status=0"
  echo "while [ ! -z \$1 ];"
  echo "do "
  
  # p preserve permission
  # l copy link as link
  # t preserve modification time
  # g preserve group
  # o preserve owner
  # D preserve device
  # A preserve ACL
  # X preserve extended attributes
  # E preserve executability
  # --sparse keep holes in file during synchro 
  myrsync="rsync $1 -e 'ssh -o StrictHostKeyChecking=no' --sockopts=SO_SNDBUF=1048576,SO_RCVBUF=1048576 "
  if [ ${BW} != "0" ];
  then
    myrsync="${myrsync} --bwlimit $((BW*1024))"
  fi  

  echo "  if ! ${myrsync} --stats --sparse -d -lptgoDAXE --delete-before --update ${MNT}/\$1/ --rsync-path=\"mkdir -p ${BASE_DST}/\$1 && ${myrsync} \" root@${TARGET}:${BASE_DST}/\$1"
  echo "  then"
  echo "    status=\$((status+1))"
  echo "  fi"
  echo "  shift 1"
  echo "done" 
  echo "exit \${status}"
}
#_________________________________________________
# Build rsync script
#_________________________________________________
build_rsync_file() {
  display_rsync_file > $1.sh
  chmod +x $1.sh
  display_rsync_file -vv > $1.verbose.sh
  chmod +x $1.verbose.sh
  
}
#_________________________________________________
# Build rsync script
#_________________________________________________
build_rsync_files() {
  build_rsync_file ${DIR}/process_synchro
  build_rsync_file_recursive ${DIR}/process_synchro_recursive
}
#_________________________________________________
# Check whether rsync script needs to be generated
#_________________________________________________
build_rsync_file_when_needed() {
  # Create the rsync script if it does not yet exist
  if [ ! -f ${DIR}/process_synchro.sh ];
  then
    build_rsync_files
    return
  fi 

  # Re-create the rsync script if configuration has changed
  if [  ${DIR}/process_synchro.sh -ot ${CFG} ];
  then
    build_rsync_files
    return
  fi

  # Re-create the rsync script if configuration has changed
  myshell=`which rozo_synchro.sh`
  if [ $? -eq 0 ]
  then
    if [  ${DIR}/process_synchro.sh -ot ${myshell} ];
    then
      build_rsync_files
    fi
  fi  
}
#_________________________________________________
# Build source host list 
#_________________________________________________
build_src_host_list() {
  SRC_HOST=""
  for target in `cat ${DIR}/../.nodefile`
  do
    ${PING} $target > /dev/null 2>&1
    if [ $? -eq 0 ];
    then 
      ssh $target "ls  > /dev/null 2>&1"
      if [ $? -eq 0 ];
      then         
        SRC_HOST="-S ${target} ${SRC_HOST}"
      fi  
    fi    
  done
}
#_________________________________________________
# Update the state 
# $1 the state
# $2 the cause (eventually)
#_________________________________________________
update_state() {

  # 
  # Compute dureation of last phase,
  # print it, go to next ligne and display new state
  #  
  if [ "$1" == "STARTING SYNCHRO" ]
  then
    printf "__RozoFS synchronization ${NAME}\n"
    SECONDS=0
    LASTTIME=0
  else 
    DELAY=$((SECONDS-LASTTIME))
    LASTTIME=${SECONDS}
    DELAY=`convertsecs ${DELAY}`
    printf "%s\n" ${DELAY}
  fi      
  printf "  $1 \t"
  
  # 
  # Write state file
  #
  if [ "$2" == "" ]
  then
    printf "STATE : %s\nCAUSE : NONE\n" "$1"
  else  
    printf "STATE : %s\nCAUSE : %s\n" "$1" "$2"
  fi > ${DIR}/state
      
  # 
  # When cause is given, execution is ended
  # Update history file
  #  
  if [ "$2" != "" ]
  then
    echo ""
    
    if [ "$2" == "NONE" ]
    then
      printf "%s\n" "$1"
    else
      printf "%s - %s\n" "$1" "$2" 
    fi >> ${TMPHISTORY}

    if [ "$2" == "Up to date" ] 
    then
       grep DURATION ${TMPRESULTS} | sed 's/[ ",\t{}]//g' | sed '/^$/d' >> ${TMPHISTORY}              
    else
      # Write results into history file
      if [ -f ${TMPRESULTS} ];
      then
        cat ${TMPRESULTS} | sed 's/[",\t{}]//g' | sed -e 's/^[ ]*//' |  sed '/^[[:space:]]*$/d' | sed 's/LAST SYNCHRONIZATION://g' |  sed 's/DIRECTORY/##  DIRECTORY/g' >> ${TMPHISTORY}    
      fi
    fi  
    #
    # Switch history files when full
    #
    cat ${TMPHISTORY} >> ${HISTORY}
    sz=`stat -c%s ${HISTORY}`
    if [ $sz -ge 524288 ];
    then
      mv ${HISTORY}.3 ${HISTORY}.4
      mv ${HISTORY}.2 ${HISTORY}.3
      mv ${HISTORY}.1 ${HISTORY}.2
      mv ${HISTORY} ${HISTORY}.1
    fi    
  fi
 
}
#_________________________________________________
#
# Append and sort some file lists
# $1 result file
# $2.... file to append
#_________________________________________________
file_append_and_sort() {

  result=$1
  shift 1

  #
  # Make the list of existing files in the rest of parameters
  #
  list=""
  while [ ! -z $1 ];
  do
    if [ -f $1 ];
    then
      list="$list $1"
    fi
    shift 1  
  done  
  
  cat ${list} | sort -u > ${result}
}
#_________________________________________________
#
# Remove from a list the names not satisfying a
# directory filter
# $1 input file 
# $2 output file
#_________________________________________________
do_filter_directories() {

  case "${DIRECTORY}" in
    "")  mv $1 $2;;
    "/") mv $1 $2;;
    *) {
          if [ "${DIRECTORY: -1}" == "/" ]
          then
            DIRECTORY="${DIRECTORY::-1}"
          fi      
          for dir in `cat $1`
          do
            if [[ ${dir} == .${DIRECTORY}/* ]]
            then
              echo ${dir}
            elif [[ ${dir} == .${DIRECTORY} ]]
            then
              echo ${dir}
            fi
          done > $2
    };;
  esac
}
#_________________________________________________
#
# Scan for directories that have been moved from 
# a given date
#
# @params
# $1 Result file to write the results of then scan in
#_________________________________________________
do_scan_moved() {

  result=$1
  shift 1

  #
  # Scan directories not in trash of the given eid that has an atime time more recent 
  # than the given one (PREVIOUS_TS)
  # Eventualy add a project number filter if any.
  #
  if [ "${PROJECT}" == "" ]
  then
    rozo_scan_by_criteria eid ${EID} dir notrash parent ge .${DIRECTORY} atime ge "${PREVIOUS_TS}" atime le "${CURRENT_TS}" cr8 le "${PREVIOUS_TS}" 
  else  
    rozo_scan_by_criteria eid ${EID} dir notrash parent ge .${DIRECTORY} atime ge "${PREVIOUS_TS}" atime le "${CURRENT_TS}" cr8 le "${PREVIOUS_TS}" project=${PROJECT}
  fi  > ${result}
  
  #
  # Scanning has failed for unknown reason
  #
  if [ $? -ne 0 ]
  then
    update_state "FAILED" "rozo_scan_by_criteria moved dir failed" 
    exit 1
  fi

  #
  # Apply sub-directory filter
  #
  #  do_filter_directories ${result}.tmp ${result} 
  #  rm -f ${result}.tmp
}
#_________________________________________________
#
# Scan for directories that have been updated from 
# a given date
#
# @params
# $1 Result file to write the results of then scan in
# $2 A file to append to the result file (previous FAILED)
#_________________________________________________
do_scan() {

  result=$1
  shift 1

  #
  # Scan directories not in trash of the given eid that has an update time more recent 
  # than the given one (PREVIOUS_TS)
  # Eventualy add a project number filter if any.
  #
  if [ "${PROJECT}" == "" ]
  then
    rozo_scan_by_criteria eid ${EID} dir skipdate notrash parent ge .${DIRECTORY} update ge "${PREVIOUS_TS}" update le "${CURRENT_TS}"
  else  
    rozo_scan_by_criteria eid ${EID} dir skipdate notrash parent ge .${DIRECTORY} update ge "${PREVIOUS_TS}" update le "${CURRENT_TS}" project=${PROJECT}
  fi  > ${result}
  
  #
  # Scanning has failed for unknown reason
  #
  if [ $? -ne 0 ]
  then
    update_state "FAILED" "rozo_scan_by_criteria failed" 
    exit 1
  fi

  #
  # Apply sub-directory filter
  #
  #do_filter_directories ${result}.tmp ${result} 
  #rm -f ${result}.tmp
}
#_________________________________________________
# Check that a directory still exist using attr command
#_________________________________________________
check_exist() {
  cd ${MNT}
  attr -g rozofs ${MNT}/$1 > /dev/null 2>&1
  if [ $? -eq 0 ]
  then
    echo $1
  fi
}
#_________________________________________________
# Purge a directory list from non existing directories
#_________________________________________________
remove_deleted_dir() {
  #
  # If file does not exist just return
  if [ ! -f $1 ]
  then
    return
  fi  
  
  truncate -s 0 /tmp/${DIRNAME}.delete.tmp  
  cd ${MNT}
  for f in `cat $1`
  do
    attr -g rozofs ${MNT}/$f > /dev/null 2>&1
    if [ $? -eq 0 ]
    then
      echo $f
    fi
  done > /tmp/${DIRNAME}.delete.tmp
  mv /tmp/${DIRNAME}.delete.tmp $1

}
#_________________________________________________
#
# Scan directorie to build a list of directories 
# to update by a non recursive rsync (${SCAN_RESULT})
# plus a list of directories to update by a recursive 
# rsync (${SCAN_RESULT}.recursive)
# output is 
#_________________________________________________
do_scan_directories() {
  #
  # Scanning command to get the list of directory that have an update time
  #
  do_scan ${SCAN_RESULT}.update.tmp
  if [ "${verbose}" == "-v" ]
  then
    printf "\n  ~~~~SCANNED~UPDATED~DIRS~~~~~~~~~~~~~~~~~~~~\n"
    cat ${SCAN_RESULT}.update.tmp
    printf "  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n"
  fi

  if [ "${verbose}" == "-v" ]
  then
    printf "  ~~~~REMAINING~UPDATED~DIRS~~~~~~~~~~~~~~~~~~\n"
    cat ${REMAINING}.update 2>/dev/null
    printf "  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n"
  fi  

  #
  # Remove deleted directories from the failed list in the meanwhile
  #
  remove_deleted_dir ${FAILED}.update
  if [ "${verbose}" == "-v" ]
  then
    printf "  ~~~~FAILED~UPDATED~DIRS~~~~~~~~~~~~~~~~~~~~~\n"
    cat ${FAILED}.update 2>/dev/null
    printf "  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n"
  fi  
  
  if [ "${verbose}" == "-v" ]
  then
    printf "  ~~~~REMAINING~RECURSIVE~DIRS~~~~~~~~~~~~~~~~\n"
    cat ${REMAINING}.recursive 2>/dev/null
    printf "  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n"
  fi  

  #
  # Remove deleted directories from the failed list in the meanwhile
  #
  remove_deleted_dir ${FAILED}.recursive
  if [ "${verbose}" == "-v" ]
  then
    printf "  ~~~~FAILED~RECURSIVE~DIRS~~~~~~~~~~~~~~~~~~~\n"
    cat ${FAILED}.recursive 2>/dev/null
    printf "  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n"
  fi 

  #
  # Scanning command to get the list of directory that have been moved
  #
  do_scan_moved ${SCAN_RESULT}.recursive.tmp
  if [ "${verbose}" == "-v" ]
  then
    printf "  ~~~~SCANNED~MOVED~DIRS~~~~~~~~~~~~~~~~~~~~~~\n"
    cat ${SCAN_RESULT}.recursive.tmp 2>/dev/null
    printf "  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n"
  fi

  #
  # Check we have enough directory in the recursive list
  # If not put these directories in the local rsync list
  # and add their sub-directories in the recursive list.
  #
  truncate -s 0 ${SCAN_RESULT}.moved2update
  nb=`cat ${SCAN_RESULT}.recursive.tmp | wc -l`
  round=5
  while [[ ${nb} -lt 32 && ${nb} -gt 0 && ${round} -gt 0 ]]
  do
    round=$((round-1))
    truncate -s 0 ${SCAN_RESULT}.new_recursive
    cd ${MNT}
    for dir in `cat ${SCAN_RESULT}.recursive.tmp`
    do
      ls -1 -d ${dir}/*/ >> ${SCAN_RESULT}.new_recursive 2>/dev/null
      echo ${dir} >> ${SCAN_RESULT}.moved2update
    done
    mv ${SCAN_RESULT}.new_recursive ${SCAN_RESULT}.recursive.tmp
    nb=`cat ${SCAN_RESULT}.recursive.tmp | wc -l`
  done  
  if [ "${verbose}" == "-v" ]
  then
    printf "  ~~~~MOVED2UPDATE-DIRS~~~~~~~~~~~~~~~~~~~~~~~\n"
    cat ${SCAN_RESULT}.moved2update 2>/dev/null
    printf "  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n"
    printf "  ~~~~RECURSIVE~SUBDIRS~~~~~~~~~~~~~~~~~~~~~~~${nb}\n"
    cat ${SCAN_RESULT}.recursive.tmp 2>/dev/null
    printf "  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n"
  fi

  file_append_and_sort ${SCAN_RESULT}.update ${SCAN_RESULT}.moved2update ${SCAN_RESULT}.update.tmp ${REMAINING}.update ${FAILED}.update
  file_append_and_sort ${SCAN_RESULT}.recursive ${SCAN_RESULT}.recursive.tmp ${REMAINING}.recursive ${FAILED}.recursive

  rm -f ${SCAN_RESULT}.recursive.tmp
  rm -f ${SCAN_RESULT}.new_recursive   
  rm -f ${SCAN_RESULT}.moved2update   
  rm -f ${SCAN_RESULT}.update.tmp
}  
#_________________________________________________
# Process to a dry run
# i.e diplay what would have to be synchronized
#_________________________________________________
dry_run() {
  #
  # Read the last synchro timestamp
  #
  if [ "$full" == "YES" ]
  then
    PREVIOUS_TS=${ORIG_TS}
    REMAINING="${REMAINING}.NOREMAINING"
    FAILED="${FAILED}.NOFAILED"
  else  
    if [ ! -f  ${TIMESTAMP} ];
    then
      PREVIOUS_TS=${ORIG_TS}
    else
      PREVIOUS_TS=`cat ${TIMESTAMP}`
    fi 
  fi  

  #
  # Scanning command to get the list of directory that have 
  # too be sync in a non recursive maner ${SCAN_RESULT}
  # plus a list of directories to be recursively synchronized
  # (${SCAN_RESULT}.recursive)
  #
  verbose="-v"
  do_scan_directories
  exit 0
  
}



           #_________________________________________________#
           #                   M A I N                       #
           #_________________________________________________#


ORIG_TS="2000-01-01-00:00:00"
#
# Read parameters
#
DIR=""
full="NO"
dry="NO"
verbose=""
while [ ! -z $1 ];
do
  case "$1" in 
    #
    # Directory name of the synchro
    #      
    "-d") {
       DIR=$2
       shift 2
    };;
    #
    # Verbose mode
    #
    "-v") verbose="-v"; shift 1;;
    #
    # debug mode
    #
    "-debug") {
      set -x
      verbose="-v"
      shift 1
    };;
    #
    # Full synchro
    #
    "-f") {
       full="YES"
       shift 1
    };;
    #
    # Dry run
    #    
    "-D") {
       dry="YES"
       shift 1
    };;    
    *) {
       echo "Unexpected parameter $1"
       shift 1
    };;
  esac
done


#
# Control that input directory is given and exists
#
if [ "${DIR}" == "" ]
then
  echo "Missing input directory"
  exit 1  
fi
if [ ! -d  ${DIR} ]
then
  echo "No such RozoFS synchronization directory"
  exit 1
fi

#
# Initialize file names
#
# remove last / if any
if [ "${DIR: -1}" == "/" ]
then
  DIR="${DIR::-1}"
fi  

#
# Build variables used by the shell sript
#
NAME=`basename ${DIR}`
DIRNAME=`echo $DIR | tr \/ _`

JOBLOG="/tmp/${DIRNAME}.log_gnu_parallel"
RSYNCLOG="/tmp/${DIRNAME}.log_rsync"
SRC_HOST=""
SCAN_RESULT="/tmp/${DIRNAME}.scan_result"
CFG="${DIR}/cfg"
TIMESTAMP="${DIR}/timestamp"

FAILED="${DIR}/failed_directories"
TMPFAILED="/tmp/${DIRNAME}.failed_directories"

REMAINING="${DIR}/remaining_directories"
TMPREMAINING="/tmp/${DIRNAME}.remaining_directories"

TMPRESULTS="/tmp/${DIRNAME}.results"
RESULTS="${DIR}/results"
HISTORY="${DIR}/history"
TMPHISTORY="/tmp/${DIRNAME}.history"

#
#  GNU parallel must be installed
#
which parallel > /dev/null
if [ $? -ne 0 ]
then
  update_state "FAILED" "GNU parallel is not installed" 
  exit 1
fi

#
# Parse configuration file
#
EID=`read_cfg EID`
PROJECT=`read_cfg PROJECT`
BW=`read_cfg BW 0`
MNT=`read_cfg MNT`
DIRECTORY=`read_cfg DIRECTORY`
TARGET=`read_cfg TARGET`
BASE_DST=`read_cfg BASE_DST`


rm -f /tmp/${DIRNAME}.*

total_success=0
total_failed=0
total_remaining=0
total_success_recursive=0
total_failed_recursive=0
total_remaining_recursive=0
#
# Create the rsync script if it needs to
#
build_rsync_file_when_needed

#sleep 8

CURRENT_TS=`date "+%Y-%m-%d-%H:%M:%S" --date "-1 sec"`

#
# Case of the dry run
#
if [ "${dry}" == "YES" ]
then
  dry_run
fi


update_state "STARTING SYNCHRO"

#
# Get current time stamp
#
printf "\n#   %s : " "${CURRENT_TS}" > ${TMPHISTORY}

#
# Full synchro : remove timestamp file
#
if [ "$full" == "YES" ]
then
  rm -f ${TIMESTAMP}
  rm -f ${FAILED}.update    
  rm -f ${REMAINING}.update
  rm -f ${FAILED}.recursive    
  rm -f ${REMAINING}.recursive   
fi

#
# Ping remote target
#
${PING} ${TARGET} > /dev/null
if [ $? -ne 0 ]
then
  update_state "FAILED" "Remote target ${TARGET} is not reachable"
  exit 1
fi


#
# Build list of node that will run parallel
#
build_src_host_list 
if [ "${SRC_HOST}" == "" ]
then
  update_state "FAILED" "No synchronization source host available"
  exit  1
fi

#
# Read the last synchro timestamp
#
if [ ! -f  ${TIMESTAMP} ];
then
  PREVIOUS_TS=${ORIG_TS}
  rm -f ${FAILED}.update
  rm -f ${REMAINING}.update
  rm -f ${FAILED}.recursive    
  rm -f ${REMAINING}.recursive   
else
  PREVIOUS_TS=`cat ${TIMESTAMP}`
fi

update_state "SCANNING DIRECTORIES" 

#
# Scanning command to get the list of directory that have 
# too be sync in a non recursive maner ${SCAN_RESULT}
# plus a list of directories to be recursively synchronized
# (${SCAN_RESULT}.recursive)
#
do_scan_directories
if [ ! -s ${SCAN_RESULT}.update -a ! -s ${SCAN_RESULT}.recursive ]
then
  build_result_file
  update_state "SUCCESS" "Up to date"
  #
  # Update time stamp
  #
  echo ${CURRENT_TS} > ${TIMESTAMP}
  
  rm -f /tmp/${DIRNAME}.*
  exit 0  
fi


#
# Run //
#
update_state "RUNNING PARALLEL"

status=0
if [ -s ${SCAN_RESULT}.update ]
then
  if [ "${verbose}" == "-v" ]
  then
    parallel -a ${SCAN_RESULT}.update -X -L 4 --joblog ${JOBLOG}.update ${SRC_HOST} --jobs 4 --retries 3 ${DIR}/process_synchro.verbose.sh {} 2>/dev/null | sed 's/,//g' >>  ${RSYNCLOG}.update
  else
    parallel -a ${SCAN_RESULT}.update -X -L 4 --joblog ${JOBLOG}.update ${SRC_HOST} --jobs 4 --retries 3 ${DIR}/process_synchro.sh {} 2>/dev/null | sed 's/,//g' >>  ${RSYNCLOG}.update
  fi
  status=$?
fi

status_recursive=0
if [ -s ${SCAN_RESULT}.recursive ]
then
  if [ "${verbose}" == "-v" ]
  then
    parallel -a ${SCAN_RESULT}.recursive -X -L 4 --joblog ${JOBLOG}.recursive ${SRC_HOST} --jobs 4 --retries 3 ${DIR}/process_synchro_recursive.verbose.sh {} 2>/dev/null | sed 's/,//g' >>  ${RSYNCLOG}.recursive
  else
    parallel -a ${SCAN_RESULT}.recursive -X -L 4 --joblog ${JOBLOG}.recursive ${SRC_HOST} --jobs 4 --retries 3 ${DIR}/process_synchro_recursive.sh {} 2>/dev/null | sed 's/,//g' >>  ${RSYNCLOG}.recursive
  fi
  status_recursive=$?
fi

#
# Analyze the results
#
update_state "ANALYZING RESULTS"

if [ -s ${SCAN_RESULT}.update ]
then
  res=`rozo_synchro_analyze_result ${DIR} ${SCAN_RESULT}.update ${JOBLOG}.update ${TMPFAILED}.update ${TMPREMAINING}.update`
  if [ $? -ne 0 ];
  then
    update_state "FAILED" "rozo_synchro_analyze_result update"
    exit 1
  fi 
  total_success=`echo ${res} | awk '{print $2;}'`
  total_failed=`echo ${res} | awk '{print $4;}'`
  total_remaining=`echo ${res} | awk '{print $6;}'`
  
  if [ -f ${TMPFAILED}.update ]
  then
    mv ${TMPFAILED}.update ${FAILED}.update
  else
    rm -f ${FAILED}.update 2>/dev/null 
  fi
  
  if [ -f ${TMPREMAINING}.update ]
  then    
    mv ${TMPREMAINING}.update ${REMAINING}.update
  else
    rm -f ${REMAINING}.update 2>/dev/null 
  fi  
fi

if [ -s ${SCAN_RESULT}.recursive ]
then
  res=`rozo_synchro_analyze_result ${DIR} ${SCAN_RESULT}.recursive ${JOBLOG}.recursive ${TMPFAILED}.recursive ${TMPREMAINING}.recursive`
  if [ $? -ne 0 ];
  then
    update_state "FAILED" "rozo_synchro_analyze_result recursive"
    exit 1
  fi 
  total_success_recursive=`echo ${res} | awk '{print $2;}'`
  total_failed_recursive=`echo ${res} | awk '{print $4;}'`
  total_remaining_recursive=`echo ${res} | awk '{print $6;}'`

  if [ -f ${TMPFAILED}.recursive ]
  then
    mv ${TMPFAILED}.recursive ${FAILED}.recursive
  else
    rm -f ${FAILED}.recursive 2>/dev/null 
  fi

  if [ -f ${TMPREMAINING}.recursive ]
  then    
    mv ${TMPREMAINING}.recursive ${REMAINING}.recursive
  else
    rm -f ${REMAINING}.recursive 2>/dev/null 
  fi  
fi


build_result_file


#
# Update time stamp
#
echo ${CURRENT_TS} > ${TIMESTAMP}

#
# Success 
#
if [ "$((total_failed+total_remaining+total_failed_recursive+total_remaining_recursive))" == "0" ]
then 
  update_state "SUCCESS" "NONE"
  rm -f ${FAILED}.update
  rm -f ${REMAINING}.update
  rm -f ${FAILED}.recursive
  rm -f ${REMAINING}.recursive
  if [ "${verbose}" == "-v" ]
  then
    exit 0
  fi  
  rm -f /tmp/${DIRNAME}.*
  exit 0
fi

#
# Command has been aborted
#
if [ "$((total_remaining+total_remaining_recursive))" != "0" ]
then
  #
  # Aborted
  #
  update_state "ABORTED" "NONE"
  exit 1  
fi


#
# Failed
#
rm -f ${REMAINING}.update
rm -f ${REMAINING}.recursive

update_state "FAILED" "NONE"
exit ${status}
