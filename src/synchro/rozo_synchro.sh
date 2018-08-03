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
  awk '{if ($1==element) print $3;}' element=$1 ${CFG}
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
  
  printf "      , \"LAST SYNCHRONIZATION\"\t: {\n"                                              >  ${RESULTS}
  printf "          \"DURATION\"\t: \"%s\",\n"  ${DELAY}                                        >> ${RESULTS} 
  printf "          \"DIRECTORIES\" : {\n"                                                      >> ${RESULTS}
  printf "             \"SUCCESS\" \t\t: %s,\n" ${total_success}                                >> ${RESULTS}
  printf "             \"FAILED\" \t\t: %s,\n"  ${total_failed}                                 >> ${RESULTS}
  printf "             \"REMAINING\" \t: %s,\n"  ${total_remaining}                             >> ${RESULTS}
  printf "             \"TOTAL\" \t\t: %s\n"    $((total_success+total_failed+total_remaining)) >> ${RESULTS}
  printf "          },\n"                                                                       >> ${RESULTS}
  #
  # File stats
  #
  printf "          \"FILES\"\t: {\n"                                                           >>  ${RESULTS}
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
    printf("            \"CREATED\"\t: %s,\n",cr);
    printf("            \"DELETED\"\t: %s,\n",del);
    printf("            \"B.SENT\" \t: %s,\n",snd);
    printf("            \"B.RECV\" \t: %s\n",rcv);
  } ' ${RSYNCLOG}                                                                          >> ${RESULTS}
  printf "          }\n"                                                                   >> ${RESULTS}
  printf "      }\n"                                                                       >> ${RESULTS}

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
# g preserve grou
# o preserve owner
# D preserve device
# A preserve ACL
# X preserve extended attributes
# E preserve executability
#  echo "  if ! rsync --stats --sparse -d -lptgoDAXE --sockopts=SO_SNDBUF=1048576,SO_RCVBUF=1048576 --delete-before --update ${MNT}/\$1/ --rsync-path=\"mkdir -p ${BASE_DST}/\$1 && rsync --sockopts=SO_SNDBUF=1048576,SO_RCVBUF=1048576 \" root@${TARGET}:${BASE_DST}/\$1"
  if [ "${SPARSE}" == "YES" ]
  then
    sparse=" --sparse "
  else
    sparse=" "
  fi    
  echo "  if ! rsync --stats -d -lptgoDAXE${sparse}--sockopts=SO_SNDBUF=1048576,SO_RCVBUF=1048576 --delete-before --update ${MNT}/\$1/ --rsync-path=\"mkdir -p ${BASE_DST}/\$1 && rsync --sockopts=SO_SNDBUF=1048576,SO_RCVBUF=1048576 \" root@${TARGET}:${BASE_DST}/\$1"
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
  display_rsync_file > $1
  chmod +x $1
}
#_________________________________________________
# Check whether rsync script needs to be generated
#_________________________________________________
build_rsync_file_when_needed() {
  # Create the rsync script if it does not yet exist
  if [ ! -f ${DIR}/process_synchro.sh ];
  then
    build_rsync_file ${DIR}/process_synchro.sh
    return
  fi 

  # Re-create the rsync script if configuration has changed
  if [  ${DIR}/process_synchro.sh -ot ${CFG} ];
  then
    build_rsync_file ${DIR}/process_synchro.sh
    return
  fi

  # Re-create the rsync script if configuration has changed
  myshell=`which rozo_synchro.sh`
  if [ $? -eq 0 ]
  then
    if [  ${DIR}/process_synchro.sh -ot ${myshell} ];
    then
      build_rsync_file ${DIR}/process_synchro.sh
    fi
  fi  
}
#_________________________________________________
# Build source host list 
#_________________________________________________
build_src_host_list_without_master_export() {
  SRC_HOST=""
  for target in `cat ${DIR}/../.nodefile`
  do
  
    if [ "${target}" == "${HOSTNAME}" ]
    then 
      continue
    fi
    
    ${PING} $target > /dev/null
    if [ $? -eq 0 ];
    then 
      SRC_HOST="-S ${target} ${SRC_HOST}"
    fi
    
  done
}
#_________________________________________________
# Build source host list 
#_________________________________________________
build_src_host_list() {
  SRC_HOST=""
  for target in `cat ${DIR}/../.nodefile`
  do
    ${PING} $target > /dev/null
    if [ $? -eq 0 ];
    then 
      SRC_HOST="-S ${target} ${SRC_HOST}"
    fi
  done
}
#_________________________________________________
# Update the state 
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
      printf " %s\n" "$1"
    else
      printf " %s_%s\n" "$1" "$2" 
    fi >> ${HISTORY}
    
    # Write results into history file
    awk '{if (($2==":")&&($3!="{")&&($3!="0")&&($3!="0,")) {printf("%s : %s\n",$1,$3);}}' ${RESULTS} | sed 's/"//g' | sed 's/,//g' >> ${HISTORY}    
    #
    # Switch history files when full
    #
    sz=`stat -c%s ${HISTORY}`
    if [ $sz -ge 524288 ];
    then
      mv ${HISTORY} ${HISTORY}.old
    fi    
  fi
 
}
#_________________________________________________
# Build scan result file
# $1 scan result file
# $2 file to append to the result (previous FAILED)
#_________________________________________________
do_scan() {

  #
  # Depending whether there is a project filter
  #
  if [ "${PROJECT}" == "" ]
  then
    rozo_scan_by_criteria --eid ${EID} -T --dir --update --ge "${PREVIOUS_TS}" --le "${CURRENT_TS}"
  else  
    rozo_scan_by_criteria --eid ${EID} -T --dir --update --ge "${PREVIOUS_TS}" --le "${CURRENT_TS}" --project --eq ${PROJECT}
  fi  > $1
  
  if [ $? -ne 0 ]
  then
    update_state "FAILED" "rozo_scan_by_criteria failed" 
    exit 1
  fi

  #
  # There may also exist a sub-directory filter
  #
  case "${DIRECTORY}" in
    "")  mv $1 $1.tmp;;
    "/") mv $1 $1.tmp;;
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
          done > $1.tmp
    };;
  esac

  #
  # Append previous failed directories if any
  #
  list="$1.tmp"
  if [ "$2" != "" ]
  then 
    if [ -f $2 ]
    then
      list="$list $2"
    fi  
  fi
  if [ "$3" != "" ]
  then
    if [ -f $3 ]
    then
      list="$list $3"
    fi  
  fi
  cat $list | sort -u > $1
  rm -f $1.tmp
}
#_________________________________________________
# BProcess to a dry run
#_________________________________________________
dry_run() {
  failed_directories=""

  #
  # Read the last synchro timestamp
  #
  if [ "$full" == "YES" ]
  then
    PREVIOUS_TS="2000-01-01 00:00:00"
  else  
    if [ ! -f  ${TIMESTAMP} ];
     then
       PREVIOUS_TS="2000-01-01 00:00:00"
     else
       PREVIOUS_TS=`cat ${TIMESTAMP}`
       failed_directories="${FAILED} ${REMAINING}"
     fi 
  fi  

  #
  # Process scanning 
  #
  do_scan /tmp/${DIRNAME}.dry ${failed_directories}
  cat /tmp/${DIRNAME}.dry
  nb=`cat /tmp/${DIRNAME}.dry | wc -l`
  printf "\n %s directories\n\n" ${nb}
  rm -f /tmp/${DIRNAME}.dry
  exit 0
  
}



           #_________________________________________________#
           #                   M A I N                       #
           #_________________________________________________#


#
# Read parameters
#
DIR=""
full="NO"
dry="NO"
while [ ! -z $1 ];
do
  case "$1" in 
    "-d") {
       DIR=$2
       shift 2
    };;
    
    # Verbose mode
    "-v") set -x; shift 1;;
    "-f") {
       full="YES"
       shift 1
    };;
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
NAME=`basename ${DIR}`
DIRNAME=`echo $DIR | tr \/ _`
JOBLOG="/tmp/${DIRNAME}.log_gnu_parallel"
RSYNCLOG="/tmp/${DIRNAME}.log_rsync"
SRC_HOST=""
SCAN_RESULT="/tmp/${DIRNAME}.scan_result"
CFG="${DIR}/cfg"
TIMESTAMP="${DIR}/timestamp"

FAILED="${DIR}/failed_directories"
SUCCESS="/tmp/success_directories"
REMAINING="${DIR}/remaining_directories"
RESULTS="${DIR}/results"
HISTORY="${DIR}/history"

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
MNT=`read_cfg MNT`
DIRECTORY=`read_cfg DIRECTORY`
TARGET=`read_cfg TARGET`
BASE_DST=`read_cfg BASE_DST`
SPARSE=`read_cfg  SPARSE`
total_success=0
total_failed=0
total_remaining=0
#
# Create the rsync script if it needs to
#
build_rsync_file_when_needed
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
printf "\n_%s_" "${CURRENT_TS}" >> ${HISTORY}

#
# Full synchro : remove timestamp file
#
if [ "$full" == "YES" ]
then
  rm -f ${TIMESTAMP}
  rm -f ${FAILED}    
  rm -f ${REMAINING}
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
  update_state "FAILED" "No source host reachable"
  exit  1
fi

#
# Read the last synchro timestamp
#
if [ ! -f  ${TIMESTAMP} ];
then
  PREVIOUS_TS="2000-01-01 00:00:00"
  rm -f ${FAILED}
  rm -f ${REMAINING}
else
  PREVIOUS_TS=`cat ${TIMESTAMP}`
fi

#
# Scanning command to get the list of directory that neeed synchronization
#
update_state "SCANNING DIRECTORIES" 
do_scan ${SCAN_RESULT} ${REMAINING} ${FAILED}
if [ ! -s ${SCAN_RESULT} ]
then
  truncate -s 0 ${RSYNCLOG}
  build_result_file
  update_state "SUCCESS" "Up to date"
  #
  # Update time stamp
  #
  echo ${CURRENT_TS} > ${TIMESTAMP}
  exit 0  
fi

#
# Run //
#
update_state "RUNNING PARALLEL"
parallel -a ${SCAN_RESULT} -X -L 4 --joblog ${JOBLOG} ${SRC_HOST} --jobs 4 --retries 3 ${DIR}/process_synchro.sh {} 2>/dev/null | sed 's/,//g' >  ${RSYNCLOG}
status=$?

#
# Analyze the results
#
update_state "ANALYZING RESULTS"
res=`rozo_synchro_analyze_result ${DIR} ${SCAN_RESULT} ${JOBLOG}`
if [ $? -ne 0 ];
then
  update_state "FAILED" "rozo_synchro_analyze_result"
  exit 1
fi
total_success=`echo ${res} | awk '{print $2;}'`
total_failed=`echo ${res} | awk '{print $4;}'`
total_remaining=`echo ${res} | awk '{print $6;}'`
build_result_file
#
# Update time stamp
#
echo ${CURRENT_TS} > ${TIMESTAMP}

#
# Success 
#
if [ "$((total_failed+total_remaining))" == "0" ]
then 
  update_state "SUCCESS" "NONE"
  rm -f ${FAILED}
  rm -f ${REMAINING}
  exit 0
fi

#
# Command has been aborted
#
if [ "${total_remaining}" != "0" ]
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
rm -f ${REMAINING}
update_state "FAILED" "NONE"
exit ${status}
