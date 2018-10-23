#!/bin/bash

LIST="date commit branch mojette bpo pretty"

syntax() {
  usage="Usage : cmake_get.sh <source directory> ["
  or=""
  for topic in $LIST
  do
    usage=$usage$or$topic
    or="|"
  done
  usage=$usage"]"
  echo $usage
  exit 1
}  
rozo_date() {
  ROZO_DATE=`date "+%Y/%m/%d %HH%M" | tr -d '\n' `
  printf "%s" "${ROZO_DATE}"
}
rozo_commit() {
  ROZO_COMMIT=`git rev-parse HEAD`
  printf "%s" "${ROZO_COMMIT}"   
}
rozo_branch() {
  ROZO_BRANCH=`git rev-parse --abbrev-ref HEAD `
  printf "%s" "${ROZO_BRANCH}"   
}
rozo_mojette() {
  case `grep "./moj128_opt/" ./src/storcli/CMakeLists.txt` in
    "") ROZO_MOJETTE="LEGACY";;
    *)  ROZO_MOJETTE="FSX";;
  esac
  printf "%s" "${ROZO_MOJETTE}"   
}
# Get bpo version to append on each debian packages builded
rozo_bpo() {
  [ -x "/usr/bin/lsb_release" ] || exit 0
  lsb_release=`lsb_release -c | sed 's/Codename://g' | sed 's/^[ \t]*//;s/[ \t]*$//'` 

  ROZO_BPO=""
  case $lsb_release in
    wheezy)
      ROZO_BPO="~bpo70+1"
    ;;
    jessie)
      ROZO_BPO="~bpo8+1"
    ;;
    stretch)
      ROZO_BPO="~bpo9+1"
    ;;
    buster)
      ROZO_BPO="~bpo10+1"
    ;;
    trusty)
      ROZO_BPO="~trusty0+1"
    ;;
    utopic)
      ROZO_BPO="~utopic0+1"
    ;;
    vivid)
      ROZO_BPO="~vivid0+1"
    ;;
    xenial)
      ROZO_BPO="~xenial0+1"
    ;;
    bionic)
      ROZO_BPO="~bionic0+1"
    ;;
    cosmic)
      ROZO_BPO="~cosmic0+1"
    ;;
    *)
      ROZO_BPO=""
    ;;
  esac
  printf "%s" "${ROZO_BPO}"
}
# Get bpo version to append on each debian packages builded
rozo_pretty() {
  if [ -s "/etc/os-release" ] 
  then
    PRETTY_NAME=`awk -F'"' '{if ($1=="PRETTY_NAME=") print $2; }' /etc/os-release`
  elif [ -s "/etc/system-release" ]
  then
    PRETTY_NAME=`cat /etc/system-release`
  elif [ -s "/etc/redhat-release" ]  
  then  
    PRETTY_NAME=`cat /etc/redhat-release`  
  elif [ -s "/etc/centos-release" ]   
  then
    PRETTY_NAME=`cat /etc/centos-release` 
  else
    PRETTY_NAME="??"  
  fi    
  printf "%s" "${PRETTY_NAME}"
}
rozo_param() {
  lower=`echo "$1" | tr '[:upper:]' '[:lower:]'`
  rozo_$lower
}  

if [ $# -lt 2 ];
then 
  syntax
fi

if [ ! -d "$1" ];
then 
  printf "$1 is not a directory\n"  
  syntax
fi

cd $1
shift 1

separator=""
while [ ! -z $1 ];
do
  printf "%s" "$separator"
  rozo_param $1
  shift 1
  separator=" "
done
