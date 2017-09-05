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
VFSTAT=/tmp/vfstat_rozodiag.$$
VERSION="Version 1.0"
PROGNAME=`basename $0`
SYNCHRO=/tmp/synchro_rozodiag.$$
# Exit codes
STATE_OK=0
STATE_WARNING=1
STATE_CRITICAL=2
STATE_UNKNOWN=3

rozodiag_PATHS=". /usr/bin /usr/local/bin $ROZO_TESTS/build/src/rozodiag" 
resolve_rozodiag() {

  if [ ! -z "$port" ];
  then
    option="-i $1 -p $port"
  else
    option="-i $1 -T $DBGTARGET"   
  fi
  

  if [ ! -z "$time" ];
  then
    option=`echo "$option -t $time"`
  fi

  for path in $rozodiag_PATHS
  do
    if [ -f $path/rozodiag ];
    then
      ROZDBG="$path/rozodiag $option"
      return
    fi  
  done
  echo "Can not find rozodiag"
  exit $STATE_UNKNOWN 
}

# Help functions
print_revision() {
   # Print the revision number
   echo "$PROGNAME - $VERSION"
}
print_usage() {
   # Print a short usage statement
   echo "Usage: $PROGNAME [-v] [-p <port>] [-t <seconds>] -H <host> [-volume <volume#>] -w <percent%> -c <percent%>"
}
print_help() {

   # Print detailed help information
   print_revision
   print_usage

  echo "Options:"
  echo "{-h|--help}"
  echo "    Print detailed help screen"
  echo "{-V|--version}"
  echo "    Print version information"
  echo "-volume <volume#>"
  echo "    Volume number to monitor"  
  echo "{-H|--hostname} <host name>"
  echo "    Destination host of the export daemon"
  echo "{-t|--timeout} <seconds>"
  echo "    Time out value"  
  echo "{-w|--warning} <threshold|percent%>"
  echo "    Exit with WARNING status if less than <threshold> GB or <percent>% of disk is free"
  echo "{-c|--critical} <threshold|percent%>"
  echo "    Exit with CRITICAL status if less than <threshold> GB or <percent>% of disk is free"
  echo "-p <port>"
  echo "    To eventually redefine the export debug port"
  echo "{-v|--verbose}"
  echo "    Verbose output"
  exit $STATE_OK
}

display_output() {
  case $1 in
   "$STATE_OK")         echo "OK <> FREE $free $percent%";;
   "$STATE_WARNING")    echo "WARNING <$2> FREE $free $percent%";;
   "$STATE_CRITICAL")   echo "CRITICAL <$2> FREE $free $percent%";; 
   "$STATE_UNKNOWN")    echo "UNKNOWN <$2> FREE $free $percent%";;    
  esac
  rm -f $VFSTAT
  exit $1
}
set_default() {
  verbosity=0
  thresh_warn=""
  thresh_warn_percent=""
  thresh_crit=""
  thresh_crit_percent=""
  host=""
  time="" 
  free=0
  percent=0
  volume=1
  DBGTARGET="export"
}

is_numeric () {
  if [ "$(echo $1 | grep "^[ [:digit:] ]*$")" ] 
  then 
    value=$1
    return 1
  fi
  return 0
}
is_percent () {
  last=`echo $1 | tail -c2`
  if [ $last != '%' ] 
  then 
    return 0
  fi
      
  value=`echo $1 | sed 's/.\{1\}$//g'`
  is_numeric $value
  if [ $? = 0 ]
  then
    display_output $STATE_UNKNOWN "Percent value is not numeric"    
  fi
  if [ $value -gt 100 ];
  then
    display_output $STATE_UNKNOWN "Percent must not exceed 100"
  fi
  if [ $value -lt 0 ];
  then
    display_output $STATE_UNKNOWN "Percent must not be less than 0"
  fi
  return 1 
}

scan_numeric_value () {
  if [ -z "$2" ];
  then 
      display_output $STATE_UNKNOWN "Option $1 requires an argument"   
  fi
  is_numeric $2
  if [ $? = 0 ];
  then
      display_output $STATE_UNKNOWN "Option $1 requires a numeric argument"   	   	
  fi
  value=$2
}
scan_value () {
  if [ -z "$2" ];
  then 
      display_output $STATE_UNKNOWN "Option $1 requires an argument"   
  fi
  value=$2
}
do_ping() {
  
  ping $1 -c 1 -w 1 >> /dev/null
  if [ $? == 0 ]
  then
    return 0
  fi  

  ping $1 -c 1 -w 1 >> /dev/null
  if [ $? == 0 ]
  then
    return 0
  fi  
  return 
}
# Run rozodiag vfstat_vol to check a given volume
#
do_vfstat_vol() {
 
  attempt=1
  cmd="vfstat_vol"

  while [ 1 ]
  do
  
    # Rozodiag command
    $ROZDBG -c $cmd >  $VFSTAT
        
    # No answer
    res=`grep $cmd $VFSTAT`
    case $res in
      "") return 1;;
    esac
    
    # Check the given volume is there
    STRING="Volume: $volume"
    res=`grep "$STRING" $VFSTAT`
    case $res in
      "");;
      *) return 0;;  
    esac
    
    # No such volume. Re attempt if max not exceeded
    case $attempt in
      "3") return 2;;
    esac  
    attempt=$((attempt+1))
    sleep 1
  done
  
  return 1
}

# Run rozodiag do_vfstat_stor to check a given volume
#
do_vfstat_stor() {
 
  attempt=1
  cmd="vfstat_stor"

  while [ 1 ]
  do
  
    # Rozodiag command
    $ROZDBG -c $cmd >  $VFSTAT
        
    # No answer
    res=`grep $cmd $VFSTAT`
    case $res in
      "") return 1;;
    esac
    
    # Check the given volume is there
    STRING="Volume: $volume"
    res=`awk '{if ($1==volume) printf("0K");}' volume=$volume $VFSTAT`
    case $res in
      "");;
      *) return 0;;  
    esac
    
    # No such volume. Re attempt if max not exceeded
    case $attempt in
      "3") return 2;;
    esac  
    attempt=$((attempt+1))
    sleep 1
  done
  
  return 1
}

date > /var/tmp/rozo
echo "call $0 $1 $2 $3 $4 $5 $6 $7 $8 $9" >> /var/tmp/rozo


#######################################################
#         M A I N
#######################################################

# Set default values
set_default


# Parse command line options
while [ "$1" ]; do
   case "$1" in
       -h | --help)           print_help;;
       -V | --version)        print_revision; exit $STATE_OK;;
       -v | --verbose)        set -x; verbosity=1; shift 1;;
       -w | --warning | -c | --critical) {
           if [ -z "$2" ];
	   then 
               display_output $STATE_UNKNOWN "Option $1 requires an argument"   
	   fi
	   is_percent $2
	   if [ $? = 1 ];
	   then
	     [[ "$1" = *-w* ]] && thresh_warn_percent=$value || thresh_crit_percent=$value  	       
	   else
	     is_numeric $2
	     if [ $? = 1 ];
	     then
	       [[ "$1" = *-w* ]] && thresh_warn=$value || thresh_crit=$value  	     
	     else
               display_output $STATE_UNKNOWN "Threshold must be integer or percentage"
             fi
	   fi
           shift 2
        };;
	-H | --hostname)  scan_value $1 $2;         host=$value;       shift 2;;	
	-volume)          scan_numeric_value $1 $2; volume=$value;     shift 2;;	
	-t | --time)      scan_numeric_value $1 $2; time=$value;       shift 2;;
	-p)               scan_numeric_value $1 $2; port=$value;       shift 2;;
       *) display_output $STATE_UNKNOWN "Unexpected parameter or option $1"
   esac
done


# Check mandatory parameters are set

if [ -z $host ];
then
   display_output $STATE_UNKNOWN "-H option is mandatory"
fi  

if [ ${host} = "ROZO_EXPORT_HA" ]
then
  # In this case we will compute the current master IP for exportd service
  # thanks cluster tools

  # Check if corosync and pacemaker are installed
  CLUSTER_TOOLS="crm corosync-cfgtool crm_resource crm_node pgrep sudo"
  for exe in ${CLUSTER_TOOLS}; do
    test=`which ${exe}`
    if [ "$?" -eq 1 ] ; then
      display_output $STATE_UNKNOWN "${exe} binary is required for getting rozofs-exportd IP"
    fi
  done

  # check if corosync is running
  test=` pgrep corosync > /dev/null 2>&1`
  if [ "$?" -eq 1 ] ; then
      display_output $STATE_CRITICAL "corosync is not running"
  fi

  # check if pacemaker is running
  test=`sudo crm node status 2>&1`
  if [ "$?" -eq 1 ] ; then
      display_output $STATE_CRITICAL "pacemaker is not running"
  fi

  # Check if resource exportd-rozofs is running
  EXPORTD_STATUS=`sudo crm resource status exportd-rozofs 2>&1 | grep -q 'is running'`
   if [ "$?" -eq 1 ] ; then
       display_output $STATE_CRITICAL "resource exportd-rozofs is not running"
  fi

  # get master node name
  NODE_NAME=`sudo crm_resource --resource exportd-rozofs --locate -Q`
  NODE_ID=`sudo crm_node -l | grep ${NODE_NAME} | awk '{print $1;}'`
  IP_LIST=`sudo corosync-cfgtool -a ${NODE_ID}`
  HOST_PARAM=`echo ${IP_LIST/' '/\/}`
  host=${HOST_PARAM}
fi  

if [ -z "$thresh_warn" ];
then
  if [ -z "$thresh_warn_percent" ];
  then
    display_output $STATE_UNKNOWN "-w option is mandatory"
  fi     
fi  


if [ -z "$thresh_crit" ];
then
  if [ -z "$thresh_crit_percent" ];
  then
    display_output $STATE_UNKNOWN "-c option is mandatory"
  fi     
fi  


# ping every destination host
# $host is a list '/' separated hosts
host=`echo $host | sed 's/\// /' `
ok=0
hosts=""
initial_hosts="$host"
for h in $host
do
  
  do_ping $h
  if [ $? == 0 ]
  then
    hosts[$ok]=$h
    if [ -z $pinged_hosts ]
    then
      pinged_hosts="$h"
    else
      pinged_hosts="$pinged_hosts $h"     
    fi  
    ok=$((ok+1))
  fi  
    
done  
case $ok in
  "0") display_output $STATE_CRITICAL "$host do not respond to ping"
esac
# hosts is now the array of host responding to ping


# Find rozodiag utility and prepare command line parameters



ok=0
errors=""
for i in $(seq ${#hosts[@]} )
do

  resolve_rozodiag ${hosts[$((i-1))]}

  # Run vfstat_vol debug command on export to get volume statistics

  do_vfstat_vol
  res=$?
  case $res in
    "0") ok=1; break;;
    *) errors="$errors $res"
  esac  
done
case $ok in
  "0") display_output $STATE_CRITICAL "CFG[$initial_hosts] PING[$pinged_hosts] ERRORS[$errors] do not respond to rozodiag vfstat_vol";;
esac
host=${hosts[$((i-1))]}


# Check DRDB synchronization status
$ROZDBG -c synchro drbd >  $SYNCHRO
synchro_warning=`awk '
{
  if (($1=="0:")||($1=="1:")) 
  {
    if (($2!= "cs:Connected")||($3!= "ro:Primary/Secondary")||($4!= "ds:UpToDate/UpToDate")) {
      print "drdb $1 out of synchro";
    }	
    else {
      print "OK";
    }
  }
}' $SYNCHRO`
case "$synchro_warning" in
  "OK");;
  "")  ;;
  *)  display_output $STATE_CRITICAL "$res";;    
esac

  
# Check pacemaker synchronization status
$ROZDBG -c synchro crm >  $SYNCHRO
grep --quiet "Failed" $SYNCHRO
STATUS=$?
if [ ${STATUS} -eq 0 ]
then
  DETAILS=`awk '/Failed/ {f=1}f' $SYNCHRO | grep --invert-match Failed`
  display_output $STATE_CRITICAL "Failed action(s): ${DETAILS}"
fi


# Extract volume usage from the debug output

res=`awk '{if (($1=="Volume:") && ($2==volume)) printf("%s %s\n",$8,$12);}' volume=$volume $VFSTAT`
case $res in 
  "") display_output $STATE_CRITICAL "$host do not host volume $volume"
esac
free=`echo $res | awk '{print $1}'`
percent=`echo $res | awk '{print $2}'`


# Run vfstat_stor debug command on export to check storage status

do_vfstat_stor
res=$?
case $res in
  "0");;
  *) {
    display_output $STATE_CRITICAL "$host do not respond to rozodiag vfstat_stor"
  };;  
esac

up=`awk 'BEGIN {nb=0;} {if (($1==volume) && ($7=="UP")) nb++;} END {printf("%d\n",nb);}' volume=$volume $VFSTAT`
if [ $up -eq 0 ]
then
  display_output $STATE_CRITICAL "No storage is UP"
fi
down=`awk 'BEGIN {nb=0;} {if (($1==volume) && ($7=="DOWN")) nb++;} END {printf("%d\n",nb);}' volume=$volume $VFSTAT`
if  [ $down -gt 0 ] 
then
  display_output $STATE_WARNING "$down storage(s) down"
fi


# Check if critical threshold is excedeed

if [ ! -z $thresh_crit  ];
then
  if [ $free -le $thresh_crit ];
  then
    display_output $STATE_CRITICAL "Free volume threshold less than $thresh_cri"    
  fi
else 
  if [ $percent -le $thresh_crit_percent ];
  then
    display_output $STATE_CRITICAL "Free volume threshold less than $thresh_crit_percent%"    
  fi
fi  

# Check if warning threshold is excedeed

if [ ! -z $thresh_warn  ];
then 
  if [ $free -le $thresh_warn ];
  then
    display_output $STATE_WARNING "Free volume threshold less than $thresh_warn"    
  fi
else 
  if [ $percent -le $thresh_warn_percent ];
  then
    display_output $STATE_WARNING "Free volume threshold less than $thresh_warn_percent%"    
  fi
fi  


case "$synchro_warning" in
  "OK")display_output $STATE_OK;;
  "")  display_output $STATE_WARNING "No drdb synchro";;
  *)   display_output $STATE_CRITICAL "$synchro_warning";;    
esac
 
# Hurra !!!
  
