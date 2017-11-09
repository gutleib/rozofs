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

TMPFILE=/tmp/rozofsmount_rozodiag.$$
TMPFILE2=/tmp/rozofsmount_rozodiag2.$$
TMPFILE3=/tmp/rozofsmount_rozodiag3.$$
VERSION="Version 1.0"
PROGNAME=`basename $0`

# Exit codes
STATE_OK=0
STATE_WARNING=1
STATE_CRITICAL=2
STATE_UNKNOWN=3

rozodiag_PATHS=". /usr/bin /usr/local/bin $ROZO_TESTS/build/src/rozodiag" 
resolve_rozodiag() {

  if [ ! -z "$port" ];
  then
    option="-i $host -p $port"
  else
    option="-i $host -T $DBGTARGET"   
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
   echo "Usage: $PROGNAME [-v] [-p <port>] [-t <seconds>] -H <host> [-instance <instance#>] "
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
  echo "-instance <instance#>"
  echo "    instance number of the Rozofsmount"  
  echo "{-H|--hostname} <host name>"
  echo "    Destination host of the Rozofsmount instance"
  echo "{-t|--timeout} <seconds>"
  echo "    Time out value"  
#  echo "{-w|--warning} <threshold|percent%>"
#  echo "    Exit with WARNING status if less than <threshold> GB or <percent>% of disk is free"
#  echo "{-c|--critical} <threshold|percent%>"
#  echo "    Exit with CRITICAL status if less than <threshold> GB or <percent>% of disk is free"
  echo "-p <port>"
  echo "    To eventually redefine the debug port"
  echo "{-v|--verbose}"
  echo "    Verbose output"
  exit $STATE_OK
}

display_output() {
  case $1 in
   "$STATE_OK")         echo "OK <> ";;
   "$STATE_WARNING")    echo "WARNING <$2> ";;
   "$STATE_CRITICAL")   echo "CRITICAL <$2> ";; 
   "$STATE_UNKNOWN")    echo "UNKNOWN <$2> ";;    
  esac
  rm -f $TMPFILE
  rm -f $TMPFILE2
  rm -f $TMPFILE3
  exit $1
}
set_default() {
  verbosity=0
  host=""
  time=""
  instance=0
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

test_storcli()
{
  # 1st storcli
  $ROZDBG -c storaged_status >  $TMPFILE

  res=`grep cid $TMPFILE`
  case $res in
   "") {
     display_output $STATE_CRITICAL "$host do not respond to rozodiag"
   };;  
  esac

  sed '1,2d'  $TMPFILE > $TMPFILE2
  res=`awk 'BEGIN {up=0;down=0;} {if (($9=="UP") && ($11=="UP")) up++; if (($9=="DOWN") || ($11=="DOWN")) down++;} END {printf("%d %d\n",up, down);}' $TMPFILE2`
  up=`echo $res | awk '{print $1}'`
  down=`echo $res | awk '{print $2}'`
  if [ $down -eq 1 ]
  then
   res=`awk ' {if (($9!="UP") || ($11!="UP")) printf("%s ",$5);} END {printf("\n");}' $TMPFILE2`  
   display_output $STATE_WARNING "Storage(s) $res unreachable from I/O process $1"
  fi
  if [ $down -gt 1 ]
  then
   res=`awk ' {if (($9!="UP") || ($11!="UP")) printf("%s ",$5);} END {printf("\n");}' $TMPFILE2`  
   display_output $STATE_CRITICAL "Storage(s) $res unreachable from I/O process $1"
  fi
}


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
	-instance)          scan_value $1 $2; instance=$value;     shift 2;;	
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



# ping the destination host

ping $host -c 1 >> /dev/null
if [ $? != 0 ]
then
  # re attempt a ping
  ping $host -c 2 >> /dev/null
  if [ $? != 0 ]
  then  
    display_output $STATE_CRITICAL "$host do not respond to ping"
  fi 
fi

if [ ! -z "$instance" ];
then
  if [ ${instance} = "ALL" ]
  then
    instance_list=` grep -e '^rozofsmount' /etc/fstab | grep -o 'instance=.*' | cut -f1 -d, | cut -f2 -d= | tr '\n' ' '`
  else  
    instance_list=${instance}
  fi
fi

for ins in ${instance_list}; do
  
  # check if instance is a number
  scan_numeric_value "instance" $ins;

  DBGTARGET="mount:$ins"

  # Find rozodiag utility and prepare command line parameters
  resolve_rozodiag

  $ROZDBG -c lbg_entries >  $TMPFILE
  res=`grep "LBG Name" $TMPFILE`
  case $res in
    "") {
      # Try to get the mountpoint from fstab
      mountpoint=$(grep instance=${ins} /etc/fstab  | cut -f2 -d" ")

      if [ ${host} = "localhost" ] && [ ! -z ${mountpoint} ]
      then
        message="mountpoint: ${mountpoint} do not respond to rozodiag"
      elif [ ! -z ${mountpoint} ]
      then
        message="on host ${host}, mountpoint: ${mountpoint} do not respond to rozodiag"
      else
        message="$host:mount:$ins $mountpoint do not respond to rozodiag"
      fi
      display_output $STATE_CRITICAL "${message}"
    };;  
  esac

  exp_up=`awk 'BEGIN {nb=0;} {if (($1=="EXPORTD") && ($9=="UP")) nb++;} END {printf("%d\n",nb);}' $TMPFILE`
  if [ $exp_up -lt 1 ]
  then
    display_output $STATE_CRITICAL "No exportd connectivity"
  fi

  # Get the number of storcli 
  $ROZDBG -c stclbg >  $TMPFILE3
  NBSTORCLI=`awk -F':' '{if ($1=="number of configured storcli") { print $2 }}' $TMPFILE3`
  storcli_up=`awk 'BEGIN {nb=0;} {if (($1=="STORCLI") && ($9=="UP")) nb++;} END {printf("%d\n",nb);}' $TMPFILE`
  if [ $storcli_up -ne $NBSTORCLI ]
  then
    display_output $STATE_CRITICAL "No internal I/O connectivity ($storcli_up/$NBSTORCLI)"
  fi

  for i in $(seq $NBSTORCLI)
  do
    if [ ! -z "$port" ];
    then
      port=`expr $port + 1 `
    else
      DBGTARGET="mount:$ins:$i"   
    fi    
    resolve_rozodiag
    
    test_storcli $i
  done

done

# Hurra !!!
display_output $STATE_OK 
