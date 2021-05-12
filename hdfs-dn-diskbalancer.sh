#!/usr/bin/env bash

function log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') ${*}" | tee -a "${LOGFILE:-/mnt/disk1/log/hadoop-hdfs/hdfs-dn-diskbalancer.log}"
}

function log_error() {

    echo "${*}" >/dev/stderr
}

function printHelp() {

cat <<EOF
$@

Usage: $(basename $0) 
        --hdfs-config|-c FILENAME  Specify the datanode's HDFS configuration file
                                   Default: /etc/ecm/hadoop-conf/hdfs-site.xml
        --threshold|-t PERCENTAGE  Tolerate up to this % of difference between 2 disks.
                                   Integer value. Default: 10                                   
        --purge-volumes|-p DISKS_TO_PURGE
                                   Specify the comma-delimited disk(s) to be purged.
                                   eg. /mnt/disk1 or /mnt/disk1,/mnt/disk2
        --force|-f                 Force running when executed as root

EOF
    
exit 2

}


# This function fill a global array with the disks
# mentioned in datanode's HDFS config file
function parseDisks() {

  [ -f "${HDFS_CONF}" ] || { log "HDFS config file ${HDFS_CONF} does not exist. Exiting."; exit 2; }
  log "Loaded datanode config file ${HDFS_CONF}"
  IFS="," read -r -a HDFS_DISKS < <(grep "<name>dfs.datanode.data.dir</name>"\
                                    "${HDFS_CONF}"\
                                    -A 1|tail -n1|sed -r "s_^[ ]*<value>(.*)</value>_\1_"|tr -d ' ')

  [ ${#HDFS_DISKS[@]} -lt 2 ] && { log "We need at least 2 disks to balance. Found only ${#HDFS_DISKS[@]} in config file.";
                                   exit 2; }
  log "Data disks to be balanced: ${HDFS_DISKS[*]}"

  PURGE_DISKS=;
  [ "${PURGE_VOLUMES}" == "" ] || { log "The volume(s) would be purged: ${PURGE_VOLUMES}."; IFS="," PURGE_DISKS=($PURGE_VOLUMES);
    #for i in "${!PURGE_DISKS[@]}"; do
    #  log "DEBUG Parsing PURGE_DISKS $i=>${PURGE_DISKS[i]}"
    #done
  }
}

# Skip the disks in purge disks list
function skipDisk() {
  local e match="$1"
  shift
  for e; do
    if [[ "$e" == "$match" ]]; then
     printf 0;
     return;
    fi
  done
  printf 1
  return;
}

# Get a disk based on "disk used size" 
function getUsedDisk() {

  [[ "${1}" == "most" ]] && ORDER=""
  [[ "${1}" == "least" ]] && ORDER="r"

  FIELD=2
  [[ "${2}" == "size" ]] && FIELD=1

  for d in "${HDFS_DISKS[@]}"
  do
    # filter file:// and /hdfs in file:///mnt/disk1/hdfs
    dir=`echo $d | sed 's/file\:\/\///g' | sed 's/\/hdfs//g'`

    #log "getUsedDisk skipDisk "
    # if no mounted disk is found, fall back to root
    if [[ $(skipDisk ${dir} ${PURGE_DISKS[@]}) -eq 1 ]]; then
         df -ml --output="used,target" | grep "${dir}" \
        || { df -ml --output="used,target" |grep -E "/$"|sed -r "s_/\$_${dir}_"; }
    fi
  done | sort -n${ORDER} -k1 | tail -n1 \
   | awk "{print \$${FIELD}}"

}


# returns the RELATIVE path to the biggest subdir in the most used data disk
function getBiggestSubdir() {

#  BIGGEST_DISK=$(getUsedDisk most)
  BIGGEST_DISK=$1
  cd "${BIGGEST_DISK}"
  # find the biggest 1st level "subdirNN"
  find hdfs/current/BP-*/current/finalized/  -mindepth 2 -maxdepth 2 -type d -print0 \
  | xargs -0 -n 8 du -d0|sort -k1 -n|tail -n1|awk '{print $2}'
}

function checkDatanodeRunning() {

  DNPID=$(pgrep -f -- "-Dproc_datanode") && \
      { log_error "Cannot do anything while datanode is running (PID: ${DNPID})";\
      exit 2; }
}

function checkRunningUser() {

  id|grep -q "uid=0" && { log "Running as root user, exiting. Use --force to override"; exit 2; }
}

# moveSubdir FROM_DISK TO_DISK
function moveSubdir() {
  
  local SOURCE_DISK
  local DEST_DISK

  SOURCE_DISK=$1
  DEST_DISK=$2

  [[ "${SOURCE_DISK}" == "${DEST_DISK}" ]] && \
    { log_error "Cannot continue, source and destination disk are the same (${SOURCE_DISK})";\
      exit 2; }

  SUBDIR=$(getBiggestSubdir ${SOURCE_DISK}) 

  DEST_SUBDIR=$(dirname ${SUBDIR})
  log "Moving ${SOURCE_DISK}/${SUBDIR} to ${DEST_DISK}/${DEST_SUBDIR}"
  mkdir -p "${DEST_DISK}/${SUBDIR}" # just in case dest dir does not exist
  rsync -a --remove-source-files "${SOURCE_DISK}/${SUBDIR}" "${DEST_DISK}/${DEST_SUBDIR}"

}

function isThresholdTraspassed() {

  SMALL=$1
  BIG=$2

  (( (BIG-SMALL) * 100 / (BIG+SMALL) > BALANCE_THRESHOLD )) && return 0 || return 1
}

function getPurgeDisk() {
  for i in "${!PURGE_DISKS[@]}"; do
    if [[ ${PURGE_DISKS[i]} =~ .*/mnt/disk.* ]]; then
      cd "${PURGE_DISKS[i]}"
      find hdfs/current/BP-*/current/finalized/  -mindepth 2 -maxdepth 2 -type d -not -empty -print0 | egrep '.*' 2>&1 1>/dev/null
      if [[ $? -eq 0 ]]; then
        printf ${PURGE_DISKS[i]}
        return 0;
      fi
    fi
  done
}

function isPurgeFinish() {
  #log "DEBUG PURGE_DISKS_SIZE: ${#PURGE_DISKS[@]} "
  if [ ${#PURGE_DISKS[@]} ]; then
    for i in "${!PURGE_DISKS[@]}"; do
      #log "DEBUG while ${PURGE_DISKS[i]}"
      if [[ ${PURGE_DISKS[i]} =~ .*/mnt/disk.* ]]; then
        #log "DEBUG isPurgeFinish $i=>${PURGE_DISKS[i]}"
        cd "${PURGE_DISKS[i]}"
        find hdfs/current/BP-*/current/finalized/  -mindepth 2 -maxdepth 2 -type d -not -empty -print0 | egrep '.*'
        if [[ $? -eq 0 ]]; then
          #log "DEBUG isPurgeFinish return 0"
          return 0;
        fi
      fi
    done
  fi
  #log "DEBUG isPurgeFinish return 1"
  return 1;
}

function purgeDisks() {
  local PURGE_DISK
  local SMALLEST_DISK

  while (isPurgeFinish)
  do
    SMALLEST_DISK="$(getUsedDisk least)"
    PURGE_DISK="$(getPurgeDisk)"

    log "Purge Disk ${PURGE_DISK} to ${SMALLEST_DISK} moving data."
    moveSubdir "$PURGE_DISK" "$SMALLEST_DISK"
  done

  log "Purge Disks Finished."
  return 0
}

function balanceDisks() {

  local BIGGEST_DISK
  local SMALLEST_DISK
  local BIGGEST_DISK_SIZE
  local SMALLEST_DISK_SIZE

  BIGGEST_DISK_SIZE=$(getUsedDisk most size)
  SMALLEST_DISK_SIZE=$(getUsedDisk least size)

  while isThresholdTraspassed "$SMALLEST_DISK_SIZE" "$BIGGEST_DISK_SIZE"
  do
    BIGGEST_DISK="$(getUsedDisk most)"
    SMALLEST_DISK="$(getUsedDisk least)"
    log "${BALANCE_THRESHOLD}% threshold between ${BIGGEST_DISK} and ${SMALLEST_DISK} exceeded, balancing data."
    moveSubdir "$BIGGEST_DISK" "$SMALLEST_DISK"
    BIGGEST_DISK_SIZE="$(getUsedDisk most size)"
    SMALLEST_DISK_SIZE="$(getUsedDisk least size)"
  done
  
  log "No disks are exceeding the balance threshold."
  return 0
}

# main starts here
HDFS_CONF="/etc/ecm/hadoop-conf/hdfs-site.xml"
BALANCE_THRESHOLD=10 # in %
FORCE_RUN=0
PURGE_VOLUMES=""
while [ $# -gt 0 ]  
do
    case "$1" in
        --hdfs-config|-c)    HDFS_CONF="$2";             shift 2;;
        --threshold|-t)      BALANCE_THRESHOLD="$2";     shift 2;;
        --purge-volumes|-p)  PURGE_VOLUMES="$2";         shift 2;;
        --force|-f)          FORCE_RUN="1";              shift 1;;
        *)                   printHelp "Wrong parameter" ;;
    esac        
done

log "Starting DataNode local disks balancing, logging at ${LOGFILE:-/mnt/disk1/log/hadoop-hdfs/hdfs-dn-diskbalancer.log}"
checkDatanodeRunning
[ ${FORCE_RUN} -eq 0 ] && checkRunningUser

parseDisks
[ "${PURGE_VOLUMES}" == "" ] || purgeDisks
balanceDisks

log "DataNode local disks balancing finished"
