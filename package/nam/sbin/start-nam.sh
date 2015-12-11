#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
PID_DIR=/tmp
HOSTNAME=`hostname`

DAEMON_NAME=fcs_runtime-nodemanager-${USER}-${HOSTNAME}

# Environment Setup
LD_LIBRARY_PATH=$DIR/../lib:$DIR/../../extern/boost_1_55_0/lib:$LD_LIBRARY_PATH

# Check if the manager is already started
PID_FNAME=$PID_DIR/$DAEMON_NAME.pid
if [ -f $PID_FNAME ]; then
  echo "Node manager already started on $HOSTNAME"
  exit -1
fi

# Renaming log file if it already exists
LOG_FNAME=$DIR/../logs/$DAEMON_NAME.log
OLDLOG_FNAME=$LOG_FNAME
counter=0
while [ -f $OLDLOG_FNAME ]; do
  counter=$((counter + 1))
  OLDLOG_FNAME=$DIR/../logs/${DAEMON_NAME}-${counter}.log
done

if [[ "$LOG_FNAME" != "$OLDLOG_FNAME" ]]; then
  mv $LOG_FNAME $OLDLOG_FNAME
fi

# Locate the configuration file
CONF_FNAME=$DIR/../conf/acc_conf/$HOSTNAME
if [ ! -f $CONF_FNAME ]; then
  CONF_FNAME=$DIR/../conf/acc_conf/default
fi

# Start the node manager daemon
nohup $DIR/../bin/nam_daemon $CONF_FNAME > $LOG_FNAME 2>&1&
PID=$!
echo $PID > $PID_FNAME

# Wait a little while to see if the manager starts
sleep 1
if [[ ! $(ps -p "$PID" -o comm=) =~ "nam" ]]; then
  echo "Failed to launch node manager on $HOSTNAME:"
  tail "$LOG_FNAME"
  echo "Full log in $LOG_FNAME"
  rm -f $PID_FNAME
else
  # delete possible sdaccel profiling files
  rm -rf $PWD/debug_hal.log
  rm -rf $PWD/sdaccel_profile_summary.*
  echo "Node manager started on $HOSTNAME, execution log in $LOG_FNAME"
fi
