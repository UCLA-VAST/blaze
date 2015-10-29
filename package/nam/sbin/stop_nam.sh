#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
PID_DIR=/tmp
HOSTNAME=`hostname`
DAEMON_NAME=fcs_runtime-nodemanager-$USER-$HOSTNAME
PID_FNAME=$PID_DIR/$DAEMON_NAME.pid

if [ -f $PID_FNAME ]; then
  PID=`cat $PID_FNAME` 
  rm -f $PID_FNAME
  if [ -z "$PID" ]; then
    echo "No node manager is running on $HOSTNAME"
  else 
    kill $PID
    echo "Node manager on $HOSTNAME is stopped"
  fi
else
  echo "No node manager is running on $HOSTNAME"
fi



