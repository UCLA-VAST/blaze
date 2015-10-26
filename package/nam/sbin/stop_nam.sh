#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
PID_DIR=/tmp
HOSTNAME=`hostname`
DAEMON_NAME=falcon_runtime-nodemanager-$USER-$HOSTNAME
PID_FNAME=$PID_DIR/$DAEMON_NAME.pid

if [ -f $PID_FNAME ]; then
  PID=`cat $PID_FNAME` 
  rm -f $PID_FNAME
  if [ -z "$PID" ]; then
    echo "no node manager is running on $HOSTNAME"
  else 
    kill $PID
    echo "node manager on $HOSTNAME is stopped"
  fi
else
  echo "no node manager is running on $HOSTNAME"
fi



