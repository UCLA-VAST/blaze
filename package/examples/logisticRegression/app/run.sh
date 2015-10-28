#!/bin/bash

if [ -z $FCS_RT_ROOT ]; then
  FCS_RT_ROOT=../../..
fi
if [ ! -f "$FCS_RT_ROOT/nam/bin/nam_daemon" ]; then
  echo "FCS_RT_ROOT seems incorrect, please set it to the root directory of FCS_RUNTIME"
  exit -1
fi
SPARK_HOME=$FCS_RT_ROOT/spark-1.4.0-falcon
$SPARK_HOME/bin/spark-submit --class LogisticRegression \
	--driver-memory 8G \
	--executor-memory 4G \
	--master local[*] \
	target/logistic-1.0.jar $@

