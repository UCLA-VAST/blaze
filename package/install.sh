#!/bin/bash

HADOOP_VERSION=2.6.0
SPARK_VERSION=1.5.1

echo "Setting up Falcon Computing Solutions (FCS) Runtime System..."

echo "#1 Unpacking Hadoop..."
HADOOP_DIR=hadoop-${HADOOP_VERSION}-bin-fcs
mkdir $HADOOP_DIR
if tar zxf ${HADOOP_DIR}.tar.gz -C $HADOOP_DIR --strip-components 1; then 
  rm -rf ${HADOOP_DIR}.tar.gz
else
  echo "Failed to unpack hadoop-2.6.0-bin-fcs.tar.gz"
  exit -1
fi 

echo "#2 Unpacking Spark..."
SPARK_DIR=spark-${SPARK_VERSION}-bin-fcs
mkdir $SPARK_DIR
if tar zxf ${SPARK_DIR}.tar.gz -C $SPARK_DIR --strip-components 1; then 
  rm -rf ${SPARK_DIR}.tar.gz
else
  echo "Failed to unpack spark-1.4.0-bin-fcs.tar.gz"
  exit -1
fi 

echo "#3 Unpacking External Libraries..."
mkdir extern
if tar zxf boost_1_55_0.tar.gz; then 
  mv boost_1_55_0 extern
  rm -rf boost_1_55_0.tar.gz
else
  echo "Failed to unpack boost_1_55_0.tar.gz"
  exit -1
fi 
if tar zxf googletools.tar.gz; then 
  mv googletools extern
  rm -rf googletools.tar.gz
else
  echo "Failed to unpack boost_1_55_0.tar.gz"
  exit -1
fi

echo "#4 Configuring examples..."
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
ROOTDIR=$SCRIPT_DIR

EXAMPLE_DIR=$ROOTDIR/examples/pi/app

sed -i "s/\[FCSROOT\]/$(echo $ROOTDIR | sed -e 's/[\/&]/\\&/g')/g" $ROOTDIR/nam/conf/acc_conf/default
sed -i "s/\[FCSROOT\]/$(echo $ROOTDIR | sed -e 's/[\/&]/\\&/g')/g" $EXAMPLE_DIR/map.sh
sed -i "s/\[FCSROOT\]/$(echo $ROOTDIR | sed -e 's/[\/&]/\\&/g')/g" $EXAMPLE_DIR/reduce.sh

echo "FCS Runtime System is successfully installed!"
echo "Please refer to the quick start for examples at README.md"

rm $0
