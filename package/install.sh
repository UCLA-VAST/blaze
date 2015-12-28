#!/bin/bash

echo "Setting up Falcon Computing Solutions (FCS) Runtime System..."

echo "#1 Unpacking Hadoop..."
if tar zxf hadoop-2.6.0-bin-fcs.tar.gz; then 
  rm -rf hadoop-2.6.0-bin-fcs.tar.gz
else
  echo "Failed to unpack hadoop-2.6.0-bin-fcs.tar.gz"
  exit -1
fi 

echo "#2 Unpacking Spark..."
if tar zxf spark-1.4.0-bin-fcs.tar.gz; then 
  rm -rf spark-1.4.0-bin-fcs.tar.gz
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

echo "FCS Runtime System is successfully installed!"
echo "Please refer to the quick start for examples at README.md"

rm $0
