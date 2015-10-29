#!/bin/bash

echo "Setting up FCS Runtime System..."

echo "#1 Unpack Hadoop..."
if tar zxf hadoop-2.6.0-bin-fcs.tar.gz; then 
  rm -rf hadoop-2.6.0-bin-fcs.tar.gz
else
  echo "Failed to unpack hadoop-2.6.0-bin-fcs.tar.gz"
  exit -1
fi 

echo "#2 Unpack Spark..."
if tar zxf spark-1.4.0-bin-fcs.tar.gz; then 
  rm -rf spark-1.4.0-bin-fcs.tar.gz
else
  echo "Failed to unpack spark-1.4.0-bin-fcs.tar.gz"
  exit -1
fi 

echo "#3 Unpack Boost Library..."
if tar zxf boost_1_55_0.tar.gz; then 
  rm -rf boost_1_55_0.tar.gz
else
  echo "Failed to unpack boost_1_55_0.tar.gz"
  exit -1
fi 

echo "FCS Runtime System is successfully installed!"
echo "Below is a quick start on the provided examples"
cat README
