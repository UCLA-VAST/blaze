#!/bin/bash

export RDDACC_HOME=/curr/cody/Spark_FPGA/rddacc

spark-submit --class TestApp$1 \
	--jars ${RDDACC_HOME}/target/RDD_ACC-1.0-SNAPSHOT.jar \
	--master local[*] \
	target/testapp-0.0.0.jar


