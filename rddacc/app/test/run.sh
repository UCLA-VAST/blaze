#!/bin/bash

export ACC_HOME=/curr/cody/Spark_ACC/acc_runtime/rddacc

spark-submit --class TestApp$1 \
	--jars ${ACC_HOME}/target/RDD_ACC-1.0-SNAPSHOT.jar \
	--master local[*] \
	target/testapp-0.0.0.jar


