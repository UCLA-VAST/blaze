#!/bin/bash

SPARK_HOME=/home/ubuntu/prog/blaze/spark-1.4.0-bin-fcs
$SPARK_HOME/bin/spark-submit --class LogisticRegression \
	--driver-memory 16G \
	--executor-memory 16G \
	--master local[*] \
	target/logistic-1.0.jar $@

