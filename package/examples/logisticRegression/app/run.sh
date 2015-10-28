#!/bin/bash

SPARK_HOME=../../../spark-1.4.0-falcon
$SPARK_HOME/bin/spark-submit --class LogisticRegression \
	--driver-memory 8G \
	--executor-memory 4G \
	--master local[*] \
	target/logistic-1.0.jar $@

