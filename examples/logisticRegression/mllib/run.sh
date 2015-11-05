#!/bin/bash

#/curr/diwu/prog/blaze/package/spark-1.4.0-falcon/bin/spark-submit --class LogisticRegression \
#/curr/diwu/prog/blaze/spark-1.4.0/bin/spark-submit --class LogisticRegression \
/curr/diwu/tools/spark-1.4.0/bin/spark-submit --class LogisticRegression \
	--driver-memory 8G \
	--executor-memory 4G \
	--master local[*] \
	target/logistic-1.0.jar $@

