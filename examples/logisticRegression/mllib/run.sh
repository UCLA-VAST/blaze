#!/bin/bash

/curr/diwu/prog/spark-1.4.1/bin/spark-submit --class LogisticRegression \
	--driver-memory 16G \
	--executor-memory 16G \
	--master local[*] \
	target/logistic-1.0.jar $@

