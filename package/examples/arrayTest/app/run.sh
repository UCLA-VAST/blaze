#!/bin/bash

$SPARK_HOME/bin/spark-submit --class TestApp \
	--driver-memory 8G \
	--executor-memory 8G \
	--master local[*] \
	target/test-0.0.0.jar $@

