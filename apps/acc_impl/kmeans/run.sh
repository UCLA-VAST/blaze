#!/bin/bash

if [[ $# != 1 ]]; then
    echo usage: run.sh niters
    exit 1
fi

spark-submit --class SparkKMeans \
        --jars ${ACC_HOME}/target/ACC_RUNTIME-1.0-SNAPSHOT.jar \
        --master local[*] \
        target/sparkkmeans-0.0.0.jar \
        3 $1 hdfs://cdsc0:9000/user/cody/kmeans_input_small.txt
