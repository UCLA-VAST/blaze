#!/bin/bash

if [[ $# != 1 ]]; then
    echo usage: run.sh niters
    exit 1
fi

spark-submit --class SparkKMeans \
        --master local[*] \
        target/sparkkmeans-0.0.0.jar \
        3 $1 /curr/cody/Spark_ACC/acc_runtime/rddacc/app/kmeans/input.txt
