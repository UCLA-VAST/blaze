#!/bin/bash
../build/mvn test -DwildcardSuites=org.apache.spark.mllib.$1 -Dtest=none
