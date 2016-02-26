#!/bin/bash
if [[ "$FCS_RT_ROOT" == "" ]]; then 
  FCS_RT_ROOT=../../..
fi
if [ ! -e $FCS_RT_ROOT ]; then
  echo FCS_RT_ROOT seems incorrect, please set it to the root directory of FCS_RUNTIME
fi

LD_LIBRARY_PATH=$FCS_RT_ROOT/nam/lib:$FCS_RT_ROOT/extern/boost_1_55_0/lib:$LD_LIBRARY_PATH
./app $@
