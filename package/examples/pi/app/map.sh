#!/bin/bash

FCS_RT_ROOT=[FCSROOT]

if [ ! -e $FCS_RT_ROOT/nam/bin ]; then
  echo "FCS_RT_ROOT is not set properly"
  exit -1
fi

LD_LIBRARY_PATH=$FCS_RT_ROOT/nam/lib:$FCS_RT_ROOT/extern/boost_1_55_0/lib:$FCS_RT_ROOT/extern/googletools/lib:$LD_LIBRARY_PATH
./map-bin $@
