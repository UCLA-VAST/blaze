#!/bin/bash

FCS_RT_ROOT=../../..
LD_LIBRARY_PATH=$FCS_RT_ROOT/nam/lib:$FCS_RT_ROOT/extern/boost_1_55_0/lib:$LD_LIBRARY_PATH
./map-bin $@
