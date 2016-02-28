#!/bin/bash

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
export FCS_RT_ROOT=$SCRIPT_DIR

export PATH=$FCS_RT_ROOT/bin:$PATH
export LD_LIBRARY_PATH=$FCS_RT_ROOT/nam/lib:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$FCS_RT_ROOT/extern/boost_1_55_0/lib:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$FCS_RT_ROOT/extern/googletools/lib:$LD_LIBRARY_PATH
