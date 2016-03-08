#!/bin/tcsh
set called=($_)
#if ("$called" != "") then
#    echo "sourced $called[2]"    # the script was sourced from this location
#endif
set rootdir = `dirname "$called[2]"`
set script_dir = `cd $rootdir; pwd`

setenv FCS_RT_ROOT $script_dir

setenv PATH $FCS_RT_ROOT/bin:$PATH
setenv LD_LIBRARY_PATH $FCS_RT_ROOT/nam/lib:$LD_LIBRARY_PATH
setenv LD_LIBRARY_PATH $FCS_RT_ROOT/extern/boost_1_55_0/lib:$LD_LIBRARY_PATH
setenv LD_LIBRARY_PATH $FCS_RT_ROOT/extern/googletools/lib:$LD_LIBRARY_PATH
