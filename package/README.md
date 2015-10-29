## Quick Start

### Installation

* Software prerequisite
    * Linux x86_64
    * Java
    * Apache Maven
    * (Optional) Xilinx SDAccel 2015.1.5 (FPGA accelerator test)

* Software setup: execute `install.sh` after unpack
* License setup: ensure correct path to the FCS license is configured:  
  `export LM_LICENSE_FILE=port@host:$LM_LICENSE_FILE`

### Run example Spark application locally

Assuming the FCS Runtime System is installed in `$FCSROOT`, the examples are
located at $FCSROOT/examples. Below is a step-by-step to test an example 
application `$EXAMPLE`:

1. Compile Spark application
    * run `mvn package` in $FCSROOT/examples/$EXAMPLE/app
    * use `run-local.sh` to validate application using local Spark on CPU

2. Setup accelerator
    * (Optional) Compile accelerator task with `make` if *.so file does not exist in 
      $FCSROOT/examples/$EXAMPLE/acc_task
    * Setup accelerator configuration file for node manager:  
      run the `$FCSROOT/bin/fcs-setup-example` in 
      $FCSROOT/examples/$EXAMPLE/acc_task folder

3. Start node manager 
    * (Optional) To use FPGA accelerator in the example, please ensure *libxilinxopencl.so*
      from Xilinx SDAccel is in LD_LIBRARY_PATH search paths
    * Start node manager: `$FCSROOT/nam/sbin/start-nam.sh`
    * (Optional) Check log file to see if node manager is successfully launched, the log is
      available at $ROOTDIR/nam/logs/falcon_runtime_nodemanager-$USER-$HOSTNAME.log

4. Run Spark application
    * Under folder $FCSROOT/examples/logisticRegression/app, execute the following command:  
      ./run-local.sh <option list>

5. Stop node manager: `$FCSROOT/nam/sbin/stop-nam.sh`
