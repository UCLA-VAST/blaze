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

### Run example Spark applications locally

Assuming the FCS Kestrel Runtime System is installed in `$FCS_RT_ROOT`, the examples are
located at `$FCS_RT_ROOT/examples`. Below is a step-by-step to test an example 
application `$EXAMPLE`:

1. Compile Spark application
    * run `mvn package` in `$FCS_RT_ROOT/examples/$EXAMPLE/app`
    * use `run-local.sh` to validate application using local Spark on CPU

2. Setup accelerator
    * (Optional) Compile accelerator task with `make` if \*.so file does not exist in  
      `$FCS_RT_ROOT/examples/$EXAMPLE/acc_task`
    * Setup accelerator configuration file for node manager:  
      run `$FCS_RT_ROOT/bin/fcs-setup-example` in  
      `$FCS_RT_ROOT/examples/$EXAMPLE/acc_task`

3. Start node manager 
    * (Optional) To use FPGA accelerator in the example, please ensure *libxilinxopencl.so*
      from Xilinx SDAccel is in LD_LIBRARY_PATH search paths
    * Start node manager:  
      `$FCS_RT_ROOT/nam/sbin/start-nam.sh`
    * (Optional) Check log file to see if node manager is successfully launched, the log is
      available at `$FCS_RT_ROOT/nam/logs/falcon_runtime_nodemanager-$USER-$HOSTNAME.log`

4. Run Spark application
    * Under folder `$FCS_RT_ROOT/examples/$EXAMPLE/app`, execute:  
      `./run-local.sh <argument list>`

5. Stop node manager: `$FCS_RT_ROOT/nam/sbin/stop-nam.sh`
