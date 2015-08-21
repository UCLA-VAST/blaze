# Blaze: Deploying Accelerators at Datacenter Scale
### Description
Blaze is an accelerator-aware programming framework for warehouse-scale accelerator deployment. Blaze provides a programming interface that is compatible to Spark, an in-memory compute engine for large-scale data processing, and a runtime system that provides transparent accelerator management. With Blaze, the deployment effort of accelerator task is reduced by more than 10x compared to traditional frameworks such as OpenCL. Blaze is designed to efficiently manage accelerator platforms with intelligent data caching and task pipelining schemes to minimize the communication and data movement overheads.

### Installing Blaze
0. **Prerequisites**
    0. Boost (tested with 1.55.0)
    0. Google Protobuf (tested with 2.5.0)
    0. Apache Spark (tested with 1.3.1)
    0. Apache Hadoop (tested with 2.5.2)
0. **Compiling**
    0. Edit Makefile.config
    0. Edit setup.sh and then `source setup.sh`
    0. run `make`

### Running Loopback example
0. **Prerequisites**
    0. Intel AALSDK 4.1.7
    0. Intel AAL NLB example RTL
0. **Compiling**
    0. `cd examples/loopback`
    0. To compile Spark program: `cd app; mvn package`
    0. To compile ACCTask: `cd acc_task; make`
0. **Execution**
    0. Start NLB ASE
    0. On terminal 1 start Manager: `./manager/bin/acc_manager ./examples/loopback/acc_task/conf.prototxt`
    0. On terminal 2 tart Spark program: `./examples/loopback/app/run.sh` or `./examples/loopback/app/run_local.sh`

### Contacts
For any question or discussion, please contact the authors:
  * Di Wu: allwu@cs.ucla.edu
  * Hao Yu: comaniac0422@gmail.com

  
