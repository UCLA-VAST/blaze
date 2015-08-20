Description
=======
Blaze is an accelerator-aware programming framework for warehouse-scale accelerator deployment. Blaze provides a programming interface that is compatible to Spark, an in-memory compute engine for large-scale data processing, and a runtime system that provides transparent accelerator management. With Blaze, the deployment effort of accelerator task is reduced by more than 10x compared to traditional frameworks such as OpenCL. Blaze is designed to efficiently manage accelerator platforms with intelligent data caching and task pipelining schemes to minimize the communication and data movement overheads.

Environment setup
=======
- source setup.sh
- Add JDK library (for example: /jdk1.7.0_79/jre/lib/amd64/server) to LD_LIBRARY_PATH

How to Use
=======
- Build

./build.sh

- Run the sample application
