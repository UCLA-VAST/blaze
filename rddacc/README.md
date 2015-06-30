=== Setup environment variable ===
source setup.sh

=== Issues ===
#1: Avoid to fetch data from HDFS (disk)
	In order to reduce the overhead of transmitting data from HDFS to Spark, we would like to send HDFS file path to NAM instead of data. To do so, we need to check if the data has been maintained by block manager or not in advance. However, the actions of check and fetch blocks are implemented in RDD.iterator method, which cannot be overwritten.

	Solution plan: define a new method "inMemoryCheck" to check if the block is in the block manager. If yes, then call RDD.iterator, or just send the file path and offset to the NAM.

	Drawback: Since RDD.iterator also needs to check the block, we will have an additional checking overhead if the block is cached.

	Status: Working.

#2: Implicit broadcast inputs
	There have some input data that referred by the map function. Spark broadcasts these input data to all workers automatically before executing the map function. However, there has no way for a RDD to realize them so that we do not know which input data should be broadcast followed by partitions.

	Solution plan 1: Require user to write broadcast explicitly.

	Drawback: The user has responsibility to write broadcast for all necessary referred data. The NAM will encounter errors if the user misses to write some of them.

	Solution plan 2: Leverage Aparapi front-end to analyze the necessary inputs.

	Drawback: 1. Migrate and adapt an Aparapi front-end needs some efforts. 
						2. Analyze and extract required inputs cause overhead.

