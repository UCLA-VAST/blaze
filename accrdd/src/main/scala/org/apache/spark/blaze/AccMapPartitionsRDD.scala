/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.blaze

import java.io._
import java.util.ArrayList     
import java.nio.ByteBuffer     
import java.nio.ByteOrder      

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._
import scala.collection.mutable._

import org.apache.spark._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.scheduler._
import org.apache.spark.util.random.RandomSampler

/**
  * A RDD that uses accelerator to accelerate the computation. The behavior of AccMapPartitionsRDD is 
  * similar to Spark MapPartitionsRDD which performs the computation for a whole partition at a
  * time. AccMapPartitionsRDD also processes a partition to reduce the communication overhead.
  *
  * @param appId The unique application ID of Spark.
  * @param prev The original RDD of Spark.
  * @param acc The developer extended accelerator class.
  */
class AccMapPartitionsRDD[U: ClassTag, T: ClassTag](
  appId: String, 
  prev: RDD[T], 
  acc: Accelerator[T, U], 
  port: Int,
  sampler: RandomSampler[Int, Int]
) extends AccRDD[U, T](appId, prev, acc, port, sampler) with Logging {

  /**
    * In case of failing to execute the computation on accelerator, 
    * Blaze will execute the computation on JVM instead to guarantee the
    * functionality correctness, even the overhead of failing to execute 
    * on accelerator is quite large.
    *
    * @param split The partition to be executed on JVM.
    * @param context TaskContext of Spark.
    * @return The output array
    */
  override def computeOnJTP(split: Partition, context: TaskContext, partitionMask: Array[Byte]): Iterator[U] = {
    logInfo("Compute partition " + split.index + " using CPU")
    val input: Iterator[T] = firstParent[T].iterator(split, context)

    if (partitionMask != null) { // Sampled dataset
      val sampledInput = new Iterator[T] {
        val inputAry = input.toArray
        var sampledList = List[T]()
        var idx = 0
        while (idx < inputAry.length) {
          if (partitionMask(idx) != 0)
            sampledList = sampledList :+ inputAry(idx)
          idx += 1
        }
        idx = 0

        def hasNext : Boolean = {
          idx < sampledList.length
        }

        def next : T = {
          idx += 1
          sampledList(idx - 1)
        }
      }
      acc.call(sampledInput)
    }
    else {
      logWarning("Partition " + split.index + " has no mask")
      acc.call(input)
    }
  }
}

