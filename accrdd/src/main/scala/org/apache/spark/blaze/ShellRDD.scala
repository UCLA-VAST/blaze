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

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.scheduler._

/**
  * ShellRDD is only used for wrapping a Spark RDD. It returns a AccRDD
  * if the developer decides to execute the computation on the accelerator 
  * by calling `map_acc` method.
  *
  * @param appId The application ID.
  * @param prev The original Spark RDD.
  */
class ShellRDD[T: ClassTag](appId: Int, prev: RDD[T]) 
  extends RDD[T](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  // do nothing
  override def compute(split: Partition, context: TaskContext) = {
    val iter = new Iterator[T] {
      val nested = firstParent[T].iterator(split, context)

      def hasNext : Boolean = {
        nested.hasNext
      }

      def next : T = {
        nested.next
      }
    }
    iter
  }

  def map_acc[U: ClassTag](clazz: Accelerator[T, U]): AccRDD[U, T] = {
    new AccRDD(appId, this, clazz)
  }

  def mapPartitions_acc[U: ClassTag](clazz: Accelerator[T, U]): AccMapPartitionsRDD[U, T] = {
    new AccMapPartitionsRDD(appId, this, clazz)
  }
}

