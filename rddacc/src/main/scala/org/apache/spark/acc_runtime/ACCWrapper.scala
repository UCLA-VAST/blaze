package org.apache.spark.acc_runtime

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.scheduler._
import org.apache.spark.broadcast._

object ACCWrapper {
  def wrap[T: ClassTag](rdd : RDD[T]) : RDD_FAKE[T] = {
    new RDD_FAKE[T](rdd)
  }

  def wrap[T: ClassTag](bd : Broadcast[T]) : Broadcast_ACC[T] = {
    new Broadcast_ACC[T](bd)
  }
}

