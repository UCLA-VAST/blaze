package org.apache.spark.acc_runtime

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.scheduler._
import org.apache.spark.broadcast._

class ACCRuntime(sc: SparkContext) extends Logging {

  var BroadcastList: List[Broadcast_ACC[_]] = List()

  def stop() = {
    sc.stop
  }

  def wrap[T: ClassTag](rdd : RDD[T]) : ShellRDD[T] = {
    new ShellRDD[T](rdd)
  }

  def wrap[T: ClassTag](bd : Broadcast[T]) : Broadcast_ACC[T] = {
    val newBrdcst = new Broadcast_ACC[T](bd)
    BroadcastList = BroadcastList :+ newBrdcst
    newBrdcst
  }
}

