
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
}

