package org.apache.spark.rddacc

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.scheduler._

class RDD_FAKE[T: ClassTag](prev: RDD[T]) 
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

  def map_acc[U:ClassTag](f: T => U): RDD[U] = {
    val cleanF = sparkContext.clean(f)
    new RDD_ACC[U, T](this, cleanF)
  }
}

object ACCWrapper {
  def wrap[T: ClassTag](rdd : RDD[T]) : RDD_FAKE[T] = {
    new RDD_FAKE[T](rdd)
  }
}

