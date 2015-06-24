package org.apache.spark.rddacc

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.scheduler._

class RDD_ACC[U:ClassTag, T: ClassTag](prev: RDD[T], f: T => U) 
  extends RDD[U](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) = {
    println("Compute partition " + split.index + " using accelerator")
    val iter = new Iterator[U] {
      val nested = firstParent[T].iterator(split, context)

      def hasNext : Boolean = {
        nested.hasNext
      }

      def next : U = {
        f(nested.next)
      }
    }
    iter
  }

  // do the samething: force to use accelerators
  override def map[V:ClassTag](f: U => V): RDD[V] = {
    new RDD_ACC(this, sparkContext.clean(f))
  }

  def map_acc[V:ClassTag](f: U => V): RDD[V] = {
    new RDD_ACC(this, sparkContext.clean(f))
  }

}

