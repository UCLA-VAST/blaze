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
    val input_iter = firstParent[T].iterator(split, context)

    val output_iter = new Iterator[U] {
      var iter: Iterator[U] = null

      // in case of using CPU
      println("Compute partition " + split.index + " using CPU")
      iter = input_iter.map(e => f(e))

      def hasNext(): Boolean = {
        iter.hasNext
      }

      def next(): U = {
        iter.next
      }
    }
    output_iter
  }
}

