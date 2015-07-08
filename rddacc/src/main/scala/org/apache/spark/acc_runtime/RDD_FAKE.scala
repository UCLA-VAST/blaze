package org.apache.spark.acc_runtime

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

  def broadcast_acc[U:ClassTag](value: U) = {
    val isArray: Boolean = value.getClass.getName.contains("[I")
    val typeName = (isArray) match {
      case true =>  value.asInstanceOf[Array[_]](0).getClass.getName
      case false => value.getClass.getName
    }

    if (isArray)
      println("array")

    if (typeName.contains("java.lang")) {
      if (typeName.contains("String"))
        println("broadcast string")
      else
        println("broadcast primitive")
    }
    else if (typeName.contains("org.apache.spark.rdd") || typeName.contains("org.apache.spark.acc_runtime.RDD")) {
      println("broadcast RDD")
    }
    else { // Other unsupported objects
      throw new RuntimeException("Unsupported object type " + typeName + " cannot be broadcast to accelerators")
    }
  }

  def map_acc[U:ClassTag](f: T => U): RDD_ACC[U, T] = {
    val cleanF = sparkContext.clean(f)
    new RDD_ACC[U, T](this, cleanF)
  }
}

