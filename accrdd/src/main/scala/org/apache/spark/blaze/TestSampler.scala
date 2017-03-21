package org.apache.spark.blaze

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.collection.mutable._

import org.apache.spark._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.util.random._

abstract class TestSampler[T] extends RandomSampler[T, T] {

  override def setSeed(r: Long) = {}

  override def sample(items: Iterator[T]): Iterator[T] = {
    var out: List[T] = List()
    val N: Int = 30
    var idx: Int = 0

    while (items.hasNext) {
      val v: T = items.next
      if (idx < N)
        out = out :+ v
      idx += 1
    }
    out.iterator
  }

  //override def clone = new TestSampler[T]
}
