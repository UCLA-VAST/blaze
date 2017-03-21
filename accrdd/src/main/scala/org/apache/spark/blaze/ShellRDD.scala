package org.apache.spark.blaze

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.collection.mutable._

import org.apache.spark._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.scheduler._
import org.apache.spark.util.random._

/**
  * ShellRDD is only used for wrapping a Spark RDD. It returns a AccRDD
  * if the developer decides to execute the computation on the accelerator 
  * by calling `map_acc` method.
  *
  * @param appId The application ID.
  * @param prev The original Spark RDD.
  */
class ShellRDD[T: ClassTag](
  appId: String, 
  prev: RDD[T],
  port: Int,
  sampler: RandomSampler[Int, Int]
) extends RDD[T](prev) {

  def this(id: String, prev: RDD[T], port: Int) = this(id, prev, port, null)

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
    new AccRDD(appId, this, clazz, port, sampler)
  }

  def sample_acc (
    withReplacement: Boolean,
    fraction: Double,
    seed: Long = Util.random.nextLong): ShellRDD[T] = { 
    require(fraction >= 0.0, "Negative fraction value: " + fraction)

    var thisSampler: RandomSampler[Int, Int] = null

    if (withReplacement)
      thisSampler = new PoissonSampler[Int](fraction)
    else
      thisSampler = new BernoulliSampler[Int](fraction)
    thisSampler.setSeed(seed)

    //if (seed == 904401792) { // Test mode
    //  thisSampler = new TestSampler[Int]
    //}

    new ShellRDD(appId, this, port, thisSampler)
  }

  def mapPartitions_acc[U: ClassTag](clazz: Accelerator[T, U]): AccMapPartitionsRDD[U, T] = {
    new AccMapPartitionsRDD(appId, this, clazz, port, sampler)
  }
}

