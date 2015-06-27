package org.apache.spark.rddacc

import java.io.OutputStream    
import java.io.FileOutputStream
import java.util.ArrayList     
import java.nio.ByteBuffer     
import java.nio.ByteOrder      

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
      val inputAry: Array[T] = input_iter.toArray
      val outputAry: Array[U] = new Array[U](inputAry.length)
      var idx: Int = 0

      val mappedFile: String = Util.serializePartition(inputAry, split.index)
      println("Create memory mapped file " + mappedFile)
//      var msg: AccMessage.TaskMsg = DataTransmitter.createMsg(split.index)
//      DataTransmitter.send(msg)
//      msg = DataTransmitter.receive()

      // in case of using CPU
      println("Compute partition " + split.index + " using CPU")
      for (e <- inputAry) {
        outputAry(idx) = f(e.asInstanceOf[T])
        idx = idx + 1
      }
      idx = 0

      def hasNext(): Boolean = {
        idx < inputAry.length
      }

      def next(): U = {
        idx = idx + 1
        outputAry(idx - 1)
      }
    }
    output_iter
  }
}

