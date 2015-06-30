package org.apache.spark.acc_runtime

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
    val splitInfo: String = split.asInstanceOf[HadoopPartition].inputSplit.toString

    // Parse Hadoop file string: file:<path>:<offset>+<size>
    val filePath: String = splitInfo.substring(
        splitInfo.indexOf(':') + 1, splitInfo.lastIndexOf(':'))
    val fileOffset: Int = splitInfo.substring(
        splitInfo.lastIndexOf(':') + 1, splitInfo.lastIndexOf('+')).toInt
    val fileSize: Int = splitInfo.substring(
        splitInfo.lastIndexOf('+') + 1, splitInfo.length).toInt

    val splitKey = RDDBlockId(this.id, split.index)
//    SparkEnv.blockManager

    val inputIter = firstParent[T].iterator(split, context)

    val outputIter = new Iterator[U] {
      val inputAry: Array[T] = inputIter.toArray
      val outputAry: Array[U] = new Array[U](inputAry.length)
      var idx: Int = 0

      // TODO: We should send either data (memory mapped file) or file path,
      // but now we just send data.
      val mappedFileInfo = Util.serializePartition(inputAry, split.index)
      val transmitter = new DataTransmitter()

      var msg: AccMessage.TaskMsg = 
        transmitter.createTaskMsg(split.index, AccMessage.MsgType.ACCREQUEST)
      transmitter.send(msg)
      msg = transmitter.receive()

      // TODO: We should retry if rejected.
      if (msg.getType() != AccMessage.MsgType.ACCGRANT)
        throw new RuntimeException("Request reject.")
      else
        println("Acquire resource, sending data...")

      msg = transmitter.createDataMsg(
          split.index, 
          mappedFileInfo._2, 
          mappedFileInfo._3,
          mappedFileInfo._1)

      transmitter.send(msg)
      msg = transmitter.receive()

      if (msg.getType() == AccMessage.MsgType.ACCFINISH) {
        // read result
        Util.readMemoryMappedFile(outputAry, mappedFileInfo._1 + ".out")
      }
      else {
        // in case of using CPU
        println("Compute partition " + split.index + " using CPU")
        for (e <- inputAry) {
          outputAry(idx) = f(e.asInstanceOf[T])
          idx = idx + 1
        }
        idx = 0
      }

      def hasNext(): Boolean = {
        idx < inputAry.length
      }

      def next(): U = {
        idx = idx + 1
        outputAry(idx - 1)
      }
    }
    outputIter
  }
}

