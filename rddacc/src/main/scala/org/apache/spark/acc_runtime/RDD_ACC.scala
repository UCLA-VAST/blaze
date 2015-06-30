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

    var inputIter: Iterator[T] = isInMemory(split, context) match {
      case true =>
        println(split.index + " cached.")
        firstParent[T].iterator(split, context)
      case false =>
        println(split.indx + " uncached.")
        null
    }

    val outputIter = new Iterator[U] {
      val outputAry: Array[U] = new Array[U](inputAry.length)
      var idx: Int = 0
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

      if (inputIter) { // Send data from memory
        val inputAry: Array[T] = inputIter.toArray
        val mappedFileInfo = Util.serializePartition(inputAry, split.index)

        msg = transmitter.createDataMsgForMem(
            split.index, 
            mappedFileInfo._2, 
            mappedFileInfo._3,
            mappedFileInfo._1)
      }
      else { // Send HDFS file information
        val splitInfo: String = split.asInstanceOf[HadoopPartition].inputSplit.toString

        // Parse Hadoop file string: file:<path>:<offset>+<size>
        val filePath: String = splitInfo.substring(
            splitInfo.indexOf(':') + 1, splitInfo.lastIndexOf(':'))
        val fileOffset: Int = splitInfo.substring(
            splitInfo.lastIndexOf(':') + 1, splitInfo.lastIndexOf('+')).toInt
        val fileSize: Int = splitInfo.substring(
            splitInfo.lastIndexOf('+') + 1, splitInfo.length).toInt
     
        msg = transmitter.createDataMsgForPath(
            split.index, filePath, fileOffset, fileSize)
      }

      transmitter.send(msg)
      msg = transmitter.receive()

      if (msg.getType() == AccMessage.MsgType.ACCFINISH) {
        // read result
        Util.readMemoryMappedFile(outputAry, mappedFileInfo._1 + ".out")
      }
      else {
        // in case of using CPU
        println("Compute partition " + split.index + " using CPU")
        if (!inputIter)
          inputIter = firstParent[T].iterator(split, context)
        val inputAry: Array[T] = inputIter.toArray
        for (e <- inputAry) {
          outputAry(idx) = f(e.asInstanceOf[T])
          idx = idx + 1
        }
        idx = 0
      }

      def hasNext(): Boolean = {
        idx < outputAry.length
      }

      def next(): U = {
        idx = idx + 1
        outputAry(idx - 1)
      }
    }
    outputIter
  }

  def isInMemory(split: Partition, context: TaskContext): Boolean = {
    if (getStorageLevel == StorageLevel.NONE)
      false
    else {
      val splitKey = RDDBlockId(this.id, split.index)
      SparkEnv.blockManager.get(splitKey) match {
        case Some(result) =>
          true
        case None =>
          false
      }
    }
  }
}

