package org.apache.spark.acc_runtime

import java.io.OutputStream    
import java.io.FileOutputStream
import java.util.ArrayList     
import java.nio.ByteBuffer     
import java.nio.ByteOrder      

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._

import org.apache.spark._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.scheduler._

class RDD_ACC[U:ClassTag, T: ClassTag](prev: RDD[T], f: T => U) 
  extends RDD[U](prev) {

  def getRDD() = this

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) = {
    val numBlock = 1 // FIXME: Assume 1 at this time
    val splitInfo: String = split.asInstanceOf[HadoopPartition].inputSplit.toString

    // Parse Hadoop file string: file:<path>:<offset>+<size>
    val filePath: String = splitInfo.substring(
        splitInfo.indexOf(':') + 1, splitInfo.lastIndexOf(':'))
    val fileOffset: Int = splitInfo.substring(
        splitInfo.lastIndexOf(':') + 1, splitInfo.lastIndexOf('+')).toInt
    val fileSize: Int = splitInfo.substring(
        splitInfo.lastIndexOf('+') + 1, splitInfo.length).toInt

    val inputIter = firstParent[T].iterator(split, context)
/*
    var inputIter: Iterator[T] = inMemoryCheck(split, context) match {
      case true =>
        println(split.index + " cached.")
        firstParent[T].iterator(split, context)
      case false =>
        println(split.index + " uncached.")
        null
    }
*/
    val outputIter = new Iterator[U] {
      val N: Int = fileSize / Util.getTypeSizeByRDD(getRDD())
      val outputAry: Array[U] = new Array[U](N)
      var idx: Int = 0
      val transmitter = new DataTransmitter()

      var msg = transmitter.buildRequest(split.index)
      transmitter.send(msg)
      var revMsg = transmitter.receive()

      // TODO: We should retry or use CPU if rejected.
      if (revMsg.getType() != AccMessage.MsgType.ACCGRANT)
        throw new RuntimeException("Request reject.")
      else
        println("Acquire resource, sending data...")

      val dataMsg = transmitter.buildData(split.index)

      var i: Int = 0
      var blockFileName: Array[String] = new Array[String](revMsg.getDataCount())
      while (i < numBlock) {
        if (!revMsg.getData(i).getCached()) {
          if (inputIter != null) { // Send data from memory
            val inputAry: Array[T] = inputIter.toArray
            val mappedFileInfo = Util.serializePartition(inputAry, split.index)
            blockFileName(i) = mappedFileInfo._1
            println("Mapped file name " + blockFileName(i))

            transmitter.addData(
                dataMsg,
                split.index * 100 + i, // id
                N, // width
                mappedFileInfo._2, // size
                0, // offset
                blockFileName(i) + "!" // path
            )
          }
          else { // Send HDFS file information     
            transmitter.addData(
                dataMsg, split.index * 100 + i, N, fileSize, fileOffset, filePath)
          }
        }
        i = i + 1
      }
      transmitter.send(dataMsg)
      revMsg = transmitter.receive()

      if (revMsg.getType() == AccMessage.MsgType.ACCFINISH) {
        // read result
        i = 0
        while (i < numBlock) {
          Util.readMemoryMappedFile(outputAry, blockFileName(i) + ".out")
          i = i + 1
        }
      }
      else {
        // in case of using CPU
        println("Compute partition " + split.index + " using CPU")
//        if (inputIter == null)
//          inputIter = firstParent[T].iterator(split, context)
        val inputAry: Array[T] = inputIter.toArray
        i = 0
        for (e <- inputAry) {
          outputAry(i) = f(e.asInstanceOf[T])
          i = i + 1
        }
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

  def inMemoryCheck(split: Partition, context: TaskContext): Boolean = {
    if (getStorageLevel == StorageLevel.NONE)
      false
    else {
    // FIXME
//      val splitKey = RDDBlockId(this.id, split.index)
//      SparkEnv.get.cacheManager.blockManager.get(splitKey) match {
//        case Some(result) =>
          true
//        case None =>
//          false
//      }
    }
  }
}

