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

  def getPrevRDD() = prev
  def getRDD() = this

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) = {
    val numBlock: Int = 1 // Now we just use 1 block
    val blockId = new Array[Int](numBlock)
    var ii = 0
    while (ii < numBlock) {
      blockId(ii) = Util.getBlockID(getRDD.id, split.index, ii)
      ii = ii + 1
    }

    val splitInfo: String = split.asInstanceOf[HadoopPartition].inputSplit.toString

    // Parse Hadoop file string: file:<path>:<offset>+<size>
    val filePath: String = splitInfo.substring(
        splitInfo.indexOf(':') + 1, splitInfo.lastIndexOf(':'))
    val fileOffset: Int = splitInfo.substring(
        splitInfo.lastIndexOf(':') + 1, splitInfo.lastIndexOf('+')).toInt
    val fileSize: Int = splitInfo.substring(
        splitInfo.lastIndexOf('+') + 1, splitInfo.length).toInt

    val typeSize: Int = Util.getTypeSizeByRDD(getRDD())

    val isCached = inMemoryCheck(split)

    val outputIter = new Iterator[U] {
      var outputAry: Array[U] = null // Length is unknown before read the input
      var idx: Int = 0
      var dataLength: Int = -1
      val transmitter = new DataTransmitter()

      var msg = transmitter.buildRequest("SimpleAddition" /*FIXME: Accelerator ID*/, blockId)
      var startTime = System.nanoTime
      transmitter.send(msg)
      var revMsg = transmitter.receive()
      var elapseTime = System.nanoTime - startTime
      println("Communication latency: " + elapseTime + " ns")

      // TODO: We should retry or use CPU if rejected.
      if (revMsg.getType() != AccMessage.MsgType.ACCGRANT)
        throw new RuntimeException("Request reject.")

//      startTime = System.nanoTime

      val dataMsg = transmitter.buildMessage(AccMessage.MsgType.ACCDATA)

      var i = 0
      while (i < numBlock) {
        if (!revMsg.getData(i).getCached()) {
          if (isCached == true) { // Send data from memory
            val inputAry: Array[T] = (firstParent[T].iterator(split, context)).toArray
            val mappedFileInfo = Util.serializePartition(inputAry, blockId(i))
            dataLength = dataLength + mappedFileInfo._2 // We know element # by reading the file
            transmitter.addData(dataMsg, blockId(i), mappedFileInfo._2,
                mappedFileInfo._2 * typeSize, 0, mappedFileInfo._1)
          }
          else { // Send HDFS file information: unknown length
            transmitter.addData(dataMsg, blockId(i), -1, 
                fileSize, fileOffset, filePath)
          }
        }
        i = i + 1
      }
//      elapseTime = System.nanoTime - startTime
//      println("Preprocess time: " + elapseTime + " ns")

      transmitter.send(dataMsg)
      revMsg = transmitter.receive()

      if (revMsg.getType() == AccMessage.MsgType.ACCFINISH) {
        // set length
        i = 0
        dataLength = 0
        val subLength = new Array[Int](numBlock)
        while (i < numBlock) {
          subLength(i) = revMsg.getData(i).getLength()
          dataLength = dataLength + subLength(i)
          i = i + 1
        }
        outputAry = new Array[U](dataLength)

        startTime = System.nanoTime
        // read result
        i = 0
        idx = 0
        while (i < numBlock) { // We just simply concatenate all blocks
//          println(split.index + " reads result from " + revMsg.getData(i).getPath())
          Util.readMemoryMappedFile(outputAry, idx, subLength(i), revMsg.getData(i).getPath())
          idx = idx + subLength(i)
          i = i + 1
        }
        idx = 0
//        elapseTime = System.nanoTime - startTime
//        println("Read memory mapped file time: " + elapseTime + " ns")
      }
      else {
        // in case of using CPU
        println("Compute partition " + split.index + " using CPU")
        val inputAry: Array[T] = (firstParent[T].iterator(split, context)).toArray
        dataLength = inputAry.length
        outputAry = new Array[U](dataLength)
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

  def map_acc[V:ClassTag](f: U => V): RDD[V] = {
    val cleanF = sparkContext.clean(f)
    new RDD_ACC[V, U](this, cleanF)
  }

  def inMemoryCheck(split: Partition): Boolean = { 
    val splitKey = RDDBlockId(getPrevRDD.id, split.index)
    val result = SparkEnv.get.blockManager.getStatus(splitKey)
    if (result.isDefined && result.get.isCached == true) {
      true
    }
    else {
      false
    }
  }
}

