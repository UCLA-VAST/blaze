package org.apache.spark.acc_runtime

import java.io._
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

class AccRDD[U: ClassTag, T: ClassTag](appId: Int, prev: RDD[T], acc: Accelerator[T, U]) 
  extends RDD[U](prev) {

  def getPrevRDD() = prev
  def getRDD() = this

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) = {
    val numBlock: Int = 1 // Now we just use 1 block
    var j: Int = 0 // Don't use "i" or you will encounter an unknown compiler error.

    // Generate normal block ID array
    val blockId = new Array[Long](numBlock)
    while (j < numBlock) {
      blockId(j) = Util.getBlockID(appId, getPrevRDD.id, split.index, j)
      j = j + 1
    }
    j = 0

    // Generate broadcast block ID array (set later)
    val brdcstId = new Array[Long](acc.getArgNum)

    val isCached = inMemoryCheck(split)

    val outputIter = new Iterator[U] {
      var outputAry: Array[U] = null // Length is unknown before reading the input
      var idx: Int = 0
      var dataLength: Int = -1
      val typeSize: Int = Util.getTypeSizeByRDD(getRDD())

      try {
        if (typeSize == -1)
          throw new RuntimeException("Cannot recognize RDD data type")

        // Set broadcast block info and check available
        for (j <- 0 until brdcstId.length) {
          val arg = acc.getArg(j)
          if (arg.isDefined == false)
            throw new RuntimeException("Argument index " + j + " is out of range.")
          if (arg.get.isBroadcast == false)
            throw new RuntimeException("Broadcast data is not prepared.")

          brdcstId(j) = arg.get.brdcst_id
        }

        val transmitter = new DataTransmitter()
        if (transmitter.isConnect == false)
          throw new RuntimeException("Connection refuse.")
        var msg = transmitter.buildRequest(acc.id, blockId, brdcstId)
//        var startTime = System.nanoTime
        transmitter.send(msg)
        var revMsg = transmitter.receive()
//        var elapseTime = System.nanoTime - startTime
//        println("Communication latency: " + elapseTime + " ns")

        if (revMsg.getType() != AccMessage.MsgType.ACCGRANT)
          throw new RuntimeException("Request reject.")

        var startTime = System.nanoTime

        val dataMsg = transmitter.buildMessage(AccMessage.MsgType.ACCDATA)

        // Prepare input data blocks
        var i = 0
        var requireData: Boolean = false
        while (i < numBlock) {
          if (!revMsg.getData(i).getCached()) {
            requireData = true
            if (isCached == true) { // Send data from memory
              val inputAry: Array[T] = (firstParent[T].iterator(split, context)).toArray
              val mappedFileInfo = Util.serializePartition(appId, inputAry, blockId(i))
              dataLength = dataLength + mappedFileInfo._2 // We know element # by reading the file
              transmitter.addData(dataMsg, blockId(i), mappedFileInfo._2,
                  mappedFileInfo._2 * typeSize, 0, mappedFileInfo._1)
            }
            else { // Send HDFS file information: unknown length
              val splitInfo: String = split.asInstanceOf[HadoopPartition].inputSplit.toString

              // Parse Hadoop file string: file:<path>:<offset>+<size>
              val filePath: String = splitInfo.substring(
                  splitInfo.indexOf(':') + 1, splitInfo.lastIndexOf(':'))
              val fileOffset: Int = splitInfo.substring(
                  splitInfo.lastIndexOf(':') + 1, splitInfo.lastIndexOf('+')).toInt
              val fileSize: Int = splitInfo.substring(
                  splitInfo.lastIndexOf('+') + 1, splitInfo.length).toInt
           
              transmitter.addData(dataMsg, blockId(i), -1, 
                  fileSize, fileOffset, filePath)
            }
          }
          i = i + 1
        }

        // Prepare broadcast blocks
        i = 0
        while (i < brdcstId.length) {
          transmitter.addBroadcastData(dataMsg, brdcstId(i))
          i = i + 1
        }

        var elapseTime = System.nanoTime - startTime
        println("Preprocess time: " + elapseTime + " ns");

        if (requireData == true) {
          Util.logMsg(dataMsg)
          transmitter.send(dataMsg)
        }

        revMsg = transmitter.receive()
        Util.logMsg(revMsg)

        if (revMsg.getType() == AccMessage.MsgType.ACCFINISH) {
          // set length
          i = 0
          var numItems = 0

          val blkLength = new Array[Int](numBlock)
          val itemLength = new Array[Int](numBlock)
          while (i < numBlock) {
            blkLength(i) = revMsg.getData(i).getLength()
            if (revMsg.getData(i).hasNumItems()) {
              itemLength(i) = blkLength(i) / revMsg.getData(i).getNumItems()
            }
            else {
              itemLength(i) = 1
            }
            numItems += blkLength(i) / itemLength(i)
            i = i + 1
          }
          if (numItems == 0)
            throw new RuntimeException("Manager returns an invalid data length")

          outputAry = new Array[U](numItems)

          startTime = System.nanoTime
          // read result
          i = 0
          idx = 0
          while (i < numBlock) { // We just simply concatenate all blocks

            Util.readMemoryMappedFile(
                outputAry,
                idx, 
                blkLength(i), itemLength(i), 
                revMsg.getData(i).getPath())

            idx = idx + blkLength(i) / itemLength(i)
            i = i + 1
          }
          idx = 0
          elapseTime = System.nanoTime - startTime
          println("Read memory mapped file time: " + elapseTime + " ns")
        }
        else
          throw new RuntimeException("Task failed.")
      }
      catch {
        case e: Throwable =>
          val sw = new StringWriter
          e.printStackTrace(new PrintWriter(sw))
          Util.logInfo(this, "Fail to execute on accelerator: " + sw.toString)
          outputAry = computeOnJTP(split, context)
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

  def map_acc[V: ClassTag](clazz: Accelerator[U, V]): AccRDD[V, U] = {
    new AccRDD(appId, this, clazz)
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

  def computeOnJTP(split: Partition, context: TaskContext): Array[U] = {
    Util.logInfo(this, "Compute partition " + split.index + " using CPU")
    val inputAry: Array[T] = (firstParent[T].iterator(split, context)).toArray
    val dataLength = inputAry.length
    val outputAry = new Array[U](dataLength)
    var j: Int = 0
    while (j < inputAry.length) {
      outputAry(j) = acc.call(inputAry(j).asInstanceOf[T])
      j = j + 1
    }
    outputAry
  }
}

