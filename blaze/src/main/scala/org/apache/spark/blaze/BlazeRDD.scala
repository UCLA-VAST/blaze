package org.apache.spark.blaze

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

class BlazeRDD[U: ClassTag, T: ClassTag](appId: Int, prev: RDD[T], acc: Accelerator[T, U]) 
  extends RDD[U](prev) with Logging {

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

          /* Issue #21: We don't send broadcast data in advance anymore
          if (arg.get.isBroadcast == false)
            throw new RuntimeException("Broadcast data is not prepared.")
          */

          brdcstId(j) = arg.get.brdcst_id
        }

        val transmitter = new DataTransmitter()
        if (transmitter.isConnect == false)
          throw new RuntimeException("Connection refuse.")
        var msg = DataTransmitter.buildRequest(acc.id, blockId, brdcstId)
//        var startTime = System.nanoTime
        transmitter.send(msg)
        val revMsg = transmitter.receive()
//        var elapseTime = System.nanoTime - startTime
//        println("Communication latency: " + elapseTime + " ns")

        if (revMsg.getType() != AccMessage.MsgType.ACCGRANT)
          throw new RuntimeException("Request reject.")

        var startTime = System.nanoTime

        val dataMsg = DataTransmitter.buildMessage(AccMessage.MsgType.ACCDATA)

        // Prepare input data blocks
        val requireData = Array(false)
        for (i <- 0 until numBlock) {
          if (!revMsg.getData(i).getCached()) {
            requireData(0) = true
            if (isCached == true || !split.isInstanceOf[HadoopPartition]) { // Send data from memory
              // Issue #26: This partition might be a CoalescedRDDPartition, 
              // which has no file information so we have to load it in advance.

              val inputAry: Array[T] = (firstParent[T].iterator(split, context)).toArray
              val mappedFileInfo = Util.serializePartition(appId, inputAry, blockId(i))
              dataLength = dataLength + mappedFileInfo._2 // We know element # by reading the file
              DataTransmitter.addData(dataMsg, blockId(i), mappedFileInfo._2,
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
           
              DataTransmitter.addData(dataMsg, blockId(i), -1, 
                  fileSize, fileOffset, filePath)
            }
          }
        }

        // Prepare broadcast blocks
        for (i <- 0 until brdcstId.length) {
          if (!revMsg.getData(i + numBlock).getCached()) {
            requireData(0) = true
            val bcData = acc.getArg(i).get.data
            if (bcData.getClass.isArray) {
              val arrayData = bcData.asInstanceOf[Array[_]]
              val mappedFileInfo = Util.serializePartition(appId, arrayData, brdcstId(i))
              val typeName = arrayData(0).getClass.getName.replace("java.lang.", "").toLowerCase
              val typeSize = Util.getTypeSizeByName(typeName)
              assert(typeSize != 0, "Cannot find the size of type " + typeName)

              DataTransmitter.addData(dataMsg, brdcstId(i), arrayData.length, 
                arrayData.length * typeSize, 0, mappedFileInfo._1)
              acc.getArg(i).get.length = arrayData.length
              acc.getArg(i).get.size = arrayData.length * typeSize
            }
            else {
              val longData: Long = Util.casting(bcData, classOf[Long])
              DataTransmitter.addScalarData(dataMsg, brdcstId(i), longData)
              acc.getArg(i).get.length = 1
              acc.getArg(i).get.size = 4
            }
          }
        }

        var elapseTime = System.nanoTime - startTime
        println("Preprocess time: " + elapseTime + " ns");

        if (requireData(0) == true) {
          logInfo(Util.logMsg(dataMsg))
          transmitter.send(dataMsg)
        }

        val finalRevMsg = transmitter.receive()
        logInfo(Util.logMsg(finalRevMsg))

        if (finalRevMsg.getType() == AccMessage.MsgType.ACCFINISH) {
          // set length
          val numItems = Array(0)

          val blkLength = new Array[Int](numBlock)
          val itemLength = new Array[Int](numBlock)
          for (i <- 0 until numBlock) {
            blkLength(i) = finalRevMsg.getData(i).getLength()
            if (finalRevMsg.getData(i).hasNumItems()) {
              itemLength(i) = blkLength(i) / finalRevMsg.getData(i).getNumItems()
            }
            else {
              itemLength(i) = 1
            }
            numItems(0) += blkLength(i) / itemLength(i)
          }
          if (numItems(0) == 0)
            throw new RuntimeException("Manager returns an invalid data length")

          outputAry = new Array[U](numItems(0))

          startTime = System.nanoTime
          // read result
          idx = 0
          for (i <- 0 until numBlock) { // We just simply concatenate all blocks

            Util.readMemoryMappedFile(
                outputAry,
                idx, 
                blkLength(i), itemLength(i), 
                finalRevMsg.getData(i).getPath())

            idx = idx + blkLength(i) / itemLength(i)
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
          logInfo("Fail to execute on accelerator: " + sw.toString)
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

  def map_acc[V: ClassTag](clazz: Accelerator[U, V]): BlazeRDD[V, U] = {
    new BlazeRDD(appId, this, clazz)
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
    logInfo("Compute partition " + split.index + " using CPU")
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
