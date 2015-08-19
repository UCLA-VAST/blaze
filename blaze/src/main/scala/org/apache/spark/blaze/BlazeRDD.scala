/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

/**
  * A RDD that uses accelerator to accelerate the computation. The behavior of BlazeRDD is 
  * similar to Spark partition RDD which performs the computation for a whole partition at a
  * time. BlazeRDD also processes a partition to reduce the communication overhead.
  *
  * @param appId The unique application ID of Spark.
  * @param prev The original RDD of Spark.
  * @param acc The developer extended accelerator class.
  */
class BlazeRDD[U: ClassTag, T: ClassTag](appId: Int, prev: RDD[T], acc: Accelerator[T, U]) 
  extends RDD[U](prev) with Logging {

  def getPrevRDD() = prev
  def getRDD() = this

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) = {
    val numBlock: Int = 1 // Now we just use 1 block for an input partition.

    val blockId = new Array[Long](numBlock)
    val brdcstId = new Array[Long](acc.getArgNum)

    // Generate an input data block ID array
    for (j <- 0 until numBlock) {
      blockId(j) = Util.getBlockID(appId, getPrevRDD.id, split.index, j)
    }

    val isCached = inMemoryCheck(split)

    val outputIter = new Iterator[U] {
      var outputAry: Array[U] = null // Length is unknown before reading the input
      var idx: Int = 0
      var dataLength: Int = -1
      val typeSize: Int = Util.getTypeSizeByRDD(getRDD())

      var startTime: Long = 0
      var elapseTime: Long = 0

      try {
        if (typeSize == -1)
          throw new RuntimeException("Cannot recognize RDD data type")

        // Get broadcast block IDs
        for (j <- 0 until brdcstId.length) {
          val arg = acc.getArg(j)
          if (arg.isDefined == false)
            throw new RuntimeException("Argument index " + j + " is out of range.")

          brdcstId(j) = arg.get.brdcst_id
        }

        val transmitter = new DataTransmitter()
        if (transmitter.isConnect == false)
          throw new RuntimeException("Connection refuse.")
        var msg = DataTransmitter.buildRequest(acc.id, blockId, brdcstId)

        startTime = System.nanoTime
        transmitter.send(msg)
        val revMsg = transmitter.receive()
        elapseTime = System.nanoTime - startTime
        logInfo("Partition " + split.index + " communication latency: " + elapseTime + " ns")

        if (revMsg.getType() != AccMessage.MsgType.ACCGRANT)
          throw new RuntimeException("Request reject.")

        startTime = System.nanoTime

        val dataMsg = DataTransmitter.buildMessage(AccMessage.MsgType.ACCDATA)

        // Prepare input data blocks
        val requireData = Array(false)
        for (i <- 0 until numBlock) {
          if (!revMsg.getData(i).getCached()) { // Send data block if Blaze manager hasn't cached it.
            requireData(0) = true

            // The data has been read and cached by Spark, serialize it and send memory mapped file path.
            if (isCached == true || !split.isInstanceOf[HadoopPartition]) {
              // Issue #26: This partition might be a CoalescedRDDPartition, 
              // which has no file information so we have to load it in advance.

              val inputAry: Array[T] = (firstParent[T].iterator(split, context)).toArray
              val mappedFileInfo = Util.serializePartition(appId, inputAry, blockId(i))
              dataLength = dataLength + mappedFileInfo._2 // We know element # by reading the file
              DataTransmitter.addData(dataMsg, blockId(i), mappedFileInfo._2,
                  mappedFileInfo._2 * typeSize, 0, mappedFileInfo._1)
            }
            else { // The data hasn't been read by Spark, send HDFS file path directly (data length is unknown)
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
            if (bcData.getClass.isArray) { // Serialize array and use memory mapped file to send the data.
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
            else { // Send a scalar data through the socket directly.
              val longData: Long = Util.casting(bcData, classOf[Long])
              DataTransmitter.addScalarData(dataMsg, brdcstId(i), longData)
              acc.getArg(i).get.length = 1
              acc.getArg(i).get.size = 4
            }
          }
        }

        elapseTime = System.nanoTime - startTime
        logInfo("Partition " + split.index + " preprocesses time: " + elapseTime + " ns");

        // Send ACCDATA message only when it is required.
        if (requireData(0) == true) {
          logInfo(Util.logMsg(dataMsg))
          transmitter.send(dataMsg)
        }

        val finalRevMsg = transmitter.receive()
        logInfo(Util.logMsg(finalRevMsg))

        if (finalRevMsg.getType() == AccMessage.MsgType.ACCFINISH) {
          val numItems = Array(0)
          val blkLength = new Array[Int](numBlock)
          val itemLength = new Array[Int](numBlock)

          // First read: Compute the correct number of output items.
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
          
          // Second read: Read outputs from memory mapped file.
          idx = 0
          for (i <- 0 until numBlock) { // Concatenate all blocks (currently only 1 block)

            Util.readMemoryMappedFile(
                outputAry,
                idx, 
                blkLength(i), itemLength(i), 
                finalRevMsg.getData(i).getPath())

            idx = idx + blkLength(i) / itemLength(i)
          }
          idx = 0
          elapseTime = System.nanoTime - startTime
          logInfo("Partition " + split.index + " reads memory mapped file: " + elapseTime + " ns")
        }
        else
          throw new RuntimeException("Task failed.")
      }
      catch {
        case e: Throwable =>
          val sw = new StringWriter
          e.printStackTrace(new PrintWriter(sw))
          logInfo("Partition " + split.index + " fails to be executed on accelerator: " + sw.toString)
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

  /**
    * A method for the developer to execute the computation on the accelerator.
    * The original Spark API `map` is still available.
    *
    * @param clazz Extended accelerator class.
    * @return A transformed BlazeRDD.
    */
  def map_acc[V: ClassTag](clazz: Accelerator[U, V]): BlazeRDD[V, U] = {
    new BlazeRDD(appId, this, clazz)
  }

  /**
    * Consult Spark block manager to see if the partition is cached or not.
    *
    * @param split The partition of a RDD.
    * @return A boolean value to indicate if the partition is cached.
    */
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

  /**
    * In case of failing to execute the computation on accelerator, 
    * Blaze will execute the computation on JVM instead to guarantee the
    * functionality correctness, even the overhead of failing to execute 
    * on accelerator is quite large.
    *
    * @param split The partition to be executed on JVM.
    * @param context TaskContext of Spark.
    * @return The output array
    */
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

