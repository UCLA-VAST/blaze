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
import scala.collection.mutable._

import org.apache.spark._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.scheduler._
import org.apache.spark.util.random._

/**
  * A RDD that uses accelerator to accelerate the computation. The behavior of AccRDD is 
  * similar to Spark partition RDD which performs the computation for a whole partition at a
  * time. AccRDD also processes a partition to reduce the communication overhead.
  *
  * @param appId The unique application ID of Spark.
  * @param prev The original RDD of Spark.
  * @param acc The developer extended accelerator class.
  */
class AccRDD[U: ClassTag, T: ClassTag](
  appId: String, 
  prev: RDD[T], 
  acc: Accelerator[T, U],
  port: Int,
  sampler: RandomSampler[Int, Int]
) extends RDD[U](prev) with Logging {

  def getPrevRDD() = prev
  def getRDD() = this
  def getIntId() = Math
      .abs(("""\d+""".r findAllIn appId)
      .addString(new StringBuilder).toLong.toInt)

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) = {
    val brdcstIdOrValue = new Array[(Long, Boolean)](acc.getArgNum)
    val brdcstBlockInfo = new Array[BlockInfo](acc.getArgNum)

    // Generate an input data block ID array.
    // (only Tuple types cause multiple sub-blocks. Now we support to Tuple2)
    val numBlock: Int = Util.getBlockNum(getRDD.asInstanceOf[RDD[T]])
    val blockInfo = new Array[BlockInfo](numBlock)
    for (j <- 0 until numBlock) {
      blockInfo(j) = new BlockInfo
      blockInfo(j).id = Util.getBlockID(getIntId(), getPrevRDD.id, split.index, j)
    }

    // Followed by a mask ID
    val maskId: Long = Util.getBlockID(getIntId(), getPrevRDD.id, split.index, numBlock)

    val isCached = inMemoryCheck(split)

    val resultIter = new Iterator[U] {
      var outputIter: Iterator[U] = null
      var outputAry: Array[U] = null // Length is unknown before reading the input
      var dataLength: Int = -1
      val isPrimitiveType: Boolean = Util.isPrimitiveTypeRDD(getRDD)
      val isModeledType: Boolean = Util.isModeledTypeRDD(getRDD)
      var partitionMask: Array[Byte] = null
      var isSampled: Boolean = false

      var startTime: Long = 0
      var elapseTime: Long = 0

      try {
        if (!isPrimitiveType && !isModeledType) {
          throw new RuntimeException("RDD data type is not supported")
        }

        val transmitter = new DataTransmitter("127.0.0.1", port)
        if (transmitter.isConnect == false) {
          throw new RuntimeException("Connection refuse from port "+
             port.toString)
        }

        // Get broadcast block IDs
        for (j <- 0 until brdcstIdOrValue.length) {

          // a tuple of (val, isID)
          brdcstIdOrValue(j) = acc.getArg(j) match {
            case Some(v: BlazeBroadcast[_]) => (v.brdcst_id, true)
            case Some(v: Long) => (Util.casting(v, classOf[Long]), false)
            case Some(v: Int) => (Util.casting(v, classOf[Long]), false)
            case Some(v: Double) => (Util.casting(v, classOf[Long]), false)
            case Some(v: Float) => (Util.casting(v, classOf[Long]), false)
            case _ => throw new RuntimeException("Invalid Broadcast arguement "+j)
          }
        }

        // Sample data if necessary (all sub-blocks use the same mask)
        if (sampler != null && partitionMask == null) {
          partitionMask = samplePartition(split, context)
          isSampled = true
        }

        var msg = DataTransmitter.buildMessage(acc.id, appId, AccMessage.MsgType.ACCREQUEST)
        for (i <- 0 until numBlock)
          DataTransmitter.addData(msg, blockInfo(i), isSampled, null)

        for (i <- 0 until brdcstIdOrValue.length) {
          val bcData = acc.getArg(i).get
          if (bcData.isInstanceOf[BlazeBroadcast[_]]) {
            val arrayData = (bcData.asInstanceOf[BlazeBroadcast[Array[_]]]).data
            brdcstBlockInfo(i) = new BlockInfo
            brdcstBlockInfo(i).id = brdcstIdOrValue(i)._1
            brdcstBlockInfo(i).numElt = arrayData.length
            brdcstBlockInfo(i).eltLength = if (arrayData(0).isInstanceOf[Array[_]]) {
              arrayData(0).asInstanceOf[Array[_]].length 
            } else {
              1
            }
            brdcstBlockInfo(i).eltSize = Util.getTypeSize(arrayData(0)) * brdcstBlockInfo(i).eltLength
            if (brdcstBlockInfo(i).eltSize < 0)
              throw new RuntimeException("Unsupported broadcast data type.")

            DataTransmitter.addData(msg, brdcstBlockInfo(i), false, null)
          }
          else // Send a scalar data through the socket directly.
            DataTransmitter.addScalarData(msg, brdcstIdOrValue(i)._1)
        }

        logInfo("Partition "+split.index+" sent an AccRequest for accelerator: "+acc.id);

        transmitter.send(msg)
        val revMsg = transmitter.receive()

        if (revMsg.getType() != AccMessage.MsgType.ACCGRANT)
          throw new RuntimeException("Request reject.")

        startTime = System.nanoTime

        val dataMsg = DataTransmitter.buildMessage(appId, AccMessage.MsgType.ACCDATA)

        // Prepare input data blocks
        var requireData: Boolean = false

        val maskFileInfo: BlazeMemoryFileHandler = if (isSampled) {
          val handler = new BlazeMemoryFileHandler(partitionMask)
          handler.serialization(getIntId(), maskId) 
          handler
        } else { 
          null
        }

        for (i <- 0 until numBlock) {
          if (!revMsg.getData(i).getCached()) { // Send data block if Blaze manager hasn't cached it.
            requireData = true

            // Serialize it and send memory mapped file path if:
            val inputAry = (firstParent[T].iterator(split, context)).toArray

            // Get real input array considering Tuple types
            val subInputAry = if (numBlock == 1) inputAry else {
              i match {
                case 0 => inputAry.asInstanceOf[Array[Tuple2[_,_]]].map(e => e._1)
                case 1 => inputAry.asInstanceOf[Array[Tuple2[_,_]]].map(e => e._2)
              }
            }
            val mappedFileInfo = new BlazeMemoryFileHandler(subInputAry)
            mappedFileInfo.serialization(getIntId(), blockInfo(i).id)

            dataLength = dataLength + mappedFileInfo.numElt // We know element # by reading the file

            if (isSampled) {
              DataTransmitter.addData(dataMsg, mappedFileInfo, true, maskFileInfo.fileName)
            }
            else {
              DataTransmitter.addData(dataMsg, mappedFileInfo, false, null)
            }
          }
          else if (revMsg.getData(i).getSampled()) {
            requireData = true
            DataTransmitter.addData(dataMsg, blockInfo(i), true, maskFileInfo.fileName)
          }
        }
        elapseTime = System.nanoTime - startTime
        logInfo("Partition "+split.index+" preparing input blocks takes: "+elapseTime+"ns");

        startTime = System.nanoTime

        // Prepare broadcast blocks
        var numBrdcstBlock: Int = 0
        for (i <- 0 until brdcstIdOrValue.length) {
          if (brdcstIdOrValue(i)._2 == true && !revMsg.getData(numBrdcstBlock + numBlock).getCached()) {
            requireData = true

            require (acc.getArg(i).get.isInstanceOf[BlazeBroadcast[_]], 
              "Uncached data is not BlazeBroadcast!")

            val bcData = (acc.getArg(i).get.asInstanceOf[BlazeBroadcast[_]]).data 
            if (bcData.getClass.isArray) { // Serialize array and use memory mapped file to send the data.
              val arrayData = bcData.asInstanceOf[Array[_]]
              val mappedFileInfo = new BlazeMemoryFileHandler(arrayData)
              mappedFileInfo.serialization(getIntId(), brdcstIdOrValue(i)._1)

              DataTransmitter.addData(dataMsg, mappedFileInfo, false, null)
              (acc.getArg(i).get.asInstanceOf[BlazeBroadcast[_]]).length = mappedFileInfo.numElt
              (acc.getArg(i).get.asInstanceOf[BlazeBroadcast[_]]).size = mappedFileInfo.numElt * mappedFileInfo.eltSize
            }
            else { // Send a scalar data through the socket directly.
              DataTransmitter.addScalarData(dataMsg, brdcstIdOrValue(i)._1)
              (acc.getArg(i).get.asInstanceOf[BlazeBroadcast[_]]).length = 1
              (acc.getArg(i).get.asInstanceOf[BlazeBroadcast[_]]).size = 4
            }
            numBrdcstBlock += 1
          }
        }
        elapseTime = System.nanoTime - startTime
        logInfo("Partition "+split.index+" preparing broadcast blocks takes: "+elapseTime+"ns");

        // Send ACCDATA message only when it is required.
        if (requireData == true) {
          transmitter.send(dataMsg)
        }

        logInfo("Sent partition "+split.index+" to the accelerator");

        val finalRevMsg = transmitter.receive()

        if (finalRevMsg.getType() == AccMessage.MsgType.ACCFINISH) {
          val numOutputBlock: Int = finalRevMsg.getDataCount
          var numElt: Int = -1
          val eltLength = new Array[Int](numOutputBlock)

          // First read: Compute the correct number of output items.
          for (i <- 0 until numOutputBlock) {
            val blkEltNum = finalRevMsg.getData(i).getNumElements()
            if (finalRevMsg.getData(i).hasElementLength()) {
              eltLength(i) = finalRevMsg.getData(i).getElementLength()
            }
            else {
              eltLength(i) = 1
            }

            if (numElt != -1 && numElt != blkEltNum)
              throw new RuntimeException("Element number is not consist!")
            numElt = blkEltNum
          }

          // Allocate output array
          outputAry = new Array[U](numElt)

          startTime = System.nanoTime

          // Second read: Read outputs from memory mapped file.
          if (!outputAry.isInstanceOf[Array[Tuple2[_,_]]]) { // Primitive/Array type
            if (outputAry.isInstanceOf[Array[Array[_]]]) { // Array type
              for (i <- 0 until numElt) {
                if (classTag[U] == classTag[Array[Int]])
                  outputAry(i) = (new Array[Int](eltLength(0))).asInstanceOf[U]
                else if (classTag[U] == classTag[Array[Float]])
                  outputAry(i) = (new Array[Float](eltLength(0))).asInstanceOf[U]
                else if (classTag[U] == classTag[Array[Long]])
                  outputAry(i) = (new Array[Long](eltLength(0))).asInstanceOf[U]
                else if (classTag[U] == classTag[Array[Double]])
                  outputAry(i) = (new Array[Double](eltLength(0))).asInstanceOf[U]
                else
                  throw new RuntimeException("Unsupported output type.")
              }
            }
            val mappedFileInfo = new BlazeMemoryFileHandler(outputAry)
            mappedFileInfo.readMemoryMappedFile(
              null,
              numElt,
              eltLength(0),
              finalRevMsg.getData(0).getFilePath())
          }
          else { // Tuple2 type
            val tmpOutputAry = new Array[Any](numOutputBlock)
            val inputAry: Array[T] = ((firstParent[T].iterator(split, context)).toArray)
            var sampledIn: Option[T] = Some(inputAry(0))
            var sampledOut: Option[Any] = Some(acc.call(sampledIn.get).asInstanceOf[Any])
         
            for (j <- 0 until 2) {
              val s = if (j == 0) sampledOut.get.asInstanceOf[Tuple2[_,_]]._1 
                      else sampledOut.get.asInstanceOf[Tuple2[_,_]]._2

              if (s.isInstanceOf[Array[_]]) {
                tmpOutputAry(j) = new Array[Array[Any]](numElt)
                for (i <- 0 until numElt) {
                  if (s.isInstanceOf[Array[Int]])
                    tmpOutputAry(j).asInstanceOf[Array[Any]](i) = new Array[Int](eltLength(i))
                  else if (s.isInstanceOf[Array[Float]])
                    tmpOutputAry(j).asInstanceOf[Array[Any]](i) = new Array[Float](eltLength(i))
                  else if (s.isInstanceOf[Array[Long]])
                    tmpOutputAry(j).asInstanceOf[Array[Any]](i) = new Array[Long](eltLength(i))
                  else if (s.isInstanceOf[Array[Double]])
                    tmpOutputAry(j).asInstanceOf[Array[Any]](i) = new Array[Double](eltLength(i))
                  else
                    throw new RuntimeException("Unsupported output type.")
                    
                }
              }
              else {
                if (s.isInstanceOf[Int])
                  tmpOutputAry(j) = new Array[Int](numElt)
                else if (s.isInstanceOf[Float])
                  tmpOutputAry(j) = new Array[Float](numElt)
                else if (s.isInstanceOf[Long])
                  tmpOutputAry(j) = new Array[Long](numElt)
                else if (s.isInstanceOf[Double])
                  tmpOutputAry(j) = new Array[Double](numElt)
                else
                  throw new RuntimeException("Unsupported output type.")
              }

              val mappedFileInfo = new BlazeMemoryFileHandler(tmpOutputAry(j).asInstanceOf[Array[_]])
              mappedFileInfo.readMemoryMappedFile(
                s,
                numElt, 
                eltLength(j), 
                finalRevMsg.getData(j).getFilePath())
            }
            outputAry = tmpOutputAry(0).asInstanceOf[Array[_]]
                                       .zip(tmpOutputAry(1).asInstanceOf[Array[_]])
                                       .asInstanceOf[Array[U]]
          }
          outputIter = outputAry.iterator

          elapseTime = System.nanoTime - startTime
          logInfo("Partition " + split.index + " finishes executing on accelerator")
        }
        else
          throw new RuntimeException("Task failed.")
      }
      catch {
        case e: Throwable =>
          val sw = new StringWriter
          e.printStackTrace(new PrintWriter(sw))
          logWarning("Partition " + split.index + " fails to be executed on accelerator")
          outputIter = computeOnJTP(split, context, partitionMask)
      }

      def hasNext(): Boolean = {
        outputIter.hasNext
      }

      def next(): U = {
        outputIter.next
      }
    }
    resultIter
  }

  /**
    * A method for the developer to execute the computation on the accelerator.
    * The original Spark API `map` is still available.
    *
    * @param clazz Extended accelerator class.
    * @return A transformed AccRDD.
    */
  def map_acc[V: ClassTag](clazz: Accelerator[U, V]): AccRDD[V, U] = {
    new AccRDD(appId, this, clazz, port, null)
  }

  /**
    * A method for the developer to execute the computation on the accelerator.
    * The original Spark API `mapPartition` is still available.
    *
    * @param clazz Extended accelerator class.
    * @return A transformed AccMapPartitionsRDD.
    */
  def mapPartitions_acc[V: ClassTag](clazz: Accelerator[U, V]): AccMapPartitionsRDD[V, U] = {
    new AccMapPartitionsRDD(appId, this.asInstanceOf[RDD[U]], clazz, port, null)
  }
  

  /**
    * A method for developer to sample a part of RDD.
    *
    * @param withReplacement 
    * @param fraction The fraction of sampled data.
    * @param seed The optinal value for developer to assign a random seed.
    * @return A RDD with a specific sampler.
    */
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

    new ShellRDD(appId, this.asInstanceOf[RDD[T]], port, thisSampler)
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

  def samplePartition(split: Partition, context: TaskContext): Array[Byte] = {
    require(sampler != null)
    val thisSampler = sampler.clone
    val inputIter = firstParent[T].iterator(split, context)
    val inputAry = inputIter.toArray
    var idx: Int = 0

    val mask = Array.fill[Byte](inputAry.length)(0)
    val maskIdx = new Array[Int](inputAry.length)
    while (idx < inputAry.length) {
      maskIdx(idx) = idx
      idx += 1
    }
    val sampledIter = thisSampler.sample(maskIdx.iterator)

    while (sampledIter.hasNext) {
      mask(sampledIter.next) = 1 
    }
    mask
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
  def computeOnJTP(split: Partition, context: TaskContext, partitionMask: Array[Byte]): Iterator[U] = {
    logInfo("Compute partition " + split.index + " using CPU")

    val inputAry: Array[T] = (firstParent[T].iterator(split, context)).toArray
    val dataLength = inputAry.length
    var outputList = List[U]()

    //if (partitionMask == null)
    //  logWarning("Partition " + split.index + " has no mask")

    var j: Int = 0
    while (j < inputAry.length) {
      if (partitionMask == null || partitionMask(j) != 0)
        outputList = outputList :+ acc.call(inputAry(j).asInstanceOf[T])
      j = j + 1
    }
    outputList.iterator
  }
}

