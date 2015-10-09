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
import java.util.Calendar
import java.text.SimpleDateFormat
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.net.InetAddress
import java.net.UnknownHostException
import java.util.Random

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._         
import scala.collection.mutable        
import scala.collection.mutable.HashMap
                                                
import org.apache.spark._                       
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._                   

/**
  * Various utility methods used by Blaze.
  */
object Util {

  // The format of block IDs.
  // Format: | Broadcast, 1 | App ID, 32 | RDD ID, 18 | Partition ID, 12 | Sub-block ID, 1 |
  val APP_BIT_START = 31
  val RDD_BIT_START = 13
  val PARTITION_BIT_START = 1
  val BLOCK_BIT_START = 0

  def random = new Random()

  /**
    * Generate a string of message information.
    */
  def logMsg(msgBuilder: AccMessage.TaskMsg.Builder): String = {
    val msg = msgBuilder.build()
    logMsg(msg)
  }

  /**
    * Generate a string of message information.
    */
  def logMsg(msg: AccMessage.TaskMsg): String = {
    var logStr = "Message: Type: "
    logStr = logStr + msg.getType() + ", Data: " + msg.getDataCount() + "\n"

    for (i <- 0 until msg.getDataCount()) {
      logStr = logStr + "Data " + i + ": "
      if (msg.getData(i).hasPartitionId())
        logStr = logStr + "ID: " + msg.getData(i).getPartitionId() + ", "
      if (msg.getData(i).hasNumElements())
        logStr = logStr + "#Element: " + msg.getData(i).getNumElements() + ", "
      if (msg.getData(i).hasElementSize())
        logStr = logStr + "ElementSize: " + msg.getData(i).getElementSize() + ", "
      if (msg.getData(i).hasElementLength())
        logStr = logStr + "ElementLength: " + msg.getData(i).getElementLength() + ", "

      logStr = logStr + "isSampled: " + msg.getData(i).getSampled() + ", "
   
      if (msg.getData(i).hasScalarValue())
        logStr = logStr + "ScalarValue: " + msg.getData(i).getScalarValue() + ", " 
      if (msg.getData(i).hasFileSize())
        logStr = logStr + "FileSize: " + msg.getData(i).getFileSize() + ", "
      if (msg.getData(i).hasFilePath())
        logStr = logStr + "FilePath: " + msg.getData(i).getFilePath() + ", "
      if (msg.getData(i).hasMaskPath())
        logStr = logStr + "MaskPath: " + msg.getData(i).getMaskPath()
      logStr = logStr + "\n"
    }
    logStr
  }

  /**
    * Consult the IP address by host name.
    * 
    * @param host Host name.
    * @return An IP address in string, or None if fails to find it.
    */
  def getIPByHostName(host: String): Option[String] = {
    try {
      val inetAddr = InetAddress.getByName(host)
      Some(inetAddr.getHostAddress)
    } catch {
      case e: UnknownHostException =>
        None
    }
  }

    /**
    * Calculate the number of blocks needed by the RDD.
    * (Now only support Tuple2)
    *
    * @param rdd RDD.
    * @return the number of blocks.
    */
  def getBlockNum[T: ClassTag](rdd: RDD[T]): Int = {
    if (classTag[T] == classTag[Tuple2[_, _]])
      2
    else
      1
  }

  /**
    * Get a type size of the input variable.
    *
    * @param in Input variable.
    * @return Type size of the variable in byte, return -1 for non-supported types.
    */
  def getTypeSize[T: ClassTag](in: T): Int = in match {
    case _: Byte =>   1
    case _: Char =>   2
    case _: Short =>  2
    case _: Int =>    4
    case _: Float =>  4
    case _: Long =>   8
    case _: Double => 8
    case _ =>        -1
  }

   /**
    * Get a type name initial of the input variable.
    *
    * @param in Input variable.
    * @return Type name initial of the variable in byte, 
    * return an empty string for non-supported types.
    */
  def getTypeName[T: ClassTag](in: T): String = in match {
    case _: Byte =>   "bype"
    case _: Char =>   "char"
    case _: Short =>  "short"
    case _: Int =>    "int"
    case _: Float =>  "float"
    case _: Long =>   "long"
    case _: Double => "double"
    case _: Any => "any"
    case _ =>         ""
  }
 
  /**
    * Get a primitive type size by type name.
    *
    * @param dataType Type name in string.
    * @return Type size in byte.
    */
  def getTypeSizeByName(dataType: String): Int = {
    var typeName = dataType
    if (dataType.length > 1)
      typeName = dataType(0).toString

    val typeSize: Int = typeName match {
      case "c" => 2
      case "i" => 4
      case "f" => 4
      case "l" => 8
      case "d" => 8
      case _ => -1
    }
    typeSize
  }

   /**
    * Check the type of the RDD is primitive or not.
    *
    * @param rdd RDD.
    * @return true for primitive type, false otherwise.
    */
  def isPrimitiveTypeRDD[T: ClassTag](rdd: RDD[T]): Boolean = {
    if ((classTag[T] == classTag[Byte] || classTag[T] == classTag[Array[Byte]])   ||
        (classTag[T] == classTag[Char] || classTag[T] == classTag[Array[Char]])   ||
        (classTag[T] == classTag[Short] || classTag[T] == classTag[Array[Short]]) ||  
        (classTag[T] == classTag[Int] || classTag[T] == classTag[Array[Int]])     ||
        (classTag[T] == classTag[Float] || classTag[T] == classTag[Array[Float]]) ||  
        (classTag[T] == classTag[Long] || classTag[T] == classTag[Array[Long]])   ||
        (classTag[T] == classTag[Double] || classTag[T] == classTag[Array[Double]])) 
      true
    else
      false
  }

   /**
    * Check the type of the RDD is modeled by Blaze or not.
    *
    * @param rdd RDD.
    * @return true for the modeled type, false otherwise.
    */
  def isModeledTypeRDD[T: ClassTag](rdd: RDD[T]): Boolean = {
    if (classTag[T] == classTag[Tuple2[_,_]])
      true
    else
      false
  }
 
  /**
    * Casting a primitive scalar value to another primitive scalar value.
    *
    * @param value Input value (must be in primitive type).
    * @param clazz The type of the output value (must be in primitive type).
    * @return Casted value.
    */
  def casting[T: ClassTag, U: ClassTag](value: T, clazz: Class[U]): U = {
 	  val buf = ByteBuffer.allocate(8)
	  buf.order(ByteOrder.LITTLE_ENDIAN)

	  if (value.isInstanceOf[Int])
	    buf.putInt(value.asInstanceOf[Int])
    else if (value.isInstanceOf[Float])
	    buf.putFloat(value.asInstanceOf[Float])
    else if (value.isInstanceOf[Long])
	    buf.putLong(value.asInstanceOf[Long])
    else if (value.isInstanceOf[Double])
	    buf.putDouble(value.asInstanceOf[Double])
    else
      throw new RuntimeException("Unsupported casting type")

    if (clazz == classOf[Long])
  	  buf.getLong(0).asInstanceOf[U]
    else if (clazz == classOf[Int])
      buf.getInt(0).asInstanceOf[U]
    else if (clazz == classOf[Float])
      buf.getFloat(0).asInstanceOf[U]
    else if (clazz == classOf[Double])
      buf.getDouble(0).asInstanceOf[U]
    else
      throw new RuntimeException("Unsupported casting type")
  }

  /**
    * Generate a block ID by given information. There has two ID format for 
    * input and broadcast data blocks.
    * Input data: + | App ID, 32 | RDD ID, 18 | Partition ID, 12 | Sub-block ID, 1 |
    * Broadcast : - | App ID, 32 | Broadcast ID + 1, 31 |
    *
    * @param appId Application ID.
    * @param objId RDD or broadcast ID.
    * @param partitionIdx Partition index of a RDD. Do not specify for broadcast blocks.
    * @param blockIdx Sub-block index of a partition. Do not specify for broadcast blocks.
    * @return Block ID.
    */
  def getBlockID(appId: Int, objId: Int, partitionIdx: Int = -1, blockIdx: Int = -1): Long = {
    if (partitionIdx == -1) { // broadcast block
      -((appId.toLong << APP_BIT_START) + (objId + 1))
    }
    else { // input block
      (appId.toLong << APP_BIT_START) + (objId << RDD_BIT_START) + 
        (partitionIdx << PARTITION_BIT_START) + (blockIdx << BLOCK_BIT_START)
    }
  }
}
