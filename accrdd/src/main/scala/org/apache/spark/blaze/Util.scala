
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

  /**
    * Generate a string of message information.
    */
  def logMsg(msgBuilder: AccMessage.TaskMsg.Builder): String = {
    val msg = msgBuilder.build()
    val logStr: Array[String] = Array("Message: Type: ")
    logStr(0) = logStr(0) + msg.getType() + ", Data: " + msg.getDataCount() + "\n"

    for (i <- 0 until msg.getDataCount()) {
      logStr(0) = logStr(0) + "Data " + i + ": "
      if (msg.getData(i).hasPartitionId())
        logStr(0) = logStr(0) + "ID: " + msg.getData(i).getPartitionId() + ", "
      if (msg.getData(i).hasLength())
        logStr(0) = logStr(0) + "Length: " + msg.getData(i).getLength() + ", "
      if (msg.getData(i).hasSize())
        logStr(0) = logStr(0) + "Size: " + msg.getData(i).getSize() + ", "
      if (msg.getData(i).hasPath())
        logStr(0) = logStr(0) + "Path: " + msg.getData(i).getPath()
      logStr(0) = logStr(0) + "\n"
    }
    logStr(0)
  }

  /**
    * Generate a string of message information.
    */
  def logMsg(msg: AccMessage.TaskMsg): String = {
    val logStr: Array[String] = Array("Message: Type: ")
    logStr(0) = logStr(0) + msg.getType() + ", Data: " + msg.getDataCount() + "\n"

    for (i <- 0 until msg.getDataCount()) {
      logStr(0) = logStr(0) + "Data " + i + ": "
      if (msg.getData(i).hasPartitionId())
        logStr(0) = logStr(0) + "ID: " + msg.getData(i).getPartitionId() + ", "
      if (msg.getData(i).hasLength())
        logStr(0) = logStr(0) + "Length: " + msg.getData(i).getLength() + ", "
      if (msg.getData(i).hasSize())
        logStr(0) = logStr(0) + "Size: " + msg.getData(i).getSize() + ", "
      if (msg.getData(i).hasPath())
        logStr(0) = logStr(0) + "Path: " + msg.getData(i).getPath()
      logStr(0) = logStr(0) + "\n"
    }
    logStr(0)
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
    * Get a primitive type size of a RDD.
    *
    * @param rdd RDD.
    * @return Type size of the RDD in byte.
    */
  def getTypeSizeByRDD[T: ClassTag](rdd: RDD[T]): Int = {
    if (classTag[T] == classTag[Byte] || classTag[T] == classTag[Array[Byte]])          1
    else if (classTag[T] == classTag[Short] || classTag[T] == classTag[Array[Short]])   2
    else if (classTag[T] == classTag[Char] || classTag[T] == classTag[Array[Char]])     2
    else if (classTag[T] == classTag[Int] || classTag[T] == classTag[Array[Int]])       4
    else if (classTag[T] == classTag[Float] || classTag[T] == classTag[Array[Float]])   4
    else if (classTag[T] == classTag[Long] || classTag[T] == classTag[Array[Long]])     8
    else if (classTag[T] == classTag[Double] || classTag[T] == classTag[Array[Double]]) 8
    else -1
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
      case "i" => 4
      case "f" => 4
      case "l" => 8
      case "d" => 8
      case _ => -1
    }
    typeSize
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

  /**
    * Serialize a partition and write the data to memory mapped file.
    *
    * @param prefix The prefix of memory mapped file. Usually use application ID.
    * @param input The data to be serialized.
    * @param id The ID of the data block.
    * @return A pair of (file name, size)
    */
  def serializePartition[T: ClassTag](prefix: Int, input: Array[T], id: Long): (String, Int, Int) = {
    var fileName: String = System.getProperty("java.io.tmpdir") + "/" + prefix
    if (id < 0) // Broadcast data
      fileName = fileName + "_brdcst_" + (-id) + ".dat"
    else // Partition data
      fileName = fileName + id + ".dat"

    val typeName = input(0).getClass.getName.replace("java.lang.", "").toLowerCase()
    val dataType: String = typeName.replace("[", "")(0).toString // Fetch the element data type.
    val typeSize: Int = getTypeSizeByName(dataType)

    val isArray: Boolean = typeName.contains("[")
    val itemNum: Int = if (isArray) input.length else 1

    if ((typeName.split('[').length - 1) > 1)
      throw new RuntimeException("Unsupport multi-dimension arrays: " + typeName)

    // Calculate buffer length
    val bufferLength = Array(0)
    if (isArray) {
      for (e <- input) {
        val a = e.asInstanceOf[Array[_]]
        bufferLength(0) = bufferLength(0) + a.length
      }
    }
    else
      bufferLength(0) = input.length

    // Create and write memory mapped file
    var raf: RandomAccessFile = null

    try {
      raf = new RandomAccessFile(fileName, "rw")
    } catch {
      case e: IOException =>
        throw new IOException("Fail to create memory mapped file " + fileName + ": " + e.toString)
    }
    val fc: FileChannel = raf.getChannel()
    val buf: ByteBuffer = fc.map(MapMode.READ_WRITE, 0, bufferLength(0) * typeSize)
    buf.order(ByteOrder.LITTLE_ENDIAN)

    for (e <- input) {
      if (isArray) {
        for (a <- e.asInstanceOf[Array[_]]) {
          dataType match {
            case "i" => buf.putInt(a.asInstanceOf[Int].intValue)
            case "f" => buf.putFloat(a.asInstanceOf[Float].floatValue)
            case "l" => buf.putLong(a.asInstanceOf[Long].longValue)
            case "d" => buf.putDouble(a.asInstanceOf[Double].doubleValue)
            case _ =>
              throw new RuntimeException("Unsupported type " + dataType)
          }
        }
      }
      else {
        dataType match {
          case "i" => buf.putInt(e.asInstanceOf[Int].intValue)
          case "f" => buf.putFloat(e.asInstanceOf[Float].floatValue)
          case "l" => buf.putLong(e.asInstanceOf[Long].longValue)
          case "d" => buf.putDouble(e.asInstanceOf[Double].doubleValue)
          case _ =>
            throw new RuntimeException("Unsupported type " + dataType)
        }
      }
    }
   
    try {
      fc.close()
      raf.close()
    } catch {
      case e: IOException =>
        throw new IOException("Fail to close memory mapped file " + fileName + ": " + e.toString)
    }

    (fileName, bufferLength(0), itemNum)
  }

  /**
    * Deserialize the data from memory mapped file.
    *
    * @param out The array used for storing the deserialized data.
    * @param offset The offset of memory mapped file.
    * @param length The total length (total #element) of output.
    * @param itemLength The length (#element) of a item. If not 1, means array type.
    * @param fileName File name of memory mapped file.
    */
  def readMemoryMappedFile[T: ClassTag](
      out: Array[T], 
      offset: Int, 
      length: Int,
      itemLength: Int,
      fileName: String) = {

    if (out(offset) == null) {
      if (classTag[T] == classTag[Array[Int]])
        out(offset) = (new Array[Int](itemLength)).asInstanceOf[T]
      else if (classTag[T] == classTag[Array[Float]])
        out(offset) = (new Array[Float](itemLength)).asInstanceOf[T]
      else if (classTag[T] == classTag[Array[Long]])
        out(offset) = (new Array[Long](itemLength)).asInstanceOf[T]
      else if (classTag[T] == classTag[Array[Double]])
        out(offset) = (new Array[Double](itemLength)).asInstanceOf[T]
      else
        throw new RuntimeException("Unsupport type")
    }

    // Fetch size information
    val typeName = out(offset).getClass.getName.replace("java.lang.", "").toLowerCase()
    val dataType: String = typeName.replace("[", "")(0).toString // Only fetch the initial
    val typeSize: Int = getTypeSizeByName(dataType)

    val isArray: Boolean = typeName.contains("[")

    // Create and write memory mapped file
    var raf: RandomAccessFile = null

    try {
      if ((typeName.split('[').length - 1) > 1)
        throw new RuntimeException("Unsupport multi-dimension arrays: " + typeName)

      if (typeSize == -1)
        throw new RuntimeException("Unsupported type " + dataType)

      raf = new RandomAccessFile(fileName, "r")
    } catch {
      case e: IOException =>
        throw new IOException("Fail to read memory mapped file " + fileName + ": " + e.toString)
    }

    val fc: FileChannel = raf.getChannel()
    val buf: ByteBuffer = fc.map(MapMode.READ_ONLY, 0, length * typeSize)
    buf.order(ByteOrder.LITTLE_ENDIAN)

    for (idx <- offset until length/itemLength) {
      if (isArray) {
        for (ii <- 0 until itemLength) {
          dataType match {
            case "i" => out(idx).asInstanceOf[Array[Int]](ii) = buf.getInt()
            case "f" => out(idx).asInstanceOf[Array[Float]](ii) = buf.getFloat()
            case "l" => out(idx).asInstanceOf[Array[Long]](ii) = buf.getLong()
            case "d" => out(idx).asInstanceOf[Array[Double]](ii) = buf.getDouble()
            case _ =>
              throw new RuntimeException("Unsupported type " + dataType)
          }
        }
      }
      else {
         dataType match {
          case "i" => out(idx) = buf.getInt().asInstanceOf[T]
          case "f" => out(idx) = buf.getFloat().asInstanceOf[T]
          case "l" => out(idx) = buf.getLong().asInstanceOf[T]
          case "d" => out(idx) = buf.getDouble().asInstanceOf[T]
          case _ =>
            throw new RuntimeException("Unsupported type " + dataType)
        }
      }
    }
   
    try {
      fc.close
      raf.close

      // Issue #22: Delete memory mapped file after used.
      new File(fileName).delete
    } catch {
      case e: IOException =>
        throw new IOException("Fail to close/delete memory mapped file " + fileName + ": " + e.toString)
    }
  }
}
