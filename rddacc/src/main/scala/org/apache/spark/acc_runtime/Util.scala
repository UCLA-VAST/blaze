package org.apache.spark.acc_runtime

import java.io._
import java.util.Calendar
import java.text.SimpleDateFormat
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.io.OutputStream    
import java.io.FileOutputStream

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._         
import scala.collection.mutable        
import scala.collection.mutable.HashMap
                                                
import org.apache.spark._                       
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._                   

object Util {

  // These config values should be moved to another sutiable place.
  // Format: | Broadcast, 1 | App ID, 32 | RDD ID, 18 | Partition ID, 12 | Sub-block ID, 1 |
  val APP_BIT_START = 31
  val RDD_BIT_START = 13
  val PARTITION_BIT_START = 1
  val BLOCK_BIT_START = 0

  val logFile = new PrintWriter(new File("acc_runtime.log"))
  println("Logging ACCRuntime at acc_runtime.log")

  def logInfo[T: ClassTag](clazz: T, msg: String) = {
    val clazzName = clazz.getClass.getName.replace("org.apache.spark.acc_runtime.", "")
    var name = clazzName
    if (name.indexOf("$") != -1)
      name = name.substring(0, name.indexOf("$"))

    val time = Calendar.getInstance.getTime
    logFile.write("[INFO][" + time + "] " + name + ": " + msg + "\n")
  }

  def closeLog() = {
    logFile.close
  }

  def logMsg(msgBuilder: AccMessage.TaskMsg.Builder) = {
    val msg = msgBuilder.build()
    val time = Calendar.getInstance.getTime
    logFile.write("[INFO][" + time + "] Message: ")
    logFile.write("Type: " + msg.getType() + ", ")
    logFile.write("Data: " + msg.getDataCount() + "\n")

    for (i <- 0 until msg.getDataCount()) {
      logFile.write("Data " + i + ": ")
      if (msg.getData(i).hasPartitionId())
        logFile.write("ID: " + msg.getData(i).getPartitionId() + ", ")
      if (msg.getData(i).hasLength())
        logFile.write("Length: " + msg.getData(i).getLength() + ", ")
      if (msg.getData(i).hasSize())
        logFile.write("Size: " + msg.getData(i).getSize() + ", ")
      if (msg.getData(i).hasPath())
        logFile.write("Path: " + msg.getData(i).getPath()) 
      logFile.write("\n")
    }
  }

  def logMsg(msg: AccMessage.TaskMsg) = {
    val time = Calendar.getInstance.getTime
    logFile.write("[INFO][" + time + "] Message: ")
    logFile.write("Type: " + msg.getType() + ", ")
    logFile.write("Data: " + msg.getDataCount() + "\n")

    for (i <- 0 until msg.getDataCount()) {
      logFile.write("Data " + i + ": ")
      if (msg.getData(i).hasPartitionId())
        logFile.write("ID: " + msg.getData(i).getPartitionId() + ", ")
      if (msg.getData(i).hasLength())
        logFile.write("Length: " + msg.getData(i).getLength() + ", ")
      if (msg.getData(i).hasSize())
        logFile.write("Size: " + msg.getData(i).getSize() + ", ")
      if (msg.getData(i).hasPath())
        logFile.write("Path: " + msg.getData(i).getPath()) 
      logFile.write("\n")
    }
  }


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

  def getBlockID(first: Int, second: Int, third: Int = -1, fourth: Int = -1): Long = {
    if (third == -1) { // broadcast block
      -((first.toLong << APP_BIT_START) + (second + 1))
    }
    else { // normal block
      (first.toLong << APP_BIT_START) + (second << RDD_BIT_START) + 
        (third << PARTITION_BIT_START) + (fourth << BLOCK_BIT_START)
    }
  }

  def serializePartition[T: ClassTag](prefix: Int, input: Array[T], id: Long): (String, Int) = {
    var fileName: String = System.getProperty("java.io.tmpdir") + "/" + prefix
    if (id < 0) // Broadcast data
      fileName = fileName + "_brdcst_" + (-id) + ".dat"
    else // Normal data
      fileName = fileName + id + ".dat"

    val typeName = input(0).getClass.getName.replace("java.lang.", "").toLowerCase()
    val dataType: String = typeName.replace("[", "")(0).toString // Only fetch the initial
    val typeSize: Int = getTypeSizeByName(dataType)

    val isArray: Boolean = typeName.contains("[")

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
        logInfo(this, "Fail to create memory mapped file " + fileName + ": " + e.toString)
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
        logInfo(this, "Fail to close memory mapped file " + fileName + ": " + e.toString)
    }

    (fileName, bufferLength(0))
  }

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
        logInfo(this, "Fail to read memory mapped file " + fileName + ": " + e.toString)
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
      fc.close()
      raf.close()
    } catch {
      case e: IOException =>
        logInfo(this, "Fail to close memory mapped file " + fileName + ": " + e.toString)
    }
  }
}
