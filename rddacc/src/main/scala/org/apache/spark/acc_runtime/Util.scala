package org.apache.spark.acc_runtime

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
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

  def getTypeSizeByRDD[T: ClassTag](rdd: RDD[T]): Int = {
    if (classTag[T] == classTag[Byte])        1
    else if (classTag[T] == classTag[Short])  2
    else if (classTag[T] == classTag[Char])   2
    else if (classTag[T] == classTag[Int])    4
    else if (classTag[T] == classTag[Float])  4
    else if (classTag[T] == classTag[Long])   8
    else if (classTag[T] == classTag[Double]) 8
    else 0
  }

  def getTypeSizeByName(dataType: String): Int = {
    val typeSize: Int = dataType match {
      case "int" => 4
      case "float" => 4
      case "long" => 8
      case "double" => 8
      case _ => 0
    }
    typeSize
  }

  def serializePartition[T: ClassTag](input: Array[T], id: Int): (String, Int) = {
    val fileName: String = System.getProperty("java.io.tmpdir") + "/spark_acc" + id + ".dat"
    val dataType: String = input(0).getClass.getName.replace("java.lang.", "").toLowerCase()
    val typeSize: Int = getTypeSizeByName(dataType)

    // Create and write memory mapped file
    var raf: RandomAccessFile = null

    try {
      raf = new RandomAccessFile(fileName, "rw")
    } catch {
      case e: IOException =>
        println("Fail to create memory mapped file " + fileName + ": " + e.toString)
    }
    val fc: FileChannel = raf.getChannel()
    val buf: ByteBuffer = fc.map(MapMode.READ_WRITE, 0, input.length * typeSize)
    buf.order(ByteOrder.LITTLE_ENDIAN)

    for (e <- input) {
      dataType match {
        case "int" => buf.putInt(e.asInstanceOf[Int].intValue)
        case "float" => buf.putFloat(e.asInstanceOf[Float].floatValue)
        case "long" => buf.putLong(e.asInstanceOf[Long].longValue)
        case "double" => buf.putDouble(e.asInstanceOf[Double].doubleValue)
        case _ =>
          throw new RuntimeException("Unsupported type" + dataType)
      }
    }
   
    try {
      fc.close()
      raf.close()
    } catch {
      case e: IOException =>
        println("Fail to close memory mapped file " + fileName + ": " + e.toString)
    }

    (fileName, input.length)
  }

  def readMemoryMappedFile[T: ClassTag](
      out: Array[T], 
      offset: Int, 
      length: Int,
      fileName: String) = {

     // Fetch size information
    val dataType: String = out(0).getClass.getName.replace("java.lang.", "").toLowerCase()
    val typeSize: Int = getTypeSizeByName(dataType)

    // Create and write memory mapped file
    var raf: RandomAccessFile = null

    try {
      raf = new RandomAccessFile(fileName, "r")
    } catch {
      case e: IOException =>
        println("Fail to read memory mapped file " + fileName + ": " + e.toString)
    }

    val fc: FileChannel = raf.getChannel()
    val buf: ByteBuffer = fc.map(MapMode.READ_ONLY, 0, length * typeSize)
    buf.order(ByteOrder.LITTLE_ENDIAN)

    var idx: Int = offset
    while (idx < offset + length) {
      dataType match {
        case "int" => out(idx) = buf.getInt().asInstanceOf[T]
        case "float" => out(idx) = buf.getFloat().asInstanceOf[T]
        case "long" => out(idx) = buf.getLong().asInstanceOf[T]
        case "double" => out(idx) = buf.getDouble().asInstanceOf[T]
        case _ =>
          throw new RuntimeException("Unsupported type" + dataType)
      }
      idx = idx + 1
    }
   
    try {
      fc.close()
      raf.close()
    } catch {
      case e: IOException =>
        println("Fail to close memory mapped file " + fileName + ": " + e.toString)
    }
  }
}
