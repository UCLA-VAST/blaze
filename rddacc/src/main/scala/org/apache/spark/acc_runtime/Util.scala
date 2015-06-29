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

import scala.reflect.ClassTag                   
import scala.reflect.runtime.universe._         
import scala.collection.mutable        
import scala.collection.mutable.HashMap
                                                
import org.apache.spark._                       
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._                   

object Util {
  val sizeof = mutable.HashMap[String, Int]()
  sizeof("int") = 4
  sizeof("float") = 4
  sizeof("long") = 8
  sizeof("double") = 8

  def serializePartition[T: ClassTag](input: Array[T], id: Int): (String, String, Int) = {
    val fileName: String = System.getProperty("java.io.tmpdir") + "/spark_acc" + id + ".dat"

     // Fetch size information
    val dataType: String = input(0).getClass.getName.replace("java.lang.", "").toLowerCase()

    if (!sizeof.exists(_._1 == dataType)) // TODO: Support String and objects
      throw new RuntimeException("Unsupported type " + dataType);

    val typeSize: Int = sizeof(dataType)

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

    (fileName, dataType, input.length * typeSize)
  }

  def readMemoryMappedFile[T: ClassTag](out: Array[T], fileName: String) = {
     // Fetch size information
    val dataType: String = out(0).getClass.getName.replace("java.lang.", "").toLowerCase()

    if (!sizeof.exists(_._1 == dataType)) // TODO: Support String and objects
      throw new RuntimeException("Unsupported type " + dataType);

    val typeSize: Int = sizeof(dataType)

    // Create and write memory mapped file
    var raf: RandomAccessFile = null

    try {
      raf = new RandomAccessFile(fileName, "r")
    } catch {
      case e: IOException =>
        println("Fail to read memory mapped file " + fileName + ": " + e.toString)
    }

    val fc: FileChannel = raf.getChannel()
    val buf: ByteBuffer = fc.map(MapMode.READ_ONLY, 0, out.length * typeSize)
    buf.order(ByteOrder.LITTLE_ENDIAN)

    var idx: Int = 0
    while (idx < out.length) {
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
