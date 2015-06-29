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
  val sizeof = mutable.HashMap[AccMessage.Data.Type, Int]()
  sizeof(AccMessage.Data.Type.INT) = 4
  sizeof(AccMessage.Data.Type.FLOAT) = 4
  sizeof(AccMessage.Data.Type.LONG) = 8
  sizeof(AccMessage.Data.Type.DOUBLE) = 8

  def serializePartition[T: ClassTag](input: Array[T], id: Int): (String, AccMessage.Data.Type, Int) = {
    val fileName: String = System.getProperty("java.io.tmpdir") + "/spark_acc" + id + ".dat"

     // Fetch size information
    val dataType: AccMessage.Data.Type = input(0).getClass.getName.replace("java.lang.", "").toLowerCase() match {
      case "int" => AccMessage.Data.Type.INT
      case "float" => AccMessage.Data.Type.FLOAT
      case "long" => AccMessage.Data.Type.LONG
      case "double" => AccMessage.Data.Type.DOUBLE
      case _ => null
    }

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
        case AccMessage.Data.Type.INT => buf.putInt(e.asInstanceOf[Int].intValue)
        case AccMessage.Data.Type.FLOAT => buf.putFloat(e.asInstanceOf[Float].floatValue)
        case AccMessage.Data.Type.LONG => buf.putLong(e.asInstanceOf[Long].longValue)
        case AccMessage.Data.Type.DOUBLE => buf.putDouble(e.asInstanceOf[Double].doubleValue)
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
    val dataType: AccMessage.Data.Type = out(0).getClass.getName.replace("java.lang.", "").toLowerCase() match {
      case "int" => AccMessage.Data.Type.INT
      case "float" => AccMessage.Data.Type.FLOAT
      case "long" => AccMessage.Data.Type.LONG
      case "double" => AccMessage.Data.Type.DOUBLE
      case _ => null
    }

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
        case AccMessage.Data.Type.INT => out(idx) = buf.getInt().asInstanceOf[T]
        case AccMessage.Data.Type.FLOAT => out(idx) = buf.getFloat().asInstanceOf[T]
        case AccMessage.Data.Type.LONG => out(idx) = buf.getLong().asInstanceOf[T]
        case AccMessage.Data.Type.DOUBLE => out(idx) = buf.getDouble().asInstanceOf[T]
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
