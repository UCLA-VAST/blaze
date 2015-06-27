package org.apache.spark.rddacc

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
  sizeof("java.lang.Int") = 4
  sizeof("java.lang.Float") = 4
  sizeof("java.lang.Long") = 8
  sizeof("java.lang.Double") = 8

  def serializePartition[T: ClassTag](input: Array[T], id: Int): String = {
    val fileName: String = System.getProperty("java.io.tmpdir") + "/spark_acc" + id + ".dat"

     // Fetch size information
    val dataType: String = input(0).getClass.getName

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
        case "java.lang.Int" => buf.putInt(e.asInstanceOf[Int].intValue)
        case "java.lang.Float" => buf.putFloat(e.asInstanceOf[Float].floatValue)
        case "java.lang.Long" => buf.putLong(e.asInstanceOf[Long].longValue)
        case "java.lang.Double" => buf.putDouble(e.asInstanceOf[Double].doubleValue)
      }
    }
   
    try {
      fc.close()
      raf.close()
    } catch {
      case e: IOException =>
        println("Fail to close memory mapped file " + fileName + ": " + e.toString)
    }

    fileName
  }
}
