package org.apache.spark.acc_runtime

import java.io.Serializable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkException
import org.apache.spark.Logging
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

class Broadcast_ACC[T: ClassTag](val bd: Broadcast[T]) extends java.io.Serializable {
  var brdcst_id = Util.getBlockID(bd.id.asInstanceOf[Int])
  val data = bd.value
  var isBroadcast: Boolean = false

  try {
    val transmitter = new DataTransmitter()
    val msg = transmitter.buildMessage(AccMessage.MsgType.ACCBROADCAST)
 
    if (data.getClass.isArray) {
      val arrayData = data.asInstanceOf[Array[_]]
      val mappedFileInfo = Util.serializePartition(arrayData, brdcst_id)
      val typeSize = Util.getTypeSizeByName(arrayData(0).getClass.getName.replace("java.lang", "").toLowerCase)
      transmitter.addData(msg, brdcst_id, arrayData.length,
          arrayData.length * typeSize, 0, mappedFileInfo._1)
    }
    else {
      val longData: Long = Util.casting(data, classOf[Long])
      transmitter.addScalarData(msg, brdcst_id, longData)
    }

    transmitter.send(msg)
    val revMsg = transmitter.receive()

    if (revMsg.getType() != AccMessage.MsgType.ACCFINISH)
      throw new RuntimeException("Broadcast failed.")
    isBroadcast = true
  }
  catch {
    case e: Throwable =>
      println("Fail to broadcast data: " + e) 
  }  
}
