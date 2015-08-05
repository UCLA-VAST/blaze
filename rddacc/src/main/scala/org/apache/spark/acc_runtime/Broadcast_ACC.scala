package org.apache.spark.acc_runtime

import java.io._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkException
import org.apache.spark.Logging
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

class Broadcast_ACC[T: ClassTag](appId: Int, bd: Broadcast[T]) extends java.io.Serializable {
  var brdcst_id: Long = Util.getBlockID(appId, bd.id.asInstanceOf[Int])
  val data = bd.value
  var isBroadcast: Boolean = false // Issue #21: Never used now
  var length: Int = 0
  var size: Int = 0
/*
  try {
    // Issue #21: Don't send broadcast data here because only driver will execute this constructor.

    val transmitter = new DataTransmitter()
    val msg = transmitter.buildMessage(AccMessage.MsgType.ACCBROADCAST)
 
    if (data.getClass.isArray) {
      val arrayData = data.asInstanceOf[Array[_]]
      val mappedFileInfo = Util.serializePartition(appId, arrayData, brdcst_id)
      val typeName = arrayData(0).getClass.getName.replace("java.lang.", "").toLowerCase
      val typeSize = Util.getTypeSizeByName(typeName)
      assert(typeSize != 0, "Cannot find the size of type " + typeName)

      transmitter.addData(msg, brdcst_id, arrayData.length,
          arrayData.length * typeSize, 0, mappedFileInfo._1)
      length = arrayData.length
      size = arrayData.length * typeSize
    }
    else {
      val longData: Long = Util.casting(data, classOf[Long])
      transmitter.addScalarData(msg, brdcst_id, longData)
      length = 1
      size = 4
    }

    transmitter.send(msg)

    val revMsg = transmitter.receive()
    if (revMsg.getType() != AccMessage.MsgType.ACCFINISH)
      throw new RuntimeException("Manager returns failure.")
    isBroadcast = true
  }
  catch {
    case e: Throwable =>
      val sw = new StringWriter
      e.printStackTrace(new PrintWriter(sw))
      Util.logInfo(this, "Fail to broadcast data: " + sw.toString)
  }  
*/
}
