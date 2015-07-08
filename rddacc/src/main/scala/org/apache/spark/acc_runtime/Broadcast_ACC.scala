package org.apache.spark.acc_runtime

import java.io.Serializable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkException
import org.apache.spark.Logging
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

class Broadcast_ACC[T: ClassTag](val bd: Broadcast[T]) {
  val acc_id = bd.id + "_brdcst"
  val data = bd.value
  
  val transmitter = new DataTransmitter()
  var msg = transmitter.buildRequest(acc_id, Array(0))
  transmitter.send(msg)
  var revMsg = transmitter.receive()

  if (revMsg.getType() != AccMessage.MsgType.ACCGRANT)
    throw new RuntimeException("Broadcast failed.")

  val dataMsg = transmitter.buildData()
 
  if (data.getClass.isArray) {
    val arrayData = data.asInstanceOf[Array[_]]
    val mappedFileInfo = Util.serializePartition(arrayData, acc_id)
    val typeSize = Util.getTypeSizeByName(arrayData(0).getClass.getName.replace("java.lang", "").toLowerCase)
    transmitter.addData(dataMsg, -1, arrayData.length,
        arrayData.length * typeSize, 0, mappedFileInfo._1)
  }
  else {
    val longData: Long = Util.casting(data, classOf[Long])
    transmitter.addScalarData(dataMsg, longData)
  }

  transmitter.send(dataMsg)
  revMsg = transmitter.receive()

  if (revMsg.getType() != AccMessage.MsgType.ACCFINISH)
    throw new RuntimeException("Broadcast failed.")
  
}
