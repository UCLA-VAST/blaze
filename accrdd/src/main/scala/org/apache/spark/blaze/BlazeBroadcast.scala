package org.apache.spark.blaze

import java.io._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkException
import org.apache.spark.Logging
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

/**
  * A BlazeBroadcast variable. A BlazeBroadcast variable wraps a Spark broadcast variable with 
  * an unique broadcast ID within the application. BlazeBroadcast variables will be broadcast 
  * to the Blaze manager lazily and cached until the application has been terminated.
  *
  * @tparam T Type of broadcast data.
  */
class BlazeBroadcast[T: ClassTag](appId: String, bd: Broadcast[T]) extends java.io.Serializable {
  def getIntId() = Math
      .abs(("""\d+""".r findAllIn appId)
      .addString(new StringBuilder).toLong.toInt)

  var brdcst_id: Long = Util.getBlockID(getIntId(), bd.id.asInstanceOf[Int])
  lazy val data = bd.value
  var length: Int = 0
  var size: Int = 0
}
