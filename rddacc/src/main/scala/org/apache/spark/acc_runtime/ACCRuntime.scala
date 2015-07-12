package org.apache.spark.acc_runtime

import scala.util.matching.Regex
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.scheduler._
import org.apache.spark.broadcast._

class ACCRuntime(sc: SparkContext) extends Logging {

  // FIXME: Not a perfect solution
  val appSignature: Int = Math.abs(("""\d+""".r findAllIn sc.applicationId).addString(new StringBuilder).toLong.toInt)
  var BroadcastList: List[Broadcast_ACC[_]] = List()

  def stop() = {
    try {
      val transmitter = new DataTransmitter()
      val msg = transmitter.buildMessage(AccMessage.MsgType.ACCBROADCAST)

      for (e <- BroadcastList) {
        transmitter.addBroadcastData(msg, e.brdcst_id)
      }
      transmitter.send(msg)
      val revMsg = transmitter.receive()
      if (revMsg.getType() == AccMessage.MsgType.ACCFINISH)
        println("Successfully release broadcast blocks from Manager")
      else
        println("Fail to release broadcast blocks from Manager")
    }
    catch {
      case e: Throwable =>
        println("Fail to release broadcast data: " + e)
    }
    sc.stop
  }

  def wrap[T: ClassTag](rdd : RDD[T]) : ShellRDD[T] = {
    new ShellRDD[T](appSignature, rdd)
  }

  def wrap[T: ClassTag](bd : Broadcast[T]) : Broadcast_ACC[T] = {
    val newBrdcst = new Broadcast_ACC[T](appSignature, bd)
    BroadcastList = BroadcastList :+ newBrdcst
    newBrdcst
  }
}

