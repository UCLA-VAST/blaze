/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.blaze

import java.io._
import scala.util.matching.Regex
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.scheduler._
import org.apache.spark.broadcast._

/**
  * The entry point of Blaze runtime system. BlazeRuntime is mainly used for 
  * wrapping RDD and broadcast variables, and maintaining the broadcast data list
  * for releasing.
  *
  * @param sc Spark context.
  */
class BlazeRuntime(sc: SparkContext) extends Logging {

  // The application signature generated based on Spark application ID.
  val appSignature: String = sc.applicationId

  var accPort: Int = 1027

  // Configure the port for AccManager
  def setAccPort(port: Int) = {
    accPort = port
  }

  var BroadcastList: List[BlazeBroadcast[_]] = List()

  /**
    * This method should be called by the developer at the end of the application 
    * to release all broadcast blocks from Blaze manager and shutdown the SparkContext.
    * Ignore this method causes useless broadcast blocks occupy Blaze manager scratch memory.
    */
  def stop() = {
    if (BroadcastList.length == 0)
      logInfo("No broadcast block to be released")
    else {
      // FIXME: Currently we use the fixed port number (1027) for all managers
      val WorkerList: Array[(String, Int)] = (sc.getExecutorStorageStatus)
        .map(w => w.blockManagerId.host)
        .distinct
        .map(w => (w, accPort))

      logInfo("Releasing broadcast blocks from workers (" + WorkerList.length + "): " + 
        WorkerList.map(w => w._1).mkString(", "))

      val msg = DataTransmitter.buildMessage(appSignature, AccMessage.MsgType.ACCTERM)
  
      for (e <- BroadcastList) {
        DataTransmitter.addBroadcastData(msg, e.brdcst_id)
        logInfo("Broadcast block to be released: " + e.brdcst_id)
      }

      for (worker <- WorkerList) {
        try {
          val workerIP = Util.getIPByHostName(worker._1)
          if (!workerIP.isDefined)
            throw new RuntimeException("Cannot resolve host name " + worker._1)
          val transmitter = new DataTransmitter(workerIP.get, worker._2)
          transmitter.send(msg)
          val revMsg = transmitter.receive()
          if (revMsg.getType() == AccMessage.MsgType.ACCFINISH)
            logInfo("Successfully release " + BroadcastList.length + " broadcast blocks from Manager " + worker._1)
          else
            logInfo("Fail to release broadcast blocks from Manager " + worker._1)
        }
        catch {
          case e: Throwable =>
            val sw = new StringWriter
            logInfo("Fail to release broadcast data from Manager " + worker._1 + ": " + sw.toString)
        }
      }
    }
    // do not shutdown SparkContext here since BlazeRuntime does not construct
    // a SparkContext
    // sc.stop
    // logInfo("Application " + appSignature + " shutdown.")
  }

  /**
    * Wrap a Spark RDD in a ShellRDD of Blaze.
    */
  def wrap[T: ClassTag](rdd : RDD[T]) : ShellRDD[T] = {
    new ShellRDD[T](appSignature, rdd, accPort)
  }

  /**
    * Wrap a Spark broadcast in a BlazeBroadcast.
    */
  def wrap[T: ClassTag](bd : Broadcast[T]) : BlazeBroadcast[T] = {
    val newBrdcst = new BlazeBroadcast[T](appSignature, bd)
    BroadcastList = BroadcastList :+ newBrdcst
    newBrdcst
  }
}

