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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import org.apache.spark.rdd._

import scala.math._
import java.util._

// comaniac: Import extended package
import org.apache.spark.blaze._

class LoopBack() 
  extends Accelerator[Array[Float], Array[Float]] {

  val id: String = "LoopBack"

  def getArg(idx: Int): Option[BlazeBroadcast[_]] = {
    None
  }

  def getArgNum(): Int = 0

  def call(data: Array[Float]): Array[Float] = {
    val D = 768
    val out = new Array[Float](D)
    
    for (i <- 0 until D) {
      out(i) = data(i)
    }
    out
  }
}

object LoopBack {
    val D = 768

    def main(args : Array[String]) {
      val sc = get_spark_context("LoopBack")
      val acc = new BlazeRuntime(sc)

      if (args.length < 1) {
        System.err.println("Usage: LoopBack <file>")
        System.exit(1)
      }

      val reps: Int = 8

      val dataPoints = acc.wrap(sc.textFile(args(0)).map(line => {
        val strArray = line.split(" ")
        val points = new Array[Float](D)
        for (i <- 0 until (D))
          points(i) = strArray(i).toFloat
        points
      }).repartition(reps))
      .cache()

      val pointNum = dataPoints.count
      println("Total " + pointNum + " data")

      var start_time = System.nanoTime
      val result = dataPoints.map_acc(new LoopBack).collect.length
      println("Result " + result + " data")
        
      var elapsed_time = System.nanoTime - start_time
      System.out.println("Time: "+ elapsed_time/1e6 + "ms")

      acc.stop()
    }

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)
        
        return new SparkContext(conf)
    }
}

