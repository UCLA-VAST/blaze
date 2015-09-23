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
import org.apache.spark.util.random._
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

// comaniac: Import extended package
import org.apache.spark.blaze._

// TODO: Tuple2 as an output type
class Tuple2Test extends Accelerator[Tuple2[Double, Double], Double] {
  val id: String = "Tuple2Test"

  def getArgNum(): Int = 0

  def getArg(idx: Int): Option[_] = None

  override def call(in: Tuple2[Double, Double]): Double = {
    in._1 + in._2
  }

  override def call(in: Iterator[Tuple2[Double, Double]]): Iterator[Double] = {
    val inAry = in.toArray
    val length: Int = inAry.length
    val outAry = new Array[Double](length)

    for (i <- 0 until length)
      outAry(i) = inAry(i)._1 + inAry(i)._2

    outAry.iterator
  }
}

object TestApp {
    def main(args : Array[String]) {
      val sc = get_spark_context("Tuple2 Test")
      val data = new Array[Double](1024).map(e => (random, random))
      val rdd = sc.parallelize(data, 8).cache

      val acc = new BlazeRuntime(sc)
      val rdd_acc = acc.wrap(rdd)

      println("map Result: " + rdd_acc.map_acc(new Tuple2Test).reduce((a, b) => (a + b)))
      println("mapPartition Result: " + rdd_acc.mapPartitions_acc(new Tuple2Test).reduce((a, b) => (a + b)))
      println("CPU Result: " + rdd_acc.map({case (a, b) => (a + b)}).reduce((a, b) => (a + b)))

      acc.stop()
    }

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)
        
        return new SparkContext(conf)
    }
}

