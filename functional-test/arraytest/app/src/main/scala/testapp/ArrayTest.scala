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

class ArrayTest(v: BlazeBroadcast[Array[Double]]) 
  extends Accelerator[Array[Double], Array[Double]] {

  val id: String = "ArrayTest"

  def getArgNum(): Int = 1

  def getArg(idx: Int): Option[_] = {
    if (idx == 0)
      Some(v)
    else
      None
  }

  override def call(in: Array[Double]): Array[Double] = {
    val ary = new Array[Double](in.length)
    val s = v.data.sum
    for (i <- 0 until in.length) {
      ary(i) = in(i) + s
    }
    ary
  }

  override def call(in: Iterator[Array[Double]]): Iterator[Array[Double]] = {
    val inAry = in.toArray
    val length: Int = inAry.length
    val itemLength: Int = inAry(0).length
    val outAry = new Array[Array[Double]](length)
    val s = v.data.sum

    for (i <- 0 until length) {
      outAry(i) = new Array[Double](itemLength)
      for (j <- 0 until itemLength)
        outAry(i)(j) = inAry(i)(j) + s
    }

    outAry.iterator
  }
}

object TestApp {
    def main(args : Array[String]) {
      val sc = get_spark_context("Test App")
      val acc = new BlazeRuntime(sc)

      val v = Array(1.1, 2.2, 3.3)

      println("Functional test: array type AccRDD with array type broadcast value")

      val data = new Array[Array[Double]](16)
      for (i <- 0 until 16) {
        data(i) = new Array[Double](8).map(e => random)
      }
      val rdd = sc.parallelize(data, 4)

      val rdd_acc = acc.wrap(rdd)    
      val brdcst_v = acc.wrap(sc.broadcast(v))

      val res0 = rdd_acc.map_acc(new ArrayTest(brdcst_v)).collect
      val res1 = rdd_acc.mapPartitions_acc(new ArrayTest(brdcst_v)).collect
      val res2 = rdd.map(e => e.map(ee => ee + v.sum)).collect

      // compare results
      if (res0.deep != res1.deep ||
          res1.deep != res2.deep ||
          res0.deep != res2.deep)
      {
        println("input: \n" + data.deep.mkString("\n"))
        println("map result: \n" + res2.deep.mkString("\n"))
        println("map_acc results: \n" + res0.deep.mkString("\n"))
        println("mapParititions_acc results: \n" + res1.deep.mkString("\n"))
      }
      else {
        println("result correct")
      }
      acc.stop()
    }

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)
        
        return new SparkContext(conf)
    }
}

