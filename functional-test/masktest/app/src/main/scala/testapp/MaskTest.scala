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
import scala.util.Random
import org.apache.spark.rdd._
import java.net._

// comaniac: Import extended package
import org.apache.spark.blaze._

class MaskTest(v: Int) extends Accelerator[Double, Double] {
  val id: String = "MaskTest"

  def getArgNum(): Int = 1

  def getArg(idx: Int): Option[_] = {
    if (idx == 0)
      Some(v)
    else
      None
  }

  override def call(in: Double): Double = {
    if (in > 0.5)
      in + v
    else
      0
  }

  override def call(in: Iterator[Double]): Iterator[Double] = {
    val inAry = in.toArray
    val length: Int = inAry.length
    val outAry = new Array[Double](length)

    for (i <- 0 until length) {
      if (inAry(i) > 0.5)
        outAry(i) = inAry(i) + v
      else
        outAry(i) = 0
    }
    outAry.iterator
  }
}

object TestApp {
    def main(args : Array[String]) {
      val sc = get_spark_context("Test App")
      println("Functional test: AccRDD.sample_acc")
      println("Set random seed as 904401792 to activate TestSampler.")

      val rander = new Random(0)
      val data = new Array[Double](20000).map(e => rander.nextDouble)
      val rdd = sc.parallelize(data, 10)

      val acc = new BlazeRuntime(sc)
      val rdd_acc = acc.wrap(rdd)

      val v = 2

      rdd_acc.cache
      rdd_acc.collect
      val sampled_rdd_acc = rdd_acc.sample_acc(true, 0.4, 904401792)
      sampled_rdd_acc.cache
      sampled_rdd_acc.collect
      val rdd_acc2 = sampled_rdd_acc.map_acc(new MaskTest(v))
      println("Result: " + rdd_acc2.reduce((a, b) => (a + b)))
      println("App Mask: ")
      val rdd_cpu = rdd.map(a => { 
        if (a > 0.5)
          a + v
        else
          0
      })
      println("CPU result: " + rdd_cpu.reduce((a, b) => (a + b)))

      acc.stop()
    }

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)
        
        return new SparkContext(conf)
    }
}

