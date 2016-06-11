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

class MaskTest() extends Accelerator[Double, Double] {
  val id: String = "MaskTest"

  def getArg(idx: Int): Option[_] = None
  def getArgNum(): Int = 0

  override def call(in: Iterator[Double]): Iterator[Double] = {
    in.filter { a => 
      a > 0.5 
    }
  }
}

object TestApp {
    def main(args : Array[String]) {
      val sc = get_spark_context("Test App")
      println("Functional test: AccRDD.sample_acc")
      println("Set random seed as 904401792 to activate TestSampler.")

      val rander = new Random(0)
      val data = new Array[Double](150).map(e => rander.nextDouble)
      val rdd = sc.parallelize(data, 4).cache()

      val acc = new BlazeRuntime(sc)
      val rdd_acc = acc.wrap(rdd)

      val rdd_sampled = rdd_acc.sample_acc(true, 0.4, 904401792)
      val res_acc = rdd_sampled.mapPartitions_acc(new MaskTest()).collect
      val res_cpu = rdd.mapPartitions(iter => {
        val out = new Array[Double](30)
        for (i <- 0 until 30)
          out(i) = iter.next
        out.iterator
      }).collect

      if (res_cpu.deep != res_acc.deep) {
        println("CPU result: \n" + res_cpu.deep.mkString("\n"))
        println("ACC result: \n" + res_acc.deep.mkString("\n"))
      } else {
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

