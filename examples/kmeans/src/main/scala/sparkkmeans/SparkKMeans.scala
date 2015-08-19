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
import scala.math._
import org.apache.spark.rdd._
import java.net._

import org.apache.spark.mllib.linalg.Vectors

object SparkKMeans {
    def main(args : Array[String]) {
      if (args.length != 3) {
        println("usage: SparkKMeans run K iters input-path");
        return;
      }
      val sc = get_spark_context("Spark KMeans");

      val K = args(0).toInt;
      val iters = args(1).toInt;
      val inputPath = args(2);

      val points = sc.textFile(inputPath)
        .map(line => Vectors.dense(line.split(' ').map(_.toDouble)))
        .cache()

      val cost = KMeans.train(points, K, iters)
      println("Within set sum of squared errors = " + cost)
    }


    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)
        
        return new SparkContext(conf)
    }
}
