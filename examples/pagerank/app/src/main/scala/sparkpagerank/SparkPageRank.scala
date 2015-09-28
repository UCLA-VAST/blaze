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

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.blaze._
/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 */
object SparkPageRank {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <output dir> <iter>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("PageRank")
    val iters = if (args.length > 0) args(2).toInt else 1
    val sc = new SparkContext(sparkConf)
    val acc = new BlazeRuntime(sc)

    val lines = sc.textFile(args(0), 10)
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0).toInt, parts(1).toInt)
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0f)

    println("Total " + links.count + " links")

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.map({
        case (urls, rank) => 
        (urls.toArray, rank)
      })
      val new_rank = acc.wrap(contribs).map_acc(new PageRank)
//      val new_rank = contribs.map({ case (urls, rank) => 
//        rank / urls.length
//      })
      
      val real_contribs = contribs.zip(new_rank).map({
        case ((urls, old_rank), new_rank) =>
        (urls, new_rank)
      })
      .flatMap({ 
        case (urls, rank) =>
        urls.map(url => (url, rank))
      })
      ranks = real_contribs.reduceByKey(_ + _).mapValues(0.15f + 0.85f * _)

      println("Iteration " + i + " done.")
    }

    ranks.saveAsTextFile(args(1))
    val output = ranks.collect()
    println(output(0)._1 + " has rank: " + output(0)._2 + ".")

    acc.stop()
  }
}

class PageRank extends Accelerator[Tuple2[Array[Int], Float], Float] {
  val id: String = "PageRank"

  def getArgNum = 0

  def getArg(idx: Int) = None

  override def call(in: (Array[Int], Float)): Float = {
    in._2 / in._1.length
  }
}


