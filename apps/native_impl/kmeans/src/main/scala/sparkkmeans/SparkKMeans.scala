import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
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

      val clusters = KMeans.train(points, K, iters, 1, "random", 99)
      println("Within set sum of squared errors = " + clusters.computeCost(points))
    }


    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)
        
        return new SparkContext(conf)
    }
}
