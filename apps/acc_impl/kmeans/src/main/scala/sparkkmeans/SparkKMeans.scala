import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

import org.apache.spark.acc_runtime._

class KMeansClassified(
  b_centers: Broadcast_ACC[Array[Float]], 
  b_D: Broadcast_ACC[Int]
  ) extends Accelerator[Array[Float], Int] {
  
  val id: String = "KMeans"

  def getArgNum = 2

  def getArg(idx: Int): Option[Broadcast_ACC[_]] = {
    if (idx == 0)
      Some(b_centers)
    else if (idx == 1)
      Some(b_D)
    else
      None
  }

  def call(in: Array[Float]): Int = {
    val centers = b_centers.data
    val D: Int = b_D.data
    val K: Int = centers.length / D

    var closest_center = -1
    var closest_center_dist = -1.0

    for (i <- 0 until K) {
      var allDiff = 0.0
      for (j <- 0 until D)
        allDiff = allDiff + pow(centers(i * D + j) - in(j), 2)
      val dist = sqrt(allDiff)
      
      if (closest_center == -1 || dist < closest_center_dist) {
        closest_center = i
        closest_center_dist = dist
      }
    }

    closest_center
  }
}

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

      val acc = new ACCRuntime(sc)

      val points: ShellRDD[Array[Float]] = acc.wrap(sc.textFile(inputPath)
        .map(line => {
          var fList: List[Float] = List()
          line.split(" ").foreach(e => (fList = fList :+ e.toFloat))
          fList.toArray
        }))
      points.cache()

      val samples: Array[Array[Float]] = points.takeSample(false, K, 99);
      val D: Int = samples(0).length
      println("Dimension: " + D)

      var centers = new Array[Float](K * D)
      for (i <- 0 until K) {
        for (j <- 0 until D)
          centers(i * D + j) = samples(i)(j)
      }

      val b_D = acc.wrap(sc.broadcast(D))
      for (iter <- 0 until iters) {
        val b_centers = acc.wrap(sc.broadcast(centers))
        val classified_center = points.map_acc(new KMeansClassified(b_centers, b_D))
        val classified = classified_center.zip(points)

        val counts = classified.countByKey()
        val sums = classified.reduceByKey((a, b) => {
            val ary = new Array[Float](D)
            for (ii <- 0 until D)
              ary(ii) = a(ii) + b(ii)
            ary
          })

        val averages = sums.map(kv => {
            val cluster_index: Int = kv._1;
            val p: Array[Float] = kv._2;
            val ary = new Array[Float](D)
            for (ii <- 0 until D)
              ary(ii) = p(ii) / counts(cluster_index)
            ary
          }).collect

        for (i <- 0 until K) {
          for (j <- 0 until D)
            centers(i * D + j) = averages(i)(j)
        }

        println("Iteration " + (iter + 1))
        for (i <- 0 until K) {
          print("  Cluster " + i + ", (")
          for (j <- 0 until (D - 1))
            print(centers(i * D + j) + ", ")
          println(centers(i * D + (D - 1)) + ")")
        }
      }
      acc.stop 
    }


    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)
        
        return new SparkContext(conf)
    }
}
