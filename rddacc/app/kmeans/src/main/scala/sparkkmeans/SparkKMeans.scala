import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

import org.apache.spark.acc_runtime._

class KMeansClassified(b_centers: Broadcast_ACC[Array[Float]]) 
  extends Accelerator[Array[Float], Int] {
  
  val id: String = "KMeans"

  def getArgNum = 1

  def getArg(idx: Int): Option[Broadcast_ACC[_]] = {
    if (idx == 0)
      Some(b_centers)
    else
      None
  }

  def call(in: Array[Float]): Int = {
    val x = in(0)
    val y = in(1)
    val z = in(2)
    val centers = b_centers.data
    val K: Int = centers.length / 3

    var closest_center = -1
    var closest_center_dist = -1.0

    for (i <- 0 until K) {
      val diffx = centers(i * 3 + 0) - x
      val diffy = centers(i * 3 + 1) - y
      val diffz = centers(i * 3 + 2) - z
      val dist = sqrt(pow(diffx, 2) + pow(diffy, 2) + pow(diffz, 2))

      if (closest_center == -1 || dist < closest_center_dist) {
        closest_center = i
        closest_center_dist = dist
      }
    }

    closest_center
  }
}

class Point(x:Float, y:Float, z:Float) extends java.io.Externalizable {
    private var _x = x
    private var _y = y
    private var _z = z

    def this() = this(0.0f, 0.0f, 0.0f)

    def get_x : Float = return _x
    def get_y : Float = return _y
    def get_z : Float = return _z

    def readExternal(in:java.io.ObjectInput) {
        _x = in.readFloat
        _y = in.readFloat
        _z = in.readFloat
    }

    def writeExternal(out:java.io.ObjectOutput) {
        out.writeFloat(_x)
        out.writeFloat(_y)
        out.writeFloat(_z)
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

      val samples : Array[Array[Float]] = points.takeSample(false, K, 99);
      var centers = new Array[Float](K * 3)
      for (i <- 0 until K) {
        for (j <- 0 until 3)
          centers(i * 3 + j) = samples(i)(j)
      }

      for (iter <- 0 until iters) {
        val b_centers = acc.wrap(sc.broadcast(centers))
        val classified_center = points.map_acc(new KMeansClassified(b_centers))
        val classified = classified_center.zip(points)

        val counts = classified.countByKey()
        val sums = classified.reduceByKey((a, b) => Array(a(0) + b(0), a(1) + b(1), a(2) + b(2)))

        val averages = sums.map(kv => {
            val cluster_index: Int = kv._1;
            val p: Array[Float] = kv._2;
            Array(
              p(0) / counts(cluster_index),
              p(1) / counts(cluster_index),
              p(2) / counts(cluster_index)) 
          }).collect

        
        for (i <- 0 until K) {
          for (j <- 0 until 3)
            centers(i * 3 + j) = averages(i)(j)
        }

        println("Iteration " + (iter + 1))
        for (i <- 0 until K) {
          print("  Cluster " + i + ", (")
          for (j <- 0 until 2)
            print(centers(i * 3 + j) + ", ")
          println(centers(i * 3 + 2) + ")")
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
