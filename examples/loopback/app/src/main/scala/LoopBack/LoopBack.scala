import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import org.apache.spark.rdd._

import scala.math._
import java.util._

import org.apache.spark.blaze._

class LoopBack() 
  extends Accelerator[Array[Double], Array[Double]] {

  val id: String = "LoopBack"
  def getArg(idx: Int): Option[BlazeBroadcast[_]] = None
  def getArgNum(): Int = 0

  override def call(data: Array[Double]): Array[Double] = {
    val out = new Array[Double](data.size)
    Array.copy(data, 0, out, 0, data.size)
    out
  }
}

object LoopBack {

  def genData(
      sc: SparkContext,
      n_elements: Int,
      n_parts: Int): RDD[Array[Double]] = {

    val data = sc.parallelize(0 until n_elements, n_parts).map{ idx =>
      val rand = new Random(42 + idx)
      val x = Array.fill[Double](8) { rand.nextGaussian() }
      x
    }
    data
  }

  def main(args : Array[String]) {

    if (args.length != 2) {
      System.err.println("Usage: LoopBack <#elements> <#parts>")
      System.exit(1)
    }
    val sc = get_spark_context("LoopBack")
    val acc = new BlazeRuntime(sc)

    val n_elements = args(0).toInt
    val n_parts = args(1).toInt

    val data = acc.wrap(genData(sc, n_elements, n_parts))

    val src = data.collect
    val dst = data.map_acc(new LoopBack).collect


    if (src.deep == dst.deep) {
      println("results correct") 
      acc.stop()
    }
    else {
      println("results incorrect") 
      println(src.deep.mkString("\n"))
      println(dst.deep.mkString("\n"))
      acc.stop()
      System.exit(1)
    }
  }

  def get_spark_context(appName : String) : SparkContext = {
    val conf = new SparkConf()
      conf.setAppName(appName)

      return new SparkContext(conf)
  }
}

