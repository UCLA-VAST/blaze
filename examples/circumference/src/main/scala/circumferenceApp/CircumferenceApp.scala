import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

// comaniac: Import extended package
import org.apache.spark.blaze._

class Circumference(b_pi: BlazeBroadcast[Double]) extends Accelerator[Double, Double] {
  val id: String = "Circumference"

  def getArg(idx: Int): Option[BlazeBroadcast[Double]] = {
    if (idx == 0)
      Some(b_pi)
    else
      None
  }

  def getArgNum(): Int = 1

  def call(in: Double): Double = {
    in * 2.0 * b_pi.data
  }
}

object CircumferenceApp {
    def main(args : Array[String]) {
      val sc = get_spark_context("Circumference App")
      val rdd = sc.textFile("/curr/cody/test/testInput.txt", 15)

      val acc = new BlazeRuntime(sc)
      val rdd_acc = acc.wrap(rdd.map(a => a.toDouble))

      val b_pi = acc.wrap(sc.broadcast(3.1415926))

      rdd_acc.cache
      rdd_acc.collect
      val rdd_acc2 = rdd_acc.map_acc(new Circumference(b_pi))
      println("Result: " + rdd_acc2.reduce((a, b) => (a + b)))

      println("Expect: " + rdd_acc.map(e => e * 2.0 * 3.1415926).reduce((a, b) => (a + b)))

      acc.stop()
    }

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)
        
        return new SparkContext(conf)
    }
}

