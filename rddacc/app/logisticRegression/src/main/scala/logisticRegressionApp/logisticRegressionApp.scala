import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

// comaniac: Import extended package
import org.apache.spark.acc_runtime._

class LogisticRegression(b_pi: Broadcast_ACC[Double]) extends Accelerator[Double, Double] {
  val id: String = "Circumference" // FIXME

  def getArg(idx: Int): Option[Broadcast_ACC[Double]] = {
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

object LogisticRegressionApp {
    def main(args : Array[String]) {
      val sc = get_spark_context("LogisticRegression App")
      val rdd = sc.textFile("/curr/cody/test/testInput.txt", 8)

      val acc = new ACCRuntime(sc)
      val rdd_acc = acc.wrap(rdd.map(a => a.toDouble))

      val w = new Array[Double](1)
      w(0) = 1.0
      for (i <- 0 until 5) {
        val b_w = acc.wrap(sc.broadcast(w(0)))

        rdd_acc.cache
        rdd_acc.collect
        val rdd_acc2 = rdd_acc.map_acc(new LogisticRegression(b_w))
        println("Result: " + rdd_acc2.reduce((a, b) => (a + b)))

        val sum = rdd_acc.map(e => e * 2.0 * w(0)).reduce((a, b) => (a + b))
        println("Expect: " + sum)
        w(0) = (w(0) + sum) / 1e5
      }
      acc.stop()
    }

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)
        
        return new SparkContext(conf)
    }
}

