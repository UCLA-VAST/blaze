import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

// comaniac: Import extended package
import org.apache.spark.acc_runtime._

class SimpleAddition extends Accelerator[Double, Double] {
  val id: String = "SimpleAddition"

  def getArgNum(): Int = 0

  def getArg(idx: Int): Option[Broadcast_ACC[_]] = {
    None
  }

  def call(in: Double): Double = {
    in + 1.0
  }
}

object TestApp {
    def main(args : Array[String]) {
      val sc = get_spark_context("Test App")
      val rdd = sc.textFile("/curr/cody/test/testInput.txt", 1)

      val acc = new ACCRuntime(sc)
      val rdd_acc = acc.wrap(rdd.map(a => a.toDouble))

      val b = acc.wrap(sc.broadcast(Array(1, 2, 3)))
//      val c = acc.wrap(sc.broadcast(5))

      rdd_acc.cache
      rdd_acc.collect
      val rdd_acc2 = rdd_acc.map_acc(new SimpleAddition())
      println(rdd_acc2.reduce((a, b) => (a + b)))

      acc.stop()
    }

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)
        
        return new SparkContext(conf)
    }
}

