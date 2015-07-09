import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

// comaniac: Import extended package
import org.apache.spark.acc_runtime._

object TestApp {
    def main(args : Array[String]) {
      val sc = get_spark_context("Test App")
      val rdd = sc.textFile("/curr/cody/test/testInput.txt", 1)
      val rdd_acc = ACCWrapper.wrap(rdd.map(a => a.toDouble))

      val b = ACCWrapper.wrap(sc.broadcast(Array(1, 2, 3)))

//      rdd_acc.cache
//      rdd_acc.collect
//      val rdd_acc2 = rdd_acc.map_acc(a => (a + 1.0))
//      val rdd_acc3 = rdd_acc.map_acc(a => (a + 1.0)).map_acc(a => (a + 1.0))
//      println(rdd_acc2.reduce((a, b) => (a + b)))
    }

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)
        
        return new SparkContext(conf)
    }
}

