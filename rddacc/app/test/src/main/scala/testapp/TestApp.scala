import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

// comaniac: Import extended package
import org.apache.spark.rddacc._

object TestApp {
    def main(args : Array[String]) {
      val sc = get_spark_context("Test App")
      val rdd = sc.textFile("/curr/cody/test/testInput.txt", 10)
      val rdd_acc = ACCWrapper.wrap(rdd)
      println(rdd_acc.map(a => (a.toDouble + 1.0)).reduce((a, b) => (a + b)))
      println(rdd_acc.map_acc(a => (a.toDouble + 1.0)).reduce((a, b) => (a + b)))
    }

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)
        
        return new SparkContext(conf)
    }
}

