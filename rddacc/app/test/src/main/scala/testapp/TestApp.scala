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
//      val ref = rdd.collect
//      val dref = ref(0).toDouble

//      val bref = sc.broadcast(dref)
//      println(rdd.map(a => (a.toDouble + bref.value)).reduce((a, b) => (a + b)))

      println(rdd_acc.map_acc(a => (a + 1.0)).reduce((a, b) => (a + b)))
//      println(rdd_acc.map_acc(a => (a + 1.0)).reduce((a, b) => (a + b)))

    }

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)
        
        return new SparkContext(conf)
    }
}

