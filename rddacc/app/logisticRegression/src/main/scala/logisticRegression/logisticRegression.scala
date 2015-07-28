import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import Array._
import org.apache.spark.rdd._

import scala.math._
import java.util._

// comaniac: Import extended package
import org.apache.spark.acc_runtime._

class LogisticRegression(b_w: Broadcast_ACC[Array[Float]]) 
  extends Accelerator[Array[Float], Array[Float]] {

  val id: String = "Logistic"

  def getArg(idx: Int): Option[Broadcast_ACC[Array[Float]]] = {
    if (idx == 0)
      Some(b_w)
    else
      None
  }

  def getArgNum(): Int = 1

  def call(data: Array[Float]): Array[Float] = {
    val _L: Int = 10
    val _D: Int = 784

    val grad = new Array[Float](_L * _D)
    val dot = new Array[Float](1)
    val w = b_w.data

    for (i <- 0 until _L) {
      dot(0) = 0.0f
      for (j <- 0 until _D)
        dot(0) = dot(0) + w(i * _D + j) * data(j + _L)
      
      val c: Float = (1.0f / (1.0f + Math.exp(-data(i) * dot(0)).toFloat) - 1.0f) * data(i)

      for (j <- 0 until _D)
        grad(i * _D + j) = grad(i * _D + j) + c * data(j + _L)
    }
    grad
  }
}

object LogisticRegression {
    val L = 10
    val D = 784

    def main(args : Array[String]) {
      val sc = get_spark_context("LogisticRegression")
      val acc = new ACCRuntime(sc)

      if (args.length < 3) {
        System.err.println("Usage: LogisticRegression <file> <reps> <iter>")
        System.exit(1)
      }
      val rand = new Random(42)
      val ITERATION = args(2).toInt
      val upperbound: Float = 24.0f / (Math.sqrt(L + D)).toFloat;

      val reps: Int = args(1).toInt

      val dataPoints = acc.wrap(sc.textFile(args(0)).map(line => {
        val strArray = line.split(" ")
        val points = new Array[Float](L + D)
        for (i <- 0 until (L + D))
          points(i) = strArray(i).toFloat
        points
      }).repartition(reps))
      .cache()

      val pointNum = dataPoints.count
      println("Total " + pointNum + " points")

      val w = new Array[Float](L * D)
      for (i <- 0 until L) {
        for (j <- 0 until D)
          w(i * D + j) = (rand.nextFloat - 0.5f) * 2.0f * upperbound
      }

      for (k <- 1 to ITERATION) {
        println("On iteration " + k)
        var start_time = System.nanoTime
        val b_w = acc.wrap(sc.broadcast(w))
        val gradient = dataPoints
          /*.map(points => runOnJTP(points, w))*/
          .map_acc(new LogisticRegression(b_w))
          .reduce((a, b) => {
            val res = new Array[Float](L * D)
            for (i <- 0 until L) {
              for (j <- 0 until D)
                res(i * D + j) = a(i * D + j) + b(i * D + j)
            }
            res
          })
        
        for (i <- 0 until L) {
          for (j <- 0 until D)
            w(i * D + j) = w(i * D + j) - 0.13f * gradient(i * D + j) / pointNum;
        }
        var elapsed_time = System.nanoTime - start_time
        System.out.println("Time: "+ elapsed_time/1e6 + "ms")

        // Verification 
        //val errNum = dataPoints
        //  .map(points => predictor(w, points))
        //  .reduce((a, b) => (a + b))
        //println("Error rate: " + ((errNum.toFloat / pointNum.toFloat) * 100) + "%")
      }

      acc.stop()
    }

    def predictor(w: Array[Float], data: Array[Float]): Int = {
      val maxPred = new Array[Float](1)
      val maxIdx = new Array[Int](1)
      maxPred(0) = 0.0f
      
      for (i <- 0 until L) {
        val dot = new Array[Float](1)
        dot(0) = 0.0f
        for(j <- 0 until D)
          dot(0) = dot(0) + w(i * D + j) * data(L + j)
        dot(0) = dot(0) + w(i * D)
        val pred = 1 / (1 + Math.exp(-dot(0)).toFloat)
        if (pred > maxPred(0)) {
          maxPred(0) = pred
          maxIdx(0) = i
        }
      }
      if (data(maxIdx(0)) < 0.5)
        1
      else
        0
    }

     def runOnJTP(data: Array[Float], w: Array[Float]): Array[Float] = {
      val grad = new Array[Float](L * (D + 1))
      val dot = new Array[Float](1)

      for (i <- 0 until L) {
        dot(0) = 0.0f
        for (j <- 0 until D)
          dot(0) = dot(0) + w(i * (D + 1) + j) * data(j + L)
        
        val c: Float = (1.0f / (1.0f + Math.exp(-data(i) * dot(0)).toFloat) - 1.0f) * data(i)

        for (j <- 0 until D)
          grad(i * (D + 1) + j) = grad(i * (D + 1) + j) + c * data(j + L)
      }
      grad
    }
   

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)
        
        return new SparkContext(conf)
    }
}

