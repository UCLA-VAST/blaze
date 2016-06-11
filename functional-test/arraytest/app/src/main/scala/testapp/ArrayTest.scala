import Array._
import scala.math._
import java.net._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.util.random._
import org.apache.spark.blaze._

class ArrayTest(v: BlazeBroadcast[Array[Double]]) 
  extends Accelerator[Array[Double], Array[Double]] {

  val id: String = "ArrayTest"

  def getArgNum(): Int = 1

  def getArg(idx: Int): Option[_] = {
    if (idx == 0)
      Some(v)
    else
      None
  }

  override def call(in: Array[Double]): Array[Double] = {
    val ary = new Array[Double](in.length)
    val s = v.data.sum
    for (i <- 0 until in.length) {
      ary(i) = in(i) + s
    }
    ary
  }

  override def call(in: Iterator[Array[Double]]): Iterator[Array[Double]] = {
    val inAry = in.toArray
    val length: Int = inAry.length
    val itemLength: Int = inAry(0).length
    val outAry = new Array[Array[Double]](length)
    val s = v.data.sum

    for (i <- 0 until length) {
      outAry(i) = new Array[Double](itemLength)
      for (j <- 0 until itemLength)
        outAry(i)(j) = inAry(i)(j) + s
    }

    outAry.iterator
  }
}

object TestApp {
  def main(args : Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("TestApp")

    val sc = new SparkContext(conf)
    val acc = new BlazeRuntime(sc)

    val v = Array(1.1, 2.2, 3.3)

    println("Functional test: array type AccRDD with array type broadcast value")

    val data = new Array[Array[Double]](256)
    for (i <- 0 until 256) {
      data(i) = new Array[Double](2).map(e => random)
    }
    val rdd = sc.parallelize(data, 256)

    val rdd_acc = acc.wrap(rdd)    
    val brdcst_v = acc.wrap(sc.broadcast(v))

    val res0 = rdd_acc.map_acc(new ArrayTest(brdcst_v)).collect
    val res1 = rdd_acc.mapPartitions_acc(new ArrayTest(brdcst_v)).collect
    val res2 = rdd.map(e => e.map(ee => ee + v.sum)).collect

    // compare results
    if (res0.deep != res1.deep ||
        res1.deep != res2.deep ||
        res0.deep != res2.deep)
    {
      println("input: \n" + data.deep.mkString("\n"))
        println("map result: \n" + res2.deep.mkString("\n"))
        println("map_acc results: \n" + res0.deep.mkString("\n"))
        println("mapParititions_acc results: \n" + res1.deep.mkString("\n"))
    }
    else {
      println("result correct")
    }
    acc.stop()
  }
}

