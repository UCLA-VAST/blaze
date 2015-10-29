import Array._
import scala.math._
import scala.util.Random
import java.net._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
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
    (in, v.data).zipped.map(_ + _)
  }
}

object TestApp {
  def main(args : Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("TestApp")

    val sc = new SparkContext(conf)
    val acc = new BlazeRuntime(sc)

    var num_sample = 32
    var num_part = 4

    if (args.size == 2) {
      num_sample = args(0).toInt
      num_part = args(1).toInt
    }

    val data = Array.fill(num_sample)(Array.fill(8)(Random.nextDouble));
    val offset = Array.fill(8)(Random.nextDouble);

    val rdd = sc.parallelize(data, num_part)
    val rdd_acc = acc.wrap(rdd)    
    
    val bc_data = sc.broadcast(offset)
    val bc_data_acc = acc.wrap(bc_data)

    val res_acc = rdd_acc.map_acc(new ArrayTest(bc_data_acc)).collect
    val res_cpu = rdd.map(e => (e, bc_data.value).zipped.map(_ + _)).collect

    // compare results
    if (res_acc.deep != res_cpu.deep)
    {
      println("Result incorrect")
      println("map result: \n" + res_cpu.deep.mkString("\n"))
      println("map_acc results: \n" + res_acc.deep.mkString("\n"))
    }
    else {
      println("result correct")
    }
    acc.stop()
  }
}

