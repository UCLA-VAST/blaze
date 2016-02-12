import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.blaze._

object Kmeans {

  def main(args : Array[String]) {

    var conf = new SparkConf().setAppName("LogisticRegression")
    val sc = new SparkContext(conf)

    if (args.length < 3) {
      System.err.println("Usage: LogisticRegression <train_file> <k> <part_num>")
      System.exit(1)
    }

    val k = args(1).toInt
    val reps = args(2).toInt

    var data = MLUtils.loadLibSVMFile(sc, args(0))
    val splits = data.randomSplit(Array(0.8, 0.2), seed = 42L)
    val train = splits(0).map(e => e.features).repartition(reps).cache()

    val model = new KMeans()
      .setInitializationMode(KMeans.RANDOM)
      .setK(k)
      .setMaxIterations(50)
      .run(train)

    println("train finished.")

  }
}


