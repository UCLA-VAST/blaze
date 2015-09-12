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

    if (args.length < 2) {
      System.err.println("Usage: LogisticRegression <train_file> <k> <part_num>")
      System.exit(1)
    }

    val k = args(1).toInt
    val reps = args(2).toInt

    val train = sc.textFile(args(0)).map( line => {
        var parts = line.split(',') 
        Vectors.dense(parts(1).split(' ').map(_.toDouble))
        }).repartition(reps).cache()

    val model = new KMeans()
      .setInitializationMode(KMeans.RANDOM)
      .setK(k)
      .setMaxIterations(20)
      .run(train)

    println("train finished.")

  }
}


