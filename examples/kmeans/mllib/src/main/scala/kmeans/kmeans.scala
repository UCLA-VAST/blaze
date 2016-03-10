import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
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
    //val splits = data.randomSplit(Array(0.8, 0.2), seed = 42L)
    val train = data.map(e => e.features).repartition(reps).cache()

    /*
    val test = sc.textFile(args(1)).map( line => {
        var parts = line.split(',') 
        LabeledPoint(parts(0).toDouble, 
            Vectors.dense(parts(1).split(' ').map(_.toDouble)))
        });
    */

    val startTime = System.nanoTime

    val model = new KMeans()
      .setInitializationMode(KMeans.RANDOM)
      .setK(k)
      .setMaxIterations(50)
      .run(train)

    val elapseTime = System.nanoTime - startTime
    println("Training finished in "+ (elapseTime.toDouble / 1e9) +"s.")

    /*
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features).toDouble
        (prediction, label)
      }
    
    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    println("Precision = " + precision)
    */
  }
}


