import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.blaze._

object LogisticRegression {

  def main(args : Array[String]) {

    var conf = new SparkConf().setAppName("LogisticRegression")
    val sc = new SparkContext(conf)

    if (args.length < 2) {
      System.err.println("Usage: LogisticRegression <train_file> <test_file> <part_num>")
      System.exit(1)
    }

    val reps = args(1).toInt

    val train = sc.textFile(args(0)).map( line => {
        var parts = line.split(',') 
        LabeledPoint(parts(0).toDouble, 
            Vectors.dense(parts(1).split(' ').map(_.toDouble)))
        }).repartition(reps).cache()

    /*
    val test = sc.textFile(args(1)).map( line => {
        var parts = line.split(',') 
        LabeledPoint(parts(0).toDouble, 
            Vectors.dense(parts(1).split(' ').map(_.toDouble)))
        });
    */

    val startTime = System.nanoTime

    // start training 
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(train)

    val elapseTime = System.nanoTime - startTime
    println("Training finished in "+ (elapseTime.toDouble / 1e9) +"s.")

    /*
    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
        (prediction, label)
      }
    
    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    println("Precision = " + precision)
    */
  }
}


