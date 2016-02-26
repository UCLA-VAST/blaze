import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils

object LogisticRegression {

  def main(args : Array[String]) {

    var conf = new SparkConf().setAppName("LogisticRegression")
    val sc = new SparkContext(conf)

    if (args.length < 2) {
      System.err.println("Usage: LogisticRegression <train_file> <part_num>")
      System.exit(1)
    }

    val reps = args(1).toInt

    var data = MLUtils.loadLibSVMFile(sc, args(0))
    val splits = data.randomSplit(Array(0.7, 0.3), seed = 42L)
    val train = splits(0).repartition(reps).cache()
    //val train = splits(0).cache()
    val test = splits(1)

    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(train)

    println("training finished.")

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
          val prediction = model.predict(features)
          (prediction, label)
        }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision

    println("Precision = " + precision) 
  }
}


