/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.optimization

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.{DenseVector => BDV}

import org.apache.spark.annotation.{Experimental, DeveloperApi}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.BLAS.axpy
import org.apache.spark.mllib.linalg.{Vectors, Vector}

import org.apache.spark.blaze._

/**
 * Class used to solve an optimization problem using Gradient Descent.
 * @param gradient Gradient function to be used.
 * @param updater Updater to be used to update weights after every iteration.
 */
class GradientDescent private[mllib] (private var gradient: Gradient, private var updater: Updater)
  extends Optimizer with Logging {

  private var stepSize: Double = 1.0
  private var numIterations: Int = 100
  private var regParam: Double = 0.0
  private var miniBatchFraction: Double = 1.0

  /**
   * Set the initial step size of SGD for the first step. Default 1.0.
   * In subsequent steps, the step size will decrease with stepSize/sqrt(t)
   */
  def setStepSize(step: Double): this.type = {
    this.stepSize = step
    this
  }

  /**
   * :: Experimental ::
   * Set fraction of data to be used for each SGD iteration.
   * Default 1.0 (corresponding to deterministic/classical gradient descent)
   */
  @Experimental
  def setMiniBatchFraction(fraction: Double): this.type = {
    this.miniBatchFraction = fraction
    this
  }

  /**
   * Set the number of iterations for SGD. Default 100.
   */
  def setNumIterations(iters: Int): this.type = {
    this.numIterations = iters
    this
  }

  /**
   * Set the regularization parameter. Default 0.0.
   */
  def setRegParam(regParam: Double): this.type = {
    this.regParam = regParam
    this
  }

  /**
   * Set the gradient function (of the loss function of one single data example)
   * to be used for SGD.
   */
  def setGradient(gradient: Gradient): this.type = {
    this.gradient = gradient
    this
  }


  /**
   * Set the updater function to actually perform a gradient step in a given direction.
   * The updater is responsible to perform the update from the regularization term as well,
   * and therefore determines what kind or regularization is used, if any.
   */
  def setUpdater(updater: Updater): this.type = {
    this.updater = updater
    this
  }

  /**
   * :: DeveloperApi ::
   * Runs gradient descent on the given training data.
   * @param data training data
   * @param initialWeights initial weights
   * @return solution vector
   */
  @DeveloperApi
  def optimize(data: RDD[(Double, Vector)], initialWeights: Vector): Vector = {
    val (weights, _) = GradientDescent.runMiniBatchSGD(
      data,
      gradient,
      updater,
      stepSize,
      numIterations,
      regParam,
      miniBatchFraction,
      initialWeights)
    weights
  }

}

/**
 * :: DeveloperApi ::
 * Top-level method to run gradient descent.
 */
@DeveloperApi
object GradientDescent extends Logging {
  /**
   * Run stochastic gradient descent (SGD) in parallel using mini batches.
   * In each iteration, we sample a subset (fraction miniBatchFraction) of the total data
   * in order to compute a gradient estimate.
   * Sampling, and averaging the subgradients over this subset is performed using one standard
   * spark map-reduce in each iteration.
   *
   * @param data - Input data for SGD. RDD of the set of data examples, each of
   *               the form (label, [feature values]).
   * @param gradient - Gradient object (used to compute the gradient of the loss function of
   *                   one single data example)
   * @param updater - Updater function to actually perform a gradient step in a given direction.
   * @param stepSize - initial step size for the first step
   * @param numIterations - number of iterations that SGD should be run.
   * @param regParam - regularization parameter
   * @param miniBatchFraction - fraction of the input data set that should be used for
   *                            one iteration of SGD. Default value 1.0.
   *
   * @return A tuple containing two elements. The first element is a column matrix containing
   *         weights for every feature, and the second element is an array containing the
   *         stochastic loss computed for every iteration.
   */
  def runMiniBatchSGD(
      data: RDD[(Double, Vector)],
      gradient: Gradient,
      updater: Updater,
      stepSize: Double,
      numIterations: Int,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Vector): (Vector, Array[Double]) = {

    val stochasticLossHistory = new ArrayBuffer[Double](numIterations)

    val numExamples = data.count()

    // if no data, return initial weights to avoid NaNs
    if (numExamples == 0) {
      logWarning("GradientDescent.runMiniBatchSGD returning initial weights, no data found")
      return (initialWeights, stochasticLossHistory.toArray)
    }

    if (numExamples * miniBatchFraction < 1) {
      logWarning("The miniBatchFraction is too small")
    }

    // Initialize weights as a column vector
    var weights = Vectors.dense(initialWeights.toArray)
    val n = weights.size

    /**
     * For the first iteration, the regVal will be initialized as sum of weight squares
     * if it's L2 updater; for L1 updater, the same logic is followed.
     */
    var regVal = updater.compute(
      weights, Vectors.dense(new Array[Double](weights.size)), 0, 1, regParam)._2


    /**
     * Setup Blaze runtime to allow accelerator of CostFun
     */
    val blaze = new BlazeRuntime(data.context)

    var blaze_data = blaze.wrap(data.map( x => x match {
          case (label, features) => label +: features.toArray
          }))

    for (i <- 1 to numIterations) {
      val bcWeights = data.context.broadcast(weights.toArray)
      // setup for Blaze execution
      var blaze_weight = blaze.wrap(bcWeights);

      // Sample a subset (fraction miniBatchFraction) of the total data
      // compute and sum up the subgradients on this subset (this is one map-reduce)
      var (gradientSum, lossSum, miniBatchSize) = blaze_data.sample_acc(false, miniBatchFraction, 42 + i
        ).mapPartitions_acc(
          new GradientDescentWithACC(weights.size, gradient, blaze_weight)
        ).map( 
          a => (Vectors.dense(a.slice(0, a.length-2)), a(a.length-2), a(a.length-1).toLong)
        ).reduce( (a, b) => (a, b) match { 
          case ((grad1, loss1, count1), (grad2, loss2, count2)) => 
            axpy(1.0, grad2, grad1)
            (grad1, loss1 + loss2, count1 + count2)
        })

      if (miniBatchSize > 0) {
        /**
         * NOTE(Xinghao): lossSum is computed using the weights from the previous iteration
         * and regVal is the regularization value computed in the previous iteration as well.
         */
        stochasticLossHistory.append(lossSum / miniBatchSize + regVal)
        val update = updater.compute(
          weights, Vectors.dense(gradientSum.toArray.map(e => e / miniBatchSize.toDouble)), 
          stepSize, i, regParam)
        weights = update._1
        regVal = update._2
      } else {
        logWarning(s"Iteration ($i/$numIterations). The size of sampled batch is zero")
      }
    }

    logInfo("GradientDescent.runMiniBatchSGD finished. Last 10 stochastic losses %s".format(
      stochasticLossHistory.takeRight(10).mkString(", ")))

    blaze.stop()

    (weights, stochasticLossHistory.toArray)

  }
  /**
  * GradientDescentWithACC implements Blaze Accelerator[T, T],
  * it calculates the gradient and loss from input data using accelerator 
  */
  private class GradientDescentWithACC(
    n: Int,
    localGradient: Gradient,
    weights: BlazeBroadcast[Array[Double]]
  ) extends Accelerator[Array[Double], Array[Double]] {
    
    val id = localGradient match {
      case grad : LogisticGradient => "LogisticGradient"
      case grad : LeastSquaresGradient => "LeastSquaresGradient"
      case grad : HingeGradient => "HingeGradient"
      case _ => "GenericGradientACC"
    }

    def getArg(idx: Int): Option[BlazeBroadcast[Array[Double]]] = {
      if (idx == 0) {
        Some(weights)
      }
      else {
        None
      }
    }
    def getArgNum(): Int = 1

    // function for AccRDD.mapParititions_acc()
    override def call(iter: Iterator[Array[Double]]): Iterator[Array[Double]] = {
      var result_iter = new Iterator[Array[Double]] {
        var idx = 0
        var grad = Vectors.zeros(n)
        var loss = 0.0
        var count = 0
        while (iter.hasNext) {
          val array = iter.next()
          var label = array(0)
          var features = Vectors.dense(array.slice(1, array.length))
          val l = localGradient.compute(
            features, label, Vectors.dense(weights.data), grad)
          loss = loss + l
          count += 1
        }
        def hasNext = (idx < 1)
        def next = {
          idx = idx + 1
          grad.toArray :+ loss :+ count.toDouble
        }
      }
      result_iter
    }
  }
}
