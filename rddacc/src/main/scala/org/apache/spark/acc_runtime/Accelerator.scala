package org.apache.spark.acc_runtime

trait Accelerator[T, U] extends java.io.Serializable {
  val id: String
  def call(in: T): U
}
