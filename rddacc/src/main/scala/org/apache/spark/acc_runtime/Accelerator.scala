package org.apache.spark.acc_runtime

trait Accelerator[T, U] extends java.io.Serializable {

  /** This is the corresponding accelerator ID of hardware accelerator library.
    * You should consult the library for the correct ID to use it in the runtime 
    * if available.
  **/
  val id: String

  /** The developer should specify the reference variables at the constructor of 
    * extended Accelerator class. This method is used for mapping from the constructor 
    * arguments to an index. For example, we might create an Accelerator class:
    *
    * {{{
    * class Circumference(pi: Broadcast_ACC[Double]) extends Accelerator
    *   def getArg(idx: Int): Option[Broadcast_ACC[Double]] = {
    *     if (idx == 0) Some(pi)
    *     else None
    *   }
    * }}}
    *
    * @param idx The index of each variable.
    * @return The corresponding variable within Some. Return None if 
    *         the index is out of range.
  **/
  def getArg(idx: Int): Option[Broadcast_ACC[_]]

  /** Report the total number of reference (broadcast) variables.
    * @return The total number of reference variables.
  **/
  def getArgNum(): Int

  /** The content of map function in Scala.
    * This method is used when accelerator is not available.
    *
    * @param in An element of the input.
    * @return A corresponding element of the output.
  **/
  def call(in: T): U
}
