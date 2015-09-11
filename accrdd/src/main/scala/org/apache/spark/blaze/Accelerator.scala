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
package org.apache.spark.blaze

/**
  * An accelerator interface. Accelerator interface allows the developer to
  * extend and assign the hardware accelerator to accelerate the function. 
  * The value `id` is used to assign the accelerator which should be provided 
  * by accelerator manager. In addition, in case of unavailable accelerators, 
  * the developer also has to create a software implementation, `call` method, 
  * with the same functionality as the accelerator.
  *
  * @tparam T Type of accelerator input. Only allow primitive types and 1-D array.
  * @tparam U Type of accelerator output. Only allow primitive types and 1-D array.
  */
trait Accelerator[T, U] extends java.io.Serializable {

  /** The corresponding accelerator ID of the hardware accelerator.
    * The developer should consult the manager for the correct ID to use it.
  **/
  val id: String

  /** The developer should specify the reference variables as arguments of extended 
    * Accelerator class. This method is used for mapping from the class arguments to 
    * the order in the hardware accelerator. For example:
    *
    * {{{
    * class MyAccelerator(ref1: BlazeBroadcast[Array[Int]], ref2: BlazeBroadcast[Int]) 
    *   extends Accelerator[Array[Int], Int] {
    *   def getArg(idx: Int): Option[BlazeBroadcast[_]] = {
    *     if (idx == 0) Some(ref1)
    *     else if (idx == 1) Some(ref2)
    *     else None
    *   }
    * }
    * }}}
    *
    * In this case, the protocol of hardware accelerator kernel would be:
    *
    * {{{
    * void acc(int *input, int *ref1, int ref2)
    * }}}
    *
    * @param idx The index of each variable.
    * @return The corresponding variable within Some. Return None if 
    *         the index is out of range.
  **/
  def getArg(idx: Int): Option[_]

  /** Report the total number of reference (broadcast) variables.
    * @return The total number of reference variables.
  **/
  def getArgNum(): Int

  /** The content of the function in Scala which has the same functionality as 
    * the hardware accelerator. This method is used when accelerator is not available.
    *
    * @param in An element of the input.
    * @return A corresponding element of the output.
  **/
  def call(in: T): U = in.asInstanceOf[U]

  def call(in: Iterator[T]): Iterator[U] = in.asInstanceOf[Iterator[U]]
}
