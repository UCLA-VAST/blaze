package org.apache.spark.blaze

import java.io._

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

/**
  *
  */
class BlockInfo {
  var id: Long = 0
  var numElt: Int = -1
  var eltLength: Int = -1
  var eltSize: Int = -1
  var fileName: String = null
  var fileSize: Int = 0
  var offset: Int = 0
}
