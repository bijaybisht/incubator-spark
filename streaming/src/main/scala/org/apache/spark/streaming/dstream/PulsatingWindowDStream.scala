package org.apache.spark.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.{UnionRDD, RDD}

private[streaming]
class PulsatingWindowDStream[T: ClassManifest](parent: DStream[T],
                                               sizeNumBatches: Int,
                                               delayNumBins: Int)
  extends DStream[T](parent.ssc) {

  parent.persist(StorageLevel.MEMORY_ONLY_SER)

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def parentRememberDuration: Duration = rememberDuration + parent.slideDuration * sizeNumBatches * (delayNumBins + 1)

  override def compute(validTime: Time): Option[RDD[T]] = {

    val binStart = (validTime - Duration(1)).floor(slideDuration * sizeNumBatches) - slideDuration * sizeNumBatches * delayNumBins

    val currentWindow = new Interval(binStart + slideDuration, validTime)

    Some(new UnionRDD(parent.ssc.sc, parent.slice(currentWindow)))
  }
}



