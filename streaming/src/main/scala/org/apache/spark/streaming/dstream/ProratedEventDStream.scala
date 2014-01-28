package org.apache.spark.streaming

import org.apache.spark.rdd.RDD


private[streaming]
class ProratedEventDStream[T: ClassManifest](parent: DStream[T],
                                             filterFunc: (Time,Time) => T => Boolean,
                                             prorateFunc: (Time,Time) => T => (T,Double),
                                             sizeNumBatches: Int,

                                             delayNumBins: Int

                                             )
  extends DStream[(T, Double)](parent.ssc) {

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time) = {

    //
    // Assumption: start(x) <= end(x) <= boundaryEnd
    //

    def binStart = (validTime - Duration(1)).floor(slideDuration * sizeNumBatches) - slideDuration * sizeNumBatches * delayNumBins
    def binEnd = binStart + slideDuration * sizeNumBatches

    parent.getOrCompute(validTime).map(

      _.filter(
        filterFunc(binStart, binEnd)
      ).map(
        prorateFunc(binStart, binEnd)
      )
    )
  }
}

