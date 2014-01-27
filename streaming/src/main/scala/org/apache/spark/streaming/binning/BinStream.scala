package org.apache.spark.streaming.binning

import org.apache.spark.streaming.DStream

class BinStream[T: ClassManifest](@transient ds: DStream[T], sizeInNumBatches: Int, delayInNumBatches: Int) {

  def map[U: ClassManifest]( func: DStream[T] => DStream[U]) = {
    new BinStream(func(ds), sizeInNumBatches, delayInNumBatches)
  }

  private [streaming] def getDStream = ds
}
