package com.couchbase.client.performer.scala.util

import com.couchbase.client.performer.core.perf.PerRun
import com.couchbase.client.performer.core.stream.IteratorBasedStreamer
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.streams.Config

import java.util.function.Function

class ScalaIteratorStreamer[T](iterator: Iterator[T], perRun: PerRun, streamId: String, streamConfig: Config, convert: Function[T, Result])
  extends IteratorBasedStreamer[T](perRun, streamId, streamConfig, convert) {
  override protected def next = iterator.next

  override protected def hasNext = iterator.hasNext
}
