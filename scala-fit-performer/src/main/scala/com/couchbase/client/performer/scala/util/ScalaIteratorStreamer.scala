package com.couchbase.client.performer.scala.util

import com.couchbase.client.performer.core.perf.PerRun
import com.couchbase.client.performer.core.stream.IteratorBasedStreamer
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.streams.Config

import java.util.function.Function

class ScalaIteratorStreamer[T](iterator: Iterator[T],
                               perRun: PerRun,
                               streamId: String,
                               streamConfig: Config,
                               convertResult: Function[T, Result],
                               convertException: Function[Throwable, com.couchbase.client.protocol.shared.Exception])
  extends IteratorBasedStreamer[T](perRun, streamId, streamConfig, convertResult, convertException) {
  override protected def next = iterator.next

  override protected def hasNext = iterator.hasNext
}
