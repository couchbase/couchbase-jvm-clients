package com.couchbase.client.performer.scala.util

import com.couchbase.client.performer.core.perf.PerRun
import com.couchbase.client.performer.core.stream.Streamer
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.streams.{Config, RequestItemsRequest}
import org.reactivestreams.Subscription
import org.slf4j.{Logger, LoggerFactory}
import reactor.core.scala.publisher.SFlux

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.function.Function

class ScalaFluxStreamer[T](
    results: SFlux[T],
    perRun: PerRun,
    streamId: String,
    streamConfig: Config,
    convertResult: Function[T, Result],
    convertException: Function[Throwable, com.couchbase.client.protocol.shared.Exception]
) extends Streamer[T](perRun, streamId, streamConfig, convertResult, convertException) {
  protected val logger: Logger = LoggerFactory.getLogger(classOf[ScalaFluxStreamer[T]])
  private val subscriberRef    = new AtomicReference[Subscription]

  override def cancel(): Unit = subscriberRef.get().cancel()

  override def requestItems(request: RequestItemsRequest): Unit = {
    subscriberRef.get().request(request.getNumItems())
  }

  override def isCreated: Boolean = ???

  override def run(): Unit = {
    val done = new AtomicBoolean(false)

    val subscriber = new org.reactivestreams.Subscriber[T] {
      override def onSubscribe(subscription: Subscription): Unit = {
        subscriberRef.set(subscription)

        if (streamConfig.hasAutomatically) {
          subscription.request(Long.MaxValue)
        } else if (!streamConfig.hasOnDemand) {
          throw new UnsupportedOperationException("Unknown stream config")
        }
      }

      override def onNext(value: T): Unit = {
        logger.info("Flux streamer {} sending one", streamId)
        perRun.resultsStream.enqueue(convertResult.apply(value))
        streamed.incrementAndGet
      }

      override def onComplete(): Unit = {
        perRun.resultsStream.enqueue(
          Result.newBuilder
            .setStream(
              com.couchbase.client.protocol.streams.Signal.newBuilder
                .setComplete(
                  com.couchbase.client.protocol.streams.Complete.newBuilder
                    .setStreamId(streamId)
                )
            )
            .build
        )

        logger.info(
          "Flux streamer {} has finished streaming back {} results",
          streamId,
          streamed.get
        )

        done.set(true)
      }

      override def onError(throwable: Throwable): Unit = {
        // logger.info("Flux streamer {} errored with {}", streamId, throwable.toString)

        perRun.resultsStream.enqueue(
          Result.newBuilder
            .setStream(
              com.couchbase.client.protocol.streams.Signal.newBuilder
                .setError(
                  com.couchbase.client.protocol.streams.Error.newBuilder
                    .setException(convertException.apply(throwable))
                    .setStreamId(streamId)
                )
            )
            .build
        )

        done.set(true)
      }

    }

    results.subscribe(subscriber)

    logger.info("Waiting for flux streamer {}", streamId)

    while (!done.get) {
      try {
        Thread.sleep(10)
      } catch {
        case e: InterruptedException => throw new RuntimeException(e)
      }
    }

    logger.info("Flux streamer {} has ended", streamId)

  }
}
