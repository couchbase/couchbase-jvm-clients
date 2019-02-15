package com.couchbase.client.scala.kv

import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{DecrementRequest, DecrementResponse, IncrementRequest, IncrementResponse}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.api.CounterResult
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.util.Validate
import io.opentracing.Span

import scala.compat.java8.OptionConverters._
import scala.util.{Success, Try}

class BinaryDecrementHandler(hp: HandlerParams) extends RequestHandler[DecrementResponse, CounterResult] {

  def request[T](id: String,
                 delta: Long,
                 initial: Option[Long] = None,
                 cas: Long = 0,
                 durability: Durability,
                 expiration: java.time.Duration,
                 parentSpan: Option[Span],
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
  : Try[DecrementRequest] = {

    val validations: Try[DecrementRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(delta, "delta")
      _ <- Validate.optNotNull(initial, "initial")
      _ <- Validate.notNull(cas, "cas")
      _ <- Validate.notNull(durability, "durability")
      _ <- Validate.notNull(parentSpan, "parentSpan")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    }
    else {
      Success(new DecrementRequest(
        timeout,
        hp.core.context(),
        hp.bucketName,
        retryStrategy,
        id,
        cas,
        hp.collectionIdEncoded,
        delta,
        initial.asJava.map(_.asInstanceOf[java.lang.Long]),
        expiration.getSeconds.toInt,
        durability.toDurabilityLevel
      ))
    }
  }

  def response(id: String, response: DecrementResponse): CounterResult = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        CounterResult(response.cas(), response.mutationToken().asScala, response.value())

      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }
}
