package com.couchbase.client.scala.kv

import com.couchbase.client.core.error.EncodingFailedException
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{UpsertRequest, UpsertResponse}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.Validators
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.api.MutationResult
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.durability.Durability
import io.opentracing.Span

import scala.compat.java8.OptionConverters._
import scala.util.{Failure, Success, Try}

class UpsertHandler(hp: HandlerParams) extends RequestHandler[UpsertResponse, MutationResult] {

  def request[T](id: String,
                 content: T,
                 durability: Durability,
                 expiration: java.time.Duration,
                 parentSpan: Option[Span],
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
                (implicit ev: Conversions.Encodable[T])
  : Try[UpsertRequest] = {
    Validators.notNullOrEmpty(id, "id")
    Validators.notNull(content, "content")
    Validators.notNull(content, "timeout")

    ev.encode(content) match {
      case Success(encoded) =>
        Success(new UpsertRequest(id,
          hp.collectionIdEncoded,
          encoded._1,
          expiration.getSeconds,
          encoded._2.flags,
          timeout,
          hp.core.context(),
          hp.bucketName,
          retryStrategy,
          durability.toDurabilityLevel))

      case Failure(err) =>
        Failure(new EncodingFailedException(err))
    }
  }

  def response(id: String, response: UpsertResponse): MutationResult = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        MutationResult(response.cas(), response.mutationToken().asScala)

      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }
}
