package com.couchbase.client.scala.kv

import com.couchbase.client.core.Core
import com.couchbase.client.core.error.{DocumentAlreadyExistsException, EncodingFailedException}
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{InsertRequest, InsertResponse, ObserveViaCasRequest, ObserveViaCasResponse}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.Validators
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.api.{ExistsResult, MutationResult}
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.durability.{Disabled, Durability}
import com.couchbase.client.scala.util.Validate
import io.opentracing.Span

import scala.compat.java8.OptionConverters._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

class InsertHandler(hp: HandlerParams) extends RequestHandler[InsertResponse, MutationResult] {

  def request[T](id: String,
                 content: T,
                 durability: Durability,
                 expiration: java.time.Duration,
                 parentSpan: Option[Span],
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
                (implicit ev: Conversions.Encodable[T])
  : Try[InsertRequest] = {

    val validations: Try[InsertRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(content, "content")
      _ <- Validate.notNull(durability, "durability")
      _ <- Validate.notNull(expiration, "expiration")
      _ <- Validate.notNull(parentSpan, "parentSpan")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    }
    else {
      ev.encode(content) match {
        case Success(encoded) =>
          Success(new InsertRequest(id,
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
  }

  def response(id: String, response: InsertResponse): MutationResult = {
    response.status() match {
      case ResponseStatus.EXISTS =>
        throw new DocumentAlreadyExistsException()

      case ResponseStatus.SUCCESS =>
        MutationResult(response.cas(), response.mutationToken().asScala)

      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }
}
