package com.couchbase.client.scala.kv

import com.couchbase.client.core.error.{DocumentAlreadyExistsException, EncodingFailedException}
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv._
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.Validators
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.api.MutationResult
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.util.Validate
import io.opentracing.Span

import scala.compat.java8.OptionConverters._
import scala.util.{Failure, Success, Try}


/**
  * Handles requests and responses for KV replace operations.
  *
  * @author Graham Pople
  */
class ReplaceHandler(hp: HandlerParams) extends RequestHandler[ReplaceResponse, MutationResult] {

  def request[T](id: String,
                 content: T,
                 cas: Long,
                 durability: Durability,
                 expiration: java.time.Duration,
                 parentSpan: Option[Span],
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
                (implicit ev: Conversions.Encodable[T])
  : Try[ReplaceRequest] = {
    val validations: Try[ReplaceRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(content, "content")
      _ <- Validate.notNull(cas, "cas")
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
          Success(new ReplaceRequest(id,
            hp.collectionIdEncoded,
            encoded._1,
            expiration.getSeconds,
            encoded._2.flags,
            timeout,
            cas,
            hp.core.context(),
            hp.bucketName,
            retryStrategy,
            durability.toDurabilityLevel))

        case Failure(err) =>
          Failure(new EncodingFailedException(err))
      }
    }
  }

  def response(id: String, response: ReplaceResponse): MutationResult = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        MutationResult(response.cas(), response.mutationToken().asScala)

      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }
}
