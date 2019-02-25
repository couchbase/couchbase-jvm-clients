package com.couchbase.client.scala.kv

import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{GetAndTouchRequest, GetAndTouchResponse, InsertRequest}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.Validators
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.util.Validate
import io.opentracing.Span
import com.couchbase.client.scala.durability.Durability._

import scala.util.{Success, Try}


/**
  * Handles requests and responses for KV get-and-touch operations.
  *
  * @author Graham Pople
  */
class GetAndTouchHandler(hp: HandlerParams) extends RequestHandler[GetAndTouchResponse, GetResult] {

  def request[T](id: String,
                 expiration: java.time.Duration,
                 durability: Durability = Disabled,
                 parentSpan: Option[Span] = None,
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
  : Try[GetAndTouchRequest] = {
    val validations: Try[GetAndTouchRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
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

      Success(new GetAndTouchRequest(id,
        hp.collectionIdEncoded,
        timeout,
        hp.core.context(),
        hp.bucketName,
        retryStrategy,
        expiration,
        durability.toDurabilityLevel))
    }
  }

  def response(id: String, response: GetAndTouchResponse): GetResult = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        new GetResult(id, Left(response.content), response.flags(), response.cas, Option.empty)

      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }
}