package com.couchbase.client.scala.kv

import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{GetAndLockRequest, GetAndLockResponse, InsertRequest}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.Validators
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.document.GetResult
import com.couchbase.client.scala.durability.{Durability}
import com.couchbase.client.scala.util.Validate
import io.opentracing.Span

import scala.util.{Success, Try}


/**
  * Handles requests and responses for KV get-and-lock operations.
  *
  * @author Graham Pople
  */
class GetAndLockHandler(hp: HandlerParams) extends RequestHandler[GetAndLockResponse, GetResult] {

  def request[T](id: String,
                 expiration: java.time.Duration,
                 parentSpan: Option[Span] = None,
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
  : Try[GetAndLockRequest] = {
    val validations: Try[GetAndLockRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(expiration, "expiration")
      _ <- Validate.notNull(parentSpan, "parentSpan")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    }
    else {
      Success(new GetAndLockRequest(id,
        hp.collectionIdEncoded,
        timeout,
        hp.core.context(),
        hp.bucketName,
        retryStrategy,
        expiration))
    }
  }

  def response(id: String, response: GetAndLockResponse): GetResult = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        new GetResult(id, Left(response.content), response.flags(), response.cas, Option.empty)

      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }
}