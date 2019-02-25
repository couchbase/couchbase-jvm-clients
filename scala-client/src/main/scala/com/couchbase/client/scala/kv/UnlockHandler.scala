package com.couchbase.client.scala.kv

import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{GetAndLockRequest, GetAndLockResponse, UnlockRequest, UnlockResponse}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.util.Validate
import io.opentracing.Span

import scala.util.{Success, Try}


/**
  * Handles requests and responses for KV unlock operations.
  *
  * @author Graham Pople
  */
class UnlockHandler(hp: HandlerParams) extends RequestHandler[UnlockResponse, Unit] {

  def request[T](id: String,
                 cas: Long,
                 parentSpan: Option[Span] = None,
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
  : Try[UnlockRequest] = {
    val validations: Try[UnlockRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(cas, "cas")
      _ <- Validate.notNull(parentSpan, "parentSpan")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    }
    else {
      Success(new UnlockRequest(
        timeout,
        hp.core.context(),
        hp.bucketName,
        retryStrategy,
        id,
        hp.collectionIdEncoded,
        cas))
    }
  }

  def response(id: String, response: UnlockResponse): Unit = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }
}