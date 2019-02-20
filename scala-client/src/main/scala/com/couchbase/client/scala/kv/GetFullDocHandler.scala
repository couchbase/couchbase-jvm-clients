package com.couchbase.client.scala.kv

import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{GetRequest, GetResponse, InsertRequest}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.Validators
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.api.MutationResult
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.document.GetResult
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.util.Validate
import io.opentracing.Span

import scala.compat.java8.OptionConverters._
import scala.util.{Success, Try}


/**
  * Handles requests and responses for KV regular get operations.
  *
  * @author Graham Pople
  */
class GetFullDocHandler(hp: HandlerParams) extends RequestHandler[GetResponse, GetResult] {

  def request[T](id: String,
                 parentSpan: Option[Span],
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
  : Try[GetRequest] = {
    val validations: Try[GetRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(parentSpan, "parentSpan")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    }
    else {
      Success(new GetRequest(id,
        hp.collectionIdEncoded,
        timeout,
        hp.core.context(),
        hp.bucketName,
        retryStrategy))
    }
  }

  def response(id: String, response: GetResponse): GetResult = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        new GetResult(id, response.content, response.flags(), response.cas, Option.empty)

      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }
}