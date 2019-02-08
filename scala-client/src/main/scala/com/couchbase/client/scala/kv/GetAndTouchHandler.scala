package com.couchbase.client.scala.kv

import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{GetAndTouchRequest, GetAndTouchResponse}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.Validators
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.document.GetResult
import com.couchbase.client.scala.durability.{Disabled, Durability}
import io.opentracing.Span

import scala.concurrent.duration.FiniteDuration
import scala.util.{Success, Try}


class GetAndTouchHandler(hp: HandlerParams) extends RequestHandler[GetAndTouchResponse, GetResult] {

  def request[T](id: String,
                 expiration: java.time.Duration,
                 durability: Durability = Disabled,
                 parentSpan: Option[Span] = None,
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
  : Try[GetAndTouchRequest] = {
    Validators.notNullOrEmpty(id, "id")

    Success(new GetAndTouchRequest(id,
      hp.collectionIdEncoded,
      timeout,
      hp.core.context(),
      hp.bucketName,
      retryStrategy,
      expiration,
      durability.toDurabilityLevel))
  }

  def response(id: String, response: GetAndTouchResponse): GetResult = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        new GetResult(id, response.content, response.flags(), response.cas, Option.empty)

      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }
}