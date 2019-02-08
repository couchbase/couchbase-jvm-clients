package com.couchbase.client.scala.kv

import com.couchbase.client.core.error.EncodingFailedException
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{RemoveRequest, RemoveResponse, ReplaceRequest, ReplaceResponse}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.Validators
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.api.MutationResult
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.durability.Durability
import io.opentracing.Span

import scala.compat.java8.OptionConverters._
import scala.util.{Failure, Success, Try}


class RemoveHandler(hp: HandlerParams) extends RequestHandler[RemoveResponse, MutationResult] {

  def request[T](id: String,
                 cas: Long,
                 durability: Durability,
                 parentSpan: Option[Span],
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
  : Try[RemoveRequest] = {
    Validators.notNullOrEmpty(id, "id")

    Success(new RemoveRequest(id,
      hp.collectionIdEncoded,
      cas,
      timeout,
      hp.core.context(),
      hp.bucketName,
      retryStrategy,
      durability.toDurabilityLevel))
  }

  def response(id: String, response: RemoveResponse): MutationResult = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        MutationResult(response.cas(), response.mutationToken().asScala)

      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }
}