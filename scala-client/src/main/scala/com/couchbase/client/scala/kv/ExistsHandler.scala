package com.couchbase.client.scala.kv

import com.couchbase.client.core.{Core, CoreContext}
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{ObserveViaCasRequest, ObserveViaCasResponse}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.api.ExistsResult
import io.opentracing.Span

import scala.compat.java8.FutureConverters
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Success, Try}



// TODO how to handle flush - this will create a new collectionId
class ExistsHandler(hp: HandlerParams) extends RequestHandler[ObserveViaCasResponse, ExistsResult] {
  def request(id: String,
              parentSpan: Option[Span],
              timeout: java.time.Duration,
              retryStrategy: RetryStrategy
             ): Try[ObserveViaCasRequest] = {
    Success(new ObserveViaCasRequest(timeout,
      hp.core.context(),
      hp.bucketName,
      retryStrategy,
      id,
      hp.collectionIdEncoded))
  }

  override def response(id: String, response: ObserveViaCasResponse): ExistsResult = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        val exists: Boolean = response.observeStatus() match {
          case ObserveViaCasResponse.ObserveStatus.FOUND_PERSISTED | ObserveViaCasResponse.ObserveStatus.FOUND_NOT_PERSISTED => true
          case _ => false
        }

        ExistsResult(exists)

      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }
}
