package com.couchbase.client.scala.query

import com.couchbase.client.core.Core
import com.couchbase.client.core.error.EncodingFailedException
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{UpsertRequest, UpsertResponse}
import com.couchbase.client.core.msg.query.QueryRequest
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.{DurationConversions, HandlerParams}
import com.couchbase.client.scala.api.{MutationResult, QueryOptions}
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.{DefaultErrors, RequestHandler}
import com.couchbase.client.scala.util.Validate
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.scala.transformers.JacksonTransformers
import io.opentracing.Span

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/**
  * Handles requests and responses for KV upsert operations.
  *
  * @author Graham Pople
  */
class QueryHandler() {
  import DurationConversions._

  def request[T](statement: String, options: QueryOptions, core: Core, environment: ClusterEnvironment)
  : Try[QueryRequest] = {
    // TODO prepared query

    val validations: Try[QueryRequest] = for {
      _ <- Validate.notNullOrEmpty(statement, "statement")
      _ <- Validate.notNull(options, "options")
      _ <- Validate.optNotNull(options.namedParameters, "namedParameters")
      _ <- Validate.optNotNull(options.positionalParameters, "positionalParameters")
      _ <- Validate.optNotNull(options.clientContextId, "clientContextId")
      _ <- Validate.optNotNull(options.credentials, "credentials")
      _ <- Validate.optNotNull(options.maxParallelism, "maxParallelism")
      _ <- Validate.optNotNull(options.disableMetrics, "disableMetrics")
      _ <- Validate.optNotNull(options.pipelineBatch, "pipelineBatch")
      _ <- Validate.optNotNull(options.pipelineCap, "pipelineCap")
      _ <- Validate.optNotNull(options.profile, "profile")
      _ <- Validate.optNotNull(options.readonly, "readonly")
      _ <- Validate.optNotNull(options.retryStrategy, "retryStrategy")
      _ <- Validate.optNotNull(options.scanCap, "scanCap")
      _ <- Validate.optNotNull(options.scanConsistency, "scanConsistency")
      _ <- Validate.optNotNull(options.serverSideTimeout, "serverSideTimeout")
      _ <- Validate.optNotNull(options.timeout, "timeout")
    } yield null

    if (validations.isFailure) {
      validations
    }
    else {
      val params = options.encode()
      params.put("statement", statement)

      Try(JacksonTransformers.MAPPER.writeValueAsString(params)).map(queryStr => {
        val queryBytes = queryStr.getBytes(CharsetUtil.UTF_8)

        val timeout: Duration = options.timeout.getOrElse(environment.timeoutConfig.queryTimeout())
        val retryStrategy = options.retryStrategy.getOrElse(environment.retryStrategy())

        val request: QueryRequest = new QueryRequest(timeout,
          core.context(),
          retryStrategy,
          environment.credentials(),
          queryBytes)

        request
      })
    }
  }
}
