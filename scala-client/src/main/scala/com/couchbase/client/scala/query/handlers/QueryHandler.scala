/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.scala.query.handlers

import java.nio.charset.StandardCharsets
import java.util.Collections

import com.couchbase.client.core.config.{ClusterCapabilities, ClusterConfig}
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.core.error.{CouchbaseException, ErrorCodeAndMessage}
import com.couchbase.client.core.msg.query.{QueryChunkRow, QueryRequest, QueryResponse}
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.core.util.Golang.encodeDurationToMs
import com.couchbase.client.core.util.LRUCache
import com.couchbase.client.scala.HandlerBasicParams
import com.couchbase.client.scala.codec.JsonDeserializer
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.json.{JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.query._
import com.couchbase.client.scala.transformers.JacksonTransformers
import com.couchbase.client.scala.util.{DurationConversions, FutureConversions, Validate}
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
  * Holds a cache entry, which might either be the full plan or just the name, depending on the
  * cluster state.
  */
private[scala] case class QueryCacheEntry(name: String, fullPlan: Boolean, value: Option[String]) {
  def export = {
    val result = JsonObject.create
    result.put("prepared", name)
    if (fullPlan) value.foreach(plan => result.put("encoded_plan", plan))
    result
  }

}

/**
  * Handles requests and responses for N1QL query operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class QueryHandler(hp: HandlerBasicParams)(implicit ec: ExecutionContext) {
  private val core = hp.core
  import DurationConversions._

  private val QueryCacheSize = 5000
  private val queryCache =
    Collections.synchronizedMap(new LRUCache[String, QueryCacheEntry](QueryCacheSize))
  @volatile private var enhancedPreparedEnabled = false

  updateEnhancedPreparedEnabled(core.clusterConfig())

  // Subscribe to cluster config changes
  core
    .configurationProvider()
    .configs()
    .subscribe(config => updateEnhancedPreparedEnabled(config))

  private def updateEnhancedPreparedEnabled(config: ClusterConfig): Unit = {
    if (!enhancedPreparedEnabled) {
      val caps = config.clusterCapabilities.get(ServiceType.QUERY)
      enhancedPreparedEnabled = caps != null && caps.contains(
        ClusterCapabilities.ENHANCED_PREPARED_STATEMENTS
      )
    }
  }

  private def request[T](
      statement: String,
      options: QueryOptions,
      environment: ClusterEnvironment
  ): Try[QueryRequest] = {

    val validations = for {
      _ <- Validate.notNullOrEmpty(statement, "statement")
      _ <- Validate.notNull(options, "options")
      _ <- Validate.optNotNull(options.parameters, "parameters")
      _ <- Validate.optNotNull(options.clientContextId, "clientContextId")
      _ <- Validate.optNotNull(options.maxParallelism, "maxParallelism")
      _ <- Validate.notNull(options.metrics, "metrics")
      _ <- Validate.optNotNull(options.pipelineBatch, "pipelineBatch")
      _ <- Validate.optNotNull(options.pipelineCap, "pipelineCap")
      _ <- Validate.optNotNull(options.profile, "profile")
      _ <- Validate.optNotNull(options.readonly, "readonly")
      _ <- Validate.optNotNull(options.retryStrategy, "retryStrategy")
      _ <- Validate.optNotNull(options.scanCap, "scanCap")
      _ <- Validate.optNotNull(options.scanConsistency, "scanConsistency")
      _ <- Validate.optNotNull(options.timeout, "timeout")
      _ <- Validate.optNotNull(options.parentSpan, "parentSpan")
    } yield null

    if (validations.isFailure) {
      validations
    } else {
      options.deferredException match {
        case Some(deferredException) => Failure(deferredException)
        case _ =>
          val params = options.encode()
          params.put("statement", statement)

          Try(JacksonTransformers.MAPPER.writeValueAsString(params)).map(queryStr => {
            val queryBytes = queryStr.getBytes(CharsetUtil.UTF_8)

            val timeout: Duration =
              options.timeout.getOrElse(environment.timeoutConfig.queryTimeout())
            val retryStrategy = options.retryStrategy.getOrElse(environment.retryStrategy)

            val request = new QueryRequest(
              timeout,
              core.context(),
              retryStrategy,
              core.context().authenticator(),
              statement,
              queryBytes,
              options.readonly.getOrElse(false),
              params.str("client_context_id"),
              hp.tracer.internalSpan(QueryRequest.OPERATION_NAME, options.parentSpan.orNull)
            )

            request
          })
      }
    }
  }

  /** Convert a core QueryResponse into a client ReactiveQueryResult.  This does no buffering.
    *
    * @param response the response to convert
    *
    * @return a ReactiveQueryResult
    */
  private def convertResponse(response: QueryResponse): ReactiveQueryResult = {
    val rows: SFlux[QueryChunkRow] = FutureConversions.javaFluxToScalaFlux(response.rows())

    val meta: SMono[QueryMetaData] = FutureConversions
      .javaMonoToScalaMono(response.trailer())
      .map(addl => {
        val warnings: collection.Seq[QueryWarning] = addl.warnings.asScala
          .map(
            warnings =>
              ErrorCodeAndMessage
                .fromJsonArray(warnings)
                .asScala
                .map(warning => QueryWarning(warning.code(), warning.message()))
          )
          .getOrElse(Seq())

        val status: QueryStatus = addl.status match {
          case "running"   => QueryStatus.Running
          case "success"   => QueryStatus.Success
          case "errors"    => QueryStatus.Errors
          case "completed" => QueryStatus.Completed
          case "stopped"   => QueryStatus.Stopped
          case "timeout"   => QueryStatus.Timeout
          case "closed"    => QueryStatus.Closed
          case "fatal"     => QueryStatus.Fatal
          case "aborted"   => QueryStatus.Aborted
          case _           => QueryStatus.Unknown
        }

        val out = QueryMetaData(
          response.header().requestId(),
          response.header().clientContextId().asScala.getOrElse(""),
          response.header().signature().asScala,
          addl.metrics().asScala.flatMap(QueryMetrics.fromBytes),
          warnings,
          status,
          addl.profile.asScala
        )

        out
      })

    ReactiveQueryResult(
      rows,
      meta
    )
  }

  /**
    * Internal method to dispatch the request into the core and return it as a mono.
    *
    * @param request the request to perform.
    * @param options query options to use.
    * @param adhoc   if this query is adhoc.
    *
    * @return the mono once the result is complete.
    */
  private def queryInternal(request: QueryRequest, options: QueryOptions, adhoc: Boolean) = {
    if (adhoc) {
      SMono.defer(() => {
        core.send(request)
        FutureConversions.wrap(request, request.response, propagateCancellation = true)
      })
    } else maybePrepareAndExecute(request, options)
  }

  /**
    * Main method to drive the prepare and execute cycle.
    *
    * <p>Depending on if the statement is already cached, this method checks if a prepare needs to be executed,
    * and if so does it. In both cases, afterwards a subsequent execute is conducted with the primed cache and
    * the options that were present in the original query.</p>
    *
    * <p>The code also checks if the cache entry is still valid, to handle the upgrade scenario an potentially
    * flush the cache entry in this case to then execute with the newer approach.</p>
    *
    * @param request the request to perform.
    * @param options query options to use.
    *
    * @return the mono once the result is complete.
    */
  private def maybePrepareAndExecute(
      request: QueryRequest,
      options: QueryOptions
  ): SMono[QueryResponse] = {
    val cacheEntry      = queryCache.get(request.statement)
    val enhancedEnabled = enhancedPreparedEnabled

    if (cacheEntry != null && cacheEntryStillValid(cacheEntry, enhancedEnabled)) {
      queryInternal(buildExecuteRequest(cacheEntry, request, options), options, true)
    } else if (enhancedEnabled) {
      queryInternal(buildPrepareRequest(request, options), options, true)
        .flatMap((qr: QueryResponse) => {
          val preparedName = qr.header().prepared()
          if (!preparedName.isPresent) {
            SMono.raiseError(
              new CouchbaseException("No prepared name present but must be, this is a query bug!")
            )
          } else {
            queryCache.put(
              request.statement(),
              QueryCacheEntry(preparedName.get(), fullPlan = false, None)
            )
            SMono.just(qr)
          }
        })
    } else {
      SMono
        .defer(() => {
          val req = buildPrepareRequest(request, options)
          core.send(req)
          FutureConversions.wrap(req, req.response, propagateCancellation = true)
        })
        .flatMapMany(result => result.rows())
        // Only expect one row back, but no harm handling multiple
        .doOnNext(row => {
          val json: Try[JsonObjectSafe] =
            JsonDeserializer.JsonObjectSafeConvert.deserialize(row.data())
          val nameOpt: Option[String] = json.flatMap(_.str("name")).toOption
          val plan: Option[String] =
            if (enhancedEnabled) None else json.flatMap(_.str("encoded_plan")).toOption

          nameOpt match {
            case Some(name) =>
              val entry = QueryCacheEntry(name, !enhancedEnabled, plan)
              queryCache.put(request.statement, entry)
            case _ =>
          }
        })
        .`then`()
        .`then`(SMono.defer(() => maybePrepareAndExecute(request, options)))
        .onErrorResume(err => {
          // The logic here is that if the prepare-execute
          // phase fails, the user isn't interested.  So just perform the query as an adhoc one
          // so they hopefully get something back.
          queryInternal(request, options, true)
        })
    }
  }

  /**
    * Performs a N1QL query and returns the result as a Mono.
    *
    * @param request the request to perform.
    * @param options query options to use.
    *
    * @return the mono once the result is complete.
    */
  private def queryReactive(
      request: QueryRequest,
      options: QueryOptions
  ): SMono[ReactiveQueryResult] = {
    SMono.defer(() => {
      queryInternal(request, options, options.adhoc)
        .map(v => convertResponse(v))
    })
  }

  /** Builds the request to prepare a prepared statement.
    *
    * @param original the original request from which params are extracted.
    *
    * @return the created request, ready to be sent over the wire.
    */
  private def buildPrepareRequest(original: QueryRequest, options: QueryOptions) = {
    val statement = "PREPARE " + original.statement

    val query = JsonObject.create
    query.put("statement", statement)
    query.put("timeout", encodeDurationToMs(original.timeout))

    if (enhancedPreparedEnabled) {
      query.put("auto_execute", true)
      options.encode(query)
    }

    new QueryRequest(
      original.timeout,
      original.context,
      original.retryStrategy(),
      original.credentials,
      statement,
      query.toString.getBytes(StandardCharsets.UTF_8),
      true,
      query.str("client_context_id"),
      hp.tracer.internalSpan(QueryRequest.OPERATION_NAME, options.parentSpan.orNull)
    )
  }

  /**
    * Constructs the execute request from the primed cache and the original request options.
    *
    * @param cacheEntry      the primed cache entry.
    * @param original        the original request.
    * @param originalOptions the original request options.
    *
    * @return the created request, ready to be sent over the wire.
    */
  private def buildExecuteRequest(
      cacheEntry: QueryCacheEntry,
      original: QueryRequest,
      originalOptions: QueryOptions
  ): QueryRequest = {
    val query = cacheEntry.export

    query.put("timeout", encodeDurationToMs(original.timeout))

    originalOptions.encode(query)

    new QueryRequest(
      original.timeout,
      original.context,
      original.retryStrategy(),
      original.credentials,
      original.statement,
      query.toString.getBytes(StandardCharsets.UTF_8),
      originalOptions.readonly.getOrElse(false),
      query.str("client_context_id"),
      hp.tracer.internalSpan(QueryRequest.OPERATION_NAME, originalOptions.parentSpan.orNull)
    )
  }

  /**
    * If an upgrade has happened and we can now do enhanced prepared, the cache got invalid.
    *
    * @param entry           the entry to check.
    * @param enhancedEnabled if enhanced prepared statementd are enabled.
    *
    * @return true if still valid, false otherwise.
    */
  private def cacheEntryStillValid(entry: QueryCacheEntry, enhancedEnabled: Boolean) = {
    (enhancedEnabled && !entry.fullPlan) || (!enhancedEnabled && entry.fullPlan)
  }

  /* Performs a N1QL query, returning the result as a Mono
   *
   * Rows are not buffered in-memory, they are streamed back.
   */
  def queryReactive(
      statement: String,
      options: QueryOptions,
      environment: ClusterEnvironment
  ): SMono[ReactiveQueryResult] = {
    request(statement, options, environment) match {
      case Success(req) => queryReactive(req, options)
      case Failure(err) => SMono.raiseError(err)
    }
  }

  /* Performs a N1QL query, returning the result as a Future.
   *
   * Rows are buffered in-memory.
   */
  def queryAsync(
      statement: String,
      options: QueryOptions,
      environment: ClusterEnvironment
  ): Future[QueryResult] = {

    request(statement, options, environment) match {
      case Success(req) =>
        queryReactive(req, options)
        // Buffer the responses
          .flatMap(
            response =>
              response.rows
                .collectSeq()
                .flatMap(rows => {
                  response.metaData
                    .map(meta => {
                      QueryResult(rows, meta)
                    })
                })
          )
          .toFuture

      case Failure(err) => Future.failed(err)
    }
  }
}
