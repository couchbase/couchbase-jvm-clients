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
import java.util
import java.util.{Collections, Map, Set}

import com.couchbase.client.core.{Core, Reactor}
import com.couchbase.client.core.config.{ClusterCapabilities, ClusterConfig}
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.core.error.{CouchbaseException, QueryException}
import com.couchbase.client.core.msg.query.{QueryChunkRow, QueryRequest, QueryResponse}
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.core.util.Golang.encodeDurationToMs
import com.couchbase.client.core.util.LRUCache
import com.couchbase.client.scala.codec.{Conversions, DocumentFlags}
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.json.{JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.query._
import com.couchbase.client.scala.transformers.JacksonTransformers
import com.couchbase.client.scala.util.{DurationConversions, FutureConversions, Validate}
import reactor.core.scala.publisher.{Flux, Mono}
import reactor.core.publisher.{Flux => JavaFlux, Mono => JavaMono}

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import scala.compat.java8.OptionConverters._


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
private[scala] class QueryHandler(core: Core) {

  import DurationConversions._

  private val QueryCacheSize = 5000
  private val queryCache = Collections.synchronizedMap(new LRUCache[String, QueryCacheEntry](QueryCacheSize))
  @volatile private var enhancedPreparedEnabled = false

  updateEnhancedPreparedEnabled(core.clusterConfig())

  // Subscribe to cluster config changes
  core.configurationProvider()
    .configs()
    .subscribe(config => updateEnhancedPreparedEnabled(config))

  private def updateEnhancedPreparedEnabled(config: ClusterConfig): Unit = {
    if (!enhancedPreparedEnabled) {
      val caps = config.clusterCapabilities.get(ServiceType.QUERY)
      enhancedPreparedEnabled = caps != null && caps.contains(ClusterCapabilities.ENHANCED_PREPARED_STATEMENTS)
    }
  }

  private def request[T](statement: String,
                         options: QueryOptions,
                         environment: ClusterEnvironment)
  : Try[QueryRequest] = {

    val validations = for {
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
        val retryStrategy = options.retryStrategy.getOrElse(environment.retryStrategy)

        val request = new QueryRequest(timeout,
          core.context(),
          retryStrategy,
          environment.credentials,
          statement,
          queryBytes)

        request
      })
    }
  }

  /** Convert a core QueryResponse into a client ReactiveQueryResult.  This does no buffering.
    *
    * @param response the response to convert
    *
    * @return a ReactiveQueryResult
    */
  private def convertResponse(response: QueryResponse): ReactiveQueryResult = {
    val rows: Flux[QueryChunkRow] = FutureConversions.javaFluxToScalaFlux(response.rows())

    val meta: Mono[QueryMeta] = FutureConversions.javaMonoToScalaMono(response.trailer())
      .map(addl => {
        QueryMeta(
          response.header().requestId(),
          response.header().clientContextId().asScala,
          response.header().signature().asScala.map(QuerySignature),
          addl.metrics().asScala.map(QueryMetrics.fromBytes),
          addl.warnings.asScala.map(QueryWarnings),
          addl.status,
          addl.profile.asScala.map(v => QueryProfile(v))
        )
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
      core.send(request)
      FutureConversions.javaMonoToScalaMono(Reactor.wrap(request, request.response, true))
    }
    else maybePrepareAndExecute(request, options)
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
  private def maybePrepareAndExecute(request: QueryRequest, options: QueryOptions): Mono[QueryResponse] = {
    val cacheEntry = queryCache.get(request.statement)
    val enhancedEnabled = enhancedPreparedEnabled

    if (cacheEntry != null && cacheEntryStillValid(cacheEntry, enhancedEnabled)) {
      queryInternal(buildExecuteRequest(cacheEntry, request, options), options, true)
    }
    else if (enhancedEnabled) {
      queryInternal(buildPrepareRequest(request, options), options, true)
        .flatMap((qr: QueryResponse) => {
          val preparedName = qr.header().prepared()
          if (!preparedName.isPresent) {
            Mono.error(new CouchbaseException("No prepared name present but must be, this is a query bug!"))
          }
          else {
            queryCache.put(
              request.statement(),
              QueryCacheEntry(preparedName.get(), fullPlan = false, None)
            )
            return Mono.just(qr)
          }
        })
    }
    else {
      Mono.defer(() => {
        val req = buildPrepareRequest(request, options)
        core.send(req)
        FutureConversions.javaMonoToScalaMono(Reactor.wrap(req, req.response, true))
      })

        .flatMapMany(result => result.rows())

        // Only expect one row back, but no harm handling multiple
        .doOnNext(row => {
        val json: Try[JsonObjectSafe] = Conversions.Decodable.JsonObjectSafeConvert.decode(row.data(),
          Conversions.JsonFlags)
        val nameOpt: Option[String] = json.flatMap(_.str("name")).toOption
        val plan: Option[String] = if (enhancedEnabled) None else json.flatMap(_.str("encoded_plan")).toOption

        nameOpt match {
          case Some(name) =>
            val entry = QueryCacheEntry(name, !enhancedEnabled, plan)
            queryCache.put(request.statement, entry)
          case _ =>
        }
      })

        .`then`()

        .`then`(Mono.defer(() => maybePrepareAndExecute(request, options)))

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
  private def queryReactive(request: QueryRequest, options: QueryOptions): Mono[ReactiveQueryResult] = {
    queryInternal(request, options, options.adhoc)
      .map(v => convertResponse(v))
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

    new QueryRequest(original.timeout, original.context, original.retryStrategy, original.credentials,
      statement, query.toString.getBytes(StandardCharsets.UTF_8))
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
  private def buildExecuteRequest(cacheEntry: QueryCacheEntry, original: QueryRequest,
                                  originalOptions: QueryOptions): QueryRequest = {
    val query = cacheEntry.export

    query.put("timeout", encodeDurationToMs(original.timeout))

    originalOptions.encode(query)

    new QueryRequest(original.timeout,
      original.context,
      original.retryStrategy,
      original.credentials,
      original.statement,
      query.toString.getBytes(StandardCharsets.UTF_8))
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
  def queryReactive(statement: String,
                    options: QueryOptions,
                    environment: ClusterEnvironment): Mono[ReactiveQueryResult] = {
    request(statement, options, environment) match {
      case Success(req) => queryReactive(req, options)
      case Failure(err) => Mono.error(err)
    }
  }

  /* Performs a N1QL query, returning the result as a Future.
   *
   * Rows are buffered in-memory.
   */
  def queryAsync(statement: String,
                 options: QueryOptions,
                 environment: ClusterEnvironment): Future[QueryResult] = {

    request(statement, options, environment) match {
      case Success(req) =>
        queryReactive(req, options)
          // Buffer the responses
          .flatMap(response => response.rows.collectSeq()
          .flatMap(rows => response.meta
            .map(meta => QueryResult(
              rows,
              meta)
            )
          )
        ).toFuture

      case Failure(err) => Future.failed(err)
    }
  }
}
