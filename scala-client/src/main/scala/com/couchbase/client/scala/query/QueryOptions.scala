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

package com.couchbase.client.scala.query

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.api.query.{
  CoreQueryOptions,
  CoreQueryProfile,
  CoreQueryScanConsistency
}
import com.couchbase.client.core.api.shared.CoreMutationState
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.logging.RedactableArgument.redactUser
import com.couchbase.client.core.msg.kv.MutationToken
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.transaction.config.CoreSingleQueryTransactionOptions
import com.couchbase.client.scala.json.{JsonArray, JsonArraySafe, JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.util.DurationConversions._

import java.{lang, time}
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

/** Customize the execution of a N1QL query.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
case class QueryOptions(
    private[scala] val parameters: Option[QueryParameters] = None,
    private[scala] val clientContextId: Option[String] = None,
    private[scala] val maxParallelism: Option[Int] = None,
    private[scala] val metrics: Boolean = false,
    private[scala] val pipelineBatch: Option[Int] = None,
    private[scala] val pipelineCap: Option[Int] = None,
    private[scala] val profile: Option[QueryProfile] = None,
    private[scala] val readonly: Option[Boolean] = None,
    private[scala] val retryStrategy: Option[RetryStrategy] = None,
    private[scala] val scanCap: Option[Int] = None,
    private[scala] val scanConsistency: Option[QueryScanConsistency] = None,
    private[scala] val consistentWith: Option[Seq[MutationToken]] = None,
    private[scala] val timeout: Option[Duration] = None,
    private[scala] val adhoc: Boolean = true,
    private[scala] val deferredException: Option[RuntimeException] = None,
    private[scala] val parentSpan: Option[RequestSpan] = None,
    private[scala] val raw: Option[Map[String, Any]] = None,
    private[scala] val flexIndex: Boolean = false,
    @SinceCouchbase("7.1") private[scala] val preserveExpiry: Option[Boolean] = None,
    @SinceCouchbase("7.6") private[scala] val useReplica: Option[Boolean] = None
) {

  /** Sets the parent `RequestSpan`.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  @Volatile
  def parentSpan(value: RequestSpan): QueryOptions = {
    copy(parentSpan = Some(value))
  }

  /** Provides named or positional parameters, for queries parameterised that way.
    *
    * See [[QueryParameters]] for details.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def parameters(values: QueryParameters): QueryOptions = {
    val de = deferredException.orElse(values match {
      case QueryParameters.Named(params) => checkTypes(params.values)
      case v: QueryParameters.Positional => checkTypes(v.parameters)
      case _                             => None
    })
    copy(
      parameters = Some(values),
      deferredException = de
    )
  }

  private def checkTypes(in: Iterable[Any]): Option[RuntimeException] = {
    var out: Option[RuntimeException] = None

    in.foreach(value => {
      if (value != null) {
        value match {
          case _: String         =>
          case _: Int            =>
          case _: Long           =>
          case _: Double         =>
          case _: Float          =>
          case _: Short          =>
          case _: Boolean        =>
          case _: JsonObject     =>
          case _: JsonObjectSafe =>
          case _: JsonArray      =>
          case _: JsonArraySafe  =>
          case _ =>
            out = Some(
              new IllegalArgumentException(s"Value '${redactUser(value)}' is not a valid JSON type")
            )
        }
      }
    })

    out
  }

  /** Adds a client context ID to the request, that will be sent back in the response, allowing clients
    * to meaningfully trace requests/responses when many are exchanged.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def clientContextId(contextId: String): QueryOptions = copy(clientContextId = Option(contextId))

  /** Allows to override the default maximum parallelism for the query execution on the server side.
    *
    * @param maxParallelism the maximum parallelism for this query, 0 or negative values disable it.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def maxParallelism(maxParallelism: Int): QueryOptions =
    copy(maxParallelism = Option(maxParallelism))

  /** Advanced: Maximum number of items each execution operator can buffer between various operators.
    *
    * @param pipelineCap the pipeline_cap param.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def pipelineCap(pipelineCap: Int): QueryOptions = copy(pipelineCap = Some(pipelineCap))

  /** Advanced: Controls the number of items execution operators can batch for Fetch from the KV.
    *
    * @param pipelineBatch the pipeline_batch param.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def pipelineBatch(pipelineBatch: Int): QueryOptions = copy(pipelineBatch = Some(pipelineBatch))

  /** Controls where metrics are returned by the server.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def metrics(metrics: Boolean): QueryOptions = copy(metrics = metrics)

  /** Set the profiling information level for query execution
    *
    * This is an Enterprise Edition feature.  On Community Edition the parameter will be accepted, but no profiling
    * information returned.
    *
    * @param profile the query profile level to be used
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def profile(profile: QueryProfile): QueryOptions = copy(profile = Option(profile))

  /** If set to true, it will signal the query engine on the server that only non-data modifying requests
    * are allowed. Note that this rule is enforced on the server and not the SDK side.
    *
    * Controls whether a query can change a resulting record set.
    *
    * If readonly is true, then the following statements are not allowed:
    *  - CREATE INDEX
    *  - DROP INDEX
    *  - INSERT
    *  - MERGE
    *  - UPDATE
    *  - UPSERT
    *  - DELETE
    *
    * @param readonly true if readonly should be forced, false is the default and will use the server side
    *                        default.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def readonly(readonly: Boolean): QueryOptions = copy(readonly = Option(readonly))

  /** Advanced: Maximum buffered channel size between the indexer client and the query service for index scans.
    *
    * This parameter controls when to use scan backfill. Use 0 or a negative number to disable.
    *
    * @param scanCap the scan_cap param, use 0 or negative number to disable.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def scanCap(scanCap: Int): QueryOptions = copy(scanCap = Option(scanCap))

  /** Scan consistency for the query
    *
    * @param scanConsistency the index scan consistency to be used
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def scanConsistency(scanConsistency: QueryScanConsistency): QueryOptions =
    copy(scanConsistency = Some(scanConsistency))

  /** Sets a maximum timeout for processing.
    *
    * @param timeout the duration of the timeout.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def timeout(timeout: Duration): QueryOptions = {
    copy(timeout = Option(timeout))
  }

  /** Sets what retry strategy to use if the operation fails.
    *
    * @param strategy the retry strategy to use
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def retryStrategy(strategy: RetryStrategy): QueryOptions = {
    copy(retryStrategy = Some(strategy))
  }

  /** If true (the default), adhoc mode is enabled: queries are just run.
    *
    * If false, adhoc mode is disabled and transparent prepared statement mode is enabled: queries
    * are first prepared so they can be executed more efficiently in the future.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def adhoc(adhoc: Boolean): QueryOptions = {
    copy(adhoc = adhoc)
  }

  /** Allows providing custom JSON key/value pairs for advanced usage.
    *
    * If available, it is recommended to use the methods on this object to customize the query. This method should
    * only be used if no such setter can be found (i.e. if an undocumented property should be set or you are using
    * an older client and a new server-configuration property has been added to the cluster).
    *
    * Note that the values will be passed through a JSON encoder, so do not provide already encoded JSON as the value. If
    * you want to pass objects or arrays, you can use `JsonObject` and `JsonArray` respectively.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def raw(raw: Map[String, Any]): QueryOptions = {
    copy(raw = Some(raw))
  }

  /** Tells the query engine to use a flex index (utilizing the search service).
    *
    * The default is false.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def flexIndex(flexIndex: Boolean): QueryOptions = {
    copy(flexIndex = flexIndex)
  }

  /** Tells the query engine to preserve expiration values set on any documents modified by this query.
    *
    * The default is false.
    *
    * This feature works from Couchbase Server 7.1.0 onwards.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  @SinceCouchbase("7.1")
  def preserveExpiry(preserveExpiry: Boolean): QueryOptions = {
    copy(preserveExpiry = Some(preserveExpiry))
  }

  /** Tells the query engine that replicas can be used.
    *
    * The default is not set
    *
    * This feature works from Couchbase Server 7.6.0 onwards.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  @SinceCouchbase("7.6")
  def useReplica(useReplica: Boolean): QueryOptions = {
    copy(useReplica = Some(useReplica))
  }

  private[scala] def toCore: CoreQueryOptions = {
    val x = this

    val t: java.util.Optional[java.time.Duration] = timeout
      .map(v => com.couchbase.client.scala.util.DurationConversions.scalaDurationToJava(v))
      .asJava
    val common = CoreCommonOptions.ofOptional(t, retryStrategy.asJava, parentSpan.asJava)

    new CoreQueryOptions {
      override def adhoc(): Boolean = x.adhoc

      override def clientContextId(): String = x.clientContextId.orNull

      override def consistentWith(): CoreMutationState = x.scanConsistency match {
        case Some(QueryScanConsistency.ConsistentWith(cw)) =>
          new CoreMutationState(cw.tokens.asJava)
        case _ => null
      }

      override def maxParallelism(): Integer = x.maxParallelism match {
        case Some(value) => value
        case _           => null
      }

      override def metrics(): Boolean = x.metrics

      override def namedParameters(): ObjectNode = parameters match {
        case Some(v: QueryParameters.Named) =>
          try {
            Mapper.convertValue(v.parameters.asJava, classOf[ObjectNode])
          } catch {
            case e: JsonProcessingException =>
              throw new InvalidArgumentException("Unable to convert named parameters", e, null)
          }
        case _ => null
      }

      override def pipelineBatch(): Integer = x.pipelineBatch match {
        case Some(value) => value
        case _           => null
      }

      override def pipelineCap(): Integer = x.pipelineCap match {
        case Some(value) => value
        case _           => null
      }

      override def positionalParameters(): ArrayNode = parameters match {
        case Some(v: QueryParameters.Positional) =>
          try {
            Mapper.convertValue(v.parameters.asJava, classOf[ArrayNode])
          } catch {
            case e: JsonProcessingException =>
              throw new InvalidArgumentException("Unable to convert named parameters", e, null)
          }
        case _ => null
      }

      override def profile(): CoreQueryProfile = x.profile match {
        case Some(QueryProfile.Off)     => CoreQueryProfile.OFF
        case Some(QueryProfile.Phases)  => CoreQueryProfile.PHASES
        case Some(QueryProfile.Timings) => CoreQueryProfile.TIMINGS
        case _                          => null
      }

      override def raw(): JsonNode = x.raw match {
        case Some(value) => Mapper.convertValue(value.asJava, classOf[ObjectNode])
        case _           => null
      }

      override def readonly(): Boolean = x.readonly.getOrElse(false)

      override def scanWait(): time.Duration = x.scanConsistency match {
        case Some(QueryScanConsistency.RequestPlus(Some(scanWait))) => scanWait
        case _                                                      => null
      }

      override def scanCap(): Integer = x.scanCap match {
        case Some(value) => value
        case _           => null
      }

      override def scanConsistency(): CoreQueryScanConsistency = x.scanConsistency match {
        case Some(_: QueryScanConsistency.RequestPlus) => CoreQueryScanConsistency.REQUEST_PLUS
        case Some(QueryScanConsistency.NotBounded)     => CoreQueryScanConsistency.NOT_BOUNDED
        case _                                         => null
      }

      override def flexIndex(): Boolean = x.flexIndex

      override def preserveExpiry(): lang.Boolean = x.preserveExpiry match {
        case Some(value) => value
        case _           => null
      }

      override def asTransactionOptions(): CoreSingleQueryTransactionOptions = null

      override def commonOptions(): CoreCommonOptions = common

      override def useReplica(): lang.Boolean = x.useReplica match {
        case Some(value) => value
        case _           => null
      }
    }
  }
}

object QueryOptions {
  def apply() = new QueryOptions()
}
