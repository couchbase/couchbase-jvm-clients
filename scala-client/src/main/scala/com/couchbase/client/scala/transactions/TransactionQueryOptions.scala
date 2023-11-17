/**
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.scala.transactions

import com.couchbase.client.core.api.query.CoreQueryOptions
import com.couchbase.client.core.logging.RedactableArgument.redactUser
import com.couchbase.client.scala.json.{JsonArray, JsonArraySafe, JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.query.{QueryOptions, QueryParameters, QueryProfile, QueryScanConsistency}

/** Customize the execution of a N1QL query performed inside a transaction.
  */
case class TransactionQueryOptions private (
    private[scala] val parameters: Option[QueryParameters] = None,
    private[scala] val clientContextId: Option[String] = None,
    private[scala] val pipelineBatch: Option[Int] = None,
    private[scala] val pipelineCap: Option[Int] = None,
    private[scala] val profile: Option[QueryProfile] = None,
    private[scala] val readonly: Option[Boolean] = None,
    private[scala] val scanCap: Option[Int] = None,
    private[scala] val scanConsistency: Option[QueryScanConsistency] = None,
    private[scala] val adhoc: Boolean = true,
    private[scala] val raw: Option[Map[String, Any]] = None,
    private[scala] val flexIndex: Boolean = false,
    private[scala] val deferredException: Option[RuntimeException] = None
) {

  /** Provides named or positional parameters, for queries parameterised that way.
    *
    * See [[QueryParameters]] for details.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def parameters(values: QueryParameters): TransactionQueryOptions = {
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
  def clientContextId(contextId: String): TransactionQueryOptions =
    copy(clientContextId = Option(contextId))

  /** Advanced: Maximum number of items each execution operator can buffer between various operators.
    *
    * @param pipelineCap the pipeline_cap param.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def pipelineCap(pipelineCap: Int): TransactionQueryOptions = copy(pipelineCap = Some(pipelineCap))

  /** Advanced: Controls the number of items execution operators can batch for Fetch from the KV.
    *
    * @param pipelineBatch the pipeline_batch param.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def pipelineBatch(pipelineBatch: Int): TransactionQueryOptions =
    copy(pipelineBatch = Some(pipelineBatch))

  /** Set the profiling information level for query execution
    *
    * This is an Enterprise Edition feature.  On Community Edition the parameter will be accepted, but no profiling
    * information returned.
    *
    * @param profile the query profile level to be used
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def profile(profile: QueryProfile): TransactionQueryOptions = copy(profile = Option(profile))

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
  def readonly(readonly: Boolean): TransactionQueryOptions = copy(readonly = Option(readonly))

  /** Advanced: Maximum buffered channel size between the indexer client and the query service for index scans.
    *
    * This parameter controls when to use scan backfill. Use 0 or a negative number to disable.
    *
    * @param scanCap the scan_cap param, use 0 or negative number to disable.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def scanCap(scanCap: Int): TransactionQueryOptions = copy(scanCap = Option(scanCap))

  /** Scan consistency for the query.
    *
    * The default inside a transaction is [[QueryScanConsistency.RequestPlus]].
    *
    * @param scanConsistency the index scan consistency to be used
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def scanConsistency(scanConsistency: QueryScanConsistency): TransactionQueryOptions =
    copy(scanConsistency = Some(scanConsistency))

  /** If true (the default), adhoc mode is enabled: queries are just run.
    *
    * If false, adhoc mode is disabled and transparent prepared statement mode is enabled: queries
    * are first prepared so they can be executed more efficiently in the future.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def adhoc(adhoc: Boolean): TransactionQueryOptions = {
    copy(adhoc = adhoc)
  }

  /** Allows providing custom JSON key/value pairs for advanced usage.
    *
    * If available, it is recommended to use the methods on this object to customize the query. This method should
    * only be used if no such setter can be found (i.e. if an undocumented property should be set or you are using
    * an older client and a new server-configuration property has been added to the cluster).
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def raw(raw: Map[String, Any]): TransactionQueryOptions = {
    copy(raw = Some(raw))
  }

  /** Tells the query engine to use a flex index (utilizing the search service).
    *
    * The default is false.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def flexIndex(flexIndex: Boolean): TransactionQueryOptions = {
    copy(flexIndex = flexIndex)
  }

  private[scala] def toCore: CoreQueryOptions = {
    val converted = QueryOptions(
      parameters,
      clientContextId,
      None,
      true,
      pipelineBatch,
      pipelineCap,
      profile,
      readonly,
      None,
      scanCap,
      scanConsistency,
      None,
      None,
      adhoc,
      deferredException,
      None,
      raw
    )
    converted.toCore
  }
}

object TransactionQueryOptions {
  def apply() = new TransactionQueryOptions()
}
