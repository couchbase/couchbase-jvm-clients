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

package com.couchbase.client.scala.view

import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.json.JsonArray
import com.couchbase.client.scala.transformers.JacksonTransformers

import scala.concurrent.duration.Duration
import scala.util.Try

/** Customize the execution of a view.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
case class ViewOptions(
    private[scala] val namespace: Option[DesignDocumentNamespace] = None,
    private[scala] val reduce: Option[Boolean] = None,
    private[scala] val limit: Option[Int] = None,
    private[scala] val group: Option[Boolean] = None,
    private[scala] val groupLevel: Option[Int] = None,
    private[scala] val inclusiveEnd: Option[Boolean] = None,
    private[scala] val skip: Option[Int] = None,
    private[scala] val onError: Option[ViewErrorMode] = None,
    private[scala] val debug: Option[Boolean] = None,
    private[scala] val order: Option[ViewOrdering] = None,
    private[scala] val key: Option[String] = None,
    private[scala] val startKeyDocId: Option[String] = None,
    private[scala] val endKeyDocId: Option[String] = None,
    private[scala] val endKey: Option[String] = None,
    private[scala] val startKey: Option[String] = None,
    private[scala] val keys: Option[String] = None,
    private[scala] val retryStrategy: Option[RetryStrategy] = None,
    private[scala] val timeout: Option[Duration] = None,
    private[scala] val scanConsistency: Option[ViewScanConsistency] = None,
    private[scala] val parentSpan: Option[RequestSpan] = None
) {

  /** Sets the parent `RequestSpan`.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  @Volatile
  def parentSpan(value: RequestSpan): ViewOptions = {
    copy(parentSpan = Some(value))
  }

  /** Sets in which namespace the view exists.
    *
    * @return this for further chaining
    */
  def namespace(value: DesignDocumentNamespace): ViewOptions = {
    copy(namespace = Some(value))
  }

  /** Explicitly enable/disable the reduce function on the query.
    *
    * @return this for further chaining
    */
  def reduce(value: Boolean): ViewOptions = {
    copy(reduce = Some(value))
  }

  /** Limit the number of the returned documents to the specified number.
    *
    * @return this for further chaining
    */
  def limit(value: Int): ViewOptions = {
    copy(limit = Some(value))
  }

  /** Group the results using the reduce function to a group or single row.
    *
    * Important: this setter and `groupLevel` should not be used
    * together. It is sufficient to only set the
    * grouping level only and use this setter in cases where you always want the
    * highest group level implicitly.
    *
    * @return this for further chaining
    */
  def group(value: Boolean): ViewOptions = {
    copy(group = Some(value))
  }

  /** Specifies the depth within the key to group results.
    *
    * Important: this setter and `groupLevel` should not be used
    * together. It is sufficient to only set the
    * grouping level only and use this setter in cases where you always want the
    * highest group level implicitly.
    *
    * @return this for further chaining
    */
  def groupLevel(level: Int): ViewOptions = {
    copy(groupLevel = Some(level))
  }

  /** Specifies whether the specified end key should be included in the result.
    *
    * @return this for further chaining
    */
  def inclusiveEnd(value: Boolean): ViewOptions = {
    copy(inclusiveEnd = Some(value))
  }

  /** Skip this number of records before starting to return the results.
    *
    * @return this for further chaining
    */
  def skip(value: Int): ViewOptions = {
    copy(skip = Some(value))
  }

  /** Sets the response in the event of an error.
    *
    * @return this for further chaining
    */
  def onError(value: ViewErrorMode): ViewOptions = {
    copy(onError = Some(value))
  }

  /** Enable debugging on view queries.
    *
    * @return this for further chaining
    */
  def debug(value: Boolean): ViewOptions = {
    copy(debug = Some(value))
  }

  /** Specifies the results ordering.  See [[ViewOrdering]] for details.
    *
    * @return this for further chaining
    */
  def order(value: ViewOrdering): ViewOptions = {
    copy(order = Some(value))
  }

  /** Specifies a specific key to fetch from the index.
    *
    * @return this for further chaining
    */
  def key(value: Any): ViewOptions = {
    value match {
      case s: String => copy(key = Some("\"" + s + "\""))
      case _         => copy(key = Some(value.toString))
    }
  }

  /** Specifies the document id to start returning results at within a number of results should `startKey` have multiple
    * entries within the index.
    *
    * @return this for further chaining
    */
  def startKeyDocId(value: String): ViewOptions = {
    copy(startKeyDocId = Some(value))
  }

  /** Specifies the document id to stop returning results at within a number of results should `endKey` have multiple
    * entries within the index.
    *
    * @return this for further chaining
    */
  def endKeyDocId(value: String): ViewOptions = {
    copy(endKeyDocId = Some(value))
  }

  /** Specifies the key to stop returning results at.
    *
    * @return this for further chaining
    */
  def endKey(value: Any): ViewOptions = {
    value match {
      case s: String =>
        copy(endKey = Try(JacksonTransformers.MAPPER.writeValueAsString(value)).toOption)
      case _ => copy(endKey = Some(value.toString))
    }
  }

  /** Specifies the key to skip to before beginning to return results.
    *
    * @return this for further chaining
    */
  def startKey(value: Any): ViewOptions = {
    value match {
      case s: String =>
        copy(startKey = Try(JacksonTransformers.MAPPER.writeValueAsString(value)).toOption)
      case _ => copy(startKey = Some(value.toString))
    }
  }

  /** Specifies a specific set of keys to fetch from the index.
    *
    * @return this for further chaining
    */
  def keys(values: Iterable[Any]): ViewOptions = {
    val arr = JsonArray.create
    values.foreach(v => {
      arr.add(v match {
        case x: String => "\"" + x + "\""
        case _         => v.toString
      })
    })
    copy(keys = Some(arr.toString))
  }

  /** When to timeout the operation.
    *
    * @return this for further chaining
    */
  def timeout(timeout: Duration): ViewOptions = {
    copy(timeout = Option(timeout))
  }

  /** Sets what retry strategy to use if the operation fails.
    *
    * @param strategy the retry strategy to use
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def retryStrategy(strategy: RetryStrategy): ViewOptions = {
    copy(retryStrategy = Some(strategy))
  }

  /** Sets what scan consistency to use.  See [[ViewScanConsistency]] for details.
    *
    * @param scanConsistency the scan consistency to use
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def scanConsistency(scanConsistency: ViewScanConsistency): ViewOptions = {
    copy(scanConsistency = Some(scanConsistency))
  }

  private[scala] def encode() = {
    val sb = new StringBuilder()
    Seq(
      "reduce"         -> reduce,
      "limit"          -> limit,
      "group"          -> group,
      "group_level"    -> groupLevel,
      "inclusive_end"  -> inclusiveEnd,
      "skip"           -> skip,
      "on_error"       -> onError.map(_.encode),
      "debug"          -> debug,
      "descending"     -> order,
      "key"            -> key,
      "startkey_docid" -> startKeyDocId,
      "endkey_docid"   -> endKeyDocId,
      "endkey"         -> endKey,
      "startkey"       -> startKey,
      "stale"          -> scanConsistency.map(_.encoded)
    ).foreach {
      case (key, valueOpt) =>
        valueOpt.foreach(sb ++= key += '=' ++= _.toString += '&')
    }
    sb.toString.stripSuffix("&")
  }
}
