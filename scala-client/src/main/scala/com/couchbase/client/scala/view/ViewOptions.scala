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

import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.json.{JsonArray, JsonArraySafe, JsonObject}
import com.couchbase.client.scala.query.QueryOptions

import scala.concurrent.duration.Duration



/** Customize the execution of a view.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
case class ViewOptions(
                        private[scala] val development: Option[Boolean] = None,
                        private[scala] val reduce: Option[Boolean] = None,
                        private[scala] val limit: Option[Int] = None,
                        private[scala] val group: Option[Boolean] = None,
                        private[scala] val groupLevel: Option[Int] = None,
                        private[scala] val inclusiveEnd: Option[Boolean] = None,
                        private[scala] val skip: Option[Int] = None,
                        private[scala] val stale: Option[Stale] = None,
                        private[scala] val onError: Option[OnError] = None,
                        private[scala] val debug: Option[Boolean] = None,
                        private[scala] val descending: Option[Boolean] = None,
                        private[scala] val key: Option[String] = None,
                        private[scala] val startKeyDocId: Option[String] = None,
                        private[scala] val endKeyDocId: Option[String] = None,
                        private[scala] val endKey: Option[String] = None,
                        private[scala] val startKey: Option[String] = None,
                        private[scala] val keys: Option[String] = None,
                        private[scala] val retryStrategy: Option[RetryStrategy] = None,
                        private[scala] val timeout: Option[Duration] = None
                      ) {

  def development(value: Boolean): ViewOptions = {
    copy(development = Some(value))
  }

  /** Explicitly enable/disable the reduce function on the query.
    *
    * @return this for further chaining
    */
  def reduce(value: Boolean): ViewOptions = {
    copy(reduce = Some(value))
  }

  /**
    * Limit the number of the returned documents to the specified number.
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

  /** Specify the group level to be used.
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

  /** Allow the results from a stale view to be used.
    *
    * The default is Stale.UpdateAfter.
    *
    * @return this for further chaining
    */
  def stale(value: Stale): ViewOptions = {
    copy(stale = Some(value))
  }

  /** Sets the response in the event of an error.
    *
    * @return this for further chaining
    */
  def onError(value: OnError): ViewOptions = {
    copy(onError = Some(value))
  }

  /** Enable debugging on view queries.
    *
    * @return this for further chaining
    */
  def debug(value: Boolean): ViewOptions = {
    copy(debug = Some(value))
  }

  /** Return the documents in descending key order.
    *
    * @return this for further chaining
    */
  def descending(value: Boolean): ViewOptions = {
    copy(descending = Some(value))
  }

  /** Return the documents in descending key order.
    *
    * @return this for further chaining
    */
  def key(value: Any): ViewOptions = {
    value match {
      case s: String => copy(key = Some('"' + s + "'"))
      case _ => copy(key = Some(value.toString))
    }
  }

  def startKeyDocId(value: String): ViewOptions = {
    copy(startKeyDocId = Some(value))
  }

  def endKeyDocId(value: String): ViewOptions = {
    copy(endKeyDocId = Some(value))
  }

  def endKey(value: Any): ViewOptions = {
    value match {
      case s: String => copy(endKey = Some('"' + s + "'"))
      case _ => copy(endKey = Some(value.toString))
    }
  }

  def startKey(value: Any): ViewOptions = {
    value match {
      case s: String => copy(startKey = Some('"' + s + "'"))
      case _ => copy(startKey = Some(value.toString))
    }
  }

  def keys(value: Any): ViewOptions = {
    value match {
      case v: String => copy(keys = Some(v))
      case JsonArray | JsonArraySafe => copy(keys = Some(value.toString))
      case _ =>
        // This will trigger a validation failure later
        copy(keys = null)
    }
  }

  def timeout(timeout: Duration): ViewOptions = {
    copy(timeout = Option(timeout))
  }

  // TODO make Scala wrappers for retry strategy

  /** Sets what retry strategy to use if the operation fails.  See [[RetryStrategy]] for details.
    *
    * @param strategy the retry strategy to use
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def retryStrategy(strategy: RetryStrategy): ViewOptions = {
    copy(retryStrategy = Some(strategy))
  }


  private[scala] def durationToN1qlFormat(duration: Duration) = {
    if (duration.toSeconds > 0) duration.toSeconds + "s"
    else duration.toNanos + "ns"
  }

  private[scala] def encode() = {
    val sb = new StringBuilder()

    reduce.foreach(v => {
      sb.append("reduce=")
      sb.append(v.toString)
      sb.append('&')
    })

    limit.foreach(v => {
      sb.append("limit=")
      sb.append(v.toString)
      sb.append('&')
    })

    group.foreach(v => {
      sb.append("group=")
      sb.append(v.toString)
      sb.append('&')
    })

    groupLevel.foreach(v => {
      sb.append("group_level=")
      sb.append(v.toString)
      sb.append('&')
    })

    inclusiveEnd.foreach(v => {
      sb.append("inclusive_end=")
      sb.append(v.toString)
      sb.append('&')
    })

    skip.foreach(v => {
      sb.append("skip=")
      sb.append(v.toString)
      sb.append('&')
    })

    stale.foreach(v => {
      sb.append("stale=")
      sb.append(v.encode)
      sb.append('&')
    })

    onError.foreach(v => {
      sb.append("on_error=")
      sb.append(v.encode)
      sb.append('&')
    })

    debug.foreach(v => {
      sb.append("debug=")
      sb.append(v.toString)
      sb.append('&')
    })

    descending.foreach(v => {
      sb.append("descending=")
      sb.append(v.toString)
      sb.append('&')
    })

    key.foreach(v => {
      sb.append("key=")
      sb.append(v.toString)
      sb.append('&')
    })

    startKeyDocId.foreach(v => {
      sb.append("startkey_docid=")
      sb.append(v.toString)
      sb.append('&')
    })

    endKeyDocId.foreach(v => {
      sb.append("endkey_docid=")
      sb.append(v.toString)
      sb.append('&')
    })

    endKey.foreach(v => {
      sb.append("endkey=")
      sb.append(v.toString)
      sb.append('&')
    })

    startKey.foreach(v => {
      sb.append("startkey=")
      sb.append(v.toString)
      sb.append('&')
    })

    sb.toString.stripSuffix("&")
  }
}





sealed trait Stale {
  private[scala] def encode: String
}

object Stale {
  case class True() extends Stale {
    private[scala] def encode: String = "ok"
  }

  case class False() extends Stale {
    private[scala] def encode: String = "false"
  }

  case class UpdateAfter() extends Stale {
    private[scala] def encode: String = "update_after"
  }
}

sealed trait OnError {
  private[scala] def encode: String
}

object OnError {
  case class Stop() extends OnError {
    private[scala] def encode: String = "stop"
  }

  case class Continue() extends Stale {
    private[scala] def encode: String = "continue"
  }
}
