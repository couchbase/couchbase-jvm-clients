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

import com.couchbase.client.core.msg.kv.MutationToken
import com.couchbase.client.scala.kv.MutationState

import scala.concurrent.duration.Duration

/** Provides some control over what consistency is required for a given query. */
sealed trait QueryScanConsistency {
  private[scala] def encoded: String
}

object QueryScanConsistency {

  /** The default.  Any indexes used by the query reflect their current content. */
  case object NotBounded extends QueryScanConsistency {
    private[scala] def encoded = "not_bounded"
  }

  /** The query blocks until any indexes used by the query are updated to reflect any pending mutations. */
  case class RequestPlus(scanWait: Option[Duration] = None) extends QueryScanConsistency {
    private[scala] def encoded = "request_plus"
  }

  /** The query blocks until any indexes used by the query are updated to reflect any pending mutations. */
  case class ConsistentWith(consistentWith: MutationState) extends QueryScanConsistency {
    private[scala] def encoded = "at_plus"
  }
}
