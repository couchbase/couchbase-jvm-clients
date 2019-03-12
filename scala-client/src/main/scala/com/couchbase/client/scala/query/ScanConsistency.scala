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

import scala.concurrent.duration.Duration

/** Provides some control over what consistency is required for a given query. */
sealed trait ScanConsistency {
  private[scala] def encoded: String
}

object ScanConsistency {

  /** The default.  Any indexes used by the query reflect their current content. */
  case object NotBounded extends ScanConsistency {
    private[scala] def encoded = "not_bounded"
  }

  //case class AtPlus(consistentWith: List[MutationToken], scanWait: Option[Duration] = None) extends ScanConsistency

  /** The query blocks until any indexes used by the query are updated to reflect any pending mutations. */
  case class RequestPlus(scanWait: Option[Duration] = None) extends ScanConsistency {
    private[scala] def encoded = "request_plus"
  }
}