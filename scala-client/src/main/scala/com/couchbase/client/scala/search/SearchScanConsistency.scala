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
package com.couchbase.client.scala.search

import com.couchbase.client.scala.kv.MutationState

/** Provides control over which mutations, if any, any required search indexes should contain, before a search query
  * is executed.
  */
sealed trait SearchScanConsistency

object SearchScanConsistency {

  /** The query will be executed immediately without waiting for any index updates.
    */
  case object NotBounded extends SearchScanConsistency

  /** The query will block execution until any indexes used are consistent with (e.g. contain) the mutations in the
    * provided `MutationState`.
    */
  case class ConsistentWith(ms: MutationState) extends SearchScanConsistency
}
