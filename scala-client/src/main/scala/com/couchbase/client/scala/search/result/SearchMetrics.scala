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

package com.couchbase.client.scala.search.result
import com.couchbase.client.core.api.search.result.CoreSearchMetrics

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

/** Metrics and status of a given FTS request. */
case class SearchMetrics(private val internal: CoreSearchMetrics) {

  /** How long a request took executing on the server side. */
  def took: Duration = Duration(internal.took.toNanos, TimeUnit.NANOSECONDS)

  /** Number of rows returned. */
  def totalRows: Long = internal.totalRows

  /** The largest score amongst the rows. */
  def maxScore: Double = internal.maxScore

  /** The total number of FTS indexes that were queried. */
  def totalPartitionCount: Long = internal.totalPartitionCount

  /** The number of FTS indexes queried that successfully answered. */
  def successPartitionCount: Long = internal.successPartitionCount

  /** The number of FTS indexes queried that gave an error. */
  def errorPartitionCount: Long = internal.errorPartitionCount
}
