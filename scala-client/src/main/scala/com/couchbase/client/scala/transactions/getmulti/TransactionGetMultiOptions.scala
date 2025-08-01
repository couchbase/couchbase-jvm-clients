/*
 * Copyright 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.scala.transactions.getmulti

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.transaction.getmulti.CoreGetMultiOptions

/** Customize how a getMulti() operation runs.
  */
@Stability.Uncommitted
case class TransactionGetMultiOptions(
    mode: Option[TransactionGetMultiMode] = None
) {

  /** Controls how the operation behaves - see [[TransactionGetMultiMode]] for details.
    *
    * If not explicitly set, the default behaviour is intentionally unspecified, and may change in future versions.
    */
  def mode(mode: TransactionGetMultiMode): TransactionGetMultiOptions =
    copy(mode = Some(mode))

  /** Converts this options instance into the core representation. */
  private[transactions] def toCore: CoreGetMultiOptions =
    new CoreGetMultiOptions(mode.map(_.toCore).orNull)
}

object TransactionGetMultiOptions {
  private[transactions] val Default: TransactionGetMultiOptions = TransactionGetMultiOptions()
}
