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

/** Specifies the scan consistency on a view query. */
sealed trait ViewScanConsistency {
  private[scala] def encoded: String
}

object ViewScanConsistency {

  /** The view should be updated with all outstanding mutations before the query returns. */
  case object RequestPlus extends ViewScanConsistency {
    private[scala] def encoded: String = "false"
  }

  /** The view should be updated after the query. */
  case object UpdateAfter extends ViewScanConsistency {
    private[scala] def encoded: String = "update_after"
  }

  /** The query will use whatever is currently in the view, which will not be updated. */
  case object NotBounded extends ViewScanConsistency {
    private[scala] def encoded: String = "ok"
  }
}
