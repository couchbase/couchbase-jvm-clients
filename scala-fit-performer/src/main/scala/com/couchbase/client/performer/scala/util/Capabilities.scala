/*
 * Copyright (c) 2022 Couchbase, Inc.
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
package com.couchbase.client.performer.scala.util

import com.couchbase.client.protocol.sdk.Caps

import java.util

object Capabilities {
  def sdkImplementationCaps: util.List[Caps] = {
    val out = new util.ArrayList[Caps]

    // [start:1.1.5]
    out.add(Caps.SDK_PRESERVE_EXPIRY)
    // [end:1.1.5]

    // [start:1.4.1]
    out.add(Caps.SDK_KV_RANGE_SCAN)
    // [end:1.4.1]

    out.add(Caps.SDK_QUERY_INDEX_MANAGEMENT)

    // [start:1.4.4]
    out.add(Caps.SDK_COLLECTION_QUERY_INDEX_MANAGEMENT)
    // [end:1.4.4]

    // SearchIndex.fromJson only added here and is crucial for testing
    // [start:1.4.5]
    out.add(Caps.SDK_SEARCH)
    // [end:1.4.5]

    //TODO: fully implement eventing function manager - Scala performer only supports getFunction currently
    out.add(Caps.SDK_EVENTING_FUNCTION_MANAGER)

    out.add(Caps.SDK_LOOKUP_IN)
    out.add(Caps.SDK_QUERY)
    out.add(Caps.SDK_QUERY_READ_FROM_REPLICA)

    // [start:1.4.11]
    out.add(Caps.SDK_BUCKET_MANAGEMENT)
    out.add(Caps.SDK_COLLECTION_MANAGEMENT)
    out.add(Caps.SDK_MANAGEMENT_HISTORY_RETENTION)
    // [end:1.4.11]

    out
  }

}
