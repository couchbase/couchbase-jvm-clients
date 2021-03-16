/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.query

import com.couchbase.client.core.msg.kv.MutationToken
import com.couchbase.client.core.util.Golang
import com.couchbase.client.kotlin.kv.MutationState
import java.time.Duration

public sealed class QueryScanConsistency(
    private val wireName: String?,
    private val scanWait: Duration?,
) {

    internal open fun inject(queryJson: MutableMap<String, Any?>): Unit {
        wireName?.let { queryJson["scan_consistency"] = it }
        scanWait?.let { queryJson["scan_wait"] = Golang.encodeDurationToMs(it) }
    }

    public companion object {
        /**
         * The indexer will return whatever state it has to the query engine at the time of query.
         *
         * This is the default (for single-statement requests). No timestamp vector is used in the index scan. This is also
         * the fastest mode, because we avoid the cost of obtaining the vector, and we also avoid any wait for the index to
         * catch up to the vector.
         */
        public fun notBounded(): QueryScanConsistency =
            NotBounded

        /**
         * The indexer will wait until all mutations have been processed at the time of request before returning to the
         * query engine.
         *
         * This implements strong consistency per request. Before processing the request, a current vector is obtained. The
         * vector is used as a lower bound for the statements in the request. If there are DML statements in the request,
         * RYOW ("read your own write") is also applied within the request.
         */
        public fun requestPlus(scanWait: Duration? = null): QueryScanConsistency =
            RequestPlus(scanWait)

        public fun consistentWith(tokens: MutationState, scanWait: Duration? = null): QueryScanConsistency =
            ConsistentWith(tokens, scanWait)

        public fun consistentWith(tokens: Iterable<MutationToken>, scanWait: Duration? = null): QueryScanConsistency =
            ConsistentWith(MutationState(tokens), scanWait)
    }

    public object NotBounded : QueryScanConsistency(null, null)

    public class RequestPlus internal constructor(scanWait: Duration? = null) :
        QueryScanConsistency("request_plus", scanWait)

    public class ConsistentWith internal constructor(
        private val tokens: MutationState,
        scanWait: Duration? = null,
    ) : QueryScanConsistency("at_plus", scanWait) {
        override fun inject(queryJson: MutableMap<String, Any?>): Unit {
            super.inject(queryJson)
            queryJson["scan_vectors"] = tokens.export()
        }
    }
}
