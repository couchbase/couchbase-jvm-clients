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

package com.couchbase.client.kotlin.analytics

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode
import com.couchbase.client.kotlin.analytics.AnalyticsScanConsistency.Companion.notBounded
import com.couchbase.client.kotlin.analytics.AnalyticsScanConsistency.Companion.requestPlus
import com.couchbase.client.kotlin.query.QueryScanConsistency.Companion.notBounded
import com.couchbase.client.kotlin.query.QueryScanConsistency.Companion.requestPlus

/**
 * Create instances using one of the factory methods.
 *
 * @see requestPlus
 * @see notBounded
 */
public sealed class AnalyticsScanConsistency(
    private val wireName: String?,
) {

    internal open fun inject(queryJson: ObjectNode) {
        wireName?.let { queryJson.put("scan_consistency", it) }
    }

    public companion object {
        /**
         * For when speed matters more than consistency. Executes the query
         * immediately, without waiting for prior K/V mutations to be indexed.
         */
        public fun notBounded(): AnalyticsScanConsistency = NotBounded

        /**
         * Strong consistency. Waits for all prior K/V mutations to be indexed
         * before executing the query.
         */
        public fun requestPlus(): AnalyticsScanConsistency = RequestPlus
    }

    private object NotBounded : AnalyticsScanConsistency(null)

    private object RequestPlus : AnalyticsScanConsistency("request_plus")

}

