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

package com.couchbase.client.kotlin

import com.couchbase.client.core.Core
import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION
import com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE
import com.couchbase.client.kotlin.analytics.AnalyticsFlowItem
import com.couchbase.client.kotlin.analytics.AnalyticsParameters
import com.couchbase.client.kotlin.analytics.AnalyticsPriority
import com.couchbase.client.kotlin.analytics.AnalyticsScanConsistency
import com.couchbase.client.kotlin.analytics.internal.AnalyticsExecutor
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.internal.toOptional
import com.couchbase.client.kotlin.query.QueryFlowItem
import com.couchbase.client.kotlin.query.QueryMetadata
import com.couchbase.client.kotlin.query.QueryParameters
import com.couchbase.client.kotlin.query.QueryProfile
import com.couchbase.client.kotlin.query.QueryResult
import com.couchbase.client.kotlin.query.QueryRow
import com.couchbase.client.kotlin.query.QueryScanConsistency
import com.couchbase.client.kotlin.query.internal.QueryExecutor
import kotlinx.coroutines.flow.Flow
import java.util.*
import java.util.concurrent.ConcurrentHashMap

public class Scope(
    public val name: String,
    public val bucket: Bucket,
) {
    internal val core: Core = bucket.core

    private val collectionCache = ConcurrentHashMap<String, Collection>()

    private val queryExecutor = QueryExecutor(core, this)
    private val analyticsExecutor = AnalyticsExecutor(core, this)

    /**
     * Opens a collection for this scope.
     *
     * @param name the collection name.
     */
    public fun collection(name: String): Collection {
        val collectionName = name
        val scopeName = this.name
        return collectionCache.computeIfAbsent(name) {
            val defaultScopeAndCollection = collectionName == DEFAULT_COLLECTION && scopeName == DEFAULT_SCOPE
            if (!defaultScopeAndCollection) {
                core.configurationProvider().refreshCollectionId(
                    CollectionIdentifier(bucket.name, scopeName.toOptional(), collectionName.toOptional())
                )
            }
            Collection(collectionName, this)
        }
    }

    public fun defaultCollection(): Collection = collection(DEFAULT_COLLECTION)

    /**
     * Returns a Flow which may be collected to execute a scope-level
     * N1QL query and process the results.
     *
     * The returned Flow is cold, meaning the query is not executed unless
     * the Flow is collected. If you collect the flow multiple times,
     * the query is executed each time.
     *
     * The extension function `Flow<QueryFlowItem>.execute()` may be used when
     * when the results are known to fit in memory. It simply collects the flow
     * into a [QueryResult].
     *
     * For larger query results, prefer the streaming version which takes a
     * lambda to invoke when each row is received from the server:
     * `Flow<QueryFlowItem>.execute { row -> ... }`.
     *
     * @param statement the N1QL statement to execute.
     *
     * @param common options common to all requests.
     *
     * @param parameters parameters to the N1QL statement.
     *
     * @param preserveExpiry pass true if you want the query engine to preserve
     * existing expiration times for any documents modified by this query.
     * *Requires Couchbase Server 7.1 or later.*
     *
     * @param serializer the serializer to use for converting parameters to JSON,
     * and the default serializer for parsing [QueryRow] content.
     * Defaults to the serializer configured on the cluster environment.
     *
     * @param consistency required if you want to read your own writes.
     * Values other than [QueryScanConsistency.notBounded]
     * tell the server to wait for the indexer to catch up with a certain
     * state of the K/V service before executing the query.
     *
     * @param readonly pass true if the N1QL statement does not modify documents.
     * This allows the client to retry the query if necessary.
     *
     * @param adhoc pass false if this is a commonly used query that should be
     * turned into a prepared statement for faster execution.
     *
     * @param flexIndex pass true to use a full-text index instead of a query index.
     *
     * @param metrics pass true to include metrics in the response (access via
     * [QueryMetadata.metrics]). Relatively inexpensive, and may be enabled
     * in production with minimal impact.
     *
     * @param profile specifies how much profiling information to include in
     * the response (access via [QueryMetadata.profile]). Profiling is
     * relatively expensive, and can impact the performance of the server
     * query engine. Not recommended for use in production, unless you're
     * diagnosing a specific issue.
     *
     * @param maxParallelism Specifies the maximum parallelism for the query.
     *
     * @param scanCap Maximum buffered channel size between the indexer client
     * and the query service for index scans. This parameter controls when to use
     * scan backfill. Use 0 or a negative number to disable. Smaller values
     * reduce GC, while larger values reduce indexer backfill.
     *
     * @param pipelineBatch Controls the number of items execution operators
     * can batch for Fetch from the KV.
     *
     * @param pipelineCap Maximum number of items each execution operator
     * can buffer between various operators.
     *
     * @param clientContextId an arbitrary string that identifies this query
     * for diagnostic purposes.
     *
     * @param raw an "escape hatch" for passing arbitrary query options that
     * aren't otherwise exposed by this method.
     */
    public fun query(
        statement: String,
        common: CommonOptions = CommonOptions.Default,
        parameters: QueryParameters = QueryParameters.None,
        @SinceCouchbase("7.1") preserveExpiry: Boolean = false,

        serializer: JsonSerializer? = null,

        consistency: QueryScanConsistency = QueryScanConsistency.notBounded(),
        readonly: Boolean = false,
        adhoc: Boolean = true,
        flexIndex: Boolean = false,

        metrics: Boolean = false,
        profile: QueryProfile = QueryProfile.OFF,

        maxParallelism: Int? = null,
        scanCap: Int? = null,
        pipelineBatch: Int? = null,
        pipelineCap: Int? = null,

        clientContextId: String? = UUID.randomUUID().toString(),
        raw: Map<String, Any?> = emptyMap(),

        ): Flow<QueryFlowItem> {

        return queryExecutor.query(
            statement,
            common,
            parameters,
            preserveExpiry,
            serializer,
            consistency,
            readonly,
            adhoc,
            flexIndex,
            metrics,
            profile,
            maxParallelism,
            scanCap,
            pipelineBatch,
            pipelineCap,
            clientContextId,
            raw,
        )
    }

    public fun analyticsQuery(
        statement: String,
        common: CommonOptions = CommonOptions.Default,
        parameters: AnalyticsParameters = AnalyticsParameters.None,

        serializer: JsonSerializer? = null,

        consistency: AnalyticsScanConsistency = AnalyticsScanConsistency.notBounded(),
        @SinceCouchbase("6.5") readonly: Boolean = false,
        priority: AnalyticsPriority = AnalyticsPriority.normal(),

        clientContextId: String? = UUID.randomUUID().toString(),
        raw: Map<String, Any?> = emptyMap(),

        ): Flow<AnalyticsFlowItem> {

        return analyticsExecutor.query(
            statement,
            common,
            parameters,
            serializer,
            consistency,
            readonly,
            priority,
            clientContextId,
            raw
        )
    }
}
