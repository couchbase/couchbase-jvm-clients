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

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.core.api.manager.CoreBucketAndScope
import com.couchbase.client.core.api.query.CoreQueryContext
import com.couchbase.client.core.api.search.CoreHighlightStyle
import com.couchbase.client.core.api.search.CoreSearchKeyset
import com.couchbase.client.core.api.search.CoreSearchOps
import com.couchbase.client.core.api.search.CoreSearchOptions
import com.couchbase.client.core.api.search.CoreSearchScanConsistency
import com.couchbase.client.core.api.search.facet.CoreSearchFacet
import com.couchbase.client.core.api.search.sort.CoreSearchSort
import com.couchbase.client.core.api.shared.CoreMutationState
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.core.error.FeatureNotAvailableException
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION
import com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.kotlin.analytics.AnalyticsFlowItem
import com.couchbase.client.kotlin.analytics.AnalyticsParameters
import com.couchbase.client.kotlin.analytics.AnalyticsPriority
import com.couchbase.client.kotlin.analytics.AnalyticsScanConsistency
import com.couchbase.client.kotlin.analytics.internal.AnalyticsExecutor
import com.couchbase.client.kotlin.annotations.UncommittedCouchbaseApi
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.internal.requireUnique
import com.couchbase.client.kotlin.internal.toOptional
import com.couchbase.client.kotlin.manager.search.ScopeSearchIndexManager
import com.couchbase.client.kotlin.query.QueryFlowItem
import com.couchbase.client.kotlin.query.QueryMetadata
import com.couchbase.client.kotlin.query.QueryParameters
import com.couchbase.client.kotlin.query.QueryProfile
import com.couchbase.client.kotlin.query.QueryResult
import com.couchbase.client.kotlin.query.QueryRow
import com.couchbase.client.kotlin.query.QueryScanConsistency
import com.couchbase.client.kotlin.query.internal.QueryExecutor
import com.couchbase.client.kotlin.search.Direction
import com.couchbase.client.kotlin.search.Highlight
import com.couchbase.client.kotlin.search.Score
import com.couchbase.client.kotlin.search.SearchFacet
import com.couchbase.client.kotlin.search.SearchFlowItem
import com.couchbase.client.kotlin.search.SearchMetadata
import com.couchbase.client.kotlin.search.SearchPage
import com.couchbase.client.kotlin.search.SearchResult
import com.couchbase.client.kotlin.search.SearchRow
import com.couchbase.client.kotlin.search.SearchScanConsistency
import com.couchbase.client.kotlin.search.SearchSort
import com.couchbase.client.kotlin.search.SearchSpec
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitSingle
import java.util.*
import java.util.concurrent.ConcurrentHashMap

public class Scope(
    public val name: String,
    public val bucket: Bucket,
) {
    internal val couchbaseOps: CoreCouchbaseOps = bucket.couchbaseOps
    private val searchOps = couchbaseOps.searchOps(CoreBucketAndScope(bucket.name, name))

    private val collectionCache = ConcurrentHashMap<String, Collection>()

    private val queryExecutor = QueryExecutor(
        couchbaseOps.queryOps(),
        CoreQueryContext.of(bucket.name, name),
        bucket.env.jsonSerializer,
    )

    private val analyticsExecutor
        get() = AnalyticsExecutor(couchbaseOps.asCore(), this)

    /**
     * A manager for administering scope-level Full-Text Search indexes.
     */
    @SinceCouchbase("7.6")
    public val searchIndexes: ScopeSearchIndexManager = ScopeSearchIndexManager(
        couchbaseOps.scopeSearchIndexManager(CoreBucketAndScope(bucket.name, name))
    )

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
                couchbaseOps.ifCore {
                    configurationProvider().refreshCollectionId(
                        CollectionIdentifier(bucket.name, scopeName.toOptional(), collectionName.toOptional())
                    )
                }
            }
            Collection(collectionName, this)
        }
    }

    /**
     * Returns a Flow which may be collected to execute a scope-level
     * SQL++ query and process the results.
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
     * @param statement the SQL++ statement to execute.
     *
     * @param common options common to all requests.
     *
     * @param parameters parameters to the SQL++ statement.
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
     * @param readonly pass true if the SQL++ statement does not modify documents.
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
     * diagnosing a specific issue.  Note this is an Enterprise Edition feature.
     * On Community Edition the parameter will be accepted, but no profiling
     * information returned.
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
        @SinceCouchbase("7.6") useReplica: Boolean? = null,
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
            useReplica
        )
    }

    @Deprecated(level = DeprecationLevel.HIDDEN, message = "Use similar method with additional useReplica parameter.")
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
        raw: Map<String, Any?> = emptyMap()

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
            null
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

    /**
     * Returns a Flow which can be collected to execute a Full-Text Search
     * (vector, non-vector, or mixed-mode) against a scope-level index.
     *
     * The returned Flow is cold, meaning the query is not executed unless
     * the Flow is collected. If you collect the flow multiple times,
     * the query is executed each time.
     *
     * The extension function `Flow<SearchFlowItem>.execute()` may be used
     * when the results are known to fit in memory. It simply collects the flow
     * into a [SearchResult].
     *
     * For larger query results, prefer the streaming version which takes a
     * lambda to invoke when each row is received from the server:
     * `Flow<SearchFlowItem>.execute { row -> ... }`.
     *
     * @param indexName Index to search.
     *
     * @param spec Condition a document must match in order to be included in the search results.
     *
     * @param page Specifies which rows of the search result to return.
     *
     * @param limit Number of rows to return (page size).
     *
     * @param sort Specifies how the results should be sorted.
     * For tiered sort (sort by X then by Y) see [SearchSort.then].
     *
     * @param fields Stored fields to include in the result rows, or `listOf("*")`
     * to include all stored fields.
     *
     * @param facets Specifies the facets to include in the search results.
     * A facet counts the number of documents in the full, unpaginated search results
     * that meet certain criteria. Facet results may be retrieved from either
     * [SearchResult] or [SearchMetadata], whichever is more convenient.
     *
     * @param highlight Specifies whether to include fragments of text
     * with the matching search term highlighted. If highlighting is requested,
     * the result also includes location information for matched terms.
     *
     * @param includeLocations If true, triggers the inclusion of location information
     * for matched terms. If highlighting is requested, locations are always included
     * and this parameter has no effect.
     *
     * @param score The scoring algorithm to use.
     *
     * @param explain If true, [SearchRow.explanation] holds the bytes of a JSON Object
     * describing how the score was calculated.
     *
     * @param collections If not empty, only search within the named collections.
     * Requires an index defined on a non-default scope containing the collections.
     *
     * @param consistency Specifies whether to wait for the index to catch up with the
     * latest versions of certain documents. The default (unbounded) is fast, but means
     * the results might not reflect the latest document mutations.
     *
     * @param serializer Default serializer to use for [SearchRow.fieldsAs].
     * If not specified, defaults to the cluster environment's default serializer.
     *
     * @param raw The keys and values in this map are added to the query specification JSON.
     * This is an "escape hatch" for sending arguments supported by Couchbase Server, but not
     * by this version of the SDK.
     *
     * @sample com.couchbase.client.kotlin.samples.searchSimple
     * @sample com.couchbase.client.kotlin.samples.checkSearchResultForPartialFailure
     * @sample com.couchbase.client.kotlin.samples.searchQueryWithFacets
     * @sample com.couchbase.client.kotlin.samples.searchSimpleVector
     * @sample com.couchbase.client.kotlin.samples.searchSpecMixedMode
     */
    @UncommittedCouchbaseApi
    @SinceCouchbase("7.6")
    public fun search(
        indexName: String,
        spec: SearchSpec,
        common: CommonOptions = CommonOptions.Default,
        page: SearchPage = SearchPage.startAt(offset = 0),
        limit: Int? = null,
        sort: SearchSort = SearchSort.byScore(Direction.DESCENDING),
        fields: List<String> = emptyList(),
        facets: List<SearchFacet> = emptyList(),
        highlight: Highlight = Highlight.none(),
        includeLocations: Boolean = false,
        score: Score = Score.default(),
        explain: Boolean = false,
        @SinceCouchbase("7.0") collections: List<String> = emptyList(),
        consistency: SearchScanConsistency = SearchScanConsistency.notBounded(),
        serializer: JsonSerializer? = null,
        raw: Map<String, Any?> = emptyMap(),
    ): Flow<SearchFlowItem> = searchOps.search(
        indexName = indexName,
        spec = spec,
        common = common,
        page = page,
        limit = limit,
        sort = sort,
        fields = fields,
        facets = facets,
        highlight = highlight,
        includeLocations = includeLocations,
        score = score,
        explain = explain,
        collections = collections,
        consistency = consistency,
        serializer = serializer ?: bucket.env.jsonSerializer,
        raw = raw,
    )
}

internal fun CoreSearchOps.search(
    indexName: String,
    spec: SearchSpec,
    common: CommonOptions,
    page: SearchPage,
    limit: Int?,
    sort: SearchSort,
    fields: List<String>,
    facets: List<SearchFacet>,
    highlight: Highlight,
    includeLocations: Boolean,
    score: Score,
    explain: Boolean,
    @SinceCouchbase("7.0") collections: List<String>,
    consistency: SearchScanConsistency,
    serializer: JsonSerializer,
    raw: Map<String, Any?>,
): Flow<SearchFlowItem> {

    requireUnique(facets.map { it.name }) { duplicate ->
        "Each facet used in a request must have a unique name, but got multiple facts named '$duplicate'."
    }

    val opts = object : CoreSearchOptions {
        override fun collections() = collections

        override fun consistency(): CoreSearchScanConsistency? =
            when (consistency) {
                is SearchScanConsistency.NotBounded -> CoreSearchScanConsistency.NOT_BOUNDED
                is SearchScanConsistency.ConsistentWith -> null
            }

        override fun consistentWith(): CoreMutationState? =
            when (consistency) {
                is SearchScanConsistency.NotBounded -> null
                is SearchScanConsistency.ConsistentWith -> CoreMutationState(consistency.tokens)
            }

        override fun disableScoring(): Boolean = score is Score.None

        override fun explain(): Boolean? = explain

        override fun facets(): Map<String, CoreSearchFacet> {
            if (facets.isEmpty()) return emptyMap()
            val map = mutableMapOf<String, CoreSearchFacet>()
            facets.forEach { map[it.name] = it.core }
            return map
        }

        override fun fields(): List<String> = fields

        override fun highlightFields(): List<String> = when (highlight) {
            is Highlight.Companion.None -> emptyList()
            is Highlight.Companion.Specific -> highlight.fields
        }

        override fun highlightStyle(): CoreHighlightStyle? = when (highlight) {
            is Highlight.Companion.None -> null
            is Highlight.Companion.Specific -> {
                when (highlight.style?.name?.uppercase()) {
                    null -> CoreHighlightStyle.SERVER_DEFAULT
                    "HTML" -> CoreHighlightStyle.HTML
                    "ANSI" -> CoreHighlightStyle.ANSI
                    else -> throw FeatureNotAvailableException("Highlight style '${highlight.style}' not supported")
                }
            }
        }

        override fun limit(): Int? = limit

        override fun raw(): JsonNode? {
            return if (raw.isEmpty()) null else Mapper.convertValue(raw, ObjectNode::class.java)
        }

        override fun skip(): Int? = (page as? SearchPage.StartAt)?.offset
        override fun searchBefore(): CoreSearchKeyset? = (page as? SearchPage.SearchBefore)?.keyset?.core
        override fun searchAfter(): CoreSearchKeyset? = (page as? SearchPage.SearchAfter)?.keyset?.core

        override fun sort(): List<CoreSearchSort> = sort.core

        override fun includeLocations(): Boolean? = includeLocations

        override fun commonOptions(): CoreCommonOptions = common.toCore()
    }

    return flow {
        val response = searchReactive(
            indexName,
            spec.coreRequest,
            opts,
        ).awaitSingle()

        emitAll(response.rows().asFlow()
            .map { SearchRow.from(it, serializer) })

        val coreMetadata = response.metaData().awaitSingle()
        val coreFacets = response.facets().awaitSingle()

        emit(SearchMetadata(coreMetadata, coreFacets))
    }
}
