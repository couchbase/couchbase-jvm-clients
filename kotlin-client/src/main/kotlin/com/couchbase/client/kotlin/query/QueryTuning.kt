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

/**
 * Advanced performance tuning options.
 *
 * @param flexIndex Specifies that the query should use a full-text index.
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

 * @param pipelineCap Maximum number of items each execution operator
 * can buffer between various operators.
 */
public class QueryTuning(
    public val flexIndex: Boolean = false,
    public val maxParallelism: Int? = null,
    public val scanCap: Int? = null,
    public val pipelineBatch: Int? = null,
    public val pipelineCap: Int? = null,
) {

    public companion object {
        public val Default: QueryTuning = QueryTuning()
    }

    override fun toString(): String {
        return "QueryTuning(flexIndex=$flexIndex, maxParallelism=$maxParallelism, scanCap=$scanCap, pipelineBatch=$pipelineBatch, pipelineCap=$pipelineCap)"
    }

    internal fun inject(queryJson: MutableMap<String, Any?>): Unit {
        maxParallelism?.let { queryJson["max_parallelism"] = it.toString() }
        pipelineCap?.let { queryJson["pipeline_cap"] = it.toString() }
        pipelineBatch?.let { queryJson["pipeline_batch"] = it.toString() }
        scanCap?.let { queryJson["scan_cap"] = it.toString() }
        if (flexIndex) queryJson["use_fts"] = true
    }

}
