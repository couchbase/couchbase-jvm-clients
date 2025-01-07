/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.performer.kotlin.query

import com.couchbase.client.kotlin.Scope
import com.couchbase.client.kotlin.query.QueryParameters
import com.couchbase.client.kotlin.query.QueryProfile
import com.couchbase.client.kotlin.query.QueryScanConsistency
import com.couchbase.client.kotlin.query.execute
import com.couchbase.client.performer.kotlin.util.ClusterConnection
import com.couchbase.client.performer.kotlin.util.ConverterUtil.Companion.createCommon
import com.couchbase.client.performer.kotlin.util.convert
import com.couchbase.client.performer.kotlin.util.setSuccess
import com.couchbase.client.performer.kotlin.util.toKotlin
import com.couchbase.client.performer.kotlin.util.toProtobufDuration
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.sdk.ClusterLevelCommand
import com.couchbase.client.protocol.sdk.Command
import com.couchbase.client.protocol.sdk.ScopeLevelCommand
import com.couchbase.client.protocol.sdk.query.QueryOptions
import com.couchbase.client.protocol.shared.ScanConsistency
import com.google.protobuf.ByteString
import kotlinx.coroutines.runBlocking
import java.util.Locale
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds

class QueryHelper {
    companion object {
        fun handleClusterQuery(
            connection: ClusterConnection,
            command: Command,
            clc: ClusterLevelCommand
        ): Result.Builder {
            val req = clc.getQuery()
            val cluster = connection.cluster
            val out = Result.newBuilder()

            val start = System.nanoTime()

            val result = runBlocking {
                if (req.hasOptions()) {
                    val opts = req.options
                    cluster.query(
                        req.statement,
                        common = createCommon(opts.hasTimeoutMillis(), opts.timeoutMillis.toInt()),
                        parameters = queryParameters(opts),
                        preserveExpiry = if (opts.hasPreserveExpiry()) opts.preserveExpiry else false,
                        serializer = null,
                        consistency = queryScanConsistency(opts),
                        readonly = if (opts.hasReadonly()) opts.readonly else false,
                        adhoc = if (opts.hasAdhoc()) opts.adhoc else true,
                        flexIndex = if (opts.hasFlexIndex()) opts.flexIndex else false,
                        metrics = if (opts.hasMetrics()) opts.metrics else false,
                        profile = queryProfile(opts),
                        maxParallelism = if (opts.hasMaxParallelism()) opts.maxParallelism else null,
                        scanCap = if (opts.hasScanCap()) opts.scanCap else null,
                        pipelineBatch = if (opts.hasPipelineBatch()) opts.pipelineBatch else null,
                        pipelineCap = if (opts.hasPipelineCap()) opts.pipelineCap else null,
                        clientContextId = if (opts.hasClientContextId()) opts.clientContextId else UUID.randomUUID().toString(),
                        raw = if (opts.rawCount > 0) opts.rawMap else emptyMap(),
                        // [if:1.1.9]
                        useReplica = if (opts.hasUseReplica()) opts.useReplica else null
                        // [end]
                    ).execute()
                } else {
                    cluster.query(req.statement).execute()
                }
            }

            out.setElapsedNanos(System.nanoTime() - start)

            if (command.getReturnResult()) populateResult(req, out, result)
            else out.setSuccess()

            return out
        }

        private fun queryParameters(opts: QueryOptions) = if (opts.parametersNamedCount > 0) {
            QueryParameters.named(opts.parametersNamedMap)
        } else if (opts.parametersPositionalCount > 0) {
            QueryParameters.positional(opts.parametersPositionalList)
        } else QueryParameters.None

        private fun queryProfile(opts: QueryOptions) = if (opts.hasProfile()) when (opts.profile) {
            "off" -> QueryProfile.OFF
            "phases" -> QueryProfile.PHASES
            "timings" -> QueryProfile.TIMINGS
            else -> throw UnsupportedOperationException("Unknown query profile " + opts.profile)
        }
        else QueryProfile.OFF

        private fun queryScanConsistency(opts: QueryOptions) = if (opts.hasScanConsistency()) {
            if (opts.scanConsistency == ScanConsistency.NOT_BOUNDED) QueryScanConsistency.notBounded()
            else if (opts.scanConsistency == ScanConsistency.REQUEST_PLUS) QueryScanConsistency.requestPlus(
                if (opts.hasScanWaitMillis()) opts.scanWaitMillis.milliseconds else null
            )
            else throw UnsupportedOperationException("Unknown scan consistency")
        } else if (opts.hasConsistentWith()) {
            QueryScanConsistency.consistentWith(
                opts.consistentWith.toKotlin(),
                if (opts.hasScanWaitMillis()) opts.scanWaitMillis.milliseconds else null
            )
        } else QueryScanConsistency.notBounded()

        fun handleScopeQuery(
            scope: Scope,
            command: Command,
            slc: ScopeLevelCommand
        ): Result.Builder {
            val req = slc.getQuery()
            val out = Result.newBuilder()

            val start = System.nanoTime()

            val result = runBlocking {
                if (req.hasOptions()) {
                    val opts = req.options
                    scope.query(
                        req.statement,
                        common = createCommon(opts.hasTimeoutMillis(), opts.timeoutMillis.toInt()),
                        parameters = queryParameters(opts),
                        preserveExpiry = if (opts.hasPreserveExpiry()) opts.preserveExpiry else false,
                        serializer = null,
                        consistency = queryScanConsistency(opts),
                        readonly = if (opts.hasReadonly()) opts.readonly else false,
                        adhoc = if (opts.hasAdhoc()) opts.adhoc else true,
                        flexIndex = if (opts.hasFlexIndex()) opts.flexIndex else false,
                        metrics = if (opts.hasMetrics()) opts.metrics else false,
                        profile = queryProfile(opts),
                        maxParallelism = if (opts.hasMaxParallelism()) opts.maxParallelism else null,
                        scanCap = if (opts.hasScanCap()) opts.scanCap else null,
                        pipelineBatch = if (opts.hasPipelineBatch()) opts.pipelineBatch else null,
                        pipelineCap = if (opts.hasPipelineCap()) opts.pipelineCap else null,
                        clientContextId = if (opts.hasClientContextId()) opts.clientContextId else UUID.randomUUID().toString(),
                        raw = if (opts.rawCount > 0) opts.rawMap else emptyMap(),
                        // [if:1.1.9]
                        useReplica = if (opts.hasUseReplica()) opts.useReplica else null,
                        // [end]
                    ).execute()
                } else {
                    scope.query(req.statement).execute()
                }
            }

            out.setElapsedNanos(System.nanoTime() - start)

            if (command.getReturnResult()) populateResult(req, out, result)
            else out.setSuccess()

            return out
        }

        private fun populateResult(
            request: com.couchbase.client.protocol.sdk.query.Command,
            out: com.couchbase.client.protocol.run.Result.Builder,
            result: com.couchbase.client.kotlin.query.QueryResult
        ): Unit {
            val content = result.rows.map { row -> request.contentAs.convert(row) }
            val md = result.metadata

            val metaData = com.couchbase.client.protocol.sdk.query.QueryMetaData.newBuilder()
                .setRequestId(md.requestId)
                .setClientContextId(md.clientContextId)
                .setStatus(
                    com.couchbase.client.protocol.sdk.query.QueryStatus.valueOf(md.status.toString().uppercase(Locale.getDefault()))
                )
                .addAllWarnings(md.warnings.map {
                    com.couchbase.client.protocol.sdk.query.QueryWarning.newBuilder()
                        .setCode(it.code)
                        .setMessage(it.message)
                        .build()
                })

            md.signatureBytes?.let { metaData.signature = ByteString.copyFrom(it) }
            md.profileBytes?.let { metaData.profile = ByteString.copyFrom(it) }

            md.metrics?.let { metrics ->
                metaData.setMetrics(
                    com.couchbase.client.protocol.sdk.query.QueryMetrics
                        .newBuilder()
                        .setSortCount(metrics.sortCount)
                        .setResultCount(metrics.resultCount)
                        .setResultSize(metrics.resultSize)
                        .setMutationCount(metrics.mutationCount)
                        .setErrorCount(metrics.errorCount)
                        .setWarningCount(metrics.warningCount)
                        .setElapsedTime(metrics.elapsedTime.toProtobufDuration())
                        .setExecutionTime(metrics.executionTime.toProtobufDuration())
                )
            }

            out.setSdk(
                com.couchbase.client.protocol.sdk.Result.newBuilder()
                    .setQueryResult(
                        com.couchbase.client.protocol.sdk.query.QueryResult.newBuilder()
                            .addAllContent(content)
                            .setMetaData(metaData)
                    )
            )
        }
    }
}
