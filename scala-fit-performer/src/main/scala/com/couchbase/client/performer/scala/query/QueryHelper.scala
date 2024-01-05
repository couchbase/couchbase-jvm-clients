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
package com.couchbase.client.performer.scala.query

import com.couchbase.client.performer.scala.ScalaSdkCommandExecutor.{convertDuration, convertMutationState, setSuccess}
import com.couchbase.client.performer.scala.util.SerializableValidation.assertIsSerializable
import com.couchbase.client.performer.scala.util.{ClusterConnection, ContentAsUtil}
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.sdk.{ClusterLevelCommand, Command, ScopeLevelCommand}
import com.couchbase.client.protocol.shared.ScanConsistency
import com.couchbase.client.scala.Scope
import com.couchbase.client.scala.json.{JsonArray, JsonObject}
import com.couchbase.client.scala.query.{QueryParameters, QueryProfile, QueryScanConsistency}
import com.google.protobuf.ByteString

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

object QueryHelper {
    def handleClusterQuery(
                                  connection: ClusterConnection,
                                  command: Command,
                                  clc: ClusterLevelCommand
                          ): Result.Builder = {
        val req = clc.getQuery
        val options = createOptions(req.hasOptions, req.getOptions)
        val cluster = connection.cluster
        val out = Result.newBuilder

        val start = System.nanoTime

        val result = options match {
            case Some(opts) => cluster.query(req.getStatement, opts)
            case None => cluster.query(req.getStatement)
        }

        out.setElapsedNanos(System.nanoTime - start)

        if (command.getReturnResult) populateResult(req, out, result.get)
        else setSuccess(out)

        out
    }

    def handleScopeQuery(
                                scope: Scope,
                                command: Command,
                                slc: ScopeLevelCommand
                        ): Result.Builder = {
        // [start:<1.0.9]
        /*
            throw new UnsupportedOperationException(
              "This version of the Scala SDK does not support scoped queries"
            )
            // [end:<1.0.9]
        */

        // [start:1.0.9]
        val req = slc.getQuery
        val options = createOptions(req.hasOptions, req.getOptions)
        val out = Result.newBuilder

        val start = System.nanoTime

        val result = options match {
            case Some(opts) => scope.query(req.getStatement, opts)
            case None => scope.query(req.getStatement)
        }

        out.setElapsedNanos(System.nanoTime - start)

        if (command.getReturnResult) populateResult(req, out, result.get)
        else setSuccess(out)

        out
        // [end:1.0.9]
    }

    private def createOptions(
                                     hasOptions: Boolean,
                                     opts: com.couchbase.client.protocol.sdk.query.QueryOptions
                             ): Option[com.couchbase.client.scala.query.QueryOptions] = {
        if (hasOptions) {
            var out = com.couchbase.client.scala.query.QueryOptions()
            if (opts.hasScanConsistency) {
                if (opts.getScanConsistency == ScanConsistency.NOT_BOUNDED) {
                    out = out.scanConsistency(QueryScanConsistency.NotBounded)
                } else if (opts.getScanConsistency == ScanConsistency.REQUEST_PLUS) {
                    out = out.scanConsistency(QueryScanConsistency.RequestPlus(if (opts.hasScanWaitMillis) Some(Duration(opts.getScanWaitMillis, TimeUnit.MILLISECONDS)) else None))
                } else throw new UnsupportedOperationException(s"Unknown scan consistency in ${opts}")
            }
            if (opts.hasConsistentWith) {
                out = out.scanConsistency(
                    QueryScanConsistency.ConsistentWith(convertMutationState(opts.getConsistentWith))
                )
            }
            if (opts.getRawCount > 0) {
                out = out.raw(opts.getRawMap.asScala.map(x => x._1 -> x._2.asInstanceOf[Any]).toMap)
            }
            if (opts.hasAdhoc) {
                out = out.adhoc(opts.getAdhoc)
            }
            if (opts.hasProfile) {
                out = out.profile(opts.getProfile.toLowerCase match {
                    case "off" => QueryProfile.Off
                    case "phases" => QueryProfile.Phases
                    case "timings" => QueryProfile.Timings
                    case x => throw new UnsupportedOperationException(s"Unknown profile ${x}")
                })
            }
            if (opts.hasReadonly) {
                out = out.readonly(opts.getReadonly)
            }
            if (opts.getParametersPositionalCount > 0) {
                out = out.parameters(QueryParameters.Positional(opts.getParametersPositionalList.asScala.toList: _*))
            }
            if (opts.getParametersNamedCount > 0) {
                out = out.parameters(QueryParameters.Named(opts.getParametersNamedMap.asScala.toMap))
            }
            if (opts.getParametersNamedCount > 0 && opts.getParametersPositionalCount > 0) {
                throw new UnsupportedOperationException(
                    "SDK cannot support both named and positional params"
                )
            }
            // [start:1.0.9]
            if (opts.hasFlexIndex) {
                out = out.flexIndex(opts.getFlexIndex)
            }
            // [end:1.0.9]
            if (opts.hasPipelineCap) {
                out = out.pipelineCap(opts.getPipelineCap)
            }
            if (opts.hasPipelineBatch) {
                out = out.pipelineBatch(opts.getPipelineBatch)
            }
            if (opts.hasScanCap) {
                out = out.scanCap(opts.getScanCap)
            }
            if (opts.hasTimeoutMillis) {
                out = out.timeout(Duration(opts.getTimeoutMillis, TimeUnit.MILLISECONDS))
            }
            if (opts.hasMaxParallelism) {
                out = out.maxParallelism(opts.getMaxParallelism)
            }
            if (opts.hasMetrics) {
                out = out.metrics(opts.getMetrics)
            }
            if (opts.hasSingleQueryTransactionOptions) {
                throw new UnsupportedOperationException("SDK does not support transactions")
            }
            if (opts.hasParentSpanId) {
                throw new UnsupportedOperationException("Performer does not yet support OBSERVABILITY_1")
            }
            if (opts.hasUseReplica) {
                // [start:1.4.9]
                out = out.useReplica(opts.getUseReplica)
                // [end:1.4.9]
            }
            if (opts.hasClientContextId) {
                out = out.clientContextId(opts.getClientContextId)
            }
            if (opts.hasPreserveExpiry) {
                // [start:1.2.5]
                out = out.preserveExpiry(opts.getPreserveExpiry)
                // [end:1.2.5]
            }
            assertIsSerializable(out)
            Some(out)
        } else None
    }

    private def populateResult(
                                      request: com.couchbase.client.protocol.sdk.query.Command,
                                      out: com.couchbase.client.protocol.run.Result.Builder,
                                      result: com.couchbase.client.scala.query.QueryResult
                              ): Unit = {
        // Skipping assertIsSerializable: QueryResult is not serializable but the Spark Connector does not return it directly.
        val content = ContentAsUtil
                .contentTypeSeq(
                    request.getContentAs,
                    () => result.rowsAs[Array[Byte]],
                    () => result.rowsAs[String],
                    () => result.rowsAs[JsonObject],
                    () => result.rowsAs[JsonArray],
                    () => result.rowsAs[Boolean],
                    () => result.rowsAs[Int],
                    () => result.rowsAs[Double]
                )
                .get

        val md = result.metaData

        val metaData = com.couchbase.client.protocol.sdk.query.QueryMetaData.newBuilder
                .setRequestId(md.requestId)
                .setClientContextId(md.clientContextId)
                .setStatus(
                    com.couchbase.client.protocol.sdk.query.QueryStatus.valueOf(md.status.toString.toUpperCase)
                )
                .addAllWarnings(
                    md.warnings
                            .map(
                                warning =>
                                    com.couchbase.client.protocol.sdk.query.QueryWarning.newBuilder
                                            .setCode(warning.code)
                                            .setMessage(warning.message)
                                            .build
                            )
                            .asJava
                )

        md.signatureAs[JsonObject] match {
            case Failure(_) =>
            case Success(value) => metaData.setSignature(ByteString.copyFrom(value.toString.getBytes))
        }

        md.profileAs[JsonObject] match {
            case Failure(_) =>
            case Success(value) => metaData.setProfile(ByteString.copyFrom(value.toString.getBytes))
        }

        md.metrics match {
            case Some(metrics) =>
                metaData.setMetrics(
                    com.couchbase.client.protocol.sdk.query.QueryMetrics
                            .newBuilder()
                            .setSortCount(metrics.sortCount)
                            .setResultCount(metrics.resultCount)
                            .setResultSize(metrics.resultSize)
                            .setMutationCount(metrics.mutationCount)
                            .setErrorCount(metrics.errorCount)
                            .setWarningCount(metrics.warningCount)
                            .setElapsedTime(convertDuration(metrics.elapsedTime))
                            .setExecutionTime(convertDuration(metrics.executionTime))
                )
            case None =>
        }

        out.setSdk(
            com.couchbase.client.protocol.sdk.Result.newBuilder
                    .setQueryResult(
                        com.couchbase.client.protocol.sdk.query.QueryResult.newBuilder
                                .addAllContent(content.asJava)
                                .setMetaData(metaData)
                    )
        )
    }
}
