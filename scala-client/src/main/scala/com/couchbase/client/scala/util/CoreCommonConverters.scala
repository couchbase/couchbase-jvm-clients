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
package com.couchbase.client.scala.util

import com.couchbase.client.core.api.kv._
import com.couchbase.client.core.api.manager.search.CoreSearchIndex
import com.couchbase.client.core.api.query.{
  CoreQueryMetaData,
  CoreQueryMetrics,
  CoreQueryResult,
  CoreQueryStatus,
  CoreReactiveQueryResult
}
import com.couchbase.client.core.api.search.result.{
  CoreDateRangeSearchFacetResult,
  CoreNumericRangeSearchFacetResult,
  CoreSearchFacetResult,
  CoreSearchRowLocations,
  CoreTermSearchFacetResult
}
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.core.msg.kv.{DurabilityLevel, MutationToken}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.service.kv.Observe.{ObservePersistTo, ObserveReplicateTo}
import com.couchbase.client.scala.codec._
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.query.{
  QueryMetaData,
  QueryMetrics,
  QueryResult,
  QueryStatus,
  QueryWarning
}
import com.couchbase.client.scala.search.result.{SearchFacetResult, SearchRowLocations}
import com.couchbase.client.scala.util.DurationConversions._
import reactor.core.publisher.{Flux, Mono}
import reactor.util.annotation.Nullable

import java.util.function.Supplier
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.reflectiveCalls

private[scala] object CoreCommonConverters {
  type HasCommonOptions = {
    val timeout: Duration
    val parentSpan: Option[RequestSpan]
    val retryStrategy: Option[RetryStrategy]
  }

  def convert[T <: HasCommonOptions](options: T): CoreCommonOptions = {
    CoreCommonOptions.of(
      if (options.timeout == Duration.MinusInf) null
      else java.time.Duration.ofNanos(options.timeout.toNanos),
      options.retryStrategy.orNull,
      options.parentSpan.orNull
    )
  }

  def makeCommonOptions(
      timeout: Duration,
      retryStrategy: RetryStrategy = null,
      parentSpan: RequestSpan = null
  ): CoreCommonOptions = {
    CoreCommonOptions.of(
      if (timeout == Duration.MinusInf) null
      else java.time.Duration.ofNanos(timeout.toNanos),
      retryStrategy,
      parentSpan
    )
  }

  def convert(
      in: CoreGetResult,
      env: ClusterEnvironment,
      transcoder: Option[Transcoder]
  ): GetResult = {
    GetResult(
      in.key(),
      Left(in.content()),
      in.flags(),
      in.cas(),
      Option(in.expiry()),
      transcoder.getOrElse(env.transcoder)
    )
  }

  def convertReplica(
      in: CoreGetResult,
      env: ClusterEnvironment,
      transcoder: Option[Transcoder]
  ): GetReplicaResult = {
    new GetReplicaResult(
      in.key(),
      Left(in.content()),
      in.flags(),
      in.cas(),
      Option(in.expiry()),
      in.replica(),
      transcoder.getOrElse(env.transcoder)
    )
  }

  def convertLookupInReplica(
      in: CoreSubdocGetResult,
      env: ClusterEnvironment
  ): LookupInReplicaResult = {
    LookupInReplicaResult(
      in,
      None,
      in.replica()
    )
  }

  def convert(in: CoreMutationResult): MutationResult = {
    MutationResult(
      in.cas(),
      in.mutationToken()
        .asScala
        .map(
          mt =>
            new MutationToken(mt.partitionID, mt.partitionUUID, mt.sequenceNumber, mt.bucketName)
        )
    )
  }

  def convert(in: CoreSubdocMutateResult): MutateInResult = {
    MutateInResult(
      in.key(),
      in,
      in.cas(),
      in.mutationToken()
        .asScala
        .map(
          mt =>
            new MutationToken(mt.partitionID, mt.partitionUUID, mt.sequenceNumber, mt.bucketName)
        )
    )
  }

  def convert(in: CoreExistsResult): ExistsResult = {
    ExistsResult(in.exists(), in.cas())
  }

  def convert(in: CoreCounterResult): CounterResult = {
    CounterResult(in.cas, in.mutationToken.asScala, in.content)
  }

  def convert(in: CoreQueryResult): QueryResult = {
    QueryResult(
      in.collectRows().asScala.toSeq, // toSeq for 2.13
      convert(in.metaData)
    )
  }
  
  def convert(in: CoreQueryMetaData): QueryMetaData = {
    QueryMetaData(
      in.requestId,
      in.clientContextId,
      in.signature.asScala,
      in.metrics.asScala.map(v => convert(v)),
      in.warnings.asScala.map(v => QueryWarning(v.code, v.message)),
      convert(in.status),
      in.profile.asScala
    )
  }

  def convert(in: CoreQueryMetrics): QueryMetrics = {
    QueryMetrics(
      in.elapsedTime,
      in.executionTime,
      in.resultCount,
      in.resultSize,
      in.mutationCount,
      in.sortCount,
      in.errorCount,
      in.warningCount
    )
  }

  def convert(in: CoreQueryStatus): QueryStatus = {
    in match {
      case CoreQueryStatus.RUNNING   => QueryStatus.Running
      case CoreQueryStatus.SUCCESS   => QueryStatus.Success
      case CoreQueryStatus.ERRORS    => QueryStatus.Errors
      case CoreQueryStatus.COMPLETED => QueryStatus.Completed
      case CoreQueryStatus.STOPPED   => QueryStatus.Stopped
      case CoreQueryStatus.TIMEOUT   => QueryStatus.Timeout
      case CoreQueryStatus.CLOSED    => QueryStatus.Closed
      case CoreQueryStatus.FATAL     => QueryStatus.Fatal
      case CoreQueryStatus.ABORTED   => QueryStatus.Aborted
      case CoreQueryStatus.UNKNOWN   => QueryStatus.Unknown
    }
  }

  def convert(in: CoreSearchRowLocations): SearchRowLocations = SearchRowLocations(in)

  def convert(in: CoreSearchFacetResult): SearchFacetResult = {
    in match {
      case v: CoreTermSearchFacetResult      => SearchFacetResult.TermSearchFacetResult(v)
      case v: CoreDateRangeSearchFacetResult => SearchFacetResult.DateRangeSearchFacetResult(v)
      case v: CoreNumericRangeSearchFacetResult =>
        SearchFacetResult.NumericRangeSearchFacetResult(v)
    }
  }

  def convert(in: Map[String, Any]): ujson.Obj = {
    def convertInternal(in: Any): Option[ujson.Value] = {
      in match {
        case x: String                               => Some(ujson.Str(x))
        case x: Int                                  => Some(ujson.Num(x))
        case x: Double                               => Some(ujson.Num(x))
        case x: Boolean                              => Some(ujson.Bool(x))
        case x: java.util.LinkedHashMap[String, Any] => Some(convert(x.asScala.toMap))
        case x: java.util.ArrayList[Any]             =>
          // For some reason ujson.Arr takes an ArrayBuffer
          val ab = new ArrayBuffer[ujson.Value]()
          x.forEach(
            v =>
              convertInternal(v) match {
                case Some(value) => ab += value
                case _           =>
              }
          )
          Some(ujson.Arr(ab))
        case _ =>
          // There's no mechanism for us to return a decoding failure in this path, so any
          // JSON that's not parsed is dropped.
          None
      }
    }

    val out = in.map(x => x._1 -> convertInternal(x._2)) collect {
      case x: (String, Option[ujson.Value]) if x._2.isDefined => x._1 -> x._2.get
    }

    out
  }
  
  def convert[T](in: => CoreAsyncResponse[T])(implicit ec: ExecutionContext): Future[T] = {
    // Argument validation can cause this to throw immediately
    try {
      FutureConversions.javaCFToScalaFutureMappingExceptions(in.toFuture)
    } catch {
      case err: Throwable => Future.failed(err)
    }
  }
  
  def convert(in: Durability): CoreDurability = {
    in match {
      case Durability.Disabled => CoreDurability.NONE
      case Durability.ClientVerified(replicateTo, persistTo) =>
        CoreDurability.of(
          persistTo match {
            case com.couchbase.client.scala.durability.PersistTo.None  => ObservePersistTo.NONE
            case com.couchbase.client.scala.durability.PersistTo.One   => ObservePersistTo.ONE
            case com.couchbase.client.scala.durability.PersistTo.Two   => ObservePersistTo.TWO
            case com.couchbase.client.scala.durability.PersistTo.Three => ObservePersistTo.THREE
          },
          replicateTo match {
            case com.couchbase.client.scala.durability.ReplicateTo.None  => ObserveReplicateTo.NONE
            case com.couchbase.client.scala.durability.ReplicateTo.One   => ObserveReplicateTo.ONE
            case com.couchbase.client.scala.durability.ReplicateTo.Two   => ObserveReplicateTo.TWO
            case com.couchbase.client.scala.durability.ReplicateTo.Three => ObserveReplicateTo.THREE
          }
        )
      case Durability.Majority => CoreDurability.of(DurabilityLevel.MAJORITY)
      case Durability.MajorityAndPersistToActive =>
        CoreDurability.of(DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE)
      case Durability.PersistToMajority => CoreDurability.of(DurabilityLevel.PERSIST_TO_MAJORITY)
    }
  }

  /** @Nullable is not very Scala, but is required for backwards compatibility in e.g. IncrementOptions
    */
  def convertExpiry(@Nullable in: Duration): CoreExpiry = {
    if (in == null) CoreExpiry.NONE
    else CoreExpiry.of(in)
  }

  def convert(in: Duration): java.time.Duration = {
    java.time.Duration.ofMillis(in.toMillis)
  }

  def convert(in: StoreSemantics): CoreStoreSemantics = {
    in match {
      case StoreSemantics.Replace => CoreStoreSemantics.REPLACE
      case StoreSemantics.Insert  => CoreStoreSemantics.INSERT
      case StoreSemantics.Upsert  => CoreStoreSemantics.UPSERT
    }
  }

  def encoder[T](
      transcoder: Transcoder,
      serializer: JsonSerializer[T],
      content: T
  ): Supplier[CoreEncodedContent] = { () =>
    {
      val value: EncodedValue = (transcoder match {
        case x: TranscoderWithSerializer    => x.encode(content, serializer)
        case x: TranscoderWithoutSerializer => x.encode(content)
      }).get

      new CoreEncodedContent {
        override def encoded(): Array[Byte] = value.encoded

        override def flags(): Int = value.flags
      }
    }
  }
}
