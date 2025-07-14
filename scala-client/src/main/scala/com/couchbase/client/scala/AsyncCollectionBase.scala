/*
 * Copyright (c) 2025 Couchbase, Inc.
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
package com.couchbase.client.scala

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.core.api.kv.{
  CoreExpiry,
  CoreReadPreference,
  CoreSubdocGetCommand,
  CoreSubdocGetResult
}
import com.couchbase.client.core.api.shared.CoreMutationState
import com.couchbase.client.core.cnc.{RequestSpan, RequestTracer}
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.kv._
import com.couchbase.client.core.msg.Response
import com.couchbase.client.core.msg.kv._
import com.couchbase.client.core.protostellar.CoreProtostellarUtil
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.service.kv.{Observe, ObserveContext}
import com.couchbase.client.core.{Core, CoreKeyspace}
import com.couchbase.client.scala.codec._
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.durability._
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.util.CoreCommonConverters._
import com.couchbase.client.scala.util.{ExpiryUtil, FutureConversions, TimeoutUtil}
import reactor.core.publisher.Flux

import java.lang
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.Optional
import scala.compat.java8.FutureConverters
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}
import scala.concurrent.Await

private[scala] case class HandlerParams(
    core: Core,
    bucketName: String,
    collectionIdentifier: CollectionIdentifier,
    env: ClusterEnvironment
) {
  def tracer: RequestTracer = core.coreResources.requestTracer
}

private[scala] case class HandlerBasicParams(core: Core) {
  def tracer: RequestTracer = core.context.coreResources.requestTracer
}

/** Provides asynchronous access to all collection APIs, based around Scala `Future`s.  This is the main entry-point
  * for key-value (KV) operations.
  *
  * <p>If synchronous, blocking access is needed, we recommend looking at the [[Collection]].  If a more advanced
  * async API based around reactive programming is desired, then check out the [[ReactiveCollection]].
  *
  * @author Graham Pople
  * @since 1.0.0
  * @define Same             This asynchronous version performs the same functionality and takes the same parameters,
  *                          but returns the same result object asynchronously in a `Future`.
  */
trait AsyncCollectionBase { this: AsyncCollection =>

  private[scala] implicit val ec: ExecutionContext = environment.ec

  import com.couchbase.client.scala.util.DurationConversions._

  private[scala] val kvTimeout: Durability => Duration = TimeoutUtil.kvTimeout(environment)
  private[scala] val kvReadTimeout: Duration           = environment.timeoutConfig.kvTimeout()
  private[scala] val collectionIdentifier              =
    new CollectionIdentifier(bucketName, Optional.of(scopeName), Optional.of(name))
  private[scala] val keyspace = CoreKeyspace.from(collectionIdentifier)
  private[scala] val kvOps    = couchbaseOps.kvOps(keyspace)

  // Would remove, but has been part of public AsyncCollection API
  def core: Core = couchbaseOps match {
    case core: Core => core
    case _          => throw CoreProtostellarUtil.unsupportedCurrentlyInProtostellar()
  }

  val binary = new AsyncBinaryCollection(this)

  protected[scala] def getSubDoc(
      id: String,
      spec: collection.Seq[LookupInSpec],
      withExpiry: Boolean,
      timeout: Duration,
      retryStrategy: RetryStrategy,
      transcoder: Transcoder,
      parentSpan: Option[RequestSpan],
      accessDeleted: Option[Boolean]
  ): Future[LookupInResult] = {
    var commands = LookupInSpec.map(spec)
    if (withExpiry) {
      commands = commands :+ new CoreSubdocGetCommand(
        SubdocCommandType.GET,
        "$document.exptime",
        true
      )
    }

    convert(
      kvOps
        .subdocGetAsync(
          makeCommonOptions(timeout, retryStrategy, parentSpan.orNull),
          id,
          commands.asJava,
          accessDeleted.getOrElse(false)
        )
    ).flatMap(result => {
      val (fieldsWithoutExp, expTime) = if (withExpiry) {
        val fields        = result.fields().asScala
        val expField      = fields.last
        val expTime       = new String(expField.value(), StandardCharsets.UTF_8)
        val fieldsWithout = fields.dropRight(1).asJava
        (fieldsWithout, Some(Instant.ofEpochSecond(expTime.toLong)))
      } else {
        (result.fields(), None)
      }

      val modified = new CoreSubdocGetResult(
        result.keyspace,
        result.key,
        result.meta,
        fieldsWithoutExp,
        result.cas,
        result.tombstone
      )

      Future.successful(LookupInResult(modified, expTime, transcoder))
    })
  }

  private[scala] def scanRequest(
      scanType: ScanType,
      opts: ScanOptions
  ): Flux[ScanResult] = {

    val timeoutActual: java.time.Duration =
      if (opts.timeout == Duration.MinusInf) environment.timeoutConfig.kvScanTimeout()
      else opts.timeout

    val consistencyTokens = new java.util.LinkedList[MutationToken]()

    opts.consistentWith match {
      case Some(cw) => cw.tokens.foreach(t => consistencyTokens.add(t))
      case _        =>
    }

    val _idsOnly = opts.idsOnly.getOrElse(false)

    val options = new CoreScanOptions() {
      override def commonOptions(): CoreCommonOptions =
        CoreCommonOptions.of(
          timeoutActual,
          opts.retryStrategy.getOrElse(null),
          opts.parentSpan.getOrElse(null)
        )

      override def idsOnly(): Boolean = _idsOnly

      override def consistentWith(): CoreMutationState =
        new CoreMutationState(consistencyTokens)

      override def batchItemLimit(): Int =
        opts.batchItemLimit
          .getOrElse(RangeScanOrchestrator.RANGE_SCAN_DEFAULT_BATCH_ITEM_LIMIT)

      override def batchByteLimit(): Int =
        opts.batchByteLimit
          .getOrElse(RangeScanOrchestrator.RANGE_SCAN_DEFAULT_BATCH_BYTE_LIMIT)
    }

    val rangeScan = scanType match {
      case scan: ScanType.RangeScan =>
        new CoreRangeScan() {
          override def from(): CoreScanTerm = scan.from.map(_.toCore).getOrElse(CoreScanTerm.MIN)

          override def to(): CoreScanTerm = scan.to.map(_.toCore).getOrElse(CoreScanTerm.MAX)
        }

      case scan: ScanType.PrefixScan =>
        CoreRangeScan.forPrefix(scan.prefix)

      case scan: ScanType.SamplingScan =>
        new CoreSamplingScan {
          override def limit(): Long = scan.limit

          override def seed(): Optional[lang.Long] = Optional.ofNullable(scan.seed)
        }
    }

    Flux
      .from(kvOps.scanRequestReactive(rangeScan, options))
      .map(item => ScanResult(item, opts.transcoder.getOrElse(environment.transcoder)))
  }
}

object AsyncCollectionBase {
  private[scala] val EmptyList = new java.util.ArrayList[String]()
}
