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
// [skip:<1.5.0]

package com.couchbase.client.performer.scala.kv

import com.couchbase.client.performer.core.perf.PerRun
import com.couchbase.client.performer.core.util.TimeUtil.getTimeNow
import com.couchbase.client.performer.scala.Content.{ContentByteArray, ContentJson, ContentNull, ContentString}
import com.couchbase.client.performer.scala.ScalaSdkCommandExecutor.{convertContent, convertDurability, convertExpiry, setSuccess}
import com.couchbase.client.performer.scala.util.{ClusterConnection, ContentAsUtil}
import com.couchbase.client.performer.scala.{Content, ScalaSdkCommandExecutor}
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.sdk.collection.mutatein.ContentOrMacro
import com.couchbase.client.protocol.sdk.{CollectionLevelCommand, Command}
import com.couchbase.client.protocol.shared.DocLocation
import com.couchbase.client.scala.json.{JsonArray, JsonObject}
import com.couchbase.client.scala.kv.MutateInSpec._
import com.couchbase.client.scala.kv._
import reactor.core.scala.publisher.SMono

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

sealed trait IsContentOrMacro
object IsContentOrMacro {
  case class IsContent(value: Content)     extends IsContentOrMacro
  case class IsMacro(value: MutateInMacro) extends IsContentOrMacro
}

object MutateInHelper {
  def handleMutateIn(
      perRun: PerRun,
      connection: ClusterConnection,
      command: com.couchbase.client.protocol.sdk.Command,
      docId: (DocLocation) => String
  ): com.couchbase.client.protocol.run.Result.Builder = {
    val clc = command.getCollectionCommand
    val out = com.couchbase.client.protocol.run.Result.newBuilder()
    out.setInitiated(getTimeNow)
    handleMutateIn(connection, command, docId, clc, out)
    out
  }

  def handleMutateInReactive(
      perRun: PerRun,
      connection: ClusterConnection,
      command: com.couchbase.client.protocol.sdk.Command,
      docId: (DocLocation) => String
  ): SMono[com.couchbase.client.protocol.run.Result.Builder] = {
    val clc = command.getCollectionCommand
    val out = com.couchbase.client.protocol.run.Result.newBuilder()
    out.setInitiated(getTimeNow)

    handleMutateInReactive(connection, command, docId, clc, out)
      .map(_ => out)
  }

  private def handleMutateIn(
      connection: ClusterConnection,
      command: Command,
      getId: (DocLocation) => String,
      clc: CollectionLevelCommand,
      out: Result.Builder
  ): Unit = {
    val req        = clc.getMutateIn
    val collection = connection.collection(req.getLocation)
    val options    = createOptions(req)
    val docId      = getId(req.getLocation)

    val specs = mapSpecs(req.getSpecList.asScala)

    val start = System.nanoTime

    val result = options match {
      case Some(opts) => collection.mutateIn(docId, specs, opts)
      case None       => collection.mutateIn(docId, specs)
    }

    out.setElapsedNanos(System.nanoTime - start)

    if (command.getReturnResult) populateResult(req, out, result.get)
    else setSuccess(out)
  }

  private def handleMutateInReactive(
      connection: ClusterConnection,
      command: Command,
      getId: (DocLocation) => String,
      clc: CollectionLevelCommand,
      out: Result.Builder
  ): SMono[Unit] = {
    val req        = clc.getMutateIn
    val collection = connection.collection(req.getLocation).reactive
    val options    = createOptions(req)
    val docId      = getId(req.getLocation)

    val specs = mapSpecs(req.getSpecList.asScala)

    val start = System.nanoTime

    (options match {
      case Some(opts) => collection.mutateIn(docId, specs, opts)
      case None       => collection.mutateIn(docId, specs)
    }).map(result => {
      out.setElapsedNanos(System.nanoTime - start)

      if (command.getReturnResult) populateResult(req, out, result)
      else setSuccess(out)
    })
  }

  private def mapSpecs(
      specList: Seq[com.couchbase.client.protocol.sdk.collection.mutatein.MutateInSpec]
  ): Seq[com.couchbase.client.scala.kv.MutateInSpec] = {
    val specs = specList.map(v => {
      if (v.hasUpsert) {
        val s    = v.getUpsert
        val path = s.getPath
        var spec = isContentOrMacro(s.getContent) match {
          case IsContentOrMacro.IsMacro(mac)                       => upsert(path, mac)
          case IsContentOrMacro.IsContent(ContentString(value))    => upsert(path, value)
          case IsContentOrMacro.IsContent(ContentJson(value))      => upsert(path, value)
          case IsContentOrMacro.IsContent(ContentByteArray(value)) => upsert(path, value)
          case IsContentOrMacro.IsContent(ContentNull(value))      => upsert(path, value)
        }
        if (s.hasXattr) spec = spec.xattr
        if (s.hasCreatePath) spec = spec.createPath
        spec
      } else if (v.hasInsert) {
        val s    = v.getInsert
        val path = s.getPath
        var spec = isContentOrMacro(s.getContent) match {
          case IsContentOrMacro.IsMacro(mac)                       => insert(path, mac)
          case IsContentOrMacro.IsContent(ContentString(value))    => insert(path, value)
          case IsContentOrMacro.IsContent(ContentJson(value))      => insert(path, value)
          case IsContentOrMacro.IsContent(ContentByteArray(value)) => insert(path, value)
          case IsContentOrMacro.IsContent(ContentNull(value))      => insert(path, value)
        }
        if (s.hasXattr) spec = spec.xattr
        if (s.hasCreatePath) spec = spec.createPath
        spec
      } else if (v.hasReplace) {
        val s    = v.getReplace
        val path = s.getPath
        var spec = isContentOrMacro(s.getContent) match {
          case IsContentOrMacro.IsMacro(mac)                       => replace(path, mac)
          case IsContentOrMacro.IsContent(ContentString(value))    => replace(path, value)
          case IsContentOrMacro.IsContent(ContentJson(value))      => replace(path, value)
          case IsContentOrMacro.IsContent(ContentByteArray(value)) => replace(path, value)
          case IsContentOrMacro.IsContent(ContentNull(value))      => replace(path, value)
        }
        if (s.hasXattr) spec = spec.xattr
        spec
      } else if (v.hasRemove) {
        val s    = v.getRemove
        val path = s.getPath
        var spec = remove(path)
        if (s.hasXattr) spec = spec.xattr
        spec
      } else if (v.hasArrayAppend) {
        val s       = v.getArrayAppend
        val path    = s.getPath
        val content = s.getContentList.asScala
        // We use the type of the first element and assume it's homogenous.
        var spec = isContentOrMacro(s.getContent(0)) match {
          case IsContentOrMacro.IsMacro(_) => arrayAppend(path, mapContent[MutateInMacro](content))
          case IsContentOrMacro.IsContent(ContentString(_)) =>
            arrayAppend(path, mapContent[String](content))
          case IsContentOrMacro.IsContent(ContentJson(_)) =>
            arrayAppend(path, mapContent[JsonObject](content))
          case IsContentOrMacro.IsContent(ContentByteArray(_)) =>
            arrayAppend(path, mapContent[Array[Byte]](content))
          case IsContentOrMacro.IsContent(ContentNull(_)) =>
            arrayAppend(path, mapContent[String](content))
        }
        if (s.hasXattr) spec = spec.xattr
        if (s.hasCreatePath) spec = spec.createPath
        spec
      } else if (v.hasArrayPrepend) {
        val s       = v.getArrayPrepend
        val path    = s.getPath
        val content = s.getContentList.asScala
        var spec = isContentOrMacro(s.getContent(0)) match {
          case IsContentOrMacro.IsMacro(_) => arrayPrepend(path, mapContent[MutateInMacro](content))
          case IsContentOrMacro.IsContent(ContentString(_)) =>
            arrayPrepend(path, mapContent[String](content))
          case IsContentOrMacro.IsContent(ContentJson(_)) =>
            arrayPrepend(path, mapContent[JsonObject](content))
          case IsContentOrMacro.IsContent(ContentByteArray(_)) =>
            arrayPrepend(path, mapContent[Array[Byte]](content))
          case IsContentOrMacro.IsContent(ContentNull(_)) =>
            arrayPrepend(path, mapContent[String](content))
        }
        if (s.hasXattr) spec = spec.xattr
        if (s.hasCreatePath) spec = spec.createPath
        spec
      } else if (v.hasArrayInsert) {
        val s       = v.getArrayInsert
        val path    = s.getPath
        val content = s.getContentList.asScala
        var spec = isContentOrMacro(s.getContent(0)) match {
          case IsContentOrMacro.IsMacro(_) => arrayInsert(path, mapContent[MutateInMacro](content))
          case IsContentOrMacro.IsContent(ContentString(_)) =>
            arrayInsert(path, mapContent[String](content))
          case IsContentOrMacro.IsContent(ContentJson(_)) =>
            arrayInsert(path, mapContent[JsonObject](content))
          case IsContentOrMacro.IsContent(ContentByteArray(_)) =>
            arrayInsert(path, mapContent[Array[Byte]](content))
          case IsContentOrMacro.IsContent(ContentNull(_)) =>
            arrayInsert(path, mapContent[String](content))
        }
        if (s.hasXattr) spec = spec.xattr
        if (s.hasCreatePath) spec = spec.createPath
        spec
      } else if (v.hasArrayAddUnique) {
        val s    = v.getArrayAddUnique
        val path = s.getPath
        var spec = isContentOrMacro(s.getContent) match {
          case IsContentOrMacro.IsMacro(mac)                       => arrayAddUnique(path, mac)
          case IsContentOrMacro.IsContent(ContentString(value))    => arrayAddUnique(path, value)
          case IsContentOrMacro.IsContent(ContentJson(value))      => arrayAddUnique(path, value)
          case IsContentOrMacro.IsContent(ContentByteArray(value)) => arrayAddUnique(path, value)
          case IsContentOrMacro.IsContent(ContentNull(value))      => arrayAddUnique(path, value)
        }
        if (s.hasXattr) spec = spec.xattr
        if (s.hasCreatePath) spec = spec.createPath
        spec
      } else if (v.hasIncrement) {
        val s    = v.getIncrement
        val path = s.getPath
        var spec = increment(path, s.getDelta)
        if (s.hasXattr) spec = spec.xattr
        if (s.hasCreatePath) spec = spec.createPath
        spec
      } else if (v.hasDecrement) {
        val s    = v.getDecrement
        val path = s.getPath
        var spec = decrement(path, s.getDelta)
        if (s.hasXattr) spec = spec.xattr
        if (s.hasCreatePath) spec = spec.createPath
        spec
      } else throw new UnsupportedOperationException("Unknown mutateIn spec")
    })
    specs
  }

  private def isContentOrMacro(
      input: com.couchbase.client.protocol.sdk.collection.mutatein.ContentOrMacro
  ): IsContentOrMacro = {
    if (input.hasMacro) {
      IsContentOrMacro.IsMacro(input.getMacro match {
        case com.couchbase.client.protocol.sdk.collection.mutatein.MutateInMacro.CAS =>
          MutateInMacro.CAS
        case com.couchbase.client.protocol.sdk.collection.mutatein.MutateInMacro.SEQ_NO =>
          MutateInMacro.SeqNo
        case com.couchbase.client.protocol.sdk.collection.mutatein.MutateInMacro.VALUE_CRC_32C =>
          MutateInMacro.ValueCrc32c
      })
    } else if (input.hasContent) {
      IsContentOrMacro.IsContent(convertContent(input.getContent))
    } else throw new UnsupportedOperationException("Unknown isContentOrMacro")

  }

  private def createOptions(
      request: com.couchbase.client.protocol.sdk.collection.mutatein.MutateIn
  ) = {
    if (request.hasOptions) {
      val opts = request.getOptions
      var out  = MutateInOptions()
      if (opts.hasTimeoutMillis)
        out = out.timeout(Duration.create(opts.getTimeoutMillis, TimeUnit.MILLISECONDS))
      if (opts.hasAccessDeleted) out = out.accessDeleted(true)
      // [start:1.5.0]
      if (opts.hasPreserveExpiry) out = out.preserveExpiry(opts.getPreserveExpiry)
      if (opts.hasExpiry) out = convertExpiry(opts.getExpiry) match {
        case Left(expiry)  => out.expiry(expiry)
        case Right(expiry) => out.expiry(expiry)
      }
      // [end:1.5.0]
      if (opts.hasCas) out = out.cas(opts.getCas)
      if (opts.hasDurability) out = out.durability(convertDurability(opts.getDurability))
      if (opts.hasStoreSemantics) out = out.document(opts.getStoreSemantics match {
        case com.couchbase.client.protocol.sdk.collection.mutatein.StoreSemantics.INSERT =>
          StoreSemantics.Insert
        case com.couchbase.client.protocol.sdk.collection.mutatein.StoreSemantics.REPLACE =>
          StoreSemantics.Replace
        case com.couchbase.client.protocol.sdk.collection.mutatein.StoreSemantics.UPSERT =>
          StoreSemantics.Upsert
        case _ => throw new UnsupportedOperationException()
      })
      // [start:1.5.0]
      if (opts.hasCreateAsDeleted) out = out.createAsDeleted(opts.getCreateAsDeleted)
      // [end:1.5.0]
      Some(out)
    } else None
  }

  def mapContent[T](content: Seq[ContentOrMacro]): Seq[T] = {
    val out: Seq[Any] = content.map(v => {
      isContentOrMacro(v) match {
        case IsContentOrMacro.IsMacro(mac)                       => mac
        case IsContentOrMacro.IsContent(ContentString(value))    => value
        case IsContentOrMacro.IsContent(ContentJson(value))      => value
        case IsContentOrMacro.IsContent(ContentByteArray(value)) => value
        case IsContentOrMacro.IsContent(ContentNull(value))      => value
      }
    })

    out.map(v => v.asInstanceOf[T])
  }

  private def populateResult(
      request: com.couchbase.client.protocol.sdk.collection.mutatein.MutateIn,
      out: com.couchbase.client.protocol.run.Result.Builder,
      result: MutateInResult
  ): Unit = {
    val specResults = Range(0, request.getSpecCount)
      .map(i => {
        val spec = request.getSpec(i)

        val builder =
          com.couchbase.client.protocol.sdk.collection.mutatein.MutateInSpecResult.newBuilder

        if (spec.hasContentAs) {
          val contentAs = spec.getContentAs

          val content = ContentAsUtil.contentType(
            contentAs,
            () => result.contentAs[Array[Byte]](i),
            () => result.contentAs[String](i),
            () => result.contentAs[JsonObject](i),
            () => result.contentAs[JsonArray](i),
            () => result.contentAs[Boolean](i),
            () => result.contentAs[Int](i),
            () => result.contentAs[Double](i)
          )

          val contentResult: com.couchbase.client.protocol.shared.ContentOrError = content match {
            case Failure(err) =>
              com.couchbase.client.protocol.shared.ContentOrError.newBuilder
                .setException(ScalaSdkCommandExecutor.convertException(err))
                .build
            case Success(value) =>
              com.couchbase.client.protocol.shared.ContentOrError.newBuilder
                .setContent(value)
                .build
          }

          builder.setContentAsResult(contentResult)
        }

        builder.build
      })

    val builder = com.couchbase.client.protocol.sdk.collection.mutatein.MutateInResult.newBuilder
      .setCas(result.cas)
      .addAllResults(specResults.asJava)

    result.mutationToken.foreach(mt => {
      builder.setMutationToken(
        com.couchbase.client.protocol.shared.MutationToken.newBuilder
          .setPartitionId(mt.partitionID)
          .setPartitionUuid(mt.partitionUUID)
          .setSequenceNumber(mt.sequenceNumber)
          .setBucketName(mt.bucketName)
      )
    })

    out.setSdk(
      com.couchbase.client.protocol.sdk.Result.newBuilder
        .setMutateInResult(builder)
    )
  }
}
