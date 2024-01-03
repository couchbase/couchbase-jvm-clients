package com.couchbase.client.performer.scala.kv

import com.couchbase.client.performer.core.perf.PerRun
import com.couchbase.client.performer.core.util.TimeUtil.getTimeNow
import com.couchbase.client.performer.scala.ScalaSdkCommandExecutor
import com.couchbase.client.performer.scala.ScalaSdkCommandExecutor.{convertException, setSuccess}
import com.couchbase.client.performer.scala.util.SerializableValidation.assertIsSerializable
import com.couchbase.client.performer.scala.util.{ClusterConnection, ContentAsUtil, ScalaFluxStreamer, ScalaIteratorStreamer}
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.sdk.kv.lookupin
import com.couchbase.client.protocol.sdk.{CollectionLevelCommand, Command}
import com.couchbase.client.protocol.shared.DocLocation
import com.couchbase.client.scala.json.{JsonArray, JsonObject}
import com.couchbase.client.scala.kv._
import reactor.core.scala.publisher.{SFlux, SMono}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object LookupInHelper {
  def handleLookupIn(
      perRun: PerRun,
      connection: ClusterConnection,
      command: com.couchbase.client.protocol.sdk.Command,
      docId: (DocLocation) => String
  ): com.couchbase.client.protocol.run.Result.Builder = {
    val clc = command.getCollectionCommand
    val out = com.couchbase.client.protocol.run.Result.newBuilder()
    out.setInitiated(getTimeNow)

    if (clc.hasLookupIn) {
      handleLookupIn(connection, command, docId, clc, out)
    } else if (clc.hasLookupInAllReplicas) {
      handleLookupInAllReplicas(perRun, connection, command, docId, clc, out)
    } else if (clc.hasLookupInAnyReplica) {
      handleLookupInAnyReplica(connection, command, docId, clc, out)
    }

    out
  }

  def handleLookupInReactive(
      perRun: PerRun,
      connection: ClusterConnection,
      command: com.couchbase.client.protocol.sdk.Command,
      docId: (DocLocation) => String
  ): SMono[com.couchbase.client.protocol.run.Result.Builder] = {
    val clc = command.getCollectionCommand
    val out = com.couchbase.client.protocol.run.Result.newBuilder()
    out.setInitiated(getTimeNow)

    val result: SMono[Unit] = if (clc.hasLookupIn) {
      handleLookupInReactive(connection, command, docId, clc, out)
    } else if (clc.hasLookupInAllReplicas) {
      handleLookupInAllReplicasReactive(perRun, connection, command, docId, clc, out)
    } else {
      if (!clc.hasLookupInAnyReplica) {
        throw new UnsupportedOperationException()
      }
      handleLookupInAnyReplicaReactive(connection, command, docId, clc, out)
    }

    result.map(_ => out)
  }

  private def handleLookupIn(
      connection: ClusterConnection,
      command: Command,
      getId: (DocLocation) => String,
      clc: CollectionLevelCommand,
      out: Result.Builder
  ): Unit = {
    val req        = clc.getLookupIn
    val collection = connection.collection(req.getLocation)
    val options    = createOptions(req)
    val docId      = getId(req.getLocation)

    val specs = mapSpecs(req.getSpecList.asScala)

    val start = System.nanoTime

    val result = options match {
      case Some(opts) => collection.lookupIn(docId, specs, opts)
      case None       => collection.lookupIn(docId, specs)
    }

    out.setElapsedNanos(System.nanoTime - start)

    if (command.getReturnResult) populateResult(req, out, result.get)
    else setSuccess(out)
  }

  private def handleLookupInAnyReplica(
      connection: ClusterConnection,
      command: Command,
      getId: (DocLocation) => String,
      clc: CollectionLevelCommand,
      out: Result.Builder
  ): Unit = {
    // [start:<1.4.10]
    /*
      throw new UnsupportedOperationException("This version of the Scala SDK does not support lookupInAllReplicas")
      // [end:<1.4.10]
     */

    // [start:1.4.10]
    val req        = clc.getLookupInAnyReplica
    val collection = connection.collection(req.getLocation)
    val options    = createOptions(req)
    val docId      = getId(req.getLocation)

    val specs = mapSpecs(req.getSpecList.asScala)

    val start = System.nanoTime

    val result = options match {
      case Some(opts) => collection.lookupInAnyReplica(docId, specs, opts)
      case None       => collection.lookupInAnyReplica(docId, specs)
    }

    out.setElapsedNanos(System.nanoTime - start)

    if (command.getReturnResult) {
      val r = populateResult(req.getSpecList.asScala, result.get)
      out.setSdk(
        com.couchbase.client.protocol.sdk.Result.newBuilder.setLookupInAnyReplicaResult(r)
      )
    } else setSuccess(out)
    // [end:1.4.10]
  }

  private def handleLookupInAllReplicas(
      perRun: PerRun,
      connection: ClusterConnection,
      command: Command,
      getId: (DocLocation) => String,
      clc: CollectionLevelCommand,
      out: Result.Builder
  ): Unit = {
    // [start:<1.4.10]
    /*
    throw new UnsupportedOperationException("This version of the Scala SDK does not support lookupInAllReplicas")
    // [end:<1.4.10]
     */

    // [start:1.4.10]
    val req        = clc.getLookupInAllReplicas
    val collection = connection.collection(req.getLocation)
    val options    = createOptions(req)
    val docId      = getId(req.getLocation)

    val specs = mapSpecs(req.getSpecList.asScala)

    val start = System.nanoTime

    val iterator = options match {
      case Some(opts) => collection.lookupInAllReplicas(docId, specs, opts)
      case None       => collection.lookupInAllReplicas(docId, specs)
    }

    out.setElapsedNanos(System.nanoTime - start)

    val streamer = new ScalaIteratorStreamer[LookupInReplicaResult](
      iterator.get.toIterator,
      perRun,
      req.getStreamConfig.getStreamId,
      req.getStreamConfig,
      (r: LookupInReplicaResult) => {
        com.couchbase.client.protocol.run.Result.newBuilder
          .setSdk(
            com.couchbase.client.protocol.sdk.Result.newBuilder
              .setLookupInAllReplicasResult(
                com.couchbase.client.protocol.sdk.kv.lookupin.LookupInAllReplicasResult.newBuilder
                  .setStreamId(req.getStreamConfig.getStreamId)
                  .setLookupInReplicaResult(populateResult(req.getSpecList.asScala, r))
              )
          )
          .build
      },
      (err: Throwable) => convertException(err)
    )
    perRun.streamerOwner.addAndStart(streamer)
    out.setStream(
      com.couchbase.client.protocol.streams.Signal.newBuilder
        .setCreated(
          com.couchbase.client.protocol.streams.Created.newBuilder
            .setType(com.couchbase.client.protocol.streams.Type.STREAM_KV_RANGE_SCAN)
            .setStreamId(streamer.streamId)
        )
    )
    // [end:1.4.10]
  }

  private def handleLookupInReactive(
      connection: ClusterConnection,
      command: Command,
      getId: (DocLocation) => String,
      clc: CollectionLevelCommand,
      out: Result.Builder
  ): SMono[Unit] = {
    val req        = clc.getLookupIn
    val collection = connection.collection(req.getLocation).reactive
    val options    = createOptions(req)
    val docId      = getId(req.getLocation)

    val specs = mapSpecs(req.getSpecList.asScala)

    val start = System.nanoTime

    (options match {
      case Some(opts) => collection.lookupIn(docId, specs, opts)
      case None       => collection.lookupIn(docId, specs)
    }).map(result => {
      out.setElapsedNanos(System.nanoTime - start)

      if (command.getReturnResult) populateResult(req, out, result)
      else setSuccess(out)
    })
  }

  private def handleLookupInAnyReplicaReactive(
      connection: ClusterConnection,
      command: Command,
      getId: (DocLocation) => String,
      clc: CollectionLevelCommand,
      out: Result.Builder
  ): SMono[Unit] = {
    // [start:<1.4.10]
    /*
          throw new UnsupportedOperationException("This version of the Scala SDK does not support lookupInAnyReplica")
          // [end:<1.4.10]
     */

    // [start:1.4.10]
    val req        = clc.getLookupInAnyReplica
    val collection = connection.collection(req.getLocation).reactive
    val options    = createOptions(req)
    val docId      = getId(req.getLocation)

    val specs = mapSpecs(req.getSpecList.asScala)

    val start = System.nanoTime

    (options match {
      case Some(opts) => collection.lookupInAnyReplica(docId, specs, opts)
      case None       => collection.lookupInAnyReplica(docId, specs)
    }).map(result => {
      out.setElapsedNanos(System.nanoTime - start)

      if (command.getReturnResult) {
        val r = populateResult(req.getSpecList.asScala, result)
        out.setSdk(
          com.couchbase.client.protocol.sdk.Result.newBuilder.setLookupInAnyReplicaResult(r)
        )
      } else setSuccess(out)
    })
    // [end:1.4.10]
  }

  private def handleLookupInAllReplicasReactive(
      perRun: PerRun,
      connection: ClusterConnection,
      command: Command,
      getId: (DocLocation) => String,
      clc: CollectionLevelCommand,
      out: Result.Builder
  ): SMono[Unit] = {
    // [start:<1.4.10]
    /*
        throw new UnsupportedOperationException("This version of the Scala SDK does not support lookupInAllReplicas")
        // [end:<1.4.10]
     */

    // [start:1.4.10]
    val req        = clc.getLookupInAllReplicas
    val collection = connection.collection(req.getLocation).reactive
    val options    = createOptions(req)
    val docId      = getId(req.getLocation)

    val specs = mapSpecs(req.getSpecList.asScala)

    val start = System.nanoTime

    val results: SFlux[LookupInReplicaResult] = options match {
      case Some(opts) => collection.lookupInAllReplicas(docId, specs, opts)
      case None       => collection.lookupInAllReplicas(docId, specs)
    }

    out.setElapsedNanos(System.nanoTime - start)

    val streamer = new ScalaFluxStreamer[LookupInReplicaResult](
      results,
      perRun,
      req.getStreamConfig.getStreamId,
      req.getStreamConfig,
      (r: LookupInReplicaResult) => {
        com.couchbase.client.protocol.run.Result.newBuilder
          .setSdk(
            com.couchbase.client.protocol.sdk.Result.newBuilder
              .setLookupInAllReplicasResult(
                com.couchbase.client.protocol.sdk.kv.lookupin.LookupInAllReplicasResult.newBuilder
                  .setStreamId(req.getStreamConfig.getStreamId)
                  .setLookupInReplicaResult(populateResult(req.getSpecList.asScala, r))
              )
          )
          .build
      },
      (err: Throwable) => convertException(err)
    )
    perRun.streamerOwner.addAndStart(streamer)
    out.setStream(
      com.couchbase.client.protocol.streams.Signal.newBuilder
        .setCreated(
          com.couchbase.client.protocol.streams.Created.newBuilder
            .setType(com.couchbase.client.protocol.streams.Type.STREAM_KV_RANGE_SCAN)
            .setStreamId(streamer.streamId)
        )
    )

    SMono.empty
    // [end:1.4.10]
  }

  private def mapSpecs(specList: Seq[lookupin.LookupInSpec]): Seq[LookupInSpec] = {
    val specs = specList.map(v => {
      if (v.hasExists) {
        val out = LookupInSpec.exists(v.getExists.getPath)
        if (v.getExists.hasXattr && v.getExists.getXattr) out.xattr
        else out
      } else if (v.hasGet) {
        val out = LookupInSpec.get(v.getGet.getPath)
        if (v.getGet.hasXattr && v.getGet.getXattr) out.xattr
        else out
      } else if (v.hasCount) {
        val out = LookupInSpec.count(v.getCount.getPath)
        if (v.getCount.hasXattr && v.getCount.getXattr) out.xattr
        else out
      } else throw new UnsupportedOperationException(s"Unknown spec ${v}")
    })
    specs
  }

  private def createOptions(request: com.couchbase.client.protocol.sdk.kv.lookupin.LookupIn) = {
    if (request.hasOptions) {
      val opts = request.getOptions
      var out  = LookupInOptions()
      if (opts.hasTimeoutMillis)
        out = out.timeout(Duration.create(opts.getTimeoutMillis, TimeUnit.MILLISECONDS))
      if (opts.hasAccessDeleted)
        throw new UnsupportedOperationException(
          "SCBC-417: Scala SDK does not support accessDeleted"
        )
      assertIsSerializable(out)
      Some(out)
    } else None
  }

  // [start:1.4.10]
  private def createOptions(
      request: com.couchbase.client.protocol.sdk.kv.lookupin.LookupInAnyReplica
  ) = {
    if (request.hasOptions) {
      val opts = request.getOptions
      var out  = LookupInAnyReplicaOptions()
      if (opts.hasTimeoutMillis)
        out = out.timeout(Duration.create(opts.getTimeoutMillis, TimeUnit.MILLISECONDS))
      if (opts.hasParentSpanId)
        throw new UnsupportedOperationException(
          "Scala performer does not yet support OBSERVABILITY_1"
        )
      assertIsSerializable(out)
      Some(out)
    } else None
  }

  private def createOptions(
      request: com.couchbase.client.protocol.sdk.kv.lookupin.LookupInAllReplicas
  ) = {
    if (request.hasOptions) {
      val opts = request.getOptions
      var out  = LookupInAllReplicasOptions()
      if (opts.hasTimeoutMillis)
        out = out.timeout(Duration.create(opts.getTimeoutMillis, TimeUnit.MILLISECONDS))
      if (opts.hasParentSpanId)
        throw new UnsupportedOperationException(
          "Scala performer does not yet support OBSERVABILITY_1"
        )
      assertIsSerializable(out)
      Some(out)
    } else None
  }
  // [end:1.4.10]

  private def populateResult(
      request: com.couchbase.client.protocol.sdk.kv.lookupin.LookupIn,
      out: com.couchbase.client.protocol.run.Result.Builder,
      result: LookupInResult
  ): Unit = {
    assertIsSerializable(result)
    val specResults = Range(0, request.getSpecCount)
      .map(i => {
        val spec = request.getSpec(i)

        val existsResult = Try(result.exists(i)) match {
          case Success(value) => value
          case Failure(err) =>
            throw new IllegalStateException(
              "Scala SDK lookupInResults.exists(i) should never throw as it's not permitted by the signature (no Try)",
              err
            )
        }

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

        com.couchbase.client.protocol.sdk.kv.lookupin.LookupInSpecResult.newBuilder
          .setExistsResult(
            com.couchbase.client.protocol.sdk.kv.lookupin.BooleanOrError.newBuilder
              .setValue(existsResult)
          )
          .setContentAsResult(contentResult)
          .build
      })

    out.setSdk(
      com.couchbase.client.protocol.sdk.Result.newBuilder
        .setLookupInResult(
          com.couchbase.client.protocol.sdk.kv.lookupin.LookupInResult.newBuilder
            .setCas(result.cas)
            .addAllResults(specResults.asJava)
        )
    )
  }

  // [start:1.4.10]
  private def populateResult(
      specs: Seq[com.couchbase.client.protocol.sdk.kv.lookupin.LookupInSpec],
      result: LookupInReplicaResult
  ): com.couchbase.client.protocol.sdk.kv.lookupin.LookupInReplicaResult = {
    assertIsSerializable(result)
    val specResults = specs.zipWithIndex
      .map(x => {
        val spec = x._1
        val i    = x._2
        val existsResult = Try(result.exists(i)) match {
          case Success(value) => value
          case Failure(err) =>
            throw new IllegalStateException(
              "Scala SDK lookupInResults.exists(i) should never throw as it's not permitted by the signature (no Try)",
              err
            )
        }

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

        com.couchbase.client.protocol.sdk.kv.lookupin.LookupInSpecResult.newBuilder
          .setExistsResult(
            com.couchbase.client.protocol.sdk.kv.lookupin.BooleanOrError.newBuilder
              .setValue(existsResult)
          )
          .setContentAsResult(contentResult)
          .build
      })

    com.couchbase.client.protocol.sdk.kv.lookupin.LookupInReplicaResult.newBuilder
      .setIsReplica(result.isReplica)
      .setCas(result.cas)
      .addAllResults(specResults.asJava)
      .build
  }
  // [end:1.4.10]
}
